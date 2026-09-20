use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::fs;
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bicdb_analytics::{rebuild_sidecar, BicDataFusionContext};
use bicdb_core::{
    process_resident_bytes, BicDb, BicDbError, DbConfig, Event, EventProjection, Geometry,
    GraphProjection, HnswIndexConfig, IndexDefinition, IndexField, IndexKind, Memory,
    MemoryRecallOptions, MemoryType, NodeId, Record, ResidencyReport, StoredEvent, SyncCheckpoint,
    TimeSeriesFilter, VectorMetric, MEMORY_EVENT_STREAM,
};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use bicdb_sql::{SqlEngine, SqlValue};
use postgres::{Client as PostgresClient, NoTls, SimpleQueryMessage};
#[cfg(feature = "comparison-engines")]
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub type Result<T> = anyhow::Result<T>;

#[cfg(feature = "comparison-engines")]
const REDB_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("records");
const DAY_SECONDS: i64 = 86_400;

/// Stable machine-readable schema for the paged recovery benchmark.
pub const PAGED_RECOVERY_BENCH_FORMAT_VERSION: u32 = 5;
const MAX_PAGED_RECOVERY_RSS_SAMPLE_INTERVAL_MS: u64 = 1_000;
pub const MAX_PAGED_RECOVERY_EVIDENCE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PAGED_RECOVERY_ENVIRONMENT_TEXT_BYTES: usize = 512;
const RECOVERY_HASH_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ServerBenchScenario {
    IdlePooled,
    ReadOnly,
    Mixed,
    LongScan,
    CancelContention,
    Churn,
}

impl ServerBenchScenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IdlePooled => "idle-pooled",
            Self::ReadOnly => "read-only",
            Self::Mixed => "mixed",
            Self::LongScan => "long-scan",
            Self::CancelContention => "cancel-contention",
            Self::Churn => "churn",
        }
    }
}

impl std::str::FromStr for ServerBenchScenario {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "idle-pooled" | "idle" => Ok(Self::IdlePooled),
            "read-only" | "readonly" => Ok(Self::ReadOnly),
            "mixed" | "read-write" => Ok(Self::Mixed),
            "long-scan" | "scan" | "vector" => Ok(Self::LongScan),
            "cancel-contention" | "cancel" | "cancellation" => Ok(Self::CancelContention),
            "churn" | "connection-churn" => Ok(Self::Churn),
            _ => anyhow::bail!(
                "unknown server benchmark scenario {value}; expected idle-pooled, read-only, mixed, long-scan, cancel-contention, or churn"
            ),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CollectionSizeReport {
    pub name: String,
    pub segment_bytes: u64,
    pub logical_record_bytes: u64,
    pub storage_overhead_bytes: u64,
}

/// PubMed-shaped ingest against the paged engine.
///
/// Answers the questions that decide whether a corpus-scale import is viable,
/// on the real engine rather than by extrapolation:
///
/// - does RSS stop growing once the corpus exceeds the buffer pool?
/// - what does ingest actually cost per record?
/// - how long does restart take, and does it scale with the corpus?
///
/// Those are gates 1, 2, 4 and 6 of the PubMed plan.
#[derive(Clone, Debug, Serialize)]
pub struct PagedIngestReport {
    pub mode: &'static str,
    pub records: usize,
    pub record_bytes: usize,
    pub buffer_pool_bytes: u64,
    pub page_size: u32,
    pub logical_bytes: u64,
    pub on_disk_bytes: u64,
    #[serde(skip)]
    pub ingest_elapsed: Duration,
    pub records_per_sec: f64,
    #[serde(skip)]
    pub reopen_elapsed: Duration,
    /// RSS after ingest. The number that must NOT track corpus size.
    pub steady_resident_bytes: Option<u64>,
    pub peak_resident_bytes: Option<u64>,
    /// RSS divided by the configured pool budget. Near 1 plus a fixed process
    /// baseline is the goal; growing with the corpus is the failure.
    pub resident_over_budget: Option<f64>,
    pub buffer_pool: bicdb_page::BufferPoolSnapshot,
    pub wal: bicdb_page::WalSnapshot,
    pub pages_replayed_on_reopen: u64,
    /// Point lookups sampled after reopen, to show reads work cold.
    pub sampled_reads: usize,
    #[serde(skip)]
    pub sampled_read_elapsed: Duration,
}

impl PagedIngestReport {
    pub fn to_json(&self) -> Result<String> {
        let mut value = serde_json::to_value(self)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("ingest_ms".into(), json!(duration_ms(self.ingest_elapsed)));
            object.insert("reopen_ms".into(), json!(duration_ms(self.reopen_elapsed)));
            object.insert(
                "sampled_read_ms".into(),
                json!(duration_ms(self.sampled_read_elapsed)),
            );
        }
        Ok(serde_json::to_string_pretty(&value)?)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,logical_bytes,on_disk_bytes,buffer_pool_bytes,ingest_ms,\
             records_per_sec,reopen_ms,steady_resident_bytes,peak_resident_bytes,\
             resident_over_budget,hit_ratio,wal_bytes\n\
             {},{},{},{},{},{:.3},{:.1},{:.3},{},{},{:.3},{:.4},{}\n",
            self.mode,
            self.records,
            self.logical_bytes,
            self.on_disk_bytes,
            self.buffer_pool_bytes,
            duration_ms(self.ingest_elapsed),
            self.records_per_sec,
            duration_ms(self.reopen_elapsed),
            self.steady_resident_bytes.unwrap_or_default(),
            self.peak_resident_bytes.unwrap_or_default(),
            self.resident_over_budget.unwrap_or_default(),
            self.buffer_pool.hit_ratio(),
            self.wal.bytes_appended,
        )
    }
}

impl Display for PagedIngestReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "paged ingest")?;
        writeln!(
            f,
            "  records                 {} x ~{} B",
            self.records, self.record_bytes
        )?;
        writeln!(f, "  buffer pool budget      {}", self.buffer_pool_bytes)?;
        writeln!(f, "  logical bytes           {}", self.logical_bytes)?;
        writeln!(f, "  on disk                 {}", self.on_disk_bytes)?;
        writeln!(
            f,
            "  ingest                  {:.1} ms ({:.0} rec/s)",
            duration_ms(self.ingest_elapsed),
            self.records_per_sec
        )?;
        writeln!(
            f,
            "  reopen                  {:.1} ms ({} pages replayed)",
            duration_ms(self.reopen_elapsed),
            self.pages_replayed_on_reopen
        )?;
        writeln!(
            f,
            "  {} sampled reads     {:.1} ms",
            self.sampled_reads,
            duration_ms(self.sampled_read_elapsed)
        )?;
        writeln!(
            f,
            "  pool resident           {} of {} budget ({} pages, {:.1}% hit)",
            self.buffer_pool.resident_bytes,
            self.buffer_pool.budget_bytes,
            self.buffer_pool.resident_pages,
            self.buffer_pool.hit_ratio() * 100.0
        )?;
        if let Some(steady) = self.steady_resident_bytes {
            writeln!(f, "  process RSS             {steady}")?;
        }
        if let Some(peak) = self.peak_resident_bytes {
            writeln!(f, "  peak process RSS        {peak}")?;
        }
        if let Some(ratio) = self.resident_over_budget {
            writeln!(f, "  RSS / pool budget       {ratio:.2}x")?;
        }
        writeln!(f, "  wal bytes               {}", self.wal.bytes_appended)?;
        Ok(())
    }
}

/// Ingest `records` PubMed-shaped rows through the paged engine.
pub fn run_paged_ingest(
    path: impl AsRef<Path>,
    records: usize,
    record_bytes: usize,
    buffer_pool_bytes: u64,
    page_size: u32,
    batch: usize,
) -> Result<PagedIngestReport> {
    use bicdb_page::{PagedStore, PagedStoreOptions};

    let path = path.as_ref().to_path_buf();
    let options = PagedStoreOptions::default()
        .with_page_size(page_size)
        .with_buffer_pool_bytes(buffer_pool_bytes)
        .with_fsync(false)
        // Checkpoint often enough that the log stays bounded, which is what a
        // long import needs; an unbounded log would make recovery track total
        // writes rather than the post-checkpoint suffix.
        .with_wal_max_bytes(buffer_pool_bytes.max(8 * 1024 * 1024));

    let mut peak = process_resident_bytes();
    let mut observe = |peak: &mut Option<u64>| {
        if let (Some(current), Some(best)) = (process_resident_bytes(), peak.as_mut()) {
            if current > *best {
                *best = current;
            }
        }
    };

    let (store, _) = PagedStore::open(&path, options.clone())?;

    // A PubMed-ish record: an accession-style key and a metadata blob standing
    // in for title/abstract/authors.
    let abstract_text = "e".repeat(record_bytes.saturating_sub(120).max(1));
    let started = Instant::now();
    let mut logical_bytes = 0u64;
    let mut transaction = store.begin();

    for index in 0..records {
        let key = format!("PMID{index:012}");
        let value = format!(
            "{{\"pmid\":{index},\"title\":\"study {index}\",\"journal\":\"J{}\",\"abstract\":\"{}\"}}",
            index % 2_000,
            abstract_text
        );
        logical_bytes += (key.len() + value.len()) as u64;
        store.put(transaction, key.as_bytes(), value.as_bytes())?;

        if (index + 1) % batch.max(1) == 0 {
            store.commit(transaction)?;
            transaction = store.begin();
            observe(&mut peak);
        }
    }
    store.commit(transaction)?;
    store.checkpoint()?;
    let ingest_elapsed = started.elapsed();
    observe(&mut peak);

    let buffer_pool = store.buffer_pool().snapshot();
    let wal = store.wal().snapshot();
    let steady_resident_bytes = process_resident_bytes();
    let on_disk_bytes = total_dir_bytes(&path);
    drop(store);

    // Restart: the number that decides whether a 40M-row corpus can be
    // restarted in a sane amount of time.
    let reopen_started = Instant::now();
    let (store, recovery) = PagedStore::open(&path, options)?;
    let reopen_elapsed = reopen_started.elapsed();

    // Cold point reads, scattered so they do not all hit one page.
    let sampled_reads = 1_000.min(records);
    let read_started = Instant::now();
    let stride = (records / sampled_reads.max(1)).max(1);
    for index in (0..records).step_by(stride).take(sampled_reads) {
        let key = format!("PMID{index:012}");
        let found = store.get(key.as_bytes())?;
        if found.is_none() {
            return Err(anyhow::anyhow!(
                "record {index} was lost: committed then unreadable after reopen"
            ));
        }
    }
    let sampled_read_elapsed = read_started.elapsed();
    observe(&mut peak);

    Ok(PagedIngestReport {
        mode: "paged_ingest",
        records,
        record_bytes,
        buffer_pool_bytes,
        page_size,
        logical_bytes,
        on_disk_bytes,
        ingest_elapsed,
        records_per_sec: records as f64 / ingest_elapsed.as_secs_f64().max(f64::EPSILON),
        reopen_elapsed,
        steady_resident_bytes,
        peak_resident_bytes: peak,
        resident_over_budget: steady_resident_bytes
            .map(|rss| rss as f64 / buffer_pool_bytes as f64),
        buffer_pool,
        wal,
        pages_replayed_on_reopen: recovery.pages_replayed,
        sampled_reads,
        sampled_read_elapsed,
    })
}

/// Durable fixture metadata produced before recovery is measured in a clean
/// process. Keeping preparation and measurement separate prevents allocator
/// pages retained by bulk fixture generation from being charged to recovery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PagedRecoveryFixtureReport {
    pub format_version: u32,
    pub path: PathBuf,
    pub requested_checkpointed_data_bytes: u64,
    pub actual_checkpointed_data_bytes: u64,
    pub checkpointed_page_file_bytes: u64,
    pub checkpointed_records: u64,
    pub requested_wal_bytes: u64,
    pub actual_wal_bytes: u64,
    pub page_file_bytes: u64,
    pub page_size: u32,
    pub buffer_pool_bytes: u64,
    pub record_bytes: usize,
    pub batch_size: usize,
    pub suffix_records: u64,
    pub suffix_transactions: u64,
    pub fsync_enabled: bool,
    pub generation_ms: f64,
}

/// Recovery measurements emitted by the clean probe process.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PagedRecoveryProbeReport {
    pub format_version: u32,
    pub path: PathBuf,
    pub page_size: u32,
    pub buffer_pool_bytes: u64,
    pub fsync_enabled: bool,
    pub sample_interval_ms: u64,
    pub expected_checkpointed_records: u64,
    pub expected_suffix_records: u64,
    pub recovery_open_ms: f64,
    pub rss_before_recovery_bytes: Option<u64>,
    pub peak_recovery_rss_bytes: Option<u64>,
    pub rss_after_recovery_bytes: Option<u64>,
    pub recovery_rss_growth_bytes: Option<u64>,
    pub verified_checkpointed_records: u64,
    pub verified_suffix_records: u64,
    pub recovery: bicdb_page::RecoveryReport,
}

/// Operator-observed cache state for one recovery measurement. BicDB cannot
/// evict the host page cache safely, so the preparation is explicit evidence
/// rather than an inferred claim.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PagedRecoveryCacheState {
    Uncontrolled,
    Cold,
    Warm,
}

impl PagedRecoveryCacheState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uncontrolled => "uncontrolled",
            Self::Cold => "cold",
            Self::Warm => "warm",
        }
    }
}

impl std::str::FromStr for PagedRecoveryCacheState {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "uncontrolled" => Ok(Self::Uncontrolled),
            "cold" => Ok(Self::Cold),
            "warm" => Ok(Self::Warm),
            _ => anyhow::bail!(
                "unknown recovery cache state {value}; expected uncontrolled, cold, or warm"
            ),
        }
    }
}

/// Reproducibility envelope bound into a recovery report. It intentionally
/// excludes hostname and record data while retaining the physical facts needed
/// to compare release evidence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedRecoveryEnvironmentReport {
    pub format_version: u32,
    pub bicdb_version: String,
    pub source_revision: Option<String>,
    pub executable_path: PathBuf,
    pub executable_bytes: u64,
    pub executable_sha256: String,
    pub database_path: PathBuf,
    pub operating_system: String,
    pub architecture: String,
    pub kernel_release: Option<String>,
    pub cpu_model: Option<String>,
    pub logical_cpu_count: usize,
    pub total_memory_bytes: Option<u64>,
    pub filesystem_type: Option<String>,
    pub filesystem_source: Option<String>,
    pub filesystem_mount_point: Option<PathBuf>,
    pub filesystem_mount_options: Option<String>,
    pub filesystem_device: Option<String>,
    pub cache_state: PagedRecoveryCacheState,
    pub cache_preparation: Option<String>,
    pub captured_at_unix_ms: u64,
}

/// Optional release limits. Omitted limits are reported but do not invent a
/// pass threshold for hardware the operator has not declared.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PagedRecoveryBenchLimits {
    pub max_recovery_ms: Option<f64>,
    pub max_peak_rss_bytes: Option<u64>,
    pub max_rss_growth_bytes: Option<u64>,
    /// Require complete, durable, attributable production evidence rather than
    /// accepting a development smoke measurement.
    pub require_release_evidence: bool,
}

/// Final, machine-readable recovery certification artifact.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PagedRecoveryBenchReport {
    pub format_version: u32,
    pub mode: String,
    pub environment: PagedRecoveryEnvironmentReport,
    pub fixture: PagedRecoveryFixtureReport,
    pub probe: PagedRecoveryProbeReport,
    pub limits: PagedRecoveryBenchLimits,
    pub passed: bool,
    pub failures: Vec<String>,
    pub checksum_sha256: String,
}

impl PagedRecoveryBenchReport {
    pub fn calculate_checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&unsigned)?)))
    }

    /// Verify a persisted artifact independently of its generating process.
    /// The checksum detects accidental byte-field substitution; recomputing
    /// the complete gate outcome prevents a serialized pass/failure claim from
    /// disagreeing with the measurements it contains.
    pub fn verify_integrity(&self) -> Result<()> {
        if self.format_version != PAGED_RECOVERY_BENCH_FORMAT_VERSION {
            anyhow::bail!("paged recovery report version mismatch");
        }
        validate_paged_recovery_components(
            &self.environment,
            &self.fixture,
            &self.probe,
            &self.limits,
        )?;
        if self.mode != "paged_recovery" {
            anyhow::bail!("unexpected paged recovery report mode {}", self.mode);
        }
        let failures = evaluate_paged_recovery_bench(
            &self.environment,
            &self.fixture,
            &self.probe,
            &self.limits,
        );
        if self.failures != failures || self.passed != failures.is_empty() {
            anyhow::bail!("paged recovery report outcome was not derived from its evidence");
        }
        validate_sha256_text("paged recovery report checksum", &self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            anyhow::bail!("paged recovery report checksum mismatch");
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn to_csv(&self) -> String {
        let header = [
            "mode",
            "bicdb_version",
            "source_revision",
            "executable_bytes",
            "executable_sha256",
            "operating_system",
            "architecture",
            "kernel_release",
            "cpu_model",
            "logical_cpu_count",
            "total_memory_bytes",
            "filesystem_type",
            "filesystem_source",
            "filesystem_mount_point",
            "filesystem_mount_options",
            "filesystem_device",
            "cache_state",
            "cache_preparation",
            "captured_at_unix_ms",
            "requested_checkpointed_data_bytes",
            "actual_checkpointed_data_bytes",
            "checkpointed_page_file_bytes",
            "checkpointed_records",
            "requested_wal_bytes",
            "actual_wal_bytes",
            "page_file_bytes",
            "suffix_records",
            "suffix_transactions",
            "page_size",
            "buffer_pool_bytes",
            "recovery_open_ms",
            "rss_before_recovery_bytes",
            "peak_recovery_rss_bytes",
            "rss_after_recovery_bytes",
            "recovery_rss_growth_bytes",
            "records_scanned",
            "pages_replayed",
            "transaction_outcomes",
            "terminal_outcome_records",
            "peak_transaction_outcome_bytes",
            "frozen_outcomes_at_open",
            "abort_exceptions_at_open",
            "status_spill_entries_at_open",
            "status_spill_pages_at_open",
            "scan_passes",
            "peak_record_bytes",
            "verified_checkpointed_records",
            "verified_suffix_records",
            "require_release_evidence",
            "passed",
            "checksum_sha256",
            "path",
        ];
        let mount_point = self
            .environment
            .filesystem_mount_point
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        let row = vec![
            self.mode.clone(),
            bench_csv_escape(&self.environment.bicdb_version),
            bench_csv_escape(
                self.environment
                    .source_revision
                    .as_deref()
                    .unwrap_or_default(),
            ),
            self.environment.executable_bytes.to_string(),
            self.environment.executable_sha256.clone(),
            bench_csv_escape(&self.environment.operating_system),
            bench_csv_escape(&self.environment.architecture),
            bench_csv_escape(
                self.environment
                    .kernel_release
                    .as_deref()
                    .unwrap_or_default(),
            ),
            bench_csv_escape(self.environment.cpu_model.as_deref().unwrap_or_default()),
            self.environment.logical_cpu_count.to_string(),
            self.environment
                .total_memory_bytes
                .unwrap_or_default()
                .to_string(),
            bench_csv_escape(
                self.environment
                    .filesystem_type
                    .as_deref()
                    .unwrap_or_default(),
            ),
            bench_csv_escape(
                self.environment
                    .filesystem_source
                    .as_deref()
                    .unwrap_or_default(),
            ),
            bench_csv_escape(&mount_point),
            bench_csv_escape(
                self.environment
                    .filesystem_mount_options
                    .as_deref()
                    .unwrap_or_default(),
            ),
            bench_csv_escape(
                self.environment
                    .filesystem_device
                    .as_deref()
                    .unwrap_or_default(),
            ),
            self.environment.cache_state.as_str().to_string(),
            bench_csv_escape(
                self.environment
                    .cache_preparation
                    .as_deref()
                    .unwrap_or_default(),
            ),
            self.environment.captured_at_unix_ms.to_string(),
            self.fixture.requested_checkpointed_data_bytes.to_string(),
            self.fixture.actual_checkpointed_data_bytes.to_string(),
            self.fixture.checkpointed_page_file_bytes.to_string(),
            self.fixture.checkpointed_records.to_string(),
            self.fixture.requested_wal_bytes.to_string(),
            self.fixture.actual_wal_bytes.to_string(),
            self.fixture.page_file_bytes.to_string(),
            self.fixture.suffix_records.to_string(),
            self.fixture.suffix_transactions.to_string(),
            self.fixture.page_size.to_string(),
            self.fixture.buffer_pool_bytes.to_string(),
            format!("{:.3}", self.probe.recovery_open_ms),
            self.probe
                .rss_before_recovery_bytes
                .unwrap_or_default()
                .to_string(),
            self.probe
                .peak_recovery_rss_bytes
                .unwrap_or_default()
                .to_string(),
            self.probe
                .rss_after_recovery_bytes
                .unwrap_or_default()
                .to_string(),
            self.probe
                .recovery_rss_growth_bytes
                .unwrap_or_default()
                .to_string(),
            self.probe.recovery.records_scanned.to_string(),
            self.probe.recovery.pages_replayed.to_string(),
            self.probe.recovery.transaction_outcomes.to_string(),
            self.probe.recovery.terminal_outcome_records.to_string(),
            self.probe
                .recovery
                .peak_transaction_outcome_bytes
                .to_string(),
            self.probe.recovery.frozen_outcomes_at_open.to_string(),
            self.probe.recovery.abort_exceptions_at_open.to_string(),
            self.probe.recovery.status_spill_entries_at_open.to_string(),
            self.probe.recovery.status_spill_pages_at_open.to_string(),
            self.probe.recovery.scan_passes.to_string(),
            self.probe.recovery.peak_record_bytes.to_string(),
            self.probe.verified_checkpointed_records.to_string(),
            self.probe.verified_suffix_records.to_string(),
            self.limits.require_release_evidence.to_string(),
            self.passed.to_string(),
            self.checksum_sha256.clone(),
            bench_csv_escape(&self.fixture.path.display().to_string()),
        ];
        debug_assert_eq!(header.len(), row.len());
        format!("{}\n{}\n", header.join(","), row.join(","))
    }
}

impl Display for PagedRecoveryBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "paged WAL recovery")?;
        writeln!(
            formatter,
            "  binary version / SHA-256 {} / {}",
            self.environment.bicdb_version, self.environment.executable_sha256
        )?;
        writeln!(
            formatter,
            "  cache state              {}",
            self.environment.cache_state.as_str()
        )?;
        writeln!(
            formatter,
            "  WAL requested / actual   {} / {}",
            self.fixture.requested_wal_bytes, self.fixture.actual_wal_bytes
        )?;
        writeln!(
            formatter,
            "  checkpointed data/rows   {} / {}",
            self.fixture.actual_checkpointed_data_bytes, self.fixture.checkpointed_records
        )?;
        writeln!(
            formatter,
            "  suffix rows/transactions {} / {}",
            self.fixture.suffix_records, self.fixture.suffix_transactions
        )?;
        writeln!(
            formatter,
            "  recovery open            {:.1} ms",
            self.probe.recovery_open_ms
        )?;
        writeln!(
            formatter,
            "  WAL scans / records      {} / {}",
            self.probe.recovery.scan_passes, self.probe.recovery.records_scanned
        )?;
        writeln!(
            formatter,
            "  pages / outcomes         {} / {}",
            self.probe.recovery.pages_replayed, self.probe.recovery.transaction_outcomes
        )?;
        writeln!(
            formatter,
            "  outcome records / bytes {} / {}",
            self.probe.recovery.terminal_outcome_records,
            self.probe.recovery.peak_transaction_outcome_bytes
        )?;
        writeln!(
            formatter,
            "  frozen / exceptions     {} / {}",
            self.probe.recovery.frozen_outcomes_at_open,
            self.probe.recovery.abort_exceptions_at_open
        )?;
        writeln!(
            formatter,
            "  peak record buffer       {} bytes",
            self.probe.recovery.peak_record_bytes
        )?;
        if let Some(before) = self.probe.rss_before_recovery_bytes {
            writeln!(formatter, "  RSS before recovery      {before}")?;
        }
        if let Some(peak) = self.probe.peak_recovery_rss_bytes {
            writeln!(formatter, "  peak recovery RSS        {peak}")?;
        }
        if let Some(growth) = self.probe.recovery_rss_growth_bytes {
            writeln!(formatter, "  recovery RSS growth      {growth}")?;
        }
        writeln!(
            formatter,
            "  verified base/suffix     {} / {}",
            self.probe.verified_checkpointed_records, self.probe.verified_suffix_records
        )?;
        writeln!(
            formatter,
            "  release limits           {}",
            if self.passed { "PASS" } else { "FAIL" }
        )?;
        for failure in &self.failures {
            writeln!(formatter, "    - {failure}")?;
        }
        writeln!(
            formatter,
            "  report SHA-256           {}",
            self.checksum_sha256
        )?;
        Ok(())
    }
}

#[derive(Debug)]
struct RecoveryFilesystemEnvironment {
    filesystem_type: String,
    source: String,
    mount_point: PathBuf,
    mount_options: String,
    device: String,
}

/// Capture the executable and physical host context used by one recovery run.
/// File hashing uses a fixed buffer, and procfs reads have hard byte ceilings.
pub fn collect_paged_recovery_environment(
    executable: impl AsRef<Path>,
    database_path: impl AsRef<Path>,
    source_revision: Option<String>,
    cache_state: PagedRecoveryCacheState,
    cache_preparation: Option<String>,
) -> Result<PagedRecoveryEnvironmentReport> {
    let executable_path = executable.as_ref().canonicalize().with_context(|| {
        format!(
            "cannot resolve recovery benchmark executable {}",
            executable.as_ref().display()
        )
    })?;
    let metadata = fs::metadata(&executable_path)?;
    if !metadata.is_file() || metadata.len() == 0 {
        anyhow::bail!(
            "recovery benchmark executable is not a non-empty regular file: {}",
            executable_path.display()
        );
    }
    let executable_sha256 = sha256_file_bounded(&executable_path)?;
    let source_revision = validate_optional_environment_text("source revision", source_revision)?;
    let cache_preparation =
        validate_optional_environment_text("cache preparation", cache_preparation)?;
    let database_path = database_path.as_ref().to_path_buf();
    let filesystem = recovery_filesystem_environment(&database_path)?;
    let kernel_release = read_text_file_bounded(Path::new("/proc/sys/kernel/osrelease"), 4096)
        .ok()
        .and_then(first_nonempty_line);
    let cpu_model = read_text_file_bounded(Path::new("/proc/cpuinfo"), 4 * 1024 * 1024)
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                matches!(name.trim(), "model name" | "Hardware" | "Processor")
                    .then(|| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        });
    let total_memory_bytes = read_text_file_bounded(Path::new("/proc/meminfo"), 1024 * 1024)
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                let rest = line.strip_prefix("MemTotal:")?.trim();
                let kib = rest.split_whitespace().next()?.parse::<u64>().ok()?;
                kib.checked_mul(1024)
            })
        });
    let captured_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);

    let report = PagedRecoveryEnvironmentReport {
        format_version: PAGED_RECOVERY_BENCH_FORMAT_VERSION,
        bicdb_version: env!("CARGO_PKG_VERSION").to_string(),
        source_revision,
        executable_path,
        executable_bytes: metadata.len(),
        executable_sha256,
        database_path,
        operating_system: std::env::consts::OS.to_string(),
        architecture: std::env::consts::ARCH.to_string(),
        kernel_release,
        cpu_model,
        logical_cpu_count: thread::available_parallelism().map_or(0, usize::from),
        total_memory_bytes,
        filesystem_type: filesystem
            .as_ref()
            .map(|value| value.filesystem_type.clone()),
        filesystem_source: filesystem.as_ref().map(|value| value.source.clone()),
        filesystem_mount_point: filesystem.as_ref().map(|value| value.mount_point.clone()),
        filesystem_mount_options: filesystem.as_ref().map(|value| value.mount_options.clone()),
        filesystem_device: filesystem.map(|value| value.device),
        cache_state,
        cache_preparation,
        captured_at_unix_ms,
    };
    validate_paged_recovery_environment(&report)?;
    Ok(report)
}

/// Fail before generating a large fixture when production evidence inputs are
/// incomplete. The final report repeats these gates so offline verification
/// cannot be weakened by bypassing the CLI preflight.
pub fn validate_paged_recovery_release_environment(
    environment: &PagedRecoveryEnvironmentReport,
    fsync_enabled: bool,
) -> Result<()> {
    validate_paged_recovery_environment(environment)?;
    let failures = paged_recovery_release_environment_failures(environment, fsync_enabled);
    if !failures.is_empty() {
        anyhow::bail!(
            "recovery release-evidence preflight failed: {}",
            failures.join("; ")
        );
    }
    Ok(())
}

fn sha256_file_bounded(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let before = file.metadata()?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; RECOVERY_HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        anyhow::bail!(
            "file changed while hashing recovery evidence: {}",
            path.display()
        );
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_text_file_bounded(path: &Path, max_bytes: u64) -> Result<String> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        anyhow::bail!(
            "environment file exceeds {max_bytes} bytes: {}",
            path.display()
        );
    }
    Ok(String::from_utf8(bytes)?)
}

fn first_nonempty_line(text: String) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

fn validate_optional_environment_text(name: &str, value: Option<String>) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty()
        || value.len() > MAX_PAGED_RECOVERY_ENVIRONMENT_TEXT_BYTES
        || value.chars().any(char::is_control)
        || value.trim() != value
    {
        anyhow::bail!(
            "{name} must be 1..={MAX_PAGED_RECOVERY_ENVIRONMENT_TEXT_BYTES} bytes without surrounding whitespace or control characters"
        );
    }
    Ok(Some(value))
}

fn recovery_filesystem_environment(path: &Path) -> Result<Option<RecoveryFilesystemEnvironment>> {
    if std::env::consts::OS != "linux" {
        return Ok(None);
    }
    let mut existing = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    while !existing.exists() {
        if !existing.pop() {
            anyhow::bail!("cannot locate an existing ancestor for {}", path.display());
        }
    }
    let target = existing.canonicalize()?;
    let mountinfo = read_text_file_bounded(Path::new("/proc/self/mountinfo"), 4 * 1024 * 1024)?;
    let mut best: Option<RecoveryFilesystemEnvironment> = None;
    for line in mountinfo.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let left: Vec<_> = left.split_whitespace().collect();
        let right: Vec<_> = right.split_whitespace().collect();
        if left.len() < 6 || right.len() < 3 {
            continue;
        }
        let mount_point = PathBuf::from(decode_mountinfo_field(left[4])?);
        if !target.starts_with(&mount_point)
            || best.as_ref().is_some_and(|current| {
                current.mount_point.as_os_str().len() >= mount_point.as_os_str().len()
            })
        {
            continue;
        }
        let options = format!("{},{}", left[5], right[2]);
        best = Some(RecoveryFilesystemEnvironment {
            filesystem_type: right[0].to_string(),
            source: decode_mountinfo_field(right[1])?,
            mount_point,
            mount_options: options,
            device: left[2].to_string(),
        });
    }
    Ok(best)
}

fn decode_mountinfo_field(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if index + 3 >= bytes.len()
                || !bytes[index + 1..=index + 3]
                    .iter()
                    .all(|byte| matches!(byte, b'0'..=b'7'))
            {
                anyhow::bail!("invalid escaped field in Linux mount metadata");
            }
            let octal = u16::from(bytes[index + 1] - b'0') * 64
                + u16::from(bytes[index + 2] - b'0') * 8
                + u16::from(bytes[index + 3] - b'0');
            decoded.push(
                u8::try_from(octal)
                    .map_err(|_| anyhow::anyhow!("invalid octal value in Linux mount metadata"))?,
            );
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(String::from_utf8(decoded)?)
}

/// Build a post-checkpoint WAL suffix without crossing the automatic
/// checkpoint threshold. The caller must provide a new path: a benchmark must
/// never overwrite a real database merely because `--path` was mistyped.
pub fn prepare_paged_recovery_fixture(
    path: impl AsRef<Path>,
    requested_checkpointed_data_bytes: u64,
    requested_wal_bytes: u64,
    record_bytes: usize,
    buffer_pool_bytes: u64,
    page_size: u32,
    batch_size: usize,
    fsync: bool,
) -> Result<PagedRecoveryFixtureReport> {
    use bicdb_page::{PagedStore, PagedStoreOptions};

    let path = path.as_ref().to_path_buf();
    if requested_wal_bytes == 0 {
        anyhow::bail!("requested WAL suffix must be greater than zero");
    }
    if record_bytes == 0 {
        anyhow::bail!("record size must be greater than zero");
    }
    if batch_size == 0 {
        anyhow::bail!("transaction batch size must be greater than zero");
    }
    if path.exists() {
        anyhow::bail!(
            "paged recovery benchmark refuses existing path {}",
            path.display()
        );
    }

    let options = PagedStoreOptions::default()
        .with_page_size(page_size)
        .with_buffer_pool_bytes(buffer_pool_bytes)
        .with_fsync(fsync)
        // This fixture intentionally represents the suffix that exists before
        // the checkpoint supervisor runs. A maximum threshold avoids a setup
        // commit racing the benchmark by beginning an automatic checkpoint.
        .with_wal_max_bytes(u64::MAX);
    let generation_started = Instant::now();
    let (store, _) = PagedStore::open(&path, options)?;

    // Build an independently sized, fully checkpointed database generation.
    // Holding the WAL suffix constant while varying this input is the test that
    // recovery follows recent log bytes instead of total database bytes.
    let mut checkpointed_records = 0u64;
    let mut actual_checkpointed_data_bytes = 0u64;
    while actual_checkpointed_data_bytes < requested_checkpointed_data_bytes {
        let transaction = store.begin();
        for _ in 0..batch_size {
            if actual_checkpointed_data_bytes >= requested_checkpointed_data_bytes {
                break;
            }
            let key = recovery_bench_base_key(checkpointed_records);
            let value = recovery_bench_base_value(checkpointed_records, record_bytes);
            store.put(transaction, key.as_bytes(), &value)?;
            actual_checkpointed_data_bytes =
                actual_checkpointed_data_bytes.saturating_add((key.len() + value.len()) as u64);
            checkpointed_records = checkpointed_records.saturating_add(1);
        }
        store.commit(transaction)?;
    }
    store.checkpoint()?;
    let checkpointed_page_file_bytes = store.snapshot()?.page_file_bytes;

    let mut suffix_records = 0u64;
    let mut suffix_transactions = 0u64;
    let actual_wal_bytes = loop {
        let transaction = store.begin();
        for _ in 0..batch_size {
            let key = recovery_bench_key(suffix_records);
            let value = recovery_bench_value(suffix_records, record_bytes);
            store.put(transaction, key.as_bytes(), &value)?;
            suffix_records = suffix_records.saturating_add(1);
        }
        store.commit(transaction)?;
        suffix_transactions = suffix_transactions.saturating_add(1);
        let wal_bytes = store.snapshot()?.wal_bytes;
        if wal_bytes >= requested_wal_bytes {
            break wal_bytes;
        }
    };
    let snapshot = store.snapshot()?;
    drop(store);

    Ok(PagedRecoveryFixtureReport {
        format_version: PAGED_RECOVERY_BENCH_FORMAT_VERSION,
        path,
        requested_checkpointed_data_bytes,
        actual_checkpointed_data_bytes,
        checkpointed_page_file_bytes,
        checkpointed_records,
        requested_wal_bytes,
        actual_wal_bytes,
        page_file_bytes: snapshot.page_file_bytes,
        page_size,
        buffer_pool_bytes,
        record_bytes,
        batch_size,
        suffix_records,
        suffix_transactions,
        fsync_enabled: fsync,
        generation_ms: evidence_duration_ms(generation_started.elapsed()),
    })
}

/// Open and verify a prepared fixture. Production callers should execute this
/// function in a fresh process; the CLI does that automatically.
pub fn run_paged_recovery_probe(
    path: impl AsRef<Path>,
    expected_checkpointed_records: u64,
    expected_suffix_records: u64,
    page_size: u32,
    buffer_pool_bytes: u64,
    fsync: bool,
    sample_interval_ms: u64,
) -> Result<PagedRecoveryProbeReport> {
    use bicdb_page::{PagedStore, PagedStoreOptions};

    if expected_suffix_records == 0 {
        anyhow::bail!("expected suffix record count must be greater than zero");
    }
    if !(1..=MAX_PAGED_RECOVERY_RSS_SAMPLE_INTERVAL_MS).contains(&sample_interval_ms) {
        anyhow::bail!(
            "RSS sample interval must be between 1 and {MAX_PAGED_RECOVERY_RSS_SAMPLE_INTERVAL_MS} milliseconds"
        );
    }
    let path = path.as_ref().to_path_buf();
    let rss_before_recovery_bytes = process_resident_bytes();
    let peak = Arc::new(AtomicU64::new(
        rss_before_recovery_bytes.unwrap_or_default(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = rss_before_recovery_bytes.map(|_| {
        let peak = peak.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if let Some(current) = process_resident_bytes() {
                    peak.fetch_max(current, Ordering::Relaxed);
                }
                thread::park_timeout(Duration::from_millis(sample_interval_ms));
            }
        })
    });

    let options = PagedStoreOptions::default()
        .with_page_size(page_size)
        .with_buffer_pool_bytes(buffer_pool_bytes)
        .with_fsync(fsync)
        .with_wal_max_bytes(u64::MAX);
    let recovery_started = Instant::now();
    let opened = PagedStore::open(&path, options);
    let recovery_open_ms = evidence_duration_ms(recovery_started.elapsed());
    let rss_after_recovery_bytes = process_resident_bytes();
    if let Some(after) = rss_after_recovery_bytes {
        peak.fetch_max(after, Ordering::Relaxed);
    }
    stop.store(true, Ordering::Release);
    if let Some(sampler) = sampler {
        sampler.thread().unpark();
        sampler
            .join()
            .map_err(|_| anyhow::anyhow!("recovery RSS sampler thread panicked"))?;
    }
    let (store, recovery) = opened?;

    let checkpointed_sample_indexes = recovery_bench_sample_indexes(expected_checkpointed_records);
    for index in &checkpointed_sample_indexes {
        let key = recovery_bench_base_key(*index);
        let value = store
            .get(key.as_bytes())?
            .ok_or_else(|| anyhow::anyhow!("checkpointed row {index} is missing"))?;
        if value != recovery_bench_base_value(*index, value.len()) {
            anyhow::bail!("checkpointed row {index} has the wrong value");
        }
    }
    let suffix_sample_indexes = recovery_bench_sample_indexes(expected_suffix_records);
    for index in &suffix_sample_indexes {
        let key = recovery_bench_key(*index);
        let value = store
            .get(key.as_bytes())?
            .ok_or_else(|| anyhow::anyhow!("recovered row {index} is missing"))?;
        if value != recovery_bench_value(*index, value.len()) {
            anyhow::bail!("recovered row {index} has the wrong value");
        }
    }

    let peak_recovery_rss_bytes = rss_before_recovery_bytes.map(|_| peak.load(Ordering::Relaxed));
    Ok(PagedRecoveryProbeReport {
        format_version: PAGED_RECOVERY_BENCH_FORMAT_VERSION,
        path,
        page_size,
        buffer_pool_bytes,
        fsync_enabled: fsync,
        sample_interval_ms,
        expected_checkpointed_records,
        expected_suffix_records,
        recovery_open_ms,
        rss_before_recovery_bytes,
        peak_recovery_rss_bytes,
        rss_after_recovery_bytes,
        recovery_rss_growth_bytes: peak_recovery_rss_bytes
            .zip(rss_before_recovery_bytes)
            .map(|(peak, before)| peak.saturating_sub(before)),
        verified_checkpointed_records: checkpointed_sample_indexes.len() as u64,
        verified_suffix_records: suffix_sample_indexes.len() as u64,
        recovery,
    })
}

/// Bind independently produced fixture and clean-process reports, enforce
/// structural invariants, and apply operator-declared release limits.
pub fn finish_paged_recovery_bench(
    environment: PagedRecoveryEnvironmentReport,
    fixture: PagedRecoveryFixtureReport,
    probe: PagedRecoveryProbeReport,
    limits: PagedRecoveryBenchLimits,
) -> Result<PagedRecoveryBenchReport> {
    validate_paged_recovery_components(&environment, &fixture, &probe, &limits)?;
    let failures = evaluate_paged_recovery_bench(&environment, &fixture, &probe, &limits);
    let mut report = PagedRecoveryBenchReport {
        format_version: PAGED_RECOVERY_BENCH_FORMAT_VERSION,
        mode: "paged_recovery".to_string(),
        environment,
        fixture,
        probe,
        limits,
        passed: failures.is_empty(),
        failures,
        checksum_sha256: String::new(),
    };
    report.checksum_sha256 = report.calculate_checksum()?;
    report.verify_integrity()?;
    Ok(report)
}

fn validate_paged_recovery_components(
    environment: &PagedRecoveryEnvironmentReport,
    fixture: &PagedRecoveryFixtureReport,
    probe: &PagedRecoveryProbeReport,
    limits: &PagedRecoveryBenchLimits,
) -> Result<()> {
    if fixture.format_version != PAGED_RECOVERY_BENCH_FORMAT_VERSION
        || probe.format_version != PAGED_RECOVERY_BENCH_FORMAT_VERSION
    {
        anyhow::bail!("paged recovery benchmark report version mismatch");
    }
    validate_paged_recovery_environment(environment)?;
    if environment.database_path != fixture.path
        || fixture.path != probe.path
        || fixture.page_size != probe.page_size
        || fixture.buffer_pool_bytes != probe.buffer_pool_bytes
        || fixture.fsync_enabled != probe.fsync_enabled
        || fixture.checkpointed_records != probe.expected_checkpointed_records
        || fixture.suffix_records != probe.expected_suffix_records
    {
        anyhow::bail!(
            "paged recovery environment, fixture, and probe do not describe the same run"
        );
    }
    if fixture.requested_wal_bytes == 0
        || fixture.actual_wal_bytes < fixture.requested_wal_bytes
        || fixture.suffix_records == 0
        || fixture.suffix_transactions == 0
        || fixture.record_bytes == 0
        || fixture.batch_size == 0
        || fixture.page_file_bytes < fixture.checkpointed_page_file_bytes
        || fixture.actual_checkpointed_data_bytes < fixture.requested_checkpointed_data_bytes
        || !fixture.generation_ms.is_finite()
        || fixture.generation_ms < 0.0
        || !probe.recovery_open_ms.is_finite()
        || probe.recovery_open_ms < 0.0
    {
        anyhow::bail!("paged recovery fixture or probe contains impossible measurements");
    }
    for (name, limit) in [
        ("max_recovery_ms", limits.max_recovery_ms),
        (
            "max_peak_rss_bytes",
            limits.max_peak_rss_bytes.map(|value| value as f64),
        ),
        (
            "max_rss_growth_bytes",
            limits.max_rss_growth_bytes.map(|value| value as f64),
        ),
    ] {
        if limit.is_some_and(|value| !value.is_finite() || value < 0.0) {
            anyhow::bail!("{name} must be finite and non-negative");
        }
    }
    Ok(())
}

fn validate_paged_recovery_environment(environment: &PagedRecoveryEnvironmentReport) -> Result<()> {
    if environment.format_version != PAGED_RECOVERY_BENCH_FORMAT_VERSION {
        anyhow::bail!("paged recovery environment version mismatch");
    }
    validate_sha256_text(
        "recovery benchmark executable SHA-256",
        &environment.executable_sha256,
    )?;
    if environment.bicdb_version.is_empty()
        || environment.bicdb_version.len() > MAX_PAGED_RECOVERY_ENVIRONMENT_TEXT_BYTES
        || environment.operating_system.is_empty()
        || environment.architecture.is_empty()
        || environment.executable_bytes == 0
        || environment.executable_path.as_os_str().is_empty()
        || environment.database_path.as_os_str().is_empty()
        || environment.logical_cpu_count == 0
        || environment.total_memory_bytes == Some(0)
        || environment
            .filesystem_mount_point
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        || environment.captured_at_unix_ms == 0
    {
        anyhow::bail!("paged recovery environment contains missing or impossible identity fields");
    }
    validate_optional_environment_text("source revision", environment.source_revision.clone())?;
    validate_optional_environment_text("cache preparation", environment.cache_preparation.clone())?;
    for (name, value) in [
        ("kernel release", environment.kernel_release.as_deref()),
        ("CPU model", environment.cpu_model.as_deref()),
        ("filesystem type", environment.filesystem_type.as_deref()),
        (
            "filesystem source",
            environment.filesystem_source.as_deref(),
        ),
        (
            "filesystem mount options",
            environment.filesystem_mount_options.as_deref(),
        ),
        (
            "filesystem device",
            environment.filesystem_device.as_deref(),
        ),
    ] {
        if let Some(value) = value {
            validate_optional_environment_text(name, Some(value.to_string()))?;
        }
    }
    Ok(())
}

fn validate_sha256_text(name: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{name} must be exactly 64 hexadecimal characters");
    }
    Ok(())
}

fn paged_recovery_release_environment_failures(
    environment: &PagedRecoveryEnvironmentReport,
    fsync_enabled: bool,
) -> Vec<String> {
    let mut failures = Vec::new();
    if !fsync_enabled {
        failures.push("release evidence requires fsync=true".to_string());
    }
    if !environment
        .source_revision
        .as_deref()
        .is_some_and(|revision| {
            matches!(revision.len(), 40 | 64)
                && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        failures
            .push("release evidence requires an exact 40- or 64-hex source revision".to_string());
    }
    if environment.cache_state == PagedRecoveryCacheState::Uncontrolled {
        failures.push("release evidence requires an explicit cold or warm cache state".to_string());
    }
    if environment.cache_preparation.is_none() {
        failures.push("release evidence requires bounded cache-preparation details".to_string());
    }
    if environment.kernel_release.is_none() {
        failures.push("release evidence requires the kernel release".to_string());
    }
    if environment.cpu_model.is_none() {
        failures.push("release evidence requires the CPU model".to_string());
    }
    if environment.total_memory_bytes.is_none() {
        failures.push("release evidence requires total host memory".to_string());
    }
    if environment.filesystem_type.is_none()
        || environment.filesystem_source.is_none()
        || environment.filesystem_mount_point.is_none()
        || environment.filesystem_mount_options.is_none()
        || environment.filesystem_device.is_none()
    {
        failures.push(
            "release evidence requires filesystem, mount, source, and device identity".to_string(),
        );
    }
    failures
}

fn evaluate_paged_recovery_bench(
    environment: &PagedRecoveryEnvironmentReport,
    fixture: &PagedRecoveryFixtureReport,
    probe: &PagedRecoveryProbeReport,
    limits: &PagedRecoveryBenchLimits,
) -> Vec<String> {
    let mut failures = if limits.require_release_evidence {
        paged_recovery_release_environment_failures(environment, fixture.fsync_enabled)
    } else {
        Vec::new()
    };
    if probe.recovery.scan_passes != 2 {
        failures.push(format!(
            "recovery used {} WAL passes, expected exactly 2",
            probe.recovery.scan_passes
        ));
    }
    if probe.recovery.wal_bytes_scanned != fixture.actual_wal_bytes {
        failures.push(format!(
            "recovery scanned {} WAL bytes, fixture recorded {}",
            probe.recovery.wal_bytes_scanned, fixture.actual_wal_bytes
        ));
    }
    let max_record_bytes = (bicdb_page::MAX_WAL_RECORD_BYTES as u64)
        .saturating_sub(u64::from(bicdb_page::MAX_PAGE_SIZE))
        .saturating_add(u64::from(fixture.page_size));
    if probe.recovery.peak_record_bytes > max_record_bytes {
        failures.push(format!(
            "recovery record buffer {} exceeded hard limit {}",
            probe.recovery.peak_record_bytes, max_record_bytes
        ));
    }
    if probe.recovery.terminal_outcome_records < probe.recovery.transaction_outcomes {
        failures.push(format!(
            "recovery reported {} terminal records for {} distinct outcomes",
            probe.recovery.terminal_outcome_records, probe.recovery.transaction_outcomes
        ));
    }
    let minimum_outcome_bytes = probe.recovery.transaction_outcomes.saturating_mul(9);
    if probe.recovery.peak_transaction_outcome_bytes < minimum_outcome_bytes {
        failures.push(format!(
            "recovery outcome allocation {} is below the {}-byte compact representation",
            probe.recovery.peak_transaction_outcome_bytes, minimum_outcome_bytes
        ));
    }
    let exception_capacity =
        u64::try_from(bicdb_page::abort_exception_capacity(fixture.page_size)).unwrap_or(u64::MAX);
    if probe.recovery.abort_exceptions_at_open > exception_capacity {
        failures.push(format!(
            "recovery retained {} abort exceptions beyond the {}-entry durable capacity",
            probe.recovery.abort_exceptions_at_open, exception_capacity
        ));
    }
    // The fixture closes every transaction before restart, so nothing may pin
    // the watermark: the open-time freeze must absorb the entire outcome set.
    // A gap here means recovery left a suffix-sized resident state behind.
    if probe.recovery.frozen_outcomes_at_open != probe.recovery.transaction_outcomes {
        failures.push(format!(
            "open-time freeze absorbed {} of {} terminal outcomes",
            probe.recovery.frozen_outcomes_at_open, probe.recovery.transaction_outcomes
        ));
    }
    let spill_per_page =
        u64::try_from(bicdb_page::status_spill_entries_per_page(fixture.page_size))
            .unwrap_or(u64::MAX);
    let spill_capacity = probe
        .recovery
        .status_spill_pages_at_open
        .saturating_mul(spill_per_page);
    if probe.recovery.status_spill_entries_at_open > spill_capacity {
        failures.push(format!(
            "recovery spill holds {} entries beyond {} pages' {}-entry capacity",
            probe.recovery.status_spill_entries_at_open,
            probe.recovery.status_spill_pages_at_open,
            spill_capacity
        ));
    }
    // A fully closed fixture pins nothing, so no outcome may be left in the
    // disk-backed spill at open.
    if probe.recovery.status_spill_entries_at_open != 0 {
        failures.push(format!(
            "open-time recovery retained {} spill entries for a fully closed fixture",
            probe.recovery.status_spill_entries_at_open
        ));
    }
    let expected_checkpointed_verified = fixture.checkpointed_records.min(3);
    if probe.verified_checkpointed_records != expected_checkpointed_verified {
        failures.push(format!(
            "verified {} checkpointed rows, expected {}",
            probe.verified_checkpointed_records, expected_checkpointed_verified
        ));
    }
    let expected_suffix_verified = fixture.suffix_records.min(3);
    if probe.verified_suffix_records != expected_suffix_verified {
        failures.push(format!(
            "verified {} suffix rows, expected {}",
            probe.verified_suffix_records, expected_suffix_verified
        ));
    }
    if let Some(limit) = limits.max_recovery_ms {
        if probe.recovery_open_ms > limit {
            failures.push(format!(
                "recovery {:.3} ms exceeded {:.3} ms limit",
                probe.recovery_open_ms, limit
            ));
        }
    }
    if let Some(limit) = limits.max_peak_rss_bytes {
        match probe.peak_recovery_rss_bytes {
            Some(actual) if actual > limit => failures.push(format!(
                "peak recovery RSS {actual} exceeded {limit} byte limit"
            )),
            None => failures.push("peak recovery RSS is unavailable on this platform".to_string()),
            Some(_) => {}
        }
    }
    if let Some(limit) = limits.max_rss_growth_bytes {
        match probe.recovery_rss_growth_bytes {
            Some(actual) if actual > limit => failures.push(format!(
                "recovery RSS growth {actual} exceeded {limit} byte limit"
            )),
            None => {
                failures.push("recovery RSS growth is unavailable on this platform".to_string())
            }
            Some(_) => {}
        }
    }

    failures
}

fn recovery_bench_key(index: u64) -> String {
    format!("recovery-{index:020}")
}

fn recovery_bench_value(index: u64, record_bytes: usize) -> Vec<u8> {
    let mut value = format!("record-{index:020}:").into_bytes();
    value.resize(record_bytes.max(value.len()), b'x');
    value
}

fn recovery_bench_base_key(index: u64) -> String {
    format!("checkpointed-{index:020}")
}

fn recovery_bench_base_value(index: u64, record_bytes: usize) -> Vec<u8> {
    let mut value = format!("base-record-{index:020}:").into_bytes();
    value.resize(record_bytes.max(value.len()), b'b');
    value
}

fn recovery_bench_sample_indexes(records: u64) -> BTreeSet<u64> {
    if records == 0 {
        return BTreeSet::new();
    }
    [0, records / 2, records - 1].into_iter().collect()
}

fn total_dir_bytes(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total += metadata.len();
            }
        }
    }
    total
}

/// Phase 0 storage baseline: what the current `embedded_memory` engine costs in
/// memory and open time at a given data size.
///
/// Every gate in `docs/server-paged-storage-todo.md` is stated relative to these
/// numbers ("open does not scan every row", "steady RSS stays inside the
/// envelope", "recovery is proportional to the WAL suffix, not total bytes"), so
/// they have to be measured before there is anything to compare against — and
/// measured by a repeatable harness rather than recorded once by hand.
#[derive(Clone, Debug, Serialize)]
pub struct StorageBaselineReport {
    pub mode: &'static str,
    pub storage_mode: String,
    pub records: usize,
    pub metadata_bytes_per_record: usize,
    pub vector_dim: usize,
    pub indexes: usize,
    /// Bytes of record metadata generated, before any storage overhead. The
    /// denominator for every ratio below.
    pub logical_bytes: u64,
    #[serde(skip)]
    pub build_elapsed: Duration,
    /// Time to open the database from a cold process state. Expected to grow
    /// with total data in `embedded_memory`; the Phase 3 gate is that it stops.
    #[serde(skip)]
    pub open_elapsed: Duration,
    /// On-disk bytes across the whole database directory.
    pub database_size_bytes: u64,
    /// On-disk bytes per logical byte stored. A cold open reads and decodes
    /// essentially all of this, so it is the read amplification that sets open
    /// time today.
    pub on_disk_amplification: f64,
    /// Bytes in materialized collection segments.
    ///
    /// **Zero until the first checkpoint.** Freshly written records live in the
    /// transaction log and are folded into segments by checkpoint/compaction, so
    /// a build-then-measure run sees `database_size_bytes` dominated by the WAL
    /// and `segment_bytes == 0`. Reported separately rather than summed so the
    /// split is visible instead of misleading.
    pub segment_bytes: u64,
    /// RSS immediately after open, before any query traffic.
    pub steady_resident_bytes: Option<u64>,
    /// Highest RSS observed across build and open.
    pub peak_resident_bytes: Option<u64>,
    /// Resident bytes per logical byte stored. The single number that says
    /// whether the engine can hold a database larger than RAM: while it is above
    /// 1.0, it cannot.
    pub resident_per_logical_byte: Option<f64>,
    pub residency: ResidencyReport,
}

impl StorageBaselineReport {
    pub fn to_json(&self) -> Result<String> {
        let mut value = serde_json::to_value(self)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("build_ms".into(), json!(duration_ms(self.build_elapsed)));
            object.insert("open_ms".into(), json!(duration_ms(self.open_elapsed)));
        }
        Ok(serde_json::to_string_pretty(&value)?)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,logical_bytes,database_size_bytes,build_ms,open_ms,\
             steady_resident_bytes,peak_resident_bytes,accounted_bytes,\
             resident_per_logical_byte,on_disk_amplification\n\
             {},{},{},{},{:.3},{:.3},{},{},{},{:.4},{:.4}\n",
            self.mode,
            self.records,
            self.logical_bytes,
            self.database_size_bytes,
            duration_ms(self.build_elapsed),
            duration_ms(self.open_elapsed),
            self.steady_resident_bytes.unwrap_or_default(),
            self.peak_resident_bytes.unwrap_or_default(),
            self.residency.accounted_bytes,
            self.resident_per_logical_byte.unwrap_or_default(),
            self.on_disk_amplification,
        )
    }
}

impl Display for StorageBaselineReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "storage baseline ({})", self.storage_mode)?;
        writeln!(
            f,
            "  records                    {} x {} B metadata{}",
            self.records,
            self.metadata_bytes_per_record,
            if self.vector_dim > 0 {
                format!(" + {}-dim vector", self.vector_dim)
            } else {
                String::new()
            }
        )?;
        writeln!(f, "  logical bytes              {}", self.logical_bytes)?;
        writeln!(
            f,
            "  build / open               {:.1} ms / {:.1} ms",
            duration_ms(self.build_elapsed),
            duration_ms(self.open_elapsed)
        )?;
        writeln!(
            f,
            "  database on disk           {} ({:.2}x logical, segments {})",
            self.database_size_bytes, self.on_disk_amplification, self.segment_bytes
        )?;
        writeln!(
            f,
            "  accounted resident         {}",
            self.residency.accounted_bytes
        )?;
        writeln!(
            f,
            "    rows                     {}",
            self.residency.rows_bytes
        )?;
        writeln!(
            f,
            "    version chains           {}",
            self.residency.version_chains_bytes
        )?;
        writeln!(
            f,
            "    primary-key maps         {}",
            self.residency.primary_key_maps_bytes
        )?;
        writeln!(
            f,
            "    secondary indexes        {}",
            self.residency.secondary_indexes_bytes
        )?;
        writeln!(
            f,
            "    exact vectors            {}",
            self.residency.exact_vectors_bytes
        )?;
        if let Some(steady) = self.steady_resident_bytes {
            writeln!(f, "  process RSS after open     {steady}")?;
        }
        if let Some(peak) = self.peak_resident_bytes {
            writeln!(f, "  peak process RSS           {peak}")?;
        }
        if let Some(ratio) = self.resident_per_logical_byte {
            writeln!(f, "  resident per logical byte  {ratio:.2}")?;
        }
        Ok(())
    }
}

/// Build a database of a chosen shape, then measure what it costs to hold and to
/// reopen. See [`StorageBaselineReport`].
pub fn run_storage_baseline(
    path: impl AsRef<Path>,
    records: usize,
    metadata_bytes: usize,
    vector_dim: usize,
    index_count: usize,
    batch_size: usize,
) -> Result<StorageBaselineReport> {
    let path = path.as_ref().to_path_buf();
    let mut peak = process_resident_bytes();
    let mut observe_peak = |peak: &mut Option<u64>| {
        if let (Some(current), Some(best)) = (process_resident_bytes(), peak.as_mut()) {
            if current > *best {
                *best = current;
            }
        }
    };

    // fsync off: this measures memory and open cost, not the durability path,
    // and leaving it on would make the build time dominated by disk sync.
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("baseline")?;
    for index in 0..index_count {
        db.create_index(IndexDefinition {
            name: format!("baseline_field{index}"),
            collection: "baseline".to_string(),
            fields: vec![IndexField::MetadataPath(vec![format!("field{index}")])],
            kind: IndexKind::BTree,
            unique: false,
            predicate: None,
            exclusion: None,
        })?;
    }

    // A filler string sized so the serialized record lands near the requested
    // per-record metadata budget.
    let filler = "x".repeat(metadata_bytes.saturating_sub(96).max(1));
    let started = Instant::now();
    let mut logical_bytes = 0u64;
    let mut batch = Vec::with_capacity(batch_size.max(1));
    for index in 0..records {
        let metadata = json!({
            "idx": index,
            "field0": index % 1_000,
            "field1": format!("group-{}", index % 97),
            "body": filler,
        });
        logical_bytes += serde_json::to_vec(&metadata)?.len() as u64;
        let mut record = Record::new(format!("record-{index:012}")).with_metadata(metadata);
        if vector_dim > 0 {
            record = record.with_vector(vec![0.125_f32; vector_dim]);
            logical_bytes += (vector_dim * std::mem::size_of::<f32>()) as u64;
        }
        batch.push(record);
        if batch.len() >= batch_size.max(1) {
            db.batch_insert("baseline", std::mem::take(&mut batch))?;
            observe_peak(&mut peak);
        }
    }
    if !batch.is_empty() {
        db.batch_insert("baseline", batch)?;
    }
    db.flush()?;
    let build_elapsed = started.elapsed();
    observe_peak(&mut peak);
    drop(db);

    // Reopen to measure cold open cost. In `embedded_memory` this rebuilds the
    // full resident projection, which is exactly the behaviour the roadmap's
    // Phase 3 gate is meant to eliminate.
    let open_started = Instant::now();
    let db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let open_elapsed = open_started.elapsed();
    observe_peak(&mut peak);

    let residency = db.residency_report()?;
    let stats = db.stats()?;
    let steady_resident_bytes = process_resident_bytes();
    // Fold the steady reading in: it is taken after the report and stats walks,
    // both of which allocate, so it can legitimately exceed every earlier
    // sample. A "peak" below the steady value would be plainly wrong.
    if let (Some(steady), Some(best)) = (steady_resident_bytes, peak.as_mut()) {
        if steady > *best {
            *best = steady;
        }
    }
    let segment_bytes: u64 = stats
        .collections
        .iter()
        .map(|collection| collection.segment_bytes)
        .sum();

    Ok(StorageBaselineReport {
        mode: "storage_baseline",
        storage_mode: bicdb_core::storage_mode(&path)?.to_string(),
        records,
        metadata_bytes_per_record: metadata_bytes,
        vector_dim,
        indexes: index_count,
        logical_bytes,
        build_elapsed,
        open_elapsed,
        database_size_bytes: stats.size_bytes,
        segment_bytes,
        on_disk_amplification: if logical_bytes == 0 {
            0.0
        } else {
            stats.size_bytes as f64 / logical_bytes as f64
        },
        steady_resident_bytes,
        peak_resident_bytes: peak,
        resident_per_logical_byte: steady_resident_bytes
            .and_then(|rss| (logical_bytes > 0).then(|| rss as f64 / logical_bytes as f64)),
        residency,
    })
}

#[derive(Clone, Debug, Serialize)]
pub struct InsertBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub batch_size: usize,
    #[serde(skip)]
    pub elapsed: Duration,
    pub records_per_sec: f64,
    pub database_size_bytes: u64,
    pub collection_sizes: Vec<CollectionSizeReport>,
    #[serde(skip)]
    pub startup_recovery_time: Duration,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct VectorBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub dim: usize,
    pub top_k: usize,
    #[serde(skip)]
    pub insert_elapsed: Duration,
    #[serde(skip)]
    pub search_p50: Duration,
    #[serde(skip)]
    pub search_p95: Duration,
    #[serde(skip)]
    pub search_p99: Duration,
    pub database_size_bytes: u64,
    pub collection_sizes: Vec<CollectionSizeReport>,
    #[serde(skip)]
    pub startup_recovery_time: Duration,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct VectorProfileReport {
    pub mode: &'static str,
    pub strategy: &'static str,
    pub records: usize,
    pub dim: usize,
    pub top_k: usize,
    pub searches: usize,
    pub metric: &'static str,
    pub insert_elapsed_ms: f64,
    pub total_p50_ms: f64,
    pub total_p95_ms: f64,
    pub total_p99_ms: f64,
    pub avg_read_vectors_ms: f64,
    pub avg_similarity_ms: f64,
    pub avg_top_k_heap_ms: f64,
    pub avg_final_sort_ms: f64,
    pub candidates_scanned: usize,
    pub vectors_read: usize,
    pub allocation_count_total: u64,
    pub allocation_count_per_search: f64,
    pub allocated_bytes_total: u64,
    pub allocated_bytes_per_search: f64,
    pub allocation_bytes_max: u64,
    pub result_count: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct VectorSearchHotReport {
    pub mode: &'static str,
    pub strategy: &'static str,
    pub records: usize,
    pub dim: usize,
    pub top_k: usize,
    pub searches: usize,
    pub metric: &'static str,
    pub search_p50_ms: f64,
    pub search_p95_ms: f64,
    pub search_p99_ms: f64,
    pub elapsed_ms: f64,
    pub result_count: usize,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct AnnBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub dim: usize,
    pub top_k: usize,
    pub searches: usize,
    pub insert_elapsed_ms: f64,
    pub index_build_ms: f64,
    pub index_size_bytes: u64,
    pub memory_estimate_bytes: u64,
    pub exact_p50_ms: f64,
    pub exact_p95_ms: f64,
    pub exact_p99_ms: f64,
    pub ann_ef20_p50_ms: f64,
    pub ann_ef20_p95_ms: f64,
    pub ann_ef20_p99_ms: f64,
    pub ann_ef20_recall_at_k: f64,
    pub ann_ef50_p50_ms: f64,
    pub ann_ef50_p95_ms: f64,
    pub ann_ef50_p99_ms: f64,
    pub ann_ef50_recall_at_k: f64,
    pub ann_ef100_p50_ms: f64,
    pub ann_ef100_p95_ms: f64,
    pub ann_ef100_p99_ms: f64,
    pub ann_ef100_recall_at_k: f64,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct GraphBenchReport {
    pub mode: &'static str,
    pub entities: usize,
    pub edges: usize,
    pub insert_elapsed_ms: f64,
    pub build_elapsed_ms: f64,
    pub neighbor_lookup_ms: f64,
    pub path_query_ms: f64,
    pub edge_scan_ms: f64,
    pub node_count: usize,
    pub edge_count: usize,
    pub graph_size_bytes: u64,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct SpatialBenchReport {
    pub mode: &'static str,
    pub points: usize,
    pub insert_elapsed_ms: f64,
    pub points_per_sec: f64,
    pub index_build_ms: f64,
    pub index_size_bytes: u64,
    pub database_size_bytes: u64,
    pub collection_sizes: Vec<CollectionSizeReport>,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct SpatialNearestBenchReport {
    pub mode: &'static str,
    pub points: usize,
    pub queries: usize,
    pub nearest_p50_ms: f64,
    pub nearest_p95_ms: f64,
    pub nearest_p99_ms: f64,
    pub radius_p50_ms: f64,
    pub radius_p95_ms: f64,
    pub radius_p99_ms: f64,
    pub nearest_result_count: usize,
    pub radius_result_count: usize,
    pub radius_meters: f64,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct RouteBenchReport {
    pub mode: &'static str,
    pub nodes: usize,
    pub edges: usize,
    pub insert_elapsed_ms: f64,
    pub route_p50_ms: f64,
    pub route_p95_ms: f64,
    pub route_p99_ms: f64,
    pub route_result_count: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventBenchReport {
    pub mode: &'static str,
    pub events: usize,
    pub append_elapsed_ms: f64,
    pub append_events_per_sec: f64,
    pub subscriber_p50_ms: f64,
    pub subscriber_p95_ms: f64,
    pub subscriber_p99_ms: f64,
    pub replay_elapsed_ms: f64,
    pub replay_events_per_sec: f64,
    pub replayed_events: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueueBenchReport {
    pub mode: &'static str,
    pub messages: usize,
    pub consume_batch_size: usize,
    pub publish_elapsed_ms: f64,
    pub publish_messages_per_sec: f64,
    pub consume_elapsed_ms: f64,
    pub consume_messages_per_sec: f64,
    pub consumed_messages: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProjectionBenchReport {
    pub mode: &'static str,
    pub events: usize,
    pub entities: usize,
    pub append_elapsed_ms: f64,
    pub append_events_per_sec: f64,
    pub rebuild_elapsed_ms: f64,
    pub rebuild_events_per_sec: f64,
    pub projected_entities: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VectorProfileStrategy {
    OptimizedStore,
    RecordScan,
}

#[derive(Clone, Debug, Serialize)]
pub struct WearableBenchReport {
    pub mode: &'static str,
    pub devices: usize,
    pub records: usize,
    #[serde(skip)]
    pub insert_elapsed: Duration,
    pub records_per_sec: f64,
    #[serde(skip)]
    pub time_range_scan_elapsed: Duration,
    pub scanned_records: usize,
    #[serde(skip)]
    pub latest_lookup_elapsed: Duration,
    pub summary_count: usize,
    pub database_size_bytes: u64,
    pub collection_sizes: Vec<CollectionSizeReport>,
    #[serde(skip)]
    pub startup_recovery_time: Duration,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct SqlBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub insert_elapsed_ms: f64,
    pub select_by_id_sql_ms: f64,
    pub select_by_id_direct_ms: f64,
    pub timestamp_range_sql_ms: f64,
    pub timestamp_range_direct_ms: f64,
    pub timestamp_range_rows: usize,
    pub count_sql_ms: f64,
    pub count_direct_ms: f64,
    pub count_rows: usize,
    pub avg_sql_ms: f64,
    pub avg_direct_ms: f64,
    pub avg_value: f64,
    pub order_by_limit_sql_ms: f64,
    pub order_by_limit_direct_ms: f64,
    pub order_by_limit_rows: usize,
    pub database_size_bytes: u64,
    pub collection_sizes: Vec<CollectionSizeReport>,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct IndexBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub insert_without_indexes_ms: f64,
    pub insert_with_indexes_ms: f64,
    pub write_overhead_ms: f64,
    pub index_build_ms: f64,
    pub index_rebuild_ms: f64,
    pub index_size_bytes: u64,
    pub peak_memory_estimate_bytes: u64,
    pub point_lookup_scan_ms: f64,
    pub point_lookup_index_ms: f64,
    pub point_lookup_rows: usize,
    pub timestamp_range_scan_ms: f64,
    pub timestamp_range_index_ms: f64,
    pub timestamp_range_rows: usize,
    pub metadata_equality_scan_ms: f64,
    pub metadata_equality_index_ms: f64,
    pub metadata_equality_rows: usize,
    pub composite_lookup_scan_ms: f64,
    pub composite_lookup_index_ms: f64,
    pub composite_lookup_rows: usize,
    pub order_by_scan_ms: f64,
    pub order_by_index_ms: f64,
    pub order_by_rows: usize,
    pub database_size_bytes: u64,
    pub collection_sizes: Vec<CollectionSizeReport>,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransactionBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub batch_size: usize,
    pub single_insert_tx_ms: f64,
    pub single_insert_tx_per_sec: f64,
    pub batch_insert_tx_ms: f64,
    pub batch_insert_tx_per_sec: f64,
    pub rollback_ms: f64,
    pub rollback_records_per_sec: f64,
    pub recovery_ms: f64,
    pub recovered_records: usize,
    pub snapshot_scan_ms: f64,
    pub snapshot_records: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct AnalyticsBenchReport {
    pub mode: &'static str,
    pub records: usize,
    pub insert_elapsed_ms: f64,
    pub sidecar_rebuild_ms: f64,
    pub sidecar_size_bytes: u64,
    pub direct_count_ms: f64,
    pub query_exec_count_ms: f64,
    pub datafusion_count_p50_ms: f64,
    pub datafusion_count_p95_ms: f64,
    pub datafusion_count_p99_ms: f64,
    pub direct_avg_ms: f64,
    pub query_exec_avg_ms: f64,
    pub datafusion_avg_p50_ms: f64,
    pub datafusion_avg_p95_ms: f64,
    pub datafusion_avg_p99_ms: f64,
    pub direct_min_max_ms: f64,
    pub query_exec_min_max_ms: f64,
    pub datafusion_min_max_p50_ms: f64,
    pub datafusion_min_max_p95_ms: f64,
    pub datafusion_min_max_p99_ms: f64,
    pub datafusion_group_by_metric_p50_ms: f64,
    pub datafusion_group_by_metric_p95_ms: f64,
    pub datafusion_group_by_metric_p99_ms: f64,
    pub datafusion_group_by_device_p50_ms: f64,
    pub datafusion_group_by_device_p95_ms: f64,
    pub datafusion_group_by_device_p99_ms: f64,
    pub datafusion_timestamp_range_p50_ms: f64,
    pub datafusion_timestamp_range_p95_ms: f64,
    pub datafusion_timestamp_range_p99_ms: f64,
    pub datafusion_rows_per_sec: f64,
    pub arrow_memory_bytes: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct MemoryBenchReport {
    pub mode: &'static str,
    pub memories: usize,
    pub dim: usize,
    pub top_k: usize,
    pub insert_elapsed_ms: f64,
    pub memories_per_sec: f64,
    pub recall_p50_ms: f64,
    pub recall_p95_ms: f64,
    pub recall_p99_ms: f64,
    pub ranking_memories_per_sec: f64,
    pub recall_result_count: usize,
    pub timeline_elapsed_ms: f64,
    pub timeline_memories: usize,
    pub workspace_load_ms: f64,
    pub workspace_memories: usize,
    pub memory_event_count: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct SyncBenchReport {
    pub mode: &'static str,
    pub records_per_node: usize,
    pub left_export_events: usize,
    pub right_export_events: usize,
    pub left_export_elapsed_ms: f64,
    pub right_export_elapsed_ms: f64,
    pub export_events_per_sec: f64,
    pub right_import_elapsed_ms: f64,
    pub left_import_elapsed_ms: f64,
    pub import_events_per_sec: f64,
    pub records_merged: usize,
    pub merge_records_per_sec: f64,
    pub conflicts_resolved: usize,
    pub conflicts_per_sec: f64,
    pub converged: bool,
    pub audit_events: usize,
    pub database_size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServerBenchReport {
    pub mode: &'static str,
    pub scenario: &'static str,
    pub clients: usize,
    pub active_query_concurrency: usize,
    pub queries: usize,
    pub select_queries: usize,
    pub insert_queries: usize,
    pub setup_latency_p50_ms: f64,
    pub setup_latency_p95_ms: f64,
    pub setup_latency_p99_ms: f64,
    pub elapsed_ms: f64,
    pub queries_per_sec: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub active_connections_peak: usize,
    pub rejected_connections: u64,
    pub server_max_queued_queries: usize,
    pub server_queued_queries_max: usize,
    pub server_active_reads_peak: usize,
    pub server_queued_reads_max: usize,
    pub server_active_writes_peak: usize,
    pub server_queued_writes_max: usize,
    pub server_query_queue_wait_p50_ms: f64,
    pub server_query_queue_wait_p95_ms: f64,
    pub server_query_queue_wait_p99_ms: f64,
    pub server_reported_queries: u64,
    pub server_failed_queries: u64,
    pub server_canceled_queries: u64,
    pub server_timed_out_queries: u64,
    pub server_writes_executed: u64,
    pub server_max_queued_writes: usize,
    pub server_write_queue_depth_max: usize,
    pub server_write_wait_avg_ms: f64,
    pub server_write_wait_max_ms: f64,
    pub server_write_execution_avg_ms: f64,
    pub server_write_execution_max_ms: f64,
    pub server_write_rejected_count: u64,
    pub server_write_timed_out_count: u64,
    pub server_memory_estimate_bytes: u64,
    pub db_lock_acquisitions: u64,
    pub db_lock_wait_avg_ms: f64,
    pub db_lock_wait_max_ms: f64,
    pub db_lock_hold_avg_ms: f64,
    pub db_lock_hold_max_ms: f64,
    pub database_size_bytes: u64,
    pub final_patient_count: Option<usize>,
    pub rss_bytes: Option<u64>,
    pub thread_count: Option<usize>,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServerCertificationBudget {
    pub max_rss_bytes: u64,
    pub max_thread_count: usize,
    pub min_connection_success_rate: f64,
    pub read_min_queries_per_sec: f64,
    pub read_max_p95_ms: f64,
    pub read_max_p99_ms: f64,
    pub mixed_min_queries_per_sec: f64,
    pub mixed_max_p95_ms: f64,
    pub mixed_max_p99_ms: f64,
    pub churn_max_p99_ms: f64,
    pub cancel_short_read_max_p99_ms: f64,
    pub max_rejected_connections: u64,
    pub max_unexpected_failed_queries: u64,
    pub max_timed_out_queries: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServerCertificationScenarioReport {
    pub scenario: &'static str,
    pub passed: bool,
    pub failures: Vec<String>,
    pub report: ServerBenchReport,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServerCertificationReport {
    pub mode: &'static str,
    pub profile: String,
    pub clients: usize,
    pub active_query_concurrency: usize,
    pub queries_per_workload: usize,
    pub idle_soak_ms: u64,
    pub passed: bool,
    pub budget: ServerCertificationBudget,
    pub scenarios: Vec<ServerCertificationScenarioReport>,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresCompatExpectation {
    Works,
    ClearUnsupportedError,
    ExpectedDifference,
    NotYetTested,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresCompatStatus {
    Passed,
    Failed,
    NotRun,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresCompatCoverageState {
    Supported,
    Unsupported,
    ExpectedDifference,
    NotYetTested,
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresCompatCaseReport {
    pub id: String,
    pub category: String,
    pub description: String,
    pub expectation: PostgresCompatExpectation,
    pub status: PostgresCompatStatus,
    pub coverage_state: PostgresCompatCoverageState,
    pub elapsed_ms: f64,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresCompatCategoryScore {
    pub category: String,
    pub executable_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub score_percent: f64,
    pub supported_cases: usize,
    pub unsupported_cases: usize,
    pub expected_difference_cases: usize,
    pub not_yet_tested_cases: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresCompatGap {
    pub category: String,
    pub feature: String,
    pub status: String,
    pub recommendation: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresCompatReport {
    pub mode: &'static str,
    pub target_version: String,
    pub protocol_target: String,
    pub protocol_supported: String,
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub clear_unsupported_cases: usize,
    pub score_percent: f64,
    pub score_interpretation: String,
    pub database_size_bytes: u64,
    pub path: PathBuf,
    pub category_scores: Vec<PostgresCompatCategoryScore>,
    pub cases: Vec<PostgresCompatCaseReport>,
    pub known_gaps: Vec<PostgresCompatGap>,
}

#[derive(Clone, Debug)]
pub struct PostgresDiffConfig {
    pub target_version: String,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_database: String,
    pub pg_user: String,
    pub pg_password: String,
    pub fixtures_dir: Option<PathBuf>,
}

impl Default for PostgresDiffConfig {
    fn default() -> Self {
        Self {
            target_version: "18.4".to_string(),
            pg_host: std::env::var("PG18_BIND_HOST")
                .or_else(|_| std::env::var("PG18_HOST"))
                .unwrap_or_else(|_| "127.0.0.1".to_string()),
            pg_port: std::env::var("PG18_HOST_PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(55432),
            pg_database: std::env::var("PG18_DB").unwrap_or_else(|_| "compat".to_string()),
            pg_user: std::env::var("PG18_USER").unwrap_or_else(|_| "postgres".to_string()),
            pg_password: std::env::var("PG18_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
            fixtures_dir: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresDiffExpectation {
    Match,
    ExpectedDifference,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresDiffStatus {
    Passed,
    Failed,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PostgresDiffFixture {
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub cleanup_sql: Vec<String>,
    pub sql: Vec<String>,
    #[serde(default = "default_diff_expectation")]
    pub expectation: PostgresDiffExpectation,
    #[serde(default)]
    pub expected_difference: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PostgresObservedColumn {
    pub name: String,
    pub type_oid: Option<u32>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PostgresObservedStep {
    pub sql: String,
    pub columns: Vec<PostgresObservedColumn>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: Option<String>,
    pub command_rows: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresDiffCaseReport {
    pub id: String,
    pub description: String,
    pub expectation: PostgresDiffExpectation,
    pub status: PostgresDiffStatus,
    pub elapsed_ms: f64,
    pub detail: String,
    pub metadata_differences: Vec<String>,
    pub expected_difference: Option<String>,
    pub postgres: Vec<PostgresObservedStep>,
    pub bicdb: Vec<PostgresObservedStep>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresDiffReport {
    pub mode: &'static str,
    pub target_version: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub expected_difference_cases: usize,
    pub elapsed_ms: f64,
    pub path: PathBuf,
    pub fixtures_dir: Option<PathBuf>,
    pub cases: Vec<PostgresDiffCaseReport>,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BaselineEngine {
    BicDb,
    Redb,
    Fjall,
    SqliteVec,
    LanceDb,
    Qdrant,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BaselineStatus {
    Completed,
    Skipped,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct BaselineReport {
    pub engine: BaselineEngine,
    pub status: BaselineStatus,
    pub records: usize,
    pub batch_size: usize,
    pub elapsed_ms: Option<f64>,
    pub records_per_sec: Option<f64>,
    pub database_size_bytes: Option<u64>,
    pub message: String,
    pub path: Option<PathBuf>,
}

pub fn default_bench_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("bicdb-{label}-{}", Uuid::new_v4()))
}

pub fn run_insert_bench(
    path: impl AsRef<Path>,
    records: usize,
    batch_size: usize,
) -> Result<InsertBenchReport> {
    let path = path.as_ref().to_path_buf();
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_inserts")?;

    let started = Instant::now();
    let mut batch = Vec::with_capacity(batch_size.max(1));
    for idx in 0..records {
        batch.push(Record::new(format!("record-{idx}")).with_metadata(json!({
            "idx": idx,
            "kind": "insert_bench",
        })));

        if batch.len() >= batch_size.max(1) {
            db.batch_insert("bench_inserts", std::mem::take(&mut batch))?;
        }
    }

    if !batch.is_empty() {
        db.batch_insert("bench_inserts", batch)?;
    }
    db.flush()?;
    let elapsed = started.elapsed();
    let stats = db.stats()?;
    drop(db);

    let recovery_started = Instant::now();
    let recovered = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let startup_recovery_time = recovery_started.elapsed();
    drop(recovered);

    Ok(InsertBenchReport {
        mode: "inserts",
        records,
        batch_size: batch_size.max(1),
        elapsed,
        records_per_sec: throughput(records, elapsed),
        database_size_bytes: stats.size_bytes,
        collection_sizes: collection_sizes(&stats),
        startup_recovery_time,
        path,
    })
}

pub fn run_vector_bench(
    path: impl AsRef<Path>,
    records: usize,
    dim: usize,
    top_k: usize,
    searches: usize,
) -> Result<VectorBenchReport> {
    let path = path.as_ref().to_path_buf();
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_vectors")?;

    let started = Instant::now();
    let mut batch = Vec::with_capacity(1_000);
    for idx in 0..records {
        batch.push(
            Record::new(format!("vector-{idx}"))
                .with_vector(make_vector(idx, dim))
                .with_metadata(json!({
                    "idx": idx,
                    "bucket": idx % 16,
                })),
        );

        if batch.len() >= 1_000 {
            db.batch_insert("bench_vectors", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("bench_vectors", batch)?;
    }
    db.flush()?;
    let insert_elapsed = started.elapsed();

    let query = make_vector(17, dim);
    let mut samples = Vec::with_capacity(searches.max(1));
    for _ in 0..searches.max(1) {
        let started = Instant::now();
        let results = db.search_vector("bench_vectors", &query, top_k.max(1), None)?;
        samples.push(started.elapsed());
        if records > 0 && results.is_empty() {
            return Err(bicdb_core::BicDbError::CollectionNotFound(
                "vector benchmark produced no results".to_string(),
            )
            .into());
        }
    }

    let stats = db.stats()?;
    drop(db);

    let recovery_started = Instant::now();
    let recovered = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let startup_recovery_time = recovery_started.elapsed();
    drop(recovered);

    Ok(VectorBenchReport {
        mode: "vectors",
        records,
        dim,
        top_k: top_k.max(1),
        insert_elapsed,
        search_p50: percentile(samples.clone(), 0.50),
        search_p95: percentile(samples.clone(), 0.95),
        search_p99: percentile(samples, 0.99),
        database_size_bytes: stats.size_bytes,
        collection_sizes: collection_sizes(&stats),
        startup_recovery_time,
        path,
    })
}

pub fn run_vector_profile(
    path: impl AsRef<Path>,
    records: usize,
    dim: usize,
    top_k: usize,
    searches: usize,
    metric: VectorMetric,
) -> Result<VectorProfileReport> {
    run_vector_profile_with_strategy(
        path,
        records,
        dim,
        top_k,
        searches,
        metric,
        VectorProfileStrategy::OptimizedStore,
    )
}

/// Allocation totals for a measured region. Mirrors the subset of
/// `allocation_counter::AllocationInfo` consumed by the vector profile report.
#[derive(Default)]
struct AllocMeasurement {
    count_total: u64,
    bytes_total: u64,
    bytes_max: u64,
}

/// Runs `f`, measuring allocations when the `alloc-counting` feature is enabled.
/// When the feature is off (the default, including the production CLI build) the
/// allocation-counter global allocator is not linked, so this just runs `f` and
/// reports zeros instead of taxing every allocation in the process.
#[cfg(feature = "alloc-counting")]
fn measure_allocations<F: FnOnce()>(f: F) -> AllocMeasurement {
    let info = allocation_counter::measure(f);
    AllocMeasurement {
        count_total: info.count_total,
        bytes_total: info.bytes_total,
        bytes_max: info.bytes_max,
    }
}

#[cfg(not(feature = "alloc-counting"))]
fn measure_allocations<F: FnOnce()>(f: F) -> AllocMeasurement {
    f();
    AllocMeasurement::default()
}

pub fn run_vector_profile_with_strategy(
    path: impl AsRef<Path>,
    records: usize,
    dim: usize,
    top_k: usize,
    searches: usize,
    metric: VectorMetric,
    strategy: VectorProfileStrategy,
) -> Result<VectorProfileReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_vectors")?;

    let insert_started = Instant::now();
    let mut batch = Vec::with_capacity(10_000);
    for idx in 0..records {
        batch.push(
            Record::new(format!("vector-{idx}"))
                .with_vector(make_vector(idx, dim))
                .with_metadata(json!({
                    "idx": idx,
                    "bucket": idx % 16,
                })),
        );

        if batch.len() >= 10_000 {
            db.batch_insert("bench_vectors", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("bench_vectors", batch)?;
    }
    db.flush()?;
    let insert_elapsed = insert_started.elapsed();

    let query = make_vector(17, dim);
    let mut profiles = Vec::with_capacity(searches.max(1));
    let mut result_count = 0;
    let allocation_info = measure_allocations(|| {
        for _ in 0..searches.max(1) {
            let profiled = match strategy {
                VectorProfileStrategy::OptimizedStore => db.profile_vector_search_with_metric(
                    "bench_vectors",
                    &query,
                    top_k.max(1),
                    None,
                    metric,
                ),
                VectorProfileStrategy::RecordScan => db
                    .profile_vector_search_record_scan_with_metric(
                        "bench_vectors",
                        &query,
                        top_k.max(1),
                        None,
                        metric,
                    ),
            }
            .expect("profile vector search");
            result_count = profiled.results.len();
            profiles.push(profiled.profile);
        }
    });

    let totals = profiles
        .iter()
        .map(|profile| profile.total)
        .collect::<Vec<_>>();
    let stats = db.stats()?;
    let searches = searches.max(1);
    let avg = |duration: fn(&bicdb_core::VectorSearchProfile) -> Duration| -> f64 {
        profiles
            .iter()
            .map(|profile| duration(profile).as_secs_f64() * 1_000.0)
            .sum::<f64>()
            / searches as f64
    };

    Ok(VectorProfileReport {
        mode: "vector_profile",
        strategy: strategy_name(strategy),
        records,
        dim: dim.max(1),
        top_k: top_k.max(1),
        searches,
        metric: metric_name(metric),
        insert_elapsed_ms: duration_ms(insert_elapsed),
        total_p50_ms: duration_ms(percentile(totals.clone(), 0.50)),
        total_p95_ms: duration_ms(percentile(totals.clone(), 0.95)),
        total_p99_ms: duration_ms(percentile(totals, 0.99)),
        avg_read_vectors_ms: avg(|profile| profile.read_vectors),
        avg_similarity_ms: avg(|profile| profile.similarity),
        avg_top_k_heap_ms: avg(|profile| profile.top_k_heap),
        avg_final_sort_ms: avg(|profile| profile.final_sort),
        candidates_scanned: profiles
            .first()
            .map(|profile| profile.candidate_records)
            .unwrap_or_default(),
        vectors_read: profiles
            .first()
            .map(|profile| profile.vectors_read)
            .unwrap_or_default(),
        allocation_count_total: allocation_info.count_total,
        allocation_count_per_search: allocation_info.count_total as f64 / searches as f64,
        allocated_bytes_total: allocation_info.bytes_total,
        allocated_bytes_per_search: allocation_info.bytes_total as f64 / searches as f64,
        allocation_bytes_max: allocation_info.bytes_max,
        result_count,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_vector_search_hot(
    path: impl AsRef<Path>,
    records: usize,
    dim: usize,
    top_k: usize,
    searches: usize,
    metric: VectorMetric,
    strategy: VectorProfileStrategy,
) -> Result<VectorSearchHotReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_vectors")?;

    let mut batch = Vec::with_capacity(10_000);
    for idx in 0..records {
        batch.push(
            Record::new(format!("vector-{idx}"))
                .with_vector(make_vector(idx, dim))
                .with_metadata(json!({
                    "idx": idx,
                    "bucket": idx % 16,
                })),
        );

        if batch.len() >= 10_000 {
            db.batch_insert("bench_vectors", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("bench_vectors", batch)?;
    }
    db.flush()?;

    let query = make_vector(17, dim);
    let searches = searches.max(1);
    let top_k = top_k.max(1);
    let mut samples = Vec::with_capacity(searches);
    let mut result_count = 0;
    let elapsed_started = Instant::now();
    for _ in 0..searches {
        let search_started = Instant::now();
        let results = match strategy {
            VectorProfileStrategy::OptimizedStore => {
                db.search_vector_with_metric("bench_vectors", &query, top_k, None, metric)
            }
            VectorProfileStrategy::RecordScan => db.search_vector_record_scan_with_metric(
                "bench_vectors",
                &query,
                top_k,
                None,
                metric,
            ),
        }?;
        samples.push(search_started.elapsed());
        result_count = results.len();
        black_box(result_count);
    }
    let elapsed = elapsed_started.elapsed();

    Ok(VectorSearchHotReport {
        mode: "vector_search_hot",
        strategy: strategy_name(strategy),
        records,
        dim: dim.max(1),
        top_k,
        searches,
        metric: metric_name(metric),
        search_p50_ms: duration_ms(percentile(samples.clone(), 0.50)),
        search_p95_ms: duration_ms(percentile(samples.clone(), 0.95)),
        search_p99_ms: duration_ms(percentile(samples, 0.99)),
        elapsed_ms: duration_ms(elapsed),
        result_count,
        path,
    })
}

pub fn run_ann_bench(
    path: impl AsRef<Path>,
    records: usize,
    dim: usize,
    top_k: usize,
) -> Result<AnnBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_ann")?;

    let records = records.max(1);
    let dim = dim.max(1);
    let top_k = top_k.max(1);
    let started = Instant::now();
    let mut batch = Vec::with_capacity(5_000);
    for idx in 0..records {
        batch.push(
            Record::new(format!("vector-{idx}"))
                .with_vector(make_vector(idx, dim))
                .with_metadata(json!({"idx": idx})),
        );
        if batch.len() >= 5_000 {
            db.batch_insert("bench_ann", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("bench_ann", batch)?;
    }
    let insert_elapsed = started.elapsed();

    let build_started = Instant::now();
    db.create_vector_index(
        "bench_ann",
        HnswIndexConfig {
            m: 16,
            ef_construction: 100,
            ef_search: 20,
            distance: VectorMetric::Cosine,
        },
    )?;
    let index_build = build_started.elapsed();

    let searches = 5usize.min(records).max(1);
    let queries = (0..searches)
        .map(|idx| make_vector(idx * 17 + 3, dim))
        .collect::<Vec<_>>();

    let mut exact_samples = Vec::with_capacity(searches);
    let mut exact_results = Vec::with_capacity(searches);
    for query in &queries {
        let started = Instant::now();
        let results = db.search_vector_exact("bench_ann", query, top_k)?;
        exact_samples.push(started.elapsed());
        exact_results.push(results);
    }

    let (ann_ef20, recall_ef20) = ann_latency_and_recall(&db, &queries, &exact_results, top_k, 20)?;
    let (ann_ef50, recall_ef50) = ann_latency_and_recall(&db, &queries, &exact_results, top_k, 50)?;
    let (ann_ef100, recall_ef100) =
        ann_latency_and_recall(&db, &queries, &exact_results, top_k, 100)?;
    let stats = db.stats()?;

    Ok(AnnBenchReport {
        mode: "ann",
        records,
        dim,
        top_k,
        searches,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        index_build_ms: duration_ms(index_build),
        index_size_bytes: db.vector_index_size_bytes("bench_ann")?,
        memory_estimate_bytes: db.vector_index_memory_estimate_bytes("bench_ann")?,
        exact_p50_ms: duration_ms(percentile(exact_samples.clone(), 0.50)),
        exact_p95_ms: duration_ms(percentile(exact_samples.clone(), 0.95)),
        exact_p99_ms: duration_ms(percentile(exact_samples, 0.99)),
        ann_ef20_p50_ms: duration_ms(percentile(ann_ef20.clone(), 0.50)),
        ann_ef20_p95_ms: duration_ms(percentile(ann_ef20.clone(), 0.95)),
        ann_ef20_p99_ms: duration_ms(percentile(ann_ef20, 0.99)),
        ann_ef20_recall_at_k: recall_ef20,
        ann_ef50_p50_ms: duration_ms(percentile(ann_ef50.clone(), 0.50)),
        ann_ef50_p95_ms: duration_ms(percentile(ann_ef50.clone(), 0.95)),
        ann_ef50_p99_ms: duration_ms(percentile(ann_ef50, 0.99)),
        ann_ef50_recall_at_k: recall_ef50,
        ann_ef100_p50_ms: duration_ms(percentile(ann_ef100.clone(), 0.50)),
        ann_ef100_p95_ms: duration_ms(percentile(ann_ef100.clone(), 0.95)),
        ann_ef100_p99_ms: duration_ms(percentile(ann_ef100, 0.99)),
        ann_ef100_recall_at_k: recall_ef100,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

fn ann_latency_and_recall(
    db: &BicDb,
    queries: &[Vec<f32>],
    exact_results: &[Vec<bicdb_core::VectorSearchResult>],
    top_k: usize,
    ef_search: usize,
) -> Result<(Vec<Duration>, f64)> {
    let mut samples = Vec::with_capacity(queries.len());
    let mut recall_sum = 0.0;
    for (query, exact) in queries.iter().zip(exact_results.iter()) {
        let started = Instant::now();
        let ann = db.search_vector_ann("bench_ann", query, top_k, ef_search)?;
        samples.push(started.elapsed());
        recall_sum += recall_at_k(&ann, exact);
    }
    Ok((samples, recall_sum / queries.len().max(1) as f64))
}

fn recall_at_k(
    ann: &[bicdb_core::VectorSearchResult],
    exact: &[bicdb_core::VectorSearchResult],
) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let hits = ann
        .iter()
        .filter(|ann| exact.iter().any(|exact| exact.record.id == ann.record.id))
        .count();
    hits as f64 / exact.len() as f64
}

pub fn run_graph_bench(
    path: impl AsRef<Path>,
    entities: usize,
    edges: usize,
) -> Result<GraphBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("entities")?;
    db.create_collection("groups")?;
    db.create_collection("links")?;

    let entities = entities.max(1);
    let edges = edges.max(1);
    let groups = entities.clamp(1, 10_000);
    let insert_started = Instant::now();

    let entity_records = (0..entities).map(|idx| {
        Record::new(format!("e{idx}")).with_metadata(json!({
            "name": format!("Entity {idx}"),
            "partition": format!("partition-{}", idx % 32),
        }))
    });
    db.batch_insert("entities", entity_records)?;

    let group_records = (0..groups).map(|idx| {
        Record::new(format!("g{idx}")).with_metadata(json!({
            "name": format!("Group {idx}"),
            "class": format!("class-{}", idx % 16),
        }))
    });
    db.batch_insert("groups", group_records)?;

    let mut batch = Vec::with_capacity(5_000);
    for idx in 0..edges {
        batch.push(
            Record::new(format!("l{idx}"))
                .with_metadata(json!({
                    "entity_id": format!("e{}", idx % entities),
                    "group_id": format!("g{}", idx % groups),
                    "kind": "member",
                }))
                .with_timestamp(1_710_000_000 + idx as i64),
        );
        if batch.len() >= 5_000 {
            db.batch_insert("links", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("links", batch)?;
    }
    let insert_elapsed = insert_started.elapsed();

    let projection = GraphProjection::new("bench_graph")
        .nodes_from("entities", "Entity")
        .nodes_from("groups", "Group")
        .edge_from_field("links", "entity_id", "group_id", "MEMBER_OF");

    let build_started = Instant::now();
    let graph = db.build_graph_projection(projection)?;
    let build_elapsed = build_started.elapsed();

    let neighbor_started = Instant::now();
    let neighbor_count = graph.neighbors("Entity:e0").len();
    black_box(neighbor_count);
    let neighbor_lookup = neighbor_started.elapsed();

    let path_started = Instant::now();
    let graph_path = graph.path("Entity:e0", "Group:g0", 3);
    black_box(graph_path);
    let path_query = path_started.elapsed();

    let edge_scan_started = Instant::now();
    let edge_count = graph
        .edges
        .values()
        .filter(|edge| edge.label == "MEMBER_OF")
        .count();
    black_box(edge_count);
    let edge_scan = edge_scan_started.elapsed();
    let stats = db.stats()?;

    Ok(GraphBenchReport {
        mode: "graph",
        entities,
        edges,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        build_elapsed_ms: duration_ms(build_elapsed),
        neighbor_lookup_ms: duration_ms(neighbor_lookup),
        path_query_ms: duration_ms(path_query),
        edge_scan_ms: duration_ms(edge_scan),
        node_count: graph.nodes.len(),
        edge_count: graph.edges.len(),
        graph_size_bytes: graph.graph_size_bytes(),
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_spatial_bench(path: impl AsRef<Path>, points: usize) -> Result<SpatialBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_spatial")?;

    let points = points.max(1);
    let insert_started = Instant::now();
    insert_spatial_points(&mut db, "bench_spatial", points)?;
    db.flush()?;
    let insert_elapsed = insert_started.elapsed();

    let index_started = Instant::now();
    db.create_spatial_index("bench_spatial", "geometry")?;
    let index_build = index_started.elapsed();
    let index_size_bytes = db.index_stats()?.size_bytes;
    let stats = db.stats()?;

    Ok(SpatialBenchReport {
        mode: "spatial",
        points,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        points_per_sec: throughput(points, insert_elapsed),
        index_build_ms: duration_ms(index_build),
        index_size_bytes,
        database_size_bytes: stats.size_bytes,
        collection_sizes: collection_sizes(&stats),
        path,
    })
}

pub fn run_spatial_nearest_bench(
    path: impl AsRef<Path>,
    points: usize,
    queries: usize,
) -> Result<SpatialNearestBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("bench_spatial")?;

    let points = points.max(1);
    let queries = queries.max(1);
    insert_spatial_points(&mut db, "bench_spatial", points)?;
    db.create_spatial_index("bench_spatial", "geometry")?;

    let radius_meters = 2_000.0;
    let mut nearest_samples = Vec::with_capacity(queries);
    let mut radius_samples = Vec::with_capacity(queries);
    let mut nearest_result_count = 0;
    let mut radius_result_count = 0;

    for idx in 0..queries {
        let (lon, lat) = spatial_query_point(idx, points);

        let started = Instant::now();
        let nearest = db.nearest("bench_spatial", "geometry", lon, lat, 10)?;
        nearest_samples.push(started.elapsed());
        nearest_result_count += nearest.len();
        black_box(nearest);

        let started = Instant::now();
        let radius = db.within_radius("bench_spatial", "geometry", lon, lat, radius_meters)?;
        radius_samples.push(started.elapsed());
        radius_result_count += radius.len();
        black_box(radius);
    }

    let stats = db.stats()?;
    Ok(SpatialNearestBenchReport {
        mode: "spatial_nearest",
        points,
        queries,
        nearest_p50_ms: duration_ms(percentile(nearest_samples.clone(), 0.50)),
        nearest_p95_ms: duration_ms(percentile(nearest_samples.clone(), 0.95)),
        nearest_p99_ms: duration_ms(percentile(nearest_samples, 0.99)),
        radius_p50_ms: duration_ms(percentile(radius_samples.clone(), 0.50)),
        radius_p95_ms: duration_ms(percentile(radius_samples.clone(), 0.95)),
        radius_p99_ms: duration_ms(percentile(radius_samples, 0.99)),
        nearest_result_count,
        radius_result_count,
        radius_meters,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_route_bench(
    path: impl AsRef<Path>,
    nodes: usize,
    edges: usize,
) -> Result<RouteBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("roads_nodes")?;
    db.create_collection("roads_edges")?;

    let nodes = nodes.max(2);
    let edges = edges.max(nodes - 1);
    let insert_started = Instant::now();
    insert_route_graph(&mut db, nodes, edges)?;
    db.flush()?;
    let insert_elapsed = insert_started.elapsed();

    let queries = nodes.clamp(10, 100);
    let mut samples = Vec::with_capacity(queries);
    let mut route_result_count = 0;
    for idx in 0..queries {
        let start_idx = (idx * 37) % nodes;
        let mut end_idx = nodes - 1 - ((idx * 53) % nodes);
        if start_idx == end_idx {
            end_idx = (end_idx + 1) % nodes;
        }
        let start = route_node_geometry(start_idx);
        let end = route_node_geometry(end_idx);
        let started = Instant::now();
        let route = db.shortest_path("roads", &start, &end)?;
        samples.push(started.elapsed());
        route_result_count += route.node_ids.len();
        black_box(route);
    }

    let stats = db.stats()?;
    Ok(RouteBenchReport {
        mode: "route",
        nodes,
        edges,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        route_p50_ms: duration_ms(percentile(samples.clone(), 0.50)),
        route_p95_ms: duration_ms(percentile(samples.clone(), 0.95)),
        route_p99_ms: duration_ms(percentile(samples, 0.99)),
        route_result_count,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_event_bench(path: impl AsRef<Path>, events: usize) -> Result<EventBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let events = events.max(1);

    let latency_started = Arc::new(Mutex::new(Instant::now()));
    let (latency_tx, latency_rx) = mpsc::channel();
    let latency_started_for_handler = Arc::clone(&latency_started);
    db.events_mut().subscribe("bench-events", move |_| {
        let elapsed = latency_started_for_handler.lock().unwrap().elapsed();
        let _ = latency_tx.send(elapsed);
    });

    let append_started = Instant::now();
    let mut subscriber_samples = Vec::with_capacity(events);
    for idx in 0..events {
        *latency_started.lock().unwrap() = Instant::now();
        db.events_mut().append(bench_event(idx))?;
        subscriber_samples.push(latency_rx.recv().context("subscriber latency sample")?);
    }
    db.flush()?;
    let append_elapsed = append_started.elapsed();

    let replay_started = Instant::now();
    let replayed_events = db.events().replay("bench-events").len();
    let replay_elapsed = replay_started.elapsed();
    let database_size_bytes = db.stats()?.size_bytes;

    Ok(EventBenchReport {
        mode: "events",
        events,
        append_elapsed_ms: duration_ms(append_elapsed),
        append_events_per_sec: throughput(events, append_elapsed),
        subscriber_p50_ms: duration_ms(percentile(subscriber_samples.clone(), 0.50)),
        subscriber_p95_ms: duration_ms(percentile(subscriber_samples.clone(), 0.95)),
        subscriber_p99_ms: duration_ms(percentile(subscriber_samples, 0.99)),
        replay_elapsed_ms: duration_ms(replay_elapsed),
        replay_events_per_sec: throughput(replayed_events, replay_elapsed),
        replayed_events,
        database_size_bytes,
        path,
    })
}

pub fn run_queue_bench(
    path: impl AsRef<Path>,
    messages: usize,
    consume_batch_size: usize,
) -> Result<QueueBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let messages = messages.max(1);
    let consume_batch_size = consume_batch_size.max(1);

    let publish_started = Instant::now();
    for idx in 0..messages {
        db.queue("bench-queue").publish(
            json!({
                "message_id": format!("msg-{idx}"),
                "kind": "QueueMessage",
                "idx": idx,
            }),
            json!({"source": "bench"}),
        )?;
    }
    db.flush()?;
    let publish_elapsed = publish_started.elapsed();

    let consume_started = Instant::now();
    let mut consumed_messages = 0;
    while consumed_messages < messages {
        let batch = db.queue("bench-queue").consume(consume_batch_size)?;
        if batch.is_empty() {
            break;
        }
        consumed_messages += batch.len();
        black_box(consumed_messages);
    }
    let consume_elapsed = consume_started.elapsed();
    let database_size_bytes = db.stats()?.size_bytes;

    Ok(QueueBenchReport {
        mode: "queue",
        messages,
        consume_batch_size,
        publish_elapsed_ms: duration_ms(publish_elapsed),
        publish_messages_per_sec: throughput(messages, publish_elapsed),
        consume_elapsed_ms: duration_ms(consume_elapsed),
        consume_messages_per_sec: throughput(consumed_messages, consume_elapsed),
        consumed_messages,
        database_size_bytes,
        path,
    })
}

#[derive(Clone, Debug, Default)]
struct BenchmarkEntityView {
    entities: BTreeMap<String, serde_json::Value>,
}

impl EventProjection for BenchmarkEntityView {
    fn apply(&mut self, event: &StoredEvent) -> bicdb_core::Result<()> {
        if !matches!(
            event.event.event_type.as_str(),
            "EntityCreated" | "EntityUpdated"
        ) {
            return Ok(());
        }
        let id = event.event.payload["entity_id"]
            .as_str()
            .ok_or_else(|| BicDbError::ProjectionError("entity event has no entity_id".into()))?
            .to_string();
        let entry = self.entities.entry(id).or_insert_with(|| json!({}));
        let object = entry.as_object_mut().ok_or_else(|| {
            BicDbError::ProjectionError("entity projection state is not an object".into())
        })?;
        let patch = event.event.payload.as_object().ok_or_else(|| {
            BicDbError::ProjectionError("entity event payload is not an object".into())
        })?;
        object.extend(patch.clone());
        Ok(())
    }
}

pub fn run_projection_bench(
    path: impl AsRef<Path>,
    entities: usize,
    updates_per_entity: usize,
) -> Result<ProjectionBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let entities = entities.max(1);
    let updates_per_entity = updates_per_entity.max(1);

    let append_started = Instant::now();
    for entity_idx in 0..entities {
        let entity_id = format!("entity-{entity_idx}");
        db.events_mut().append(
            Event::new(
                "entity-events",
                "EntityCreated",
                json!({
                    "entity_id": entity_id,
                    "name": format!("Entity {entity_idx}"),
                    "partition": format!("partition-{}", entity_idx % 64),
                }),
            )
            .with_timestamp(1_710_000_000 + entity_idx as i64),
        )?;

        for update_idx in 0..updates_per_entity {
            db.events_mut().append(
                Event::new(
                    "entity-events",
                    "EntityUpdated",
                    json!({
                        "entity_id": format!("entity-{entity_idx}"),
                        "update_count": update_idx + 1,
                        "score": ((entity_idx + update_idx) % 100) as f64 / 100.0,
                    }),
                )
                .with_timestamp(1_710_100_000 + update_idx as i64),
            )?;
        }
    }
    db.flush()?;
    let append_elapsed = append_started.elapsed();
    let events = entities.saturating_mul(updates_per_entity.saturating_add(1));

    let rebuild_started = Instant::now();
    let view: BenchmarkEntityView = db.events().rebuild_projection("entity-events")?;
    let rebuild_elapsed = rebuild_started.elapsed();
    let projected_entities = view.entities.len();
    let database_size_bytes = db.stats()?.size_bytes;

    Ok(ProjectionBenchReport {
        mode: "projections",
        events,
        entities,
        append_elapsed_ms: duration_ms(append_elapsed),
        append_events_per_sec: throughput(events, append_elapsed),
        rebuild_elapsed_ms: duration_ms(rebuild_elapsed),
        rebuild_events_per_sec: throughput(events, rebuild_elapsed),
        projected_entities,
        database_size_bytes,
        path,
    })
}

pub fn run_wearable_bench(
    path: impl AsRef<Path>,
    devices: usize,
    records: usize,
) -> Result<WearableBenchReport> {
    let path = path.as_ref().to_path_buf();
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_timeseries_collection("wearable")?;

    let devices = devices.max(1);
    let started = Instant::now();
    let mut batch = Vec::with_capacity(2_000);
    for idx in 0..records {
        let device = idx % devices;
        let metric = match idx % 3 {
            0 => "hrv",
            1 => "heart_rate",
            _ => "steps",
        };
        batch.push(
            Record::new(format!("wearable-{idx}"))
                .with_timestamp(1_710_000_000 + idx as i64)
                .with_metadata(json!({
                    "device_id": format!("band-{device}"),
                    "user_id": format!("user-{}", device % 100),
                    "metric": metric,
                    "value": ((idx % 200) as f64) + 0.25,
                })),
        );

        if batch.len() >= 2_000 {
            db.batch_insert("wearable", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("wearable", batch)?;
    }
    db.flush()?;
    let insert_elapsed = started.elapsed();

    let range_started = Instant::now();
    let scanned = db.scan_time_range("wearable", 1_710_000_000, 1_710_000_000 + 10_000)?;
    let time_range_scan_elapsed = range_started.elapsed();

    let latest_started = Instant::now();
    let _latest = db.latest_value_per_device("wearable", "band-0", "hrv")?;
    let latest_lookup_elapsed = latest_started.elapsed();

    let summary = db.time_series_summary(
        "wearable",
        &TimeSeriesFilter::new(1_710_000_000, 1_710_000_000 + records as i64).metric("hrv"),
    )?;

    let stats = db.stats()?;
    drop(db);

    let recovery_started = Instant::now();
    let recovered = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let startup_recovery_time = recovery_started.elapsed();
    drop(recovered);

    Ok(WearableBenchReport {
        mode: "wearable",
        devices,
        records,
        insert_elapsed,
        records_per_sec: throughput(records, insert_elapsed),
        time_range_scan_elapsed,
        scanned_records: scanned.len(),
        latest_lookup_elapsed,
        summary_count: summary.count,
        database_size_bytes: stats.size_bytes,
        collection_sizes: collection_sizes(&stats),
        startup_recovery_time,
        path,
    })
}

pub fn run_sql_bench(path: impl AsRef<Path>, records: usize) -> Result<SqlBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_timeseries_collection("wearable")?;

    let records = records.max(1);
    let started = Instant::now();
    let mut batch = Vec::with_capacity(2_000);
    for idx in 0..records {
        let metric = if idx % 2 == 0 { "hrv" } else { "steps" };
        batch.push(
            Record::new(format!("sql-record-{idx}"))
                .with_timestamp(1_710_000_000 + idx as i64)
                .with_metadata(json!({
                    "metric": metric,
                    "clinic": format!("clinic-{}", idx % 32),
                    "value": (idx % 200) as f64 + 0.25,
                })),
        );

        if batch.len() >= 2_000 {
            db.batch_insert("wearable", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("wearable", batch)?;
    }
    db.flush()?;
    let insert_elapsed = started.elapsed();

    let engine = SqlEngine::new(&db);
    let target_id = format!("sql-record-{}", records / 2);
    let range_start = 1_710_000_000 + (records / 4) as i64;
    let range_end = (range_start + 1_000).min(1_710_000_000 + records as i64 - 1);

    let sql_started = Instant::now();
    let select_by_id = engine.execute(&format!(
        "SELECT * FROM wearable WHERE id = '{target_id}' LIMIT 1"
    ))?;
    black_box(&select_by_id);
    let select_by_id_sql = sql_started.elapsed();

    let direct_started = Instant::now();
    let direct_id = db.get("wearable", &target_id)?;
    black_box(&direct_id);
    let select_by_id_direct = direct_started.elapsed();

    let sql_started = Instant::now();
    let timestamp_range = engine.execute(&format!(
        "SELECT id FROM wearable WHERE timestamp >= {range_start} AND timestamp <= {range_end}"
    ))?;
    black_box(&timestamp_range);
    let timestamp_range_sql = sql_started.elapsed();

    let direct_started = Instant::now();
    let direct_range = db.scan_time_range("wearable", range_start, range_end)?;
    black_box(&direct_range);
    let timestamp_range_direct = direct_started.elapsed();

    let sql_started = Instant::now();
    let count = engine.execute("SELECT COUNT(*) FROM wearable")?;
    black_box(&count);
    let count_sql = sql_started.elapsed();

    let direct_started = Instant::now();
    let direct_count = db.scan_collection("wearable")?.len();
    black_box(direct_count);
    let count_direct = direct_started.elapsed();

    let sql_started = Instant::now();
    let avg =
        engine.execute("SELECT AVG(metadata.value) FROM wearable WHERE metadata.metric = 'hrv'")?;
    black_box(&avg);
    let avg_sql = sql_started.elapsed();

    let direct_started = Instant::now();
    let direct_avg = db.time_series_summary(
        "wearable",
        &TimeSeriesFilter::new(1_710_000_000, 1_710_000_000 + records as i64).metric("hrv"),
    )?;
    black_box(&direct_avg);
    let avg_direct = direct_started.elapsed();

    let sql_started = Instant::now();
    let order_by_limit =
        engine.execute("SELECT id, timestamp FROM wearable ORDER BY timestamp DESC LIMIT 10")?;
    black_box(&order_by_limit);
    let order_by_limit_sql = sql_started.elapsed();

    let direct_started = Instant::now();
    let mut direct_order = db.scan_collection("wearable")?;
    direct_order.sort_by_key(|record| std::cmp::Reverse(record.timestamp));
    direct_order.truncate(10);
    black_box(&direct_order);
    let order_by_limit_direct = direct_started.elapsed();

    let stats = db.stats()?;
    Ok(SqlBenchReport {
        mode: "sql",
        records,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        select_by_id_sql_ms: duration_ms(select_by_id_sql),
        select_by_id_direct_ms: duration_ms(select_by_id_direct),
        timestamp_range_sql_ms: duration_ms(timestamp_range_sql),
        timestamp_range_direct_ms: duration_ms(timestamp_range_direct),
        timestamp_range_rows: timestamp_range.rows.len(),
        count_sql_ms: duration_ms(count_sql),
        count_direct_ms: duration_ms(count_direct),
        count_rows: count_value(&count).unwrap_or(direct_count),
        avg_sql_ms: duration_ms(avg_sql),
        avg_direct_ms: duration_ms(avg_direct),
        avg_value: avg_value(&avg).or(direct_avg.avg).unwrap_or(0.0),
        order_by_limit_sql_ms: duration_ms(order_by_limit_sql),
        order_by_limit_direct_ms: duration_ms(order_by_limit_direct),
        order_by_limit_rows: order_by_limit.rows.len(),
        database_size_bytes: stats.size_bytes,
        collection_sizes: collection_sizes(&stats),
        path,
    })
}

pub fn run_index_bench(path: impl AsRef<Path>, records: usize) -> Result<IndexBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_timeseries_collection("wearable")?;
    db.create_timeseries_collection("wearable_indexed_write")?;

    let records = records.max(1);
    let insert_started = Instant::now();
    insert_index_bench_records(&mut db, "wearable", records, "scan")?;
    db.flush()?;
    let insert_without_indexes = insert_started.elapsed();

    let target_device = format!("band-{}", records.min(256) / 2);
    let target_clinic = format!("clinic-{}", records.min(64) / 2);
    let range_start = 1_710_000_000 + (records / 3) as i64;
    let range_end = (range_start + 10_000).min(1_710_000_000 + records as i64 - 1);

    let point_sql = format!("SELECT id FROM wearable WHERE metadata.device_id = '{target_device}'");
    let timestamp_sql = format!(
        "SELECT id FROM wearable WHERE timestamp >= {range_start} AND timestamp <= {range_end}"
    );
    let metadata_sql = format!("SELECT id FROM wearable WHERE metadata.clinic = '{target_clinic}'");
    let composite_sql = format!(
        "SELECT id FROM wearable WHERE metadata.device_id = '{target_device}' AND metadata.metric = 'hrv'"
    );
    let order_sql = "SELECT id, timestamp FROM wearable ORDER BY timestamp DESC LIMIT 10";

    let engine = SqlEngine::new(&db);
    let (point_lookup_scan, point_scan_result) = timed_sql(&engine, &point_sql)?;
    let (timestamp_range_scan, timestamp_scan_result) = timed_sql(&engine, &timestamp_sql)?;
    let (metadata_equality_scan, metadata_scan_result) = timed_sql(&engine, &metadata_sql)?;
    let (composite_lookup_scan, composite_scan_result) = timed_sql(&engine, &composite_sql)?;
    let (order_by_scan, order_scan_result) = timed_sql(&engine, order_sql)?;

    let build_started = Instant::now();
    create_index_bench_indexes(&mut db, "wearable", "idx_bench")?;
    let index_build = build_started.elapsed();
    let index_size_bytes = db.index_stats()?.size_bytes;
    let rebuild_started = Instant::now();
    let rebuild_report = db.rebuild_all_indexes_online()?;
    let index_rebuild = rebuild_started.elapsed();
    let peak_memory_estimate_bytes = rebuild_report
        .reports
        .iter()
        .map(|report| report.size_bytes)
        .sum::<u64>()
        .max(index_size_bytes);

    let engine = SqlEngine::new(&db);
    let (point_lookup_index, point_index_result) = timed_sql(&engine, &point_sql)?;
    let (timestamp_range_index, timestamp_index_result) = timed_sql(&engine, &timestamp_sql)?;
    let (metadata_equality_index, metadata_index_result) = timed_sql(&engine, &metadata_sql)?;
    let (composite_lookup_index, composite_index_result) = timed_sql(&engine, &composite_sql)?;
    let (order_by_index, order_index_result) = timed_sql(&engine, order_sql)?;

    black_box(&point_scan_result);
    black_box(&timestamp_scan_result);
    black_box(&metadata_scan_result);
    black_box(&composite_scan_result);
    black_box(&order_scan_result);
    black_box(&point_index_result);
    black_box(&timestamp_index_result);
    black_box(&metadata_index_result);
    black_box(&composite_index_result);
    black_box(&order_index_result);

    create_index_bench_indexes(&mut db, "wearable_indexed_write", "idx_bench_write")?;
    let indexed_insert_started = Instant::now();
    insert_index_bench_records(&mut db, "wearable_indexed_write", records, "indexed")?;
    db.flush()?;
    let insert_with_indexes = indexed_insert_started.elapsed();

    let stats = db.stats()?;
    Ok(IndexBenchReport {
        mode: "indexes",
        records,
        insert_without_indexes_ms: duration_ms(insert_without_indexes),
        insert_with_indexes_ms: duration_ms(insert_with_indexes),
        write_overhead_ms: duration_ms(insert_with_indexes) - duration_ms(insert_without_indexes),
        index_build_ms: duration_ms(index_build),
        index_rebuild_ms: duration_ms(index_rebuild),
        index_size_bytes,
        peak_memory_estimate_bytes,
        point_lookup_scan_ms: duration_ms(point_lookup_scan),
        point_lookup_index_ms: duration_ms(point_lookup_index),
        point_lookup_rows: point_index_result.rows.len(),
        timestamp_range_scan_ms: duration_ms(timestamp_range_scan),
        timestamp_range_index_ms: duration_ms(timestamp_range_index),
        timestamp_range_rows: timestamp_index_result.rows.len(),
        metadata_equality_scan_ms: duration_ms(metadata_equality_scan),
        metadata_equality_index_ms: duration_ms(metadata_equality_index),
        metadata_equality_rows: metadata_index_result.rows.len(),
        composite_lookup_scan_ms: duration_ms(composite_lookup_scan),
        composite_lookup_index_ms: duration_ms(composite_lookup_index),
        composite_lookup_rows: composite_index_result.rows.len(),
        order_by_scan_ms: duration_ms(order_by_scan),
        order_by_index_ms: duration_ms(order_by_index),
        order_by_rows: order_index_result.rows.len(),
        database_size_bytes: stats.size_bytes,
        collection_sizes: collection_sizes(&stats),
        path,
    })
}

pub fn run_transaction_bench(
    path: impl AsRef<Path>,
    records: usize,
    batch_size: usize,
) -> Result<TransactionBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_collection("tx_single")?;
    db.create_collection("tx_batch")?;
    db.create_collection("tx_rollback")?;

    let records = records.max(1);
    let batch_size = batch_size.max(1);

    let single_started = Instant::now();
    for idx in 0..records {
        let mut tx = db.begin_transaction()?;
        tx.insert(
            "tx_single",
            Record::new(format!("single-{idx}")).with_metadata(json!({"idx": idx})),
        )?;
        tx.commit()?;
    }
    let single_elapsed = single_started.elapsed();

    let batch_started = Instant::now();
    let mut idx = 0;
    while idx < records {
        let mut tx = db.begin_transaction()?;
        let end = (idx + batch_size).min(records);
        while idx < end {
            tx.insert(
                "tx_batch",
                Record::new(format!("batch-{idx}")).with_metadata(json!({"idx": idx})),
            )?;
            idx += 1;
        }
        tx.commit()?;
    }
    let batch_elapsed = batch_started.elapsed();

    let rollback_started = Instant::now();
    let mut rolled_back = 0usize;
    while rolled_back < records {
        let mut tx = db.begin_transaction()?;
        let end = (rolled_back + batch_size).min(records);
        while rolled_back < end {
            tx.insert(
                "tx_rollback",
                Record::new(format!("rollback-{rolled_back}"))
                    .with_metadata(json!({"idx": rolled_back})),
            )?;
            rolled_back += 1;
        }
        tx.rollback()?;
    }
    let rollback_elapsed = rollback_started.elapsed();

    let snapshot_started = Instant::now();
    let snapshot = db.snapshot()?;
    let snapshot_records = snapshot.scan("tx_batch").len();
    black_box(snapshot_records);
    let snapshot_elapsed = snapshot_started.elapsed();

    db.flush()?;
    drop(db);

    let recovery_started = Instant::now();
    let recovered = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let recovery_elapsed = recovery_started.elapsed();
    let recovered_records = recovered.scan_collection("tx_batch")?.len();
    let stats = recovered.stats()?;

    Ok(TransactionBenchReport {
        mode: "transactions",
        records,
        batch_size,
        single_insert_tx_ms: duration_ms(single_elapsed),
        single_insert_tx_per_sec: throughput(records, single_elapsed),
        batch_insert_tx_ms: duration_ms(batch_elapsed),
        batch_insert_tx_per_sec: throughput(records, batch_elapsed),
        rollback_ms: duration_ms(rollback_elapsed),
        rollback_records_per_sec: throughput(records, rollback_elapsed),
        recovery_ms: duration_ms(recovery_elapsed),
        recovered_records,
        snapshot_scan_ms: duration_ms(snapshot_elapsed),
        snapshot_records,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_analytics_bench(path: impl AsRef<Path>, records: usize) -> Result<AnalyticsBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    db.create_timeseries_collection("wearable")?;

    let records = records.max(1);
    let insert_started = Instant::now();
    let mut batch = Vec::with_capacity(5_000);
    for idx in 0..records {
        let metric = match idx % 3 {
            0 => "hrv",
            1 => "heart_rate",
            _ => "steps",
        };
        batch.push(
            Record::new(format!("analytics-record-{idx}"))
                .with_timestamp(1_710_000_000 + idx as i64)
                .with_metadata(json!({
                    "device_id": format!("band-{}", idx % 256),
                    "user_id": format!("user-{}", idx % 10_000),
                    "metric": metric,
                    "value": (idx % 200) as f64 + 0.25,
                })),
        );
        if batch.len() >= 5_000 {
            db.batch_insert("wearable", std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert("wearable", batch)?;
    }
    db.flush()?;
    let insert_elapsed = insert_started.elapsed();

    let sidecar_started = Instant::now();
    let sidecar = rebuild_sidecar(&db, &path, "wearable")?;
    let sidecar_rebuild = sidecar_started.elapsed();

    let arrow_batch = bicdb_analytics::BicDbAnalyticsExt::to_record_batch(&db, "wearable")?;
    let arrow_memory_bytes = arrow_batch.get_array_memory_size();

    let direct_started = Instant::now();
    let scanned = db.scan_collection("wearable")?;
    let direct_count = scanned.len();
    black_box(direct_count);
    let direct_count_elapsed = direct_started.elapsed();

    let query_exec_started = Instant::now();
    let query_exec_all = db.time_series_summary(
        "wearable",
        &TimeSeriesFilter::new(1_710_000_000, 1_710_000_000 + records as i64),
    )?;
    black_box(&query_exec_all);
    let query_exec_count_elapsed = query_exec_started.elapsed();

    let direct_started = Instant::now();
    let direct_avg = direct_avg(&scanned, "hrv");
    black_box(direct_avg);
    let direct_avg_elapsed = direct_started.elapsed();

    let query_exec_started = Instant::now();
    let query_exec_avg = db.time_series_summary(
        "wearable",
        &TimeSeriesFilter::new(1_710_000_000, 1_710_000_000 + records as i64).metric("hrv"),
    )?;
    black_box(&query_exec_avg);
    let query_exec_avg_elapsed = query_exec_started.elapsed();

    let range_start = 1_710_000_000 + (records / 2) as i64;
    let direct_started = Instant::now();
    let direct_min_max = direct_min_max(&scanned, range_start);
    black_box(direct_min_max);
    let direct_min_max_elapsed = direct_started.elapsed();

    let query_exec_started = Instant::now();
    let query_exec_min_max = db.time_series_summary(
        "wearable",
        &TimeSeriesFilter::new(range_start, 1_710_000_000 + records as i64),
    )?;
    black_box(&query_exec_min_max);
    let query_exec_min_max_elapsed = query_exec_started.elapsed();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let ctx = BicDataFusionContext::new(&db);
    let samples = runtime.block_on(async {
        Ok::<_, anyhow::Error>(AnalyticsSamples {
            count: sample_datafusion(
                &ctx,
                "SELECT COUNT(*) FROM wearable",
                analytics_samples(records),
            )
            .await?,
            avg: sample_datafusion(
                &ctx,
                "SELECT AVG(value) FROM wearable WHERE metric = 'hrv'",
                analytics_samples(records),
            )
            .await?,
            min_max: sample_datafusion(
                &ctx,
                &format!("SELECT MIN(value), MAX(value) FROM wearable WHERE timestamp >= {range_start}"),
                analytics_samples(records),
            )
            .await?,
            group_by_metric: sample_datafusion(
                &ctx,
                "SELECT metric, AVG(value) FROM wearable GROUP BY metric",
                analytics_samples(records),
            )
            .await?,
            group_by_device: sample_datafusion(
                &ctx,
                "SELECT device_id, AVG(value) FROM wearable WHERE metric = 'hrv' GROUP BY device_id",
                analytics_samples(records),
            )
            .await?,
            timestamp_range: sample_datafusion(
                &ctx,
                &format!("SELECT COUNT(*) FROM wearable WHERE timestamp >= {range_start}"),
                analytics_samples(records),
            )
            .await?,
        })
    })?;

    let stats = db.stats()?;
    let datafusion_elapsed_total = samples
        .count
        .iter()
        .chain(samples.avg.iter())
        .chain(samples.min_max.iter())
        .chain(samples.group_by_metric.iter())
        .chain(samples.group_by_device.iter())
        .chain(samples.timestamp_range.iter())
        .copied()
        .sum();

    Ok(AnalyticsBenchReport {
        mode: "analytics",
        records,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        sidecar_rebuild_ms: duration_ms(sidecar_rebuild),
        sidecar_size_bytes: sidecar.sidecar_bytes,
        direct_count_ms: duration_ms(direct_count_elapsed),
        query_exec_count_ms: duration_ms(query_exec_count_elapsed),
        datafusion_count_p50_ms: duration_ms(percentile(samples.count.clone(), 0.50)),
        datafusion_count_p95_ms: duration_ms(percentile(samples.count.clone(), 0.95)),
        datafusion_count_p99_ms: duration_ms(percentile(samples.count, 0.99)),
        direct_avg_ms: duration_ms(direct_avg_elapsed),
        query_exec_avg_ms: duration_ms(query_exec_avg_elapsed),
        datafusion_avg_p50_ms: duration_ms(percentile(samples.avg.clone(), 0.50)),
        datafusion_avg_p95_ms: duration_ms(percentile(samples.avg.clone(), 0.95)),
        datafusion_avg_p99_ms: duration_ms(percentile(samples.avg, 0.99)),
        direct_min_max_ms: duration_ms(direct_min_max_elapsed),
        query_exec_min_max_ms: duration_ms(query_exec_min_max_elapsed),
        datafusion_min_max_p50_ms: duration_ms(percentile(samples.min_max.clone(), 0.50)),
        datafusion_min_max_p95_ms: duration_ms(percentile(samples.min_max.clone(), 0.95)),
        datafusion_min_max_p99_ms: duration_ms(percentile(samples.min_max, 0.99)),
        datafusion_group_by_metric_p50_ms: duration_ms(percentile(
            samples.group_by_metric.clone(),
            0.50,
        )),
        datafusion_group_by_metric_p95_ms: duration_ms(percentile(
            samples.group_by_metric.clone(),
            0.95,
        )),
        datafusion_group_by_metric_p99_ms: duration_ms(percentile(samples.group_by_metric, 0.99)),
        datafusion_group_by_device_p50_ms: duration_ms(percentile(
            samples.group_by_device.clone(),
            0.50,
        )),
        datafusion_group_by_device_p95_ms: duration_ms(percentile(
            samples.group_by_device.clone(),
            0.95,
        )),
        datafusion_group_by_device_p99_ms: duration_ms(percentile(samples.group_by_device, 0.99)),
        datafusion_timestamp_range_p50_ms: duration_ms(percentile(
            samples.timestamp_range.clone(),
            0.50,
        )),
        datafusion_timestamp_range_p95_ms: duration_ms(percentile(
            samples.timestamp_range.clone(),
            0.95,
        )),
        datafusion_timestamp_range_p99_ms: duration_ms(percentile(samples.timestamp_range, 0.99)),
        datafusion_rows_per_sec: throughput(
            records * 6 * analytics_samples(records),
            datafusion_elapsed_total,
        ),
        arrow_memory_bytes,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_memory_bench(
    path: impl AsRef<Path>,
    memories: usize,
    dim: usize,
    top_k: usize,
) -> Result<MemoryBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    let mut db = BicDb::open_with_config(&path, DbConfig::default().with_fsync(false))?;
    let memories = memories.max(1);
    let dim = dim.max(1);
    let top_k = top_k.max(1);
    let now = 1_710_000_000;

    let insert_started = Instant::now();
    {
        let mut memory = db.memory();
        for idx in 0..memories {
            let user_id = if idx % 4 == 0 {
                "john-smith".to_string()
            } else {
                format!("user-{}", idx % 1_000)
            };
            let memory_type = bench_memory_type(idx);
            let content = match memory_type {
                MemoryType::Semantic => format!("{user_id} likes low-impact exercise"),
                MemoryType::Episodic => format!("{user_id} asked about HRV on day {}", idx % 30),
                MemoryType::Procedural => "Always verify prescriptions before sending".to_string(),
                MemoryType::Preference => format!("{user_id} prefers metric units"),
                MemoryType::Fact => format!("{user_id} has clinic code rural-{}", idx % 17),
                MemoryType::Goal => format!("{user_id} wants better sleep consistency"),
                MemoryType::Task => format!("Call {user_id} about follow-up {}", idx % 10),
                MemoryType::Conversation => {
                    format!("{user_id} discussed blood pressure trend {}", idx % 20)
                }
                MemoryType::Observation => format!(
                    "{user_id} BP reading was {}/{}",
                    120 + idx % 20,
                    80 + idx % 10
                ),
            };
            memory.remember(
                Memory::new(
                    format!("memory-{idx}"),
                    memory_type,
                    "assistant-agent",
                    content,
                )
                .with_user_id(user_id)
                .with_embedding(make_vector(idx, dim))
                .with_importance((idx % 100) as f32 / 100.0)
                .with_confidence(0.5 + ((idx % 50) as f32 / 100.0))
                .with_created_at(now - (idx % (30 * DAY_SECONDS as usize)) as i64)
                .with_metadata(json!({
                    "source": "memory_bench",
                    "conversation_id": format!("visit-{}", idx % 128),
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                })),
            )?;
        }
    }
    db.flush()?;
    let insert_elapsed = insert_started.elapsed();

    let query = make_vector(7, dim);
    let samples = memory_samples(memories);
    let mut recall_durations = Vec::with_capacity(samples);
    let mut recall_result_count = 0;
    {
        let memory = db.memory();
        for _ in 0..samples {
            let started = Instant::now();
            let results = memory.recall(
                &query,
                top_k,
                MemoryRecallOptions::default()
                    .with_agent_id("assistant-agent")
                    .with_user_id("john-smith")
                    .with_now(now),
            )?;
            recall_result_count = results.len();
            black_box(&results);
            recall_durations.push(started.elapsed());
        }
    }
    let ranking_elapsed_total = recall_durations.iter().copied().sum();

    let timeline_started = Instant::now();
    let timeline = db.memory().timeline("john-smith", now)?;
    black_box(&timeline);
    let timeline_elapsed = timeline_started.elapsed();
    let timeline_memories =
        timeline.yesterday.len() + timeline.last_week.len() + timeline.last_month.len();

    let workspace_started = Instant::now();
    let workspace = db
        .memory()
        .workspace("assistant-agent", Some("john-smith"), now)?;
    black_box(&workspace);
    let workspace_elapsed = workspace_started.elapsed();

    let stats = db.stats()?;
    let memory_event_count = db.events().read(MEMORY_EVENT_STREAM).len();

    Ok(MemoryBenchReport {
        mode: "memory",
        memories,
        dim,
        top_k,
        insert_elapsed_ms: duration_ms(insert_elapsed),
        memories_per_sec: throughput(memories, insert_elapsed),
        recall_p50_ms: duration_ms(percentile(recall_durations.clone(), 0.50)),
        recall_p95_ms: duration_ms(percentile(recall_durations.clone(), 0.95)),
        recall_p99_ms: duration_ms(percentile(recall_durations, 0.99)),
        ranking_memories_per_sec: throughput(memories * samples, ranking_elapsed_total),
        recall_result_count,
        timeline_elapsed_ms: duration_ms(timeline_elapsed),
        timeline_memories,
        workspace_load_ms: duration_ms(workspace_elapsed),
        workspace_memories: workspace.memories.len(),
        memory_event_count,
        database_size_bytes: stats.size_bytes,
        path,
    })
}

pub fn run_sync_bench(path: impl AsRef<Path>, records_per_node: usize) -> Result<SyncBenchReport> {
    let root = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root)?;
    let left_path = root.join("left");
    let right_path = root.join("right");
    let left_bundle = root.join("left.syncbundle");
    let right_bundle = root.join("right.syncbundle");
    let records_per_node = records_per_node.max(1);
    let config = DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true);

    let mut left =
        BicDb::open_with_node_id(&left_path, config.clone(), NodeId(Uuid::from_u128(0x10)))?;
    let mut right = BicDb::open_with_node_id(&right_path, config, NodeId(Uuid::from_u128(0x20)))?;

    for db in [&mut left, &mut right] {
        db.create_collection("patients")?;
        db.create_collection("vectors")?;
        db.create_timeseries_collection("wearable")?;
        // Mesh replication is opt-in PER COLLECTION: a collection that has
        // not been authorized is silently skipped by the export path, which
        // is correct (a mixed database must not leak un-authorized data into
        // a bundle) but means a benchmark that forgets to opt in measures
        // exporting nothing and reports `converged: false` rather than
        // failing. A real deployment authorizes what it replicates; so does
        // this bench now.
        for collection in ["patients", "vectors", "wearable"] {
            db.set_collection_mesh_sync_enabled(collection, true)?;
        }
    }

    // Imports are refused from unpinned origins under `require_signed_imports`.
    // Exchange verifying keys the way an operator would when introducing two
    // nodes to each other, rather than disabling the check.
    if let Some(key) = right.mesh_verifying_key() {
        left.pin_node_key(&NodeId(Uuid::from_u128(0x20)), &key)?;
    }
    if let Some(key) = left.mesh_verifying_key() {
        right.pin_node_key(&NodeId(Uuid::from_u128(0x10)), &key)?;
    }

    let conflicts = (records_per_node / 10).max(1);
    let mut left_patients = Vec::with_capacity(records_per_node + conflicts);
    let mut right_patients = Vec::with_capacity(records_per_node + conflicts);
    let mut left_vectors = Vec::with_capacity(records_per_node);
    let mut right_vectors = Vec::with_capacity(records_per_node);
    let mut left_wearable = Vec::with_capacity(records_per_node);
    let mut right_wearable = Vec::with_capacity(records_per_node);

    for idx in 0..records_per_node {
        left_patients.push(
            Record::new(format!("left-patient-{idx}"))
                .with_timestamp(1_710_000_000 + idx as i64)
                .with_metadata(json!({"node": "left", "idx": idx})),
        );
        right_patients.push(
            Record::new(format!("right-patient-{idx}"))
                .with_timestamp(1_710_500_000 + idx as i64)
                .with_metadata(json!({"node": "right", "idx": idx})),
        );
        left_vectors.push(
            Record::new(format!("left-vector-{idx}"))
                .with_vector(make_vector(idx, 16))
                .with_metadata(json!({"node": "left"})),
        );
        right_vectors.push(
            Record::new(format!("right-vector-{idx}"))
                .with_vector(make_vector(idx + records_per_node, 16))
                .with_metadata(json!({"node": "right"})),
        );
        left_wearable.push(
            Record::new(format!("left-wearable-{idx}"))
                .with_timestamp(1_710_000_000 + idx as i64)
                .with_metadata(
                    json!({"device_id": "left-band", "metric": "hrv", "value": (idx % 200) as f64}),
                ),
        );
        right_wearable.push(
            Record::new(format!("right-wearable-{idx}"))
                .with_timestamp(1_710_500_000 + idx as i64)
                .with_metadata(json!({"device_id": "right-band", "metric": "hrv", "value": (idx % 200) as f64})),
        );
    }

    for idx in 0..conflicts {
        left_patients.push(
            Record::new(format!("conflict-{idx}"))
                .with_timestamp(100)
                .with_metadata(json!({"winner": "left", "idx": idx})),
        );
        right_patients.push(
            Record::new(format!("conflict-{idx}"))
                .with_timestamp(200)
                .with_metadata(json!({"winner": "right", "idx": idx})),
        );
    }

    left.batch_insert("patients", left_patients)?;
    left.batch_insert("vectors", left_vectors)?;
    left.batch_insert("wearable", left_wearable)?;
    right.batch_insert("patients", right_patients)?;
    right.batch_insert("vectors", right_vectors)?;
    right.batch_insert("wearable", right_wearable)?;

    let left_export_started = Instant::now();
    let left_export = left
        .sync()
        .export_to_path(SyncCheckpoint::default(), &left_bundle)?;
    let left_export_elapsed = left_export_started.elapsed();

    let right_import_started = Instant::now();
    let right_import = right.import_sync_bundle_file(&left_bundle)?;
    let right_import_elapsed = right_import_started.elapsed();

    let right_export_started = Instant::now();
    let right_export = right
        .sync()
        .export_to_path(SyncCheckpoint::default(), &right_bundle)?;
    let right_export_elapsed = right_export_started.elapsed();

    let left_import_started = Instant::now();
    let left_import = left.import_sync_bundle_file(&right_bundle)?;
    let left_import_elapsed = left_import_started.elapsed();

    let left_patients_count = left.scan_collection("patients")?.len();
    let right_patients_count = right.scan_collection("patients")?.len();
    let left_vectors_count = left.scan_collection("vectors")?.len();
    let right_vectors_count = right.scan_collection("vectors")?.len();
    let left_wearable_count = left.scan_collection("wearable")?.len();
    let right_wearable_count = right.scan_collection("wearable")?.len();
    let converged = left_patients_count == right_patients_count
        && left_vectors_count == right_vectors_count
        && left_wearable_count == right_wearable_count
        && left.get("patients", "conflict-0")? == right.get("patients", "conflict-0")?;

    let export_events = left_export.event_count + right_export.event_count;
    let import_events = right_import.imported_events + left_import.imported_events;
    let export_elapsed = left_export_elapsed + right_export_elapsed;
    let import_elapsed = right_import_elapsed + left_import_elapsed;
    let records_merged = right_import.records_merged + left_import.records_merged;
    let conflicts_resolved = right_import.conflicts_resolved + left_import.conflicts_resolved;
    let stats = left.stats()?;
    let audit_events = left.events().read(bicdb_core::RECORD_AUDIT_STREAM).len();

    Ok(SyncBenchReport {
        mode: "sync",
        records_per_node,
        left_export_events: left_export.event_count,
        right_export_events: right_export.event_count,
        left_export_elapsed_ms: duration_ms(left_export_elapsed),
        right_export_elapsed_ms: duration_ms(right_export_elapsed),
        export_events_per_sec: throughput(export_events, export_elapsed),
        right_import_elapsed_ms: duration_ms(right_import_elapsed),
        left_import_elapsed_ms: duration_ms(left_import_elapsed),
        import_events_per_sec: throughput(import_events, import_elapsed),
        records_merged,
        merge_records_per_sec: throughput(records_merged, import_elapsed),
        conflicts_resolved,
        conflicts_per_sec: throughput(conflicts_resolved, import_elapsed),
        converged,
        audit_events,
        database_size_bytes: stats.size_bytes,
        path: root,
    })
}

pub fn run_server_bench(
    path: impl AsRef<Path>,
    clients: usize,
    queries: usize,
) -> Result<ServerBenchReport> {
    run_server_bench_with_config(path, clients, clients, queries, ServerBenchScenario::Mixed)
}

pub fn run_server_certification(
    path: impl AsRef<Path>,
    profile: impl Into<String>,
    clients: usize,
    active_query_concurrency: usize,
    queries_per_workload: usize,
) -> Result<ServerCertificationReport> {
    let path = path.as_ref().to_path_buf();
    let profile = profile.into();
    let clients = clients.max(2);
    let active_query_concurrency = active_query_concurrency.max(1).min(clients);
    let queries_per_workload = queries_per_workload.max(1);
    let budget = server_certification_budget(&profile);
    let idle_soak_duration = server_certification_idle_soak_duration(&profile);
    let scenarios = [
        ServerBenchScenario::IdlePooled,
        ServerBenchScenario::ReadOnly,
        ServerBenchScenario::Mixed,
        ServerBenchScenario::CancelContention,
        ServerBenchScenario::Churn,
    ];
    let mut scenario_reports = Vec::new();

    for scenario in scenarios {
        let scenario_path = path.join(scenario.as_str());
        let report = run_server_bench_with_options(
            &scenario_path,
            clients,
            active_query_concurrency,
            queries_per_workload,
            scenario,
            if scenario == ServerBenchScenario::IdlePooled {
                idle_soak_duration
            } else {
                Duration::from_millis(100)
            },
        )?;
        let failures = certify_server_scenario(&report, scenario, &budget);
        scenario_reports.push(ServerCertificationScenarioReport {
            scenario: scenario.as_str(),
            passed: failures.is_empty(),
            failures,
            report,
        });
    }

    let passed = scenario_reports.iter().all(|scenario| scenario.passed);
    Ok(ServerCertificationReport {
        mode: "server-certification",
        profile,
        clients,
        active_query_concurrency,
        queries_per_workload,
        idle_soak_ms: idle_soak_duration.as_millis().min(u128::from(u64::MAX)) as u64,
        passed,
        budget,
        scenarios: scenario_reports,
        path,
    })
}

pub fn run_server_bench_with_config(
    path: impl AsRef<Path>,
    clients: usize,
    active_query_concurrency: usize,
    queries: usize,
    scenario: ServerBenchScenario,
) -> Result<ServerBenchReport> {
    run_server_bench_with_options(
        path,
        clients,
        active_query_concurrency,
        queries,
        scenario,
        Duration::from_millis(100),
    )
}

fn run_server_bench_with_options(
    path: impl AsRef<Path>,
    clients: usize,
    active_query_concurrency: usize,
    queries: usize,
    scenario: ServerBenchScenario,
    idle_hold_duration: Duration,
) -> Result<ServerBenchReport> {
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path)?;

    let clients = if scenario == ServerBenchScenario::CancelContention {
        clients.max(2)
    } else {
        clients.max(1)
    };
    let queries = if scenario == ServerBenchScenario::IdlePooled {
        0
    } else {
        queries.max(1)
    };
    let active_query_concurrency = active_query_concurrency.max(1).min(clients);
    let listener = TcpListener::bind("127.0.0.1:0").context("bind benchmark pgwire listener")?;
    let address = listener.local_addr()?;
    let server = PgWireServer::open(
        &path,
        PgWireConfig {
            host: "127.0.0.1".to_string(),
            port: address.port(),
            max_connections: clients + 8,
            flush_interval: Duration::from_secs(3_600),
            checkpoint_interval: Duration::from_secs(3_600),
            metrics_interval: Duration::from_secs(3_600),
            shutdown_grace_period: Duration::from_millis(100),
            ..PgWireConfig::default()
        },
    )?;
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut setup = bench_pg_connect(address)?;
    bench_pg_simple_query(
        &mut setup,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT);",
    )?;
    for idx in 0..200 {
        bench_pg_simple_query(
            &mut setup,
            &format!(
                "INSERT INTO patients (id, name, age) VALUES ('seed-{idx}', 'seed', {});",
                20 + (idx % 70)
            ),
        )?;
    }
    bench_pg_close(&mut setup)?;

    let started = Instant::now();
    let mut setup_samples = Vec::new();
    let mut samples = Vec::with_capacity(queries.max(1));
    let mut select_queries = 0;
    let mut insert_queries = 0;
    let mut rss_bytes = current_rss_bytes();
    let mut thread_count = current_thread_count();

    if scenario == ServerBenchScenario::CancelContention {
        let short_client_count = active_query_concurrency
            .saturating_sub(1)
            .max(1)
            .min(clients - 1);
        let idle_client_count = clients.saturating_sub(short_client_count + 1);
        let connect_started = Instant::now();
        let (mut long_client, backend_key) = bench_pg_connect_with_backend_key(address)?;
        setup_samples.push(connect_started.elapsed());
        bench_pg_simple_query_raw(&mut long_client, "SELECT pg_sleep(2);")?;
        thread::sleep(Duration::from_millis(100));

        let mut short_clients = Vec::with_capacity(short_client_count);
        for _ in 0..short_client_count {
            let connect_started = Instant::now();
            short_clients.push(bench_pg_connect(address)?);
            setup_samples.push(connect_started.elapsed());
        }
        let mut idle_clients = Vec::with_capacity(idle_client_count);
        for _ in 0..idle_client_count {
            let connect_started = Instant::now();
            idle_clients.push(bench_pg_connect(address)?);
            setup_samples.push(connect_started.elapsed());
        }
        rss_bytes = max_optional_u64(rss_bytes, current_rss_bytes());
        thread_count = max_optional_usize(thread_count, current_thread_count());

        let handles = short_clients
            .into_iter()
            .enumerate()
            .map(|(client_idx, mut client)| {
                let count = queries / short_client_count
                    + usize::from(client_idx < queries % short_client_count);
                thread::spawn(move || -> Result<ServerClientBenchResult> {
                    let mut result = ServerClientBenchResult::default();
                    for _ in 0..count {
                        let query_started = Instant::now();
                        bench_pg_simple_query(&mut client, "SELECT COUNT(*) FROM patients;")?;
                        result.query_samples.push(query_started.elapsed());
                        result.select_queries += 1;
                    }
                    bench_pg_close(&mut client)?;
                    Ok(result)
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            let result = handle
                .join()
                .map_err(|_| anyhow::anyhow!("server benchmark client thread panicked"))??;
            samples.extend(result.query_samples);
            select_queries += result.select_queries;
        }

        bench_pg_send_cancel_request(address, backend_key)?;
        let outcome = bench_pg_read_outcome(&mut long_client)?;
        if outcome.ready_status != b'I' {
            anyhow::bail!(
                "expected ReadyForQuery idle after cancel, got {}",
                outcome.ready_status as char
            );
        }
        let error = outcome
            .error
            .ok_or_else(|| anyhow::anyhow!("cancelled long query unexpectedly succeeded"))?;
        if !error.contains("57014") {
            anyhow::bail!("expected SQLSTATE 57014 cancel error, got {error:?}");
        }
        expect_rows(
            bench_pg_simple_query(&mut long_client, "SELECT 1;")?,
            &[&["1"]],
        )?;
        bench_pg_close(&mut long_client)?;
        for mut client in idle_clients {
            bench_pg_close(&mut client)?;
        }
    } else if scenario == ServerBenchScenario::Churn {
        let handles = (0..active_query_concurrency)
            .map(|worker_idx| {
                let count = queries / active_query_concurrency
                    + usize::from(worker_idx < queries % active_query_concurrency);
                thread::spawn(move || -> Result<ServerClientBenchResult> {
                    let mut result = ServerClientBenchResult::default();
                    for query_idx in 0..count {
                        let global_query_idx = worker_idx + query_idx * active_query_concurrency;
                        let connect_started = Instant::now();
                        let mut client = bench_pg_connect(address)?;
                        result.setup_samples.push(connect_started.elapsed());
                        let sql = server_bench_sql(scenario, worker_idx, global_query_idx);
                        let query_started = Instant::now();
                        bench_pg_simple_query(&mut client, &sql)?;
                        result.query_samples.push(query_started.elapsed());
                        if server_bench_sql_is_insert(scenario, global_query_idx) {
                            result.insert_queries += 1;
                        } else {
                            result.select_queries += 1;
                        }
                        bench_pg_close(&mut client)?;
                    }
                    Ok(result)
                })
            })
            .collect::<Vec<_>>();
        rss_bytes = max_optional_u64(rss_bytes, current_rss_bytes());
        thread_count = max_optional_usize(thread_count, current_thread_count());
        for handle in handles {
            let result = handle
                .join()
                .map_err(|_| anyhow::anyhow!("server benchmark client thread panicked"))??;
            setup_samples.extend(result.setup_samples);
            samples.extend(result.query_samples);
            select_queries += result.select_queries;
            insert_queries += result.insert_queries;
        }
    } else {
        let mut clients_and_setup = Vec::with_capacity(clients);
        for _ in 0..clients {
            let connect_started = Instant::now();
            let client = bench_pg_connect(address)?;
            setup_samples.push(connect_started.elapsed());
            clients_and_setup.push(client);
        }
        rss_bytes = max_optional_u64(rss_bytes, current_rss_bytes());
        thread_count = max_optional_usize(thread_count, current_thread_count());

        if scenario == ServerBenchScenario::IdlePooled {
            thread::sleep(idle_hold_duration);
            rss_bytes = max_optional_u64(rss_bytes, current_rss_bytes());
            thread_count = max_optional_usize(thread_count, current_thread_count());
        } else {
            let mut active_clients = Vec::with_capacity(active_query_concurrency);
            for _ in 0..active_query_concurrency {
                if let Some(client) = clients_and_setup.pop() {
                    active_clients.push(client);
                }
            }
            let handles = active_clients
                .into_iter()
                .enumerate()
                .map(|(client_idx, mut client)| {
                    let count = queries / active_query_concurrency
                        + usize::from(client_idx < queries % active_query_concurrency);
                    thread::spawn(move || -> Result<ServerClientBenchResult> {
                        let mut result = ServerClientBenchResult::default();
                        for query_idx in 0..count {
                            let global_query_idx =
                                client_idx + query_idx * active_query_concurrency;
                            let sql = server_bench_sql(scenario, client_idx, global_query_idx);
                            let query_started = Instant::now();
                            bench_pg_simple_query(&mut client, &sql)?;
                            result.query_samples.push(query_started.elapsed());
                            if server_bench_sql_is_insert(scenario, global_query_idx) {
                                result.insert_queries += 1;
                            } else {
                                result.select_queries += 1;
                            }
                        }
                        bench_pg_close(&mut client)?;
                        Ok(result)
                    })
                })
                .collect::<Vec<_>>();
            rss_bytes = max_optional_u64(rss_bytes, current_rss_bytes());
            thread_count = max_optional_usize(thread_count, current_thread_count());
            for handle in handles {
                let result = handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("server benchmark client thread panicked"))??;
                samples.extend(result.query_samples);
                select_queries += result.select_queries;
                insert_queries += result.insert_queries;
            }
        }

        for mut client in clients_and_setup {
            bench_pg_close(&mut client)?;
        }
    }
    let mut verifier = bench_pg_connect(address)?;
    let final_patient_count =
        bench_pg_simple_query(&mut verifier, "SELECT COUNT(*) FROM patients;")?
            .first()
            .and_then(|row| row.first())
            .and_then(|value| value.parse::<usize>().ok());
    bench_pg_close(&mut verifier)?;
    if let Some(count) = final_patient_count {
        let expected = 200 + insert_queries;
        if scenario == ServerBenchScenario::Mixed || scenario == ServerBenchScenario::Churn {
            anyhow::ensure!(
                count == expected,
                "expected {expected} patients after {} workload inserts, found {count}",
                insert_queries
            );
        }
    }
    let elapsed = started.elapsed();

    let stats = server.stats_snapshot();
    server.request_shutdown();
    server_thread
        .join()
        .map_err(|_| anyhow::anyhow!("server benchmark listener thread panicked"))??;
    // The server handle owns the exclusive on-disk writer lease even after the
    // listener exits. Release it before reopening the same path for offline
    // statistics; a second live handle is intentionally rejected by BicDB.
    drop(server);
    let db_stats = BicDb::open(&path)?.stats()?;
    let db_lock_wait_avg_ms = nanos_avg_ms(stats.db_lock_wait_total_ns, stats.db_lock_acquisitions);
    let db_lock_hold_avg_ms = nanos_avg_ms(stats.db_lock_hold_total_ns, stats.db_lock_acquisitions);
    let write_wait_avg_ms = nanos_avg_ms(stats.write_wait_total_ns, stats.writes_executed);
    let write_execution_avg_ms =
        nanos_avg_ms(stats.write_execution_total_ns, stats.writes_executed);

    Ok(ServerBenchReport {
        mode: "server",
        scenario: scenario.as_str(),
        clients,
        active_query_concurrency,
        queries,
        select_queries,
        insert_queries,
        setup_latency_p50_ms: duration_ms(percentile(setup_samples.clone(), 0.50)),
        setup_latency_p95_ms: duration_ms(percentile(setup_samples.clone(), 0.95)),
        setup_latency_p99_ms: duration_ms(percentile(setup_samples, 0.99)),
        elapsed_ms: duration_ms(elapsed),
        queries_per_sec: throughput(queries, elapsed),
        latency_p50_ms: duration_ms(percentile(samples.clone(), 0.50)),
        latency_p95_ms: duration_ms(percentile(samples.clone(), 0.95)),
        latency_p99_ms: duration_ms(percentile(samples, 0.99)),
        active_connections_peak: stats.peak_active_connections,
        rejected_connections: stats.rejected_connections,
        server_max_queued_queries: stats.max_queued_queries,
        server_queued_queries_max: stats.queued_queries_max,
        server_active_reads_peak: stats.peak_active_reads,
        server_queued_reads_max: stats.queued_reads_max,
        server_active_writes_peak: stats.peak_active_writes,
        server_queued_writes_max: stats.queued_writes_max,
        server_query_queue_wait_p50_ms: nanos_to_ms(stats.query_queue_wait_p50_ns),
        server_query_queue_wait_p95_ms: nanos_to_ms(stats.query_queue_wait_p95_ns),
        server_query_queue_wait_p99_ms: nanos_to_ms(stats.query_queue_wait_p99_ns),
        server_reported_queries: stats.queries_executed,
        server_failed_queries: stats.failed_queries,
        server_canceled_queries: stats.canceled_queries,
        server_timed_out_queries: stats.timed_out_queries,
        server_writes_executed: stats.writes_executed,
        server_max_queued_writes: stats.max_queued_writes,
        server_write_queue_depth_max: stats.write_queue_depth_max,
        server_write_wait_avg_ms: write_wait_avg_ms,
        server_write_wait_max_ms: nanos_to_ms(stats.write_wait_max_ns),
        server_write_execution_avg_ms: write_execution_avg_ms,
        server_write_execution_max_ms: nanos_to_ms(stats.write_execution_max_ns),
        server_write_rejected_count: stats.write_rejected_count,
        server_write_timed_out_count: stats.write_timed_out_count,
        server_memory_estimate_bytes: stats
            .db_size_bytes
            .saturating_add((stats.peak_active_connections as u64).saturating_mul(64 * 1024)),
        db_lock_acquisitions: stats.db_lock_acquisitions,
        db_lock_wait_avg_ms,
        db_lock_wait_max_ms: nanos_to_ms(stats.db_lock_wait_max_ns),
        db_lock_hold_avg_ms,
        db_lock_hold_max_ms: nanos_to_ms(stats.db_lock_hold_max_ns),
        database_size_bytes: db_stats.size_bytes,
        final_patient_count,
        rss_bytes,
        thread_count,
        path,
    })
}

fn server_certification_budget(profile: &str) -> ServerCertificationBudget {
    if matches!(profile, "full" | "manual" | "nightly" | "1000") {
        ServerCertificationBudget {
            max_rss_bytes: 256 * 1024 * 1024,
            max_thread_count: 160,
            min_connection_success_rate: 1.0,
            read_min_queries_per_sec: 100.0,
            read_max_p95_ms: 250.0,
            read_max_p99_ms: 500.0,
            mixed_min_queries_per_sec: 25.0,
            mixed_max_p95_ms: 2_500.0,
            mixed_max_p99_ms: 5_000.0,
            churn_max_p99_ms: 1_500.0,
            cancel_short_read_max_p99_ms: 500.0,
            max_rejected_connections: 0,
            max_unexpected_failed_queries: 0,
            max_timed_out_queries: 0,
        }
    } else {
        ServerCertificationBudget {
            max_rss_bytes: 256 * 1024 * 1024,
            max_thread_count: 160,
            min_connection_success_rate: 1.0,
            read_min_queries_per_sec: 1.0,
            read_max_p95_ms: 2_000.0,
            read_max_p99_ms: 5_000.0,
            mixed_min_queries_per_sec: 1.0,
            mixed_max_p95_ms: 5_000.0,
            mixed_max_p99_ms: 10_000.0,
            churn_max_p99_ms: 5_000.0,
            cancel_short_read_max_p99_ms: 5_000.0,
            max_rejected_connections: 0,
            max_unexpected_failed_queries: 0,
            max_timed_out_queries: 0,
        }
    }
}

fn server_certification_idle_soak_duration(profile: &str) -> Duration {
    if matches!(profile, "full" | "manual" | "nightly" | "1000") {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(1)
    }
}

fn certify_server_scenario(
    report: &ServerBenchReport,
    scenario: ServerBenchScenario,
    budget: &ServerCertificationBudget,
) -> Vec<String> {
    let mut failures = Vec::new();
    let connection_success_rate = report.active_connections_peak as f64 / report.clients as f64;

    if scenario != ServerBenchScenario::Churn
        && connection_success_rate < budget.min_connection_success_rate
    {
        failures.push(format!(
            "connection success rate {:.3} below {:.3}",
            connection_success_rate, budget.min_connection_success_rate
        ));
    }
    if report.rejected_connections > budget.max_rejected_connections {
        failures.push(format!(
            "rejected connections {} above {}",
            report.rejected_connections, budget.max_rejected_connections
        ));
    }
    if let Some(rss_bytes) = report.rss_bytes {
        if rss_bytes > budget.max_rss_bytes {
            failures.push(format!(
                "RSS {} above {}",
                fmt_bytes(rss_bytes),
                fmt_bytes(budget.max_rss_bytes)
            ));
        }
    }
    if let Some(thread_count) = report.thread_count {
        if thread_count > budget.max_thread_count {
            failures.push(format!(
                "thread count {thread_count} above {}",
                budget.max_thread_count
            ));
        }
    }
    if report.server_timed_out_queries > budget.max_timed_out_queries {
        failures.push(format!(
            "timed-out queries {} above {}",
            report.server_timed_out_queries, budget.max_timed_out_queries
        ));
    }

    match scenario {
        ServerBenchScenario::IdlePooled => {
            if report.queries != 0 {
                failures.push(format!(
                    "idle-pooled queries was {}, expected 0",
                    report.queries
                ));
            }
        }
        ServerBenchScenario::ReadOnly => {
            if report.server_failed_queries > budget.max_unexpected_failed_queries {
                failures.push(format!(
                    "read-only failures {}",
                    report.server_failed_queries
                ));
            }
            if report.queries_per_sec < budget.read_min_queries_per_sec {
                failures.push(format!(
                    "read-only throughput {:.2} below {:.2}",
                    report.queries_per_sec, budget.read_min_queries_per_sec
                ));
            }
            if report.latency_p95_ms > budget.read_max_p95_ms {
                failures.push(format!(
                    "read-only p95 {:.3}ms above {:.3}ms",
                    report.latency_p95_ms, budget.read_max_p95_ms
                ));
            }
            if report.latency_p99_ms > budget.read_max_p99_ms {
                failures.push(format!(
                    "read-only p99 {:.3}ms above {:.3}ms",
                    report.latency_p99_ms, budget.read_max_p99_ms
                ));
            }
        }
        ServerBenchScenario::Mixed => {
            if report.server_failed_queries > budget.max_unexpected_failed_queries {
                failures.push(format!("mixed failures {}", report.server_failed_queries));
            }
            if report.insert_queries == 0 || report.select_queries == 0 {
                failures.push("mixed workload did not include both reads and writes".to_string());
            }
            if let Some(count) = report.final_patient_count {
                let expected = 200 + report.insert_queries;
                if count != expected {
                    failures.push(format!(
                        "mixed final patient count {count} did not match expected {expected}"
                    ));
                }
            } else {
                failures.push("mixed final patient count was not measured".to_string());
            }
            if report.queries_per_sec < budget.mixed_min_queries_per_sec {
                failures.push(format!(
                    "mixed throughput {:.2} below {:.2}",
                    report.queries_per_sec, budget.mixed_min_queries_per_sec
                ));
            }
            if report.latency_p95_ms > budget.mixed_max_p95_ms {
                failures.push(format!(
                    "mixed p95 {:.3}ms above {:.3}ms",
                    report.latency_p95_ms, budget.mixed_max_p95_ms
                ));
            }
            if report.latency_p99_ms > budget.mixed_max_p99_ms {
                failures.push(format!(
                    "mixed p99 {:.3}ms above {:.3}ms",
                    report.latency_p99_ms, budget.mixed_max_p99_ms
                ));
            }
        }
        ServerBenchScenario::CancelContention => {
            if report.server_canceled_queries != 1 {
                failures.push(format!(
                    "cancel-contention canceled queries {}, expected 1",
                    report.server_canceled_queries
                ));
            }
            if report.server_failed_queries != 1 {
                failures.push(format!(
                    "cancel-contention failed queries {}, expected the canceled long query only",
                    report.server_failed_queries
                ));
            }
            if report.latency_p99_ms > budget.cancel_short_read_max_p99_ms {
                failures.push(format!(
                    "cancel-contention short-read p99 {:.3}ms above {:.3}ms",
                    report.latency_p99_ms, budget.cancel_short_read_max_p99_ms
                ));
            }
        }
        ServerBenchScenario::Churn => {
            if report.server_failed_queries > budget.max_unexpected_failed_queries {
                failures.push(format!("churn failures {}", report.server_failed_queries));
            }
            if report.latency_p99_ms > budget.churn_max_p99_ms {
                failures.push(format!(
                    "churn p99 {:.3}ms above {:.3}ms",
                    report.latency_p99_ms, budget.churn_max_p99_ms
                ));
            }
        }
        ServerBenchScenario::LongScan => {}
    }

    failures
}

#[derive(Default)]
struct ServerClientBenchResult {
    setup_samples: Vec<Duration>,
    query_samples: Vec<Duration>,
    select_queries: usize,
    insert_queries: usize,
}

fn server_bench_sql(scenario: ServerBenchScenario, client_idx: usize, query_idx: usize) -> String {
    match scenario {
        ServerBenchScenario::IdlePooled => "SELECT 1;".to_string(),
        ServerBenchScenario::ReadOnly => "SELECT COUNT(*) FROM patients;".to_string(),
        ServerBenchScenario::Mixed | ServerBenchScenario::Churn => {
            if server_bench_sql_is_insert(scenario, query_idx) {
                format!(
                    "INSERT INTO patients (id, name, age) VALUES ('server-{client_idx}-{query_idx}', 'name-{client_idx}', {});",
                    20 + (query_idx % 70)
                )
            } else {
                "SELECT COUNT(*) FROM patients;".to_string()
            }
        }
        ServerBenchScenario::LongScan => {
            "SELECT id, name, age FROM patients ORDER BY age DESC LIMIT 100;".to_string()
        }
        ServerBenchScenario::CancelContention => "SELECT COUNT(*) FROM patients;".to_string(),
    }
}

fn server_bench_sql_is_insert(scenario: ServerBenchScenario, query_idx: usize) -> bool {
    matches!(
        scenario,
        ServerBenchScenario::Mixed | ServerBenchScenario::Churn
    ) && query_idx.is_multiple_of(5)
}

pub fn run_postgres_compat_suite(
    path: impl AsRef<Path>,
    target_version: impl Into<String>,
) -> Result<PostgresCompatReport> {
    let path = path.as_ref().to_path_buf();
    let target_version = target_version.into();
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path)?;

    let listener =
        TcpListener::bind("127.0.0.1:0").context("bind compatibility pgwire listener")?;
    let address = listener.local_addr()?;
    let server = PgWireServer::open(
        &path,
        PgWireConfig {
            host: "127.0.0.1".to_string(),
            port: address.port(),
            max_connections: 8,
            flush_interval: Duration::from_secs(3_600),
            checkpoint_interval: Duration::from_secs(3_600),
            metrics_interval: Duration::from_secs(3_600),
            ..PgWireConfig::default()
        },
    )?;
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut cases = Vec::new();
    let mut client = bench_pg_connect(address)?;

    record_pg_case(
        &mut cases,
        "protocol.startup.ready_for_query_idle",
        "protocol",
        "StartupMessage completes with ReadyForQuery idle status",
        PostgresCompatExpectation::Works,
        || {
            let mut startup_client =
                TcpStream::connect(address).context("connect startup probe client")?;
            startup_client.set_read_timeout(Some(Duration::from_secs(30)))?;
            startup_client.set_write_timeout(Some(Duration::from_secs(30)))?;
            bench_pg_startup(&mut startup_client)?;
            let status = bench_pg_read_until_ready(&mut startup_client)?;
            bench_pg_close(&mut startup_client)?;
            if status == b'I' {
                Ok("startup returned ReadyForQuery I, matching PostgreSQL 18.4 idle startup behavior".to_string())
            } else {
                anyhow::bail!("expected ReadyForQuery I, got {}", status as char)
            }
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.startup.protocol_3_2",
        "protocol",
        "StartupMessage protocol 3.2 completes with ReadyForQuery idle status",
        PostgresCompatExpectation::Works,
        || {
            let mut startup_client =
                TcpStream::connect(address).context("connect protocol 3.2 startup probe client")?;
            startup_client.set_read_timeout(Some(Duration::from_secs(30)))?;
            startup_client.set_write_timeout(Some(Duration::from_secs(30)))?;
            bench_pg_startup_version(&mut startup_client, 196_610, true)?;
            let status = bench_pg_read_until_ready(&mut startup_client)?;
            bench_pg_close(&mut startup_client)?;
            if status == b'I' {
                Ok("protocol 3.2 startup returned ReadyForQuery I, matching PostgreSQL 18.4 protocol acceptance".to_string())
            } else {
                anyhow::bail!(
                    "expected ReadyForQuery I for protocol 3.2, got {}",
                    status as char
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.startup.protocol_3_2_option_negotiation",
        "protocol",
        "StartupMessage protocol 3.2 unsupported _pq_. option receives NegotiateProtocolVersion",
        PostgresCompatExpectation::Works,
        || {
            let mut startup_client = TcpStream::connect(address)
                .context("connect protocol option startup probe client")?;
            startup_client.set_read_timeout(Some(Duration::from_secs(30)))?;
            startup_client.set_write_timeout(Some(Duration::from_secs(30)))?;
            bench_pg_startup_version_with_options(
                &mut startup_client,
                196_610,
                true,
                &[("_pq_.bicdb_probe", "1")],
            )?;
            let (protocol_version, unsupported_options) =
                bench_pg_read_negotiate_protocol_version(&mut startup_client)?;
            let status = bench_pg_read_until_ready(&mut startup_client)?;
            bench_pg_close(&mut startup_client)?;
            if protocol_version == 196_610
                && unsupported_options == vec!["_pq_.bicdb_probe".to_string()]
                && status == b'I'
            {
                Ok("unsupported _pq_. startup option received NegotiateProtocolVersion before ReadyForQuery I".to_string())
            } else {
                anyhow::bail!(
                    "expected NegotiateProtocolVersion 196610 with _pq_.bicdb_probe and ReadyForQuery I, got version={protocol_version}, options={unsupported_options:?}, status={}",
                    status as char
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.startup.malformed_safe_error",
        "protocol",
        "Malformed StartupMessage returns ErrorResponse and leaves listener healthy",
        PostgresCompatExpectation::Works,
        || {
            let mut malformed_client =
                TcpStream::connect(address).context("connect malformed startup probe client")?;
            malformed_client.set_read_timeout(Some(Duration::from_secs(30)))?;
            malformed_client.set_write_timeout(Some(Duration::from_secs(30)))?;
            bench_pg_startup_version(&mut malformed_client, 196_608, false)?;
            let error = bench_pg_read_startup_error(&mut malformed_client)?;
            if !error.contains("FATAL")
                || !error.contains("08P01")
                || !error.contains("startup message missing terminator")
            {
                anyhow::bail!("expected PostgreSQL-shaped startup ErrorResponse, got {error:?}");
            }

            let mut valid_client = TcpStream::connect(address)
                .context("connect post-malformed startup probe client")?;
            valid_client.set_read_timeout(Some(Duration::from_secs(30)))?;
            valid_client.set_write_timeout(Some(Duration::from_secs(30)))?;
            bench_pg_startup(&mut valid_client)?;
            let status = bench_pg_read_until_ready(&mut valid_client)?;
            bench_pg_close(&mut valid_client)?;
            if status == b'I' {
                Ok(format!("malformed startup returned clear ErrorResponse ({}) and a subsequent startup succeeded", compact_error(&error)))
            } else {
                anyhow::bail!(
                    "expected subsequent ReadyForQuery I, got {}",
                    status as char
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.ssl_request.no_tls",
        "protocol",
        "SSLRequest receives PostgreSQL N response when TLS is not configured",
        PostgresCompatExpectation::Works,
        || {
            let response = bench_pg_ssl_request(address)?;
            if response == b'N' {
                Ok("SSLRequest returned N without TLS, matching PostgreSQL's one-byte negotiation shape".to_string())
            } else {
                anyhow::bail!("expected SSLRequest response N, got {}", response as char)
            }
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.simple_query.select_1",
        "protocol",
        "Simple Query protocol returns a scalar result for SELECT 1",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT 1;")?;
            expect_rows(rows, &[&["1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "metadata.current_database",
        "metadata",
        "current_database() returns a database name",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT current_database();")?;
            expect_rows(rows, &[&["bicdb"]])
        },
    );
    record_pg_case(
        &mut cases,
        "metadata.current_schema",
        "metadata",
        "current_schema() returns public",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT current_schema();")?;
            expect_rows(rows, &[&["public"]])
        },
    );
    record_pg_case(
        &mut cases,
        "metadata.version",
        "metadata",
        "version() returns BicDB identity and its PostgreSQL wire target",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT version();")?;
            expect_nonempty_first_cell(rows, "version")
        },
    );
    record_pg_case(
        &mut cases,
        "metadata.show_server_version",
        "metadata",
        "SHOW server_version returns a server version string",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SHOW server_version;")?;
            expect_nonempty_first_cell(rows, "server_version")
        },
    );
    record_pg_case(
        &mut cases,
        "metadata.show_application_name",
        "metadata",
        "SHOW application_name returns the client-supplied name or PostgreSQL's empty default",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SHOW application_name;")?;
            expect_rows(rows, &[&[""]])
        },
    );
    record_pg_case(
        &mut cases,
        "ddl.create_table",
        "ddl",
        "CREATE TABLE maps a PostgreSQL table to a BicDB collection",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT, metadata JSONB);",
            )?;
            expect_rows(rows, &[])
        },
    );
    record_pg_case(
        &mut cases,
        "ddl.create_index",
        "ddl",
        "CREATE INDEX records secondary index metadata",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "CREATE INDEX idx_patients_age ON patients(age);",
            )?;
            expect_rows(rows, &[])
        },
    );
    record_pg_case(
        &mut cases,
        "dml.insert_values",
        "dml",
        "INSERT INTO writes rows with typed columns and JSONB metadata",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "INSERT INTO patients (id, name, age, metadata) VALUES ('p1', 'John', 45, '{\"clinic\":\"rural-7\"}'::jsonb), ('p2', 'Ada', 36, '{\"clinic\":\"rural-8\"}'::jsonb);",
            )?;
            expect_rows(rows, &[])
        },
    );
    record_pg_case(
        &mut cases,
        "copy.from_stdin_csv",
        "copy",
        "COPY patients FROM STDIN CSV imports rows over the PostgreSQL wire protocol",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "CREATE TABLE copy_patients (id TEXT PRIMARY KEY, name TEXT, age INT, metadata JSONB);",
                )?,
                &[],
            )?;
            bench_pg_copy_from_stdin_csv(
                &mut client,
                "COPY copy_patients FROM STDIN CSV;",
                "p_copy_1,Copy One,31,\"{ \"\"clinic\"\":\"\"copy\"\" }\"\n",
            )?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT name, age FROM copy_patients WHERE id = 'p_copy_1';",
            )?;
            expect_rows(rows, &[&["Copy One", "31"]])
        },
    );
    record_pg_case(
        &mut cases,
        "copy.to_stdout_csv",
        "copy",
        "COPY patients TO STDOUT CSV exports rows over the PostgreSQL wire protocol",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_copy_to_stdout_csv(
                &mut client,
                "COPY (SELECT id, name, age FROM copy_patients WHERE id = 'p_copy_1') TO STDOUT CSV;",
            )?;
            if rows == vec!["p_copy_1,Copy One,31\n".to_string()] {
                Ok("COPY TO STDOUT CSV exported expected row".to_string())
            } else {
                anyhow::bail!("unexpected COPY CSV rows {rows:?}")
            }
        },
    );
    record_pg_case(
        &mut cases,
        "dml.select_projection_where",
        "dml",
        "SELECT projection with WHERE id returns a row",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT name, age FROM patients WHERE id = 'p1';",
            )?;
            expect_rows(rows, &[&["John", "45"]])
        },
    );
    record_pg_case(
        &mut cases,
        "dml.update_where",
        "dml",
        "UPDATE mutates selected rows",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "UPDATE patients SET age = 46 WHERE id = 'p1';",
            )?;
            expect_rows(rows, &[])?;
            let rows =
                bench_pg_simple_query(&mut client, "SELECT age FROM patients WHERE id = 'p1';")?;
            expect_rows(rows, &[&["46"]])
        },
    );
    record_pg_case(
        &mut cases,
        "dml.delete_where",
        "dml",
        "DELETE removes selected rows",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "DELETE FROM patients WHERE id = 'p2';")?;
            expect_rows(rows, &[])?;
            let rows = bench_pg_simple_query(&mut client, "SELECT COUNT(*) FROM patients;")?;
            expect_rows(rows, &[&["1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "transactions.rollback",
        "transactions",
        "ROLLBACK hides uncommitted writes",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(bench_pg_simple_query(&mut client, "BEGIN;")?, &[])?;
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "INSERT INTO patients (id, name, age) VALUES ('p3', 'Rolled Back', 30);",
                )?,
                &[],
            )?;
            expect_rows(bench_pg_simple_query(&mut client, "ROLLBACK;")?, &[])?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT COUNT(*) FROM patients WHERE id = 'p3';",
            )?;
            expect_rows(rows, &[&["0"]])
        },
    );
    record_pg_case(
        &mut cases,
        "transactions.commit",
        "transactions",
        "COMMIT makes writes visible",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(bench_pg_simple_query(&mut client, "BEGIN;")?, &[])?;
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "INSERT INTO patients (id, name, age) VALUES ('p4', 'Committed', 31);",
                )?,
                &[],
            )?;
            expect_rows(bench_pg_simple_query(&mut client, "COMMIT;")?, &[])?;
            let rows =
                bench_pg_simple_query(&mut client, "SELECT name FROM patients WHERE id = 'p4';")?;
            expect_rows(rows, &[&["Committed"]])
        },
    );
    record_pg_case(
        &mut cases,
        "transactions.isolation_read_committed_forms",
        "transactions",
        "SET TRANSACTION and BEGIN isolation commands accept READ COMMITTED",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;",
                )?,
                &[],
            )?;
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE;",
                )?,
                &[],
            )?;
            expect_rows(bench_pg_simple_query(&mut client, "COMMIT;")?, &[])?;
            expect_rows(
                bench_pg_simple_query(&mut client, "BEGIN ISOLATION LEVEL READ COMMITTED;")?,
                &[],
            )?;
            expect_rows(bench_pg_simple_query(&mut client, "ROLLBACK;")?, &[])
        },
    );
    record_pg_case(
        &mut cases,
        "transactions.read_committed_visibility",
        "transactions",
        "READ COMMITTED currently uses BicDB's transaction snapshot instead of PostgreSQL per-statement visibility",
        PostgresCompatExpectation::ExpectedDifference,
        || {
            let mut writer = bench_pg_connect(address)?;
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "CREATE TABLE rc_visibility (id TEXT PRIMARY KEY, name TEXT);",
                )?,
                &[],
            )?;
            expect_rows(
                bench_pg_simple_query(&mut client, "BEGIN ISOLATION LEVEL READ COMMITTED;")?,
                &[],
            )?;
            expect_rows(
                bench_pg_simple_query(&mut client, "SELECT COUNT(*) FROM rc_visibility;")?,
                &[&["0"]],
            )?;
            expect_rows(bench_pg_simple_query(&mut writer, "BEGIN;")?, &[])?;
            expect_rows(
                bench_pg_simple_query(
                    &mut writer,
                    "INSERT INTO rc_visibility (id, name) VALUES ('p1', 'Committed');",
                )?,
                &[],
            )?;
            expect_rows(
                bench_pg_simple_query(&mut client, "SELECT COUNT(*) FROM rc_visibility;")?,
                &[&["0"]],
            )?;
            expect_rows(bench_pg_simple_query(&mut writer, "COMMIT;")?, &[])?;
            let _ = bench_pg_close(&mut writer);
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT COUNT(*) FROM rc_visibility;",
            )?;
            expect_rows(bench_pg_simple_query(&mut client, "ROLLBACK;")?, &[])?;
            if rows == vec![vec!["0".to_string()]] {
                Ok("BicDB transaction snapshot did not observe the concurrent commit; PostgreSQL READ COMMITTED would see it on the next statement".to_string())
            } else {
                anyhow::bail!("expected BicDB snapshot count 0 for documented READ COMMITTED gap, got {}", format_rows(&rows))
            }
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.ready_for_query_transaction_status",
        "protocol",
        "ReadyForQuery reports idle, transaction, and failed-transaction status bytes",
        PostgresCompatExpectation::Works,
        || {
            let create = bench_pg_simple_query_outcome(
                &mut client,
                "CREATE TABLE tx_status_probe (id TEXT PRIMARY KEY);",
            )?;
            if create.ready_status != b'I' {
                anyhow::bail!(
                    "expected idle status after CREATE, got {}",
                    create.ready_status as char
                );
            }
            let begin = bench_pg_simple_query_outcome(&mut client, "BEGIN;")?;
            if begin.ready_status != b'T' {
                anyhow::bail!(
                    "expected transaction status after BEGIN, got {}",
                    begin.ready_status as char
                );
            }
            let failed = bench_pg_simple_query_outcome(
                &mut client,
                "SELECT * FROM missing_tx_status_probe;",
            )?;
            if failed.ready_status != b'E' || failed.error.is_none() {
                anyhow::bail!("expected failed transaction status E with ErrorResponse");
            }
            let blocked = bench_pg_simple_query_outcome(&mut client, "SELECT 1;")?;
            if blocked.ready_status != b'E'
                || !blocked
                    .error
                    .as_deref()
                    .unwrap_or_default()
                    .contains("current transaction is aborted")
            {
                anyhow::bail!("expected PostgreSQL-shaped aborted transaction error");
            }
            let rollback = bench_pg_simple_query_outcome(&mut client, "ROLLBACK;")?;
            if rollback.ready_status != b'I' {
                anyhow::bail!(
                    "expected idle status after ROLLBACK, got {}",
                    rollback.ready_status as char
                );
            }
            Ok("observed ReadyForQuery I/T/E/I status transition, matching PostgreSQL 18.4 transaction status behavior".to_string())
        },
    );
    record_pg_case(
        &mut cases,
        "sql.inner_join_on",
        "sql",
        "INNER JOIN with an ON predicate returns joined rows",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT);",
                )?,
                &[],
            )?;
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p4', 'Dr. Kim');",
                )?,
                &[],
            )?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT patients.name, appointments.doctor FROM patients JOIN appointments ON patients.id = appointments.patient_id WHERE appointments.doctor = 'Dr. Rao';",
            )?;
            expect_rows(rows, &[&["John", "Dr. Rao"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.left_join_null_extension",
        "sql",
        "LEFT JOIN preserves unmatched left rows with NULL-extended right columns",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "INSERT INTO patients (id, name, age) VALUES ('p5', 'No Appointment', 29);",
                )?,
                &[],
            )?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT patients.name, appointments.doctor FROM patients LEFT JOIN appointments ON patients.id = appointments.patient_id WHERE appointments.id IS NULL ORDER BY patients.id;",
            )?;
            expect_rows(rows, &[&["No Appointment", ""]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.right_join_null_extension",
        "sql",
        "RIGHT JOIN currently over-includes null-extended right rows",
        PostgresCompatExpectation::ExpectedDifference,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a3', 'missing', 'Dr. Null');",
                )?,
                &[],
            )?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT patients.name, appointments.doctor FROM patients RIGHT JOIN appointments ON patients.id = appointments.patient_id WHERE patients.id IS NULL ORDER BY appointments.id;",
            )?;
            if rows
                == vec![
                    vec!["".to_string(), "Dr. Rao".to_string()],
                    vec!["".to_string(), "Dr. Kim".to_string()],
                    vec!["".to_string(), "Dr. Null".to_string()],
                ]
            {
                Ok("BicDB returned the unmatched right row plus matched right rows with NULL-extended left columns; PostgreSQL would return only Dr. Null".to_string())
            } else {
                anyhow::bail!(
                    "unexpected RIGHT JOIN expected-difference rows {}",
                    format_rows(&rows)
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "sql.full_outer_join_null_extension",
        "sql",
        "FULL OUTER JOIN preserves unmatched rows from both sides",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT patients.id, appointments.id FROM patients FULL OUTER JOIN appointments ON patients.id = appointments.patient_id WHERE patients.id IS NULL OR appointments.id IS NULL ORDER BY patients.id, appointments.id;",
            )?;
            expect_rows(rows, &[&["p5", ""], &["", "a3"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.group_by_count",
        "sql",
        "GROUP BY with COUNT aggregates rows by a field",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT doctor, COUNT(*) FROM appointments GROUP BY doctor ORDER BY doctor;",
            )?;
            expect_rows(
                rows,
                &[&["Dr. Kim", "1"], &["Dr. Null", "1"], &["Dr. Rao", "1"]],
            )
        },
    );
    record_pg_case(
        &mut cases,
        "sql.in_subquery",
        "sql",
        "IN subqueries can filter outer rows",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT id FROM patients WHERE id IN (SELECT patient_id FROM appointments WHERE doctor = 'Dr. Rao');",
            )?;
            expect_rows(rows, &[&["p1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.scalar_subquery",
        "sql",
        "Scalar subqueries can be used in comparisons",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT name FROM patients WHERE id = (SELECT patient_id FROM appointments WHERE id = 'a2');",
            )?;
            expect_rows(rows, &[&["Committed"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.expression_arithmetic_case_coalesce",
        "sql",
        "Arithmetic, concatenation, CASE, COALESCE, and casts work in SELECT projections",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT id, age + 4 AS age_plus, name || '-x' AS label, CASE WHEN age >= 40 THEN 'senior' ELSE 'adult' END AS band, COALESCE(age::text, 'n/a') AS age_text FROM patients WHERE id = 'p1';",
            )?;
            expect_rows(rows, &[&["p1", "50", "John-x", "senior", "46"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.expression_boolean_null_like_in",
        "sql",
        "Boolean logic, NULL comparisons, LIKE/ILIKE, and IN list filters work",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT id FROM patients WHERE (age > 40 OR age = NULL) AND name ILIKE 'jo%' AND id IN ('p1', 'p5') ORDER BY id;",
            )?;
            expect_rows(rows, &[&["p1"]])?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT id FROM patients WHERE name LIKE 'No%' ORDER BY id;",
            )?;
            expect_rows(rows, &[&["p5"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.orm_expression_regressions",
        "sql",
        "ORM-shaped aliases, casts, COALESCE, and IN expressions remain compatible",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT patients.id AS \"patients_id\", COALESCE(patients.name, '') AS \"patients_name\" FROM patients WHERE patients.id IN ('p1', 'p4') ORDER BY patients.id;",
            )?;
            expect_rows(rows, &[&["p1", "John"], &["p4", "Committed"]])
        },
    );
    record_pg_case(
        &mut cases,
        "sql.explain_estimated_cost",
        "sql",
        "EXPLAIN includes estimated rows and cost for the chosen plan",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "EXPLAIN SELECT id FROM patients WHERE age = 46;",
            )?;
            let has_index = rows.iter().any(|row| {
                row.first()
                    .is_some_and(|cell| cell.contains("IndexScan idx_patients_age"))
            });
            let has_rows = rows.iter().any(|row| {
                row.first()
                    .is_some_and(|cell| cell.starts_with("EstimatedRows "))
            });
            let has_cost = rows
                .iter()
                .any(|row| row.first().is_some_and(|cell| cell.starts_with("Cost ")));
            if has_index && has_rows && has_cost {
                Ok(format!("returned {}", format_rows(&rows)))
            } else {
                anyhow::bail!(
                    "expected IndexScan, EstimatedRows, and Cost, got {}",
                    format_rows(&rows)
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.information_schema_tables",
        "catalog",
        "information_schema.tables exposes user collections",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'patients';",
            )?;
            expect_rows(rows, &[&["patients"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.information_schema_columns",
        "catalog",
        "information_schema.columns exposes table columns",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'patients' AND column_name = 'name';",
            )?;
            expect_rows(rows, &[&["name"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_database_common_columns",
        "catalog",
        "pg_catalog.pg_database exposes common database metadata columns",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT datname, datallowconn, datconnlimit FROM pg_catalog.pg_database;",
            )?;
            expect_rows(rows, &[&["bicdb", "t", "-1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_namespace_common_columns",
        "catalog",
        "pg_catalog.pg_namespace exposes public schema metadata",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT nspname, nspowner FROM pg_catalog.pg_namespace WHERE nspname = 'public';",
            )?;
            expect_rows(rows, &[&["public", "10"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_type_jsonb",
        "catalog",
        "pg_catalog.pg_type exposes common PostgreSQL type OIDs",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT oid FROM pg_catalog.pg_type WHERE typname = 'jsonb';",
            )?;
            expect_rows(rows, &[&["3802"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_type_common_columns",
        "catalog",
        "pg_catalog.pg_type exposes type namespace, kind, and input function",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT typname, typnamespace, typtype, typinput FROM pg_catalog.pg_type WHERE typname = 'jsonb';",
            )?;
            expect_rows(rows, &[&["jsonb", "11", "b", "jsonb_in"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_class_table",
        "catalog",
        "pg_catalog.pg_class exposes table names for introspection",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'patients';",
            )?;
            expect_rows(rows, &[&["patients"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_class_common_columns",
        "catalog",
        "pg_catalog.pg_class exposes table relation kind, attribute count, and index flag",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT relname, relkind, relnatts, relhasindex, relpersistence FROM pg_catalog.pg_class WHERE relname = 'patients';",
            )?;
            expect_rows(rows, &[&["patients", "r", "4", "t", "p"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_attribute_common_columns",
        "catalog",
        "pg_catalog.pg_attribute exposes column number, type OID, nullability, and dropped flag",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT attname, attnum, attnotnull, atttypid, attisdropped FROM pg_catalog.pg_attribute WHERE attrelid = 'patients'::regclass AND attname = 'id';",
            )?;
            expect_rows(rows, &[&["id", "1", "t", "25", "f"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_index_primary_key",
        "catalog",
        "pg_catalog.pg_index exposes primary-key index metadata",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT indisprimary, indisunique, indisvalid, indkey FROM pg_catalog.pg_index WHERE indrelid = 'patients'::regclass AND indisprimary = true;",
            )?;
            expect_rows(rows, &[&["t", "t", "t", "1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_index_secondary",
        "catalog",
        "pg_catalog.pg_index exposes secondary-index key columns",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT indisprimary, indisunique, indisvalid, indkey FROM pg_catalog.pg_index WHERE indisprimary = false;",
            )?;
            expect_rows(rows, &[&["f", "f", "t", "3"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.pg_constraint_primary_key",
        "catalog",
        "pg_catalog.pg_constraint exposes primary-key constraints",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT conname, contype, convalidated FROM pg_catalog.pg_constraint WHERE conname = 'patients_pkey';",
            )?;
            expect_rows(rows, &[&["patients_pkey", "p", "t"]])
        },
    );
    record_pg_case(
        &mut cases,
        "catalog.client_introspection_helpers",
        "catalog",
        "pg_catalog exposes ORM and GUI client helper tables and functions",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT attname, format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute WHERE attrelid = 'patients'::regclass AND attname = 'name';",
            )?;
            expect_rows(rows, &[&["name", "text"]])?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT relname, pg_table_is_visible(oid), pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE relname = 'patients';",
            )?;
            expect_rows(rows, &[&["patients", "t", "bicdb"]])?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT objoid, description FROM pg_catalog.pg_description WHERE objoid = 0;",
            )?;
            expect_rows(rows, &[])?;
            Ok("client introspection helper catalog queries returned honest rows".to_string())
        },
    );
    record_pg_case(
        &mut cases,
        "types.cast_int4",
        "types",
        "PostgreSQL cast syntax supports int4",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT '123'::int4;")?;
            expect_rows(rows, &[&["123"]])
        },
    );
    record_pg_case(
        &mut cases,
        "types.cast_jsonb",
        "types",
        "PostgreSQL cast syntax supports jsonb",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT '{\"a\":1}'::jsonb;")?;
            expect_rows(rows, &[&["{\"a\": 1}"]])
        },
    );
    record_pg_case(
        &mut cases,
        "functions.now",
        "functions",
        "now() returns a timestamp-like value",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT now();")?;
            let value = rows
                .first()
                .and_then(|row| row.first())
                .ok_or_else(|| anyhow::anyhow!("now returned no rows"))?;
            if value.len() >= "YYYY-MM-DD HH:MM:SS".len()
                && value.as_bytes().get(4) == Some(&b'-')
                && value.as_bytes().get(7) == Some(&b'-')
                && value.as_bytes().get(10) == Some(&b' ')
                && value.as_bytes().get(13) == Some(&b':')
                && value.as_bytes().get(16) == Some(&b':')
            {
                Ok(format!("now returned timestamp-like value {value:?}"))
            } else {
                anyhow::bail!("now returned non-timestamp value {value:?}")
            }
        },
    );
    record_pg_case(
        &mut cases,
        "jsonb.text_arrow",
        "jsonb",
        "metadata->>'field' projects JSONB text",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT metadata->>'clinic' FROM patients WHERE id = 'p1';",
            )?;
            expect_rows(rows, &[&["rural-7"]])
        },
    );
    record_pg_case(
        &mut cases,
        "jsonb.contains",
        "jsonb",
        "metadata @> jsonb literal filters records",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT COUNT(*) FROM patients WHERE metadata @> '{\"clinic\":\"rural-7\"}'::jsonb;",
            )?;
            expect_rows(rows, &[&["1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "extended.parse_bind_execute",
        "protocol",
        "Extended Query Parse/Bind/Describe/Execute/Sync supports text parameters",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "CREATE TABLE prepared_patients (id TEXT PRIMARY KEY, name TEXT, age INT);",
                )?,
                &[],
            )?;
            expect_rows(
                bench_pg_extended_query(
                    &mut client,
                    "insert_prepared_patient",
                    "INSERT INTO prepared_patients (id, name, age) VALUES ($1, $2, $3)",
                    &[25, 25, 23],
                    &["pp1", "Prepared", "52"],
                )?,
                &[],
            )?;
            let rows = bench_pg_extended_query(
                &mut client,
                "select_prepared_patient",
                "SELECT name, age FROM prepared_patients WHERE id = $1",
                &[25],
                &["pp1"],
            )?;
            expect_rows(rows, &[&["Prepared", "52"]])
        },
    );
    record_pg_case(
        &mut cases,
        "extended.close_statement_and_portal",
        "protocol",
        "Extended Query Close returns CloseComplete for portal and prepared statement",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_extended_close_query(
                &mut client,
                "close_prepared_patient",
                "SELECT name FROM prepared_patients WHERE id = $1",
                &[25],
                &["pp1"],
            )?;
            expect_rows(rows, &[&["Prepared"]])
        },
    );
    record_pg_case(
        &mut cases,
        "extended.error_recovery_on_sync",
        "protocol",
        "Extended Query protocol errors return ErrorResponse and recover on Sync",
        PostgresCompatExpectation::Works,
        || {
            let error = bench_pg_extended_error_recovery(&mut client)?;
            if error.contains("ERROR") && error.contains("08P01") {
                Ok("protocol error returned ErrorResponse and SELECT 1 succeeded after Sync recovery".to_string())
            } else {
                anyhow::bail!("expected PostgreSQL-shaped ErrorResponse, got {error:?}")
            }
        },
    );
    record_pg_case(
        &mut cases,
        "extended.pipeline.prepared_query",
        "protocol",
        "Pipelined Extended Query messages can run multiple prepared flows before Sync",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_extended_pipeline_query(&mut client)?;
            expect_rows(rows, &[&["Pipelined"]])
        },
    );
    record_pg_case(
        &mut cases,
        "protocol.cancel_request.pg_sleep",
        "protocol",
        "CancelRequest interrupts a cancellable long-running query and the connection recovers",
        PostgresCompatExpectation::Works,
        || bench_pg_cancel_request_smoke(address),
    );
    record_pg_case(
        &mut cases,
        "vector.pgvector_ordering",
        "vector",
        "pgvector-style ORDER BY embedding <=> literal works through SQL",
        PostgresCompatExpectation::Works,
        || {
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "CREATE TABLE memories (id TEXT PRIMARY KEY, content TEXT, embedding VECTOR(3));",
                )?,
                &[],
            )?;
            expect_rows(
                bench_pg_simple_query(
                    &mut client,
                    "INSERT INTO memories (id, content, embedding) VALUES ('m1', 'near', '[1,0,0]'), ('m2', 'far', '[0,1,0]');",
                )?,
                &[],
            )?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT id FROM memories ORDER BY embedding <=> '[1,0,0]' LIMIT 1;",
            )?;
            expect_rows(rows, &[&["m1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "server.status_tables",
        "server",
        "BicDB server status virtual tables are queryable",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(&mut client, "SELECT * FROM bicdb_server_stats;")?;
            if rows.len() == 1 {
                Ok(format!("returned one stats row: {:?}", rows[0]))
            } else {
                anyhow::bail!("expected one stats row, got {}", format_rows(&rows))
            }
        },
    );
    record_pg_case(
        &mut cases,
        "guardrails.unsupported_copy_binary_error",
        "guardrails",
        "Unsupported COPY BINARY returns a clear PostgreSQL-shaped error",
        PostgresCompatExpectation::ClearUnsupportedError,
        || {
            let error =
                bench_pg_simple_query_error(&mut client, "COPY patients TO STDOUT BINARY;")?;
            if error.contains("ERROR")
                && error.contains("XX000")
                && error.contains("COPY BINARY is not supported")
            {
                Ok(format!("clear error: {}", compact_error(&error)))
            } else {
                anyhow::bail!("expected clear COPY BINARY error, got {error:?}")
            }
        },
    );
    record_pg_case(
        &mut cases,
        "guardrails.unsupported_recursive_cte_error",
        "guardrails",
        "Simple recursive CTE executes",
        PostgresCompatExpectation::Works,
        || {
            let rows = bench_pg_simple_query(
                &mut client,
                "WITH RECURSIVE nums(n) AS (SELECT 1) SELECT n FROM nums;",
            )?;
            expect_rows(rows, &[&["1"]])
        },
    );
    record_pg_case(
        &mut cases,
        "guardrails.unsupported_repeatable_read_error",
        "guardrails",
        "Writable REPEATABLE READ isolation returns a clear PostgreSQL-shaped error",
        PostgresCompatExpectation::ClearUnsupportedError,
        || {
            let error =
                bench_pg_simple_query_error(&mut client, "BEGIN ISOLATION LEVEL REPEATABLE READ;")?;
            if error.contains("ERROR")
                && error.contains("0A000")
                && error.contains("transaction isolation level REPEATABLE READ requires READ ONLY")
            {
                Ok(format!("clear error: {}", compact_error(&error)))
            } else {
                anyhow::bail!(
                    "expected clear writable REPEATABLE READ isolation error, got {error:?}"
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "guardrails.unsupported_serializable_error",
        "guardrails",
        "Unsupported SERIALIZABLE isolation returns a clear PostgreSQL-shaped error",
        PostgresCompatExpectation::ClearUnsupportedError,
        || {
            let error = bench_pg_simple_query_error(
                &mut client,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
            )?;
            if error.contains("ERROR")
                && error.contains("0A000")
                && error.contains("transaction isolation level SERIALIZABLE is not supported")
            {
                Ok(format!("clear error: {}", compact_error(&error)))
            } else {
                anyhow::bail!("expected clear SERIALIZABLE isolation error, got {error:?}")
            }
        },
    );
    for (id, sql, function, expected) in [
        (
            "window.row_number",
            "SELECT ROW_NUMBER() OVER (ORDER BY 1);",
            "ROW_NUMBER",
            "1",
        ),
        (
            "window.rank",
            "SELECT RANK() OVER (ORDER BY 1);",
            "RANK",
            "1",
        ),
        ("window.lag", "SELECT LAG(1) OVER (ORDER BY 1);", "LAG", ""),
        (
            "window.lead",
            "SELECT LEAD(1) OVER (ORDER BY 1);",
            "LEAD",
            "",
        ),
    ] {
        record_pg_case(
            &mut cases,
            id,
            "window_functions",
            &format!("{function} window function executes"),
            PostgresCompatExpectation::Works,
            || expect_rows(bench_pg_simple_query(&mut client, sql)?, &[&[expected]]),
        );
    }
    record_pg_case(
        &mut cases,
        "guardrails.unsupported_similar_to_expression_error",
        "guardrails",
        "Unsupported SIMILAR TO expression returns a clear PostgreSQL-shaped error",
        PostgresCompatExpectation::ClearUnsupportedError,
        || {
            let error = bench_pg_simple_query_error(
                &mut client,
                "SELECT id FROM patients WHERE id SIMILAR TO 'p[0-9]';",
            )?;
            if error.contains("ERROR")
                && error.contains("0A000")
                && error.contains("SIMILAR TO is not supported")
            {
                Ok(format!("clear error: {}", compact_error(&error)))
            } else {
                anyhow::bail!("expected clear SIMILAR TO unsupported error, got {error:?}")
            }
        },
    );
    record_pg_case(
        &mut cases,
        "procedural.supported.sql_routine_metadata",
        "procedural",
        "SQL-language function/procedure DDL stores pg_proc metadata",
        PostgresCompatExpectation::Works,
        || {
            let _ = bench_pg_simple_query(&mut client, "DROP PROCEDURE IF EXISTS score_proc();");
            let _ = bench_pg_simple_query(&mut client, "DROP FUNCTION IF EXISTS score_fn();");
            bench_pg_simple_query(
                &mut client,
                "CREATE FUNCTION score_fn() RETURNS INT LANGUAGE SQL AS 'SELECT 42';",
            )?;
            let function_rows = bench_pg_simple_query(
                &mut client,
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'score_fn';",
            )?;
            bench_pg_simple_query(
                &mut client,
                "CREATE PROCEDURE score_proc() LANGUAGE SQL AS 'SELECT 1';",
            )?;
            let procedure_rows = bench_pg_simple_query(
                &mut client,
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'score_proc';",
            )?;
            bench_pg_simple_query(&mut client, "DROP PROCEDURE score_proc();")?;
            bench_pg_simple_query(&mut client, "DROP FUNCTION score_fn();")?;
            if function_rows == vec![vec!["score_fn".to_string(), "f".to_string()]]
                && procedure_rows == vec![vec!["score_proc".to_string(), "p".to_string()]]
            {
                Ok("SQL routine metadata is visible in pg_proc".to_string())
            } else {
                anyhow::bail!(
                    "unexpected routine catalog rows function={function_rows:?} procedure={procedure_rows:?}"
                )
            }
        },
    );
    record_pg_case(
        &mut cases,
        "procedural.supported.plpgsql_metadata",
        "procedural",
        "PL/pgSQL function DDL is accepted as metadata-only pg_proc catalog state",
        PostgresCompatExpectation::Works,
        || {
            let _ = bench_pg_simple_query(&mut client, "DROP FUNCTION IF EXISTS score_pl();");
            bench_pg_simple_query(
                &mut client,
                "CREATE FUNCTION score_pl() RETURNS INT LANGUAGE plpgsql AS 'BEGIN RETURN 1; END';",
            )?;
            let rows = bench_pg_simple_query(
                &mut client,
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'score_pl';",
            )?;
            bench_pg_simple_query(&mut client, "DROP FUNCTION score_pl();")?;
            if rows == vec![vec!["score_pl".to_string(), "f".to_string()]] {
                Ok("PL/pgSQL routine metadata is visible in pg_proc; execution remains unsupported".to_string())
            } else {
                anyhow::bail!("unexpected PL/pgSQL routine catalog rows {rows:?}")
            }
        },
    );
    record_pg_case(
        &mut cases,
        "procedural.call_execution",
        "procedural",
        "Procedures execute through CALL",
        // This case asserted the OPPOSITE until 1.0.201-beta: that CALL
        // returned SQLSTATE 0A000. CALL execution landed during the TPC-C
        // work, so the scorecard was reporting a capability BicDB has as one
        // it lacks. A compat report that understates the engine is as wrong
        // as one that overstates it.
        PostgresCompatExpectation::Works,
        || {
            let _ = bench_pg_simple_query(&mut client, "DROP PROCEDURE IF EXISTS score_call();");
            bench_pg_simple_query(
                &mut client,
                "CREATE PROCEDURE score_call() LANGUAGE SQL AS 'SELECT 1';",
            )?;
            let rows = bench_pg_simple_query(&mut client, "CALL score_call();")?;
            bench_pg_simple_query(&mut client, "DROP PROCEDURE score_call();")?;
            // A procedure returns no result set, exactly as in PostgreSQL.
            if rows.is_empty() {
                Ok("CALL executed the procedure and returned no rows".to_string())
            } else {
                anyhow::bail!("CALL returned unexpected rows {rows:?}")
            }
        },
    );
    add_pg_documented_case(
        &mut cases,
        "ctes.expected_difference.recursive_cte",
        "ctes",
        "Recursive CTEs intentionally differ from PostgreSQL until execution support exists",
        PostgresCompatExpectation::ExpectedDifference,
        PostgresCompatCoverageState::ExpectedDifference,
        "covered by fixtures/postgres-compat/010-expected-difference-recursive-cte.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "ctes.supported.non_recursive_select",
        "ctes",
        "Non-recursive SELECT CTE behavior is covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/003-expected-difference-cte.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "alter_table.supported.common_migrations",
        "alter_table",
        "Common ALTER TABLE migration operations are covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/008-alter-table-migrations.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "constraints.supported.common_constraints_and_foreign_keys",
        "constraints",
        "Common NOT NULL, UNIQUE, CHECK, and FOREIGN KEY behavior is covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/004-constraints-foreign-keys.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "joins.supported.outer_joins",
        "joins",
        "LEFT, RIGHT, and FULL OUTER JOIN NULL-extension behavior is covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/009-outer-joins.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "expressions.supported.baseline",
        "expressions",
        "Arithmetic, boolean NULL behavior, CASE, COALESCE, LIKE/ILIKE, IN, concatenation, and common casts are covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/023-expressions-operators-casts.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "expressions.expected_difference.unsupported_similar_to",
        "expressions",
        "Unsupported SIMILAR TO expression behavior is covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::ExpectedDifference,
        PostgresCompatCoverageState::ExpectedDifference,
        "covered by fixtures/postgres-compat/024-expected-difference-unsupported-expressions.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "views.supported.logical_views",
        "views",
        "Logical CREATE VIEW, SELECT from views, DROP VIEW, and view catalog introspection are covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/012-views.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "procedural.supported.sql_routine_ddl_fixture",
        "procedural",
        "SQL-language CREATE/DROP FUNCTION and PROCEDURE catalog behavior is covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/020-procedural-ddl.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "procedural.expected_difference.plpgsql_functions",
        "procedural",
        "General PL/pgSQL function execution remains an expected difference",
        PostgresCompatExpectation::ExpectedDifference,
        PostgresCompatCoverageState::ExpectedDifference,
        "covered by fixtures/postgres-compat/021-expected-difference-procedural-languages.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "errors.supported.common_sqlstates",
        "errors",
        "Common syntax, type, and constraint SQLSTATE compatibility is covered by PostgreSQL diff fixtures",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/017-error-syntax-sqlstate.json, 018-error-type-sqlstate.json, and 019-error-constraint-sqlstate.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "window_functions.supported.over",
        "window_functions",
        "Ranking, aggregate, offset/value, partitioned, ordered, and ROWS-framed windows execute",
        PostgresCompatExpectation::Works,
        PostgresCompatCoverageState::Supported,
        "covered by fixtures/postgres-compat/011-window-functions.json in compat diff",
    );
    add_pg_documented_case(
        &mut cases,
        "copy.unsupported.binary_and_advanced_options",
        "copy",
        "Binary COPY and advanced COPY options are unsupported",
        PostgresCompatExpectation::ClearUnsupportedError,
        PostgresCompatCoverageState::Unsupported,
        "basic text/CSV COPY is executable in this scorecard; binary COPY is explicitly rejected and broader COPY option parity is not implemented",
    );
    add_pg_documented_case(
        &mut cases,
        "views.unsupported.updatable_materialized_views",
        "views",
        "Updatable and materialized views are unsupported",
        PostgresCompatExpectation::ClearUnsupportedError,
        PostgresCompatCoverageState::Unsupported,
        "simple logical views are covered; PostgreSQL updatable/materialized view semantics are not implemented",
    );
    add_pg_documented_case(
        &mut cases,
        "alter_table.not_yet_tested.exhaustive_parity",
        "alter_table",
        "Exhaustive ALTER TABLE parity is not yet tested",
        PostgresCompatExpectation::NotYetTested,
        PostgresCompatCoverageState::NotYetTested,
        "common migration-style ALTER TABLE is covered by compat diff; exhaustive PostgreSQL ALTER TABLE behavior is outside this suite",
    );
    add_pg_documented_case(
        &mut cases,
        "sequences.not_yet_tested.advanced_semantics",
        "sequences",
        "Advanced sequence ownership, permissions, and catalog parity are not yet tested",
        PostgresCompatExpectation::NotYetTested,
        PostgresCompatCoverageState::NotYetTested,
        "basic sequences are documented; advanced PostgreSQL sequence semantics need focused scorecard cases",
    );
    add_pg_documented_case(
        &mut cases,
        "catalog.not_yet_tested.full_pg_catalog",
        "catalog",
        "Complete pg_catalog parity is not yet tested",
        PostgresCompatExpectation::NotYetTested,
        PostgresCompatCoverageState::NotYetTested,
        "the scorecard covers practical client introspection rows, not every PostgreSQL catalog table or storage column",
    );
    add_pg_documented_case(
        &mut cases,
        "auth_tls.unsupported.strict_policy",
        "auth_tls",
        "Client certificates, channel binding, and full auth policy parity are unsupported",
        PostgresCompatExpectation::ClearUnsupportedError,
        PostgresCompatCoverageState::Unsupported,
        "password auth, SCRAM-SHA-256, TLS listener support, and TLS-required plaintext startup rejection exist outside this scorecard; client certificate and SCRAM channel binding support are not implemented",
    );
    add_pg_documented_case(
        &mut cases,
        "client_matrix.not_yet_tested.gui_automation",
        "client_matrix",
        "GUI clients and Prisma introspection are not fully automated in the scorecard",
        PostgresCompatExpectation::NotYetTested,
        PostgresCompatCoverageState::NotYetTested,
        "node-postgres, psycopg, SQLAlchemy, and tokio-postgres have gauntlet coverage; Prisma and GUI clients remain reproducible/manual flows",
    );

    let _ = bench_pg_close(&mut client);
    server.request_shutdown();
    server_thread
        .join()
        .map_err(|_| anyhow::anyhow!("compatibility listener thread panicked"))??;
    drop(server);

    let database_size_bytes = BicDb::open(&path)?.stats()?.size_bytes;
    let total_cases = cases
        .iter()
        .filter(|case| case.status != PostgresCompatStatus::NotRun)
        .count();
    let passed_cases = cases
        .iter()
        .filter(|case| case.status == PostgresCompatStatus::Passed)
        .count();
    let failed_cases = total_cases.saturating_sub(passed_cases);
    let clear_unsupported_cases = cases
        .iter()
        .filter(|case| {
            case.status == PostgresCompatStatus::Passed
                && case.expectation == PostgresCompatExpectation::ClearUnsupportedError
        })
        .count();
    let score_percent = if total_cases == 0 {
        0.0
    } else {
        passed_cases as f64 * 100.0 / total_cases as f64
    };
    let category_scores = postgres_compat_category_scores(&cases);

    Ok(PostgresCompatReport {
        mode: "postgres_compat",
        target_version,
        protocol_target: "PostgreSQL 18 protocol family, including protocol 3.2 startup acceptance and NegotiateProtocolVersion option probes".to_string(),
        protocol_supported: "BicDB accepts PostgreSQL protocol 3.0 and 3.2 startup packets, sends NegotiateProtocolVersion for unsupported _pq_. options, then uses simple query, basic COPY text/CSV, extended query flows, common pipelined prepared flows, and CancelRequest for cancellable paths".to_string(),
        total_cases,
        passed_cases,
        failed_cases,
        clear_unsupported_cases,
        score_percent,
        score_interpretation: "Score is the pass rate for BicDB's executable PostgreSQL compatibility suite only; 100% here does not mean complete PostgreSQL equivalence unless the suite itself covers that breadth.".to_string(),
        database_size_bytes,
        path,
        category_scores,
        cases,
        known_gaps: postgres_compat_known_gaps(),
    })
}

pub fn run_postgres_diff_suite(
    path: impl AsRef<Path>,
    config: PostgresDiffConfig,
) -> Result<PostgresDiffReport> {
    let started = Instant::now();
    let path = path.as_ref().to_path_buf();
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path)?;

    let fixtures = load_postgres_diff_fixtures(config.fixtures_dir.as_deref())?;

    let listener = TcpListener::bind("127.0.0.1:0").context("bind BicDB diff pgwire listener")?;
    let address = listener.local_addr()?;
    let server = PgWireServer::open(
        &path,
        PgWireConfig {
            host: "127.0.0.1".to_string(),
            port: address.port(),
            max_connections: 8,
            flush_interval: Duration::from_secs(3_600),
            checkpoint_interval: Duration::from_secs(3_600),
            metrics_interval: Duration::from_secs(3_600),
            ..PgWireConfig::default()
        },
    )?;
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let pg_conn = format!(
        "host={} port={} dbname={} user={} password={}",
        config.pg_host, config.pg_port, config.pg_database, config.pg_user, config.pg_password
    );
    let mut pg_client = PostgresClient::connect(&pg_conn, NoTls).with_context(|| {
        format!(
            "connect PostgreSQL {} target at {}:{}/{}",
            config.target_version, config.pg_host, config.pg_port, config.pg_database
        )
    })?;
    let mut bicdb_client = bench_pg_connect(address)?;

    let mut cases = Vec::new();
    for fixture in fixtures {
        let case_started = Instant::now();
        let pg = run_postgres_fixture(&mut pg_client, &fixture);
        let bicdb = run_bicdb_fixture(&mut bicdb_client, &fixture);
        let elapsed_ms = duration_ms(case_started.elapsed());
        cases.push(compare_postgres_diff_fixture(
            fixture, pg, bicdb, elapsed_ms,
        ));
    }

    let _ = bench_pg_close(&mut bicdb_client);
    server.request_shutdown();
    server_thread
        .join()
        .map_err(|_| anyhow::anyhow!("diff listener thread panicked"))??;

    let total_cases = cases.len();
    let passed_cases = cases
        .iter()
        .filter(|case| case.status == PostgresDiffStatus::Passed)
        .count();
    let failed_cases = total_cases.saturating_sub(passed_cases);
    let expected_difference_cases = cases
        .iter()
        .filter(|case| case.expectation == PostgresDiffExpectation::ExpectedDifference)
        .count();

    Ok(PostgresDiffReport {
        mode: "postgres_diff",
        target_version: config.target_version,
        postgres_host: config.pg_host,
        postgres_port: config.pg_port,
        total_cases,
        passed_cases,
        failed_cases,
        expected_difference_cases,
        elapsed_ms: duration_ms(started.elapsed()),
        path,
        fixtures_dir: config.fixtures_dir,
        cases,
    })
}

pub fn run_pg18_nightmare_suite(
    path: impl AsRef<Path>,
    mut config: PostgresDiffConfig,
) -> Result<PostgresDiffReport> {
    if config.fixtures_dir.is_none() {
        config.fixtures_dir = Some(PathBuf::from("fixtures/pg18-nightmare"));
    }
    let mut report = run_postgres_diff_suite(path, config)?;
    report.mode = "pg18_nightmare";
    Ok(report)
}

#[cfg(feature = "comparison-engines")]
pub fn run_insert_baseline_suite(
    path: impl AsRef<Path>,
    records: usize,
    batch_size: usize,
) -> Result<Vec<BaselineReport>> {
    let root = path.as_ref().to_path_buf();
    fs::create_dir_all(&root)?;

    let reports = vec![
        match run_insert_bench(root.join("bicdb"), records, batch_size) {
            Ok(report) => BaselineReport {
                engine: BaselineEngine::BicDb,
                status: BaselineStatus::Completed,
                records,
                batch_size: batch_size.max(1),
                elapsed_ms: Some(duration_ms(report.elapsed)),
                records_per_sec: Some(report.records_per_sec),
                database_size_bytes: Some(report.database_size_bytes),
                message: "BicDB append-only segment baseline".to_string(),
                path: Some(report.path),
            },
            Err(error) => failed(BaselineEngine::BicDb, records, batch_size, error),
        },
        match run_redb_insert_baseline(&root.join("redb.redb"), records, batch_size) {
            Ok(report) => report,
            Err(error) => failed(BaselineEngine::Redb, records, batch_size, error),
        },
        match run_fjall_insert_baseline(&root.join("fjall"), records, batch_size) {
            Ok(report) => report,
            Err(error) => failed(BaselineEngine::Fjall, records, batch_size, error),
        },
        skipped(
            BaselineEngine::SqliteVec,
            records,
            batch_size,
            "sqlite-vec is tracked as an optional vector baseline; default v0.1 does not compile the SQLite extension harness.",
        ),
        skipped(
            BaselineEngine::LanceDb,
            records,
            batch_size,
            "LanceDB Rust baseline is documented but skipped in the default harness to avoid pulling the async Arrow stack into v0.1.",
        ),
        skipped(
            BaselineEngine::Qdrant,
            records,
            batch_size,
            "Qdrant requires a running external server; pass a server-specific harness in a future benchmark profile.",
        ),
    ];

    Ok(reports)
}

#[cfg(feature = "comparison-engines")]
fn run_redb_insert_baseline(
    path: &Path,
    records: usize,
    batch_size: usize,
) -> Result<BaselineReport> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _ = fs::remove_file(path);
    let db = Database::create(path).context("create redb database")?;
    let batch_size = batch_size.max(1);
    let started = Instant::now();

    let mut idx = 0;
    while idx < records {
        let write_txn = db.begin_write().context("begin redb write transaction")?;
        {
            let mut table = write_txn
                .open_table(REDB_TABLE)
                .context("open redb table")?;
            let end = (idx + batch_size).min(records);
            while idx < end {
                let key = format!("record-{idx}");
                let value = record_payload(idx)?;
                table
                    .insert(key.as_str(), value.as_slice())
                    .context("insert redb record")?;
                idx += 1;
            }
        }
        write_txn
            .commit()
            .context("commit redb write transaction")?;
    }

    let elapsed = started.elapsed();
    let database_size_bytes = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Ok(BaselineReport {
        engine: BaselineEngine::Redb,
        status: BaselineStatus::Completed,
        records,
        batch_size,
        elapsed_ms: Some(duration_ms(elapsed)),
        records_per_sec: Some(throughput(records, elapsed)),
        database_size_bytes: Some(database_size_bytes),
        message: "redb embedded copy-on-write B-tree baseline".to_string(),
        path: Some(path.to_path_buf()),
    })
}

#[cfg(feature = "comparison-engines")]
fn run_fjall_insert_baseline(
    path: &Path,
    records: usize,
    batch_size: usize,
) -> Result<BaselineReport> {
    let _ = fs::remove_dir_all(path);
    fs::create_dir_all(path)?;
    let keyspace = fjall::Config::new(path)
        .open()
        .context("open fjall keyspace")?;
    let items = keyspace
        .open_partition("records", fjall::PartitionCreateOptions::default())
        .context("open fjall partition")?;
    let batch_size = batch_size.max(1);
    let started = Instant::now();

    let mut pending = 0;
    for idx in 0..records {
        let key = format!("record-{idx}");
        let value = record_payload(idx)?;
        items
            .insert(key.as_bytes(), value)
            .context("insert fjall record")?;
        pending += 1;
        if pending >= batch_size {
            keyspace
                .persist(fjall::PersistMode::SyncAll)
                .context("persist fjall batch")?;
            pending = 0;
        }
    }
    keyspace
        .persist(fjall::PersistMode::SyncAll)
        .context("persist fjall final batch")?;

    let elapsed = started.elapsed();
    Ok(BaselineReport {
        engine: BaselineEngine::Fjall,
        status: BaselineStatus::Completed,
        records,
        batch_size,
        elapsed_ms: Some(duration_ms(elapsed)),
        records_per_sec: Some(throughput(records, elapsed)),
        database_size_bytes: Some(path_size(path)?),
        message: "fjall embedded LSM-tree baseline".to_string(),
        path: Some(path.to_path_buf()),
    })
}

impl InsertBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&json!({
            "mode": self.mode,
            "records": self.records,
            "batch_size": self.batch_size,
            "elapsed_ms": duration_ms(self.elapsed),
            "records_per_sec": self.records_per_sec,
            "database_size_bytes": self.database_size_bytes,
            "collection_sizes": self.collection_sizes,
            "startup_recovery_ms": duration_ms(self.startup_recovery_time),
            "path": self.path,
        }))
        .map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,batch_size,elapsed_ms,records_per_sec,database_size_bytes,startup_recovery_ms,path\n{},{},{},{:.3},{:.2},{},{:.3},{}\n",
            self.mode,
            self.records,
            self.batch_size,
            duration_ms(self.elapsed),
            self.records_per_sec,
            self.database_size_bytes,
            duration_ms(self.startup_recovery_time),
            self.path.display()
        )
    }
}

impl VectorBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&json!({
            "mode": self.mode,
            "records": self.records,
            "dim": self.dim,
            "top_k": self.top_k,
            "insert_elapsed_ms": duration_ms(self.insert_elapsed),
            "search_p50_ms": duration_ms(self.search_p50),
            "search_p95_ms": duration_ms(self.search_p95),
            "search_p99_ms": duration_ms(self.search_p99),
            "database_size_bytes": self.database_size_bytes,
            "collection_sizes": self.collection_sizes,
            "startup_recovery_ms": duration_ms(self.startup_recovery_time),
            "path": self.path,
        }))
        .map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,dim,top_k,insert_elapsed_ms,search_p50_ms,search_p95_ms,search_p99_ms,database_size_bytes,startup_recovery_ms,path\n{},{},{},{},{:.3},{:.3},{:.3},{:.3},{},{:.3},{}\n",
            self.mode,
            self.records,
            self.dim,
            self.top_k,
            duration_ms(self.insert_elapsed),
            duration_ms(self.search_p50),
            duration_ms(self.search_p95),
            duration_ms(self.search_p99),
            self.database_size_bytes,
            duration_ms(self.startup_recovery_time),
            self.path.display()
        )
    }
}

impl VectorProfileReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,strategy,records,dim,top_k,searches,metric,insert_elapsed_ms,total_p50_ms,total_p95_ms,total_p99_ms,avg_read_vectors_ms,avg_similarity_ms,avg_top_k_heap_ms,avg_final_sort_ms,candidates_scanned,vectors_read,allocation_count_total,allocation_count_per_search,allocated_bytes_total,allocated_bytes_per_search,allocation_bytes_max,result_count,database_size_bytes,path\n{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.6},{},{},{},{:.2},{},{:.2},{},{},{},{}\n",
            self.mode,
            self.strategy,
            self.records,
            self.dim,
            self.top_k,
            self.searches,
            self.metric,
            self.insert_elapsed_ms,
            self.total_p50_ms,
            self.total_p95_ms,
            self.total_p99_ms,
            self.avg_read_vectors_ms,
            self.avg_similarity_ms,
            self.avg_top_k_heap_ms,
            self.avg_final_sort_ms,
            self.candidates_scanned,
            self.vectors_read,
            self.allocation_count_total,
            self.allocation_count_per_search,
            self.allocated_bytes_total,
            self.allocated_bytes_per_search,
            self.allocation_bytes_max,
            self.result_count,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl AnnBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,dim,top_k,searches,insert_elapsed_ms,index_build_ms,index_size_bytes,memory_estimate_bytes,exact_p50_ms,exact_p95_ms,exact_p99_ms,ann_ef20_p50_ms,ann_ef20_p95_ms,ann_ef20_p99_ms,ann_ef20_recall_at_k,ann_ef50_p50_ms,ann_ef50_p95_ms,ann_ef50_p99_ms,ann_ef50_recall_at_k,ann_ef100_p50_ms,ann_ef100_p95_ms,ann_ef100_p99_ms,ann_ef100_recall_at_k,database_size_bytes,path\n{},{},{},{},{},{:.3},{:.3},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.4},{:.3},{:.3},{:.3},{:.4},{:.3},{:.3},{:.3},{:.4},{},{}\n",
            self.mode,
            self.records,
            self.dim,
            self.top_k,
            self.searches,
            self.insert_elapsed_ms,
            self.index_build_ms,
            self.index_size_bytes,
            self.memory_estimate_bytes,
            self.exact_p50_ms,
            self.exact_p95_ms,
            self.exact_p99_ms,
            self.ann_ef20_p50_ms,
            self.ann_ef20_p95_ms,
            self.ann_ef20_p99_ms,
            self.ann_ef20_recall_at_k,
            self.ann_ef50_p50_ms,
            self.ann_ef50_p95_ms,
            self.ann_ef50_p99_ms,
            self.ann_ef50_recall_at_k,
            self.ann_ef100_p50_ms,
            self.ann_ef100_p95_ms,
            self.ann_ef100_p99_ms,
            self.ann_ef100_recall_at_k,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl GraphBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,entities,edges,insert_elapsed_ms,build_elapsed_ms,neighbor_lookup_ms,path_query_ms,edge_scan_ms,node_count,edge_count,graph_size_bytes,database_size_bytes,path\n{},{},{},{:.3},{:.3},{:.6},{:.6},{:.3},{},{},{},{},{}\n",
            self.mode,
            self.entities,
            self.edges,
            self.insert_elapsed_ms,
            self.build_elapsed_ms,
            self.neighbor_lookup_ms,
            self.path_query_ms,
            self.edge_scan_ms,
            self.node_count,
            self.edge_count,
            self.graph_size_bytes,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl SpatialBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,points,insert_elapsed_ms,points_per_sec,index_build_ms,index_size_bytes,database_size_bytes,path\n{},{},{:.3},{:.2},{:.3},{},{},{}\n",
            self.mode,
            self.points,
            self.insert_elapsed_ms,
            self.points_per_sec,
            self.index_build_ms,
            self.index_size_bytes,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl SpatialNearestBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,points,queries,nearest_p50_ms,nearest_p95_ms,nearest_p99_ms,radius_p50_ms,radius_p95_ms,radius_p99_ms,nearest_result_count,radius_result_count,radius_meters,database_size_bytes,path\n{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{:.3},{},{}\n",
            self.mode,
            self.points,
            self.queries,
            self.nearest_p50_ms,
            self.nearest_p95_ms,
            self.nearest_p99_ms,
            self.radius_p50_ms,
            self.radius_p95_ms,
            self.radius_p99_ms,
            self.nearest_result_count,
            self.radius_result_count,
            self.radius_meters,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl RouteBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,nodes,edges,insert_elapsed_ms,route_p50_ms,route_p95_ms,route_p99_ms,route_result_count,database_size_bytes,path\n{},{},{},{:.3},{:.3},{:.3},{:.3},{},{},{}\n",
            self.mode,
            self.nodes,
            self.edges,
            self.insert_elapsed_ms,
            self.route_p50_ms,
            self.route_p95_ms,
            self.route_p99_ms,
            self.route_result_count,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl EventBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,events,append_elapsed_ms,append_events_per_sec,subscriber_p50_ms,subscriber_p95_ms,subscriber_p99_ms,replay_elapsed_ms,replay_events_per_sec,replayed_events,database_size_bytes,path\n{},{},{:.3},{:.2},{:.6},{:.6},{:.6},{:.3},{:.2},{},{},{}\n",
            self.mode,
            self.events,
            self.append_elapsed_ms,
            self.append_events_per_sec,
            self.subscriber_p50_ms,
            self.subscriber_p95_ms,
            self.subscriber_p99_ms,
            self.replay_elapsed_ms,
            self.replay_events_per_sec,
            self.replayed_events,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl QueueBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,messages,consume_batch_size,publish_elapsed_ms,publish_messages_per_sec,consume_elapsed_ms,consume_messages_per_sec,consumed_messages,database_size_bytes,path\n{},{},{},{:.3},{:.2},{:.3},{:.2},{},{},{}\n",
            self.mode,
            self.messages,
            self.consume_batch_size,
            self.publish_elapsed_ms,
            self.publish_messages_per_sec,
            self.consume_elapsed_ms,
            self.consume_messages_per_sec,
            self.consumed_messages,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl ProjectionBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,events,entities,append_elapsed_ms,append_events_per_sec,rebuild_elapsed_ms,rebuild_events_per_sec,projected_entities,database_size_bytes,path\n{},{},{},{:.3},{:.2},{:.3},{:.2},{},{},{}\n",
            self.mode,
            self.events,
            self.entities,
            self.append_elapsed_ms,
            self.append_events_per_sec,
            self.rebuild_elapsed_ms,
            self.rebuild_events_per_sec,
            self.projected_entities,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl WearableBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&json!({
            "mode": self.mode,
            "devices": self.devices,
            "records": self.records,
            "insert_elapsed_ms": duration_ms(self.insert_elapsed),
            "records_per_sec": self.records_per_sec,
            "time_range_scan_ms": duration_ms(self.time_range_scan_elapsed),
            "scanned_records": self.scanned_records,
            "latest_lookup_ms": duration_ms(self.latest_lookup_elapsed),
            "summary_count": self.summary_count,
            "database_size_bytes": self.database_size_bytes,
            "collection_sizes": self.collection_sizes,
            "startup_recovery_ms": duration_ms(self.startup_recovery_time),
            "path": self.path,
        }))
        .map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,devices,records,insert_elapsed_ms,records_per_sec,time_range_scan_ms,scanned_records,latest_lookup_ms,summary_count,database_size_bytes,startup_recovery_ms,path\n{},{},{},{:.3},{:.2},{:.3},{},{:.3},{},{},{:.3},{}\n",
            self.mode,
            self.devices,
            self.records,
            duration_ms(self.insert_elapsed),
            self.records_per_sec,
            duration_ms(self.time_range_scan_elapsed),
            self.scanned_records,
            duration_ms(self.latest_lookup_elapsed),
            self.summary_count,
            self.database_size_bytes,
            duration_ms(self.startup_recovery_time),
            self.path.display()
        )
    }
}

impl SqlBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,insert_elapsed_ms,select_by_id_sql_ms,select_by_id_direct_ms,timestamp_range_sql_ms,timestamp_range_direct_ms,timestamp_range_rows,count_sql_ms,count_direct_ms,count_rows,avg_sql_ms,avg_direct_ms,avg_value,order_by_limit_sql_ms,order_by_limit_direct_ms,order_by_limit_rows,database_size_bytes,path\n{},{},{:.3},{:.6},{:.6},{:.3},{:.3},{},{:.3},{:.3},{},{:.3},{:.3},{:.6},{:.3},{:.3},{},{},{}\n",
            self.mode,
            self.records,
            self.insert_elapsed_ms,
            self.select_by_id_sql_ms,
            self.select_by_id_direct_ms,
            self.timestamp_range_sql_ms,
            self.timestamp_range_direct_ms,
            self.timestamp_range_rows,
            self.count_sql_ms,
            self.count_direct_ms,
            self.count_rows,
            self.avg_sql_ms,
            self.avg_direct_ms,
            self.avg_value,
            self.order_by_limit_sql_ms,
            self.order_by_limit_direct_ms,
            self.order_by_limit_rows,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl IndexBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,insert_without_indexes_ms,insert_with_indexes_ms,write_overhead_ms,index_build_ms,index_rebuild_ms,index_size_bytes,peak_memory_estimate_bytes,point_lookup_scan_ms,point_lookup_index_ms,point_lookup_rows,timestamp_range_scan_ms,timestamp_range_index_ms,timestamp_range_rows,metadata_equality_scan_ms,metadata_equality_index_ms,metadata_equality_rows,composite_lookup_scan_ms,composite_lookup_index_ms,composite_lookup_rows,order_by_scan_ms,order_by_index_ms,order_by_rows,database_size_bytes,path\n{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{:.3},{:.3},{},{:.3},{:.3},{},{:.3},{:.3},{},{:.3},{:.3},{},{:.3},{:.3},{},{},{}\n",
            self.mode,
            self.records,
            self.insert_without_indexes_ms,
            self.insert_with_indexes_ms,
            self.write_overhead_ms,
            self.index_build_ms,
            self.index_rebuild_ms,
            self.index_size_bytes,
            self.peak_memory_estimate_bytes,
            self.point_lookup_scan_ms,
            self.point_lookup_index_ms,
            self.point_lookup_rows,
            self.timestamp_range_scan_ms,
            self.timestamp_range_index_ms,
            self.timestamp_range_rows,
            self.metadata_equality_scan_ms,
            self.metadata_equality_index_ms,
            self.metadata_equality_rows,
            self.composite_lookup_scan_ms,
            self.composite_lookup_index_ms,
            self.composite_lookup_rows,
            self.order_by_scan_ms,
            self.order_by_index_ms,
            self.order_by_rows,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl TransactionBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,batch_size,single_insert_tx_ms,single_insert_tx_per_sec,batch_insert_tx_ms,batch_insert_tx_per_sec,rollback_ms,rollback_records_per_sec,recovery_ms,recovered_records,snapshot_scan_ms,snapshot_records,database_size_bytes,path\n{},{},{},{:.3},{:.2},{:.3},{:.2},{:.3},{:.2},{:.3},{},{:.3},{},{},{}\n",
            self.mode,
            self.records,
            self.batch_size,
            self.single_insert_tx_ms,
            self.single_insert_tx_per_sec,
            self.batch_insert_tx_ms,
            self.batch_insert_tx_per_sec,
            self.rollback_ms,
            self.rollback_records_per_sec,
            self.recovery_ms,
            self.recovered_records,
            self.snapshot_scan_ms,
            self.snapshot_records,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl AnalyticsBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records,insert_elapsed_ms,sidecar_rebuild_ms,sidecar_size_bytes,direct_count_ms,query_exec_count_ms,datafusion_count_p50_ms,datafusion_count_p95_ms,datafusion_count_p99_ms,direct_avg_ms,query_exec_avg_ms,datafusion_avg_p50_ms,datafusion_avg_p95_ms,datafusion_avg_p99_ms,direct_min_max_ms,query_exec_min_max_ms,datafusion_min_max_p50_ms,datafusion_min_max_p95_ms,datafusion_min_max_p99_ms,datafusion_group_by_metric_p50_ms,datafusion_group_by_metric_p95_ms,datafusion_group_by_metric_p99_ms,datafusion_group_by_device_p50_ms,datafusion_group_by_device_p95_ms,datafusion_group_by_device_p99_ms,datafusion_timestamp_range_p50_ms,datafusion_timestamp_range_p95_ms,datafusion_timestamp_range_p99_ms,datafusion_rows_per_sec,arrow_memory_bytes,database_size_bytes,path\n{},{},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.2},{},{},{}\n",
            self.mode,
            self.records,
            self.insert_elapsed_ms,
            self.sidecar_rebuild_ms,
            self.sidecar_size_bytes,
            self.direct_count_ms,
            self.query_exec_count_ms,
            self.datafusion_count_p50_ms,
            self.datafusion_count_p95_ms,
            self.datafusion_count_p99_ms,
            self.direct_avg_ms,
            self.query_exec_avg_ms,
            self.datafusion_avg_p50_ms,
            self.datafusion_avg_p95_ms,
            self.datafusion_avg_p99_ms,
            self.direct_min_max_ms,
            self.query_exec_min_max_ms,
            self.datafusion_min_max_p50_ms,
            self.datafusion_min_max_p95_ms,
            self.datafusion_min_max_p99_ms,
            self.datafusion_group_by_metric_p50_ms,
            self.datafusion_group_by_metric_p95_ms,
            self.datafusion_group_by_metric_p99_ms,
            self.datafusion_group_by_device_p50_ms,
            self.datafusion_group_by_device_p95_ms,
            self.datafusion_group_by_device_p99_ms,
            self.datafusion_timestamp_range_p50_ms,
            self.datafusion_timestamp_range_p95_ms,
            self.datafusion_timestamp_range_p99_ms,
            self.datafusion_rows_per_sec,
            self.arrow_memory_bytes,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl MemoryBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,memories,dim,top_k,insert_elapsed_ms,memories_per_sec,recall_p50_ms,recall_p95_ms,recall_p99_ms,ranking_memories_per_sec,recall_result_count,timeline_elapsed_ms,timeline_memories,workspace_load_ms,workspace_memories,memory_event_count,database_size_bytes,path\n{},{},{},{},{:.3},{:.2},{:.3},{:.3},{:.3},{:.2},{},{:.3},{},{:.3},{},{},{},{}\n",
            self.mode,
            self.memories,
            self.dim,
            self.top_k,
            self.insert_elapsed_ms,
            self.memories_per_sec,
            self.recall_p50_ms,
            self.recall_p95_ms,
            self.recall_p99_ms,
            self.ranking_memories_per_sec,
            self.recall_result_count,
            self.timeline_elapsed_ms,
            self.timeline_memories,
            self.workspace_load_ms,
            self.workspace_memories,
            self.memory_event_count,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl SyncBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,records_per_node,left_export_events,right_export_events,left_export_elapsed_ms,right_export_elapsed_ms,export_events_per_sec,right_import_elapsed_ms,left_import_elapsed_ms,import_events_per_sec,records_merged,merge_records_per_sec,conflicts_resolved,conflicts_per_sec,converged,audit_events,database_size_bytes,path\n{},{},{},{},{:.3},{:.3},{:.2},{:.3},{:.3},{:.2},{},{:.2},{},{:.2},{},{},{},{}\n",
            self.mode,
            self.records_per_node,
            self.left_export_events,
            self.right_export_events,
            self.left_export_elapsed_ms,
            self.right_export_elapsed_ms,
            self.export_events_per_sec,
            self.right_import_elapsed_ms,
            self.left_import_elapsed_ms,
            self.import_events_per_sec,
            self.records_merged,
            self.merge_records_per_sec,
            self.conflicts_resolved,
            self.conflicts_per_sec,
            self.converged,
            self.audit_events,
            self.database_size_bytes,
            self.path.display()
        )
    }
}

impl ServerBenchReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        format!(
            "mode,scenario,clients,active_query_concurrency,queries,select_queries,insert_queries,setup_latency_p50_ms,setup_latency_p95_ms,setup_latency_p99_ms,elapsed_ms,queries_per_sec,latency_p50_ms,latency_p95_ms,latency_p99_ms,active_connections_peak,rejected_connections,server_max_queued_queries,server_queued_queries_max,server_active_reads_peak,server_queued_reads_max,server_active_writes_peak,server_queued_writes_max,server_query_queue_wait_p50_ms,server_query_queue_wait_p95_ms,server_query_queue_wait_p99_ms,server_reported_queries,server_failed_queries,server_canceled_queries,server_timed_out_queries,server_writes_executed,server_max_queued_writes,server_write_queue_depth_max,server_write_wait_avg_ms,server_write_wait_max_ms,server_write_execution_avg_ms,server_write_execution_max_ms,server_write_rejected_count,server_write_timed_out_count,server_memory_estimate_bytes,db_lock_acquisitions,db_lock_wait_avg_ms,db_lock_wait_max_ms,db_lock_hold_avg_ms,db_lock_hold_max_ms,database_size_bytes,final_patient_count,rss_bytes,thread_count,path\n{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.2},{:.3},{:.3},{:.3},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{},{},{},{},{}\n",
            self.mode,
            self.scenario,
            self.clients,
            self.active_query_concurrency,
            self.queries,
            self.select_queries,
            self.insert_queries,
            self.setup_latency_p50_ms,
            self.setup_latency_p95_ms,
            self.setup_latency_p99_ms,
            self.elapsed_ms,
            self.queries_per_sec,
            self.latency_p50_ms,
            self.latency_p95_ms,
            self.latency_p99_ms,
            self.active_connections_peak,
            self.rejected_connections,
            self.server_max_queued_queries,
            self.server_queued_queries_max,
            self.server_active_reads_peak,
            self.server_queued_reads_max,
            self.server_active_writes_peak,
            self.server_queued_writes_max,
            self.server_query_queue_wait_p50_ms,
            self.server_query_queue_wait_p95_ms,
            self.server_query_queue_wait_p99_ms,
            self.server_reported_queries,
            self.server_failed_queries,
            self.server_canceled_queries,
            self.server_timed_out_queries,
            self.server_writes_executed,
            self.server_max_queued_writes,
            self.server_write_queue_depth_max,
            self.server_write_wait_avg_ms,
            self.server_write_wait_max_ms,
            self.server_write_execution_avg_ms,
            self.server_write_execution_max_ms,
            self.server_write_rejected_count,
            self.server_write_timed_out_count,
            self.server_memory_estimate_bytes,
            self.db_lock_acquisitions,
            self.db_lock_wait_avg_ms,
            self.db_lock_wait_max_ms,
            self.db_lock_hold_avg_ms,
            self.db_lock_hold_max_ms,
            self.database_size_bytes,
            self.final_patient_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            self.rss_bytes
                .map(|value| value.to_string())
                .unwrap_or_default(),
            self.thread_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            self.path.display()
        )
    }

    pub fn to_markdown(&self) -> String {
        format!(
            "# BicDB Server Concurrency Benchmark\n\n| Metric | Value |\n| --- | ---: |\n| Scenario | {} |\n| Clients | {} |\n| Active query concurrency | {} |\n| Queries | {} |\n| SELECT queries | {} |\n| INSERT queries | {} |\n| Connection setup p50/p95/p99 ms | {:.3} / {:.3} / {:.3} |\n| Query p50/p95/p99 ms | {:.3} / {:.3} / {:.3} |\n| Throughput queries/sec | {:.2} |\n| Peak active connections | {} |\n| Rejected connections | {} |\n| Failed queries | {} |\n| Canceled queries | {} |\n| Timed out queries | {} |\n| DB lock acquisitions | {} |\n| DB lock wait avg/max ms | {:.6} / {:.6} |\n| DB lock hold avg/max ms | {:.6} / {:.6} |\n| Server memory estimate bytes | {} |\n| RSS bytes | {} |\n| Thread count | {} |\n| Database size bytes | {} |\n| Final patient count | {} |\n| Path | {} |\n",
            self.scenario,
            self.clients,
            self.active_query_concurrency,
            self.queries,
            self.select_queries,
            self.insert_queries,
            self.setup_latency_p50_ms,
            self.setup_latency_p95_ms,
            self.setup_latency_p99_ms,
            self.latency_p50_ms,
            self.latency_p95_ms,
            self.latency_p99_ms,
            self.queries_per_sec,
            self.active_connections_peak,
            self.rejected_connections,
            self.server_failed_queries,
            self.server_canceled_queries,
            self.server_timed_out_queries,
            self.db_lock_acquisitions,
            self.db_lock_wait_avg_ms,
            self.db_lock_wait_max_ms,
            self.db_lock_hold_avg_ms,
            self.db_lock_hold_max_ms,
            self.server_memory_estimate_bytes,
            self.rss_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            self.thread_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            self.database_size_bytes,
            self.final_patient_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            self.path.display()
        )
    }
}

impl ServerCertificationReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        let mut csv = String::from(
            "mode,profile,cert_passed,idle_soak_ms,scenario,scenario_passed,clients,active_query_concurrency,queries,queries_per_sec,latency_p50_ms,latency_p95_ms,latency_p99_ms,setup_latency_p95_ms,active_connections_peak,rejected_connections,failed_queries,canceled_queries,timed_out_queries,server_memory_estimate_bytes,rss_bytes,thread_count,final_patient_count,failures,path\n",
        );
        for scenario in &self.scenarios {
            let report = &scenario.report;
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{:.2},{:.3},{:.3},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{}\n",
                self.mode,
                bench_csv_escape(&self.profile),
                self.passed,
                self.idle_soak_ms,
                scenario.scenario,
                scenario.passed,
                report.clients,
                report.active_query_concurrency,
                report.queries,
                report.queries_per_sec,
                report.latency_p50_ms,
                report.latency_p95_ms,
                report.latency_p99_ms,
                report.setup_latency_p95_ms,
                report.active_connections_peak,
                report.rejected_connections,
                report.server_failed_queries,
                report.server_canceled_queries,
                report.server_timed_out_queries,
                report.server_memory_estimate_bytes,
                report
                    .rss_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                report
                    .thread_count
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                report
                    .final_patient_count
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                bench_csv_escape(&scenario.failures.join("; ")),
                bench_csv_escape(&report.path.display().to_string())
            ));
        }
        csv
    }

    pub fn to_markdown(&self) -> String {
        let mut markdown = String::new();
        markdown.push_str("# BicDB Server Certification\n\n");
        markdown.push_str(&format!(
            "- Profile: {}\n- Passed: {}\n- Clients: {}\n- Active query concurrency: {}\n- Queries per workload: {}\n- Idle soak ms: {}\n\n",
            self.profile,
            self.passed,
            self.clients,
            self.active_query_concurrency,
            self.queries_per_workload,
            self.idle_soak_ms
        ));
        markdown.push_str("## Budgets\n\n");
        markdown.push_str(&format!(
            "- Max RSS: {}\n- Max threads: {}\n- Min connection success rate: {:.3}\n- Read-only: min {:.2} q/s, p95 <= {:.3}ms, p99 <= {:.3}ms\n- Mixed: min {:.2} q/s, p95 <= {:.3}ms, p99 <= {:.3}ms\n- Churn p99 <= {:.3}ms\n- Cancel-contention short-read p99 <= {:.3}ms\n\n",
            fmt_bytes(self.budget.max_rss_bytes),
            self.budget.max_thread_count,
            self.budget.min_connection_success_rate,
            self.budget.read_min_queries_per_sec,
            self.budget.read_max_p95_ms,
            self.budget.read_max_p99_ms,
            self.budget.mixed_min_queries_per_sec,
            self.budget.mixed_max_p95_ms,
            self.budget.mixed_max_p99_ms,
            self.budget.churn_max_p99_ms,
            self.budget.cancel_short_read_max_p99_ms
        ));
        markdown.push_str("## Scenarios\n\n");
        markdown.push_str("| Scenario | Passed | q/s | p50 / p95 / p99 ms | Setup p95 ms | Peak active | Rejected | Failed / Canceled / Timed out | RSS | Threads | Final patients | Failures |\n");
        markdown.push_str(
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n",
        );
        for scenario in &self.scenarios {
            let report = &scenario.report;
            markdown.push_str(&format!(
                "| {} | {} | {:.2} | {:.3} / {:.3} / {:.3} | {:.3} | {} | {} | {} / {} / {} | {} | {} | {} | {} |\n",
                markdown_escape(scenario.scenario),
                scenario.passed,
                report.queries_per_sec,
                report.latency_p50_ms,
                report.latency_p95_ms,
                report.latency_p99_ms,
                report.setup_latency_p95_ms,
                report.active_connections_peak,
                report.rejected_connections,
                report.server_failed_queries,
                report.server_canceled_queries,
                report.server_timed_out_queries,
                report
                    .rss_bytes
                    .map(fmt_bytes)
                    .unwrap_or_else(|| "n/a".to_string()),
                report
                    .thread_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
                report
                    .final_patient_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
                markdown_escape(&scenario.failures.join("; "))
            ));
        }
        markdown
    }
}

impl PostgresCompatReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_csv(&self) -> String {
        let mut csv = String::from(
            "mode,target_version,category,id,expectation,status,coverage_state,elapsed_ms,description,detail,path\n",
        );
        for case in &self.cases {
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{:.3},{},{},{}\n",
                self.mode,
                bench_csv_escape(&self.target_version),
                bench_csv_escape(&case.category),
                bench_csv_escape(&case.id),
                postgres_compat_expectation_name(case.expectation),
                postgres_compat_status_name(case.status),
                postgres_compat_coverage_state_name(case.coverage_state),
                case.elapsed_ms,
                bench_csv_escape(&case.description),
                bench_csv_escape(&case.detail),
                bench_csv_escape(&self.path.display().to_string())
            ));
        }
        csv.push_str("\ncategory,executable_cases,passed_cases,failed_cases,score_percent,supported_cases,unsupported_cases,expected_difference_cases,not_yet_tested_cases\n");
        for category in &self.category_scores {
            csv.push_str(&format!(
                "{},{},{},{},{:.3},{},{},{},{}\n",
                bench_csv_escape(&category.category),
                category.executable_cases,
                category.passed_cases,
                category.failed_cases,
                category.score_percent,
                category.supported_cases,
                category.unsupported_cases,
                category.expected_difference_cases,
                category.not_yet_tested_cases
            ));
        }
        csv.push_str("\nknown_gap_category,known_gap_feature,known_gap_status,recommendation\n");
        for gap in &self.known_gaps {
            csv.push_str(&format!(
                "{},{},{},{}\n",
                bench_csv_escape(&gap.category),
                bench_csv_escape(&gap.feature),
                bench_csv_escape(&gap.status),
                bench_csv_escape(&gap.recommendation)
            ));
        }
        csv
    }

    pub fn to_markdown(&self) -> String {
        let mut markdown = String::new();
        markdown.push_str("# BicDB PostgreSQL Compatibility Scorecard\n\n");
        markdown.push_str(&format!(
            "- Target: PostgreSQL {}\n- Executable suite score: {}/{} ({:.1}%)\n- Clear unsupported-feature checks passed: {}\n- Interpretation: {}\n\n",
            self.target_version,
            self.passed_cases,
            self.total_cases,
            self.score_percent,
            self.clear_unsupported_cases,
            self.score_interpretation
        ));
        markdown.push_str("## Category Scores\n\n");
        markdown.push_str("| Category | Executable | Passed | Failed | Score | Supported | Unsupported | Expected Difference | Not Yet Tested |\n");
        markdown.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for category in &self.category_scores {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {:.1}% | {} | {} | {} | {} |\n",
                markdown_escape(&category.category),
                category.executable_cases,
                category.passed_cases,
                category.failed_cases,
                category.score_percent,
                category.supported_cases,
                category.unsupported_cases,
                category.expected_difference_cases,
                category.not_yet_tested_cases
            ));
        }
        markdown.push_str("\n## Case Details\n\n");
        markdown.push_str("| Category | Case | Expectation | Status | Coverage State | Detail |\n");
        markdown.push_str("| --- | --- | --- | --- | --- | --- |\n");
        for case in &self.cases {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                markdown_escape(&case.category),
                markdown_escape(&case.id),
                postgres_compat_expectation_name(case.expectation),
                postgres_compat_status_name(case.status),
                postgres_compat_coverage_state_name(case.coverage_state),
                markdown_escape(&case.detail)
            ));
        }
        markdown.push_str("\n## Known Gaps\n\n");
        markdown.push_str("| Category | Feature | Status | Recommendation |\n");
        markdown.push_str("| --- | --- | --- | --- |\n");
        for gap in &self.known_gaps {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                markdown_escape(&gap.category),
                markdown_escape(&gap.feature),
                markdown_escape(&gap.status),
                markdown_escape(&gap.recommendation)
            ));
        }
        markdown
    }
}

impl PostgresDiffReport {
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    pub fn to_markdown(&self) -> String {
        let mut markdown = String::new();
        let title = if self.mode == "pg18_nightmare" {
            "BicDB PostgreSQL 18 Nightmare Gauntlet Report"
        } else {
            "PostgreSQL Differential Compatibility Report"
        };
        markdown.push_str(&format!("# {title}\n\n"));
        markdown.push_str(&format!(
            "- Target: PostgreSQL {}\n- Endpoint: {}:{}\n- Cases: {}/{} passed\n- Expected-difference fixtures: {}\n- Elapsed: {:.1} ms\n\n",
            self.target_version,
            self.postgres_host,
            self.postgres_port,
            self.passed_cases,
            self.total_cases,
            self.expected_difference_cases,
            self.elapsed_ms
        ));
        markdown.push_str("| Fixture | Expectation | Status | Detail |\n");
        markdown.push_str("| --- | --- | --- | --- |\n");
        for case in &self.cases {
            markdown.push_str(&format!(
                "| {} | {:?} | {:?} | {} |\n",
                markdown_escape(&case.id),
                case.expectation,
                case.status,
                markdown_escape(&case.markdown_detail())
            ));
        }
        markdown
    }

    pub fn write_repro_files(&self, out_dir: impl AsRef<Path>) -> Result<()> {
        let out_dir = out_dir.as_ref();
        fs::create_dir_all(out_dir)
            .with_context(|| format!("create repro dir {}", out_dir.display()))?;

        let mut failing_bundle = String::new();
        let mut minimized_bundle = String::new();
        for case in self
            .cases
            .iter()
            .filter(|case| case.status == PostgresDiffStatus::Failed)
        {
            let sql = case.repro_sql();
            let minimized = case.minimized_repro_sql();
            fs::write(out_dir.join(format!("{}.sql", case.id)), &sql)
                .with_context(|| format!("write repro SQL for {}", case.id))?;
            fs::write(out_dir.join(format!("{}.min.sql", case.id)), &minimized)
                .with_context(|| format!("write minimized repro SQL for {}", case.id))?;
            failing_bundle.push_str(&sql);
            failing_bundle.push('\n');
            minimized_bundle.push_str(&minimized);
            minimized_bundle.push('\n');
        }

        if failing_bundle.is_empty() {
            failing_bundle.push_str("-- No failing pg18-nightmare fixtures in this run.\n");
            minimized_bundle.push_str("-- No failing pg18-nightmare fixtures in this run.\n");
        }

        fs::write(out_dir.join("failing.sql"), failing_bundle)
            .with_context(|| format!("write {}", out_dir.join("failing.sql").display()))?;
        fs::write(out_dir.join("minimized.sql"), minimized_bundle)
            .with_context(|| format!("write {}", out_dir.join("minimized.sql").display()))?;
        Ok(())
    }
}

impl PostgresDiffCaseReport {
    fn markdown_detail(&self) -> String {
        if self.metadata_differences.is_empty() {
            self.detail.clone()
        } else {
            format!(
                "{}; metadata: {}",
                self.detail,
                self.metadata_differences.join("; ")
            )
        }
    }

    fn repro_sql(&self) -> String {
        let mut sql = format!(
            "-- Fixture: {}\n-- Expectation: {:?}\n-- Detail: {}\n",
            self.id, self.expectation, self.detail
        );
        if let Some(expected_difference) = &self.expected_difference {
            sql.push_str(&format!("-- Intentional gap: {expected_difference}\n"));
        }
        for step in self.postgres.iter().map(|step| step.sql.as_str()) {
            sql.push_str(step);
            if !step.trim_end().ends_with(';') {
                sql.push(';');
            }
            sql.push('\n');
        }
        sql
    }

    fn minimized_repro_sql(&self) -> String {
        let mut sql = format!(
            "-- Minimized repro for fixture: {}\n-- Detail: {}\n",
            self.id, self.detail
        );
        let mismatch_index = self
            .postgres
            .iter()
            .zip(self.bicdb.iter())
            .position(|(postgres, bicdb)| postgres != bicdb)
            .unwrap_or_else(|| self.postgres.len().saturating_sub(1));
        for step in self
            .postgres
            .iter()
            .take(mismatch_index.saturating_add(1))
            .map(|step| step.sql.as_str())
        {
            sql.push_str(step);
            if !step.trim_end().ends_with(';') {
                sql.push(';');
            }
            sql.push('\n');
        }
        sql
    }
}

impl Display for PostgresDiffReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "PostgreSQL differential compatibility")?;
        writeln!(f, "Target: PostgreSQL {}", self.target_version)?;
        writeln!(f, "Endpoint: {}:{}", self.postgres_host, self.postgres_port)?;
        writeln!(
            f,
            "Cases: {}/{} passed",
            self.passed_cases, self.total_cases
        )?;
        writeln!(
            f,
            "Expected differences: {}",
            self.expected_difference_cases
        )?;
        for case in &self.cases {
            writeln!(
                f,
                "- {} [{:?}/{:?}]: {}",
                case.id, case.expectation, case.status, case.detail
            )?;
        }
        Ok(())
    }
}

impl BaselineReport {
    pub fn suite_to_json(reports: &[BaselineReport]) -> Result<String> {
        serde_json::to_string_pretty(reports).map_err(Into::into)
    }

    pub fn suite_to_csv(reports: &[BaselineReport]) -> String {
        let mut csv = String::from(
            "engine,status,records,batch_size,elapsed_ms,records_per_sec,database_size_bytes,message,path\n",
        );
        for report in reports {
            csv.push_str(&format!(
                "{:?},{:?},{},{},{},{},{},\"{}\",{}\n",
                report.engine,
                report.status,
                report.records,
                report.batch_size,
                report
                    .elapsed_ms
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_default(),
                report
                    .records_per_sec
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_default(),
                report
                    .database_size_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                report.message.replace('"', "'"),
                report
                    .path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            ));
        }
        csv
    }
}

impl Display for InsertBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Benchmark")?;
        writeln!(formatter, "---------------")?;
        writeln!(formatter, "Mode: inserts")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Batch size: {}", self.batch_size)?;
        writeln!(formatter, "Insert elapsed: {}", fmt_duration(self.elapsed))?;
        writeln!(
            formatter,
            "Insert throughput: {:.2} records/sec",
            self.records_per_sec
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        write_collection_sizes(formatter, &self.collection_sizes)?;
        writeln!(
            formatter,
            "Startup recovery time: {}",
            fmt_duration(self.startup_recovery_time)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for VectorBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Benchmark")?;
        writeln!(formatter, "---------------")?;
        writeln!(formatter, "Mode: vectors")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Dimension: {}", self.dim)?;
        writeln!(formatter, "Top K: {}", self.top_k)?;
        writeln!(
            formatter,
            "Insert elapsed: {}",
            fmt_duration(self.insert_elapsed)
        )?;
        writeln!(
            formatter,
            "Vector search latency: p50 {} / p95 {} / p99 {}",
            fmt_duration(self.search_p50),
            fmt_duration(self.search_p95),
            fmt_duration(self.search_p99)
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        write_collection_sizes(formatter, &self.collection_sizes)?;
        writeln!(
            formatter,
            "Startup recovery time: {}",
            fmt_duration(self.startup_recovery_time)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for VectorProfileReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Vector Search Profile")?;
        writeln!(formatter, "---------------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Dimension: {}", self.dim)?;
        writeln!(formatter, "Top K: {}", self.top_k)?;
        writeln!(formatter, "Searches: {}", self.searches)?;
        writeln!(formatter, "Metric: {}", self.metric)?;
        writeln!(formatter, "Strategy: {}", self.strategy)?;
        writeln!(
            formatter,
            "Total latency: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms",
            self.total_p50_ms, self.total_p95_ms, self.total_p99_ms
        )?;
        writeln!(
            formatter,
            "Phase avg: read_vectors {:.3}ms, similarity {:.3}ms, top_k_heap {:.3}ms, final_sort {:.6}ms",
            self.avg_read_vectors_ms,
            self.avg_similarity_ms,
            self.avg_top_k_heap_ms,
            self.avg_final_sort_ms
        )?;
        writeln!(
            formatter,
            "Allocations: total {} allocs / {} bytes, per search {:.2} allocs / {:.2} bytes, max live {} bytes",
            self.allocation_count_total,
            self.allocated_bytes_total,
            self.allocation_count_per_search,
            self.allocated_bytes_per_search,
            self.allocation_bytes_max
        )?;
        writeln!(formatter, "Candidates scanned: {}", self.candidates_scanned)?;
        writeln!(formatter, "Vectors read: {}", self.vectors_read)?;
        writeln!(formatter, "Result count: {}", self.result_count)?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for VectorSearchHotReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Vector Search Hot Loop")?;
        writeln!(formatter, "----------------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Dimension: {}", self.dim)?;
        writeln!(formatter, "Top K: {}", self.top_k)?;
        writeln!(formatter, "Searches: {}", self.searches)?;
        writeln!(formatter, "Metric: {}", self.metric)?;
        writeln!(formatter, "Strategy: {}", self.strategy)?;
        writeln!(
            formatter,
            "Search latency: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms",
            self.search_p50_ms, self.search_p95_ms, self.search_p99_ms
        )?;
        writeln!(formatter, "Hot loop elapsed: {:.3}ms", self.elapsed_ms)?;
        writeln!(formatter, "Result count: {}", self.result_count)?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for AnnBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB ANN Vector Benchmark")?;
        writeln!(formatter, "--------------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Dimension: {}", self.dim)?;
        writeln!(formatter, "Top K: {}", self.top_k)?;
        writeln!(formatter, "Searches: {}", self.searches)?;
        writeln!(formatter, "Insert elapsed: {:.3}ms", self.insert_elapsed_ms)?;
        writeln!(
            formatter,
            "HNSW build: {:.3}ms, index size {}, memory estimate {}",
            self.index_build_ms,
            fmt_bytes(self.index_size_bytes),
            fmt_bytes(self.memory_estimate_bytes)
        )?;
        writeln!(
            formatter,
            "Exact search: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms",
            self.exact_p50_ms, self.exact_p95_ms, self.exact_p99_ms
        )?;
        writeln!(
            formatter,
            "HNSW ef=20: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms / recall@{} {:.4}",
            self.ann_ef20_p50_ms,
            self.ann_ef20_p95_ms,
            self.ann_ef20_p99_ms,
            self.top_k,
            self.ann_ef20_recall_at_k
        )?;
        writeln!(
            formatter,
            "HNSW ef=50: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms / recall@{} {:.4}",
            self.ann_ef50_p50_ms,
            self.ann_ef50_p95_ms,
            self.ann_ef50_p99_ms,
            self.top_k,
            self.ann_ef50_recall_at_k
        )?;
        writeln!(
            formatter,
            "HNSW ef=100: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms / recall@{} {:.4}",
            self.ann_ef100_p50_ms,
            self.ann_ef100_p95_ms,
            self.ann_ef100_p99_ms,
            self.top_k,
            self.ann_ef100_recall_at_k
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for GraphBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Graph Benchmark")?;
        writeln!(formatter, "---------------------")?;
        writeln!(formatter, "Entities: {}", self.entities)?;
        writeln!(formatter, "Edges requested: {}", self.edges)?;
        writeln!(formatter, "Insert elapsed: {:.3}ms", self.insert_elapsed_ms)?;
        writeln!(formatter, "Graph build: {:.3}ms", self.build_elapsed_ms)?;
        writeln!(
            formatter,
            "Neighbor lookup: {:.6}ms",
            self.neighbor_lookup_ms
        )?;
        writeln!(formatter, "Path query: {:.6}ms", self.path_query_ms)?;
        writeln!(formatter, "Edge scan: {:.3}ms", self.edge_scan_ms)?;
        writeln!(
            formatter,
            "Graph: {} nodes, {} edges, size {}",
            self.node_count,
            self.edge_count,
            fmt_bytes(self.graph_size_bytes)
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for SpatialBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Spatial Benchmark")?;
        writeln!(formatter, "-----------------------")?;
        writeln!(formatter, "Points: {}", self.points)?;
        writeln!(
            formatter,
            "Insert: {:.3}ms ({:.2} points/sec)",
            self.insert_elapsed_ms, self.points_per_sec
        )?;
        writeln!(
            formatter,
            "Spatial index build: {:.3}ms, catalog size {}",
            self.index_build_ms,
            fmt_bytes(self.index_size_bytes)
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        write_collection_sizes(formatter, &self.collection_sizes)?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for SpatialNearestBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Spatial Nearest Benchmark")?;
        writeln!(formatter, "-------------------------------")?;
        writeln!(formatter, "Points: {}", self.points)?;
        writeln!(formatter, "Queries: {}", self.queries)?;
        writeln!(
            formatter,
            "Nearest latency: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms ({} total results)",
            self.nearest_p50_ms,
            self.nearest_p95_ms,
            self.nearest_p99_ms,
            self.nearest_result_count
        )?;
        writeln!(
            formatter,
            "Radius latency ({:.0}m): p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms ({} total results)",
            self.radius_meters,
            self.radius_p50_ms,
            self.radius_p95_ms,
            self.radius_p99_ms,
            self.radius_result_count
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for RouteBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Route Benchmark")?;
        writeln!(formatter, "---------------------")?;
        writeln!(formatter, "Nodes: {}", self.nodes)?;
        writeln!(formatter, "Edges: {}", self.edges)?;
        writeln!(formatter, "Insert elapsed: {:.3}ms", self.insert_elapsed_ms)?;
        writeln!(
            formatter,
            "Route latency: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms ({} route nodes returned)",
            self.route_p50_ms, self.route_p95_ms, self.route_p99_ms, self.route_result_count
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for EventBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Event Benchmark")?;
        writeln!(formatter, "---------------------")?;
        writeln!(formatter, "Events: {}", self.events)?;
        writeln!(
            formatter,
            "Append throughput: {:.2} events/sec in {:.3}ms",
            self.append_events_per_sec, self.append_elapsed_ms
        )?;
        writeln!(
            formatter,
            "Subscriber latency: p50 {:.6}ms / p95 {:.6}ms / p99 {:.6}ms",
            self.subscriber_p50_ms, self.subscriber_p95_ms, self.subscriber_p99_ms
        )?;
        writeln!(
            formatter,
            "Replay throughput: {:.2} events/sec ({} events in {:.3}ms)",
            self.replay_events_per_sec, self.replayed_events, self.replay_elapsed_ms
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for QueueBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Queue Benchmark")?;
        writeln!(formatter, "---------------------")?;
        writeln!(formatter, "Messages: {}", self.messages)?;
        writeln!(formatter, "Consume batch size: {}", self.consume_batch_size)?;
        writeln!(
            formatter,
            "Publish throughput: {:.2} messages/sec in {:.3}ms",
            self.publish_messages_per_sec, self.publish_elapsed_ms
        )?;
        writeln!(
            formatter,
            "Consume throughput: {:.2} messages/sec ({} messages in {:.3}ms)",
            self.consume_messages_per_sec, self.consumed_messages, self.consume_elapsed_ms
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for ProjectionBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Projection Benchmark")?;
        writeln!(formatter, "--------------------------")?;
        writeln!(formatter, "Events: {}", self.events)?;
        writeln!(formatter, "Entities: {}", self.entities)?;
        writeln!(
            formatter,
            "Append throughput: {:.2} events/sec in {:.3}ms",
            self.append_events_per_sec, self.append_elapsed_ms
        )?;
        writeln!(
            formatter,
            "Projection rebuild: {:.2} events/sec ({} entities in {:.3}ms)",
            self.rebuild_events_per_sec, self.projected_entities, self.rebuild_elapsed_ms
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for WearableBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Benchmark")?;
        writeln!(formatter, "---------------")?;
        writeln!(formatter, "Mode: wearable")?;
        writeln!(formatter, "Devices: {}", self.devices)?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(
            formatter,
            "Wearable ingestion throughput: {:.2} records/sec",
            self.records_per_sec
        )?;
        writeln!(
            formatter,
            "Time range scan throughput: {} records in {}",
            self.scanned_records,
            fmt_duration(self.time_range_scan_elapsed)
        )?;
        writeln!(
            formatter,
            "Latest value lookup: {}",
            fmt_duration(self.latest_lookup_elapsed)
        )?;
        writeln!(formatter, "Summary count: {}", self.summary_count)?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        write_collection_sizes(formatter, &self.collection_sizes)?;
        writeln!(
            formatter,
            "Startup recovery time: {}",
            fmt_duration(self.startup_recovery_time)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for SqlBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB SQL Benchmark")?;
        writeln!(formatter, "-------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Insert elapsed: {:.3}ms", self.insert_elapsed_ms)?;
        writeln!(
            formatter,
            "SELECT by id: SQL {:.6}ms / direct {:.6}ms",
            self.select_by_id_sql_ms, self.select_by_id_direct_ms
        )?;
        writeln!(
            formatter,
            "Timestamp range scan: SQL {:.3}ms / direct {:.3}ms ({} rows)",
            self.timestamp_range_sql_ms, self.timestamp_range_direct_ms, self.timestamp_range_rows
        )?;
        writeln!(
            formatter,
            "COUNT(*): SQL {:.3}ms / direct {:.3}ms ({} rows)",
            self.count_sql_ms, self.count_direct_ms, self.count_rows
        )?;
        writeln!(
            formatter,
            "AVG(metadata.value): SQL {:.3}ms / direct {:.3}ms ({:.6})",
            self.avg_sql_ms, self.avg_direct_ms, self.avg_value
        )?;
        writeln!(
            formatter,
            "ORDER BY timestamp DESC LIMIT 10: SQL {:.3}ms / direct {:.3}ms ({} rows)",
            self.order_by_limit_sql_ms, self.order_by_limit_direct_ms, self.order_by_limit_rows
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        write_collection_sizes(formatter, &self.collection_sizes)?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for IndexBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Index Benchmark")?;
        writeln!(formatter, "---------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(
            formatter,
            "Insert without indexes: {:.3}ms",
            self.insert_without_indexes_ms
        )?;
        writeln!(
            formatter,
            "Insert with indexes: {:.3}ms (overhead {:.3}ms)",
            self.insert_with_indexes_ms, self.write_overhead_ms
        )?;
        writeln!(
            formatter,
            "Index build/rebuild: {:.3}/{:.3}ms, catalog size {}, peak memory estimate {}",
            self.index_build_ms,
            self.index_rebuild_ms,
            fmt_bytes(self.index_size_bytes),
            fmt_bytes(self.peak_memory_estimate_bytes)
        )?;
        writeln!(
            formatter,
            "Point lookup: scan {:.3}ms / index {:.3}ms ({} rows)",
            self.point_lookup_scan_ms, self.point_lookup_index_ms, self.point_lookup_rows
        )?;
        writeln!(
            formatter,
            "Timestamp range: scan {:.3}ms / index {:.3}ms ({} rows)",
            self.timestamp_range_scan_ms, self.timestamp_range_index_ms, self.timestamp_range_rows
        )?;
        writeln!(
            formatter,
            "Metadata equality: scan {:.3}ms / index {:.3}ms ({} rows)",
            self.metadata_equality_scan_ms,
            self.metadata_equality_index_ms,
            self.metadata_equality_rows
        )?;
        writeln!(
            formatter,
            "Composite lookup: scan {:.3}ms / index {:.3}ms ({} rows)",
            self.composite_lookup_scan_ms,
            self.composite_lookup_index_ms,
            self.composite_lookup_rows
        )?;
        writeln!(
            formatter,
            "ORDER BY timestamp DESC LIMIT 10: scan {:.3}ms / index {:.3}ms ({} rows)",
            self.order_by_scan_ms, self.order_by_index_ms, self.order_by_rows
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        write_collection_sizes(formatter, &self.collection_sizes)?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for TransactionBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Transaction Benchmark")?;
        writeln!(formatter, "---------------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Batch size: {}", self.batch_size)?;
        writeln!(
            formatter,
            "Single insert tx: {:.2} tx/sec in {:.3}ms",
            self.single_insert_tx_per_sec, self.single_insert_tx_ms
        )?;
        writeln!(
            formatter,
            "Batch insert tx: {:.2} records/sec in {:.3}ms",
            self.batch_insert_tx_per_sec, self.batch_insert_tx_ms
        )?;
        writeln!(
            formatter,
            "Rollback: {:.2} records/sec in {:.3}ms",
            self.rollback_records_per_sec, self.rollback_ms
        )?;
        writeln!(
            formatter,
            "Recovery: {} records in {:.3}ms",
            self.recovered_records, self.recovery_ms
        )?;
        writeln!(
            formatter,
            "Snapshot scan: {} records in {:.3}ms",
            self.snapshot_records, self.snapshot_scan_ms
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for AnalyticsBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Analytics Benchmark")?;
        writeln!(formatter, "-------------------------")?;
        writeln!(formatter, "Records: {}", self.records)?;
        writeln!(formatter, "Insert elapsed: {:.3}ms", self.insert_elapsed_ms)?;
        writeln!(
            formatter,
            "Sidecar rebuild: {:.3}ms, size {}",
            self.sidecar_rebuild_ms,
            fmt_bytes(self.sidecar_size_bytes)
        )?;
        writeln!(
            formatter,
            "COUNT(*): direct {:.3}ms / query_exec {:.3}ms / DataFusion p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms",
            self.direct_count_ms,
            self.query_exec_count_ms,
            self.datafusion_count_p50_ms,
            self.datafusion_count_p95_ms,
            self.datafusion_count_p99_ms
        )?;
        writeln!(
            formatter,
            "AVG(value WHERE metric='hrv'): direct {:.3}ms / query_exec {:.3}ms / DataFusion p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms",
            self.direct_avg_ms,
            self.query_exec_avg_ms,
            self.datafusion_avg_p50_ms,
            self.datafusion_avg_p95_ms,
            self.datafusion_avg_p99_ms
        )?;
        writeln!(
            formatter,
            "MIN/MAX timestamp range: direct {:.3}ms / query_exec {:.3}ms / DataFusion p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms",
            self.direct_min_max_ms,
            self.query_exec_min_max_ms,
            self.datafusion_min_max_p50_ms,
            self.datafusion_min_max_p95_ms,
            self.datafusion_min_max_p99_ms
        )?;
        writeln!(
            formatter,
            "GROUP BY metric: DataFusion p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms",
            self.datafusion_group_by_metric_p50_ms,
            self.datafusion_group_by_metric_p95_ms,
            self.datafusion_group_by_metric_p99_ms
        )?;
        writeln!(
            formatter,
            "GROUP BY device_id: DataFusion p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms",
            self.datafusion_group_by_device_p50_ms,
            self.datafusion_group_by_device_p95_ms,
            self.datafusion_group_by_device_p99_ms
        )?;
        writeln!(
            formatter,
            "Timestamp filter COUNT: DataFusion p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms",
            self.datafusion_timestamp_range_p50_ms,
            self.datafusion_timestamp_range_p95_ms,
            self.datafusion_timestamp_range_p99_ms
        )?;
        writeln!(
            formatter,
            "DataFusion throughput estimate: {:.2} rows/sec",
            self.datafusion_rows_per_sec
        )?;
        writeln!(
            formatter,
            "Arrow memory: {}, database size: {}",
            fmt_bytes(self.arrow_memory_bytes as u64),
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for MemoryBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Memory Benchmark")?;
        writeln!(formatter, "----------------------")?;
        writeln!(formatter, "Memories: {}", self.memories)?;
        writeln!(formatter, "Dimension: {}", self.dim)?;
        writeln!(formatter, "Top K: {}", self.top_k)?;
        writeln!(
            formatter,
            "Insert: {:.3}ms ({:.2} memories/sec)",
            self.insert_elapsed_ms, self.memories_per_sec
        )?;
        writeln!(
            formatter,
            "Recall latency: p50 {:.3}ms p95 {:.3}ms p99 {:.3}ms ({} results)",
            self.recall_p50_ms, self.recall_p95_ms, self.recall_p99_ms, self.recall_result_count
        )?;
        writeln!(
            formatter,
            "Ranking speed: {:.2} memories/sec",
            self.ranking_memories_per_sec
        )?;
        writeln!(
            formatter,
            "Timeline: {:.3}ms ({} windowed memories)",
            self.timeline_elapsed_ms, self.timeline_memories
        )?;
        writeln!(
            formatter,
            "Workspace load: {:.3}ms ({} memories)",
            self.workspace_load_ms, self.workspace_memories
        )?;
        writeln!(formatter, "Memory events: {}", self.memory_event_count)?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for SyncBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Sync Mesh Benchmark")?;
        writeln!(formatter, "--------------------------")?;
        writeln!(formatter, "Records per node: {}", self.records_per_node)?;
        writeln!(
            formatter,
            "Export: {} left events in {:.3}ms, {} right events in {:.3}ms ({:.2} events/sec)",
            self.left_export_events,
            self.left_export_elapsed_ms,
            self.right_export_events,
            self.right_export_elapsed_ms,
            self.export_events_per_sec
        )?;
        writeln!(
            formatter,
            "Import: right {:.3}ms, left {:.3}ms ({:.2} events/sec)",
            self.right_import_elapsed_ms, self.left_import_elapsed_ms, self.import_events_per_sec
        )?;
        writeln!(
            formatter,
            "Merge: {} records ({:.2} records/sec)",
            self.records_merged, self.merge_records_per_sec
        )?;
        writeln!(
            formatter,
            "Conflicts: {} resolved ({:.2} conflicts/sec)",
            self.conflicts_resolved, self.conflicts_per_sec
        )?;
        writeln!(formatter, "Converged: {}", self.converged)?;
        writeln!(formatter, "Audit events preserved: {}", self.audit_events)?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for ServerBenchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Server Benchmark")?;
        writeln!(formatter, "-----------------------")?;
        writeln!(formatter, "Scenario: {}", self.scenario)?;
        writeln!(formatter, "Clients: {}", self.clients)?;
        writeln!(
            formatter,
            "Active query concurrency: {}",
            self.active_query_concurrency
        )?;
        writeln!(formatter, "Queries: {}", self.queries)?;
        writeln!(
            formatter,
            "Query mix: {} SELECT / {} INSERT",
            self.select_queries, self.insert_queries
        )?;
        writeln!(
            formatter,
            "Throughput: {:.2} queries/sec in {:.3}ms",
            self.queries_per_sec, self.elapsed_ms
        )?;
        writeln!(
            formatter,
            "Latency: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms",
            self.latency_p50_ms, self.latency_p95_ms, self.latency_p99_ms
        )?;
        writeln!(
            formatter,
            "Connection setup: p50 {:.3}ms / p95 {:.3}ms / p99 {:.3}ms",
            self.setup_latency_p50_ms, self.setup_latency_p95_ms, self.setup_latency_p99_ms
        )?;
        writeln!(
            formatter,
            "Connections: peak active {}, rejected {}",
            self.active_connections_peak, self.rejected_connections
        )?;
        writeln!(
            formatter,
            "Server counters: {} queries, {} failed, {} canceled, {} timed out, {} writes",
            self.server_reported_queries,
            self.server_failed_queries,
            self.server_canceled_queries,
            self.server_timed_out_queries,
            self.server_writes_executed
        )?;
        writeln!(
            formatter,
            "Query queue: max queued {}, peak {}, read active/queued peaks {}/{}, write active/queued peaks {}/{}, wait p50/p95/p99 {:.6}/{:.6}/{:.6}ms",
            self.server_max_queued_queries,
            self.server_queued_queries_max,
            self.server_active_reads_peak,
            self.server_queued_reads_max,
            self.server_active_writes_peak,
            self.server_queued_writes_max,
            self.server_query_queue_wait_p50_ms,
            self.server_query_queue_wait_p95_ms,
            self.server_query_queue_wait_p99_ms
        )?;
        writeln!(
            formatter,
            "Write queue: max queued {}, peak depth {}, wait avg/max {:.6}/{:.6}ms, execution avg/max {:.6}/{:.6}ms, rejected {}, timed out {}",
            self.server_max_queued_writes,
            self.server_write_queue_depth_max,
            self.server_write_wait_avg_ms,
            self.server_write_wait_max_ms,
            self.server_write_execution_avg_ms,
            self.server_write_execution_max_ms,
            self.server_write_rejected_count,
            self.server_write_timed_out_count
        )?;
        writeln!(
            formatter,
            "DB lock: {} acquisitions, wait avg/max {:.6}/{:.6}ms, hold avg/max {:.6}/{:.6}ms",
            self.db_lock_acquisitions,
            self.db_lock_wait_avg_ms,
            self.db_lock_wait_max_ms,
            self.db_lock_hold_avg_ms,
            self.db_lock_hold_max_ms
        )?;
        writeln!(
            formatter,
            "Server memory estimate: {}",
            fmt_bytes(self.server_memory_estimate_bytes)
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        if let Some(final_patient_count) = self.final_patient_count {
            writeln!(formatter, "Final patient count: {final_patient_count}")?;
        }
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for ServerCertificationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB Server Certification")?;
        writeln!(formatter, "---------------------------")?;
        writeln!(formatter, "Profile: {}", self.profile)?;
        writeln!(formatter, "Passed: {}", self.passed)?;
        writeln!(formatter, "Clients: {}", self.clients)?;
        writeln!(
            formatter,
            "Active query concurrency: {}",
            self.active_query_concurrency
        )?;
        writeln!(
            formatter,
            "Queries per workload: {}",
            self.queries_per_workload
        )?;
        writeln!(formatter, "Idle soak: {}ms", self.idle_soak_ms)?;
        writeln!(
            formatter,
            "Budgets: RSS <= {}, threads <= {}, read p99 <= {:.3}ms, mixed p99 <= {:.3}ms",
            fmt_bytes(self.budget.max_rss_bytes),
            self.budget.max_thread_count,
            self.budget.read_max_p99_ms,
            self.budget.mixed_max_p99_ms
        )?;
        for scenario in &self.scenarios {
            let report = &scenario.report;
            writeln!(
                formatter,
                "- {}: passed={}, {:.2} q/s, p95/p99 {:.3}/{:.3}ms, peak={}, rejected={}, failed/canceled/timed_out={}/{}/{}",
                scenario.scenario,
                scenario.passed,
                report.queries_per_sec,
                report.latency_p95_ms,
                report.latency_p99_ms,
                report.active_connections_peak,
                report.rejected_connections,
                report.server_failed_queries,
                report.server_canceled_queries,
                report.server_timed_out_queries
            )?;
            if !scenario.failures.is_empty() {
                writeln!(formatter, "  failures: {}", scenario.failures.join("; "))?;
            }
        }
        writeln!(formatter, "Path: {}", self.path.display())
    }
}

impl Display for PostgresCompatReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "BicDB PostgreSQL Compatibility Scorecard")?;
        writeln!(formatter, "----------------------------------------")?;
        writeln!(formatter, "Target: PostgreSQL {}", self.target_version)?;
        writeln!(formatter, "Protocol target: {}", self.protocol_target)?;
        writeln!(formatter, "Protocol supported: {}", self.protocol_supported)?;
        writeln!(
            formatter,
            "Score: {}/{} ({:.1}%)",
            self.passed_cases, self.total_cases, self.score_percent
        )?;
        writeln!(formatter, "Interpretation: {}", self.score_interpretation)?;
        writeln!(
            formatter,
            "Clear unsupported-feature checks passed: {}",
            self.clear_unsupported_cases
        )?;
        writeln!(
            formatter,
            "Database size: {}",
            fmt_bytes(self.database_size_bytes)
        )?;
        writeln!(formatter, "Path: {}", self.path.display())?;
        writeln!(formatter)?;
        writeln!(formatter, "Cases")?;
        for case in &self.cases {
            writeln!(
                formatter,
                "- {} [{}] {} ({}): {} ({:.3}ms)",
                postgres_compat_status_name(case.status),
                case.category,
                case.id,
                postgres_compat_coverage_state_name(case.coverage_state),
                case.detail,
                case.elapsed_ms
            )?;
        }
        writeln!(formatter)?;
        writeln!(formatter, "Category Scores")?;
        for category in &self.category_scores {
            writeln!(
                formatter,
                "- {}: {}/{} executable passed ({:.1}%); supported={}, unsupported={}, expected_difference={}, not_yet_tested={}",
                category.category,
                category.passed_cases,
                category.executable_cases,
                category.score_percent,
                category.supported_cases,
                category.unsupported_cases,
                category.expected_difference_cases,
                category.not_yet_tested_cases
            )?;
        }
        writeln!(formatter)?;
        writeln!(formatter, "Known Gaps")?;
        for gap in &self.known_gaps {
            writeln!(
                formatter,
                "- [{}] {}: {}",
                gap.category, gap.feature, gap.status
            )?;
        }
        Ok(())
    }
}

fn postgres_compat_expectation_name(expectation: PostgresCompatExpectation) -> &'static str {
    match expectation {
        PostgresCompatExpectation::Works => "works",
        PostgresCompatExpectation::ClearUnsupportedError => "clear_unsupported_error",
        PostgresCompatExpectation::ExpectedDifference => "expected_difference",
        PostgresCompatExpectation::NotYetTested => "not_yet_tested",
    }
}

fn postgres_compat_status_name(status: PostgresCompatStatus) -> &'static str {
    match status {
        PostgresCompatStatus::Passed => "passed",
        PostgresCompatStatus::Failed => "failed",
        PostgresCompatStatus::NotRun => "not_run",
    }
}

fn postgres_compat_coverage_state_name(state: PostgresCompatCoverageState) -> &'static str {
    match state {
        PostgresCompatCoverageState::Supported => "supported",
        PostgresCompatCoverageState::Unsupported => "unsupported",
        PostgresCompatCoverageState::ExpectedDifference => "expected_difference",
        PostgresCompatCoverageState::NotYetTested => "not_yet_tested",
    }
}

fn postgres_compat_category_scores(
    cases: &[PostgresCompatCaseReport],
) -> Vec<PostgresCompatCategoryScore> {
    let mut by_category: BTreeMap<String, PostgresCompatCategoryScore> = BTreeMap::new();
    for case in cases {
        let entry = by_category.entry(case.category.clone()).or_insert_with(|| {
            PostgresCompatCategoryScore {
                category: case.category.clone(),
                executable_cases: 0,
                passed_cases: 0,
                failed_cases: 0,
                score_percent: 0.0,
                supported_cases: 0,
                unsupported_cases: 0,
                expected_difference_cases: 0,
                not_yet_tested_cases: 0,
            }
        });

        if case.status != PostgresCompatStatus::NotRun {
            entry.executable_cases += 1;
            if case.status == PostgresCompatStatus::Passed {
                entry.passed_cases += 1;
            } else {
                entry.failed_cases += 1;
            }
        }

        match case.coverage_state {
            PostgresCompatCoverageState::Supported => entry.supported_cases += 1,
            PostgresCompatCoverageState::Unsupported => entry.unsupported_cases += 1,
            PostgresCompatCoverageState::ExpectedDifference => entry.expected_difference_cases += 1,
            PostgresCompatCoverageState::NotYetTested => entry.not_yet_tested_cases += 1,
        }
    }

    for category in by_category.values_mut() {
        category.score_percent = if category.executable_cases == 0 {
            0.0
        } else {
            category.passed_cases as f64 * 100.0 / category.executable_cases as f64
        };
    }
    by_category.into_values().collect()
}

fn record_pg_case<F>(
    cases: &mut Vec<PostgresCompatCaseReport>,
    id: &str,
    category: &str,
    description: &str,
    expectation: PostgresCompatExpectation,
    run: F,
) where
    F: FnOnce() -> Result<String>,
{
    let started = Instant::now();
    let result = run();
    let elapsed_ms = duration_ms(started.elapsed());
    let coverage_state = match expectation {
        PostgresCompatExpectation::Works => PostgresCompatCoverageState::Supported,
        PostgresCompatExpectation::ClearUnsupportedError => {
            PostgresCompatCoverageState::Unsupported
        }
        PostgresCompatExpectation::ExpectedDifference => {
            PostgresCompatCoverageState::ExpectedDifference
        }
        PostgresCompatExpectation::NotYetTested => PostgresCompatCoverageState::NotYetTested,
    };
    match result {
        Ok(detail) => cases.push(PostgresCompatCaseReport {
            id: id.to_string(),
            category: category.to_string(),
            description: description.to_string(),
            expectation,
            status: PostgresCompatStatus::Passed,
            coverage_state,
            elapsed_ms,
            detail,
        }),
        Err(error) => cases.push(PostgresCompatCaseReport {
            id: id.to_string(),
            category: category.to_string(),
            description: description.to_string(),
            expectation,
            status: PostgresCompatStatus::Failed,
            coverage_state,
            elapsed_ms,
            detail: error.to_string(),
        }),
    }
}

fn add_pg_documented_case(
    cases: &mut Vec<PostgresCompatCaseReport>,
    id: &str,
    category: &str,
    description: &str,
    expectation: PostgresCompatExpectation,
    coverage_state: PostgresCompatCoverageState,
    detail: &str,
) {
    cases.push(PostgresCompatCaseReport {
        id: id.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        expectation,
        status: PostgresCompatStatus::NotRun,
        coverage_state,
        elapsed_ms: 0.0,
        detail: detail.to_string(),
    });
}

fn expect_rows(rows: Vec<Vec<String>>, expected: &[&[&str]]) -> Result<String> {
    let expected_rows = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    if rows == expected_rows {
        return Ok(format!("returned {}", format_rows(&rows)));
    }
    anyhow::bail!(
        "expected {}, got {}",
        format_rows(&expected_rows),
        format_rows(&rows)
    );
}

fn expect_nonempty_first_cell(rows: Vec<Vec<String>>, label: &str) -> Result<String> {
    let value = rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| anyhow::anyhow!("{label} returned no rows"))?;
    if value.is_empty() {
        anyhow::bail!("{label} returned an empty value");
    }
    Ok(format!("{label}={value}"))
}

fn format_rows(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return "[]".to_string();
    }
    format!("{rows:?}")
}

fn compact_error(error: &str) -> String {
    error
        .split('\0')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn postgres_compat_known_gaps() -> Vec<PostgresCompatGap> {
    vec![
        PostgresCompatGap {
            category: "catalog".to_string(),
            feature: "Full pg_catalog coverage".to_string(),
            status: "broader virtual catalogs cover common database, namespace, class, attribute, type, proc, trigger, sequence, view, index, constraint, default-expression, access-method, tablespace, role, and empty unsupported-object introspection; complete PostgreSQL catalog parity is not implemented".to_string(),
            recommendation: "Continue expanding pg_catalog tables and storage-specific columns from real client introspection failures without faking unsupported storage semantics".to_string(),
        },
        PostgresCompatGap {
            category: "sql".to_string(),
            feature: "Recursive CTEs, window-frame exclusion clauses, binary COPY and advanced COPY options, exhaustive ALTER TABLE parity, regex expressions, and persisted arrays".to_string(),
            status: "non-recursive SELECT CTEs, metadata-only SQL routines/triggers, narrow PL/pgSQL trigger notification hooks, inner and outer joins, GROUP BY, ranking/aggregate/value window functions with ROWS/RANGE/GROUPS frames, grouped-window execution, named-window inheritance, NULL treatment, subqueries, common migration-style ALTER TABLE, baseline expressions/casts, one-dimensional persisted arrays, business date/interval arithmetic, and basic COPY text/CSV import/export work; general procedural execution, complete trigger firing, non-PL/pgSQL languages, window-frame exclusion clauses, row constructors, multidimensional/lower-bound-complete arrays, and broader SQL remain unsupported with explicit errors".to_string(),
            recommendation: "Keep unsupported SQL failing clearly while adding planner and execution support incrementally".to_string(),
        },
        PostgresCompatGap {
            category: "auth_tls".to_string(),
            feature: "Advanced authentication and TLS policy".to_string(),
            status: "cleartext password auth, opt-in SCRAM-SHA-256, native Rustls TLS listener support, and TLS-required plaintext startup rejection exist; client certificates, SCRAM channel binding, and full PostgreSQL auth policy coverage remain unsupported".to_string(),
            recommendation: "Keep local defaults ergonomic while adding explicit, fail-closed coverage for unsupported PostgreSQL auth policy surfaces".to_string(),
        },
        PostgresCompatGap {
            category: "transactions".to_string(),
            feature: "Serializable and repeatable-read isolation".to_string(),
            status: "READ COMMITTED-style local transactions with savepoints; READ COMMITTED isolation commands are accepted, stronger isolation levels are rejected with SQLSTATE 0A000".to_string(),
            recommendation: "Keep rejecting unsupported stronger isolation until real repeatable-read snapshots or serializable validation are implemented".to_string(),
        },
        PostgresCompatGap {
            category: "protocol".to_string(),
            feature: "Cooperative query cancellation preemption".to_string(),
            status: "common pipelined Extended Query prepared flows are covered; CancelRequest is accepted and interrupts cancellable pgwire paths such as SELECT pg_sleep(...), scan filtering, row joins, grouping/projection, vector ordering, ANN search, and COPY finalization with SQLSTATE 57014".to_string(),
            recommendation: "Keep cancellation cooperative at natural loop boundaries, and add explicit checkpoints as new long-running SQL/storage paths are introduced".to_string(),
        },
        PostgresCompatGap {
            category: "client_matrix".to_string(),
            feature: "DBeaver, DataGrip, TablePlus, Prisma, SQLAlchemy, psycopg, node-postgres, tokio-postgres live runs".to_string(),
            status: "node-postgres, psycopg, SQLAlchemy, and tokio-postgres have automated gauntlet coverage for DDL, CRUD, rollback, and prepared-parameter flows; Prisma introspection has a reproducible manual script and currently exposes a BicDB parameterized introspection gap; GUI clients have documented manual SQL smoke coverage".to_string(),
            recommendation: "Continue converting real external client introspection failures into focused fixtures and client gauntlet cases".to_string(),
        },
    ]
}

#[derive(Debug)]
struct PgQueryOutcome {
    rows: Vec<Vec<String>>,
    error: Option<String>,
    ready_status: u8,
}

fn bench_pg_connect(address: SocketAddr) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(address).context("connect benchmark pgwire client")?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    bench_pg_startup(&mut stream)?;
    bench_pg_read_until_ready(&mut stream)?;
    Ok(stream)
}

fn bench_pg_connect_with_backend_key(address: SocketAddr) -> Result<(TcpStream, (i32, i32))> {
    let mut stream = TcpStream::connect(address).context("connect benchmark pgwire client")?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    bench_pg_startup(&mut stream)?;
    let backend_key = bench_pg_read_backend_key_until_ready(&mut stream)?;
    Ok((stream, backend_key))
}

fn bench_pg_startup(stream: &mut TcpStream) -> Result<()> {
    bench_pg_startup_version(stream, 196_608, true)
}

fn bench_pg_startup_version(
    stream: &mut TcpStream,
    protocol_version: i32,
    terminated: bool,
) -> Result<()> {
    bench_pg_startup_version_with_options(stream, protocol_version, terminated, &[])
}

fn bench_pg_startup_version_with_options(
    stream: &mut TcpStream,
    protocol_version: i32,
    terminated: bool,
    options: &[(&str, &str)],
) -> Result<()> {
    let mut payload = Vec::new();
    bench_put_i32(&mut payload, protocol_version);
    bench_cstr(&mut payload, "user");
    bench_cstr(&mut payload, "bicdb");
    bench_cstr(&mut payload, "database");
    bench_cstr(&mut payload, "bicdb");
    for (key, value) in options {
        bench_cstr(&mut payload, key);
        bench_cstr(&mut payload, value);
    }
    if terminated {
        payload.push(0);
    }

    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(&payload)?;
    Ok(())
}

fn bench_pg_read_negotiate_protocol_version(stream: &mut TcpStream) -> Result<(i32, Vec<String>)> {
    let (tag, payload) = bench_pg_read_message(stream)?;
    if tag != b'v' {
        anyhow::bail!("expected NegotiateProtocolVersion, got tag {}", tag as char);
    }
    if payload.len() < 8 {
        anyhow::bail!("NegotiateProtocolVersion payload too short");
    }
    let protocol_version = i32::from_be_bytes(payload[0..4].try_into().unwrap());
    let option_count = i32::from_be_bytes(payload[4..8].try_into().unwrap());
    if option_count < 0 {
        anyhow::bail!("NegotiateProtocolVersion option count is negative");
    }
    let mut idx = 8;
    let mut options = Vec::new();
    for _ in 0..option_count {
        let end = payload[idx..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| idx + offset)
            .ok_or_else(|| anyhow::anyhow!("NegotiateProtocolVersion option missing terminator"))?;
        options.push(String::from_utf8_lossy(&payload[idx..end]).to_string());
        idx = end + 1;
    }
    Ok((protocol_version, options))
}

fn bench_pg_read_startup_error(stream: &mut TcpStream) -> Result<String> {
    loop {
        let (tag, payload) = bench_pg_read_message(stream)?;
        if tag == b'E' {
            return Ok(String::from_utf8_lossy(&payload).to_string());
        }
    }
}

fn bench_pg_ssl_request(address: SocketAddr) -> Result<u8> {
    let mut stream = TcpStream::connect(address).context("connect SSLRequest probe client")?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    stream.write_all(&8_i32.to_be_bytes())?;
    stream.write_all(&80_877_103_i32.to_be_bytes())?;
    let mut response = [0_u8; 1];
    stream.read_exact(&mut response)?;
    if response[0] == b'N' {
        bench_pg_startup(&mut stream)?;
        bench_pg_read_until_ready(&mut stream)?;
        bench_pg_close(&mut stream)?;
    }
    Ok(response[0])
}

fn bench_pg_simple_query(stream: &mut TcpStream, query: &str) -> Result<Vec<Vec<String>>> {
    Ok(bench_pg_simple_query_outcome(stream, query)?.rows)
}

fn bench_pg_simple_query_outcome(stream: &mut TcpStream, query: &str) -> Result<PgQueryOutcome> {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q")?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(&payload)?;
    bench_pg_read_outcome(stream)
}

fn bench_pg_simple_query_error(stream: &mut TcpStream, query: &str) -> Result<String> {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q")?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(&payload)?;
    let outcome = bench_pg_read_outcome(stream)?;
    let error = outcome.error.ok_or_else(|| {
        anyhow::anyhow!("query unexpectedly succeeded with rows {:?}", outcome.rows)
    });
    if outcome.ready_status == b'E' || outcome.ready_status == b'T' {
        let rollback = bench_pg_simple_query_outcome(stream, "ROLLBACK;")?;
        if rollback.ready_status != b'I' {
            anyhow::bail!(
                "expected ReadyForQuery idle after guardrail recovery rollback, got {}",
                rollback.ready_status as char
            );
        }
    }
    error
}

fn bench_pg_copy_from_stdin_csv(stream: &mut TcpStream, query: &str, data: &str) -> Result<()> {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q")?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(&payload)?;
    let (tag, response) = bench_pg_read_message(stream)?;
    if tag != b'G' {
        anyhow::bail!(
            "expected CopyInResponse, got tag {} payload {:?}",
            tag as char,
            response
        );
    }
    bench_pg_send_message(stream, b'd', data.as_bytes())?;
    bench_pg_send_message(stream, b'c', &[])?;
    let outcome = bench_pg_read_outcome(stream)?;
    if let Some(error) = outcome.error {
        anyhow::bail!("COPY FROM STDIN failed: {}", compact_error(&error));
    }
    if outcome.ready_status != b'I' {
        anyhow::bail!(
            "expected ReadyForQuery idle after COPY FROM, got {}",
            outcome.ready_status as char
        );
    }
    Ok(())
}

fn bench_pg_copy_to_stdout_csv(stream: &mut TcpStream, query: &str) -> Result<Vec<String>> {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q")?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(&payload)?;

    let mut rows = Vec::new();
    let mut saw_copy_out = false;
    loop {
        let (tag, payload) = bench_pg_read_message(stream)?;
        match tag {
            b'H' => saw_copy_out = true,
            b'd' => rows.push(String::from_utf8_lossy(&payload).to_string()),
            b'E' => anyhow::bail!(
                "COPY TO STDOUT failed: {}",
                compact_error(&String::from_utf8_lossy(&payload))
            ),
            b'Z' => {
                if !saw_copy_out {
                    anyhow::bail!("COPY TO STDOUT did not send CopyOutResponse");
                }
                if payload.first().copied() != Some(b'I') {
                    anyhow::bail!("expected ReadyForQuery idle after COPY TO");
                }
                return Ok(rows);
            }
            _ => {}
        }
    }
}

fn bench_pg_extended_query(
    stream: &mut TcpStream,
    statement: &str,
    sql: &str,
    oids: &[i32],
    params: &[&str],
) -> Result<Vec<Vec<String>>> {
    bench_pg_send_parse(stream, statement, sql, oids)?;
    bench_pg_send_bind(stream, "", statement, params)?;
    bench_pg_send_describe_portal(stream, "")?;
    bench_pg_send_execute(stream, "")?;
    bench_pg_send_sync(stream)?;
    bench_pg_read_rows(stream)
}

fn bench_pg_extended_close_query(
    stream: &mut TcpStream,
    statement: &str,
    sql: &str,
    oids: &[i32],
    params: &[&str],
) -> Result<Vec<Vec<String>>> {
    bench_pg_send_parse(stream, statement, sql, oids)?;
    bench_pg_send_bind(stream, "", statement, params)?;
    bench_pg_send_describe_portal(stream, "")?;
    bench_pg_send_execute(stream, "")?;
    bench_pg_send_close(stream, b'P', "")?;
    bench_pg_send_close(stream, b'S', statement)?;
    bench_pg_send_sync(stream)?;
    let mut rows = Vec::new();
    let mut close_count = 0;
    loop {
        let (tag, payload) = bench_pg_read_message(stream)?;
        match tag {
            b'D' => rows.push(bench_parse_data_row(&payload)?),
            b'E' => anyhow::bail!(
                "pgwire extended close query failed: {}",
                String::from_utf8_lossy(&payload)
            ),
            b'3' => close_count += 1,
            b'Z' => {
                if payload.first().copied() != Some(b'I') {
                    anyhow::bail!("expected ReadyForQuery idle after Close flow");
                }
                if close_count != 2 {
                    anyhow::bail!("expected two CloseComplete messages, got {close_count}");
                }
                return Ok(rows);
            }
            _ => {}
        }
    }
}

fn bench_pg_extended_error_recovery(stream: &mut TcpStream) -> Result<String> {
    bench_pg_send_bind(stream, "", "missing_statement", &[])?;
    bench_pg_send_execute(stream, "")?;
    bench_pg_send_sync(stream)?;
    let outcome = bench_pg_read_outcome(stream)?;
    if outcome.ready_status != b'I' {
        anyhow::bail!(
            "expected ReadyForQuery idle after Sync recovery, got {}",
            outcome.ready_status as char
        );
    }
    let error = outcome
        .error
        .ok_or_else(|| anyhow::anyhow!("extended protocol error unexpectedly succeeded"))?;
    let rows = bench_pg_simple_query(stream, "SELECT 1;")?;
    expect_rows(rows, &[&["1"]])?;
    Ok(error)
}

fn bench_pg_extended_pipeline_query(stream: &mut TcpStream) -> Result<Vec<Vec<String>>> {
    expect_rows(
        bench_pg_simple_query(
            stream,
            "CREATE TABLE pipeline_scorecard (id TEXT PRIMARY KEY, name TEXT);",
        )?,
        &[],
    )?;
    bench_pg_send_parse(
        stream,
        "pipeline_scorecard_insert",
        "INSERT INTO pipeline_scorecard (id, name) VALUES ($1, $2)",
        &[25, 25],
    )?;
    bench_pg_send_bind(
        stream,
        "pipeline_scorecard_insert_portal",
        "pipeline_scorecard_insert",
        &["p1", "Pipelined"],
    )?;
    bench_pg_send_execute(stream, "pipeline_scorecard_insert_portal")?;
    bench_pg_send_parse(
        stream,
        "pipeline_scorecard_select",
        "SELECT name FROM pipeline_scorecard WHERE id = $1",
        &[25],
    )?;
    bench_pg_send_bind(
        stream,
        "pipeline_scorecard_select_portal",
        "pipeline_scorecard_select",
        &["p1"],
    )?;
    bench_pg_send_describe_portal(stream, "pipeline_scorecard_select_portal")?;
    bench_pg_send_execute(stream, "pipeline_scorecard_select_portal")?;
    bench_pg_send_sync(stream)?;
    bench_pg_read_rows(stream)
}

fn bench_pg_cancel_request_smoke(address: SocketAddr) -> Result<String> {
    let (mut stream, backend_key) = bench_pg_connect_with_backend_key(address)?;
    bench_pg_simple_query_raw(&mut stream, "SELECT pg_sleep(2);")?;
    thread::sleep(Duration::from_millis(100));
    bench_pg_send_cancel_request(address, backend_key)?;
    let outcome = bench_pg_read_outcome(&mut stream)?;
    if outcome.ready_status != b'I' {
        anyhow::bail!(
            "expected ReadyForQuery idle after cancel, got {}",
            outcome.ready_status as char
        );
    }
    let error = outcome
        .error
        .ok_or_else(|| anyhow::anyhow!("cancelled query unexpectedly succeeded"))?;
    if !error.contains("57014") {
        anyhow::bail!("expected SQLSTATE 57014 cancel error, got {error:?}");
    }
    expect_rows(bench_pg_simple_query(&mut stream, "SELECT 1;")?, &[&["1"]])?;
    bench_pg_close(&mut stream)?;
    Ok("CancelRequest returned 57014 and SELECT 1 succeeded after ReadyForQuery I".to_string())
}

fn bench_pg_simple_query_raw(stream: &mut TcpStream, query: &str) -> Result<()> {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q")?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(&payload)?;
    Ok(())
}

fn bench_pg_send_cancel_request(address: SocketAddr, backend_key: (i32, i32)) -> Result<()> {
    let mut stream = TcpStream::connect(address).context("connect CancelRequest client")?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    stream.write_all(&16_i32.to_be_bytes())?;
    stream.write_all(&80_877_102_i32.to_be_bytes())?;
    stream.write_all(&backend_key.0.to_be_bytes())?;
    stream.write_all(&backend_key.1.to_be_bytes())?;
    Ok(())
}

fn bench_pg_close(stream: &mut TcpStream) -> Result<()> {
    stream.write_all(b"X\0\0\0\x04")?;
    Ok(())
}

fn bench_pg_read_until_ready(stream: &mut TcpStream) -> Result<u8> {
    loop {
        let (tag, payload) = bench_pg_read_message(stream)?;
        if tag == b'Z' {
            return payload
                .first()
                .copied()
                .ok_or_else(|| anyhow::anyhow!("ReadyForQuery missing transaction status"));
        }
    }
}

fn bench_pg_read_backend_key_until_ready(stream: &mut TcpStream) -> Result<(i32, i32)> {
    let mut backend_key = None;
    loop {
        let (tag, payload) = bench_pg_read_message(stream)?;
        match tag {
            b'K' => {
                if payload.len() < 8 {
                    anyhow::bail!("BackendKeyData payload too short");
                }
                backend_key = Some((
                    i32::from_be_bytes(payload[0..4].try_into().unwrap()),
                    i32::from_be_bytes(payload[4..8].try_into().unwrap()),
                ));
            }
            b'Z' => {
                return backend_key
                    .ok_or_else(|| anyhow::anyhow!("startup did not return BackendKeyData"));
            }
            _ => {}
        }
    }
}

fn bench_pg_read_rows(stream: &mut TcpStream) -> Result<Vec<Vec<String>>> {
    let outcome = bench_pg_read_outcome(stream)?;
    if let Some(error) = outcome.error {
        anyhow::bail!("pgwire benchmark query failed: {error}");
    }
    Ok(outcome.rows)
}

fn bench_pg_read_outcome(stream: &mut TcpStream) -> Result<PgQueryOutcome> {
    let mut rows = Vec::new();
    let mut error = None;
    loop {
        let (tag, payload) = bench_pg_read_message(stream)?;
        match tag {
            b'D' => rows.push(bench_parse_data_row(&payload)?),
            b'E' => error = Some(String::from_utf8_lossy(&payload).to_string()),
            b'Z' => {
                let ready_status = payload
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("ReadyForQuery missing transaction status"))?;
                return Ok(PgQueryOutcome {
                    rows,
                    error,
                    ready_status,
                });
            }
            _ => {}
        }
    }
}

fn bench_pg_send_parse(stream: &mut TcpStream, name: &str, sql: &str, oids: &[i32]) -> Result<()> {
    let mut payload = Vec::new();
    bench_cstr(&mut payload, name);
    bench_cstr(&mut payload, sql);
    bench_put_i16(&mut payload, oids.len() as i16);
    for oid in oids {
        bench_put_i32(&mut payload, *oid);
    }
    bench_pg_send_message(stream, b'P', &payload)
}

fn bench_pg_send_bind(
    stream: &mut TcpStream,
    portal: &str,
    statement: &str,
    params: &[&str],
) -> Result<()> {
    let mut payload = Vec::new();
    bench_cstr(&mut payload, portal);
    bench_cstr(&mut payload, statement);
    bench_put_i16(&mut payload, 0);
    bench_put_i16(&mut payload, params.len() as i16);
    for param in params {
        bench_put_i32(&mut payload, param.len() as i32);
        payload.extend_from_slice(param.as_bytes());
    }
    bench_put_i16(&mut payload, 0);
    bench_pg_send_message(stream, b'B', &payload)
}

fn bench_pg_send_describe_portal(stream: &mut TcpStream, portal: &str) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(b'P');
    bench_cstr(&mut payload, portal);
    bench_pg_send_message(stream, b'D', &payload)
}

fn bench_pg_send_execute(stream: &mut TcpStream, portal: &str) -> Result<()> {
    let mut payload = Vec::new();
    bench_cstr(&mut payload, portal);
    bench_put_i32(&mut payload, 0);
    bench_pg_send_message(stream, b'E', &payload)
}

fn bench_pg_send_close(stream: &mut TcpStream, target: u8, name: &str) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(target);
    bench_cstr(&mut payload, name);
    bench_pg_send_message(stream, b'C', &payload)
}

fn bench_pg_send_sync(stream: &mut TcpStream) -> Result<()> {
    bench_pg_send_message(stream, b'S', &[])
}

fn bench_pg_send_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> Result<()> {
    stream.write_all(&[tag])?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

fn bench_pg_read_message(stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag)?;
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len)?;
    let len = i32::from_be_bytes(len);
    if len < 4 {
        anyhow::bail!("invalid pgwire message length {len}");
    }
    let mut payload = vec![0_u8; (len - 4) as usize];
    stream.read_exact(&mut payload)?;
    Ok((tag[0], payload))
}

fn bench_parse_data_row(payload: &[u8]) -> Result<Vec<String>> {
    if payload.len() < 2 {
        anyhow::bail!("invalid pgwire data row");
    }
    let count = i16::from_be_bytes(payload[0..2].try_into()?) as usize;
    let mut idx = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if idx + 4 > payload.len() {
            anyhow::bail!("invalid pgwire data row value header");
        }
        let len = i32::from_be_bytes(payload[idx..idx + 4].try_into()?);
        idx += 4;
        if len < 0 {
            values.push(String::new());
            continue;
        }
        let end = idx + len as usize;
        if end > payload.len() {
            anyhow::bail!("invalid pgwire data row value length");
        }
        values.push(String::from_utf8(payload[idx..end].to_vec())?);
        idx = end;
    }
    Ok(values)
}

fn bench_cstr(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
}

fn bench_put_i32(payload: &mut Vec<u8>, value: i32) {
    payload.extend_from_slice(&value.to_be_bytes());
}

fn bench_put_i16(payload: &mut Vec<u8>, value: i16) {
    payload.extend_from_slice(&value.to_be_bytes());
}

fn default_diff_expectation() -> PostgresDiffExpectation {
    PostgresDiffExpectation::Match
}

fn load_postgres_diff_fixtures(fixtures_dir: Option<&Path>) -> Result<Vec<PostgresDiffFixture>> {
    if let Some(fixtures_dir) = fixtures_dir {
        let mut paths = fs::read_dir(fixtures_dir)
            .with_context(|| format!("read fixtures dir {}", fixtures_dir.display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
        paths.sort();

        let mut fixtures = Vec::new();
        for path in paths {
            let data = fs::read_to_string(&path)
                .with_context(|| format!("read fixture {}", path.display()))?;
            let fixture: PostgresDiffFixture = serde_json::from_str(&data)
                .with_context(|| format!("parse fixture {}", path.display()))?;
            fixtures.push(fixture);
        }

        if fixtures.is_empty() {
            anyhow::bail!("no JSON fixtures found in {}", fixtures_dir.display());
        }

        return Ok(fixtures);
    }

    Ok(default_postgres_diff_fixtures())
}

fn default_postgres_diff_fixtures() -> Vec<PostgresDiffFixture> {
    vec![
        PostgresDiffFixture {
            id: "select_1".to_string(),
            description: "SELECT 1 parity".to_string(),
            cleanup_sql: Vec::new(),
            sql: vec!["SELECT 1;".to_string()],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "create_insert_select".to_string(),
            description: "CREATE TABLE, INSERT, and SELECT parity".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS diff_patients;".to_string()],
            sql: vec![
                "CREATE TABLE diff_patients (id TEXT PRIMARY KEY, name TEXT, age INT);".to_string(),
                "INSERT INTO diff_patients (id, name, age) VALUES ('p1', 'Ada', 36);".to_string(),
                "SELECT id, name, age FROM diff_patients WHERE id = 'p1';".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "constraints_foreign_keys".to_string(),
            description: "Common constraint enforcement and foreign-key update/delete actions"
                .to_string(),
            cleanup_sql: vec![
                "DROP TABLE IF EXISTS diff_fk_children;".to_string(),
                "DROP TABLE IF EXISTS diff_fk_parents;".to_string(),
            ],
            sql: vec![
                "CREATE TABLE diff_fk_parents (id TEXT PRIMARY KEY, code TEXT NOT NULL UNIQUE, label TEXT CHECK (label <> ''));".to_string(),
                "CREATE TABLE diff_fk_children (id TEXT PRIMARY KEY, parent_code TEXT, CONSTRAINT diff_fk_children_parent_fkey FOREIGN KEY (parent_code) REFERENCES diff_fk_parents(code) ON UPDATE CASCADE ON DELETE SET NULL);".to_string(),
                "INSERT INTO diff_fk_parents (id, code, label) VALUES ('p1', 'alpha', 'Alpha');".to_string(),
                "INSERT INTO diff_fk_children (id, parent_code) VALUES ('c1', 'alpha');".to_string(),
                "UPDATE diff_fk_parents SET code = 'beta' WHERE code = 'alpha';".to_string(),
                "SELECT parent_code FROM diff_fk_children WHERE id = 'c1';".to_string(),
                "DELETE FROM diff_fk_parents WHERE code = 'beta';".to_string(),
                "SELECT parent_code FROM diff_fk_children WHERE id = 'c1';".to_string(),
                "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'diff_fk_children' ORDER BY constraint_name;".to_string(),
                "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE conname = 'diff_fk_children_parent_fkey';".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "outer_joins".to_string(),
            description: "LEFT, RIGHT, and FULL OUTER JOIN NULL-extension parity".to_string(),
            cleanup_sql: vec![
                "DROP TABLE IF EXISTS diff_join_appointments;".to_string(),
                "DROP TABLE IF EXISTS diff_join_patients;".to_string(),
            ],
            sql: vec![
                "CREATE TABLE diff_join_patients (id TEXT PRIMARY KEY, name TEXT);".to_string(),
                "CREATE TABLE diff_join_appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT);".to_string(),
                "INSERT INTO diff_join_patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada');".to_string(),
                "INSERT INTO diff_join_appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p3', 'Dr. Kim');".to_string(),
                "SELECT diff_join_patients.id, diff_join_patients.name, diff_join_appointments.doctor FROM diff_join_patients LEFT JOIN diff_join_appointments ON diff_join_patients.id = diff_join_appointments.patient_id ORDER BY diff_join_patients.id;".to_string(),
                "SELECT diff_join_patients.name, diff_join_appointments.id, diff_join_appointments.doctor FROM diff_join_patients RIGHT JOIN diff_join_appointments ON diff_join_patients.id = diff_join_appointments.patient_id ORDER BY diff_join_appointments.id;".to_string(),
                "SELECT diff_join_patients.id, diff_join_appointments.id, diff_join_appointments.doctor FROM diff_join_patients FULL OUTER JOIN diff_join_appointments ON diff_join_patients.id = diff_join_appointments.patient_id WHERE diff_join_patients.id IS NULL OR diff_join_appointments.id IS NULL ORDER BY diff_join_patients.id, diff_join_appointments.id;".to_string(),
                "SELECT diff_join_patients.id FROM diff_join_patients LEFT JOIN diff_join_appointments ON diff_join_patients.id = diff_join_appointments.patient_id WHERE diff_join_appointments.id IS NULL;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "expressions_operators_casts".to_string(),
            description: "Arithmetic, boolean NULL behavior, CASE, COALESCE, LIKE/ILIKE, IN, concatenation, and common casts".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS diff_exprs;".to_string()],
            sql: vec![
                "CREATE TABLE diff_exprs (id TEXT PRIMARY KEY, name TEXT, age INT, score FLOAT8, active BOOLEAN, joined DATE);".to_string(),
                "INSERT INTO diff_exprs (id, name, age, score, active, joined) VALUES ('e1', 'Ada', 36, 9.5, true, '2024-01-02'::date), ('e2', 'Bob', NULL, NULL, false, NULL), ('e3', 'ALICE', 41, 7.0, NULL, '2023-12-31'::date);".to_string(),
                "SELECT id, age + 4 AS age_plus, score * 2 AS doubled, name || '-x' AS label FROM diff_exprs WHERE id = 'e1';".to_string(),
                "SELECT id, CASE WHEN age IS NULL THEN 'missing' WHEN age >= 40 THEN 'senior' ELSE 'adult' END AS band, COALESCE(age::text, 'n/a') AS age_text FROM diff_exprs ORDER BY id;".to_string(),
                "SELECT id FROM diff_exprs WHERE (active OR age > 40) AND name ILIKE 'a%' ORDER BY id;".to_string(),
                "SELECT id FROM diff_exprs WHERE name LIKE 'A%' AND id IN ('e1', 'e3') ORDER BY id;".to_string(),
                "SELECT id FROM diff_exprs WHERE age = NULL OR active = NULL ORDER BY id;".to_string(),
                "SELECT '123'::int4 AS cast_int, 7::text AS cast_text, '2024-01-02'::date AS cast_date;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "type_breadth_supported".to_string(),
            description: "Supported numeric, interval, date/time variants, casts, and pg_type catalog visibility".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS diff_type_breadth;".to_string()],
            sql: vec![
                "CREATE TABLE diff_type_breadth (id TEXT PRIMARY KEY, amount NUMERIC, day DATE, at_time TIME, span INTERVAL, starts_at TIMESTAMP WITH TIME ZONE);".to_string(),
                "INSERT INTO diff_type_breadth (id, amount, day, at_time, span, starts_at) VALUES ('t1', '12.30'::numeric, '2024-01-02'::date, '02:03:04'::time, '1 day'::interval, '2024-01-02 03:04:05+00'::timestamptz);".to_string(),
                "SELECT amount, day, at_time, span, starts_at FROM diff_type_breadth WHERE id = 't1';".to_string(),
                "SELECT '7'::numeric AS cast_numeric, '1 day'::interval AS cast_interval, '2024-01-02 03:04:05'::timestamp AS cast_timestamp;".to_string(),
                "SELECT typname, oid FROM pg_catalog.pg_type WHERE typname IN ('numeric', 'interval') ORDER BY typname;".to_string(),
                "SELECT a.attname, a.atttypid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid WHERE c.relname = 'diff_type_breadth' AND a.attname IN ('amount', 'span') ORDER BY a.attname;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "views".to_string(),
            description: "CREATE VIEW, SELECT from simple and join views, DROP VIEW, and catalog introspection".to_string(),
            cleanup_sql: vec![
                "DROP VIEW IF EXISTS diff_patient_appointments;".to_string(),
                "DROP VIEW IF EXISTS diff_patient_names;".to_string(),
                "DROP TABLE IF EXISTS diff_view_appointments;".to_string(),
                "DROP TABLE IF EXISTS diff_view_patients;".to_string(),
            ],
            sql: vec![
                "CREATE TABLE diff_view_patients (id TEXT PRIMARY KEY, name TEXT, age INT);".to_string(),
                "CREATE TABLE diff_view_appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT);".to_string(),
                "INSERT INTO diff_view_patients (id, name, age) VALUES ('p1', 'Ada', 36), ('p2', 'John', 45);".to_string(),
                "INSERT INTO diff_view_appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p2', 'Dr. Kim');".to_string(),
                "CREATE VIEW diff_patient_names AS SELECT id, name FROM diff_view_patients;".to_string(),
                "SELECT id, name FROM diff_patient_names ORDER BY id;".to_string(),
                "CREATE VIEW diff_patient_appointments AS SELECT diff_view_patients.name AS patient_name, diff_view_appointments.doctor AS doctor FROM diff_view_patients JOIN diff_view_appointments ON diff_view_patients.id = diff_view_appointments.patient_id;".to_string(),
                "SELECT patient_name, doctor FROM diff_patient_appointments WHERE doctor = 'Dr. Rao';".to_string(),
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'diff_patient_names';".to_string(),
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'diff_patient_names' ORDER BY ordinal_position;".to_string(),
                "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'diff_patient_names';".to_string(),
                "DROP VIEW diff_patient_appointments;".to_string(),
                "DROP VIEW diff_patient_names;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "cte_select".to_string(),
            description: "Non-recursive CTE SELECT parity".to_string(),
            cleanup_sql: Vec::new(),
            sql: vec!["WITH one(value) AS (SELECT 1) SELECT value FROM one;".to_string()],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "procedural_ddl".to_string(),
            description: "SQL-language CREATE/DROP FUNCTION and PROCEDURE plus pg_proc catalog visibility".to_string(),
            cleanup_sql: vec![
                "DROP PROCEDURE IF EXISTS diff_proc();".to_string(),
                "DROP FUNCTION IF EXISTS diff_fn();".to_string(),
            ],
            sql: vec![
                "CREATE FUNCTION diff_fn() RETURNS INT LANGUAGE SQL AS 'SELECT 42';".to_string(),
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'diff_fn';".to_string(),
                "CREATE PROCEDURE diff_proc() LANGUAGE SQL AS 'SELECT 1';".to_string(),
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'diff_proc';".to_string(),
                "DROP PROCEDURE diff_proc();".to_string(),
                "DROP FUNCTION diff_fn();".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "expected_difference_recursive_cte".to_string(),
            description: "Recursive CTE is explicitly rejected as unsupported".to_string(),
            cleanup_sql: Vec::new(),
            sql: vec![
                "WITH RECURSIVE nums(n) AS (SELECT 1) SELECT n FROM nums;".to_string(),
            ],
            expectation: PostgresDiffExpectation::ExpectedDifference,
            expected_difference: Some("BicDB explicitly rejects recursive CTEs".to_string()),
        },
        PostgresDiffFixture {
            id: "window_functions".to_string(),
            description: "Window functions execute with PostgreSQL-compatible ranking, frames, inheritance, and grouped aggregates".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS diff_window_patients;".to_string()],
            sql: vec![
                "CREATE TABLE diff_window_patients (id TEXT PRIMARY KEY, name TEXT, age INT);"
                    .to_string(),
                "INSERT INTO diff_window_patients (id, name, age) VALUES ('p1', 'Ada', 36), ('p2', 'John', 45);".to_string(),
                "SELECT id, ROW_NUMBER() OVER (ORDER BY age) FROM diff_window_patients ORDER BY id;".to_string(),
                "SELECT id, PERCENT_RANK() OVER ordered, CUME_DIST() OVER ordered, SUM(age) OVER framed FROM diff_window_patients WINDOW base AS (), ordered AS (base ORDER BY age), framed AS (ordered GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY id;".to_string(),
                "SELECT age, COUNT(*), RANK() OVER (ORDER BY COUNT(*)) FROM diff_window_patients GROUP BY age ORDER BY age;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "array_columns_supported_subset".to_string(),
            description: "Persisted one-dimensional text arrays round-trip with catalog OIDs"
                .to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS diff_array_types;".to_string()],
            sql: vec![
                "CREATE TABLE diff_array_types (id TEXT PRIMARY KEY, tags TEXT[]);".to_string(),
                "INSERT INTO diff_array_types (id, tags) VALUES ('a1', ARRAY['alpha', 'beta']::text[]);".to_string(),
                "SELECT id, tags FROM diff_array_types ORDER BY id;".to_string(),
                "SELECT a.attname, a.atttypid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 'diff_array_types' AND a.attname = 'tags';".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "enum_types".to_string(),
            description: "Enum type DDL, values, ordering, and catalogs match PostgreSQL".to_string(),
            cleanup_sql: vec![
                "DROP TABLE IF EXISTS diff_enum_rows;".to_string(),
                "DROP TYPE IF EXISTS diff_mood;".to_string(),
            ],
            sql: vec![
                "CREATE TYPE diff_mood AS ENUM ('happy', 'sad');".to_string(),
                "CREATE TABLE diff_enum_rows (id TEXT PRIMARY KEY, mood diff_mood);".to_string(),
                "INSERT INTO diff_enum_rows VALUES ('e1', 'happy');".to_string(),
                "SELECT id, mood FROM diff_enum_rows ORDER BY mood;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "domain_types".to_string(),
            description: "Domain DDL, constraints, defaults, casts, and catalogs match PostgreSQL".to_string(),
            cleanup_sql: vec![
                "DROP TABLE IF EXISTS diff_domain_rows;".to_string(),
                "DROP DOMAIN IF EXISTS diff_positive_int;".to_string(),
            ],
            sql: vec![
                "CREATE DOMAIN diff_positive_int AS INT CHECK (VALUE > 0);".to_string(),
                "CREATE TABLE diff_domain_rows (id TEXT PRIMARY KEY, amount diff_positive_int);".to_string(),
                "INSERT INTO diff_domain_rows VALUES ('d1', 1);".to_string(),
                "SELECT id, amount FROM diff_domain_rows;".to_string(),
            ],
            expectation: PostgresDiffExpectation::Match,
            expected_difference: None,
        },
        PostgresDiffFixture {
            id: "expected_difference_unsupported_expressions".to_string(),
            description: "Unsupported expression forms return stable unsupported errors".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS diff_expr_unsupported;".to_string()],
            sql: vec![
                "CREATE TABLE diff_expr_unsupported (id TEXT PRIMARY KEY, age INT);".to_string(),
                "INSERT INTO diff_expr_unsupported (id, age) VALUES ('e1', 36);".to_string(),
                "SELECT id FROM diff_expr_unsupported WHERE id SIMILAR TO 'e[0-9]';".to_string(),
            ],
            expectation: PostgresDiffExpectation::ExpectedDifference,
            expected_difference: Some(
                "BicDB explicitly rejects unsupported SIMILAR TO expressions".to_string(),
            ),
        },
        PostgresDiffFixture {
            id: "expected_difference_procedural_languages".to_string(),
            description: "Non-trigger PL/pgSQL functions are explicitly rejected as unsupported"
                .to_string(),
            cleanup_sql: vec![
                "DROP FUNCTION IF EXISTS diff_plpgsql_fn();".to_string(),
            ],
            sql: vec![
                "CREATE FUNCTION diff_plpgsql_fn() RETURNS INT LANGUAGE plpgsql AS 'BEGIN RETURN 1; END';".to_string(),
            ],
            expectation: PostgresDiffExpectation::ExpectedDifference,
            expected_difference: Some(
                "BicDB explicitly rejects non-trigger PL/pgSQL functions".to_string(),
            ),
        },
    ]
}

fn run_postgres_fixture(
    client: &mut PostgresClient,
    fixture: &PostgresDiffFixture,
) -> Vec<PostgresObservedStep> {
    for sql in &fixture.cleanup_sql {
        let _ = client.simple_query(sql);
    }

    fixture
        .sql
        .iter()
        .map(|sql| observe_postgres_step(client, sql))
        .collect()
}

fn observe_postgres_step(client: &mut PostgresClient, sql: &str) -> PostgresObservedStep {
    match client.simple_query(sql) {
        Ok(messages) => {
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let mut command_rows = None;
            for message in messages {
                match message {
                    SimpleQueryMessage::RowDescription(description) => {
                        columns = description
                            .iter()
                            .map(|column| PostgresObservedColumn {
                                name: column.name().to_string(),
                                type_oid: None,
                            })
                            .collect();
                    }
                    SimpleQueryMessage::Row(row) => {
                        rows.push(
                            (0..row.len())
                                .map(|idx| row.get(idx).map(str::to_string))
                                .collect(),
                        );
                    }
                    SimpleQueryMessage::CommandComplete(count) => {
                        command_rows = Some(count);
                    }
                    _ => {}
                }
            }

            if !rows.is_empty() || !columns.is_empty() {
                if let Ok(metadata_rows) = client.query(sql, &[]) {
                    if let Some(row) = metadata_rows.first() {
                        columns = row
                            .columns()
                            .iter()
                            .map(|column| PostgresObservedColumn {
                                name: column.name().to_string(),
                                type_oid: Some(column.type_().oid()),
                            })
                            .collect();
                    }
                }
            }

            PostgresObservedStep {
                sql: sql.to_string(),
                columns,
                rows,
                command_tag: None,
                command_rows,
                error: None,
            }
        }
        Err(error) => PostgresObservedStep {
            sql: sql.to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: None,
            command_rows: None,
            error: Some(postgres_error_observation(&error)),
        },
    }
}

fn run_bicdb_fixture(
    client: &mut TcpStream,
    fixture: &PostgresDiffFixture,
) -> Vec<PostgresObservedStep> {
    for sql in &fixture.cleanup_sql {
        let _ = observe_bicdb_step(client, sql);
    }

    fixture
        .sql
        .iter()
        .map(|sql| observe_bicdb_step(client, sql))
        .collect()
}

fn observe_bicdb_step(client: &mut TcpStream, sql: &str) -> PostgresObservedStep {
    let send = (|| -> Result<()> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        client.write_all(b"Q")?;
        client.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
        client.write_all(&payload)?;
        Ok(())
    })();

    if let Err(error) = send {
        return PostgresObservedStep {
            sql: sql.to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: None,
            command_rows: None,
            error: Some(compact_error(&error.to_string())),
        };
    }

    match read_bicdb_observed_step(sql, client) {
        Ok(step) => step,
        Err(error) => PostgresObservedStep {
            sql: sql.to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: None,
            command_rows: None,
            error: Some(compact_error(&error.to_string())),
        },
    }
}

fn read_bicdb_observed_step(sql: &str, client: &mut TcpStream) -> Result<PostgresObservedStep> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut command_tag = None;
    let mut command_rows = None;
    let mut error = None;

    loop {
        let (tag, payload) = bench_pg_read_message(client)?;
        match tag {
            b'T' => columns = parse_pg_row_description(&payload)?,
            b'D' => rows.push(parse_pg_data_row_optional(&payload)?),
            b'C' => {
                let tag = read_pg_cstr(&payload, 0)
                    .map(|(tag, _)| tag)
                    .unwrap_or_default();
                command_rows = parse_command_rows(&tag);
                command_tag = Some(tag);
            }
            b'E' => error = Some(bicdb_error_observation(&payload)),
            b'Z' => {
                return Ok(PostgresObservedStep {
                    sql: sql.to_string(),
                    columns,
                    rows,
                    command_tag,
                    command_rows,
                    error,
                })
            }
            _ => {}
        }
    }
}

fn parse_pg_row_description(payload: &[u8]) -> Result<Vec<PostgresObservedColumn>> {
    if payload.len() < 2 {
        anyhow::bail!("invalid pgwire row description");
    }
    let count = i16::from_be_bytes(payload[0..2].try_into()?) as usize;
    let mut idx = 2;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let (name, next) = read_pg_cstr(payload, idx)?;
        idx = next;
        if idx + 18 > payload.len() {
            anyhow::bail!("invalid pgwire row description field");
        }
        idx += 6;
        let type_oid = u32::from_be_bytes(payload[idx..idx + 4].try_into()?);
        idx += 12;
        columns.push(PostgresObservedColumn {
            name,
            type_oid: Some(type_oid),
        });
    }
    Ok(columns)
}

fn parse_pg_data_row_optional(payload: &[u8]) -> Result<Vec<Option<String>>> {
    if payload.len() < 2 {
        anyhow::bail!("invalid pgwire data row");
    }
    let count = i16::from_be_bytes(payload[0..2].try_into()?) as usize;
    let mut idx = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if idx + 4 > payload.len() {
            anyhow::bail!("invalid pgwire data row value header");
        }
        let len = i32::from_be_bytes(payload[idx..idx + 4].try_into()?);
        idx += 4;
        if len < 0 {
            values.push(None);
            continue;
        }
        let end = idx + len as usize;
        if end > payload.len() {
            anyhow::bail!("invalid pgwire data row value length");
        }
        values.push(Some(String::from_utf8(payload[idx..end].to_vec())?));
        idx = end;
    }
    Ok(values)
}

fn read_pg_cstr(payload: &[u8], start: usize) -> Result<(String, usize)> {
    let end = payload[start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| start + offset)
        .ok_or_else(|| anyhow::anyhow!("missing pgwire cstring terminator"))?;
    Ok((String::from_utf8(payload[start..end].to_vec())?, end + 1))
}

fn postgres_error_observation(error: &postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db_error| format!("SQLSTATE={}", db_error.code().code()))
        .unwrap_or_else(|| compact_error(&error.to_string()))
}

fn bicdb_error_observation(payload: &[u8]) -> String {
    parse_error_response_field(payload, b'C')
        .map(|code| format!("SQLSTATE={code}"))
        .unwrap_or_else(|| compact_error(&String::from_utf8_lossy(payload)))
}

fn parse_error_response_field(payload: &[u8], field_tag: u8) -> Option<String> {
    let mut idx = 0;
    while idx < payload.len() {
        let tag = payload[idx];
        idx += 1;
        if tag == 0 {
            break;
        }
        let end = payload[idx..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| idx + offset)?;
        if tag == field_tag {
            return Some(String::from_utf8_lossy(&payload[idx..end]).to_string());
        }
        idx = end + 1;
    }
    None
}

fn parse_command_rows(tag: &str) -> Option<u64> {
    tag.rsplit_once(' ')
        .and_then(|(_, count)| count.parse().ok())
}

fn markdown_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\n', "<br>")
}

fn compare_postgres_diff_fixture(
    fixture: PostgresDiffFixture,
    postgres: Vec<PostgresObservedStep>,
    bicdb: Vec<PostgresObservedStep>,
    elapsed_ms: f64,
) -> PostgresDiffCaseReport {
    let differences = diff_observations(&postgres, &bicdb);
    let (status, detail) = match fixture.expectation {
        PostgresDiffExpectation::Match if differences.is_empty() => {
            (PostgresDiffStatus::Passed, "matched PostgreSQL".to_string())
        }
        PostgresDiffExpectation::Match => (PostgresDiffStatus::Failed, differences.join("; ")),
        PostgresDiffExpectation::ExpectedDifference if differences.is_empty() => (
            PostgresDiffStatus::Failed,
            "expected a documented difference but observations matched".to_string(),
        ),
        PostgresDiffExpectation::ExpectedDifference => (
            PostgresDiffStatus::Passed,
            format!("expected difference: {}", differences.join("; ")),
        ),
    };

    PostgresDiffCaseReport {
        id: fixture.id,
        description: fixture.description,
        expectation: fixture.expectation,
        status,
        elapsed_ms,
        detail,
        metadata_differences: metadata_differences(&postgres, &bicdb),
        expected_difference: fixture.expected_difference,
        postgres,
        bicdb,
    }
}

fn metadata_differences(
    postgres: &[PostgresObservedStep],
    bicdb: &[PostgresObservedStep],
) -> Vec<String> {
    let mut differences = Vec::new();
    for (idx, (pg, bicdb)) in postgres.iter().zip(bicdb.iter()).enumerate() {
        let step = idx + 1;
        let pg_types = pg
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.type_oid))
            .collect::<Vec<_>>();
        let bicdb_types = bicdb
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.type_oid))
            .collect::<Vec<_>>();
        if pg_types != bicdb_types {
            differences.push(format!(
                "step {step} type OIDs differ: postgres={pg_types:?} bicdb={bicdb_types:?}"
            ));
        }
    }
    differences
}

fn diff_observations(
    postgres: &[PostgresObservedStep],
    bicdb: &[PostgresObservedStep],
) -> Vec<String> {
    let mut differences = Vec::new();
    if postgres.len() != bicdb.len() {
        differences.push(format!(
            "step count differs: postgres={} bicdb={}",
            postgres.len(),
            bicdb.len()
        ));
    }

    for (idx, (pg, bicdb)) in postgres.iter().zip(bicdb.iter()).enumerate() {
        let step = idx + 1;
        if pg.error != bicdb.error {
            differences.push(format!(
                "step {step} error differs: postgres={:?} bicdb={:?}",
                pg.error, bicdb.error
            ));
        }
        let pg_column_names = pg
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let bicdb_column_names = bicdb
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        if pg_column_names != bicdb_column_names {
            differences.push(format!("step {step} column names differ"));
        }
        if pg.rows != bicdb.rows {
            differences.push(format!("step {step} rows differ"));
        }
        if normalized_command_rows(pg.command_rows) != normalized_command_rows(bicdb.command_rows) {
            differences.push(format!(
                "step {step} command row count differs: postgres={:?} bicdb={:?}",
                pg.command_rows, bicdb.command_rows
            ));
        }
        if pg.command_tag.is_some()
            && bicdb.command_tag.is_some()
            && pg.command_tag != bicdb.command_tag
        {
            differences.push(format!(
                "step {step} command tag differs: postgres={:?} bicdb={:?}",
                pg.command_tag, bicdb.command_tag
            ));
        }
    }

    differences
}

fn normalized_command_rows(rows: Option<u64>) -> Option<u64> {
    rows.or(Some(0))
}

fn make_vector(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim.max(1))
        .map(|idx| (((seed * 31 + idx * 17) % 1000) as f32) / 1000.0)
        .collect()
}

fn insert_index_bench_records(
    db: &mut BicDb,
    collection: &str,
    records: usize,
    prefix: &str,
) -> Result<()> {
    let mut batch = Vec::with_capacity(5_000);
    for idx in 0..records {
        let metric = if idx % 2 == 0 { "hrv" } else { "steps" };
        batch.push(
            Record::new(format!("{prefix}-record-{idx}"))
                .with_timestamp(1_710_000_000 + idx as i64)
                .with_metadata(json!({
                    "device_id": format!("band-{}", idx % 256),
                    "metric": metric,
                    "clinic": format!("clinic-{}", idx % 64),
                    "value": (idx % 200) as f64 + 0.25,
                })),
        );
        if batch.len() >= 5_000 {
            db.batch_insert(collection, std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert(collection, batch)?;
    }
    Ok(())
}

fn create_index_bench_indexes(db: &mut BicDb, collection: &str, prefix: &str) -> Result<()> {
    for definition in [
        IndexDefinition {
            name: format!("{prefix}_{collection}_device"),
            collection: collection.to_string(),
            fields: vec![IndexField::MetadataPath(vec!["device_id".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        },
        IndexDefinition {
            name: format!("{prefix}_{collection}_device_metric"),
            collection: collection.to_string(),
            fields: vec![
                IndexField::MetadataPath(vec!["device_id".to_string()]),
                IndexField::MetadataPath(vec!["metric".to_string()]),
            ],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        },
        IndexDefinition {
            name: format!("{prefix}_{collection}_timestamp"),
            collection: collection.to_string(),
            fields: vec![IndexField::Timestamp],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        },
        IndexDefinition {
            name: format!("{prefix}_{collection}_clinic"),
            collection: collection.to_string(),
            fields: vec![IndexField::MetadataPath(vec!["clinic".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        },
    ] {
        db.create_index(definition)?;
    }
    Ok(())
}

fn insert_spatial_points(db: &mut BicDb, collection: &str, points: usize) -> Result<()> {
    let mut batch = Vec::with_capacity(5_000);
    for idx in 0..points {
        let (lon, lat) = spatial_point(idx);
        batch.push(
            Record::new(format!("pt{idx}"))
                .with_geometry(Geometry::point(lon, lat)?)
                .with_metadata(json!({
                    "tile": idx % 100,
                    "source": "synthetic_spatial_bench",
                })),
        );
        if batch.len() >= 5_000 {
            db.batch_insert(collection, std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.batch_insert(collection, batch)?;
    }
    Ok(())
}

fn spatial_point(idx: usize) -> (f64, f64) {
    let lon = -122.50 + ((idx * 37) % 1_000) as f64 * 0.0002;
    let lat = 37.70 + ((idx * 53) % 1_000) as f64 * 0.0002;
    (lon, lat)
}

fn spatial_query_point(idx: usize, points: usize) -> (f64, f64) {
    let base = (idx * 97) % points.max(1);
    let (lon, lat) = spatial_point(base);
    (lon + 0.00003, lat - 0.00002)
}

fn insert_route_graph(db: &mut BicDb, nodes: usize, edges: usize) -> Result<()> {
    let mut node_batch = Vec::with_capacity(5_000);
    for idx in 0..nodes {
        node_batch.push(
            Record::new(route_node_id(idx))
                .with_geometry(route_node_geometry(idx))
                .with_metadata(json!({
                    "lon": route_node_lon(idx),
                    "lat": route_node_lat(idx),
                    "source": "synthetic_route_bench",
                })),
        );
        if node_batch.len() >= 5_000 {
            db.batch_insert("roads_nodes", std::mem::take(&mut node_batch))?;
        }
    }
    if !node_batch.is_empty() {
        db.batch_insert("roads_nodes", node_batch)?;
    }

    let mut edge_batch = Vec::with_capacity(5_000);
    for idx in 0..edges {
        let from = if idx < nodes - 1 {
            idx
        } else {
            (idx * 37) % nodes
        };
        let mut to = if idx < nodes - 1 {
            idx + 1
        } else {
            (from + 1 + ((idx * 19) % (nodes - 1))) % nodes
        };
        if from == to {
            to = (to + 1) % nodes;
        }
        edge_batch.push(route_edge_record(idx, from, to));
        if edge_batch.len() >= 5_000 {
            db.batch_insert("roads_edges", std::mem::take(&mut edge_batch))?;
        }
    }
    if !edge_batch.is_empty() {
        db.batch_insert("roads_edges", edge_batch)?;
    }
    Ok(())
}

fn route_edge_record(idx: usize, from: usize, to: usize) -> Record {
    let distance_m = route_distance_m(from, to);
    Record::new(format!("edge{idx}")).with_metadata(json!({
        "from": route_node_id(from),
        "to": route_node_id(to),
        "distance_m": distance_m,
        "duration_s": distance_m / 13.4,
        "road_class": "synthetic",
        "metadata": {
            "source": "bench"
        }
    }))
}

fn route_node_geometry(idx: usize) -> Geometry {
    Geometry::point(route_node_lon(idx), route_node_lat(idx)).expect("synthetic route coordinate")
}

fn route_node_id(idx: usize) -> String {
    format!("node{idx}")
}

fn route_node_lon(idx: usize) -> f64 {
    -122.40 + (idx % 100) as f64 * 0.0002
}

fn route_node_lat(idx: usize) -> f64 {
    37.75 + (idx / 100) as f64 * 0.0002
}

fn route_distance_m(from: usize, to: usize) -> f64 {
    let dx = (route_node_lon(from) - route_node_lon(to)) * 111_320.0;
    let dy = (route_node_lat(from) - route_node_lat(to)) * 111_320.0;
    (dx.mul_add(dx, dy * dy)).sqrt().max(1.0)
}

fn timed_sql(engine: &SqlEngine<'_>, sql: &str) -> Result<(Duration, bicdb_sql::SqlResult)> {
    let started = Instant::now();
    let result = engine.execute(sql)?;
    Ok((started.elapsed(), result))
}

fn bench_event(idx: usize) -> Event {
    Event::new(
        "bench-events",
        "DeviceMeasurementReceived",
        json!({
            "event_id": format!("event-{idx}"),
            "device_id": format!("device-{}", idx % 1_000),
            "metric": "hrv",
            "value": (idx % 200) as f64 + 0.25,
        }),
    )
    .with_timestamp(1_710_000_000 + idx as i64)
}

fn metric_name(metric: VectorMetric) -> &'static str {
    match metric {
        VectorMetric::Cosine => "cosine",
        VectorMetric::Dot => "dot",
        VectorMetric::L2 => "l2",
    }
}

fn strategy_name(strategy: VectorProfileStrategy) -> &'static str {
    match strategy {
        VectorProfileStrategy::OptimizedStore => "optimized_store",
        VectorProfileStrategy::RecordScan => "record_scan",
    }
}

fn bench_memory_type(idx: usize) -> MemoryType {
    match idx % 9 {
        0 => MemoryType::Semantic,
        1 => MemoryType::Episodic,
        2 => MemoryType::Procedural,
        3 => MemoryType::Preference,
        4 => MemoryType::Fact,
        5 => MemoryType::Goal,
        6 => MemoryType::Task,
        7 => MemoryType::Conversation,
        _ => MemoryType::Observation,
    }
}

fn memory_samples(memories: usize) -> usize {
    if memories >= 100_000 {
        5
    } else {
        20
    }
}

#[derive(Clone, Debug)]
struct AnalyticsSamples {
    count: Vec<Duration>,
    avg: Vec<Duration>,
    min_max: Vec<Duration>,
    group_by_metric: Vec<Duration>,
    group_by_device: Vec<Duration>,
    timestamp_range: Vec<Duration>,
}

async fn sample_datafusion(
    ctx: &BicDataFusionContext<'_>,
    sql: &str,
    samples: usize,
) -> Result<Vec<Duration>> {
    let mut durations = Vec::with_capacity(samples.max(1));
    for _ in 0..samples.max(1) {
        let started = Instant::now();
        let result = ctx.sql(sql).await?;
        black_box(result.num_rows());
        durations.push(started.elapsed());
    }
    Ok(durations)
}

fn analytics_samples(records: usize) -> usize {
    if records >= 1_000_000 {
        3
    } else {
        5
    }
}

fn direct_avg(records: &[Record], metric: &str) -> Option<f64> {
    let mut count = 0_usize;
    let mut sum = 0.0;
    for record in records {
        if record
            .metadata
            .get("metric")
            .and_then(serde_json::Value::as_str)
            != Some(metric)
        {
            continue;
        }
        if let Some(value) = record
            .metadata
            .get("value")
            .and_then(serde_json::Value::as_f64)
        {
            count += 1;
            sum += value;
        }
    }
    (count > 0).then_some(sum / count as f64)
}

fn direct_min_max(records: &[Record], start_ts: i64) -> (Option<f64>, Option<f64>) {
    let mut min = None::<f64>;
    let mut max = None::<f64>;
    for record in records {
        if record.timestamp.unwrap_or_default() < start_ts {
            continue;
        }
        let Some(value) = record
            .metadata
            .get("value")
            .and_then(serde_json::Value::as_f64)
        else {
            continue;
        };
        min = Some(min.map_or(value, |current| current.min(value)));
        max = Some(max.map_or(value, |current| current.max(value)));
    }
    (min, max)
}

#[cfg(feature = "comparison-engines")]
fn record_payload(idx: usize) -> Result<Vec<u8>> {
    serde_json::to_vec(&json!({
        "id": format!("record-{idx}"),
        "idx": idx,
        "kind": "insert_bench",
    }))
    .map_err(Into::into)
}

fn collection_sizes(stats: &bicdb_core::DbStats) -> Vec<CollectionSizeReport> {
    stats
        .collections
        .iter()
        .map(|collection| CollectionSizeReport {
            name: collection.name.clone(),
            segment_bytes: collection.segment_bytes,
            logical_record_bytes: collection.logical_record_bytes,
            storage_overhead_bytes: collection.storage_overhead_bytes,
        })
        .collect()
}

fn count_value(result: &bicdb_sql::SqlResult) -> Option<usize> {
    match result.rows.first()?.first()? {
        SqlValue::Int(value) => Some(*value as usize),
        _ => None,
    }
}

fn avg_value(result: &bicdb_sql::SqlResult) -> Option<f64> {
    result.rows.first()?.first()?.as_f64()
}

#[cfg(feature = "comparison-engines")]
fn skipped(
    engine: BaselineEngine,
    records: usize,
    batch_size: usize,
    message: impl Into<String>,
) -> BaselineReport {
    BaselineReport {
        engine,
        status: BaselineStatus::Skipped,
        records,
        batch_size: batch_size.max(1),
        elapsed_ms: None,
        records_per_sec: None,
        database_size_bytes: None,
        message: message.into(),
        path: None,
    }
}

#[cfg(feature = "comparison-engines")]
fn failed(
    engine: BaselineEngine,
    records: usize,
    batch_size: usize,
    error: anyhow::Error,
) -> BaselineReport {
    BaselineReport {
        engine,
        status: BaselineStatus::Failed,
        records,
        batch_size: batch_size.max(1),
        elapsed_ms: None,
        records_per_sec: None,
        database_size_bytes: None,
        message: error.to_string(),
        path: None,
    }
}

fn path_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total += path_size(&entry.path())?;
    }
    Ok(total)
}

fn throughput(records: usize, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return records as f64;
    }
    records as f64 / elapsed.as_secs_f64()
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

/// Stable millisecond evidence at microsecond precision. Quantizing before JSON
/// hashing prevents an adjacent floating-point representation from changing
/// when a report is parsed and independently verified.
fn evidence_duration_ms(duration: Duration) -> f64 {
    (duration_ms(duration) * 1_000.0).round() / 1_000.0
}

fn nanos_to_ms(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000.0
}

fn nanos_avg_ms(total_nanos: u64, count: u64) -> f64 {
    total_nanos
        .checked_div(count)
        .map(nanos_to_ms)
        .unwrap_or(0.0)
}

fn current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kb = line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())?;
    Some(kb.saturating_mul(1024))
}

fn current_thread_count() -> Option<usize> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|line| line.starts_with("Threads:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<usize>().ok())
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_optional_usize(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn bench_csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn percentile(mut samples: Vec<Duration>, percentile: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort();
    let index = ((samples.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[index]
}

fn fmt_duration(duration: Duration) -> String {
    if duration.as_secs() > 0 {
        format!("{:.3}s", duration.as_secs_f64())
    } else if duration.as_millis() > 0 {
        format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3}us", duration.as_secs_f64() * 1_000_000.0)
    }
}

fn fmt_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{:.2} GiB", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{:.2} MiB", bytes_f / MIB)
    } else if bytes_f >= KIB {
        format!("{:.2} KiB", bytes_f / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn write_collection_sizes(
    formatter: &mut fmt::Formatter<'_>,
    collection_sizes: &[CollectionSizeReport],
) -> fmt::Result {
    if collection_sizes.is_empty() {
        return Ok(());
    }

    writeln!(formatter, "Collection sizes:")?;
    for collection in collection_sizes {
        writeln!(
            formatter,
            "  - {}: segment={}, logical={}, overhead={}",
            collection.name,
            fmt_bytes(collection.segment_bytes),
            fmt_bytes(collection.logical_record_bytes),
            fmt_bytes(collection.storage_overhead_bytes)
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paged_recovery_fixture_is_measured_and_verified_with_explicit_limits() {
        let path = default_bench_path("paged-recovery-report-test");
        let fixture = prepare_paged_recovery_fixture(
            &path,
            32 * 1024,
            128 * 1024,
            128,
            128 * 512,
            512,
            16,
            false,
        )
        .unwrap();
        assert!(fixture.actual_wal_bytes >= fixture.requested_wal_bytes);
        assert!(
            fixture.actual_checkpointed_data_bytes >= fixture.requested_checkpointed_data_bytes
        );
        assert!(fixture.checkpointed_records > 0);
        assert!(fixture.suffix_records > 0);

        let probe = run_paged_recovery_probe(
            &path,
            fixture.checkpointed_records,
            fixture.suffix_records,
            fixture.page_size,
            fixture.buffer_pool_bytes,
            false,
            1,
        )
        .unwrap();
        assert_eq!(probe.recovery.scan_passes, 2);
        assert_eq!(probe.recovery.wal_bytes_scanned, fixture.actual_wal_bytes);
        assert_eq!(
            probe.verified_checkpointed_records,
            fixture.checkpointed_records.min(3)
        );
        assert_eq!(probe.verified_suffix_records, fixture.suffix_records.min(3));
        let exact_record_limit = (bicdb_page::MAX_WAL_RECORD_BYTES as u64)
            .saturating_sub(u64::from(bicdb_page::MAX_PAGE_SIZE))
            .saturating_add(u64::from(fixture.page_size));
        assert!(probe.recovery.peak_record_bytes <= exact_record_limit);

        let environment = collect_paged_recovery_environment(
            std::env::current_exe().unwrap(),
            &path,
            None,
            PagedRecoveryCacheState::Uncontrolled,
            None,
        )
        .unwrap();
        let report = finish_paged_recovery_bench(
            environment.clone(),
            fixture.clone(),
            probe.clone(),
            PagedRecoveryBenchLimits::default(),
        )
        .unwrap();
        assert!(report.passed, "{:?}", report.failures);
        assert!(report.to_json().unwrap().contains("\"scan_passes\": 2"));
        assert!(report
            .to_csv()
            .starts_with("mode,bicdb_version,source_revision"));
        report.verify_integrity().unwrap();
        assert_eq!(report.checksum_sha256.len(), 64);
        let decoded: PagedRecoveryBenchReport =
            serde_json::from_str(&report.to_json().unwrap()).unwrap();
        assert_eq!(report, decoded);
        assert_eq!(
            report.calculate_checksum().unwrap(),
            decoded.calculate_checksum().unwrap()
        );
        decoded.verify_integrity().unwrap();
        let mut tampered = decoded;
        tampered.probe.recovery.scan_passes = 1;
        assert!(tampered.verify_integrity().is_err());

        let mut release_environment = environment.clone();
        release_environment.source_revision = Some("a".repeat(40));
        release_environment.cache_state = PagedRecoveryCacheState::Cold;
        release_environment.cache_preparation = Some("test cache preparation".to_string());
        release_environment
            .kernel_release
            .get_or_insert("test".to_string());
        release_environment
            .cpu_model
            .get_or_insert("test".to_string());
        release_environment.total_memory_bytes.get_or_insert(1);
        release_environment
            .filesystem_type
            .get_or_insert("test".to_string());
        release_environment
            .filesystem_source
            .get_or_insert("test".to_string());
        release_environment
            .filesystem_mount_point
            .get_or_insert(PathBuf::from("/"));
        release_environment
            .filesystem_mount_options
            .get_or_insert("rw".to_string());
        release_environment
            .filesystem_device
            .get_or_insert("0:0".to_string());
        validate_paged_recovery_release_environment(&release_environment, true).unwrap();
        let release_failed = finish_paged_recovery_bench(
            release_environment,
            fixture.clone(),
            probe.clone(),
            PagedRecoveryBenchLimits {
                require_release_evidence: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!release_failed.passed);
        assert!(release_failed
            .failures
            .iter()
            .any(|failure| failure.contains("fsync=true")));

        let mut mismatched_probe = probe.clone();
        mismatched_probe.fsync_enabled = !mismatched_probe.fsync_enabled;
        assert!(finish_paged_recovery_bench(
            environment.clone(),
            fixture.clone(),
            mismatched_probe,
            PagedRecoveryBenchLimits::default()
        )
        .unwrap_err()
        .to_string()
        .contains("do not describe the same run"));

        let failed = finish_paged_recovery_bench(
            environment,
            fixture,
            probe,
            PagedRecoveryBenchLimits {
                max_recovery_ms: None,
                max_peak_rss_bytes: Some(0),
                max_rss_growth_bytes: None,
                require_release_evidence: false,
            },
        )
        .unwrap();
        assert!(!failed.passed);
        assert!(!failed.failures.is_empty());
        let invalid_interval = run_paged_recovery_probe(&path, 0, 1, 512, 128 * 512, false, 0)
            .unwrap_err()
            .to_string();
        assert!(invalid_interval.contains("between 1 and 1000"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn linux_mount_fields_are_decoded_strictly() {
        assert_eq!(
            decode_mountinfo_field("/data\\040with\\011tab").unwrap(),
            "/data with\ttab"
        );
        assert!(decode_mountinfo_field("/bad\\09escape").is_err());
    }

    #[test]
    fn default_diff_fixtures_cover_required_cases() {
        let fixtures = default_postgres_diff_fixtures();

        assert!(fixtures.iter().any(|fixture| fixture.id == "select_1"));
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.id == "create_insert_select"));
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.id == "constraints_foreign_keys"));
        assert!(fixtures.iter().any(|fixture| fixture.id == "outer_joins"));
        assert!(fixtures.iter().any(|fixture| fixture.id == "views"));
        let cte = fixtures
            .iter()
            .find(|fixture| fixture.id == "cte_select")
            .expect("missing CTE parity fixture");
        assert_eq!(cte.expectation, PostgresDiffExpectation::Match);
        let expected = fixtures
            .iter()
            .find(|fixture| fixture.id == "expected_difference_recursive_cte")
            .expect("missing recursive CTE expected-difference fixture");
        assert_eq!(
            expected.expectation,
            PostgresDiffExpectation::ExpectedDifference
        );
        assert!(expected.expected_difference.is_some());
        let expected = fixtures
            .iter()
            .find(|fixture| fixture.id == "window_functions")
            .expect("missing window functions parity fixture");
        assert_eq!(expected.expectation, PostgresDiffExpectation::Match);
        assert!(expected.expected_difference.is_none());
        let arrays = fixtures
            .iter()
            .find(|fixture| fixture.id == "array_columns_supported_subset")
            .expect("missing supported array-column fixture");
        assert_eq!(arrays.expectation, PostgresDiffExpectation::Match);
        for id in ["enum_types", "domain_types"] {
            let fixture = fixtures
                .iter()
                .find(|fixture| fixture.id == id)
                .unwrap_or_else(|| panic!("missing {id} capability fixture"));
            assert_eq!(fixture.expectation, PostgresDiffExpectation::Match);
            assert!(fixture.expected_difference.is_none());
        }
    }

    #[test]
    fn generated_type_capability_fixtures_match_builtin_defaults() {
        let fixture_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/postgres-compat");
        let generated = load_postgres_diff_fixtures(Some(&fixture_dir)).unwrap();
        let defaults = default_postgres_diff_fixtures();
        for id in [
            "array_columns_supported_subset",
            "enum_types",
            "domain_types",
        ] {
            let generated = generated
                .iter()
                .find(|fixture| fixture.id == id)
                .unwrap_or_else(|| panic!("missing generated fixture {id}"));
            let builtin = defaults
                .iter()
                .find(|fixture| fixture.id == id)
                .unwrap_or_else(|| panic!("missing built-in fixture {id}"));
            assert_eq!(generated, builtin, "generated fixture {id} drifted");
        }
    }

    #[test]
    fn expected_difference_requires_observable_difference() {
        let fixture = PostgresDiffFixture {
            id: "unsupported".to_string(),
            description: "unsupported".to_string(),
            cleanup_sql: Vec::new(),
            sql: vec!["WITH one AS (SELECT 1) SELECT 1;".to_string()],
            expectation: PostgresDiffExpectation::ExpectedDifference,
            expected_difference: Some("CTEs unsupported".to_string()),
        };
        let postgres = vec![observed_rows("WITH one AS (SELECT 1) SELECT 1;", &[&["1"]])];
        let bicdb = vec![PostgresObservedStep {
            sql: "WITH one AS (SELECT 1) SELECT 1;".to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: None,
            command_rows: None,
            error: Some("unsupported CTE".to_string()),
        }];

        let report = compare_postgres_diff_fixture(fixture, postgres, bicdb, 1.0);

        assert_eq!(report.status, PostgresDiffStatus::Passed);
        assert!(report.detail.contains("expected difference"));
    }

    #[test]
    fn diff_report_exports_json_and_markdown() {
        let report = PostgresDiffReport {
            mode: "postgres_diff",
            target_version: "18.4".to_string(),
            postgres_host: "127.0.0.1".to_string(),
            postgres_port: 55432,
            total_cases: 1,
            passed_cases: 1,
            failed_cases: 0,
            expected_difference_cases: 0,
            elapsed_ms: 1.0,
            path: PathBuf::from("target/test-diff"),
            fixtures_dir: Some(PathBuf::from("fixtures/postgres-compat")),
            cases: vec![PostgresDiffCaseReport {
                id: "select_1".to_string(),
                description: "SELECT 1 parity".to_string(),
                expectation: PostgresDiffExpectation::Match,
                status: PostgresDiffStatus::Passed,
                elapsed_ms: 1.0,
                detail: "matched PostgreSQL".to_string(),
                metadata_differences: Vec::new(),
                expected_difference: None,
                postgres: vec![observed_rows("SELECT 1;", &[&["1"]])],
                bicdb: vec![observed_rows("SELECT 1;", &[&["1"]])],
            }],
        };

        let json = report.to_json().unwrap();
        assert!(json.contains("\"mode\": \"postgres_diff\""));
        assert!(json.contains("\"select_1\""));

        let markdown = report.to_markdown();
        assert!(markdown.contains("# PostgreSQL Differential Compatibility Report"));
        assert!(markdown.contains("| select_1 |"));
    }

    fn observed_rows(sql: &str, rows: &[&[&str]]) -> PostgresObservedStep {
        PostgresObservedStep {
            sql: sql.to_string(),
            columns: vec![PostgresObservedColumn {
                name: "?column?".to_string(),
                type_oid: Some(23),
            }],
            rows: rows
                .iter()
                .map(|row| row.iter().map(|value| Some((*value).to_string())).collect())
                .collect(),
            command_tag: None,
            command_rows: Some(rows.len() as u64),
            error: None,
        }
    }
}
