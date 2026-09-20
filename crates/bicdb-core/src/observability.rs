use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::db::{
    BicDb, DbStats, HaStatus, DEFAULT_COMPACTION_CHECKPOINT, DEFAULT_HA_STATE,
    DEFAULT_INDEX_CATALOG, DEFAULT_INDEX_MAINTENANCE, DEFAULT_PLANNER_STATS,
    DEFAULT_TRANSACTION_LOG,
};
use crate::error::Result;
use crate::residency::{self, ResidencyReport};

pub const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 1_000;
const REDACTED: &str = "[REDACTED]";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StructuredLogEvent {
    pub timestamp: i64,
    pub event: String,
    pub severity: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,
}

impl StructuredLogEvent {
    pub fn new(event: impl Into<String>, severity: impl Into<String>) -> Self {
        Self {
            timestamp: unix_timestamp(),
            event: event.into(),
            severity: severity.into(),
            fields: BTreeMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        self.fields.insert(
            key.into(),
            serde_json::to_value(value).unwrap_or(Value::Null),
        );
        self
    }

    pub fn to_json_line(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

pub fn append_structured_log(path: impl AsRef<Path>, event: &StructuredLogEvent) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.as_ref())?;
    writeln!(file, "{}", event.to_json_line()?)?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RedactionConfig {
    pub redact_query_text: bool,
    pub redact_bind_parameters: bool,
    pub sensitive_fields: Vec<String>,
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self {
            redact_query_text: false,
            redact_bind_parameters: true,
            sensitive_fields: default_sensitive_fields(),
        }
    }
}

pub fn default_sensitive_fields() -> Vec<String> {
    [
        "password",
        "passphrase",
        "secret",
        "token",
        "key",
        "ssn",
        "social_security",
        "dob",
        "date_of_birth",
        "mrn",
        "patient",
        "diagnosis",
        "phi",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// Render SQL for a log, audit row or introspection view without its data.
///
/// `redact_query_text` replaces the statement wholesale. Otherwise every
/// LITERAL is removed while the statement's shape is kept, which is what
/// makes slow-query logs useful and safe at the same time.
///
/// The previous default redacted only values that followed a configured
/// field name and an `=`, which missed every other way a secret reaches a
/// statement: `CREATE ROLE app PASSWORD 'hunter2'` (no `=` at all),
/// `SELECT hmac(x, 'KEY', ...) WHERE id = 5` (redacted the 5, logged the
/// key), and `INSERT INTO patients VALUES ('alice', '123-45-6789')`
/// (redacted the first literal only). Redacting by SYNTAX rather than by
/// field name removes the whole class instead of the listed cases.
pub fn redact_query_text(sql: &str, config: &RedactionConfig) -> String {
    if config.redact_query_text {
        return REDACTED.to_string();
    }
    redact_sql_literals(sql)
}

/// Replace every literal in `sql` with the redaction marker.
///
/// Handles single-quoted strings (including `''` escapes), `E'...'` escape
/// strings with backslash escapes, dollar-quoted bodies, numeric literals,
/// and comment bodies. Identifiers, keywords and punctuation survive, so
/// the statement remains recognisable for diagnosis.
pub fn redact_sql_literals(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        // Line comment: the body may quote anything at all.
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            out.push_str("--");
            out.push_str(REDACTED);
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        // Block comment.
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            out.push_str("/*");
            out.push_str(REDACTED);
            index += 2;
            while index < bytes.len()
                && !(bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/'))
            {
                index += 1;
            }
            out.push_str("*/");
            index = (index + 2).min(bytes.len());
            continue;
        }
        // Dollar-quoted string: $tag$ ... $tag$.
        if byte == b'$' {
            if let Some(tag_end) = sql[index + 1..]
                .find('$')
                .map(|offset| index + 1 + offset)
                .filter(|end| {
                    sql[index + 1..*end]
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
            {
                let tag = &sql[index..=tag_end];
                if let Some(close) = sql[tag_end + 1..].find(tag) {
                    out.push_str(tag);
                    out.push_str(REDACTED);
                    out.push_str(tag);
                    index = tag_end + 1 + close + tag.len();
                    continue;
                }
            }
        }
        // Escape string: E'...' with backslash escapes.
        if (byte == b'e' || byte == b'E') && bytes.get(index + 1) == Some(&b'\'') {
            out.push(char::from(byte));
            out.push('\'');
            out.push_str(REDACTED);
            out.push('\'');
            index += 2;
            let mut escaped = false;
            while index < bytes.len() {
                if escaped {
                    escaped = false;
                } else if bytes[index] == b'\\' {
                    escaped = true;
                } else if bytes[index] == b'\'' {
                    index += 1;
                    break;
                }
                index += 1;
            }
            continue;
        }
        // Ordinary string literal, '' being an embedded quote.
        if byte == b'\'' {
            out.push('\'');
            out.push_str(REDACTED);
            out.push('\'');
            index += 1;
            while index < bytes.len() {
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                        continue;
                    }
                    index += 1;
                    break;
                }
                index += 1;
            }
            continue;
        }
        // Quoted identifier: kept, it names schema rather than data.
        if byte == b'"' {
            out.push('"');
            index += 1;
            while index < bytes.len() {
                out.push(char::from(bytes[index]));
                if bytes[index] == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        out.push('"');
                        index += 2;
                        continue;
                    }
                    index += 1;
                    break;
                }
                index += 1;
            }
            continue;
        }
        // Numeric literal — an MRN, account number or date can be numeric.
        // Only when it starts a token, so `column1` keeps its digits.
        if byte.is_ascii_digit()
            && !bytes
                .get(index.wrapping_sub(1))
                .is_some_and(|previous| previous.is_ascii_alphanumeric() || *previous == b'_')
        {
            out.push_str(REDACTED);
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric()
                    || bytes[index] == b'.'
                    || ((bytes[index] == b'+' || bytes[index] == b'-')
                        && matches!(bytes.get(index - 1), Some(b'e') | Some(b'E'))))
            {
                index += 1;
            }
            continue;
        }
        let character = sql[index..].chars().next().expect("index is on a boundary");
        out.push(character);
        index += character.len_utf8();
    }
    out
}

pub fn redact_bind_parameters(values: &[String], config: &RedactionConfig) -> Vec<String> {
    if config.redact_bind_parameters {
        values.iter().map(|_| REDACTED.to_string()).collect()
    } else {
        values.to_vec()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlowQueryLogEntry {
    pub timestamp: i64,
    pub elapsed_ms: u64,
    pub rows: usize,
    pub query: String,
    pub bind_parameters: Vec<String>,
}

impl SlowQueryLogEntry {
    pub fn new(
        elapsed_ms: u64,
        rows: usize,
        query: &str,
        bind_parameters: &[String],
        redaction: &RedactionConfig,
    ) -> Self {
        Self {
            timestamp: unix_timestamp(),
            elapsed_ms,
            rows,
            query: redact_query_text(query, redaction),
            bind_parameters: redact_bind_parameters(bind_parameters, redaction),
        }
    }
}

pub fn append_slow_query_log(path: impl AsRef<Path>, entry: &SlowQueryLogEntry) -> Result<()> {
    append_structured_log(
        path,
        &StructuredLogEvent::new("query.slow", "warn")
            .with_field("elapsed_ms", entry.elapsed_ms)
            .with_field("rows", entry.rows)
            .with_field("query", entry.query.clone())
            .with_field("bind_parameters", entry.bind_parameters.clone()),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationalMetrics {
    pub generated_at: i64,
    pub server: BTreeMap<String, i64>,
    pub storage: BTreeMap<String, i64>,
    pub planner: BTreeMap<String, i64>,
    pub transaction: BTreeMap<String, i64>,
    pub backup: BTreeMap<String, i64>,
    pub compaction: BTreeMap<String, i64>,
    pub security: BTreeMap<String, i64>,
    pub replication: BTreeMap<String, i64>,
    /// Resident-memory accounting. Populated cheaply by [`Self::from_db`]
    /// (process RSS only) and fully by [`Self::from_db_with_residency`] — see
    /// those docs for why the full walk is not on the scrape path.
    #[serde(default)]
    pub memory: BTreeMap<String, i64>,
}

impl OperationalMetrics {
    /// Scrape-safe metrics. Memory accounting is limited to process RSS, which
    /// is a single short `/proc` read.
    ///
    /// The full residency walk visits every resident record and is therefore
    /// O(rows) — acceptable for an on-demand diagnostic, not for something a
    /// monitoring system polls every few seconds. Use
    /// [`Self::from_db_with_residency`] when that cost is intended.
    pub fn from_db(db: &BicDb) -> Result<Self> {
        let stats = db.stats()?;
        let ha = db.ha_status()?;
        let mut metrics = Self::from_parts(&stats, &ha);
        if let Some(snapshot) = db.paged_storage_snapshot()? {
            metrics.apply_paged_storage(&snapshot);
            if let Some(schedule) = db.paged_checkpoint_maintenance_status()? {
                metrics.apply_paged_checkpoint_schedule(&schedule);
            }
        }
        if let Some(rss) = residency::process_resident_bytes() {
            metrics
                .memory
                .insert("process_resident_bytes".to_string(), rss as i64);
        }
        Ok(metrics)
    }

    /// Metrics including the full resident-memory breakdown. Walks every
    /// resident record; call deliberately (diagnostics, benchmark reports), not
    /// on a scrape interval.
    pub fn from_db_with_residency(db: &BicDb) -> Result<Self> {
        let mut metrics = Self::from_db(db)?;
        metrics.apply_residency(&db.residency_report()?);
        Ok(metrics)
    }

    /// Fold a residency report into the `memory` section. Only aggregate
    /// category totals are emitted — collection and index names are
    /// user-controlled and must not become metric labels.
    pub fn apply_residency(&mut self, report: &ResidencyReport) {
        for (name, value) in report.category_totals() {
            self.memory.insert(name.to_string(), value as i64);
        }
        if let Some(rss) = report.process_resident_bytes {
            self.memory
                .insert("process_resident_bytes".to_string(), rss as i64);
        }
        if let Some(gap) = report.unaccounted_bytes {
            self.memory.insert("unaccounted_bytes".to_string(), gap);
        }
    }

    /// Fold the fixed-cardinality page/cache/WAL snapshot into scrape metrics.
    /// Field names are static: no database path, collection, page, tenant, or
    /// query identity can become a metric name or label.
    pub fn apply_paged_storage(&mut self, snapshot: &crate::PagedStoreSnapshot) {
        insert_u64(
            &mut self.storage,
            "paged_snapshot_format_version",
            u64::from(snapshot.format_version),
        );
        insert_u64(
            &mut self.storage,
            "paged_page_size_bytes",
            u64::from(snapshot.page_size),
        );
        insert_u64(&mut self.storage, "paged_page_count", snapshot.page_count);
        insert_u64(
            &mut self.storage,
            "paged_logical_page_bytes",
            snapshot.logical_page_bytes,
        );
        insert_u64(
            &mut self.storage,
            "paged_page_file_bytes",
            snapshot.page_file_bytes,
        );
        insert_u64(
            &mut self.storage,
            "paged_used_data_pages",
            snapshot.used_data_pages,
        );
        insert_u64(&mut self.storage, "paged_free_pages", snapshot.free_pages);
        insert_u64(&mut self.storage, "paged_free_bytes", snapshot.free_bytes);
        self.storage.insert(
            "paged_fsync_enabled".to_string(),
            i64::from(snapshot.fsync_enabled),
        );

        for (name, value) in [
            ("paged_page_reads_total", snapshot.page_io.reads),
            ("paged_page_writes_total", snapshot.page_io.writes),
            ("paged_page_bytes_read_total", snapshot.page_io.bytes_read),
            (
                "paged_page_bytes_written_total",
                snapshot.page_io.bytes_written,
            ),
            ("paged_page_allocations_total", snapshot.page_io.allocations),
            ("paged_page_frees_total", snapshot.page_io.frees),
            ("paged_page_syncs_total", snapshot.page_io.syncs),
            (
                "paged_page_segments_synced_total",
                snapshot.page_io.segments_synced,
            ),
            (
                "paged_page_checksum_failures_total",
                snapshot.page_io.checksum_failures,
            ),
            ("paged_page_torn_total", snapshot.page_io.torn_pages),
            ("paged_page_short_reads_total", snapshot.page_io.short_reads),
            (
                "paged_tail_reclaim_attempts_total",
                snapshot.page_io.tail_reclaim_attempts,
            ),
            (
                "paged_tail_reclaim_deferrals_total",
                snapshot.page_io.tail_reclaim_deferrals,
            ),
            (
                "paged_tail_reclaim_pages_truncated_total",
                snapshot.page_io.tail_reclaim_pages_truncated,
            ),
        ] {
            insert_u64(&mut self.storage, name, value);
        }

        let page_types = snapshot.page_io.page_types;
        for (name, class) in [
            ("superblock", page_types.superblock),
            ("heap", page_types.heap),
            ("overflow", page_types.overflow),
            ("btree_interior", page_types.btree_interior),
            ("btree_leaf", page_types.btree_leaf),
            ("free", page_types.free),
            ("unclassified", page_types.unclassified),
        ] {
            insert_page_class_metrics(&mut self.storage, name, class);
        }
        insert_fixed_latency_metrics(
            &mut self.storage,
            "paged_page_read_latency",
            snapshot.page_io.read_latency,
        );
        insert_fixed_latency_metrics(
            &mut self.storage,
            "paged_page_write_latency",
            snapshot.page_io.write_latency,
        );
        insert_fixed_latency_metrics(
            &mut self.storage,
            "paged_page_sync_latency",
            snapshot.page_io.sync_latency,
        );

        for (name, value) in [
            ("paged_wal_bytes", snapshot.wal_bytes),
            ("paged_wal_max_bytes", snapshot.wal_max_bytes),
            (
                "paged_wal_records_appended_total",
                snapshot.wal.records_appended,
            ),
            (
                "paged_wal_bytes_appended_total",
                snapshot.wal.bytes_appended,
            ),
            ("paged_wal_syncs_total", snapshot.wal.syncs),
            ("paged_wal_sync_failures_total", snapshot.wal.sync_failures),
            (
                "paged_wal_sync_records_total",
                snapshot.wal.sync_records_total,
            ),
            (
                "paged_wal_last_sync_records",
                snapshot.wal.last_sync_records,
            ),
            ("paged_wal_max_sync_records", snapshot.wal.max_sync_records),
            (
                "paged_wal_group_commit_savings_total",
                snapshot.wal.group_commit_savings,
            ),
            ("paged_wal_checkpoints_total", snapshot.wal.checkpoints),
            (
                "paged_wal_last_checkpoint_completed_at_millis",
                snapshot.wal.last_checkpoint_completed_at_millis,
            ),
            (
                "paged_wal_checkpoint_age_millis",
                snapshot.wal.checkpoint_age_millis,
            ),
            ("paged_wal_durable_lsn", snapshot.wal.durable_lsn),
            ("paged_wal_next_lsn", snapshot.wal.next_lsn),
            ("paged_wal_redo_lsn", snapshot.wal.redo_lsn),
            (
                "paged_recovery_wal_bytes_scanned",
                snapshot.recovery.wal_bytes_scanned,
            ),
            (
                "paged_recovery_duration_nanos",
                snapshot.recovery.duration_nanos,
            ),
            ("paged_recovery_scan_passes", snapshot.recovery.scan_passes),
            (
                "paged_recovery_peak_record_bytes",
                snapshot.recovery.peak_record_bytes,
            ),
            (
                "paged_recovery_transaction_outcomes",
                snapshot.recovery.transaction_outcomes,
            ),
            (
                "paged_recovery_terminal_outcome_records",
                snapshot.recovery.terminal_outcome_records,
            ),
            (
                "paged_recovery_peak_transaction_outcome_bytes",
                snapshot.recovery.peak_transaction_outcome_bytes,
            ),
            (
                "paged_recovery_records_scanned",
                snapshot.recovery.records_scanned,
            ),
            (
                "paged_recovery_pages_replayed",
                snapshot.recovery.pages_replayed,
            ),
            (
                "paged_recovery_uncommitted_skipped",
                snapshot.recovery.uncommitted_skipped,
            ),
            (
                "paged_recovery_truncated_bytes",
                snapshot.recovery.truncated_bytes,
            ),
            ("paged_recovery_redo_lsn", snapshot.recovery.redo_lsn),
            ("paged_recovery_end_lsn", snapshot.recovery.end_lsn),
            (
                "paged_recovery_frozen_outcomes_at_open",
                snapshot.recovery.frozen_outcomes_at_open,
            ),
            (
                "paged_recovery_abort_exceptions_at_open",
                snapshot.recovery.abort_exceptions_at_open,
            ),
            (
                "paged_transaction_frozen_xid",
                snapshot.transaction_frozen_xid,
            ),
            ("paged_transaction_next_xid", snapshot.transaction_next_xid),
            (
                "paged_transaction_resident_entries",
                snapshot.resident_transaction_entries,
            ),
            ("paged_abort_exceptions", snapshot.abort_exceptions),
            (
                "paged_abort_exception_capacity",
                snapshot.abort_exception_capacity,
            ),
            (
                "paged_recovery_status_spill_entries_at_open",
                snapshot.recovery.status_spill_entries_at_open,
            ),
            (
                "paged_recovery_status_spill_pages_at_open",
                snapshot.recovery.status_spill_pages_at_open,
            ),
            ("paged_status_spill_entries", snapshot.status_spill_entries),
            ("paged_status_spill_pages", snapshot.status_spill_pages),
            (
                "paged_status_spill_lookup_failures_total",
                snapshot.status_spill_lookup_failures,
            ),
        ] {
            insert_u64(&mut self.transaction, name, value);
        }
        insert_fixed_latency_metrics(
            &mut self.transaction,
            "paged_wal_sync_latency",
            snapshot.wal.sync_latency,
        );

        let pool = snapshot.buffer_pool;
        for (name, value) in [
            ("paged_buffer_pool_budget_bytes", pool.budget_bytes),
            ("paged_buffer_pool_total_frames", pool.total_frames),
            ("paged_buffer_pool_resident_pages", pool.resident_pages),
            ("paged_buffer_pool_resident_bytes", pool.resident_bytes),
            ("paged_buffer_pool_dirty_pages", pool.dirty_pages),
            (
                "paged_buffer_pool_dirty_bytes",
                pool.dirty_pages.saturating_mul(u64::from(pool.page_size)),
            ),
            ("paged_buffer_pool_pinned_pages", pool.pinned_pages),
            ("paged_buffer_pool_evictable_pages", pool.evictable_pages),
            ("paged_buffer_pool_free_frames", pool.free_frames),
            ("paged_buffer_pool_protected_pages", pool.protected_pages),
            (
                "paged_buffer_pool_probationary_pages",
                pool.probationary_pages,
            ),
            ("paged_buffer_pool_hits_total", pool.hits),
            ("paged_buffer_pool_misses_total", pool.misses),
            (
                "paged_buffer_pool_hit_ratio_basis_points",
                snapshot.buffer_pool_hit_ratio_basis_points(),
            ),
            ("paged_buffer_pool_evictions_total", pool.evictions),
            ("paged_buffer_pool_writebacks_total", pool.writebacks),
            (
                "paged_buffer_pool_writeback_steps_total",
                pool.writeback_steps,
            ),
            (
                "paged_buffer_pool_background_writebacks_total",
                pool.background_writebacks,
            ),
            (
                "paged_buffer_pool_writeback_candidate_limit_stops_total",
                pool.writeback_candidate_limit_stops,
            ),
            (
                "paged_buffer_pool_writeback_io_limit_stops_total",
                pool.writeback_io_limit_stops,
            ),
            (
                "paged_buffer_pool_writeback_duration_limit_stops_total",
                pool.writeback_duration_limit_stops,
            ),
            (
                "paged_buffer_pool_writeback_pinned_skips_total",
                pool.writeback_pinned_skips,
            ),
            (
                "paged_buffer_pool_read_ahead_queue_capacity",
                pool.read_ahead_queue_capacity,
            ),
            (
                "paged_buffer_pool_read_ahead_queue_depth",
                pool.read_ahead_queue_depth,
            ),
            (
                "paged_buffer_pool_read_ahead_requests_total",
                pool.read_ahead_requests,
            ),
            (
                "paged_buffer_pool_read_ahead_undriven_total",
                pool.read_ahead_undriven,
            ),
            (
                "paged_buffer_pool_read_ahead_enqueued_total",
                pool.read_ahead_enqueued,
            ),
            (
                "paged_buffer_pool_read_ahead_already_resident_total",
                pool.read_ahead_already_resident,
            ),
            (
                "paged_buffer_pool_read_ahead_already_queued_total",
                pool.read_ahead_already_queued,
            ),
            (
                "paged_buffer_pool_read_ahead_out_of_bounds_total",
                pool.read_ahead_out_of_bounds,
            ),
            (
                "paged_buffer_pool_read_ahead_queue_full_total",
                pool.read_ahead_queue_full,
            ),
            (
                "paged_buffer_pool_read_ahead_steps_total",
                pool.read_ahead_steps,
            ),
            (
                "paged_buffer_pool_read_ahead_pages_loaded_total",
                pool.read_ahead_pages_loaded,
            ),
            (
                "paged_buffer_pool_read_ahead_pages_used_total",
                pool.read_ahead_pages_used,
            ),
            (
                "paged_buffer_pool_read_ahead_pages_wasted_total",
                pool.read_ahead_pages_wasted,
            ),
            (
                "paged_buffer_pool_read_ahead_admission_declines_total",
                pool.read_ahead_admission_declines,
            ),
            (
                "paged_buffer_pool_read_ahead_read_errors_total",
                pool.read_ahead_read_errors,
            ),
            (
                "paged_buffer_pool_read_ahead_foreground_cancellations_total",
                pool.read_ahead_foreground_cancellations,
            ),
            (
                "paged_buffer_pool_read_ahead_candidate_limit_stops_total",
                pool.read_ahead_candidate_limit_stops,
            ),
            (
                "paged_buffer_pool_read_ahead_io_limit_stops_total",
                pool.read_ahead_io_limit_stops,
            ),
            (
                "paged_buffer_pool_read_ahead_duration_limit_stops_total",
                pool.read_ahead_duration_limit_stops,
            ),
            (
                "paged_buffer_pool_admission_failures_total",
                pool.admission_failures,
            ),
            (
                "paged_buffer_pool_shard_lock_waits_total",
                pool.shard_lock_waits,
            ),
            (
                "paged_buffer_pool_shard_lock_wait_nanos_total",
                pool.shard_lock_wait_nanos,
            ),
            (
                "paged_buffer_pool_shard_lock_max_wait_nanos",
                pool.shard_lock_max_wait_nanos,
            ),
            (
                "paged_buffer_pool_page_latch_waits_total",
                pool.page_latch_waits,
            ),
            (
                "paged_buffer_pool_page_latch_wait_nanos_total",
                pool.page_latch_wait_nanos,
            ),
            (
                "paged_buffer_pool_page_latch_max_wait_nanos",
                pool.page_latch_max_wait_nanos,
            ),
            (
                "paged_buffer_pool_oldest_dirty_page_age_millis",
                pool.oldest_dirty_page_age_millis,
            ),
            (
                "paged_buffer_pool_writeback_lag_pages",
                pool.writeback_lag_pages,
            ),
            (
                "paged_buffer_pool_writeback_lag_bytes",
                pool.writeback_lag_bytes,
            ),
            (
                "paged_buffer_pool_scan_demotions_total",
                pool.scan_demotions,
            ),
        ] {
            insert_u64(&mut self.memory, name, value);
        }
        self.memory.insert(
            "paged_buffer_pool_read_only".to_string(),
            i64::from(pool.read_only),
        );
    }

    /// Fold the durable checkpoint worker state into fixed-cardinality
    /// compaction metrics. UUIDs, errors, paths, and operator reasons are
    /// deliberately excluded.
    pub fn apply_paged_checkpoint_schedule(&mut self, schedule: &crate::PagedCheckpointSchedule) {
        let phase = match schedule.cursor.phase {
            crate::PagedCheckpointPhase::Drain => 0,
            crate::PagedCheckpointPhase::Freeze => 1,
            crate::PagedCheckpointPhase::Finalize => 2,
            crate::PagedCheckpointPhase::Complete => 3,
        };
        self.compaction
            .insert("paged_checkpoint_phase".to_string(), phase);
        self.compaction.insert(
            "paged_checkpoint_active".to_string(),
            i64::from(!schedule.completed && schedule.paused_reason.is_none()),
        );
        self.compaction.insert(
            "paged_checkpoint_paused".to_string(),
            i64::from(schedule.paused_reason.is_some()),
        );
        self.compaction.insert(
            "paged_checkpoint_completed".to_string(),
            i64::from(schedule.completed),
        );
        for (name, value) in [
            ("paged_checkpoint_state_sequence", schedule.state_sequence),
            (
                "paged_checkpoint_steps_total",
                schedule.totals.successful_steps,
            ),
            (
                "paged_checkpoint_pages_written_total",
                schedule.totals.pages_written,
            ),
            (
                "paged_checkpoint_logical_writeback_bytes_total",
                schedule.totals.logical_writeback_bytes,
            ),
            (
                "paged_checkpoint_active_duration_nanos_total",
                schedule.totals.active_duration_nanos,
            ),
            (
                "paged_checkpoint_wall_duration_millis",
                schedule
                    .updated_at_ms
                    .saturating_sub(schedule.started_at_ms),
            ),
            (
                "paged_checkpoint_transactions_frozen_total",
                schedule.totals.transactions_frozen,
            ),
            (
                "paged_checkpoint_abort_exceptions_recorded_total",
                schedule.totals.abort_exceptions_recorded,
            ),
            (
                "paged_checkpoint_wal_truncations_total",
                schedule.totals.wal_truncations,
            ),
            (
                "paged_checkpoint_wal_bytes_checkpointed_total",
                schedule.totals.wal_bytes_checkpointed,
            ),
            (
                "paged_checkpoint_tail_pages_truncated_total",
                schedule.totals.tail_pages_truncated,
            ),
            ("paged_checkpoint_failures_total", schedule.totals.failures),
            (
                "paged_checkpoint_resource_deferrals_total",
                schedule.totals.resource_deferrals,
            ),
        ] {
            insert_u64(&mut self.compaction, name, value);
        }
    }

    fn from_parts(stats: &DbStats, ha: &HaStatus) -> Self {
        let mut server = BTreeMap::new();
        server.insert("up".to_string(), 1);

        let mut storage = BTreeMap::new();
        storage.insert("database_size_bytes".to_string(), stats.size_bytes as i64);
        storage.insert("collections".to_string(), stats.collection_count as i64);
        storage.insert("records".to_string(), stats.record_count as i64);
        storage.insert(
            "logical_record_bytes".to_string(),
            stats
                .collections
                .iter()
                .map(|collection| collection.logical_record_bytes as i64)
                .sum(),
        );
        storage.insert(
            "storage_overhead_bytes".to_string(),
            stats
                .collections
                .iter()
                .map(|collection| collection.storage_overhead_bytes as i64)
                .sum(),
        );

        let mut planner = BTreeMap::new();
        planner.insert(
            "stats_tables".to_string(),
            file_nonempty(&stats.path.join(DEFAULT_PLANNER_STATS)) as i64,
        );

        let mut transaction = BTreeMap::new();
        transaction.insert(
            "pending_sync_ops".to_string(),
            stats.pending_sync_ops as i64,
        );
        transaction.insert(
            "transaction_log_bytes".to_string(),
            file_len(&stats.path.join(DEFAULT_TRANSACTION_LOG)) as i64,
        );

        let mut backup = BTreeMap::new();
        backup.insert(
            "artifacts_found".to_string(),
            backup_artifacts(&stats.path) as i64,
        );

        let mut compaction = BTreeMap::new();
        compaction.insert(
            "checkpoint_present".to_string(),
            stats.path.join(DEFAULT_COMPACTION_CHECKPOINT).exists() as i64,
        );

        let mut security = BTreeMap::new();
        security.insert(
            "ha_state_present".to_string(),
            stats.path.join(DEFAULT_HA_STATE).exists() as i64,
        );

        let mut replication = BTreeMap::new();
        replication.insert("lag_bytes".to_string(), ha.lag_bytes as i64);
        replication.insert("ready".to_string(), ha.ready as i64);

        Self {
            generated_at: unix_timestamp(),
            server,
            storage,
            planner,
            transaction,
            backup,
            compaction,
            security,
            replication,
            memory: BTreeMap::new(),
        }
    }

    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        for (section, metrics) in [
            ("server", &self.server),
            ("storage", &self.storage),
            ("planner", &self.planner),
            ("transaction", &self.transaction),
            ("backup", &self.backup),
            ("compaction", &self.compaction),
            ("security", &self.security),
            ("replication", &self.replication),
            ("memory", &self.memory),
        ] {
            for (name, value) in metrics {
                out.push_str(&format!("bicdb_{section}_{name} {value}\n"));
            }
        }
        out
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Live,
    Ready,
    Degraded,
    NotReady,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthReport {
    pub state: HealthState,
    pub checks: BTreeMap<String, bool>,
    pub recommendations: Vec<String>,
}

impl HealthReport {
    pub fn liveness(path: impl AsRef<Path>) -> Self {
        let mut checks = BTreeMap::new();
        checks.insert("path_exists".to_string(), path.as_ref().exists());
        let state = if checks.values().all(|passed| *passed) {
            HealthState::Live
        } else {
            HealthState::NotReady
        };
        Self {
            state,
            checks,
            recommendations: Vec::new(),
        }
    }

    pub fn readiness(db: &BicDb, max_size_bytes: Option<u64>) -> Result<Self> {
        let stats = db.stats()?;
        let integrity = db.verify_integrity()?;
        let ha = db.ha_status()?;
        let integrity_ok = integrity.large_value_checksum_failures.is_empty();
        let mut checks = BTreeMap::new();
        checks.insert("database_opens".to_string(), true);
        checks.insert("integrity_ok".to_string(), integrity_ok);
        checks.insert("replication_ready".to_string(), ha.ready);
        if let Some(max_size_bytes) = max_size_bytes {
            checks.insert(
                "storage_below_limit".to_string(),
                stats.size_bytes <= max_size_bytes,
            );
        }
        checks.insert(
            "compaction_not_in_progress".to_string(),
            !stats.path.join(DEFAULT_COMPACTION_CHECKPOINT).exists(),
        );
        let mut recommendations = Vec::new();
        if !integrity_ok {
            recommendations.push("run `bicdb integrity check` and restore from a verified backup if corruption is confirmed".to_string());
        }
        if !ha.ready {
            recommendations.push(
                "inspect `bicdb ha status --json` and resolve standby apply errors or lag"
                    .to_string(),
            );
        }
        if checks.get("storage_below_limit") == Some(&false) {
            recommendations
                .push("expand disk or run compaction after confirming backup health".to_string());
        }
        if checks.get("compaction_not_in_progress") == Some(&false) {
            recommendations
                .push("allow compaction to finish before removing the checkpoint file".to_string());
        }
        let state = if checks.values().all(|passed| *passed) {
            HealthState::Ready
        } else {
            HealthState::Degraded
        };
        Ok(Self {
            state,
            checks,
            recommendations,
        })
    }

    pub fn success(&self) -> bool {
        matches!(self.state, HealthState::Live | HealthState::Ready)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorReport {
    pub generated_at: i64,
    pub database_id: String,
    pub stats: DbStats,
    pub metrics: OperationalMetrics,
    pub health: HealthReport,
    pub files: BTreeMap<String, bool>,
    pub recommendations: Vec<String>,
    pub sanitized: bool,
    /// Durable storage engine for this database.
    #[serde(default)]
    pub storage_mode: String,
    /// Full resident-memory breakdown. `doctor` is an on-demand diagnostic, so
    /// unlike the scrape path it pays for the complete walk.
    #[serde(default)]
    pub residency: ResidencyReport,
    /// Scrape-safe page/cache/WAL state. Present only for `server_paged`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paged_storage: Option<crate::PagedStoreSnapshot>,
}

impl DoctorReport {
    pub fn collect(db: &BicDb) -> Result<Self> {
        let stats = db.stats()?;
        let paged_storage = db.paged_storage_snapshot()?;
        let residency = db.residency_report()?;
        let mut metrics = OperationalMetrics::from_db(db)?;
        metrics.apply_residency(&residency);
        let health = HealthReport::readiness(db, None)?;
        let mut files = BTreeMap::new();
        for name in [
            DEFAULT_TRANSACTION_LOG,
            DEFAULT_PLANNER_STATS,
            DEFAULT_INDEX_CATALOG,
            DEFAULT_INDEX_MAINTENANCE,
            DEFAULT_COMPACTION_CHECKPOINT,
            DEFAULT_HA_STATE,
        ] {
            files.insert(name.to_string(), stats.path.join(name).exists());
        }
        let mut recommendations = health.recommendations.clone();
        if metrics.backup.get("artifacts_found").copied().unwrap_or(0) == 0 {
            recommendations.push("no local .bicbackup artifacts found beside the database; verify external backup monitoring".to_string());
        }
        if metrics
            .storage
            .get("storage_overhead_bytes")
            .copied()
            .unwrap_or(0)
            > 0
        {
            recommendations.push(
                "track storage overhead and schedule compaction when reclaimed bytes justify IO"
                    .to_string(),
            );
        }
        if let Some(gap) = residency.unaccounted_bytes {
            // The share of RSS this accounting cannot explain is the figure that
            // decides whether the memory envelope in
            // `docs/server-paged-storage-todo.md` is trustworthy, so surface it
            // when it dominates rather than leaving it to be derived.
            //
            // Only above an absolute floor, though: every process pays a fixed
            // baseline (binary, runtime, allocator arenas) that swamps a small
            // database's accounted bytes no matter how good the accounting is.
            // Below that floor the ratio is arithmetic, not a finding.
            const MATERIAL_ACCOUNTED_BYTES: u64 = 64 * 1024 * 1024;
            if gap > 0
                && residency.accounted_bytes >= MATERIAL_ACCOUNTED_BYTES
                && gap as u64 > residency.accounted_bytes.saturating_mul(2)
            {
                recommendations.push(format!(
                    "resident memory accounting explains only {} of {} bytes RSS; \
                     the remainder is allocator overhead or uninstrumented structures",
                    residency.accounted_bytes,
                    residency.accounted_bytes as i64 + gap
                ));
            }
        }
        if let Some(snapshot) = paged_storage {
            if snapshot.page_io.tail_reclaim_deferrals > 0 {
                recommendations.push(format!(
                    "checkpoint tail reclamation deferred {} times at its bounded I/O envelope; schedule resumable file/extent rewrite maintenance",
                    snapshot.page_io.tail_reclaim_deferrals
                ));
            }
            if snapshot.buffer_pool.admission_failures > 0 {
                recommendations.push(format!(
                    "the paged buffer pool has refused {} admissions; raise its explicit budget or reduce concurrently pinned work",
                    snapshot.buffer_pool.admission_failures
                ));
            }
            if snapshot.buffer_pool.total_frames > 0
                && snapshot.buffer_pool.dirty_pages.saturating_mul(4)
                    >= snapshot.buffer_pool.total_frames.saturating_mul(3)
            {
                recommendations.push(
                    "at least 75% of paged buffer frames are dirty; inspect checkpoint and storage writeback pressure"
                        .to_string(),
                );
            }
            if snapshot.wal_max_bytes > 0
                && snapshot.wal_bytes.saturating_mul(5) >= snapshot.wal_max_bytes.saturating_mul(4)
            {
                recommendations.push(
                    "the paged WAL is at least 80% of its configured checkpoint trigger; inspect active transactions and checkpoint progress"
                        .to_string(),
                );
            }
            if snapshot.page_file_bytes > snapshot.logical_page_bytes {
                recommendations.push(
                    "the paged file retains an ignored physical tail after a safe reclamation crash window; schedule another checkpoint/reclamation pass"
                        .to_string(),
                );
            }
        }

        Ok(Self {
            generated_at: unix_timestamp(),
            database_id: path_hash(&stats.path),
            storage_mode: crate::format::storage_mode(&stats.path)
                .map(|mode| mode.to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            stats,
            metrics,
            health,
            files,
            recommendations,
            sanitized: true,
            residency,
            paged_storage,
        })
    }
}

fn scan_sql_value_end(input: &str, start: usize) -> usize {
    let bytes = input.as_bytes();
    if bytes.get(start) == Some(&b'\'') {
        let mut idx = start + 1;
        while idx < bytes.len() {
            if bytes[idx] == b'\'' {
                return idx + 1;
            }
            idx += 1;
        }
        return input.len();
    }
    let mut idx = start;
    while idx < input.len() {
        let ch = input[idx..].chars().next().unwrap();
        if matches!(ch, ',' | ')' | ';') || ch.is_ascii_whitespace() {
            break;
        }
        idx += ch.len_utf8();
    }
    idx
}

fn backup_artifacts(path: &Path) -> usize {
    let Some(parent) = path.parent() else {
        return 0;
    };
    fs::read_dir(parent)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("bicbackup")
        })
        .count()
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn file_nonempty(path: &Path) -> bool {
    file_len(path) > 0
}

fn path_hash(path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.to_string_lossy().as_bytes());
    hex::encode(&hasher.finalize()[..16])
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

pub fn operational_event_json(event: &str, severity: &str, fields: Value) -> Result<String> {
    let mut structured = StructuredLogEvent::new(event, severity);
    if let Value::Object(map) = fields {
        for (key, value) in map {
            structured.fields.insert(key, value);
        }
    } else {
        structured.fields.insert("value".to_string(), fields);
    }
    structured.to_json_line()
}

pub fn doctor_bundle_json(report: &DoctorReport) -> Result<Value> {
    Ok(json!({
        "generated_at": report.generated_at,
        "database_id": report.database_id,
        "sanitized": report.sanitized,
        "stats": report.stats,
        "metrics": report.metrics,
        "health": report.health,
        "files": report.files,
        "recommendations": report.recommendations,
        "storage_mode": report.storage_mode,
        // Safe to include in a sanitized bundle: the residency report carries
        // byte counts plus collection and index names, never record content.
        "residency": report.residency,
        "paged_storage": report.paged_storage,
    }))
}

fn insert_u64(metrics: &mut BTreeMap<String, i64>, name: &str, value: u64) {
    metrics.insert(name.to_string(), i64::try_from(value).unwrap_or(i64::MAX));
}

fn insert_page_class_metrics(
    metrics: &mut BTreeMap<String, i64>,
    class: &'static str,
    snapshot: crate::PageClassIoSnapshot,
) {
    for (suffix, value) in [
        ("reads_total", snapshot.reads),
        ("writes_total", snapshot.writes),
        ("bytes_read_total", snapshot.bytes_read),
        ("bytes_written_total", snapshot.bytes_written),
    ] {
        insert_u64(metrics, &format!("paged_page_{class}_{suffix}"), value);
    }
}

fn insert_fixed_latency_metrics(
    metrics: &mut BTreeMap<String, i64>,
    prefix: &'static str,
    snapshot: crate::PageIoLatencySnapshot,
) {
    for (suffix, value) in [
        ("observations_total", snapshot.count),
        ("nanos_total", snapshot.sum_nanos),
        ("max_nanos", snapshot.max_nanos),
        ("le_1us_total", snapshot.cumulative_buckets[0]),
        ("le_10us_total", snapshot.cumulative_buckets[1]),
        ("le_100us_total", snapshot.cumulative_buckets[2]),
        ("le_1ms_total", snapshot.cumulative_buckets[3]),
        ("le_10ms_total", snapshot.cumulative_buckets[4]),
        ("le_100ms_total", snapshot.cumulative_buckets[5]),
        ("le_1s_total", snapshot.cumulative_buckets[6]),
        ("le_10s_total", snapshot.cumulative_buckets[7]),
        ("inf_total", snapshot.cumulative_buckets[8]),
    ] {
        insert_u64(metrics, &format!("{prefix}_{suffix}"), value);
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    /// The three shapes the field-driven redactor missed, each of which put
    /// a live secret or protected data value into the slow-query log, stderr and the
    /// connections view.
    #[test]
    fn every_literal_is_redacted_whatever_syntax_carries_it() {
        let config = RedactionConfig::default();
        let redact = |sql: &str| redact_query_text(sql, &config);

        // 1. Role DDL: no `=` anywhere, so nothing was redacted at all.
        let rendered = redact("CREATE ROLE app LOGIN PASSWORD 'hunter2'");
        assert!(!rendered.contains("hunter2"), "password leaked: {rendered}");
        assert!(
            rendered.contains("CREATE ROLE app"),
            "shape lost: {rendered}"
        );

        // 2. A key argument followed by a later `=`: the key was logged and
        //    the harmless comparison value redacted instead.
        let rendered = redact("SELECT hmac(x, 'SECRETKEY', 'sha256') FROM t WHERE id = 5");
        assert!(
            !rendered.contains("SECRETKEY"),
            "hmac key leaked: {rendered}"
        );
        assert!(!rendered.contains(" 5"), "numeric value leaked: {rendered}");
        assert!(rendered.contains("hmac"), "shape lost: {rendered}");

        // 3. Multi-column INSERT: only the first literal was redacted.
        let rendered = redact("INSERT INTO patients VALUES ('alice', '123-45-6789', 42)");
        assert!(!rendered.contains("alice"), "name leaked: {rendered}");
        assert!(!rendered.contains("123-45-6789"), "SSN leaked: {rendered}");
        assert!(
            !rendered.contains("42"),
            "numeric protected data leaked: {rendered}"
        );
        assert!(
            rendered.contains("INSERT INTO patients"),
            "shape lost: {rendered}"
        );
    }

    #[test]
    fn exotic_literal_forms_are_redacted_too() {
        let config = RedactionConfig::default();
        let redact = |sql: &str| redact_query_text(sql, &config);

        // Escaped quotes inside a string must not end it early.
        let rendered = redact("SELECT * FROM t WHERE name = 'O''Brien-SECRET'");
        assert!(
            !rendered.contains("Brien"),
            "escaped-quote string leaked: {rendered}"
        );

        // E'' strings with backslash escapes.
        let rendered = redact(r"SELECT * FROM t WHERE a = E'se\'cret'");
        assert!(
            !rendered.contains("cret"),
            "escape string leaked: {rendered}"
        );

        // Dollar quoting.
        let rendered = redact("SELECT $tag$ssn 123-45-6789$tag$");
        assert!(
            !rendered.contains("123-45-6789"),
            "dollar-quoted leaked: {rendered}"
        );

        // Comments can carry anything.
        let rendered = redact("SELECT 1 -- patient bob has hiv\n");
        assert!(!rendered.contains("bob"), "comment leaked: {rendered}");
        let rendered = redact("SELECT /* mrn 55512345 */ 1");
        assert!(
            !rendered.contains("55512345"),
            "block comment leaked: {rendered}"
        );

        // Identifiers survive: they are schema, not data, and the log is
        // useless without them.
        let rendered = redact("SELECT \"patient_id\" FROM \"records\" WHERE id = 7");
        assert!(
            rendered.contains("patient_id"),
            "identifier lost: {rendered}"
        );
        assert!(rendered.contains("records"), "identifier lost: {rendered}");
        assert!(!rendered.contains('7'), "value kept: {rendered}");

        // Digits inside identifiers are part of the name, not a literal.
        let rendered = redact("SELECT col1, col2 FROM t1");
        assert!(
            rendered.contains("col1") && rendered.contains("t1"),
            "{rendered}"
        );
    }

    #[test]
    fn full_redaction_still_removes_everything() {
        let config = RedactionConfig {
            redact_query_text: true,
            ..RedactionConfig::default()
        };
        assert_eq!(redact_query_text("SELECT 'x' FROM t", &config), REDACTED);
    }
}
