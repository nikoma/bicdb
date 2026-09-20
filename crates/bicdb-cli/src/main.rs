// The default system allocator is a notable cost under bicdb's
// allocation-heavy OLTP/stored-procedure workloads. mimalloc is a faster
// drop-in global allocator for this access pattern.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod app_command;
mod sync_server;

use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "bench")]
use std::io::Read;
use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use bicdb_analytics::{
    rebuild_sidecar, verify_sidecar, AnalyticsQueryResult, BicDataFusionContext,
};
use bicdb_app_runtime::HttpHostPolicy;
#[cfg(feature = "bench")]
use bicdb_bench::{
    collect_paged_recovery_environment, default_bench_path, finish_paged_recovery_bench,
    prepare_paged_recovery_fixture, run_analytics_bench, run_ann_bench, run_event_bench,
    run_graph_bench, run_index_bench, run_insert_bench, run_memory_bench, run_paged_ingest,
    run_paged_recovery_probe, run_pg18_nightmare_suite, run_postgres_compat_suite,
    run_postgres_diff_suite, run_projection_bench, run_queue_bench, run_route_bench,
    run_server_bench, run_server_bench_with_config, run_server_certification, run_spatial_bench,
    run_spatial_nearest_bench, run_sql_bench, run_storage_baseline, run_sync_bench,
    run_transaction_bench, run_vector_bench, run_vector_profile_with_strategy,
    run_vector_search_hot, run_wearable_bench, validate_paged_recovery_release_environment,
    PagedRecoveryBenchLimits, PagedRecoveryBenchReport, PagedRecoveryCacheState,
    PagedRecoveryProbeReport, PostgresDiffConfig, ServerBenchScenario, VectorProfileStrategy,
    MAX_PAGED_RECOVERY_EVIDENCE_BYTES,
};
#[cfg(feature = "bench-comparison-engines")]
use bicdb_bench::{run_insert_baseline_suite, BaselineReport};
use bicdb_cell::{
    load_verified_manifest, open_inherited_key_lease_reader, parse_trusted_key_specs,
    rotate_cell_key, AttestedKeyLeaseCellKeyProvider, CellAdmissionRuntimeConfig,
    CellApplicationHostConfig, CellDeviceRuntimeConfig, CellGrantRuntimeConfig,
    CellHaRuntimeConfig, CellId, CellKeyProvider, CellKeyRotationConfig, CellReplicaRole,
    CellRuntime, CellRuntimeConfig, DevelopmentFileCellKeyProvider, Sha256Digest,
};
use bicdb_core::replication_transport;
use bicdb_core::{
    append_slow_query_log, capture_cluster_certification_artifact, create_backup,
    doctor_bundle_json, drill_backup_restore_with_limits, finalize_cluster_certification_bundle,
    initialize_cluster_certification_bundle, load_cluster_certification_plan,
    load_cluster_certification_publication_manifest, load_cluster_certification_state,
    load_distribution_config, plan_point_query, provision_cluster_member_directory,
    record_cluster_certification_observation, register_cluster_certification_artifact,
    restore_backup, restore_backup_to_point_with_limits, save_cluster_certification_plan,
    save_distribution_config, storage_mode, verify_backup, verify_backup_chain,
    verify_cluster_certification_bundle, BackupCreateOptions, BackupPitrReplayLimits,
    BackupPointInTimeRestoreOptions, BackupRestoreOptions, BicDb, BicDbError,
    ClusterCertificationArtifactKind, ClusterCertificationObservation, ClusterCertificationPlan,
    ClusterCertificationRawArtifactPayloadFormat, ClusterId, ClusterNetworkTransportConfig,
    ClusterNode, ClusterNodeId, ClusterOperationalMetrics, ClusterScaleProfile, ClusterTopology,
    CommitFrame, CompactionOptions, CompactionReport, ConsensusConfig, ConsensusPeer,
    ConsensusRole, DbConfig, DistributionConfig, DistributionStore, DoctorReport, EncryptionConfig,
    Geometry, GraphProjection, GraphProjectionData, GraphVerifyReport, HaStatus, HealthReport,
    HnswIndexConfig, HnswIndexVerifyReport, IndexMaintenanceAllReport, IndexMaintenanceReport,
    MetadataConsensusRole, MetadataConsensusStore, MetadataMemberRole, ModelRegistryEntry,
    OperationalMetrics, OsmImportBbox, OsmImportReport, PagedRecords, PagedRecordsOptions,
    PlacementPolicy, RebalanceOptions, Record, RedactionConfig, ReplicationConfig,
    ReplicationFrame, ReplicationMode, ReplicationTlsConfig, ResidencyReport, RoutePath,
    SlowQueryLogEntry, SpatialPackReport, SpatialPackStrategy, SpatialQueryResult,
    StandardFailureDomain, StorageMode, SyncCheckpoint, TcpClusterBootstrapClient,
    TcpClusterRelocationTransport, TupleLocator, VectorMetric,
    DEFAULT_CLUSTER_CERTIFICATION_MANIFEST, DEFAULT_CLUSTER_CERTIFICATION_PLAN,
    DEFAULT_CLUSTER_CERTIFICATION_REPORT, DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE,
    DEFAULT_PAGED_DIR, DEFAULT_SLOW_QUERY_THRESHOLD_MS,
};
use bicdb_page::{DirectoryLock, PageStore, PageStoreOptions, PagedPaths, TxStatus};
use bicdb_pgwire::{
    create_user_with_identity, serve_cluster_with_host_services, serve_with_host_services,
    set_user_security_identity, AuthMethod, ManualDistributionHostService, PgWireConfig,
    PgWireHostService, PgWireUserIdentity,
};
use bicdb_sql::{
    integrity_check, plan_migration_sql, split_migration_sql, MigrationStatementSafety, SqlResult,
    SqlSession, SqlValue,
};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

mod tui;

#[derive(Debug, Parser)]
#[command(name = "bicdb")]
#[command(version)]
#[command(about = "BicDB: Because the internet is optional.")]
#[cfg_attr(
    not(feature = "bench"),
    command(
        after_help = "Developer bench and compat commands require a CLI built with --features bench."
    )
)]
struct Cli {
    #[arg(long, value_name = "PATH", num_args = 0..=1, default_missing_value = ".")]
    tui: Option<PathBuf>,
    #[arg(long = "tui-key")]
    tui_key: Option<String>,
    #[arg(long = "tui-key-env")]
    tui_key_env: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliAuthMethod {
    Cleartext,
    #[value(name = "scram-sha-256")]
    ScramSha256,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliChannelBindingPolicy {
    Require,
    Prefer,
    Disable,
}

impl From<CliChannelBindingPolicy> for bicdb_pgwire::ChannelBindingPolicy {
    fn from(value: CliChannelBindingPolicy) -> Self {
        match value {
            CliChannelBindingPolicy::Require => Self::Require,
            CliChannelBindingPolicy::Prefer => Self::Prefer,
            CliChannelBindingPolicy::Disable => Self::Disable,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliStorageSync {
    Durable,
    Buffered,
}

impl std::fmt::Display for CliAuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cleartext => write!(f, "cleartext"),
            Self::ScramSha256 => write!(f, "scram-sha-256"),
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliEvictionPolicy {
    Noeviction,
    #[value(name = "allkeys-random")]
    AllkeysRandom,
    #[value(name = "volatile-ttl")]
    VolatileTtl,
}

impl std::fmt::Display for CliEvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noeviction => write!(f, "noeviction"),
            Self::AllkeysRandom => write!(f, "allkeys-random"),
            Self::VolatileTtl => write!(f, "volatile-ttl"),
        }
    }
}

impl From<CliEvictionPolicy> for bicdb_resp::EvictionPolicy {
    fn from(value: CliEvictionPolicy) -> Self {
        match value {
            CliEvictionPolicy::Noeviction => Self::NoEviction,
            CliEvictionPolicy::AllkeysRandom => Self::AllKeysRandom,
            CliEvictionPolicy::VolatileTtl => Self::VolatileTtl,
        }
    }
}

impl From<CliAuthMethod> for AuthMethod {
    fn from(value: CliAuthMethod) -> Self {
        match value {
            CliAuthMethod::Cleartext => Self::Cleartext,
            CliAuthMethod::ScramSha256 => Self::ScramSha256,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum HardeningProfile {
    LocalDev,
    #[value(alias = "clinic-lan")]
    SharedLan,
    ProductionServer,
    #[value(alias = "regulated-phi")]
    RegulatedData,
}

impl std::fmt::Display for HardeningProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalDev => write!(f, "local-dev"),
            Self::SharedLan => write!(f, "shared-lan"),
            Self::ProductionServer => write!(f, "production-server"),
            Self::RegulatedData => write!(f, "regulated-data"),
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Operate the capability-reduced, one-cell runtime construction path.
    Cell {
        #[command(subcommand)]
        command: CellCommand,
    },
    /// Install, inspect, and run signed ABI-v2 BicDB applications.
    App {
        /// BicDB database directory shared by the application host and pgwire.
        path: PathBuf,
        /// Durable signed-package catalog (defaults to <path>/applications).
        #[arg(long)]
        package_root: Option<PathBuf>,
        /// Trusted Ed25519 package key in the form KEY_ID=FILE.
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
        /// Host-only secret in the form NAME=VERSION=FILE.
        #[arg(long = "secret")]
        secrets: Vec<String>,
        /// Operator-owned local or S3-compatible blob provider configuration.
        #[arg(long)]
        blob_config: Option<PathBuf>,
        /// Operator-owned application HTTP and Redis provider bindings.
        #[arg(long)]
        integration_config: Option<PathBuf>,
        #[command(subcommand)]
        command: app_command::AppCommand,
    },
    Init {
        path: PathBuf,
        #[arg(long)]
        encrypted: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    /// Physical page-store layout operations.
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
    Inspect {
        path: PathBuf,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    /// Stream a collection as restartable JSON Lines in primary-key order.
    Export {
        path: PathBuf,
        #[arg(long)]
        collection: String,
        /// Resume strictly after this primary key.
        #[arg(long, conflicts_with = "after_id_hex")]
        after_id: Option<String>,
        /// Resume strictly after the UTF-8 primary key encoded as hexadecimal.
        /// Use this form for typed/internal keys that cannot be represented in
        /// an operating-system argument (for example, keys containing NUL).
        #[arg(long, conflicts_with = "after_id")]
        after_id_hex: Option<String>,
        #[arg(long, default_value_t = 1_000)]
        batch_rows: usize,
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        batch_bytes: usize,
        /// Resolve each bounded paged-store batch in physical heap-page order.
        /// Output and resume cursors remain in primary-key order.
        #[arg(long)]
        locality: bool,
    },
    Verify {
        path: PathBuf,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Check {
        path: PathBuf,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long = "backup")]
        backups: Vec<PathBuf>,
        #[arg(long = "backup-key")]
        backup_key: Option<String>,
        #[arg(long = "backup-key-env")]
        backup_key_env: Option<String>,
    },
    Integrity {
        #[command(subcommand)]
        command: IntegrityCommand,
    },
    Security {
        #[command(subcommand)]
        command: SecurityCommand,
    },
    Compact {
        path: PathBuf,
        #[arg(long)]
        collection: Option<String>,
        #[arg(long, default_value_t = 0)]
        reclaim_threshold_percent: u8,
        #[arg(long)]
        schedule: Option<String>,
        #[arg(long)]
        max_io_bytes_per_sec: Option<u64>,
        #[arg(long)]
        max_pause_ms: Option<u64>,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    #[cfg(feature = "bench")]
    Bench {
        #[command(subcommand)]
        command: BenchCommand,
    },
    #[cfg(feature = "bench")]
    Compat {
        #[command(subcommand)]
        command: CompatCommand,
    },
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    Migrate {
        #[command(subcommand)]
        command: MigrateCommand,
    },
    Analytics {
        target: String,
        query: Option<String>,
        #[arg(long)]
        collection: Option<String>,
        #[arg(long)]
        table: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    Sql {
        path: PathBuf,
        query: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
        #[arg(long)]
        slow_query_log: Option<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_SLOW_QUERY_THRESHOLD_MS)]
        slow_query_threshold_ms: u64,
        #[arg(long)]
        redact_query_text: bool,
        #[arg(long)]
        redact_bind_parameters: bool,
        #[arg(long = "redact-field")]
        redact_fields: Vec<String>,
    },
    Metrics {
        path: PathBuf,
        #[arg(long)]
        prometheus: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Health {
        #[command(subcommand)]
        command: HealthCommand,
    },
    Doctor {
        path: PathBuf,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    /// Initialize and operate an automatically sharded BicDB cluster.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    Serve {
        path: PathBuf,
        /// Treat path as a PostgreSQL-style cluster containing multiple databases.
        #[arg(long)]
        cluster: bool,
        /// Database selected when the client omits dbname in cluster mode.
        #[arg(long, default_value = "bicdb")]
        default_database: String,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 5433)]
        port: u16,
        /// PostgreSQL compatibility version advertised to clients; BicDB's own version is unchanged.
        #[arg(long, default_value = bicdb_sql::POSTGRES_COMPATIBILITY_VERSION)]
        postgres_server_version: String,
        /// Numeric companion to --postgres-server-version; derived automatically when omitted.
        #[arg(long)]
        postgres_server_version_num: Option<String>,
        /// Use a PostgreSQL-shaped version() banner while retaining bicdb_version() discovery.
        #[arg(long)]
        postgres_version_banner: bool,
        #[arg(long)]
        require_auth: bool,
        #[arg(long, value_enum, default_value_t = CliAuthMethod::Cleartext)]
        auth_method: CliAuthMethod,
        /// SCRAM channel binding. Default: require with TLS, disable without TLS.
        #[arg(long, value_enum)]
        channel_binding: Option<CliChannelBindingPolicy>,
        #[command(flatten)]
        operator: OperatorArgs,
        #[arg(long)]
        allow_remote_no_auth: bool,
        #[arg(long, default_value_t = 100)]
        max_connections: usize,
        /// Per-client-address connection cap (must be at least 1). Connection
        /// pools and load generators on one host exceed the default easily.
        #[arg(long, default_value_t = 20)]
        max_connections_per_ip: usize,
        #[arg(long, default_value_t = 100)]
        max_pending_accepts: usize,
        #[arg(long, default_value_t = default_max_active_queries())]
        max_active_queries: usize,
        #[arg(long, default_value_t = default_max_queued_queries())]
        max_queued_queries: usize,
        #[arg(long, default_value_t = default_max_active_queries())]
        max_active_reads: usize,
        #[arg(long, default_value_t = default_max_queued_queries())]
        max_queued_reads: usize,
        #[arg(long, default_value_t = default_max_active_queries())]
        max_active_writes: usize,
        #[arg(long, default_value_t = default_max_queued_writes())]
        max_queued_writes: usize,
        #[arg(long, default_value_t = 300)]
        idle_timeout_seconds: u64,
        #[arg(long, default_value_t = 10)]
        shutdown_grace_seconds: u64,
        /// Statement timeout in milliseconds; 0 matches PostgreSQL's disabled default.
        #[arg(long, default_value_t = 0)]
        query_timeout_ms: u64,
        #[arg(long, default_value_t = 30_000)]
        overload_timeout_ms: u64,
        #[arg(long, default_value_t = 30_000)]
        write_timeout_ms: u64,
        #[arg(long, default_value_t = 100_000)]
        max_result_rows: usize,
        #[arg(long, default_value_t = 10 * 1024 * 1024)]
        max_request_bytes: usize,
        #[arg(long, default_value_t = 16 * 1024 * 1024)]
        per_connection_memory_limit: usize,
        /// Build the compact row-id registry used by ordinary secondary-index hydration.
        /// Set false only for scan/direct-PK/generation-backed-FTS serving.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        paged_rowid_registry: bool,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        require_tls: bool,
        #[arg(long)]
        tls_client_ca: Option<PathBuf>,
        #[arg(long)]
        slow_query_log: Option<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_SLOW_QUERY_THRESHOLD_MS)]
        slow_query_threshold_ms: u64,
        #[arg(long)]
        redact_query_text: bool,
        #[arg(long = "redact-field")]
        redact_fields: Vec<String>,
        /// Storage sync mode. Buffered disables configured fsyncs and is not crash durable.
        #[arg(long, value_enum, default_value_t = CliStorageSync::Durable)]
        storage_sync: CliStorageSync,
    },
    /// Serve the HTTP sync API for browser working-set caches
    /// (one BicDB scope database per user under --root).
    SyncServe {
        /// Directory holding one `<scope>/db` BicDB per working set.
        root: PathBuf,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Port to bind; 0 picks a free port (printed on startup).
        #[arg(long, default_value_t = 8787)]
        port: u16,
        /// Require clients to send `Authorization: Bearer <token>` (falls
        /// back to the BICDB_SYNC_TOKEN environment variable).
        #[arg(long)]
        token: Option<String>,
        /// Enable the composition endpoint POST /v1/<scope>/sql, gated by
        /// this separate bearer token (falls back to BICDB_SYNC_ADMIN_TOKEN).
        #[arg(long)]
        admin_token: Option<String>,
        /// fsync scope databases on every commit.
        #[arg(long)]
        fsync: bool,
        /// Access-Control-Allow-Origin header value for browser clients.
        #[arg(long, default_value = "*")]
        cors_origin: String,
        /// Retention policy JSON file: {"defaults": [{"table", "column",
        /// "max_age_seconds"}], "scopes": {"<scope>": [...]}}. Deletes ride
        /// the sync stream to every device; the sweep compacts afterwards.
        #[arg(long)]
        retention: Option<PathBuf>,
        /// Seconds between retention sweeps.
        #[arg(long, default_value_t = 3600)]
        retention_interval_seconds: u64,
        /// Multi-tenant RLS composition config JSON: one master database
        /// with row-level-security policies; per-user scopes are composed
        /// from each user's RLS view and client writes are applied back
        /// under the user's session (see docs/browser-sync.md).
        #[arg(long)]
        rls_compose: Option<PathBuf>,
        /// Enable event-horizon trimming: bound each scope's record-audit
        /// history to what sync still needs (superseded events drop
        /// immediately, deletes once every live client pulled past them).
        #[arg(long)]
        event_horizon: bool,
        /// Client checkpoints idle longer than this don't hold the horizon
        /// back; those devices re-bootstrap on next sync (default 30 days).
        #[arg(long, default_value_t = 2_592_000)]
        horizon_max_checkpoint_age_seconds: u64,
        /// Comma-separated collections clients may NOT write: a push whose
        /// bundle touches one is rejected whole (server-authoritative
        /// synced content — control planes, catalogs, announcements).
        #[arg(long, value_delimiter = ',')]
        server_write_only: Vec<String>,
    },
    /// Serve the Redis-compatible (RESP) cache protocol.
    CacheServe {
        path: PathBuf,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 6379)]
        port: u16,
        /// Require clients to AUTH with this password (falls back to the
        /// BICDB_CACHE_PASSWORD environment variable).
        #[arg(long)]
        requirepass: Option<String>,
        /// fsync every write commit (durable across power loss, slower).
        #[arg(long)]
        fsync: bool,
        /// Total key budget across all databases; unbounded when omitted.
        #[arg(long)]
        max_keys: Option<usize>,
        #[arg(long, value_enum, default_value_t = CliEvictionPolicy::Noeviction)]
        eviction: CliEvictionPolicy,
        #[arg(long, default_value_t = 1000)]
        max_connections: usize,
        /// Enable HotView: the SQL command and HOTVIEW.* materialized cache
        /// entries. Off by default (exposes SQL execution on the cache port).
        #[arg(long)]
        hotview: bool,
        /// Memory-only cache entries: SET at memory speed, keys lost on
        /// restart. SQL tables and HotView definitions stay durable and
        /// materialized views recompute at startup.
        #[arg(long)]
        ephemeral: bool,
    },
    ServePg {
        path: PathBuf,
        /// Treat path as a PostgreSQL-style cluster containing multiple databases.
        #[arg(long)]
        cluster: bool,
        /// Database selected when the client omits dbname in cluster mode.
        #[arg(long, default_value = "bicdb")]
        default_database: String,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 5433)]
        port: u16,
        /// PostgreSQL compatibility version advertised to clients; BicDB's own version is unchanged.
        #[arg(long, default_value = bicdb_sql::POSTGRES_COMPATIBILITY_VERSION)]
        postgres_server_version: String,
        /// Numeric companion to --postgres-server-version; derived automatically when omitted.
        #[arg(long)]
        postgres_server_version_num: Option<String>,
        /// Use a PostgreSQL-shaped version() banner while retaining bicdb_version() discovery.
        #[arg(long)]
        postgres_version_banner: bool,
        #[arg(long)]
        require_auth: bool,
        #[arg(long, value_enum, default_value_t = CliAuthMethod::Cleartext)]
        auth_method: CliAuthMethod,
        /// SCRAM channel binding. Default: require with TLS, disable without TLS.
        #[arg(long, value_enum)]
        channel_binding: Option<CliChannelBindingPolicy>,
        #[command(flatten)]
        operator: OperatorArgs,
        #[arg(long)]
        allow_remote_no_auth: bool,
        #[arg(long, default_value_t = 100)]
        max_connections: usize,
        /// Per-client-address connection cap (must be at least 1). Connection
        /// pools and load generators on one host exceed the default easily.
        #[arg(long, default_value_t = 20)]
        max_connections_per_ip: usize,
        #[arg(long, default_value_t = 100)]
        max_pending_accepts: usize,
        #[arg(long, default_value_t = default_max_active_queries())]
        max_active_queries: usize,
        #[arg(long, default_value_t = default_max_queued_queries())]
        max_queued_queries: usize,
        #[arg(long, default_value_t = default_max_active_queries())]
        max_active_reads: usize,
        #[arg(long, default_value_t = default_max_queued_queries())]
        max_queued_reads: usize,
        #[arg(long, default_value_t = default_max_active_queries())]
        max_active_writes: usize,
        #[arg(long, default_value_t = default_max_queued_writes())]
        max_queued_writes: usize,
        #[arg(long, default_value_t = 300)]
        idle_timeout_seconds: u64,
        #[arg(long, default_value_t = 10)]
        shutdown_grace_seconds: u64,
        /// Statement timeout in milliseconds; 0 matches PostgreSQL's disabled default.
        #[arg(long, default_value_t = 0)]
        query_timeout_ms: u64,
        #[arg(long, default_value_t = 30_000)]
        overload_timeout_ms: u64,
        #[arg(long, default_value_t = 30_000)]
        write_timeout_ms: u64,
        #[arg(long, default_value_t = 100_000)]
        max_result_rows: usize,
        #[arg(long, default_value_t = 10 * 1024 * 1024)]
        max_request_bytes: usize,
        #[arg(long, default_value_t = 16 * 1024 * 1024)]
        per_connection_memory_limit: usize,
        /// Build the compact row-id registry used by ordinary secondary-index hydration.
        /// Set false only for scan/direct-PK/generation-backed-FTS serving.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        paged_rowid_registry: bool,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        require_tls: bool,
        #[arg(long)]
        tls_client_ca: Option<PathBuf>,
        #[arg(long)]
        slow_query_log: Option<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_SLOW_QUERY_THRESHOLD_MS)]
        slow_query_threshold_ms: u64,
        #[arg(long)]
        redact_query_text: bool,
        #[arg(long = "redact-field")]
        redact_fields: Vec<String>,
        /// Storage sync mode. Buffered disables configured fsyncs and is not crash durable.
        #[arg(long, value_enum, default_value_t = CliStorageSync::Durable)]
        storage_sync: CliStorageSync,
    },
    Vector {
        #[command(subcommand)]
        command: VectorCommand,
    },
    Model {
        #[command(subcommand)]
        command: ModelCommand,
    },
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
    Index {
        #[command(subcommand)]
        command: IndexCommand,
    },
    Spatial {
        #[command(subcommand)]
        command: SpatialCommand,
    },
    Graph {
        #[command(subcommand)]
        command: GraphCommand,
    },
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
    Ha {
        #[command(subcommand)]
        command: HaCommand,
    },
    Replication {
        #[command(subcommand)]
        command: ReplicationCommand,
    },
    Consensus {
        #[command(subcommand)]
        command: ConsensusCommand,
    },
}

#[derive(Debug, Subcommand)]
enum CellCommand {
    /// Verify and open exactly one signed, volume-bound cell.
    Serve {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        volume: PathBuf,
        #[arg(long)]
        expected_cell_id: String,
        #[arg(long)]
        expected_volume_id: String,
        #[arg(long)]
        guest_image_digest: String,
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        #[arg(long, conflicts_with = "key_file")]
        key_lease_fd: Option<i32>,
        #[arg(long = "kms-trusted-key")]
        kms_trusted_keys: Vec<String>,
        #[arg(long)]
        attestation_nonce: Option<String>,
        #[arg(long)]
        artifact_root: PathBuf,
        #[arg(long)]
        release_policy: Option<PathBuf>,
        #[arg(long)]
        identity_policy: Option<PathBuf>,
        #[arg(long)]
        egress_policy: Option<PathBuf>,
        #[arg(long)]
        authorization_policy: Option<PathBuf>,
        #[arg(long)]
        feature_certification: Option<PathBuf>,
        #[arg(long)]
        fleet_trust_policy: Option<PathBuf>,
        #[arg(long)]
        fleet_activation_bundle: Option<PathBuf>,
        #[arg(long)]
        previous_manifest: Option<PathBuf>,
        #[arg(long)]
        ha_trust_policy: Option<PathBuf>,
        #[arg(long)]
        ha_writer_epoch: Option<PathBuf>,
        #[arg(long)]
        ha_replica_lease: Option<PathBuf>,
        #[arg(long)]
        ha_previous_writer_epoch: Option<PathBuf>,
        #[arg(long)]
        ha_previous_primary_lease: Option<PathBuf>,
        #[arg(long)]
        ha_replica_signing_key: Option<PathBuf>,
        #[arg(long)]
        device_trust_policy: Option<PathBuf>,
        #[arg(long)]
        device_exporter_key_id: Option<String>,
        #[arg(long)]
        device_exporter_signing_key: Option<PathBuf>,
        #[arg(long)]
        grant_trust_policy: Option<PathBuf>,
        #[arg(long)]
        grant_exporter_key_id: Option<String>,
        #[arg(long)]
        grant_exporter_signing_key: Option<PathBuf>,
        #[arg(long)]
        grant_recipient_key_id: Option<String>,
        #[arg(long)]
        grant_recipient_private_key: Option<PathBuf>,
        #[arg(long)]
        admission_trust_policy: Option<PathBuf>,
        #[arg(long)]
        admission_evidence_bundle: Option<PathBuf>,
        #[arg(long)]
        deployment_isolation_tier: Option<String>,
        #[arg(long)]
        http_listen: Option<SocketAddr>,
        #[arg(long)]
        http_tls_cert: Option<PathBuf>,
        #[arg(long)]
        http_tls_key: Option<PathBuf>,
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
    /// Verify the signed manifest without opening or mutating cell storage.
    Verify {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Offline, crash-safe rotation through an exact signed Phase-2 manifest transition.
    RotateKey {
        #[arg(long)]
        current_manifest: PathBuf,
        #[arg(long)]
        next_manifest: PathBuf,
        #[arg(long)]
        volume: PathBuf,
        #[arg(long)]
        expected_cell_id: String,
        #[arg(long)]
        expected_volume_id: String,
        #[arg(long)]
        guest_image_digest: String,
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        current_key_lease_fd: i32,
        #[arg(long)]
        current_attestation_nonce: String,
        #[arg(long)]
        next_key_lease_fd: i32,
        #[arg(long)]
        next_attestation_nonce: String,
        #[arg(long = "kms-trusted-key")]
        kms_trusted_keys: Vec<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Rewrite the paged store between monolithic and extent-segmented
    /// layouts (offline: refuses to run while the database is open, and a
    /// crash mid-swap is rolled back on the next run). The WAL is untouched
    /// — pending records replay against the new layout on the next open.
    Segment {
        /// Database directory (the one containing `paged/`).
        path: PathBuf,
        /// Extent size in bytes; 0 restores the single-file layout.
        #[arg(long, default_value_t = 1024 * 1024 * 1024)]
        extent_bytes: u64,
        /// Page size the store was created with.
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
    },
    /// Transform a meta page written by an engine predating durable abort
    /// exceptions into the current layout. Without `--confirm` this only
    /// inspects and reports; with it, the store is opened with
    /// `accept_legacy_meta`, the WAL is replayed, and the meta page is
    /// rewritten and checkpointed in place. Offline: refuses to run while the
    /// database is open.
    MigrateMeta {
        /// Database directory (the one containing `paged/`).
        path: PathBuf,
        /// Page size the store was created with.
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
        /// Actually transform; the default is a read-only report.
        #[arg(long)]
        confirm: bool,
    },
    /// Copy a database into a NEW directory running a different storage
    /// engine. `embedded_memory` keeps every row and index resident, so a
    /// store whose working set outgrew RAM belongs in `server_paged` with its
    /// bounded buffer pool. ADR-004 forbids reinterpreting a directory in
    /// place, so this copies; the source is opened read-only and never
    /// modified. Offline on both sides.
    MigrateMode {
        /// Existing database directory to read.
        source: PathBuf,
        /// New directory to create and write.
        target: PathBuf,
        /// Target engine: `server_paged` or `embedded_memory`.
        #[arg(long, default_value = "server_paged")]
        mode: String,
        /// Records copied per batch.
        #[arg(long, default_value_t = bicdb_core::storage_mode_migration::DEFAULT_MIGRATION_BATCH)]
        batch: usize,
    },
    /// Rebuild a damaged transaction-status spill from an exact, independently
    /// reconstructed outcome file. Offline and dry-run by default. The input
    /// is tab-separated `xid<TAB>committed|aborted`, strictly ascending.
    RepairStatusSpill {
        /// Database directory (the one containing `paged/`).
        path: PathBuf,
        /// Exact descriptor values from the open failure or forensic report.
        #[arg(long)]
        expected_head: u64,
        #[arg(long)]
        expected_pages: u64,
        #[arg(long)]
        expected_entries: u64,
        #[arg(long)]
        outcomes: PathBuf,
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
        /// Actually publish the replacement. Without this flag no data changes.
        #[arg(long)]
        confirm: bool,
        #[arg(long)]
        json: bool,
    },
    /// Stream validated commit/abort decisions from an offline WAL into the
    /// tab-separated input accepted by `repair-status-spill`.
    ExtractWalOutcomes {
        /// Database directory (the one containing `paged/`).
        path: PathBuf,
        #[arg(long)]
        start_xid: u64,
        #[arg(long)]
        end_xid: u64,
        /// New output file; an existing path is never overwritten.
        #[arg(long)]
        output: PathBuf,
    },
    /// Validate and extract an operator-selected status-spill chain even when
    /// the database meta descriptor points at a different, damaged chain.
    ExtractStatusSpill {
        path: PathBuf,
        #[arg(long)]
        head: u64,
        #[arg(long)]
        pages: u64,
        #[arg(long)]
        entries: u64,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
    },
}

#[derive(Debug, Subcommand)]
enum VectorCommand {
    Index {
        #[command(subcommand)]
        command: VectorIndexCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ModelCommand {
    Enable {
        path: PathBuf,
        model: String,
        #[arg(long)]
        source: Option<PathBuf>,
        #[arg(long, default_value_t = 768)]
        dimension: usize,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        no_progress: bool,
    },
    Status {
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum MemoryCommand {
    Process {
        path: PathBuf,
        #[arg(long, default_value_t = usize::MAX)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    Search {
        path: PathBuf,
        table: String,
        field: String,
        query: String,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SecurityCommand {
    #[command(visible_alias = "protected_data-release-gate")]
    ProtectedDataReleaseGate {
        path: PathBuf,
        #[arg(long, default_value = ".")]
        source_root: PathBuf,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
    },
    ProductionGate {
        path: PathBuf,
        #[arg(long, value_enum)]
        profile: HardeningProfile,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long)]
        require_auth: bool,
        #[arg(long, value_enum, default_value_t = CliAuthMethod::Cleartext)]
        auth_method: CliAuthMethod,
        #[arg(long)]
        allow_remote_no_auth: bool,
        #[arg(long)]
        require_tls: bool,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        tls_client_ca: Option<PathBuf>,
        #[arg(long = "db-key-env")]
        db_key_env: Option<String>,
        #[arg(
            long = "protected-data-field-key-env",
            visible_alias = "protected_data-field-key-env"
        )]
        protected_data_field_key_env: Option<String>,
        #[arg(
            long = "protected-data-hmac-key-env",
            visible_alias = "protected_data-hmac-key-env"
        )]
        protected_data_hmac_key_env: Option<String>,
        #[arg(long = "backup-key-env")]
        backup_key_env: Option<String>,
        #[arg(long, default_value_t = 365)]
        audit_retention_days: u64,
        #[arg(long)]
        audit_tamper_evidence: bool,
        #[arg(long)]
        protected_data_evidence: Option<PathBuf>,
        #[arg(long)]
        dependency_evidence: Option<PathBuf>,
        #[arg(long, default_value_t = 30)]
        evidence_max_age_days: u64,
        #[arg(long)]
        json: bool,
    },
    SupplyChainAudit {
        #[arg(long, default_value = ".")]
        source_root: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum IntegrityCommand {
    Check {
        path: PathBuf,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Inspect one server-paged record's MVCC chain without changing it.
    ChainInspect {
        path: PathBuf,
        #[arg(long)]
        collection: String,
        #[arg(long = "record-id")]
        record_id: String,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Replace one confirmed cyclic/over-limit chain from an authoritative record.
    ChainRepair {
        path: PathBuf,
        #[arg(long)]
        collection: String,
        #[arg(long = "record-id")]
        record_id: String,
        /// Exact `page:slot:generation` token printed by chain-inspect.
        #[arg(long = "expected-head")]
        expected_head: String,
        /// JSON-encoded BicDB Record reconstructed from an independent ledger.
        #[arg(long = "record-json")]
        record_json: PathBuf,
        /// Actually publish the repair. Without this flag the command is a dry run.
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Verify the page B-tree and every reachable MVCC version chain.
    ChainVerify {
        path: PathBuf,
        #[arg(long, default_value_t = 64)]
        max_fault_samples: usize,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Truncate a REACHABLE free list at its first corrupt link, keeping the
    /// provably-valid prefix and abandoning (leaking) everything after it.
    /// This is the repair for "page N has type T, expected 6" allocation
    /// failures after a crash or kill mid-write; the detached-count case is
    /// `free-list-repair`.
    FreeListTruncate {
        path: PathBuf,
        /// Actually publish the repair. Without this flag the command is a dry run.
        #[arg(long)]
        apply: bool,
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
        #[arg(long)]
        json: bool,
    },
    /// Abandon an orphaned free-page count after an interrupted list detach.
    FreeListRepair {
        path: PathBuf,
        /// Exact detached count reported by the corruption diagnostic.
        #[arg(long)]
        expected_free_pages: u64,
        /// Actually publish the repair. Without this flag the command is a dry run.
        #[arg(long)]
        apply: bool,
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum VectorIndexCommand {
    Build {
        path: PathBuf,
        #[arg(long)]
        collection: String,
        #[arg(long, default_value_t = 16)]
        m: usize,
        #[arg(long, default_value_t = 100)]
        ef_construction: usize,
        #[arg(long, default_value_t = 50)]
        ef_search: usize,
        #[arg(long, default_value = "cosine")]
        metric: String,
    },
    Verify {
        path: PathBuf,
        #[arg(long)]
        collection: String,
    },
    Rebuild {
        path: PathBuf,
        #[arg(long)]
        collection: String,
    },
}

#[derive(Debug, Subcommand)]
enum IndexCommand {
    Verify {
        path: PathBuf,
        /// Verify one index by name (omit and pass --all for every index).
        name: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    Rebuild {
        path: PathBuf,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    /// Report authoritative full-text build state from BicDB checkpoints and
    /// published generations. Omit NAME and pass --all to list every build.
    FtsStatus {
        path: PathBuf,
        name: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// Idempotently resume one interrupted full-text build. Row-backed scans
    /// advance by the document budget; completed tokenization is finalized.
    FtsReconcile {
        path: PathBuf,
        name: String,
        #[arg(long, default_value_t = 1_000_000)]
        budget_documents: u64,
        #[arg(long)]
        json: bool,
    },
    /// Migrate a v2 paged B-tree index to v3 (intern-keyed) entries.
    /// Bounded self-checkpointing batches: safe to interrupt and re-run; the
    /// durable format record flips only when no v2 entry remains.
    Rekey {
        path: PathBuf,
        /// B-tree index name.
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Bulk-pack a spatial index into an immutable durable node tree
    /// (server_paged databases only). Hilbert packs run through the
    /// resumable bounded-memory pipeline: a crash-interrupted pack resumes
    /// from its checkpoint when re-run.
    Pack {
        path: PathBuf,
        /// Spatial index name (e.g. idx_places_geometry_spatial).
        name: String,
        /// Packing order: hilbert (default) or str.
        #[arg(long, default_value = "hilbert")]
        strategy: String,
        /// Create the index as PACKED if it does not exist yet, on this
        /// collection's intrinsic geometry field — skipping the resident
        /// tree build entirely (peak memory stays bounded regardless of
        /// corpus size).
        #[arg(long, value_name = "COLLECTION")]
        create: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SpatialCommand {
    ImportOsm {
        file: PathBuf,
        #[arg(long, allow_hyphen_values = true)]
        bbox: String,
        #[arg(long, default_value = ".")]
        path: PathBuf,
        #[arg(long, default_value = "roads")]
        graph: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    Route {
        path: PathBuf,
        #[arg(long, allow_hyphen_values = true)]
        from: String,
        #[arg(long, allow_hyphen_values = true)]
        to: String,
        #[arg(long, default_value = "roads")]
        graph: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    Nearest {
        path: PathBuf,
        collection: String,
        #[arg(long, allow_hyphen_values = true)]
        point: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long, default_value = "geometry")]
        field: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    WithinRadius {
        path: PathBuf,
        collection: String,
        #[arg(long, allow_hyphen_values = true)]
        point: String,
        #[arg(long)]
        meters: f64,
        #[arg(long, default_value = "geometry")]
        field: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
}

#[derive(Debug, Subcommand)]
enum GraphCommand {
    Build {
        path: PathBuf,
        #[arg(long)]
        projection: PathBuf,
    },
    Rebuild {
        path: PathBuf,
        #[arg(long)]
        projection: PathBuf,
    },
    Verify {
        path: PathBuf,
        #[arg(long)]
        projection: PathBuf,
    },
    Query {
        path: PathBuf,
        query: String,
        #[arg(long)]
        projection: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum UserCommand {
    /// Authorize this application login to delegate transaction identities.
    Delegate {
        username: String,
        #[arg(long, default_value = ".")]
        path: PathBuf,
        /// Read the HMAC key from this environment variable. Omit with --revoke.
        #[arg(long, required_unless_present = "revoke", conflicts_with = "revoke")]
        signing_key_env: Option<String>,
        /// Allowed tenant IDs, or an explicit '*' for all tenants.
        #[arg(long = "tenant", required_unless_present = "revoke")]
        tenants: Vec<String>,
        #[arg(long)]
        revoke: bool,
    },
    Create {
        username: String,
        #[arg(
            long,
            required_unless_present = "password_env",
            conflicts_with = "password_env"
        )]
        password: Option<String>,
        /// Read the password from this environment variable instead of process arguments.
        #[arg(long)]
        password_env: Option<String>,
        #[arg(long, default_value = ".")]
        path: PathBuf,
        /// Stable application user id (defaults to the login name).
        #[arg(long)]
        user_id: Option<String>,
        /// Trusted tenant assigned by the operator. Empty means no tenant authority.
        #[arg(long, default_value = "")]
        tenant: String,
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long = "role")]
        roles: Vec<String>,
        #[arg(long = "scope")]
        scopes: Vec<String>,
    },
    /// Bind or replace the trusted identity for an existing login.
    BindIdentity {
        username: String,
        #[arg(long, default_value = ".")]
        path: PathBuf,
        /// Stable application user id (defaults to the login name).
        #[arg(long)]
        user_id: Option<String>,
        /// Trusted tenant assigned by the operator. Empty means no tenant authority.
        #[arg(long, default_value = "")]
        tenant: String,
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long = "role")]
        roles: Vec<String>,
        #[arg(long = "scope")]
        scopes: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ServerCommand {
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliFailureDomain {
    Server,
    Rack,
    Zone,
    Region,
}

impl From<CliFailureDomain> for StandardFailureDomain {
    fn from(value: CliFailureDomain) -> Self {
        match value {
            CliFailureDomain::Server => Self::Server,
            CliFailureDomain::Rack => Self::Rack,
            CliFailureDomain::Zone => Self::Zone,
            CliFailureDomain::Region => Self::Region,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliClusterScaleProfile {
    #[value(name = "1tb")]
    OneTb,
    #[value(name = "5tb")]
    FiveTb,
    #[value(name = "20tb")]
    TwentyTb,
}

impl From<CliClusterScaleProfile> for ClusterScaleProfile {
    fn from(value: CliClusterScaleProfile) -> Self {
        match value {
            CliClusterScaleProfile::OneTb => Self::OneTb,
            CliClusterScaleProfile::FiveTb => Self::FiveTb,
            CliClusterScaleProfile::TwentyTb => Self::TwentyTb,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliClusterCertificationArtifactKind {
    Hardware,
    EffectiveConfiguration,
    TopologyBefore,
    TopologyAfter,
    Commands,
    ResourceSamples,
    WorkloadLatency,
    BackgroundSaturation,
    RebalanceTimeline,
    FailureTimeline,
    Restore,
    FullTextIndex,
    Checksums,
}

impl From<CliClusterCertificationArtifactKind> for ClusterCertificationArtifactKind {
    fn from(value: CliClusterCertificationArtifactKind) -> Self {
        match value {
            CliClusterCertificationArtifactKind::Hardware => Self::Hardware,
            CliClusterCertificationArtifactKind::EffectiveConfiguration => {
                Self::EffectiveConfiguration
            }
            CliClusterCertificationArtifactKind::TopologyBefore => Self::TopologyBefore,
            CliClusterCertificationArtifactKind::TopologyAfter => Self::TopologyAfter,
            CliClusterCertificationArtifactKind::Commands => Self::Commands,
            CliClusterCertificationArtifactKind::ResourceSamples => Self::ResourceSamples,
            CliClusterCertificationArtifactKind::WorkloadLatency => Self::WorkloadLatency,
            CliClusterCertificationArtifactKind::BackgroundSaturation => Self::BackgroundSaturation,
            CliClusterCertificationArtifactKind::RebalanceTimeline => Self::RebalanceTimeline,
            CliClusterCertificationArtifactKind::FailureTimeline => Self::FailureTimeline,
            CliClusterCertificationArtifactKind::Restore => Self::Restore,
            CliClusterCertificationArtifactKind::FullTextIndex => Self::FullTextIndex,
            CliClusterCertificationArtifactKind::Checksums => Self::Checksums,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliClusterCertificationPayloadFormat {
    Json,
    JsonLines,
    PrometheusText,
    Text,
    Binary,
}

impl From<CliClusterCertificationPayloadFormat> for ClusterCertificationRawArtifactPayloadFormat {
    fn from(value: CliClusterCertificationPayloadFormat) -> Self {
        match value {
            CliClusterCertificationPayloadFormat::Json => Self::Json,
            CliClusterCertificationPayloadFormat::JsonLines => Self::JsonLines,
            CliClusterCertificationPayloadFormat::PrometheusText => Self::PrometheusText,
            CliClusterCertificationPayloadFormat::Text => Self::Text,
            CliClusterCertificationPayloadFormat::Binary => Self::Binary,
        }
    }
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    Init {
        path: PathBuf,
        #[arg(long, default_value = "default")]
        cluster_id: String,
        #[arg(long, default_value = "n1")]
        node_id: String,
        #[arg(long, default_value = "127.0.0.1:9444")]
        address: String,
        #[arg(long, default_value_t = 1_099_511_627_776)]
        capacity_bytes: u64,
        #[arg(long, default_value_t = 3)]
        replication_factor: u8,
        #[arg(long, default_value_t = 256)]
        initial_ranges: u32,
        #[arg(long, value_enum, value_delimiter = ',')]
        failure_domains: Vec<CliFailureDomain>,
        #[arg(long = "label", value_name = "KEY=VALUE")]
        labels: Vec<String>,
        #[arg(long)]
        cluster_tls_cert: Option<PathBuf>,
        #[arg(long)]
        cluster_tls_key: Option<PathBuf>,
        #[arg(long)]
        cluster_tls_ca: Option<PathBuf>,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    Join {
        /// Existing member directory for admin-assisted join. Omit when using
        /// the remote seed options from the empty server.
        path: Option<PathBuf>,
        #[arg(long)]
        seed_node_id: Option<String>,
        #[arg(long)]
        seed_address: Option<String>,
        #[arg(long)]
        cluster_id: Option<String>,
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        address: String,
        #[arg(long, default_value_t = 1)]
        incarnation: u64,
        #[arg(long, default_value_t = 1_099_511_627_776)]
        capacity_bytes: u64,
        #[arg(long = "label", value_name = "KEY=VALUE")]
        labels: Vec<String>,
        /// Local data directory to provision from the quorum-committed
        /// bootstrap snapshot.
        #[arg(long)]
        node_root: PathBuf,
        #[arg(long)]
        cluster_tls_cert: Option<PathBuf>,
        #[arg(long)]
        cluster_tls_key: Option<PathBuf>,
        #[arg(long)]
        cluster_tls_ca: Option<PathBuf>,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Quorum-stage new local mTLS material. Restart this node after the
    /// command commits; its first heartbeat activates the new certificate.
    RotateCertificate {
        path: PathBuf,
        #[arg(long)]
        cluster_tls_cert: PathBuf,
        #[arg(long)]
        cluster_tls_key: PathBuf,
        #[arg(long)]
        cluster_tls_ca: PathBuf,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Cancel a staged rotation and atomically restore the active certificate
    /// paths in local configuration.
    AbortCertificateRotation {
        path: PathBuf,
        #[arg(long)]
        cluster_tls_cert: PathBuf,
        #[arg(long)]
        cluster_tls_key: PathBuf,
        #[arg(long)]
        cluster_tls_ca: PathBuf,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        prometheus: bool,
    },
    Route {
        path: PathBuf,
        namespace: String,
        key: String,
        #[arg(long)]
        json: bool,
    },
    Rebalance {
        path: PathBuf,
        #[arg(long)]
        apply: bool,
        #[arg(long, default_value_t = 64)]
        max_moves: usize,
        #[arg(long, default_value_t = 4)]
        max_moves_per_node: usize,
        #[arg(long, default_value_t = 256 * 1024 * 1024 * 1024)]
        max_bytes_in_flight: u64,
        #[arg(long)]
        json: bool,
    },
    Drain {
        path: PathBuf,
        node_id: String,
        #[arg(long)]
        json: bool,
    },
    Remove {
        path: PathBuf,
        node_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Freeze an immutable production-scale contract from a healthy live
    /// five-voter, RF3, server-paged cluster.
    CertifyPlan {
        path: PathBuf,
        #[arg(long, value_enum)]
        profile: CliClusterScaleProfile,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long, default_value = DEFAULT_CLUSTER_CERTIFICATION_PLAN)]
        output: PathBuf,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Start or resume an atomically checkpointed evidence collector.
    CertifyStart {
        bundle: PathBuf,
        #[arg(long, default_value = DEFAULT_CLUSTER_CERTIFICATION_PLAN)]
        plan: PathBuf,
        #[arg(long)]
        source_commit: String,
        #[arg(long)]
        source_dirty: bool,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Record one typed hardware, measurement, failure, restore, or expansion
    /// observation from a JSON file.
    CertifyRecord {
        bundle: PathBuf,
        input: PathBuf,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Stream a payload into a run-bound artifact, publish it atomically, and
    /// checkpoint its registration.
    CertifyCapture {
        bundle: PathBuf,
        #[arg(long, value_enum)]
        kind: CliClusterCertificationArtifactKind,
        payload: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, value_enum)]
        payload_format: CliClusterCertificationPayloadFormat,
        #[arg(long)]
        records: u64,
        #[arg(long)]
        started_at_ms: u64,
        #[arg(long)]
        completed_at_ms: u64,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Hash and register one raw file already stored inside the bundle.
    CertifyArtifact {
        bundle: PathBuf,
        #[arg(long, value_enum)]
        kind: CliClusterCertificationArtifactKind,
        path: PathBuf,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show durable evidence-collection progress.
    CertifyStatus {
        bundle: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Atomically materialize, verify, and publish the immutable bundle manifest.
    CertifyFinish {
        bundle: PathBuf,
        #[arg(long)]
        no_fsync: bool,
        #[arg(long)]
        json: bool,
    },
    /// Verify a completed manifest-bound production evidence bundle and fail closed.
    CertifyVerify {
        bundle: PathBuf,
        #[arg(long, default_value = DEFAULT_CLUSTER_CERTIFICATION_PLAN)]
        plan: PathBuf,
        #[arg(long, default_value = DEFAULT_CLUSTER_CERTIFICATION_REPORT)]
        report: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum HealthCommand {
    Liveness {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Readiness {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        max_size_bytes: Option<u64>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        max_size_bytes: Option<u64>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum HaCommand {
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Ship {
        primary: PathBuf,
        standby: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Promote {
        standby: PathBuf,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ReplicationCommand {
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Stream {
        path: PathBuf,
        #[arg(long, default_value_t = 0)]
        from: u64,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        listen: Option<String>,
        #[arg(long)]
        dev_localhost_plaintext: bool,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long, default_value = "default")]
        cluster_id: String,
        #[arg(long, default_value = "primary")]
        node_id: String,
        #[arg(long = "allowed-node-id")]
        allowed_node_ids: Vec<String>,
    },
    Follow {
        path: PathBuf,
        #[arg(long)]
        primary: String,
        #[arg(long, default_value = "localhost")]
        server_name: String,
        #[arg(long, default_value_t = 128 * 1024 * 1024)]
        max_frame_bytes: usize,
        #[arg(long)]
        dev_localhost_plaintext: bool,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long)]
        continuous: bool,
        #[arg(long, default_value_t = 1000)]
        reconnect_backoff_ms: u64,
        #[arg(long)]
        max_attempts: Option<usize>,
        #[arg(long, default_value = "default")]
        cluster_id: String,
        #[arg(long, default_value = "standby")]
        node_id: String,
    },
    Apply {
        path: PathBuf,
        file: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Lag {
        path: PathBuf,
        #[arg(long)]
        source_commit_seq: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    Cert {
        #[command(subcommand)]
        command: ReplicationCertCommand,
    },
    Snapshot {
        #[command(subcommand)]
        command: ReplicationSnapshotCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ConsensusCommand {
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Run {
        path: PathBuf,
        #[arg(long)]
        listen: String,
        #[arg(long, default_value = "default")]
        cluster_id: String,
        #[arg(long)]
        node_id: String,
        #[arg(long = "peer")]
        peers: Vec<String>,
        #[arg(long)]
        dev_localhost_plaintext: bool,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long, default_value_t = 1000)]
        election_timeout_ms: u64,
        #[arg(long, default_value_t = 250)]
        heartbeat_interval_ms: u64,
        #[arg(long, default_value_t = 128 * 1024 * 1024)]
        max_frame_bytes: usize,
        /// Also serve the PostgreSQL wire protocol for this node, on the same
        /// database this node replicates.
        ///
        /// A consensus node without this serves no clients, and a `serve-pg`
        /// process cannot be pointed at the same directory (one writer per
        /// database). Both halves therefore have to live in one process, over
        /// one shared handle, for a replicated node to be reachable by SQL.
        #[arg(long)]
        pg_listen: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ReplicationCertCommand {
    Check {
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        ca: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ReplicationSnapshotCommand {
    Create {
        path: PathBuf,
        out: PathBuf,
        #[arg(long, default_value = "snapshot")]
        snapshot_id: String,
        #[arg(long, default_value = "default")]
        cluster_id: String,
        #[arg(long, default_value = "local")]
        node_id: String,
        #[arg(long, default_value_t = 1024 * 1024)]
        chunk_bytes: usize,
    },
    Restore {
        input: PathBuf,
        target: PathBuf,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SyncCommand {
    Export {
        path: PathBuf,
        out: PathBuf,
        #[arg(long, default_value_t = 0)]
        since: u64,
        #[arg(long = "db-key")]
        db_key: Option<String>,
        #[arg(long = "db-key-env")]
        db_key_env: Option<String>,
        #[arg(long = "bundle-key")]
        bundle_key: Option<String>,
        #[arg(long = "bundle-key-env")]
        bundle_key_env: Option<String>,
    },
    Import {
        path: PathBuf,
        bundle: PathBuf,
        #[arg(long = "db-key")]
        db_key: Option<String>,
        #[arg(long = "db-key-env")]
        db_key_env: Option<String>,
        #[arg(long = "bundle-key")]
        bundle_key: Option<String>,
        #[arg(long = "bundle-key-env")]
        bundle_key_env: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum BackupCommand {
    /// Online checkpoint-consistent backup of a paged database: streams a
    /// base archive plus a WAL-tail archive (the consistency cut) while the
    /// handle stays open. Requires an extent-segmented store. To restore,
    /// pass BOTH artifacts to `backup restore` (base first).
    Online {
        path: PathBuf,
        base_out: PathBuf,
        wal_tail_out: PathBuf,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long = "db-key")]
        db_key: Option<String>,
        #[arg(long = "db-key-env")]
        db_key_env: Option<String>,
    },
    /// Roll a RESTORED paged database forward from a WAL archive directory
    /// (segments written by the engine's archive_wal_segments). Run after
    /// `backup restore`, before the first open.
    ApplyWal { target: PathBuf, archive: PathBuf },
    /// Delete archived WAL segments already contained in verified retained
    /// bases. Repeat --base for every base still retained; the oldest floor
    /// governs pruning.
    PruneArchive {
        archive: PathBuf,
        #[arg(long, required = true)]
        base: Vec<PathBuf>,
    },
    Create {
        path: PathBuf,
        out: PathBuf,
        #[arg(long)]
        base: Option<PathBuf>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long = "db-key")]
        db_key: Option<String>,
        #[arg(long = "db-key-env")]
        db_key_env: Option<String>,
    },
    Verify {
        backup: Vec<PathBuf>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Restore {
        /// One archive, or a chain applied in order (base first, then
        /// incrementals / the online WAL tail).
        #[arg(required = true)]
        backup: Vec<PathBuf>,
        target: PathBuf,
        #[arg(long)]
        force: bool,
        #[arg(long = "target-timestamp")]
        target_timestamp: Option<i64>,
        #[arg(long, default_value_t = 1_024)]
        pitr_record_batch: usize,
        #[arg(long, default_value_t = 1_024)]
        pitr_event_batch: usize,
        #[arg(long, default_value_t = 8 * 1024 * 1024)]
        pitr_event_bytes: usize,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Drill {
        backup: PathBuf,
        #[arg(long)]
        target: Option<PathBuf>,
        #[arg(long = "target-timestamp")]
        target_timestamp: Option<i64>,
        #[arg(long, default_value_t = 1_024)]
        pitr_record_batch: usize,
        #[arg(long, default_value_t = 1_024)]
        pitr_event_batch: usize,
        #[arg(long, default_value_t = 8 * 1024 * 1024)]
        pitr_event_bytes: usize,
        #[arg(long = "json-out")]
        json_out: Option<PathBuf>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum MigrateCommand {
    DryRun {
        path: PathBuf,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 30_000)]
        lock_timeout_ms: u64,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Apply {
        path: PathBuf,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 30_000)]
        lock_timeout_ms: u64,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Status {
        path: PathBuf,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Rollback {
        path: PathBuf,
        #[arg(long)]
        down: PathBuf,
        #[arg(long, default_value_t = 30_000)]
        lock_timeout_ms: u64,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
    Repair {
        path: PathBuf,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        key_env: Option<String>,
    },
}

#[cfg(feature = "bench")]
#[derive(Debug, Subcommand)]
enum CompatCommand {
    Test {
        path: Option<PathBuf>,
        #[arg(long, default_value = "18.4")]
        target_version: String,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
        #[arg(long)]
        markdown_out: Option<PathBuf>,
        #[arg(long)]
        fail_under: Option<f64>,
    },
    Diff {
        path: Option<PathBuf>,
        #[arg(long, default_value = "18.4")]
        target_version: String,
        #[arg(long)]
        fixtures: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        markdown_out: Option<PathBuf>,
        #[arg(long)]
        pg_host: Option<String>,
        #[arg(long)]
        pg_port: Option<u16>,
        #[arg(long)]
        pg_database: Option<String>,
        #[arg(long)]
        pg_user: Option<String>,
        #[arg(long)]
        pg_password: Option<String>,
    },
    Nightmare {
        path: Option<PathBuf>,
        #[arg(long, default_value = "18.4")]
        target_version: String,
        #[arg(long, default_value = "fixtures/pg18-nightmare")]
        fixtures: PathBuf,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        markdown_out: Option<PathBuf>,
        #[arg(long)]
        repro_dir: Option<PathBuf>,
        #[arg(long)]
        pg_host: Option<String>,
        #[arg(long)]
        pg_port: Option<u16>,
        #[arg(long)]
        pg_database: Option<String>,
        #[arg(long)]
        pg_user: Option<String>,
        #[arg(long)]
        pg_password: Option<String>,
    },
}

#[cfg(feature = "bench")]
#[derive(Debug, Subcommand)]
enum BenchCommand {
    /// PubMed-shaped ingest through the paged engine: throughput, RSS against a
    /// fixed buffer pool, and restart time.
    PagedIngest {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        /// Approximate bytes per record.
        #[arg(long, default_value_t = 1_200)]
        record_bytes: usize,
        /// Hard buffer-pool ceiling. The point of the benchmark is that RSS
        /// tracks this, not the corpus.
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        buffer_pool_bytes: u64,
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
        /// Records per transaction.
        #[arg(long, default_value_t = 1_000)]
        batch: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    /// Measure post-checkpoint WAL recovery time and RSS in a clean child
    /// process, with optional release limits.
    PagedRecovery {
        /// Logical row bytes made durable before the measured WAL suffix.
        /// Vary this while holding --wal-bytes fixed to test database-size
        /// independence.
        #[arg(long, default_value_t = 0)]
        checkpointed_data_bytes: u64,
        /// Minimum WAL suffix bytes generated after a durable checkpoint.
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        wal_bytes: u64,
        /// Approximate value bytes per generated row.
        #[arg(long, default_value_t = 1_200)]
        record_bytes: usize,
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        buffer_pool_bytes: u64,
        #[arg(long, default_value_t = 8192)]
        page_size: u32,
        #[arg(long, default_value_t = 1_000)]
        batch: usize,
        /// Include physical durability in fixture generation and recovery.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        fsync: bool,
        /// RSS sampling period used by the clean recovery process.
        #[arg(long, default_value_t = 2)]
        sample_interval_ms: u64,
        #[arg(long)]
        max_recovery_ms: Option<f64>,
        #[arg(long)]
        max_peak_rss_bytes: Option<u64>,
        #[arg(long)]
        max_rss_growth_bytes: Option<u64>,
        /// Exact source revision embedded in release evidence. Production
        /// certification requires a full 40- or 64-hex revision.
        #[arg(long)]
        source_revision: Option<String>,
        /// Operating-system cache condition declared by the operator.
        #[arg(long, default_value = "uncontrolled")]
        cache_state: String,
        /// Bounded description of how the declared cache state was prepared.
        #[arg(long)]
        cache_preparation: Option<String>,
        /// Fail preflight unless durable, attributable production evidence can
        /// be emitted.
        #[arg(long)]
        require_release_evidence: bool,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
        /// Internal clean-process recovery phase.
        #[arg(long, hide = true)]
        probe: bool,
        /// Fixture row count passed to the internal recovery phase.
        #[arg(long, hide = true)]
        expected_suffix_records: Option<u64>,
        /// Checkpointed fixture row count passed to the internal recovery phase.
        #[arg(long, hide = true)]
        expected_checkpointed_records: Option<u64>,
    },
    /// Independently verify a persisted paged-recovery JSON artifact.
    PagedRecoveryVerify {
        #[arg(long)]
        report: PathBuf,
    },
    /// Phase 0 storage baseline: memory, open time, and read amplification at a
    /// chosen data size (docs/server-paged-storage-todo.md).
    StorageBaseline {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        /// Approximate serialized metadata bytes per record.
        #[arg(long, default_value_t = 256)]
        metadata_bytes: usize,
        /// 0 disables vectors.
        #[arg(long, default_value_t = 0)]
        vector_dim: usize,
        #[arg(long, default_value_t = 0)]
        indexes: usize,
        #[arg(long, default_value_t = 1_000)]
        batch_size: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Inserts {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long, default_value_t = 1_000)]
        batch_size: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Vectors {
        #[arg(long, default_value_t = 10_000)]
        records: usize,
        #[arg(long, default_value_t = 384)]
        dim: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long, default_value_t = 20)]
        searches: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    VectorProfile {
        #[arg(long, default_value_t = 10_000)]
        records: usize,
        #[arg(long, default_value_t = 64)]
        dim: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long, default_value_t = 5)]
        searches: usize,
        #[arg(long, default_value = "cosine")]
        metric: String,
        #[arg(long, default_value = "optimized-store")]
        strategy: String,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    VectorSearchHot {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long, default_value_t = 64)]
        dim: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long, default_value_t = 1_000)]
        searches: usize,
        #[arg(long, default_value = "cosine")]
        metric: String,
        #[arg(long, default_value = "optimized-store")]
        strategy: String,
        #[arg(long)]
        path: Option<PathBuf>,
    },
    Ann {
        #[arg(long, default_value_t = 1_000_000)]
        records: usize,
        #[arg(long, default_value_t = 768)]
        dim: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Graph {
        #[arg(long, default_value_t = 100_000)]
        entities: usize,
        #[arg(long, default_value_t = 1_000_000)]
        edges: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Spatial {
        #[arg(long, default_value_t = 100_000)]
        points: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    SpatialNearest {
        #[arg(long, default_value_t = 100_000)]
        points: usize,
        #[arg(long, default_value_t = 10_000)]
        queries: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Route {
        #[arg(long, default_value_t = 10_000)]
        nodes: usize,
        #[arg(long, default_value_t = 30_000)]
        edges: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Events {
        #[arg(long, default_value_t = 100_000)]
        events: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Queue {
        #[arg(long, default_value_t = 100_000)]
        messages: usize,
        #[arg(long, default_value_t = 1_000)]
        consume_batch_size: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Projections {
        #[arg(long, default_value_t = 10_000)]
        entities: usize,
        #[arg(long, default_value_t = 2)]
        updates_per_entity: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Wearable {
        #[arg(long, default_value_t = 1_000)]
        devices: usize,
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Sql {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Indexes {
        #[arg(long, default_value_t = 1_000_000)]
        records: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Transactions {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long, default_value_t = 1_000)]
        batch_size: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Sync {
        #[arg(long, default_value_t = 10_000)]
        records: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Analytics {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Memory {
        #[arg(long, default_value_t = 100_000)]
        memories: usize,
        #[arg(long, default_value_t = 128)]
        dim: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
    Server {
        #[arg(long, default_value_t = 10)]
        clients: usize,
        #[arg(long)]
        active_query_concurrency: Option<usize>,
        #[arg(long, default_value_t = 10_000)]
        queries: usize,
        #[arg(long, default_value = "mixed")]
        scenario: String,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
        #[arg(long)]
        markdown_out: Option<PathBuf>,
    },
    ServerCert {
        #[arg(long, default_value = "ci")]
        profile: String,
        #[arg(long)]
        clients: Option<usize>,
        #[arg(long)]
        active_query_concurrency: Option<usize>,
        #[arg(long, default_value_t = 40)]
        queries: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
        #[arg(long)]
        markdown_out: Option<PathBuf>,
    },
    #[cfg(feature = "bench-comparison-engines")]
    Compare {
        #[arg(long, default_value_t = 100_000)]
        records: usize,
        #[arg(long, default_value_t = 1_000)]
        batch_size: usize,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[arg(long)]
        csv_out: Option<PathBuf>,
    },
}

const MIGRATION_VERSION_COLLECTION: &str = "__bicdb_schema_version";
const MIGRATION_HISTORY_COLLECTION: &str = "__bicdb_migration_history";
const MIGRATION_LOCK_ID: &str = "__lock";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MigrationRecordStatus {
    Pending,
    Applied,
    Failed,
    RolledBack,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationHistoryRecord {
    id: String,
    file: String,
    checksum: String,
    status: MigrationRecordStatus,
    started_at: i64,
    finished_at: Option<i64>,
    statements: usize,
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationVersionRecord {
    version: u64,
    latest_migration: Option<String>,
    applied_count: usize,
    updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationLockRecord {
    holder: String,
    acquired_at: i64,
    timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationFilePlan {
    id: String,
    file: String,
    checksum: String,
    statements: Vec<bicdb_sql::MigrationStatementPlan>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationRunReport {
    operation: String,
    safe: bool,
    applied: Vec<String>,
    skipped: Vec<String>,
    planned: Vec<MigrationFilePlan>,
    errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationStatusReport {
    version: u64,
    latest_migration: Option<String>,
    applied_count: usize,
    in_progress: Vec<MigrationHistoryRecord>,
    failed: Vec<MigrationHistoryRecord>,
    history: Vec<MigrationHistoryRecord>,
}

fn migrate_dry_run(
    db: &mut BicDb,
    dir: &Path,
    _lock_timeout_ms: u64,
) -> Result<MigrationRunReport> {
    let applied = applied_migration_ids(db)?;
    let planned = plan_pending_migration_files(dir, &applied)?;
    let errors = migration_plan_errors(&planned);
    Ok(MigrationRunReport {
        operation: "dry_run".to_string(),
        safe: errors.is_empty(),
        applied: Vec::new(),
        skipped: applied.into_iter().collect(),
        planned,
        errors,
    })
}

fn migrate_apply(db: &mut BicDb, dir: &Path, lock_timeout_ms: u64) -> Result<MigrationRunReport> {
    let applied_ids = applied_migration_ids(db)?;
    let planned = plan_pending_migration_files(dir, &applied_ids)?;
    let errors = migration_plan_errors(&planned);
    if !errors.is_empty() {
        return Ok(MigrationRunReport {
            operation: "apply".to_string(),
            safe: false,
            applied: Vec::new(),
            skipped: applied_ids.into_iter().collect(),
            planned,
            errors,
        });
    }

    acquire_migration_lock(db, lock_timeout_ms)?;
    let mut applied = Vec::new();
    let mut run_errors = Vec::new();
    for file in &planned {
        match apply_migration_file(db, file) {
            Ok(()) => applied.push(file.id.clone()),
            Err(error) => {
                let message = error.to_string();
                mark_migration_failed(db, file, &message)?;
                run_errors.push(format!("{}: {message}", file.id));
                break;
            }
        }
    }
    release_migration_lock(db)?;
    Ok(MigrationRunReport {
        operation: "apply".to_string(),
        safe: run_errors.is_empty(),
        applied,
        skipped: applied_ids.into_iter().collect(),
        planned,
        errors: run_errors,
    })
}

fn migrate_rollback(
    db: &mut BicDb,
    down: &Path,
    lock_timeout_ms: u64,
) -> Result<MigrationRunReport> {
    let history = load_migration_history(db)?;
    let Some(last_applied) = history
        .iter()
        .rev()
        .find(|record| record.status == MigrationRecordStatus::Applied)
        .cloned()
    else {
        bail!("no applied migration is available for rollback");
    };
    let sql = std::fs::read_to_string(down)?;
    let statements = plan_migration_sql(&sql)?;
    let checksum = checksum_hex(sql.as_bytes());
    let file = MigrationFilePlan {
        id: format!("rollback-{}-{}", unix_now(), last_applied.id),
        file: down.display().to_string(),
        checksum,
        statements,
    };
    let planned = vec![file.clone()];
    let errors = migration_plan_errors(&planned);
    if !errors.is_empty() {
        return Ok(MigrationRunReport {
            operation: "rollback".to_string(),
            safe: false,
            applied: Vec::new(),
            skipped: Vec::new(),
            planned,
            errors,
        });
    }
    acquire_migration_lock(db, lock_timeout_ms)?;
    let result = apply_migration_file(db, &file);
    if let Err(error) = result {
        let message = error.to_string();
        mark_migration_failed(db, &file, &message)?;
        release_migration_lock(db)?;
        return Ok(MigrationRunReport {
            operation: "rollback".to_string(),
            safe: false,
            applied: Vec::new(),
            skipped: Vec::new(),
            planned,
            errors: vec![message],
        });
    }
    mark_existing_migration_status(
        db,
        &last_applied.id,
        MigrationRecordStatus::RolledBack,
        None,
    )?;
    release_migration_lock(db)?;
    Ok(MigrationRunReport {
        operation: "rollback".to_string(),
        safe: true,
        applied: vec![file.id],
        skipped: Vec::new(),
        planned,
        errors: Vec::new(),
    })
}

fn migration_repair(db: &mut BicDb) -> Result<MigrationStatusReport> {
    let mut history = load_migration_history(db)?;
    for record in &mut history {
        if record.status == MigrationRecordStatus::Pending {
            record.status = MigrationRecordStatus::Failed;
            record.finished_at = Some(unix_now());
            record.error = Some(
                "repair marked an interrupted migration as failed; inspect schema before retry"
                    .to_string(),
            );
            save_migration_history_record(db, record)?;
        }
    }
    release_migration_lock(db)?;
    migration_status(db)
}

fn migration_status(db: &BicDb) -> Result<MigrationStatusReport> {
    let version = load_migration_version(db)?;
    let history = load_migration_history(db)?;
    let in_progress = history
        .iter()
        .filter(|record| record.status == MigrationRecordStatus::Pending)
        .cloned()
        .collect();
    let failed = history
        .iter()
        .filter(|record| record.status == MigrationRecordStatus::Failed)
        .cloned()
        .collect();
    Ok(MigrationStatusReport {
        version: version.version,
        latest_migration: version.latest_migration,
        applied_count: version.applied_count,
        in_progress,
        failed,
        history,
    })
}

fn plan_pending_migration_files(
    dir: &Path,
    applied: &BTreeSet<String>,
) -> Result<Vec<MigrationFilePlan>> {
    let mut files = std::fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    files.retain(|path| path.extension().is_some_and(|ext| ext == "sql"));
    files.sort();
    let mut planned = Vec::new();
    for path in files {
        let id = migration_id_from_path(&path)?;
        if applied.contains(&id) {
            continue;
        }
        let sql = std::fs::read_to_string(&path)?;
        planned.push(MigrationFilePlan {
            id,
            file: path.display().to_string(),
            checksum: checksum_hex(sql.as_bytes()),
            statements: plan_migration_sql(&sql)?,
        });
    }
    Ok(planned)
}

fn migration_plan_errors(planned: &[MigrationFilePlan]) -> Vec<String> {
    planned
        .iter()
        .flat_map(|file| {
            file.statements.iter().filter_map(move |statement| {
                if matches!(
                    statement.safety,
                    MigrationStatementSafety::Unsupported | MigrationStatementSafety::RowRewrite
                ) {
                    Some(format!(
                        "{} statement {}: {}",
                        file.id, statement.ordinal, statement.reason
                    ))
                } else {
                    None
                }
            })
        })
        .collect()
}

fn apply_migration_file(db: &mut BicDb, file: &MigrationFilePlan) -> Result<()> {
    let started_at = unix_now();
    save_migration_history_record(
        db,
        &MigrationHistoryRecord {
            id: file.id.clone(),
            file: file.file.clone(),
            checksum: file.checksum.clone(),
            status: MigrationRecordStatus::Pending,
            started_at,
            finished_at: None,
            statements: file.statements.len(),
            error: None,
        },
    )?;
    let sql = std::fs::read_to_string(&file.file)?;
    let statements = split_migration_sql(&sql)?;
    {
        let mut engine = SqlSession::new(db);
        for statement in statements {
            engine.execute(&statement)?;
        }
    }
    save_migration_history_record(
        db,
        &MigrationHistoryRecord {
            status: MigrationRecordStatus::Applied,
            finished_at: Some(unix_now()),
            ..load_migration_history_record(db, &file.id)?.unwrap_or(MigrationHistoryRecord {
                id: file.id.clone(),
                file: file.file.clone(),
                checksum: file.checksum.clone(),
                status: MigrationRecordStatus::Pending,
                started_at,
                finished_at: None,
                statements: file.statements.len(),
                error: None,
            })
        },
    )?;
    update_migration_version(db)?;
    Ok(())
}

fn mark_migration_failed(db: &mut BicDb, file: &MigrationFilePlan, error: &str) -> Result<()> {
    let existing = load_migration_history_record(db, &file.id)?;
    save_migration_history_record(
        db,
        &MigrationHistoryRecord {
            id: file.id.clone(),
            file: file.file.clone(),
            checksum: file.checksum.clone(),
            status: MigrationRecordStatus::Failed,
            started_at: existing
                .map(|record| record.started_at)
                .unwrap_or_else(unix_now),
            finished_at: Some(unix_now()),
            statements: file.statements.len(),
            error: Some(error.to_string()),
        },
    )
}

fn mark_existing_migration_status(
    db: &mut BicDb,
    id: &str,
    status: MigrationRecordStatus,
    error: Option<String>,
) -> Result<()> {
    let Some(mut record) = load_migration_history_record(db, id)? else {
        bail!("migration history record {id} not found");
    };
    record.status = status;
    record.finished_at = Some(unix_now());
    record.error = error;
    save_migration_history_record(db, &record)?;
    update_migration_version(db)
}

fn ensure_migration_collections(db: &mut BicDb) -> Result<()> {
    db.create_collection(MIGRATION_VERSION_COLLECTION)?;
    db.create_collection(MIGRATION_HISTORY_COLLECTION)?;
    Ok(())
}

fn load_migration_history(db: &BicDb) -> Result<Vec<MigrationHistoryRecord>> {
    let records = match db.scan_collection(MIGRATION_HISTORY_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut history = records
        .into_iter()
        .filter(|record| record.id != MIGRATION_LOCK_ID)
        .map(|record| serde_json::from_value::<MigrationHistoryRecord>(record.metadata))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    history.sort_by(|left, right| {
        left.started_at
            .cmp(&right.started_at)
            .then(left.id.cmp(&right.id))
    });
    Ok(history)
}

fn load_migration_history_record(db: &BicDb, id: &str) -> Result<Option<MigrationHistoryRecord>> {
    match db.get(MIGRATION_HISTORY_COLLECTION, id) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn save_migration_history_record(db: &mut BicDb, record: &MigrationHistoryRecord) -> Result<()> {
    ensure_migration_collections(db)?;
    db.insert(
        MIGRATION_HISTORY_COLLECTION,
        Record::new(&record.id).with_metadata(serde_json::to_value(record)?),
    )?;
    Ok(())
}

fn load_migration_version(db: &BicDb) -> Result<MigrationVersionRecord> {
    match db.get(MIGRATION_VERSION_COLLECTION, "current") {
        Ok(Some(record)) => Ok(serde_json::from_value(record.metadata.clone())?),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(MigrationVersionRecord {
            version: 0,
            latest_migration: None,
            applied_count: 0,
            updated_at: 0,
        }),
        Err(error) => Err(error.into()),
    }
}

fn save_migration_version(db: &mut BicDb, version: &MigrationVersionRecord) -> Result<()> {
    ensure_migration_collections(db)?;
    db.insert(
        MIGRATION_VERSION_COLLECTION,
        Record::new("current").with_metadata(serde_json::to_value(version)?),
    )?;
    Ok(())
}

fn update_migration_version(db: &mut BicDb) -> Result<()> {
    let history = load_migration_history(db)?;
    let applied = history
        .iter()
        .filter(|record| record.status == MigrationRecordStatus::Applied)
        .collect::<Vec<_>>();
    let latest_migration = applied.last().map(|record| record.id.clone());
    save_migration_version(
        db,
        &MigrationVersionRecord {
            version: applied.len() as u64,
            latest_migration,
            applied_count: applied.len(),
            updated_at: unix_now(),
        },
    )
}

fn applied_migration_ids(db: &BicDb) -> Result<BTreeSet<String>> {
    Ok(load_migration_history(db)?
        .into_iter()
        .filter(|record| record.status == MigrationRecordStatus::Applied)
        .map(|record| record.id)
        .collect())
}

fn acquire_migration_lock(db: &mut BicDb, timeout_ms: u64) -> Result<()> {
    ensure_migration_collections(db)?;
    if let Ok(Some(record)) = db.get(MIGRATION_HISTORY_COLLECTION, MIGRATION_LOCK_ID) {
        let lock: MigrationLockRecord = serde_json::from_value(record.metadata.clone())?;
        let age_ms = (unix_now() - lock.acquired_at).max(0) as u64 * 1000;
        if age_ms < timeout_ms {
            bail!(
                "migration lock is held by {} for {}ms; retry after --lock-timeout-ms",
                lock.holder,
                age_ms
            );
        }
    }
    let lock = MigrationLockRecord {
        holder: format!("pid-{}", std::process::id()),
        acquired_at: unix_now(),
        timeout_ms,
    };
    db.insert(
        MIGRATION_HISTORY_COLLECTION,
        Record::new(MIGRATION_LOCK_ID).with_metadata(serde_json::to_value(lock)?),
    )?;
    Ok(())
}

fn release_migration_lock(db: &mut BicDb) -> Result<()> {
    match db.delete(MIGRATION_HISTORY_COLLECTION, MIGRATION_LOCK_ID) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn migration_id_from_path(path: &Path) -> Result<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow::anyhow!("invalid migration filename {}", path.display()))
}

fn checksum_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn print_migration_report(report: &MigrationRunReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    println!("BicDB migration {}", report.operation);
    println!("Safe: {}", report.safe);
    println!("Planned files: {}", report.planned.len());
    println!("Applied files: {}", report.applied.len());
    for file in &report.planned {
        println!(
            "- {}: {} statement(s), checksum {}",
            file.id,
            file.statements.len(),
            file.checksum
        );
        for statement in &file.statements {
            println!(
                "  {}. {:?}: {}",
                statement.ordinal, statement.safety, statement.reason
            );
        }
    }
    for error in &report.errors {
        println!("Error: {error}");
    }
    Ok(())
}

fn print_migration_status(status: &MigrationStatusReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(status)?);
        return Ok(());
    }
    println!("BicDB migration status");
    println!("Version: {}", status.version);
    println!(
        "Latest migration: {}",
        status.latest_migration.as_deref().unwrap_or("(none)")
    );
    println!("Applied count: {}", status.applied_count);
    println!("In progress: {}", status.in_progress.len());
    println!("Failed: {}", status.failed.len());
    for record in &status.history {
        println!("- {} {:?} {}", record.id, record.status, record.file);
    }
    Ok(())
}

fn read_status_spill_outcomes(path: &Path) -> Result<Vec<(u64, TxStatus)>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("status-spill outcomes must be a regular file, not a symlink");
    }
    let file = std::fs::File::open(path)?;
    let mut outcomes = Vec::new();
    for (line_index, line) in io::BufReader::new(file).lines().enumerate() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line == "xid\tstatus" {
            continue;
        }
        let Some((xid, status)) = line.split_once('\t') else {
            bail!(
                "{}:{} must be tab-separated xid and status",
                path.display(),
                line_index + 1
            );
        };
        let xid = xid.parse::<u64>().map_err(|error| {
            anyhow::anyhow!(
                "{}:{} has invalid xid: {error}",
                path.display(),
                line_index + 1
            )
        })?;
        let status = match status {
            "committed" | "1" => TxStatus::Committed,
            "aborted" | "2" => TxStatus::Aborted,
            _ => bail!(
                "{}:{} status must be committed, aborted, 1, or 2",
                path.display(),
                line_index + 1
            ),
        };
        outcomes.push((xid, status));
    }
    Ok(outcomes)
}

fn write_status_spill_outcomes(path: &Path, outcomes: &[(u64, TxStatus)]) -> Result<()> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    let mut writer = io::BufWriter::new(file);
    writeln!(writer, "xid\tstatus")?;
    for (xid, status) in outcomes {
        let status = match status {
            TxStatus::Committed => "committed",
            TxStatus::Aborted => "aborted",
            TxStatus::InProgress => bail!("cannot write a non-terminal status-spill outcome"),
        };
        writeln!(writer, "{xid}\t{status}")?;
    }
    writer.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime_mode = std::env::var("BICDB_RUNTIME_MODE").ok();

    if runtime_mode.as_deref() == Some("cell") {
        if cli.command.is_none() && cli.tui.is_none() {
            return run_cell_serve_from_environment();
        }
        if !matches!(cli.command.as_ref(), Some(Command::Cell { .. })) {
            bail!(
                "BICDB_RUNTIME_MODE=cell may launch only `bicdb cell ...`; general BicDB commands are unavailable"
            );
        }
    } else if runtime_mode
        .as_deref()
        .is_some_and(|mode| !matches!(mode, "development" | "general-server"))
    {
        bail!("BICDB_RUNTIME_MODE must be development, general-server, or cell");
    }

    if let Some(path) = cli.tui {
        if cli.command.is_some() {
            bail!("--tui cannot be combined with a subcommand");
        }
        let encryption = db_encryption_for_path(&path, cli.tui_key, cli.tui_key_env)?;
        let db = match encryption.clone() {
            Some(config) => BicDb::open_with_encryption(&path, DbConfig::default(), config)?,
            None => BicDb::open(&path)?,
        };
        return tui::run_tui(path, db, encryption);
    }

    let Some(command) = cli.command else {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    };

    match command {
        Command::Cell { command } => run_cell_command(command)?,
        Command::App {
            path,
            package_root,
            trusted_keys,
            secrets,
            blob_config,
            integration_config,
            command,
        } => {
            app_command::run(
                path,
                package_root,
                trusted_keys,
                secrets,
                blob_config,
                integration_config,
                command,
            )?;
        }
        Command::Init {
            path,
            encrypted,
            key,
            key_env,
        } => {
            let db = if encrypted {
                BicDb::open_with_encryption(
                    &path,
                    DbConfig::default(),
                    required_passphrase_config(key, key_env, "BICDB_KEY", "database encryption")?,
                )?
            } else {
                BicDb::open(&path)?
            };
            db.flush()?;
            println!("Initialized BicDB at {}", path.display());
            if encrypted {
                println!("Encryption: enabled");
            }
        }
        Command::Export {
            path,
            collection,
            after_id,
            after_id_hex,
            batch_rows,
            batch_bytes,
            locality,
        } => {
            if batch_rows == 0 || batch_bytes == 0 {
                bail!("--batch-rows and --batch-bytes must be non-zero");
            }
            let db = BicDb::open_with_config(
                &path,
                DbConfig::default()
                    .with_storage_mode(storage_mode(&path)?)
                    .with_paged_rowid_registry(false)
                    .with_sync_outbox(false)
                    .with_secondary_indexes(false),
            )?;
            let after_id = match after_id_hex {
                Some(encoded) => Some(
                    String::from_utf8(hex::decode(&encoded).map_err(|error| {
                        anyhow::anyhow!("--after-id-hex is not valid hexadecimal: {error}")
                    })?)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "--after-id-hex does not encode a UTF-8 primary key: {error}"
                        )
                    })?,
                ),
                None => after_id,
            };
            let stdout = io::stdout();
            let mut writer = io::BufWriter::new(stdout.lock());
            let mut cursor = after_id;
            loop {
                let rows = if locality {
                    db.scan_collection_locality_batch_after(
                        &collection,
                        cursor.as_deref(),
                        batch_rows,
                        batch_bytes,
                    )?
                } else {
                    db.scan_collection_batch_after(
                        &collection,
                        cursor.as_deref(),
                        batch_rows,
                        batch_bytes,
                    )?
                };
                if rows.is_empty() {
                    break;
                }
                for row in &rows {
                    serde_json::to_writer(&mut writer, row)?;
                    writer.write_all(b"\n")?;
                }
                cursor = rows.last().map(|row| row.id.clone());
            }
            writer.flush()?;
        }
        Command::Store { command } => match command {
            StoreCommand::Segment {
                path,
                extent_bytes,
                page_size,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                let pages_path = paged_dir.join("store.pages");
                if !pages_path.is_file() {
                    bail!(
                        "no paged store at {}; `store segment` only applies to server_paged databases",
                        pages_path.display()
                    );
                }
                // The lock refuses to run while the database is open, and no
                // other opener can start mid-relayout.
                let _lock = DirectoryLock::acquire(&paged_dir)?;
                let started = std::time::Instant::now();
                bicdb_page::convert_extent_layout(&pages_path, page_size, extent_bytes, true)?;
                let layout = if extent_bytes == 0 {
                    "monolithic".to_string()
                } else {
                    let mut segments = 1usize;
                    while paged_dir.join(format!("store.pages.{segments}")).is_file() {
                        segments += 1;
                    }
                    format!("{segments} segment file(s) of up to {extent_bytes} bytes")
                };
                println!("Relayout complete in {:?}: {}", started.elapsed(), layout);
            }
            StoreCommand::MigrateMode {
                source,
                target,
                mode,
                batch,
            } => {
                let target_mode = bicdb_core::StorageMode::from_name(&mode);
                if !target_mode.is_supported() {
                    return Err(anyhow::anyhow!(
                        "unknown storage mode `{mode}`; use `server_paged` or `embedded_memory`"
                    ));
                }
                let report = bicdb_core::storage_mode_migration::migrate_storage_mode(
                    &source,
                    &target,
                    target_mode,
                    batch,
                )?;
                println!(
                    "Migrated {} -> {}: {} collections, {} records, {} indexes ({} -> {})",
                    source.display(),
                    target.display(),
                    report.collections,
                    report.records,
                    report.indexes,
                    report.source_mode,
                    report.target_mode
                );
                println!(
                    "The new database must be OPENED as `{}` — a default open is refused by the \
                     storage-mode fence. Point the service at {} once verified.",
                    report.target_mode,
                    target.display()
                );
            }
            StoreCommand::MigrateMeta {
                path,
                page_size,
                confirm,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                if !paged_dir.join("store.pages").is_file() {
                    bail!(
                        "no paged store at {}; `store migrate-meta` only applies to server_paged databases",
                        paged_dir.join("store.pages").display()
                    );
                }
                let inspection = bicdb_page::inspect_meta_page(&paged_dir, page_size)?;
                println!(
                    "Meta page {} (pre-recovery, on-disk bytes):",
                    inspection.meta_page
                );
                if !inspection.initialized {
                    println!("  never written — nothing to migrate");
                    return Ok(());
                }
                println!("  next_xid: {}", inspection.next_xid);
                match &inspection.extension_error {
                    None => {
                        println!("  extension region: valid under the current layout");
                        println!("  nothing to transform");
                        return Ok(());
                    }
                    Some(reason) => {
                        println!("  extension region: INVALID — {reason}");
                        println!(
                            "  consistent with a store last written by an engine predating \
                             durable abort exceptions"
                        );
                    }
                }
                if !confirm {
                    println!(
                        "dry run only — re-run with --confirm to replay the WAL and transform the meta page"
                    );
                    return Ok(());
                }
                let started = std::time::Instant::now();
                // The full open replays the WAL first (recovery rewrites the
                // meta page from old-layout images), then rewrites the page in
                // the current layout and checkpoints — see
                // `PagedStoreOptions::accept_legacy_meta`.
                let db = BicDb::open_with_config(
                    &path,
                    DbConfig::default()
                        .with_storage_mode(StorageMode::ServerPaged)
                        .with_paged_page_size(page_size)
                        .with_paged_accept_legacy_meta(true),
                )?;
                db.close()?;
                let after = bicdb_page::inspect_meta_page(&paged_dir, page_size)?;
                if let Some(reason) = after.extension_error {
                    bail!("transform failed — extension region still invalid: {reason}");
                }
                println!(
                    "Transform complete in {:?}: meta page now uses the current layout; \
                     the store opens on every engine version without accept_legacy_meta",
                    started.elapsed()
                );
            }
            StoreCommand::RepairStatusSpill {
                path,
                expected_head,
                expected_pages,
                expected_entries,
                outcomes,
                page_size,
                confirm,
                json,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                if !paged_dir.join("store.pages").is_file() {
                    bail!(
                        "no paged store at {}; `store repair-status-spill` only applies to server_paged databases",
                        paged_dir.join("store.pages").display()
                    );
                }
                let outcomes = read_status_spill_outcomes(&outcomes)?;
                let report = bicdb_page::repair_status_spill(
                    &paged_dir,
                    page_size,
                    expected_head,
                    expected_pages,
                    expected_entries,
                    &outcomes,
                    confirm,
                )?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!("BicDB status-spill repair");
                    println!(
                        "Previous: {} entries / {} pages at head {}",
                        report.previous_entries, report.previous_pages, report.previous_head
                    );
                    println!(
                        "Replacement: {} entries / {} pages; xid {:?} through {:?}",
                        report.replacement_entries,
                        report.replacement_pages,
                        report.first_xid,
                        report.last_xid
                    );
                    if report.applied {
                        println!(
                            "Applied and read-back verified at head {}",
                            report.replacement_head
                        );
                    } else {
                        println!(
                            "Dry run only — re-run with --confirm to publish this exact replacement"
                        );
                    }
                }
            }
            StoreCommand::ExtractWalOutcomes {
                path,
                start_xid,
                end_xid,
                output,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                let wal_path = paged_dir.join("store.wal");
                if !wal_path.is_file() {
                    bail!("no paged WAL at {}", wal_path.display());
                }
                let _lock = DirectoryLock::acquire(&paged_dir)?;
                let wal = bicdb_page::Wal::open(&wal_path, false)?;
                let (outcomes, truncated_bytes) =
                    wal.terminal_outcomes_in_range(start_xid, end_xid)?;
                write_status_spill_outcomes(&output, &outcomes)?;
                println!(
                    "Wrote {} terminal outcomes in [{start_xid}, {end_xid}) to {}; validated WAL torn-tail bytes: {truncated_bytes}",
                    outcomes.len(),
                    output.display()
                );
            }
            StoreCommand::ExtractStatusSpill {
                path,
                head,
                pages,
                entries,
                output,
                page_size,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                if !paged_dir.join("store.pages").is_file() {
                    bail!("no paged store at {}", paged_dir.display());
                }
                let outcomes =
                    bicdb_page::extract_status_spill(&paged_dir, page_size, head, pages, entries)?;
                write_status_spill_outcomes(&output, &outcomes)?;
                println!(
                    "Validated and wrote {} status-spill outcomes from head {head} to {}",
                    outcomes.len(),
                    output.display()
                );
            }
        },
        Command::Inspect { path, key, key_env } => {
            let db = open_db_with_key_args(&path, key, key_env)?;
            let stats = db.stats()?;
            println!("BicDB Inspect");
            println!("-------------");
            println!("Path: {}", stats.path.display());
            println!("Collections: {}", stats.collection_count);
            println!("Records: {}", stats.record_count);
            println!("Pending sync ops: {}", stats.pending_sync_ops);
            println!("Database size: {} bytes", stats.size_bytes);
            for collection in stats.collections {
                println!(
                    "- {}: mode={:?}, records={}, vector_dim={:?}, segment_bytes={}, logical_record_bytes={}, storage_overhead_bytes={}, last_segment_offset={:?}",
                    collection.name,
                    collection.mode,
                    collection.record_count,
                    collection.vector_dim,
                    collection.segment_bytes,
                    collection.logical_record_bytes,
                    collection.storage_overhead_bytes,
                    collection.last_segment_offset
                );
            }
        }
        Command::Verify { path, key, key_env } => {
            let db = open_db_with_key_args(&path, key, key_env)?;
            let report = db.verify_integrity()?;
            print_integrity_report("Verified BicDB integrity", &report);
            db.close()?;
        }
        Command::Check {
            path,
            key,
            key_env,
            json,
            backups,
            backup_key: backup_key_arg,
            backup_key_env,
        } => {
            let (report, sql_report) = database_integrity_check(&path, key, key_env)?;
            if !json {
                print_integrity_report("Checked BicDB integrity", &report);
                print_sql_integrity_report(&sql_report);
            }
            if !backups.is_empty() {
                let passphrase = backup_key(backup_key_arg, backup_key_env)?;
                let mut backup_reports = Vec::new();
                for backup in backups {
                    let backup_report = verify_backup(&backup, &passphrase)?;
                    if json {
                        backup_reports.push(json!({
                            "path": backup,
                            "report": backup_report,
                        }));
                    } else {
                        println!(
                            "Backup verified: {} id={} files_included={} manifest={}",
                            backup.display(),
                            backup_report.backup_id,
                            backup_report.files_included,
                            backup_report.manifest_hash
                        );
                    }
                }
                if json {
                    print_database_integrity_json(&report, &sql_report, backup_reports)?;
                }
            } else if json {
                print_database_integrity_json(&report, &sql_report, Vec::new())?;
            }
        }
        Command::Integrity { command } => match command {
            IntegrityCommand::Check {
                path,
                key,
                key_env,
                json,
            } => {
                let (report, sql_report) = database_integrity_check(&path, key, key_env)?;
                if json {
                    print_database_integrity_json(&report, &sql_report, Vec::new())?;
                } else {
                    print_integrity_report("Checked BicDB integrity", &report);
                    print_sql_integrity_report(&sql_report);
                }
            }
            IntegrityCommand::ChainInspect {
                path,
                collection,
                record_id,
                key,
                key_env,
                json,
            } => {
                let paged = open_paged_records_for_recovery(&path, key, key_env)?;
                let inspection = paged.inspect_version_chain(&collection, &record_id)?;
                let head_token = inspection.head.map(version_chain_locator_token);
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "collection": collection,
                            "record_id": record_id,
                            "head_token": head_token,
                            "inspection": inspection,
                        }))?
                    );
                } else {
                    println!("BicDB MVCC chain inspection");
                    println!("Collection: {collection}");
                    println!("Record id: {record_id}");
                    println!("Head: {}", head_token.as_deref().unwrap_or("(none)"));
                    println!("Versions examined: {}", inspection.versions_examined);
                    println!("Terminal: {:?}", inspection.terminal);
                    println!("Healthy: {}", inspection.healthy());
                    for version in &inspection.samples {
                        println!(
                            "- {} xmin={} xmax={} previous={}",
                            version_chain_locator_token(version.locator),
                            version.xmin,
                            version.xmax,
                            version
                                .previous
                                .map(version_chain_locator_token)
                                .as_deref()
                                .unwrap_or("(none)")
                        );
                    }
                }
            }
            IntegrityCommand::ChainRepair {
                path,
                collection,
                record_id,
                expected_head,
                record_json,
                apply,
                key,
                key_env,
                json,
            } => {
                let expected_head = parse_version_chain_locator(&expected_head)?;
                let replacement: Record = serde_json::from_slice(&std::fs::read(&record_json)?)?;
                if replacement.id != record_id {
                    bail!(
                        "replacement record id `{}` does not match --record-id `{record_id}`",
                        replacement.id
                    );
                }
                let paged = open_paged_records_for_recovery(&path, key, key_env)?;
                let inspection = paged.inspect_version_chain(&collection, &record_id)?;
                if inspection.head != Some(expected_head) {
                    bail!(
                        "chain head changed: expected {}, found {}; inspect again before repair",
                        version_chain_locator_token(expected_head),
                        inspection
                            .head
                            .map(version_chain_locator_token)
                            .as_deref()
                            .unwrap_or("(none)")
                    );
                }
                if !inspection.terminal.is_repairable() {
                    bail!(
                        "repair refused: chain ends with {:?}, not a cycle or safety-limit fault",
                        inspection.terminal
                    );
                }
                if !apply {
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "dry_run": true,
                                "collection": collection,
                                "record_id": record_id,
                                "expected_head": version_chain_locator_token(expected_head),
                                "record_json": record_json,
                                "inspection": inspection,
                            }))?
                        );
                    } else {
                        println!("BicDB MVCC chain repair dry run");
                        println!("Collection: {collection}");
                        println!("Record id: {record_id}");
                        println!(
                            "Confirmed fault: {:?} at head {}",
                            inspection.terminal,
                            version_chain_locator_token(expected_head)
                        );
                        println!("Replacement: {}", record_json.display());
                        println!("No data changed. Add --apply to publish this exact repair.");
                    }
                } else {
                    let repair = paged.replace_faulty_version_chain(
                        &collection,
                        &record_id,
                        expected_head,
                        &replacement,
                    )?;
                    let integrity = paged.verify_integrity(64)?;
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "dry_run": false,
                                "collection": collection,
                                "record_id": record_id,
                                "repair": repair,
                                "integrity": integrity,
                            }))?
                        );
                    } else {
                        println!("Repaired BicDB MVCC chain");
                        println!("Collection: {collection}");
                        println!("Record id: {record_id}");
                        println!(
                            "Head: {} -> {}",
                            version_chain_locator_token(repair.previous_head),
                            version_chain_locator_token(repair.replacement_head)
                        );
                        print_paged_integrity_report(&integrity);
                    }
                    if !integrity.valid {
                        bail!(
                            "the targeted repair committed, but post-repair paged integrity verification found other faults"
                        );
                    }
                }
            }
            IntegrityCommand::ChainVerify {
                path,
                max_fault_samples,
                key,
                key_env,
                json,
            } => {
                let paged = open_paged_records_for_recovery(&path, key, key_env)?;
                let report = paged.verify_integrity(max_fault_samples)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    print_paged_integrity_report(&report);
                }
                if !report.valid {
                    bail!("paged storage integrity verification found faults");
                }
            }
            IntegrityCommand::FreeListTruncate {
                path,
                apply,
                page_size,
                json,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                let _lock = DirectoryLock::acquire(&paged_dir)?;
                let paths = PagedPaths::in_dir(&paged_dir);
                let page_store = PageStore::open(
                    &paths.pages,
                    PageStoreOptions {
                        page_size,
                        fsync: true,
                        create: false,
                        extent_bytes: 0,
                    },
                )?;
                let report = page_store.truncate_free_list_at_corruption(apply)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "BicDB free-list truncation {}",
                        if apply { "repair" } else { "dry run" }
                    );
                    println!("Path: {}", path.display());
                    println!("Advertised free pages: {}", report.advertised);
                    println!("Provably-valid prefix: {}", report.valid_prefix);
                    match (&report.corrupt_page, &report.corrupt_reason) {
                        (Some(page), Some(reason)) => {
                            println!("Corrupt at page {page}: {reason}");
                        }
                        _ => println!("No corruption found."),
                    }
                    if report.applied {
                        println!(
                            "Repair published: list truncated to {} pages; {} advertised pages abandoned.",
                            report.valid_prefix,
                            report.advertised.saturating_sub(report.valid_prefix)
                        );
                    } else if !apply {
                        println!("No data changed. Add --apply to publish the truncation.");
                    } else {
                        println!("List is healthy; nothing to repair.");
                    }
                }
            }
            IntegrityCommand::FreeListRepair {
                path,
                expected_free_pages,
                apply,
                page_size,
                json,
            } => {
                let paged_dir = path.join(DEFAULT_PAGED_DIR);
                let _lock = DirectoryLock::acquire(&paged_dir)?;
                let paths = PagedPaths::in_dir(&paged_dir);
                let page_store = PageStore::open(
                    &paths.pages,
                    PageStoreOptions {
                        page_size,
                        fsync: true,
                        create: false,
                        extent_bytes: 0,
                    },
                )?;
                let head = page_store.free_list_head();
                let found = page_store.free_page_count();
                if head != 0 {
                    bail!(
                        "repair refused: free-list head is {head}; this command only abandons a detached count with head zero"
                    );
                }
                if found != expected_free_pages || found == 0 {
                    bail!(
                        "repair fence failed: expected {expected_free_pages} detached free pages, found {found}"
                    );
                }
                if !apply {
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "dry_run": true,
                                "path": path,
                                "free_list_head": head,
                                "detached_free_pages": found,
                            }))?
                        );
                    } else {
                        println!("BicDB detached free-list repair dry run");
                        println!("Path: {}", path.display());
                        println!("Free-list head: {head}");
                        println!("Detached free pages: {found}");
                        println!(
                            "No data changed. Add --apply to abandon only this reusable-space claim."
                        );
                    }
                } else {
                    let abandoned =
                        page_store.abandon_detached_free_page_count(expected_free_pages)?;
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "dry_run": false,
                                "path": path,
                                "previous_free_list_head": head,
                                "abandoned_free_pages": abandoned,
                                "free_pages": page_store.free_page_count(),
                            }))?
                        );
                    } else {
                        println!("Repaired detached BicDB free-list metadata");
                        println!("Path: {}", path.display());
                        println!("Abandoned free pages: {abandoned}");
                        println!(
                            "Free pages now advertised: {}",
                            page_store.free_page_count()
                        );
                    }
                }
            }
        },
        Command::Security { command } => match command {
            SecurityCommand::ProtectedDataReleaseGate {
                path,
                source_root,
                key,
                key_env,
                json,
            } => {
                let mut db = open_db_with_key_args(&path, key, key_env)?;
                let report = db.protected_data_security_release_gate(&source_root)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "protected data security release gate: {}",
                        if report.passed { "passed" } else { "failed" }
                    );
                    println!(
                        "Backfill: scanned={}, encrypted_rows={}, blind_index_rows_written={}, skipped={}, verified={}",
                        report.backfill.scanned_rows,
                        report.backfill.encrypted_rows,
                        report.backfill.blind_index_rows_written,
                        report.backfill.skipped_rows,
                        report.backfill.verification_status
                    );
                    println!(
                        "Blind-index coverage: passed={}, missing={}",
                        report.blind_index_coverage.passed,
                        report.blind_index_coverage.missing_indexes.len()
                    );
                    println!(
                        "Ciphertext sampling: passed={}, sampled={}, findings={}",
                        report.ciphertext_sampling.passed,
                        report.ciphertext_sampling.sampled_fields,
                        report.ciphertext_sampling.plaintext_findings.len()
                    );
                    println!(
                        "Tenant isolation: passed={}, surfaces={}, leaks={}",
                        report.tenant_isolation.passed,
                        report.tenant_isolation.surfaces_checked.len(),
                        report.tenant_isolation.leaks.len()
                    );
                    println!(
                        "Raw SQL audit: passed={}, scanned_files={}, violations={}",
                        report.raw_sql_audit.passed,
                        report.raw_sql_audit.scanned_files,
                        report.raw_sql_audit.violations.len()
                    );
                }
                if !report.passed {
                    bail!("protected data security release gate failed");
                }
            }
            SecurityCommand::ProductionGate {
                path,
                profile,
                host,
                require_auth,
                auth_method,
                allow_remote_no_auth,
                require_tls,
                tls_cert,
                tls_key,
                tls_client_ca,
                db_key_env,
                protected_data_field_key_env,
                protected_data_hmac_key_env,
                backup_key_env,
                audit_retention_days,
                audit_tamper_evidence,
                protected_data_evidence,
                dependency_evidence,
                evidence_max_age_days,
                json,
            } => {
                let report = production_gate_report(ProductionGateInput {
                    path,
                    profile,
                    host,
                    require_auth,
                    auth_method,
                    allow_remote_no_auth,
                    require_tls,
                    tls_cert,
                    tls_key,
                    tls_client_ca,
                    db_key_env,
                    protected_data_field_key_env,
                    protected_data_hmac_key_env,
                    backup_key_env,
                    audit_retention_days,
                    audit_tamper_evidence,
                    protected_data_evidence,
                    dependency_evidence,
                    evidence_max_age_days,
                })?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "Production security gate: {}",
                        if report.passed { "passed" } else { "failed" }
                    );
                    println!("Profile: {}", report.profile);
                    for check in &report.checks {
                        println!(
                            "- {}: {}{}",
                            check.name,
                            if check.passed { "passed" } else { "failed" },
                            check
                                .detail
                                .as_ref()
                                .map(|detail| format!(" ({detail})"))
                                .unwrap_or_default()
                        );
                    }
                }
                if !report.passed {
                    bail!("production security gate failed");
                }
            }
            SecurityCommand::SupplyChainAudit { source_root, json } => {
                let report = supply_chain_audit_report(&source_root);
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "Supply-chain audit: {}",
                        if report.passed { "passed" } else { "failed" }
                    );
                    println!("{}", report.detail);
                }
                if !report.passed {
                    bail!("supply-chain audit failed");
                }
            }
        },
        Command::Compact {
            path,
            collection,
            reclaim_threshold_percent,
            schedule,
            max_io_bytes_per_sec,
            max_pause_ms,
            force,
            key,
            key_env,
        } => {
            let mut db = open_db_with_key_args(&path, key, key_env)?;
            let report = if let Some(collection) = collection {
                db.compact_collection(&collection)?
            } else {
                db.compact_with_options(CompactionOptions {
                    reclaim_threshold_percent,
                    schedule,
                    max_io_bytes_per_sec,
                    max_pause_ms,
                    force,
                    ..Default::default()
                })?
            };
            print_compaction_report(&report);
        }
        #[cfg(feature = "bench")]
        Command::Bench { command } => match command {
            BenchCommand::PagedIngest {
                records,
                record_bytes,
                buffer_pool_bytes,
                page_size,
                batch,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("paged-ingest"));
                let report = run_paged_ingest(
                    path,
                    records,
                    record_bytes,
                    buffer_pool_bytes,
                    page_size,
                    batch,
                )?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::PagedRecovery {
                checkpointed_data_bytes,
                wal_bytes,
                record_bytes,
                buffer_pool_bytes,
                page_size,
                batch,
                fsync,
                sample_interval_ms,
                max_recovery_ms,
                max_peak_rss_bytes,
                max_rss_growth_bytes,
                source_revision,
                cache_state,
                cache_preparation,
                require_release_evidence,
                path,
                json_out,
                csv_out,
                probe,
                expected_suffix_records,
                expected_checkpointed_records,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("paged-recovery"));
                if probe {
                    let expected_suffix_records = expected_suffix_records.ok_or_else(|| {
                        anyhow::anyhow!(
                            "internal recovery probe requires --expected-suffix-records"
                        )
                    })?;
                    let expected_checkpointed_records =
                        expected_checkpointed_records.ok_or_else(|| {
                            anyhow::anyhow!(
                                "internal recovery probe requires --expected-checkpointed-records"
                            )
                        })?;
                    let report = run_paged_recovery_probe(
                        path,
                        expected_checkpointed_records,
                        expected_suffix_records,
                        page_size,
                        buffer_pool_bytes,
                        fsync,
                        sample_interval_ms,
                    )?;
                    println!("{}", serde_json::to_string(&report)?);
                    return Ok(());
                }

                let cache_state = cache_state.parse::<PagedRecoveryCacheState>()?;
                let executable = std::env::current_exe()?;
                let environment = collect_paged_recovery_environment(
                    &executable,
                    &path,
                    source_revision,
                    cache_state,
                    cache_preparation,
                )?;
                if require_release_evidence {
                    validate_paged_recovery_release_environment(&environment, fsync)?;
                }
                let fixture = prepare_paged_recovery_fixture(
                    &path,
                    checkpointed_data_bytes,
                    wal_bytes,
                    record_bytes,
                    buffer_pool_bytes,
                    page_size,
                    batch,
                    fsync,
                )?;
                let output = ProcessCommand::new(executable)
                    .arg("bench")
                    .arg("paged-recovery")
                    .arg("--path")
                    .arg(&path)
                    .arg("--checkpointed-data-bytes")
                    .arg(checkpointed_data_bytes.to_string())
                    .arg("--wal-bytes")
                    .arg(wal_bytes.to_string())
                    .arg("--record-bytes")
                    .arg(record_bytes.to_string())
                    .arg("--buffer-pool-bytes")
                    .arg(buffer_pool_bytes.to_string())
                    .arg("--page-size")
                    .arg(page_size.to_string())
                    .arg("--batch")
                    .arg(batch.to_string())
                    .arg("--fsync")
                    .arg(fsync.to_string())
                    .arg("--sample-interval-ms")
                    .arg(sample_interval_ms.to_string())
                    .arg("--probe")
                    .arg("--expected-suffix-records")
                    .arg(fixture.suffix_records.to_string())
                    .arg("--expected-checkpointed-records")
                    .arg(fixture.checkpointed_records.to_string())
                    .output()?;
                if !output.status.success() {
                    bail!(
                        "clean recovery probe failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }
                let probe: PagedRecoveryProbeReport = serde_json::from_slice(&output.stdout)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "clean recovery probe emitted invalid JSON: {error}; output: {}",
                            String::from_utf8_lossy(&output.stdout).trim()
                        )
                    })?;
                let report = finish_paged_recovery_bench(
                    environment,
                    fixture,
                    probe,
                    PagedRecoveryBenchLimits {
                        max_recovery_ms,
                        max_peak_rss_bytes,
                        max_rss_growth_bytes,
                        require_release_evidence,
                    },
                )?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
                if !report.passed {
                    bail!("paged recovery benchmark exceeded its release limits");
                }
            }
            BenchCommand::PagedRecoveryVerify { report } => {
                let bytes = read_paged_recovery_evidence(&report)?;
                let evidence: PagedRecoveryBenchReport = serde_json::from_slice(&bytes)?;
                evidence.verify_integrity()?;
                println!(
                    "verified paged recovery report {} ({})",
                    evidence.checksum_sha256,
                    if evidence.passed { "PASS" } else { "FAIL" }
                );
                if !evidence.passed {
                    bail!(
                        "paged recovery report is internally valid but failed its declared gates"
                    );
                }
            }
            BenchCommand::StorageBaseline {
                records,
                metadata_bytes,
                vector_dim,
                indexes,
                batch_size,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("storage-baseline"));
                let report = run_storage_baseline(
                    path,
                    records,
                    metadata_bytes,
                    vector_dim,
                    indexes,
                    batch_size,
                )?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Inserts {
                records,
                batch_size,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("inserts"));
                let report = run_insert_bench(path, records, batch_size)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Vectors {
                records,
                dim,
                top_k,
                searches,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("vectors"));
                let report = run_vector_bench(path, records, dim, top_k, searches)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::VectorProfile {
                records,
                dim,
                top_k,
                searches,
                metric,
                strategy,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("vector-profile"));
                let report = run_vector_profile_with_strategy(
                    path,
                    records,
                    dim,
                    top_k,
                    searches,
                    parse_vector_metric(&metric)?,
                    parse_vector_profile_strategy(&strategy)?,
                )?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::VectorSearchHot {
                records,
                dim,
                top_k,
                searches,
                metric,
                strategy,
                path,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("vector-search-hot"));
                let report = run_vector_search_hot(
                    path,
                    records,
                    dim,
                    top_k,
                    searches,
                    parse_vector_metric(&metric)?,
                    parse_vector_profile_strategy(&strategy)?,
                )?;
                print!("{report}");
            }
            BenchCommand::Ann {
                records,
                dim,
                top_k,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("ann"));
                let report = run_ann_bench(path, records, dim, top_k)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Graph {
                entities,
                edges,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("graph"));
                let report = run_graph_bench(path, entities, edges)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Spatial {
                points,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("spatial"));
                let report = run_spatial_bench(path, points)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::SpatialNearest {
                points,
                queries,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("spatial-nearest"));
                let report = run_spatial_nearest_bench(path, points, queries)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Route {
                nodes,
                edges,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("route"));
                let report = run_route_bench(path, nodes, edges)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Events {
                events,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("events"));
                let report = run_event_bench(path, events)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Queue {
                messages,
                consume_batch_size,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("queue"));
                let report = run_queue_bench(path, messages, consume_batch_size)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Projections {
                entities,
                updates_per_entity,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("projections"));
                let report = run_projection_bench(path, entities, updates_per_entity)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Wearable {
                devices,
                records,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("wearable"));
                let report = run_wearable_bench(path, devices, records)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Sql {
                records,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("sql"));
                let report = run_sql_bench(path, records)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Indexes {
                records,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("indexes"));
                let report = run_index_bench(path, records)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Transactions {
                records,
                batch_size,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("transactions"));
                let report = run_transaction_bench(path, records, batch_size)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Sync {
                records,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("sync"));
                let report = run_sync_bench(path, records)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Analytics {
                records,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("analytics"));
                let report = run_analytics_bench(path, records)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Memory {
                memories,
                dim,
                top_k,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("memory"));
                let report = run_memory_bench(path, memories, dim, top_k)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
            }
            BenchCommand::Server {
                clients,
                active_query_concurrency,
                queries,
                scenario,
                path,
                json_out,
                csv_out,
                markdown_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("server"));
                let scenario = scenario.parse::<ServerBenchScenario>()?;
                let active_query_concurrency = active_query_concurrency.unwrap_or(clients);
                let report = if scenario == ServerBenchScenario::Mixed
                    && active_query_concurrency == clients
                {
                    run_server_bench(path, clients, queries)?
                } else {
                    run_server_bench_with_config(
                        path,
                        clients,
                        active_query_concurrency,
                        queries,
                        scenario,
                    )?
                };
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
                write_markdown_export(markdown_out, report.to_markdown())?;
            }
            BenchCommand::ServerCert {
                profile,
                clients,
                active_query_concurrency,
                queries,
                path,
                json_out,
                csv_out,
                markdown_out,
            } => {
                let clients = clients.unwrap_or({
                    if matches!(profile.as_str(), "full" | "manual" | "nightly" | "1000") {
                        1000
                    } else {
                        8
                    }
                });
                let active_query_concurrency = active_query_concurrency.unwrap_or_else(|| {
                    if matches!(profile.as_str(), "full" | "manual" | "nightly" | "1000") {
                        32
                    } else {
                        clients.min(4)
                    }
                });
                let path = path.unwrap_or_else(|| default_bench_path("server-cert"));
                let report = run_server_certification(
                    path,
                    profile,
                    clients,
                    active_query_concurrency,
                    queries,
                )?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
                write_markdown_export(markdown_out, report.to_markdown())?;
                if !report.passed {
                    return Err(anyhow::anyhow!("server certification failed"));
                }
            }
            #[cfg(feature = "bench-comparison-engines")]
            BenchCommand::Compare {
                records,
                batch_size,
                path,
                json_out,
                csv_out,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("compare"));
                let reports = run_insert_baseline_suite(path, records, batch_size)?;
                print_baseline_reports(&reports);
                write_exports(
                    json_out,
                    csv_out,
                    BaselineReport::suite_to_json(&reports)?,
                    BaselineReport::suite_to_csv(&reports),
                )?;
            }
        },
        #[cfg(feature = "bench")]
        Command::Compat { command } => match command {
            CompatCommand::Test {
                path,
                target_version,
                json_out,
                csv_out,
                markdown_out,
                fail_under,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("postgres-compat"));
                let report = run_postgres_compat_suite(path, target_version)?;
                print!("{report}");
                write_exports(json_out, csv_out, report.to_json()?, report.to_csv())?;
                write_markdown_export(markdown_out, report.to_markdown())?;
                if let Some(threshold) = fail_under {
                    if report.score_percent < threshold {
                        return Err(anyhow::anyhow!(
                            "PostgreSQL compatibility score {:.1}% is below --fail-under {:.1}%",
                            report.score_percent,
                            threshold
                        ));
                    }
                }
            }
            CompatCommand::Diff {
                path,
                target_version,
                fixtures,
                json_out,
                markdown_out,
                pg_host,
                pg_port,
                pg_database,
                pg_user,
                pg_password,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("postgres-diff"));
                let defaults = PostgresDiffConfig::default();
                let config = PostgresDiffConfig {
                    target_version,
                    pg_host: pg_host.unwrap_or(defaults.pg_host),
                    pg_port: pg_port.unwrap_or(defaults.pg_port),
                    pg_database: pg_database.unwrap_or(defaults.pg_database),
                    pg_user: pg_user.unwrap_or(defaults.pg_user),
                    pg_password: pg_password.unwrap_or(defaults.pg_password),
                    fixtures_dir: fixtures,
                };
                let report = run_postgres_diff_suite(path, config)?;
                print!("{report}");
                write_json_markdown_exports(
                    json_out,
                    markdown_out,
                    report.to_json()?,
                    report.to_markdown(),
                )?;
                if report.failed_cases > 0 {
                    return Err(anyhow::anyhow!(
                        "PostgreSQL differential compatibility failed {}/{} cases",
                        report.failed_cases,
                        report.total_cases
                    ));
                }
            }
            CompatCommand::Nightmare {
                path,
                target_version,
                fixtures,
                json_out,
                markdown_out,
                repro_dir,
                pg_host,
                pg_port,
                pg_database,
                pg_user,
                pg_password,
            } => {
                let path = path.unwrap_or_else(|| default_bench_path("pg18-nightmare"));
                let defaults = PostgresDiffConfig::default();
                let config = PostgresDiffConfig {
                    target_version,
                    pg_host: pg_host.unwrap_or(defaults.pg_host),
                    pg_port: pg_port.unwrap_or(defaults.pg_port),
                    pg_database: pg_database.unwrap_or(defaults.pg_database),
                    pg_user: pg_user.unwrap_or(defaults.pg_user),
                    pg_password: pg_password.unwrap_or(defaults.pg_password),
                    fixtures_dir: Some(fixtures),
                };
                let report = run_pg18_nightmare_suite(&path, config)?;
                let repro_dir = repro_dir.unwrap_or_else(|| path.join("repros"));
                report.write_repro_files(&repro_dir)?;
                print!("{report}");
                write_json_markdown_exports(
                    json_out,
                    markdown_out,
                    report.to_json()?,
                    report.to_markdown(),
                )?;
                if report.failed_cases > 0 {
                    return Err(anyhow::anyhow!(
                        "pg18-nightmare found {}/{} non-intentional PostgreSQL differences; repros in {}",
                        report.failed_cases,
                        report.total_cases,
                        repro_dir.display()
                    ));
                }
            }
        },
        Command::Sync { command } => match command {
            SyncCommand::Export {
                path,
                out,
                since,
                db_key,
                db_key_env,
                bundle_key,
                bundle_key_env,
            } => {
                let mut db = open_db_with_key_args(&path, db_key, db_key_env)?;
                let bundle_encryption =
                    optional_passphrase_config(bundle_key, bundle_key_env, None, "sync bundle")?;
                let report = if let Some(bundle_encryption) = bundle_encryption {
                    db.sync().export_encrypted_to_path(
                        SyncCheckpoint::new(since),
                        &out,
                        bundle_encryption,
                    )?
                } else {
                    db.sync().export_to_path(SyncCheckpoint::new(since), &out)?
                };
                println!("Exported BicDB sync bundle");
                println!("Bundle: {}", report.bundle_id);
                println!("Source node: {}", report.source_node_id);
                println!("Events: {}", report.event_count);
                println!(
                    "Checkpoint: {} -> {}",
                    report.from_checkpoint.event_offset, report.next_checkpoint.event_offset
                );
                println!("Path: {}", report.path.display());
            }
            SyncCommand::Import {
                path,
                bundle,
                db_key,
                db_key_env,
                bundle_key,
                bundle_key_env,
            } => {
                let mut db = open_db_with_key_args(&path, db_key, db_key_env)?;
                let report = db.import_sync_bundle_file_auto(
                    &bundle,
                    optional_passphrase_config(bundle_key, bundle_key_env, None, "sync bundle")?,
                )?;
                println!("Imported BicDB sync bundle");
                println!("Bundle: {}", report.bundle_id);
                println!("Source node: {}", report.source_node_id);
                println!("Imported events: {}", report.imported_events);
                println!("Duplicate events: {}", report.duplicate_events);
                println!("Records merged: {}", report.records_merged);
                println!("Conflicts resolved: {}", report.conflicts_resolved);
                println!("Last sync: {}", report.last_sync_at);
            }
        },
        Command::Backup { command } => match command {
            BackupCommand::PruneArchive { archive, base } => {
                let report = bicdb_core::prune_archived_wal(&archive, &base)?;
                println!(
                    "Pruned {} of {} archived segments below floor {} ({} bytes freed, {} kept)",
                    report.pruned,
                    report.examined,
                    report.keep_from_sequence,
                    report.bytes_freed,
                    report.kept
                );
            }
            BackupCommand::ApplyWal { target, archive } => {
                let applied = bicdb_core::apply_archived_wal(&target, &archive)?;
                println!(
                    "Applied {applied} archived WAL segment(s) to {}",
                    target.display()
                );
                println!("The roll-forward replays on the next open.");
            }
            BackupCommand::Create {
                path,
                out,
                base,
                key,
                key_env,
                db_key,
                db_key_env,
            } => {
                let db = open_db_with_key_args(&path, db_key, db_key_env)?;
                db.flush()?;
                drop(db);
                let report = create_backup(
                    &path,
                    &out,
                    BackupCreateOptions {
                        passphrase: backup_key(key, key_env)?,
                        base_backup: base,
                    },
                )?;
                println!("Created encrypted BicDB backup");
                println!("Backup: {}", report.backup_id);
                println!("Full: {}", report.full);
                println!("Files: {}/{}", report.files_included, report.files_total);
                println!("Manifest: {}", report.manifest_hash);
                println!("Encrypted bytes: {}", report.encrypted_bytes);
                println!("Path: {}", report.path.display());
            }
            BackupCommand::Online {
                path,
                base_out,
                wal_tail_out,
                key,
                key_env,
                db_key,
                db_key_env,
            } => {
                // Online backup is paged-only; open in server_paged mode
                // outright (a non-paged database fails with the storage-mode
                // compatibility error, which is the right message).
                let paged_config = DbConfig::default().with_storage_mode(StorageMode::ServerPaged);
                let db = match db_encryption_for_path(&path, db_key, db_key_env)? {
                    Some(encryption) => {
                        BicDb::open_with_encryption(&path, paged_config, encryption)?
                    }
                    None => BicDb::open_with_config(&path, paged_config)?,
                };
                let report = db.create_online_backup(
                    &base_out,
                    &wal_tail_out,
                    BackupCreateOptions {
                        passphrase: backup_key(key, key_env)?,
                        base_backup: None,
                    },
                )?;
                drop(db);
                println!("Created online BicDB backup chain");
                println!("Base backup: {}", report.base.backup_id);
                println!(
                    "Base files: {}/{} ({} bytes encrypted)",
                    report.base.files_included,
                    report.base.files_total,
                    report.base.encrypted_bytes
                );
                println!("WAL tail backup: {}", report.wal_tail.backup_id);
                println!(
                    "WAL tail files: {} ({} bytes encrypted)",
                    report.wal_tail.files_included, report.wal_tail.encrypted_bytes
                );
                println!("Base path: {}", report.base.path.display());
                println!("WAL tail path: {}", report.wal_tail.path.display());
                println!(
                    "Restore with: bicdb backup restore {} {} <target>",
                    report.base.path.display(),
                    report.wal_tail.path.display()
                );
            }
            BackupCommand::Verify {
                backup,
                key,
                key_env,
                json,
            } => {
                if backup.is_empty() {
                    bail!("provide at least one backup path to verify");
                }
                let passphrase = backup_key(key, key_env)?;
                if backup.len() == 1 {
                    let report = verify_backup(&backup[0], &passphrase)?;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    } else {
                        println!("Verified encrypted BicDB backup");
                        println!("Backup: {}", report.backup_id);
                        println!("Full: {}", report.full);
                        println!("Files: {}/{}", report.files_included, report.files_total);
                        println!("Manifest: {}", report.manifest_hash);
                        println!("Archived events: {}", report.archived_events);
                    }
                } else {
                    let report = verify_backup_chain(&backup, &passphrase)?;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    } else {
                        println!("Verified encrypted BicDB backup chain");
                        println!("Backups: {}", report.backups_verified);
                        println!("Base backup: {}", report.base_backup_id);
                        println!("Final backup: {}", report.final_backup_id);
                        println!("Files restored by chain: {}", report.files_included);
                        println!("Final manifest: {}", report.final_manifest_hash);
                        println!("Archived events: {}", report.archived_events);
                    }
                }
            }
            BackupCommand::Restore {
                backup,
                target,
                force,
                target_timestamp,
                pitr_record_batch,
                pitr_event_batch,
                pitr_event_bytes,
                key,
                key_env,
            } => {
                let passphrase = backup_key(key, key_env)?;
                if backup.len() > 1 {
                    if target_timestamp.is_some() {
                        bail!(
                            "point-in-time restore takes a single archive; restore the chain \
                             first, then run the PITR restore against the drilled copy"
                        );
                    }
                    // Chain: verify end-to-end before touching the target,
                    // then apply in order. `force` applies to the full base;
                    // incrementals land on the restored tree.
                    verify_backup_chain(&backup, &passphrase)?;
                }
                let report = if target_timestamp.is_some() {
                    restore_backup_to_point_with_limits(
                        &backup[0],
                        &target,
                        BackupPointInTimeRestoreOptions {
                            passphrase,
                            force,
                            target_timestamp,
                        },
                        BackupPitrReplayLimits {
                            max_records_per_batch: pitr_record_batch,
                            max_events_per_batch: pitr_event_batch,
                            max_event_bytes_per_batch: pitr_event_bytes,
                        },
                    )?
                } else {
                    let mut report = restore_backup(
                        &backup[0],
                        &target,
                        BackupRestoreOptions {
                            passphrase: passphrase.clone(),
                            force,
                        },
                    )?;
                    for incremental in &backup[1..] {
                        report = restore_backup(
                            incremental,
                            &target,
                            BackupRestoreOptions {
                                passphrase: passphrase.clone(),
                                force: false,
                            },
                        )?;
                    }
                    report
                };
                println!("Restored encrypted BicDB backup");
                println!("Backup: {}", report.backup_id);
                println!("Files restored: {}", report.files_restored);
                println!("Manifest: {}", report.manifest_hash);
                if let Some(target_timestamp) = report.target_timestamp {
                    println!("Target timestamp: {target_timestamp}");
                    println!(
                        "Restored records: {}",
                        report.restored_records.unwrap_or_default()
                    );
                }
                println!("Target: {}", report.target_path.display());
            }
            BackupCommand::Drill {
                backup,
                target,
                target_timestamp,
                pitr_record_batch,
                pitr_event_batch,
                pitr_event_bytes,
                json_out,
                key,
                key_env,
            } => {
                let target = target.unwrap_or_else(default_backup_drill_target);
                let report = drill_backup_restore_with_limits(
                    &backup,
                    &target,
                    BackupPointInTimeRestoreOptions {
                        passphrase: backup_key(key, key_env)?,
                        force: true,
                        target_timestamp,
                    },
                    BackupPitrReplayLimits {
                        max_records_per_batch: pitr_record_batch,
                        max_events_per_batch: pitr_event_batch,
                        max_event_bytes_per_batch: pitr_event_bytes,
                    },
                )?;
                let json_report = serde_json::to_string_pretty(&report)?;
                if let Some(path) = json_out {
                    std::fs::write(&path, json_report.as_bytes())?;
                    println!("Backup drill report: {}", path.display());
                } else {
                    println!("{json_report}");
                }
            }
        },
        Command::Migrate { command } => match command {
            MigrateCommand::DryRun {
                path,
                dir,
                lock_timeout_ms,
                json,
                key,
                key_env,
            } => {
                let mut db = open_db_with_key_args(&path, key, key_env)?;
                let report = migrate_dry_run(&mut db, &dir, lock_timeout_ms)?;
                print_migration_report(&report, json)?;
                if !report.safe {
                    bail!("migration dry-run found unsupported operations");
                }
            }
            MigrateCommand::Apply {
                path,
                dir,
                lock_timeout_ms,
                json,
                key,
                key_env,
            } => {
                let mut db = open_db_with_key_args(&path, key, key_env)?;
                let report = migrate_apply(&mut db, &dir, lock_timeout_ms)?;
                print_migration_report(&report, json)?;
                if !report.safe {
                    bail!("migration apply found unsupported operations");
                }
            }
            MigrateCommand::Status {
                path,
                json,
                key,
                key_env,
            } => {
                let db = open_db_with_key_args(&path, key, key_env)?;
                let status = migration_status(&db)?;
                print_migration_status(&status, json)?;
            }
            MigrateCommand::Rollback {
                path,
                down,
                lock_timeout_ms,
                json,
                key,
                key_env,
            } => {
                let mut db = open_db_with_key_args(&path, key, key_env)?;
                let report = migrate_rollback(&mut db, &down, lock_timeout_ms)?;
                print_migration_report(&report, json)?;
                if !report.safe {
                    bail!("migration rollback found unsupported operations");
                }
            }
            MigrateCommand::Repair {
                path,
                json,
                key,
                key_env,
            } => {
                let mut db = open_db_with_key_args(&path, key, key_env)?;
                let status = migration_repair(&mut db)?;
                print_migration_status(&status, json)?;
            }
        },
        Command::Analytics {
            target,
            query,
            collection,
            table,
            json,
            csv,
        } => {
            run_analytics_command(
                target,
                query,
                collection,
                OutputMode::new(json, csv)?,
                table,
            )?;
        }
        Command::Sql {
            path,
            query,
            json,
            csv,
            slow_query_log,
            slow_query_threshold_ms,
            redact_query_text,
            redact_bind_parameters,
            redact_fields,
        } => {
            run_sql_command(
                path,
                query,
                OutputMode::new(json, csv)?,
                SlowQueryOptions {
                    log_path: slow_query_log,
                    threshold_ms: slow_query_threshold_ms,
                    redaction: redaction_config(
                        redact_query_text,
                        redact_bind_parameters,
                        redact_fields,
                    ),
                },
            )?;
        }
        Command::Metrics {
            path,
            prometheus,
            json,
            key,
            key_env,
        } => {
            if prometheus && json {
                bail!("--prometheus and --json are mutually exclusive");
            }
            let db = open_db_with_key_args(&path, key, key_env)?;
            let metrics = OperationalMetrics::from_db(&db)?;
            if prometheus {
                print!("{}", metrics.to_prometheus());
            } else if json {
                println!("{}", serde_json::to_string_pretty(&metrics)?);
            } else {
                print_operational_metrics(&metrics);
            }
        }
        Command::Health { command } => match command {
            HealthCommand::Liveness { path, json } => {
                let report = HealthReport::liveness(path);
                print_health_report(&report, json)?;
                if !report.success() {
                    bail!("bicdb liveness check failed");
                }
            }
            HealthCommand::Readiness {
                path,
                max_size_bytes,
                json,
                key,
                key_env,
            }
            | HealthCommand::Status {
                path,
                max_size_bytes,
                json,
                key,
                key_env,
            } => {
                let db = open_db_with_key_args(&path, key, key_env)?;
                let report = HealthReport::readiness(&db, max_size_bytes)?;
                print_health_report(&report, json)?;
                if !report.success() {
                    bail!("bicdb readiness check failed");
                }
            }
        },
        Command::Doctor {
            path,
            json,
            out,
            key,
            key_env,
        } => {
            let db = open_db_with_key_args(&path, key, key_env)?;
            let report = DoctorReport::collect(&db)?;
            let bundle = doctor_bundle_json(&report)?;
            if let Some(out) = out {
                std::fs::write(&out, serde_json::to_vec_pretty(&bundle)?)?;
                println!("Wrote sanitized BicDB doctor bundle to {}", out.display());
            } else if json {
                println!("{}", serde_json::to_string_pretty(&bundle)?);
            } else {
                print_doctor_report(&report);
            }
        }
        Command::SyncServe {
            root,
            host,
            port,
            token,
            admin_token,
            fsync,
            cors_origin,
            retention,
            retention_interval_seconds,
            rls_compose,
            event_horizon,
            horizon_max_checkpoint_age_seconds,
            server_write_only,
        } => {
            let token = token.or_else(|| std::env::var("BICDB_SYNC_TOKEN").ok());
            let admin_token = admin_token.or_else(|| std::env::var("BICDB_SYNC_ADMIN_TOKEN").ok());
            let retention = match retention {
                Some(path) => {
                    let bytes = std::fs::read(&path)?;
                    Some(serde_json::from_slice::<sync_server::RetentionConfig>(
                        &bytes,
                    )?)
                }
                None => None,
            };
            let rls_compose = match rls_compose {
                Some(path) => {
                    let bytes = std::fs::read(&path)?;
                    Some(serde_json::from_slice::<sync_server::RlsComposeConfig>(
                        &bytes,
                    )?)
                }
                None => None,
            };
            sync_server::serve_blocking(sync_server::SyncServeConfig {
                root,
                host,
                port,
                token,
                admin_token,
                fsync,
                cors_origin,
                retention,
                retention_interval_seconds,
                rls_compose,
                event_horizon,
                horizon_max_checkpoint_age_seconds,
                server_write_only,
            })?;
        }
        Command::CacheServe {
            path,
            host,
            port,
            requirepass,
            fsync,
            max_keys,
            eviction,
            max_connections,
            hotview,
            ephemeral,
        } => {
            let password = requirepass.or_else(|| std::env::var("BICDB_CACHE_PASSWORD").ok());
            let auth = if password.is_some() { "on" } else { "off" };
            let hotview_state = if hotview { "on" } else { "off" };
            let durability = if ephemeral { "ephemeral" } else { "durable" };
            println!(
                "bicdb cache server (RESP) listening on {host}:{port} \
                 (auth {auth}, fsync {fsync}, hotview {hotview_state}, entries {durability})"
            );
            bicdb_resp::serve(
                &path,
                bicdb_resp::RespConfig {
                    host,
                    port,
                    password,
                    fsync,
                    max_keys,
                    eviction: eviction.into(),
                    max_connections,
                    hotview,
                    ephemeral,
                },
            )?;
        }
        Command::Serve {
            path,
            cluster,
            default_database,
            host,
            port,
            postgres_server_version,
            postgres_server_version_num,
            postgres_version_banner,
            require_auth,
            auth_method,
            channel_binding,
            operator,
            allow_remote_no_auth,
            max_connections,
            max_connections_per_ip,
            max_pending_accepts,
            max_active_queries,
            max_queued_queries,
            max_active_reads,
            max_queued_reads,
            max_active_writes,
            max_queued_writes,
            idle_timeout_seconds,
            shutdown_grace_seconds,
            query_timeout_ms,
            overload_timeout_ms,
            write_timeout_ms,
            max_result_rows,
            max_request_bytes,
            per_connection_memory_limit,
            paged_rowid_registry,
            tls_cert,
            tls_key,
            require_tls,
            tls_client_ca,
            slow_query_log,
            slow_query_threshold_ms,
            redact_query_text,
            redact_fields,
            storage_sync,
        }
        | Command::ServePg {
            path,
            cluster,
            default_database,
            host,
            port,
            postgres_server_version,
            postgres_server_version_num,
            postgres_version_banner,
            require_auth,
            auth_method,
            channel_binding,
            operator,
            allow_remote_no_auth,
            max_connections,
            max_connections_per_ip,
            max_pending_accepts,
            max_active_queries,
            max_queued_queries,
            max_active_reads,
            max_queued_reads,
            max_active_writes,
            max_queued_writes,
            idle_timeout_seconds,
            shutdown_grace_seconds,
            query_timeout_ms,
            overload_timeout_ms,
            write_timeout_ms,
            max_result_rows,
            max_request_bytes,
            per_connection_memory_limit,
            paged_rowid_registry,
            tls_cert,
            tls_key,
            require_tls,
            tls_client_ca,
            slow_query_log,
            slow_query_threshold_ms,
            redact_query_text,
            redact_fields,
            storage_sync,
        } => {
            let config = pgwire_config_from_cli(
                host,
                port,
                postgres_server_version,
                postgres_server_version_num,
                postgres_version_banner,
                require_auth,
                auth_method,
                channel_binding,
                allow_remote_no_auth,
                max_connections,
                max_connections_per_ip,
                max_pending_accepts,
                max_active_queries,
                max_queued_queries,
                max_active_reads,
                max_queued_reads,
                max_active_writes,
                max_queued_writes,
                idle_timeout_seconds,
                shutdown_grace_seconds,
                query_timeout_ms,
                overload_timeout_ms,
                write_timeout_ms,
                max_result_rows,
                max_request_bytes,
                per_connection_memory_limit,
                paged_rowid_registry,
                tls_cert,
                tls_key,
                require_tls,
                tls_client_ca,
                slow_query_log,
                slow_query_threshold_ms,
                redact_query_text,
                redact_fields,
                storage_sync,
            )?;
            let host_services = cli_host_services(&operator)?;
            if cluster {
                serve_cluster_with_host_services(path, default_database, config, host_services)?;
            } else {
                run_serve_command(path, config, host_services)?;
            }
        }
        Command::Index { command } => match command {
            IndexCommand::Verify {
                path,
                name,
                all,
                json,
                csv,
            } => match name {
                Some(name) => {
                    if all {
                        bail!("pass either an index name or --all, not both");
                    }
                    let db = BicDb::open(&path)?;
                    let report = db.verify_index(&name)?;
                    match OutputMode::new(json, csv)? {
                        OutputMode::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                        OutputMode::Csv => {
                            println!(
                                "index_name,collection,kind,valid,indexed_records,\
                                 expected_records,missing_entries,stale_entries,wrong_entries"
                            );
                            println!(
                                "{},{},{:?},{},{},{},{},{},{}",
                                report.index_name,
                                report.collection,
                                report.kind,
                                report.valid,
                                report.indexed_records,
                                report.expected_records,
                                report.missing_entries,
                                report.stale_entries,
                                report.wrong_entries
                            );
                        }
                        OutputMode::Table => {
                            println!(
                                "verify `{}` on `{}`: valid={} indexed={} expected={} \
                                 missing={} stale={} wrong={}",
                                report.index_name,
                                report.collection,
                                report.valid,
                                report.indexed_records,
                                report.expected_records,
                                report.missing_entries,
                                report.stale_entries,
                                report.wrong_entries
                            );
                        }
                    }
                    if !report.valid {
                        bail!("index `{name}` is stale or corrupt");
                    }
                }
                None => {
                    if !all {
                        bail!("bicdb index verify requires an index name or --all");
                    }
                    let mut db = BicDb::open(&path)?;
                    let report = db.verify_all_indexes()?;
                    print_index_all_report(&report, OutputMode::new(json, csv)?)?;
                    if !report.valid {
                        bail!("one or more indexes are stale or corrupt");
                    }
                }
            },
            IndexCommand::Rebuild {
                path,
                all,
                json,
                csv,
            } => {
                if !all {
                    bail!("bicdb index rebuild currently requires --all");
                }
                let mut db = BicDb::open(&path)?;
                let report = db.rebuild_all_indexes_online()?;
                print_index_all_report(&report, OutputMode::new(json, csv)?)?;
                if !report.valid {
                    bail!("one or more indexes failed verification after rebuild");
                }
            }
            IndexCommand::FtsStatus {
                path,
                name,
                all,
                json,
            } => {
                if name.is_some() == all {
                    bail!("pass either an FTS index name or --all");
                }
                if !path.join("format.json").is_file() {
                    bail!(
                        "no BicDB database at `{}` (format.json missing)",
                        path.display()
                    );
                }
                let db = BicDb::open_with_config(
                    &path,
                    DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
                )?;
                let statuses = match name {
                    Some(name) => vec![db.full_text_build_lifecycle(&name)?],
                    None => db.full_text_build_lifecycles()?,
                };
                if json {
                    println!("{}", serde_json::to_string_pretty(&statuses)?);
                } else if statuses.is_empty() {
                    println!("No full-text indexes or build workspaces found");
                } else {
                    for status in statuses {
                        let documents = status
                            .progress
                            .as_ref()
                            .map(|progress| progress.documents_tokenized)
                            .or_else(|| {
                                status
                                    .published
                                    .as_ref()
                                    .map(|generation| generation.document_count)
                            })
                            .unwrap_or(0);
                        println!(
                            "{}: {:?} reason={} action={:?} serving={} documents={}",
                            status.logical_index,
                            status.state,
                            status.reason_code,
                            status.recommended_action,
                            status.serving,
                            documents
                        );
                    }
                }
            }
            IndexCommand::FtsReconcile {
                path,
                name,
                budget_documents,
                json,
            } => {
                if !path.join("format.json").is_file() {
                    bail!(
                        "no BicDB database at `{}` (format.json missing)",
                        path.display()
                    );
                }
                let mut db = BicDb::open_with_config(
                    &path,
                    DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
                )?;
                let report = db.reconcile_full_text_build(&name, budget_documents.max(1))?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "{}: {:?}; {} documents advanced; state {:?} -> {:?}; reason={}",
                        name,
                        report.outcome,
                        report.documents_indexed,
                        report.before.state,
                        report.after.state,
                        report.after.reason_code
                    );
                }
            }
            IndexCommand::Rekey { path, name, json } => {
                if !path.join("format.json").is_file() {
                    bail!(
                        "no BicDB database at `{}` (format.json missing)",
                        path.display()
                    );
                }
                let mut db = BicDb::open_with_config(
                    &path,
                    DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
                )?;
                let rewritten = db.rekey_index(&name)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "index": name,
                            "entries_rewritten": rewritten,
                            "entry_format": "v3",
                        }))?
                    );
                } else {
                    println!("Rekeyed `{name}` to v3 entries");
                    println!("Entries rewritten this run: {rewritten}");
                }
            }
            IndexCommand::Pack {
                path,
                name,
                strategy,
                create,
                json,
                csv,
            } => {
                let strategy = match strategy.to_ascii_lowercase().as_str() {
                    "hilbert" => SpatialPackStrategy::Hilbert,
                    "str" => SpatialPackStrategy::Str,
                    other => bail!("unknown pack strategy `{other}` (expected hilbert or str)"),
                };
                // Refuse before opening: BicDb::open CREATES a database at a
                // missing path, so a typo'd path would leave a fresh empty
                // paged database behind and then fail with "index not found".
                if !path.join("format.json").is_file() {
                    bail!(
                        "no BicDB database at `{}` (format.json missing)",
                        path.display()
                    );
                }
                if let Some(collection) = &create {
                    let derived = format!("idx_{collection}_geometry_spatial");
                    if name != derived {
                        bail!(
                            "--create {collection} derives index name `{derived}`, \
                             which does not match `{name}`"
                        );
                    }
                }
                // Packing only exists on the paged engine, so request that
                // mode outright: the open fence adopts an existing paged
                // database regardless of BICDB_STORAGE_MODE, and refuses an
                // embedded one with a message naming the mode mismatch.
                let mut db = BicDb::open_with_config(
                    &path,
                    DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
                )?;
                let index_exists = db
                    .index_definitions()
                    .iter()
                    .any(|definition| definition.name == name);
                let report = match (&create, index_exists) {
                    (Some(collection), false) => {
                        db.create_packed_spatial_index(collection, "geometry", strategy)?
                    }
                    _ => db.pack_spatial_index_with_strategy(&name, strategy)?,
                };
                print_spatial_pack_report(&report, OutputMode::new(json, csv)?)?;
            }
        },
        Command::Vector { command } => match command {
            VectorCommand::Index { command } => match command {
                VectorIndexCommand::Build {
                    path,
                    collection,
                    m,
                    ef_construction,
                    ef_search,
                    metric,
                } => {
                    let mut db = BicDb::open(&path)?;
                    let config = HnswIndexConfig {
                        m,
                        ef_construction,
                        ef_search,
                        distance: parse_vector_metric(&metric)?,
                    };
                    let report = if db.has_vector_index(&collection) {
                        db.rebuild_vector_index(&collection)?
                    } else {
                        db.create_vector_index(&collection, config)?
                    };
                    print_vector_index_report("Built", &report);
                }
                VectorIndexCommand::Verify { path, collection } => {
                    let db = BicDb::open(&path)?;
                    let report = db.verify_vector_index(&collection)?;
                    print_vector_index_report("Verified", &report);
                    if !report.valid {
                        return Err(anyhow::anyhow!(
                            "vector index for collection `{}` is not valid",
                            report.collection
                        ));
                    }
                }
                VectorIndexCommand::Rebuild { path, collection } => {
                    let db = BicDb::open(&path)?;
                    let report = db.rebuild_vector_index(&collection)?;
                    print_vector_index_report("Rebuilt", &report);
                }
            },
        },
        Command::Model { command } => match command {
            ModelCommand::Enable {
                path,
                model,
                source,
                dimension,
                json,
                no_progress,
            } => {
                let source = source.unwrap_or_else(|| default_embedding_model_source(&model));
                if !json && !no_progress {
                    print_model_enable_animation(&model, &source);
                }
                let mut db = BicDb::open(&path)?;
                let entry = db.register_local_onnx_embedding_model(&model, &source, dimension)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&entry)?);
                } else {
                    println!("Enabled model `{}`", entry.name);
                    println!("Runtime: {:?}", entry.runtime);
                    println!("Dimension: {}", entry.dimension);
                    if let Some(model_dir) = &entry.model_dir {
                        println!("Model dir: {}", model_dir.display());
                    }
                    for (file, checksum) in &entry.checksums {
                        println!("{checksum}  {file}");
                    }
                }
            }
            ModelCommand::Status { path, json } => {
                let db = BicDb::open(&path)?;
                let models = db.embedding_models();
                if json {
                    println!("{}", serde_json::to_string_pretty(&models)?);
                } else if models.is_empty() {
                    println!("No embedding models enabled");
                } else {
                    for model in models {
                        print_model_registry_entry(&model);
                    }
                }
            }
        },
        Command::Memory { command } => match command {
            MemoryCommand::Process { path, limit, json } => {
                let db = BicDb::open(&path)?;
                let report = db.process_memory_index_jobs(limit)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "Processed memory jobs: processed={}, failed={}, pending={}",
                        report.processed, report.failed, report.pending
                    );
                }
            }
            MemoryCommand::Search {
                path,
                table,
                field,
                query,
                top_k,
                json,
            } => {
                let db = BicDb::open(&path)?;
                let hits = db.search_memory_index(&table, &field, &query, top_k)?;
                if json {
                    let rows = hits
                        .into_iter()
                        .map(|hit| {
                            json!({
                                "id": hit.record.id,
                                "score": hit.score,
                                "metadata": hit.record.metadata,
                            })
                        })
                        .collect::<Vec<_>>();
                    println!("{}", serde_json::to_string_pretty(&rows)?);
                } else {
                    for hit in hits {
                        println!("{}\t{:.6}", hit.record.id, hit.score);
                    }
                }
            }
        },
        Command::Spatial { command } => match command {
            SpatialCommand::ImportOsm {
                file,
                bbox,
                path,
                graph,
                json,
                csv,
            } => {
                let mut db = BicDb::open(&path)?;
                let bbox = parse_bbox(&bbox)?;
                let report = db.import_osm_pbf_as(&graph, &file, bbox)?;
                print_osm_import_report(&report, OutputMode::new(json, csv)?)?;
            }
            SpatialCommand::Route {
                path,
                from,
                to,
                graph,
                json,
                csv,
            } => {
                let db = BicDb::open(&path)?;
                let (from_lon, from_lat) = parse_lon_lat(&from)?;
                let (to_lon, to_lat) = parse_lon_lat(&to)?;
                let route = db.shortest_path(
                    &graph,
                    &Geometry::point(from_lon, from_lat)?,
                    &Geometry::point(to_lon, to_lat)?,
                )?;
                print_route_path(&route, OutputMode::new(json, csv)?)?;
            }
            SpatialCommand::Nearest {
                path,
                collection,
                point,
                limit,
                field,
                json,
                csv,
            } => {
                let db = BicDb::open(&path)?;
                let (lon, lat) = parse_lon_lat(&point)?;
                let results = db.nearest(&collection, &field, lon, lat, limit)?;
                print_spatial_results(&results, OutputMode::new(json, csv)?)?;
            }
            SpatialCommand::WithinRadius {
                path,
                collection,
                point,
                meters,
                field,
                json,
                csv,
            } => {
                let db = BicDb::open(&path)?;
                let (lon, lat) = parse_lon_lat(&point)?;
                let results = db.within_radius(&collection, &field, lon, lat, meters)?;
                print_spatial_results(&results, OutputMode::new(json, csv)?)?;
            }
        },
        Command::Graph { command } => match command {
            GraphCommand::Build { path, projection } => {
                let mut db = BicDb::open(&path)?;
                let graph = db.build_graph_projection(graph_projection_by_name(&projection)?)?;
                print_graph_build_report("Built", &graph);
            }
            GraphCommand::Rebuild { path, projection } => {
                let mut db = BicDb::open(&path)?;
                let graph = db.rebuild_graph_projection(graph_projection_by_name(&projection)?)?;
                print_graph_build_report("Rebuilt", &graph);
            }
            GraphCommand::Verify { path, projection } => {
                let db = BicDb::open(&path)?;
                let report = db.verify_graph_projection(&graph_projection_by_name(&projection)?)?;
                print_graph_verify_report(&report);
                if !report.valid {
                    return Err(anyhow::anyhow!(
                        "graph projection `{}` is not valid",
                        report.projection
                    ));
                }
            }
            GraphCommand::Query {
                path,
                query,
                projection,
            } => {
                let db = BicDb::open(&path)?;
                let definition = graph_projection_by_name(&projection)?;
                let graph = db.graph_projection(&definition.name)?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "graph projection `{}` has not been built; run `bicdb graph build {} --projection {}` first",
                        definition.name,
                        path.display()
                        ,projection.display()
                    )
                })?;
                run_graph_query(&graph, &query)?;
            }
        },
        Command::User { command } => match command {
            UserCommand::Create {
                username,
                password,
                password_env,
                path,
                user_id,
                tenant,
                workspace,
                client_id,
                roles,
                scopes,
            } => {
                let password = match (password, password_env) {
                    (Some(password), None) => password,
                    (None, Some(name)) => std::env::var(&name).map_err(|_| {
                        anyhow::anyhow!("password environment variable is missing or not Unicode")
                    })?,
                    _ => bail!("exactly one password source is required"),
                };
                let mut identity =
                    PgWireUserIdentity::new(user_id.unwrap_or_else(|| username.clone()), tenant)
                        .with_roles(roles)
                        .with_scopes(scopes);
                if let Some(workspace) = workspace {
                    identity = identity.with_workspace_id(workspace);
                }
                if let Some(client_id) = client_id {
                    identity = identity.with_client_id(client_id);
                }
                create_user_with_identity(&path, &username, &password, identity)?;
                println!(
                    "Created BicDB server user `{username}` in {}",
                    path.display()
                );
            }
            UserCommand::Delegate {
                username,
                path,
                signing_key_env,
                tenants,
                revoke,
            } => {
                let policy = if revoke {
                    None
                } else {
                    let variable =
                        signing_key_env.context("delegation key environment variable required")?;
                    let signing_key = std::env::var(variable)
                        .context("delegation key environment variable unavailable")?;
                    Some(bicdb_pgwire::DelegationPolicy {
                        signing_key,
                        tenants,
                    })
                };
                bicdb_pgwire::set_user_delegation_policy(&path, &username, policy)?;
                println!("Updated delegation policy for `{username}`");
            }
            UserCommand::BindIdentity {
                username,
                path,
                user_id,
                tenant,
                workspace,
                client_id,
                roles,
                scopes,
            } => {
                let mut identity =
                    PgWireUserIdentity::new(user_id.unwrap_or_else(|| username.clone()), tenant)
                        .with_roles(roles)
                        .with_scopes(scopes);
                if let Some(workspace) = workspace {
                    identity = identity.with_workspace_id(workspace);
                }
                if let Some(client_id) = client_id {
                    identity = identity.with_client_id(client_id);
                }
                set_user_security_identity(&path, &username, identity)?;
                println!(
                    "Bound trusted identity for BicDB server user `{username}` in {}",
                    path.display()
                );
            }
        },
        Command::Server { command } => match command {
            ServerCommand::Status { path } => {
                print_server_status(path)?;
            }
        },
        Command::Cluster { command } => run_cluster_command(command)?,
        Command::Ha { command } => match command {
            HaCommand::Status { path, json } => {
                let db = BicDb::open(&path)?;
                print_ha_status(&db.ha_status()?, json)?;
            }
            HaCommand::Ship {
                primary,
                standby,
                json,
            } => {
                let report = BicDb::configure_standby_from(&standby, &primary)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!("Shipped BicDB standby");
                    println!("Primary: {}", report.source_path.display());
                    println!("Standby: {}", report.standby_path.display());
                    println!("Files copied: {}", report.files_copied);
                    println!(
                        "Checkpoint bytes: {} -> {}",
                        report.source_checkpoint_bytes, report.applied_checkpoint_bytes
                    );
                }
            }
            HaCommand::Promote {
                standby,
                force,
                json,
            } => {
                let status = BicDb::promote_standby(&standby, force)?;
                print_ha_status(&status, json)?;
            }
        },
        Command::Replication { command } => match command {
            ReplicationCommand::Status { path, json } => {
                let db = BicDb::open(&path)?;
                print_replication_status(&db, json)?;
            }
            ReplicationCommand::Stream {
                path,
                from,
                limit,
                listen,
                dev_localhost_plaintext,
                tls_cert,
                tls_key,
                tls_ca,
                cluster_id,
                node_id,
                allowed_node_ids,
            } => {
                if let Some(addr) = listen {
                    replication_stream_listen_once(
                        &addr,
                        &path,
                        from,
                        limit,
                        dev_localhost_plaintext,
                        tls_cert,
                        tls_key,
                        tls_ca,
                        &cluster_id,
                        &node_id,
                        &allowed_node_ids,
                    )?;
                } else {
                    let db = BicDb::open(&path)?;
                    let frames = db.export_replication_frames_since(from, limit)?;
                    println!("{}", serde_json::to_string_pretty(&frames)?);
                }
            }
            ReplicationCommand::Follow {
                path,
                primary,
                server_name,
                max_frame_bytes,
                dev_localhost_plaintext,
                tls_cert,
                tls_key,
                tls_ca,
                continuous,
                reconnect_backoff_ms,
                max_attempts,
                cluster_id,
                node_id,
            } => {
                let mut db = BicDb::open_with_config(
                    &path,
                    replication_follow_db_config(&cluster_id, &node_id),
                )?;
                let report = if continuous {
                    replication_follow_loop(
                        &primary,
                        &server_name,
                        max_frame_bytes,
                        dev_localhost_plaintext,
                        tls_cert,
                        tls_key,
                        tls_ca,
                        reconnect_backoff_ms,
                        max_attempts,
                        &cluster_id,
                        &node_id,
                        &mut db,
                    )?
                } else {
                    replication_follow_once(
                        &primary,
                        &server_name,
                        max_frame_bytes,
                        dev_localhost_plaintext,
                        tls_cert,
                        tls_key,
                        tls_ca,
                        &cluster_id,
                        &node_id,
                        &mut db,
                    )?
                };
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            ReplicationCommand::Apply { path, file, json } => {
                let mut db = BicDb::open(&path)?;
                let bytes = std::fs::read(&file)?;
                let frames: Vec<CommitFrame> = serde_json::from_slice(&bytes)?;
                let report = db.apply_replication_batch(&frames)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!("Applied replication batch");
                    println!("Frames applied: {}", report.applied);
                    println!("Duplicates ignored: {}", report.duplicates);
                    println!(
                        "Last applied commit_seq: {}",
                        report.last_applied_commit_seq
                    );
                }
            }
            ReplicationCommand::Lag {
                path,
                source_commit_seq,
                json,
            } => {
                let db = BicDb::open(&path)?;
                let source = source_commit_seq.unwrap_or_else(|| db.current_commit_seq());
                let lag = db.replication_lag(source);
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "source_commit_seq": source,
                            "last_applied_commit_seq": db.last_applied_commit_seq(),
                            "lag_commits": lag,
                        }))?
                    );
                } else {
                    println!("Source commit_seq: {source}");
                    println!("Last applied commit_seq: {}", db.last_applied_commit_seq());
                    println!("Lag commits: {lag}");
                }
            }
            ReplicationCommand::Cert { command } => match command {
                ReplicationCertCommand::Check { cert, key, ca } => {
                    for path in [&cert, &key, &ca] {
                        let metadata = std::fs::metadata(path)?;
                        if !metadata.is_file() || metadata.len() == 0 {
                            bail!(
                                "replication certificate file {} is empty or not a file",
                                path.display()
                            );
                        }
                    }
                    println!("Replication certificate files are present and non-empty");
                }
            },
            ReplicationCommand::Snapshot { command } => match command {
                ReplicationSnapshotCommand::Create {
                    path,
                    out,
                    snapshot_id,
                    cluster_id,
                    node_id,
                    chunk_bytes,
                } => {
                    let db = BicDb::open(&path)?;
                    db.flush()?;
                    let snapshot_commit_seq = db.current_commit_seq();
                    drop(db);
                    let mut archive = Vec::new();
                    replication_transport::write_snapshot_archive(&path, &mut archive)?;
                    let frames = replication_transport::snapshot_frames_from_reader(
                        &snapshot_id,
                        &cluster_id,
                        &node_id,
                        snapshot_commit_seq,
                        &mut &archive[..],
                        chunk_bytes,
                    )?;
                    std::fs::write(&out, serde_json::to_vec_pretty(&frames)?)?;
                    println!(
                        "Wrote replication snapshot {} at commit_seq {} to {}",
                        snapshot_id,
                        snapshot_commit_seq,
                        out.display()
                    );
                }
                ReplicationSnapshotCommand::Restore {
                    input,
                    target,
                    force,
                } => {
                    let bytes = std::fs::read(&input)?;
                    let frames: Vec<ReplicationFrame> = serde_json::from_slice(&bytes)?;
                    let mut archive = Vec::new();
                    let restored_bytes = replication_transport::restore_snapshot_frames_to_writer(
                        &frames,
                        &mut archive,
                    )?;
                    replication_transport::restore_snapshot_archive_atomic(
                        &mut &archive[..],
                        &target,
                        force,
                    )?;
                    println!(
                        "Restored replication snapshot bytes={} to {}",
                        restored_bytes,
                        target.display()
                    );
                }
            },
        },
        Command::Consensus { command } => match command {
            ConsensusCommand::Status { path, json } => {
                let db = BicDb::open(&path)?;
                print_consensus_status(&db, json)?;
            }
            ConsensusCommand::Run {
                path,
                listen,
                cluster_id,
                node_id,
                peers,
                dev_localhost_plaintext,
                tls_cert,
                tls_key,
                tls_ca,
                election_timeout_ms,
                heartbeat_interval_ms,
                max_frame_bytes,
                pg_listen,
            } => {
                consensus_run_loop(
                    path,
                    listen,
                    cluster_id,
                    node_id,
                    peers,
                    dev_localhost_plaintext,
                    tls_cert,
                    tls_key,
                    tls_ca,
                    election_timeout_ms,
                    heartbeat_interval_ms,
                    max_frame_bytes,
                    pg_listen,
                )?;
            }
        },
    }

    Ok(())
}

fn default_max_active_queries() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .max(1)
}

fn default_max_queued_writes() -> usize {
    default_max_active_queries().saturating_mul(4).max(16)
}

fn default_max_queued_queries() -> usize {
    default_max_active_queries().saturating_mul(4).max(16)
}

fn default_backup_drill_target() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!("bicdb-backup-drill-{nanos}"))
}

#[derive(Debug)]
struct ProductionGateInput {
    path: PathBuf,
    profile: HardeningProfile,
    host: String,
    require_auth: bool,
    auth_method: CliAuthMethod,
    allow_remote_no_auth: bool,
    require_tls: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_client_ca: Option<PathBuf>,
    db_key_env: Option<String>,
    protected_data_field_key_env: Option<String>,
    protected_data_hmac_key_env: Option<String>,
    backup_key_env: Option<String>,
    audit_retention_days: u64,
    audit_tamper_evidence: bool,
    protected_data_evidence: Option<PathBuf>,
    dependency_evidence: Option<PathBuf>,
    evidence_max_age_days: u64,
}

#[derive(Debug, Serialize)]
struct ProductionGateReport {
    passed: bool,
    profile: String,
    checks: Vec<ProductionGateCheck>,
}

#[derive(Debug, Serialize)]
struct ProductionGateCheck {
    name: &'static str,
    passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct SupplyChainAuditReport {
    passed: bool,
    command: String,
    detail: String,
}

fn production_gate_report(input: ProductionGateInput) -> Result<ProductionGateReport> {
    let mut checks = Vec::new();
    let strict = !matches!(input.profile, HardeningProfile::LocalDev);
    let protected_data = matches!(input.profile, HardeningProfile::RegulatedData);
    let remote = !is_cli_local_host(&input.host);

    checks.push(check(
        "database path exists",
        input.path.exists(),
        Some(format!("{}", input.path.display())),
    ));
    checks.push(check(
        "database path permissions",
        path_permissions_private(&input.path),
        Some("directory must not be group/world writable".to_string()),
    ));
    checks.push(check(
        "remote no-auth rejected",
        (input.require_auth || !remote) && !input.allow_remote_no_auth,
        Some("non-local deployments must require authentication".to_string()),
    ));
    checks.push(check(
        "SCRAM authentication",
        !strict || (input.require_auth && matches!(input.auth_method, CliAuthMethod::ScramSha256)),
        Some("clinic LAN and production profiles require --auth-method scram-sha-256".to_string()),
    ));
    checks.push(check(
        "TLS required",
        !strict || input.require_tls,
        Some("clinic LAN and production profiles require TLS".to_string()),
    ));
    checks.push(check(
        "TLS certificate pair",
        input.tls_cert.is_some() == input.tls_key.is_some()
            && (!input.require_tls || input.tls_cert.is_some()),
        Some("both --tls-cert and --tls-key are required when TLS is required".to_string()),
    ));
    if let Some(path) = &input.tls_cert {
        checks.push(check(
            "TLS certificate exists",
            path.exists(),
            Some(format!("{} must exist", path.display())),
        ));
    }
    if let Some(path) = &input.tls_key {
        checks.push(check(
            "TLS key permissions",
            path_permissions_private(path),
            Some(format!(
                "{} must not be group/world readable or writable",
                path.display()
            )),
        ));
    }
    checks.push(check(
        "client certificate policy",
        input.tls_client_ca.is_none(),
        Some("client certificate authentication is currently unsupported".to_string()),
    ));
    checks.push(secret_env_check(
        "database encryption key",
        input.db_key_env.as_deref(),
        strict,
    ));
    checks.push(secret_env_check(
        "backup key",
        input.backup_key_env.as_deref(),
        strict,
    ));
    checks.push(secret_env_check(
        "protected data field encryption key",
        input.protected_data_field_key_env.as_deref(),
        protected_data,
    ));
    checks.push(secret_env_check(
        "protected data blind-index HMAC key",
        input.protected_data_hmac_key_env.as_deref(),
        protected_data,
    ));
    checks.push(check(
        "audit retention",
        !strict || input.audit_retention_days >= 365,
        Some("production profiles require at least 365 days".to_string()),
    ));
    checks.push(check(
        "audit tamper evidence",
        !strict || input.audit_tamper_evidence,
        Some("production profiles require hash-chained or immutable audit evidence".to_string()),
    ));
    checks.push(evidence_check(
        "protected data/security evidence",
        input.protected_data_evidence.as_deref(),
        strict,
        input.evidence_max_age_days,
    ));
    checks.push(evidence_check(
        "dependency audit evidence",
        input.dependency_evidence.as_deref(),
        strict,
        input.evidence_max_age_days,
    ));

    let passed = checks.iter().all(|check| check.passed);
    Ok(ProductionGateReport {
        passed,
        profile: input.profile.to_string(),
        checks,
    })
}

fn check(name: &'static str, passed: bool, detail: Option<String>) -> ProductionGateCheck {
    ProductionGateCheck {
        name,
        passed,
        detail: if passed { None } else { detail },
    }
}

fn secret_env_check(
    name: &'static str,
    env_name: Option<&str>,
    required: bool,
) -> ProductionGateCheck {
    if !required {
        return check(name, true, None);
    }
    let Some(env_name) = env_name else {
        return check(
            name,
            false,
            Some(format!(
                "{name} must be supplied by an explicit env var name"
            )),
        );
    };
    match std::env::var(env_name) {
        Ok(value) if strong_secret_value(&value) => check(name, true, None),
        Ok(_) => check(
            name,
            false,
            Some(format!(
                "{env_name} is empty, too short, or matches a default/weak pattern"
            )),
        ),
        Err(_) => check(name, false, Some(format!("{env_name} is not set"))),
    }
}

fn strong_secret_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.len() < 32 {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    ![
        "change-me",
        "changeme",
        "default",
        "password",
        "secret",
        "use-a-real-secret",
        "test",
    ]
    .iter()
    .any(|weak| lower.contains(weak))
}

fn evidence_check(
    name: &'static str,
    path: Option<&Path>,
    required: bool,
    max_age_days: u64,
) -> ProductionGateCheck {
    if !required {
        return check(name, true, None);
    }
    let Some(path) = path else {
        return check(name, false, Some("evidence file is required".to_string()));
    };
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return check(name, false, Some(format!("{}: {error}", path.display())));
        }
    };
    let json: Value = match serde_json::from_slice(&bytes) {
        Ok(json) => json,
        Err(error) => {
            return check(
                name,
                false,
                Some(format!("{} is not JSON: {error}", path.display())),
            );
        }
    };
    let passed = json.get("passed").and_then(Value::as_bool).unwrap_or(false);
    if !passed {
        return check(
            name,
            false,
            Some(format!("{} does not contain passed=true", path.display())),
        );
    }
    match evidence_is_fresh(path, max_age_days) {
        Ok(true) => check(name, true, None),
        Ok(false) => check(
            name,
            false,
            Some(format!(
                "{} is older than {max_age_days} days",
                path.display()
            )),
        ),
        Err(error) => check(name, false, Some(error)),
    }
}

fn evidence_is_fresh(path: &Path, max_age_days: u64) -> std::result::Result<bool, String> {
    let modified = std::fs::metadata(path)
        .map_err(|error| format!("{}: {error}", path.display()))?
        .modified()
        .map_err(|error| format!("{} modified time: {error}", path.display()))?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_else(|_| Duration::from_secs(0));
    Ok(age <= Duration::from_secs(max_age_days.saturating_mul(24 * 60 * 60)))
}

fn path_permissions_private(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Ok(metadata) = std::fs::metadata(path) else {
            return false;
        };
        let mode = metadata.permissions().mode();
        if metadata.is_dir() {
            mode & 0o022 == 0
        } else {
            mode & 0o077 == 0
        }
    }
    #[cfg(not(unix))]
    {
        path.exists()
    }
}

fn is_cli_local_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn supply_chain_audit_report(source_root: &Path) -> SupplyChainAuditReport {
    let cargo_audit = ProcessCommand::new("cargo")
        .args(["audit", "--deny", "warnings"])
        .current_dir(source_root)
        .output();
    if let Ok(output) = cargo_audit {
        let detail = command_detail(&output.stdout, &output.stderr);
        if !output.status.success()
            && (detail.contains("no such command")
                || detail.contains("no command named `audit`")
                || detail.contains("unrecognized subcommand"))
        {
            return cargo_tree_locked_report(source_root);
        }
        return SupplyChainAuditReport {
            passed: output.status.success(),
            command: "cargo audit --deny warnings".to_string(),
            detail,
        };
    }

    cargo_tree_locked_report(source_root)
}

fn cargo_tree_locked_report(source_root: &Path) -> SupplyChainAuditReport {
    let tree = ProcessCommand::new("cargo")
        .args(["tree", "--locked"])
        .current_dir(source_root)
        .output();
    match tree {
        Ok(output) => SupplyChainAuditReport {
            passed: output.status.success(),
            command: "cargo tree --locked".to_string(),
            detail: command_detail(&output.stdout, &output.stderr),
        },
        Err(error) => SupplyChainAuditReport {
            passed: false,
            command: "cargo audit --deny warnings".to_string(),
            detail: format!("failed to run cargo audit or cargo tree: {error}"),
        },
    }
}

fn command_detail(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout);
    let stderr = String::from_utf8_lossy(stderr);
    let detail = if stderr.trim().is_empty() {
        stdout.trim().to_string()
    } else if stdout.trim().is_empty() {
        stderr.trim().to_string()
    } else {
        format!("{}\n{}", stdout.trim(), stderr.trim())
    };
    if detail.is_empty() {
        "command completed without output".to_string()
    } else {
        detail
    }
}

#[allow(clippy::too_many_arguments)]
fn pgwire_config_from_cli(
    host: String,
    port: u16,
    postgres_server_version: String,
    postgres_server_version_num: Option<String>,
    postgres_version_banner: bool,
    require_auth: bool,
    auth_method: CliAuthMethod,
    channel_binding: Option<CliChannelBindingPolicy>,
    allow_remote_no_auth: bool,
    max_connections: usize,
    max_connections_per_ip: usize,
    max_pending_accepts: usize,
    max_active_queries: usize,
    max_queued_queries: usize,
    max_active_reads: usize,
    max_queued_reads: usize,
    max_active_writes: usize,
    max_queued_writes: usize,
    idle_timeout_seconds: u64,
    shutdown_grace_seconds: u64,
    query_timeout_ms: u64,
    overload_timeout_ms: u64,
    write_timeout_ms: u64,
    max_result_rows: usize,
    max_request_bytes: usize,
    per_connection_memory_limit: usize,
    paged_rowid_registry: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    require_tls: bool,
    tls_client_ca: Option<PathBuf>,
    slow_query_log: Option<PathBuf>,
    slow_query_threshold_ms: u64,
    redact_query_text: bool,
    redact_fields: Vec<String>,
    storage_sync: CliStorageSync,
) -> Result<PgWireConfig> {
    let derived_version_num =
        bicdb_pgwire::derive_postgres_server_version_num(&postgres_server_version)
            .map_err(anyhow::Error::msg)?;
    let postgres_server_version_num = match postgres_server_version_num {
        Some(version_num) if version_num != derived_version_num => {
            bail!(
                "--postgres-server-version-num {version_num} does not match \
                 --postgres-server-version {postgres_server_version} (expected {derived_version_num})"
            );
        }
        Some(version_num) => version_num,
        None => derived_version_num,
    };
    Ok(PgWireConfig {
        host,
        port,
        postgres_server_version,
        postgres_server_version_num,
        postgres_version_banner,
        require_auth,
        auth_method: auth_method.into(),
        channel_binding: channel_binding.map(Into::into),
        allow_remote_no_auth,
        max_connections,
        max_connections_per_ip,
        max_pending_accepts,
        max_active_queries,
        max_queued_queries,
        max_active_reads,
        max_queued_reads,
        max_active_writes,
        max_queued_writes,
        idle_timeout: Duration::from_secs(idle_timeout_seconds),
        shutdown_grace_period: Duration::from_secs(shutdown_grace_seconds),
        query_timeout: Duration::from_millis(query_timeout_ms),
        overload_timeout: Duration::from_millis(overload_timeout_ms),
        write_timeout: Duration::from_millis(write_timeout_ms),
        max_result_rows,
        max_request_bytes,
        per_connection_memory_limit,
        paged_rowid_registry,
        tls_cert,
        tls_key,
        require_tls,
        tls_client_ca,
        slow_query_log,
        slow_query_threshold: Duration::from_millis(slow_query_threshold_ms),
        slow_query_redaction: redaction_config(redact_query_text, true, redact_fields),
        fsync: matches!(storage_sync, CliStorageSync::Durable),
        ..PgWireConfig::default()
    })
}

fn parse_cluster_labels(labels: &[String]) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for label in labels {
        let (key, value) = label.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("invalid cluster label `{label}`; expected KEY=VALUE")
        })?;
        if key.is_empty() || value.is_empty() {
            bail!("invalid cluster label `{label}`; key and value must not be empty");
        }
        if parsed.insert(key.to_string(), value.to_string()).is_some() {
            bail!("duplicate cluster label `{key}`");
        }
    }
    Ok(parsed)
}

fn cluster_address_is_explicit_loopback(address: &str) -> bool {
    address.starts_with("127.")
        || address.starts_with("[::1]:")
        || address == "localhost"
        || address.starts_with("localhost:")
}

fn open_distribution_store(path: &Path, fsync: bool) -> Result<DistributionStore> {
    let config = load_distribution_config(path)?;
    Ok(DistributionStore::open(path, config, fsync)?)
}

fn cluster_rebalance_options(
    max_moves: usize,
    max_moves_per_node: usize,
    max_bytes_in_flight: u64,
) -> RebalanceOptions {
    RebalanceOptions {
        max_replica_moves: max_moves,
        max_moves_per_node,
        max_bytes_in_flight,
        ..RebalanceOptions::default()
    }
}

#[derive(Clone, Debug)]
enum ClusterCertificateMutation {
    Stage(String),
    Abort,
}

fn cluster_certificate_mutation_satisfied(
    topology: &ClusterTopology,
    node_id: &ClusterNodeId,
    mutation: &ClusterCertificateMutation,
) -> Result<bool> {
    let node = topology
        .nodes
        .get(node_id)
        .ok_or_else(|| anyhow::anyhow!("local cluster node {node_id} is not registered"))?;
    match mutation {
        ClusterCertificateMutation::Stage(fingerprint) => {
            if node.tls_certificate_sha256.as_ref() == Some(fingerprint) {
                if node.pending_tls_certificate_sha256.is_some() {
                    bail!(
                        "node {node_id} already uses the requested certificate but has another rotation pending"
                    );
                }
                return Ok(true);
            }
            if let Some(pending) = &node.pending_tls_certificate_sha256 {
                if pending != fingerprint {
                    bail!(
                        "node {node_id} already has a different pending TLS certificate; abort it first"
                    );
                }
                return Ok(true);
            }
            Ok(false)
        }
        ClusterCertificateMutation::Abort => Ok(node.pending_tls_certificate_sha256.is_none()),
    }
}

fn wait_for_cluster_certificate_mutation(
    path: &Path,
    rpc: &TcpClusterRelocationTransport,
    node_id: &ClusterNodeId,
    mutation: ClusterCertificateMutation,
    mut topology: ClusterTopology,
) -> Result<ClusterTopology> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let now_ms = unix_now_millis();
    let mut last_error = None;
    loop {
        if cluster_certificate_mutation_satisfied(&topology, node_id, &mutation)? {
            return Ok(topology);
        }
        let mut candidates = Vec::new();
        if let Ok(status) = MetadataConsensusStore::inspect(path) {
            let leader = if status.role == MetadataConsensusRole::Leader {
                Some(status.node_id)
            } else {
                status.leader_id
            };
            if let Some(leader) = leader {
                candidates.push(leader);
            }
        }
        for voter in topology.metadata_voters() {
            if !candidates.contains(&voter) {
                candidates.push(voter);
            }
        }
        for candidate in candidates {
            let response = match &mutation {
                ClusterCertificateMutation::Stage(fingerprint) => {
                    rpc.stage_tls_certificate_rotation(&candidate, fingerprint.clone(), now_ms)
                }
                ClusterCertificateMutation::Abort => {
                    rpc.abort_tls_certificate_rotation(&candidate, now_ms)
                }
            };
            match response {
                Ok(updated) => {
                    rpc.install_topology(updated.clone())?;
                    topology = updated;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            match rpc.fetch_topology(&candidate) {
                Ok(updated) => {
                    rpc.install_topology(updated.clone())?;
                    topology = updated;
                    if cluster_certificate_mutation_satisfied(&topology, node_id, &mutation)? {
                        return Ok(topology);
                    }
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for quorum-committed TLS certificate mutation: {}",
                last_error.unwrap_or_else(|| "metadata leader is not currently known".to_string())
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn run_cluster_command(command: ClusterCommand) -> Result<()> {
    match command {
        ClusterCommand::Init {
            path,
            cluster_id,
            node_id,
            address,
            capacity_bytes,
            replication_factor,
            initial_ranges,
            failure_domains,
            labels,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            no_fsync,
            json,
        } => {
            let node_id = ClusterNodeId::new(node_id)?;
            let mut node_labels = parse_cluster_labels(&labels)?;
            node_labels
                .entry("server".to_string())
                .or_insert_with(|| node_id.as_str().to_string());
            let placement = PlacementPolicy::default().with_standard_failure_domains(
                failure_domains.into_iter().map(StandardFailureDomain::from),
            );
            let (transport, node_tls_certificate_sha256) = match (
                cluster_tls_cert,
                cluster_tls_key,
                cluster_tls_ca,
            ) {
                (Some(cert_path), Some(key_path), Some(ca_path)) => {
                    let fingerprint =
                        replication_transport::replication_certificate_sha256(&cert_path)?;
                    (
                        ClusterNetworkTransportConfig {
                            dev_localhost_plaintext: false,
                            tls: Some(ReplicationTlsConfig {
                                cert_path,
                                key_path,
                                ca_path,
                                require_client_cert: true,
                                dev_localhost_plaintext: false,
                            }),
                            ..ClusterNetworkTransportConfig::default()
                        },
                        Some(fingerprint),
                    )
                }
                (None, None, None) if cluster_address_is_explicit_loopback(&address) => {
                    (ClusterNetworkTransportConfig::default(), None)
                }
                (None, None, None) => bail!(
                    "remote cluster address `{address}` requires --cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca"
                ),
                _ => bail!(
                    "--cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca must be supplied together"
                ),
            };
            let config = DistributionConfig {
                enabled: true,
                cluster_id: ClusterId::new(cluster_id)?,
                node_id: node_id.clone(),
                node_address: address,
                node_tls_certificate_sha256,
                node_incarnation: 1,
                node_capacity_bytes: capacity_bytes,
                replication_factor,
                initial_ranges,
                default_placement: placement,
                transport,
                ..DistributionConfig::default()
            };
            let now_ms = unix_now_millis();
            let mut store = DistributionStore::initialize_at(&path, config, !no_fsync, now_ms)?;
            store.heartbeat(
                &node_id,
                1,
                0,
                capacity_bytes,
                node_labels,
                now_ms.saturating_add(1),
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(store.topology())?);
            } else {
                println!(
                    "Initialized cluster {} at {}",
                    store.topology().cluster_id,
                    path.display()
                );
                println!(
                    "Node: {} ({})",
                    node_id,
                    store.topology().nodes[&node_id].address
                );
                println!(
                    "Ranges: {}  Replication factor: {}",
                    store.topology().ranges.len(),
                    store.topology().replication_factor
                );
            }
        }
        ClusterCommand::Join {
            path,
            seed_node_id,
            seed_address,
            cluster_id,
            node_id,
            address,
            incarnation,
            capacity_bytes,
            labels,
            node_root,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            no_fsync,
            json,
        } => {
            if seed_node_id.is_some() || seed_address.is_some() || cluster_id.is_some() {
                let seed_node_id = seed_node_id
                    .ok_or_else(|| anyhow::anyhow!("remote join requires --seed-node-id"))?;
                let seed_address = seed_address
                    .ok_or_else(|| anyhow::anyhow!("remote join requires --seed-address"))?;
                let cluster_id = cluster_id
                    .ok_or_else(|| anyhow::anyhow!("remote join requires --cluster-id"))?;
                if path.is_some() {
                    bail!(
                        "remote join does not accept an existing-member path; run it on the empty server"
                    );
                }
                return run_remote_cluster_join(
                    cluster_id,
                    seed_node_id,
                    seed_address,
                    node_id,
                    address,
                    incarnation,
                    capacity_bytes,
                    labels,
                    node_root,
                    cluster_tls_cert,
                    cluster_tls_key,
                    cluster_tls_ca,
                    !no_fsync,
                    json,
                );
            }
            let path = path.ok_or_else(|| {
                anyhow::anyhow!(
                    "admin-assisted join requires an existing member path, or use all remote --seed-* options"
                )
            })?;
            let fsync = !no_fsync;
            let store = open_distribution_store(&path, fsync)?;
            let actor = store.config().node_id.clone();
            let now_ms = unix_now_millis();
            if !store.topology().nodes.contains_key(&actor) {
                bail!("local cluster node {actor} is not registered");
            }
            if !path.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE).exists() {
                bail!(
                    "cluster join requires a live metadata quorum; start `bicdb serve {}` first",
                    path.display()
                );
            }
            let node_id = ClusterNodeId::new(node_id)?;
            let mut node_labels = parse_cluster_labels(&labels)?;
            node_labels
                .entry("server".to_string())
                .or_insert_with(|| node_id.as_str().to_string());
            let mut node = ClusterNode::new(
                node_id.clone(),
                address,
                incarnation,
                capacity_bytes,
                now_ms,
            )?;
            for (key, value) in node_labels {
                node = node.with_label(key, value)?;
            }
            node = node.as_metadata_learner();
            let (target_transport, node_tls_certificate_sha256) = match (
                cluster_tls_cert,
                cluster_tls_key,
                cluster_tls_ca,
            ) {
                (Some(cert_path), Some(key_path), Some(ca_path)) => {
                    let fingerprint =
                        replication_transport::replication_certificate_sha256(&cert_path)?;
                    (
                        ClusterNetworkTransportConfig {
                            dev_localhost_plaintext: false,
                            tls: Some(ReplicationTlsConfig {
                                cert_path,
                                key_path,
                                ca_path,
                                require_client_cert: true,
                                dev_localhost_plaintext: false,
                            }),
                            ..store.config().transport.clone()
                        },
                        Some(fingerprint),
                    )
                }
                (None, None, None) if store.config().transport.dev_localhost_plaintext => {
                    (store.config().transport.clone(), None)
                }
                (None, None, None) => bail!(
                    "provisioning a remote member requires its --cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca"
                ),
                _ => bail!(
                    "--cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca must be supplied together"
                ),
            };
            if let Some(fingerprint) = node_tls_certificate_sha256 {
                node = node.with_tls_certificate_sha256(fingerprint)?;
            }
            let originally_registered = store.topology().nodes.contains_key(&node_id);
            let rpc = TcpClusterRelocationTransport::from_topology(
                store.topology().cluster_id.clone(),
                actor.clone(),
                store.topology().clone(),
                store.config().transport.clone(),
            )?;
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut last_error = None;
            let committed_topology = loop {
                let status = MetadataConsensusStore::inspect(&path)?;
                let leader_id = if status.role == MetadataConsensusRole::Leader {
                    Some(status.node_id)
                } else {
                    status.leader_id
                };
                if let Some(leader_id) = leader_id {
                    match rpc.register_metadata_learner(&leader_id, node.clone(), now_ms) {
                        Ok(topology) => {
                            rpc.install_topology(topology)?;
                            match rpc.fetch_topology(&leader_id) {
                                Ok(topology) => {
                                    rpc.install_topology(topology.clone())?;
                                    match topology.nodes.get(&node_id) {
                                        Some(committed)
                                            if committed.address == node.address
                                                && committed.incarnation == node.incarnation
                                                && committed.capacity_bytes
                                                    == node.capacity_bytes
                                                && committed.labels == node.labels
                                                && matches!(
                                                    committed.metadata_role,
                                                    MetadataMemberRole::Learner
                                                        | MetadataMemberRole::Voter
                                                ) =>
                                        {
                                            break topology;
                                        }
                                        Some(_) => {
                                            bail!(
                                                "metadata leader committed conflicting registration for node {node_id}"
                                            );
                                        }
                                        None => {}
                                    }
                                }
                                Err(error) => last_error = Some(error.to_string()),
                            }
                        }
                        Err(error) => last_error = Some(error.to_string()),
                    }
                } else {
                    last_error = Some("metadata leader is not currently known".to_string());
                }
                if Instant::now() >= deadline {
                    bail!(
                        "timed out waiting for quorum-committed learner registration: {}",
                        last_error.unwrap_or_else(|| "no leader response".to_string())
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            };
            let committed_node = &committed_topology.nodes[&node_id];
            let target_consensus_path = node_root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
            if committed_node.metadata_role == MetadataMemberRole::Voter {
                if !target_consensus_path.exists() {
                    bail!(
                        "node {node_id} is already a metadata voter; refusing to provision an \
                         empty voter directory—use its existing data directory or replace the \
                         identity with a new incarnation"
                    );
                }
                let target_status = MetadataConsensusStore::inspect(&node_root)?;
                if target_status.cluster_id != committed_topology.cluster_id
                    || target_status.node_id != node_id
                {
                    bail!(
                        "existing metadata consensus state in {} does not belong to node {} in \
                         cluster {}",
                        node_root.display(),
                        node_id,
                        committed_topology.cluster_id
                    );
                }
            }
            store.provision_member_directory_from_snapshot(
                &node_root,
                committed_topology.clone(),
                &node_id,
                target_transport,
                fsync,
            )?;
            let joined = !originally_registered;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "joined": joined,
                        "node_id": node_id,
                        "metadata_role": committed_node.metadata_role,
                        "topology_generation": committed_topology.generation,
                        "provisioned_root": node_root,
                    }))?
                );
            } else {
                println!(
                    "Node {} {} cluster {} as a metadata {:?}",
                    node_id,
                    if joined {
                        "joined"
                    } else {
                        "already belongs to"
                    },
                    committed_topology.cluster_id,
                    committed_node.metadata_role,
                );
                println!("Provisioned member directory: {}", node_root.display());
                println!(
                    "Start `bicdb serve {}`; BicDB will catch up and promote it automatically.",
                    node_root.display()
                );
            }
        }
        ClusterCommand::RotateCertificate {
            path,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            no_fsync,
            json,
        } => {
            let fsync = !no_fsync;
            let mut store = open_distribution_store(&path, fsync)?;
            if store.config().transport.dev_localhost_plaintext {
                bail!("TLS certificate rotation is unavailable in plaintext development mode");
            }
            if !path.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE).exists() {
                bail!(
                    "TLS certificate rotation requires a live metadata quorum; start `bicdb serve {}` first",
                    path.display()
                );
            }
            let actor = store.config().node_id.clone();
            let new_fingerprint =
                replication_transport::replication_certificate_sha256(&cluster_tls_cert)?;
            let new_tls = ReplicationTlsConfig {
                cert_path: cluster_tls_cert,
                key_path: cluster_tls_key,
                ca_path: cluster_tls_ca,
                require_client_cert: true,
                dev_localhost_plaintext: false,
            };
            let _ = replication_transport::build_client_tls(&new_tls)?;
            let _ = replication_transport::build_server_tls(&new_tls)?;
            let new_transport = ClusterNetworkTransportConfig {
                dev_localhost_plaintext: false,
                tls: Some(new_tls),
                ..store.config().transport.clone()
            };
            new_transport.validate()?;

            let rpc = TcpClusterRelocationTransport::from_topology(
                store.topology().cluster_id.clone(),
                actor.clone(),
                store.topology().clone(),
                store.config().transport.clone(),
            )?;
            let committed = wait_for_cluster_certificate_mutation(
                &path,
                &rpc,
                &actor,
                ClusterCertificateMutation::Stage(new_fingerprint.clone()),
                store.topology().clone(),
            )?;
            store.install_authoritative_topology(committed.clone())?;

            let mut new_config = store.config().clone();
            new_config.node_tls_certificate_sha256 = Some(new_fingerprint.clone());
            new_config.transport = new_transport;
            DistributionStore::open(&path, new_config.clone(), fsync)?;
            save_distribution_config(&path, &new_config, fsync)?;
            let active = committed.nodes[&actor].tls_certificate_sha256.as_deref()
                == Some(new_fingerprint.as_str());
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "node_id": actor,
                        "active": active,
                        "pending_tls_certificate_sha256":
                            committed.nodes[&actor].pending_tls_certificate_sha256,
                        "configuration_updated": true,
                        "restart_required": !active,
                    }))?
                );
            } else if active {
                println!("Node {actor} already uses the requested TLS certificate");
                println!("Local TLS configuration was verified and updated atomically");
            } else {
                println!(
                    "Quorum staged TLS certificate {} for node {actor}",
                    new_fingerprint
                );
                println!("Local TLS configuration was updated atomically");
                println!(
                    "Restart `bicdb serve {}`; the first new-certificate heartbeat will quorum-activate it",
                    path.display()
                );
            }
        }
        ClusterCommand::AbortCertificateRotation {
            path,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            no_fsync,
            json,
        } => {
            let fsync = !no_fsync;
            let store = open_distribution_store(&path, fsync)?;
            if store.config().transport.dev_localhost_plaintext {
                bail!("TLS certificate rotation is unavailable in plaintext development mode");
            }
            if !path.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE).exists() {
                bail!(
                    "aborting TLS certificate rotation requires a live metadata quorum; start `bicdb serve {}` first",
                    path.display()
                );
            }
            let actor = store.config().node_id.clone();
            let active_fingerprint =
                replication_transport::replication_certificate_sha256(&cluster_tls_cert)?;
            let node = &store.topology().nodes[&actor];
            if node.tls_certificate_sha256.as_deref() != Some(&active_fingerprint) {
                bail!(
                    "supplied certificate is not node {actor}'s active quorum-committed certificate"
                );
            }
            let active_tls = ReplicationTlsConfig {
                cert_path: cluster_tls_cert,
                key_path: cluster_tls_key,
                ca_path: cluster_tls_ca,
                require_client_cert: true,
                dev_localhost_plaintext: false,
            };
            let _ = replication_transport::build_client_tls(&active_tls)?;
            let _ = replication_transport::build_server_tls(&active_tls)?;
            let restored_transport = ClusterNetworkTransportConfig {
                dev_localhost_plaintext: false,
                tls: Some(active_tls),
                ..store.config().transport.clone()
            };
            restored_transport.validate()?;
            let mut restored_config = store.config().clone();
            restored_config.node_tls_certificate_sha256 = Some(active_fingerprint);
            restored_config.transport = restored_transport;
            let mut publishing_store =
                DistributionStore::open(&path, restored_config.clone(), fsync)?;
            // Restore the active material before removing the pending
            // fingerprint from quorum membership. If the command or host dies
            // after this point, either topology state still accepts the
            // atomically persisted configuration.
            save_distribution_config(&path, &restored_config, fsync)?;

            let rpc = TcpClusterRelocationTransport::from_topology(
                store.topology().cluster_id.clone(),
                actor.clone(),
                store.topology().clone(),
                store.config().transport.clone(),
            )?;
            let committed = wait_for_cluster_certificate_mutation(
                &path,
                &rpc,
                &actor,
                ClusterCertificateMutation::Abort,
                store.topology().clone(),
            )?;
            publishing_store.install_authoritative_topology(committed)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "node_id": actor,
                        "aborted": true,
                        "configuration_restored": true,
                        "restart_required": true,
                    }))?
                );
            } else {
                println!("Aborted the pending TLS certificate rotation for node {actor}");
                println!("Restored the active TLS configuration atomically");
                println!(
                    "Restart `bicdb serve {}` with the active certificate",
                    path.display()
                );
            }
        }
        ClusterCommand::Status {
            path,
            json,
            prometheus,
        } => {
            if json && prometheus {
                bail!("--json and --prometheus are mutually exclusive");
            }
            let store = open_distribution_store(&path, false)?;
            let now_ms = unix_now_millis();
            let repair = store.plan_failure_repair(&RebalanceOptions::default(), now_ms)?;
            let metrics =
                ClusterOperationalMetrics::from_topology(store.topology(), &repair, now_ms);
            let metadata = path
                .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
                .exists()
                .then(|| MetadataConsensusStore::inspect(&path))
                .transpose()?;
            if prometheus {
                print!("{}", metrics.to_prometheus());
                if let Some(metadata) = metadata {
                    println!(
                        "bicdb_cluster_metadata_current_term {}",
                        metadata.current_term
                    );
                    println!(
                        "bicdb_cluster_metadata_commit_index {}",
                        metadata.commit_index
                    );
                    println!(
                        "bicdb_cluster_metadata_last_log_index {}",
                        metadata.last_log_index
                    );
                    println!(
                        "bicdb_cluster_metadata_topology_generation {}",
                        metadata.topology_generation
                    );
                    for role in [
                        MetadataConsensusRole::Follower,
                        MetadataConsensusRole::Candidate,
                        MetadataConsensusRole::Leader,
                    ] {
                        let name = match role {
                            MetadataConsensusRole::Follower => "follower",
                            MetadataConsensusRole::Candidate => "candidate",
                            MetadataConsensusRole::Leader => "leader",
                        };
                        println!(
                            "bicdb_cluster_metadata_role_{name} {}",
                            u8::from(metadata.role == role)
                        );
                    }
                }
            } else if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "topology": store.topology(),
                        "failure_repair": repair,
                        "metrics": metrics,
                        "metadata_consensus": metadata,
                    }))?
                );
            } else {
                println!(
                    "Cluster {} generation {}",
                    store.topology().cluster_id,
                    store.topology().generation
                );
                println!(
                    "Nodes: {} (live {} suspect {} dead {} draining {} decommissioned {})",
                    metrics.nodes_total,
                    metrics.nodes_live,
                    metrics.nodes_suspect,
                    metrics.nodes_dead,
                    metrics.nodes_draining,
                    metrics.nodes_decommissioned
                );
                println!(
                    "Ranges: {}  Replicas: {}  Leaders: {}",
                    metrics.ranges_total, metrics.replicas_total, metrics.leaders_total
                );
                println!(
                    "Under-replicated: {}  Unavailable: {}  Active relocations: {}",
                    metrics.under_replicated_ranges,
                    metrics.unavailable_ranges,
                    metrics.active_relocations
                );
                println!(
                    "Skew: replicas {} leaders {} bytes {}",
                    metrics.replica_skew, metrics.leader_skew, metrics.replica_byte_skew
                );
                if let Some(metadata) = metadata {
                    println!(
                        "Metadata: {:?} term {} leader {} commit {}/{}",
                        metadata.role,
                        metadata.current_term,
                        metadata
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("-"),
                        metadata.commit_index,
                        metadata.last_log_index
                    );
                }
            }
        }
        ClusterCommand::Route {
            path,
            namespace,
            key,
            json,
        } => {
            let store = open_distribution_store(&path, false)?;
            let plan = plan_point_query(store.topology(), &namespace, &key)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                let target = &plan.targets[0];
                println!(
                    "{} / {} -> {} epoch {} leader {} ({})",
                    namespace,
                    key,
                    target.range_id,
                    target.range_epoch,
                    target.leader_node,
                    target.leader_address
                );
            }
        }
        ClusterCommand::Rebalance {
            path,
            apply,
            max_moves,
            max_moves_per_node,
            max_bytes_in_flight,
            json,
        } => {
            let mut store = open_distribution_store(&path, true)?;
            let actor = store.config().node_id.clone();
            let options =
                cluster_rebalance_options(max_moves, max_moves_per_node, max_bytes_in_flight);
            let plan = store.plan_rebalance(&options, unix_now_millis())?;
            let relocations = if apply {
                store.apply_rebalance_plan(&plan, &actor, unix_now_millis())?
            } else {
                Vec::new()
            };
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "applied": apply,
                        "plan": plan,
                        "relocations_started": relocations,
                    }))?
                );
            } else {
                println!(
                    "Rebalance plan {}: {} replica moves, {} leader transfers, {} bytes",
                    plan.id,
                    plan.replica_moves.len(),
                    plan.leader_transfers.len(),
                    plan.estimated_bytes_in_flight
                );
                println!(
                    "{}",
                    if apply {
                        "Plan applied; the cluster supervisor will drive durable relocations"
                    } else {
                        "Dry run; pass --apply to allocate relocations"
                    }
                );
            }
        }
        ClusterCommand::Drain {
            path,
            node_id,
            json,
        } => {
            let mut store = open_distribution_store(&path, true)?;
            let actor = store.config().node_id.clone();
            let node_id = ClusterNodeId::new(node_id)?;
            let changed = store.begin_drain(&node_id, &actor, unix_now_millis())?;
            let (repair, relocations) = store.start_failure_repair_cycle(
                &RebalanceOptions::default(),
                &actor,
                unix_now_millis(),
            )?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "changed": changed,
                        "node_id": node_id,
                        "rebalance_plan_id": repair.rebalance.id,
                        "relocations_started": relocations,
                    }))?
                );
            } else {
                println!("Node {node_id} is draining");
                println!("Relocations started: {}", relocations.len());
            }
        }
        ClusterCommand::Remove {
            path,
            node_id,
            json,
        } => {
            let mut store = open_distribution_store(&path, true)?;
            let actor = store.config().node_id.clone();
            let node_id = ClusterNodeId::new(node_id)?;
            let safety = store.assess_node_removal(&node_id)?;
            if !safety.can_decommission {
                if json {
                    println!("{}", serde_json::to_string_pretty(&safety)?);
                }
                bail!(
                    "node {node_id} cannot be removed safely: {}",
                    safety.reasons.join("; ")
                );
            }
            let changed = store.decommission_node(&node_id, &actor, unix_now_millis())?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "removed": changed,
                        "safety": safety,
                    }))?
                );
            } else {
                println!("Node {node_id} decommissioned");
            }
        }
        ClusterCommand::CertifyPlan {
            path,
            profile,
            run_id,
            output,
            no_fsync,
            json,
        } => {
            let store = open_distribution_store(&path, false)?;
            let profile = ClusterScaleProfile::from(profile);
            let run_id = run_id.unwrap_or_else(|| {
                format!(
                    "{}-{}-{}",
                    store.topology().cluster_id,
                    profile,
                    unix_now_millis()
                )
            });
            let metadata = MetadataConsensusStore::inspect(&path).map_err(|error| {
                anyhow::anyhow!(
                    "certification planning requires a running, quorum-committed cluster at {}: {error}",
                    path.display()
                )
            })?;
            let plan = ClusterCertificationPlan::from_live_cluster(
                profile,
                run_id,
                store.topology(),
                store.config(),
                &metadata,
                storage_mode(&path)?,
                unix_now_millis(),
            )?;
            save_cluster_certification_plan(&output, &plan, !no_fsync)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                println!(
                    "Created {} certification plan {}",
                    plan.profile, plan.run_id
                );
                println!("Plan: {}", output.display());
                println!(
                    "Locked topology: cluster {} generation {} SHA-256 {}",
                    plan.cluster_id, plan.topology_generation, plan.topology_sha256
                );
                println!(
                    "Preflight: {} live nodes, metadata term {} commit {}, schema {}, config SHA-256 {}",
                    plan.preflight.metrics.nodes_live,
                    plan.preflight.metadata_current_term,
                    plan.preflight.metadata_commit_index,
                    plan.preflight.schema_sha256,
                    plan.preflight.distribution_config_sha256,
                );
                println!(
                    "Gate: {} nodes, RF{}, at least {} ranges and {} logical bytes",
                    plan.gates.required_nodes,
                    plan.gates.required_replication_factor,
                    plan.gates.minimum_ranges,
                    plan.gates.minimum_logical_dataset_bytes
                );
            }
        }
        ClusterCommand::CertifyStart {
            bundle,
            plan,
            source_commit,
            source_dirty,
            no_fsync,
            json,
        } => {
            let plan_path = if plan.is_absolute() {
                plan
            } else {
                bundle.join(plan)
            };
            let state = initialize_cluster_certification_bundle(
                &bundle,
                &plan_path,
                source_commit,
                source_dirty,
                unix_now_millis(),
                !no_fsync,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&state)?);
            } else {
                println!(
                    "Certification collector {}: {}",
                    state.run_id,
                    if state.completed_at_ms.is_some() {
                        "finalized"
                    } else {
                        "open"
                    }
                );
                println!("Bundle: {}", bundle.display());
                println!("Plan SHA-256: {}", state.plan_sha256);
                println!(
                    "Source: {}{}",
                    state.source_commit,
                    if state.source_dirty { " (dirty)" } else { "" }
                );
            }
        }
        ClusterCommand::CertifyRecord {
            bundle,
            input,
            no_fsync,
            json,
        } => {
            let observation =
                serde_json::from_slice::<ClusterCertificationObservation>(&std::fs::read(&input)?)?;
            let state = record_cluster_certification_observation(
                &bundle,
                observation,
                unix_now_millis(),
                !no_fsync,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&state)?);
            } else {
                println!("Recorded {} in {}", input.display(), state.run_id);
                println!(
                    "Progress: {}/5 hardware, saturation {}, {}/8 failures, {} artifacts",
                    state.hardware.len(),
                    if state.background_saturation.is_some() {
                        "recorded"
                    } else {
                        "missing"
                    },
                    state.failure_trials.len(),
                    state.artifacts.len()
                );
            }
        }
        ClusterCommand::CertifyCapture {
            bundle,
            kind,
            payload,
            output,
            payload_format,
            records,
            started_at_ms,
            completed_at_ms,
            no_fsync,
            json,
        } => {
            let now_ms = unix_now_millis();
            let evidence = capture_cluster_certification_artifact(
                &bundle,
                kind.into(),
                &payload,
                &output,
                payload_format.into(),
                records,
                started_at_ms,
                completed_at_ms,
                now_ms,
                !no_fsync,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&evidence)?);
            } else {
                println!(
                    "Captured and registered {} ({} bytes, SHA-256 {})",
                    evidence.relative_path.display(),
                    evidence.size_bytes,
                    evidence.sha256
                );
                println!("Payload: {}", payload.display());
                println!("Records: {records}");
                println!("Interval: {started_at_ms}..={completed_at_ms}");
            }
        }
        ClusterCommand::CertifyArtifact {
            bundle,
            kind,
            path,
            no_fsync,
            json,
        } => {
            let evidence = register_cluster_certification_artifact(
                &bundle,
                kind.into(),
                &path,
                unix_now_millis(),
                !no_fsync,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&evidence)?);
            } else {
                println!(
                    "Registered {} ({} bytes, SHA-256 {})",
                    evidence.relative_path.display(),
                    evidence.size_bytes,
                    evidence.sha256
                );
            }
        }
        ClusterCommand::CertifyStatus { bundle, json } => {
            let state = load_cluster_certification_state(&bundle)?;
            let plan =
                load_cluster_certification_plan(bundle.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN))?;
            let publication_path = bundle.join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST);
            let publication = if publication_path.exists() {
                Some(load_cluster_certification_publication_manifest(
                    &publication_path,
                )?)
            } else {
                None
            };
            let missing_nodes = plan
                .node_ids
                .iter()
                .filter(|node_id| !state.hardware.contains_key(*node_id))
                .cloned()
                .collect::<Vec<_>>();
            let missing_failures = plan
                .required_failure_points
                .iter()
                .filter(|failure_point| !state.failure_trials.contains_key(*failure_point))
                .copied()
                .collect::<Vec<_>>();
            let artifact_kinds = state
                .artifacts
                .values()
                .map(|artifact| artifact.kind)
                .collect::<BTreeSet<_>>();
            let missing_artifact_kinds = plan
                .required_artifact_kinds
                .iter()
                .filter(|kind| !artifact_kinds.contains(*kind))
                .copied()
                .collect::<Vec<_>>();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "state": &state,
                        "missing_nodes": missing_nodes,
                        "missing_failure_points": missing_failures,
                        "missing_artifact_kinds": missing_artifact_kinds,
                        "measurements_missing": state.measurements.is_none(),
                        "background_saturation_missing": state.background_saturation.is_none(),
                        "restore_missing": state.restore.is_none(),
                        "expansion_missing": state.expansion.is_none(),
                        "publication_manifest": publication,
                    }))?
                );
            } else {
                println!(
                    "Certification collector {}: {}",
                    state.run_id,
                    if state.completed_at_ms.is_some() {
                        "finalized"
                    } else {
                        "open"
                    }
                );
                println!("Profile: {}", state.profile);
                println!(
                    "Hardware: {}/{} nodes",
                    state.hardware.len(),
                    plan.node_ids.len()
                );
                if !missing_nodes.is_empty() {
                    println!("Missing hardware nodes: {missing_nodes:?}");
                }
                println!(
                    "Measurements: {}",
                    if state.measurements.is_some() {
                        "recorded"
                    } else {
                        "missing"
                    }
                );
                println!(
                    "Background saturation: {}",
                    if state.background_saturation.is_some() {
                        "recorded"
                    } else {
                        "missing"
                    }
                );
                println!(
                    "Failure trials: {}/{}",
                    state.failure_trials.len(),
                    plan.required_failure_points.len()
                );
                if !missing_failures.is_empty() {
                    println!("Missing failure trials: {missing_failures:?}");
                }
                println!(
                    "Restore: {}; expansion: {}",
                    if state.restore.is_some() {
                        "recorded"
                    } else {
                        "missing"
                    },
                    if state.expansion.is_some() {
                        "recorded"
                    } else {
                        "missing"
                    }
                );
                println!(
                    "Raw artifact kinds: {}/{} ({} files)",
                    artifact_kinds.len(),
                    plan.required_artifact_kinds.len(),
                    state.artifacts.len()
                );
                if !missing_artifact_kinds.is_empty() {
                    println!("Missing raw artifact kinds: {missing_artifact_kinds:?}");
                }
                if let Some(publication) = &publication {
                    println!(
                        "Publication: {} artifacts, {} bytes, bundle SHA-256 {}",
                        publication.artifact_count,
                        publication.artifact_bytes,
                        publication.bundle_sha256
                    );
                } else {
                    println!("Publication: not published");
                }
            }
        }
        ClusterCommand::CertifyFinish {
            bundle,
            no_fsync,
            json,
        } => {
            let verification =
                finalize_cluster_certification_bundle(&bundle, unix_now_millis(), !no_fsync)?;
            let passed = verification.passed;
            let failure_count = verification.failures.len();
            if json {
                println!("{}", serde_json::to_string_pretty(&verification)?);
            } else {
                println!(
                    "Cluster certification {}: {}",
                    verification.run_id,
                    if passed { "PASS" } else { "FAIL" }
                );
                println!(
                    "Cryptographically verified artifacts: {}",
                    verification.verified_artifacts
                );
                println!(
                    "Verified raw artifact bytes: {}",
                    verification.verified_artifact_bytes
                );
                println!(
                    "Publication manifest SHA-256: {}",
                    verification.publication_manifest_sha256
                );
                for failure in &verification.failures {
                    println!("FAIL: {failure}");
                }
            }
            if !passed {
                bail!(
                    "cluster certification bundle failed {failure_count} verification condition(s)"
                );
            }
        }
        ClusterCommand::CertifyVerify {
            bundle,
            plan,
            report,
            json,
        } => {
            let plan_path = if plan.is_absolute() {
                plan
            } else {
                bundle.join(plan)
            };
            let report_path = if report.is_absolute() {
                report
            } else {
                bundle.join(report)
            };
            let verification =
                verify_cluster_certification_bundle(&bundle, &plan_path, &report_path)?;
            let passed = verification.passed;
            let failure_count = verification.failures.len();
            if json {
                println!("{}", serde_json::to_string_pretty(&verification)?);
            } else {
                println!(
                    "Cluster certification {}: {}",
                    verification.run_id,
                    if passed { "PASS" } else { "FAIL" }
                );
                println!("Profile: {}", verification.profile);
                println!(
                    "Cryptographically verified artifacts: {}",
                    verification.verified_artifacts
                );
                println!(
                    "Verified raw artifact bytes: {}",
                    verification.verified_artifact_bytes
                );
                println!(
                    "Publication manifest SHA-256: {}",
                    verification.publication_manifest_sha256
                );
                for failure in &verification.failures {
                    println!("FAIL: {failure}");
                }
            }
            if !passed {
                bail!(
                    "cluster certification bundle failed {failure_count} verification condition(s)"
                );
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_remote_cluster_join(
    cluster_id: String,
    seed_node_id: String,
    seed_address: String,
    node_id: String,
    address: String,
    incarnation: u64,
    capacity_bytes: u64,
    labels: Vec<String>,
    node_root: PathBuf,
    cluster_tls_cert: Option<PathBuf>,
    cluster_tls_key: Option<PathBuf>,
    cluster_tls_ca: Option<PathBuf>,
    fsync: bool,
    json: bool,
) -> Result<()> {
    let cluster_id = ClusterId::new(cluster_id)?;
    let seed_node_id = ClusterNodeId::new(seed_node_id)?;
    let node_id = ClusterNodeId::new(node_id)?;
    if node_id == seed_node_id {
        bail!("joining node ID must differ from the seed node ID");
    }
    let tls_paths = match (cluster_tls_cert, cluster_tls_key, cluster_tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some((cert, key, ca)),
        (None, None, None) => None,
        _ => bail!(
            "--cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca must be supplied together"
        ),
    };
    let bootstrap_transport = cluster_join_transport(
        ClusterNetworkTransportConfig::default(),
        &seed_address,
        tls_paths.as_ref(),
    )?;
    let bootstrap =
        TcpClusterBootstrapClient::new(cluster_id.clone(), node_id.clone(), bootstrap_transport)?;
    let mut node_labels = parse_cluster_labels(&labels)?;
    node_labels
        .entry("server".to_string())
        .or_insert_with(|| node_id.as_str().to_string());
    let mut node = ClusterNode::new(
        node_id.clone(),
        address.clone(),
        incarnation,
        capacity_bytes,
        unix_now_millis(),
    )?;
    for (key, value) in node_labels {
        node = node.with_label(key, value)?;
    }
    if let Some((cert_path, _, _)) = tls_paths.as_ref() {
        node = node.with_tls_certificate_sha256(
            replication_transport::replication_certificate_sha256(cert_path)?,
        )?;
    }
    node = node.as_metadata_learner();

    let deadline = Instant::now() + Duration::from_secs(30);
    let registration_time_ms = unix_now_millis();
    let mut snapshot = bootstrap.fetch_snapshot(&seed_node_id, &seed_address)?;
    let mut last_error = None;
    loop {
        if let Some(existing) = snapshot.topology.nodes.get(&node_id) {
            if existing.address != node.address
                || existing.tls_certificate_sha256 != node.tls_certificate_sha256
                || existing.incarnation != node.incarnation
                || existing.capacity_bytes != node.capacity_bytes
                || existing.labels != node.labels
                || !matches!(
                    existing.metadata_role,
                    MetadataMemberRole::Learner | MetadataMemberRole::Voter
                )
            {
                bail!(
                    "cluster contains a conflicting registration for joining node {}",
                    node_id
                );
            }
            break;
        }

        if let Some(leader_id) = snapshot.metadata_leader_id.clone() {
            if let Some(leader) = snapshot.topology.nodes.get(&leader_id) {
                if let Err(error) = bootstrap.register_metadata_learner(
                    &leader_id,
                    &leader.address,
                    node.clone(),
                    registration_time_ms,
                ) {
                    last_error = Some(error.to_string());
                }
            } else {
                last_error = Some(format!(
                    "reported metadata leader {leader_id} is absent from topology"
                ));
            }
        } else {
            last_error = Some("metadata leader is not currently known".to_string());
        }

        let mut candidates = BTreeMap::from([(seed_node_id.clone(), seed_address.clone())]);
        candidates.extend(snapshot.topology.metadata_voters().into_iter().filter_map(
            |candidate| {
                snapshot
                    .topology
                    .nodes
                    .get(&candidate)
                    .map(|node| (candidate, node.address.clone()))
            },
        ));
        let mut refreshed = None;
        for (candidate_id, candidate_address) in candidates {
            match bootstrap.fetch_snapshot(&candidate_id, &candidate_address) {
                Ok(candidate) => {
                    refreshed = Some(candidate);
                    break;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if let Some(candidate) = refreshed {
            snapshot = candidate;
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for quorum-committed remote learner registration: {}",
                last_error.unwrap_or_else(|| "no bootstrap response".to_string())
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let target_transport = cluster_join_transport(
        snapshot.config_template.transport.clone(),
        &address,
        tls_paths.as_ref(),
    )?;
    let config = provision_cluster_member_directory(
        &node_root,
        &snapshot,
        &node_id,
        target_transport,
        fsync,
    )?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "joined": true,
                "cluster_id": cluster_id,
                "node_id": node_id,
                "metadata_role": snapshot.topology.nodes[&node_id].metadata_role,
                "topology_generation": snapshot.topology.generation,
                "provisioned_root": node_root,
                "seed_node_id": seed_node_id,
                "seed_address": seed_address,
            }))?
        );
    } else {
        println!(
            "Node {} joined cluster {} remotely as metadata {:?}",
            node_id, cluster_id, snapshot.topology.nodes[&node_id].metadata_role
        );
        println!(
            "Provisioned local member directory: {}",
            node_root.display()
        );
        println!(
            "Start `bicdb serve {}`; BicDB will catch up, promote, and rebalance automatically.",
            node_root.display()
        );
        println!("Cluster RPC address: {}", config.node_address);
    }
    Ok(())
}

fn cluster_join_transport(
    mut base: ClusterNetworkTransportConfig,
    address: &str,
    tls_paths: Option<&(PathBuf, PathBuf, PathBuf)>,
) -> Result<ClusterNetworkTransportConfig> {
    match tls_paths {
        Some((cert_path, key_path, ca_path)) => {
            base.dev_localhost_plaintext = false;
            base.tls = Some(ReplicationTlsConfig {
                cert_path: cert_path.clone(),
                key_path: key_path.clone(),
                ca_path: ca_path.clone(),
                require_client_cert: true,
                dev_localhost_plaintext: false,
            });
        }
        None if base.dev_localhost_plaintext && cluster_address_is_explicit_loopback(address) => {
            base.tls = None;
        }
        None => bail!(
            "remote cluster address `{address}` requires --cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca"
        ),
    }
    base.validate()?;
    Ok(base)
}

fn run_serve_command(
    path: PathBuf,
    config: PgWireConfig,
    host_services: Vec<Arc<dyn PgWireHostService>>,
) -> Result<()> {
    if !config.require_auth
        && !config.allow_remote_no_auth
        && !matches!(config.host.as_str(), "127.0.0.1" | "localhost" | "::1")
    {
        eprintln!(
            "Refusing unauthenticated non-local server mode; pass --require-auth or --allow-remote-no-auth"
        );
    }
    println!(
        "Serving BicDB PostgreSQL wire protocol at {}:{} for {}",
        config.host,
        config.port,
        path.display()
    );
    serve_with_host_services(path, config, host_services)?;
    Ok(())
}

#[derive(Debug, clap::Args)]
struct OperatorArgs {
    /// Optional authenticated login-management HTTP address. Non-loopback requires server TLS.
    #[arg(long, requires_all = ["operator_token_file", "operator_actor"])]
    operator_listen: Option<SocketAddr>,
    /// Owner-only file containing a random operator bearer token (32..256 ASCII bytes).
    #[arg(long, requires = "operator_listen")]
    operator_token_file: Option<PathBuf>,
    /// Stable operator identity recorded in the operation log; independent of SQL roles.
    #[arg(long, requires = "operator_listen")]
    operator_actor: Option<String>,
}

fn cli_host_services(operator: &OperatorArgs) -> Result<Vec<Arc<dyn PgWireHostService>>> {
    let mut services: Vec<Arc<dyn PgWireHostService>> =
        vec![Arc::new(ManualDistributionHostService)];
    if let Some(address) = operator.operator_listen {
        services.push(Arc::new(
            bicdb_pgwire::operator::OperatorService::from_token_file(
                address,
                operator
                    .operator_actor
                    .clone()
                    .context("--operator-actor is required")?,
                operator
                    .operator_token_file
                    .as_ref()
                    .context("--operator-token-file is required")?,
            )?,
        ));
    }
    Ok(services)
}

#[allow(clippy::too_many_arguments)]
fn run_cell_runtime(
    manifest: PathBuf,
    volume: PathBuf,
    expected_cell_id: String,
    expected_volume_id: String,
    guest_image_digest: String,
    trusted_key_specs: Vec<String>,
    key_file: Option<PathBuf>,
    key_lease_fd: Option<i32>,
    kms_trusted_key_specs: Vec<String>,
    attestation_nonce: Option<String>,
    artifact_root: PathBuf,
    release_policy: Option<PathBuf>,
    identity_policy: Option<PathBuf>,
    egress_policy: Option<PathBuf>,
    authorization_policy: Option<PathBuf>,
    feature_certification: Option<PathBuf>,
    fleet_trust_policy: Option<PathBuf>,
    fleet_activation_bundle: Option<PathBuf>,
    previous_manifest: Option<PathBuf>,
    ha_trust_policy: Option<PathBuf>,
    ha_writer_epoch: Option<PathBuf>,
    ha_replica_lease: Option<PathBuf>,
    ha_previous_writer_epoch: Option<PathBuf>,
    ha_previous_primary_lease: Option<PathBuf>,
    ha_replica_signing_key: Option<PathBuf>,
    device_trust_policy: Option<PathBuf>,
    device_exporter_key_id: Option<String>,
    device_exporter_signing_key: Option<PathBuf>,
    grant_trust_policy: Option<PathBuf>,
    grant_exporter_key_id: Option<String>,
    grant_exporter_signing_key: Option<PathBuf>,
    grant_recipient_key_id: Option<String>,
    grant_recipient_private_key: Option<PathBuf>,
    admission_trust_policy: Option<PathBuf>,
    admission_evidence_bundle: Option<PathBuf>,
    deployment_isolation_tier: Option<String>,
    http_listen: Option<SocketAddr>,
    http_tls_cert: Option<PathBuf>,
    http_tls_key: Option<PathBuf>,
    check: bool,
    json_output: bool,
) -> Result<()> {
    if check && http_listen.is_some() {
        bail!("--check cannot be combined with --http-listen");
    }
    let tls = match (http_tls_cert, http_tls_key) {
        (Some(certificate), Some(private_key)) => Some((certificate, private_key)),
        (None, None) => None,
        _ => bail!("--http-tls-cert and --http-tls-key must be supplied together"),
    };
    if http_listen.is_none() && tls.is_some() {
        bail!("cell application TLS credentials require --http-listen");
    }
    if http_listen.is_some_and(|address| !address.ip().is_loopback() && tls.is_none()) {
        bail!("a non-loopback cell application listener requires TLS");
    }
    let trusted = parse_trusted_key_specs(&trusted_key_specs)?;
    let expected_cell_id = CellId::parse(expected_cell_id)?;
    let expected_guest_image_digest = Sha256Digest::parse(guest_image_digest)?;
    let verified = load_verified_manifest(&manifest, &trusted)?;
    if http_listen.is_some() && verified.manifest.applications.is_empty() {
        bail!("--http-listen requires applications pinned in the CellManifest");
    }
    if http_listen.is_some()
        && verified.manifest.uses_cell_ha()
        && verified.manifest.replication.role != CellReplicaRole::Primary
    {
        bail!("a standby/recovery Cell cannot construct an application listener");
    }
    let key_provider: Box<dyn CellKeyProvider> = if verified.manifest.uses_bound_cryptography() {
        let fd = key_lease_fd.ok_or_else(|| {
            anyhow::anyhow!("bound-cryptography manifests require --key-lease-fd")
        })?;
        if key_file.is_some() {
            bail!("bound-cryptography manifests refuse --key-file");
        }
        let nonce = attestation_nonce.ok_or_else(|| {
            anyhow::anyhow!("bound-cryptography manifests require --attestation-nonce")
        })?;
        let kms_keys = parse_trusted_key_specs(&kms_trusted_key_specs)?;
        Box::new(AttestedKeyLeaseCellKeyProvider::new(
            open_inherited_key_lease_reader(fd)?,
            kms_keys,
            nonce,
        )?)
    } else {
        if key_lease_fd.is_some()
            || !kms_trusted_key_specs.is_empty()
            || attestation_nonce.is_some()
        {
            bail!("Phase-1 manifests accept only --key-file, not attested lease options");
        }
        let key_file =
            key_file.ok_or_else(|| anyhow::anyhow!("Phase-1 manifests require --key-file"))?;
        Box::new(DevelopmentFileCellKeyProvider::new(
            key_file,
            expected_cell_id.clone(),
            verified.manifest.keys.cell_kek_id.clone(),
        ))
    };
    let application_host = match (
        release_policy,
        identity_policy,
        egress_policy,
        authorization_policy,
        feature_certification,
        fleet_trust_policy,
        fleet_activation_bundle,
        previous_manifest,
    ) {
        (
            Some(release_policy_path),
            Some(identity_policy_path),
            Some(egress_policy_path),
            authorization_policy_path,
            feature_certification_path,
            fleet_trust_policy_path,
            fleet_activation_bundle_path,
            previous_manifest_path,
        ) if authorization_policy_path.is_some() == feature_certification_path.is_some()
            && fleet_trust_policy_path.is_some() == fleet_activation_bundle_path.is_some()
            && (previous_manifest_path.is_none() || fleet_trust_policy_path.is_some()) =>
        {
            Some(CellApplicationHostConfig {
                release_policy_path,
                identity_policy_path,
                egress_policy_path,
                authorization_policy_path,
                feature_certification_path,
                fleet_trust_policy_path,
                fleet_activation_bundle_path,
                previous_manifest_path,
            })
        }
        (None, None, None, None, None, None, None, None) => None,
        _ => bail!(
            "release/identity/egress policies must be supplied together; authorization/feature-certification and fleet-policy/activation-bundle are required pairs"
        ),
    };
    let ha = match (
        ha_trust_policy,
        ha_writer_epoch,
        ha_replica_lease,
        ha_previous_writer_epoch,
        ha_previous_primary_lease,
        ha_replica_signing_key,
    ) {
        (
            Some(trust_policy_path),
            Some(writer_epoch_path),
            Some(replica_lease_path),
            previous_writer_epoch_path,
            previous_primary_lease_path,
            Some(replica_signing_key_path),
        ) if previous_writer_epoch_path.is_some() == previous_primary_lease_path.is_some() => {
            Some(CellHaRuntimeConfig {
                trust_policy_path,
                writer_epoch_path,
                replica_lease_path,
                previous_writer_epoch_path,
                previous_primary_lease_path,
                replica_signing_key_path,
            })
        }
        (None, None, None, None, None, None) => None,
        _ => bail!(
            "Phase-5 HA policy, epoch, replica lease, and replica signing key are required together; predecessor epoch/lease are an optional pair"
        ),
    };
    let device = match (
        device_trust_policy,
        device_exporter_key_id,
        device_exporter_signing_key,
    ) {
        (Some(trust_policy_path), Some(exporter_key_id), Some(exporter_signing_key_path)) => {
            Some(CellDeviceRuntimeConfig {
                trust_policy_path,
                exporter_key_id,
                exporter_signing_key_path,
            })
        }
        (None, None, None) => None,
        _ => bail!(
            "Phase-6 device trust policy, exporter key id, and exporter signing key are required together"
        ),
    };
    let grant = match (
        grant_trust_policy,
        grant_exporter_key_id,
        grant_exporter_signing_key,
        grant_recipient_key_id,
        grant_recipient_private_key,
    ) {
        (
            Some(trust_policy_path),
            Some(exporter_key_id),
            Some(exporter_signing_key_path),
            Some(recipient_key_id),
            Some(recipient_private_key_path),
        ) => Some(CellGrantRuntimeConfig {
            trust_policy_path,
            exporter_key_id,
            exporter_signing_key_path,
            recipient_key_id,
            recipient_private_key_path,
        }),
        (None, None, None, None, None) => None,
        _ => bail!(
            "Phase-7 grant trust policy, exporter key id/signing key, and recipient key id/private key are required together"
        ),
    };
    let admission = match (
        admission_trust_policy,
        admission_evidence_bundle,
        deployment_isolation_tier,
    ) {
        (
            Some(trust_policy_path),
            Some(evidence_bundle_path),
            Some(expected_deployment_isolation_tier),
        ) => Some(CellAdmissionRuntimeConfig {
            trust_policy_path,
            evidence_bundle_path,
            expected_deployment_isolation_tier,
        }),
        (None, None, None) => None,
        _ => bail!(
            "Phase-8 admission trust policy, evidence bundle, and deployment isolation tier are required together"
        ),
    };
    let runtime = CellRuntime::open(CellRuntimeConfig {
        manifest_path: manifest,
        volume_path: volume,
        expected_cell_id,
        expected_volume_id,
        expected_guest_image_digest,
        artifact_root,
        trusted_manifest_keys: trusted,
        key_provider,
        application_host,
        ha,
        device,
        grant,
        admission,
    })?;
    if check {
        print_cell_runtime(&runtime, None, json_output)?;
        runtime.close()?;
        return Ok(());
    }
    let _admission_watchdog = runtime.start_admission_watchdog()?;
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_for_handler = std::sync::Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        shutdown_for_handler.store(true, std::sync::atomic::Ordering::SeqCst)
    })?;
    if let Some(address) = http_listen {
        let host = runtime
            .application_host()
            .expect("application pins were validated before cell construction");
        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        async_runtime.block_on(async {
            let server = match tls {
                Some((certificate, private_key)) => {
                    host.serve_http_tls(
                        address,
                        certificate,
                        private_key,
                        HttpHostPolicy::default(),
                    )
                    .await?
                }
                None => {
                    let listener = tokio::net::TcpListener::bind(address).await?;
                    host.serve_http(listener, HttpHostPolicy::default()).await?
                }
            };
            print_cell_runtime(&runtime, Some(server.address), json_output)?;
            while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            server.shutdown().await.map_err(anyhow::Error::from)
        })?;
    } else {
        print_cell_runtime(&runtime, None, json_output)?;
        while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    runtime.close()?;
    Ok(())
}

fn print_cell_runtime(
    runtime: &CellRuntime,
    listener: Option<SocketAddr>,
    json_output: bool,
) -> Result<()> {
    let report = runtime.admission_report();
    let readiness = runtime.application_host().map(|host| host.readiness());
    let ha_status = runtime.ha_status();
    let grant_policy = runtime.grant_trust_policy();
    let admission_evidence = runtime.verified_admission_evidence();
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "runtime": "BicDB Cell Runtime",
                "cell_id": runtime.manifest().cell_id.as_str(),
                "volume_id": runtime.manifest().storage.volume_id,
                "manifest_generation": runtime.manifest().manifest_generation,
                "manifest_digest": runtime.manifest_digest().as_str(),
                "application_count": runtime.manifest().applications.len(),
                "application_readiness": readiness,
                "key_provider": runtime.key_provider_kind(),
                "application_listener": listener.map(|address| address.to_string()),
                "pgwire_listener": "not_constructed",
                "remote_cell_handle": "not_constructed",
                "object_grant_policy": grant_policy.map(|policy| policy.policy_id.as_str()),
                "admission_evidence_complete": admission_evidence.map(|evidence| evidence.evidence_complete),
                "admission_checkpoint_sequence": admission_evidence.map(|evidence| evidence.checkpoint_sequence),
                "admission_authorization_expires_at": admission_evidence.map(|evidence| evidence.authorization_expires_at),
                "ha": ha_status,
                "regulated_data_admission": report,
            }))?
        );
    } else {
        println!("BicDB Cell Runtime");
        println!("Cell ID             {}", runtime.manifest().cell_id);
        println!(
            "Volume ID           {}",
            runtime.manifest().storage.volume_id
        );
        println!(
            "Manifest generation {}",
            runtime.manifest().manifest_generation
        );
        println!(
            "Application pins    {}",
            runtime.manifest().applications.len()
        );
        println!(
            "Application host    {}",
            readiness
                .map(|readiness| if readiness.ready {
                    "READY"
                } else {
                    "NOT READY"
                })
                .unwrap_or("NOT CONSTRUCTED")
        );
        println!(
            "Application listener {}",
            listener
                .map(|address| address.to_string())
                .unwrap_or_else(|| "DISABLED".to_string())
        );
        println!("Pgwire listener     NOT CONSTRUCTED");
        println!("Remote Cell handle  NOT CONSTRUCTED");
        println!(
            "Object grants       {}",
            grant_policy
                .map(|policy| format!("READY ({})", policy.policy_id))
                .unwrap_or_else(|| "NOT CONSTRUCTED".to_string())
        );
        println!(
            "Admission evidence  {}",
            admission_evidence
                .map(|evidence| format!(
                    "VERIFIED (12/12; checkpoint {}; authorization expires {})",
                    evidence.checkpoint_sequence, evidence.authorization_expires_at
                ))
                .unwrap_or_else(|| "NOT CONSTRUCTED".to_string())
        );
        println!(
            "Cell HA              {}",
            ha_status
                .map(|status| format!(
                    "{:?} epoch {} lease {} expires {}{}",
                    status.role,
                    status.writer_epoch,
                    status.lease_sequence,
                    status.expires_at,
                    if status.revoked { " FENCED" } else { "" }
                ))
                .unwrap_or_else(|| "NOT CONSTRUCTED".to_string())
        );
        println!(
            "Regulated-data admission  {} ({})",
            if report.regulated_data_admitted {
                "ADMITTED"
            } else {
                "DENIED"
            },
            report.phase
        );
    }
    Ok(())
}

fn run_cell_command(command: CellCommand) -> Result<()> {
    match command {
        CellCommand::Serve {
            manifest,
            volume,
            expected_cell_id,
            expected_volume_id,
            guest_image_digest,
            trusted_keys,
            key_file,
            key_lease_fd,
            kms_trusted_keys,
            attestation_nonce,
            artifact_root,
            release_policy,
            identity_policy,
            egress_policy,
            authorization_policy,
            feature_certification,
            fleet_trust_policy,
            fleet_activation_bundle,
            previous_manifest,
            ha_trust_policy,
            ha_writer_epoch,
            ha_replica_lease,
            ha_previous_writer_epoch,
            ha_previous_primary_lease,
            ha_replica_signing_key,
            device_trust_policy,
            device_exporter_key_id,
            device_exporter_signing_key,
            grant_trust_policy,
            grant_exporter_key_id,
            grant_exporter_signing_key,
            grant_recipient_key_id,
            grant_recipient_private_key,
            admission_trust_policy,
            admission_evidence_bundle,
            deployment_isolation_tier,
            http_listen,
            http_tls_cert,
            http_tls_key,
            check,
            json,
        } => run_cell_runtime(
            manifest,
            volume,
            expected_cell_id,
            expected_volume_id,
            guest_image_digest,
            trusted_keys,
            key_file,
            key_lease_fd,
            kms_trusted_keys,
            attestation_nonce,
            artifact_root,
            release_policy,
            identity_policy,
            egress_policy,
            authorization_policy,
            feature_certification,
            fleet_trust_policy,
            fleet_activation_bundle,
            previous_manifest,
            ha_trust_policy,
            ha_writer_epoch,
            ha_replica_lease,
            ha_previous_writer_epoch,
            ha_previous_primary_lease,
            ha_replica_signing_key,
            device_trust_policy,
            device_exporter_key_id,
            device_exporter_signing_key,
            grant_trust_policy,
            grant_exporter_key_id,
            grant_exporter_signing_key,
            grant_recipient_key_id,
            grant_recipient_private_key,
            admission_trust_policy,
            admission_evidence_bundle,
            deployment_isolation_tier,
            http_listen,
            http_tls_cert,
            http_tls_key,
            check,
            json,
        ),
        CellCommand::Verify {
            manifest,
            trusted_keys,
            json,
        } => {
            let trusted = parse_trusted_key_specs(&trusted_keys)?;
            let verified = load_verified_manifest(&manifest, &trusted)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "cell_id": verified.manifest.cell_id.as_str(),
                        "manifest_generation": verified.manifest.manifest_generation,
                        "manifest_digest": verified.digest.as_str(),
                        "signer_key_id": verified.signer_key_id,
                        "regulated_data_admitted": false,
                    }))?
                );
            } else {
                println!(
                    "Verified Cell {} generation {} ({})",
                    verified.manifest.cell_id,
                    verified.manifest.manifest_generation,
                    verified.digest
                );
                println!("Regulated-data admission: DENIED (manifest verification only)");
            }
            Ok(())
        }
        CellCommand::RotateKey {
            current_manifest,
            next_manifest,
            volume,
            expected_cell_id,
            expected_volume_id,
            guest_image_digest,
            trusted_keys,
            current_key_lease_fd,
            current_attestation_nonce,
            next_key_lease_fd,
            next_attestation_nonce,
            kms_trusted_keys,
            json,
        } => {
            if current_key_lease_fd == next_key_lease_fd {
                bail!("current and next key leases require distinct inherited pipes");
            }
            let trusted_manifest_keys = parse_trusted_key_specs(&trusted_keys)?;
            let trusted_kms_keys = parse_trusted_key_specs(&kms_trusted_keys)?;
            let current_provider = AttestedKeyLeaseCellKeyProvider::new(
                open_inherited_key_lease_reader(current_key_lease_fd)?,
                trusted_kms_keys.clone(),
                current_attestation_nonce,
            )?;
            let next_provider = AttestedKeyLeaseCellKeyProvider::new(
                open_inherited_key_lease_reader(next_key_lease_fd)?,
                trusted_kms_keys,
                next_attestation_nonce,
            )?;
            let report = rotate_cell_key(CellKeyRotationConfig {
                current_manifest_path: current_manifest,
                next_manifest_path: next_manifest,
                volume_path: volume,
                expected_cell_id: CellId::parse(expected_cell_id)?,
                expected_volume_id,
                expected_guest_image_digest: Sha256Digest::parse(guest_image_digest)?,
                trusted_manifest_keys,
                current_key_provider: Box::new(current_provider),
                next_key_provider: Box::new(next_provider),
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("BicDB Cell Key Rotation");
                println!("Cell ID             {}", report.cell_id);
                println!("Volume ID           {}", report.volume_id);
                println!(
                    "Manifest generation {} -> {}",
                    report.previous_manifest_generation, report.active_manifest_generation
                );
                println!(
                    "Key epoch           {} -> {}",
                    report.storage.old_key_epoch, report.storage.new_key_epoch
                );
                println!("Storage activated   {}", report.storage.activated);
                println!(
                    "Retired ciphertext  {}",
                    report.storage.retired_path.display()
                );
                println!("Regulated-data admission  DENIED");
            }
            Ok(())
        }
    }
}

fn run_cell_serve_from_environment() -> Result<()> {
    let trusted_keys = required_cell_env("BICDB_CELL_TRUSTED_KEYS")?
        .split(';')
        .filter(|entry| !entry.trim().is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    run_cell_runtime(
        PathBuf::from(required_cell_env("BICDB_CELL_MANIFEST")?),
        PathBuf::from(required_cell_env("BICDB_CELL_VOLUME")?),
        required_cell_env("BICDB_CELL_ID")?,
        required_cell_env("BICDB_CELL_VOLUME_ID")?,
        required_cell_env("BICDB_CELL_GUEST_IMAGE_DIGEST")?,
        trusted_keys,
        optional_cell_env_path("BICDB_CELL_KEY_FILE"),
        optional_cell_env("BICDB_CELL_KEY_LEASE_FD")
            .map(|value| value.parse::<i32>())
            .transpose()
            .map_err(|error| anyhow::anyhow!("BICDB_CELL_KEY_LEASE_FD is invalid: {error}"))?,
        optional_cell_env("BICDB_CELL_KMS_TRUSTED_KEYS")
            .map(|value| {
                value
                    .split(';')
                    .filter(|entry| !entry.trim().is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        optional_cell_env("BICDB_CELL_ATTESTATION_NONCE"),
        PathBuf::from(required_cell_env("BICDB_CELL_ARTIFACT_ROOT")?),
        optional_cell_env_path("BICDB_CELL_RELEASE_POLICY"),
        optional_cell_env_path("BICDB_CELL_IDENTITY_POLICY"),
        optional_cell_env_path("BICDB_CELL_EGRESS_POLICY"),
        optional_cell_env_path("BICDB_CELL_AUTHORIZATION_POLICY"),
        optional_cell_env_path("BICDB_CELL_FEATURE_CERTIFICATION"),
        optional_cell_env_path("BICDB_CELL_FLEET_TRUST_POLICY"),
        optional_cell_env_path("BICDB_CELL_FLEET_ACTIVATION_BUNDLE"),
        optional_cell_env_path("BICDB_CELL_PREVIOUS_MANIFEST"),
        optional_cell_env_path("BICDB_CELL_HA_TRUST_POLICY"),
        optional_cell_env_path("BICDB_CELL_HA_WRITER_EPOCH"),
        optional_cell_env_path("BICDB_CELL_HA_REPLICA_LEASE"),
        optional_cell_env_path("BICDB_CELL_HA_PREVIOUS_WRITER_EPOCH"),
        optional_cell_env_path("BICDB_CELL_HA_PREVIOUS_PRIMARY_LEASE"),
        optional_cell_env_path("BICDB_CELL_HA_REPLICA_SIGNING_KEY"),
        optional_cell_env_path("BICDB_CELL_DEVICE_TRUST_POLICY"),
        optional_cell_env("BICDB_CELL_DEVICE_EXPORTER_KEY_ID"),
        optional_cell_env_path("BICDB_CELL_DEVICE_EXPORTER_SIGNING_KEY"),
        optional_cell_env_path("BICDB_CELL_GRANT_TRUST_POLICY"),
        optional_cell_env("BICDB_CELL_GRANT_EXPORTER_KEY_ID"),
        optional_cell_env_path("BICDB_CELL_GRANT_EXPORTER_SIGNING_KEY"),
        optional_cell_env("BICDB_CELL_GRANT_RECIPIENT_KEY_ID"),
        optional_cell_env_path("BICDB_CELL_GRANT_RECIPIENT_PRIVATE_KEY"),
        optional_cell_env_path("BICDB_CELL_ADMISSION_TRUST_POLICY"),
        optional_cell_env_path("BICDB_CELL_ADMISSION_EVIDENCE_BUNDLE"),
        optional_cell_env("BICDB_CELL_DEPLOYMENT_ISOLATION_TIER"),
        optional_cell_env("BICDB_CELL_HTTP_LISTEN")
            .map(|value| value.parse::<SocketAddr>())
            .transpose()
            .map_err(|error| anyhow::anyhow!("BICDB_CELL_HTTP_LISTEN is invalid: {error}"))?,
        optional_cell_env_path("BICDB_CELL_HTTP_TLS_CERT"),
        optional_cell_env_path("BICDB_CELL_HTTP_TLS_KEY"),
        env_flag("BICDB_CELL_CHECK")?,
        env_flag("BICDB_CELL_JSON")?,
    )
}

fn required_cell_env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("{name} is required in cell runtime mode"))
}

fn optional_cell_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn optional_cell_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_flag(name: &str) -> Result<bool> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(false),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{name} must be valid UTF-8 and a boolean")
        }
    };
    parse_env_flag_value(name, &value)
}

fn parse_env_flag_value(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{name} must be one of 1, true, yes, on, 0, false, no, or off"),
    }
}

fn print_server_status(path: PathBuf) -> Result<()> {
    let db = BicDb::open(&path)?;
    let stats = db.stats()?;
    let ha = db.ha_status()?;
    println!("BicDB Server Status");
    println!("-------------------");
    println!("Path: {}", stats.path.display());
    println!("Collections: {}", stats.collection_count);
    println!("Records: {}", stats.record_count);
    println!("Database size: {} bytes", stats.size_bytes);
    println!("Pending sync ops: {}", stats.pending_sync_ops);
    println!("HA role: {:?}", ha.role);
    println!("HA read-only: {}", ha.read_only);
    println!("HA lag bytes: {}", ha.lag_bytes);
    println!(
        "HA durable checkpoint bytes: {}",
        ha.last_durable_checkpoint_bytes
    );
    println!(
        "Live connection stats are available from a running server with: SELECT * FROM bicdb_server_stats; SELECT * FROM bicdb_ha_status;"
    );
    Ok(())
}

fn print_ha_status(status: &HaStatus, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(status)?);
        return Ok(());
    }
    println!("BicDB HA Status");
    println!("---------------");
    println!("Role: {:?}", status.role);
    println!("Read-only: {}", status.read_only);
    println!("Ready: {}", status.ready);
    if let Some(source) = &status.source_path {
        println!("Source: {}", source.display());
    }
    println!(
        "Checkpoint bytes: {} -> {}",
        status.source_checkpoint_bytes, status.applied_checkpoint_bytes
    );
    println!("Lag bytes: {}", status.lag_bytes);
    println!(
        "Last durable checkpoint bytes: {}",
        status.last_durable_checkpoint_bytes
    );
    if let Some(error) = &status.last_apply_error {
        println!("Last apply error: {error}");
    }
    Ok(())
}

fn print_replication_status(db: &BicDb, json: bool) -> Result<()> {
    let watermark = db.replication_watermark();
    let apply_state = db.replication_apply_state();
    let retention = db.replication_retention_status()?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "watermark": watermark,
                "apply_state": apply_state,
                "retention": retention,
                "lag_commits": db.replication_lag(watermark.current_commit_seq),
            }))?
        );
        return Ok(());
    }
    println!("BicDB Streaming Replication Status");
    println!("----------------------------------");
    println!("Cluster: {}", apply_state.cluster_id);
    println!(
        "Source node: {}",
        apply_state.source_node_id.as_deref().unwrap_or("<none>")
    );
    println!(
        "Stream: {}",
        apply_state.stream_id.as_deref().unwrap_or("<none>")
    );
    println!("Current commit_seq: {}", watermark.current_commit_seq);
    println!(
        "Last applied commit_seq: {}",
        watermark.last_applied_commit_seq
    );
    if let Some(last_applied_at) = apply_state.last_applied_at {
        println!("Last applied at: {last_applied_at}");
    }
    if let Some(error) = &apply_state.last_error {
        println!("Last replication error: {error}");
    }
    println!(
        "Lag commits: {}",
        db.replication_lag(watermark.current_commit_seq)
    );
    println!(
        "Retained commit_seq range: {}..={}",
        retention.oldest_available_commit_seq, retention.newest_available_commit_seq
    );
    println!("Retained commits: {}", retention.retained_commits);
    println!("Retained WAL bytes: {}", retention.retained_bytes);
    Ok(())
}

fn print_consensus_status(db: &BicDb, json: bool) -> Result<()> {
    let status = db.consensus_status();
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }
    println!("BicDB Consensus Status");
    println!("----------------------");
    println!("Cluster: {}", status.cluster_id);
    println!("Node: {}", status.node_id);
    println!("Role: {:?}", status.role);
    println!("Current term: {}", status.current_term);
    println!(
        "Voted for: {}",
        status.voted_for.as_deref().unwrap_or("<none>")
    );
    println!(
        "Leader: {}",
        status.leader_id.as_deref().unwrap_or("<none>")
    );
    println!("Commit index: {}", status.commit_index);
    println!("Last applied: {}", status.last_applied);
    println!("Last log index: {}", status.last_log_index);
    println!("Last log term: {}", status.last_log_term);
    println!("Voters: {}", status.voters.join(", "));
    Ok(())
}

fn consensus_run_loop(
    path: PathBuf,
    listen: String,
    cluster_id: String,
    node_id: String,
    peers: Vec<String>,
    dev_localhost_plaintext: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
    max_frame_bytes: usize,
    pg_listen: Option<String>,
) -> Result<()> {
    let (consensus_peers, peer_certificate_pins) =
        parse_consensus_peers_with_pins(&node_id, &listen, peers)?;
    let config = ConsensusConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        peers: consensus_peers.clone(),
        election_timeout_ms,
        heartbeat_interval_ms,
        lease_timeout_ms: election_timeout_ms.saturating_mul(2),
    };
    let replication_identity = ReplicationConfig {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        ..ReplicationConfig::default()
    };
    let db = BicDb::open_with_config(
        &path,
        DbConfig::default()
            .with_consensus(config.clone())
            .with_replication(replication_identity),
    )?;
    let db = std::sync::Arc::new(parking_lot::RwLock::new(db));

    // Serve SQL for this node over the same handle the consensus loop
    // replicates. Sharing is mandatory rather than an optimisation: one
    // database directory has one writer, so a separate `serve-pg` process on
    // this path would (correctly) be refused.
    if let Some(address) = pg_listen.clone() {
        let pg_db = db.clone();
        let pg_path = path.clone();
        let listener = TcpListener::bind(&address)?;
        let bound = listener.local_addr()?;
        let mut pg_config = PgWireConfig::default();
        if let Some((host, port)) = address.rsplit_once(':') {
            pg_config.host = host.to_string();
            pg_config.port = port.parse().unwrap_or(bound.port());
        }
        let server = bicdb_pgwire::PgWireServer::open_with_shared(&pg_path, pg_config, pg_db)?;
        println!("bicdb consensus node {node_id} serving pgwire on {bound}");
        std::thread::spawn(move || {
            if let Err(error) = bicdb_pgwire::serve_existing_listener(server, listener) {
                eprintln!("pgwire listener for consensus node exited: {error}");
            }
        });
    }

    let last_leader_contact_ms =
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(unix_now_millis()));
    let listener = TcpListener::bind(&listen)?;
    let bound = listener.local_addr()?;
    if dev_localhost_plaintext && !bound.ip().is_loopback() {
        bail!("--dev-localhost-plaintext consensus run must bind to loopback");
    }
    println!(
        "bicdb consensus node {node_id} listening on {bound} cluster={cluster_id} voters={}",
        consensus_peers.len()
    );

    let inbound_db = db.clone();
    let inbound_tls = if dev_localhost_plaintext {
        None
    } else {
        Some(replication_transport::build_server_tls(
            &replication_tls_config_from_paths(tls_cert.clone(), tls_key.clone(), tls_ca.clone())?,
        )?)
    };
    let inbound_last_leader_contact_ms = last_leader_contact_ms.clone();
    std::thread::spawn(move || {
        for accepted in listener.incoming() {
            match accepted {
                Ok(stream) => {
                    let peer = stream.peer_addr().ok();
                    if dev_localhost_plaintext && peer.is_some_and(|addr| !addr.ip().is_loopback())
                    {
                        eprintln!("consensus rejected non-loopback plaintext peer {peer:?}");
                        continue;
                    }
                    let db = inbound_db.clone();
                    let last_leader_contact_ms = inbound_last_leader_contact_ms.clone();
                    let tls = inbound_tls.as_ref().map(|tls| tls.config.clone());
                    let pins = peer_certificate_pins.clone();
                    std::thread::spawn(move || {
                        let result = if let Some(tls_config) = tls {
                            match replication_transport::server_tls_stream(tls_config, stream) {
                                Ok(mut stream) => {
                                    replication_transport::peer_certificate_sha256(&mut stream)
                                        .map_err(anyhow::Error::from)
                                        .and_then(|presented| {
                                            consensus_authenticated_node(
                                                &pins,
                                                Some(presented.as_str()),
                                            )
                                        })
                                        .and_then(|authenticated| {
                                            consensus_handle_connection(
                                                &db,
                                                &last_leader_contact_ms,
                                                &mut stream,
                                                max_frame_bytes,
                                                authenticated.as_deref(),
                                            )
                                        })
                                }
                                Err(error) => Err(error.into()),
                            }
                        } else {
                            let mut stream = stream;
                            consensus_handle_connection(
                                &db,
                                &last_leader_contact_ms,
                                &mut stream,
                                max_frame_bytes,
                                None,
                            )
                        };
                        if let Err(error) = result {
                            eprintln!("consensus inbound error: {error}");
                        }
                    });
                }
                Err(error) => eprintln!("consensus accept error: {error}"),
            }
        }
    });

    let client_tls = if dev_localhost_plaintext {
        None
    } else {
        Some(replication_transport::build_client_tls(
            &replication_tls_config_from_paths(tls_cert, tls_key, tls_ca)?,
        )?)
    };
    let mut last_heartbeat = std::time::Instant::now();
    let mut last_election = std::time::Instant::now();
    let mut proposed_through_commit_seq = db.write().current_commit_seq();
    loop {
        std::thread::sleep(Duration::from_millis(heartbeat_interval_ms.max(1)));
        let status = db.write().consensus_status();
        match status.role {
            ConsensusRole::Leader => {
                proposed_through_commit_seq = consensus_propose_local_frames(
                    &db,
                    &config,
                    proposed_through_commit_seq,
                    max_frame_bytes,
                    dev_localhost_plaintext,
                    client_tls.as_ref(),
                )
                .unwrap_or_else(|error| {
                    eprintln!("consensus propose error: {error}");
                    proposed_through_commit_seq
                });
                consensus_send_append_entries(
                    &db,
                    &config,
                    max_frame_bytes,
                    dev_localhost_plaintext,
                    client_tls.as_ref(),
                );
                last_heartbeat = std::time::Instant::now();
            }
            ConsensusRole::Follower | ConsensusRole::Candidate => {
                let last_contact_ms =
                    last_leader_contact_ms.load(std::sync::atomic::Ordering::SeqCst);
                let leader_contact_age_ms = unix_now_millis().saturating_sub(last_contact_ms);
                if leader_contact_age_ms >= election_timeout_ms
                    && last_election.elapsed() >= Duration::from_millis(election_timeout_ms.max(1))
                {
                    if let Err(error) = consensus_start_election_round(
                        &db,
                        &config,
                        max_frame_bytes,
                        dev_localhost_plaintext,
                        client_tls.as_ref(),
                    ) {
                        eprintln!("consensus election error: {error}");
                    }
                    last_election = std::time::Instant::now();
                }
                if last_heartbeat.elapsed() >= Duration::from_millis(election_timeout_ms.max(1)) {
                    last_heartbeat = std::time::Instant::now();
                }
            }
        }
    }
}

fn parse_consensus_peers(
    node_id: &str,
    listen: &str,
    peers: Vec<String>,
) -> Result<Vec<ConsensusPeer>> {
    Ok(parse_consensus_peers_with_pins(node_id, listen, peers)?.0)
}

/// `--peer node_id=addr` or `--peer node_id=addr=SHA256`.
///
/// The optional third field pins the SHA-256 of the leaf certificate that
/// voter must present, exactly as `--allow-node NODE_ID=SHA256` does for
/// replication. It exists because a Raft frame's `candidate_id` / `leader_id`
/// is self-asserted: mTLS proves the peer holds a certificate the CA signed,
/// never WHICH voter it is, so without a pin one compromised voter can vote or
/// append as any other.
fn parse_consensus_peers_with_pins(
    node_id: &str,
    listen: &str,
    peers: Vec<String>,
) -> Result<(Vec<ConsensusPeer>, BTreeMap<String, String>)> {
    let mut parsed = Vec::new();
    let mut pins: BTreeMap<String, String> = BTreeMap::new();
    let mut saw_local = false;
    for peer in peers {
        let mut fields = peer.splitn(3, '=');
        let (Some(peer_node), Some(address)) = (fields.next(), fields.next()) else {
            bail!("consensus --peer must use node_id=addr[=sha256], got {peer}");
        };
        if peer_node.is_empty() || address.is_empty() {
            bail!("consensus --peer must use node_id=addr[=sha256], got {peer}");
        }
        if let Some(fingerprint) = fields.next() {
            let fingerprint = fingerprint.trim().to_ascii_lowercase();
            if fingerprint.len() != 64 || !fingerprint.chars().all(|c| c.is_ascii_hexdigit()) {
                bail!(
                    "consensus --peer certificate pin must be 64 hex characters, got {fingerprint}"
                );
            }
            if let Some(existing) = pins.insert(fingerprint.clone(), peer_node.to_string()) {
                if existing != peer_node {
                    bail!(
                        "consensus --peer certificate {fingerprint} is pinned to both \
                         {existing} and {peer_node}"
                    );
                }
            }
        }
        if peer_node == node_id {
            saw_local = true;
        }
        parsed.push(ConsensusPeer::voting(peer_node, address));
    }
    if !saw_local {
        parsed.push(ConsensusPeer::voting(node_id, listen));
    }
    Ok((parsed, pins))
}

/// Bind a consensus frame's self-asserted identity to the authenticated peer.
///
/// `handle_request_vote` only checks that `candidate_id` is a known voter and
/// `handle_append_entries` trusts `leader_id` outright, so any voter holding a
/// valid certificate could assert any other voter's identity — vote as them, or
/// append as leader. The new cluster consensus already rejects this
/// (`MetadataVote`/`MetadataAppend` require `candidate_id`/`leader_id` to equal
/// the caller); this is the same invariant for the original HA path.
fn consensus_authenticated_node(
    pins: &BTreeMap<String, String>,
    presented_certificate_sha256: Option<&str>,
) -> Result<Option<String>> {
    let Some(presented) = presented_certificate_sha256 else {
        // Plaintext dev mode: there is no certificate to bind to, and the
        // listener already refuses non-loopback peers.
        return Ok(None);
    };
    if pins.is_empty() {
        return Ok(None);
    }
    let presented = presented.to_ascii_lowercase();
    pins.get(&presented).cloned().map(Some).ok_or_else(|| {
        anyhow::anyhow!("consensus peer certificate {presented} is not pinned to any voter")
    })
}

fn consensus_handle_connection<S: std::io::Read + std::io::Write>(
    db: &std::sync::Arc<parking_lot::RwLock<BicDb>>,
    last_leader_contact_ms: &std::sync::Arc<std::sync::atomic::AtomicU64>,
    stream: &mut S,
    max_frame_bytes: usize,
    authenticated_node: Option<&str>,
) -> Result<()> {
    let frame = replication_transport::recv_frame(stream, max_frame_bytes)?;
    let response = {
        let mut db = db.write();
        match frame {
            ReplicationFrame::ConsensusRequestVote(request) => {
                if let Some(peer) = authenticated_node {
                    if request.candidate_id != peer {
                        bail!(
                            "consensus vote from {peer} asserts candidate {}",
                            request.candidate_id
                        );
                    }
                }
                let response = db.consensus_handle_request_vote(request)?;
                if response.vote_granted {
                    last_leader_contact_ms
                        .store(unix_now_millis(), std::sync::atomic::Ordering::SeqCst);
                }
                ReplicationFrame::ConsensusVoteResponse(response)
            }
            ReplicationFrame::ConsensusAppendEntries(request) => {
                if let Some(peer) = authenticated_node {
                    if request.leader_id != peer {
                        bail!(
                            "consensus append from {peer} asserts leader {}",
                            request.leader_id
                        );
                    }
                }
                last_leader_contact_ms
                    .store(unix_now_millis(), std::sync::atomic::Ordering::SeqCst);
                let response = db.consensus_handle_append_entries(request)?;
                let _ = db.consensus_apply_committed();
                ReplicationFrame::ConsensusAppendResponse(response)
            }
            other => ReplicationFrame::Error {
                code: "unexpected_consensus_frame".to_string(),
                message: format!("{other:?}"),
            },
        }
    };
    replication_transport::send_frame(stream, &response, max_frame_bytes)?;
    Ok(())
}

fn consensus_start_election_round(
    db: &std::sync::Arc<parking_lot::RwLock<BicDb>>,
    config: &ConsensusConfig,
    max_frame_bytes: usize,
    dev_localhost_plaintext: bool,
    client_tls: Option<&replication_transport::ReplicationClientTls>,
) -> Result<()> {
    let request = db.write().consensus_start_election()?;
    let mut votes = 1usize;
    for peer in config
        .peers
        .iter()
        .filter(|peer| peer.node_id != config.node_id)
    {
        match consensus_send_frame_to_peer(
            peer,
            ReplicationFrame::ConsensusRequestVote(request.clone()),
            max_frame_bytes,
            dev_localhost_plaintext,
            client_tls,
        ) {
            Ok(ReplicationFrame::ConsensusVoteResponse(response)) if response.vote_granted => {
                votes += 1;
            }
            Ok(_) => {}
            Err(error) => eprintln!("consensus vote request to {} failed: {error}", peer.node_id),
        }
    }
    let voters = config.peers.iter().filter(|peer| peer.voting).count();
    if votes > voters / 2 {
        db.write().consensus_become_leader()?;
        println!(
            "consensus node {} became leader for term {} with {votes}/{voters} votes",
            config.node_id, request.term
        );
    }
    Ok(())
}

fn consensus_propose_local_frames(
    db: &std::sync::Arc<parking_lot::RwLock<BicDb>>,
    config: &ConsensusConfig,
    proposed_through_commit_seq: u64,
    max_frame_bytes: usize,
    dev_localhost_plaintext: bool,
    client_tls: Option<&replication_transport::ReplicationClientTls>,
) -> Result<u64> {
    let frames = {
        let db = db.write();
        db.export_replication_frames_since(proposed_through_commit_seq, 1024)?
    };
    let mut high = proposed_through_commit_seq;
    for frame in frames {
        high = high.max(frame.commit_seq);
        let entry = {
            let mut db = db.write();
            db.consensus_append_local_commit(frame)?
        };
        consensus_send_append_entries(
            db,
            config,
            max_frame_bytes,
            dev_localhost_plaintext,
            client_tls,
        );
        let status = db.write().consensus_status();
        if status.commit_index < entry.index {
            eprintln!(
                "consensus entry {} for commit_seq {} is pending quorum",
                entry.index, entry.commit_seq
            );
        }
    }
    Ok(high)
}

fn consensus_send_append_entries(
    db: &std::sync::Arc<parking_lot::RwLock<BicDb>>,
    config: &ConsensusConfig,
    max_frame_bytes: usize,
    dev_localhost_plaintext: bool,
    client_tls: Option<&replication_transport::ReplicationClientTls>,
) {
    let status = db.write().consensus_status();
    if status.role != ConsensusRole::Leader {
        return;
    }
    for peer in config
        .peers
        .iter()
        .filter(|peer| peer.node_id != config.node_id)
    {
        let request = match db.write().consensus_append_entries_from(1) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("consensus build append entries failed: {error}");
                continue;
            }
        };
        match consensus_send_frame_to_peer(
            peer,
            ReplicationFrame::ConsensusAppendEntries(request),
            max_frame_bytes,
            dev_localhost_plaintext,
            client_tls,
        ) {
            Ok(ReplicationFrame::ConsensusAppendResponse(response)) => {
                if let Err(error) = db
                    .write()
                    .consensus_record_append_response(&peer.node_id, &response)
                {
                    eprintln!(
                        "consensus append response from {} failed: {error}",
                        peer.node_id
                    );
                }
            }
            Ok(other) => eprintln!(
                "unexpected consensus response from {}: {other:?}",
                peer.node_id
            ),
            Err(error) => eprintln!("consensus append to {} failed: {error}", peer.node_id),
        }
    }
}

fn consensus_send_frame_to_peer(
    peer: &ConsensusPeer,
    frame: ReplicationFrame,
    max_frame_bytes: usize,
    dev_localhost_plaintext: bool,
    client_tls: Option<&replication_transport::ReplicationClientTls>,
) -> Result<ReplicationFrame> {
    let stream = TcpStream::connect(&peer.address)?;
    if dev_localhost_plaintext {
        let addr = stream.peer_addr()?;
        if !addr.ip().is_loopback() {
            bail!("plaintext consensus peer {} is not loopback", peer.node_id);
        }
        let mut stream = stream;
        replication_transport::send_frame(&mut stream, &frame, max_frame_bytes)?;
        return Ok(replication_transport::recv_frame(
            &mut stream,
            max_frame_bytes,
        )?);
    }
    let Some(tls) = client_tls else {
        bail!("consensus TLS client config missing");
    };
    let server_name = peer
        .address
        .split_once(':')
        .map(|(host, _)| host)
        .unwrap_or(peer.address.as_str());
    let mut stream =
        replication_transport::client_tls_stream(tls.config.clone(), server_name, stream)?;
    replication_transport::send_frame(&mut stream, &frame, max_frame_bytes)?;
    Ok(replication_transport::recv_frame(
        &mut stream,
        max_frame_bytes,
    )?)
}

fn replication_tls_config_from_paths(
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    ca: Option<PathBuf>,
) -> Result<ReplicationTlsConfig> {
    let (Some(cert_path), Some(key_path), Some(ca_path)) = (cert, key, ca) else {
        bail!("replication TLS requires --tls-cert, --tls-key, and --tls-ca");
    };
    Ok(ReplicationTlsConfig {
        cert_path,
        key_path,
        ca_path,
        require_client_cert: true,
        dev_localhost_plaintext: false,
    })
}

fn replication_stream_listen_once(
    addr: &str,
    path: &Path,
    from: u64,
    limit: usize,
    dev_localhost_plaintext: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    cluster_id: &str,
    node_id: &str,
    allowed_node_ids: &[String],
) -> Result<()> {
    let allowed = parse_allowed_replication_nodes(allowed_node_ids)?;
    let listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    if dev_localhost_plaintext && !bound.ip().is_loopback() {
        bail!("--dev-localhost-plaintext replication stream must bind to loopback");
    }
    eprintln!("replication stream listening once on {bound}");
    let (stream, peer) = listener.accept()?;
    if dev_localhost_plaintext {
        if !peer.ip().is_loopback() {
            bail!("plaintext replication peer must be loopback");
        }
        let mut stream = stream;
        replication_stream_send_for_ack(
            &mut stream,
            path,
            from,
            limit,
            cluster_id,
            node_id,
            &allowed,
            None,
        )?;
        return Ok(());
    }
    let tls = replication_tls_config_from_paths(tls_cert, tls_key, tls_ca)?;
    let server_tls = replication_transport::build_server_tls(&tls)?;
    let mut stream = replication_transport::server_tls_stream(server_tls.config, stream)?;
    let presented = replication_transport::peer_certificate_sha256(&mut stream)?;
    replication_stream_send_for_ack(
        &mut stream,
        path,
        from,
        limit,
        cluster_id,
        node_id,
        &allowed,
        Some(presented.as_str()),
    )?;
    Ok(())
}

/// One authorized replication peer: an identity, optionally pinned to the
/// SHA-256 of the leaf certificate that identity must present.
///
/// Written on the command line as `NODE_ID` or `NODE_ID=SHA256`. The pin is
/// mandatory whenever the connection carries a certificate, because mTLS
/// alone only proves CA membership — never which member.
#[derive(Clone, Debug)]
struct AllowedReplicationNode {
    node_id: String,
    certificate_sha256: Option<String>,
}

fn parse_allowed_replication_nodes(entries: &[String]) -> Result<Vec<AllowedReplicationNode>> {
    entries
        .iter()
        .map(|entry| {
            let (node_id, certificate_sha256) = match entry.split_once('=') {
                Some((node_id, fingerprint)) => {
                    let fingerprint = fingerprint.trim().to_ascii_lowercase();
                    if fingerprint.len() != 64
                        || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
                    {
                        bail!(
                            "--allowed-node-id `{entry}` must pin a 64-character hex SHA-256; \
                             read one with `bicdb replication certificate-fingerprint`"
                        );
                    }
                    (node_id.trim().to_string(), Some(fingerprint))
                }
                None => (entry.trim().to_string(), None),
            };
            if node_id.is_empty() {
                bail!("--allowed-node-id entries must name a node");
            }
            Ok(AllowedReplicationNode {
                node_id,
                certificate_sha256,
            })
        })
        .collect()
}

fn replication_stream_send_for_ack<S: std::io::Read + std::io::Write>(
    stream: &mut S,
    path: &Path,
    from: u64,
    limit: usize,
    cluster_id: &str,
    node_id: &str,
    allowed_node_ids: &[AllowedReplicationNode],
    peer_certificate_sha256: Option<&str>,
) -> Result<()> {
    let follower_node_id = match replication_transport::recv_frame(stream, 1024 * 1024)? {
        ReplicationFrame::Hello {
            protocol_version,
            cluster_id: hello_cluster_id,
            node_id: hello_node_id,
        } => {
            if protocol_version != bicdb_core::replication::REPLICATION_PROTOCOL_VERSION {
                send_replication_error(
                    stream,
                    cluster_id,
                    node_id,
                    "unsupported protocol version",
                )?;
                bail!("unsupported replication protocol version {protocol_version}");
            }
            if hello_cluster_id != cluster_id {
                send_replication_error(stream, cluster_id, node_id, "cluster_id mismatch")?;
                bail!(
                    "replication cluster_id mismatch: expected {cluster_id}, got {hello_cluster_id}"
                );
            }
            // FAIL CLOSED. An empty allow-list used to skip this check
            // entirely, so "no peers configured" meant "every peer
            // allowed" — the classic fail-open footgun, and with
            // `--from 0` a subscriber could pull the database from commit
            // zero.
            if allowed_node_ids.is_empty() {
                send_replication_error(stream, cluster_id, node_id, "no peers are authorized")?;
                bail!(
                    "refusing replication: no --allowed-node-id entries are configured, so no \
                     peer is authorized"
                );
            }
            let Some(allowed) = allowed_node_ids
                .iter()
                .find(|allowed| allowed.node_id == hello_node_id)
            else {
                send_replication_error(stream, cluster_id, node_id, "node is not allowed")?;
                bail!("replication node {hello_node_id} is not in --allowed-node-id");
            };
            // mTLS proves the peer holds a CA-signed key; it never proves
            // WHICH node it is. Bind the claimed identity to the presented
            // leaf certificate, exactly as the cluster data transport does.
            match (&allowed.certificate_sha256, peer_certificate_sha256) {
                (Some(expected), Some(presented)) if expected == presented => {}
                (Some(_), Some(_)) => {
                    send_replication_error(
                        stream,
                        cluster_id,
                        node_id,
                        "certificate does not match node identity",
                    )?;
                    bail!(
                        "replication node {hello_node_id} presented a certificate that is not \
                         pinned to that identity"
                    );
                }
                (Some(_), None) => {
                    send_replication_error(
                        stream,
                        cluster_id,
                        node_id,
                        "client certificate required",
                    )?;
                    bail!(
                        "replication node {hello_node_id} is pinned but presented no certificate"
                    );
                }
                (None, Some(_)) => {
                    send_replication_error(
                        stream,
                        cluster_id,
                        node_id,
                        "node identity is not pinned to a certificate",
                    )?;
                    bail!(
                        "replication node {hello_node_id} has no pinned certificate: pass \
                         --allowed-node-id {hello_node_id}=<sha256> so the identity is bound to \
                         a certificate"
                    );
                }
                // Plaintext localhost development: there is no certificate
                // to bind, and `validate()` already refuses this mode off
                // the loopback interface.
                (None, None) => {}
            }
            hello_node_id
        }
        frame => {
            send_replication_error(stream, cluster_id, node_id, "expected Hello")?;
            bail!("expected replication Hello before streaming, got {frame:?}");
        }
    };
    let requested_from = match replication_transport::recv_frame(stream, 1024 * 1024)? {
        ReplicationFrame::Ack {
            cluster_id: ack_cluster_id,
            node_id: ack_node_id,
            commit_seq,
        } => {
            if ack_cluster_id != cluster_id {
                send_replication_error(stream, cluster_id, node_id, "ack cluster_id mismatch")?;
                bail!(
                    "replication ack cluster_id mismatch: expected {cluster_id}, got {ack_cluster_id}"
                );
            }
            if ack_node_id != follower_node_id {
                send_replication_error(stream, cluster_id, node_id, "ack node_id mismatch")?;
                bail!(
                    "replication ack node_id mismatch: hello={follower_node_id}, ack={ack_node_id}"
                );
            }
            from.max(commit_seq)
        }
        frame => bail!("expected replication Ack before streaming, got {frame:?}"),
    };
    let db = BicDb::open(path)?;
    let frames = db.export_replication_frames_since(requested_from, limit)?;
    replication_transport::send_commit_batch(stream, &frames, 128 * 1024 * 1024)?;
    Ok(())
}

fn replication_follow_db_config(cluster_id: &str, node_id: &str) -> DbConfig {
    DbConfig::default().with_replication(ReplicationConfig {
        mode: ReplicationMode::Standby,
        cluster_id: cluster_id.to_string(),
        node_id: node_id.to_string(),
        ..ReplicationConfig::default()
    })
}

fn send_replication_error<W: std::io::Write>(
    writer: &mut W,
    cluster_id: &str,
    node_id: &str,
    message: &str,
) -> Result<()> {
    replication_transport::send_frame(
        writer,
        &ReplicationFrame::Error {
            code: "auth_failed".to_string(),
            message: format!("{cluster_id}/{node_id}: {message}"),
        },
        1024 * 1024,
    )?;
    Ok(())
}

fn replication_follow_once(
    primary: &str,
    server_name: &str,
    max_frame_bytes: usize,
    dev_localhost_plaintext: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    cluster_id: &str,
    node_id: &str,
    db: &mut BicDb,
) -> Result<bicdb_core::ReplicationApplyReport> {
    let stream = TcpStream::connect(primary)?;
    if dev_localhost_plaintext {
        let peer = stream.peer_addr()?;
        if !peer.ip().is_loopback() {
            bail!("--dev-localhost-plaintext replication follow requires a loopback peer");
        }
        let mut stream = stream;
        send_replication_hello_and_resume_ack(&mut stream, db, cluster_id, node_id)?;
        return receive_and_apply_replication_frames(&mut stream, max_frame_bytes, db);
    }
    let tls = replication_tls_config_from_paths(tls_cert, tls_key, tls_ca)?;
    let client_tls = replication_transport::build_client_tls(&tls)?;
    let mut stream =
        replication_transport::client_tls_stream(client_tls.config, server_name, stream)?;
    send_replication_hello_and_resume_ack(&mut stream, db, cluster_id, node_id)?;
    receive_and_apply_replication_frames(&mut stream, max_frame_bytes, db)
}

fn send_replication_hello_and_resume_ack<W: std::io::Write>(
    writer: &mut W,
    db: &BicDb,
    cluster_id: &str,
    node_id: &str,
) -> Result<()> {
    replication_transport::send_frame(
        writer,
        &ReplicationFrame::Hello {
            protocol_version: bicdb_core::replication::REPLICATION_PROTOCOL_VERSION,
            cluster_id: cluster_id.to_string(),
            node_id: node_id.to_string(),
        },
        1024 * 1024,
    )?;
    replication_transport::send_frame(
        writer,
        &ReplicationFrame::Ack {
            cluster_id: cluster_id.to_string(),
            node_id: node_id.to_string(),
            commit_seq: db.last_applied_commit_seq(),
        },
        1024 * 1024,
    )?;
    Ok(())
}

fn replication_follow_loop(
    primary: &str,
    server_name: &str,
    max_frame_bytes: usize,
    dev_localhost_plaintext: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    reconnect_backoff_ms: u64,
    max_attempts: Option<usize>,
    cluster_id: &str,
    node_id: &str,
    db: &mut BicDb,
) -> Result<bicdb_core::ReplicationApplyReport> {
    let mut attempts = 0usize;
    let mut total = bicdb_core::ReplicationApplyReport {
        applied: 0,
        duplicates: 0,
        last_applied_commit_seq: db.last_applied_commit_seq(),
    };
    loop {
        attempts += 1;
        match replication_follow_once(
            primary,
            server_name,
            max_frame_bytes,
            dev_localhost_plaintext,
            tls_cert.clone(),
            tls_key.clone(),
            tls_ca.clone(),
            cluster_id,
            node_id,
            db,
        ) {
            Ok(report) => {
                total.applied += report.applied;
                total.duplicates += report.duplicates;
                total.last_applied_commit_seq = report.last_applied_commit_seq;
            }
            Err(error) => {
                eprintln!(
                    "replication follow attempt {attempts} failed after commit_seq {}: {error}",
                    db.last_applied_commit_seq()
                );
            }
        }
        if max_attempts.is_some_and(|max| attempts >= max) {
            return Ok(total);
        }
        std::thread::sleep(Duration::from_millis(reconnect_backoff_ms));
    }
}

fn receive_and_apply_replication_frames<R: std::io::Read>(
    reader: &mut R,
    max_frame_bytes: usize,
    db: &mut BicDb,
) -> Result<bicdb_core::ReplicationApplyReport> {
    let mut total = bicdb_core::ReplicationApplyReport {
        applied: 0,
        duplicates: 0,
        last_applied_commit_seq: db.last_applied_commit_seq(),
    };
    loop {
        match replication_transport::recv_frame(reader, max_frame_bytes) {
            Ok(ReplicationFrame::Commit(frame)) => {
                let report = db.apply_replication_frame(&frame)?;
                total.applied += report.applied;
                total.duplicates += report.duplicates;
                total.last_applied_commit_seq = report.last_applied_commit_seq;
            }
            Ok(ReplicationFrame::Error { code, message, .. }) => {
                bail!("replication stream error {code}: {message}");
            }
            Ok(frame) => bail!("unexpected replication frame in follow stream: {frame:?}"),
            Err(bicdb_core::BicDbError::Io(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(total)
}

fn default_embedding_model_source(model: &str) -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .unwrap_or(&manifest_dir)
        .join("models")
        .join(format!("{model}-ONNX"))
}

fn print_model_enable_animation(model: &str, source: &Path) {
    let frames = ["[=   ]", "[==  ]", "[=== ]", "[====]"];
    let steps = [
        "locating local model files",
        "checking q4 ONNX graph",
        "checking external weights",
        "checking tokenizer",
        "writing BicDB model registry",
    ];
    for (idx, step) in steps.iter().enumerate() {
        let frame = frames[idx % frames.len()];
        eprint!("\r{frame} {model}: {step} ({})", source.display());
        let _ = io::stderr().flush();
        std::thread::sleep(Duration::from_millis(90));
    }
    eprintln!("\r[ok  ] {model}: local embedding model enabled                 ");
}

fn print_model_registry_entry(model: &ModelRegistryEntry) {
    println!("Model: {}", model.name);
    println!("  provider: {:?}", model.provider);
    println!("  runtime: {:?}", model.runtime);
    println!("  dimension: {}", model.dimension);
    if let Some(model_dir) = &model.model_dir {
        println!("  path: {}", model_dir.display());
    }
    for (file, checksum) in &model.checksums {
        println!("  {checksum}  {file}");
    }
}

fn print_compaction_report(report: &CompactionReport) {
    println!("Compacted BicDB checkpoint");
    if let Some(checkpoint_id) = &report.checkpoint_id {
        println!("Checkpoint id: {checkpoint_id}");
    }
    println!("Collections: {}", report.collections.len());
    println!("Live records: {}", report.live_records);
    println!("Dead records estimate: {}", report.dead_records);
    println!("Bytes scanned: {}", report.bytes_scanned);
    println!("Bytes before: {}", report.bytes_before);
    println!("Bytes after: {}", report.bytes_after);
    println!("Bytes reclaimed: {}", report.bytes_reclaimed);
    println!(
        "Transaction log bytes: {} -> {}",
        report.transaction_log_bytes_before, report.transaction_log_bytes_after
    );
    println!(
        "Event log bytes: {} -> {}",
        report.event_log_bytes_before, report.event_log_bytes_after
    );
    println!(
        "Sync log bytes: {} -> {}",
        report.sync_log_bytes_before, report.sync_log_bytes_after
    );
    println!("Index metadata bytes: {}", report.index_metadata_bytes);
    println!("Derived sidecar bytes: {}", report.sidecar_bytes);
    println!("Duration ms: {}", report.duration_ms);
    println!("Pause ms: {}", report.pause_time_ms);
    if let Some(error) = &report.last_error {
        println!("Last error: {error}");
    }
    for collection in &report.collections {
        println!(
            "- {}: records={}, before={}, after={}, reclaimed={}",
            collection.collection,
            collection.live_records,
            collection.bytes_before,
            collection.bytes_after,
            collection.bytes_reclaimed
        );
    }
}

fn open_paged_records_for_recovery(
    path: &Path,
    key: Option<String>,
    key_env: Option<String>,
) -> Result<PagedRecords> {
    let mode = storage_mode(path)?;
    if mode != StorageMode::ServerPaged {
        bail!(
            "MVCC chain recovery requires storage_mode=server_paged; `{}` uses {mode}",
            path.display()
        );
    }
    if db_encryption_for_path(path, key, key_env)?.is_some() {
        bail!("server_paged MVCC recovery does not support encrypted page storage");
    }
    let paged_path = path.join(DEFAULT_PAGED_DIR);
    if !paged_path.join("store.pages").is_file() {
        bail!(
            "paged store file is missing at {}; refusing to create storage during recovery",
            paged_path.join("store.pages").display()
        );
    }
    Ok(PagedRecords::open(
        paged_path,
        PagedRecordsOptions::default(),
    )?)
}

fn version_chain_locator_token(locator: TupleLocator) -> String {
    format!(
        "{}:{}:{}",
        locator.page_id, locator.slot, locator.generation
    )
}

fn parse_version_chain_locator(value: &str) -> Result<TupleLocator> {
    let mut fields = value.split(':');
    let page_id = fields
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing page id"))?
        .parse::<u64>()?;
    let slot = fields
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing slot"))?
        .parse::<u16>()?;
    let generation = fields
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing generation"))?
        .parse::<u32>()?;
    if fields.next().is_some() {
        bail!("invalid chain head `{value}`: expected page:slot:generation");
    }
    Ok(TupleLocator::new(page_id, slot, generation))
}

fn print_paged_integrity_report(report: &bicdb_core::PagedIntegrityReport) {
    println!("BicDB paged storage integrity");
    println!("Valid: {}", report.valid);
    println!("B-tree entries: {}", report.btree.entries);
    println!("B-tree height: {}", report.btree.height);
    println!(
        "B-tree ordering violations: {}",
        report.btree.ordering_violations
    );
    println!(
        "MVCC keys examined: {}",
        report.version_chains.keys_examined
    );
    println!(
        "MVCC versions examined: {}",
        report.version_chains.versions_examined
    );
    println!("MVCC cycles: {}", report.version_chains.cycles);
    println!(
        "MVCC safety-limit faults: {}",
        report.version_chains.limit_exceeded
    );
    println!(
        "MVCC malformed versions: {}",
        report.version_chains.malformed_versions
    );
    println!(
        "MVCC invalid heads: {}",
        report.version_chains.invalid_heads
    );
    for fault in &report.version_chains.fault_samples {
        let key_hex = fault
            .key
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        println!(
            "- key_hex={} head={} terminal={:?}",
            key_hex,
            fault
                .inspection
                .head
                .map(version_chain_locator_token)
                .as_deref()
                .unwrap_or("(none)"),
            fault.inspection.terminal
        );
    }
}

fn print_integrity_report(title: &str, report: &bicdb_core::IntegrityReport) {
    println!("{title}");
    println!("Path: {}", report.path.display());
    println!("Encrypted database: {}", report.encrypted);
    println!("Key version: {:?}", report.key_version);
    println!("Collections checked: {}", report.collections_checked);
    println!("Record frames: {}", report.record_frames);
    println!("Transaction frames: {}", report.tx_frames);
    println!("Event frames: {}", report.event_frames);
    println!("Sync frames: {}", report.sync_frames);
    println!("Encrypted frames: {}", report.encrypted_frames);
    println!("Unencrypted frames: {}", report.unencrypted_frames);
    println!("Bytes checked: {}", report.bytes_checked);
    println!(
        "Large value blobs checked: {}",
        report.large_value_blobs_checked
    );
    println!(
        "Large value bytes checked: {}",
        report.large_value_bytes_checked
    );
    println!(
        "Large value orphan temp files: {}",
        report.large_value_orphan_tmp_files
    );
    if !report.large_value_checksum_failures.is_empty() {
        println!("Large value checksum failures:");
        for failure in &report.large_value_checksum_failures {
            println!("- {failure}");
        }
    }
    if let Some(paged) = &report.paged_storage {
        print_paged_integrity_report(paged);
    }
}

fn database_integrity_check(
    path: &Path,
    key: Option<String>,
    key_env: Option<String>,
) -> Result<(bicdb_core::IntegrityReport, bicdb_sql::SqlIntegrityReport)> {
    // Verify the unopened path first. Normal database recovery deliberately
    // truncates a torn trailing frame, so opening before strict verification
    // could erase the evidence that `bicdb check` is supposed to report.
    let config = DbConfig::default();
    let encryption = db_encryption_for_path(path, key, key_env)?;
    let report = BicDb::verify_path(path, config.clone(), encryption.clone())?;
    let mut db = match encryption {
        Some(encryption) => BicDb::open_with_encryption(path, config, encryption)?,
        None => BicDb::open_with_config(path, config)?,
    };
    let sql_report = integrity_check(&mut db)?;
    db.close()?;
    Ok((report, sql_report))
}

fn print_database_integrity_json(
    report: &bicdb_core::IntegrityReport,
    sql_report: &bicdb_sql::SqlIntegrityReport,
    backup_reports: Vec<Value>,
) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "storage": report,
            "sql": sql_report,
            "backups": backup_reports,
        }))?
    );
    Ok(())
}

fn print_sql_integrity_report(report: &bicdb_sql::SqlIntegrityReport) {
    println!(
        "SQL integrity: {}",
        if report.valid { "valid" } else { "invalid" }
    );
    println!("Tables checked: {}", report.tables_checked);
    println!("Records checked: {}", report.records_checked);
    println!("Constraints checked: {}", report.constraints_checked);
    println!("Indexes checked: {}", report.indexes_checked);
    for violation in &report.violations {
        println!(
            "- {} | table={} | object={} | record={} | {}",
            violation.check,
            violation.table.as_deref().unwrap_or(""),
            violation.object.as_deref().unwrap_or(""),
            violation.record_id.as_deref().unwrap_or(""),
            violation.message
        );
    }
}

fn print_vector_index_report(action: &str, report: &HnswIndexVerifyReport) {
    println!("{action} BicDB HNSW vector index");
    println!("Collection: {}", report.collection);
    println!("Valid: {}", report.valid);
    println!("Live vectors: {}", report.live_vectors);
    println!("Indexed vectors: {}", report.indexed_vectors);
    println!("Tombstoned vectors: {}", report.tombstoned_vectors);
    println!("Dimension: {:?}", report.dimension);
    println!("Index size: {} bytes", report.index_size_bytes);
}

fn print_index_all_report(report: &IndexMaintenanceAllReport, output: OutputMode) -> Result<()> {
    match output {
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
        OutputMode::Csv => {
            println!(
                "operation,index_name,collection,kind,status,valid,records_scanned,records_indexed,size_bytes,build_time_ms,progress_percent,stale,corrupt,last_verified_unix_ms"
            );
            for entry in &report.reports {
                print_index_report_csv(entry);
            }
            for vector in &report.vector_reports {
                println!(
                    "{},hnsw_{},{},vector_ann,verified,{},{},{},{},0,100,{},{},",
                    report.operation,
                    vector.collection,
                    vector.collection,
                    vector.valid,
                    vector.live_vectors,
                    vector.indexed_vectors,
                    vector.index_size_bytes,
                    !vector.valid,
                    !vector.valid
                );
            }
        }
        OutputMode::Table => {
            println!("BicDB index {} report", report.operation);
            println!("Valid: {}", report.valid);
            for entry in &report.reports {
                println!(
                    "{} | {} | {:?} | {} | valid={} | records={}/{} | size={} bytes | build_ms={} | progress={}%",
                    entry.index_name,
                    entry.collection,
                    entry.kind,
                    entry.status,
                    entry.verification.valid,
                    entry.records_indexed,
                    entry.records_scanned,
                    entry.size_bytes,
                    entry.build_time_ms,
                    entry.progress_percent
                );
            }
            for vector in &report.vector_reports {
                println!(
                    "hnsw_{} | {} | vector_ann | verified | valid={} | vectors={}/{} | size={} bytes",
                    vector.collection,
                    vector.collection,
                    vector.valid,
                    vector.indexed_vectors,
                    vector.live_vectors,
                    vector.index_size_bytes
                );
            }
        }
    }
    Ok(())
}

fn print_index_report_csv(entry: &IndexMaintenanceReport) {
    println!(
        "{},{},{},{:?},{},{},{},{},{},{},{},{},{},{}",
        entry.operation,
        entry.index_name,
        entry.collection,
        entry.kind,
        entry.status,
        entry.verification.valid,
        entry.records_scanned,
        entry.records_indexed,
        entry.size_bytes,
        entry.build_time_ms,
        entry.progress_percent,
        entry.stale,
        entry.corrupt,
        entry
            .last_verified_unix_ms
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
}

fn graph_projection_by_name(path: &Path) -> Result<GraphProjection> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read graph projection {}", path.display()))?;
    let projection = serde_json::from_slice::<GraphProjection>(&bytes)
        .with_context(|| format!("parse graph projection {}", path.display()))?;
    if projection.name.trim().is_empty() {
        bail!("graph projection {} has an empty name", path.display());
    }
    Ok(projection)
}

fn print_graph_build_report(action: &str, graph: &GraphProjectionData) {
    println!("{action} BicDB graph projection");
    println!("Projection: {}", graph.name);
    println!("Nodes: {}", graph.nodes.len());
    println!("Edges: {}", graph.edges.len());
    println!("Graph size: {} bytes", graph.graph_size_bytes());
}

fn print_graph_verify_report(report: &GraphVerifyReport) {
    println!("Verified BicDB graph projection");
    println!("Projection: {}", report.projection);
    println!("Valid: {}", report.valid);
    println!("Stored nodes: {}", report.stored_nodes);
    println!("Rebuilt nodes: {}", report.rebuilt_nodes);
    println!("Stored edges: {}", report.stored_edges);
    println!("Rebuilt edges: {}", report.rebuilt_edges);
    println!("Graph size: {} bytes", report.graph_size_bytes);
}

fn run_graph_query(graph: &GraphProjectionData, query: &str) -> Result<()> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    match tokens.as_slice() {
        ["NEIGHBORS", node_id] => {
            println!("Neighbors of {node_id}");
            print_graph_nodes(graph.neighbors(node_id));
        }
        ["EDGES", node_id] => {
            println!("Edges for {node_id}");
            for edge in graph.edges(node_id) {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    edge.id,
                    edge.from,
                    edge.to,
                    edge.label,
                    edge.timestamp
                        .map(|timestamp| timestamp.to_string())
                        .unwrap_or_default()
                );
            }
        }
        ["PATH", from, to, "DEPTH", depth] => {
            let depth = depth.parse::<usize>()?;
            match graph.path(from, to, depth) {
                Some(path) => {
                    println!("Path");
                    println!("Nodes: {}", path.nodes.join(" -> "));
                    println!("Edges: {}", path.edges.join(" -> "));
                }
                None => println!("No path found"),
            }
        }
        ["TRAVERSE", start, edge_label, "DEPTH", depth] => {
            let depth = depth.parse::<usize>()?;
            println!("Traverse {start} via {edge_label}");
            print_graph_nodes(graph.traverse(start, edge_label, depth));
        }
        _ => {
            return Err(anyhow::anyhow!(
                "unsupported graph query; expected `NEIGHBORS <node>`, `EDGES <node>`, `PATH <from> <to> DEPTH <n>`, or `TRAVERSE <start> <edge_label> DEPTH <n>`"
            ));
        }
    }
    Ok(())
}

fn print_graph_nodes(nodes: Vec<bicdb_core::GraphNode>) {
    for node in nodes {
        println!("{}\t{}\t{}", node.id, node.label, node.properties);
    }
}

fn backup_key(key: Option<String>, key_env: Option<String>) -> Result<String> {
    if let Some(key) = key {
        if key.is_empty() {
            return Err(anyhow::anyhow!("backup key must not be empty"));
        }
        return Ok(key);
    }
    if let Some(name) = key_env {
        let value = std::env::var(&name)
            .map_err(|_| anyhow::anyhow!("environment variable {name} is not set"))?;
        if value.is_empty() {
            return Err(anyhow::anyhow!("environment variable {name} is empty"));
        }
        return Ok(value);
    }
    let value = std::env::var("BICDB_BACKUP_KEY").map_err(|_| {
        anyhow::anyhow!("provide --key, --key-env, or set BICDB_BACKUP_KEY for encrypted backups")
    })?;
    if value.is_empty() {
        return Err(anyhow::anyhow!("BICDB_BACKUP_KEY is empty"));
    }
    Ok(value)
}

fn open_db_with_key_args(
    path: &Path,
    key: Option<String>,
    key_env: Option<String>,
) -> Result<BicDb> {
    let encryption = db_encryption_for_path(path, key, key_env)?;
    match encryption {
        Some(encryption) => Ok(BicDb::open_with_encryption(
            path,
            DbConfig::default(),
            encryption,
        )?),
        None => Ok(BicDb::open(path)?),
    }
}

fn db_encryption_for_path(
    path: &Path,
    key: Option<String>,
    key_env: Option<String>,
) -> Result<Option<EncryptionConfig>> {
    if key.is_some() || key_env.is_some() {
        return optional_passphrase_config(key, key_env, None, "database encryption");
    }
    if path.join(bicdb_core::ENCRYPTION_METADATA_FILE).exists() {
        return optional_passphrase_config(None, None, Some("BICDB_KEY"), "database encryption");
    }
    Ok(None)
}

fn required_passphrase_config(
    key: Option<String>,
    key_env: Option<String>,
    default_env: &str,
    purpose: &str,
) -> Result<EncryptionConfig> {
    let secret = required_secret(key, key_env, default_env, purpose)?;
    Ok(EncryptionConfig::with_passphrase(secret))
}

fn optional_passphrase_config(
    key: Option<String>,
    key_env: Option<String>,
    default_env: Option<&str>,
    purpose: &str,
) -> Result<Option<EncryptionConfig>> {
    optional_secret(key, key_env, default_env, purpose)
        .map(|secret| secret.map(EncryptionConfig::with_passphrase))
}

fn required_secret(
    key: Option<String>,
    key_env: Option<String>,
    default_env: &str,
    purpose: &str,
) -> Result<String> {
    optional_secret(key, key_env, Some(default_env), purpose)?.ok_or_else(|| {
        anyhow::anyhow!("provide --key, --key-env, or set {default_env} for {purpose}")
    })
}

fn optional_secret(
    key: Option<String>,
    key_env: Option<String>,
    default_env: Option<&str>,
    purpose: &str,
) -> Result<Option<String>> {
    if let Some(key) = key {
        if key.is_empty() {
            return Err(anyhow::anyhow!("{purpose} key must not be empty"));
        }
        return Ok(Some(key));
    }
    if let Some(name) = key_env {
        let value = std::env::var(&name)
            .map_err(|_| anyhow::anyhow!("environment variable {name} is not set"))?;
        if value.is_empty() {
            return Err(anyhow::anyhow!("environment variable {name} is empty"));
        }
        return Ok(Some(value));
    }
    if let Some(default_env) = default_env {
        match std::env::var(default_env) {
            Ok(value) if !value.is_empty() => return Ok(Some(value)),
            Ok(_) => return Err(anyhow::anyhow!("{default_env} is empty")),
            Err(_) => {}
        }
    }
    Ok(None)
}

fn run_analytics_command(
    target: String,
    query: Option<String>,
    collection: Option<String>,
    output: OutputMode,
    table: bool,
) -> Result<()> {
    if table && !matches!(output, OutputMode::Table) {
        return Err(anyhow::anyhow!(
            "--table cannot be combined with --json or --csv"
        ));
    }

    match target.as_str() {
        "rebuild" => {
            let Some(path) = query else {
                return Err(anyhow::anyhow!(
                    "usage: bicdb analytics rebuild <db> --collection <name>"
                ));
            };
            let collection = collection
                .ok_or_else(|| anyhow::anyhow!("analytics rebuild requires --collection <name>"))?;
            let db = BicDb::open(&path)?;
            let report = rebuild_sidecar(&db, &path, &collection)?;
            println!("Rebuilt BicDB analytics sidecar");
            println!("Collection: {}", report.collection);
            println!("Rows: {}", report.rows);
            println!("Sidecar bytes: {}", report.sidecar_bytes);
            println!("Checksum: {}", report.checksum);
            println!("Path: {}", report.path.display());
            Ok(())
        }
        "verify" => {
            let Some(path) = query else {
                return Err(anyhow::anyhow!(
                    "usage: bicdb analytics verify <db> --collection <name>"
                ));
            };
            let collection = collection
                .ok_or_else(|| anyhow::anyhow!("analytics verify requires --collection <name>"))?;
            let db = BicDb::open(&path)?;
            let report = verify_sidecar(&db, &path, &collection)?;
            println!("Verified BicDB analytics sidecar");
            println!("Collection: {}", report.collection);
            println!("Canonical rows: {}", report.canonical_rows);
            println!("Sidecar rows: {}", report.sidecar_rows);
            println!("Record IDs match: {}", report.record_ids_match);
            println!("Timestamps match: {}", report.timestamps_match);
            println!("Checksum match: {}", report.checksum_match);
            println!("Path: {}", report.path.display());
            Ok(())
        }
        path => {
            let Some(sql) = query else {
                return Err(anyhow::anyhow!(
                    "usage: bicdb analytics <db> \"SELECT ...\""
                ));
            };
            let db = BicDb::open(path)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let result = runtime.block_on(async {
                let ctx = BicDataFusionContext::new(&db);
                ctx.sql(&sql).await
            })?;
            print_analytics_result(&result, output)
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum OutputMode {
    Table,
    Json,
    Csv,
}

#[derive(Clone, Debug)]
struct SlowQueryOptions {
    log_path: Option<PathBuf>,
    threshold_ms: u64,
    redaction: RedactionConfig,
}

impl OutputMode {
    fn new(json: bool, csv: bool) -> Result<Self> {
        match (json, csv) {
            (true, true) => Err(anyhow::anyhow!("--json and --csv are mutually exclusive")),
            (true, false) => Ok(Self::Json),
            (false, true) => Ok(Self::Csv),
            (false, false) => Ok(Self::Table),
        }
    }
}

fn run_sql_command(
    path: PathBuf,
    query: Option<String>,
    output: OutputMode,
    slow_query: SlowQueryOptions,
) -> Result<()> {
    let mut db = BicDb::open(&path)?;

    if let Some(query) = query {
        execute_sql_command(&mut db, &query, output, &slow_query)?;
        return Ok(());
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut lines = stdin.lock().lines();
    loop {
        write!(stdout, "bicdb> ")?;
        stdout.flush()?;
        let Some(line) = lines.next() else {
            break;
        };
        let line = line?;
        let query = line.trim();
        if query.is_empty() {
            continue;
        }
        if matches!(
            query.to_ascii_lowercase().as_str(),
            "\\q" | "exit" | "exit;" | "quit" | "quit;"
        ) {
            break;
        }

        match execute_sql_command(&mut db, query, output, &slow_query) {
            Ok(()) => {}
            Err(error) => eprintln!("error: {error}"),
        }
    }

    Ok(())
}

fn execute_sql_command(
    db: &mut BicDb,
    query: &str,
    output: OutputMode,
    slow_query: &SlowQueryOptions,
) -> Result<()> {
    let started = Instant::now();
    let analytics_error = if should_try_analytics_sql(query) {
        match run_datafusion_sql(&*db, query) {
            Ok(result) => {
                maybe_write_slow_query(
                    slow_query,
                    query,
                    result.rows_as_json().len(),
                    started.elapsed(),
                )?;
                print_analytics_result(&result, output)?;
                return Ok(());
            }
            Err(error) => Some(error),
        }
    } else {
        None
    };

    match SqlSession::new(db).execute(query) {
        Ok(result) => {
            maybe_write_slow_query(slow_query, query, result.rows.len(), started.elapsed())?;
            print_sql_result(&result, output)
        }
        Err(sql_error) => {
            if let Some(analytics_error) = analytics_error {
                Err(anyhow::anyhow!(
                    "DataFusion analytics path failed: {analytics_error}; SQL fallback failed: {sql_error}"
                ))
            } else {
                Err(sql_error.into())
            }
        }
    }
}

fn maybe_write_slow_query(
    options: &SlowQueryOptions,
    query: &str,
    rows: usize,
    elapsed: Duration,
) -> Result<()> {
    let Some(path) = &options.log_path else {
        return Ok(());
    };
    let elapsed_ms = elapsed.as_millis() as u64;
    if elapsed_ms < options.threshold_ms {
        return Ok(());
    }
    let entry = SlowQueryLogEntry::new(elapsed_ms, rows, query, &[], &options.redaction);
    append_slow_query_log(path, &entry)?;
    Ok(())
}

fn redaction_config(
    redact_query_text: bool,
    redact_bind_parameters: bool,
    redact_fields: Vec<String>,
) -> RedactionConfig {
    let mut config = RedactionConfig::default();
    config.redact_query_text = redact_query_text;
    config.redact_bind_parameters = redact_bind_parameters || config.redact_bind_parameters;
    if !redact_fields.is_empty() {
        config.sensitive_fields.extend(redact_fields);
    }
    config
}

fn run_datafusion_sql(db: &BicDb, sql: &str) -> Result<AnalyticsQueryResult> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let ctx = BicDataFusionContext::new(db);
        ctx.sql(sql).await
    })?;
    Ok(result)
}

fn should_try_analytics_sql(sql: &str) -> bool {
    let normalized = sql.to_ascii_lowercase();
    if normalized.contains("metadata.")
        || normalized.contains("information_schema")
        || normalized.trim_start().starts_with("show ")
        || normalized.contains("current_database")
        || normalized.contains("current_schema")
        || normalized.contains("version()")
    {
        return false;
    }

    let compact = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.contains(" group by ") {
        return true;
    }

    let no_space = normalized
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect::<String>();
    no_space.contains("avg(value)")
        || no_space.contains("min(value)")
        || no_space.contains("max(value)")
        || no_space.contains("count(*)")
}

fn print_sql_result(result: &SqlResult, output: OutputMode) -> Result<()> {
    match output {
        OutputMode::Table => {
            print_table(result);
            Ok(())
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(result)?);
            Ok(())
        }
        OutputMode::Csv => {
            print!("{}", result_to_csv(result));
            Ok(())
        }
    }
}

fn print_operational_metrics(metrics: &OperationalMetrics) {
    println!("BicDB operational metrics");
    println!("Generated at: {}", metrics.generated_at);
    for (section, values) in [
        ("server", &metrics.server),
        ("storage", &metrics.storage),
        ("planner", &metrics.planner),
        ("transaction", &metrics.transaction),
        ("backup", &metrics.backup),
        ("compaction", &metrics.compaction),
        ("security", &metrics.security),
        ("replication", &metrics.replication),
        ("memory", &metrics.memory),
    ] {
        println!("{section}:");
        for (name, value) in values {
            println!("  {name}: {value}");
        }
    }
}

fn print_health_report(report: &HealthReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    println!("BicDB health: {:?}", report.state);
    for (name, passed) in &report.checks {
        println!("{name}: {}", if *passed { "pass" } else { "fail" });
    }
    for recommendation in &report.recommendations {
        println!("recommendation: {recommendation}");
    }
    Ok(())
}

fn print_doctor_report(report: &DoctorReport) {
    println!("BicDB doctor");
    println!("Database id: {}", report.database_id);
    println!("Sanitized: {}", report.sanitized);
    println!("Storage mode: {}", report.storage_mode);
    println!("Collections: {}", report.stats.collection_count);
    println!("Records: {}", report.stats.record_count);
    println!("Database size bytes: {}", report.stats.size_bytes);
    println!("Health: {:?}", report.health.state);
    print_residency(&report.residency);
    if let Some(snapshot) = &report.paged_storage {
        print_paged_storage(snapshot);
    }
    for recommendation in &report.recommendations {
        println!("recommendation: {recommendation}");
    }
}

fn print_paged_storage(snapshot: &bicdb_core::PagedStoreSnapshot) {
    println!("Paged storage:");
    println!(
        "  page size:             {}",
        human_bytes(u64::from(snapshot.page_size))
    );
    println!("  page count:            {}", snapshot.page_count);
    println!("  used data pages:       {}", snapshot.used_data_pages);
    println!("  free pages:            {}", snapshot.free_pages);
    println!(
        "  logical page bytes:    {}",
        human_bytes(snapshot.logical_page_bytes)
    );
    println!(
        "  page file bytes:       {}",
        human_bytes(snapshot.page_file_bytes)
    );
    println!(
        "  buffer pool:           {} / {} ({:.2}% hits)",
        human_bytes(snapshot.buffer_pool.resident_bytes),
        human_bytes(snapshot.buffer_pool.budget_bytes),
        snapshot.buffer_pool_hit_ratio_basis_points() as f64 / 100.0
    );
    println!(
        "  dirty pages:           {}",
        snapshot.buffer_pool.dirty_pages
    );
    println!(
        "  pinned pages:          {}",
        snapshot.buffer_pool.pinned_pages
    );
    println!(
        "  admission failures:    {}",
        snapshot.buffer_pool.admission_failures
    );
    println!(
        "  WAL:                   {} / {}",
        human_bytes(snapshot.wal_bytes),
        human_bytes(snapshot.wal_max_bytes)
    );
    println!(
        "  transaction watermark: frozen {} / next {} ({} resident entries)",
        snapshot.transaction_frozen_xid,
        snapshot.transaction_next_xid,
        snapshot.resident_transaction_entries
    );
    println!(
        "  abort exceptions:      {} / {}",
        snapshot.abort_exceptions, snapshot.abort_exception_capacity
    );
    println!(
        "  status spill:          {} entries / {} pages",
        snapshot.status_spill_entries, snapshot.status_spill_pages
    );
}

fn print_residency(residency: &ResidencyReport) {
    println!("Resident memory (estimated):");
    for (label, bytes) in [
        ("rows", residency.rows_bytes),
        ("version chains", residency.version_chains_bytes),
        ("primary-key maps", residency.primary_key_maps_bytes),
        ("secondary indexes", residency.secondary_indexes_bytes),
        ("exact vectors", residency.exact_vectors_bytes),
        ("hnsw", residency.hnsw_bytes),
        ("graphs", residency.graphs_bytes),
    ] {
        println!("  {label:<18} {}", human_bytes(bytes));
    }
    println!(
        "  {:<18} {}",
        "accounted",
        human_bytes(residency.accounted_bytes)
    );
    match (
        residency.process_resident_bytes,
        residency.unaccounted_bytes,
    ) {
        (Some(rss), Some(gap)) => {
            println!("  {:<18} {}", "process RSS", human_bytes(rss));
            // Signed: accounting can in principle exceed RSS if pages have been
            // returned to the OS, and hiding that would mask a real problem.
            let sign = if gap < 0 { "-" } else { "" };
            println!(
                "  {:<18} {sign}{}",
                "unaccounted",
                human_bytes(gap.unsigned_abs())
            );
        }
        _ => println!("  {:<18} unavailable on this platform", "process RSS"),
    }
    if !residency.not_instrumented.is_empty() {
        println!(
            "  not instrumented:  {}",
            residency.not_instrumented.join(", ")
        );
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {} ({bytes} B)", UNITS[unit])
    }
}

fn print_analytics_result(result: &AnalyticsQueryResult, output: OutputMode) -> Result<()> {
    match output {
        OutputMode::Table => {
            print_json_table(&result.columns(), &result.rows_as_json());
            Ok(())
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(&result.rows_as_json())?);
            Ok(())
        }
        OutputMode::Csv => {
            print!("{}", result.to_csv());
            Ok(())
        }
    }
}

fn print_table(result: &SqlResult) {
    let widths = table_widths(result);
    let separator = widths
        .iter()
        .map(|width| "-".repeat(*width + 2))
        .collect::<Vec<_>>()
        .join("+");

    print_table_row(&result.columns, &widths);
    println!("{separator}");
    for row in &result.rows {
        let rendered = row.iter().map(SqlValue::to_cell).collect::<Vec<_>>();
        print_table_row(&rendered, &widths);
    }
    println!("({} rows)", result.rows.len());
}

fn print_json_table(columns: &[String], rows: &[serde_json::Value]) {
    let rendered_rows = rows
        .iter()
        .filter_map(serde_json::Value::as_object)
        .map(|row| {
            columns
                .iter()
                .map(|column| row.get(column).map(json_value_to_cell).unwrap_or_default())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut widths = columns
        .iter()
        .map(|column| column.len())
        .collect::<Vec<_>>();
    for row in &rendered_rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }
    let separator = widths
        .iter()
        .map(|width| "-".repeat(*width + 2))
        .collect::<Vec<_>>()
        .join("+");

    print_table_row(columns, &widths);
    println!("{separator}");
    for row in &rendered_rows {
        print_table_row(row, &widths);
    }
    println!("({} rows)", rendered_rows.len());
}

fn table_widths(result: &SqlResult) -> Vec<usize> {
    let mut widths = result
        .columns
        .iter()
        .map(|column| column.len())
        .collect::<Vec<_>>();
    for row in &result.rows {
        for (idx, value) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(idx) {
                *width = (*width).max(value.to_cell().len());
            }
        }
    }
    widths
}

fn print_table_row(values: &[String], widths: &[usize]) {
    let row = values
        .iter()
        .enumerate()
        .map(|(idx, value)| format!(" {:width$} ", value, width = widths[idx]))
        .collect::<Vec<_>>()
        .join("|");
    println!("{row}");
}

fn result_to_csv(result: &SqlResult) -> String {
    let mut csv = String::new();
    csv.push_str(
        &result
            .columns
            .iter()
            .map(|value| csv_escape(value))
            .collect::<Vec<_>>()
            .join(","),
    );
    csv.push('\n');
    for row in &result.rows {
        csv.push_str(
            &row.iter()
                .map(|value| csv_escape(&value.to_cell()))
                .collect::<Vec<_>>()
                .join(","),
        );
        csv.push('\n');
    }
    csv
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn json_value_to_cell(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn parse_lon_lat(point: &str) -> Result<(f64, f64)> {
    let parts = point.split(',').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(anyhow::anyhow!("invalid point `{point}`; expected lon,lat"));
    }
    let lon = parts[0]
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("invalid point `{point}`; longitude must be a number"))?;
    let lat = parts[1]
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("invalid point `{point}`; latitude must be a number"))?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err(anyhow::anyhow!(
            "invalid point `{point}`; lon and lat must be finite"
        ));
    }
    Ok((lon, lat))
}

fn parse_bbox(input: &str) -> Result<OsmImportBbox> {
    let parts = input.split(',').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 4 || parts.iter().any(|part| part.is_empty()) {
        return Err(anyhow::anyhow!(
            "invalid bbox `{input}`; expected minLon,minLat,maxLon,maxLat"
        ));
    }
    let mut values = Vec::with_capacity(4);
    for (index, part) in parts.iter().enumerate() {
        values.push(part.parse::<f64>().map_err(|_| {
            anyhow::anyhow!(
                "invalid bbox `{input}`; value {} must be a number",
                index + 1
            )
        })?);
    }
    let bbox = OsmImportBbox {
        min_lon: values[0],
        min_lat: values[1],
        max_lon: values[2],
        max_lat: values[3],
    };
    if !values.iter().all(|value| value.is_finite()) {
        return Err(anyhow::anyhow!(
            "invalid bbox `{input}`; values must be finite"
        ));
    }
    if bbox.min_lon > bbox.max_lon || bbox.min_lat > bbox.max_lat {
        return Err(anyhow::anyhow!(
            "invalid bbox `{input}`; min values must not exceed max values"
        ));
    }
    if bbox.min_lon < -180.0 || bbox.max_lon > 180.0 || bbox.min_lat < -90.0 || bbox.max_lat > 90.0
    {
        return Err(anyhow::anyhow!(
            "invalid bbox `{input}`; lon must be within [-180,180] and lat within [-90,90]"
        ));
    }
    Ok(bbox)
}

fn print_osm_import_report(report: &OsmImportReport, output: OutputMode) -> Result<()> {
    let row = serde_json::json!({
        "graph": report.graph,
        "min_lon": report.bbox.min_lon,
        "min_lat": report.bbox.min_lat,
        "max_lon": report.bbox.max_lon,
        "max_lat": report.bbox.max_lat,
        "road_ways": report.road_ways,
        "nodes": report.nodes,
        "edges": report.edges,
    });
    match output {
        OutputMode::Table => {
            print_json_table(
                &[
                    "graph".to_string(),
                    "road_ways".to_string(),
                    "nodes".to_string(),
                    "edges".to_string(),
                ],
                &[row],
            );
            Ok(())
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(&row)?);
            Ok(())
        }
        OutputMode::Csv => {
            println!("graph,min_lon,min_lat,max_lon,max_lat,road_ways,nodes,edges");
            println!(
                "{},{},{},{},{},{},{},{}",
                csv_escape(&report.graph),
                report.bbox.min_lon,
                report.bbox.min_lat,
                report.bbox.max_lon,
                report.bbox.max_lat,
                report.road_ways,
                report.nodes,
                report.edges
            );
            Ok(())
        }
    }
}

fn print_route_path(route: &RoutePath, output: OutputMode) -> Result<()> {
    let row = serde_json::json!({
        "graph": route.graph,
        "distance_m": route.distance_m,
        "node_ids": route.node_ids,
        "start": route.start,
        "end": route.end,
    });
    match output {
        OutputMode::Table => {
            print_json_table(
                &[
                    "graph".to_string(),
                    "distance_m".to_string(),
                    "node_ids".to_string(),
                ],
                &[row],
            );
            Ok(())
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(&row)?);
            Ok(())
        }
        OutputMode::Csv => {
            println!("graph,distance_m,node_ids");
            println!(
                "{},{},{}",
                csv_escape(&route.graph),
                route.distance_m,
                csv_escape(&route.node_ids.join("|"))
            );
            Ok(())
        }
    }
}

fn print_spatial_pack_report(report: &SpatialPackReport, output: OutputMode) -> Result<()> {
    match output {
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
        OutputMode::Csv => {
            println!("index_name,collection,strategy,generation,entry_count,node_count,height");
            println!(
                "{},{},{},{},{},{},{}",
                report.index_name,
                report.collection,
                report.strategy.label(),
                report.generation,
                report.entry_count,
                report.node_count,
                report.height
            );
        }
        OutputMode::Table => {
            println!(
                "packed `{}` on `{}` ({}): generation {}, {} entries, {} nodes, height {}",
                report.index_name,
                report.collection,
                report.strategy.label(),
                report.generation,
                report.entry_count,
                report.node_count,
                report.height
            );
        }
    }
    Ok(())
}

fn print_spatial_results(results: &[SpatialQueryResult], output: OutputMode) -> Result<()> {
    let rows = spatial_rows_as_json(results);
    match output {
        OutputMode::Table => {
            print_json_table(
                &[
                    "id".to_string(),
                    "distance_meters".to_string(),
                    "metadata".to_string(),
                ],
                &rows,
            );
            Ok(())
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(&rows)?);
            Ok(())
        }
        OutputMode::Csv => {
            print!("{}", spatial_rows_to_csv(&rows));
            Ok(())
        }
    }
}

fn spatial_rows_as_json(results: &[SpatialQueryResult]) -> Vec<serde_json::Value> {
    results
        .iter()
        .map(|result| {
            serde_json::json!({
                "id": &result.record.id,
                "distance_meters": result.distance_meters,
                "metadata": &result.record.metadata,
            })
        })
        .collect()
}

fn spatial_rows_to_csv(rows: &[serde_json::Value]) -> String {
    let columns = ["id", "distance_meters", "metadata"];
    let mut csv = columns.join(",");
    csv.push('\n');
    for row in rows {
        let values = columns
            .iter()
            .map(|column| row.get(*column).map(json_value_to_cell).unwrap_or_default())
            .map(|value| csv_escape(&value))
            .collect::<Vec<_>>();
        csv.push_str(&values.join(","));
        csv.push('\n');
    }
    csv
}

fn parse_vector_metric(metric: &str) -> Result<VectorMetric> {
    match metric {
        "cosine" => Ok(VectorMetric::Cosine),
        "dot" => Ok(VectorMetric::Dot),
        "l2" => Ok(VectorMetric::L2),
        other => Err(anyhow::anyhow!(
            "unsupported vector metric '{other}'; expected cosine, dot, or l2"
        )),
    }
}

#[cfg(feature = "bench")]
fn parse_vector_profile_strategy(strategy: &str) -> Result<VectorProfileStrategy> {
    match strategy {
        "optimized-store" | "optimized_store" => Ok(VectorProfileStrategy::OptimizedStore),
        "record-scan" | "record_scan" => Ok(VectorProfileStrategy::RecordScan),
        other => Err(anyhow::anyhow!(
            "unsupported vector profile strategy '{other}'; expected optimized-store or record-scan"
        )),
    }
}

#[cfg(feature = "bench")]
fn read_paged_recovery_evidence(path: &Path) -> Result<Vec<u8>> {
    let path_before = std::fs::symlink_metadata(path)?;
    if path_before.file_type().is_symlink() || !path_before.is_file() {
        bail!("paged recovery report must be a regular file, not a symlink");
    }
    let mut file = open_paged_recovery_evidence(path)?;
    let opened = file.metadata()?;
    if !same_file_identity(&path_before, &opened) {
        bail!("paged recovery report changed while it was being opened");
    }
    if opened.len() == 0 || opened.len() > MAX_PAGED_RECOVERY_EVIDENCE_BYTES {
        bail!(
            "paged recovery report must be non-empty and no larger than {} bytes",
            MAX_PAGED_RECOVERY_EVIDENCE_BYTES
        );
    }
    let mut bytes = Vec::with_capacity(usize::try_from(opened.len()).unwrap_or_default());
    (&mut file)
        .take(MAX_PAGED_RECOVERY_EVIDENCE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    let opened_after = file.metadata()?;
    let path_after = std::fs::symlink_metadata(path)?;
    if path_after.file_type().is_symlink()
        || !path_after.is_file()
        || !same_file_identity(&opened, &opened_after)
        || !same_file_identity(&opened, &path_after)
        || opened.len() != opened_after.len()
        || opened.modified().ok() != opened_after.modified().ok()
        || bytes.len() as u64 != opened.len()
    {
        bail!("paged recovery report changed while it was being read");
    }
    Ok(bytes)
}

#[cfg(unix)]
#[cfg(feature = "bench")]
fn open_paged_recovery_evidence(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
#[cfg(feature = "bench")]
fn open_paged_recovery_evidence(path: &Path) -> io::Result<std::fs::File> {
    std::fs::File::open(path)
}

#[cfg(unix)]
#[cfg(feature = "bench")]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
#[cfg(feature = "bench")]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.is_file()
        && right.is_file()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

#[cfg(feature = "bench")]
fn write_exports(
    json_out: Option<PathBuf>,
    csv_out: Option<PathBuf>,
    json: String,
    csv: String,
) -> Result<()> {
    if let Some(path) = json_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, json)?;
        println!("Wrote JSON report to {}", path.display());
    }

    if let Some(path) = csv_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, csv)?;
        println!("Wrote CSV report to {}", path.display());
    }

    Ok(())
}

#[cfg(feature = "bench")]
fn write_json_markdown_exports(
    json_out: Option<PathBuf>,
    markdown_out: Option<PathBuf>,
    json: String,
    markdown: String,
) -> Result<()> {
    if let Some(path) = json_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, json)?;
        println!("Wrote JSON report to {}", path.display());
    }

    if let Some(path) = markdown_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, markdown)?;
        println!("Wrote Markdown report to {}", path.display());
    }

    Ok(())
}

#[cfg(feature = "bench")]
fn write_markdown_export(markdown_out: Option<PathBuf>, markdown: String) -> Result<()> {
    if let Some(path) = markdown_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, markdown)?;
        println!("Wrote Markdown report to {}", path.display());
    }

    Ok(())
}

#[cfg(feature = "bench-comparison-engines")]
fn print_baseline_reports(reports: &[BaselineReport]) {
    println!("BicDB Baseline Comparison");
    println!("-------------------------");
    for report in reports {
        println!(
            "{:?}: {:?}, records={}, throughput={}, size={}, {}",
            report.engine,
            report.status,
            report.records,
            report
                .records_per_sec
                .map(|value| format!("{value:.2} records/sec"))
                .unwrap_or_else(|| "n/a".to_string()),
            report
                .database_size_bytes
                .map(|value| format!("{value} bytes"))
                .unwrap_or_else(|| "n/a".to_string()),
            report.message
        );
    }
}

#[cfg(test)]
mod cell_environment_tests {
    use super::*;

    #[test]
    fn cell_boolean_environment_values_are_strict() {
        for value in ["1", "true", "YES", " on "] {
            assert!(parse_env_flag_value("BICDB_CELL_CHECK", value).unwrap());
        }
        for value in ["0", "false", "NO", " off "] {
            assert!(!parse_env_flag_value("BICDB_CELL_CHECK", value).unwrap());
        }
        for value in ["", "enabled", "tru", "2"] {
            let error = parse_env_flag_value("BICDB_CELL_CHECK", value).unwrap_err();
            assert!(error.to_string().contains("BICDB_CELL_CHECK"));
        }
    }
}

#[cfg(test)]
mod replication_authorization_tests {
    use super::*;

    /// mTLS proves a peer holds a CA-signed key — never which node it is.
    /// The allow-list entry must therefore be able to pin the identity to a
    /// specific leaf certificate, and a malformed pin must be refused at
    /// configuration time rather than silently ignored at connection time.
    #[test]
    fn allowed_node_entries_parse_identity_and_optional_pin() {
        let fingerprint = "a".repeat(64);
        let parsed = parse_allowed_replication_nodes(&[
            "standby-1".to_string(),
            format!("standby-2={fingerprint}"),
            format!("standby-3={}", fingerprint.to_uppercase()),
        ])
        .unwrap();
        assert_eq!(parsed[0].node_id, "standby-1");
        assert_eq!(parsed[0].certificate_sha256, None);
        assert_eq!(parsed[1].node_id, "standby-2");
        assert_eq!(
            parsed[1].certificate_sha256.as_deref(),
            Some(fingerprint.as_str())
        );
        // Case is normalized so an operator pasting either form works.
        assert_eq!(
            parsed[2].certificate_sha256.as_deref(),
            Some(fingerprint.as_str())
        );

        // A pin that is not a SHA-256 is a configuration error, not a
        // silently unpinned node.
        for malformed in [
            "standby=deadbeef",
            "standby=",
            &format!("standby={}", "z".repeat(64)),
        ] {
            assert!(
                parse_allowed_replication_nodes(&[malformed.to_string()]).is_err(),
                "malformed pin `{malformed}` must be refused"
            );
        }
        assert!(parse_allowed_replication_nodes(&["=abc".to_string()]).is_err());
    }
}

#[cfg(test)]
mod consensus_identity_tests {
    use super::*;

    fn pins() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("aa".repeat(32), "n1".to_string()),
            ("bb".repeat(32), "n2".to_string()),
        ])
    }

    #[test]
    fn peer_pins_parse_and_are_validated() {
        let (peers, pins) = parse_consensus_peers_with_pins(
            "n1",
            "127.0.0.1:9001",
            vec![
                format!("n1=127.0.0.1:9001={}", "aa".repeat(32)),
                format!("n2=127.0.0.1:9002={}", "bb".repeat(32)),
            ],
        )
        .unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(pins.get(&"bb".repeat(32)).map(String::as_str), Some("n2"));

        // Unpinned peers still parse: the pin is optional so an existing
        // deployment keeps starting.
        let (peers, pins) = parse_consensus_peers_with_pins(
            "n1",
            "127.0.0.1:9001",
            vec!["n2=127.0.0.1:9002".into()],
        )
        .unwrap();
        assert_eq!(peers.len(), 2);
        assert!(pins.is_empty());

        // A malformed fingerprint is refused rather than silently ignored.
        assert!(parse_consensus_peers_with_pins(
            "n1",
            "127.0.0.1:9001",
            vec!["n2=127.0.0.1:9002=nothex".into()],
        )
        .is_err());

        // One certificate cannot claim two voters.
        assert!(parse_consensus_peers_with_pins(
            "n1",
            "127.0.0.1:9001",
            vec![
                format!("n2=127.0.0.1:9002={}", "aa".repeat(32)),
                format!("n3=127.0.0.1:9003={}", "aa".repeat(32)),
            ],
        )
        .is_err());
    }

    #[test]
    fn an_unpinned_certificate_is_refused_when_pins_are_configured() {
        let error = consensus_authenticated_node(&pins(), Some(&"cc".repeat(32))).unwrap_err();
        assert!(
            error.to_string().contains("is not pinned"),
            "unexpected error: {error}"
        );
    }

    /// The frame-level half: a voter authenticated as n1 must not be able to
    /// vote as n2 or append as leader n2. Before this binding, both were
    /// accepted — `handle_request_vote` only checked that the asserted
    /// candidate was a known voter, and `handle_append_entries` trusted
    /// `leader_id` outright.
    #[test]
    fn a_voter_cannot_assert_another_voters_identity() {
        struct Duplex {
            input: std::io::Cursor<Vec<u8>>,
            output: Vec<u8>,
        }
        impl std::io::Read for Duplex {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                std::io::Read::read(&mut self.input, buf)
            }
        }
        impl std::io::Write for Duplex {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.output.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(parking_lot::RwLock::new(
            BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false))
                .unwrap(),
        ));
        let contact = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let forged = [
            ReplicationFrame::ConsensusRequestVote(bicdb_core::RequestVote {
                cluster_id: "c1".to_string(),
                term: 9,
                candidate_id: "n2".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            }),
            ReplicationFrame::ConsensusAppendEntries(bicdb_core::AppendEntries {
                cluster_id: "c1".to_string(),
                term: 9,
                leader_id: "n2".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            }),
        ];
        for frame in forged {
            let mut bytes = Vec::new();
            replication_transport::send_frame(&mut bytes, &frame, 1 << 20).unwrap();
            let mut stream = Duplex {
                input: std::io::Cursor::new(bytes),
                output: Vec::new(),
            };
            // Authenticated as n1, asserting n2.
            let error =
                consensus_handle_connection(&db, &contact, &mut stream, 1 << 20, Some("n1"))
                    .unwrap_err();
            let rendered = error.to_string();
            assert!(
                rendered.contains("asserts candidate n2") || rendered.contains("asserts leader n2"),
                "impostor frame must be refused, got: {rendered}"
            );
        }
    }

    #[test]
    fn identity_resolves_from_the_presented_certificate() {
        assert_eq!(
            consensus_authenticated_node(&pins(), Some(&"aa".repeat(32))).unwrap(),
            Some("n1".to_string())
        );
        // No pins configured, or no certificate: nothing to bind to.
        assert_eq!(
            consensus_authenticated_node(&BTreeMap::new(), Some(&"aa".repeat(32))).unwrap(),
            None
        );
        assert_eq!(consensus_authenticated_node(&pins(), None).unwrap(), None);
    }
}
