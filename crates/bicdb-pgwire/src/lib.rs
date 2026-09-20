mod delegation;
pub mod operator;
pub use delegation::{set_user_delegation_policy, DelegationPolicy};
mod server_cluster;
pub use server_cluster::PgWireCluster;
pub(crate) use server_cluster::*;
mod query_exec;
mod routine_outcome_trace;
pub(crate) use query_exec::*;
mod sql_parsing;
pub(crate) use sql_parsing::*;
mod binary_codec;
pub(crate) use binary_codec::*;
mod protocol_wire;
pub(crate) use protocol_wire::*;
mod checkpoint_driver;
pub(crate) use checkpoint_driver::*;
mod tls_binding;
mod validation;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
pub(crate) use tls_binding::*;
pub(crate) use validation::*;

use parking_lot::{Mutex as ParkingMutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use argon2::{Algorithm, Argon2, Params, Version};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bicdb_core::replication_transport;
use bicdb_core::{
    append_slow_query_log, load_distribution_config, operational_event_json, plan_point_query,
    plan_scatter_query, plan_shard_local_write, redact_query_text, save_distribution_config,
    start_cluster_data_server, AuthenticationStrength, AutomaticRangeAntiEntropyController,
    AutomaticRangeAntiEntropyLimits, BicDb, BicDbError, CancellationToken, ClusterDataNodeService,
    ClusterNodeId, ClusterRelocationTransportConfig, ClusterRequestRouter, ClusterSupervisor,
    ClusterSupervisorConfig, DbConfig, DistributedQueryLimits, DistributedQueryPlan,
    DistributedWriteIntent, DistributionStore, HaRole, HaStatus, MetadataConsensusRole,
    MetadataConsensusStore, PagedCheckpointMaintenanceHandle, PagedCheckpointScheduleAdvance,
    PagedCheckpointScheduleLimits, PointOperation, RangeWriteCoordinator, ReadAheadLimits,
    RedactionConfig, ReplicationConfig, ReplicationMode, ReplicationTlsConfig, ResourceDemand,
    ResourceGovernor, ResourceGovernorConfig, ResourceLane, RouteDecision, RouteValidation,
    RoutedRequestHeader, SecurityContext, ShardLocalWritePlan, SlowQueryLogEntry, StorageMode,
    TcpClusterRelocationTransport, Transaction, TransportClusterRelocationDriver, TxLogHandle,
    DEFAULT_DISTRIBUTION_CONFIG, DEFAULT_SLOW_QUERY_THRESHOLD_MS, SCHEMA_BOOTSTRAP_NODE_LABEL,
    SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use bicdb_sql::{
    copy_cluster_role_catalog, ensure_primary_key_indexes, ensure_unique_constraint_indexes,
    format_pg_lsn, infer_parameter_types, infer_query_result_columns_without_from,
    infer_query_result_types, is_role_membership_ddl, parse_database_ddl,
    parse_pg_canonical_special, parse_transaction_control, pg_type_from_data_type,
    postgres_jsonb_text, routine_exception_counts_snapshot, split_sql_statements, DatabaseDdl,
    PgBinaryCodec, PgBitString, PgCanonicalValue, PgDate, PgFloat8, PgGeometric, PgInterval,
    PgIpAddress, PgMacAddress, PgNetwork, PgNetworkKind, PgNumeric, PgPoint, PgRange, PgRangeBound,
    PgSnapshot, PgTime, PgTimeTz, PgTimestamp, PgTsQuery, PgTsVector, SqlError, SqlErrorField,
    SqlResult, SqlRowStream, SqlSession, SqlSessionCatalogCache, SqlSessionDdlUndoLog,
    SqlSessionGucState, SqlSessionRuntime, SqlValue, TransactionControl,
    POSTGRES_COMPATIBILITY_VERSION, POSTGRES_COMPATIBILITY_VERSION_NUM,
};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rustls::pki_types::CertificateDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectName, SelectItem, SetExpr, Statement, TableFactor, TableObject, UnaryOperator, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::time;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, PgWireError>;

/// Derive PostgreSQL's numeric server version from its human-readable release.
/// PostgreSQL 10+ uses `major * 10000 + minor`; older releases use
/// `major * 10000 + minor * 100 + patch`.
pub fn derive_postgres_server_version_num(version: &str) -> std::result::Result<String, String> {
    let parts = version.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(format!(
            "invalid PostgreSQL server version `{version}`; expected MAJOR[.MINOR[.PATCH]]"
        ));
    }
    let numbers = parts
        .iter()
        .map(|part| {
            if !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(format!(
                    "invalid PostgreSQL server version `{version}`; components must be decimal integers"
                ));
            }
            part.parse::<u32>().map_err(|_| {
                format!("invalid PostgreSQL server version `{version}`; component is out of range")
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let major = numbers[0];
    if major == 0 || major > 99 {
        return Err(format!(
            "invalid PostgreSQL server version `{version}`; major must be 1..=99"
        ));
    }
    let minor = numbers.get(1).copied().unwrap_or(0);
    if minor > 99 {
        return Err(format!(
            "invalid PostgreSQL server version `{version}`; minor must be 0..=99"
        ));
    }
    let numeric = if major >= 10 {
        if numbers.len() > 2 {
            return Err(format!(
                "invalid PostgreSQL server version `{version}`; PostgreSQL 10+ versions have no patch component"
            ));
        }
        major * 10_000 + minor
    } else {
        let patch = numbers.get(2).copied().unwrap_or(0);
        if patch > 99 {
            return Err(format!(
                "invalid PostgreSQL server version `{version}`; patch must be 0..=99"
            ));
        }
        major * 10_000 + minor * 100 + patch
    };
    Ok(numeric.to_string())
}

const SSL_REQUEST: i32 = 80_877_103;
const CANCEL_REQUEST: i32 = 80_877_102;
const GSSENC_REQUEST: i32 = 80_877_104;
const PROTOCOL_V3: i32 = 196_608;
const PROTOCOL_V3_2: i32 = 196_610;
const PROTOCOL_MAJOR_V3: i32 = 3;
/// SCRAM-SHA-256 iteration count for newly written verifiers.
///
/// PostgreSQL's default is 4096, which is weak against an offline attack on
/// a leaked catalog — and the catalog stores the StoredKey, the softer
/// target next to the Argon2id password hash. Raised well above the
/// interoperable default; existing verifiers keep their recorded count, so
/// this applies as passwords are set.
const SCRAM_ITERATIONS: u32 = 32_768;
pub const CLEAN_SHUTDOWN_MARKER: &str = "server.shutdown";

/// How long a clean shutdown waits for background workers to exit before
/// giving up on them. Long enough for the slowest default poll interval,
/// short enough that a wedged worker cannot hang a shutdown.
pub(crate) const BACKGROUND_QUIESCE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);
pub const USER_CATALOG_FILE: &str = "server_users.json";
pub const CLUSTER_MANIFEST_FILE: &str = "bicdb-cluster.json";
const MAX_BACKGROUND_TASK_POLL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_AUTOMATIC_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const NAME_ARRAY_OID: i32 = 1003;
const COPY_IN_FLUSH_ROWS: usize = 1_000;
const COPY_IN_TRANSACTION_BATCH_ROWS: usize = 10_000;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum PgWireError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("bicdb error: {0}")]
    BicDb(#[from] bicdb_core::BicDbError),

    #[error("sql error: {0}")]
    Sql(#[from] bicdb_sql::SqlError),

    #[error("invalid PostgreSQL wire message: {0}")]
    Protocol(String),

    #[error("password authentication failed")]
    Authentication,

    #[error("database \"{0}\" does not exist")]
    DatabaseNotFound(String),

    #[error("database \"{0}\" already exists")]
    DatabaseAlreadyExists(String),

    #[error("server error: {0}")]
    Server(String),

    #[error("server error: {0}")]
    PersistentConnectionMemoryLimit(String),

    #[error("{0}")]
    QueryRejected(String),

    #[error("canceling statement due to user request")]
    QueryCanceled,

    #[error("canceling statement due to query timeout")]
    QueryTimedOut,

    #[error("current transaction is aborted, commands ignored until end of transaction block")]
    InFailedTransaction,
}

impl PgWireError {
    fn closes_connection(&self) -> bool {
        matches!(self, Self::PersistentConnectionMemoryLimit(_))
    }
}

#[derive(Clone, Debug)]
pub struct PgWireConfig {
    pub host: String,
    pub port: u16,
    /// PostgreSQL compatibility identity advertised to clients. This never
    /// changes BicDB's own build identity (`bicdb_version()`).
    pub postgres_server_version: String,
    pub postgres_server_version_num: String,
    /// Return a PostgreSQL-shaped `version()` banner for compatibility clients.
    /// This changes no authorization behavior; `bicdb_version()` remains
    /// truthful and immutable.
    pub postgres_version_banner: bool,
    pub max_connections: usize,
    /// Maximum simultaneously admitted sockets from one source address. This
    /// applies before startup/authentication so a slowloris cannot consume the
    /// entire global connection pool from one host.
    pub max_connections_per_ip: usize,
    pub max_pending_accepts: usize,
    pub max_active_queries: usize,
    pub max_queued_queries: usize,
    pub max_active_reads: usize,
    pub max_queued_reads: usize,
    pub max_active_writes: usize,
    pub max_queued_writes: usize,
    pub idle_timeout: Duration,
    /// Absolute wall-clock budget for startup, TLS negotiation, and
    /// authentication. Unlike `idle_timeout`, trickling bytes does not reset it.
    pub authentication_timeout: Duration,
    pub max_request_bytes: usize,
    pub max_result_rows: usize,
    pub query_timeout: Duration,
    pub overload_timeout: Duration,
    pub write_timeout: Duration,
    pub per_connection_memory_limit: usize,
    /// Build the compact primary-key/row-id registry required to hydrate
    /// row-id-addressed secondary-index results. Disable only for scan/direct
    /// primary-key/generation-backed FTS workloads whose startup must remain
    /// independent of corpus size.
    pub paged_rowid_registry: bool,
    pub require_auth: bool,
    pub auth_method: AuthMethod,
    /// None preserves the transport default: require PLUS with configured TLS,
    /// plain SCRAM without TLS. Explicit Require also requires authentication.
    pub channel_binding: Option<ChannelBindingPolicy>,
    pub allow_remote_no_auth: bool,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub require_tls: bool,
    pub tls_client_ca: Option<PathBuf>,
    pub flush_interval: Duration,
    pub checkpoint_interval: Duration,
    pub metrics_interval: Duration,
    pub shutdown_grace_period: Duration,
    pub security_context: Option<SecurityContext>,
    pub standby_read_only: bool,
    pub slow_query_log: Option<PathBuf>,
    pub slow_query_threshold: Duration,
    pub slow_query_redaction: RedactionConfig,
    /// Enable the first-party host driver for durable server-paged checkpoint
    /// schedules. Existing active schedules are resumed before new work starts.
    pub automatic_paged_checkpoint: bool,
    /// How often the host checks durable due times and shutdown state. This is
    /// not the checkpoint cadence; each schedule retains its own due time.
    pub automatic_paged_checkpoint_poll_interval: Duration,
    /// How often a new automatic vacuum pass may start (bounded steps run
    /// governor-paced in between). Dead-version reclamation is the
    /// autovacuum half the checkpoint loop does not cover.
    pub automatic_paged_vacuum_interval: Duration,
    pub automatic_paged_vacuum_poll_interval: Duration,
    pub automatic_paged_vacuum_limits: bicdb_core::PagedVacuumScheduleLimits,
    /// Fold an FTS index's unfolded tail once it crosses this many entries;
    /// 0 disables automatic folding. Unfolded tails are the classic ranked-
    /// search cliff, and folds also shrink the entry keyspace.
    pub automatic_fts_fold_threshold_entries: usize,
    pub automatic_fts_fold_poll_interval: Duration,
    /// Immutable page-work and resource envelope adopted by each newly started
    /// automatic checkpoint. Active schedules retain their persisted envelope.
    pub automatic_paged_checkpoint_limits: PagedCheckpointScheduleLimits,
    /// Drain exact B-tree/overflow hints on a separate background worker. The
    /// query path only submits to the hard-bounded, deduplicated queue.
    pub automatic_paged_read_ahead: bool,
    /// Idle/error polling cadence; a worker with queued work advances again
    /// immediately while the shared governor continues to admit it.
    pub automatic_paged_read_ahead_poll_interval: Duration,
    /// Immutable candidate, logical-I/O, and cooperative-duration envelope for
    /// every speculative read step.
    pub automatic_paged_read_ahead_limits: ReadAheadLimits,
    /// Explicit node-governor reservation and token-bucket charge per step.
    pub automatic_paged_read_ahead_demand: ResourceDemand,
    /// One node governor shared by automatic checkpoint and cluster repair work.
    pub resource_governor: ResourceGovernorConfig,
    /// Durable automatic range-integrity cadence, bounds, and resource demand.
    /// A changed value is adopted on restart only when no sweep is active.
    pub automatic_anti_entropy_limits: AutomaticRangeAntiEntropyLimits,
    /// Sync database persistence before acknowledgement. Disable only for
    /// explicitly non-durable diagnostic benchmarks.
    pub fsync: bool,
}

impl Default for PgWireConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5433,
            postgres_server_version: POSTGRES_COMPATIBILITY_VERSION.to_string(),
            postgres_server_version_num: POSTGRES_COMPATIBILITY_VERSION_NUM.to_string(),
            postgres_version_banner: false,
            max_connections: 100,
            max_connections_per_ip: 20,
            max_pending_accepts: 100,
            max_active_queries: default_max_active_queries(),
            max_queued_queries: default_max_queued_queries(),
            max_active_reads: default_max_active_queries(),
            max_queued_reads: default_max_queued_queries(),
            max_active_writes: default_max_active_queries(),
            max_queued_writes: default_max_queued_writes(),
            idle_timeout: Duration::from_secs(300),
            authentication_timeout: Duration::from_secs(10),
            max_request_bytes: 10 * 1024 * 1024,
            max_result_rows: 100_000,
            // PostgreSQL's statement_timeout defaults to 0 (disabled). Keep
            // the operator override available, but do not impose a timeout on
            // clients that did not ask for one.
            query_timeout: Duration::ZERO,
            overload_timeout: Duration::from_millis(30_000),
            write_timeout: Duration::from_millis(30_000),
            per_connection_memory_limit: 16 * 1024 * 1024,
            paged_rowid_registry: true,
            require_auth: false,
            auth_method: AuthMethod::ScramSha256,
            channel_binding: None,
            allow_remote_no_auth: false,
            tls_cert: None,
            tls_key: None,
            require_tls: false,
            tls_client_ca: None,
            flush_interval: Duration::from_secs(5),
            checkpoint_interval: Duration::from_secs(60),
            metrics_interval: Duration::from_secs(60),
            shutdown_grace_period: Duration::from_secs(10),
            security_context: None,
            standby_read_only: true,
            slow_query_log: None,
            slow_query_threshold: Duration::from_millis(DEFAULT_SLOW_QUERY_THRESHOLD_MS),
            slow_query_redaction: RedactionConfig::default(),
            automatic_paged_checkpoint: true,
            automatic_paged_checkpoint_poll_interval: Duration::from_millis(100),
            automatic_paged_vacuum_interval: Duration::from_secs(3_600),
            automatic_paged_vacuum_poll_interval: Duration::from_millis(250),
            automatic_paged_vacuum_limits: bicdb_core::PagedVacuumScheduleLimits::default(),
            automatic_fts_fold_threshold_entries: 16_384,
            automatic_fts_fold_poll_interval: Duration::from_secs(60),
            automatic_paged_checkpoint_limits: PagedCheckpointScheduleLimits::default(),
            automatic_paged_read_ahead: true,
            automatic_paged_read_ahead_poll_interval: Duration::from_millis(2),
            automatic_paged_read_ahead_limits: ReadAheadLimits::default(),
            automatic_paged_read_ahead_demand: ResourceDemand {
                memory_bytes: 64 * 1024,
                io_bytes: ReadAheadLimits::default().max_io_bytes,
                cpu_slots: 1,
                io_charge_bytes: ReadAheadLimits::default().max_io_bytes,
            },
            resource_governor: ResourceGovernorConfig::default(),
            automatic_anti_entropy_limits: AutomaticRangeAntiEntropyLimits::default(),
            fsync: true,
        }
    }
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

fn database_config_for_server(path: &Path, config: &PgWireConfig) -> Result<DbConfig> {
    let mut db_config = DbConfig::default()
        .with_fsync(config.fsync)
        .with_paged_rowid_registry(config.paged_rowid_registry);
    if !path.join(DEFAULT_DISTRIBUTION_CONFIG).exists() {
        return Ok(db_config);
    }
    let distribution = load_distribution_config(path)?;
    db_config = db_config.with_storage_mode(StorageMode::ServerPaged);
    let tls = if distribution.transport.dev_localhost_plaintext {
        Some(ReplicationTlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            require_client_cert: false,
            dev_localhost_plaintext: true,
        })
    } else {
        distribution.transport.tls.clone()
    };
    let replication = ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Primary,
        listen_addr: Some(distribution.node_address.clone()),
        advertise_addr: Some(distribution.node_address.clone()),
        tls,
        cluster_id: distribution.cluster_id.as_str().to_string(),
        node_id: distribution.node_id.as_str().to_string(),
        max_frame_bytes: distribution.transport.max_frame_bytes,
        ..ReplicationConfig::default()
    };
    db_config = db_config
        .with_replication(replication)
        .with_required_commit_admission(true);
    Ok(db_config)
}

#[cfg(test)]
mod database_config_tests {
    use super::*;

    #[test]
    fn server_can_disable_the_corpus_sized_rowid_registry() {
        let root = tempfile::tempdir().unwrap();
        let config = PgWireConfig {
            paged_rowid_registry: false,
            ..PgWireConfig::default()
        };

        let database = database_config_for_server(root.path(), &config).unwrap();

        assert!(!database.paged_rowid_registry);
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    Cleartext,
    ScramSha256,
}

/// Server-side SCRAM channel-binding policy. This does not relax TLS policy.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChannelBindingPolicy {
    Require,
    Prefer,
    Disable,
}

impl PgWireConfig {
    pub fn effective_channel_binding_policy(&self) -> ChannelBindingPolicy {
        self.channel_binding.unwrap_or(if self.tls_cert.is_some() {
            ChannelBindingPolicy::Require
        } else {
            ChannelBindingPolicy::Disable
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserCatalog {
    version: u32,
    users: HashMap<String, StoredUser>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredUser {
    username: String,
    salt_hex: String,
    hash_hex: String,
    algorithm: String,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    scram_salt_hex: Option<String>,
    #[serde(default)]
    scram_stored_key_hex: Option<String>,
    #[serde(default)]
    scram_server_key_hex: Option<String>,
    #[serde(default)]
    scram_iterations: Option<u32>,
    #[serde(default)]
    security_identity: Option<PgWireUserIdentity>,
}

/// Host-authorized identity attributes persisted alongside a pgwire login.
/// They are read only after password/SCRAM authentication succeeds and cannot
/// be supplied through startup parameters or SQL.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PgWireUserIdentity {
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
}

impl PgWireUserIdentity {
    pub fn new(user_id: impl Into<String>, tenant_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            tenant_id: tenant_id.into(),
            ..Self::default()
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_workspace_id(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }

    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }
}

impl Default for UserCatalog {
    fn default() -> Self {
        Self {
            version: 1,
            users: HashMap::new(),
        }
    }
}

pub fn create_user(path: impl AsRef<Path>, username: &str, password: &str) -> Result<()> {
    create_user_record(path.as_ref(), username, password, None, false)
}

fn create_user_record(
    path: &Path,
    username: &str,
    password: &str,
    security_identity: Option<PgWireUserIdentity>,
    sql_origin: bool,
) -> Result<()> {
    validate_username(username)?;
    if security_identity
        .as_ref()
        .is_some_and(|identity| identity.user_id.trim().is_empty())
    {
        return Err(PgWireError::Server(
            "security identity user_id must not be empty".to_string(),
        ));
    }
    if password.is_empty() {
        return Err(PgWireError::Server(
            "password must not be empty".to_string(),
        ));
    }
    let _lock = lock_user_catalog(path)?;
    if sql_origin {
        check_sql_credential_mutation(path)?;
    }
    let mut catalog = load_user_catalog(path)?;
    // A password change (`ALTER ROLE ... PASSWORD`, `bicdb user create` on an
    // existing login) must not silently unbind the login's trusted identity:
    // that binding is host authority, set only through
    // `set_user_security_identity`, and losing it turns a tenant-bound login
    // into an unbound one without anyone asking for it.
    let security_identity = security_identity.or_else(|| {
        catalog
            .users
            .get(username)
            .and_then(|existing| existing.security_identity.clone())
    });
    let disabled = catalog
        .users
        .get(username)
        .is_some_and(|user| user.disabled);
    let user = password_record(username, password, security_identity, disabled)?;
    if !catalog.users.contains_key(username) {
        delegation::write_policy_locked(path, username, None)?;
    }
    catalog.users.insert(username.to_owned(), user);
    persist_user_catalog(path, &catalog)
}

fn password_record(
    username: &str,
    password: &str,
    security_identity: Option<PgWireUserIdentity>,
    disabled: bool,
) -> Result<StoredUser> {
    let salt = *Uuid::new_v4().as_bytes();
    let hash = hash_password(password, &salt)?;
    let scram_salt = *Uuid::new_v4().as_bytes();
    let scram = ScramVerifier::from_password(password, &scram_salt, SCRAM_ITERATIONS)?;
    Ok(StoredUser {
        username: username.to_owned(),
        salt_hex: hex::encode(salt),
        hash_hex: hex::encode(hash),
        algorithm: "argon2id".to_owned(),
        disabled,
        scram_salt_hex: Some(hex::encode(scram_salt)),
        scram_stored_key_hex: Some(hex::encode(scram.stored_key)),
        scram_server_key_hex: Some(hex::encode(scram.server_key)),
        scram_iterations: Some(scram.iterations),
        security_identity,
    })
}

/// Creates a login and atomically binds its host-authorized security identity.
pub fn create_user_with_identity(
    path: impl AsRef<Path>,
    username: &str,
    password: &str,
    identity: PgWireUserIdentity,
) -> Result<()> {
    create_user_record(path.as_ref(), username, password, Some(identity), false)
}

/// Replaces the security identity used for future authenticated connections.
/// Existing connections retain their immutable context until disconnect.
pub fn set_user_security_identity(
    path: impl AsRef<Path>,
    username: &str,
    identity: PgWireUserIdentity,
) -> Result<()> {
    validate_username(username)?;
    if identity.user_id.trim().is_empty() {
        return Err(PgWireError::Server(
            "security identity user_id must not be empty".to_string(),
        ));
    }
    let path = path.as_ref();
    let _lock = lock_user_catalog(path)?;
    let mut catalog = load_user_catalog(path)?;
    let user = catalog.users.get_mut(username).ok_or_else(|| {
        PgWireError::Server(format!("BicDB server user `{username}` does not exist"))
    })?;
    user.security_identity = Some(identity);
    persist_user_catalog(path, &catalog)
}

pub fn user_exists(path: impl AsRef<Path>, username: &str) -> Result<bool> {
    Ok(load_user_catalog(path.as_ref())?
        .users
        .contains_key(username))
}

#[derive(Debug)]
pub struct ServerStatsSnapshot {
    pub max_connections: usize,
    pub active_connections: usize,
    pub peak_active_connections: usize,
    pub total_connections: u64,
    pub rejected_connections: u64,
    pub max_pending_accepts: usize,
    pub max_active_queries: usize,
    pub max_queued_queries: usize,
    pub active_queries: usize,
    pub peak_active_queries: usize,
    pub queued_queries: usize,
    pub queued_queries_max: usize,
    pub rejected_queries: u64,
    pub max_active_reads: usize,
    pub active_reads: usize,
    pub peak_active_reads: usize,
    pub max_queued_reads: usize,
    pub queued_reads: usize,
    pub queued_reads_max: usize,
    pub max_active_writes: usize,
    pub active_writes: usize,
    pub peak_active_writes: usize,
    pub queued_writes: usize,
    pub queued_writes_max: usize,
    pub query_queue_wait_p50_ns: u64,
    pub query_queue_wait_p95_ns: u64,
    pub query_queue_wait_p99_ns: u64,
    pub queries_executed: u64,
    pub failed_queries: u64,
    pub canceled_queries: u64,
    pub timed_out_queries: u64,
    pub last_cancel_at: Option<i64>,
    pub last_cancel_connection_id: Option<u64>,
    pub last_cancel_reason: Option<String>,
    pub last_cancel_sqlstate: Option<String>,
    pub uptime_seconds: i64,
    pub db_size_bytes: u64,
    pub memory_estimate_bytes: u64,
    pub last_checkpoint: Option<i64>,
    pub writes_executed: u64,
    pub proc_neword: u64,
    pub proc_payment: u64,
    pub proc_delivery: u64,
    pub proc_orderstatus: u64,
    pub proc_stocklevel: u64,
    pub routine_serialization_failure: u64,
    pub routine_deadlock_detected: u64,
    pub routine_no_data_found: u64,
    pub routine_other: u64,
    pub wal_written_seq: u64,
    pub wal_write_calls: u64,
    pub wal_sync_calls: u64,
    pub wal_bytes_written: u64,
    pub wal_commits_written: u64,
    pub wal_max_batch_commits: u64,
    pub max_queued_writes: usize,
    pub write_queue_depth: usize,
    pub write_queue_depth_max: usize,
    pub write_wait_total_ns: u64,
    pub write_wait_max_ns: u64,
    pub write_execution_total_ns: u64,
    pub write_execution_max_ns: u64,
    pub write_rejected_count: u64,
    pub write_timed_out_count: u64,
    pub db_lock_acquisitions: u64,
    pub db_lock_wait_total_ns: u64,
    pub db_lock_wait_max_ns: u64,
    pub db_lock_hold_total_ns: u64,
    pub db_lock_hold_max_ns: u64,
    pub db_read_lock_acquisitions: u64,
    pub db_read_lock_wait_total_ns: u64,
    pub db_read_lock_wait_max_ns: u64,
    pub db_read_lock_hold_total_ns: u64,
    pub db_read_lock_hold_max_ns: u64,
    pub db_write_lock_acquisitions: u64,
    pub db_write_lock_wait_total_ns: u64,
    pub db_write_lock_wait_max_ns: u64,
    pub db_write_lock_hold_total_ns: u64,
    pub db_write_lock_hold_max_ns: u64,
    pub rows_streamed: u64,
    pub bytes_streamed: u64,
    pub cursor_count: usize,
    pub cursor_memory_bytes: usize,
    pub spilled_to_disk_bytes: u64,
    pub ha_status: Option<HaStatus>,
}

#[derive(Clone, Debug)]
pub struct ServerConnectionSnapshot {
    pub connection_id: u64,
    pub user: String,
    pub peer_addr: Option<String>,
    pub connected_at: i64,
    pub last_query_at: Option<i64>,
    pub active_query: Option<String>,
    pub queries_executed: u64,
    pub failed_queries: u64,
    pub in_transaction: bool,
}

#[derive(Debug)]
pub struct PgWireServer {
    path: PathBuf,
    auth_path: PathBuf,
    config: PgWireConfig,
    db: Arc<RwLock<BicDb>>,
    /// Optional distributed point router shared by all sessions on this
    /// gateway. Generic transport execution lives in `ClusterPointGateway`;
    /// pgwire sessions use these hooks to route and destination-fence work.
    distribution_router: Option<ClusterRequestRouter>,
    /// Shared node-level admission state for first-party background tasks.
    /// Keeping one governor here makes compaction and repair contend inside the
    /// same declared background envelope instead of each assuming it owns it.
    resource_governor: ResourceGovernor,
    // Decoupled group-commit writer handle: lets the durable WAL write run after
    // the database write lock is released so concurrent commits pipeline.
    tx_log: TxLogHandle,
    started_at: Instant,
    // Server-wide gate for the concurrent (read-lock execute) write path. The
    // credit falls when shared commits conflict and rises when they succeed; the
    // shared path is used while credit is positive, with occasional probes while
    // negative so the server recovers when contention subsides. Under sustained
    // write contention this collapses the shared path back to serial execution,
    // avoiding writer starvation of the database write lock.
    shared_write_credit: AtomicI64,
    shared_write_probe: AtomicU64,
    // Bound on concurrent shared (read-lock) write executions. Continuous
    // read-lock execution would otherwise starve the database write lock that
    // commits need; capping the number of in-flight shared executions lets the
    // write lock interleave while still overlapping execution.
    active_shared_writes: AtomicUsize,
    active_connections: AtomicUsize,
    active_connections_by_ip: Mutex<HashMap<IpAddr, usize>>,
    /// Bounds the short-lived tasks which half-close and drain refused TCP
    /// connections. Draining the already-sent startup packet prevents Linux
    /// from replacing the PostgreSQL FATAL frame with an RST, while this
    /// semaphore keeps a connection flood from creating unbounded drain work.
    rejection_drains: Arc<tokio::sync::Semaphore>,
    peak_active_connections: AtomicUsize,
    total_connections: AtomicU64,
    rejected_connections: AtomicU64,
    /// Epoch-millis of the last saturation operator hint, shared across all
    /// admission gates so hints stay rate-limited as a group.
    saturation_hint_millis: AtomicU64,
    active_queries: AtomicUsize,
    peak_active_queries: AtomicUsize,
    queued_queries: AtomicUsize,
    queued_queries_max: AtomicUsize,
    rejected_queries: AtomicU64,
    active_reads: AtomicUsize,
    peak_active_reads: AtomicUsize,
    queued_reads: AtomicUsize,
    queued_reads_max: AtomicUsize,
    active_writes: AtomicUsize,
    peak_active_writes: AtomicUsize,
    queued_writes: AtomicUsize,
    queued_writes_max: AtomicUsize,
    query_queue_wait_samples_ns: Mutex<Vec<u64>>,
    next_connection_id: AtomicU64,
    queries_executed: AtomicU64,
    failed_queries: AtomicU64,
    canceled_queries: AtomicU64,
    timed_out_queries: AtomicU64,
    writes_executed: AtomicU64,
    proc_neword: AtomicU64,
    proc_payment: AtomicU64,
    proc_delivery: AtomicU64,
    proc_orderstatus: AtomicU64,
    proc_stocklevel: AtomicU64,
    write_queue_depth: AtomicUsize,
    write_queue_depth_max: AtomicUsize,
    write_wait_total_ns: AtomicU64,
    write_wait_max_ns: AtomicU64,
    write_execution_total_ns: AtomicU64,
    write_execution_max_ns: AtomicU64,
    write_rejected_count: AtomicU64,
    write_timed_out_count: AtomicU64,
    db_lock_acquisitions: AtomicU64,
    db_lock_wait_total_ns: AtomicU64,
    db_lock_wait_max_ns: AtomicU64,
    db_lock_hold_total_ns: AtomicU64,
    db_lock_hold_max_ns: AtomicU64,
    db_read_lock_acquisitions: AtomicU64,
    db_read_lock_wait_total_ns: AtomicU64,
    db_read_lock_wait_max_ns: AtomicU64,
    db_read_lock_hold_total_ns: AtomicU64,
    db_read_lock_hold_max_ns: AtomicU64,
    db_write_lock_acquisitions: AtomicU64,
    db_write_lock_wait_total_ns: AtomicU64,
    db_write_lock_wait_max_ns: AtomicU64,
    db_write_lock_hold_total_ns: AtomicU64,
    db_write_lock_hold_max_ns: AtomicU64,
    rows_streamed: AtomicU64,
    bytes_streamed: AtomicU64,
    active_cursors: AtomicUsize,
    cursor_memory_bytes: AtomicUsize,
    spilled_to_disk_bytes: AtomicU64,
    last_checkpoint: Mutex<Option<i64>>,
    connections: Mutex<HashMap<u64, ServerConnectionSnapshot>>,
    cancel_tokens: Mutex<HashMap<(i32, i32), Arc<AtomicBool>>>,
    last_cancel: Mutex<Option<CancelMetadata>>,
    shutdown: AtomicBool,
    database_background_started: AtomicBool,
    host_background_started: AtomicBool,
    /// Optional host-level services installed by the embedding binary before
    /// serving begins. Protocol lifecycle invokes these services, but never
    /// selects or constructs a controller implementation itself.
    host_services: Mutex<Vec<Arc<dyn PgWireHostService>>>,
    /// Number of background workers currently holding this server.
    ///
    /// Shutdown is not complete while any of them is live: they hold an
    /// `Arc<PgWireServer>`, and through it the database, and the flush and
    /// checkpoint loops still write. Reporting a clean shutdown before they
    /// have exited claims the database is closed while it is still open.
    background_workers: std::sync::atomic::AtomicUsize,
    tls_config: Option<Arc<PgWireTlsConfig>>,
    // LISTEN/NOTIFY dispatch (see NotificationBus).
    notifications: Arc<Mutex<NotificationBus>>,
    advisory_locks: Arc<AdvisoryLockManager>,
    cluster: Option<Weak<PgWireCluster>>,
}

/// A host-level service whose lifecycle is attached to a pgwire server.
///
/// Implementations are selected by the embedding binary. This keeps protocol
/// serving independent from fleet, placement, migration, or other control
/// plane policy while still providing a stable place to start such services.
pub trait PgWireHostService: std::fmt::Debug + Send + Sync + 'static {
    /// Stable diagnostic name used to reject duplicate registrations.
    fn name(&self) -> &'static str;

    /// Start the service. Implementations that need a long-running worker
    /// should spawn it here and observe [`PgWireHostContext::is_shutdown_requested`].
    fn start(&self, context: PgWireHostContext) -> Result<()>;
}

/// Narrow server context exposed to optional host-controller implementations.
///
/// The context deliberately exposes database mechanisms and lifecycle signals,
/// not ownership of listener/protocol internals.
#[derive(Clone, Debug)]
pub struct PgWireHostContext {
    server: Arc<PgWireServer>,
}

impl PgWireHostContext {
    pub fn config(&self) -> &PgWireConfig {
        self.server.config()
    }

    pub fn database_path(&self) -> &Path {
        &self.server.path
    }

    pub fn database(&self) -> Arc<RwLock<BicDb>> {
        Arc::clone(&self.server.db)
    }

    pub fn resource_governor(&self) -> ResourceGovernor {
        self.server.resource_governor.clone()
    }

    pub fn distribution_router(&self) -> Option<&ClusterRequestRouter> {
        self.server.distribution_router()
    }

    pub fn request_shutdown(&self) {
        self.server.request_shutdown();
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.server.is_shutdown_requested()
    }

    /// Install a validated topology into the protocol router, when this server
    /// was opened as a distribution member.
    pub fn install_distribution_topology(
        &self,
        topology: bicdb_core::ClusterTopology,
    ) -> Result<()> {
        if let Some(router) = self.server.distribution_router.as_ref() {
            router.install_topology(topology)?;
        }
        Ok(())
    }

    /// Start BicDB's public distribution data plane with an explicitly
    /// constructed controller runtime.
    ///
    /// The runtime contains no built-in placement policy. Community hosts can
    /// use manual convergence, while external controllers inject a planner
    /// through `ClusterSupervisor::with_planner`.
    pub fn start_distribution_controller(&self, controller: ClusterSupervisor) -> Result<()> {
        if self.server.distribution_router.is_some() {
            start_distribution_supervisor(Arc::clone(&self.server), controller);
        }
        Ok(())
    }

    /// Start controller work under pgwire's graceful-shutdown accounting.
    ///
    /// The callback receives only the public host context. This permits an
    /// out-of-tree controller to own its policy and scheduling loop without
    /// reaching into protocol internals or keeping a database directory open
    /// beyond the configured shutdown grace period.
    pub fn spawn_background_task(
        &self,
        task: impl FnOnce(PgWireHostContext) + Send + 'static,
    ) -> thread::JoinHandle<()> {
        let context = self.clone();
        thread::spawn(move || {
            let _worker = BackgroundWorker::register(Arc::clone(&context.server));
            task(context);
        })
    }

    /// Sleep in bounded slices and return `false` when shutdown interrupts the
    /// interval.
    pub fn sleep_until_shutdown(&self, total: Duration) -> bool {
        let slice = Duration::from_millis(25);
        let started = Instant::now();
        while started.elapsed() < total {
            if self.is_shutdown_requested() {
                return false;
            }
            let remaining = total.saturating_sub(started.elapsed());
            thread::sleep(remaining.min(slice));
        }
        !self.is_shutdown_requested()
    }
}

/// Community distribution host service.
///
/// It starts the replication/data-plane lifecycle and advances relocations
/// that an operator explicitly created. It does not choose placements, retry
/// failed operations, or rebalance a fleet automatically.
#[derive(Debug, Default)]
pub struct ManualDistributionHostService;

impl PgWireHostService for ManualDistributionHostService {
    fn name(&self) -> &'static str {
        "manual-distribution"
    }

    fn start(&self, context: PgWireHostContext) -> Result<()> {
        context.start_distribution_controller(ClusterSupervisor::new(ClusterSupervisorConfig {
            // Metadata consensus owns refreshes; the controller runs on an
            // ephemeral fork of committed state.
            refresh_topology_each_tick: false,
            ..ClusterSupervisorConfig::default()
        })?)
    }
}

pub fn serve(path: impl AsRef<Path>, config: PgWireConfig) -> Result<()> {
    serve_with_host_services(path, config, Vec::new())
}

/// Serve a database with explicitly selected host-level services.
pub fn serve_with_host_services(
    path: impl AsRef<Path>,
    config: PgWireConfig,
    host_services: Vec<Arc<dyn PgWireHostService>>,
) -> Result<()> {
    let address = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(address)?;
    // Open the database (recovery can take many seconds on a large dataset) on a
    // background thread while a startup gate answers any early connections with PG
    // SQLSTATE 57P03 ("the database system is starting up"), exactly like
    // PostgreSQL. Clients/poolers then retry instead of stalling on a silent hang.
    let server = open_with_startup_gate(&listener, path, config)?;
    server.install_host_services(host_services)?;
    install_process_shutdown_handler(server.clone())?;
    serve_existing_listener(server, listener)
}

pub fn serve_cluster(
    root: impl AsRef<Path>,
    default_database: impl Into<String>,
    config: PgWireConfig,
) -> Result<()> {
    serve_cluster_with_host_services(root, default_database, config, Vec::new())
}

/// Serve a multi-database root with explicitly selected host-level services.
pub fn serve_cluster_with_host_services(
    root: impl AsRef<Path>,
    default_database: impl Into<String>,
    config: PgWireConfig,
    host_services: Vec<Arc<dyn PgWireHostService>>,
) -> Result<()> {
    let address = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(address)?;
    let cluster = PgWireCluster::open(root, default_database, config)?;
    let server = cluster.default_server()?;
    server.install_host_services(host_services)?;
    install_process_shutdown_handler(server.clone())?;
    serve_existing_listener(server, listener)
}

/// Open the server on a background thread; until it is ready, accept connections on
/// `listener` and reject each with a `57P03` "starting up" error. Returns the opened
/// server (or propagates the open error).
fn open_with_startup_gate(
    listener: &TcpListener,
    path: impl AsRef<Path>,
    config: PgWireConfig,
) -> Result<Arc<PgWireServer>> {
    let path = path.as_ref().to_path_buf();
    let max_request_bytes = config.max_request_bytes;
    let ready = Arc::new(AtomicBool::new(false));
    let ready_for_open = ready.clone();
    let open_handle = std::thread::spawn(move || {
        let result = PgWireServer::open(path, config);
        ready_for_open.store(true, Ordering::SeqCst);
        result
    });

    let prev_nonblocking = listener.set_nonblocking(true);
    while !ready.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                std::thread::spawn(move || {
                    let _ = reject_connection_starting_up(stream, max_request_bytes);
                });
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(25)),
        }
    }
    // Restore blocking mode if we changed it; serve_existing_listener will set its
    // own mode regardless.
    if prev_nonblocking.is_ok() {
        let _ = listener.set_nonblocking(false);
    }

    open_handle
        .join()
        .map_err(|_| PgWireError::Server("database open thread panicked".to_string()))?
}

/// Minimal PG startup handshake that answers with a fatal `57P03` "starting up"
/// error. Handles the SSLRequest negotiation (declines TLS for the transient
/// startup window) and ignores CancelRequest.
fn reject_connection_starting_up(stream: TcpStream, max_request_bytes: usize) -> Result<()> {
    stream.set_nonblocking(false)?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut client = ClientStream::plain(stream);
    loop {
        let payload = read_startup_payload(&mut client, max_request_bytes)?;
        if payload.len() < 4 {
            return Ok(());
        }
        match i32::from_be_bytes(payload[0..4].try_into().unwrap()) {
            SSL_REQUEST => {
                client.write_all(b"N")?;
                client.flush()?;
            }
            CANCEL_REQUEST => return Ok(()),
            _ => {
                let _ = error_response_with_fields(
                    &mut client,
                    "FATAL",
                    "57P03",
                    "the database system is starting up",
                    &[],
                );
                let _ = client.flush();
                return Ok(());
            }
        }
    }
}

pub fn serve_addr(path: impl AsRef<Path>, address: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(address)?;
    serve_listener(path, listener)
}

pub fn serve_listener(path: impl AsRef<Path>, listener: TcpListener) -> Result<()> {
    serve_listener_with_config(path, listener, PgWireConfig::default())
}

pub fn serve_listener_with_config(
    path: impl AsRef<Path>,
    listener: TcpListener,
    config: PgWireConfig,
) -> Result<()> {
    let server = PgWireServer::open(path, config)?;
    serve_existing_listener(server, listener)
}

pub fn serve_cluster_listener_with_config(
    root: impl AsRef<Path>,
    default_database: impl Into<String>,
    listener: TcpListener,
    config: PgWireConfig,
) -> Result<()> {
    let cluster = PgWireCluster::open(root, default_database, config)?;
    serve_existing_listener(cluster.default_server()?, listener)
}

fn install_process_shutdown_handler(server: Arc<PgWireServer>) -> Result<()> {
    ctrlc::set_handler(move || {
        log_operational_event("server.shutdown_signal", "info", json!({}));
        server.request_shutdown();
    })
    .map_err(|error| PgWireError::Server(format!("install shutdown signal handler: {error}")))
}

pub fn serve_existing_listener(server: Arc<PgWireServer>, listener: TcpListener) -> Result<()> {
    if let Ok(address) = listener.local_addr() {
        log_operational_event(
            "server.listening",
            "info",
            json!({ "address": address.to_string() }),
        );
    }
    listener.set_nonblocking(true)?;
    start_background_tasks(server.clone())?;
    let worker_threads = server
        .config
        .max_active_queries
        .max(2)
        .min(default_max_active_queries().max(2));
    let max_blocking_threads = server
        .config
        .max_connections
        .saturating_add(server.config.max_active_queries)
        .saturating_add(8)
        .max(8);
    let runtime = TokioRuntimeBuilder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .enable_io()
        .enable_time()
        .build()
        .map_err(|error| PgWireError::Server(format!("build pgwire runtime: {error}")))?;
    let grace = runtime_shutdown_grace(&server);
    let result = runtime.block_on(async move {
        let listener = TokioTcpListener::from_std(listener)?;
        serve_existing_listener_async(server, listener).await
    });
    // Bound the teardown. Dropping a runtime waits FOREVER for blocking
    // tasks, and every connection handler is one — so a single connection
    // parked in a socket read or a long query kept the process alive past
    // the declared grace period with nothing left to do but SIGKILL it.
    // `shutdown_timeout` detaches whatever has not finished by then.
    runtime.shutdown_timeout(grace);
    result
}

/// How long teardown may wait for in-flight connection work. Slightly beyond
/// the connection-drain grace so the drain gets its full window first.
fn runtime_shutdown_grace(server: &Arc<PgWireServer>) -> Duration {
    server
        .config
        .shutdown_grace_period
        .saturating_add(Duration::from_secs(2))
}

/// One line, at startup, with every admission limit and the flag that sets
/// it. The defaults are HOST-SIZED (`max_active_queries` = this machine's
/// core count) — fine for a laptop, silently throttling for a production
/// fleet — so the effective numbers must be visible at the only moment an
/// operator is reliably looking.
fn announce_effective_limits(server: &PgWireServer, listener: &TokioTcpListener) {
    let address = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    let config = &server.config;
    eprintln!(
        "bicdb pgwire listening on {address} — effective limits: \
         --max-connections={} --max-pending-accepts={} --max-active-queries={} \
         (reads {} / writes {}) --max-queued-queries={} (reads {} / writes {}). \
         Defaults are host-sized; set these explicitly for production.",
        config.max_connections,
        config.max_pending_accepts,
        config.max_active_queries,
        config.max_active_reads,
        config.max_active_writes,
        config.max_queued_queries,
        config.max_queued_reads,
        config.max_queued_writes,
    );
    log_operational_event(
        "server.limits",
        "info",
        json!({
            "address": address,
            "max_connections": config.max_connections,
            "max_pending_accepts": config.max_pending_accepts,
            "max_active_queries": config.max_active_queries,
            "max_active_reads": config.max_active_reads,
            "max_active_writes": config.max_active_writes,
            "max_queued_queries": config.max_queued_queries,
            "max_queued_reads": config.max_queued_reads,
            "max_queued_writes": config.max_queued_writes,
        }),
    );
}

async fn serve_existing_listener_async(
    server: Arc<PgWireServer>,
    listener: TokioTcpListener,
) -> Result<()> {
    announce_effective_limits(&server, &listener);
    while !server.is_shutdown_requested() {
        match time::timeout(Duration::from_millis(25), listener.accept()).await {
            Ok(Ok((stream, peer_addr))) => {
                let admission = match server.admit_connection(peer_addr.ip()) {
                    Ok(admission) => admission,
                    Err(limit) => {
                        log_operational_event(
                            "connection.rejected",
                            "warn",
                            json!({
                                "peer_addr": peer_addr.to_string(),
                                "reason": limit.reason()
                            }),
                        );
                        server.saturation_operator_hint(|| {
                            limit.operator_hint(server.rejected_connections.load(Ordering::SeqCst))
                        });
                        let drain_permit = server.rejection_drains.clone().try_acquire_owned().ok();
                        if drain_permit.is_some() {
                            let max_drain_bytes = server.config.max_request_bytes;
                            tokio::spawn(reject_connection_at_capacity_async(
                                stream,
                                max_drain_bytes,
                                drain_permit,
                            ));
                        } else {
                            // Backpressure the accept loop once the bounded
                            // task budget is exhausted. Draining inline keeps
                            // the FATAL frame reliable without creating more
                            // concurrent work; the same timeout bounds peers
                            // which intentionally withhold input.
                            reject_connection_at_capacity_async(
                                stream,
                                server.config.max_request_bytes,
                                None,
                            )
                            .await;
                        }
                        continue;
                    }
                };
                let server = server.clone();
                tokio::spawn(async move {
                    if let Err(error) =
                        handle_client_async_with_admission(stream, peer_addr, server, admission)
                            .await
                    {
                        log_operational_event(
                            "connection.error",
                            "error",
                            json!({ "error": error.to_string() }),
                        );
                    }
                });
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {}
        }
    }
    wait_for_connection_drain_async(&server).await;
    server.close()?;
    Ok(())
}

async fn wait_for_connection_drain_async(server: &PgWireServer) {
    let started = Instant::now();
    while server.active_connections.load(Ordering::SeqCst) > 0 {
        if started.elapsed() >= server.config.shutdown_grace_period {
            // The grace period used to be advisory: it waited on a COUNT and
            // then returned, cancelling nothing, so the runtime drop below
            // inherited the stall. Cancel every in-flight query first — that
            // releases connections parked in long statements — and report
            // what is still live so the operator sees why teardown lingered.
            let remaining = server.active_connections.load(Ordering::SeqCst);
            server.cancel_all_active_queries();
            eprintln!(
                "bicdb server shutdown grace period expired with {remaining} active \
                 connections; cancelling in-flight queries and detaching"
            );
            log_operational_event(
                "server.shutdown_grace_expired",
                "warn",
                json!({ "active_connections": remaining }),
            );
            return;
        }
        time::sleep(Duration::from_millis(25)).await;
    }
}

pub fn handle_client(stream: TcpStream, db_path: impl AsRef<Path>) -> Result<()> {
    let server = PgWireServer::open(db_path, PgWireConfig::default())?;
    let peer_addr = stream.peer_addr().ok();
    handle_client_with_server(
        stream,
        peer_addr.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))),
        server,
    )
}

/// Per-connection output is buffered and flushed once per request rather than
/// written message-by-message. The raw `TcpStream` is unbuffered, so a reply built
/// from N protocol messages issued N×3 `write()` syscalls (a `SELECT 1` reply is
/// 4 messages ≈ 12 writes) — the dominant per-statement cost (the TCP send path
/// shows up as ~25-30% of CPU under a `SELECT 1` load). Batching collapses that to
/// one write. The buffer is flushed before every read (so a request/response peer
/// never blocks on bytes we still hold) and whenever it passes this threshold (so
/// large result sets don't buffer unbounded — mirrors PostgreSQL's ~8KB output
/// buffer; big replies just use several writes).
const CLIENT_WRITE_FLUSH_THRESHOLD: usize = 32 * 1024;

enum ClientStream {
    Plain(TcpStream, Vec<u8>, Option<Instant>),
    Tls(
        Box<StreamOwned<ServerConnection, TcpStream>>,
        Vec<u8>,
        Option<Instant>,
    ),
    Upgrading,
}

impl ClientStream {
    fn plain(stream: TcpStream) -> Self {
        Self::Plain(stream, Vec::new(), None)
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.tcp().set_nonblocking(nonblocking)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.tcp().set_read_timeout(timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.tcp().set_write_timeout(timeout)
    }

    fn set_read_deadline(&mut self, deadline: Option<Instant>) {
        match self {
            Self::Plain(_, _, current) | Self::Tls(_, _, current) => *current = deadline,
            Self::Upgrading => {}
        }
    }

    fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream, _, _) => stream,
            Self::Tls(stream, _, _) => stream.get_ref(),
            Self::Upgrading => panic!("client stream is temporarily upgrading"),
        }
    }

    fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(..))
    }

    fn into_plain(mut self) -> Result<TcpStream> {
        // Drain buffered output before handing the raw socket back to the async
        // read loop: the client is waiting on this reply before its next request.
        self.flush().map_err(PgWireError::from)?;
        match self {
            Self::Plain(stream, _, _) => Ok(stream),
            Self::Tls(_, _, _) => Err(PgWireError::Server(
                "TLS stream cannot enter async plain socket loop".to_string(),
            )),
            Self::Upgrading => Err(PgWireError::Server(
                "client stream is upgrading".to_string(),
            )),
        }
    }

    fn upgrade_tls(&mut self, config: Arc<ServerConfig>) -> Result<()> {
        self.flush().map_err(PgWireError::from)?;
        let current = std::mem::replace(self, Self::Upgrading);
        let tcp = match current {
            Self::Plain(stream, _, deadline) => (stream, deadline),
            other @ Self::Tls(..) => {
                *self = other;
                return Err(PgWireError::Protocol(
                    "connection is already using TLS".to_string(),
                ));
            }
            Self::Upgrading => {
                return Err(PgWireError::Protocol(
                    "connection is already upgrading TLS".to_string(),
                ));
            }
        };
        let connection = ServerConnection::new(config)
            .map_err(|error| PgWireError::Server(error.to_string()))?;
        *self = Self::Tls(
            Box::new(StreamOwned::new(connection, tcp.0)),
            Vec::new(),
            tcp.1,
        );
        Ok(())
    }
}

impl ClientStream {
    /// True once a startup/authentication read deadline has been set and has
    /// passed. Lets patient readers tell that terminal condition apart from
    /// the ordinary 250 ms poll timeout, which they retry.
    pub(crate) fn read_deadline_expired(&self) -> bool {
        match self {
            Self::Plain(_, _, Some(deadline)) | Self::Tls(_, _, Some(deadline)) => {
                deadline.saturating_duration_since(Instant::now()).is_zero()
            }
            _ => false,
        }
    }
}

impl Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Send any buffered reply before blocking on the next request so a
        // request/response peer never deadlocks on bytes we are still holding.
        self.flush()?;
        let deadline = match self {
            Self::Plain(_, _, deadline) | Self::Tls(_, _, deadline) => *deadline,
            Self::Upgrading => None,
        };
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "startup/authentication deadline exceeded",
                ));
            }
            self.tcp().set_read_timeout(Some(remaining))?;
        }
        match self {
            Self::Plain(stream, _, _) => stream.read(buf),
            Self::Tls(stream, _, _) => stream.read(buf),
            Self::Upgrading => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "client stream is upgrading",
            )),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let over = match self {
            Self::Plain(_, wbuf, _) | Self::Tls(_, wbuf, _) => {
                wbuf.extend_from_slice(buf);
                wbuf.len() >= CLIENT_WRITE_FLUSH_THRESHOLD
            }
            Self::Upgrading => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "client stream is upgrading",
                ));
            }
        };
        if over {
            self.flush()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream, wbuf, _) => {
                if !wbuf.is_empty() {
                    stream.write_all(wbuf)?;
                    wbuf.clear();
                }
                stream.flush()
            }
            Self::Tls(stream, wbuf, _) => {
                if !wbuf.is_empty() {
                    stream.write_all(wbuf)?;
                    wbuf.clear();
                }
                stream.flush()
            }
            Self::Upgrading => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "client stream is upgrading",
            )),
        }
    }
}

pub fn handle_client_with_server(
    stream: TcpStream,
    peer_addr: SocketAddr,
    server: Arc<PgWireServer>,
) -> Result<()> {
    let admission = server.try_admit_connection(peer_addr.ip()).ok_or_else(|| {
        PgWireError::Server(format!(
            "max connections reached: {}",
            server.config.max_connections
        ))
    })?;
    handle_client_with_server_admitted(stream, peer_addr, server, admission)
}

fn handle_client_with_server_admitted(
    stream: TcpStream,
    peer_addr: SocketAddr,
    server: Arc<PgWireServer>,
    admission: ConnectionAdmission,
) -> Result<()> {
    match initialize_client_with_server_admitted(stream, peer_addr, server.clone(), admission)? {
        InitializedClient::Plain(initialized) => {
            let initialized = *initialized;
            let data_server = initialized.server;
            let _control_server = initialized.control_server;
            let mut stream = ClientStream::plain(initialized.stream);
            let mut state = initialized.state;
            let result = handle_client_loop(&mut stream, &data_server, &mut state);
            cleanup_portals(&data_server, &mut state);
            // Unregistration is the RAII guard's job (it also covers the
            // panic and early-return paths this call never did).
            drop(initialized.admission);
            result
        }
        InitializedClient::Blocking(initialized) => {
            let initialized = *initialized;
            let data_server = initialized.server;
            let _control_server = initialized.control_server;
            let mut stream = initialized.stream;
            let mut state = initialized.state;
            let result = handle_client_loop(&mut stream, &data_server, &mut state);
            cleanup_portals(&data_server, &mut state);
            // Unregistration is the RAII guard's job (it also covers the
            // panic and early-return paths this call never did).
            drop(initialized.admission);
            result
        }
        InitializedClient::Done => Ok(()),
    }
}

/// Disable Nagle's algorithm on a freshly accepted client socket.
///
/// The pgwire protocol is a latency-sensitive request/response loop dominated by
/// small messages (a `CALL`/`Query` and its reply are often a few hundred bytes).
/// With Nagle's algorithm enabled the kernel withholds a small outgoing segment
/// until the previous one is acknowledged, and on a real TCP path this collides
/// with the peer's delayed-ACK timer to add up to ~40ms of latency per round
/// trip — which serializes the whole driver to ~25 transactions/second per
/// connection regardless of how fast statements actually execute. PostgreSQL sets
/// `TCP_NODELAY` on every backend socket for exactly this reason; match it.
///
/// Failure is non-fatal: a non-TCP transport (or a kernel that rejects the
/// option) should still serve the connection, just without the latency win.
fn configure_accepted_socket(stream: &TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        log_operational_event(
            "connection.set_nodelay_failed",
            "warn",
            json!({ "error": error.to_string() }),
        );
    }
}

fn initialize_client_with_server_admitted(
    stream: TcpStream,
    peer_addr: SocketAddr,
    server: Arc<PgWireServer>,
    admission: ConnectionAdmission,
) -> Result<InitializedClient> {
    configure_accepted_socket(&stream);
    let mut stream = ClientStream::plain(stream);
    stream.set_nonblocking(false)?;
    let authentication_deadline = Instant::now() + server.config.authentication_timeout;
    stream.set_read_deadline(Some(authentication_deadline));
    stream.set_read_timeout(Some(server.config.authentication_timeout))?;
    stream.set_write_timeout(Some(server.config.idle_timeout))?;
    let connection_id = server.next_connection_id.fetch_add(1, Ordering::SeqCst);
    let process_id = i32::try_from(connection_id)
        .map_err(|_| PgWireError::Server("PostgreSQL backend id space exhausted".to_string()))?;
    let secret_key = cancel_secret_key(connection_id);
    let cancel_token = server.register_cancel_token(process_id, secret_key);
    let startup = match startup(&mut stream, &server, process_id, secret_key) {
        Ok(Some(startup)) => startup,
        Ok(None) => {
            // Startup declined (e.g. SSL/auth rejection): flush any buffered
            // ErrorResponse before the stream drops and the socket closes,
            // otherwise the client sees EOF instead of the error.
            let _ = stream.flush();
            server.unregister_cancel_token(process_id, secret_key);
            return Ok(InitializedClient::Done);
        }
        Err(error) => {
            let _ = stream.flush();
            server.unregister_cancel_token(process_id, secret_key);
            return Err(error);
        }
    };
    stream.set_read_deadline(None);
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;

    let data_server = match server.cluster.as_ref().and_then(Weak::upgrade) {
        Some(cluster) => cluster.server_for_database(&startup.database)?,
        None => server.clone(),
    };

    let security_context = startup.security_context;
    let mut state = ConnectionState::new_with_security_context(
        connection_id,
        startup.user,
        Some(peer_addr.to_string()),
        security_context,
    );
    state
        .session_state
        .insert_session_guc("database".to_string(), startup.database.clone());
    // Merge startup-parameter GUCs without letting them override the
    // authenticated identity seeded by ConnectionState::new.
    for (key, value) in startup.session_gucs {
        if !bicdb_sql::is_protected_security_setting(&key)
            && !matches!(
                key.as_str(),
                "session_authorization"
                    | "role"
                    | "bicdb.initial_session_authorization"
                    | "bicdb.rls_check_as"
                    | "bicdb_version"
                    | "server_version"
                    | "server_version_num"
            )
        {
            state.session_state.insert_session_guc(key, value);
        }
    }
    state.session_state.insert_session_guc(
        "server_version".to_string(),
        data_server.config.postgres_server_version.clone(),
    );
    state.session_state.insert_session_guc(
        "server_version_num".to_string(),
        data_server.config.postgres_server_version_num.clone(),
    );
    if data_server.config.postgres_version_banner {
        state.session_state.insert_session_guc(
            "bicdb.postgres_version_banner".to_string(),
            "on".to_string(),
        );
    }
    state.cancel_token = cancel_token;
    data_server
        .connections
        .lock()
        .unwrap()
        .insert(connection_id, state.snapshot());
    log_operational_event(
        "connection.opened",
        "info",
        json!({
            "connection_id": connection_id,
            "peer_addr": peer_addr.to_string(),
            "user": state.user,
            "database": startup.database,
            "tls": stream.is_tls(),
        }),
    );

    if stream.is_tls() {
        let registration = ConnectionRegistration {
            server: Arc::clone(&data_server),
            control_server: Arc::clone(&server),
            connection_id: state.connection_id,
            process_id,
            secret_key,
        };
        Ok(InitializedClient::Blocking(Box::new(
            InitializedBlockingClient {
                _registration: registration,
                stream,
                server: data_server,
                control_server: server,
                state,
                process_id,
                secret_key,
                admission,
            },
        )))
    } else {
        // Constructed before `into_plain()`, whose flush can fail if the peer
        // vanished during startup: the guard must already own the cleanup by
        // the time that `?` can return.
        let registration = ConnectionRegistration {
            server: Arc::clone(&data_server),
            control_server: Arc::clone(&server),
            connection_id: state.connection_id,
            process_id,
            secret_key,
        };
        let plain = stream.into_plain()?;
        Ok(InitializedClient::Plain(Box::new(InitializedPlainClient {
            _registration: registration,
            stream: plain,
            server: data_server,
            control_server: server,
            state,
            process_id,
            secret_key,
            admission,
        })))
    }
}

fn cleanup_client(
    server: &PgWireServer,
    control_server: &PgWireServer,
    connection_id: u64,
    process_id: i32,
    secret_key: i32,
) {
    server.advisory_locks.disconnect(connection_id);
    if let Ok(mut bus) = server.notifications.lock() {
        bus.disconnect(connection_id);
    }
    // Poison-tolerant: a panic elsewhere while holding this lock must not
    // stop every other connection from unregistering.
    match server.connections.lock() {
        Ok(mut connections) => {
            connections.remove(&connection_id);
        }
        Err(poisoned) => {
            poisoned.into_inner().remove(&connection_id);
        }
    }
    control_server.unregister_cancel_token(process_id, secret_key);
    log_operational_event(
        "connection.closed",
        "info",
        json!({ "connection_id": connection_id }),
    );
}

async fn reject_connection_at_capacity_async(
    mut stream: TokioTcpStream,
    max_drain_bytes: usize,
    _drain_permit: Option<tokio::sync::OwnedSemaphorePermit>,
) {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"FATAL\0");
    payload.push(b'C');
    payload.extend_from_slice(b"53300\0");
    payload.push(b'M');
    payload.extend_from_slice(b"too many connections\0");
    payload.push(0);
    let len = (payload.len() + 4) as i32;
    let mut frame = Vec::with_capacity(1 + 4 + payload.len());
    frame.push(b'E');
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&payload);
    if stream.write_all(&frame).await.is_err() || stream.flush().await.is_err() {
        return;
    }

    // A close with unread peer bytes is an abortive close on Linux: the
    // kernel sends RST and may discard the FATAL frame which was just queued.
    // Half-close the write side first, then consume a bounded startup packet
    // while the error travels to the client. Refused peers never receive an
    // admission slot, and both drain concurrency and bytes are bounded.
    let _ = stream.shutdown().await;
    if max_drain_bytes == 0 {
        return;
    }
    let drain = async {
        let mut remaining = max_drain_bytes;
        let mut buffer = [0_u8; 8 * 1024];
        while remaining > 0 {
            let read_bound = remaining.min(buffer.len());
            match stream.read(&mut buffer[..read_bound]).await {
                Ok(0) | Err(_) => break,
                Ok(read) => remaining = remaining.saturating_sub(read),
            }
        }
    };
    let _ = time::timeout(Duration::from_millis(250), drain).await;
}

async fn handle_client_async_with_admission(
    stream: TokioTcpStream,
    peer_addr: SocketAddr,
    server: Arc<PgWireServer>,
    admission: ConnectionAdmission,
) -> Result<()> {
    let stream = stream.into_std()?;
    let server_for_startup = server.clone();
    let initialized = tokio::task::spawn_blocking(move || {
        initialize_client_with_server_admitted(stream, peer_addr, server_for_startup, admission)
    })
    .await
    .map_err(|error| PgWireError::Server(format!("pgwire startup task failed: {error}")))??;

    match initialized {
        InitializedClient::Plain(initialized) => tokio::task::spawn_blocking(move || {
            let initialized = *initialized;
            let data_server = initialized.server;
            let _control_server = initialized.control_server;
            let mut stream = ClientStream::plain(initialized.stream);
            let mut state = initialized.state;
            let result = handle_client_loop(&mut stream, &data_server, &mut state);
            cleanup_portals(&data_server, &mut state);
            // Unregistration is the RAII guard's job (it also covers the
            // panic and early-return paths this call never did).
            drop(initialized.admission);
            result
        })
        .await
        .map_err(|error| PgWireError::Server(format!("pgwire plain task failed: {error}")))?,
        InitializedClient::Blocking(initialized) => tokio::task::spawn_blocking(move || {
            let initialized = *initialized;
            let data_server = initialized.server;
            let _control_server = initialized.control_server;
            let mut stream = initialized.stream;
            let mut state = initialized.state;
            let result = handle_client_loop(&mut stream, &data_server, &mut state);
            cleanup_portals(&data_server, &mut state);
            // Unregistration is the RAII guard's job (it also covers the
            // panic and early-return paths this call never did).
            drop(initialized.admission);
            result
        })
        .await
        .map_err(|error| PgWireError::Server(format!("pgwire TLS task failed: {error}")))?,
        InitializedClient::Done => Ok(()),
    }
}

async fn handle_plain_client_async(
    initialized: InitializedPlainClient,
    _server: Arc<PgWireServer>,
) -> Result<()> {
    let InitializedPlainClient {
        // Held for the whole connection: unregisters on every exit path,
        // including an error return or a panic from the async loop below.
        _registration: registration,
        stream,
        server: initialized_server,
        control_server,
        state,
        process_id,
        secret_key,
        admission,
    } = initialized;
    let _registration = registration;
    let server = initialized_server;
    let connection_id = state.connection_id;
    let mut state = Some(state);
    stream.set_nonblocking(true)?;
    let mut stream = TokioTcpStream::from_std(stream)?;
    let mut last_activity = Instant::now();
    let mut result = Ok(());
    let mut read_buffer = Vec::new();

    loop {
        if server.is_shutdown_requested() {
            break;
        }
        // Push-style LISTEN/NOTIFY delivery: pending notifications are
        // written while the connection is idle (the read below polls at
        // 250ms), so a listening client wakes without issuing a query.
        if let Err(error) = flush_notifications_async(&mut stream, &server, connection_id).await {
            result = Err(error);
            break;
        }
        let frame = read_frontend_message_async(
            &mut stream,
            &mut read_buffer,
            server.config.max_request_bytes,
            Duration::from_millis(250),
        )
        .await;
        let first_message = match frame {
            Ok(AsyncFrontendMessageRead::Message(tag, payload)) => {
                last_activity = Instant::now();
                (tag, payload)
            }
            Ok(AsyncFrontendMessageRead::Eof) => break,
            Ok(AsyncFrontendMessageRead::Timeout) => {
                if last_activity.elapsed() >= server.config.idle_timeout {
                    break;
                }
                continue;
            }
            Err(error) => {
                result = Err(error);
                break;
            }
        };
        let mut messages = vec![first_message];
        match drain_available_frontend_messages(
            &mut stream,
            &mut read_buffer,
            server.config.max_request_bytes,
            32,
        ) {
            Ok(mut drained) => {
                if !drained.is_empty() {
                    last_activity = Instant::now();
                    messages.append(&mut drained);
                }
            }
            Err(error) => {
                result = Err(error);
                break;
            }
        }

        let std_stream = stream.into_std()?;
        let server_for_message = server.clone();
        let mut message_state = state
            .take()
            .ok_or_else(|| PgWireError::Server("connection state missing".to_string()))?;
        let message_result = tokio::task::spawn_blocking(move || {
            std_stream.set_nonblocking(false)?;
            let mut client_stream = ClientStream::plain(std_stream);
            let mut keep_open = true;
            for (tag, payload) in messages {
                message_state.observe_request(payload.len());
                enforce_connection_memory(&message_state, &server_for_message.config)?;
                server_for_message
                    .connections
                    .lock()
                    .unwrap()
                    .insert(message_state.connection_id, message_state.snapshot());
                if message_state.discard_until_sync && tag != b'S' {
                    message_state.finish_request();
                    continue;
                }
                keep_open = handle_frontend_message(
                    &mut client_stream,
                    &server_for_message,
                    &mut message_state,
                    tag,
                    payload,
                )?;
                message_state.finish_request();
                if !keep_open {
                    break;
                }
            }
            let std_stream = client_stream.into_plain()?;
            std_stream.set_nonblocking(true)?;
            Ok::<_, PgWireError>((std_stream, message_state, keep_open))
        })
        .await
        .map_err(|error| PgWireError::Server(format!("pgwire message task failed: {error}")))?;

        match message_result {
            Ok((std_stream, next_state, keep_open)) => {
                state = Some(next_state);
                if !keep_open {
                    break;
                }
                stream = TokioTcpStream::from_std(std_stream)?;
            }
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }

    if let Some(state) = state.as_mut() {
        cleanup_portals(&server, state);
    }
    // Unregistration happens when `_registration` drops.
    drop(admission);
    result
}

async fn flush_notifications_async(
    stream: &mut TokioTcpStream,
    server: &PgWireServer,
    connection_id: u64,
) -> Result<()> {
    let pending = match server.notifications.lock() {
        Ok(mut bus) => bus.drain(connection_id),
        Err(_) => return Ok(()),
    };
    if pending.is_empty() {
        return Ok(());
    }
    let mut buffer = Vec::new();
    for (channel, payload, sender_pid) in pending {
        let mut body = Vec::with_capacity(8 + channel.len() + payload.len() + 2);
        body.extend_from_slice(&sender_pid.to_be_bytes());
        body.extend_from_slice(channel.as_bytes());
        body.push(0);
        body.extend_from_slice(payload.as_bytes());
        body.push(0);
        buffer.push(b'A');
        buffer.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        buffer.extend_from_slice(&body);
    }
    stream.write_all(&buffer).await?;
    Ok(())
}

enum AsyncFrontendMessageRead {
    Message(u8, Vec<u8>),
    Eof,
    Timeout,
}

fn parse_frontend_message_buffer(
    buffer: &mut Vec<u8>,
    max_request_bytes: usize,
) -> Result<Option<(u8, Vec<u8>)>> {
    if buffer.len() < 5 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
    if len < 4 {
        return Err(PgWireError::Protocol(format!(
            "invalid message length {len}"
        )));
    }
    let payload_len = (len - 4) as usize;
    if payload_len > max_request_bytes {
        return Err(PgWireError::Protocol(format!(
            "frontend message length {payload_len} exceeds max_request_bytes {max_request_bytes}"
        )));
    }
    let frame_len = 1 + len as usize;
    if buffer.len() < frame_len {
        return Ok(None);
    }
    let tag = buffer[0];
    let payload = buffer[5..frame_len].to_vec();
    buffer.drain(..frame_len);
    Ok(Some((tag, payload)))
}

async fn read_frontend_message_async(
    stream: &mut TokioTcpStream,
    buffer: &mut Vec<u8>,
    max_request_bytes: usize,
    read_timeout: Duration,
) -> Result<AsyncFrontendMessageRead> {
    if let Some((tag, payload)) = parse_frontend_message_buffer(buffer, max_request_bytes)? {
        return Ok(AsyncFrontendMessageRead::Message(tag, payload));
    }
    let mut chunk = [0_u8; 8192];
    let mut partial_since: Option<Instant> = None;
    loop {
        match time::timeout(read_timeout, stream.read(&mut chunk)).await {
            Ok(Ok(0)) if buffer.is_empty() => return Ok(AsyncFrontendMessageRead::Eof),
            Ok(Ok(0)) => {
                return Err(PgWireError::Protocol(
                    "connection closed mid-message".to_string(),
                ));
            }
            Ok(Ok(n)) => {
                buffer.extend_from_slice(&chunk[..n]);
                if let Some((tag, payload)) =
                    parse_frontend_message_buffer(buffer, max_request_bytes)?
                {
                    return Ok(AsyncFrontendMessageRead::Message(tag, payload));
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) if buffer.is_empty() => return Ok(AsyncFrontendMessageRead::Timeout),
            Err(_) => {
                // A message in flight: `read_timeout` is only the idle poll
                // interval, not a protocol deadline. A large statement from a
                // slow or CPU-starved client may legitimately pause for
                // longer than that between segments; keep waiting up to
                // PARTIAL_MESSAGE_BUDGET before calling it a dead peer.
                let since = *partial_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= PARTIAL_MESSAGE_BUDGET {
                    return Err(PgWireError::Protocol(
                        "timed out reading partial frontend message".to_string(),
                    ));
                }
            }
        }
    }
}

fn drain_available_frontend_messages(
    stream: &mut TokioTcpStream,
    buffer: &mut Vec<u8>,
    max_request_bytes: usize,
    limit: usize,
) -> Result<Vec<(u8, Vec<u8>)>> {
    let mut messages = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        while messages.len() < limit {
            let Some(message) = parse_frontend_message_buffer(buffer, max_request_bytes)? else {
                break;
            };
            let stop_at_sync = message.0 == b'S';
            messages.push(message);
            if stop_at_sync {
                return Ok(messages);
            }
        }
        if messages.len() >= limit {
            return Ok(messages);
        }
        match stream.try_read(&mut chunk) {
            Ok(0) => return Ok(messages),
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(messages),
            Err(error) => return Err(error.into()),
        }
    }
}

fn cancel_secret_key(connection_id: u64) -> i32 {
    let mut hash = Sha256::new();
    hash.update(connection_id.to_be_bytes());
    hash.update(unix_timestamp().to_be_bytes());
    hash.update(Uuid::new_v4().as_bytes());
    i32::from_be_bytes(hash.finalize()[0..4].try_into().unwrap())
}

fn handle_client_loop(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<()> {
    let mut last_activity = Instant::now();
    loop {
        if server.is_shutdown_requested() {
            let _ = stream.flush();
            cleanup_portals(server, state);
            return Ok(());
        }
        // Push-style LISTEN/NOTIFY delivery: pending notifications are
        // written while the connection is idle (the read below polls on a
        // timeout), so a listening client wakes without issuing a query.
        flush_notifications(stream, server, state.connection_id)?;
        let (tag, payload) = match read_frontend_message(stream, server.config.max_request_bytes)? {
            FrontendMessageRead::Message(tag, payload) => {
                last_activity = Instant::now();
                (tag, payload)
            }
            FrontendMessageRead::Eof => {
                cleanup_portals(server, state);
                return Ok(());
            }
            FrontendMessageRead::Timeout => {
                if last_activity.elapsed() >= server.config.idle_timeout {
                    cleanup_portals(server, state);
                    return Ok(());
                }
                continue;
            }
        };
        state.observe_request(payload.len());
        enforce_connection_memory(state, &server.config)?;
        server
            .connections
            .lock()
            .unwrap()
            .insert(state.connection_id, state.snapshot());

        if state.discard_until_sync && tag != b'S' {
            continue;
        }

        let handled = handle_frontend_message(stream, server, state, tag, payload);
        state.finish_request();
        if !handled? {
            // Closing without another read: flush any buffered final reply.
            let _ = stream.flush();
            cleanup_portals(server, state);
            return Ok(());
        }
    }
}

fn cleanup_portals(server: &Arc<PgWireServer>, state: &mut ConnectionState) {
    for (_, portal) in state.portals.drain() {
        if portal.stream.is_some() {
            server.unregister_cursor_memory(portal.registered_stream_memory);
        }
    }
}

fn handle_frontend_message(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    tag: u8,
    payload: Vec<u8>,
) -> Result<bool> {
    if state.copy_in.is_some() {
        match tag {
            b'd' => {
                if let Err(error) = copy_in_data(state, &payload) {
                    abort_copy_in(state);
                    state.failed_queries += 1;
                    server.failed_queries.fetch_add(1, Ordering::SeqCst);
                    state.record_query_error();
                    error_response_for_error(stream, &error)?;
                    if error.closes_connection() {
                        return Ok(false);
                    }
                    ready_for_query_status(stream, state.transaction_status())?;
                }
            }
            b'c' => match finish_copy_in(server, state) {
                Ok(count) => {
                    command_complete(stream, &format!("COPY {count}"))?;
                    ready_for_query_status(stream, state.transaction_status())?;
                }
                Err(error) => {
                    state.failed_queries += 1;
                    server.failed_queries.fetch_add(1, Ordering::SeqCst);
                    state.record_query_error();
                    error_response_for_error(stream, &error)?;
                    if error.closes_connection() {
                        return Ok(false);
                    }
                    ready_for_query_status(stream, state.transaction_status())?;
                }
            },
            b'f' => {
                abort_copy_in(state);
                let message = cstring_payload(&payload).unwrap_or("COPY failed");
                state.failed_queries += 1;
                server.failed_queries.fetch_add(1, Ordering::SeqCst);
                state.record_query_error();
                error_response(stream, message)?;
                ready_for_query_status(stream, state.transaction_status())?;
            }
            _ => {
                abort_copy_in(state);
                state.discard_until_sync = true;
                error_response(
                    stream,
                    "expected CopyData, CopyDone, or CopyFail during COPY FROM STDIN",
                )?;
            }
        }
        return Ok(true);
    }

    match tag {
        b'Q' => {
            let query = cstring_payload(&payload)?;
            if sql_is_blank(query) {
                flush_notifications(stream, server, state.connection_id)?;
                write_message(stream, b'I', &[])?;
                ready_for_query_status(stream, state.transaction_status())?;
                return Ok(true);
            }
            if let Some(tag) = try_listen_notify_statement(server, state, query)? {
                flush_notifications(stream, server, state.connection_id)?;
                command_complete(stream, tag)?;
                ready_for_query_status(stream, state.transaction_status())?;
                return Ok(true);
            }

            match execute_server_query(stream, server, state, query) {
                Ok(QueryFlow::Complete) => {
                    flush_notifications(stream, server, state.connection_id)?;
                    ready_for_query_status(stream, state.transaction_status())?;
                }
                Ok(QueryFlow::CopyInStarted) => {}
                Err(error) => {
                    state.failed_queries += 1;
                    server.failed_queries.fetch_add(1, Ordering::SeqCst);
                    state.record_query_error();
                    log_failed_query(server, state, Some(query), &error);
                    error_response_for_error(stream, &error)?;
                    if error.closes_connection() {
                        return Ok(false);
                    }
                    ready_for_query_status(stream, state.transaction_status())?;
                }
            }
        }
        b'P' => match parse_message(&payload, server) {
            Ok(parsed) => {
                state.prepared.insert(parsed.name, parsed.statement);
                parse_complete(stream)?;
            }
            Err(error) => {
                state.discard_until_sync = true;
                error_response_for_error(stream, &error)?;
                if error.closes_connection() {
                    return Ok(false);
                }
            }
        },
        b'B' => match bind_message(&payload, &state.prepared, server) {
            Ok((portal_name, portal)) => {
                state.portals.insert(portal_name, portal);
                bind_complete(stream)?;
            }
            Err(error) => {
                state.discard_until_sync = true;
                error_response_for_error(stream, &error)?;
                if error.closes_connection() {
                    return Ok(false);
                }
            }
        },
        b'D' => {
            if let Err(error) = describe_message(stream, &payload, server, state) {
                state.discard_until_sync = true;
                error_response_for_error(stream, &error)?;
                if error.closes_connection() {
                    return Ok(false);
                }
            }
        }
        b'E' => {
            let source_sql = portal_source_sql_from_execute_payload(&payload, state);
            match execute_message(&payload, server, state) {
                Ok(executed) => {
                    let stats = write_query_result(
                        stream,
                        server,
                        &executed.result,
                        &executed.result_formats,
                        Some(&executed.source_sql),
                        false,
                        !executed.suspended,
                    )?;
                    server.record_streamed_rows(stats.rows, stats.bytes);
                    if executed.suspended {
                        portal_suspended(stream)?;
                    }
                }
                Err(error) => {
                    state.failed_queries += 1;
                    server.failed_queries.fetch_add(1, Ordering::SeqCst);
                    state.record_query_error();
                    state.discard_until_sync = true;
                    log_failed_query(server, state, source_sql.as_deref(), &error);
                    error_response_for_error(stream, &error)?;
                    if error.closes_connection() {
                        return Ok(false);
                    }
                }
            }
        }
        b'S' => {
            state.discard_until_sync = false;
            ready_for_query_status(stream, state.transaction_status())?;
        }
        b'C' => match close_message_with_server(&payload, server, state) {
            Ok(()) => close_complete(stream)?,
            Err(error) => {
                state.discard_until_sync = true;
                error_response_for_error(stream, &error)?;
                if error.closes_connection() {
                    return Ok(false);
                }
            }
        },
        b'H' => stream.flush()?,
        b'X' => return Ok(false),
        _ => {
            state.discard_until_sync = true;
            error_response(
                stream,
                "BicDB pgwire does not support this frontend message yet",
            )?;
        }
    }
    Ok(true)
}

#[derive(Clone, Debug)]
struct PreparedStatement {
    sql: String,
    param_type_oids: Vec<i32>,
}

#[derive(Clone, Debug)]
struct Portal {
    sql: String,
    source_sql: String,
    result_formats: Vec<i16>,
    stream: Option<SqlRowStream>,
    registered_stream_memory: usize,
    /// Result computed eagerly during Describe(Portal) for a statement whose
    /// result shape cannot be known without running it. Execute replays this
    /// result instead of running the statement twice.
    predescribed: Option<SqlResult>,
}

#[derive(Debug)]
struct CopyInState {
    spec: CopyStatement,
    rows: Vec<Vec<Option<String>>>,
    pending: String,
    header_seen: bool,
    resolved_columns: Option<Vec<String>>,
    inserted_rows: usize,
    spill: Option<CopySpillState>,
    started_guc_transaction: bool,
}

impl CopyInState {
    fn memory_estimate(&self) -> usize {
        let spec = self
            .spec
            .relation
            .as_ref()
            .map(String::len)
            .unwrap_or_default()
            + self.spec.columns.iter().map(String::len).sum::<usize>()
            + self
                .spec
                .query
                .as_ref()
                .map(String::len)
                .unwrap_or_default();
        let resolved_columns = self
            .resolved_columns
            .as_ref()
            .map(|columns| columns.iter().map(String::len).sum::<usize>())
            .unwrap_or_default();
        let rows = copy_rows_memory_estimate(&self.rows);
        let spill = self
            .spill
            .as_ref()
            .map(CopySpillState::memory_estimate)
            .unwrap_or_default();
        spec + resolved_columns + self.pending.len() + rows + spill
    }
}

fn copy_rows_memory_estimate(rows: &[Vec<Option<String>>]) -> usize {
    rows.iter()
        .flat_map(|row| row.iter())
        .map(|cell| cell.as_ref().map(String::len).unwrap_or_default())
        .sum::<usize>()
}

#[derive(Debug)]
struct CopySpillState {
    path: PathBuf,
    writer: BufWriter<File>,
    rows: usize,
}

impl CopySpillState {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "bicdb-copy-{}-{}.jsonl",
            std::process::id(),
            Uuid::new_v4()
        ));
        let file = File::create(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            rows: 0,
        })
    }

    fn memory_estimate(&self) -> usize {
        self.path.to_string_lossy().len()
            + self.writer.buffer().len()
            + self.rows.saturating_mul(std::mem::size_of::<usize>())
    }
}

impl Drop for CopySpillState {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Clone, Debug)]
struct CopyStatement {
    relation: Option<String>,
    columns: Vec<String>,
    direction: CopyDirection,
    format: CopyFormat,
    query: Option<String>,
    header: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CopyDirection {
    FromStdin,
    ToStdout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CopyFormat {
    Text,
    Csv,
}

#[derive(Debug, Default)]
struct ConnectionState {
    delegation_challenge: Option<String>,
    delegated_identity: Option<delegation::DelegatedIdentity>,
    connection_id: u64,
    user: String,
    security_context: Option<SecurityContext>,
    peer_addr: Option<String>,
    connected_at: i64,
    last_query_at: Option<i64>,
    active_query: Option<String>,
    queries_executed: u64,
    failed_queries: u64,
    in_transaction: bool,
    failed_transaction: bool,
    discard_until_sync: bool,
    tx_statements: Vec<TxBufferedOperation>,
    tx: Option<Transaction>,
    tx_ddl_undo: SqlSessionDdlUndoLog,
    tx_collection_generations: HashMap<String, u64>,
    tx_write_tables: FxHashSet<String>,
    tx_shared_role_ddl: Vec<String>,
    committed_shared_role_ddl: Vec<String>,
    savepoints: Vec<SavepointMark>,
    active_request_bytes: usize,
    prepared: FxHashMap<String, PreparedStatement>,
    portals: FxHashMap<String, Portal>,
    session_state: SqlSessionGucState,
    catalog_cache: SqlSessionCatalogCache,
    copy_in: Option<CopyInState>,
    cancel_token: Arc<AtomicBool>,
    // Highest commit_seq this connection has committed. Used as the snapshot
    // floor for subsequent transactions so the connection reads — and does not
    // spuriously self-conflict against — its own writes while the contiguous
    // visibility watermark lags concurrent commits.
    last_commit_seq: u64,
}

#[derive(Clone, Debug)]
struct SavepointMark {
    name: String,
    statement_len: usize,
    write_len: usize,
    lock_len: usize,
    ddl_undo_len: usize,
    shared_role_ddl_len: usize,
    session_state: SqlSessionGucState,
}

impl SavepointMark {
    fn memory_estimate(&self) -> usize {
        self.name
            .capacity()
            .saturating_add(self.session_state.memory_estimate())
    }
}

#[derive(Clone, Debug)]
enum TxBufferedOperation {
    Sql(String),
    CopyRows {
        relation: String,
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
}

impl TxBufferedOperation {
    fn memory_estimate(&self) -> usize {
        match self {
            TxBufferedOperation::Sql(statement) => statement.len(),
            TxBufferedOperation::CopyRows {
                relation,
                columns,
                rows,
            } => {
                relation.len()
                    + columns.iter().map(String::len).sum::<usize>()
                    + copy_rows_memory_estimate(rows)
            }
        }
    }

    fn write_table(&self) -> Option<String> {
        match self {
            TxBufferedOperation::Sql(statement) => {
                write_table_for_buffered_sql(&normalize_executable_sql(statement))
            }
            TxBufferedOperation::CopyRows { relation, .. } => Some(relation.clone()),
        }
    }
}

impl ConnectionState {
    fn new(connection_id: u64, user: String, peer_addr: Option<String>) -> Self {
        Self::new_with_security_context(connection_id, user, peer_addr, None)
    }

    fn new_with_security_context(
        connection_id: u64,
        user: String,
        peer_addr: Option<String>,
        security_context: Option<SecurityContext>,
    ) -> Self {
        // Seed the PostgreSQL identity GUCs from the authenticated startup
        // user so RLS, current_user, and SET SESSION AUTHORIZATION permission
        // checks see the connection's real identity.
        let mut session_gucs = HashMap::new();
        let normalized_user = user.trim().trim_matches('"').to_ascii_lowercase();
        if !normalized_user.is_empty() {
            session_gucs.insert("session_authorization".to_string(), normalized_user.clone());
            session_gucs.insert(
                "bicdb.initial_session_authorization".to_string(),
                normalized_user,
            );
        }
        Self {
            delegation_challenge: None,
            delegated_identity: None,
            connection_id,
            user,
            security_context,
            peer_addr,
            connected_at: unix_timestamp(),
            last_query_at: None,
            active_query: None,
            queries_executed: 0,
            failed_queries: 0,
            in_transaction: false,
            failed_transaction: false,
            discard_until_sync: false,
            tx_statements: Vec::new(),
            tx: None,
            tx_ddl_undo: SqlSessionDdlUndoLog::default(),
            tx_collection_generations: HashMap::new(),
            tx_write_tables: FxHashSet::default(),
            tx_shared_role_ddl: Vec::new(),
            committed_shared_role_ddl: Vec::new(),
            savepoints: Vec::new(),
            active_request_bytes: 0,
            prepared: FxHashMap::default(),
            portals: FxHashMap::default(),
            session_state: SqlSessionGucState::from_session_gucs(session_gucs),
            catalog_cache: SqlSessionCatalogCache::default(),
            copy_in: None,
            cancel_token: Arc::new(AtomicBool::new(false)),
            last_commit_seq: 0,
        }
    }

    fn snapshot(&self) -> ServerConnectionSnapshot {
        ServerConnectionSnapshot {
            connection_id: self.connection_id,
            user: self.user.clone(),
            peer_addr: self.peer_addr.clone(),
            connected_at: self.connected_at,
            last_query_at: self.last_query_at,
            active_query: self.active_query.clone(),
            queries_executed: self.queries_executed,
            failed_queries: self.failed_queries,
            in_transaction: self.in_transaction,
        }
    }

    fn transaction_status(&self) -> u8 {
        if self.failed_transaction {
            b'E'
        } else if self.in_transaction {
            b'T'
        } else {
            b'I'
        }
    }

    fn record_query_error(&mut self) {
        if self.in_transaction {
            self.failed_transaction = true;
        }
    }

    fn observe_request(&mut self, bytes: usize) {
        self.active_request_bytes = bytes;
    }

    fn finish_request(&mut self) {
        self.active_request_bytes = 0;
    }

    fn memory_estimate(&self) -> usize {
        let prepared = self
            .prepared
            .values()
            .map(|statement| statement.sql.len() + statement.param_type_oids.len() * 4)
            .sum::<usize>();
        let portals = self
            .portals
            .values()
            .map(|portal| {
                portal.sql.len()
                    + portal.source_sql.len()
                    + portal.result_formats.len() * 2
                    + portal.registered_stream_memory
            })
            .sum::<usize>();
        let buffered = self
            .tx_statements
            .iter()
            .map(TxBufferedOperation::memory_estimate)
            .sum::<usize>();
        let shared_role_ddl = self
            .tx_shared_role_ddl
            .iter()
            .chain(&self.committed_shared_role_ddl)
            .map(String::len)
            .sum::<usize>();
        let savepoints = self.savepoints.iter().fold(0_usize, |estimate, mark| {
            estimate.saturating_add(mark.memory_estimate())
        });
        let session_state = self.session_state.memory_estimate();
        let copy_in = self
            .copy_in
            .as_ref()
            .map(CopyInState::memory_estimate)
            .unwrap_or_default();
        prepared
            .saturating_add(portals)
            .saturating_add(buffered)
            .saturating_add(shared_role_ddl)
            .saturating_add(savepoints)
            .saturating_add(session_state)
            .saturating_add(copy_in)
            .saturating_add(self.active_request_bytes)
    }

    fn take_cancel_request(&self) -> bool {
        self.cancel_token.swap(false, Ordering::SeqCst)
    }
}

struct ParsedStatementMessage {
    name: String,
    statement: PreparedStatement,
}

enum QueryFlow {
    Complete,
    CopyInStarted,
}

fn execute_server_query(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
) -> Result<QueryFlow> {
    // Keep explicit transaction boundaries in the connection state, including
    // when a client sends BEGIN and subsequent statements in one Simple Query.
    // Running that batch inside a temporary SQL session committed its pending
    // transaction and discarded SET LOCAL before the next protocol message.
    if sql.contains(';') {
        let statements = split_sql_statements(sql);
        if statements.len() > 1 {
            let first = normalize_executable_sql(&statements[0]);
            let begins = is_transaction_control_candidate(&first)
                && parse_transaction_control(&statements[0])? == Some(TransactionControl::Begin);
            if state.in_transaction || begins {
                for statement in &statements[..statements.len() - 1] {
                    if matches!(
                        parse_copy_statement(statement)?,
                        Some(CopyStatement {
                            direction: CopyDirection::FromStdin,
                            ..
                        })
                    ) {
                        return Err(PgWireError::Protocol(
                            "COPY FROM STDIN must be the last statement in a query message".into(),
                        ));
                    }
                }
                let mut flow = QueryFlow::Complete;
                for statement in statements {
                    flow = execute_server_query(stream, server, state, &statement)?;
                }
                return Ok(flow);
            }
        }
    }
    if let Some(copy) = parse_copy_statement(sql)? {
        delegation::validate_delegation(server, state)?;
        match copy.direction {
            CopyDirection::FromStdin => {
                if state.failed_transaction {
                    return Err(PgWireError::InFailedTransaction);
                }
                let started_guc_transaction = !state.session_state.transaction_active();
                if started_guc_transaction {
                    state.session_state.begin_transaction();
                }
                state.copy_in = Some(CopyInState {
                    spec: copy,
                    rows: Vec::new(),
                    pending: String::new(),
                    header_seen: false,
                    resolved_columns: None,
                    inserted_rows: 0,
                    spill: None,
                    started_guc_transaction,
                });
                if let Err(error) = enforce_persistent_connection_memory(state, &server.config) {
                    abort_copy_in(state);
                    return Err(error);
                }
                let copy = &state
                    .copy_in
                    .as_ref()
                    .ok_or_else(|| {
                        PgWireError::Server("COPY FROM STDIN state is missing".to_string())
                    })?
                    .spec;
                copy_in_response(stream, server, copy)?;
                state.last_query_at = Some(unix_timestamp());
                Ok(QueryFlow::CopyInStarted)
            }
            CopyDirection::ToStdout => {
                if state.failed_transaction {
                    return Err(PgWireError::InFailedTransaction);
                }
                let result = execute_copy_query(server, state, &copy)?;
                copy_out_response(stream, &result)?;
                let db = server.read_db()?;
                let column_types = result_column_types_with_db(&db, &result, None)?;
                let deadline = (!server.config.query_timeout.is_zero())
                    .then(|| Instant::now() + server.config.query_timeout);
                let cancellation = CancellationToken::new(state.cancel_token.clone(), deadline);
                let mut bytes_streamed = 0_u64;
                for (idx, row) in result.rows.iter().enumerate() {
                    if idx % 1024 == 0 {
                        if let Err(error) = cancellation.check() {
                            return Err(classify_bicdb_interrupt(server, state, error));
                        }
                    }
                    let payload = render_copy_row(&db, row, &column_types, copy.format)?;
                    bytes_streamed = bytes_streamed.saturating_add(payload.len() as u64);
                    copy_data(stream, &payload)?;
                }
                server.record_streamed_rows(result.rows.len() as u64, bytes_streamed);
                command_complete(stream, &format!("COPY {}", result.rows.len()))?;
                Ok(QueryFlow::Complete)
            }
        }
    } else {
        // Normalized once per query; every classification below reads it.
        let normalized = normalize_executable_sql(sql);
        if let Some(result) = execute_sql_prepared_statement_query(server, state, sql, &normalized)?
        {
            write_query_result(
                stream,
                server,
                &result.result,
                &[],
                result.source_sql.as_deref(),
                true,
                true,
            )?;
        } else {
            let result =
                execute_server_sql_with_options_normalized(server, state, sql, &normalized, true)?;
            let source_sql = normalized.starts_with("select ").then_some(sql);
            write_query_result(stream, server, &result, &[], source_sql, true, true)?;
        }
        Ok(QueryFlow::Complete)
    }
}

struct SqlPreparedStatementResult {
    result: SqlResult,
    source_sql: Option<String>,
}

fn execute_sql_prepared_statement_query(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
) -> Result<Option<SqlPreparedStatementResult>> {
    if !(normalized.starts_with("prepare ")
        || normalized.starts_with("execute ")
        || normalized.starts_with("deallocate "))
    {
        return Ok(None);
    }

    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(error) if normalized.starts_with("prepare ") => {
            if let Some((name, param_type_oids, statement_sql)) = parse_raw_sql_prepare(sql)? {
                state.prepared.insert(
                    name,
                    PreparedStatement {
                        sql: statement_sql,
                        param_type_oids,
                    },
                );
                return Ok(Some(SqlPreparedStatementResult {
                    result: SqlResult::command("PREPARE"),
                    source_sql: None,
                }));
            }
            return Err(PgWireError::Protocol(error.to_string()));
        }
        Err(error) => return Err(PgWireError::Protocol(error.to_string())),
    };
    let [statement] = statements.as_slice() else {
        return Ok(None);
    };

    match statement {
        Statement::Prepare {
            name,
            data_types,
            statement,
        } => {
            let name = sql_prepared_name_from_ident(name);
            let param_type_oids = data_types
                .iter()
                .map(|data_type| require_type_oid(&data_type.to_string()))
                .collect::<Result<Vec<_>>>()?;
            state.prepared.insert(
                name,
                PreparedStatement {
                    sql: statement.to_string(),
                    param_type_oids,
                },
            );
            Ok(Some(SqlPreparedStatementResult {
                result: SqlResult::command("PREPARE"),
                source_sql: None,
            }))
        }
        Statement::Execute {
            name,
            parameters,
            has_parentheses: _,
            immediate,
            into,
            using,
            output,
            default,
        } => {
            if *immediate || !into.is_empty() || !using.is_empty() || *output || *default {
                return Err(PgWireError::Sql(SqlError::Unsupported(
                    "unsupported EXECUTE statement variant".to_string(),
                )));
            }
            let Some(name) = name else {
                return Err(PgWireError::Sql(SqlError::InvalidSql(
                    "EXECUTE requires a prepared statement name".to_string(),
                )));
            };
            let name = sql_prepared_name_from_object(name)?;
            let prepared = state.prepared.get(&name).cloned().ok_or_else(|| {
                PgWireError::Sql(SqlError::InvalidSql(format!(
                    "prepared statement {name:?} not found"
                )))
            })?;
            if parameters.len() != prepared.param_type_oids.len() {
                return Err(PgWireError::Sql(SqlError::InvalidSql(format!(
                    "prepared statement {name:?} expects {} parameters, got {}",
                    prepared.param_type_oids.len(),
                    parameters.len()
                ))));
            }
            let params = parameters
                .iter()
                .zip(prepared.param_type_oids.iter().copied())
                .map(|(expr, oid)| sql_prepared_parameter(expr, oid))
                .collect::<Result<Vec<_>>>()?;
            let sql = substitute_parameters(&prepared.sql, &params)?;
            let result = execute_server_sql_with_options(server, state, &sql, true)?;
            Ok(Some(SqlPreparedStatementResult {
                result,
                source_sql: Some(prepared.sql),
            }))
        }
        Statement::Deallocate { name, .. } => {
            let name = sql_prepared_name_from_ident(name);
            if name.eq_ignore_ascii_case("all") {
                state.prepared.clear();
            } else {
                state.prepared.remove(&name);
            }
            Ok(Some(SqlPreparedStatementResult {
                result: SqlResult::command("DEALLOCATE"),
                source_sql: None,
            }))
        }
        _ => Ok(None),
    }
}

fn parse_raw_sql_prepare(sql: &str) -> Result<Option<(String, Vec<i32>, String)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let Some(rest) = strip_prefix_ci(trimmed, "prepare ") else {
        return Ok(None);
    };
    let rest = rest.trim_start();
    let (name, mut rest) = parse_prepared_statement_name(rest)?;
    rest = rest.trim_start();

    let mut param_type_oids = Vec::new();
    if rest.starts_with('(') {
        let close = matching_close_paren(rest, 0).ok_or_else(|| {
            PgWireError::Protocol("PREPARE parameter type list is not balanced".to_string())
        })?;
        let parameter_types = &rest[1..close];
        if !parameter_types.trim().is_empty() {
            param_type_oids = split_top_level_commas(parameter_types)
                .into_iter()
                .map(require_type_oid)
                .collect::<Result<Vec<_>>>()?;
        }
        rest = rest[close + 1..].trim_start();
    }

    let Some(as_idx) = find_top_level_keyword(rest, "as") else {
        return Err(PgWireError::Protocol(
            "PREPARE requires AS query".to_string(),
        ));
    };
    let statement_sql = rest[as_idx + "as".len()..].trim();
    if statement_sql.is_empty() {
        return Err(PgWireError::Protocol(
            "PREPARE requires a query after AS".to_string(),
        ));
    }

    Ok(Some((name, param_type_oids, statement_sql.to_string())))
}

fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        .then_some(&value[prefix.len()..])
}

fn parse_prepared_statement_name(input: &str) -> Result<(String, &str)> {
    if input.starts_with('"') {
        let mut idx = 1;
        let bytes = input.as_bytes();
        while idx < bytes.len() {
            if bytes[idx] == b'"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                let name = normalize_identifier(&input[..=idx]);
                return Ok((name, &input[idx + 1..]));
            }
            idx += 1;
        }
        return Err(PgWireError::Protocol(
            "quoted PREPARE statement name is not terminated".to_string(),
        ));
    }

    let end = input
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace() || *ch == '(')
        .map(|(idx, _)| idx)
        .unwrap_or(input.len());
    let name = input[..end].trim();
    if name.is_empty() {
        return Err(PgWireError::Protocol(
            "PREPARE requires a statement name".to_string(),
        ));
    }
    Ok((
        normalize_identifier(name).to_ascii_lowercase(),
        &input[end..],
    ))
}

fn matching_close_paren(sql: &str, open_idx: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(open_idx) != Some(&b'(') {
        return None;
    }
    let mut depth = 0usize;
    let mut idx = open_idx;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_single {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if byte == b'"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn sql_prepared_name_from_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn sql_prepared_name_from_object(name: &ObjectName) -> Result<String> {
    let [part] = name.0.as_slice() else {
        return Err(PgWireError::Sql(SqlError::InvalidSql(format!(
            "prepared statement name must be unqualified, got {name}"
        ))));
    };
    let Some(ident) = part.as_ident() else {
        return Err(PgWireError::Sql(SqlError::InvalidSql(format!(
            "prepared statement name must be an identifier, got {name}"
        ))));
    };
    Ok(sql_prepared_name_from_ident(ident))
}

fn sql_prepared_parameter(expr: &Expr, oid: i32) -> Result<String> {
    let type_name = bicdb_sql::pg_type_name_by_oid(oid).ok_or_else(|| {
        PgWireError::Protocol(format!(
            "SQL prepared parameter type OID {oid} is not registered"
        ))
    })?;
    Ok(format!("CAST({expr} AS {type_name})"))
}

fn execute_server_sql_for_describe(server: &Arc<PgWireServer>, sql: &str) -> Result<SqlResult> {
    let _permit = server.acquire_query(query_kind_for_sql(sql))?;
    execute_server_sql_for_describe_inner(server, sql, &SqlSessionGucState::default(), None)
}

fn execute_server_sql_for_describe_with_session(
    server: &Arc<PgWireServer>,
    sql: &str,
    session_state: &SqlSessionGucState,
    security_context: Option<&SecurityContext>,
) -> Result<SqlResult> {
    let _permit = server.acquire_query(query_kind_for_sql(sql))?;
    execute_server_sql_for_describe_inner(server, sql, session_state, security_context)
}

fn execute_server_sql_for_describe_inner(
    server: &Arc<PgWireServer>,
    sql: &str,
    session_state: &SqlSessionGucState,
    security_context: Option<&SecurityContext>,
) -> Result<SqlResult> {
    if let Some(result) = delegation::delegation_description(sql)? {
        return Ok(result);
    }
    if let Some(result) = execute_server_virtual_query(server, sql)? {
        return Ok(result);
    }
    {
        let db = server.read_db()?;
        if let Some(columns) = infer_query_result_columns_without_from(&db, sql)? {
            let (names, types): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
            return Ok(SqlResult::empty(names).with_column_types(types));
        }
    }
    execute_server_db_sql(
        server,
        sql,
        &CancellationToken::uncancelable(),
        session_state,
        security_context,
    )
}

fn execute_server_db_sql(
    server: &Arc<PgWireServer>,
    sql: &str,
    cancellation: &CancellationToken,
    session_state: &SqlSessionGucState,
    security_context: Option<&SecurityContext>,
) -> Result<SqlResult> {
    if is_read_only_sql_for_shared_execution(sql) {
        let (result, pending) = {
            let db = server.read_db()?;
            let tx = db.begin_transaction()?;
            let mut session = sql_session_shared_for_server(server, &db)
                .with_security_context(security_context.cloned())
                .with_session_guc_state(session_state.clone())
                .with_cancellation(cancellation.clone())
                .with_pending_transaction(tx)
                .with_deferred_commit();
            let result = session.execute(sql);
            let pending = session.take_pending_transaction();
            (result, pending)
        };
        if let Some(tx) = pending {
            tx.rollback()?;
        }
        if !matches!(&result, Err(SqlError::Unsupported(message)) if message == "operation requires exclusive database access")
        {
            return result.map_err(PgWireError::from);
        }
        // A SELECT can invoke a PL/pgSQL function which writes. PostgreSQL
        // permits that call. This path is used for protocol description, so
        // evaluate it with exclusive access but defer and roll back any writes;
        // describing a prepared statement must never execute its side effects.
        ensure_server_writable(server)?;
        let admission = server.try_admit_write()?;
        let mut db = server.write_db_with_admission(&admission)?;
        let tx = db.begin_transaction()?;
        let mut session = sql_session_for_server(server, &mut db)
            .with_security_context(security_context.cloned())
            .with_session_guc_state(session_state.clone())
            .with_cancellation(cancellation.clone())
            .with_pending_transaction(tx)
            .with_deferred_commit();
        let result = session.execute(sql);
        if let Some(tx) = session.take_pending_transaction() {
            tx.rollback()?;
        }
        return result.map_err(PgWireError::from);
    }
    execute_server_write_sql(server, sql, cancellation, session_state, security_context)
}

fn execute_server_db_sql_for_state(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    let trace = routine_outcome_trace::RoutineOutcomeTrace::start(normalized);
    let result =
        execute_server_db_sql_for_state_inner(server, state, sql, normalized, cancellation);
    if let Some(trace) = trace {
        trace.finish(result.is_ok());
    }
    result
}

fn execute_server_db_sql_for_state_inner(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    let read_only_candidate = is_read_only_sql_for_shared_execution_normalized(normalized);
    if !read_only_candidate {
        ensure_server_writable(server)?;
    }

    // Concurrent path: execute the statement under a shared read lock (so
    // executions of different write transactions overlap), then apply the
    // buffered transaction under the exclusive write lock. Statements that need
    // exclusive access (DDL, non-transactional writes) error during the shared
    // attempt and fall through to the exclusive path below. A commit conflict
    // re-runs once on the exclusive path (no optimistic re-execution) and
    // debits the server-wide gate, so sustained contention collapses the shared
    // path back to serial execution and avoids writer starvation.
    let mut catalog_cache = std::mem::take(&mut state.catalog_cache);
    // Shared (concurrent) commit path with BOUNDED OPTIMISTIC RETRY on conflict.
    // Re-executing under a fresh snapshot makes forward progress WITHOUT ever
    // taking the exclusive write lock, so a burst of first-committer-wins
    // conflicts on a hot row no longer ping-pongs into the exclusive path --
    // which, under the fair RwLock, blocks all concurrent readers/committers (the
    // convoy that made vu4/vu8 regress). Only after exhausting retries (or when
    // the shared-slot gate is saturated) do we fall back to the exclusive path for
    // guaranteed progress. DDL / non-transactional statements still error out of
    // the shared attempt and take the exclusive path.
    const MAX_SHARED_COMMIT_ATTEMPTS: u32 = 8;
    for _attempt in 0..MAX_SHARED_COMMIT_ATTEMPTS {
        // Plain SELECTs must never be throttled by the write gate. A SELECT
        // can still invoke a writing PL/pgSQL function, though, so execute it
        // with deferred commit and only enter write admission when the session
        // actually produced a buffered transaction. Keeping that work under a
        // shared database guard is important: a function waiting for a row
        // owner must not hold the global exclusive guard that the owner needs
        // to COMMIT and release the row.
        let _shared_slot = if read_only_candidate {
            None
        } else {
            let Some(slot) = server.try_acquire_shared_write() else {
                break;
            };
            Some(slot)
        };
        let started = Instant::now();
        let (result, pending, session_state, cache) = {
            let db = server.read_db()?;
            let mut session = with_connection_guc_state(
                sql_session_shared_for_server(server, &db),
                server,
                state,
            )
            .with_catalog_cache(catalog_cache)
            .with_cancellation(cancellation.clone())
            .with_snapshot_floor(state.last_commit_seq)
            .with_deferred_commit();
            // Top-level DML starts its deferred transaction in the SQL
            // dispatcher. A SELECT does not, but it may invoke a writing
            // PL/pgSQL function. Seed SELECT candidates so nested DML joins
            // this statement transaction instead of trying to mutate the
            // database directly under the shared server guard.
            if read_only_candidate {
                session = session
                    .with_pending_transaction(db.begin_transaction_after(state.last_commit_seq)?);
            }
            let result = session.execute(sql);
            let session_state = session.session_guc_state();
            let pending = session.take_pending_transaction();
            let cache = session.into_catalog_cache();
            (result, pending, session_state, cache)
        };
        catalog_cache = cache;
        // Needs exclusive access (DDL / non-transactional) or hit a genuine SQL
        // error: hand off to the exclusive path, which re-runs and surfaces it.
        let Ok(res) = result else {
            break;
        };
        let Some(tx) = pending else {
            // Read-only / no buffered write: nothing to commit.
            state.session_state = session_state;
            state.catalog_cache = catalog_cache;
            if !read_only_candidate {
                server.record_write_execution(duration_nanos_u64(started.elapsed()));
            }
            return Ok(res);
        };
        if tx.write_len() == 0 && tx.broker_publish_len() == 0 && !tx.has_deferred_hooks() {
            tx.rollback()?;
            state.session_state = session_state;
            // The database transaction is empty, but the successful statement
            // still commits its implicit PostgreSQL GUC transaction: SET LOCAL
            // values revert while session-scoped values persist.
            state.session_state.commit_transaction();
            state.catalog_cache = catalog_cache;
            return Ok(res);
        }
        ensure_server_writable(server)?;
        // An autocommit statement's deferred constraint triggers fire at its
        // commit, which is here.
        let mut tx =
            match crate::sql_parsing::drain_deferred_trigger_hooks(server, state, cancellation, tx)
            {
                Ok(tx) => tx,
                Err(error) => {
                    state.session_state = session_state;
                    return Err(error);
                }
            };
        // Serialize the WAL frames off the serial commit critical section.
        let phase_exec = started.elapsed();
        tx.prepare_wal_payloads();
        let phase_wal = started.elapsed();
        let admission = server.try_admit_write()?;
        let phase_admit = started.elapsed();
        let rdb = server.read_db_for_transaction_progress()?;
        let phase_rdb = started.elapsed();
        match rdb.commit_buffered_transaction(&mut tx) {
            Ok(commit_seq) => {
                let phase_commit = started.elapsed();
                // Decoupled group commit: release the read lock, THEN make the WAL
                // durable so the next commit's apply overlaps our durable write.
                drop(rdb);
                drop(admission);
                state.session_state = session_state;
                state.session_state.commit_transaction();
                state.catalog_cache = catalog_cache;
                let durability = server.tx_log.write_durable(commit_seq);
                let admission_finalization = durability
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| BicDbError::Cluster(error.to_string()))
                    .and_then(|_| tx.finalize_commit_admission());
                let finalization = tx.finalize_committed_memory_jobs();
                commit_phase_trace(
                    phase_exec,
                    phase_wal,
                    phase_admit,
                    phase_rdb,
                    phase_commit,
                    started.elapsed(),
                );
                server.note_shared_commit(false);
                // Read-your-writes: future transactions on this connection floor
                // their snapshot at this commit so they never self-conflict on a
                // row they just wrote while the visibility watermark lags.
                state.last_commit_seq = state.last_commit_seq.max(commit_seq);
                server.record_write_execution(duration_nanos_u64(started.elapsed()));
                durability?;
                admission_finalization?;
                finalization?;
                return Ok(res);
            }
            Err(BicDbError::TransactionConflict(_)) => {
                // Lost the first-committer-wins race. Do NOT debit the gate here:
                // most conflicts under concurrent commit are TRANSIENT watermark-lag
                // false conflicts (the contiguous visibility watermark lags the
                // assigned commit_seq by the in-flight count, so a cross-warehouse
                // read/write can momentarily look stale) that HEAL on the very next
                // shared retry with a fresh snapshot. Debiting per conflict crashed
                // the gate to its floor and shoved the heavy NewOrder txn onto the
                // serialized exclusive path (3b's NOPM collapse). Retry on the shared
                // path; only debit ONCE below if every retry is exhausted (genuine
                // sustained contention). Correctness is unaffected: lost-update
                // prevention lives in the core conflict check + write_locks
                // regardless of which path commits.
                drop(tx);
                drop(rdb);
                drop(admission);
                continue;
            }
            Err(error) => return Err(PgWireError::from(error)),
        }
    }

    // Every shared retry was exhausted (or the gate was already closed): this is
    // genuine sustained contention, so debit the gate ONCE to throttle the shared
    // slot count, then fall back to the exclusive path for guaranteed progress.
    if !read_only_candidate {
        server.note_shared_commit(true);
    }

    // Exclusive fallback.
    ensure_server_writable(server)?;
    let admission = server.try_admit_write()?;
    let mut db = server.write_db_with_admission(&admission)?;
    let started = Instant::now();
    let mut session =
        with_connection_guc_state(sql_session_for_server(server, &mut db), server, state)
            .with_catalog_cache(catalog_cache)
            .with_cancellation(cancellation.clone());
    let result = session.execute(sql).map_err(PgWireError::from);
    capture_connection_guc_state(state, &session);
    state.catalog_cache = session.into_catalog_cache();
    server.record_write_execution(duration_nanos_u64(started.elapsed()));
    result
}

fn execute_server_transaction_sql(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    cancellation
        .check()
        .map_err(|error| classify_bicdb_interrupt(server, state, error))?;
    let Some(tx) = state.tx.take() else {
        return Err(PgWireError::Server(
            "transaction is open but no transaction state is available".to_string(),
        ));
    };
    let started = Instant::now();
    let catalog_cache = std::mem::take(&mut state.catalog_cache);
    let ddl_undo_log = std::mem::take(&mut state.tx_ddl_undo);
    let (result, session_state, pending_tx, catalog_cache, ddl_undo_log) = {
        let db = server.read_db_for_transaction_progress()?;
        let mut session =
            with_connection_guc_state(sql_session_shared_for_server(server, &db), server, state)
                .with_catalog_cache(catalog_cache)
                .with_ddl_undo_log(ddl_undo_log)
                .with_cancellation(cancellation.clone())
                .with_pending_transaction(tx);
        let result = session.execute(sql);
        let session_state = session.session_guc_state();
        let pending_tx = session.take_pending_transaction();
        let ddl_undo_log = session.take_ddl_undo_log();
        let catalog_cache = session.into_catalog_cache();
        (
            result,
            session_state,
            pending_tx,
            catalog_cache,
            ddl_undo_log,
        )
    };
    let (result, session_state, pending_tx, catalog_cache, ddl_undo_log) = if matches!(&result, Err(SqlError::Unsupported(message)) if message == "operation requires exclusive database access")
    {
        let Some(tx) = pending_tx else {
            return Err(PgWireError::Server(
                "transaction is open but no transaction state is available".to_string(),
            ));
        };
        ensure_server_writable(server)?;
        let admission = server.try_admit_write()?;
        let mut db = server.write_db_with_admission(&admission)?;
        let mut session = sql_session_for_server(server, &mut db)
            .with_security_context(state.security_context.clone())
            .with_session_guc_state(session_state)
            .with_catalog_cache(catalog_cache)
            .with_ddl_undo_log(ddl_undo_log)
            .with_cancellation(cancellation.clone())
            .with_pending_transaction(tx);
        let result = session.execute(sql);
        let session_state = session.session_guc_state();
        let pending_tx = session.take_pending_transaction();
        let ddl_undo_log = session.take_ddl_undo_log();
        let catalog_cache = session.into_catalog_cache();
        (
            result,
            session_state,
            pending_tx,
            catalog_cache,
            ddl_undo_log,
        )
    } else {
        (
            result,
            session_state,
            pending_tx,
            catalog_cache,
            ddl_undo_log,
        )
    };
    state.session_state = session_state;
    state.tx = pending_tx;
    // The session — not the dispatcher's string match — is authoritative for
    // whether a transaction block is still open. Non-canonical spellings
    // (`ROLLBACK WORK`, `COMMIT WORK`, `END`, `ROLLBACK TRANSACTION`) reach
    // the session directly, which ends the transaction; leaving
    // `in_transaction` set desynced the connection, so ReadyForQuery kept
    // claiming 'T' and the next BEGIN failed with `nested transactions are
    // not supported`.
    if state.tx.is_none() && state.in_transaction {
        clear_open_transaction_state(server, state);
        server
            .advisory_locks
            .release_transaction(state.connection_id);
    }
    state.tx_ddl_undo = ddl_undo_log;
    state.catalog_cache = catalog_cache;
    if is_write_sql(normalized) {
        server.record_write_execution(duration_nanos_u64(started.elapsed()));
    }
    result.map_err(PgWireError::from)
}

fn execute_server_write_sql(
    server: &Arc<PgWireServer>,
    sql: &str,
    cancellation: &CancellationToken,
    session_state: &SqlSessionGucState,
    security_context: Option<&SecurityContext>,
) -> Result<SqlResult> {
    ensure_server_writable(server)?;
    let admission = server.try_admit_write()?;
    let mut db = server.write_db_with_admission(&admission)?;
    let started = Instant::now();
    let result = sql_session_for_server(server, &mut db)
        .with_security_context(security_context.cloned())
        .with_session_guc_state(session_state.clone())
        .with_cancellation(cancellation.clone())
        .execute(sql)
        .map_err(PgWireError::from);
    server.record_write_execution(duration_nanos_u64(started.elapsed()));
    result
}

fn sql_session_for_server<'db>(server: &PgWireServer, db: &'db mut BicDb) -> SqlSession<'db> {
    match server.config.security_context.clone() {
        Some(ctx) => SqlSession::new_secure(db, ctx),
        None => SqlSession::new(db),
    }
}

fn sql_session_shared_for_server<'db>(server: &PgWireServer, db: &'db BicDb) -> SqlSession<'db> {
    match server.config.security_context.clone() {
        Some(ctx) => SqlSession::new_shared_secure(db, ctx),
        None => SqlSession::new_shared(db),
    }
}

fn with_connection_guc_state<'db>(
    session: SqlSession<'db>,
    server: &PgWireServer,
    state: &ConnectionState,
) -> SqlSession<'db> {
    session
        .with_security_context(state.security_context.clone())
        .with_session_guc_state(state.session_state.clone())
        .with_runtime(Arc::new(PgWireSqlRuntime {
            connection_id: state.connection_id,
            in_transaction: state.in_transaction,
            advisory_locks: Arc::clone(&server.advisory_locks),
        }))
}

fn capture_connection_guc_state(state: &mut ConnectionState, session: &SqlSession<'_>) {
    state.session_state = session.session_guc_state();
}

fn execute_server_sql(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
) -> Result<SqlResult> {
    execute_server_sql_with_options(server, state, sql, true)
}

fn execute_server_sql_with_options(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    enforce_max_result_rows: bool,
) -> Result<SqlResult> {
    let normalized = normalize_executable_sql(sql);
    execute_server_sql_with_options_normalized(
        server,
        state,
        sql,
        &normalized,
        enforce_max_result_rows,
    )
}

/// `execute_server_sql_with_options` for a caller that already normalized
/// the text (the simple-query path normalizes once per query).
fn execute_server_sql_with_options_normalized(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
    enforce_max_result_rows: bool,
) -> Result<SqlResult> {
    if [
        "create role ",
        "create user ",
        "alter role ",
        "alter user ",
        "drop role ",
        "drop user ",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
    {
        check_sql_credential_changes(&server.auth_path, sql)?;
    }
    let transaction_end = delegation::transaction_end(sql, normalized)?;
    if !matches!(
        transaction_end,
        Some(delegation::TransactionEnd::Rollback { .. })
    ) && !(transaction_end.is_some() && state.failed_transaction)
    {
        delegation::validate_delegation(server, state)?;
    }
    if is_server_status_query_normalized(normalized) {
        if let Some(result) = execute_server_virtual_query_for_normalized(
            server,
            sql,
            normalized,
            Some(VirtualQueryCaller {
                connection_id: state.connection_id,
                user: state.user.as_str(),
            }),
        )? {
            return Ok(result);
        }
    }
    if let Some(cluster) = server.cluster.as_ref().and_then(Weak::upgrade) {
        if let Some(DatabaseDdl::Create { name, owner }) = parse_database_ddl(sql)? {
            if state.in_transaction {
                return Err(PgWireError::Server(
                    "CREATE DATABASE cannot run inside a transaction block".to_string(),
                ));
            }
            return cluster.create_database(server, state, sql, &name, owner.as_deref());
        }
    }
    let _permit = match server.acquire_query(query_kind_for_normalized_sql(normalized)) {
        Ok(permit) => permit,
        Err(PgWireError::QueryTimedOut) => {
            server.record_cancel_metadata(state.connection_id, "timeout", "57014");
            return Err(PgWireError::QueryTimedOut);
        }
        Err(error) => return Err(error),
    };
    if state.take_cancel_request() {
        server.canceled_queries.fetch_add(1, Ordering::SeqCst);
        server.record_cancel_metadata(state.connection_id, "cancel", "57014");
        return Err(PgWireError::QueryCanceled);
    }
    state.last_query_at = Some(unix_timestamp());

    let started = Instant::now();
    let deadline = (!server.config.query_timeout.is_zero())
        .then(|| Instant::now() + server.config.query_timeout);
    let cancellation = CancellationToken::new(state.cancel_token.clone(), deadline);
    let transaction_control = if is_transaction_control_candidate(normalized) {
        Some(parse_transaction_control(sql).map_err(PgWireError::from)?)
    } else {
        None
    };
    if is_write_sql(normalized) {
        ensure_server_writable(server)?;
    }
    set_active_query(server, state, sql);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if transaction_control == Some(Some(TransactionControl::Begin)) {
            begin_buffered_transaction(server, state)
        } else if transaction_control == Some(Some(TransactionControl::SetTransaction)) {
            if state.failed_transaction {
                Err(PgWireError::InFailedTransaction)
            } else {
                Ok(SqlResult::command("SET"))
            }
        } else if let Some(ending) = transaction_end {
            let (result, chain) = match ending {
                delegation::TransactionEnd::Commit { chain } if !state.failed_transaction => (
                    commit_buffered_transaction(server, state, &cancellation)?,
                    chain,
                ),
                delegation::TransactionEnd::Commit { chain }
                | delegation::TransactionEnd::Rollback { chain } => {
                    rollback_open_transaction(server, state)?;
                    (SqlResult::command("ROLLBACK"), chain)
                }
            };
            if chain {
                begin_buffered_transaction(server, state)?;
            }
            Ok(result)
        } else if let Some(command) = parse_savepoint_command(normalized) {
            execute_savepoint_command(server, state, command)
        } else if state.failed_transaction {
            Err(PgWireError::InFailedTransaction)
        } else if let Some(result) = delegation::execute_delegation(server, state, sql)? {
            Ok(result)
        } else if state.in_transaction {
            if let Some(result) = execute_server_virtual_query_cancellable(
                server,
                state,
                sql,
                normalized,
                &cancellation,
            )? {
                Ok(result)
            } else {
                execute_server_transaction_sql(server, state, sql, normalized, &cancellation)
            }
        } else if let Some(result) =
            execute_server_virtual_query_cancellable(server, state, sql, normalized, &cancellation)?
        {
            Ok(result)
        } else {
            execute_server_db_sql_for_state(server, state, sql, normalized, &cancellation)
        }
    }))
    .unwrap_or_else(|_| {
        log_operational_event(
            "query.panic_contained",
            "error",
            json!({ "connection_id": state.connection_id }),
        );
        Err(PgWireError::Server(
            "internal query execution panic was contained".to_string(),
        ))
    });

    let result = match result {
        Ok(result) => result,
        Err(error) => {
            clear_active_query(server, state);
            return classify_query_interrupt(server, state, error);
        }
    };

    if !server.config.query_timeout.is_zero() && started.elapsed() > server.config.query_timeout {
        server.timed_out_queries.fetch_add(1, Ordering::SeqCst);
        state.take_cancel_request();
        server.record_cancel_metadata(state.connection_id, "timeout", "57014");
        clear_active_query(server, state);
        return Err(PgWireError::QueryTimedOut);
    }
    if enforce_max_result_rows && result.rows.len() > server.config.max_result_rows {
        clear_active_query(server, state);
        return Err(PgWireError::Server(format!(
            "query returned {} rows, exceeding max_result_rows {}",
            result.rows.len(),
            server.config.max_result_rows
        )));
    }
    if let Err(error) = enforce_persistent_connection_memory(state, &server.config) {
        // A resource-limit error aborts the current SQL transaction. Restore
        // its session state now so the rejected mutation cannot keep the
        // connection above the cap and a following ROLLBACK can recover it.
        state.session_state.rollback_transaction();
        let error = classify_memory_error_after_transaction_cleanup(state, &server.config, error);
        clear_active_query(server, state);
        return Err(error);
    }
    maybe_log_slow_query(server, sql, result.rows.len(), started.elapsed());

    state.queries_executed += 1;
    server.queries_executed.fetch_add(1, Ordering::SeqCst);
    record_proc_mix_if_enabled(server, &normalized);
    if is_write_sql(&normalized) {
        server.writes_executed.fetch_add(1, Ordering::SeqCst);
    }
    let shared_role_ddl = shared_role_ddl_statements(sql);
    if normalized == "discard all" {
        state.prepared.clear();
        state.portals.clear();
        state.catalog_cache = SqlSessionCatalogCache::default();
    }
    if state.in_transaction {
        state.tx_shared_role_ddl.extend(shared_role_ddl);
    } else {
        let mut committed = std::mem::take(&mut state.committed_shared_role_ddl);
        committed.extend(shared_role_ddl);
        if !committed.is_empty() {
            let sql = committed.join(";\n");
            sync_role_credentials_after_success(server, &sql);
            if let Some(cluster) = server.cluster.as_ref().and_then(Weak::upgrade) {
                cluster.sync_role_ddl(server, &sql)?;
            }
        }
    }
    clear_active_query(server, state);
    Ok(result)
}

// Role DDL executes inside bicdb-sql, which has no access to the pgwire
// credential store. After a statement batch succeeds, mirror any
// CREATE/ALTER ROLE ... PASSWORD and DROP ROLE into the store so
// least-privilege roles (e.g. NOSUPERUSER NOBYPASSRLS app roles) can
// actually authenticate over the wire.
fn role_credential_changes(sql: &str) -> Vec<(String, PasswordDdl)> {
    let mut changes = Vec::new();
    for statement in split_sql_statements(sql) {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let is_create_or_alter = lower.starts_with("create role ")
            || lower.starts_with("create user ")
            || lower.starts_with("alter role ")
            || lower.starts_with("alter user ");
        if is_create_or_alter {
            let rest = &trimmed[lower
                .find(" role ")
                .or_else(|| lower.find(" user "))
                .map(|pos| pos + " role ".len())
                .unwrap_or(trimmed.len())..];
            let Some(name) = rest.split_whitespace().next() else {
                continue;
            };
            let role = name.trim_matches('"').to_ascii_lowercase();
            match extract_password_literal(rest) {
                Some(PasswordDdl::Literal(password)) => {
                    changes.push((role, PasswordDdl::Literal(password)));
                }
                Some(PasswordDdl::Null) => changes.push((role, PasswordDdl::Null)),
                None => {}
            }
        } else if lower.starts_with("drop role ") || lower.starts_with("drop user ") {
            let mut rest = trimmed["drop role ".len()..].trim();
            if rest.to_ascii_lowercase().starts_with("if exists ") {
                rest = rest["if exists ".len()..].trim();
            }
            for name in rest.split(',') {
                let role = name.trim().trim_matches('"').to_ascii_lowercase();
                if !role.is_empty() {
                    changes.push((role, PasswordDdl::Null));
                }
            }
        }
    }
    changes
}

fn check_sql_credential_changes(path: &Path, sql: &str) -> Result<()> {
    let changes = role_credential_changes(sql);
    if changes.is_empty() || !operator_only_credentials(path)? {
        return Ok(());
    }
    let catalog = load_user_catalog(path)?;
    if changes.iter().any(|(role, change)| {
        matches!(change, PasswordDdl::Literal(_)) || catalog.users.contains_key(role)
    }) {
        return check_sql_credential_mutation(path);
    }
    Ok(())
}

fn sync_role_credentials_after_success(server: &Arc<PgWireServer>, sql: &str) {
    for (role, change) in role_credential_changes(sql) {
        let result = match change {
            PasswordDdl::Literal(password) => {
                if password.starts_with("SCRAM-SHA-256$") || password.starts_with("md5") {
                    log_operational_event(
                        "auth.password_sync_skipped",
                        "warning",
                        json!({"role": role, "reason": "pre-hashed passwords are not importable"}),
                    );
                    continue;
                }
                create_user_record(&server.auth_path, &role, &password, None, true)
            }
            PasswordDdl::Null => remove_user_record(&server.auth_path, &role, true),
        };
        if let Err(error) = result {
            log_operational_event(
                "auth.password_sync_failed",
                "warning",
                json!({ "role": role, "error": error.to_string() }),
            );
        }
    }
}

enum PasswordDdl {
    Literal(String),
    Null,
}

// Locate the PASSWORD keyword outside quoted strings, then read the
// following single-quoted literal (with '' escapes) or the NULL keyword.
fn extract_password_literal(statement: &str) -> Option<PasswordDdl> {
    let bytes = statement.as_bytes();
    let lower = statement.to_ascii_lowercase();
    let mut idx = 0;
    let mut keyword_at = None;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => {
                idx += 1;
                while idx < bytes.len() {
                    if bytes[idx] == b'\'' {
                        if bytes.get(idx + 1) == Some(&b'\'') {
                            idx += 2;
                            continue;
                        }
                        break;
                    }
                    idx += 1;
                }
            }
            b'"' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx] != b'"' {
                    idx += 1;
                }
            }
            _ => {
                if lower[idx..].starts_with("password")
                    && (idx == 0 || !bytes[idx - 1].is_ascii_alphanumeric())
                    && !bytes
                        .get(idx + "password".len())
                        .is_some_and(|next| next.is_ascii_alphanumeric() || *next == b'_')
                {
                    keyword_at = Some(idx + "password".len());
                    break;
                }
            }
        }
        idx += 1;
    }
    let after = statement[keyword_at?..].trim_start();
    if after.to_ascii_lowercase().starts_with("null") {
        return Some(PasswordDdl::Null);
    }
    let after = after.strip_prefix('\'')?;
    let mut password = String::new();
    let mut chars = after.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                password.push('\'');
                continue;
            }
            return Some(PasswordDdl::Literal(password));
        }
        password.push(ch);
    }
    None
}

pub fn remove_user(path: impl AsRef<Path>, username: &str) -> Result<()> {
    remove_user_record(path.as_ref(), username, false)
}

fn remove_user_record(path: &Path, username: &str, sql_origin: bool) -> Result<()> {
    let _lock = lock_user_catalog(path)?;
    let mut catalog = load_user_catalog(path)?;
    if sql_origin && catalog.users.contains_key(username) {
        check_sql_credential_mutation(path)?;
    }
    delegation::write_policy_locked(path, username, None)?;
    if catalog.users.remove(username).is_some() {
        persist_user_catalog(path, &catalog)?;
    }
    Ok(())
}
