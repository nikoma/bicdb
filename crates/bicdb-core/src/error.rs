use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, BicDbError>;

#[derive(Debug, Error)]
pub enum BicDbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error(
        "invalid collection name `{0}`: use at most 255 bytes of ASCII letters, digits, '-' or '_'"
    )]
    InvalidCollectionName(String),

    #[error("invalid stream name `{0}`: use ASCII letters, digits, '-', '_', '.', ':', or '/'")]
    InvalidStreamName(String),

    #[error("invalid event: {0}")]
    InvalidEvent(String),

    #[error("projection error: {0}")]
    ProjectionError(String),

    #[error("broker error: {0}")]
    Broker(String),

    #[error("sync bundle error: {0}")]
    SyncBundle(String),

    #[error("backup error: {0}")]
    Backup(String),

    #[error("format compatibility error: {0}")]
    FormatCompatibility(String),

    #[error("large value error: {0}")]
    LargeValue(String),

    #[error("high availability error: {0}")]
    HighAvailability(String),

    #[error("replication error: {0}")]
    Replication(String),

    #[error("consensus error: {0}")]
    Consensus(String),

    #[error("cluster error: {0}")]
    Cluster(String),

    #[error("database is read-only standby: {0}")]
    ReadOnlyStandby(String),

    #[error("memory error: {0}")]
    Memory(String),

    #[error("resource governance error: {0}")]
    ResourceGovernance(String),

    #[error("compaction error: {0}")]
    Compaction(String),

    #[error("index error: {0}")]
    Index(String),

    #[error("routing error: {0}")]
    Routing(String),

    #[error("authorization denied: {0}")]
    Authorization(String),

    #[error("mutation authority denied: {0}")]
    MutationDenied(String),

    #[error("commit validation failed: {0}")]
    CommitValidation(String),

    #[error("geometry error: {0}")]
    Geometry(String),

    #[error("encryption key required")]
    EncryptionKeyRequired,

    #[error("invalid encryption key: {0}")]
    EncryptionKeyInvalid(String),

    #[error("decryption failed for {path}: {message}")]
    DecryptionFailed { path: PathBuf, message: String },

    #[error("tamper evidence in {path}: {message}")]
    TamperEvidence { path: PathBuf, message: String },

    #[error("record id must not be empty")]
    EmptyRecordId,

    #[error("invalid record id: {0}")]
    InvalidRecordId(String),

    #[error("transaction conflict: {0}")]
    TransactionConflict(String),

    #[error("MVCC version chain fault: {0}")]
    VersionChain(String),

    #[error("transaction is no longer pending")]
    TransactionNotPending,

    #[error("canceling statement due to user request")]
    QueryCanceled,

    #[error("query_budget_exceeded: {resource} limit of {limit} reached")]
    QueryBudgetExceeded { resource: &'static str, limit: u64 },

    #[error("canceling statement due to query timeout")]
    QueryTimedOut,

    #[error("query vector must not be empty")]
    EmptyVector,

    #[error("vector contains a non-finite value")]
    NonFiniteVectorValue,

    #[error("top_k must be greater than zero")]
    InvalidTopK,

    #[error(
        "vector dimension mismatch for collection `{collection}`: expected {expected}, got {actual}"
    )]
    DimensionMismatch {
        collection: String,
        expected: usize,
        actual: usize,
    },

    #[error("invalid time range: start timestamp {start_ts} is after end timestamp {end_ts}")]
    InvalidTimeRange { start_ts: i64, end_ts: i64 },

    #[error("storage corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    #[error("paged storage error: {0}")]
    PagedStorage(String),
}
