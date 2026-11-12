use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, BicDbError>;

#[derive(Debug, Error)]
pub enum BicDbError {
    #[error("unique index `{index}` has duplicate keys")]
    UniqueIndexBuildConflict { index: String },
    #[error("unique index `{index}` has duplicate keys")]
    UniqueIndexConflict { index: String },
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

    #[error("branch error: {0}")]
    Branch(String),

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

    /// Flashback (time travel) request that cannot be answered: history not
    /// enabled, requested point older than retained history, or in the future.
    #[error("flashback error: {0}")]
    Flashback(String),

    /// The operation is not supported for this object or configuration.
    #[error("unsupported operation: {0}")]
    Unsupported(String),

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

    #[error("transaction deadlock: {0}")]
    TransactionDeadlock(String),

    #[error("MVCC version chain fault: {0}")]
    VersionChain(String),

    #[error("transaction is no longer pending")]
    TransactionNotPending,

    /// Required decision custody was absent on the original enforced-store read.
    #[error("native range catalogue custody is missing; authorized restore required")]
    NativeRangeCatalogueCustodyMissing,

    #[error("canceling statement due to user request")]
    QueryCanceled,

    #[error("query_budget_exceeded: {resource} limit of {limit} reached")]
    QueryBudgetExceeded { resource: &'static str, limit: u64 },

    #[error("canceling statement due to query timeout")]
    QueryTimedOut,

    #[error("query memory limit exceeded: {0}")]
    QueryMemoryLimit(String),

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

    /// A durability-critical write or sync failed. The database refuses
    /// writes (and the reads it fences) until it is reopened, which runs
    /// recovery and a verification checkpoint. SQLSTATE 58030 (io_error).
    /// See `docs/durability-failure-handling.md`.
    #[error(
        "database refuses writes after a failed durable write at {site}: {detail}; \
         reopen the database to run recovery and verification"
    )]
    WriteFailed { site: String, detail: String },

    /// A commit whose durability is unknown: its decision may or may not be
    /// on disk. It is neither committed nor rolled back from the caller's
    /// point of view; the next open's recovery decides, and honours the
    /// decision if it reached the disk. SQLSTATE 08007
    /// (transaction_resolution_unknown).
    ///
    /// `incarnation` and `sequence` identify the commit for
    /// `BicDb::resolve_commit_outcome` (SQL: `bicdb_commit_outcome`) once
    /// the database has been reopened. `sequence` is `None` for operations
    /// that commit only in the page store (bulk load, index builds): those
    /// are resolved by re-reading their result after the reopen.
    /// `transaction_id` is informational (absent when only the commit
    /// sequence is known, as in a quorum wait).
    #[error(
        "commit outcome unknown: {}: {detail}; do not assume it committed or rolled back. \
         Reopen the database, then {}",
        commit_outcome_identity_line(*sequence, *transaction_id, incarnation),
        commit_outcome_hint(*sequence, incarnation)
    )]
    CommitOutcomeUnknown {
        sequence: Option<u64>,
        transaction_id: Option<u64>,
        incarnation: String,
        detail: String,
    },

    /// An archiver (`archive` is `commit_log` or `page_log`) failed at
    /// `site`. The database stays writable: acknowledged commits never
    /// depend on the archive, and the local logs it would have released are
    /// retained. The archive itself is stopped for this handle: nothing more
    /// is marked archived until a new handle rewrites the unconfirmed part.
    #[error(
        "{archive} archive failed at {site}: {detail}; archiving is stopped and the local log \
         is retained until a reopened archive rewrites the unconfirmed part"
    )]
    ArchiveFailed {
        archive: String,
        site: String,
        detail: String,
    },

    /// Write refused by archive backpressure: an archive is failed or
    /// lagging and the local logs it needs (which are never dropped) would
    /// otherwise fill the data volume. SQLSTATE 53100 (disk_full). Writes
    /// resume on their own when the archive recovers. See
    /// `docs/durability-failure-handling.md`, section 7.6.
    #[error("write refused by archive backpressure: {0}")]
    ArchiveBackpressure(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

fn commit_outcome_identity_line(
    sequence: Option<u64>,
    transaction_id: Option<u64>,
    incarnation: &str,
) -> String {
    let show = |value: Option<u64>| value.map_or_else(|| "none".to_string(), |v| v.to_string());
    format!(
        "commit_seq={} transaction_id={} incarnation={incarnation}",
        show(sequence),
        show(transaction_id)
    )
}

fn commit_outcome_hint(sequence: Option<u64>, incarnation: &str) -> String {
    match sequence {
        Some(sequence) => {
            format!("resolve it with SELECT bicdb_commit_outcome('{incarnation}', {sequence})")
        }
        None => "re-read the operation's result: it has no commit sequence, and the page \
                 store's recovery kept it only if its commit record reached the disk"
            .to_string(),
    }
}

impl BicDbError {
    /// For `CommitOutcomeUnknown`: the outcome-resolution identity as a
    /// machine-readable detail line, and how to resolve it. pgwire sends
    /// them as the error's DETAIL and HINT fields.
    pub fn commit_outcome_identity(&self) -> Option<(String, String)> {
        let Self::CommitOutcomeUnknown {
            sequence,
            transaction_id,
            incarnation,
            ..
        } = self
        else {
            return None;
        };
        Some((
            commit_outcome_identity_line(*sequence, *transaction_id, incarnation),
            format!(
                "after reopening the database, {}",
                commit_outcome_hint(*sequence, incarnation)
            ),
        ))
    }

    /// Whether this error means the database is fenced by a durability
    /// failure, including a commit whose outcome that failure made unknown.
    pub fn is_durability_failure(&self) -> bool {
        matches!(
            self,
            Self::WriteFailed { .. } | Self::CommitOutcomeUnknown { .. }
        )
    }
}

/// Translate a page-layer error, keeping a durability fence distinct.
pub(crate) fn durability_error(error: bicdb_page::PageError) -> BicDbError {
    match error {
        bicdb_page::PageError::WriteFailed { site, detail } => {
            BicDbError::WriteFailed { site, detail }
        }
        bicdb_page::PageError::ArchiveFailed { site, detail } => BicDbError::ArchiveFailed {
            archive: "page_log".into(),
            site,
            detail,
        },
        other => BicDbError::PagedStorage(other.to_string()),
    }
}
