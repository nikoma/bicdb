use std::io;
use std::path::PathBuf;

use crate::page::PageId;
use crate::slotted::TupleLocator;

pub type Result<T> = std::result::Result<T, PageError>;

/// Failures the page store can produce.
///
/// Corruption variants carry the page id and the observed-versus-expected
/// values rather than a formatted string, so a caller can decide what to do
/// (fail the open, quarantine one page, rebuild a derived structure) instead of
/// only being able to log.
#[derive(Debug, thiserror::Error)]
pub enum PageError {
    #[error("page store io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("{path} is not a BicDB page file (magic {found:02x?})")]
    NotAPageFile { path: PathBuf, found: [u8; 4] },

    /// Another store already owns this directory. See `crate::lock` for why this
    /// is refused rather than allowed or waited on.
    #[error(
        "page store is already open by another process or handle (lock held on {path}); \
         a second writer would silently discard this one's committed transactions"
    )]
    AlreadyOpen { path: PathBuf },

    #[error(
        "page file {path} has format version {found}, but this binary supports {supported}; \
         open refused without mutating data"
    )]
    UnsupportedVersion {
        path: PathBuf,
        found: u16,
        supported: u16,
    },

    #[error(
        "page file {path} uses a {found}-byte page size, but was opened expecting {expected}; \
         page size is fixed when the file is created"
    )]
    PageSizeMismatch {
        path: PathBuf,
        found: u32,
        expected: u32,
    },

    #[error(
        "page {page_id} failed its checksum (stored {stored:#010x}, computed {computed:#010x})"
    )]
    ChecksumMismatch {
        page_id: PageId,
        stored: u32,
        computed: u32,
    },

    /// A page whose header is intact but whose trailing guard does not match the
    /// header — the signature of a write torn by a crash partway through.
    #[error("page {page_id} is torn: header generation {header} does not match trailer {trailer}")]
    TornPage {
        page_id: PageId,
        header: u64,
        trailer: u64,
    },

    #[error("page {page_id} has type {found}, expected {expected}")]
    UnexpectedPageType {
        page_id: PageId,
        found: u8,
        expected: u8,
    },

    #[error("page {page_id} is beyond the end of the file ({page_count} pages)")]
    OutOfBounds { page_id: PageId, page_count: u64 },

    #[error(
        "segmented page store advertises {page_count} pages but segment file {path} is missing"
    )]
    SegmentMissing { path: PathBuf, page_count: u64 },

    #[error(
        "page-file extent size {extent_bytes} is invalid: must be 0 (single-file layout) or a \
         multiple of the {page_size}-byte page size no smaller than 1 MiB"
    )]
    InvalidExtentBytes { extent_bytes: u64, page_size: u32 },

    #[error("page {page_id} is already free; refusing to link it into the free list twice")]
    PageAlreadyFree { page_id: PageId },

    #[error("page {page_id} is the currently published root and cannot be freed")]
    CannotFreeRootPage { page_id: PageId },

    #[error("page type {page_type} cannot be allocated as an ordinary data page")]
    InvalidAllocationPageType { page_type: u8 },

    #[error(
        "page free list is corrupt (head {head}, free pages {free_pages}, page count {page_count}): {reason}"
    )]
    FreeListCorruption {
        head: PageId,
        free_pages: u64,
        page_count: u64,
        reason: String,
    },

    #[error("short read of page {page_id}: got {got} of {want} bytes")]
    ShortRead {
        page_id: PageId,
        got: usize,
        want: usize,
    },

    #[error("short write of page {page_id}: wrote {got} of {want} requested bytes")]
    ShortWrite {
        page_id: PageId,
        got: usize,
        want: usize,
    },

    #[error("write-ahead log {path} is invalid at LSN {lsn}: {reason}")]
    WalCorruption {
        path: PathBuf,
        lsn: u64,
        reason: String,
    },

    #[error("meta page {page_id} is invalid: {reason}")]
    MetaCorruption { page_id: PageId, reason: String },

    #[error("invalid page size {0}: must be a power of two between 512 and 1048576")]
    InvalidPageSize(u32),

    #[error("buffer pool budget of {budget} bytes cannot hold even one {page_size}-byte page")]
    BudgetTooSmall { budget: u64, page_size: u32 },

    #[error("invalid bounded-maintenance limits: {reason}")]
    InvalidMaintenanceLimits { reason: String },

    #[error(
        "buffer pool exhausted: all {frames} frames are pinned, cannot admit page {page_id}. \
         Raise buffer_pool_bytes or reduce concurrent pinned cursors"
    )]
    PoolExhausted { page_id: PageId, frames: usize },

    #[error(
        "page {page_id} belongs to a read-only page source; writable guards are disabled for this buffer pool"
    )]
    ReadOnlyPageSource { page_id: PageId },

    #[error("page {page_id} was evicted while still pinned ({pins} pins)")]
    EvictedWhilePinned { page_id: PageId, pins: u32 },

    #[error("value of {len} bytes does not fit page {page_id} ({free} bytes free)")]
    ValueTooLarge {
        page_id: PageId,
        len: usize,
        free: usize,
    },

    #[error("slot {slot} does not exist on page {page_id} ({slot_count} slots)")]
    NoSuchSlot {
        page_id: PageId,
        slot: u16,
        slot_count: u16,
    },

    #[error("slot {slot} on page {page_id} is dead")]
    DeadSlot { page_id: PageId, slot: u16 },

    #[error(
        "write conflict on row `{key}`: transaction {transaction} cannot write a row \
         last written by transaction {conflicting} (in {field}), which is concurrent \
         with it"
    )]
    WriteConflict {
        transaction: u64,
        conflicting: u64,
        field: &'static str,
        key: String,
    },

    #[error(
        "corrupt version header on row `{key}`: {field} carries transaction id {xid}, \
         which is beyond every id the allocator has issued ({next_xid}); the row is \
         permanently unwritable until the transaction floor advances past it \
         (bicdb_advance_transaction_floor)"
    )]
    CorruptFutureTransactionId {
        key: String,
        field: &'static str,
        xid: u64,
        next_xid: u64,
    },

    #[error("transaction floor cannot advance to {target}: {reason}")]
    TransactionFloorRefused { target: u64, reason: String },

    #[error(
        "header repair refused for row `{key}`: {field} is {found}, expected {expected} — \
         the stamp does not match the authorized repair"
    )]
    HeaderRepairRefused {
        key: String,
        field: &'static str,
        expected: u64,
        found: u64,
    },

    #[error(
        "stale locator for page {page_id} slot {slot}: expected generation {expected}, \
         page is at {found}"
    )]
    StaleLocator {
        page_id: PageId,
        slot: u16,
        expected: u64,
        found: u64,
    },

    #[error(
        "MVCC version chain beginning at {head:?} is cyclic: locator {repeated:?} \
         first appeared at step {first_seen_step} and repeated at step {steps}"
    )]
    VersionChainCycle {
        head: TupleLocator,
        repeated: TupleLocator,
        first_seen_step: usize,
        steps: usize,
    },

    #[error(
        "MVCC version chain beginning at {head:?} exceeded the {limit}-version \
         safety limit; next locator is {next:?}"
    )]
    VersionChainLimitExceeded {
        head: TupleLocator,
        next: TupleLocator,
        limit: usize,
    },

    #[error(
        "MVCC version-chain repair expected head {expected:?}, but the key now points to {found:?}"
    )]
    VersionChainChanged {
        expected: TupleLocator,
        found: Option<TupleLocator>,
    },

    #[error("MVCC version-chain repair refused: {reason}")]
    VersionChainRepairRefused { reason: String },

    #[error("tiered page storage refused operation: {reason}")]
    TieredStorage { reason: String },
}

impl PageError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Whether this is a concurrency conflict the caller should retry.
    ///
    /// Distinct from corruption and from a caller mistake: a conflicting
    /// transaction did nothing wrong, and retrying after abort is the correct
    /// response rather than surfacing an error to a user.
    pub fn is_write_conflict(&self) -> bool {
        matches!(self, Self::WriteConflict { .. })
    }

    /// Whether this is a localized MVCC-chain integrity fault.
    ///
    /// Chain faults are deliberately distinct from physical page corruption:
    /// they do not imply a bad checksum, torn page, invalid file, or damaged
    /// neighboring records, and they have a targeted recovery path.
    pub fn is_version_chain_fault(&self) -> bool {
        matches!(
            self,
            Self::VersionChainCycle { .. }
                | Self::VersionChainLimitExceeded { .. }
                | Self::VersionChainChanged { .. }
                | Self::VersionChainRepairRefused { .. }
        )
    }

    /// Whether this error indicates on-disk damage rather than a caller mistake.
    /// Callers use it to decide between "reject this operation" and "this file
    /// needs recovery or restore".
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::NotAPageFile { .. }
                | Self::SegmentMissing { .. }
                | Self::ChecksumMismatch { .. }
                | Self::TornPage { .. }
                | Self::UnexpectedPageType { .. }
                | Self::FreeListCorruption { .. }
                | Self::ShortRead { .. }
                | Self::ShortWrite { .. }
                | Self::WalCorruption { .. }
                | Self::MetaCorruption { .. }
        )
    }
}
