//! BicDB first-party page store.
//!
//! Phase 1 of `docs/server-paged-storage-todo.md`: the disk page format, page
//! manager, and bounded buffer pool that `storage_mode = server_paged` is built
//! on.
//!
//! # Scope and non-goals
//!
//! This crate knows about **pages**. It does not know about records, MVCC,
//! transactions, SQL, or vectors, and it does not depend on `bicdb-core`. That
//! boundary is deliberate:
//!
//! - it can be tested and benchmarked without constructing a database;
//! - its dependency graph is small enough to audit against the repository's
//!   dependency-licence policy (see `deny.toml`); and
//! - record semantics cannot leak into the storage layer, which is what makes
//!   it possible to keep `embedded_memory` and `server_paged` behaviourally
//!   identical above this line.
//!
//! Everything here is first-party, per the roadmap's non-negotiable constraint
//! that BicDB never require RocksDB, an LSM product, or another database engine.
//! The only dependencies are a CRC implementation, a mutex, a hash map, and
//! serde.

pub mod btree;
pub mod catalog;
pub mod error;
pub mod heap;
pub mod lock;
pub mod manager;
pub mod mvcc;
pub mod overflow;
pub mod page;
pub mod paged;
mod pio;
pub mod pool;
pub mod slotted;
pub mod tiered;
pub mod tiered_cache;
pub mod tiered_gc;
pub mod tiered_manifest;
pub mod tiered_reader;
pub mod wal;

pub use btree::{
    BTree, BTreeNodeViolation, BTreePageExpectation, BTreeRange, BTreeVerifyCursor,
    BTreeVerifyFault, BTreeVerifyLimits, BTreeVerifyReport, BTreeVerifyStepReport,
    BTreeVerifyStopReason, MAX_BTREE_VERIFY_CURSOR_KEY_BYTES,
    MAX_BTREE_VERIFY_DURATION_MILLIS_PER_STEP, MAX_BTREE_VERIFY_ENTRIES_PER_STEP,
    MAX_BTREE_VERIFY_HEIGHT, MAX_BTREE_VERIFY_KEY_BYTES_PER_STEP,
    MAX_BTREE_VERIFY_LEAF_PAGES_PER_STEP, MAX_BTREE_VERIFY_PAGE_BYTES_PER_STEP,
};
pub use catalog::{Catalog, FREE_BUCKETS};
pub use error::{PageError, Result};
pub use heap::{HeapFile, HeapOptions, HeapScan, HeapSnapshot};
pub use lock::DirectoryLock;
pub use manager::{
    convert_extent_layout, repair_interrupted_relayout, ExtentMigrationReport, FaultInjection,
    FreeListTruncateReport, HolePunchReport, PageClassIoSnapshot, PageIoLatencyMetrics,
    PageIoLatencySnapshot, PageIoMetrics, PageIoSnapshot, PageStore, PageStoreOptions,
    PageTypeIoSnapshot, TailReclaimLimits, TailReclaimReport, MAX_TAIL_RECLAIM_IO_BYTES,
    MAX_TAIL_RECLAIM_PAGE_VISITS, PAGE_IO_LATENCY_BUCKET_COUNT,
    PAGE_IO_LATENCY_BUCKET_UPPER_BOUNDS_NANOS,
};
pub use mvcc::{Snapshot, TransactionTable, TxStatus, VersionHeader, Xid, VERSION_HEADER_BYTES};
pub use overflow::{pages_required, payload_capacity, OverflowReader};
pub use page::{
    file_offset, usable_bytes, validate_page_size, PageHeader, PageId, PageType, DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE, MIN_PAGE_SIZE, PAGE_FORMAT_VERSION, PAGE_HEADER_BYTES, PAGE_TRAILER_BYTES,
    SUPERBLOCK_PAGE_ID,
};
pub use paged::{
    abort_exception_capacity, extract_status_spill, inspect_meta_page, repair_status_spill,
    status_spill_entries_per_page, HintedRead, MetaPageInspection, PagedCheckpointCursor,
    PagedCheckpointLimits, PagedCheckpointPhase, PagedCheckpointStepReport,
    PagedCheckpointStopReason, PagedIntegrityReport, PagedLocalityBatch, PagedPaths, PagedScan,
    PagedScanHeads, PagedStore, PagedStoreOptions, PagedStoreSnapshot, StatusSpillRepairReport,
    VacuumCursor, VacuumLimits, VacuumReport, VacuumStopReason, VersionChainFaultSample,
    VersionChainInspection, VersionChainRepairReport, VersionChainTerminal,
    VersionChainVerifyCursor, VersionChainVerifyLimits, VersionChainVerifyReport,
    VersionChainVerifyStepReport, VersionChainVerifyStopReason, VersionChainVersion,
    MAX_CHECKPOINT_FREEZE_XIDS_PER_STEP, MAX_VACUUM_BYTES_PER_STEP,
    MAX_VACUUM_DURATION_MILLIS_PER_STEP, MAX_VACUUM_PAGES_PER_STEP,
    MAX_VERSION_VERIFY_BYTES_PER_STEP, MAX_VERSION_VERIFY_CURSOR_KEY_BYTES,
    MAX_VERSION_VERIFY_DURATION_MILLIS_PER_STEP, MAX_VERSION_VERIFY_FAULT_SAMPLES,
    MAX_VERSION_VERIFY_FAULT_SAMPLE_BYTES, MAX_VERSION_VERIFY_KEYS_PER_STEP,
    MAX_VERSION_VERIFY_VERSIONS_PER_STEP, PAGED_STORE_SNAPSHOT_FORMAT_VERSION,
};
pub use pool::{
    BufferPool, BufferPoolOptions, BufferPoolSnapshot, PageGuard, PageGuardMut, PageReadSource,
    ReadAheadLimits, ReadAheadStepReport, ReadAheadStopReason, ReadAheadSubmitReport,
    WritebackCursor, WritebackLimits, WritebackStepReport, WritebackStopReason,
    MAX_READ_AHEAD_CANDIDATES_PER_STEP, MAX_READ_AHEAD_DURATION_MILLIS_PER_STEP,
    MAX_READ_AHEAD_IO_BYTES_PER_STEP, MAX_READ_AHEAD_QUEUE_PAGES,
    MAX_WRITEBACK_DURATION_MILLIS_PER_STEP, MAX_WRITEBACK_IO_BYTES_PER_STEP,
    MAX_WRITEBACK_PAGES_PER_STEP,
};
pub use slotted::{SlottedPage, SlottedPageRef, TupleLocator, SLOT_BYTES};
pub use tiered::{
    seal_page_extent, verify_page_extent, verify_page_extent_reader, ExtentObjectInventoryEntry,
    ExtentObjectKey, ExtentObjectMetadata, ImmutableExtentStore, LocalImmutableExtentStore,
    PageExtentDescriptor, PageExtentTier, TieredStorageLimits, PAGE_EXTENT_FORMAT_VERSION,
};
pub use tiered_cache::{TieredExtentCache, TieredExtentCacheLimits, TieredExtentCacheSnapshot};
pub use tiered_gc::{
    run_tiered_extent_gc_step, start_tiered_extent_gc, TieredExtentGcLimits,
    TieredExtentGcProgress, TieredExtentGcState, TIERED_EXTENT_GC_FORMAT_VERSION,
};
pub use tiered_manifest::{
    ActivePageGeneration, PageGenerationManifest, TieredManifestCatalog, TieredManifestLimits,
    PAGE_GENERATION_FORMAT_VERSION,
};
pub use tiered_reader::TieredPageReader;
pub use wal::{
    recover, CheckpointReport, Checkpointer, Lsn, RecordKind, RecoveryReport, Wal, WalRecord,
    WalSnapshot, MAX_WAL_RECORD_BYTES,
};
