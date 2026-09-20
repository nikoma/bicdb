//! A durable keyed store: heap + primary B+ tree + WAL.
//!
//! This is the Phase 0 exit gate of `docs/server-paged-storage-todo.md` — "a
//! prototype [demonstrating] random point read/write and crash recovery through
//! a stable page identifier without changing public record semantics" — and the
//! narrowest useful vertical slice of Phases 1–3.
//!
//! # Shape
//!
//! - values live in a [`crate::HeapFile`], addressed by [`TupleLocator`];
//! - a [`crate::BTree`] maps logical key -> locator;
//! - every page mutation is logged before it becomes durable.
//!
//! The two-level indirection is the point. A record can move within its page
//! (compaction) or to another page (a growing update) and the *index* entry is
//! either untouched or updated in one place — nothing else in the system holds a
//! physical pointer.
//!
//! # What this is not
//!
//! There is no MVCC here yet: no version chains, no snapshot visibility, no
//! write-conflict detection. Those are the rest of Phase 2, and this store is
//! deliberately the layer beneath them rather than a competing implementation of
//! them. What it does establish is that a durable, crash-recoverable,
//! bounded-memory keyed store works end to end on first-party pages — which is
//! the thing the roadmap said had to be proven before Phase 1 implementation
//! properly begins.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use parking_lot::{Mutex, ReentrantMutex, ReentrantMutexGuard, RwLock};
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::btree::{
    BTree, BTreeVerifyCursor, BTreeVerifyLimits, BTreeVerifyReport, BTreeVerifyStepReport,
};
use crate::catalog::Catalog;
use crate::error::{PageError, Result};
use crate::heap::{HeapFile, HeapOptions};
use crate::lock::DirectoryLock;
use crate::manager::{PageStore, PageStoreOptions, TailReclaimLimits, TailReclaimReport};
use crate::mvcc::{RecoveredOutcome, Snapshot, TransactionTable, TxStatus, VersionHeader, Xid};
use crate::page::PageId;
use crate::pool::{
    BufferPool, BufferPoolOptions, BufferPoolSnapshot, ReadAheadLimits, ReadAheadStepReport,
    ReadAheadSubmitReport, WritebackCursor, WritebackLimits, WritebackStepReport,
};
use crate::slotted::TupleLocator;
use crate::wal::{self, CheckpointReport, Checkpointer, RecoveryReport, Wal, WalSnapshot};

/// Version of the stable paged-storage telemetry contract.
pub const PAGED_STORE_SNAPSHOT_FORMAT_VERSION: u32 = 11;

/// One bounded primary-key range resolved in heap-page order and returned in
/// primary-key order. Grouping head-locator reads by page turns a corpus scan
/// over randomly distributed keys into sequential page I/O without changing
/// the caller-visible order or snapshot.
#[derive(Debug)]
pub struct PagedLocalityBatch {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub last_key: Option<Vec<u8>>,
    pub exhausted: bool,
}

/// Maximum transaction-status entries one checkpoint step may freeze.
pub const MAX_CHECKPOINT_FREEZE_XIDS_PER_STEP: u64 = 65_536;

/// Durable phase of a bounded checkpoint operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PagedCheckpointPhase {
    #[default]
    Drain,
    Freeze,
    Finalize,
    Complete,
}

/// Completed online Drain passes after which the state machine proceeds to
/// Freeze even though dirty pages remain. Requiring an instant of zero dirt
/// lets a sustained write load redirty pages faster than online passes retire
/// them and park the checkpoint in Drain forever — with the WAL growing
/// unbounded behind it. The residue is drained by Finalize, whose writeback
/// runs under the writer gate and therefore always converges.
const DRAIN_ESCALATION_PASSES: u64 = 8;

/// Multiple of `wal_max_bytes` past which a committing thread stops advancing
/// bounded phases and runs the full blocking checkpoint. `wal_max_bytes` is a
/// soft trigger; this factor is the hard bound that holds even when no
/// background supervisor is driving maintenance.
const WAL_BACKPRESSURE_FACTOR: u64 = 4;

/// Inclusive restart state for the bounded checkpoint state machine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagedCheckpointCursor {
    pub phase: PagedCheckpointPhase,
    pub writeback: WritebackCursor,
    /// Completed Drain passes that still left dirty pages. Bounded by
    /// [`DRAIN_ESCALATION_PASSES`]; carried in the cursor so escalation
    /// survives a restart of the maintenance driver.
    #[serde(default)]
    pub drain_passes: u64,
}

impl PagedCheckpointCursor {
    pub fn validate(self) -> Result<()> {
        if self.phase == PagedCheckpointPhase::Complete
            && self.writeback != WritebackCursor::default()
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "completed checkpoint cursor cannot retain writeback state".to_string(),
            });
        }
        Ok(())
    }
}

/// Immutable resource envelope for one bounded checkpoint step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagedCheckpointLimits {
    pub writeback: WritebackLimits,
    pub max_freeze_xids: u64,
    pub tail_reclaim: TailReclaimLimits,
}

impl Default for PagedCheckpointLimits {
    fn default() -> Self {
        Self {
            writeback: WritebackLimits::default(),
            max_freeze_xids: 4_096,
            tail_reclaim: TailReclaimLimits::default(),
        }
    }
}

impl PagedCheckpointLimits {
    pub fn validate(self, page_size: u32) -> Result<()> {
        crate::page::validate_page_size(page_size)?;
        self.writeback.validate(page_size)?;
        self.tail_reclaim.validate(page_size)?;
        if !(1..=MAX_CHECKPOINT_FREEZE_XIDS_PER_STEP).contains(&self.max_freeze_xids) {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "checkpoint requires 1..={MAX_CHECKPOINT_FREEZE_XIDS_PER_STEP} transaction statuses per freeze step"
                ),
            });
        }
        Ok(())
    }
}

/// Why one checkpoint step yielded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PagedCheckpointStopReason {
    Writeback,
    FreezeLimit,
    PhaseBoundary,
    Complete,
}

/// Exact outcome of one bounded checkpoint step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagedCheckpointStepReport {
    pub phase_before: PagedCheckpointPhase,
    pub next_cursor: PagedCheckpointCursor,
    pub writeback: Option<WritebackStepReport>,
    pub transactions_frozen: u64,
    pub freeze_blocked: bool,
    /// Aborted or crash-orphaned transactions this step converted into durable
    /// abort exceptions so the watermark could pass them.
    #[serde(default)]
    pub abort_exceptions_recorded: u64,
    /// The step stopped at an xid needing an abort exception while the
    /// durable exception capacity was exhausted.
    #[serde(default)]
    pub freeze_exception_capacity_full: bool,
    pub dirty_pages_remaining: u64,
    pub checkpoint: Option<CheckpointReport>,
    pub tail_reclaim: Option<TailReclaimReport>,
    pub complete: bool,
    pub stop_reason: PagedCheckpointStopReason,
}

impl PagedCheckpointStepReport {
    pub fn validate(self, limits: PagedCheckpointLimits, page_size: u32) -> Result<()> {
        limits.validate(page_size)?;
        self.next_cursor.validate()?;
        if let Some(writeback) = self.writeback {
            writeback.validate(limits.writeback, page_size)?;
            if writeback.dirty_pages_remaining != self.dirty_pages_remaining {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason: "checkpoint and writeback dirty-page reports disagree".to_string(),
                });
            }
        }
        if let Some(tail_reclaim) = self.tail_reclaim {
            tail_reclaim.validate(limits.tail_reclaim, page_size)?;
        }
        let phase_transition_valid = matches!(
            (self.phase_before, self.next_cursor.phase),
            (PagedCheckpointPhase::Drain, PagedCheckpointPhase::Drain)
                | (PagedCheckpointPhase::Drain, PagedCheckpointPhase::Freeze)
                | (PagedCheckpointPhase::Freeze, PagedCheckpointPhase::Freeze)
                | (PagedCheckpointPhase::Freeze, PagedCheckpointPhase::Finalize)
                | (PagedCheckpointPhase::Finalize, PagedCheckpointPhase::Freeze)
                | (
                    PagedCheckpointPhase::Finalize,
                    PagedCheckpointPhase::Finalize
                )
                | (
                    PagedCheckpointPhase::Finalize,
                    PagedCheckpointPhase::Complete
                )
                | (
                    PagedCheckpointPhase::Complete,
                    PagedCheckpointPhase::Complete
                )
        );
        let publication_valid = match (self.checkpoint, self.tail_reclaim) {
            (Some(checkpoint), tail_reclaim) => {
                checkpoint.wal_truncated == tail_reclaim.is_some()
                    && checkpoint.checkpoint_lsn >= checkpoint.redo_lsn
                    && self.writeback.is_some_and(|writeback| {
                        checkpoint.pages_flushed == writeback.pages_written
                    })
            }
            (None, None) => true,
            (None, Some(_)) => false,
        };
        let terminal_valid = self.complete
            == (self.stop_reason == PagedCheckpointStopReason::Complete)
            && self.complete == (self.next_cursor.phase == PagedCheckpointPhase::Complete)
            && self.complete == self.checkpoint.is_some()
            && publication_valid;
        let reason_valid = match self.stop_reason {
            PagedCheckpointStopReason::Writeback => {
                self.writeback.is_some() && self.checkpoint.is_none()
            }
            PagedCheckpointStopReason::FreezeLimit => {
                self.transactions_frozen == limits.max_freeze_xids && self.checkpoint.is_none()
            }
            PagedCheckpointStopReason::PhaseBoundary => self.checkpoint.is_none(),
            PagedCheckpointStopReason::Complete => self.checkpoint.is_some(),
        };
        if self.transactions_frozen > limits.max_freeze_xids
            || self.abort_exceptions_recorded > self.transactions_frozen
            || !phase_transition_valid
            || !terminal_valid
            || !reason_valid
            || self.complete && self.dirty_pages_remaining != 0
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "checkpoint report is inconsistent with its immutable envelope".to_string(),
            });
        }
        Ok(())
    }
}

/// Cheap, bounded-cardinality operational state for one paged store.
///
/// Every field is either an atomic counter, fixed-size buffer-pool walk, or one
/// metadata call on an already-open file. It never scans heap, index, or WAL
/// contents, so it is safe on a regular metrics scrape regardless of database
/// size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagedStoreSnapshot {
    pub format_version: u32,
    pub page_size: u32,
    /// Logical pages covered by the durable superblock, including page zero.
    pub page_count: u64,
    /// Logical mutable-page bytes (`page_count * page_size`).
    pub logical_page_bytes: u64,
    /// Current file length. May temporarily exceed `logical_page_bytes` after
    /// a safe crash window during tail reclamation.
    pub page_file_bytes: u64,
    /// Data/index/catalog pages, excluding the superblock and free pages.
    pub used_data_pages: u64,
    pub free_pages: u64,
    pub free_bytes: u64,
    pub wal_bytes: u64,
    /// Checkpoint trigger, not an absolute per-transaction hard ceiling.
    pub wal_max_bytes: u64,
    pub fsync_enabled: bool,
    pub buffer_pool: BufferPoolSnapshot,
    pub page_io: crate::manager::PageIoSnapshot,
    pub wal: WalSnapshot,
    pub recovery: RecoveryReport,
    /// Durable visibility watermark: everything below it is committed unless
    /// listed in `abort_exceptions`.
    #[serde(default)]
    pub transaction_frozen_xid: u64,
    /// One past the highest transaction id assigned.
    #[serde(default)]
    pub transaction_next_xid: u64,
    /// Resident transaction-status entries above the watermark.
    #[serde(default)]
    pub resident_transaction_entries: u64,
    /// Durable abort exceptions the watermark has stepped over.
    #[serde(default)]
    pub abort_exceptions: u64,
    /// Meta-page capacity for abort exceptions.
    #[serde(default)]
    pub abort_exception_capacity: u64,
    /// Terminal outcomes retained by the disk-backed status spill.
    #[serde(default)]
    pub status_spill_entries: u64,
    /// Pages chained by the status spill.
    #[serde(default)]
    pub status_spill_pages: u64,
    /// Demand spill reads that failed since open. Nonzero means visibility
    /// fell back to the safe direction and the page file needs attention.
    #[serde(default)]
    pub status_spill_lookup_failures: u64,
}

impl PagedStoreSnapshot {
    /// Cache hit ratio in integer basis points, avoiding floating-point metric
    /// serialization and remaining deterministic across platforms.
    pub fn buffer_pool_hit_ratio_basis_points(&self) -> u64 {
        let accesses = self
            .buffer_pool
            .hits
            .saturating_add(self.buffer_pool.misses);
        if accesses == 0 {
            0
        } else {
            self.buffer_pool
                .hits
                .saturating_mul(10_000)
                .checked_div(accesses)
                .unwrap_or(0)
        }
    }
}

/// Configuration for a paged store.
#[derive(Clone, Debug)]
pub struct PagedStoreOptions {
    pub page_size: u32,
    /// Hard ceiling on cached pages. This is `buffer_pool_bytes`.
    pub buffer_pool_bytes: u64,
    /// Hard ceiling on deduplicated speculative page requests. This bounds
    /// read-ahead metadata independently of database size.
    pub read_ahead_queue_pages: usize,
    pub fsync: bool,
    /// Checkpoint once the log exceeds this. Bounds recovery time.
    pub wal_max_bytes: u64,
    /// Bytes per page-file extent for NEWLY created stores; `0` keeps one
    /// monolithic `store.pages`. Existing stores use their recorded layout.
    pub extent_bytes: u64,
    /// WAL segment size: the active `store.wal` seals into an immutable
    /// `store.wal.<seq>` once it fills past this, and checkpoint truncation
    /// deletes sealed segments. Only engaged on extent-segmented stores —
    /// their v2 superblock magic already fail-closes engines that predate
    /// segmented layouts, and a pre-segmentation engine replaying only the
    /// active file of a segmented WAL would silently lose commits.
    pub wal_segment_bytes: u64,
    /// Accept a meta page whose abort-exception/spill extension region fails
    /// validation by reading it as the pre-durable-abort-exceptions layout
    /// (roots and watermarks only, no extension), then durably rewrite the
    /// page in the current layout at open. Engines before durable abort
    /// exceptions wrote only the 40-byte meta prefix and left the rest of the
    /// page as whatever the reused frame held, so on those stores the
    /// extension bytes are garbage that the strict reader correctly rejects.
    ///
    /// Off by default deliberately: the same validation failure on a store
    /// that WAS written with the current layout means real corruption, and
    /// reinterpreting it as legacy would silently drop durable abort
    /// exceptions — a visibility hazard. Opt in only for stores known to
    /// predate durable abort exceptions.
    pub accept_legacy_meta: bool,
}

impl Default for PagedStoreOptions {
    fn default() -> Self {
        Self {
            page_size: crate::page::DEFAULT_PAGE_SIZE,
            buffer_pool_bytes: 64 * 1024 * 1024,
            read_ahead_queue_pages: 1_024,
            fsync: true,
            wal_max_bytes: 64 * 1024 * 1024,
            extent_bytes: 0,
            wal_segment_bytes: 64 * 1024 * 1024,
            accept_legacy_meta: false,
        }
    }
}

impl PagedStoreOptions {
    pub fn with_page_size(mut self, page_size: u32) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_buffer_pool_bytes(mut self, bytes: u64) -> Self {
        self.buffer_pool_bytes = bytes;
        self
    }

    pub fn with_read_ahead_queue_pages(mut self, pages: usize) -> Self {
        self.read_ahead_queue_pages = pages;
        self
    }

    pub fn with_fsync(mut self, fsync: bool) -> Self {
        self.fsync = fsync;
        self
    }

    pub fn with_wal_max_bytes(mut self, bytes: u64) -> Self {
        self.wal_max_bytes = bytes;
        self
    }

    pub fn with_extent_bytes(mut self, bytes: u64) -> Self {
        self.extent_bytes = bytes;
        self
    }

    pub fn with_wal_segment_bytes(mut self, bytes: u64) -> Self {
        self.wal_segment_bytes = bytes;
        self
    }

    pub fn with_accept_legacy_meta(mut self, accept: bool) -> Self {
        self.accept_legacy_meta = accept;
        self
    }
}

/// Outcome of a locator-hinted read ([`PagedStore::get_as_of_hinted`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HintedRead {
    /// The hinted chain produced the version visible to the snapshot. The
    /// caller must still validate row identity (slot reuse).
    Hit(Vec<u8>),
    /// The hint cannot answer authoritatively; read through the key index.
    Fallback,
}

/// One sampled version from an MVCC chain inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainVersion {
    pub locator: TupleLocator,
    pub xmin: Xid,
    pub xmax: Xid,
    pub previous: Option<TupleLocator>,
}

/// Why an MVCC chain inspection stopped.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VersionChainTerminal {
    Empty,
    End,
    /// Vacuum legitimately removed history older than every live snapshot.
    Vacuumed {
        locator: TupleLocator,
    },
    InvalidHead,
    MalformedVersion {
        locator: TupleLocator,
    },
    Cycle {
        repeated: TupleLocator,
        first_seen_step: usize,
        repeated_at_step: usize,
    },
    LimitExceeded {
        next: TupleLocator,
        limit: usize,
    },
}

impl VersionChainTerminal {
    pub fn is_fault(&self) -> bool {
        matches!(
            self,
            Self::InvalidHead
                | Self::MalformedVersion { .. }
                | Self::Cycle { .. }
                | Self::LimitExceeded { .. }
        )
    }

    pub fn is_repairable(&self) -> bool {
        matches!(self, Self::Cycle { .. } | Self::LimitExceeded { .. })
    }
}

/// Bounded diagnostic for one keyed MVCC chain.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainInspection {
    pub head: Option<TupleLocator>,
    pub versions_examined: u64,
    /// Newest-first samples, capped independently of chain length.
    pub samples: Vec<VersionChainVersion>,
    pub terminal: VersionChainTerminal,
}

impl VersionChainInspection {
    pub fn healthy(&self) -> bool {
        !self.terminal.is_fault()
    }
}

/// One bounded fault sample from a store-wide chain verification.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainFaultSample {
    pub key: Vec<u8>,
    pub inspection: VersionChainInspection,
}

/// Store-wide MVCC chain verification counters.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainVerifyReport {
    pub keys_examined: u64,
    pub versions_examined: u64,
    pub invalid_heads: u64,
    pub malformed_versions: u64,
    pub cycles: u64,
    pub limit_exceeded: u64,
    pub fault_samples: Vec<VersionChainFaultSample>,
    pub valid: bool,
}

/// Exact inclusive restart position for one bounded MVCC verification sweep.
///
/// `next_key` is the first key that has not been examined. `None` starts a new
/// sweep and is also returned after completion; callers must stop when the
/// accompanying report says `complete` rather than submitting it again.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainVerifyCursor {
    pub next_key: Option<Vec<u8>>,
}

/// Non-weakenable envelope for one resumable MVCC verification step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainVerifyLimits {
    pub max_keys: u64,
    pub max_versions: u64,
    pub max_versions_per_chain: u64,
    /// Fixed MVCC-header bytes inspected. Row payloads are never materialized
    /// or traversed as values.
    pub max_bytes: u64,
    /// Cooperative deadline checked before every key. One chain inspection may
    /// finish after it, but that chain is independently version-bounded.
    pub max_duration_millis: u64,
    pub max_fault_samples: usize,
    /// Maximum aggregate serialized bytes retained for fault samples.
    pub max_fault_sample_bytes: u64,
    /// Prevents a crafted key from making the durable cursor unbounded.
    pub max_cursor_key_bytes: usize,
}

impl Default for VersionChainVerifyLimits {
    fn default() -> Self {
        Self {
            max_keys: 1_024,
            max_versions: 65_536,
            max_versions_per_chain: 4_096,
            max_bytes: 64 * 1024 * 1024,
            max_duration_millis: 100,
            max_fault_samples: 64,
            max_fault_sample_bytes: 4 * 1024 * 1024,
            max_cursor_key_bytes: 64 * 1024,
        }
    }
}

impl VersionChainVerifyLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_keys == 0 || self.max_keys > MAX_VERSION_VERIFY_KEYS_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_keys must be between 1 and {MAX_VERSION_VERIFY_KEYS_PER_STEP}"
                ),
            });
        }
        if self.max_versions == 0
            || self.max_versions > MAX_VERSION_VERIFY_VERSIONS_PER_STEP
            || self.max_versions_per_chain == 0
            || self.max_versions_per_chain > MAX_VERSION_CHAIN as u64
            || self.max_versions_per_chain > self.max_versions
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "version limits must be non-zero, max_versions must not exceed {MAX_VERSION_VERIFY_VERSIONS_PER_STEP}, and the per-chain limit must not exceed either max_versions or {MAX_VERSION_CHAIN}"
                ),
            });
        }
        let minimum_bytes = self
            .max_versions_per_chain
            .checked_mul(crate::mvcc::VERSION_HEADER_BYTES as u64)
            .ok_or_else(|| PageError::InvalidMaintenanceLimits {
                reason: "MVCC verification byte envelope overflowed".to_string(),
            })?;
        if self.max_bytes < minimum_bytes || self.max_bytes > MAX_VERSION_VERIFY_BYTES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_bytes must cover one maximum chain ({minimum_bytes} bytes) and not exceed {MAX_VERSION_VERIFY_BYTES_PER_STEP}"
                ),
            });
        }
        if self.max_duration_millis == 0
            || self.max_duration_millis > MAX_VERSION_VERIFY_DURATION_MILLIS_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_duration_millis must be between 1 and {MAX_VERSION_VERIFY_DURATION_MILLIS_PER_STEP}"
                ),
            });
        }
        if self.max_fault_samples > MAX_VERSION_VERIFY_FAULT_SAMPLES
            || self.max_fault_sample_bytes > MAX_VERSION_VERIFY_FAULT_SAMPLE_BYTES
            || (self.max_fault_samples == 0) != (self.max_fault_sample_bytes == 0)
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "fault sampling must be disabled with two zeroes or bounded by {MAX_VERSION_VERIFY_FAULT_SAMPLES} samples and {MAX_VERSION_VERIFY_FAULT_SAMPLE_BYTES} bytes"
                ),
            });
        }
        if self.max_cursor_key_bytes == 0
            || self.max_cursor_key_bytes > MAX_VERSION_VERIFY_CURSOR_KEY_BYTES
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_cursor_key_bytes must be between 1 and {MAX_VERSION_VERIFY_CURSOR_KEY_BYTES}"
                ),
            });
        }
        Ok(())
    }
}

/// Why one bounded MVCC verification step yielded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionChainVerifyStopReason {
    #[default]
    Complete,
    KeyLimit,
    VersionLimit,
    ByteLimit,
    TimeLimit,
}

/// Exact outcome of one resumable MVCC verification step.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionChainVerifyStepReport {
    pub version_chains: VersionChainVerifyReport,
    pub bytes_examined: u64,
    pub fault_sample_bytes: u64,
    pub fault_samples_dropped: u64,
    pub next_cursor: VersionChainVerifyCursor,
    pub stop_reason: VersionChainVerifyStopReason,
    pub elapsed_millis: u64,
    pub complete: bool,
}

/// Structural verification of the page-backed key/value store.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PagedIntegrityReport {
    pub btree: BTreeVerifyReport,
    pub version_chains: VersionChainVerifyReport,
    pub valid: bool,
}

/// Result of atomically replacing one faulty chain with an authoritative value.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionChainRepairReport {
    pub previous_head: TupleLocator,
    pub replacement_head: TupleLocator,
    pub versions_examined: u64,
    pub repaired_fault: VersionChainTerminal,
}

/// A crash-recoverable keyed store over first-party pages.
#[derive(Debug)]
pub struct PagedStore {
    pool: Arc<BufferPool>,
    heap: HeapFile,
    index: BTree,
    catalog: Arc<Catalog>,
    /// Page holding the roots of every tree in the file.
    meta_page: PageId,
    wal: Arc<Wal>,
    checkpointer: Checkpointer,
    options: PagedStoreOptions,
    next_transaction: AtomicU64,
    transactions: TransactionTable,
    /// Disk-backed terminal outcomes above a pinned watermark.
    spill: Arc<PagedOutcomeSpill>,
    /// The snapshot each live transaction began with.
    ///
    /// Held rather than reconstructed: a transaction's snapshot is a fact about
    /// the moment it began, and rebuilding it later from current state loses
    /// exactly the information write-conflict detection needs — which OTHER
    /// transactions were running then. Bounded by the number of live
    /// transactions, and dropped at commit or abort.
    snapshots: Mutex<BTreeMap<Xid, Snapshot>>,
    /// Inclusive cursor for the one bounded checkpoint step opportunistically
    /// advanced by commits after the WAL trigger is crossed. This is only a
    /// latency-safety fallback; the durable core supervisor owns production
    /// scheduling. Losing this in-memory cursor on crash is safe because WAL
    /// recovery rebuilds the page file before a new sweep starts.
    automatic_checkpoint_cursor: Mutex<PagedCheckpointCursor>,
    /// Bounded startup evidence retained for diagnostics; never contains WAL
    /// records or page contents.
    last_recovery: RecoveryReport,
    /// Held (nonzero) while an online backup streams this store's files.
    /// Checkpoints still run and drain dirty pages, but WAL truncation
    /// (including sealed-segment deletion) and page-file tail reclaim are
    /// deferred — the two operations that would mutate or delete bytes the
    /// backup is reading. Writers are otherwise unaffected.
    backup_pins: AtomicU64,
    /// Serializes every operation that mutates shared structure.
    ///
    /// The heap's free list, the B+ tree's nodes, the meta page's roots, and the
    /// buffer pool's per-frame "which transaction dirtied this" stamp are all
    /// plain shared state with no internal synchronization between writers. Two
    /// concurrent `put`s splitting the same index node interleave their
    /// read-modify-write cycles and one silently overwrites the other; two
    /// concurrent `sync_roots` calls race to publish a new root and one is lost,
    /// orphaning an entire subtree. Measured before this lock existed: eight
    /// writers committing *disjoint* keys lost 378 of 1,600 committed rows, and
    /// the loss was visible before any reopen — see
    /// `tests/concurrent_commits.rs`.
    ///
    /// Serializing writers is the honest fix for a structure that was designed
    /// single-writer. It costs write concurrency, not correctness: readers are
    /// unaffected (they take no part in this lock), and commits already
    /// serialize on the WAL file lock, so the marginal cost is smaller than it
    /// looks.
    ///
    /// Lifting it means making the B+ tree safe for concurrent structure
    /// modification — latch coupling or a B-link tree — and giving each frame a
    /// *set* of dirtying transactions rather than one stamp. That is a real
    /// project, and doing it implicitly by leaving this lock out is how a
    /// database loses committed rows.
    write_lock: ReentrantMutex<()>,
    /// Excludes readers from the tree while a writer restructures it.
    ///
    /// `write_lock` serializes writers against each other, but a reader's
    /// multi-page descent took no lock at all — so a concurrent split could
    /// tear the descent and the reader would miss a key that was never absent.
    /// Measured symptom: the hottest row in a TPC-C-style mix transiently had
    /// NO index entry from a reader's point of view (`chain=[]`), surfacing as
    /// "stub bytes missing" corruption errors under concurrent SQL load.
    ///
    /// Writers take the write side per operation (inside `write_lock`, so
    /// writer-vs-writer order is unchanged); readers take the read side for
    /// the duration of one descent. Readers never nest acquisitions — scans
    /// drop the guard between steps — so the non-reentrant lock is safe.
    /// Lifting this in favour of latch coupling / a B-link tree is the same
    /// project as lifting `write_lock`, and is called out there.
    structure: RwLock<()>,
    /// Exclusive claim on the directory, released when this store is dropped.
    ///
    /// Last field so it is dropped last: the lock must outlive every component
    /// that might still write during teardown, or a second opener could take the
    /// directory while this one is still flushing.
    _lock: DirectoryLock,
}

/// Where a paged store keeps its files.
pub struct PagedPaths {
    /// One page file holds both heap and index pages — see `PagedStore::open`.
    pub pages: std::path::PathBuf,
    pub wal: std::path::PathBuf,
}

impl PagedPaths {
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            pages: dir.join("store.pages"),
            wal: dir.join("store.wal"),
        }
    }
}

impl PagedStore {
    /// Open or create a store in `dir`, recovering from the log if needed.
    pub fn open(
        dir: impl AsRef<Path>,
        options: PagedStoreOptions,
    ) -> Result<(Self, RecoveryReport)> {
        let dir = dir.as_ref();
        // Validate the complete resident/recovery envelope before creating a
        // directory or file. A typo in an operator limit must fail without
        // partially initializing a database.
        crate::page::validate_page_size(options.page_size)?;
        if options.buffer_pool_bytes < u64::from(options.page_size) {
            return Err(PageError::BudgetTooSmall {
                budget: options.buffer_pool_bytes,
                page_size: options.page_size,
            });
        }
        if options.read_ahead_queue_pages > crate::pool::MAX_READ_AHEAD_QUEUE_PAGES {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "read-ahead queue cannot exceed {} pages",
                    crate::pool::MAX_READ_AHEAD_QUEUE_PAGES
                ),
            });
        }
        if options.wal_max_bytes < u64::from(options.page_size) {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "wal_max_bytes {} cannot be smaller than one {}-byte page",
                    options.wal_max_bytes, options.page_size
                ),
            });
        }
        std::fs::create_dir_all(dir).map_err(|e| PageError::io(dir, e))?;
        let paths = PagedPaths::in_dir(dir);

        // Before recovery, and before any page is read or written: recovery
        // itself mutates the file, so two concurrent openers would replay and
        // truncate the same log against each other.
        let lock = DirectoryLock::acquire(dir)?;

        let store = Arc::new(PageStore::open(
            &paths.pages,
            PageStoreOptions {
                page_size: options.page_size,
                fsync: options.fsync,
                create: true,
                extent_bytes: options.extent_bytes,
            },
        )?);
        // WAL segmentation rides the page store's v2 format gate: sealed
        // segments only ever appear next to a store whose superblock magic
        // already fail-closes pre-segmentation engines.
        let wal_segment_bytes = if store.extent_pages() > 0 {
            options.wal_segment_bytes
        } else {
            0
        };
        let wal = Arc::new(Wal::open_with_segments_and_floor(
            &paths.wal,
            options.fsync,
            wal_segment_bytes,
            // The superblock's checkpoint LSN horizon: keeps LSNs monotonic
            // across truncate+reopen so sealed-segment names never recur.
            store.checkpoint_lsn(),
        )?);

        // Recovery runs BEFORE anything reads a page, so no caller can ever
        // observe the un-replayed state.
        let (mut recovery, recovered_status) = wal::recover_with_outcomes(&wal, &store)?;

        let pool = Arc::new(BufferPool::new(
            store.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(options.buffer_pool_bytes)
                .with_read_ahead_queue_pages(options.read_ahead_queue_pages),
        )?);
        pool.enable_wal_managed_frees();

        // Heap, index, and catalog share ONE page file, one buffer pool, and one
        // log.
        //
        // The first version gave the index its own file, which looked tidier and
        // was wrong: the WAL covered only the heap, so a crash after commit but
        // before checkpoint recovered the rows and lost the index that found
        // them — every key reading as absent while its data sat intact on disk.
        // A single page space means one log covers both, and "committed" means
        // the same thing for data and for the index that reaches it.
        //
        // Two trees need two roots, and the superblock holds one. A meta page
        // holds both, and the superblock points at it — so a root change is one
        // page write, and neither tree can overwrite the other's root.
        let published = store.root_page();
        let (
            meta_page,
            index_root,
            catalog_root,
            frozen_xid,
            next_xid,
            abort_exceptions,
            spill_head,
            spill_pages,
            spill_entries,
            fresh,
            legacy_meta,
        ) = if published == 0 {
            let meta_page = store.allocate(crate::page::PageType::Heap)?;
            // Published immediately, not at the first checkpoint. Otherwise a
            // reopen before any checkpoint finds root_page still 0, allocates a
            // SECOND meta page, and reads two empty trees — every committed row
            // present on disk and unreachable.
            store.publish_root(meta_page, 0)?;
            (meta_page, 0, 0, 1, 1, Vec::new(), 0, 0, 0, true, false)
        } else {
            let roots = read_meta(&pool, published, options.accept_legacy_meta)?;
            (
                published,
                roots.index_root,
                roots.catalog_root,
                roots.frozen_xid,
                roots.next_xid,
                roots.abort_exceptions,
                roots.spill_head,
                roots.spill_pages,
                roots.spill_entries,
                // A published superblock over an unwritten meta page is still a
                // fresh store, and must be treated as one. Deciding freshness
                // from `published` alone is what lost every root created by the
                // second and later sessions: session 1 dirtied the meta page and
                // exited without logging or flushing it, so session 2 read "no
                // magic, empty trees", built new roots, and then skipped
                // `write_meta` because it did not believe it was fresh. Its
                // roots died with the process, and session 3 opened an empty
                // database sitting on top of every committed row.
                !roots.initialized,
                roots.legacy,
            )
        };

        let index = BTree::open_at(pool.clone(), index_root)?;
        let catalog = Arc::new(Catalog::open(pool.clone(), catalog_root)?);

        let heap = HeapFile::new(pool.clone(), HeapOptions::for_page_size(options.page_size));
        // No file scan: the durable catalog already knows every heap page. This
        // is what makes open proportional to the catalog's height rather than to
        // the file's size.
        heap.set_catalog(catalog.clone());

        let checkpointer = Checkpointer::new(pool.clone(), wal.clone());
        // Eviction must not outrun the log. See `WritebackBarrier`.
        pool.set_barrier(Arc::new(WalWritebackBarrier { wal: wal.clone() }));

        // Checkpointed watermarks first, then final log outcomes on top:
        // anything the checkpoint froze is committed unless excepted, and the
        // WAL covers what came after. Moving the recovery map directly avoids
        // a second startup container proportional to the recent transaction
        // set.
        let transactions =
            TransactionTable::with_exception_capacity(abort_exception_capacity(options.page_size));
        // The spill is loaded before outcomes are installed: anything it
        // retains is visible to `status` immediately, and the open-time
        // freeze below can only shrink what it needs to cover.
        let spill = PagedOutcomeSpill::load(
            store.clone(),
            options.page_size,
            spill_head,
            spill_pages,
            spill_entries,
        )?;
        transactions.set_spill(spill.clone());
        transactions.restore(frozen_xid, next_xid, abort_exceptions);
        transactions.record_recovered_batch(recovered_status);
        // Page images from crash-lost transactions carry ids that never
        // reached a terminal record; those ids must not be reissued.
        transactions.ensure_next_xid_at_least(recovery.max_record_transaction.saturating_add(1));

        // Nothing is in flight at open, so every recovered xid is terminal or
        // crash-dead. Advance the watermark across everything representable
        // right now: committed outcomes freeze outright, and aborted or
        // orphaned ones freeze as exceptions up to the durable capacity. This
        // is what bounds the resident outcome set after recovery — a suffix
        // retained by a since-ended blocker collapses to at most the
        // exception-capacity tail instead of staying wholly resident. The
        // advance is not persisted until the next checkpoint; a crash before
        // then simply re-derives it from the intact log.
        let open_freeze = transactions.freeze_below(transactions.next_xid())?;
        recovery.frozen_outcomes_at_open = u64::try_from(open_freeze.frozen).unwrap_or(u64::MAX);
        recovery.abort_exceptions_at_open =
            u64::try_from(transactions.abort_exception_count()).unwrap_or(u64::MAX);
        recovery.status_spill_entries_at_open = spill.entry_count();
        recovery.status_spill_pages_at_open = spill.page_count();
        // Set before `Self` is built: the report is `Copy`, and `last_recovery`
        // takes its copy now. The rewrite itself happens below; if it fails the
        // open fails with it, so no caller ever observes the flag without the
        // transform having become durable.
        recovery.legacy_meta_migrated = legacy_meta;

        let paged = Self {
            pool,
            heap,
            index,
            catalog,
            meta_page,
            wal,
            checkpointer,
            options,
            next_transaction: AtomicU64::new(1),
            transactions,
            spill,
            snapshots: Mutex::new(BTreeMap::new()),
            automatic_checkpoint_cursor: Mutex::new(PagedCheckpointCursor::default()),
            last_recovery: recovery,
            backup_pins: AtomicU64::new(0),
            write_lock: ReentrantMutex::new(()),
            structure: RwLock::new(()),
            _lock: lock,
        };

        // A brand-new store must write the meta page NOW, not at the first root
        // split. `sync_roots` only fires when a root moves, so a small database
        // that never splits would leave the meta page unwritten — and the next
        // open would read no magic, assume both trees are new, and present an
        // empty database sitting on top of every committed row.
        if fresh {
            paged.write_meta()?;
            // Durable now, not merely dirty. `write_meta` only marks the pooled
            // frame; a session that opens a new store and exits without writing
            // anything logs nothing and checkpoints nothing, so that frame is
            // discarded and the meta page stays unwritten on disk. The next
            // session then cannot tell an initialized store from a new one.
            //
            // Checkpointing here is cheap precisely when it runs — the store is
            // empty, so there is at most a meta page and two empty roots to
            // flush.
            paged.checkpoint()?;
        }

        // A legacy meta page is transformed HERE, not lazily: recovery has
        // already replayed any old-layout images over the page, so rewriting it
        // now (same magic, valid empty extension and spill descriptor) and
        // checkpointing makes the current layout durable and truncates the log
        // that could reintroduce the old bytes. After this the store opens
        // strictly on every engine version — old engines ignore the extension,
        // current ones validate it — so `accept_legacy_meta` is needed exactly
        // once.
        if legacy_meta {
            paged.write_meta()?;
            paged.checkpoint()?;
        }

        Ok((paged, recovery))
    }

    /// Persist the tree roots into the meta page.
    ///
    /// Called whenever a root moves and at every checkpoint. Missing this is
    /// the failure that loses a whole tree: the next open would follow a root
    /// that is now an interior node of a larger tree, and silently see a
    /// fraction of the data.
    fn write_meta(&self) -> Result<()> {
        let mut guard = self.pool.get_mut(self.meta_page)?;
        let page = guard.bytes_mut();
        let mut header = crate::page::PageHeader::new(
            self.meta_page,
            crate::page::PageType::Heap,
            self.options.page_size,
        );
        header.encode(page);
        let body = &mut page[crate::page::PAGE_HEADER_BYTES..];
        body[0..8].copy_from_slice(&META_MAGIC);
        body[8..16].copy_from_slice(&self.index.root_page().to_le_bytes());
        body[16..24].copy_from_slice(&self.catalog.root_page().to_le_bytes());
        // Transaction watermarks travel with the roots. A checkpoint truncates
        // the log, so these are the only surviving evidence that older
        // transactions committed.
        body[24..32].copy_from_slice(&self.transactions.frozen_xid().to_le_bytes());
        body[32..40].copy_from_slice(&self.transactions.next_xid().to_le_bytes());
        // Abort exceptions travel with the watermark they qualify. They must
        // be durable before the checkpoint record that permits truncating the
        // WAL still holding the corresponding abort records.
        let exceptions = self.transactions.abort_exceptions();
        debug_assert!(
            exceptions.len() <= abort_exception_capacity(self.options.page_size),
            "the transaction table outgrew the meta-page exception region"
        );
        body[40..48].copy_from_slice(&(exceptions.len() as u64).to_le_bytes());
        for (index, xid) in exceptions.iter().enumerate() {
            let offset = META_FIXED_BYTES + index * 8;
            body[offset..offset + 8].copy_from_slice(&xid.to_le_bytes());
        }
        // Status-spill descriptor in the trailing fixed region. It becomes
        // durable only after the spill pages themselves are synced, which
        // `persist_status_spill` guarantees before this page is written.
        let (spill_head, spill_pages, spill_entries) = self.spill.descriptor();
        let usable = crate::page::usable_bytes(self.options.page_size);
        let descriptor = usable - META_SPILL_DESCRIPTOR_BYTES;
        body[descriptor..descriptor + 8].copy_from_slice(&spill_head.to_le_bytes());
        body[descriptor + 8..descriptor + 16].copy_from_slice(&spill_pages.to_le_bytes());
        body[descriptor + 16..descriptor + 24].copy_from_slice(&spill_entries.to_le_bytes());
        Ok(())
    }

    /// Record any root that moved during an operation.
    fn sync_roots(&self) -> Result<()> {
        if self.index.take_root_changed() || self.catalog.take_root_changed() {
            self.write_meta()?;
        }
        Ok(())
    }

    /// Inspect one version chain without resolving MVCC visibility.
    ///
    /// Samples are bounded even when a corrupt chain is enormous. Cycle
    /// detection remains exact: short, normal chains use stack storage and only
    /// pathological chains allocate a visited-locator map.
    pub fn inspect_version_chain(&self, key: &[u8]) -> Result<VersionChainInspection> {
        let _structure = self.structure.read();
        let encoded = self.index.get(key)?;
        self.inspect_version_chain_locked(
            encoded.as_deref(),
            MAX_VERSION_CHAIN,
            VERSION_CHAIN_SAMPLE_LIMIT,
        )
    }

    fn inspect_version_chain_locked(
        &self,
        encoded_head: Option<&[u8]>,
        limit: usize,
        sample_limit: usize,
    ) -> Result<VersionChainInspection> {
        let Some(encoded_head) = encoded_head else {
            return Ok(VersionChainInspection {
                head: None,
                versions_examined: 0,
                samples: Vec::new(),
                terminal: VersionChainTerminal::Empty,
            });
        };
        let Some(head) = TupleLocator::decode(encoded_head) else {
            return Ok(VersionChainInspection {
                head: None,
                versions_examined: 0,
                samples: Vec::new(),
                terminal: VersionChainTerminal::InvalidHead,
            });
        };

        let mut cursor = Some(head);
        let mut versions_examined = 0u64;
        let mut samples = Vec::with_capacity(sample_limit);
        let mut seen = ChainCycleDetector::default();
        for step in 0..limit {
            let Some(locator) = cursor else {
                return Ok(VersionChainInspection {
                    head: Some(head),
                    versions_examined,
                    samples,
                    terminal: VersionChainTerminal::End,
                });
            };
            if let Some(first_seen_step) = seen.observe(locator, step) {
                return Ok(VersionChainInspection {
                    head: Some(head),
                    versions_examined,
                    samples,
                    terminal: VersionChainTerminal::Cycle {
                        repeated: locator,
                        first_seen_step,
                        repeated_at_step: step,
                    },
                });
            }
            let stored = match self
                .heap
                .read_prefix(locator, crate::mvcc::VERSION_HEADER_BYTES)
            {
                Ok(stored) => stored,
                Err(
                    PageError::StaleLocator { .. }
                    | PageError::DeadSlot { .. }
                    | PageError::NoSuchSlot { .. }
                    | PageError::OutOfBounds { .. },
                ) => {
                    return Ok(VersionChainInspection {
                        head: Some(head),
                        versions_examined,
                        samples,
                        terminal: VersionChainTerminal::Vacuumed { locator },
                    });
                }
                Err(error) => return Err(error),
            };
            versions_examined = versions_examined.saturating_add(1);
            let Some((header, _)) = VersionHeader::decode(&stored) else {
                return Ok(VersionChainInspection {
                    head: Some(head),
                    versions_examined,
                    samples,
                    terminal: VersionChainTerminal::MalformedVersion { locator },
                });
            };
            if samples.len() < sample_limit {
                samples.push(VersionChainVersion {
                    locator,
                    xmin: header.xmin,
                    xmax: header.xmax,
                    previous: header.prev,
                });
            }
            cursor = header.prev;
        }

        let terminal = match cursor {
            Some(next) => VersionChainTerminal::LimitExceeded { next, limit },
            None => VersionChainTerminal::End,
        };
        Ok(VersionChainInspection {
            head: Some(head),
            versions_examined,
            samples,
            terminal,
        })
    }

    /// Verify the B-tree and every reachable MVCC chain.
    ///
    /// The scan uses bounded fault samples. Healthy short chains allocate no
    /// visited set, so the verification remains practical for very large
    /// stores while still detecting cycles exactly.
    pub fn verify_integrity(&self, max_fault_samples: usize) -> Result<PagedIntegrityReport> {
        let _structure = self.structure.read();
        let btree = self.index.verify()?;
        let mut version_chains = VersionChainVerifyReport::default();
        for entry in self.index.iter()? {
            let (key, encoded_head) = entry?;
            // The normal path samples nothing and therefore allocates nothing
            // per key. Only a bounded number of actual faults are re-inspected
            // for operator-facing samples.
            let inspection =
                self.inspect_version_chain_locked(Some(&encoded_head), MAX_VERSION_CHAIN, 0)?;
            version_chains.keys_examined = version_chains.keys_examined.saturating_add(1);
            version_chains.versions_examined = version_chains
                .versions_examined
                .saturating_add(inspection.versions_examined);
            match inspection.terminal {
                VersionChainTerminal::InvalidHead => {
                    version_chains.invalid_heads = version_chains.invalid_heads.saturating_add(1);
                }
                VersionChainTerminal::MalformedVersion { .. } => {
                    version_chains.malformed_versions =
                        version_chains.malformed_versions.saturating_add(1);
                }
                VersionChainTerminal::Cycle { .. } => {
                    version_chains.cycles = version_chains.cycles.saturating_add(1);
                }
                VersionChainTerminal::LimitExceeded { .. } => {
                    version_chains.limit_exceeded = version_chains.limit_exceeded.saturating_add(1);
                }
                VersionChainTerminal::Empty
                | VersionChainTerminal::End
                | VersionChainTerminal::Vacuumed { .. } => {}
            }
            if inspection.terminal.is_fault()
                && version_chains.fault_samples.len() < max_fault_samples
            {
                let inspection = self.inspect_version_chain_locked(
                    Some(&encoded_head),
                    MAX_VERSION_CHAIN,
                    VERSION_CHAIN_SAMPLE_LIMIT,
                )?;
                version_chains
                    .fault_samples
                    .push(VersionChainFaultSample { key, inspection });
            }
        }
        version_chains.valid = version_chains.invalid_heads == 0
            && version_chains.malformed_versions == 0
            && version_chains.cycles == 0
            && version_chains.limit_exceeded == 0;
        let valid = btree.valid && version_chains.valid;
        Ok(PagedIntegrityReport {
            btree,
            version_chains,
            valid,
        })
    }

    /// Verify one explicitly bounded and restartable slice of B+ tree
    /// structure. The structure lock covers only this finite step; callers can
    /// atomically checkpoint the returned strict cursor between calls.
    pub fn verify_btree_step(
        &self,
        cursor: BTreeVerifyCursor,
        limits: BTreeVerifyLimits,
    ) -> Result<BTreeVerifyStepReport> {
        let _structure = self.structure.read();
        self.index.verify_step(cursor, limits)
    }

    pub fn validate_new_btree_verify_limits(&self, limits: &BTreeVerifyLimits) -> Result<()> {
        self.index.validate_new_verify_limits(limits)
    }

    /// Verify reachable MVCC chains in one strictly bounded, resumable step.
    ///
    /// The inclusive cursor names the first key not yet examined. Each key is
    /// re-resolved under the structure read lock before its chain is inspected,
    /// so a cached range entry can never substitute an obsolete head. The lock
    /// is released between keys, preventing a PB-scale sweep from fencing
    /// writers for its complete duration. This is an online integrity sweep:
    /// keys inserted behind an already-published cursor belong to the next
    /// sweep rather than being silently attributed to this one.
    pub fn verify_version_chains_step(
        &self,
        cursor: VersionChainVerifyCursor,
        limits: VersionChainVerifyLimits,
    ) -> Result<VersionChainVerifyStepReport> {
        limits.validate()?;
        if cursor
            .next_key
            .as_ref()
            .is_some_and(|key| key.len() > limits.max_cursor_key_bytes)
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "MVCC verification cursor exceeds its {}-byte bound",
                    limits.max_cursor_key_bytes
                ),
            });
        }

        let started = Instant::now();
        let start_key = cursor.next_key.as_deref().unwrap_or_default();
        let mut entries = {
            let _structure = self.structure.read();
            self.index.range(start_key)?
        };
        let mut report = VersionChainVerifyStepReport::default();
        let maximum_chain_bytes = limits
            .max_versions_per_chain
            .saturating_mul(crate::mvcc::VERSION_HEADER_BYTES as u64);

        loop {
            let structure = self.structure.read();
            let Some(entry) = entries.next() else {
                drop(structure);
                return Ok(finish_version_chain_verify_step(
                    report,
                    started,
                    VersionChainVerifyStopReason::Complete,
                    None,
                ));
            };
            let (key, _) = entry?;
            if key.len() > limits.max_cursor_key_bytes {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason: format!(
                        "MVCC verification key is {} bytes, above the {}-byte cursor bound",
                        key.len(),
                        limits.max_cursor_key_bytes
                    ),
                });
            }

            let stop_reason = if report.version_chains.keys_examined >= limits.max_keys {
                Some(VersionChainVerifyStopReason::KeyLimit)
            } else if limits
                .max_versions
                .saturating_sub(report.version_chains.versions_examined)
                < limits.max_versions_per_chain
            {
                Some(VersionChainVerifyStopReason::VersionLimit)
            } else if limits.max_bytes.saturating_sub(report.bytes_examined) < maximum_chain_bytes {
                Some(VersionChainVerifyStopReason::ByteLimit)
            } else if elapsed_millis(started) >= limits.max_duration_millis {
                Some(VersionChainVerifyStopReason::TimeLimit)
            } else {
                None
            };
            if let Some(stop_reason) = stop_reason {
                drop(structure);
                return Ok(finish_version_chain_verify_step(
                    report,
                    started,
                    stop_reason,
                    Some(key),
                ));
            }

            // Re-resolve while holding the same lock as the chain walk. Range
            // leaves are copied one page at a time and may predate a concurrent
            // update; the current index head is the verification authority.
            let encoded_head = self.index.get(&key)?;
            let sample_limit =
                if report.version_chains.fault_samples.len() < limits.max_fault_samples {
                    VERSION_CHAIN_SAMPLE_LIMIT
                } else {
                    0
                };
            let inspection = self.inspect_version_chain_locked(
                encoded_head.as_deref(),
                limits.max_versions_per_chain as usize,
                sample_limit,
            )?;
            drop(structure);

            report.version_chains.keys_examined =
                report.version_chains.keys_examined.saturating_add(1);
            report.version_chains.versions_examined = report
                .version_chains
                .versions_examined
                .saturating_add(inspection.versions_examined);
            report.bytes_examined = report.bytes_examined.saturating_add(
                inspection
                    .versions_examined
                    .saturating_mul(crate::mvcc::VERSION_HEADER_BYTES as u64),
            );
            observe_version_chain_terminal(&mut report.version_chains, &inspection.terminal);

            if inspection.terminal.is_fault() {
                if report.version_chains.fault_samples.len() < limits.max_fault_samples {
                    let sample = VersionChainFaultSample { key, inspection };
                    let sample_bytes = serde_json::to_vec(&sample)
                        .map_err(|error| PageError::InvalidMaintenanceLimits {
                            reason: format!("cannot encode bounded MVCC fault sample: {error}"),
                        })?
                        .len() as u64;
                    if report.fault_sample_bytes.saturating_add(sample_bytes)
                        <= limits.max_fault_sample_bytes
                    {
                        report.fault_sample_bytes =
                            report.fault_sample_bytes.saturating_add(sample_bytes);
                        report.version_chains.fault_samples.push(sample);
                    } else {
                        report.fault_samples_dropped =
                            report.fault_samples_dropped.saturating_add(1);
                    }
                } else {
                    report.fault_samples_dropped = report.fault_samples_dropped.saturating_add(1);
                }
            }
        }
    }

    /// Replace one confirmed faulty chain with an authoritative value.
    ///
    /// This is an offline recovery primitive, not a normal write path. It
    /// refuses to run while any transaction is active, compares the exact head
    /// observed by the operator, re-diagnoses the chain under the writer lock,
    /// and only repairs cycles or limit exhaustion. The replacement is a fresh
    /// one-version chain, committed through WAL; the old chain becomes
    /// unreachable without rewriting any of its pages.
    pub fn replace_faulty_version_chain(
        &self,
        key: &[u8],
        expected_head: TupleLocator,
        value: &[u8],
    ) -> Result<VersionChainRepairReport> {
        let (transaction, replacement_head, inspection) = {
            let _guard = self.write_lock.lock();
            let _structure = self.structure.write();
            let active = self.transactions.in_progress();
            if !active.is_empty() {
                return Err(PageError::VersionChainRepairRefused {
                    reason: format!(
                        "{} transaction(s) are active; close all sessions and retry offline",
                        active.len()
                    ),
                });
            }

            let encoded_head = self.index.get(key)?;
            let found = encoded_head.as_deref().and_then(TupleLocator::decode);
            if found != Some(expected_head) {
                return Err(PageError::VersionChainChanged {
                    expected: expected_head,
                    found,
                });
            }
            let inspection =
                self.inspect_version_chain_locked(encoded_head.as_deref(), MAX_VERSION_CHAIN, 0)?;
            if !inspection.terminal.is_repairable() {
                return Err(PageError::VersionChainRepairRefused {
                    reason: format!(
                        "the current chain ends with {:?}, not a cycle or safety-limit fault",
                        inspection.terminal
                    ),
                });
            }

            let (transaction, _) = self.begin_transaction();
            self.pool.set_current_transaction(transaction);
            let header = VersionHeader::new(transaction);
            let mut tuple = Vec::with_capacity(crate::mvcc::VERSION_HEADER_BYTES + value.len());
            header.encode_into(&mut tuple);
            tuple.extend_from_slice(value);
            let replacement_head = match self.heap.insert(&tuple) {
                Ok(locator) => locator,
                Err(error) => {
                    let _ = self.abort(transaction);
                    return Err(error);
                }
            };
            if let Err(error) = self
                .index
                .insert(key, &replacement_head.encode())
                .and_then(|_| self.sync_roots())
            {
                let _ = self.abort(transaction);
                return Err(error);
            }
            (transaction, replacement_head, inspection)
        };

        if let Err(error) = self.commit(transaction) {
            let _ = self.abort(transaction);
            return Err(error);
        }
        // Make the repaired root and the transaction watermark independently
        // durable now; recovery must not depend on a later application write.
        self.checkpoint()?;
        Ok(VersionChainRepairReport {
            previous_head: expected_head,
            replacement_head,
            versions_examined: inspection.versions_examined,
            repaired_fault: inspection.terminal,
        })
    }

    /// DEBUG: the version chain for `key` as `(xmin, xmax)` pairs, newest first.
    pub fn debug_chain(&self, key: &[u8]) -> Vec<(u64, u64)> {
        self.inspect_version_chain(key)
            .map(|inspection| {
                inspection
                    .samples
                    .into_iter()
                    .map(|version| (version.xmin, version.xmax))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Hold the store's writer lock for the caller's whole multi-operation
    /// transaction.
    ///
    /// The per-operation locking inside `put`/`delete`/`commit` keeps each
    /// *operation* atomic, but two transactions' operations can still
    /// interleave between them — transaction A's `put` then B's `put` then
    /// both commits. For transactions that must be applied as a unit (a commit
    /// batch mirrored from the layer above, whose reads at the batch's own
    /// snapshot must contain the whole batch), interleaving is corruption:
    /// measured symptoms were spurious write conflicts between already-decided
    /// transactions, orphaned versions severed from their chain, and snapshots
    /// pinned to a transaction that did not contain its own batch's rows.
    ///
    /// The lock is reentrant, so the per-operation acquisitions inside the
    /// guarded region (including a `checkpoint` triggered by a commit crossing
    /// the WAL ceiling) re-enter rather than deadlock.
    pub fn write_guard(&self) -> ReentrantMutexGuard<'_, ()> {
        self.write_lock.lock()
    }

    /// Begin a transaction, returning its id and read snapshot.
    pub fn begin_transaction(&self) -> (Xid, Snapshot) {
        let (xid, snapshot) = self.transactions.begin();
        self.snapshots.lock().insert(xid, snapshot.clone());
        (xid, snapshot)
    }

    /// Begin a transaction, returning just its id. The snapshot is recoverable
    /// via [`Self::transactions`].
    pub fn begin(&self) -> Xid {
        let (xid, _) = self.begin_transaction();
        let _ = self.next_transaction.fetch_add(1, Ordering::AcqRel);
        xid
    }

    /// The snapshot a live transaction began with.
    fn snapshot_of(&self, xid: Xid) -> Snapshot {
        self.snapshots
            .lock()
            .get(&xid)
            .cloned()
            .unwrap_or_else(|| self.latest_snapshot())
    }

    /// A snapshot that sees everything committed so far.
    pub fn latest_snapshot(&self) -> Snapshot {
        self.transactions.latest_snapshot()
    }

    pub fn transactions(&self) -> &TransactionTable {
        &self.transactions
    }

    /// Store a value under `key` as a new version.
    ///
    /// The previous version is retained and marked deleted by this transaction,
    /// so a concurrent reader on an older snapshot keeps seeing it. That
    /// retention is what makes rollback free: if this transaction never commits,
    /// the old version is still the visible one.
    pub fn put(&self, transaction: Xid, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_returning_locator(transaction, key, value)
            .map(|_| ())
    }

    /// [`Self::put`], returning the heap locator of the version it wrote.
    ///
    /// The locator is the new CHAIN HEAD at the moment of the write — exactly
    /// what a secondary-index entry records as a TID-style hint so readers can
    /// try the heap directly and skip the key B-tree descent.
    pub fn put_returning_locator(
        &self,
        transaction: Xid,
        key: &[u8],
        value: &[u8],
    ) -> Result<TupleLocator> {
        let _guard = self.write_lock.lock();
        let _structure = self.structure.write();
        self.pool.set_current_transaction(transaction);
        let previous = self.index.get(key)?.and_then(|b| TupleLocator::decode(&b));
        if let Some(previous) = previous {
            self.check_write_conflict(transaction, previous, key)?;
        }

        let mut header = VersionHeader::new(transaction);
        header.prev = previous;
        let mut tuple = Vec::with_capacity(crate::mvcc::VERSION_HEADER_BYTES + value.len());
        header.encode_into(&mut tuple);
        tuple.extend_from_slice(value);

        let locator = self.heap.insert(&tuple)?;

        // Mark the superseded version as deleted by this transaction. Done AFTER
        // the new version exists, so a crash in between leaves the old version
        // live rather than leaving the key with no visible version at all.
        if let Some(previous) = previous {
            self.mark_deleted(previous, transaction)?;
        }

        self.index.insert(key, &locator.encode())?;
        self.sync_roots()?;
        Ok(locator)
    }

    /// Refuse a write to a row a concurrent transaction has already written.
    ///
    /// First-updater-wins. Without this check the second writer simply
    /// overwrites the first and one committed update vanishes with no error —
    /// the classic lost update, and the reason snapshot isolation alone is not
    /// enough for concurrent writers.
    ///
    /// A row conflicts when its newest version was written by a transaction that
    /// is still in progress (someone is mid-write), or that committed at or
    /// after this transaction's snapshot began (we would be overwriting a change
    /// we cannot even see).
    fn check_write_conflict(
        &self,
        transaction: Xid,
        locator: TupleLocator,
        key: &[u8],
    ) -> Result<()> {
        let stored = match self.heap.get(locator) {
            Ok(stored) => stored,
            // Nothing readable there: no established writer to conflict with.
            Err(_) => return Ok(()),
        };
        let Some((header, _)) = VersionHeader::decode(&stored) else {
            return Ok(());
        };

        let snapshot = self.snapshot_of(transaction);
        for (field, other) in [("xmin", header.xmin), ("xmax", header.xmax)] {
            if self.transactions.is_concurrent(other, &snapshot) {
                // An id the allocator never issued cannot belong to a live
                // transaction: it is header corruption, and without repair
                // the row is unwritable forever — surface it as such, with
                // the row named, instead of an eternal anonymous conflict.
                if other >= self.transactions.next_xid() {
                    return Err(PageError::CorruptFutureTransactionId {
                        key: String::from_utf8_lossy(key).into_owned(),
                        field,
                        xid: other,
                        next_xid: self.transactions.next_xid(),
                    });
                }
                return Err(PageError::WriteConflict {
                    transaction,
                    conflicting: other,
                    field,
                    key: String::from_utf8_lossy(key).into_owned(),
                });
            }
        }
        Ok(())
    }

    /// Stamp `xmax` on an existing version.
    fn mark_deleted(&self, locator: TupleLocator, transaction: Xid) -> Result<()> {
        let mut current = match self.heap.get(locator) {
            Ok(current) => current,
            // The superseded version was vacuumed away — and its slot may
            // already be reused (StaleLocator) or still empty (DeadSlot).
            // There is nothing to mark: readers already treat a broken prev
            // link as end-of-chain (see `walk_chain_as_of`), which is the
            // vacuum contract. Erroring here made the first WRITE after a
            // vacuum fail on any key whose old head was reclaimed (B11).
            Err(PageError::StaleLocator { .. })
            | Err(PageError::DeadSlot { .. })
            | Err(PageError::NoSuchSlot { .. })
            | Err(PageError::OutOfBounds { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        VersionHeader::patch_xmax(&mut current, transaction);
        self.heap.update(locator, &current)?;
        Ok(())
    }

    /// Read the version of `key` visible to the latest committed state.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_as_of(&self.latest_snapshot(), key)
    }

    /// Read the version of `key` visible to `snapshot`.
    ///
    /// Walks the version chain newest-first and returns the first visible
    /// version. Newest-first is deliberate: a reader at the current snapshot
    /// stops immediately, and only readers holding older snapshots pay to walk.
    pub fn get_as_of(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _structure = self.structure.read();
        self.get_as_of_locked(snapshot, key)
    }

    /// [`Self::get_as_of`] without taking the structure lock — for callers that
    /// already hold it exclusively (`delete`, `vacuum`). Calling this without
    /// holding either side races concurrent splits; that is the bug the lock
    /// exists to prevent.
    fn get_as_of_locked(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(bytes) = self.index.get(key)? else {
            return Ok(None);
        };
        self.walk_chain_as_of(snapshot, TupleLocator::decode(&bytes))
    }

    /// Walk a version chain from its newest locator, returning the first
    /// version visible to `snapshot`.
    ///
    /// Split out from [`Self::get_as_of_locked`] so scans can start from the
    /// locator the index cursor ALREADY yields. Before this, every scan step
    /// threw that locator away and re-descended the whole tree via
    /// `index.get` — a full descent plus a structure-lock acquisition per row,
    /// doubling the tree work of every scan in the system.
    ///
    /// Callers must hold the structure lock (either side): the walk itself
    /// never touches the tree, but the LOCATOR's validity does — vacuum holds
    /// the write side while it relocates tuples, so holding the read side is
    /// what keeps a just-yielded locator from going stale mid-walk.
    fn walk_chain_as_of(
        &self,
        snapshot: &Snapshot,
        cursor: Option<TupleLocator>,
    ) -> Result<Option<Vec<u8>>> {
        self.walk_chain_as_of_bounded(snapshot, cursor, MAX_VERSION_CHAIN)
    }

    fn visible_value_prefix_as_of(
        &self,
        snapshot: &Snapshot,
        mut cursor: Option<TupleLocator>,
        value_prefix_bytes: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(head) = cursor else {
            return Ok(None);
        };
        let mut seen = ChainCycleDetector::default();
        for step in 0..MAX_VERSION_CHAIN {
            let Some(locator) = cursor else {
                return Ok(None);
            };
            if let Some(first_seen_step) = seen.observe(locator, step) {
                return Err(PageError::VersionChainCycle {
                    head,
                    repeated: locator,
                    first_seen_step,
                    steps: step,
                });
            }
            let stored = match self.heap.read_prefix(
                locator,
                crate::mvcc::VERSION_HEADER_BYTES.saturating_add(value_prefix_bytes),
            ) {
                Ok(stored) => stored,
                Err(PageError::StaleLocator { .. })
                | Err(PageError::DeadSlot { .. })
                | Err(PageError::NoSuchSlot { .. })
                | Err(PageError::OutOfBounds { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let Some((header, value)) = VersionHeader::decode(&stored) else {
                return Ok(None);
            };
            if self.transactions.is_visible(&header, snapshot) {
                return Ok(Some(value.to_vec()));
            }
            cursor = header.prev;
        }
        match cursor {
            Some(next) => Err(PageError::VersionChainLimitExceeded {
                head,
                next,
                limit: MAX_VERSION_CHAIN,
            }),
            None => Ok(None),
        }
    }

    fn walk_chain_as_of_bounded(
        &self,
        snapshot: &Snapshot,
        mut cursor: Option<TupleLocator>,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(head) = cursor else {
            return Ok(None);
        };
        let mut seen = ChainCycleDetector::default();
        for step in 0..limit {
            let Some(locator) = cursor else {
                return Ok(None);
            };
            if let Some(first_seen_step) = seen.observe(locator, step) {
                return Err(PageError::VersionChainCycle {
                    head,
                    repeated: locator,
                    first_seen_step,
                    steps: step,
                });
            }
            let stored = match self.heap.get(locator) {
                Ok(stored) => stored,
                // A chain link the heap rejects means history has been vacuumed
                // away beneath this snapshot. There is nothing older to see.
                Err(PageError::StaleLocator { .. })
                | Err(PageError::DeadSlot { .. })
                // A vacuumed trailing slot can vanish entirely when the slot
                // directory shrinks — same meaning as a dead slot for a
                // chain link: nothing older to see.
                | Err(PageError::NoSuchSlot { .. })
                // The whole PAGE can vanish when a fully-dead page is freed
                // and the file later truncated behind it.
                | Err(PageError::OutOfBounds { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let Some((header, value)) = VersionHeader::decode(&stored) else {
                return Ok(None);
            };
            if self.transactions.is_visible(&header, snapshot) {
                return Ok(Some(value.to_vec()));
            }
            cursor = header.prev;
        }
        match cursor {
            Some(next) => Err(PageError::VersionChainLimitExceeded { head, next, limit }),
            None => Ok(None),
        }
    }

    /// Read a version by a TID-style locator HINT instead of a key descent.
    ///
    /// The hint is whatever some index entry recorded as the chain head at the
    /// time it was written; nothing guarantees it still is. Every way the hint
    /// can be wrong therefore maps to [`HintedRead::Fallback`], never to a
    /// fabricated result — the caller retries through the authoritative
    /// `get_as_of` key descent:
    ///
    /// - the slot was vacuumed, freed, or the page truncated (heap errors);
    /// - the tuple no longer decodes as a version;
    /// - the version's `xmax` is VISIBLE to `snapshot` — for this snapshot the
    ///   row was superseded or deleted, and the newer version (if one exists)
    ///   is reachable only from the key index, which the hint cannot see;
    /// - the chain from the hint runs out, cycles, or exceeds the walk limit.
    ///
    /// A version whose `xmin` is not yet visible walks `prev` exactly like the
    /// normal newest-first read.
    ///
    /// `expected_xmin` is the transaction that wrote both the hinted version
    /// and the hint itself, recorded alongside the locator. It is the identity
    /// check: the heap is shared, so a vacuumed slot can be REUSED by a
    /// different key's version — but never by a tuple with the original
    /// `xmin` (a slot is only reusable after its tuple is dead and reclaimed,
    /// which cannot happen to the writer's own version while that writer is
    /// the one whose hint we hold; any later reuse carries a later `xmin`).
    /// An `xmin` mismatch is a stale hint, never an error.
    pub fn get_as_of_hinted(
        &self,
        snapshot: &Snapshot,
        hint: TupleLocator,
        expected_xmin: Xid,
    ) -> Result<HintedRead> {
        // Structure read lock: vacuum relocates tuples only under the write
        // side, so this keeps the hinted locator (and every prev link we
        // follow) from moving mid-walk.
        let _structure = self.structure.read();
        let mut cursor = Some(hint);
        let mut seen = ChainCycleDetector::default();
        for step in 0..MAX_VERSION_CHAIN {
            let Some(locator) = cursor else {
                return Ok(HintedRead::Fallback);
            };
            if seen.observe(locator, step).is_some() {
                return Ok(HintedRead::Fallback);
            }
            let stored = match self.heap.get(locator) {
                Ok(stored) => stored,
                Err(PageError::StaleLocator { .. })
                | Err(PageError::DeadSlot { .. })
                | Err(PageError::NoSuchSlot { .. })
                | Err(PageError::OutOfBounds { .. }) => return Ok(HintedRead::Fallback),
                Err(error) => return Err(error),
            };
            let Some((header, value)) = VersionHeader::decode(&stored) else {
                return Ok(HintedRead::Fallback);
            };
            if step == 0 && header.xmin != expected_xmin {
                // Not the tuple the hint was written for: the slot was
                // vacuumed and reused. (Only the hinted tuple is checked —
                // prev links of a VALIDATED tuple are that row's own chain.)
                return Ok(HintedRead::Fallback);
            }
            if self.transactions.is_visible(&header, snapshot) {
                return Ok(HintedRead::Hit(value.to_vec()));
            }
            if self.transactions.xid_visible(header.xmin, snapshot) {
                // xmin visible but the version is not: its xmax is visible.
                // Superseded (or deleted) for this snapshot — only the key
                // index knows what replaced it.
                return Ok(HintedRead::Fallback);
            }
            cursor = header.prev;
        }
        Ok(HintedRead::Fallback)
    }

    /// Delete `key` by marking its newest version deleted by this transaction.
    ///
    /// The version and the index entry both stay in place. Removing the index
    /// entry outright would hide the row from readers on older snapshots that are
    /// still entitled to see it — a snapshot-isolation violation dressed up as a
    /// delete.
    pub fn delete(&self, transaction: Xid, key: &[u8]) -> Result<bool> {
        let _guard = self.write_lock.lock();
        let _structure = self.structure.write();
        self.pool.set_current_transaction(transaction);
        let Some(bytes) = self.index.get(key)? else {
            return Ok(false);
        };
        let Some(locator) = TupleLocator::decode(&bytes) else {
            return Ok(false);
        };
        // Already invisible to this transaction? Then there is nothing to delete.
        // `_locked`: `delete` holds the structure lock exclusively already.
        if self
            .get_as_of_locked(&self.snapshot_of(transaction), key)?
            .is_none()
        {
            return Ok(false);
        }
        self.check_write_conflict(transaction, locator, key)?;
        self.mark_deleted(locator, transaction)?;
        self.sync_roots()?;
        Ok(true)
    }

    /// Make a transaction's writes durable.
    pub fn commit(&self, transaction: Xid) -> Result<()> {
        // Log page images once per transaction rather than once per operation.
        //
        // A page stays dirty across a whole transaction, so logging after every
        // put re-logs pages that later puts touch again — with a 2,000-row
        // batch, a hot index page was logged 2,000 times. Measured at 24.5 GB of
        // WAL for 233 MB of data; logging at commit collapses that to one record
        // per page per transaction.
        //
        // Safe because a page evicted mid-transaction is logged by the writeback
        // barrier on its way out, so nothing reaches disk unlogged either way.
        {
            let _guard = self.write_lock.lock();
            let free_adoption = self.prepare_free_adoption(transaction, &[])?;
            self.log_dirty_pages(transaction)?;
            self.wal.commit(transaction)?;
            if let Some((head, count)) = free_adoption {
                self.pool.flush_all()?;
                self.pool.store().adopt_free_pages(head, count)?;
            }
            // Only after the commit record is durable: marking it committed first
            // would make its rows visible to readers before a crash could still
            // lose them.
            self.transactions.commit(transaction);
            self.snapshots.lock().remove(&transaction);
        }
        // Outside the guard: `checkpoint` takes the same lock, and it is not
        // reentrant. Checkpointing is also the one write-path step with no
        // reason to hold up other committers while it runs.
        let wal_bytes = self.wal.size_bytes();
        if wal_bytes > self.options.wal_max_bytes {
            let mut cursor = self.automatic_checkpoint_cursor.lock();
            if wal_bytes
                > self
                    .options
                    .wal_max_bytes
                    .saturating_mul(WAL_BACKPRESSURE_FACTOR)
            {
                // Bounded phases cannot outrun a writer that dirties more than
                // one step's envelope per commit: Finalize demands a clean
                // pool before it truncates and, unlike Drain, never escalates,
                // so a sustained bulk load parks the state machine there and
                // the log grows without bound in a process with no background
                // supervisor (observed: 190 GB against a 1 GiB trigger). Past
                // this factor the committing thread pays for the full flush
                // and truncation; below it the trigger stays soft.
                self.checkpoint()?;
                *cursor = PagedCheckpointCursor::default();
            } else {
                // Never turn the threshold-crossing commit into an all-buffer
                // flush. Advance exactly one bounded phase; later commits or
                // the durable supervisor continue from the inclusive cursor.
                let report = self.checkpoint_step(*cursor, PagedCheckpointLimits::default())?;
                *cursor = if report.complete {
                    PagedCheckpointCursor::default()
                } else {
                    report.next_cursor
                };
            }
        }
        Ok(())
    }

    /// Discard a transaction's writes.
    ///
    /// Only meaningful before a crash: recovery already refuses to apply page
    /// images from uncommitted transactions. In-memory pages are *not* rolled
    /// back here, which is precisely why concurrency control is a later phase
    /// rather than something quietly half-done.
    pub fn abort(&self, transaction: Xid) -> Result<()> {
        // Serialize the abort record with checkpoint publication/reset. An
        // abort does not mutate a page, but it does append to the same WAL a
        // checkpoint may truncate.
        let _guard = self.write_lock.lock();
        self.wal.abort(transaction)?;
        self.transactions.abort(transaction);
        self.snapshots.lock().remove(&transaction);
        Ok(())
    }

    /// Persist every resident terminal outcome in the disk-backed status
    /// spill, sync it, and evict it from the transaction table.
    ///
    /// Runs under the writer gate before the meta page records the spill
    /// descriptor, so the descriptor never names a page whose contents are
    /// not already durable. After this, no unfrozen commit remains whose
    /// only evidence is the WAL — which is what releases truncation while a
    /// long-running transaction pins the watermark. A failure leaves the
    /// resident table and the intact log untouched.
    fn persist_status_spill(&self) -> Result<()> {
        let outcomes = self.transactions.terminal_outcomes();
        let frozen_xid = self.transactions.frozen_xid();
        self.spill
            .persist(&self.pool, &self.wal, frozen_xid, outcomes)?;
        self.transactions.evict_terminal_outcomes();
        Ok(())
    }

    /// Flush pages, bound the log, and record a redo point.
    /// Engage a backup pin. While any pin is held, checkpoints keep running
    /// (dirty pages drain, recovery stays bounded going forward) but WAL
    /// truncation and page-file tail reclaim are deferred, and sealed WAL
    /// segments are retained — the invariants an online file copy needs.
    pub fn begin_backup_pin(&self) {
        self.backup_pins.fetch_add(1, Ordering::AcqRel);
    }

    /// Release one [`Self::begin_backup_pin`]. Truncation resumes at the
    /// next checkpoint once every pin is released.
    pub fn end_backup_pin(&self) {
        self.backup_pins.fetch_sub(1, Ordering::AcqRel);
    }

    fn backup_pinned(&self) -> bool {
        self.backup_pins.load(Ordering::Acquire) != 0
    }

    /// See [`crate::manager::PageStore::begin_extent_migration`].
    pub fn begin_extent_migration(&self, extent_bytes: u64) -> Result<()> {
        self.pool.store().begin_extent_migration(extent_bytes)
    }

    /// See [`crate::manager::PageStore::advance_extent_migration`].
    pub fn advance_extent_migration(
        &self,
        max_pages: u64,
    ) -> Result<crate::manager::ExtentMigrationReport> {
        self.pool.store().advance_extent_migration(max_pages)
    }

    /// Sealed (immutable) WAL segment paths, ascending — WITHOUT sealing.
    pub fn sealed_wal_segments(&self) -> Vec<std::path::PathBuf> {
        self.wal.sealed_segment_paths()
    }

    /// See [`crate::wal::Wal::enable_archive_retention`].
    pub fn enable_wal_archive_retention(&self) {
        self.wal.enable_archive_retention();
    }

    /// See [`crate::wal::Wal::mark_archived_through`].
    pub fn mark_wal_archived_through(&self, sequence: u64) {
        self.wal.mark_archived_through(sequence);
    }

    /// Seal the active WAL file into an immutable segment, returning the
    /// sealed chain's paths (ascending). The online-backup consistency cut:
    /// called AFTER the page files are streamed, the sealed chain covers
    /// every page write the copy could have observed. Errors on stores whose
    /// WAL segmentation is disabled (legacy monolithic layout).
    pub fn seal_wal_for_backup(&self) -> Result<Vec<std::path::PathBuf>> {
        self.wal.seal_active()?;
        Ok(self.wal.sealed_segment_paths())
    }

    /// REPAIR: make every transaction id below `beyond` resolve as decided,
    /// healing rows whose version headers carry ids the allocator never
    /// issued (a permanently-unwritable-row corruption observed on a
    /// hand-rebuilt store). Sequence: raise the allocator so nothing below
    /// `beyond` is ever issued again, absorb the genuinely-used id range
    /// into the watermark through the ordinary freeze, jump the watermark
    /// over the never-issued gap, and checkpoint so the repaired watermarks
    /// are durable before returning. Refused while any transaction below
    /// `beyond` is still in progress — finish or abort those first and
    /// re-run.
    pub fn advance_transaction_floor(&self, beyond: Xid) -> Result<(Xid, Xid)> {
        {
            let _guard = self.write_lock.lock();
            let allocated_before = self.transactions.next_xid();
            let oldest_active = self.transactions.oldest_in_progress_or_next();
            // Freeze the range that was genuinely allocated so every real
            // outcome is absorbed (or preserved as an abort exception)
            // before the watermark jumps the never-issued gap. Freezing is
            // ordinary maintenance; the allocator is only raised once every
            // refusal condition has cleared — a refused repair must leave
            // NOTHING mutated (the first field deployment raised the
            // allocator and then refused, silently degrading the
            // future-stamp corruption detection for the poisoned row).
            self.transactions
                .freeze_below(allocated_before.min(oldest_active))?;
            if let Some(obstacle) = self.transactions.watermark_jump_obstacle(beyond) {
                return Err(PageError::TransactionFloorRefused {
                    target: beyond,
                    reason: obstacle,
                });
            }
            self.transactions.ensure_next_xid_at_least(beyond);
            self.transactions.jump_frozen_to(beyond)?;
        }
        // Durability: the watermarks live on the meta page, which the
        // checkpoint persists; without it a crash would replay a WAL that
        // has never heard of the repaired floor.
        self.checkpoint()?;
        Ok((self.transactions.frozen_xid(), self.transactions.next_xid()))
    }

    /// SURGICAL repair for one row whose version header carries a corrupt
    /// transaction stamp: replace exactly `field == expected` with the
    /// structural committed marker (id 0), in place. Narrower than the
    /// transaction-floor advance — nothing else in the store changes — and
    /// the expected-value guard makes it idempotent and mis-target-proof:
    /// any mismatch refuses, a marker already at zero reports AlreadyClean.
    /// In-place header patching follows the established `mark_deleted`
    /// pattern (same-size update keeps the locator; the page dirties through
    /// the ordinary pool/WAL path). Callers checkpoint for durability.
    pub fn repair_version_header_stamp(
        &self,
        key: &[u8],
        field: HeaderStampField,
        expected: Xid,
    ) -> Result<HeaderRepair> {
        if expected == 0 {
            return Err(PageError::HeaderRepairRefused {
                key: String::from_utf8_lossy(key).into_owned(),
                field: field.name(),
                expected,
                found: 0,
            });
        }
        let _guard = self.write_lock.lock();
        let _structure = self.structure.write();
        let Some(locator) = self.index.get(key)?.and_then(|b| TupleLocator::decode(&b)) else {
            return Err(PageError::HeaderRepairRefused {
                key: String::from_utf8_lossy(key).into_owned(),
                field: field.name(),
                expected,
                found: 0,
            });
        };
        let mut stored = self.heap.get(locator)?;
        let Some((header, _)) = VersionHeader::decode(&stored) else {
            return Err(PageError::HeaderRepairRefused {
                key: String::from_utf8_lossy(key).into_owned(),
                field: field.name(),
                expected,
                found: 0,
            });
        };
        let found = match field {
            HeaderStampField::Xmin => header.xmin,
            HeaderStampField::Xmax => header.xmax,
        };
        if found == 0 {
            return Ok(HeaderRepair::AlreadyClean);
        }
        if found != expected {
            return Err(PageError::HeaderRepairRefused {
                key: String::from_utf8_lossy(key).into_owned(),
                field: field.name(),
                expected,
                found,
            });
        }
        match field {
            HeaderStampField::Xmin => stored[0..8].copy_from_slice(&0u64.to_le_bytes()),
            HeaderStampField::Xmax => VersionHeader::patch_xmax(&mut stored, 0),
        }
        self.heap.update(locator, &stored)?;
        self.sync_roots()?;
        Ok(HeaderRepair::Repaired)
    }

    pub fn checkpoint(&self) -> Result<()> {
        // Same lock as the writers: a checkpoint rewrites the meta page and
        // flushes frames, so running it alongside a `put` mid-split would
        // checkpoint a half-modified tree.
        let _guard = self.write_lock.lock();
        // Freeze first, so the watermark the meta page records is as advanced as
        // it can safely be. Freezing stops at the oldest still-running
        // transaction, so an in-flight writer is never frozen into visibility.
        let oldest_active = self.transactions.oldest_in_progress_or_next();
        self.transactions.freeze_below(oldest_active)?;

        // Spill what the watermark cannot absorb, so the log can truncate
        // even while a long-running transaction pins it.
        self.persist_status_spill()?;

        // The meta page is written first: the checkpoint makes it durable, so it
        // must already hold the current tree roots and watermarks.
        self.write_meta()?;

        // Truncating the log discards every commit record in it. That is only
        // safe once every unfrozen commit is proven somewhere durable: the
        // meta page's `frozen_xid` for transactions the watermark absorbed,
        // the status spill for the rest. `persist_status_spill` ran above, so
        // the gate that follows is expected to pass; it remains because
        // truncating anyway loses the sole evidence those transactions
        // committed, and their rows return invisible: in the heap, reachable
        // through the index, and judged uncommitted by every reader. Measured
        // before this check: with six writers and a checkpointer running
        // alongside them, 5-8 of 900 committed rows vanished per run, always the
        // tail each writer committed while another transaction was open.
        //
        // If the gate ever fails, flushing still happened, so the checkpoint
        // did its other job; only the truncation waits, in the same way
        // PostgreSQL cannot recycle WAL past its oldest running transaction.
        // A held backup pin also defers truncation: an online backup is
        // streaming the log chain and page tail, and discarding either
        // underneath it would corrupt the archive. Dirty pages still drain.
        if self.transactions.has_unfrozen_commits() || self.backup_pinned() {
            self.checkpointer.checkpoint(self.meta_page)?;
        } else {
            let checkpoint = self.checkpointer.checkpoint_and_truncate(self.meta_page)?;
            // The log is empty, so no replay can re-extend the file: give the
            // trailing run of free pages back to the filesystem. Stale
            // locators into the dropped range read as OutOfBounds — the same
            // end-of-chain answer a vacuumed slot gives.
            if checkpoint.wal_truncated {
                self.pool.store().truncate_trailing_free_pages()?;
            }
        }
        // The checkpoint above flushed the meta page, so the spill descriptor
        // naming the current chain is now durable. Only now may the pages the
        // last persist retired be returned to the free list.
        self.spill.reclaim_retired(self.meta_page, &self.pool)?;
        self.finalize_structural_frees()?;
        Ok(())
    }

    /// Advance one bounded, restartable checkpoint phase.
    ///
    /// Dirty pages are drained online first. Freeze work and final publication
    /// take the writer gate only for one explicitly bounded step. A crash before
    /// the caller durably records `next_cursor` safely repeats the inclusive
    /// phase: page after-images and checkpoint publication are idempotent.
    pub fn checkpoint_step(
        &self,
        cursor: PagedCheckpointCursor,
        limits: PagedCheckpointLimits,
    ) -> Result<PagedCheckpointStepReport> {
        cursor.validate()?;
        limits.validate(self.options.page_size)?;
        if cursor.phase == PagedCheckpointPhase::Complete {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "completed checkpoint operation cannot be advanced".to_string(),
            });
        }

        let report = match cursor.phase {
            PagedCheckpointPhase::Drain => {
                let writeback = self
                    .pool
                    .writeback_step(cursor.writeback, limits.writeback)?;
                let drained = writeback.complete && writeback.dirty_pages_remaining == 0;
                let drain_passes = if writeback.complete && !drained {
                    cursor.drain_passes.saturating_add(1)
                } else {
                    cursor.drain_passes
                };
                // Escalate after bounded non-converging passes: concurrent
                // writers redirtying the working set must not park the
                // checkpoint — and WAL truncation — in Drain forever.
                let escalate = drain_passes >= DRAIN_ESCALATION_PASSES;
                let next_cursor = if drained || escalate {
                    PagedCheckpointCursor {
                        phase: PagedCheckpointPhase::Freeze,
                        writeback: WritebackCursor::default(),
                        drain_passes: 0,
                    }
                } else {
                    PagedCheckpointCursor {
                        phase: PagedCheckpointPhase::Drain,
                        writeback: if writeback.complete {
                            WritebackCursor::default()
                        } else {
                            writeback.next_cursor
                        },
                        drain_passes,
                    }
                };
                PagedCheckpointStepReport {
                    phase_before: cursor.phase,
                    next_cursor,
                    writeback: Some(writeback),
                    transactions_frozen: 0,
                    freeze_blocked: false,
                    abort_exceptions_recorded: 0,
                    freeze_exception_capacity_full: false,
                    dirty_pages_remaining: writeback.dirty_pages_remaining,
                    checkpoint: None,
                    tail_reclaim: None,
                    complete: false,
                    stop_reason: if drained {
                        PagedCheckpointStopReason::PhaseBoundary
                    } else {
                        PagedCheckpointStopReason::Writeback
                    },
                }
            }
            PagedCheckpointPhase::Freeze => {
                let _guard = self.write_lock.lock();
                let oldest_active = self.transactions.oldest_in_progress_or_next();
                let max_xids = usize::try_from(limits.max_freeze_xids).map_err(|_| {
                    PageError::InvalidMaintenanceLimits {
                        reason: "checkpoint freeze bound exceeds this platform's address space"
                            .to_string(),
                    }
                })?;
                let freeze = self
                    .transactions
                    .freeze_below_bounded(oldest_active, max_xids)?;
                let next_cursor = PagedCheckpointCursor {
                    phase: if freeze.limit_reached {
                        PagedCheckpointPhase::Freeze
                    } else {
                        PagedCheckpointPhase::Finalize
                    },
                    writeback: WritebackCursor::default(),
                    drain_passes: 0,
                };
                PagedCheckpointStepReport {
                    phase_before: cursor.phase,
                    next_cursor,
                    writeback: None,
                    transactions_frozen: freeze.frozen as u64,
                    freeze_blocked: freeze.blocked,
                    abort_exceptions_recorded: freeze.exceptions_recorded as u64,
                    freeze_exception_capacity_full: freeze.exception_capacity_full,
                    dirty_pages_remaining: self.pool.dirty_page_count(),
                    checkpoint: None,
                    tail_reclaim: None,
                    complete: false,
                    stop_reason: if freeze.limit_reached {
                        PagedCheckpointStopReason::FreezeLimit
                    } else {
                        PagedCheckpointStopReason::PhaseBoundary
                    },
                }
            }
            PagedCheckpointPhase::Finalize => {
                let _guard = self.write_lock.lock();
                // Recheck the watermark under the same fence used for final
                // publication. Commits between the Freeze and Finalize ticks
                // must either be frozen now or leave the WAL untruncated.
                let oldest_active = self.transactions.oldest_in_progress_or_next();
                let max_xids = usize::try_from(limits.max_freeze_xids).map_err(|_| {
                    PageError::InvalidMaintenanceLimits {
                        reason: "checkpoint freeze bound exceeds this platform's address space"
                            .to_string(),
                    }
                })?;
                let freeze = self
                    .transactions
                    .freeze_below_bounded(oldest_active, max_xids)?;
                if freeze.limit_reached {
                    PagedCheckpointStepReport {
                        phase_before: cursor.phase,
                        next_cursor: PagedCheckpointCursor {
                            phase: PagedCheckpointPhase::Freeze,
                            writeback: WritebackCursor::default(),
                            drain_passes: 0,
                        },
                        writeback: None,
                        transactions_frozen: freeze.frozen as u64,
                        freeze_blocked: freeze.blocked,
                        abort_exceptions_recorded: freeze.exceptions_recorded as u64,
                        freeze_exception_capacity_full: freeze.exception_capacity_full,
                        dirty_pages_remaining: self.pool.dirty_page_count(),
                        checkpoint: None,
                        tail_reclaim: None,
                        complete: false,
                        stop_reason: PagedCheckpointStopReason::FreezeLimit,
                    }
                } else if self.pool.dirty_page_count() != 0 {
                    let writeback = self
                        .pool
                        .writeback_step(cursor.writeback, limits.writeback)?;
                    PagedCheckpointStepReport {
                        phase_before: cursor.phase,
                        next_cursor: PagedCheckpointCursor {
                            phase: PagedCheckpointPhase::Finalize,
                            writeback: if writeback.complete {
                                WritebackCursor::default()
                            } else {
                                writeback.next_cursor
                            },
                            drain_passes: 0,
                        },
                        writeback: Some(writeback),
                        transactions_frozen: freeze.frozen as u64,
                        freeze_blocked: freeze.blocked,
                        abort_exceptions_recorded: freeze.exceptions_recorded as u64,
                        freeze_exception_capacity_full: freeze.exception_capacity_full,
                        dirty_pages_remaining: writeback.dirty_pages_remaining,
                        checkpoint: None,
                        tail_reclaim: None,
                        complete: false,
                        stop_reason: PagedCheckpointStopReason::Writeback,
                    }
                } else {
                    // Spill what the watermark cannot absorb before the meta
                    // page names it, exactly as the one-shot checkpoint does.
                    self.persist_status_spill()?;
                    // Stage the exact roots and visibility watermark only after
                    // every preceding dirty page is durable. With the writer
                    // gate held, this creates exactly one new dirty meta page.
                    self.write_meta()?;
                    let writeback = self
                        .pool
                        .writeback_step(WritebackCursor::default(), limits.writeback)?;
                    if writeback.dirty_pages_remaining != 0 {
                        PagedCheckpointStepReport {
                            phase_before: cursor.phase,
                            next_cursor: PagedCheckpointCursor {
                                phase: PagedCheckpointPhase::Finalize,
                                writeback: if writeback.complete {
                                    WritebackCursor::default()
                                } else {
                                    writeback.next_cursor
                                },
                                drain_passes: 0,
                            },
                            writeback: Some(writeback),
                            transactions_frozen: freeze.frozen as u64,
                            freeze_blocked: freeze.blocked,
                            abort_exceptions_recorded: freeze.exceptions_recorded as u64,
                            freeze_exception_capacity_full: freeze.exception_capacity_full,
                            dirty_pages_remaining: writeback.dirty_pages_remaining,
                            checkpoint: None,
                            tail_reclaim: None,
                            complete: false,
                            stop_reason: PagedCheckpointStopReason::Writeback,
                        }
                    } else {
                        let truncate =
                            !self.transactions.has_unfrozen_commits() && !self.backup_pinned();
                        let redo_lsn = self.wal.next_lsn();
                        let checkpoint = self.checkpointer.finish_checkpoint(
                            self.meta_page,
                            redo_lsn,
                            writeback.pages_written,
                            truncate,
                        )?;
                        // The checkpoint flushed the meta page, so the spill
                        // descriptor naming the current chain is durable; the
                        // pages the last persist retired may now be freed.
                        self.spill.reclaim_retired(self.meta_page, &self.pool)?;
                        self.finalize_structural_frees()?;
                        let tail_reclaim = if checkpoint.wal_truncated {
                            Some(
                                self.pool
                                    .store()
                                    .truncate_trailing_free_pages_bounded(limits.tail_reclaim)?,
                            )
                        } else {
                            None
                        };
                        PagedCheckpointStepReport {
                            phase_before: cursor.phase,
                            next_cursor: PagedCheckpointCursor {
                                phase: PagedCheckpointPhase::Complete,
                                writeback: WritebackCursor::default(),
                                drain_passes: 0,
                            },
                            writeback: Some(writeback),
                            transactions_frozen: freeze.frozen as u64,
                            freeze_blocked: freeze.blocked,
                            abort_exceptions_recorded: freeze.exceptions_recorded as u64,
                            freeze_exception_capacity_full: freeze.exception_capacity_full,
                            dirty_pages_remaining: 0,
                            checkpoint: Some(checkpoint),
                            tail_reclaim,
                            complete: true,
                            stop_reason: PagedCheckpointStopReason::Complete,
                        }
                    }
                }
            }
            PagedCheckpointPhase::Complete => unreachable!(),
        };
        report.validate(limits, self.options.page_size)?;
        Ok(report)
    }

    /// Write back one cursor-bounded dirty-buffer window. The pool's installed
    /// barrier durably logs every after-image before its page write.
    pub fn writeback_step(
        &self,
        cursor: WritebackCursor,
        limits: WritebackLimits,
    ) -> Result<WritebackStepReport> {
        self.pool.writeback_step(cursor, limits)
    }

    /// The durable page directory and free-space map.
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Reclaim versions no snapshot can ever see again.
    ///
    /// Phase 2: "Implement background vacuum/version reclamation using the
    /// oldest active snapshot. Bound each maintenance pass by pages, bytes, and
    /// time." This is the pages bound; it stops after `max_pages` and reports
    /// where it got to, so a caller can spread the work.
    ///
    /// # What is safe to reclaim
    ///
    /// A version qualifies only if it is dead to *everyone*:
    ///
    /// - its deleter committed and began before the oldest live snapshot, so no
    ///   reader can still be entitled to it; or
    /// - its creator aborted, so no reader ever could.
    ///
    /// Breaking a chain link is therefore safe: any walk that would have reached
    /// the reclaimed version belongs to a snapshot at or after the oldest live
    /// one, and that walk stops at a newer visible version first. Reads treat an
    /// unreadable link as "nothing older is visible", which is exactly the
    /// truth once the version is gone.
    /// Return interior free-page bytes to the filesystem; see
    /// [`crate::manager::PageStore::punch_free_pages_bounded`].
    pub fn punch_free_pages(&self, max_pages: u64) -> Result<crate::manager::HolePunchReport> {
        self.pool.store().punch_free_pages_bounded(max_pages)
    }

    pub fn vacuum(&self, max_pages: usize) -> Result<VacuumReport> {
        self.vacuum_step(
            VacuumCursor::default(),
            VacuumLimits {
                max_pages: u64::try_from(max_pages)
                    .unwrap_or(u64::MAX)
                    .min(MAX_VACUUM_PAGES_PER_STEP),
                max_bytes: MAX_VACUUM_BYTES_PER_STEP,
                max_duration_millis: MAX_VACUUM_DURATION_MILLIS_PER_STEP,
            },
        )
    }

    /// Reclaim one explicitly bounded, cursor-resumable slice of MVCC history.
    ///
    /// Catalog traversal is streaming: memory is one B+tree leaf, one heap
    /// page, one fixed-size version header, and at most the bounded list of
    /// fully dead pages from this step. The returned cursor is inclusive, so a
    /// crash before its caller checkpoints the report safely repeats the last
    /// page instead of skipping it.
    pub fn vacuum_step(&self, cursor: VacuumCursor, limits: VacuumLimits) -> Result<VacuumReport> {
        limits.validate(self.pool.page_size())?;
        let started = Instant::now();
        let mut report = self.vacuum_step_with_expiry(cursor, limits, || {
            started.elapsed().as_millis() >= u128::from(limits.max_duration_millis)
        })?;
        report.elapsed_millis = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        Ok(report)
    }

    /// Validate a maintenance envelope against the store's actual durable page
    /// size without beginning a maintenance pass.
    pub fn validate_vacuum_limits(&self, limits: &VacuumLimits) -> Result<()> {
        limits.validate(self.pool.page_size())
    }

    /// Reclaim one bounded, cursor-resumable slice of **orphaned** free pages.
    ///
    /// An interrupted free-list publication can leave a page typed `Free` on
    /// disk yet absent from the intrusive free list — space that is reusable
    /// but will never be allocated. This scan walks the page file, builds the
    /// free-list membership set, and relinks any Free-typed page that is not a
    /// member. It only ever touches pages already typed `Free`, so it cannot
    /// lose data: the worst interruption leaks reusable space, never a live
    /// page.
    ///
    /// Ordering keeps it crash-consistent exactly like vacuum's free path: the
    /// relinked Free images go through the pool and are WAL-logged as
    /// structural records, the pool is flushed so the file matches, and only
    /// then does the superblock atomically adopt the segment. If the free list
    /// exceeds the step's visit budget the step defers without mutation.
    pub fn reclaim_orphaned_free_pages_step(
        &self,
        cursor: FreeReclaimCursor,
        limits: FreeReclaimLimits,
    ) -> Result<FreeReclaimReport> {
        limits.validate(self.pool.page_size())?;
        let started = Instant::now();
        let expired = || started.elapsed().as_millis() >= u128::from(limits.max_duration_millis);
        let _guard = self.write_lock.lock();
        let store = self.pool.store();
        let page_size = store.page_size();
        // The scan classifies pages from on-disk headers, so any dirty page
        // must reach the file first or a recently freed page could be read in
        // its pre-free state. `free_list_members` likewise reads each member's
        // on-disk header and treats a not-yet-Flushed Free page as corruption,
        // so the flush is a prerequisite for both.
        self.pool.flush_all()?;
        let page_count = store.page_count();

        // Membership set for the current free list. Without it we cannot tell
        // a listed Free page from an orphan, so an oversized list defers.
        let members = match store.free_list_members(limits.max_free_list_visits)? {
            Some(members) => members,
            None => {
                return Ok(FreeReclaimReport {
                    next_cursor: cursor,
                    stop_reason: FreeReclaimStopReason::FreeListTooLarge,
                    elapsed_millis: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    ..Default::default()
                });
            }
        };
        let free_list_members = members.len() as u64;
        // The flush and the membership walk are not page-bounded; if they alone
        // exhausted the deadline, yield now instead of starting the scan. The
        // cursor is unchanged, so the next step re-derives the membership set
        // and scans from the same position.
        if expired() {
            return Ok(FreeReclaimReport {
                free_list_members,
                next_cursor: cursor,
                stop_reason: FreeReclaimStopReason::TimeLimit,
                elapsed_millis: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                ..Default::default()
            });
        }

        let mut report = FreeReclaimReport {
            free_list_members,
            ..Default::default()
        };
        let mut orphans: Vec<PageId> = Vec::new();
        let start = cursor.next_page_id.unwrap_or(1).max(1);
        let mut page_id = start;
        let mut stop_reason = FreeReclaimStopReason::Complete;

        while page_id < page_count {
            if report.pages_scanned >= limits.max_pages {
                stop_reason = FreeReclaimStopReason::PageLimit;
                break;
            }
            // Strict envelope: the next page must FIT in the remaining byte
            // budget, mirroring vacuum. Checking `examined >= max` after the
            // fact would let a limit of `page_size + 1` admit two pages.
            if u64::from(page_size) > limits.max_bytes.saturating_sub(report.bytes_examined) {
                stop_reason = FreeReclaimStopReason::ByteLimit;
                break;
            }
            if expired() {
                stop_reason = FreeReclaimStopReason::TimeLimit;
                break;
            }
            let header = store.read_header(page_id)?;
            report.pages_scanned = report.pages_scanned.saturating_add(1);
            report.bytes_examined = report.bytes_examined.saturating_add(u64::from(page_size));
            if header.page_type == crate::page::PageType::Free
                && members.binary_search(&page_id).is_err()
            {
                report.orphans_found = report.orphans_found.saturating_add(1);
                orphans.push(page_id);
                if orphans.len() as u64 >= limits.max_adopt {
                    // The batch is full. Stop at the next page so a following
                    // step rediscovers any later orphans instead of skipping
                    // them.
                    stop_reason = FreeReclaimStopReason::AdoptLimit;
                    page_id = page_id.saturating_add(1);
                    break;
                }
            }
            page_id = page_id.saturating_add(1);
        }

        // Relink and adopt the collected orphans. Each becomes a Free page
        // whose next_page threads to the following orphan, the last binding to
        // the current list head — the exact segment adopt_free_pages validates.
        if !orphans.is_empty() {
            let free_adoption = self.prepare_free_adoption(0, &orphans)?;
            self.log_dirty_pages(0)?;
            self.wal.sync()?;
            self.pool.flush_all()?;
            if let Some((head, count)) = free_adoption {
                store.adopt_free_pages(head, count)?;
            }
            report.pages_reclaimed = orphans.len() as u64;
        }

        let complete = stop_reason == FreeReclaimStopReason::Complete && page_id >= page_count;
        report.next_cursor = FreeReclaimCursor {
            next_page_id: if complete { None } else { Some(page_id) },
        };
        report.stop_reason = stop_reason;
        report.complete = complete;
        report.elapsed_millis = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        Ok(report)
    }

    /// Validate a free-reclaim envelope without beginning a pass.
    pub fn validate_free_reclaim_limits(&self, limits: &FreeReclaimLimits) -> Result<()> {
        limits.validate(self.pool.page_size())
    }

    fn vacuum_step_with_expiry(
        &self,
        cursor: VacuumCursor,
        limits: VacuumLimits,
        mut time_exhausted: impl FnMut() -> bool,
    ) -> Result<VacuumReport> {
        limits.validate(self.pool.page_size())?;
        // Reclamation rewrites slots and free space, so it is a writer like any
        // other and must not interleave with `put` — or with readers.
        let _guard = self.write_lock.lock();
        let _structure = self.structure.write();
        let oldest_active = self.transactions.oldest_in_progress_or_next();

        let mut report = VacuumReport {
            oldest_active,
            ..Default::default()
        };
        let mut fully_dead: Vec<PageId> = Vec::new();
        let mut free_space_updates: Vec<(PageId, usize, usize)> = Vec::new();
        let page_size = u64::from(self.pool.page_size());
        let mut pages = self
            .catalog
            .scan_pages(crate::page::PageType::Heap, cursor.next_page_id)?;

        'pages: loop {
            let Some(page_id) = pages.next().transpose()? else {
                report.complete = true;
                report.stop_reason = VacuumStopReason::Complete;
                report.next_cursor = VacuumCursor::default();
                break;
            };
            // A PAGE IS THE ATOMIC UNIT OF BOUNDED VACUUM WORK.
            //
            // `VacuumCursor` is page-granular: it can say "resume at page N"
            // and nothing finer. A budget that could interrupt work *inside* a
            // page therefore produced a state the cursor could not represent,
            // and the only way to encode it was to point back at the page just
            // entered — so the next step redid it, forever. That is not a
            // tuning problem, it is a contract mismatch.
            //
            // Every envelope is now checked BETWEEN pages, and only once at
            // least one page has completed. The byte and time limits become
            // SOFT bounds whose maximum overshoot is one page's vacuum work,
            // which is a far healthier promise than a hard limit the cursor
            // cannot resume from.
            if report.pages_scanned >= limits.max_pages {
                report.stop_reason = VacuumStopReason::PageLimit;
                report.next_cursor.next_page_id = Some(page_id);
                break;
            }
            if report.pages_scanned > 0 && time_exhausted() {
                report.stop_reason = VacuumStopReason::TimeLimit;
                report.next_cursor.next_page_id = Some(page_id);
                break;
            }
            if report.pages_scanned > 0 && report.bytes_examined >= limits.max_bytes {
                report.stop_reason = VacuumStopReason::ByteLimit;
                report.next_cursor.next_page_id = Some(page_id);
                break;
            }
            report.pages_scanned += 1;
            report.bytes_examined += page_size;

            let before_free = {
                let guard = self.pool.get(page_id)?;
                crate::slotted::SlottedPageRef::new(guard.bytes()).free_space()
            };

            let locators = self.heap.page_locators(page_id)?;
            // No early exit below this point: the page runs to completion.
            for locator in locators {
                let (value_len, overflow_pages) = match self.heap.value_storage(locator) {
                    Ok(metadata) => metadata,
                    Err(_) => continue,
                };
                let header_bytes = value_len.min(crate::mvcc::VERSION_HEADER_BYTES as u64);
                let header_io_bytes = if overflow_pages == 0 {
                    header_bytes
                } else {
                    page_size
                };
                let header_prefix = match self
                    .heap
                    .read_prefix(locator, crate::mvcc::VERSION_HEADER_BYTES)
                {
                    Ok(prefix) => prefix,
                    Err(_) => continue,
                };
                report.bytes_examined += header_io_bytes;
                let Some((header, _)) = VersionHeader::decode(&header_prefix) else {
                    continue;
                };
                report.versions_examined += 1;

                let creator_aborted = self.transactions.status(header.xmin) == TxStatus::Aborted;
                let deleted_for_everyone = header.xmax != 0
                    && header.xmax < oldest_active
                    && self.transactions.status(header.xmax) == TxStatus::Committed;

                if creator_aborted || deleted_for_everyone {
                    let release_bytes = overflow_pages.checked_mul(page_size).ok_or_else(|| {
                        PageError::InvalidMaintenanceLimits {
                            reason: format!(
                                "overflow release byte count exceeds u64 for page {page_id}"
                            ),
                        }
                    })?;
                    self.heap.delete_for_vacuum(locator)?;
                    report.bytes_examined += release_bytes;
                    report.versions_reclaimed += 1;
                }
            }

            let (after_free, live) = {
                let guard = self.pool.get(page_id)?;
                let page = crate::slotted::SlottedPageRef::new(guard.bytes());
                (page.free_space(), page.live_count())
            };
            if after_free != before_free {
                report.bytes_reclaimed += after_free.saturating_sub(before_free) as u64;
            }
            if live == 0 {
                report.bytes_examined += page_size;
                fully_dead.push(page_id);
            } else if after_free != before_free {
                free_space_updates.push((page_id, before_free, after_free));
            }
        }
        drop(pages);

        // The catalog cursor is gone before any catalog B+tree mutation. This
        // prevents a free-space-key split from invalidating the directory leaf
        // chain that selected the next heap page.
        for (page_id, before_free, after_free) in free_space_updates {
            self.catalog
                .update_free_space(page_id, before_free, after_free)?;
        }

        // FREE fully-dead heap pages. Safe against stale locators because
        // (a) the catalog keeps the page's generation FLOOR, so a future heap
        // tenancy mints strictly above every generation this page ever handed
        // out, and (b) until reuse, a dereference sees a slotless Free page
        // (NoSuchSlot) — both read as end-of-chain. Ordering makes it
        // crash-consistent: Free images + catalog changes go through the pool
        // and are WAL-logged as structural (transaction 0) records, the pool
        // is flushed so the FILE matches (the allocator walks the list with
        // raw reads), and only then does the superblock — the atomic commit
        // point — adopt the pages. A crash anywhere earlier leaks the pages
        // (consistent), never corrupts the list.
        if !fully_dead.is_empty() {
            let store = self.pool.store().clone();
            for page_id in &fully_dead {
                let (floor, free_bytes) = {
                    let guard = self.pool.get(*page_id)?;
                    let page = crate::slotted::SlottedPageRef::new(guard.bytes());
                    (page.max_generation(), page.free_space())
                };
                self.catalog.set_page_generation_floor(*page_id, floor)?;
                self.catalog.forget_page(*page_id, free_bytes)?;
                self.heap.forget_page(*page_id);
            }
            let free_adoption = self.prepare_free_adoption(0, &fully_dead)?;
            self.log_dirty_pages(0)?;
            self.wal.sync()?;
            self.pool.flush_all()?;
            if let Some((head, count)) = free_adoption {
                store.adopt_free_pages(head, count)?;
            }
            report.pages_freed = fully_dead.len() as u64;
        }

        self.sync_roots()?;
        report.stopped_early = !report.complete;
        report.start_page = cursor.next_page_id;
        report.end_page = report.next_cursor.next_page_id;

        // THE INVARIANT that makes the liveness bug unrepresentable: a step
        // that entered a page must have moved past it. `start == end` with
        // work done is exactly the state that looped forever, so it is an
        // error rather than a success with suspicious counters.
        if report.pages_scanned > 0 && !report.complete && report.end_page == report.start_page {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "vacuum entered page {:?} and did not advance past it \
                     ({} pages scanned, {} versions reclaimed); a page-granular \
                     cursor cannot resume mid-page",
                    report.start_page, report.pages_scanned, report.versions_reclaimed
                ),
            });
        }
        Ok(report)
    }

    /// INDEX-ENTRY GC: remove B-tree keys whose entire version chain has
    /// been vacuumed away. The index maps each key to its NEWEST version;
    /// vacuum only reclaims versions dead to everyone, so a head locator
    /// that no longer resolves (stale, dead, or vanished slot) proves no
    /// snapshot — current or future — can ever see this key again. MVCC
    /// `delete` deliberately keeps keys for old snapshots; this sweep is
    /// the complement that runs AFTER vacuum has retired that history.
    ///
    /// Holds the writer and structure locks for the duration (vacuum's
    /// discipline): no concurrent reader is mid-descent and no writer can
    /// interleave, so remove-while-iterating is safe via the two-phase
    /// collect-then-remove below. Returns the number of keys removed.
    pub fn sweep_dead_index_entries(&self, max_keys: usize) -> Result<u64> {
        let _guard = self.write_lock.lock();
        let _structure = self.structure.write();
        // Phase 1: collect removable keys (bounded).
        let mut removable: Vec<Vec<u8>> = Vec::new();
        for entry in self.index.iter()? {
            let (key, value) = entry?;
            let Some(locator) = TupleLocator::decode(&value) else {
                continue;
            };
            match self.heap.get(locator) {
                Ok(_) => {}
                Err(PageError::StaleLocator { .. })
                | Err(PageError::DeadSlot { .. })
                | Err(PageError::NoSuchSlot { .. })
                | Err(PageError::OutOfBounds { .. }) => {
                    removable.push(key);
                    if removable.len() >= max_keys {
                        break;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        // Phase 2: remove.
        let mut removed = 0u64;
        for key in &removable {
            if self.index.remove(key)? {
                removed += 1;
            }
        }
        if removed > 0 {
            self.sync_roots()?;
        }
        Ok(removed)
    }

    /// Log after-images for pages modified since their last record.
    ///
    /// Logging after the mutation rather than before is safe here because the
    /// page is not yet durable: the buffer pool holds it dirty, and nothing
    /// writes it out until the writeback barrier or a checkpoint, both of which
    /// put the log first. The ordering that matters — log durable before page
    /// durable — is preserved.
    fn log_dirty_pages(&self, transaction: u64) -> Result<()> {
        let dirty = self.pool.take_pages_needing_log();
        for page_id in dirty {
            let image = {
                let guard = self.pool.get(page_id)?;
                guard.bytes().to_vec()
            };
            self.wal.log_page_image(page_id, transaction, &image)?;
        }
        Ok(())
    }

    /// Finalize every free page staged by a transaction (plus an explicit
    /// maintenance batch) into one linked segment whose tail binds to the
    /// current free-list head. The caller must then log, make the WAL durable,
    /// flush these images, and publish the returned head/count.
    fn prepare_free_adoption(
        &self,
        transaction: u64,
        extra: &[PageId],
    ) -> Result<Option<(PageId, u64)>> {
        let mut pages = self.pool.take_pending_frees(transaction);
        pages.extend_from_slice(extra);
        pages.sort_unstable();
        pages.dedup();
        if pages.is_empty() {
            return Ok(None);
        }
        if pages.len() > 65_536 {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "one durability boundary cannot free more than 65536 pages".to_string(),
            });
        }
        let store = self.pool.store();
        let old_head = store.free_list_head();
        for (index, page_id) in pages.iter().enumerate() {
            let next = pages.get(index + 1).copied().unwrap_or(old_head);
            let mut guard = self.pool.get_mut(*page_id)?;
            let bytes = guard.bytes_mut();
            let generation = crate::page::PageHeader::decode(bytes, store.path())?
                .generation
                .saturating_add(1);
            bytes.fill(0);
            let mut header = crate::page::PageHeader::new(
                *page_id,
                crate::page::PageType::Free,
                store.page_size(),
            );
            header.generation = generation;
            header.next_page = next;
            header.encode(bytes);
        }
        Ok(Some((pages[0], pages.len() as u64)))
    }

    /// Make structural (transaction-zero) frees reusable immediately. This is
    /// deliberately a small second durability boundary after status-spill
    /// publication: retired spill pages cannot be staged before the descriptor
    /// replacing them is durable, but leaving them only in memory until some
    /// later checkpoint would leak space after a clean shutdown.
    fn finalize_structural_frees(&self) -> Result<()> {
        let adoption = self.prepare_free_adoption(0, &[])?;
        if let Some((head, count)) = adoption {
            self.log_dirty_pages(0)?;
            self.wal.sync()?;
            self.pool.flush_all()?;
            self.pool.store().adopt_free_pages(head, count)?;
            if !self.transactions.has_unfrozen_commits() && !self.backup_pinned() {
                self.checkpointer.checkpoint_and_truncate(self.meta_page)?;
            }
        }
        Ok(())
    }

    /// A bounded cursor over visible rows, ascending by key, from `start`.
    ///
    /// This is the scan Phase 5 requires and the one an import or integrity
    /// pass needs: it holds one index leaf plus one heap page at a time, so
    /// memory is constant however large the range. Rows are resolved through
    /// `snapshot`, so a long verification pass sees one consistent state rather
    /// than a smear of whatever committed while it ran.
    pub fn scan_from<'a>(&'a self, snapshot: &Snapshot, start: &[u8]) -> Result<PagedScan<'a>> {
        // The initial descent to the start leaf is a structure read.
        let entries = {
            let _structure = self.structure.read();
            self.index.range(start)?
        };
        Ok(PagedScan {
            store: self,
            snapshot: snapshot.clone(),
            entries,
        })
    }

    /// Resolve one bounded key range after grouping its MVCC chain heads by
    /// heap page. Results retain primary-key order and snapshot visibility.
    pub fn scan_locality_batch(
        &self,
        snapshot: &Snapshot,
        prefix: &[u8],
        start: &[u8],
        max_candidates: usize,
    ) -> Result<PagedLocalityBatch> {
        struct Candidate {
            ordinal: usize,
            key: Vec<u8>,
            head: Option<TupleLocator>,
        }

        let _structure = self.structure.read();
        let mut entries = self.index.range(start)?;
        let mut candidates = Vec::with_capacity(max_candidates.min(65_536));
        let mut exhausted = false;
        while candidates.len() < max_candidates.max(1) {
            let Some(entry) = entries.next() else {
                exhausted = true;
                break;
            };
            let (key, value) = entry?;
            if !key.starts_with(prefix) {
                exhausted = true;
                break;
            }
            candidates.push(Candidate {
                ordinal: candidates.len(),
                key,
                head: TupleLocator::decode(&value),
            });
        }
        let last_key = candidates.last().map(|candidate| candidate.key.clone());
        candidates.sort_unstable_by_key(|candidate| {
            candidate
                .head
                .map(|head| (0_u8, head.page_id, head.slot, head.generation))
                .unwrap_or((1_u8, u64::MAX, u16::MAX, u32::MAX))
        });
        let workers = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(16)
            .min(candidates.len().max(1));
        let chunk_size = candidates.len().max(1).div_ceil(workers);
        let mut rows = std::thread::scope(|scope| -> Result<Vec<_>> {
            let handles = candidates
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || -> Result<Vec<_>> {
                        let mut rows = Vec::with_capacity(chunk.len());
                        for candidate in chunk {
                            if let Some(value) = self.walk_chain_as_of(snapshot, candidate.head)? {
                                rows.push((candidate.ordinal, candidate.key.clone(), value));
                            }
                        }
                        Ok(rows)
                    })
                })
                .collect::<Vec<_>>();
            let mut rows = Vec::with_capacity(candidates.len());
            for handle in handles {
                match handle.join() {
                    Ok(worker_rows) => rows.extend(worker_rows?),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
            Ok(rows)
        })?;
        rows.sort_unstable_by_key(|(ordinal, _, _)| *ordinal);
        Ok(PagedLocalityBatch {
            rows: rows
                .into_iter()
                .map(|(_, key, value)| (key, value))
                .collect(),
            last_key,
            exhausted,
        })
    }

    /// Point-batch companion to [`Self::scan_locality_batch`]: resolve many
    /// exact keys after grouping their MVCC chain heads by heap page. One
    /// structure-read guard covers every index descent (interior pages are
    /// few and hot) and stays held across the page-ordered parallel chain
    /// walks, so a scattered batch costs clustered heap reads instead of one
    /// random descent-plus-walk per key. Results align with `keys`.
    pub fn get_locality_batch(
        &self,
        snapshot: &Snapshot,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        struct Candidate {
            ordinal: usize,
            head: Option<TupleLocator>,
        }

        let _structure = self.structure.read();
        let mut candidates = Vec::with_capacity(keys.len());
        for (ordinal, key) in keys.iter().enumerate() {
            if let Some(value) = self.index.get(key)? {
                candidates.push(Candidate {
                    ordinal,
                    head: TupleLocator::decode(&value),
                });
            }
        }
        candidates.sort_unstable_by_key(|candidate| {
            candidate
                .head
                .map(|head| (0_u8, head.page_id, head.slot, head.generation))
                .unwrap_or((1_u8, u64::MAX, u16::MAX, u32::MAX))
        });
        let workers = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(16)
            .min(candidates.len().max(1));
        let chunk_size = candidates.len().max(1).div_ceil(workers);
        let mut results: Vec<Option<Vec<u8>>> = vec![None; keys.len()];
        let resolved = std::thread::scope(|scope| -> Result<Vec<_>> {
            let handles = candidates
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || -> Result<Vec<_>> {
                        let mut rows = Vec::with_capacity(chunk.len());
                        for candidate in chunk {
                            if let Some(value) = self.walk_chain_as_of(snapshot, candidate.head)? {
                                rows.push((candidate.ordinal, value));
                            }
                        }
                        Ok(rows)
                    })
                })
                .collect::<Vec<_>>();
            let mut rows = Vec::with_capacity(candidates.len());
            for handle in handles {
                match handle.join() {
                    Ok(worker_rows) => rows.extend(worker_rows?),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
            Ok(rows)
        })?;
        for (ordinal, value) in resolved {
            results[ordinal] = Some(value);
        }
        Ok(results)
    }

    /// A bounded cursor over visible rows with `key < bound`, DESCENDING
    /// (`None` = from the greatest key). Same shape as [`Self::scan_from`]:
    /// one index leaf plus one heap page pinned at a time, rows resolved
    /// through `snapshot`. Stepping to a previous leaf costs one root descent
    /// (leaves are forward-linked only).
    pub fn scan_rev_below<'a>(
        &'a self,
        snapshot: &Snapshot,
        bound: Option<&[u8]>,
    ) -> Result<PagedScanRev<'a>> {
        // The initial descent to the bound leaf is a structure read.
        let entries = {
            let _structure = self.structure.read();
            self.index.range_rev_below(bound)?
        };
        Ok(PagedScanRev {
            store: self,
            snapshot: snapshot.clone(),
            entries,
        })
    }

    /// [`Self::scan_from`], additionally yielding each row's chain-head
    /// locator — the exact value an index backfill records as a TID hint.
    pub fn scan_from_with_heads<'a>(
        &'a self,
        snapshot: &Snapshot,
        start: &[u8],
    ) -> Result<PagedScanHeads<'a>> {
        let entries = {
            let _structure = self.structure.read();
            self.index.range(start)?
        };
        Ok(PagedScanHeads {
            store: self,
            snapshot: snapshot.clone(),
            entries,
        })
    }

    /// A bounded cursor over KEYS only, from `start`, with no heap access.
    ///
    /// Yields every key in the index in order — including keys whose versions
    /// are all invisible to `snapshot` (deciding that requires the heap, which
    /// is exactly the cost this exists to avoid). The snapshot parameter is
    /// accepted for interface symmetry and future pruning.
    pub fn scan_keys_from<'a>(
        &'a self,
        _snapshot: &Snapshot,
        start: &[u8],
    ) -> Result<impl Iterator<Item = Result<Vec<u8>>> + 'a> {
        fn key_only(entry: Result<(Vec<u8>, Vec<u8>)>) -> Result<Vec<u8>> {
            entry.map(|(key, _)| key)
        }
        let keys = {
            let _structure = self.structure.read();
            self.index
                .range(start)?
                .map(key_only as fn(Result<(Vec<u8>, Vec<u8>)>) -> Result<Vec<u8>>)
        };
        Ok(KeyScan { store: self, keys })
    }

    /// Snapshot-visible keys plus a fixed-size prefix of each value. This is
    /// the on-page equivalent of a posting skip stream: enough bytes for rank
    /// metadata, never the positions payload.
    pub fn scan_visible_key_prefixes_from<'a>(
        &'a self,
        snapshot: &Snapshot,
        start: &[u8],
        value_prefix_bytes: usize,
    ) -> Result<VisibleKeyPrefixScan<'a>> {
        let entries = {
            let _structure = self.structure.read();
            self.index.range(start)?
        };
        Ok(VisibleKeyPrefixScan {
            store: self,
            snapshot: snapshot.clone(),
            entries,
            value_prefix_bytes,
        })
    }

    /// A bounded cursor over every visible row.
    pub fn scan<'a>(&'a self, snapshot: &Snapshot) -> Result<PagedScan<'a>> {
        self.scan_from(snapshot, b"")
    }

    /// Every key in the index, collected.
    ///
    /// **Unbounded**: allocates one entry per key. Fine for diagnostics and
    /// small collections; use [`Self::scan`] for anything whose size is not
    /// known in advance.
    pub fn keys(&self) -> Result<Vec<Vec<u8>>> {
        self.index
            .iter()?
            .map(|entry| entry.map(|(key, _)| key))
            .collect()
    }

    pub fn len(&self) -> Result<usize> {
        self.index.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    pub fn page_size(&self) -> u32 {
        self.options.page_size
    }

    pub fn validate_checkpoint_limits(&self, limits: PagedCheckpointLimits) -> Result<()> {
        limits.validate(self.options.page_size)
    }

    pub fn request_read_ahead<I>(&self, page_ids: I) -> ReadAheadSubmitReport
    where
        I: IntoIterator<Item = PageId>,
    {
        self.pool.request_read_ahead(page_ids)
    }

    pub fn read_ahead_queue_depth(&self) -> u64 {
        self.pool.read_ahead_queue_depth()
    }

    pub fn read_ahead_step(&self, limits: ReadAheadLimits) -> Result<ReadAheadStepReport> {
        self.pool.read_ahead_step(limits)
    }

    /// See [`crate::pool::BufferPool::register_read_ahead_driver`].
    pub fn register_read_ahead_driver(&self) {
        self.pool.register_read_ahead_driver();
    }

    /// See [`crate::pool::BufferPool::unregister_read_ahead_driver`].
    pub fn unregister_read_ahead_driver(&self) {
        self.pool.unregister_read_ahead_driver();
    }

    /// Return scrape-safe paged-storage telemetry without walking user data.
    pub fn snapshot(&self) -> Result<PagedStoreSnapshot> {
        let page_store = self.pool.store();
        let page_size = page_store.page_size();
        let page_count = page_store.page_count();
        let free_pages = page_store.free_page_count();
        let logical_page_bytes = page_count
            .checked_mul(u64::from(page_size))
            .ok_or(PageError::InvalidPageSize(page_size))?;
        let free_bytes = free_pages
            .checked_mul(u64::from(page_size))
            .ok_or(PageError::InvalidPageSize(page_size))?;
        let data_pages = page_count.saturating_sub(1);
        let wal = self.wal.snapshot();
        let transactions = &self.transactions;
        Ok(PagedStoreSnapshot {
            format_version: PAGED_STORE_SNAPSHOT_FORMAT_VERSION,
            page_size,
            page_count,
            logical_page_bytes,
            page_file_bytes: page_store.file_size_bytes()?,
            used_data_pages: data_pages.saturating_sub(free_pages),
            free_pages,
            free_bytes,
            wal_bytes: self.wal.size_bytes(),
            wal_max_bytes: self.options.wal_max_bytes,
            fsync_enabled: self.options.fsync,
            buffer_pool: self.pool.snapshot(),
            page_io: page_store.metrics.snapshot(),
            wal,
            recovery: self.last_recovery,
            transaction_frozen_xid: transactions.frozen_xid(),
            transaction_next_xid: transactions.next_xid(),
            resident_transaction_entries: u64::try_from(transactions.resident_entries())
                .unwrap_or(u64::MAX),
            abort_exceptions: u64::try_from(transactions.abort_exception_count())
                .unwrap_or(u64::MAX),
            abort_exception_capacity: u64::try_from(transactions.abort_exception_capacity())
                .unwrap_or(u64::MAX),
            status_spill_entries: self.spill.entry_count(),
            status_spill_pages: self.spill.page_count(),
            status_spill_lookup_failures: self.spill.lookup_failure_count(),
        })
    }

    pub fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }

    pub fn heap(&self) -> &HeapFile {
        &self.heap
    }

    pub fn index(&self) -> &BTree {
        &self.index
    }
}

fn observe_version_chain_terminal(
    report: &mut VersionChainVerifyReport,
    terminal: &VersionChainTerminal,
) {
    match terminal {
        VersionChainTerminal::InvalidHead => {
            report.invalid_heads = report.invalid_heads.saturating_add(1);
        }
        VersionChainTerminal::MalformedVersion { .. } => {
            report.malformed_versions = report.malformed_versions.saturating_add(1);
        }
        VersionChainTerminal::Cycle { .. } => {
            report.cycles = report.cycles.saturating_add(1);
        }
        VersionChainTerminal::LimitExceeded { .. } => {
            report.limit_exceeded = report.limit_exceeded.saturating_add(1);
        }
        VersionChainTerminal::Empty
        | VersionChainTerminal::End
        | VersionChainTerminal::Vacuumed { .. } => {}
    }
}

fn finish_version_chain_verify_step(
    mut report: VersionChainVerifyStepReport,
    started: Instant,
    stop_reason: VersionChainVerifyStopReason,
    next_key: Option<Vec<u8>>,
) -> VersionChainVerifyStepReport {
    report.version_chains.valid = report.version_chains.invalid_heads == 0
        && report.version_chains.malformed_versions == 0
        && report.version_chains.cycles == 0
        && report.version_chains.limit_exceeded == 0;
    report.next_cursor = VersionChainVerifyCursor { next_key };
    report.stop_reason = stop_reason;
    report.elapsed_millis = elapsed_millis(started);
    report.complete = stop_reason == VersionChainVerifyStopReason::Complete;
    report
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Bounded cursor over visible rows.
///
/// Keys come from the index one leaf at a time; each row's value is resolved
/// through the version chain as the cursor reaches it. Rows invisible to the
/// snapshot — deleted, or written by transactions the snapshot cannot see — are
/// skipped rather than returned as empty, so a caller counting results gets the
/// row count it would get from a point read of each key.
pub struct PagedScan<'a> {
    store: &'a PagedStore,
    snapshot: Snapshot,
    entries: crate::btree::BTreeRange<'a>,
}

impl Iterator for PagedScan<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // One structure-read acquisition covers both the cursor advance
            // (which may refill from the next leaf — a tree descent) and the
            // chain walk from the locator the cursor yielded. Holding it
            // across the walk is what keeps that locator valid: vacuum
            // relocates tuples only under the write side. The guard is
            // re-acquired per candidate so a scan skipping a long run of
            // invisible rows does not starve writers.
            //
            // Walking from the yielded locator — instead of discarding it and
            // re-looking the key up — removes a full tree descent and a lock
            // acquisition per row, which was half the tree work of every scan
            // in the system.
            let _structure = self.store.structure.read();
            let (key, value) = match self.entries.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error)),
            };
            match self
                .store
                .walk_chain_as_of(&self.snapshot, TupleLocator::decode(&value))
            {
                Ok(Some(visible)) => return Some(Ok((key, visible))),
                // Not visible to this snapshot: skip rather than surface a hole.
                Ok(None) => continue,
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// Which version-header stamp a targeted repair addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderStampField {
    Xmin,
    Xmax,
}

impl HeaderStampField {
    pub fn name(self) -> &'static str {
        match self {
            Self::Xmin => "xmin",
            Self::Xmax => "xmax",
        }
    }
}

/// Outcome of [`PagedStore::repair_version_header_stamp`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderRepair {
    Repaired,
    AlreadyClean,
}

/// Snapshot-visible key cursor returning only a bounded value prefix.
pub struct VisibleKeyPrefixScan<'a> {
    store: &'a PagedStore,
    snapshot: Snapshot,
    entries: crate::btree::BTreeRange<'a>,
    value_prefix_bytes: usize,
}

impl Iterator for VisibleKeyPrefixScan<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let _structure = self.store.structure.read();
            let (key, value) = match self.entries.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error)),
            };
            match self.store.visible_value_prefix_as_of(
                &self.snapshot,
                TupleLocator::decode(&value),
                self.value_prefix_bytes,
            ) {
                Ok(Some(prefix)) => return Some(Ok((key, prefix))),
                Ok(None) => continue,
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// [`PagedScan`] that also yields each visible row's chain-head locator and
/// that head's `xmin` — together, the exact TID hint an index backfill records.
///
/// The head locator comes straight from the index leaf the cursor is already
/// standing on. Note it is the CHAIN head, not necessarily the locator of the
/// yielded (visible) version — a hint must point at the head so a hinted read
/// can walk `prev` for older snapshots.
pub struct PagedScanHeads<'a> {
    store: &'a PagedStore,
    snapshot: Snapshot,
    entries: crate::btree::BTreeRange<'a>,
}

impl Iterator for PagedScanHeads<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>, TupleLocator, Xid)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Same locking contract as `PagedScan::next` — one structure-read
            // acquisition covers the cursor advance and the chain walk.
            let _structure = self.store.structure.read();
            let (key, value) = match self.entries.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error)),
            };
            let Some(head) = TupleLocator::decode(&value) else {
                continue;
            };
            match self.store.walk_chain_as_of(&self.snapshot, Some(head)) {
                Ok(Some(visible)) => {
                    // A successful walk read and decoded the head tuple, so
                    // this buffer-pooled re-read cannot fail; its xmin is the
                    // hint's identity stamp.
                    let head_xmin = match self.store.heap.get(head) {
                        Ok(stored) => match VersionHeader::decode(&stored) {
                            Some((header, _)) => header.xmin,
                            None => continue,
                        },
                        Err(error) => return Some(Err(error)),
                    };
                    return Some(Ok((key, visible, head, head_xmin)));
                }
                Ok(None) => continue,
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// The descending mirror of [`PagedScan`]: same per-candidate structure-read
/// acquisition, same chain walk from the yielded locator; only the leaf
/// iteration direction differs.
pub struct PagedScanRev<'a> {
    store: &'a PagedStore,
    snapshot: Snapshot,
    entries: crate::btree::BTreeRangeRev<'a>,
}

impl Iterator for PagedScanRev<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let _structure = self.store.structure.read();
            let (key, value) = match self.entries.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error)),
            };
            match self
                .store
                .walk_chain_as_of(&self.snapshot, TupleLocator::decode(&value))
            {
                Ok(Some(visible)) => return Some(Ok((key, visible))),
                // Not visible to this snapshot: skip rather than surface a hole.
                Ok(None) => continue,
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// A key-only cursor; each advance re-acquires the structure read lock so a
/// leaf refill cannot race a writer's split (same rule as [`PagedScan`]).
pub struct KeyScan<'a> {
    store: &'a PagedStore,
    keys: std::iter::Map<
        crate::btree::BTreeRange<'a>,
        fn(Result<(Vec<u8>, Vec<u8>)>) -> Result<Vec<u8>>,
    >,
}

impl Iterator for KeyScan<'_> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let _structure = self.store.structure.read();
        self.keys.next()
    }
}

/// Exact inclusive restart position for one bounded vacuum sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VacuumCursor {
    /// Heap page to revisit first. `None` starts at the beginning of a sweep.
    pub next_page_id: Option<PageId>,
}

/// Non-weakenable resource envelope for one vacuum step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VacuumLimits {
    pub max_pages: u64,
    /// Logical bytes inspected plus page bytes touched while releasing overflow
    /// chains. A value larger than the remaining envelope is left intact and
    /// reported at the same cursor for a later, explicitly larger step.
    pub max_bytes: u64,
    /// Cooperative deadline checked at every page and version boundary. A
    /// single positioned page I/O may finish after the deadline; vacuum never
    /// abandons a page mutation halfway through.
    pub max_duration_millis: u64,
}

/// Also bounds the temporary list of fully dead pages awaiting one atomic free
/// publication. At eight bytes per ID this is at most 512 KiB.
pub const MAX_VACUUM_PAGES_PER_STEP: u64 = crate::manager::MAX_FREE_LIST_ADOPTION_PAGES;
/// Finite compatibility ceiling: one old-style call can never authorize more
/// than 16 GiB of logical maintenance I/O.
pub const MAX_VACUUM_BYTES_PER_STEP: u64 = 16 * 1024 * 1024 * 1024;
/// Finite compatibility ceiling: one step cooperatively yields after five
/// minutes even when an older caller supplied no duration.
pub const MAX_VACUUM_DURATION_MILLIS_PER_STEP: u64 = 5 * 60 * 1_000;

impl Default for VacuumLimits {
    fn default() -> Self {
        Self {
            max_pages: 1_024,
            max_bytes: 64 * 1024 * 1024,
            max_duration_millis: 100,
        }
    }
}

impl VacuumLimits {
    pub fn validate(&self, page_size: u32) -> Result<()> {
        if self.max_pages == 0 || self.max_pages > MAX_VACUUM_PAGES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!("max_pages must be between 1 and {MAX_VACUUM_PAGES_PER_STEP}"),
            });
        }
        let minimum_step_bytes = u64::from(page_size).saturating_mul(2);
        if self.max_bytes < minimum_step_bytes {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_bytes {} cannot inspect and retire one {}-byte heap page; minimum is {}",
                    self.max_bytes, page_size, minimum_step_bytes
                ),
            });
        }
        if self.max_bytes > MAX_VACUUM_BYTES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!("max_bytes must not exceed {MAX_VACUUM_BYTES_PER_STEP} per step"),
            });
        }
        if self.max_duration_millis == 0
            || self.max_duration_millis > MAX_VACUUM_DURATION_MILLIS_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_duration_millis must be between 1 and {MAX_VACUUM_DURATION_MILLIS_PER_STEP}"
                ),
            });
        }
        Ok(())
    }
}

/// Why a bounded vacuum step returned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VacuumStopReason {
    #[default]
    Complete,
    PageLimit,
    ByteLimit,
    TimeLimit,
}

/// What a vacuum step did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VacuumReport {
    pub pages_scanned: u64,
    pub bytes_examined: u64,
    pub versions_examined: u64,
    pub versions_reclaimed: u64,
    pub bytes_reclaimed: u64,
    /// Fully-dead heap pages returned to the free list.
    pub pages_freed: u64,
    /// The snapshot boundary this pass respected.
    pub oldest_active: Xid,
    /// Exact inclusive position for the next step.
    pub next_cursor: VacuumCursor,
    /// Where this step began. Together with `end_page` this makes the one
    /// symptom that matters legible at a glance: `versions_reclaimed = 61,000`
    /// on a 600-row table looks merely odd, but `5 -> 5` repeated a thousand
    /// times names the bug outright.
    #[serde(default)]
    pub start_page: Option<PageId>,
    /// Where the next step will begin. `None` means the sweep finished.
    #[serde(default)]
    pub end_page: Option<PageId>,
    pub stop_reason: VacuumStopReason,
    pub elapsed_millis: u64,
    pub complete: bool,
    /// Compatibility alias for callers predating `stop_reason`.
    pub stopped_early: bool,
}

/// Inclusive resume point for the orphaned-free-page reclamation scan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreeReclaimCursor {
    /// First page id to inspect. `None` starts at page 1 (page 0 is the
    /// superblock and never free).
    pub next_page_id: Option<PageId>,
}

/// Non-weakenable resource envelope for one orphaned-free-page reclaim step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreeReclaimLimits {
    /// Page headers inspected while scanning for orphaned Free pages.
    pub max_pages: u64,
    /// Logical bytes inspected.
    pub max_bytes: u64,
    /// Page-header visits allowed while walking the free list to build its
    /// membership set. If the free list is larger, the step defers without
    /// mutation rather than allocating an unbounded set.
    pub max_free_list_visits: u64,
    /// Maximum orphaned pages to relink and adopt in one step.
    pub max_adopt: u64,
    /// Cooperative deadline checked at each inspected page.
    pub max_duration_millis: u64,
}

pub const MAX_FREE_RECLAIM_PAGES_PER_STEP: u64 = crate::manager::MAX_FREE_LIST_ADOPTION_PAGES;
pub const MAX_FREE_RECLAIM_BYTES_PER_STEP: u64 = 16 * 1024 * 1024 * 1024;
pub const MAX_FREE_RECLAIM_DURATION_MILLIS_PER_STEP: u64 = 5 * 60 * 1_000;
/// Hard ceiling on the free-list membership walk. The membership set is 8
/// bytes per visited page, so this also caps its resident memory (32 MiB).
pub const MAX_FREE_RECLAIM_FREE_LIST_VISITS: u64 = 4 * 1024 * 1024;

impl Default for FreeReclaimLimits {
    fn default() -> Self {
        Self {
            max_pages: 4_096,
            max_bytes: 64 * 1024 * 1024,
            max_free_list_visits: 1_048_576,
            max_adopt: 1_024,
            max_duration_millis: 100,
        }
    }
}

impl FreeReclaimLimits {
    pub fn validate(&self, page_size: u32) -> Result<()> {
        if self.max_pages == 0 || self.max_pages > MAX_FREE_RECLAIM_PAGES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_pages must be between 1 and {MAX_FREE_RECLAIM_PAGES_PER_STEP}"
                ),
            });
        }
        if self.max_bytes < u64::from(page_size) {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_bytes {} cannot inspect one {page_size}-byte page",
                    self.max_bytes
                ),
            });
        }
        if self.max_bytes > MAX_FREE_RECLAIM_BYTES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!("max_bytes must not exceed {MAX_FREE_RECLAIM_BYTES_PER_STEP}"),
            });
        }
        if self.max_free_list_visits == 0
            || self.max_free_list_visits > MAX_FREE_RECLAIM_FREE_LIST_VISITS
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_free_list_visits must be between 1 and {MAX_FREE_RECLAIM_FREE_LIST_VISITS}"
                ),
            });
        }
        if self.max_adopt == 0 || self.max_adopt > crate::manager::MAX_FREE_LIST_ADOPTION_PAGES {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_adopt must be between 1 and {}",
                    crate::manager::MAX_FREE_LIST_ADOPTION_PAGES
                ),
            });
        }
        if self.max_duration_millis == 0
            || self.max_duration_millis > MAX_FREE_RECLAIM_DURATION_MILLIS_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_duration_millis must be between 1 and {MAX_FREE_RECLAIM_DURATION_MILLIS_PER_STEP}"
                ),
            });
        }
        Ok(())
    }
}

/// Why a bounded orphaned-free-page reclaim step returned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreeReclaimStopReason {
    #[default]
    Complete,
    PageLimit,
    ByteLimit,
    TimeLimit,
    /// The step's adoption batch filled; remaining orphans lie at or after the
    /// returned cursor and are reclaimed by a following step.
    AdoptLimit,
    /// The free list was too large to build a membership set within the step's
    /// visit budget; no mutation was attempted.
    FreeListTooLarge,
}

/// What an orphaned-free-page reclaim step did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreeReclaimReport {
    pub pages_scanned: u64,
    pub bytes_examined: u64,
    /// Free-typed pages found that are not on the free list.
    pub orphans_found: u64,
    /// Orphans relinked and adopted into the free list this step.
    pub pages_reclaimed: u64,
    /// Members of the free list walked to build the membership set.
    pub free_list_members: u64,
    pub next_cursor: FreeReclaimCursor,
    pub stop_reason: FreeReclaimStopReason,
    pub elapsed_millis: u64,
    pub complete: bool,
}

/// Magic identifying the meta page, so a stray page cannot be read as roots.
const META_MAGIC: [u8; 8] = *b"BICDBMET";

/// Guard against a corrupt version chain looping. Real chains are bounded by
/// the number of updates a row has received since the last vacuum.
const MAX_VERSION_CHAIN: usize = 1_000_000;
const VERSION_CHAIN_INLINE_SEEN: usize = 32;
const VERSION_CHAIN_SAMPLE_LIMIT: usize = 64;
/// Maximum number of logical keys one MVCC verification step may inspect.
pub const MAX_VERSION_VERIFY_KEYS_PER_STEP: u64 = 65_536;
/// Maximum aggregate version headers one MVCC verification step may inspect.
pub const MAX_VERSION_VERIFY_VERSIONS_PER_STEP: u64 = MAX_VERSION_CHAIN as u64;
/// Maximum fixed-header bytes one MVCC verification step may inspect.
pub const MAX_VERSION_VERIFY_BYTES_PER_STEP: u64 = 1024 * 1024 * 1024;
/// Cooperative wall-clock ceiling for one MVCC verification step.
pub const MAX_VERSION_VERIFY_DURATION_MILLIS_PER_STEP: u64 = 5 * 60 * 1_000;
/// Hard bound for an inclusive serialized restart key.
pub const MAX_VERSION_VERIFY_CURSOR_KEY_BYTES: usize = 1024 * 1024;
/// Hard count bound for retained operator-facing fault samples.
pub const MAX_VERSION_VERIFY_FAULT_SAMPLES: usize = 1_024;
/// Hard aggregate serialized-byte bound for retained fault samples.
pub const MAX_VERSION_VERIFY_FAULT_SAMPLE_BYTES: u64 = 16 * 1024 * 1024;

/// Allocation-free cycle detection for ordinary chains, with an exact hash map
/// fallback only after a chain is already pathologically long.
#[derive(Default)]
struct ChainCycleDetector {
    inline: [Option<TupleLocator>; VERSION_CHAIN_INLINE_SEEN],
    overflow: Option<FxHashMap<TupleLocator, usize>>,
}

impl ChainCycleDetector {
    fn observe(&mut self, locator: TupleLocator, step: usize) -> Option<usize> {
        if let Some(seen) = self.overflow.as_mut() {
            return seen.insert(locator, step);
        }
        if step < VERSION_CHAIN_INLINE_SEEN {
            if let Some(first_seen_step) = self.inline[..step]
                .iter()
                .position(|candidate| *candidate == Some(locator))
            {
                return Some(first_seen_step);
            }
            self.inline[step] = Some(locator);
            return None;
        }

        let mut seen =
            FxHashMap::with_capacity_and_hasher(VERSION_CHAIN_INLINE_SEEN * 2, Default::default());
        for (first_seen_step, candidate) in self.inline.iter().copied().enumerate() {
            if let Some(candidate) = candidate {
                seen.insert(candidate, first_seen_step);
            }
        }
        let repeated = seen.insert(locator, step);
        self.overflow = Some(seen);
        repeated
    }
}

/// Fixed meta-page prefix before the abort-exception region: magic, index
/// root, catalog root, frozen watermark, next xid, and the exception count.
const META_FIXED_BYTES: usize = 8 + 8 + 8 + 8 + 8 + 8;
/// Trailing meta-page descriptor of the transaction-status spill: head page,
/// chain length, and total entry count.
const META_SPILL_DESCRIPTOR_BYTES: usize = 8 + 8 + 8;

/// How many durable abort exceptions fit the meta page of this size.
///
/// Exceptions are the watermark's side condition, so they share the meta
/// page's durability: they are flushed before the checkpoint record that
/// allows the WAL holding their abort records to be truncated. Giving them a
/// dedicated page chain would remove the bound's ceiling but add a second
/// durable structure to lose; the fixed in-page region keeps the invariant
/// local. When the region is full, freezing degrades to its pre-1.0.85-beta
/// stop behavior and reports it.
pub fn abort_exception_capacity(page_size: u32) -> usize {
    crate::page::usable_bytes(page_size)
        .saturating_sub(META_FIXED_BYTES + META_SPILL_DESCRIPTOR_BYTES)
        / 8
}

/// How many compact 9-byte terminal outcomes fit one status-spill page.
pub fn status_spill_entries_per_page(page_size: u32) -> usize {
    crate::page::usable_bytes(page_size).saturating_sub(8) / 9
}

/// Read the tree roots from the meta page.
struct MetaRoots {
    index_root: PageId,
    catalog_root: PageId,
    frozen_xid: Xid,
    next_xid: Xid,
    /// Transactions the frozen watermark stepped over. They must keep reading
    /// as aborted once their WAL abort records are truncated.
    abort_exceptions: Vec<Xid>,
    /// Status-spill descriptor: head page of the chain (0 when there is no
    /// spill), number of chained pages, and total retained entries.
    spill_head: PageId,
    spill_pages: u64,
    spill_entries: u64,
    /// Whether the meta page has ever actually been written.
    ///
    /// A published superblock does not imply a written meta page: the
    /// superblock is published at creation so a reopen cannot allocate a second
    /// meta page, but the meta page's *contents* only become durable when
    /// something logs or checkpoints them.
    initialized: bool,
    /// The extension region failed validation and was read under the
    /// pre-durable-abort-exceptions layout (`accept_legacy_meta`). The caller
    /// must rewrite the meta page in the current layout and checkpoint before
    /// serving, so the acceptance happens exactly once.
    legacy: bool,
}

fn read_meta(pool: &Arc<BufferPool>, meta_page: PageId, accept_legacy: bool) -> Result<MetaRoots> {
    let guard = pool.get(meta_page)?;
    let page_size = guard.bytes().len();
    let body = &guard.bytes()[crate::page::PAGE_HEADER_BYTES..];
    if body[0..8] != META_MAGIC {
        // A freshly allocated meta page that was never written: both trees are
        // new. Distinguishing this from corruption matters — treating an unset
        // page as corrupt would make every first open fail.
        //
        // `initialized: false` is load-bearing, not informational. The caller
        // must treat this store as fresh and write the meta page durably;
        // deciding freshness from the superblock alone loses every root this
        // session creates. See `PagedStore::open`.
        return Ok(MetaRoots {
            index_root: 0,
            catalog_root: 0,
            frozen_xid: 1,
            next_xid: 1,
            abort_exceptions: Vec::new(),
            spill_head: 0,
            spill_pages: 0,
            spill_entries: 0,
            initialized: false,
            legacy: false,
        });
    }
    let next_xid = u64::from_le_bytes(body[32..40].try_into().unwrap());
    let (abort_exceptions, spill_head, spill_pages, spill_entries, legacy) =
        match read_meta_extension(body, page_size, meta_page, next_xid) {
            Ok((abort_exceptions, spill_head, spill_pages, spill_entries)) => (
                abort_exceptions,
                spill_head,
                spill_pages,
                spill_entries,
                false,
            ),
            // Pre-durable-abort-exceptions layout: those engines wrote only
            // the 40-byte prefix and left the extension region as whatever the
            // reused page frame held, so a validation failure there is the
            // expected signature of an old store — when the operator has said
            // so. An empty exception list is the faithful reading: without a
            // durable exception region those engines could not truncate WAL
            // past an unresolved abort, so no truncated abort ever depended on
            // these bytes.
            Err(_) if accept_legacy => (Vec::new(), 0, 0, 0, true),
            Err(PageError::MetaCorruption { page_id, reason }) => {
                return Err(PageError::MetaCorruption {
                    page_id,
                    reason: format!(
                        "{reason}; if this store was last written by an engine predating \
                         durable abort exceptions, the extension region was never valid — \
                         re-open with accept_legacy_meta (BICDB_ACCEPT_LEGACY_META=1) or run \
                         `bicdb store migrate-meta <db> --confirm` to transform it in place"
                    ),
                });
            }
            Err(error) => return Err(error),
        };
    Ok(MetaRoots {
        initialized: true,
        legacy,
        index_root: u64::from_le_bytes(body[8..16].try_into().unwrap()),
        catalog_root: u64::from_le_bytes(body[16..24].try_into().unwrap()),
        frozen_xid: u64::from_le_bytes(body[24..32].try_into().unwrap()),
        next_xid,
        abort_exceptions,
        spill_head,
        spill_pages,
        spill_entries,
    })
}

/// Parse and validate the meta page's post-prefix extension: the durable
/// abort-exception region and the status-spill descriptor. Split from
/// [`read_meta`] so a validation failure can be distinguished from a
/// well-formed page — that distinction is what `accept_legacy_meta` and the
/// `store migrate-meta` inspection hang off.
fn read_meta_extension(
    body: &[u8],
    page_size: usize,
    meta_page: PageId,
    next_xid: Xid,
) -> Result<(Vec<Xid>, PageId, u64, u64)> {
    let exception_count = usize::try_from(u64::from_le_bytes(body[40..48].try_into().unwrap()))
        .map_err(|_| PageError::MetaCorruption {
            page_id: meta_page,
            reason: "abort-exception count is not representable".to_string(),
        })?;
    let capacity = abort_exception_capacity(page_size as u32);
    if exception_count > capacity {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: format!(
                "{exception_count} abort exceptions exceed the {capacity}-entry meta-page region"
            ),
        });
    }
    let mut abort_exceptions = Vec::with_capacity(exception_count);
    let mut previous = 0_u64;
    for index in 0..exception_count {
        let offset = META_FIXED_BYTES + index * 8;
        let xid = u64::from_le_bytes(body[offset..offset + 8].try_into().unwrap());
        // Strictly ascending, non-zero, and assigned: anything else is a
        // damaged region, and a damaged exception list is a visibility hazard,
        // so it fails the open rather than being guessed at.
        if xid == 0 || xid <= previous || xid >= next_xid {
            return Err(PageError::MetaCorruption {
                page_id: meta_page,
                reason: format!("abort exception {index} is out of sequence or unassigned"),
            });
        }
        previous = xid;
        abort_exceptions.push(xid);
    }
    let usable = crate::page::usable_bytes(page_size as u32);
    let descriptor = usable - META_SPILL_DESCRIPTOR_BYTES;
    let spill_head = u64::from_le_bytes(body[descriptor..descriptor + 8].try_into().unwrap());
    let spill_pages = u64::from_le_bytes(body[descriptor + 8..descriptor + 16].try_into().unwrap());
    let spill_entries =
        u64::from_le_bytes(body[descriptor + 16..descriptor + 24].try_into().unwrap());
    // A descriptor is either empty or fully materialized: a head with no
    // pages, or pages with no head, cannot describe a real spill. Zero
    // entries with retained pages IS valid — an emptied spill keeps its
    // page chain for reuse rather than leaking it.
    if (spill_head == 0) != (spill_pages == 0) || (spill_entries > 0 && spill_head == 0) {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: "status-spill descriptor is internally inconsistent".to_string(),
        });
    }
    Ok((abort_exceptions, spill_head, spill_pages, spill_entries))
}

/// Raw pre-recovery view of the meta page for `bicdb store migrate-meta`.
///
/// This reads the on-disk page without replaying the WAL, so on a store with
/// pending log records it describes the pre-replay bytes — a report, not a
/// verdict. The authoritative check is simply whether a default open succeeds.
#[derive(Clone, Debug)]
pub struct MetaPageInspection {
    pub meta_page: PageId,
    /// The meta page carries the magic; false means a store that has never
    /// checkpointed (or is not a paged store at all).
    pub initialized: bool,
    pub next_xid: Xid,
    /// `None` when the extension region validates under the current layout;
    /// otherwise the validation failure — on a store last written by a
    /// pre-durable-abort-exceptions engine this is expected, not corruption.
    pub extension_error: Option<String>,
}

/// Result of an offline, fenced replacement of a damaged status-spill chain.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusSpillRepairReport {
    pub previous_head: PageId,
    pub previous_pages: u64,
    pub previous_entries: u64,
    pub replacement_head: PageId,
    pub replacement_pages: u64,
    pub replacement_entries: u64,
    pub first_xid: Option<Xid>,
    pub last_xid: Option<Xid>,
    pub applied: bool,
}

/// Replace a damaged status-spill chain from an independently reconstructed,
/// exact outcome list. The database must be offline. `expected_*` are fencing
/// tokens read from the corruption diagnostic; a changed descriptor refuses
/// the operation. Without `apply`, this validates only and writes nothing.
///
/// Replacement is leak-safe and ordered: new pages are written and synced,
/// their allocator state is published, and only then is the meta descriptor
/// changed. The damaged predecessor is deliberately not freed.
pub fn repair_status_spill(
    dir: &Path,
    page_size: u32,
    expected_head: PageId,
    expected_pages: u64,
    expected_entries: u64,
    outcomes: &[(Xid, TxStatus)],
    apply: bool,
) -> Result<StatusSpillRepairReport> {
    crate::page::validate_page_size(page_size)?;
    let _lock = DirectoryLock::acquire(dir)?;
    let paths = PagedPaths::in_dir(dir);
    let store = Arc::new(PageStore::open(
        &paths.pages,
        PageStoreOptions {
            page_size,
            fsync: true,
            create: false,
            extent_bytes: 0,
        },
    )?);
    let meta_page = store.root_page();
    if meta_page == 0 {
        return Err(PageError::MetaCorruption {
            page_id: 0,
            reason: "status-spill repair requires an initialized meta page".to_string(),
        });
    }
    let mut meta = vec![0_u8; page_size as usize];
    let header = store.read_page(meta_page, &mut meta)?;
    if header.page_type != crate::page::PageType::Heap {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: "status-spill repair found a non-heap meta page".to_string(),
        });
    }
    let body = &meta[crate::page::PAGE_HEADER_BYTES..];
    if body[0..8] != META_MAGIC {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: "status-spill repair found an uninitialized meta page".to_string(),
        });
    }
    let frozen_xid = u64::from_le_bytes(body[24..32].try_into().unwrap());
    let next_xid = u64::from_le_bytes(body[32..40].try_into().unwrap());
    let (_, current_head, current_pages, current_entries) =
        read_meta_extension(body, page_size as usize, meta_page, next_xid)?;
    if (current_head, current_pages, current_entries)
        != (expected_head, expected_pages, expected_entries)
    {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: format!(
                "status-spill repair fence changed: expected ({expected_head}, \
                 {expected_pages}, {expected_entries}), found ({current_head}, \
                 {current_pages}, {current_entries})"
            ),
        });
    }
    if outcomes.len() as u64 != expected_entries {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: format!(
                "replacement has {} outcomes, descriptor requires {expected_entries}",
                outcomes.len()
            ),
        });
    }
    let mut encoded = Vec::with_capacity(outcomes.len());
    let mut previous = 0_u64;
    for (index, (xid, status)) in outcomes.iter().copied().enumerate() {
        if xid < frozen_xid || xid >= next_xid || xid <= previous {
            return Err(PageError::MetaCorruption {
                page_id: meta_page,
                reason: format!(
                    "replacement outcome {index} has xid {xid}, outside the ascending assigned \
                     range [{frozen_xid}, {next_xid})"
                ),
            });
        }
        let status_byte = match status {
            TxStatus::Committed => RecoveredOutcome::COMMITTED,
            TxStatus::Aborted => RecoveredOutcome::ABORTED,
            TxStatus::InProgress => {
                return Err(PageError::MetaCorruption {
                    page_id: meta_page,
                    reason: format!("replacement outcome {index} is not terminal"),
                });
            }
        };
        let mut bytes = [0_u8; 9];
        bytes[..8].copy_from_slice(&xid.to_be_bytes());
        bytes[8] = status_byte;
        encoded.push(RecoveredOutcome::from_spill_bytes(bytes).unwrap());
        previous = xid;
    }
    let cap = status_spill_entries_per_page(page_size);
    let replacement_pages = encoded.len().div_ceil(cap) as u64;
    let mut report = StatusSpillRepairReport {
        previous_head: current_head,
        previous_pages: current_pages,
        previous_entries: current_entries,
        replacement_head: 0,
        replacement_pages,
        replacement_entries: encoded.len() as u64,
        first_xid: outcomes.first().map(|outcome| outcome.0),
        last_xid: outcomes.last().map(|outcome| outcome.0),
        applied: false,
    };
    if !apply {
        return Ok(report);
    }

    let mut page_ids = Vec::with_capacity(replacement_pages as usize);
    let build = (|| -> Result<()> {
        for _ in 0..replacement_pages {
            page_ids.push(store.allocate(crate::page::PageType::Heap)?);
        }
        let mut image = vec![0_u8; page_size as usize];
        for (index, chunk) in encoded.chunks(cap).enumerate() {
            let page_id = page_ids[index];
            let next_page = page_ids.get(index + 1).copied().unwrap_or(0);
            let mut page_header =
                crate::page::PageHeader::new(page_id, crate::page::PageType::Heap, page_size);
            page_header.next_page = next_page;
            page_header.encode(&mut image);
            let usable = crate::page::usable_bytes(page_size);
            let body_end = crate::page::PAGE_HEADER_BYTES + usable;
            image[crate::page::PAGE_HEADER_BYTES..body_end].fill(0);
            let page_body = &mut image[crate::page::PAGE_HEADER_BYTES..body_end];
            page_body[0..8].copy_from_slice(&(chunk.len() as u64).to_le_bytes());
            for (entry, outcome) in chunk.iter().enumerate() {
                let offset = 8 + entry * 9;
                page_body[offset..offset + 9].copy_from_slice(&outcome.to_spill_bytes());
            }
            store.write_page(page_id, &mut image)?;
        }
        store.sync()
    })();
    if let Err(error) = build {
        // Allocated pages may already have overwritten intrusive free-list
        // links. Publish the shortened allocator state even though the pages
        // now leak; retaining a leak is safer than reopening a corrupt list.
        store.flush()?;
        return Err(error);
    }
    // Publish the allocator state before any durable descriptor can name the
    // new pages. A crash before the descriptor now leaks them safely.
    store.flush()?;

    let replacement_head = page_ids.first().copied().unwrap_or(0);
    let usable = crate::page::usable_bytes(page_size);
    let descriptor = crate::page::PAGE_HEADER_BYTES + usable - META_SPILL_DESCRIPTOR_BYTES;
    meta[descriptor..descriptor + 8].copy_from_slice(&replacement_head.to_le_bytes());
    meta[descriptor + 8..descriptor + 16].copy_from_slice(&replacement_pages.to_le_bytes());
    meta[descriptor + 16..descriptor + 24].copy_from_slice(&(encoded.len() as u64).to_le_bytes());
    store.write_page(meta_page, &mut meta)?;
    store.sync()?;

    let verified = PagedOutcomeSpill::load(
        Arc::clone(&store),
        page_size,
        replacement_head,
        replacement_pages,
        encoded.len() as u64,
    )?;
    if verified.entry_count() != encoded.len() as u64 {
        return Err(PageError::MetaCorruption {
            page_id: meta_page,
            reason: "replacement status-spill readback count changed".to_string(),
        });
    }
    report.replacement_head = replacement_head;
    report.applied = true;
    Ok(report)
}

/// Read and validate an operator-selected status-spill chain without opening
/// the database or consulting its (possibly damaged) meta descriptor.
pub fn extract_status_spill(
    dir: &Path,
    page_size: u32,
    head: PageId,
    pages: u64,
    entries: u64,
) -> Result<Vec<(Xid, TxStatus)>> {
    crate::page::validate_page_size(page_size)?;
    let _lock = DirectoryLock::acquire(dir)?;
    let paths = PagedPaths::in_dir(dir);
    let store = Arc::new(PageStore::open(
        &paths.pages,
        PageStoreOptions {
            page_size,
            fsync: false,
            create: false,
            extent_bytes: 0,
        },
    )?);
    let spill = PagedOutcomeSpill::load(Arc::clone(&store), page_size, head, pages, entries)?;
    let state = spill.state.lock().clone();
    let mut outcomes = Vec::with_capacity(entries as usize);
    let mut buffer = vec![0_u8; page_size as usize];
    for page in state {
        store.read_page(page.page_id, &mut buffer)?;
        let usable = crate::page::usable_bytes(page_size);
        let body = &buffer[crate::page::PAGE_HEADER_BYTES..crate::page::PAGE_HEADER_BYTES + usable];
        let count = u64::from_le_bytes(body[0..8].try_into().unwrap()) as usize;
        for index in 0..count {
            let offset = 8 + index * 9;
            let bytes: [u8; 9] = body[offset..offset + 9].try_into().unwrap();
            let outcome = RecoveredOutcome::from_spill_bytes(bytes).ok_or_else(|| {
                PageError::MetaCorruption {
                    page_id: page.page_id,
                    reason: format!("status spill entry {index} has an unknown status byte"),
                }
            })?;
            outcomes.push((outcome.xid(), outcome.status()));
        }
    }
    Ok(outcomes)
}

/// Inspect a paged store's meta page without opening the store.
///
/// Takes the directory lock, so it refuses to run beside a live process, and
/// performs no writes.
pub fn inspect_meta_page(dir: &Path, page_size: u32) -> Result<MetaPageInspection> {
    let paths = PagedPaths::in_dir(dir);
    let _lock = DirectoryLock::acquire(dir)?;
    let store = PageStore::open(
        &paths.pages,
        PageStoreOptions {
            page_size,
            fsync: false,
            create: false,
            extent_bytes: 0,
        },
    )?;
    let meta_page = store.root_page();
    if meta_page == 0 {
        return Ok(MetaPageInspection {
            meta_page: 0,
            initialized: false,
            next_xid: 1,
            extension_error: None,
        });
    }
    let mut buffer = vec![0u8; page_size as usize];
    store.read_page(meta_page, &mut buffer)?;
    let body = &buffer[crate::page::PAGE_HEADER_BYTES..];
    if body[0..8] != META_MAGIC {
        return Ok(MetaPageInspection {
            meta_page,
            initialized: false,
            next_xid: 1,
            extension_error: None,
        });
    }
    let next_xid = u64::from_le_bytes(body[32..40].try_into().unwrap());
    let extension_error = read_meta_extension(body, page_size as usize, meta_page, next_xid)
        .err()
        .map(|error| error.to_string());
    Ok(MetaPageInspection {
        meta_page,
        initialized: true,
        next_xid,
        extension_error,
    })
}

/// One chained status-spill page as understood by the loader and the demand
/// lookup. Empty pages retain their chain slot so the region is reused
/// rather than leaked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SpillPageState {
    page_id: PageId,
    generation: u64,
    entries: u64,
    min_xid: Xid,
    max_xid: Xid,
}

/// Disk-backed terminal-outcome store for transactions above a pinned
/// watermark.
///
/// A long-running transaction keeps the frozen watermark low, and every
/// commit above it would otherwise remain resident — and retain the WAL —
/// until it ends. Checkpoints persist those outcomes here, evict them from
/// the transaction table, and truncate the WAL; visibility resolves an
/// evicted outcome with one demand read of its spill page.
///
/// # Copy-on-write publication
///
/// [`Self::persist`] never rewrites a page a reader can reach. It writes the
/// merged outcome set to freshly allocated pages, syncs them, and only then
/// swaps the in-memory chain under `state`. The pages the swap retires stay
/// on disk, untouched, until [`Self::reclaim_retired`] frees them — and that
/// is only called once the checkpoint has made the meta-page descriptor that
/// names the NEW chain durable. Every crash window therefore leaves either the
/// old descriptor with the old (unmodified) pages, or the new descriptor with
/// the new (already synced) pages; never a descriptor pointing at a page whose
/// contents were changed out from under it.
///
/// Spill pages are written directly to the page file and synced, the same way
/// the superblock is published: they are structural metadata, not transactional
/// data. They deliberately bypass the buffer pool — the pool cannot invalidate
/// a cached frame after a direct rewrite. Each page is nevertheless logged to
/// the WAL as a structural (transaction 0) after-image: the pages come off the
/// free list, so the log may hold an older Free image of the same page, and
/// replay applies images in LSN order — without the newer spill image, a crash
/// would replay the Free image over the chain the meta descriptor names.
///
/// # Reader concurrency
///
/// [`Self::lookup`] holds the `state` lock across the whole page read. A
/// concurrent `persist` needs that same lock to swap the chain, and
/// `reclaim_retired` needs it to drain the retired list, so a lookup can never
/// observe a half-swapped chain nor read a page that is being retired. The
/// cost is that demand reads serialize; they are rare (only outcomes above a
/// pinned watermark) and correctness is the priority.
#[derive(Debug)]
pub(crate) struct PagedOutcomeSpill {
    store: Arc<PageStore>,
    page_size: u32,
    /// The current chain, ascending. Readers resolve outcomes against it.
    state: Mutex<Vec<SpillPageState>>,
    /// Pages a `persist` retired by swapping them out of `state`. They remain
    /// valid on disk (copy-on-write never mutates them) until
    /// `reclaim_retired` frees them once the replacing descriptor is durable.
    retired: Mutex<Vec<SpillPageState>>,
    /// Demand reads that failed. A nonzero count means visibility fell back
    /// to the safe direction for the affected outcomes and the page file
    /// needs integrity attention.
    lookup_failures: AtomicU64,
}

/// Decode one spill page body: entry count plus ascending, unique outcomes.
fn parse_spill_page(buffer: &[u8], page_size: u32) -> Result<(u64, Xid, Xid)> {
    let usable = crate::page::usable_bytes(page_size);
    let body = &buffer[crate::page::PAGE_HEADER_BYTES..crate::page::PAGE_HEADER_BYTES + usable];
    let count = u64::from_le_bytes(body[0..8].try_into().unwrap());
    if count as usize > status_spill_entries_per_page(page_size) {
        return Err(PageError::MetaCorruption {
            page_id: 0,
            reason: format!("status spill page claims {count} entries"),
        });
    }
    let mut min_xid = 0_u64;
    let mut max_xid = 0_u64;
    let mut previous = 0_u64;
    for index in 0..count as usize {
        let offset = 8 + index * 9;
        let bytes: [u8; 9] = body[offset..offset + 9].try_into().unwrap();
        let outcome =
            RecoveredOutcome::from_spill_bytes(bytes).ok_or_else(|| PageError::MetaCorruption {
                page_id: 0,
                reason: format!("status spill entry {index} has an unknown status byte"),
            })?;
        let xid = outcome.xid();
        if xid == 0 || xid <= previous {
            return Err(PageError::MetaCorruption {
                page_id: 0,
                reason: format!("status spill entry {index} is out of sequence"),
            });
        }
        if index == 0 {
            min_xid = xid;
        }
        max_xid = xid;
        previous = xid;
    }
    Ok((count, min_xid, max_xid))
}

impl PagedOutcomeSpill {
    /// Load and validate the chain described by the meta-page descriptor.
    fn load(
        store: Arc<PageStore>,
        page_size: u32,
        head: PageId,
        pages: u64,
        entries: u64,
    ) -> Result<Arc<Self>> {
        let spill = Arc::new(Self {
            store,
            page_size,
            state: Mutex::new(Vec::new()),
            retired: Mutex::new(Vec::new()),
            lookup_failures: AtomicU64::new(0),
        });
        if head == 0 {
            return Ok(spill);
        }
        let mut state = spill.state.lock();
        let mut buffer = vec![0_u8; page_size as usize];
        let mut page_id = head;
        let mut total = 0_u64;
        let mut previous_max = 0_u64;
        let mut visited = BTreeSet::new();
        for index in 0..pages {
            if !visited.insert(page_id) {
                return Err(PageError::MetaCorruption {
                    page_id,
                    reason: format!("status spill chain repeats page {page_id} at index {index}"),
                });
            }
            let header = spill
                .store
                .read_page(page_id, &mut buffer)
                .map_err(|error| PageError::MetaCorruption {
                    page_id,
                    reason: format!("status spill page {index} is unreadable: {error}"),
                })?;
            if header.page_type != crate::page::PageType::Heap {
                return Err(PageError::MetaCorruption {
                    page_id,
                    reason: format!("status spill page {index} is not a heap page"),
                });
            }
            let (page_entries, min_xid, max_xid) =
                parse_spill_page(&buffer, page_size).map_err(|error| {
                    PageError::MetaCorruption {
                        page_id,
                        reason: format!("status spill page {index}: {error}"),
                    }
                })?;
            total = total.saturating_add(page_entries);
            if page_entries > 0 && min_xid <= previous_max {
                return Err(PageError::MetaCorruption {
                    page_id,
                    reason: format!("status spill page {index} overlaps an earlier page"),
                });
            }
            if page_entries > 0 {
                previous_max = max_xid;
            }
            state.push(SpillPageState {
                page_id,
                generation: header.generation,
                entries: page_entries,
                min_xid,
                max_xid,
            });
            if index + 1 < pages {
                page_id = header.next_page;
                if page_id == 0 {
                    return Err(PageError::MetaCorruption {
                        page_id,
                        reason: format!("status spill chain ended at page {index}"),
                    });
                }
            }
        }
        if total != entries {
            return Err(PageError::MetaCorruption {
                page_id: head,
                reason: format!("status spill holds {total} entries, descriptor claims {entries}"),
            });
        }
        drop(state);
        Ok(spill)
    }

    /// Meta-page descriptor: head page (0 when empty), chain length, entries.
    fn descriptor(&self) -> (PageId, u64, u64) {
        let state = self.state.lock();
        let head = state.first().map(|page| page.page_id).unwrap_or(0);
        let pages = state.len() as u64;
        let entries = state.iter().map(|page| page.entries).sum();
        (head, pages, entries)
    }

    fn entry_count(&self) -> u64 {
        self.state.lock().iter().map(|page| page.entries).sum()
    }

    fn page_count(&self) -> u64 {
        self.state.lock().len() as u64
    }

    fn lookup_failure_count(&self) -> u64 {
        self.lookup_failures.load(Ordering::Acquire)
    }

    /// Resolve `xid` against the durable chain. `Ok(None)` is an ordinary
    /// miss — the spill provably retains no record. `Err` means the spill
    /// could not answer (unreadable or corrupt page); the freeze path must
    /// propagate that rather than treat it as a miss, because it turns
    /// "no record" into a permanent abort exception.
    fn lookup_checked(&self, xid: Xid) -> Result<Option<TxStatus>> {
        // The chain lock is held across the whole page read. `persist`
        // needs this lock to swap the chain and `reclaim_retired` needs it to
        // drain retired pages, so holding it here guarantees the page selected
        // below is neither swapped out from under us nor freed mid-read.
        let state = self.state.lock();
        let non_empty = state.partition_point(|page| page.entries > 0);
        let index = state[..non_empty].partition_point(|page| page.min_xid <= xid);
        if index == 0 {
            return Ok(None);
        }
        let page = state[index - 1];
        if xid > page.max_xid {
            return Ok(None);
        }
        let page_id = page.page_id;
        let mut buffer = vec![0_u8; self.page_size as usize];
        self.store.read_page(page_id, &mut buffer)?;
        let usable = crate::page::usable_bytes(self.page_size);
        let body = &buffer[crate::page::PAGE_HEADER_BYTES..crate::page::PAGE_HEADER_BYTES + usable];
        let corrupt = |reason: String| PageError::MetaCorruption { page_id, reason };
        let count_bytes: [u8; 8] = body[0..8]
            .try_into()
            .map_err(|_| corrupt("status spill page is too small for a count".to_string()))?;
        let count = u64::from_le_bytes(count_bytes) as usize;
        if count > status_spill_entries_per_page(self.page_size) {
            return Err(corrupt(format!(
                "status spill page claims {count} entries, over capacity"
            )));
        }
        let entries = &body[8..8 + count * 9];
        let target = xid.to_be_bytes();
        // Lower bound over the 9-byte entries, comparing big-endian xid
        // prefixes byte-wise (identical to numeric order).
        let mut low = 0_usize;
        let mut high = count;
        while low < high {
            let mid = low + (high - low) / 2;
            if entries[mid * 9..mid * 9 + 8] < target[..] {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if low >= count || entries[low * 9..low * 9 + 8] != target[..] {
            // Absent from a perfectly readable page: an ordinary miss.
            return Ok(None);
        }
        let position = low;
        let bytes: [u8; 9] = entries[position * 9..position * 9 + 9]
            .try_into()
            .map_err(|_| corrupt("status spill entry is truncated".to_string()))?;
        match RecoveredOutcome::from_spill_bytes(bytes) {
            Some(outcome) => Ok(Some(outcome.status())),
            None => Err(corrupt(format!(
                "status spill entry for xid {xid} holds an invalid outcome"
            ))),
        }
    }

    /// Replace the durable outcome set with `outcomes` unioned with every
    /// retained entry at or above `frozen_xid`.
    ///
    /// Copy-on-write: the merged set is written to freshly allocated pages and
    /// synced, and only then is the in-memory chain swapped. The pages the
    /// swap retires are left untouched on disk and parked in `retired`; they
    /// are freed by [`Self::reclaim_retired`] once the caller has made the
    /// descriptor naming the new chain durable. The caller must hold the
    /// writer gate.
    fn persist(
        &self,
        pool: &BufferPool,
        wal: &Wal,
        frozen_xid: Xid,
        outcomes: Vec<RecoveredOutcome>,
    ) -> Result<()> {
        let cap = status_spill_entries_per_page(self.page_size);
        let old_pages = self.state.lock().clone();
        if old_pages.is_empty() && outcomes.is_empty() {
            return Ok(());
        }

        // Merge every retained old entry (>= frozen) with the new outcomes
        // into one ascending, xid-deduplicated list. Reading the old pages is
        // a read-only act: copy-on-write never mutates them.
        let mut merged: Vec<RecoveredOutcome> = Vec::new();
        let mut read_buffer = vec![0_u8; self.page_size as usize];
        for (index, page) in old_pages.iter().enumerate() {
            if page.entries == 0 {
                continue;
            }
            self.store
                .read_page(page.page_id, &mut read_buffer)
                .map_err(|error| PageError::MetaCorruption {
                    page_id: page.page_id,
                    reason: format!("status spill page {index} is unreadable: {error}"),
                })?;
            let usable = crate::page::usable_bytes(self.page_size);
            let body = &read_buffer
                [crate::page::PAGE_HEADER_BYTES..crate::page::PAGE_HEADER_BYTES + usable];
            let count = u64::from_le_bytes(body[0..8].try_into().unwrap()) as usize;
            for entry in 0..count {
                let offset = 8 + entry * 9;
                let bytes: [u8; 9] = body[offset..offset + 9].try_into().unwrap();
                if let Some(outcome) = RecoveredOutcome::from_spill_bytes(bytes) {
                    if outcome.xid() >= frozen_xid {
                        merged.push(outcome);
                    }
                }
            }
        }
        merged.extend(outcomes.iter().copied());
        merged.sort_unstable();
        // One terminal status per xid; a duplicate here would be a
        // contradiction, which recovery already rejects upstream.
        merged.dedup_by(|a, b| a.xid() == b.xid());

        // Paginate into freshly allocated pages. Old pages are not touched.
        // Any failure past the first allocation frees what was allocated
        // before propagating: an abandoned Heap-typed page is invisible to
        // the orphaned-free-page scan and would otherwise leak forever.
        let mut image = vec![0_u8; self.page_size as usize];
        let mut new_page_ids: Vec<PageId> = Vec::new();
        let chunk_count = merged.len().div_ceil(cap);
        let built = (|| -> Result<Vec<SpillPageState>> {
            for _ in 0..chunk_count {
                new_page_ids.push(self.store.allocate(crate::page::PageType::Heap)?);
            }
            let mut new_state: Vec<SpillPageState> = Vec::new();
            for (index, chunk) in merged.chunks(cap).enumerate() {
                let next_page = new_page_ids.get(index + 1).copied().unwrap_or(0);
                self.encode_spill_page(&mut image, new_page_ids[index], next_page, chunk);
                self.store.write_page(new_page_ids[index], &mut image)?;
                let generation =
                    crate::page::PageHeader::decode(&image, self.store.path())?.generation;
                // Log the spill page as a structural (transaction 0)
                // after-image. The raw write above bypasses the WAL, but this
                // page came off the free list, and the WAL may still hold a
                // Free-typed image of it from the vacuum that freed it.
                // Without this record, a crash after the descriptor becomes
                // durable replays that Free image over the spill bytes — in
                // LSN order — and the next open fails with MetaCorruption on
                // a descriptor naming a clobbered chain. Logging the spill
                // image afterwards wins on LSN, so replay reconverges on the
                // spill contents.
                wal.log_page_image(new_page_ids[index], 0, &image)?;
                new_state.push(SpillPageState {
                    page_id: new_page_ids[index],
                    generation,
                    entries: chunk.len() as u64,
                    min_xid: chunk.first().map(|outcome| outcome.xid()).unwrap_or(0),
                    max_xid: chunk.last().map(|outcome| outcome.xid()).unwrap_or(0),
                });
            }
            self.store.sync()?;
            Ok(new_state)
        })();
        let new_state = match built {
            Ok(new_state) => new_state,
            Err(error) => {
                for page_id in new_page_ids {
                    // Best effort: the propagated error already flags the
                    // store for attention, and a page that cannot be freed
                    // now is no worse off than before this cleanup existed.
                    let _ = pool.free_page_for_transaction(page_id, 0);
                }
                return Err(error);
            }
        };

        // Atomically publish the new chain and retire the old one. Retired
        // pages remain valid on disk until reclaimed after the replacing
        // descriptor is durable.
        {
            let mut state = self.state.lock();
            let mut retired = self.retired.lock();
            for page in &old_pages {
                if !retired.iter().any(|existing| {
                    existing.page_id == page.page_id && existing.generation == page.generation
                }) {
                    retired.push(*page);
                }
            }
            *state = new_state;
        }
        Ok(())
    }

    /// Free the pages earlier `persist` calls retired.
    ///
    /// Only safe once the descriptor naming the chain that replaced them is
    /// durable; the caller guarantees that by invoking this after the
    /// checkpoint completes. Freeing any earlier could leave a still-durable
    /// old descriptor pointing at pages the free list has handed back out.
    fn reclaim_retired(&self, meta_page: PageId, pool: &BufferPool) -> Result<()> {
        // Do not trust checkpoint control flow alone for this destructive
        // transition. Read the meta page through the page store (bypassing the
        // buffer pool) and prove that the durable descriptor names the current
        // chain before returning any predecessor page to the allocator. A
        // stale durable descriptor plus a reused retired page destroys the
        // only surviving commit/abort decisions after WAL truncation.
        let state = self.state.lock();
        let mut retired_guard = self.retired.lock();
        if retired_guard.is_empty() {
            return Ok(());
        }
        let mut meta = vec![0_u8; self.page_size as usize];
        let header = self.store.read_page(meta_page, &mut meta)?;
        if header.page_type != crate::page::PageType::Heap {
            return Err(PageError::MetaCorruption {
                page_id: meta_page,
                reason: "durable meta page is not a heap page before status-spill reclaim"
                    .to_string(),
            });
        }
        let body = &meta[crate::page::PAGE_HEADER_BYTES..];
        if body[0..8] != META_MAGIC {
            return Err(PageError::MetaCorruption {
                page_id: meta_page,
                reason: "durable meta page is uninitialized before status-spill reclaim"
                    .to_string(),
            });
        }
        let next_xid = u64::from_le_bytes(body[32..40].try_into().unwrap());
        let (_, durable_head, durable_pages, durable_entries) =
            read_meta_extension(body, self.page_size as usize, meta_page, next_xid)?;
        let current_head = state.first().map(|page| page.page_id).unwrap_or(0);
        let current_pages = state.len() as u64;
        let current_entries = state.iter().map(|page| page.entries).sum::<u64>();
        if (durable_head, durable_pages, durable_entries)
            != (current_head, current_pages, current_entries)
        {
            return Err(PageError::MetaCorruption {
                page_id: meta_page,
                reason: format!(
                    "refusing status-spill reclaim: durable descriptor ({durable_head}, \
                     {durable_pages}, {durable_entries}) does not name current chain \
                     ({current_head}, {current_pages}, {current_entries})"
                ),
            });
        }
        if let Some(page) = retired_guard.iter().find(|retired| {
            state
                .iter()
                .any(|current| current.page_id == retired.page_id)
        }) {
            return Err(PageError::MetaCorruption {
                page_id: meta_page,
                reason: format!(
                    "refusing status-spill reclaim: current page {} is also retired",
                    page.page_id
                ),
            });
        }
        drop(state);
        let mut retired = { std::mem::take(&mut *retired_guard) };
        drop(retired_guard);
        retired.sort_unstable_by_key(|page| (page.page_id, page.generation));
        retired.dedup_by_key(|page| (page.page_id, page.generation));
        for (index, retired_page) in retired.iter().enumerate() {
            let header = match self.store.read_header(retired_page.page_id) {
                Ok(header) => header,
                Err(error) => {
                    self.retired.lock().extend(retired[index..].iter().copied());
                    return Err(error);
                }
            };
            // Reclamation is allowed only for the exact physical generation
            // that the spill retired. A stale retry after the page ID has
            // been reused must never free its new owner (the classic ABA
            // failure). A Free image means an earlier attempt already did the
            // work; publishing it again would corrupt the intrusive list, so
            // cleanup is idempotently complete for this retired generation.
            if header.page_type == crate::page::PageType::Free
                || header.generation != retired_page.generation
            {
                continue;
            }
            if let Err(error) = pool.free_page_for_transaction(retired_page.page_id, 0) {
                if matches!(
                    error,
                    PageError::PageAlreadyFree { page_id }
                        if page_id == retired_page.page_id
                ) {
                    // The page may already be staged in the transaction-zero
                    // adoption batch. That batch is the same idempotent
                    // cleanup operation, so do not poison the checkpoint by
                    // trying to enqueue it twice.
                    continue;
                }
                // Requeue the unfreed tail (including the failed page) so a
                // later checkpoint retries. Dropping it would strand
                // Heap-typed pages the orphaned-free-page scan cannot adopt,
                // and the page file would grow monotonically across errors.
                self.retired.lock().extend(retired[index..].iter().copied());
                return Err(error);
            }
        }
        Ok(())
    }

    fn encode_spill_page(
        &self,
        image: &mut [u8],
        page_id: PageId,
        next_page: PageId,
        entries: &[RecoveredOutcome],
    ) {
        let mut header =
            crate::page::PageHeader::new(page_id, crate::page::PageType::Heap, self.page_size);
        header.next_page = next_page;
        header.encode(image);
        let usable = crate::page::usable_bytes(self.page_size);
        let body_end = crate::page::PAGE_HEADER_BYTES + usable;
        for byte in &mut image[crate::page::PAGE_HEADER_BYTES..body_end] {
            *byte = 0;
        }
        let body = &mut image[crate::page::PAGE_HEADER_BYTES..body_end];
        body[0..8].copy_from_slice(&(entries.len() as u64).to_le_bytes());
        for (index, outcome) in entries.iter().enumerate() {
            let offset = 8 + index * 9;
            body[offset..offset + 9].copy_from_slice(&outcome.to_spill_bytes());
        }
    }
}

impl crate::mvcc::OutcomeSpill for PagedOutcomeSpill {
    fn status_checked(&self, xid: Xid) -> Result<Option<TxStatus>> {
        self.lookup_checked(xid)
    }

    fn status(&self, xid: Xid) -> Option<TxStatus> {
        match self.lookup_checked(xid) {
            Ok(status) => status,
            Err(_) => {
                // Visibility falls back to the safe direction (in-progress,
                // hence invisible); the counter flags the page file for
                // integrity attention.
                self.lookup_failures.fetch_add(1, Ordering::AcqRel);
                None
            }
        }
    }
}

/// Makes a page recoverable before it is allowed to reach disk.
///
/// Logging here rather than only at commit is what lets the pool evict freely:
/// recovery filters page images by whether their transaction committed, so an
/// evicted-but-uncommitted page is written to the file yet never replayed.
///
/// # Known limitation
///
/// That leaves the classic **steal without undo** gap: an uncommitted page
/// evicted to the page file stays there, and this store has no UNDO log to roll
/// it back. In practice a crash before commit therefore leaves such a page's
/// bytes on disk, unreferenced by the index (whose own pages are governed by the
/// same rule). It is safe for the commit-or-crash model this prototype supports,
/// and it is exactly what the rest of Phase 2 — version chains and rollback —
/// has to close. Recorded here rather than left for someone to find.
#[derive(Debug)]
struct WalWritebackBarrier {
    wal: Arc<Wal>,
}

impl crate::pool::WritebackBarrier for WalWritebackBarrier {
    fn before_writeback(&self, page_id: PageId, transaction: u64, bytes: &[u8]) -> Result<()> {
        let lsn = self.wal.log_page_image(page_id, transaction, bytes)?;
        self.wal.sync_through(lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn options() -> PagedStoreOptions {
        PagedStoreOptions::default()
            .with_page_size(512)
            .with_buffer_pool_bytes(64 * 512)
            .with_fsync(false)
    }

    fn open(dir: &TempDir) -> PagedStore {
        PagedStore::open(dir.path(), options()).unwrap().0
    }

    /// The production follow-up: the floor advance refused (an abort wall
    /// exhausted the exception capacity), so the repair must be SURGICAL —
    /// one row, one field, expected-value guarded, everything else
    /// untouched.
    #[test]
    fn a_targeted_header_repair_heals_exactly_one_stamp() {
        const POISON: Xid = 0x1fb9_0011_1fe7;
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let setup = store.begin();
            store.put(setup, b"universal_admin/recent", b"v1").unwrap();
            store.put(setup, b"universal_admin/other", b"keep").unwrap();
            store.commit(setup).unwrap();

            // Fabricate the production shape: garbage in xmin of the head.
            let locator = store
                .index
                .get(b"universal_admin/recent")
                .unwrap()
                .and_then(|bytes| TupleLocator::decode(&bytes))
                .unwrap();
            let mut stored = store.heap.get(locator).unwrap();
            stored[0..8].copy_from_slice(&POISON.to_le_bytes());
            store.heap.update(locator, &stored).unwrap();
            assert!(store.get(b"universal_admin/recent").unwrap().is_none());

            // Mismatched expectations refuse without touching anything.
            let wrong = store
                .repair_version_header_stamp(
                    b"universal_admin/recent",
                    HeaderStampField::Xmin,
                    12345,
                )
                .unwrap_err();
            assert!(
                matches!(&wrong, PageError::HeaderRepairRefused { found, .. } if *found == POISON),
                "{wrong:?}"
            );
            // Wrong field: xmax is already the clean zero marker, so the
            // guarded repair is an idempotent no-op — nothing to mutate.
            assert_eq!(
                store
                    .repair_version_header_stamp(
                        b"universal_admin/recent",
                        HeaderStampField::Xmax,
                        POISON,
                    )
                    .unwrap(),
                HeaderRepair::AlreadyClean
            );

            // The authorized repair: exact field, exact stamp.
            assert_eq!(
                store
                    .repair_version_header_stamp(
                        b"universal_admin/recent",
                        HeaderStampField::Xmin,
                        POISON,
                    )
                    .unwrap(),
                HeaderRepair::Repaired
            );
            store.checkpoint().unwrap();

            // Visible again (structural committed), writable again, and a
            // re-run is a clean no-op.
            assert_eq!(
                store.get(b"universal_admin/recent").unwrap().as_deref(),
                Some(b"v1".as_ref())
            );
            // Re-running the exact command after the repair is a no-op.
            assert_eq!(
                store
                    .repair_version_header_stamp(
                        b"universal_admin/recent",
                        HeaderStampField::Xmin,
                        POISON,
                    )
                    .unwrap(),
                HeaderRepair::AlreadyClean
            );
            let writer = store.begin();
            store.put(writer, b"universal_admin/recent", b"v2").unwrap();
            store.commit(writer).unwrap();
            assert_eq!(
                store.get(b"universal_admin/recent").unwrap().as_deref(),
                Some(b"v2".as_ref())
            );
            // Once the worker has rewritten the row, its head carries a
            // legitimate stamp — the guarded repair refuses to touch it.
            let healthy = store
                .repair_version_header_stamp(
                    b"universal_admin/recent",
                    HeaderStampField::Xmin,
                    POISON,
                )
                .unwrap_err();
            assert!(
                matches!(&healthy, PageError::HeaderRepairRefused { found, .. } if *found != 0),
                "{healthy:?}"
            );
            assert_eq!(
                store.get(b"universal_admin/other").unwrap().as_deref(),
                Some(b"keep".as_ref())
            );
        }
        // Durable across reopen.
        let store = open(&dir);
        assert_eq!(
            store.get(b"universal_admin/recent").unwrap().as_deref(),
            Some(b"v2".as_ref())
        );
        let after = store.begin();
        store.put(after, b"universal_admin/recent", b"v3").unwrap();
        store.commit(after).unwrap();
    }

    /// A refused floor advance must leave the allocator untouched — the
    /// first field deployment raised it and THEN refused.
    #[test]
    fn a_refused_floor_advance_mutates_nothing() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let writer = store.begin();
        store.put(writer, b"k", b"v").unwrap();
        // `writer` is still in progress below the target: refusal expected.
        let next_before = store.transactions.next_xid();
        let refused = store.advance_transaction_floor(1_000_000).unwrap_err();
        assert!(matches!(refused, PageError::TransactionFloorRefused { .. }));
        assert_eq!(
            store.transactions.next_xid(),
            next_before,
            "a refused advance must not raise the allocator"
        );
        store.commit(writer).unwrap();
        store.advance_transaction_floor(1_000_000).unwrap();
        assert!(store.transactions.next_xid() >= 1_000_000);
    }

    /// The production corruption: a version header stamped with an id the
    /// allocator never issued (`0x1fb9_0011_1fe7` on a hand-rebuilt store).
    /// The row must fail writes with a named corruption error — not an
    /// anonymous eternal conflict — and heal completely, durably, once the
    /// transaction floor advances past the poison.
    #[test]
    fn a_future_stamped_row_names_its_corruption_and_heals_after_floor_advance() {
        const POISON: Xid = 0x1fb9_0011_1fe7;
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let setup = store.begin();
            store.put(setup, b"jobs/canonical-row", b"v1").unwrap();
            store.put(setup, b"jobs/other-row", b"other").unwrap();
            store.commit(setup).unwrap();

            // Fabricate the corruption exactly as observed: garbage in xmax
            // of the head version, in place (same size keeps the locator).
            let locator = store
                .index
                .get(b"jobs/canonical-row")
                .unwrap()
                .and_then(|bytes| TupleLocator::decode(&bytes))
                .unwrap();
            let mut stored = store.heap.get(locator).unwrap();
            VersionHeader::patch_xmax(&mut stored, POISON);
            store.heap.update(locator, &stored).unwrap();

            // The write now names the row and the corrupt field.
            let writer = store.begin();
            let error = store.put(writer, b"jobs/canonical-row", b"v2").unwrap_err();
            match &error {
                PageError::CorruptFutureTransactionId {
                    key,
                    field,
                    xid,
                    next_xid,
                } => {
                    assert_eq!(key, "jobs/canonical-row");
                    assert_eq!(*field, "xmax");
                    assert_eq!(*xid, POISON);
                    assert!(*next_xid < POISON);
                }
                other => panic!("expected corruption error, got {other:?}"),
            }
            assert!(error.to_string().contains("jobs/canonical-row"));

            // The floor refuses to advance over a still-running transaction.
            let refused = store.advance_transaction_floor(POISON + 1).unwrap_err();
            assert!(
                matches!(refused, PageError::TransactionFloorRefused { .. }),
                "{refused:?}"
            );
            store.abort(writer).unwrap();

            // Repair: allocator raised, real range frozen, watermark jumped,
            // checkpointed.
            let (frozen, next) = store.advance_transaction_floor(POISON + 1).unwrap();
            assert!(frozen >= POISON + 1);
            assert!(next >= POISON + 1);

            // The poisoned stamp now reads committed-ancient: the write goes
            // through. (A committed xmax means the head version reads as
            // deleted — the writer recreates the row, which is exactly how a
            // wedged bookkeeping row unwedges.)
            let healed = store.begin();
            assert!(store
                .get_as_of(&store.snapshot_of(healed), b"jobs/canonical-row")
                .unwrap()
                .is_none());
            store.put(healed, b"jobs/canonical-row", b"v2").unwrap();
            store.commit(healed).unwrap();
            assert_eq!(
                store.get(b"jobs/canonical-row").unwrap().as_deref(),
                Some(b"v2".as_ref())
            );
            // Unrelated rows never flinched.
            assert_eq!(
                store.get(b"jobs/other-row").unwrap().as_deref(),
                Some(b"other".as_ref())
            );
        }

        // Durability: the repaired watermarks survive reopen — the wedge
        // must not return after a restart.
        let store = open(&dir);
        assert!(store.transactions.frozen_xid() >= POISON + 1);
        assert!(store.transactions.next_xid() >= POISON + 1);
        assert_eq!(
            store.get(b"jobs/canonical-row").unwrap().as_deref(),
            Some(b"v2".as_ref())
        );
        let after = store.begin();
        store.put(after, b"jobs/canonical-row", b"v3").unwrap();
        store.commit(after).unwrap();
        assert_eq!(
            store.get(b"jobs/canonical-row").unwrap().as_deref(),
            Some(b"v3".as_ref())
        );
    }

    /// Garbage in xmin keeps the row invisible until the floor advance
    /// blesses it as committed-ancient — then it reappears and stays
    /// writable.
    #[test]
    fn a_future_xmin_row_becomes_visible_after_floor_advance() {
        const POISON: Xid = 0x1fb9_0011_2fe7;
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        store.put(setup, b"k", b"v1").unwrap();
        store.commit(setup).unwrap();

        let locator = store
            .index
            .get(b"k")
            .unwrap()
            .and_then(|bytes| TupleLocator::decode(&bytes))
            .unwrap();
        let mut stored = store.heap.get(locator).unwrap();
        // xmin is the first 8 bytes of the header.
        stored[0..8].copy_from_slice(&POISON.to_le_bytes());
        store.heap.update(locator, &stored).unwrap();

        // Invisible: xmin is in the far future.
        assert!(store.get(b"k").unwrap().is_none());
        let writer = store.begin();
        let error = store.put(writer, b"k", b"v2").unwrap_err();
        assert!(
            matches!(
                &error,
                PageError::CorruptFutureTransactionId { field, .. } if *field == "xmin"
            ),
            "{error:?}"
        );
        store.abort(writer).unwrap();

        store.advance_transaction_floor(POISON + 1).unwrap();
        assert_eq!(store.get(b"k").unwrap().as_deref(), Some(b"v1".as_ref()));
        let healed = store.begin();
        store.put(healed, b"k", b"v2").unwrap();
        store.commit(healed).unwrap();
        assert_eq!(store.get(b"k").unwrap().as_deref(), Some(b"v2".as_ref()));
    }

    #[test]
    fn paged_store_snapshot_is_scrape_safe_stable_and_strict() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let transaction = store.begin();
        for index in 0..200 {
            store
                .put(
                    transaction,
                    format!("key-{index:04}").as_bytes(),
                    vec![index as u8; 80].as_slice(),
                )
                .unwrap();
        }
        store.commit(transaction).unwrap();
        for index in 0..200 {
            assert!(store
                .get(format!("key-{index:04}").as_bytes())
                .unwrap()
                .is_some());
        }

        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.format_version, PAGED_STORE_SNAPSHOT_FORMAT_VERSION);
        assert_eq!(snapshot.page_size, 512);
        assert_eq!(snapshot.logical_page_bytes, snapshot.page_count * 512);
        assert!(snapshot.page_file_bytes >= snapshot.logical_page_bytes);
        assert_eq!(snapshot.free_bytes, snapshot.free_pages * 512);
        assert_eq!(
            snapshot.used_data_pages + snapshot.free_pages + 1,
            snapshot.page_count
        );
        assert_eq!(snapshot.buffer_pool.budget_bytes, 64 * 512);
        assert!(snapshot.buffer_pool.resident_bytes <= snapshot.buffer_pool.budget_bytes);
        assert!(snapshot.buffer_pool.hits + snapshot.buffer_pool.misses > 0);
        assert!(snapshot.buffer_pool_hit_ratio_basis_points() <= 10_000);
        assert!(snapshot.page_io.reads > 0);
        assert!(snapshot.page_io.writes > 0);
        assert_eq!(
            snapshot.page_io.page_types.total().reads,
            snapshot.page_io.reads
        );
        assert_eq!(
            snapshot.page_io.page_types.total().writes,
            snapshot.page_io.writes
        );
        assert_eq!(snapshot.page_io.read_latency.count, snapshot.page_io.reads);
        assert_eq!(
            snapshot.page_io.write_latency.count,
            snapshot.page_io.writes
        );
        assert!(snapshot.page_io.is_consistent());
        assert!(snapshot.wal_bytes > 0);
        assert!(snapshot.wal.records_appended > 0);
        assert!(snapshot.recovery.duration_nanos > 0);
        assert_eq!(snapshot.recovery.scan_passes, 2);
        assert!(snapshot.recovery.peak_record_bytes <= crate::wal::MAX_WAL_RECORD_BYTES as u64);

        let encoded = serde_json::to_vec(&snapshot).unwrap();
        assert_eq!(
            serde_json::from_slice::<PagedStoreSnapshot>(&encoded).unwrap(),
            snapshot
        );
        let mut forged = serde_json::to_value(snapshot).unwrap();
        forged["page_identity"] = serde_json::json!("unbounded-label");
        assert!(serde_json::from_value::<PagedStoreSnapshot>(forged).is_err());
        for section in ["buffer_pool", "page_io", "wal", "recovery"] {
            let mut forged = serde_json::to_value(snapshot).unwrap();
            forged[section]["future_or_forged_field"] = serde_json::json!(1);
            assert!(
                serde_json::from_value::<PagedStoreSnapshot>(forged).is_err(),
                "nested snapshot section {section} accepted an unknown field"
            );
        }
        for section in [
            "page_types",
            "read_latency",
            "write_latency",
            "sync_latency",
        ] {
            let mut forged = serde_json::to_value(snapshot).unwrap();
            forged["page_io"][section]["future_or_forged_field"] = serde_json::json!(1);
            assert!(
                serde_json::from_value::<PagedStoreSnapshot>(forged).is_err(),
                "page I/O snapshot section {section} accepted an unknown field"
            );
        }

        let durable = (
            snapshot.page_size,
            snapshot.page_count,
            snapshot.logical_page_bytes,
            snapshot.page_file_bytes,
            snapshot.free_pages,
            snapshot.wal_max_bytes,
        );
        drop(store);
        let reopened = open(&dir);
        let reopened = reopened.snapshot().unwrap();
        assert_eq!(
            (
                reopened.page_size,
                reopened.page_count,
                reopened.logical_page_bytes,
                reopened.page_file_bytes,
                reopened.free_pages,
                reopened.wal_max_bytes,
            ),
            durable
        );
    }

    #[test]
    fn invalid_paged_resource_envelopes_fail_before_creating_storage() {
        for invalid in [
            options().with_buffer_pool_bytes(511),
            options().with_wal_max_bytes(511),
            options().with_page_size(513),
            options().with_read_ahead_queue_pages(
                crate::pool::MAX_READ_AHEAD_QUEUE_PAGES.saturating_add(1),
            ),
        ] {
            let parent = TempDir::new().unwrap();
            let root = parent.path().join("must-not-exist");
            assert!(PagedStore::open(&root, invalid).is_err());
            assert!(
                !root.exists(),
                "invalid resource configuration partially initialized storage"
            );
        }
    }

    #[test]
    fn sustained_bulk_commits_keep_the_wal_hard_bounded() {
        // Each commit dirties more pages than one bounded writeback step
        // retires, which parks the automatic checkpoint in Finalize forever
        // and lets the WAL grow without bound unless the hard cap forces a
        // full checkpoint on the committing thread.
        let dir = TempDir::new().unwrap();
        let store = PagedStore::open(
            dir.path(),
            options()
                .with_buffer_pool_bytes(4_096 * 512)
                .with_wal_max_bytes(64 * 1024),
        )
        .unwrap()
        .0;
        // Literal rather than derived from WAL_BACKPRESSURE_FACTOR so a
        // regression in the factor cannot loosen this bound with it.
        let hard_cap = 256 * 1024u64;

        let rounds = 12usize;
        let per_round = 1_500usize;
        for round in 0..rounds {
            let transaction = store.begin();
            for index in 0..per_round {
                let key = format!("bulk-{:07}", round * per_round + index);
                let value = vec![round as u8; 300];
                store
                    .put(transaction, key.as_bytes(), value.as_slice())
                    .unwrap();
            }
            store.commit(transaction).unwrap();
            let wal_bytes = store.snapshot().unwrap().wal_bytes;
            assert!(
                wal_bytes <= hard_cap,
                "round {round}: WAL at {wal_bytes} bytes exceeds the {hard_cap}-byte hard cap"
            );
        }

        for probe in [0usize, per_round + 7, rounds * per_round - 1] {
            let key = format!("bulk-{probe:07}");
            let expected = vec![(probe / per_round) as u8; 300];
            assert_eq!(
                store.get(key.as_bytes()).unwrap().as_deref(),
                Some(expected.as_slice()),
                "key {probe} lost across hard-cap checkpoints"
            );
        }
    }

    #[test]
    fn random_point_reads_and_writes_round_trip() {
        // Half of the Phase 0 exit gate.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let transaction = store.begin();

        let count = 400usize;
        const STEP: usize = 137; // coprime with `count`
        let mut position = 0usize;
        for _ in 0..count {
            position = (position + STEP) % count;
            let key = format!("key-{position:06}");
            let value = format!("value-{position}-{}", "x".repeat(position % 40));
            store
                .put(transaction, key.as_bytes(), value.as_bytes())
                .unwrap();
        }
        store.commit(transaction).unwrap();

        for index in 0..count {
            let key = format!("key-{index:06}");
            let expected = format!("value-{index}-{}", "x".repeat(index % 40));
            assert_eq!(
                store.get(key.as_bytes()).unwrap().as_deref(),
                Some(expected.as_bytes()),
                "key {index} did not round trip"
            );
        }
        assert_eq!(store.len().unwrap(), count);
    }

    #[test]
    fn committed_data_survives_reopen() {
        // The other half of the gate: recovery through a stable page identifier.
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let transaction = store.begin();
            for index in 0..200 {
                store
                    .put(
                        transaction,
                        format!("k{index:05}").as_bytes(),
                        format!("v{index}").as_bytes(),
                    )
                    .unwrap();
            }
            store.commit(transaction).unwrap();
            store.checkpoint().unwrap();
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert_eq!(store.len().unwrap(), 200);
        for index in 0..200 {
            assert_eq!(
                store
                    .get(format!("k{index:05}").as_bytes())
                    .unwrap()
                    .as_deref(),
                Some(format!("v{index}").as_bytes())
            );
        }
        let _ = recovery;
    }

    #[test]
    fn data_committed_but_not_checkpointed_is_recovered_from_the_log() {
        // No checkpoint before the drop: the only record of these writes is the
        // WAL, so this is the path that proves recovery actually works.
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let transaction = store.begin();
            for index in 0..80 {
                store
                    .put(
                        transaction,
                        format!("k{index:05}").as_bytes(),
                        format!("v{index}").as_bytes(),
                    )
                    .unwrap();
            }
            store.commit(transaction).unwrap();
            // Deliberately no checkpoint, and no flush of the pool.
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert!(
            recovery.pages_replayed > 0,
            "recovery replayed nothing; the test is not exercising the log"
        );
        for index in 0..80 {
            assert_eq!(
                store
                    .get(format!("k{index:05}").as_bytes())
                    .unwrap()
                    .as_deref(),
                Some(format!("v{index}").as_bytes()),
                "key {index} lost — it was committed"
            );
        }
    }

    #[test]
    fn wal_recovery_does_not_scan_the_complete_page_file_for_free_pages() {
        let dir = TempDir::new().unwrap();
        let free_pages_before_recovery;
        let page_count_before_recovery;
        {
            let store = open(&dir);
            let page_store = store.pool.store();
            let mut detached_candidates = Vec::new();
            for index in 0..4_096 {
                let page = page_store.allocate(crate::page::PageType::Heap).unwrap();
                if (2_000..2_064).contains(&index) {
                    detached_candidates.push(page);
                }
            }
            for page in detached_candidates {
                page_store.free(page).unwrap();
            }
            store.checkpoint().unwrap();

            let transaction = store.begin();
            store
                .put(transaction, b"recovered-key", b"recovered-value")
                .unwrap();
            store.commit(transaction).unwrap();
            free_pages_before_recovery = page_store.free_page_count();
            page_count_before_recovery = page_store.page_count();
            assert!(free_pages_before_recovery > 0);
            // Deliberately no checkpoint: reopen must replay WAL page images.
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert!(recovery.pages_replayed > 0);
        assert!(recovery.free_pages_detached >= free_pages_before_recovery);
        assert_eq!(store.pool.store().free_page_count(), 0);
        assert_eq!(
            store.get(b"recovered-key").unwrap().as_deref(),
            Some(b"recovered-value".as_ref())
        );

        let reads = store.snapshot().unwrap().page_io.reads;
        assert!(
            reads < page_count_before_recovery / 8,
            "recovery performed {reads} page reads for a {page_count_before_recovery}-page store"
        );
    }

    #[test]
    fn clean_reopen_preserves_the_persisted_free_list() {
        let dir = TempDir::new().unwrap();
        let free_pages_before_reopen;
        {
            let store = open(&dir);
            let page_store = store.pool.store();
            let mut pages = Vec::new();
            for _ in 0..64 {
                pages.push(page_store.allocate(crate::page::PageType::Heap).unwrap());
            }
            for page in pages.into_iter().take(16) {
                page_store.free(page).unwrap();
            }
            store.checkpoint().unwrap();
            free_pages_before_reopen = page_store.free_page_count();
            assert_eq!(free_pages_before_reopen, 16);
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert_eq!(recovery.pages_replayed, 0);
        assert_eq!(recovery.free_pages_detached, 0);
        assert_eq!(
            store.pool.store().free_page_count(),
            free_pages_before_reopen
        );
    }

    #[test]
    fn updates_and_deletes_are_visible_after_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let transaction = store.begin();
            for index in 0..60 {
                store
                    .put(transaction, format!("k{index:04}").as_bytes(), b"original")
                    .unwrap();
            }
            store.commit(transaction).unwrap();

            let second = store.begin();
            for index in (0..60).step_by(2) {
                store
                    .put(
                        second,
                        format!("k{index:04}").as_bytes(),
                        b"updated-and-longer",
                    )
                    .unwrap();
            }
            for index in (1..60).step_by(6) {
                store
                    .delete(second, format!("k{index:04}").as_bytes())
                    .unwrap();
            }
            store.commit(second).unwrap();
            store.checkpoint().unwrap();
        }

        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for index in 0..60 {
            let found = store.get(format!("k{index:04}").as_bytes()).unwrap();
            if index % 6 == 1 {
                assert!(found.is_none(), "key {index} should have been deleted");
            } else if index % 2 == 0 {
                assert_eq!(found.as_deref(), Some(&b"updated-and-longer"[..]));
            } else {
                assert_eq!(found.as_deref(), Some(&b"original"[..]));
            }
        }
    }

    #[test]
    fn a_working_set_larger_than_the_pool_stays_within_budget() {
        // The premise of the whole roadmap: more data than cache, bounded RSS.
        let dir = TempDir::new().unwrap();
        let store = PagedStore::open(
            dir.path(),
            PagedStoreOptions::default()
                .with_page_size(512)
                .with_buffer_pool_bytes(16 * 512)
                .with_fsync(false),
        )
        .unwrap()
        .0;

        let transaction = store.begin();
        for index in 0..1_500 {
            store
                .put(
                    transaction,
                    format!("k{index:06}").as_bytes(),
                    format!("value-{index}-{}", "p".repeat(30)).as_bytes(),
                )
                .unwrap();
        }
        store.commit(transaction).unwrap();

        let snapshot = store.buffer_pool().snapshot();
        assert!(
            snapshot.resident_bytes <= snapshot.budget_bytes,
            "resident {} exceeded budget {}",
            snapshot.resident_bytes,
            snapshot.budget_bytes
        );

        // And every key is still readable, from disk.
        for index in (0..1_500).step_by(97) {
            assert!(store
                .get(format!("k{index:06}").as_bytes())
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn large_values_round_trip_through_overflow() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let transaction = store.begin();
        let big: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();

        store.put(transaction, b"wide", &big).unwrap();
        store.put(transaction, b"narrow", b"small").unwrap();
        store.commit(transaction).unwrap();

        assert_eq!(store.get(b"wide").unwrap().unwrap(), big);
        assert_eq!(
            store.get(b"narrow").unwrap().as_deref(),
            Some(&b"small"[..])
        );
    }

    #[test]
    fn checkpointing_bounds_the_log() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let transaction = store.begin();
        for index in 0..300 {
            store
                .put(transaction, format!("k{index:05}").as_bytes(), b"payload")
                .unwrap();
        }
        store.commit(transaction).unwrap();
        assert!(store.wal().size_bytes() > 0);

        store.checkpoint().unwrap();
        assert_eq!(
            store.wal().size_bytes(),
            0,
            "checkpoint did not bound the log"
        );

        // Data still readable after the log was discarded.
        for index in (0..300).step_by(37) {
            assert!(store
                .get(format!("k{index:05}").as_bytes())
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn open_does_not_scan_the_file() {
        // The Phase 3 requirement, and gate 4 of the PubMed plan: "restart
        // without scanning every record". Before the durable catalog, open read
        // every page header to rebuild the heap page list, so this ratio grew
        // without bound.
        //
        // Asserted as a RATIO between two database sizes rather than an absolute
        // count: the absolute number depends on tree height and page size, but
        // if open were still scanning, ten times the data would mean ten times
        // the reads.
        fn reads_to_open(rows: usize) -> (u64, u64) {
            let dir = TempDir::new().unwrap();
            {
                let store = open(&dir);
                let transaction = store.begin();
                for index in 0..rows {
                    store
                        .put(
                            transaction,
                            format!("k{index:06}").as_bytes(),
                            format!("value-{index}-{}", "z".repeat(60)).as_bytes(),
                        )
                        .unwrap();
                }
                store.commit(transaction).unwrap();
                store.checkpoint().unwrap();
            }

            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            let page_store = store.buffer_pool().store();
            (page_store.metrics.snapshot().reads, page_store.page_count())
        }

        let (small_reads, small_pages) = reads_to_open(100);
        let (large_reads, large_pages) = reads_to_open(2_000);

        assert!(
            large_pages > small_pages * 5,
            "the large database is not actually larger: {small_pages} vs {large_pages} pages"
        );
        assert!(
            large_reads < small_reads * 3,
            "open reads grew with the file ({small_reads} -> {large_reads} for \
             {small_pages} -> {large_pages} pages); it is still scanning"
        );
    }

    #[test]
    fn free_space_is_reused_even_after_many_other_pages_are_touched() {
        // Two properties at once, and the order matters under MVCC:
        //
        // 1. a delete does NOT free space — it marks the version deleted, so
        //    readers on older snapshots keep seeing it. Space comes back only
        //    from vacuum. (This test predates MVCC and used to assume otherwise.)
        // 2. once vacuum has reclaimed it, the DURABLE catalog finds that space
        //    again even on pages long evicted from any recency ring — which the
        //    old bounded in-memory ring could not.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);

        let transaction = store.begin();
        for index in 0..600 {
            store
                .put(
                    transaction,
                    format!("k{index:05}").as_bytes(),
                    &vec![b'x'; 60],
                )
                .unwrap();
        }
        store.commit(transaction).unwrap();

        // Empty out the earliest rows — pages long since evicted from any
        // recency-based candidate ring.
        let second = store.begin();
        for index in 0..200 {
            store
                .delete(second, format!("k{index:05}").as_bytes())
                .unwrap();
        }
        store.commit(second).unwrap();

        // Nothing is reclaimable while the deleting transaction is the newest
        // thing around, so vacuum after committing it.
        let vacuumed = store.vacuum(10_000).unwrap();
        assert!(
            vacuumed.versions_reclaimed > 0,
            "vacuum reclaimed nothing after 200 committed deletes"
        );

        let pages_before = store.buffer_pool().store().page_count();
        let third = store.begin();
        for index in 1_000..1_180 {
            store
                .put(third, format!("k{index:05}").as_bytes(), &vec![b'y'; 60])
                .unwrap();
        }
        store.commit(third).unwrap();
        let pages_after = store.buffer_pool().store().page_count();

        assert!(
            pages_after < pages_before + 40,
            "refilling freed space grew the file from {pages_before} to {pages_after} pages; \
             the free-space map is not finding old holes"
        );
    }

    #[test]
    fn an_aborted_transaction_leaves_no_trace_in_reads() {
        // Rollback without an UNDO log: the version is on disk and unreachable.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);

        let first = store.begin();
        store.put(first, b"k", b"committed").unwrap();
        store.commit(first).unwrap();

        let doomed = store.begin();
        store.put(doomed, b"k", b"rolled-back").unwrap();
        store.put(doomed, b"new-key", b"also-rolled-back").unwrap();
        store.abort(doomed).unwrap();

        assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"committed"[..]));
        assert_eq!(
            store.get(b"new-key").unwrap(),
            None,
            "an aborted insert stayed visible"
        );
    }

    #[test]
    fn a_snapshot_does_not_see_writes_committed_after_it_began() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);

        let setup = store.begin();
        store.put(setup, b"k", b"original").unwrap();
        store.commit(setup).unwrap();

        // Reader takes its snapshot here.
        let (_reader, snapshot) = store.begin_transaction();

        let writer = store.begin();
        store.put(writer, b"k", b"changed").unwrap();
        store.commit(writer).unwrap();

        assert_eq!(
            store.get_as_of(&snapshot, b"k").unwrap().as_deref(),
            Some(&b"original"[..]),
            "snapshot isolation violated: the reader saw a later commit"
        );
        // While a fresh read does see it.
        assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"changed"[..]));
    }

    #[test]
    fn a_snapshot_still_sees_a_row_deleted_after_it_began() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);

        let setup = store.begin();
        store.put(setup, b"k", b"present").unwrap();
        store.commit(setup).unwrap();

        let (_reader, snapshot) = store.begin_transaction();

        let deleter = store.begin();
        assert!(store.delete(deleter, b"k").unwrap());
        store.commit(deleter).unwrap();

        assert_eq!(
            store.get_as_of(&snapshot, b"k").unwrap().as_deref(),
            Some(&b"present"[..]),
            "a later delete was visible to an older snapshot"
        );
        assert_eq!(store.get(b"k").unwrap(), None);
    }

    #[test]
    fn a_transaction_reads_its_own_writes_before_committing() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let (xid, snapshot) = store.begin_transaction();
        store.put(xid, b"k", b"mine").unwrap();
        assert_eq!(
            store.get_as_of(&snapshot, b"k").unwrap().as_deref(),
            Some(&b"mine"[..])
        );
        // But nobody else can see it yet.
        let (_, other) = store.begin_transaction();
        assert_eq!(store.get_as_of(&other, b"k").unwrap(), None);
    }

    #[test]
    fn repeated_updates_build_a_chain_that_vacuum_collapses() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        for round in 0..25 {
            let xid = store.begin();
            store
                .put(xid, b"k", format!("version-{round}").as_bytes())
                .unwrap();
            store.commit(xid).unwrap();
        }
        assert_eq!(
            store.get(b"k").unwrap().as_deref(),
            Some(&b"version-24"[..])
        );

        let report = store.vacuum(10_000).unwrap();
        assert!(
            report.versions_reclaimed >= 20,
            "vacuum left {} of 25 versions behind",
            25 - report.versions_reclaimed
        );
        // The live version survives.
        assert_eq!(
            store.get(b"k").unwrap().as_deref(),
            Some(&b"version-24"[..]),
            "vacuum reclaimed the live version"
        );
    }

    #[test]
    fn vacuum_respects_the_oldest_live_snapshot() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);

        let setup = store.begin();
        store.put(setup, b"k", b"original").unwrap();
        store.commit(setup).unwrap();

        // An open transaction holds the boundary back.
        let (_holder, snapshot) = store.begin_transaction();

        let writer = store.begin();
        store.put(writer, b"k", b"newer").unwrap();
        store.commit(writer).unwrap();

        store.vacuum(10_000).unwrap();
        assert_eq!(
            store.get_as_of(&snapshot, b"k").unwrap().as_deref(),
            Some(&b"original"[..]),
            "vacuum reclaimed a version an open snapshot still needed"
        );
    }

    #[test]
    fn vacuum_is_bounded_by_its_page_budget() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        for index in 0..600 {
            store
                .put(xid, format!("k{index:05}").as_bytes(), &vec![b'v'; 60])
                .unwrap();
        }
        store.commit(xid).unwrap();

        let report = store.vacuum(3).unwrap();
        assert_eq!(report.pages_scanned, 3);
        assert!(
            report.stopped_early,
            "vacuum did not report stopping at its budget"
        );
        assert_eq!(report.stop_reason, VacuumStopReason::PageLimit);
        assert!(report.next_cursor.next_page_id.is_some());
        assert!(!report.complete);
    }

    #[test]
    fn vacuum_rejects_zero_oversized_and_undersized_envelopes() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        for limits in [
            VacuumLimits {
                max_pages: 0,
                ..VacuumLimits::default()
            },
            VacuumLimits {
                max_pages: MAX_VACUUM_PAGES_PER_STEP + 1,
                ..VacuumLimits::default()
            },
            VacuumLimits {
                max_bytes: 512,
                ..VacuumLimits::default()
            },
            VacuumLimits {
                max_bytes: MAX_VACUUM_BYTES_PER_STEP + 1,
                ..VacuumLimits::default()
            },
            VacuumLimits {
                max_duration_millis: 0,
                ..VacuumLimits::default()
            },
            VacuumLimits {
                max_duration_millis: MAX_VACUUM_DURATION_MILLIS_PER_STEP + 1,
                ..VacuumLimits::default()
            },
        ] {
            assert!(matches!(
                store.vacuum_step(VacuumCursor::default(), limits),
                Err(PageError::InvalidMaintenanceLimits { .. })
            ));
        }
        assert!(
            serde_json::from_str::<VacuumCursor>(r#"{"next_page_id":1,"forged":"field"}"#).is_err()
        );
    }

    #[test]
    fn bounded_vacuum_cursor_advances_without_materializing_the_catalog() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        for index in 0..600 {
            store
                .put(xid, format!("k{index:05}").as_bytes(), &vec![b'v'; 60])
                .unwrap();
        }
        store.commit(xid).unwrap();

        let limits = VacuumLimits {
            max_pages: 3,
            max_bytes: MAX_VACUUM_BYTES_PER_STEP,
            max_duration_millis: MAX_VACUUM_DURATION_MILLIS_PER_STEP,
        };
        let mut cursor = VacuumCursor::default();
        let mut previous = None;
        let mut pages_scanned = 0_u64;
        for _ in 0..1_000 {
            let report = store.vacuum_step(cursor, limits).unwrap();
            assert!(report.pages_scanned <= limits.max_pages);
            pages_scanned += report.pages_scanned;
            if report.complete {
                assert_eq!(report.stop_reason, VacuumStopReason::Complete);
                assert_eq!(report.next_cursor, VacuumCursor::default());
                break;
            }
            let next = report.next_cursor.next_page_id.unwrap();
            if let Some(previous) = previous {
                assert!(
                    next > previous,
                    "vacuum cursor repeated/regressed from {previous} to {next}"
                );
            }
            previous = Some(next);
            cursor = report.next_cursor;
        }
        assert!(pages_scanned > limits.max_pages);
        assert!(
            pages_scanned < 1_000,
            "cursor did not converge over a small store"
        );
    }

    #[test]
    fn a_tight_byte_budget_still_reclaims_a_wide_dead_value() {
        // Previously this asserted the OPPOSITE: that a tight budget left the
        // wide value unreclaimed for a later, larger step. That is the
        // mid-page interruption a page-granular cursor cannot resume from,
        // and it is what looped forever. A page is now atomic: the budget
        // bounds how many pages a step takes, never whether a page finishes.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        store.put(xid, b"wide", &vec![b'x'; 100_000]).unwrap();
        store.commit(xid).unwrap();
        let deleting = store.begin();
        assert!(store.delete(deleting, b"wide").unwrap());
        store.commit(deleting).unwrap();

        let page_size = u64::from(store.buffer_pool().page_size());
        let tight = VacuumLimits {
            max_pages: 10,
            max_bytes: 4 * page_size,
            max_duration_millis: 10_000,
        };
        let step = store.vacuum_step(VacuumCursor::default(), tight).unwrap();

        // Not SKIPPED: the value is gone after one step.
        assert!(
            step.versions_reclaimed > 0,
            "a tight budget skipped the wide dead value instead of finishing its page"
        );
        assert_eq!(store.get(b"wide").unwrap(), None);
        // And the sweep moved on: either it finished outright or the cursor
        // advanced past the page. Standing still is the failure.
        assert!(
            step.complete || step.start_page != step.end_page,
            "the sweep neither completed nor advanced past {:?}",
            step.start_page
        );

        // Overshoot is BOUNDED by the work that page actually required.
        // Releasing a 100 KB value costs roughly 100 KB of overflow-page
        // release however the budget is set — the point is that the excess is
        // explained by real work and does not grow without limit.
        assert!(
            step.bytes_examined < 100_000 * 2 + 8 * page_size,
            "bytes_examined {} exceeds what releasing a 100 KB value can explain",
            step.bytes_examined
        );
    }

    #[test]
    fn a_time_limit_stops_between_pages_and_names_the_page_to_retry() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        // Enough data to span several heap pages, so the clock can be checked
        // between them.
        for index in 0..200 {
            store
                .put(xid, format!("k{index:03}").as_bytes(), &vec![b'v'; 4096])
                .unwrap();
        }
        store.commit(xid).unwrap();

        let mut checks = 0_u64;
        let timed = store
            .vacuum_step_with_expiry(
                VacuumCursor::default(),
                VacuumLimits {
                    max_pages: 100,
                    max_bytes: 1024 * 1024,
                    max_duration_millis: 1,
                },
                || {
                    checks += 1;
                    true
                },
            )
            .unwrap();
        assert_eq!(timed.stop_reason, VacuumStopReason::TimeLimit);
        assert!(!timed.complete);
        // The clock is only consulted BETWEEN pages, and never before the
        // first one — a step always makes progress.
        assert_eq!(timed.pages_scanned, 1);
        assert_ne!(timed.start_page, timed.end_page);

        let retry_page = timed.next_cursor.next_page_id.unwrap();
        let resumed = store
            .vacuum_step(
                timed.next_cursor,
                VacuumLimits {
                    max_pages: 10_000,
                    max_bytes: 1024 * 1024,
                    max_duration_millis: 10_000,
                },
            )
            .unwrap();
        assert!(resumed.pages_scanned > 0);
        assert!(
            resumed.next_cursor.next_page_id.unwrap_or(u64::MAX) > retry_page || resumed.complete
        );
    }

    #[test]
    fn transaction_status_stays_bounded_across_many_transactions() {
        // The roadmap's "keep only active transaction state and recent commit
        // status resident". Without freezing this grows forever.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        for index in 0..400 {
            let xid = store.begin();
            store
                .put(xid, format!("k{index:04}").as_bytes(), b"v")
                .unwrap();
            store.commit(xid).unwrap();
        }
        let before = store.transactions().resident_entries();
        store.checkpoint().unwrap();
        let after = store.transactions().resident_entries();

        assert!(
            after < before / 4,
            "checkpoint did not bound transaction status: {before} -> {after}"
        );
        // And the data is still visible through the frozen watermark.
        assert_eq!(store.get(b"k0000").unwrap().as_deref(), Some(&b"v"[..]));
        assert_eq!(store.get(b"k0399").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn concurrent_writers_to_one_row_conflict_rather_than_losing_an_update() {
        // Without this check the second writer overwrites the first and one
        // committed update vanishes with no error — the classic lost update.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);

        let setup = store.begin();
        store.put(setup, b"k", b"original").unwrap();
        store.commit(setup).unwrap();

        let first = store.begin();
        let second = store.begin();

        store.put(first, b"k", b"first-wins").unwrap();
        store.commit(first).unwrap();

        let error = store.put(second, b"k", b"second-loses").unwrap_err();
        assert!(
            error.is_write_conflict(),
            "expected a write conflict, got {error:?}"
        );
        store.abort(second).unwrap();

        assert_eq!(
            store.get(b"k").unwrap().as_deref(),
            Some(&b"first-wins"[..]),
            "the first writer's committed update was lost"
        );
    }

    #[test]
    fn an_in_flight_writer_blocks_another_writer_on_the_same_row() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        store.put(setup, b"k", b"v").unwrap();
        store.commit(setup).unwrap();

        let holder = store.begin();
        store.put(holder, b"k", b"being-written").unwrap();

        let other = store.begin();
        assert!(
            store
                .put(other, b"k", b"clash")
                .unwrap_err()
                .is_write_conflict(),
            "a second writer was allowed onto a row mid-write"
        );
    }

    #[test]
    fn a_writer_may_update_the_same_row_repeatedly() {
        // Self-conflict would make any read-modify-write transaction impossible.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        store.put(xid, b"k", b"one").unwrap();
        store.put(xid, b"k", b"two").unwrap();
        store.put(xid, b"k", b"three").unwrap();
        store.commit(xid).unwrap();
        assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"three"[..]));
    }

    #[test]
    fn a_row_written_by_an_aborted_transaction_is_writable_again() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        store.put(setup, b"k", b"v").unwrap();
        store.commit(setup).unwrap();

        let doomed = store.begin();
        store.put(doomed, b"k", b"never").unwrap();
        store.abort(doomed).unwrap();

        // The abort released the row; a later writer must not be blocked by a
        // transaction that will never commit.
        let next = store.begin();
        store.put(next, b"k", b"after-abort").unwrap();
        store.commit(next).unwrap();
        assert_eq!(
            store.get(b"k").unwrap().as_deref(),
            Some(&b"after-abort"[..])
        );
    }

    #[test]
    fn concurrent_deletes_of_one_row_conflict() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        store.put(setup, b"k", b"v").unwrap();
        store.commit(setup).unwrap();

        let first = store.begin();
        let second = store.begin();
        assert!(store.delete(first, b"k").unwrap());
        store.commit(first).unwrap();

        match store.delete(second, b"k") {
            Err(error) => assert!(error.is_write_conflict(), "unexpected: {error:?}"),
            // Also acceptable: the row is already invisible to this snapshot, so
            // there is nothing to delete. What must NOT happen is a silent
            // second delete succeeding against a concurrently deleted row.
            Ok(deleted) => assert!(!deleted),
        }
    }

    #[test]
    fn writers_to_different_rows_do_not_conflict() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let a = store.begin();
        let b = store.begin();
        store.put(a, b"key-a", b"va").unwrap();
        store.put(b, b"key-b", b"vb").unwrap();
        store.commit(a).unwrap();
        store.commit(b).unwrap();

        assert_eq!(store.get(b"key-a").unwrap().as_deref(), Some(&b"va"[..]));
        assert_eq!(store.get(b"key-b").unwrap().as_deref(), Some(&b"vb"[..]));
    }

    #[test]
    fn a_scan_returns_every_visible_row_in_key_order() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        for index in 0..300 {
            store
                .put(
                    xid,
                    format!("k{index:05}").as_bytes(),
                    format!("v{index}").as_bytes(),
                )
                .unwrap();
        }
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        let rows: Vec<(Vec<u8>, Vec<u8>)> = store
            .scan(&snapshot)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();

        assert_eq!(rows.len(), 300);
        let keys: Vec<&[u8]> = rows.iter().map(|(k, _)| k.as_slice()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "scan was not in key order");
        assert_eq!(rows[0].1, b"v0");
    }

    #[test]
    fn a_scan_skips_rows_the_snapshot_cannot_see() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        for index in 0..50 {
            store
                .put(setup, format!("k{index:04}").as_bytes(), b"v")
                .unwrap();
        }
        store.commit(setup).unwrap();

        let deleter = store.begin();
        for index in (0..50).step_by(5) {
            store
                .delete(deleter, format!("k{index:04}").as_bytes())
                .unwrap();
        }
        store.commit(deleter).unwrap();

        let rows: Vec<_> = store
            .scan(&store.latest_snapshot())
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();
        assert_eq!(
            rows.len(),
            40,
            "scan returned deleted rows, or dropped live ones"
        );
    }

    #[test]
    fn get_locality_batch_matches_point_gets_across_visibility() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        for index in 0..300u32 {
            store
                .put(
                    setup,
                    format!("k{index:04}").as_bytes(),
                    &vec![index as u8; 512],
                )
                .unwrap();
        }
        store.commit(setup).unwrap();
        let churn = store.begin();
        store.put(churn, b"k0007", &[b'n'; 2_048]).unwrap();
        store.delete(churn, b"k0011").unwrap();
        store.commit(churn).unwrap();

        let snapshot = store.latest_snapshot();
        let keys: Vec<Vec<u8>> = ["k0000", "k0299", "k0007", "k0011", "missing", "k0100"]
            .iter()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        let batch = store.get_locality_batch(&snapshot, &keys).unwrap();
        for (key, batched) in keys.iter().zip(&batch) {
            let single = store.get_as_of(&snapshot, key).unwrap();
            assert_eq!(&single, batched, "key {:?}", String::from_utf8_lossy(key));
        }
        assert_eq!(batch[2].as_ref().map(Vec::len), Some(2_048));
        assert!(batch[3].is_none());
        assert!(batch[4].is_none());
    }

    #[test]
    fn visible_key_prefix_scan_filters_deletes_and_bounds_values() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        store.put(setup, b"alpha", &[b'a'; 4_096]).unwrap();
        store.put(setup, b"beta", &[b'b'; 4_096]).unwrap();
        store.commit(setup).unwrap();

        let update = store.begin();
        store.put(update, b"alpha", &[b'n'; 4_096]).unwrap();
        store.delete(update, b"beta").unwrap();
        store.commit(update).unwrap();

        let rows = store
            .scan_visible_key_prefixes_from(&store.latest_snapshot(), b"", 16)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, vec![(b"alpha".to_vec(), vec![b'n'; 16])]);
    }

    #[test]
    fn a_scan_is_stable_against_concurrent_writes() {
        // What an integrity pass needs: one consistent state, not a smear of
        // whatever committed while it ran.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        for index in 0..100 {
            store
                .put(setup, format!("k{index:04}").as_bytes(), b"before")
                .unwrap();
        }
        store.commit(setup).unwrap();

        let (_reader, snapshot) = store.begin_transaction();

        let writer = store.begin();
        for index in 0..100 {
            store
                .put(writer, format!("k{index:04}").as_bytes(), b"after")
                .unwrap();
        }
        store.put(writer, b"zzz-new", b"added").unwrap();
        store.commit(writer).unwrap();

        let rows: Vec<_> = store
            .scan(&snapshot)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();
        assert_eq!(rows.len(), 100, "scan saw a row added after its snapshot");
        assert!(
            rows.iter().all(|(_, value)| value == b"before"),
            "scan saw updates committed after its snapshot"
        );
    }

    #[test]
    fn a_scan_stays_within_the_buffer_pool_budget() {
        let dir = TempDir::new().unwrap();
        let store = PagedStore::open(
            dir.path(),
            PagedStoreOptions::default()
                .with_page_size(512)
                .with_buffer_pool_bytes(16 * 512)
                .with_fsync(false),
        )
        .unwrap()
        .0;
        let xid = store.begin();
        for index in 0..1_200 {
            store
                .put(xid, format!("k{index:06}").as_bytes(), &vec![b'p'; 40])
                .unwrap();
        }
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        let mut seen = 0;
        for entry in store.scan(&snapshot).unwrap() {
            entry.unwrap();
            seen += 1;
            let pool = store.buffer_pool().snapshot();
            assert!(
                pool.resident_bytes <= pool.budget_bytes,
                "scan exceeded the pool budget at row {seen}"
            );
        }
        assert_eq!(seen, 1_200);
    }

    #[test]
    fn scan_from_starts_at_the_requested_key() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let xid = store.begin();
        for index in 0..200 {
            store
                .put(xid, format!("k{index:05}").as_bytes(), b"v")
                .unwrap();
        }
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        let rows: Vec<_> = store
            .scan_from(&snapshot, b"k00150")
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();
        assert_eq!(rows.len(), 50);
        assert_eq!(rows[0].0, b"k00150");
    }

    #[test]
    fn locality_batch_restores_key_order_and_snapshot_visibility() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let setup = store.begin();
        for index in (0..200).rev() {
            store
                .put(setup, format!("k{index:05}").as_bytes(), b"before")
                .unwrap();
        }
        store.commit(setup).unwrap();
        let (_reader, snapshot) = store.begin_transaction();

        let writer = store.begin();
        store.put(writer, b"k00055", b"after").unwrap();
        store.delete(writer, b"k00060").unwrap();
        store.commit(writer).unwrap();

        let batch = store
            .scan_locality_batch(&snapshot, b"k", b"k00050", 25)
            .unwrap();
        assert!(!batch.exhausted);
        assert_eq!(batch.rows.len(), 25);
        assert_eq!(batch.rows.first().unwrap().0, b"k00050");
        assert_eq!(batch.rows.last().unwrap().0, b"k00074");
        assert!(batch.rows.iter().all(|(_, value)| value == b"before"));
        assert_eq!(batch.last_key.as_deref(), Some(&b"k00074"[..]));
    }

    #[test]
    fn cyclic_chain_is_precise_and_can_be_replaced_without_touching_other_keys() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let first = store.begin();
        store.put(first, b"pubmed_state", b"old-state").unwrap();
        store.put(first, b"document", b"still-readable").unwrap();
        store.commit(first).unwrap();

        let broken = store.begin();
        store
            .put(broken, b"pubmed_state", b"uncommitted-state")
            .unwrap();
        let encoded_head = store.index.get(b"pubmed_state").unwrap().unwrap();
        let head = TupleLocator::decode(&encoded_head).unwrap();
        let stored = store.heap.get(head).unwrap();
        let (mut header, value) = VersionHeader::decode(&stored).unwrap();
        header.prev = Some(head);
        let mut cyclic = Vec::with_capacity(stored.len());
        header.encode_into(&mut cyclic);
        cyclic.extend_from_slice(value);
        assert_eq!(store.heap.update(head, &cyclic).unwrap(), head);
        store.abort(broken).unwrap();

        let error = store.get(b"pubmed_state").unwrap_err();
        assert!(matches!(error, PageError::VersionChainCycle { .. }));
        assert!(error.is_version_chain_fault());
        let message = error.to_string();
        assert!(message.contains("MVCC version chain"));
        assert!(message.contains(&format!("{head:?}")));
        assert!(
            !message.contains("page 0 has type 0, expected 0"),
            "the old fake corruption sentinel leaked through: {message}"
        );
        assert!(
            !error.is_corruption(),
            "a localized chain cycle must not masquerade as physical page corruption"
        );
        assert_eq!(
            store.get(b"document").unwrap().as_deref(),
            Some(&b"still-readable"[..])
        );

        let inspection = store.inspect_version_chain(b"pubmed_state").unwrap();
        assert_eq!(inspection.head, Some(head));
        assert!(matches!(
            inspection.terminal,
            VersionChainTerminal::Cycle {
                repeated,
                first_seen_step: 0,
                repeated_at_step: 1,
            } if repeated == head
        ));
        let before = store.verify_integrity(8).unwrap();
        assert!(!before.valid);
        assert_eq!(before.version_chains.cycles, 1);
        assert_eq!(before.version_chains.fault_samples.len(), 1);
        let bounded = store
            .verify_version_chains_step(
                VersionChainVerifyCursor::default(),
                VersionChainVerifyLimits {
                    max_duration_millis: 10_000,
                    ..VersionChainVerifyLimits::default()
                },
            )
            .unwrap();
        assert!(bounded.complete);
        assert!(!bounded.version_chains.valid);
        assert_eq!(bounded.version_chains.cycles, 1);
        assert_eq!(bounded.version_chains.fault_samples.len(), 1);

        let active = store.begin();
        let refused = store
            .replace_faulty_version_chain(b"pubmed_state", head, b"must-not-publish")
            .unwrap_err();
        assert!(matches!(
            refused,
            PageError::VersionChainRepairRefused { .. }
        ));
        store.abort(active).unwrap();

        let repair = store
            .replace_faulty_version_chain(b"pubmed_state", head, b"reconstructed-from-ledger")
            .unwrap();
        assert_eq!(repair.previous_head, head);
        assert_ne!(repair.replacement_head, head);
        assert_eq!(
            store.get(b"pubmed_state").unwrap().as_deref(),
            Some(&b"reconstructed-from-ledger"[..])
        );
        let after = store.verify_integrity(8).unwrap();
        assert!(after.valid);
        assert_eq!(after.version_chains.cycles, 0);
        assert_eq!(
            store.get(b"document").unwrap().as_deref(),
            Some(&b"still-readable"[..])
        );

        drop(store);
        let reopened = open(&dir);
        assert_eq!(
            reopened.get(b"pubmed_state").unwrap().as_deref(),
            Some(&b"reconstructed-from-ledger"[..])
        );
        assert_eq!(
            reopened.get(b"document").unwrap().as_deref(),
            Some(&b"still-readable"[..])
        );
        assert!(reopened.verify_integrity(8).unwrap().valid);
    }

    #[test]
    fn version_chain_limit_has_a_dedicated_non_corruption_error() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let initial = store.begin();
        store.put(initial, b"hot-control-row", b"v0").unwrap();
        store.commit(initial).unwrap();
        let (reader, old_snapshot) = store.begin_transaction();

        for value in [b"v1", b"v2", b"v3", b"v4"] {
            let update = store.begin();
            store.put(update, b"hot-control-row", value).unwrap();
            store.commit(update).unwrap();
        }
        let encoded_head = store.index.get(b"hot-control-row").unwrap().unwrap();
        let head = TupleLocator::decode(&encoded_head).unwrap();
        let error = store
            .walk_chain_as_of_bounded(&old_snapshot, Some(head), 2)
            .unwrap_err();
        assert!(matches!(
            error,
            PageError::VersionChainLimitExceeded {
                head: found_head,
                limit: 2,
                ..
            } if found_head == head
        ));
        assert!(error.is_version_chain_fault());
        assert!(!error.is_corruption());
        store.abort(reader).unwrap();
    }

    #[test]
    fn bounded_version_verification_resumes_exactly_across_reopen() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let transaction = store.begin();
        for index in 0..10 {
            store
                .put(transaction, format!("k{index:04}").as_bytes(), b"value")
                .unwrap();
        }
        store.commit(transaction).unwrap();
        store.checkpoint().unwrap();

        let limits = VersionChainVerifyLimits {
            max_keys: 3,
            max_duration_millis: 10_000,
            max_fault_samples: 0,
            max_fault_sample_bytes: 0,
            ..VersionChainVerifyLimits::default()
        };
        let first = store
            .verify_version_chains_step(VersionChainVerifyCursor::default(), limits)
            .unwrap();
        assert_eq!(first.version_chains.keys_examined, 3);
        assert_eq!(first.version_chains.versions_examined, 3);
        assert_eq!(
            first.bytes_examined,
            3 * crate::mvcc::VERSION_HEADER_BYTES as u64
        );
        assert_eq!(first.stop_reason, VersionChainVerifyStopReason::KeyLimit);
        assert!(!first.complete);
        assert_eq!(first.next_cursor.next_key.as_deref(), Some(&b"k0003"[..]));
        assert!(first.version_chains.valid);

        let encoded = serde_json::to_value(&first.next_cursor).unwrap();
        assert_eq!(
            serde_json::from_value::<VersionChainVerifyCursor>(encoded.clone()).unwrap(),
            first.next_cursor
        );
        let mut forged = encoded;
        forged["unbounded_future_field"] = serde_json::json!(true);
        assert!(serde_json::from_value::<VersionChainVerifyCursor>(forged).is_err());
        let mut forged_report = serde_json::to_value(&first).unwrap();
        forged_report["version_chains"]["unbounded_future_field"] = serde_json::json!(true);
        assert!(serde_json::from_value::<VersionChainVerifyStepReport>(forged_report).is_err());

        let mut cursor = first.next_cursor;
        let mut keys_examined = first.version_chains.keys_examined;
        let mut versions_examined = first.version_chains.versions_examined;
        drop(store);
        store = open(&dir);
        loop {
            let step = store.verify_version_chains_step(cursor, limits).unwrap();
            assert!(step.version_chains.keys_examined <= limits.max_keys);
            assert!(step.version_chains.versions_examined <= limits.max_versions);
            assert!(step.bytes_examined <= limits.max_bytes);
            assert!(step.version_chains.valid);
            keys_examined += step.version_chains.keys_examined;
            versions_examined += step.version_chains.versions_examined;
            if step.complete {
                assert_eq!(step.stop_reason, VersionChainVerifyStopReason::Complete);
                assert_eq!(step.next_cursor, VersionChainVerifyCursor::default());
                break;
            }
            cursor = step.next_cursor;
        }
        assert_eq!(keys_examined, 10);
        assert_eq!(versions_examined, 10);

        let version_limited = store
            .verify_version_chains_step(
                VersionChainVerifyCursor::default(),
                VersionChainVerifyLimits {
                    max_keys: 10,
                    max_versions: 3,
                    max_versions_per_chain: 2,
                    max_duration_millis: 10_000,
                    max_fault_samples: 0,
                    max_fault_sample_bytes: 0,
                    ..VersionChainVerifyLimits::default()
                },
            )
            .unwrap();
        assert_eq!(version_limited.version_chains.keys_examined, 2);
        assert_eq!(
            version_limited.stop_reason,
            VersionChainVerifyStopReason::VersionLimit
        );
        assert_eq!(
            version_limited.next_cursor.next_key.as_deref(),
            Some(&b"k0002"[..])
        );

        let byte_limited = store
            .verify_version_chains_step(
                VersionChainVerifyCursor::default(),
                VersionChainVerifyLimits {
                    max_keys: 10,
                    max_versions_per_chain: 2,
                    max_bytes: 2 * crate::mvcc::VERSION_HEADER_BYTES as u64,
                    max_duration_millis: 10_000,
                    max_fault_samples: 0,
                    max_fault_sample_bytes: 0,
                    ..VersionChainVerifyLimits::default()
                },
            )
            .unwrap();
        assert_eq!(byte_limited.version_chains.keys_examined, 1);
        assert_eq!(
            byte_limited.stop_reason,
            VersionChainVerifyStopReason::ByteLimit
        );
        assert_eq!(
            byte_limited.next_cursor.next_key.as_deref(),
            Some(&b"k0001"[..])
        );

        let invalid = VersionChainVerifyLimits {
            max_versions: 2,
            max_versions_per_chain: 3,
            ..VersionChainVerifyLimits::default()
        };
        assert!(matches!(
            store.verify_version_chains_step(VersionChainVerifyCursor::default(), invalid),
            Err(PageError::InvalidMaintenanceLimits { .. })
        ));
        let oversized_cursor = VersionChainVerifyCursor {
            next_key: Some(vec![b'x'; limits.max_cursor_key_bytes + 1]),
        };
        assert!(matches!(
            store.verify_version_chains_step(oversized_cursor, limits),
            Err(PageError::InvalidMaintenanceLimits { .. })
        ));
    }

    #[test]
    fn bounded_version_verification_reads_only_wide_row_headers() {
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            for version in 0..6_u8 {
                let transaction = store.begin();
                store
                    .put(transaction, b"wide", &vec![version; 256 * 1024])
                    .unwrap();
                store.commit(transaction).unwrap();
            }
            store.checkpoint().unwrap();
        }

        let store = open(&dir);
        let before = store.buffer_pool().store().metrics.snapshot();
        let limits = VersionChainVerifyLimits {
            max_keys: 1,
            max_versions: 3,
            max_versions_per_chain: 3,
            max_bytes: 3 * crate::mvcc::VERSION_HEADER_BYTES as u64,
            max_duration_millis: 10_000,
            max_fault_samples: 1,
            max_fault_sample_bytes: 64 * 1024,
            max_cursor_key_bytes: 1024,
        };
        let report = store
            .verify_version_chains_step(VersionChainVerifyCursor::default(), limits)
            .unwrap();
        let after = store.buffer_pool().store().metrics.snapshot();

        assert!(report.complete);
        assert!(!report.version_chains.valid);
        assert_eq!(report.version_chains.keys_examined, 1);
        assert_eq!(report.version_chains.versions_examined, 3);
        assert_eq!(report.version_chains.limit_exceeded, 1);
        assert_eq!(report.bytes_examined, limits.max_bytes);
        assert_eq!(report.version_chains.fault_samples.len(), 1);
        assert!(report.fault_sample_bytes <= limits.max_fault_sample_bytes);
        assert!(matches!(
            report.version_chains.fault_samples[0].inspection.terminal,
            VersionChainTerminal::LimitExceeded { limit: 3, .. }
        ));
        let bytes_read = after.bytes_read.saturating_sub(before.bytes_read);
        assert!(
            bytes_read < 128 * 1024,
            "fixed-header verification read {bytes_read} bytes from wide row payloads"
        );
        assert!(
            store.buffer_pool().snapshot().resident_bytes
                <= store.buffer_pool().snapshot().budget_bytes
        );

        let dropped = store
            .verify_version_chains_step(
                VersionChainVerifyCursor::default(),
                VersionChainVerifyLimits {
                    max_fault_sample_bytes: 1,
                    ..limits
                },
            )
            .unwrap();
        assert!(dropped.version_chains.fault_samples.is_empty());
        assert_eq!(dropped.fault_samples_dropped, 1);
        assert_eq!(dropped.fault_sample_bytes, 0);
    }

    #[test]
    fn missing_keys_read_as_none() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        assert_eq!(store.get(b"never-written").unwrap(), None);
        let transaction = store.begin();
        store.put(transaction, b"present", b"v").unwrap();
        store.commit(transaction).unwrap();
        assert_eq!(store.get(b"still-not-here").unwrap(), None);
    }

    #[test]
    fn an_aborted_transaction_no_longer_pins_the_checkpoint_watermark() {
        // Before 1.0.85-beta the freeze stopped forever at the aborted xid, so
        // the later commit stayed unfrozen, `has_unfrozen_commits` refused the
        // truncation, and the WAL — and the next recovery's outcome set —
        // grew without bound. The durable abort exception lets the watermark
        // step over it.
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let first = store.begin();
            store.put(first, b"committed-before", b"yes").unwrap();
            store.commit(first).unwrap();

            let doomed = store.begin();
            store
                .put(doomed, b"aborted-row", b"must-stay-invisible")
                .unwrap();
            store.abort(doomed).unwrap();

            let third = store.begin();
            store.put(third, b"committed-after", b"yes").unwrap();
            store.commit(third).unwrap();

            store.checkpoint().unwrap();
            assert_eq!(
                store.wal().size_bytes(),
                0,
                "an abort still blocks WAL truncation"
            );
            let snapshot = store.snapshot().unwrap();
            assert_eq!(snapshot.abort_exceptions, 1);
            assert_eq!(snapshot.abort_exception_capacity, 46);
            assert_eq!(snapshot.resident_transaction_entries, 0);
        }

        // The WAL is empty, so the ONLY surviving evidence that the middle
        // transaction aborted is the meta page's exception list.
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        assert_eq!(
            store.get(b"aborted-row").unwrap(),
            None,
            "an aborted row became visible after its abort record was truncated"
        );
        assert_eq!(
            store.get(b"committed-before").unwrap().as_deref(),
            Some(&b"yes"[..])
        );
        assert_eq!(
            store.get(b"committed-after").unwrap().as_deref(),
            Some(&b"yes"[..])
        );
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.abort_exceptions, 1);
        assert_eq!(snapshot.recovery.abort_exceptions_at_open, 1);
    }

    #[test]
    fn abort_exceptions_are_rederived_when_a_crash_precedes_the_checkpoint() {
        // Crash before any checkpoint persists the exception: the WAL still
        // holds the abort record, recovery relearns it, and the open-time
        // freeze records the exception again.
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let first = store.begin();
            store.put(first, b"keep", b"yes").unwrap();
            store.commit(first).unwrap();
            let doomed = store.begin();
            store.put(doomed, b"drop", b"no").unwrap();
            store.abort(doomed).unwrap();
            let third = store.begin();
            store.put(third, b"also-keep", b"yes").unwrap();
            store.commit(third).unwrap();
            // Deliberately no checkpoint.
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert_eq!(recovery.transaction_outcomes, 3);
        assert_eq!(recovery.frozen_outcomes_at_open, 3);
        assert_eq!(recovery.abort_exceptions_at_open, 1);
        assert_eq!(store.get(b"drop").unwrap(), None);
        assert_eq!(store.get(b"keep").unwrap().as_deref(), Some(&b"yes"[..]));
        assert_eq!(
            store.get(b"also-keep").unwrap().as_deref(),
            Some(&b"yes"[..])
        );
        assert_eq!(store.transactions().resident_entries(), 0);
    }

    #[test]
    fn exception_capacity_overflow_stops_the_watermark_but_not_the_log() {
        // A 512-byte meta page holds 46 exceptions. The 47th abort cannot be
        // represented, so the watermark stops there — but since 1.0.86-beta
        // the status spill carries the outcomes the watermark cannot absorb,
        // so the WAL still truncates. The degradation is a pinned watermark,
        // not a retained log.
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        assert_eq!(store.snapshot().unwrap().abort_exception_capacity, 46);

        for round in 0..60 {
            let committer = store.begin();
            store
                .put(committer, format!("c{round:03}").as_bytes(), b"yes")
                .unwrap();
            store.commit(committer).unwrap();
            let doomed = store.begin();
            store.abort(doomed).unwrap();
        }
        let tail = store.begin();
        store.put(tail, b"tail", b"yes").unwrap();
        store.commit(tail).unwrap();

        store.checkpoint().unwrap();
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.abort_exceptions, 46);
        assert!(
            snapshot.transaction_frozen_xid < snapshot.transaction_next_xid,
            "the watermark advanced past a full exception region"
        );
        assert_eq!(
            snapshot.wal_bytes, 0,
            "the status spill did not release WAL truncation"
        );
        assert!(
            snapshot.status_spill_entries > 0,
            "outcomes above the pinned watermark were not spilled"
        );

        // Visibility stays exact even in the degraded state.
        assert_eq!(store.get(b"tail").unwrap().as_deref(), Some(&b"yes"[..]));
        assert_eq!(store.get(b"c000").unwrap().as_deref(), Some(&b"yes"[..]));
    }

    #[test]
    fn recovery_collapses_a_retained_suffix_into_the_watermark_at_open() {
        // The pathological shape 1.0.84-beta documented: a blocker kept the
        // suffix large. Once the blocker ends and the store restarts, the
        // open-time freeze must absorb every representable outcome instead of
        // leaving the whole suffix resident.
        let dir = TempDir::new().unwrap();
        {
            let store = open(&dir);
            let blocker = store.begin();
            for round in 0..500 {
                let committer = store.begin();
                store
                    .put(committer, format!("k{round:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(committer).unwrap();
                if round % 97 == 0 {
                    let doomed = store.begin();
                    store.abort(doomed).unwrap();
                }
            }
            store.commit(blocker).unwrap();
            // Crash before a checkpoint: the entire suffix survives.
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert!(recovery.transaction_outcomes >= 500);
        assert_eq!(
            recovery.frozen_outcomes_at_open, recovery.transaction_outcomes,
            "a terminal suffix with no live blocker must freeze completely"
        );
        assert_eq!(recovery.abort_exceptions_at_open, 6);
        assert_eq!(store.transactions().resident_entries(), 0);
        assert_eq!(store.len().unwrap(), 500);
    }

    #[test]
    fn spilled_outcomes_survive_restart_with_a_pinned_watermark() {
        // A long-running transaction pins the watermark, so the commits after
        // it cannot freeze and must be carried by the status spill across the
        // checkpoint that truncates the WAL. After a restart those rows must
        // still be visible — resolved on demand from the spill.
        let dir = TempDir::new().unwrap();
        let rows = 200usize;
        {
            let store = open(&dir);
            let blocker = store.begin();
            for index in 0..rows {
                let xid = store.begin();
                store
                    .put(xid, format!("k{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.checkpoint().unwrap();
            let snapshot = store.snapshot().unwrap();
            assert!(
                snapshot.status_spill_entries >= rows as u64,
                "blocker-pinned commits were not spilled: {}",
                snapshot.status_spill_entries
            );
            assert_eq!(snapshot.wal_bytes, 0, "spill must release truncation");
            store.commit(blocker).unwrap();
        }

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert!(recovery.status_spill_entries_at_open >= rows as u64);
        let mut missing = 0usize;
        for index in 0..rows {
            if store
                .get(format!("k{index:05}").as_bytes())
                .unwrap()
                .is_none()
            {
                missing += 1;
            }
        }
        assert_eq!(missing, 0, "{missing} spilled rows lost across restart");
    }

    #[test]
    fn repeated_spill_checkpoints_merge_without_losing_outcomes() {
        // Several checkpoints while the watermark stays pinned: each one must
        // merge the newly committed outcomes into the existing spill pages
        // without dropping what earlier checkpoints already persisted.
        let dir = TempDir::new().unwrap();
        let per_round = 40usize;
        let rounds = 5usize;
        {
            let store = open(&dir);
            let blocker = store.begin();
            for round in 0..rounds {
                for index in 0..per_round {
                    let n = round * per_round + index;
                    let xid = store.begin();
                    store
                        .put(xid, format!("k{n:05}").as_bytes(), b"payload")
                        .unwrap();
                    store.commit(xid).unwrap();
                }
                store.checkpoint().unwrap();
            }
            let snapshot = store.snapshot().unwrap();
            assert_eq!(snapshot.wal_bytes, 0);
            assert!(
                snapshot.status_spill_entries >= (rounds * per_round) as u64,
                "spill lost entries across merges: {}",
                snapshot.status_spill_entries
            );
            store.commit(blocker).unwrap();
        }

        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let mut missing = 0usize;
        for n in 0..rounds * per_round {
            if store.get(format!("k{n:05}").as_bytes()).unwrap().is_none() {
                missing += 1;
            }
        }
        assert_eq!(
            missing, 0,
            "{missing} merged-spill rows lost across restart"
        );
    }

    #[test]
    fn watermark_advancing_past_spilled_xids_keeps_them_visible() {
        // The case the pinned-watermark tests do not cover: outcomes are
        // spilled while a blocker pins the watermark, the blocker then ends,
        // and a later checkpoint advances the watermark across the spilled
        // xids and drops them from the spill. They must remain visible via the
        // watermark, and any still unfrozen must remain in the spill.
        let dir = TempDir::new().unwrap();
        let first_wave = 60usize;
        let second_wave = 60usize;
        {
            let store = open(&dir);
            let blocker = store.begin();
            for index in 0..first_wave {
                let xid = store.begin();
                store
                    .put(xid, format!("a{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.checkpoint().unwrap();
            assert!(store.snapshot().unwrap().status_spill_entries >= first_wave as u64);

            // Blocker ends; the watermark can now advance across the spill.
            store.commit(blocker).unwrap();
            for index in 0..second_wave {
                let xid = store.begin();
                store
                    .put(xid, format!("b{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.checkpoint().unwrap();
            assert_eq!(store.snapshot().unwrap().wal_bytes, 0);
        }

        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let mut missing = Vec::new();
        for index in 0..first_wave {
            if store
                .get(format!("a{index:05}").as_bytes())
                .unwrap()
                .is_none()
            {
                missing.push(format!("a{index}"));
            }
        }
        for index in 0..second_wave {
            if store
                .get(format!("b{index:05}").as_bytes())
                .unwrap()
                .is_none()
            {
                missing.push(format!("b{index}"));
            }
        }
        assert!(
            missing.is_empty(),
            "{} rows lost when the watermark advanced past the spill: {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        );
    }

    /// Write a standalone Free image for `page_id` directly to the page file,
    /// bypassing the free list — the exact shape an interrupted publication
    /// leaves behind.
    fn orphan_free_page(store: &PagedStore, page_id: PageId) {
        let page_store = store.buffer_pool().store();
        let page_size = page_store.page_size();
        let mut image = vec![0_u8; page_size as usize];
        let mut header =
            crate::page::PageHeader::new(page_id, crate::page::PageType::Free, page_size);
        header.next_page = 0;
        header.encode(&mut image);
        page_store.write_page(page_id, &mut image).unwrap();
    }

    #[test]
    fn orphaned_free_pages_are_reclaimed_into_the_free_list() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let page_store = store.buffer_pool().store().clone();

        // Allocate real pages, then strand three of them as Free images that
        // are never linked into the free list.
        let mut stranded = Vec::new();
        for _ in 0..3 {
            stranded.push(page_store.allocate(crate::page::PageType::Heap).unwrap());
        }
        let free_before = page_store.free_page_count();
        for page_id in &stranded {
            orphan_free_page(&store, *page_id);
        }
        assert_eq!(
            page_store.free_page_count(),
            free_before,
            "writing a Free image must not touch the free list"
        );

        let report = store
            .reclaim_orphaned_free_pages_step(FreeReclaimCursor::default(), Default::default())
            .unwrap();
        assert_eq!(report.orphans_found, 3, "report: {report:?}");
        assert_eq!(report.pages_reclaimed, 3);
        assert!(report.complete);
        assert_eq!(
            page_store.free_page_count(),
            free_before + 3,
            "reclaimed orphans must join the free list"
        );

        // A second pass finds nothing: the reclaimed pages are now listed.
        let again = store
            .reclaim_orphaned_free_pages_step(FreeReclaimCursor::default(), Default::default())
            .unwrap();
        assert_eq!(again.orphans_found, 0);
        assert_eq!(again.pages_reclaimed, 0);
        assert!(again.complete);
    }

    #[test]
    fn orphan_reclaim_resumes_across_steps_and_honors_the_adopt_batch() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let page_store = store.buffer_pool().store().clone();

        let mut stranded = Vec::new();
        for _ in 0..5 {
            stranded.push(page_store.allocate(crate::page::PageType::Heap).unwrap());
        }
        for page_id in &stranded {
            orphan_free_page(&store, *page_id);
        }
        let free_before = page_store.free_page_count();

        let limits = FreeReclaimLimits {
            max_adopt: 2,
            ..Default::default()
        };
        let mut cursor = FreeReclaimCursor::default();
        let mut total_reclaimed = 0u64;
        let mut steps = 0u64;
        loop {
            let report = store
                .reclaim_orphaned_free_pages_step(cursor, limits)
                .unwrap();
            total_reclaimed += report.pages_reclaimed;
            cursor = report.next_cursor;
            steps += 1;
            if report.complete {
                break;
            }
            assert!(steps < 20, "reclaim did not converge");
        }
        assert_eq!(total_reclaimed, 5);
        assert_eq!(page_store.free_page_count(), free_before + 5);
    }

    #[test]
    fn orphan_reclaim_defers_when_the_free_list_is_too_large() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let page_store = store.buffer_pool().store().clone();

        // Build a real free list larger than the visit budget: allocate pages
        // up front (so none is reused), then free them all onto the list.
        let mut pages = Vec::new();
        for _ in 0..5 {
            pages.push(page_store.allocate(crate::page::PageType::Heap).unwrap());
        }
        for page_id in pages {
            page_store.free(page_id).unwrap();
        }
        assert!(page_store.free_page_count() >= 5);

        // Also strand an orphan that a completed scan WOULD reclaim, to prove
        // the deferral path mutates nothing. Allocating it reuses one of the
        // freed pages, so capture the baseline afterwards.
        let stranded = page_store.allocate(crate::page::PageType::Heap).unwrap();
        orphan_free_page(&store, stranded);
        let free_before = page_store.free_page_count();

        let limits = FreeReclaimLimits {
            max_free_list_visits: 2,
            ..Default::default()
        };
        let report = store
            .reclaim_orphaned_free_pages_step(FreeReclaimCursor::default(), limits)
            .unwrap();
        assert_eq!(
            report.stop_reason,
            FreeReclaimStopReason::FreeListTooLarge,
            "a 5-member list must exceed a 2-visit budget: {report:?}"
        );
        assert_eq!(report.pages_reclaimed, 0, "deferral must not mutate");
        assert_eq!(
            page_store.free_page_count(),
            free_before,
            "deferral must not touch the free list"
        );

        // With a budget that fits, the same orphan is reclaimed.
        let ok = store
            .reclaim_orphaned_free_pages_step(
                FreeReclaimCursor::default(),
                FreeReclaimLimits {
                    max_free_list_visits: 64,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(ok.orphans_found, 1);
        assert_eq!(ok.pages_reclaimed, 1);
    }

    #[test]
    fn an_interrupted_spill_persist_leaves_the_store_recoverable() {
        // Copy-on-write crash atomicity: fail a write partway through a spill
        // persist. The checkpoint must fail WITHOUT corrupting the chain the
        // previous checkpoint published, and a restart must recover every
        // committed row (the intact WAL re-derives what the failed persist did
        // not publish).
        let dir = TempDir::new().unwrap();
        let rows = 30usize;
        {
            let store = open(&dir);
            let page_store = store.buffer_pool().store().clone();
            let blocker = store.begin();
            for index in 0..20 {
                let xid = store.begin();
                store
                    .put(xid, format!("k{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.checkpoint().unwrap();
            assert!(store.snapshot().unwrap().status_spill_entries >= 20);
            // More commits whose persist we are about to interrupt.
            for index in 20..rows {
                let xid = store.begin();
                store
                    .put(xid, format!("k{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            // Fail the second page-file write of the next checkpoint: the first
            // is the fresh page allocation, the second its content write, so the
            // persist dies mid-publish of the NEW chain.
            let base = page_store.faults.write_count();
            page_store.faults.fail_nth_write(base + 2);
            let failed = store.checkpoint().is_err();
            page_store.faults.clear();
            assert!(
                failed,
                "the injected write failure must fail the checkpoint"
            );
            // Drop without a clean checkpoint: this is the crash.
        }

        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let mut missing = 0usize;
        for index in 0..rows {
            if store
                .get(format!("k{index:05}").as_bytes())
                .unwrap()
                .is_none()
            {
                missing += 1;
            }
        }
        assert_eq!(
            missing, 0,
            "{missing} rows lost after an interrupted spill persist"
        );
    }

    #[test]
    fn spill_reclaim_refuses_a_stale_durable_descriptor() {
        // Reproduce the invariant violated by the production incident: memory
        // has published a replacement chain, but the durable meta page still
        // names its predecessor. Reclaim must fail closed and leave every old
        // page intact until a real checkpoint durably publishes the new chain.
        let dir = TempDir::new().unwrap();
        let old_pages;
        {
            let store = open(&dir);
            let blocker = store.begin();
            for index in 0..30 {
                let xid = store.begin();
                store
                    .put(xid, format!("a{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.checkpoint().unwrap();
            old_pages = store
                .spill
                .state
                .lock()
                .iter()
                .map(|page| page.page_id)
                .collect::<Vec<_>>();
            assert!(!old_pages.is_empty());

            for index in 0..10 {
                let xid = store.begin();
                store
                    .put(xid, format!("b{index:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.persist_status_spill().unwrap();
            let error = store
                .spill
                .reclaim_retired(store.meta_page, &store.pool)
                .expect_err("stale durable descriptor must fence reclamation");
            assert!(error.to_string().contains("does not name current chain"));
            assert_eq!(store.spill.retired.lock().len(), old_pages.len());
            let page_store = store.buffer_pool().store();
            let mut page = vec![0_u8; store.options.page_size as usize];
            for page_id in &old_pages {
                let header = page_store.read_page(*page_id, &mut page).unwrap();
                assert_eq!(header.page_type, crate::page::PageType::Heap);
            }

            // A complete checkpoint publishes the replacement and may then
            // reclaim both generations safely.
            store.checkpoint().unwrap();
            assert!(store.spill.retired.lock().is_empty());
            store.commit(blocker).unwrap();
        }

        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for index in 0..30 {
            assert!(store
                .get(format!("a{index:05}").as_bytes())
                .unwrap()
                .is_some());
        }
        for index in 0..10 {
            assert!(store
                .get(format!("b{index:05}").as_bytes())
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn spill_reclaim_is_idempotent_for_duplicate_and_already_freed_pages() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let blocker = store.begin();
        for index in 0..30 {
            let xid = store.begin();
            store
                .put(xid, format!("a{index:05}").as_bytes(), b"payload")
                .unwrap();
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        for index in 0..10 {
            let xid = store.begin();
            store
                .put(xid, format!("b{index:05}").as_bytes(), b"payload")
                .unwrap();
            store.commit(xid).unwrap();
        }
        store.persist_status_spill().unwrap();
        store.write_meta().unwrap();
        store
            .checkpointer
            .checkpoint(store.meta_page)
            .expect("publish replacement descriptor");

        let first = store.spill.retired.lock()[0];
        store.spill.retired.lock().push(first);
        store
            .pool
            .free_page_for_transaction(first.page_id, 0)
            .expect("simulate cleanup completed before a retry");

        store
            .spill
            .reclaim_retired(store.meta_page, &store.pool)
            .expect("duplicate cleanup must be idempotent");
        store.finalize_structural_frees().unwrap();
        assert!(store.spill.retired.lock().is_empty());
        store.commit(blocker).unwrap();
    }

    #[test]
    fn stale_spill_reclaim_never_frees_a_reused_page_id() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        let blocker = store.begin();
        for index in 0..30 {
            let xid = store.begin();
            store
                .put(xid, format!("a{index:05}").as_bytes(), b"payload")
                .unwrap();
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        for index in 0..10 {
            let xid = store.begin();
            store
                .put(xid, format!("b{index:05}").as_bytes(), b"payload")
                .unwrap();
            store.commit(xid).unwrap();
        }
        store.persist_status_spill().unwrap();
        store.write_meta().unwrap();
        store
            .checkpointer
            .checkpoint(store.meta_page)
            .expect("publish replacement descriptor");

        let mut retired = std::mem::take(&mut *store.spill.retired.lock());
        retired.sort_unstable_by_key(|page| page.page_id);
        let stale = retired[0];
        store
            .pool
            .free_page_for_transaction(stale.page_id, 0)
            .unwrap();
        store.finalize_structural_frees().unwrap();
        let reused = store
            .pool
            .store()
            .allocate(crate::page::PageType::Heap)
            .unwrap();
        assert_eq!(reused, stale.page_id, "test must exercise page-ID reuse");
        let reused_header = store.pool.store().read_header(reused).unwrap();
        assert_ne!(reused_header.generation, stale.generation);

        store.spill.retired.lock().push(stale);
        store
            .spill
            .reclaim_retired(store.meta_page, &store.pool)
            .expect("stale cleanup must be ignored after page reuse");
        let after = store.pool.store().read_header(reused).unwrap();
        assert_eq!(after.page_type, crate::page::PageType::Heap);
        assert_eq!(after.generation, reused_header.generation);
        store.commit(blocker).unwrap();
    }

    #[test]
    fn offline_status_spill_repair_replaces_a_corrupt_chain_exactly() {
        let dir = TempDir::new().unwrap();
        let outcomes;
        let (old_head, old_pages, old_entries);
        {
            let store = open(&dir);
            let _blocker = store.begin();
            outcomes = (0..30)
                .map(|index| {
                    let xid = store.begin();
                    store
                        .put(xid, format!("k{index:05}").as_bytes(), b"payload")
                        .unwrap();
                    store.commit(xid).unwrap();
                    (xid, TxStatus::Committed)
                })
                .collect::<Vec<_>>();
            store.checkpoint().unwrap();
            (old_head, old_pages, old_entries) = store.spill.descriptor();
        }

        assert_eq!(
            extract_status_spill(dir.path(), 512, old_head, old_pages, old_entries).unwrap(),
            outcomes
        );

        // Destroy the named head exactly as an allocator reuse did in the
        // incident. Normal PagedStore open must now fail closed.
        let paths = PagedPaths::in_dir(dir.path());
        let page_store = PageStore::open(
            &paths.pages,
            PageStoreOptions {
                page_size: 512,
                fsync: true,
                create: false,
                extent_bytes: 0,
            },
        )
        .unwrap();
        let mut replacement = vec![0_u8; 512];
        crate::page::PageHeader::new(old_head, crate::page::PageType::Overflow, 512)
            .encode(&mut replacement);
        page_store.write_page(old_head, &mut replacement).unwrap();
        page_store.sync().unwrap();
        drop(page_store);
        assert!(PagedStore::open(dir.path(), options()).is_err());

        let dry_run = repair_status_spill(
            dir.path(),
            512,
            old_head,
            old_pages,
            old_entries,
            &outcomes,
            false,
        )
        .unwrap();
        assert!(!dry_run.applied);
        assert_eq!(dry_run.replacement_entries, outcomes.len() as u64);

        let repaired = repair_status_spill(
            dir.path(),
            512,
            old_head,
            old_pages,
            old_entries,
            &outcomes,
            true,
        )
        .unwrap();
        assert!(repaired.applied);
        assert_ne!(repaired.replacement_head, old_head);

        let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
        assert_eq!(recovery.status_spill_entries_at_open, outcomes.len() as u64);
        for index in 0..30 {
            assert!(store
                .get(format!("k{index:05}").as_bytes())
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn concurrent_readers_and_spill_checkpoints_stay_consistent() {
        // Readers resolve spilled outcomes while checkpoints republish the
        // chain underneath them (copy-on-write swap). No reader may observe a
        // half-swapped chain or a retired page.
        use std::sync::Arc;
        let dir = TempDir::new().unwrap();
        let store = Arc::new(open(&dir));
        let spilled_rows = 40usize;
        let blocker = store.begin();
        for index in 0..spilled_rows {
            let xid = store.begin();
            store
                .put(xid, format!("k{index:05}").as_bytes(), b"payload")
                .unwrap();
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        assert!(store.snapshot().unwrap().status_spill_entries >= spilled_rows as u64);

        let reader_store = Arc::clone(&store);
        let reader = std::thread::spawn(move || {
            for _ in 0..30 {
                for index in 0..spilled_rows {
                    let value = reader_store.get(format!("k{index:05}").as_bytes()).unwrap();
                    assert!(
                        value.is_some(),
                        "a concurrent spill checkpoint hid committed row k{index:05}"
                    );
                }
            }
        });
        // While the reader resolves spilled outcomes, republish the chain
        // repeatedly by committing more and checkpointing.
        for round in 0..5 {
            for index in 0..8 {
                let n = spilled_rows + round * 8 + index;
                let xid = store.begin();
                store
                    .put(xid, format!("k{n:05}").as_bytes(), b"payload")
                    .unwrap();
                store.commit(xid).unwrap();
            }
            store.checkpoint().unwrap();
        }
        reader.join().unwrap();
        store.commit(blocker).unwrap();
    }
}
