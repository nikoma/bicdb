//! Read-only detection verifier for `server_paged` stores.
//!
//! This pass answers one question about a store on disk: does it hold any of
//! the known damage states, and which ones can be proven from the store alone?
//! It looks for three families:
//!
//! - **Vacuum row loss.** A stale free-space record let an insert write a
//!   committed row onto a heap page vacuum had already freed. The row is
//!   either still on the page, which is still typed `Free` (state A), or gone
//!   because the page was allocated again (state B).
//! - **Unreachable or unreadable committed data** after an uncommitted split
//!   reached the page file (GAP-1 in `docs/recovery-semantics.md`). A
//!   committed heap version that the key tree no longer reaches, a tree or
//!   heap page that a committed structure references but that cannot be read,
//!   or a tree whose shape is broken.
//! - **Page-log shapes** left by an interrupted log reset (GAP-2). They are
//!   classified exactly as the crash-ordered reset fix classifies them at
//!   open. A store an engine without that fix has already replayed looks like
//!   older valid versions, which only external evidence can show.
//!
//! # Read-only by construction
//!
//! The pass never constructs a [`crate::PagedStore`], [`crate::PageStore`],
//! [`crate::wal::Wal`] or buffer pool: each of those opens files for writing,
//! and opening a store runs recovery, which rewrites pages and truncates the
//! log. Instead:
//!
//! - every file is opened through [`open_read_only`]: read access only,
//!   `O_NOFOLLOW`, and `O_NOATIME` where the kernel permits it. No code path
//!   here calls `write`, `set_len`, `create`, `rename` or `remove`;
//! - the directory lock is *probed* with `F_GETLK`, never taken, and
//!   `store.lock` is never created ([`crate::lock::probe_directory_lock`]);
//! - recovery is reproduced in memory: the committed page images recovery
//!   would replay are read from the log in place of the page file's copies,
//!   so the checks see the post-recovery state without recovery running.
//!
//! The test `tests/verify_store.rs` hashes every file of the store before and
//! after a run and requires them to be byte-identical.
//!
//! # Bounded memory and time
//!
//! Trees are walked one page at a time. Memory is a small page cache, two
//! bits per page for tree membership and the page directory, the log's
//! replay map (one entry per page with a pending image), and a slot bitmap
//! for the heap reachability join. The slot bitmap covers a window of heap
//! pages sized to [`StoreVerifyOptions::max_memory_bytes`]; a store larger
//! than one window is verified in several passes over the key tree. A
//! deadline stops the pass early and the report then says it is incomplete.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::FxHashSet;

use crate::btree::{validate_node, validated_entry, BTreePageExpectation, BTreeVerifyFault};
use crate::error::{PageError, Result};
use crate::heap::{decode_tuple, Tuple};
use crate::lock::{probe_directory_lock, DirectoryLockProbe};
use crate::manager::{decode_superblock, segment_path_for, Superblock};
use crate::mvcc::{TxStatus, VersionHeader, Xid, VERSION_HEADER_BYTES};
use crate::page::{PageHeader, PageId, PageType, PAGE_HEADER_BYTES, PAGE_TRAILER_BYTES};
use crate::slotted::{TupleLocator, SLOT_BYTES};
use crate::wal::{analyze_log_read_only, LogResetShape, ReadOnlyLog, ReadOnlyScanLimits};

/// Version of the JSON report layout.
pub const STORE_VERIFY_REPORT_FORMAT_VERSION: u32 = 1;

/// Open a file for reading only. Never creates, truncates or follows a
/// symlink; asks the kernel not to update the access time when allowed.
pub(crate) fn open_read_only(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        #[cfg(target_os = "linux")]
        {
            match std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NOATIME | libc::O_NONBLOCK)
                .open(path)
            {
                Ok(file) => return regular_file(file, path),
                // O_NOATIME needs file ownership or CAP_FOWNER.
                Err(error) if error.raw_os_error() == Some(libc::EPERM) => {}
                Err(error) => return Err(error),
            }
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        regular_file(file, path)
    }
    #[cfg(not(unix))]
    {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        regular_file(file, path)
    }
}

fn regular_file(file: File, path: &Path) -> std::io::Result<File> {
    if file.metadata()?.is_file() {
        Ok(file)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not a regular file", path.display()),
        ))
    }
}

/// Limits for one verification run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreVerifyOptions {
    /// Soft ceiling on the verifier's own memory: page cache, page bitmaps and
    /// the heap slot bitmap. The slot bitmap window shrinks to fit, and the
    /// key tree is walked once per window. At least 1 MiB.
    pub max_memory_bytes: u64,
    /// Stop early (and report `complete: false`) after this long.
    pub max_duration: Option<Duration>,
    /// Maximum findings kept per class; the memory budget may retain fewer.
    /// Counts are exact when the run completes; the sink receives all findings.
    pub max_samples_per_class: usize,
    /// Versions followed per key before the chain is reported as too long.
    pub max_versions_per_chain: usize,
    /// Pages held by the read cache.
    pub page_cache_pages: usize,
    /// Read a store whose directory lock another handle holds. The result
    /// then describes a moving target; meant only for diagnosis on a copy
    /// whose lock file was copied along with it.
    pub allow_live_store: bool,
    /// Heap pages per reachability window; `None` sizes it from
    /// `max_memory_bytes`. Smaller windows mean more key-tree passes.
    pub heap_window_pages: Option<u64>,
}

impl Default for StoreVerifyOptions {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024,
            max_duration: None,
            max_samples_per_class: 20,
            max_versions_per_chain: 100_000,
            page_cache_pages: 2_048,
            allow_live_store: false,
            heap_window_pages: None,
        }
    }
}

/// How bad one finding is.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    /// Expected byproduct of crashes or maintenance; not damage.
    Info,
    /// No damage yet, but a known bug can turn this state into damage.
    AtRisk,
    /// Consistent with damage and with a legitimate history; the store alone
    /// cannot tell them apart. See [`StoreVerifyReport::external_evidence`].
    NeedsExternalCrossCheck,
    /// Damage, provable from the store alone.
    Inconsistent,
}

/// The overall answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreVerdict {
    Clean,
    AtRisk,
    NeedsExternalCrossCheck,
    Inconsistent,
}

/// Every class the verifier reports. Each is counted separately.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingClass {
    // --- Vacuum row loss -------------------------------------------------
    /// State A: a version the key tree reaches is stored on a page typed
    /// `Free`. Readable now; destroyed when the page is allocated again.
    RowOnFreePage,
    /// The version a reader needs (the head, or the first committed version
    /// below uncommitted heads) points into a page that now holds another
    /// structure (B-tree, overflow, superblock, meta or status-spill page).
    /// State B, or a deleted key whose head vacuum reclaimed and whose index
    /// entry was never swept.
    MissingRowPageReused,
    /// A reachable version's slot resolves on a page of another type
    /// (overflow, B-tree, ...): the insert wrote the row into a page another
    /// structure owns, or the locator happens to match foreign bytes. Reads
    /// return whatever the slot holds, and the row and the other structure
    /// overwrite each other.
    RowOnNonHeapPage,
    /// The needed version's locator names a `Free` page with no such slot.
    MissingRowOnFreePage,
    /// The needed version's locator names a heap page whose slot is missing,
    /// dead or of another generation.
    MissingRowStaleHeapSlot,
    /// The needed version's locator lies beyond the page count.
    MissingRowBeyondPageCount,
    /// A free-space record advertises a page that is not a heap page: the
    /// precondition of the vacuum row-loss bug. An engine without the fix
    /// can place the next insert on that page.
    StaleFreeSpaceRecord,
    /// A page-directory entry names a page whose type differs from the one
    /// recorded.
    DirectoryEntryWrongType,

    // --- Reachability and readability (GAP-1) ----------------------------
    /// A key-tree value is not a tuple locator.
    InvalidHeadEncoding,
    /// A tree child or leaf sibling lies beyond the page count.
    TreePageOutOfBounds,
    /// A tree page cannot be read (checksum, torn write, short read, wrong
    /// magic or page id).
    TreePageUnreadable,
    /// A tree edge leads to a page that is not a B-tree page of the expected
    /// kind.
    TreePageWrongType,
    /// A tree node fails structural validation.
    TreeNodeMalformed,
    /// A tree page is reachable twice (cycle or shared child).
    TreeCycle,
    /// A key lies outside the range its parent's separators give it.
    TreeKeyOutOfRange,
    /// A leaf's sibling link is not the next leaf in key order.
    TreeLeafLinkMismatch,
    /// Leaves at different depths, or a tree deeper than any legitimate one.
    TreeShapeInvalid,
    /// A page a committed structure references (a version on the needed
    /// part of a chain) cannot be read.
    ReferencedPageUnreadable,
    /// A version's slot points outside its page.
    VersionOutOfBounds,
    /// A version's MVCC header cannot be decoded.
    MalformedVersion,
    /// A version chain revisits a version.
    ChainCycle,
    /// A version chain is longer than the configured limit.
    ChainTooLong,
    /// The overflow chain of a reachable version is missing, unreadable, of
    /// the wrong type or of the wrong length.
    OverflowChainBroken,
    /// A heap version whose creator committed and whose deleter did not is
    /// not reachable from the key tree: a committed row no lookup or scan
    /// can find.
    UnreachableCommittedLiveVersion,
    /// The status spill cannot be read; a real open refuses the store.
    StatusSpillUnreadable,

    // --- Page log (GAP-2) -------------------------------------------------
    /// Sealed log segments that all predate the checkpoint horizon next to
    /// an empty active file: an interrupted log reset. An engine without the
    /// reset fix replays them over newer pages at its next open.
    LogInterruptedReset,
    /// An engine without the reset fix already opened this store after an
    /// interrupted reset and kept committing. Rows may have regressed to older
    /// committed versions; that engine's next open also discards the newer
    /// active log as a torn tail.
    LogStaleChainAlreadyReplayed,
    /// The log ends before the checkpoint horizon in another shape.
    LogEndsBeforeHorizon,
    /// A real open would refuse the log (undecodable sealed segment,
    /// contradictory outcomes, short image).
    LogRefusedByOpen,

    // --- Informational -----------------------------------------------------
    /// Unreachable heap version whose deleter committed: awaiting vacuum.
    UnreachableDeadVersion,
    /// Unreachable heap version whose creator never committed.
    UnreachableUncommittedVersion,
    /// A version whose transaction status could not be resolved.
    VersionStatusUnresolved,
    /// A heap page missing from the page directory.
    HeapPageNotInDirectory,
    /// A B-tree page neither tree reaches (leaked by a crash).
    OrphanTreePage,
    /// A page no structure references and that cannot be read.
    UnreadableUnreferencedPage,
    /// A tuple on a `Free` page that the key tree does not reach.
    FreePageUnreferencedTuple,
    /// The meta page's extension region needs `store migrate-meta`.
    MetaNeedsMigration,
}

impl FindingClass {
    pub fn severity(self) -> FindingSeverity {
        use FindingClass::*;
        match self {
            RowOnFreePage
            | RowOnNonHeapPage
            | DirectoryEntryWrongType
            | InvalidHeadEncoding
            | TreePageOutOfBounds
            | TreePageUnreadable
            | TreePageWrongType
            | TreeNodeMalformed
            | TreeCycle
            | TreeKeyOutOfRange
            | TreeLeafLinkMismatch
            | TreeShapeInvalid
            | ReferencedPageUnreadable
            | VersionOutOfBounds
            | MalformedVersion
            | ChainCycle
            | ChainTooLong
            | OverflowChainBroken
            | UnreachableCommittedLiveVersion
            | StatusSpillUnreadable
            | LogEndsBeforeHorizon
            | LogRefusedByOpen => FindingSeverity::Inconsistent,
            MissingRowPageReused
            | MissingRowOnFreePage
            | MissingRowStaleHeapSlot
            | MissingRowBeyondPageCount
            | LogStaleChainAlreadyReplayed => FindingSeverity::NeedsExternalCrossCheck,
            StaleFreeSpaceRecord | LogInterruptedReset | MetaNeedsMigration => {
                FindingSeverity::AtRisk
            }
            UnreachableDeadVersion
            | UnreachableUncommittedVersion
            | VersionStatusUnresolved
            | HeapPageNotInDirectory
            | OrphanTreePage
            | UnreadableUnreferencedPage
            | FreePageUnreferencedTuple => FindingSeverity::Info,
        }
    }

    /// Which detection family the class belongs to.
    pub fn family(self) -> &'static str {
        use FindingClass::*;
        match self {
            RowOnFreePage
            | MissingRowPageReused
            | RowOnNonHeapPage
            | MissingRowOnFreePage
            | MissingRowStaleHeapSlot
            | MissingRowBeyondPageCount
            | StaleFreeSpaceRecord
            | DirectoryEntryWrongType
            | FreePageUnreferencedTuple => "vacuum_row_loss",
            LogInterruptedReset
            | LogStaleChainAlreadyReplayed
            | LogEndsBeforeHorizon
            | LogRefusedByOpen => "page_log",
            _ => "heap_reachability",
        }
    }

    pub fn description(self) -> &'static str {
        use FindingClass::*;
        match self {
            RowOnFreePage => "state A: a reachable version is stored on a page typed Free; readable now, destroyed when the page is allocated again",
            MissingRowPageReused => "the version a reader needs points into a page now holding another structure: state B (row lost, page reused) or a deleted key whose index entry was never swept",
            RowOnNonHeapPage => "a reachable version's slot resolves on a page another structure owns: the row and that structure overwrite each other, and reads return whatever the slot holds",
            MissingRowOnFreePage => "the version a reader needs points at a Free page without that slot: state B before reuse, or a deleted and vacuumed key",
            MissingRowStaleHeapSlot => "the version a reader needs points at a heap slot that is missing, dead or re-generated: state B (page reused as heap), or a deleted and vacuumed key",
            MissingRowBeyondPageCount => "the version a reader needs lies beyond the page count: a page truncated after vacuum freed it, or a page that never became durable",
            StaleFreeSpaceRecord => "a free-space record advertises a page that is not a heap page (the vacuum row-loss precondition)",
            DirectoryEntryWrongType => "a page-directory entry disagrees with the page's type",
            InvalidHeadEncoding => "a key-tree value is not a tuple locator",
            TreePageOutOfBounds => "a tree edge leads beyond the page count",
            TreePageUnreadable => "a tree page cannot be read",
            TreePageWrongType => "a tree edge leads to a page of the wrong type",
            TreeNodeMalformed => "a tree node fails structural validation",
            TreeCycle => "a tree page is reachable twice",
            TreeKeyOutOfRange => "a key lies outside its parent's separator range",
            TreeLeafLinkMismatch => "a leaf's sibling link is not the next leaf in key order",
            TreeShapeInvalid => "leaves at different depths, or an impossible height",
            ReferencedPageUnreadable => "a page holding a version readers need cannot be read",
            VersionOutOfBounds => "a version's slot points outside its page",
            MalformedVersion => "a version's MVCC header cannot be decoded",
            ChainCycle => "a version chain revisits a version",
            ChainTooLong => "a version chain exceeds the configured limit",
            OverflowChainBroken => "a reachable version's overflow chain is broken",
            UnreachableCommittedLiveVersion => "a committed, undeleted heap version the key tree does not reach",
            StatusSpillUnreadable => "the transaction status spill cannot be read",
            LogInterruptedReset => "interrupted log reset: stale sealed segments below the checkpoint horizon",
            LogStaleChainAlreadyReplayed => "an engine without the reset fix already replayed a stale log chain and kept committing",
            LogEndsBeforeHorizon => "the page log ends before the checkpoint horizon",
            LogRefusedByOpen => "a real open would refuse the page log",
            UnreachableDeadVersion => "unreachable version whose deleter committed (awaiting vacuum)",
            UnreachableUncommittedVersion => "unreachable version whose creator never committed",
            VersionStatusUnresolved => "a version's transaction status could not be resolved",
            HeapPageNotInDirectory => "a heap page missing from the page directory",
            OrphanTreePage => "a B-tree page neither tree reaches",
            UnreadableUnreferencedPage => "an unreferenced page that cannot be read",
            FreePageUnreferencedTuple => "a tuple on a Free page that nothing reaches",
            MetaNeedsMigration => "the meta page's extension region needs `store migrate-meta`",
        }
    }
}

mod hex_bytes {
    pub fn serialize<S: serde::Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => serializer.serialize_some(&hex::encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<Vec<u8>>, D::Error> {
        use serde::Deserialize;
        let text: Option<String> = Option::deserialize(deserializer)?;
        text.map(|text| hex::decode(text).map_err(serde::de::Error::custom))
            .transpose()
    }
}

/// One detected state.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreFinding {
    pub class: FindingClass,
    pub severity: FindingSeverity,
    /// The key-tree key, hex encoded in JSON.
    #[serde(with = "hex_bytes", default)]
    pub key: Option<Vec<u8>>,
    pub page_id: Option<PageId>,
    pub locator: Option<TupleLocator>,
    /// Position in the version chain, 0 = the head.
    pub chain_depth: Option<u64>,
    pub found_page_type: Option<String>,
    pub xmin: Option<Xid>,
    pub xmax: Option<Xid>,
    pub detail: String,
}

impl StoreFinding {
    fn new(class: FindingClass) -> Self {
        Self {
            class,
            severity: class.severity(),
            key: None,
            page_id: None,
            locator: None,
            chain_depth: None,
            found_page_type: None,
            xmin: None,
            xmax: None,
            detail: String::new(),
        }
    }
}

fn finding_heap_bytes(finding: &StoreFinding) -> u64 {
    finding.key.as_ref().map_or(0, |key| key.capacity() as u64)
        + finding.detail.capacity() as u64
        + finding
            .found_page_type
            .as_ref()
            .map_or(0, |kind| kind.capacity() as u64)
        + 64
}

/// Count and bounded samples for one class.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FindingClassSummary {
    pub class: FindingClass,
    pub family: String,
    pub severity: FindingSeverity,
    pub count: u64,
    pub description: String,
    pub samples: Vec<StoreFinding>,
}

/// Facts about the store read from its superblock and meta page.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreFacts {
    pub directory: PathBuf,
    pub page_size: u32,
    pub layout: String,
    pub page_files: u64,
    pub superblock_page_count: u64,
    /// Page count after the pending log images recovery would replay.
    pub effective_page_count: u64,
    pub free_list_head: PageId,
    pub free_page_count: u64,
    pub checkpoint_lsn: u64,
    pub meta_page: PageId,
    pub meta_initialized: bool,
    pub index_root: PageId,
    pub catalog_root: PageId,
    pub frozen_xid: Xid,
    pub next_xid: Xid,
    pub abort_exceptions: u64,
    pub status_spill_pages: u64,
    pub status_spill_entries: u64,
}

/// Evidence that the run could not have modified the store.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReadOnlyEvidence {
    /// The directory lock as probed (`F_GETLK`; never taken, never created).
    pub lock: DirectoryLockProbe,
    /// Every file the run opened, each read-only.
    pub files_opened_read_only: Vec<PathBuf>,
    pub recovery_ran: bool,
    pub note: String,
}

/// What `stale_records_discarded` would say.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StaleRecordsDiscardedState {
    /// The crash-ordered reset fix reports discarded stale segments in the
    /// `RecoveryReport` of the open that discards them and writes nothing
    /// durable about it; a store already cleaned up carries no trace.
    pub recorded_in_store: bool,
    /// Sealed segments the fixed open would discard from these files now.
    pub segments_a_fixed_open_would_discard: u64,
    pub explanation: String,
}

/// The page log as recovery would see it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogFacts {
    pub sealed_segments: u64,
    pub sealed_bytes: u64,
    pub active_bytes: u64,
    pub reset_shape: LogResetShape,
    pub replay_scope: String,
    pub records: u64,
    pub first_lsn: Option<u64>,
    pub last_lsn: Option<u64>,
    pub torn_tail_bytes: u64,
    pub checkpoint_records: u64,
    pub redo_lsn: u64,
    pub committed_transactions: u64,
    /// Committed page images recovery would replay; the verifier reads these
    /// pages from the log.
    pub pending_page_images: u64,
    pub distinct_pages_overlaid: u64,
    pub uncommitted_images_skipped: u64,
    /// Replay's page-count growth truncates the monolithic page file here;
    /// pages above it that no replayed image covers read as zeros after a
    /// real open (the verifier reads them that way).
    pub page_file_truncated_by_recovery_from: Option<u64>,
    pub open_would_refuse: Option<String>,
    pub stale_records_discarded: StaleRecordsDiscardedState,
}

/// Exact counters.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreVerifyCounters {
    pub index_entries: u64,
    pub index_tree_pages: u64,
    pub index_tree_height: u64,
    pub catalog_entries: u64,
    pub catalog_tree_pages: u64,
    pub directory_heap_pages: u64,
    pub free_space_records: u64,
    pub versions_examined: u64,
    /// Keys whose visible committed version exists and is not deleted.
    pub visible_rows: u64,
    /// Keys whose visible committed version is deleted (not yet swept).
    pub deleted_rows: u64,
    /// Keys whose chain holds no committed version at all.
    pub keys_without_committed_version: u64,
    /// Chain links below a committed version that vacuum legitimately cut.
    pub vacuumed_links: u64,
    pub heap_pages_swept: u64,
    pub heap_versions_swept: u64,
    pub reachable_versions_swept: u64,
    pub key_tree_passes: u64,
    pub pages_read_from_page_files: u64,
    pub pages_read_from_log: u64,
    pub page_cache_hits: u64,
    pub bytes_read: u64,
    pub slot_bitmap_window_pages: u64,
    pub peak_bitmap_bytes: u64,
    pub elapsed_millis: u64,
}

/// The verifier's answer.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreVerifyReport {
    pub format_version: u32,
    pub verdict: StoreVerdict,
    /// False when a limit stopped the run; findings are then a lower bound.
    pub complete: bool,
    pub stop_reason: Option<String>,
    pub store: StoreFacts,
    pub read_only: ReadOnlyEvidence,
    pub log: LogFacts,
    pub counters: StoreVerifyCounters,
    /// Every class with a non-zero count, most severe first.
    pub classes: Vec<FindingClassSummary>,
    /// What settles the classes that need an external cross-check.
    pub external_evidence: Vec<String>,
}

impl StoreVerifyReport {
    /// Exact count of one class.
    pub fn count(&self, class: FindingClass) -> u64 {
        self.classes
            .iter()
            .find(|summary| summary.class == class)
            .map_or(0, |summary| summary.count)
    }
}

/// Verify the store in `dir` (the directory holding `store.pages`) without
/// modifying it.
pub fn verify_store_read_only(
    dir: &Path,
    options: &StoreVerifyOptions,
) -> Result<StoreVerifyReport> {
    verify_store_read_only_streaming(dir, options, &mut |_| {})
}

/// As [`verify_store_read_only`], also handing every finding (not only the
/// sampled ones) to `sink` as it is found.
pub fn verify_store_read_only_streaming(
    dir: &Path,
    options: &StoreVerifyOptions,
    sink: &mut dyn FnMut(&StoreFinding),
) -> Result<StoreVerifyReport> {
    if options.max_memory_bytes < 1024 * 1024
        || options.max_samples_per_class > 100_000
        || options.max_versions_per_chain == 0
        || options.page_cache_pages == 0
        || options.page_cache_pages > 1 << 20
    {
        return Err(PageError::InvalidMaintenanceLimits {
            reason: "store verification needs max_memory_bytes >= 1 MiB, a non-zero chain limit, \
                     1..=1048576 cache pages and at most 100000 samples per class"
                .to_string(),
        });
    }
    let started = Instant::now();
    let lock = probe_directory_lock(dir)?;
    if lock.is_held() && !options.allow_live_store {
        return Err(PageError::AlreadyOpen {
            path: dir.join("store.lock"),
        });
    }
    let mut verifier = match Verifier::open(dir, options, lock, started, sink) {
        Ok(verifier) => verifier,
        Err(PageError::InvalidMaintenanceLimits { reason })
            if reason.starts_with("read-only verification limit:") =>
        {
            return Ok(setup_stopped_report(dir, lock, started, reason));
        }
        Err(error) => return Err(error),
    };
    verifier.run();
    Ok(verifier.finish())
}

fn setup_stopped_report(
    dir: &Path,
    lock: DirectoryLockProbe,
    started: Instant,
    reason: String,
) -> StoreVerifyReport {
    StoreVerifyReport {
        format_version: STORE_VERIFY_REPORT_FORMAT_VERSION,
        verdict: StoreVerdict::NeedsExternalCrossCheck,
        complete: false,
        stop_reason: Some(if reason.contains("time limit") { "time limit" } else { "memory limit" }.to_string()),
        store: StoreFacts { directory: dir.to_path_buf(), ..Default::default() },
        read_only: ReadOnlyEvidence {
            lock,
            files_opened_read_only: Vec::new(),
            recovery_ran: false,
            note: format!("{reason}. Setup stopped before file inventory and recovery analysis completed; no engine was opened and all setup access was read-only."),
        },
        log: LogFacts {
            sealed_segments: 0, sealed_bytes: 0, active_bytes: 0,
            reset_shape: LogResetShape::Normal, replay_scope: "not_analyzed".to_string(),
            records: 0, first_lsn: None, last_lsn: None, torn_tail_bytes: 0,
            checkpoint_records: 0, redo_lsn: 0, committed_transactions: 0,
            pending_page_images: 0, distinct_pages_overlaid: 0, uncommitted_images_skipped: 0,
            page_file_truncated_by_recovery_from: None, open_would_refuse: None,
            stale_records_discarded: StaleRecordsDiscardedState {
                recorded_in_store: false, segments_a_fixed_open_would_discard: 0,
                explanation: "Setup limit reached; log reset state was not established.".to_string(),
            },
        },
        counters: StoreVerifyCounters {
            elapsed_millis: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            ..Default::default()
        },
        classes: Vec::new(),
        external_evidence: vec!["Run the verifier again with sufficient limits; this incomplete result is not a clean-store verdict.".to_string()],
    }
}

// ---------------------------------------------------------------------------
// Page access.

/// Why a page could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PageFault {
    OutOfBounds,
    Short {
        got: usize,
    },
    Checksum,
    Torn,
    NotAPage,
    UnknownType(u8),
    WrongPageId(PageId),
    Unsupported(String),
    Io(String),
    /// Recovery's page-count growth truncates the file below this page and
    /// no replayed image rewrites it: zeros after a real open.
    ZeroedByRecovery,
}

impl std::fmt::Display for PageFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfBounds => write!(f, "beyond the page count"),
            Self::Short { got } => write!(f, "short read ({got} bytes): never written"),
            Self::Checksum => write!(f, "checksum mismatch"),
            Self::Torn => write!(f, "torn write (header and trailer generations differ)"),
            Self::NotAPage => write!(f, "no page magic"),
            Self::UnknownType(found) => write!(f, "unknown page type {found}"),
            Self::WrongPageId(stored) => write!(f, "page records id {stored}"),
            Self::Unsupported(what) => write!(f, "unsupported: {what}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::ZeroedByRecovery => write!(
                f,
                "recovery truncates the page file below this page and replays no image of it: \
                 it reads as zeros after a real open"
            ),
        }
    }
}

type Page = Rc<[u8]>;

struct PageSource {
    page_size: u32,
    superblock: Superblock,
    segments: Vec<(PathBuf, File)>,
    log: ReadOnlyLog,
    page_count: u64,
    cache: Vec<Option<(PageId, Page)>>,
    pages_from_files: u64,
    pages_from_log: u64,
    cache_hits: u64,
    bytes_read: u64,
}

impl PageSource {
    fn locate(&self, page_id: PageId) -> (usize, u64) {
        let page_size = u64::from(self.page_size);
        let extent = self.superblock.extent_pages;
        if extent > 0 {
            return ((page_id / extent) as usize, (page_id % extent) * page_size);
        }
        let target = self.superblock.migration_target_pages;
        if target > 0 && page_id < self.superblock.migration_watermark {
            return ((page_id / target) as usize, (page_id % target) * page_size);
        }
        (0, page_id * page_size)
    }

    fn read_uncached(&mut self, page_id: PageId) -> std::result::Result<Page, PageFault> {
        if page_id >= self.page_count {
            return Err(PageFault::OutOfBounds);
        }
        let mut buffer = vec![0_u8; self.page_size as usize];
        let from_log = match self.log.images.get(&page_id).copied() {
            Some(image) => {
                self.log
                    .read_image(image, &mut buffer)
                    .map_err(|error| PageFault::Io(error.to_string()))?;
                self.pages_from_log += 1;
                true
            }
            None if self.log.truncated_from.is_some_and(|from| page_id >= from) => {
                return Err(PageFault::ZeroedByRecovery);
            }
            None => {
                let (segment, offset) = self.locate(page_id);
                let Some((path, file)) = self.segments.get(segment) else {
                    return Err(PageFault::Short { got: 0 });
                };
                let got = crate::pio::read_at(file, &mut buffer, offset)
                    .map_err(|error| PageFault::Io(format!("{}: {error}", path.display())))?;
                self.pages_from_files += 1;
                if got != buffer.len() {
                    self.bytes_read += got as u64;
                    return Err(PageFault::Short { got });
                }
                false
            }
        };
        self.bytes_read += buffer.len() as u64;
        // A logged image is covered by its log record's CRC; its checksum
        // and trailer are stamped only when recovery writes it out.
        if !from_log {
            match crate::page::verify(&buffer, page_id) {
                Ok(()) => {}
                Err(PageError::ChecksumMismatch { .. }) => {
                    return Err(if buffer[0..4] == crate::page::PAGE_MAGIC {
                        PageFault::Checksum
                    } else {
                        PageFault::NotAPage
                    })
                }
                Err(PageError::TornPage { .. }) => return Err(PageFault::Torn),
                Err(error) => return Err(PageFault::Io(error.to_string())),
            }
        }
        let header = match PageHeader::decode(&buffer, Path::new("")) {
            Ok(header) => header,
            Err(PageError::UnexpectedPageType { found, .. }) => {
                return Err(PageFault::UnknownType(found))
            }
            Err(PageError::NotAPageFile { .. }) => return Err(PageFault::NotAPage),
            Err(error) => return Err(PageFault::Unsupported(error.to_string())),
        };
        if header.page_id != page_id {
            return Err(PageFault::WrongPageId(header.page_id));
        }
        if header.is_compressed() || header.is_encrypted() {
            return Err(PageFault::Unsupported(
                "compressed or encrypted page image".to_string(),
            ));
        }
        Ok(Rc::from(buffer.into_boxed_slice()))
    }

    fn read(&mut self, page_id: PageId) -> std::result::Result<Page, PageFault> {
        let slot = (page_id % self.cache.len() as u64) as usize;
        if let Some((cached, page)) = &self.cache[slot] {
            if *cached == page_id {
                self.cache_hits += 1;
                return Ok(page.clone());
            }
        }
        let page = self.read_uncached(page_id)?;
        self.cache[slot] = Some((page_id, page.clone()));
        Ok(page)
    }
}

fn page_type(page: &[u8]) -> PageType {
    // Only called on pages `PageHeader::decode` accepted.
    PageType::from_u8(page[6]).unwrap_or(PageType::Free)
}

fn page_type_name(page_type: PageType) -> &'static str {
    match page_type {
        PageType::Superblock => "superblock",
        PageType::Heap => "heap",
        PageType::Overflow => "overflow",
        PageType::BTreeInterior => "btree_interior",
        PageType::BTreeLeaf => "btree_leaf",
        PageType::Free => "free",
    }
}

/// A bounds-checked slot lookup, safe on any page bytes.
enum SlotLookup<'a> {
    Live(&'a [u8]),
    NoSuchSlot,
    Dead,
    OtherGeneration(u32),
    OutOfPage,
}

fn slot_count(page: &[u8]) -> u16 {
    u16::from_le_bytes([page[40], page[41]])
}

fn slot_entry(page: &[u8], slot: u16) -> Option<(u16, u16, u32)> {
    if slot >= slot_count(page) {
        return None;
    }
    let at = PAGE_HEADER_BYTES + slot as usize * SLOT_BYTES;
    if at + SLOT_BYTES > page.len() - PAGE_TRAILER_BYTES {
        return None;
    }
    Some((
        u16::from_le_bytes([page[at], page[at + 1]]),
        u16::from_le_bytes([page[at + 2], page[at + 3]]),
        u32::from_le_bytes(page[at + 4..at + 8].try_into().unwrap()),
    ))
}

fn tuple_bytes(page: &[u8], offset: u16, len: u16) -> Option<&[u8]> {
    let start = offset as usize;
    let end = start.checked_add(len as usize)?;
    (start >= PAGE_HEADER_BYTES && end <= page.len() - PAGE_TRAILER_BYTES)
        .then(|| &page[start..end])
}

fn lookup_slot(page: &[u8], locator: TupleLocator) -> SlotLookup<'_> {
    let Some((offset, len, generation)) = slot_entry(page, locator.slot) else {
        return SlotLookup::NoSuchSlot;
    };
    if offset == 0 {
        return SlotLookup::Dead;
    }
    if generation != locator.generation {
        return SlotLookup::OtherGeneration(generation);
    }
    match tuple_bytes(page, offset, len) {
        Some(bytes) => SlotLookup::Live(bytes),
        None => SlotLookup::OutOfPage,
    }
}

/// A plain bitmap over page ids or slots.
struct Bits(Vec<u64>);

impl Bits {
    fn new(bits: u64) -> Self {
        Self(vec![0; bits.div_ceil(64) as usize])
    }
    fn bytes(&self) -> u64 {
        self.0.len() as u64 * 8
    }
    fn get(&self, bit: u64) -> bool {
        self.0
            .get((bit / 64) as usize)
            .is_some_and(|word| word & (1 << (bit % 64)) != 0)
    }
    /// Set a bit, returning whether it was already set.
    fn set(&mut self, bit: u64) -> bool {
        match self.0.get_mut((bit / 64) as usize) {
            Some(word) => {
                let mask = 1 << (bit % 64);
                let was = *word & mask != 0;
                *word |= mask;
                was
            }
            None => false,
        }
    }
    fn clear(&mut self) {
        self.0.iter_mut().for_each(|word| *word = 0);
    }
}

// ---------------------------------------------------------------------------
// Transaction status, resolved as an open would.

struct StatusResolver {
    frozen_xid: Xid,
    exceptions: Vec<Xid>,
    /// (page, min xid, max xid) of each status-spill page with entries.
    spill: Vec<(PageId, Xid, Xid)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Committed,
    Aborted,
    Unresolved,
}

// ---------------------------------------------------------------------------
// The verifier.

enum Resolution {
    Live {
        bytes: Vec<u8>,
        on_free_page: bool,
        /// Set when the page is of another type and the slot resolves anyway.
        foreign: Option<String>,
    },
    BeyondPageCount,
    Unreadable(PageFault),
    Reused {
        page_type: String,
    },
    FreeWithoutSlot,
    StaleHeapSlot(String),
    OutOfPage,
}

struct TreeFrame {
    page_id: PageId,
    depth: u64,
    low: Option<Rc<[u8]>>,
    high: Option<Rc<[u8]>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TreeKind {
    Index,
    Catalog,
}

struct Verifier<'a> {
    options: StoreVerifyOptions,
    started: Instant,
    sink: &'a mut dyn FnMut(&StoreFinding),
    source: PageSource,
    facts: StoreFacts,
    lock: DirectoryLockProbe,
    files_opened: Vec<PathBuf>,
    status: StatusResolver,
    /// Meta page and status-spill pages: typed Heap but not slotted.
    special_pages: FxHashSet<PageId>,
    /// Pages either tree reaches (pass 0).
    tree_pages: Bits,
    /// Pages the current tree walk has reached.
    visited: Bits,
    in_directory: Bits,
    slot_bits_per_page: u64,
    slots: Bits,
    window: (PageId, PageId),
    pass: u64,
    counters: StoreVerifyCounters,
    summaries: Vec<FindingClassSummary>,
    sample_bytes: u64,
    complete: bool,
    stop_reason: Option<String>,
    deadline: Option<Instant>,
    empty_store: bool,
    /// Pages already reported as unreadable through a reference.
    unreadable_reported: FxHashSet<PageId>,
}

impl<'a> Verifier<'a> {
    fn open(
        dir: &Path,
        options: &StoreVerifyOptions,
        lock: DirectoryLockProbe,
        started: Instant,
        sink: &'a mut dyn FnMut(&StoreFinding),
    ) -> Result<Self> {
        let deadline = options
            .max_duration
            .and_then(|limit| started.checked_add(limit));
        let setup_limits = ReadOnlyScanLimits::new(options.max_memory_bytes / 2, deadline);
        setup_limits.check(0)?;
        let mut files_opened = Vec::new();
        let base = dir.join("store.pages");
        let mut first = open_read_only(&base).map_err(|error| PageError::io(&base, error))?;
        files_opened.push(base.clone());
        // Learn the page size from the superblock prefix, as
        // `PageStore::open_existing_auto` does, then read page 0 whole.
        let mut prefix = [0_u8; PAGE_HEADER_BYTES + 12];
        {
            use std::io::Read as _;
            first
                .read_exact(&mut prefix)
                .map_err(|error| PageError::io(&base, error))?;
        }
        let page_size = u32::from_le_bytes(prefix[PAGE_HEADER_BYTES + 8..].try_into().unwrap());
        crate::page::validate_page_size(page_size)?;
        let mut page0 = vec![0_u8; page_size as usize];
        let got = crate::pio::read_at(&first, &mut page0, 0)
            .map_err(|error| PageError::io(&base, error))?;
        if got != page0.len() {
            return Err(PageError::ShortRead {
                page_id: 0,
                got,
                want: page0.len(),
            });
        }
        crate::page::verify(&page0, 0)?;
        let (superblock, _) = decode_superblock(&page0, &base, page_size)?;
        // Never allocate bitmaps from an unchecked on-disk count. A valid
        // checksum does not establish that the advertised count is sensible.
        setup_limits.check(superblock.page_count.div_ceil(64).saturating_mul(24))?;

        let mut segments = vec![(base.clone(), first)];
        if superblock.extent_pages > 0 || superblock.migration_target_pages > 0 {
            loop {
                setup_limits.check((segments.len() as u64 + 1).saturating_mul(512))?;
                let path = segment_path_for(&base, segments.len());
                match open_read_only(&path) {
                    Ok(file) => {
                        files_opened.push(path.clone());
                        segments.push((path, file));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Err(error) => return Err(PageError::io(&path, error)),
                }
            }
        }

        let wal_path = dir.join("store.wal");
        let monolithic_file_pages = if superblock.extent_pages == 0 {
            let length = segments[0]
                .1
                .metadata()
                .map_err(|error| PageError::io(&base, error))?
                .len();
            Some(length / u64::from(page_size))
        } else {
            None
        };
        let log = analyze_log_read_only(
            &wal_path,
            page_size,
            superblock.checkpoint_lsn,
            superblock.page_count,
            monolithic_file_pages,
            &open_read_only,
            &setup_limits,
        )?;
        files_opened.extend(log.files.iter().map(|(path, _)| path.clone()));

        let page_count = superblock.page_count.max(
            log.max_replayed_page
                .map_or(0, |page| page.saturating_add(1)),
        );
        setup_limits.check(page_count.div_ceil(64).saturating_mul(24))?;
        let cache_pages = options
            .page_cache_pages
            .min((options.max_memory_bytes / 4 / u64::from(page_size)).max(1) as usize);
        let fixed_state = page_count
            .div_ceil(64)
            .saturating_mul(24)
            .saturating_add(log.images.len() as u64 * 128)
            .saturating_add(log.outcomes.len() as u64 * 32)
            .saturating_add(files_opened.len() as u64 * 512)
            .saturating_add(cache_pages as u64 * u64::from(page_size))
            .saturating_add(4 * u64::from(page_size));
        ReadOnlyScanLimits::new(options.max_memory_bytes, deadline).check(fixed_state)?;
        let source = PageSource {
            page_size,
            superblock,
            segments,
            log,
            page_count,
            cache: (0..cache_pages).map(|_| None).collect(),
            pages_from_files: 0,
            pages_from_log: 0,
            cache_hits: 0,
            bytes_read: 0,
        };
        let layout = if superblock.extent_pages > 0 {
            "segmented"
        } else if superblock.migration_target_pages > 0 {
            "migrating"
        } else {
            "monolithic"
        };
        let facts = StoreFacts {
            directory: dir.to_path_buf(),
            page_size,
            layout: layout.to_string(),
            page_files: source.segments.len() as u64,
            superblock_page_count: superblock.page_count,
            effective_page_count: page_count,
            free_list_head: superblock.free_list_head,
            free_page_count: superblock.free_page_count,
            checkpoint_lsn: superblock.checkpoint_lsn,
            meta_page: superblock.root_page,
            ..Default::default()
        };
        // Bits per page for the slot bitmap: the most slots a page can hold.
        let slot_bits_per_page =
            ((page_size as usize - PAGE_HEADER_BYTES - PAGE_TRAILER_BYTES) / SLOT_BYTES) as u64;
        Ok(Self {
            options: options.clone(),
            started,
            sink,
            source,
            facts,
            lock,
            files_opened,
            status: StatusResolver {
                frozen_xid: 0,
                exceptions: Vec::new(),
                spill: Vec::new(),
            },
            special_pages: FxHashSet::default(),
            tree_pages: Bits::new(page_count),
            visited: Bits::new(page_count),
            in_directory: Bits::new(page_count),
            slot_bits_per_page,
            slots: Bits::new(0),
            window: (0, 0),
            pass: 0,
            counters: StoreVerifyCounters::default(),
            summaries: Vec::new(),
            sample_bytes: 0,
            complete: true,
            stop_reason: None,
            deadline,
            empty_store: false,
            unreadable_reported: FxHashSet::default(),
        })
    }

    fn report(&mut self, finding: StoreFinding) {
        let bytes = finding_heap_bytes(&finding) + 3 * std::mem::size_of::<StoreFinding>() as u64;
        let retained = self.options.max_samples_per_class > 0
            && self
                .summaries
                .iter()
                .find(|s| s.class == finding.class)
                .is_none_or(|s| s.samples.len() < self.options.max_samples_per_class)
            && self.sample_bytes.saturating_add(bytes) <= self.options.max_memory_bytes / 4;
        if !self.within_budget(0) {
            return;
        }
        if retained {
            self.sample_bytes += bytes;
        }
        (self.sink)(&finding);
        let limit = self.options.max_samples_per_class;
        match self
            .summaries
            .iter_mut()
            .find(|summary| summary.class == finding.class)
        {
            Some(summary) => {
                summary.count += 1;
                if retained && summary.samples.len() < limit {
                    summary.samples.push(finding);
                }
            }
            None => {
                let class = finding.class;
                self.summaries.push(FindingClassSummary {
                    class,
                    family: class.family().to_string(),
                    severity: class.severity(),
                    count: 1,
                    description: class.description().to_string(),
                    samples: if retained && limit > 0 {
                        vec![finding]
                    } else {
                        Vec::new()
                    },
                });
            }
        }
    }

    fn out_of_time(&mut self) -> bool {
        if !self.complete {
            return true;
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.complete = false;
            self.stop_reason = Some("time limit".to_string());
            return true;
        }
        if self.estimated_state_bytes() > self.options.max_memory_bytes {
            self.complete = false;
            self.stop_reason = Some("memory limit".to_string());
            return true;
        }
        false
    }

    fn estimated_state_bytes(&self) -> u64 {
        self.tree_pages.bytes() + self.visited.bytes() + self.in_directory.bytes() + self.slots.bytes()
            + self.source.cache.len() as u64 * u64::from(self.source.page_size)
            + self.source.log.images.len() as u64 * 128
            + self.source.log.outcomes.len() as u64 * 32
            + (self.special_pages.capacity() + self.unreadable_reported.capacity()) as u64 * 64
            + self.status.spill.capacity() as u64 * 24
            + self.status.exceptions.capacity() as u64 * 8
            + self.sample_bytes
            // Temporary decoded pages, entry copies and formatting buffers.
            + 4 * u64::from(self.source.page_size)
    }

    fn within_budget(&mut self, temporary_bytes: u64) -> bool {
        if self.out_of_time() {
            return false;
        }
        if self.estimated_state_bytes().saturating_add(temporary_bytes)
            > self.options.max_memory_bytes
        {
            self.complete = false;
            self.stop_reason = Some("memory limit".to_string());
            return false;
        }
        true
    }

    fn run(&mut self) {
        if self.out_of_time() {
            return;
        }
        self.check_log();
        if !self.load_meta() {
            return;
        }
        if self.empty_store {
            return;
        }
        self.load_status_spill();
        if self.out_of_time() {
            return;
        }
        let catalog_root = self.facts.catalog_root;
        if catalog_root != 0 {
            self.walk_tree(catalog_root, TreeKind::Catalog, true);
        }

        // Size the heap slot-bitmap window from what is left of the budget.
        let page_count = self.source.page_count;
        let fixed = self
            .estimated_state_bytes()
            .saturating_add(self.options.max_memory_bytes / 4);
        let available = self
            .options
            .max_memory_bytes
            .saturating_sub(fixed)
            .max(64 * 1024);
        let window_pages = self
            .options
            .heap_window_pages
            .unwrap_or(available * 8 / self.slot_bits_per_page)
            .min(available * 8 / self.slot_bits_per_page)
            .clamp(1, page_count.max(1));
        self.counters.slot_bitmap_window_pages = window_pages;
        self.slots = Bits::new(window_pages * self.slot_bits_per_page);

        let index_root = self.facts.index_root;
        let mut low = 0;
        while low < page_count {
            if self.out_of_time() {
                break;
            }
            let high = low.saturating_add(window_pages).min(page_count);
            self.window = (low, high);
            self.slots.clear();
            if index_root != 0 {
                let first = self.pass == 0;
                self.walk_tree(index_root, TreeKind::Index, first);
            }
            self.counters.key_tree_passes += 1;
            if self.out_of_time() {
                break;
            }
            self.sweep_window(low, high);
            self.pass += 1;
            low = high;
        }
        self.counters.peak_bitmap_bytes = self.tree_pages.bytes()
            + self.visited.bytes()
            + self.in_directory.bytes()
            + self.slots.bytes();
    }

    fn check_log(&mut self) {
        let shape = self.source.log.shape.clone();
        match shape {
            LogResetShape::Normal => {}
            LogResetShape::InterruptedReset {
                stale_segments,
                stale_last_lsn,
                stale_images_replayed_by_unfixed_open,
            } => {
                let mut finding = StoreFinding::new(FindingClass::LogInterruptedReset);
                finding.detail = format!(
                    "{stale_segments} sealed segment(s) end at LSN {stale_last_lsn}, before the \
                     checkpoint record {} of horizon {}, and the active log is empty. The page \
                     file is newer than every record and was verified as is. An engine without \
                     the crash-ordered reset fix replays {stale_images_replayed_by_unfixed_open} \
                     committed page image(s) over newer pages at its next open: do not open this \
                     store with such an engine; preserve the files.",
                    self.facts.checkpoint_lsn.saturating_sub(1),
                    self.facts.checkpoint_lsn
                );
                self.report(finding);
            }
            LogResetShape::StaleSealedBeforeActive {
                stale_segments,
                stale_last_lsn,
                active_records,
            } => {
                let mut finding = StoreFinding::new(FindingClass::LogStaleChainAlreadyReplayed);
                finding.detail = format!(
                    "{stale_segments} stale sealed segment(s) end at LSN {stale_last_lsn}, and the \
                     active log ({active_records} record(s)) starts at the horizon {}: an engine \
                     without the reset fix opened this store after an interrupted reset, replayed \
                     the stale chain over newer pages, and kept committing. Rows last written \
                     before that reset may read as older committed versions; nothing in the \
                     store distinguishes them. The same engine's next open discards the active \
                     log as a torn tail. The verifier read the active log only, as the fixed \
                     engine would.",
                    self.facts.checkpoint_lsn
                );
                self.report(finding);
            }
            LogResetShape::EndsBeforeHorizon { reason } => {
                let mut finding = StoreFinding::new(FindingClass::LogEndsBeforeHorizon);
                finding.detail = format!("{reason}; the page file was verified without the log");
                self.report(finding);
            }
        }
        if let Some(reason) = self.source.log.corruption.clone() {
            let mut finding = StoreFinding::new(FindingClass::LogRefusedByOpen);
            finding.detail = format!("{reason}; the page file was verified without the log");
            self.report(finding);
        }
    }

    /// Read the meta page. Returns false when the store cannot be verified
    /// further (the failure is reported).
    fn load_meta(&mut self) -> bool {
        let meta_page = self.facts.meta_page;
        if meta_page == 0 {
            self.empty_store = true;
            return true;
        }
        let page = match self.source.read(meta_page) {
            Ok(page) => page,
            Err(fault) => {
                let mut finding = StoreFinding::new(FindingClass::ReferencedPageUnreadable);
                finding.page_id = Some(meta_page);
                finding.detail = format!("the meta page cannot be read: {fault}");
                self.report(finding);
                return false;
            }
        };
        match crate::paged::decode_meta(&page, meta_page, true) {
            Ok(meta) => {
                if !meta.initialized {
                    self.empty_store = true;
                    return true;
                }
                if meta.legacy {
                    let mut finding = StoreFinding::new(FindingClass::MetaNeedsMigration);
                    finding.page_id = Some(meta_page);
                    finding.detail = "the extension region is invalid; a default open refuses \
                                      this store until `store migrate-meta` runs; verified with \
                                      no abort exceptions"
                        .to_string();
                    self.report(finding);
                }
                self.facts.meta_initialized = true;
                self.facts.index_root = meta.index_root;
                self.facts.catalog_root = meta.catalog_root;
                self.facts.frozen_xid = meta.frozen_xid;
                self.facts.next_xid = meta.next_xid;
                self.facts.abort_exceptions = meta.abort_exceptions.len() as u64;
                self.facts.status_spill_pages = meta.spill_pages;
                self.facts.status_spill_entries = meta.spill_entries;
                self.status.frozen_xid = meta.frozen_xid;
                self.status.exceptions = meta.abort_exceptions;
                self.special_pages.insert(meta_page);
                if meta.spill_head != 0 {
                    self.status.spill.push((meta.spill_head, 0, 0));
                }
                true
            }
            Err(error) => {
                let mut finding = StoreFinding::new(FindingClass::TreeNodeMalformed);
                finding.page_id = Some(meta_page);
                finding.found_page_type = Some("meta".to_string());
                finding.detail = format!("the meta page cannot be decoded: {error}");
                self.report(finding);
                false
            }
        }
    }

    fn load_status_spill(&mut self) {
        let Some((head, _, _)) = self.status.spill.pop() else {
            return;
        };
        let pages = self.facts.status_spill_pages;
        let mut page_id = head;
        let mut chain = Vec::new();
        for index in 0..pages {
            if !self.within_budget((chain.capacity() as u64 + 1).saturating_mul(48)) {
                return;
            }
            if !self.special_pages.insert(page_id) {
                self.spill_fault(page_id, format!("status spill repeats page {page_id}"));
                return;
            }
            let page = match self.source.read(page_id) {
                Ok(page) => page,
                Err(fault) => {
                    self.spill_fault(page_id, format!("status spill page {index}: {fault}"));
                    return;
                }
            };
            match crate::paged::parse_spill_page(&page, self.source.page_size) {
                Ok((entries, min, max)) => {
                    if entries > 0 {
                        chain.push((page_id, min, max));
                    }
                }
                Err(error) => {
                    self.spill_fault(page_id, format!("status spill page {index}: {error}"));
                    return;
                }
            }
            if index + 1 < pages {
                page_id = u64::from_le_bytes(page[44..52].try_into().unwrap());
            }
        }
        self.status.spill = chain;
    }

    fn spill_fault(&mut self, page_id: PageId, detail: String) {
        let mut finding = StoreFinding::new(FindingClass::StatusSpillUnreadable);
        finding.page_id = Some(page_id);
        finding.detail = format!("{detail}; statuses it would resolve are reported as unresolved");
        self.report(finding);
        self.status.spill.clear();
        self.status.spill.push((0, 1, u64::MAX));
    }

    /// Status as `TransactionTable::status` resolves it after an open: below
    /// the frozen watermark committed unless excepted, then the log's
    /// outcomes, then the spill; anything else died with an earlier process.
    fn status_of(&mut self, xid: Xid) -> Status {
        if xid == 0 {
            return Status::Committed;
        }
        if xid < self.status.frozen_xid {
            return if self.status.exceptions.binary_search(&xid).is_ok() {
                Status::Aborted
            } else {
                Status::Committed
            };
        }
        match self.source.log.outcome(xid) {
            Some(TxStatus::Committed) => return Status::Committed,
            Some(TxStatus::Aborted) => return Status::Aborted,
            _ => {}
        }
        let spill_page = self
            .status
            .spill
            .iter()
            .find(|(_, min, max)| *min <= xid && xid <= *max)
            .copied();
        if let Some((page_id, _, _)) = spill_page {
            if page_id == 0 {
                return Status::Unresolved;
            }
            let Ok(page) = self.source.read(page_id) else {
                return Status::Unresolved;
            };
            let body = &page[PAGE_HEADER_BYTES..];
            let count = u64::from_le_bytes(body[0..8].try_into().unwrap()) as usize;
            let (mut low, mut high) = (0_usize, count);
            while low < high {
                let middle = (low + high) / 2;
                let at = 8 + middle * 9;
                let Some(entry) = body.get(at..at + 9) else {
                    return Status::Unresolved;
                };
                let found = u64::from_be_bytes(entry[0..8].try_into().unwrap());
                match found.cmp(&xid) {
                    std::cmp::Ordering::Less => low = middle + 1,
                    std::cmp::Ordering::Greater => high = middle,
                    std::cmp::Ordering::Equal => {
                        return match entry[8] {
                            1 => Status::Committed,
                            2 => Status::Aborted,
                            _ => Status::Unresolved,
                        }
                    }
                }
            }
        }
        Status::Aborted
    }

    // --- Trees ----------------------------------------------------------------

    fn walk_tree(&mut self, root: PageId, kind: TreeKind, first_pass: bool) {
        self.visited.clear();
        let mut stack = vec![TreeFrame {
            page_id: root,
            depth: 0,
            low: None,
            high: None,
        }];
        let mut leaf_depth: Option<u64> = None;
        // The previous leaf in key order and its sibling link.
        let mut previous_leaf: Option<(PageId, PageId)> = None;
        let mut steps = 0_u64;
        while let Some(frame) = stack.pop() {
            let stack_bytes = stack.capacity() as u64 * std::mem::size_of::<TreeFrame>() as u64
                + stack
                    .iter()
                    .map(|frame| {
                        frame.low.as_ref().map_or(0, |key| key.len() as u64 + 32)
                            + frame.high.as_ref().map_or(0, |key| key.len() as u64 + 32)
                    })
                    .sum::<u64>();
            if !self.within_budget(stack_bytes) {
                return;
            }
            steps += 1;
            if steps % 256 == 0 && self.out_of_time() {
                return;
            }
            let page_id = frame.page_id;
            let tree_name = match kind {
                TreeKind::Index => "key tree",
                TreeKind::Catalog => "catalog tree",
            };
            if frame.depth > u64::from(crate::btree::MAX_BTREE_VERIFY_HEIGHT) {
                if first_pass {
                    let mut finding = StoreFinding::new(FindingClass::TreeShapeInvalid);
                    finding.page_id = Some(page_id);
                    finding.detail = format!("{tree_name} deeper than any legitimate tree");
                    self.report(finding);
                }
                continue;
            }
            if page_id == 0 || page_id >= self.source.page_count {
                if first_pass {
                    let mut finding = StoreFinding::new(FindingClass::TreePageOutOfBounds);
                    finding.page_id = Some(page_id);
                    finding.detail = format!(
                        "{tree_name} edge at depth {} leads to page {page_id}; the page count is \
                         {}; unreachable {}",
                        frame.depth,
                        self.source.page_count,
                        key_range(&frame)
                    );
                    self.report(finding);
                }
                continue;
            }
            if self.visited.set(page_id) {
                if first_pass {
                    let mut finding = StoreFinding::new(FindingClass::TreeCycle);
                    finding.page_id = Some(page_id);
                    finding.detail = format!(
                        "{tree_name} reaches page {page_id} twice (again at depth {}, {})",
                        frame.depth,
                        key_range(&frame)
                    );
                    self.report(finding);
                }
                continue;
            }
            if first_pass && self.tree_pages.set(page_id) {
                // Reached by the other tree: both trees' readers use it.
                let mut finding = StoreFinding::new(FindingClass::TreeCycle);
                finding.page_id = Some(page_id);
                finding.detail = format!(
                    "page {page_id} belongs to both the key tree and the catalog tree ({tree_name} \
                     at depth {}, {})",
                    frame.depth,
                    key_range(&frame)
                );
                self.report(finding);
            }
            let page = match self.source.read(page_id) {
                Ok(page) => page,
                Err(fault) => {
                    if first_pass {
                        let mut finding = StoreFinding::new(FindingClass::TreePageUnreadable);
                        finding.page_id = Some(page_id);
                        finding.detail = format!(
                            "{tree_name} page at depth {}: {fault}; unreachable {}",
                            frame.depth,
                            key_range(&frame)
                        );
                        self.report(finding);
                    }
                    continue;
                }
            };
            let node = match validate_node(page_id, &page, BTreePageExpectation::Node) {
                Ok(node) => node,
                Err(fault) => {
                    if first_pass {
                        let (class, found) = match &fault {
                            BTreeVerifyFault::UnexpectedPageType { found, .. } => (
                                FindingClass::TreePageWrongType,
                                PageType::from_u8(*found)
                                    .map(|found| page_type_name(found).to_string()),
                            ),
                            _ => (FindingClass::TreeNodeMalformed, None),
                        };
                        let mut finding = StoreFinding::new(class);
                        finding.page_id = Some(page_id);
                        finding.found_page_type = found;
                        finding.detail = format!(
                            "{tree_name} page at depth {}: {fault:?}; unreachable {}",
                            frame.depth,
                            key_range(&frame)
                        );
                        self.report(finding);
                    }
                    continue;
                }
            };
            if first_pass {
                match kind {
                    TreeKind::Index => self.counters.index_tree_pages += 1,
                    TreeKind::Catalog => self.counters.catalog_tree_pages += 1,
                }
            }
            // Keys must stay within the separator range the parent gave.
            let mut out_of_range = None;
            for index in 0..node.count {
                let (key, _) = validated_entry(&page, index);
                let below = frame.low.as_deref().is_some_and(|low| key < low);
                let above = frame.high.as_deref().is_some_and(|high| key >= high);
                if below || above {
                    out_of_range = Some(index);
                    break;
                }
            }
            if let Some(index) = out_of_range {
                if first_pass {
                    let mut finding = StoreFinding::new(FindingClass::TreeKeyOutOfRange);
                    finding.page_id = Some(page_id);
                    finding.key = Some(validated_entry(&page, index).0.to_vec());
                    finding.detail = format!("{tree_name} entry {index} at depth {}", frame.depth);
                    self.report(finding);
                }
            }
            if node.leaf {
                if first_pass {
                    match leaf_depth {
                        None => leaf_depth = Some(frame.depth),
                        Some(depth) if depth != frame.depth => {
                            let mut finding = StoreFinding::new(FindingClass::TreeShapeInvalid);
                            finding.page_id = Some(page_id);
                            finding.detail = format!(
                                "{tree_name} leaf at depth {} where others are at depth {depth}",
                                frame.depth
                            );
                            self.report(finding);
                        }
                        Some(_) => {}
                    }
                    if let Some((previous, link)) = previous_leaf {
                        if link != page_id {
                            self.report_leaf_link(tree_name, previous, link, Some(page_id));
                        }
                    }
                    previous_leaf = Some((page_id, node.next_page));
                }
                // Copy the entries out before processing: processing reads
                // other pages and may evict this one from the cache.
                let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..node.count)
                    .map(|index| {
                        let (key, value) = validated_entry(&page, index);
                        (key.to_vec(), value.to_vec())
                    })
                    .collect();
                drop(page);
                for (key, value) in entries {
                    match kind {
                        TreeKind::Index => self.index_entry(&key, &value, first_pass),
                        TreeKind::Catalog => self.catalog_entry(&key, &value),
                    }
                }
                continue;
            }
            // Interior: child i holds keys < key[i] (and >= key[i-1]); the
            // header link holds keys >= the last separator.
            if !self.within_budget(
                stack_bytes
                    + (node.count as u64 + 1) * 4 * std::mem::size_of::<TreeFrame>() as u64
                    + 2 * u64::from(self.source.page_size),
            ) {
                return;
            }
            let mut children = Vec::with_capacity(node.count + 1);
            let mut low = frame.low.clone();
            for index in 0..node.count {
                let (key, value) = validated_entry(&page, index);
                let child = u64::from_le_bytes(value.try_into().unwrap());
                let high: Rc<[u8]> = Rc::from(key);
                children.push(TreeFrame {
                    page_id: child,
                    depth: frame.depth + 1,
                    low: low.clone(),
                    high: Some(high.clone()),
                });
                low = Some(high);
            }
            children.push(TreeFrame {
                page_id: node.next_page,
                depth: frame.depth + 1,
                low,
                high: frame.high.clone(),
            });
            stack.extend(children.into_iter().rev());
        }
        if first_pass {
            if let Some((previous, link)) = previous_leaf {
                if link != 0 {
                    self.report_leaf_link(tree_name_of(kind), previous, link, None);
                }
            }
            if kind == TreeKind::Index {
                self.counters.index_tree_height = leaf_depth.map_or(0, |depth| depth + 1);
            }
        }
    }

    fn report_leaf_link(
        &mut self,
        tree_name: &str,
        leaf: PageId,
        link: PageId,
        expected: Option<PageId>,
    ) {
        let target = match self.source.read(link) {
            _ if link == 0 => "no sibling".to_string(),
            Ok(page) => format!("a {} page", page_type_name(page_type(&page))),
            Err(fault) => fault.to_string(),
        };
        let mut finding = StoreFinding::new(FindingClass::TreeLeafLinkMismatch);
        finding.page_id = Some(leaf);
        finding.detail = match expected {
            Some(expected) => format!(
                "{tree_name} leaf {leaf} links to page {link} ({target}); the next leaf in key \
                 order is {expected}"
            ),
            None => format!(
                "{tree_name} leaf {leaf} is the last leaf in key order but links to page {link} \
                 ({target})"
            ),
        };
        self.report(finding);
    }

    fn catalog_entry(&mut self, key: &[u8], value: &[u8]) {
        self.counters.catalog_entries += 1;
        match (key.first(), key.len()) {
            (Some(0x00), 9) => {
                let page_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                self.counters.directory_heap_pages += 1;
                self.in_directory.set(page_id);
                let recorded = value.first().copied().and_then(PageType::from_u8);
                let found = self.page_kind(page_id);
                if found.as_deref() != recorded.map(page_type_name) {
                    let mut finding = StoreFinding::new(FindingClass::DirectoryEntryWrongType);
                    finding.page_id = Some(page_id);
                    finding.found_page_type = found;
                    finding.detail = format!(
                        "the directory records page {page_id} as {:?}",
                        recorded.map(page_type_name)
                    );
                    self.report(finding);
                }
            }
            (Some(0x01), 10) => {
                let page_id = u64::from_be_bytes(key[2..10].try_into().unwrap());
                self.counters.free_space_records += 1;
                let found = self.page_kind(page_id);
                if found.as_deref() != Some("heap") {
                    let mut finding = StoreFinding::new(FindingClass::StaleFreeSpaceRecord);
                    finding.page_id = Some(page_id);
                    finding.found_page_type = found.clone();
                    finding.detail = format!(
                        "free-space bucket {} advertises page {page_id}, which is {}",
                        key[1],
                        found.unwrap_or_else(|| "unreadable".to_string())
                    );
                    self.report(finding);
                }
            }
            _ => {}
        }
    }

    /// A page's type name; `meta` / `status_spill` for the special heap-typed
    /// pages; `None` when unreadable or beyond the page count.
    fn page_kind(&mut self, page_id: PageId) -> Option<String> {
        if self.special_pages.contains(&page_id) {
            return Some(if page_id == self.facts.meta_page {
                "meta".to_string()
            } else {
                "status_spill".to_string()
            });
        }
        if page_id == 0 {
            return Some("superblock".to_string());
        }
        self.source
            .read(page_id)
            .ok()
            .map(|page| page_type_name(page_type(&page)).to_string())
    }

    // --- Key-tree entries and version chains ----------------------------------

    fn resolve(&mut self, locator: TupleLocator) -> Resolution {
        let page_id = locator.page_id;
        if page_id >= self.source.page_count {
            return Resolution::BeyondPageCount;
        }
        if page_id == 0 {
            return Resolution::Reused {
                page_type: "superblock".to_string(),
            };
        }
        if self.special_pages.contains(&page_id) {
            let page_type = if page_id == self.facts.meta_page {
                "meta"
            } else {
                "status_spill"
            };
            return Resolution::Reused {
                page_type: page_type.to_string(),
            };
        }
        let page = match self.source.read(page_id) {
            Ok(page) => page,
            Err(fault) => return Resolution::Unreadable(fault),
        };
        let kind = page_type(&page);
        let lookup = lookup_slot(&page, locator);
        match kind {
            PageType::Heap | PageType::Free => match lookup {
                SlotLookup::Live(bytes) => Resolution::Live {
                    bytes: bytes.to_vec(),
                    on_free_page: kind == PageType::Free,
                    foreign: None,
                },
                SlotLookup::OutOfPage => Resolution::OutOfPage,
                _ if kind == PageType::Free => Resolution::FreeWithoutSlot,
                SlotLookup::NoSuchSlot => Resolution::StaleHeapSlot(format!(
                    "slot {} beyond the page's {} slot(s)",
                    locator.slot,
                    slot_count(&page)
                )),
                SlotLookup::Dead => {
                    Resolution::StaleHeapSlot(format!("slot {} is dead", locator.slot))
                }
                SlotLookup::OtherGeneration(found) => Resolution::StaleHeapSlot(format!(
                    "slot {} holds generation {found}, the locator names {}",
                    locator.slot, locator.generation
                )),
            },
            other => match lookup {
                SlotLookup::Live(bytes) => Resolution::Live {
                    bytes: bytes.to_vec(),
                    on_free_page: false,
                    foreign: Some(page_type_name(other).to_string()),
                },
                _ => Resolution::Reused {
                    page_type: page_type_name(other).to_string(),
                },
            },
        }
    }

    fn mark_reachable(&mut self, locator: TupleLocator) {
        let (low, high) = self.window;
        if locator.page_id >= low
            && locator.page_id < high
            && u64::from(locator.slot) < self.slot_bits_per_page
        {
            self.slots
                .set((locator.page_id - low) * self.slot_bits_per_page + u64::from(locator.slot));
        }
    }

    fn index_entry(&mut self, key: &[u8], value: &[u8], first_pass: bool) {
        if first_pass {
            self.counters.index_entries += 1;
        }
        let Some(head) = TupleLocator::decode(value) else {
            if first_pass {
                let mut finding = StoreFinding::new(FindingClass::InvalidHeadEncoding);
                finding.key = Some(key.to_vec());
                finding.detail = format!("a {}-byte value", value.len());
                self.report(finding);
            }
            return;
        };
        let mut locator = head;
        let mut depth = 0_u64;
        // Until a version whose creator committed is reached, every version
        // is one a reader at the latest snapshot must pass through.
        let mut needed = true;
        let mut recent: Vec<TupleLocator> = Vec::new();
        let mut visited: Option<FxHashSet<TupleLocator>> = None;
        loop {
            if !self.within_budget(depth.saturating_add(1).saturating_mul(64)) {
                return;
            }
            if depth >= self.options.max_versions_per_chain as u64 {
                if first_pass {
                    let mut finding = StoreFinding::new(FindingClass::ChainTooLong);
                    finding.key = Some(key.to_vec());
                    finding.locator = Some(locator);
                    finding.detail =
                        format!("more than {} versions", self.options.max_versions_per_chain);
                    self.report(finding);
                }
                return;
            }
            let repeated = match visited.as_mut() {
                Some(set) => !set.insert(locator),
                None => {
                    if recent.contains(&locator) {
                        true
                    } else {
                        recent.push(locator);
                        if recent.len() > 64 {
                            visited = Some(recent.drain(..).collect());
                        }
                        false
                    }
                }
            };
            if repeated {
                if first_pass {
                    let mut finding = StoreFinding::new(FindingClass::ChainCycle);
                    finding.key = Some(key.to_vec());
                    finding.locator = Some(locator);
                    finding.chain_depth = Some(depth);
                    finding.detail = "the chain revisits this version".to_string();
                    self.report(finding);
                }
                return;
            }
            match self.resolve(locator) {
                Resolution::Live {
                    bytes,
                    on_free_page,
                    foreign,
                } => {
                    self.mark_reachable(locator);
                    if first_pass {
                        self.counters.versions_examined += 1;
                    }
                    let header = self.version_header(&bytes, locator, first_pass && needed, key);
                    let Some(header) = header else {
                        if first_pass && self.complete {
                            let mut finding = StoreFinding::new(FindingClass::MalformedVersion);
                            finding.key = Some(key.to_vec());
                            finding.locator = Some(locator);
                            finding.chain_depth = Some(depth);
                            finding.detail = "the MVCC header cannot be decoded".to_string();
                            self.report(finding);
                        }
                        return;
                    };
                    let creator = self.status_of(header.xmin);
                    if first_pass && on_free_page {
                        let deleter = if header.xmax == 0 {
                            "none".to_string()
                        } else {
                            format!("{:?}", self.status_of(header.xmax))
                        };
                        let mut finding = StoreFinding::new(FindingClass::RowOnFreePage);
                        finding.key = Some(key.to_vec());
                        finding.page_id = Some(locator.page_id);
                        finding.locator = Some(locator);
                        finding.chain_depth = Some(depth);
                        finding.found_page_type = Some("free".to_string());
                        finding.xmin = Some(header.xmin);
                        finding.xmax = Some(header.xmax);
                        finding.detail = format!(
                            "{} bytes; creator {creator:?}, deleter {deleter}; {}",
                            bytes.len(),
                            if needed && creator == Status::Committed {
                                "this is the version readers see: rescue it before the page is \
                                 allocated again"
                            } else {
                                "an older or uncommitted version"
                            }
                        );
                        self.report(finding);
                    }
                    if let (true, Some(page_type)) = (first_pass, foreign) {
                        let mut finding = StoreFinding::new(FindingClass::RowOnNonHeapPage);
                        finding.key = Some(key.to_vec());
                        finding.page_id = Some(locator.page_id);
                        finding.locator = Some(locator);
                        finding.chain_depth = Some(depth);
                        finding.found_page_type = Some(page_type.clone());
                        finding.xmin = Some(header.xmin);
                        finding.xmax = Some(header.xmax);
                        finding.detail = format!(
                            "{} bytes in slot {} of a {page_type} page; creator {creator:?}; any \
                             other structure using page {} is damaged too",
                            bytes.len(),
                            locator.slot,
                            locator.page_id
                        );
                        self.report(finding);
                    }
                    if needed && creator == Status::Committed {
                        needed = false;
                        if first_pass {
                            let deleted = header.xmax != 0
                                && self.status_of(header.xmax) == Status::Committed;
                            if deleted {
                                self.counters.deleted_rows += 1;
                            } else {
                                self.counters.visible_rows += 1;
                            }
                        }
                    }
                    match header.prev {
                        Some(previous) => {
                            locator = previous;
                            depth += 1;
                        }
                        None => {
                            if first_pass && needed {
                                self.counters.keys_without_committed_version += 1;
                            }
                            return;
                        }
                    }
                }
                other => {
                    if first_pass {
                        if needed {
                            self.missing_needed_version(key, locator, depth, other);
                        } else {
                            self.counters.vacuumed_links += 1;
                        }
                    }
                    return;
                }
            }
        }
    }

    fn missing_needed_version(
        &mut self,
        key: &[u8],
        locator: TupleLocator,
        depth: u64,
        resolution: Resolution,
    ) {
        let position = if depth == 0 {
            "the head".to_string()
        } else {
            format!("the first committed version, below {depth} uncommitted version(s),")
        };
        let (class, page_type, detail) = match resolution {
            Resolution::Live { .. } => return,
            Resolution::BeyondPageCount => (
                FindingClass::MissingRowBeyondPageCount,
                None,
                format!(
                    "{position} names page {}; the page count is {}",
                    locator.page_id, self.source.page_count
                ),
            ),
            Resolution::Unreadable(fault) => {
                self.unreadable_reported.insert(locator.page_id);
                (
                    FindingClass::ReferencedPageUnreadable,
                    None,
                    format!("{position} is on page {}: {fault}", locator.page_id),
                )
            }
            Resolution::Reused { page_type } => (
                FindingClass::MissingRowPageReused,
                Some(page_type.clone()),
                format!("{position} names a page that is now a {page_type} page"),
            ),
            Resolution::FreeWithoutSlot => (
                FindingClass::MissingRowOnFreePage,
                Some("free".to_string()),
                format!("{position} names slot {} of a Free page", locator.slot),
            ),
            Resolution::StaleHeapSlot(reason) => (
                FindingClass::MissingRowStaleHeapSlot,
                Some("heap".to_string()),
                format!("{position}: {reason}"),
            ),
            Resolution::OutOfPage => (
                FindingClass::VersionOutOfBounds,
                None,
                format!("{position}: the slot's range leaves the page"),
            ),
        };
        let mut finding = StoreFinding::new(class);
        finding.key = Some(key.to_vec());
        finding.page_id = Some(locator.page_id);
        finding.locator = Some(locator);
        finding.chain_depth = Some(depth);
        finding.found_page_type = page_type;
        finding.detail = detail;
        self.report(finding);
    }

    /// The MVCC header of a stored tuple. For an overflow value, `check`
    /// also verifies the whole overflow chain.
    fn version_header(
        &mut self,
        tuple: &[u8],
        locator: TupleLocator,
        check: bool,
        key: &[u8],
    ) -> Option<VersionHeader> {
        match decode_tuple(tuple).ok()? {
            Tuple::Inline(bytes) => VersionHeader::decode(bytes).map(|(header, _)| header),
            Tuple::Overflow { head, len } => {
                let (prefix, broken) = self.overflow_prefix(head, len, check);
                if let Some(reason) = broken {
                    if check {
                        let mut finding = StoreFinding::new(FindingClass::OverflowChainBroken);
                        finding.key = Some(key.to_vec());
                        finding.locator = Some(locator);
                        finding.detail = reason;
                        self.report(finding);
                    }
                }
                VersionHeader::decode(&prefix).map(|(header, _)| header)
            }
        }
    }

    /// Up to the first `VERSION_HEADER_BYTES` of an overflow value, and why
    /// the chain is broken if it is. Walks the whole chain when `full`.
    fn overflow_prefix(&mut self, head: PageId, len: u64, full: bool) -> (Vec<u8>, Option<String>) {
        let capacity = crate::overflow::payload_capacity(self.source.page_size) as u64;
        let expected_pages = len.div_ceil(capacity).max(1);
        let mut prefix = Vec::with_capacity(VERSION_HEADER_BYTES);
        let mut page_id = head;
        let mut total = 0_u64;
        if expected_pages > self.source.page_count {
            return (
                prefix,
                Some(
                    "overflow length requires more distinct pages than the store contains"
                        .to_string(),
                ),
            );
        }
        let mut visited = FxHashSet::default();
        for index in 0..expected_pages {
            if !self.within_budget((visited.capacity() as u64 + 1).saturating_mul(64)) {
                return (prefix, None);
            }
            if !visited.insert(page_id) {
                return (
                    prefix,
                    Some(format!("overflow chain revisits page {page_id}")),
                );
            }
            if page_id == 0 || page_id >= self.source.page_count {
                return (
                    prefix,
                    Some(format!(
                        "overflow page {index} ({page_id}) is beyond the page count"
                    )),
                );
            }
            let page = match self.source.read(page_id) {
                Ok(page) => page,
                Err(fault) => {
                    return (
                        prefix,
                        Some(format!("overflow page {index} ({page_id}): {fault}")),
                    )
                }
            };
            if page_type(&page) != PageType::Overflow {
                return (
                    prefix,
                    Some(format!(
                        "overflow page {index} ({page_id}) is a {} page",
                        page_type_name(page_type(&page))
                    )),
                );
            }
            let payload_len = u64::from(u32::from_le_bytes(page[52..56].try_into().unwrap()));
            if payload_len > capacity {
                return (
                    prefix,
                    Some(format!(
                        "overflow page {index} ({page_id}) claims {payload_len} bytes"
                    )),
                );
            }
            let wanted = (VERSION_HEADER_BYTES - prefix.len()).min(payload_len as usize);
            prefix.extend_from_slice(&page[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + wanted]);
            total += payload_len;
            let next = u64::from_le_bytes(page[44..52].try_into().unwrap());
            if !full && prefix.len() >= VERSION_HEADER_BYTES {
                return (prefix, None);
            }
            if index + 1 == expected_pages {
                if next != 0 || total != len {
                    return (
                        prefix,
                        Some(format!(
                            "the chain holds {total} of {len} bytes over {expected_pages} pages and \
                             ends with link {next}"
                        )),
                    );
                }
                return (prefix, None);
            }
            page_id = next;
        }
        (prefix, None)
    }

    // --- Heap sweep -------------------------------------------------------

    fn sweep_window(&mut self, low: PageId, high: PageId) {
        for page_id in low.max(1)..high {
            if self.out_of_time() {
                return;
            }
            if self.special_pages.contains(&page_id) {
                continue;
            }
            // Bypass the cache: a sequential sweep would only evict the
            // tree and chain pages the cache exists for.
            let page = match self.source.read_uncached(page_id) {
                Ok(page) => page,
                Err(fault) => {
                    if !self.tree_pages.get(page_id) && !self.unreadable_reported.contains(&page_id)
                    {
                        let mut finding =
                            StoreFinding::new(FindingClass::UnreadableUnreferencedPage);
                        finding.page_id = Some(page_id);
                        finding.detail = fault.to_string();
                        self.report(finding);
                    }
                    continue;
                }
            };
            match page_type(&page) {
                PageType::Heap => {
                    // Only pages the heap registered are slotted row pages
                    // for certain. Every page a committed heap insert
                    // allocates is registered in the same transaction.
                    if !self.in_directory.get(page_id) && self.facts.catalog_root != 0 {
                        let mut finding = StoreFinding::new(FindingClass::HeapPageNotInDirectory);
                        finding.page_id = Some(page_id);
                        finding.detail = format!(
                            "{} slot(s); not swept for unreachable versions",
                            slot_count(&page)
                        );
                        self.report(finding);
                        continue;
                    }
                    self.counters.heap_pages_swept += 1;
                    self.sweep_slots(&page, page_id, false);
                }
                PageType::Free => self.sweep_slots(&page, page_id, true),
                PageType::BTreeInterior | PageType::BTreeLeaf => {
                    if !self.tree_pages.get(page_id) {
                        let mut finding = StoreFinding::new(FindingClass::OrphanTreePage);
                        finding.page_id = Some(page_id);
                        finding.found_page_type =
                            Some(page_type_name(page_type(&page)).to_string());
                        self.report(finding);
                    }
                }
                PageType::Overflow | PageType::Superblock => {}
            }
        }
    }

    fn sweep_slots(&mut self, page: &[u8], page_id: PageId, free_page: bool) {
        let low = self.window.0;
        for slot in 0..slot_count(page) {
            let Some((offset, len, generation)) = slot_entry(page, slot) else {
                break;
            };
            if offset == 0 {
                continue;
            }
            let locator = TupleLocator::new(page_id, slot, generation);
            let reachable = u64::from(slot) < self.slot_bits_per_page
                && self
                    .slots
                    .get((page_id - low) * self.slot_bits_per_page + u64::from(slot));
            if free_page {
                if !reachable {
                    let mut finding = StoreFinding::new(FindingClass::FreePageUnreferencedTuple);
                    finding.page_id = Some(page_id);
                    finding.locator = Some(locator);
                    self.report(finding);
                }
                continue;
            }
            self.counters.heap_versions_swept += 1;
            if reachable {
                self.counters.reachable_versions_swept += 1;
                continue;
            }
            let Some(tuple) = tuple_bytes(page, offset, len) else {
                let mut finding = StoreFinding::new(FindingClass::VersionOutOfBounds);
                finding.page_id = Some(page_id);
                finding.locator = Some(locator);
                finding.detail = "an unreachable slot's range leaves the page".to_string();
                self.report(finding);
                continue;
            };
            let tuple = tuple.to_vec();
            let Some(header) = self.version_header(&tuple, locator, false, &[]) else {
                let mut finding = StoreFinding::new(FindingClass::MalformedVersion);
                finding.page_id = Some(page_id);
                finding.locator = Some(locator);
                finding.detail = "an unreachable slot's MVCC header cannot be decoded".to_string();
                self.report(finding);
                continue;
            };
            let creator = self.status_of(header.xmin);
            let deleter = if header.xmax == 0 {
                Status::Aborted
            } else {
                self.status_of(header.xmax)
            };
            let class = match (creator, deleter) {
                (Status::Unresolved, _) | (Status::Committed, Status::Unresolved) => {
                    FindingClass::VersionStatusUnresolved
                }
                (Status::Aborted, _) => FindingClass::UnreachableUncommittedVersion,
                (Status::Committed, Status::Committed) => FindingClass::UnreachableDeadVersion,
                (Status::Committed, Status::Aborted) => {
                    FindingClass::UnreachableCommittedLiveVersion
                }
            };
            let mut finding = StoreFinding::new(class);
            finding.page_id = Some(page_id);
            finding.locator = Some(locator);
            finding.xmin = Some(header.xmin);
            finding.xmax = Some(header.xmax);
            if class == FindingClass::UnreachableCommittedLiveVersion {
                finding.detail = format!(
                    "{} stored bytes; no key-tree entry reaches this version (the page store \
                     does not record a version's key; in a database the record id inside the \
                     value names it)",
                    tuple.len()
                );
            }
            self.report(finding);
        }
    }

    // --- Report -----------------------------------------------------------

    fn finish(mut self) -> StoreVerifyReport {
        self.counters.pages_read_from_page_files = self.source.pages_from_files;
        self.counters.pages_read_from_log = self.source.pages_from_log;
        self.counters.page_cache_hits = self.source.cache_hits;
        self.counters.bytes_read = self.source.bytes_read;
        self.counters.elapsed_millis =
            u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.summaries
            .sort_by(|a, b| b.severity.cmp(&a.severity).then(a.class.cmp(&b.class)));
        let worst = self
            .summaries
            .iter()
            .map(|summary| summary.severity)
            .max()
            .unwrap_or(FindingSeverity::Info);
        let verdict = match worst {
            FindingSeverity::Info => StoreVerdict::Clean,
            FindingSeverity::AtRisk => StoreVerdict::AtRisk,
            FindingSeverity::NeedsExternalCrossCheck => StoreVerdict::NeedsExternalCrossCheck,
            FindingSeverity::Inconsistent => StoreVerdict::Inconsistent,
        };
        let log = &self.source.log;
        let would_discard = match &log.shape {
            LogResetShape::InterruptedReset { stale_segments, .. }
            | LogResetShape::StaleSealedBeforeActive { stale_segments, .. } => *stale_segments,
            _ => 0,
        };
        let log_facts = LogFacts {
            sealed_segments: log.sealed_segments,
            sealed_bytes: log.sealed_bytes,
            active_bytes: log.active_bytes,
            reset_shape: log.shape.clone(),
            replay_scope: log.replay_scope.to_string(),
            records: log.records,
            first_lsn: log.first_lsn,
            last_lsn: log.last_lsn,
            torn_tail_bytes: log.torn_tail_bytes,
            checkpoint_records: log.checkpoint_records,
            redo_lsn: log.redo_lsn,
            committed_transactions: log.committed_transactions,
            pending_page_images: log.images_replayed,
            distinct_pages_overlaid: log.images.len() as u64,
            uncommitted_images_skipped: log.uncommitted_images_skipped,
            page_file_truncated_by_recovery_from: log.truncated_from,
            open_would_refuse: log.corruption.clone(),
            stale_records_discarded: StaleRecordsDiscardedState {
                recorded_in_store: false,
                segments_a_fixed_open_would_discard: would_discard,
                explanation: "The crash-ordered reset fix reports stale segments it discards \
                              (`stale_segments_discarded`) only in the RecoveryReport of that \
                              open; it persists nothing, so a store a fixed engine already \
                              cleaned up carries no trace of it. `reset_shape` is the same \
                              classification computed from these files now."
                    .to_string(),
            },
        };
        let mut external_evidence = Vec::new();
        let needs =
            |class: FindingClass| self.summaries.iter().any(|summary| summary.class == class);
        if [
            FindingClass::MissingRowPageReused,
            FindingClass::MissingRowOnFreePage,
            FindingClass::MissingRowStaleHeapSlot,
            FindingClass::MissingRowBeyondPageCount,
        ]
        .into_iter()
        .any(needs)
        {
            external_evidence.push(
                "missing_row_*: the store keeps no record of whether the key was deleted. If the \
                 last committed operation on the key was a write, the row is lost; if it was a \
                 delete, the entry is a dangling index entry vacuum left behind (autovacuum does \
                 not sweep index entries). Settle each key against: a standby (it applies \
                 logical frames, so it holds the row if it was not deleted); the logical \
                 transaction log, or the commit-log archive, for the key's last operation; \
                 archived page-log segments, rolled forward into a scratch copy up to before the \
                 page's re-allocation, which expose the row's bytes; or a backup taken after the \
                 last write. In a database, a live secondary-index entry or foreign key that \
                 still names the record also proves the row was not deleted."
                    .to_string(),
            );
        }
        if needs(FindingClass::RowOnFreePage) {
            external_evidence.push(
                "row_on_free_page: the bytes are still on the page. Stop automatic vacuum and \
                 hole punching, take a backup, then write each listed key again through the \
                 normal API in one transaction (on an engine with the placement fix) so it gets \
                 a new version on a heap page."
                    .to_string(),
            );
        }
        if needs(FindingClass::LogStaleChainAlreadyReplayed)
            || needs(FindingClass::LogInterruptedReset)
        {
            external_evidence.push(
                "page log: a stale chain that was already replayed leaves rows looking like older \
                 committed versions, which no check of one store can tell from real history. \
                 Compare the newest committed value of recently written keys against the \
                 commit-log archive, a standby that applied the same commits, or the \
                 application's own ledger. While the shape is only `interrupted_reset`, nothing \
                 has regressed yet: open the store only with an engine that has the \
                 crash-ordered reset fix."
                    .to_string(),
            );
        }
        if needs(FindingClass::UnreachableCommittedLiveVersion) {
            external_evidence.push(
                "unreachable_committed_live_version: the row's bytes are intact on its heap page \
                 but no key reaches them. In a database the record id inside each value names \
                 the key; compare with a standby or backup to decide which version to restore."
                    .to_string(),
            );
        }
        StoreVerifyReport {
            format_version: STORE_VERIFY_REPORT_FORMAT_VERSION,
            verdict,
            complete: self.complete,
            stop_reason: self.stop_reason,
            store: self.facts,
            read_only: ReadOnlyEvidence {
                lock: self.lock,
                files_opened_read_only: self.files_opened,
                recovery_ran: false,
                note: "Files were opened read-only (O_NOFOLLOW, O_NOATIME where permitted); the \
                       directory lock was probed with F_GETLK and not taken; recovery was \
                       reproduced in memory by reading committed page images from the log."
                    .to_string(),
            },
            log: log_facts,
            counters: self.counters,
            classes: self.summaries,
            external_evidence,
        }
    }
}

/// The key range a tree frame covers, hex encoded.
fn key_range(frame: &TreeFrame) -> String {
    let bound = |key: &Option<Rc<[u8]>>| key.as_deref().map(hex::encode);
    format!(
        "keys [{}, {})",
        bound(&frame.low).unwrap_or_else(|| "-inf".to_string()),
        bound(&frame.high).unwrap_or_else(|| "+inf".to_string())
    )
}

fn tree_name_of(kind: TreeKind) -> &'static str {
    match kind {
        TreeKind::Index => "key tree",
        TreeKind::Catalog => "catalog tree",
    }
}
