//! Write-ahead log, checkpoints, and crash recovery.
//!
//! Phase 3 of `docs/server-paged-storage-todo.md`:
//!
//! - "Extend the WAL with enough page/tuple intent to repeat committed changes
//!   idempotently and reject torn or out-of-order records."
//! - "Enforce write-ahead ordering: the relevant WAL is durable before a dirty
//!   data/index page may reach durable storage."
//! - "Implement group commit without weakening `fsync` semantics."
//! - "Implement fuzzy checkpoints that record a durable redo point while writes
//!   continue."
//!
//! # Physical logging, and what it costs
//!
//! Records carry **full page after-images**. Replay is then trivially
//! idempotent — writing the same page image twice is indistinguishable from
//! writing it once — which is what makes the crash matrix tractable: recovery
//! can replay from any point without tracking which changes it has already
//! applied.
//!
//! The cost is write amplification: modifying one 40-byte tuple logs a whole
//! page. Logical or physiological logging would log less but requires each
//! record to be replayable against a page in *unknown* state, which is where
//! recovery bugs live. Physical logging first, measured, is the right order —
//! and the amplification is visible in [`WalSnapshot::bytes_appended`] rather
//! than being a surprise later.
//!
//! # Write-ahead ordering
//!
//! [`Checkpointer`] owns the ordering. `log_and_write` appends the page image
//! before the page is made dirty in the pool, and `checkpoint` syncs the log
//! before flushing pages, so no dirty page can reach disk ahead of its log
//! record. The invariant lives in one type rather than in a rule everyone has
//! to remember at every call site.
//!
//! # Group commit
//!
//! Commits accumulate and one `fsync` covers all of them. `fsync` semantics are
//! not weakened: a commit does not return until the log is durable *past its
//! own LSN*. Waiters simply share the sync that satisfies them, which is the
//! whole point — throughput comes from amortizing the sync, never from skipping
//! it.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex};

use crate::error::{PageError, Result};
use crate::mvcc::{RecoveredOutcomes, TxStatus};
use crate::page::{PageId, MAX_PAGE_SIZE};

/// Log sequence number. Monotonic, gap-free, never reused.
pub type Lsn = u64;

const RECORD_MAGIC: [u8; 4] = *b"BWAL";
/// magic + lsn + kind + reserved + payload_len + crc
const RECORD_HEADER_BYTES: usize = 4 + 8 + 1 + 3 + 4 + 4;
const RECORD_FIXED_BODY_BYTES: usize = 8 + 8;
/// Hard allocation ceiling for one decoded WAL record. Page images are the
/// largest supported payload; transaction and checkpoint records have none.
pub const MAX_WAL_RECORD_BYTES: usize =
    RECORD_HEADER_BYTES + RECORD_FIXED_BODY_BYTES + MAX_PAGE_SIZE as usize;

/// What a log record describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    /// Full after-image of one page.
    PageImage = 1,
    /// A transaction became durable. Everything logged before it is committed.
    Commit = 2,
    /// A checkpoint: everything before `redo_lsn` is already in the page file.
    Checkpoint = 3,
    /// A transaction was abandoned; its page images before this must not be
    /// treated as committed.
    Abort = 4,
}

impl RecordKind {
    fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::PageImage,
            2 => Self::Commit,
            3 => Self::Checkpoint,
            4 => Self::Abort,
            _ => return None,
        })
    }
}

/// One decoded log record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub kind: RecordKind,
    /// For `PageImage`, the page this is an image of.
    pub page_id: PageId,
    /// For `Commit`/`Abort`, the transaction; for `Checkpoint`, the redo LSN.
    pub transaction: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalSnapshot {
    pub records_appended: u64,
    pub bytes_appended: u64,
    pub syncs: u64,
    pub sync_failures: u64,
    /// WAL records newly covered by successful physical sync operations.
    pub sync_records_total: u64,
    pub last_sync_records: u64,
    pub max_sync_records: u64,
    pub sync_latency: crate::manager::PageIoLatencySnapshot,
    /// Commits that were satisfied by another thread's sync.
    pub group_commit_savings: u64,
    pub checkpoints: u64,
    pub last_checkpoint_completed_at_millis: u64,
    /// Computed when the snapshot is taken; zero means no checkpoint has
    /// completed in this process.
    pub checkpoint_age_millis: u64,
    pub durable_lsn: Lsn,
    pub next_lsn: Lsn,
    pub redo_lsn: Lsn,
    /// Active-file seals into immutable `store.wal.<seq>` segments.
    #[serde(default)]
    pub segments_sealed: u64,
}

/// Outcome of replaying a log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryReport {
    pub records_scanned: u64,
    pub pages_replayed: u64,
    /// Pages advertised by the pre-recovery allocator list and detached before
    /// the first replayed page image. Replayed allocations may make some live;
    /// the remainder stay safely orphaned until bounded reclamation.
    #[serde(default)]
    pub free_pages_detached: u64,
    /// Page images that belonged to no committed transaction and were skipped.
    pub uncommitted_skipped: u64,
    pub committed_transactions: u64,
    /// Records after the last valid one, discarded as a torn tail.
    pub truncated_bytes: u64,
    /// Logical WAL bytes present when recovery began.
    pub wal_bytes_scanned: u64,
    /// Number of bounded forward scans. Recovery deliberately makes two.
    pub scan_passes: u64,
    /// Largest validated header + body retained by the streaming scanner.
    pub peak_record_bytes: u64,
    /// Final commit/abort decisions restored into the recent MVCC table.
    pub transaction_outcomes: u64,
    /// Commit and abort records observed before duplicate decisions collapse.
    #[serde(default)]
    pub terminal_outcome_records: u64,
    /// Peak heap capacity used by compact transaction outcomes during pass one.
    #[serde(default)]
    pub peak_transaction_outcome_bytes: u64,
    /// Terminal outcomes absorbed into the durable watermark by the open-time
    /// freeze. Bounded by the exception capacity rather than left resident.
    #[serde(default)]
    pub frozen_outcomes_at_open: u64,
    /// Durable abort exceptions resident after the open-time freeze.
    #[serde(default)]
    pub abort_exceptions_at_open: u64,
    /// Terminal outcomes retained by the disk-backed status spill at open.
    #[serde(default)]
    pub status_spill_entries_at_open: u64,
    /// Status-spill pages chained at open.
    #[serde(default)]
    pub status_spill_pages_at_open: u64,
    /// The meta page used the pre-durable-abort-exceptions layout and was
    /// rewritten in the current layout at open (`accept_legacy_meta`).
    #[serde(default)]
    pub legacy_meta_migrated: bool,
    /// Highest transaction id named by any record in the log — including page
    /// images whose transaction never reached a terminal record. A crashed
    /// writer that evicted pages (steal) but never committed appears only
    /// here; `next_xid` must advance past it, or its id is reissued and the
    /// reissued transaction's commit retroactively blesses the crashed one's
    /// on-disk versions.
    #[serde(default)]
    pub max_record_transaction: u64,
    pub duration_nanos: u64,
    pub redo_lsn: Lsn,
    pub end_lsn: Lsn,
}

struct WalRecordView<'a> {
    lsn: Lsn,
    kind: RecordKind,
    page_id: PageId,
    transaction: u64,
    payload: &'a mut [u8],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WalScanReport {
    records: u64,
    valid_bytes: u64,
    truncated_bytes: u64,
    peak_record_bytes: u64,
    last_lsn: Option<Lsn>,
}

struct SyncState {
    /// Highest LSN known durable on disk.
    durable_lsn: Lsn,
    /// A sync is in flight.
    syncing: bool,
}

#[derive(Debug, Default)]
struct CheckpointFreshness {
    completed_at_millis: u64,
    completed_at: Option<Instant>,
}

/// The write-ahead log.
#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: Mutex<File>,
    /// Fixed segment size; `0` = one unbounded active file (legacy). When an
    /// append would push the ACTIVE file past this at a record boundary, the
    /// file is made durable, renamed `store.wal.<seq>` (sealed, immutable —
    /// the unit of archiving and backup), and a fresh active file starts.
    segment_bytes: u64,
    /// Ascending first-LSN names of sealed segments currently on disk.
    /// Mutated only under the file mutex. First-LSN naming (not a restart-
    /// prone counter) plus the reopen floor makes every sealed name unique
    /// for the store's lifetime — the invariant a WAL archive needs.
    sealed: Mutex<Vec<u64>>,
    /// First LSN the CURRENT active file will contain (next_lsn at the
    /// moment it was created or reset). Becomes the file's sealed name.
    active_first_lsn: AtomicU64,
    /// Sealed segments with names <= this are safe for checkpoint reset to
    /// DELETE while archive retention is on (the archiver copied them).
    archived_through: AtomicU64,
    /// When set, reset() retains sealed segments beyond `archived_through`
    /// instead of deleting them, so a WAL archive never gaps. Growth is
    /// bounded only by the archiver keeping up — same failure mode as
    /// PostgreSQL's archive_command, and deliberately so.
    archive_retention: AtomicBool,
    /// Total bytes across sealed segments (the active file's bytes are
    /// `end_offset`).
    sealed_bytes: AtomicU64,
    next_lsn: AtomicU64,
    /// Bytes written to the ACTIVE file, i.e. its append offset.
    end_offset: AtomicU64,
    sync_state: Mutex<SyncState>,
    sync_signal: Condvar,
    fsync: bool,
    redo_lsn: AtomicU64,
    metrics: Mutex<WalSnapshot>,
    checkpoint_freshness: Mutex<CheckpointFreshness>,
    /// Transactions with a commit record in the current log generation.
    /// Consulted by [`Self::abort`] so a commit-then-abort contradiction can
    /// never be written: recovery treats contradictory terminal records as
    /// corruption and refuses to open, so the natural cleanup path after a
    /// failed commit fsync (the record may be on disk even though the sync
    /// failed) must not append an abort. Cleared on [`Self::reset`], which
    /// discards the commit records it tracks. Contiguous transaction IDs are
    /// represented as ranges, with a hard range-count ceiling so a pinned
    /// checkpoint cannot turn this safety index into unbounded memory.
    committed_this_generation: Mutex<CommittedRanges>,
}

const MAX_COMMITTED_RANGES: usize = 4_096;

#[derive(Debug, Default)]
struct CommittedRanges {
    ranges: Vec<(u64, u64)>,
}

impl CommittedRanges {
    fn contains(&self, xid: u64) -> bool {
        self.ranges
            .binary_search_by(|(start, end)| {
                if xid < *start {
                    std::cmp::Ordering::Greater
                } else if xid > *end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    fn insert(&mut self, xid: u64) -> bool {
        if self.contains(xid) {
            return true;
        }
        let position = self.ranges.partition_point(|(_, end)| *end < xid);
        let merge_previous =
            position > 0 && self.ranges[position - 1].1.checked_add(1) == Some(xid);
        let merge_next =
            position < self.ranges.len() && xid.checked_add(1) == Some(self.ranges[position].0);
        match (merge_previous, merge_next) {
            (true, true) => {
                let next_end = self.ranges[position].1;
                self.ranges[position - 1].1 = next_end;
                self.ranges.remove(position);
            }
            (true, false) => self.ranges[position - 1].1 = xid,
            (false, true) => self.ranges[position].0 = xid,
            (false, false) if self.ranges.len() < MAX_COMMITTED_RANGES => {
                self.ranges.insert(position, (xid, xid));
            }
            (false, false) => return false,
        }
        true
    }

    fn clear(&mut self) {
        self.ranges.clear();
    }
}

#[allow(dead_code)]
impl std::fmt::Debug for SyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncState")
            .field("durable_lsn", &self.durable_lsn)
            .field("syncing", &self.syncing)
            .finish()
    }
}

impl Wal {
    /// Open or create a log with one unbounded active file.
    pub fn open(path: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        Self::open_with_segments(path, fsync, 0)
    }

    /// Open or create a log; `segment_bytes > 0` seals the active file into
    /// immutable `store.wal.<seq>` segments as it fills. Sealed segments left
    /// by an earlier run are discovered and replayed regardless of the
    /// current setting, so the option can change between opens.
    pub fn open_with_segments(
        path: impl AsRef<Path>,
        fsync: bool,
        segment_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_segments_and_floor(path, fsync, segment_bytes, 0)
    }

    /// `lsn_floor` (the superblock's checkpoint LSN horizon) keeps LSNs
    /// monotonic across truncate+reopen cycles: without it an emptied log
    /// restarts at 1 and sealed-segment names could recur.
    pub fn open_with_segments_and_floor(
        path: impl AsRef<Path>,
        fsync: bool,
        segment_bytes: u64,
        lsn_floor: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| PageError::io(parent, e))?;
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| PageError::io(&path, e))?;
        let end_offset = file.metadata().map_err(|e| PageError::io(&path, e))?.len();
        let (sealed, sealed_bytes) = discover_sealed_segments(&path)?;
        let initial_lsn = lsn_floor.max(1);

        Ok(Self {
            path,
            file: Mutex::new(file),
            segment_bytes,
            sealed: Mutex::new(sealed),
            active_first_lsn: AtomicU64::new(initial_lsn),
            archived_through: AtomicU64::new(0),
            archive_retention: AtomicBool::new(false),
            sealed_bytes: AtomicU64::new(sealed_bytes),
            next_lsn: AtomicU64::new(initial_lsn),
            end_offset: AtomicU64::new(end_offset),
            sync_state: Mutex::new(SyncState {
                durable_lsn: 0,
                syncing: false,
            }),
            sync_signal: Condvar::new(),
            fsync,
            redo_lsn: AtomicU64::new(0),
            metrics: Mutex::new(WalSnapshot::default()),
            checkpoint_freshness: Mutex::new(CheckpointFreshness::default()),
            committed_this_generation: Mutex::new(CommittedRanges::default()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn sealed_path(&self, sequence: u64) -> PathBuf {
        let mut os = self.path.as_os_str().to_os_string();
        os.push(format!(".{sequence}"));
        PathBuf::from(os)
    }

    /// Paths of the immutable sealed segments, ascending. What a WAL
    /// archiver or online backup streams before the active file.
    pub fn sealed_segment_paths(&self) -> Vec<PathBuf> {
        self.sealed
            .lock()
            .iter()
            .map(|sequence| self.sealed_path(*sequence))
            .collect()
    }

    /// Explicitly seal the active file (online backup's consistency cut).
    /// A no-op when the active file is empty. Refused when segmentation is
    /// disabled: sealed files next to a legacy-layout store would be
    /// invisible to a pre-segmentation engine.
    pub fn seal_active(&self) -> Result<()> {
        if self.segment_bytes == 0 {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "WAL segmentation is disabled for this store (legacy monolithic \
                         layout); re-segment the page store to enable online-consistent backup"
                    .to_string(),
            });
        }
        let mut file = self.file.lock();
        let active = self.end_offset.load(Ordering::Acquire);
        if active == 0 {
            return Ok(());
        }
        self.seal_active_locked(&mut file, active)
    }

    /// Seal the active file: make it durable, rename it to the next sealed
    /// sequence, and start a fresh active file. Caller holds the file mutex.
    fn seal_active_locked(&self, file: &mut File, active_bytes: u64) -> Result<()> {
        if self.fsync {
            file.sync_data().map_err(|e| PageError::io(&self.path, e))?;
        } else {
            file.flush().map_err(|e| PageError::io(&self.path, e))?;
        }
        let sequence = self.active_first_lsn.load(Ordering::Acquire);
        let sealed_path = self.sealed_path(sequence);
        std::fs::rename(&self.path, &sealed_path).map_err(|e| PageError::io(&sealed_path, e))?;
        let fresh = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| PageError::io(&self.path, e))?;
        if self.fsync {
            if let Some(parent) = self.path.parent() {
                if let Ok(dir) = File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
        }
        *file = fresh;
        self.active_first_lsn
            .store(self.next_lsn.load(Ordering::Acquire), Ordering::Release);
        self.sealed.lock().push(sequence);
        self.sealed_bytes.fetch_add(active_bytes, Ordering::AcqRel);
        self.end_offset.store(0, Ordering::Release);
        let mut metrics = self.metrics.lock();
        metrics.segments_sealed = metrics.segments_sealed.saturating_add(1);
        Ok(())
    }

    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn.load(Ordering::Acquire)
    }

    pub fn durable_lsn(&self) -> Lsn {
        self.sync_state.lock().durable_lsn
    }

    pub fn redo_lsn(&self) -> Lsn {
        self.redo_lsn.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> WalSnapshot {
        let mut snapshot = *self.metrics.lock();
        snapshot.durable_lsn = self.durable_lsn();
        snapshot.next_lsn = self.next_lsn();
        snapshot.redo_lsn = self.redo_lsn();
        let checkpoint = self.checkpoint_freshness.lock();
        snapshot.last_checkpoint_completed_at_millis = checkpoint.completed_at_millis;
        snapshot.checkpoint_age_millis = checkpoint
            .completed_at
            .map(|completed| u64::try_from(completed.elapsed().as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        snapshot
    }

    /// Append a record, returning its LSN. Does not sync.
    fn append(
        &self,
        kind: RecordKind,
        page_id: PageId,
        transaction: u64,
        payload: &[u8],
    ) -> Result<Lsn> {
        let mut body = Vec::with_capacity(16 + payload.len());
        body.extend_from_slice(&page_id.to_le_bytes());
        body.extend_from_slice(&transaction.to_le_bytes());
        body.extend_from_slice(payload);

        // The LSN is assigned *under the file lock*, together with the append.
        //
        // Assigning it first and locking afterwards is the obvious-looking
        // version and it is wrong: two threads can take LSNs 5 and 6 and then
        // write in the opposite order, leaving the log physically out of
        // sequence. Recovery reads the log in file order, so it would stop at
        // the discontinuity and silently discard everything after it — losing
        // committed work with no error anywhere. Holding one lock across both
        // makes file order and LSN order the same thing by construction.
        let lsn;
        let written;
        {
            let mut file = self.file.lock();
            // Seal at record boundaries only: a record never spans segment
            // files, so every sealed file is a self-contained record run.
            if self.segment_bytes > 0 {
                let active = self.end_offset.load(Ordering::Acquire);
                let record_bytes = (RECORD_HEADER_BYTES + body.len()) as u64;
                if active > 0 && active.saturating_add(record_bytes) > self.segment_bytes {
                    self.seal_active_locked(&mut file, active)?;
                }
            }
            lsn = self.next_lsn.fetch_add(1, Ordering::AcqRel);

            let mut buffer = Vec::with_capacity(RECORD_HEADER_BYTES + body.len());
            buffer.extend_from_slice(&RECORD_MAGIC);
            buffer.extend_from_slice(&lsn.to_le_bytes());
            buffer.push(kind as u8);
            buffer.extend_from_slice(&[0u8; 3]);
            buffer.extend_from_slice(&(body.len() as u32).to_le_bytes());

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buffer);
            hasher.update(&body);
            buffer.extend_from_slice(&hasher.finalize().to_le_bytes());
            buffer.extend_from_slice(&body);

            file.seek(SeekFrom::End(0))
                .map_err(|e| PageError::io(&self.path, e))?;
            file.write_all(&buffer)
                .map_err(|e| PageError::io(&self.path, e))?;
            written = buffer.len();
            // Keep the published append offset in the same critical section as
            // the bytes it describes. A concurrent segment seal/reset must
            // never observe the new file length with the old counter (or the
            // reverse) and make a retention or truncation decision from a
            // torn logical state.
            self.end_offset.fetch_add(written as u64, Ordering::AcqRel);
        }

        let mut metrics = self.metrics.lock();
        metrics.records_appended = metrics.records_appended.saturating_add(1);
        metrics.bytes_appended = metrics.bytes_appended.saturating_add(written as u64);
        Ok(lsn)
    }

    /// Log a full page after-image.
    pub fn log_page_image(&self, page_id: PageId, transaction: u64, image: &[u8]) -> Result<Lsn> {
        self.append(RecordKind::PageImage, page_id, transaction, image)
    }

    /// Log a commit and make the log durable through it.
    ///
    /// Returns after the commit record is on disk, never before.
    pub fn commit(&self, transaction: u64) -> Result<Lsn> {
        if !self.committed_this_generation.lock().insert(transaction) {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "WAL generation exceeds the bounded {MAX_COMMITTED_RANGES}-range commit index"
                ),
            });
        }
        let lsn = self.append(RecordKind::Commit, 0, transaction, &[])?;
        // Recorded before the sync: the record is in the log whether or not
        // the fsync below succeeds, and it is the record's existence that
        // must forbid a later abort for the same transaction.
        self.sync_through(lsn)?;
        Ok(lsn)
    }

    /// Log an abort. Not synced: an abandoned transaction that is lost to a
    /// crash is indistinguishable from one that never committed, and recovery
    /// discards uncommitted work anyway.
    ///
    /// Commit wins: if this log generation already holds a commit record for
    /// `transaction` — including one whose fsync failed, since the bytes may
    /// be on disk regardless — the abort is not appended. Recovery treats a
    /// commit-then-abort pair as corruption and refuses to open, so the pair
    /// must be unwritable in the first place.
    pub fn abort(&self, transaction: u64) -> Result<Lsn> {
        if self.committed_this_generation.lock().contains(transaction) {
            return Ok(self.next_lsn().saturating_sub(1));
        }
        self.append(RecordKind::Abort, 0, transaction, &[])
    }

    /// Make the log durable through `lsn`, sharing one `fsync` across waiters.
    ///
    /// The sharing is what makes this group commit. It does not weaken
    /// durability: a caller returns only once `durable_lsn >= lsn`, so its own
    /// record is on disk. It simply may be another thread's `fsync` that put it
    /// there.
    pub fn sync_through(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.sync_state.lock();
        loop {
            if state.durable_lsn >= lsn {
                return Ok(());
            }
            if state.syncing {
                // Someone else is syncing; their sync may cover us.
                let mut metrics = self.metrics.lock();
                metrics.group_commit_savings = metrics.group_commit_savings.saturating_add(1);
                drop(metrics);
                self.sync_signal.wait(&mut state);
                continue;
            }

            state.syncing = true;
            // The target is captured before releasing the lock: anything
            // appended after this point is not promised by this sync.
            let target = self.next_lsn.load(Ordering::Acquire).saturating_sub(1);
            drop(state);

            let mut file = self.file.lock();
            let started = Instant::now();
            let result = if self.fsync {
                file.sync_data().map_err(|e| PageError::io(&self.path, e))
            } else {
                file.flush().map_err(|e| PageError::io(&self.path, e))
            };
            let elapsed = started.elapsed();
            drop(file);

            state = self.sync_state.lock();
            state.syncing = false;
            match result {
                Ok(()) => {
                    let records = target.saturating_sub(state.durable_lsn);
                    state.durable_lsn = state.durable_lsn.max(target);
                    let mut metrics = self.metrics.lock();
                    metrics.syncs = metrics.syncs.saturating_add(1);
                    metrics.sync_records_total = metrics.sync_records_total.saturating_add(records);
                    metrics.last_sync_records = records;
                    metrics.max_sync_records = metrics.max_sync_records.max(records);
                    metrics.sync_latency.observe(elapsed);
                }
                Err(error) => {
                    let mut metrics = self.metrics.lock();
                    metrics.sync_failures = metrics.sync_failures.saturating_add(1);
                    self.sync_signal.notify_all();
                    return Err(error);
                }
            }
            self.sync_signal.notify_all();
        }
    }

    /// Make everything appended so far durable.
    pub fn sync(&self) -> Result<()> {
        let target = self.next_lsn.load(Ordering::Acquire).saturating_sub(1);
        self.sync_through(target)
    }

    /// Record a checkpoint: everything before `redo_lsn` is already in the page
    /// file, so recovery may start there.
    ///
    /// This is a **fuzzy** checkpoint — it names a redo point without pausing
    /// writers. The caller's contract is only that every page dirtied before
    /// `redo_lsn` has been flushed; pages dirtied after it are covered by the
    /// log records that follow.
    pub fn checkpoint(&self, redo_lsn: Lsn, root_page: PageId) -> Result<Lsn> {
        let lsn = self.append(RecordKind::Checkpoint, root_page, redo_lsn, &[])?;
        self.sync_through(lsn)?;
        self.redo_lsn.store(redo_lsn, Ordering::Release);
        {
            let mut metrics = self.metrics.lock();
            metrics.checkpoints = metrics.checkpoints.saturating_add(1);
        }
        {
            let mut checkpoint = self.checkpoint_freshness.lock();
            checkpoint.completed_at_millis = unix_time_millis();
            checkpoint.completed_at = Some(Instant::now());
        }
        Ok(lsn)
    }

    /// Read every valid record in the log, in order.
    ///
    /// Stops at the first record that fails to decode and reports how many
    /// bytes were discarded. A torn tail is *expected* after a crash — the log
    /// is appended to continuously, so a crash almost always lands mid-record —
    /// and truncating it is correct rather than a corruption event: those bytes
    /// were never acknowledged to anyone.
    ///
    /// This compatibility helper materializes the returned records. Startup
    /// recovery uses the bounded streaming scanner directly and never calls it.
    pub fn read_all(&self) -> Result<(Vec<WalRecord>, u64)> {
        let mut records = Vec::new();
        let scan = self.scan_records(
            MAX_WAL_RECORD_BYTES - RECORD_HEADER_BYTES,
            false,
            |record| {
                records.push(WalRecord {
                    lsn: record.lsn,
                    kind: record.kind,
                    page_id: record.page_id,
                    transaction: record.transaction,
                    payload: record.payload.to_vec(),
                });
                Ok(())
            },
        )?;

        Ok((records, scan.truncated_bytes))
    }

    /// Validate the complete sealed+active chain without retaining record
    /// payloads. Verification uses the same bounded streaming decoder as
    /// recovery and refuses sealed corruption or LSN gaps.
    pub fn verify_integrity(&self) -> Result<u64> {
        Ok(self
            .scan_records(
                MAX_WAL_RECORD_BYTES - RECORD_HEADER_BYTES,
                false,
                |_| Ok(()),
            )?
            .truncated_bytes)
    }

    /// Stream only terminal decisions in `[start_xid, end_xid)`, validating
    /// the WAL while keeping page-image payload memory bounded to one record.
    /// Intended for offline forensic reconstruction of a damaged status spill.
    pub fn terminal_outcomes_in_range(
        &self,
        start_xid: u64,
        end_xid: u64,
    ) -> Result<(Vec<(u64, TxStatus)>, u64)> {
        if start_xid >= end_xid {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "WAL outcome range must have start_xid < end_xid".to_string(),
            });
        }
        let mut outcomes = std::collections::BTreeMap::new();
        let scan = self.scan_records(
            MAX_WAL_RECORD_BYTES - RECORD_HEADER_BYTES,
            false,
            |record| {
                let status = match record.kind {
                    RecordKind::Commit => TxStatus::Committed,
                    RecordKind::Abort => TxStatus::Aborted,
                    _ => return Ok(()),
                };
                if record.transaction < start_xid || record.transaction >= end_xid {
                    return Ok(());
                }
                if let Some(previous) = outcomes.insert(record.transaction, status) {
                    if previous != status {
                        return Err(PageError::WalCorruption {
                            path: self.path.clone(),
                            lsn: record.lsn,
                            reason: format!(
                                "transaction {} has contradictory commit and abort records",
                                record.transaction
                            ),
                        });
                    }
                }
                Ok(())
            },
        )?;
        Ok((outcomes.into_iter().collect(), scan.truncated_bytes))
    }

    /// Stream valid records using one reusable, explicitly bounded body
    /// allocation. When requested, torn-tail truncation happens while the WAL
    /// file lock is still held, so a concurrent append cannot be discarded.
    fn scan_records<F>(
        &self,
        max_body_bytes: usize,
        truncate_torn_tail: bool,
        mut visit: F,
    ) -> Result<WalScanReport>
    where
        F: FnMut(WalRecordView<'_>) -> Result<()>,
    {
        let max_body_bytes = max_body_bytes.min(MAX_WAL_RECORD_BYTES - RECORD_HEADER_BYTES);
        let mut file = self.file.lock();
        let mut scan = WalScanReport::default();
        let mut expected_lsn = None;

        // Sealed segments first, ascending. They are immutable and were made
        // durable before sealing, so a scan that stops early inside one — or
        // an LSN chain break between files — is CORRUPTION, never a torn
        // tail: truncating here would silently drop later sealed segments
        // full of acknowledged commits.
        let sealed = self.sealed.lock().clone();
        for sequence in &sealed {
            let path = self.sealed_path(*sequence);
            let mut segment = File::open(&path).map_err(|e| PageError::io(&path, e))?;
            let (valid, _) = scan_wal_file(
                &mut segment,
                &path,
                max_body_bytes,
                &mut expected_lsn,
                &mut scan,
                &mut visit,
            )?;
            let segment_bytes = segment
                .metadata()
                .map_err(|e| PageError::io(&path, e))?
                .len();
            if valid != segment_bytes {
                return Err(PageError::WalCorruption {
                    path,
                    lsn: scan.last_lsn.unwrap_or(0),
                    reason: format!(
                        "sealed WAL segment holds {segment_bytes} bytes but only {valid} decode; \
                         a sealed segment can never have a torn tail"
                    ),
                });
            }
        }

        // The active file: a torn tail here is expected after a crash.
        file.seek(SeekFrom::Start(0))
            .map_err(|error| PageError::io(&self.path, error))?;
        let file_bytes = file
            .metadata()
            .map_err(|error| PageError::io(&self.path, error))?
            .len();
        let (active_valid, active_first_lsn) = scan_wal_file(
            &mut *file,
            &self.path,
            max_body_bytes,
            &mut expected_lsn,
            &mut scan,
            &mut visit,
        )?;

        scan.truncated_bytes = file_bytes.saturating_sub(active_valid);
        if truncate_torn_tail && scan.truncated_bytes > 0 {
            file.set_len(active_valid)
                .map_err(|error| PageError::io(&self.path, error))?;
        }
        drop(file);

        if truncate_torn_tail {
            self.end_offset.store(active_valid, Ordering::Release);
            let floor = self.next_lsn.load(Ordering::Acquire);
            let next = scan
                .last_lsn
                .map_or(floor.max(1), |lsn| (lsn + 1).max(floor));
            self.next_lsn.store(next, Ordering::Release);
            self.active_first_lsn
                .store(active_first_lsn.unwrap_or(next), Ordering::Release);
        }

        Ok(scan)
    }

    /// Discard a torn tail so later appends are contiguous.
    pub fn truncate_torn_tail(&self) -> Result<u64> {
        Ok(self
            .scan_records(MAX_WAL_RECORD_BYTES - RECORD_HEADER_BYTES, true, |_| Ok(()))?
            .truncated_bytes)
    }

    /// Reset the log after a checkpoint has made it redundant: truncate the
    /// active file and DELETE every sealed segment.
    ///
    /// Only safe once every page dirtied by the discarded records is durable in
    /// the page file. Callers get there through [`Checkpointer::checkpoint`].
    pub fn reset(&self) -> Result<bool> {
        let mut file = self.file.lock();
        let retention = self.archive_retention.load(Ordering::Acquire);
        // Under archive retention EVERY record must flow through a sealed
        // segment before local truncation discards it — the checkpoint
        // record and any commits since the last seal are redundant for
        // local recovery (their pages are durable) but a hole in the
        // archive's LSN chain would fail roll-forward closed forever.
        if retention && self.segment_bytes > 0 {
            let active = self.end_offset.load(Ordering::Acquire);
            if active > 0 {
                self.seal_active_locked(&mut file, active)?;
            }
        }
        file.set_len(0).map_err(|e| PageError::io(&self.path, e))?;
        let archived_through = self.archived_through.load(Ordering::Acquire);
        let mut sealed = self.sealed.lock();
        let mut retained = Vec::new();
        let mut retained_bytes = 0u64;
        for sequence in sealed.drain(..) {
            let path = self.sealed_path(sequence);
            // With archive retention on, a sealed segment the archiver has
            // not confirmed yet must survive local truncation — deleting it
            // would gap the archive chain forever.
            if retention && sequence > archived_through {
                retained_bytes = retained_bytes.saturating_add(
                    std::fs::metadata(&path)
                        .map(|metadata| metadata.len())
                        .unwrap_or(0),
                );
                retained.push(sequence);
                continue;
            }
            std::fs::remove_file(&path).map_err(|e| PageError::io(&path, e))?;
        }
        let locally_empty = retained.is_empty();
        *sealed = retained;
        drop(sealed);
        self.sealed_bytes.store(retained_bytes, Ordering::Release);
        drop(file);
        self.end_offset.store(0, Ordering::Release);
        let next = self.next_lsn.load(Ordering::Acquire);
        self.active_first_lsn.store(next, Ordering::Release);
        self.sync_state.lock().durable_lsn = next.saturating_sub(1);
        // The truncation discarded every commit record this set tracked.
        self.committed_this_generation.lock().clear();
        Ok(locally_empty)
    }

    /// Turn on archive retention: from now until the handle closes,
    /// checkpoint truncation keeps sealed segments the archiver has not
    /// confirmed via [`Self::mark_archived_through`].
    pub fn enable_archive_retention(&self) {
        self.archive_retention.store(true, Ordering::Release);
    }

    /// The archiver durably holds every sealed segment named <= `sequence`;
    /// local truncation may delete them.
    pub fn mark_archived_through(&self, sequence: u64) {
        self.archived_through.fetch_max(sequence, Ordering::AcqRel);
    }

    /// Total log bytes: every sealed segment plus the active file.
    pub fn size_bytes(&self) -> u64 {
        self.sealed_bytes
            .load(Ordering::Acquire)
            .saturating_add(self.end_offset.load(Ordering::Acquire))
    }
}

/// Decode records from one WAL file sequentially, feeding `scan` and
/// `expected_lsn` across calls so LSN continuity spans segment files.
/// Returns the byte length of the valid prefix of THIS file and its first LSN.
fn scan_wal_file<F>(
    file: &mut File,
    path: &Path,
    max_body_bytes: usize,
    expected_lsn: &mut Option<Lsn>,
    scan: &mut WalScanReport,
    visit: &mut F,
) -> Result<(u64, Option<Lsn>)>
where
    F: FnMut(WalRecordView<'_>) -> Result<()>,
{
    file.seek(SeekFrom::Start(0))
        .map_err(|error| PageError::io(path, error))?;
    let file_bytes = file
        .metadata()
        .map_err(|error| PageError::io(path, error))?
        .len();
    let mut valid_bytes = 0u64;
    let mut first_lsn = None;
    let mut header = [0_u8; RECORD_HEADER_BYTES];
    let mut body = Vec::new();

    while file_bytes.saturating_sub(valid_bytes) >= RECORD_HEADER_BYTES as u64 {
        file.read_exact(&mut header)
            .map_err(|error| PageError::io(path, error))?;
        if header[0..4] != RECORD_MAGIC || header[13..16] != [0, 0, 0] {
            break;
        }
        let lsn = u64::from_le_bytes(header[4..12].try_into().unwrap());
        let Some(kind) = RecordKind::from_u8(header[12]) else {
            break;
        };
        let body_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let Some(record_bytes) = RECORD_HEADER_BYTES.checked_add(body_len) else {
            break;
        };
        if body_len < RECORD_FIXED_BODY_BYTES
            || body_len > max_body_bytes
            || record_bytes as u64 > file_bytes.saturating_sub(valid_bytes)
            || lsn == u64::MAX
            || expected_lsn.is_some_and(|expected| lsn != expected)
        {
            break;
        }

        body.resize(body_len, 0);
        file.read_exact(&mut body)
            .map_err(|error| PageError::io(path, error))?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[..RECORD_HEADER_BYTES - 4]);
        hasher.update(&body);
        if hasher.finalize() != checksum {
            break;
        }

        visit(WalRecordView {
            lsn,
            kind,
            page_id: u64::from_le_bytes(body[0..8].try_into().unwrap()),
            transaction: u64::from_le_bytes(body[8..16].try_into().unwrap()),
            payload: &mut body[RECORD_FIXED_BODY_BYTES..],
        })?;
        first_lsn.get_or_insert(lsn);
        scan.records = scan.records.saturating_add(1);
        scan.valid_bytes = scan.valid_bytes.saturating_add(record_bytes as u64);
        scan.peak_record_bytes = scan.peak_record_bytes.max(record_bytes as u64);
        scan.last_lsn = Some(lsn);
        valid_bytes = valid_bytes.saturating_add(record_bytes as u64);
        *expected_lsn = Some(lsn + 1);
    }
    Ok((valid_bytes, first_lsn))
}

/// Sealed segments on disk next to `path`, ascending, with their total size.
fn discover_sealed_segments(path: &Path) -> Result<(Vec<u64>, u64)> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok((Vec::new(), 0));
    };
    let Some(base_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok((Vec::new(), 0));
    };
    let mut sealed = Vec::new();
    let mut total = 0u64;
    for entry in std::fs::read_dir(parent).map_err(|e| PageError::io(parent, e))? {
        let entry = entry.map_err(|e| PageError::io(parent, e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name
            .strip_prefix(base_name)
            .and_then(|rest| rest.strip_prefix('.'))
        else {
            continue;
        };
        let Ok(sequence) = suffix.parse::<u64>() else {
            continue;
        };
        total = total.saturating_add(
            entry
                .metadata()
                .map_err(|e| PageError::io(entry.path(), e))?
                .len(),
        );
        sealed.push(sequence);
    }
    sealed.sort_unstable();
    Ok((sealed, total))
}

/// Transaction outcomes recorded in the log.
///
/// Read separately from [`recover`] because the *page* state and the
/// *transaction* state are recovered by different owners: the page store replays
/// images, while the MVCC transaction table needs to know which ids committed so
/// a crashed transaction's versions stay invisible. Without this, a restart would
/// have no record that a transaction was unfinished, and visibility would have to
/// guess.
pub fn transaction_outcomes(wal: &Wal) -> Result<Vec<(u64, crate::mvcc::TxStatus)>> {
    let mut outcomes = Vec::new();
    wal.scan_records(
        MAX_WAL_RECORD_BYTES - RECORD_HEADER_BYTES,
        false,
        |record| {
            match record.kind {
                RecordKind::Commit => {
                    outcomes.push((record.transaction, crate::mvcc::TxStatus::Committed))
                }
                RecordKind::Abort => {
                    outcomes.push((record.transaction, crate::mvcc::TxStatus::Aborted))
                }
                _ => {}
            }
            Ok(())
        },
    )?;
    Ok(outcomes)
}

/// Replay a log into a page file.
///
/// Two passes, which is what makes "no uncommitted row is exposed" true rather
/// than hoped for:
///
/// 1. scan forward to learn which transactions committed;
/// 2. scan again applying only page images belonging to those transactions.
///
/// A single pass cannot do this: a page image appears *before* the commit
/// record that blesses it, so a one-pass replay would have to apply images
/// speculatively and undo them, which needs before-images the log does not
/// carry.
pub fn recover(wal: &Wal, store: &crate::manager::PageStore) -> Result<RecoveryReport> {
    recover_with_outcomes(wal, store).map(|(report, _)| report)
}

pub(crate) fn recover_with_outcomes(
    wal: &Wal,
    store: &crate::manager::PageStore,
) -> Result<(RecoveryReport, RecoveredOutcomes)> {
    let started = Instant::now();
    let wal_bytes_scanned = wal.size_bytes();
    let max_body_bytes = RECORD_FIXED_BODY_BYTES.saturating_add(store.page_size() as usize);
    let mut outcomes = RecoveredOutcomes::default();
    let mut terminal_outcome_records = 0_u64;
    let mut redo_lsn = 0_u64;
    let mut max_record_transaction = 0_u64;
    let first = wal.scan_records(max_body_bytes, true, |record| {
        // Checkpoint records reuse the transaction field for a redo LSN, so
        // they are excluded; transaction 0 marks structural images. Page
        // images matter here: a crashed transaction that evicted pages left
        // its id only on them, and that id must never be reissued.
        if record.kind != RecordKind::Checkpoint {
            max_record_transaction = max_record_transaction.max(record.transaction);
        }
        match record.kind {
            RecordKind::Commit => {
                if record.transaction == u64::MAX {
                    return Err(PageError::WalCorruption {
                        path: wal.path().to_path_buf(),
                        lsn: record.lsn,
                        reason: "transaction ID cannot advance beyond u64::MAX".to_string(),
                    });
                }
                let inserted = outcomes.push(record.transaction, TxStatus::Committed);
                debug_assert!(inserted);
                terminal_outcome_records = terminal_outcome_records.saturating_add(1);
            }
            RecordKind::Abort => {
                if record.transaction == u64::MAX {
                    return Err(PageError::WalCorruption {
                        path: wal.path().to_path_buf(),
                        lsn: record.lsn,
                        reason: "transaction ID cannot advance beyond u64::MAX".to_string(),
                    });
                }
                let inserted = outcomes.push(record.transaction, TxStatus::Aborted);
                debug_assert!(inserted);
                terminal_outcome_records = terminal_outcome_records.saturating_add(1);
            }
            RecordKind::Checkpoint => redo_lsn = record.transaction,
            RecordKind::PageImage => {}
        }
        Ok(())
    })?;
    let peak_transaction_outcome_bytes = outcomes.allocated_bytes();
    if let Some(xid) = outcomes.normalize() {
        return Err(PageError::WalCorruption {
            path: wal.path().to_path_buf(),
            lsn: first.last_lsn.unwrap_or(0),
            reason: format!("transaction {xid} has contradictory commit and abort records"),
        });
    }
    // A checkpoint record's redo point promises "every page dirtied before
    // this is durable in the page files" — a promise about the files the
    // checkpoint FLUSHED, which are not necessarily the files being
    // recovered. A rolled-forward restore replays an archived WAL over an
    // OLDER base copy whose superblock horizon predates later checkpoints
    // in the stream; skipping to their redo point would leave the pages
    // those checkpoints flushed (in the source!) as zeros here. Only trust
    // a redo point the accompanying page files' own recorded LSN horizon
    // vouches for; otherwise replay everything — idempotent, just slower.
    let redo_lsn = if store.checkpoint_lsn() >= redo_lsn {
        redo_lsn
    } else {
        0
    };
    let mut report = RecoveryReport {
        records_scanned: first.records,
        truncated_bytes: first.truncated_bytes,
        wal_bytes_scanned,
        scan_passes: 2,
        peak_record_bytes: first.peak_record_bytes,
        transaction_outcomes: outcomes.len() as u64,
        terminal_outcome_records,
        peak_transaction_outcome_bytes,
        committed_transactions: outcomes.committed_count() as u64,
        max_record_transaction,
        redo_lsn,
        end_lsn: first.last_lsn.unwrap_or(0),
        ..Default::default()
    };

    // Pass 2: apply committed page images at or after the redo point.
    //
    // Replay is idempotent because records are full page images: applying one
    // twice leaves the same bytes. That is what lets recovery restart safely
    // after being interrupted.
    let mut free_list_detached = false;
    let second = wal.scan_records(max_body_bytes, false, |record| {
        if record.kind != RecordKind::PageImage {
            return Ok(());
        }
        if record.lsn < redo_lsn {
            return Ok(());
        }
        // Transaction 0 means "not part of a transaction" — structural writes
        // such as a new root, which are safe to replay unconditionally.
        if record.transaction != 0
            && outcomes.status(record.transaction) != Some(TxStatus::Committed)
        {
            report.uncommitted_skipped = report.uncommitted_skipped.saturating_add(1);
            return Ok(());
        }
        if record.payload.len() != store.page_size() as usize {
            return Err(PageError::ShortRead {
                page_id: record.page_id,
                got: record.payload.len(),
                want: store.page_size() as usize,
            });
        }
        if !free_list_detached {
            report.free_pages_detached = store.detach_free_list_for_recovery()?;
            free_list_detached = true;
        }
        let page_count = record
            .page_id
            .checked_add(1)
            .ok_or_else(|| PageError::WalCorruption {
                path: wal.path().to_path_buf(),
                lsn: record.lsn,
                reason: "page ID cannot be represented as a page count".to_string(),
            })?;
        store.reserve(page_count)?;
        store.write_page(record.page_id, record.payload)?;
        report.pages_replayed = report.pages_replayed.saturating_add(1);
        Ok(())
    })?;
    if second.truncated_bytes != 0
        || second.records != first.records
        || second.last_lsn != first.last_lsn
    {
        return Err(PageError::WalCorruption {
            path: wal.path().to_path_buf(),
            lsn: first.last_lsn.unwrap_or(0),
            reason: "WAL changed between bounded recovery passes".to_string(),
        });
    }
    report.peak_record_bytes = report.peak_record_bytes.max(second.peak_record_bytes);

    store.flush()?;
    wal.redo_lsn.store(redo_lsn, Ordering::Release);
    report.duration_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    Ok((report, outcomes))
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Ties the buffer pool, page store, and WAL together so write-ahead ordering
/// cannot be violated by forgetting to call something.
#[derive(Debug)]
pub struct Checkpointer {
    pool: std::sync::Arc<crate::pool::BufferPool>,
    wal: std::sync::Arc<Wal>,
}

impl Checkpointer {
    pub fn new(pool: std::sync::Arc<crate::pool::BufferPool>, wal: std::sync::Arc<Wal>) -> Self {
        Self { pool, wal }
    }

    /// Log a page's after-image, then mark it dirty in the pool.
    ///
    /// The order is the invariant: the log record exists before the page is
    /// eligible for writeback, so no dirty page can reach disk ahead of its log
    /// record.
    pub fn log_and_write(&self, page_id: PageId, transaction: u64, image: &[u8]) -> Result<Lsn> {
        let lsn = self.wal.log_page_image(page_id, transaction, image)?;
        let mut guard = self.pool.get_mut(page_id)?;
        guard.bytes_mut().copy_from_slice(image);
        Ok(lsn)
    }

    /// Flush dirty pages and record a durable redo point.
    ///
    /// Order matters and is the entire content of the guarantee:
    ///
    /// 1. note where the log currently ends — this becomes the redo point;
    /// 2. sync the log, so every page about to be written is already logged;
    /// 3. write dirty pages and fsync the page file;
    /// 4. only now record the checkpoint.
    ///
    /// Doing 4 before 3 would let a crash leave a checkpoint claiming pages are
    /// durable when they are not, and recovery would skip exactly the records
    /// needed to fix them.
    pub fn checkpoint(&self, root_page: PageId) -> Result<CheckpointReport> {
        let redo_lsn = self.wal.next_lsn();
        self.wal.sync()?;
        let flushed = self.pool.flush_all()?;
        self.pool.store().flush()?;
        self.finish_checkpoint(root_page, redo_lsn, flushed, false)
    }

    /// Checkpoint, then discard the now-redundant log prefix.
    ///
    /// This is what bounds the WAL. Without it the log grows without limit and
    /// recovery time grows with total writes rather than with the post-checkpoint
    /// suffix — which is the gate Phase 3 is judged by.
    pub fn checkpoint_and_truncate(&self, root_page: PageId) -> Result<CheckpointReport> {
        let redo_lsn = self.wal.next_lsn();
        self.wal.sync()?;
        let flushed = self.pool.flush_all()?;
        self.pool.store().flush()?;
        self.finish_checkpoint(root_page, redo_lsn, flushed, true)
    }

    /// Publish a checkpoint after a caller has independently proven every dirty
    /// page durable.
    ///
    /// This is the terminal operation used by the bounded checkpoint state
    /// machine. It deliberately does not inspect or flush the buffer pool. The
    /// caller must hold its writer fence and observe zero dirty pages after the
    /// final page-file sync. Keeping publication separate is what allows a large
    /// dirty set to be drained across many governed steps instead of one
    /// unbounded `flush_all` call.
    pub(crate) fn finish_checkpoint(
        &self,
        root_page: PageId,
        redo_lsn: Lsn,
        pages_flushed: u64,
        truncate: bool,
    ) -> Result<CheckpointReport> {
        self.wal.sync()?;
        self.pool.store().flush()?;
        let wal_bytes_before = self.wal.size_bytes();
        let checkpoint_lsn = self.wal.checkpoint(redo_lsn, root_page)?;
        // Persist the new LSN horizon before any reset removes the records
        // that establish it. Otherwise a crash in the reset/publish gap can
        // reopen an empty WAL at an older floor and reuse LSNs or segment
        // names from the discarded generation.
        self.pool
            .store()
            .publish_root(root_page, self.wal.next_lsn())?;
        let wal_truncated = if truncate {
            // Safe only under the caller contract above: every page represented
            // by the discarded log is already durable.
            self.wal.reset()?
        } else {
            false
        };
        Ok(CheckpointReport {
            redo_lsn,
            checkpoint_lsn,
            pages_flushed,
            wal_bytes_before,
            wal_truncated,
        })
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointReport {
    pub redo_lsn: Lsn,
    pub checkpoint_lsn: Lsn,
    pub pages_flushed: u64,
    pub wal_bytes_before: u64,
    pub wal_truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{PageStore, PageStoreOptions};
    use crate::page::{PageHeader, PageType, PAGE_HEADER_BYTES};
    use crate::pool::{BufferPool, BufferPoolOptions};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn wal(dir: &TempDir) -> Wal {
        Wal::open(dir.path().join("test.wal"), false).unwrap()
    }

    fn store(dir: &TempDir, page_size: u32) -> Arc<PageStore> {
        Arc::new(
            PageStore::open(
                dir.path().join("test.pages"),
                PageStoreOptions::default()
                    .with_page_size(page_size)
                    .with_fsync(false),
            )
            .unwrap(),
        )
    }

    fn page_image(page_id: PageId, page_size: u32, marker: u8) -> Vec<u8> {
        let mut page = vec![0u8; page_size as usize];
        PageHeader::new(page_id, PageType::Heap, page_size).encode(&mut page);
        page[PAGE_HEADER_BYTES] = marker;
        crate::page::finalize(&mut page, 1);
        page
    }

    #[test]
    fn records_round_trip_in_order() {
        let dir = TempDir::new().unwrap();
        let wal = wal(&dir);
        let a = wal.log_page_image(5, 1, b"image-a").unwrap();
        let b = wal.log_page_image(6, 1, b"image-b").unwrap();
        let c = wal.commit(1).unwrap();

        let (records, truncated) = wal.read_all().unwrap();
        assert_eq!(truncated, 0);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].lsn, a);
        assert_eq!(records[0].page_id, 5);
        assert_eq!(records[0].payload, b"image-a");
        assert_eq!(records[1].lsn, b);
        assert_eq!(records[2].lsn, c);
        assert_eq!(records[2].kind, RecordKind::Commit);
        assert!(a < b && b < c, "LSNs must increase");
    }

    #[test]
    fn terminal_outcome_range_streams_only_requested_decisions() {
        let dir = TempDir::new().unwrap();
        let wal = wal(&dir);
        wal.commit(8).unwrap();
        wal.log_page_image(5, 9, b"ignored page image").unwrap();
        wal.abort(9).unwrap();
        wal.commit(10).unwrap();
        wal.commit(11).unwrap();

        let (outcomes, truncated) = wal.terminal_outcomes_in_range(9, 11).unwrap();
        assert_eq!(truncated, 0);
        assert_eq!(
            outcomes,
            vec![(9, TxStatus::Aborted), (10, TxStatus::Committed)]
        );
    }

    #[test]
    fn sync_batches_latency_and_checkpoint_freshness_are_exact_and_strict() {
        let dir = TempDir::new().unwrap();
        let wal = wal(&dir);
        for page_id in 1..=3 {
            wal.log_page_image(page_id, 1, b"image").unwrap();
        }
        assert_eq!(wal.snapshot().syncs, 0);
        wal.sync().unwrap();

        let first = wal.snapshot();
        assert_eq!(first.syncs, 1);
        assert_eq!(first.sync_failures, 0);
        assert_eq!(first.sync_records_total, 3);
        assert_eq!(first.last_sync_records, 3);
        assert_eq!(first.max_sync_records, 3);
        assert_eq!(first.sync_latency.count, first.syncs);
        assert_eq!(first.sync_latency.cumulative_buckets[8], first.syncs);
        assert!(first.sync_latency.is_consistent());

        wal.sync().unwrap();
        assert_eq!(
            wal.snapshot().syncs,
            first.syncs,
            "an already-durable target synced again"
        );

        wal.checkpoint(wal.next_lsn(), 0).unwrap();
        let checkpointed = wal.snapshot();
        assert_eq!(checkpointed.checkpoints, 1);
        assert!(checkpointed.last_checkpoint_completed_at_millis > 0);
        assert!(checkpointed.checkpoint_age_millis < 10_000);
        assert_eq!(checkpointed.sync_records_total, 4);
        assert_eq!(checkpointed.last_sync_records, 1);
        assert_eq!(checkpointed.max_sync_records, 3);
        assert_eq!(checkpointed.sync_latency.count, checkpointed.syncs);

        let encoded = serde_json::to_value(checkpointed).unwrap();
        assert_eq!(
            serde_json::from_value::<WalSnapshot>(encoded.clone()).unwrap(),
            checkpointed
        );
        let mut forged = encoded;
        forged["sync_latency"]["dynamic_bucket"] = serde_json::json!(1);
        assert!(serde_json::from_value::<WalSnapshot>(forged).is_err());
    }

    #[test]
    fn a_corrupted_record_stops_the_replay_at_that_point() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        {
            let wal = Wal::open(&path, false).unwrap();
            for index in 0..5u64 {
                wal.log_page_image(index, 1, b"payload").unwrap();
            }
            wal.sync().unwrap();
        }

        // Corrupt a byte inside the third record's payload.
        let mut bytes = std::fs::read(&path).unwrap();
        let record_len = RECORD_HEADER_BYTES + 16 + 7;
        let target = record_len * 2 + RECORD_HEADER_BYTES + 16;
        bytes[target] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let wal = Wal::open(&path, false).unwrap();
        let (records, truncated) = wal.read_all().unwrap();
        assert_eq!(records.len(), 2, "replay continued past a bad checksum");
        assert!(truncated > 0);
    }

    #[test]
    fn a_torn_tail_is_truncated_rather_than_treated_as_corruption() {
        // A crash almost always lands mid-record, so this is the NORMAL case,
        // not an error: those bytes were never acknowledged to anyone.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        {
            let wal = Wal::open(&path, false).unwrap();
            wal.log_page_image(1, 1, b"complete").unwrap();
            wal.commit(1).unwrap();
            wal.log_page_image(2, 2, b"interrupted").unwrap();
            wal.sync().unwrap();
        }

        // Chop the final record in half.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 10).unwrap();
        drop(file);

        let wal = Wal::open(&path, false).unwrap();
        let truncated = wal.truncate_torn_tail().unwrap();
        assert!(truncated > 0);

        let (records, remaining) = wal.read_all().unwrap();
        assert_eq!(remaining, 0, "torn tail was not removed");
        assert_eq!(records.len(), 2);

        // And the log is appendable again, contiguously.
        wal.log_page_image(3, 3, b"after-recovery").unwrap();
        let (records, _) = wal.read_all().unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn recovery_applies_committed_pages_and_skips_uncommitted_ones() {
        let dir = TempDir::new().unwrap();
        let page_size = 512u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        store.reserve(10).unwrap();

        // Transaction 1 commits; transaction 2 does not.
        wal.log_page_image(1, 1, &page_image(1, page_size, 0xAA))
            .unwrap();
        wal.commit(1).unwrap();
        wal.log_page_image(2, 2, &page_image(2, page_size, 0xBB))
            .unwrap();
        wal.sync().unwrap();

        let report = recover(&wal, &store).unwrap();
        assert_eq!(report.pages_replayed, 1);
        assert_eq!(report.uncommitted_skipped, 1);
        assert_eq!(report.committed_transactions, 1);
        assert_eq!(report.wal_bytes_scanned, wal.size_bytes());
        assert_eq!(report.scan_passes, 2);
        assert_eq!(report.transaction_outcomes, 1);
        assert_eq!(report.terminal_outcome_records, 1);
        assert!(report.peak_transaction_outcome_bytes >= 9);
        assert_eq!(
            report.peak_record_bytes,
            (RECORD_HEADER_BYTES + RECORD_FIXED_BODY_BYTES + page_size as usize) as u64
        );
        assert!(report.peak_record_bytes <= MAX_WAL_RECORD_BYTES as u64);
        assert!(report.duration_nanos > 0);

        let mut page = vec![0u8; page_size as usize];
        store.read_page(1, &mut page).unwrap();
        assert_eq!(page[PAGE_HEADER_BYTES], 0xAA, "committed page not restored");

        // The uncommitted page must NOT be visible.
        assert!(
            store.read_page(2, &mut page).is_err() || page[PAGE_HEADER_BYTES] != 0xBB,
            "an uncommitted page was exposed by recovery"
        );
    }

    #[test]
    fn recovery_streams_a_large_suffix_with_one_page_sized_record_buffer() {
        let dir = TempDir::new().unwrap();
        let page_size = 512_u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        let image = page_image(1, page_size, 0x5A);
        let records = 20_000_u64;

        // No transaction outcome is recorded, so the second pass validates and
        // skips every image without growing the page file. The WAL itself is
        // more than ten thousand times larger than the retained record buffer.
        for page_id in 1..=records {
            wal.log_page_image(page_id, 99, &image).unwrap();
        }
        wal.sync().unwrap();
        let wal_bytes = wal.size_bytes();
        assert!(wal_bytes > 10 * 1024 * 1024);

        let report = recover(&wal, &store).unwrap();
        assert_eq!(report.records_scanned, records);
        assert_eq!(report.scan_passes, 2);
        assert_eq!(report.uncommitted_skipped, records);
        assert_eq!(report.pages_replayed, 0);
        assert_eq!(report.transaction_outcomes, 0);
        assert_eq!(report.terminal_outcome_records, 0);
        assert_eq!(report.peak_transaction_outcome_bytes, 0);
        assert_eq!(report.wal_bytes_scanned, wal_bytes);
        assert_eq!(
            report.peak_record_bytes,
            (RECORD_HEADER_BYTES + RECORD_FIXED_BODY_BYTES + page_size as usize) as u64
        );
        assert!(wal_bytes > report.peak_record_bytes.saturating_mul(10_000));
    }

    #[test]
    fn recovery_compacts_many_terminal_outcomes() {
        let dir = TempDir::new().unwrap();
        let page_size = 512_u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        let outcomes = 20_000_u64;

        for xid in (1..=outcomes).rev() {
            wal.abort(xid).unwrap();
        }
        wal.sync().unwrap();

        let report = recover(&wal, &store).unwrap();
        assert_eq!(report.terminal_outcome_records, outcomes);
        assert_eq!(report.transaction_outcomes, outcomes);
        assert_eq!(report.committed_transactions, 0);
        assert!(report.peak_transaction_outcome_bytes >= outcomes.saturating_mul(9));
        assert!(report.peak_transaction_outcome_bytes < outcomes.saturating_mul(20));
    }

    #[test]
    fn abort_after_commit_is_not_appended_and_commit_survives_recovery() {
        let dir = TempDir::new().unwrap();
        let page_size = 512_u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        wal.commit(7).unwrap();
        // The natural cleanup after a failed commit fsync: the commit record
        // may be on disk anyway, so the abort must not create a contradiction
        // that recovery would refuse to open.
        wal.abort(7).unwrap();
        wal.sync().unwrap();

        let (report, outcomes) = recover_with_outcomes(&wal, &store).unwrap();
        assert_eq!(report.transaction_outcomes, 1);
        assert_eq!(outcomes.status(7), Some(crate::mvcc::TxStatus::Committed));
    }

    #[test]
    fn recovery_rejects_contradictory_terminal_outcomes() {
        let dir = TempDir::new().unwrap();
        let page_size = 512_u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        wal.commit(7).unwrap();
        // The live abort path refuses to write this contradiction; a
        // corrupted or hand-built log can still contain one, and recovery
        // must keep rejecting it.
        wal.append(RecordKind::Abort, 0, 7, &[]).unwrap();
        wal.sync().unwrap();

        let error = recover(&wal, &store).unwrap_err();
        assert!(matches!(error, PageError::WalCorruption { .. }));
        assert!(error.to_string().contains("contradictory commit and abort"));
    }

    #[test]
    fn oversized_tail_length_is_truncated_before_body_allocation() {
        let dir = TempDir::new().unwrap();
        let page_size = 512_u32;
        let store = store(&dir, page_size);
        let path = dir.path().join("test.wal");
        let wal = Wal::open(&path, false).unwrap();
        wal.log_page_image(1, 1, &page_image(1, page_size, 0xA5))
            .unwrap();
        wal.commit(1).unwrap();
        let valid_bytes = wal.size_bytes();

        let mut forged = Vec::with_capacity(RECORD_HEADER_BYTES);
        forged.extend_from_slice(&RECORD_MAGIC);
        forged.extend_from_slice(&wal.next_lsn().to_le_bytes());
        forged.push(RecordKind::PageImage as u8);
        forged.extend_from_slice(&[0_u8; 3]);
        forged.extend_from_slice(&u32::MAX.to_le_bytes());
        forged.extend_from_slice(&0_u32.to_le_bytes());
        assert_eq!(forged.len(), RECORD_HEADER_BYTES);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&forged)
            .unwrap();
        wal.end_offset
            .store(valid_bytes + forged.len() as u64, Ordering::Release);

        let report = recover(&wal, &store).unwrap();
        assert_eq!(report.truncated_bytes, RECORD_HEADER_BYTES as u64);
        assert_eq!(wal.size_bytes(), valid_bytes);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_bytes);
        assert!(report.peak_record_bytes <= MAX_WAL_RECORD_BYTES as u64);

        // Recovery also restored the append position; no LSN hole is created.
        wal.log_page_image(2, 2, &page_image(2, page_size, 0x5A))
            .unwrap();
        assert_eq!(wal.read_all().unwrap().1, 0);
    }

    #[test]
    fn recovery_rejects_identifiers_that_would_overflow_runtime_state() {
        let dir = TempDir::new().unwrap();
        let page_size = 512_u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("page-id.wal"), false).unwrap();
        wal.log_page_image(PageId::MAX, 1, &page_image(1, page_size, 0xA5))
            .unwrap();
        wal.commit(1).unwrap();
        let error = recover(&wal, &store).unwrap_err();
        assert!(matches!(error, PageError::WalCorruption { .. }));
        assert!(error.is_corruption());

        let wal = Wal::open(dir.path().join("transaction-id.wal"), false).unwrap();
        wal.commit(u64::MAX).unwrap();
        let error = recover(&wal, &store).unwrap_err();
        assert!(matches!(error, PageError::WalCorruption { .. }));
        assert!(error.is_corruption());
    }

    #[test]
    fn recovery_is_idempotent() {
        // Replay must survive being interrupted and restarted, which is only
        // true because records are full page images.
        let dir = TempDir::new().unwrap();
        let page_size = 512u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        store.reserve(10).unwrap();

        for index in 1..=5u64 {
            wal.log_page_image(index, 1, &page_image(index, page_size, index as u8))
                .unwrap();
        }
        wal.commit(1).unwrap();

        let first = recover(&wal, &store).unwrap();
        let second = recover(&wal, &store).unwrap();
        let third = recover(&wal, &store).unwrap();
        assert_eq!(first.pages_replayed, second.pages_replayed);
        assert_eq!(second.pages_replayed, third.pages_replayed);

        for index in 1..=5u64 {
            let mut page = vec![0u8; page_size as usize];
            store.read_page(index, &mut page).unwrap();
            assert_eq!(page[PAGE_HEADER_BYTES], index as u8);
        }
    }

    #[test]
    fn an_aborted_transactions_pages_are_never_applied() {
        let dir = TempDir::new().unwrap();
        let page_size = 512u32;
        let store = store(&dir, page_size);
        let wal = Wal::open(dir.path().join("test.wal"), false).unwrap();
        store.reserve(10).unwrap();

        wal.log_page_image(1, 7, &page_image(1, page_size, 0x11))
            .unwrap();
        wal.abort(7).unwrap();
        wal.sync().unwrap();

        let report = recover(&wal, &store).unwrap();
        assert_eq!(report.pages_replayed, 0);
        assert_eq!(report.uncommitted_skipped, 1);
    }

    #[test]
    fn a_checkpoint_moves_the_redo_point_forward() {
        let dir = TempDir::new().unwrap();
        let page_size = 512u32;
        let store = store(&dir, page_size);
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default().with_budget_bytes(32 * 512),
            )
            .unwrap(),
        );
        let wal = Arc::new(Wal::open(dir.path().join("test.wal"), false).unwrap());
        let checkpointer = Checkpointer::new(pool, wal.clone());

        for index in 0..5u64 {
            let page_id = store.allocate(PageType::Heap).unwrap();
            checkpointer
                .log_and_write(page_id, 1, &page_image(page_id, page_size, index as u8))
                .unwrap();
        }
        wal.commit(1).unwrap();

        let report = checkpointer.checkpoint(0).unwrap();
        assert!(report.redo_lsn > 0);
        assert_eq!(wal.redo_lsn(), report.redo_lsn);
        // The checkpoint record itself takes the redo LSN: "everything BEFORE
        // redo_lsn is already durable" is satisfied by the record that declares
        // it, so equality is correct rather than off by one.
        assert!(report.checkpoint_lsn >= report.redo_lsn);
    }

    #[test]
    fn recovery_after_a_checkpoint_only_replays_the_suffix() {
        // The Phase 3 gate: recovery cost tracks the post-checkpoint suffix, not
        // total writes.
        let dir = TempDir::new().unwrap();
        let page_size = 512u32;
        let store = store(&dir, page_size);
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default().with_budget_bytes(64 * 512),
            )
            .unwrap(),
        );
        let wal = Arc::new(Wal::open(dir.path().join("test.wal"), false).unwrap());
        let checkpointer = Checkpointer::new(pool, wal.clone());

        let mut pages = Vec::new();
        for index in 0..40u64 {
            let page_id = store.allocate(PageType::Heap).unwrap();
            pages.push(page_id);
            checkpointer
                .log_and_write(page_id, 1, &page_image(page_id, page_size, index as u8))
                .unwrap();
        }
        wal.commit(1).unwrap();
        checkpointer.checkpoint(0).unwrap();
        // The redo point is only honored when the page files' own recorded
        // horizon vouches for it (see recover_with_outcomes) — publish it,
        // exactly as PagedStore's checkpoint does.
        store.publish_root(0, wal.next_lsn()).unwrap();

        // A little more work after the checkpoint.
        for index in 0..3u64 {
            let page_id = store.allocate(PageType::Heap).unwrap();
            checkpointer
                .log_and_write(
                    page_id,
                    2,
                    &page_image(page_id, page_size, 0xF0 | index as u8),
                )
                .unwrap();
        }
        wal.commit(2).unwrap();

        let report = recover(&wal, &store).unwrap();
        assert!(
            report.pages_replayed <= 4,
            "recovery replayed {} pages; it should only cover the post-checkpoint \
             suffix, not all 43",
            report.pages_replayed
        );
        assert!(report.redo_lsn > 0);
    }

    #[test]
    fn truncating_after_a_checkpoint_bounds_the_log() {
        let dir = TempDir::new().unwrap();
        let page_size = 512u32;
        let store = store(&dir, page_size);
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default().with_budget_bytes(64 * 512),
            )
            .unwrap(),
        );
        let wal = Arc::new(Wal::open(dir.path().join("test.wal"), false).unwrap());
        let checkpointer = Checkpointer::new(pool, wal.clone());

        for index in 0..30u64 {
            let page_id = store.allocate(PageType::Heap).unwrap();
            checkpointer
                .log_and_write(page_id, 1, &page_image(page_id, page_size, index as u8))
                .unwrap();
        }
        wal.commit(1).unwrap();
        let before = wal.size_bytes();
        assert!(before > 0);

        checkpointer.checkpoint_and_truncate(0).unwrap();
        assert_eq!(wal.size_bytes(), 0, "log was not bounded by the checkpoint");

        // The data is still there, from the page file rather than the log.
        let mut page = vec![0u8; page_size as usize];
        store.read_page(1, &mut page).unwrap();
    }

    #[test]
    fn group_commit_shares_one_sync_without_weakening_durability() {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(Wal::open(dir.path().join("test.wal"), false).unwrap());

        let mut handles = Vec::new();
        for transaction in 0..8u64 {
            let wal = wal.clone();
            handles.push(std::thread::spawn(move || {
                wal.log_page_image(transaction, transaction, b"work")
                    .unwrap();
                let lsn = wal.commit(transaction).unwrap();
                // The contract: on return, this commit IS durable.
                assert!(
                    wal.durable_lsn() >= lsn,
                    "commit returned before its own record was durable"
                );
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = wal.snapshot();
        assert_eq!(snapshot.records_appended, 16);
        assert!(snapshot.syncs >= 1);
        assert!(
            snapshot.syncs <= 8,
            "no group commit happened: {} syncs for 8 commits",
            snapshot.syncs
        );
        assert_eq!(snapshot.sync_latency.count, snapshot.syncs);
        assert_eq!(snapshot.sync_records_total, snapshot.records_appended);
        assert!(snapshot.last_sync_records <= snapshot.max_sync_records);
        assert!(snapshot.max_sync_records >= 2);
    }

    #[test]
    fn concurrent_appends_produce_contiguous_lsns() {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(Wal::open(dir.path().join("test.wal"), false).unwrap());

        let mut handles = Vec::new();
        for transaction in 0..6u64 {
            let wal = wal.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..20 {
                    wal.log_page_image(transaction, transaction, b"x").unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        wal.sync().unwrap();

        let (records, truncated) = wal.read_all().unwrap();
        assert_eq!(truncated, 0, "concurrent appends interleaved mid-record");
        assert_eq!(records.len(), 120);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.lsn, index as u64 + 1, "LSNs are not contiguous");
        }
    }

    #[test]
    fn an_empty_log_recovers_to_nothing() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let wal = wal(&dir);
        let report = recover(&wal, &store).unwrap();
        assert_eq!(report.records_scanned, 0);
        assert_eq!(report.pages_replayed, 0);
    }

    #[test]
    fn a_log_with_out_of_order_lsns_stops_at_the_discontinuity() {
        // Two log generations concatenated, or a partial overwrite. Replaying
        // across it would apply records whose ordering cannot be trusted.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        {
            let wal = Wal::open(&path, false).unwrap();
            wal.log_page_image(1, 1, b"one").unwrap();
            wal.log_page_image(2, 1, b"two").unwrap();
            wal.sync().unwrap();
        }
        let first = std::fs::read(&path).unwrap();
        {
            // A second generation restarting from LSN 1.
            std::fs::remove_file(&path).unwrap();
            let wal = Wal::open(&path, false).unwrap();
            wal.log_page_image(3, 1, b"three").unwrap();
            wal.sync().unwrap();
        }
        let second = std::fs::read(&path).unwrap();
        let mut concatenated = first;
        concatenated.extend_from_slice(&second);
        std::fs::write(&path, &concatenated).unwrap();

        let wal = Wal::open(&path, false).unwrap();
        let (records, truncated) = wal.read_all().unwrap();
        assert_eq!(records.len(), 2, "replay crossed an LSN discontinuity");
        assert!(truncated > 0);
    }

    #[test]
    fn reopen_restores_the_active_segment_identity_before_the_next_rotation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store.wal");
        let payload = vec![7_u8; 96];
        {
            let wal = Wal::open_with_segments_and_floor(&path, false, 180, 1).unwrap();
            for page_id in 1..=5 {
                wal.log_page_image(page_id, 0, &payload).unwrap();
            }
            wal.sync().unwrap();
            assert!(!wal.sealed_segment_paths().is_empty());
            assert!(wal.end_offset.load(Ordering::Acquire) > 0);
        }

        // A checkpoint floor can be older than the first record in the
        // surviving active file after a crash during checkpoint/reset. Open
        // starts from that floor; recovery must replace it with the active
        // file's observed first LSN before another rotation chooses a name.
        let wal = Wal::open_with_segments_and_floor(&path, false, 180, 1).unwrap();
        wal.truncate_torn_tail().unwrap();
        let active_first = wal.active_first_lsn.load(Ordering::Acquire);
        assert!(active_first > 1);
        wal.log_page_image(6, 0, &payload).unwrap();

        let sealed = wal.sealed.lock().clone();
        let unique = sealed
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            sealed.len(),
            unique.len(),
            "WAL segment identity was reused"
        );
        wal.reset()
            .expect("checkpoint cleanup must not address one segment twice");
    }
}
