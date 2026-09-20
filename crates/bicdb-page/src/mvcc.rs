//! Disk-resident MVCC: version chains, snapshot visibility, and rollback.
//!
//! Phase 2 of `docs/server-paged-storage-todo.md`:
//!
//! - "Store tuple MVCC metadata on disk: creator transaction, deleter
//!   transaction, previous-version locator, and visibility/status information."
//! - "Keep only active transaction state, recent commit status, lock state, and
//!   bounded hot metadata resident. Older transaction visibility data must be
//!   checkpointed and pageable."
//!
//! # Version layout
//!
//! Each stored version carries a fixed header ahead of its value:
//!
//! ```text
//! xmin  u64   transaction that created this version
//! xmax  u64   transaction that deleted/superseded it, 0 if live
//! prev  14 B  locator of the previous version, all-zero if none
//! ```
//!
//! The primary index maps a key to its **newest** version, and each version
//! points backwards. A snapshot read walks that chain until it reaches a version
//! it can see. Newest-first is the right direction because the common case — a
//! reader at the latest snapshot — stops immediately, and only readers holding
//! old snapshots pay to walk.
//!
//! # Rollback comes for free, and that is the point
//!
//! A previous milestone recorded "steal without undo" as an open gap: an
//! uncommitted page evicted to disk stayed there with no way to take it back.
//! Version chains close it without an UNDO log. An aborted transaction's
//! versions are simply never *visible* — visibility is decided by the
//! transaction's recorded status, not by the bytes being absent. So a rolled-back
//! or crashed transaction leaves versions on disk that no snapshot will ever
//! return.
//!
//! The bytes are still there, so this is a **space** obligation (vacuum) rather
//! than a correctness one. That is a much better place to be than needing undo
//! records to be correct.
//!
//! # Bounded transaction status
//!
//! Status for every transaction that ever ran cannot be resident — that grows
//! without limit. Instead a `frozen_xid` watermark divides history: every
//! transaction below it is known committed unless it appears in the bounded
//! **abort exception** set, and only transactions at or above it need a
//! resident entry. The map is therefore bounded by in-flight plus recently
//! finished transactions, not by total transactions ever.
//!
//! Before 1.0.85-beta the watermark could only advance across a contiguous run
//! of *committed* transactions: one aborted transaction pinned it forever,
//! which kept every later commit unfrozen, blocked WAL truncation, and let
//! recovery's outcome set grow with the whole post-checkpoint suffix. Freezing
//! now records an aborted — or crash-orphaned, terminal-record-less —
//! transaction as a durable exception and advances past it. Exceptions are
//! persisted by the checkpoint's meta page before the WAL holding their abort
//! records may be truncated, so a restart learns them from the page file
//! rather than from the log. The set has a fixed capacity derived from the
//! page size; when that capacity is exhausted the watermark degrades to its
//! old behavior and stops, which is reported rather than hidden.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::Result;
use crate::slotted::TupleLocator;

/// Transaction identifier. Monotonic, never reused.
pub type Xid = u64;

/// Bytes of the version header preceding each value.
pub const VERSION_HEADER_BYTES: usize = 8 + 8 + 14;

/// What became of a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxStatus {
    InProgress,
    Committed,
    Aborted,
}

/// One terminal WAL decision in a compact sortable encoding.
///
/// A tuple `(u64, TxStatus)` occupies 16 bytes because of alignment. Recovery
/// can retain millions of decisions when a long-running transaction delays a
/// checkpoint, so that padding is material. Big-endian XID bytes preserve
/// numeric order and one explicit status byte makes this exactly nine bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RecoveredOutcome([u8; 9]);

impl RecoveredOutcome {
    pub(crate) const COMMITTED: u8 = 1;
    pub(crate) const ABORTED: u8 = 2;

    fn new(xid: Xid, status: TxStatus) -> Option<Self> {
        let encoded_status = match status {
            TxStatus::Committed => Self::COMMITTED,
            TxStatus::Aborted => Self::ABORTED,
            TxStatus::InProgress => return None,
        };
        let mut bytes = [0_u8; 9];
        bytes[..8].copy_from_slice(&xid.to_be_bytes());
        bytes[8] = encoded_status;
        Some(Self(bytes))
    }

    /// Decode one on-disk spill entry, rejecting unknown status bytes.
    pub(crate) fn from_spill_bytes(bytes: [u8; 9]) -> Option<Self> {
        match bytes[8] {
            Self::COMMITTED | Self::ABORTED => Some(Self(bytes)),
            _ => None,
        }
    }

    pub(crate) fn to_spill_bytes(self) -> [u8; 9] {
        self.0
    }

    pub(crate) fn xid(self) -> Xid {
        Xid::from_be_bytes(self.0[..8].try_into().expect("fixed outcome XID"))
    }

    pub(crate) fn status(self) -> TxStatus {
        match self.0[8] {
            Self::COMMITTED => TxStatus::Committed,
            Self::ABORTED => TxStatus::Aborted,
            _ => unreachable!("compact outcome status is constructed internally"),
        }
    }
}

/// Compact terminal decisions learned during the first WAL pass. Sorting is
/// allocation-free, lookup is logarithmic, and ownership moves directly into
/// the live MVCC table after replay.
#[derive(Debug, Default)]
pub(crate) struct RecoveredOutcomes {
    entries: Vec<RecoveredOutcome>,
    start: usize,
}

impl RecoveredOutcomes {
    pub(crate) fn push(&mut self, xid: Xid, status: TxStatus) -> bool {
        let Some(outcome) = RecoveredOutcome::new(xid, status) else {
            return false;
        };
        self.entries.push(outcome);
        true
    }

    /// Sort, collapse repeated identical decisions, and identify a transaction
    /// with contradictory terminal records. Returns that XID on contradiction.
    pub(crate) fn normalize(&mut self) -> Option<Xid> {
        self.entries.sort_unstable();
        let mut write = 0usize;
        for read in 0..self.entries.len() {
            let current = self.entries[read];
            if write > 0 && self.entries[write - 1].xid() == current.xid() {
                if self.entries[write - 1].status() != current.status() {
                    return Some(current.xid());
                }
                continue;
            }
            self.entries[write] = current;
            write += 1;
        }
        self.entries.truncate(write);
        None
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len().saturating_sub(self.start)
    }

    pub(crate) fn allocated_bytes(&self) -> u64 {
        u64::try_from(self.entries.capacity())
            .unwrap_or(u64::MAX)
            .saturating_mul(std::mem::size_of::<RecoveredOutcome>() as u64)
    }

    pub(crate) fn committed_count(&self) -> usize {
        self.entries
            .get(self.start..)
            .unwrap_or_default()
            .iter()
            .filter(|outcome| outcome.status() == TxStatus::Committed)
            .count()
    }

    pub(crate) fn status(&self, xid: Xid) -> Option<TxStatus> {
        self.entries[self.start..]
            .binary_search_by(|outcome| outcome.xid().cmp(&xid))
            .ok()
            .map(|index| self.entries[self.start + index].status())
    }

    pub(crate) fn max_xid(&self) -> Option<Xid> {
        self.entries
            .get(self.start..)
            .and_then(|entries| entries.last())
            .copied()
            .map(RecoveredOutcome::xid)
    }

    fn discard_below(&mut self, frozen_xid: Xid) {
        self.start = self
            .entries
            .partition_point(|outcome| outcome.xid() < frozen_xid);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.entries.shrink_to_fit();
        self.start = 0;
    }

    fn front(&self) -> Option<RecoveredOutcome> {
        self.entries.get(self.start).copied()
    }

    fn pop_front(&mut self) {
        if self.start < self.entries.len() {
            self.start += 1;
        }
        if self.start == self.entries.len() {
            self.entries.clear();
            self.entries.shrink_to_fit();
            self.start = 0;
        }
    }

    fn iter(&self) -> impl Iterator<Item = RecoveredOutcome> + '_ {
        self.entries[self.start..].iter().copied()
    }
}

/// MVCC metadata for one stored version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionHeader {
    pub xmin: Xid,
    pub xmax: Xid,
    pub prev: Option<TupleLocator>,
}

impl VersionHeader {
    pub fn new(xmin: Xid) -> Self {
        Self {
            xmin,
            xmax: 0,
            prev: None,
        }
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.xmin.to_le_bytes());
        out.extend_from_slice(&self.xmax.to_le_bytes());
        match self.prev {
            Some(locator) => out.extend_from_slice(&locator.encode()),
            None => out.extend_from_slice(&[0u8; 14]),
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<(Self, &[u8])> {
        if bytes.len() < VERSION_HEADER_BYTES {
            return None;
        }
        let xmin = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let xmax = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let prev_bytes = &bytes[16..30];
        // An all-zero locator means "no previous version". Page 0 is the
        // superblock, so a real locator can never be all-zero — the encoding is
        // unambiguous rather than merely conventional.
        let prev = if prev_bytes.iter().all(|byte| *byte == 0) {
            None
        } else {
            TupleLocator::decode(prev_bytes)
        };
        Some((Self { xmin, xmax, prev }, &bytes[VERSION_HEADER_BYTES..]))
    }

    /// Rewrite `xmax` in an encoded version, in place.
    pub fn patch_xmax(bytes: &mut [u8], xmax: Xid) {
        if bytes.len() >= 16 {
            bytes[8..16].copy_from_slice(&xmax.to_le_bytes());
        }
    }
}

/// A read snapshot.
///
/// # Why the in-flight set is necessary
///
/// A transaction id alone cannot decide visibility. Ids are assigned at BEGIN,
/// but a transaction becomes visible at COMMIT, and those orders differ: a
/// transaction that began before this snapshot may commit after it, and its
/// writes must stay invisible.
///
/// An earlier version of this used "xid < my own id" as the whole test, which
/// silently exposed exactly those commits — a snapshot-isolation violation that
/// looks correct in any test where transactions commit in the order they began.
/// Recording which transactions were running at snapshot time is what closes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The reading transaction. Its own uncommitted writes are visible to it.
    /// Zero for a read outside any transaction.
    pub xid: Xid,
    /// One past the highest id assigned when this snapshot was taken. Anything
    /// at or above began afterwards.
    pub xmax: Xid,
    /// Transactions in progress when this snapshot was taken. Invisible for the
    /// snapshot's whole life, whatever they go on to do.
    pub in_flight: Arc<BTreeSet<Xid>>,
}

impl Snapshot {
    /// Whether `xid`'s work began before this snapshot and was not still running.
    fn began_before(&self, xid: Xid) -> bool {
        xid < self.xmax && !self.in_flight.contains(&xid)
    }
}

/// Result of one allocation-free, bounded transaction-watermark advance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FreezeBelowReport {
    pub frozen: usize,
    /// Aborted or crash-orphaned transactions converted into abort exceptions
    /// so the watermark could advance past them.
    pub exceptions_recorded: usize,
    pub limit_reached: bool,
    /// The next xid is still in progress, so no exception can represent it.
    pub blocked: bool,
    /// The next xid would need an abort exception but the durable exception
    /// capacity is exhausted. Freezing degrades to its pre-1.0.85-beta stop
    /// behavior until capacity is available again.
    pub exception_capacity_full: bool,
}

/// Disk-backed lookup for terminal outcomes evicted from the resident table.
///
/// A long-running transaction pins the frozen watermark, and every commit
/// above it would otherwise stay resident — and retain the WAL — until it
/// ends. Checkpoints instead persist those outcomes to a status spill and
/// evict them here; visibility then resolves them on demand from disk.
pub(crate) trait OutcomeSpill: Send + Sync + std::fmt::Debug {
    /// The recorded outcome of `xid`, if the spill retains it.
    ///
    /// `Err` means the spill could not answer — an unreadable or corrupt
    /// page, not an absent record. Callers deciding *durable* state (the
    /// freeze path, which turns "no record" into a permanent abort
    /// exception) must propagate the error rather than degrade it to
    /// "no record": one transient read error would otherwise invert a
    /// committed transaction's visibility forever.
    fn status_checked(&self, xid: Xid) -> Result<Option<TxStatus>>;

    /// Fail-safe variant for the visibility read path: an unanswerable
    /// lookup degrades to `None`, which readers treat as in-progress
    /// (invisible) — the safe direction for a transient fault.
    fn status(&self, xid: Xid) -> Option<TxStatus> {
        self.status_checked(xid).unwrap_or(None)
    }
}

/// Transaction status with a bounded resident footprint.
#[derive(Debug)]
pub struct TransactionTable {
    inner: Mutex<TableInner>,
}

#[derive(Debug)]
struct TableInner {
    next_xid: Xid,
    /// Status for transactions at or above `frozen_xid` only.
    status: BTreeMap<Xid, TxStatus>,
    /// Sorted terminal outcomes moved from WAL recovery without rebuilding one
    /// allocation-heavy tree node per transaction.
    recovered: RecoveredOutcomes,
    /// Active transactions only. This avoids scanning or materializing every
    /// retained commit status when checkpoint/vacuum needs the oldest snapshot.
    in_progress: BTreeSet<Xid>,
    /// Everything below this is known committed unless excepted.
    frozen_xid: Xid,
    /// Terminal non-committed decisions the watermark has advanced past.
    /// Persisted by the checkpoint meta page so they survive WAL truncation.
    /// Bounded by `abort_exception_capacity`.
    abort_exceptions: BTreeSet<Xid>,
    abort_exception_capacity: usize,
    /// Disk-backed outcomes evicted by a checkpoint's status spill.
    spill: Option<Arc<dyn OutcomeSpill>>,
}

impl Default for TransactionTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionTable {
    /// A table whose watermark stops at any aborted transaction. Retained for
    /// contexts without a durable exception store; [`Self::with_exception_capacity`]
    /// is what a checkpointed store uses.
    pub fn new() -> Self {
        Self::with_exception_capacity(0)
    }

    pub fn with_exception_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(TableInner {
                // Xid 0 is reserved: it means "no transaction" in `xmax`, and
                // structural writes use it to mean "always visible".
                next_xid: 1,
                status: BTreeMap::new(),
                recovered: RecoveredOutcomes::default(),
                in_progress: BTreeSet::new(),
                frozen_xid: 1,
                abort_exceptions: BTreeSet::new(),
                abort_exception_capacity: capacity,
                spill: None,
            }),
        }
    }

    /// Install the disk-backed outcome store a checkpointed store uses to
    /// evict terminal status above a pinned watermark.
    pub(crate) fn set_spill(&self, spill: Arc<dyn OutcomeSpill>) {
        self.inner.lock().spill = Some(spill);
    }

    /// Begin a transaction, returning its id and read snapshot.
    pub fn begin(&self) -> (Xid, Snapshot) {
        let mut inner = self.inner.lock();
        let xid = inner.next_xid;
        inner.next_xid += 1;
        // Captured BEFORE inserting ourselves: the set is who else was running.
        let in_flight = inner.in_progress.clone();
        inner.status.insert(xid, TxStatus::InProgress);
        inner.in_progress.insert(xid);
        (
            xid,
            Snapshot {
                xid,
                xmax: xid,
                in_flight: Arc::new(in_flight),
            },
        )
    }

    /// A snapshot seeing everything committed as of now, outside any transaction.
    pub fn latest_snapshot(&self) -> Snapshot {
        let inner = self.inner.lock();
        let in_flight = inner.in_progress.clone();
        Snapshot {
            xid: 0,
            xmax: inner.next_xid,
            in_flight: Arc::new(in_flight),
        }
    }

    pub fn commit(&self, xid: Xid) {
        let mut inner = self.inner.lock();
        inner.status.insert(xid, TxStatus::Committed);
        inner.in_progress.remove(&xid);
    }

    pub fn abort(&self, xid: Xid) {
        let mut inner = self.inner.lock();
        // Commit wins: a recorded commit is durable (or at least written to
        // the log), and downgrading it here would diverge the resident table
        // from what recovery reconstructs.
        if inner.status.get(&xid) == Some(&TxStatus::Committed) {
            return;
        }
        inner.status.insert(xid, TxStatus::Aborted);
        inner.in_progress.remove(&xid);
    }

    pub fn status(&self, xid: Xid) -> TxStatus {
        if xid == 0 {
            // Structural writes are unconditionally visible.
            return TxStatus::Committed;
        }
        let resident;
        let spill;
        {
            let inner = self.inner.lock();
            if xid < inner.frozen_xid {
                // Below the watermark everything committed — except transactions
                // the watermark explicitly stepped over by recording them here.
                return if inner.abort_exceptions.contains(&xid) {
                    TxStatus::Aborted
                } else {
                    TxStatus::Committed
                };
            }
            resident = resident_terminal_status(&inner, xid);
            spill = inner.spill.clone();
        }
        if let Some(status) = resident {
            return status;
        }
        // Evicted by a checkpoint's status spill: resolve on demand from disk,
        // without holding the table lock across the read.
        if let Some(status) = spill.as_ref().and_then(|spill| spill.status(xid)) {
            return status;
        }
        // A spill miss is not yet proof of a crash-lost transaction: between
        // dropping the lock above and the lookup, a concurrent freeze +
        // persist cycle may have advanced the watermark past `xid` and
        // dropped its (committed) entry from the spill. Re-check under the
        // lock before concluding in-progress, or a committed row transiently
        // reads as invisible.
        {
            let inner = self.inner.lock();
            if xid < inner.frozen_xid {
                return if inner.abort_exceptions.contains(&xid) {
                    TxStatus::Aborted
                } else {
                    TxStatus::Committed
                };
            }
            if let Some(status) = resident_terminal_status(&inner, xid) {
                return status;
            }
        }
        // A transaction we have no record of at or above the watermark never
        // finished — it was lost to a crash. Treating an unknown transaction as
        // in-progress (hence invisible) is the safe direction: the alternative
        // exposes uncommitted rows.
        TxStatus::InProgress
    }

    /// Restore the watermarks and abort exceptions persisted by a checkpoint.
    ///
    /// Without this, a checkpoint that truncates the log destroys the only
    /// record that older transactions committed, and every row they wrote
    /// becomes invisible on the next open — the data is intact on disk and
    /// unreachable. The watermark is what lets status be discarded safely.
    ///
    /// The exceptions are the watermark's side condition: transactions it has
    /// stepped over must not read back as committed once their abort records
    /// are truncated with the log.
    pub fn restore(&self, frozen_xid: Xid, next_xid: Xid, abort_exceptions: Vec<Xid>) {
        let mut inner = self.inner.lock();
        inner.frozen_xid = inner.frozen_xid.max(frozen_xid);
        inner.next_xid = inner.next_xid.max(next_xid).max(1);
        for xid in abort_exceptions {
            inner.abort_exceptions.insert(xid);
        }
    }

    /// Note a transaction's outcome learned from the log during recovery.
    pub fn record_recovered(&self, xid: Xid, status: TxStatus) {
        let mut inner = self.inner.lock();
        inner.status.insert(xid, status);
        if status == TxStatus::InProgress {
            inner.in_progress.insert(xid);
        } else {
            inner.in_progress.remove(&xid);
        }
        if xid >= inner.next_xid {
            inner.next_xid = xid + 1;
        }
    }

    /// Ensure `next_xid` is at least `candidate`.
    ///
    /// Recovery calls this with one past the highest transaction id named by
    /// ANY WAL record — page images included. A crashed transaction that
    /// evicted dirty pages (steal) but never reached a terminal record leaves
    /// its id only on page images; deriving `next_xid` from terminal outcomes
    /// alone would reissue that id, and the reissued transaction's commit
    /// would retroactively bless the crashed one's on-disk versions.
    pub fn ensure_next_xid_at_least(&self, candidate: Xid) {
        let mut inner = self.inner.lock();
        inner.next_xid = inner.next_xid.max(candidate).max(1);
    }

    /// Install the final outcomes learned by bounded WAL recovery without
    /// copying them through a second container at startup.
    pub(crate) fn record_recovered_batch(&self, mut outcomes: RecoveredOutcomes) {
        let mut inner = self.inner.lock();
        if let Some(max_xid) = outcomes.max_xid() {
            inner.next_xid = inner.next_xid.max(max_xid.saturating_add(1));
        }
        outcomes.discard_below(inner.frozen_xid);
        inner.recovered = outcomes;
    }

    /// Every resident terminal outcome, ascending. In-progress transactions
    /// are excluded: their outcome is still undecided.
    pub(crate) fn terminal_outcomes(&self) -> Vec<RecoveredOutcome> {
        let inner = self.inner.lock();
        let mut outcomes: Vec<RecoveredOutcome> = inner
            .status
            .iter()
            .filter(|(_, status)| **status != TxStatus::InProgress)
            .filter_map(|(xid, status)| RecoveredOutcome::new(*xid, *status))
            .collect();
        outcomes.extend(inner.recovered.iter());
        outcomes.sort_unstable();
        outcomes.dedup();
        outcomes
    }

    /// Discard every resident terminal outcome. Called only after
    /// [`Self::terminal_outcomes`] has been persisted in the durable status
    /// spill — that spill is what visibility resolves them from afterwards.
    /// This eviction is what bounds resident transaction memory, and releases
    /// the WAL truncation gate, while a long-running transaction pins the
    /// watermark.
    pub(crate) fn evict_terminal_outcomes(&self) {
        let mut inner = self.inner.lock();
        inner
            .status
            .retain(|_, status| *status == TxStatus::InProgress);
        inner.recovered.clear();
    }

    /// Discard status for finished transactions below the oldest live snapshot.
    ///
    /// This is what keeps the table bounded. Committed transactions freeze
    /// outright; aborted ones freeze as abort exceptions so they keep reading
    /// as aborted after the watermark passes them.
    pub fn freeze_below(&self, oldest_active: Xid) -> Result<FreezeBelowReport> {
        self.freeze_below_bounded(oldest_active, usize::MAX)
    }

    /// Advance the durable-visibility watermark through at most `max_xids`
    /// contiguous decided transactions.
    ///
    /// This performs no collection proportional to resident transaction state:
    /// it repeatedly examines only the current watermark. `limit_reached`
    /// means another bounded call can continue immediately. `blocked` means
    /// the next xid is still in progress. `exception_capacity_full` means the
    /// next xid is aborted or crash-orphaned and would need an abort
    /// exception, but the durable exception capacity is exhausted.
    ///
    /// A transaction with no terminal record at all is treated like an abort
    /// here: below `oldest_active` every assigned xid has finished or been
    /// lost to a crash, and a lost transaction can never commit — its id is
    /// never reused — so recording it as an exception is exactly the status
    /// its rows must keep.
    ///
    /// A spill read error fails the whole step: "no record" is only decidable
    /// against a readable spill, and degrading an error to "no record" would
    /// permanently record a committed transaction as an abort exception.
    pub fn freeze_below_bounded(
        &self,
        oldest_active: Xid,
        max_xids: usize,
    ) -> Result<FreezeBelowReport> {
        let mut inner = self.inner.lock();
        let mut frozen = 0usize;
        let mut exceptions_recorded = 0usize;
        let mut exception_capacity_full = false;
        while frozen < max_xids && inner.frozen_xid < oldest_active {
            let xid = inner.frozen_xid;
            match terminal_status(&inner, xid)? {
                Some(TxStatus::InProgress) => break,
                Some(TxStatus::Committed) => {}
                Some(TxStatus::Aborted) | None => {
                    if inner.abort_exceptions.len() >= inner.abort_exception_capacity {
                        exception_capacity_full = true;
                        break;
                    }
                    inner.abort_exceptions.insert(xid);
                    exceptions_recorded = exceptions_recorded.saturating_add(1);
                }
            }
            if inner
                .recovered
                .front()
                .is_some_and(|outcome| outcome.xid() == xid)
            {
                inner.recovered.pop_front();
            } else {
                inner.status.remove(&xid);
            }
            inner.frozen_xid = xid.saturating_add(1);
            frozen = frozen.saturating_add(1);
        }
        let still_freezable = inner.frozen_xid < oldest_active
            && match terminal_status(&inner, inner.frozen_xid)? {
                Some(TxStatus::Committed) => true,
                Some(TxStatus::InProgress) => false,
                Some(TxStatus::Aborted) | None => {
                    inner.abort_exceptions.len() < inner.abort_exception_capacity
                }
            };
        Ok(FreezeBelowReport {
            frozen,
            exceptions_recorded,
            limit_reached: frozen == max_xids && still_freezable,
            blocked: inner.frozen_xid < oldest_active
                && !still_freezable
                && !exception_capacity_full,
            exception_capacity_full,
        })
    }

    /// Jump the durable-visibility watermark to `target`, blessing every id
    /// below it with no surviving record as committed-ancient. This is the
    /// REPAIR primitive for version headers stamped with transaction ids the
    /// allocator never issued (observed in production as `0x1fb9_0011_1fe7`
    /// on a hand-rebuilt store): such an id can never terminate, so the row
    /// it stamps is permanently unwritable through the ordinary conflict
    /// check — and the incremental freeze cannot help, because it advances
    /// one id at a time and each unknown id would consume one of the bounded
    /// abort-exception slots.
    ///
    /// The jump is refused unless it is provably safe:
    /// - `target` must not exceed the allocator — raise that first, so no
    ///   future `begin` can hand out an id below the watermark;
    /// - no transaction below `target` may be in progress — a running
    ///   transaction below the watermark would read as committed;
    /// - every resident outcome below `target` must be committed — an
    ///   unfrozen abort would silently flip to committed-by-default.
    ///   (Aborts already absorbed as exceptions keep their status: the
    ///   exception set is consulted before the below-watermark rule and is
    ///   preserved by the jump.)
    /// Read-only preflight for [`Self::jump_frozen_to`]: the first obstacle
    /// that would make the jump refuse, or `None` when it would succeed.
    /// Lets the repair orchestration refuse BEFORE mutating anything.
    /// The allocator condition is deliberately NOT checked here: the floor
    /// advance raises the allocator after this preflight clears, and
    /// [`Self::jump_frozen_to`] re-verifies it as its own invariant.
    pub fn watermark_jump_obstacle(&self, target: Xid) -> Option<String> {
        let inner = self.inner.lock();
        if target <= inner.frozen_xid {
            return None;
        }
        if let Some(active) = inner.in_progress.range(..target).next() {
            return Some(format!(
                "transaction {active} is still in progress below the target"
            ));
        }
        if let Some((xid, status)) = inner
            .status
            .iter()
            .filter(|(xid, _)| **xid < target)
            .find(|(_, status)| **status != TxStatus::Committed)
        {
            return Some(format!(
                "transaction {xid} below the target holds an unfrozen {status:?} outcome"
            ));
        }
        None
    }

    pub fn jump_frozen_to(&self, target: Xid) -> Result<Xid> {
        let mut inner = self.inner.lock();
        if target <= inner.frozen_xid {
            return Ok(inner.frozen_xid);
        }
        if target > inner.next_xid {
            return Err(crate::error::PageError::TransactionFloorRefused {
                target,
                reason: format!(
                    "the allocator has only issued ids below {}; advance it first",
                    inner.next_xid
                ),
            });
        }
        if let Some(active) = inner.in_progress.range(..target).next() {
            return Err(crate::error::PageError::TransactionFloorRefused {
                target,
                reason: format!("transaction {active} is still in progress below the target"),
            });
        }
        if let Some((xid, status)) = inner
            .status
            .iter()
            .filter(|(xid, _)| **xid < target)
            .find(|(_, status)| **status != TxStatus::Committed)
        {
            return Err(crate::error::PageError::TransactionFloorRefused {
                target,
                reason: format!(
                    "transaction {xid} below the target holds an unfrozen {status:?} outcome"
                ),
            });
        }
        if let Some(outcome) = inner
            .recovered
            .front()
            .filter(|outcome| outcome.xid() < target && outcome.status() != TxStatus::Committed)
        {
            let xid = outcome.xid();
            let _ = &outcome;
            return Err(crate::error::PageError::TransactionFloorRefused {
                target,
                reason: format!("recovered transaction {xid} below the target is not committed"),
            });
        }
        // Committed residue below the watermark is redundant once the
        // below-watermark rule covers it.
        inner.status.retain(|xid, _| *xid >= target);
        while inner
            .recovered
            .front()
            .is_some_and(|outcome| outcome.xid() < target)
        {
            inner.recovered.pop_front();
        }
        inner.frozen_xid = target;
        Ok(target)
    }

    pub fn frozen_xid(&self) -> Xid {
        self.inner.lock().frozen_xid
    }

    pub fn next_xid(&self) -> Xid {
        self.inner.lock().next_xid
    }

    /// Whether any transaction is recorded as committed but not yet frozen.
    ///
    /// These are the transactions whose commit is durable *only* in the log.
    /// [`Self::freeze_below`] advances across a contiguous committed prefix, so a
    /// single in-flight transaction leaves every later commit unfrozen — and the
    /// meta page records only `frozen_xid`, not the status map. Discarding the
    /// log while any of these exist destroys the sole evidence that they
    /// committed, and their rows come back invisible: present in the heap,
    /// reachable through the index, and judged uncommitted by the reader.
    ///
    /// So this is the question a checkpoint must ask before truncating.
    pub fn has_unfrozen_commits(&self) -> bool {
        let inner = self.inner.lock();
        inner
            .status
            .range(inner.frozen_xid..)
            .any(|(_, status)| *status == TxStatus::Committed)
            || inner.recovered.iter().any(|outcome| {
                outcome.xid() >= inner.frozen_xid && outcome.status() == TxStatus::Committed
            })
    }

    /// Transactions still in progress.
    pub fn in_progress(&self) -> Vec<Xid> {
        self.inner.lock().in_progress.iter().copied().collect()
    }

    /// Oldest active transaction, or the next unassigned xid when idle.
    /// Allocation-free and logarithmic regardless of retained commit history.
    pub fn oldest_in_progress_or_next(&self) -> Xid {
        let inner = self.inner.lock();
        inner.in_progress.first().copied().unwrap_or(inner.next_xid)
    }

    /// Resident entries — the figure that must stay bounded. Exceptions are
    /// excluded: they are bounded separately by the durable capacity.
    pub fn resident_entries(&self) -> usize {
        let inner = self.inner.lock();
        inner.status.len().saturating_add(inner.recovered.len())
    }

    /// Durable abort exceptions in ascending order, for persistence in the
    /// checkpoint meta page.
    pub fn abort_exceptions(&self) -> Vec<Xid> {
        let inner = self.inner.lock();
        inner.abort_exceptions.iter().copied().collect()
    }

    pub fn abort_exception_count(&self) -> usize {
        self.inner.lock().abort_exceptions.len()
    }

    pub fn abort_exception_capacity(&self) -> usize {
        self.inner.lock().abort_exception_capacity
    }

    /// Whether `snapshot` can see a version with this header.
    ///
    /// Two conditions, and both matter:
    ///
    /// - the creating transaction must be visible — committed and begun before
    ///   the snapshot, or the snapshot's own transaction;
    /// - the deleting transaction must NOT be visible — otherwise the version
    ///   has been superseded as far as this snapshot is concerned.
    pub fn is_visible(&self, header: &VersionHeader, snapshot: &Snapshot) -> bool {
        if !self.xid_visible(header.xmin, snapshot) {
            return false;
        }
        if header.xmax == 0 {
            return true;
        }
        !self.xid_visible(header.xmax, snapshot)
    }

    pub(crate) fn xid_visible(&self, xid: Xid, snapshot: &Snapshot) -> bool {
        if xid == 0 {
            return true;
        }
        // A transaction always sees its own work, committed or not.
        if xid != 0 && xid == snapshot.xid {
            return true;
        }
        // Begun after the snapshot, or still running when it was taken: not
        // visible regardless of what it does later.
        if !snapshot.began_before(xid) {
            return false;
        }
        self.status(xid) == TxStatus::Committed
    }

    /// Whether `xid` is concurrent with `snapshot` — neither visible to it nor
    /// aborted. Writing over such a transaction's row is a lost update.
    pub fn is_concurrent(&self, xid: Xid, snapshot: &Snapshot) -> bool {
        if xid == 0 || xid == snapshot.xid {
            return false;
        }
        if self.status(xid) == TxStatus::Aborted {
            return false;
        }
        !snapshot.began_before(xid) || self.status(xid) == TxStatus::InProgress
    }
}

/// Resident-only terminal lookup: the in-memory status map and the recovered
/// outcome vector, but not the disk-backed spill.
fn resident_terminal_status(inner: &TableInner, xid: Xid) -> Option<TxStatus> {
    if let Some(status) = inner.status.get(&xid).copied() {
        return Some(status);
    }
    inner.recovered.status(xid)
}

/// Terminal lookup including the disk-backed spill.
///
/// The spill is authoritative for exactly the range the resident table covers,
/// so an outcome evicted into it must resolve identically here. Omitting it is
/// what made the open-time freeze mistake an evicted commit for a crash-orphan
/// and record it as an abort exception — silently inverting its visibility.
fn terminal_status(inner: &TableInner, xid: Xid) -> Result<Option<TxStatus>> {
    if let Some(status) = resident_terminal_status(inner, xid) {
        return Ok(Some(status));
    }
    match inner.spill.as_ref() {
        Some(spill) => spill.status_checked(xid),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locator(page: u64) -> TupleLocator {
        TupleLocator::new(page, 3, 7)
    }

    #[test]
    fn version_headers_round_trip() {
        let mut header = VersionHeader::new(42);
        header.xmax = 99;
        header.prev = Some(locator(1234));

        let mut encoded = Vec::new();
        header.encode_into(&mut encoded);
        encoded.extend_from_slice(b"the value");

        let (decoded, value) = VersionHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(value, b"the value");
    }

    #[test]
    fn a_missing_previous_version_is_unambiguous() {
        // An all-zero locator means "none". Page 0 is the superblock, so no real
        // locator can collide with it.
        let header = VersionHeader::new(1);
        let mut encoded = Vec::new();
        header.encode_into(&mut encoded);
        encoded.extend_from_slice(b"v");
        let (decoded, _) = VersionHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.prev, None);
    }

    #[test]
    fn patching_xmax_leaves_everything_else_intact() {
        let header = VersionHeader::new(5);
        let mut encoded = Vec::new();
        header.encode_into(&mut encoded);
        encoded.extend_from_slice(b"payload");

        VersionHeader::patch_xmax(&mut encoded, 77);
        let (decoded, value) = VersionHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.xmin, 5);
        assert_eq!(decoded.xmax, 77);
        assert_eq!(value, b"payload");
    }

    #[test]
    fn a_transaction_sees_its_own_uncommitted_write() {
        let table = TransactionTable::new();
        let (xid, snapshot) = table.begin();
        let header = VersionHeader::new(xid);
        assert!(table.is_visible(&header, &snapshot));
        let _ = &snapshot;
    }

    #[test]
    fn an_uncommitted_write_is_invisible_to_others() {
        let table = TransactionTable::new();
        let (writer, _) = table.begin();
        let (_, reader_snapshot) = table.begin();

        let header = VersionHeader::new(writer);
        assert!(
            !table.is_visible(&header, &reader_snapshot),
            "an in-progress transaction's write leaked to another snapshot"
        );
    }

    #[test]
    fn a_transaction_that_commits_after_a_snapshot_began_stays_invisible_to_it() {
        // The case begin-order alone gets wrong: `writer` has a LOWER id than
        // `reader`, so an id comparison says "earlier, therefore visible" — but
        // it was still running when `reader` took its snapshot, and only
        // committed afterwards.
        let table = TransactionTable::new();
        let (writer, _) = table.begin();
        let (_, reader) = table.begin();
        table.commit(writer);

        let header = VersionHeader::new(writer);
        assert!(
            !table.is_visible(&header, &reader),
            "a commit that happened after the snapshot was visible to it"
        );

        // A snapshot taken after the commit does see it.
        let (_, later) = table.begin();
        assert!(table.is_visible(&header, &later));
    }

    #[test]
    fn a_concurrent_transaction_is_reported_as_such() {
        let table = TransactionTable::new();
        let (writer, _) = table.begin();
        let (_, reader) = table.begin();
        assert!(table.is_concurrent(writer, &reader));
        table.commit(writer);
        assert!(
            table.is_concurrent(writer, &reader),
            "committing does not make a concurrent transaction non-concurrent"
        );
        table.abort(writer);
        assert!(
            table.is_concurrent(writer, &reader),
            "commit wins: a spurious abort after commit must not downgrade it"
        );
        let (aborted, _) = table.begin();
        table.abort(aborted);
        assert!(
            !table.is_concurrent(aborted, &reader),
            "an aborted transaction cannot cause a lost update"
        );
    }

    #[test]
    fn an_aborted_transactions_versions_are_never_visible() {
        // This is rollback: no UNDO log, the version is simply unreachable.
        let table = TransactionTable::new();
        let (writer, _) = table.begin();
        table.abort(writer);
        let (_, snapshot) = table.begin();

        let header = VersionHeader::new(writer);
        assert!(
            !table.is_visible(&header, &snapshot),
            "an aborted transaction's version was visible"
        );
    }

    #[test]
    fn a_transaction_lost_to_a_crash_is_treated_as_unfinished() {
        // No status record at or above the watermark means it never completed.
        // Reading that as committed would expose uncommitted rows after a crash.
        let table = TransactionTable::new();
        let (_, snapshot) = table.begin();
        let ghost = 9_999;
        assert_eq!(table.status(ghost), TxStatus::InProgress);
        assert!(!table.is_visible(&VersionHeader::new(ghost), &snapshot));
    }

    #[test]
    fn a_deleted_version_is_invisible_once_the_deleter_commits() {
        let table = TransactionTable::new();
        let (creator, _) = table.begin();
        table.commit(creator);
        let (deleter, _) = table.begin();

        let mut header = VersionHeader::new(creator);
        header.xmax = deleter;

        // Before the delete commits, other snapshots still see the row.
        let (_, other) = table.begin();
        assert!(table.is_visible(&header, &other));

        table.commit(deleter);
        let (_, after) = table.begin();
        assert!(
            !table.is_visible(&header, &after),
            "a committed delete left the row visible"
        );
    }

    #[test]
    fn a_snapshot_taken_before_a_delete_still_sees_the_row() {
        // The whole point of MVCC: an old reader is unaffected by a later delete.
        let table = TransactionTable::new();
        let (creator, _) = table.begin();
        table.commit(creator);
        let (_, reader) = table.begin();
        let (deleter, _) = table.begin();
        table.commit(deleter);

        let mut header = VersionHeader::new(creator);
        header.xmax = deleter;
        assert!(
            table.is_visible(&header, &reader),
            "a reader's snapshot was violated by a later delete"
        );
    }

    #[test]
    fn freezing_bounds_the_resident_table() {
        let table = TransactionTable::new();
        for _ in 0..500 {
            let (xid, _) = table.begin();
            table.commit(xid);
        }
        assert_eq!(table.resident_entries(), 500);

        let report = table.freeze_below(table.next_xid()).unwrap();
        assert_eq!(report.frozen, 500);
        assert_eq!(report.exceptions_recorded, 0);
        assert_eq!(
            table.resident_entries(),
            0,
            "the transaction table is not bounded"
        );
        // And frozen transactions still read as committed.
        assert_eq!(table.status(1), TxStatus::Committed);
        assert_eq!(table.status(250), TxStatus::Committed);
    }

    #[test]
    fn bounded_freezing_advances_exactly_the_declared_prefix() {
        let table = TransactionTable::new();
        for _ in 0..5 {
            let (xid, _) = table.begin();
            table.commit(xid);
        }
        let first = table.freeze_below_bounded(table.next_xid(), 2).unwrap();
        assert_eq!(first.frozen, 2);
        assert!(first.limit_reached);
        assert!(!first.blocked);
        assert_eq!(table.frozen_xid(), 3);

        let second = table.freeze_below_bounded(table.next_xid(), 2).unwrap();
        assert_eq!(second.frozen, 2);
        assert!(second.limit_reached);
        let final_step = table.freeze_below_bounded(table.next_xid(), 2).unwrap();
        assert_eq!(final_step.frozen, 1);
        assert!(!final_step.limit_reached);
        assert!(!final_step.blocked);
        assert_eq!(table.resident_entries(), 0);
    }

    #[test]
    fn oldest_active_index_tracks_commit_and_abort_without_status_scan() {
        let table = TransactionTable::new();
        let (first, _) = table.begin();
        let (second, _) = table.begin();
        let (third, _) = table.begin();
        assert_eq!(table.oldest_in_progress_or_next(), first);
        table.commit(first);
        assert_eq!(table.oldest_in_progress_or_next(), second);
        table.abort(second);
        assert_eq!(table.oldest_in_progress_or_next(), third);
        table.commit(third);
        assert_eq!(table.oldest_in_progress_or_next(), table.next_xid());
    }

    #[test]
    fn freezing_stops_at_an_aborted_transaction() {
        // Freezing past an abort would make its rows retroactively visible,
        // because everything below the watermark reads as committed.
        let table = TransactionTable::new();
        let (first, _) = table.begin();
        table.commit(first);
        let (doomed, _) = table.begin();
        table.abort(doomed);
        let (third, _) = table.begin();
        table.commit(third);

        table.freeze_below(table.next_xid()).unwrap();
        assert_eq!(
            table.status(doomed),
            TxStatus::Aborted,
            "an aborted transaction was frozen away and became visible"
        );
        assert!(table.frozen_xid() <= doomed);
    }

    #[test]
    fn freezing_does_not_discard_in_progress_transactions() {
        let table = TransactionTable::new();
        let (running, _) = table.begin();
        table.freeze_below(table.next_xid()).unwrap();
        assert_eq!(table.status(running), TxStatus::InProgress);
    }

    #[test]
    fn freezing_passes_an_abort_by_recording_a_durable_exception() {
        // The pre-1.0.85-beta watermark stopped forever at an aborted xid,
        // pinning every later commit. With exception capacity the watermark
        // steps over it, and the aborted transaction must STILL read as
        // aborted — otherwise its rows would become retroactively visible.
        let table = TransactionTable::with_exception_capacity(8);
        let (first, _) = table.begin();
        table.commit(first);
        let (doomed, _) = table.begin();
        table.abort(doomed);
        let (third, _) = table.begin();
        table.commit(third);

        let report = table.freeze_below(table.next_xid()).unwrap();
        assert_eq!(report.frozen, 3);
        assert_eq!(report.exceptions_recorded, 1);
        assert!(!report.blocked);
        assert!(!report.exception_capacity_full);
        assert_eq!(table.frozen_xid(), third + 1);
        assert_eq!(
            table.status(doomed),
            TxStatus::Aborted,
            "an aborted transaction frozen via exception must remain aborted"
        );
        assert_eq!(table.status(first), TxStatus::Committed);
        assert_eq!(table.status(third), TxStatus::Committed);
        assert_eq!(table.resident_entries(), 0);
        assert_eq!(table.abort_exceptions(), vec![doomed]);
    }

    #[test]
    fn a_crash_orphaned_xid_freezes_as_an_exception() {
        // A xid with no terminal record below the oldest active transaction
        // was lost to a crash: it can never commit because its id is never
        // reused. Freezing records it as an exception, which keeps its rows
        // invisible exactly like an abort.
        let table = TransactionTable::with_exception_capacity(8);
        table.record_recovered(8, TxStatus::Committed);
        // Xids 1..=7 were assigned but carry no terminal record: orphans.

        let report = table.freeze_below(table.next_xid()).unwrap();
        assert_eq!(report.frozen, 8);
        assert_eq!(report.exceptions_recorded, 7);
        assert_eq!(table.status(3), TxStatus::Aborted);
        assert_eq!(table.status(8), TxStatus::Committed);
    }

    #[test]
    fn freeze_exceptions_stop_at_the_durable_capacity() {
        let table = TransactionTable::with_exception_capacity(2);
        for _ in 0..4 {
            let (xid, _) = table.begin();
            table.abort(xid);
        }

        let report = table.freeze_below(table.next_xid()).unwrap();
        assert_eq!(report.frozen, 2);
        assert_eq!(report.exceptions_recorded, 2);
        assert!(report.exception_capacity_full);
        assert!(!report.blocked);
        assert_eq!(table.frozen_xid(), 3);

        // A table without any exception capacity degrades to the old stop
        // behavior at the first abort.
        let legacy = TransactionTable::new();
        let (doomed, _) = legacy.begin();
        legacy.abort(doomed);
        let legacy_report = legacy.freeze_below(legacy.next_xid()).unwrap();
        assert_eq!(legacy_report.frozen, 0);
        assert!(legacy_report.exception_capacity_full);
        assert!(!legacy_report.blocked);
    }

    #[test]
    fn restored_exceptions_keep_aborted_transactions_invisible_below_the_watermark() {
        let table = TransactionTable::with_exception_capacity(4);
        table.restore(100, 101, vec![42, 99]);
        assert_eq!(table.status(41), TxStatus::Committed);
        assert_eq!(table.status(42), TxStatus::Aborted);
        assert_eq!(table.status(99), TxStatus::Aborted);
        assert_eq!(table.status(100), TxStatus::InProgress);
        assert_eq!(table.abort_exception_count(), 2);
    }

    #[test]
    fn recovered_status_advances_the_id_counter() {
        // After recovery, new transactions must not reuse ids the log already
        // mentions, or a new transaction would inherit an old one's visibility.
        let table = TransactionTable::new();
        table.record_recovered(500, TxStatus::Committed);
        let (xid, _) = table.begin();
        assert!(xid > 500, "reused a recovered transaction id");
        assert_eq!(table.status(500), TxStatus::Committed);
    }

    #[test]
    fn recovered_batch_moves_terminal_outcomes_and_advances_ids() {
        let table = TransactionTable::new();
        let mut outcomes = RecoveredOutcomes::default();
        assert!(outcomes.push(41, TxStatus::Aborted));
        assert!(outcomes.push(40, TxStatus::Committed));
        assert!(!outcomes.push(42, TxStatus::InProgress));
        assert_eq!(outcomes.normalize(), None);
        table.record_recovered_batch(outcomes);

        assert_eq!(table.status(40), TxStatus::Committed);
        assert_eq!(table.status(41), TxStatus::Aborted);
        assert_eq!(
            table.status(42),
            TxStatus::InProgress,
            "unknown recovery state must remain invisible"
        );
        let (next, _) = table.begin();
        assert!(next > 41, "reused an xid mentioned by recovery");
    }

    #[test]
    fn recovered_outcomes_use_nine_bytes_and_deduplicate_in_place() {
        assert_eq!(std::mem::size_of::<RecoveredOutcome>(), 9);
        let mut outcomes = RecoveredOutcomes::default();
        assert!(outcomes.push(9, TxStatus::Committed));
        assert!(outcomes.push(3, TxStatus::Aborted));
        assert!(outcomes.push(9, TxStatus::Committed));
        assert_eq!(outcomes.normalize(), None);
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes.status(3), Some(TxStatus::Aborted));
        assert_eq!(outcomes.status(9), Some(TxStatus::Committed));
        assert_eq!(outcomes.status(4), None);
    }

    #[test]
    fn contradictory_recovered_outcomes_are_detected() {
        let mut outcomes = RecoveredOutcomes::default();
        assert!(outcomes.push(7, TxStatus::Committed));
        assert!(outcomes.push(7, TxStatus::Aborted));
        assert_eq!(outcomes.normalize(), Some(7));
    }

    #[test]
    fn freezing_reclaims_the_compact_recovered_prefix() {
        let table = TransactionTable::new();
        let mut outcomes = RecoveredOutcomes::default();
        for xid in 1..=128 {
            assert!(outcomes.push(xid, TxStatus::Committed));
        }
        assert_eq!(outcomes.normalize(), None);
        table.record_recovered_batch(outcomes);
        assert_eq!(table.resident_entries(), 128);

        let report = table.freeze_below_bounded(129, 128).unwrap();
        assert_eq!(report.frozen, 128);
        assert_eq!(table.resident_entries(), 0);
        assert_eq!(table.status(64), TxStatus::Committed);
    }

    #[test]
    fn in_progress_lists_only_unfinished_transactions() {
        let table = TransactionTable::new();
        let (a, _) = table.begin();
        let (b, _) = table.begin();
        let (c, _) = table.begin();
        table.commit(a);
        table.abort(c);
        assert_eq!(table.in_progress(), vec![b]);
    }
}
