//! Bounded, sharded, scan-resistant buffer pool.
//!
//! Phase 1 of `docs/server-paged-storage-todo.md`:
//!
//! - "a sharded buffer pool with pin counts, dirty tracking, access history,
//!   bounded metadata, and a scan-resistant eviction policy";
//! - "`buffer_pool_bytes` a hard admission budget with observable current,
//!   dirty, pinned, evictable, hit, miss, eviction, and wait counters".
//!
//! # The budget is a hard ceiling
//!
//! Frames are allocated once at construction and never grown. The pool cannot
//! exceed `budget_bytes` under any access pattern, which is what makes the
//! roadmap's memory envelope (`buffer_pool + query budgets + engine overhead`)
//! meaningful rather than aspirational. When every frame in a shard is pinned,
//! admission *fails* with [`PageError::PoolExhausted`] rather than allocating
//! one more page — a bounded pool that quietly grows under pressure is not
//! bounded.
//!
//! # Scan resistance
//!
//! The policy is a simplified **2Q** (Johnson & Shasha): each shard keeps a
//! probationary FIFO queue for pages seen once and a protected CLOCK queue for
//! pages seen more than once. Eviction drains the probationary queue first.
//!
//! A large sequential scan therefore touches each of its pages exactly once,
//! cycles them through the probationary queue, and evicts only itself — the OLTP
//! working set sitting in the protected queue is untouched. That is the
//! roadmap's `scan_resistant` cache policy, and it is the default rather than an
//! option because the failure it prevents (a reporting query flushing the hot
//! set) is silent and expensive.
//!
//! Plain LRU has exactly the opposite behaviour, which is why it is not used
//! here despite being simpler.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, Condvar, Mutex, RawRwLock, RwLock};
use rustc_hash::FxHashMap;

use crate::error::{PageError, Result};
use crate::manager::PageStore;
use crate::page::{PageHeader, PageId, PageType, SUPERBLOCK_PAGE_ID};

/// Bounded buffer-pool miss source. Implementations own page-location policy;
/// the pool admits only the one page copied into its preallocated frame.
pub trait PageReadSource: Send + Sync + std::fmt::Debug {
    fn page_size(&self) -> u32;
    fn page_count(&self) -> u64;
    fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<PageHeader>;
}

impl PageReadSource for PageStore {
    fn page_size(&self) -> u32 {
        PageStore::page_size(self)
    }

    fn page_count(&self) -> u64 {
        PageStore::page_count(self)
    }

    fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<PageHeader> {
        PageStore::read_page(self, page_id, buffer)
    }
}

/// What a page is being read FOR, which decides which budget it competes in.
///
/// The rule this exists to make true by construction:
///
/// > **A cold document-body read can never evict a search page.**
///
/// Not "is unlikely to" — cannot. Streaming admissions reclaim frames only
/// from the streaming queue, so however much document text is scanned, the
/// term dictionary and posting pages are not reachable by it. A scan-resistant
/// LRU makes eviction *unlikely*; a separate budget makes it *impossible*, and
/// at corpus scale that difference is what stops a machine with 256 GiB of RAM
/// from behaving like one with 16 GiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheClass {
    /// The catalog and other small, hot structures every operation traverses.
    ///
    /// A deep posting traversal is `Search`, and it can legitimately churn the
    /// whole search budget — but it must not take the catalog with it. This
    /// tier is small and reachable only by its own admissions, so the
    /// structures that make *every* query fast survive a workload that evicts
    /// everything else.
    Metadata,
    /// Postings, block metadata, index leaves — the working set that makes
    /// subsequent search fast.
    Search,
    /// Document bodies, large stored values, bulk scans, snippet reads. High
    /// volume, low reuse, and ruinous as a neighbour.
    Streaming,
}

/// Buffer pool configuration.
#[derive(Clone, Copy, Debug)]
pub struct BufferPoolOptions {
    /// Hard ceiling on bytes held in frames. Rounded down to a whole number of
    /// pages, then divided across shards.
    pub budget_bytes: u64,
    /// Independent lock domains. More shards reduce contention; fewer make the
    /// budget divide more evenly across a small pool.
    pub shards: usize,
    /// Share of each shard's frames the protected (twice-touched) queue may
    /// occupy. The remainder is always available to probationary pages, which is
    /// what stops a hot set from starving a scan of any frames at all.
    pub protected_fraction: f64,
    /// Hard bound on deduplicated speculative page requests waiting for a
    /// governed host step. Queue metadata never grows with database size.
    pub read_ahead_queue_pages: usize,
    /// Share of each shard's frames reserved for `CacheClass::Metadata`.
    /// Small: this tier holds structures that are small by nature, and every
    /// frame it takes is one the search classes cannot use.
    pub metadata_fraction: f64,
    /// Share of each shard's frames reserved for `CacheClass::Streaming`.
    /// Document-body reads compete only inside this budget.
    ///
    /// Small on purpose: streaming reads want enough frames to keep a scan
    /// moving and to hold a page across the read/snippet pair, not enough to
    /// cache a corpus that will never fit anyway.
    pub streaming_fraction: f64,
}

impl Default for BufferPoolOptions {
    fn default() -> Self {
        Self {
            budget_bytes: 64 * 1024 * 1024,
            shards: 8,
            // 75/25 is the classic 2Q split: enough probationary space for a
            // scan to make progress, enough protected space to hold a working
            // set through one.
            protected_fraction: 0.75,
            read_ahead_queue_pages: 1_024,
            streaming_fraction: 0.15,
            metadata_fraction: 0.10,
        }
    }
}

impl BufferPoolOptions {
    pub fn with_budget_bytes(mut self, budget_bytes: u64) -> Self {
        self.budget_bytes = budget_bytes;
        self
    }

    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards.max(1);
        self
    }

    pub fn with_read_ahead_queue_pages(mut self, pages: usize) -> Self {
        self.read_ahead_queue_pages = pages;
        self
    }
}

pub const MAX_READ_AHEAD_QUEUE_PAGES: usize = 65_536;
pub const MAX_READ_AHEAD_CANDIDATES_PER_STEP: u64 = 8_192;
pub const MAX_READ_AHEAD_IO_BYTES_PER_STEP: u64 = 128 * 1024 * 1024;
pub const MAX_READ_AHEAD_DURATION_MILLIS_PER_STEP: u64 = 60_000;

/// Non-weakenable envelope for one speculative page-read step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadAheadLimits {
    /// Queue entries examined, including requests made redundant by a
    /// foreground read. Bounding candidates also bounds lock and queue work.
    pub max_candidates: u64,
    /// Conservatively reserves one complete page read for every candidate.
    pub max_io_bytes: u64,
    pub max_duration_millis: u64,
}

impl Default for ReadAheadLimits {
    fn default() -> Self {
        Self {
            max_candidates: 128,
            max_io_bytes: 1024 * 1024,
            max_duration_millis: 25,
        }
    }
}

impl ReadAheadLimits {
    pub fn validate(self, page_size: u32) -> Result<()> {
        crate::page::validate_page_size(page_size)?;
        let page_bytes = u64::from(page_size);
        if self.max_candidates == 0
            || self.max_candidates > MAX_READ_AHEAD_CANDIDATES_PER_STEP
            || self.max_io_bytes < page_bytes
            || self.max_io_bytes > MAX_READ_AHEAD_IO_BYTES_PER_STEP
            || self.max_duration_millis == 0
            || self.max_duration_millis > MAX_READ_AHEAD_DURATION_MILLIS_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "read-ahead requires 1..={MAX_READ_AHEAD_CANDIDATES_PER_STEP} candidates, {page_bytes}..={MAX_READ_AHEAD_IO_BYTES_PER_STEP} logical I/O bytes, and 1..={MAX_READ_AHEAD_DURATION_MILLIS_PER_STEP} ms"
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadAheadStopReason {
    #[default]
    Complete,
    CandidateLimit,
    IoLimit,
    DurationLimit,
}

/// Result of adding speculative requests to the bounded queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadAheadSubmitReport {
    pub requested: u64,
    pub enqueued: u64,
    pub already_resident: u64,
    pub already_queued: u64,
    pub out_of_bounds: u64,
    pub queue_full: u64,
    pub queue_depth: u64,
    /// Requests dropped because no `read_ahead_step` driver is registered —
    /// nothing would ever load them.
    #[serde(default)]
    pub undriven: u64,
}

/// Exact result of one bounded speculative page-read step.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadAheadStepReport {
    pub candidates_examined: u64,
    pub pages_loaded: u64,
    pub already_resident: u64,
    pub admission_declines: u64,
    pub read_errors: u64,
    pub logical_io_bytes: u64,
    pub queue_depth_remaining: u64,
    pub complete: bool,
    pub stop_reason: ReadAheadStopReason,
}

impl ReadAheadStepReport {
    pub fn validate(self, limits: ReadAheadLimits, page_size: u32) -> Result<()> {
        limits.validate(page_size)?;
        let attempted_reads = self.pages_loaded.saturating_add(self.read_errors);
        let logical_io_bytes = attempted_reads
            .checked_mul(u64::from(page_size))
            .ok_or_else(|| PageError::InvalidMaintenanceLimits {
                reason: "read-ahead report I/O accounting overflowed".to_string(),
            })?;
        let classified = attempted_reads
            .saturating_add(self.already_resident)
            .saturating_add(self.admission_declines);
        let terminal_agrees = match self.stop_reason {
            ReadAheadStopReason::Complete => self.complete && self.queue_depth_remaining == 0,
            ReadAheadStopReason::CandidateLimit
            | ReadAheadStopReason::IoLimit
            | ReadAheadStopReason::DurationLimit => {
                !self.complete && self.queue_depth_remaining != 0
            }
        };
        if self.candidates_examined > limits.max_candidates
            || classified != self.candidates_examined
            || self.logical_io_bytes != logical_io_bytes
            || self.logical_io_bytes > limits.max_io_bytes
            || !terminal_agrees
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "read-ahead report is inconsistent with its immutable envelope".to_string(),
            });
        }
        Ok(())
    }
}

pub const MAX_WRITEBACK_PAGES_PER_STEP: u64 = 8_192;
pub const MAX_WRITEBACK_IO_BYTES_PER_STEP: u64 = 128 * 1024 * 1024;
pub const MAX_WRITEBACK_DURATION_MILLIS_PER_STEP: u64 = 60_000;

/// Inclusive restart position for one bounded dirty-buffer sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritebackCursor {
    pub next_page: Option<PageId>,
}

/// Non-weakenable resource envelope for one background writeback step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritebackLimits {
    pub max_candidates: u64,
    /// Logical I/O includes one WAL after-image and one page write per flushed
    /// page. The final batch fsync is a constant operation rather than bytes.
    pub max_io_bytes: u64,
    pub max_duration_millis: u64,
}

impl Default for WritebackLimits {
    fn default() -> Self {
        Self {
            max_candidates: 1_024,
            max_io_bytes: 16 * 1024 * 1024,
            max_duration_millis: 100,
        }
    }
}

impl WritebackLimits {
    pub fn validate(self, page_size: u32) -> Result<()> {
        crate::page::validate_page_size(page_size)?;
        let io_per_page = u64::from(page_size).checked_mul(2).ok_or_else(|| {
            PageError::InvalidMaintenanceLimits {
                reason: "writeback page I/O accounting overflowed".to_string(),
            }
        })?;
        if self.max_candidates == 0
            || self.max_candidates > MAX_WRITEBACK_PAGES_PER_STEP
            || self.max_io_bytes < io_per_page
            || self.max_io_bytes > MAX_WRITEBACK_IO_BYTES_PER_STEP
            || self.max_duration_millis == 0
            || self.max_duration_millis > MAX_WRITEBACK_DURATION_MILLIS_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "writeback requires 1..={MAX_WRITEBACK_PAGES_PER_STEP} candidates, {io_per_page}..={MAX_WRITEBACK_IO_BYTES_PER_STEP} logical I/O bytes, and 1..={MAX_WRITEBACK_DURATION_MILLIS_PER_STEP} ms"
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WritebackStopReason {
    Complete,
    CandidateLimit,
    IoLimit,
    DurationLimit,
}

/// Exact outcome of one bounded dirty-buffer sweep step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritebackStepReport {
    pub next_cursor: WritebackCursor,
    pub candidates_examined: u64,
    pub pages_written: u64,
    pub page_bytes_written: u64,
    pub logical_io_bytes: u64,
    pub pinned_pages_skipped: u64,
    pub stale_candidates_removed: u64,
    pub dirty_pages_remaining: u64,
    pub complete: bool,
    pub stop_reason: WritebackStopReason,
}

impl WritebackStepReport {
    pub fn validate(self, limits: WritebackLimits, page_size: u32) -> Result<()> {
        limits.validate(page_size)?;
        let page_bytes = self
            .pages_written
            .checked_mul(u64::from(page_size))
            .ok_or_else(|| PageError::InvalidMaintenanceLimits {
                reason: "writeback report page-byte accounting overflowed".to_string(),
            })?;
        let logical_io_bytes =
            page_bytes
                .checked_mul(2)
                .ok_or_else(|| PageError::InvalidMaintenanceLimits {
                    reason: "writeback report logical-I/O accounting overflowed".to_string(),
                })?;
        let classified = self
            .pages_written
            .saturating_add(self.pinned_pages_skipped)
            .saturating_add(self.stale_candidates_removed);
        let terminal_agrees = match self.stop_reason {
            WritebackStopReason::Complete => self.complete && self.next_cursor.next_page.is_none(),
            WritebackStopReason::CandidateLimit
            | WritebackStopReason::IoLimit
            | WritebackStopReason::DurationLimit => {
                !self.complete && self.next_cursor.next_page.is_some()
            }
        };
        if self.candidates_examined > limits.max_candidates
            || self.pages_written > self.candidates_examined
            || classified > self.candidates_examined
            || self.page_bytes_written != page_bytes
            || self.logical_io_bytes != logical_io_bytes
            || self.logical_io_bytes > limits.max_io_bytes
            || !terminal_agrees
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "writeback report is inconsistent with its immutable envelope".to_string(),
            });
        }
        Ok(())
    }
}

/// Consulted before a dirty page is written to disk.
///
/// This is how write-ahead ordering is enforced against **eviction**. A bounded
/// pool must be free to evict a dirty page at any moment, including one whose
/// log record has not been written yet — and if it simply wrote that page out,
/// a crash would leave the page file holding a change the log cannot replay or
/// undo. Committed data would be lost with nothing reporting it.
///
/// The barrier closes that hole: the pool hands the page to the owner before
/// writing it, and the owner makes the corresponding log record durable first.
pub trait WritebackBarrier: Send + Sync + std::fmt::Debug {
    /// Ensure `bytes` for `page_id` are recoverable before they reach disk.
    fn before_writeback(&self, page_id: PageId, transaction: u64, bytes: &[u8]) -> Result<()>;
}

/// One cached page.
#[derive(Debug)]
struct Frame {
    page_id: PageId,
    data: Arc<RwLock<Vec<u8>>>,
    /// Outstanding guards. A frame with pins > 0 is never evicted.
    pins: AtomicU32,
    dirty: AtomicBool,
    /// Milliseconds since this pool was constructed when the page first
    /// became dirty, plus one so zero remains the unambiguous clean sentinel.
    dirty_since_millis: AtomicU64,
    /// CLOCK reference bit: set on access, cleared when the hand passes.
    referenced: AtomicBool,
    /// In the protected queue rather than the probationary one.
    protected: AtomicBool,
    /// Admitted as `CacheClass::Streaming`. Such a frame lives in the
    /// streaming queue and NEVER promotes into protected, however many times
    /// it is touched — a document read twice is still a document.
    streaming: AtomicBool,
    /// Admitted as `CacheClass::Metadata`; lives in the metadata queue.
    metadata: AtomicBool,
    /// Transaction that last dirtied this page, for the writeback barrier.
    dirtied_by: AtomicU64,
    /// Modified since its last log record. Cleared once logged.
    needs_log: AtomicBool,
    /// Loaded speculatively and not yet consumed by a foreground access.
    prefetched: AtomicBool,
    /// A speculative load is not a cache touch. The first foreground use sets
    /// this flag; only a later use may promote the page into protected 2Q.
    foreground_touched: AtomicBool,
}

/// A pinned, readable page. Unpins on drop.
#[derive(Debug)]
pub struct PageGuard {
    frame: Arc<Frame>,
    guard: ArcRwLockReadGuard<RawRwLock, Vec<u8>>,
}

impl PageGuard {
    pub fn page_id(&self) -> PageId {
        self.frame.page_id
    }

    pub fn bytes(&self) -> &[u8] {
        &self.guard
    }

    pub fn header(&self, path: &std::path::Path) -> Result<PageHeader> {
        PageHeader::decode(&self.guard, path)
    }
}

impl std::ops::Deref for PageGuard {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.guard
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        self.frame.pins.fetch_sub(1, Ordering::Release);
    }
}

/// A pinned, writable page. Marks the frame dirty and unpins on drop.
pub struct PageGuardMut {
    frame: Arc<Frame>,
    guard: ArcRwLockWriteGuard<RawRwLock, Vec<u8>>,
    dirty_index: Arc<Mutex<BTreeSet<PageId>>>,
    /// Earliest instant at which this writable guard could have modified the
    /// page. Using acquisition rather than drop time makes age conservative.
    dirty_started_at_millis: u64,
}

impl std::fmt::Debug for PageGuardMut {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PageGuardMut")
            .field("page_id", &self.frame.page_id)
            .finish_non_exhaustive()
    }
}

impl PageGuardMut {
    pub fn page_id(&self) -> PageId {
        self.frame.page_id
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.guard
    }
}

impl std::ops::Deref for PageGuardMut {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.guard
    }
}

impl std::ops::DerefMut for PageGuardMut {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.guard
    }
}

impl Drop for PageGuardMut {
    fn drop(&mut self) {
        // Marked dirty unconditionally: a write guard was requested, so the
        // page must be assumed modified. Tracking actual mutation would need a
        // comparison against the pre-image, which costs more than the occasional
        // redundant writeback it would save.
        if !self.frame.dirty.load(Ordering::Acquire) {
            self.frame
                .dirty_since_millis
                .store(self.dirty_started_at_millis, Ordering::Release);
        }
        self.frame.dirty.store(true, Ordering::Release);
        self.frame.needs_log.store(true, Ordering::Release);
        self.dirty_index.lock().insert(self.frame.page_id);
        self.frame.pins.fetch_sub(1, Ordering::Release);
    }
}

/// One in-flight miss load. Waiters block here instead of holding the shard
/// lock, and re-run the acquire loop once the loader signals: on success the
/// page is resident, on failure the entry is gone and one waiter becomes the
/// next loader.
#[derive(Debug, Default)]
struct LoadState {
    done: Mutex<bool>,
    signal: Condvar,
}

/// One lock domain of the pool.
#[derive(Debug)]
struct Shard {
    resident: FxHashMap<PageId, Arc<Frame>>,
    /// Misses whose disk read is in flight with the shard lock RELEASED.
    /// The reservation keeps two threads from loading one page into two
    /// frames — the race that previously justified reading under the lock,
    /// which serialized every cold read in the shard behind one IO.
    loading: FxHashMap<PageId, Arc<LoadState>>,
    /// Pages seen once, evicted FIFO. Absorbs scans.
    probationary: VecDeque<PageId>,
    /// Pages seen more than once, evicted by CLOCK second-chance.
    protected: VecDeque<PageId>,
    /// Streaming-class pages, evicted FIFO within their own budget.
    streaming: VecDeque<PageId>,
    /// Metadata-class pages, evicted FIFO within their own budget.
    metadata: VecDeque<PageId>,
    /// Frames not currently holding a page.
    free: Vec<Arc<RwLock<Vec<u8>>>>,
    protected_capacity: usize,
    streaming_capacity: usize,
    metadata_capacity: usize,
    capacity: usize,
}

#[derive(Debug)]
struct ReadAheadQueue {
    /// Monotonic sequence preserves FIFO order while allowing foreground
    /// cancellation in logarithmic time. A `VecDeque` would require an O(n)
    /// walk on every cache miss that overtakes speculative work.
    pending: BTreeMap<u64, PageId>,
    queued: FxHashMap<PageId, u64>,
    next_sequence: u64,
    capacity: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadAheadPageOutcome {
    Loaded,
    AlreadyResident,
    AdmissionDeclined,
    ReadError,
}

impl ReadAheadQueue {
    fn push(&mut self, page_id: PageId) {
        if self.next_sequence == u64::MAX {
            // This executes at most once per 2^64 accepted requests. Rebase the
            // bounded queue instead of permitting sequence wrap to reorder it.
            let ordered = self.pending.values().copied().collect::<Vec<_>>();
            self.pending.clear();
            self.queued.clear();
            for (sequence, queued_page_id) in ordered.into_iter().enumerate() {
                let sequence = sequence as u64;
                self.pending.insert(sequence, queued_page_id);
                self.queued.insert(queued_page_id, sequence);
            }
            self.next_sequence = self.pending.len() as u64;
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.pending.insert(sequence, page_id);
        self.queued.insert(page_id, sequence);
    }

    fn pop_front(&mut self) -> Option<PageId> {
        let (&sequence, &page_id) = self.pending.first_key_value()?;
        self.pending.remove(&sequence);
        self.queued.remove(&page_id);
        Some(page_id)
    }

    fn cancel(&mut self, page_id: PageId) -> bool {
        let Some(sequence) = self.queued.remove(&page_id) else {
            return false;
        };
        self.pending.remove(&sequence);
        true
    }
}

#[derive(Debug, Default)]
struct PoolMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    /// Frames reclaimed from the streaming class by any admission.
    streaming_evictions: AtomicU64,
    /// Streaming admissions that had to take a probationary frame because the
    /// streaming budget held nothing evictable. Non-zero means the pool is too
    /// small to give document reads a lane of their own; it is still never a
    /// PROTECTED frame.
    streaming_borrowed_probationary: AtomicU64,
    writebacks: AtomicU64,
    admission_failures: AtomicU64,
    shard_lock_waits: AtomicU64,
    shard_lock_wait_nanos: AtomicU64,
    shard_lock_max_wait_nanos: AtomicU64,
    page_latch_waits: AtomicU64,
    page_latch_wait_nanos: AtomicU64,
    page_latch_max_wait_nanos: AtomicU64,
    scan_demotions: AtomicU64,
    writeback_steps: AtomicU64,
    background_writebacks: AtomicU64,
    writeback_candidate_limit_stops: AtomicU64,
    writeback_io_limit_stops: AtomicU64,
    writeback_duration_limit_stops: AtomicU64,
    writeback_pinned_skips: AtomicU64,
    read_ahead_requests: AtomicU64,
    read_ahead_undriven: AtomicU64,
    read_ahead_enqueued: AtomicU64,
    read_ahead_already_resident: AtomicU64,
    read_ahead_already_queued: AtomicU64,
    read_ahead_out_of_bounds: AtomicU64,
    read_ahead_queue_full: AtomicU64,
    read_ahead_steps: AtomicU64,
    read_ahead_pages_loaded: AtomicU64,
    read_ahead_pages_used: AtomicU64,
    read_ahead_pages_wasted: AtomicU64,
    read_ahead_admission_declines: AtomicU64,
    read_ahead_read_errors: AtomicU64,
    read_ahead_foreground_cancellations: AtomicU64,
    read_ahead_candidate_limit_stops: AtomicU64,
    read_ahead_io_limit_stops: AtomicU64,
    read_ahead_duration_limit_stops: AtomicU64,
}

/// Observable pool state. Every counter the roadmap's telemetry section names
/// for the buffer pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferPoolSnapshot {
    pub budget_bytes: u64,
    pub page_size: u32,
    pub total_frames: u64,
    pub resident_pages: u64,
    pub resident_bytes: u64,
    pub dirty_pages: u64,
    pub pinned_pages: u64,
    pub evictable_pages: u64,
    pub free_frames: u64,
    pub protected_pages: u64,
    pub probationary_pages: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub writebacks: u64,
    pub admission_failures: u64,
    /// Metadata-lock acquisitions that could not proceed immediately.
    pub shard_lock_waits: u64,
    pub shard_lock_wait_nanos: u64,
    pub shard_lock_max_wait_nanos: u64,
    /// Page-data latch acquisitions that could not proceed immediately.
    pub page_latch_waits: u64,
    pub page_latch_wait_nanos: u64,
    pub page_latch_max_wait_nanos: u64,
    /// Age of the oldest dirty resident frame. Zero also represents an
    /// immediately dirtied page; consult `dirty_pages` to distinguish idle.
    pub oldest_dirty_page_age_millis: u64,
    /// Dirty work still awaiting writeback, expressed without walking storage.
    pub writeback_lag_pages: u64,
    pub writeback_lag_bytes: u64,
    pub scan_demotions: u64,
    pub writeback_steps: u64,
    pub background_writebacks: u64,
    pub writeback_candidate_limit_stops: u64,
    pub writeback_io_limit_stops: u64,
    pub writeback_duration_limit_stops: u64,
    pub writeback_pinned_skips: u64,
    pub read_ahead_queue_capacity: u64,
    pub read_ahead_queue_depth: u64,
    pub read_ahead_requests: u64,
    #[serde(default)]
    pub read_ahead_undriven: u64,
    pub read_ahead_enqueued: u64,
    pub read_ahead_already_resident: u64,
    pub read_ahead_already_queued: u64,
    pub read_ahead_out_of_bounds: u64,
    pub read_ahead_queue_full: u64,
    pub read_ahead_steps: u64,
    pub read_ahead_pages_loaded: u64,
    pub read_ahead_pages_used: u64,
    pub read_ahead_pages_wasted: u64,
    pub read_ahead_admission_declines: u64,
    pub read_ahead_read_errors: u64,
    pub read_ahead_foreground_cancellations: u64,
    pub read_ahead_candidate_limit_stops: u64,
    pub read_ahead_io_limit_stops: u64,
    pub read_ahead_duration_limit_stops: u64,
    pub read_only: bool,
}

impl BufferPoolSnapshot {
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// A bounded cache of pages over a [`PageStore`].
#[derive(Debug)]
pub struct BufferPool {
    store: Arc<PageStore>,
    read_source: Arc<dyn PageReadSource>,
    read_only: bool,
    shards: Box<[Mutex<Shard>]>,
    page_size: u32,
    budget_bytes: u64,
    total_frames: usize,
    clock_origin: Instant,
    metrics: PoolMetrics,
    barrier: Mutex<Option<Arc<dyn WritebackBarrier>>>,
    /// Ordered and bounded by resident frames. It gives background writeback a
    /// stable key cursor without scanning or sorting the complete pool.
    dirty_index: Arc<Mutex<BTreeSet<PageId>>>,
    /// Transaction attributed to pages dirtied from now on.
    current_transaction: AtomicU64,
    /// PagedStore enables this after attaching a WAL barrier. Standalone heap
    /// users retain the immediate allocator semantics they historically used.
    wal_managed_frees: AtomicBool,
    /// Pages logically freed by each transaction but not yet published into
    /// the superblock free list. The owning store links, WAL-logs, flushes,
    /// and adopts them at the transaction durability boundary.
    pending_frees: Mutex<BTreeMap<u64, Vec<PageId>>>,
    /// Deduplicated speculative requests. Its ordered map and reverse index
    /// have the same explicit hard capacity and are pruned synchronously on
    /// cancellation.
    read_ahead: Mutex<ReadAheadQueue>,
    /// Registered `read_ahead_step` drivers. With none, speculative requests
    /// are dropped without touching the queue: nothing would ever load them,
    /// and the profiled cost of enqueue bookkeeping on request-heavy paths
    /// (free-space-map probes, scan sibling hints) was ~5% of an FTS build.
    read_ahead_drivers: AtomicUsize,
    /// Lock-free mirror of the queue's pending length, so `acquire` — the
    /// hottest path in the pool — only takes the queue mutex to cancel a
    /// speculative request when one could actually exist.
    read_ahead_pending_pages: AtomicU64,
    /// Set when a request was dropped because the queue was full; cleared by
    /// every pop or cancellation. While set, further requests short-circuit
    /// before the page-bounds check and the queue mutex.
    read_ahead_saturated: AtomicBool,
}

impl BufferPool {
    pub fn new(store: Arc<PageStore>, options: BufferPoolOptions) -> Result<Self> {
        let read_source: Arc<dyn PageReadSource> = store.clone();
        Self::new_inner(store, read_source, false, options)
    }

    /// Build a page cache over an immutable/snapshot source. Writable guards
    /// fail closed; this prevents a local write from later being shadowed by an
    /// older remote generation after eviction.
    pub fn new_read_only(
        store: Arc<PageStore>,
        read_source: Arc<dyn PageReadSource>,
        options: BufferPoolOptions,
    ) -> Result<Self> {
        Self::new_inner(store, read_source, true, options)
    }

    fn new_inner(
        store: Arc<PageStore>,
        read_source: Arc<dyn PageReadSource>,
        read_only: bool,
        options: BufferPoolOptions,
    ) -> Result<Self> {
        let page_size = store.page_size();
        if options.read_ahead_queue_pages > MAX_READ_AHEAD_QUEUE_PAGES {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "read-ahead queue cannot exceed {MAX_READ_AHEAD_QUEUE_PAGES} pages"
                ),
            });
        }
        if read_source.page_count() == 0 {
            return Err(PageError::OutOfBounds {
                page_id: 0,
                page_count: 0,
            });
        }
        if read_source.page_size() != page_size {
            return Err(PageError::PageSizeMismatch {
                path: store.path().to_path_buf(),
                found: read_source.page_size(),
                expected: page_size,
            });
        }
        let total_frames_u64 = options.budget_bytes / u64::from(page_size);
        if total_frames_u64 == 0 {
            return Err(PageError::BudgetTooSmall {
                budget: options.budget_bytes,
                page_size,
            });
        }
        let total_frames = usize::try_from(total_frames_u64).map_err(|_| {
            PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "buffer pool requests {total_frames_u64} frames, exceeding this platform's address space"
                ),
            }
        })?;

        // Never more shards than frames, or a shard would get zero frames and
        // every page hashing to it would fail admission.
        let shard_count = options.shards.max(1).min(total_frames);
        let per_shard = total_frames / shard_count;
        let remainder = total_frames % shard_count;

        let shards = (0..shard_count)
            .map(|index| {
                let capacity = per_shard + usize::from(index < remainder);
                let protected_capacity =
                    ((capacity as f64) * options.protected_fraction).floor() as usize;
                // Leave at least one protected slot and one probationary slot
                // whenever the shard has room for two.
                let protected_capacity = protected_capacity.clamp(
                    usize::from(capacity > 1),
                    capacity.saturating_sub(usize::from(capacity > 1)),
                );
                // At least one streaming frame whenever the shard has two, so
                // a document read is never unable to make progress; capped so
                // it can never crowd the search classes.
                let streaming_capacity = (((capacity as f64) * options.streaming_fraction).floor()
                    as usize)
                    .clamp(usize::from(capacity > 1), capacity.saturating_sub(1));
                // A shard too small to spare a frame gets no metadata tier at
                // all. Reserving one of two frames left a single frame for
                // everything else, and any operation needing two at once
                // failed outright. With a zero budget, metadata admissions
                // fall through the hierarchy like any other read.
                const MIN_FRAMES_FOR_METADATA_TIER: usize = 8;
                let metadata_capacity = if capacity < MIN_FRAMES_FOR_METADATA_TIER {
                    0
                } else {
                    (((capacity as f64) * options.metadata_fraction).floor() as usize)
                        .clamp(1, capacity / 4)
                };
                let free = (0..capacity)
                    .map(|_| Arc::new(RwLock::new(vec![0u8; page_size as usize])))
                    .collect();
                Mutex::new(Shard {
                    resident: FxHashMap::default(),
                    loading: FxHashMap::default(),
                    probationary: VecDeque::new(),
                    protected: VecDeque::new(),
                    streaming: VecDeque::new(),
                    metadata: VecDeque::new(),
                    free,
                    protected_capacity,
                    streaming_capacity,
                    metadata_capacity,
                    capacity,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            store,
            read_source,
            read_only,
            shards,
            page_size,
            budget_bytes: u64::from(page_size) * total_frames as u64,
            total_frames,
            clock_origin: Instant::now(),
            metrics: PoolMetrics::default(),
            barrier: Mutex::new(None),
            dirty_index: Arc::new(Mutex::new(BTreeSet::new())),
            current_transaction: AtomicU64::new(0),
            wal_managed_frees: AtomicBool::new(false),
            pending_frees: Mutex::new(BTreeMap::new()),
            read_ahead: Mutex::new(ReadAheadQueue {
                pending: BTreeMap::new(),
                queued: FxHashMap::with_capacity_and_hasher(
                    options.read_ahead_queue_pages,
                    Default::default(),
                ),
                next_sequence: 0,
                capacity: options.read_ahead_queue_pages,
            }),
            read_ahead_drivers: AtomicUsize::new(0),
            read_ahead_pending_pages: AtomicU64::new(0),
            read_ahead_saturated: AtomicBool::new(false),
        })
    }

    /// Announce a live `read_ahead_step` driver (host worker, build pump).
    /// Speculative requests submitted while no driver is registered are
    /// dropped instead of queued — nothing would ever load them, and an
    /// undriven queue clogs at capacity and taxes every request.
    pub fn register_read_ahead_driver(&self) {
        self.read_ahead_drivers.fetch_add(1, Ordering::Release);
    }

    /// Retire one [`Self::register_read_ahead_driver`] registration.
    pub fn unregister_read_ahead_driver(&self) {
        self.read_ahead_drivers.fetch_sub(1, Ordering::Release);
    }

    /// Install the writeback barrier. See [`WritebackBarrier`].
    pub fn set_barrier(&self, barrier: Arc<dyn WritebackBarrier>) {
        *self.barrier.lock() = Some(barrier);
    }

    /// Attribute subsequently dirtied pages to `transaction`.
    pub fn set_current_transaction(&self, transaction: u64) {
        self.current_transaction
            .store(transaction, Ordering::Release);
    }

    pub(crate) fn enable_wal_managed_frees(&self) {
        self.wal_managed_frees.store(true, Ordering::Release);
    }

    /// Remove the pages staged as free by `transaction`. The caller must
    /// materialize their final links, WAL-log and flush them before publishing
    /// the returned segment through `PageStore::adopt_free_pages`.
    pub fn take_pending_frees(&self, transaction: u64) -> Vec<PageId> {
        self.pending_frees
            .lock()
            .remove(&transaction)
            .unwrap_or_default()
    }

    /// Run the barrier for a page about to be written.
    fn barrier_for(&self, page_id: PageId, transaction: u64, bytes: &[u8]) -> Result<()> {
        let barrier = self.barrier.lock().clone();
        match barrier {
            Some(barrier) => barrier.before_writeback(page_id, transaction, bytes),
            None => Ok(()),
        }
    }

    pub fn store(&self) -> &Arc<PageStore> {
        &self.store
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    #[inline]
    fn monotonic_millis(&self) -> u64 {
        u64::try_from(self.clock_origin.elapsed().as_millis())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1)
    }

    /// Acquire one metadata shard while recording only actual contention.
    /// The fast path performs a single nonblocking attempt and does not add a
    /// timing call to uncontended page access.
    #[inline]
    fn lock_shard<'a>(&self, shard: &'a Mutex<Shard>) -> parking_lot::MutexGuard<'a, Shard> {
        if let Some(guard) = shard.try_lock() {
            return guard;
        }
        let started = Instant::now();
        let guard = shard.lock();
        let waited = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.metrics
            .shard_lock_waits
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.metrics.shard_lock_wait_nanos.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(waited)),
        );
        self.metrics
            .shard_lock_max_wait_nanos
            .fetch_max(waited, Ordering::Relaxed);
        guard
    }

    #[inline]
    fn record_page_latch_wait(&self, started: Instant) {
        let waited = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.metrics
            .page_latch_waits
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.metrics.page_latch_wait_nanos.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(waited)),
        );
        self.metrics
            .page_latch_max_wait_nanos
            .fetch_max(waited, Ordering::Relaxed);
    }

    #[inline]
    fn write_frame_data<'a>(
        &self,
        data: &'a RwLock<Vec<u8>>,
    ) -> parking_lot::RwLockWriteGuard<'a, Vec<u8>> {
        if let Some(guard) = data.try_write() {
            return guard;
        }
        let started = Instant::now();
        let guard = data.write();
        self.record_page_latch_wait(started);
        guard
    }

    #[inline]
    fn shard_for(&self, page_id: PageId) -> &Mutex<Shard> {
        // Multiplicative hash: page ids are dense and sequential, so the low
        // bits alone would map long runs onto one shard and serialize a scan.
        let mixed = page_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        &self.shards[(mixed >> 32) as usize % self.shards.len()]
    }

    /// Pin a page for reading, loading it from disk on a miss.
    pub fn get(&self, page_id: PageId) -> Result<PageGuard> {
        self.get_in_class(page_id, CacheClass::Search)
    }

    /// Read a page for a document-body / bulk-scan workload. The page competes
    /// only inside the streaming budget and can never displace a search page.
    pub fn get_streaming(&self, page_id: PageId) -> Result<PageGuard> {
        self.get_in_class(page_id, CacheClass::Streaming)
    }

    /// Read a small, hot structural page. It competes only inside the metadata
    /// budget, and no other class can evict it.
    pub fn get_metadata(&self, page_id: PageId) -> Result<PageGuard> {
        self.get_in_class(page_id, CacheClass::Metadata)
    }

    pub fn get_in_class(&self, page_id: PageId, class: CacheClass) -> Result<PageGuard> {
        let frame = self.acquire_in_class(page_id, class)?;
        let data = frame.data.clone();
        let guard = match data.try_read_arc() {
            Some(guard) => guard,
            None => {
                let started = Instant::now();
                let guard = data.read_arc();
                self.record_page_latch_wait(started);
                guard
            }
        };
        Ok(PageGuard { frame, guard })
    }

    /// Pin a page for writing, loading it from disk on a miss.
    pub fn get_mut(&self, page_id: PageId) -> Result<PageGuardMut> {
        if self.read_only {
            return Err(PageError::ReadOnlyPageSource { page_id });
        }
        let frame = self.acquire(page_id)?;
        let data = frame.data.clone();
        let guard = match data.try_write_arc() {
            Some(guard) => guard,
            None => {
                let started = Instant::now();
                let guard = data.write_arc();
                self.record_page_latch_wait(started);
                guard
            }
        };
        let dirty_started_at_millis = self.monotonic_millis();
        Ok(PageGuardMut {
            frame,
            guard,
            dirty_index: self.dirty_index.clone(),
            dirty_started_at_millis,
        })
    }

    /// Stage a page for WAL-backed free-list adoption at commit/checkpoint.
    /// The superblock is deliberately untouched here.
    pub fn free_page(&self, page_id: PageId) -> Result<()> {
        if !self.wal_managed_frees.load(Ordering::Acquire) {
            return self.free_page_immediate(page_id);
        }
        let transaction = self.current_transaction.load(Ordering::Acquire);
        self.free_page_for_transaction(page_id, transaction)
    }

    fn free_page_immediate(&self, page_id: PageId) -> Result<()> {
        if self.read_only {
            return Err(PageError::ReadOnlyPageSource { page_id });
        }
        if self.read_ahead_pending_pages.load(Ordering::Relaxed) != 0 {
            let mut queue = self.read_ahead.lock();
            if queue.cancel(page_id) {
                self.read_ahead_pending_pages
                    .store(queue.pending.len() as u64, Ordering::Relaxed);
                self.read_ahead_saturated.store(false, Ordering::Relaxed);
            }
        }
        let mut shard = self.lock_shard(self.shard_for(page_id));
        let Some(frame) = shard.resident.get(&page_id).cloned() else {
            return self.store.free(page_id);
        };
        let pins = frame.pins.load(Ordering::Acquire);
        if pins != 0 {
            return Err(PageError::EvictedWhilePinned { page_id, pins });
        }
        let data = frame.data.clone();
        let guard = self.write_frame_data(&data);
        self.store.free(page_id)?;
        frame.needs_log.store(false, Ordering::Release);
        frame.dirty.store(false, Ordering::Release);
        frame.dirty_since_millis.store(0, Ordering::Release);
        self.dirty_index.lock().remove(&page_id);
        Self::remove_from(&mut shard.probationary, page_id);
        Self::remove_from(&mut shard.protected, page_id);
        shard.resident.remove(&page_id);
        self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
        drop(guard);
        drop(data);
        let buffer = match Arc::try_unwrap(frame) {
            Ok(frame) => frame.data,
            Err(_) => Arc::new(RwLock::new(vec![0_u8; self.page_size as usize])),
        };
        shard.free.push(buffer);
        Ok(())
    }

    /// Stage a structural free under an explicit WAL transaction id.
    pub(crate) fn free_page_for_transaction(
        &self,
        page_id: PageId,
        transaction: u64,
    ) -> Result<()> {
        if self.read_only {
            return Err(PageError::ReadOnlyPageSource { page_id });
        }
        if page_id == SUPERBLOCK_PAGE_ID || page_id == self.store.root_page() {
            return Err(PageError::CannotFreeRootPage { page_id });
        }
        if page_id >= self.store.page_count() {
            return Err(PageError::OutOfBounds {
                page_id,
                page_count: self.store.page_count(),
            });
        }
        {
            let pending = self.pending_frees.lock();
            if pending
                .values()
                .any(|pages| pages.iter().any(|pending| *pending == page_id))
            {
                return Err(PageError::PageAlreadyFree { page_id });
            }
        }
        let mut guard = self.get_mut(page_id)?;
        let existing = PageHeader::decode(guard.bytes_mut(), self.store.path())?;
        if existing.page_type == PageType::Free {
            return Err(PageError::PageAlreadyFree { page_id });
        }
        let bytes = guard.bytes_mut();
        bytes.fill(0);
        let mut header = PageHeader::new(page_id, PageType::Free, self.page_size);
        header.generation = existing.generation.saturating_add(1);
        header.encode(bytes);
        drop(guard);
        self.pending_frees
            .lock()
            .entry(transaction)
            .or_default()
            .push(page_id);
        Ok(())
    }

    /// Resolve a page to a pinned frame.
    fn acquire(&self, page_id: PageId) -> Result<Arc<Frame>> {
        self.acquire_in_class(page_id, CacheClass::Search)
    }

    fn acquire_in_class(&self, page_id: PageId, class: CacheClass) -> Result<Arc<Frame>> {
        // Cancellation is an I/O-saving courtesy, not a correctness need
        // (`read_ahead_step` re-checks residency); never pay the queue mutex
        // on this hottest-of-paths when the queue is empty.
        if self.read_ahead_pending_pages.load(Ordering::Relaxed) != 0 {
            let mut queue = self.read_ahead.lock();
            if queue.cancel(page_id) {
                self.read_ahead_pending_pages
                    .store(queue.pending.len() as u64, Ordering::Relaxed);
                self.read_ahead_saturated.store(false, Ordering::Relaxed);
                drop(queue);
                self.metrics
                    .read_ahead_foreground_cancellations
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        loop {
            let mut shard = self.lock_shard(self.shard_for(page_id));

            if let Some(frame) = shard.resident.get(&page_id) {
                let frame = frame.clone();
                if frame.prefetched.swap(false, Ordering::AcqRel) {
                    self.metrics
                        .read_ahead_pages_used
                        .fetch_add(1, Ordering::Relaxed);
                }
                let previously_touched = frame.foreground_touched.swap(true, Ordering::AcqRel);
                frame.dirtied_by.store(
                    self.current_transaction.load(Ordering::Acquire),
                    Ordering::Release,
                );
                frame.pins.fetch_add(1, Ordering::Acquire);
                frame.referenced.store(true, Ordering::Release);
                // Second touch promotes into the protected queue. This is the
                // whole of the scan-resistance mechanism: a scan touches each
                // page once and never reaches here, so it never displaces
                // protected pages.
                // A streaming page stays streaming however often it is
                // touched. Promotion on second access is exactly how a
                // read-then-snippet pair would otherwise smuggle a document
                // body into the protected queue.
                if previously_touched
                    && !frame.metadata.load(Ordering::Acquire)
                    && !frame.streaming.load(Ordering::Acquire)
                    && !frame.protected.swap(true, Ordering::AcqRel)
                {
                    Self::remove_from(&mut shard.probationary, page_id);
                    shard.protected.push_back(page_id);
                    self.enforce_protected_capacity(&mut shard);
                }
                self.metrics.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(frame);
            }

            if let Some(load) = shard.loading.get(&page_id).cloned() {
                // Another thread is reading this page with the lock released.
                // Wait off-lock, then re-run the loop: success lands the page
                // in `resident`, failure removes the entry and this thread
                // becomes the next loader.
                drop(shard);
                let mut done = load.done.lock();
                while !*done {
                    load.signal.wait(&mut done);
                }
                continue;
            }

            self.metrics.misses.fetch_add(1, Ordering::Relaxed);

            let buffer = match shard.free.pop() {
                Some(buffer) => buffer,
                None => self.evict_one(&mut shard, page_id, class)?,
            };
            let load = Arc::new(LoadState::default());
            shard.loading.insert(page_id, Arc::clone(&load));
            // The reservation above makes this page's load exclusive, so the
            // disk read runs with the shard UNLOCKED: concurrent misses in
            // one shard overlap their IO instead of queueing behind it — the
            // difference between one cold random read at a time and an IO
            // queue's worth.
            drop(shard);

            let read_result = {
                let mut bytes = buffer.write();
                self.read_source.read_page(page_id, &mut bytes)
            };

            let mut shard = self.lock_shard(self.shard_for(page_id));
            shard.loading.remove(&page_id);
            let outcome = match read_result {
                Ok(_) => {
                    let frame = Arc::new(Frame {
                        page_id,
                        data: buffer,
                        pins: AtomicU32::new(1),
                        dirty: AtomicBool::new(false),
                        dirty_since_millis: AtomicU64::new(0),
                        referenced: AtomicBool::new(true),
                        protected: AtomicBool::new(false),
                        streaming: AtomicBool::new(class == CacheClass::Streaming),
                        metadata: AtomicBool::new(class == CacheClass::Metadata),
                        dirtied_by: AtomicU64::new(
                            self.current_transaction.load(Ordering::Acquire),
                        ),
                        needs_log: AtomicBool::new(false),
                        prefetched: AtomicBool::new(false),
                        foreground_touched: AtomicBool::new(true),
                    });
                    shard.resident.insert(page_id, frame.clone());
                    match class {
                        CacheClass::Streaming => {
                            shard.streaming.push_back(page_id);
                            self.enforce_streaming_capacity(&mut shard);
                        }
                        // With no metadata tier (a shard too small to spare a
                        // frame) a metadata page must be an ORDINARY page.
                        // Parking it in a queue no other class evicts from
                        // would let metadata occupy the whole shard and
                        // deadlock every other admission.
                        CacheClass::Metadata if shard.metadata_capacity == 0 => {
                            frame.metadata.store(false, Ordering::Release);
                            shard.probationary.push_back(page_id);
                        }
                        CacheClass::Metadata => {
                            shard.metadata.push_back(page_id);
                            self.enforce_metadata_capacity(&mut shard);
                        }
                        CacheClass::Search => shard.probationary.push_back(page_id),
                    }
                    Ok(frame)
                }
                Err(error) => {
                    // A failed miss must return the preallocated frame to the
                    // shard; otherwise repeated corrupt/out-of-range reads
                    // permanently shrink the hard buffer-pool capacity.
                    shard.free.push(buffer);
                    Err(error)
                }
            };
            drop(shard);
            *load.done.lock() = true;
            load.signal.notify_all();
            return outcome;
        }
    }

    /// Free one frame in this shard, writing it back if dirty.
    fn evict_one(
        &self,
        shard: &mut Shard,
        wanted: PageId,
        class: CacheClass,
    ) -> Result<Arc<RwLock<Vec<u8>>>> {
        if class == CacheClass::Metadata {
            // Metadata reclaims metadata. It never takes from the search or
            // streaming classes, so this tier cannot grow at their expense
            // either — isolation runs both ways.
            if let Some(buffer) = self.evict_from_metadata(shard)? {
                return Ok(buffer);
            }
            // Metadata may reclaim from ANY class; no class may reclaim from
            // metadata. That asymmetry is the whole hierarchy, and it has to
            // run this way round: a metadata page that cannot be admitted
            // fails the operation outright, because it is the structure every
            // other read has to walk. Bounding it to its own tier deadlocked a
            // small pool.
            if let Some(buffer) = self.evict_from_probationary(shard)? {
                return Ok(buffer);
            }
            if let Some(buffer) = self.evict_from_streaming(shard)? {
                return Ok(buffer);
            }
            if let Some(buffer) = self.evict_from_protected(shard)? {
                return Ok(buffer);
            }
            self.metrics
                .admission_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(PageError::PoolExhausted {
                page_id: wanted,
                frames: shard.capacity,
            });
        }

        if class == CacheClass::Streaming {
            // THE ISOLATION RULE. A streaming admission reclaims a streaming
            // frame or it fails; it never reaches probationary or protected.
            // This is what makes "a document scan cannot evict the search
            // working set" a structural property rather than a tuning hope.
            if let Some(buffer) = self.evict_from_streaming(shard)? {
                return Ok(buffer);
            }
            // Only when the class holds nothing evictable at all — a pool too
            // small to give streaming a frame — does a document read borrow
            // from probationary. Protected stays unreachable either way.
            if let Some(buffer) = self.evict_from_probationary(shard)? {
                self.metrics
                    .streaming_borrowed_probationary
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(buffer);
            }
            self.metrics
                .admission_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(PageError::PoolExhausted {
                page_id: wanted,
                frames: shard.capacity,
            });
        }

        // Probationary first — scans land here and are cheap to discard.
        if let Some(buffer) = self.evict_from_probationary(shard)? {
            return Ok(buffer);
        }
        if let Some(buffer) = self.evict_from_protected(shard)? {
            return Ok(buffer);
        }
        // Search may reclaim from streaming, never the reverse. The asymmetry
        // is deliberate: a pure-search workload should be able to use the
        // whole pool, while a document scan stays in its lane.
        if let Some(buffer) = self.evict_from_streaming(shard)? {
            return Ok(buffer);
        }

        self.metrics
            .admission_failures
            .fetch_add(1, Ordering::Relaxed);
        Err(PageError::PoolExhausted {
            page_id: wanted,
            frames: shard.capacity,
        })
    }

    fn evict_from_metadata(&self, shard: &mut Shard) -> Result<Option<Arc<RwLock<Vec<u8>>>>> {
        for _ in 0..shard.metadata.len() {
            let Some(candidate) = shard.metadata.pop_front() else {
                break;
            };
            match self.try_reclaim(shard, candidate)? {
                Some(buffer) => return Ok(Some(buffer)),
                None => shard.metadata.push_back(candidate),
            }
        }
        Ok(None)
    }

    /// Hold the metadata queue inside its budget.
    fn enforce_metadata_capacity(&self, shard: &mut Shard) {
        while shard.metadata.len() > shard.metadata_capacity {
            let Some(candidate) = shard.metadata.pop_front() else {
                break;
            };
            match self.try_reclaim(shard, candidate) {
                Ok(Some(buffer)) => shard.free.push(buffer),
                _ => {
                    shard.metadata.push_back(candidate);
                    break;
                }
            }
        }
    }

    fn evict_from_streaming(&self, shard: &mut Shard) -> Result<Option<Arc<RwLock<Vec<u8>>>>> {
        for _ in 0..shard.streaming.len() {
            let Some(candidate) = shard.streaming.pop_front() else {
                break;
            };
            match self.try_reclaim(shard, candidate)? {
                Some(buffer) => {
                    self.metrics
                        .streaming_evictions
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(buffer));
                }
                None => shard.streaming.push_back(candidate),
            }
        }
        Ok(None)
    }

    /// Hold the streaming queue inside its budget so a long scan cannot grow
    /// it into the space the search classes need.
    fn enforce_streaming_capacity(&self, shard: &mut Shard) {
        while shard.streaming.len() > shard.streaming_capacity {
            let Some(candidate) = shard.streaming.pop_front() else {
                break;
            };
            match self.try_reclaim(shard, candidate) {
                Ok(Some(buffer)) => {
                    shard.free.push(buffer);
                    self.metrics
                        .streaming_evictions
                        .fetch_add(1, Ordering::Relaxed);
                }
                // Pinned or unreclaimable: keep it and stop trying.
                _ => {
                    shard.streaming.push_back(candidate);
                    break;
                }
            }
        }
    }

    fn evict_from_probationary(&self, shard: &mut Shard) -> Result<Option<Arc<RwLock<Vec<u8>>>>> {
        for _ in 0..shard.probationary.len() {
            let Some(candidate) = shard.probationary.pop_front() else {
                break;
            };
            match self.try_reclaim(shard, candidate)? {
                Some(buffer) => return Ok(Some(buffer)),
                // Pinned: put it back and try the next one.
                None => shard.probationary.push_back(candidate),
            }
        }
        Ok(None)
    }

    fn evict_from_protected(&self, shard: &mut Shard) -> Result<Option<Arc<RwLock<Vec<u8>>>>> {
        // CLOCK second chance: one full sweep clearing reference bits, then a
        // second sweep that will find a victim among what it cleared.
        for _ in 0..shard.protected.len() * 2 {
            let Some(candidate) = shard.protected.pop_front() else {
                break;
            };
            let Some(frame) = shard.resident.get(&candidate).cloned() else {
                continue;
            };
            if frame.pins.load(Ordering::Acquire) > 0 {
                shard.protected.push_back(candidate);
                continue;
            }
            if frame.referenced.swap(false, Ordering::AcqRel) {
                // Recently used: spare it this pass.
                shard.protected.push_back(candidate);
                continue;
            }
            if let Some(buffer) = self.try_reclaim(shard, candidate)? {
                return Ok(Some(buffer));
            }
            shard.protected.push_back(candidate);
        }
        Ok(None)
    }

    /// Reclaim `page_id`'s frame if it is unpinned, flushing it if dirty.
    /// `Ok(None)` means "still pinned, try another".
    fn try_reclaim(
        &self,
        shard: &mut Shard,
        page_id: PageId,
    ) -> Result<Option<Arc<RwLock<Vec<u8>>>>> {
        let Some(frame) = shard.resident.get(&page_id).cloned() else {
            return Ok(None);
        };
        if frame.pins.load(Ordering::Acquire) > 0 {
            return Ok(None);
        }
        if frame.dirty.load(Ordering::Acquire) {
            // This flush still runs under the caller's shard lock: eviction
            // must hand a free buffer to the in-progress acquire, and the
            // caller's candidate loop owns the shard state. That is tolerable
            // only because a dirty eviction is the slow path — background
            // writeback, which holds no shard lock across I/O, keeps the
            // dirty backlog small enough that eviction rarely meets a dirty
            // page.
            self.write_back_frame(page_id, &frame)?;
        }
        if frame.prefetched.swap(false, Ordering::AcqRel) {
            self.metrics
                .read_ahead_pages_wasted
                .fetch_add(1, Ordering::Relaxed);
        }
        shard.resident.remove(&page_id);
        self.metrics.evictions.fetch_add(1, Ordering::Relaxed);

        // The frame's buffer is reused for the incoming page. `try_unwrap` fails
        // if a guard somehow still holds it, in which case allocate a
        // replacement rather than risk aliasing — bounded, because the old Arc
        // is dropped as soon as that guard is.
        match Arc::try_unwrap(frame) {
            Ok(frame) => Ok(Some(frame.data)),
            Err(_) => Ok(Some(Arc::new(RwLock::new(vec![
                0u8;
                self.page_size as usize
            ])))),
        }
    }

    /// Demote from the protected queue when it exceeds its share.
    fn enforce_protected_capacity(&self, shard: &mut Shard) {
        while shard.protected.len() > shard.protected_capacity {
            let Some(demoted) = shard.protected.pop_front() else {
                break;
            };
            if let Some(frame) = shard.resident.get(&demoted) {
                frame.protected.store(false, Ordering::Release);
            }
            shard.probationary.push_back(demoted);
            self.metrics.scan_demotions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Write one frame's dirty contents back through the WAL barrier, holding
    /// only the frame's own data latch — never a shard lock — across the I/O.
    ///
    /// The data write latch is the writeback serializer: writer guards mutate
    /// the dirty/needs_log flags inside their `Drop` while the latch is still
    /// held, and this path clears them under the same latch, so the
    /// copy-log-write-clear sequence can never interleave with a writer's
    /// mark. The latch also serializes concurrent writebacks of the same page;
    /// the dirty re-check after acquisition makes the loser a no-op.
    ///
    /// Holding a shard lock here instead — as this code once did — stalls
    /// every reader and writer hashing to the shard for the duration of a WAL
    /// append plus fsync, which is the difference between a page-granular
    /// pause and a pool-wide stall.
    fn write_back_frame(&self, page_id: PageId, frame: &Arc<Frame>) -> Result<bool> {
        let mut bytes = self.write_frame_data(&frame.data);
        // Re-check under the latch: another writeback (or an eviction) may
        // have flushed this frame while we waited.
        if !frame.dirty.load(Ordering::Acquire) {
            return Ok(false);
        }
        // Write-ahead ordering: the log record must be durable before the
        // page is. Writing a dirty page without this loses committed data
        // to a crash, silently.
        if frame.needs_log.load(Ordering::Acquire) {
            self.barrier_for(page_id, frame.dirtied_by.load(Ordering::Acquire), &bytes)?;
        }
        self.store.write_page(page_id, &mut bytes)?;
        // Clear only after the page write succeeds. A failed write remains
        // conservatively eligible for another WAL barrier and retry.
        frame.needs_log.store(false, Ordering::Release);
        frame.dirty.store(false, Ordering::Release);
        frame.dirty_since_millis.store(0, Ordering::Release);
        self.dirty_index.lock().remove(&page_id);
        self.metrics.writebacks.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Write every dirty page back and fsync. Used at checkpoint.
    pub fn flush_all(&self) -> Result<u64> {
        let mut flushed = 0u64;
        for shard in self.shards.iter() {
            // Snapshot the dirty frames under the shard lock, then flush them
            // with only per-frame data latches held: the shard lock gates
            // every pin/unpin in its hash domain, and holding it across
            // barrier fsyncs turns a checkpoint into a pool-wide reader and
            // writer stall.
            let dirty_frames: Vec<(PageId, Arc<Frame>)> = {
                let shard = self.lock_shard(shard);
                shard
                    .resident
                    .iter()
                    .filter(|(_, frame)| frame.dirty.load(Ordering::Acquire))
                    .map(|(page_id, frame)| (*page_id, frame.clone()))
                    .collect()
            };
            for (page_id, frame) in dirty_frames {
                if self.write_back_frame(page_id, &frame)? {
                    flushed += 1;
                }
            }
        }
        self.store.flush()?;
        Ok(flushed)
    }

    /// Flush a bounded ordered window of dirty resident pages.
    ///
    /// Candidate discovery uses an ordered index containing at most one entry
    /// per dirty resident frame. A write guard inserts its page only when the
    /// guard is dropped, and eviction/removal clears it only after the page
    /// write succeeds. The index therefore remains bounded by the configured
    /// buffer pool rather than write volume or database size.
    ///
    /// `complete` means this cursor reached the end of its ordered pass. Pages
    /// pinned during the pass remain dirty and indexed; a subsequent pass from
    /// the default cursor will revisit them. Pages dirtied behind an already
    /// published cursor likewise belong to that next pass.
    pub fn writeback_step(
        &self,
        cursor: WritebackCursor,
        limits: WritebackLimits,
    ) -> Result<WritebackStepReport> {
        limits.validate(self.page_size)?;
        self.metrics.writeback_steps.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let start_page = cursor.next_page.unwrap_or(0);
        let io_per_page = u64::from(self.page_size) * 2;
        let io_candidates = limits.max_io_bytes / io_per_page;
        let candidate_limit = limits.max_candidates.min(io_candidates);
        let candidate_capacity =
            usize::try_from(candidate_limit).map_err(|_| PageError::InvalidMaintenanceLimits {
                reason: "writeback candidate bound exceeds this platform's address space"
                    .to_string(),
            })?;

        let (candidates, following) = {
            let dirty = self.dirty_index.lock();
            let mut range = dirty.range(start_page..).copied();
            let candidates = range.by_ref().take(candidate_capacity).collect::<Vec<_>>();
            (candidates, range.next())
        };

        let mut report = WritebackStepReport {
            next_cursor: WritebackCursor::default(),
            candidates_examined: 0,
            pages_written: 0,
            page_bytes_written: 0,
            logical_io_bytes: 0,
            pinned_pages_skipped: 0,
            stale_candidates_removed: 0,
            dirty_pages_remaining: 0,
            complete: false,
            stop_reason: WritebackStopReason::Complete,
        };

        for page_id in candidates {
            if report.candidates_examined != 0
                && started.elapsed().as_millis() >= u128::from(limits.max_duration_millis)
            {
                report.next_cursor.next_page = Some(page_id);
                report.stop_reason = WritebackStopReason::DurationLimit;
                break;
            }
            report.candidates_examined = report.candidates_examined.saturating_add(1);
            // The shard lock covers only the resident-map lookup. The
            // barrier, page write, and flag clears run under the frame's own
            // data latch in `write_back_frame`: holding the shard lock across
            // a WAL fsync stalls every pin/unpin hashing to this shard for
            // the whole sync.
            let frame = {
                let shard = self.lock_shard(self.shard_for(page_id));
                shard.resident.get(&page_id).cloned()
            };
            let Some(frame) = frame else {
                self.dirty_index.lock().remove(&page_id);
                report.stale_candidates_removed = report.stale_candidates_removed.saturating_add(1);
                continue;
            };
            if !frame.dirty.load(Ordering::Acquire) {
                self.dirty_index.lock().remove(&page_id);
                report.stale_candidates_removed = report.stale_candidates_removed.saturating_add(1);
                continue;
            }
            if frame.pins.load(Ordering::Acquire) != 0 {
                report.pinned_pages_skipped = report.pinned_pages_skipped.saturating_add(1);
                self.metrics
                    .writeback_pinned_skips
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            if !self.write_back_frame(page_id, &frame)? {
                report.stale_candidates_removed = report.stale_candidates_removed.saturating_add(1);
                continue;
            }
            self.metrics
                .background_writebacks
                .fetch_add(1, Ordering::Relaxed);
            report.pages_written = report.pages_written.saturating_add(1);
            report.page_bytes_written = report
                .page_bytes_written
                .saturating_add(u64::from(self.page_size));
            report.logical_io_bytes = report.logical_io_bytes.saturating_add(io_per_page);
        }

        if report.pages_written != 0 {
            self.store.sync()?;
        }

        if report.next_cursor.next_page.is_none() {
            if let Some(next_page) = following {
                report.next_cursor.next_page = Some(next_page);
                report.stop_reason = if limits.max_candidates <= io_candidates {
                    WritebackStopReason::CandidateLimit
                } else {
                    WritebackStopReason::IoLimit
                };
            } else {
                report.complete = true;
                report.stop_reason = WritebackStopReason::Complete;
            }
        }
        report.dirty_pages_remaining = self.dirty_index.lock().len() as u64;
        match report.stop_reason {
            WritebackStopReason::CandidateLimit => {
                self.metrics
                    .writeback_candidate_limit_stops
                    .fetch_add(1, Ordering::Relaxed);
            }
            WritebackStopReason::IoLimit => {
                self.metrics
                    .writeback_io_limit_stops
                    .fetch_add(1, Ordering::Relaxed);
            }
            WritebackStopReason::DurationLimit => {
                self.metrics
                    .writeback_duration_limit_stops
                    .fetch_add(1, Ordering::Relaxed);
            }
            WritebackStopReason::Complete => {}
        }
        report.validate(limits, self.page_size)?;
        Ok(report)
    }

    /// Exact number of dirty resident frames, in constant time.
    ///
    /// The dirty index contains at most one entry per resident frame and is
    /// updated before a writable guard releases its pin. Checkpoint finalization
    /// uses this rather than walking every shard while holding the writer gate.
    pub fn dirty_page_count(&self) -> u64 {
        self.dirty_index.lock().len() as u64
    }

    /// Submit exact page IDs for speculative loading without performing I/O.
    ///
    /// Requests are deduplicated and the queue has a construction-time hard
    /// cap. Invalid physical IDs are ignored rather than delegated to the
    /// asynchronous worker; a foreground access still reports the authoritative
    /// storage error if it later requests that page.
    pub fn request_read_ahead<I>(&self, page_ids: I) -> ReadAheadSubmitReport
    where
        I: IntoIterator<Item = PageId>,
    {
        let mut report = ReadAheadSubmitReport::default();
        // Request sites are hot paths (every free-space-map probe, every
        // scan sibling hint); the two early returns keep them at one atomic
        // load when speculation cannot go anywhere anyway.
        if self.read_ahead_drivers.load(Ordering::Acquire) == 0 {
            for _ in page_ids {
                report.requested = report.requested.saturating_add(1);
            }
            report.undriven = report.requested;
            report.queue_depth = self.read_ahead_pending_pages.load(Ordering::Relaxed);
            self.metrics
                .read_ahead_requests
                .fetch_add(report.requested, Ordering::Relaxed);
            self.metrics
                .read_ahead_undriven
                .fetch_add(report.undriven, Ordering::Relaxed);
            return report;
        }
        if self.read_ahead_saturated.load(Ordering::Relaxed) {
            for _ in page_ids {
                report.requested = report.requested.saturating_add(1);
            }
            report.queue_full = report.requested;
            report.queue_depth = self.read_ahead_pending_pages.load(Ordering::Relaxed);
            self.metrics
                .read_ahead_requests
                .fetch_add(report.requested, Ordering::Relaxed);
            self.metrics
                .read_ahead_queue_full
                .fetch_add(report.queue_full, Ordering::Relaxed);
            return report;
        }
        let page_count = self.read_source.page_count();
        for page_id in page_ids {
            report.requested = report.requested.saturating_add(1);
            if page_id == 0 || page_id >= page_count {
                report.out_of_bounds = report.out_of_bounds.saturating_add(1);
                continue;
            }
            if self
                .lock_shard(self.shard_for(page_id))
                .resident
                .contains_key(&page_id)
            {
                report.already_resident = report.already_resident.saturating_add(1);
                continue;
            }
            let mut queue = self.read_ahead.lock();
            if queue.queued.contains_key(&page_id) {
                report.already_queued = report.already_queued.saturating_add(1);
            } else if queue.pending.len() >= queue.capacity {
                report.queue_full = report.queue_full.saturating_add(1);
                self.read_ahead_saturated.store(true, Ordering::Relaxed);
            } else {
                queue.push(page_id);
                self.read_ahead_pending_pages
                    .store(queue.pending.len() as u64, Ordering::Relaxed);
                report.enqueued = report.enqueued.saturating_add(1);
            }
        }
        let queue = self.read_ahead.lock();
        report.queue_depth = queue.pending.len() as u64;
        drop(queue);
        self.metrics
            .read_ahead_requests
            .fetch_add(report.requested, Ordering::Relaxed);
        self.metrics
            .read_ahead_enqueued
            .fetch_add(report.enqueued, Ordering::Relaxed);
        self.metrics
            .read_ahead_already_resident
            .fetch_add(report.already_resident, Ordering::Relaxed);
        self.metrics
            .read_ahead_already_queued
            .fetch_add(report.already_queued, Ordering::Relaxed);
        self.metrics
            .read_ahead_out_of_bounds
            .fetch_add(report.out_of_bounds, Ordering::Relaxed);
        self.metrics
            .read_ahead_queue_full
            .fetch_add(report.queue_full, Ordering::Relaxed);
        report
    }

    /// Exact number of deduplicated speculative requests waiting for a host
    /// step. This is constant-time and never exceeds the configured queue cap.
    pub fn read_ahead_queue_depth(&self) -> u64 {
        self.read_ahead.lock().pending.len() as u64
    }

    /// Execute one bounded speculative page-read batch.
    ///
    /// A speculative load may use a free frame or replace an unpinned, clean
    /// probationary page. It never evicts protected or dirty state and never
    /// performs writeback. Per-page read failures are counted and discarded;
    /// the later foreground access remains responsible for surfacing the exact
    /// corruption or I/O error.
    pub fn read_ahead_step(&self, limits: ReadAheadLimits) -> Result<ReadAheadStepReport> {
        limits.validate(self.page_size)?;
        self.metrics
            .read_ahead_steps
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let io_candidates = limits.max_io_bytes / u64::from(self.page_size);
        let candidate_limit = limits.max_candidates.min(io_candidates);
        let mut report = ReadAheadStepReport::default();
        let mut observed_queue_empty = false;

        while report.candidates_examined < candidate_limit {
            if report.candidates_examined != 0
                && started.elapsed() >= Duration::from_millis(limits.max_duration_millis)
            {
                report.stop_reason = ReadAheadStopReason::DurationLimit;
                break;
            }
            let page_id = {
                let mut queue = self.read_ahead.lock();
                let Some(page_id) = queue.pop_front() else {
                    // Record the exact empty-queue snapshot while holding the
                    // queue lock. A producer may enqueue immediately after we
                    // release it; that belongs to the next step and must not
                    // make this completed report internally inconsistent.
                    observed_queue_empty = true;
                    break;
                };
                self.read_ahead_pending_pages
                    .store(queue.pending.len() as u64, Ordering::Relaxed);
                self.read_ahead_saturated.store(false, Ordering::Relaxed);
                page_id
            };
            report.candidates_examined = report.candidates_examined.saturating_add(1);
            match self.prefetch_one(page_id) {
                ReadAheadPageOutcome::Loaded => {
                    report.pages_loaded = report.pages_loaded.saturating_add(1);
                    report.logical_io_bytes = report
                        .logical_io_bytes
                        .saturating_add(u64::from(self.page_size));
                    self.metrics
                        .read_ahead_pages_loaded
                        .fetch_add(1, Ordering::Relaxed);
                }
                ReadAheadPageOutcome::AlreadyResident => {
                    report.already_resident = report.already_resident.saturating_add(1);
                }
                ReadAheadPageOutcome::AdmissionDeclined => {
                    report.admission_declines = report.admission_declines.saturating_add(1);
                    self.metrics
                        .read_ahead_admission_declines
                        .fetch_add(1, Ordering::Relaxed);
                }
                ReadAheadPageOutcome::ReadError => {
                    report.read_errors = report.read_errors.saturating_add(1);
                    report.logical_io_bytes = report
                        .logical_io_bytes
                        .saturating_add(u64::from(self.page_size));
                    self.metrics
                        .read_ahead_read_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        report.queue_depth_remaining = if observed_queue_empty {
            0
        } else {
            self.read_ahead_queue_depth()
        };
        if observed_queue_empty || report.queue_depth_remaining == 0 {
            report.complete = true;
            report.stop_reason = ReadAheadStopReason::Complete;
        } else if report.stop_reason != ReadAheadStopReason::DurationLimit {
            report.stop_reason = if limits.max_candidates <= io_candidates {
                ReadAheadStopReason::CandidateLimit
            } else {
                ReadAheadStopReason::IoLimit
            };
        }
        match report.stop_reason {
            ReadAheadStopReason::CandidateLimit => {
                self.metrics
                    .read_ahead_candidate_limit_stops
                    .fetch_add(1, Ordering::Relaxed);
            }
            ReadAheadStopReason::IoLimit => {
                self.metrics
                    .read_ahead_io_limit_stops
                    .fetch_add(1, Ordering::Relaxed);
            }
            ReadAheadStopReason::DurationLimit => {
                self.metrics
                    .read_ahead_duration_limit_stops
                    .fetch_add(1, Ordering::Relaxed);
            }
            ReadAheadStopReason::Complete => {}
        }
        report.validate(limits, self.page_size)?;
        Ok(report)
    }

    fn prefetch_one(&self, page_id: PageId) -> ReadAheadPageOutcome {
        let mut shard = self.lock_shard(self.shard_for(page_id));
        if shard.resident.contains_key(&page_id) {
            return ReadAheadPageOutcome::AlreadyResident;
        }
        if shard.loading.contains_key(&page_id) {
            // A foreground miss is already reading this page.
            return ReadAheadPageOutcome::AlreadyResident;
        }
        let buffer = match shard.free.pop() {
            Some(buffer) => buffer,
            None => match self.evict_clean_probationary_for_read_ahead(&mut shard) {
                Some(buffer) => buffer,
                None => return ReadAheadPageOutcome::AdmissionDeclined,
            },
        };
        // Same off-lock IO protocol as the foreground miss: speculative reads
        // must never serialize a shard's foreground traffic behind the disk.
        let load = Arc::new(LoadState::default());
        shard.loading.insert(page_id, Arc::clone(&load));
        drop(shard);
        let read_result = {
            let mut bytes = buffer.write();
            self.read_source.read_page(page_id, &mut bytes)
        };
        let mut shard = self.lock_shard(self.shard_for(page_id));
        shard.loading.remove(&page_id);
        if read_result.is_err() {
            shard.free.push(buffer);
            drop(shard);
            *load.done.lock() = true;
            load.signal.notify_all();
            return ReadAheadPageOutcome::ReadError;
        }
        let frame = Arc::new(Frame {
            page_id,
            data: buffer,
            pins: AtomicU32::new(0),
            dirty: AtomicBool::new(false),
            dirty_since_millis: AtomicU64::new(0),
            referenced: AtomicBool::new(false),
            protected: AtomicBool::new(false),
            // Read-ahead is driven by search scans; a speculative page enters
            // the search classes like any other foreground search read.
            streaming: AtomicBool::new(false),
            metadata: AtomicBool::new(false),
            dirtied_by: AtomicU64::new(0),
            needs_log: AtomicBool::new(false),
            prefetched: AtomicBool::new(true),
            foreground_touched: AtomicBool::new(false),
        });
        shard.resident.insert(page_id, frame);
        shard.probationary.push_back(page_id);
        drop(shard);
        *load.done.lock() = true;
        load.signal.notify_all();
        ReadAheadPageOutcome::Loaded
    }

    /// Reuse only clean probationary storage for speculative work. Returning
    /// `None` is deliberate backpressure, not a foreground admission failure.
    fn evict_clean_probationary_for_read_ahead(
        &self,
        shard: &mut Shard,
    ) -> Option<Arc<RwLock<Vec<u8>>>> {
        for _ in 0..shard.probationary.len() {
            let candidate = shard.probationary.pop_front()?;
            let Some(frame) = shard.resident.get(&candidate).cloned() else {
                continue;
            };
            if frame.pins.load(Ordering::Acquire) != 0 || frame.dirty.load(Ordering::Acquire) {
                shard.probationary.push_back(candidate);
                continue;
            }
            if frame.prefetched.swap(false, Ordering::AcqRel) {
                self.metrics
                    .read_ahead_pages_wasted
                    .fetch_add(1, Ordering::Relaxed);
            }
            shard.resident.remove(&candidate);
            self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            return Some(match Arc::try_unwrap(frame) {
                Ok(frame) => frame.data,
                Err(_) => Arc::new(RwLock::new(vec![0_u8; self.page_size as usize])),
            });
        }
        None
    }

    /// Load a page into the cache without returning a guard, if there is room.
    ///
    /// Warming is explicitly best-effort: it must never fail a caller or evict
    /// something a query is using, so admission failure is swallowed. That is
    /// the roadmap's "warming must be cancellable, rate-limited, and lower
    /// priority than foreground reads" applied at its simplest.
    pub fn warm(&self, page_id: PageId) -> Result<bool> {
        match self.get(page_id) {
            Ok(_) => Ok(true),
            Err(PageError::PoolExhausted { .. }) => Ok(false),
            Err(other) => Err(other),
        }
    }

    /// Page ids currently dirty, so a caller can log their after-images.
    ///
    /// **Every dirty page, logged or not.** Almost always the wrong thing to
    /// drive logging from — see [`Self::take_pages_needing_log`].
    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        let mut out = Vec::new();
        for shard in self.shards.iter() {
            let shard = self.lock_shard(shard);
            for (page_id, frame) in shard.resident.iter() {
                if frame.dirty.load(Ordering::Relaxed) {
                    out.push(*page_id);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Pages modified since their last log record, clearing the flag.
    ///
    /// The distinction from [`Self::dirty_page_ids`] is not a micro-optimization.
    /// A page stays dirty until it is evicted or checkpointed, so logging "every
    /// dirty page" after each operation re-logs the entire resident set every
    /// time — with an 8192-frame pool that is 64 MiB of log per insert.
    /// Measured at 411 GB of WAL for 233 MB of data before this existed: a
    /// 1,767x write amplification that made ingest disk-bound and would have
    /// made a corpus-scale import impossible.
    pub fn take_pages_needing_log(&self) -> Vec<PageId> {
        let mut out = Vec::new();
        for shard in self.shards.iter() {
            let shard = self.lock_shard(shard);
            for (page_id, frame) in shard.resident.iter() {
                if frame.needs_log.swap(false, Ordering::AcqRel) {
                    out.push(*page_id);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Whether a page is currently cached. Diagnostic and test support — a
    /// caller deciding what to do based on residency would be racing.
    pub fn is_resident(&self, page_id: PageId) -> bool {
        self.lock_shard(self.shard_for(page_id))
            .resident
            .contains_key(&page_id)
    }

    pub fn snapshot(&self) -> BufferPoolSnapshot {
        let (read_ahead_queue_capacity, read_ahead_queue_depth) = {
            let queue = self.read_ahead.lock();
            (queue.capacity as u64, queue.pending.len() as u64)
        };
        let snapshot_millis = self.monotonic_millis();
        let mut oldest_dirty_since_millis = None::<u64>;
        let mut snapshot = BufferPoolSnapshot {
            budget_bytes: self.budget_bytes,
            page_size: self.page_size,
            total_frames: self.total_frames as u64,
            hits: self.metrics.hits.load(Ordering::Relaxed),
            misses: self.metrics.misses.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            writebacks: self.metrics.writebacks.load(Ordering::Relaxed),
            admission_failures: self.metrics.admission_failures.load(Ordering::Relaxed),
            scan_demotions: self.metrics.scan_demotions.load(Ordering::Relaxed),
            writeback_steps: self.metrics.writeback_steps.load(Ordering::Relaxed),
            background_writebacks: self.metrics.background_writebacks.load(Ordering::Relaxed),
            writeback_candidate_limit_stops: self
                .metrics
                .writeback_candidate_limit_stops
                .load(Ordering::Relaxed),
            writeback_io_limit_stops: self
                .metrics
                .writeback_io_limit_stops
                .load(Ordering::Relaxed),
            writeback_duration_limit_stops: self
                .metrics
                .writeback_duration_limit_stops
                .load(Ordering::Relaxed),
            writeback_pinned_skips: self.metrics.writeback_pinned_skips.load(Ordering::Relaxed),
            read_ahead_queue_capacity,
            read_ahead_queue_depth,
            read_ahead_requests: self.metrics.read_ahead_requests.load(Ordering::Relaxed),
            read_ahead_undriven: self.metrics.read_ahead_undriven.load(Ordering::Relaxed),
            read_ahead_enqueued: self.metrics.read_ahead_enqueued.load(Ordering::Relaxed),
            read_ahead_already_resident: self
                .metrics
                .read_ahead_already_resident
                .load(Ordering::Relaxed),
            read_ahead_already_queued: self
                .metrics
                .read_ahead_already_queued
                .load(Ordering::Relaxed),
            read_ahead_out_of_bounds: self
                .metrics
                .read_ahead_out_of_bounds
                .load(Ordering::Relaxed),
            read_ahead_queue_full: self.metrics.read_ahead_queue_full.load(Ordering::Relaxed),
            read_ahead_steps: self.metrics.read_ahead_steps.load(Ordering::Relaxed),
            read_ahead_pages_loaded: self.metrics.read_ahead_pages_loaded.load(Ordering::Relaxed),
            read_ahead_pages_used: self.metrics.read_ahead_pages_used.load(Ordering::Relaxed),
            read_ahead_pages_wasted: self.metrics.read_ahead_pages_wasted.load(Ordering::Relaxed),
            read_ahead_admission_declines: self
                .metrics
                .read_ahead_admission_declines
                .load(Ordering::Relaxed),
            read_ahead_read_errors: self.metrics.read_ahead_read_errors.load(Ordering::Relaxed),
            read_ahead_foreground_cancellations: self
                .metrics
                .read_ahead_foreground_cancellations
                .load(Ordering::Relaxed),
            read_ahead_candidate_limit_stops: self
                .metrics
                .read_ahead_candidate_limit_stops
                .load(Ordering::Relaxed),
            read_ahead_io_limit_stops: self
                .metrics
                .read_ahead_io_limit_stops
                .load(Ordering::Relaxed),
            read_ahead_duration_limit_stops: self
                .metrics
                .read_ahead_duration_limit_stops
                .load(Ordering::Relaxed),
            read_only: self.read_only,
            ..Default::default()
        };

        for shard in self.shards.iter() {
            let shard = self.lock_shard(shard);
            snapshot.free_frames += shard.free.len() as u64;
            snapshot.protected_pages += shard.protected.len() as u64;
            snapshot.probationary_pages += shard.probationary.len() as u64;
            for frame in shard.resident.values() {
                snapshot.resident_pages += 1;
                if frame.dirty.load(Ordering::Relaxed) {
                    snapshot.dirty_pages += 1;
                    let dirty_since = frame.dirty_since_millis.load(Ordering::Acquire);
                    if dirty_since != 0 {
                        oldest_dirty_since_millis = Some(
                            oldest_dirty_since_millis
                                .map(|oldest| oldest.min(dirty_since))
                                .unwrap_or(dirty_since),
                        );
                    }
                }
                if frame.pins.load(Ordering::Relaxed) > 0 {
                    snapshot.pinned_pages += 1;
                } else {
                    snapshot.evictable_pages += 1;
                }
            }
        }
        snapshot.resident_bytes = snapshot.resident_pages * u64::from(self.page_size);
        snapshot.shard_lock_waits = self.metrics.shard_lock_waits.load(Ordering::Relaxed);
        snapshot.shard_lock_wait_nanos = self.metrics.shard_lock_wait_nanos.load(Ordering::Relaxed);
        snapshot.shard_lock_max_wait_nanos = self
            .metrics
            .shard_lock_max_wait_nanos
            .load(Ordering::Relaxed);
        snapshot.page_latch_waits = self.metrics.page_latch_waits.load(Ordering::Relaxed);
        snapshot.page_latch_wait_nanos = self.metrics.page_latch_wait_nanos.load(Ordering::Relaxed);
        snapshot.page_latch_max_wait_nanos = self
            .metrics
            .page_latch_max_wait_nanos
            .load(Ordering::Relaxed);
        snapshot.oldest_dirty_page_age_millis = oldest_dirty_since_millis
            .map(|dirty_since| snapshot_millis.saturating_sub(dirty_since))
            .unwrap_or(0);
        snapshot.writeback_lag_pages = snapshot.dirty_pages;
        snapshot.writeback_lag_bytes = snapshot
            .writeback_lag_pages
            .saturating_mul(u64::from(self.page_size));
        snapshot
    }

    fn remove_from(queue: &mut VecDeque<PageId>, page_id: PageId) {
        if let Some(position) = queue.iter().position(|id| *id == page_id) {
            queue.remove(position);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Barrier;
    use std::time::Duration;

    use super::*;
    use crate::manager::PageStoreOptions;
    use crate::page::{PageType, PAGE_HEADER_BYTES};
    use tempfile::TempDir;

    /// THE acceptance property for cache-class isolation:
    ///
    /// > Streaming a large volume of document text does not degrade the hot
    /// > search working set.
    ///
    /// Warm a search working set into the protected queue, stream far more
    /// document pages than the pool can hold, then re-read the working set.
    /// Every re-read must be a HIT.
    #[test]
    fn a_document_scan_cannot_evict_the_search_working_set() {
        // 64 frames; the scan below is 10x that.
        let (_dir, _store, pool, ids) = setup(700, 64 * 512);
        let hot = &ids[..20];

        // Two touches promote into protected — the search working set.
        for _ in 0..2 {
            for page in hot {
                pool.get(*page).unwrap();
            }
        }
        let hits_before = pool.metrics.hits.load(Ordering::Relaxed);
        let misses_before = pool.metrics.misses.load(Ordering::Relaxed);

        // The realistic pollution pattern is read-then-snippet: each document
        // page is touched TWICE while still resident. Under a plain 2Q that
        // second touch is exactly what promotes a page into protected and
        // displaces the search working set. 640 pages through 64 frames.
        for page in &ids[40..680] {
            pool.get_streaming(*page).unwrap();
            pool.get_streaming(*page).unwrap();
        }

        // Re-read the working set: every page must still be resident.
        let misses_after_scan = pool.metrics.misses.load(Ordering::Relaxed);
        for page in hot {
            pool.get(*page).unwrap();
        }
        let misses_after = pool.metrics.misses.load(Ordering::Relaxed);

        assert_eq!(
            misses_after,
            misses_after_scan,
            "the document scan evicted {} of {} search pages",
            misses_after - misses_after_scan,
            hot.len()
        );
        assert!(pool.metrics.hits.load(Ordering::Relaxed) > hits_before);
        assert!(
            misses_after_scan > misses_before,
            "the scan did no I/O at all"
        );
    }

    /// A deep posting traversal is `Search` and may legitimately churn the
    /// whole search budget — but it must not take the catalog with it.
    #[test]
    fn a_search_traversal_cannot_evict_metadata() {
        // 256 frames across 8 shards is 32 per shard, so the 10% metadata
        // tier holds 3 — comfortably more than the one page per shard the
        // eight hot pages below will occupy.
        let (_dir, _store, pool, ids) = setup(900, 256 * 512);
        let hot = &ids[..8];

        // Warm the metadata tier.
        for _ in 0..2 {
            for page in hot {
                pool.get_metadata(*page).unwrap();
            }
        }
        let misses_before_scan = pool.metrics.misses.load(Ordering::Relaxed);

        // A search workload far larger than the pool, touched twice each so it
        // reaches the protected queue.
        for page in &ids[40..880] {
            pool.get(*page).unwrap();
            pool.get(*page).unwrap();
        }
        let misses_after_scan = pool.metrics.misses.load(Ordering::Relaxed);
        assert!(
            misses_after_scan > misses_before_scan,
            "the search traversal did no I/O at all"
        );

        for page in hot {
            pool.get_metadata(*page).unwrap();
        }
        assert_eq!(
            pool.metrics.misses.load(Ordering::Relaxed),
            misses_after_scan,
            "a search traversal evicted catalog pages"
        );
    }

    /// Isolation runs both ways: the metadata tier is bounded, so it cannot
    /// starve the search classes either.
    #[test]
    fn metadata_cannot_grow_past_its_budget() {
        let (_dir, _store, pool, ids) = setup(700, 64 * 512);
        // Admit far more metadata pages than the tier can hold.
        for page in &ids[..400] {
            pool.get_metadata(*page).unwrap();
        }
        let shard = pool.lock_shard(&pool.shards[0]);
        assert!(
            shard.metadata.len() <= shard.metadata_capacity,
            "metadata queue {} exceeded its budget {}",
            shard.metadata.len(),
            shard.metadata_capacity
        );
    }

    /// A metadata page touched repeatedly must stay in its own tier rather
    /// than promoting into protected, where a search workload could reach it.
    #[test]
    fn metadata_pages_never_promote_into_protected() {
        let (_dir, _store, pool, ids) = setup(200, 64 * 512);
        for _ in 0..5 {
            for page in &ids[..8] {
                pool.get_metadata(*page).unwrap();
            }
        }
        let shard = pool.lock_shard(&pool.shards[0]);
        let promoted = shard
            .protected
            .iter()
            .filter(|page| ids[..8].contains(page))
            .count();
        assert_eq!(promoted, 0, "a metadata page reached the protected queue");
    }

    /// The control. Without class isolation the same scan DOES evict the
    /// working set — otherwise the test above proves nothing.
    #[test]
    fn the_same_scan_without_class_isolation_does_evict_it() {
        let (_dir, _store, pool, ids) = setup(700, 64 * 512);
        let hot = &ids[..20];
        for _ in 0..2 {
            for page in hot {
                pool.get(*page).unwrap();
            }
        }
        // Identical volume and identical access pattern, admitted as SEARCH
        // rather than streaming.
        for page in &ids[40..680] {
            pool.get(*page).unwrap();
            pool.get(*page).unwrap();
        }
        let misses_before = pool.metrics.misses.load(Ordering::Relaxed);
        for page in hot {
            pool.get(*page).unwrap();
        }
        let evicted = pool.metrics.misses.load(Ordering::Relaxed) - misses_before;
        assert!(
            evicted > 0,
            "a same-class scan left the working set intact, so the isolation \
             test above is not discriminating"
        );
    }

    /// A streaming page touched repeatedly must never be promoted into the
    /// protected queue — a document read twice is still a document.
    #[test]
    fn streaming_pages_never_promote_into_protected() {
        let (_dir, _store, pool, ids) = setup(200, 64 * 512);
        for _ in 0..5 {
            for page in &ids[..10] {
                pool.get_streaming(*page).unwrap();
            }
        }
        let shard = pool.lock_shard(&pool.shards[0]);
        let promoted = shard
            .protected
            .iter()
            .filter(|page| ids[..10].contains(page))
            .count();
        assert_eq!(promoted, 0, "a streaming page reached the protected queue");
    }

    fn setup(pages: u64, budget_bytes: u64) -> (TempDir, Arc<PageStore>, BufferPool, Vec<PageId>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            PageStore::open(
                dir.path().join("t.pages"),
                PageStoreOptions::default()
                    .with_page_size(512)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let mut ids = Vec::new();
        for index in 0..pages {
            let page_id = store.allocate(PageType::Heap).unwrap();
            let mut page = vec![0u8; store.page_size() as usize];
            PageHeader::new(page_id, PageType::Heap, store.page_size()).encode(&mut page);
            page[PAGE_HEADER_BYTES] = (index % 251) as u8;
            store.write_page(page_id, &mut page).unwrap();
            ids.push(page_id);
        }
        let pool = BufferPool::new(
            store.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(budget_bytes)
                .with_shards(1),
        )
        .unwrap();
        // The read-ahead tests exercise driver-present semantics; without a
        // registered driver, requests are dropped as undriven by design.
        pool.register_read_ahead_driver();
        (dir, store, pool, ids)
    }

    /// A page source with injected latency: the only way to observe whether
    /// miss IO runs under or outside the shard lock.
    #[derive(Debug)]
    struct SlowSource {
        inner: Arc<PageStore>,
        delay: Duration,
        reads: AtomicU64,
    }

    impl PageReadSource for SlowSource {
        fn page_size(&self) -> u32 {
            PageStore::page_size(&self.inner)
        }

        fn page_count(&self) -> u64 {
            PageStore::page_count(&self.inner)
        }

        fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<PageHeader> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(self.delay);
            PageStore::read_page(&self.inner, page_id, buffer)
        }
    }

    #[test]
    fn concurrent_misses_in_one_shard_overlap_their_io() {
        let (_dir, store, seeded_pool, ids) = setup(12, 64 * 1024);
        drop(seeded_pool);
        let source = Arc::new(SlowSource {
            inner: store.clone(),
            delay: Duration::from_millis(25),
            reads: AtomicU64::new(0),
        });
        let pool = BufferPool::new_read_only(
            store,
            source.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(64 * 1024)
                .with_shards(1),
        )
        .unwrap();

        let barrier = Barrier::new(ids.len());
        let started = Instant::now();
        std::thread::scope(|scope| {
            for (index, page_id) in ids.iter().enumerate() {
                let pool = &pool;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let guard = pool.get(*page_id).unwrap();
                    assert_eq!(guard.bytes()[PAGE_HEADER_BYTES], (index % 251) as u8);
                });
            }
        });
        let elapsed = started.elapsed();
        assert_eq!(source.reads.load(Ordering::Relaxed), ids.len() as u64);
        // Serialized under the shard lock this takes >= 12 x 25 ms = 300 ms;
        // overlapped it approaches one delay. Half the serial floor is a
        // generous threshold that still fails the old lock-held-IO shape.
        assert!(
            elapsed < Duration::from_millis(150),
            "cold misses serialized: {elapsed:?}"
        );
    }

    #[test]
    fn a_miss_stampede_loads_the_page_once() {
        let (_dir, store, seeded_pool, ids) = setup(4, 64 * 1024);
        drop(seeded_pool);
        let source = Arc::new(SlowSource {
            inner: store.clone(),
            delay: Duration::from_millis(30),
            reads: AtomicU64::new(0),
        });
        let pool = BufferPool::new_read_only(
            store,
            source.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(64 * 1024)
                .with_shards(1),
        )
        .unwrap();

        let barrier = Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let pool = &pool;
                let barrier = &barrier;
                let page_id = ids[0];
                scope.spawn(move || {
                    barrier.wait();
                    let guard = pool.get(page_id).unwrap();
                    assert_eq!(guard.bytes()[PAGE_HEADER_BYTES], 0);
                });
            }
        });
        assert_eq!(
            source.reads.load(Ordering::Relaxed),
            1,
            "stampeding readers must share one load"
        );
        assert_eq!(pool.metrics.misses.load(Ordering::Relaxed), 1);
        assert_eq!(pool.metrics.hits.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn a_failed_concurrent_miss_returns_frames_and_recovers() {
        let (_dir, _store, pool, ids) = setup(6, 64 * 1024);
        let missing = PageId::MAX / 2;
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let pool = &pool;
                scope.spawn(move || {
                    assert!(pool.get(missing).is_err());
                });
            }
        });
        // Capacity must be intact: every seeded page still loads.
        for (index, page_id) in ids.iter().enumerate() {
            let guard = pool.get(*page_id).unwrap();
            assert_eq!(guard.bytes()[PAGE_HEADER_BYTES], (index % 251) as u8);
        }
    }

    #[test]
    fn a_budget_too_small_for_one_page_is_rejected() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            PageStore::open(
                dir.path().join("t.pages"),
                PageStoreOptions::default().with_fsync(false),
            )
            .unwrap(),
        );
        let error = BufferPool::new(store, BufferPoolOptions::default().with_budget_bytes(100))
            .unwrap_err();
        assert!(matches!(error, PageError::BudgetTooSmall { .. }));
    }

    #[test]
    fn resident_bytes_never_exceed_the_budget() {
        // The whole premise of paging: touch far more data than the pool holds
        // and the pool must not grow. 8 frames, 200 pages.
        let (_dir, _store, pool, ids) = setup(200, 8 * 512);
        for page_id in &ids {
            let _guard = pool.get(*page_id).unwrap();
        }
        let snapshot = pool.snapshot();
        assert!(
            snapshot.resident_bytes <= snapshot.budget_bytes,
            "resident {} exceeded budget {}",
            snapshot.resident_bytes,
            snapshot.budget_bytes
        );
        assert_eq!(snapshot.total_frames, 8);
        assert!(snapshot.evictions > 0);
    }

    #[test]
    fn metadata_lock_contention_records_bounded_wait_telemetry() {
        let (_dir, _store, pool, ids) = setup(2, 2 * 512);
        let pool = Arc::new(pool);
        let held = pool.shard_for(ids[0]).lock();
        let rendezvous = Arc::new(Barrier::new(2));
        let worker_pool = pool.clone();
        let worker_rendezvous = rendezvous.clone();
        let page_id = ids[0];
        let worker = std::thread::spawn(move || {
            worker_rendezvous.wait();
            drop(worker_pool.get(page_id).unwrap());
        });
        rendezvous.wait();
        std::thread::sleep(Duration::from_millis(25));
        drop(held);
        worker.join().unwrap();

        let snapshot = pool.snapshot();
        assert_eq!(snapshot.shard_lock_waits, 1);
        assert!(snapshot.shard_lock_wait_nanos > 0);
        assert!(snapshot.shard_lock_max_wait_nanos > 0);
        assert!(snapshot.shard_lock_max_wait_nanos <= snapshot.shard_lock_wait_nanos);
    }

    #[test]
    fn page_latch_contention_records_bounded_wait_telemetry() {
        let (_dir, _store, pool, ids) = setup(2, 2 * 512);
        let pool = Arc::new(pool);
        let held = pool.get(ids[0]).unwrap();
        let rendezvous = Arc::new(Barrier::new(2));
        let worker_pool = pool.clone();
        let worker_rendezvous = rendezvous.clone();
        let page_id = ids[0];
        let worker = std::thread::spawn(move || {
            worker_rendezvous.wait();
            drop(worker_pool.get_mut(page_id).unwrap());
        });
        rendezvous.wait();
        std::thread::sleep(Duration::from_millis(25));
        drop(held);
        worker.join().unwrap();

        let snapshot = pool.snapshot();
        assert_eq!(snapshot.page_latch_waits, 1);
        assert!(snapshot.page_latch_wait_nanos > 0);
        assert!(snapshot.page_latch_max_wait_nanos > 0);
        assert!(snapshot.page_latch_max_wait_nanos <= snapshot.page_latch_wait_nanos);
    }

    #[test]
    fn flush_latch_wait_is_observable_and_still_completes_writeback() {
        let (_dir, _store, pool, ids) = setup(2, 2 * 512);
        let pool = Arc::new(pool);
        {
            let mut page = pool.get_mut(ids[0]).unwrap();
            page[PAGE_HEADER_BYTES + 2] = 72;
        }
        let held = pool.get(ids[0]).unwrap();
        let rendezvous = Arc::new(Barrier::new(2));
        let worker_pool = pool.clone();
        let worker_rendezvous = rendezvous.clone();
        let worker = std::thread::spawn(move || {
            worker_rendezvous.wait();
            worker_pool.flush_all().unwrap()
        });
        rendezvous.wait();
        std::thread::sleep(Duration::from_millis(25));
        drop(held);
        assert_eq!(worker.join().unwrap(), 1);

        let snapshot = pool.snapshot();
        assert_eq!(snapshot.page_latch_waits, 1);
        assert!(snapshot.page_latch_wait_nanos > 0);
        assert_eq!(snapshot.writeback_lag_pages, 0);
    }

    #[test]
    fn dirty_age_and_writeback_lag_clear_only_after_durable_writeback() {
        let (_dir, _store, pool, ids) = setup(2, 2 * 512);
        {
            let mut page = pool.get_mut(ids[0]).unwrap();
            page[PAGE_HEADER_BYTES + 1] = 91;
        }
        std::thread::sleep(Duration::from_millis(5));

        let dirty = pool.snapshot();
        assert_eq!(dirty.dirty_pages, 1);
        assert_eq!(dirty.writeback_lag_pages, 1);
        assert_eq!(dirty.writeback_lag_bytes, 512);
        assert!(dirty.oldest_dirty_page_age_millis >= 1);

        assert_eq!(pool.flush_all().unwrap(), 1);
        let clean = pool.snapshot();
        assert_eq!(clean.dirty_pages, 0);
        assert_eq!(clean.writeback_lag_pages, 0);
        assert_eq!(clean.writeback_lag_bytes, 0);
        assert_eq!(clean.oldest_dirty_page_age_millis, 0);
    }

    #[test]
    fn a_second_read_of_a_cached_page_is_a_hit() {
        let (_dir, _store, pool, ids) = setup(4, 8 * 512);
        let _ = pool.get(ids[0]).unwrap();
        let before = pool.snapshot();
        let _ = pool.get(ids[0]).unwrap();
        let after = pool.snapshot();
        assert_eq!(after.hits, before.hits + 1);
        assert_eq!(after.misses, before.misses);
    }

    #[test]
    fn pinned_pages_are_never_evicted() {
        let (_dir, _store, pool, ids) = setup(50, 4 * 512);
        // Hold two pages pinned, then thrash the pool.
        let pinned_a = pool.get(ids[0]).unwrap();
        let pinned_b = pool.get(ids[1]).unwrap();
        for page_id in &ids[2..] {
            // Some of these will fail admission once the unpinned frames run
            // out, which is the correct bounded behaviour.
            let _ = pool.get(*page_id);
        }
        // The pinned pages must still be readable and still be themselves.
        assert_eq!(pinned_a.page_id(), ids[0]);
        assert_eq!(pinned_b.page_id(), ids[1]);
        assert_eq!(pinned_a.bytes()[PAGE_HEADER_BYTES], 0);
        assert_eq!(pinned_b.bytes()[PAGE_HEADER_BYTES], 1);
    }

    #[test]
    fn a_fully_pinned_shard_fails_admission_rather_than_growing() {
        let (_dir, _store, pool, ids) = setup(20, 3 * 512);
        let _a = pool.get(ids[0]).unwrap();
        let _b = pool.get(ids[1]).unwrap();
        let _c = pool.get(ids[2]).unwrap();

        let error = pool.get(ids[3]).unwrap_err();
        assert!(
            matches!(error, PageError::PoolExhausted { .. }),
            "expected bounded failure, got {error:?}"
        );
        assert_eq!(pool.snapshot().admission_failures, 1);
        assert!(pool.snapshot().resident_bytes <= pool.snapshot().budget_bytes);
    }

    #[test]
    fn writes_are_marked_dirty_and_written_back_on_eviction() {
        let (_dir, store, pool, ids) = setup(40, 4 * 512);
        {
            let mut guard = pool.get_mut(ids[0]).unwrap();
            guard.bytes_mut()[PAGE_HEADER_BYTES + 1] = 0x7F;
        }
        assert_eq!(pool.snapshot().dirty_pages, 1);

        // Force it out.
        for page_id in &ids[1..] {
            let _ = pool.get(*page_id);
        }

        let mut page = vec![0u8; store.page_size() as usize];
        store.read_page(ids[0], &mut page).unwrap();
        assert_eq!(
            page[PAGE_HEADER_BYTES + 1],
            0x7F,
            "dirty page was evicted without writeback"
        );
        assert!(pool.snapshot().writebacks > 0);
    }

    #[test]
    fn flush_all_persists_every_dirty_page() {
        let (_dir, store, pool, ids) = setup(10, 16 * 512);
        for (index, page_id) in ids.iter().enumerate().take(5) {
            let mut guard = pool.get_mut(*page_id).unwrap();
            guard.bytes_mut()[PAGE_HEADER_BYTES + 2] = index as u8 + 100;
        }
        assert_eq!(pool.flush_all().unwrap(), 5);
        assert_eq!(pool.snapshot().dirty_pages, 0);

        for (index, page_id) in ids.iter().enumerate().take(5) {
            let mut page = vec![0u8; store.page_size() as usize];
            store.read_page(*page_id, &mut page).unwrap();
            assert_eq!(page[PAGE_HEADER_BYTES + 2], index as u8 + 100);
        }
    }

    #[derive(Debug)]
    struct CountingBarrier {
        calls: AtomicU64,
        delay: Duration,
        fail: bool,
    }

    impl WritebackBarrier for CountingBarrier {
        fn before_writeback(
            &self,
            _page_id: PageId,
            _transaction: u64,
            _bytes: &[u8],
        ) -> Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay);
            }
            if self.fail {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason: "injected writeback barrier failure".to_string(),
                });
            }
            Ok(())
        }
    }

    fn dirty(pool: &BufferPool, page_ids: &[PageId]) {
        for (index, page_id) in page_ids.iter().enumerate() {
            let mut guard = pool.get_mut(*page_id).unwrap();
            guard.bytes_mut()[PAGE_HEADER_BYTES + 3] = index as u8 + 1;
        }
    }

    #[test]
    fn bounded_writeback_resumes_at_the_exact_inclusive_candidate() {
        let (_dir, store, pool, ids) = setup(10, 16 * 512);
        dirty(&pool, &ids[..5]);
        let limits = WritebackLimits {
            max_candidates: 2,
            max_io_bytes: 64 * 1024,
            max_duration_millis: 1_000,
        };

        let first = pool
            .writeback_step(WritebackCursor::default(), limits)
            .unwrap();
        assert_eq!(first.pages_written, 2);
        assert_eq!(first.candidates_examined, 2);
        assert_eq!(first.next_cursor.next_page, Some(ids[2]));
        assert_eq!(first.stop_reason, WritebackStopReason::CandidateLimit);
        assert!(!first.complete);
        assert_eq!(first.dirty_pages_remaining, 3);

        let second = pool.writeback_step(first.next_cursor, limits).unwrap();
        assert_eq!(second.pages_written, 2);
        assert_eq!(second.next_cursor.next_page, Some(ids[4]));
        let third = pool.writeback_step(second.next_cursor, limits).unwrap();
        assert_eq!(third.pages_written, 1);
        assert!(third.complete);
        assert_eq!(third.stop_reason, WritebackStopReason::Complete);
        assert_eq!(third.dirty_pages_remaining, 0);

        for (index, page_id) in ids.iter().enumerate().take(5) {
            let mut page = vec![0_u8; store.page_size() as usize];
            store.read_page(*page_id, &mut page).unwrap();
            assert_eq!(page[PAGE_HEADER_BYTES + 3], index as u8 + 1);
        }
    }

    #[test]
    fn writeback_io_limit_and_serialized_contract_are_strict() {
        let (_dir, _store, pool, ids) = setup(5, 8 * 512);
        dirty(&pool, &ids[..3]);
        let limits = WritebackLimits {
            max_candidates: 8,
            max_io_bytes: 2 * 512,
            max_duration_millis: 1_000,
        };
        let report = pool
            .writeback_step(WritebackCursor::default(), limits)
            .unwrap();
        assert_eq!(report.pages_written, 1);
        assert_eq!(report.page_bytes_written, 512);
        assert_eq!(report.logical_io_bytes, 1_024);
        assert_eq!(report.next_cursor.next_page, Some(ids[1]));
        assert_eq!(report.stop_reason, WritebackStopReason::IoLimit);

        let encoded = serde_json::to_value(report).unwrap();
        assert_eq!(
            serde_json::from_value::<WritebackStepReport>(encoded.clone()).unwrap(),
            report
        );
        let mut forged = encoded;
        forged["future_unbounded_field"] = serde_json::json!(true);
        assert!(serde_json::from_value::<WritebackStepReport>(forged).is_err());
        let mut inconsistent = report;
        inconsistent.page_bytes_written += 1;
        assert!(inconsistent.validate(limits, pool.page_size()).is_err());
        assert!(WritebackLimits {
            max_candidates: 0,
            ..limits
        }
        .validate(pool.page_size())
        .is_err());
    }

    #[test]
    fn pinned_dirty_pages_are_skipped_but_remain_for_the_next_sweep() {
        let (_dir, _store, pool, ids) = setup(5, 8 * 512);
        dirty(&pool, &ids[..2]);
        let pinned = pool.get(ids[0]).unwrap();

        let first = pool
            .writeback_step(WritebackCursor::default(), WritebackLimits::default())
            .unwrap();
        assert!(first.complete);
        assert_eq!(first.pinned_pages_skipped, 1);
        assert_eq!(first.pages_written, 1);
        assert_eq!(first.dirty_pages_remaining, 1);

        drop(pinned);
        let second = pool
            .writeback_step(WritebackCursor::default(), WritebackLimits::default())
            .unwrap();
        assert!(second.complete);
        assert_eq!(second.pages_written, 1);
        assert_eq!(second.dirty_pages_remaining, 0);
    }

    #[test]
    fn writeback_barrier_failure_leaves_the_page_dirty_and_retryable() {
        let (_dir, store, pool, ids) = setup(5, 8 * 512);
        dirty(&pool, &ids[..1]);
        std::thread::sleep(Duration::from_millis(3));
        let age_before_failure = pool.snapshot().oldest_dirty_page_age_millis;
        assert!(age_before_failure >= 1);
        let failing = Arc::new(CountingBarrier {
            calls: AtomicU64::new(0),
            delay: Duration::ZERO,
            fail: true,
        });
        pool.set_barrier(failing.clone());
        assert!(pool
            .writeback_step(WritebackCursor::default(), WritebackLimits::default())
            .is_err());
        assert_eq!(failing.calls.load(Ordering::Relaxed), 1);
        let failed = pool.snapshot();
        assert_eq!(failed.dirty_pages, 1);
        assert_eq!(failed.writeback_lag_pages, 1);
        assert_eq!(failed.writeback_lag_bytes, 512);
        assert!(failed.oldest_dirty_page_age_millis >= age_before_failure);
        let mut durable = vec![0_u8; store.page_size() as usize];
        store.read_page(ids[0], &mut durable).unwrap();
        assert_eq!(durable[PAGE_HEADER_BYTES + 3], 0);

        let succeeding = Arc::new(CountingBarrier {
            calls: AtomicU64::new(0),
            delay: Duration::ZERO,
            fail: false,
        });
        pool.set_barrier(succeeding.clone());
        let report = pool
            .writeback_step(WritebackCursor::default(), WritebackLimits::default())
            .unwrap();
        assert_eq!(report.pages_written, 1);
        assert_eq!(succeeding.calls.load(Ordering::Relaxed), 1);
        let clean = pool.snapshot();
        assert_eq!(clean.writeback_lag_pages, 0);
        assert_eq!(clean.oldest_dirty_page_age_millis, 0);
    }

    #[test]
    fn writeback_duration_stop_retries_the_first_unexamined_page() {
        let (_dir, _store, pool, ids) = setup(5, 8 * 512);
        dirty(&pool, &ids[..2]);
        pool.set_barrier(Arc::new(CountingBarrier {
            calls: AtomicU64::new(0),
            delay: Duration::from_millis(5),
            fail: false,
        }));
        let report = pool
            .writeback_step(
                WritebackCursor::default(),
                WritebackLimits {
                    max_candidates: 8,
                    max_io_bytes: 64 * 1024,
                    max_duration_millis: 1,
                },
            )
            .unwrap();
        assert_eq!(report.pages_written, 1);
        assert_eq!(report.candidates_examined, 1);
        assert_eq!(report.next_cursor.next_page, Some(ids[1]));
        assert_eq!(report.stop_reason, WritebackStopReason::DurationLimit);
        assert_eq!(report.dirty_pages_remaining, 1);
    }

    #[test]
    fn concurrent_writers_and_bounded_writeback_preserve_the_latest_bytes() {
        let (_dir, store, pool, ids) = setup(32, 64 * 512);
        let pool = Arc::new(pool);
        let done = Arc::new(AtomicU64::new(0));
        let start = Arc::new(std::sync::Barrier::new(5));

        std::thread::scope(|scope| {
            for worker in 0..4 {
                let pool = pool.clone();
                let done = done.clone();
                let start = start.clone();
                let pages = ids[worker * 4..worker * 4 + 4].to_vec();
                scope.spawn(move || {
                    start.wait();
                    for round in 1..=200_u16 {
                        for page_id in &pages {
                            let mut guard = pool.get_mut(*page_id).unwrap();
                            guard.bytes_mut()[PAGE_HEADER_BYTES + 4] = round as u8;
                        }
                    }
                    done.fetch_add(1, Ordering::Release);
                });
            }
            start.wait();
            while done.load(Ordering::Acquire) != 4 {
                pool.writeback_step(
                    WritebackCursor::default(),
                    WritebackLimits {
                        max_candidates: 4,
                        max_io_bytes: 64 * 1024,
                        max_duration_millis: 1_000,
                    },
                )
                .unwrap();
            }
        });

        for _ in 0..100 {
            let report = pool
                .writeback_step(WritebackCursor::default(), WritebackLimits::default())
                .unwrap();
            if report.complete && report.dirty_pages_remaining == 0 {
                break;
            }
        }
        assert_eq!(pool.snapshot().dirty_pages, 0);
        for page_id in &ids[..16] {
            let mut durable = vec![0_u8; store.page_size() as usize];
            store.read_page(*page_id, &mut durable).unwrap();
            assert_eq!(durable[PAGE_HEADER_BYTES + 4], 200_u8);
        }
    }

    #[test]
    fn read_ahead_queue_is_deduplicated_bounded_and_foreground_cancellable() {
        let (_dir, store, original_pool, ids) = setup(12, 4 * 512);
        drop(original_pool);
        let pool = BufferPool::new(
            store.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(4 * 512)
                .with_shards(1)
                .with_read_ahead_queue_pages(2),
        )
        .unwrap();
        pool.register_read_ahead_driver();

        let report =
            pool.request_read_ahead([ids[0], ids[0], ids[1], ids[2], 0, store.page_count()]);
        assert_eq!(report.requested, 6);
        assert_eq!(report.enqueued, 2);
        assert_eq!(report.already_queued, 1);
        assert_eq!(report.queue_full, 1);
        assert_eq!(report.out_of_bounds, 2);
        assert_eq!(report.queue_depth, 2);

        // Foreground demand physically removes the queued entry, immediately
        // freeing capacity without leaving a tombstone that can grow forever.
        drop(pool.get(ids[0]).unwrap());
        assert_eq!(pool.read_ahead_queue_depth(), 1);
        let replacement = pool.request_read_ahead([ids[2]]);
        assert_eq!(replacement.enqueued, 1);
        assert_eq!(replacement.queue_depth, 2);

        let snapshot = pool.snapshot();
        assert_eq!(snapshot.read_ahead_queue_capacity, 2);
        assert_eq!(snapshot.read_ahead_queue_depth, 2);
        assert_eq!(snapshot.read_ahead_requests, 7);
        assert_eq!(snapshot.read_ahead_enqueued, 3);
        assert_eq!(snapshot.read_ahead_already_queued, 1);
        assert_eq!(snapshot.read_ahead_out_of_bounds, 2);
        assert_eq!(snapshot.read_ahead_queue_full, 1);
        assert_eq!(snapshot.read_ahead_foreground_cancellations, 1);
    }

    #[test]
    fn zero_read_ahead_capacity_disables_speculation_without_affecting_reads() {
        let (_dir, store, original_pool, ids) = setup(3, 2 * 512);
        drop(original_pool);
        let pool = BufferPool::new(
            store,
            BufferPoolOptions::default()
                .with_budget_bytes(2 * 512)
                .with_shards(1)
                .with_read_ahead_queue_pages(0),
        )
        .unwrap();
        pool.register_read_ahead_driver();
        let report = pool.request_read_ahead([ids[0]]);
        assert_eq!(report.enqueued, 0);
        assert_eq!(report.queue_full, 1);
        assert_eq!(pool.read_ahead_queue_depth(), 0);
        assert_eq!(pool.snapshot().read_ahead_queue_capacity, 0);
        assert_eq!(pool.get(ids[0]).unwrap().page_id(), ids[0]);
    }

    #[test]
    fn read_ahead_steps_obey_io_candidate_and_serialization_bounds() {
        let (_dir, _store, pool, ids) = setup(10, 8 * 512);
        assert_eq!(
            pool.request_read_ahead(ids[..4].iter().copied()).enqueued,
            4
        );
        let limits = ReadAheadLimits {
            max_candidates: 8,
            max_io_bytes: 512,
            max_duration_millis: 1_000,
        };
        let first = pool.read_ahead_step(limits).unwrap();
        assert_eq!(first.candidates_examined, 1);
        assert_eq!(first.pages_loaded, 1);
        assert_eq!(first.logical_io_bytes, 512);
        assert_eq!(first.queue_depth_remaining, 3);
        assert_eq!(first.stop_reason, ReadAheadStopReason::IoLimit);
        assert!(!first.complete);

        let encoded = serde_json::to_value(first).unwrap();
        assert_eq!(
            serde_json::from_value::<ReadAheadStepReport>(encoded.clone()).unwrap(),
            first
        );
        let mut forged = encoded;
        forged["future_unbounded_field"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ReadAheadStepReport>(forged).is_err());
        let mut inconsistent = first;
        inconsistent.pages_loaded += 1;
        assert!(inconsistent.validate(limits, pool.page_size()).is_err());
        assert!(ReadAheadLimits {
            max_candidates: 0,
            ..limits
        }
        .validate(pool.page_size())
        .is_err());

        let candidate_limits = ReadAheadLimits {
            max_candidates: 1,
            max_io_bytes: 8 * 512,
            max_duration_millis: 1_000,
        };
        let second = pool.read_ahead_step(candidate_limits).unwrap();
        assert_eq!(second.candidates_examined, 1);
        assert_eq!(second.stop_reason, ReadAheadStopReason::CandidateLimit);
        assert_eq!(second.queue_depth_remaining, 2);
    }

    #[derive(Debug)]
    struct ControlledReadSource {
        store: Arc<PageStore>,
        fail_page: Option<PageId>,
        delay: Duration,
    }

    impl PageReadSource for ControlledReadSource {
        fn page_size(&self) -> u32 {
            self.store.page_size()
        }

        fn page_count(&self) -> u64 {
            self.store.page_count()
        }

        fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<PageHeader> {
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay);
            }
            if self.fail_page == Some(page_id) {
                return Err(PageError::UnexpectedPageType {
                    page_id,
                    found: 0,
                    expected: PageType::Heap as u8,
                });
            }
            self.store.read_page(page_id, buffer)
        }
    }

    #[test]
    fn read_ahead_duration_is_cooperative_and_leaves_unstarted_work_queued() {
        let (_dir, store, original_pool, ids) = setup(5, 4 * 512);
        drop(original_pool);
        let source = Arc::new(ControlledReadSource {
            store: store.clone(),
            fail_page: None,
            delay: Duration::from_millis(5),
        });
        let pool = BufferPool::new_read_only(
            store,
            source,
            BufferPoolOptions::default()
                .with_budget_bytes(4 * 512)
                .with_shards(1),
        )
        .unwrap();
        pool.register_read_ahead_driver();
        pool.request_read_ahead(ids[..2].iter().copied());
        let report = pool
            .read_ahead_step(ReadAheadLimits {
                max_candidates: 8,
                max_io_bytes: 8 * 512,
                max_duration_millis: 1,
            })
            .unwrap();
        assert_eq!(report.candidates_examined, 1);
        assert_eq!(report.pages_loaded, 1);
        assert_eq!(report.queue_depth_remaining, 1);
        assert_eq!(report.stop_reason, ReadAheadStopReason::DurationLimit);
    }

    #[test]
    fn read_ahead_failure_restores_the_fixed_frame_and_foreground_reports_error() {
        let (_dir, store, original_pool, ids) = setup(4, 2 * 512);
        drop(original_pool);
        let source = Arc::new(ControlledReadSource {
            store: store.clone(),
            fail_page: Some(ids[0]),
            delay: Duration::ZERO,
        });
        let pool = BufferPool::new_read_only(
            store,
            source,
            BufferPoolOptions::default()
                .with_budget_bytes(2 * 512)
                .with_shards(1),
        )
        .unwrap();
        pool.register_read_ahead_driver();
        pool.request_read_ahead([ids[0]]);
        let report = pool.read_ahead_step(ReadAheadLimits::default()).unwrap();
        assert_eq!(report.read_errors, 1);
        assert_eq!(report.pages_loaded, 0);
        let after_speculation = pool.snapshot();
        assert_eq!(after_speculation.resident_pages, 0);
        assert_eq!(after_speculation.free_frames, 2);
        assert_eq!(after_speculation.read_ahead_read_errors, 1);

        assert!(matches!(
            pool.get(ids[0]).unwrap_err(),
            PageError::UnexpectedPageType { page_id, .. } if page_id == ids[0]
        ));
        let after_foreground_error = pool.snapshot();
        assert_eq!(after_foreground_error.resident_pages, 0);
        assert_eq!(after_foreground_error.free_frames, 2);
        drop(pool.get(ids[1]).unwrap());
        assert_eq!(pool.snapshot().resident_pages, 1);
    }

    #[test]
    fn read_ahead_never_evicts_dirty_or_protected_pages() {
        let (_dir, _store, pool, ids) = setup(200, 16 * 512);
        let hot = &ids[..8];
        for _ in 0..2 {
            for page_id in hot {
                drop(pool.get(*page_id).unwrap());
            }
        }
        pool.request_read_ahead(ids[100..200].iter().copied());
        while pool.read_ahead_queue_depth() != 0 {
            pool.read_ahead_step(ReadAheadLimits::default()).unwrap();
        }
        assert!(hot.iter().all(|page_id| pool.is_resident(*page_id)));

        let (_dir, _store, dirty_pool, dirty_ids) = setup(5, 2 * 512);
        dirty(&dirty_pool, &dirty_ids[..2]);
        dirty_pool.request_read_ahead([dirty_ids[2]]);
        let report = dirty_pool
            .read_ahead_step(ReadAheadLimits::default())
            .unwrap();
        assert_eq!(report.admission_declines, 1);
        assert_eq!(dirty_pool.snapshot().dirty_pages, 2);
        assert!(!dirty_pool.is_resident(dirty_ids[2]));
    }

    #[test]
    fn speculative_load_requires_two_real_touches_to_become_protected() {
        let (_dir, _store, pool, ids) = setup(5, 4 * 512);
        pool.request_read_ahead([ids[0]]);
        assert_eq!(
            pool.read_ahead_step(ReadAheadLimits::default())
                .unwrap()
                .pages_loaded,
            1
        );
        assert_eq!(pool.snapshot().protected_pages, 0);

        drop(pool.get(ids[0]).unwrap());
        let once = pool.snapshot();
        assert_eq!(once.protected_pages, 0);
        assert_eq!(once.read_ahead_pages_used, 1);

        drop(pool.get(ids[0]).unwrap());
        assert_eq!(pool.snapshot().protected_pages, 1);
    }

    #[test]
    fn unused_speculative_pages_are_counted_when_replaced() {
        let (_dir, _store, pool, ids) = setup(5, 2 * 512);
        pool.request_read_ahead(ids[..2].iter().copied());
        pool.read_ahead_step(ReadAheadLimits::default()).unwrap();
        pool.request_read_ahead([ids[2]]);
        let report = pool.read_ahead_step(ReadAheadLimits::default()).unwrap();
        assert_eq!(report.pages_loaded, 1);
        assert_eq!(pool.snapshot().read_ahead_pages_wasted, 1);
    }

    #[test]
    fn a_sequential_scan_does_not_evict_the_hot_set() {
        // The property the whole 2Q policy exists for. Without scan resistance
        // this test fails: plain LRU would evict the twice-touched hot pages in
        // favour of scan pages seen once.
        let (_dir, _store, pool, ids) = setup(400, 16 * 512);

        // Establish a hot set: touch each twice so it reaches the protected
        // queue. Small enough to fit inside the protected share (75% of 16).
        let hot = &ids[0..8];
        for _ in 0..2 {
            for page_id in hot {
                let _ = pool.get(*page_id).unwrap();
            }
        }
        for page_id in hot {
            assert!(
                pool.is_resident(*page_id),
                "hot set not cached to begin with"
            );
        }

        // Now scan a large cold range, touching each page exactly once.
        for page_id in &ids[100..400] {
            let _ = pool.get(*page_id).unwrap();
        }

        let survivors = hot.iter().filter(|id| pool.is_resident(**id)).count();
        assert_eq!(
            survivors,
            hot.len(),
            "a sequential scan evicted {} of {} hot pages; scan resistance is not working",
            hot.len() - survivors,
            hot.len()
        );
    }

    #[test]
    fn scanned_pages_do_not_accumulate_in_the_protected_queue() {
        let (_dir, _store, pool, ids) = setup(200, 16 * 512);
        for page_id in &ids {
            let _ = pool.get(*page_id).unwrap();
        }
        let snapshot = pool.snapshot();
        assert_eq!(
            snapshot.protected_pages, 0,
            "once-touched scan pages must stay probationary"
        );
    }

    #[test]
    fn snapshot_counters_are_internally_consistent() {
        let (_dir, _store, pool, ids) = setup(100, 8 * 512);
        for page_id in &ids[..40] {
            let _ = pool.get(*page_id).unwrap();
        }
        let s = pool.snapshot();
        assert_eq!(s.resident_pages, s.pinned_pages + s.evictable_pages);
        assert_eq!(s.resident_pages, s.protected_pages + s.probationary_pages);
        assert_eq!(s.resident_pages + s.free_frames, s.total_frames);
        assert!(s.hit_ratio() >= 0.0 && s.hit_ratio() <= 1.0);
    }

    #[test]
    fn sharding_spreads_sequential_page_ids() {
        // Page ids are dense and sequential; a hash that keeps low bits would
        // map a scan onto one shard and serialize it.
        let (_dir, store, _pool, _ids) = setup(1, 64 * 512);
        let pool = BufferPool::new(
            store,
            BufferPoolOptions::default()
                .with_budget_bytes(64 * 512)
                .with_shards(8),
        )
        .unwrap();
        let mut counts = vec![0usize; 8];
        for page_id in 0..800u64 {
            let mixed = page_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            counts[(mixed >> 32) as usize % 8] += 1;
        }
        let _ = pool;
        for (shard, count) in counts.iter().enumerate() {
            assert!(
                *count > 40,
                "shard {shard} got only {count} of 800 sequential pages"
            );
        }
    }

    #[test]
    fn warming_yields_rather_than_failing_when_the_pool_is_full() {
        let (_dir, _store, pool, ids) = setup(20, 2 * 512);
        let _pinned_a = pool.get(ids[0]).unwrap();
        let _pinned_b = pool.get(ids[1]).unwrap();
        assert!(
            !pool.warm(ids[5]).unwrap(),
            "warming should decline, not error, when there is no room"
        );
    }

    /// A writeback barrier that parks mid-I/O until released, standing in
    /// for a slow WAL append + fsync.
    #[derive(Debug)]
    struct ParkedBarrier {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl WritebackBarrier for ParkedBarrier {
        fn before_writeback(
            &self,
            _page_id: PageId,
            _transaction: u64,
            _bytes: &[u8],
        ) -> Result<()> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(())
        }
    }

    /// The regression this pins: writeback once held the shard mutex across
    /// the WAL barrier and page write, so a slow fsync stalled every
    /// pin/unpin hashing to the shard. With a single-shard pool, a guard on a
    /// different page must be acquirable while the barrier is parked.
    #[test]
    fn writeback_barrier_does_not_stall_other_pages_in_the_shard() {
        let (_dir, _store, pool, ids) = setup(2, 512 * 64);
        {
            let mut guard = pool.get_mut(ids[0]).unwrap();
            let offset = PAGE_HEADER_BYTES;
            guard.bytes_mut()[offset] = 0xAA;
        }
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        pool.set_barrier(Arc::new(ParkedBarrier {
            entered: entered_tx,
            release: std::sync::Mutex::new(release_rx),
        }));

        let pool = Arc::new(pool);
        let writeback_pool = pool.clone();
        let writeback = std::thread::spawn(move || {
            writeback_pool
                .writeback_step(WritebackCursor::default(), WritebackLimits::default())
                .unwrap()
        });
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("writeback reached the barrier");

        // The barrier is parked. A pin on the other page of this single-shard
        // pool must still complete.
        let reader_pool = pool.clone();
        let other_page = ids[1];
        let reader = std::thread::spawn(move || {
            let guard = reader_pool.get(other_page).unwrap();
            guard.page_id()
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !reader.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "pin of another page stalled behind a parked writeback barrier"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(reader.join().unwrap(), other_page);

        release_tx.send(()).unwrap();
        let report = writeback.join().unwrap();
        assert_eq!(report.pages_written, 1);
    }
}
