//! Heap file: durable variable-size tuples over the buffer pool.
//!
//! Combines [`crate::slotted`] pages, [`crate::overflow`] chains, and a bounded
//! free-space map into the storage primitive Phase 2 of
//! `docs/server-paged-storage-todo.md` builds record storage on.
//!
//! # Large values leave the hot page
//!
//! A value above [`HeapOptions::inline_limit`] is written to an overflow chain
//! and the heap tuple keeps only a 17-byte reference. The roadmap's "keep large
//! payload/attachment ownership outside hot heap pages" applies here: a table
//! with occasional multi-megabyte rows must not push its *small* rows apart,
//! because that inflates every scan and index lookup over the table forever.
//!
//! # The free-space map is bounded on purpose
//!
//! A `page_id -> free_bytes` map for the whole file would grow with the
//! database, which is precisely the property server-paged mode exists to
//! eliminate — a memory-resident structure proportional to total data. Instead
//! this keeps a fixed-size ring of recently-useful pages
//! ([`HeapOptions::free_space_candidates`]). An insert tries the candidates and
//! allocates a fresh page if none fit.
//!
//! The cost is that space in a page which has fallen out of the ring is not
//! found again until a maintenance pass reintroduces it, so the file can grow
//! while free space exists elsewhere. That is a deliberate trade: bounded memory
//! now, with a durable FSM as Phase 2/3 work. It is recorded here rather than
//! discovered later from a file that grows faster than its data.

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::catalog::Catalog;
use crate::error::{PageError, Result};
use crate::overflow;
use crate::page::{PageId, PageType};
use crate::pool::BufferPool;
use crate::slotted::{SlottedPage, SlottedPageRef, TupleLocator};

/// Tag byte distinguishing an inline tuple from an overflow reference.
const TAG_INLINE: u8 = 0;
const TAG_OVERFLOW: u8 = 1;

/// Bytes of an overflow reference tuple: tag + head page id + total length.
const OVERFLOW_REF_BYTES: usize = 1 + 8 + 8;

#[derive(Clone, Copy, Debug)]
pub struct HeapOptions {
    /// Values larger than this go to an overflow chain.
    ///
    /// Defaulted to a quarter of the usable page so that at least four average
    /// tuples share a page; a limit near the full page size would let one wide
    /// row monopolise a page while still counting as "inline".
    pub inline_limit: usize,
    /// How many pages the free-space ring remembers.
    pub free_space_candidates: usize,
    /// Ceiling on a single collected value, for [`HeapFile::get`].
    pub max_value_bytes: usize,
}

impl HeapOptions {
    pub fn for_page_size(page_size: u32) -> Self {
        Self {
            inline_limit: crate::page::usable_bytes(page_size) / 4,
            free_space_candidates: 64,
            max_value_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Counters for heap-level behaviour.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeapSnapshot {
    pub inserts: u64,
    pub overflow_inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub pages_allocated: u64,
    pub compactions: u64,
    /// Inserts that had to allocate because no candidate page had room.
    pub candidate_misses: u64,
    /// Free-space catalog entries that named a heap page without the
    /// advertised room and were repaired on use.
    #[serde(default)]
    pub stale_free_space_entries: u64,
    /// Free-space records found naming a page that is no longer a heap page
    /// (freed by vacuum, possibly reused), dropped instead of written to.
    #[serde(default)]
    pub non_heap_free_space_records: u64,
}

/// How many stale free-space records one insert drops or repairs before it
/// stops consulting the catalog and falls back to the ring.
const STALE_RECORD_ATTEMPTS: usize = 4;

/// Outcome of trying one candidate page for an insert.
enum Placement {
    Placed {
        locator: TupleLocator,
        free_before: usize,
        free_after: usize,
    },
    /// A heap page without room for the tuple; `free` is its real free space.
    NoRoom { free: usize },
    /// Compacted, gaining free space, yet still without room for the tuple.
    CompactedWithoutRoom {
        free_before: usize,
        free_after: usize,
    },
    /// Not a heap page (any more).
    NotHeap,
    /// Could not be pinned right now.
    Unavailable,
}

/// A heap of variable-size tuples.
#[derive(Debug)]
pub struct HeapFile {
    pool: Arc<BufferPool>,
    options: HeapOptions,
    /// Durable page directory and free-space map. When present it is
    /// authoritative and the in-memory ring below is only a fast path.
    catalog: Mutex<Option<Arc<Catalog>>>,
    /// Pages believed to have room, most recently useful first. Bounded.
    candidates: Mutex<VecDeque<PageId>>,
    /// Every heap page in allocation order, so scans have something to walk.
    ///
    /// Bounded-memory caveat: this vector *does* grow with the file (8 bytes
    /// per page, so ~1 MB per 8 GB of data at 8 KiB pages). It is the one
    /// structure here that is not yet constant-size, and Phase 2's durable
    /// page directory replaces it. Recorded rather than hidden.
    pages: Mutex<Vec<PageId>>,
    metrics: Mutex<HeapSnapshot>,
}

impl HeapFile {
    pub fn new(pool: Arc<BufferPool>, options: HeapOptions) -> Self {
        Self {
            pool,
            options,
            catalog: Mutex::new(None),
            candidates: Mutex::new(VecDeque::new()),
            pages: Mutex::new(Vec::new()),
            metrics: Mutex::new(HeapSnapshot::default()),
        }
    }

    pub fn with_defaults(pool: Arc<BufferPool>) -> Self {
        let options = HeapOptions::for_page_size(pool.page_size());
        Self::new(pool, options)
    }

    /// Attach a durable catalog. Once set, free space and the page directory
    /// are persisted rather than reconstructed, so open no longer scans the
    /// file and space is never lost by falling out of the in-memory ring.
    pub fn set_catalog(&self, catalog: Arc<Catalog>) {
        *self.catalog.lock() = Some(catalog);
    }

    fn catalog(&self) -> Option<Arc<Catalog>> {
        self.catalog.lock().clone()
    }

    pub fn snapshot(&self) -> HeapSnapshot {
        *self.metrics.lock()
    }

    /// Heap pages currently known to this handle.
    ///
    /// Reads the durable catalog when one is attached, so a scan does not
    /// depend on an in-memory list rebuilt by walking the file.
    pub fn page_ids(&self) -> Vec<PageId> {
        if let Some(catalog) = self.catalog() {
            if let Ok(pages) = catalog.pages_of_type(PageType::Heap) {
                return pages;
            }
        }
        self.pages.lock().clone()
    }

    /// Adopt an existing set of heap pages, e.g. after reopening a file.
    pub fn adopt_pages(&self, pages: Vec<PageId>) {
        let mut candidates = self.candidates.lock();
        candidates.clear();
        for page_id in pages.iter().rev().take(self.options.free_space_candidates) {
            candidates.push_back(*page_id);
        }
        *self.pages.lock() = pages;
    }

    /// Store a value and return its durable locator.
    pub fn insert(&self, value: &[u8]) -> Result<TupleLocator> {
        if value.is_empty() {
            return Err(PageError::ValueTooLarge {
                page_id: 0,
                len: 0,
                free: 0,
            });
        }

        let tuple = if value.len() > self.options.inline_limit {
            let head = overflow::write_chain_pooled(&self.pool, value)?;
            self.metrics.lock().overflow_inserts += 1;
            encode_overflow_ref(head, value.len() as u64)
        } else {
            let mut tuple = Vec::with_capacity(value.len() + 1);
            tuple.push(TAG_INLINE);
            tuple.extend_from_slice(value);
            tuple
        };

        let locator = self.insert_tuple(&tuple)?;
        self.metrics.lock().inserts += 1;
        Ok(locator)
    }

    /// Drop a page from the heap's in-memory structures — it was freed and
    /// must never again be offered as an insert target. (The durable side —
    /// catalog directory, free-space map, floor — is the caller's job.)
    pub fn forget_page(&self, page_id: PageId) {
        self.pages.lock().retain(|id| *id != page_id);
        self.candidates.lock().retain(|id| *id != page_id);
    }

    /// Place an already-encoded tuple on some page with room.
    ///
    /// # Only the page that receives the tuple is written
    ///
    /// Candidates are examined under an upgradable latch
    /// ([`BufferPool::get_upgradable`]) and the latch is upgraded only on the
    /// page that takes the tuple. A candidate that is rejected is released
    /// clean: it is not dirtied, so the commit does not log an image of it and
    /// writeback does not rewrite it. Taking `get_mut` on every candidate used
    /// to do both, for pages whose bytes had not changed.
    ///
    /// This does not touch durability: a page is logged when it is modified,
    /// and a rejected candidate is not modified. Its last logged image (or its
    /// on-disk copy, if that image predates the redo point) already holds its
    /// bytes, so recovery needs nothing new from it.
    fn insert_tuple(&self, tuple: &[u8]) -> Result<TupleLocator> {
        let needed = SlottedPage::required_space(tuple.len());

        // The durable catalog is authoritative and complete: it knows about
        // every page, not just the recently-touched ones, so it finds space the
        // in-memory ring would have forgotten.
        if let Some(catalog) = self.catalog() {
            // An entry that over-claims is repaired and the search repeated;
            // bounded so a catalog full of such entries (an older store)
            // cannot turn one insert into a walk of the whole map.
            for _ in 0..STALE_RECORD_ATTEMPTS {
                let Some((page_id, recorded)) = catalog.find_free_space_entry(needed)? else {
                    break;
                };
                match self.try_place(page_id, tuple, false)? {
                    Placement::Placed {
                        locator,
                        free_before,
                        free_after,
                    } => {
                        catalog.record_free_space(
                            page_id,
                            Some(recorded),
                            free_before,
                            free_after,
                        )?;
                        return Ok(locator);
                    }
                    Placement::NoRoom { free } => {
                        self.metrics.lock().stale_free_space_entries += 1;
                        catalog.repair_free_space_entry(page_id, recorded, Some(free))?;
                    }
                    Placement::NotHeap => {
                        // Freed by vacuum (possibly reused) while a record
                        // still named it: drop every record of the page.
                        self.metrics.lock().non_heap_free_space_records += 1;
                        catalog.forget_free_space(page_id)?;
                    }
                    Placement::CompactedWithoutRoom { .. } => {
                        unreachable!("the catalog path never compacts")
                    }
                    Placement::Unavailable => break,
                }
            }
        }

        // Then the free-space ring: recently allocated pages and pages that
        // just had tuples deleted, which may fit a tuple the catalog's
        // conservative floor does not advertise, or fit it after compaction.
        let candidates: Vec<PageId> = self.candidates.lock().iter().copied().collect();
        for page_id in candidates {
            match self.try_place(page_id, tuple, true)? {
                Placement::Placed {
                    locator,
                    free_before,
                    free_after,
                } => {
                    // Keep the catalog in step. Leaving it behind made it
                    // over-claim this page, so every later insert of the
                    // same size was offered the page, bounced off it, and
                    // fell back to this ring scan again.
                    if let Some(catalog) = self.catalog() {
                        catalog.record_free_space(page_id, None, free_before, free_after)?;
                    }
                    return Ok(locator);
                }
                Placement::CompactedWithoutRoom {
                    free_before,
                    free_after,
                } => {
                    if let Some(catalog) = self.catalog() {
                        catalog.record_free_space(page_id, None, free_before, free_after)?;
                    }
                }
                Placement::NotHeap => {
                    self.candidates.lock().retain(|id| *id != page_id);
                }
                Placement::NoRoom { .. } | Placement::Unavailable => continue,
            }
        }

        self.metrics.lock().candidate_misses += 1;
        self.allocate_and_insert(tuple)
    }

    /// Insert `tuple` on `page_id` if it is a heap page with room, compacting
    /// first when `allow_compaction` and dead tuples would make the room.
    ///
    /// The page is written (and so dirtied and later logged) only when the
    /// tuple is placed on it; every other outcome leaves it clean.
    fn try_place(
        &self,
        page_id: PageId,
        tuple: &[u8],
        allow_compaction: bool,
    ) -> Result<Placement> {
        let guard = match self.pool.get_upgradable(page_id) {
            Ok(guard) => guard,
            // A full pool is a reason to try another page, not to fail:
            // some other page may already be resident.
            Err(PageError::PoolExhausted { .. }) => return Ok(Placement::Unavailable),
            Err(other) => return Err(other),
        };
        if PageType::from_u8(guard.bytes()[6]) != Some(PageType::Heap) {
            // Freed since it was recorded, possibly reused by another
            // structure: interpreting it as a slotted page would be wrong.
            return Ok(Placement::NotHeap);
        }
        let (free_before, fits, compaction_would_fit) = {
            let page = SlottedPageRef::new(guard.bytes());
            let free = page.free_space();
            let fits = page.can_fit(tuple.len());
            // Only compact when it is certain to make the room: compaction
            // rewrites the page, so a compaction that then does not fit (or
            // reclaims nothing because an earlier one already did) would
            // dirty and log the page for no insert.
            let compaction_would_fit = !fits
                && allow_compaction
                && page.free_space_after_compaction() > free
                && page.can_fit_after_compaction(tuple.len());
            (free, fits, compaction_would_fit)
        };
        if !fits && !compaction_would_fit {
            return Ok(Placement::NoRoom { free: free_before });
        }
        let page_size = self.pool.page_size();
        let mut guard = guard.upgrade();
        let mut heap = SlottedPage::new(guard.bytes_mut());
        if compaction_would_fit {
            // Reclaimable space from deleted tuples can be recovered in place
            // rather than growing the file. Compaction rewrites the page, so
            // the page is modified (and logged) even if the tuple then does
            // not fit.
            heap.compact(page_size)?;
            self.metrics.lock().compactions += 1;
            if !heap.can_fit(tuple.len()) {
                // Not expected (the fit was computed above), but the page
                // was rewritten with more free space, so the catalog must
                // still hear about it.
                return Ok(Placement::CompactedWithoutRoom {
                    free_before,
                    free_after: heap.free_space(),
                });
            }
        }
        let locator = heap.insert(tuple)?;
        Ok(Placement::Placed {
            locator,
            free_before,
            free_after: heap.free_space(),
        })
    }

    fn allocate_and_insert(&self, tuple: &[u8]) -> Result<TupleLocator> {
        let page_size = self.pool.page_size();
        let page_id = self.pool.store().allocate(PageType::Heap)?;
        // A reused page id must mint slot generations above every generation
        // any earlier heap tenancy handed out — the floor the catalog kept
        // when the page was freed. Fresh ids have no floor and start at 1.
        let floor = match self.catalog() {
            Some(catalog) => catalog.page_generation_floor(page_id)?,
            None => 0,
        };
        {
            let mut guard = self.pool.get_mut(page_id)?;
            let mut heap =
                SlottedPage::init_with_floor(guard.bytes_mut(), page_id, page_size, floor);
            let locator = heap.insert(tuple)?;

            let free_after = heap.free_space();
            self.pages.lock().push(page_id);
            if let Some(catalog) = self.catalog() {
                catalog.register_page(page_id, PageType::Heap, free_after)?;
            }
            let mut candidates = self.candidates.lock();
            candidates.push_front(page_id);
            while candidates.len() > self.options.free_space_candidates {
                candidates.pop_back();
            }
            self.metrics.lock().pages_allocated += 1;
            Ok(locator)
        }
    }

    /// Read a value, collecting it into memory.
    ///
    /// Bounded by [`HeapOptions::max_value_bytes`]. For values that may be
    /// arbitrarily large, use [`Self::read_streaming`].
    pub fn get(&self, locator: TupleLocator) -> Result<Vec<u8>> {
        let guard = self.pool.get(locator.page_id)?;
        let heap = SlottedPageRef::new(guard.bytes());
        let tuple = heap.get(locator)?;
        match decode_tuple(tuple)? {
            Tuple::Inline(bytes) => Ok(bytes.to_vec()),
            Tuple::Overflow { head, len } => {
                if len > self.options.max_value_bytes as u64 {
                    return Err(PageError::ValueTooLarge {
                        page_id: locator.page_id,
                        len: len as usize,
                        free: self.options.max_value_bytes,
                    });
                }
                // The page guard is released before the chain walk: holding a
                // pin across an arbitrarily long read would let one wide value
                // keep a frame out of circulation for the whole read.
                drop(guard);
                overflow::read_chain_pooled(&self.pool, head, self.options.max_value_bytes)
            }
        }
    }

    /// Read at most `max_bytes` from the beginning of a value.
    ///
    /// Maintenance frequently needs only a fixed header. Calling [`Self::get`]
    /// for that job materializes the complete overflow value first, so one
    /// wide row can consume tens of MiB merely to inspect thirty MVCC bytes.
    /// This path holds one overflow page at a time and allocates no more than
    /// the requested prefix.
    pub fn read_prefix(&self, locator: TupleLocator, max_bytes: usize) -> Result<Vec<u8>> {
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let guard = self.pool.get(locator.page_id)?;
        let heap = SlottedPageRef::new(guard.bytes());
        match decode_tuple(heap.get(locator)?)? {
            Tuple::Inline(bytes) => Ok(bytes[..bytes.len().min(max_bytes)].to_vec()),
            Tuple::Overflow { head, len } => {
                let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
                let wanted = usize::try_from(len.min(max_bytes_u64)).map_err(|_| {
                    PageError::ValueTooLarge {
                        page_id: locator.page_id,
                        len: usize::MAX,
                        free: max_bytes,
                    }
                })?;
                drop(guard);
                let mut prefix = vec![0_u8; wanted];
                let mut reader = overflow::PooledOverflowReader::new(&self.pool, head);
                reader
                    .read_exact(&mut prefix)
                    .map_err(|error| PageError::io(self.pool.store().path(), error))?;
                Ok(prefix)
            }
        }
    }

    /// Logical value length and overflow pages owned by one tuple.
    ///
    /// The metadata lives in the heap reference, so this never walks or
    /// materializes an overflow chain. Vacuum uses it to reserve its byte
    /// budget before reading a header or releasing a large value.
    pub(crate) fn value_storage(&self, locator: TupleLocator) -> Result<(u64, u64)> {
        let guard = self.pool.get(locator.page_id)?;
        let heap = SlottedPageRef::new(guard.bytes());
        match decode_tuple(heap.get(locator)?)? {
            Tuple::Inline(bytes) => Ok((bytes.len() as u64, 0)),
            Tuple::Overflow { len, .. } => {
                let capacity = overflow::payload_capacity(self.pool.page_size()) as u64;
                Ok((len, len.div_ceil(capacity)))
            }
        }
    }

    /// Byte length of a value without reading it.
    pub fn value_len(&self, locator: TupleLocator) -> Result<u64> {
        let guard = self.pool.get(locator.page_id)?;
        let heap = SlottedPageRef::new(guard.bytes());
        match decode_tuple(heap.get(locator)?)? {
            Tuple::Inline(bytes) => Ok(bytes.len() as u64),
            Tuple::Overflow { len, .. } => Ok(len),
        }
    }

    /// Call `consume` with a streaming reader for an overflow value, or the
    /// inline bytes directly. Never materializes an unbounded value.
    pub fn read_streaming<T>(
        &self,
        locator: TupleLocator,
        consume: impl FnOnce(&mut dyn std::io::Read) -> std::io::Result<T>,
    ) -> Result<T> {
        let guard = self.pool.get(locator.page_id)?;
        let heap = SlottedPageRef::new(guard.bytes());
        match decode_tuple(heap.get(locator)?)? {
            Tuple::Inline(bytes) => {
                let mut cursor = std::io::Cursor::new(bytes.to_vec());
                drop(guard);
                consume(&mut cursor).map_err(|e| PageError::io(self.pool.store().path(), e))
            }
            Tuple::Overflow { head, .. } => {
                drop(guard);
                let mut reader = overflow::PooledOverflowReader::new(&self.pool, head);
                consume(&mut reader).map_err(|e| PageError::io(self.pool.store().path(), e))
            }
        }
    }

    /// Replace a value, keeping the locator valid where possible.
    ///
    /// Returns the (possibly new) locator: a value that no longer fits its page
    /// moves, and callers must persist the new locator. Reporting that honestly
    /// is better than silently leaving a forwarding pointer, which would make
    /// every later read pay an extra page fetch forever.
    pub fn update(&self, locator: TupleLocator, value: &[u8]) -> Result<TupleLocator> {
        if value.is_empty() {
            return Err(PageError::ValueTooLarge {
                page_id: locator.page_id,
                len: 0,
                free: 0,
            });
        }

        // Release any overflow chain the old value owned.
        let previous = {
            let guard = self.pool.get(locator.page_id)?;
            let heap = SlottedPageRef::new(guard.bytes());
            decode_tuple(heap.get(locator)?)?.owned()
        };

        let tuple = if value.len() > self.options.inline_limit {
            let head = overflow::write_chain_pooled(&self.pool, value)?;
            self.metrics.lock().overflow_inserts += 1;
            encode_overflow_ref(head, value.len() as u64)
        } else {
            let mut tuple = Vec::with_capacity(value.len() + 1);
            tuple.push(TAG_INLINE);
            tuple.extend_from_slice(value);
            tuple
        };

        let (result, free_before, free_after) = {
            let mut guard = self.pool.get_mut(locator.page_id)?;
            let mut heap = SlottedPage::new(guard.bytes_mut());
            let before = heap.free_space();
            let result = match heap.update(locator, &tuple) {
                Ok(()) => Ok(locator),
                Err(PageError::ValueTooLarge { .. }) => {
                    // No room here: remove and place elsewhere.
                    heap.delete(locator)?;
                    Err(())
                }
                Err(other) => return Err(other),
            };
            (result, before, heap.free_space())
        };
        // A value that grew in place consumed free space the catalog may be
        // advertising. Same-size rewrites (the `xmax` stamp) change nothing
        // and skip the catalog entirely.
        if free_before != free_after {
            if let Some(catalog) = self.catalog() {
                catalog.record_free_space(locator.page_id, None, free_before, free_after)?;
            }
        }

        let new_locator = match result {
            Ok(locator) => locator,
            Err(()) => self.insert_tuple(&tuple)?,
        };

        if let OwnedTuple::Overflow { head } = previous {
            overflow::free_chain_pooled(&self.pool, head)?;
        }
        self.metrics.lock().updates += 1;
        Ok(new_locator)
    }

    /// Remove a value and release any overflow pages it owned.
    pub fn delete(&self, locator: TupleLocator) -> Result<()> {
        self.delete_inner(locator, true)
    }

    /// Vacuum already owns a streaming catalog cursor. Mutating that same B+
    /// tree's free-space namespace while its leaf cursor is live can split the
    /// boundary leaf and make the cursor skip a directory entry. The caller
    /// therefore records the final free-space update after dropping its scan.
    pub(crate) fn delete_for_vacuum(&self, locator: TupleLocator) -> Result<()> {
        self.delete_inner(locator, false)
    }

    fn delete_inner(&self, locator: TupleLocator, update_catalog: bool) -> Result<()> {
        let owned = {
            let guard = self.pool.get(locator.page_id)?;
            let heap = SlottedPageRef::new(guard.bytes());
            decode_tuple(heap.get(locator)?)?.owned()
        };
        let (free_before, free_after) = {
            let mut guard = self.pool.get_mut(locator.page_id)?;
            let mut heap = SlottedPage::new(guard.bytes_mut());
            let before = heap.free_space();
            heap.delete(locator)?;
            (before, heap.free_space())
        };
        if let OwnedTuple::Overflow { head } = owned {
            overflow::free_chain_pooled(&self.pool, head)?;
        }

        if update_catalog {
            if let Some(catalog) = self.catalog() {
                // The record is found from `free_before` and moved only if
                // the hysteresis band requires it, so a delete that frees a
                // few bytes leaves the catalog untouched.
                catalog.record_free_space(locator.page_id, None, free_before, free_after)?;
            }
        }

        // A page with room is worth remembering.
        let mut candidates = self.candidates.lock();
        if !candidates.contains(&locator.page_id) {
            candidates.push_front(locator.page_id);
            while candidates.len() > self.options.free_space_candidates {
                candidates.pop_back();
            }
        }
        self.metrics.lock().deletes += 1;
        Ok(())
    }

    /// Locators for every live tuple on one page.
    pub fn page_locators(&self, page_id: PageId) -> Result<Vec<TupleLocator>> {
        let guard = self.pool.get(page_id)?;
        Ok(SlottedPageRef::new(guard.bytes()).locators())
    }

    /// A cursor over every live tuple, pinning one page at a time.
    ///
    /// This is the bounded scan Phase 5 requires: memory is one page plus the
    /// caller's own use of each value, whatever the heap's size.
    pub fn scan(&self) -> HeapScan<'_> {
        HeapScan {
            heap: self,
            catalog: self.catalog(),
            next_catalog_page: None,
            catalog_complete: false,
            legacy_page_index: 0,
            locators: Vec::new(),
            locator_index: 0,
        }
    }

    /// Write every dirty page back and fsync.
    pub fn flush(&self) -> Result<()> {
        self.pool.flush_all()?;
        Ok(())
    }
}

/// Bounded cursor over a heap.
pub struct HeapScan<'a> {
    heap: &'a HeapFile,
    catalog: Option<Arc<Catalog>>,
    next_catalog_page: Option<PageId>,
    catalog_complete: bool,
    legacy_page_index: usize,
    locators: Vec<TupleLocator>,
    locator_index: usize,
}

impl Iterator for HeapScan<'_> {
    type Item = Result<(TupleLocator, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.locator_index < self.locators.len() {
                let locator = self.locators[self.locator_index];
                self.locator_index += 1;
                return Some(self.heap.get(locator).map(|value| (locator, value)));
            }
            let page_id = if let Some(catalog) = &self.catalog {
                if self.catalog_complete {
                    return None;
                }
                match catalog.next_page_of_type(PageType::Heap, self.next_catalog_page) {
                    Ok(Some(page_id)) => {
                        match page_id.checked_add(1) {
                            Some(next) => self.next_catalog_page = Some(next),
                            None => self.catalog_complete = true,
                        }
                        page_id
                    }
                    Ok(None) => {
                        self.catalog_complete = true;
                        return None;
                    }
                    Err(error) => {
                        self.catalog_complete = true;
                        return Some(Err(error));
                    }
                }
            } else {
                let page_id = self.heap.pages.lock().get(self.legacy_page_index).copied();
                let Some(page_id) = page_id else {
                    return None;
                };
                self.legacy_page_index += 1;
                page_id
            };
            match self.heap.page_locators(page_id) {
                Ok(locators) => {
                    self.locators = locators;
                    self.locator_index = 0;
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

pub(crate) enum Tuple<'a> {
    Inline(&'a [u8]),
    Overflow { head: PageId, len: u64 },
}

/// The part of a tuple that owns pages elsewhere, decoupled from the borrow.
enum OwnedTuple {
    Inline,
    Overflow { head: PageId },
}

impl Tuple<'_> {
    fn owned(&self) -> OwnedTuple {
        match self {
            Tuple::Inline(_) => OwnedTuple::Inline,
            Tuple::Overflow { head, .. } => OwnedTuple::Overflow { head: *head },
        }
    }
}

fn encode_overflow_ref(head: PageId, len: u64) -> Vec<u8> {
    let mut tuple = Vec::with_capacity(OVERFLOW_REF_BYTES);
    tuple.push(TAG_OVERFLOW);
    tuple.extend_from_slice(&head.to_le_bytes());
    tuple.extend_from_slice(&len.to_le_bytes());
    tuple
}

pub(crate) fn decode_tuple(tuple: &[u8]) -> Result<Tuple<'_>> {
    match tuple.first() {
        Some(&TAG_INLINE) => Ok(Tuple::Inline(&tuple[1..])),
        Some(&TAG_OVERFLOW) if tuple.len() >= OVERFLOW_REF_BYTES => Ok(Tuple::Overflow {
            head: u64::from_le_bytes(tuple[1..9].try_into().unwrap()),
            len: u64::from_le_bytes(tuple[9..17].try_into().unwrap()),
        }),
        _ => Err(PageError::ShortRead {
            page_id: 0,
            got: tuple.len(),
            want: OVERFLOW_REF_BYTES,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{PageStore, PageStoreOptions};
    use crate::pool::BufferPoolOptions;
    use tempfile::TempDir;

    fn heap(dir: &TempDir, page_size: u32, budget: u64) -> (Arc<PageStore>, HeapFile) {
        let store = Arc::new(
            PageStore::open(
                dir.path().join("h.pages"),
                PageStoreOptions::default()
                    .with_page_size(page_size)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default()
                    .with_budget_bytes(budget)
                    .with_shards(2),
            )
            .unwrap(),
        );
        (store, HeapFile::with_defaults(pool))
    }

    fn payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| ((i as u8).wrapping_add(seed)) % 251)
            .collect()
    }

    #[test]
    fn small_values_round_trip() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 64 * 1024);
        let a = heap.insert(b"alpha").unwrap();
        let b = heap.insert(b"beta").unwrap();
        assert_eq!(heap.get(a).unwrap(), b"alpha");
        assert_eq!(heap.get(b).unwrap(), b"beta");
        assert_eq!(heap.snapshot().inserts, 2);
        assert_eq!(heap.snapshot().overflow_inserts, 0);
    }

    #[test]
    fn large_values_go_to_overflow_and_still_round_trip() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 64 * 1024);
        let big = payload(20_000, 3);
        let locator = heap.insert(&big).unwrap();

        assert_eq!(heap.snapshot().overflow_inserts, 1);
        assert_eq!(heap.get(locator).unwrap(), big);
        assert_eq!(heap.value_len(locator).unwrap(), 20_000);
    }

    #[test]
    fn a_wide_row_does_not_push_small_rows_off_their_page() {
        // The reason for the inline limit. If the wide value were stored inline
        // it would consume the page and the small rows would scatter.
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 64 * 1024);

        let wide = heap.insert(&payload(50_000, 1)).unwrap();
        let mut small = Vec::new();
        for index in 0..8 {
            small.push(heap.insert(format!("small-{index}").as_bytes()).unwrap());
        }

        // All the small rows fit alongside the wide row's 17-byte reference.
        let distinct_pages: std::collections::BTreeSet<_> =
            small.iter().map(|l| l.page_id).collect();
        assert_eq!(
            distinct_pages.len(),
            1,
            "small rows scattered across pages: {distinct_pages:?}"
        );
        assert_eq!(heap.get(wide).unwrap().len(), 50_000);
    }

    #[test]
    fn streaming_read_does_not_materialize_the_value() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 512, 32 * 1024);
        let big = payload(200_000, 7);
        let locator = heap.insert(&big).unwrap();

        let mut total = 0usize;
        let mut checksum = 0u64;
        heap.read_streaming(locator, |reader| {
            let mut chunk = [0u8; 64];
            loop {
                let read = reader.read(&mut chunk)?;
                if read == 0 {
                    break;
                }
                total += read;
                for byte in &chunk[..read] {
                    checksum = checksum.wrapping_mul(31).wrapping_add(*byte as u64);
                }
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(total, big.len());
        let expected = big
            .iter()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(*b as u64));
        assert_eq!(checksum, expected);
    }

    #[test]
    fn streaming_works_for_inline_values_too() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 32 * 1024);
        let locator = heap.insert(b"tiny inline value").unwrap();
        let mut out = Vec::new();
        heap.read_streaming(locator, |reader| {
            std::io::Read::read_to_end(reader, &mut out)
        })
        .unwrap();
        assert_eq!(out, b"tiny inline value");
    }

    #[test]
    fn update_within_a_page_keeps_the_locator() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 32 * 1024);
        let locator = heap.insert(b"original").unwrap();
        let same = heap.update(locator, b"changed!").unwrap();
        assert_eq!(same, locator);
        assert_eq!(heap.get(locator).unwrap(), b"changed!");
    }

    #[test]
    fn deleting_an_overflow_value_releases_its_chain() {
        let dir = TempDir::new().unwrap();
        let (store, heap) = heap(&dir, 512, 32 * 1024);
        let big = payload(30_000, 11);

        let free_before = store.free_page_count();
        let locator = heap.insert(&big).unwrap();
        heap.delete(locator).unwrap();

        assert!(
            store.free_page_count() > free_before,
            "overflow chain leaked on delete"
        );
        assert!(heap.get(locator).is_err());
    }

    #[test]
    fn updating_an_overflow_value_releases_the_old_chain() {
        let dir = TempDir::new().unwrap();
        let (store, heap) = heap(&dir, 512, 32 * 1024);
        let locator = heap.insert(&payload(30_000, 2)).unwrap();
        let freed_before = store.free_page_count();

        let replacement = payload(25_000, 5);
        let new_locator = heap.update(locator, &replacement).unwrap();

        assert!(
            store.free_page_count() > freed_before,
            "old overflow chain leaked on update"
        );
        assert_eq!(heap.get(new_locator).unwrap(), replacement);
    }

    #[test]
    fn a_value_shrinking_out_of_overflow_still_reads_back() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 32 * 1024);
        let locator = heap.insert(&payload(40_000, 9)).unwrap();
        let new_locator = heap.update(locator, b"now tiny").unwrap();
        assert_eq!(heap.get(new_locator).unwrap(), b"now tiny");
    }

    #[test]
    fn deleted_space_is_reused_rather_than_growing_the_file() {
        let dir = TempDir::new().unwrap();
        let (store, heap) = heap(&dir, 1024, 64 * 1024);

        let mut locators = Vec::new();
        for index in 0..40 {
            locators.push(heap.insert(&payload(80, index as u8)).unwrap());
        }
        let pages_after_fill = store.page_count();
        for locator in &locators {
            heap.delete(*locator).unwrap();
        }
        for index in 0..40 {
            heap.insert(&payload(80, index as u8)).unwrap();
        }

        assert_eq!(
            store.page_count(),
            pages_after_fill,
            "refilling after delete grew the file instead of reusing space"
        );
    }

    #[test]
    fn scanning_visits_every_live_tuple_and_no_deleted_one() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 64 * 1024);

        let mut expected = std::collections::BTreeMap::new();
        let mut locators = Vec::new();
        for index in 0..60usize {
            let value = format!("row-{index:04}").into_bytes();
            let locator = heap.insert(&value).unwrap();
            locators.push(locator);
            expected.insert(locator, value);
        }
        for locator in locators.iter().step_by(3) {
            heap.delete(*locator).unwrap();
            expected.remove(locator);
        }

        let mut seen = std::collections::BTreeMap::new();
        for entry in heap.scan() {
            let (locator, value) = entry.unwrap();
            seen.insert(locator, value);
        }
        assert_eq!(seen, expected);
    }

    #[test]
    fn a_scan_stays_within_the_buffer_pool_budget() {
        // Bounded scan: 8 frames, far more data than that.
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            PageStore::open(
                dir.path().join("h.pages"),
                PageStoreOptions::default()
                    .with_page_size(512)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default()
                    .with_budget_bytes(8 * 512)
                    .with_shards(1),
            )
            .unwrap(),
        );
        let heap = HeapFile::with_defaults(pool.clone());

        for index in 0..500usize {
            heap.insert(format!("row-{index:05}-padding").as_bytes())
                .unwrap();
        }

        let mut count = 0;
        for entry in heap.scan() {
            entry.unwrap();
            count += 1;
            let snapshot = pool.snapshot();
            assert!(
                snapshot.resident_bytes <= snapshot.budget_bytes,
                "scan exceeded the pool budget at row {count}"
            );
        }
        assert_eq!(count, 500);
    }

    #[test]
    fn values_survive_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("h.pages");
        let (locators, pages) = {
            let store = Arc::new(
                PageStore::open(
                    &path,
                    PageStoreOptions::default()
                        .with_page_size(1024)
                        .with_fsync(false),
                )
                .unwrap(),
            );
            let pool = Arc::new(
                BufferPool::new(
                    store.clone(),
                    BufferPoolOptions::default().with_budget_bytes(64 * 1024),
                )
                .unwrap(),
            );
            let heap = HeapFile::with_defaults(pool);
            let mut locators = Vec::new();
            for index in 0..50usize {
                locators.push(
                    heap.insert(format!("durable-{index:04}").as_bytes())
                        .unwrap(),
                );
            }
            // One overflow value too.
            locators.push(heap.insert(&payload(30_000, 4)).unwrap());
            heap.flush().unwrap();
            store.flush().unwrap();
            (locators, heap.page_ids())
        };

        let store = Arc::new(
            PageStore::open(
                &path,
                PageStoreOptions::default()
                    .with_page_size(1024)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default().with_budget_bytes(64 * 1024),
            )
            .unwrap(),
        );
        let heap = HeapFile::with_defaults(pool);
        heap.adopt_pages(pages);

        for (index, locator) in locators.iter().enumerate().take(50) {
            assert_eq!(
                heap.get(*locator).unwrap(),
                format!("durable-{index:04}").as_bytes()
            );
        }
        assert_eq!(
            heap.get(*locators.last().unwrap()).unwrap(),
            payload(30_000, 4)
        );
    }

    #[test]
    fn a_stale_locator_never_resolves_to_a_recycled_row() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 1024, 32 * 1024);
        let old = heap.insert(b"the original row").unwrap();
        heap.delete(old).unwrap();

        // Refill the page; one of these will take the recycled slot.
        for index in 0..20 {
            heap.insert(format!("replacement-{index}").as_bytes())
                .unwrap();
        }

        match heap.get(old) {
            Err(PageError::StaleLocator { .. }) | Err(PageError::DeadSlot { .. }) => {}
            Ok(value) => panic!(
                "stale locator resolved to {:?} instead of failing",
                String::from_utf8_lossy(&value)
            ),
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn get_refuses_a_value_above_the_configured_ceiling() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            PageStore::open(
                dir.path().join("h.pages"),
                PageStoreOptions::default()
                    .with_page_size(512)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default().with_budget_bytes(64 * 512),
            )
            .unwrap(),
        );
        let mut options = HeapOptions::for_page_size(512);
        options.max_value_bytes = 1_000;
        let heap = HeapFile::new(pool, options);

        let locator = heap.insert(&payload(10_000, 1)).unwrap();
        assert!(matches!(
            heap.get(locator),
            Err(PageError::ValueTooLarge { .. })
        ));
        assert_eq!(
            heap.read_prefix(locator, 30).unwrap(),
            payload(10_000, 1)[..30],
            "a fixed header read must not inherit the full-value ceiling"
        );
        assert_eq!(heap.value_storage(locator).unwrap(), (10_000, 23));
        // But streaming it is still fine — that is the point of the ceiling.
        let mut count = 0usize;
        heap.read_streaming(locator, |reader| {
            let mut sink = Vec::new();
            count = reader.read_to_end(&mut sink)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 10_000);
    }

    /// A heap with a durable catalog attached, as the paged store runs it.
    fn cataloged_heap(
        dir: &TempDir,
        page_size: u32,
        budget: u64,
    ) -> (Arc<PageStore>, Arc<BufferPool>, Arc<Catalog>, HeapFile) {
        let store = Arc::new(
            PageStore::open(
                dir.path().join("h.pages"),
                PageStoreOptions::default()
                    .with_page_size(page_size)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default()
                    .with_budget_bytes(budget)
                    .with_shards(2),
            )
            .unwrap(),
        );
        let catalog = Arc::new(Catalog::create(pool.clone()).unwrap());
        let heap = HeapFile::with_defaults(pool.clone());
        heap.set_catalog(catalog.clone());
        (store, pool, catalog, heap)
    }

    fn page_type_of(pool: &BufferPool, page_id: PageId) -> Option<PageType> {
        PageType::from_u8(pool.get(page_id).unwrap().bytes()[6])
    }

    fn heap_pages_needing_log(pool: &BufferPool) -> Vec<PageId> {
        pool.take_pages_needing_log()
            .into_iter()
            .filter(|page| page_type_of(pool, *page) == Some(PageType::Heap))
            .collect()
    }

    #[test]
    fn an_insert_that_probes_full_pages_dirties_only_the_page_it_writes() {
        let dir = TempDir::new().unwrap();
        let (_store, pool, _catalog, heap) = cataloged_heap(&dir, 4096, 4 * 1024 * 1024);

        // Four 900-byte tuples per page leave each page about 400 bytes
        // free, and the free-space ring full of such pages.
        for index in 0..60 {
            heap.insert(&payload(900, index as u8)).unwrap();
        }
        let full_pages = heap.page_ids();
        assert!(full_pages.len() >= 10, "want many full pages");
        pool.take_pages_needing_log();
        pool.flush_all().unwrap();
        assert!(pool.dirty_page_ids().is_empty());

        // Fits on no existing page (but stays inline): every candidate is
        // probed and rejected, and a fresh page takes it.
        let placed = heap.insert(&payload(950, 7)).unwrap();
        assert!(!full_pages.contains(&placed.page_id));
        assert_eq!(
            heap_pages_needing_log(&pool),
            vec![placed.page_id],
            "a rejected candidate was queued for the WAL"
        );
        let dirty_heap: Vec<PageId> = pool
            .dirty_page_ids()
            .into_iter()
            .filter(|page| page_type_of(&pool, *page) == Some(PageType::Heap))
            .collect();
        assert_eq!(
            dirty_heap,
            vec![placed.page_id],
            "a rejected candidate was dirtied"
        );

        // A small tuple: exactly the one page that takes it.
        pool.flush_all().unwrap();
        let small = heap.insert(&payload(40, 9)).unwrap();
        assert_eq!(heap_pages_needing_log(&pool), vec![small.page_id]);
        assert_eq!(heap.snapshot().stale_free_space_entries, 0);
    }

    #[test]
    fn an_insert_without_a_catalog_also_leaves_rejected_candidates_clean() {
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 4096, 4 * 1024 * 1024);
        for index in 0..60 {
            heap.insert(&payload(900, index as u8)).unwrap();
        }
        heap.pool.take_pages_needing_log();
        let placed = heap.insert(&payload(950, 7)).unwrap();
        assert_eq!(heap.pool.take_pages_needing_log(), vec![placed.page_id]);
    }

    #[test]
    fn a_compaction_that_cannot_make_room_is_not_attempted() {
        // Dead slots keep their length after a compaction, so counting dead
        // bytes as reclaimable made the ring rewrite an already compacted
        // page, unchanged, on a later miss.
        let dir = TempDir::new().unwrap();
        let (_store, heap) = heap(&dir, 4096, 4 * 1024 * 1024);
        let small: Vec<_> = (0..3)
            .map(|index| heap.insert(&payload(10, index)).unwrap())
            .collect();
        let big: Vec<_> = (0..4)
            .map(|index| heap.insert(&payload(900, index)).unwrap())
            .collect();
        let page = big[3].page_id;
        assert!(small.iter().chain(&big).all(|l| l.page_id == page));
        for locator in small.iter().chain(&big[..1]) {
            heap.delete(*locator).unwrap();
        }
        // Needs the compaction, and gets it.
        assert_eq!(heap.insert(&payload(1000, 5)).unwrap().page_id, page);
        assert_eq!(heap.snapshot().compactions, 1);

        // Three dead slots remain, one still carrying 900 bytes of length
        // that the compaction already returned.
        heap.pool.take_pages_needing_log();
        let placed = heap.insert(&payload(900, 6)).unwrap();
        assert_ne!(placed.page_id, page);
        assert_eq!(heap.snapshot().compactions, 1, "a useless compaction ran");
        assert_eq!(heap.pool.take_pages_needing_log(), vec![placed.page_id]);
    }

    #[test]
    fn a_catalog_entry_naming_a_page_without_room_is_repaired_on_use() {
        let dir = TempDir::new().unwrap();
        let (_store, pool, catalog, heap) = cataloged_heap(&dir, 4096, 4 * 1024 * 1024);
        let page = heap.insert(&payload(900, 1)).unwrap().page_id;
        for index in 0..3 {
            assert_eq!(heap.insert(&payload(900, index)).unwrap().page_id, page);
        }
        // Forge an over-claim, as an older binary's stale entry would be.
        let real = catalog.recorded_bucket(page, 0).unwrap().unwrap();
        catalog.repair_free_space_entry(page, real, None).unwrap();
        catalog.record_free_space(page, None, 4000, 4000).unwrap();
        assert!(catalog.recorded_bucket(page, 4000).unwrap().unwrap() > real);
        pool.take_pages_needing_log();

        // 900 bytes do not fit the ~400 the page really has.
        let placed = heap.insert(&payload(900, 2)).unwrap();
        assert_ne!(placed.page_id, page);
        assert!(
            !pool.take_pages_needing_log().contains(&page),
            "the over-claimed page was written although nothing fit"
        );
        assert_eq!(heap.snapshot().stale_free_space_entries, 1);
        // Recorded honestly now, and the next insert does not trip on it.
        let free = SlottedPageRef::new(pool.get(page).unwrap().bytes()).free_space();
        let recorded = catalog.free_space_buckets(page).unwrap();
        assert_eq!(recorded.len(), 1);
        assert!(recorded[0] as usize * (4096 / crate::catalog::FREE_BUCKETS as usize) <= free);
        heap.insert(&payload(900, 3)).unwrap();
        assert_eq!(heap.snapshot().stale_free_space_entries, 1);
    }

    #[test]
    fn a_catalog_entry_naming_a_non_heap_page_is_dropped_not_written() {
        let dir = TempDir::new().unwrap();
        let (store, pool, catalog, heap) = cataloged_heap(&dir, 4096, 4 * 1024 * 1024);
        for index in 0..4 {
            heap.insert(&payload(900, index)).unwrap();
        }
        // A page that belongs to another structure, wrongly advertised.
        let foreign = store.allocate(PageType::Overflow).unwrap();
        {
            let mut guard = pool.get_mut(foreign).unwrap();
            let header = crate::page::PageHeader::new(foreign, PageType::Overflow, 4096);
            header.encode(guard.bytes_mut());
        }
        catalog
            .record_free_space(foreign, None, 4000, 4000)
            .unwrap();
        pool.flush_all().unwrap();
        let before = pool.get(foreign).unwrap().bytes().to_vec();
        pool.take_pages_needing_log();

        let placed = heap.insert(&payload(900, 9)).unwrap();
        assert_ne!(placed.page_id, foreign);
        assert!(!pool.take_pages_needing_log().contains(&foreign));
        assert_eq!(pool.get(foreign).unwrap().bytes(), &before[..]);
        assert!(catalog.free_space_buckets(foreign).unwrap().is_empty());
        assert_eq!(heap.snapshot().non_heap_free_space_records, 1);
    }

    /// The free-space map's contract after every operation of a churn: each
    /// heap page has exactly one record, the record never claims more room
    /// than the page has, and it lags the exact class by at most the slack.
    fn assert_free_space_contract(pool: &BufferPool, catalog: &Catalog, heap: &HeapFile) {
        use crate::catalog::{FREE_BUCKETS, FREE_SPACE_SLACK};
        let span = (pool.page_size() as usize / FREE_BUCKETS as usize).max(1);
        for page in heap.page_ids() {
            let free = SlottedPageRef::new(pool.get(page).unwrap().bytes()).free_space();
            let exact = (free / span).min(FREE_BUCKETS as usize - 1);
            let recorded = catalog.free_space_buckets(page).unwrap();
            assert_eq!(recorded.len(), 1, "page {page} has records {recorded:?}");
            let r = recorded[0] as usize;
            assert!(
                r <= exact,
                "page {page}: record {r} over-claims exact class {exact}"
            );
            assert!(
                r + FREE_SPACE_SLACK as usize >= exact,
                "page {page}: record {r} lags exact class {exact} beyond the slack"
            );
            assert!(
                exact == 0 || r >= 1,
                "page {page} hidden in bucket 0 with {free} free"
            );
        }
        for needed in (16..4000).step_by(37) {
            if let Some(page) = catalog.find_page_with_room(needed).unwrap() {
                let free = SlottedPageRef::new(pool.get(page).unwrap().bytes()).free_space();
                assert!(
                    free >= needed,
                    "offered page {page} ({free} free) for {needed}"
                );
            }
        }
    }

    #[test]
    fn the_free_space_map_holds_its_contract_under_insert_update_delete_churn() {
        let dir = TempDir::new().unwrap();
        let (_store, pool, catalog, heap) = cataloged_heap(&dir, 4096, 8 * 1024 * 1024);
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut live: Vec<(TupleLocator, Vec<u8>)> = Vec::new();
        for step in 0..3_000u32 {
            let roll = next() % 10;
            if roll < 5 || live.is_empty() {
                let value = payload(8 + (next() % 990) as usize, step as u8);
                live.push((heap.insert(&value).unwrap(), value));
            } else if roll < 8 {
                let at = (next() as usize) % live.len();
                let value = payload(8 + (next() % 990) as usize, step as u8 ^ 0x5A);
                let locator = heap.update(live[at].0, &value).unwrap();
                live[at] = (locator, value);
            } else {
                let at = (next() as usize) % live.len();
                let (locator, _) = live.swap_remove(at);
                heap.delete(locator).unwrap();
            }
            if step % 50 == 0 {
                assert_free_space_contract(&pool, &catalog, &heap);
            }
        }
        assert_free_space_contract(&pool, &catalog, &heap);
        assert_eq!(
            heap.snapshot().stale_free_space_entries,
            0,
            "a healthy catalog should never need a repair"
        );
        for (locator, value) in &live {
            assert_eq!(&heap.get(*locator).unwrap(), value);
        }
    }

    #[test]
    fn an_insert_never_writes_to_a_page_that_was_freed() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            PageStore::open(
                dir.path().join("h.pages"),
                PageStoreOptions::default()
                    .with_page_size(4096)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default()
                    .with_budget_bytes(4 * 1024 * 1024)
                    .with_shards(2),
            )
            .unwrap(),
        );
        let catalog = Arc::new(Catalog::create(pool.clone()).unwrap());
        let heap = HeapFile::with_defaults(pool.clone());
        heap.set_catalog(catalog.clone());

        let doomed = heap.insert(&payload(900, 1)).unwrap().page_id;
        // Free the page the way vacuum does (zero it under a Free header),
        // but leave its free-space record behind, as main's forget_page left
        // a record that had drifted from the page's free space.
        heap.forget_page(doomed);
        {
            let mut guard = pool.get_mut(doomed).unwrap();
            let bytes = guard.bytes_mut();
            bytes.fill(0);
            crate::page::PageHeader::new(doomed, PageType::Free, 4096).encode(bytes);
        }
        assert_eq!(catalog.find_page_with_room(1000).unwrap(), Some(doomed));
        let freed = pool.get(doomed).unwrap().bytes().to_vec();
        pool.take_pages_needing_log();

        let placed = heap.insert(&payload(900, 2)).unwrap();
        assert_ne!(
            placed.page_id, doomed,
            "a committed row was placed on a freed page"
        );
        assert_eq!(
            pool.get(placed.page_id).unwrap().bytes()[6],
            PageType::Heap as u8
        );
        assert_eq!(
            pool.get(doomed).unwrap().bytes(),
            &freed[..],
            "the freed page was written"
        );
        assert!(!pool.take_pages_needing_log().contains(&doomed));
        assert!(catalog.free_space_buckets(doomed).unwrap().is_empty());
        assert_eq!(heap.snapshot().non_heap_free_space_records, 1);
        assert_eq!(heap.get(placed).unwrap(), payload(900, 2));
    }
}
