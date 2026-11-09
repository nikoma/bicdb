//! Durable page directory and free-space map.
//!
//! Phase 2 of `docs/server-paged-storage-todo.md`: "Add a free-space map and
//! reuse policy that cannot expose stale tuple data after slot reuse", and the
//! Phase 3 requirement that open "load only format metadata, catalogs, root
//! pages, checkpoint state, and the WAL suffix. Do not scan every heap or index
//! page."
//!
//! # What this replaces
//!
//! Two structures that were proportional to the file rather than to the working
//! set:
//!
//! - the heap's in-memory `Vec<PageId>` of every heap page, rebuilt at open by
//!   reading every page header — an `O(file)` open, which is precisely what
//!   server-paged mode exists to avoid;
//! - a fixed-size ring of free-space candidates, which was bounded but *lossy*:
//!   a page that fell out of the ring was never reused, so the file grew while
//!   free space sat unused inside it.
//!
//! Both now live in a page-backed B+ tree, so they are durable, complete, and
//! themselves paged.
//!
//! # Key layout
//!
//! One tree, two namespaces distinguished by a leading byte, so a single root
//! covers both and one flush makes both durable:
//!
//! ```text
//! directory:  0x00 || page_id (8 bytes, BIG-endian)  ->  [page_type]
//! free space: 0x01 || bucket (1) || page_id (8, BE)  ->  []
//! ```
//!
//! Page ids are stored big-endian so that bytewise key order — the only order
//! the tree knows — matches numeric order. Little-endian would interleave pages
//! arbitrarily and make a directory scan return them in an order that looks
//! random, which is exactly the kind of thing that works in tests with small ids
//! and falls apart past 256 pages.
//!
//! # Free-space buckets
//!
//! Exact free-byte counts as keys would mean an update on almost every insert.
//! Instead free space is bucketed into [`FREE_BUCKETS`] classes. Finding a page
//! with room for `n` bytes is a range scan starting one class above `n`'s.
//!
//! The recorded bucket is a floor, deliberately. A page recorded in bucket `b`
//! has *at least* `b`'s worth of free space, never less, so a scan does not
//! hand back a page that turns out to be too full — the failure mode that would
//! send an insert back to allocation and grow the file anyway.
//!
//! # Hysteresis: the recorded bucket lags the exact one by a bounded amount
//!
//! Every class change is a remove plus an insert in a catalog leaf, and under
//! full-page WAL logging that is a whole leaf image for a few changed bytes.
//! Moving on every class crossing made the insert-target page cost one catalog
//! image per commit. So the recorded bucket `r` of a page whose exact class is
//! `e` is only kept inside a band (see [`settled_bucket`]):
//!
//! ```text
//! e - FREE_SPACE_SLACK <= r <= e        (and r >= 1 whenever e >= 1)
//! ```
//!
//! - **Down** (the page filled): the move is mandatory once `r > e`, because
//!   the floor would otherwise claim space the page does not have. It goes to
//!   `e - FREE_SPACE_SLACK`, so the next few class crossings need no move.
//! - **Up** (space was freed): lazy. The page moves only once the floor
//!   under-states it by more than `FREE_SPACE_SLACK` classes.
//! - **Never to 0 while any class is free.** Bucket 0 is invisible to every
//!   search, so a page with some free space keeps `r >= 1` and stays findable
//!   for small tuples.
//!
//! The price is that a page can under-advertise up to `FREE_SPACE_SLACK`
//! classes. It never over-advertises, and the heap verifies the page under its
//! latch before using it and repairs any entry that does (one left by an
//! older binary, for example).
//!
//! # Finding a page's recorded bucket
//!
//! Keys are bucket-major, so the bucket a page is recorded under is not
//! derivable from the page id. Callers pass the page's free space before the
//! change as a hint ([`Catalog::record_free_space`]); the hint's band is probed
//! first with point lookups, then every bucket. Lookups are reads: nothing is
//! dirtied, and so nothing is logged, unless a key actually moves.

use std::sync::Arc;

use crate::btree::{BTree, BTreeRange};
use crate::error::Result;
use crate::page::{PageId, PageType};
use crate::pool::BufferPool;

const NS_DIRECTORY: u8 = 0x00;
const NS_FREE_SPACE: u8 = 0x01;
/// Generation floors for freed heap pages: `0x02 || page_id -> u32 floor`.
/// Written when a fully-dead heap page is freed, read when a page id is
/// re-initialized as heap. Lives in the catalog because the catalog is
/// durable and WAL-coherent, so the floor survives any number of
/// intervening tenancies (overflow, btree) that would clobber an in-page
/// stash.
const NS_GENERATION_FLOOR: u8 = 0x02;

/// Number of free-space size classes.
pub const FREE_BUCKETS: u8 = 32;

/// How many classes the recorded bucket may lag below the exact one before
/// the page is moved (see the module docs). 3 classes is 3/32 of a page:
/// 768 bytes at 8 KiB pages.
pub const FREE_SPACE_SLACK: u8 = 3;

/// Where a page whose exact class is `exact` should be recorded, given that it
/// is currently recorded at `recorded`. `None` means leave it where it is.
///
/// The returned bucket, and any `recorded` this leaves alone, always satisfy
/// `exact - FREE_SPACE_SLACK <= r <= exact` and `r >= 1` when `exact >= 1`.
fn settled_bucket(recorded: Option<u8>, exact: u8) -> Option<u8> {
    let lowest_allowed = exact.saturating_sub(FREE_SPACE_SLACK).max(exact.min(1));
    match recorded {
        Some(r) if r <= exact && r >= lowest_allowed => None,
        // Over-claims (the page filled): move down, to the bottom of the band
        // so the next few class crossings are free.
        Some(r) if r > exact => Some(lowest_allowed),
        // Under-claims by more than the slack (space was freed), or no record
        // at all: advertise the exact class.
        _ => Some(exact),
    }
}

/// Which bucket a given number of free bytes belongs to.
///
/// Linear rather than logarithmic: heap pages are one size, so free space is
/// uniformly distributed across `0..page_size` and linear classes divide it
/// evenly. Logarithmic buckets would crowd almost every page into the top class
/// and make the map useless for choosing between them.
fn bucket_for(free_bytes: usize, page_size: u32) -> u8 {
    let span = (page_size as usize / FREE_BUCKETS as usize).max(1);
    ((free_bytes / span).min(FREE_BUCKETS as usize - 1)) as u8
}

fn directory_key(page_id: PageId) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(NS_DIRECTORY);
    key.extend_from_slice(&page_id.to_be_bytes());
    key
}

fn free_key(bucket: u8, page_id: PageId) -> Vec<u8> {
    let mut key = Vec::with_capacity(10);
    key.push(NS_FREE_SPACE);
    key.push(bucket);
    key.extend_from_slice(&page_id.to_be_bytes());
    key
}

fn page_id_from_directory_key(key: &[u8]) -> Option<PageId> {
    if key.len() != 9 || key[0] != NS_DIRECTORY {
        return None;
    }
    Some(u64::from_be_bytes(key[1..9].try_into().ok()?))
}

fn page_id_from_free_key(key: &[u8]) -> Option<PageId> {
    if key.len() != 10 || key[0] != NS_FREE_SPACE {
        return None;
    }
    Some(u64::from_be_bytes(key[2..10].try_into().ok()?))
}

/// Durable directory of pages and their free space.
#[derive(Debug)]
pub struct Catalog {
    tree: BTree,
    page_size: u32,
}

impl Catalog {
    pub fn create(pool: Arc<BufferPool>) -> Result<Self> {
        let page_size = pool.page_size();
        Ok(Self {
            tree: BTree::create_at(pool)?,
            page_size,
        })
    }

    pub fn open(pool: Arc<BufferPool>, root: PageId) -> Result<Self> {
        let page_size = pool.page_size();
        Ok(Self {
            // The catalog is small, hot, and walked by every operation. Give
            // it a budget nothing else can evict.
            tree: BTree::open_at(pool, root)?.in_cache_class(crate::pool::CacheClass::Metadata),
            page_size,
        })
    }

    pub fn root_page(&self) -> PageId {
        self.tree.root_page()
    }

    pub fn take_root_changed(&self) -> bool {
        self.tree.take_root_changed()
    }

    /// Record a page and its current free space.
    pub fn register_page(
        &self,
        page_id: PageId,
        page_type: PageType,
        free_bytes: usize,
    ) -> Result<()> {
        self.tree
            .insert(&directory_key(page_id), &[page_type as u8])?;
        let bucket = bucket_for(free_bytes, self.page_size);
        self.tree.insert(&free_key(bucket, page_id), &[])?;
        Ok(())
    }

    /// Update a page's free space after it changed from `previous_free` to
    /// `free_bytes`. Equivalent to [`Self::record_free_space`] with no known
    /// bucket: `previous_free` is only a hint for finding the current record.
    pub fn update_free_space(
        &self,
        page_id: PageId,
        previous_free: usize,
        free_bytes: usize,
    ) -> Result<()> {
        self.record_free_space(page_id, None, previous_free, free_bytes)?;
        Ok(())
    }

    /// Bring a page's free-space record in line with `free_bytes`, moving the
    /// key only when the hysteresis band requires it (module docs).
    ///
    /// `recorded` is the bucket the caller knows the page is recorded under,
    /// for example from [`Self::find_free_space_entry`]. Without it the record
    /// is located by point lookups, starting at `previous_free`'s band. A page
    /// with no record at all is recorded at its exact class.
    ///
    /// Returns whether the catalog was modified. When it returns `false` no
    /// catalog page was latched for writing, so none is dirtied or logged.
    pub fn record_free_space(
        &self,
        page_id: PageId,
        recorded: Option<u8>,
        previous_free: usize,
        free_bytes: usize,
    ) -> Result<bool> {
        let exact = bucket_for(free_bytes, self.page_size);
        let current = match recorded {
            Some(bucket) => Some(bucket),
            None => self.recorded_bucket(page_id, previous_free)?,
        };
        let Some(target) = settled_bucket(current, exact) else {
            return Ok(false);
        };
        if current == Some(target) {
            return Ok(false);
        }
        if let Some(old) = current {
            self.tree.remove(&free_key(old, page_id))?;
        }
        self.tree.insert(&free_key(target, page_id), &[])?;
        Ok(true)
    }

    /// The bucket `page_id` is recorded under, if any. Read-only.
    ///
    /// Probes the band a page whose free space was `hint_free` would sit in
    /// first (almost always a hit), then every other bucket.
    pub fn recorded_bucket(&self, page_id: PageId, hint_free: usize) -> Result<Option<u8>> {
        let hint = bucket_for(hint_free, self.page_size);
        let low = hint.saturating_sub(FREE_SPACE_SLACK);
        for bucket in (low..=hint).rev() {
            if self.tree.get(&free_key(bucket, page_id))?.is_some() {
                return Ok(Some(bucket));
            }
        }
        for bucket in (0..FREE_BUCKETS).filter(|b| *b < low || *b > hint) {
            if self.tree.get(&free_key(bucket, page_id))?.is_some() {
                return Ok(Some(bucket));
            }
        }
        Ok(None)
    }

    /// Drop one free-space entry that turned out to be wrong when the heap
    /// checked the page it named: the page is no longer a heap page, or it has
    /// less room than the entry claimed. `actual_free` is the page's real free
    /// space when it is still a heap page; it is then recorded correctly.
    pub fn repair_free_space_entry(
        &self,
        page_id: PageId,
        bucket: u8,
        actual_free: Option<usize>,
    ) -> Result<()> {
        if self.tree.get(&free_key(bucket, page_id))?.is_some() {
            self.tree.remove(&free_key(bucket, page_id))?;
        }
        if let Some(actual_free) = actual_free {
            self.record_free_space(page_id, None, actual_free, actual_free)?;
        }
        Ok(())
    }

    /// A page with at least `needed` free bytes, if the catalog knows of one.
    pub fn find_page_with_room(&self, needed: usize) -> Result<Option<PageId>> {
        Ok(self
            .find_free_space_entry(needed)?
            .map(|(page_id, _)| page_id))
    }

    /// Like [`Self::find_page_with_room`], also returning the bucket the page
    /// is recorded under, so the caller can update the record without looking
    /// it up again.
    ///
    /// Scans upward from one bucket above `needed`'s. Because a recorded bucket
    /// is a floor, the first hit should fit; the heap still checks the page
    /// under its latch and repairs the entry if it does not.
    pub fn find_free_space_entry(&self, needed: usize) -> Result<Option<(PageId, u8)>> {
        // Start one bucket above the exact class: a page in `needed`'s own
        // bucket has *at least* the bucket floor free, which may be less than
        // `needed` itself. Starting a class higher keeps the guarantee exact.
        let bucket = bucket_for(needed, self.page_size).saturating_add(1);
        if bucket >= FREE_BUCKETS {
            return Ok(None);
        }
        let start = free_key(bucket, 0);
        // One descent and one entry, not a materialized leaf: this runs on every
        // insert, so the difference between the two shapes is most of the
        // ingest cost.
        let Some((key, _)) = self.tree.first_at_or_after(&start)? else {
            return Ok(None);
        };
        if key.first() != Some(&NS_FREE_SPACE) || key.len() != 10 {
            return Ok(None);
        }
        Ok(page_id_from_free_key(&key).map(|page_id| (page_id, key[1])))
    }

    /// Remove a page from the catalog entirely.
    ///
    /// Every free-space record of the page goes, not only the one
    /// `free_bytes` implies. Records can drift from the page's current free
    /// space (an insert placed through the heap's in-memory ring does not
    /// update the map, and a delete records a guessed previous bucket), and a
    /// record left behind here names a page vacuum is about to free. An
    /// insert offered that page used to write a committed row onto it, and
    /// the row was lost when the page was next allocated.
    pub fn forget_page(&self, page_id: PageId, free_bytes: usize) -> Result<()> {
        let _ = free_bytes;
        self.tree.remove(&directory_key(page_id))?;
        self.forget_free_space(page_id)
    }

    /// Remove every free-space record naming `page_id`, whatever bucket it
    /// is under. Point lookups first, so only records that exist are removed
    /// (a remove of an absent key would still latch the leaf for writing).
    pub fn forget_free_space(&self, page_id: PageId) -> Result<()> {
        for bucket in self.free_space_buckets(page_id)? {
            self.tree.remove(&free_key(bucket, page_id))?;
        }
        Ok(())
    }

    /// Every bucket `page_id` has a free-space record under. Read-only.
    pub fn free_space_buckets(&self, page_id: PageId) -> Result<Vec<u8>> {
        let mut found = Vec::new();
        for bucket in 0..FREE_BUCKETS {
            if self.tree.get(&free_key(bucket, page_id))?.is_some() {
                found.push(bucket);
            }
        }
        Ok(found)
    }

    /// Every registered page of a given type, in ascending page-id order.
    ///
    /// This is what makes a heap scan possible without walking the file: the
    /// directory is read from pages the buffer pool bounds, not from a
    /// header-by-header sweep.
    /// The generation floor recorded for `page_id`, 0 if none.
    pub fn page_generation_floor(&self, page_id: PageId) -> Result<u32> {
        let mut key = Vec::with_capacity(9);
        key.push(NS_GENERATION_FLOOR);
        key.extend_from_slice(&page_id.to_be_bytes());
        Ok(self
            .tree
            .get(&key)?
            .and_then(|value| value.try_into().ok().map(u32::from_le_bytes))
            .unwrap_or(0))
    }

    /// Record `floor` for `page_id`; floors only ever grow.
    pub fn set_page_generation_floor(&self, page_id: PageId, floor: u32) -> Result<()> {
        let current = self.page_generation_floor(page_id)?;
        if floor <= current {
            return Ok(());
        }
        let mut key = Vec::with_capacity(9);
        key.push(NS_GENERATION_FLOOR);
        key.extend_from_slice(&page_id.to_be_bytes());
        self.tree.insert(&key, &floor.to_le_bytes())?;
        Ok(())
    }

    pub fn pages_of_type(&self, page_type: PageType) -> Result<Vec<PageId>> {
        self.scan_pages(page_type, None)?.collect()
    }

    /// Stream matching pages from the durable directory in page-id order.
    ///
    /// The old maintenance path called [`Self::pages_of_type`], which builds a
    /// `Vec<PageId>` proportional to the whole database before processing its
    /// first page. A 20 TB file at 8 KiB pages would need roughly 20 GiB only
    /// for that temporary list. This cursor retains one B+tree leaf at a time
    /// and can resume inclusively at an exact page ID.
    pub(crate) fn scan_pages(
        &self,
        page_type: PageType,
        start_at: Option<PageId>,
    ) -> Result<CatalogPageScan<'_>> {
        let start = start_at
            .map(directory_key)
            .unwrap_or_else(|| vec![NS_DIRECTORY]);
        Ok(CatalogPageScan {
            entries: self.tree.range(&start)?,
            page_type,
            complete: false,
        })
    }

    pub(crate) fn next_page_of_type(
        &self,
        page_type: PageType,
        start_at: Option<PageId>,
    ) -> Result<Option<PageId>> {
        self.scan_pages(page_type, start_at)?.next().transpose()
    }

    /// Total registered pages.
    pub fn page_count(&self) -> Result<usize> {
        Ok(self
            .tree
            .range(&[NS_DIRECTORY][..])?
            .take_while(|entry| {
                entry
                    .as_ref()
                    .map(|(key, _)| key.first() == Some(&NS_DIRECTORY))
                    .unwrap_or(false)
            })
            .count())
    }

    pub fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Bounded cursor over one catalog page type.
pub(crate) struct CatalogPageScan<'a> {
    entries: BTreeRange<'a>,
    page_type: PageType,
    complete: bool,
}

impl Iterator for CatalogPageScan<'_> {
    type Item = Result<PageId>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.complete {
            return None;
        }
        loop {
            let entry = self.entries.next()?;
            let (key, value) = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    self.complete = true;
                    return Some(Err(error));
                }
            };
            if key.first() != Some(&NS_DIRECTORY) {
                self.complete = true;
                return None;
            }
            let Some(page_id) = page_id_from_directory_key(&key) else {
                continue;
            };
            if value.first() == Some(&(self.page_type as u8)) {
                return Some(Ok(page_id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{PageStore, PageStoreOptions};
    use crate::pool::BufferPoolOptions;
    use tempfile::TempDir;

    fn catalog(dir: &TempDir, page_size: u32) -> (Arc<BufferPool>, Catalog) {
        let store = Arc::new(
            PageStore::open(
                dir.path().join("c.pages"),
                PageStoreOptions::default()
                    .with_page_size(page_size)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default().with_budget_bytes(256 * 1024),
            )
            .unwrap(),
        );
        let catalog = Catalog::create(pool.clone()).unwrap();
        (pool, catalog)
    }

    #[test]
    fn registered_pages_are_listed_in_page_id_order() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);

        // Register out of order, and across the 256 boundary where a
        // little-endian key encoding would scramble the ordering.
        for page_id in [500u64, 3, 257, 1, 999, 256, 42] {
            catalog.register_page(page_id, PageType::Heap, 100).unwrap();
        }

        let pages = catalog.pages_of_type(PageType::Heap).unwrap();
        assert_eq!(pages, vec![1, 3, 42, 256, 257, 500, 999]);
        assert_eq!(
            catalog
                .scan_pages(PageType::Heap, Some(257))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            vec![257, 500, 999],
            "the streaming cursor must resume inclusively at the checkpointed page"
        );
    }

    #[test]
    fn page_types_are_listed_separately() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        catalog.register_page(1, PageType::Heap, 10).unwrap();
        catalog.register_page(2, PageType::Overflow, 10).unwrap();
        catalog.register_page(3, PageType::Heap, 10).unwrap();

        assert_eq!(catalog.pages_of_type(PageType::Heap).unwrap(), vec![1, 3]);
        assert_eq!(catalog.pages_of_type(PageType::Overflow).unwrap(), vec![2]);
        assert_eq!(catalog.page_count().unwrap(), 3);
    }

    #[test]
    fn a_page_with_room_is_found_and_actually_has_room() {
        let dir = TempDir::new().unwrap();
        let page_size = 1024u32;
        let (_pool, catalog) = catalog(&dir, page_size);

        catalog.register_page(1, PageType::Heap, 8).unwrap();
        catalog.register_page(2, PageType::Heap, 900).unwrap();
        catalog.register_page(3, PageType::Heap, 40).unwrap();

        let found = catalog.find_page_with_room(500).unwrap();
        assert_eq!(found, Some(2), "did not find the only page with 500 free");

        // Nothing has room for a whole page.
        assert_eq!(
            catalog.find_page_with_room(page_size as usize).unwrap(),
            None
        );
    }

    #[test]
    fn a_returned_page_never_has_less_room_than_requested() {
        // The bucket is a floor, so a hit must genuinely fit. If this were off
        // by one class, inserts would bounce off "found" pages and grow the
        // file anyway — the exact bug the free-space map exists to prevent.
        let dir = TempDir::new().unwrap();
        let page_size = 1024u32;
        let (_pool, catalog) = catalog(&dir, page_size);

        let mut free_by_page = std::collections::BTreeMap::new();
        for page_id in 1..=60u64 {
            let free = (page_id as usize * 17) % (page_size as usize);
            catalog
                .register_page(page_id, PageType::Heap, free)
                .unwrap();
            free_by_page.insert(page_id, free);
        }

        for needed in (1..900).step_by(23) {
            if let Some(page_id) = catalog.find_page_with_room(needed).unwrap() {
                let actual = free_by_page[&page_id];
                assert!(
                    actual >= needed,
                    "catalog offered page {page_id} with {actual} free for a {needed}-byte request"
                );
            }
        }
    }

    #[test]
    fn free_space_updates_move_a_page_between_buckets() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        catalog.register_page(7, PageType::Heap, 900).unwrap();
        assert_eq!(catalog.find_page_with_room(700).unwrap(), Some(7));

        // The page fills up.
        catalog.update_free_space(7, 900, 20).unwrap();
        assert_eq!(
            catalog.find_page_with_room(700).unwrap(),
            None,
            "a full page is still being offered"
        );

        // And empties again.
        catalog.update_free_space(7, 20, 950).unwrap();
        assert_eq!(catalog.find_page_with_room(700).unwrap(), Some(7));
    }

    #[test]
    fn an_update_within_one_bucket_is_a_no_op() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        catalog.register_page(1, PageType::Heap, 900).unwrap();
        // Both land in the same class, so the tree should not be touched.
        catalog.update_free_space(1, 900, 905).unwrap();
        assert_eq!(catalog.find_page_with_room(800).unwrap(), Some(1));
    }

    #[test]
    fn forgetting_a_page_removes_it_from_both_namespaces() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        catalog.register_page(5, PageType::Heap, 800).unwrap();
        catalog.forget_page(5, 800).unwrap();

        assert!(catalog.pages_of_type(PageType::Heap).unwrap().is_empty());
        assert_eq!(catalog.find_page_with_room(100).unwrap(), None);
        assert_eq!(catalog.page_count().unwrap(), 0);
    }

    #[test]
    fn the_catalog_survives_reopen_through_its_root() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("c.pages");
        let root = {
            let store = Arc::new(
                PageStore::open(
                    &path,
                    PageStoreOptions::default()
                        .with_page_size(512)
                        .with_fsync(false),
                )
                .unwrap(),
            );
            let pool = Arc::new(
                BufferPool::new(
                    store.clone(),
                    BufferPoolOptions::default().with_budget_bytes(256 * 1024),
                )
                .unwrap(),
            );
            let catalog = Catalog::create(pool.clone()).unwrap();
            for page_id in 1..=400u64 {
                catalog
                    .register_page(page_id, PageType::Heap, (page_id as usize * 7) % 500)
                    .unwrap();
            }
            pool.flush_all().unwrap();
            store.flush().unwrap();
            catalog.root_page()
        };

        let store = Arc::new(
            PageStore::open(
                &path,
                PageStoreOptions::default()
                    .with_page_size(512)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default().with_budget_bytes(256 * 1024),
            )
            .unwrap(),
        );
        let catalog = Catalog::open(pool, root).unwrap();
        assert_eq!(catalog.page_count().unwrap(), 400);
        assert_eq!(catalog.pages_of_type(PageType::Heap).unwrap().len(), 400);
    }

    #[test]
    fn buckets_are_a_floor_not_a_ceiling() {
        let page_size = 1024u32;
        let span = page_size as usize / FREE_BUCKETS as usize;
        assert_eq!(bucket_for(0, page_size), 0);
        assert_eq!(bucket_for(span - 1, page_size), 0);
        assert_eq!(bucket_for(span, page_size), 1);
        // Saturates rather than overflowing on a full page.
        assert_eq!(bucket_for(page_size as usize, page_size), FREE_BUCKETS - 1);
        assert_eq!(bucket_for(usize::MAX / 2, page_size), FREE_BUCKETS - 1);
    }

    #[test]
    fn the_directory_namespace_does_not_bleed_into_free_space() {
        // Two namespaces in one tree: a scan of either must stop at its own
        // boundary rather than running into the other.
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        for page_id in 1..=50u64 {
            catalog.register_page(page_id, PageType::Heap, 500).unwrap();
        }
        let pages = catalog.pages_of_type(PageType::Heap).unwrap();
        assert_eq!(pages.len(), 50, "directory scan crossed into free space");
        assert!(pages.iter().all(|id| (1..=50).contains(id)));
    }

    #[test]
    fn the_settled_bucket_stays_inside_the_band() {
        for exact in 0..FREE_BUCKETS {
            let lowest = exact.saturating_sub(FREE_SPACE_SLACK).max(exact.min(1));
            for recorded in (0..FREE_BUCKETS).map(Some).chain([None]) {
                let settled = settled_bucket(recorded, exact).or(recorded).unwrap();
                assert!(
                    settled <= exact,
                    "{recorded:?}/{exact} -> {settled} over-claims"
                );
                assert!(
                    settled >= lowest,
                    "{recorded:?}/{exact} -> {settled} lags too far"
                );
                if let Some(r) = recorded {
                    if r <= exact && r >= lowest {
                        assert_eq!(settled_bucket(recorded, exact), None, "{r}/{exact} moved");
                    }
                }
            }
        }
    }

    #[test]
    fn a_page_filling_inside_the_band_does_not_touch_the_catalog() {
        let dir = TempDir::new().unwrap();
        let (pool, catalog) = catalog(&dir, 1024);
        let span = 1024 / FREE_BUCKETS as usize;
        catalog.register_page(9, PageType::Heap, 1000).unwrap();
        pool.acknowledge_log_images_for_test();

        // Filling the page one class at a time moves it only once per
        // FREE_SPACE_SLACK + 1 classes, and a call that does not move it
        // latches no catalog page for writing.
        let mut free = 1000usize;
        let mut moves = 0;
        while free > span {
            let next = free - span;
            let moved = catalog.record_free_space(9, None, free, next).unwrap();
            let logged = pool.acknowledge_log_images_for_test();
            assert_eq!(
                moved,
                !logged.is_empty(),
                "free {free} -> {next}: {logged:?}"
            );
            moves += usize::from(moved);
            let r = catalog.recorded_bucket(9, next).unwrap().unwrap() as usize;
            assert!(r * span <= next, "record {r} over-claims {next} free bytes");
            free = next;
        }
        let crossings = (1000 / span) - 1;
        // One move per band, plus the forced drops near the bottom.
        assert!(
            moves <= crossings / (FREE_SPACE_SLACK as usize + 1) + 2,
            "{moves} moves for {crossings} class crossings"
        );
        assert_eq!(catalog.free_space_buckets(9).unwrap().len(), 1);
    }

    #[test]
    fn freed_space_is_advertised_lazily_but_never_lost() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        let span = 1024 / FREE_BUCKETS as usize;
        catalog.register_page(4, PageType::Heap, 10 * span).unwrap();
        // A small gain stays inside the band: not advertised.
        catalog
            .record_free_space(4, None, 10 * span, 12 * span)
            .unwrap();
        assert_eq!(catalog.free_space_buckets(4).unwrap(), vec![10]);
        // A gain past the slack is.
        catalog
            .record_free_space(4, None, 12 * span, 20 * span)
            .unwrap();
        assert_eq!(catalog.free_space_buckets(4).unwrap(), vec![20]);
        // A page with any class of free space stays findable for small tuples.
        catalog
            .record_free_space(4, None, 20 * span, span + 3)
            .unwrap();
        assert_eq!(catalog.free_space_buckets(4).unwrap(), vec![1]);
        assert_eq!(catalog.find_page_with_room(span - 1).unwrap(), Some(4));
    }

    #[test]
    fn forgetting_a_page_removes_drifted_free_space_records_too() {
        let dir = TempDir::new().unwrap();
        let (_pool, catalog) = catalog(&dir, 1024);
        catalog.register_page(7, PageType::Heap, 500).unwrap();
        // A second record the map failed to move: inserts placed through the
        // heap's ring never update it, and a delete records a guessed
        // previous bucket, so a page's records drift from its free space.
        catalog.tree.insert(&free_key(28, 7), &[]).unwrap();
        assert_eq!(catalog.free_space_buckets(7).unwrap().len(), 2);

        // Vacuum frees the page, reporting its current free space.
        catalog.forget_page(7, 20).unwrap();
        assert!(catalog.free_space_buckets(7).unwrap().is_empty());
        assert_eq!(
            catalog.find_page_with_room(100).unwrap(),
            None,
            "a forgotten page is still offered for inserts"
        );
    }
}
