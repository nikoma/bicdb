//! Overflow pages for values too large for one heap page.
//!
//! Phase 1 of `docs/server-paged-storage-todo.md`: "Implement overflow pages for
//! values that do not fit the row/page policy. Stream them; never collect an
//! unbounded value merely to satisfy a read."
//!
//! A value is split across a singly-linked chain of [`PageType::Overflow`]
//! pages, linked through the header's `next_page` field, each recording how many
//! payload bytes it carries in `payload_len`. The heap tuple keeps only a
//! reference to the chain head, so a wide value never inflates the hot page that
//! indexes it — the same ownership principle the existing large-value/attachment
//! facility already uses.
//!
//! # Streaming is the default, not an option
//!
//! [`OverflowReader`] implements [`std::io::Read`] and holds exactly one page
//! at a time. There is a `read_chain` convenience that collects into a `Vec`,
//! but it takes an explicit byte limit and refuses to exceed it. That asymmetry
//! is deliberate: the roadmap's whole memory envelope collapses if a single
//! `SELECT` of a wide row can allocate without bound, so the unbounded version
//! simply does not exist.

use std::io::{self, Read};

use crate::error::{PageError, Result};
use crate::manager::PageStore;
use crate::page::{PageHeader, PageId, PageType, PAGE_HEADER_BYTES, PAGE_TRAILER_BYTES};

/// Payload bytes one overflow page can carry.
#[inline]
pub fn payload_capacity(page_size: u32) -> usize {
    page_size as usize - PAGE_HEADER_BYTES - PAGE_TRAILER_BYTES
}

/// Pages needed to hold `len` bytes.
pub fn pages_required(len: usize, page_size: u32) -> u64 {
    let capacity = payload_capacity(page_size);
    if len == 0 {
        0
    } else {
        len.div_ceil(capacity) as u64
    }
}

/// Write an overflow chain **through the buffer pool**, so the pool's
/// writeback barrier logs each page.
///
/// [`write_chain`] writes to the page store directly, which bypasses the pool
/// and therefore the write-ahead log. That is fine for a store with no WAL, and
/// silently loses data in one that has one: the chain lands on disk with no log
/// record, so a crash leaves a heap tuple pointing at pages recovery knows
/// nothing about. Anything durable must use this variant.
pub fn write_chain_pooled(pool: &crate::pool::BufferPool, value: &[u8]) -> Result<PageId> {
    if value.is_empty() {
        return Err(PageError::ValueTooLarge {
            page_id: 0,
            len: 0,
            free: 0,
        });
    }
    let store = pool.store();
    let page_size = store.page_size();
    let capacity = payload_capacity(page_size);

    let chunks: Vec<&[u8]> = value.chunks(capacity).collect();
    let mut next: PageId = 0;
    let mut head: PageId = 0;

    for chunk in chunks.iter().rev() {
        let page_id = store.allocate(PageType::Overflow)?;
        let mut guard = pool.get_mut(page_id)?;
        let page = guard.bytes_mut();
        page.fill(0);
        let mut header = PageHeader::new(page_id, PageType::Overflow, page_size);
        header.next_page = next;
        header.payload_len = chunk.len() as u32;
        header.encode(page);
        page[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + chunk.len()].copy_from_slice(chunk);
        drop(guard);
        next = page_id;
        head = page_id;
    }

    Ok(head)
}

/// Read a chain header through the pool, so pages still dirty in cache are
/// visible. Reading via the store would miss them entirely.
fn pooled_header(pool: &crate::pool::BufferPool, page_id: PageId) -> Result<PageHeader> {
    // Overflow pages ARE the document bodies and large stored values. They are
    // high volume and low reuse, and they must not be able to displace the
    // postings and dictionary that make search fast.
    let guard = pool.get_streaming(page_id)?;
    let header = PageHeader::decode(guard.bytes(), pool.store().path())?;
    if header.page_type != PageType::Overflow {
        return Err(PageError::UnexpectedPageType {
            page_id,
            found: header.page_type as u8,
            expected: PageType::Overflow as u8,
        });
    }
    Ok(header)
}

/// Total payload bytes in a chain, via the pool.
pub fn chain_len_pooled(pool: &crate::pool::BufferPool, head: PageId) -> Result<u64> {
    let mut total = 0u64;
    let mut current = head;
    while current != 0 {
        let header = pooled_header(pool, current)?;
        total += u64::from(header.payload_len);
        current = header.next_page;
    }
    Ok(total)
}

/// Release every page in a chain, via the pool.
pub fn free_chain_pooled(pool: &crate::pool::BufferPool, head: PageId) -> Result<u64> {
    let mut freed = 0;
    let mut current = head;
    while current != 0 {
        let next = pooled_header(pool, current)?.next_page;
        pool.free_page(current)?;
        freed += 1;
        current = next;
    }
    Ok(freed)
}

/// Streaming reader over a chain, through the pool. Holds one page at a time.
pub struct PooledOverflowReader<'a> {
    pool: &'a crate::pool::BufferPool,
    next_page: PageId,
    chunk: Vec<u8>,
    consumed: usize,
}

impl<'a> PooledOverflowReader<'a> {
    pub fn new(pool: &'a crate::pool::BufferPool, head: PageId) -> Self {
        Self {
            pool,
            next_page: head,
            chunk: Vec::new(),
            consumed: 0,
        }
    }

    fn advance(&mut self) -> Result<bool> {
        if self.next_page == 0 {
            return Ok(false);
        }
        let guard = self.pool.get_streaming(self.next_page)?;
        let header = PageHeader::decode(guard.bytes(), self.pool.store().path())?;
        if header.page_type != PageType::Overflow {
            return Err(PageError::UnexpectedPageType {
                page_id: self.next_page,
                found: header.page_type as u8,
                expected: PageType::Overflow as u8,
            });
        }
        let payload = header.payload_len as usize;
        let capacity = payload_capacity(self.pool.page_size());
        if payload > capacity {
            return Err(PageError::ShortRead {
                page_id: self.next_page,
                got: capacity,
                want: payload,
            });
        }
        // One page copied out at a time, so the pin is released promptly and
        // the reader's footprint stays one page whatever the chain's length.
        self.chunk = guard.bytes()[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + payload].to_vec();
        self.consumed = 0;
        self.next_page = header.next_page;
        if self.next_page != 0 {
            // The overflow header supplies the exact next dependency. The
            // request only enters the bounded queue; query threads never issue
            // speculative I/O themselves.
            self.pool.request_read_ahead([self.next_page]);
        }
        Ok(true)
    }
}

impl Read for PooledOverflowReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.consumed == self.chunk.len() {
            match self.advance() {
                Ok(true) => {}
                Ok(false) => return Ok(0),
                Err(error) => return Err(io::Error::other(error.to_string())),
            }
        }
        let take = (self.chunk.len() - self.consumed).min(out.len());
        out[..take].copy_from_slice(&self.chunk[self.consumed..self.consumed + take]);
        self.consumed += take;
        Ok(take)
    }
}

/// Collect a chain via the pool, refusing to exceed `limit`.
pub fn read_chain_pooled(
    pool: &crate::pool::BufferPool,
    head: PageId,
    limit: usize,
) -> Result<Vec<u8>> {
    let total = chain_len_pooled(pool, head)?;
    if total > limit as u64 {
        return Err(PageError::ValueTooLarge {
            page_id: head,
            len: total as usize,
            free: limit,
        });
    }
    let mut out = Vec::with_capacity(total as usize);
    PooledOverflowReader::new(pool, head)
        .read_to_end(&mut out)
        .map_err(|e| PageError::io(pool.store().path(), e))?;
    Ok(out)
}

/// Write `value` as an overflow chain, returning the head page id.
///
/// Pages are allocated and written head-first, but each page's `next_page` is
/// only known after its successor is allocated, so the chain is built from the
/// tail backwards. That ordering matters for crash safety: at no point does a
/// durable page point at a page that has not been written.
pub fn write_chain(store: &PageStore, value: &[u8]) -> Result<PageId> {
    if value.is_empty() {
        return Err(PageError::ValueTooLarge {
            page_id: 0,
            len: 0,
            free: 0,
        });
    }
    let page_size = store.page_size();
    let capacity = payload_capacity(page_size);

    // Split into chunks, then write them in reverse so `next_page` always
    // refers to an already-durable page.
    let chunks: Vec<&[u8]> = value.chunks(capacity).collect();
    let mut next: PageId = 0;
    let mut head: PageId = 0;

    for chunk in chunks.iter().rev() {
        let page_id = store.allocate(PageType::Overflow)?;
        let mut page = vec![0u8; page_size as usize];
        let mut header = PageHeader::new(page_id, PageType::Overflow, page_size);
        header.next_page = next;
        header.payload_len = chunk.len() as u32;
        header.encode(&mut page);
        page[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + chunk.len()].copy_from_slice(chunk);
        store.write_page(page_id, &mut page)?;
        next = page_id;
        head = page_id;
    }

    Ok(head)
}

/// Release every page in a chain.
pub fn free_chain(store: &PageStore, head: PageId) -> Result<u64> {
    let mut freed = 0;
    let mut current = head;
    while current != 0 {
        let header = store.read_header(current)?;
        if header.page_type != PageType::Overflow {
            return Err(PageError::UnexpectedPageType {
                page_id: current,
                found: header.page_type as u8,
                expected: PageType::Overflow as u8,
            });
        }
        let next = header.next_page;
        store.free(current)?;
        freed += 1;
        current = next;
    }
    Ok(freed)
}

/// Total payload bytes in a chain, without reading the payload.
pub fn chain_len(store: &PageStore, head: PageId) -> Result<u64> {
    let mut total = 0u64;
    let mut current = head;
    while current != 0 {
        let header = store.read_header(current)?;
        if header.page_type != PageType::Overflow {
            return Err(PageError::UnexpectedPageType {
                page_id: current,
                found: header.page_type as u8,
                expected: PageType::Overflow as u8,
            });
        }
        total += u64::from(header.payload_len);
        current = header.next_page;
    }
    Ok(total)
}

/// Streaming reader over an overflow chain. Holds one page at a time.
pub struct OverflowReader<'a> {
    store: &'a PageStore,
    next_page: PageId,
    buffer: Vec<u8>,
    /// Payload bytes currently in `buffer`.
    filled: usize,
    /// How much of `buffer` the caller has consumed.
    consumed: usize,
}

impl<'a> OverflowReader<'a> {
    pub fn new(store: &'a PageStore, head: PageId) -> Self {
        let page_size = store.page_size() as usize;
        Self {
            store,
            next_page: head,
            buffer: vec![0u8; page_size],
            filled: 0,
            consumed: 0,
        }
    }

    /// Load the next page of the chain. Returns false at the end.
    fn advance(&mut self) -> Result<bool> {
        if self.next_page == 0 {
            return Ok(false);
        }
        let header = self.store.read_page(self.next_page, &mut self.buffer)?;
        if header.page_type != PageType::Overflow {
            return Err(PageError::UnexpectedPageType {
                page_id: self.next_page,
                found: header.page_type as u8,
                expected: PageType::Overflow as u8,
            });
        }
        let capacity = payload_capacity(self.store.page_size());
        let payload = header.payload_len as usize;
        if payload > capacity {
            // A corrupt length would otherwise index out of the buffer.
            return Err(PageError::ShortRead {
                page_id: self.next_page,
                got: capacity,
                want: payload,
            });
        }
        self.filled = payload;
        self.consumed = 0;
        self.next_page = header.next_page;
        Ok(true)
    }
}

impl Read for OverflowReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.consumed == self.filled {
            match self.advance() {
                Ok(true) => {}
                Ok(false) => return Ok(0),
                Err(error) => return Err(io::Error::other(error.to_string())),
            }
        }
        let available = self.filled - self.consumed;
        let take = available.min(out.len());
        let from = PAGE_HEADER_BYTES + self.consumed;
        out[..take].copy_from_slice(&self.buffer[from..from + take]);
        self.consumed += take;
        Ok(take)
    }
}

/// Collect a chain into memory, refusing to exceed `limit` bytes.
///
/// The limit is mandatory. A caller that genuinely cannot bound the size should
/// use [`OverflowReader`] and stream; there is deliberately no unbounded
/// collect, because one is all it takes to make the memory envelope untrue.
pub fn read_chain(store: &PageStore, head: PageId, limit: usize) -> Result<Vec<u8>> {
    let total = chain_len(store, head)?;
    if total > limit as u64 {
        return Err(PageError::ValueTooLarge {
            page_id: head,
            len: total as usize,
            free: limit,
        });
    }
    let mut out = Vec::with_capacity(total as usize);
    OverflowReader::new(store, head)
        .read_to_end(&mut out)
        .map_err(|e| PageError::io(store.path(), e))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::manager::PageStoreOptions;
    use crate::pool::{BufferPool, BufferPoolOptions};
    use tempfile::TempDir;

    fn store(dir: &TempDir, page_size: u32) -> PageStore {
        PageStore::open(
            dir.path().join("o.pages"),
            PageStoreOptions::default()
                .with_page_size(page_size)
                .with_fsync(false),
        )
        .unwrap()
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|index| (index % 251) as u8).collect()
    }

    #[test]
    fn a_value_smaller_than_one_page_round_trips() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 1024);
        let value = payload(100);
        let head = write_chain(&store, &value).unwrap();
        assert_eq!(read_chain(&store, head, 4096).unwrap(), value);
        assert_eq!(chain_len(&store, head).unwrap(), 100);
    }

    #[test]
    fn a_value_spanning_many_pages_round_trips_exactly() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let capacity = payload_capacity(512);
        let value = payload(capacity * 7 + 13);

        let head = write_chain(&store, &value).unwrap();
        assert_eq!(chain_len(&store, head).unwrap(), value.len() as u64);
        assert_eq!(read_chain(&store, head, 1 << 20).unwrap(), value);
    }

    #[test]
    fn values_landing_exactly_on_a_page_boundary_round_trip() {
        // Off-by-one territory: an exact multiple must not produce a trailing
        // empty page, and must not lose the last chunk.
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let capacity = payload_capacity(512);

        for multiple in 1..=4 {
            let value = payload(capacity * multiple);
            let head = write_chain(&store, &value).unwrap();
            assert_eq!(
                chain_len(&store, head).unwrap(),
                value.len() as u64,
                "exact multiple {multiple} lost or gained bytes"
            );
            assert_eq!(read_chain(&store, head, 1 << 20).unwrap(), value);
            assert_eq!(
                pages_required(value.len(), 512),
                multiple as u64,
                "exact multiple {multiple} allocated a spurious page"
            );
        }
    }

    #[test]
    fn streaming_reads_hold_one_page_regardless_of_value_size() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let value = payload(payload_capacity(512) * 40);
        let head = write_chain(&store, &value).unwrap();

        let mut reader = OverflowReader::new(&store, head);
        let mut collected = Vec::new();
        let mut chunk = [0u8; 37]; // deliberately not a page multiple
        loop {
            let read = reader.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            collected.extend_from_slice(&chunk[..read]);
        }
        assert_eq!(collected, value);

        // The reader's own buffer is one page, whatever the value's size.
        assert_eq!(reader.buffer.len(), 512);
    }

    #[test]
    fn pooled_reader_submits_the_exact_next_chain_page_without_inline_io() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(store(&dir, 512));
        let write_pool = BufferPool::new(
            store.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(8 * 512)
                .with_shards(1),
        )
        .unwrap();
        let value = payload(payload_capacity(512) * 3);
        let head = write_chain_pooled(&write_pool, &value).unwrap();
        write_pool.flush_all().unwrap();
        drop(write_pool);

        let read_pool = BufferPool::new(
            store,
            BufferPoolOptions::default()
                .with_budget_bytes(8 * 512)
                .with_shards(1),
        )
        .unwrap();
        // Chain hints are only queued while a read-ahead driver exists.
        read_pool.register_read_ahead_driver();
        let mut reader = PooledOverflowReader::new(&read_pool, head);
        let mut one_byte = [0_u8; 1];
        assert_eq!(reader.read(&mut one_byte).unwrap(), 1);
        assert_eq!(read_pool.read_ahead_queue_depth(), 1);
        assert_eq!(read_pool.snapshot().read_ahead_steps, 0);
    }

    #[test]
    fn pooled_free_discards_dirty_frames_before_page_reuse() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(store(&dir, 512));
        let pool = BufferPool::new(
            store.clone(),
            BufferPoolOptions::default()
                .with_budget_bytes(8 * 512)
                .with_shards(1),
        )
        .unwrap();
        let value = payload(payload_capacity(512) * 3);
        let head = write_chain_pooled(&pool, &value).unwrap();

        free_chain_pooled(&pool, head).unwrap();
        pool.flush_all().unwrap();
        assert_eq!(store.read_header(head).unwrap().page_type, PageType::Free);

        let replacement = write_chain_pooled(&pool, &value).unwrap();
        pool.flush_all().unwrap();
        assert_eq!(read_chain(&store, replacement, 1 << 20).unwrap(), value);
    }

    #[test]
    fn read_chain_refuses_to_exceed_its_limit() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let value = payload(payload_capacity(512) * 5);
        let head = write_chain(&store, &value).unwrap();

        let error = read_chain(&store, head, 100).unwrap_err();
        assert!(
            matches!(error, PageError::ValueTooLarge { .. }),
            "an over-limit collect must fail, not allocate: {error:?}"
        );
    }

    #[test]
    fn freeing_a_chain_returns_every_page() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let value = payload(payload_capacity(512) * 6);

        let head = write_chain(&store, &value).unwrap();
        let expected = pages_required(value.len(), 512);
        assert_eq!(store.free_page_count(), 0);

        assert_eq!(free_chain(&store, head).unwrap(), expected);
        assert_eq!(store.free_page_count(), expected);
    }

    #[test]
    fn freed_chain_pages_are_reused_by_the_next_write() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let value = payload(payload_capacity(512) * 3);

        let head = write_chain(&store, &value).unwrap();
        let pages_before = store.page_count();
        free_chain(&store, head).unwrap();

        let second = write_chain(&store, &value).unwrap();
        assert_eq!(
            store.page_count(),
            pages_before,
            "a rewrite after free should recycle, not extend the file"
        );
        assert_eq!(read_chain(&store, second, 1 << 20).unwrap(), value);
    }

    #[test]
    fn a_chain_pointed_at_a_non_overflow_page_is_refused() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        let heap = store.allocate(PageType::Heap).unwrap();

        assert!(matches!(
            chain_len(&store, heap),
            Err(PageError::UnexpectedPageType { .. })
        ));
        assert!(matches!(
            read_chain(&store, heap, 1 << 20),
            Err(PageError::UnexpectedPageType { .. })
        ));
    }

    #[test]
    fn empty_values_are_rejected_rather_than_producing_an_empty_chain() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir, 512);
        assert!(write_chain(&store, b"").is_err());
    }

    #[test]
    fn chains_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("o.pages");
        let value = payload(payload_capacity(1024) * 4 + 7);

        let head = {
            let store = PageStore::open(
                &path,
                PageStoreOptions::default()
                    .with_page_size(1024)
                    .with_fsync(false),
            )
            .unwrap();
            let head = write_chain(&store, &value).unwrap();
            store.flush().unwrap();
            head
        };

        let store = PageStore::open(
            &path,
            PageStoreOptions::default()
                .with_page_size(1024)
                .with_fsync(false),
        )
        .unwrap();
        assert_eq!(read_chain(&store, head, 1 << 20).unwrap(), value);
    }

    #[test]
    fn pages_required_matches_what_write_chain_allocates() {
        let dir = TempDir::new().unwrap();
        for page_size in [512u32, 1024, 4096] {
            for len in [1usize, 10, 500, 5_000, 50_000] {
                let sub = TempDir::new_in(dir.path()).unwrap();
                let store = store(&sub, page_size);
                let before = store.page_count();
                let head = write_chain(&store, &payload(len)).unwrap();
                let allocated = store.page_count() - before;
                assert_eq!(
                    allocated,
                    pages_required(len, page_size),
                    "page_size {page_size}, len {len}"
                );
                assert_eq!(chain_len(&store, head).unwrap(), len as u64);
            }
        }
    }
}
