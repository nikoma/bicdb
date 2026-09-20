//! 64-bit offset tests using sparse files.
//!
//! Phase 1 of `docs/server-paged-storage-todo.md`: "Use 64-bit page IDs,
//! lengths, counters, and offsets throughout. Add sparse file tests beyond 4 GB
//! and 1 TB so accidental `usize`/32-bit truncation is caught without
//! allocating that much physical disk in CI."
//!
//! The bug being hunted is specific and nasty: a `u64` offset truncated through
//! a `usize` or `u32` somewhere on the I/O path wraps, and the store silently
//! reads or writes the *wrong page* instead of failing. On a 64-bit host most
//! such bugs are invisible; they surface on a 32-bit target or with an
//! `as u32` that nobody noticed. Either way the symptom is data corruption at a
//! scale nobody wants to debug on.
//!
//! These tests write real pages at offsets past 4 GiB and 1 TiB. The files are
//! sparse, so the physical cost is a handful of pages regardless of the
//! apparent size — verified explicitly by `sparse_files_do_not_consume_disk`.
//!
//! # Unix only, deliberately
//!
//! These tests reserve up to 1 TiB to exercise 64-bit offset arithmetic, which
//! is affordable only because `set_len` on unix leaves the file sparse — no
//! blocks are allocated until a page is actually written. NTFS files are not
//! sparse unless explicitly marked (`FSCTL_SET_SPARSE`), so on Windows the same
//! reservation would try to allocate real disk and either fill the volume or
//! fail outright.
//!
//! This does NOT affect the store in production on Windows: the only caller of
//! `reserve` outside tests is WAL recovery (`reserve(record.page_id + 1)`),
//! which grows the file to the size the database already needs. Sparseness is a
//! property these tests exploit, not one the engine depends on.
//!
//! To run large-offset coverage on Windows, the file would have to be marked
//! sparse at creation — a `DeviceIoControl` call and therefore a new
//! dependency, which is not worth it while the arithmetic under test is
//! platform-independent.

#![cfg(unix)]

use std::path::Path;

use bicdb_page::{PageHeader, PageId, PageStore, PageStoreOptions, PageType, PAGE_HEADER_BYTES};
use tempfile::TempDir;

const PAGE_SIZE: u32 = 4096;
const FOUR_GIB: u64 = 4 * 1024 * 1024 * 1024;
const ONE_TIB: u64 = 1024 * 1024 * 1024 * 1024;

fn open(dir: &TempDir) -> PageStore {
    PageStore::open(
        dir.path().join("sparse.pages"),
        PageStoreOptions::default()
            .with_page_size(PAGE_SIZE)
            .with_fsync(false),
    )
    .unwrap()
}

/// Write a recognizable marker into a page and read it back.
fn round_trip(store: &PageStore, page_id: PageId, marker: &[u8]) {
    let mut page = vec![0u8; PAGE_SIZE as usize];
    let mut header = PageHeader::new(page_id, PageType::Heap, PAGE_SIZE);
    header.lsn = page_id ^ 0x5555_5555_5555_5555;
    header.encode(&mut page);
    page[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + marker.len()].copy_from_slice(marker);
    store.write_page(page_id, &mut page).unwrap();

    let mut read_back = vec![0u8; PAGE_SIZE as usize];
    let header = store.read_page(page_id, &mut read_back).unwrap();

    // The header's own page_id is the truncation canary: if the offset wrapped,
    // we read some other page, and that page knows which one it is.
    assert_eq!(
        header.page_id, page_id,
        "read the wrong page — offset arithmetic truncated"
    );
    assert_eq!(header.lsn, page_id ^ 0x5555_5555_5555_5555);
    assert_eq!(
        &read_back[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + marker.len()],
        marker
    );
}

/// Physical blocks actually allocated to a file, in bytes.
fn allocated_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path).unwrap();
    metadata.blocks() * 512
}

#[test]
fn pages_beyond_four_gibibytes_round_trip() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);

    let first_page_past_4gib = FOUR_GIB / u64::from(PAGE_SIZE);
    store.reserve(first_page_past_4gib + 16).unwrap();

    // Straddle the boundary: the page immediately below, at, and above 4 GiB.
    for page_id in [
        first_page_past_4gib - 1,
        first_page_past_4gib,
        first_page_past_4gib + 1,
        first_page_past_4gib + 15,
    ] {
        round_trip(&store, page_id, b"past-4gib");
    }
}

#[test]
fn pages_beyond_one_tebibyte_round_trip() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);

    let first_page_past_1tib = ONE_TIB / u64::from(PAGE_SIZE);
    store.reserve(first_page_past_1tib + 8).unwrap();

    for page_id in [
        first_page_past_1tib - 1,
        first_page_past_1tib,
        first_page_past_1tib + 7,
    ] {
        round_trip(&store, page_id, b"past-1tib");
    }
}

#[test]
fn pages_beyond_the_thirty_two_bit_page_id_boundary_round_trip() {
    // Distinct from the byte-offset tests above: here the *page id* itself
    // exceeds u32, which catches a truncation in id handling even when the byte
    // offset arithmetic is correct. An `as u32` on the id would wrap page
    // 2^32 to page 0 and silently overwrite the superblock.
    //
    // Uses the minimum page size deliberately: 2^32 pages x 4 KiB would be a
    // 16 TiB file, which exceeds the maximum file size on common filesystems
    // (the reservation fails with EFBIG). At 512 bytes the same page-id range
    // is a 2 TiB sparse file, which every target filesystem accepts — and the
    // id arithmetic under test is identical either way.
    const SMALL_PAGE: u32 = 512;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wide-ids.pages");
    let store = PageStore::open(
        &path,
        PageStoreOptions::default()
            .with_page_size(SMALL_PAGE)
            .with_fsync(false),
    )
    .unwrap();

    let boundary = u64::from(u32::MAX);
    store.reserve(boundary + 4).unwrap();

    for page_id in [boundary - 1, boundary, boundary + 1, boundary + 3] {
        let mut page = vec![0u8; SMALL_PAGE as usize];
        let mut header = PageHeader::new(page_id, PageType::Heap, SMALL_PAGE);
        header.lsn = page_id;
        header.encode(&mut page);
        page[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 11].copy_from_slice(b"past-u32-id");
        store.write_page(page_id, &mut page).unwrap();

        let mut read_back = vec![0u8; SMALL_PAGE as usize];
        let header = store.read_page(page_id, &mut read_back).unwrap();
        assert_eq!(
            header.page_id, page_id,
            "page id {page_id} truncated on the I/O path"
        );
        assert_eq!(header.lsn, page_id);
    }

    // The superblock must be untouched: a wrapped id would have landed on it.
    store.flush().unwrap();
    let reopened = PageStore::open(
        &path,
        PageStoreOptions::default()
            .with_page_size(SMALL_PAGE)
            .with_fsync(false),
    )
    .unwrap();
    assert!(reopened.page_count() >= boundary);
    assert_eq!(reopened.page_size(), SMALL_PAGE);
}

#[test]
fn sparse_files_do_not_consume_disk() {
    // Guards the test technique itself. If reserve() ever stopped being sparse
    // — say by pre-filling — CI would start writing terabytes, and these tests
    // would have to be deleted rather than fixed.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sparse.pages");
    let store = open(&dir);

    let page_at_1tib = ONE_TIB / u64::from(PAGE_SIZE);
    store.reserve(page_at_1tib + 2).unwrap();
    round_trip(&store, page_at_1tib, b"sparse");
    store.flush().unwrap();

    let apparent = std::fs::metadata(&path).unwrap().len();
    let physical = allocated_bytes(&path);

    assert!(
        apparent >= ONE_TIB,
        "file should appear at least 1 TiB, got {apparent}"
    );
    assert!(
        physical < 64 * 1024 * 1024,
        "file consumed {physical} physical bytes; reserve() is no longer sparse \
         and this test would fill the disk"
    );
}

#[test]
fn the_page_count_survives_reopen_past_four_gibibytes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sparse.pages");
    let page_at_8gib = (8 * 1024 * 1024 * 1024u64) / u64::from(PAGE_SIZE);

    {
        let store = open(&dir);
        store.reserve(page_at_8gib + 1).unwrap();
        round_trip(&store, page_at_8gib, b"persisted");
        store.flush().unwrap();
    }

    let store = PageStore::open(
        &path,
        PageStoreOptions::default()
            .with_page_size(PAGE_SIZE)
            .with_fsync(false),
    )
    .unwrap();
    assert_eq!(store.page_count(), page_at_8gib + 1);

    // And the page is still readable at its enormous offset after reopen.
    let mut page = vec![0u8; PAGE_SIZE as usize];
    let header = store.read_page(page_at_8gib, &mut page).unwrap();
    assert_eq!(header.page_id, page_at_8gib);
    assert_eq!(
        &page[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 9],
        b"persisted"
    );
}

#[test]
fn a_never_written_reserved_page_reads_as_corrupt_not_as_a_valid_empty_page() {
    // Sparse holes read back as zeros. Those zeros must not be mistaken for a
    // legitimate page: a zeroed page has no magic and a zero checksum, and the
    // store has to say so rather than hand back a plausible-looking empty page.
    let dir = TempDir::new().unwrap();
    let store = open(&dir);
    let page_at_4gib = FOUR_GIB / u64::from(PAGE_SIZE);
    store.reserve(page_at_4gib + 2).unwrap();

    let mut page = vec![0u8; PAGE_SIZE as usize];
    let error = store.read_page(page_at_4gib, &mut page).unwrap_err();
    assert!(
        error.is_corruption(),
        "an unwritten sparse hole must be reported as corruption, got {error:?}"
    );
}
