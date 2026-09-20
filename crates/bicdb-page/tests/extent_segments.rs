//! Extent-segmented page store: fixed-size segment files
//! (`store.pages`, `store.pages.1`, …) behind the same `PageStore` API.
//!
//! Pins the layout contract: pure-arithmetic page routing across segment
//! boundaries, superblock round-trip of the extent size (with the v2 magic
//! so pre-segmentation engines fail closed), crash recovery replaying across
//! segments, checkpoint tail-reclaim deleting whole trailing segment files,
//! and full back-compatibility of the legacy monolithic layout.

use std::path::Path;

use bicdb_page::{PageStore, PageStoreOptions, PagedStore, PagedStoreOptions};
use tempfile::TempDir;

const PAGE_SIZE: u32 = 4096;
/// Smallest legal extent (1 MiB) = 256 pages per segment at 4 KiB.
const EXTENT_BYTES: u64 = 1024 * 1024;
const PAGES_PER_SEGMENT: u64 = EXTENT_BYTES / PAGE_SIZE as u64;

fn paged_options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(64 * 1024 * 1024)
        .with_extent_bytes(EXTENT_BYTES)
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:07}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}-{}", "x".repeat(512)).into_bytes()
}

fn segment_file(dir: &Path, index: usize) -> std::path::PathBuf {
    if index == 0 {
        dir.join("store.pages")
    } else {
        dir.join(format!("store.pages.{index}"))
    }
}

#[test]
fn raw_store_reads_and_writes_across_segment_boundaries() {
    let dir = TempDir::new().unwrap();
    let store = PageStore::open(
        dir.path().join("store.pages"),
        PageStoreOptions::default()
            .with_page_size(PAGE_SIZE)
            .with_fsync(false)
            .with_extent_bytes(EXTENT_BYTES),
    )
    .unwrap();
    assert_eq!(store.extent_pages(), PAGES_PER_SEGMENT);

    // Allocate three segments' worth of pages and stamp each with its id.
    let total = PAGES_PER_SEGMENT * 2 + 7;
    let mut ids = Vec::new();
    for _ in 0..total {
        let page_id = store.allocate(bicdb_page::PageType::Heap).unwrap();
        let mut page = vec![0u8; PAGE_SIZE as usize];
        bicdb_page::PageHeader::new(page_id, bicdb_page::PageType::Heap, PAGE_SIZE)
            .encode(&mut page);
        page[100..108].copy_from_slice(&page_id.to_le_bytes());
        store.write_page(page_id, &mut page).unwrap();
        ids.push(page_id);
    }
    store.flush().unwrap();

    assert_eq!(store.segment_paths().len(), 3, "expected 3 segment files");
    for path in store.segment_paths() {
        assert!(path.exists(), "{} missing", path.display());
    }

    // Every page reads back through the same API, boundary pages included.
    let mut buffer = vec![0u8; PAGE_SIZE as usize];
    for page_id in &ids {
        store.read_page(*page_id, &mut buffer).unwrap();
        assert_eq!(
            u64::from_le_bytes(buffer[100..108].try_into().unwrap()),
            *page_id
        );
    }

    // file_size_bytes sums all segments.
    let expected = (total + 1) * u64::from(PAGE_SIZE); // + superblock page
    assert_eq!(store.file_size_bytes().unwrap(), expected);

    // Reopen: the extent size comes from the superblock, not the options.
    drop(store);
    let store = PageStore::open(
        dir.path().join("store.pages"),
        PageStoreOptions::default()
            .with_page_size(PAGE_SIZE)
            .with_fsync(false), // note: no extent option on reopen
    )
    .unwrap();
    assert_eq!(store.extent_pages(), PAGES_PER_SEGMENT);
    for page_id in &ids {
        store.read_page(*page_id, &mut buffer).unwrap();
        assert_eq!(
            u64::from_le_bytes(buffer[100..108].try_into().unwrap()),
            *page_id
        );
    }
}

#[test]
fn segmented_superblock_uses_v2_magic_and_legacy_stays_v1() {
    let dir = TempDir::new().unwrap();
    let segmented_path = dir.path().join("segmented.pages");
    let legacy_path = dir.path().join("legacy.pages");
    drop(
        PageStore::open(
            &segmented_path,
            PageStoreOptions::default()
                .with_page_size(PAGE_SIZE)
                .with_fsync(false)
                .with_extent_bytes(EXTENT_BYTES),
        )
        .unwrap(),
    );
    drop(
        PageStore::open(
            &legacy_path,
            PageStoreOptions::default()
                .with_page_size(PAGE_SIZE)
                .with_fsync(false),
        )
        .unwrap(),
    );
    let header_bytes = 64; // PAGE_HEADER_BYTES
    let segmented = std::fs::read(&segmented_path).unwrap();
    let legacy = std::fs::read(&legacy_path).unwrap();
    assert_eq!(
        &segmented[header_bytes..header_bytes + 8],
        b"BICDBPG2",
        "segmented store must publish the v2 superblock magic so old engines fail closed"
    );
    assert_eq!(&legacy[header_bytes..header_bytes + 8], b"BICDBPG\0");
}

#[test]
fn invalid_extent_sizes_are_rejected() {
    let dir = TempDir::new().unwrap();
    for bad in [1024_u64, EXTENT_BYTES + 1, PAGE_SIZE as u64] {
        let result = PageStore::open(
            dir.path().join(format!("bad-{bad}.pages")),
            PageStoreOptions::default()
                .with_page_size(PAGE_SIZE)
                .with_fsync(false)
                .with_extent_bytes(bad),
        );
        assert!(result.is_err(), "extent size {bad} must be rejected");
    }
}

#[test]
fn paged_store_round_trips_and_recovers_across_segments() {
    let dir = TempDir::new().unwrap();
    let rows = 3_000usize; // ~1.6 MB of values → several segments
    {
        let (store, _) = PagedStore::open(dir.path(), paged_options()).unwrap();
        for chunk in (0..rows).collect::<Vec<_>>().chunks(200) {
            let xid = store.begin();
            for index in chunk {
                store.put(xid, &key(*index), &value(*index)).unwrap();
            }
            store.commit(xid).unwrap();
        }
        // Crash: drop without checkpoint — recovery must replay the WAL into
        // pages that live beyond segment 0.
    }
    assert!(
        segment_file(dir.path(), 1).exists() || segment_file(dir.path(), 0).exists(),
        "expected segment files on disk"
    );
    let (store, _) = PagedStore::open(dir.path(), paged_options()).unwrap();
    for index in 0..rows {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "row {index} lost across segmented recovery"
        );
    }
    store.checkpoint().unwrap();
    drop(store);

    // Clean reopen after checkpoint still sees everything.
    let (store, _) = PagedStore::open(dir.path(), paged_options()).unwrap();
    assert_eq!(
        store.get(&key(rows - 1)).unwrap().as_deref(),
        Some(value(rows - 1).as_slice())
    );
}

#[test]
fn tail_reclaim_deletes_trailing_segment_files() {
    let dir = TempDir::new().unwrap();
    let store = PageStore::open(
        dir.path().join("store.pages"),
        PageStoreOptions::default()
            .with_page_size(PAGE_SIZE)
            .with_fsync(false)
            .with_extent_bytes(EXTENT_BYTES),
    )
    .unwrap();
    let total = PAGES_PER_SEGMENT * 3;
    let mut ids = Vec::new();
    for _ in 0..total {
        let page_id = store.allocate(bicdb_page::PageType::Heap).unwrap();
        let mut page = vec![0u8; PAGE_SIZE as usize];
        bicdb_page::PageHeader::new(page_id, bicdb_page::PageType::Heap, PAGE_SIZE)
            .encode(&mut page);
        store.write_page(page_id, &mut page).unwrap();
        ids.push(page_id);
    }
    assert_eq!(store.segment_paths().len(), 4);
    let last_kept = ids[(PAGES_PER_SEGMENT / 2) as usize];
    for page_id in ids.iter().rev() {
        if *page_id <= last_kept {
            break;
        }
        store.free(*page_id).unwrap();
    }
    store.flush().unwrap();
    let report = store
        .truncate_trailing_free_pages_bounded(bicdb_page::TailReclaimLimits::default())
        .unwrap();
    assert!(report.pages_truncated > 0, "nothing reclaimed: {report:?}");
    let survivors = store.segment_paths();
    assert_eq!(
        survivors.len(),
        1,
        "trailing segments should be deleted: {survivors:?}"
    );
    let deleted = dir.path().join("store.pages.2");
    assert!(
        !deleted.exists(),
        "segment 2 file must be deleted from disk"
    );
    // Store still fully usable after the shrink.
    let mut buffer = vec![0u8; PAGE_SIZE as usize];
    store.read_page(ids[3], &mut buffer).unwrap();
}

/// Offline relayout round-trip: monolithic → segmented → back, with a
/// PENDING WAL both ways — WAL records address page ids, so replay must
/// succeed against either layout.
#[test]
fn convert_extent_layout_round_trips_with_pending_wal() {
    let dir = TempDir::new().unwrap();
    let rows = 2_000usize;
    // Build a MONOLITHIC store and crash with a dirty WAL.
    {
        let (store, _) =
            PagedStore::open(dir.path(), paged_options().with_extent_bytes(0)).unwrap();
        for chunk in (0..rows).collect::<Vec<_>>().chunks(250) {
            let xid = store.begin();
            for index in chunk {
                store.put(xid, &key(*index), &value(*index)).unwrap();
            }
            store.commit(xid).unwrap();
        }
        // Materialize pages so the relayout has real content to move…
        store.checkpoint().unwrap();
        // …then leave a PENDING WAL tail behind the crash.
        let xid = store.begin();
        store
            .put(xid, b"wal-tail", b"replayed-after-relayout")
            .unwrap();
        store.commit(xid).unwrap();
    }
    let pages_path = dir.path().join("store.pages");

    // Monolithic → segmented, WAL untouched.
    bicdb_page::convert_extent_layout(&pages_path, PAGE_SIZE, EXTENT_BYTES, false).unwrap();
    assert!(
        segment_file(dir.path(), 1).exists(),
        "conversion should have produced multiple segments"
    );
    {
        let (store, _) = PagedStore::open(dir.path(), paged_options()).unwrap();
        for index in (0..rows).step_by(97) {
            assert_eq!(
                store.get(&key(index)).unwrap().as_deref(),
                Some(value(index).as_slice()),
                "row {index} lost by mono→segmented relayout"
            );
        }
        assert_eq!(
            store.get(b"wal-tail").unwrap().as_deref(),
            Some(b"replayed-after-relayout".as_slice()),
            "pending WAL must replay against the new layout"
        );
        // More writes on the segmented layout, crash again.
        let xid = store.begin();
        store.put(xid, b"post-convert", b"survives").unwrap();
        store.commit(xid).unwrap();
    }

    // Segmented → monolithic.
    bicdb_page::convert_extent_layout(&pages_path, PAGE_SIZE, 0, false).unwrap();
    assert!(
        !segment_file(dir.path(), 1).exists(),
        "conversion back to monolithic should remove extra segments"
    );
    let (store, _) = PagedStore::open(dir.path(), paged_options().with_extent_bytes(0)).unwrap();
    for index in (0..rows).step_by(97) {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "row {index} lost by segmented→mono relayout"
        );
    }
    assert_eq!(
        store.get(b"post-convert").unwrap().as_deref(),
        Some(b"survives".as_slice())
    );
}

/// ONLINE extent migration: a live monolithic store re-segments while a
/// writer commits the whole time; progress is durable (crash + reopen
/// resumes); the finished store is a first-class segmented store.
#[test]
fn online_extent_migration_relayouts_a_live_store() {
    let dir = TempDir::new().unwrap();
    let mono = PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(1024 * 1024 * 1024)
        .with_extent_bytes(0);
    let rows = 2_500usize;
    {
        let (store, _) = PagedStore::open(dir.path(), mono.clone()).unwrap();
        // Pin the transaction watermark for the whole migration. Every later
        // commit must survive WAL truncation in the status-spill chain while
        // page routing moves from the monolith into extents.
        let _blocker = store.begin();
        for chunk in (0..rows).collect::<Vec<_>>().chunks(250) {
            let xid = store.begin();
            for index in chunk {
                store.put(xid, &key(*index), &value(*index)).unwrap();
            }
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap(); // materialize pages worth migrating

        let store = std::sync::Arc::new(store);
        store.begin_extent_migration(EXTENT_BYTES).unwrap();

        // Writer hammers throughout the migration. `committed` lets the main
        // thread wait for the writer's first batch before stopping it: on a
        // loaded runner the migration steps can finish before the writer
        // thread ever ran, and `written_live > 0` failed for scheduling
        // reasons alone.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let committed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let writer = {
            let store = std::sync::Arc::clone(&store);
            let stop = std::sync::Arc::clone(&stop);
            let committed = std::sync::Arc::clone(&committed);
            std::thread::spawn(move || {
                let mut index = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    let xid = store.begin();
                    for _ in 0..5 {
                        store
                            .put(xid, format!("live-{index:07}").as_bytes(), &value(index))
                            .unwrap();
                        index += 1;
                    }
                    store.commit(xid).unwrap();
                    committed.store(index, std::sync::atomic::Ordering::Release);
                }
                index
            })
        };

        // Drive a few batches, then "crash" mid-migration.
        for _ in 0..3 {
            store.advance_extent_migration(64).unwrap();
        }
        store.checkpoint().unwrap();
        assert!(store.snapshot().unwrap().status_spill_entries > 0);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while committed.load(std::sync::atomic::Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "live writer made no progress in 30 s"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        stop.store(true, std::sync::atomic::Ordering::Release);
        let written_live = writer.join().unwrap();
        assert!(written_live > 0);
        // Reads correct mid-migration.
        assert_eq!(
            store.get(&key(7)).unwrap().as_deref(),
            Some(value(7).as_slice())
        );
        // Drop without completing (and without checkpoint): recovery + the
        // durable watermark must both survive.
    }

    // Reopen mid-migration: routing honors the durable watermark.
    let (store, recovery) = PagedStore::open(dir.path(), mono.clone()).unwrap();
    assert!(
        recovery.status_spill_entries_at_open > 0,
        "status spill was not recoverable across mid-migration restart"
    );
    for index in (0..rows).step_by(191) {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "row {index} lost across mid-migration reopen"
        );
    }
    assert!(store.get(b"live-0000000").unwrap().is_some());

    // Drive to completion.
    let mut steps = 0usize;
    loop {
        let report = store.advance_extent_migration(512).unwrap();
        steps += 1;
        assert!(steps < 10_000, "migration did not converge");
        if report.complete {
            break;
        }
    }
    // Post-flip: fully segmented store, monolithic tail truncated.
    let seg0_len = std::fs::metadata(dir.path().join("store.pages"))
        .unwrap()
        .len();
    assert_eq!(
        seg0_len, EXTENT_BYTES,
        "segment 0 should shrink to one extent"
    );
    assert!(segment_file(dir.path(), 1).exists());
    for index in (0..rows).step_by(191) {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "row {index} lost by online migration"
        );
    }
    let xid = store.begin();
    store.put(xid, b"post-flip", b"segmented").unwrap();
    store.commit(xid).unwrap();
    store.checkpoint().unwrap();
    drop(store);

    // Reopen as what it now is: a segmented store.
    let (store, _) = PagedStore::open(dir.path(), mono.with_extent_bytes(EXTENT_BYTES)).unwrap();
    assert_eq!(
        store.get(b"post-flip").unwrap().as_deref(),
        Some(b"segmented".as_slice())
    );
    assert_eq!(
        store.get(&key(1234)).unwrap().as_deref(),
        Some(value(1234).as_slice())
    );
}
