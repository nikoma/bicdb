use std::sync::{Arc, Barrier};

use bicdb_page::{
    PageBackupLimits, PageHeader, PageStore, PageStoreOptions, PageType, PAGE_HEADER_BYTES,
};

fn open(root: &std::path::Path) -> Arc<PageStore> {
    Arc::new(
        PageStore::open(
            root.join("store.pages"),
            PageStoreOptions {
                extent_bytes: 1024 * 1024,
                fsync: true,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn write(store: &PageStore, page: u64, value: u8) {
    let mut bytes = vec![0; store.page_size() as usize];
    PageHeader::new(page, PageType::Heap, store.page_size()).encode(&mut bytes);
    bytes[PAGE_HEADER_BYTES] = value;
    store.write_page(page, &mut bytes).unwrap();
}

#[test]
fn capture_preserves_old_bytes_while_a_real_writer_completes() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path());
    let page = store.allocate(PageType::Heap).unwrap();
    write(&store, page, 41);
    store.sync().unwrap();
    let journal = root.path().join("before-images");
    let snapshot = store
        .begin_backup_snapshot(&journal, PageBackupLimits::default())
        .unwrap();
    let gate = Arc::new(Barrier::new(2));
    let writer = {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            gate.wait();
            for value in 42..=64 {
                write(&store, page, value);
            }
            store.sync().unwrap();
        })
    };
    gate.wait();
    writer.join().unwrap(); // Snapshot remains active throughout every write.
    let mut bytes = vec![0; store.page_size() as usize];
    snapshot.read_page(page, &mut bytes).unwrap();
    assert_eq!(bytes[PAGE_HEADER_BYTES], 41);
    store.read_page(page, &mut bytes).unwrap();
    assert_eq!(bytes[PAGE_HEADER_BYTES], 64);
    assert_eq!(snapshot.stats().unwrap().before_image_pages, 1);
    drop(snapshot);
    assert!(!journal.exists());
}

#[test]
fn budget_exhaustion_aborts_backup_without_rejecting_page_writes() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path());
    let first = store.allocate(PageType::Heap).unwrap();
    let second = store.allocate(PageType::Heap).unwrap();
    write(&store, first, 1);
    write(&store, second, 2);
    let snapshot = store
        .begin_backup_snapshot(
            &root.path().join("journal"),
            PageBackupLimits {
                max_before_image_bytes: u64::from(store.page_size()),
                max_before_image_pages: 1,
                ..Default::default()
            },
        )
        .unwrap();
    write(&store, first, 11);
    write(&store, second, 22);
    let mut bytes = vec![0; store.page_size() as usize];
    assert!(snapshot
        .read_page(first, &mut bytes)
        .unwrap_err()
        .to_string()
        .contains("budget"));
    assert!(snapshot.stats().is_err());
    store.read_page(second, &mut bytes).unwrap();
    assert_eq!(bytes[PAGE_HEADER_BYTES], 22);
}

#[test]
fn consumed_pages_do_not_accumulate_images_and_cancel_is_sticky() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path());
    let first = store.allocate(PageType::Heap).unwrap();
    let second = store.allocate(PageType::Heap).unwrap();
    let snapshot = store
        .begin_backup_snapshot(&root.path().join("journal"), PageBackupLimits::default())
        .unwrap();
    assert!(store
        .begin_backup_snapshot(&root.path().join("other"), PageBackupLimits::default())
        .is_err());
    let mut bytes = vec![0; store.page_size() as usize];
    snapshot.read_page(first, &mut bytes).unwrap();
    write(&store, first, 99);
    assert_eq!(snapshot.stats().unwrap().before_image_pages, 0);
    snapshot.cancel();
    write(&store, second, 100);
    assert!(snapshot
        .read_page(second, &mut bytes)
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    drop(snapshot);
    store
        .begin_backup_snapshot(&root.path().join("new"), PageBackupLimits::default())
        .unwrap();
}

#[test]
fn extent_epochs_survive_reopen_and_distinguish_writes_after_the_cut() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path());
    let per_extent = 1024 * 1024 / u64::from(store.page_size());
    store.reserve(per_extent * 3).unwrap();
    write(&store, per_extent + 1, 11);
    let capture = |store: &PageStore, name: &str| {
        store
            .begin_backup_snapshot(&root.path().join(name), PageBackupLimits::default())
            .unwrap()
    };
    let first = capture(&store, "a");
    let baseline = first.generations.clone().unwrap();
    write(&store, per_extent + 1, 12); // Must belong to the NEXT cut.
    drop(first);
    let second = capture(&store, "b");
    let changed = second.generations.clone().unwrap();
    drop(second);
    assert_eq!(baseline.identity, changed.identity);
    assert_eq!(baseline.versions[0], changed.versions[0]);
    assert_ne!(baseline.versions[1], changed.versions[1]);
    assert_eq!(baseline.versions[2], changed.versions[2]);
    drop(store);
    let store = open(root.path());
    let third = capture(&store, "c");
    let reopened = third.generations.clone().unwrap();
    drop(third);
    assert_eq!(changed.identity, reopened.identity);
    assert_eq!(changed.versions, reopened.versions);
    let tracker = root.path().join(".backup-extents-store.pages");
    drop(store);
    // Simulate a torn tracking record. Its old identity must never authorize reuse.
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&tracker)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    let store = open(root.path());
    let durable = std::fs::read(&tracker).unwrap();
    assert_ne!(
        hex::encode(&durable[8..24]),
        reopened.identity,
        "corrupt tracking identity must be replaced before any writes, even without another backup"
    );
    write(&store, per_extent + 1, 13);
    drop(store);
    let store = open(root.path()); // A second reopen must keep the reset durable.
    let reset = capture(&store, "d");
    assert_ne!(
        reopened.identity,
        reset.generations.as_ref().unwrap().identity
    );
}

#[test]
fn mandatory_tracked_profiles_preserve_relayout_and_old_magic_separation() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path());
    let page_size = store.page_size();
    let path = root.path().join("store.pages");
    let snapshot = store
        .begin_backup_snapshot(&root.path().join("journal"), PageBackupLimits::default())
        .unwrap();
    drop(snapshot);
    drop(store);
    let magic = || {
        let bytes = std::fs::read(&path).unwrap();
        bytes[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 8].to_vec()
    };
    // This is a byte/source compatibility assertion, not historical-binary
    // execution: main519's literal accepted set was PG0/PG2/PG3/PG4.
    let pre_gate_magics = [*b"BICDBPG\0", *b"BICDBPG2", *b"BICDBPG3", *b"BICDBPG4"];
    let assert_current_profile = |expected: &[u8; 8], stage: &str| {
        let actual = magic();
        eprintln!("ACTUAL_MANDATORY_TRACKED_PROFILE stage={stage} bytes={actual:?}");
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "exact mandatory profile at {stage}"
        );
        assert!(
            pre_gate_magics
                .iter()
                .all(|old| old.as_slice() != actual.as_slice()),
            "current tracked output must be outside the pre-gate accepted byte set at {stage}"
        );
        actual
    };
    let tracked_segmented = assert_current_profile(b"BICDBPG7", "initial-tracked-segmented");
    let store = PageStore::open_existing_auto(&path, true).unwrap();
    store.flush().unwrap();
    drop(store);
    assert_current_profile(b"BICDBPG7", "reopened-tracked-segmented");
    bicdb_page::convert_extent_layout(&path, page_size, 0, true).unwrap();
    let tracked_monolithic = assert_current_profile(b"BICDBPG8", "relayout-tracked-monolithic");
    assert_ne!(
        tracked_segmented, tracked_monolithic,
        "tracking and reader fencing must preserve the layout distinction"
    );
    let store = PageStore::open_existing_auto(&path, true).unwrap();
    assert_eq!(store.extent_pages(), 0);
    drop(store);
    bicdb_page::convert_extent_layout(&path, page_size, 1024 * 1024, true).unwrap();
    assert_current_profile(b"BICDBPG7", "relayout-back-tracked-segmented");
    let store = PageStore::open_existing_auto(&path, true).unwrap();
    assert_eq!(store.extent_pages(), (1024 * 1024) / u64::from(page_size));
    let snapshot = store
        .begin_backup_snapshot(
            &root.path().join("after-relayout"),
            PageBackupLimits::default(),
        )
        .unwrap();
    assert!(snapshot.generations.is_some());
}
