//! Sealed-segment WAL: the active `store.wal` seals into immutable
//! `store.wal.<seq>` files as it fills; recovery replays sealed segments in
//! order then the active tail; checkpoint truncation deletes sealed files.
//! Only engaged on extent-segmented (v2) stores.

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

const PAGE_SIZE: u32 = 4096;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(1024 * 1024 * 1024) // never trip backpressure here
        .with_extent_bytes(1024 * 1024) // v2 store → segmentation active
        .with_wal_segment_bytes(64 * 1024) // tiny segments: force seals
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:06}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}-{}", "w".repeat(300)).into_bytes()
}

fn sealed_segments(dir: &TempDir) -> Vec<std::path::PathBuf> {
    let mut sealed: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.strip_prefix("store.wal.")
                        .is_some_and(|suffix| suffix.parse::<u64>().is_ok())
                })
        })
        .collect();
    sealed.sort();
    sealed
}

#[test]
fn wal_seals_segments_and_recovery_replays_the_whole_chain() {
    let dir = TempDir::new().unwrap();
    let rows = 800usize;
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for chunk in (0..rows).collect::<Vec<_>>().chunks(50) {
            let xid = store.begin();
            for index in chunk {
                store.put(xid, &key(*index), &value(*index)).unwrap();
            }
            store.commit(xid).unwrap();
        }
        assert!(
            !sealed_segments(&dir).is_empty(),
            "expected sealed WAL segments after ~{}KB of log",
            rows * 350 / 1024
        );
        // Crash without checkpoint.
    }
    let sealed_before = sealed_segments(&dir).len();
    assert!(sealed_before >= 2, "want a real chain, got {sealed_before}");

    // Recovery must replay sealed segments in order, then the active tail.
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    for index in 0..rows {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "row {index} lost across sealed-segment recovery"
        );
    }

    // Checkpoint truncation deletes every sealed segment.
    store.checkpoint().unwrap();
    assert!(
        sealed_segments(&dir).is_empty(),
        "checkpoint truncation must delete sealed segments"
    );
    let active_len = std::fs::metadata(dir.path().join("store.wal"))
        .unwrap()
        .len();
    assert_eq!(active_len, 0, "active WAL should be reset");

    // Writes keep flowing after truncation, sealing anew.
    for chunk in (0..rows).collect::<Vec<_>>().chunks(50) {
        let xid = store.begin();
        for index in chunk {
            store
                .put(xid, &format!("post-{index}").into_bytes(), &value(*index))
                .unwrap();
        }
        store.commit(xid).unwrap();
    }
    assert!(!sealed_segments(&dir).is_empty());
    drop(store);
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(store.get(b"post-7").unwrap().is_some());
    assert_eq!(
        store.get(&key(5)).unwrap().as_deref(),
        Some(value(5).as_slice())
    );
}

#[test]
fn corrupt_sealed_segment_fails_closed_instead_of_truncating() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for chunk in (0..600).collect::<Vec<_>>().chunks(50) {
            let xid = store.begin();
            for index in chunk {
                store.put(xid, &key(*index), &value(*index)).unwrap();
            }
            store.commit(xid).unwrap();
        }
    }
    let sealed = sealed_segments(&dir);
    assert!(sealed.len() >= 2);
    // Flip a byte in the MIDDLE of the first sealed segment.
    let target = &sealed[0];
    let mut bytes = std::fs::read(target).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xFF;
    std::fs::write(target, bytes).unwrap();

    let result = PagedStore::open(dir.path(), options());
    assert!(
        result.is_err(),
        "a corrupt sealed segment must refuse to open, not silently truncate \
         away the later segments' committed transactions"
    );
}

#[test]
fn monolithic_stores_keep_a_single_unsegmented_wal() {
    let dir = TempDir::new().unwrap();
    let mono = PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(1024 * 1024 * 1024)
        .with_extent_bytes(0)
        .with_wal_segment_bytes(64 * 1024);
    let (store, _) = PagedStore::open(dir.path(), mono).unwrap();
    for chunk in (0..600).collect::<Vec<_>>().chunks(50) {
        let xid = store.begin();
        for index in chunk {
            store.put(xid, &key(*index), &value(*index)).unwrap();
        }
        store.commit(xid).unwrap();
    }
    assert!(
        sealed_segments(&dir).is_empty(),
        "legacy monolithic stores must not grow sealed WAL segments a \
         pre-segmentation engine would ignore"
    );
}
