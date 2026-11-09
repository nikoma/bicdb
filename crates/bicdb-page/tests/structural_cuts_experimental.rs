//! Proof obligations for the disabled, disposable-store-only prototype.
#![cfg(feature = "experimental-structural-cuts")]

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(512)
        .with_buffer_pool_bytes(64 * 512)
        .with_fsync(true)
        .with_wal_max_bytes(1 << 30)
}

fn fixture(dir: &TempDir) -> PagedStore {
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    for batch in 0..10 {
        let xid = store.begin();
        for i in batch * 10..batch * 10 + 10 {
            store
                .put(xid, format!("kept-{i:05}").as_bytes(), b"acknowledged")
                .unwrap();
        }
        store.commit(xid).unwrap();
    }
    store.checkpoint().unwrap();
    store
}

fn assert_kept(store: &PagedStore) {
    for i in 0..100 {
        assert_eq!(
            store.get(format!("kept-{i:05}").as_bytes()).unwrap(),
            Some(b"acknowledged".to_vec())
        );
    }
}

#[test]
fn cuts_keep_live_versions_invisible_and_never_reissue_their_xids() {
    for checkpoint in [false, true] {
        let dir = TempDir::new().unwrap();
        let pending = {
            let store = fixture(&dir);
            let pending = store.begin();
            for i in 0..2_000 {
                store
                    .put(
                        pending,
                        format!("pending-{i:05}").as_bytes(),
                        b"uncommitted",
                    )
                    .unwrap();
                if checkpoint && i % 50 == 0 {
                    store.checkpoint().unwrap();
                }
                let snapshot = store.buffer_pool().snapshot();
                // Default BufferPoolOptions uses eight shards. The prototype
                // admits at most one emergency frame per shard.
                assert!(
                    snapshot.resident_pages <= snapshot.total_frames + 8,
                    "{snapshot:?}"
                );
                assert_eq!(snapshot.unlogged_steals, 0);
            }
            assert_kept(&store);
            pending
        };
        for reopen in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_kept(&store);
            for i in 0..2_000 {
                assert_eq!(
                    store.get(format!("pending-{i:05}").as_bytes()).unwrap(),
                    None,
                    "checkpoint={checkpoint}, reopen={reopen}"
                );
            }
            let next = store.begin();
            assert!(next > pending);
            store.put(next, b"after-recovery", b"committed").unwrap();
            store.commit(next).unwrap();
        }
    }
}

#[test]
fn a_failed_cut_at_every_image_and_decision_never_publishes_partial_pages() {
    // Construct the same boundary repeatedly, then fail each image append,
    // the terminal decision, and the first append after it in turn.
    let dirty = {
        let dir = TempDir::new().unwrap();
        let store = fixture(&dir);
        let xid = store.begin();
        for i in 0..2_000 {
            if store.buffer_pool().unlogged_pressure().is_some() {
                break;
            }
            store
                .put(xid, format!("pending-{i:05}").as_bytes(), b"uncommitted")
                .unwrap();
        }
        assert!(store.buffer_pool().unlogged_pressure().is_some());
        store.buffer_pool().unlogged_pages()
    };
    for failing_write in 1..=dirty + 2 {
        let dir = TempDir::new().unwrap();
        {
            let store = fixture(&dir);
            let xid = store.begin();
            for i in 0..2_000 {
                if store.buffer_pool().unlogged_pressure().is_some() {
                    break;
                }
                store
                    .put(xid, format!("pending-{i:05}").as_bytes(), b"uncommitted")
                    .unwrap();
            }
            let fault = bicdb_page::test_faults::arm_io_failure(
                "page_log.write",
                dir.path(),
                failing_write,
                true,
            );
            let result = store.put(xid, b"trigger-cut", b"uncommitted");
            if failing_write <= dirty + 1 {
                assert!(result.is_err(), "write {failing_write}");
            }
            assert_eq!(store.buffer_pool().unlogged_steals(), 0);
            drop(fault);
        }
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_kept(&store);
            assert_eq!(store.get(b"trigger-cut").unwrap(), None);
            for i in 0..2_000 {
                assert_eq!(
                    store.get(format!("pending-{i:05}").as_bytes()).unwrap(),
                    None
                );
            }
        }
    }
}

#[test]
fn failed_maintenance_cannot_be_persisted_by_a_later_cut() {
    for sweep in [false, true] {
        let dir = TempDir::new().unwrap();
        {
            let store = fixture(&dir);
            let xid = store.begin();
            for i in 0..40 {
                store
                    .put(xid, format!("doomed-{i:04}").as_bytes(), b"deleted later")
                    .unwrap();
            }
            store.commit(xid).unwrap();
            let xid = store.begin();
            for i in 0..40 {
                store
                    .delete(xid, format!("doomed-{i:04}").as_bytes())
                    .unwrap();
            }
            store.commit(xid).unwrap();
            store.checkpoint().unwrap();
            if sweep {
                store.vacuum(10_000).unwrap();
            }
            let site = if sweep {
                "experimental.sweep.after_first_mutation"
            } else {
                "experimental.vacuum.after_first_mutation"
            };
            let fault = bicdb_page::test_faults::arm_io_failure(site, dir.path(), 1, false);
            let result = if sweep {
                store.sweep_dead_index_entries(10_000).map(|_| ())
            } else {
                store.vacuum(10_000).map(|_| ())
            };
            assert!(
                result.is_err(),
                "the maintenance fault must fire: sweep={sweep}"
            );
            assert_eq!(fault.fired(), 1);
            assert!(store.durability_health().check().is_err());
            assert!(
                store.checkpoint().is_err(),
                "a later cut must not publish failed maintenance"
            );
            drop(fault);
        }
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_kept(&store);
            for i in 0..40 {
                assert_eq!(
                    store.get(format!("doomed-{i:04}").as_bytes()).unwrap(),
                    None
                );
            }
        }
    }
}

#[test]
fn streaming_replacements_keep_old_snapshots_and_crash_visible_values() {
    let old = vec![b'o'; 100_000];
    let replacement = vec![b'n'; 120_000];
    let dir = TempDir::new().unwrap();
    {
        let store = fixture(&dir);
        let xid = store.begin();
        store.put(xid, b"wide", &old).unwrap();
        store.commit(xid).unwrap();
        let (reader, snapshot) = store.begin_transaction();
        let writer = store.begin();
        store.put(writer, b"wide", &replacement).unwrap();
        assert_eq!(
            store.get_as_of(&snapshot, b"wide").unwrap(),
            Some(old.clone())
        );
        store.abort(writer).unwrap();
        assert_eq!(store.get(b"wide").unwrap(), Some(old.clone()));
        store.abort(reader).unwrap();
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
    }
    for _ in 0..2 {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        assert_kept(&store);
        assert_eq!(store.get(b"wide").unwrap(), Some(old.clone()));
    }
    for failing_write in 1..=8 {
        let dir = TempDir::new().unwrap();
        {
            let store = fixture(&dir);
            let xid = store.begin();
            store.put(xid, b"wide", &old).unwrap();
            store.commit(xid).unwrap();
            let writer = store.begin();
            let fault = bicdb_page::test_faults::arm_io_failure(
                "page_log.write",
                dir.path(),
                failing_write,
                true,
            );
            assert!(store.put(writer, b"wide", &replacement).is_err());
            assert_eq!(fault.fired(), 1);
            assert!(store.checkpoint().is_err());
            drop(fault);
        }
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_kept(&store);
            assert_eq!(
                store.get(b"wide").unwrap(),
                Some(old.clone()),
                "write {failing_write}"
            );
            let xid = store.begin();
            store.put(xid, b"later", b"committed").unwrap();
            store.commit(xid).unwrap();
            assert_eq!(store.get(b"wide").unwrap(), Some(old.clone()));
        }
    }
}
