//! Concurrent committers must not lose each other's rows.
//!
//! Ingestion is the workload this engine exists for, and ingestion is
//! concurrent: several writers commit disjoint keys at once. Every key that
//! returned `Ok` from `commit` must be readable afterwards, and must still be
//! readable after the store is reopened.
//!
//! Disjoint keys are the important case. Writers touching the *same* key are
//! allowed to conflict — that is what first-updater-wins means. Writers touching
//! different keys have no logical reason to interfere, so any loss there is the
//! engine dropping committed work.

use std::sync::Arc;

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(false)
}

fn key(writer: usize, index: usize) -> Vec<u8> {
    format!("w{writer:03}-k{index:05}").into_bytes()
}

#[test]
fn concurrent_committers_of_disjoint_keys_lose_nothing() {
    let dir = TempDir::new().unwrap();
    let writers = 8usize;
    let per_writer = 200usize;

    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let store = Arc::new(store);

        std::thread::scope(|scope| {
            for writer in 0..writers {
                let store = Arc::clone(&store);
                scope.spawn(move || {
                    for index in 0..per_writer {
                        let (xid, _) = store.begin_transaction();
                        store
                            .put(xid, &key(writer, index), b"value")
                            .expect("put must succeed");
                        store
                            .commit(xid)
                            .expect("commit of a disjoint key must succeed");
                    }
                });
            }
        });

        // Visible immediately, before any reopen.
        let mut missing_live = Vec::new();
        for writer in 0..writers {
            for index in 0..per_writer {
                if store.get(&key(writer, index)).unwrap().is_none() {
                    missing_live.push(format!("w{writer}-k{index}"));
                }
            }
        }
        assert!(
            missing_live.is_empty(),
            "{} of {} committed rows are already missing before any reopen; first few: {:?}",
            missing_live.len(),
            writers * per_writer,
            &missing_live[..missing_live.len().min(10)]
        );
    }

    // And after a reopen, which replays the log.
    let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
    let mut missing = Vec::new();
    for writer in 0..writers {
        for index in 0..per_writer {
            if store.get(&key(writer, index)).unwrap().is_none() {
                missing.push(format!("w{writer}-k{index}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {} committed rows were lost across reopen \
         (replayed {} pages, discarded {} torn bytes); first few: {:?}",
        missing.len(),
        writers * per_writer,
        recovery.pages_replayed,
        recovery.truncated_bytes,
        &missing[..missing.len().min(10)]
    );
}

#[test]
fn concurrent_committers_survive_a_checkpoint_mid_flight() {
    // The fuzzy checkpoint runs while commits are in progress, which is when the
    // interaction between "what the log still holds" and "what the pages already
    // hold" is at its most delicate.
    let dir = TempDir::new().unwrap();
    let writers = 6usize;
    let per_writer = 150usize;

    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let store = Arc::new(store);
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        std::thread::scope(|scope| {
            {
                let store = Arc::clone(&store);
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    // Deadline as well as a completion count. A committer that
                    // panics never increments `done`, and a checkpointer that
                    // waits only on `done` would then spin forever — which means
                    // `scope` never returns, so the panic is never reported and
                    // the whole test hangs instead of failing. The bound turns
                    // that into a normal test failure.
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
                    while done.load(std::sync::atomic::Ordering::SeqCst) < writers
                        && std::time::Instant::now() < deadline
                    {
                        let _ = store.checkpoint();
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                });
            }
            for writer in 0..writers {
                let store = Arc::clone(&store);
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    for index in 0..per_writer {
                        let (xid, _) = store.begin_transaction();
                        store.put(xid, &key(writer, index), b"value").unwrap();
                        store.commit(xid).expect("commit must succeed");
                    }
                    done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
            }
        });
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    let mut missing = Vec::new();
    for writer in 0..writers {
        for index in 0..per_writer {
            if store.get(&key(writer, index)).unwrap().is_none() {
                missing.push(format!("w{writer}-k{index}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {} committed rows lost across a mid-flight checkpoint; first few: {:?}",
        missing.len(),
        writers * per_writer,
        &missing[..missing.len().min(10)]
    );
}

#[test]
fn deferring_truncation_still_bounds_the_log() {
    // A checkpoint declines to truncate while any commit sits above the freeze
    // watermark, because truncating would destroy the only evidence those
    // transactions committed. That safety rule must not turn into an unbounded
    // log: once writers finish, a checkpoint must be able to reclaim it.
    let dir = TempDir::new().unwrap();
    let (store, _) =
        PagedStore::open(dir.path(), options().with_wal_max_bytes(256 * 1024)).unwrap();

    for index in 0..3_000u32 {
        let (xid, _) = store.begin_transaction();
        store
            .put(xid, format!("k{index:06}").as_bytes(), &[b'v'; 128])
            .unwrap();
        store.commit(xid).unwrap();
    }
    // No transaction is open here, so this checkpoint can and must truncate.
    store.checkpoint().unwrap();

    let wal_bytes = std::fs::metadata(dir.path().join("store.wal"))
        .unwrap()
        .len();
    assert!(
        wal_bytes < 4 * 1024 * 1024,
        "log was not reclaimed after the writers finished: {wal_bytes} bytes"
    );

    // And the data is all still there.
    for index in (0..3_000u32).step_by(97) {
        assert!(
            store
                .get(format!("k{index:06}").as_bytes())
                .unwrap()
                .is_some(),
            "row {index} lost"
        );
    }
}
