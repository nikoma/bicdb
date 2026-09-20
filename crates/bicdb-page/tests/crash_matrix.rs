//! Crash-safety matrix.
//!
//! Phase 2's gate in `docs/server-paged-storage-todo.md`: "a crash/restart
//! matrix at every WAL/page/checkpoint boundary loses no committed row, exposes
//! no uncommitted row, and replays idempotently."
//!
//! # How a crash is simulated
//!
//! Dropping a [`PagedStore`] without checkpointing is a faithful stand-in for a
//! process kill: nothing is flushed on drop, the buffer pool's dirty pages are
//! simply lost, and the only durable record of recent work is the WAL. Reopening
//! then exercises the real recovery path.
//!
//! Truncation-based crashes go further and damage the log itself, covering the
//! case where the kill landed mid-write.
//!
//! # The three properties
//!
//! Every test here asserts against one or more of:
//!
//! 1. **no committed row is lost** — if `commit` returned, the row survives;
//! 2. **no uncommitted row is exposed** — work without a commit record never
//!    becomes visible;
//! 3. **replay is idempotent** — recovering twice equals recovering once, so an
//!    interrupted recovery is safe to restart.

use std::fs::OpenOptions;
use std::path::Path;

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(512)
        .with_buffer_pool_bytes(32 * 512)
        .with_fsync(false)
        // Large enough that no automatic checkpoint fires: these tests decide
        // when checkpoints happen.
        .with_wal_max_bytes(1 << 30)
}

fn open(dir: &Path) -> PagedStore {
    PagedStore::open(dir, options()).unwrap().0
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:06}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}-{}", "d".repeat(index % 25)).into_bytes()
}

/// Write `count` rows and commit them. Returns without checkpointing.
fn write_committed(store: &PagedStore, range: std::ops::Range<usize>) {
    let transaction = store.begin();
    for index in range {
        store.put(transaction, &key(index), &value(index)).unwrap();
    }
    store.commit(transaction).unwrap();
}

fn assert_present(store: &PagedStore, range: std::ops::Range<usize>) {
    for index in range {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "committed row {index} was lost"
        );
    }
}

#[test]
fn crash_immediately_after_commit_loses_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..120);
        // Crash: no checkpoint, no flush.
    }

    let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(
        recovery.pages_replayed > 0,
        "nothing was replayed; the log is not carrying the writes"
    );
    assert_present(&store, 0..120);
}

#[test]
fn crash_before_commit_exposes_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..40);

        // A second transaction that never commits.
        let doomed = store.begin();
        for index in 100..160 {
            store.put(doomed, &key(index), &value(index)).unwrap();
        }
        // Crash before commit.
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_present(&store, 0..40);
    for index in 100..160 {
        assert!(
            store.get(&key(index)).unwrap().is_none(),
            "uncommitted row {index} became visible after recovery"
        );
    }
}

#[test]
fn crash_after_checkpoint_loses_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..100);
        store.checkpoint().unwrap();
        // Crash right after the checkpoint.
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_present(&store, 0..100);
}

#[test]
fn crash_between_checkpoint_and_further_commits_keeps_both() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..80);
        store.checkpoint().unwrap();
        // More work after the checkpoint, committed but not checkpointed.
        write_committed(&store, 80..140);
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_present(&store, 0..140);
}

#[test]
fn repeated_crashes_accumulate_correctly() {
    // Each generation commits without checkpointing, then "crashes". Every
    // generation's rows must survive every later crash.
    let dir = TempDir::new().unwrap();
    for generation in 0..6usize {
        let store = open(dir.path());
        assert_present(&store, 0..generation * 20);
        write_committed(&store, generation * 20..(generation + 1) * 20);
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_present(&store, 0..120);
    assert_eq!(store.len().unwrap(), 120);
}

#[test]
fn recovery_is_idempotent_across_repeated_reopens() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..90);
    }

    // Reopening repeatedly must converge, not drift: an interrupted recovery
    // that is restarted has to be safe.
    let mut lengths = Vec::new();
    for _ in 0..4 {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        assert_present(&store, 0..90);
        lengths.push(store.len().unwrap());
    }
    assert!(
        lengths.windows(2).all(|w| w[0] == w[1]),
        "row count drifted across reopens: {lengths:?}"
    );
    assert_eq!(lengths[0], 90);
}

#[test]
fn a_log_truncated_mid_record_recovers_everything_before_the_tear() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..60);
        store.checkpoint().unwrap();
        write_committed(&store, 60..90);
    }

    // Tear the log's tail, as a kill mid-write would.
    let wal_path = dir.path().join("store.wal");
    let len = std::fs::metadata(&wal_path).unwrap().len();
    assert!(len > 64, "log is too small for this test to mean anything");
    let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
    file.set_len(len - 32).unwrap();
    drop(file);

    let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(
        recovery.truncated_bytes > 0,
        "the torn tail was not detected"
    );
    // Everything checkpointed is certainly safe. Rows whose commit record
    // survived the tear must also be present; rows whose commit was destroyed
    // may legitimately be gone — that is what "not committed" means.
    assert_present(&store, 0..60);
}

#[test]
fn a_log_truncated_to_nothing_falls_back_to_the_last_checkpoint() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..70);
        store.checkpoint().unwrap();
        write_committed(&store, 70..110);
    }

    // Destroy the log entirely: everything after the checkpoint is
    // unrecoverable, everything up to it must still be intact.
    std::fs::write(dir.path().join("store.wal"), b"").unwrap();

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_present(&store, 0..70);
}

#[test]
fn a_garbage_log_does_not_corrupt_checkpointed_data() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..50);
        store.checkpoint().unwrap();
    }

    // Random bytes where the log should be. Recovery must reject them rather
    // than interpret them as records.
    std::fs::write(dir.path().join("store.wal"), vec![0x5Au8; 4096]).unwrap();

    let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(
        recovery.pages_replayed, 0,
        "garbage was interpreted as replayable records"
    );
    assert_present(&store, 0..50);
}

#[test]
fn updates_and_deletes_survive_a_crash_with_the_right_final_state() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..60);
        store.checkpoint().unwrap();

        let transaction = store.begin();
        for index in (0..60).step_by(2) {
            store
                .put(transaction, &key(index), b"updated-value-that-is-longer")
                .unwrap();
        }
        for index in (1..60).step_by(10) {
            store.delete(transaction, &key(index)).unwrap();
        }
        store.commit(transaction).unwrap();
        // Crash without checkpointing the second transaction.
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    for index in 0..60 {
        let found = store.get(&key(index)).unwrap();
        if index % 10 == 1 {
            assert!(found.is_none(), "row {index} should have stayed deleted");
        } else if index % 2 == 0 {
            assert_eq!(
                found.as_deref(),
                Some(&b"updated-value-that-is-longer"[..]),
                "row {index} lost its update"
            );
        } else {
            assert_eq!(found.as_deref(), Some(value(index).as_slice()));
        }
    }
}

#[test]
fn large_values_survive_a_crash() {
    let dir = TempDir::new().unwrap();
    let big: Vec<u8> = (0..40_000).map(|i| (i % 251) as u8).collect();
    {
        let store = open(dir.path());
        let transaction = store.begin();
        store.put(transaction, b"wide", &big).unwrap();
        store.put(transaction, b"narrow", b"small").unwrap();
        store.commit(transaction).unwrap();
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(
        store.get(b"wide").unwrap().unwrap(),
        big,
        "an overflow-chained value did not survive the crash"
    );
    assert_eq!(
        store.get(b"narrow").unwrap().as_deref(),
        Some(&b"small"[..])
    );
}

#[test]
fn recovery_stays_within_the_buffer_pool_budget() {
    // Recovery must not be the thing that blows the memory envelope: it writes
    // through the page store rather than pulling every page into cache.
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        write_committed(&store, 0..800);
    }

    let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(recovery.pages_replayed > 0);
    let snapshot = store.buffer_pool().snapshot();
    assert!(
        snapshot.resident_bytes <= snapshot.budget_bytes,
        "recovery left {} bytes resident against a {} budget",
        snapshot.resident_bytes,
        snapshot.budget_bytes
    );
    assert_present(&store, 0..800);
}

#[test]
fn a_crash_during_a_growing_update_keeps_one_coherent_version() {
    // A growing update moves the tuple and rewrites the index entry. A crash
    // must leave either the old row or the new one — never a locator pointing
    // at a slot that no longer holds it.
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        let transaction = store.begin();
        store.put(transaction, b"k", b"short").unwrap();
        store.commit(transaction).unwrap();
        store.checkpoint().unwrap();

        let second = store.begin();
        store.put(second, b"k", &vec![b'g'; 5_000]).unwrap();
        store.commit(second).unwrap();
    }

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    let found = store.get(b"k").unwrap().expect("row vanished entirely");
    assert!(
        found == b"short".to_vec() || found == vec![b'g'; 5_000],
        "row is neither the old nor the new version ({} bytes)",
        found.len()
    );
}
