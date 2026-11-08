//! Fail-closed handling of durability-critical I/O failures in the page log
//! and the page file. See `docs/durability-failure-handling.md`.
//!
//! Every test injects `EIO` at a named site through
//! `bicdb_page::test_faults::arm_io_failure`, scoped to its own directory so
//! the tests can run in parallel. Most faults are one-shot: the call after
//! the failing one would succeed. That is the dangerous case on Linux, where
//! a failed `fsync` can drop the dirty pages and clear the error, so a retry
//! reports success for data that never reached the disk. The store must stay
//! fenced regardless.

use std::sync::{Arc, Barrier};

use bicdb_page::test_faults::arm_io_failure;
use bicdb_page::wal::transaction_outcomes;
use bicdb_page::{DurabilityState, PagedStore, PagedStoreOptions, TxStatus, Wal};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(true)
        .with_wal_max_bytes(1024 * 1024 * 1024)
}

fn segmented_options() -> PagedStoreOptions {
    options()
        .with_extent_bytes(1024 * 1024)
        .with_wal_segment_bytes(64 * 1024)
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:06}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}-{}", "v".repeat(200)).into_bytes()
}

fn commit_rows(store: &PagedStore, rows: std::ops::Range<usize>) {
    let xid = store.begin();
    for index in rows {
        store.put(xid, &key(index), &value(index)).unwrap();
    }
    store.commit(xid).unwrap();
}

fn failed_site(state: DurabilityState) -> String {
    match state {
        DurabilityState::Healthy => panic!("expected WriteFailed, store is healthy"),
        DurabilityState::WriteFailed(failure) => failure.site,
    }
}

/// Reopen and check every acknowledged row; then prove the reopened store
/// takes writes again.
fn reopen_and_verify(dir: &TempDir, options: PagedStoreOptions, acknowledged: usize) {
    let (store, _) = PagedStore::open(dir.path(), options).unwrap();
    assert!(store.durability_health().state().is_healthy());
    for index in 0..acknowledged {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "acknowledged row {index} lost after a fail-closed reopen"
        );
    }
    commit_rows(&store, 900_000..900_001);
    store.checkpoint().unwrap();
    assert!(store.get(&key(900_000)).unwrap().is_some());
}

/// The bug the qualification found: a failed page-log fsync, then a
/// successful one, used to publish the records written before the failure as
/// durable. Now the failure is sticky and `durable_lsn` never advances.
#[test]
fn a_failed_log_sync_is_sticky_and_no_later_sync_publishes_durability() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.wal");
    let wal = Wal::open(&path, true).unwrap();
    wal.log_page_image(1, 1, &[1u8; 64]).unwrap();
    wal.commit(1).unwrap();
    let durable = wal.durable_lsn();

    let fault = arm_io_failure("page_log.sync", dir.path(), 1, false);
    wal.log_page_image(1, 2, &[2u8; 64]).unwrap();
    let error = wal.commit(2).unwrap_err();
    assert!(error.is_write_failed(), "{error}");
    assert_eq!(fault.fired(), 1);
    assert_eq!(wal.durable_lsn(), durable);

    // The injected fault is spent, so an fsync would now succeed. It must
    // not be attempted, and nothing may be reported durable.
    let error = wal.sync().unwrap_err();
    assert!(error.is_write_failed(), "{error}");
    let error = wal.sync_through(wal.next_lsn() - 1).unwrap_err();
    assert!(error.is_write_failed(), "{error}");
    assert_eq!(fault.seen(), 1, "a fenced log must not retry the fsync");
    assert_eq!(wal.durable_lsn(), durable);
    // No append is accepted either.
    assert!(wal
        .log_page_image(1, 3, &[3u8; 64])
        .unwrap_err()
        .is_write_failed());
    assert!(wal.commit(3).unwrap_err().is_write_failed());
    // An LSN already durable before the failure stays durable.
    wal.sync_through(durable).unwrap();
    assert_eq!(failed_site(wal.health().state()), "page_log.sync");
    drop(wal);

    // Recovery: commit 1 is there, commit 3 never reached the log. Commit 2's
    // record was written before its fsync failed, so its outcome is unknown;
    // either answer is consistent, and the reopened log is healthy.
    let wal = Wal::open(&path, true).unwrap();
    assert!(wal.health().state().is_healthy());
    let outcomes = transaction_outcomes(&wal).unwrap();
    assert!(outcomes.contains(&(1, TxStatus::Committed)));
    assert!(!outcomes.iter().any(|(xid, _)| *xid == 3));
}

#[test]
fn a_failed_log_write_fences_the_log() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.wal");
    let wal = Wal::open(&path, true).unwrap();
    wal.commit(1).unwrap();
    let _fault = arm_io_failure("page_log.write", dir.path(), 1, false);
    assert!(wal.commit(2).unwrap_err().is_write_failed());
    assert!(wal.commit(3).unwrap_err().is_write_failed());
    assert!(wal.sync().unwrap_err().is_write_failed());
    assert_eq!(failed_site(wal.health().state()), "page_log.write");
}

/// Group commit: every waiter on a failing sync fails, including those that
/// would retry the sync themselves. The records are appended first, so every
/// waiter's record precedes the failing fsync, and the fault is one-shot:
/// under the old behaviour the next waiter's own fsync succeeded and
/// acknowledged records whose writeback had failed.
#[test]
fn concurrent_commits_waiting_on_a_failing_sync_all_fail() {
    for writers in [8u64, 32] {
        concurrent_waiters_on_a_failing_sync(writers);
    }
}

fn concurrent_waiters_on_a_failing_sync(writers: u64) {
    let dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::open(dir.path().join("store.wal"), true).unwrap());
    wal.commit(1).unwrap();
    let durable = wal.durable_lsn();
    let lsns: Vec<_> = (0..writers)
        .map(|writer| {
            wal.log_page_image(1, 10 + writer, &[7u8; 256]).unwrap();
            wal.commit_unsynced(10 + writer).unwrap()
        })
        .collect();
    let fault = arm_io_failure("page_log.sync", dir.path(), 1, false);
    let start = Arc::new(Barrier::new(writers as usize));
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = lsns
            .iter()
            .map(|&lsn| {
                let wal = wal.clone();
                let start = start.clone();
                scope.spawn(move || {
                    start.wait();
                    wal.sync_through(lsn)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(fault.fired(), 1);
    for result in &results {
        let error = result.as_ref().expect_err("no waiter may be acknowledged");
        assert!(error.is_write_failed(), "{error}");
    }
    assert_eq!(fault.seen(), 1, "exactly one fsync attempt, never a retry");
    assert_eq!(wal.durable_lsn(), durable);
}

/// A paged store: the failed commit fences the store; reads continue at the
/// page layer; reopening recovers every acknowledged row and writes resume.
#[test]
fn a_failed_commit_sync_fences_the_paged_store_until_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        commit_rows(&store, 0..20);
        let _fault = arm_io_failure("page_log.sync", dir.path(), 1, false);
        let xid = store.begin();
        store.put(xid, &key(20), &value(20)).unwrap();
        assert!(store.commit(xid).unwrap_err().is_write_failed());
        assert_eq!(
            failed_site(store.durability_health().state()),
            "page_log.sync"
        );
        // The failed commit is not visible.
        assert!(store.get(&key(20)).unwrap().is_none());
        // Reads of acknowledged data continue.
        assert!(store.get(&key(3)).unwrap().is_some());
        // Every write path refuses.
        let xid = store.begin();
        let refused = store
            .put(xid, &key(21), &value(21))
            .and_then(|()| store.commit(xid));
        assert!(refused.unwrap_err().is_write_failed());
        assert!(store.checkpoint().unwrap_err().is_write_failed());
    }
    reopen_and_verify(&dir, options(), 20);
}

/// A failure in each checkpoint phase fences the store and the next open
/// recovers every acknowledged row.
#[test]
fn a_failure_in_each_checkpoint_phase_fences_and_recovers() {
    // (site, options): a log sync (the pre-flush sync, a writeback barrier
    // or the checkpoint record's), the page-file sync, a log append (a
    // barrier image or the checkpoint record), and the log truncation, on a
    // monolithic and on a segmented store.
    let cases: &[(&str, fn() -> PagedStoreOptions)] = &[
        ("page_log.sync", options),
        ("page_file.sync", options),
        ("page_log.write", options),
        ("page_log.reset", options),
        ("page_log.reset", segmented_options),
    ];
    for (site, make_options) in cases {
        let dir = TempDir::new().unwrap();
        {
            let (store, _) = PagedStore::open(dir.path(), make_options()).unwrap();
            commit_rows(&store, 0..40);
            store.checkpoint().unwrap();
            commit_rows(&store, 40..80);
            let fault = arm_io_failure(site, dir.path(), 1, false);
            let error = store.checkpoint().unwrap_err();
            assert_eq!(fault.fired(), 1, "{site}: fault not reached");
            assert!(error.is_write_failed(), "{site}: {error}");
            assert_eq!(failed_site(store.durability_health().state()), *site);
            let xid = store.begin();
            let refused = store
                .put(xid, &key(80), &value(80))
                .and_then(|()| store.commit(xid));
            assert!(refused.unwrap_err().is_write_failed(), "{site}");
            assert!(store.checkpoint().unwrap_err().is_write_failed(), "{site}");
        }
        reopen_and_verify(&dir, make_options(), 80);
    }
}

/// Segment seal: a failed sync or directory sync of a sealing segment
/// fences the log. The directory sync used to be ignored.
#[test]
fn a_failed_segment_seal_fences_the_log() {
    for site in ["page_log.seal_sync", "page_log.seal_dir_sync"] {
        let dir = TempDir::new().unwrap();
        let mut acknowledged = 0usize;
        {
            let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
            let fault = arm_io_failure(site, dir.path(), 1, false);
            let mut failed = false;
            for chunk in 0..200usize {
                let rows = chunk * 10..chunk * 10 + 10;
                let xid = store.begin();
                let result = rows
                    .clone()
                    .try_for_each(|index| store.put(xid, &key(index), &value(index)))
                    .and_then(|()| store.commit(xid));
                match result {
                    Ok(()) => {
                        assert!(!failed, "{site}: commit acknowledged after the failure");
                        acknowledged = rows.end;
                    }
                    Err(error) => {
                        assert!(error.is_write_failed(), "{site}: {error}");
                        failed = true;
                    }
                }
                if failed && fault.fired() > 0 && chunk > 60 {
                    break;
                }
            }
            assert!(failed, "{site}: no segment sealed");
            assert_eq!(failed_site(store.durability_health().state()), site);
        }
        reopen_and_verify(&dir, segmented_options(), acknowledged);
    }
}

/// A failed page-file write is retryable, not fatal: the frame stays dirty
/// and the log still holds its redo. The retry then succeeds end to end.
#[test]
fn a_failed_page_write_is_retryable_and_not_a_durability_failure() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        commit_rows(&store, 0..40);
        let fault = arm_io_failure("page_file.write", dir.path(), 1, false);
        let error = store.checkpoint().unwrap_err();
        assert_eq!(fault.fired(), 1);
        assert!(!error.is_write_failed(), "{error}");
        assert!(store.durability_health().state().is_healthy());
        store.checkpoint().unwrap();
        commit_rows(&store, 40..60);
    }
    reopen_and_verify(&dir, options(), 60);
}

/// A failure while recovery replays the log fails the open and leaves the
/// log intact; the next open replays it completely.
#[test]
fn a_failure_during_recovery_replay_fails_the_open_and_a_retry_recovers() {
    for site in ["page_file.write", "page_file.sync"] {
        let dir = TempDir::new().unwrap();
        {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            commit_rows(&store, 0..40);
            // Dropped without a checkpoint: the rows live only in the log.
        }
        {
            let fault = arm_io_failure(site, dir.path(), 1, true);
            let error = PagedStore::open(dir.path(), options())
                .err()
                .unwrap_or_else(|| panic!("{site}: open must fail"));
            assert!(fault.fired() >= 1, "{site}: replay never reached the site");
            eprintln!("{site}: recovery failed as injected: {error}");
        }
        reopen_and_verify(&dir, options(), 40);
    }
}
