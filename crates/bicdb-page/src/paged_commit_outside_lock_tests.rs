//! Qualification of row commits whose page-log sync runs outside the writer
//! lock (`paged_commit_outside_lock.rs`, `docs/commit-fsync-outside-locks.md`).

use super::*;
use crate::test_faults::{arm_io_failure, arm_io_hook};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(20);

fn options(gate: bool) -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(true)
        .with_commit_sync_outside_lock(gate)
}

fn open(dir: &TempDir, gate: bool) -> PagedStore {
    PagedStore::open(dir.path(), options(gate)).unwrap().0
}

/// One row commit, appended under the row-commit guard. Not awaited.
fn append(store: &PagedStore, key: &[u8], value: &[u8]) -> CommitTicket {
    let _guard = store.row_commit_guard();
    let xid = store.begin_row_commit();
    store.put(xid, key, value).unwrap();
    store.commit_append(xid).unwrap()
}

fn row_commit(store: &PagedStore, key: &[u8], value: &[u8]) -> Result<()> {
    let ticket = append(store, key, value);
    store.await_commit(ticket)
}

/// A pause at the next page-log sync of `dir`: `reached` fires when the sync
/// is about to run, and it proceeds when `release` is sent.
fn pause_next_sync(
    dir: &TempDir,
) -> (
    crate::test_faults::IoFaultGuard,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let reached_tx = parking_lot::Mutex::new(reached_tx);
    let release_rx = parking_lot::Mutex::new(release_rx);
    let guard = arm_io_hook("page_log.sync", dir.path(), 1, move || {
        let _ = reached_tx.lock().send(());
        let _ = release_rx.lock().recv_timeout(WAIT);
    });
    (guard, reached_rx, release_tx)
}

#[test]
fn the_gate_is_off_by_default_and_off_means_the_locked_commit() {
    assert!(!PagedStoreOptions::default().commit_sync_outside_lock);
    let dir = TempDir::new().unwrap();
    let store = open(&dir, false);
    assert!(!store.commit_sync_outside_lock());
    for i in 0..20u32 {
        let key = format!("k{i}");
        row_commit(&store, key.as_bytes(), b"v").unwrap();
        // Gate off: commit_append is the synced commit, visible on return.
        assert_eq!(store.get(key.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]));
    }
    let tx = store.begin();
    store.put(tx, b"plain", b"v").unwrap();
    store.commit(tx).unwrap();
    store.checkpoint().unwrap();
    assert_eq!(store.committing_stats(), CommittingStats::default());
    // Off never records a deferred commit either: every commit was synced.
    assert_eq!(store.wal().deferred_commit_lsn(), 0);
}

#[test]
fn a_writer_proceeds_while_a_commit_sync_is_in_flight_and_readers_do_not_see_it() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    row_commit(&store, b"warm", b"v").unwrap();
    let (_hook, reached, release) = pause_next_sync(&dir);
    std::thread::scope(|scope| {
        let committer = scope.spawn(|| row_commit(&store, b"k1", b"v1"));
        reached.recv_timeout(WAIT).expect("the commit reached its sync");
        // The record is appended but not durable; the commit is not visible.
        assert!(store.wal().durable_lsn() < store.wal().next_lsn() - 1);
        assert_eq!(store.get(b"k1").unwrap(), None, "visible before durable");
        assert_eq!(store.committing_stats().depth, 1);
        // The writer lock is free: another row commit can do its page work.
        let other = scope.spawn(|| {
            let guard = store
                .write_lock
                .try_lock_for(Duration::from_secs(5))
                .expect("the writer lock is held across a commit sync");
            let xid = store.begin_row_commit();
            store.put(xid, b"k2", b"v2").unwrap();
            drop(guard);
            xid
        });
        let xid2 = other.join().unwrap();
        assert_eq!(store.get(b"k1").unwrap(), None);
        release.send(()).unwrap();
        committer.join().unwrap().unwrap();
        assert_eq!(store.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
        let ticket = {
            let _guard = store.row_commit_guard();
            store.commit_append(xid2).unwrap()
        };
        store.await_commit(ticket).unwrap();
    });
    assert_eq!(store.get(b"k2").unwrap().as_deref(), Some(&b"v2"[..]));
    let stats = store.committing_stats();
    assert_eq!(stats.enqueued, 3);
    assert_eq!(stats.published, 3);
    assert_eq!(stats.depth, 0);
    assert_eq!(stats.overlap_waits, 0);
}

#[test]
fn publication_is_durable_first_and_in_commit_record_order() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let recorded = parking_lot::Mutex::new(Vec::<(Lsn, Vec<u8>)>::new());
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        for writer in 0..8u32 {
            let store = &store;
            let recorded = &recorded;
            scope.spawn(move || {
                for i in 0..60u32 {
                    let key = format!("w{writer}-{i}").into_bytes();
                    let ticket = append(store, &key, b"v");
                    recorded.lock().push((ticket.lsn, key));
                    store.await_commit(ticket).unwrap();
                }
            });
        }
        let monitor = scope.spawn(|| {
            let mut checks = 0u64;
            while !done.load(Ordering::Acquire) {
                let entries = recorded.lock().clone();
                let snapshot = store.latest_snapshot();
                let mut visible = Vec::new();
                for (lsn, key) in &entries {
                    let seen = store.get_as_of(&snapshot, key).unwrap().is_some();
                    visible.push((*lsn, seen));
                }
                let durable = store.wal().durable_lsn();
                let max_visible = visible
                    .iter()
                    .filter(|(_, seen)| *seen)
                    .map(|(lsn, _)| *lsn)
                    .max();
                for (lsn, seen) in &visible {
                    if *seen {
                        assert!(*lsn <= durable, "commit {lsn} visible before durable {durable}");
                    } else if let Some(max) = max_visible {
                        assert!(*lsn > max, "commit {lsn} invisible behind visible {max}");
                    }
                }
                checks += 1;
            }
            checks
        });
        // Writers finish, then stop the monitor.
        while recorded.lock().len() < 8 * 60 {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        done.store(true, Ordering::Release);
        assert!(monitor.join().unwrap() > 0);
    });
    let stats = store.committing_stats();
    assert_eq!(stats.enqueued, 480);
    assert_eq!(stats.published, 480);
    assert_eq!(stats.overlap_waits, 0);
    drop(store);
    let store = open(&dir, true);
    for writer in 0..8u32 {
        for i in 0..60u32 {
            let key = format!("w{writer}-{i}");
            assert!(store.get(key.as_bytes()).unwrap().is_some(), "{key} lost");
        }
    }
}

/// The checkpoint barrier: a checkpoint that runs while a commit is
/// committing must resolve it first. Without the barrier, the checkpoint
/// truncates the log under the commit, the waiter returns success, and the
/// commit is gone after reopen.
#[test]
fn a_checkpoint_resolves_committing_transactions_before_it_truncates() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let ticket = append(&store, b"k", b"v");
    assert_eq!(store.committing_stats().depth, 1);
    store.checkpoint().unwrap();
    assert_eq!(
        store.transactions.status(ticket.xid),
        TxStatus::Committed,
        "the barrier publishes the committing transaction before the checkpoint"
    );
    assert!(store.committing_stats().barrier_drains >= 1);
    store.await_commit(ticket).unwrap();
    drop(store);
    let store = open(&dir, true);
    assert_eq!(
        store.get(b"k").unwrap().as_deref(),
        Some(&b"v"[..]),
        "an acknowledged commit was lost across a checkpoint"
    );
}

#[test]
fn a_bounded_checkpoint_resolves_committing_transactions_before_it_truncates() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let ticket = append(&store, b"k", b"v");
    let mut cursor = PagedCheckpointCursor::default();
    for _ in 0..64 {
        let report = store
            .checkpoint_step(cursor, PagedCheckpointLimits::default())
            .unwrap();
        if report.complete {
            break;
        }
        cursor = report.next_cursor;
    }
    assert_eq!(store.transactions.status(ticket.xid), TxStatus::Committed);
    store.await_commit(ticket).unwrap();
    drop(store);
    let store = open(&dir, true);
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

/// A checkpoint racing a commit whose sync is paused waits for it, then
/// both finish and the commit survives reopen.
#[test]
fn a_checkpoint_waits_for_a_paused_commit_sync() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let (_hook, reached, release) = pause_next_sync(&dir);
    std::thread::scope(|scope| {
        let committer = scope.spawn(|| row_commit(&store, b"k", b"v"));
        reached.recv_timeout(WAIT).unwrap();
        let checkpoint = scope.spawn(|| store.checkpoint());
        std::thread::sleep(Duration::from_millis(100));
        assert!(!checkpoint.is_finished(), "checkpoint ran past a committing commit");
        release.send(()).unwrap();
        committer.join().unwrap().unwrap();
        checkpoint.join().unwrap().unwrap();
    });
    drop(store);
    let store = open(&dir, true);
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

/// Backup capture is a writer-lock holder too: it sees no committing entry.
#[test]
fn non_row_writers_never_coexist_with_a_committing_transaction() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let first = append(&store, b"a", b"1");
    // A non-row page transaction (bulk-load shape) drains at its writer lock.
    {
        let _guard = store.write_guard();
        assert_eq!(store.committing_stats().depth, 0);
        assert_eq!(store.transactions.status(first.xid), TxStatus::Committed);
    }
    let second = append(&store, b"b", b"2");
    store.vacuum(16).unwrap();
    assert_eq!(store.transactions.status(second.xid), TxStatus::Committed);
    store.await_commit(first).unwrap();
    store.await_commit(second).unwrap();
}

#[test]
fn one_failed_sync_fails_every_queued_commit_and_publishes_nothing() {
    for queued in [1usize, 4, 16] {
        let dir = TempDir::new().unwrap();
        let store = open(&dir, true);
        row_commit(&store, b"before", b"v").unwrap();
        let tickets: Vec<_> = (0..queued)
            .map(|i| append(&store, format!("q{i}").as_bytes(), b"v"))
            .collect();
        let failure = arm_io_failure("page_log.sync", dir.path(), 1, false);
        let results: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = tickets
                .iter()
                .map(|ticket| {
                    let store = &store;
                    let ticket = *ticket;
                    scope.spawn(move || store.await_commit(ticket))
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(failure.fired(), 1, "exactly one fsync attempted and failed");
        for (i, result) in results.iter().enumerate() {
            assert!(
                matches!(result, Err(PageError::WriteFailed { .. })),
                "queued commit {i} of {queued}: {result:?}"
            );
            assert_eq!(store.get(format!("q{i}").as_bytes()).unwrap(), None);
        }
        let stats = store.committing_stats();
        assert_eq!(stats.depth as usize, queued, "a failed queue is never published");
        assert_eq!(stats.published, 1);
        // Later syncs, barriers and checkpoints refuse: nothing is published.
        assert!(store.wal().sync().is_err());
        assert!(store.checkpoint().is_err());
        assert_eq!(store.committing_stats().published, 1);
        assert_eq!(store.get(b"before").unwrap().as_deref(), Some(&b"v"[..]));
    }
}

/// A commit a successful sync covered, but which was not yet published when
/// a later failure fenced the store, is never reported committed.
#[test]
fn a_covered_but_unpublished_commit_is_not_reported_committed_after_a_failure() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let covered = append(&store, b"covered", b"v");
    store.wal().sync().unwrap();
    assert!(store.wal().durable_lsn() >= covered.lsn);
    let failure = arm_io_failure("page_log.sync", dir.path(), 1, false);
    let later = append(&store, b"later", b"v");
    assert!(store.await_commit(later).is_err());
    assert_eq!(failure.fired(), 1);
    assert!(matches!(
        store.await_commit(covered),
        Err(PageError::WriteFailed { .. })
    ));
    assert_eq!(store.get(b"covered").unwrap(), None);
    assert_eq!(store.get(b"later").unwrap(), None);
}

/// The overlap safety net: a row commit touching a key a committing
/// transaction wrote waits for it instead of raising a write conflict, and
/// its pre-image read sees the committing value.
#[test]
fn an_overlapping_row_commit_waits_instead_of_conflicting() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let first = append(&store, b"k", b"v1");
    let second = {
        let _guard = store.row_commit_guard();
        let xid = store.begin_row_commit();
        let pre = Snapshot {
            xid: 0,
            xmax: xid,
            in_flight: Arc::new(Default::default()),
        };
        assert_eq!(
            store.get_as_of(&pre, b"k").unwrap().as_deref(),
            Some(&b"v1"[..]),
            "the pre-image read missed a decided commit"
        );
        store.put(xid, b"k", b"v2").expect("spurious write conflict");
        store.commit_append(xid).unwrap()
    };
    assert_eq!(store.transactions.status(first.xid), TxStatus::Committed);
    store.await_commit(first).unwrap();
    store.await_commit(second).unwrap();
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    assert!(store.committing_stats().overlap_waits >= 1);
    drop(store);
    let store = open(&dir, true);
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
}

/// Writeback of a page logged by a committing transaction syncs the log
/// first (I5), even with a tiny pool forcing eviction.
#[test]
fn eviction_of_a_committing_page_syncs_the_log_first() {
    let dir = TempDir::new().unwrap();
    let store = PagedStore::open(
        dir.path(),
        PagedStoreOptions::default()
            .with_page_size(512)
            .with_buffer_pool_bytes(32 * 512)
            .with_fsync(true)
            .with_commit_sync_outside_lock(true),
    )
    .unwrap()
    .0;
    let mut tickets = Vec::new();
    for i in 0..200u32 {
        tickets.push(append(&store, format!("key-{i:05}").as_bytes(), &[7u8; 64]));
    }
    // Eviction ran while commits were committing; each writeback synced the
    // log through them first, so some were published by a writeback's sync.
    assert!(store.pool.store().page_count() > 32);
    for ticket in tickets {
        store.await_commit(ticket).unwrap();
    }
    drop(store);
    let store = PagedStore::open(
        dir.path(),
        PagedStoreOptions::default()
            .with_page_size(512)
            .with_buffer_pool_bytes(32 * 512)
            .with_fsync(true),
    )
    .unwrap()
    .0;
    for i in 0..200u32 {
        assert!(store.get(format!("key-{i:05}").as_bytes()).unwrap().is_some());
    }
}

/// The same safety net on the write path alone (no pre-image read), for a
/// put and for a delete.
#[test]
fn an_overlapping_write_without_a_pre_image_read_waits() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let first = append(&store, b"p", b"v1");
    let gone = append(&store, b"d", b"v1");
    let second = {
        let _guard = store.row_commit_guard();
        let xid = store.begin_row_commit();
        store.put(xid, b"p", b"v2").expect("spurious write conflict on put");
        assert!(store.delete(xid, b"d").expect("spurious write conflict on delete"));
        store.commit_append(xid).unwrap()
    };
    for ticket in [first, gone, second] {
        store.await_commit(ticket).unwrap();
    }
    assert_eq!(store.get(b"p").unwrap().as_deref(), Some(&b"v2"[..]));
    assert_eq!(store.get(b"d").unwrap(), None);
    // One wait suffices: its sync covers, and publishes, both earlier commits.
    assert_eq!(store.committing_stats().overlap_waits, 1);
}

/// Publication stops at the durable LSN: a commit appended after the last
/// sync stays invisible even when an earlier, durable commit is published.
#[test]
fn publication_never_covers_a_record_past_the_durable_lsn() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir, true);
    let durable = append(&store, b"a", b"v");
    store.wal().sync().unwrap();
    let volatile = append(&store, b"b", b"v");
    assert!(store.wal().durable_lsn() < volatile.lsn);
    store.await_commit(durable).unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(store.get(b"b").unwrap(), None, "published before durable");
    assert_eq!(store.committing_stats().depth, 1);
    store.await_commit(volatile).unwrap();
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"v"[..]));
}

