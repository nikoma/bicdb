//! Qualification of [`Wal::with_unlocked_sync`] (see
//! `docs/wal-unlocked-sync.md`). Each test pins one property of the gated
//! sync step; where a property also holds for the default locked step, the
//! test runs both and compares.
//!
//! The pause points ([`SyncPhase`]) exist only in the unlocked step: in the
//! locked step there is no window to pause in, because the append lock is
//! held from the covered-offset read to the end of the sync.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::*;

const WAIT: Duration = Duration::from_secs(10);

fn open(dir: &TempDir, fsync: bool, segment_bytes: u64, unlocked: bool) -> Arc<Wal> {
    Arc::new(
        Wal::open_with_segments(dir.path().join("test.wal"), fsync, segment_bytes)
            .unwrap()
            .with_unlocked_sync(unlocked),
    )
}

fn image(wal: &Wal, page: PageId) -> Lsn {
    wal.log_page_image(page, 1, &[page as u8; 64]).unwrap()
}

/// Run `f` on another thread and fail the test if it does not finish within
/// [`WAIT`]: an append that blocks on a lock the sync still holds shows up as
/// a timeout here instead of a hung test.
fn within<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(f());
    });
    receiver
        .recv_timeout(WAIT)
        .expect("operation blocked: the append lock is still held across the sync")
}

/// Pause the next unlocked sync at `phase`. Returns (reached, release): the
/// syncing thread signals `reached` and waits for `release`.
fn pause_once(wal: &Wal, phase: SyncPhase) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let reached_tx = Mutex::new(reached_tx);
    let release_rx = Mutex::new(release_rx);
    let armed = AtomicBool::new(true);
    wal.test_hooks().on_phase(move |at| {
        if at == phase && armed.swap(false, Ordering::AcqRel) {
            reached_tx.lock().send(()).unwrap();
            release_rx.lock().recv_timeout(WAIT).unwrap();
        }
    });
    (reached_rx, release_tx)
}

fn count_phase(wal: &Wal, phase: SyncPhase) -> Arc<AtomicU64> {
    let count = Arc::new(AtomicU64::new(0));
    let seen = count.clone();
    wal.test_hooks().on_phase(move |at| {
        if at == phase {
            seen.fetch_add(1, Ordering::AcqRel);
        }
    });
    count
}

fn end_offset(wal: &Wal) -> u64 {
    wal.end_offset.load(Ordering::Acquire)
}

// ---------------------------------------------------------------------------
// Gate
// ---------------------------------------------------------------------------

#[test]
fn the_gate_is_off_by_default_and_off_means_the_locked_step() {
    assert!(!crate::paged::PagedStoreOptions::default().unlocked_wal_sync);
    for fsync in [false, true] {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(Wal::open(dir.path().join("test.wal"), fsync).unwrap());
        assert!(!wal.unlocked_sync());
        let unlocked = count_phase(&wal, SyncPhase::Unlocked);
        let synced = count_phase(&wal, SyncPhase::Synced);
        for transaction in 1..=20 {
            image(&wal, transaction as PageId);
            let lsn = wal.commit(transaction).unwrap();
            assert!(wal.durable_lsn() >= lsn);
        }
        wal.sync().unwrap();
        assert_eq!(unlocked.load(Ordering::Acquire), 0, "fsync={fsync}");
        assert_eq!(synced.load(Ordering::Acquire), 0, "fsync={fsync}");
        assert_eq!(wal.synced_offset_for_test(), end_offset(&wal));
    }
}

#[test]
fn the_paged_store_passes_the_gate_through_and_defaults_off() {
    let options = crate::paged::PagedStoreOptions::default()
        .with_page_size(512)
        .with_buffer_pool_bytes(64 * 512)
        .with_fsync(false);
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = crate::paged::PagedStore::open(dir.path(), options.clone()).unwrap();
        assert!(!store.wal().unlocked_sync());
    }
    let (store, _) =
        crate::paged::PagedStore::open(dir.path(), options.with_unlocked_wal_sync(true)).unwrap();
    assert!(store.wal().unlocked_sync());
}

// ---------------------------------------------------------------------------
// (a) Concurrent commit ordering
// ---------------------------------------------------------------------------

#[test]
fn an_unlocked_sync_lets_appends_through_and_claims_only_its_target() {
    for fsync in [false, true] {
        let dir = TempDir::new().unwrap();
        let wal = open(&dir, fsync, 0, true);
        let a = image(&wal, 1);
        let offset_a = end_offset(&wal);
        let (reached, release) = pause_once(&wal, SyncPhase::Unlocked);
        let syncing = {
            let wal = wal.clone();
            std::thread::spawn(move || wal.sync_through(a))
        };
        reached.recv_timeout(WAIT).unwrap();

        // The sync is in flight and the append lock is free.
        let b = {
            let wal = wal.clone();
            within(move || image(&wal, 2))
        };
        assert!(b > a);
        assert_eq!(
            wal.durable_lsn(),
            0,
            "nothing is durable before the sync returns"
        );

        release.send(()).unwrap();
        syncing.join().unwrap().unwrap();
        assert_eq!(
            wal.durable_lsn(),
            a,
            "fsync={fsync}: a record appended during the sync must not be reported durable by it"
        );
        assert_eq!(wal.synced_offset_for_test(), offset_a);

        wal.sync_through(b).unwrap();
        assert!(wal.durable_lsn() >= b);
        assert_eq!(wal.synced_offset_for_test(), end_offset(&wal));
    }
}

#[test]
fn the_unlocked_sync_publishes_nothing_until_the_sync_has_returned() {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 0, true);
    let a = image(&wal, 1);
    let (reached, release) = pause_once(&wal, SyncPhase::Synced);
    let syncing = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(a))
    };
    reached.recv_timeout(WAIT).unwrap();
    // The fdatasync finished; durability must still be unpublished, both the
    // LSN waiters read and the offset the power-loss emulation keeps.
    assert_eq!(wal.durable_lsn(), 0);
    assert_eq!(wal.synced_offset_for_test(), 0);
    release.send(()).unwrap();
    syncing.join().unwrap().unwrap();
    assert_eq!(wal.durable_lsn(), a);
}

#[test]
fn group_commit_waiters_return_only_once_their_own_lsn_is_durable() {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 0, true);
    let a = image(&wal, 1);
    let (reached, release) = pause_once(&wal, SyncPhase::Unlocked);
    let leader = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(a).map(|()| wal.durable_lsn()))
    };
    reached.recv_timeout(WAIT).unwrap();
    let syncs_before = wal.snapshot().syncs;

    let b = {
        let wal = wal.clone();
        within(move || image(&wal, 2))
    };
    let waiter_b = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(b).map(|()| wal.durable_lsn()))
    };
    let waiter_a = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(a).map(|()| wal.durable_lsn()))
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !waiter_a.is_finished(),
        "a waiter returned before its LSN was durable"
    );
    assert!(
        !waiter_b.is_finished(),
        "a waiter returned before its LSN was durable"
    );
    assert_eq!(wal.durable_lsn(), 0);

    release.send(()).unwrap();
    assert!(leader.join().unwrap().unwrap() >= a);
    assert!(waiter_a.join().unwrap().unwrap() >= a);
    assert!(waiter_b.join().unwrap().unwrap() >= b);
    let snapshot = wal.snapshot();
    // The leader's sync covered `a` only; `b` needed one more sync. The
    // waiter for `a` shared the leader's.
    assert_eq!(snapshot.syncs - syncs_before, 2, "{snapshot:?}");
    assert!(snapshot.group_commit_savings >= 2, "{snapshot:?}");
}

/// Many committers against a segmented log: every commit is durable when it
/// returns, durability never runs ahead of what was appended, and the log
/// reads back with LSN order equal to file order.
#[test]
fn concurrent_commits_keep_lsn_order_equal_to_file_order() {
    for unlocked in [false, true] {
        let dir = TempDir::new().unwrap();
        let wal = open(&dir, true, 8 * 1024, unlocked);
        let stop = Arc::new(AtomicBool::new(false));
        let monitor = {
            let wal = wal.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut last = 0;
                while !stop.load(Ordering::Acquire) {
                    let durable = wal.durable_lsn();
                    assert!(durable >= last, "durable_lsn went backwards");
                    assert!(
                        durable < wal.next_lsn(),
                        "durable_lsn ran past the appended log"
                    );
                    last = durable;
                }
            })
        };
        let next_tx = Arc::new(AtomicU64::new(1));
        let writers: Vec<_> = (0..8)
            .map(|_| {
                let wal = wal.clone();
                let next_tx = next_tx.clone();
                std::thread::spawn(move || {
                    let mut acked = Vec::new();
                    for _ in 0..100 {
                        let tx = next_tx.fetch_add(1, Ordering::AcqRel);
                        image(&wal, tx as PageId);
                        let lsn = wal.commit(tx).unwrap();
                        assert!(wal.durable_lsn() >= lsn, "commit returned before durable");
                        acked.push((tx, lsn));
                    }
                    acked
                })
            })
            .collect();
        let acked: Vec<(u64, Lsn)> = writers
            .into_iter()
            .flat_map(|writer| writer.join().unwrap())
            .collect();
        stop.store(true, Ordering::Release);
        monitor.join().unwrap();

        assert!(
            wal.snapshot().segments_sealed > 0,
            "the workload must seal segments"
        );
        let (records, torn) = wal.read_all().unwrap();
        assert_eq!(torn, 0);
        for pair in records.windows(2) {
            assert_eq!(
                pair[1].lsn,
                pair[0].lsn + 1,
                "unlocked={unlocked}: file order != LSN order"
            );
        }
        let commits: std::collections::HashMap<u64, Lsn> = records
            .iter()
            .filter(|record| record.kind == RecordKind::Commit)
            .map(|record| (record.transaction, record.lsn))
            .collect();
        for (tx, lsn) in acked {
            assert_eq!(
                commits.get(&tx),
                Some(&lsn),
                "unlocked={unlocked}: commit {tx} missing"
            );
        }
    }
}

/// Emulated power cuts in the middle of a concurrent, segment-sealing
/// workload: every commit acknowledged before the cut is in what a power cut
/// may keep (sealed segments plus the synced prefix of the active file).
#[test]
fn a_power_cut_never_loses_an_acknowledged_commit() {
    for unlocked in [false, true] {
        let dir = TempDir::new().unwrap();
        let segment_bytes = 4 * 1024;
        let wal = open(&dir, true, segment_bytes, unlocked);
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
        let next_tx = Arc::new(AtomicU64::new(1));
        let writers: Vec<_> = (0..6)
            .map(|_| {
                let (wal, stop, acked, next_tx) =
                    (wal.clone(), stop.clone(), acked.clone(), next_tx.clone());
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        let tx = next_tx.fetch_add(1, Ordering::AcqRel);
                        image(&wal, tx as PageId);
                        wal.commit(tx).unwrap();
                        acked.lock().push(tx);
                    }
                })
            })
            .collect();

        for cut in 0..6 {
            std::thread::sleep(Duration::from_millis(30));
            let cut_dir = TempDir::new().unwrap();
            let cut_path = cut_dir.path().join("test.wal");
            let acknowledged = {
                // The emulation's own discipline: hold the append lock, so no
                // sync is half-published and no append lands after the cut.
                let _file = wal.file.lock();
                let acknowledged = acked.lock().clone();
                let synced = wal.synced_offset.load(Ordering::Acquire);
                for sequence in wal.sealed.lock().iter() {
                    std::fs::copy(
                        wal.sealed_path(*sequence),
                        cut_dir.path().join(format!("test.wal.{sequence}")),
                    )
                    .unwrap();
                }
                std::fs::copy(wal.path(), &cut_path).unwrap();
                OpenOptions::new()
                    .write(true)
                    .open(&cut_path)
                    .unwrap()
                    .set_len(synced)
                    .unwrap();
                acknowledged
            };
            let survivor = Wal::open_with_segments(&cut_path, false, segment_bytes).unwrap();
            let (records, _) = survivor.read_all().unwrap();
            let committed: std::collections::HashSet<u64> = records
                .iter()
                .filter(|record| record.kind == RecordKind::Commit)
                .map(|record| record.transaction)
                .collect();
            for tx in &acknowledged {
                assert!(
                    committed.contains(tx),
                    "unlocked={unlocked} cut {cut}: acknowledged commit {tx} lost ({} acked)",
                    acknowledged.len()
                );
            }
            assert!(!acknowledged.is_empty() || cut == 0);
        }
        stop.store(true, Ordering::Release);
        for writer in writers {
            writer.join().unwrap();
        }
        assert!(wal.snapshot().segments_sealed > 0);
    }
}

// ---------------------------------------------------------------------------
// (c) Checkpoint interaction: seal, reset, archive retention
// ---------------------------------------------------------------------------

#[test]
fn a_seal_inside_the_window_publishes_only_records_the_old_file_holds() {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 1024, true);
    let a = image(&wal, 1);
    let first_sequence = wal.active_first_lsn.load(Ordering::Acquire);
    let (reached, release) = pause_once(&wal, SyncPhase::Unlocked);
    let syncing = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(a))
    };
    reached.recv_timeout(WAIT).unwrap();

    // Fill the active file until it seals: the duplicate the paused sync
    // holds now names the renamed, sealed segment.
    let mut last = a;
    while wal.snapshot().segments_sealed == 0 {
        let wal2 = wal.clone();
        last = within(move || image(&wal2, 3));
    }
    let post_seal = image(&wal, 4);
    assert!(post_seal > last);

    release.send(()).unwrap();
    syncing.join().unwrap().unwrap();
    assert_eq!(
        wal.durable_lsn(),
        a,
        "records after the captured target are not durable"
    );
    assert_eq!(
        wal.synced_offset_for_test(),
        0,
        "the old file's covered offset must not be applied to the new active file"
    );

    // What a power cut keeps right now holds `a` (in the sealed segment).
    let sealed = wal.sealed_path(first_sequence);
    assert!(sealed.exists());
    let cut_dir = TempDir::new().unwrap();
    std::fs::copy(
        &sealed,
        cut_dir.path().join(format!("test.wal.{first_sequence}")),
    )
    .unwrap();
    std::fs::File::create(cut_dir.path().join("test.wal")).unwrap();
    let survivor = Wal::open_with_segments(cut_dir.path().join("test.wal"), false, 1024).unwrap();
    let (records, _) = survivor.read_all().unwrap();
    assert!(records.iter().any(|record| record.lsn == a));

    wal.sync_through(post_seal).unwrap();
    assert!(wal.durable_lsn() >= post_seal);
    assert_eq!(wal.synced_offset_for_test(), end_offset(&wal));
}

#[test]
fn a_reset_inside_the_window_never_makes_later_records_durable() {
    for phase in [SyncPhase::Unlocked, SyncPhase::Synced] {
        let dir = TempDir::new().unwrap();
        let wal = open(&dir, true, 0, true);
        let a = image(&wal, 1);
        let (reached, release) = pause_once(&wal, phase);
        let syncing = {
            let wal = wal.clone();
            std::thread::spawn(move || wal.sync_through(a))
        };
        reached.recv_timeout(WAIT).unwrap();

        // A checkpoint's truncation, then new work in the fresh generation.
        let wal2 = wal.clone();
        within(move || wal2.reset().unwrap());
        assert_eq!(wal.durable_lsn(), a, "reset keeps its own guarantee");
        let b = image(&wal, 2);

        release.send(()).unwrap();
        syncing.join().unwrap().unwrap();
        assert_eq!(
            wal.durable_lsn(),
            a,
            "{phase:?}: a post-reset record was reported durable"
        );
        assert_eq!(
            wal.synced_offset_for_test(),
            0,
            "{phase:?}: the pre-reset covered offset must not describe the emptied file"
        );
        assert_eq!(
            std::fs::metadata(wal.path()).unwrap().len(),
            end_offset(&wal)
        );

        wal.sync_through(b).unwrap();
        assert!(wal.durable_lsn() >= b);
        assert_eq!(wal.synced_offset_for_test(), end_offset(&wal));
        let (records, _) = wal.read_all().unwrap();
        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![b]
        );
    }
}

#[test]
fn archive_retention_reset_inside_the_window_keeps_the_segment_and_its_records() {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 64 * 1024, true);
    wal.enable_archive_retention();
    let a = image(&wal, 1);
    let sequence = wal.active_first_lsn.load(Ordering::Acquire);
    let (reached, release) = pause_once(&wal, SyncPhase::Unlocked);
    let syncing = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(a))
    };
    reached.recv_timeout(WAIT).unwrap();
    // Retention seals the active file (the one the duplicate names) and keeps
    // it because the archiver has not confirmed it.
    let wal2 = wal.clone();
    assert!(!within(move || wal2.reset().unwrap()));
    assert_eq!(wal.sealed_segment_paths(), vec![wal.sealed_path(sequence)]);
    release.send(()).unwrap();
    syncing.join().unwrap().unwrap();
    assert_eq!(wal.durable_lsn(), a);
    let (records, _) = wal.read_all().unwrap();
    assert!(records.iter().any(|record| record.lsn == a));
}

#[test]
fn an_archived_segment_deleted_inside_the_window_does_not_fail_the_sync() {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 64 * 1024, true);
    wal.enable_archive_retention();
    wal.mark_archived_through(u64::MAX);
    let a = image(&wal, 1);
    let (reached, release) = pause_once(&wal, SyncPhase::Unlocked);
    let syncing = {
        let wal = wal.clone();
        std::thread::spawn(move || wal.sync_through(a))
    };
    reached.recv_timeout(WAIT).unwrap();
    // Seal-then-delete: the duplicate now names an unlinked inode.
    let wal2 = wal.clone();
    assert!(within(move || wal2.reset().unwrap()));
    assert!(wal.sealed_segment_paths().is_empty());
    release.send(()).unwrap();
    syncing.join().unwrap().unwrap();
    assert_eq!(wal.durable_lsn(), a);
    assert_eq!(wal.synced_offset_for_test(), 0);
}

// ---------------------------------------------------------------------------
// (d) fsync failures
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
struct FailureOutcome {
    durable_after_failed_commit: Lsn,
    failures_after_failed_commit: u64,
    abort_appended_after_failed_commit: bool,
    retry_ok: bool,
    durable_covers_retry: bool,
    persistent_results: Vec<bool>,
    durable_unchanged_under_persistent_failure: bool,
    recovered_after_clear: bool,
}

fn failure_outcome(unlocked: bool) -> FailureOutcome {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 0, unlocked);
    image(&wal, 1);
    wal.test_hooks().fail_next_syncs(1);
    assert!(wal.commit(1).is_err());
    let commit_lsn = wal.next_lsn() - 1;
    let durable_after_failed_commit = wal.durable_lsn();
    let failures_after_failed_commit = wal.snapshot().sync_failures;
    // Commit wins: the record may be on disk although its sync failed.
    let before_abort = wal.next_lsn();
    wal.abort(1).unwrap();
    let abort_appended_after_failed_commit = wal.next_lsn() != before_abort;
    let retry_ok = wal.sync_through(commit_lsn).is_ok();
    let durable_covers_retry = wal.durable_lsn() >= commit_lsn;

    // A persistent failure with concurrent committers: each one gets its
    // own error (a waiter that wakes after a failed sync retries the sync
    // itself), and durability never advances.
    let durable_before = wal.durable_lsn();
    wal.test_hooks().fail_next_syncs(u64::MAX);
    let committers: Vec<_> = (0..4u64)
        .map(|i| {
            let wal = wal.clone();
            std::thread::spawn(move || {
                image(&wal, 10 + i as PageId);
                wal.commit(100 + i).is_ok()
            })
        })
        .collect();
    let persistent_results: Vec<bool> = committers
        .into_iter()
        .map(|committer| committer.join().unwrap())
        .collect();
    let durable_unchanged_under_persistent_failure = wal.durable_lsn() == durable_before;
    wal.test_hooks().clear();
    let recovered_after_clear = wal.sync().is_ok() && wal.durable_lsn() == wal.next_lsn() - 1;
    FailureOutcome {
        durable_after_failed_commit,
        failures_after_failed_commit,
        abort_appended_after_failed_commit,
        retry_ok,
        durable_covers_retry,
        persistent_results,
        durable_unchanged_under_persistent_failure,
        recovered_after_clear,
    }
}

#[test]
fn sync_failures_behave_exactly_as_on_the_locked_path() {
    let locked = failure_outcome(false);
    let unlocked = failure_outcome(true);
    assert_eq!(
        locked,
        FailureOutcome {
            durable_after_failed_commit: 0,
            failures_after_failed_commit: 1,
            abort_appended_after_failed_commit: false,
            retry_ok: true,
            durable_covers_retry: true,
            persistent_results: vec![false; 4],
            durable_unchanged_under_persistent_failure: true,
            recovered_after_clear: true,
        }
    );
    assert_eq!(unlocked, locked);
}

/// One failure, two callers: exactly one error, whoever ran the failed sync;
/// the other caller's own sync succeeds. Same on both paths.
#[test]
fn one_failed_sync_fails_exactly_one_caller_on_both_paths() {
    for unlocked in [false, true] {
        for _ in 0..20 {
            let dir = TempDir::new().unwrap();
            let wal = open(&dir, true, 0, unlocked);
            let a = image(&wal, 1);
            wal.test_hooks().fail_next_syncs(1);
            let callers: Vec<_> = (0..2)
                .map(|_| {
                    let wal = wal.clone();
                    std::thread::spawn(move || wal.sync_through(a).is_ok())
                })
                .collect();
            let ok = callers
                .into_iter()
                .map(|caller| caller.join().unwrap())
                .filter(|ok| *ok)
                .count();
            assert_eq!(ok, 1, "unlocked={unlocked}");
            assert!(wal.durable_lsn() >= a);
        }
    }
}

#[test]
fn a_failure_inside_the_window_wakes_waiters_without_publishing() {
    for failures in [1u64, 2] {
        let dir = TempDir::new().unwrap();
        let wal = open(&dir, true, 0, true);
        let a = image(&wal, 1);
        wal.test_hooks().fail_next_syncs(failures);
        let (reached, release) = pause_once(&wal, SyncPhase::Synced);
        let leader = {
            let wal = wal.clone();
            std::thread::spawn(move || wal.sync_through(a))
        };
        reached.recv_timeout(WAIT).unwrap();
        let waiter = {
            let wal = wal.clone();
            std::thread::spawn(move || wal.sync_through(a))
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished());
        release.send(()).unwrap();
        assert!(leader.join().unwrap().is_err());
        let waited = waiter.join().unwrap();
        if failures == 1 {
            // The waiter retried and its own sync succeeded.
            waited.unwrap();
            assert!(wal.durable_lsn() >= a);
        } else {
            assert!(waited.is_err());
            assert_eq!(
                wal.durable_lsn(),
                0,
                "a failed sync must never publish durability"
            );
            assert_eq!(wal.synced_offset_for_test(), 0);
        }
        assert_eq!(wal.snapshot().sync_failures, failures);
    }
}

#[test]
fn a_failed_descriptor_duplication_falls_back_to_the_locked_sync() {
    let dir = TempDir::new().unwrap();
    let wal = open(&dir, true, 0, true);
    let unlocked = count_phase(&wal, SyncPhase::Unlocked);
    wal.test_hooks().fail_next_clones(1);
    image(&wal, 1);
    let lsn = wal.commit(1).unwrap();
    assert!(wal.durable_lsn() >= lsn);
    assert_eq!(
        unlocked.load(Ordering::Acquire),
        0,
        "the fallback runs under the lock"
    );
    assert_eq!(wal.synced_offset_for_test(), end_offset(&wal));
    assert_eq!(wal.snapshot().sync_failures, 0);
    // The next sync is unlocked again.
    image(&wal, 2);
    wal.commit(2).unwrap();
    assert_eq!(unlocked.load(Ordering::Acquire), 1);
}
