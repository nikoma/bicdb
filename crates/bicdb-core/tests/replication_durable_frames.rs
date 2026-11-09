//! Replication ships only durable frames.
//!
//! A commit's bytes reach `transactions.log` before its O_DSYNC write (or
//! write plus sync) returns. Until it returns, the commit can still be lost:
//! the sync fails and the tail repair cuts the bytes back, or the host loses
//! power. Every path that ships or exports log content must therefore stop at
//! the durable sequence the writer publishes (`written_seq`), never at the end
//! of the file. Otherwise a standby can hold a commit the primary's recovery
//! drops: the two diverge, and a promoted standby exposes a commit whose
//! client was told it failed, or was never told it succeeded.
//!
//! Each case pauses a commit between its write and the return of its sync
//! (an I/O hook on `transaction_log.sync`, after the bytes are in the file),
//! exports and ships from every live path inside that window, then lets the
//! sync succeed or fail. The primary is then dropped (its handle is the
//! crash; recovery runs on the reopen), and the case reconciles the primary,
//! the standby and the promoted standby against the ledger of acknowledged
//! commits. See `docs/durability-failure-handling.md`, section 12.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use bicdb_core::{
    BicDb, BicDbError, CommitFrame, DbConfig, Record, ReplicationConfig, ReplicationCursor,
    ReplicationMode, ReplicationTlsConfig, StandbyResume, StorageMode,
};
use bicdb_page::test_faults::{arm_io_failure, arm_io_hook, IoFaultGuard};
use serde_json::json;

fn config(storage: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(storage)
}

fn storage_modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

fn replication(mode: ReplicationMode) -> ReplicationConfig {
    ReplicationConfig {
        enabled: true,
        mode,
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: Default::default(),
            key_path: Default::default(),
            ca_path: Default::default(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    }
}

fn open_replica(path: &std::path::Path, storage: StorageMode, mode: ReplicationMode) -> BicDb {
    BicDb::open_with_config(path, config(storage).with_replication(replication(mode))).unwrap()
}

fn put(db: &BicDb, id: &str) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    tx.insert("items", Record::new(id).with_metadata(json!({ "id": id })))?;
    tx.commit()
}

/// Every row of `items`, by id.
fn rows(db: &BicDb) -> BTreeMap<String, String> {
    db.scan_collection("items")
        .unwrap()
        .into_iter()
        .map(|record| (record.id.clone(), record.metadata.to_string()))
        .collect()
}

/// The two shipping APIs a primary offers: the live sender's cursor (also
/// the commit-log archive's) and the export used by the replication CLI,
/// consensus proposals, Cell replication and range catch-up.
#[derive(Clone, Copy, Debug)]
enum Shipper {
    Live,
    Export,
}

fn ship(
    shipper: Shipper,
    primary: &BicDb,
    standby: &mut BicDb,
    cursor: &mut ReplicationCursor,
) -> bicdb_core::Result<()> {
    loop {
        let frames = match shipper {
            Shipper::Live => primary.replication_frames_after(cursor, 1_000)?,
            Shipper::Export => {
                primary.export_replication_frames_since(standby.last_applied_commit_seq(), 1_000)?
            }
        };
        if frames.is_empty() {
            return Ok(());
        }
        standby.apply_replication_batch(&frames)?;
    }
}

/// What happens to the paused commit once its sync returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fate {
    /// The sync succeeds: the commit is durable and acknowledged.
    Durable,
    /// The sync fails and the tail repair cuts the bytes back: the frame is
    /// lost, exactly as a power cut before the sync would lose it.
    Lost,
    /// The sync fails and the repair fails too: the bytes stay on the
    /// primary's disk, the outcome is uncertain, and recovery keeps it.
    Uncertain,
    /// The write itself fails before any byte reaches the log. There is no
    /// window to pause in; the case only exercises failover.
    NotWritten,
}

const FATES: [Fate; 4] = [Fate::Durable, Fate::Lost, Fate::Uncertain, Fate::NotWritten];

/// Every frame each live path would ship right now, starting after `after`.
fn everything_shippable(primary: &BicDb, after: u64) -> Vec<CommitFrame> {
    let mut cursor = ReplicationCursor::new(after);
    let mut frames = primary
        .replication_frames_after(&mut cursor, 1_000)
        .unwrap();
    frames.extend(
        primary
            .export_replication_frames_since(after, 1_000)
            .unwrap(),
    );
    frames.extend(primary.subscribe_replication_stream(after).unwrap());
    frames
}

fn mentions(frames: &[CommitFrame], id: &str) -> bool {
    frames
        .iter()
        .any(|frame| frame.writes.iter().any(|write| write.record_id == id))
}

struct Window {
    /// The commit's bytes were in the file while it was paused.
    log_grew: bool,
    /// Durable sequence the writer published while it was paused.
    durable_during: u64,
    /// Everything the live paths offered while it was paused.
    shipped_during: Vec<CommitFrame>,
}

/// Commit `id` on `primary` with the given fate, pausing it between its
/// write and its sync's return to collect what every path would ship.
fn commit_with_fate(
    primary: &Arc<BicDb>,
    dir: &std::path::Path,
    fate: Fate,
    id: &str,
) -> (bicdb_core::Result<()>, Option<Window>) {
    if fate == Fate::NotWritten {
        let _write = arm_io_failure("transaction_log.write", dir, 1, false);
        return (put(primary, id), None);
    }
    let log = dir.join("transactions.log");
    let log_before = std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0);
    let paused = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    // The hook runs after the bytes were written and before the sync's
    // outcome is returned. The failure plans are armed after it, so on the
    // same call the hook runs first and the failure is returned after it.
    let hook = {
        let paused = paused.clone();
        let resume = resume.clone();
        arm_io_hook("transaction_log.sync", dir, 1, move || {
            paused.wait();
            resume.wait();
        })
    };
    let mut faults: Vec<IoFaultGuard> = Vec::new();
    if matches!(fate, Fate::Lost | Fate::Uncertain) {
        faults.push(arm_io_failure("transaction_log.sync", dir, 1, false));
    }
    if fate == Fate::Uncertain {
        faults.push(arm_io_failure("transaction_log.repair", dir, 1, false));
    }
    let committer = {
        let primary = primary.clone();
        let id = id.to_string();
        std::thread::spawn(move || put(&primary, &id))
    };
    paused.wait();
    // Inside the window. Nothing here may perform injected I/O: the hook
    // holds the fault registry, so this only reads.
    let log_grew = std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0) > log_before;
    let durable_during = primary.replication_feed().durable_seq();
    let shipped_during = everything_shippable(primary, 0);
    resume.wait();
    let result = committer.join().unwrap();
    assert_eq!(
        hook.fired(),
        1,
        "{fate:?}: the commit never reached its sync"
    );
    for fault in &faults {
        assert_eq!(fault.fired(), 1, "{fate:?}");
    }
    (
        result,
        Some(Window {
            log_grew,
            durable_during,
            shipped_during,
        }),
    )
}

#[derive(Clone, Copy, Debug)]
enum AfterCrash {
    /// Reopen the primary (recovery), catch the standby up from it.
    Recover,
    /// Promote the standby; the old primary reopens as a standby.
    Failover,
}

fn run_case(storage: StorageMode, fate: Fate, shipper: Shipper, after: AfterCrash) {
    let label = format!("{storage:?} {fate:?} {shipper:?} {after:?}");
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();
    let mut standby = open_replica(
        standby_dir.path(),
        storage.clone(),
        ReplicationMode::Standby,
    );
    let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
    // The ledger: every commit a client was told succeeded.
    let mut acknowledged: Vec<String> = Vec::new();
    {
        let mut primary = open_replica(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        primary.create_collection("items").unwrap();
        for index in 0..3 {
            let id = format!("a{index}");
            put(&primary, &id).unwrap();
            acknowledged.push(id);
        }
        ship(shipper, &primary, &mut standby, &mut cursor).unwrap();
        let durable_before = primary.replication_feed().durable_seq();
        assert_eq!(standby.last_applied_commit_seq(), durable_before, "{label}");

        let primary = Arc::new(primary);
        let (result, window) = commit_with_fate(&primary, primary_dir.path(), fate, "inflight");
        if let Some(window) = window {
            assert!(
                window.log_grew,
                "{label}: the paused commit's bytes were not in the log, so the window is not real"
            );
            assert_eq!(
                window.durable_during, durable_before,
                "{label}: the durable sequence advanced before the sync returned"
            );
            assert!(
                window.shipped_during.iter().all(|frame| frame.commit_seq <= durable_before),
                "{label}: a frame past the durable prefix {durable_before} was offered (live, export, subscribe): {:?}",
                window
                    .shipped_during
                    .iter()
                    .map(|frame| frame.commit_seq)
                    .collect::<Vec<_>>()
            );
            assert!(
                !mentions(&window.shipped_during, "inflight"),
                "{label}: a commit whose sync had not returned was offered for shipping"
            );
            // A sender that captured them would apply them now.
            let mut captured: BTreeMap<u64, CommitFrame> = BTreeMap::new();
            for frame in window.shipped_during {
                if frame.commit_seq > standby.last_applied_commit_seq() {
                    captured.entry(frame.commit_seq).or_insert(frame);
                }
            }
            let captured: Vec<CommitFrame> = captured.into_values().collect();
            standby.apply_replication_batch(&captured).unwrap();
        }
        match fate {
            Fate::Durable => {
                result.unwrap();
                acknowledged.push("inflight".into());
            }
            Fate::Lost | Fate::Uncertain | Fate::NotWritten => {
                let error = result.unwrap_err();
                assert!(error.is_durability_failure(), "{label}: {error}");
                assert!(
                    matches!(
                        error,
                        BicDbError::CommitOutcomeUnknown { .. } | BicDbError::WriteFailed { .. }
                    ),
                    "{label}: {error}"
                );
            }
        }
        // After the outcome is known, every path still stops at the
        // durable prefix (on a fenced primary: the prefix before the
        // failure).
        let shippable = everything_shippable(&primary, 0);
        assert_eq!(
            mentions(&shippable, "inflight"),
            fate == Fate::Durable,
            "{label}: after the sync returned"
        );
        // Drain whatever is shippable before the crash.
        ship(shipper, &primary, &mut standby, &mut cursor).unwrap();
        // The crash: the handle goes away without a clean close.
        drop(primary);
    }
    let standby_rows = rows(&standby);
    for id in &acknowledged {
        assert!(
            standby_rows.contains_key(id),
            "{label}: acknowledged {id} was not shipped"
        );
    }

    match after {
        AfterCrash::Recover => {
            drop(standby);
            let primary = open_replica(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            assert!(primary.durability_state().is_healthy(), "{label}");
            let primary_rows = rows(&primary);
            for id in &acknowledged {
                assert!(
                    primary_rows.contains_key(id),
                    "{label}: acknowledged {id} lost by the primary's recovery"
                );
            }
            assert_eq!(
                primary_rows.contains_key("inflight"),
                matches!(fate, Fate::Durable | Fate::Uncertain),
                "{label}: recovery kept what reached the disk"
            );
            let mut standby = open_replica(
                standby_dir.path(),
                storage.clone(),
                ReplicationMode::Standby,
            );
            // No standby holds a commit the primary's recovery dropped.
            let standby_rows = rows(&standby);
            for (id, value) in &standby_rows {
                assert_eq!(
                    primary_rows.get(id),
                    Some(value),
                    "{label}: the standby holds {id}, which the primary's recovery does not"
                );
            }
            assert!(
                standby.last_applied_commit_seq() <= primary.current_commit_seq(),
                "{label}: the standby is ahead of the recovered primary"
            );
            let resume = primary
                .standby_resume(0, standby.last_applied_commit_seq())
                .unwrap();
            match resume {
                StandbyResume::Stream => {
                    // Catch up from the recovered primary: identical histories.
                    let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
                    ship(shipper, &primary, &mut standby, &mut cursor).unwrap();
                    assert_eq!(rows(&standby), primary_rows, "{label}: after catch-up");
                }
                StandbyResume::Snapshot(reason) => {
                    // The reopen's verification checkpoint may cut the log past
                    // the uncertain commit it kept; the standby (which holds no
                    // retention slot here) is then bootstrapped, never streamed
                    // across the gap.
                    assert_eq!(fate, Fate::Uncertain, "{label}: {reason}");
                    assert!(
                        reason.contains("log only reaches back"),
                        "{label}: {reason}"
                    );
                    let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
                    assert!(
                        ship(shipper, &primary, &mut standby, &mut cursor).is_err()
                            || !rows(&standby).contains_key("inflight"),
                        "{label}: streamed across a cut log"
                    );
                }
            }
        }
        AfterCrash::Failover => {
            let promoted_at = standby.last_applied_commit_seq();
            standby.promote_by_election(1, "standby", None).unwrap();
            drop(standby);
            let promoted = open_replica(
                standby_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            let promoted_rows = rows(&promoted);
            for id in &acknowledged {
                assert!(
                    promoted_rows.contains_key(id),
                    "{label}: acknowledged {id} missing after failover"
                );
            }
            assert!(
                !promoted_rows.contains_key("inflight") || fate == Fate::Durable,
                "{label}: the promoted standby exposes a commit that was never acknowledged"
            );
            put(&promoted, "after-failover").unwrap();
            // The old primary's recovered history, against the new one.
            let old = open_replica(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Standby,
            );
            let old_rows = rows(&old);
            for (id, value) in &promoted_rows {
                assert_eq!(
                    old_rows.get(id),
                    Some(value),
                    "{label}: the promoted standby holds {id}, which the old primary's recovery does not"
                );
            }
            let resume = promoted
                .standby_resume(0, old.last_applied_commit_seq())
                .unwrap();
            if fate == Fate::Uncertain {
                // The uncertain commit reached the old primary's disk past
                // the promotion point: a divergent tail, never streamed.
                assert!(old_rows.contains_key("inflight"), "{label}");
                assert!(old.last_applied_commit_seq() > promoted_at, "{label}");
                assert!(
                    matches!(resume, StandbyResume::Snapshot(_)),
                    "{label}: the old primary must be rebuilt"
                );
            } else {
                assert!(old.last_applied_commit_seq() <= promoted_at, "{label}");
                assert!(
                    matches!(resume, StandbyResume::Stream),
                    "{label}: {resume:?}"
                );
            }
        }
    }
}

#[test]
fn no_path_ships_a_frame_whose_sync_has_not_returned_and_recovery_agrees() {
    for storage in storage_modes() {
        for fate in FATES {
            for shipper in [Shipper::Live, Shipper::Export] {
                run_case(storage.clone(), fate, shipper, AfterCrash::Recover);
            }
        }
    }
}

#[test]
fn failover_after_a_crash_at_each_point_keeps_every_acknowledged_commit() {
    for storage in storage_modes() {
        for fate in FATES {
            for shipper in [Shipper::Live, Shipper::Export] {
                run_case(storage.clone(), fate, shipper, AfterCrash::Failover);
            }
        }
    }
}

/// Recovery accepts every complete frame in the log, including a batch whose
/// write had not returned when the previous process stopped, and that batch
/// may be only in the page cache. The open syncs the log before the
/// recovered sequence becomes shippable, and a failed sync fails the open.
#[test]
fn the_open_syncs_the_transaction_log_before_it_becomes_shippable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(dir.path(), config(StorageMode::EmbeddedMemory)).unwrap();
        db.create_collection("items").unwrap();
        put(&db, "a0").unwrap();
    }
    {
        let sync = arm_io_failure("transaction_log.open_sync", dir.path(), 1, false);
        let error = BicDb::open_with_config(dir.path(), config(StorageMode::EmbeddedMemory))
            .err()
            .expect("an open whose log sync failed must fail");
        assert_eq!(sync.fired(), 1, "{error}");
    }
    let sync = arm_io_failure("transaction_log.open_sync", dir.path(), 2, false);
    let db = BicDb::open_with_config(dir.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    assert_eq!(sync.seen(), 1, "the open syncs the log exactly once");
    assert!(db.get("items", "a0").unwrap().is_some());
}

/// (b) The standby side. A batch with a frame above the primary's durable
/// horizon is refused before any of it is applied: nothing is exposed to
/// readers, nothing advances the acknowledged position, nothing ships
/// onward from the standby.
#[test]
fn a_standby_applies_nothing_above_the_primarys_durable_horizon() {
    for storage in storage_modes() {
        let label = format!("{storage:?}");
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut primary = open_replica(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        let mut standby = open_replica(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        primary.create_collection("items").unwrap();
        put(&primary, "a0").unwrap();
        let mut cursor = ReplicationCursor::new(0);
        let first = primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap();
        let horizon = primary.replication_feed().durable_seq();
        standby
            .apply_replication_batch_within(&first, horizon)
            .unwrap();
        put(&primary, "a1").unwrap();
        put(&primary, "a2").unwrap();
        let next = primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap();
        assert_eq!(next.len(), 2, "{label}");
        let applied = standby.last_applied_commit_seq();
        // A horizon that covers only the first frame of the batch.
        let error = standby
            .apply_replication_batch_within(&next, next[0].commit_seq)
            .unwrap_err();
        assert!(
            error.to_string().contains("durable commit sequence"),
            "{label}: {error}"
        );
        assert_eq!(
            standby.last_applied_commit_seq(),
            applied,
            "{label}: acknowledged position moved"
        );
        assert!(
            standby.get("items", "a1").unwrap().is_none(),
            "{label}: exposed to readers"
        );
        let mut onward = ReplicationCursor::new(applied);
        assert!(
            standby
                .replication_frames_after(&mut onward, 1_000)
                .unwrap()
                .is_empty(),
            "{label}: shipped onward"
        );
        // The real horizon: applied, and identical to the primary.
        let horizon = primary.replication_feed().durable_seq();
        standby
            .apply_replication_batch_within(&next, horizon)
            .unwrap();
        assert_eq!(rows(&standby), rows(&primary), "{label}");
    }
    // Wire compatibility: a heartbeat from a sender that predates the field
    // decodes with no horizon; the current sender always sets it.
    let legacy = r#"{"frame":"heartbeat","cluster_id":"c","node_id":"n","last_commit_seq":3}"#;
    match serde_json::from_str::<bicdb_core::replication::ReplicationFrame>(legacy).unwrap() {
        bicdb_core::replication::ReplicationFrame::Heartbeat {
            durable_commit_seq, ..
        } => assert_eq!(durable_commit_seq, None),
        other => panic!("{other:?}"),
    }
}

/// How the standby's own log write fails while it imports a batch.
#[derive(Clone, Copy, Debug)]
enum StandbyCrash {
    /// Nothing reached the standby's log.
    BeforeWrite,
    /// The write's sync failed and the repair cut the bytes back.
    SyncLost,
    /// The sync and the repair failed: the bytes stay on the standby's disk.
    SyncKept,
}

/// (c) A standby crash mid-apply. Whatever the standby's own log kept, the
/// standby never holds a commit the primary lacks, never acknowledged more
/// than its disk holds, catches up after its reopen, and a standby promoted
/// right after the crash holds every commit it had acknowledged.
#[test]
fn a_standby_crash_mid_apply_never_diverges_from_the_primary() {
    for storage in storage_modes() {
        for crash in [
            StandbyCrash::BeforeWrite,
            StandbyCrash::SyncLost,
            StandbyCrash::SyncKept,
        ] {
            for promote in [false, true] {
                let label = format!("{storage:?} {crash:?} promote={promote}");
                let primary_dir = tempfile::tempdir().unwrap();
                let standby_dir = tempfile::tempdir().unwrap();
                let mut primary = open_replica(
                    primary_dir.path(),
                    storage.clone(),
                    ReplicationMode::Primary,
                );
                primary.create_collection("items").unwrap();
                let mut acknowledged = Vec::new();
                for index in 0..3 {
                    let id = format!("a{index}");
                    put(&primary, &id).unwrap();
                    acknowledged.push(id);
                }
                let standby_acked;
                {
                    let mut standby = open_replica(
                        standby_dir.path(),
                        storage.clone(),
                        ReplicationMode::Standby,
                    );
                    let mut cursor = ReplicationCursor::new(0);
                    let frames = primary.replication_frames_after(&mut cursor, 2).unwrap();
                    standby
                        .apply_replication_batch_within(
                            &frames,
                            primary.replication_feed().durable_seq(),
                        )
                        .unwrap();
                    put(&primary, "b0").unwrap();
                    acknowledged.push("b0".into());
                    let rest = primary
                        .replication_frames_after(&mut cursor, 1_000)
                        .unwrap();
                    let mut faults = Vec::new();
                    match crash {
                        StandbyCrash::BeforeWrite => faults.push(arm_io_failure(
                            "transaction_log.write",
                            standby_dir.path(),
                            1,
                            false,
                        )),
                        StandbyCrash::SyncLost => faults.push(arm_io_failure(
                            "transaction_log.sync",
                            standby_dir.path(),
                            1,
                            false,
                        )),
                        StandbyCrash::SyncKept => {
                            faults.push(arm_io_failure(
                                "transaction_log.sync",
                                standby_dir.path(),
                                1,
                                false,
                            ));
                            faults.push(arm_io_failure(
                                "transaction_log.repair",
                                standby_dir.path(),
                                1,
                                false,
                            ));
                        }
                    }
                    let before = standby.last_applied_commit_seq();
                    let error = standby
                        .apply_replication_batch_within(
                            &rest,
                            primary.replication_feed().durable_seq(),
                        )
                        .unwrap_err();
                    assert!(error.is_durability_failure(), "{label}: {error}");
                    for fault in &faults {
                        assert_eq!(fault.fired(), 1, "{label}");
                    }
                    // What the standby would acknowledge: never past the
                    // prefix its own log made durable before the failure.
                    standby_acked = standby.last_applied_commit_seq();
                    assert_eq!(
                        standby_acked, before,
                        "{label}: acknowledged a failed import"
                    );
                    // The crash.
                }
                let mut standby = open_replica(
                    standby_dir.path(),
                    storage.clone(),
                    ReplicationMode::Standby,
                );
                assert!(
                    standby.last_applied_commit_seq() >= standby_acked,
                    "{label}: the reopen lost an acknowledged import"
                );
                assert!(
                    standby.last_applied_commit_seq() <= primary.replication_feed().durable_seq(),
                    "{label}: the standby is ahead of the primary's durable sequence"
                );
                let primary_rows = rows(&primary);
                for (id, value) in rows(&standby) {
                    assert_eq!(primary_rows.get(&id), Some(&value), "{label}: {id}");
                }
                if promote {
                    let at = standby.last_applied_commit_seq();
                    standby.promote_by_election(1, "standby", None).unwrap();
                    drop(standby);
                    let promoted = open_replica(
                        standby_dir.path(),
                        storage.clone(),
                        ReplicationMode::Primary,
                    );
                    let promoted_rows = rows(&promoted);
                    for (id, value) in &promoted_rows {
                        assert_eq!(primary_rows.get(id), Some(value), "{label}: {id}");
                    }
                    assert!(promoted.current_commit_seq() >= at, "{label}");
                } else {
                    let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
                    ship(Shipper::Live, &primary, &mut standby, &mut cursor).unwrap();
                    let standby_rows = rows(&standby);
                    for id in &acknowledged {
                        assert!(
                            standby_rows.contains_key(id),
                            "{label}: {id} after catch-up"
                        );
                    }
                    assert_eq!(standby_rows, primary_rows, "{label}");
                }
            }
        }
    }
}

/// (d) Promotion while a commit is in flight, under synchronous replication
/// (one standby must confirm). The commit is paused between its write and
/// its sync's return, the standby is promoted after the window with the
/// commit's frame still undelivered, and the commit then becomes durable on
/// the old primary only. Its quorum wait must fail, so no client is told it
/// succeeded; the promoted primary never applies the late frame; the old
/// primary comes back with a longer tail that was never durable in the
/// cluster, and it is rebuilt, never streamed or merged.
#[test]
fn promotion_with_a_commit_in_flight_never_acknowledges_or_merges_it() {
    for storage in storage_modes() {
        let label = format!("{storage:?}");
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut standby = open_replica(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        let mut acknowledged: Vec<String> = Vec::new();
        let promoted_at;
        let late_frames;
        {
            let mut primary = open_replica(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            primary.create_collection("items").unwrap();
            let quorum = primary.sync_quorum();
            quorum.configure(
                1,
                std::time::Duration::from_millis(300),
                bicdb_core::SyncTimeoutPolicy::Fail,
            );
            let mut cursor = ReplicationCursor::new(0);
            for index in 0..3 {
                let id = format!("a{index}");
                put(&primary, &id).unwrap();
                ship(Shipper::Live, &primary, &mut standby, &mut cursor).unwrap();
                quorum.record_ack("standby", standby.last_applied_commit_seq());
                quorum
                    .wait_for(primary.replication_feed().durable_seq())
                    .unwrap();
                acknowledged.push(id);
            }
            let primary = Arc::new(primary);
            let (result, window) =
                commit_with_fate(&primary, primary_dir.path(), Fate::Durable, "inflight");
            let window = window.unwrap();
            assert!(window.log_grew, "{label}");
            assert!(!mentions(&window.shipped_during, "inflight"), "{label}");
            result.unwrap();
            let seq = primary.replication_feed().durable_seq();
            // The frame is now shippable, but the standby is promoted before
            // it is delivered.
            late_frames = primary
                .replication_frames_after(&mut cursor, 1_000)
                .unwrap();
            assert!(mentions(&late_frames, "inflight"), "{label}");
            promoted_at = standby.last_applied_commit_seq();
            standby.promote_by_election(1, "standby", None).unwrap();
            // The old primary's client waits for its quorum confirmation,
            // which the promoted standby never sends.
            let refused = quorum.wait_for(seq).unwrap_err();
            assert!(
                !matches!(refused, BicDbError::WriteFailed { .. }),
                "{label}: {refused}"
            );
            // The crash of the old primary.
        }
        drop(standby);
        let mut promoted = open_replica(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        // A late delivery of the in-flight frame is never applied.
        assert!(
            promoted.apply_replication_batch(&late_frames).is_err(),
            "{label}: the promoted primary applied the deposed primary's frame"
        );
        let promoted_rows = rows(&promoted);
        for id in &acknowledged {
            assert!(
                promoted_rows.contains_key(id),
                "{label}: acknowledged {id} lost"
            );
        }
        assert!(!promoted_rows.contains_key("inflight"), "{label}");
        put(&promoted, "after-failover").unwrap();
        let old = open_replica(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        assert!(
            rows(&old).contains_key("inflight"),
            "{label}: durable on the old disk"
        );
        assert!(old.last_applied_commit_seq() > promoted_at, "{label}");
        assert!(
            matches!(
                promoted
                    .standby_resume(0, old.last_applied_commit_seq())
                    .unwrap(),
                StandbyResume::Snapshot(_)
            ),
            "{label}: the old primary's longer tail must be rebuilt, never streamed"
        );
    }
}

/// A replicated logical decision can be much larger than either node's pool.
/// Fail standby page-image writes while structural cuts stream that decision;
/// recovery must preserve its older acknowledged prefix and the new batch must
/// stay whole, catch up exactly, and remain whole through promotion/reopening.
#[test]
#[cfg(feature = "experimental-structural-cuts")]
fn structural_cuts_replicate_large_batches_through_apply_failure_and_promotion() {
    let small = |mode| {
        config(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(256 * 1024)
            .with_replication(replication(mode))
    };
    for fail_after in [1, 5, 20, 100] {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut primary =
            BicDb::open_with_config(primary_dir.path(), small(ReplicationMode::Primary)).unwrap();
        primary.create_collection("items").unwrap();
        let mut tx = primary.begin_transaction().unwrap();
        for i in 0..300 {
            tx.insert(
                "items",
                Record::new(format!("acked-{i:05}"))
                    .with_metadata(json!({"id": i, "payload": "a".repeat(1024)})),
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let older = rows(&primary);
        let mut standby =
            BicDb::open_with_config(standby_dir.path(), small(ReplicationMode::Standby)).unwrap();
        let mut cursor = ReplicationCursor::new(0);
        ship(Shipper::Live, &primary, &mut standby, &mut cursor).unwrap();
        let acknowledged_prefix = standby.last_applied_commit_seq();
        assert_eq!(rows(&standby), older);
        let mut tx = primary.begin_transaction().unwrap();
        for i in 0..2_000 {
            tx.insert(
                "items",
                Record::new(format!("large-{i:05}"))
                    .with_metadata(json!({"id": i, "payload": "b".repeat(1024)})),
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let expected = rows(&primary);
        let frames = primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap();
        let fault = arm_io_failure("page_log.write", standby_dir.path(), fail_after, false);
        let result = standby
            .apply_replication_batch_within(&frames, primary.replication_feed().durable_seq());
        assert!(fault.fired() > 0, "image fault {fail_after} never reached");
        assert!(
            result.is_err(),
            "failed page apply {fail_after} acknowledged success"
        );
        assert!(standby.last_applied_commit_seq() >= acknowledged_prefix);
        drop(fault);
        drop(standby);
        let mut standby =
            BicDb::open_with_config(standby_dir.path(), small(ReplicationMode::Standby)).unwrap();
        let recovered = rows(&standby);
        for (id, value) in &older {
            assert_eq!(
                recovered.get(id),
                Some(value),
                "fault {fail_after}: lost {id}"
            );
        }
        let large = recovered
            .keys()
            .filter(|id| id.starts_with("large-"))
            .count();
        assert!(
            large == 0 || large == 2_000,
            "fault {fail_after}: partial batch {large}"
        );
        assert!(standby.last_applied_commit_seq() <= primary.replication_feed().durable_seq());
        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        ship(Shipper::Live, &primary, &mut standby, &mut cursor).unwrap();
        assert_eq!(
            rows(&standby),
            expected,
            "fault {fail_after}: catch-up differs"
        );
        standby.promote_by_election(1, "standby", None).unwrap();
        drop(standby);
        for reopen in 0..2 {
            let promoted =
                BicDb::open_with_config(standby_dir.path(), small(ReplicationMode::Primary))
                    .unwrap();
            assert_eq!(
                rows(&promoted),
                expected,
                "fault {fail_after}: promoted reopen {reopen}"
            );
            drop(promoted);
        }
    }
}
