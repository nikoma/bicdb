//! Fail-closed durability at the database level: transaction outcomes,
//! fencing, the reopen-and-verify procedure, log cuts, checkpoints,
//! concurrent commits and replication. See
//! `docs/durability-failure-handling.md`.
//!
//! Faults are injected with `bicdb_page::test_faults::arm_io_failure`,
//! scoped to each test's directory, so these tests run in parallel.

use std::sync::{Arc, Barrier, Mutex};

use bicdb_core::{
    BicDb, BicDbError, CommitResolution, DbConfig, DurabilityState, Record, ReplicationConfig,
    ReplicationCursor, ReplicationMode, ReplicationTlsConfig, StorageMode, TransactionOutcome,
    DURABILITY_FAILURE_MARKER,
};
use bicdb_page::test_faults::{arm_io_failure, arm_io_hook};
use serde_json::json;

fn config(storage: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(storage)
}

fn storage_modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

fn put(db: &BicDb, id: &str) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    tx.insert("items", Record::new(id).with_metadata(json!({ "id": id })))?;
    tx.commit()
}

fn present(db: &BicDb, id: &str) -> bool {
    db.get("items", id).unwrap().is_some()
}

fn failed_site(db: &BicDb) -> String {
    match db.durability_state() {
        DurabilityState::Healthy => panic!("expected WriteFailed"),
        DurabilityState::WriteFailed(failure) => failure.site,
    }
}

/// Everything a fenced handle must refuse.
fn assert_fenced(db: &BicDb, dir: &std::path::Path, label: &str) {
    let error = put(db, "refused").unwrap_err();
    assert!(
        matches!(error, BicDbError::WriteFailed { .. }),
        "{label}: write after the failure: {error}"
    );
    assert!(db.check_recovery_state().is_err(), "{label}");
    assert!(db.checkpoint_for_resume().is_err(), "{label}");
    // Reads are fenced too (the same rule a failed materialization uses).
    assert!(
        db.get("items", "a0").is_err(),
        "{label}: read on a fenced handle"
    );
    assert!(
        dir.join(DURABILITY_FAILURE_MARKER).exists(),
        "{label}: marker"
    );
}

/// Reopen: recovery + verification clears the marker, every acknowledged
/// row is present, and writes resume.
fn reopen_and_verify(path: &std::path::Path, storage: StorageMode, acked: &[String]) -> BicDb {
    let db = BicDb::open_with_config(path, config(storage)).unwrap();
    assert!(db.durability_state().is_healthy());
    assert!(!path.join(DURABILITY_FAILURE_MARKER).exists());
    for id in acked {
        assert!(present(&db, id), "acknowledged {id} lost after reopen");
    }
    put(&db, "after-reopen").unwrap();
    db.checkpoint_for_resume().unwrap();
    db
}

fn seed(db: &mut BicDb, rows: usize) -> Vec<String> {
    db.create_collection("items").unwrap();
    (0..rows)
        .map(|index| {
            let id = format!("a{index}");
            put(db, &id).unwrap();
            id
        })
        .collect()
}

/// `transaction_log.write` fails before any byte reaches the log. The commit
/// reports an unknown outcome (08007), the receipt says recovery required,
/// and after the reopen the commit is absent.
#[test]
fn a_failed_transaction_log_write_fences_and_reports_an_unknown_outcome() {
    for storage in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        let acked;
        {
            let mut db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
            acked = seed(&mut db, 10);
            let fault = arm_io_failure("transaction_log.write", dir.path(), 1, false);
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("items", Record::new("x")).unwrap();
            let observer = tx.observe_outcome();
            let error = tx.commit().unwrap_err();
            assert_eq!(fault.fired(), 1);
            assert!(
                matches!(error, BicDbError::CommitOutcomeUnknown { .. }),
                "{storage:?}: {error}"
            );
            assert!(matches!(
                observer.outcome(),
                TransactionOutcome::RecoveryRequired { .. }
            ));
            assert_eq!(failed_site(&db), "transaction_log.write");
            assert_fenced(&db, dir.path(), &format!("{storage:?}"));
            assert_eq!(fault.seen(), 1, "{storage:?}: a fenced log never retries");
        }
        let db = reopen_and_verify(dir.path(), storage, &acked);
        assert!(
            !present(&db, "x"),
            "nothing of the failed write reached the log"
        );
    }
}

/// `transaction_log.sync`: the bytes were written, the sync failed. With
/// the tail repair working the record is removed; with the repair failing
/// too it stays, and recovery honours it. Both report an unknown outcome.
#[test]
fn an_unknown_outcome_resolves_to_what_reached_the_disk() {
    for storage in storage_modes() {
        for repair_fails in [false, true] {
            let label = format!("{storage:?} repair_fails={repair_fails}");
            let dir = tempfile::tempdir().unwrap();
            let acked;
            {
                let mut db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
                acked = seed(&mut db, 5);
                let _sync = arm_io_failure("transaction_log.sync", dir.path(), 1, false);
                let _repair = repair_fails
                    .then(|| arm_io_failure("transaction_log.repair", dir.path(), 1, false));
                let error = put(&db, "x").unwrap_err();
                assert!(
                    matches!(error, BicDbError::CommitOutcomeUnknown { .. }),
                    "{label}: {error}"
                );
                assert_fenced(&db, dir.path(), &label);
            }
            let db = reopen_and_verify(dir.path(), storage.clone(), &acked);
            assert_eq!(present(&db, "x"), repair_fails, "{label}");
        }
    }
}

/// Paged: the transaction-log decision is durable, then the page log's sync
/// fails while the commit materializes. The commit reports an unknown
/// outcome; recovery replays the durable decision, so the row is present.
#[test]
fn a_durable_decision_whose_page_commit_fails_is_kept_by_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let acked;
    {
        let mut db = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
        acked = seed(&mut db, 5);
        let fault = arm_io_failure("page_log.sync", dir.path(), 1, false);
        let error = put(&db, "x").unwrap_err();
        assert_eq!(fault.fired(), 1);
        assert!(
            matches!(error, BicDbError::CommitOutcomeUnknown { .. }),
            "{error}"
        );
        assert_eq!(failed_site(&db), "page_log.sync");
        assert_fenced(&db, dir.path(), "paged");
    }
    let db = reopen_and_verify(dir.path(), StorageMode::ServerPaged, &acked);
    assert!(present(&db, "x"), "the durable decision must be honoured");
}

/// Concurrent committers against a failing transaction-log write: every
/// commit acknowledged before the failure survives, none is acknowledged
/// after it (per writer: once a writer saw an error it never sees `Ok`),
/// and every failure is a durability failure.
#[test]
fn concurrent_commits_around_a_failing_write_never_acknowledge_after_it() {
    for (storage, writers) in storage_modes()
        .into_iter()
        .flat_map(|storage| [(storage.clone(), 8usize), (storage, 32usize)])
    {
        let dir = tempfile::tempdir().unwrap();
        let acked = Arc::new(Mutex::new(Vec::new()));
        {
            let mut db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
            seed(&mut db, 1);
            let fault = arm_io_failure("transaction_log.write", dir.path(), 25, false);
            let db = Arc::new(db);
            let start = Arc::new(Barrier::new(writers));
            std::thread::scope(|scope| {
                for writer in 0..writers {
                    let db = db.clone();
                    let acked = acked.clone();
                    let start = start.clone();
                    scope.spawn(move || {
                        start.wait();
                        let mut failed = false;
                        for index in 0..40 {
                            let id = format!("w{writer}-{index}");
                            match put(&db, &id) {
                                Ok(()) => {
                                    assert!(!failed, "{id} acknowledged after a failure");
                                    acked.lock().unwrap().push(id);
                                }
                                Err(error) => {
                                    assert!(error.is_durability_failure(), "{id}: {error}");
                                    failed = true;
                                }
                            }
                        }
                        assert!(failed, "writer {writer} never saw the failure");
                    });
                }
            });
            assert_eq!(fault.fired(), 1);
            assert_eq!(fault.seen(), 25, "{storage:?}: no write after the failure");
            assert_fenced(&db, dir.path(), &format!("{storage:?}"));
        }
        let mut acked = acked.lock().unwrap().clone();
        acked.push("a0".into());
        reopen_and_verify(dir.path(), storage, &acked);
    }
}

/// A failed page-file sync in the checkpoint fences the database.
#[test]
fn a_failed_checkpoint_fences_and_the_reopen_verifies() {
    for site in ["page_file.sync", "page_log.sync", "page_log.reset"] {
        let dir = tempfile::tempdir().unwrap();
        let acked;
        {
            let mut db =
                BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
            acked = seed(&mut db, 20);
            let fault = arm_io_failure(site, dir.path(), 1, false);
            let error = db.checkpoint_for_resume().unwrap_err();
            assert_eq!(fault.fired(), 1, "{site}");
            assert!(
                matches!(error, BicDbError::WriteFailed { .. }),
                "{site}: {error}"
            );
            assert_eq!(failed_site(&db), site);
            assert_fenced(&db, dir.path(), site);
        }
        reopen_and_verify(dir.path(), StorageMode::ServerPaged, &acked);
    }
}

/// The transaction-log cut. After the new log is renamed over the old one,
/// a failed sync used to leave the writer's descriptor on the unlinked old
/// inode, so later commits were acknowledged and then vanished on restart.
/// Now the database is fenced. A failure before the rename (the copy) is an
/// ordinary, retryable checkpoint error.
#[test]
fn a_failed_log_cut_fences_after_the_rename_and_not_before() {
    for (site, fences) in [
        ("transaction_log.cut_sync", true),
        ("transaction_log.cut_rename", true),
        ("transaction_log.cut_copy", false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut acked;
        {
            let mut db =
                BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
            acked = seed(&mut db, 20);
            let fault = arm_io_failure(site, dir.path(), 1, false);
            let result = db.checkpoint_for_resume();
            if fault.fired() == 0 {
                // A cut that drops the whole log truncates instead of
                // copying a tail, so the copy site may not be reached; the
                // case then only asserts that nothing fences.
                assert!(!fences, "{site}: fault not reached");
                assert!(result.is_ok(), "{site}");
            } else {
                let error = result.unwrap_err();
                assert_eq!(
                    matches!(error, BicDbError::WriteFailed { .. }),
                    fences,
                    "{site}: {error}"
                );
            }
            if fences {
                assert_eq!(failed_site(&db), "transaction_log.cut");
                assert_fenced(&db, dir.path(), site);
            } else {
                assert!(db.durability_state().is_healthy(), "{site}");
                put(&db, "after-copy-failure").unwrap();
                acked.push("after-copy-failure".into());
                db.checkpoint_for_resume().unwrap();
            }
        }
        reopen_and_verify(dir.path(), StorageMode::ServerPaged, &acked);
    }
}

/// Verification is required: a reopen whose verification checkpoint fails
/// keeps the marker and fails the open; a later clean open succeeds.
#[test]
fn a_reopen_whose_verification_fails_stays_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let acked;
    {
        let mut db = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
        acked = seed(&mut db, 5);
        let _fault = arm_io_failure("transaction_log.write", dir.path(), 1, false);
        put(&db, "x").unwrap_err();
    }
    let marker = dir.path().join(DURABILITY_FAILURE_MARKER);
    assert!(marker.exists());
    {
        let _fault = arm_io_failure("page_file.sync", dir.path(), 1, true);
        let error = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged))
            .err()
            .expect("an open whose recovery or verification fails must fail");
        eprintln!("open refused as expected: {error}");
        assert!(marker.exists(), "a failed verification keeps the marker");
    }
    reopen_and_verify(dir.path(), StorageMode::ServerPaged, &acked);
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

fn ship(
    primary: &BicDb,
    standby: &mut BicDb,
    cursor: &mut ReplicationCursor,
) -> bicdb_core::Result<()> {
    loop {
        let frames = primary.replication_frames_after(cursor, 1_000)?;
        if frames.is_empty() {
            return Ok(());
        }
        standby.apply_replication_batch(&frames)?;
    }
}

/// A fenced primary ships exactly its durable prefix and confirms nothing
/// more; the standby keeps serving it. After the primary's reopen,
/// replication resumes.
#[test]
fn a_fenced_primary_ships_only_its_durable_prefix_and_the_standby_keeps_working() {
    for storage in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut standby = open_replica(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        let durable_seq;
        {
            let mut primary = open_replica(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            seed(&mut primary, 5);
            ship(&primary, &mut standby, &mut cursor).unwrap();
            durable_seq = primary.current_commit_seq();
            let _fault = arm_io_failure("transaction_log.write", primary_dir.path(), 1, false);
            let error = put(&primary, "x").unwrap_err();
            assert!(error.is_durability_failure(), "{storage:?}: {error}");
            // Nothing past the durable prefix ships.
            ship(&primary, &mut standby, &mut cursor).unwrap();
            assert_eq!(
                standby.last_applied_commit_seq(),
                durable_seq,
                "{storage:?}"
            );
            // A quorum confirmation is never issued by a fenced primary,
            // even for a sequence the standby holds.
            primary.sync_quorum().record_ack("standby", durable_seq);
            let refused = primary.sync_quorum().wait_for(durable_seq).unwrap_err();
            assert!(
                matches!(refused, BicDbError::CommitOutcomeUnknown { .. }),
                "{storage:?}: {refused}"
            );
            assert!(put(&primary, "y").is_err());
        }
        // The standby is unaffected: healthy and serving the durable prefix.
        assert!(standby.durability_state().is_healthy());
        assert!(present(&standby, "a4"), "{storage:?}");
        assert!(!present(&standby, "x"), "{storage:?}");

        let primary = open_replica(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        assert!(primary.durability_state().is_healthy());
        assert!(!present(&primary, "x"));
        put(&primary, "after").unwrap();
        // The reopen's verification checkpoint may have cut the log; a
        // sender resumes from the standby's applied position.
        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        ship(&primary, &mut standby, &mut cursor).unwrap();
        assert!(
            present(&standby, "after"),
            "{storage:?}: replication resumes"
        );
    }
}

/// A standby whose own log write fails is fenced: it imports nothing more
/// and its applied position (what it acknowledges) stops at its durable
/// prefix. The primary is unaffected; the reopened standby catches up.
#[test]
fn a_failing_standby_is_fenced_and_catches_up_after_reopen() {
    let storage = StorageMode::EmbeddedMemory;
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();
    let mut primary = open_replica(
        primary_dir.path(),
        storage.clone(),
        ReplicationMode::Primary,
    );
    seed(&mut primary, 3);
    let applied;
    {
        let mut standby = open_replica(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        ship(&primary, &mut standby, &mut cursor).unwrap();
        applied = standby.last_applied_commit_seq();
        put(&primary, "b0").unwrap();
        put(&primary, "b1").unwrap();
        let _fault = arm_io_failure("transaction_log.write", standby_dir.path(), 1, false);
        assert!(ship(&primary, &mut standby, &mut cursor).is_err());
        assert_eq!(standby.last_applied_commit_seq(), applied);
        assert_eq!(failed_site(&standby), "transaction_log.write");
        // A sender retries from the standby's applied position.
        let mut retry = ReplicationCursor::new(standby.last_applied_commit_seq());
        assert!(
            ship(&primary, &mut standby, &mut retry).is_err(),
            "fenced standby"
        );
        assert_eq!(standby.last_applied_commit_seq(), applied);
    }
    assert!(primary.durability_state().is_healthy());
    put(&primary, "b2").unwrap();
    let mut standby = open_replica(standby_dir.path(), storage, ReplicationMode::Standby);
    assert!(standby.durability_state().is_healthy());
    let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
    ship(&primary, &mut standby, &mut cursor).unwrap();
    for id in ["a2", "b0", "b1", "b2"] {
        assert!(present(&standby, id), "{id}");
    }
}

/// Outcome resolution by identity. Every 08007 carries the incarnation, the
/// commit sequence and the transaction id; after the reopen,
/// `resolve_commit_outcome` gives a definitive answer that matches what
/// recovery kept, in each direction.
#[test]
fn an_unknown_commit_resolves_by_identity_to_what_recovery_kept() {
    // (storage, faults, recovery keeps it)
    let cases: Vec<(StorageMode, Vec<&str>, bool)> = vec![
        (
            StorageMode::EmbeddedMemory,
            vec!["transaction_log.write"],
            false,
        ),
        (
            StorageMode::ServerPaged,
            vec!["transaction_log.write"],
            false,
        ),
        (
            StorageMode::EmbeddedMemory,
            vec!["transaction_log.sync"],
            false,
        ),
        (
            StorageMode::EmbeddedMemory,
            vec!["transaction_log.sync", "transaction_log.repair"],
            true,
        ),
        (
            StorageMode::ServerPaged,
            vec!["transaction_log.sync", "transaction_log.repair"],
            true,
        ),
        (StorageMode::ServerPaged, vec!["page_log.sync"], true),
    ];
    for (storage, faults, kept) in cases {
        let label = format!("{storage:?} {faults:?}");
        let dir = tempfile::tempdir().unwrap();
        let (incarnation, sequence, transaction_id);
        {
            let mut db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
            seed(&mut db, 3);
            let _faults: Vec<_> = faults
                .iter()
                .map(|site| arm_io_failure(site, dir.path(), 1, false))
                .collect();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("items", Record::new("x")).unwrap();
            let observer = tx.observe_outcome();
            let error = tx.commit().unwrap_err();
            let BicDbError::CommitOutcomeUnknown {
                sequence: seq,
                transaction_id: tx_id,
                incarnation: inc,
                ..
            } = &error
            else {
                panic!("{label}: {error}");
            };
            assert_eq!(inc, db.incarnation_id(), "{label}");
            assert_eq!(*tx_id, Some(observer.transaction_id().0), "{label}");
            assert_eq!(
                observer.outcome(),
                TransactionOutcome::RecoveryRequired {
                    sequence: seq.unwrap()
                },
                "{label}: the receipt names the same commit"
            );
            let (detail, hint) = error.commit_outcome_identity().unwrap();
            assert!(
                detail.contains(&format!("commit_seq={}", seq.unwrap())),
                "{label}: {detail}"
            );
            assert!(hint.contains(inc.as_str()), "{label}: {hint}");
            // The failed handle itself never answers.
            assert_eq!(
                db.resolve_commit_outcome(inc, seq.unwrap()).unwrap(),
                CommitResolution::Unresolved,
                "{label}"
            );
            (incarnation, sequence, transaction_id) = (inc.clone(), seq.unwrap(), *tx_id);
        }
        let _ = transaction_id;
        let db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
        let resolution = db.resolve_commit_outcome(&incarnation, sequence).unwrap();
        assert_eq!(present(&db, "x"), kept, "{label}");
        assert_eq!(
            resolution,
            if kept {
                CommitResolution::Committed
            } else {
                CommitResolution::NotCommitted
            },
            "{label}"
        );
        // New commits may reuse the sequence number; the answer is stable
        // because it is keyed by the failed incarnation.
        put(&db, "after").unwrap();
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config(storage)).unwrap();
        assert_eq!(
            db.resolve_commit_outcome(&incarnation, sequence).unwrap(),
            resolution,
            "{label}: the answer must not change"
        );
    }
}

/// Failover after a primary failure. The primary's uncertain commit reached
/// its own disk (the tail repair failed too) but was never shipped: shipping
/// stops at the durable prefix. The promoted standby's history, which is
/// authoritative, does not contain it, takes new writes, and recognises the
/// old primary's divergent tail (it must be rebuilt from a snapshot, never
/// streamed into the new history).
#[test]
fn a_primary_that_fails_mid_stream_is_replaced_by_a_promoted_standby() {
    for storage in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let start;
        {
            let mut standby = open_replica(
                standby_dir.path(),
                storage.clone(),
                ReplicationMode::Standby,
            );
            let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
            let mut primary = open_replica(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            seed(&mut primary, 4);
            ship(&primary, &mut standby, &mut cursor).unwrap();
            put(&primary, "streamed").unwrap();
            let _sync = arm_io_failure("transaction_log.sync", primary_dir.path(), 1, false);
            let _repair = arm_io_failure("transaction_log.repair", primary_dir.path(), 1, false);
            let error = put(&primary, "x").unwrap_err();
            assert!(
                matches!(error, BicDbError::CommitOutcomeUnknown { .. }),
                "{storage:?}: {error}"
            );
            ship(&primary, &mut standby, &mut cursor).unwrap();
            assert!(present(&standby, "streamed"), "{storage:?}");
            assert!(
                !present(&standby, "x"),
                "{storage:?}: an uncertain commit shipped"
            );
            start = standby.last_applied_commit_seq();
            standby.promote_by_election(1, "standby", None).unwrap();
        }
        let promoted = open_replica(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        assert_eq!(promoted.replication_role(), bicdb_core::NodeRole::Primary);
        assert!(!present(&promoted, "x"), "{storage:?}");
        put(&promoted, "after-failover").unwrap();
        // The old primary recovers its own disk, which holds the uncertain
        // commit past the promotion point: a divergent history.
        let old = open_replica(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        assert!(
            present(&old, "x"),
            "{storage:?}: the decision reached the old primary's disk"
        );
        assert!(old.last_applied_commit_seq() > start, "{storage:?}");
        assert!(
            matches!(
                promoted
                    .standby_resume(0, old.last_applied_commit_seq())
                    .unwrap(),
                bicdb_core::StandbyResume::Snapshot(_)
            ),
            "{storage:?}: the old primary must be rebuilt, never streamed"
        );
    }
}

/// A fence that lands after the commit is durable and visible (here: the
/// record-audit publication marker's append fails) must not turn the
/// commit into a refused write: the commit is not ambiguous, and a 58030
/// would make the client retry and duplicate it. It reports success; the
/// database is fenced for everything after it; recovery keeps the commit.
#[test]
fn a_fence_after_a_durable_commit_does_not_report_the_commit_as_refused() {
    let dir = tempfile::tempdir().unwrap();
    let acked;
    {
        let mut db = BicDb::open_with_config(
            dir.path(),
            config(StorageMode::EmbeddedMemory).with_audit_events(true),
        )
        .unwrap();
        acked = seed(&mut db, 3);
        // Call 1 is the commit's own batch; call 2 is the publication marker.
        let fault = arm_io_failure("transaction_log.write", dir.path(), 2, false);
        put(&db, "x").expect("a durable, visible commit is reported committed");
        assert_eq!(fault.fired(), 1, "the publication marker was never written");
        assert_eq!(failed_site(&db), "transaction_log.auxiliary");
        assert_fenced(&db, dir.path(), "after the publication failure");
    }
    let db = BicDb::open_with_config(
        dir.path(),
        config(StorageMode::EmbeddedMemory).with_audit_events(true),
    )
    .unwrap();
    assert!(db.durability_state().is_healthy());
    for id in acked.iter().map(String::as_str).chain(["x"]) {
        assert!(present(&db, id), "{id} lost");
    }
    put(&db, "after").unwrap();
}

/// A bulk load commits only in the page store: its page-log commit record
/// is the decision, and a failed sync may still have put it on disk. That
/// is an unknown outcome (08007, no commit sequence), not a refused write.
/// Recovery keeps the batch all or nothing.
#[test]
fn a_failed_bulk_load_commit_reports_an_unknown_outcome() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
        db.create_collection("bulk").unwrap();
    }
    {
        let mut db = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
        let fault = arm_io_failure("page_log.sync", dir.path(), 1, false);
        let records: Vec<_> = (0..50).map(|i| Record::new(format!("b{i}"))).collect();
        let error = db.bulk_load_insert("bulk", records).unwrap_err();
        assert_eq!(fault.fired(), 1);
        assert!(
            matches!(
                error,
                BicDbError::CommitOutcomeUnknown { sequence: None, .. }
            ),
            "{error}"
        );
        assert!(db.check_recovery_state().is_err());
    }
    let db = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
    let present = (0..50)
        .filter(|i| db.get("bulk", &format!("b{i}")).unwrap().is_some())
        .count();
    assert!(
        present == 0 || present == 50,
        "a partial batch survived: {present}"
    );
}

/// A backup-gate timeout (a backup or branch holding the gate too long)
/// happens before any byte is written. It is an ordinary error: it must not
/// fence a healthy primary, and the next commit succeeds.
#[test]
fn a_backup_gate_timeout_does_not_fence() {
    for storage in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
        seed(&mut db, 2);
        {
            // Scoped to the transaction log: the gate wait of the commit's
            // own durable write, after its decision was queued.
            let fault = arm_io_failure(
                "backup_gate.timeout",
                &dir.path().join("transactions.log"),
                1,
                false,
            );
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("items", Record::new("x")).unwrap();
            let observer = tx.observe_outcome();
            let error = tx.commit().unwrap_err();
            assert_eq!(fault.fired(), 1, "{storage:?}");
            // Nothing was written and the commit was withdrawn: it did not
            // commit, and the error is neither 58030 nor 08007.
            assert!(
                matches!(error, BicDbError::Backup(_)),
                "{storage:?}: {error}"
            );
            assert_eq!(
                observer.outcome(),
                TransactionOutcome::Aborted,
                "{storage:?}"
            );
        }
        assert!(
            db.durability_state().is_healthy(),
            "{storage:?}: a gate timeout fenced"
        );
        put(&db, "y").unwrap();
        assert!(present(&db, "y"), "{storage:?}: later commits stay visible");
        assert!(!present(&db, "x"), "{storage:?}");
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
        assert!(present(&db, "a1") && present(&db, "y"), "{storage:?}");
        assert!(
            !present(&db, "x"),
            "{storage:?}: a withdrawn commit came back"
        );
    }
}

/// Verification rewrites the transaction log to a new, synced inode (an
/// fsync of page-cache pages that a failed fsync left clean would write
/// nothing). A normal embedded open leaves the log alone.
///
/// The test holds the old log open while the database runs. Without that,
/// the comparison was racy: a paged open cuts the log (copy and rename) and
/// so does the verification and the follow-up checkpoint, each freeing the
/// previous inode, and the filesystem may hand a freed inode number to the
/// next copy. The log could then end on the same inode number it started on
/// although it was rewritten, which failed this test intermittently. An open
/// descriptor keeps the old inode allocated, so a rewritten log always has a
/// different number.
#[cfg(unix)]
#[test]
fn verification_rewrites_the_transaction_log() {
    use std::os::unix::fs::MetadataExt;
    for storage in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("transactions.log");
        let acked;
        {
            let mut db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
            acked = seed(&mut db, 4);
        }
        let inode = || std::fs::metadata(&log).unwrap().ino();
        // Pin the inode the log has before each step.
        let pin = || std::fs::File::open(&log).unwrap();
        let before_normal = inode();
        let pinned_normal = pin();
        drop(BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap());
        let after_normal = inode();
        drop(pinned_normal);
        {
            let db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
            let _fault = arm_io_failure("transaction_log.write", dir.path(), 1, false);
            put(&db, "x").unwrap_err();
        }
        assert!(
            dir.path().join(DURABILITY_FAILURE_MARKER).exists(),
            "{storage:?}: no failure marker, so the next open would not verify"
        );
        let before_verify = inode();
        let pinned_verify = pin();
        let db = BicDb::open_with_config(dir.path(), config(storage.clone())).unwrap();
        let after_verify = inode();
        drop(pinned_verify);
        assert!(
            !dir.path().join(DURABILITY_FAILURE_MARKER).exists(),
            "{storage:?}: the open did not verify"
        );
        assert_ne!(
            after_verify, before_verify,
            "{storage:?}: verification did not rewrite the log"
        );
        drop(db);
        reopen_and_verify(dir.path(), storage.clone(), &acked);
        if storage == StorageMode::EmbeddedMemory {
            assert_eq!(before_normal, after_normal, "a normal open rewrote the log");
        }
    }
}

/// A fence that lands while a transaction-log cut is between its flush and
/// its rename stops the cut: a fenced database's log is its redo.
#[cfg(unix)]
#[test]
fn a_fence_during_a_log_cut_stops_the_rename() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config(StorageMode::ServerPaged)).unwrap();
    let acked = seed(&mut db, 5);
    let log = dir.path().join("transactions.log");
    let before = std::fs::metadata(&log).unwrap().ino();
    let health = db.durability_health_for_test();
    let hook = arm_io_hook("transaction_log.cut_window", dir.path(), 1, move || {
        health.record_failure("test.concurrent_fence", &"injected while the cut ran");
    });
    let error = db.checkpoint_for_resume().unwrap_err();
    assert_eq!(hook.fired(), 1, "the cut never reached its window");
    assert!(matches!(error, BicDbError::WriteFailed { .. }), "{error}");
    assert_eq!(
        std::fs::metadata(&log).unwrap().ino(),
        before,
        "a fenced database still replaced its log"
    );
    drop(db);
    reopen_and_verify(dir.path(), StorageMode::ServerPaged, &acked);
}
