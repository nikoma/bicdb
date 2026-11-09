//! Torn-tail recovery and failed-append repair for the generic frame logs:
//! the change-data-capture outbox (`sync.log`), the event log (user events
//! and the record-audit outbox behind commits), record segments, and the
//! live readers of the transaction log. See
//! `docs/durability-failure-handling.md`, section 7.4.
//!
//! Faults are injected with `bicdb_page::test_faults::arm_io_failure`,
//! scoped to one file of each test's directory, so these tests run in
//! parallel. Every test reconciles what the database holds after a reopen
//! against what it acknowledged before it.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use bicdb_core::{
    salvage_frame_log, BicDb, BicDbError, DbConfig, DurabilityState, Event, Record, StorageMode,
    DEFAULT_EVENTS_DIR, DEFAULT_EVENTS_SEGMENT, DEFAULT_TRANSACTION_LOG, DURABILITY_FAILURE_MARKER,
    RECORD_AUDIT_STREAM,
};
use bicdb_page::test_faults::arm_io_failure;
use serde_json::json;

/// The three ways an append fails: nothing written, half the batch written,
/// the whole batch written but its sync failed.
const APPEND_FAILURES: [&str; 3] = ["frame_log.write", "frame_log.short_write", "frame_log.sync"];

fn storage_modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

fn config(storage: &StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(storage.clone())
        .with_sync_outbox(true)
        .with_audit_events(false)
}

fn sync_log(dir: &Path) -> PathBuf {
    dir.join("sync.log")
}

fn event_log(dir: &Path) -> PathBuf {
    dir.join(DEFAULT_EVENTS_DIR).join(DEFAULT_EVENTS_SEGMENT)
}

fn len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn ids(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index}"))
        .collect()
}

/// One transaction inserting every id: one multi-frame outbox batch.
fn commit_rows(db: &BicDb, ids: &[String]) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    for id in ids {
        tx.insert("items", Record::new(id).with_metadata(json!({ "id": id })))?;
    }
    tx.commit()
}

fn failed_site(db: &BicDb) -> String {
    match db.durability_state() {
        DurabilityState::Healthy => panic!("expected WriteFailed"),
        DurabilityState::WriteFailed(failure) => failure.site,
    }
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

/// The CDC outbox holds exactly one pending op per acknowledged row, none
/// for a row whose commit failed, and the rows themselves agree.
fn reconcile_sync_outbox(db: &BicDb, acknowledged: &[String], failed: &[String], label: &str) {
    let pending = sorted(
        db.pending_sync_ops()
            .into_iter()
            .map(|op| op.record_id)
            .collect(),
    );
    assert_eq!(
        pending,
        sorted(acknowledged.to_vec()),
        "{label}: CDC outbox must hold exactly the acknowledged commits' ops"
    );
    for id in acknowledged {
        assert!(db.get("items", id).unwrap().is_some(), "{label}: {id} lost");
    }
    for id in failed {
        assert!(
            db.get("items", id).unwrap().is_none(),
            "{label}: {id} of a failed commit is visible"
        );
    }
}

/// A failed, short or unsynced `sync.log` append. The append happens after
/// the commit is durable and visible (`docs/durability-failure-handling.md`,
/// section 7.5), so the commit stays committed and returns success; the
/// failed bytes are cut back; the database is fenced (`sync_log.publish`);
/// and the reopen republishes the commit's ops from the transaction log, so
/// the outbox holds exactly one op per committed row, across two reopens.
#[test]
fn a_failed_sync_log_append_keeps_the_commit_and_recovery_publishes_its_ops_once() {
    for storage in storage_modes() {
        for site in APPEND_FAILURES {
            let label = format!("{storage:?} {site}");
            let dir = tempfile::tempdir().unwrap();
            let mut committed = Vec::new();
            {
                let mut db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
                db.create_collection("items").unwrap();
                for batch in 0..4 {
                    let rows = ids(&format!("before{batch}"), 3);
                    commit_rows(&db, &rows).unwrap();
                    committed.extend(rows);
                }
                let before = len(&sync_log(dir.path()));
                let fault = arm_io_failure(site, &sync_log(dir.path()), 1, false);
                let durable = ids("durable", 6);
                commit_rows(&db, &durable).unwrap();
                assert_eq!(fault.fired(), 1, "{label}");
                assert_eq!(failed_site(&db), "sync_log.publish", "{label}");
                assert_eq!(
                    len(&sync_log(dir.path())),
                    before,
                    "{label}: the failed batch must be cut back"
                );
                drop(fault);
                committed.extend(durable);
            }
            for round in 0..2 {
                let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
                assert!(db.durability_state().is_healthy(), "{label}");
                reconcile_sync_outbox(&db, &committed, &[], &format!("{label} reopen {round}"));
            }
            let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            let rows = ids("after", 3);
            commit_rows(&db, &rows).unwrap();
            committed.extend(rows);
            reconcile_sync_outbox(&db, &committed, &[], &format!("{label} resumed"));
        }
    }
}

/// When the cut-back fails too, the outbox is poisoned and the database
/// fenced (`frame_log.repair`, the first failure); the commit, durable and
/// visible, still succeeds. The reopen's recovery cuts the failed bytes (the
/// marker), then republishes the commit's ops from the transaction log:
/// exactly one op per committed row, and commits resume.
#[test]
fn a_failed_sync_log_repair_fences_and_recovery_publishes_the_ops_once() {
    for storage in storage_modes() {
        for site in ["frame_log.short_write", "frame_log.sync"] {
            let label = format!("{storage:?} {site}");
            let dir = tempfile::tempdir().unwrap();
            let mut committed = Vec::new();
            {
                let mut db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
                db.create_collection("items").unwrap();
                for batch in 0..3 {
                    let rows = ids(&format!("before{batch}"), 3);
                    commit_rows(&db, &rows).unwrap();
                    committed.extend(rows);
                }
                let before = len(&sync_log(dir.path()));
                let failing = arm_io_failure(site, &sync_log(dir.path()), 1, false);
                let repair = arm_io_failure("frame_log.repair", &sync_log(dir.path()), 1, false);
                let durable = ids("durable", 8);
                commit_rows(&db, &durable).unwrap();
                assert_eq!((failing.fired(), repair.fired()), (1, 1), "{label}");
                assert_eq!(failed_site(&db), "frame_log.repair", "{label}");
                assert!(
                    dir.path().join(DURABILITY_FAILURE_MARKER).exists(),
                    "{label}"
                );
                assert!(
                    len(&sync_log(dir.path())) > before,
                    "{label}: failed bytes stayed"
                );
                drop((failing, repair));
                committed.extend(durable);
                let refused = commit_rows(&db, &ids("refused", 1)).unwrap_err();
                assert!(refused.is_durability_failure(), "{label}: {refused}");
            }
            let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            assert!(db.durability_state().is_healthy(), "{label}");
            assert!(
                !dir.path().join(DURABILITY_FAILURE_MARKER).exists(),
                "{label}"
            );
            reconcile_sync_outbox(&db, &committed, &[], &format!("{label} reopen"));
            let rows = ids("resumed", 2);
            commit_rows(&db, &rows).unwrap();
            committed.extend(rows);
            drop(db);
            let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            reconcile_sync_outbox(&db, &committed, &[], &format!("{label} second reopen"));
        }
    }
}

/// A failed `mark_synced` leaves the ops pending (the consumer retries), and
/// the log keeps a clean boundary for the next acknowledgment.
#[test]
fn a_failed_sync_acknowledgment_keeps_the_ops_pending_and_the_log_clean() {
    for site in APPEND_FAILURES {
        let dir = tempfile::tempdir().unwrap();
        let storage = StorageMode::EmbeddedMemory;
        let rows = ids("row", 6);
        {
            let mut db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            db.create_collection("items").unwrap();
            commit_rows(&db, &rows).unwrap();
            let ops = db.pending_sync_ops();
            let first: Vec<_> = ops[..3].iter().map(|op| op.op_id).collect();
            let before = len(&sync_log(dir.path()));
            let fault = arm_io_failure(site, &sync_log(dir.path()), 1, false);
            assert!(db.mark_synced(&first).is_err(), "{site}");
            assert_eq!(fault.fired(), 1);
            drop(fault);
            assert_eq!(len(&sync_log(dir.path())), before, "{site}");
            assert_eq!(db.pending_sync_ops().len(), 6, "{site}");
            db.mark_synced(&first).unwrap();
        }
        let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
        assert_eq!(db.pending_sync_ops().len(), 3, "{site}");
    }
}

fn app_event(index: usize, tag: &str) -> Event {
    Event::new("app", "Thing", json!({ "tag": tag, "n": index }))
}

fn app_event_tags(db: &BicDb) -> Vec<String> {
    db.events()
        .read("app")
        .unwrap()
        .into_iter()
        .map(|stored| stored.event.payload["tag"].as_str().unwrap().to_string())
        .collect()
}

/// Event-log appends: a failed one returns an error and leaves no bytes;
/// every acknowledged event, including those appended after the failure,
/// is present after a reopen, in order, exactly once.
#[test]
fn a_failed_event_append_is_cut_back_and_later_events_survive_reopen() {
    for storage in storage_modes() {
        for site in APPEND_FAILURES {
            let label = format!("{storage:?} {site}");
            let dir = tempfile::tempdir().unwrap();
            let mut acknowledged = Vec::new();
            {
                let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
                for index in 0..5 {
                    let tag = format!("before{index}");
                    db.events().append(app_event(index, &tag)).unwrap();
                    acknowledged.push(tag);
                }
                let before = len(&event_log(dir.path()));
                let fault = arm_io_failure(site, &event_log(dir.path()), 1, false);
                assert!(
                    db.events().append(app_event(99, "failed")).is_err(),
                    "{label}"
                );
                assert_eq!(fault.fired(), 1, "{label}");
                drop(fault);
                assert_eq!(len(&event_log(dir.path())), before, "{label}");
                assert!(db.durability_state().is_healthy(), "{label}");
                for index in 0..5 {
                    let tag = format!("after{index}");
                    db.events().append(app_event(index, &tag)).unwrap();
                    acknowledged.push(tag);
                }
                assert_eq!(app_event_tags(&db), acknowledged, "{label}");
            }
            for round in 0..2 {
                let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
                assert_eq!(app_event_tags(&db), acknowledged, "{label} reopen {round}");
            }
        }
    }
}

/// A failed event-log repair fences the database; the reopen cuts the
/// failed event and keeps every acknowledged one.
#[test]
fn a_failed_event_log_repair_fences_and_recovery_keeps_only_acknowledged_events() {
    for storage in storage_modes() {
        let label = format!("{storage:?}");
        let dir = tempfile::tempdir().unwrap();
        let mut acknowledged = Vec::new();
        {
            let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            for index in 0..4 {
                let tag = format!("before{index}");
                db.events().append(app_event(index, &tag)).unwrap();
                acknowledged.push(tag);
            }
            let failing = arm_io_failure("frame_log.sync", &event_log(dir.path()), 1, false);
            let repair = arm_io_failure("frame_log.repair", &event_log(dir.path()), 1, false);
            let error = db.events().append(app_event(99, "failed")).unwrap_err();
            assert!(error.is_durability_failure(), "{label}: {error}");
            drop((failing, repair));
            assert_eq!(failed_site(&db), "frame_log.repair", "{label}");
            let refused = db.events().append(app_event(100, "refused")).unwrap_err();
            assert!(refused.is_durability_failure(), "{label}: {refused}");
        }
        let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
        assert!(db.durability_state().is_healthy(), "{label}");
        assert_eq!(app_event_tags(&db), acknowledged, "{label}");
        db.events().append(app_event(5, "resumed")).unwrap();
        acknowledged.push("resumed".into());
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
        assert_eq!(app_event_tags(&db), acknowledged, "{label}");
    }
}

fn audit_config(storage: &StorageMode) -> DbConfig {
    config(storage)
        .with_sync_outbox(false)
        .with_audit_events(true)
}

/// Record-audit events per record id.
fn audit_counts(db: &BicDb) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for stored in db.events().read(RECORD_AUDIT_STREAM).unwrap() {
        if let Some(id) = stored.event.payload["record_id"].as_str() {
            *counts.entry(id.to_string()).or_default() += 1;
        }
    }
    counts
}

fn reconcile_audit(db: &BicDb, committed: &[String], label: &str) {
    let counts = audit_counts(db);
    for id in committed {
        assert!(db.get("items", id).unwrap().is_some(), "{label}: {id} lost");
        assert_eq!(
            counts.get(id).copied(),
            Some(1),
            "{label}: {id} must have exactly one audit event"
        );
    }
    assert_eq!(
        counts.len(),
        committed.len(),
        "{label}: unexpected audit events"
    );
}

/// A full checkpoint that cuts the transaction log.
fn try_checkpoint(db: &mut BicDb, storage: &StorageMode) -> bicdb_core::Result<()> {
    match storage {
        StorageMode::EmbeddedMemory => {
            let plan = db.checkpoint_begin()?;
            db.checkpoint_write_segments(&plan)?;
            db.checkpoint_truncate_wal(plan.keep_from())?;
            Ok(())
        }
        _ => db.checkpoint_for_resume(),
    }
}

fn checkpoint(db: &mut BicDb, storage: &StorageMode) {
    try_checkpoint(db, storage).unwrap();
}

/// Audit events are an outbox behind a durable commit. When publishing them
/// fails after the commit is durable and visible, the commit is reported as
/// committed (a refusal would make the client retry and duplicate it), and
/// the database is fenced so no checkpoint on that handle cuts the outbox
/// out of the transaction log; the reopen republishes it. After a
/// checkpoint and further reopens, every committed row has exactly one
/// audit event.
#[test]
fn audit_events_behind_a_commit_survive_a_failed_event_append() {
    for storage in storage_modes() {
        for site in APPEND_FAILURES {
            let label = format!("{storage:?} {site}");
            let dir = tempfile::tempdir().unwrap();
            let mut committed = Vec::new();
            {
                let mut db = BicDb::open_with_config(dir.path(), audit_config(&storage)).unwrap();
                db.create_collection("items").unwrap();
                for batch in 0..3 {
                    let rows = ids(&format!("before{batch}"), 2);
                    commit_rows(&db, &rows).unwrap();
                    committed.extend(rows);
                }
                let before = len(&event_log(dir.path()));
                let fault = arm_io_failure(site, &event_log(dir.path()), 1, false);
                let deferred = ids("deferred", 2);
                commit_rows(&db, &deferred).unwrap();
                assert_eq!(fault.fired(), 1, "{label}");
                drop(fault);
                assert_eq!(failed_site(&db), "record_audit.publish", "{label}");
                assert_eq!(len(&event_log(dir.path())), before, "{label}");
                // The fenced handle refuses the checkpoint that would cut the
                // unpublished outbox out of the transaction log.
                assert!(try_checkpoint(&mut db, &storage).is_err(), "{label}");
                committed.extend(deferred);
            }
            {
                let mut db = BicDb::open_with_config(dir.path(), audit_config(&storage)).unwrap();
                assert!(db.durability_state().is_healthy(), "{label}");
                reconcile_audit(&db, &committed, &format!("{label} reopen"));
                for batch in 0..2 {
                    let rows = ids(&format!("after{batch}"), 2);
                    commit_rows(&db, &rows).unwrap();
                    committed.extend(rows);
                }
                checkpoint(&mut db, &storage);
                reconcile_audit(&db, &committed, &format!("{label} after checkpoint"));
            }
            for round in 0..2 {
                let db = BicDb::open_with_config(dir.path(), audit_config(&storage)).unwrap();
                reconcile_audit(&db, &committed, &format!("{label} final reopen {round}"));
            }
        }
    }
}

fn segment(dir: &Path) -> PathBuf {
    dir.join("segments").join("items.seg")
}

/// A record-segment delta append that fails during a checkpoint fails the
/// checkpoint and is cut back; the retried checkpoint appends on a clean
/// boundary and cuts the transaction log; every acknowledged row survives
/// the reopen. (Before the repair, the retried delta landed after the torn
/// prefix and the reopen lost every row behind it.)
#[test]
fn a_failed_segment_append_during_checkpoint_loses_no_acknowledged_row() {
    let storage = StorageMode::EmbeddedMemory;
    for site in APPEND_FAILURES {
        let dir = tempfile::tempdir().unwrap();
        let mut acknowledged = Vec::new();
        {
            let mut db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            db.create_collection("items").unwrap();
            let rows = ids("base", 4);
            commit_rows(&db, &rows).unwrap();
            acknowledged.extend(rows);
            checkpoint(&mut db, &storage);
            assert!(len(&segment(dir.path())) > 0);

            let rows = ids("delta", 6);
            commit_rows(&db, &rows).unwrap();
            acknowledged.extend(rows);
            let before = len(&segment(dir.path()));
            let fault = arm_io_failure(site, &segment(dir.path()), 1, false);
            let plan = db.checkpoint_begin().unwrap();
            assert!(db.checkpoint_write_segments(&plan).is_err(), "{site}");
            db.checkpoint_abort(plan);
            assert_eq!(fault.fired(), 1, "{site}");
            drop(fault);
            assert_eq!(len(&segment(dir.path())), before, "{site}");
            assert!(db.durability_state().is_healthy(), "{site}");

            let rows = ids("later", 3);
            commit_rows(&db, &rows).unwrap();
            acknowledged.extend(rows);
            checkpoint(&mut db, &storage);
        }
        for round in 0..2 {
            let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            for id in &acknowledged {
                assert!(
                    db.get("items", id).unwrap().is_some(),
                    "{site} reopen {round}: acknowledged {id} lost"
                );
            }
        }
    }
}

fn flip_byte(path: &Path, offset: u64) {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0_u8; 1];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0x5A]).unwrap();
}

/// Damage inside the first frame of a log that holds later valid frames is
/// not a torn tail. The open refuses with `Corruption` and leaves the file
/// unchanged; `salvage_frame_log` keeps the frames before the damage,
/// preserves the rest in a side file, and the database then opens.
#[test]
fn damage_in_the_middle_of_a_frame_log_refuses_the_open_until_salvaged() {
    for storage in storage_modes() {
        for log in ["sync", "event"] {
            let label = format!("{storage:?} {log}");
            let dir = tempfile::tempdir().unwrap();
            {
                let mut db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
                db.create_collection("items").unwrap();
                for batch in 0..3 {
                    commit_rows(&db, &ids(&format!("row{batch}"), 2)).unwrap();
                    db.events().append(app_event(batch, "e")).unwrap();
                }
            }
            let path = match log {
                "sync" => sync_log(dir.path()),
                _ => event_log(dir.path()),
            };
            // Byte 20 is inside the first frame's payload.
            flip_byte(&path, 20);
            let damaged = std::fs::read(&path).unwrap();
            let error = BicDb::open_with_config(dir.path(), config(&storage)).unwrap_err();
            assert!(
                matches!(&error, BicDbError::Corruption { message, .. } if message.contains("valid frame follows")),
                "{label}: {error}"
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                damaged,
                "{label}: file changed"
            );

            let salvage = salvage_frame_log(&path).unwrap();
            assert_eq!(salvage.kept_bytes, 0, "{label}");
            assert_eq!(salvage.moved_bytes, damaged.len() as u64, "{label}");
            assert_eq!(
                std::fs::read(salvage.moved_to.unwrap()).unwrap(),
                damaged,
                "{label}: the moved bytes are preserved"
            );
            let db = BicDb::open_with_config(dir.path(), config(&storage)).unwrap();
            for id in ids("row0", 2) {
                assert!(db.get("items", &id).unwrap().is_some(), "{label}");
            }
        }
    }
}

/// The replication readers of the live transaction log never truncate a
/// frame that is still being written. They used the recovery reader, which
/// took the in-flight frame for a torn tail and cut it off under the
/// writer, deleting a commit that the writer then acknowledged.
#[test]
fn replication_readers_never_truncate_the_live_transaction_log() {
    let storage = StorageMode::EmbeddedMemory;
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), config(&storage).with_sync_outbox(false)).unwrap();
    let mut db = db;
    db.create_collection("items").unwrap();
    for batch in 0..3 {
        commit_rows(&db, &ids(&format!("row{batch}"), 2)).unwrap();
    }
    let log = dir.path().join(DEFAULT_TRANSACTION_LOG);
    // A frame whose header is written and whose payload is not, yet.
    let mut in_flight = b"BICF\x01\x04\x00\x00".to_vec();
    in_flight.extend_from_slice(&64_u32.to_le_bytes());
    in_flight.extend_from_slice(&[0_u8; 4]);
    in_flight.extend_from_slice(b"{\"type\":");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(&in_flight)
        .unwrap();
    let before = std::fs::read(&log).unwrap();

    let status = db.replication_retention_status().unwrap();
    assert_eq!(status.retained_commits, 3);
    assert_eq!(
        std::fs::read(&log).unwrap(),
        before,
        "status truncated the live log"
    );
    let frames = db.export_replication_frames_since(0, 100).unwrap();
    assert_eq!(frames.len(), 3);
    assert_eq!(
        std::fs::read(&log).unwrap(),
        before,
        "export truncated the live log"
    );
}
