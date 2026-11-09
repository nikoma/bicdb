//! Outboxes behind a commit: change capture (`sync.log`) and record audit
//! events. Every committed transaction's entries are delivered exactly once,
//! across reopens, crashes and interrupted republication, and an aborted
//! transaction never produces one. Mid-file damage in any frame log fails
//! closed. See `docs/durability-failure-handling.md`, section 7.5.
//!
//! The crash tests run this test binary again as a child
//! (`OUTBOX_CHILD=<db>|<ledger>|<mode>|<phase>`), which aborts at the crash
//! point named by `BICDB_TEST_CRASH_AT`. Its ledger records only what it saw
//! acknowledged; the parent reconciles the reopened database against it.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use bicdb_core::{
    BicDb, BicDbError, DbConfig, DurabilityState, Record, StorageMode, DEFAULT_EVENTS_DIR,
    DEFAULT_EVENTS_SEGMENT, DEFAULT_TRANSACTION_LOG, RECORD_AUDIT_STREAM,
};
use bicdb_page::test_faults::arm_io_failure;
use serde_json::json;

fn storage_modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

fn mode_name(storage: &StorageMode) -> &'static str {
    match storage {
        StorageMode::EmbeddedMemory => "embedded",
        _ => "paged",
    }
}

fn mode_from(name: &str) -> StorageMode {
    match name {
        "embedded" => StorageMode::EmbeddedMemory,
        _ => StorageMode::ServerPaged,
    }
}

fn config(storage: &StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(storage.clone())
        .with_sync_outbox(true)
        .with_audit_events(true)
}

fn sync_log(dir: &Path) -> PathBuf {
    dir.join("sync.log")
}

fn event_log(dir: &Path) -> PathBuf {
    dir.join(DEFAULT_EVENTS_DIR).join(DEFAULT_EVENTS_SEGMENT)
}

fn len(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

fn commit_rows(db: &BicDb, ids: &[String]) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    for id in ids {
        tx.insert("items", Record::new(id).with_metadata(json!({ "id": id })))?;
    }
    tx.commit()
}

fn ids(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index}"))
        .collect()
}

fn failed_site(db: &BicDb) -> Option<String> {
    match db.durability_state() {
        DurabilityState::Healthy => None,
        DurabilityState::WriteFailed(failure) => Some(failure.site),
    }
}

fn rows(db: &BicDb) -> BTreeSet<String> {
    db.scan_collection("items")
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect()
}

/// Pending change-capture ops per record id (a multiset).
fn pending_ops(db: &BicDb) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    let mut op_ids = BTreeSet::new();
    for op in db.pending_sync_ops() {
        assert!(op_ids.insert(op.op_id), "op {} pending twice", op.op_id);
        *counts.entry(op.record_id).or_default() += 1;
    }
    counts
}

/// Record-audit events per record id.
fn audit_events(db: &BicDb) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for stored in db.events().read(RECORD_AUDIT_STREAM).unwrap() {
        if let Some(id) = stored.event.payload["record_id"].as_str() {
            *counts.entry(id.to_string()).or_default() += 1;
        }
    }
    counts
}

/// Exactly one change-capture op (pending, or acknowledged by the consumer)
/// and exactly one audit event per committed row; none for anything else.
fn reconcile(db: &BicDb, committed: &BTreeSet<String>, acked: &BTreeSet<String>, label: &str) {
    let present = rows(db);
    assert_eq!(&present, committed, "{label}: committed rows");
    let pending = pending_ops(db);
    for (id, count) in &pending {
        assert_eq!(*count, 1, "{label}: {id} has {count} pending ops");
        assert!(committed.contains(id), "{label}: op for uncommitted {id}");
        assert!(
            !acked.contains(id),
            "{label}: acknowledged op of {id} came back"
        );
    }
    for id in committed {
        assert!(
            pending.contains_key(id) || acked.contains(id),
            "{label}: committed {id} has no change-capture op"
        );
    }
    let audit = audit_events(db);
    for id in committed {
        assert_eq!(
            audit.get(id).copied(),
            Some(1),
            "{label}: audit events of {id}"
        );
    }
    assert_eq!(
        audit.len(),
        committed.len(),
        "{label}: audit events of uncommitted rows"
    );
}

fn open(dir: &Path, storage: &StorageMode) -> BicDb {
    BicDb::open_with_config(dir, config(storage)).unwrap()
}

/// Reopen twice; the second reopen appends nothing to either outbox.
fn reopen_and_reconcile(
    dir: &Path,
    storage: &StorageMode,
    committed: &BTreeSet<String>,
    acked: &BTreeSet<String>,
    label: &str,
) {
    {
        let db = open(dir, storage);
        reconcile(&db, committed, acked, &format!("{label} reopen 1"));
    }
    let lengths = (len(&sync_log(dir)), len(&event_log(dir)));
    {
        let db = open(dir, storage);
        reconcile(&db, committed, acked, &format!("{label} reopen 2"));
    }
    assert_eq!(
        (len(&sync_log(dir)), len(&event_log(dir))),
        lengths,
        "{label}: a second reopen republished again"
    );
}

// ---------------------------------------------------------------------------
// Aborted transactions never produce change-capture ops.

/// Commits that end without a durable decision: a failed decision write
/// (08007, nothing reached the disk), a commit withdrawn after a backup-gate
/// timeout, a unique-key conflict, and a rollback. None of them may leave a
/// change-capture op or an audit event, in the running database or after a
/// reopen. Before this change, ops were appended to `sync.log` ahead of the
/// decision, and the first two left ops of commits that never happened.
#[test]
fn aborted_transactions_never_produce_change_capture_ops() {
    for storage in storage_modes() {
        for case in [
            "decision_write_failed",
            "withdrawn",
            "unique_conflict",
            "rollback",
        ] {
            let label = format!("{} {case}", mode_name(&storage));
            let dir = tempfile::tempdir().unwrap();
            let mut committed = BTreeSet::new();
            {
                let mut db = open(dir.path(), &storage);
                db.create_collection("items").unwrap();
                if case == "unique_conflict" {
                    db.create_index(bicdb_core::IndexDefinition {
                        nulls_not_distinct: false,
                        name: "items-by-id".to_string(),
                        collection: "items".to_string(),
                        fields: vec![bicdb_core::IndexField::MetadataPath(vec!["id".to_string()])],
                        kind: bicdb_core::IndexKind::BTree,
                        unique: true,
                        predicate: None,
                        exclusion: None,
                    })
                    .unwrap();
                }
                let before = ids("before", 3);
                commit_rows(&db, &before).unwrap();
                committed.extend(before);
                let aborted = ids("aborted", 3);
                match case {
                    "decision_write_failed" => {
                        let fault = arm_io_failure("transaction_log.write", dir.path(), 1, false);
                        let error = commit_rows(&db, &aborted).unwrap_err();
                        assert_eq!(fault.fired(), 1, "{label}");
                        assert!(
                            matches!(error, BicDbError::CommitOutcomeUnknown { .. }),
                            "{label}: {error}"
                        );
                    }
                    "withdrawn" => {
                        let fault = arm_io_failure(
                            "backup_gate.timeout",
                            &dir.path().join(DEFAULT_TRANSACTION_LOG),
                            1,
                            false,
                        );
                        let error = commit_rows(&db, &aborted).unwrap_err();
                        assert_eq!(fault.fired(), 1, "{label}");
                        assert!(matches!(error, BicDbError::Backup(_)), "{label}: {error}");
                    }
                    "unique_conflict" => {
                        // The conflict is reported by the insert or by the
                        // commit; either way nothing commits.
                        let mut tx = db.begin_transaction().unwrap();
                        let inserted = tx.insert(
                            "items",
                            Record::new("aborted-0").with_metadata(json!({"id": "before-0"})),
                        );
                        if inserted.is_ok() {
                            assert!(tx.commit().is_err(), "{label}");
                        }
                    }
                    _ => {
                        let mut tx = db.begin_transaction().unwrap();
                        for id in &aborted {
                            tx.insert("items", Record::new(id)).unwrap();
                        }
                        tx.rollback().unwrap();
                    }
                }
                // Visible to a consumer right now: only committed ops.
                let pending = pending_ops(&db);
                assert!(
                    pending.keys().all(|id| committed.contains(id)),
                    "{label}: consumer sees ops of an aborted commit: {pending:?}"
                );
                if case == "withdrawn" || case == "unique_conflict" || case == "rollback" {
                    let after = ids("after", 2);
                    commit_rows(&db, &after).unwrap();
                    committed.extend(after);
                }
            }
            reopen_and_reconcile(dir.path(), &storage, &committed, &BTreeSet::new(), &label);
        }
    }
}

// ---------------------------------------------------------------------------
// A durable commit stays committed when an outbox publication fails.

/// The commit is durable before its outboxes are published. A failure while
/// publishing the change-capture ops, the audit events, or the audit
/// publication marker never turns it into a failure: the client sees success
/// (or 08007 when #55's rules apply), the database is fenced, and the reopen
/// publishes everything exactly once.
#[test]
fn a_durable_commit_stays_committed_when_its_outbox_publication_fails() {
    for storage in storage_modes() {
        for (site, scope) in [
            ("frame_log.write", "sync"),
            ("frame_log.short_write", "sync"),
            ("frame_log.sync", "sync"),
            ("frame_log.write", "events"),
            ("frame_log.short_write", "events"),
            ("transaction_log.write", "marker"),
        ] {
            let label = format!("{} {site} {scope}", mode_name(&storage));
            let dir = tempfile::tempdir().unwrap();
            let mut committed = BTreeSet::new();
            {
                let mut db = open(dir.path(), &storage);
                db.create_collection("items").unwrap();
                let before = ids("before", 2);
                commit_rows(&db, &before).unwrap();
                committed.extend(before);
                // The marker is the transaction log's second write of the
                // commit (the first is the commit batch itself).
                let (path, nth) = match scope {
                    "sync" => (sync_log(dir.path()), 1),
                    "events" => (event_log(dir.path()), 1),
                    _ => (dir.path().join(DEFAULT_TRANSACTION_LOG), 2),
                };
                let fault = arm_io_failure(site, &path, nth, false);
                let durable = ids("durable", 3);
                match commit_rows(&db, &durable) {
                    Ok(()) => {}
                    Err(error @ BicDbError::CommitOutcomeUnknown { .. }) => {
                        // Allowed by #55 when the failure lands before the
                        // commit became visible; recovery keeps it.
                        let _ = error;
                    }
                    Err(error) => panic!("{label}: a durable commit failed: {error}"),
                }
                assert_eq!(fault.fired(), 1, "{label}");
                drop(fault);
                assert!(failed_site(&db).is_some(), "{label}: not fenced");
                committed.extend(durable);
            }
            reopen_and_reconcile(dir.path(), &storage, &committed, &BTreeSet::new(), &label);
        }
    }
}

// ---------------------------------------------------------------------------
// Republication is exactly once.

/// A commit whose change-capture publication failed (so its ops are only in
/// the transaction log), then reopens whose republication is interrupted by
/// a failed append, then clean reopens. Each op is delivered once; an
/// acknowledged op never comes back, also after a compaction.
#[test]
fn change_capture_republication_is_exactly_once_across_interrupted_reopens() {
    for storage in storage_modes() {
        let label = mode_name(&storage).to_string();
        let dir = tempfile::tempdir().unwrap();
        let mut committed = BTreeSet::new();
        {
            let mut db = open(dir.path(), &storage);
            db.create_collection("items").unwrap();
            let before = ids("before", 2);
            commit_rows(&db, &before).unwrap();
            committed.extend(before);
            let fault = arm_io_failure("frame_log.write", &sync_log(dir.path()), 1, false);
            let durable = ids("durable", 4);
            commit_rows(&db, &durable).unwrap();
            assert_eq!(fault.fired(), 1);
            assert_eq!(
                failed_site(&db).as_deref(),
                Some("sync_log.publish"),
                "{label}"
            );
            committed.extend(durable);
        }
        // Republication fails: the open fails and appends nothing.
        for site in ["frame_log.write", "frame_log.short_write", "frame_log.sync"] {
            let before = len(&sync_log(dir.path()));
            let fault = arm_io_failure(site, &sync_log(dir.path()), 1, false);
            assert!(
                BicDb::open_with_config(dir.path(), config(&storage)).is_err(),
                "{label} {site}"
            );
            assert_eq!(fault.fired(), 1, "{label} {site}");
            assert_eq!(len(&sync_log(dir.path())), before, "{label} {site}");
        }
        reopen_and_reconcile(dir.path(), &storage, &committed, &BTreeSet::new(), &label);

        // Acknowledge some ops, then compact: neither brings them back.
        let mut acked = BTreeSet::new();
        {
            let mut db = open(dir.path(), &storage);
            let ops = db.pending_sync_ops();
            let first: Vec<_> = ops.iter().take(3).map(|op| op.op_id).collect();
            acked.extend(ops.iter().take(3).map(|op| op.record_id.clone()));
            db.mark_synced(&first).unwrap();
        }
        reopen_and_reconcile(
            dir.path(),
            &storage,
            &committed,
            &acked,
            &format!("{label} acked"),
        );
        {
            let mut db = open(dir.path(), &storage);
            db.compact().unwrap();
        }
        reopen_and_reconcile(
            dir.path(),
            &storage,
            &committed,
            &acked,
            &format!("{label} compacted"),
        );
    }
}

/// Audit events whose publication failed, then reopens whose republication
/// fails after some events (`frame_log.write` on the second event) or after
/// all events but before the publication marker (`frame_log.write` on the
/// transaction log at open): exactly one audit event per committed row.
#[test]
fn audit_republication_is_exactly_once_across_interrupted_reopens() {
    for storage in storage_modes() {
        let label = mode_name(&storage).to_string();
        let dir = tempfile::tempdir().unwrap();
        let mut committed = BTreeSet::new();
        {
            let mut db = open(dir.path(), &storage);
            db.create_collection("items").unwrap();
            let before = ids("before", 2);
            commit_rows(&db, &before).unwrap();
            committed.extend(before);
            let fault = arm_io_failure("frame_log.write", &event_log(dir.path()), 1, false);
            let durable = ids("durable", 4);
            commit_rows(&db, &durable).unwrap();
            assert_eq!(fault.fired(), 1);
            assert_eq!(
                failed_site(&db).as_deref(),
                Some("record_audit.publish"),
                "{label}"
            );
            committed.extend(durable);
        }
        // After one republished event.
        let fault = arm_io_failure("frame_log.write", &event_log(dir.path()), 2, false);
        assert!(
            BicDb::open_with_config(dir.path(), config(&storage)).is_err(),
            "{label}"
        );
        assert_eq!(fault.fired(), 1);
        drop(fault);
        // After every event, before the marker.
        let fault = arm_io_failure(
            "frame_log.write",
            &dir.path().join(DEFAULT_TRANSACTION_LOG),
            1,
            false,
        );
        assert!(
            BicDb::open_with_config(dir.path(), config(&storage)).is_err(),
            "{label}"
        );
        assert_eq!(fault.fired(), 1);
        drop(fault);
        reopen_and_reconcile(dir.path(), &storage, &committed, &BTreeSet::new(), &label);
    }
}

// ---------------------------------------------------------------------------
// Crashes at every boundary, out of process.

const CHILD_COMMITS: usize = 24;

fn child_rows(commit: usize) -> Vec<String> {
    vec![format!("c{commit:02}-a"), format!("c{commit:02}-b")]
}

fn append_line(path: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}

fn read_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .map(|text| text.lines().map(str::to_string).collect())
        .unwrap_or_default()
}

/// The child's workload: one commit of two rows at a time, each recorded in
/// the ledger after `commit` returned; every 5th commit acknowledges the
/// oldest pending op (recorded after `mark_synced` returned); a checkpoint
/// (which cuts the transaction log) every 6th commit, and a compaction at
/// commit 13.
fn child_workload(db_dir: &Path, ledger: &Path, storage: &StorageMode) {
    let mut db = open(db_dir, storage);
    if db.collections().iter().all(|meta| meta.name != "items") {
        db.create_collection("items").unwrap();
    }
    for commit in 0..CHILD_COMMITS {
        append_line(&ledger.join("attempt"), &commit.to_string());
        commit_rows(&db, &child_rows(commit)).unwrap();
        append_line(&ledger.join("committed"), &commit.to_string());
        if commit % 5 == 4 {
            if let Some(op) = db.pending_sync_ops().into_iter().next() {
                db.mark_synced(&[op.op_id]).unwrap();
                append_line(&ledger.join("acked"), &op.record_id);
            }
        }
        if commit % 6 == 5 {
            match storage {
                StorageMode::EmbeddedMemory => {
                    let plan = db.checkpoint_begin().unwrap();
                    db.checkpoint_write_segments(&plan).unwrap();
                    db.checkpoint_truncate_wal(plan.keep_from()).unwrap();
                }
                _ => db.checkpoint_for_resume().unwrap(),
            }
        }
        if commit == 13 {
            db.compact().unwrap();
        }
    }
}

/// Entry point of the child process; a no-op in the ordinary test run.
#[test]
fn outbox_child_entry() {
    let Ok(spec) = std::env::var("OUTBOX_CHILD") else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let (db_dir, ledger, storage, phase) = (
        Path::new(parts[0]),
        Path::new(parts[1]),
        mode_from(parts[2]),
        parts[3],
    );
    match phase {
        "workload" => child_workload(db_dir, ledger, &storage),
        // Open (republishing) and close; a crash point may abort it.
        _ => drop(open(db_dir, &storage)),
    }
}

fn run_child(
    db_dir: &Path,
    ledger: &Path,
    storage: &StorageMode,
    phase: &str,
    crash_at: &str,
) -> bool {
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "outbox_child_entry",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(
            "OUTBOX_CHILD",
            format!(
                "{}|{}|{}|{phase}",
                db_dir.display(),
                ledger.display(),
                mode_name(storage)
            ),
        )
        .env("BICDB_TEST_CRASH_AT", crash_at)
        .output()
        .unwrap();
    status.status.success()
}

/// Every boundary of a commit's outboxes, and of their republication at
/// open, killed with `abort()`. Reconciled against the child's ledger: every
/// acknowledged commit's rows are present; the one commit in flight at the
/// crash is either wholly present or wholly absent; every present row has
/// exactly one change-capture op (or was acknowledged, and stays so) and
/// exactly one audit event; nothing exists for an absent row; and a second
/// reopen changes nothing.
#[test]
fn crashes_at_every_outbox_boundary_deliver_each_committed_entry_exactly_once() {
    let workload_points = [
        "commit.before_decision:9",
        "commit.after_txlog_durable:9",
        "commit.after_page_apply:9",
        "commit.after_sync_publish:9",
        "commit.after_txlog_durable:15",
        "txlog_cut.after_floor:2",
    ];
    // A crash after the decision leaves the outboxes to recovery; these
    // crash that recovery itself, then recover again.
    let open_points = [
        "open.audit_republish.after_event:1",
        "open.after_sync_republish:1",
    ];
    let mut cases = Vec::new();
    for point in workload_points {
        cases.push((point, None));
    }
    for point in open_points {
        cases.push(("commit.after_txlog_durable:9", Some(point)));
    }
    let mut failures = Vec::new();
    for storage in storage_modes() {
        for (workload_crash, open_crash) in &cases {
            if workload_crash.starts_with("commit.after_page_apply")
                && storage == StorageMode::EmbeddedMemory
            {
                continue; // paged only
            }
            let label = format!(
                "{} {workload_crash} {}",
                mode_name(&storage),
                open_crash.unwrap_or("-")
            );
            let root = tempfile::tempdir().unwrap();
            let (db_dir, ledger) = (root.path().join("db"), root.path().join("ledger"));
            std::fs::create_dir_all(&ledger).unwrap();
            let finished = run_child(&db_dir, &ledger, &storage, "workload", workload_crash);
            if finished {
                failures.push(format!("{label}: the crash point was never reached"));
                continue;
            }
            if let Some(point) = open_crash {
                if run_child(&db_dir, &ledger, &storage, "open", point) {
                    failures.push(format!("{label}: the open crash point was never reached"));
                    continue;
                }
            }
            let acknowledged: BTreeSet<usize> = read_lines(&ledger.join("committed"))
                .iter()
                .map(|line| line.parse().unwrap())
                .collect();
            let in_flight: usize = read_lines(&ledger.join("attempt"))
                .last()
                .unwrap()
                .parse()
                .unwrap();
            let acked: BTreeSet<String> = read_lines(&ledger.join("acked")).into_iter().collect();
            let result = std::panic::catch_unwind(|| {
                let db = open(&db_dir, &storage);
                let present = rows(&db);
                drop(db);
                let mut committed = BTreeSet::new();
                for commit in &acknowledged {
                    committed.extend(child_rows(*commit));
                }
                let flight = child_rows(in_flight);
                let flight_present = flight.iter().filter(|id| present.contains(*id)).count();
                assert!(
                    flight_present == 0 || flight_present == flight.len(),
                    "in-flight commit partially present"
                );
                if flight_present > 0 && !acknowledged.contains(&in_flight) {
                    committed.extend(flight);
                }
                if workload_crash.starts_with("commit.before_decision") {
                    assert_eq!(
                        flight_present, 0,
                        "a commit crashed before its decision is present"
                    );
                }
                reopen_and_reconcile(&db_dir, &storage, &committed, &acked, &label);
            });
            if let Err(panic) = result {
                let message = panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                failures.push(format!("{label}: {message}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failing cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Mid-file damage fails closed in every frame log.

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn frame_count(path: &Path) -> usize {
    std::fs::read(path)
        .unwrap_or_default()
        .windows(5)
        .filter(|window| *window == b"BICF\x01")
        .count()
}

/// For each frame log (the transaction log included), a crash image of a
/// running database with damage inside the log's first frame and valid frames
/// after it: the open fails with `Corruption` and leaves the file unchanged.
#[test]
fn mid_file_damage_fails_closed_in_every_frame_log() {
    for storage in storage_modes() {
        let source = tempfile::tempdir().unwrap();
        let db = open(source.path(), &storage);
        // Created through a transaction-free call so the transaction log
        // starts with commit frames.
        let mut db = db;
        db.create_collection("items").unwrap();
        for batch in 0..4 {
            commit_rows(&db, &ids(&format!("row{batch}"), 2)).unwrap();
        }
        if storage == StorageMode::EmbeddedMemory {
            // Two checkpoint deltas: the segment holds frames from both.
            for _ in 0..2 {
                let plan = db.checkpoint_begin().unwrap();
                db.checkpoint_write_segments(&plan).unwrap();
                db.checkpoint_truncate_wal(plan.keep_from()).unwrap();
                commit_rows(
                    &db,
                    &ids(&format!("more{}", len(&sync_log(source.path()))), 2),
                )
                .unwrap();
            }
        }
        let mut logs = vec![
            DEFAULT_TRANSACTION_LOG.to_string(),
            "sync.log".to_string(),
            format!("{DEFAULT_EVENTS_DIR}/{DEFAULT_EVENTS_SEGMENT}"),
        ];
        if storage == StorageMode::EmbeddedMemory {
            logs.push("segments/items.seg".to_string());
        }
        for log in logs {
            let label = format!("{} {log}", mode_name(&storage));
            // A crash image: the files as a running database left them.
            let image = tempfile::tempdir().unwrap();
            copy_dir(source.path(), image.path());
            let _ = std::fs::remove_file(image.path().join("store.lock"));
            let path = image.path().join(&log);
            assert!(
                frame_count(&path) >= 2,
                "{label}: needs two frames, has {}",
                frame_count(&path)
            );
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[20] ^= 0x5A;
            std::fs::write(&path, &bytes).unwrap();
            let error = BicDb::open_with_config(image.path(), config(&storage)).unwrap_err();
            assert!(
                matches!(&error, BicDbError::Corruption { message, .. } if message.contains("valid frame follows")),
                "{label}: {error}"
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                bytes,
                "{label}: file changed"
            );
        }
        drop(db);
    }
}
