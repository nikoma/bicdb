use bicdb_core::{BicDb, DbConfig, Record, TransactionOutcome, TransactionWriteRollback};
use serde_json::json;

fn row(value: i64) -> Record {
    Record::new("one").with_metadata(json!({"value":value}))
}

#[test]
fn rollback_receipts_do_not_resurrect_when_write_slots_are_reused() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row(0)).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.update("rows", row(1)).unwrap();
    let first = tx.observe_record_write("rows", "one").unwrap();
    let mark = tx.rollback_mark();
    tx.update("rows", row(2)).unwrap();
    let discarded = tx.observe_record_write("rows", "one").unwrap();
    tx.rollback_to_mark(mark).unwrap();
    let boundary = TransactionWriteRollback {
        retained_write_len: mark.write_len(),
        receipt_limit: discarded.id(),
    };
    assert_eq!(discarded.rollback_boundary(), Some(boundary));
    assert_eq!(first.rollback_boundary(), None);
    tx.update("rows", row(3)).unwrap();
    let replacement = tx.observe_record_write("rows", "one").unwrap();
    assert_eq!(replacement.write_len(), discarded.write_len());
    assert!(replacement.id() > discarded.id());
    assert!(first
        .transaction()
        .same_transaction(replacement.transaction()));
    let sequence = tx.commit_with_sequence().unwrap();
    assert_eq!(
        first.transaction().outcome(),
        TransactionOutcome::Committed { sequence }
    );
    assert_eq!(
        replacement.transaction().outcome(),
        TransactionOutcome::Committed { sequence }
    );
    assert_eq!(discarded.rollback_boundary(), Some(boundary));
    assert_eq!(db.get("rows", "one").unwrap().unwrap().metadata["value"], 3);
}

#[test]
fn receipts_follow_nested_rollback_prefixes_and_late_registration() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let outer = tx.rollback_mark();
    tx.insert("rows", row(1)).unwrap();
    let inner = tx.rollback_mark();
    let earlier_write_registered_late = tx.observe_record_write("rows", "one").unwrap();
    tx.update("rows", row(2)).unwrap();
    let later = tx.observe_record_write("rows", "one").unwrap();
    tx.rollback_to_mark(inner).unwrap();
    assert_eq!(earlier_write_registered_late.rollback_boundary(), None);
    let later_boundary = later.rollback_boundary().unwrap();
    tx.rollback_to_mark(outer).unwrap();
    assert_eq!(
        earlier_write_registered_late
            .rollback_boundary()
            .unwrap()
            .retained_write_len,
        0
    );
    // The first invalidation remains a historical boundary, even as its
    // retained parent prefix is subsequently rolled back.
    assert_eq!(later.rollback_boundary(), Some(later_boundary));
    assert!(tx.observe_record_write("rows", "one").is_err());
    tx.rollback().unwrap();
    assert_eq!(later.transaction().outcome(), TransactionOutcome::Aborted);
    assert!(db.get("rows", "one").unwrap().is_none());
}

#[test]
fn explicit_rollback_and_abandonment_survive_the_transaction_object() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    let first = {
        let mut tx = db.begin_transaction().unwrap();
        let observer = tx.observe_outcome();
        assert_eq!(observer.outcome(), TransactionOutcome::Pending);
        assert!(observer.same_transaction(&tx.observe_outcome()));
        assert!(tx.observe_record_write("rows", "missing").is_err());
        assert_eq!(tx.write_len(), 0);
        assert_eq!(tx.lock_len(), 0);
        tx.insert("rows", row(1)).unwrap();
        observer
    };
    assert_eq!(first.outcome(), TransactionOutcome::Aborted);
    let tx = db.begin_transaction().unwrap();
    let second = tx.observe_outcome();
    assert!(!first.same_transaction(&second));
    assert_ne!(first.transaction_id(), second.transaction_id());
    tx.rollback().unwrap();
    assert_eq!(second.outcome(), TransactionOutcome::Aborted);
    assert!(db.get("rows", "one").unwrap().is_none());
    drop(db);
    assert_eq!(first.outcome(), TransactionOutcome::Aborted);
}

#[test]
fn write_truncation_and_buffered_commit_publish_the_same_receipt_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(true)).unwrap();
    db.create_collection("rows").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("rows", row(1)).unwrap();
    let discarded = tx.observe_record_write("rows", "one").unwrap();
    tx.truncate_writes(0).unwrap();
    assert_eq!(discarded.rollback_boundary().unwrap().retained_write_len, 0);
    tx.insert("rows", row(2)).unwrap();
    let kept = tx.observe_record_write("rows", "one").unwrap();
    let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
    db.tx_log_handle().write_durable(sequence).unwrap();
    tx.finalize_commit_admission().unwrap();
    assert_eq!(
        kept.transaction().outcome(),
        TransactionOutcome::Committed { sequence }
    );
    assert_eq!(
        tx.observe_outcome().outcome(),
        TransactionOutcome::Committed { sequence }
    );
    assert!(discarded.rollback_boundary().is_some());
    drop(tx);
    drop(db);
    let db = BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(true)).unwrap();
    assert_eq!(db.get("rows", "one").unwrap().unwrap().metadata["value"], 2);
    assert_eq!(
        kept.transaction().outcome(),
        TransactionOutcome::Committed { sequence }
    );
}

#[test]
fn outcome_observers_can_cross_threads_without_retaining_database_pointers() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let tx = db.begin_transaction().unwrap();
    let observer = tx.observe_outcome();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        assert_eq!(observer.outcome(), TransactionOutcome::Pending);
        ready_tx.send(()).unwrap();
        done_rx.recv().unwrap();
        observer.outcome()
    });
    ready_rx.recv().unwrap();
    tx.rollback().unwrap();
    drop(db);
    done_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), TransactionOutcome::Aborted);
}

#[test]
fn predecision_validation_failure_remains_pending_until_retry_or_drop() {
    use bicdb_core::{IndexDefinition, IndexField, IndexKind};
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    db.create_index(IndexDefinition {
        name: "unique_values".into(),
        collection: "rows".into(),
        fields: vec![IndexField::MetadataPath(vec!["value".into()])],
        kind: IndexKind::BTree,
        unique: true,
        nulls_not_distinct: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.insert("rows", row(1)).unwrap();
    for retry in [false, true] {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("rows", Record::new("two").with_metadata(json!({"value":1})))
            .unwrap();
        let receipt = tx.observe_record_write("rows", "two").unwrap();
        assert!(db.commit_buffered_transaction(&mut tx).is_err());
        assert_eq!(receipt.transaction().outcome(), TransactionOutcome::Pending);
        assert_eq!(receipt.rollback_boundary(), None);
        db.check_recovery_state().unwrap();
        if retry {
            tx.truncate_writes(0).unwrap();
            tx.insert("rows", Record::new("two").with_metadata(json!({"value":2})))
                .unwrap();
            let sequence = tx.commit_with_sequence().unwrap();
            assert_eq!(
                receipt.transaction().outcome(),
                TransactionOutcome::Committed { sequence }
            );
            assert!(receipt.rollback_boundary().is_some());
        } else {
            drop(tx);
            assert_eq!(receipt.transaction().outcome(), TransactionOutcome::Aborted);
            assert!(db.get("rows", "two").unwrap().is_none());
        }
    }
    assert_eq!(db.get("rows", "two").unwrap().unwrap().metadata["value"], 2);
}

#[cfg(target_os = "linux")]
#[test]
fn failed_wal_append_reports_recovery_required_instead_of_abort() {
    const CHILD: &str = "BICDB_RECEIPT_WAL_APPEND_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // File-size limits and signal dispositions are process-wide. Keep the
        // kernel-enforced I/O failure out of the concurrently running harness.
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "failed_wal_append_reports_recovery_required_instead_of_abort",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "isolated WAL append receipt audit failed");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(bicdb_core::StorageMode::EmbeddedMemory),
    )
    .unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row(1)).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.update(
        "rows",
        Record::new("one").with_metadata(json!({"payload":"x".repeat(1_048_576)})),
    )
    .unwrap();
    let receipt = tx.observe_record_write("rows", "one").unwrap();
    let durable_before = db.tx_log_handle().stats().written_seq;
    let before = std::fs::metadata(root.path().join("transactions.log"))
        .unwrap()
        .len();
    let mut original: libc::rlimit = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_FSIZE, &mut original) },
        0
    );
    let signal = unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_IGN) };
    assert_ne!(signal, libc::SIG_ERR);
    // Leave room for small pre-decision control files. The much larger redo
    // must fail in WAL append, not during commit-sequence reservation.
    let limited = libc::rlimit {
        rlim_cur: before + 64 * 1024,
        rlim_max: original.rlim_max,
    };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &limited) }, 0);
    let result = db.commit_buffered_transaction(&mut tx);
    // Restore the child's resources before inspecting the error or closing
    // the database. Nothing modifies the parent process's limits or signals.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &original) }, 0);
    assert_ne!(
        unsafe { libc::signal(libc::SIGXFSZ, signal) },
        libc::SIG_ERR
    );
    assert!(result.is_err());
    assert_eq!(
        db.tx_log_handle().stats().written_seq,
        durable_before,
        "the failure must precede durable WAL publication"
    );
    assert!(
        db.check_recovery_state().is_err(),
        "commit result: {result:?}; outcome: {:?}",
        receipt.transaction().outcome()
    );
    assert!(matches!(
        receipt.transaction().outcome(),
        TransactionOutcome::RecoveryRequired { .. }
    ));
    drop(tx);
    assert!(matches!(
        receipt.transaction().outcome(),
        TransactionOutcome::RecoveryRequired { .. }
    ));
    drop(db);
    assert!(matches!(
        receipt.transaction().outcome(),
        TransactionOutcome::RecoveryRequired { .. }
    ));
}

#[test]
fn write_tags_follow_native_savepoints_without_cross_transaction_state() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row(0)).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    assert!(tx.annotate_record_write("rows", "one", 1, 7, 1).is_err());
    assert_eq!(tx.record_write_tag(1, 7).unwrap(), None);
    tx.update("rows", row(1)).unwrap();
    let first = tx.annotate_record_write("rows", "one", 1, 7, 11).unwrap();
    let outer = tx.rollback_mark();
    tx.update("rows", row(2)).unwrap();
    tx.annotate_record_write("rows", "one", 1, 7, 22).unwrap();
    tx.annotate_record_write("rows", "one", 2, 7, 99).unwrap();
    let inner = tx.rollback_mark();
    tx.update("rows", row(3)).unwrap();
    tx.annotate_record_write("rows", "one", 1, 7, 33).unwrap();
    assert_eq!(tx.record_write_tag(1, 7).unwrap(), Some(33));
    tx.rollback_to_mark(inner).unwrap();
    assert_eq!(tx.record_write_tag(1, 7).unwrap(), Some(22));
    tx.rollback_to_mark(outer).unwrap();
    assert_eq!(tx.record_write_tag(1, 7).unwrap(), Some(11));
    assert_eq!(tx.record_write_tag(2, 7).unwrap(), None);
    tx.update("rows", row(4)).unwrap();
    let replacement = tx.annotate_record_write("rows", "one", 1, 7, 44).unwrap();
    assert!(replacement.id() > first.id());
    assert_eq!(tx.record_write_tag(1, 7).unwrap(), Some(44));
    let other = db.begin_transaction().unwrap();
    assert_eq!(other.record_write_tag(1, 7).unwrap(), None);
    drop(other);
    let seq = db.commit_buffered_transaction(&mut tx).unwrap();
    db.tx_log_handle().write_durable(seq).unwrap();
    tx.finalize_commit_admission().unwrap();
    assert!(tx.record_write_tag(1, 7).is_err());
    let next = db.begin_transaction().unwrap();
    assert_eq!(next.record_write_tag(1, 7).unwrap(), None);
}

#[test]
fn late_write_tag_registration_survives_rollback_to_its_existing_write() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("rows", row(1)).unwrap();
    let mark = tx.rollback_mark();
    tx.annotate_record_write("rows", "one", 1, 7, 11).unwrap();
    tx.annotate_record_write("rows", "one", 1, 7, 12).unwrap();
    tx.rollback_to_mark(mark).unwrap();
    assert_eq!(tx.record_write_tag(1, 7).unwrap(), Some(12));
    tx.rollback().unwrap();
}
