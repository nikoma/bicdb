//! Genuine fsynced services with faults only after actual Commit persistence.
//! These controls establish uncertainty/no ACK, not canonical abort recovery.
use super::*;
use crate::TransactionOutcome;

fn kept_fixture() -> ActualNativeRf3Fixture {
    let mut fixture = ActualNativeRf3Fixture::new();
    fixture.root.disable_cleanup(true);
    eprintln!(
        "native Commit uncertainty case root: {}",
        fixture.root.path().display()
    );
    fixture
}
fn cleanup(fixture: ActualNativeRf3Fixture) {
    let root = fixture.root.path().to_path_buf();
    drop(fixture);
    std::fs::remove_dir_all(root).unwrap();
}
fn range(fixture: &ActualNativeRf3Fixture) -> RangeDescriptor {
    fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone()
}
fn no_local_effects(
    fixture: &ActualNativeRf3Fixture,
    range: &RangeDescriptor,
    key: &str,
    local_b: u64,
) {
    for (id, db) in &fixture.databases {
        let db = db.read();
        assert!(db.get("items", key).unwrap().is_none(), "{id}");
        let native = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(native.range_index(), 0, "{id}");
        if id == &range.leader {
            assert_eq!(native.actual_local_visibility_sequence(), local_b);
        }
    }
}
fn unknown_without_local_sequence(error: &BicDbError, tx: u64) {
    assert!(
        matches!(error, BicDbError::CommitOutcomeUnknown {
        sequence: None, transaction_id: Some(id), ..
    } if *id == tx),
        "{error}"
    );
}

#[test]
fn native_commit_uncertainty_lost_genuine_durable_commit_replies_never_abort_or_ack() {
    let fixture = kept_fixture();
    let range = range(&fixture);
    let key = "native-lost-durable-commit-replies";
    let before = fixture.databases[&range.leader]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .actual_local_visibility_sequence();
    fixture.transport.lose_commit_replies_after_durability(true);
    let db = fixture.databases[&range.leader].write();
    let before_wal = db.tx_log_handle().stats();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new(key)).unwrap();
    let observer = tx.observe_outcome();
    let error = tx.commit().unwrap_err();
    unknown_without_local_sequence(&error, observer.transaction_id().0);
    assert_eq!(
        observer.outcome(),
        TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    assert_eq!(
        db.tx_log_handle().stats().written_seq,
        before_wal.written_seq
    );
    assert_eq!(
        db.tx_log_handle().stats().commits_written,
        before_wal.commits_written
    );
    drop(db);
    let actual = fixture.transport.observations();
    assert_eq!(
        actual
            .iter()
            .filter(|o| o.operation == "commit"
                && o.state == Some(RangeWriteState::Committed)
                && o.error.is_none())
            .count(),
        2
    );
    assert!(!actual
        .iter()
        .any(|o| matches!(o.operation, "abort" | "certify" | "apply")));
    for (id, service) in &fixture.services {
        let probe = service
            .lock()
            .probe_range_write_for_peer(&range, &range.leader, 1)
            .unwrap();
        assert_eq!(
            probe.entry.unwrap().state,
            RangeWriteState::Committed,
            "{id}"
        );
        assert_eq!(probe.progress.resolved_through, 0);
    }
    no_local_effects(&fixture, &range, key, before);
    fixture
        .transport
        .lose_commit_replies_after_durability(false);
    let retry = fixture
        .append("native-after-unknown-must-recover")
        .unwrap_err();
    assert!(
        retry
            .to_string()
            .contains("leader recovery is not complete"),
        "{retry}"
    );
    assert_eq!(
        observer.outcome(),
        TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    cleanup(fixture);
}

#[test]
fn native_commit_uncertainty_actual_local_commit_publication_error_keeps_decision() {
    let fixture = kept_fixture();
    let range = range(&fixture);
    let key = "native-local-commit-published-error";
    let db = fixture.databases[&range.leader].write();
    let before = db
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .actual_local_visibility_sequence();
    let before_wal = db.tx_log_handle().stats();
    fixture.services[&range.leader]
        .lock()
        .test_fail_native_commit_after_publication();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new(key)).unwrap();
    let observer = tx.observe_outcome();
    let error = tx.commit().unwrap_err();
    unknown_without_local_sequence(&error, observer.transaction_id().0);
    assert_eq!(
        observer.outcome(),
        TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    assert_eq!(
        db.tx_log_handle().stats().written_seq,
        before_wal.written_seq
    );
    assert_eq!(
        db.tx_log_handle().stats().commits_written,
        before_wal.commits_written
    );
    // Read only actual persisted JSON. This inspection is data, not a permit.
    let bytes = std::fs::read(
        db.data_path()
            .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
    )
    .unwrap();
    let persisted: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let saved = persisted["catalog"]["ranges"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(saved["entries"]["1"]["state"], "committed");
    assert_eq!(saved["resolved_through"], 0);
    drop(db);
    // Failed publication must leave the live service conservatively blocked.
    let blocked = fixture.services[&range.leader]
        .lock()
        .probe_range_write_for_peer(&range, &range.leader, 1)
        .unwrap_err();
    assert!(
        blocked.to_string().contains("persistence is uncertain"),
        "{blocked}"
    );
    assert!(!fixture
        .transport
        .observations()
        .iter()
        .any(|o| matches!(o.operation, "commit" | "abort" | "certify" | "apply")));
    no_local_effects(&fixture, &range, key, before);
    let retry = fixture
        .append("native-local-unknown-must-recover")
        .unwrap_err();
    assert!(
        retry.to_string().contains("persistence is uncertain")
            || retry
                .to_string()
                .contains("leader recovery is not complete"),
        "{retry}"
    );
    assert_eq!(
        observer.outcome(),
        TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    cleanup(fixture);
}

#[test]
fn native_commit_uncertainty_prepare_only_failure_remains_no_ack_and_definite_local_abort() {
    let fixture = kept_fixture();
    let range = range(&fixture);
    let key = "native-prepare-only-no-decision";
    let before = fixture.databases[&range.leader]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .actual_local_visibility_sequence();
    for id in fixture.services.keys().filter(|id| *id != &range.leader) {
        fixture.transport.set_unavailable(id, true);
    }
    let db = fixture.databases[&range.leader].write();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new(key)).unwrap();
    let observer = tx.observe_outcome();
    let error = tx.commit().unwrap_err();
    assert!(!matches!(error, BicDbError::CommitOutcomeUnknown { .. }));
    assert!(error.to_string().contains("prepare quorum"), "{error}");
    assert_eq!(observer.outcome(), TransactionOutcome::Aborted);
    drop(db);
    let probe = fixture.services[&range.leader]
        .lock()
        .probe_range_write_for_peer(&range, &range.leader, 1)
        .unwrap();
    assert_eq!(probe.entry.unwrap().state, RangeWriteState::Aborted);
    assert!(!fixture
        .transport
        .observations()
        .iter()
        .any(|o| o.operation == "commit"));
    no_local_effects(&fixture, &range, key, before);
    cleanup(fixture);
}
