use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDb, BicDbError, MutationActor, Record, TransactionIsolation};
use serde_json::json;

fn actor(trace: &str) -> MutationActor {
    MutationActor {
        actor_id: "carrier-transaction-test".to_string(),
        roles: BTreeSet::new(),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "carrier-transaction-test".to_string(),
        originating_resource: None,
        originating_action: Some("test".to_string()),
        trace_id: trace.to_string(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000,
    }
}

fn account(id: &str, balance: i64) -> Record {
    Record::new(id).with_metadata(json!({"balance": balance}))
}

#[test]
fn repeatable_read_keeps_its_fixed_mvcc_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();

    let repeatable = db
        .begin_application_transaction_with_isolation(
            actor("repeatable"),
            TransactionIsolation::RepeatableRead,
        )
        .unwrap();
    assert_eq!(
        repeatable
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        100
    );

    let mut writer = db.begin_transaction().unwrap();
    writer.update("accounts", account("a", 75)).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        repeatable
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        100
    );
    repeatable.commit().unwrap();
    assert_eq!(
        db.get("accounts", "a").unwrap().unwrap().metadata["balance"],
        75
    );
}

#[test]
fn read_committed_refreshes_at_each_host_statement_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();

    let mut read_committed = db
        .begin_application_transaction_with_isolation(
            actor("read-committed"),
            TransactionIsolation::ReadCommitted,
        )
        .unwrap();
    assert_eq!(
        read_committed
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        100
    );
    let mut writer = db.begin_transaction().unwrap();
    writer.update("accounts", account("a", 75)).unwrap();
    writer.commit().unwrap();

    read_committed.refresh_read_committed_snapshot().unwrap();
    assert_eq!(
        read_committed
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        75
    );
    read_committed.commit().unwrap();
}

#[test]
fn serializable_relation_reads_reject_write_skew() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();
    db.insert("accounts", account("b", 100)).unwrap();

    let mut first = db
        .begin_application_transaction_with_isolation(
            actor("serializable-first"),
            TransactionIsolation::Serializable,
        )
        .unwrap();
    let mut second = db
        .begin_application_transaction_with_isolation(
            actor("serializable-second"),
            TransactionIsolation::Serializable,
        )
        .unwrap();
    first.record_serializable_read("accounts").unwrap();
    second.record_serializable_read("accounts").unwrap();
    assert_eq!(
        first.scan_collection_authorized("accounts").unwrap().len(),
        2
    );
    assert_eq!(
        second.scan_collection_authorized("accounts").unwrap().len(),
        2
    );

    first.update("accounts", account("a", 0)).unwrap();
    second.update("accounts", account("b", 0)).unwrap();
    first.commit().unwrap();
    let error = second
        .commit()
        .expect_err("the stale serializable writer must abort");
    assert!(matches!(error, BicDbError::TransactionConflict(_)));
    assert_eq!(
        db.get("accounts", "a").unwrap().unwrap().metadata["balance"],
        0
    );
    assert_eq!(
        db.get("accounts", "b").unwrap().unwrap().metadata["balance"],
        100
    );
}
