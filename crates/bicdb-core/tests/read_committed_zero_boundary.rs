use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StoredRecord};
use serde_json::json;
use std::sync::Arc;

fn observed_absence_survives_refresh(refresh_before_buffering: bool) {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("business").unwrap();
    db.create_collection("independent").unwrap();
    let mut stale = db.begin_transaction().unwrap();
    stale.set_snapshot_isolation(false, 0).unwrap();
    assert!(stale.get("business", "shared").unwrap().is_none());

    db.insert(
        "business",
        Record::new("shared").with_metadata(json!({"winner": true})),
    )
    .unwrap();
    if refresh_before_buffering {
        db.insert("independent", Record::new("nonce")).unwrap();
        stale.refresh_read_committed_snapshot().unwrap();
    }
    stale
        .insert(
            "business",
            Record::new("shared").with_metadata(json!({"winner": false})),
        )
        .unwrap();
    if !refresh_before_buffering {
        db.insert("independent", Record::new("nonce")).unwrap();
        stale.refresh_read_committed_snapshot().unwrap();
    }

    assert!(
        matches!(stale.commit(), Err(BicDbError::TransactionConflict(_))),
        "refreshing after an independent commit must not authorize replacing a key observed absent"
    );
    assert_eq!(
        db.get("business", "shared").unwrap().unwrap().metadata["winner"],
        true
    );
    assert!(db.get("independent", "nonce").unwrap().is_some());
}

#[test]
fn refresh_after_buffering_cannot_rebase_observed_absent_key() {
    observed_absence_survives_refresh(false);
}

#[test]
fn refresh_before_buffering_cannot_rebase_observed_absent_key() {
    observed_absence_survives_refresh(true);
}

#[test]
fn independent_refresh_keeps_new_uncontested_key_committable() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("business").unwrap();
    db.create_collection("independent").unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction.set_snapshot_isolation(false, 0).unwrap();
    assert!(transaction.get("business", "new").unwrap().is_none());
    transaction.insert("business", Record::new("new")).unwrap();
    db.insert("independent", Record::new("nonce")).unwrap();
    transaction.refresh_read_committed_snapshot().unwrap();
    transaction.commit().unwrap();
    assert!(db.get("business", "new").unwrap().is_some());
    drop(db);
    let reopened = BicDb::open(directory.path()).unwrap();
    assert!(reopened.get("business", "new").unwrap().is_some());
    assert!(reopened.get("independent", "nonce").unwrap().is_some());
}

#[test]
fn zero_snapshot_optimistic_insert_keeps_its_original_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("business").unwrap();
    db.create_collection("independent").unwrap();
    assert_eq!(
        db.last_commit_seq(),
        0,
        "exercise a genuine zero snapshot without point reads"
    );
    let mut stale = db.begin_transaction().unwrap();
    stale.set_snapshot_isolation(false, 0).unwrap();
    let pending = Arc::new(
        StoredRecord::from_record(&Record::new("shared").with_metadata(json!({"winner":false})))
            .unwrap(),
    );
    stale
        .write_stored_inserts_with_statement_snapshots("business", [(pending, 0)])
        .unwrap();
    db.insert(
        "business",
        Record::new("shared").with_metadata(json!({"winner":true})),
    )
    .unwrap();
    db.insert("independent", Record::new("nonce")).unwrap();
    stale.refresh_read_committed_snapshot().unwrap();
    assert!(matches!(
        stale.commit(),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(
        db.get("business", "shared").unwrap().unwrap().metadata["winner"],
        true
    );
}
