#![cfg(feature = "experimental-structural-cuts")]
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
#[test]
fn an_oversized_value_is_rejected_before_a_logical_decision_and_never_blocks_recovery() {
    let root = tempfile::tempdir().unwrap();
    let config = DbConfig::default().with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false).with_paged_buffer_pool_bytes(128 * 1024);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_collection("rows").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("rows", Record::new("acknowledged").with_metadata(json!({"v": 1}))).unwrap();
    tx.commit().unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("rows", Record::new("oversized").with_metadata(json!({"blob": "x".repeat(100_000)}))).unwrap();
    let error = tx.commit().expect_err("size must be rejected before its logical decision");
    assert!(error.to_string().contains("does not fit"), "{error}");
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("rows", Record::new("after-rejection").with_metadata(json!({"v": 2}))).unwrap();
    tx.commit().unwrap();
    db.close().unwrap();
    for _ in 0..2 {
        let db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        assert!(db.get("rows", "acknowledged").unwrap().is_some());
        assert!(db.get("rows", "after-rejection").unwrap().is_some());
        assert!(db.get("rows", "oversized").unwrap().is_none());
        db.close().unwrap();
    }
}
