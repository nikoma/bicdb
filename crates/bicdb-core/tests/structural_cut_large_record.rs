#![cfg(feature = "experimental-structural-cuts")]
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
#[test]
fn a_large_logical_value_streams_without_stealing_and_survives_recovery() {
    let root = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        .with_paged_buffer_pool_bytes(128 * 1024);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_collection("rows").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "rows",
        Record::new("acknowledged").with_metadata(json!({"v": 1})),
    )
    .unwrap();
    tx.commit().unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "rows",
        Record::new("oversized").with_metadata(json!({"blob": "x".repeat(100_000)})),
    )
    .unwrap();
    tx.commit().expect("a logical value larger than the pool reserve must remain supported");
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "rows",
        Record::new("after-large-value").with_metadata(json!({"v": 2})),
    )
    .unwrap();
    tx.commit().unwrap();
    db.close().unwrap();
    for _ in 0..2 {
        let db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        assert!(db.get("rows", "acknowledged").unwrap().is_some());
        assert!(db.get("rows", "after-large-value").unwrap().is_some());
        let large = db.get("rows", "oversized").unwrap().unwrap();
        assert_eq!(large.metadata["blob"].as_str().unwrap().len(), 100_000);
        db.close().unwrap();
    }
}
