use bicdb_core::{BicDb, BicDbError, CancellationToken, DbConfig, Record, StorageMode};
use serde_json::json;

fn config(pool: u64) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fsync(false)
        .with_sync_outbox(false)
        .with_paged_buffer_pool_bytes(pool)
}

fn seeded() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config(32 * 1024 * 1024)).unwrap();
    db.create_collection("rows").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    for i in 0..128 {
        tx.insert(
            "rows",
            Record::new(format!("row-{i:04}"))
                .with_metadata(json!({"i": i, "large": "x".repeat(32 * 1024)})),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    db.close().unwrap();
    dir
}

#[test]
fn reopened_paged_count_avoids_payload_materialization() {
    let dir = seeded();
    let db = BicDb::open_with_config(dir.path(), config(2 * 1024 * 1024)).unwrap();
    let tx = db.begin_transaction().unwrap();
    let before = db.paged_storage_snapshot().unwrap().unwrap();
    let scope = bicdb_core::query_budget::QueryMemoryScope::new(64 * 1024);
    assert_eq!(
        tx.try_count_collection_visible_cancellable("rows", &CancellationToken::default())
            .unwrap(),
        Some(128)
    );
    assert!(scope.peak_bytes() < 64 * 1024);
    let after = db.paged_storage_snapshot().unwrap().unwrap();
    let read_bytes = after
        .buffer_pool
        .misses
        .saturating_sub(before.buffer_pool.misses)
        .saturating_mul(u64::from(after.page_size));
    assert!(
        read_bytes < 2 * 1024 * 1024,
        "count read {read_bytes} bytes instead of bounded headers for 4MiB of payloads"
    );
    drop(scope);
    let _scope = bicdb_core::query_budget::QueryMemoryScope::new(64 * 1024);
    assert!(
        matches!(
            tx.scan_collection("rows"),
            Err(BicDbError::QueryMemoryLimit(_))
        ),
        "the full payload scan must actually exceed this envelope"
    );
}

#[test]
fn paged_count_merges_snapshots_touched_chains_and_final_pending_writes() {
    let dir = seeded();
    let db = BicDb::open_with_config(dir.path(), config(2 * 1024 * 1024)).unwrap();
    let token = CancellationToken::default();
    let old = db.begin_transaction().unwrap();
    assert_eq!(
        old.try_count_collection_visible_cancellable("rows", &token)
            .unwrap(),
        Some(128)
    );
    let mut writer = db.begin_transaction().unwrap();
    writer.delete("rows", "row-0000").unwrap();
    writer
        .update(
            "rows",
            Record::new("row-0001").with_metadata(json!({"updated": true})),
        )
        .unwrap();
    writer.insert("rows", Record::new("later")).unwrap();
    writer.commit().unwrap();
    assert_eq!(
        old.try_count_collection_visible_cancellable("rows", &token)
            .unwrap(),
        Some(128)
    );
    assert!(old.get("rows", "row-0000").unwrap().is_some());
    assert!(old.get("rows", "later").unwrap().is_none());
    let mut tx = db.begin_transaction().unwrap();
    assert_eq!(
        tx.try_count_collection_visible_cancellable("rows", &token)
            .unwrap(),
        Some(128)
    );
    tx.insert("rows", Record::new("own")).unwrap();
    tx.update(
        "rows",
        Record::new("own").with_metadata(json!({"again": true})),
    )
    .unwrap();
    assert_eq!(
        tx.try_count_collection_visible_cancellable("rows", &token)
            .unwrap(),
        Some(129)
    );
    let mark = tx.rollback_mark();
    tx.delete("rows", "own").unwrap();
    tx.delete("rows", "row-0002").unwrap();
    assert_eq!(
        tx.try_count_collection_visible_cancellable("rows", &token)
            .unwrap(),
        Some(127)
    );
    tx.rollback_to_mark(mark).unwrap();
    assert_eq!(
        tx.try_count_collection_visible_cancellable("rows", &token)
            .unwrap(),
        Some(129)
    );
    assert_eq!(tx.scan_collection("rows").unwrap().len(), 129);
    tx.rollback().unwrap();
    old.rollback().unwrap();
}
