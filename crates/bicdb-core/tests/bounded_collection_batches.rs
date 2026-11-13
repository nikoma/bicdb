use bicdb_core::{BicDb, DbConfig, Record, StorageMode};

#[test]
fn bounded_batches_do_not_decode_an_entire_embedded_collection() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(true)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        )
        .unwrap();
        db.create_collection("ack").unwrap();
        db.batch_insert(
            "ack",
            (0..64)
                .rev()
                .map(|i| {
                    Record::new(format!("{i:03}"))
                        .with_metadata(serde_json::json!({"value":"x".repeat(2048)}))
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut ids = Vec::new();
        for _ in 0..64 {
            let scope = bicdb_core::query_budget::QueryMemoryScope::new(64 * 1024);
            let rows = db.scan_collection_batch_after("ack", ids.last().map(String::as_str), 1, 4096).expect("one bounded row fits even though the whole collection exceeds the query budget");
            assert_eq!(rows.len(), 1);
            assert!(scope.peak_bytes() <= 64 * 1024);
            ids.push(rows[0].id.clone());
        }
        assert_eq!(ids, (0..64).map(|i| format!("{i:03}")).collect::<Vec<_>>());
        assert!(db
            .scan_collection_batch_after("ack", ids.last().map(String::as_str), 1, 4096)
            .unwrap()
            .is_empty());
        assert!(db.scan_collection_batch_after("ack", None, 1, 128).is_err());
        db.close().unwrap();
    }
}
