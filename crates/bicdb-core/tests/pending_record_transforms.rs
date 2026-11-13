use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StorageMode};

#[test]
fn effective_pending_transforms_preserve_deletes_and_savepoint_history() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        db.create_collection("values").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        for id in 0..40 {
            tx.insert(
                "values",
                Record::new(id.to_string()).with_metadata(serde_json::json!({"value":id})),
            )
            .unwrap();
            if id % 2 == 0 {
                tx.update(
                    "values",
                    Record::new(id.to_string()).with_metadata(serde_json::json!({"value":id+40})),
                )
                .unwrap();
            }
            if id % 4 == 0 {
                tx.delete("values", &id.to_string()).unwrap();
            }
        }
        let mark = tx.rollback_mark();
        let mut calls = 0;
        tx.transform_pending_records("values", |record| -> Result<Record, BicDbError> {
            calls += 1;
            let mut record = record.clone();
            record.metadata["value"] =
                serde_json::json!(record.metadata["value"].as_i64().unwrap() + 100);
            Ok(record)
        })
        .unwrap();
        assert_eq!(calls, 30, "visit only the final live upsert for each key");
        assert_eq!(
            tx.get("values", "2").unwrap().unwrap().metadata["value"],
            142
        );
        assert!(tx.get("values", "0").unwrap().is_none());
        tx.rollback_to_mark(mark).unwrap();
        assert_eq!(
            tx.get("values", "2").unwrap().unwrap().metadata["value"],
            42
        );
        assert!(tx.get("values", "0").unwrap().is_none());
        tx.commit().unwrap();
        assert_eq!(
            db.get("values", "2").unwrap().unwrap().metadata["value"],
            42
        );
        assert!(db.get("values", "0").unwrap().is_none());
        db.close().unwrap();
    }
}
