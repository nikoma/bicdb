//! A catalog rename must not retarget an already-buffered write by name reuse.
use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StorageMode};
use serde_json::json;

#[test]
fn plain_rename_cannot_retarget_a_buffered_write_to_a_replacement() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("authored").unwrap();
        let mut pending = db.begin_transaction().unwrap();
        pending
            .insert(
                "authored",
                Record::new("original").with_metadata(json!({"value":"authored"})),
            )
            .unwrap();
        db.rename_collection("authored", "renamed").unwrap();
        db.create_collection("authored").unwrap();
        db.insert(
            "authored",
            Record::new("replacement").with_metadata(json!({"value":"replacement"})),
        )
        .unwrap();
        assert!(db.get("authored", "original").unwrap().is_none());
        assert!(matches!(
            pending.commit(),
            Err(BicDbError::TransactionConflict(_))
        ));
        assert!(db.get("authored", "original").unwrap().is_none());
        assert!(db.get("renamed", "original").unwrap().is_none());
        drop(db);
        let db = BicDb::open_with_config(root.path(), config).unwrap();
        assert!(db.get("authored", "original").unwrap().is_none());
        assert!(db.get("renamed", "replacement").unwrap().is_none());
        assert_eq!(db.scan_collection("authored").unwrap().len(), 1);
        assert!(db.scan_collection("renamed").unwrap().is_empty());
    }
}

#[test]
fn owned_rename_keeps_its_pending_writes_and_refuses_other_live_transactions() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        db.create_collection("authored").unwrap();
        let mut own = db.begin_transaction().unwrap();
        own.insert("authored", Record::new("owned")).unwrap();
        let other = db.begin_transaction().unwrap();
        assert!(matches!(
            db.rename_collection_preserving_pending_writes(&mut own, "authored", "renamed"),
            Err(BicDbError::TransactionConflict(_))
        ));
        assert!(db.has_collection("authored"));
        assert!(!db.has_collection("renamed"));
        other.rollback().unwrap();
        db.rename_collection_preserving_pending_writes(&mut own, "authored", "renamed")
            .unwrap();
        assert!(own.get("renamed", "owned").unwrap().is_some());
        own.commit().unwrap();
        assert!(db.get("renamed", "owned").unwrap().is_some());
        db.create_collection("authored").unwrap();
        assert!(db.get("authored", "owned").unwrap().is_none());
    }
}

#[test]
fn drop_and_recreate_with_the_same_storage_name_cannot_inherit_buffered_writes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        db.create_collection("authored").unwrap();
        let mut pending = db.begin_transaction().unwrap();
        pending.insert("authored", Record::new("original")).unwrap();
        db.drop_collection("authored").unwrap();
        db.create_collection("authored").unwrap();
        db.insert("authored", Record::new("replacement")).unwrap();
        assert!(matches!(
            pending.commit(),
            Err(BicDbError::TransactionConflict(_))
        ));
        assert!(db.get("authored", "original").unwrap().is_none());
        assert!(db.get("authored", "replacement").unwrap().is_some());
    }
}

#[test]
fn a_prior_read_cannot_supply_an_image_to_a_new_collection_with_the_same_name() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        db.create_collection("authored").unwrap();
        db.insert("authored", Record::new("original")).unwrap();
        let mut reader = db.begin_transaction().unwrap();
        assert!(reader.get("authored", "original").unwrap().is_some());
        db.rename_collection("authored", "renamed").unwrap();
        db.create_collection("authored").unwrap();
        assert!(matches!(
            reader.insert("authored", Record::new("later")),
            Err(BicDbError::TransactionConflict(_))
        ));
        reader.rollback().unwrap();
        assert!(db.get("authored", "later").unwrap().is_none());
        assert!(db.get("renamed", "original").unwrap().is_some());
    }
}
