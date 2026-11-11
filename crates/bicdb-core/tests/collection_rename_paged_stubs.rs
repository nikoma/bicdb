//! A stored row pins its physical backing even when a logical name is reused.
use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode, VisibleRow,
};
use serde_json::json;

fn put(db: &BicDb, label: &str) {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "authored",
        Record::new("same-key").with_metadata(json!({"label":label,"terms":[label]})),
    )
    .unwrap();
    tx.commit().unwrap();
}

fn materialized_index(db: &mut BicDb) {
    db.create_index(IndexDefinition {
        name: "authored_terms".into(),
        collection: "authored".into(),
        fields: vec![IndexField::MetadataPath(vec!["terms".into()])],
        unique: false,
        kind: IndexKind::Array,
        predicate: None,
        exclusion: None,
        nulls_not_distinct: false,
    })
    .unwrap();
}

fn exercise(phase: u8) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.create_collection("authored").unwrap();
        put(&db, "original");
        if phase == 2 {
            materialized_index(&mut db);
        }
        if phase != 0 {
            drop(db);
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        if phase == 1 {
            // A non-page-backed index materializes the reopened lazy rows as
            // stored stubs without replacing their pinned snapshot.
            materialized_index(&mut db);
        }
        let reader = db.begin_transaction().unwrap();
        let row = reader.get_stored("authored", "same-key").unwrap().unwrap();
        assert!(matches!(row, VisibleRow::Stored(_)));
        db.rename_collection("authored", "renamed").unwrap();
        db.create_collection("authored").unwrap();
        put(&db, "replacement");
        let expected = json!({"label":"original","terms":["original"]});
        assert_eq!(row.to_record().unwrap().metadata, expected);
        assert_eq!(
            reader
                .get_stored("renamed", "same-key")
                .unwrap()
                .unwrap()
                .to_record()
                .unwrap()
                .metadata,
            expected
        );
        assert_eq!(
            db.get_stored("authored", "same-key")
                .unwrap()
                .unwrap()
                .to_record()
                .unwrap()
                .metadata["label"],
            "replacement"
        );
        drop(row);
        reader.rollback().unwrap();
        drop(db);
        let db = BicDb::open_with_config(root.path(), cfg).unwrap();
        assert_eq!(
            db.get("renamed", "same-key").unwrap().unwrap().metadata,
            expected
        );
        assert_eq!(
            db.get("authored", "same-key").unwrap().unwrap().metadata["label"],
            "replacement"
        );
    }
}

#[test]
fn committed_stub_keeps_its_physical_backing_after_name_reuse() {
    exercise(0);
}

#[test]
fn materialized_lazy_stub_keeps_its_physical_backing_after_name_reuse() {
    exercise(1);
}

#[test]
fn reopened_nondurable_index_stub_keeps_its_physical_backing_after_name_reuse() {
    exercise(2);
}
