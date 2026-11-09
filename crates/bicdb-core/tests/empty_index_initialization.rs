use bicdb_core::{
    BicDb, BicDbError, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode,
};

fn index() -> IndexDefinition {
    IndexDefinition {
        name: "fresh_value".into(),
        collection: "fresh".into(),
        fields: vec![IndexField::MetadataPath(vec!["value".into()])],
        unique: true,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

#[test]
fn empty_initialization_refuses_pending_target_rows_and_nonempty_corpora() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    db.create_collection("fresh").unwrap();
    let mut pending = db.begin_transaction().unwrap();
    pending
        .insert(
            "fresh",
            Record::new("pending").with_metadata(serde_json::json!({"value": 1})),
        )
        .unwrap();
    assert!(matches!(
        db.create_empty_btree_index(index()),
        Err(BicDbError::Index(_))
    ));
    assert!(db.index_definitions().is_empty());
    pending.rollback().unwrap();
    db.insert(
        "fresh",
        Record::new("committed").with_metadata(serde_json::json!({"value": 1})),
    )
    .unwrap();
    assert!(matches!(
        db.create_empty_btree_index(index()),
        Err(BicDbError::Index(_))
    ));
    assert!(db.index_definitions().is_empty());
}
