use bicdb_core::{BicDb, BicDbError, DbConfig, IndexDefinition, IndexField, IndexKind, Record};

#[test]
fn unique_index_build_conflict_is_structured_and_does_not_publish_an_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    for id in ["first", "second"] {
        db.insert(
            "rows",
            Record::new(id).with_metadata(serde_json::json!({"value": 7})),
        )
        .unwrap();
    }
    let error = db
        .create_index(IndexDefinition {
            name: "unique_value".into(),
            collection: "rows".into(),
            fields: vec![IndexField::MetadataPath(vec!["value".into()])],
            unique: true,
            nulls_not_distinct: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap_err();
    assert!(
        matches!(&error, BicDbError::UniqueIndexBuildConflict { index } if index == "unique_value")
    );
    assert_eq!(
        error.to_string(),
        "unique index `unique_value` has duplicate keys"
    );
    assert!(db.index_definitions().is_empty());
    drop(db);
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    assert!(db.index_definitions().is_empty());
    assert_eq!(db.scan_collection("rows").unwrap().len(), 2);
}
