use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

#[test]
fn a_legacy_array_backfill_is_queryable_immediately_and_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fsync(false)
        .with_sync_outbox(false);
    {
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("documents").unwrap();
        db.bulk_load_insert(
            "documents",
            (0..250).map(|i| {
                Record::new(format!("document-{i:04}")).with_metadata(json!({
                    "terms": ["shared", format!("unique{i}")]
                }))
            }),
        )
        .unwrap();
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_index(IndexDefinition {
        name: "documents_terms".into(),
        collection: "documents".into(),
        fields: vec![IndexField::MetadataPath(vec!["terms".into()])],
        unique: false,
        kind: IndexKind::FullText,
        nulls_not_distinct: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    let assert_complete = |db: &BicDb| {
        assert_eq!(
            db.lookup_full_text_term("documents_terms", "shared", false)
                .unwrap()
                .len(),
            250
        );
        assert_eq!(
            db.lookup_full_text_term("documents_terms", "unique17", false)
                .unwrap(),
            vec!["document-0017"]
        );
        assert!(db.verify_index("documents_terms").unwrap().valid);
    };
    assert_complete(&db);
    db.close().unwrap();
    let db = BicDb::open_with_config(root.path(), config).unwrap();
    assert_complete(&db);
    db.close().unwrap();
}
