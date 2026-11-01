use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::{json, Value};
use std::sync::{Arc, Barrier};

fn definition(nulls_not_distinct: bool) -> IndexDefinition {
    IndexDefinition {
        name: "unique_keys".into(),
        collection: "keys".into(),
        fields: vec![
            IndexField::MetadataPath(vec!["a".into()]),
            IndexField::MetadataPath(vec!["b".into()]),
        ],
        unique: true,
        nulls_not_distinct,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}
fn row(id: &str, a: i64, b: Value) -> Record {
    Record::new(id).with_metadata(json!({"a":a,"b":b}))
}
fn modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

#[test]
fn nulls_not_distinct_backfill_rejects_duplicate_null_keys() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        db.create_collection("keys").unwrap();
        db.insert("keys", row("one", 1, Value::Null)).unwrap();
        db.insert("keys", row("two", 1, Value::Null)).unwrap();
        assert!(db.create_index(definition(true)).is_err());
        assert_eq!(db.scan_collection("keys").unwrap().len(), 2);
        // Abandon the failed online build before testing the legacy policy.
        db.drop_index("unique_keys").unwrap();
        db.discard_paged_btree_build("unique_keys", Default::default())
            .unwrap();
        // The legacy/default PostgreSQL policy permits these duplicates.
        db.create_index(definition(false)).unwrap();
        db.insert("keys", row("three", 1, Value::Null)).unwrap();
        assert!(db.verify_index("unique_keys").unwrap().valid);
    }
}

#[test]
fn nulls_not_distinct_atomic_mutations_and_reopen() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("keys").unwrap();
            db.insert("keys", row("one", 1, Value::Null)).unwrap();
            db.create_index(definition(true)).unwrap();
            assert!(db.insert("keys", row("duplicate", 1, Value::Null)).is_err());
            db.insert("keys", row("text", 1, json!("NULL"))).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("keys", row("two", 2, Value::Null)).unwrap();
            tx.insert("keys", row("three", 2, Value::Null)).unwrap();
            assert!(tx.commit().is_err());
            assert!(db.get("keys", "two").unwrap().is_none());
            assert!(db.get("keys", "three").unwrap().is_none());
            let mut tx = db.begin_transaction().unwrap();
            tx.update("keys", row("one", 3, Value::Null)).unwrap();
            tx.insert("keys", row("replacement", 1, Value::Null))
                .unwrap();
            tx.commit().unwrap();
            assert!(db.verify_index("unique_keys").unwrap().valid);
            let format: Value =
                serde_json::from_slice(&std::fs::read(root.path().join("format.json")).unwrap())
                    .unwrap();
            assert!(format["feature_flags"]
                .as_array()
                .unwrap()
                .contains(&json!("unique_nulls_not_distinct_v1")));
        }
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        assert!(db.verify_index("unique_keys").unwrap().valid);
        assert!(db.insert("keys", row("again", 1, Value::Null)).is_err());
        assert_eq!(db.scan_collection("keys").unwrap().len(), 3);
    }
}

#[test]
fn nulls_not_distinct_concurrent_commits_have_one_winner() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        db.create_collection("keys").unwrap();
        db.create_index(definition(true)).unwrap();
        let db = Arc::new(db);
        let ready = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|n| {
                let db = db.clone();
                let ready = ready.clone();
                std::thread::spawn(move || {
                    let mut tx = db.begin_transaction().unwrap();
                    tx.insert("keys", row(&format!("row-{n}"), 1, Value::Null))
                        .unwrap();
                    ready.wait();
                    tx.commit().is_ok()
                })
            })
            .collect::<Vec<_>>();
        let wins = handles
            .into_iter()
            .map(|h| usize::from(h.join().unwrap()))
            .sum::<usize>();
        assert_eq!(wins, 1);
        assert_eq!(db.scan_collection("keys").unwrap().len(), 1);
        assert!(db.verify_index("unique_keys").unwrap().valid);
    }
}

#[test]
fn nulls_not_distinct_wire_is_explicit_and_legacy_defaults_are_preserved() {
    #[derive(serde::Deserialize)]
    struct OldReader {
        #[allow(dead_code)]
        unique: bool,
    }
    let value = serde_json::to_value(definition(true)).unwrap();
    assert_eq!(value["unique"], json!("nulls_not_distinct"));
    assert!(serde_json::from_value::<OldReader>(value.clone()).is_err());
    assert_eq!(
        serde_json::from_value::<IndexDefinition>(value).unwrap(),
        definition(true)
    );
    let legacy = serde_json::to_value(definition(false)).unwrap();
    assert_eq!(legacy["unique"], json!(true));
    assert!(legacy.get("nulls_not_distinct").is_none());
    assert!(
        !serde_json::from_value::<IndexDefinition>(legacy.clone())
            .unwrap()
            .nulls_not_distinct
    );
    let mut explicit = legacy;
    explicit["nulls_not_distinct"] = json!(true);
    assert!(
        serde_json::from_value::<IndexDefinition>(explicit.clone())
            .unwrap()
            .nulls_not_distinct
    );
    explicit["unique"] = json!(false);
    assert!(serde_json::from_value::<IndexDefinition>(explicit).is_err());
}
