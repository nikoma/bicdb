use bicdb_core::{
    timestamp_day_index_value, BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue,
    Record, StorageMode,
};
use serde_json::json;

const DAY: i64 = 86_400_000_000;
fn label(micros: i64) -> IndexValue {
    let mut key = vec![1, 1];
    key.extend_from_slice(&((micros as u64) ^ (1_u64 << 63)).to_be_bytes());
    IndexValue::String(format!("\0bicdb:typed:{}", hex::encode(key)))
}
fn row(id: &str, micros: i64) -> Record {
    let IndexValue::String(key) = label(micros) else {
        unreachable!()
    };
    Record::new(id).with_metadata(json!({"seen":{"$bicdb_typed":{"version":1,"pg_type":"timestamp","index_key":key.strip_prefix("\0bicdb:typed:").unwrap()}}}))
}
fn definition(unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: "seen_day".into(),
        collection: "rows".into(),
        fields: vec![IndexField::TimestampDay(Box::new(
            IndexField::MetadataPath(vec!["seen".into()]),
        ))],
        unique,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}
#[test]
fn day_floor_preserves_null_infinity_and_negative_epoch_order() {
    for (input, expected) in [
        (0, 0),
        (1, 0),
        (DAY - 1, 0),
        (DAY, DAY),
        (-1, -DAY),
        (-DAY, -DAY),
        (-DAY - 1, -2 * DAY),
        (9_000_000 * DAY + 123, 9_000_000 * DAY),
    ] {
        assert_eq!(
            timestamp_day_index_value(&label(input)),
            Some(label(expected))
        );
    }
    assert_eq!(
        timestamp_day_index_value(&IndexValue::Null),
        Some(IndexValue::Null)
    );
    for key in ["0100", "0102"] {
        let value = IndexValue::String(format!("\0bicdb:typed:{key}"));
        assert_eq!(timestamp_day_index_value(&value), Some(value));
    }
    for value in [
        IndexValue::Int(5),
        IndexValue::String("2026-01-01".into()),
        IndexValue::String("\0bicdb:typed:0101zz".into()),
    ] {
        assert!(timestamp_day_index_value(&value).is_none());
    }
}
#[test]
fn physical_day_keys_backfill_mutate_rollback_and_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", row("before", -1)).unwrap();
        db.insert("rows", row("same", -DAY + 1)).unwrap();
        db.create_index(definition(false)).unwrap();
        let mut found = db.lookup_index("seen_day", &[label(-DAY)]).unwrap();
        found.sort();
        assert_eq!(found, vec!["before", "same"]);
        db.insert("rows", row("before", DAY + 12)).unwrap();
        assert_eq!(
            db.lookup_index("seen_day", &[label(-DAY)]).unwrap(),
            vec!["same"]
        );
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("rows", row("same", DAY * 3)).unwrap();
        tx.rollback().unwrap();
        assert_eq!(
            db.lookup_index("seen_day", &[label(-DAY)]).unwrap(),
            vec!["same"]
        );
        assert!(db.verify_index("seen_day").unwrap().valid);
        db.flush().unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        assert_eq!(
            db.lookup_index("seen_day", &[label(DAY)]).unwrap(),
            vec!["before"]
        );
        db.delete("rows", "before").unwrap();
        assert!(db
            .lookup_index("seen_day", &[label(DAY)])
            .unwrap()
            .is_empty());
        assert!(db.verify_index("seen_day").unwrap().valid);
    }
}
#[test]
fn unique_day_rejection_and_unsupported_storage_fail_without_publishing_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row("first", 5)).unwrap();
    db.create_index(definition(true)).unwrap();
    assert!(db.insert("rows", row("duplicate", 25)).is_err());
    assert!(db.get("rows", "duplicate").unwrap().is_none());
    let malformed = Record::new("bad").with_metadata(json!({"seen":"not a timestamp key"}));
    assert!(db.insert("rows", malformed).is_err());
    assert!(db.get("rows", "bad").unwrap().is_none());
    let mut wrong_type = row("wrong_type", DAY + 3);
    wrong_type.metadata["seen"]["$bicdb_typed"]["pg_type"] = json!("numeric");
    assert!(db.insert("rows", wrong_type).is_err());
    assert!(db.get("rows", "wrong_type").unwrap().is_none());
    db.drop_index("seen_day").unwrap();
    db.insert(
        "rows",
        Record::new("legacy").with_metadata(json!({"seen":"2000-01-01"})),
    )
    .unwrap();
    assert!(db.create_index(definition(false)).is_err());
    assert!(db.index_definitions().is_empty());
    db.delete("rows", "legacy").unwrap();
    db.create_index(definition(false)).unwrap();
    assert!(db
        .insert(
            "rows",
            Record::new("bad_nonunique").with_metadata(json!({"seen":"invalid"}))
        )
        .is_err());
    assert!(db.get("rows", "bad_nonunique").unwrap().is_none());
}
