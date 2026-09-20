use bicdb_core::{BicDb, Record};
use bicdb_sql::{migrate_legacy_typed_storage, SqlSession, SqlValue};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../fixtures/postgresql-18/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn legacy_typed_storage_migrates_to_the_v1_golden_idempotently() {
    let root = tempfile::tempdir().unwrap();
    let legacy = fixture("typed-storage-v0.json");
    let expected = fixture("typed-storage-v1.json");
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(legacy["schema"].as_str().unwrap())
        .unwrap();
    for source in legacy["records"].as_array().unwrap() {
        db.insert(
            "legacy_typed_values",
            Record::new(source["id"].as_str().unwrap()).with_metadata(source["metadata"].clone()),
        )
        .unwrap();
    }

    let report = migrate_legacy_typed_storage(&mut db, 1).unwrap();
    assert_eq!(report.tables_scanned, 1);
    assert_eq!(report.records_scanned, 2);
    assert_eq!(report.records_rewritten, 1);
    assert_eq!(report.values_rewritten, 11);

    let records = db.scan_collection("legacy_typed_values").unwrap();
    for golden in expected["records"].as_array().unwrap() {
        let record = records
            .iter()
            .find(|record| record.id == golden["id"].as_str().unwrap())
            .unwrap();
        if let Some(columns) = golden["typed_columns"].as_object() {
            for (column, pg_type) in columns {
                let envelope = &record.metadata[column]["$bicdb_typed"];
                assert_eq!(envelope["version"], 1, "{column} storage version");
                assert_eq!(envelope["pg_type"], *pg_type, "{column} PostgreSQL type");
                assert!(
                    envelope["index_key"]
                        .as_str()
                        .is_some_and(|key| !key.is_empty()),
                    "{column} typed index key"
                );
                assert!(
                    !envelope["value"].is_null() || envelope["text"].is_string(),
                    "{column} canonical value"
                );
            }
        }
        if let Some(columns) = golden["null_columns"].as_array() {
            for column in columns {
                assert!(record.metadata[column.as_str().unwrap()].is_null());
            }
        }
        if let Some(values) = golden["plain_values"].as_object() {
            for (column, value) in values {
                assert_eq!(&record.metadata[column], value);
            }
        }
    }

    let second = migrate_legacy_typed_storage(&mut db, 7).unwrap();
    assert_eq!(second.records_rewritten, 0);
    assert_eq!(second.values_rewritten, 0);
    drop(db);

    let mut reopened = BicDb::open(root.path()).unwrap();
    let rows = SqlSession::new(&mut reopened)
        .execute(
            "SELECT amount, single, double_value, happened_on, payload, flags, tags, address, window
             FROM legacy_typed_values WHERE id = 'legacy-1'",
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        SqlValue::String("9007199254740993.0100".to_string())
    );
    assert!(
        matches!(rows[0][1], SqlValue::Float(value) if (value as f32).to_bits() == (-0.0_f32).to_bits())
    );
    assert_eq!(rows[0][2], SqlValue::Float(42.5));
    assert_eq!(rows[0][3], SqlValue::String("2026-07-17".to_string()));
    assert_eq!(rows[0][4], SqlValue::String("\\x00017fff".to_string()));
    assert_eq!(rows[0][5], SqlValue::String("00101".to_string()));
    assert_eq!(
        rows[0][6],
        SqlValue::Json(serde_json::json!(["alpha", null, "omega"]))
    );
    assert_eq!(rows[0][7], SqlValue::String("2001:db8::1/64".to_string()));
    assert_eq!(rows[0][8], SqlValue::String("[10,20)".to_string()));
}

#[test]
fn typed_storage_migration_rejects_a_zero_batch_size() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let error = migrate_legacy_typed_storage(&mut db, 0).unwrap_err();
    assert!(error
        .to_string()
        .contains("batch size must be greater than zero"));
}
