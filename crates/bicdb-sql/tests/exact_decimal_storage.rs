use bicdb_core::{BicDb, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

#[test]
fn numeric_storage_is_typed_exact_and_backward_readable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE exact_decimals (id TEXT PRIMARY KEY, amount NUMERIC)")
            .unwrap();
        session
            .execute(
                "INSERT INTO exact_decimals VALUES
                    ('small', 0.10::numeric),
                    ('large', 900719925474099301234567890.0100::numeric),
                    ('normalized', '00012.3400'::numeric)",
            )
            .unwrap();
    }

    let stored = db.get("exact_decimals", "large").unwrap().unwrap();
    let envelope = &stored.metadata["amount"]["$bicdb_typed"];
    assert_eq!(envelope["version"], 1);
    assert_eq!(envelope["pg_type"], "numeric");
    // Compact envelope (1.0.366+): canonical text plus hex index key, no
    // structured PgNumeric copy.
    assert_eq!(envelope["text"], "900719925474099301234567890.0100");
    assert!(envelope["value"].is_null());
    assert!(envelope["index_key"]
        .as_str()
        .is_some_and(|key| !key.is_empty()));
    assert!(!stored.metadata["amount"].is_string());

    // Records written before typed storage remain readable without migration.
    db.insert(
        "exact_decimals",
        Record::new("legacy").with_metadata(json!({"amount": "42.500"})),
    )
    .unwrap();

    let rows = SqlSession::new(&mut db)
        .execute("SELECT id, amount FROM exact_decimals ORDER BY id")
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![
                SqlValue::String("large".to_string()),
                SqlValue::String("900719925474099301234567890.0100".to_string()),
            ],
            vec![
                SqlValue::String("legacy".to_string()),
                SqlValue::String("42.500".to_string()),
            ],
            vec![
                SqlValue::String("normalized".to_string()),
                SqlValue::String("12.3400".to_string()),
            ],
            vec![
                SqlValue::String("small".to_string()),
                SqlValue::String("0.10".to_string()),
            ],
        ]
    );

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    let result = SqlSession::new(&mut reopened)
        .execute("SELECT amount FROM exact_decimals WHERE id = 'large'")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String(
            "900719925474099301234567890.0100".to_string()
        )]]
    );
}

#[test]
fn numeric_updates_replace_the_typed_envelope_and_preserve_scale() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE decimal_updates (id TEXT PRIMARY KEY, amount NUMERIC)")
        .unwrap();
    session
        .execute("INSERT INTO decimal_updates VALUES ('entry', 1.00::numeric)")
        .unwrap();
    session
        .execute("UPDATE decimal_updates SET amount = '-0007.2500'::numeric WHERE id = 'entry'")
        .unwrap();
    let result = session
        .execute("SELECT amount FROM decimal_updates WHERE id = 'entry'")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("-7.2500".to_string())]]
    );
}
