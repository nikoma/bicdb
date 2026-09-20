use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn floats_preserve_ieee_bits_in_versioned_metadata_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE typed_floats (
                    id TEXT PRIMARY KEY,
                    single FLOAT4,
                    double_value FLOAT8
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO typed_floats VALUES
                    ('negative-zero', '-0'::float4, '-0'::float8),
                    ('nan', 'NaN'::float4, 'NaN'::float8),
                    ('positive-infinity', 'Infinity'::float4, 'Infinity'::float8),
                    ('negative-infinity', '-Infinity'::float4, '-Infinity'::float8)",
            )
            .unwrap();
    }

    let db = BicDb::open(root.path()).unwrap();
    for record in db.scan_collection("typed_floats").unwrap() {
        for column in ["single", "double_value"] {
            let envelope = &record.metadata[column]["$bicdb_typed"];
            assert_eq!(envelope["version"], 1);
            assert_eq!(
                envelope["pg_type"],
                if column == "single" {
                    "float4"
                } else {
                    "float8"
                }
            );
            assert!(envelope["index_key"].is_string());
            assert!(envelope["value"].get("type").is_some());
        }
    }
    drop(db);

    let mut reopened = BicDb::open(root.path()).unwrap();
    let rows = SqlSession::new(&mut reopened)
        .execute("SELECT id, single, double_value FROM typed_floats ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 4);

    let negative_zero = rows
        .iter()
        .find(|row| row[0] == SqlValue::String("negative-zero".to_string()))
        .unwrap();
    let SqlValue::Float(single) = negative_zero[1] else {
        panic!("expected float4 value")
    };
    let SqlValue::Float(double_value) = negative_zero[2] else {
        panic!("expected float8 value")
    };
    assert_eq!((single as f32).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(double_value.to_bits(), (-0.0_f64).to_bits());

    let nan = rows
        .iter()
        .find(|row| row[0] == SqlValue::String("nan".to_string()))
        .unwrap();
    assert!(matches!(nan[1], SqlValue::Float(value) if value.is_nan()));
    assert!(matches!(nan[2], SqlValue::Float(value) if value.is_nan()));

    let positive = rows
        .iter()
        .find(|row| row[0] == SqlValue::String("positive-infinity".to_string()))
        .unwrap();
    assert!(matches!(positive[1], SqlValue::Float(value) if value == f64::INFINITY));
    assert!(matches!(positive[2], SqlValue::Float(value) if value == f64::INFINITY));

    let negative = rows
        .iter()
        .find(|row| row[0] == SqlValue::String("negative-infinity".to_string()))
        .unwrap();
    assert!(matches!(negative[1], SqlValue::Float(value) if value == f64::NEG_INFINITY));
    assert!(matches!(negative[2], SqlValue::Float(value) if value == f64::NEG_INFINITY));
}

#[test]
fn legacy_json_float_values_remain_readable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE legacy_floats (id TEXT PRIMARY KEY, value FLOAT8)")
            .unwrap();
    }
    db.insert(
        "legacy_floats",
        bicdb_core::Record::new("legacy").with_metadata(serde_json::json!({"value": 42.5})),
    )
    .unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT value FROM legacy_floats WHERE id = 'legacy'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Float(42.5)]]
    );
}

#[test]
fn money_preserves_exact_cents_in_versioned_metadata_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE ledger (id TEXT PRIMARY KEY, amount MONEY)")
            .unwrap();
        session
            .execute("INSERT INTO ledger VALUES ('credit', '$1,234.50'), ('debit', '-0.01')")
            .unwrap();
    }

    let db = BicDb::open(root.path()).unwrap();
    for record in db.scan_collection("ledger").unwrap() {
        let envelope = &record.metadata["amount"]["$bicdb_typed"];
        assert_eq!(envelope["version"], 1);
        assert_eq!(envelope["pg_type"], "money");
        assert_eq!(envelope["value"]["type"], "money");
        assert!(envelope["index_key"].is_string());
    }
    drop(db);

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT id, amount FROM ledger ORDER BY amount")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("debit".into()),
                SqlValue::String("-0.01".into())
            ],
            vec![
                SqlValue::String("credit".into()),
                SqlValue::String("1234.50".into())
            ],
        ]
    );
}
