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
#[test]
fn postgres_catalog_vectors_use_their_input_and_ordering_rules() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let scalar = session
        .execute(
            "SELECT '2 10'::oidvector > '2 2', '3'::oidvector < '2 100',
                '3'::int2vector < '2 100', '-1 10'::int2vector > '-1 2',
                ' 2  10 ' = '2 10'::oidvector",
        )
        .unwrap();
    assert_eq!(
        scalar.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT '2 10'::oidvector = '2 bogus'")
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
    assert_eq!(
        session
            .execute("SELECT '2 10'::oidvector = '2 10'::text")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
}
#[test]
fn bit_literals_and_position_keep_their_postgresql_type_and_result_name() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("SELECT position(B'' IN B''), position('' IN ''), position(B'0' IN 'x8'), overlay(B'010' PLACING B'1' FROM 1)")
        .unwrap();
    assert_eq!(
        result.columns,
        ["position", "position", "position", "overlay"]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(0),
            SqlValue::Int(1),
            SqlValue::Int(2),
            SqlValue::String("110".into()),
        ]]
    );
    session
        .execute("CREATE TABLE bit_positions(bits bit varying)")
        .unwrap();
    session
        .execute("INSERT INTO bit_positions VALUES (B'')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT position(B'' IN bits) FROM bit_positions")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    for (sql, state, message) in [
        (
            "SELECT B'01z'",
            "22P02",
            "\"z\" is not a valid binary digit",
        ),
        (
            "SELECT X'F✕'",
            "22P02",
            "\"✕\" is not a valid hexadecimal digit",
        ),
        (
            "SELECT position(''::text IN B'0')",
            "42883",
            "function pg_catalog.position(bit, text) does not exist",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), state, "{sql}: {error:?}");
        assert!(error.to_string().contains(message), "{sql}: {error:?}");
    }
}

#[test]
fn prefixed_integer_input_and_lcm_zero_preserve_postgresql_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session.execute("SELECT int8 '-0B_101', int4 '+0x_12', int2 '0o_17', lcm((-2147483648)::int4, 0), lcm(0, '-9223372036854775808'::int8)").unwrap().rows,
        vec![vec![SqlValue::Int(-5), SqlValue::Int(18), SqlValue::Int(15), SqlValue::Int(0), SqlValue::Int(0)]]
    );
    for sql in [
        "SELECT int2 '0x__a'",
        "SELECT int4 '0b_'",
        "SELECT int8 '0x_FF_'",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22P02");
    }
    for sql in [
        "SELECT lcm((-2147483648)::int4, 1)",
        "SELECT gcd((-2147483648)::int4, 0)",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22003");
    }
}
