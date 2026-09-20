use bicdb_core::{BicDb, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

const CREATE_TABLE: &str = r#"
    CREATE TABLE special_values (
        id TEXT PRIMARY KEY,
        address INET,
        subnet CIDR,
        hardware MACADDR,
        exact_range NUMRANGE,
        day_range DATERANGE,
        object_type REGTYPE,
        object_id OID,
        addresses INET[]
    )
"#;

#[test]
fn special_values_use_canonical_storage_and_remain_legacy_compatible() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute(CREATE_TABLE).unwrap();
        session
            .execute(
                "INSERT INTO special_values VALUES (
                    'typed',
                    '192.0.2.3/24'::inet,
                    '192.0.2.0/24'::cidr,
                    '08:00:2b:01:02:03'::macaddr,
                    '[0.10,9007199254740993.01)'::numrange,
                    '[2024-02-29,2025-01-01)'::daterange,
                    'numeric'::regtype,
                    (-1)::int4::oid,
                    '{192.0.2.1,2001:db8::1/64}'::inet[]
                )",
            )
            .unwrap();
    }

    let stored = db.get("special_values", "typed").unwrap().unwrap();
    let address = &stored.metadata["address"]["$bicdb_typed"];
    assert_eq!(address["version"], 1);
    assert_eq!(address["pg_type"], "inet");
    assert_eq!(address["value"]["type"], "network");
    assert_eq!(address["value"]["value"]["prefix"], 24);
    assert!(!stored.metadata["address"].is_string());

    let exact = &stored.metadata["exact_range"]["$bicdb_typed"];
    assert_eq!(exact["value"]["type"], "range");
    assert_eq!(exact["value"]["value"]["subtype"], "numeric");
    assert_eq!(
        exact["value"]["value"]["upper"]["value"]["value"]["coefficient"],
        "900719925474099301"
    );
    assert_eq!(
        stored.metadata["object_type"]["$bicdb_typed"]["value"]["type"],
        "oid_alias"
    );
    assert_eq!(
        stored.metadata["object_id"]["$bicdb_typed"]["value"],
        json!({"type": "oid", "value": 4294967295_u64})
    );
    assert_eq!(
        stored.metadata["addresses"]["$bicdb_typed"]["value"]["elements"][0]["type"],
        "network"
    );

    let expected = vec![
        SqlValue::String("192.0.2.3/24".to_string()),
        SqlValue::String("192.0.2.0/24".to_string()),
        SqlValue::String("08:00:2b:01:02:03".to_string()),
        SqlValue::String("[0.10,9007199254740993.01)".to_string()),
        SqlValue::String("[2024-02-29,2025-01-01)".to_string()),
        SqlValue::Int(1700),
        SqlValue::Int(4_294_967_295),
        SqlValue::Json(json!(["192.0.2.1", "2001:db8::1/64"])),
    ];
    assert_eq!(
        SqlSession::new(&mut db)
            .execute(
                "SELECT address, subnet, hardware, exact_range, day_range,
                        object_type, object_id, addresses
                 FROM special_values WHERE id = 'typed'",
            )
            .unwrap()
            .rows,
        vec![expected.clone()]
    );

    db.insert(
        "special_values",
        Record::new("legacy").with_metadata(json!({
            "address": "198.51.100.8",
            "subnet": "198.51.100.0/24",
            "hardware": "08:00:2b:aa:bb:cc",
            "exact_range": "empty",
            "day_range": "[2020-01-01,2020-02-01)",
            "object_type": "text",
            "object_id": 25,
            "addresses": ["198.51.100.9"]
        })),
    )
    .unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute(
                "SELECT address, exact_range, object_id FROM special_values WHERE id = 'legacy'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("198.51.100.8".to_string()),
            SqlValue::String("empty".to_string()),
            SqlValue::Int(25),
        ]]
    );

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT address, subnet, hardware, exact_range, day_range,
                        object_type, object_id, addresses
                 FROM special_values WHERE id = 'typed'",
            )
            .unwrap()
            .rows,
        vec![expected]
    );
}

#[test]
fn special_updates_replace_canonical_payloads_and_invalid_values_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(CREATE_TABLE).unwrap();
    session
        .execute(
            "INSERT INTO special_values (id, address, subnet, exact_range, object_id)
             VALUES ('entry', '192.0.2.1', '192.0.2.0/24', '[1,2)', 26)",
        )
        .unwrap();
    session
        .execute(
            "UPDATE special_values
             SET address = '2001:db8::1/64', exact_range = '[1.25,2.50)', object_id = 2205
             WHERE id = 'entry'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT address, exact_range, object_id FROM special_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("2001:db8::1/64".to_string()),
            SqlValue::String("[1.25,2.50)".to_string()),
            SqlValue::Int(2205),
        ]]
    );
    assert!(session
        .execute("UPDATE special_values SET subnet = '192.0.2.1/24' WHERE id = 'entry'")
        .is_err());
    assert!(session
        .execute("UPDATE special_values SET exact_range = '[two,three)' WHERE id = 'entry'")
        .is_err());
    session
        .execute("UPDATE special_values SET object_id = -1 WHERE id = 'entry'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT object_id FROM special_values WHERE id = 'entry'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4_294_967_295)]]
    );
    assert!(session
        .execute("UPDATE special_values SET object_id = (-1)::int8 WHERE id = 'entry'")
        .is_err());
}

#[test]
fn all_builtin_ranges_store_typed_bounds_and_read_legacy_strings() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE typed_ranges (
                    id TEXT PRIMARY KEY,
                    ints INT4RANGE,
                    bigints INT8RANGE,
                    exacts NUMRANGE,
                    days DATERANGE,
                    local_times TSRANGE,
                    instants TSTZRANGE
                )",
            )
            .unwrap();
        session
            .execute(
                r#"INSERT INTO typed_ranges VALUES (
                    'typed',
                    '[1,5]',
                    '(,9007199254740993]',
                    'empty',
                    '[2024-02-29,)',
                    '["2024-02-29 12:34:56",)',
                    '(,"2024-02-29 12:34:56+00"]'
                )"#,
            )
            .unwrap();
    }

    let stored = db.get("typed_ranges", "typed").unwrap().unwrap();
    for (column, subtype) in [
        ("ints", "int4"),
        ("bigints", "int8"),
        ("exacts", "numeric"),
        ("days", "date"),
        ("local_times", "timestamp"),
        ("instants", "timestamptz"),
    ] {
        let value = &stored.metadata[column]["$bicdb_typed"]["value"];
        assert_eq!(value["type"], "range", "{column} was not typed");
        assert_eq!(value["value"]["subtype"], subtype, "{column} subtype");
    }
    let ints = &stored.metadata["ints"]["$bicdb_typed"]["value"]["value"];
    assert_eq!(ints["lower"]["kind"], "inclusive");
    assert_eq!(ints["upper"]["kind"], "exclusive");
    assert_eq!(ints["upper"]["value"]["value"], 6);
    let bigints = &stored.metadata["bigints"]["$bicdb_typed"]["value"]["value"];
    assert_eq!(bigints["lower"]["kind"], "unbounded");
    assert_eq!(bigints["upper"]["kind"], "exclusive");
    assert_eq!(
        stored.metadata["exacts"]["$bicdb_typed"]["value"]["value"]["empty"],
        true
    );
    assert_eq!(
        stored.metadata["days"]["$bicdb_typed"]["value"]["value"]["upper"]["kind"],
        "unbounded"
    );

    db.insert(
        "typed_ranges",
        Record::new("legacy").with_metadata(json!({
            "ints": "[2,7)",
            "bigints": "(,9007199254740995]",
            "exacts": "empty",
            "days": "[2020-01-01,)",
            "local_times": "[2020-01-01 00:00:00,)",
            "instants": "(,2020-01-01 00:00:00+00]"
        })),
    )
    .unwrap();
    let legacy = SqlSession::new(&mut db)
        .execute(
            "SELECT ints, bigints, exacts, days, local_times, instants
             FROM typed_ranges WHERE id = 'legacy'",
        )
        .unwrap();
    assert_eq!(legacy.rows[0][0].to_cell(), "[2,7)");
    assert_eq!(legacy.rows[0][1].to_cell(), "(,9007199254740996)");
    assert_eq!(legacy.rows[0][2].to_cell(), "empty");
    assert_eq!(legacy.rows[0][3].to_cell(), "[2020-01-01,)");
    assert_eq!(legacy.rows[0][4].to_cell(), "[\"2020-01-01 00:00:00\",)");
    assert_eq!(legacy.rows[0][5].to_cell(), "(,\"2020-01-01 00:00:00+00\"]");

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    let typed = SqlSession::new(&mut reopened)
        .execute(
            "SELECT ints, bigints, exacts, days, local_times, instants
             FROM typed_ranges WHERE id = 'typed'",
        )
        .unwrap();
    assert_eq!(typed.rows[0][0].to_cell(), "[1,6)");
    assert_eq!(typed.rows[0][1].to_cell(), "(,9007199254740994)");
    assert_eq!(typed.rows[0][2].to_cell(), "empty");
    assert_eq!(typed.rows[0][3].to_cell(), "[2024-02-29,)");
}
