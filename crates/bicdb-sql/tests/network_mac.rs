use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

#[test]
fn mac_casts_and_storage_are_canonical_and_durable() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            session
                .execute(
                    "SELECT '8:0:2b:1:2:3'::macaddr,
                            '08002b-010203'::macaddr,
                            '08.00.2b.01.02.03'::macaddr8,
                            '08002b0102030405'::macaddr8,
                            '08:00:2b:01:02:03'::macaddr::macaddr8,
                            '08:00:2b:ff:fe:01:02:03'::macaddr8::macaddr",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("08:00:2b:01:02:03".to_string()),
                SqlValue::String("08:00:2b:01:02:03".to_string()),
                SqlValue::String("08:00:2b:ff:fe:01:02:03".to_string()),
                SqlValue::String("08:00:2b:01:02:03:04:05".to_string()),
                SqlValue::String("08:00:2b:ff:fe:01:02:03".to_string()),
                SqlValue::String("08:00:2b:01:02:03".to_string()),
            ]],
        );

        session
            .execute(
                "CREATE TABLE mac_values (
                    id int4 PRIMARY KEY,
                    address macaddr NOT NULL,
                    extended macaddr8 NOT NULL,
                    addresses macaddr[] NOT NULL,
                    extended_addresses macaddr8[] NOT NULL
                 )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO mac_values VALUES (
                    1,
                    '0800.2b01.0203',
                    '08.00.2b.01.02.03',
                    '{8:0:2b:1:2:3,08002b-010204}'::macaddr[],
                    '{08002b010203,08-00-2b-01-02-03-04-05}'::macaddr8[]
                 )",
            )
            .unwrap();
        assert_mac_row(&mut session);
        let result = session
            .execute("SELECT address::text::varchar FROM mac_values")
            .unwrap();
        assert_eq!(result.columns, vec!["address"]);
    }

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_mac_row(&mut SqlSession::new(&mut reopened));
}

fn assert_mac_row(session: &mut SqlSession<'_>) {
    assert_eq!(
        session
            .execute(
                "SELECT address, extended, addresses, extended_addresses
                 FROM mac_values",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("08:00:2b:01:02:03".to_string()),
            SqlValue::String("08:00:2b:ff:fe:01:02:03".to_string()),
            SqlValue::Json(json!(["08:00:2b:01:02:03", "08:00:2b:01:02:04"])),
            SqlValue::Json(json!([
                "08:00:2b:ff:fe:01:02:03",
                "08:00:2b:01:02:03:04:05"
            ])),
        ]],
    );
}

#[test]
fn mac_rejects_invalid_input_and_unsafe_reverse_conversion() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for (input, pg_type) in [
        ("08:00-2b:01:02:03", "macaddr"),
        ("08.00.2b.01.02.03", "macaddr"),
        ("08:00:2b:01:02:03:04:05", "macaddr"),
        ("8:0:2b:1:2:3", "macaddr8"),
        ("08:00-2b:01:02:03", "macaddr8"),
        ("08:00:2b:01:02:03:04", "macaddr8"),
    ] {
        let error = session
            .execute(&format!("SELECT '{input}'::{pg_type}"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22P02", "{pg_type} {input}: {error}");
    }

    let error = session
        .execute("SELECT '100:00:2b:01:02:03'::macaddr")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22003");

    let error = session
        .execute("SELECT '08:00:2b:11:22:01:02:03'::macaddr8::macaddr")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22003");
}
