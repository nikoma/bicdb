use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

#[test]
fn cidr_casts_and_storage_are_canonical_and_durable() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            session
                .execute(
                    "SELECT '10'::cidr,
                            '10.1/24'::cidr,
                            '192.000.002.000/024'::cidr,
                            '2001:0DB8:0:0:0:0:0:0/32'::cidr,
                            '::ffff:c000:200/120'::cidr",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("10.0.0.0/8".to_string()),
                SqlValue::String("10.1.0.0/24".to_string()),
                SqlValue::String("192.0.2.0/24".to_string()),
                SqlValue::String("2001:db8::/32".to_string()),
                SqlValue::String("::ffff:192.0.2.0/120".to_string()),
            ]],
        );

        session
            .execute(
                "CREATE TABLE cidr_values (
                    id int4 PRIMARY KEY,
                    ipv4 cidr NOT NULL,
                    ipv6 cidr NOT NULL,
                    networks cidr[] NOT NULL
                 )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO cidr_values VALUES (
                    1,
                    '192.000.002/024',
                    '2001:0DB8:0:0:0:0:0:0/32',
                    '{10,192.000.002/024,::ffff:c000:200/120}'::cidr[]
                 )",
            )
            .unwrap();
        assert_cidr_row(&mut session);
    }

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_cidr_row(&mut SqlSession::new(&mut reopened));
}

fn assert_cidr_row(session: &mut SqlSession<'_>) {
    assert_eq!(
        session
            .execute("SELECT ipv4, ipv6, networks FROM cidr_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("192.0.2.0/24".to_string()),
            SqlValue::String("2001:db8::/32".to_string()),
            SqlValue::Json(json!([
                "10.0.0.0/8",
                "192.0.2.0/24",
                "::ffff:192.0.2.0/120"
            ])),
        ]],
    );
}

#[test]
fn cidr_rejects_host_bits_and_invalid_input_with_22p02() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for input in [
        "",
        " 192.0.2.0/24",
        "192.0.2.0/24 ",
        "192.0.2.1/24",
        "10.1/8",
        "192.0.2.0/33",
        "256.0.0.0/24",
        "2001:db8::1/64",
        "2001:db8::/064",
        "fe80::%eth0/64",
    ] {
        let escaped = input.replace('\'', "''");
        let error = session
            .execute(&format!("SELECT '{escaped}'::cidr"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22P02", "input: {input:?}: {error}");
    }
}
