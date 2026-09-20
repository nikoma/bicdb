use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn inet_casts_and_storage_are_canonical_and_durable() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            session
                .execute(
                    "SELECT '192.000.002.001/024'::inet,
                            '001.002.003.004/00'::inet,
                            '2001:0DB8:0:0:0:0:0:1/64'::inet,
                            '::ffff:192.0.2.1/120'::inet,
                            '::192.0.2.1'::inet",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("192.0.2.1/24".to_string()),
                SqlValue::String("1.2.3.4/0".to_string()),
                SqlValue::String("2001:db8::1/64".to_string()),
                SqlValue::String("::ffff:192.0.2.1/120".to_string()),
                SqlValue::String("::192.0.2.1/128".to_string()),
            ]],
        );

        session
            .execute(
                "CREATE TABLE inet_values (
                    id int4 PRIMARY KEY,
                    ipv4 inet NOT NULL,
                    ipv6 inet NOT NULL,
                    mapped inet NOT NULL
                 )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO inet_values VALUES (
                    1,
                    '192.000.002.001/024',
                    '2001:0DB8:0:0:0:0:0:1/64',
                    '::ffff:c000:201/120'
                 )",
            )
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT ipv4, ipv6, mapped FROM inet_values")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("192.0.2.1/24".to_string()),
                SqlValue::String("2001:db8::1/64".to_string()),
                SqlValue::String("::ffff:192.0.2.1/120".to_string()),
            ]],
        );
    }

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT ipv4, ipv6, mapped FROM inet_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("192.0.2.1/24".to_string()),
            SqlValue::String("2001:db8::1/64".to_string()),
            SqlValue::String("::ffff:192.0.2.1/120".to_string()),
        ]],
    );
}

#[test]
fn inet_rejects_non_postgres_input_with_22p02() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for input in [
        "",
        " 192.0.2.1",
        "192.0.2.1 ",
        "192.0.2.1/33",
        "192.0.2.1/+1",
        "192.0.2.1/ 24",
        "192.168.1",
        "256.0.0.1",
        "2001:db8::1/064",
        "2001:db8::1/129",
        "fe80::1%eth0",
    ] {
        let escaped = input.replace('\'', "''");
        let error = session
            .execute(&format!("SELECT '{escaped}'::inet"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22P02", "input: {input:?}: {error}");
    }
}
