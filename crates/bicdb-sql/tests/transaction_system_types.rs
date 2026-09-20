use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn transaction_and_tuple_types_preserve_unsigned_values_arrays_and_ordering() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE transaction_values (
                    id int4 PRIMARY KEY,
                    transaction_id xid NOT NULL,
                    full_transaction_id xid8 NOT NULL,
                    command_id cid NOT NULL,
                    tuple_id tid NOT NULL,
                    transaction_ids xid8[] NOT NULL
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO transaction_values VALUES
                    (1, '-1', '-1', '-1', '(4294967295,65535)',
                     ARRAY['0'::xid8, '18446744073709551615'::xid8]),
                    (2, '0', '0', '0', '(0,1)', ARRAY['1'::xid8])",
            )
            .unwrap();

        let result = session
            .execute(
                "SELECT id, transaction_id::text, full_transaction_id::text,
                        command_id::text, tuple_id::text, transaction_ids::text,
                        pg_typeof(transaction_id), pg_typeof(full_transaction_id),
                        pg_typeof(command_id), pg_typeof(tuple_id)
                 FROM transaction_values
                 ORDER BY full_transaction_id, tuple_id",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], SqlValue::Int(2));
        assert_eq!(result.rows[1][0], SqlValue::Int(1));
        assert_eq!(result.rows[1][1], SqlValue::String("4294967295".into()));
        assert_eq!(
            result.rows[1][2],
            SqlValue::String("18446744073709551615".into())
        );
        assert_eq!(result.rows[1][3], SqlValue::String("4294967295".into()));
        assert_eq!(
            result.rows[1][4],
            SqlValue::String("(4294967295,65535)".into())
        );
        assert_eq!(result.rows[0][5], SqlValue::String("{1}".into()));
        assert_eq!(
            result.rows[1][5],
            SqlValue::String("{0,18446744073709551615}".into())
        );
        assert_eq!(result.rows[1][6], SqlValue::String("xid".into()));
        assert_eq!(result.rows[1][7], SqlValue::String("xid8".into()));
        assert_eq!(result.rows[1][8], SqlValue::String("cid".into()));
        assert_eq!(result.rows[1][9], SqlValue::String("tid".into()));
    }

    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT full_transaction_id::text, tuple_id::text
                 FROM transaction_values WHERE id = 1",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("18446744073709551615".into()),
            SqlValue::String("(4294967295,65535)".into()),
        ]]
    );
}

#[test]
fn user_rows_expose_typed_system_columns_without_expanding_wildcards() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE system_rows (id int4 PRIMARY KEY, value text);
             INSERT INTO system_rows VALUES (1, 'one'), (2, 'two')",
        )
        .unwrap();

    let wildcard = session
        .execute("SELECT * FROM system_rows ORDER BY id")
        .unwrap();
    assert_eq!(wildcard.columns, vec!["id", "value"]);

    let qualified = session
        .execute("SELECT rows.* FROM system_rows AS rows ORDER BY id")
        .unwrap();
    assert_eq!(qualified.columns, vec!["id", "value"]);

    let cte = session
        .execute(
            "WITH rows AS (SELECT source.* FROM system_rows AS source)
             SELECT rows.* FROM rows ORDER BY id",
        )
        .unwrap();
    assert_eq!(cte.columns, vec!["id", "value"]);

    let before = session
        .execute(
            "SELECT id, tableoid::regclass::text, pg_typeof(tableoid),
                    xmin::text, pg_typeof(xmin), xmax::text, pg_typeof(xmax),
                    cmin::text, pg_typeof(cmin), cmax::text, pg_typeof(cmax),
                    ctid::text, pg_typeof(ctid)
             FROM system_rows ORDER BY id",
        )
        .unwrap();
    assert_eq!(before.rows.len(), 2);
    assert_eq!(before.rows[0][1], SqlValue::String("system_rows".into()));
    assert_eq!(before.rows[0][2], SqlValue::String("oid".into()));
    assert_eq!(before.rows[0][4], SqlValue::String("xid".into()));
    assert_eq!(before.rows[0][6], SqlValue::String("xid".into()));
    assert_eq!(before.rows[0][8], SqlValue::String("cid".into()));
    assert_eq!(before.rows[0][10], SqlValue::String("cid".into()));
    assert_eq!(before.rows[0][12], SqlValue::String("tid".into()));
    assert_ne!(before.rows[0][11], before.rows[1][11]);

    let previous_xmin = before.rows[1][3].clone();
    let previous_ctid = before.rows[1][11].clone();
    session
        .execute("UPDATE system_rows SET value = 'updated' WHERE id = 2")
        .unwrap();
    let after = session
        .execute("SELECT xmin::text, xmax::text, cmin::text, cmax::text, ctid::text FROM system_rows WHERE id = 2")
        .unwrap();
    assert_ne!(after.rows[0][0], previous_xmin);
    assert_eq!(after.rows[0][1], SqlValue::String("0".into()));
    assert_eq!(after.rows[0][2], SqlValue::String("0".into()));
    assert_eq!(after.rows[0][3], SqlValue::String("0".into()));
    assert_ne!(after.rows[0][4], previous_ctid);
}

#[test]
fn transaction_and_tuple_types_reject_out_of_range_input() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT '4294967296'::xid",
        "SELECT '-2147483649'::cid",
        "SELECT '18446744073709551616'::xid8",
        "SELECT '-18446744073709551616'::xid8",
        "SELECT '(4294967296,1)'::tid",
        "SELECT '(1,65536)'::tid",
        "SELECT '1'::xid < '2'::xid",
        "SELECT '1'::cid < '2'::cid",
        "CREATE TABLE invalid_system_column (xmin int4)",
    ] {
        assert!(session.execute(sql).is_err(), "{sql}");
    }
}
