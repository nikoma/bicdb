use bicdb_core::BicDb;
use bicdb_sql::{infer_query_result_types, SqlSession, SqlValue};

#[test]
fn snapshot_function_types_are_available_without_execution() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();

    assert_eq!(
        infer_query_result_types(
            &db,
            "SELECT pg_current_snapshot(), txid_current_snapshot(),
                    pg_snapshot_xmin('10:20:10,14,15'),
                    txid_snapshot_xmax('10:20:10,14,15'),
                    pg_visible_in_snapshot('11'::xid8, '10:20:10,14,15')",
        )
        .unwrap(),
        Some(vec![
            Some("pg_snapshot".into()),
            Some("txid_snapshot".into()),
            Some("xid8".into()),
            Some("int8".into()),
            Some("bool".into()),
        ])
    );
}

#[test]
fn pg_snapshot_storage_functions_and_catalogs_match_postgres() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let canonical = session
        .execute(
            "SELECT '010:020:010,010,014,'::pg_snapshot::text,
                    '010:020:010,010,014,'::txid_snapshot::text",
        )
        .unwrap();
    assert_eq!(
        canonical.rows,
        vec![vec![
            SqlValue::String("10:20:10,14".into()),
            SqlValue::String("10:20:10,14".into()),
        ]]
    );

    let accessors = session
        .execute(
            "SELECT pg_snapshot_xmin('10:20:10,14,15'),
                    pg_snapshot_xmax('10:20:10,14,15'),
                    txid_snapshot_xmin('10:20:10,14,15'),
                    txid_snapshot_xmax('10:20:10,14,15'),
                    pg_visible_in_snapshot('9'::xid8, '10:20:10,14,15'),
                    pg_visible_in_snapshot('10'::xid8, '10:20:10,14,15'),
                    pg_visible_in_snapshot('11'::xid8, '10:20:10,14,15'),
                    pg_visible_in_snapshot('20'::xid8, '10:20:10,14,15')",
        )
        .unwrap();
    assert_eq!(
        accessors.column_types,
        vec![
            Some("xid8".into()),
            Some("xid8".into()),
            Some("int8".into()),
            Some("int8".into()),
            Some("bool".into()),
            Some("bool".into()),
            Some("bool".into()),
            Some("bool".into()),
        ]
    );
    assert_eq!(
        accessors.rows,
        vec![vec![
            SqlValue::String("10".into()),
            SqlValue::String("20".into()),
            SqlValue::Int(10),
            SqlValue::Int(20),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );

    let xip = session
        .execute(
            "SELECT pg_snapshot_xip('10:20:10,14,15'::pg_snapshot),
                    txid_snapshot_xip('10:20:10,14,15'::txid_snapshot)",
        )
        .unwrap();
    assert_eq!(
        xip.column_types,
        vec![Some("xid8".into()), Some("int8".into())]
    );
    assert_eq!(
        xip.rows,
        vec![
            vec![SqlValue::String("10".into()), SqlValue::Int(10)],
            vec![SqlValue::String("14".into()), SqlValue::Int(14)],
            vec![SqlValue::String("15".into()), SqlValue::Int(15)],
        ]
    );

    session
        .execute(
            "CREATE TABLE snapshot_values (
                id int4 PRIMARY KEY,
                captured pg_snapshot NOT NULL,
                legacy txid_snapshot NOT NULL,
                history pg_snapshot[] NOT NULL
             );
             INSERT INTO snapshot_values VALUES
                (1, '10:20:10,14,15', '10:20:10,14,15',
                 ARRAY['10:20:10,14,15'::pg_snapshot, '20:20:'::pg_snapshot])",
        )
        .unwrap();
    drop(session);
    drop(db);

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut reopened_session = SqlSession::new(&mut reopened);
    assert_eq!(
        reopened_session
            .execute("SELECT captured::text, legacy::text, history::text FROM snapshot_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("10:20:10,14,15".into()),
            SqlValue::String("10:20:10,14,15".into()),
            SqlValue::String("{\"10:20:10,14,15\",20:20:}".into()),
        ]]
    );
    assert_eq!(
        reopened_session
            .execute(
                "SELECT oid, typname, typlen, typbyval, typalign, typstorage, typarray
                 FROM pg_type
                 WHERE typname IN ('pg_snapshot', 'txid_snapshot')
                 ORDER BY oid",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(2970),
                SqlValue::String("txid_snapshot".into()),
                SqlValue::Int(-1),
                SqlValue::Bool(false),
                SqlValue::String("d".into()),
                SqlValue::String("x".into()),
                SqlValue::Int(2949),
            ],
            vec![
                SqlValue::Int(5038),
                SqlValue::String("pg_snapshot".into()),
                SqlValue::Int(-1),
                SqlValue::Bool(false),
                SqlValue::String("d".into()),
                SqlValue::String("x".into()),
                SqlValue::Int(5039),
            ],
        ]
    );
}

#[test]
fn current_snapshots_follow_the_transaction_visibility_watermark() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("BEGIN").unwrap();
    let before = session
        .execute(
            "SELECT pg_current_snapshot(), txid_current_snapshot(),
                    pg_typeof(pg_current_snapshot()),
                    pg_typeof(txid_current_snapshot())",
        )
        .unwrap();
    assert_eq!(before.rows[0][0], before.rows[0][1]);
    assert_eq!(before.rows[0][2], SqlValue::String("pg_snapshot".into()));
    assert_eq!(before.rows[0][3], SqlValue::String("txid_snapshot".into()));
    session
        .execute("CREATE TABLE snapshot_commit_probe (id int4 PRIMARY KEY); INSERT INTO snapshot_commit_probe VALUES (1)")
        .unwrap();
    let still_fixed = session.execute("SELECT pg_current_snapshot()").unwrap();
    assert_eq!(still_fixed.rows[0][0], before.rows[0][0]);
    session.execute("COMMIT").unwrap();

    let after = session.execute("SELECT pg_current_snapshot()").unwrap();
    assert_ne!(after.rows[0][0], before.rows[0][0]);
}

#[test]
fn pg_snapshot_nulls_and_invalid_inputs_fail_like_postgres() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT pg_snapshot_xmin(NULL::pg_snapshot),
                        pg_snapshot_xmax(NULL::pg_snapshot),
                        pg_visible_in_snapshot(NULL::xid8, '10:20:'::pg_snapshot),
                        pg_visible_in_snapshot('11'::xid8, NULL::pg_snapshot)",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null; 4]]
    );
    assert!(session
        .execute("SELECT pg_snapshot_xip(NULL::pg_snapshot)")
        .unwrap()
        .rows
        .is_empty());

    for sql in [
        "SELECT '0:1:'::pg_snapshot",
        "SELECT '20:10:'::pg_snapshot",
        "SELECT '10:20:14,11'::pg_snapshot",
        "SELECT '10:20:9'::pg_snapshot",
        "SELECT '10:20:20'::pg_snapshot",
        "SELECT ' 10:20:11 '::pg_snapshot",
    ] {
        assert!(session.execute(sql).is_err(), "{sql}");
    }

    session
        .execute(
            "CREATE TABLE snapshot_ops (
                id int4 PRIMARY KEY,
                captured pg_snapshot NOT NULL,
                legacy txid_snapshot NOT NULL
             );
             INSERT INTO snapshot_ops VALUES
                (1, '10:20:10,14', '10:20:10,14'),
                (2, '20:20:', '20:20:')",
        )
        .unwrap();
    for sql in [
        "SELECT '10:20:'::pg_snapshot = '10:20:'::pg_snapshot",
        "SELECT * FROM snapshot_ops WHERE captured = '10:20:10,14'::pg_snapshot",
        "SELECT captured FROM snapshot_ops ORDER BY captured",
        "SELECT DISTINCT captured FROM snapshot_ops",
        "SELECT captured, count(*) FROM snapshot_ops GROUP BY captured",
        "CREATE INDEX snapshot_ops_captured_idx ON snapshot_ops(captured)",
        "CREATE UNIQUE INDEX snapshot_ops_legacy_idx ON snapshot_ops(legacy)",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert!(
            matches!(error.sqlstate(), "42883" | "42704"),
            "{sql}: {error}"
        );
    }
}
