use bicdb_core::BicDb;
use bicdb_sql::{infer_query_result_types, SqlSession, SqlValue};

fn lsn_value(value: &SqlValue) -> u64 {
    let SqlValue::String(value) = value else {
        panic!("expected pg_lsn text, got {value:?}");
    };
    let (high, low) = value.split_once('/').unwrap();
    (u64::from(u32::from_str_radix(high, 16).unwrap()) << 32)
        | u64::from(u32::from_str_radix(low, 16).unwrap())
}

#[test]
fn pg_lsn_arithmetic_comparison_arrays_and_catalogs_match_postgres() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();

    assert_eq!(
        infer_query_result_types(
            &db,
            "SELECT '0/1'::pg_lsn + 1.4,
                    2 + '0/1'::pg_lsn,
                    '0/10'::pg_lsn - 2.5,
                    '0/10'::pg_lsn - '0/1'::pg_lsn",
        )
        .unwrap(),
        Some(vec![
            Some("pg_lsn".into()),
            Some("pg_lsn".into()),
            Some("pg_lsn".into()),
            Some("numeric".into()),
        ])
    );

    let mut session = SqlSession::new(&mut db);

    let arithmetic = session
        .execute(
            "SELECT '0/1'::pg_lsn + 1.4,
                    '0/1'::pg_lsn + 1.5,
                    '0/1'::pg_lsn + 1.6,
                    '0/10'::pg_lsn + 2.5,
                    '0/10'::pg_lsn + 3.5,
                    '0/10'::pg_lsn + (-1.5),
                    '0/10'::pg_lsn + (-2.5),
                    '0/1'::pg_lsn - '0/10'::pg_lsn,
                    pg_typeof('0/1'::pg_lsn - '0/10'::pg_lsn)",
        )
        .unwrap();
    assert_eq!(
        arithmetic.rows,
        vec![vec![
            SqlValue::String("0/2".into()),
            SqlValue::String("0/3".into()),
            SqlValue::String("0/3".into()),
            SqlValue::String("0/13".into()),
            SqlValue::String("0/14".into()),
            SqlValue::String("0/F".into()),
            SqlValue::String("0/E".into()),
            SqlValue::String("-15".into()),
            SqlValue::String("numeric".into()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT 'F/FFFFFFFF'::pg_lsn < '10/0'::pg_lsn,
                        'FFFFFFFF/FFFFFFFF'::pg_lsn - '0/0'::pg_lsn",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::String("18446744073709551615".into()),
        ]]
    );

    session
        .execute(
            "CREATE TABLE lsn_values (
                id int4 PRIMARY KEY,
                position pg_lsn NOT NULL,
                positions pg_lsn[] NOT NULL
             );
             INSERT INTO lsn_values VALUES
                (1, '10/0', ARRAY['0/1'::pg_lsn, 'FFFFFFFF/FFFFFFFF'::pg_lsn]),
                (2, 'F/FFFFFFFF', ARRAY['0/2'::pg_lsn])",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id, position::text, positions::text
                 FROM lsn_values ORDER BY position",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(2),
                SqlValue::String("F/FFFFFFFF".into()),
                SqlValue::String("{0/2}".into()),
            ],
            vec![
                SqlValue::Int(1),
                SqlValue::String("10/0".into()),
                SqlValue::String("{0/1,FFFFFFFF/FFFFFFFF}".into()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT oid, typlen, typbyval, typalign, typstorage, typarray
                 FROM pg_type WHERE typname = 'pg_lsn'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(3220),
            SqlValue::Int(8),
            SqlValue::Bool(true),
            SqlValue::String("d".into()),
            SqlValue::String("p".into()),
            SqlValue::Int(3221),
        ]]
    );
}

#[test]
fn pg_lsn_wal_functions_follow_durable_commit_positions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let before = session
        .execute(
            "SELECT pg_current_wal_lsn(), pg_current_wal_insert_lsn(), pg_current_wal_flush_lsn()",
        )
        .unwrap();
    assert_eq!(before.column_types, vec![Some("pg_lsn".into()); 3]);
    assert_eq!(before.rows[0][0], before.rows[0][1]);
    assert_eq!(before.rows[0][1], before.rows[0][2]);

    session
        .execute("CREATE TABLE wal_position_rows (id int4 PRIMARY KEY); INSERT INTO wal_position_rows VALUES (1)")
        .unwrap();
    let after = session
        .execute(
            "SELECT pg_current_wal_lsn(),
                    pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'::pg_lsn),
                    pg_typeof(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'::pg_lsn)),
                    pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(),
                    pg_is_in_recovery()",
        )
        .unwrap();
    assert!(lsn_value(&after.rows[0][0]) > lsn_value(&before.rows[0][0]));
    assert_eq!(
        after.rows[0][1],
        SqlValue::String(lsn_value(&after.rows[0][0]).to_string())
    );
    assert_eq!(after.rows[0][2], SqlValue::String("numeric".into()));
    assert_eq!(after.rows[0][3], SqlValue::Null);
    assert_eq!(after.rows[0][4], SqlValue::Null);
    assert_eq!(after.rows[0][5], SqlValue::Bool(false));

    let committed_lsn = lsn_value(&after.rows[0][0]);
    drop(session);
    drop(db);

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut reopened_session = SqlSession::new(&mut reopened);
    let recovered = reopened_session
        .execute("SELECT pg_current_wal_lsn()")
        .unwrap();
    assert!(lsn_value(&recovered.rows[0][0]) >= committed_lsn);
}

#[test]
fn pg_lsn_rejects_invalid_input_and_out_of_range_arithmetic() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT 'G/0'::pg_lsn",
        "SELECT '1/2/3'::pg_lsn",
        "SELECT 'FFFFFFFF/FFFFFFFF'::pg_lsn + 1",
        "SELECT '0/0'::pg_lsn - 1",
    ] {
        assert!(session.execute(sql).is_err(), "{sql}");
    }
}
