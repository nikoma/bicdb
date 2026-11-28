use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn seeded_random_functions_share_a_connection_stream_across_statements() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT setseed(0.25)")
            .unwrap()
            .column_types,
        vec![Some("void".into())]
    );
    let result = session
        .execute("SELECT random(), random(1,10), random(1.0,2.0), random_normal(5,1)")
        .unwrap();
    assert_eq!(
        result.column_types,
        vec![
            Some("float8".into()),
            Some("int4".into()),
            Some("numeric".into()),
            Some("float8".into())
        ]
    );
    let [SqlValue::Float(unit), SqlValue::Int(integer), SqlValue::String(numeric), SqlValue::Float(normal)] =
        result.rows[0].as_slice()
    else {
        panic!("{result:?}");
    };
    assert_eq!(*unit, 0.19726358498438978);
    assert_eq!(*integer, 4);
    assert_eq!(numeric, "1.2");
    assert!((normal - 4.632018974786062).abs() < 1e-14);

    session.execute("SELECT setseed(0.25)").unwrap();
    let connection_state = session.session_guc_state();
    drop(session);

    let mut unrelated = SqlSession::new(&mut db);
    unrelated.execute("SELECT setseed(0.5)").unwrap();
    unrelated.execute("SELECT random()").unwrap();
    drop(unrelated);

    let mut session = SqlSession::new(&mut db).with_session_guc_state(connection_state);
    assert_eq!(
        session.execute("SELECT random()").unwrap().rows,
        vec![vec![SqlValue::Float(0.19726358498438978)]]
    );
    session.execute("BEGIN").unwrap();
    session.execute("SELECT setseed(0.25)").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute("SELECT random()").unwrap().rows,
        vec![vec![SqlValue::Float(0.19726358498438978)]]
    );
}

#[test]
fn random_range_resolves_types_and_handles_nulls_and_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT random(42,42), random(42::bigint,42),
                    random(1.2,1.20000), random_normal(-10,0),
                    random(NULL::integer,0), random_normal(NULL)",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(42),
            SqlValue::Int(42),
            SqlValue::String("1.20000".into()),
            SqlValue::Float(-10.0),
            SqlValue::Null,
            SqlValue::Null,
        ]]
    );
    for (sql, state) in [
        ("SELECT random(10,1)", "22023"),
        ("SELECT random('NaN'::numeric,1)", "22023"),
        ("SELECT random(1,'Infinity'::numeric)", "22023"),
        ("SELECT random('1','2')", "42725"),
        ("SELECT random(1::smallint,2::smallint)", "42725"),
        ("SELECT random(1::float8,2::float8)", "42883"),
        ("SELECT setseed(2)", "22023"),
    ] {
        let error = session.execute(sql).expect_err(sql);
        assert_eq!(error.sqlstate(), state, "{sql}: {error}");
    }
}
