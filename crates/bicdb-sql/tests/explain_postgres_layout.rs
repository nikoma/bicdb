use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn plan(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(SqlValue::String(line)) => line,
            other => panic!("unexpected plan row {other:?}"),
        })
        .collect()
}

#[test]
fn explain_option_lists_render_postgres_plan_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ex_t (a int, b text, c numeric)")
        .unwrap();

    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (COSTS OFF) SELECT * FROM ex_t WHERE a = 1"
        ),
        vec!["Seq Scan on ex_t", "  Filter: (a = 1)"]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (COSTS OFF) SELECT * FROM ex_t WHERE a > 1 AND b = 'x' ORDER BY b DESC LIMIT 2"
        ),
        vec![
            "Limit",
            "  ->  Sort",
            "        Sort Key: b DESC",
            "        ->  Seq Scan on ex_t",
            "              Filter: ((a > 1) AND (b = 'x'::text))",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM ex_t WHERE c = 1"
        ),
        vec![
            "Aggregate",
            "  ->  Seq Scan on ex_t",
            "        Filter: (c = '1'::numeric)",
        ]
    );
    assert_eq!(
        plan(&mut session, "EXPLAIN (COSTS OFF) SELECT 1"),
        vec!["Result"]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (COSTS OFF) UPDATE ex_t SET b = 'z' WHERE a IN (1, 2)"
        ),
        vec![
            "Update on ex_t",
            "  ->  Seq Scan on ex_t",
            "        Filter: (a = ANY ('{1,2}'::integer[]))",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (COSTS OFF) INSERT INTO ex_t VALUES (1, 'a', 1), (2, 'b', 2)"
        ),
        vec!["Insert on ex_t", "  ->  Values Scan on \"*VALUES*\""]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (COSTS OFF) SELECT a FROM ex_t UNION ALL SELECT a FROM ex_t"
        ),
        vec![
            "Append",
            "  ->  Seq Scan on ex_t",
            "  ->  Seq Scan on ex_t ex_t_1",
        ]
    );
    // EXPLAIN without ANALYZE writes nothing.
    let count = session.execute("SELECT count(*) FROM ex_t").unwrap();
    assert_eq!(count.rows, vec![vec![SqlValue::Int(0)]]);

    let json = plan(
        &mut session,
        "EXPLAIN (COSTS OFF, FORMAT JSON) SELECT * FROM ex_t WHERE a = 1",
    );
    assert_eq!(json.len(), 1);
    let document: serde_json::Value = serde_json::from_str(&json[0]).unwrap();
    assert_eq!(document[0]["Plan"]["Node Type"], "Seq Scan");
    assert_eq!(document[0]["Plan"]["Relation Name"], "ex_t");
    assert_eq!(document[0]["Plan"]["Filter"], "(a = 1)");

    for (sql, sqlstate, message) in [
        (
            "EXPLAIN (BOGUS) SELECT 1",
            "42601",
            "unrecognized EXPLAIN option \"bogus\"",
        ),
        (
            "EXPLAIN (FORMAT CSV) SELECT 1",
            "22023",
            "unrecognized value for EXPLAIN option \"format\": \"csv\"",
        ),
        (
            "EXPLAIN (TIMING) SELECT 1",
            "22023",
            "EXPLAIN option TIMING requires ANALYZE",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}");
        assert!(error.to_string().contains(message), "{sql}: {error}");
    }

    // EXPLAIN ANALYZE runs the statement and reports its rows.
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) INSERT INTO ex_t VALUES (7, 'q', 1)"
        ),
        vec![
            "Insert on ex_t (actual rows=0.00 loops=1)",
            "  ->  Result (actual rows=1.00 loops=1)",
        ]
    );
    let count = session.execute("SELECT count(*) FROM ex_t").unwrap();
    assert_eq!(count.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn bare_explain_select_keeps_the_native_access_report() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE ex_native (a int)").unwrap();
    let lines = plan(&mut session, "EXPLAIN SELECT * FROM ex_native");
    assert_eq!(lines[0], "FullScan");
    // A statement kind the native report does not cover uses the PostgreSQL layout.
    let lines = plan(&mut session, "EXPLAIN DELETE FROM ex_native WHERE a = 1");
    assert_eq!(lines[0], "Delete on ex_native");
}

#[test]
fn explain_requires_the_privileges_of_the_explained_statement() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE ex_secret (a int)").unwrap();
    session.execute("CREATE ROLE ex_reader LOGIN").unwrap();
    session.execute("SET ROLE ex_reader").unwrap();
    for sql in [
        "EXPLAIN (COSTS OFF) SELECT * FROM ex_secret",
        "EXPLAIN (COSTS OFF) DELETE FROM ex_secret",
        "EXPLAIN (COSTS OFF) INSERT INTO ex_secret VALUES (1)",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{sql}: {error}");
    }
}
