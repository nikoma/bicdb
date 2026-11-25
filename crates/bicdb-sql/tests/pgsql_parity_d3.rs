//! PostgreSQL parity for user-defined aggregates and casts, ROWS FROM,
//! ORDER BY ... USING and cursors under savepoints. Expected values are the
//! ones PostgreSQL 18 returns; `fixtures/postgres-compat/pgsql-parity/
//! d3.cases` diffs the same statements against a live server.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

fn error_state(session: &mut SqlSession<'_>, sql: &str) -> String {
    match session.execute(sql) {
        Ok(_) => panic!("{sql} succeeded"),
        Err(error) => error.sqlstate().to_string(),
    }
}

#[test]
fn user_aggregates_fold_with_grouping_filter_order_and_windows() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE FUNCTION add_sq(s int, x int) RETURNS int LANGUAGE sql AS 'SELECT s + x*x'",
            )
            .unwrap();
        session
            .execute("CREATE AGGREGATE sumsq(int) (sfunc = add_sq, stype = int, initcond = '0')")
            .unwrap();
        assert_eq!(
            rows(&mut session, "SELECT sumsq(x) FROM generate_series(1,3) x"),
            strings(&[&["14"]])
        );
        session.execute("CREATE TABLE t(g text, v int)").unwrap();
        session
            .execute("INSERT INTO t VALUES ('a',1),('a',2),('b',3)")
            .unwrap();
        assert_eq!(
            rows(
                &mut session,
                "SELECT g, sumsq(v) FROM t GROUP BY g ORDER BY g"
            ),
            strings(&[&["a", "5"], &["b", "9"]])
        );
        assert_eq!(
            rows(
                &mut session,
                "SELECT x, sumsq(x) OVER (ORDER BY x) FROM generate_series(1,3) x"
            ),
            strings(&[&["1", "1"], &["2", "5"], &["3", "14"]])
        );
        session
            .execute(
                "CREATE FUNCTION cat_acc(s text, x text) RETURNS text LANGUAGE sql \
                 AS 'SELECT CASE WHEN s IS NULL THEN x ELSE s || '','' || x END'",
            )
            .unwrap();
        session
            .execute("CREATE AGGREGATE mycat(text) (sfunc = cat_acc, stype = text)")
            .unwrap();
        assert_eq!(
            rows(
                &mut session,
                "SELECT mycat(x ORDER BY x DESC), mycat(x) FILTER (WHERE x <> 'b') \
                 FROM (VALUES ('a'),('c'),('b')) v(x)"
            )[0][0],
            "c,b,a"
        );
        assert_eq!(
            rows(
                &mut session,
                "SELECT aggtransfn = 'add_sq'::regproc, agginitval FROM pg_aggregate WHERE aggfnoid = 'sumsq'::regproc"
            ),
            strings(&[&["true", "0"]])
        );
        assert_eq!(
            error_state(
                &mut session,
                "CREATE AGGREGATE sumsq(int) (sfunc = add_sq, stype = int)"
            ),
            "42723"
        );
    }
    // The aggregate survives a restart.
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(&mut session, "SELECT sumsq(x) FROM generate_series(1,2) x"),
        strings(&[&["5"]])
    );
    session.execute("DROP AGGREGATE sumsq(int)").unwrap();
    assert_eq!(
        error_state(&mut session, "DROP AGGREGATE sumsq(int)"),
        "42883"
    );
}

#[test]
fn user_casts_convert_explicit_and_are_catalogued() {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TYPE money2 AS (cents int)")
        .unwrap();
    session
        .execute("CREATE FUNCTION int_to_m2(i int) RETURNS money2 LANGUAGE sql AS 'SELECT ROW(i * 100)::money2'")
        .unwrap();
    session
        .execute("CREATE CAST (int AS money2) WITH FUNCTION int_to_m2(int) AS ASSIGNMENT")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT (5::money2).cents"),
        strings(&[&["500"]])
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT castcontext, castmethod FROM pg_cast WHERE casttarget = 'money2'::regtype"
        ),
        strings(&[&["a", "f"]])
    );
    assert_eq!(
        error_state(
            &mut session,
            "CREATE CAST (int AS money2) WITH FUNCTION int_to_m2(int)"
        ),
        "42710"
    );
    session.execute("DROP CAST (int AS money2)").unwrap();
    assert_eq!(
        error_state(&mut session, "DROP CAST (int AS money2)"),
        "42704"
    );
}

#[test]
fn rows_from_order_by_using_and_cursor_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM ROWS FROM (generate_series(1,3), unnest(ARRAY['a','b'])) AS t(n, s)"
        ),
        strings(&[&["1", "a"], &["2", "b"], &["3", ""]])
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT x FROM generate_series(1,3) x ORDER BY x USING >"
        ),
        strings(&[&["3"], &["2"], &["1"]])
    );
    session.execute("BEGIN").unwrap();
    session.execute("DECLARE s1 CURSOR FOR SELECT 1").unwrap();
    session.execute("SAVEPOINT sp").unwrap();
    session.execute("DECLARE s2 CURSOR FOR SELECT 2").unwrap();
    session.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT name FROM pg_cursors"),
        strings(&[&["s1"]])
    );
    assert_eq!(error_state(&mut session, "FETCH s2"), "34000");
    session.execute("ROLLBACK").unwrap();
}
