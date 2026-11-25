//! PostgreSQL parity for multi-dimensional array subscripts, planner/resource
//! settings, relation comments, catalog privileges, ts_headline overloads,
//! SQL regular expressions (SIMILAR TO), pg_notify and function call forms
//! (named arguments, OUT parameters, VARIADIC). Expected values are the ones
//! PostgreSQL 18 returns; `fixtures/postgres-compat/pgsql-parity/
//! functions-misc.cases` diffs the same statements against a live server.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

fn cells(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    let result = session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    result.rows[0].iter().map(SqlValue::to_cell).collect()
}

fn error_state(session: &mut SqlSession<'_>, sql: &str) -> String {
    match session.execute(sql) {
        Ok(_) => panic!("{sql} succeeded"),
        Err(error) => error.sqlstate().to_string(),
    }
}

#[test]
fn multi_dimensional_subscripts_yield_elements_and_slices_arrays() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("SELECT (ARRAY[[1,2],[3,4]])[2][1], (ARRAY[1,2,3,4])[2:3]")
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Int(3));
    assert_eq!(result.column_types[0].as_deref(), Some("int4"));
    assert_eq!(result.column_types[1].as_deref(), Some("int4[]"));
    assert_eq!(
        cells(
            &mut session,
            "SELECT pg_typeof((ARRAY[[1,2],[3,4]])[2][1]), pg_typeof((ARRAY[1,2,3])[1:2])"
        ),
        ["integer", "integer[]"]
    );
    session.execute("CREATE TABLE md(m int[])").unwrap();
    session
        .execute("INSERT INTO md VALUES ('{{1,2},{3,4}}')")
        .unwrap();
    let result = session.execute("SELECT m[2][1], m[1][2] FROM md").unwrap();
    assert_eq!(result.rows[0], [SqlValue::Int(3), SqlValue::Int(2)]);
    assert_eq!(result.column_types[0].as_deref(), Some("int4"));
}

#[test]
fn planner_and_resource_settings_behave_like_postgres() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(cells(&mut session, "SHOW work_mem"), ["4MB"]);
    session.execute("SET work_mem = '64MB'").unwrap();
    assert_eq!(cells(&mut session, "SHOW work_mem"), ["64MB"]);
    session.execute("SET work_mem = 65536").unwrap();
    assert_eq!(
        cells(&mut session, "SELECT current_setting('work_mem')"),
        ["64MB"]
    );
    assert_eq!(
        cells(&mut session, "SELECT set_config('work_mem', '2048', false)"),
        ["2MB"]
    );
    session.execute("RESET work_mem").unwrap();
    assert_eq!(cells(&mut session, "SHOW work_mem"), ["4MB"]);
    session.execute("SET enable_seqscan = off").unwrap();
    assert_eq!(cells(&mut session, "SHOW enable_seqscan"), ["off"]);
    session.execute("SET random_page_cost = 1.1").unwrap();
    assert_eq!(cells(&mut session, "SHOW random_page_cost"), ["1.1"]);
    session.execute("BEGIN").unwrap();
    session.execute("SET LOCAL work_mem = '32MB'").unwrap();
    assert_eq!(cells(&mut session, "SHOW work_mem"), ["32MB"]);
    session.execute("COMMIT").unwrap();
    assert_eq!(cells(&mut session, "SHOW work_mem"), ["4MB"]);
    for sql in [
        "SET work_mem = '64XB'",
        "SET work_mem = '10kB'",
        "SET enable_seqscan = 'maybe'",
        "SET random_page_cost = -1",
        "SET plan_cache_mode = 'bogus'",
    ] {
        assert_eq!(error_state(&mut session, sql), "22023", "{sql}");
    }
    // Durability is not a planner knob: accepting it without honouring it
    // would be wrong, so it stays refused.
    assert!(session.execute("SET synchronous_commit = off").is_err());
}

#[test]
fn comments_on_tables_and_columns_are_described() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE cmt(a int, b text)").unwrap();
    session
        .execute("COMMENT ON TABLE cmt IS 'the table'")
        .unwrap();
    session
        .execute("COMMENT ON COLUMN cmt.b IS 'column b'")
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT obj_description('cmt'::regclass, 'pg_class'), col_description('cmt'::regclass, 2)"
        ),
        ["the table", "column b"]
    );
    session.execute("COMMENT ON TABLE cmt IS ''").unwrap();
    let result = session
        .execute("SELECT obj_description('cmt'::regclass, 'pg_class')")
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Null);
    assert_eq!(
        error_state(&mut session, "COMMENT ON COLUMN cmt.nope IS 'x'"),
        "42703"
    );
    assert_eq!(
        error_state(&mut session, "COMMENT ON TABLE missing_table IS 'x'"),
        "42P01"
    );
}

#[test]
fn catalog_privileges_and_ts_headline_overloads() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        cells(
            &mut session,
            "SELECT has_table_privilege('pg_class','SELECT'), has_table_privilege('pg_catalog.pg_class','INSERT')"
        ),
        ["true", "true"]
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT ts_headline('english'::regconfig, 'quick fox'::text, to_tsquery('english','fox'))"
        ),
        ["quick <b>fox</b>"]
    );
}

#[test]
fn similar_to_matches_like_postgres() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        cells(
            &mut session,
            "SELECT 'abc' SIMILAR TO 'a%', 'abc' SIMILAR TO 'b%', 'abc' NOT SIMILAR TO 'a_c', \
             'abc' SIMILAR TO '(a|x)(b|y)c', 'a%c' SIMILAR TO 'a#%c' ESCAPE '#'"
        ),
        ["true", "false", "false", "true", "true"]
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT substring('foobar' SIMILAR '%#\"o_b#\"%' ESCAPE '#'), similar_to_escape('a%')"
        ),
        ["oob", "^(?:a.*)$"]
    );
    assert_eq!(
        error_state(&mut session, "SELECT 'a' SIMILAR TO 'a' ESCAPE 'xy'"),
        "22025"
    );
}

#[test]
fn pg_notify_queues_like_notify() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("SELECT pg_notify('ch', 'payload')")
        .unwrap();
    assert_eq!(
        session.take_pending_notifications(),
        [("ch".to_string(), "payload".to_string())]
    );
    assert_eq!(
        error_state(&mut session, "SELECT pg_notify('', 'x')"),
        "22023"
    );
}

#[test]
fn named_and_mixed_arguments_with_defaults() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE FUNCTION fna(a int, b int DEFAULT 1) RETURNS int LANGUAGE sql AS 'SELECT a*10+b'")
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT fna(b => 2, a => 1), fna(a => 3), fna(4, b => 5), fna(6), fna(a := 7, b := 8)"
        ),
        ["12", "31", "45", "61", "78"]
    );
    session
        .execute(
            "CREATE FUNCTION fnp(x text, y text DEFAULT 'd', z text DEFAULT 'z') RETURNS text \
             LANGUAGE plpgsql AS $$BEGIN RETURN x || y || z; END$$",
        )
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT fnp('a', z => 'Z'), fnp(y => 'Y', x => 'X')"
        ),
        ["adZ", "XYz"]
    );
    for sql in [
        "SELECT fna(a => 1, c => 2)",
        "SELECT fna(1, a => 2)",
        "SELECT fna(b => 2)",
    ] {
        assert_eq!(error_state(&mut session, sql), "42883", "{sql}");
    }
}

#[test]
fn out_parameters_return_a_record() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE FUNCTION op(a int, OUT b int, OUT c int) LANGUAGE sql AS 'SELECT a, a*2'")
        .unwrap();
    assert_eq!(
        cells(&mut session, "SELECT (op(1)).c, (op(2)).b"),
        ["2", "2"]
    );
    assert_eq!(cells(&mut session, "SELECT * FROM op(5)"), ["5", "10"]);
    session
        .execute("CREATE FUNCTION op4(a int, OUT b int) LANGUAGE sql AS 'SELECT a * 3'")
        .unwrap();
    assert_eq!(
        cells(&mut session, "SELECT op4(2), pg_typeof(op4(2))"),
        ["6", "integer"]
    );
    session
        .execute(
            "CREATE FUNCTION op5(a int, OUT s int, OUT p int) LANGUAGE plpgsql AS \
             $$BEGIN s := a + a; p := a * a; END$$",
        )
        .unwrap();
    assert_eq!(
        cells(&mut session, "SELECT (op5(3)).p, (op5(3)).s"),
        ["9", "6"]
    );
}

#[test]
fn variadic_parameters_collect_their_arguments() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION va(VARIADIC xs int[]) RETURNS int LANGUAGE sql \
             AS 'SELECT sum(x)::int FROM unnest(xs) x'",
        )
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT va(1,2,3), va(5), va(VARIADIC ARRAY[4,5])"
        ),
        ["6", "5", "9"]
    );
    session
        .execute(
            "CREATE FUNCTION vb(sep text, VARIADIC parts text[]) RETURNS text LANGUAGE sql \
             AS 'SELECT array_to_string(parts, sep)'",
        )
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT vb('-', 'a', 'b', 'c'), vb(',', VARIADIC ARRAY['x','y'])"
        ),
        ["a-b-c", "x,y"]
    );
}
