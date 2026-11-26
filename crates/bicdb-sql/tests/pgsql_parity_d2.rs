//! PostgreSQL parity for set-returning functions in select lists, partial
//! array subscripts, SQL cursors and SQL/JSON. Expected values are the ones
//! PostgreSQL 18 returns; `fixtures/postgres-compat/pgsql-parity/d2.cases`
//! diffs the same statements against a live server.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

fn error_state(session: &mut SqlSession<'_>, sql: &str) -> String {
    match session.execute(sql) {
        Ok(_) => panic!("{sql} succeeded"),
        Err(error) => error.sqlstate().to_string(),
    }
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

#[test]
fn set_returning_functions_in_select_lists_project_sets() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE FUNCTION srf(n int) RETURNS SETOF int LANGUAGE sql AS 'SELECT generate_series(1, n)'")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION rt(n int) RETURNS TABLE(i int, sq int) LANGUAGE sql \
             AS 'SELECT g, g*g FROM generate_series(1, n) g'",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT srf(3)"),
        strings(&[&["1"], &["2"], &["3"]])
    );
    assert_eq!(
        rows(&mut session, "SELECT (rt(3)).sq"),
        strings(&[&["1"], &["4"], &["9"]])
    );
    assert_eq!(
        rows(&mut session, "SELECT srf(3) AS a, srf(1) AS b"),
        strings(&[&["1", "1"], &["2", ""], &["3", ""]])
    );
    assert_eq!(
        rows(&mut session, "SELECT generate_series(1,3) * 10 AS x"),
        strings(&[&["10"], &["20"], &["30"]])
    );
    session.execute("CREATE TABLE t(id int)").unwrap();
    session.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, srf(id) FROM t ORDER BY id, 2"),
        strings(&[&["1", "1"], &["2", "1"], &["2", "2"]])
    );
    assert!(rows(&mut session, "SELECT 1 AS k, srf(0)").is_empty());
    session
        .execute(
            "CREATE FUNCTION srfp(n int) RETURNS SETOF int LANGUAGE plpgsql AS \
             $$BEGIN FOR k IN 1..n LOOP RETURN NEXT k * 2; END LOOP; END$$",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT srfp(2)"),
        strings(&[&["2"], &["4"]])
    );
}

#[test]
fn partial_subscripts_of_multi_dimensional_arrays_are_null() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("SELECT (ARRAY[[1,2],[3,4]])[2], (ARRAY['[1,2]'::json])[1]")
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Null);
    assert_eq!(result.rows[0][1].to_cell(), "[1,2]");
}

#[test]
fn cursors_fetch_move_close_and_end_with_their_transaction() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        error_state(&mut session, "DECLARE c0 CURSOR FOR SELECT 1"),
        "25P01"
    );
    session
        .execute("CREATE TABLE cur(id int PRIMARY KEY, v text)")
        .unwrap();
    session
        .execute("INSERT INTO cur VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("DECLARE c SCROLL CURSOR FOR SELECT id FROM cur ORDER BY id")
        .unwrap();
    assert_eq!(
        rows(&mut session, "FETCH 2 FROM c"),
        strings(&[&["1"], &["2"]])
    );
    assert_eq!(rows(&mut session, "FETCH PRIOR FROM c"), strings(&[&["1"]]));
    assert_eq!(rows(&mut session, "FETCH LAST FROM c"), strings(&[&["3"]]));
    session.execute("MOVE ABSOLUTE 0 IN c").unwrap();
    assert_eq!(
        rows(&mut session, "FETCH ALL FROM c"),
        strings(&[&["1"], &["2"], &["3"]])
    );
    session
        .execute("DECLARE u CURSOR FOR SELECT id, v FROM cur ORDER BY id FOR UPDATE")
        .unwrap();
    session.execute("FETCH 2 FROM u").unwrap();
    session
        .execute("UPDATE cur SET v = 'X' WHERE CURRENT OF u")
        .unwrap();
    session
        .execute("DECLARE h CURSOR WITH HOLD FOR SELECT v FROM cur ORDER BY id")
        .unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(error_state(&mut session, "FETCH c"), "34000");
    assert_eq!(
        rows(&mut session, "FETCH ALL FROM h"),
        strings(&[&["a"], &["X"], &["c"]])
    );
    assert_eq!(
        rows(&mut session, "SELECT name, is_holdable FROM pg_cursors"),
        strings(&[&["h", "true"]])
    );
    session.execute("CLOSE h").unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("DECLARE r CURSOR FOR SELECT 1").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(error_state(&mut session, "FETCH r"), "34000");
}

#[test]
fn sql_json_query_functions_and_json_table() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT JSON_VALUE('{\"a\":{\"b\":5}}', '$.a.b'), \
             JSON_VALUE('{\"a\":1}', '$.zz' DEFAULT 'd' ON EMPTY), \
             JSON_VALUE('{\"a\":\"12\"}', '$.a' RETURNING int) + 1, \
             JSON_QUERY('{\"a\":[1,2]}', '$.a[*]' WITH WRAPPER), \
             JSON_EXISTS('{\"a\":[1,2,3]}', '$.a[*] ? (@ > 2)')"
        ),
        strings(&[&["5", "d", "13", "[1,2]", "true"]])
    );
    assert_eq!(
        error_state(
            &mut session,
            "SELECT JSON_VALUE('{\"a\":[1,2]}', '$.a' ERROR ON ERROR)"
        ),
        "2203F"
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM JSON_TABLE('[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]', '$[*]' \
             COLUMNS (id FOR ORDINALITY, a int PATH '$.a', b text PATH '$.b'))"
        ),
        strings(&[&["1", "1", "x"], &["2", "2", "y"]])
    );
    session
        .execute("CREATE TABLE docs(id int, doc jsonb)")
        .unwrap();
    session
        .execute(
            "INSERT INTO docs VALUES (1, '{\"items\":[{\"n\":\"p\"},{\"n\":\"r\"}]}'), (2, '{\"items\":[]}')",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT docs.id, j.n FROM docs, JSON_TABLE(docs.doc, '$.items[*]' COLUMNS (n text PATH '$.n')) AS j ORDER BY 1, 2"
        ),
        strings(&[&["1", "p"], &["1", "r"]])
    );
}
