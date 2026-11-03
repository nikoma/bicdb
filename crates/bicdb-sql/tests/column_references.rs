//! Column references resolve like PostgreSQL: a name no relation in scope
//! provides is an error (42703; 42P01 for an unknown qualifier), not NULL.
//! Before this, a misspelled column in WHERE/JOIN/ORDER BY/GROUP BY, or any
//! reference over a derived table, CTE or table function, evaluated to NULL
//! and `UPDATE/DELETE ... WHERE typo` silently changed nothing.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn session_with_fixture(db: &mut BicDb) -> SqlSession<'_> {
    let mut session = SqlSession::new(db);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, data JSONB)",
        "CREATE TABLE u (id INT PRIMARY KEY, t_id INT, tags TEXT[])",
        "INSERT INTO t VALUES (1, 'a', '{\"k\": [1, 2]}')",
        "INSERT INTO u VALUES (1, 1, ARRAY['x', 'y'])",
        "CREATE FUNCTION rr(a TEXT) RETURNS TABLE (x TEXT) LANGUAGE sql STABLE AS $f$ SELECT a $f$",
        "CREATE VIEW tv AS SELECT id, name FROM t",
        "CREATE TYPE contact AS (name TEXT, age INT)",
        "CREATE TABLE c (id INT PRIMARY KEY, person contact)",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    session
}

#[test]
fn unknown_column_references_are_errors() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = session_with_fixture(&mut db);
    for (sql, sqlstate) in [
        ("SELECT t.nosuch FROM t", "42703"),
        ("SELECT id FROM t WHERE nosuch = 1", "42703"),
        ("SELECT t.id FROM t JOIN u ON u.t_id = t.id WHERE u.nosuch = 1", "42703"),
        ("SELECT t.id FROM t JOIN u ON u.nosuch = t.id", "42703"),
        ("SELECT id FROM t ORDER BY nosuch", "42703"),
        ("SELECT count(*) FROM t GROUP BY nosuch", "42703"),
        ("SELECT nosuch FROM (SELECT 1 AS x) AS d", "42703"),
        ("SELECT d.nosuch FROM (SELECT 1 AS x) AS d", "42703"),
        ("WITH c AS (SELECT id FROM t) SELECT id FROM c WHERE c.nosuch = 1", "42703"),
        ("SELECT nosuch FROM rr('v')", "42703"),
        ("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.nosuch)", "42703"),
        ("SELECT id FROM tv WHERE nosuch = 1", "42703"),
        ("UPDATE t SET name = 'b' WHERE nosuch = 1", "42703"),
        ("UPDATE t SET name = nosuch WHERE id = 1", "42703"),
        ("DELETE FROM t WHERE nosuch = 1", "42703"),
        ("SELECT x.id FROM t", "42P01"),
    ] {
        let error = session.execute(sql).expect_err(sql);
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
    // The failed UPDATE/DELETE changed nothing.
    assert_eq!(session.execute("SELECT name FROM t").unwrap().rows.len(), 1);
}

#[test]
fn valid_references_keep_working() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = session_with_fixture(&mut db);
    for sql in [
        "SELECT t.id FROM t AS x",
        "SELECT x.id FROM t AS x",
        "SELECT id AS ident FROM t ORDER BY ident",
        "SELECT name, count(*) AS n FROM t GROUP BY name ORDER BY n DESC",
        "SELECT id, (SELECT count(*) FROM u WHERE u.t_id = t.id) AS c FROM t",
        "SELECT r.x FROM rr('v') AS r",
        "SELECT n FROM generate_series(1, 2) AS s(n)",
        "WITH c(a) AS (SELECT id FROM t) SELECT a FROM c",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
        "SELECT t.id, l.c FROM t, LATERAL (SELECT count(*) AS c FROM u WHERE u.t_id = t.id) AS l",
        "SELECT e FROM t, jsonb_array_elements(t.data -> 'k') AS e",
        "SELECT tag FROM u, unnest(u.tags) AS tag",
        "SELECT t FROM t",
        "SELECT current_date, current_user, current_timestamp",
        "SELECT id, row_number() OVER (PARTITION BY name ORDER BY id) FROM t",
        "UPDATE t SET name = u.tags[1] FROM u WHERE u.t_id = t.id",
        "DELETE FROM u USING t WHERE u.t_id = t.id AND t.id = 99",
        "SELECT relname FROM pg_class WHERE relname = 't'",
        "SELECT id FROM t UNION SELECT id FROM u ORDER BY id",
        "SELECT DISTINCT ON (name) name, id FROM t ORDER BY name, id",
        "SELECT (person).name, (person).age FROM c",
        "SELECT make_interval(days => 2, hours => 3)",
        "SELECT count(*) FROM t HAVING count(*) > 0",
        // Correlated CTE and derived table whose select lists read the
        // enclosing row (the Hub's document access walk).
        "SELECT t.id FROM t WHERE EXISTS (WITH RECURSIVE walk AS (SELECT t.id, t.name, 0::bigint AS depth \
         UNION ALL SELECT p.id, p.name, s.depth + 1 FROM walk s JOIN t p ON p.id = s.id + 100) \
         SELECT 1 FROM walk WHERE walk.name = 'a' AND walk.depth >= 0)",
        "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM (SELECT t.name, t.id AS ident) AS d WHERE d.name = 'a' AND d.ident = 1)",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
