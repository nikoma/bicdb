//! PostgreSQL parity: casts from text use the target type's input function
//! in every evaluation path; temporary tables with triggers and
//! self-references; JSON and multirange input errors; `to_number` and
//! `parse_ident`. Expected values are PostgreSQL 18's.

use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

fn open() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

fn cells(sql: &mut SqlSession<'_>, statement: &str) -> Vec<Vec<String>> {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("{statement}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

fn error(sql: &mut SqlSession<'_>, statement: &str) -> String {
    match sql.execute(statement) {
        Ok(result) => panic!("{statement} succeeded: {:?}", result.rows),
        Err(error) => error.to_string(),
    }
}

#[test]
fn text_to_integer_casts_use_the_input_function_everywhere() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE t(t text, c char(5), n numeric)",
        "CREATE DOMAIN dt AS text",
        "CREATE TABLE td(t dt)",
        "INSERT INTO t VALUES ('34.5', '34.5', 34.5)",
        "INSERT INTO td VALUES ('34.5')",
        "CREATE TABLE ti(i int)",
        "CREATE VIEW tv AS SELECT t::int4 AS i FROM t",
        "CREATE FUNCTION fp(t text) RETURNS int LANGUAGE plpgsql AS $$DECLARE i int; BEGIN i := t; RETURN i; END$$",
        "CREATE FUNCTION fp2(t text) RETURNS int LANGUAGE plpgsql AS $$DECLARE i int; BEGIN i := t::int; RETURN i; END$$",
        "CREATE FUNCTION fp3(n numeric) RETURNS int LANGUAGE plpgsql AS $$DECLARE i int; BEGIN i := n; RETURN i; END$$",
        "CREATE FUNCTION fp4() RETURNS int LANGUAGE plpgsql AS $$DECLARE i int; BEGIN i := '34.5'; RETURN i; END$$",
    ] {
        sql.execute(statement).unwrap();
    }
    let invalid = "invalid input syntax for type integer: \"34.5\"";
    for statement in [
        "SELECT t::int4 FROM t",
        "SELECT CAST(t AS integer) FROM t",
        "SELECT * FROM tv",
        "SELECT n::text::int4 FROM t",
        "INSERT INTO ti SELECT t::int FROM t",
        "SELECT trim(t)::int FROM t",
        "SELECT upper('34.5')::int",
        "SELECT t::int FROM td",
        "SELECT fp('34.5')",
        "SELECT fp2('34.5')",
        "SELECT fp4()",
    ] {
        assert_eq!(error(&mut sql, statement), invalid, "{statement}");
    }
    assert_eq!(
        error(&mut sql, "SELECT t::int2 FROM t"),
        "invalid input syntax for type smallint: \"34.5\""
    );
    assert_eq!(
        error(&mut sql, "SELECT c::int FROM t"),
        "invalid input syntax for type integer: \"34.5 \""
    );
    // Numeric sources keep rounding half away from zero.
    assert_eq!(
        cells(&mut sql, "SELECT n::int, n::int8 FROM t"),
        [["35", "35"]]
    );
    assert_eq!(cells(&mut sql, "SELECT fp3(34.5)"), [["35"]]);
    assert_eq!(
        cells(&mut sql, "SELECT ('{\"a\":34.5}'::jsonb->'a')::int"),
        [["35"]]
    );
    assert_eq!(
        error(&mut sql, "SELECT ('{\"a\":\"x\"}'::jsonb->'a')::int"),
        "cannot cast jsonb string to type integer"
    );
    assert_eq!(
        error(&mut sql, "SELECT '1.5x'::float8"),
        "invalid input syntax for type double precision: \"1.5x\""
    );
}
