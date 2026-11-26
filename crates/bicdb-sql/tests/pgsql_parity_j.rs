//! PostgreSQL parity: casts from text use the target type's input function
//! in every evaluation path; temporary tables with triggers and
//! self-references; JSON and multirange input errors; `to_number` and
//! `parse_ident`. Expected values are PostgreSQL 18's.

use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlErrorField, SqlSession, SqlValue};
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

#[test]
fn temporary_tables_take_triggers_and_self_references() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TEMP TABLE tt(a int, b int)",
        "CREATE FUNCTION tf() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN new.b := 7; RETURN new; END$$",
        "CREATE TRIGGER tt_t BEFORE INSERT ON tt FOR EACH ROW EXECUTE PROCEDURE tf()",
        "INSERT INTO tt VALUES (1, 0)",
        "CREATE TEMP TABLE tfk(id int primary key, parent int references tfk)",
        "INSERT INTO tfk VALUES (1, NULL)",
        "INSERT INTO tfk VALUES (2, 1)",
        "CREATE TEMP TABLE tfp(id int primary key)",
        "CREATE TEMP TABLE tfc(id int references tfp)",
    ] {
        sql.execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    assert_eq!(cells(&mut sql, "SELECT a, b FROM tt"), [["1", "7"]]);
    assert!(error(&mut sql, "INSERT INTO tfk VALUES (3, 9)").contains("\"tfk_parent_fkey\""));
    assert_eq!(
        error(&mut sql, "INSERT INTO tfc VALUES (2)"),
        "insert or update on table \"tfc\" violates foreign key constraint \"tfc_id_fkey\""
    );
}

fn error_fields(sql: &mut SqlSession<'_>, statement: &str) -> (String, Vec<SqlErrorField>) {
    match sql.execute(statement) {
        Ok(result) => panic!("{statement} succeeded: {:?}", result.rows),
        Err(error) => (error.to_string(), error.fields()),
    }
}

#[test]
fn json_input_errors_carry_postgres_detail_and_context() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for (statement, detail, context) in [
        (
            "SELECT '[1,2,]'::json",
            "Expected JSON value, but found \"]\".",
            "JSON data, line 1: [1,2,]",
        ),
        (
            "SELECT '{\"abc\":1,3}'::jsonb",
            "Expected string, but found \"3\".",
            "JSON data, line 1: {\"abc\":1,3...",
        ),
    ] {
        let (message, fields) = error_fields(&mut sql, statement);
        assert_eq!(message, "invalid input syntax for type json", "{statement}");
        assert!(
            fields.contains(&SqlErrorField::Detail(detail.into())),
            "{fields:?}"
        );
        assert!(
            fields.contains(&SqlErrorField::Context(context.into())),
            "{fields:?}"
        );
    }
}

#[test]
fn multirange_input_errors_carry_postgres_detail() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for (literal, message, detail) in [
        (
            "",
            "malformed multirange literal: \"\"",
            "Missing left brace.",
        ),
        (
            "{,}",
            "malformed multirange literal: \"{,}\"",
            "Expected range start.",
        ),
        (
            "{(,)}.",
            "malformed multirange literal: \"{(,)}.\"",
            "Junk after closing right brace.",
        ),
        (
            "{[1,3) - }",
            "malformed multirange literal: \"{[1,3) - }\"",
            "Expected comma or end of multirange.",
        ),
        (
            "{(,,3)}",
            "malformed range literal: \"(,,3)\"",
            "Too many commas.",
        ),
    ] {
        let (got, fields) = error_fields(&mut sql, &format!("SELECT '{literal}'::int4multirange"));
        assert_eq!(got, message, "{literal}");
        assert!(
            fields.contains(&SqlErrorField::Detail(detail.into())),
            "{literal}: {fields:?}"
        );
    }
}

#[test]
fn to_number_and_parse_ident_match_postgres() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        cells(
            &mut sql,
            "SELECT to_number('-34,338,492.654,878', '99G999G999D999G999'), to_number('<564646.654564>', '999999.999999PR'), to_number('CvIiI', 'rn')"
        ),
        [["-34338492.654878", "-564646.654564", "108"]]
    );
    assert_eq!(
        error(&mut sql, "SELECT to_number('viv', 'RN')"),
        "invalid Roman numeral"
    );
    assert_eq!(
        cells(&mut sql, "SELECT parse_ident('\"SchemaX\".TableY')"),
        [["[\"SchemaX\",\"tabley\"]"]]
    );
    let (message, fields) = error_fields(&mut sql, "SELECT parse_ident(' .aaa')");
    assert_eq!(message, "string is not a valid identifier: \" .aaa\"");
    assert!(fields.contains(&SqlErrorField::Detail(
        "No valid identifier before \".\".".into()
    )));
}
