//! PostgreSQL parity: soft input errors (`pg_input_is_valid`,
//! `pg_input_error_info`), VACUUM/ANALYZE statement forms, and FROM-less
//! SELECT evaluation. Expected values are PostgreSQL 18's.

use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

fn open() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

fn text(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Null => None,
        other => Some(other.to_cell()),
    }
}

fn error_info(sql: &mut SqlSession<'_>, input: &str, type_name: &str) -> Vec<Option<String>> {
    let result = sql
        .execute(&format!(
            "SELECT * FROM pg_input_error_info('{input}', '{type_name}')"
        ))
        .unwrap();
    assert_eq!(
        result.columns,
        ["message", "detail", "hint", "sql_error_code"],
        "{result:?}"
    );
    assert_eq!(result.rows.len(), 1);
    result.rows[0].iter().map(text).collect()
}

#[test]
fn pg_input_error_info_reports_soft_input_errors() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        error_info(&mut sql, "junk", "int4"),
        [
            Some("invalid input syntax for type integer: \"junk\"".to_string()),
            None,
            None,
            Some("22P02".to_string()),
        ]
    );
    assert_eq!(
        error_info(&mut sql, "50000", "int2"),
        [
            Some("value \"50000\" is out of range for type smallint".to_string()),
            None,
            None,
            Some("22003".to_string()),
        ]
    );
    assert_eq!(error_info(&mut sql, "42", "int4"), [None, None, None, None]);
    let result = sql
        .execute("SELECT message, sql_error_code FROM pg_input_error_info('junk', 'bool') AS e")
        .unwrap();
    assert_eq!(
        result.rows[0].iter().map(text).collect::<Vec<_>>(),
        [
            Some("invalid input syntax for type boolean: \"junk\"".to_string()),
            Some("22P02".to_string()),
        ]
    );
}

#[test]
fn pg_input_soft_errors_cover_user_types_and_raise_hard_errors() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TYPE soft_rainbow AS ENUM ('red', 'green')")
        .unwrap();
    sql.execute("CREATE DOMAIN soft_positive AS int CHECK (value > 0)")
        .unwrap();
    let valid = sql
        .execute(
            "SELECT pg_input_is_valid('red', 'soft_rainbow'), \
             pg_input_is_valid('mauve', 'soft_rainbow'), \
             pg_input_is_valid('1', 'soft_positive'), \
             pg_input_is_valid('-1', 'soft_positive'), \
             pg_input_is_valid('junk', 'soft_positive'), \
             pg_input_is_valid(NULL, 'int4')",
        )
        .unwrap();
    assert_eq!(
        valid.rows[0],
        [
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Null,
        ]
    );
    assert_eq!(
        error_info(&mut sql, "-1", "soft_positive"),
        [
            Some(
                "value for domain soft_positive violates check constraint \"soft_positive_check\""
                    .to_string()
            ),
            None,
            None,
            Some("23514".to_string()),
        ]
    );
    // An undefined type is not an input error: it is raised.
    let error = sql
        .execute("SELECT pg_input_is_valid('1', 'no_such_type')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    assert_eq!(error.to_string(), "type \"no_such_type\" does not exist");
    let error = sql
        .execute("SELECT * FROM pg_input_error_info('1', 'no_such_type')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    // An error raised while evaluating a domain CHECK expression is not
    // trapped either.
    sql.execute("CREATE DOMAIN soft_weird AS float8 CHECK (1 / value > 0)")
        .unwrap();
    let error = sql
        .execute("SELECT * FROM pg_input_error_info('0', 'soft_weird')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22012", "{error}");
}

#[test]
fn vacuum_and_analyze_accept_postgres_forms() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE vac_t (a int, b text)").unwrap();
    sql.execute("INSERT INTO vac_t VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    for (statement, tag) in [
        ("VACUUM vac_t", "VACUUM"),
        ("VACUUM (ANALYZE) vac_t", "VACUUM"),
        ("VACUUM ANALYZE vac_t", "VACUUM"),
        ("VACUUM FULL vac_t", "VACUUM"),
        ("VACUUM FREEZE VERBOSE vac_t", "VACUUM"),
        ("VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) vac_t", "VACUUM"),
        ("VACUUM (PARALLEL 2, INDEX_CLEANUP off) vac_t", "VACUUM"),
        ("VACUUM ANALYZE vac_t (a, b)", "VACUUM"),
        ("VACUUM ANALYZE", "VACUUM"),
        ("VACUUM (ONLY_DATABASE_STATS)", "VACUUM"),
        ("ANALYZE vac_t", "ANALYZE"),
        ("ANALYZE VERBOSE vac_t (a)", "ANALYZE"),
        ("ANALYZE (VERBOSE, SKIP_LOCKED) vac_t", "ANALYZE"),
        ("ANALYZE public.vac_t", "ANALYZE"),
    ] {
        let result = sql.execute(statement).unwrap_or_else(|error| {
            panic!("{statement}: {error}");
        });
        assert_eq!(result.command_tag.as_deref(), Some(tag), "{statement}");
        assert!(result.rows.is_empty(), "{statement}");
    }
    // VACUUM ANALYZE refreshed the statistics.
    let stats = sql
        .execute("SELECT count(*) FROM pg_stats WHERE tablename = 'vac_t'")
        .unwrap();
    assert_ne!(stats.rows[0][0], SqlValue::Int(0), "{stats:?}");
}

#[test]
fn vacuum_and_analyze_errors_match_postgres() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE vac_e (a int, b int)").unwrap();
    for (statement, sqlstate, message) in [
        (
            "VACUUM vac_e, vac_missing",
            "42P01",
            "relation \"vac_missing\" does not exist",
        ),
        (
            "ANALYZE vac_missing",
            "42P01",
            "relation \"vac_missing\" does not exist",
        ),
        (
            "VACUUM (nonexistentarg) vac_e",
            "42601",
            "unrecognized VACUUM option \"nonexistentarg\"",
        ),
        (
            "ANALYZE (nonexistentarg) vac_e",
            "42601",
            "unrecognized ANALYZE option \"nonexistentarg\"",
        ),
        (
            "VACUUM vac_e (a)",
            "0A000",
            "ANALYZE option must be specified when a column list is provided",
        ),
        (
            "ANALYZE vac_e (nope)",
            "42703",
            "column \"nope\" of relation \"vac_e\" does not exist",
        ),
        (
            "ANALYZE vac_e (a, b, b)",
            "42701",
            "column \"b\" of relation \"vac_e\" appears more than once",
        ),
        (
            "VACUUM (PARALLEL 2, FULL TRUE) vac_e",
            "0A000",
            "VACUUM FULL cannot be performed in parallel",
        ),
        (
            "VACUUM (PARALLEL) vac_e",
            "42601",
            "parallel option requires a value between 0 and 1024",
        ),
        (
            "VACUUM (PARALLEL -1) vac_e",
            "42601",
            "parallel workers for vacuum must be between 0 and 1024",
        ),
        (
            "VACUUM (ONLY_DATABASE_STATS) vac_e",
            "0A000",
            "ONLY_DATABASE_STATS cannot be specified with a list of tables",
        ),
    ] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{statement}: {error}");
        assert_eq!(error.to_string(), message, "{statement}");
    }
    sql.execute("BEGIN").unwrap();
    for statement in ["VACUUM vac_e", "VACUUM"] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), "25001", "{statement}");
        assert_eq!(
            error.to_string(),
            "VACUUM cannot run inside a transaction block"
        );
        sql.execute("ROLLBACK").unwrap();
        sql.execute("BEGIN").unwrap();
    }
    // ANALYZE may run inside a transaction block.
    sql.execute("ANALYZE vac_e").unwrap();
    sql.execute("COMMIT").unwrap();
}

#[test]
fn vacuum_skips_views_and_foreign_tables_with_postgres_warnings() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE vac_w (a int)").unwrap();
    sql.execute("CREATE VIEW vac_wv AS SELECT a FROM vac_w")
        .unwrap();
    let result = sql.execute("VACUUM vac_wv").unwrap();
    assert_eq!(
        result
            .notices
            .iter()
            .map(|notice| notice.message.as_str())
            .collect::<Vec<_>>(),
        ["skipping \"vac_wv\" --- cannot vacuum non-tables or special system tables"]
    );
    assert!(result.notices[0]
        .fields
        .contains(&bicdb_sql::SqlErrorField::Severity("WARNING".into())));
    sql.execute("CREATE ROLE vac_other").unwrap();
    sql.execute("SET ROLE vac_other").unwrap();
    for (statement, message) in [
        (
            "VACUUM vac_w",
            "permission denied to vacuum \"vac_w\", skipping it",
        ),
        (
            "VACUUM (ANALYZE) vac_w",
            "permission denied to vacuum \"vac_w\", skipping it",
        ),
        (
            "ANALYZE vac_w",
            "permission denied to analyze \"vac_w\", skipping it",
        ),
    ] {
        let result = sql.execute(statement).unwrap();
        assert_eq!(
            result
                .notices
                .iter()
                .map(|notice| notice.message.as_str())
                .collect::<Vec<_>>(),
            [message],
            "{statement}"
        );
    }
}
