//! PostgreSQL string constants reach every statement with the value
//! PostgreSQL's lexer gives them: escape strings, Unicode escapes,
//! standard_conforming_strings, quote_literal and float output digits.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn open(root: &std::path::Path) -> BicDb {
    BicDb::open_with_config(root, DbConfig::default().with_fsync(false)).unwrap()
}

fn text(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .map(|value| match value {
            SqlValue::Null => "NULL".to_string(),
            other => other.to_cell(),
        })
        .collect()
}

#[test]
fn escape_strings_keep_every_backslash_and_quote() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        text(
            &mut session,
            r"SELECT E'a\\b', E'q\'r', E'''1 \\''2'''::tsvector::text"
        ),
        vec![r"a\b", "q'r", "'1 ''2'"]
    );
    session
        .execute(r"CREATE TABLE t(s text); INSERT INTO t VALUES (E'x\\''y'), ('c\'), (E'a\';b')")
        .unwrap();
    assert_eq!(
        text(
            &mut session,
            r"SELECT s FROM t WHERE s <> E'c\\' ORDER BY s"
        ),
        vec!["a';b", r"x\'y"]
    );
    assert_eq!(
        text(
            &mut session,
            r"SELECT U&'d!0061t!+000061' UESCAPE '!', E'\uD83D\uDE00'"
        ),
        vec!["data", "😀"]
    );
    assert_eq!(
        text(
            &mut session,
            r"SELECT quote_literal(E'a\\b'), quote_literal('it''s')"
        ),
        vec![r"E'a\\b'", "'it''s'"]
    );
    let error = session.execute(r"SELECT E'\u12'").unwrap_err();
    assert_eq!(error.sqlstate(), "22025");
    let error = session.execute("SELECT 'a' 'b'").unwrap_err();
    assert_eq!(error.sqlstate(), "42601");
    assert_eq!(text(&mut session, "SELECT 'a'\n  'b'"), vec!["ab"]);
}

#[test]
fn standard_conforming_strings_off_reads_backslash_escapes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("SET standard_conforming_strings = off")
        .unwrap();
    assert_eq!(
        text(&mut session, "SHOW standard_conforming_strings"),
        vec!["off"]
    );
    assert_eq!(
        text(&mut session, r"SELECT 'a\\b', 'q\'r', length('t\tu')"),
        vec![r"a\b", "q'r", "3"]
    );
    let error = session.execute("SELECT U&'x'").unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    session
        .execute("SET standard_conforming_strings = on")
        .unwrap();
    assert_eq!(text(&mut session, r"SELECT 'a\\b'"), vec![r"a\\b"]);
}
