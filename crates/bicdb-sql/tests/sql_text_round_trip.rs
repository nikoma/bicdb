//! Definitions the catalog stores as SQL text, and statements rebuilt as SQL
//! text before they run, keep their string literals exactly. The values here
//! hold two adjacent quotes or a backslash before a quote: forms a generic
//! re-rendering of a parsed tree writes back wrong.
use bicdb_core::BicDb;
use bicdb_sql::{split_migration_sql, SqlSession, SqlValue};

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

#[test]
fn stored_definitions_keep_quotes_in_literals() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        r#"CREATE VIEW quoted AS SELECT 'a''''b' AS pair, 'c\''d' AS backslash"#,
        r#"CREATE DOMAIN note AS TEXT DEFAULT 'd''''d' CHECK (VALUE <> 'x''''x')"#,
        r#"CREATE TABLE items (
             id INT,
             label TEXT DEFAULT 'l''''l' CHECK (label <> 'n''''n'),
             tag TEXT GENERATED ALWAYS AS (label || '''''') STORED,
             body note,
             CONSTRAINT items_label CHECK (label <> 'm''''m')
           )"#,
        "INSERT INTO items (id) VALUES (1)",
        r#"CREATE FUNCTION pair() RETURNS TEXT LANGUAGE sql AS 'SELECT ''a''''''''b'''"#,
    ] {
        rows(&mut session, sql);
    }
    assert_eq!(
        rows(&mut session, "SELECT pair, backslash FROM quoted"),
        vec![vec![text("a''b"), text(r"c\'d")]]
    );
    assert_eq!(
        rows(&mut session, "SELECT label, tag, body FROM items"),
        vec![vec![text("l''l"), text("l''l''"), text("d''d")]]
    );
    assert_eq!(
        rows(&mut session, "SELECT pair()"),
        vec![vec![text("a''b")]]
    );
    for rejected in [
        "INSERT INTO items (id, label) VALUES (2, 'n''''n')",
        "INSERT INTO items (id, label) VALUES (2, 'm''''m')",
        "INSERT INTO items (id, body) VALUES (2, 'x''''x')",
    ] {
        assert!(session.execute(rejected).is_err(), "{rejected}");
    }
    // The near misses the checks must accept.
    rows(
        &mut session,
        "INSERT INTO items (id, label, body) VALUES (2, 'n''n', 'x''x')",
    );
    rows(
        &mut session,
        "ALTER TABLE items ALTER COLUMN label SET DEFAULT 'z''''z'",
    );
    rows(&mut session, "INSERT INTO items (id) VALUES (3)");
    assert_eq!(
        rows(&mut session, "SELECT label FROM items WHERE id = 3"),
        vec![vec![text("z''z")]]
    );
}

#[test]
fn split_migration_statements_keep_quotes_in_literals() {
    let sql = r#"INSERT INTO t VALUES ('a''''b'); UPDATE t SET s = 'c\''d'"#;
    let statements = split_migration_sql(sql).unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    rows(&mut session, "CREATE TABLE t (s TEXT)");
    rows(&mut session, &statements[0]);
    assert_eq!(
        rows(&mut session, "SELECT s FROM t"),
        vec![vec![text("a''b")]]
    );
    rows(&mut session, &statements[1]);
    assert_eq!(
        rows(&mut session, "SELECT s FROM t"),
        vec![vec![text(r"c\'d")]]
    );
}

/// Keys that differ only in how many quotes they hold find their own rows,
/// one lookup after the other (cached plans are keyed by statement text).
#[test]
fn point_lookups_distinguish_literals_that_differ_in_quotes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    rows(
        &mut session,
        "CREATE TABLE keyed (k TEXT PRIMARY KEY, v INT)",
    );
    rows(
        &mut session,
        "INSERT INTO keyed VALUES ('a''b', 1), ('a''''b', 2)",
    );
    for _ in 0..3 {
        assert_eq!(
            rows(&mut session, "SELECT v FROM keyed WHERE k = 'a''b'"),
            vec![vec![SqlValue::Int(1)]]
        );
        assert_eq!(
            rows(&mut session, "SELECT v FROM keyed WHERE k = 'a''''b'"),
            vec![vec![SqlValue::Int(2)]]
        );
    }
}
