//! A multi-statement text behaves exactly like its statements sent one by
//! one: forms the SQL parser cannot read (handled before it) work inside a
//! batch, statements commit as they would on their own, and every
//! statement's notices are reported.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn open(root: &std::path::Path) -> BicDb {
    BicDb::open_with_config(root, DbConfig::default().with_fsync(false)).unwrap()
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Bool(value) => if value { "t" } else { "f" }.to_string(),
                    other => other.to_cell(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// Statements the parser cannot read on their own, mixed with ordinary ones.
const STATEMENTS: &[&str] = &[
    "CREATE TABLE b1 (a int NOT NULL, b text)",
    "ALTER TABLE b1 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME b1_seq)",
    "INSERT INTO b1 (b) VALUES ('x'), ('y')",
    "ALTER TABLE b1 SET UNLOGGED",
    "CREATE TABLE b2 (a int, v int GENERATED ALWAYS AS (a * 2))",
    "INSERT INTO b2 (a) VALUES (21)",
    "COMMENT ON TABLE b2 IS 'two'",
    "ALTER TABLE b1 ALTER COLUMN a RESTART WITH 100",
    "INSERT INTO b1 (b) VALUES ('z')",
    "CREATE TABLE b3 (a int, CONSTRAINT b3_pos CHECK (a > 0) NO INHERIT)",
    "CREATE TABLE b3c () INHERITS (b3)",
    "INSERT INTO b3c VALUES (-1)",
];

const CHECKS: &[&str] = &[
    "SELECT a, b FROM b1 ORDER BY a",
    "SELECT relpersistence FROM pg_class WHERE relname = 'b1'",
    "SELECT a, v FROM b2",
    "SELECT obj_description('b2'::regclass, 'pg_class')",
    "SELECT attidentity FROM pg_attribute WHERE attrelid = 'b1'::regclass AND attname = 'a'",
    "SELECT count(*) FROM b3",
    "SELECT conname, connoinherit FROM pg_constraint WHERE conrelid = 'b3'::regclass",
];

fn state(session: &mut SqlSession<'_>) -> Vec<Vec<String>> {
    CHECKS.iter().map(|sql| rows(session, sql)).collect()
}

#[test]
fn a_batch_matches_the_same_statements_sent_one_by_one() {
    let one_root = tempfile::tempdir().unwrap();
    let mut one_db = open(one_root.path());
    let mut one = SqlSession::new(&mut one_db);
    for statement in STATEMENTS {
        one.execute(statement).unwrap();
    }
    let expected = state(&mut one);

    let batch_root = tempfile::tempdir().unwrap();
    let mut batch_db = open(batch_root.path());
    let mut batch = SqlSession::new(&mut batch_db);
    batch.execute(&STATEMENTS.join(";\n")).unwrap();
    assert_eq!(state(&mut batch), expected);
    assert_eq!(expected[0], vec!["1|x", "2|y", "100|z"]);
}

#[test]
fn a_failing_statement_keeps_the_earlier_ones_as_sent_one_by_one() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute("CREATE TABLE f1 (a int); INSERT INTO f1 VALUES (1); SELECT nosuch FROM f1; INSERT INTO f1 VALUES (2)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42703");
    // The statements before the failure committed on their own; the one
    // after it did not run.
    assert_eq!(rows(&mut session, "SELECT a FROM f1"), vec!["1"]);
    // A transaction block inside a batch is honoured.
    session
        .execute("BEGIN; CREATE TABLE f2 (a int); ALTER TABLE f2 SET UNLOGGED; ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT count(*) FROM pg_class WHERE relname = 'f2'"
        ),
        vec!["0"]
    );
    assert!(!session.in_transaction());
}

#[test]
fn a_batch_reports_every_statements_notices() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("DROP TABLE IF EXISTS gone1; DROP TABLE IF EXISTS gone2")
        .unwrap();
    let notices = result
        .notices
        .iter()
        .map(|notice| notice.message.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(notices.contains("gone1"), "{notices}");
    assert!(notices.contains("gone2"), "{notices}");
}

#[test]
fn nested_comments_do_not_split_a_statement_or_produce_a_phantom_statement() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    let sql = "/* outer /* inner */ ; ' */ SELECT /* 1 /* 2 /* 3 */ ' ; */ ; */ 7;
               /* trailing comment /* inner */ ; ' */";
    assert_eq!(bicdb_sql::split_sql_statements(sql).len(), 1);
    assert_eq!(rows(&mut session, sql), ["7"]);
    assert_eq!(
        rows(&mut session, "SELECT 1; SELECT /* /* nested */ ; ' */ 2"),
        ["2"],
    );
    assert!(session.execute("SELECT 1 /* /* nested */").is_err());
}
