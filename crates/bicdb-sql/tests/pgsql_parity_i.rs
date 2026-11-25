//! PostgreSQL parity: set-returning SQL functions in select lists, hstore's
//! populate_record, text search catalog functions and defaults, and
//! inheritance ALTER forms.
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

#[test]
fn set_returning_sql_functions_expand_in_the_select_list() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE n(v int); INSERT INTO n VALUES (1), (2);
             CREATE FUNCTION s(k int) RETURNS SETOF int LANGUAGE sql
               AS 'SELECT g FROM generate_series(1, k) g';
             CREATE FUNCTION t(k int) RETURNS TABLE(a int, b text) LANGUAGE sql
               AS $$ SELECT g, 'v' || g FROM generate_series(1, k) g $$",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "SELECT s(2)"), vec!["1", "2"]);
    assert_eq!(
        rows(&mut session, "SELECT v, s(v) FROM n ORDER BY 1, 2"),
        vec!["1|1", "2|1", "2|2"]
    );
    assert_eq!(rows(&mut session, "SELECT (t(2)).b"), vec!["v1", "v2"]);
    assert_eq!(rows(&mut session, "SELECT s(2) * 10"), vec!["10", "20"]);
    assert_eq!(
        rows(&mut session, "SELECT * FROM t(2) WITH ORDINALITY"),
        vec!["1|v1|1", "2|v2|2"]
    );
}

#[test]
fn hstore_set_functions_and_populate_record() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE EXTENSION hstore; CREATE TABLE r(a int, b text)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT skeys('a=>1,b=>2'::hstore) ORDER BY 1"),
        vec!["a", "b"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM populate_record(NULL::r, 'a=>7,b=>x'::hstore)"
        ),
        vec!["7|x"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT (populate_record(ROW(5, 'q')::r, 'b=>z'::hstore)).*"
        ),
        vec!["5|z"]
    );
}
