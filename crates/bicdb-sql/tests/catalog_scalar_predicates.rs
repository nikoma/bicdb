use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn catalog_null_predicates_keep_database_context_without_from() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE catalog_probe(id integer)")
        .unwrap();
    for sql in [
        "SELECT to_regclass('public.catalog_probe') IS NOT NULL, to_regclass('public.absent_probe') IS NULL",
        "VALUES (to_regclass('public.catalog_probe') IS NOT NULL, to_regclass('public.absent_probe') IS NULL)",
        "WITH expected(present, absent) AS (VALUES (to_regclass('public.catalog_probe') IS NOT NULL, to_regclass('public.absent_probe') IS NULL)) SELECT present, absent FROM expected",
    ] {
        assert_eq!(session.execute(sql).unwrap().rows,
                   vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]], "{sql}");
    }
}

#[test]
fn catalog_boolean_predicates_preserve_sql_null_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session.execute("VALUES ((to_regclass('absent_probe') IS NULL) IS TRUE, (to_regclass('absent_probe') IS NOT NULL) IS FALSE, (to_regclass('absent_probe') = NULL) IS UNKNOWN, (to_regclass('absent_probe') = NULL) IS NOT TRUE)").unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Bool(true); 4]]);
}
