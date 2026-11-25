//! Table inheritance is not implemented, so INHERITS fails clearly instead of
//! creating a child without the parent's columns.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn create_table_inherits_is_refused_with_0a000() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE parent_rows(a INT)").unwrap();
    assert_eq!(
        sql.execute("CREATE TABLE child_rows(b INT) INHERITS (parent_rows)")
            .unwrap_err()
            .sqlstate(),
        "0A000"
    );
    // No child relation was created.
    assert!(sql.execute("SELECT * FROM child_rows").is_err());
}
