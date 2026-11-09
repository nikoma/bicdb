//! Top-level WITH DELETE shares the native DELETE transaction/CTE path.
//! Results independently checked against PostgreSQL16.13.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;
#[test]
fn top_level_with_delete_uses_ctes_for_using_returning_and_subqueries() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE compat_cte_delete(id int primary key,v text)")
        .unwrap();
    session
        .execute("INSERT INTO compat_cte_delete VALUES(1,'one'),(2,'two'),(3,'three')")
        .unwrap();
    let rows=session.execute("WITH doomed AS (SELECT 2 AS id) DELETE FROM compat_cte_delete t USING doomed d WHERE t.id=d.id RETURNING t.id,t.v").unwrap().rows;
    assert_eq!(
        rows.iter()
            .map(|r| r.iter().map(|v| v.to_cell()).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["2", "two"]]
    );
    session.execute("WITH doomed AS (SELECT 1 AS id) DELETE FROM compat_cte_delete WHERE id IN (SELECT id FROM doomed)").unwrap();
    let rows = session
        .execute("SELECT * FROM compat_cte_delete ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter()
            .map(|r| r.iter().map(|v| v.to_cell()).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["3", "three"]]
    );
}

#[test]
fn top_level_delete_rolls_back_cte_mutations_on_constraint_failure() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE parent(id int PRIMARY KEY)",
        "CREATE TABLE child(id int REFERENCES parent(id))",
        "INSERT INTO parent VALUES(1)",
        "INSERT INTO child VALUES(1)",
    ] {
        session.execute(sql).unwrap();
    }
    let error=session.execute("WITH added AS (INSERT INTO parent VALUES(2) RETURNING id) DELETE FROM parent WHERE id=1").unwrap_err();
    assert_eq!(error.sqlstate(), "23503");
    assert_eq!(
        session
            .execute("SELECT id FROM parent ORDER BY id")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "1"
    );
    assert_eq!(
        session.execute("SELECT count(*) FROM parent").unwrap().rows[0][0].to_cell(),
        "1"
    );
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "WITH doomed AS (SELECT 1 AS id) DELETE FROM child WHERE id IN(SELECT id FROM doomed)",
        )
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM child").unwrap().rows[0][0].to_cell(),
        "1"
    );
}
