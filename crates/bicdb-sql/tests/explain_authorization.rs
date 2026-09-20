//! Regression: EXPLAIN (and EXPLAIN ANALYZE) require SELECT on the referenced
//! tables, matching PostgreSQL. Otherwise the plan shape and row estimates are
//! an oracle, and ANALYZE leaks the real matching-row count.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn seeded() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE ROLE mallory LOGIN NOSUPERUSER")
            .unwrap();
        owner
            .execute("CREATE TABLE sales (receipt INT PRIMARY KEY, region TEXT, price INT)")
            .unwrap();
        owner
            .execute("INSERT INTO sales VALUES (1,'secret',100),(2,'secret',250),(3,'other',7)")
            .unwrap();
    }
    (directory, db)
}

#[test]
fn explain_requires_select_privilege() {
    let (_d, mut db) = seeded();
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");
    for sql in [
        "EXPLAIN SELECT * FROM sales WHERE receipt = 1",
        "EXPLAIN SELECT * FROM sales WHERE region = 'secret'",
        "EXPLAIN ANALYZE SELECT * FROM sales WHERE region = 'secret'",
    ] {
        let err = m.execute(sql).expect_err(&format!("{sql} must be refused"));
        assert!(
            format!("{err}").contains("permission denied for table sales"),
            "{sql}: got {err}"
        );
    }
}

#[test]
fn explain_allowed_with_grant() {
    let (_d, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("GRANT SELECT ON sales TO mallory").unwrap();
    }
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");
    m.execute("EXPLAIN SELECT * FROM sales WHERE receipt = 1")
        .unwrap();
}
