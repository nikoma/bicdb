//! The centralized authorize-before-execute pass must not OVER-reach: a CTE (or
//! subquery alias) that shadows a real table name must still run for a role
//! that lacks privilege on the real table, because the query never touches it.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    {
        let mut o = SqlSession::new(&mut db);
        o.execute("CREATE ROLE mallory LOGIN NOSUPERUSER").unwrap();
        o.execute("CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        o.execute("INSERT INTO secrets VALUES (1,'REAL')").unwrap();
    }
    (directory, db)
}

#[test]
fn cte_shadowing_a_real_table_is_not_over_authorized() {
    let (_d, mut db) = db();
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");
    // Uses the CTE, never the real `secrets` — must succeed.
    let r = m
        .execute("WITH secrets AS (SELECT 99 AS id) SELECT id FROM secrets")
        .expect("a CTE shadowing a protected table must run without touching it");
    assert_eq!(format!("{:?}", r.rows), "[[Int(99)]]");
    // The real table is still denied.
    assert!(m.execute("SELECT * FROM secrets").is_err());
    // A derived-table alias shadowing the name likewise runs.
    m.execute("SELECT id FROM (SELECT 7 AS id) secrets")
        .unwrap();
}
