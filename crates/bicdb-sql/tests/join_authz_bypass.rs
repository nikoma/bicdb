//! Regression: joining a protected table against a table function (or any
//! left source) must NOT bypass the table's SELECT authorization. The indexed
//! equi-join fast path fetched the right table through an index lookup with no
//! privilege check, so `generate_series(...) JOIN secrets ON secrets.id = g`
//! read a table the role could not SELECT directly.
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
        owner.execute("CREATE ROLE bob LOGIN NOSUPERUSER").unwrap();
        owner
            .execute("CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        owner
            .execute("INSERT INTO secrets VALUES (1,'TOPSECRET'),(2,'CLASSIFIED')")
            .unwrap();
        owner.execute("GRANT SELECT ON secrets TO bob").unwrap();
    }
    (directory, db)
}

const JOINS: &[&str] = &[
    "SELECT s.v FROM generate_series(1,2) g JOIN secrets s ON s.id=g",
    "SELECT s.v FROM (VALUES (1),(2)) g(n) JOIN secrets s ON s.id=g.n",
    "SELECT s.v FROM generate_series(1,2) g LEFT JOIN secrets s ON s.id=g",
    "SELECT s.v FROM generate_series(1,2) g, secrets s WHERE s.id=g",
];

#[test]
fn join_against_table_function_requires_select_on_the_joined_table() {
    let (_d, mut db) = seeded();
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");
    for sql in JOINS {
        let err = m.execute(sql).expect_err(&format!("{sql} must be refused"));
        let msg = format!("{err}");
        assert!(
            msg.contains("permission denied for table secrets"),
            "{sql}: expected a table-authorization refusal, got: {msg}"
        );
    }
}

#[test]
fn the_same_joins_work_with_a_grant() {
    let (_d, mut db) = seeded();
    let mut b = SqlSession::new_unprivileged(&mut db, "bob");
    for sql in JOINS {
        let out = b
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql} should work for bob: {e}"));
        assert_eq!(out.rows.len(), 2, "{sql}");
    }
}
