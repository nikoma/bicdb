//! Regression: reading a cube (`bicdb_projection.<name>`) and listing cubes
//! (`bicdb_projections`) must honor SELECT authorization on the base table.
//! A cube is a materialized aggregate over a base table; an unprivileged role
//! could read its dimension values and measures, bypassing the table gate.
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
            .execute("INSERT INTO sales VALUES (1,'secret-region',100),(2,'secret-region',250),(3,'other',7)")
            .unwrap();
        owner
            .execute("CREATE CUBE sales_cube ON sales DIMENSIONS (region) MEASURES (SUM(price)) WITH FORCE")
            .unwrap();
    }
    (directory, db)
}

#[test]
fn cube_read_requires_base_table_select() {
    let (_d, mut db) = seeded();
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");

    // Direct read of the cube must be refused, just like the base table.
    let err = m
        .execute("SELECT * FROM bicdb_projection.sales_cube")
        .expect_err("cube read must be refused for a role with no SELECT on sales");
    let msg = format!("{err}");
    assert!(
        msg.contains("permission denied for table sales"),
        "expected base-table refusal, got: {msg}"
    );

    // The status catalog must not disclose the cube either.
    let listed = m.execute("SELECT * FROM bicdb_projections").unwrap();
    assert!(
        listed.rows.is_empty(),
        "bicdb_projections leaked cube metadata to an unprivileged role: {:?}",
        listed.rows
    );
}

#[test]
fn cube_read_allowed_with_grant() {
    let (_d, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("GRANT SELECT ON sales TO mallory").unwrap();
    }
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");
    let read = m
        .execute("SELECT * FROM bicdb_projection.sales_cube")
        .unwrap();
    assert!(!read.rows.is_empty(), "granted role should read the cube");
    let listed = m.execute("SELECT * FROM bicdb_projections").unwrap();
    assert_eq!(
        listed.rows.len(),
        1,
        "granted role should see its cube listed"
    );
}
