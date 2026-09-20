//! The raw-SQL fast-path handlers dispatched from `execute_inner`.
//!
//! BicDB recognizes a number of statements ahead of the parsed-`Statement`
//! dispatch, in hand-written handlers. Those handlers mutate the same table
//! metadata as their parsed equivalents, but never received the ownership gate
//! the parsed paths enforce (`execute_create_index`, `execute_alter_table`) —
//! so for each of these statements there were two implementations of one
//! effect, and only one of them was authorized.
//!
//! Each test below is that exploit, now expected to be refused.

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
        for sql in [
            "CREATE ROLE mallory LOGIN NOSUPERUSER",
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, n INT)",
            "INSERT INTO t VALUES (1, 'a', 1)",
        ] {
            owner.execute(sql).unwrap();
        }
    }
    (directory, db)
}

fn as_mallory(db: &mut BicDb) -> SqlSession<'_> {
    SqlSession::new_unprivileged(db, "mallory")
}

fn assert_refused(sql: &str, outcome: Result<bicdb_sql::SqlResult, bicdb_sql::SqlError>) {
    match outcome {
        Ok(rows) => panic!("{sql} must be refused, returned {rows:?}"),
        Err(error) => {
            let rendered = format!("{error}");
            assert!(
                rendered.contains("owner")
                    || rendered.contains("permission")
                    || rendered.contains("superuser"),
                "{sql} was rejected for the wrong reason: {rendered}"
            );
        }
    }
}

/// Every raw handler that edits another tenant's table metadata.
#[test]
fn raw_ddl_handlers_require_table_ownership() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    for sql in [
        "CREATE INDEX m_idx ON ONLY t (v)",
        "CREATE SPATIAL INDEX m_spatial ON t (geometry)",
        "ALTER TABLE t ALTER COLUMN n ADD GENERATED ALWAYS AS IDENTITY",
        "ALTER TABLE t ALTER COLUMN n RESTART WITH 100",
        "ALTER TABLE t ALTER COLUMN v SET COMPRESSION lz4",
        "ALTER TABLE t ADD CONSTRAINT m_excl EXCLUDE (n WITH =)",
    ] {
        let outcome = mallory.execute(sql);
        assert_refused(sql, outcome);
    }
}

/// Partitioning attaches storage to someone else's parent table.
#[test]
fn partition_ddl_requires_ownership_of_the_parent() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE TABLE parted (id INT, part INT) PARTITION BY RANGE (part)")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let sql = "CREATE TABLE m_part PARTITION OF parted FOR VALUES FROM (0) TO (10)";
    assert_refused(sql, mallory.execute(sql));

    mallory
        .execute("CREATE TABLE mine (id INT, part INT)")
        .unwrap();
    let sql = "ALTER TABLE parted ATTACH PARTITION mine FOR VALUES FROM (10) TO (20)";
    assert_refused(sql, mallory.execute(sql));
}

/// An index belongs to its table, so repacking one is the table's call.
#[test]
fn pack_spatial_index_requires_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE SPATIAL INDEX t_spatial ON t (geometry)")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let sql = "PACK SPATIAL INDEX t_spatial";
    assert_refused(sql, mallory.execute(sql));
}

/// Cube admin commands recompute against the cube's source table.
#[test]
fn materialized_aggregate_admin_requires_ownership_of_the_source() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE CUBE t_cube ON t DIMENSIONS (v) MEASURES (count(*)) WITH FORCE")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    for sql in [
        "RECONCILE MATERIALIZED AGGREGATE t_cube",
        "EXPLAIN MATERIALIZED AGGREGATE t_cube",
        "MERGE MATERIALIZED AGGREGATE t_cube",
    ] {
        let outcome = mallory.execute(sql);
        assert_refused(sql, outcome);
    }
}

/// The owner must still be able to run every one of these.
#[test]
fn the_owner_can_still_run_the_raw_ddl() {
    let (_directory, mut db) = seeded();
    let mut owner = SqlSession::new(&mut db);
    for sql in [
        "CREATE INDEX o_idx ON ONLY t (v)",
        "ALTER TABLE t ALTER COLUMN v SET COMPRESSION lz4",
        "CREATE SPATIAL INDEX o_spatial ON t (geometry)",
        "PACK SPATIAL INDEX o_spatial",
    ] {
        owner
            .execute(sql)
            .unwrap_or_else(|error| panic!("the owner must still be able to run {sql}: {error:?}"));
    }
}
