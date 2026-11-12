//! PostgreSQL partition data semantics, separate from Oracle metadata lowering.
//! These retain the positive requirements even while the old implementation
//! fails. Expected rows/bounds match the independent Native PostgreSQL control.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn setup(db: &mut BicDb) {
    let mut sql = SqlSession::new(db);
    sql.execute("CREATE TABLE parent(item_key INT,payload TEXT) PARTITION BY RANGE(item_key)")
        .unwrap();
    sql.execute("CREATE TABLE child PARTITION OF parent FOR VALUES FROM (0) TO (10)")
        .unwrap();
    sql.execute("INSERT INTO child VALUES(1,'child-ack')")
        .unwrap();
}
fn rows(db: &mut BicDb, table: &str) -> Vec<Vec<SqlValue>> {
    SqlSession::new(db)
        .execute(&format!(
            "SELECT item_key,payload FROM {table} ORDER BY item_key"
        ))
        .unwrap()
        .rows
}
fn row(key: i64, payload: &str) -> Vec<SqlValue> {
    vec![SqlValue::Int(key), SqlValue::String(payload.into())]
}

#[test]
fn parent_reads_include_acknowledged_child_rows_across_two_reopens() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        for reopen in 0..3 {
            assert_eq!(rows(&mut db, "child"), vec![row(1, "child-ack")]);
            let actual = rows(&mut db, "parent");
            if actual != vec![row(1, "child-ack")] {
                failures.push(format!("{mode} reopen{reopen}: parent={actual:?}"));
            }
            db.close().unwrap();
            if reopen < 2 {
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn parent_inserts_route_to_the_actual_child_and_keep_both_acks() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        SqlSession::new(&mut db)
            .execute("INSERT INTO parent VALUES(2,'parent-ack')")
            .unwrap();
        for reopen in 0..3 {
            let actual = rows(&mut db, "child");
            if actual != vec![row(1, "child-ack"), row(2, "parent-ack")] {
                failures.push(format!("{mode} reopen{reopen}: child={actual:?}"));
            }
            let parent = rows(&mut db, "parent");
            if parent != vec![row(1, "child-ack"), row(2, "parent-ack")] {
                failures.push(format!("{mode} reopen{reopen}: parent={parent:?}"));
            }
            db.close().unwrap();
            if reopen < 2 {
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn direct_child_insert_enforces_bounds_before_mutation_and_preserves_prior_ack() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("BEGIN").unwrap();
            match sql.execute("INSERT INTO child VALUES(50,'must-refuse')") {
                Ok(_) => failures.push(format!("{mode}: out-of-bound child INSERT accepted")),
                Err(error) => {
                    if error.sqlstate() != "23514" {
                        failures.push(format!("{mode}: wrong bound diagnostic {error}"));
                    }
                }
            }
            sql.execute("ROLLBACK").unwrap();
        }
        for reopen in 0..3 {
            assert_eq!(rows(&mut db, "child"), vec![row(1, "child-ack")]);
            db.close().unwrap();
            if reopen < 2 {
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn attached_default_partition_routes_new_rows_and_keeps_existing_acknowledged_data() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE fallback(item_key INT,payload TEXT)")
                .unwrap();
            sql.execute("INSERT INTO fallback VALUES(50,'fallback-ack')")
                .unwrap();
            match sql.execute("ALTER TABLE parent ATTACH PARTITION fallback DEFAULT") {
                Ok(_) => {
                    sql.execute("INSERT INTO parent VALUES(11,'routed-default-ack')")
                        .unwrap();
                }
                Err(error) => failures.push(format!("{mode}: attach DEFAULT refused: {error}")),
            }
        }
        for reopen in 0..3 {
            let actual = rows(&mut db, "fallback");
            if actual != vec![row(11, "routed-default-ack"), row(50, "fallback-ack")] {
                failures.push(format!("{mode} reopen{reopen}: fallback={actual:?}"));
            }
            assert_eq!(rows(&mut db, "child"), vec![row(1, "child-ack")]);
            db.close().unwrap();
            if reopen < 2 {
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
