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
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
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
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
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
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
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
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn parent_update_moves_rows_between_partitions_without_losing_the_ack() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        SqlSession::new(&mut db)
            .execute("CREATE TABLE upper_child PARTITION OF parent FOR VALUES FROM (10) TO (20)")
            .unwrap();
        SqlSession::new(&mut db)
            .execute("UPDATE parent SET item_key=11 WHERE item_key=1")
            .unwrap();
        for reopen in 0..3 {
            let low = rows(&mut db, "child");
            let high = rows(&mut db, "upper_child");
            if !low.is_empty() || high != vec![row(11, "child-ack")] {
                failures.push(format!("{mode} reopen{reopen}: old={low:?} new={high:?}"));
            }
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn parent_delete_visits_descendants_and_preserves_unselected_acknowledged_rows() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE upper_child PARTITION OF parent FOR VALUES FROM (10) TO (20)")
            .unwrap();
        sql.execute("INSERT INTO upper_child VALUES(11,'other-ack')")
            .unwrap();
        sql.execute("DELETE FROM parent WHERE item_key=1").unwrap();
        drop(sql);
        for reopen in 0..3 {
            let low = rows(&mut db, "child");
            if !low.is_empty() {
                failures.push(format!(
                    "{mode} reopen{reopen}: DELETE omitted child={low:?}"
                ));
            }
            assert_eq!(rows(&mut db, "upper_child"), vec![row(11, "other-ack")]);
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn partition_queries_use_the_privilege_and_rls_policy_of_the_accessed_relation() {
    let mut failures = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE ROLE partition_reader").unwrap();
            sql.execute(
                "CREATE TABLE parent(item_key INT,payload TEXT) PARTITION BY RANGE(item_key)",
            )
            .unwrap();
            sql.execute("ALTER TABLE parent ENABLE ROW LEVEL SECURITY")
                .unwrap();
            sql.execute(
                "CREATE POLICY parent_select_policy ON parent FOR SELECT USING(item_key>=2)",
            )
            .unwrap();
            sql.execute("CREATE TABLE child PARTITION OF parent FOR VALUES FROM (0) TO (10)")
                .unwrap();
            sql.execute("INSERT INTO child VALUES(1,'child-one-ack'),(2,'child-two-ack')")
                .unwrap();
            sql.execute("GRANT SELECT ON parent TO partition_reader")
                .unwrap();
            sql.execute("SET ROLE partition_reader").unwrap();
            let actual = sql
                .execute("SELECT item_key,payload FROM parent ORDER BY item_key")
                .unwrap()
                .rows;
            if actual != vec![row(2, "child-two-ack")] {
                failures.push(format!("{mode}: parent access/policy={actual:?}"));
            }
            assert_eq!(
                sql.execute("SELECT * FROM child").unwrap_err().sqlstate(),
                "42501"
            );
            sql.execute("RESET ROLE").unwrap();
            sql.execute("GRANT SELECT ON child TO partition_reader")
                .unwrap();
        }
        for reopen in 0..3 {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("SET ROLE partition_reader").unwrap();
            let actual = sql
                .execute("SELECT item_key,payload FROM child ORDER BY item_key")
                .unwrap()
                .rows;
            if actual != vec![row(1, "child-one-ack"), row(2, "child-two-ack")] {
                failures.push(format!(
                    "{mode} reopen{reopen}: child must use own policies={actual:?}"
                ));
            }
            sql.execute("RESET ROLE").unwrap();
            drop(sql);
            assert_eq!(
                rows(&mut db, "child"),
                vec![row(1, "child-one-ack"), row(2, "child-two-ack")]
            );
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn parent_query_shapes_keep_each_physical_source_and_its_system_columns() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        SqlSession::new(&mut db)
            .execute("INSERT INTO child VALUES(2,'second-ack')")
            .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE INDEX parent_lookup ON parent(item_key)")
            .unwrap();
        for reopen in 0..3 {
            let mut sql = SqlSession::new(&mut db);
            for statement in [
                "SELECT count(*) FROM parent",
                "SELECT count(*) FROM parent WHERE item_key>=1",
                "SELECT count(*) FROM (SELECT * FROM parent) q",
                "SELECT count(*) FROM generate_series(1,2) g(n) JOIN parent p ON p.item_key=g.n",
                "SELECT count(*) FROM parent p JOIN generate_series(1,2) g(n) ON p.item_key=g.n",
                "SELECT count(*) FROM parent WHERE EXISTS(SELECT 1 FROM child WHERE child.item_key=parent.item_key)",
            ] {
                assert_eq!(sql.execute(statement).unwrap().rows, vec![vec![SqlValue::Int(2)]], "{statement}");
            }
            assert_eq!(
                sql.execute("SELECT item_key FROM parent WHERE item_key=2 LIMIT 1")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(2)]]
            );
            assert_eq!(
                sql.execute("SELECT sum(item_key),min(item_key),max(item_key) FROM parent")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(3), SqlValue::Int(1), SqlValue::Int(2)]]
            );
            let physical = sql
                .execute("SELECT tableoid,xmin,ctid FROM child ORDER BY item_key")
                .unwrap()
                .rows;
            assert_eq!(
                sql.execute("SELECT tableoid,xmin,ctid FROM parent ORDER BY item_key")
                    .unwrap()
                    .rows,
                physical
            );
            assert_eq!(
                sql.execute("SELECT p.tableoid,p.xmin,p.ctid FROM parent p ORDER BY item_key")
                    .unwrap()
                    .rows,
                physical
            );
            drop(sql);
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
}

#[test]
fn parent_primary_key_exists_probes_use_descendant_rows_instead_of_empty_parent_indexes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)")
                .unwrap();
            sql.execute("CREATE TABLE c PARTITION OF p FOR VALUES FROM(0) TO(10)")
                .unwrap();
            sql.execute("INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack')")
                .unwrap();
            sql.execute("CREATE INDEX empty_parent_probe ON p(k)")
                .unwrap();
            sql.execute("CREATE TABLE outer_numbers(n INT PRIMARY KEY)")
                .unwrap();
            sql.execute("INSERT INTO outer_numbers VALUES(1),(2),(3)")
                .unwrap();
        }
        for reopen in 0..3 {
            let mut sql = SqlSession::new(&mut db);
            for statement in [
                "SELECT g.n FROM outer_numbers g WHERE EXISTS(SELECT 1 FROM p WHERE p.k=g.n) ORDER BY g.n",
                "SELECT g.n FROM outer_numbers g WHERE g.n IN(SELECT k FROM p) ORDER BY g.n",
            ] {
                assert_eq!(sql.execute(statement).unwrap().rows,vec![vec![SqlValue::Int(1)],vec![SqlValue::Int(2)]],"{statement} reopen{reopen}");
            }
            assert_eq!(
                sql.execute("SELECT EXISTS(SELECT 1 FROM p WHERE k=2)")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Bool(true)]]
            );
            drop(sql);
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
}
