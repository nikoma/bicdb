//! Bounds must be enforced before an acknowledged row or attachment changes.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn with_stores(mut run: impl FnMut(&mut BicDb)) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        run(&mut db);
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            let rows = SqlSession::new(&mut db)
                .execute("SELECT payload FROM ack_leaf ORDER BY payload")
                .unwrap()
                .rows;
            assert_eq!(rows, vec![vec![SqlValue::String("original-ack".into())]]);
            db.close().unwrap();
        }
    }
}

#[test]
fn range_insert_update_and_multirow_failure_preserve_ack_and_bounds() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF p FOR VALUES FROM(0) TO(10)")
            .unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(1,'original-ack')")
            .unwrap();
        for statement in [
            "INSERT INTO ack_leaf VALUES(10,'out')",
            "INSERT INTO ack_leaf VALUES(NULL,'out')",
            "INSERT INTO ack_leaf VALUES(2,'would-fit'),(12,'out')",
            "UPDATE ack_leaf SET k=10 WHERE k=1",
        ] {
            assert_eq!(
                sql.execute(statement).unwrap_err().sqlstate(),
                "23514",
                "{statement}"
            );
            assert_eq!(
                sql.execute("SELECT k,payload FROM ack_leaf").unwrap().rows,
                vec![vec![
                    SqlValue::Int(1),
                    SqlValue::String("original-ack".into())
                ]]
            );
        }
        // Neither the resident-row fast path nor an explicit transaction may
        // bypass the same bound check.
        sql.execute("BEGIN").unwrap();
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES(100,'out')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        sql.execute("ROLLBACK").unwrap();
    });
}

#[test]
fn attach_checks_existing_rows_overlap_and_actual_parent_before_publication() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("CREATE TABLE other_parent(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf(k INT,payload TEXT)")
            .unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(1,'original-ack')")
            .unwrap();
        assert_eq!(
            sql.execute("ALTER TABLE p ATTACH PARTITION ack_leaf FOR VALUES FROM(10) TO(20)")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        // Failed attachment leaves the relation independently writable.
        sql.execute("BEGIN").unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(100,'independent')")
            .unwrap();
        sql.execute("ROLLBACK").unwrap();
        sql.execute("ALTER TABLE p ATTACH PARTITION ack_leaf FOR VALUES FROM(0) TO(10)")
            .unwrap();
        assert!(sql
            .execute("CREATE TABLE overlap PARTITION OF p FOR VALUES FROM(9) TO(11)")
            .is_err());
        assert!(sql.execute("SELECT * FROM overlap").is_err());
        assert!(sql
            .execute("ALTER TABLE other_parent DETACH PARTITION ack_leaf")
            .is_err());
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES(10,'still-bound')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        sql.execute("ALTER TABLE p DETACH PARTITION ack_leaf")
            .unwrap();
        sql.execute("BEGIN").unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(100,'detached')")
            .unwrap();
        sql.execute("ROLLBACK").unwrap();
    });
}

#[test]
fn default_excludes_siblings_and_new_attachment_checks_default_acknowledged_rows() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF p DEFAULT")
            .unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(50,'original-ack')")
            .unwrap();
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES(1,'out')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert_eq!(
            sql.execute("CREATE TABLE steals_ack PARTITION OF p FOR VALUES FROM(40) TO(60)")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert!(sql.execute("SELECT * FROM steals_ack").is_err());
        sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
            .unwrap();
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES(11,'out')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert!(sql
            .execute("CREATE TABLE second_default PARTITION OF p DEFAULT")
            .is_err());
    });
}

#[test]
fn list_bounds_accept_null_and_quoted_commas_without_conflating_values() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k TEXT,payload TEXT) PARTITION BY LIST(k)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF p FOR VALUES IN('a,b',NULL)")
            .unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES('a,b','original-ack')")
            .unwrap();
        sql.execute("BEGIN").unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(NULL,'null-fit')")
            .unwrap();
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES('a','out')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert!(sql
            .execute("CREATE TABLE duplicate_null PARTITION OF p FOR VALUES IN(NULL)")
            .is_err());
        assert!(sql
            .execute("CREATE TABLE duplicate_text PARTITION OF p FOR VALUES IN('a,b')")
            .is_err());
        sql.execute("CREATE TABLE disjoint PARTITION OF p FOR VALUES IN('a','b')")
            .unwrap();
    });
}

#[test]
fn range_uses_typed_decimal_and_lexicographic_multicolumn_limits() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k NUMERIC,tie INT,payload TEXT) PARTITION BY RANGE(k,tie)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF p FOR VALUES FROM(0.5000000000000000000000,MINVALUE) TO(0.5000000000000000000002,MAXVALUE)").unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(0.5000000000000000000001,1,'original-ack')")
            .unwrap();
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES(0.5000000000000000000003,1,'out')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert!(sql.execute("CREATE TABLE malformed PARTITION OF p FOR VALUES FROM(MINVALUE,0) TO(MAXVALUE,MAXVALUE)").is_err());
        assert!(sql
            .execute("CREATE TABLE backwards PARTITION OF p FOR VALUES FROM(2,0) TO(1,0)")
            .is_err());
        assert!(sql.execute("CREATE TABLE bad_expression PARTITION OF p FOR VALUES FROM(current_date,0) TO(MAXVALUE,MAXVALUE)").is_err());
    });
}

#[test]
fn child_rls_is_independent_when_parent_policy_changes_and_after_reopen() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE ROLE bounds_reader").unwrap();
        sql.execute("CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("ALTER TABLE p ENABLE ROW LEVEL SECURITY")
            .unwrap();
        sql.execute("CREATE POLICY deny_parent ON p FOR SELECT USING(false)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF p FOR VALUES FROM(0) TO(10)")
            .unwrap();
        sql.execute("INSERT INTO ack_leaf VALUES(1,'original-ack')")
            .unwrap();
        sql.execute("GRANT SELECT ON ack_leaf TO bounds_reader")
            .unwrap();
        sql.execute("SET ROLE bounds_reader").unwrap();
        assert_eq!(
            sql.execute("SELECT payload FROM ack_leaf")
                .unwrap()
                .rows
                .len(),
            1
        );
        sql.execute("RESET ROLE").unwrap();
        sql.execute("ALTER TABLE ack_leaf ENABLE ROW LEVEL SECURITY")
            .unwrap();
        sql.execute("CREATE POLICY allow_child ON ack_leaf FOR SELECT USING(k=1)")
            .unwrap();
        sql.execute("ALTER TABLE p FORCE ROW LEVEL SECURITY")
            .unwrap();
        sql.execute("SET ROLE bounds_reader").unwrap();
        assert_eq!(
            sql.execute("SELECT payload FROM ack_leaf")
                .unwrap()
                .rows
                .len(),
            1
        );
        sql.execute("RESET ROLE").unwrap();
    });
}

#[test]
fn parent_insert_select_and_values_route_all_rows_before_any_mutation() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF p FOR VALUES FROM(0) TO(10)")
            .unwrap();
        sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
            .unwrap();
        sql.execute("INSERT INTO p VALUES(1,'original-ack')")
            .unwrap();
        // One unroutable row refuses the entire statement, including an
        // earlier row that could have been staged into another leaf.
        assert_eq!(
            sql.execute("INSERT INTO p VALUES(2,'must-rollback'),(30,'no-partition')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert_eq!(
            sql.execute("SELECT count(*) FROM p").unwrap().rows,
            vec![vec![SqlValue::Int(1)]]
        );
        sql.execute("BEGIN").unwrap();
        let inserted = sql
            .execute("INSERT INTO p VALUES(2,'low'),(12,'high') RETURNING k")
            .unwrap();
        assert_eq!(
            inserted.rows,
            vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(12)]]
        );
        assert_eq!(
            sql.execute("SELECT k FROM high").unwrap().rows,
            vec![vec![SqlValue::Int(12)]]
        );
        sql.execute("ROLLBACK").unwrap();
        sql.execute("CREATE TABLE source(k INT,payload TEXT)")
            .unwrap();
        sql.execute("INSERT INTO source VALUES(3,'select-low'),(13,'select-high')")
            .unwrap();
        sql.execute("BEGIN").unwrap();
        sql.execute("INSERT INTO p SELECT * FROM source").unwrap();
        assert_eq!(
            sql.execute("SELECT k FROM high").unwrap().rows,
            vec![vec![SqlValue::Int(13)]]
        );
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            sql.execute("SELECT count(*) FROM high").unwrap().rows,
            vec![vec![SqlValue::Int(0)]]
        );
    });
}

#[test]
fn parent_routes_through_attached_partitioned_subtree_and_preserves_leaf_ack() {
    with_stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)")
            .unwrap();
        sql.execute("CREATE TABLE branch(k INT,payload TEXT) PARTITION BY LIST(k)")
            .unwrap();
        sql.execute("CREATE TABLE ack_leaf PARTITION OF branch FOR VALUES IN(1)")
            .unwrap();
        sql.execute("ALTER TABLE p ATTACH PARTITION branch FOR VALUES FROM(0) TO(10)")
            .unwrap();
        sql.execute("INSERT INTO p VALUES(1,'original-ack')")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT payload FROM p").unwrap().rows,
            vec![vec![SqlValue::String("original-ack".into())]]
        );
        assert_eq!(
            sql.execute("INSERT INTO p VALUES(2,'no-nested-match')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert_eq!(
            sql.execute("INSERT INTO ack_leaf VALUES(11,'outside-ancestor')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert!(sql
            .execute("ALTER TABLE branch ATTACH PARTITION p DEFAULT")
            .is_err());
    });
}
