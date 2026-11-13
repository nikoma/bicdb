use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn setup(db: &mut BicDb) {
    let mut sql = SqlSession::new(db);
    for statement in [
        "CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)",
        "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)",
        "CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)",
        "INSERT INTO p VALUES(1,'original-ack')",
    ] {
        sql.execute(statement).unwrap();
    }
}
fn data(keys: &[i64]) -> Vec<Vec<Option<String>>> {
    keys.iter()
        .map(|k| vec![Some(k.to_string()), Some(format!("copy-{k}"))])
        .collect()
}
fn columns() -> Vec<String> {
    vec!["k".into(), "payload".into()]
}

#[test]
fn copy_routes_each_physical_leaf_in_one_commit_and_late_failure_rolls_back() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let sequence = db.last_commit_seq();
        assert_eq!(
            SqlSession::new(&mut db)
                .copy_insert_rows("p", &columns(), data(&[2, 12, 3]))
                .unwrap(),
            3
        );
        assert_eq!(db.last_commit_seq() - sequence, 1);
        assert!(db.scan_collection("p").unwrap().is_empty());
        let error = SqlSession::new(&mut db)
            .copy_insert_rows("p", &columns(), data(&[4, 30]))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "23514");
        for reopen in 0..3 {
            assert!(db.scan_collection("p").unwrap().is_empty());
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT k FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(1)],
                    vec![SqlValue::Int(2)],
                    vec![SqlValue::Int(3)],
                    vec![SqlValue::Int(12)]
                ]
            );
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|report| report.valid));
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
}

#[test]
fn copy_uses_leaf_triggers_and_each_before_row_sees_prior_physical_writes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE audit(seq BIGSERIAL,phase TEXT,k INT,rel TEXT,seen INT)")
            .unwrap();
        sql.execute("CREATE FUNCTION capture_copy() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO audit(phase,k,rel,seen) VALUES(TG_WHEN,NEW.k,TG_TABLE_NAME,(SELECT COUNT(*) FROM p)); RETURN NEW; END$$").unwrap();
        sql.execute(
            "CREATE TRIGGER copy_b BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION capture_copy()",
        )
        .unwrap();
        sql.execute(
            "CREATE TRIGGER copy_a AFTER INSERT ON p FOR EACH ROW EXECUTE FUNCTION capture_copy()",
        )
        .unwrap();
        assert_eq!(
            sql.copy_insert_rows("p", &columns(), data(&[2, 12, 3]))
                .unwrap(),
            3
        );
        drop(sql);
        let expected = [
            ("BEFORE", 2, "low", 1),
            ("BEFORE", 12, "high", 2),
            ("BEFORE", 3, "low", 3),
            ("AFTER", 2, "low", 4),
            ("AFTER", 12, "high", 4),
            ("AFTER", 3, "low", 4),
        ]
        .into_iter()
        .map(|(phase, k, rel, n)| {
            vec![
                SqlValue::String(phase.into()),
                SqlValue::Int(k),
                SqlValue::String(rel.into()),
                SqlValue::Int(n),
            ]
        })
        .collect::<Vec<_>>();
        for reopen in 0..3 {
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT phase,k,rel,seen FROM audit ORDER BY seq")
                    .unwrap()
                    .rows,
                expected
            );
            assert!(db.scan_collection("p").unwrap().is_empty());
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
}

#[test]
fn copy_preserves_parent_insert_privilege_and_rls_without_child_sql_grants() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        for statement in [
            "CREATE ROLE copy_writer",
            "GRANT INSERT ON p TO copy_writer",
            "ALTER TABLE p ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE p FORCE ROW LEVEL SECURITY",
            "CREATE POLICY allow_small ON p FOR INSERT WITH CHECK(k<5)",
            "SET ROLE copy_writer",
        ] {
            sql.execute(statement).unwrap();
        }
        assert_eq!(
            sql.copy_insert_rows("p", &columns(), data(&[2])).unwrap(),
            1
        );
        assert_eq!(
            sql.copy_insert_rows("p", &columns(), data(&[3, 12]))
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        sql.execute("RESET ROLE").unwrap();
        // The COPY checks above deliberately had no SELECT policy. Give the
        // verification query one only after the allowed/refused writes have
        // finished; FORCE RLS otherwise correctly hides this postcondition.
        sql.execute("CREATE POLICY verify_ack ON p FOR SELECT USING(true)")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT k FROM p ORDER BY k").unwrap().rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        drop(sql);
        assert!(db.scan_collection("p").unwrap().is_empty());
        assert_eq!(db.scan_collection("low").unwrap().len(), 2);
        assert!(db.scan_collection("high").unwrap().is_empty());
        db.close().unwrap();
    }
}
