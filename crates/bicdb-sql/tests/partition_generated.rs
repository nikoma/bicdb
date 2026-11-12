use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn setup(db: &mut BicDb, expression: &str) {
    let mut sql = SqlSession::new(db);
    for text in [
        format!("CREATE TABLE p(k INT PRIMARY KEY,b INT,g INT GENERATED ALWAYS AS({expression}) STORED) PARTITION BY RANGE(k)"),
        "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)".into(),
        "CREATE TABLE high(k INT PRIMARY KEY,b INT,g INT GENERATED ALWAYS AS(b*3) STORED)".into(),
        "ALTER TABLE p ATTACH PARTITION high FOR VALUES FROM(10) TO(20)".into(),
    ] {sql.execute(&text).unwrap();}
}
fn expect_reopens(mut db: BicDb, root: &std::path::Path, cfg: &DbConfig, expected: &[[i64; 3]]) {
    for reopen in 0..3 {
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT k,b,g FROM p ORDER BY k,g")
                .unwrap()
                .rows,
            expected
                .iter()
                .map(|row| row.iter().copied().map(SqlValue::Int).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
        assert!(db
            .verify_indexes_consistently()
            .unwrap()
            .iter()
            .all(|report| report.valid));
        db.close().unwrap();
        if reopen < 2 {
            db = BicDb::open_with_config(root, cfg.clone()).unwrap();
        } else {
            return;
        }
    }
}

#[test]
fn insert_copy_upsert_and_parent_add_keep_the_physical_generated_expression() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "b*2");
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "INSERT INTO p(k,b) VALUES(1,10),(11,10)",
            "ALTER TABLE p ADD COLUMN note TEXT DEFAULT 'new'",
            "INSERT INTO p(k,b) VALUES(11,20) ON CONFLICT(k) DO UPDATE SET b=excluded.b",
            "INSERT INTO high(k,b) VALUES(12,14)",
        ] {
            sql.execute(text).unwrap();
        }
        assert_eq!(
            sql.copy_insert_rows(
                "p",
                &["k".into(), "b".into()],
                vec![vec![Some("13".into()), Some("15".into())]]
            )
            .unwrap(),
            1
        );
        drop(sql);
        expect_reopens(
            &mut db,
            root.path(),
            &cfg,
            &[[1, 10, 20], [11, 20, 60], [12, 14, 42], [13, 15, 45]],
        );
    }
}

#[test]
fn parent_update_and_returning_use_the_actual_source_leaf_expression() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "b*2");
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO p(k,b) VALUES(11,10)").unwrap();
        assert_eq!(
            sql.execute("UPDATE p SET b=11 WHERE k=11 RETURNING k,b,g")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(11),
                SqlValue::Int(11),
                SqlValue::Int(33)
            ]]
        );
        sql.execute("UPDATE high SET b=12 WHERE k=11").unwrap();
        drop(sql);
        expect_reopens(db, root.path(), &cfg, &[[11, 12, 36]]);
    }
}

#[test]
fn unused_logical_parent_expression_cannot_raise_on_a_physical_child_update() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "100/(k-10)");
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO p(k,b) VALUES(10,2)").unwrap();
        assert_eq!(
            sql.execute("UPDATE p SET b=3 WHERE k=10 RETURNING g")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(9)]]
        );
        drop(sql);
        expect_reopens(db, root.path(), &cfg, &[[10, 3, 9]]);
    }
}

#[test]
fn movement_recomputes_destination_generation_and_savepoint_undo_restores_it() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "b*2");
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO p(k,b) VALUES(1,10),(11,10)")
            .unwrap();
        assert_eq!(
            sql.execute("UPDATE p SET k=12 WHERE k=1 RETURNING g")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(30)]]
        );
        for text in [
            "BEGIN",
            "SAVEPOINT s",
            "UPDATE p SET k=2 WHERE k=12",
            "ROLLBACK TO s",
            "COMMIT",
        ] {
            sql.execute(text).unwrap();
        }
        assert_eq!(
            sql.execute("SELECT g FROM high WHERE k=12").unwrap().rows,
            vec![vec![SqlValue::Int(30)]]
        );
        sql.execute("UPDATE p SET b=12 WHERE k=12").unwrap();
        drop(sql);
        expect_reopens(db, root.path(), &cfg, &[[11, 10, 30], [12, 12, 36]]);
    }
}

#[test]
fn source_generated_error_precedes_movement_and_preserves_every_raw_ack() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "100/(k-10)");
        SqlSession::new(&mut db)
            .execute("INSERT INTO p(k,b) VALUES(9,1),(10,3)")
            .unwrap();
        let before = ["p", "low", "high"]
            .into_iter()
            .flat_map(|name| {
                db.scan_collection(name)
                    .unwrap()
                    .into_iter()
                    .map(move |row| (name.to_string(), row.content_hash().unwrap()))
            })
            .collect::<Vec<_>>();
        let sequence = db.last_commit_seq();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("UPDATE p SET k=10,b=4 WHERE k=9")
                .unwrap_err()
                .sqlstate(),
            "22012"
        );
        assert_eq!(db.last_commit_seq(), sequence);
        let after = ["p", "low", "high"]
            .into_iter()
            .flat_map(|name| {
                db.scan_collection(name)
                    .unwrap()
                    .into_iter()
                    .map(move |row| (name.to_string(), row.content_hash().unwrap()))
            })
            .collect::<Vec<_>>();
        assert_eq!(after, before);
        expect_reopens(db, root.path(), &cfg, &[[9, 1, -100], [10, 3, 9]]);
    }
}

#[test]
fn generated_composite_primary_keys_use_final_leaf_values_and_canonical_native_ids() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in ["CREATE TABLE p(k INT,b INT,g INT GENERATED ALWAYS AS(b*2) STORED,PRIMARY KEY(k,g)) PARTITION BY RANGE(k)","CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)","CREATE TABLE high(k INT,b INT,g INT GENERATED ALWAYS AS(b*3) STORED,PRIMARY KEY(k,g))","ALTER TABLE p ATTACH PARTITION high FOR VALUES FROM(10) TO(20)","INSERT INTO p(k,b) VALUES(11,10)","UPDATE p SET b=11 WHERE k=11","INSERT INTO p(k,b) VALUES(11,12)"] {sql.execute(text).unwrap();}
        drop(sql);
        assert!(db.get("high", r#"["11","30"]"#).unwrap().is_none());
        assert!(db.get("high", r#"["11","33"]"#).unwrap().is_some());
        assert!(db.get("high", r#"["11","36"]"#).unwrap().is_some());
        expect_reopens(db, root.path(), &cfg, &[[11, 11, 33], [11, 12, 36]]);
    }
}
