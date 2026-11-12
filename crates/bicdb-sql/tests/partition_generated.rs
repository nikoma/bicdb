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
            db,
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

#[test]
fn before_triggers_see_pending_generation_and_computed_keys_use_their_base_changes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in ["CREATE TABLE p(k INT,b INT,g INT GENERATED ALWAYS AS(b*2) STORED,PRIMARY KEY(k,g)) PARTITION BY RANGE(k)","CREATE TABLE high(k INT,b INT,g INT GENERATED ALWAYS AS(b*3) STORED,PRIMARY KEY(k,g))","ALTER TABLE p ATTACH PARTITION high FOR VALUES FROM(10) TO(20)","CREATE FUNCTION generated_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.g IS NOT NULL THEN RAISE EXCEPTION 'generated NEW visible before evaluation'; END IF; NEW.b:=NEW.b+1; NEW.g:=999; RETURN NEW; END $$","CREATE TRIGGER generated_before BEFORE INSERT OR UPDATE ON high FOR EACH ROW EXECUTE FUNCTION generated_before()","INSERT INTO p(k,b) VALUES(12,1)","UPDATE p SET b=2 WHERE k=12"] {sql.execute(text).unwrap();}
        assert_eq!(
            sql.execute("UPDATE high SET b=3 WHERE k=12 RETURNING g")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(12)]]
        );
        drop(sql);
        assert!(db.get("high", r#"["12","6"]"#).unwrap().is_none());
        assert!(db.get("high", r#"["12","9"]"#).unwrap().is_none());
        assert!(db.get("high", r#"["12","12"]"#).unwrap().is_some());
        expect_reopens(db, root.path(), &cfg, &[[12, 4, 12]]);
    }
}

#[test]
fn ordinary_computed_keys_share_the_safe_insert_copy_and_update_plan() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE ordinary(b INT,g INT GENERATED ALWAYS AS(b*2) STORED PRIMARY KEY)",
        )
        .unwrap();
        sql.execute("INSERT INTO ordinary(b) VALUES(2)").unwrap();
        sql.copy_insert_rows("ordinary", &["b".into()], vec![vec![Some("3".into())]])
            .unwrap();
        sql.execute("CREATE ROLE generated_writer").unwrap();
        sql.execute("GRANT SELECT,INSERT,UPDATE ON ordinary TO generated_writer")
            .unwrap();
        sql.execute("SET ROLE generated_writer").unwrap();
        assert_eq!(
            sql.execute("UPDATE ordinary SET b=4 WHERE g=4 RETURNING g")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(8)]]
        );
        assert_eq!(
            sql.execute("UPDATE ordinary SET b=4 WHERE g=6")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        assert_eq!(
            sql.execute("INSERT INTO ordinary(b,g) VALUES(5,100)")
                .unwrap_err()
                .sqlstate(),
            "428C9"
        );
        drop(sql);
        assert!(db.get("ordinary", "4").unwrap().is_none());
        assert!(db.get("ordinary", "6").unwrap().is_some());
        assert!(db.get("ordinary", "8").unwrap().is_some());
        for _ in 0..2 {
            bicdb_sql::ensure_primary_key_indexes(&mut db).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT b,g FROM ordinary ORDER BY g")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(3), SqlValue::Int(6)],
                    vec![SqlValue::Int(4), SqlValue::Int(8)]
                ]
            );
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn generated_text_and_json_null_primary_keys_never_store_preparation_identities() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in ["CREATE TABLE text_key(b INT,g TEXT GENERATED ALWAYS AS(b::text||'!') STORED PRIMARY KEY)","INSERT INTO text_key(b) VALUES(2),(3)","UPDATE text_key SET b=4 WHERE g='2!'","CREATE TABLE json_key(b INT,g JSONB GENERATED ALWAYS AS(coalesce(b::text,'null')::jsonb) STORED PRIMARY KEY)","INSERT INTO json_key(b) VALUES(NULL),(2)"] {sql.execute(text).unwrap();}
        drop(sql);
        assert!(db.get("text_key", "2!").unwrap().is_none());
        assert!(db.get("text_key", "3!").unwrap().is_some());
        assert!(db.get("text_key", "4!").unwrap().is_some());
        for _ in 0..2 {
            bicdb_sql::ensure_primary_key_indexes(&mut db).unwrap();
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT g FROM text_key ORDER BY g")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::String("3!".into())],
                    vec![SqlValue::String("4!".into())]
                ]
            );
            assert_eq!(
                sql.execute("SELECT count(*) FROM json_key WHERE g='null'::jsonb")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1)]]
            );
            assert_eq!(
                sql.execute("SELECT count(*) FROM json_key WHERE g='2'::jsonb")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1)]]
            );
            drop(sql);
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|report| report.valid));
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn computed_key_upsert_retires_the_old_id_and_savepoint_undo_restores_it() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE TABLE ordinary(b INT,g INT GENERATED ALWAYS AS(b*2) STORED PRIMARY KEY)",
            "INSERT INTO ordinary(b) VALUES(3)",
            "BEGIN",
            "SAVEPOINT s",
        ] {
            sql.execute(text).unwrap();
        }
        assert_eq!(
            sql.execute(
                "INSERT INTO ordinary(b) VALUES(3) ON CONFLICT(g) DO UPDATE SET b=10 RETURNING g"
            )
            .unwrap()
            .rows,
            vec![vec![SqlValue::Int(20)]]
        );
        sql.execute("ROLLBACK TO s").unwrap();
        sql.execute("COMMIT").unwrap();
        assert_eq!(
            sql.execute("SELECT g FROM ordinary").unwrap().rows,
            vec![vec![SqlValue::Int(6)]]
        );
        sql.execute("INSERT INTO ordinary(b) VALUES(3) ON CONFLICT(g) DO UPDATE SET b=10")
            .unwrap();
        drop(sql);
        for _ in 0..2 {
            assert!(db.get("ordinary", "6").unwrap().is_none());
            assert!(db.get("ordinary", "20").unwrap().is_some());
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT count(*) FROM ordinary")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1)]]
            );
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn generated_child_keys_follow_foreign_key_update_and_set_null_actions() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in ["CREATE TABLE cascade_parent(k INT PRIMARY KEY)","CREATE TABLE cascade_child(b INT REFERENCES cascade_parent(k) ON UPDATE CASCADE ON DELETE SET NULL,g INT GENERATED ALWAYS AS(coalesce(b,0)*2) STORED PRIMARY KEY)","INSERT INTO cascade_parent VALUES(1)","INSERT INTO cascade_child(b) VALUES(1)","UPDATE cascade_parent SET k=2 WHERE k=1"] {sql.execute(text).unwrap();}
        assert_eq!(
            sql.execute("SELECT b,g FROM cascade_child").unwrap().rows,
            vec![vec![SqlValue::Int(2), SqlValue::Int(4)]]
        );
        sql.execute("DELETE FROM cascade_parent WHERE k=2").unwrap();
        drop(sql);
        for _ in 0..2 {
            assert!(db.get("cascade_child", "2").unwrap().is_none());
            assert!(db.get("cascade_child", "4").unwrap().is_none());
            assert!(db.get("cascade_child", "0").unwrap().is_some());
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT b,g FROM cascade_child")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Null, SqlValue::Int(0)]]
            );
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn late_null_generated_keys_leave_every_previously_acknowledged_row_unchanged() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for partitioned in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            let mut sql = SqlSession::new(&mut db);
            if partitioned {
                sql.execute("CREATE TABLE p(k INT,b INT,g INT GENERATED ALWAYS AS(b*2) STORED,PRIMARY KEY(k,g)) PARTITION BY RANGE(k)").unwrap();
                sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
                    .unwrap();
                sql.execute("INSERT INTO p(k,b) VALUES(1,2)").unwrap();
            } else {
                sql.execute(
                    "CREATE TABLE low(b INT,g INT GENERATED ALWAYS AS(b*2) STORED PRIMARY KEY)",
                )
                .unwrap();
                sql.execute("INSERT INTO low(b) VALUES(2)").unwrap();
            }
            drop(sql);
            let before = db
                .scan_collection("low")
                .unwrap()
                .into_iter()
                .map(|r| r.content_hash().unwrap())
                .collect::<Vec<_>>();
            let sequence = db.last_commit_seq();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute(if partitioned {
                        "INSERT INTO p(k,b) VALUES(2,3),(3,NULL)"
                    } else {
                        "INSERT INTO low(b) VALUES(3),(NULL)"
                    })
                    .unwrap_err()
                    .sqlstate(),
                "23502"
            );
            assert_eq!(db.last_commit_seq(), sequence);
            assert_eq!(
                db.scan_collection("low")
                    .unwrap()
                    .into_iter()
                    .map(|r| r.content_hash().unwrap())
                    .collect::<Vec<_>>(),
                before
            );
            db.close().unwrap();
            let db = BicDb::open_with_config(root.path(), cfg).unwrap();
            assert_eq!(
                db.scan_collection("low")
                    .unwrap()
                    .into_iter()
                    .map(|r| r.content_hash().unwrap())
                    .collect::<Vec<_>>(),
                before
            );
            db.close().unwrap();
        }
    }
}
