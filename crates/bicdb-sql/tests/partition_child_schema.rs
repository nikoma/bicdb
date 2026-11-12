use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{ensure_primary_key_indexes, SqlSession, SqlValue};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

fn setup(db: &mut BicDb) {
    let mut sql = SqlSession::new(db);
    for text in [
        "CREATE TABLE p(k INT PRIMARY KEY,payload TEXT DEFAULT 'parent',qty INT DEFAULT 10) PARTITION BY RANGE(k)",
        "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)",
        "CREATE TABLE high(k INT PRIMARY KEY,payload TEXT DEFAULT 'high',qty INT DEFAULT 20)",
        "ALTER TABLE p ATTACH PARTITION high FOR VALUES FROM(10) TO(20)",
        "ALTER TABLE low ALTER COLUMN payload SET DEFAULT 'low'",
    ] {
        sql.execute(text).unwrap();
    }
}

fn row(k: i64, payload: &str, qty: SqlValue) -> Vec<SqlValue> {
    vec![SqlValue::Int(k), SqlValue::String(payload.into()), qty]
}

fn rows(db: &mut BicDb) -> Vec<Vec<SqlValue>> {
    SqlSession::new(db)
        .execute("SELECT k,payload,qty FROM p ORDER BY k")
        .unwrap()
        .rows
}

#[test]
fn local_child_and_parent_routed_defaults_use_the_statement_relation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "INSERT INTO low(k) VALUES(1)",
            "INSERT INTO p(k) VALUES(2)",
            "INSERT INTO high(k) VALUES(11)",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        let expected = vec![
            row(1, "low", SqlValue::Int(10)),
            row(2, "parent", SqlValue::Int(10)),
            row(11, "high", SqlValue::Int(20)),
        ];
        for _ in 0..2 {
            assert_eq!(rows(&mut db), expected);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn parent_add_and_type_preserve_unrelated_child_defaults() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "INSERT INTO low VALUES(1,'low-ack',10)",
            "INSERT INTO high VALUES(11,'high-ack',20)",
            "ALTER TABLE p ADD COLUMN note TEXT DEFAULT 'new'",
            "ALTER TABLE p ALTER COLUMN payload TYPE VARCHAR(40) USING payload||'!'",
            "INSERT INTO low(k) VALUES(2)",
            "INSERT INTO high(k) VALUES(12)",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        let expected = vec![
            row(1, "low-ack!", SqlValue::Int(10)),
            row(2, "low", SqlValue::Int(10)),
            row(11, "high-ack!", SqlValue::Int(20)),
            row(12, "high", SqlValue::Int(20)),
        ];
        for _ in 0..2 {
            assert_eq!(rows(&mut db), expected);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn parent_set_and_drop_default_propagate_only_the_target_column_atomically() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let before = db.current_commit_seq();
        SqlSession::new(&mut db)
            .execute("ALTER TABLE p ALTER COLUMN qty SET DEFAULT 30")
            .unwrap();
        assert_eq!(db.current_commit_seq() - before, 1);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO low(k) VALUES(1)").unwrap();
        sql.execute("INSERT INTO high(k) VALUES(11)").unwrap();
        drop(sql);
        let before = db.current_commit_seq();
        SqlSession::new(&mut db)
            .execute("ALTER TABLE p ALTER COLUMN qty DROP DEFAULT")
            .unwrap();
        assert_eq!(db.current_commit_seq() - before, 1);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO low(k) VALUES(2)").unwrap();
        sql.execute("INSERT INTO high(k) VALUES(12)").unwrap();
        drop(sql);
        let expected = vec![
            row(1, "low", SqlValue::Int(30)),
            row(2, "low", SqlValue::Null),
            row(11, "high", SqlValue::Int(30)),
            row(12, "high", SqlValue::Null),
        ];
        for _ in 0..2 {
            assert_eq!(rows(&mut db), expected);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn independent_child_primary_key_survives_attach_parent_ddl_and_parent_insert() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)",
            "CREATE TABLE low(k INT,payload TEXT PRIMARY KEY)",
            "ALTER TABLE p ATTACH PARTITION low FOR VALUES FROM(0) TO(10)",
            "INSERT INTO low VALUES(1,'direct-ack')",
            "ALTER TABLE p ADD COLUMN note TEXT",
            "ALTER TABLE p ALTER COLUMN payload TYPE VARCHAR(40)",
            "INSERT INTO p(k,payload) VALUES(2,'routed-ack')",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        for _ in 0..2 {
            ensure_primary_key_indexes(&mut db).unwrap();
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,payload FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(1), SqlValue::String("direct-ack".into())],
                    vec![SqlValue::Int(2), SqlValue::String("routed-ack".into())]
                ]
            );
            assert_eq!(sql.execute("SELECT count(*) FROM pg_constraint WHERE conrelid='low'::regclass AND contype='p'").unwrap().rows,vec![vec![SqlValue::Int(1)]]);
            assert_eq!(
                sql.execute("INSERT INTO p(k,payload) VALUES(3,'direct-ack')")
                    .unwrap_err()
                    .sqlstate(),
                "23505"
            );
            drop(sql);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn parent_updates_and_deletes_keep_the_independent_leaf_key_and_lock_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)",
            "CREATE TABLE low(k INT,payload TEXT PRIMARY KEY)",
            "ALTER TABLE p ATTACH PARTITION low FOR VALUES FROM(0) TO(10)",
            "INSERT INTO low VALUES(1,'one')",
            "INSERT INTO p VALUES(2,'two')",
            "BEGIN",
            "SELECT k,payload FROM p ORDER BY k FOR UPDATE",
        ] {
            sql.execute(text).unwrap();
        }
        assert_eq!(
            sql.execute("UPDATE p SET payload='renamed' WHERE k=1 RETURNING k,payload")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1), SqlValue::String("renamed".into())]]
        );
        assert_eq!(
            sql.execute("DELETE FROM p WHERE payload='two' RETURNING k")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)]]
        );
        sql.execute("COMMIT").unwrap();
        drop(sql);
        assert!(db.get("low", "one").unwrap().is_none());
        assert!(db.get("low", "two").unwrap().is_none());
        assert!(db.get("low", "renamed").unwrap().is_some());
        for _ in 0..2 {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,payload FROM p WHERE payload='renamed'")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1), SqlValue::String("renamed".into())]]
            );
            assert_eq!(
                sql.execute("INSERT INTO p VALUES(3,'renamed')")
                    .unwrap_err()
                    .sqlstate(),
                "23505"
            );
            drop(sql);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn moving_between_different_child_keys_preserves_acknowledged_values_and_rollback() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE TABLE p(k INT,payload TEXT) PARTITION BY RANGE(k)",
            "CREATE TABLE low(k INT,payload TEXT PRIMARY KEY)",
            "CREATE TABLE high(k INT,payload TEXT,PRIMARY KEY(k,payload))",
            "ALTER TABLE p ATTACH PARTITION low FOR VALUES FROM(0) TO(10)",
            "ALTER TABLE p ATTACH PARTITION high FOR VALUES FROM(10) TO(20)",
            "INSERT INTO low VALUES(1,'ack')",
            "INSERT INTO high VALUES(13,'other')",
        ] {
            sql.execute(text).unwrap();
        }
        assert_eq!(
            sql.execute("UPDATE p SET k=12 WHERE k=1 RETURNING k,payload")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(12), SqlValue::String("ack".into())]]
        );
        sql.execute("BEGIN").unwrap();
        sql.execute("SAVEPOINT s").unwrap();
        sql.execute("UPDATE p SET k=2 WHERE k=12").unwrap();
        assert_eq!(
            sql.execute("SELECT k,payload FROM low").unwrap().rows,
            vec![vec![SqlValue::Int(2), SqlValue::String("ack".into())]]
        );
        sql.execute("ROLLBACK TO s").unwrap();
        sql.execute("COMMIT").unwrap();
        drop(sql);
        for _ in 0..2 {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,payload FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(12), SqlValue::String("ack".into())],
                    vec![SqlValue::Int(13), SqlValue::String("other".into())]
                ]
            );
            drop(sql);
            assert_eq!(db.scan_collection("low").unwrap().len(), 0);
            assert_eq!(db.scan_collection("high").unwrap().len(), 2);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}
