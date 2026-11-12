use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{check_sql_partition_admission, SqlSession, SqlValue};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

fn setup(db: &mut BicDb, high: &str) {
    let mut sql = SqlSession::new(db);
    for statement in [
        "CREATE TABLE p(k INT PRIMARY KEY,raw TEXT,qty INT) PARTITION BY RANGE(k)",
        "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)",
        "CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)",
        "INSERT INTO low VALUES(2,'12',3)",
        &format!("INSERT INTO high VALUES(12,'{high}',4)"),
    ] {
        sql.execute(statement).unwrap();
    }
}

fn raw_snapshot(db: &BicDb) -> serde_json::Value {
    let mut snapshot = serde_json::Map::new();
    for name in ["p", "low", "high", "__bicdb_pg_schema"] {
        let mut records = db.scan_collection(name).unwrap();
        records.sort_by(|a, b| a.id.cmp(&b.id));
        snapshot.insert(name.into(), serde_json::to_value(records).unwrap());
    }
    serde_json::Value::Object(snapshot)
}

#[test]
fn parent_type_change_publishes_all_leaf_rows_and_schemas_in_one_native_commit() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "20");
        let before = db.current_commit_seq();
        SqlSession::new(&mut db)
            .execute("ALTER TABLE p ALTER COLUMN raw TYPE INT USING raw::int+qty")
            .unwrap();
        assert_eq!(db.current_commit_seq() - before, 1);
        for _ in 0..2 {
            check_sql_partition_admission(&db).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT k,raw,pg_typeof(raw)::text FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![
                        SqlValue::Int(2),
                        SqlValue::Int(15),
                        SqlValue::String("integer".into())
                    ],
                    vec![
                        SqlValue::Int(12),
                        SqlValue::Int(24),
                        SqlValue::String("integer".into())
                    ],
                ]
            );
            assert_eq!(db.scan_collection("p").unwrap().len(), 0);
            for name in ["p", "low", "high"] {
                let raw = db.get("__bicdb_pg_schema", name).unwrap().unwrap();
                assert!(raw.metadata["columns"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|c| c["name"] == "raw" && c["pg_type"] == "int4"));
            }
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn failed_cast_in_a_later_leaf_preserves_all_raw_acknowledged_rows_and_schemas() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "not-an-integer");
        let before = raw_snapshot(&db);
        let sequence = db.current_commit_seq();
        assert!(SqlSession::new(&mut db)
            .execute("ALTER TABLE p ALTER COLUMN raw TYPE INT USING raw::int+qty")
            .is_err());
        assert_eq!(db.current_commit_seq(), sequence);
        for _ in 0..2 {
            check_sql_partition_admission(&db).unwrap();
            assert_eq!(raw_snapshot(&db), before);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn savepoint_and_full_rollback_restore_the_complete_partition_type_transition() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "20");
        let before = raw_snapshot(&db);
        for rollback in ["ROLLBACK TO s", "ROLLBACK"] {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("BEGIN").unwrap();
            sql.execute("SAVEPOINT s").unwrap();
            sql.execute("ALTER TABLE p ALTER COLUMN raw TYPE INT USING raw::int+qty")
                .unwrap();
            assert_eq!(
                sql.execute("SELECT SUM(raw) FROM p").unwrap().rows,
                vec![vec![SqlValue::Int(39)]]
            );
            sql.execute(rollback).unwrap();
            if rollback == "ROLLBACK TO s" {
                sql.execute("COMMIT").unwrap();
            }
            drop(sql);
            check_sql_partition_admission(&db).unwrap();
            assert_eq!(raw_snapshot(&db), before);
        }
        for _ in 0..2 {
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            check_sql_partition_admission(&db).unwrap();
            assert_eq!(raw_snapshot(&db), before);
        }
        db.close().unwrap();
    }
}

#[test]
fn parent_added_defaults_and_generated_values_reach_both_leaves_atomically() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "20");
        for ddl in [
            "ALTER TABLE p ADD COLUMN note TEXT DEFAULT 'new' NOT NULL",
            "ALTER TABLE p ADD COLUMN doubled INT GENERATED ALWAYS AS(qty*2) STORED",
        ] {
            let before = db.current_commit_seq();
            SqlSession::new(&mut db).execute(ddl).unwrap();
            assert_eq!(db.current_commit_seq() - before, 1);
        }
        for _ in 0..2 {
            check_sql_partition_admission(&db).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT k,note,doubled FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![
                        SqlValue::Int(2),
                        SqlValue::String("new".into()),
                        SqlValue::Int(6)
                    ],
                    vec![
                        SqlValue::Int(12),
                        SqlValue::String("new".into()),
                        SqlValue::Int(8)
                    ],
                ]
            );
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn failed_partition_add_not_null_preserves_rows_schema_and_commit_sequence() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        setup(&mut db, "20");
        let before = raw_snapshot(&db);
        let sequence = db.current_commit_seq();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("ALTER TABLE p ADD COLUMN required TEXT NOT NULL")
                .unwrap_err()
                .sqlstate(),
            "23502"
        );
        assert_eq!(db.current_commit_seq(), sequence);
        assert_eq!(raw_snapshot(&db), before);
        check_sql_partition_admission(&db).unwrap();
        db.close().unwrap();
    }
}

#[test]
fn rollback_of_added_partition_default_restores_all_raw_rows_and_descriptors() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db, "20");
        let before = raw_snapshot(&db);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("BEGIN").unwrap();
        sql.execute("ALTER TABLE p ADD COLUMN note TEXT DEFAULT 'new'")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT COUNT(*) FROM p WHERE note='new'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)]]
        );
        sql.execute("ROLLBACK").unwrap();
        drop(sql);
        for _ in 0..2 {
            check_sql_partition_admission(&db).unwrap();
            assert_eq!(raw_snapshot(&db), before);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}
