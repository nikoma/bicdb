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

#[test]
fn parent_default_savepoint_rollback_restores_exact_child_definitions() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let before: Vec<_> = ["p", "low", "high"]
            .iter()
            .map(|name| {
                db.get("__bicdb_pg_schema", name)
                    .unwrap()
                    .unwrap()
                    .metadata
                    .clone()
            })
            .collect();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "BEGIN",
            "SAVEPOINT s",
            "ALTER TABLE p ALTER COLUMN qty SET DEFAULT 30",
            "INSERT INTO low(k) VALUES(1)",
            "ROLLBACK TO s",
            "COMMIT",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        let after: Vec<_> = ["p", "low", "high"]
            .iter()
            .map(|name| {
                db.get("__bicdb_pg_schema", name)
                    .unwrap()
                    .unwrap()
                    .metadata
                    .clone()
            })
            .collect();
        assert_eq!(after, before);
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT count(*) FROM p")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(0)]]
        );
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO low(k) VALUES(1)").unwrap();
        sql.execute("INSERT INTO high(k) VALUES(11)").unwrap();
        drop(sql);
        for _ in 0..2 {
            assert_eq!(
                rows(&mut db),
                vec![
                    row(1, "low", SqlValue::Int(10)),
                    row(11, "high", SqlValue::Int(20))
                ]
            );
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn identity_add_populates_existing_partitions_and_shares_one_sequence() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO p VALUES(1,'ack',10)").unwrap();
        sql.execute("ALTER TABLE p ADD COLUMN ident BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)").unwrap();
        sql.execute("INSERT INTO low(k) VALUES(2)").unwrap();
        sql.execute("INSERT INTO high(k) VALUES(12)").unwrap();
        sql.execute("INSERT INTO p(k) VALUES(3)").unwrap();
        drop(sql);
        for _ in 0..2 {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,payload,qty,ident FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![
                        SqlValue::Int(1),
                        SqlValue::String("ack".into()),
                        SqlValue::Int(10),
                        SqlValue::Int(100)
                    ],
                    vec![
                        SqlValue::Int(2),
                        SqlValue::String("low".into()),
                        SqlValue::Int(10),
                        SqlValue::Int(105)
                    ],
                    vec![
                        SqlValue::Int(3),
                        SqlValue::String("parent".into()),
                        SqlValue::Int(10),
                        SqlValue::Int(115)
                    ],
                    vec![
                        SqlValue::Int(12),
                        SqlValue::String("high".into()),
                        SqlValue::Int(20),
                        SqlValue::Int(110)
                    ],
                ]
            );
            assert_eq!(
                sql.execute("SELECT last_value FROM p_ident_seq")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(115)]]
            );
            drop(sql);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn identity_created_before_partitions_is_shared_by_parent_and_children() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE TABLE p(k INT) PARTITION BY RANGE(k)",
            "ALTER TABLE p ADD COLUMN ident BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)",
            "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)",
            "INSERT INTO p(k) VALUES(1)",
        ] {sql.execute(text).unwrap();}
        assert_eq!(sql.execute("SELECT is_identity,column_default FROM information_schema.columns WHERE table_name='low' AND column_name='ident'").unwrap().rows,vec![vec![SqlValue::String("YES".into()),SqlValue::Null]]);
        sql.execute("INSERT INTO low(k) VALUES(2)").unwrap();
        assert_eq!(
            sql.execute("INSERT INTO low(k,ident) VALUES(3,9000)")
                .unwrap_err()
                .sqlstate(),
            "428C9"
        );
        sql.execute("INSERT INTO p(k) VALUES(3)").unwrap();
        drop(sql);
        for _ in 0..2 {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,ident FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(1), SqlValue::Int(100)],
                    vec![SqlValue::Int(2), SqlValue::Int(105)],
                    vec![SqlValue::Int(3), SqlValue::Int(110)]
                ]
            );
            assert_eq!(
                sql.execute("SELECT last_value FROM p_ident_seq")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(110)]]
            );
            drop(sql);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

fn physical_state(db: &BicDb) -> Vec<(String, Vec<(String, serde_json::Value)>)> {
    ["__bicdb_pg_schema", "p", "low", "high"]
        .into_iter()
        .map(|name| {
            let mut rows = db
                .scan_collection(name)
                .unwrap()
                .into_iter()
                .map(|record| (record.id, record.metadata))
                .collect::<Vec<_>>();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            (name.to_string(), rows)
        })
        .collect()
}

#[test]
fn serial_add_fills_every_leaf_and_publishes_one_shared_generator() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE SEQUENCE warmup",
            "INSERT INTO low VALUES(1,'low-ack',10)",
            "INSERT INTO high VALUES(11,'high-ack',20)",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        let before = db.current_commit_seq();
        SqlSession::new(&mut db)
            .execute("ALTER TABLE p ADD COLUMN serial_value BIGSERIAL")
            .unwrap();
        assert_eq!(db.current_commit_seq() - before, 1);
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(sql.execute("SELECT count(*),min(serial_value),max(serial_value),count(DISTINCT serial_value) FROM p").unwrap().rows,
            vec![vec![SqlValue::Int(2),SqlValue::Int(1),SqlValue::Int(2),SqlValue::Int(2)]]);
        for text in [
            "INSERT INTO p(k) VALUES(2)",
            "INSERT INTO low(k) VALUES(3)",
            "INSERT INTO high(k) VALUES(12)",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        for _ in 0..2 {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,serial_value FROM p WHERE k IN(2,3,12) ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(2), SqlValue::Int(3)],
                    vec![SqlValue::Int(3), SqlValue::Int(4)],
                    vec![SqlValue::Int(12), SqlValue::Int(5)]
                ]
            );
            assert_eq!(
                sql.execute("SELECT k,payload FROM p WHERE k IN(1,11) ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(1), SqlValue::String("low-ack".into())],
                    vec![SqlValue::Int(11), SqlValue::String("high-ack".into())]
                ]
            );
            assert_eq!(
                sql.execute("SELECT last_value FROM p_serial_value_seq")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(5)]]
            );
            drop(sql);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn identity_sequence_exhaustion_leaves_no_partial_column_or_sequence() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO low VALUES(1,'low-ack',10)")
            .unwrap();
        sql.execute("INSERT INTO high VALUES(11,'high-ack',20)")
            .unwrap();
        drop(sql);
        let before = physical_state(&db);
        let seq = db.current_commit_seq();
        let error = SqlSession::new(&mut db).execute("ALTER TABLE p ADD COLUMN ident INT GENERATED ALWAYS AS IDENTITY (START WITH 1 MAXVALUE 1 NO CYCLE)").unwrap_err();
        assert_eq!(error.sqlstate(), "2200H");
        assert_eq!(db.current_commit_seq(), seq);
        assert_eq!(physical_state(&db), before);
        for _ in 0..2 {
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT count(*) FROM pg_class WHERE relname='p_ident_seq'")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(0)]]
            );
            assert_eq!(physical_state(&db), before);
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}

#[test]
fn identity_savepoint_undo_restores_rows_schemas_and_owned_sequence_privileges() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        setup(&mut db);
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE ROLE seq_reader",
            "ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO seq_reader",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO seq_reader",
            "INSERT INTO low VALUES(1,'low-ack',10)",
            "INSERT INTO high VALUES(11,'high-ack',20)",
        ] {
            sql.execute(text).unwrap();
        }
        drop(sql);
        let before = physical_state(&db);
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "BEGIN", "SAVEPOINT s",
            "ALTER TABLE p ADD COLUMN ident BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)",
        ] {sql.execute(text).unwrap();}
        assert_eq!(
            sql.execute("SELECT has_sequence_privilege('seq_reader','p_ident_seq','USAGE')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(true)]]
        );
        sql.execute("INSERT INTO p(k) VALUES(2)").unwrap();
        sql.execute("ROLLBACK TO s").unwrap();
        sql.execute("COMMIT").unwrap();
        drop(sql);
        assert_eq!(physical_state(&db), before);
        for _ in 0..2 {
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT count(*) FROM pg_class WHERE relname='p_ident_seq'")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(0)]]
            );
            assert_eq!(physical_state(&db), before);
            assert!(db
                .scan_collection("__bicdb_pg_privileges")
                .unwrap()
                .iter()
                .all(
                    |row| row.metadata.get("object_name").and_then(|v| v.as_str())
                        != Some("p_ident_seq")
                ));
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}
