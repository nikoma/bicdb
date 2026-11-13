use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{check_sql_partition_admission, SqlEngine, SqlSession, SqlValue};

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
        "INSERT INTO p VALUES(2,'ack')",
    ] {
        sql.execute(statement).unwrap();
    }
}

#[test]
fn fresh_partition_publication_is_fenced_certified_and_readable_after_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE ordinary(k INT PRIMARY KEY)")
            .unwrap();
        assert!(!db.sql_partition_execution_feature_active());
        for ddl in [
            "CREATE TABLE invalid_pk(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(payload)",
            "CREATE TABLE invalid_unique(k INT,payload TEXT,UNIQUE(payload)) PARTITION BY RANGE(k)",
        ] {
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute(ddl)
                    .unwrap_err()
                    .sqlstate(),
                "0A000"
            );
            assert!(!db.sql_partition_execution_feature_active());
        }
        assert!(!db.has_collection("invalid_pk"));
        assert!(!db.has_collection("invalid_unique"));
        setup(&mut db);
        bicdb_sql::ensure_primary_key_indexes(&mut db)
            .expect("each physical child receives its own PK index");
        assert!(db.sql_partition_execution_feature_active());
        for (relation, field) in [("p", "partitioning"), ("low", "partition_of")] {
            assert_eq!(
                db.get("__bicdb_pg_schema", relation)
                    .unwrap()
                    .unwrap()
                    .metadata[field]["execution_version"],
                serde_json::json!(1)
            );
        }
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            let sequence = db.last_commit_seq();
            assert_eq!(
                SqlEngine::new(&db)
                    .execute("SELECT k,payload FROM p")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(2), SqlValue::String("ack".into())]]
            );
            assert_eq!(
                db.last_commit_seq(),
                sequence,
                "read-only admission cannot mint or rewrite a certificate"
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn visible_id_without_authored_pk_remains_nonunique_in_a_partitioned_table() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for statement in [
            "CREATE TABLE p(id INT,part INT) PARTITION BY RANGE(part)",
            "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)",
            "INSERT INTO p VALUES(1,2),(1,3)",
        ] {
            sql.execute(statement).unwrap();
        }
        assert_eq!(
            sql.execute("SELECT id,part FROM p ORDER BY part")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::Int(1), SqlValue::Int(2)],
                vec![SqlValue::Int(1), SqlValue::Int(3)]
            ]
        );
        drop(sql);
        bicdb_sql::ensure_primary_key_indexes(&mut db).unwrap();
        db.close().unwrap();
    }
}

#[test]
fn native_descriptor_tampering_and_same_generation_catalog_recreation_refuse() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        setup(&mut db);
        check_sql_partition_admission(&db).unwrap();
        let old_generation = db.collection_generation("__bicdb_pg_schema");
        let mut records = db.scan_collection("__bicdb_pg_schema").unwrap();
        records
            .iter_mut()
            .find(|raw| raw.id == "low")
            .unwrap()
            .metadata["partition_of"]["bound"] = serde_json::json!("FOR VALUES FROM (0) TO (9)");
        db.drop_collection("__bicdb_pg_schema").unwrap();
        db.create_collection("__bicdb_pg_schema").unwrap();
        db.batch_insert("__bicdb_pg_schema", records.clone())
            .unwrap();
        while db.collection_generation("__bicdb_pg_schema") < old_generation {
            db.insert("__bicdb_pg_schema", records[0].clone()).unwrap();
        }
        assert_eq!(
            db.collection_generation("__bicdb_pg_schema"),
            old_generation
        );
        let sequence = db.last_commit_seq();
        assert_eq!(
            check_sql_partition_admission(&db).unwrap_err().sqlstate(),
            "55000"
        );
        assert_eq!(
            SqlEngine::new(&db)
                .execute("SELECT k FROM p")
                .unwrap_err()
                .sqlstate(),
            "55000"
        );
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT k FROM p")
                .unwrap_err()
                .sqlstate(),
            "55000"
        );
        assert_eq!(db.last_commit_seq(), sequence);
        assert_eq!(db.scan_collection("low").unwrap().len(), 1);
        db.close().unwrap();
    }
}
