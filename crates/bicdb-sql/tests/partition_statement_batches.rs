use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

fn expected_rows() -> Vec<Vec<SqlValue>> {
    vec![
        vec![SqlValue::Int(2), SqlValue::String("ack;low".into())],
        vec![SqlValue::Int(12), SqlValue::String("ack;high".into())],
    ]
}

#[test]
fn mixed_partition_creation_batch_routes_every_ack_and_survives_two_reopens() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let result = SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k);
                 CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
                 /* A mixed batch must retain both child definitions. */
                 CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20);
                 INSERT INTO p VALUES(2,'ack;low'),(12,'ack;high');
                 SELECT k,payload FROM p ORDER BY k",
            )
            .unwrap();
        assert_eq!(result.rows, expected_rows());
        assert_eq!(db.scan_collection("p").unwrap().len(), 0);
        assert_eq!(db.scan_collection("low").unwrap().len(), 1);
        assert_eq!(db.scan_collection("high").unwrap().len(), 1);
        for _ in 0..2 {
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT k,payload FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                expected_rows()
            );
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("INSERT INTO low VALUES(12,'must refuse')")
                    .unwrap_err()
                    .sqlstate(),
                "23514"
            );
        }
        db.close().unwrap();
    }
}

#[test]
fn direct_ast_partition_creation_uses_the_same_physical_routing_and_constraints() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for text in [
            "CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)",
            "CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)",
            "CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)",
            "INSERT INTO p VALUES(2,'ack;low'),(12,'ack;high')",
        ] {
            let statements = Parser::parse_sql(&PostgreSqlDialect {}, text).unwrap();
            assert_eq!(statements.len(), 1);
            sql.execute_statement(&statements[0]).unwrap();
        }
        assert_eq!(
            sql.execute("SELECT k,payload FROM p ORDER BY k")
                .unwrap()
                .rows,
            expected_rows()
        );
        assert_eq!(
            sql.execute("INSERT INTO high VALUES(2,'must refuse')")
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        drop(sql);
        assert_eq!(db.scan_collection("p").unwrap().len(), 0);
        assert_eq!(db.scan_collection("low").unwrap().len(), 1);
        assert_eq!(db.scan_collection("high").unwrap().len(), 1);
        db.close().unwrap();
    }
}

#[test]
fn rollback_of_a_mixed_partition_batch_removes_its_rows_and_catalog() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        SqlSession::new(&mut db)
            .execute(
                "BEGIN;
                 CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k);
                 CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
                 INSERT INTO p VALUES(2,'rolled back');
                 ROLLBACK",
            )
            .unwrap();
        for _ in 0..2 {
            assert!(!db.has_collection("p"));
            assert!(!db.has_collection("low"));
            assert!(db.get("__bicdb_pg_schema", "p").unwrap().is_none());
            assert!(db.get("__bicdb_pg_schema", "low").unwrap().is_none());
            db.close().unwrap();
            db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        }
        db.close().unwrap();
    }
}
