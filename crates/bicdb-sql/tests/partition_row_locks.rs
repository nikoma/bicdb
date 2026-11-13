use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn partition_parent_and_child_lock_the_same_physical_rows_with_and_without_primary_keys() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for keyed in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let cfg = DbConfig::default()
                .with_storage_mode(mode.clone())
                .with_fsync(true)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute(&format!(
                    "CREATE TABLE p(k INT {},payload TEXT) PARTITION BY RANGE(k)",
                    if keyed { "PRIMARY KEY" } else { "" }
                ))
                .unwrap();
                sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
                    .unwrap();
                sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
                    .unwrap();
                sql.execute("INSERT INTO p VALUES(1,'low-ack'),(11,'high-ack')")
                    .unwrap();
            }
            {
                let mut holder = SqlSession::new_shared(&db);
                let mut other = SqlSession::new_shared(&db);
                holder.execute("BEGIN").unwrap();
                assert_eq!(
                    holder
                        .execute("SELECT k FROM p WHERE k=1 FOR UPDATE")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(1)]]
                );
                other.execute("BEGIN").unwrap();
                assert_eq!(
                    other
                        .execute("SELECT k FROM low WHERE k=1 FOR UPDATE NOWAIT")
                        .unwrap_err()
                        .sqlstate(),
                    "55P03"
                );
                other.execute("ROLLBACK").unwrap();
                holder.execute("SAVEPOINT high_lock").unwrap();
                assert_eq!(
                    holder
                        .execute("SELECT k FROM p WHERE k=11 FOR UPDATE")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(11)]]
                );
                holder.execute("ROLLBACK TO high_lock").unwrap();
                other.execute("BEGIN").unwrap();
                assert_eq!(
                    other
                        .execute("SELECT k FROM p ORDER BY k LIMIT 1 FOR UPDATE SKIP LOCKED")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(11)]]
                );
                other.execute("ROLLBACK").unwrap();
                holder.execute("ROLLBACK").unwrap();
                other.execute("BEGIN").unwrap();
                other
                    .execute("SELECT k FROM low WHERE k=1 FOR UPDATE")
                    .unwrap();
                holder.execute("BEGIN").unwrap();
                assert_eq!(
                    holder
                        .execute("SELECT k FROM p WHERE k=1 FOR UPDATE NOWAIT")
                        .unwrap_err()
                        .sqlstate(),
                    "55P03"
                );
                holder.execute("ROLLBACK").unwrap();
                other.execute("ROLLBACK").unwrap();
            }
            db.close().unwrap();
            for _ in 0..2 {
                let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                assert_eq!(
                    SqlSession::new(&mut db)
                        .execute("SELECT k,payload FROM p ORDER BY k")
                        .unwrap()
                        .rows,
                    vec![
                        vec![SqlValue::Int(1), SqlValue::String("low-ack".into())],
                        vec![SqlValue::Int(11), SqlValue::String("high-ack".into())]
                    ]
                );
                db.close().unwrap();
            }
        }
    }
}
