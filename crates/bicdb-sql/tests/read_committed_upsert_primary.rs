use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn reordered_composite_conflict_columns_keep_authoritative_primary_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for target in ["a,b", "b,a"] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                dir.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap();
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE composite_upsert(a INT,b INT,v INT,PRIMARY KEY(a,b)); INSERT INTO composite_upsert VALUES(1,2,20),(2,1,50); BEGIN").unwrap();
            let result = sql.execute(&format!("INSERT INTO composite_upsert VALUES(1,2,10) ON CONFLICT({target}) DO UPDATE SET v=composite_upsert.v+EXCLUDED.v RETURNING a,b,v")).unwrap();
            assert_eq!(
                result.rows,
                vec![vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(30)]],
                "target order {target}"
            );
            sql.execute("COMMIT").unwrap();
            assert_eq!(
                sql.execute("SELECT a,b,v FROM composite_upsert ORDER BY a,b")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(30)],
                    vec![SqlValue::Int(2), SqlValue::Int(1), SqlValue::Int(50)]
                ]
            );
        }
    }
}

#[test]
fn native_fixed_snapshot_secondary_upsert_retains_retry_requirement() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE fixed_arbiter(id INT PRIMARY KEY,k TEXT UNIQUE,v INT)")
            .unwrap();
        let mut writer = SqlSession::new_shared(&db);
        writer
            .execute("BEGIN; INSERT INTO fixed_arbiter VALUES(1,'same',20)")
            .unwrap();
        let owner = writer.take_pending_transaction().unwrap();
        let mut fixed = db.begin_transaction().unwrap();
        fixed.set_snapshot_isolation(true, 0).unwrap();
        owner.commit().unwrap();
        let mut waiter = SqlSession::new_shared(&db).with_pending_transaction(fixed);
        let error = waiter.execute("INSERT INTO fixed_arbiter VALUES(2,'same',10) ON CONFLICT(k) DO UPDATE SET v=fixed_arbiter.v+EXCLUDED.v").unwrap_err();
        assert!(
            matches!(
                error,
                bicdb_sql::SqlError::BicDb(bicdb_core::BicDbError::TransactionConflict(_))
            ),
            "fixed-snapshot conflict must require retry: {error}"
        );
        waiter
            .take_pending_transaction()
            .unwrap()
            .rollback()
            .unwrap();
        assert_eq!(
            SqlSession::new_shared(&db)
                .execute("SELECT id,v FROM fixed_arbiter")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1), SqlValue::Int(20)]]
        );
    }
}
