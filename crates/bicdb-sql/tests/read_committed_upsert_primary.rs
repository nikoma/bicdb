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
