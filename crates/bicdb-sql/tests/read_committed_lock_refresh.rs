//! Earlier plain reads must not override a subsequently locked current row.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn primary_key_update_uses_the_current_statement_after_an_earlier_plain_read() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for key_type in ["INT", "NUMERIC"] {
            for lock in [
                None,
                Some("FOR UPDATE"),
                Some("FOR UPDATE NOWAIT"),
                Some("FOR UPDATE SKIP LOCKED"),
            ] {
                let root = tempfile::tempdir().unwrap();
                let mut db = BicDb::open_with_config(
                    root.path(),
                    DbConfig::default().with_storage_mode(mode.clone()),
                )
                .unwrap();
                SqlSession::new(&mut db).execute(&format!("CREATE TABLE current_rows(rowkey {key_type} PRIMARY KEY,payload INT); INSERT INTO current_rows VALUES(1,10)")).unwrap();
                let mut reader = SqlSession::new_shared(&db);
                let mut writer = SqlSession::new_shared(&db);
                reader.execute("BEGIN").unwrap();
                assert_eq!(
                    reader
                        .execute("SELECT payload FROM current_rows WHERE rowkey=1")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(10)]]
                );
                writer
                    .execute("UPDATE current_rows SET payload=20 WHERE rowkey=1")
                    .unwrap();
                if let Some(lock) = lock {
                    assert_eq!(
                        reader
                            .execute(&format!(
                                "SELECT payload FROM current_rows WHERE rowkey=1 {lock}"
                            ))
                            .unwrap()
                            .rows,
                        vec![vec![SqlValue::Int(20)]],
                        "{mode} {key_type} {lock}"
                    );
                }
                assert_eq!(reader.execute("UPDATE current_rows SET payload=payload+1 WHERE rowkey=1 RETURNING payload").unwrap_or_else(|error| panic!("{mode} {key_type} {lock:?}: {error}")).rows, vec![vec![SqlValue::Int(21)]], "{mode} {key_type} {lock:?}");
                reader.execute("COMMIT").unwrap();
                assert_eq!(
                    writer
                        .execute("SELECT payload FROM current_rows WHERE rowkey=1")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(21)]],
                    "{mode} {key_type} {lock:?}"
                );
            }
        }
    }
}
