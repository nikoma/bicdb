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
                reader
                    .execute("COMMIT")
                    .unwrap_or_else(|error| panic!("{mode} {key_type} {lock:?} COMMIT: {error}"));
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

#[test]
fn current_update_and_delete_release_the_actual_unique_key_after_an_earlier_read() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for delete in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE indexed_rows(rowkey NUMERIC PRIMARY KEY,payload INT UNIQUE); INSERT INTO indexed_rows VALUES(1,10)").unwrap();
            let mut reader = SqlSession::new_shared(&db);
            let mut writer = SqlSession::new_shared(&db);
            reader.execute("BEGIN").unwrap();
            assert_eq!(
                reader
                    .execute("SELECT payload FROM indexed_rows WHERE rowkey=1")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(10)]]
            );
            writer
                .execute("UPDATE indexed_rows SET payload=20 WHERE rowkey=1")
                .unwrap();
            let statement = if delete {
                "DELETE FROM indexed_rows WHERE rowkey=1 RETURNING payload"
            } else {
                "UPDATE indexed_rows SET payload=payload+1 WHERE rowkey=1 RETURNING payload"
            };
            assert_eq!(
                reader.execute(statement).unwrap().rows,
                vec![vec![SqlValue::Int(if delete { 20 } else { 21 })]]
            );
            reader
                .execute("COMMIT")
                .unwrap_or_else(|error| panic!("{mode} delete={delete}: {error}"));
            // Reuse the peer's actual previous unique key. A stale before-image
            // must not leave its reservation or index entry behind.
            writer
                .execute("INSERT INTO indexed_rows VALUES(2,20)")
                .unwrap();
            assert_eq!(
                writer
                    .execute("SELECT payload FROM indexed_rows ORDER BY payload")
                    .unwrap()
                    .rows,
                if delete {
                    vec![vec![SqlValue::Int(20)]]
                } else {
                    vec![vec![SqlValue::Int(20)], vec![SqlValue::Int(21)]]
                }
            );
        }
    }
}
