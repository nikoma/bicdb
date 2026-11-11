//! A table rename must preserve and retype its own pending composite values.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn old_type_name(value: &serde_json::Value, old: &str) -> bool {
    match value {
        serde_json::Value::Object(fields) => fields.iter().any(|(key, value)| {
            (key == "type_name" && value.as_str() == Some(old)) || old_type_name(value, old)
        }),
        serde_json::Value::Array(values) => values.iter().any(|value| old_type_name(value, old)),
        _ => false,
    }
}

#[test]
fn rename_retypes_pending_inserts_updates_and_arrays_without_reviving_deletes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for rollback in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            {
                let mut session = SqlSession::new(&mut db);
                session.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'source'); CREATE TABLE holder(id INT PRIMARY KEY,item source,items source[]); INSERT INTO holder VALUES(1,ROW(1,'seed')::source,ARRAY[ROW(1,'seed-array')::source]),(3,ROW(1,'deleted')::source,ARRAY[ROW(1,'deleted-array')::source])").unwrap();
                session.execute("BEGIN; UPDATE holder SET item=ROW(1,'pending-update')::source,items=ARRAY[ROW(1,'pending-array')::source] WHERE id=1; INSERT INTO holder VALUES(2,ROW(1,'pending-insert')::source,ARRAY[ROW(1,'insert-array')::source]); DELETE FROM holder WHERE id=3; SAVEPOINT before_rename").unwrap();
                session
                    .execute("ALTER TABLE source RENAME TO renamed")
                    .unwrap_or_else(|error| panic!("{mode} rollback={rollback}: {error}"));
                if rollback {
                    session.execute("ROLLBACK TO before_rename").unwrap();
                }
                let name = if rollback { "source" } else { "renamed" };
                let expected = vec![
                    vec![
                        SqlValue::Int(1),
                        SqlValue::String(name.into()),
                        SqlValue::String("pending-update".into()),
                        SqlValue::String("pending-array".into()),
                    ],
                    vec![
                        SqlValue::Int(2),
                        SqlValue::String(name.into()),
                        SqlValue::String("pending-insert".into()),
                        SqlValue::String("insert-array".into()),
                    ],
                ];
                assert_eq!(session.execute("SELECT id,pg_typeof(item),(item).payload,(items[1]).payload FROM holder ORDER BY id").unwrap().rows,expected,"{mode} rollback={rollback} pending");
                session.execute("COMMIT").unwrap();
                assert_eq!(session.execute("SELECT id,pg_typeof(item),(item).payload,(items[1]).payload FROM holder ORDER BY id").unwrap().rows,expected,"{mode} rollback={rollback} committed");
            }
            let forbidden = if rollback { "renamed" } else { "source" };
            for record in db.scan_collection("holder").unwrap() {
                assert!(
                    !old_type_name(&record.metadata, forbidden),
                    "{mode} rollback={rollback} stale stored type in {}",
                    record.id
                );
            }
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT id,(item).payload,(items[1]).payload FROM holder ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![
                        SqlValue::Int(1),
                        SqlValue::String("pending-update".into()),
                        SqlValue::String("pending-array".into())
                    ],
                    vec![
                        SqlValue::Int(2),
                        SqlValue::String("pending-insert".into()),
                        SqlValue::String("insert-array".into())
                    ]
                ],
                "{mode} rollback={rollback} reopened"
            );
            db.close().unwrap();
        }
    }
}
