//! Native rollback journals must not be exposed as ordinary SQL relations.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::SqlSession;

const JOURNALS: [&str; 3] = [
    "__bicdb_recycle_intents",
    "__bicdb_recycle_purges",
    "__bicdb_recycle_creations",
];

#[test]
fn native_journals_are_hidden_and_cannot_be_mutated_through_sql() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        for name in JOURNALS {
            db.create_collection(name).unwrap();
            db.insert(
                name,
                Record::new("control").with_metadata(serde_json::json!({"marker":"retained"})),
            )
            .unwrap();
        }
        {
            let mut sql = SqlSession::new(&mut db);
            for name in JOURNALS {
                assert!(
                    sql.execute(&format!(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='{name}'"
                    ))
                    .unwrap()
                    .rows
                    .is_empty(),
                    "journal registered as a user table: {name}"
                );
                assert!(sql
                    .execute(&format!(
                        "SELECT relname FROM pg_class WHERE relname='{name}'"
                    ))
                    .unwrap()
                    .rows
                    .is_empty());
                for statement in [
                    format!("SELECT * FROM {name}"),
                    format!("CREATE TABLE {name}(rowkey INT PRIMARY KEY)"),
                    format!("DELETE FROM {name}"),
                    format!("TRUNCATE TABLE {name}"),
                    format!("DROP TABLE {name}"),
                    format!("ALTER TABLE {name} RENAME TO exposed_journal"),
                ] {
                    assert!(
                        sql.execute(&statement).is_err(),
                        "journal SQL admitted: {statement}"
                    );
                }
            }
        }
        for name in JOURNALS {
            let row = db.get(name, "control").unwrap().unwrap();
            assert_eq!(row.metadata, serde_json::json!({"marker":"retained"}));
        }
    }
}

#[test]
fn same_named_private_schema_relations_remain_user_tables() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA journal_user").unwrap();
        for name in JOURNALS {
            sql.execute(&format!(
                "CREATE TABLE journal_user.\"{name}\"(rowkey INT PRIMARY KEY)"
            ))
            .unwrap();
            sql.execute(&format!("INSERT INTO journal_user.\"{name}\" VALUES(1)"))
                .unwrap();
            assert_eq!(
                sql.execute(&format!("SELECT rowkey FROM journal_user.\"{name}\""))
                    .unwrap()
                    .rows[0][0]
                    .to_cell(),
                "1"
            );
            assert_eq!(
                sql.execute(&format!("SELECT table_name FROM information_schema.tables WHERE table_schema='journal_user' AND table_name='{name}'"))
                    .unwrap()
                    .rows
                    .len(),
                1
            );
        }
    }
}
