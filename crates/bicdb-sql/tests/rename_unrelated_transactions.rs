use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_rename_and_undo_preserve_an_unrelated_native_writer() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for commit in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'before')").unwrap();
            db.create_collection("unrelated_native").unwrap();
            // The embedded host upgrades namespace identity at startup,
            // before admitting either SQL or native application owners.
            bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
            let mut native = db.begin_transaction().unwrap();
            native
                .insert("unrelated_native", Record::new("acknowledged-native"))
                .unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute("BEGIN; UPDATE source SET payload='after' WHERE id=1")
                    .unwrap();
                sql.execute("ALTER TABLE source RENAME TO renamed").unwrap_or_else(|error| panic!("{mode} commit={commit}: unrelated pending native work must not prevent scoped rename: {error}"));
                // Its ACK must survive either outcome of the SQL owner.
                native.commit().unwrap();
                sql.execute(if commit { "COMMIT" } else { "ROLLBACK" })
                    .unwrap();
                let query = if commit {
                    "SELECT payload FROM renamed WHERE id=1"
                } else {
                    "SELECT payload FROM source WHERE id=1"
                };
                assert_eq!(
                    sql.execute(query).unwrap().rows,
                    vec![vec![SqlValue::String(
                        if commit { "after" } else { "before" }.into()
                    )]]
                );
            }
            assert!(db
                .get("unrelated_native", "acknowledged-native")
                .unwrap()
                .is_some());
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            assert!(db
                .get("unrelated_native", "acknowledged-native")
                .unwrap()
                .is_some());
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute(if commit {
                        "SELECT payload FROM renamed WHERE id=1"
                    } else {
                        "SELECT payload FROM source WHERE id=1"
                    })
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String(
                    if commit { "after" } else { "before" }.into()
                )]]
            );
            db.close().unwrap();
        }
    }
}
