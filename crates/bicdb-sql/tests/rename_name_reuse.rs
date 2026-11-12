//! Reusing the source spelling must keep the original relation, replacement,
//! ACLs and unrelated acknowledged work distinct across every owner outcome.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn payload(sql: &mut SqlSession<'_>, table: &str, expected: &str) {
    assert_eq!(
        sql.execute(&format!("SELECT payload FROM {table}"))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(expected.into())]],
        "{table}"
    );
}

fn check(db: &mut BicDb, outcome: &str) {
    assert!(db.get("native_ack", "unrelated-ack").unwrap().is_some());
    let mut sql = SqlSession::new(db);
    if outcome == "commit" {
        payload(&mut sql, "renamed", "business-update");
        payload(&mut sql, "original", "replacement");
        sql.execute("SET ROLE prior_reader").unwrap();
        payload(&mut sql, "renamed", "business-update");
        sql.execute("RESET ROLE; SET ROLE later_reader").unwrap();
        payload(&mut sql, "original", "replacement");
        let denied = sql.execute("SELECT payload FROM renamed").unwrap_err();
        assert_eq!(denied.sqlstate(), "42501");
        sql.execute("RESET ROLE").unwrap();
    } else {
        payload(&mut sql, "original", "acknowledged-original");
        assert!(sql.execute("SELECT payload FROM renamed").is_err());
        sql.execute("SET ROLE prior_reader").unwrap();
        payload(&mut sql, "original", "acknowledged-original");
        sql.execute("RESET ROLE; SET ROLE later_reader").unwrap();
        let denied = sql.execute("SELECT payload FROM original").unwrap_err();
        assert_eq!(denied.sqlstate(), "42501");
        sql.execute("RESET ROLE").unwrap();
    }
}

#[test]
fn replacement_metadata_and_acl_follow_commit_savepoint_and_full_rollback() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for outcome in ["commit", "savepoint", "rollback"] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
            SqlSession::new(&mut db).execute("CREATE ROLE prior_reader; CREATE ROLE later_reader; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO prior_reader; CREATE TABLE original(id INT PRIMARY KEY,payload TEXT); INSERT INTO original VALUES(1,'acknowledged-original'); ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO later_reader").unwrap();
            db.create_collection("native_ack").unwrap();
            let mut native = db.begin_transaction().unwrap();
            native
                .insert("native_ack", Record::new("unrelated-ack"))
                .unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute("BEGIN; SAVEPOINT before_rename; UPDATE original SET payload='business-update'; ALTER TABLE original RENAME TO renamed; SAVEPOINT before_replacement").unwrap();
                sql.execute("CREATE TABLE original(id INT PRIMARY KEY,payload TEXT); INSERT INTO original VALUES(2,'first-replacement')").unwrap_or_else(|error| panic!("{mode} {outcome}: {error}"));
                // Undo only the replacement while preserving its parent's
                // still-live rename scope, then create it again under that scope.
                sql.execute("ROLLBACK TO before_replacement; CREATE TABLE original(id INT PRIMARY KEY,payload TEXT); INSERT INTO original VALUES(2,'replacement')").unwrap();
                native.commit().unwrap();
                sql.execute(match outcome {
                    "commit" => "COMMIT",
                    "savepoint" => "ROLLBACK TO before_rename; COMMIT",
                    _ => "ROLLBACK",
                })
                .unwrap_or_else(|error| panic!("{mode} {outcome}: {error}"));
            }
            check(&mut db, outcome);
            db.close().unwrap();
            for _ in 0..2 {
                let mut reopened = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                bicdb_sql::oracle::initialize_existing_database_profile(&mut reopened).unwrap();
                check(&mut reopened, outcome);
                reopened.close().unwrap();
            }
        }
    }
}
