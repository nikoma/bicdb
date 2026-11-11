use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn denied(sql: &mut SqlSession<'_>, role: &str, table: &str) {
    sql.execute(&format!("SET ROLE {role}")).unwrap();
    let result = sql.execute(&format!("SELECT payload FROM {table}"));
    sql.execute("RESET ROLE").unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
}

fn allowed(sql: &mut SqlSession<'_>, role: &str, table: &str, value: &str) {
    sql.execute(&format!("SET ROLE {role}")).unwrap();
    let result = sql.execute(&format!("SELECT payload FROM {table}"));
    sql.execute("RESET ROLE").unwrap();
    assert_eq!(
        result.unwrap().rows,
        vec![vec![SqlValue::String(value.into())]]
    );
}

#[test]
fn rename_preserves_existing_acl_without_reapplying_changed_creation_defaults() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for commit in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let cfg = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute("CREATE ROLE prior_reader; CREATE ROLE later_reader; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO prior_reader; CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'kept'); ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM prior_reader; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO later_reader; CREATE TABLE after_defaults(id INT PRIMARY KEY,payload TEXT); INSERT INTO after_defaults VALUES(1,'new-table')").unwrap();
                allowed(&mut sql, "prior_reader", "source", "kept");
                denied(&mut sql, "later_reader", "source");
                allowed(&mut sql, "later_reader", "after_defaults", "new-table");
                sql.execute("BEGIN; ALTER TABLE source RENAME TO renamed")
                    .unwrap();
                sql.execute(if commit { "COMMIT" } else { "ROLLBACK" })
                    .unwrap();
                if commit {
                    allowed(&mut sql, "prior_reader", "renamed", "kept");
                    // A rename is the same relation and must not acquire new
                    // access from defaults changed after its creation.
                    denied(&mut sql, "later_reader", "renamed");
                } else {
                    allowed(&mut sql, "prior_reader", "source", "kept");
                    denied(&mut sql, "later_reader", "source");
                    // An orphan grant from a rolled-back rename must not
                    // authorize a later relation reusing the destination.
                    sql.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM later_reader; CREATE TABLE renamed(id INT PRIMARY KEY,payload TEXT); INSERT INTO renamed VALUES(1,'private-replacement')").unwrap();
                    denied(&mut sql, "later_reader", "renamed");
                }
                allowed(&mut sql, "later_reader", "after_defaults", "new-table");
            }
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                allowed(
                    &mut sql,
                    "prior_reader",
                    if commit { "renamed" } else { "source" },
                    "kept",
                );
                denied(&mut sql, "later_reader", "renamed");
                allowed(&mut sql, "later_reader", "after_defaults", "new-table");
            }
            db.close().unwrap();
        }
    }
}
