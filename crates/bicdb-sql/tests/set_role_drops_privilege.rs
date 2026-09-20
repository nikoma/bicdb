//! SET ROLE must actually reduce privilege. A pooler or app that does
//! `SET ROLE lowpriv` before running generated/untrusted SQL is relying on it.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (dir, db)
}

#[test]
fn set_role_drops_role_management_privilege() {
    let (_dir, mut db) = db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE lowpriv LOGIN").unwrap();

    // As bootstrap superuser this is allowed.
    sql.execute("CREATE ROLE ok_before").unwrap();

    // Drop privilege. Role DDL must now be refused.
    sql.execute("SET ROLE lowpriv").unwrap();
    let created = sql.execute("CREATE ROLE evil SUPERUSER LOGIN");
    println!(
        "after SET ROLE, CREATE ROLE evil SUPERUSER -> {:?}",
        created
            .as_ref()
            .map(|_| "ALLOWED")
            .map_err(|e| e.to_string())
    );
    assert!(
        created.is_err(),
        "SET ROLE did not drop role-creation privilege"
    );

    let altered = sql.execute("ALTER ROLE lowpriv SUPERUSER");
    println!(
        "after SET ROLE, ALTER ROLE ... SUPERUSER -> {:?}",
        altered
            .as_ref()
            .map(|_| "ALLOWED")
            .map_err(|e| e.to_string())
    );
    assert!(
        altered.is_err(),
        "SET ROLE did not drop role-alteration privilege"
    );

    // Returning to the session role restores it.
    sql.execute("RESET ROLE").unwrap();
    sql.execute("CREATE ROLE ok_after").unwrap();
}
