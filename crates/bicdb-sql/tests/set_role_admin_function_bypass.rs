//! Regression: `SET ROLE` must drop privilege for `bicdb_*` admin functions,
//! not just for the table gate.
//!
//! A pooler/application authenticates once as a superuser, then lowers its
//! role per tenant with `SET ROLE`. The table gate already honored the
//! effective role; the admin-function gate keyed on `session_user`, so every
//! `bicdb_*` function — including the state-mutating
//! `bicdb_advance_transaction_floor` — stayed reachable by the lowered role.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (directory, db)
}

#[test]
fn set_role_drops_admin_function_privilege() {
    let (_d, mut db) = db();
    let mut s = SqlSession::new(&mut db); // bootstrap superuser
    s.execute("CREATE ROLE tenant LOGIN NOSUPERUSER").unwrap();
    s.execute("SET ROLE tenant").unwrap();

    // Sanity: SET ROLE took effect for the effective role but session_user
    // remains the superuser — the exact shape that used to leak.
    let who = s.execute("SELECT current_user, session_user").unwrap();
    assert_eq!(format!("{:?}", who.rows[0][0]), "String(\"tenant\")");

    // Read-only disclosure functions must now be refused.
    for sql in [
        "SELECT bicdb_space_report()",
        "SELECT bicdb_memory_report()",
    ] {
        let err = s
            .execute(sql)
            .expect_err(&format!("{sql} must be refused after SET ROLE"));
        let msg = format!("{err}");
        assert!(
            msg.contains("superuser is required"),
            "{sql}: expected superuser refusal, got: {msg}"
        );
    }

    // The state-mutating one must also be refused — this is the severe case.
    let err = s
        .execute("SELECT bicdb_advance_transaction_floor(1)")
        .expect_err("advance_transaction_floor must be refused after SET ROLE");
    assert!(format!("{err}").contains("superuser is required"));
}

#[test]
fn superuser_session_can_still_call_admin_functions() {
    let (_d, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    // Without lowering the role, the superuser retains access.
    s.execute("SELECT bicdb_space_report()").unwrap();
    s.execute("SELECT bicdb_memory_report()").unwrap();
}
