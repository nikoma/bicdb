//! CR-2/CR-3/CR-4 from the 2026-08-16 external review: GRANT/REVOKE,
//! ALTER TABLE and policy DDL had no ownership check, so any authenticated
//! user could grant themselves privileges, switch off a table's RLS, take it
//! over, or attach a permissive policy. One missing layer, tested as one.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

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

/// Set up: `alice` owns `secrets`; `mallory` is an ordinary role.
fn setup(sql: &mut SqlSession) {
    sql.execute("CREATE ROLE alice LOGIN").unwrap();
    sql.execute("CREATE ROLE mallory LOGIN").unwrap();
    sql.execute("CREATE TABLE secrets (id TEXT PRIMARY KEY, value TEXT)")
        .unwrap();
    sql.execute("INSERT INTO secrets VALUES ('s1', 'classified')")
        .unwrap();
    sql.execute("ALTER TABLE secrets OWNER TO alice").unwrap();
}

fn as_role(sql: &mut SqlSession, role: &str) {
    sql.execute(&format!("SET ROLE {role}")).unwrap();
}

#[test]
fn cr2_grant_requires_ownership() {
    let (_dir, mut db) = db();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);

    as_role(&mut sql, "mallory");
    let denied = sql.execute("GRANT SELECT ON secrets TO mallory");
    assert!(
        denied.is_err(),
        "CR-2: a non-owner granted itself SELECT on someone else's table"
    );
    assert!(denied.unwrap_err().to_string().contains("owner"));

    // REVOKE is equally powerful (access denial) and equally gated.
    assert!(
        sql.execute("REVOKE SELECT ON secrets FROM alice").is_err(),
        "CR-2: a non-owner revoked another role's access"
    );

    // The owner may still do both.
    as_role(&mut sql, "alice");
    sql.execute("GRANT SELECT ON secrets TO mallory").unwrap();
    sql.execute("REVOKE SELECT ON secrets FROM mallory")
        .unwrap();
}

#[test]
fn cr3_alter_table_requires_ownership() {
    let (_dir, mut db) = db();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);
    as_role(&mut sql, "alice");
    sql.execute("ALTER TABLE secrets ENABLE ROW LEVEL SECURITY")
        .unwrap();

    as_role(&mut sql, "mallory");
    // The RLS off-switch.
    assert!(
        sql.execute("ALTER TABLE secrets DISABLE ROW LEVEL SECURITY")
            .is_err(),
        "CR-3: a non-owner disabled row level security"
    );
    // Owner takeover.
    assert!(
        sql.execute("ALTER TABLE secrets OWNER TO mallory").is_err(),
        "CR-3: a non-owner took ownership of the table"
    );
    // Ordinary shape changes are the same layer.
    assert!(
        sql.execute("ALTER TABLE secrets ADD COLUMN backdoor TEXT")
            .is_err(),
        "CR-3: a non-owner altered the table's shape"
    );

    // The owner is unaffected.
    as_role(&mut sql, "alice");
    sql.execute("ALTER TABLE secrets ADD COLUMN note TEXT")
        .unwrap();
    sql.execute("ALTER TABLE secrets DISABLE ROW LEVEL SECURITY")
        .unwrap();
}

#[test]
fn cr4_policy_ddl_requires_ownership() {
    let (_dir, mut db) = db();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);
    as_role(&mut sql, "alice");
    sql.execute("ALTER TABLE secrets ENABLE ROW LEVEL SECURITY")
        .unwrap();
    sql.execute("CREATE POLICY owner_only ON secrets FOR SELECT TO PUBLIC USING (false)")
        .unwrap();

    as_role(&mut sql, "mallory");
    // Policy injection.
    assert!(
        sql.execute("CREATE POLICY open_all ON secrets FOR SELECT TO PUBLIC USING (true)")
            .is_err(),
        "CR-4: a non-owner injected a permissive policy"
    );
    // Dropping the isolating policy is equally powerful.
    assert!(
        sql.execute("DROP POLICY owner_only ON secrets").is_err(),
        "CR-4: a non-owner dropped the isolating policy"
    );
    assert!(
        sql.execute("ALTER POLICY owner_only ON secrets TO PUBLIC")
            .is_err(),
        "CR-4: a non-owner altered the isolating policy"
    );

    // The owner still controls its own policies.
    as_role(&mut sql, "alice");
    sql.execute("CREATE POLICY extra ON secrets FOR SELECT TO PUBLIC USING (true)")
        .unwrap();
    sql.execute("DROP POLICY extra ON secrets").unwrap();
}

/// Superusers bypass ownership (as in PostgreSQL), and legacy tables with no
/// recorded owner remain administrable rather than becoming bricked.
#[test]
fn superuser_and_legacy_ownerless_tables_still_work() {
    let (_dir, mut db) = db();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);

    // Bootstrap superuser (no SET ROLE) may administer a table it does not own.
    sql.execute("GRANT SELECT ON secrets TO mallory").unwrap();
    sql.execute("ALTER TABLE secrets ADD COLUMN admin_note TEXT")
        .unwrap();

    // A table created and never re-owned is bootstrap-owned and still usable.
    sql.execute("CREATE TABLE legacy (id TEXT PRIMARY KEY)")
        .unwrap();
    sql.execute("ALTER TABLE legacy ADD COLUMN v TEXT").unwrap();
    sql.execute("GRANT SELECT ON legacy TO mallory").unwrap();
    let rows = sql.execute("SELECT count(*) FROM secrets").unwrap();
    assert_eq!(rows.rows[0][0], SqlValue::Int(1));
}

/// Ownership held through role membership counts, as in PostgreSQL.
#[test]
fn membership_in_the_owning_role_confers_ownership() {
    let (_dir, mut db) = db();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);
    sql.execute("CREATE ROLE deputy LOGIN").unwrap();
    sql.execute("GRANT alice TO deputy").unwrap();

    as_role(&mut sql, "deputy");
    sql.execute("ALTER TABLE secrets ADD COLUMN delegated TEXT")
        .unwrap();
    sql.execute("GRANT SELECT ON secrets TO mallory").unwrap();

    // A role with no membership is still refused.
    as_role(&mut sql, "mallory");
    assert!(sql
        .execute("ALTER TABLE secrets ADD COLUMN nope TEXT")
        .is_err());
}
