//! Cross-tenant exfiltration through an unauthorized trigger (P0).
//!
//! An ordinary non-superuser role could `CREATE TRIGGER` on a table it did
//! not own, had no `SELECT` on, and held no `TRIGGER` right for. The trigger
//! body copied `NEW.<column>` into a persisted notification, and the
//! notification catalog was global — so the attacker read the owner's future
//! plaintext straight out of `pg_catalog.bicdb_notifications`.
//!
//! Each test below is a step of that exploit, now expected to be refused.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn seeded() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    {
        let mut owner = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE mallory LOGIN NOSUPERUSER",
            "CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)",
        ] {
            owner.execute(sql).unwrap();
        }
    }
    (directory, db)
}

fn as_mallory(db: &mut BicDb) -> SqlSession<'_> {
    SqlSession::new_unprivileged(db, "mallory")
}

fn assert_refused(error: bicdb_sql::SqlError) {
    let rendered = format!("{error}");
    assert!(
        rendered.contains("owner") || rendered.contains("permission"),
        "unexpected refusal: {rendered}"
    );
}

const LEAK_FUNCTION: &str = "CREATE FUNCTION leak() RETURNS trigger AS $$ BEGIN \
     PERFORM pg_notify('leak', NEW.v); RETURN NEW; END; $$ LANGUAGE plpgsql";

#[test]
fn create_trigger_requires_table_ownership() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory.execute(LEAK_FUNCTION).unwrap();
    let error = mallory
        .execute(
            "CREATE TRIGGER steal AFTER INSERT ON secrets FOR EACH ROW EXECUTE FUNCTION leak()",
        )
        .expect_err("CREATE TRIGGER on another role's table must be refused");
    assert_refused(error);
}

#[test]
fn create_or_replace_trigger_requires_table_ownership() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory.execute(LEAK_FUNCTION).unwrap();
    let error = mallory
        .execute(
            "CREATE OR REPLACE TRIGGER steal AFTER INSERT ON secrets \
             FOR EACH ROW EXECUTE FUNCTION leak()",
        )
        .expect_err("CREATE OR REPLACE TRIGGER on another role's table must be refused");
    assert_refused(error);
}

#[test]
fn drop_trigger_requires_table_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute(LEAK_FUNCTION).unwrap();
        owner
            .execute(
                "CREATE TRIGGER audit AFTER INSERT ON secrets \
                 FOR EACH ROW EXECUTE FUNCTION leak()",
            )
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("DROP TRIGGER audit ON secrets")
        .expect_err("DROP TRIGGER on another role's table must be refused");
    assert_refused(error);
}

#[test]
fn trigger_creation_requires_execute_on_its_function() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute(LEAK_FUNCTION).unwrap();
        owner
            .execute("CREATE TABLE mallory_own (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        owner
            .execute("ALTER TABLE mallory_own OWNER TO mallory")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute(
            "CREATE TRIGGER own AFTER INSERT ON mallory_own \
             FOR EACH ROW EXECUTE FUNCTION leak()",
        )
        .expect_err("binding a function without EXECUTE must be refused");
    let rendered = format!("{error}");
    assert!(
        rendered.contains("function"),
        "expected the function-EXECUTE refusal, got: {rendered}"
    );
}

/// The catalog half of the exploit: even a notification that already exists
/// must not be readable by a role that could not read the table it came from.
#[test]
fn notifications_are_filtered_by_table_privilege() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute(LEAK_FUNCTION).unwrap();
        owner
            .execute(
                "CREATE TRIGGER audit AFTER INSERT ON secrets \
                 FOR EACH ROW EXECUTE FUNCTION leak()",
            )
            .unwrap();
        owner
            .execute("INSERT INTO secrets VALUES (1, 'P0-FUTURE-SECRET')")
            .unwrap();
        let visible = owner
            .execute("SELECT payload FROM pg_catalog.bicdb_notifications")
            .unwrap();
        assert!(
            format!("{visible:?}").contains("P0-FUTURE-SECRET"),
            "the owner must still see its own notifications"
        );
    }
    let mut mallory = as_mallory(&mut db);
    let rows = mallory
        .execute("SELECT payload FROM pg_catalog.bicdb_notifications")
        .unwrap();
    assert!(
        !format!("{rows:?}").contains("P0-FUTURE-SECRET"),
        "notification payloads leaked to a role with no SELECT on the source table: {rows:?}"
    );
}
