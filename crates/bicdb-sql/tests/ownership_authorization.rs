//! Three P0 authorization bypasses that all end in cross-tenant reads or
//! cross-tenant destruction, and all share one root shape: an ownership gate
//! that was written for tables and never extended to the sibling object kinds.
//!
//! 1. `ALTER VIEW ... OWNER TO` checked that the caller was the *current*
//!    owner but never that it held the *new* owner role. Views run with
//!    definer semantics, so laundering a view's ownership through a
//!    privileged role handed the caller that role's read access.
//! 2. `GRANT`/`REVOKE` only gated `Table` objects, and that branch failed
//!    open for views (`load_schema` finds no view). Sequences, schemas, and
//!    databases were never gated at all — free self-grants.
//! 3. `DROP VIEW` / `DROP SEQUENCE` / `DROP TYPE` had no ownership check,
//!    though `DROP TABLE` next to them did.
//!
//! Each test below is that exploit, now expected to be refused.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

/// Bootstrap-owned objects, plus an ordinary role holding nothing at all.
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
            "CREATE ROLE alice LOGIN NOSUPERUSER",
            "CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)",
            "INSERT INTO secrets VALUES (1, 'P0-CANARY')",
            "CREATE VIEW secret_view AS SELECT id, v FROM secrets",
            "CREATE SEQUENCE secret_seq",
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

fn assert_no_canary(session: &mut SqlSession<'_>, sql: &str) {
    match session.execute(sql) {
        Ok(rows) => assert!(
            !format!("{rows:?}").contains("P0-CANARY"),
            "{sql} leaked the canary: {rows:?}"
        ),
        Err(error) => assert_refused(error),
    }
}

// ---------------------------------------------------------------- P0 #1 ----

/// The full laundering chain: own a view over someone else's table, keep
/// SELECT on it, then hand the view to a role you do not hold. The transfer
/// is the step that must fail.
#[test]
fn alter_view_owner_requires_membership_in_the_new_owner() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("CREATE VIEW leak AS SELECT id, v FROM secrets")
        .unwrap();
    mallory.execute("GRANT SELECT ON leak TO mallory").unwrap();
    let error = mallory
        .execute("ALTER VIEW leak OWNER TO alice")
        .expect_err("handing a view to an unheld role must be refused");
    assert_refused(error);
    assert_no_canary(&mut mallory, "SELECT * FROM leak");
}

#[test]
fn alter_sequence_owner_requires_membership_in_the_new_owner() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory.execute("CREATE SEQUENCE mine").unwrap();
    let error = mallory
        .execute("ALTER SEQUENCE mine OWNER TO alice")
        .expect_err("handing a sequence to an unheld role must be refused");
    assert_refused(error);
}

#[test]
fn alter_schema_owner_requires_authority() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("ALTER SCHEMA public OWNER TO mallory")
        .expect_err("taking ownership of a schema must be refused");
    assert_refused(error);
}

#[test]
fn alter_database_owner_requires_authority() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("ALTER DATABASE bicdb OWNER TO mallory")
        .expect_err("taking ownership of the database must be refused");
    assert_refused(error);
}

// ---------------------------------------------------------------- P0 #2 ----

/// The grantor gate fell through for views because they are not in the table
/// store — the exact fail-open that turned a self-grant into a table read.
#[test]
fn grant_on_a_view_requires_ownership() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("GRANT SELECT ON secret_view TO mallory")
        .expect_err("self-granting on another role's view must be refused");
    assert_refused(error);
    assert_no_canary(&mut mallory, "SELECT * FROM secret_view");
}

#[test]
fn grant_on_a_sequence_schema_or_database_requires_ownership() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    for sql in [
        "GRANT USAGE ON SEQUENCE secret_seq TO mallory",
        "GRANT CREATE ON SCHEMA public TO mallory",
        "GRANT ALL ON DATABASE bicdb TO mallory",
    ] {
        let error = mallory
            .execute(sql)
            .err()
            .unwrap_or_else(|| panic!("{sql} must be refused"));
        assert_refused(error);
    }
}

/// REVOKE shares the gate, so stripping someone else's privileges must fail
/// for the same reason a self-grant does.
#[test]
fn revoke_on_a_view_requires_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT SELECT ON secret_view TO alice")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("REVOKE SELECT ON secret_view FROM alice")
        .expect_err("revoking on another role's view must be refused");
    assert_refused(error);
}

/// The gate must not have swung shut on legitimate owners.
#[test]
fn the_owner_can_still_grant_and_revoke() {
    let (_directory, mut db) = seeded();
    let mut owner = SqlSession::new(&mut db);
    for sql in [
        "GRANT SELECT ON secret_view TO alice",
        "REVOKE SELECT ON secret_view FROM alice",
        "GRANT USAGE ON SEQUENCE secret_seq TO alice",
        "GRANT CREATE ON SCHEMA public TO alice",
        "GRANT ALL ON DATABASE bicdb TO alice",
        "GRANT SELECT ON secrets TO alice",
    ] {
        owner
            .execute(sql)
            .unwrap_or_else(|error| panic!("the owner must still be able to run {sql}: {error:?}"));
    }
}

// ---------------------------------------------------------------- P0 #3 ----

#[test]
fn drop_view_requires_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut mallory = as_mallory(&mut db);
        let error = mallory
            .execute("DROP VIEW secret_view")
            .expect_err("dropping another role's view must be refused");
        assert_refused(error);
    }
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("SELECT * FROM secret_view")
        .expect("the view must survive the refused drop");
}

#[test]
fn drop_sequence_requires_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut mallory = as_mallory(&mut db);
        let error = mallory
            .execute("DROP SEQUENCE secret_seq")
            .expect_err("dropping another role's sequence must be refused");
        assert_refused(error);
    }
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("SELECT nextval('secret_seq')")
        .expect("the sequence must survive the refused drop");
}

#[test]
fn drop_type_requires_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE TYPE mood AS ENUM ('ok', 'bad')")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("DROP TYPE mood")
        .expect_err("dropping another role's type must be refused");
    assert_refused(error);
}

/// Owners must still be able to destroy their own objects.
#[test]
fn the_owner_can_still_drop() {
    let (_directory, mut db) = seeded();
    let mut owner = SqlSession::new(&mut db);
    owner.execute("DROP VIEW secret_view").unwrap();
    owner.execute("DROP SEQUENCE secret_seq").unwrap();
}
