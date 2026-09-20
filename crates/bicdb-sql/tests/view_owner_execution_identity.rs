//! A view with no recorded owner must not execute as the bootstrap role.
//!
//! `ViewSchema::owner` is an `Option`, and views persisted before ownership
//! tracking carry `None`. Everywhere that matters for OWNERSHIP, the
//! convention reads "treated as owned by the bootstrap role", which is
//! fail-closed: only a superuser may alter or drop such a view.
//!
//! Definer EXECUTION used the same convention and it inverted:
//! `view.owner.unwrap_or_else(current_role_name)` resolves to the bootstrap
//! role, so an ownerless view ran its body with SUPERUSER read authority over
//! every underlying table. Anyone holding SELECT on the view — a grant that
//! survives an upgrade — read through it as bootstrap.
//!
//! Same family as H-14 and H-15: missing security information read as maximum
//! authority. Here the identical default is fail-closed on one side of the
//! system and fail-open on the other.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::SqlSession;

const VIEWS: &str = "__bicdb_pg_views";

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
            "INSERT INTO secrets VALUES (1, 'P0-CANARY')",
            "CREATE VIEW readable AS SELECT id, v FROM secrets",
            "GRANT SELECT ON readable TO mallory",
        ] {
            owner.execute(sql).unwrap();
        }
    }
    (directory, db)
}

/// Rewrite the stored view record without an `owner` field, exactly as a view
/// persisted before ownership tracking deserializes.
fn strip_owner(db: &mut BicDb, view: &str) {
    let existing = db.get(VIEWS, view).unwrap().unwrap();
    let mut metadata = existing.metadata.clone();
    metadata.as_object_mut().unwrap().remove("owner");
    assert!(metadata.get("owner").is_none());
    db.insert(VIEWS, Record::new(view).with_metadata(metadata))
        .unwrap();
}

#[test]
fn an_ownerless_view_does_not_execute_as_the_bootstrap_role() {
    let (_directory, mut db) = seeded();
    strip_owner(&mut db, "readable");

    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    mallory
        .execute("SELECT v FROM secrets")
        .expect_err("the control: mallory cannot read the base table");
    match mallory.execute("SELECT v FROM readable") {
        Ok(rows) => assert!(
            !format!("{:?}", rows.rows).contains("P0-CANARY"),
            "an ownerless view leaked the base table: {:?}",
            rows.rows
        ),
        Err(error) => {
            let rendered = format!("{error}");
            assert!(
                rendered.contains("permission denied"),
                "unexpected refusal: {rendered}"
            );
        }
    }
}

/// The fallback is invoker semantics, not a hard refusal: a caller who could
/// read the sources anyway keeps working, so an upgrade does not break every
/// legacy view.
#[test]
fn an_ownerless_view_still_works_for_a_caller_who_can_read_the_sources() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("GRANT SELECT ON secrets TO mallory").unwrap();
    }
    strip_owner(&mut db, "readable");

    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    let rows = mallory
        .execute("SELECT v FROM readable")
        .expect("a caller granted the base table must still read the view");
    assert!(format!("{:?}", rows.rows).contains("P0-CANARY"));
}

/// An OWNED view keeps definer semantics — the whole point of the feature.
#[test]
fn an_owned_view_still_executes_as_its_owner() {
    let (_directory, mut db) = seeded();
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    mallory
        .execute("SELECT v FROM secrets")
        .expect_err("the control: mallory cannot read the base table");
    let rows = mallory
        .execute("SELECT v FROM readable")
        .expect("a bootstrap-owned view must still let a grantee read through it");
    assert!(
        format!("{:?}", rows.rows).contains("P0-CANARY"),
        "definer semantics must survive: {:?}",
        rows.rows
    );
}

/// Ownership checks keep treating a missing owner as bootstrap-owned, which is
/// the fail-CLOSED direction and must not have been loosened.
#[test]
fn an_ownerless_view_is_still_only_alterable_by_a_superuser() {
    let (_directory, mut db) = seeded();
    strip_owner(&mut db, "readable");
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    let error = mallory
        .execute("DROP VIEW readable")
        .expect_err("an ownerless view must still be superuser-only to drop");
    assert!(
        format!("{error}").contains("owner"),
        "unexpected refusal: {error}"
    );
}
