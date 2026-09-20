//! A SECURITY DEFINER routine with no recorded owner must not run as bootstrap.
//!
//! Sibling of the ownerless-view finding, and the same inversion:
//! `RoutineSchema::owner` defaulted to the bootstrap role via
//! `#[serde(default = "default_routine_owner")]`, so a routine persisted
//! before ownership tracking deserialized as bootstrap-owned. For OWNERSHIP
//! that is fail-closed — only a superuser may `ALTER FUNCTION ... OWNER TO`
//! it. For SECURITY DEFINER EXECUTION it inverted: the body ran with superuser
//! authority over every table it touched.
//!
//! Worse than the view case, because a routine can WRITE.
//!
//! Three execution sites read that owner (SQL functions, procedures, and the
//! PL/pgSQL engine path); making the field an `Option` is what surfaced the
//! third.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::SqlSession;

const ROUTINES: &str = "__bicdb_pg_routines";

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
        ] {
            owner.execute(sql).unwrap();
        }
    }
    {
        let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
        mallory
            .execute(
                "CREATE FUNCTION peek() RETURNS TEXT AS $$ SELECT v FROM secrets LIMIT 1 $$ \
                 LANGUAGE sql SECURITY DEFINER",
            )
            .unwrap();
    }
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT EXECUTE ON FUNCTION peek() TO mallory")
            .unwrap();
    }
    (directory, db)
}

/// Rewrite the stored routine without an `owner` field, exactly as a routine
/// persisted before ownership tracking deserializes.
fn strip_owner(db: &mut BicDb) {
    let ids: Vec<String> = db
        .scan_collection(ROUTINES)
        .unwrap()
        .iter()
        .map(|record| record.id.clone())
        .collect();
    assert!(!ids.is_empty(), "the routine must have been persisted");
    for id in ids {
        let existing = db.get(ROUTINES, &id).unwrap().unwrap();
        let mut metadata = existing.metadata.clone();
        metadata.as_object_mut().unwrap().remove("owner");
        db.insert(ROUTINES, Record::new(&id).with_metadata(metadata))
            .unwrap();
    }
}

fn peek(session: &mut SqlSession<'_>) -> String {
    match session.execute("SELECT peek() FROM generate_series(1,1)") {
        Ok(rows) => format!("{:?}", rows.rows),
        Err(error) => format!("{error}"),
    }
}

/// The A/B: the only variable is whether the owner field is recorded.
#[test]
fn an_ownerless_definer_routine_does_not_run_as_bootstrap() {
    let (_directory, mut db) = seeded();
    {
        let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
        assert!(
            peek(&mut mallory).contains("permission denied"),
            "control: with its owner recorded the body runs as mallory"
        );
    }
    strip_owner(&mut db);
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    let outcome = peek(&mut mallory);
    assert!(
        !outcome.contains("P0-CANARY"),
        "an ownerless SECURITY DEFINER routine ran as bootstrap: {outcome}"
    );
}

/// The fallback is invoker semantics, not a refusal: a caller who could read
/// the sources anyway keeps working, so an upgrade does not break every
/// legacy routine.
#[test]
fn an_ownerless_definer_routine_still_works_for_an_authorized_caller() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("GRANT SELECT ON secrets TO mallory").unwrap();
    }
    strip_owner(&mut db);
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    assert!(
        peek(&mut mallory).contains("P0-CANARY"),
        "a caller granted the base table must still call the routine"
    );
}

/// An OWNED definer routine keeps definer semantics — that is the feature.
#[test]
fn an_owned_definer_routine_still_runs_as_its_owner() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute(
                "CREATE FUNCTION owned_peek() RETURNS TEXT AS $$ SELECT v FROM secrets LIMIT 1 $$ \
                 LANGUAGE sql SECURITY DEFINER",
            )
            .unwrap();
        owner
            .execute("GRANT EXECUTE ON FUNCTION owned_peek() TO mallory")
            .unwrap();
    }
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    let rows = mallory
        .execute("SELECT owned_peek() FROM generate_series(1,1)")
        .expect("a bootstrap-OWNED definer routine must still elevate for its grantee");
    assert!(
        format!("{:?}", rows.rows).contains("P0-CANARY"),
        "definer semantics must survive: {:?}",
        rows.rows
    );
}

/// Ownership stays fail-closed: an unrecorded owner is still bootstrap-owned,
/// so an unprivileged role cannot seize the routine.
#[test]
fn an_ownerless_routine_is_still_only_reassignable_by_a_superuser() {
    let (_directory, mut db) = seeded();
    strip_owner(&mut db);
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    let error = mallory
        .execute("ALTER FUNCTION peek() OWNER TO mallory")
        .expect_err("an ownerless routine must stay superuser-only to reassign");
    let rendered = format!("{error}");
    assert!(
        rendered.contains("owner") || rendered.contains("permission"),
        "unexpected refusal: {rendered}"
    );
}
