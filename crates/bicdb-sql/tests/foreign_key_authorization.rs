//! Foreign keys attached to relations the caller has no rights on.
//!
//! Reported as "FK ON DELETE CASCADE bypasses child-table privileges", but the
//! cascade is not the defect: PostgreSQL runs referential actions with the
//! constraint's authority, not the caller's, so gating the cascade on the
//! deleter's DELETE privilege would diverge from PostgreSQL and break the
//! ordinary pattern of a role holding DELETE on a parent but not its children.
//!
//! The real missing gate is one step earlier — the REFERENCES privilege on the
//! table an FK points at, which BicDB never checked. Probing an unprivileged
//! role showed that gap is worse than the cascade:
//!
//! - **Existence oracle.** An insert into the child succeeds only when the
//!   parent row exists, so the parent's key space is enumerable with no SELECT
//!   privilege — a cross-tenant read primitive.
//! - **Lock-in.** The parent's owner can no longer delete the referenced rows
//!   (23503), and cannot drop the constraint, because it lives on a table
//!   belonging to the attacker.
//! - It is also the precondition for wiring a CASCADE into an unowned table
//!   in the first place.
//!
//! With REFERENCES enforced, a cascade into a table the caller does not own can
//! only exist because someone with authority over the parent allowed the link —
//! which is exactly PostgreSQL's model.

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
            "INSERT INTO secrets VALUES (7, 'P0-CANARY')",
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

#[test]
fn create_table_referencing_an_unreadable_table_is_refused() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("SELECT * FROM secrets")
        .expect_err("the control: mallory cannot read secrets");
    let error = mallory
        .execute("CREATE TABLE oracle (id INT PRIMARY KEY, k INT REFERENCES secrets(id))")
        .expect_err("an FK onto an unprivileged table must be refused");
    assert_refused(error);
}

#[test]
fn alter_table_add_foreign_key_to_an_unreadable_table_is_refused() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("CREATE TABLE oracle (id INT PRIMARY KEY, k INT)")
        .unwrap();
    let error = mallory
        .execute("ALTER TABLE oracle ADD CONSTRAINT fk FOREIGN KEY (k) REFERENCES secrets(id)")
        .expect_err("ALTER TABLE must gate the FK target too");
    assert_refused(error);
}

/// The reason the gate matters: without it the child table answers "does this
/// key exist in the parent?" for a role with no SELECT on the parent.
#[test]
fn the_foreign_key_existence_oracle_is_closed() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    assert!(
        mallory
            .execute("CREATE TABLE oracle (id INT PRIMARY KEY, k INT REFERENCES secrets(id))")
            .is_err(),
        "the oracle table must not be creatable"
    );
    // Nothing to probe with: the discriminating insert pair cannot even be set up.
    assert!(
        mallory.execute("INSERT INTO oracle VALUES (1, 7)").is_err(),
        "no oracle table may exist to probe with"
    );
}

/// The lock-in half: the parent's owner must not lose control of their own rows.
#[test]
fn the_owner_keeps_control_of_the_referenced_rows() {
    let (_directory, mut db) = seeded();
    {
        let mut mallory = as_mallory(&mut db);
        // Both steps are refused now; on the unfixed build they succeed and
        // the child row is what pins the owner's row in place.
        let _ = mallory
            .execute("CREATE TABLE oracle (id INT PRIMARY KEY, k INT REFERENCES secrets(id))");
        let _ = mallory.execute("INSERT INTO oracle VALUES (1, 7)");
    }
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("DELETE FROM secrets WHERE id = 7")
        .expect("the owner must still be able to delete their own row");
}

/// The gate must not break the legitimate case: with REFERENCES granted, the
/// link is allowed and its cascade behaves exactly as PostgreSQL's does — the
/// deleter needs no privilege on the child.
#[test]
fn granted_references_still_allows_the_link_and_its_cascade() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT REFERENCES, SELECT, DELETE ON secrets TO mallory")
            .unwrap();
    }
    {
        let mut mallory = as_mallory(&mut db);
        mallory
            .execute(
                "CREATE TABLE child (id INT PRIMARY KEY, k INT REFERENCES secrets(id) \
                 ON DELETE CASCADE)",
            )
            .expect("a granted REFERENCES must permit the foreign key");
        mallory.execute("INSERT INTO child VALUES (1, 7)").unwrap();
        mallory
            .execute("DELETE FROM secrets WHERE id = 7")
            .expect("the cascade runs with the constraint's authority, as in PostgreSQL");
        let rows = mallory.execute("SELECT id FROM child").unwrap();
        assert!(
            rows.rows.is_empty(),
            "the cascade must have removed the child row"
        );
    }
}

/// A self-referencing FK needs no separate authority — the caller is already
/// creating the table.
#[test]
fn self_referencing_foreign_keys_still_work() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("CREATE TABLE tree (id INT PRIMARY KEY, parent INT REFERENCES tree(id))")
        .expect("a self-reference must not require a separate privilege");
}

/// The path-B bypass of this very gate: `LIKE ... INCLUDING CONSTRAINTS`
/// clones the source table's foreign keys onto the new table, attaching it to
/// whatever those keys point at — without ever going through the two declared
/// FK-creation paths the gate was placed on.
#[test]
fn like_including_constraints_does_not_clone_foreign_keys_past_the_gate() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE TABLE src (id INT PRIMARY KEY, p INT REFERENCES secrets(id))")
            .unwrap();
    }
    {
        let mut mallory = as_mallory(&mut db);
        let error = mallory
            .execute("CREATE TABLE mycopy (LIKE src INCLUDING CONSTRAINTS)")
            .expect_err("cloning an FK onto an unprivileged table must be refused");
        assert_refused(error);
        // No oracle to probe with, and no lock-in.
        assert!(
            mallory.execute("INSERT INTO mycopy VALUES (1, 7)").is_err(),
            "the cloned table must not exist"
        );
    }
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("DELETE FROM secrets WHERE id = 7")
        .expect("the owner must keep control of the referenced row");
}

/// LIKE without the constraints, and LIKE of a table with no foreign keys,
/// must both still work — the gate is on the cloned FKs, not on LIKE.
#[test]
fn like_without_foreign_keys_still_works() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("CREATE TABLE plain (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    mallory
        .execute("CREATE TABLE copy_a (LIKE plain INCLUDING CONSTRAINTS)")
        .expect("cloning a table with no foreign keys must still work");
}
