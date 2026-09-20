//! Write-path and cascade authorization holes.
//!
//! Round two of the same audit. Every finding here is a gate that exists on
//! one statement and is missing on a path that reaches the same effect:
//! COPY reaches INSERT, RETURNING reaches SELECT, TRUNCATE CASCADE reaches
//! tables the caller never named, and the recognized `DO $carrier_*$` blocks
//! reach the privilege store without passing GRANT/REVOKE at all.
//!
//! Each test below is that exploit, now expected to be refused.

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

fn seeded() -> (tempfile::TempDir, BicDb) {
    let (directory, mut db) = db();
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
    (directory, db)
}

fn as_mallory(db: &mut BicDb) -> SqlSession<'_> {
    SqlSession::new_unprivileged(db, "mallory")
}

fn assert_refused(error: bicdb_sql::SqlError) {
    let rendered = format!("{error}");
    assert!(
        rendered.contains("owner")
            || rendered.contains("permission")
            || rendered.contains("superuser"),
        "unexpected refusal: {rendered}"
    );
}

/// COPY FROM is an INSERT with a different wire shape; it enforced RLS but
/// never the INSERT privilege.
#[test]
fn copy_from_requires_the_insert_privilege() {
    let (_directory, mut db) = seeded();
    {
        let mut mallory = as_mallory(&mut db);
        mallory
            .execute("INSERT INTO secrets VALUES (2, 'x')")
            .expect_err("the control: plain INSERT is gated");
        let error = mallory
            .copy_insert_rows(
                "secrets",
                &["id".to_string(), "v".to_string()],
                vec![vec![
                    Some("2".to_string()),
                    Some("HACKED-VIA-COPY".to_string()),
                ]],
            )
            .expect_err("COPY FROM must require INSERT too");
        assert_refused(error);
    }
    let mut owner = SqlSession::new(&mut db);
    let rows = owner.execute("SELECT v FROM secrets").unwrap();
    assert!(
        !format!("{rows:?}").contains("HACKED-VIA-COPY"),
        "COPY loaded a row into a table the caller could not INSERT into: {rows:?}"
    );
}

/// RETURNING projects the affected rows back, so a write-only grant became a
/// full table read.
#[test]
fn returning_requires_the_select_privilege() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT UPDATE, DELETE, INSERT ON secrets TO mallory")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("SELECT * FROM secrets")
        .expect_err("the control: mallory has no SELECT");
    for sql in [
        "UPDATE secrets SET v = v RETURNING *",
        "DELETE FROM secrets RETURNING *",
        "INSERT INTO secrets VALUES (9, 'z') RETURNING *",
    ] {
        match mallory.execute(sql) {
            Ok(rows) => panic!("{sql} must require SELECT, returned {rows:?}"),
            Err(error) => assert_refused(error),
        }
    }
}

/// A write-only grant must still work without RETURNING — the gate must be
/// on the projection, not on the write.
#[test]
fn writes_without_returning_still_work_for_a_write_only_grant() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT UPDATE, DELETE, INSERT ON secrets TO mallory")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("INSERT INTO secrets VALUES (7, 'written')")
        .expect("a write-only grant must still be able to write");
    mallory
        .execute("UPDATE secrets SET v = 'changed' WHERE id = 7")
        .expect("a write-only grant must still be able to update");
}

/// The cascade loop appended FK children to the truncate set AFTER the
/// ownership check ran, so naming a table you own emptied tables you don't.
#[test]
fn truncate_cascade_authorizes_the_children_it_pulls_in() {
    let (_directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE mallory LOGIN NOSUPERUSER",
            "CREATE TABLE bait (id INT PRIMARY KEY)",
            "CREATE TABLE victim (id INT PRIMARY KEY, bait_id INT REFERENCES bait(id))",
            "INSERT INTO bait VALUES (1)",
            "INSERT INTO victim VALUES (1, 1)",
            "ALTER TABLE bait OWNER TO mallory",
        ] {
            owner.execute(sql).unwrap();
        }
    }
    {
        let mut mallory = as_mallory(&mut db);
        let error = mallory
            .execute("TRUNCATE bait CASCADE")
            .expect_err("cascading into a table mallory does not own must be refused");
        assert_refused(error);
    }
    let mut owner = SqlSession::new(&mut db);
    let rows = owner.execute("SELECT id FROM victim").unwrap();
    assert!(
        !rows.rows.is_empty(),
        "the victim table was emptied by a cascade the caller was never authorized for"
    );
}

#[test]
fn lock_table_requires_a_privilege() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("LOCK TABLE secrets")
        .expect_err("locking another tenant's table must be refused");
    assert_refused(error);
}

#[test]
fn alter_table_owner_requires_membership_in_the_new_owner() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE ROLE alice LOGIN NOSUPERUSER")
            .unwrap();
        owner
            .execute("CREATE TABLE mallory_own (id INT PRIMARY KEY)")
            .unwrap();
        owner
            .execute("ALTER TABLE mallory_own OWNER TO mallory")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("ALTER TABLE mallory_own OWNER TO alice")
        .expect_err("handing a table to an unheld role must be refused");
    assert_refused(error);
}

/// The recognized provisioning block wipes every table grant to
/// public/carrier_app straight through the privilege store.
#[test]
fn the_carrier_revoke_provisioning_block_requires_superuser() {
    let (_directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE mallory LOGIN NOSUPERUSER",
            "CREATE ROLE carrier_app LOGIN NOSUPERUSER",
            "CREATE TABLE t2 (id INT PRIMARY KEY)",
            "GRANT SELECT ON t2 TO carrier_app",
        ] {
            owner.execute(sql).unwrap();
        }
    }
    let block = "DO $carrier_revoke_relations$ BEGIN \
         PERFORM 1 FROM pg_catalog.pg_class AS class; \
         EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC, carrier_app', 't2'); \
         END $carrier_revoke_relations$;";
    {
        let mut mallory = as_mallory(&mut db);
        let error = mallory
            .execute(block)
            .expect_err("an unprivileged role must not be able to wipe every grant");
        assert_refused(error);
    }
    let mut carrier = SqlSession::new_unprivileged(&mut db, "carrier_app");
    carrier
        .execute("SELECT * FROM t2")
        .expect("carrier_app must keep the grant the block tried to wipe");
}

/// The chain the audit asked about: use the ungated authored-function
/// provisioning block to grant yourself EXECUTE on a SECURITY DEFINER routine
/// owned by a privileged role, then call it to read what you cannot select.
/// The block gate must break the first link.
#[test]
fn the_authored_function_block_cannot_launder_execute_on_a_definer_routine() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute(
                "CREATE FUNCTION peek() RETURNS TEXT AS $$ SELECT v FROM secrets LIMIT 1 $$ \
                 LANGUAGE sql SECURITY DEFINER",
            )
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let denied = mallory
        .execute("SELECT peek()")
        .expect_err("the control: mallory has no EXECUTE on the definer routine");
    assert_refused(denied);

    let block = "DO $carrier_grant_authored_functions$ BEGIN \
         EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %s', 'peek()', 'mallory'); \
         END $carrier_grant_authored_functions$;";
    // Either the block is refused outright or it is not recognized; what must
    // never happen is mallory ending up able to call the routine.
    let _ = mallory.execute(block);
    match mallory.execute("SELECT peek()") {
        Ok(rows) => assert!(
            !format!("{rows:?}").contains("P0-CANARY"),
            "EXECUTE was laundered through the provisioning block: {rows:?}"
        ),
        Err(error) => assert_refused(error),
    }
}
