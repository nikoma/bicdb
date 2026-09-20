//! A database directory has exactly one writer.
//!
//! Two processes sharing one directory is not a degraded mode, it is silent
//! data loss. Reproduced before the fix by running two `bicdb serve-pg`
//! servers against one path: both accepted writes and acknowledged them, and
//! the store then refused to open at all, reporting
//! `[recovery] skipped 3 committed write(s) to dropped collection` followed by
//! `collection not found`. The rows the clients had been told were committed
//! were gone and the database was unopenable.
//!
//! The paged engine had always claimed its own subdirectory. Nothing claimed
//! the database root, so the default storage mode was unprotected.

use bicdb_core::BicDb;

#[test]
fn a_second_handle_on_the_same_directory_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut first = BicDb::open(dir.path()).expect("first open succeeds");

    let second = BicDb::open(dir.path());
    let error = match second {
        Ok(_) => panic!(
            "a second writer was allowed on {}: this is the configuration \
             that silently discarded committed transactions",
            dir.path().display()
        ),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("already open"),
        "the refusal must say why, got: {error}"
    );

    // The incumbent is untouched by the refused open.
    first
        .create_collection("kept")
        .expect("the first handle still owns the database");
    drop(first);
}

#[test]
fn the_claim_is_released_when_the_owner_is_dropped() {
    let dir = tempfile::tempdir().expect("tempdir");

    let mut first = BicDb::open(dir.path()).expect("first open");
    first.create_collection("survives").expect("create");
    drop(first);

    // A normal reopen must still work: the lock excludes concurrent owners,
    // not sequential ones.
    let second = BicDb::open(dir.path()).expect("reopen after drop succeeds");
    assert!(
        second.collections().iter().any(|c| c.name == "survives"),
        "the reopened database must still see committed state"
    );
}
