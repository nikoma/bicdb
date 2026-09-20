//! The absence of a security context must never increase privilege.
//!
//! A session with no identity resolved its user through the GUC chain to
//! the bootstrap role — which `current_user_is_superuser` treats as
//! superuser and which owns every table by default. So any component that
//! built a SQL session without a context (the RESP HotView path did) got
//! silent, complete administrative access: RLS bypassed, tenant policy
//! bypassed, GRANTs irrelevant, `ALTER TABLE ... DISABLE ROW LEVEL
//! SECURITY` available.
//!
//! That default is correct for the embedded API, whose caller already owns
//! the `&mut BicDb` it is holding. It is wrong for anything that builds a
//! session per network request, which is what
//! `SqlSession::new_unprivileged` exists for.

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
fn an_unprivileged_session_is_not_a_superuser() {
    let (_directory, mut db) = db();
    {
        // Owner-side setup through the trusted embedded API.
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE TABLE secrets (id INT PRIMARY KEY, value TEXT)")
            .unwrap();
        owner
            .execute("INSERT INTO secrets VALUES (1, 'classified')")
            .unwrap();
        owner
            .execute("ALTER TABLE secrets ENABLE ROW LEVEL SECURITY")
            .unwrap();
        owner
            .execute("ALTER TABLE secrets FORCE ROW LEVEL SECURITY")
            .unwrap();
    }

    let mut session = SqlSession::new_unprivileged(&mut db, "cache_service");

    // It must not be able to switch RLS off — the move that turned the
    // implicit-superuser bug into a total bypass.
    let error = session
        .execute("ALTER TABLE secrets DISABLE ROW LEVEL SECURITY")
        .expect_err("an unprivileged session must not disable RLS");
    let rendered = format!("{error}");
    assert!(
        rendered.contains("owner") || rendered.contains("permission"),
        "unexpected refusal: {rendered}"
    );

    // With FORCE RLS on and no policy granting it anything, it must not
    // read the protected rows.
    let rows = session
        .execute("SELECT value FROM secrets")
        .map(|result| result.rows.len())
        .unwrap_or(0);
    assert_eq!(rows, 0, "an unprivileged session read RLS-protected rows");

    // Sanity: the trusted embedded session retains full authority, so this
    // change does not disturb the in-process API. (FORCE RLS applies to the
    // owner too, exactly as in PostgreSQL, so the owner turns it off first
    // — the very operation the unprivileged session was refused.)
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("ALTER TABLE secrets NO FORCE ROW LEVEL SECURITY")
        .expect("the embedded API keeps administrative authority");
    assert_eq!(
        owner
            .execute("SELECT value FROM secrets")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn the_unprivileged_constructor_refuses_to_hand_back_the_bootstrap_identity() {
    let (_directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .unwrap();
        owner
            .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
            .unwrap();
        owner
            .execute("ALTER TABLE t FORCE ROW LEVEL SECURITY")
            .unwrap();
    }
    // Asking for the bootstrap role by name through the constructor whose
    // purpose is to withhold it must not grant it.
    for requested in ["bicdb", "BICDB", "", "  "] {
        let mut session = SqlSession::new_unprivileged(&mut db, requested);
        assert!(
            session
                .execute("ALTER TABLE t DISABLE ROW LEVEL SECURITY")
                .is_err(),
            "requesting `{requested}` yielded administrative authority"
        );
    }
}
