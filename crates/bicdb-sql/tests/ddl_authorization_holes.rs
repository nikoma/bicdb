//! Authorization checks that were simply absent.
//!
//! A re-audit ran these as an ordinary non-superuser role against tables it
//! had no rights to, and they all succeeded: `TRUNCATE` emptied the table,
//! `DROP TABLE` destroyed it, `min`/`max` read primary keys straight out of
//! it, and `ts_rewrite` executed caller-supplied SQL as the bootstrap
//! superuser. The GRANT/ALTER/policy gates added earlier never reached
//! these paths.
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

/// Owner-side setup, then an ordinary role with no grants at all.
fn seeded() -> (tempfile::TempDir, BicDb) {
    let (directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("CREATE ROLE mallory LOGIN").unwrap();
        owner
            .execute("CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        owner
            .execute("INSERT INTO secrets VALUES (1, 'classified'), (2, 'also classified')")
            .unwrap();
    }
    (directory, db)
}

fn as_mallory(db: &mut BicDb) -> SqlSession<'_> {
    SqlSession::new_unprivileged(db, "mallory")
}

#[test]
fn truncate_requires_authority() {
    let (_directory, mut db) = seeded();
    {
        let mut mallory = as_mallory(&mut db);
        let error = mallory
            .execute("TRUNCATE secrets")
            .expect_err("TRUNCATE must require authority");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("owner") || rendered.contains("permission"),
            "unexpected refusal: {rendered}"
        );
    }
    // The rows survived.
    let mut owner = SqlSession::new(&mut db);
    assert_eq!(
        owner.execute("SELECT id FROM secrets").unwrap().rows.len(),
        2,
        "TRUNCATE destroyed rows despite being refused"
    );
}

#[test]
fn drop_table_and_index_require_authority() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE INDEX ix_secrets ON secrets (v)")
            .unwrap();
    }
    {
        let mut mallory = as_mallory(&mut db);
        assert!(
            mallory.execute("DROP TABLE secrets").is_err(),
            "DROP TABLE must require authority"
        );
        assert!(
            mallory.execute("DROP INDEX ix_secrets").is_err(),
            "DROP INDEX must require authority"
        );
        assert!(
            mallory
                .execute("CREATE INDEX ix_mallory ON secrets (id)")
                .is_err(),
            "CREATE INDEX must require authority"
        );
    }
    // The table and its rows survived.
    let mut owner = SqlSession::new(&mut db);
    assert_eq!(
        owner.execute("SELECT id FROM secrets").unwrap().rows.len(),
        2
    );
}

/// `min`/`max` over a primary key took an index fast path that ran BEFORE
/// the SELECT-privilege gate. With a composite key it was also an
/// existence and range oracle over another tenant's records.
#[test]
fn indexed_extreme_aggregate_requires_select_privilege() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute(
                "CREATE TABLE tenanted (tenant TEXT, record TEXT, PRIMARY KEY (tenant, record))",
            )
            .unwrap();
        owner
            .execute("INSERT INTO tenanted VALUES ('acme', 'r1'), ('acme', 'r9'), ('other', 'r5')")
            .unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    for statement in [
        "SELECT min(id) FROM secrets",
        "SELECT max(id) FROM secrets",
        // The oracle form: bind the leading key column, read the range.
        "SELECT max(record) FROM tenanted WHERE tenant = 'acme'",
        "SELECT min(record) FROM tenanted WHERE tenant = 'acme'",
    ] {
        let error = mallory
            .execute(statement)
            .expect_err("the extreme-aggregate fast path must check SELECT");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("permission") || rendered.contains("denied"),
            "unexpected refusal for `{statement}`: {rendered}"
        );
    }
}

/// `ts_rewrite`'s second argument is caller-supplied SQL, and it ran in a
/// sub-engine with no security context — i.e. as the bootstrap role, which
/// holds SELECT on every relation. Full exfiltration, not just an oracle.
#[test]
fn ts_rewrite_runs_with_the_callers_identity() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);

    // Reading a table it has no rights to must now be refused.
    let error = mallory
        .execute(
            "SELECT ts_rewrite('a'::tsquery, \
             'SELECT ''a''::tsquery, plainto_tsquery(v) FROM secrets')",
        )
        .expect_err("the ts_rewrite sub-engine must carry the caller's identity");
    let rendered = format!("{error}");
    assert!(
        !rendered.contains("classified"),
        "the refusal leaked the data it refused: {rendered}"
    );

    // And it must no longer report itself as the bootstrap superuser.
    if let Ok(result) = mallory.execute(
        "SELECT ts_rewrite('a'::tsquery, \
         'SELECT ''a''::tsquery, plainto_tsquery(current_user)')",
    ) {
        let rendered = format!("{:?}", result.rows);
        assert!(
            !rendered.contains("bicdb"),
            "ts_rewrite still evaluates as the bootstrap role: {rendered}"
        );
    }
}
