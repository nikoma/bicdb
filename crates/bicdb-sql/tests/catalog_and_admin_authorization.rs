//! Two more audit rounds, two more shapes.
//!
//! 1. `filter_virtual_rows_for_role` filtered exactly one catalog
//!    (`bicdb_notifications`) and returned every other virtual catalog
//!    unfiltered — so any catalog carrying tenant data was world-readable.
//!    `pg_stats` is the worst of them: it publishes most_common_vals and
//!    histogram_bounds, which are ACTUAL column values sampled over every row
//!    with RLS ignored.
//! 2. The raw-SQL handlers dispatched from `execute_inner` never got the
//!    gates the parsed-Statement paths did — extensions, VACUUM, TRIM AUDIT
//!    HISTORY, ALTER TABLE SET SCHEMA, CREATE SCHEMA, ANALYZE.
//!
//! Each test below is that exploit, now expected to be refused.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

const CARD: &str = "4111-1111-1111-1111";

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
            "CREATE TABLE secrets (id INT PRIMARY KEY, card TEXT DEFAULT 'hardcoded-secret-token')",
        ] {
            owner.execute(sql).unwrap();
        }
        owner
            .execute(&format!("INSERT INTO secrets VALUES (1, '{CARD}')"))
            .unwrap();
        // Statistics are gathered over every row with RLS ignored, so lock the
        // table down AFTER the data is in and analyzed — that is exactly the
        // state in which pg_stats was handing the values back out.
        owner.execute("ANALYZE secrets").unwrap();
        for sql in [
            "ALTER TABLE secrets ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE secrets FORCE ROW LEVEL SECURITY",
            "CREATE POLICY tenant_only ON secrets USING (card = 'acme-secret-literal')",
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

fn assert_hidden(session: &mut SqlSession<'_>, sql: &str, needle: &str) {
    match session.execute(sql) {
        Ok(rows) => assert!(
            !format!("{rows:?}").contains(needle),
            "{sql} leaked {needle}: {rows:?}"
        ),
        Err(error) => assert_refused(error),
    }
}

// ------------------------------------------------------- catalog leaks ----

/// The worst of the set: statistics are computed over every row with RLS
/// ignored, so pg_stats handed out real column values from a table under
/// FORCE ROW LEVEL SECURITY.
#[test]
fn pg_stats_does_not_leak_column_values() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("SELECT * FROM secrets")
        .expect_err("the control: mallory cannot read secrets");
    assert_hidden(
        &mut mallory,
        "SELECT most_common_vals, histogram_bounds FROM pg_catalog.pg_stats",
        CARD,
    );
}

#[test]
fn pg_policies_does_not_leak_policy_expressions() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    assert_hidden(
        &mut mallory,
        "SELECT qual FROM pg_catalog.pg_policies",
        "acme-secret-literal",
    );
    assert_hidden(
        &mut mallory,
        "SELECT polqual FROM pg_catalog.pg_policy",
        "acme-secret-literal",
    );
}

#[test]
fn pg_attrdef_does_not_leak_column_defaults() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    assert_hidden(
        &mut mallory,
        "SELECT adbin FROM pg_catalog.pg_attrdef",
        "hardcoded-secret-token",
    );
}

/// The owner must still see their own catalog rows — the filter is per-role,
/// not a blanket blackout.
#[test]
fn the_owner_still_sees_its_own_catalog_rows() {
    let (_directory, mut db) = seeded();
    let mut owner = SqlSession::new(&mut db);
    for (sql, needle) in [
        (
            "SELECT qual FROM pg_catalog.pg_policies",
            "acme-secret-literal",
        ),
        (
            "SELECT adbin FROM pg_catalog.pg_attrdef",
            "hardcoded-secret-token",
        ),
    ] {
        let rows = owner.execute(sql).unwrap();
        assert!(
            format!("{rows:?}").contains(needle),
            "the owner must still see {needle} via {sql}: {rows:?}"
        );
    }
}

/// ANALYZE force-feeds the statistics catalog, so leaving it ungated made the
/// leak on-demand instead of dependent on the owner analyzing.
#[test]
fn analyze_requires_ownership() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("ANALYZE secrets")
        .expect_err("analyzing another tenant's table must be refused");
    assert_refused(error);
    // A bare ANALYZE is PostgreSQL-shaped: it succeeds but only touches the
    // tables the caller owns, so it must neither fail nor feed `secrets` into
    // the statistics catalog.
    mallory
        .execute("ANALYZE")
        .expect("bare ANALYZE analyzes only owned tables and succeeds");
    assert_hidden(
        &mut mallory,
        "SELECT most_common_vals::text FROM pg_stats WHERE tablename = 'secrets'",
        CARD,
    );
}

// --------------------------------------------------- ungated raw DDL ----

#[test]
fn extension_ddl_requires_superuser() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    for sql in ["CREATE EXTENSION myext", "DROP EXTENSION myext"] {
        match mallory.execute(sql) {
            Ok(rows) => panic!("{sql} must be refused, returned {rows:?}"),
            Err(error) => assert_refused(error),
        }
    }
}

#[test]
fn trim_audit_history_requires_superuser() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("TRIM AUDIT HISTORY")
        .expect_err("tampering with the audit trail must be refused");
    assert_refused(error);
}

#[test]
fn vacuum_requires_superuser() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("VACUUM")
        .expect_err("a store-wide vacuum must be refused");
    assert_refused(error);
}

#[test]
fn alter_table_set_schema_requires_ownership() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("CREATE SCHEMA elsewhere").unwrap();
    }
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("ALTER TABLE secrets SET SCHEMA elsewhere")
        .expect_err("relocating another tenant's table must be refused");
    assert_refused(error);
}

#[test]
fn create_materialized_view_requires_select_on_its_sources() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    let error = mallory
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id, card FROM secrets")
        .expect_err("a materialized view reads its sources and must require SELECT");
    assert_refused(error);
}

/// A plain view still stores only its definition and is gated when read, so
/// the materialized-view check must not have swept it up.
#[test]
fn a_plain_view_over_an_unreadable_table_still_creates() {
    let (_directory, mut db) = seeded();
    let mut mallory = as_mallory(&mut db);
    mallory
        .execute("CREATE VIEW v AS SELECT id, card FROM secrets")
        .expect("plain views are gated at read time, not creation time");
    assert_hidden(&mut mallory, "SELECT * FROM v", CARD);
}

/// Row counts are a real signal about another tenant's data — size, growth,
/// whether a probe landed. PostgreSQL exposes them broadly, so the rows stay
/// visible for introspection and only the statistic is zeroed.
#[test]
fn per_relation_row_counts_are_redacted_for_roles_without_access() {
    let (_directory, mut db) = seeded();
    {
        // `seeded()` already inserted and analyzed before locking the table
        // down, so the statistics are populated and non-zero here.
        let mut owner = SqlSession::new(&mut db);
        let rows = owner
            .execute("SELECT reltuples FROM pg_catalog.pg_class WHERE relname = 'secrets'")
            .unwrap();
        assert!(
            !format!("{:?}", rows.rows).contains("Float(0.0)"),
            "precondition: the statistic must be non-zero before redaction"
        );
    }
    let mut mallory = as_mallory(&mut db);
    let rows = mallory
        .execute("SELECT relname, reltuples FROM pg_catalog.pg_class WHERE relname = 'secrets'")
        .unwrap();
    assert!(
        !rows.rows.is_empty(),
        "the row must remain visible for introspection"
    );
    for row in &rows.rows {
        let rendered = format!("{:?}", row[1]);
        assert!(
            rendered.contains("0.0") || rendered.contains("Int(0)"),
            "reltuples must be redacted, got {rendered}"
        );
    }
    let stats = mallory
        .execute("SELECT n_live_tup FROM pg_catalog.pg_stat_user_tables WHERE relname = 'secrets'")
        .unwrap();
    for row in &stats.rows {
        assert_eq!(
            row[0],
            bicdb_sql::SqlValue::Int(0),
            "n_live_tup must be redacted"
        );
    }
}

/// The owner's own counts must survive the redaction.
#[test]
fn the_owner_still_sees_its_own_row_counts() {
    let (_directory, mut db) = seeded();
    let mut owner = SqlSession::new(&mut db);
    owner.execute("ANALYZE secrets").unwrap();
    let rows = owner
        .execute("SELECT reltuples FROM pg_catalog.pg_class WHERE relname = 'secrets'")
        .unwrap();
    assert!(
        !format!("{:?}", rows.rows).contains("Float(0.0)"),
        "the owner must still see real statistics: {:?}",
        rows.rows
    );
}
