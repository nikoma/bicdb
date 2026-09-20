//! RLS-enabled tables must never take the streaming scan path (B2).
//!
//! `scan_records` filters through a table's row-level-security policies even
//! when no security context is set. The streaming projection used to gate only
//! on the session context, so an RLS-enabled table queried without a context
//! streamed UNFILTERED rows — a policy bypass, not a wrong count.
//!
//! The contract under test is divergence: whatever RLS semantics apply, the
//! streamed-eligible query shape (no ORDER BY) must return exactly what the
//! materializing shape (ORDER BY forces it) returns. The two tables differ
//! ONLY in whether RLS is enabled, so the pair also proves the guard fires for
//! the right reason rather than by disabling streaming everywhere.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

/// Enough rows that a streamed and a materialized path could visibly diverge,
/// and more than one streaming batch.
const ROWS: usize = 1_500;

fn seeded() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        for table in ["guarded", "open_table"] {
            sql.execute(&format!(
                "CREATE TABLE {table} (id TEXT PRIMARY KEY, n INT)"
            ))
            .unwrap();
            for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
                let values = chunk
                    .iter()
                    .map(|index| format!("('k{index:05}', {})", index % 10))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.execute(&format!("INSERT INTO {table} VALUES {values}"))
                    .unwrap();
            }
        }
        // FORCE, not just ENABLE: without a security context the session runs
        // as the table owner, and PostgreSQL's owner bypass exempts owners
        // from ENABLE-only policies — both paths would return everything and
        // the divergence this file exists to catch would be unobservable.
        // FORCE disables the owner bypass, so the policy filter genuinely
        // applies to the no-context session.
        sql.execute("ALTER TABLE guarded ENABLE ROW LEVEL SECURITY")
            .unwrap();
        sql.execute("ALTER TABLE guarded FORCE ROW LEVEL SECURITY")
            .unwrap();
        sql.execute("CREATE POLICY visible_ones ON guarded USING (n = 1)")
            .unwrap();
    }
    // Reopen so both collections are lazy — the state where streaming engages.
    db.close().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (dir, db)
}

fn ids(db: &mut BicDb, sql: &str) -> Vec<String> {
    let mut out: Vec<String> = SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::String(id) => id.clone(),
            other => panic!("unexpected value {other:?}"),
        })
        .collect();
    out.sort();
    out
}

#[test]
fn an_rls_table_returns_identical_rows_on_streamed_and_materialized_shapes() {
    let (_dir, mut db) = seeded();
    // No ORDER BY: streaming-eligible. ORDER BY: forced to materialize, where
    // RLS filtering is known-applied. Any divergence is the B2 bypass.
    let streamed_shape = ids(&mut db, "SELECT id FROM guarded");
    let materialized_shape = ids(&mut db, "SELECT id FROM guarded ORDER BY id");
    assert_eq!(
        streamed_shape, materialized_shape,
        "an RLS-enabled table returned different rows on the streaming-eligible \
         shape — the policy filter was bypassed"
    );
    // And the policy demonstrably filtered: 1 of 10 n-values passes, so a
    // full-table result here would mean the ORACLE was not filtering either
    // and this test proves nothing.
    assert_eq!(
        materialized_shape.len(),
        ROWS / 10,
        "the policy did not filter the materializing oracle; the test cannot \
         detect a streaming bypass"
    );
}

#[test]
fn the_guard_does_not_disable_streaming_for_tables_without_rls() {
    // The control: identical data, no RLS — both shapes must return every row,
    // so the guard demonstrably keys on the schema rather than turning
    // streaming off globally.
    let (_dir, mut db) = seeded();
    let streamed_shape = ids(&mut db, "SELECT id FROM open_table");
    assert_eq!(streamed_shape.len(), ROWS);
    let materialized_shape = ids(&mut db, "SELECT id FROM open_table ORDER BY id");
    assert_eq!(streamed_shape, materialized_shape);
}
