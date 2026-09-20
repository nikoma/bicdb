//! Streaming full-scan projection (Phase 5, first slice).
//!
//! A full scan with no ORDER BY, no aggregate and no DISTINCT can emit each row
//! and forget it. These tests pin what a batched streaming cursor is most likely
//! to get wrong and what the ordinary suite cannot reach, because its tables are
//! far smaller than one 1024-row batch:
//!
//! - results spanning the batch boundary, which exercise resume-past-last-key;
//! - `OFFSET` landing mid-batch, skipping rows already streamed past;
//! - `LIMIT` beyond the end of the table;
//! - dropped or duplicated rows, which a count alone cannot distinguish.
//!
//! **These queries deliberately carry no `ORDER BY`.** That is not an oversight:
//! `try_streaming_projection` refuses any query with an ORDER BY (sorting is not
//! one pass over an unordered input), so an `ORDER BY` here would silently test
//! the materializing path instead — a test that passes while exercising nothing
//! it claims to. Because ordering is then unspecified, cross-mode comparisons
//! sort first and assert on multisets.
//!
//! `embedded_memory` is the oracle throughout rather than hardcoded
//! expectations: a hardcoded expectation can drift with both paths at once,
//! while a cross-mode comparison catches exactly the divergence that matters.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

/// Enough rows to cross the streaming batch size (1024) several times.
const ROWS: usize = 3_500;

fn seeded(mode: StorageMode) -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap_or_else(|error| panic!("open in {mode} failed: {error}"));
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE t (id TEXT PRIMARY KEY, n INT, pad TEXT)")
            .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
            let values = chunk
                .iter()
                .map(|index| format!("('k{index:05}', {}, 'p{index}')", index % 97))
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO t VALUES {values}"))
                .unwrap();
        }
    }
    // Reopen: the streaming path requires a lazy paged collection with no
    // chains from this session, which is what a fresh open produces.
    db.close().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap();
    (dir, db)
}

fn run(mode: StorageMode, sql: &str) -> Vec<Vec<SqlValue>> {
    let (_dir, mut db) = seeded(mode.clone());
    SqlSession::new(&mut db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("[{mode}] `{sql}` failed: {error}"))
        .rows
}

fn sorted(mut rows: Vec<Vec<SqlValue>>) -> Vec<String> {
    let mut out: Vec<String> = rows.drain(..).map(|row| format!("{row:?}")).collect();
    out.sort();
    out
}

/// Assert both modes return the same multiset of rows. The streaming path must
/// be indistinguishable from the materializing one.
fn assert_modes_agree(sql: &str) -> Vec<Vec<SqlValue>> {
    let memory = run(StorageMode::EmbeddedMemory, sql);
    let paged = run(StorageMode::ServerPaged, sql);
    assert_eq!(
        sorted(memory.clone()),
        sorted(paged.clone()),
        "embedded_memory and server_paged disagreed on: {sql}"
    );
    paged
}

/// The ids a query returned, as a de-duplicated sorted set plus the raw count —
/// enough to tell a dropped row from a duplicated one.
fn ids_and_count(rows: &[Vec<SqlValue>]) -> (Vec<String>, usize) {
    let mut ids: Vec<String> = rows
        .iter()
        .map(|row| match &row[0] {
            SqlValue::String(id) => id.clone(),
            other => panic!("unexpected id value {other:?}"),
        })
        .collect();
    let total = ids.len();
    ids.sort();
    ids.dedup();
    (ids, total)
}

#[test]
fn a_limit_inside_the_first_batch_agrees() {
    let rows = assert_modes_agree("SELECT id FROM t LIMIT 5");
    assert_eq!(rows.len(), 5);
}

#[test]
fn a_limit_spanning_batches_agrees() {
    // 2000 > 1024, so the result spans batches and exercises the
    // resume-past-last-key step.
    let rows = run(StorageMode::ServerPaged, "SELECT id FROM t LIMIT 2000");
    let (ids, total) = ids_and_count(&rows);
    assert_eq!(total, 2000, "wrong row count across a batch boundary");
    assert_eq!(ids.len(), total, "duplicate rows across a batch boundary");
}

#[test]
fn an_offset_across_a_batch_boundary_returns_the_right_window() {
    // The offset lands mid-second-batch. Without ORDER BY the *which* rows is
    // unspecified, so this asserts the window's size and distinctness, and that
    // both modes agree on the multiset.
    let rows = assert_modes_agree("SELECT id FROM t LIMIT 10 OFFSET 1020");
    let (ids, total) = ids_and_count(&rows);
    assert_eq!(total, 10);
    assert_eq!(ids.len(), 10, "offset window contained duplicates");
}

#[test]
fn a_limit_larger_than_the_table_returns_every_row() {
    let rows = run(
        StorageMode::ServerPaged,
        &format!("SELECT id FROM t LIMIT {}", ROWS + 500),
    );
    let (ids, total) = ids_and_count(&rows);
    assert_eq!(total, ROWS, "a LIMIT past the end changed the row count");
    assert_eq!(ids.len(), ROWS);
}

#[test]
fn an_unlimited_streamed_scan_returns_every_row_exactly_once() {
    // The whole-table case: every row present, none twice. This is the
    // assertion that would fail if resume-past-last-key were off by one in
    // either direction.
    let rows = run(StorageMode::ServerPaged, "SELECT id FROM t");
    let (ids, total) = ids_and_count(&rows);
    assert_eq!(total, ROWS, "streamed scan lost or duplicated rows");
    assert_eq!(ids.len(), ROWS, "streamed scan returned duplicate ids");
    // And the ids are exactly the ones inserted.
    let expected: Vec<String> = (0..ROWS).map(|index| format!("k{index:05}")).collect();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    assert_eq!(ids, expected_sorted);
}

#[test]
fn a_multi_column_projection_streams_identically() {
    let rows = assert_modes_agree("SELECT id, n, pad FROM t LIMIT 1500");
    assert_eq!(rows.len(), 1500);
}

#[test]
fn ordered_and_filtered_queries_still_agree_while_they_materialize() {
    // ORDER BY and a row-evaluator WHERE both take the materializing path
    // today (sorting is not one-pass; a typed predicate routes through
    // `execute_row_query`). Pinned here so the streaming work cannot quietly
    // change what they return.
    assert_modes_agree("SELECT id FROM t ORDER BY id LIMIT 20");
    assert_modes_agree("SELECT id, n FROM t WHERE n = 5 ORDER BY id");
}

#[test]
fn aggregates_over_a_large_paged_table_agree() {
    // Aggregates deliberately do NOT stream yet; this pins that they still
    // return the right answer while they materialize.
    assert_modes_agree("SELECT COUNT(*) FROM t");
    assert_modes_agree("SELECT SUM(n), MIN(n), MAX(n) FROM t");
}
