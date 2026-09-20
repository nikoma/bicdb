//! `SELECT COUNT(*)` answered from the row count instead of by reading rows.
//!
//! This fast path bypasses the entire execution engine, so its correctness
//! rests wholly on its guards: every condition that could make "live rows in
//! the collection" differ from what `COUNT(*)` should return has to send the
//! query back to the counting path.
//!
//! The tests therefore come in two halves:
//!
//! 1. **the count is right** — across empty tables, deletes, updates,
//!    re-inserts, and both storage modes;
//! 2. **every guard fires** — each shape that must NOT take the fast path is
//!    checked against a value the fast path would get wrong. A guard that
//!    silently stopped working would return the table's total row count, so
//!    each of these asserts a number that differs from that total.
//!
//! The second half is the point. A fast path whose guards rot returns
//! confidently wrong answers rather than failing, which is the worst failure
//! mode a database has.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

const ROWS: usize = 200;

fn open(dir: &TempDir, mode: &StorageMode) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap_or_else(|error| panic!("open in {mode} failed: {error}"))
}

fn seeded(mode: &StorageMode) -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir, mode);
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE t (id TEXT PRIMARY KEY, n INT, grp TEXT)")
            .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(50) {
            let values = chunk
                .iter()
                .map(|index| {
                    format!(
                        "('k{index:04}', {}, '{}')",
                        index % 10,
                        if index % 2 == 0 { "even" } else { "odd" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO t VALUES {values}"))
                .unwrap();
        }
    }
    (dir, db)
}

fn count(db: &mut BicDb, sql: &str) -> i64 {
    let rows = SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
        .rows;
    assert_eq!(rows.len(), 1, "`{sql}` returned {} rows", rows.len());
    match &rows[0][0] {
        SqlValue::Int(value) => *value,
        other => panic!("`{sql}` returned {other:?}, not an integer"),
    }
}

/// The count the ordinary execution path produces, used as the oracle.
/// `COUNT(*) WHERE TRUE` carries a selection, so the fast path declines it and
/// the engine counts rows the long way.
fn oracle(db: &mut BicDb) -> i64 {
    count(db, "SELECT COUNT(*) FROM t WHERE TRUE")
}

fn each_mode(mut test: impl FnMut(StorageMode)) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        test(mode);
    }
}

// ---------------------------------------------------------------------------
// The count is right
// ---------------------------------------------------------------------------

#[test]
fn counts_a_seeded_table_in_both_modes() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        assert_eq!(
            count(&mut db, "SELECT COUNT(*) FROM t"),
            ROWS as i64,
            "{mode}"
        );
        assert_eq!(oracle(&mut db), ROWS as i64, "{mode}: oracle disagreed");
    });
}

#[test]
fn counts_an_empty_table_in_both_modes() {
    each_mode(|mode| {
        let dir = TempDir::new().unwrap();
        let mut db = open(&dir, &mode);
        SqlSession::new(&mut db)
            .execute("CREATE TABLE t (id TEXT PRIMARY KEY, n INT, grp TEXT)")
            .unwrap();
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 0, "{mode}");
    });
}

#[test]
fn the_count_follows_deletes_updates_and_reinserts() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("DELETE FROM t WHERE n = 0").unwrap(); // 20 rows
            sql.execute("UPDATE t SET n = 99 WHERE n = 1").unwrap(); // count unchanged
            sql.execute("INSERT INTO t VALUES ('extra-1', 7, 'even')")
                .unwrap();
        }
        let expected = ROWS as i64 - 20 + 1;
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), expected, "{mode}");
        assert_eq!(oracle(&mut db), expected, "{mode}: oracle disagreed");
    });
}

#[test]
fn the_count_survives_a_reopen() {
    each_mode(|mode| {
        let (dir, mut db) = seeded(&mode);
        SqlSession::new(&mut db)
            .execute("DELETE FROM t WHERE n = 3")
            .unwrap();
        db.close().unwrap();
        let mut db = open(&dir, &mode);
        let expected = ROWS as i64 - 20;
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), expected, "{mode}");
        assert_eq!(oracle(&mut db), expected, "{mode}: oracle disagreed");
    });
}

#[test]
fn an_alias_does_not_change_the_count() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        assert_eq!(
            count(&mut db, "SELECT COUNT(*) AS n FROM t"),
            ROWS as i64,
            "{mode}"
        );
    });
}

// ---------------------------------------------------------------------------
// Every guard fires
//
// Each case asserts a value the fast path would get WRONG, so a guard that
// stopped firing fails the test instead of silently returning the row total.
// ---------------------------------------------------------------------------

#[test]
fn a_where_clause_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        // 20 of 200 rows, so the row total would be visibly wrong.
        assert_eq!(
            count(&mut db, "SELECT COUNT(*) FROM t WHERE n = 4"),
            20,
            "{mode}"
        );
    });
}

#[test]
fn a_group_by_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        let rows = SqlSession::new(&mut db)
            .execute("SELECT grp, COUNT(*) FROM t GROUP BY grp ORDER BY grp")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2, "{mode}: GROUP BY collapsed to one row");
        assert_eq!(rows[0][1], SqlValue::Int(100), "{mode}");
        assert_eq!(rows[1][1], SqlValue::Int(100), "{mode}");
    });
}

#[test]
fn a_having_clause_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        let rows = SqlSession::new(&mut db)
            .execute("SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) > 1000")
            .unwrap()
            .rows;
        assert!(rows.is_empty(), "{mode}: HAVING did not filter");
    });
}

#[test]
fn count_of_a_column_is_not_treated_as_count_star() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        // COUNT(col) skips NULLs and is a different aggregate entirely.
        SqlSession::new(&mut db)
            .execute("INSERT INTO t VALUES ('null-row', NULL, 'even')")
            .unwrap();
        assert_eq!(
            count(&mut db, "SELECT COUNT(*) FROM t"),
            ROWS as i64 + 1,
            "{mode}: COUNT(*) must include the NULL row"
        );
        assert_eq!(
            count(&mut db, "SELECT COUNT(n) FROM t"),
            ROWS as i64,
            "{mode}: COUNT(n) must skip the NULL"
        );
    });
}

#[test]
fn a_distinct_count_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        assert_eq!(
            count(&mut db, "SELECT COUNT(DISTINCT n) FROM t"),
            10,
            "{mode}: DISTINCT count was short-circuited"
        );
    });
}

#[test]
fn limit_and_offset_apply_to_the_aggregate_result() {
    // PostgreSQL applies LIMIT/OFFSET to the aggregate's own result row.
    // BicDB returned from `execute_aggregates` before `apply_limit` ran, so
    // both were silently ignored — a pre-existing bug this file found and the
    // same commit fixes. `OFFSET 1` past a one-row result must yield nothing.
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        let rows = SqlSession::new(&mut db)
            .execute("SELECT COUNT(*) FROM t OFFSET 1")
            .unwrap()
            .rows;
        assert!(
            rows.is_empty(),
            "{mode}: OFFSET past the result was ignored"
        );

        let rows = SqlSession::new(&mut db)
            .execute("SELECT COUNT(*) FROM t LIMIT 0")
            .unwrap()
            .rows;
        assert!(rows.is_empty(), "{mode}: LIMIT 0 was ignored");

        // And a limit that keeps the row still returns it, unchanged.
        let rows = SqlSession::new(&mut db)
            .execute("SELECT COUNT(*) FROM t LIMIT 1")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1, "{mode}: LIMIT 1 dropped the result row");
        assert_eq!(rows[0][0], SqlValue::Int(ROWS as i64), "{mode}");
    });
}

#[test]
fn pending_transaction_writes_are_counted() {
    // The fast path declines inside a transaction, because the row count does
    // not yet include the transaction's own buffered writes. If that guard
    // stopped firing this would report the pre-transaction total.
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("BEGIN").unwrap();
        sql.execute("INSERT INTO t VALUES ('tx-row', 1, 'even')")
            .unwrap();
        let rows = sql.execute("SELECT COUNT(*) FROM t").unwrap().rows;
        assert_eq!(
            rows[0][0],
            SqlValue::Int(ROWS as i64 + 1),
            "{mode}: a transaction's own insert was not counted"
        );
        // And a rollback must put the count back.
        sql.execute("ROLLBACK").unwrap();
        let rows = sql.execute("SELECT COUNT(*) FROM t").unwrap().rows;
        assert_eq!(
            rows[0][0],
            SqlValue::Int(ROWS as i64),
            "{mode}: the rolled-back insert still counted"
        );
    });
}

#[test]
fn a_join_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE u (id TEXT PRIMARY KEY, n INT)")
                .unwrap();
            sql.execute("INSERT INTO u VALUES ('a', 0), ('b', 1)")
                .unwrap();
        }
        // 200 rows joined against 2 matching values is not 200.
        let joined = count(&mut db, "SELECT COUNT(*) FROM t JOIN u ON t.n = u.n");
        assert_eq!(joined, 40, "{mode}: join was short-circuited");
    });
}

#[test]
fn a_view_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        SqlSession::new(&mut db)
            .execute("CREATE VIEW v AS SELECT * FROM t WHERE n = 5")
            .unwrap();
        assert_eq!(
            count(&mut db, "SELECT COUNT(*) FROM v"),
            20,
            "{mode}: a view's filter was short-circuited"
        );
    });
}

#[test]
fn a_cte_is_not_short_circuited() {
    each_mode(|mode| {
        let (_dir, mut db) = seeded(&mode);
        assert_eq!(
            count(
                &mut db,
                "WITH c AS (SELECT * FROM t WHERE n = 6) SELECT COUNT(*) FROM c"
            ),
            20,
            "{mode}: a CTE's filter was short-circuited"
        );
    });
}
