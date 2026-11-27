//! Index-only projections: `SELECT <index columns> ... ORDER BY <them>` is
//! answered from B-tree entries without reading rows. Each case compares the
//! rows (in order), column names and EXPLAIN with the row path, which a
//! computed expression forces, and checks the route ran exactly when EXPLAIN
//! says Index Only Scan.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DiagnosticsOptions};
use bicdb_sql::{SqlResult, SqlSession};

const SETUP: &str = "CREATE TABLE v (id int4 PRIMARY KEY, k int4, j int4, payload text);
     INSERT INTO v SELECT g, CASE WHEN g % 17 = 0 THEN NULL ELSE g % 10 END,
       CASE WHEN g % 23 = 0 THEN NULL ELSE (g * 7) % 13 END, 'p' || g
       FROM generate_series(1, 2000) g;
     CREATE INDEX v_kj ON v (k, j);";

/// (index-only route ran, EXPLAIN shows Index Only Scan, result).
fn run(session: &mut SqlSession<'_>, sql: &str) -> (bool, bool, SqlResult) {
    let explained = session
        .execute(&format!("EXPLAIN (COSTS OFF) {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN {sql}: {error}"))
        .rows
        .into_iter()
        .any(|row| row[0].to_cell().contains("Index Only Scan"));
    let (result, decisions) =
        with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
    let result = result.unwrap_or_else(|error| panic!("{sql}: {error}"));
    let ran = decisions
        .iter()
        .any(|decision| decision.candidates[decision.chosen].signature.kind == "IndexOnlyScan");
    (ran, explained, result)
}

fn rows(result: &SqlResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

/// (SQL, its row-path twin, whether the index-only route should answer).
const CASES: &[(&str, &str, bool)] = &[
    (
        "SELECT k, j FROM v WHERE k = 3 ORDER BY j, k",
        "SELECT k + 0 AS k, j + 0 AS j FROM v WHERE k + 0 = 3 ORDER BY j, k",
        true,
    ),
    (
        "SELECT k, j FROM v WHERE k = 4 ORDER BY j DESC, k LIMIT 7",
        "SELECT k + 0 AS k, j + 0 AS j FROM v WHERE k + 0 = 4 ORDER BY j DESC, k LIMIT 7",
        true,
    ),
    (
        "SELECT k FROM v ORDER BY k NULLS FIRST",
        "SELECT k + 0 AS k FROM v ORDER BY k NULLS FIRST",
        true,
    ),
    (
        "SELECT k AS kk, j FROM v ORDER BY kk DESC, j",
        "SELECT k + 0 AS kk, j + 0 AS j FROM v ORDER BY kk DESC, j",
        true,
    ),
    (
        "SELECT j FROM v WHERE k = 5 ORDER BY j OFFSET 3",
        "SELECT j + 0 AS j FROM v WHERE k + 0 = 5 ORDER BY j OFFSET 3",
        true,
    ),
    // The bounded ordered index path serves `prefix ORDER BY next LIMIT n`
    // (NULL-aware), and the index-only route defers to it.
    (
        "SELECT j FROM v WHERE k = 4 ORDER BY j DESC LIMIT 7",
        "SELECT j + 0 AS j FROM v WHERE k + 0 = 4 ORDER BY j DESC LIMIT 7",
        false,
    ),
    // Not covered by ORDER BY, not index columns, or no ORDER BY: the row
    // path answers, in its own order.
    (
        "SELECT k, j FROM v WHERE k = 3 ORDER BY j",
        "SELECT k + 0 AS k, j + 0 AS j FROM v WHERE k + 0 = 3 ORDER BY j",
        false,
    ),
    (
        "SELECT j FROM v WHERE k = 3",
        "SELECT j + 0 AS j FROM v WHERE k + 0 = 3",
        false,
    ),
    (
        "SELECT j, payload FROM v WHERE k = 3 ORDER BY j, payload",
        "SELECT j + 0 AS j, payload FROM v WHERE k + 0 = 3 ORDER BY j, payload",
        false,
    ),
];

fn check_cases(session: &mut SqlSession<'_>, condition: &str, expect_route: bool) -> Vec<String> {
    let mut failures = Vec::new();
    for (sql, twin, eligible) in CASES {
        let (ran, explained, result) = run(session, sql);
        let (_, _, expected) = run(session, twin);
        let want = *eligible && expect_route;
        if ran != want || explained != ran {
            failures.push(format!(
                "[{condition}] {sql}: route ran {ran}, EXPLAIN index-only {explained}, expected {want}"
            ));
        }
        if result.columns != expected.columns || rows(&result) != rows(&expected) {
            failures.push(format!(
                "[{condition}] {sql}: {:?} {:?}\n    row path {:?} {:?}",
                result.columns,
                rows(&result),
                expected.columns,
                rows(&expected)
            ));
        }
    }
    failures
}

#[test]
fn index_only_projections_match_the_row_path_in_order() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db).execute(SETUP).unwrap();
    let db = db;
    let mut reader = SqlSession::new_shared(&db);
    let mut writer = SqlSession::new_shared(&db);
    let mut failures = check_cases(&mut reader, "plain", true);

    // Key-preserving and key-moving updates, deletes.
    writer
        .execute("UPDATE v SET payload = 'q' WHERE k = 3; UPDATE v SET k = 4, j = NULL WHERE id IN (3, 13); DELETE FROM v WHERE id IN (23, 33)")
        .unwrap();
    failures.extend(check_cases(&mut reader, "after updates", true));

    // A delete committed by another session between two read-committed
    // statements of an open transaction.
    reader.execute("BEGIN").unwrap();
    failures.extend(check_cases(&mut reader, "read committed, before", true));
    writer
        .execute("DELETE FROM v WHERE k = 3 AND j = 2")
        .unwrap();
    failures.extend(check_cases(&mut reader, "read committed, after", true));
    reader.execute("COMMIT").unwrap();

    // A repeatable-read snapshot older than the committed index: declines.
    reader
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    let (_, _, before) = run(&mut reader, "SELECT j FROM v WHERE k = 5 ORDER BY j");
    writer.execute("DELETE FROM v WHERE k = 5").unwrap();
    failures.extend(check_cases(&mut reader, "repeatable read", false));
    let (ran, _, after) = run(&mut reader, "SELECT j FROM v WHERE k = 5 ORDER BY j");
    if ran || rows(&before) != rows(&after) {
        failures.push(format!(
            "[repeatable read] snapshot not kept: ran {ran}, {:?} vs {:?}",
            rows(&before),
            rows(&after)
        ));
    }
    // Versions the old snapshot keeps alive are not seen by a fresh statement.
    failures.extend(check_cases(&mut writer, "dead versions retained", true));
    reader.execute("COMMIT").unwrap();

    // The session's own uncommitted writes: declines.
    writer.execute("BEGIN").unwrap();
    writer.execute("UPDATE v SET k = 3 WHERE id = 40").unwrap();
    failures.extend(check_cases(&mut writer, "pending writes", false));
    writer.execute("ROLLBACK").unwrap();

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn read_through_paged_indexes_fall_back_to_the_row_path() {
    // After a reopen a server-paged collection's B-tree is read through
    // from pages; the entry listing declines and the row path answers.
    let root = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    {
        let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
        SqlSession::new(&mut db).execute(SETUP).unwrap();
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let mut failures = Vec::new();
    for (sql, twin, _) in CASES {
        let (ran, explained, result) = run(&mut session, sql);
        let (_, _, expected) = run(&mut session, twin);
        if ran || explained {
            failures.push(format!(
                "[paged] {sql}: a read-through index must decline (ran {ran}, EXPLAIN {explained})"
            ));
        }
        if rows(&result) != rows(&expected) {
            failures.push(format!(
                "[paged] {sql}: {:?} vs {:?}",
                rows(&result),
                rows(&expected)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
