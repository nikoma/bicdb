//! Streaming simple aggregates: COUNT/SUM/MIN/MAX/AVG/BOOL_AND/BOOL_OR with an
//! optional WHERE fold over the scan in batches instead of materializing the
//! table. Correctness oracle: the same statement inside an open transaction —
//! a transaction forces the streaming path to decline, so both answers come
//! from the same engine with only the execution strategy differing. AVG folds
//! through a shared incremental accumulator with the materializing path, so the
//! two strategies cannot diverge.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const ROWS: usize = 3_000;

fn seeded(mode: StorageMode) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE m (id TEXT PRIMARY KEY, n INT, f DOUBLE PRECISION, t TEXT, b BOOLEAN)",
        )
        .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
            let values = chunk
                .iter()
                .map(|index| {
                    // Every third row has NULLs in n/f/t/b; bools alternate.
                    if index % 3 == 0 {
                        format!("('k{index:05}', NULL, NULL, NULL, NULL)")
                    } else {
                        format!(
                            "('k{index:05}', {}, {}.5, 'w{:03}', {})",
                            index % 100,
                            index % 7,
                            index % 250,
                            index % 2 == 0
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO m VALUES {values}"))
                .unwrap();
        }
    }
    // Reopen: streaming engages only on a lazy collection untouched this
    // session (a session that wrote has chains overriding the page store).
    db.close().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    (dir, db)
}

const QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM m WHERE n > 50",
    "SELECT COUNT(n) FROM m",
    "SELECT COUNT(n) FROM m WHERE id > 'k00100'",
    "SELECT SUM(n) FROM m",
    "SELECT SUM(n) FROM m WHERE n < 10",
    "SELECT SUM(f) FROM m WHERE b",
    "SELECT MIN(n), MAX(n) FROM m WHERE n BETWEEN 13 AND 77",
    "SELECT MIN(t), MAX(t) FROM m",
    "SELECT BOOL_AND(b), BOOL_OR(b) FROM m WHERE n = 4",
    "SELECT COUNT(*) AS total, SUM(n) AS s, MAX(t) AS top FROM m WHERE n % 2 = 0",
    // Empty match set: COUNT 0, everything else NULL.
    "SELECT COUNT(*), COUNT(n), SUM(n), MIN(n), MAX(t), BOOL_AND(b) FROM m WHERE n = -1",
    // AVG folds through the same streaming scan: integer and float columns,
    // with and without a predicate, NULL-heavy input, and the empty set.
    "SELECT AVG(n) FROM m",
    "SELECT AVG(n) FROM m WHERE n < 50",
    "SELECT AVG(f) FROM m",
    "SELECT AVG(f) FROM m WHERE b",
    "SELECT AVG(n) FROM m WHERE n = -1",
    "SELECT COUNT(*) AS c, AVG(n) AS mean, MAX(n) AS top FROM m WHERE n % 2 = 0",
    // LIMIT/OFFSET on the single aggregate row.
    "SELECT COUNT(*) FROM m OFFSET 1",
    "SELECT COUNT(*) FROM m LIMIT 1",
];

fn run_mode(mode: StorageMode) {
    let (_dir, mut db) = seeded(mode);
    let mut streamed_results = Vec::new();
    {
        let mut sql = SqlSession::new(&mut db);
        for query in QUERIES {
            let result = sql
                .execute(query)
                .unwrap_or_else(|error| panic!("`{query}` failed: {error}"));
            streamed_results.push((result.columns.clone(), result.rows.clone()));
        }
        // Oracle pass: inside a transaction the streaming path declines, so
        // these answers come from the materializing engine.
        sql.execute("BEGIN").unwrap();
        for (query, streamed) in QUERIES.iter().zip(&streamed_results) {
            let oracle = sql
                .execute(query)
                .unwrap_or_else(|error| panic!("oracle `{query}` failed: {error}"));
            assert_eq!(
                &(oracle.columns.clone(), oracle.rows.clone()),
                streamed,
                "streamed and materializing answers diverged for `{query}`"
            );
        }
        sql.execute("COMMIT").unwrap();
    }
}

#[test]
fn streamed_aggregates_match_the_materializing_oracle_embedded() {
    run_mode(StorageMode::EmbeddedMemory);
}

#[test]
fn streamed_aggregates_match_the_materializing_oracle_paged() {
    run_mode(StorageMode::ServerPaged);
}

#[test]
fn spot_values_are_the_expected_constants() {
    // Belt and braces: oracle equality alone would pass if BOTH paths broke
    // identically, so pin a few closed-form answers. Rows k00000, k00003, ...
    // (multiples of 3) are all-NULL; 2,000 rows carry values.
    let (_dir, mut db) = seeded(StorageMode::ServerPaged);
    let mut sql = SqlSession::new(&mut db);
    let result = sql.execute("SELECT COUNT(*), COUNT(n) FROM m").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(ROWS as i64), SqlValue::Int(2_000)]]
    );
    let result = sql.execute("SELECT MIN(n), MAX(n) FROM m").unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(0), SqlValue::Int(99)]]);
    let result = sql
        .execute("SELECT COUNT(*) FROM m WHERE n IS NULL")
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(1_000)]]);
    // Every row matching `n = 5` carries exactly 5, so the average is 5 no
    // matter how many rows match. Pins AVG's value, not just strategy parity.
    let result = sql.execute("SELECT AVG(n) FROM m WHERE n = 5").unwrap();
    assert_eq!(result.rows.len(), 1);
    let cell = result.rows[0][0].to_cell();
    let value: f64 = cell
        .parse()
        .unwrap_or_else(|_| panic!("AVG cell {cell:?} is not numeric"));
    assert!(
        (value - 5.0).abs() < 1e-9,
        "streamed AVG(n) WHERE n = 5 = {value}, expected 5"
    );
}
