//! Streaming DISTINCT and GROUP BY (Phase 5c): spill through the external
//! sorter, dedup/fold adjacent keys during the merge. Oracle: the same
//! statement inside a transaction (streaming declines there). Row ORDER is
//! not part of either contract — both sides are compared as sorted sets.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const ROWS: usize = 4_000;

fn seeded() -> (tempfile::TempDir, BicDb) {
    // Tiny spill budget so every query below exercises real runs.
    std::env::set_var("BICDB_SORT_SPILL_ROWS", "64");
    let dir = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE g (id TEXT PRIMARY KEY, dept TEXT, n INT, b BOOLEAN)")
            .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
            let values = chunk
                .iter()
                .map(|index| {
                    if index % 13 == 0 {
                        format!("('k{index:05}', NULL, NULL, NULL)")
                    } else {
                        format!(
                            "('k{index:05}', 'd{:02}', {}, {})",
                            index % 23,
                            index % 500,
                            index % 3 == 0
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO g VALUES {values}"))
                .unwrap();
        }
    }
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    (dir, db)
}

fn sorted_cells(rows: &[Vec<SqlValue>]) -> Vec<Vec<String>> {
    let mut cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect();
    cells.sort();
    cells
}

const QUERIES: &[&str] = &[
    "SELECT DISTINCT dept FROM g",
    "SELECT DISTINCT dept, b FROM g",
    "SELECT DISTINCT dept FROM g WHERE n % 2 = 0",
    "SELECT dept, COUNT(*) FROM g GROUP BY dept",
    "SELECT dept, COUNT(n), SUM(n), MIN(n), MAX(n) FROM g GROUP BY dept",
    "SELECT dept, b, COUNT(*) FROM g GROUP BY dept, b",
    "SELECT dept, SUM(n) FROM g WHERE n % 3 = 1 GROUP BY dept",
    "SELECT dept, BOOL_AND(b), BOOL_OR(b) FROM g GROUP BY dept",
    "SELECT COUNT(*), dept FROM g GROUP BY dept",
    // AVG and HAVING decline the streamer; fallback must agree with itself.
    "SELECT dept, AVG(n) FROM g GROUP BY dept",
];

#[test]
fn streaming_grouping_matches_the_materializing_oracle() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let mut streamed = Vec::new();
    for query in QUERIES {
        let result = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("`{query}` failed: {error}"));
        streamed.push((result.columns.clone(), sorted_cells(&result.rows)));
    }
    sql.execute("BEGIN").unwrap();
    for (query, streamed) in QUERIES.iter().zip(&streamed) {
        let oracle = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("oracle `{query}` failed: {error}"));
        assert_eq!(
            &(oracle.columns.clone(), sorted_cells(&oracle.rows)),
            streamed,
            "streaming grouping diverged for `{query}`"
        );
    }
    sql.execute("COMMIT").unwrap();
}

#[test]
fn grouping_constants_are_exact() {
    // Closed-form pins so identical bugs on both paths cannot hide: 23 depts
    // + the NULL group; d00 has fewer members than the others because
    // multiples of 13 that are also ≡0 (mod 23) went NULL.
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);

    let result = sql
        .execute("SELECT COUNT(*) FROM (SELECT DISTINCT dept FROM g) AS d")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(24)]],
        "23 depts + NULL"
    );

    // LIMIT without ORDER BY has no contract on WHICH rows — only on how
    // many, and that they are genuinely distinct values from the column.
    let limited = sql.execute("SELECT DISTINCT b FROM g LIMIT 2").unwrap();
    assert_eq!(limited.rows.len(), 2);
    let full = sql.execute("SELECT DISTINCT b FROM g").unwrap();
    assert_eq!(full.rows.len(), 3, "NULL, true, false");
    for row in &limited.rows {
        assert!(
            full.rows.contains(row),
            "LIMIT emitted a non-distinct value"
        );
    }

    let result = sql
        .execute("SELECT dept, COUNT(*) FROM g GROUP BY dept")
        .unwrap();
    assert_eq!(result.rows.len(), 24);
    let null_group: i64 = result
        .rows
        .iter()
        .find(|row| matches!(row[0], SqlValue::Null))
        .map(|row| match row[1] {
            SqlValue::Int(count) => count,
            _ => panic!("count type"),
        })
        .expect("NULL keys must form ONE group");
    assert_eq!(null_group, (ROWS as i64 + 12) / 13);
}
