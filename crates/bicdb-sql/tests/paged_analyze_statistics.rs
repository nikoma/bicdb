//! ANALYZE on a lazily paged table after a reopen must read the page store.
//! It used to sample only resident rows, of which there are none after a
//! reopen: it recorded an empty table, the planner priced a full scan at zero,
//! and every indexed query on that table became a full scan.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DiagnosticsOptions};
use bicdb_sql::SqlSession;

const ROWS: usize = 20_000;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn reopen(db: BicDb, root: &std::path::Path) -> BicDb {
    db.close().unwrap();
    BicDb::open_with_config(root, config()).unwrap()
}

/// The access path and row count of `SELECT id FROM t WHERE a = 3`.
fn indexed_lookup(db: &mut BicDb) -> (Vec<String>, usize) {
    let mut session = SqlSession::new(db);
    let (result, decisions) = with_plan_diagnostics(DiagnosticsOptions::default(), || {
        session.execute("SELECT id FROM t WHERE a = 3")
    });
    let rows = result.unwrap().rows.len();
    let paths = decisions
        .iter()
        .map(|d| d.candidates[d.chosen].signature.label())
        .collect();
    (paths, rows)
}

#[test]
fn analyze_after_reopen_counts_paged_rows_and_keeps_index_plans() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    SqlSession::new(&mut db)
        .execute(&format!(
            "CREATE TABLE t (id int4 PRIMARY KEY, a int4, b int4);
             INSERT INTO t SELECT g, g % 100, g % 7 FROM generate_series(1, {ROWS}) g;
             CREATE INDEX t_a ON t (a);"
        ))
        .unwrap();
    let mut db = reopen(db, root.path());
    assert!(
        db.record_batch_streaming_possible("t"),
        "fixture must exercise the lazy paged path"
    );

    SqlSession::new(&mut db).execute("ANALYZE t").unwrap();
    let check = |db: &BicDb, when: &str| {
        let stats = db.table_statistics("t").expect("statistics");
        assert_eq!(stats.row_count, ROWS, "{when}: row_count");
        let a = &stats.columns["a"];
        assert_eq!(a.row_count, ROWS, "{when}: column row_count");
        assert!(
            (90..=110).contains(&a.distinct_count),
            "{when}: distinct(a) = {}",
            a.distinct_count
        );
    };
    check(&db, "after ANALYZE");
    let (paths, rows) = indexed_lookup(&mut db);
    assert_eq!(rows, ROWS / 100);
    assert!(
        paths.iter().all(|p| p.starts_with("IndexLookup(t_a")),
        "after ANALYZE the lookup must use t_a, got {paths:?}"
    );

    let mut db = reopen(db, root.path());
    check(&db, "after ANALYZE and reopen");
    let (paths, rows) = indexed_lookup(&mut db);
    assert_eq!(rows, ROWS / 100);
    assert!(
        paths.iter().all(|p| p.starts_with("IndexLookup(t_a")),
        "after reopen the lookup must use t_a, got {paths:?}"
    );
}
