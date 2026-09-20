//! Regression: a non-streaming aggregate (string_agg/array_agg/json_agg) over
//! an unbounded scan is refused before it materializes the whole table, so a
//! single query cannot OOM the node. Own test binary so the env-gated cap
//! initializes at 2.
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
fn non_streaming_aggregate_over_large_scan_is_refused() {
    std::env::set_var("BICDB_MAX_AGGREGATE_ROWS", "2");
    let (_d, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        .unwrap();

    // string_agg over the full table (5 rows > cap 2) must be refused, not run.
    let err = s
        .execute("SELECT string_agg(v, ',') FROM t")
        .expect_err("string_agg over a scan beyond the cap must be refused");
    let msg = format!("{err}");
    assert!(
        msg.contains("non-streaming materialization limit"),
        "expected the aggregate scan-cap refusal, got: {msg}"
    );

    // array_agg is the same class.
    assert!(s.execute("SELECT array_agg(v) FROM t").is_err());

    // Streaming aggregates are unaffected — they never materialize the table.
    let count = s.execute("SELECT count(*) FROM t").unwrap();
    assert_eq!(format!("{:?}", count.rows[0][0]), "Int(5)");
    s.execute("SELECT sum(id) FROM t").unwrap();

    // avg without DISTINCT streams — allowed.
    s.execute("SELECT avg(id) FROM t").unwrap();
    // A DISTINCT aggregate holds O(rows) state — refused like the collectors.
    assert!(s.execute("SELECT count(DISTINCT v) FROM t").is_err());
    // A WHERE filter (any predicate) takes the query out of the unfiltered
    // whole-table shape the guard targets — allowed.
    s.execute("SELECT string_agg(v, ',') FROM t WHERE id = 1")
        .unwrap();
    s.execute("SELECT string_agg(v, ',') FROM t WHERE id > 0")
        .unwrap();
    // GROUP BY streams per group — allowed.
    s.execute("SELECT string_agg(v, ',') FROM t GROUP BY id")
        .unwrap();
}
