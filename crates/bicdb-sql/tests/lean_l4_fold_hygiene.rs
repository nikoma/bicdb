//! L4: fold hygiene — the tail metric the auto-fold worker triggers on,
//! and the fold clearing it without changing query results.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn unfolded_tail_metric_drives_folds_and_queries_stay_exact() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    let seed: Vec<String> = (0..200)
        .map(|index| format!("('s{index:04}', 'ashwagandha anxiety trial {index}')"))
        .collect();
    sql.execute(&format!("INSERT INTO docs VALUES {}", seed.join(", ")))
        .unwrap();
    sql.execute(
        "CREATE INDEX idx_docs_fts ON docs USING GIN (to_tsvector('english', COALESCE(body, '')))",
    )
    .unwrap();
    // Fresh writes after the build create the unfolded tail.
    let tail: Vec<String> = (0..150)
        .map(|index| format!("('t{index:04}', 'ashwagandha valerian tail {index}')"))
        .collect();
    sql.execute(&format!("INSERT INTO docs VALUES {}", tail.join(", ")))
        .unwrap();

    let count_hits = |sql: &mut SqlSession| -> i64 {
        match sql
            .execute(
                "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
                 @@ to_tsquery('english', 'ashwagandha')",
            )
            .unwrap()
            .rows[0][0]
        {
            SqlValue::Int(count) => count,
            ref other => panic!("expected count, got {other:?}"),
        }
    };
    let hits_before = count_hits(&mut sql);
    assert_eq!(hits_before, 350);

    // The metric sees the tail; a small cap reports the over-threshold
    // signal (None) exactly as the worker consumes it.
    let entries = db
        .full_text_unfolded_entries_capped("idx_docs_fts", 1_000_000)
        .unwrap()
        .expect("under a huge cap the count is exact");
    assert!(entries > 0, "fresh writes must appear as unfolded entries");
    assert!(
        db.full_text_unfolded_entries_capped("idx_docs_fts", 10)
            .unwrap()
            .is_none(),
        "a tail beyond the cap reports the fold-now signal"
    );

    // Folding clears the tail; results are unchanged.
    let (terms, blocks) = db.compact_full_text_index("idx_docs_fts").unwrap();
    assert!(terms > 0 && blocks > 0);
    let entries_after = db
        .full_text_unfolded_entries_capped("idx_docs_fts", 1_000_000)
        .unwrap()
        .expect("post-fold count is exact");
    assert!(
        entries_after < entries / 10,
        "fold must clear the tail: {entries} -> {entries_after}"
    );
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(count_hits(&mut sql), 350, "fold must not change results");
}
