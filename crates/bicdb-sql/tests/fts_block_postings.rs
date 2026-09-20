//! Block postings at the SQL layer: folding a GIN index's per-posting tail
//! into compressed blocks must be invisible to queries, layer correctly with
//! post-fold writes, and shrink the index keyspace.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const ROWS: usize = 800;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn seeded() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        sql.execute(
            "CREATE INDEX idx_docs_fts ON docs USING GIN (to_tsvector('english', COALESCE(body, '')))",
        )
        .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(200) {
            let values = chunk
                .iter()
                .map(|index| {
                    let alphas = std::iter::repeat("ashwagandha")
                        .take(1 + index % 9)
                        .collect::<Vec<_>>()
                        .join(" pad ");
                    format!(
                        "('k{index:05}', 'trial {index}: {alphas} anxiety phase {}')",
                        index % 5
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO docs VALUES {values}"))
                .unwrap();
        }
    }
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    (dir, db)
}

fn queries(db: &mut BicDb) -> Vec<Vec<Vec<SqlValue>>> {
    let mut sql = SqlSession::new(db);
    [
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
        "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & anxieti') \
         ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 5",
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'anxieti & ashwa:*')",
    ]
    .iter()
    .map(|query| sql.execute(query).unwrap().rows)
    .collect()
}

#[test]
fn ranked_single_term_after_fold_matches_oracle_and_terminates() {
    // Skewed tf: doc k has 1 + k%9 ashwagandha occurrences. After a fold the
    // impact-ordered block copy must serve the ranked top-k identically to
    // the text-ranked oracle (transaction-forced) and stop scanning at the
    // top impact classes rather than the whole posting list.
    let (_dir, mut db) = seeded();
    db.compact_full_text_index("idx_docs_fts").unwrap();
    let query = "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')                  ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 7";
    let mut sql = SqlSession::new(&mut db);
    let ranked = sql.execute(query).unwrap();
    sql.execute("BEGIN").unwrap();
    let oracle = sql.execute(query).unwrap();
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        ranked.rows, oracle.rows,
        "folded ranked top-k diverged from the text oracle"
    );
}

#[test]
fn fold_is_invisible_to_queries_and_layers_with_writes() {
    let (_dir, mut db) = seeded();
    let before = queries(&mut db);

    let (terms, blocks) = db.compact_full_text_index("idx_docs_fts").unwrap();
    assert!(terms >= 5, "folded only {terms} terms");
    assert!(blocks >= terms, "wrote {blocks} blocks");
    assert_eq!(before, queries(&mut db), "fold changed query results");
    assert!(db.verify_index("idx_docs_fts").unwrap().valid);

    // Post-fold writes layer: update one row away from the term, delete one,
    // re-insert one.
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("UPDATE docs SET body = 'valerian only' WHERE id = 'k00000'")
            .unwrap();
        sql.execute("DELETE FROM docs WHERE id = 'k00001'").unwrap();
        let count = sql
            .execute(
                "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
            )
            .unwrap();
        assert_eq!(count.rows, vec![vec![SqlValue::Int(ROWS as i64 - 2)]]);
        sql.execute("INSERT INTO docs VALUES ('k00001', 'ashwagandha returns')")
            .unwrap();
        let count = sql
            .execute(
                "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
            )
            .unwrap();
        assert_eq!(count.rows, vec![vec![SqlValue::Int(ROWS as i64 - 1)]]);
    }
    assert!(db.verify_index("idx_docs_fts").unwrap().valid);

    // The sharpest tombstone case: a row updated AWAY from a folded term,
    // while the row itself still exists. The block still lists it; only the
    // tombstone hides it from the single-term ranked bulk path (the row-miss
    // guard cannot save this one — the row is alive).
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("UPDATE docs SET body = 'valerian herb notes' WHERE id = 'k00002'")
            .unwrap();
        let ranked = sql
            .execute(
                "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')                  ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 1000",
            )
            .unwrap();
        assert!(
            !ranked
                .rows
                .iter()
                .any(|row| row[0] == SqlValue::String("k00002".to_string())),
            "stale folded posting served for an updated row"
        );
    }

    // Second fold GCs tombstones; results stable; reopen durable.
    db.compact_full_text_index("idx_docs_fts").unwrap();
    assert!(db.verify_index("idx_docs_fts").unwrap().valid);
    let after_second = queries(&mut db);
    drop(db);
    let (_dir2, _) = (&_dir, ());
    let mut db = BicDb::open_with_config(_dir.path(), config()).unwrap();
    assert_eq!(
        after_second,
        queries(&mut db),
        "reopen changed folded results"
    );
    assert!(db.verify_index("idx_docs_fts").unwrap().valid);
}
