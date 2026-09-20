//! SQL full-text search against a `server_paged` database whose postings are
//! durable: after reopen the collection is in registry mode (zero resident
//! rows), the GIN index loads from the page store's reserved keyspace instead
//! of a corpus rebuild, and `@@` queries answer through `FullTextIndexScan`.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

const ROWS: usize = 300;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn seeded() -> TempDir {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE articles (id TEXT PRIMARY KEY, title TEXT)")
            .unwrap();
        sql.execute(
            "CREATE INDEX idx_articles_fts ON articles \
             USING GIN (to_tsvector('english', COALESCE(title, '')))",
        )
        .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(100) {
            let values = chunk
                .iter()
                .map(|index| {
                    let topic = match index % 3 {
                        0 => "ashwagandha reduces anxiety",
                        1 => "valerian improves sleep quality",
                        _ => "yoga lowers blood pressure",
                    };
                    format!("('k{index:05}', 'Study {index}: {topic}')")
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO articles VALUES {values}"))
                .unwrap();
        }
    }
    db.close().unwrap();
    dir
}

#[test]
fn reopened_paged_table_answers_tsquery_through_the_loaded_index() {
    let dir = seeded();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();

    // Registry mode: the FTS index no longer forces O(n) residency.
    let resident = db
        .residency_report()
        .unwrap()
        .collections
        .iter()
        .find(|entry| entry.name == "articles")
        .map(|entry| entry.record_count)
        .unwrap_or(0);
    assert_eq!(
        resident, 0,
        "an FTS-indexed paged table must reopen with zero resident rows"
    );

    let mut sql = SqlSession::new(&mut db);
    let explain = sql
        .execute(
            "EXPLAIN SELECT id FROM articles \
             WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'ashwagandha')",
        )
        .unwrap();
    assert!(
        explain.rows.iter().any(|row| row[0]
            .to_cell()
            .contains("FullTextIndexScan idx_articles_fts")),
        "planner did not choose the loaded FTS index: {:?}",
        explain.rows
    );

    let rows = sql
        .execute(
            "SELECT count(*) FROM articles \
             WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'ashwagandha & anxieti:*')",
        )
        .unwrap();
    assert_eq!(rows.rows, vec![vec![SqlValue::Int(ROWS as i64 / 3)]]);

    // Writes against the reopened lazy table keep the postings in sync.
    sql.execute("UPDATE articles SET title = 'Study 0: chamomile for calm' WHERE id = 'k00000'")
        .unwrap();
    let rows = sql
        .execute(
            "SELECT id FROM articles \
             WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'chamomile') \
             ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::String("k00000".to_string())]]
    );
    let rows = sql
        .execute(
            "SELECT count(*) FROM articles \
             WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'ashwagandha')",
        )
        .unwrap();
    assert_eq!(rows.rows, vec![vec![SqlValue::Int(ROWS as i64 / 3 - 1)]]);
}
