use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn paged_fts_hydrates_only_the_requested_page_and_rechecks_all_predicates() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE mail (id text PRIMARY KEY, account text, subject text);
        INSERT INTO mail SELECT 'k' || lpad(g::text,5,'0'), 'alice', 'red blue' FROM generate_series(1,3000) g;
        INSERT INTO mail VALUES ('k01001a','bob','red blue'),('k01001b','alice','red green blue');
        CREATE INDEX mail_fts ON mail USING GIN(to_tsvector('simple',COALESCE(subject,'')))").unwrap();
    let q="SELECT id FROM mail WHERE account='alice' AND id>'k01000' AND to_tsvector('simple',COALESCE(subject,'')) @@ to_tsquery('simple','red <-> blue') ORDER BY id LIMIT 3";
    sql.execute("BEGIN").unwrap();
    let expected = vec![
        vec![SqlValue::String("k01001".into())],
        vec![SqlValue::String("k01002".into())],
        vec![SqlValue::String("k01003".into())],
    ];
    assert_eq!(sql.execute(q).unwrap().rows, expected);
    let plan = sql
        .execute(&format!("EXPLAIN ANALYZE {q}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("FullTextIndexScan mail_fts"), "{plan}");
    assert!(plan.contains("ActualRowsFetched 5"), "{plan}");
    let desc = q.replace("ORDER BY id LIMIT 3", "ORDER BY id DESC LIMIT 3");
    assert_eq!(
        sql.execute(&desc).unwrap().rows[0][0],
        SqlValue::String("k03000".into())
    );
    sql.execute("INSERT INTO mail VALUES ('k01000a','alice','red blue')")
        .unwrap();
    assert_eq!(
        sql.execute(q).unwrap().rows[0][0],
        SqlValue::String("k01000a".into())
    );
    sql.execute("ROLLBACK").unwrap();
    sql.execute("DROP INDEX mail_fts").unwrap();
    assert_eq!(sql.execute(q).unwrap().rows, expected);
}
