//! Exercise the actual transactional row executor, rather than EXPLAIN's planner.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn transactional_fts_candidates_preserve_tenant_phrase_order_and_pending_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE mail (id text PRIMARY KEY, account text, subject text);
        INSERT INTO mail SELECT 'noise-' || g, 'alice', 'unrelated' FROM generate_series(1, 1000) g;
        INSERT INTO mail VALUES ('a','alice','red blue'),('b','bob','red blue'),('c','alice','red green blue'),('d','alice','red blue');
        CREATE INDEX mail_fts ON mail USING GIN(to_tsvector('simple',COALESCE(subject,'')))").unwrap();
    let query = "SELECT id FROM mail WHERE account='alice' AND id>'' AND to_tsvector('simple',COALESCE(subject,'')) @@ to_tsquery('simple','red <-> blue') ORDER BY id DESC LIMIT 1";
    sql.execute("BEGIN").unwrap();
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("d".into())]]
    );
    let plan = sql
        .execute(&format!("EXPLAIN ANALYZE {query}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("FullTextIndexScan mail_fts"), "{plan}");
    assert!(plan.contains("ActualRowsFetched 4"), "{plan}");
    sql.execute("INSERT INTO mail VALUES ('z','alice','red blue')")
        .unwrap();
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("z".into())]]
    );
    sql.execute("UPDATE mail SET subject='unrelated' WHERE id='z'")
        .unwrap();
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("d".into())]]
    );
    sql.execute("ROLLBACK").unwrap();
    sql.execute("DROP INDEX mail_fts").unwrap();
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("d".into())]]
    );
}

#[test]
fn transactional_fts_candidates_recheck_row_security_and_hide_prefilter_counts() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE mail_reader NOSUPERUSER NOBYPASSRLS;
        CREATE TABLE mail (id text PRIMARY KEY, account text, subject text);
        INSERT INTO mail VALUES ('a','alice','red blue'),('b','bob','red blue'),('c','alice','red green blue');
        CREATE INDEX mail_fts ON mail USING GIN(to_tsvector('simple',COALESCE(subject,'')));
        ALTER TABLE mail ENABLE ROW LEVEL SECURITY;
        CREATE POLICY own_mail ON mail FOR SELECT TO mail_reader USING (account='alice');
        GRANT SELECT ON mail TO mail_reader;
        SET ROLE mail_reader;
        BEGIN").unwrap();
    let query = "SELECT id FROM mail WHERE to_tsvector('simple',COALESCE(subject,'')) @@ to_tsquery('simple','red <-> blue') ORDER BY id";
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("a".into())]]
    );
    let plan = sql
        .execute(&format!("EXPLAIN ANALYZE {query}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("FullTextIndexScan mail_fts"), "{plan}");
    assert!(!plan.contains("ActualRowsFetched"), "{plan}");
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn transactional_fts_budget_failure_does_not_fall_back_to_full_scan() {
    use bicdb_core::{DbConfig, FtsQueryLimits, StorageMode};
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE mail (id text PRIMARY KEY, subject text);
            INSERT INTO mail VALUES ('a','red blue'),('b','red blue');
            CREATE INDEX mail_fts ON mail USING GIN(to_tsvector('simple',COALESCE(subject,'')))",
        )
        .unwrap();
    }
    let mut sql = SqlSession::new(&mut db).with_fts_limits(FtsQueryLimits {
        max_postings: Some(0),
        ..FtsQueryLimits::UNLIMITED
    });
    sql.execute("BEGIN").unwrap();
    let error = sql.execute("SELECT id FROM mail WHERE to_tsvector('simple',COALESCE(subject,'')) @@ to_tsquery('simple','red') ORDER BY id").unwrap_err().to_string();
    assert!(error.contains("query_budget_exceeded"), "{error}");
}

#[test]
fn fixed_snapshot_fts_reads_old_terms_after_another_session_updates_the_index() {
    use bicdb_core::{DbConfig, StorageMode};
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE mail (id text PRIMARY KEY, subject text); INSERT INTO mail VALUES ('a','red blue'); CREATE INDEX mail_fts ON mail USING GIN(to_tsvector('simple',COALESCE(subject,'')))").unwrap();
    let query = "SELECT id FROM mail WHERE to_tsvector('simple',COALESCE(subject,'')) @@ to_tsquery('simple','red') ORDER BY id";
    let mut reader = SqlSession::new_shared(&db);
    let mut writer = SqlSession::new_shared(&db);
    reader
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .unwrap();
    let before = reader.execute(query).unwrap().rows;
    assert_eq!(before, vec![vec![SqlValue::String("a".into())]]);
    writer
        .execute("UPDATE mail SET subject='green' WHERE id='a'")
        .unwrap();
    assert_eq!(reader.execute(query).unwrap().rows, before);
    reader.execute("COMMIT").unwrap();
    assert!(reader.execute(query).unwrap().rows.is_empty());
}
