use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn authenticated_reader_uses_bounded_fts_and_missing_select_is_still_denied() {
    use bicdb_core::{AuthenticationStrength, SecurityContext};
    use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DiagnosticsOptions};
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    SqlSession::new(&mut db).execute("CREATE ROLE page_reader NOSUPERUSER; CREATE TABLE docs(id text PRIMARY KEY, body text);
        INSERT INTO docs SELECT 'k'||lpad(g::text,5,'0'),'shared term' FROM generate_series(1,1000) g;
        CREATE INDEX docs_fts ON docs USING GIN(to_tsvector('simple',COALESCE(body,'')));
        GRANT SELECT ON docs TO page_reader").unwrap();
    let context = SecurityContext::authenticated(
        "page_reader",
        "fixture",
        AuthenticationStrength::ScramSha256,
    );
    let gucs = std::collections::HashMap::from([
        ("role".into(), "page_reader".into()),
        ("session_authorization".into(), "page_reader".into()),
    ]);
    let q="SELECT id FROM docs WHERE id>'k00500' AND to_tsvector('simple',COALESCE(body,'')) @@ to_tsquery('simple','shared') ORDER BY id LIMIT 3";
    let mut sql =
        SqlSession::new_shared_secure(&db, context.clone()).with_session_gucs(gucs.clone());
    let (result, decisions) =
        with_plan_diagnostics(DiagnosticsOptions::default(), || sql.execute(q));
    assert_eq!(result.unwrap().rows.len(), 3);
    assert!(
        decisions.iter().any(|d| d.rows_fetched == Some(3)),
        "{decisions:?}"
    );
    drop(sql);
    SqlSession::new(&mut db)
        .execute("REVOKE SELECT ON docs FROM page_reader")
        .unwrap();
    let mut sql = SqlSession::new_shared_secure(&db, context).with_session_gucs(gucs);
    assert!(sql.execute(q).is_err());
    assert!(sql.execute(&q.replace("LIMIT 3", "LIMIT 0")).is_err());
}

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
