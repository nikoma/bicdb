use bicdb_core::{BicDb, DbConfig, IndexValue, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn setup(db: &mut BicDb) {
    SqlSession::new(db).execute("CREATE TABLE members(id text PRIMARY KEY, owner text, box text, uid bigint NOT NULL, created bigint, removed bigint);
        INSERT INTO members SELECT 'k'||g, 'alice', 'inbox', g, 1, NULL FROM generate_series(1,3000) g;
        INSERT INTO members VALUES ('gap','alice','inbox',2000000000,1,NULL),('other','bob','inbox',1503,1,NULL);
        CREATE UNIQUE INDEX member_uid ON members(owner,box,uid)").unwrap();
}

#[test]
fn authenticated_reader_uses_bounded_ranges_without_bypassing_select_grants() {
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
    setup(&mut db);
    SqlSession::new(&mut db)
        .execute("CREATE ROLE range_reader NOSUPERUSER; GRANT SELECT ON members TO range_reader")
        .unwrap();
    let context = SecurityContext::authenticated(
        "range_reader",
        "fixture",
        AuthenticationStrength::ScramSha256,
    );
    let gucs = std::collections::HashMap::from([
        ("role".into(), "range_reader".into()),
        ("session_authorization".into(), "range_reader".into()),
    ]);
    let q="SELECT uid FROM members WHERE owner='alice' AND box='inbox' AND uid>1500 AND removed IS NULL ORDER BY uid LIMIT 3";
    let mut sql =
        SqlSession::new_shared_secure(&db, context.clone()).with_session_gucs(gucs.clone());
    let (result, decisions) =
        with_plan_diagnostics(DiagnosticsOptions::default(), || sql.execute(q));
    assert_eq!(result.unwrap().rows[0][0], SqlValue::Int(1501));
    assert!(
        decisions.iter().any(|d| d.rows_fetched == Some(3)),
        "{decisions:?}"
    );
    drop(sql);
    SqlSession::new(&mut db)
        .execute("REVOKE SELECT ON members FROM range_reader")
        .unwrap();
    let mut sql = SqlSession::new_shared_secure(&db, context).with_session_gucs(gucs);
    assert!(sql.execute(q).is_err());
    assert!(sql.execute(&q.replace("LIMIT 3", "LIMIT 0")).is_err());
}

#[test]
fn paged_range_limit_filters_tombstones_and_visibility_before_counting_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    setup(&mut db);
    let mut sql = SqlSession::new(&mut db);
    sql.execute("UPDATE members SET removed=1 WHERE id='k1501'; UPDATE members SET created=100 WHERE id='k1502'").unwrap();
    let q="SELECT uid FROM members WHERE owner='alice' AND box='inbox' AND uid>1500 AND uid<=1510 AND created<=50 AND removed IS NULL ORDER BY uid LIMIT 3";
    let expected = vec![
        vec![SqlValue::Int(1503)],
        vec![SqlValue::Int(1504)],
        vec![SqlValue::Int(1505)],
    ];
    assert_eq!(sql.execute(q).unwrap().rows, expected);
    assert_eq!(
        sql.execute(&q.replace("created<=50", "created<='50'"))
            .unwrap()
            .rows,
        expected
    );
    let plan = sql
        .execute(&format!("EXPLAIN ANALYZE {q}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("ActualRowsFetched 5"), "{plan}");
    assert_eq!(
        sql.execute(&q.replace("uid>1500 AND uid<=1510", "uid>3000"))
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2000000000)]]
    );
    sql.execute("BEGIN; UPDATE members SET removed=NULL WHERE id='k1501'")
        .unwrap();
    assert_eq!(sql.execute(q).unwrap().rows[0][0], SqlValue::Int(1501));
    sql.execute("ROLLBACK; DROP INDEX member_uid").unwrap();
    assert_eq!(sql.execute(q).unwrap().rows, expected);
}

#[test]
fn unique_range_cursor_keeps_its_snapshot_across_batches_and_updates() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    setup(&mut db);
    let prefix = vec![IndexValue::from("alice"), IndexValue::from("inbox")];
    let mut cursor = db
        .unique_index_range_cursor(
            "member_uid",
            &prefix,
            (IndexValue::from(1500i64), false),
            Some((IndexValue::from(1510i64), true)),
        )
        .unwrap();
    let first = cursor.next_batch(3).unwrap();
    assert_eq!(first.len(), 3);
    SqlSession::new(&mut db)
        .execute("UPDATE members SET uid=4000 WHERE id='k1505'")
        .unwrap();
    let rest = cursor.next_batch(20).unwrap();
    assert_eq!(rest.len(), 7);
    assert!(cursor.next_batch(20).unwrap().is_empty());
    let all = first
        .into_iter()
        .chain(rest)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(all.len(), 10);
    let mut current = db
        .unique_index_range_cursor(
            "member_uid",
            &prefix,
            (IndexValue::from(1500i64), false),
            Some((IndexValue::from(1510i64), true)),
        )
        .unwrap();
    assert_eq!(current.next_batch(20).unwrap().len(), 9);
}
