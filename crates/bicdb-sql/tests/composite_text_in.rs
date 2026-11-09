use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn fixture() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE members(id text PRIMARY KEY,tenant text,mailbox text,msg text,visible boolean);
        INSERT INTO members SELECT 'm-' || g,'own','Inbox','k' || g,g % 2 = 0 FROM generate_series(1,1000) g;
        INSERT INTO members VALUES ('duplicate','own','Inbox','k12',true),('foreign','other','Inbox','k12',true),('archive','own','Archive','k12',true);
        CREATE INDEX member_text_key ON members(tenant,mailbox,msg)").unwrap();
    (root, db)
}
const QUERY: &str = "SELECT id FROM members WHERE tenant='own' AND mailbox='Inbox' AND msg IN ('k7','k12','k12',NULL,'absent') AND visible ORDER BY id";
fn expected() -> Vec<Vec<SqlValue>> {
    vec![
        vec![SqlValue::String("duplicate".into())],
        vec![SqlValue::String("m-12".into())],
    ]
}
fn plan(sql: &mut SqlSession<'_>, query: &str) -> String {
    sql.execute(&format!("EXPLAIN ANALYZE {query}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn complete_composite_in_reads_only_candidates_and_matches_scan_with_nulls_and_duplicates() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(sql.execute(QUERY).unwrap().rows, expected());
    let p = plan(&mut sql, QUERY);
    assert!(p.contains("IndexScan member_text_key"), "{p}");
    assert!(p.contains("PrefixKeys 3"), "{p}");
    assert!(p.contains("ActualRowsFetched 3"), "{p}");
    sql.execute("DROP INDEX member_text_key").unwrap();
    assert_eq!(sql.execute(QUERY).unwrap().rows, expected());
}

#[test]
fn composite_in_parameters_are_rebound_across_accounts() {
    let (_root, db) = fixture();
    let query="SELECT id FROM members m WHERE m.tenant=$1 AND m.mailbox=$2 AND m.msg IN ($3,$4,NULL) AND m.visible ORDER BY m.id";
    for (tenant, want) in [
        ("own", expected()),
        ("other", vec![vec![SqlValue::String("foreign".into())]]),
        ("own", expected()),
    ] {
        let mut sql = SqlSession::new_shared(&db).with_positional_parameters(vec![
            SqlValue::String(tenant.into()),
            SqlValue::String("Inbox".into()),
            SqlValue::String("k12".into()),
            SqlValue::String("k7".into()),
        ]);
        assert_eq!(sql.execute(query).unwrap().rows, want);
    }
}

#[test]
fn composite_in_keeps_pending_insert_update_and_delete_visible() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; INSERT INTO members VALUES ('pending','own','Inbox','new-key',true)")
        .unwrap();
    let query="SELECT id FROM members WHERE tenant='own' AND mailbox='Inbox' AND msg IN ('new-key') ORDER BY id";
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("pending".into())]]
    );
    sql.execute("UPDATE members SET msg='new-key' WHERE id='m-12'")
        .unwrap();
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![
            vec![SqlValue::String("m-12".into())],
            vec![SqlValue::String("pending".into())]
        ]
    );
    sql.execute("DELETE FROM members WHERE id='pending'")
        .unwrap();
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::String("m-12".into())]]
    );
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(sql.execute(QUERY).unwrap().rows, expected());
}

#[test]
fn composite_in_fixed_snapshot_keeps_the_old_index_value_visible() {
    let (_root, db) = fixture();
    let mut reader = SqlSession::new_shared(&db);
    let mut writer = SqlSession::new_shared(&db);
    reader
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .unwrap();
    assert_eq!(reader.execute(QUERY).unwrap().rows, expected());
    writer
        .execute("UPDATE members SET msg='changed' WHERE id='m-12'")
        .unwrap();
    assert_eq!(reader.execute(QUERY).unwrap().rows, expected());
    reader.execute("COMMIT").unwrap();
    assert_eq!(
        reader.execute(QUERY).unwrap().rows,
        vec![vec![SqlValue::String("duplicate".into())]]
    );
}

#[test]
fn composite_in_rechecks_row_security_and_hides_prefilter_counts() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE in_reader NOSUPERUSER NOBYPASSRLS; ALTER TABLE members ENABLE ROW LEVEL SECURITY;
        CREATE POLICY own_rows ON members FOR SELECT TO in_reader USING (tenant='own' AND visible);
        GRANT SELECT ON members TO in_reader; SET ROLE in_reader; BEGIN").unwrap();
    assert_eq!(sql.execute(QUERY).unwrap().rows, expected());
    let p = plan(&mut sql, QUERY);
    assert!(!p.contains("ActualRowsFetched"), "{p}");
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn composite_in_preserves_negation_nulls_and_expression_fallback() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    for suffix in ["msg IN (NULL)", "msg NOT IN ('k12',NULL)"] {
        assert!(sql
            .execute(&format!(
                "SELECT id FROM members WHERE tenant='own' AND mailbox='Inbox' AND {suffix}"
            ))
            .unwrap()
            .rows
            .is_empty());
    }
    assert_eq!(sql.execute("SELECT id FROM members WHERE tenant='own' AND mailbox='Inbox' AND msg IN (lower('K12')) AND visible ORDER BY id").unwrap().rows,expected());
}
