use bicdb_app_host::{
    database::{Database, Query, QueryBatch},
    manifest::Release,
};
use bicdb_sql::SqlValue;
use serde_json::json;
use std::path::Path;

fn open(path: &Path) -> Database {
    let release = Release::read(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap();
    let mut database = Database::open(path).unwrap();
    database.prepare(&release).unwrap();
    database
}
fn query(sql: &str) -> Query {
    Query {
        sql: sql.into(),
        params: vec![],
    }
}
fn execute(database: &mut Database, sql: &str) {
    database
        .query(&QueryBatch {
            statements: vec![query(sql)],
            idempotency_key: None,
            jobs: vec![],
        })
        .unwrap();
}

#[test]
fn live_queries_preserve_parameters_emit_row_deltas_and_skip_unchanged_results() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(root.path());
    let mut live = database
        .subscribe(Query {
            sql: "SELECT id, value FROM counters WHERE id = $1".into(),
            params: vec![json!("requests")],
        })
        .unwrap();
    let initial = database.poll_subscription(&mut live).unwrap().unwrap();
    assert_eq!(initial.columns, vec!["commit_seq", "change", "id", "value"]);
    assert_eq!(initial.rows.len(), 1);
    assert_eq!(initial.rows[0][1], SqlValue::String("+".into()));
    assert_eq!(initial.rows[0][3], SqlValue::Int(0));
    let other_root = tempfile::tempdir().unwrap();
    let other = open(other_root.path());
    assert!(
        other.poll_subscription(&mut live).is_err(),
        "a subscription cannot cross application databases"
    );
    assert!(database.poll_subscription(&mut live).unwrap().is_none());
    execute(
        &mut database,
        "UPDATE counters SET value = 1 WHERE id = 'requests'",
    );
    let changes = database.poll_subscription(&mut live).unwrap().unwrap();
    assert_eq!(changes.rows.len(), 2);
    assert!(changes
        .rows
        .iter()
        .any(|row| row[1] == SqlValue::String("-".into()) && row[3] == SqlValue::Int(0)));
    assert!(changes
        .rows
        .iter()
        .any(|row| row[1] == SqlValue::String("+".into()) && row[3] == SqlValue::Int(1)));
    execute(
        &mut database,
        "UPDATE counters SET value = 1 WHERE id = 'requests'",
    );
    assert!(database.poll_subscription(&mut live).unwrap().is_none());
    execute(&mut database, "DELETE FROM counters WHERE id = 'requests'");
    let changes = database.poll_subscription(&mut live).unwrap().unwrap();
    assert_eq!(changes.rows.len(), 1);
    assert_eq!(changes.rows[0][1], SqlValue::String("-".into()));
}

#[test]
fn live_queries_refuse_effects_private_metadata_and_oversized_results() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(root.path());
    for sql in [
        "DELETE FROM counters RETURNING *",
        "SELECT * INTO stolen FROM counters",
        "SELECT * FROM counters FOR UPDATE",
        "SELECT * FROM __bic_platform_jobs",
        "SELECT * FROM public.\"__bic_platform_peer_grants\"",
        "SELECT nextval('serial')",
        "SELECT pg_notify('channel', 'payload')",
        "WITH deleted AS (DELETE FROM counters RETURNING *) SELECT * FROM deleted",
        "SELECT * FROM counters; SELECT 1",
    ] {
        assert!(
            database.subscribe(query(sql)).is_err(),
            "accepted unsafe live query: {sql}"
        );
    }
    let mut huge = database
        .subscribe(Query {
            sql: "SELECT $1::text AS a, $1::text AS b, $1::text AS c".into(),
            params: vec![json!("x".repeat(512 * 1024))],
        })
        .unwrap();
    assert!(database.poll_subscription(&mut huge).is_err());
    let result = database
        .query(&QueryBatch {
            statements: vec![query("SELECT value FROM counters WHERE id = 'requests'")],
            idempotency_key: None,
            jobs: vec![],
        })
        .unwrap();
    assert_eq!(result[0].rows[0][0], SqlValue::Int(0));
}
