use bicdb_app_host::{
    database::{Database, Query, QueryBatch},
    manifest::{Release, TableSync},
    preview,
    store::Store,
};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;
use std::path::Path;

fn fixture() -> Release {
    Release::read(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap()
}
fn effect() -> QueryBatch {
    QueryBatch {
        statements: vec![Query {
            sql: "UPDATE counters SET value = value + 1 WHERE id = 'requests' RETURNING value"
                .into(),
            params: vec![],
        }],
        idempotency_key: Some("shared-receipt-key".into()),
        jobs: vec![json!({"action":"private-source-outbox"})],
    }
}
fn value(database: &mut Database) -> i64 {
    let rows = database
        .query(&QueryBatch {
            statements: vec![Query {
                sql: "SELECT value FROM counters WHERE id = 'requests'".into(),
                params: vec![],
            }],
            idempotency_key: None,
            jobs: vec![],
        })
        .unwrap();
    let SqlValue::Int(value) = rows[0].rows[0][0] else {
        panic!("invalid counter")
    };
    value
}

#[test]
fn previews_copy_public_data_without_replaying_migrations_jobs_or_effect_receipts() {
    let root = tempfile::tempdir().unwrap();
    let mut source = Database::open(&root.path().join("source")).unwrap();
    let mut release = fixture();
    release.manifest.sync.insert(
        "counters".into(),
        TableSync::SingleWriter {
            node: source.db.read().node_id(),
        },
    );
    source.prepare(&release).unwrap();
    source.query(&effect()).unwrap();
    let queued = source
        .enqueue(json!({"action":"source-job"}), Some("same-job-key".into()))
        .unwrap();
    let parent = source.db.read().node_id();
    let parent_key = source.db.read().mesh_verifying_key();
    let store = Store::open(&root.path().join("catalog")).unwrap();
    let deployment = preview::create(&store, &source, &release, "counter-preview").unwrap();
    assert!(!deployment.enabled);
    let copied_release = store
        .load_release("counter-preview", &deployment.release)
        .unwrap();
    assert!(copied_release.manifest.sync.is_empty());
    let path = store.app_dir("counter-preview").unwrap();
    assert!(!path.join("peers.json").exists());
    let mut copied = Database::open(&path.join("database")).unwrap();
    assert_ne!(copied.db.read().node_id(), parent);
    assert_ne!(copied.db.read().mesh_verifying_key(), parent_key);
    copied.prepare(&copied_release).unwrap(); // Applied seed INSERTs must not run again.
    assert_eq!(value(&mut copied), 1);
    assert_eq!(copied.drain_outbox(128).unwrap(), 0);
    assert!(copied.consume("preview-worker", 1000).unwrap().is_none());
    copied.query(&effect()).unwrap();
    assert_eq!(
        value(&mut copied),
        2,
        "the source effect receipt must not deduplicate preview work"
    );
    assert_eq!(value(&mut source), 1);
    assert_eq!(
        source
            .consume("source-worker", 1000)
            .unwrap()
            .unwrap()
            .message_id
            .to_string(),
        queued["id"]
    );
    assert!(copied
        .db
        .write()
        .export_sync_bundle_delta(&Default::default())
        .unwrap()
        .events
        .is_empty());
    assert!(preview::create(&store, &source, &release, "counter-preview").is_err());
    assert_eq!(
        value(&mut copied),
        2,
        "an existing preview cannot be replaced"
    );
}

#[test]
fn previews_preserve_serial_counters_and_indexes_and_refuse_incomplete_history() {
    use base64::Engine;
    let root = tempfile::tempdir().unwrap();
    let mut source = Database::open(&root.path().join("source")).unwrap();
    let mut release = fixture();
    release.manifest.files.push("002.sql".into());
    release.manifest.migrations.push("002.sql".into());
    release.files.insert("002.sql".into(), base64::engine::general_purpose::STANDARD.encode(
        "CREATE TABLE notes (id BIGSERIAL PRIMARY KEY, body TEXT UNIQUE); INSERT INTO notes (body) VALUES ('original')"));
    source.prepare(&release).unwrap();
    let store = Store::open(&root.path().join("catalog")).unwrap();
    let deployment = preview::create(&store, &source, &release, "serial-preview").unwrap();
    let copied_release = store
        .load_release("serial-preview", &deployment.release)
        .unwrap();
    let mut copied =
        Database::open(&store.app_dir("serial-preview").unwrap().join("database")).unwrap();
    copied.prepare(&copied_release).unwrap();
    let mut db = copied.db.write();
    let result = SqlSession::new(&mut db)
        .execute("INSERT INTO notes (body) VALUES ('copy') RETURNING id")
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Int(2));
    assert!(SqlSession::new(&mut db)
        .execute("INSERT INTO notes (body) VALUES ('original')")
        .is_err());
    drop(db);
    let mut incomplete = release.clone();
    incomplete.manifest.files.push("003.sql".into());
    incomplete.manifest.migrations.push("003.sql".into());
    incomplete.files.insert(
        "003.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("INSERT INTO notes (body) VALUES ('must-not-run')"),
    );
    assert!(preview::create(&store, &source, &incomplete, "incomplete-preview").is_err());
    assert!(!store.app_dir("incomplete-preview").unwrap().exists());
}

#[test]
fn preview_rejects_row_policies_and_runtime_queries_cannot_modify_internal_schemas() {
    let root = tempfile::tempdir().unwrap();
    let mut source = Database::open(&root.path().join("source")).unwrap();
    let release = fixture();
    source.prepare(&release).unwrap();
    for sql in [
        "SELECT * FROM public.\"__bicdb_pg_roles\"",
        "SELECT * FROM __bicdb_pg_schema AS schema_cache",
        "WITH private AS (SELECT * FROM __bicdb_pg_sequences) SELECT * FROM private",
        "UPDATE __bicdb_pg_schema SET name = 'changed'",
    ] {
        assert!(source
            .query(&QueryBatch {
                statements: vec![Query {
                    sql: sql.into(),
                    params: vec![]
                }],
                idempotency_key: None,
                jobs: vec![]
            })
            .is_err());
    }
    SqlSession::new(&mut source.db.write())
        .execute("ALTER TABLE counters ENABLE ROW LEVEL SECURITY")
        .unwrap();
    let store = Store::open(&root.path().join("catalog")).unwrap();
    assert!(preview::create(&store, &source, &release, "protected-preview").is_err());
    assert!(!store.app_dir("protected-preview").unwrap().exists());
}

#[test]
fn previews_rebuild_vector_indexes_with_the_original_metric_and_tuning() {
    use base64::Engine;
    use bicdb_core::{HnswIndexConfig, VectorMetric};

    let root = tempfile::tempdir().unwrap();
    let mut source = Database::open(&root.path().join("source")).unwrap();
    let mut release = fixture();
    release.manifest.files.push("002.sql".into());
    release.manifest.migrations.push("002.sql".into());
    release.files.insert("002.sql".into(), base64::engine::general_purpose::STANDARD.encode(
        "CREATE TABLE memories (id TEXT PRIMARY KEY, embedding VECTOR(3)); INSERT INTO memories VALUES ('near', '[1,0,0]'), ('far', '[0,1,0]')"));
    source.prepare(&release).unwrap();
    let configuration = HnswIndexConfig {
        m: 8,
        ef_construction: 32,
        ef_search: 16,
        distance: VectorMetric::L2,
    };
    source
        .db
        .write()
        .create_vector_index("memories", configuration)
        .unwrap();
    let store = Store::open(&root.path().join("catalog")).unwrap();
    preview::create(&store, &source, &release, "vector-preview").unwrap();
    let copied =
        Database::open(&store.app_dir("vector-preview").unwrap().join("database")).unwrap();
    let mut db = copied.db.write();
    assert_eq!(db.vector_index_config("memories"), Some(configuration));
    let verified = db.verify_vector_index("memories").unwrap();
    assert!(verified.valid);
    assert_eq!(verified.indexed_vectors, 2);
    let mut session = SqlSession::new(&mut db);
    session.execute("SET bicdb.vector_search = 'ann'").unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM memories ORDER BY embedding <-> '[1,0,0]' LIMIT 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("near".into())]]
    );
}
