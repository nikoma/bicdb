use base64::Engine;
use bicdb_app_host::{
    database::{Database, Query, QueryBatch},
    manifest::{Manifest, Release, Resources, Runtime},
    store::Store,
};
use serde_json::json;
use std::collections::BTreeMap;

fn release(sql: &str) -> Release {
    Release {
        manifest: Manifest {
            format: 1,
            name: "example".into(),
            version: "1".into(),
            runtime: Runtime::Node,
            isolation: Default::default(),
            placement: None,
            entrypoint: "app.mjs".into(),
            files: vec!["app.mjs".into(), "001.sql".into()],
            migrations: vec!["001.sql".into()],
            resources: Resources::default(),
            sync: BTreeMap::new(),
        },
        files: BTreeMap::from([
            (
                "app.mjs".into(),
                base64::engine::general_purpose::STANDARD
                    .encode("export default {fetch(){return new Response('ok')}}"),
            ),
            (
                "001.sql".into(),
                base64::engine::general_purpose::STANDARD.encode(sql),
            ),
        ]),
    }
}
fn batch(sql: &str, key: Option<&str>) -> QueryBatch {
    QueryBatch {
        statements: vec![Query {
            sql: sql.into(),
            params: vec![],
        }],
        idempotency_key: key.map(str::to_owned),
        jobs: vec![],
    }
}

#[test]
fn committed_migrations_and_idempotent_effects_survive_restart() {
    let root = tempfile::tempdir().unwrap();
    let package = release(
        "CREATE TABLE counts (id INT PRIMARY KEY, value INT); INSERT INTO counts VALUES (1, 0)",
    );
    let request = batch(
        "UPDATE counts SET value = value + 1 WHERE id = 1 RETURNING value",
        Some("one"),
    );
    let first = {
        let mut database = Database::open(root.path()).unwrap();
        database.migrate(&package).unwrap();
        let result = database.query(&request).unwrap();
        assert_eq!(serde_json::to_value(&result[0].rows).unwrap(), json!([[1]]));
        result
    };
    let mut database = Database::open(root.path()).unwrap();
    database.migrate(&package).unwrap();
    assert_eq!(database.query(&request).unwrap(), first);
    assert_eq!(
        serde_json::to_value(
            database
                .query(&batch("SELECT value FROM counts", None))
                .unwrap()[0]
                .rows
                .clone()
        )
        .unwrap(),
        json!([[1]])
    );
    assert!(database
        .query(&batch("DELETE FROM counts", Some("one")))
        .is_err());
    let mut edited = package.clone();
    edited.files.insert(
        "001.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("CREATE TABLE counts (id INT PRIMARY KEY)"),
    );
    assert!(database.migrate(&edited).is_err());
}

#[test]
fn failed_batch_rolls_back_all_writes_and_parameter_binding_is_not_sql_interpolation() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path()).unwrap();
    database
        .migrate(&release(
            "CREATE TABLE items (id TEXT PRIMARY KEY, value INT)",
        ))
        .unwrap();
    let text = "x'); DELETE FROM items; --";
    let request = QueryBatch {
        statements: vec![Query {
            sql: "INSERT INTO items VALUES ($1, $2)".into(),
            params: vec![json!(text), json!(7)],
        }],
        idempotency_key: None,
        jobs: vec![],
    };
    database.query(&request).unwrap();
    let mut invalid = batch("UPDATE items SET value = 99", None);
    invalid.statements.push(Query {
        sql: "INSERT INTO missing VALUES (1)".into(),
        params: vec![],
    });
    assert!(database.query(&invalid).is_err());
    let rows = &database
        .query(&batch("SELECT id, value FROM items", None))
        .unwrap()[0]
        .rows;
    assert_eq!(serde_json::to_value(rows).unwrap(), json!([[text, 7]]));
    assert!(database.query(&batch("COMMIT", None)).is_err());
    assert!(database
        .query(&batch("SELECT 1; DELETE FROM items", None))
        .is_err());
}

#[test]
fn failed_migration_is_not_recorded_or_partially_applied() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path()).unwrap();
    let mut package =
        release("CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO absent VALUES (1)");
    assert!(database.migrate(&package).is_err());
    package.files.insert(
        "001.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO items VALUES (1)"),
    );
    database.migrate(&package).unwrap();
    assert_eq!(
        database.query(&batch("SELECT * FROM items", None)).unwrap()[0]
            .rows
            .len(),
        1
    );
}

#[test]
fn release_paths_integrity_and_supervisor_ownership_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    assert!(Store::open(root.path()).is_err());
    let mut package = release("CREATE TABLE items (id INT PRIMARY KEY)");
    let digest = store.save_release(&package).unwrap();
    assert_eq!(
        store
            .load_release("example", &digest)
            .unwrap()
            .digest()
            .unwrap(),
        digest
    );
    package.manifest.files.push("../outside".into());
    package.files.insert("../outside".into(), "eA==".into());
    assert!(store.save_release(&package).is_err());
    assert!(store.app_dir("../escape").is_err());
    assert!(store.load_release("example", "../../outside").is_err());
}

#[test]
fn transactional_job_outbox_commits_with_data_and_recovers_without_duplicate_publication() {
    let root = tempfile::tempdir().unwrap();
    let package = release(
        "CREATE TABLE counts (id INT PRIMARY KEY, value INT); INSERT INTO counts VALUES (1, 0)",
    );
    let mut database = Database::open(root.path()).unwrap();
    database.prepare(&package).unwrap();
    let mut invalid = batch(
        "UPDATE counts SET value = value + 1 WHERE id = 1",
        Some("failed"),
    );
    invalid.jobs.push(json!({"action":"work"}));
    invalid.statements.push(Query {
        sql: "INSERT INTO absent VALUES (1)".into(),
        params: vec![],
    });
    assert!(database.query(&invalid).is_err());
    assert_eq!(database.drain_outbox(32).unwrap(), 0);
    assert!(database.consume("worker", 1000).unwrap().is_none());
    let mut valid = batch(
        "UPDATE counts SET value = value + 1 WHERE id = 1 RETURNING value",
        Some("valid"),
    );
    valid.jobs.push(json!({"action":"work"}));
    let result = database.query(&valid).unwrap();
    assert_eq!(database.query(&valid).unwrap(), result);
    drop(database);
    let mut database = Database::open(root.path()).unwrap();
    database.prepare(&package).unwrap();
    assert_eq!(database.drain_outbox(32).unwrap(), 1);
    let message = database.consume("worker", 1000).unwrap().unwrap();
    assert_eq!(message.payload, json!({"action":"work"}));
    database.settle("worker", &message, true).unwrap();
    assert_eq!(database.query(&valid).unwrap(), result);
    assert_eq!(database.drain_outbox(32).unwrap(), 0);
    assert!(database.consume("worker", 1000).unwrap().is_none());
    assert_eq!(
        serde_json::to_value(
            database
                .query(&batch("SELECT value FROM counts", None))
                .unwrap()[0]
                .rows
                .clone()
        )
        .unwrap(),
        json!([[1]])
    );
}

#[test]
fn default_isolation_preserves_the_digest_of_existing_releases() {
    use sha2::{Digest, Sha256};
    let legacy = br#"{"manifest":{"format":1,"name":"legacy","version":"1","runtime":"node","entrypoint":"app.mjs","files":["app.mjs"],"migrations":[],"resources":{"memory_mb":128,"request_timeout_ms":30000,"max_concurrent_requests":64},"sync":{}},"files":{"app.mjs":"ZXhwb3J0IGRlZmF1bHQge2ZldGNoKCl7cmV0dXJuIG5ldyBSZXNwb25zZSgnb2snKX19"}}"#;
    let release: Release = serde_json::from_slice(legacy).unwrap();
    release.validate().unwrap();
    assert_eq!(
        release.digest().unwrap(),
        format!("{:x}", Sha256::digest(legacy))
    );
}

#[test]
fn an_expired_database_deadline_refuses_mutations_before_execution() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path()).unwrap();
    database
        .prepare(&release(
            "CREATE TABLE counts (id INT PRIMARY KEY, value INT); INSERT INTO counts VALUES (1, 0)",
        ))
        .unwrap();
    assert!(database
        .query_with_deadline(
            &batch("UPDATE counts SET value = 1", None),
            std::time::Instant::now() - std::time::Duration::from_millis(1)
        )
        .is_err());
    assert_eq!(
        serde_json::to_value(
            database
                .query(&batch("SELECT value FROM counts", None))
                .unwrap()[0]
                .rows
                .clone()
        )
        .unwrap(),
        json!([[0]])
    );
}
