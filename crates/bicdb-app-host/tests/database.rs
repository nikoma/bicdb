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
fn transaction_upserts_keep_sql_compatibility_on_the_shared_engine_path() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path()).unwrap();
    database
        .prepare(&release(
            "CREATE TABLE items (id INT PRIMARY KEY, value INT)",
        ))
        .unwrap();
    database.query(&batch("INSERT INTO items VALUES (1,1) ON CONFLICT (id) DO UPDATE SET value = items.value + 1 RETURNING value",None)).unwrap();
    let result=database.query(&batch("INSERT INTO items VALUES (1,1) ON CONFLICT (id) DO UPDATE SET value = items.value + 1 RETURNING value",None)).unwrap();
    assert_eq!(serde_json::to_value(&result[0].rows).unwrap(), json!([[2]]));
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

#[test]
fn job_identity_attempts_and_completion_travel_with_the_application_database() {
    use bicdb_sql::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let target = root.path().join("target");
    let package = release(
        "CREATE TABLE effects (id INT PRIMARY KEY, value INT); INSERT INTO effects VALUES (1,0)",
    );
    let mut database = Database::open(&source).unwrap();
    database.prepare(&package).unwrap();
    let done = database
        .enqueue(json!({"action":"done"}), Some("done".into()))
        .unwrap();
    let completed = database.consume("worker", 1000).unwrap().unwrap();
    database.settle("worker", &completed, true).unwrap();
    let receipt = database
        .enqueue(json!({"action":"pending"}), Some("pending".into()))
        .unwrap();
    let first = database.consume("worker", 1000).unwrap().unwrap();
    database
        .query(&batch(
            "UPDATE effects SET value = value + 1",
            Some(&format!("job:{}", first.message_id)),
        ))
        .unwrap();
    // The worker vanished after committing its effect. Expire its delivery
    // lease, then copy only data, without the historical broker directory.
    SqlSession::new(&mut database.db.write())
        .execute("UPDATE __bic_platform_jobs SET lease_until = 0 WHERE status = 'running'")
        .unwrap();
    std::fs::create_dir(&target).unwrap();
    database
        .db
        .read()
        .create_branch(&target.join("data"))
        .unwrap();
    drop(database);
    let mut recovered = Database::open(&target).unwrap();
    recovered.prepare(&package).unwrap();
    let repeated = recovered
        .enqueue(json!({"action":"pending"}), Some("pending".into()))
        .unwrap();
    assert_eq!(receipt["id"], repeated["id"]);
    assert_eq!(repeated["deduplicated"], true);
    assert_eq!(
        recovered
            .enqueue(json!({"action":"done"}), Some("done".into()))
            .unwrap()["id"],
        done["id"]
    );
    assert!(recovered
        .enqueue(json!({"action":"changed"}), Some("pending".into()))
        .is_err());
    let second = recovered.consume("worker", 1000).unwrap().unwrap();
    assert_eq!(first.message_id, second.message_id);
    assert_eq!(second.attempts, 2);
    assert!(
        recovered.settle("worker", &first, true).is_err(),
        "stale attempt cannot settle its replacement"
    );
    recovered
        .query(&batch(
            "UPDATE effects SET value = value + 1",
            Some(&format!("job:{}", second.message_id)),
        ))
        .unwrap();
    recovered.settle("worker", &second, true).unwrap();
    assert!(recovered.consume("worker", 1000).unwrap().is_none());
    assert_eq!(
        serde_json::to_value(
            &recovered
                .query(&batch("SELECT value FROM effects", None))
                .unwrap()[0]
                .rows
        )
        .unwrap(),
        json!([[1]])
    );
}

#[test]
fn legacy_job_migration_preserves_ids_leases_acknowledgements_and_dead_letters() {
    use bicdb_core::{BicDb, ConsumeOptions, NackOptions, PublishOptions};
    let root = tempfile::tempdir().unwrap();
    let mut legacy = BicDb::open(root.path().join("jobs")).unwrap();
    let first = legacy
        .broker()
        .publish_with(
            bicdb_app_host::database::JOB_QUEUE,
            json!({"work":1}),
            PublishOptions {
                idempotency_key: Some("first".into()),
                ..Default::default()
            },
        )
        .unwrap();
    legacy
        .broker()
        .publish(
            bicdb_app_host::database::JOB_QUEUE,
            json!({"work":2}),
            json!({}),
        )
        .unwrap();
    legacy
        .broker()
        .publish(
            bicdb_app_host::database::JOB_QUEUE,
            json!({"work":3}),
            json!({}),
        )
        .unwrap();
    legacy
        .broker()
        .publish(
            bicdb_app_host::database::JOB_QUEUE,
            json!({"work":4}),
            json!({}),
        )
        .unwrap();
    let messages = legacy
        .broker()
        .consume(
            bicdb_app_host::database::JOB_QUEUE,
            bicdb_app_host::database::JOB_GROUP,
            "legacy-worker",
            ConsumeOptions {
                max_messages: 3,
                visibility_timeout_ms: 60_000,
            },
        )
        .unwrap();
    legacy
        .broker()
        .ack_delivery("legacy-worker", &messages[0])
        .unwrap();
    legacy
        .broker()
        .nack_delivery(
            "legacy-worker",
            &messages[1],
            NackOptions {
                requeue: false,
                delay_ms: None,
                error: Some("failed".into()),
            },
        )
        .unwrap();
    drop(legacy);
    let mut database = Database::open(root.path()).unwrap();
    assert_eq!(
        database
            .enqueue(json!({"work":1}), Some("first".into()))
            .unwrap()["id"],
        first.message_id.to_string()
    );
    let status = database.job_status().unwrap();
    assert_eq!(status["dead_letters"], 1);
    assert_eq!(status["in_flight"], 1);
    assert_eq!(status["queued"], 1);
    let next = database.consume("new-worker", 1000).unwrap().unwrap();
    assert_eq!(next.payload, json!({"work":4}));
    database.settle("new-worker", &next, true).unwrap();
    database
        .settle("legacy-worker", &messages[2], true)
        .unwrap();
    drop(database);
    let mut database = Database::open(root.path()).unwrap();
    assert!(database.consume("new-worker", 1000).unwrap().is_none());
    assert_eq!(database.job_status().unwrap()["dead_letters"], 1);
}

#[test]
fn interrupted_job_migration_resumes_from_the_last_committed_batch() {
    use bicdb_core::{BicDb, PublishOptions};
    use bicdb_sql::SqlSession;
    let root = tempfile::tempdir().unwrap();
    // Construct a valid pre-migration catalog with one malformed key entry
    // that will fail the second import batch. The first batch must survive.
    let database = Database::open(root.path()).unwrap();
    SqlSession::new(&mut database.db.write())
        .execute("UPDATE __bic_platform_job_migration SET cursor = 0, complete = FALSE")
        .unwrap();
    SqlSession::new(&mut database.db.write())
        .execute(
            "INSERT INTO __bic_platform_job_keys VALUES ('legacy:key-139','conflict','conflict')",
        )
        .unwrap();
    drop(database);
    let mut legacy = BicDb::open(root.path().join("jobs")).unwrap();
    for index in 0..140 {
        legacy
            .broker()
            .publish_with(
                bicdb_app_host::database::JOB_QUEUE,
                json!({"index":index}),
                PublishOptions {
                    idempotency_key: Some(format!("key-{index}")),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    drop(legacy);
    assert!(Database::open(root.path()).is_err());
    let mut data = BicDb::open(root.path().join("data")).unwrap();
    let mut session = SqlSession::new(&mut data);
    assert_eq!(
        serde_json::to_value(
            session
                .execute("SELECT COUNT(*) FROM __bic_platform_jobs")
                .unwrap()
                .rows
        )
        .unwrap(),
        json!([[128]])
    );
    assert_eq!(
        serde_json::to_value(
            session
                .execute("SELECT cursor, complete FROM __bic_platform_job_migration")
                .unwrap()
                .rows
        )
        .unwrap(),
        json!([[128, false]])
    );
    session
        .execute("DELETE FROM __bic_platform_job_keys WHERE key = 'legacy:key-139'")
        .unwrap();
    drop(session);
    drop(data);
    let mut recovered = Database::open(root.path()).unwrap();
    assert_eq!(recovered.job_status().unwrap()["queued"], 140);
    for index in 0..140 {
        let message = recovered.consume("worker", 1000).unwrap().unwrap();
        assert_eq!(message.payload, json!({"index":index}));
        recovered.settle("worker", &message, true).unwrap();
    }
    assert!(recovered.consume("worker", 1000).unwrap().is_none());
}

#[test]
fn failed_job_deliveries_keep_their_identity_and_reach_the_attempt_ceiling() {
    use bicdb_sql::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path()).unwrap();
    let payload = json!({"work":"O'Reilly\\tmp\n\u{0000}", "items":[true,null,17], "label":"雪"});
    let key = "quoted'\\key";
    let receipt = database.enqueue(payload.clone(), Some(key.into())).unwrap();
    assert_eq!(
        database.enqueue(payload.clone(), Some(key.into())).unwrap()["id"],
        receipt["id"]
    );
    for attempt in 1..=bicdb_core::DEFAULT_MAX_ATTEMPTS {
        SqlSession::new(&mut database.db.write())
            .execute("UPDATE __bic_platform_jobs SET available_at = 0 WHERE status = 'ready'")
            .unwrap();
        let message = database.consume("worker", 1000).unwrap().unwrap();
        assert_eq!(message.payload, payload);
        assert_eq!(receipt["id"], message.message_id.to_string());
        assert_eq!(message.attempts, attempt);
        assert!(database.consume("other-worker", 1000).unwrap().is_none());
        database.settle("worker", &message, false).unwrap();
    }
    assert!(database.consume("worker", 1000).unwrap().is_none());
    let status = database.job_status().unwrap();
    assert_eq!(status["dead_letters"], 1);
    assert_eq!(status["in_flight"], 0);
    assert_eq!(status["pending_redelivery"], 0);
}
