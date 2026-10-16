use std::path::Path;

use bicdb_app_host::{database::Database, manifest::Release, store::Store};
use bicdb_core::{create_backup_from_open_database, BackupCreateOptions, BicDb, PublishOptions};
use bicdb_sql::SqlSession;
use serde_json::json;

#[test]
fn format_one_recovery_images_migrate_pending_local_broker_jobs() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let mut legacy = BicDb::open(source.join("jobs")).unwrap();
    let receipt = legacy
        .broker()
        .publish_with(
            "bic.platform.jobs",
            json!({"action":"legacy-work"}),
            PublishOptions {
                idempotency_key: Some("legacy-key".into()),
                ..Default::default()
            },
        )
        .unwrap();
    drop(legacy);
    let release = Release::read(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap();
    let mut database = Database::open(&source).unwrap();
    database.prepare(&release).unwrap();
    // Reconstruct the previous host's data schema: its jobs lived exclusively
    // in the separate broker image and these new ledger tables did not exist.
    for table in [
        "__bic_platform_jobs",
        "__bic_platform_job_payloads",
        "__bic_platform_job_keys",
        "__bic_platform_job_migration",
    ] {
        SqlSession::new(&mut database.db.write())
            .execute(&format!("DROP TABLE {table}"))
            .unwrap();
    }
    let data = source.join("data");
    let passphrase = "format-one-recovery-test-key-with-at-least-32-characters";
    database
        .snapshot_jobs(&data.join("platform-recovery-jobs.bicbak"), passphrase)
        .unwrap();
    bicdb_app_host::store::atomic_write(
        &data.join("platform-recovery.json"),
        &serde_json::to_vec(&json!({
            "format":1,"deployment":{"application":"counter","release":release.digest().unwrap(),
                "previous":null,"generation":1,"enabled":true},"release":release,"previous":null
        }))
        .unwrap(),
    )
    .unwrap();
    let archive = root.path().join("legacy.bicbak");
    create_backup_from_open_database(
        &database.db.read(),
        &archive,
        BackupCreateOptions {
            passphrase: passphrase.into(),
            base_backup: None,
        },
    )
    .unwrap();
    drop(database);
    let target = Store::open(&root.path().join("target")).unwrap();
    let deployment = bicdb_app_host::recovery::restore(&target, &archive, passphrase).unwrap();
    assert!(!deployment.enabled);
    let mut recovered =
        Database::open(&target.app_dir("counter").unwrap().join("database")).unwrap();
    let replay = recovered
        .enqueue(json!({"action":"legacy-work"}), Some("legacy-key".into()))
        .unwrap();
    assert_eq!(replay["id"], receipt.message_id.to_string());
    assert_eq!(replay["deduplicated"], true);
    let message = recovered.consume("worker", 1000).unwrap().unwrap();
    assert_eq!(message.message_id, receipt.message_id);
    assert_eq!(message.payload, json!({"action":"legacy-work"}));
    recovered.settle("worker", &message, true).unwrap();
    assert!(recovered.consume("worker", 1000).unwrap().is_none());
}
