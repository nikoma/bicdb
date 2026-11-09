use bicdb_core::{
    BicDb, ConsumeOptions, DbConfig, PublishOptions, Record, ReplicationConfig, ReplicationMode,
    ReplicationTlsConfig, StorageMode, RECORD_AUDIT_STREAM,
};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        // Keep the transaction log, as a primary with a lagging standby must.
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Primary,
            listen_addr: Some("127.0.0.1:0".to_string()),
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..ReplicationConfig::default()
        })
}

fn consumed() -> ConsumeOptions {
    ConsumeOptions {
        max_messages: 10,
        visibility_timeout_ms: 30_000,
    }
}

fn trimmed_outbox_stays_trimmed(mode: StorageMode) {
    let root = tempfile::tempdir().unwrap();
    let config = config(mode);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    let tx = db.begin_transaction().unwrap();
    let message = tx
        .buffer_broker_publish(
            "business-events",
            json!({"committed": 1}),
            PublishOptions::default(),
        )
        .unwrap();
    tx.commit().unwrap();
    {
        let mut broker = db.broker();
        let batch = broker
            .consume("business-events", "consumer", "worker", consumed())
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].message_id, message);
        broker
            .ack("business-events", "consumer", "worker", message)
            .unwrap();
        assert_eq!(broker.trim("business-events").unwrap().removed_messages, 1);
        assert!(broker
            .consume("business-events", "consumer", "worker", consumed())
            .unwrap()
            .is_empty());
    }
    drop(db);
    for _ in 0..2 {
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        let batch = db
            .broker()
            .consume("business-events", "consumer", "worker", consumed())
            .unwrap();
        assert!(
            batch.is_empty(),
            "a committed, acknowledged and trimmed message was republished: {batch:?}"
        );
    }
}

#[test]
fn trimmed_transactional_message_is_not_republished_in_memory() {
    trimmed_outbox_stays_trimmed(StorageMode::EmbeddedMemory);
}

#[test]
fn trimmed_transactional_message_is_not_republished_paged() {
    trimmed_outbox_stays_trimmed(StorageMode::ServerPaged);
}

#[test]
fn a_checkpoint_between_core_commit_and_caller_finalization_keeps_committed_outboxes() {
    let root = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fsync(true)
        .with_audit_events(true);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_collection("items").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new("acknowledged-business-row"))
        .unwrap();
    tx.buffer_broker_publish(
        "business-events",
        json!({"committed": 1}),
        PublishOptions::default(),
    )
    .unwrap();
    // The core decision is now durable. A shared SQL caller releases its
    // database read lock here, before invoking the separate finalizers.
    db.commit_buffered_transaction(&mut tx).unwrap();
    db.checkpoint_for_resume().unwrap();
    drop(tx);
    drop(db);
    let mut db = BicDb::open_with_config(root.path(), config).unwrap();
    assert!(db
        .get("items", "acknowledged-business-row")
        .unwrap()
        .is_some());
    let messages = db
        .broker()
        .consume("business-events", "consumer", "worker", consumed())
        .unwrap()
        .len();
    let audit = db
        .events()
        .read(RECORD_AUDIT_STREAM)
        .iter()
        .filter(|entry| entry.event.payload["record_id"] == "acknowledged-business-row")
        .count();
    assert_eq!(
        (messages, audit),
        (1, 1),
        "a durable business row must keep both committed outboxes across a checkpoint"
    );
}
