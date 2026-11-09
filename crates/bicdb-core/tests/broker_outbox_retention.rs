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
        .unwrap()
        .unwrap()
        .iter()
        .filter(|entry| entry.event.payload["record_id"] == "acknowledged-business-row")
        .count();
    assert_eq!(
        (messages, audit),
        (1, 1),
        "a durable business row must keep both committed outboxes across a checkpoint"
    );
}

#[test]
fn a_failed_committed_broker_publication_fences_until_verified_recovery() {
    use bicdb_core::{BicDbError, DurabilityState, DEFAULT_EVENTS_DIR, DEFAULT_EVENTS_SEGMENT};
    use bicdb_page::test_faults::arm_io_failure;
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for site in ["frame_log.write", "frame_log.short_write", "frame_log.sync"] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_storage_mode(mode.clone())
                .with_fsync(true)
                .with_audit_events(false)
                .with_sync_outbox(false);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            let log = root
                .path()
                .join(DEFAULT_EVENTS_DIR)
                .join(DEFAULT_EVENTS_SEGMENT);
            let fault = arm_io_failure(site, &log, 1, false);
            let tx = db.begin_transaction().unwrap();
            let id = tx
                .buffer_broker_publish("q", json!({"committed": 1}), PublishOptions::default())
                .unwrap();
            tx.commit().unwrap();
            assert!(matches!(
                db.durability_state(),
                DurabilityState::WriteFailed(_)
            ));
            assert!(matches!(
                db.checkpoint_for_resume(),
                Err(BicDbError::WriteFailed { .. })
            ));
            assert!(matches!(
                db.broker().publish("q", json!(2), serde_json::Value::Null),
                Err(BicDbError::WriteFailed { .. })
            ));
            assert!(matches!(
                db.broker().consume("q", "blocked", "worker", consumed()),
                Err(BicDbError::WriteFailed { .. })
            ));
            drop(fault);
            drop(db);
            for reopen in 0..2 {
                let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                let batch = db
                    .broker()
                    .consume("q", &format!("reopen-{reopen}"), "worker", consumed())
                    .unwrap();
                assert_eq!(batch.len(), 1, "{mode:?} {site} reopen {reopen}");
                assert_eq!(batch[0].message_id, id);
            }
        }
    }
}

#[test]
fn idempotency_aliases_stay_trimmed_but_new_commits_can_reuse_the_key() {
    use bicdb_core::{DEFAULT_EVENTS_DIR, DEFAULT_EVENTS_SEGMENT};
    use bicdb_page::test_faults::arm_io_failure;
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        let options = PublishOptions {
            idempotency_key: Some("business-key".into()),
            ..PublishOptions::default()
        };
        let mut ids = Vec::new();
        for value in [1, 2] {
            let tx = db.begin_transaction().unwrap();
            ids.push(
                tx.buffer_broker_publish("q", json!(value), options.clone())
                    .unwrap(),
            );
            tx.commit().unwrap();
        }
        let batch = db.broker().consume("q", "g", "w", consumed()).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].message_id, ids[0]);
        db.broker().ack("q", "g", "w", ids[0]).unwrap();
        db.broker().trim("q").unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        assert!(db
            .broker()
            .consume("q", "g", "w", consumed())
            .unwrap()
            .is_empty());
        // A fresh committed publication may reuse the key after retention,
        // even if it crashes before reaching the broker. Old aliases must
        // not be confused with this new transaction's distinct UUID.
        let log = root
            .path()
            .join(DEFAULT_EVENTS_DIR)
            .join(DEFAULT_EVENTS_SEGMENT);
        let fault = arm_io_failure("frame_log.write", &log, 1, false);
        let tx = db.begin_transaction().unwrap();
        let fresh = tx.buffer_broker_publish("q", json!(3), options).unwrap();
        tx.commit().unwrap();
        drop(fault);
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let batch = db.broker().consume("q", "g", "w", consumed()).unwrap();
        assert_eq!(batch.len(), 1, "{mode:?}: {batch:?}");
        assert_eq!(batch[0].message_id, fresh);
        assert_eq!(batch[0].payload, json!(3));
    }
}

fn record_acknowledgments(root: &std::path::Path, rows: &[&str]) {
    use std::io::Write;
    let mut ledger = std::fs::File::create(root.join("acknowledged.json")).unwrap();
    ledger
        .write_all(&serde_json::to_vec(rows).unwrap())
        .unwrap();
    ledger.sync_all().unwrap();
    #[cfg(unix)]
    std::fs::File::open(root).unwrap().sync_all().unwrap();
}

#[test]
#[ignore = "out-of-process crash harness child; invoked by the parent matrix"]
fn broker_crash_child() {
    let root = std::path::PathBuf::from(std::env::var("BICDB_BROKER_CHILD_ROOT").unwrap());
    let mode = if std::env::var("BICDB_BROKER_CHILD_MODE").unwrap() == "paged" {
        StorageMode::ServerPaged
    } else {
        StorageMode::EmbeddedMemory
    };
    if std::env::var("BICDB_BROKER_CHILD_ACTION").unwrap() == "reopen" {
        let _db = BicDb::open_with_config(root.join("db"), config(mode)).unwrap();
        panic!("the reopen crash point was not reached");
    }
    let trim_case = std::env::var("BICDB_BROKER_CHILD_ACTION").unwrap() == "trim";
    let mut db = BicDb::open_with_config(root.join("db"), config(mode)).unwrap();
    db.create_collection("items").unwrap();
    for row in ["first", "second"] {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new(row)).unwrap();
        let key = if row == "first" || trim_case {
            "same"
        } else {
            "fresh"
        };
        tx.buffer_broker_publish(
            "q",
            json!({"row": row}),
            PublishOptions {
                idempotency_key: Some(key.into()),
                ..PublishOptions::default()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        record_acknowledgments(
            &root,
            if row == "first" {
                &["first"]
            } else {
                &["first", "second"]
            },
        );
        if row == "first" && !trim_case {
            let messages = db.broker().consume("q", "g", "w", consumed()).unwrap();
            assert_eq!(messages.len(), 1);
            db.broker()
                .ack("q", "g", "w", messages[0].message_id)
                .unwrap();
            db.broker().trim("q").unwrap();
        }
    }
    let messages = db.broker().consume("q", "g", "w", consumed()).unwrap();
    assert_eq!(messages.len(), 1);
    db.broker()
        .ack("q", "g", "w", messages[0].message_id)
        .unwrap();
    db.broker().trim("q").unwrap();
    panic!("the commit/retention crash point was not reached");
}

fn crash_child(root: &std::path::Path, mode: &str, action: &str, point: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "broker_crash_child", "--nocapture"])
        .env("BICDB_BROKER_CHILD_ROOT", root)
        .env("BICDB_BROKER_CHILD_MODE", mode)
        .env("BICDB_BROKER_CHILD_ACTION", action)
        .env("BICDB_TEST_CRASH_AT", point)
        .env("BICDB_TEST_WAL_VOLATILE_TAIL", "1")
        .output()
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGABRT),
            "{mode} {point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[cfg(not(unix))]
    assert!(!output.status.success(), "{mode} {point}");
}

#[test]
fn crashes_reconcile_acknowledged_rows_and_publications_across_retention_and_reopen() {
    for (name, mode) in [
        ("embedded", StorageMode::EmbeddedMemory),
        ("paged", StorageMode::ServerPaged),
    ] {
        for point in [
            "commit.before_broker_publish:2",
            "commit.after_broker_publish:2",
            "broker.trim.before_rewrite:1",
            "broker.trim.after_rewrite:1",
        ] {
            let root = tempfile::tempdir().unwrap();
            let trim_case = point.starts_with("broker.trim");
            crash_child(
                root.path(),
                name,
                if trim_case { "trim" } else { "commit" },
                point,
            );
            let acknowledged: Vec<String> = serde_json::from_slice(
                &std::fs::read(root.path().join("acknowledged.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(acknowledged.len(), if trim_case { 2 } else { 1 });
            // Interrupt republication too, after both retained decisions have
            // been examined and the missing second publication was appended.
            if point.starts_with("commit.before") {
                crash_child(
                    root.path(),
                    name,
                    "reopen",
                    "open.broker_republish.after_event:2",
                );
            }
            for reopen in 0..2 {
                let mut db =
                    BicDb::open_with_config(root.path().join("db"), config(mode.clone())).unwrap();
                for row in &acknowledged {
                    assert!(
                        db.get("items", row).unwrap().is_some(),
                        "{name} {point} lost acknowledged {row}"
                    );
                }
                assert_eq!(
                    db.scan_collection("items").unwrap().len(),
                    2,
                    "{name} {point}"
                );
                if trim_case {
                    assert!(
                        db.broker()
                            .consume("q", "g", "w", consumed())
                            .unwrap()
                            .is_empty(),
                        "{name} {point} resurrected an acknowledged message"
                    );
                    let retained = db
                        .broker()
                        .stats()
                        .unwrap()
                        .queues
                        .into_iter()
                        .find(|queue| queue.queue == "q")
                        .unwrap()
                        .retained_messages;
                    assert_eq!(
                        retained,
                        usize::from(point.contains("before_rewrite")),
                        "{name} {point}"
                    );
                } else {
                    let batch = db
                        .broker()
                        .consume("q", &format!("observer-{reopen}"), "w", consumed())
                        .unwrap();
                    assert_eq!(batch.len(), 1, "{name} {point}");
                    assert_eq!(batch[0].payload, json!({"row": "second"}));
                }
            }
        }
    }
}
