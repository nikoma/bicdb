use bicdb_core::{
    BicDb, ConsumeOptions, DbConfig, NackOptions, PublishOptions, DEFAULT_MAX_ATTEMPTS,
};
use serde_json::{json, Value};

fn open_temp() -> (tempfile::TempDir, BicDb) {
    let temp = tempfile::tempdir().expect("tempdir");
    let db = BicDb::open_with_config(temp.path(), DbConfig::default()).expect("open db");
    (temp, db)
}

fn opts(max_messages: usize, visibility_timeout_ms: u64) -> ConsumeOptions {
    ConsumeOptions {
        max_messages,
        visibility_timeout_ms,
    }
}

#[test]
fn groups_fan_out_and_consumers_in_a_group_compete() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker
        .publish(
            "commerce.orders",
            json!({"order": 1}),
            json!({"tenant": "t1"}),
        )
        .unwrap();
    assert_eq!(receipt.sequence, 0);
    assert!(!receipt.deduplicated);

    // Two groups each receive the message once.
    let enrollment = broker
        .consume(
            "commerce.orders",
            "sis-enrollment",
            "worker-1",
            opts(10, 30_000),
        )
        .unwrap();
    let billing = broker
        .consume("commerce.orders", "billing", "worker-1", opts(10, 30_000))
        .unwrap();
    assert_eq!(enrollment.len(), 1);
    assert_eq!(billing.len(), 1);
    assert_eq!(enrollment[0].message_id, receipt.message_id);
    assert_eq!(billing[0].message_id, receipt.message_id);
    assert_eq!(enrollment[0].payload, json!({"order": 1}));
    assert_eq!(enrollment[0].headers, json!({"tenant": "t1"}));
    assert_eq!(enrollment[0].attempts, 1);
    assert_eq!(enrollment[0].max_attempts, DEFAULT_MAX_ATTEMPTS);

    // A second consumer of the same group gets nothing while the message is
    // in flight.
    let competitor = broker
        .consume(
            "commerce.orders",
            "sis-enrollment",
            "worker-2",
            opts(10, 30_000),
        )
        .unwrap();
    assert!(competitor.is_empty());
}

#[test]
fn ack_is_terminal_for_the_group() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    let batch = broker.consume("q", "g", "w1", opts(1, 0)).unwrap();
    assert_eq!(batch.len(), 1);
    broker.ack("q", "g", "w1", receipt.message_id).unwrap();

    // Even with an expired visibility timeout, an acked message never comes
    // back, and a second ack errors.
    assert!(broker
        .consume("q", "g", "w1", opts(10, 0))
        .unwrap()
        .is_empty());
    assert!(broker.ack("q", "g", "w1", receipt.message_id).is_err());

    let info = broker.group_info("q", "g").unwrap();
    assert_eq!(info.last_delivered_sequence, Some(0));
    assert_eq!(info.in_flight, 0);
    assert_eq!(info.pending_redelivery, 0);
}

#[test]
fn nack_requeue_redelivers_with_incremented_attempts() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    let first = broker.consume("q", "g", "w1", opts(1, 30_000)).unwrap();
    assert_eq!(first[0].attempts, 1);

    broker
        .nack(
            "q",
            "g",
            "w1",
            receipt.message_id,
            NackOptions {
                requeue: true,
                delay_ms: None,
                error: Some("temporary failure".into()),
            },
        )
        .unwrap();

    // Another consumer of the group picks the retry up.
    let second = broker.consume("q", "g", "w2", opts(1, 30_000)).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].message_id, receipt.message_id);
    assert_eq!(second[0].attempts, 2);

    // The original consumer no longer owns the delivery.
    assert!(broker.ack("q", "g", "w1", receipt.message_id).is_err());
    broker.ack("q", "g", "w2", receipt.message_id).unwrap();
}

#[test]
fn nack_requeue_delay_defers_redelivery() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    broker.consume("q", "g", "w1", opts(1, 30_000)).unwrap();
    broker
        .nack(
            "q",
            "g",
            "w1",
            receipt.message_id,
            NackOptions {
                requeue: true,
                delay_ms: Some(60_000),
                error: None,
            },
        )
        .unwrap();

    // Still delayed: nothing deliverable.
    assert!(broker
        .consume("q", "g", "w1", opts(10, 0))
        .unwrap()
        .is_empty());
    let info = broker.group_info("q", "g").unwrap();
    assert_eq!(info.pending_redelivery, 1);
}

#[test]
fn visibility_timeout_expiry_redelivers_to_another_consumer() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    // Zero visibility timeout: the delivery is immediately reclaimable.
    let first = broker.consume("q", "g", "w1", opts(1, 0)).unwrap();
    assert_eq!(first[0].attempts, 1);

    let second = broker.consume("q", "g", "w2", opts(1, 30_000)).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].message_id, receipt.message_id);
    assert_eq!(second[0].attempts, 2);
}

#[test]
fn exhausted_attempts_dead_letter_via_nack_and_via_expiry() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    // (a) nack path: max_attempts=2, nack twice with requeue.
    let receipt = broker
        .publish_with(
            "q",
            json!({"n": 1}),
            PublishOptions {
                max_attempts: Some(2),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    for consumer in ["w1", "w2"] {
        let batch = broker.consume("q", "g", consumer, opts(1, 30_000)).unwrap();
        assert_eq!(batch.len(), 1, "attempt for {consumer}");
        broker
            .nack(
                "q",
                "g",
                consumer,
                receipt.message_id,
                NackOptions {
                    requeue: true,
                    delay_ms: None,
                    error: Some("boom".into()),
                },
            )
            .unwrap();
    }
    // Second nack hit attempts == max_attempts and dead-lettered directly.
    assert!(broker
        .consume("q", "g", "w3", opts(10, 0))
        .unwrap()
        .is_empty());
    let dead = broker.dead_letters("q", "g");
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].event.payload, json!({"n": 1}));
    let meta = dead[0].event.metadata.get("broker").unwrap();
    assert_eq!(meta.get("attempts").and_then(Value::as_u64), Some(2));
    assert_eq!(meta.get("last_error").and_then(Value::as_str), Some("boom"));

    // (b) expiry path: max_attempts=1, delivery expires, next consume
    // dead-letters instead of redelivering.
    let receipt2 = broker
        .publish_with(
            "q2",
            json!({"n": 2}),
            PublishOptions {
                max_attempts: Some(1),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    let batch = broker.consume("q2", "g", "w1", opts(1, 0)).unwrap();
    assert_eq!(batch.len(), 1);
    assert!(broker
        .consume("q2", "g", "w2", opts(10, 0))
        .unwrap()
        .is_empty());
    let dead = broker.dead_letters("q2", "g");
    assert_eq!(dead.len(), 1);
    let meta = dead[0].event.metadata.get("broker").unwrap();
    assert_eq!(
        meta.get("original_message_id").and_then(Value::as_str),
        Some(receipt2.message_id.to_string().as_str())
    );
}

#[test]
fn nack_without_requeue_dead_letters_immediately() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    broker.consume("q", "g", "w1", opts(1, 30_000)).unwrap();
    broker
        .nack(
            "q",
            "g",
            "w1",
            receipt.message_id,
            NackOptions {
                requeue: false,
                delay_ms: None,
                error: Some("poison".into()),
            },
        )
        .unwrap();

    assert!(broker
        .consume("q", "g", "w1", opts(10, 0))
        .unwrap()
        .is_empty());
    let dead = broker.dead_letters("q", "g");
    assert_eq!(dead.len(), 1);
    let meta = dead[0].event.metadata.get("broker").unwrap();
    assert_eq!(
        meta.get("reason").and_then(Value::as_str),
        Some("nacked without requeue")
    );
}

#[test]
fn idempotency_key_deduplicates_publishes() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let first = broker
        .publish_with(
            "q",
            json!({"n": 1}),
            PublishOptions {
                idempotency_key: Some("evt-42".into()),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    let second = broker
        .publish_with(
            "q",
            json!({"n": 1, "retry": true}),
            PublishOptions {
                idempotency_key: Some("evt-42".into()),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    assert!(second.deduplicated);
    assert_eq!(second.message_id, first.message_id);
    assert_eq!(second.sequence, first.sequence);

    // Same key on a different queue is a distinct message.
    let other_queue = broker
        .publish_with(
            "q-other",
            json!({"n": 1}),
            PublishOptions {
                idempotency_key: Some("evt-42".into()),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    assert!(!other_queue.deduplicated);

    let batch = broker.consume("q", "g", "w1", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].idempotency_key.as_deref(), Some("evt-42"));
}

#[test]
fn delayed_publish_holds_the_head_of_the_queue() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    broker
        .publish_with(
            "q",
            json!({"n": "delayed"}),
            PublishOptions {
                delay_ms: Some(60_000),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    broker
        .publish("q", json!({"n": "ready"}), Value::Null)
        .unwrap();

    // Fresh delivery stops at the first unavailable message so per-group
    // ordering is preserved.
    assert!(broker
        .consume("q", "g", "w1", opts(10, 30_000))
        .unwrap()
        .is_empty());
}

#[test]
fn plain_event_queue_messages_are_broker_consumable() {
    let (_temp, mut db) = open_temp();
    db.queue("legacy")
        .publish(json!({"n": 1}), json!({"src": "old"}))
        .unwrap();

    let mut broker = db.broker();
    let batch = broker
        .consume("legacy", "g", "w1", opts(10, 30_000))
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].payload, json!({"n": 1}));
    assert_eq!(batch[0].headers, Value::Null);
    assert_eq!(batch[0].max_attempts, DEFAULT_MAX_ATTEMPTS);
    broker
        .ack("legacy", "g", "w1", batch[0].message_id)
        .unwrap();
}

#[test]
fn ack_and_nack_enforce_delivery_ownership() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();

    // Unknown delivery.
    assert!(broker.ack("q", "g", "w1", receipt.message_id).is_err());

    broker.consume("q", "g", "w1", opts(1, 30_000)).unwrap();
    // Wrong consumer.
    assert!(broker.ack("q", "g", "w2", receipt.message_id).is_err());
    assert!(broker
        .nack("q", "g", "w2", receipt.message_id, NackOptions::default())
        .is_err());
    // Right consumer.
    broker.ack("q", "g", "w1", receipt.message_id).unwrap();
}

#[test]
fn broker_state_survives_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let (acked_id, inflight_id, undelivered_id) = {
        let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
        let mut broker = db.broker();
        let a = broker
            .publish("q", json!({"n": "acked"}), Value::Null)
            .unwrap();
        let b = broker
            .publish("q", json!({"n": "inflight"}), Value::Null)
            .unwrap();
        let c = broker
            .publish("q", json!({"n": "undelivered"}), Value::Null)
            .unwrap();

        let batch = broker.consume("q", "g", "w1", opts(1, 0)).unwrap();
        assert_eq!(batch[0].message_id, a.message_id);
        broker.ack("q", "g", "w1", a.message_id).unwrap();

        // Delivered with an already-expired visibility timeout, never acked.
        let batch = broker.consume("q", "g", "w1", opts(1, 0)).unwrap();
        assert_eq!(batch[0].message_id, b.message_id);

        (a.message_id, b.message_id, c.message_id)
    };

    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    let mut broker = db.broker();

    let info = broker.group_info("q", "g").unwrap();
    assert_eq!(info.last_delivered_sequence, Some(1));
    assert_eq!(info.in_flight, 1);

    // After reopen: the expired in-flight message redelivers with attempts=2,
    // then the never-delivered message follows; the acked one never returns.
    let batch = broker.consume("q", "g", "w2", opts(10, 30_000)).unwrap();
    let ids: Vec<_> = batch.iter().map(|m| m.message_id).collect();
    assert_eq!(ids, vec![inflight_id, undelivered_id]);
    assert_eq!(batch[0].attempts, 2);
    assert_eq!(batch[1].attempts, 1);
    assert!(!ids.contains(&acked_id));
}

#[test]
fn broker_state_survives_compaction() {
    let (_temp, mut db) = open_temp();
    let (first, second) = {
        let mut broker = db.broker();
        let first = broker
            .publish_with(
                "q",
                json!({"n": 1}),
                PublishOptions {
                    idempotency_key: Some("k1".into()),
                    ..PublishOptions::default()
                },
            )
            .unwrap();
        let second = broker.publish("q", json!({"n": 2}), Value::Null).unwrap();
        let batch = broker.consume("q", "g", "w1", opts(1, 30_000)).unwrap();
        broker.ack("q", "g", "w1", batch[0].message_id).unwrap();
        (first, second)
    };

    db.compact().unwrap();

    let mut broker = db.broker();
    // Sequences and cursors are logical, so compaction must not disturb them.
    let dup = broker
        .publish_with(
            "q",
            json!({"n": 1}),
            PublishOptions {
                idempotency_key: Some("k1".into()),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    assert!(dup.deduplicated);
    assert_eq!(dup.message_id, first.message_id);

    let batch = broker.consume("q", "g", "w1", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].message_id, second.message_id);
    assert_eq!(batch[0].sequence, 1);
}

#[test]
fn consume_validates_names_and_zero_budget() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    assert!(broker.consume("bad/queue", "g", "w", opts(1, 0)).is_err());
    assert!(broker.consume("q", "bad/group", "w", opts(1, 0)).is_err());
    assert!(broker.consume("q", "g", "", opts(1, 0)).is_err());
    assert!(broker
        .publish("bad/queue", Value::Null, Value::Null)
        .is_err());

    broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    assert!(broker
        .consume("q", "g", "w", opts(0, 0))
        .unwrap()
        .is_empty());
}

// ---------- Slice 2: retention, trimming, DLQ lifecycle, queue config ----------

#[test]
fn trim_removes_only_what_every_group_settled_and_preserves_sequences() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    let mut broker = db.broker();

    let receipts: Vec<_> = (0..3)
        .map(|n| broker.publish("q", json!({"n": n}), Value::Null).unwrap())
        .collect();

    // fast group settles all three; slow group settles only the first two.
    let batch = broker.consume("q", "fast", "w", opts(3, 30_000)).unwrap();
    for message in &batch {
        broker.ack("q", "fast", "w", message.message_id).unwrap();
    }
    let batch = broker.consume("q", "slow", "w", opts(2, 30_000)).unwrap();
    for message in &batch {
        broker.ack("q", "slow", "w", message.message_id).unwrap();
    }

    let report = broker.trim("q").unwrap();
    assert_eq!(report.removed_messages, 2);
    assert_eq!(report.through_sequence, Some(1));
    assert!(report.bytes_after < report.bytes_before);
    // Idempotent second pass: nothing more is settled by all groups.
    assert_eq!(broker.trim("q").unwrap().removed_messages, 0);

    // The survivor keeps its original sequence and is still deliverable to
    // the slow group; a new publish continues the sequence counter.
    let batch = broker.consume("q", "slow", "w", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].message_id, receipts[2].message_id);
    assert_eq!(batch[0].sequence, 2);
    let next = broker.publish("q", json!({"n": 3}), Value::Null).unwrap();
    assert_eq!(next.sequence, 3);

    // Durable across reopen: the trim marker carries the floor.
    drop(db);
    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    let mut broker = db.broker();
    let info = broker.group_info("q", "fast").unwrap();
    assert_eq!(info.last_delivered_sequence, Some(2));
    let batch = broker.consume("q", "fast", "w", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].sequence, 3);
    let republished = broker.publish("q", json!({"n": 4}), Value::Null).unwrap();
    assert_eq!(republished.sequence, 4);
}

#[test]
fn trim_rewrites_mixed_delivery_batches_without_replay_ghosts() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    let mut broker = db.broker();

    for n in 0..3 {
        broker.publish("q", json!({"n": n}), Value::Null).unwrap();
    }
    let batch = broker.consume("q", "g", "w", opts(3, 30_000)).unwrap();
    broker.ack("q", "g", "w", batch[0].message_id).unwrap();
    broker.ack("q", "g", "w", batch[1].message_id).unwrap();

    let report = broker.trim("q").unwrap();
    assert_eq!(report.removed_messages, 2);
    let group = broker
        .stats()
        .queues
        .iter()
        .find(|queue| queue.queue == "q")
        .unwrap()
        .groups[0]
        .clone();
    assert_eq!(group.in_flight, 1);
    assert_eq!(group.settled_floor, 2);

    drop(db);
    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    let group = db
        .broker()
        .stats()
        .queues
        .iter()
        .find(|queue| queue.queue == "q")
        .unwrap()
        .groups[0]
        .clone();
    assert_eq!(group.in_flight, 1);
    assert_eq!(group.settled_floor, 2);
}

#[test]
fn retention_policy_trims_unsettled_messages_when_configured() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    for n in 0..3 {
        broker.publish("q", json!({"n": n}), Value::Null).unwrap();
    }
    // A group exists but has settled nothing; without config nothing trims.
    assert!(broker
        .consume("q", "g", "w", opts(0, 0))
        .unwrap()
        .is_empty());
    assert_eq!(broker.trim("q").unwrap().removed_messages, 0);

    broker
        .configure_queue(
            "q",
            bicdb_core::QueueConfig {
                retention_max_messages: Some(1),
                ..bicdb_core::QueueConfig::default()
            },
        )
        .unwrap();
    assert_eq!(
        broker.queue_config("q").unwrap().retention_max_messages,
        Some(1)
    );

    let report = broker.trim("q").unwrap();
    assert_eq!(report.removed_messages, 2);
    assert_eq!(report.through_sequence, Some(1));

    // The group's cursor skips the trimmed prefix; only the survivor arrives.
    let batch = broker.consume("q", "g", "w", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].sequence, 2);
    assert_eq!(batch[0].payload, json!({"n": 2}));
}

#[test]
fn trim_drops_in_flight_deliveries_of_policy_trimmed_messages() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let first = broker.publish("q", json!({"n": 0}), Value::Null).unwrap();
    broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    let batch = broker.consume("q", "g", "w", opts(1, 30_000)).unwrap();
    assert_eq!(batch[0].message_id, first.message_id);

    broker
        .configure_queue(
            "q",
            bicdb_core::QueueConfig {
                retention_max_messages: Some(1),
                ..bicdb_core::QueueConfig::default()
            },
        )
        .unwrap();
    let report = broker.trim("q").unwrap();
    assert_eq!(report.removed_messages, 1);

    // The in-flight delivery of the trimmed message is gone: acking it errors,
    // and it never redelivers.
    assert!(broker.ack("q", "g", "w", first.message_id).is_err());
    let batch = broker.consume("q", "g", "w", opts(10, 0)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].sequence, 1);
}

#[test]
fn redrive_republishes_dead_letters_once() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    let original = broker
        .publish("q", json!({"n": 1}), json!({"tenant": "t1"}))
        .unwrap();
    broker.consume("q", "g", "w", opts(1, 30_000)).unwrap();
    broker
        .nack(
            "q",
            "g",
            "w",
            original.message_id,
            NackOptions {
                requeue: false,
                delay_ms: None,
                error: Some("poison".into()),
            },
        )
        .unwrap();
    assert_eq!(broker.dead_letters("q", "g").len(), 1);

    let receipts = broker.redrive_dead_letters("q", "g", 10).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_ne!(receipts[0].message_id, original.message_id);

    // Redriven copy arrives as a fresh message with attempts reset and the
    // original headers.
    let batch = broker.consume("q", "g", "w", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].message_id, receipts[0].message_id);
    assert_eq!(batch[0].attempts, 1);
    assert_eq!(batch[0].headers, json!({"tenant": "t1"}));

    // A second redrive pass is a no-op; the DLQ copy stays for audit.
    assert!(broker
        .redrive_dead_letters("q", "g", 10)
        .unwrap()
        .is_empty());
    assert_eq!(broker.dead_letters("q", "g").len(), 1);
}

#[test]
fn purge_dead_letters_empties_the_dlq() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    let mut broker = db.broker();

    let receipt = broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    broker.consume("q", "g", "w", opts(1, 30_000)).unwrap();
    broker
        .nack("q", "g", "w", receipt.message_id, NackOptions::default())
        .unwrap();
    assert_eq!(broker.dead_letters("q", "g").len(), 1);

    assert_eq!(broker.purge_dead_letters("q", "g").unwrap(), 1);
    assert!(broker.dead_letters("q", "g").is_empty());
    assert!(broker
        .redrive_dead_letters("q", "g", 10)
        .unwrap()
        .is_empty());
    assert_eq!(broker.purge_dead_letters("q", "g").unwrap(), 0);

    // Durable across reopen.
    drop(db);
    let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
    assert!(db.broker().dead_letters("q", "g").is_empty());
}

#[test]
fn queue_config_default_max_attempts_applies_to_publishes() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    broker
        .configure_queue(
            "q",
            bicdb_core::QueueConfig {
                default_max_attempts: Some(3),
                ..bicdb_core::QueueConfig::default()
            },
        )
        .unwrap();
    broker.publish("q", json!({"n": 1}), Value::Null).unwrap();
    let batch = broker.consume("q", "g", "w", opts(1, 30_000)).unwrap();
    assert_eq!(batch[0].max_attempts, 3);

    // Explicit per-message setting still wins.
    broker
        .publish_with(
            "q",
            json!({"n": 2}),
            PublishOptions {
                max_attempts: Some(7),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    broker.ack("q", "g", "w", batch[0].message_id).unwrap();
    let batch = broker.consume("q", "g", "w", opts(1, 30_000)).unwrap();
    assert_eq!(batch[0].max_attempts, 7);
}

// ---------- Slice 4: drain pump, deliverability probe, stats ----------

#[test]
fn drain_pumps_acks_and_retries_through_a_handler() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    for n in 0..5 {
        broker
            .publish_with(
                "q",
                json!({"n": n}),
                PublishOptions {
                    max_attempts: Some(2),
                    ..PublishOptions::default()
                },
            )
            .unwrap();
    }

    // Handler rejects odd payloads on every attempt.
    let mut seen = Vec::new();
    let report = broker
        .drain(
            "q",
            "g",
            "w",
            bicdb_core::DrainOptions {
                batch_size: 2,
                visibility_timeout_ms: 30_000,
                max_messages: 0,
                nack_delay_ms: None,
            },
            |message| {
                let n = message.payload["n"].as_i64().unwrap();
                seen.push((n, message.attempts));
                if n % 2 == 0 {
                    Ok(())
                } else {
                    Err(format!("odd payload {n}"))
                }
            },
        )
        .unwrap();

    // 3 evens acked on attempt 1; 2 odds retried once (attempt 2) and then
    // dead-lettered by the second nack (attempts == max_attempts == 2).
    assert_eq!(report.acked, 3);
    assert_eq!(report.nacked, 4);
    assert_eq!(report.delivered, 7);
    assert_eq!(broker.dead_letters("q", "g").len(), 2);
    assert!(!broker.has_deliverable("q", "g"));
    // Every rejected message was retried exactly once.
    assert_eq!(
        seen.iter().filter(|(_, attempts)| *attempts == 2).count(),
        2
    );
}

#[test]
fn drain_respects_max_messages() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();
    for n in 0..5 {
        broker.publish("q", json!({"n": n}), Value::Null).unwrap();
    }
    let report = broker
        .drain(
            "q",
            "g",
            "w",
            bicdb_core::DrainOptions {
                max_messages: 2,
                ..bicdb_core::DrainOptions::default()
            },
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(report.delivered, 2);
    assert_eq!(report.acked, 2);
    assert!(broker.has_deliverable("q", "g"));
}

#[test]
fn has_deliverable_tracks_availability_and_delays() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    assert!(!broker.has_deliverable("q", "g"));
    broker
        .publish_with(
            "q",
            json!({"n": "later"}),
            PublishOptions {
                delay_ms: Some(60_000),
                ..PublishOptions::default()
            },
        )
        .unwrap();
    // Head-of-line delayed message: nothing deliverable yet, even for a
    // group that does not exist.
    assert!(!broker.has_deliverable("q", "g"));

    broker.publish("q2", json!({"n": 1}), Value::Null).unwrap();
    assert!(broker.has_deliverable("q2", "g"));
    let batch = broker.consume("q2", "g", "w", opts(1, 30_000)).unwrap();
    assert!(!broker.has_deliverable("q2", "g"));
    broker
        .nack(
            "q2",
            "g",
            "w",
            batch[0].message_id,
            NackOptions {
                requeue: true,
                delay_ms: None,
                error: None,
            },
        )
        .unwrap();
    assert!(broker.has_deliverable("q2", "g"));
}

#[test]
fn stats_snapshot_reports_queues_and_groups() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    for n in 0..3 {
        broker.publish("q", json!({"n": n}), Value::Null).unwrap();
    }
    let batch = broker.consume("q", "g", "w", opts(2, 30_000)).unwrap();
    broker.ack("q", "g", "w", batch[0].message_id).unwrap();
    broker
        .nack("q", "g", "w", batch[1].message_id, NackOptions::default())
        .unwrap(); // dead-letters (no requeue)

    let stats = broker.stats();
    let queue = stats
        .queues
        .iter()
        .find(|queue| queue.queue == "q")
        .unwrap();
    assert_eq!(queue.retained_messages, 3);
    assert_eq!(queue.next_sequence, 3);
    assert!(!queue.configured);
    assert_eq!(queue.groups.len(), 1);
    let group = &queue.groups[0];
    assert_eq!(group.in_flight, 0);
    assert_eq!(group.pending_redelivery, 0);
    assert_eq!(group.settled_floor, 2); // seq 0 acked, seq 1 dead-lettered
    assert_eq!(group.lag, 1); // seq 2 undelivered
    assert_eq!(group.dead_letters, 1);
}

// ---------- Slice 5: automatic trimming ----------

#[test]
fn publish_trims_opportunistically_past_twice_the_retention_bound() {
    let (_temp, mut db) = open_temp();
    let mut broker = db.broker();

    broker
        .configure_queue(
            "q",
            bicdb_core::QueueConfig {
                retention_max_messages: Some(2),
                ..bicdb_core::QueueConfig::default()
            },
        )
        .unwrap();
    for n in 0..6 {
        broker.publish("q", json!({"n": n}), Value::Null).unwrap();
    }
    // The 6th publish found 5 retained (> 2x2) and trimmed down to the bound
    // before appending.
    let stats = broker.stats();
    let queue = stats.queues.iter().find(|q| q.queue == "q").unwrap();
    assert_eq!(queue.retained_messages, 3);
    assert_eq!(queue.next_sequence, 6);
}

#[test]
fn database_compaction_enforces_configured_retention() {
    let (_temp, mut db) = open_temp();
    {
        let mut broker = db.broker();
        broker
            .configure_queue(
                "q",
                bicdb_core::QueueConfig {
                    retention_max_messages: Some(1),
                    ..bicdb_core::QueueConfig::default()
                },
            )
            .unwrap();
        for n in 0..3 {
            broker.publish("q", json!({"n": n}), Value::Null).unwrap();
        }
        // An unconfigured queue is untouched by automatic trimming.
        for n in 0..3 {
            broker
                .publish("keep", json!({"n": n}), Value::Null)
                .unwrap();
        }
    }

    db.compact().unwrap();

    let mut broker = db.broker();
    let stats = broker.stats();
    let trimmed = stats.queues.iter().find(|q| q.queue == "q").unwrap();
    let kept = stats.queues.iter().find(|q| q.queue == "keep").unwrap();
    assert_eq!(trimmed.retained_messages, 1);
    assert_eq!(kept.retained_messages, 3);
    let batch = broker.consume("q", "g", "w", opts(10, 30_000)).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].sequence, 2);
}
