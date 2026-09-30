use bicdb_core::{BicDb, ConsumeOptions, NackOptions};
use serde_json::json;

#[test]
fn expired_or_previous_attempt_cannot_settle_reused_consumer_name() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.broker()
        .publish("jobs", json!({"id":1}), json!({}))
        .unwrap();
    let options = ConsumeOptions {
        max_messages: 1,
        visibility_timeout_ms: 20,
    };
    let first = db
        .broker()
        .consume("jobs", "workers", "same-worker", options)
        .unwrap()
        .pop()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(db.broker().ack_delivery("same-worker", &first).is_err());
    let second = db
        .broker()
        .consume("jobs", "workers", "same-worker", ConsumeOptions::default())
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.attempts, first.attempts + 1);
    assert!(db.broker().ack_delivery("same-worker", &first).is_err());
    assert!(db
        .broker()
        .nack_delivery("same-worker", &first, NackOptions::default())
        .is_err());
    db.broker().ack_delivery("same-worker", &second).unwrap();
    assert!(db
        .broker()
        .consume("jobs", "workers", "next-worker", ConsumeOptions::default())
        .unwrap()
        .is_empty());
}
