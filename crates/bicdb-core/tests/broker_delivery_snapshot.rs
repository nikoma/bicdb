use bicdb_core::{BicDb, BrokerDeliveryState, ConsumeOptions, NackOptions};
use serde_json::json;

#[test]
fn delivery_export_preserves_group_state_without_consuming_or_acknowledging() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for value in 0..4 {
        db.broker()
            .publish("work", json!(value), json!({}))
            .unwrap();
    }
    let messages = db
        .broker()
        .consume(
            "work",
            "group",
            "worker",
            ConsumeOptions {
                max_messages: 3,
                visibility_timeout_ms: 60_000,
            },
        )
        .unwrap();
    db.broker().ack_delivery("worker", &messages[0]).unwrap();
    db.broker()
        .nack_delivery(
            "worker",
            &messages[1],
            NackOptions {
                requeue: true,
                delay_ms: Some(5000),
                error: None,
            },
        )
        .unwrap();
    let before = db.broker().group_info("work", "group").unwrap();
    let snapshot = db
        .broker()
        .delivery_snapshot("work", "group", 0, 4, 1024 * 1024)
        .unwrap();
    assert_eq!(snapshot.len(), 4);
    assert_eq!(snapshot[0].state, BrokerDeliveryState::Settled);
    assert!(matches!(
        snapshot[1].state,
        BrokerDeliveryState::Available { .. }
    ));
    assert_eq!(snapshot[1].attempts, 1);
    assert_eq!(
        snapshot[2].state,
        BrokerDeliveryState::InFlight {
            consumer: "worker".into(),
            visibility_deadline: messages[2].visibility_deadline
        }
    );
    assert_eq!(snapshot[2].delivered_at, messages[2].delivered_at);
    assert_eq!(snapshot[3].state, BrokerDeliveryState::Unseen);
    assert_eq!(db.broker().group_info("work", "group").unwrap(), before);
    assert_eq!(
        db.broker()
            .delivery_snapshot("work", "group", 1, 2, 1024 * 1024)
            .unwrap(),
        snapshot[1..3]
    );
    assert_eq!(
        db.broker()
            .delivery_snapshot("work", "absent", 0, 1, 1024 * 1024)
            .unwrap()[0]
            .state,
        BrokerDeliveryState::Unseen
    );
    assert!(db.broker().group_info("work", "absent").is_none());
    assert!(db
        .broker()
        .delivery_snapshot("work", "group", 0, 4, 1)
        .is_err());
    let prefix = db
        .broker()
        .delivery_snapshot("work", "group", 0, 4, 1800)
        .unwrap();
    assert_eq!(prefix, snapshot[..1]);
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        db.broker()
            .delivery_snapshot("work", "group", 0, 4, 1024 * 1024)
            .unwrap(),
        snapshot
    );
    db.broker().ack_delivery("worker", &messages[2]).unwrap();
}
