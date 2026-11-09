//! Snapshot and subscription reads must report a recovery fence, including
//! empty reads and events already queued before the fence arrived.
use bicdb_core::{BicDb, BicDbError, DbConfig, Event, StorageMode};
use serde_json::json;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

fn fenced<T>(result: bicdb_core::Result<T>) {
    assert!(matches!(result, Err(BicDbError::WriteFailed { .. })));
}

#[test]
fn every_event_and_broker_snapshot_reports_the_fence_then_recovers() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(storage);
        let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
        db.events_mut()
            .append(Event::new("events", "created", json!({"id":1})))
            .unwrap();
        db.checkpoint_for_resume().unwrap();
        let id = db.events().read("events").unwrap()[0].event.id;
        db.durability_health_for_test()
            .record_failure("test.read_fence", &"injected");
        fenced(db.events().read("events"));
        fenced(db.events().read("absent"));
        fenced(db.events().read_since(0));
        fenced(db.events().read_stream_since("events", 0));
        fenced(db.events().replay("events"));
        fenced(db.events().replay_from("events", 0));
        fenced(db.events().stream_head_offset("events"));
        fenced(db.events().contains_event(&id));
        fenced(db.events_mut().queue("empty").consume(0));
        fenced(db.events_mut().subscribe("events", |_| {}));
        fenced(db.events_mut().subscribe_prefix("events", |_| {}));
        fenced(db.events_mut().subscribe_async("events", |_| {}));
        fenced(db.export_events_since(0));
        fenced(db.pending_changes());
        fenced(db.sync_status());
        let broker = db.broker();
        fenced(broker.peek("absent", 0, 0));
        fenced(broker.stats());
        fenced(broker.group_info("absent", "g"));
        fenced(broker.queue_config("absent"));
        fenced(broker.dead_letters("absent", "g"));
        fenced(broker.dead_letter_attempts("absent", "g"));
        fenced(broker.has_deliverable("absent", "g"));
        fenced(broker.delivery_snapshot("absent", "g", 0, 1, 1024));
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config).unwrap();
        assert_eq!(db.events().read("events").unwrap().len(), 1);
        assert!(db.events().contains_event(&id).unwrap());
    }
}

#[test]
fn queued_and_idle_async_subscriptions_report_errors_without_delivering_queued_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = gate.clone();
    let (tx, rx) = mpsc::channel();
    db.events_mut()
        .subscribe_async("events", move |result| {
            let first = result.is_ok();
            tx.send(result.map(|event| event.event.event_type)).unwrap();
            if first {
                let (lock, wake) = &*callback_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            }
        })
        .unwrap();
    let (idle_tx, idle_rx) = mpsc::channel();
    db.events_mut()
        .subscribe_async("idle", move |event| {
            idle_tx.send(event).unwrap();
        })
        .unwrap();
    db.events_mut()
        .append(Event::new("events", "first", json!({})))
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap(),
        "first"
    );
    db.events_mut()
        .append(Event::new("events", "queued", json!({})))
        .unwrap();
    db.durability_health_for_test()
        .record_failure("test.async_fence", &"injected");
    let (lock, wake) = &*gate;
    *lock.lock().unwrap() = true;
    wake.notify_all();
    fenced(rx.recv_timeout(Duration::from_secs(5)).unwrap());
    fenced(idle_rx.recv_timeout(Duration::from_secs(5)).unwrap());
    assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
}
