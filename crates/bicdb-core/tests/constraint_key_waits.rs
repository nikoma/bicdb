use bicdb_core::{BicDb, BicDbError, CancellationToken, MutationActor};
use std::{
    collections::BTreeSet,
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < deadline, "waiter did not enter its wait");
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn cancelled_constraint_wait_releases_graph_edges_without_stealing_owner_keys() {
    let _serial = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(BicDb::open(dir.path()).unwrap());
    let owner = db.begin_transaction().unwrap();
    owner.reserve_integrity_key("u", "b").unwrap();
    let waiter = db.begin_transaction().unwrap();
    waiter.reserve_integrity_key("u", "a").unwrap();
    let cancel = CancellationToken::uncancelable();
    let child = cancel.clone();
    let (done, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = waiter.reserve_integrity_key_cancellable("u", "b", &child);
        done.send((waiter, result)).unwrap();
    });
    wait_until(bicdb_core::row_lock_waits_active);
    cancel.cancel();
    let (waiter, result) = received.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(result, Err(BicDbError::QueryCanceled)));
    worker.join().unwrap();
    let probe = db.begin_transaction().unwrap();
    assert!(matches!(
        probe.reserve_integrity_key("u", "b"),
        Err(BicDbError::TransactionConflict(_))
    ));
    probe.rollback().unwrap();
    // If the cancelled A->B edge leaked, this legitimate B->A wait would be
    // falsely declared a deadlock even though A is about to roll back.
    let (done, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result =
            owner.reserve_integrity_key_cancellable("u", "a", &CancellationToken::uncancelable());
        done.send(result).unwrap();
    });
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    waiter.rollback().unwrap();
    received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
}
#[test]
fn constraint_cycle_is_detected_and_winner_can_finish() {
    let _serial = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(BicDb::open(dir.path()).unwrap());
    let a = db.begin_transaction().unwrap();
    let b = db.begin_transaction().unwrap();
    a.reserve_integrity_key("u", "a").unwrap();
    b.reserve_integrity_key("u", "b").unwrap();
    let (done, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result =
            a.reserve_integrity_key_cancellable("u", "b", &CancellationToken::uncancelable());
        done.send(result).unwrap();
    });
    wait_until(bicdb_core::row_lock_waits_active);
    assert!(matches!(
        b.reserve_integrity_key_cancellable("u", "a", &CancellationToken::uncancelable()),
        Err(BicDbError::TransactionDeadlock(_))
    ));
    b.rollback().unwrap();
    received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    let fresh = db.begin_transaction().unwrap();
    fresh.reserve_integrity_key("u", "a").unwrap();
    fresh.reserve_integrity_key("u", "b").unwrap();
}
#[test]
fn savepoint_release_wakes_waiter_without_releasing_earlier_key() {
    let _serial = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(BicDb::open(dir.path()).unwrap());
    let mut owner = db.begin_transaction().unwrap();
    owner.reserve_integrity_key("u", "kept").unwrap();
    let mark = owner.rollback_mark();
    owner.reserve_integrity_key("u", "released").unwrap();
    let waiter = db.begin_transaction().unwrap();
    let (done, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        done.send(waiter.reserve_integrity_key_cancellable(
            "u",
            "released",
            &CancellationToken::uncancelable(),
        ))
        .unwrap();
    });
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    owner.rollback_to_mark(mark).unwrap();
    received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    let probe = db.begin_transaction().unwrap();
    assert!(matches!(
        probe.reserve_integrity_key("u", "kept"),
        Err(BicDbError::TransactionConflict(_))
    ));
}
#[test]
fn exclusive_wait_observes_all_shared_reference_owners() {
    let _serial = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(BicDb::open(dir.path()).unwrap());
    let a = db.begin_transaction().unwrap();
    let b = db.begin_transaction().unwrap();
    a.reserve_integrity_key_shared("u", "key").unwrap();
    b.reserve_integrity_key_shared("u", "key").unwrap();
    let waiter = db.begin_transaction().unwrap();
    let (done, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        done.send(waiter.reserve_integrity_key_cancellable(
            "u",
            "key",
            &CancellationToken::uncancelable(),
        ))
        .unwrap();
    });
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    a.rollback().unwrap();
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    b.rollback().unwrap();
    received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
}
#[test]
fn query_deadline_and_native_actor_deadline_bound_key_waits() {
    let _serial = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    let owner = db.begin_transaction().unwrap();
    owner.reserve_integrity_key("u", "key").unwrap();
    let waiter = db.begin_transaction().unwrap();
    let token = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_millis(50));
    assert!(matches!(
        waiter.reserve_integrity_key_cancellable("u", "key", &token),
        Err(BicDbError::QueryTimedOut)
    ));
    let actor = MutationActor {
        actor_id: "a".into(),
        roles: BTreeSet::new(),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "test".into(),
        originating_resource: None,
        originating_action: None,
        trace_id: "deadline".into(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 50,
    };
    let waiter = db.begin_application_transaction(actor).unwrap();
    assert!(matches!(
        waiter.reserve_integrity_key_cancellable("u", "key", &CancellationToken::uncancelable()),
        Err(BicDbError::QueryTimedOut)
    ));
}
#[test]
fn durability_fence_terminates_a_constraint_key_wait() {
    let _serial = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(BicDb::open(dir.path()).unwrap());
    let owner = db.begin_transaction().unwrap();
    owner.reserve_integrity_key("u", "key").unwrap();
    let waiter = db.begin_transaction().unwrap();
    let (done, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        done.send(waiter.reserve_integrity_key_cancellable(
            "u",
            "key",
            &CancellationToken::uncancelable(),
        ))
        .unwrap();
    });
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    db.durability_health_for_test()
        .record_failure("test.constraint_wait", &"injected");
    assert!(matches!(
        received.recv_timeout(Duration::from_secs(5)).unwrap(),
        Err(BicDbError::WriteFailed { .. })
    ));
    worker.join().unwrap();
    owner.rollback().unwrap();
}
