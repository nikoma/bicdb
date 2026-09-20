use super::*;

#[test]
fn first_update_waits_for_owner_but_second_lock_keeps_deadlock_policy() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let (requester, owner) = match row_lock_policy() {
        RowLockPolicy::OldDies => (TransactionId(1), TransactionId(2)),
        RowLockPolicy::WaitDie => (TransactionId(2), TransactionId(1)),
        // This test exercises the age-based policies. Graph cycle detection
        // has separate tests and deliberately permits acyclic second waits.
        RowLockPolicy::WaitGraph => return,
    };
    let held = Mutex::new(LockedKeys::default());
    db.lock_tx_record_with_attempts(owner, "test", "x", 0, false, false, &held)
        .unwrap();
    let requested = Mutex::new(LockedKeys::default());
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            send.send(db.lock_tx_record_for_read_committed_update_with_first_wait(
                requester, "test", "x", &requested, true,
            ))
            .unwrap();
        });
        assert!(receive.recv_timeout(Duration::from_millis(20)).is_err());
        release_owned_write_locks(&db.write_locks, owner, &held.lock().take_all());
        receive
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
    });
    assert_eq!(requested.lock().len(), 1);
    db.lock_tx_record_with_attempts(owner, "test", "y", 0, false, false, &held)
        .unwrap();
    assert!(
        db.lock_tx_record_for_read_committed_update_with_first_wait(
            requester, "test", "y", &requested, true,
        )
        .is_err(),
        "a transaction already holding x must not bypass deadlock prevention"
    );
    release_owned_write_locks(&db.write_locks, owner, &held.lock().take_all());
    release_owned_write_locks(&db.write_locks, requester, &requested.lock().take_all());
}

#[test]
fn wait_graph_allows_acyclic_second_lock_and_cleans_up_timeout() {
    if !matches!(row_lock_policy(), RowLockPolicy::WaitGraph) {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let requester = TransactionId(1);
    let owner = TransactionId(2);
    let requested = Mutex::new(LockedKeys::default());
    let held = Mutex::new(LockedKeys::default());
    db.lock_tx_record_with_attempts(requester, "test", "x", 0, false, false, &requested)
        .unwrap();
    db.lock_tx_record_with_attempts(owner, "test", "y", 0, false, false, &held)
        .unwrap();
    // A bounded unsuccessful wait must not leave an edge behind.
    assert!(db
        .lock_tx_record_with_attempts(requester, "test", "y", 12, true, false, &requested)
        .is_err());
    assert!(db.wait_graph.wait_for(owner, requester).is_some());
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            send.send(db.lock_tx_record_with_attempts(
                requester, "test", "y", 20_000, true, false, &requested,
            ))
            .unwrap();
        });
        assert!(receive.recv_timeout(Duration::from_millis(20)).is_err());
        release_owned_write_locks(&db.write_locks, owner, &held.lock().take_all());
        receive
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
    });
    assert_eq!(requested.lock().len(), 2);
    assert!(db.wait_graph.wait_for(owner, requester).is_some());
    release_owned_write_locks(&db.write_locks, requester, &requested.lock().take_all());
}

#[test]
fn wait_graph_row_cycle_aborts_one_and_releases_survivor() {
    if !matches!(row_lock_policy(), RowLockPolicy::WaitGraph) {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let a = TransactionId(1);
    let b = TransactionId(2);
    let locks_a = Mutex::new(LockedKeys::default());
    let locks_b = Mutex::new(LockedKeys::default());
    db.lock_tx_record_with_attempts(a, "test", "x", 0, false, false, &locks_a)
        .unwrap();
    db.lock_tx_record_with_attempts(b, "test", "y", 0, false, false, &locks_b)
        .unwrap();
    let barrier = std::sync::Barrier::new(2);
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for (tx, key, locks) in [(a, "y", &locks_a), (b, "x", &locks_b)] {
            let (db, barrier, send) = (&db, &barrier, &send);
            scope.spawn(move || {
                barrier.wait();
                let result =
                    db.lock_tx_record_with_attempts(tx, "test", key, 20_000, true, false, locks);
                release_owned_write_locks(&db.write_locks, tx, &locks.lock().take_all());
                send.send(result.is_ok()).unwrap();
            });
        }
        let first = receive.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = receive.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_ne!(
            first, second,
            "one cycle participant must abort and the other acquire"
        );
    });
    let edge = db.wait_graph.wait_for(a, b).unwrap();
    drop(edge);
    assert!(db.wait_graph.wait_for(b, a).is_some());
}
