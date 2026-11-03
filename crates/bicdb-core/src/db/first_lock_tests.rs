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

#[test]
fn cancellable_row_wait_resumes_after_owner_release() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let held = Mutex::new(LockedKeys::default());
    let requested = Mutex::new(LockedKeys::default());
    let owner = TransactionId(2);
    let requester = TransactionId(1);
    db.lock_tx_record_with_attempts(owner, "rows", "x", 0, false, false, &held)
        .unwrap();
    let token = crate::CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() + Duration::from_secs(2)),
    );
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            send.send(db.lock_tx_record_cancellable(requester, "rows", "x", &requested, &token))
                .unwrap();
        });
        let early = receive.recv_timeout(Duration::from_millis(30));
        release_owned_write_locks(&db.write_locks, owner, &held.lock().take_all());
        assert!(
            early.is_err(),
            "waiter acquired a row still owned by another transaction"
        );
        receive
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
    });
    assert_eq!(requested.lock().len(), 1);
    assert!(db.wait_graph.wait_for(owner, requester).is_some());
    release_owned_write_locks(&db.write_locks, requester, &requested.lock().take_all());
}

#[test]
fn cancellable_row_wait_cleans_up_after_deadline_and_explicit_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let held = Mutex::new(LockedKeys::default());
    let requested = Mutex::new(LockedKeys::default());
    let owner = TransactionId(2);
    let requester = TransactionId(1);
    db.lock_tx_record_with_attempts(owner, "rows", "x", 0, false, false, &held)
        .unwrap();
    let timeout = crate::CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() + Duration::from_millis(30)),
    );
    assert!(matches!(
        db.lock_tx_record_cancellable(requester, "rows", "x", &requested, &timeout),
        Err(BicDbError::QueryTimedOut)
    ));
    assert_eq!(requested.lock().len(), 0);
    assert!(db.wait_graph.wait_for(owner, requester).is_some());
    let token = crate::CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() + Duration::from_secs(2)),
    );
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            send.send(db.lock_tx_record_cancellable(requester, "rows", "x", &requested, &token))
                .unwrap();
        });
        let early = receive.recv_timeout(Duration::from_millis(30));
        token.cancel();
        assert!(early.is_err());
        assert!(matches!(
            receive.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(BicDbError::QueryCanceled)
        ));
    });
    assert!(db.wait_graph.wait_for(owner, requester).is_some());
    assert_eq!(requested.lock().len(), 0);
    assert_eq!(held.lock().len(), 1);
    release_owned_write_locks(&db.write_locks, owner, &held.lock().take_all());
}

#[test]
fn cancellable_row_wait_detects_cycles_without_an_environment_override() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let a = TransactionId(1);
    let b = TransactionId(2);
    let locks_a = Mutex::new(LockedKeys::default());
    let locks_b = Mutex::new(LockedKeys::default());
    db.lock_tx_record_with_attempts(a, "rows", "x", 0, false, false, &locks_a)
        .unwrap();
    db.lock_tx_record_with_attempts(b, "rows", "y", 0, false, false, &locks_b)
        .unwrap();
    let barrier = std::sync::Barrier::new(2);
    let token = crate::CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() + Duration::from_secs(2)),
    );
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for (tx, key, locks) in [(a, "y", &locks_a), (b, "x", &locks_b)] {
            let (db, barrier, token, send) = (&db, &barrier, &token, &send);
            scope.spawn(move || {
                barrier.wait();
                let result = db.lock_tx_record_cancellable(tx, "rows", key, locks, token);
                release_owned_write_locks(&db.write_locks, tx, &locks.lock().take_all());
                send.send(result).unwrap();
            });
        }
        let first = receive.recv_timeout(Duration::from_secs(3)).unwrap();
        let second = receive.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            matches!(
                (&first, &second),
                (Ok(()), Err(BicDbError::TransactionDeadlock(_)))
                    | (Err(BicDbError::TransactionDeadlock(_)), Ok(()))
            ),
            "expected one deadlock victim and one survivor: {first:?}, {second:?}"
        );
    });
    assert!(db.wait_graph.wait_for(a, b).is_some());
    assert!(db.wait_graph.wait_for(b, a).is_some());
}
