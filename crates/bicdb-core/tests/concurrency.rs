//! Concurrency and crash-recovery safety harness for write transactions.
//!
//! These tests assert the invariants that must hold both under today's
//! globally-serialized write model and under the future concurrent-writer
//! model (snapshot execution + serialized commit with conflict retry). They are
//! the safety net for reworking the transaction ownership model: they must keep
//! passing through every increment of that change.
//!
//! Each worker thread shares one `RwLock<BicDb>` exactly as the pgwire server
//! does, takes the write side for a transaction, and retries on
//! `TransactionConflict`. Under the current model conflicts never occur (the
//! write lock is held for the whole transaction); the retry loop is what keeps
//! these tests valid once execution and commit stop being one critical section.

use std::sync::RwLock;

use bicdb_core::{BicDb, BicDbError, Record};
use serde_json::json;

fn balance(db: &RwLock<BicDb>, account: &str) -> i64 {
    let guard = db.read().unwrap();
    guard
        .get("accounts", account)
        .unwrap()
        .and_then(|record| record.metadata.get("bal").and_then(|v| v.as_i64()))
        .unwrap_or(0)
}

fn total_balance(db: &RwLock<BicDb>, accounts: usize) -> i64 {
    (0..accounts)
        .map(|i| balance(db, &format!("acct-{i}")))
        .sum()
}

/// Attempt one unit transfer from `a` to `b`. Returns true when the transaction
/// resolves (committed or nothing-to-do), false on a conflict that should retry.
fn try_transfer(db: &RwLock<BicDb>, a: &str, b: &str) -> bool {
    let mut guard = db.write().unwrap();
    let mut tx = guard.begin_transaction().unwrap();
    let ra = tx.get("accounts", a).unwrap().expect("account a exists");
    let rb = tx.get("accounts", b).unwrap().expect("account b exists");
    let ba = ra.metadata.get("bal").and_then(|v| v.as_i64()).unwrap_or(0);
    let bb = rb.metadata.get("bal").and_then(|v| v.as_i64()).unwrap_or(0);
    if ba <= 0 {
        return true; // nothing to transfer; still a clean resolution
    }
    tx.update(
        "accounts",
        Record::new(a).with_metadata(json!({ "bal": ba - 1 })),
    )
    .unwrap();
    tx.update(
        "accounts",
        Record::new(b).with_metadata(json!({ "bal": bb + 1 })),
    )
    .unwrap();
    match tx.commit() {
        Ok(()) => true,
        Err(BicDbError::TransactionConflict(_)) => false,
        Err(other) => panic!("unexpected commit error: {other:?}"),
    }
}

#[test]
fn concurrent_transfers_conserve_total_balance() {
    let dir = tempfile::tempdir().unwrap();
    const ACCOUNTS: usize = 8;
    const THREADS: usize = 6;
    const TRANSFERS_PER_THREAD: usize = 200;
    const STARTING_BALANCE: i64 = 100;

    let db = {
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("accounts").unwrap();
        for i in 0..ACCOUNTS {
            db.insert(
                "accounts",
                Record::new(format!("acct-{i}")).with_metadata(json!({ "bal": STARTING_BALANCE })),
            )
            .unwrap();
        }
        RwLock::new(db)
    };

    let expected_total = STARTING_BALANCE * ACCOUNTS as i64;

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let db = &db;
            scope.spawn(move || {
                for i in 0..TRANSFERS_PER_THREAD {
                    // Overlapping account pairs across threads create contention.
                    let from = format!("acct-{}", (t + i) % ACCOUNTS);
                    let to = format!("acct-{}", (t + i + 1) % ACCOUNTS);
                    if from == to {
                        continue;
                    }
                    // Retry until the transfer resolves.
                    while !try_transfer(db, &from, &to) {}
                }
            });
        }
    });

    // No unit of balance may be created or destroyed by concurrent transfers.
    assert_eq!(total_balance(&db, ACCOUNTS), expected_total);
    // And every balance must be non-negative (no double-spend).
    for i in 0..ACCOUNTS {
        assert!(balance(&db, &format!("acct-{i}")) >= 0);
    }
}

#[test]
fn concurrent_inserts_lose_no_records() {
    let dir = tempfile::tempdir().unwrap();
    const THREADS: usize = 6;
    const PER_THREAD: usize = 150;

    let db = {
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("items").unwrap();
        RwLock::new(db)
    };

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let db = &db;
            scope.spawn(move || {
                for i in 0..PER_THREAD {
                    let id = format!("item-{t}-{i}");
                    loop {
                        let mut guard = db.write().unwrap();
                        let mut tx = guard.begin_transaction().unwrap();
                        tx.insert("items", Record::new(&id)).unwrap();
                        match tx.commit() {
                            Ok(()) => break,
                            Err(BicDbError::TransactionConflict(_)) => continue,
                            Err(other) => panic!("unexpected commit error: {other:?}"),
                        }
                    }
                }
            });
        }
    });

    let guard = db.read().unwrap();
    assert_eq!(
        guard.scan_collection("items").unwrap().len(),
        THREADS * PER_THREAD
    );
}

#[test]
fn decoupled_group_commit_is_durable_across_reopen() {
    // Exercises the deferred-write (decoupled group-commit) path the server uses:
    // execute under a read lock, commit (enqueue) under the write lock, then make
    // the WAL durable via the tx-log handle after releasing the write lock. Every
    // acknowledged commit must survive recovery, in order.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = {
            let mut d = BicDb::open(dir.path()).unwrap();
            d.create_collection("gc").unwrap();
            RwLock::new(d)
        };
        for i in 0..6 {
            let mut tx = {
                let guard = db.read().unwrap();
                let mut tx = guard.begin_transaction().unwrap();
                tx.insert("gc", Record::new(format!("g-{i}"))).unwrap();
                tx
            };
            let commit_seq = {
                let mut guard = db.write().unwrap();
                guard.commit_buffered_transaction(&mut tx).unwrap()
            };
            db.read()
                .unwrap()
                .tx_log_handle()
                .write_durable(commit_seq)
                .unwrap();
        }
    }
    let db = BicDb::open(dir.path()).unwrap();
    for i in 0..6 {
        assert!(
            db.get("gc", &format!("g-{i}")).unwrap().is_some(),
            "group-committed record g-{i} must be durable across reopen"
        );
    }
}

#[test]
fn recovery_keeps_committed_and_drops_abandoned_transactions() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("log").unwrap();
        // Two committed transactions.
        for i in 0..2 {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("log", Record::new(format!("committed-{i}")))
                .unwrap();
            tx.commit().unwrap();
        }
        // An abandoned (never committed) transaction: drop without commit.
        {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("log", Record::new("abandoned")).unwrap();
            // tx dropped here without commit() — its writes must not survive.
        }
        // No clean shutdown marker on purpose; simulate crash by dropping db.
    }

    let db = BicDb::open(dir.path()).unwrap();
    let ids: Vec<String> = db
        .scan_collection("log")
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect();
    assert!(ids.contains(&"committed-0".to_string()));
    assert!(ids.contains(&"committed-1".to_string()));
    assert!(
        !ids.contains(&"abandoned".to_string()),
        "abandoned transaction writes must not be durable"
    );
    assert_eq!(ids.len(), 2);
}

#[test]
fn multi_record_lock_order_conflict_breaks_deadlock_cycle_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("locks").unwrap();
    db.insert("locks", Record::new("a")).unwrap();
    db.insert("locks", Record::new("b")).unwrap();

    // The first transaction is older. Each transaction takes a different
    // record, which would form a cycle if both were then allowed to wait for
    // the other record until the full lock timeout.
    let mut older = db.begin_transaction().unwrap();
    let mut newer = db.begin_transaction().unwrap();
    older.update("locks", Record::new("a")).unwrap();
    newer.update("locks", Record::new("b")).unwrap();

    let started = std::time::Instant::now();
    assert!(matches!(
        older.update("locks", Record::new("b")),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "lock-order loser must abort immediately instead of waiting for the deadlock timeout"
    );

    drop(older);
    newer.update("locks", Record::new("a")).unwrap();
}

// --- Concurrent execution model: begin + read + buffer under a shared read
// lock, then apply through BicDb's interior commit serialization under another
// shared guard. This matches pgwire's transaction-progress path and avoids an
// outer RwLock inversion with record-lock waits. ---

fn try_transfer_concurrent(db: &RwLock<BicDb>, a: &str, b: &str) -> bool {
    // Phase 1: execute (begin + snapshot reads + buffer writes) under a shared
    // read lock so executions of different transactions overlap.
    let mut tx = {
        let guard = db.read().unwrap();
        let mut tx = guard.begin_transaction().unwrap();
        let ra = tx.get("accounts", a).unwrap().expect("account a exists");
        let rb = tx.get("accounts", b).unwrap().expect("account b exists");
        let ba = ra.metadata.get("bal").and_then(|v| v.as_i64()).unwrap_or(0);
        let bb = rb.metadata.get("bal").and_then(|v| v.as_i64()).unwrap_or(0);
        if ba <= 0 {
            return true;
        }
        for (account, bal) in [(a, ba - 1), (b, bb + 1)] {
            match tx.update(
                "accounts",
                Record::new(account).with_metadata(json!({ "bal": bal })),
            ) {
                Ok(()) => {}
                // Another transaction holds this record's write lock; abandon
                // and retry. Dropping `tx` releases any locks it did take.
                Err(BicDbError::TransactionConflict(_)) => return false,
                Err(other) => panic!("unexpected buffer error: {other:?}"),
            }
        }
        tx
    };
    // Phase 2: apply through the shared transaction-progress path.
    // detect_commit_conflicts rejects an invalidated snapshot.
    let guard = db.read().unwrap();
    let commit_seq = match guard.commit_buffered_transaction(&mut tx) {
        Ok(seq) => seq,
        Err(BicDbError::TransactionConflict(_)) => return false,
        Err(other) => panic!("unexpected commit error: {other:?}"),
    };
    // Decoupled group commit: release the write lock, then make the WAL durable.
    drop(guard);
    db.read()
        .unwrap()
        .tx_log_handle()
        .write_durable(commit_seq)
        .unwrap();
    true
}

#[test]
fn concurrent_execution_commit_conserves_total_balance() {
    let dir = tempfile::tempdir().unwrap();
    const ACCOUNTS: usize = 8;
    const THREADS: usize = 6;
    const TRANSFERS_PER_THREAD: usize = 200;
    const STARTING_BALANCE: i64 = 100;

    let db = {
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("accounts").unwrap();
        for i in 0..ACCOUNTS {
            db.insert(
                "accounts",
                Record::new(format!("acct-{i}")).with_metadata(json!({ "bal": STARTING_BALANCE })),
            )
            .unwrap();
        }
        RwLock::new(db)
    };
    let expected_total = STARTING_BALANCE * ACCOUNTS as i64;

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let db = &db;
            scope.spawn(move || {
                for i in 0..TRANSFERS_PER_THREAD {
                    let from = format!("acct-{}", (t + i) % ACCOUNTS);
                    let to = format!("acct-{}", (t + i + 1) % ACCOUNTS);
                    if from == to {
                        continue;
                    }
                    while !try_transfer_concurrent(db, &from, &to) {}
                }
            });
        }
    });

    assert_eq!(total_balance(&db, ACCOUNTS), expected_total);
    for i in 0..ACCOUNTS {
        assert!(balance(&db, &format!("acct-{i}")) >= 0);
    }
}

// --- Throughput comparison: the same contended read-heavy write workload run
// (a) fully serialized (write lock held for the whole transaction) vs
// (b) concurrent execution + serialized commit. Reads simulate the per-call
// work of a stored procedure so execution is non-trivial relative to commit.
// Printed with --nocapture; not asserted (timing is environment dependent). ---

fn read_heavy_transfer_serial(db: &RwLock<BicDb>, a: &str, b: &str, reads: usize) -> bool {
    let guard = db.read().unwrap();
    let mut tx = guard.begin_transaction().unwrap();
    for r in 0..reads {
        let _ = tx.get("accounts", &format!("acct-{}", r % 8)).unwrap();
    }
    let ba = tx
        .get("accounts", a)
        .unwrap()
        .and_then(|r| r.metadata.get("bal").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    let bb = tx
        .get("accounts", b)
        .unwrap()
        .and_then(|r| r.metadata.get("bal").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    if ba <= 0 {
        return true;
    }
    tx.update(
        "accounts",
        Record::new(a).with_metadata(json!({ "bal": ba - 1 })),
    )
    .unwrap();
    tx.update(
        "accounts",
        Record::new(b).with_metadata(json!({ "bal": bb + 1 })),
    )
    .unwrap();
    matches!(tx.commit(), Ok(()))
}

fn read_heavy_transfer_concurrent(db: &RwLock<BicDb>, a: &str, b: &str, reads: usize) -> bool {
    let mut tx = {
        let guard = db.read().unwrap();
        let mut tx = guard.begin_transaction().unwrap();
        for r in 0..reads {
            let _ = tx.get("accounts", &format!("acct-{}", r % 8)).unwrap();
        }
        let ba = tx
            .get("accounts", a)
            .unwrap()
            .and_then(|r| r.metadata.get("bal").and_then(|v| v.as_i64()))
            .unwrap_or(0);
        let bb = tx
            .get("accounts", b)
            .unwrap()
            .and_then(|r| r.metadata.get("bal").and_then(|v| v.as_i64()))
            .unwrap_or(0);
        if ba <= 0 {
            return true;
        }
        for (acct, bal) in [(a, ba - 1), (b, bb + 1)] {
            match tx.update(
                "accounts",
                Record::new(acct).with_metadata(json!({ "bal": bal })),
            ) {
                Ok(()) => {}
                Err(BicDbError::TransactionConflict(_)) => return false,
                Err(e) => panic!("{e:?}"),
            }
        }
        tx
    };
    let mut guard = db.write().unwrap();
    let commit_seq = match guard.commit_buffered_transaction(&mut tx) {
        Ok(seq) => seq,
        Err(_) => return false,
    };
    drop(guard);
    db.read()
        .unwrap()
        .tx_log_handle()
        .write_durable(commit_seq)
        .unwrap();
    true
}

#[test]
fn throughput_concurrent_vs_serial_report() {
    // Disjoint per-thread partitions => low write contention, like TPC-C at
    // scale (different warehouses/rows). Each thread only touches its own
    // accounts, so transactions never conflict and the optimistic concurrent
    // path does not thrash on retries.
    const THREADS: usize = 6;
    const PARTITION: usize = 4;
    const ACCOUNTS: usize = THREADS * PARTITION;
    const PER_THREAD: usize = 400;
    const READS: usize = 600; // execution-dominated, like a TPC-C proc body
    const START: i64 = 100_000;

    let setup = || {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("accounts").unwrap();
        for i in 0..ACCOUNTS {
            db.insert(
                "accounts",
                Record::new(format!("acct-{i}")).with_metadata(json!({ "bal": START })),
            )
            .unwrap();
        }
        (dir, RwLock::new(db))
    };

    let run = |db: &RwLock<BicDb>, concurrent: bool| {
        let start = std::time::Instant::now();
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let db = &*db;
                scope.spawn(move || {
                    let base = t * PARTITION;
                    for i in 0..PER_THREAD {
                        let a = format!("acct-{}", base + (i % PARTITION));
                        let b = format!("acct-{}", base + ((i + 1) % PARTITION));
                        if a == b {
                            continue;
                        }
                        if concurrent {
                            while !read_heavy_transfer_concurrent(db, &a, &b, READS) {}
                        } else {
                            while !read_heavy_transfer_serial(db, &a, &b, READS) {}
                        }
                    }
                });
            }
        });
        start.elapsed()
    };

    let (_d1, db1) = setup();
    let serial = run(&db1, false);
    let (_d2, db2) = setup();
    let concurrent = run(&db2, true);

    let total = THREADS * PER_THREAD;
    let serial_tps = total as f64 / serial.as_secs_f64();
    let conc_tps = total as f64 / concurrent.as_secs_f64();
    println!(
        "THROUGHPUT serial={:.0} tx/s  concurrent={:.0} tx/s  speedup={:.2}x  (threads={THREADS}, reads/tx={READS})",
        serial_tps, conc_tps, conc_tps / serial_tps
    );

    // Both must conserve the total balance.
    assert_eq!(total_balance(&db1, ACCOUNTS), START * ACCOUNTS as i64);
    assert_eq!(total_balance(&db2, ACCOUNTS), START * ACCOUNTS as i64);
}

/// Reproducible xorshift64 PRNG (no external crate) for deterministic fuzzing.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn randomized_concurrent_stress_preserves_invariants() {
    // Aggressive randomized stress (a `test/format`-style fuzz harness): many
    // threads hammer the concurrent execute-then-commit path with a mix of
    // conserving transfers and unique inserts under heavy contention on
    // overlapping accounts. This is the validation net for the concurrency added
    // this session (Arc record sharing, the decoupled WAL writer, shared-lock
    // execution). Seeded PRNG keeps any failure reproducible.
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    let dir = tempfile::tempdir().unwrap();
    const ACCOUNTS: usize = 16;
    const THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 600;
    const STARTING_BALANCE: i64 = 1000;

    let db = {
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("accounts").unwrap();
        db.create_collection("ledger").unwrap();
        for i in 0..ACCOUNTS {
            db.insert(
                "accounts",
                Record::new(format!("acct-{i}")).with_metadata(json!({ "bal": STARTING_BALANCE })),
            )
            .unwrap();
        }
        RwLock::new(db)
    };
    let expected_total = STARTING_BALANCE * ACCOUNTS as i64;
    let inserted = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let db = &db;
            let inserted = &inserted;
            scope.spawn(move || {
                let mut rng =
                    0x9E37_79B9_7F4A_7C15u64 ^ (t as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
                for op in 0..OPS_PER_THREAD {
                    match xorshift(&mut rng) % 3 {
                        0 | 1 => {
                            // Conserving transfer between two overlapping accounts.
                            let a = xorshift(&mut rng) as usize % ACCOUNTS;
                            let b = xorshift(&mut rng) as usize % ACCOUNTS;
                            if a == b {
                                continue;
                            }
                            while !try_transfer_concurrent(
                                db,
                                &format!("acct-{a}"),
                                &format!("acct-{b}"),
                            ) {}
                        }
                        _ => {
                            // Unique insert through the concurrent group-commit path.
                            let id = format!("led-{t}-{op}");
                            loop {
                                let mut tx = {
                                    let guard = db.read().unwrap();
                                    let mut tx = guard.begin_transaction().unwrap();
                                    tx.insert("ledger", Record::new(&id)).unwrap();
                                    tx
                                };
                                let seq = {
                                    let guard = db.read().unwrap();
                                    match guard.commit_buffered_transaction(&mut tx) {
                                        Ok(seq) => seq,
                                        Err(BicDbError::TransactionConflict(_)) => continue,
                                        Err(other) => panic!("unexpected commit error: {other:?}"),
                                    }
                                };
                                db.read()
                                    .unwrap()
                                    .tx_log_handle()
                                    .write_durable(seq)
                                    .unwrap();
                                inserted.fetch_add(1, Relaxed);
                                break;
                            }
                        }
                    }
                }
            });
        }
    });

    // Invariants: conservation, no double-spend, and every unique insert survives.
    assert_eq!(
        total_balance(&db, ACCOUNTS),
        expected_total,
        "concurrent transfers must conserve the total balance"
    );
    for i in 0..ACCOUNTS {
        assert!(
            balance(&db, &format!("acct-{i}")) >= 0,
            "no negative balance"
        );
    }
    let guard = db.read().unwrap();
    assert_eq!(
        guard.scan_collection("ledger").unwrap().len(),
        inserted.load(Relaxed),
        "every committed unique insert must be present exactly once"
    );

    // Survives recovery with the same invariants. The original database is
    // dropped first, not just its read guard: reopening while the first handle
    // is still alive means two engines own one directory, which the paged engine
    // refuses outright and the in-memory engine only survives by luck.
    drop(guard);
    drop(db);
    let reopened = BicDb::open(dir.path()).unwrap();
    let total: i64 = (0..ACCOUNTS)
        .map(|i| {
            reopened
                .get("accounts", &format!("acct-{i}"))
                .unwrap()
                .and_then(|r| r.metadata.get("bal").and_then(|v| v.as_i64()))
                .unwrap_or(0)
        })
        .sum();
    assert_eq!(
        total, expected_total,
        "conservation must hold after recovery"
    );
    assert_eq!(
        reopened.scan_collection("ledger").unwrap().len(),
        inserted.load(Relaxed),
        "all inserts must be durable across recovery"
    );
}
