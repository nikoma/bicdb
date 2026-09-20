//! An acknowledged commit must be durable when `commit()` returns.
//!
//! The hazard: `commit_transaction` publishes a commit sequence with a plain
//! `fetch_add` and only afterwards performs fallible WAL encoding and enqueues
//! the bytes. Commits run concurrently (there is no global commit lock), so two
//! committers can interleave:
//!
//! 1. committer N reserves sequence N and stalls before enqueue;
//! 2. committer N+1 reserves, encodes, enqueues and calls `write_durable(N+1)`;
//! 3. `drain_contiguous` can only drain a contiguous prefix, and N's slot is
//!    empty, so it drains nothing;
//! 4. `write_durable` returned `Ok(())` on that empty drain without ever
//!    checking that it had reached N+1.
//!
//! N+1's client was therefore acknowledged while N+1's WAL was still in memory.
//! A crash in that window loses an acknowledged commit — the one failure a
//! durable store must never have.
//!
//! This was recorded in a benchmark handoff note on 2026-07-09 and was still
//! present six weeks later. The existing concurrent-recovery tests miss it
//! because they wait for every committer to finish, which closes the window.
//!
//! `BICDB_TEST_SEQ_RESERVED_PAUSE_MS` holds the first committer inside the
//! window. Env vars are process-global and the pause fires once per process,
//! so this file contains exactly one test.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn a_commit_is_durable_before_it_is_acknowledged() {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    db.create_collection("t").unwrap();
    let db = Arc::new(db);

    // Hold the first committer to reach the reservation window for long enough
    // that the second one runs the whole way through while its slot is empty.
    std::env::set_var("BICDB_TEST_SEQ_RESERVED_PAUSE_MS", "1500");

    let stalled_db = Arc::clone(&db);
    let stalled = std::thread::spawn(move || {
        let mut tx = stalled_db.begin_transaction().unwrap();
        tx.insert("t", Record::new("n").with_metadata(json!({"n": 1})))
            .unwrap();
        tx.commit()
    });

    // Let the first committer reserve its sequence and enter the pause.
    std::thread::sleep(Duration::from_millis(300));

    let mut tx = db.begin_transaction().unwrap();
    tx.insert("t", Record::new("n+1").with_metadata(json!({"n": 2})))
        .unwrap();
    let overtaking_seq = {
        // `commit()` must not return until this commit is durable.
        tx.commit().expect("the overtaking commit succeeds");
        db.tx_log_handle().stats().written_seq
    };

    // THE ASSERTION: at the moment the second commit was acknowledged, the log
    // must already carry it. Before the fix `written_seq` was still behind,
    // because `write_durable` reported success on an empty contiguous drain.
    let durable_at_ack = overtaking_seq;

    stalled
        .join()
        .unwrap()
        .expect("the stalled commit succeeds");
    std::env::remove_var("BICDB_TEST_SEQ_RESERVED_PAUSE_MS");

    let final_seq = db.tx_log_handle().stats().written_seq;
    assert!(
        durable_at_ack >= final_seq - 1,
        "a commit was acknowledged while its WAL was not durable: written_seq \
         was {durable_at_ack} at acknowledgement, and the two commits occupy \
         sequences up to {final_seq}"
    );
    assert_eq!(
        db.scan_collection("t").unwrap().len(),
        2,
        "both commits are visible"
    );
}
