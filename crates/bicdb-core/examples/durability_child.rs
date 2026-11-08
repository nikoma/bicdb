//! Crash-matrix child for commit durability (see
//! `docs/commit-durability-invariants.md`, section 6).
//!
//! Usage: `durability_child <db-dir> <ledger-dir> <mode> <commits-per-writer>`
//! where `<mode>` is `all-logs` (production) or `transaction-log`
//! (experimental). The parent sets `BICDB_TEST_CRASH_AT` and, for power-loss
//! emulation, `BICDB_TEST_WAL_VOLATILE_TAIL=1`.
//!
//! Four writers commit concurrently. Writer `w`'s commit `i`:
//! - inserts `r-w-i` with a unique key `u-w-i` and owner `w`;
//! - upserts its hot row `hot-w` with `seq = i` (so the hot row names the
//!   newest durable commit of that writer);
//! - deletes `r-w-(i-3)` when `i % 7 == 0`.
//!
//! A commit is appended to `ledger-dir/writer-w` (fsynced) only after
//! `commit` returned: every ledger line is an acknowledged commit. A DDL thread
//! creates and drops side collections and records each in `ledger-dir/ddl`.
//! A checkpoint thread runs the resume checkpoint (page checkpoint plus
//! transaction-log cut) every 40 ms.
//!
//! `DURABILITY_POOL_BYTES` shrinks the page buffer pool so eviction and
//! writeback run during the workload.
//!
//! Exit 0 after every writer finishes, unless a crash point aborts first.
//!
//! With `BICDB_TEST_FAIL_IO` set (fail-closed matrix, see
//! `docs/durability-failure-handling.md`) a failed operation does not panic.
//! A writer whose commit fails tries [`AFTER_FAILURE_ATTEMPTS`] more commits
//! and appends a line to `ledger-dir/violations` if any of them is
//! acknowledged, or if any error is not a durability failure. The other
//! threads stop at their first error. On exit the child records the
//! database's durability state in `ledger-dir/state`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

/// Commits a writer still attempts after its first failure; none may succeed.
const AFTER_FAILURE_ATTEMPTS: u64 = 3;

fn fail_io_mode() -> bool {
    std::env::var_os("BICDB_TEST_FAIL_IO").is_some()
}

/// See `checkpoints` in `main`.
const MIN_CHECKPOINTS: u64 = 20;
pub const WRITERS: usize = 4;

fn config(mode: &str) -> DbConfig {
    let config = DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_transaction_log_commit_durability(mode == "transaction-log");
    // A small pool forces eviction and writeback during the workload.
    match std::env::var("DURABILITY_POOL_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        Some(bytes) => config.with_paged_buffer_pool_bytes(bytes),
        None => config,
    }
}

fn append_line(path: &Path, line: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open ledger");
    file.write_all(line.as_bytes()).expect("write ledger");
    file.write_all(b"\n").expect("write ledger");
    file.sync_data().expect("sync ledger");
}

fn index(name: &str, field: &str, unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        collection: "rows".into(),
        fields: vec![IndexField::MetadataPath(vec![field.into()])],
        unique,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let db_dir = PathBuf::from(args.next().expect("db dir"));
    let ledger = PathBuf::from(args.next().expect("ledger dir"));
    let mode = args.next().expect("mode");
    let commits: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(400);
    std::fs::create_dir_all(&ledger).expect("ledger dir");

    let mut db = BicDb::open_with_config(&db_dir, config(&mode)).expect("open db");
    // The matrix must exercise the path it names: the experimental row-commit
    // sync outside the writer lock engages exactly when requested.
    let requested = std::env::var("BICDB_PAGED_COMMIT_SYNC_OUTSIDE_LOCK").as_deref() == Ok("1");
    let engaged = db
        .paged_records_handle()
        .is_some_and(|paged| paged.store().commit_sync_outside_lock());
    assert_eq!(engaged, requested, "commit sync outside lock engaged={engaged}");
    if !db.collections().iter().any(|meta| meta.name == "rows") {
        db.create_collection("rows").expect("create rows");
        db.create_index(index("rows_owner", "owner", false))
            .expect("owner index");
        db.create_index(index("rows_unique_key", "uk", true))
            .expect("unique index");
    }
    let db = Arc::new(RwLock::new(db));
    let done = Arc::new(AtomicBool::new(false));
    // The crash matrix names checkpoint boundaries by hit count (up to
    // `checkpoint.after_wal_reset:15`). Checkpoints run on a timer, so a fast
    // workload (the transaction-log mode commits faster) can finish before
    // the n-th hit. Keep the run going until enough checkpoints happened.
    let checkpoints = Arc::new(AtomicU64::new(0));

    let fail_io = fail_io_mode();
    let violations = ledger.join("violations");
    let writers_done = Arc::new(AtomicU64::new(0));
    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let db = db.clone();
            let violations = violations.clone();
            let writers_done = writers_done.clone();
            let ledger = ledger.join(format!("writer-{writer}"));
            scope.spawn(move || {
                let mut failures = 0u64;
                for i in 1..=commits {
                    let guard = db.read().expect("db lock");
                    let result = (|| -> bicdb_core::Result<()> {
                        let mut tx = guard.begin_transaction()?;
                        tx.insert(
                            "rows",
                            Record::new(format!("r-{writer}-{i}")).with_metadata(
                                json!({"owner": writer, "seq": i, "uk": format!("u-{writer}-{i}")}),
                            ),
                        )?;
                        tx.update(
                            "rows",
                            Record::new(format!("hot-{writer}")).with_metadata(
                                json!({"owner": writer, "seq": i, "uk": format!("h-{writer}")}),
                            ),
                        )?;
                        if i % 7 == 0 && i > 3 {
                            tx.delete("rows", &format!("r-{writer}-{}", i - 3))?;
                        }
                        tx.commit()
                    })();
                    drop(guard);
                    match result {
                        Ok(()) if failures > 0 => {
                            append_line(&violations, &format!("writer {writer}: commit {i} acknowledged after a failure"));
                        }
                        Ok(()) => append_line(&ledger, &i.to_string()),
                        Err(error) if fail_io => {
                            if !error.is_durability_failure() {
                                append_line(&violations, &format!("writer {writer}: commit {i}: not a durability failure: {error}"));
                            }
                            failures += 1;
                            if failures > AFTER_FAILURE_ATTEMPTS {
                                break;
                            }
                        }
                        Err(error) => panic!("commit: {error}"),
                    }
                }
                writers_done.fetch_add(1, Ordering::AcqRel);
            });
        }
        {
            let db = db.clone();
            let done = done.clone();
            let ledger = ledger.join("ddl");
            scope.spawn(move || {
                // Three names, reused: a dropped and recreated collection must
                // never get an older generation's rows back from replay.
                let mut generation = 0u64;
                while !done.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(25));
                    generation += 1;
                    let name = format!("side_{}", generation % 3);
                    if fail_io {
                        if db.read().expect("db lock").durability_state().is_healthy() {
                            continue_ddl(&db, &ledger, &name, generation);
                        }
                        continue;
                    }
                    {
                        let mut guard = db.write().expect("db lock");
                        if guard.collections().iter().any(|meta| meta.name == name) {
                            guard.drop_collection(&name).expect("drop side");
                            drop(guard);
                            append_line(&ledger, &format!("drop {name} {generation}"));
                            guard = db.write().expect("db lock");
                        }
                        guard.create_collection(&name).expect("create side");
                    }
                    {
                        let guard = db.read().expect("db lock");
                        let mut tx = guard.begin_transaction().expect("begin");
                        tx.insert(
                            &name,
                            Record::new("only").with_metadata(json!({"g": generation})),
                        )
                        .expect("insert side");
                        tx.commit().expect("commit side");
                    }
                    append_line(&ledger, &format!("create {name} {generation}"));
                }
            });
        }
        {
            let db = db.clone();
            let done = done.clone();
            let checkpoints = checkpoints.clone();
            scope.spawn(move || {
                while !done.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(40));
                    let guard = db.read().expect("db lock");
                    match guard.checkpoint_for_resume() {
                        Ok(()) => {}
                        // The checkpoint itself may be the failing site; it
                        // counts so the run is not held open forever.
                        Err(_) if fail_io => {}
                        Err(error) => panic!("checkpoint: {error}"),
                    }
                    checkpoints.fetch_add(1, Ordering::AcqRel);
                }
            });
        }
        // Writers are the first WRITERS spawned threads; wait for them by
        // polling their ledgers rather than joining inside the scope.
        loop {
            std::thread::sleep(Duration::from_millis(20));
            let finished = commits == 0
                || writers_done.load(Ordering::Acquire) == WRITERS as u64
                    && (fail_io || checkpoints.load(Ordering::Acquire) >= MIN_CHECKPOINTS)
                || checkpoints.load(Ordering::Acquire) >= MIN_CHECKPOINTS
                    && (0..WRITERS).all(|writer| {
                        std::fs::read_to_string(ledger.join(format!("writer-{writer}")))
                            .map(|text| text.lines().last() == Some(commits.to_string().as_str()))
                            .unwrap_or(false)
                    });
            if finished {
                done.store(true, Ordering::Release);
                break;
            }
        }
    });
    let db = Arc::try_unwrap(db)
        .ok()
        .expect("sole owner")
        .into_inner()
        .expect("db lock");
    if fail_io {
        let state = match db.durability_state() {
            bicdb_core::DurabilityState::Healthy => "healthy".to_string(),
            bicdb_core::DurabilityState::WriteFailed(failure) => format!("failed {}", failure.site),
        };
        append_line(&ledger.join("state"), &state);
        if !db.durability_state().is_healthy() {
            // A fenced handle refuses close's final checkpoint; the next
            // open recovers and verifies instead.
            if db.checkpoint_for_resume().is_ok() {
                append_line(&violations, "checkpoint succeeded on a fenced database");
            }
            return;
        }
    }
    if let Ok(Some(snapshot)) = db.paged_storage_snapshot() {
        eprintln!(
            "durability child pool: evictions {} writebacks {}",
            snapshot.buffer_pool.evictions, snapshot.buffer_pool.writebacks
        );
    }
    db.close().expect("close");
}

/// One DDL round of the fail-closed matrix: errors end the round quietly,
/// and nothing is recorded unless it was acknowledged.
fn continue_ddl(db: &RwLock<BicDb>, ledger: &Path, name: &str, generation: u64) {
    {
        let mut guard = db.write().expect("db lock");
        if guard.collections().iter().any(|meta| meta.name == name) {
            if guard.drop_collection(name).is_err() {
                return;
            }
            append_line(ledger, &format!("drop {name} {generation}"));
        }
        if guard.create_collection(name).is_err() {
            return;
        }
    }
    let guard = db.read().expect("db lock");
    let committed = guard.begin_transaction().and_then(|mut tx| {
        tx.insert(
            name,
            Record::new("only").with_metadata(json!({"g": generation})),
        )?;
        tx.commit()
    });
    if committed.is_ok() {
        append_line(ledger, &format!("create {name} {generation}"));
    }
}
