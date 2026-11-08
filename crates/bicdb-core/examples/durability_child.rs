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
//! `DURABILITY_WAL_MAX_BYTES` lowers the page-log threshold at which commits
//! advance the bounded checkpoint (`checkpoint_step`) themselves, and
//! `DURABILITY_WAL_SEGMENT_BYTES` the size at which the page log seals into
//! `store.wal.<lsn>` segments. `DURABILITY_CHECKPOINTS=bounded` replaces the
//! resume-checkpoint thread with the server's bounded checkpoint supervisor
//! (`start_checkpoint_maintenance` / `tick_checkpoint_maintenance` through the
//! storage-only handle, without the database guard, as the server runs it):
//! the transaction log is then cut only at open and close, and the run lasts
//! until enough bounded checkpoints completed.
//!
//! Exit 0 after every writer finishes, unless a crash point aborts first.
//!
//! Two more modes probe GAP-1 of `docs/recovery-semantics.md` end to end:
//! `q1-setup` commits `<commits>` rows `r-NNNNN` to collection `q1`, one per
//! transaction, each appended to `ledger-dir/q1` after commit returns.
//! `q1-split` then commits ONE transaction of `<commits>` rows `a-NNNNN`,
//! whose keys sort before every `r-` key, so the B-tree splits that it causes
//! move committed keys to new right siblings.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, PagedCheckpointScheduleAdvance,
    PagedCheckpointScheduleLimits, Record, ResourceGovernor, ResourceGovernorConfig, StorageMode,
};
use serde_json::json;

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
    let env = |name: &str| std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok());
    let config = match env("DURABILITY_POOL_BYTES") {
        Some(bytes) => config.with_paged_buffer_pool_bytes(bytes),
        None => config,
    };
    let config = match env("DURABILITY_WAL_MAX_BYTES") {
        Some(bytes) => config.with_paged_wal_max_bytes(bytes),
        None => config,
    };
    match env("DURABILITY_WAL_SEGMENT_BYTES") {
        Some(bytes) => config.with_paged_wal_segment_bytes(bytes),
        None => config,
    }
}

fn bounded_checkpoints() -> bool {
    std::env::var("DURABILITY_CHECKPOINTS").is_ok_and(|value| value == "bounded")
}

/// Completed bounded checkpoints a `bounded` run waits for.
const MIN_BOUNDED_CHECKPOINTS: u64 = 6;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// The server's checkpoint worker: one supervised bounded phase per tick,
/// resuming an operation a crash left active.
fn run_bounded_checkpoints(
    handle: bicdb_core::PagedCheckpointMaintenanceHandle,
    done: &AtomicBool,
    completed: &AtomicU64,
) {
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), now_ms())
        .expect("resource governor");
    let mut limits = PagedCheckpointScheduleLimits::default();
    limits.step_interval_ms = 1;
    let mut operation = None;
    while !done.load(Ordering::Acquire) {
        let id = match operation {
            Some(id) => id,
            None => {
                let id = match handle.start_checkpoint_maintenance(now_ms(), limits) {
                    Ok(schedule) => schedule.operation_id,
                    Err(_) => {
                        handle
                            .checkpoint_maintenance_status()
                            .expect("checkpoint status")
                            .expect("an active checkpoint operation")
                            .operation_id
                    }
                };
                operation = Some(id);
                id
            }
        };
        match handle
            .tick_checkpoint_maintenance(id, &governor, now_ms())
            .expect("checkpoint tick")
        {
            PagedCheckpointScheduleAdvance::Complete { .. } => {
                completed.fetch_add(1, Ordering::AcqRel);
                operation = None;
            }
            PagedCheckpointScheduleAdvance::Paused { reason } => {
                panic!("bounded checkpoint paused: {reason}")
            }
            _ => {}
        }
        std::thread::sleep(Duration::from_millis(2));
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

fn q1(db_dir: &Path, ledger: &Path, phase: &str, rows: u64) {
    let db = BicDb::open_with_config(db_dir, config("all-logs")).expect("open db");
    if !db.collections().iter().any(|meta| meta.name == "q1") {
        let mut db = db;
        db.create_collection("q1").expect("create q1");
        db.close().expect("close");
        return q1(db_dir, ledger, phase, rows);
    }
    match phase {
        "setup" => {
            for i in 0..rows {
                let mut tx = db.begin_transaction().expect("begin");
                tx.insert(
                    "q1",
                    Record::new(format!("r-{i:05}")).with_metadata(json!({"i": i})),
                )
                .expect("insert");
                tx.commit().expect("commit");
                append_line(&ledger.join("q1"), &format!("r-{i:05}"));
            }
        }
        "split" => {
            let mut tx = db.begin_transaction().expect("begin");
            for i in 0..rows {
                tx.insert(
                    "q1",
                    Record::new(format!("a-{i:05}")).with_metadata(json!({"i": i})),
                )
                .expect("insert");
            }
            tx.commit().expect("commit");
            append_line(&ledger.join("q1"), "split");
        }
        other => panic!("unknown q1 phase {other}"),
    }
    db.close().expect("close");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let db_dir = PathBuf::from(args.next().expect("db dir"));
    let ledger = PathBuf::from(args.next().expect("ledger dir"));
    let mode = args.next().expect("mode");
    let commits: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(400);
    std::fs::create_dir_all(&ledger).expect("ledger dir");

    if let Some(phase) = mode.strip_prefix("q1-") {
        q1(&db_dir, &ledger, phase, commits);
        return;
    }

    let mut db = BicDb::open_with_config(&db_dir, config(&mode)).expect("open db");
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

    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let db = db.clone();
            let ledger = ledger.join(format!("writer-{writer}"));
            scope.spawn(move || {
                for i in 1..=commits {
                    let guard = db.read().expect("db lock");
                    let mut tx = guard.begin_transaction().expect("begin");
                    tx.insert(
                        "rows",
                        Record::new(format!("r-{writer}-{i}")).with_metadata(
                            json!({"owner": writer, "seq": i, "uk": format!("u-{writer}-{i}")}),
                        ),
                    )
                    .expect("insert");
                    tx.update(
                        "rows",
                        Record::new(format!("hot-{writer}")).with_metadata(
                            json!({"owner": writer, "seq": i, "uk": format!("h-{writer}")}),
                        ),
                    )
                    .expect("upsert hot");
                    if i % 7 == 0 && i > 3 {
                        tx.delete("rows", &format!("r-{writer}-{}", i - 3))
                            .expect("delete");
                    }
                    tx.commit().expect("commit");
                    drop(guard);
                    append_line(&ledger, &i.to_string());
                }
            });
        }
        // A run with no commits only reopens (recovery); it does no DDL either.
        if commits > 0 {
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
        if bounded_checkpoints() && commits > 0 {
            let handle = db
                .read()
                .expect("db lock")
                .paged_checkpoint_maintenance_handle()
                .expect("server_paged");
            let done = done.clone();
            let checkpoints = checkpoints.clone();
            scope.spawn(move || run_bounded_checkpoints(handle, &done, &checkpoints));
        } else {
            let db = db.clone();
            let done = done.clone();
            let checkpoints = checkpoints.clone();
            scope.spawn(move || {
                while !done.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(40));
                    let guard = db.read().expect("db lock");
                    guard.checkpoint_for_resume().expect("checkpoint");
                    checkpoints.fetch_add(1, Ordering::AcqRel);
                }
            });
        }
        // Writers are the first WRITERS spawned threads; wait for them by
        // polling their ledgers rather than joining inside the scope.
        loop {
            std::thread::sleep(Duration::from_millis(20));
            let wanted = if bounded_checkpoints() {
                MIN_BOUNDED_CHECKPOINTS
            } else {
                MIN_CHECKPOINTS
            };
            let finished = commits == 0
                || checkpoints.load(Ordering::Acquire) >= wanted
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
    if let Ok(Some(snapshot)) = db.paged_storage_snapshot() {
        eprintln!(
            "durability child pool: evictions {} writebacks {}",
            snapshot.buffer_pool.evictions, snapshot.buffer_pool.writebacks
        );
    }
    db.close().expect("close");
}
