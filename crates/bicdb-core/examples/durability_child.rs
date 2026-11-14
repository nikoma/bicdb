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
//! With `BICDB_TEST_FAIL_IO` set (fail-closed matrix, see
//! `docs/durability-failure-handling.md`) a failed operation does not panic.
//! A writer whose commit fails tries [`AFTER_FAILURE_ATTEMPTS`] more commits
//! and appends a line to `ledger-dir/violations` if any of them is
//! acknowledged, or if any error is not a durability failure. The other
//! threads stop at their first error. On exit the child records the
//! database's durability state in `ledger-dir/state`.
//!
//! Two more modes probe GAP-1 of `docs/recovery-semantics.md` end to end:
//! `q1-setup` commits `<commits>` rows `r-NNNNN` to collection `q1`, one per
//! transaction, each appended to `ledger-dir/q1` after commit returns.
//! `q1-split` then commits ONE transaction of `<commits>` rows `a-NNNNN`,
//! whose keys sort before every `r-` key, so the B-tree splits that it causes
//! move committed keys to new right siblings.
//!
//! With `DURABILITY_ARCHIVE=<dir>` (fail-closed archiving, see
//! `docs/durability-failure-handling.md`, section 7.5) the database runs as
//! a replication primary, takes a commit-log base backup and an online
//! page-log backup into `<dir>` before the writers start, and an archiver
//! thread feeds both archives (`<dir>/commits`, `<dir>/wal`) every 30 ms.
//! The DDL thread is off. After each archive call that returned success the
//! archiver appends to `ledger-dir/archived-commits` or
//! `ledger-dir/archived-wal` the per-writer acknowledged commits it read
//! BEFORE the call: those commits must be restorable from that archive. The
//! first failure of an archiver is recorded in `ledger-dir/archive-state`;
//! that archiver then checks once that its handle refuses, and stops.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, PagedCheckpointScheduleAdvance,
    PagedCheckpointScheduleLimits, Record, ReplicationConfig, ReplicationCursor, ReplicationMode,
    ReplicationTlsConfig, ResourceGovernor, ResourceGovernorConfig, StorageMode,
};
use serde_json::json;

/// Commits a writer still attempts after its first failure; none may succeed.
const AFTER_FAILURE_ATTEMPTS: u64 = 3;

fn fail_io_mode() -> bool {
    std::env::var_os("BICDB_TEST_FAIL_IO").is_some()
}

/// See `checkpoints` in `main`.
const MIN_CHECKPOINTS: u64 = 20;

/// How long a fail-closed run whose writers finished keeps its DDL and
/// checkpoint threads going while the armed fault has not fenced the
/// database. Reached only when the fault cannot be reached at all.
const FAULT_REACH_BOUND: Duration = Duration::from_secs(60);
pub const WRITERS: usize = 4;

/// Passphrase of the base backups taken in archive mode.
pub const ARCHIVE_KEY: &str = "durability-child-archive-passphrase-0123";

fn archive_dir() -> Option<PathBuf> {
    std::env::var_os("DURABILITY_ARCHIVE").map(PathBuf::from)
}

fn config(mode: &str) -> DbConfig {
    let config = base_config(mode);
    if archive_dir().is_some() {
        config.with_replication(ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Primary,
            cluster_id: "durability-child".to_string(),
            node_id: "child".to_string(),
            listen_addr: Some("127.0.0.1:0".to_string()),
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..ReplicationConfig::default()
        })
    } else {
        config
    }
}

fn base_config(mode: &str) -> DbConfig {
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
        let advance = match handle.tick_checkpoint_maintenance(id, &governor, now_ms()) {
            Ok(advance) => advance,
            // The checkpoint itself may be the failing site.
            Err(_) if fail_io_mode() => return,
            Err(error) => panic!("checkpoint tick: {error}"),
        };
        match advance {
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
            // With `DURABILITY_CHECKPOINTS=bounded`, the server's checkpoint
            // worker runs next to the big transaction's page apply (GAP-1 W2).
            let done = Arc::new(AtomicBool::new(false));
            let checkpoints = Arc::new(AtomicU64::new(0));
            std::thread::scope(|scope| {
                if bounded_checkpoints() {
                    let handle = db
                        .paged_checkpoint_maintenance_handle()
                        .expect("server_paged");
                    let done = done.clone();
                    let checkpoints = checkpoints.clone();
                    scope.spawn(move || run_bounded_checkpoints(handle, &done, &checkpoints));
                }
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
                // How often the page apply took the last-resort steal (a
                // decided commit larger than the pool), for the steal-path
                // tests. Recorded only once the commit has returned.
                if let Ok(Some(snapshot)) = db.paged_storage_snapshot() {
                    append_line(
                        &ledger.join("q1-steals"),
                        &snapshot.buffer_pool.unlogged_steals.to_string(),
                    );
                }
                done.store(true, Ordering::Release);
            });
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
    let archive = archive_dir();
    if let Some(dir) = &archive {
        // Bases first: a later archiver failure must still leave something
        // to roll forward from.
        let commits = dir.join("commits");
        std::fs::create_dir_all(&commits).expect("archive dir");
        if bicdb_core::log_archive::list_base_backups(&commits)
            .expect("list bases")
            .is_empty()
        {
            db.archive_base_backup(&commits, ARCHIVE_KEY)
                .expect("commit-log base backup");
            db.create_online_backup(
                dir.join("base.bicbackup"),
                dir.join("tail.bicbackup"),
                bicdb_core::BackupCreateOptions {
                    passphrase: ARCHIVE_KEY.to_string(),
                    base_backup: None,
                },
            )
            .expect("online page-log backup");
            // Archive retention must start with the base: segments sealed
            // between the base and the first archive call would otherwise
            // be checkpointed away, a gap no restore can cross.
            db.archive_wal_segments(dir.join("wal"))
                .expect("first page-log archive");
        }
    }
    let acked: Arc<Vec<AtomicU64>> = Arc::new((0..WRITERS).map(|_| AtomicU64::new(0)).collect());
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
            let acked = acked.clone();
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
                        Ok(()) => {
                            append_line(&ledger, &i.to_string());
                            acked[writer].store(i, Ordering::Release);
                        }
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
        if let Some(dir) = archive.clone() {
            let db = db.clone();
            let done = done.clone();
            let acked = acked.clone();
            let ledger = ledger.clone();
            scope.spawn(move || archiver(&db, &dir, &ledger, &acked, &done));
        }
        // A run with no commits only reopens (recovery); it does no DDL either.
        if commits > 0 && archive.is_none() {
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
        let mut writers_finished_at = None;
        loop {
            std::thread::sleep(Duration::from_millis(20));
            let wanted = if bounded_checkpoints() {
                MIN_BOUNDED_CHECKPOINTS
            } else {
                MIN_CHECKPOINTS
            };
            let writers_finished = writers_done.load(Ordering::Acquire) == WRITERS as u64;
            let enough_checkpoints = checkpoints.load(Ordering::Acquire) >= wanted;
            let finished = if commits == 0 {
                true
            } else if fail_io && archive.is_some() {
                // Archive faults do not fence the database; the archiver
                // records them itself.
                writers_finished
            } else if fail_io {
                // The fail-closed matrix names its fault by call count
                // (`page_log.sync:60`), and how many calls of a site the
                // writers cause depends on timing: in the transaction-log
                // mode commits do not sync the page log at all, only the
                // materializer and checkpoint timers do, so writers on a
                // fast disk finish before the n-th call. The DDL and
                // checkpoint threads keep exercising every site, so keep
                // the run going until the fault fenced the database. The
                // bound only ends a run whose fault can never be reached;
                // the parent then reports exactly that.
                writers_finished && {
                    let since = *writers_finished_at.get_or_insert_with(std::time::Instant::now);
                    since.elapsed() >= FAULT_REACH_BOUND
                        || !db.read().expect("db lock").durability_state().is_healthy()
                }
            } else {
                enough_checkpoints
                    && (writers_finished
                        || (0..WRITERS).all(|writer| {
                            std::fs::read_to_string(ledger.join(format!("writer-{writer}")))
                                .map(|text| {
                                    text.lines().last() == Some(commits.to_string().as_str())
                                })
                                .unwrap_or(false)
                        }))
            };
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
            // The fault may have fired after the last writer finished, so
            // no writer saw it: a commit must be refused all the same.
            let late = db.begin_transaction().and_then(|mut tx| {
                tx.insert(
                    "rows",
                    Record::new("after-fence")
                        .with_metadata(json!({"owner": WRITERS, "seq": 0, "uk": "after-fence"})),
                )?;
                tx.commit()
            });
            match late {
                Ok(()) => append_line(&violations, "commit acknowledged on a fenced database"),
                Err(error) if !error.is_durability_failure() => append_line(
                    &violations,
                    &format!("commit on a fenced database: not a durability failure: {error}"),
                ),
                Err(_) => {}
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

/// Archive mode: feed both archives until the run ends. See the module docs.
fn archiver(db: &RwLock<BicDb>, dir: &Path, ledger: &Path, acked: &[AtomicU64], done: &AtomicBool) {
    let snapshot = || {
        acked
            .iter()
            .map(|seq| seq.load(Ordering::Acquire).to_string())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let commits_dir = dir.join("commits");
    let wal_dir = dir.join("wal");
    let state = ledger.join("archive-state");
    let violations = ledger.join("violations");
    let mut archive = {
        let guard = db.read().expect("db lock");
        match guard.open_log_archive(&commits_dir, true) {
            Ok(archive) => Some(archive),
            Err(error) => {
                append_line(&state, &format!("commit_log failed open: {error}"));
                None
            }
        }
    };
    let mut cursor = ReplicationCursor::new(archive.as_ref().map_or(0, |a| a.last_commit_seq()));
    let mut wal_alive = true;
    // At least a few rounds after the writers stop, so the final commits
    // reach both archives on a run without faults.
    let mut tail_rounds = 0;
    while tail_rounds < 3 {
        if done.load(Ordering::Acquire) {
            tail_rounds += 1;
        }
        std::thread::sleep(Duration::from_millis(30));
        let guard = db.read().expect("db lock");
        if let Some(log) = archive.as_mut() {
            let before = snapshot();
            let result = loop {
                match guard.archive_commits(log, &mut cursor, 64) {
                    Ok(0) => break Ok(()),
                    Ok(_) => continue,
                    Err(error) => break Err(error),
                }
            };
            match result {
                Ok(()) => append_line(&ledger.join("archived-commits"), &before),
                Err(error) => {
                    let site = match &error {
                        bicdb_core::BicDbError::ArchiveFailed { site, .. } => site.clone(),
                        other => {
                            append_line(
                                &violations,
                                &format!("commit archive: not an archive failure: {other}"),
                            );
                            "other".to_string()
                        }
                    };
                    append_line(&state, &format!("commit_log failed {site}"));
                    if guard.archive_commits(log, &mut cursor, 64).is_ok() {
                        append_line(&violations, "commit archive accepted work after a failure");
                    }
                    archive = None;
                }
            }
        }
        if wal_alive {
            let before = snapshot();
            match guard.archive_wal_segments(&wal_dir) {
                Ok(_) => append_line(&ledger.join("archived-wal"), &before),
                Err(error) => {
                    let site = match &error {
                        bicdb_core::BicDbError::ArchiveFailed { site, .. } => site.clone(),
                        other => {
                            append_line(
                                &violations,
                                &format!("page-log archive: not an archive failure: {other}"),
                            );
                            "other".to_string()
                        }
                    };
                    append_line(&state, &format!("page_log failed {site}"));
                    if guard.archive_wal_segments(&wal_dir).is_ok() {
                        append_line(
                            &violations,
                            "page-log archive accepted work after a failure",
                        );
                    }
                    wal_alive = false;
                }
            }
        }
        if !guard.durability_state().is_healthy() {
            append_line(&violations, "an archive failure fenced the database");
        }
    }
}
