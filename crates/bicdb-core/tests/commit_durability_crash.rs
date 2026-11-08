//! Crash matrix for commit durability in `server_paged`, run identically
//! against the production path (`all-logs`) and the experimental
//! transaction-log-only path (`transaction-log`). See
//! `docs/commit-durability-invariants.md`, sections 4 and 6.
//!
//! Each case runs `examples/durability_child` (concurrent writers, a unique
//! and a B-tree index, deletes, DDL that reuses collection names, periodic
//! checkpoints with transaction-log cuts) until a named crash point aborts it.
//! Power loss of the unsynced page-log tail is emulated at the abort. The
//! parent then reopens the database and reconciles it independently against
//! the child's ledger of acknowledged commits:
//!
//! 1. every acknowledged commit is present;
//! 2. each commit is all-or-nothing (its row, its hot-row update and its
//!    delete agree with one durable prefix per writer);
//! 3. index lookups equal a scan, and the unique index resolves each key to
//!    exactly its row;
//! 4. a recreated collection never holds an older generation's row;
//! 5. a second reopen yields the identical state.
//!
//! The child must be built first: `cargo build -p bicdb-core --example
//! durability_child` (a plain `cargo test` builds examples).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use bicdb_core::{BicDb, DbConfig, IndexValue, StorageMode};

const WRITERS: usize = 4;
const COMMITS: u64 = 120;

fn child_binary() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    let examples = path.join("examples");
    let exact = examples.join("durability_child");
    if exact.exists() {
        return exact;
    }
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(&examples).into_iter().flatten().flatten() {
        let name = entry.file_name();
        let name = name.to_str().unwrap_or_default();
        if !name.starts_with("durability_child-") || name.ends_with(".d") {
            continue;
        }
        let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
            continue;
        };
        if newest.as_ref().is_none_or(|(best, _)| modified > *best) {
            newest = Some((modified, entry.path()));
        }
    }
    newest
        .map(|(_, path)| path)
        .expect("build the child first: cargo build -p bicdb-core --example durability_child")
}

/// Small enough that the workload evicts and writes back pages.
const SMALL_POOL_BYTES: u64 = 256 * 1024;

/// A case's mode is `all-logs` or `transaction-log`, optionally suffixed
/// `+small-pool` and/or `+outside-lock` (the experimental row-commit sync
/// outside the page store's writer lock, docs/commit-fsync-outside-locks.md).
fn durability(mode: &str) -> &str {
    mode.split('+').next().unwrap_or(mode)
}

fn small_pool(mode: &str) -> bool {
    mode.contains("+small-pool")
}

fn outside_lock(mode: &str) -> bool {
    mode.contains("+outside-lock")
}

const OUTSIDE_LOCK_ENV: &str = "BICDB_PAGED_COMMIT_SYNC_OUTSIDE_LOCK";

/// `+small-wal`: a page-log trigger small enough that commits run bounded
/// checkpoint steps themselves, under the commit admission other commits
/// share, so a checkpoint's writer lock meets committing transactions (the
/// committing barrier). The resume checkpoint excludes commits entirely.
const SMALL_WAL_BYTES: u64 = 256 * 1024;

fn set_outside_lock_env(command: &mut Command, mode: &str) {
    if outside_lock(mode) {
        command.env(OUTSIDE_LOCK_ENV, "1");
    } else {
        command.env_remove(OUTSIDE_LOCK_ENV);
    }
    if mode.contains("+small-wal") {
        command.env("BICDB_PAGED_WAL_MAX_BYTES", SMALL_WAL_BYTES.to_string());
    } else {
        command.env_remove("BICDB_PAGED_WAL_MAX_BYTES");
    }
}

fn config(mode: &str) -> DbConfig {
    let config = DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_transaction_log_commit_durability(durability(mode) == "transaction-log")
        .with_paged_commit_sync_outside_lock(outside_lock(mode));
    if small_pool(mode) {
        config.with_paged_buffer_pool_bytes(SMALL_POOL_BYTES)
    } else {
        config
    }
}

#[derive(Debug)]
enum Outcome {
    Crashed,
    Completed,
}

fn run_child(db: &Path, ledger: &Path, mode: &str, crash_at: Option<&str>) -> Outcome {
    run_child_with(db, ledger, mode, crash_at, COMMITS)
}

fn run_child_with(
    db: &Path,
    ledger: &Path,
    mode: &str,
    crash_at: Option<&str>,
    commits: u64,
) -> Outcome {
    let mut command = Command::new(child_binary());
    command
        .arg(db)
        .arg(ledger)
        .arg(durability(mode))
        .arg(commits.to_string())
        .env("BICDB_TEST_WAL_VOLATILE_TAIL", "1")
        .env_remove("BICDB_TEST_CRASH_AT");
    if small_pool(mode) {
        command.env("DURABILITY_POOL_BYTES", SMALL_POOL_BYTES.to_string());
    } else {
        command.env_remove("DURABILITY_POOL_BYTES");
    }
    set_outside_lock_env(&mut command, mode);
    if let Some(point) = crash_at {
        command.env("BICDB_TEST_CRASH_AT", point);
    }
    let output = command.output().expect("run durability child");
    if output.status.success() {
        return Outcome::Completed;
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("bicdb test crash point reached"),
        "child failed without reaching its crash point ({mode}, {crash_at:?}): {}\n{stderr}",
        output.status
    );
    Outcome::Crashed
}

fn ledger_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// The reconciled state: a digest of every row, compared across reopens.
fn reconcile(db_dir: &Path, ledger: &Path, mode: &str, label: &str) -> BTreeMap<String, String> {
    let db = BicDb::open_with_config(db_dir, config(mode))
        .unwrap_or_else(|error| panic!("{label}: reopen failed: {error}"));
    let mut digest = BTreeMap::new();
    if !db.collections().iter().any(|meta| meta.name == "rows") {
        // A crash during setup, before the schema was durable, leaves
        // nothing; that is only correct if nothing was acknowledged.
        for writer in 0..WRITERS {
            assert!(
                ledger_lines(&ledger.join(format!("writer-{writer}"))).is_empty(),
                "{label}: schema missing although writer {writer} has acknowledged commits"
            );
        }
        db.close().expect("close after reconcile");
        return digest;
    }
    let rows = db
        .scan_collection("rows")
        .unwrap_or_else(|error| panic!("{label}: scan rows: {error}"));
    let by_id: BTreeMap<String, serde_json::Value> = rows
        .iter()
        .map(|record| (record.id.clone(), record.metadata.clone()))
        .collect();
    for (id, metadata) in &by_id {
        digest.insert(format!("rows/{id}"), metadata.to_string());
    }
    for writer in 0..WRITERS {
        let acknowledged = ledger_lines(&ledger.join(format!("writer-{writer}")))
            .last()
            .map(|line| line.parse::<u64>().unwrap())
            .unwrap_or(0);
        let durable = by_id
            .get(&format!("hot-{writer}"))
            .map(|metadata| metadata["seq"].as_u64().unwrap())
            .unwrap_or(0);
        assert!(
            durable >= acknowledged,
            "{label}: writer {writer} acknowledged commit {acknowledged} but only {durable} survived"
        );
        for i in 1..=COMMITS {
            let id = format!("r-{writer}-{i}");
            let deleted = (i + 3) % 7 == 0 && i + 3 <= durable;
            let expected = i <= durable && !deleted;
            assert_eq!(
                by_id.contains_key(&id),
                expected,
                "{label}: {id} presence disagrees with durable prefix {durable} \
                 (acknowledged {acknowledged})"
            );
            let unique = db
                .lookup_index(
                    "rows_unique_key",
                    &[IndexValue::String(format!("u-{writer}-{i}"))],
                )
                .expect("unique lookup");
            let want: Vec<String> = if expected { vec![id.clone()] } else { vec![] };
            assert_eq!(unique, want, "{label}: unique index for {id}");
        }
        let mut via_index = db
            .lookup_index("rows_owner", &[IndexValue::Int(writer as i64)])
            .expect("owner lookup");
        via_index.sort();
        let via_scan: Vec<String> = by_id
            .iter()
            .filter(|(_, metadata)| metadata["owner"].as_u64() == Some(writer as u64))
            .map(|(id, _)| id.clone())
            .collect();
        assert_eq!(
            via_index, via_scan,
            "{label}: owner index != scan for writer {writer}"
        );
    }
    let mut created: BTreeMap<String, u64> = BTreeMap::new();
    for line in ledger_lines(&ledger.join("ddl")) {
        let parts: Vec<&str> = line.split(' ').collect();
        if parts[0] == "create" {
            created.insert(parts[1].to_string(), parts[2].parse().unwrap());
        }
    }
    for meta in db.collections() {
        if !meta.name.starts_with("side_") {
            continue;
        }
        let side = db.scan_collection(&meta.name).expect("scan side");
        assert!(
            side.len() <= 1,
            "{label}: {} holds {} rows",
            meta.name,
            side.len()
        );
        if let Some(record) = side.first() {
            let generation = record.metadata["g"].as_u64().unwrap();
            let floor = created.get(&meta.name).copied().unwrap_or(0);
            assert!(
                generation >= floor,
                "{label}: {} holds generation {generation} after generation {floor} was created",
                meta.name
            );
            digest.insert(format!("{}/only", meta.name), generation.to_string());
        } else {
            digest.insert(format!("{}/", meta.name), String::new());
        }
    }
    db.close().expect("close after reconcile");
    digest
}

fn crash_case(mode: &str, crash_at: Option<&str>, expect_crash: bool) {
    let label = format!("{mode} {}", crash_at.unwrap_or("no-crash"));
    let temp = tempfile::tempdir().unwrap();
    let keep = KeepOnPanic(temp.path().to_path_buf(), label.clone());
    let db = temp.path().join("db");
    let ledger = temp.path().join("ledger");
    let outcome = run_child(&db, &ledger, mode, crash_at);
    if expect_crash {
        assert!(
            matches!(outcome, Outcome::Crashed),
            "{label}: crash point was never reached"
        );
    }
    // The crashed, not yet recovered state, kept with the case on failure.
    let _ = Command::new("cp")
        .arg("-a")
        .arg(&db)
        .arg(temp.path().join("crashed-db"))
        .status();
    let first = reconcile(&db, &ledger, mode, &label);
    let second = reconcile(&db, &ledger, mode, &format!("{label} (second reopen)"));
    assert_eq!(first, second, "{label}: a second reopen changed the state");
    drop(keep);
}

/// On panic, copy the case directory aside for inspection.
struct KeepOnPanic(PathBuf, String);

impl Drop for KeepOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let name: String = self
                .1
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            let target = std::env::temp_dir().join(format!(
                "durability-failure-{name}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_micros())
                    .unwrap_or(0)
            ));
            let _ = std::fs::remove_dir_all(&target);
            let _ = Command::new("cp")
                .arg("-a")
                .arg(&self.0)
                .arg(&target)
                .status();
            eprintln!("kept failing case at {}", target.display());
        }
    }
}

/// Crash once in the workload, then crash again in the middle of recovery
/// replay, then recover and reconcile.
fn interrupted_recovery_case(mode: &str, crash_at: &str) {
    let label = format!("{mode} {crash_at} + replay");
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("db");
    let ledger = temp.path().join("ledger");
    assert!(matches!(
        run_child(&db, &ledger, mode, Some(crash_at)),
        Outcome::Crashed
    ));
    // Reopen in a child that dies on the second replayed write and does no
    // new work. If fewer than two writes needed replay it simply closes.
    let _ = run_child_with(&db, &ledger, mode, Some("replay.after_write:2"), 0);
    let first = reconcile(&db, &ledger, mode, &label);
    let second = reconcile(&db, &ledger, mode, &format!("{label} (second reopen)"));
    assert_eq!(first, second, "{label}: a second reopen changed the state");
}

const COMMON_POINTS: &[&str] = &[
    // Early checkpoint hits land in open and schema setup.
    "checkpoint.after_wal_sync:2",
    "commit.after_txlog_durable:5",
    "commit.after_txlog_durable:150",
    "commit.after_page_apply:7",
    "commit.after_page_apply:200",
    "commit.after_marker:9",
    "commit.after_marker:300",
    "checkpoint.after_wal_sync:9",
    "checkpoint.after_page_flush:11",
    "checkpoint.after_publish:13",
    "checkpoint.after_wal_reset:15",
    "txlog_cut.after_barrier:3",
    "txlog_cut.after_floor:2",
    // A page just reached the page file: its log records must be durable.
    "pool.after_writeback:40",
    "pool.after_writeback:120",
];

const DEFERRED_POINTS: &[&str] = &[
    "materialize.after_wal_sync:3",
    "materialize.after_wal_sync:40",
    "materialize.torn_markers:5",
    "materialize.after_markers:6",
];

/// Run every case and report all failures together, so one bad boundary
/// does not hide the rest of the matrix.
fn run_matrix(cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)>) {
    let mut failures = Vec::new();
    for (label, case) in cases {
        if let Err(panic) = std::panic::catch_unwind(case) {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            failures.push(format!("{label}: {message}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failing cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn matrix(mode: &'static str, points: Vec<&'static str>, replays: &[&'static str]) {
    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> = vec![(
        "no-crash".into(),
        Box::new(move || crash_case(mode, None, false)),
    )];
    for point in points {
        cases.push((
            point.into(),
            Box::new(move || crash_case(mode, Some(point), true)),
        ));
    }
    for &point in replays {
        cases.push((
            format!("{point} + replay"),
            Box::new(move || interrupted_recovery_case(mode, point)),
        ));
    }
    run_matrix(cases);
}

// The same matrices with a buffer pool small enough to evict, so pages reach
// the page file between checkpoints (the WAL-before-data path, I5).

#[test]
fn production_path_survives_every_crash_point_with_eviction() {
    matrix(
        "all-logs+small-pool",
        COMMON_POINTS.to_vec(),
        &["commit.after_page_apply:100"],
    );
}

#[test]
fn transaction_log_only_path_survives_every_crash_point_with_eviction() {
    matrix(
        "transaction-log+small-pool",
        COMMON_POINTS
            .iter()
            .chain(DEFERRED_POINTS)
            .copied()
            .collect(),
        &[
            "commit.after_page_apply:100",
            "materialize.after_wal_sync:20",
        ],
    );
}

#[test]
fn production_path_survives_every_crash_point() {
    matrix(
        "all-logs",
        COMMON_POINTS.to_vec(),
        &["commit.after_page_apply:100"],
    );
}

#[test]
fn transaction_log_only_path_survives_every_crash_point() {
    matrix(
        "transaction-log",
        COMMON_POINTS
            .iter()
            .chain(DEFERRED_POINTS)
            .copied()
            .collect(),
        &[
            "commit.after_page_apply:100",
            "materialize.after_wal_sync:20",
        ],
    );
}

// The same production matrices with the row-commit sync outside the writer
// lock, plus crash points inside its new windows (after the lock is
// released, after the sync before publication, after publication, and after
// a writer-lock barrier drained committing transactions).
const OUTSIDE_LOCK_POINTS: &[&str] = &[
    "commit.page.appended_unlocked:4",
    "commit.page.appended_unlocked:60",
    "commit.page.appended_unlocked:250",
    "commit.page.synced_unpublished:4",
    "commit.page.synced_unpublished:60",
    "commit.page.synced_unpublished:250",
    "commit.page.published:4",
    "commit.page.published:60",
    "commit.page.published:250",
];

/// Reached only when a checkpoint meets committing transactions (`+small-wal`).
const BARRIER_POINTS: &[&str] = &[
    "commit.barrier.drained:1",
    "commit.barrier.drained:5",
    "commit.barrier.drained:20",
];

fn outside_lock_points() -> Vec<&'static str> {
    COMMON_POINTS
        .iter()
        .chain(OUTSIDE_LOCK_POINTS)
        .copied()
        .collect()
}

#[test]
fn outside_lock_path_survives_crashes_inside_the_checkpoint_barrier() {
    matrix(
        "all-logs+outside-lock+small-wal",
        BARRIER_POINTS
            .iter()
            .chain(OUTSIDE_LOCK_POINTS)
            .copied()
            .collect(),
        &["commit.barrier.drained:5"],
    );
}

#[test]
fn outside_lock_path_survives_crashes_inside_the_checkpoint_barrier_with_eviction() {
    matrix(
        "all-logs+small-pool+outside-lock+small-wal",
        BARRIER_POINTS
            .iter()
            .chain(OUTSIDE_LOCK_POINTS)
            .copied()
            .collect(),
        &["commit.barrier.drained:5"],
    );
}

#[test]
fn outside_lock_path_survives_every_crash_point() {
    matrix(
        "all-logs+outside-lock",
        outside_lock_points(),
        &[
            "commit.after_page_apply:100",
            "commit.page.synced_unpublished:60",
        ],
    );
}

#[test]
fn outside_lock_path_survives_every_crash_point_with_eviction() {
    matrix(
        "all-logs+small-pool+outside-lock",
        outside_lock_points(),
        &[
            "commit.after_page_apply:100",
            "commit.page.appended_unlocked:60",
        ],
    );
}

#[test]
fn outside_lock_path_fails_closed_at_every_io_failure_site() {
    fail_io_matrix("all-logs+outside-lock");
}

#[test]
fn outside_lock_path_fails_closed_at_every_io_failure_site_with_eviction() {
    fail_io_matrix("all-logs+small-pool+outside-lock");
}

// Fail-closed matrix (docs/durability-failure-handling.md): instead of a
// crash, a durability-critical write or sync returns EIO at its n-th call.
// The child keeps running and must not acknowledge anything after the
// failure; the reopen then runs recovery plus the verification checkpoint,
// and the same reconciler proves every acknowledged commit survived.

const FAIL_IO_SPECS: &[&str] = &[
    // transactions.log: nothing written / written but not synced / written,
    // not synced and the tail repair failed too (the decision stays on disk
    // and recovery must honour it).
    "transaction_log.write:150",
    "transaction_log.sync:150",
    "transaction_log.sync:150,transaction_log.repair:1",
    // Page log and page file, in commits, writebacks and checkpoints.
    "page_log.sync:60",
    "page_log.write:300",
    "page_file.sync:12",
    "page_log.reset:3",
    // The commit point of a transaction-log cut.
    "transaction_log.cut_sync:3",
];

fn fail_io_case(mode: &str, spec: &str) {
    let label = format!("{mode} fail-io {spec}");
    let temp = tempfile::tempdir().unwrap();
    let keep = KeepOnPanic(temp.path().to_path_buf(), label.clone());
    let db = temp.path().join("db");
    let ledger = temp.path().join("ledger");
    let mut command = Command::new(child_binary());
    command
        .arg(&db)
        .arg(&ledger)
        .arg(durability(mode))
        .arg(COMMITS.to_string())
        .env("BICDB_TEST_FAIL_IO", spec)
        .env_remove("BICDB_TEST_WAL_VOLATILE_TAIL")
        .env_remove("BICDB_TEST_CRASH_AT");
    if small_pool(mode) {
        command.env("DURABILITY_POOL_BYTES", SMALL_POOL_BYTES.to_string());
    } else {
        command.env_remove("DURABILITY_POOL_BYTES");
    }
    set_outside_lock_env(&mut command, mode);
    let output = command.output().expect("run durability child");
    assert!(
        output.status.success(),
        "{label}: child failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let state = ledger_lines(&ledger.join("state"));
    assert!(
        state.len() == 1 && state[0].starts_with("failed "),
        "{label}: the fault was never reached or did not fence the database: {state:?}"
    );
    let violations = ledger_lines(&ledger.join("violations"));
    assert!(violations.is_empty(), "{label}: {violations:#?}");
    let marker = db.join(bicdb_core::DURABILITY_FAILURE_MARKER);
    assert!(
        marker.exists(),
        "{label}: no failure marker for the next open"
    );
    let first = reconcile(&db, &ledger, mode, &label);
    assert!(
        !marker.exists(),
        "{label}: verification did not clear the marker"
    );
    let second = reconcile(&db, &ledger, mode, &format!("{label} (second reopen)"));
    assert_eq!(first, second, "{label}: a second reopen changed the state");
    drop(keep);
}

fn fail_io_matrix(mode: &'static str) {
    run_matrix(
        FAIL_IO_SPECS
            .iter()
            .map(|&spec| {
                (
                    spec.to_string(),
                    Box::new(move || fail_io_case(mode, spec))
                        as Box<dyn FnOnce() + std::panic::UnwindSafe>,
                )
            })
            .collect(),
    );
}

#[test]
fn production_path_fails_closed_at_every_io_failure_site() {
    fail_io_matrix("all-logs");
}

#[test]
fn production_path_fails_closed_at_every_io_failure_site_with_eviction() {
    fail_io_matrix("all-logs+small-pool");
}

#[test]
fn transaction_log_only_path_fails_closed_at_every_io_failure_site() {
    fail_io_matrix("transaction-log");
}

/// Diagnostic: repeat one case. `DURABILITY_CASE=<mode>/<point>` and
/// `DURABILITY_REPEAT=<n>`; failing case directories are kept.
#[test]
#[ignore]
fn repeat_one_case() {
    let spec = std::env::var("DURABILITY_CASE").expect("DURABILITY_CASE=<mode>/<point>");
    let repeat: usize = std::env::var("DURABILITY_REPEAT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let (mode, point) = spec.split_once('/').expect("<mode>/<point>");
    let mode: &'static str = Box::leak(mode.to_string().into_boxed_str());
    let point: &'static str = Box::leak(point.to_string().into_boxed_str());
    let cases = (0..repeat)
        .map(|round| {
            (
                format!("round {round}"),
                Box::new(move || crash_case(mode, Some(point), true))
                    as Box<dyn FnOnce() + std::panic::UnwindSafe>,
            )
        })
        .collect();
    run_matrix(cases);
}

/// Merge-gate qualification: repeat the core cases of the fail-closed matrix
/// and of the production crash matrix `DURABILITY_ROUNDS` times (default
/// 100) on `DURABILITY_WORKERS` threads (default 4). Every case reconciles
/// against its ledger as in the ordinary matrices. Reports every failure.
#[test]
#[ignore]
fn repeat_core_matrices() {
    let rounds: usize = std::env::var("DURABILITY_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);
    let workers: usize = std::env::var("DURABILITY_WORKERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4)
        .max(1);
    #[derive(Clone, Copy)]
    enum Case {
        FailIo(&'static str, &'static str),
        Crash(&'static str, &'static str),
    }
    let mut cases = Vec::new();
    for round in 0..rounds {
        for mode in ["all-logs", "all-logs+small-pool"] {
            for &spec in FAIL_IO_SPECS {
                cases.push((round, Case::FailIo(mode, spec)));
            }
        }
        for &point in COMMON_POINTS {
            cases.push((round, Case::Crash("all-logs", point)));
        }
    }
    // `DURABILITY_ONLY=<substring>` keeps only matching cases (diagnosis).
    if let Ok(only) = std::env::var("DURABILITY_ONLY") {
        cases.retain(|(_, case)| match case {
            Case::FailIo(mode, spec) => format!("{mode} fail-io {spec}").contains(&only),
            Case::Crash(mode, point) => format!("{mode} crash {point}").contains(&only),
        });
    }
    let total = cases.len();
    let queue = std::sync::Mutex::new(cases.into_iter());
    let failures = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let Some((round, case)) = queue.lock().unwrap().next() else {
                    return;
                };
                let (label, result) = match case {
                    Case::FailIo(mode, spec) => (
                        format!("round {round} {mode} fail-io {spec}"),
                        std::panic::catch_unwind(|| fail_io_case(mode, spec)),
                    ),
                    Case::Crash(mode, point) => (
                        format!("round {round} {mode} crash {point}"),
                        std::panic::catch_unwind(|| crash_case(mode, Some(point), true)),
                    ),
                };
                if let Err(panic) = result {
                    let message = panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default();
                    failures.lock().unwrap().push(format!("{label}: {message}"));
                }
            });
        }
    });
    let failures = failures.into_inner().unwrap();
    eprintln!(
        "repeat_core_matrices: {total} cases ({rounds} rounds), {} failed",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} of {total} cases failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
