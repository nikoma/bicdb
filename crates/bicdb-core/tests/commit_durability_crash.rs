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

/// Page-log segment size for `+segments` cases: small enough that the page
/// log seals several `store.wal.<lsn>` segments between checkpoints.
const SMALL_SEGMENT_BYTES: u64 = 64 * 1024;

/// Page-log threshold for `+bounded` cases. Those run the server's bounded
/// checkpoint supervisor instead of resume checkpoints, and with this
/// threshold the commits also advance their own bounded checkpoint
/// (`checkpoint_step`), one phase per commit.
const SMALL_WAL_MAX_BYTES: u64 = 256 * 1024;

/// A case's mode is `all-logs` or `transaction-log`, optionally followed by
/// `+small-pool`, `+segments` and `+bounded` (any order).
fn durability(mode: &str) -> &str {
    mode.split('+').next().unwrap_or(mode)
}

fn has_flag(mode: &str, flag: &str) -> bool {
    mode.split('+').skip(1).any(|part| part == flag)
}

fn small_pool(mode: &str) -> bool {
    has_flag(mode, "small-pool")
}

fn config(mode: &str) -> DbConfig {
    let config = DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_transaction_log_commit_durability(durability(mode) == "transaction-log");
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
    if has_flag(mode, "segments") {
        command.env(
            "DURABILITY_WAL_SEGMENT_BYTES",
            SMALL_SEGMENT_BYTES.to_string(),
        );
    } else {
        command.env_remove("DURABILITY_WAL_SEGMENT_BYTES");
    }
    if has_flag(mode, "bounded") {
        command
            .env("DURABILITY_WAL_MAX_BYTES", SMALL_WAL_MAX_BYTES.to_string())
            .env("DURABILITY_CHECKPOINTS", "bounded");
    } else {
        command
            .env_remove("DURABILITY_WAL_MAX_BYTES")
            .env_remove("DURABILITY_CHECKPOINTS");
    }
    if let Some(point) = crash_at {
        command.env("BICDB_TEST_CRASH_AT", point);
    }
    let output = output_with_deadline(command, CHILD_DEADLINE)
        .unwrap_or_else(|| panic!("durability child hung ({mode}, {crash_at:?})"));
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

/// A child that runs longer than this is killed and reported as hung (a
/// deadlock at a crash point, for example), instead of stalling the matrix.
const CHILD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);

fn output_with_deadline(
    mut command: Command,
    deadline: std::time::Duration,
) -> Option<std::process::Output> {
    use std::io::Read;
    let mut child = command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn durability child");
    let mut stderr = child.stderr.take().expect("child stderr");
    let reader = std::thread::spawn(move || {
        let mut text = Vec::new();
        let _ = stderr.read_to_end(&mut text);
        text
    });
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait for durability child") {
            break status;
        }
        if started.elapsed() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    Some(std::process::Output {
        status,
        stdout: Vec::new(),
        stderr: reader.join().unwrap_or_default(),
    })
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
    let floors = (0..WRITERS)
        .map(|writer| acknowledged(ledger, writer))
        .collect::<Vec<_>>();
    reconcile_against(db_dir, ledger, config(mode), &floors, label)
}

/// Writer `writer`'s last acknowledged commit.
fn acknowledged(ledger: &Path, writer: usize) -> u64 {
    ledger_lines(&ledger.join(format!("writer-{writer}")))
        .last()
        .map(|line| line.parse::<u64>().unwrap())
        .unwrap_or(0)
}

/// [`reconcile`] with an explicit floor per writer: every commit up to the
/// floor must be present, and the state must be one consistent prefix per
/// writer. A restore from an archive uses the floor the archive confirmed.
fn reconcile_against(
    db_dir: &Path,
    ledger: &Path,
    config: DbConfig,
    floors: &[u64],
    label: &str,
) -> BTreeMap<String, String> {
    let db = BicDb::open_with_config(db_dir, config)
        .unwrap_or_else(|error| panic!("{label}: reopen failed: {error}"));
    let mut digest = BTreeMap::new();
    if !db.collections().iter().any(|meta| meta.name == "rows") {
        // A crash during setup, before the schema was durable, leaves
        // nothing; that is only correct if nothing was acknowledged.
        for (writer, floor) in floors.iter().enumerate() {
            assert!(
                *floor == 0,
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
        let acknowledged = floors[writer];
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
    // A commit's page images are in the log, its page commit record is not.
    "paged.after_page_images:7",
    "paged.after_page_images:150",
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
    // The collection catalog (DDL thread): memory is ahead of the file at
    // every step, so each fences; the directory sync follows the rename.
    "catalog.write:4",
    "catalog.dir_sync:5",
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
    if has_flag(mode, "segments") {
        command.env(
            "DURABILITY_WAL_SEGMENT_BYTES",
            SMALL_SEGMENT_BYTES.to_string(),
        );
    } else {
        command.env_remove("DURABILITY_WAL_SEGMENT_BYTES");
    }
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

/// Every step of the page-log reset, with sealed segments present: a failed
/// segment unlink (first and a later one), directory sync, truncation and
/// truncation sync. Each must fence the database and recover every
/// acknowledged commit. Opening a new database resets the log twice (the
/// fresh store's checkpoint and the recovery checkpoint), so truncation
/// counts start above that.
const RESET_FAIL_IO_SPECS: &[&str] = &[
    "page_log.reset_unlink:1",
    "page_log.reset_unlink:4",
    "page_log.reset_dir_sync:1",
    "page_log.reset_dir_sync:3",
    "page_log.reset:4",
    "page_log.reset_sync:4",
];

#[test]
fn production_path_fails_closed_at_every_log_reset_step() {
    run_specs("all-logs+segments", RESET_FAIL_IO_SPECS);
}

#[test]
fn production_path_fails_closed_at_every_log_reset_step_with_eviction() {
    run_specs("all-logs+small-pool+segments", RESET_FAIL_IO_SPECS);
}

fn fail_io_matrix(mode: &'static str) {
    run_specs(mode, FAIL_IO_SPECS);
}

fn run_specs(mode: &'static str, specs: &'static [&'static str]) {
    run_matrix(
        specs
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

// ---------------------------------------------------------------------------
// Recovery semantics (`docs/recovery-semantics.md`): every checkpoint phase
// interrupted, recovery itself interrupted, and convergence of an interrupted
// recovery to the uninterrupted one.

/// Phases of the full checkpoint (`PagedStore::checkpoint`, the resume
/// checkpoint's page half) and of the page-log seal.
const FULL_CHECKPOINT_POINTS: &[&str] = &[
    "checkpoint.after_status_spill:5",
    "checkpoint.after_status_spill:14",
    "checkpoint.after_wal_sync:6",
    "checkpoint.after_page_flush:7",
    "checkpoint.after_publish:8",
    "checkpoint.after_wal_reset:10",
    "checkpoint.after_tail_reclaim:4",
    "checkpoint.after_structural_frees:9",
    "checkpoint.after_structural_frees:16",
    "wal_seal.after_rename:1",
    "wal_seal.after_rename:5",
];

/// Phases of the bounded checkpoint (`PagedStore::checkpoint_step`), driven
/// by the supervisor and by the commits themselves.
const BOUNDED_CHECKPOINT_POINTS: &[&str] = &[
    "checkpoint_step.after_drain:2",
    "checkpoint_step.after_drain:12",
    "checkpoint_step.after_freeze:2",
    "checkpoint_step.after_freeze:8",
    "checkpoint_step.after_finalize_drain:2",
    "checkpoint_step.after_status_spill:2",
    "checkpoint_step.after_meta_writeback:3",
    "checkpoint_step.after_finish:1",
    "checkpoint_step.after_finish:5",
    "checkpoint_step.after_tail_reclaim:2",
    // Shared with the full path (`Checkpointer::finish_checkpoint`). Hit 1 is
    // the full checkpoint at open.
    "checkpoint.after_publish:3",
    "checkpoint.after_wal_reset:3",
];

#[test]
fn production_path_survives_interrupted_full_checkpoints() {
    matrix("all-logs+segments", FULL_CHECKPOINT_POINTS.to_vec(), &[]);
}

#[test]
fn production_path_survives_interrupted_full_checkpoints_with_eviction() {
    matrix(
        "all-logs+small-pool+segments",
        FULL_CHECKPOINT_POINTS.to_vec(),
        &[],
    );
}

#[test]
fn production_path_survives_interrupted_bounded_checkpoints() {
    matrix("all-logs+bounded", BOUNDED_CHECKPOINT_POINTS.to_vec(), &[]);
}

#[test]
fn production_path_survives_interrupted_bounded_checkpoints_with_eviction() {
    matrix(
        "all-logs+small-pool+bounded",
        BOUNDED_CHECKPOINT_POINTS.to_vec(),
        &[],
    );
}

#[test]
fn transaction_log_only_path_survives_interrupted_checkpoints() {
    matrix(
        "transaction-log+segments",
        FULL_CHECKPOINT_POINTS.to_vec(),
        &[],
    );
    matrix(
        "transaction-log+bounded",
        BOUNDED_CHECKPOINT_POINTS.to_vec(),
        &[],
    );
}

/// GAP-2: a crash at every step of the page-log reset while sealed segments
/// exist. The reset used to truncate the active file before deleting the
/// sealed segments, and a crash in between replayed a stale chain over newer
/// pages (3 of 4 cases lost acknowledged commits). It now deletes the
/// segments first and truncates the active file last.
#[test]
fn production_path_survives_an_interrupted_log_reset() {
    matrix(
        "all-logs+segments",
        vec![
            "wal_reset.after_segment_delete:1",
            "wal_reset.after_segment_delete:4",
            "wal_reset.after_segments_removed:2",
            "wal_reset.after_segments_removed:6",
            "wal_reset.after_active_truncate:2",
            "wal_reset.after_active_truncate:6",
        ],
        &[],
    );
}

#[test]
fn production_path_survives_an_interrupted_log_reset_with_eviction() {
    matrix(
        "all-logs+small-pool+segments",
        vec![
            "wal_reset.after_segment_delete:2",
            "wal_reset.after_segments_removed:3",
            "wal_reset.after_active_truncate:4",
        ],
        &[],
    );
}

/// GAP-1 end to end on the production row-commit path. 150 rows commit one
/// by one and are acknowledged; the database closes cleanly, so every one of
/// them is only in the page file. One transaction then inserts 2,000 rows
/// whose keys sort first. Its page apply splits the B-tree root. The crash
/// lands at `crash_at` inside or right after that page apply. The
/// transaction is durable in the transaction log (its decision precedes the
/// page apply), so recovery replays it. Every acknowledged row must survive,
/// and the big transaction must be all or nothing. Reconciled against the
/// child's ledger, which records a row only after its commit returned.
fn gap1_split_case(mode: &str, crash_at: &str) {
    gap1_split_case_with(mode, crash_at, true)
}

/// `require_crash = false` accepts a run in which `crash_at` is never reached
/// (the transaction then completes and must be fully present).
fn gap1_split_case_with(mode: &str, crash_at: &str, require_crash: bool) {
    let temp = tempfile::tempdir().unwrap();
    let keep = KeepOnPanic(temp.path().to_path_buf(), format!("gap1 {mode} {crash_at}"));
    let db_dir = temp.path().join("db");
    let ledger = temp.path().join("ledger");
    let flags: String = mode
        .split('+')
        .skip(1)
        .map(|flag| format!("+{flag}"))
        .collect();
    assert!(matches!(
        run_child_with(&db_dir, &ledger, &format!("q1-setup{flags}"), None, 150),
        Outcome::Completed
    ));
    let outcome = run_child_with(
        &db_dir,
        &ledger,
        &format!("q1-split{flags}"),
        Some(crash_at),
        2_000,
    );
    assert!(
        !require_crash || matches!(outcome, Outcome::Crashed),
        "{crash_at} must be reached during the big transaction"
    );
    let lines = ledger_lines(&ledger.join("q1"));
    let acknowledged: Vec<String> = lines
        .iter()
        .filter(|line| line.starts_with("r-"))
        .cloned()
        .collect();
    assert_eq!(acknowledged.len(), 150);
    let split_acknowledged = lines.iter().any(|line| line == "split");
    let db = BicDb::open_with_config(&db_dir, config(&format!("all-logs{flags}")))
        .unwrap_or_else(|error| panic!("reopen after the split crash failed: {error}"));
    let mut missing = Vec::new();
    for id in &acknowledged {
        match db.get("q1", id) {
            Ok(Some(_)) => {}
            Ok(None) => missing.push(format!("{id}: absent")),
            Err(error) => missing.push(format!("{id}: {error}")),
        }
    }
    let replayed = db
        .scan_collection("q1")
        .map(|rows| rows.iter().filter(|row| row.id.starts_with("a-")).count())
        .unwrap_or_else(|error| panic!("scan after the split crash failed: {error}"));
    db.close().expect("close");
    assert!(
        missing.is_empty(),
        "{} of 150 acknowledged rows lost, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    assert!(
        replayed == 0 || replayed == 2_000,
        "the big transaction is partial after recovery: {replayed} of 2000 rows"
    );
    assert!(
        !split_acknowledged || replayed == 2_000,
        "the big transaction was acknowledged but did not survive"
    );
    drop(keep);
}

/// W3: the crash lands right after the root-split flush. Before the
/// interim, 124 of the 150 acknowledged rows were lost here.
#[test]
fn gap1_a_crash_after_an_uncommitted_root_split_keeps_acknowledged_rows() {
    gap1_split_case("all-logs", "btree.after_root_split_flush:1");
}

/// The shape of the saved P0 crash state (`all-logs`, `pool.after_writeback:40`,
/// about 2 in 240 runs on one head): the root-split flush wrote the split
/// leaf, its new right sibling and the new root under the uncommitted page
/// transaction, the crash landed inside that flush, and the reopen refused
/// with "page 6 is beyond the end of the file". Made deterministic here: the
/// crash lands at each of the first writebacks of the process that runs the
/// splitting transaction, which before the interim fell inside the
/// root-split flush. With no-steal that flush writes no unlogged page, so
/// these points land later or never; either way every acknowledged row must
/// survive and the reopen must succeed.
#[test]
fn gap1_a_crash_inside_the_root_split_flush_reopens_and_keeps_acknowledged_rows() {
    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> = Vec::new();
    for hit in 1..=8u32 {
        let point: &'static str = Box::leak(format!("pool.after_writeback:{hit}").into_boxed_str());
        cases.push((
            point.into(),
            Box::new(move || gap1_split_case_with("all-logs", point, false)),
        ));
    }
    run_matrix(cases);
}

/// The big transaction's page apply interrupted at its other boundaries:
/// after its page images are logged but before its page commit record (the
/// window in which a commit's pages used to count as logged already), after
/// the page apply, and at the transaction-log decision.
#[test]
fn gap1_a_crash_at_every_boundary_of_a_splitting_page_apply_keeps_acknowledged_rows() {
    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> = Vec::new();
    for point in [
        "commit.after_txlog_durable:1",
        "btree.after_root_split_flush:1",
        "paged.after_page_images:1",
        "commit.after_page_apply:1",
    ] {
        cases.push((
            point.into(),
            Box::new(move || gap1_split_case("all-logs+bounded", point)),
        ));
    }
    run_matrix(cases);
}

/// Replay at open of a decided commit larger than the pool. The crash lands
/// right after the 2,000-row transaction's decision is durable in the
/// transaction log and before its page apply; the next open replays it into
/// a 32-frame pool. The replay must complete through structural cuts,
/// preserving every previously acknowledged row.
#[test]
fn a_decided_commit_larger_than_the_pool_replays_at_open() {
    gap1_split_case_with("all-logs+small-pool", "commit.after_txlog_durable:1", true);
}

/// The former last-resort path must now report zero unlogged steals.
/// Crash after its complete page apply and recover every acknowledged row.
#[test]
fn the_last_resort_steal_path_recovers_after_the_stealing_commit() {
    let temp = tempfile::tempdir().unwrap();
    let db_dir = temp.path().join("db");
    let ledger = temp.path().join("ledger");
    assert!(matches!(
        run_child_with(&db_dir, &ledger, "q1-setup+small-pool", None, 150),
        Outcome::Completed
    ));
    assert!(matches!(
        run_child_with(&db_dir, &ledger, "q1-split+small-pool", None, 2_000),
        Outcome::Completed
    ));
    let steals: u64 = ledger_lines(&ledger.join("q1-steals"))
        .last()
        .and_then(|line| line.parse().ok())
        .expect("the child records its steal count");
    assert_eq!(steals, 0, "structural cuts must never steal unlogged pages");

    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> = Vec::new();
    for point in ["commit.after_page_apply:1", "commit.after_marker:1"] {
        cases.push((
            point.into(),
            Box::new(move || gap1_split_case_with("all-logs+small-pool", point, false)),
        ));
    }
    run_matrix(cases);
}

/// A decided logical commit larger than the pool must preserve earlier acknowledgements.
#[test]

fn gap1_a_decided_commit_larger_than_the_pool_keeps_acknowledged_rows() {
    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> = Vec::new();
    for point in [
        "pool.after_writeback:20",
        "btree.after_root_split_flush:1",
        "paged.after_page_images:1",
    ] {
        cases.push((
            point.into(),
            Box::new(move || gap1_split_case("all-logs+small-pool", point)),
        ));
    }
    run_matrix(cases);
}

/// Reopens of a crashed database that do no new work, each a chain: the
/// first reopen crashes at the first point, the next at the second, and so
/// on. A point a reopen never reaches lets that reopen finish normally.
const RECOVERY_CRASH_CHAINS: &[&[&str]] = &[
    &["page_recovery.after_scan:1"],
    &["page_recovery.after_page_replay:1"],
    &["page_recovery.after_page_replay:20"],
    &["page_recovery.after_flush:1"],
    &["replay.after_write:1"],
    &["replay.after_write:3"],
    &["checkpoint.after_status_spill:1"],
    &["checkpoint.after_page_flush:1"],
    &["checkpoint.after_publish:1"],
    &["checkpoint.after_wal_reset:1"],
    &["open.after_recovery_checkpoint:1"],
    &["open.after_txlog_floor:1"],
    &[
        "replay.after_write:2",
        "page_recovery.after_page_replay:2",
        "replay.after_write:1",
        "checkpoint.after_publish:1",
        "open.after_txlog_floor:1",
    ],
];

fn copy_dir(from: &Path, to: &Path) {
    let status = Command::new("cp")
        .arg("-a")
        .arg(from)
        .arg(to)
        .status()
        .expect("cp");
    assert!(
        status.success(),
        "copy {} -> {}",
        from.display(),
        to.display()
    );
}

fn digest_difference(a: &BTreeMap<String, String>, b: &BTreeMap<String, String>) -> String {
    let mut lines = Vec::new();
    for (key, value) in a {
        match b.get(key) {
            Some(other) if other == value => {}
            other => lines.push(format!("{key}: {value} vs {other:?}")),
        }
    }
    for key in b.keys().filter(|key| !a.contains_key(*key)) {
        lines.push(format!("{key}: absent vs {:?}", b[key]));
    }
    let total = lines.len();
    lines.truncate(4);
    format!("{total} differing entries, e.g. {}", lines.join("; "))
}

/// Crash a workload at `workload_point`, then recover the same crashed state
/// once without interruption (the baseline) and once per recovery chain. Every
/// interrupted recovery must reconcile against the ledger and end in exactly
/// the baseline state, and reopening it again must change nothing. `rounds`
/// repeats the whole procedure with fresh crash states.
fn recovery_convergence(mode: &'static str, workload_point: &'static str, rounds: usize) {
    let mut failures = Vec::new();
    for round in 0..rounds {
        let temp = tempfile::tempdir().unwrap();
        let crashed = temp.path().join("crashed");
        let ledger = temp.path().join("ledger");
        assert!(
            matches!(
                run_child(&crashed, &ledger, mode, Some(workload_point)),
                Outcome::Crashed
            ),
            "{mode} {workload_point}: workload crash point never reached"
        );
        let base = temp.path().join("baseline");
        copy_dir(&crashed, &base);
        let label = format!("{mode} {workload_point} round {round}");
        let baseline = reconcile(&base, &ledger, mode, &format!("{label} baseline"));
        for (index, chain) in RECOVERY_CRASH_CHAINS.iter().enumerate() {
            let db = temp.path().join(format!("case-{index}"));
            copy_dir(&crashed, &db);
            let case = format!("{label} recovery {chain:?}");
            let outcome = std::panic::catch_unwind(|| {
                let mut reached = Vec::new();
                for point in chain.iter() {
                    if matches!(
                        run_child_with(&db, &ledger, mode, Some(point), 0),
                        Outcome::Crashed
                    ) {
                        reached.push(*point);
                    }
                }
                let case = format!("{case} (crashed at {reached:?})");
                let first = reconcile(&db, &ledger, mode, &case);
                let again = reconcile(&db, &ledger, mode, &format!("{case} (reopen)"));
                (case, first, again)
            });
            match outcome {
                Err(panic) => failures.push(
                    panic
                        .downcast_ref::<String>()
                        .cloned()
                        .unwrap_or_else(|| format!("{case}: panicked")),
                ),
                Ok((case, first, again)) => {
                    if first != baseline {
                        failures.push(format!(
                            "{case}: differs from the uninterrupted recovery: {}",
                            digest_difference(&baseline, &first)
                        ));
                    }
                    if again != first {
                        failures.push(format!("{case}: a further reopen changed the state"));
                    }
                }
            }
            let _ = std::fs::remove_dir_all(&db);
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn interrupted_recovery_converges_after_an_unmarked_page_commit() {
    recovery_convergence("all-logs", "commit.after_page_apply:100", 1);
}

#[test]
fn interrupted_recovery_converges_after_a_replay_only_commit_with_eviction() {
    recovery_convergence("all-logs+small-pool", "commit.after_txlog_durable:150", 1);
}

#[test]
fn interrupted_recovery_converges_after_an_interrupted_checkpoint() {
    recovery_convergence("all-logs+segments", "checkpoint.after_page_flush:9", 1);
}

#[test]
fn interrupted_recovery_converges_in_transaction_log_only_mode() {
    recovery_convergence("transaction-log", "materialize.after_wal_sync:20", 1);
}

/// Diagnostic: `recovery_convergence` for `DURABILITY_CASE=<mode>/<point>`,
/// `DURABILITY_REPEAT=<rounds>` times.
#[test]
#[ignore]
fn repeat_recovery_convergence() {
    let spec = std::env::var("DURABILITY_CASE").expect("DURABILITY_CASE=<mode>/<point>");
    let rounds: usize = std::env::var("DURABILITY_REPEAT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let (mode, point) = spec.split_once('/').expect("<mode>/<point>");
    let mode: &'static str = Box::leak(mode.to_string().into_boxed_str());
    let point: &'static str = Box::leak(point.to_string().into_boxed_str());
    recovery_convergence(mode, point, rounds);
}

/// The experimental closure must recover the previously failing large
/// decided transaction at every boundary, without accepting an ignored case.
#[test]
fn experimental_structural_cuts_preserve_large_decided_commits() {
    gap1_a_decided_commit_larger_than_the_pool_keeps_acknowledged_rows();
}

// Fail-closed archiving (docs/durability-failure-handling.md, section 7.5):
// the child also feeds the commit-log and page-log archives
// (`DURABILITY_ARCHIVE`). A crash point or an injected EIO hits an archive
// step. Then, independently of the child:
// 1. the database reconciles against every acknowledged commit;
// 2. a point-in-time restore from each archive AS LEFT reconciles against
//    the commits that archive confirmed (no false "archived");
// 3. archiving resumes with new handles, and a restore from each archive
//    reconciles against every acknowledged commit (nothing acknowledged was
//    lost to an archive failure, and the archive chain has no gap).

/// Must match `ARCHIVE_KEY` in `examples/durability_child.rs`.
const ARCHIVE_KEY: &str = "durability-child-archive-passphrase-0123";

fn archive_config(mode: &str) -> DbConfig {
    config(mode).with_replication(bicdb_core::ReplicationConfig {
        enabled: true,
        mode: bicdb_core::ReplicationMode::Primary,
        cluster_id: "durability-child".to_string(),
        node_id: "child".to_string(),
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(bicdb_core::ReplicationTlsConfig {
            cert_path: Default::default(),
            key_path: Default::default(),
            ca_path: Default::default(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..bicdb_core::ReplicationConfig::default()
    })
}

/// The last per-writer floor an archive confirmed (zeros before any).
fn archived_floor(ledger: &Path, name: &str) -> Vec<u64> {
    ledger_lines(&ledger.join(name))
        .last()
        .map(|line| line.split(' ').map(|v| v.parse().unwrap()).collect())
        .unwrap_or_else(|| vec![0; WRITERS])
}

/// Restores both archives into `out` and reconciles each against `floors`.
fn restore_and_reconcile(
    archive: &Path,
    out: &Path,
    mode: &str,
    ledger: &Path,
    commit_floor: &[u64],
    wal_floor: &[u64],
    label: &str,
) -> Vec<BTreeMap<String, String>> {
    use bicdb_core::BackupRestoreOptions;
    let mut digests = Vec::new();
    let restored = out.join("from-commit-log");
    if !archive.join("commits/archive.json").exists() {
        // The archiver never confirmed a first append.
        assert!(commit_floor.iter().all(|seq| *seq == 0), "{label}");
    } else {
        bicdb_core::restore_point_in_time(
            &archive.join("commits"),
            &restored,
            ARCHIVE_KEY,
            true,
            bicdb_core::log_archive::ReplayTarget::default(),
        )
        .unwrap_or_else(|error| panic!("{label}: commit-log restore: {error}"));
        digests.push(reconcile_against(
            &restored,
            ledger,
            config(mode),
            commit_floor,
            &format!("{label} (commit-log restore)"),
        ));
    }
    let restored = out.join("from-page-log");
    for (artifact, force) in [("base.bicbackup", true), ("tail.bicbackup", false)] {
        bicdb_core::restore_backup(
            archive.join(artifact),
            &restored,
            BackupRestoreOptions {
                passphrase: ARCHIVE_KEY.to_string(),
                force,
            },
        )
        .unwrap_or_else(|error| panic!("{label}: page-log base restore: {error}"));
    }
    if archive.join("wal").is_dir() {
        bicdb_core::apply_archived_wal(&restored, archive.join("wal"))
            .unwrap_or_else(|error| panic!("{label}: page-log roll-forward: {error}"));
    }
    digests.push(reconcile_against(
        &restored,
        ledger,
        config(mode),
        wal_floor,
        &format!("{label} (page-log restore)"),
    ));
    digests
}

/// Each writer's newest commit in a reconciled state (its hot row).
fn hot_seqs(digest: &BTreeMap<String, String>) -> Vec<u64> {
    (0..WRITERS)
        .map(|writer| {
            digest
                .get(&format!("rows/hot-{writer}"))
                .map(|metadata| {
                    serde_json::from_str::<serde_json::Value>(metadata).unwrap()["seq"]
                        .as_u64()
                        .unwrap()
                })
                .unwrap_or(0)
        })
        .collect()
}

/// No restore may hold a commit the database itself does not: an archive
/// never makes an uncertain transaction look committed.
fn assert_within(primary: &[u64], restored: &[BTreeMap<String, String>], label: &str) {
    for digest in restored {
        let seqs = hot_seqs(digest);
        for writer in 0..WRITERS {
            assert!(
                seqs[writer] <= primary[writer],
                "{label}: a restore holds writer {writer} commit {} beyond the database's {}",
                seqs[writer],
                primary[writer]
            );
        }
    }
}

fn archive_case(mode: &str, crash_at: Option<&str>, fail_io: Option<&str>) {
    let label = format!(
        "{mode} archive {}",
        crash_at.or(fail_io).unwrap_or("no-fault")
    );
    let temp = tempfile::tempdir().unwrap();
    let keep = KeepOnPanic(temp.path().to_path_buf(), label.clone());
    let db = temp.path().join("db");
    let ledger = temp.path().join("ledger");
    let archive = temp.path().join("archive");
    let mut command = Command::new(child_binary());
    command
        .arg(&db)
        .arg(&ledger)
        .arg(durability(mode))
        .arg(COMMITS.to_string())
        .env("DURABILITY_ARCHIVE", &archive)
        .env("BICDB_TEST_WAL_VOLATILE_TAIL", "1")
        .env_remove("DURABILITY_POOL_BYTES")
        .env_remove("BICDB_TEST_CRASH_AT")
        .env_remove("BICDB_TEST_FAIL_IO");
    if let Some(point) = crash_at {
        command.env("BICDB_TEST_CRASH_AT", point);
    }
    if let Some(spec) = fail_io {
        command.env("BICDB_TEST_FAIL_IO", spec);
    }
    let output = command.output().expect("run durability child");
    let stderr = String::from_utf8_lossy(&output.stderr);
    if crash_at.is_some() {
        assert!(
            !output.status.success() && stderr.contains("bicdb test crash point reached"),
            "{label}: the crash point was never reached: {}\n{stderr}",
            output.status
        );
    } else {
        assert!(output.status.success(), "{label}: child failed\n{stderr}");
    }
    if fail_io.is_some() {
        assert_eq!(
            ledger_lines(&ledger.join("state")),
            vec!["healthy".to_string()],
            "{label}: an archive failure must not fence the database"
        );
    }
    if let Some(spec) = fail_io {
        let site = spec.split(':').next().unwrap();
        let state = ledger_lines(&ledger.join("archive-state"));
        assert!(
            state
                .iter()
                .any(|line| line.ends_with(&format!("failed {site}"))),
            "{label}: the fault was never reached or not reported: {state:?}"
        );
    }
    let violations = ledger_lines(&ledger.join("violations"));
    assert!(violations.is_empty(), "{label}: {violations:#?}");
    let acked: Vec<u64> = (0..WRITERS).map(|w| acknowledged(&ledger, w)).collect();
    assert!(
        acked.iter().any(|seq| *seq > 0),
        "{label}: nothing acknowledged"
    );

    // 1. The database itself.
    let primary = hot_seqs(&reconcile_against(
        &db,
        &ledger,
        archive_config(mode),
        &acked,
        &label,
    ));
    // 2. Each archive as the failure or crash left it.
    let as_left = restore_and_reconcile(
        &archive,
        &temp.path().join("as-left"),
        mode,
        &ledger,
        &archived_floor(&ledger, "archived-commits"),
        &archived_floor(&ledger, "archived-wal"),
        &label,
    );
    assert_within(&primary, &as_left, &label);
    // 3. Resume with new handles; then everything acknowledged is archived.
    {
        let db = BicDb::open_with_config(&db, archive_config(mode)).unwrap();
        let mut log = db
            .open_log_archive(&archive.join("commits"), true)
            .unwrap_or_else(|error| panic!("{label}: reopen commit archive: {error}"));
        let mut cursor = bicdb_core::ReplicationCursor::new(log.last_commit_seq());
        while db.archive_commits(&mut log, &mut cursor, 64).unwrap() > 0 {}
        db.archive_wal_segments(archive.join("wal"))
            .unwrap_or_else(|error| panic!("{label}: resume page-log archive: {error}"));
        drop(log);
        db.close().unwrap();
    }
    let resumed = restore_and_reconcile(
        &archive,
        &temp.path().join("resumed"),
        mode,
        &ledger,
        &acked,
        &acked,
        &format!("{label} (resumed)"),
    );
    assert_within(&primary, &resumed, &label);
    drop(keep);
}

const ARCHIVE_CRASH_POINTS: &[&str] = &[
    "log_archive.after_sync:3",
    "log_archive.after_sync:12",
    "wal_archive.after_rename:4",
    "wal_archive.before_mark:3",
];

const ARCHIVE_FAIL_IO_SPECS: &[&str] = &[
    "log_archive.write:8",
    "log_archive.sync:4",
    "wal_archive.copy:5",
    "wal_archive.sync:5",
    "wal_archive.rename:5",
    "wal_archive.dir_sync:3",
];

#[test]
fn archives_fail_closed_and_roll_forward_to_every_acknowledged_commit() {
    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> = vec![(
        "no-fault".to_string(),
        Box::new(|| archive_case("all-logs", None, None)),
    )];
    for &point in ARCHIVE_CRASH_POINTS {
        cases.push((
            format!("crash {point}"),
            Box::new(move || archive_case("all-logs", Some(point), None)),
        ));
    }
    for &spec in ARCHIVE_FAIL_IO_SPECS {
        cases.push((
            format!("fail-io {spec}"),
            Box::new(move || archive_case("all-logs", None, Some(spec))),
        ));
    }
    run_matrix(cases);
}
