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

fn config(mode: &str) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_transaction_log_commit_durability(mode == "transaction-log")
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
        .arg(mode)
        .arg(commits.to_string())
        .env("BICDB_TEST_WAL_VOLATILE_TAIL", "1")
        .env_remove("BICDB_TEST_CRASH_AT");
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
                .lookup_index("rows_unique_key", &[IndexValue::String(format!("u-{writer}-{i}"))])
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
        assert_eq!(via_index, via_scan, "{label}: owner index != scan for writer {writer}");
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
        assert!(side.len() <= 1, "{label}: {} holds {} rows", meta.name, side.len());
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
            let _ = Command::new("cp").arg("-a").arg(&self.0).arg(&target).status();
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
    assert!(matches!(run_child(&db, &ledger, mode, Some(crash_at)), Outcome::Crashed));
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
    assert!(failures.is_empty(), "{} failing cases:\n{}", failures.len(), failures.join("\n"));
}

fn matrix(mode: &'static str, points: Vec<&'static str>, replays: &[&'static str]) {
    let mut cases: Vec<(String, Box<dyn FnOnce() + std::panic::UnwindSafe>)> =
        vec![("no-crash".into(), Box::new(move || crash_case(mode, None, false)))];
    for point in points {
        cases.push((point.into(), Box::new(move || crash_case(mode, Some(point), true))));
    }
    for &point in replays {
        cases.push((
            format!("{point} + replay"),
            Box::new(move || interrupted_recovery_case(mode, point)),
        ));
    }
    run_matrix(cases);
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
        COMMON_POINTS.iter().chain(DEFERRED_POINTS).copied().collect(),
        &["commit.after_page_apply:100", "materialize.after_wal_sync:20"],
    );
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
