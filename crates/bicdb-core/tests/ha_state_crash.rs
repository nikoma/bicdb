//! Crash points between the steps of every HA, consensus and catalog state
//! file replacement, with power-loss emulation of unsynced directory
//! entries, and recovery evidence after the reopen.
//!
//! Each case runs `examples/ha_state_child` twice: `setup`, then `run` with
//! `BICDB_TEST_CRASH_AT` set to one crash point and
//! `BICDB_TEST_DIR_VOLATILE=1`. At the abort, every rename that no
//! directory `fsync` covered is undone, which is what a power loss may do.
//! The parent then reopens the database and checks, against the child's
//! ledger of acknowledged results:
//!
//! - a vote reported granted is still held, and a second candidate in the
//!   same term is refused (never two votes in one term);
//! - a fence reported is still in force (no unfenced stale primary), a
//!   drain reported still refuses writes, a promotion reported holds;
//! - a collection reported created exists, with its acknowledged row;
//! - every acknowledged commit is present;
//! - a crash after the rename and before the directory `fsync` loses the
//!   change (the emulation works, and nothing was acknowledged), and a
//!   crash after the directory `fsync` keeps it.
//!
//! The child must be built first: `cargo build -p bicdb-core --example
//! ha_state_child` (a plain `cargo test` builds examples).

use std::path::{Path, PathBuf};
use std::process::Command;

use bicdb_core::{
    BicDb, ConsensusConfig, ConsensusPeer, DbConfig, NodeRole, ReplicationConfig, ReplicationMode,
    ReplicationTlsConfig, RequestVote, StorageMode, DURABILITY_FAILURE_MARKER,
};

fn child_binary() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    let examples = path.join("examples");
    let exact = examples.join("ha_state_child");
    if exact.exists() {
        return exact;
    }
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(&examples).into_iter().flatten().flatten() {
        let name = entry.file_name();
        let name = name.to_str().unwrap_or_default();
        if !name.starts_with("ha_state_child-") || name.ends_with(".d") {
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
        .expect("build the child first: cargo build -p bicdb-core --example ha_state_child")
}

fn run_child(db: &Path, ledger: &Path, op: &str, phase: &str, crash_at: Option<&str>) -> bool {
    let mut command = Command::new(child_binary());
    command
        .arg(db)
        .arg(ledger)
        .arg(op)
        .arg(phase)
        .env_remove("BICDB_TEST_CRASH_AT")
        .env_remove("BICDB_TEST_FAIL_IO")
        .env_remove("BICDB_TEST_WAL_VOLATILE_TAIL");
    match crash_at {
        Some(point) => {
            command
                .env("BICDB_TEST_CRASH_AT", point)
                .env("BICDB_TEST_DIR_VOLATILE", "1");
        }
        None => {
            command.env_remove("BICDB_TEST_DIR_VOLATILE");
        }
    }
    let output = command.output().expect("run ha_state_child");
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success() {
        return false;
    }
    assert!(
        stderr.contains("bicdb test crash point reached"),
        "{op} {phase} {crash_at:?}: child failed without reaching its crash point: {}\n{stderr}",
        output.status
    );
    true
}

fn ledger(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("ledger"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn replicated(path: &Path, mode: ReplicationMode) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_replication(ReplicationConfig {
                enabled: true,
                mode,
                listen_addr: Some("127.0.0.1:0".to_string()),
                tls: Some(ReplicationTlsConfig {
                    cert_path: Default::default(),
                    key_path: Default::default(),
                    ca_path: Default::default(),
                    require_client_cert: true,
                    dev_localhost_plaintext: true,
                }),
                ..ReplicationConfig::default()
            }),
    )
    .expect("reopen replicated db")
}

fn consensus(path: &Path) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(true)
            .with_consensus(ConsensusConfig {
                enabled: true,
                cluster_id: "test".to_string(),
                node_id: "a".to_string(),
                peers: vec![
                    ConsensusPeer::voting("a", "127.0.0.1:1"),
                    ConsensusPeer::voting("b", "127.0.0.1:2"),
                    ConsensusPeer::voting("c", "127.0.0.1:3"),
                ],
                ..ConsensusConfig::default()
            }),
    )
    .expect("reopen consensus db")
}

fn rows_present(db: &BicDb, lines: &[String], label: &str) {
    for line in lines {
        if let Some(id) = line.strip_prefix("row r") {
            let id = format!("r{id}");
            assert!(
                db.get("items", &id).unwrap().is_some(),
                "{label}: acknowledged commit {id} lost"
            );
        }
    }
}

fn write_refused(db: &BicDb) -> bool {
    db.begin_transaction()
        .and_then(|mut tx| {
            tx.insert("items", bicdb_core::Record::new("probe"))?;
            tx.commit()
        })
        .is_err()
}

/// Which crash points a case uses, and whether the state change is
/// expected to survive each one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    /// Crashed before the rename or before the directory sync: the change
    /// was never acknowledged and (with the emulation) did not survive.
    Lost,
    /// Crashed after the directory sync: the change survives.
    Kept,
}

fn points(site: &str) -> Vec<(String, Expect)> {
    vec![
        (format!("{site}.after_write:1"), Expect::Lost),
        (format!("{site}.after_sync:1"), Expect::Lost),
        (format!("{site}.after_rename:1"), Expect::Lost),
        (format!("{site}.after_dir_sync:1"), Expect::Kept),
        ("child.after_ack:1".to_string(), Expect::Kept),
    ]
}

fn case(op: &str, point: &str, expect: Expect) {
    let label = format!("{op} @ {point}");
    let temp = tempfile::tempdir().unwrap();
    let db_dir = temp.path().join("db");
    let ledger_dir = temp.path().join("ledger");
    assert!(
        !run_child(&db_dir, &ledger_dir, op, "setup", None),
        "{label}: setup crashed"
    );
    assert!(
        run_child(&db_dir, &ledger_dir, op, "run", Some(point)),
        "{label}: the crash point was never reached"
    );
    let lines = ledger(&ledger_dir);
    assert!(
        lines.iter().any(|line| line == "start"),
        "{label}: crashed before the operation started: {lines:?}"
    );
    let acked = |prefix: &str| lines.iter().any(|line| line.starts_with(prefix));
    assert!(
        !temp
            .path()
            .join("db")
            .join(DURABILITY_FAILURE_MARKER)
            .exists(),
        "{label}: a crash is not a write failure"
    );
    match op {
        "vote" => {
            let standby = replicated(&db_dir, ReplicationMode::Standby);
            let own = standby.last_applied_commit_seq();
            let held = standby.ha_node_status().voted == Some((5, "a".to_string()));
            assert_eq!(held, expect == Expect::Kept, "{label}: vote on disk");
            if acked("granted") {
                assert!(held, "{label}: an acknowledged vote was lost");
            }
            let second = standby.consider_vote(5, "b", 0, own).unwrap();
            assert!(
                !(acked("granted") && second),
                "{label}: two votes granted in term 5"
            );
            assert_eq!(second, !held, "{label}");
        }
        "consensus-vote" => {
            let mut db = consensus(&db_dir);
            let status = db.consensus_status();
            let held = status.current_term == 1 && status.voted_for.as_deref() == Some("b");
            assert_eq!(
                held,
                expect == Expect::Kept,
                "{label}: vote on disk ({status:?})"
            );
            if acked("granted") {
                assert!(held, "{label}: an acknowledged vote was lost");
            }
            let second = db
                .consensus_handle_request_vote(RequestVote {
                    cluster_id: "test".to_string(),
                    term: 1,
                    candidate_id: "c".to_string(),
                    last_log_index: 0,
                    last_log_term: 0,
                })
                .unwrap()
                .vote_granted;
            assert!(
                !(acked("granted") && second),
                "{label}: two votes granted in term 1"
            );
            assert_eq!(second, !held, "{label}");
        }
        "fence" => {
            let primary = replicated(&db_dir, ReplicationMode::Primary);
            rows_present(&primary, &lines, &label);
            let fenced = primary.replication_role() == NodeRole::Fenced;
            assert_eq!(fenced, expect == Expect::Kept, "{label}: fence on disk");
            if acked("fenced") {
                assert!(
                    fenced,
                    "{label}: an acknowledged fence was lost (stale primary)"
                );
                assert!(primary.replication_epoch() >= 3, "{label}");
                assert!(write_refused(&primary), "{label}: a fenced primary wrote");
            }
        }
        "drain" => {
            let primary = replicated(&db_dir, ReplicationMode::Primary);
            rows_present(&primary, &lines, &label);
            let drained = primary.planned_switchover().is_some();
            assert_eq!(drained, expect == Expect::Kept, "{label}: drain on disk");
            if acked("drained") {
                assert!(drained, "{label}: an acknowledged drain was lost");
                assert!(write_refused(&primary), "{label}: a drained primary wrote");
            }
        }
        "promote" => {
            let node = replicated(&db_dir, ReplicationMode::Standby);
            let primary = node.replication_role() == NodeRole::Primary;
            assert_eq!(
                primary,
                expect == Expect::Kept,
                "{label}: promotion on disk"
            );
            if let Some(line) = lines.iter().find(|line| line.starts_with("promoted ")) {
                let term: u64 = line["promoted ".len()..].parse().unwrap();
                assert!(primary, "{label}: an acknowledged promotion was lost");
                assert_eq!(node.replication_epoch(), term, "{label}");
            }
        }
        "catalog" => {
            let db = BicDb::open_with_config(
                &db_dir,
                DbConfig::default()
                    .with_fsync(true)
                    .with_storage_mode(StorageMode::ServerPaged),
            )
            .unwrap();
            rows_present(&db, &lines, &label);
            let exists = db.collections().iter().any(|meta| meta.name == "other");
            if acked("created") {
                assert!(
                    exists,
                    "{label}: an acknowledged CREATE COLLECTION was lost"
                );
            }
            if acked("row other/x") {
                assert!(
                    db.get("other", "x").unwrap().is_some(),
                    "{label}: an acknowledged row in the new collection was lost"
                );
            }
            if expect == Expect::Kept {
                assert!(exists, "{label}");
            }
        }
        "ship" => {
            let standby = db_dir.join("standby");
            let promoted = BicDb::promote_standby(&standby, false);
            if acked("shipped") {
                let status = promoted.expect("a completed apply promotes");
                assert!(!status.read_only, "{label}");
            } else {
                // A standby whose files are a mix of two checkpoints must
                // not be promoted without `force`.
                assert!(
                    promoted.is_err(),
                    "{label}: an interrupted apply was promoted without force"
                );
            }
        }
        other => panic!("unknown op {other}"),
    }
}

/// A file-shipping apply crashed in the middle of its copy refuses
/// promotion without `force`; one that finished promotes.
#[test]
fn an_interrupted_ship_is_never_promoted_without_force() {
    let mut failures = Vec::new();
    for (point, expect) in [
        ("ha_ship.after_file:1", Expect::Lost),
        ("ha_ship.after_file:3", Expect::Lost),
        ("ha_state.after_dir_sync:1", Expect::Lost),
        ("child.after_ack:1", Expect::Kept),
    ] {
        if let Err(panic) = std::panic::catch_unwind(move || case("ship", point, expect)) {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            failures.push(format!("ship @ {point}: {message}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn matrix(op: &str, site: &str) {
    let mut failures = Vec::new();
    let mut cases = points(site);
    if op == "catalog" {
        // After the row in the new collection was acknowledged too.
        cases.push(("child.after_ack:2".to_string(), Expect::Kept));
    }
    for (point, expect) in cases {
        let op = op.to_string();
        let label = format!("{op} @ {point}");
        if let Err(panic) = std::panic::catch_unwind(move || case(&op, &point, expect)) {
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

#[test]
fn a_vote_survives_every_crash_point_and_is_never_given_twice() {
    matrix("vote", "ha_node");
}

#[test]
fn a_consensus_vote_survives_every_crash_point_and_is_never_given_twice() {
    matrix("consensus-vote", "consensus_state");
}

#[test]
fn a_fence_survives_every_crash_point() {
    matrix("fence", "ha_node");
}

#[test]
fn a_switchover_drain_survives_every_crash_point() {
    matrix("drain", "ha_node");
}

#[test]
fn an_election_promotion_survives_every_crash_point() {
    matrix("promote", "ha_node");
}

#[test]
fn a_created_collection_survives_every_crash_point() {
    matrix("catalog", "catalog");
}
