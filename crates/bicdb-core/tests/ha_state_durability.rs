//! Durable replacement of HA, consensus and catalog state files, and what
//! a failure at each step means (`docs/durability-failure-handling.md`,
//! sections 7 and 8).
//!
//! Every state file is written as: new temporary file, `fsync`, rename,
//! directory `fsync`. A failure up to and including the rename leaves the
//! old file and the old memory, and is returned. A failed directory `fsync`
//! after the rename leaves the outcome unknown, so it fences the database
//! (`WriteFailed`); the reopen runs the verification step and the state is
//! then whatever the disk holds. In no case is a vote, a term, an epoch or a fence
//! reported before it is durable, and in no case can two candidates get
//! this node's vote in one term.
//!
//! Faults are injected with `arm_io_failure`, scoped to each test's
//! directory, so these tests run in parallel. The crash points between the
//! steps, with power-loss emulation, are in `ha_state_crash.rs`.

use std::path::Path;

use bicdb_core::{
    finish_standby_bootstrap, stage_standby_bootstrap, standby_bootstrap_staging, BicDb,
    BicDbError, ConsensusConfig, ConsensusPeer, DbConfig, DurabilityState, NodeRole, Record,
    ReplicationConfig, ReplicationMode, ReplicationTlsConfig, RequestVote, StorageMode,
    DURABILITY_FAILURE_MARKER, HA_NODE_FILE,
};
use bicdb_page::test_faults::arm_io_failure;
use serde_json::json;

const STEPS: [&str; 4] = ["write", "sync", "rename", "dir_sync"];

/// Whether a failure at `step` happens after the rename landed (only the
/// directory `fsync`; a failed rename renamed nothing).
fn fatal(step: &str) -> bool {
    step == "dir_sync"
}

fn storage_modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

fn replication(mode: ReplicationMode) -> ReplicationConfig {
    ReplicationConfig {
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
    }
}

fn replicated(path: &Path, storage: StorageMode, mode: ReplicationMode) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(storage)
            .with_replication(replication(mode)),
    )
    .unwrap()
}

fn put(db: &BicDb, id: &str) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    tx.insert("items", Record::new(id).with_metadata(json!({ "id": id })))?;
    tx.commit()
}

fn failed_site(db: &BicDb) -> Option<String> {
    match db.durability_state() {
        DurabilityState::Healthy => None,
        DurabilityState::WriteFailed(failure) => Some(failure.site),
    }
}

fn no_temporaries(dir: &Path, label: &str) {
    let leftovers: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{label}: {leftovers:?}");
}

fn persisted_vote(dir: &Path) -> Option<(u64, String)> {
    let bytes = std::fs::read(dir.join(HA_NODE_FILE)).ok()?;
    let file: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    serde_json::from_value(file["voted"].clone()).unwrap()
}

/// A standby's vote: a failure at any step never grants it, a failure
/// before the rename changes nothing, a failure after it fences the node,
/// and after the reopen a second candidate in the same term is granted
/// exactly when the first vote did not reach the disk.
#[test]
fn a_vote_is_never_granted_ahead_of_the_disk_and_never_twice_per_term() {
    for storage in storage_modes() {
        for step in STEPS {
            let label = format!("{storage:?} ha_node.{step}");
            let dir = tempfile::tempdir().unwrap();
            let own;
            {
                let standby = replicated(dir.path(), storage.clone(), ReplicationMode::Standby);
                own = standby.last_applied_commit_seq();
                let fault = arm_io_failure(&format!("ha_node.{step}"), dir.path(), 1, false);
                let error = standby.consider_vote(5, "a", 0, own).unwrap_err();
                assert_eq!(fault.fired(), 1, "{label}");
                // Nothing in memory says this node voted.
                assert_eq!(standby.ha_node_status().voted, None, "{label}");
                no_temporaries(dir.path(), &label);
                if fatal(step) {
                    assert!(
                        matches!(&error, BicDbError::WriteFailed { site, .. } if site == &format!("ha_node.{step}")),
                        "{label}: {error}"
                    );
                    assert_eq!(
                        failed_site(&standby).as_deref(),
                        Some(&*format!("ha_node.{step}"))
                    );
                    assert!(
                        dir.path().join(DURABILITY_FAILURE_MARKER).exists(),
                        "{label}"
                    );
                    // A fenced node changes no HA state: no second vote.
                    let refused = standby.consider_vote(5, "b", 0, own).unwrap_err();
                    assert!(refused.is_durability_failure(), "{label}: {refused}");
                    assert!(standby.begin_candidacy("me").is_err(), "{label}");
                    assert!(standby.observe_election_term(9).is_err(), "{label}");
                } else {
                    assert!(!error.is_durability_failure(), "{label}: {error}");
                    assert!(standby.durability_state().is_healthy(), "{label}");
                    assert_eq!(persisted_vote(dir.path()), None, "{label}: old file");
                    // The refused grant was never given: another candidate
                    // may still win this term, once.
                    assert!(standby.consider_vote(5, "b", 0, own).unwrap(), "{label}");
                    assert!(!standby.consider_vote(5, "a", 0, own).unwrap(), "{label}");
                    continue;
                }
            }
            let on_disk = persisted_vote(dir.path());
            // The rename itself was refused; the directory sync failed after
            // a real rename.
            let expected = (step == "dir_sync").then(|| (5, "a".to_string()));
            assert_eq!(on_disk, expected, "{label}");
            let standby = replicated(dir.path(), storage.clone(), ReplicationMode::Standby);
            assert!(standby.durability_state().is_healthy(), "{label}");
            assert!(
                !dir.path().join(DURABILITY_FAILURE_MARKER).exists(),
                "{label}"
            );
            let granted_b = standby.consider_vote(5, "b", 0, own).unwrap();
            assert_eq!(
                granted_b,
                on_disk.is_none(),
                "{label}: a second vote in term 5 must follow only from a vote that never reached the disk"
            );
            assert_eq!(standby.replication_role(), NodeRole::Standby, "{label}");
        }
    }
}

/// A primary that learns of a newer epoch stops acknowledging writes at
/// once, whatever happens to the file that records its fence; once the
/// fence is reported, it survives the reopen.
#[test]
fn a_primary_whose_fence_cannot_be_persisted_still_stops_writing() {
    for storage in storage_modes() {
        for step in STEPS {
            let label = format!("{storage:?} ha_node.{step}");
            let dir = tempfile::tempdir().unwrap();
            {
                let mut primary = replicated(dir.path(), storage.clone(), ReplicationMode::Primary);
                primary.create_collection("items").unwrap();
                put(&primary, "acked").unwrap();
                let fault = arm_io_failure(&format!("ha_node.{step}"), dir.path(), 1, false);
                assert!(primary.observe_replication_epoch(3).is_err(), "{label}");
                assert_eq!(fault.fired(), 1, "{label}");
                let refused = put(&primary, "after").unwrap_err();
                assert!(
                    !matches!(refused, BicDbError::Io(_)),
                    "{label}: a superseded primary must refuse writes: {refused}"
                );
                assert_eq!(
                    failed_site(&primary).is_some(),
                    fatal(step),
                    "{label}: {:?}",
                    primary.durability_state()
                );
                if !fatal(step) {
                    // Retried, the fence is durable and reported.
                    assert!(primary.observe_replication_epoch(3).unwrap(), "{label}");
                    assert_eq!(primary.replication_role(), NodeRole::Fenced, "{label}");
                }
            }
            let reopened = replicated(dir.path(), storage.clone(), ReplicationMode::Primary);
            assert!(reopened.get("items", "acked").unwrap().is_some(), "{label}");
            assert!(reopened.get("items", "after").unwrap().is_none(), "{label}");
            if !fatal(step) || step == "dir_sync" {
                assert_eq!(reopened.replication_role(), NodeRole::Fenced, "{label}");
                assert_eq!(reopened.replication_epoch(), 3, "{label}");
                assert!(put(&reopened, "post-reopen").is_err(), "{label}");
            }
        }
    }
}

/// A drained primary's offer and an elected primary's role are durable
/// before they are reported.
#[test]
fn switchover_drain_and_election_promotion_fail_closed() {
    for step in STEPS {
        let label = format!("ha_node.{step}");
        let dir = tempfile::tempdir().unwrap();
        {
            let mut primary = replicated(
                dir.path(),
                StorageMode::ServerPaged,
                ReplicationMode::Primary,
            );
            primary.create_collection("items").unwrap();
            put(&primary, "acked").unwrap();
            let fault = arm_io_failure(&format!("ha_node.{step}"), dir.path(), 1, false);
            assert!(primary.drain_for_switchover("target").is_err(), "{label}");
            assert_eq!(fault.fired(), 1, "{label}");
            assert_eq!(primary.planned_switchover(), None, "{label}");
            if !fatal(step) {
                let offer = primary.drain_for_switchover("target").unwrap();
                assert_eq!(primary.planned_switchover(), Some(offer), "{label}");
                assert!(put(&primary, "after-drain").is_err(), "{label}");
            } else {
                assert!(primary.drain_for_switchover("target").is_err(), "{label}");
            }
        }
        let reopened = replicated(
            dir.path(),
            StorageMode::ServerPaged,
            ReplicationMode::Primary,
        );
        assert!(reopened.get("items", "acked").unwrap().is_some(), "{label}");
        assert!(
            reopened.get("items", "after-drain").unwrap().is_none(),
            "{label}"
        );
        if !fatal(step) {
            assert!(reopened.planned_switchover().is_some(), "{label}");
            assert!(
                put(&reopened, "x").is_err(),
                "{label}: drained after reopen"
            );
        }

        let dir = tempfile::tempdir().unwrap();
        {
            let standby = replicated(
                dir.path(),
                StorageMode::ServerPaged,
                ReplicationMode::Standby,
            );
            let term = standby.begin_candidacy("me").unwrap();
            let fault = arm_io_failure(&format!("ha_node.{step}"), dir.path(), 1, false);
            assert!(
                standby.promote_by_election(term, "me", None).is_err(),
                "{label}"
            );
            assert_eq!(fault.fired(), 1, "{label}");
            assert_eq!(standby.replication_role(), NodeRole::Standby, "{label}");
        }
        let reopened = replicated(
            dir.path(),
            StorageMode::ServerPaged,
            ReplicationMode::Standby,
        );
        let role = reopened.replication_role();
        if step == "dir_sync" {
            assert_eq!(role, NodeRole::Primary, "{label}: the rename landed");
        } else {
            assert_eq!(role, NodeRole::Standby, "{label}");
        }
    }
}

fn consensus_config(node: &str) -> ConsensusConfig {
    ConsensusConfig {
        enabled: true,
        cluster_id: "test".to_string(),
        node_id: node.to_string(),
        peers: vec![
            ConsensusPeer::voting("a", "127.0.0.1:1"),
            ConsensusPeer::voting("b", "127.0.0.1:2"),
            ConsensusPeer::voting("c", "127.0.0.1:3"),
        ],
        ..ConsensusConfig::default()
    }
}

fn consensus_db(dir: &Path) -> BicDb {
    BicDb::open_with_config(
        dir,
        DbConfig::default()
            .with_fsync(true)
            .with_consensus(consensus_config("a")),
    )
    .unwrap()
}

fn vote_request(term: u64, candidate: &str) -> RequestVote {
    RequestVote {
        cluster_id: "test".to_string(),
        term,
        candidate_id: candidate.to_string(),
        last_log_index: 0,
        last_log_term: 0,
    }
}

fn persisted_consensus(dir: &Path) -> (u64, Option<String>) {
    let bytes = std::fs::read(dir.join("consensus-state.json")).unwrap();
    let state: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (
        state["current_term"].as_u64().unwrap(),
        state["voted_for"].as_str().map(str::to_string),
    )
}

/// The consensus term and vote: memory goes back to the state before the
/// change on any failure, so a vote is never granted from memory that the
/// disk does not hold, and the reopen never grants a second candidate in a
/// term whose vote reached the disk.
#[test]
fn a_consensus_vote_is_durable_before_it_is_granted() {
    for step in STEPS {
        let label = format!("consensus_state.{step}");
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = consensus_db(dir.path());
            // Seed the file (term 0, no vote; `z` is not a voter) so there
            // is an old version to keep.
            assert!(
                !db.consensus_handle_request_vote(vote_request(0, "z"))
                    .unwrap()
                    .vote_granted
            );
            let fault = arm_io_failure(&format!("consensus_state.{step}"), dir.path(), 1, false);
            let error = db
                .consensus_handle_request_vote(vote_request(1, "b"))
                .unwrap_err();
            assert_eq!(fault.fired(), 1, "{label}");
            let status = db.consensus_status();
            assert_eq!(
                (status.current_term, status.voted_for),
                (0, None),
                "{label}: memory rolled back"
            );
            no_temporaries(dir.path(), &label);
            if fatal(step) {
                assert!(
                    matches!(&error, BicDbError::WriteFailed { site, .. } if site == &format!("consensus_state.{step}")),
                    "{label}: {error}"
                );
                assert!(db
                    .consensus_handle_request_vote(vote_request(1, "c"))
                    .unwrap_err()
                    .is_durability_failure());
                assert!(db.consensus_start_election().is_err(), "{label}");
            } else {
                assert!(db.durability_state().is_healthy(), "{label}");
                assert_eq!(persisted_consensus(dir.path()), (0, None), "{label}");
                assert!(
                    db.consensus_handle_request_vote(vote_request(1, "c"))
                        .unwrap()
                        .vote_granted
                );
                assert!(
                    !db.consensus_handle_request_vote(vote_request(1, "b"))
                        .unwrap()
                        .vote_granted
                );
                continue;
            }
        }
        let on_disk = persisted_consensus(dir.path());
        let expected = if step == "dir_sync" {
            (1, Some("b".to_string()))
        } else {
            (0, None)
        };
        assert_eq!(on_disk, expected, "{label}");
        let mut db = consensus_db(dir.path());
        assert!(db.durability_state().is_healthy(), "{label}");
        let granted_c = db
            .consensus_handle_request_vote(vote_request(1, "c"))
            .unwrap()
            .vote_granted;
        assert_eq!(
            granted_c,
            on_disk.1.is_none(),
            "{label}: two votes in term 1"
        );
    }
}

/// The file-shipping HA state: a promotion is durable before it is
/// reported, and an apply that did not finish leaves a standby that refuses
/// promotion without `force`.
#[test]
fn file_shipping_promotion_and_apply_fail_closed() {
    for step in STEPS {
        let label = format!("ha_state.{step}");
        let root = tempfile::tempdir().unwrap();
        let primary_dir = root.path().join("primary");
        let standby_dir = root.path().join("standby");
        {
            let mut primary = BicDb::open(&primary_dir).unwrap();
            primary.create_collection("items").unwrap();
            put(&primary, "shipped").unwrap();
        }
        BicDb::configure_standby_from(&standby_dir, &primary_dir).unwrap();
        let fault = arm_io_failure(&format!("ha_state.{step}"), &standby_dir, 1, false);
        assert!(
            BicDb::promote_standby(&standby_dir, false).is_err(),
            "{label}"
        );
        assert_eq!(fault.fired(), 1, "{label}");
        drop(fault);
        no_temporaries(&standby_dir, &label);
        let state: serde_json::Value =
            serde_json::from_slice(&std::fs::read(standby_dir.join("ha-state.json")).unwrap())
                .unwrap();
        let expected = if step == "dir_sync" {
            "primary"
        } else {
            "standby"
        };
        assert_eq!(state["role"], expected, "{label}");
        if step != "dir_sync" {
            let status = BicDb::promote_standby(&standby_dir, false).unwrap();
            assert!(!status.read_only, "{label}");
        }

        // The apply's final state write fails: the in-progress record that
        // preceded the copy keeps the standby from being promoted.
        let standby_dir = root.path().join("standby-2");
        BicDb::configure_standby_from(&standby_dir, &primary_dir).unwrap();
        // First write: the in-progress record; second: the final state.
        let fault = arm_io_failure(&format!("ha_state.{step}"), &standby_dir, 2, false);
        assert!(
            BicDb::configure_standby_from(&standby_dir, &primary_dir).is_err(),
            "{label}"
        );
        assert_eq!(fault.fired(), 1, "{label}");
        drop(fault);
        if step != "dir_sync" {
            let refused = BicDb::promote_standby(&standby_dir, false).unwrap_err();
            assert!(refused.to_string().contains("force"), "{label}: {refused}");
        }
        // A forced promotion after manual verification still works.
        BicDb::promote_standby(&standby_dir, true).unwrap();
    }
}

/// The install of a staged standby bootstrap: a failed rename leaves the
/// old database whole and the staging directory ready; a failed directory
/// sync fails the open and the next open completes the install.
#[test]
fn a_bootstrap_install_completes_only_after_its_directory_sync() {
    for step in ["rename", "dir_sync"] {
        let label = format!("bootstrap.{step}");
        let root = tempfile::tempdir().unwrap();
        let primary_dir = root.path().join("primary");
        let standby_dir = root.path().join("standby");
        let mut primary = replicated(
            &primary_dir,
            StorageMode::EmbeddedMemory,
            ReplicationMode::Primary,
        );
        primary.create_collection("items").unwrap();
        put(&primary, "from-primary").unwrap();
        {
            let mut old = replicated(
                &standby_dir,
                StorageMode::EmbeddedMemory,
                ReplicationMode::Primary,
            );
            old.create_collection("items").unwrap();
            put(&old, "old-only").unwrap();
        }
        let archive = root.path().join("snapshot.bak");
        let key = "0123456789abcdef0123456789abcdef";
        primary.create_standby_snapshot(&archive, key).unwrap();
        stage_standby_bootstrap(&standby_dir, &archive, key, true).unwrap();

        let fault = arm_io_failure(&format!("bootstrap.{step}"), &standby_dir, 1, false);
        let error = finish_standby_bootstrap(&standby_dir).unwrap_err();
        assert!(
            matches!(&error, BicDbError::WriteFailed { site, .. } if site == &label),
            "{label}: {error}"
        );
        assert_eq!(fault.fired(), 1, "{label}");
        drop(fault);
        if step == "rename" {
            assert!(standby_bootstrap_staging(&standby_dir).exists(), "{label}");
            let old = replicated(
                &standby_dir,
                StorageMode::EmbeddedMemory,
                ReplicationMode::Standby,
            );
            // The open installed it on the retry.
            assert!(
                old.get("items", "from-primary").unwrap().is_some(),
                "{label}"
            );
            assert!(old.get("items", "old-only").unwrap().is_none(), "{label}");
            continue;
        }
        // Renamed into place, marker kept until a sync succeeds.
        assert!(!standby_bootstrap_staging(&standby_dir).exists(), "{label}");
        assert!(standby_dir.join("BOOTSTRAP_READY").exists(), "{label}");
        assert!(
            finish_standby_bootstrap(&standby_dir).unwrap(),
            "{label}: completed"
        );
        assert!(!standby_dir.join("BOOTSTRAP_READY").exists(), "{label}");
        assert!(!root.path().join("standby.replaced").exists(), "{label}");
        let standby = replicated(
            &standby_dir,
            StorageMode::EmbeddedMemory,
            ReplicationMode::Standby,
        );
        assert!(
            standby.get("items", "from-primary").unwrap().is_some(),
            "{label}"
        );
        assert_eq!(standby.replication_role(), NodeRole::Standby, "{label}");
    }
}

/// The collection catalog: memory changes before the file, so a failure at
/// any step fences. Before, a failed CREATE retried on the same handle found
/// the collection in memory and succeeded without persisting it, and rows
/// acknowledged into it could vanish at the next open.
#[test]
fn a_catalog_write_failure_fences_and_acknowledged_rows_survive() {
    for storage in storage_modes() {
        for step in STEPS {
            let label = format!("{storage:?} catalog.{step}");
            let dir = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(storage.clone());
            {
                let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
                db.create_collection("items").unwrap();
                put(&db, "acked").unwrap();
                let fault = arm_io_failure(&format!("catalog.{step}"), dir.path(), 1, false);
                let error = db.create_collection("other").unwrap_err();
                assert_eq!(fault.fired(), 1, "{label}");
                assert!(
                    matches!(&error, BicDbError::WriteFailed { site, .. } if site == &format!("catalog.{step}")),
                    "{label}: {error}"
                );
                // The retry that used to succeed without persisting.
                assert!(db.create_collection("other").is_err(), "{label}: retry");
                assert!(put(&db, "refused").is_err(), "{label}");
                no_temporaries(dir.path(), &label);
            }
            let db = BicDb::open_with_config(dir.path(), config).unwrap();
            assert!(db.durability_state().is_healthy(), "{label}");
            assert!(db.get("items", "acked").unwrap().is_some(), "{label}");
            put(&db, "after-reopen").unwrap();
        }
    }
}

/// Planner statistics are deliberately not fatal: they only steer plans,
/// and ANALYZE rebuilds them.
#[test]
fn a_planner_statistics_failure_is_not_fatal() {
    for step in STEPS {
        let dir = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(true)).unwrap();
        db.create_collection("items").unwrap();
        put(&db, "a").unwrap();
        let fault = arm_io_failure(&format!("planner_stats.{step}"), dir.path(), 1, false);
        assert!(db.analyze().is_err(), "planner_stats.{step}");
        assert_eq!(fault.fired(), 1, "planner_stats.{step}");
        assert!(db.durability_state().is_healthy(), "planner_stats.{step}");
        drop(fault);
        db.analyze().unwrap();
        put(&db, "b").unwrap();
    }
}
