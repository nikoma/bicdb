//! Crash child for HA, consensus and catalog state files (see
//! `tests/ha_state_crash.rs` and `docs/durability-failure-handling.md`).
//!
//! Usage: `ha_state_child <db-dir> <ledger-dir> <op> <setup|run>`.
//!
//! `setup` creates the database for `<op>` and exits normally. `run`
//! appends `start` to the ledger, performs the operation, and appends one
//! fsynced ledger line for each result it was given (an acknowledgment),
//! followed by the crash point `child.after_ack`. The parent sets
//! `BICDB_TEST_CRASH_AT` (one of the state file's step crash points or
//! `child.after_ack`) and `BICDB_TEST_DIR_VOLATILE=1`, which undoes every
//! rename no directory `fsync` covered at the crash, as a power loss may.
//!
//! Operations:
//! - `vote`: a standby grants its vote in term 5 to `a` (`granted`).
//! - `consensus-vote`: a consensus node grants term 1 to `b` (`granted`).
//! - `fence`: a primary commits rows (`row <id>`) and then learns of epoch
//!   3 (`fenced`).
//! - `drain`: a primary commits rows and drains for a switchover
//!   (`drained`).
//! - `promote`: a standby that began a candidacy in setup is promoted
//!   (`promoted <term>`).
//! - `catalog`: CREATE COLLECTION `other` (`created`), then a row in it
//!   (`row other/x`).
//! - `ship`: file-shipping HA. Setup ships `<db-dir>/primary` to
//!   `<db-dir>/standby` once; run commits a row on the primary and ships
//!   again (`shipped`). The parent crashes it in the middle of the copy.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use bicdb_core::{
    BicDb, ConsensusConfig, ConsensusPeer, DbConfig, Record, ReplicationConfig, ReplicationMode,
    ReplicationTlsConfig, RequestVote, StorageMode,
};
use serde_json::json;

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

fn acknowledged(ledger: &Path, line: &str) {
    append_line(ledger, line);
    bicdb_page::test_faults::crash_point("child.after_ack");
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
    .expect("open replicated db")
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
    .expect("open consensus db")
}

fn plain(path: &Path) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .expect("open db")
}

fn put(db: &BicDb, collection: &str, id: &str) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    tx.insert(
        collection,
        Record::new(id).with_metadata(json!({ "id": id })),
    )?;
    tx.commit()
}

fn commit_rows(db: &BicDb, ledger: &Path, from: u32, to: u32) {
    for i in from..=to {
        let id = format!("r{i}");
        put(db, "items", &id).expect("commit");
        append_line(ledger, &format!("row {id}"));
    }
}

pub fn vote_request(term: u64, candidate: &str) -> RequestVote {
    RequestVote {
        cluster_id: "test".to_string(),
        term,
        candidate_id: candidate.to_string(),
        last_log_index: 0,
        last_log_term: 0,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let db_dir = PathBuf::from(args.next().expect("db dir"));
    let ledger_dir = PathBuf::from(args.next().expect("ledger dir"));
    let op = args.next().expect("op");
    let phase = args.next().expect("phase");
    std::fs::create_dir_all(&ledger_dir).expect("ledger dir");
    let ledger = ledger_dir.join("ledger");
    let setup = phase == "setup";
    match op.as_str() {
        "vote" => {
            let db = replicated(&db_dir, ReplicationMode::Standby);
            if setup {
                return;
            }
            let own = db.last_applied_commit_seq();
            append_line(&ledger, "start");
            if db.consider_vote(5, "a", 0, own).expect("vote") {
                acknowledged(&ledger, "granted");
            }
        }
        "consensus-vote" => {
            let mut db = consensus(&db_dir);
            if setup {
                // `z` is not a voter: persists term 0 with no vote.
                db.consensus_handle_request_vote(vote_request(0, "z"))
                    .expect("seed");
                return;
            }
            append_line(&ledger, "start");
            if db
                .consensus_handle_request_vote(vote_request(1, "b"))
                .expect("vote")
                .vote_granted
            {
                acknowledged(&ledger, "granted");
            }
        }
        "fence" | "drain" => {
            let mut db = replicated(&db_dir, ReplicationMode::Primary);
            if setup {
                db.create_collection("items").expect("create");
                commit_rows(&db, &ledger, 1, 5);
                return;
            }
            commit_rows(&db, &ledger, 6, 8);
            append_line(&ledger, "start");
            if op == "fence" {
                if db.observe_replication_epoch(3).expect("observe") {
                    acknowledged(&ledger, "fenced");
                }
            } else {
                db.drain_for_switchover("target").expect("drain");
                acknowledged(&ledger, "drained");
            }
        }
        "promote" => {
            let db = replicated(&db_dir, ReplicationMode::Standby);
            if setup {
                db.begin_candidacy("me").expect("candidacy");
                return;
            }
            let term = db.replication_election_term();
            append_line(&ledger, "start");
            db.promote_by_election(term, "me", None).expect("promote");
            acknowledged(&ledger, &format!("promoted {term}"));
        }
        "catalog" => {
            let mut db = plain(&db_dir);
            if setup {
                db.create_collection("items").expect("create");
                commit_rows(&db, &ledger, 1, 5);
                return;
            }
            append_line(&ledger, "start");
            db.create_collection("other").expect("create other");
            acknowledged(&ledger, "created");
            put(&db, "other", "x").expect("row in other");
            acknowledged(&ledger, "row other/x");
        }
        "ship" => {
            let primary_dir = db_dir.join("primary");
            let standby_dir = db_dir.join("standby");
            if setup {
                let mut primary = BicDb::open(&primary_dir).expect("open primary");
                primary.create_collection("items").expect("create");
                put(&primary, "items", "first").expect("commit");
                drop(primary);
                BicDb::configure_standby_from(&standby_dir, &primary_dir).expect("ship");
                return;
            }
            {
                let primary = BicDb::open(&primary_dir).expect("open primary");
                put(&primary, "items", "second").expect("commit");
            }
            append_line(&ledger, "start");
            BicDb::configure_standby_from(&standby_dir, &primary_dir).expect("ship");
            acknowledged(&ledger, "shipped");
        }
        other => panic!("unknown op {other}"),
    }
}
