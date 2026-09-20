//! Only the consensus leader may accept writes.
//!
//! A follower used to accept writes over its own SQL port and keep them: local
//! commits are proposed to the consensus log by the leader's loop only, so a
//! follower's writes replicated nowhere and that node diverged permanently.
//! Two clients on two nodes could each be told `INSERT 0 1` for different rows
//! and never converge.

use bicdb_core::{BicDb, ConsensusConfig, ConsensusPeer, DbConfig, Record};

fn consensus_db(dir: &std::path::Path, node: &str) -> BicDb {
    let config = ConsensusConfig {
        enabled: true,
        cluster_id: "test".to_string(),
        node_id: node.to_string(),
        peers: vec![
            ConsensusPeer::voting("a", "127.0.0.1:1"),
            ConsensusPeer::voting("b", "127.0.0.1:2"),
            ConsensusPeer::voting("c", "127.0.0.1:3"),
        ],
        ..ConsensusConfig::default()
    };
    BicDb::open_with_config(dir, DbConfig::default().with_consensus(config)).expect("open")
}

#[test]
fn a_follower_refuses_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = consensus_db(dir.path(), "a");
    db.create_collection("rows").expect("create collection");

    let error = db
        .insert("rows", Record::new("r1"))
        .expect_err("a follower must not accept a write");
    let message = error.to_string();
    assert!(
        message.contains("not the consensus leader"),
        "the refusal must say why, got: {message}"
    );
}

#[test]
fn a_leader_accepts_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = consensus_db(dir.path(), "a");
    db.create_collection("rows").expect("create collection");

    db.consensus_start_election().expect("start election");
    db.consensus_become_leader().expect("become leader");

    db.insert("rows", Record::new("r1"))
        .expect("the leader must accept writes");
    assert!(db.get("rows", "r1").unwrap().is_some());
}

#[test]
fn a_follower_still_serves_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = consensus_db(dir.path(), "a");
    db.create_collection("rows").expect("create collection");
    db.consensus_start_election().expect("start election");
    db.consensus_become_leader().expect("become leader");
    db.insert("rows", Record::new("r1")).expect("leader write");

    // Reopening as a plain follower: reads must keep working. A node that
    // cannot take writes is still a perfectly good place to read from.
    drop(db);
    let follower = consensus_db(dir.path(), "a");
    assert!(
        follower.get("rows", "r1").unwrap().is_some(),
        "a follower must still serve reads"
    );
}
