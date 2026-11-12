//! Genuine fsynced stores/elections: an untouched divergent follower suffix
//! must not be acknowledged as matching the leader's bounded append batch.
//! All requests and votes below come from the actual FSM; only delivery drops.
use super::*;
use crate::distribution::{ClusterNode, DistributionConfig, DistributionStore};

fn node(name: &str) -> ClusterNodeId {
    ClusterNodeId::new(name).unwrap()
}
fn initial_topology(root: &Path) -> ClusterTopology {
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("bounded-append-frontier").unwrap(),
        node_id: node("n1"),
        node_address: "127.0.0.1:18001".into(),
        replication_factor: 1,
        initial_ranges: 2,
        ..DistributionConfig::default()
    };
    let mut distribution = DistributionStore::initialize_at(root, config, true, 10).unwrap();
    for number in 2..=3 {
        distribution
            .join_node(
                ClusterNode::new(
                    node(&format!("n{number}")),
                    format!("127.0.0.1:{}", 18000 + number),
                    1,
                    1_000_000,
                    10 + number,
                )
                .unwrap(),
                &node("n1"),
                10 + number,
            )
            .unwrap();
    }
    distribution.topology().clone()
}
fn open(root: &Path, name: &str, topology: &ClusterTopology) -> MetadataConsensusStore {
    MetadataConsensusStore::open(
        root,
        topology.cluster_id.clone(),
        node(name),
        topology.clone(),
        true,
    )
    .unwrap()
}
fn elect(
    candidate: &mut MetadataConsensusStore,
    voter: &mut MetadataConsensusStore,
    trace: &mut Vec<serde_json::Value>,
) {
    let request = candidate.start_election().unwrap();
    let response = voter.handle_vote_request(request.clone()).unwrap();
    assert!(response.vote_granted, "{request:?}: {response:?}");
    assert_eq!(response.voter_id, voter.status().node_id);
    let votes = BTreeSet::from([request.candidate_id.clone(), response.voter_id.clone()]);
    candidate.become_leader(&votes).unwrap();
    trace.push(
        serde_json::json!({"event":"actual_election", "request":request,
        "response":response, "candidate_status":candidate.status()}),
    );
}
fn report(root: &Path, name: &str, value: &impl Serialize) {
    crate::storage::write_atomic(
        &root.join(name),
        &serde_json::to_vec_pretty(value).unwrap(),
        true,
    )
    .unwrap();
}

#[test]
fn metadata_append_actual_bounded_prefix_ack_cannot_commit_divergent_tail() {
    // Keep all original metadata files and reports on failure AND success.
    // No keys, log entries, terms, counters or response indices are fabricated.
    let root = if let Some(evidence_root) = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT") {
        fs::create_dir_all(&evidence_root).unwrap();
        tempfile::Builder::new()
            .prefix("metadata-append-frontier-")
            .tempdir_in(evidence_root)
            .unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
    .keep();
    eprintln!("RETAINED_METADATA_APPEND_FRONTIER {}", root.display());
    let topology = initial_topology(&root.join("topology-seed"));
    let mut a = open(&root.join("a"), "n1", &topology);
    let mut b = open(&root.join("b"), "n2", &topology);
    let mut c = open(&root.join("c"), "n3", &topology);
    let mut trace = Vec::new();

    // B wins term1 with C. C receives the real barrier1, but B never receives
    // that real ACK, so neither store has any committed entry.
    elect(&mut b, &mut c, &mut trace);
    let first = b.append_request_from(1, 1).unwrap();
    let lost = c.handle_append_request(first.clone()).unwrap();
    assert!(lost.success);
    assert_eq!(lost.match_index, 1);
    trace.push(
        serde_json::json!({"event":"drop_actual_barrier1_reply", "request":first, "response":lost}),
    );
    assert_eq!(b.status().commit_index, 0);
    assert_eq!(c.status().commit_index, 0);

    // B wins terms2 and3 with actual A votes. No Append reaches A or C.
    elect(&mut b, &mut a, &mut trace);
    elect(&mut b, &mut a, &mut trace);
    assert_eq!(b.status().current_term, 3);
    assert_eq!(b.status().last_log_index, 3);
    assert_eq!(a.status().last_log_index, 0);
    let mismatching_heartbeat = b.append_request_from(4, 1).unwrap();
    assert!(mismatching_heartbeat.entries.is_empty());
    let rejection = c
        .handle_append_request(mismatching_heartbeat.clone())
        .unwrap();
    assert!(!rejection.success);
    assert_eq!(c.status().current_term, 3);
    assert_eq!(c.status().last_log_index, 1);
    trace.push(serde_json::json!({"event":"actual_mismatching_heartbeat_updates_term", "request":mismatching_heartbeat, "response":rejection}));

    // C wins term4 with actual A (C's shared entry1 is newer than A's empty
    // log). It adds barrier2 and proposes a genuine, distinct topology at3.
    elect(&mut c, &mut a, &mut trace);
    assert_eq!(c.status().current_term, 4);
    let mut changed = topology.clone();
    changed.generation += 1;
    changed.nodes.get_mut(&node("n3")).unwrap().used_bytes = 333;
    let proposed = c.propose_topology(changed.clone()).unwrap();
    assert_eq!(proposed.index, 3);
    assert!(!proposed.leadership_barrier);
    let b_tail_before = b.state.log.clone();
    assert!(b_tail_before.last().unwrap().leadership_barrier);
    assert_ne!(b_tail_before.last().unwrap(), &proposed);
    assert_eq!(c.status().commit_index, 0);

    // The request matches ONLY old shared entry1. B still has its unrelated
    // barriers at2/3. Record the actual ACK before asserting so the retained
    // report also captures whether C prematurely commits its unreplicated3.
    let bounded = c.append_request_from(1, 1).unwrap();
    assert_eq!(bounded.prev_log_index, 0);
    assert_eq!(bounded.entries.len(), 1);
    assert_eq!(bounded.entries[0].index, 1);
    let covered = bounded.entries.last().unwrap().index;
    let response = b.handle_append_request(bounded.clone()).unwrap();
    assert!(response.success);
    assert_eq!(b.state.log, b_tail_before);
    let advanced = c.record_append_response(&node("n2"), &response).unwrap();
    trace.push(
        serde_json::json!({"event":"actual_bounded_prefix_reply_recorded", "request":bounded,
        "response":response, "covered_frontier":covered, "leader_reported_commit":advanced,
        "a_status":a.status(), "b_status":b.status(), "c_status":c.status(),
        "b_actual_log":b.state.log, "c_actual_state":c.state}),
    );
    report(
        &root,
        "bounded-prefix-result.json",
        &serde_json::json!({
            "source_scope":"public-main481 genuine direct metadata stores; no TLS/physical claim",
            "node_roles":{"A":"n1","B":"n2","C":"n3"},
            "trace":trace, "proposed":proposed, "actual_ack":response,
            "covered_frontier":covered, "leader_advanced":advanced,
            "a_status":a.status(), "b_status":b.status(), "c_status":c.status(),
        }),
    );
    // Keep C's exact pre-recovery files. C is now unavailable to the real
    // A+B election. B's private tail is newer than A's empty log, so A votes B
    // in term5; that majority commits B's barrier4 with the original topology.
    for name in ["a", "b", "c"] {
        let bytes = fs::read(
            root.join(name)
                .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
        )
        .unwrap();
        crate::storage::write_atomic(
            &root.join(format!("before-recovery-{name}.json")),
            &bytes,
            true,
        )
        .unwrap();
    }
    elect(&mut b, &mut a, &mut trace);
    assert_eq!(b.status().current_term, 5);
    assert_eq!(b.status().last_log_index, 4);
    for index in 1..=4 {
        let request = b.append_request_from(index, 1).unwrap();
        assert_eq!(request.entries.len(), 1);
        let reply = a.handle_append_request(request.clone()).unwrap();
        assert!(reply.success);
        assert_eq!(reply.match_index, index);
        let committed = b.record_append_response(&node("n1"), &reply).unwrap();
        assert_eq!(committed, index == 4);
        trace.push(serde_json::json!({"event":"actual_A_B_majority_recovery", "request":request, "response":reply}));
    }
    assert_eq!(b.status().commit_index, 4);
    assert_eq!(b.committed_topology(), &topology);
    let heartbeat = b.append_request_from(5, 1).unwrap();
    assert!(heartbeat.entries.is_empty());
    let heartbeat_reply = a.handle_append_request(heartbeat.clone()).unwrap();
    assert!(heartbeat_reply.success);
    assert_eq!(heartbeat_reply.match_index, 4);
    assert_eq!(a.status().commit_index, 4);
    // Heal C using an actual committed snapshot covering4 with zero entry
    // payloads. After the fix this discards only an UNCOMMITTED proposal;
    // before it, the report records that C had claimed the same3 committed.
    let snapshot = b.append_request_from(1, 1).unwrap();
    assert!(snapshot.entries.is_empty());
    assert_eq!(snapshot.snapshot.as_ref().unwrap().last_included_index, 4);
    let snapshot_reply = c.handle_append_request(snapshot.clone()).unwrap();
    assert!(snapshot_reply.success);
    assert_eq!(snapshot_reply.match_index, 4);
    assert_eq!(c.status().commit_index, 4);
    assert_eq!(c.committed_topology(), &topology);
    trace.push(serde_json::json!({"event":"actual_committed_heartbeat_and_healed_snapshot", "heartbeat":heartbeat, "heartbeat_reply":heartbeat_reply,
        "snapshot":snapshot, "snapshot_reply":snapshot_reply}));
    drop((a, b, c));
    for (directory, id) in [("a", "n1"), ("b", "n2"), ("c", "n3")] {
        let actual = open(&root.join(directory), id, &topology);
        assert_eq!(actual.status().commit_index, 4);
        assert_eq!(actual.committed_topology(), &topology);
    }
    report(
        &root,
        "majority-recovery-result.json",
        &serde_json::json!({
            "trace":trace, "reopen_verified":true, "C_proposal_claimed_committed":advanced,
            "C_proposal_only_existed_on_C":true, "C_proposal_discarded_after_actual_majority_recovery":true,
            "scope":"metadata acknowledgement only; no business-row/TLS/VM/Azure claim",
        }),
    );
    eprintln!(
        "METADATA_APPEND_FRONTIER_SUMMARY {}",
        serde_json::json!({
            "matched_frontier":covered, "returned_ack":response.match_index,
            "premature_topology_commit":advanced, "strict_reopened_stores":3,
            "final_generation":topology.generation, "evidence_root":root,
            "scope":"metadata only; no business-row/TLS/VM/Azure claim",
        })
    );
    assert_eq!(
        response.match_index,
        covered,
        "an untouched local suffix is not a replication ACK; retained {}",
        root.display()
    );
    assert!(
        !advanced,
        "one shared old entry cannot commit new topology3"
    );
}
