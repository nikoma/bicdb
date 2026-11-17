//! Operator-requested metadata leadership transfer over three genuine
//! members: real stores, real supervisors, pinned mutual TLS. No role, term
//! or vote is set by hand; every election is the supervisors' own.
use super::*;
use crate::MetadataConsensusStatus;

fn backend(f: &Network, i: usize) -> NativeMetadataControlBackend {
    NativeMetadataControlBackend::borrow_initial(f.launch(i))
}
fn status(f: &Network, i: usize) -> MetadataConsensusStatus {
    f.launch(i).metadata.lock().status()
}
/// Member 0 elected by actual votes, all three supervisors running.
fn led_by_first() -> Network {
    let mut f = Network::new();
    f.start(0);
    assert_eq!(f.wait_leader(&[0]), 0);
    f.start(1);
    f.start(2);
    f.fresh_mq(1, 0);
    f.wait_replicated(1, 0);
    f.wait_replicated(2, 0);
    f
}
fn next_configuration(f: &Network, leader: usize) -> ClusterTopology {
    let mut topology = f
        .launch(leader)
        .metadata
        .lock()
        .committed_topology()
        .clone();
    topology.generation += 1;
    topology
        .nodes
        .get_mut(&node(leader))
        .unwrap()
        .last_heartbeat_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    topology
}
/// One configuration proposed on `leader` and committed by its supervisor.
fn commit_through(f: &Network, leader: usize) -> ClusterTopology {
    let topology = next_configuration(f, leader);
    let entry = f
        .launch(leader)
        .metadata
        .lock()
        .propose_topology(topology.clone())
        .unwrap();
    let until = Instant::now() + Duration::from_secs(8);
    loop {
        {
            let store = f.launch(leader).metadata.lock();
            if store.status().commit_index >= entry.index {
                assert_eq!(store.committed_topology(), &topology);
                return topology;
            }
        }
        assert!(
            Instant::now() < until,
            "configuration never committed through member {leader}: {:?}",
            status(f, leader)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn stable_leader(f: &Network, leader: usize, term: u64) {
    let now = status(f, leader);
    assert_eq!(now.role, MetadataConsensusRole::Leader, "{now:?}");
    assert_eq!(now.current_term, term, "{now:?}");
}

/// (a) A caught-up voter becomes the leader in a higher term, the old leader
/// follows it, nothing committed is lost and the new leader commits.
#[test]
fn native_metadata_leadership_transfer_to_caught_up_voter_moves_leadership_and_keeps_log() {
    let f = led_by_first();
    let committed = commit_through(&f, 0);
    f.wait_replicated(1, 0);
    f.wait_replicated(2, 0);
    let before = status(&f, 0);

    let report = backend(&f, 0)
        .transfer_metadata_leadership(&node(1), &budget())
        .unwrap();
    eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER report={report:?}");
    assert_eq!((&report.from, &report.to), (&node(0), &node(1)));
    assert_eq!(report.from_term, before.current_term);
    assert!(report.term > before.current_term);

    // The outcome the old leader reported is its own persisted follower state.
    let old = status(&f, 0);
    assert_eq!(old.role, MetadataConsensusRole::Follower);
    assert_eq!(old.leader_id, Some(node(1)));
    assert_eq!(old.current_term, report.term);
    // The target is the anchored leader of that term: its barrier committed.
    assert_eq!(f.wait_leader(&[1]), 1);
    let new = status(&f, 1);
    assert_eq!(new.current_term, report.term);
    f.fresh_mq(0, 1);
    f.fresh_mq(2, 1);
    f.wait_replicated(0, 1);
    f.wait_replicated(2, 1);
    // Exactly one leader, and every committed entry is still there.
    for i in 0..3 {
        let store = f.launch(i).metadata.lock();
        let now = store.status();
        assert_eq!(now.current_term, report.term, "member {i}: {now:?}");
        assert_eq!(
            now.role == MetadataConsensusRole::Leader,
            i == 1,
            "member {i}: {now:?}"
        );
        assert!(
            now.commit_index > before.commit_index,
            "member {i}: {now:?}"
        );
        assert_eq!(store.committed_topology(), &committed, "member {i}");
    }
    // The old leader no longer proposes; the new one commits a configuration
    // that reaches every member.
    let stale = next_configuration(&f, 0);
    let refused = f
        .launch(0)
        .metadata
        .lock()
        .propose_topology(stale)
        .unwrap_err();
    assert!(refused.to_string().contains("only the leader"), "{refused}");
    let next = commit_through(&f, 1);
    f.wait_replicated(0, 1);
    f.wait_replicated(2, 1);
    for i in 0..3 {
        assert_eq!(f.launch(i).metadata.lock().committed_topology(), &next);
    }
    stable_leader(&f, 1, report.term);
}

/// (c) Only the leader accepts the action, and a member only campaigns for
/// the leader it follows in its current term.
#[test]
fn native_metadata_leadership_transfer_requested_on_non_leader_is_refused() {
    let f = led_by_first();
    let before = (0..3).map(|i| status(&f, i)).collect::<Vec<_>>();
    let refused = backend(&f, 1)
        .transfer_metadata_leadership(&node(2), &budget())
        .unwrap_err()
        .to_string();
    eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER non-leader refused: {refused}");
    assert!(
        refused.contains("this node (n1) is not the metadata leader")
            && refused.contains("the leader is n0"),
        "{refused}"
    );
    // The peer request itself: member 2 is an authenticated voter but not the
    // leader member 1 follows. Member 1 refuses and does not campaign.
    let request = crate::distribution_consensus::MetadataLeadershipTransferRequest {
        cluster_id: before[1].cluster_id.clone(),
        term: before[1].current_term,
        leader_id: node(2),
        target_id: node(1),
        last_log_index: before[1].last_log_index,
        last_log_term: before[1].last_log_term,
    };
    let reply = f
        .launch(2)
        .transport
        .request_metadata_leadership_transfer(request.clone(), &budget())
        .unwrap();
    assert!(!reply.accepted);
    assert!(
        reply
            .refusal
            .as_deref()
            .is_some_and(|text| text.contains("is not the leader this node follows")),
        "{reply:?}"
    );
    // A request that names another sender than the transport's own identity
    // never leaves the caller.
    let mut forged = request;
    forged.leader_id = node(0);
    assert!(f
        .launch(2)
        .transport
        .request_metadata_leadership_transfer(forged, &budget())
        .unwrap_err()
        .to_string()
        .contains("must be the transport caller"));
    std::thread::sleep(Duration::from_millis(900));
    for i in 0..3 {
        let now = status(&f, i);
        assert_eq!(now.current_term, before[i].current_term, "member {i}");
        assert_eq!(now.role, before[i].role, "member {i}");
    }
    commit_through(&f, 0);
}

/// (d) An unreachable target: the action fails inside its bound, the old
/// leader keeps its term and commits again. Also the proposal window itself.
#[test]
fn native_metadata_leadership_transfer_unreachable_target_fails_and_old_leader_keeps_leading() {
    let mut f = led_by_first();
    let term = status(&f, 0).current_term;
    // The target is caught up and acknowledged, then loses its worker and
    // its ingress.
    std::thread::sleep(Duration::from_millis(600));
    f.stop(1);
    let started = Instant::now();
    let failed = backend(&f, 0)
        .transfer_metadata_leadership(&node(1), &budget())
        .unwrap_err()
        .to_string();
    eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER unreachable (caught up): {failed}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        (failed.contains("could not be asked to campaign")
            || failed.contains("was not brought up to date"))
            && failed.contains(&format!("this node keeps leading term {term}")),
        "{failed}"
    );
    stable_leader(&f, 0, term);
    // Proposals resumed: the remaining majority commits. The stopped target
    // is now behind, so the next attempt ends in the catch-up bound.
    commit_through(&f, 0);
    let started = Instant::now();
    let failed = backend(&f, 0)
        .transfer_metadata_leadership(&node(1), &budget())
        .unwrap_err()
        .to_string();
    eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER unreachable (behind): {failed}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        failed.contains("was not brought up to date")
            && failed.contains(&format!("this node keeps leading term {term}")),
        "{failed}"
    );
    stable_leader(&f, 0, term);
    commit_through(&f, 0);

    // The window: while open, this leader refuses every proposal; it closes
    // explicitly and also by itself.
    {
        let mut store = f.launch(0).metadata.lock();
        let opened = store
            .begin_leadership_transfer(&node(2), Duration::from_millis(300))
            .unwrap();
        assert_eq!(opened, term);
        let again = store
            .begin_leadership_transfer(&node(2), Duration::from_millis(300))
            .unwrap_err();
        assert!(again.to_string().contains("already in progress"), "{again}");
    }
    let during = next_configuration(&f, 0);
    let blocked = f
        .launch(0)
        .metadata
        .lock()
        .propose_topology(during)
        .unwrap_err();
    assert!(
        blocked
            .to_string()
            .contains("metadata leadership is being transferred to n2"),
        "{blocked}"
    );
    std::thread::sleep(Duration::from_millis(350));
    commit_through(&f, 0);
    {
        let mut store = f.launch(0).metadata.lock();
        store
            .begin_leadership_transfer(&node(2), Duration::from_secs(30))
            .unwrap();
        store.end_leadership_transfer(term);
    }
    commit_through(&f, 0);
    stable_leader(&f, 0, term);

    // The returning target catches up and a transfer to it then succeeds.
    f.restart(1);
    f.wait_replicated(1, 0);
    let report = backend(&f, 0)
        .transfer_metadata_leadership(&node(1), &budget())
        .unwrap();
    assert!(report.term > term);
    assert_eq!(f.wait_leader(&[1]), 1);
}
