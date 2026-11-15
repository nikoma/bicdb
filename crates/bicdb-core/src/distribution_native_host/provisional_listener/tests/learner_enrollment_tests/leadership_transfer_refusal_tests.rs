//! Refused metadata leadership transfers on four genuine voters and one
//! registered metadata LEARNER. Nothing is sent and leadership is unchanged.
use super::*;
use crate::distribution_consensus::MetadataConsensusRole;

/// (b) A learner, an unknown node and the leader itself are refused.
#[test]
fn native_metadata_leadership_transfer_to_non_voter_unknown_or_self_is_refused_unchanged() {
    let f = EnrollmentFixture::new();
    let token = CancellationToken::uncancelable()
        .child_with_deadline(std::time::Instant::now() + Duration::from_secs(5));
    let before = f.launches[0].metadata.lock().status();
    assert_eq!(before.role, MetadataConsensusRole::Leader);
    assert!(before.learners.contains(&node(4)));
    let backend = NativeMetadataControlBackend::borrow_initial(&f.launches[0]);
    let unknown = ClusterNodeId::new("n9").unwrap();
    for (target, expected) in [
        (node(4), "n4 is not an active metadata voter (role Learner"),
        (unknown, "n9 is not a member of the committed configuration"),
        (node(0), "n0 is this node and already leads metadata"),
    ] {
        let refused = backend
            .transfer_metadata_leadership(&target, &token)
            .unwrap_err()
            .to_string();
        eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER refused: {refused}");
        assert!(
            refused.contains("leadership transfer refused") && refused.contains(expected),
            "{refused}"
        );
        assert_eq!(f.launches[0].metadata.lock().status(), before);
    }
    // No window was left open: the leader still proposes and commits.
    let mut next = f.launches[0].metadata.lock().committed_topology().clone();
    next.generation += 1;
    f.launches[0]
        .metadata
        .lock()
        .propose_topology(next.clone())
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    for launch in &f.launches {
        assert_eq!(launch.metadata.lock().committed_topology(), &next);
        assert_eq!(
            launch.metadata.lock().status().current_term,
            before.current_term
        );
    }
    f.shutdown();
}
