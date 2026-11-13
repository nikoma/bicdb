//! Required cases: actual planner retirement and actual metadata-leader caller.
//! A topology candidate is data, never image/quorum/serving authority.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AssignmentMode {
    OriginalLeader,
    AutomaticReplicaPlan,
    MetadataLeader,
}
pub(super) fn select(
    topology: &ClusterTopology,
    config: &crate::DistributionConfig,
    mode: AssignmentMode,
) -> (Vec<crate::RangeDescriptor>, Option<crate::RebalancePlan>) {
    if mode != AssignmentMode::AutomaticReplicaPlan {
        return (topology.ranges.values().take(2).cloned().collect(), None);
    }
    let options = crate::RebalanceOptions {
        max_replica_moves: 2,
        ..Default::default()
    };
    // Use the actual fixture's committed membership clock. These are placement
    // hints; real TLS/role/MStore/native admission still gates each mutation.
    let now = topology
        .nodes
        .values()
        .map(|n| n.last_heartbeat_ms)
        .max()
        .unwrap();
    let plan = crate::build_rebalance_plan(topology, config, &options, now).unwrap();
    assert_eq!(plan.topology_generation, topology.generation);
    assert_eq!(
        plan.replica_moves.len(),
        2,
        "actual planner did not offer two moves: {plan:?}"
    );
    assert!(plan.unplaced_ranges.is_empty());
    let ranges = plan
        .replica_moves
        .iter()
        .map(|movement| {
            assert_eq!(
                movement.target,
                node(4),
                "do not rewrite actual planner targets"
            );
            assert_eq!(movement.reason, crate::ReplicaMoveReason::LoadBalance);
            let range = topology.range_by_id(movement.range_id).unwrap();
            assert_eq!(movement.expected_epoch, range.epoch);
            assert!(movement.source.as_ref().is_some_and(|s| range
                .replicas
                .iter()
                .any(|r| &r.node_id == s && r.role == RangeReplicaRole::Voter)));
            range.clone()
        })
        .collect::<Vec<_>>();
    assert!(
        plan.replica_moves
            .iter()
            .zip(&ranges)
            .any(|(movement, range)| movement.source.as_ref() != Some(&range.leader)),
        "required nonleader-retirement condition not reached; do not substitute a leader-only plan"
    );
    (ranges, Some(plan))
}
pub(super) fn pending(
    current: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    plan: &crate::RebalancePlan,
) -> ClusterTopology {
    assert_eq!(plan.topology_generation, current.generation);
    let mut candidate = assign_two_ranges(current, ranges);
    // Reuse only the fixture's Pending allocator. Preserve the real planner's
    // selected retirement/target/reason unchanged. Actual native consensus
    // commits this candidate; no file-store or planned count grants a fence.
    for movement in &plan.replica_moves {
        let proposal = candidate
            .relocations
            .values_mut()
            .find(|r| r.range_id == movement.range_id)
            .unwrap();
        assert_eq!(proposal.expected_epoch, movement.expected_epoch);
        assert_eq!(proposal.target, movement.target);
        proposal.source = movement.source.clone();
        proposal.reason = movement.reason;
        proposal.plan_id = plan.id.clone();
    }
    candidate.applied_rebalance_plans.push_back(plan.id.clone());
    candidate.validate().unwrap();
    candidate
}
#[test]
fn native_registered_fifth_placement_actual_planner_retiree_is_not_initiator() {
    run_registered_image_baseline_with_placement(
        Case::Required,
        AssignmentMode::AutomaticReplicaPlan,
    );
}
#[test]
fn native_registered_fifth_placement_actual_metadata_leader_can_initiate() {
    run_registered_image_baseline_with_placement(Case::Required, AssignmentMode::MetadataLeader);
}
