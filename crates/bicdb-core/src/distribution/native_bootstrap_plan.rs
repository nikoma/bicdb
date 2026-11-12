//! Configuration planning BEFORE any store or user data exists. This reuses
//! the normal placement policy; it is not an online relocation execution.
use super::*;

pub(crate) fn plan_initial_native_topology(
    config: &DistributionConfig,
    members: Vec<ClusterNode>,
    now_ms: u64,
    cancellation: &crate::CancellationToken,
) -> Result<ClusterTopology> {
    config.validate()?;
    cancellation.check()?;
    if !config.enabled
        || config.range_image_bootstrap_id.is_none()
        || members.is_empty()
        || members.len() > 64
        || usize::from(config.replication_factor) > members.len()
    {
        return Err(cluster_error("invalid native initial cluster membership"));
    }
    let mut nodes = BTreeMap::new();
    for node in members {
        if node.lifecycle != ClusterNodeLifecycle::Active
            || node.metadata_role != MetadataMemberRole::Voter
            || node.native_store_birth_v1.is_some()
            || node.tls_certificate_sha256.is_none()
            || node.pending_tls_certificate_sha256.is_some()
            || node.used_bytes != 0
        {
            return Err(cluster_error(
                "initial native members must be active pinned empty unenrolled voters",
            ));
        }
        if nodes.insert(node.id.clone(), node).is_some() {
            return Err(cluster_error("duplicate native initial node identity"));
        }
    }
    let local = nodes
        .get(&config.node_id)
        .ok_or_else(|| cluster_error("native initial coordinator is absent"))?
        .clone();
    let mut topology = ClusterTopology::new(config, local, now_ms)?;
    topology.nodes = nodes;
    topology.validate()?;
    let options = RebalanceOptions {
        max_replica_moves: 4096,
        max_moves_per_node: 4096,
        max_leader_transfers: 4096,
        max_bytes_in_flight: u64::MAX,
        max_target_utilization_per_million: 1_000_000,
        unknown_range_bytes: 1,
    };
    let mut seen = BTreeSet::new();
    let limit = usize::try_from(config.initial_ranges)
        .unwrap_or(65_536)
        .saturating_mul(usize::from(config.replication_factor).saturating_add(2))
        .saturating_add(64);
    for _ in 0..limit {
        cancellation.check()?;
        // Bound the actual serialized metadata before running another pass.
        crate::distribution_range_consensus::validate_native_json_size(&topology, 2 * 1024 * 1024)?;
        let signature = topology
            .ranges
            .values()
            .map(|r| {
                (
                    r.id,
                    r.leader.clone(),
                    r.replicas
                        .iter()
                        .map(|p| p.node_id.clone())
                        .collect::<BTreeSet<_>>(),
                )
            })
            .collect::<Vec<_>>();
        if !seen.insert(signature) {
            return Err(cluster_error("native initial placement did not converge"));
        }
        let plan = build_rebalance_plan(&topology, config, &options, now_ms)?;
        if plan.replica_moves.is_empty() && plan.leader_transfers.is_empty() {
            if !plan.unplaced_ranges.is_empty()
                || topology
                    .ranges
                    .values()
                    .any(|r| r.voter_count() != usize::from(config.replication_factor))
            {
                return Err(cluster_error(
                    "native initial placement cannot satisfy replica policy",
                ));
            }
            topology.validate()?;
            return Ok(topology);
        }
        for movement in plan.replica_moves {
            cancellation.check()?;
            let replica = ReplicaId::new(topology.next_replica_id)?;
            topology.next_replica_id = topology
                .next_replica_id
                .checked_add(1)
                .ok_or_else(|| cluster_error("native initial replica identity exhausted"))?;
            let range = topology
                .ranges
                .values_mut()
                .find(|r| r.id == movement.range_id)
                .ok_or_else(|| cluster_error("initial planner referred to an absent range"))?;
            if range.epoch != 1 || movement.expected_epoch != 1 {
                return Err(cluster_error(
                    "initial placement cannot perform a live epoch transition",
                ));
            }
            if range.replicas.iter().any(|p| p.node_id == movement.target) {
                return Err(cluster_error("initial planner duplicated a replica"));
            }
            if let Some(source) = &movement.source {
                range.replicas.retain(|p| &p.node_id != source);
                if &range.leader == source {
                    range.leader = movement.target.clone();
                }
            }
            range.replicas.push(RangeReplica {
                id: replica,
                node_id: movement.target,
                role: RangeReplicaRole::Voter,
            });
        }
        for transfer in plan.leader_transfers {
            cancellation.check()?;
            let range = topology
                .ranges
                .values_mut()
                .find(|r| r.id == transfer.range_id)
                .ok_or_else(|| {
                    cluster_error("initial leader planner referred to an absent range")
                })?;
            if transfer.expected_epoch != 1
                || !range
                    .replicas
                    .iter()
                    .any(|p| p.node_id == transfer.target && p.role == RangeReplicaRole::Voter)
            {
                return Err(cluster_error("initial leader is not a planned voter"));
            }
            range.leader = transfer.target;
        }
        topology.validate()?;
    }
    Err(cluster_error(
        "native initial placement exceeded its bounded pass count",
    ))
}
