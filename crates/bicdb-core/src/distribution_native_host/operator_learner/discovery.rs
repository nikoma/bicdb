//! Pre-root routing discovery. Bootstrap snapshots and leader hints are DATA;
//! only an actual freshly acquired OWN Host MQ can admit a factory requester.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use std::collections::{BTreeSet, VecDeque};

type Discovery = (
    Arc<TcpClusterRelocationTransport>,
    Arc<RwLock<ClusterTopology>>,
    HostMetadataReadObservation,
    Uuid,
);

pub(super) fn discover(
    cluster: &crate::ClusterId,
    local: &ClusterNodeId,
    seed: ClusterNodeId,
    seed_address: &str,
    network: &ClusterNetworkTransportConfig,
    parent: &CancellationToken,
) -> Result<Discovery> {
    let bootstrap =
        TcpClusterBootstrapClient::new(cluster.clone(), local.clone(), network.clone())?;
    let mut queue = VecDeque::from([(seed, seed_address.to_owned())]);
    let mut seen = BTreeSet::new();
    let mut last = String::from("no live native metadata route");
    // Finite even when routing DATA changes or advertises arbitrary cohorts.
    for _ in 0..8 {
        parent.check()?;
        let Some((peer, address)) = queue.pop_front() else {
            break;
        };
        if !seen.insert(peer.clone()) {
            continue;
        }
        let attempt = parent.child_with_deadline(Instant::now() + Duration::from_millis(500));
        let snapshot = match bootstrap.fetch_snapshot_cancellable(&attempt, &peer, &address) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                last = error.to_string();
                continue;
            }
        };
        parent.check()?;
        snapshot.validate()?;
        let mut routes = Vec::new();
        if let Some(leader) = &snapshot.metadata_leader_id {
            if let Some(member) = snapshot.topology.nodes.get(leader) {
                if member.lifecycle == ClusterNodeLifecycle::Active
                    && member.metadata_role == MetadataMemberRole::Voter
                {
                    routes.push((leader.clone(), member.address.clone()));
                }
            }
        }
        routes.extend(
            snapshot
                .topology
                .nodes
                .values()
                .filter(|member| {
                    member.lifecycle == ClusterNodeLifecycle::Active
                        && member.metadata_role == MetadataMemberRole::Voter
                })
                .take(7)
                .map(|member| (member.id.clone(), member.address.clone())),
        );
        let mut unique = BTreeSet::new();
        routes.retain(|route| unique.insert(route.0.clone()));
        // Prioritize the hint without granting it authority. Retain only a
        // bounded number of known routes, with no unbounded hint graph.
        for route in routes.into_iter().rev() {
            if !seen.contains(&route.0) && queue.len() < 16 {
                queue.push_front(route);
            }
        }
        let Some(leader) = snapshot.metadata_leader_id else {
            continue;
        };
        // The actual transport needs a routing identity/incarnation. Absence
        // in stale DATA is a reason to try another route, never a final denial.
        if !snapshot.topology.nodes.contains_key(local) {
            continue;
        }
        let routing = Arc::new(RwLock::new(snapshot.topology));
        let transport = match TcpClusterRelocationTransport::new(
            cluster.clone(),
            local.clone(),
            Arc::clone(&routing),
            network.clone(),
        ) {
            Ok(transport) => Arc::new(transport),
            Err(error) => {
                last = error.to_string();
                continue;
            }
        };
        let probe_business = Uuid::now_v7();
        let probe_budget = parent.child_with_deadline(Instant::now() + Duration::from_secs(1));
        let probe = match transport.acquire_host_metadata_observation(
            &leader,
            probe_business,
            Duration::from_secs(5),
            &probe_budget,
        ) {
            Ok(observed) => observed,
            Err(error) => {
                last = error.to_string();
                continue;
            }
        };
        probe.require_live_request(probe_business)?;
        parent.check()?;
        // Re-attest with a distinct actual MQ under the SAME original parent,
        // so the short discovery attempt cannot silently shorten the accepted
        // factory proof's lifetime. This never extends the caller's budget.
        transport.install_topology(probe.committed_topology(probe_business)?.clone())?;
        let leader = probe.metadata_leader_id();
        let business = Uuid::now_v7();
        let observed = transport.acquire_host_metadata_observation(
            leader,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        observed.require_live_request(business)?;
        parent.check()?;
        transport.install_topology(observed.committed_topology(business)?.clone())?;
        return Ok((transport, routing, observed, business));
    }
    parent.check()?;
    Err(invalid(&format!(
        "fresh learner current metadata unavailable: {last}"
    )))
}

pub(super) fn require_requester(
    observed: &HostMetadataReadObservation,
    business: Uuid,
    local: &ClusterNodeId,
    address: &str,
    incarnation: u64,
) -> Result<()> {
    let topology = observed.committed_topology(business)?;
    let member = topology.nodes.get(local).ok_or_else(|| {
        invalid("learner identity must be actually registered before factory creation")
    })?;
    if observed.requester_node_id() != local
        || observed.requester_incarnation() != incarnation
        || member.address != address
        || member.incarnation != incarnation
        || member.native_store_birth_v1.is_some()
        || member.metadata_role != MetadataMemberRole::Learner
        || member.lifecycle != ClusterNodeLifecycle::Active
        || topology.ranges.values().any(|range| {
            range
                .replicas
                .iter()
                .any(|replica| &replica.node_id == local)
        })
    {
        return Err(invalid(
            "fresh learner registration/address/incarnation/birth differs",
        ));
    }
    Ok(())
}

pub(super) fn schema(
    transport: &TcpClusterRelocationTransport,
    observed: &HostMetadataReadObservation,
    business: Uuid,
    parent: &CancellationToken,
) -> Result<crate::ClusterSchemaBundle> {
    let topology = observed.committed_topology(business)?;
    let range = topology
        .ranges
        .values()
        .next()
        .ok_or_else(|| invalid("learner has no committed schema domain"))?;
    let expected = range
        .required_schema_sha256(&topology.nodes)
        .ok_or_else(|| invalid("learner has no committed schema digest"))?;
    let eligible = |id: &ClusterNodeId| {
        topology.nodes.get(id).is_some_and(|member| {
            member.lifecycle == ClusterNodeLifecycle::Active
                && member.metadata_role == MetadataMemberRole::Voter
                && member.address.parse::<std::net::SocketAddr>().is_ok()
                && member
                    .native_store_birth_v1
                    .as_ref()
                    .is_some_and(|birth| birth.writer_enforcement_version() == 1)
                && member
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    .is_some_and(|sha| sha.as_str() == expected)
        })
    };
    let mut peers = Vec::new();
    if eligible(observed.metadata_leader_id()) {
        peers.push(observed.metadata_leader_id().clone());
    }
    peers.extend(
        topology
            .nodes
            .keys()
            .filter(|id| *id != observed.metadata_leader_id() && eligible(id))
            .take(7)
            .cloned(),
    );
    let mut last = String::from("no current enrolled schema source");
    for peer in peers {
        observed.require_live_request(business)?;
        parent.check()?;
        let attempt = parent.child_with_deadline(Instant::now() + Duration::from_millis(750));
        match transport.export_native_learner_schema(&peer, range, &attempt) {
            Ok(bundle) => {
                bundle.verify()?;
                if bundle.fingerprint.sha256 != expected {
                    return Err(invalid(
                        "native schema source differs from actual committed digest",
                    ));
                }
                observed.require_live_request(business)?;
                parent.check()?;
                return Ok(bundle);
            }
            Err(error) => last = error.to_string(),
        }
    }
    parent.check()?;
    Err(invalid(&format!(
        "fresh learner current schema unavailable: {last}"
    )))
}
