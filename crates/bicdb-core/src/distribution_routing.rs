use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::distribution::{
    distribution_key_token, ClusterId, ClusterNode, ClusterNodeId, ClusterNodeLifecycle,
    ClusterNodeLiveness, ClusterTopology, DistributionConfig, RangeDescriptor, RangeId,
    RangeReplicaRole, ReplicaId,
};
use crate::error::{BicDbError, Result};

fn routing_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PointReadPolicy {
    Leader,
    BoundedStaleness { max_staleness_ms: u64 },
    NearestReplica { max_staleness_ms: u64 },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum PointOperation {
    Read { policy: PointReadPolicy },
    Write,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaRoutingProgress {
    pub replica_id: ReplicaId,
    pub durable_commit_sequence: u64,
    /// Commit timestamp of `durable_commit_sequence`, used to enforce a
    /// wall-clock bounded-staleness contract when the leader is ahead.
    pub last_applied_commit_timestamp_ms: u64,
    pub observed_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteTarget {
    pub node_id: ClusterNodeId,
    pub address: String,
    pub replica_id: ReplicaId,
    pub leader: bool,
    pub locality_matches: usize,
    pub observed_staleness_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutedRequestHeader {
    pub cluster_id: ClusterId,
    pub hash_version: u32,
    pub topology_generation: u64,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub token: u64,
    pub operation: PointOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PointRoute {
    pub header: RoutedRequestHeader,
    pub primary: RouteTarget,
    /// Ordered failover candidates. Writes never have fallbacks.
    pub fallbacks: Vec<RouteTarget>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteRetryCode {
    ClusterMismatch,
    HashVersionMismatch,
    StaleTopology,
    StaleRangeEpoch,
    RangeMoved,
    NotReplica,
    NotLeader,
    ReplicaTooStale,
    NoLiveReplica,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteRetry {
    pub code: RouteRetryCode,
    pub message: String,
    pub refresh_topology: bool,
    pub topology_generation: u64,
    pub range_id: Option<RangeId>,
    pub current_range_epoch: Option<u64>,
    pub redirect: Option<RouteTarget>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum RouteDecision {
    Routed { route: PointRoute },
    Retry { retry: RouteRetry },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum RouteValidation {
    Accepted,
    Retry { retry: RouteRetry },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopologyInstall {
    Installed,
    Unchanged,
    StaleIgnored,
}

#[derive(Clone, Debug)]
struct RoutingCacheState {
    topology: ClusterTopology,
    progress: BTreeMap<ReplicaId, ReplicaRoutingProgress>,
}

/// Thread-safe generation cache shared by gateways and connection pools.
///
/// A generation can be installed only once with one exact value. Receiving a
/// different topology at the same generation is treated as split-brain
/// evidence and fails closed.
#[derive(Clone, Debug)]
pub struct ClusterRequestRouter {
    config: DistributionConfig,
    state: Arc<RwLock<RoutingCacheState>>,
}

impl ClusterRequestRouter {
    pub fn new(config: DistributionConfig, topology: ClusterTopology) -> Result<Self> {
        config.validate()?;
        topology.validate()?;
        if config.cluster_id != topology.cluster_id {
            return Err(routing_error(format!(
                "router cluster {} does not match topology {}",
                config.cluster_id, topology.cluster_id
            )));
        }
        Ok(Self {
            config,
            state: Arc::new(RwLock::new(RoutingCacheState {
                topology,
                progress: BTreeMap::new(),
            })),
        })
    }

    pub fn topology(&self) -> Result<ClusterTopology> {
        Ok(self
            .state
            .read()
            .map_err(|_| routing_error("routing topology lock is poisoned"))?
            .topology
            .clone())
    }

    pub fn topology_generation(&self) -> Result<u64> {
        Ok(self
            .state
            .read()
            .map_err(|_| routing_error("routing topology lock is poisoned"))?
            .topology
            .generation)
    }

    pub fn install_topology(&self, topology: ClusterTopology) -> Result<TopologyInstall> {
        topology.validate()?;
        let mut state = self
            .state
            .write()
            .map_err(|_| routing_error("routing topology lock is poisoned"))?;
        if topology.cluster_id != state.topology.cluster_id {
            return Err(routing_error(format!(
                "cannot install topology for cluster {} into router for {}",
                topology.cluster_id, state.topology.cluster_id
            )));
        }
        if topology.hash_version != state.topology.hash_version {
            return Err(routing_error(format!(
                "cannot change routing hash version from {} to {}",
                state.topology.hash_version, topology.hash_version
            )));
        }
        if topology.generation < state.topology.generation {
            return Ok(TopologyInstall::StaleIgnored);
        }
        if topology.generation == state.topology.generation {
            if topology == state.topology {
                return Ok(TopologyInstall::Unchanged);
            }
            return Err(routing_error(format!(
                "conflicting topology payloads at generation {}",
                topology.generation
            )));
        }
        let live_replica_ids = topology
            .ranges
            .values()
            .flat_map(|range| range.replicas.iter().map(|replica| replica.id))
            .collect::<std::collections::BTreeSet<_>>();
        state
            .progress
            .retain(|replica_id, _| live_replica_ids.contains(replica_id));
        state.topology = topology;
        Ok(TopologyInstall::Installed)
    }

    pub fn update_replica_progress(&self, progress: ReplicaRoutingProgress) -> Result<bool> {
        let mut state = self
            .state
            .write()
            .map_err(|_| routing_error("routing topology lock is poisoned"))?;
        if !state.topology.ranges.values().any(|range| {
            range
                .replicas
                .iter()
                .any(|replica| replica.id == progress.replica_id)
        }) {
            return Err(routing_error(format!(
                "routing progress references unknown replica {}",
                progress.replica_id
            )));
        }
        if let Some(current) = state.progress.get(&progress.replica_id) {
            if progress.durable_commit_sequence < current.durable_commit_sequence
                || progress.last_applied_commit_timestamp_ms
                    < current.last_applied_commit_timestamp_ms
                || progress.observed_at_ms < current.observed_at_ms
            {
                return Err(routing_error(format!(
                    "routing progress for {} cannot move backwards",
                    progress.replica_id
                )));
            }
            if current == &progress {
                return Ok(false);
            }
        }
        state.progress.insert(progress.replica_id, progress);
        Ok(true)
    }

    pub fn route_point(
        &self,
        namespace: &str,
        key: &str,
        operation: PointOperation,
        requester: Option<&ClusterNodeId>,
        now_ms: u64,
    ) -> Result<RouteDecision> {
        let state = self
            .state
            .read()
            .map_err(|_| routing_error("routing topology lock is poisoned"))?;
        let token = distribution_key_token(namespace, key);
        let range = state.topology.range_for_token(token)?;
        let header = RoutedRequestHeader {
            cluster_id: state.topology.cluster_id.clone(),
            hash_version: state.topology.hash_version,
            topology_generation: state.topology.generation,
            range_id: range.id,
            range_epoch: range.epoch,
            token,
            operation,
        };
        let candidates = route_candidates(
            &state.topology,
            &state.progress,
            range,
            operation,
            requester,
            now_ms,
            &self.config,
        );
        let Some(primary) = candidates.first().cloned() else {
            return Ok(RouteDecision::Retry {
                retry: route_retry(
                    RouteRetryCode::NoLiveReplica,
                    format!(
                        "range {} has no live replica satisfying {operation:?}",
                        range.id
                    ),
                    &state.topology,
                    Some(range),
                    leader_target(&state.topology, &state.progress, range, requester, now_ms),
                    false,
                ),
            });
        };
        let fallbacks = if matches!(operation, PointOperation::Write) {
            Vec::new()
        } else {
            candidates.into_iter().skip(1).collect()
        };
        Ok(RouteDecision::Routed {
            route: PointRoute {
                header,
                primary,
                fallbacks,
            },
        })
    }

    /// Validate a routed request at its selected owner before touching data.
    /// This is the stale-owner fence: topology/range/epoch/leader checks happen
    /// again on the destination, not only at the gateway.
    pub fn validate_at_node(
        &self,
        local_node: &ClusterNodeId,
        header: &RoutedRequestHeader,
        now_ms: u64,
    ) -> Result<RouteValidation> {
        let state = self
            .state
            .read()
            .map_err(|_| routing_error("routing topology lock is poisoned"))?;
        let topology = &state.topology;
        if header.cluster_id != topology.cluster_id {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::ClusterMismatch,
                format!(
                    "request cluster {} does not match {}",
                    header.cluster_id, topology.cluster_id
                ),
                topology,
                None,
                None,
                true,
            )));
        }
        if header.hash_version != topology.hash_version {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::HashVersionMismatch,
                format!(
                    "request hash version {} does not match {}",
                    header.hash_version, topology.hash_version
                ),
                topology,
                None,
                None,
                true,
            )));
        }
        let current = topology.range_for_token(header.token)?;
        let redirect = leader_target(topology, &state.progress, current, Some(local_node), now_ms);
        if header.topology_generation > topology.generation {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::StaleTopology,
                format!(
                    "request uses topology generation {}, local cache has {}",
                    header.topology_generation, topology.generation
                ),
                topology,
                Some(current),
                redirect,
                true,
            )));
        }
        if current.id != header.range_id {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::RangeMoved,
                format!(
                    "token {} moved from request range {} to {}",
                    header.token, header.range_id, current.id
                ),
                topology,
                Some(current),
                redirect,
                true,
            )));
        }
        if current.epoch != header.range_epoch {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::StaleRangeEpoch,
                format!(
                    "range {} request epoch {} is stale; current epoch is {}",
                    current.id, header.range_epoch, current.epoch
                ),
                topology,
                Some(current),
                redirect,
                true,
            )));
        }
        let Some(replica) = current
            .replicas
            .iter()
            .find(|replica| &replica.node_id == local_node)
        else {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::NotReplica,
                format!(
                    "node {local_node} is not a replica for range {}",
                    current.id
                ),
                topology,
                Some(current),
                redirect,
                true,
            )));
        };
        if replica.role != RangeReplicaRole::Voter {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::NotReplica,
                format!(
                    "node {local_node} is only a learner for range {}",
                    current.id
                ),
                topology,
                Some(current),
                redirect,
                false,
            )));
        }
        if matches!(header.operation, PointOperation::Write) && &current.leader != local_node {
            return Ok(retry_validation(route_retry(
                RouteRetryCode::NotLeader,
                format!("node {local_node} is not leader for range {}", current.id),
                topology,
                Some(current),
                redirect,
                false,
            )));
        }
        if let PointOperation::Read {
            policy:
                PointReadPolicy::BoundedStaleness { max_staleness_ms }
                | PointReadPolicy::NearestReplica { max_staleness_ms },
        } = header.operation
        {
            if &current.leader != local_node {
                let staleness = state
                    .progress
                    .get(&replica.id)
                    .and_then(|progress| {
                        replica_staleness_ms(&state.progress, current, progress, now_ms)
                    })
                    .unwrap_or(u64::MAX);
                if staleness > max_staleness_ms {
                    return Ok(retry_validation(route_retry(
                        RouteRetryCode::ReplicaTooStale,
                        format!(
                            "replica {} staleness {staleness}ms exceeds {max_staleness_ms}ms",
                            replica.id
                        ),
                        topology,
                        Some(current),
                        redirect,
                        false,
                    )));
                }
            }
        }
        Ok(RouteValidation::Accepted)
    }
}

fn retry_validation(retry: RouteRetry) -> RouteValidation {
    RouteValidation::Retry { retry }
}

fn route_retry(
    code: RouteRetryCode,
    message: String,
    topology: &ClusterTopology,
    range: Option<&RangeDescriptor>,
    redirect: Option<RouteTarget>,
    refresh_topology: bool,
) -> RouteRetry {
    RouteRetry {
        code,
        message,
        refresh_topology,
        topology_generation: topology.generation,
        range_id: range.map(|range| range.id),
        current_range_epoch: range.map(|range| range.epoch),
        redirect,
    }
}

fn route_candidates(
    topology: &ClusterTopology,
    progress: &BTreeMap<ReplicaId, ReplicaRoutingProgress>,
    range: &RangeDescriptor,
    operation: PointOperation,
    requester: Option<&ClusterNodeId>,
    now_ms: u64,
    config: &DistributionConfig,
) -> Vec<RouteTarget> {
    if matches!(
        operation,
        PointOperation::Write
            | PointOperation::Read {
                policy: PointReadPolicy::Leader
            }
    ) {
        return leader_target(topology, progress, range, requester, now_ms)
            .filter(|target| {
                topology.nodes.get(&target.node_id).is_some_and(|node| {
                    route_node_is_live(
                        node,
                        now_ms,
                        config,
                        matches!(operation, PointOperation::Write),
                    )
                })
            })
            .into_iter()
            .collect();
    }
    let max_staleness_ms = match operation {
        PointOperation::Read {
            policy:
                PointReadPolicy::BoundedStaleness { max_staleness_ms }
                | PointReadPolicy::NearestReplica { max_staleness_ms },
        } => max_staleness_ms,
        _ => unreachable!("leader-only operations returned above"),
    };
    let leader_progress = range
        .replicas
        .iter()
        .find(|replica| replica.node_id == range.leader)
        .and_then(|replica| progress.get(&replica.id));
    let mut candidates = range
        .replicas
        .iter()
        .filter(|replica| replica.role == RangeReplicaRole::Voter)
        .filter_map(|replica| {
            let node = topology.nodes.get(&replica.node_id)?;
            if !route_node_is_live(node, now_ms, config, false) {
                return None;
            }
            let leader = replica.node_id == range.leader;
            let observed_staleness_ms = if leader {
                0
            } else {
                let replica_progress = progress.get(&replica.id)?;
                let leader_progress = leader_progress?;
                if replica_progress.durable_commit_sequence
                    >= leader_progress.durable_commit_sequence
                {
                    0
                } else {
                    now_ms.saturating_sub(replica_progress.last_applied_commit_timestamp_ms)
                }
            };
            if observed_staleness_ms > max_staleness_ms {
                return None;
            }
            Some(RouteTarget {
                node_id: node.id.clone(),
                address: node.address.clone(),
                replica_id: replica.id,
                leader,
                locality_matches: locality_matches(topology, requester, node),
                observed_staleness_ms,
            })
        })
        .collect::<Vec<_>>();
    match operation {
        PointOperation::Read {
            policy: PointReadPolicy::BoundedStaleness { .. },
        } => candidates.sort_by_key(|target| {
            (
                target.observed_staleness_ms,
                std::cmp::Reverse(target.locality_matches),
                !target.leader,
                target.node_id.clone(),
            )
        }),
        PointOperation::Read {
            policy: PointReadPolicy::NearestReplica { .. },
        } => candidates.sort_by_key(|target| {
            (
                std::cmp::Reverse(target.locality_matches),
                target.observed_staleness_ms,
                !target.leader,
                target.node_id.clone(),
            )
        }),
        _ => unreachable!("leader-only operations returned above"),
    }
    candidates
}

fn replica_staleness_ms(
    progress: &BTreeMap<ReplicaId, ReplicaRoutingProgress>,
    range: &RangeDescriptor,
    replica_progress: &ReplicaRoutingProgress,
    now_ms: u64,
) -> Option<u64> {
    let leader = range
        .replicas
        .iter()
        .find(|replica| replica.node_id == range.leader)?;
    let leader_progress = progress.get(&leader.id)?;
    Some(
        if replica_progress.durable_commit_sequence >= leader_progress.durable_commit_sequence {
            0
        } else {
            now_ms.saturating_sub(replica_progress.last_applied_commit_timestamp_ms)
        },
    )
}

fn route_node_is_live(
    node: &ClusterNode,
    now_ms: u64,
    config: &DistributionConfig,
    require_live: bool,
) -> bool {
    if node.lifecycle == ClusterNodeLifecycle::Decommissioned {
        return false;
    }
    match node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms) {
        ClusterNodeLiveness::Live => true,
        ClusterNodeLiveness::Suspect => !require_live,
        ClusterNodeLiveness::Dead | ClusterNodeLiveness::Decommissioned => false,
    }
}

fn leader_target(
    topology: &ClusterTopology,
    _progress: &BTreeMap<ReplicaId, ReplicaRoutingProgress>,
    range: &RangeDescriptor,
    requester: Option<&ClusterNodeId>,
    _now_ms: u64,
) -> Option<RouteTarget> {
    let replica = range.replicas.iter().find(|replica| {
        replica.node_id == range.leader && replica.role == RangeReplicaRole::Voter
    })?;
    let node = topology.nodes.get(&replica.node_id)?;
    Some(RouteTarget {
        node_id: node.id.clone(),
        address: node.address.clone(),
        replica_id: replica.id,
        leader: true,
        locality_matches: locality_matches(topology, requester, node),
        observed_staleness_ms: 0,
    })
}

fn locality_matches(
    topology: &ClusterTopology,
    requester: Option<&ClusterNodeId>,
    candidate: &ClusterNode,
) -> usize {
    requester
        .and_then(|requester| topology.nodes.get(requester))
        .map(|requester| {
            requester
                .labels
                .iter()
                .filter(|(key, value)| candidate.labels.get(*key) == Some(*value))
                .count()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{ClusterNode, DistributionStore, PlacementPolicy, RangeReplica};

    fn config() -> DistributionConfig {
        DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("cluster-routing").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "10.0.0.1:9444".to_string(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 10_000,
            replication_factor: 3,
            initial_ranges: 4,
            default_placement: PlacementPolicy::default(),
            suspect_after_ms: 1_000,
            dead_after_ms: 2_000,
            topology_history_limit: 64,
            metadata_election_timeout_ms: 1_500,
            metadata_heartbeat_interval_ms: 300,
            transport: Default::default(),
        }
    }

    fn node(id: &str, now_ms: u64) -> ClusterNode {
        ClusterNode::new(
            ClusterNodeId::new(id).unwrap(),
            format!("10.0.0.{}:9444", &id[1..]),
            1,
            10_000,
            now_ms,
        )
        .unwrap()
    }

    fn topology() -> (DistributionConfig, ClusterTopology, RangeId) {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let actor = config.node_id.clone();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 100).unwrap();
        store.join_node(node("n2", 100), &actor, 101).unwrap();
        store.join_node(node("n3", 100), &actor, 102).unwrap();
        let mut topology = store.topology().clone();
        topology.nodes.get_mut(&actor).unwrap().labels =
            BTreeMap::from([("zone".to_string(), "west-b".to_string())]);
        topology
            .nodes
            .get_mut(&ClusterNodeId::new("n2").unwrap())
            .unwrap()
            .labels = BTreeMap::from([("zone".to_string(), "west-a".to_string())]);
        topology
            .nodes
            .get_mut(&ClusterNodeId::new("n3").unwrap())
            .unwrap()
            .labels = BTreeMap::from([("zone".to_string(), "east-a".to_string())]);
        let range_id = topology.range_for_token(0).unwrap().id;
        let range = topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
            .unwrap();
        range.replicas = vec![
            RangeReplica {
                id: ReplicaId::new(1_000).unwrap(),
                node_id: ClusterNodeId::new("n1").unwrap(),
                role: RangeReplicaRole::Voter,
            },
            RangeReplica {
                id: ReplicaId::new(1_001).unwrap(),
                node_id: ClusterNodeId::new("n2").unwrap(),
                role: RangeReplicaRole::Voter,
            },
            RangeReplica {
                id: ReplicaId::new(1_002).unwrap(),
                node_id: ClusterNodeId::new("n3").unwrap(),
                role: RangeReplicaRole::Voter,
            },
        ];
        range.leader = ClusterNodeId::new("n1").unwrap();
        topology.next_replica_id = 2_000;
        topology.validate().unwrap();
        (config, topology, range_id)
    }

    fn key_in_range(topology: &ClusterTopology, range_id: RangeId) -> String {
        (0..100_000)
            .map(|number| format!("key-{number}"))
            .find(|key| {
                topology
                    .range_for_key("documents", key)
                    .is_ok_and(|range| range.id == range_id)
            })
            .unwrap()
    }

    #[test]
    fn routes_writes_to_leader_and_nearest_reads_to_fresh_local_replica() {
        let (config, topology, range_id) = topology();
        let key = key_in_range(&topology, range_id);
        let router = ClusterRequestRouter::new(config, topology).unwrap();
        router
            .update_replica_progress(ReplicaRoutingProgress {
                replica_id: ReplicaId::new(1_000).unwrap(),
                durable_commit_sequence: 51,
                last_applied_commit_timestamp_ms: 195,
                observed_at_ms: 195,
            })
            .unwrap();
        router
            .update_replica_progress(ReplicaRoutingProgress {
                replica_id: ReplicaId::new(1_001).unwrap(),
                durable_commit_sequence: 50,
                last_applied_commit_timestamp_ms: 190,
                observed_at_ms: 190,
            })
            .unwrap();
        router
            .update_replica_progress(ReplicaRoutingProgress {
                replica_id: ReplicaId::new(1_002).unwrap(),
                durable_commit_sequence: 49,
                last_applied_commit_timestamp_ms: 100,
                observed_at_ms: 100,
            })
            .unwrap();

        let RouteDecision::Routed { route: write } = router
            .route_point(
                "documents",
                &key,
                PointOperation::Write,
                Some(&ClusterNodeId::new("n2").unwrap()),
                200,
            )
            .unwrap()
        else {
            panic!("write did not route");
        };
        assert_eq!(write.primary.node_id, ClusterNodeId::new("n1").unwrap());
        assert!(write.primary.leader);
        assert!(write.fallbacks.is_empty());

        let RouteDecision::Routed { route: nearest } = router
            .route_point(
                "documents",
                &key,
                PointOperation::Read {
                    policy: PointReadPolicy::NearestReplica {
                        max_staleness_ms: 50,
                    },
                },
                Some(&ClusterNodeId::new("n2").unwrap()),
                200,
            )
            .unwrap()
        else {
            panic!("nearest read did not route");
        };
        assert_eq!(nearest.primary.node_id, ClusterNodeId::new("n2").unwrap());
        assert_eq!(nearest.primary.observed_staleness_ms, 10);
        assert!(nearest
            .fallbacks
            .iter()
            .any(|target| target.node_id == ClusterNodeId::new("n1").unwrap()));
    }

    #[test]
    fn owner_fences_stale_epoch_and_redirects_to_new_leader() {
        let (config, topology, range_id) = topology();
        let key = key_in_range(&topology, range_id);
        let router = ClusterRequestRouter::new(config, topology.clone()).unwrap();
        let RouteDecision::Routed { route } = router
            .route_point("documents", &key, PointOperation::Write, None, 200)
            .unwrap()
        else {
            panic!("write did not route");
        };

        let mut moved = topology;
        moved.generation += 1;
        let range = moved
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
            .unwrap();
        range.epoch += 1;
        range.leader = ClusterNodeId::new("n2").unwrap();
        moved.validate().unwrap();
        assert_eq!(
            router.install_topology(moved).unwrap(),
            TopologyInstall::Installed
        );

        let RouteValidation::Retry { retry } = router
            .validate_at_node(&ClusterNodeId::new("n1").unwrap(), &route.header, 210)
            .unwrap()
        else {
            panic!("stale request was accepted");
        };
        assert_eq!(retry.code, RouteRetryCode::StaleRangeEpoch);
        assert!(retry.refresh_topology);
        assert_eq!(
            retry.redirect.unwrap().node_id,
            ClusterNodeId::new("n2").unwrap()
        );
    }

    #[test]
    fn topology_cache_is_generation_monotonic_and_detects_split_brain() {
        let (config, topology, _) = topology();
        let router = ClusterRequestRouter::new(config, topology.clone()).unwrap();
        assert_eq!(
            router.install_topology(topology.clone()).unwrap(),
            TopologyInstall::Unchanged
        );
        let mut newer = topology.clone();
        newer.generation += 1;
        assert_eq!(
            router.install_topology(newer.clone()).unwrap(),
            TopologyInstall::Installed
        );
        assert_eq!(
            router.install_topology(topology).unwrap(),
            TopologyInstall::StaleIgnored
        );
        let mut conflict = newer;
        conflict
            .nodes
            .get_mut(&ClusterNodeId::new("n2").unwrap())
            .unwrap()
            .address = "10.9.9.9:9444".to_string();
        assert!(router
            .install_topology(conflict)
            .unwrap_err()
            .to_string()
            .contains("conflicting topology"));
    }

    #[test]
    fn unknown_or_too_stale_followers_are_never_selected() {
        let (config, topology, range_id) = topology();
        let key = key_in_range(&topology, range_id);
        let router = ClusterRequestRouter::new(config, topology).unwrap();
        let RouteDecision::Routed { route } = router
            .route_point(
                "documents",
                &key,
                PointOperation::Read {
                    policy: PointReadPolicy::NearestReplica {
                        max_staleness_ms: 5,
                    },
                },
                Some(&ClusterNodeId::new("n2").unwrap()),
                200,
            )
            .unwrap()
        else {
            panic!("leader fallback did not route");
        };
        assert!(route.primary.leader);
        assert!(route.fallbacks.is_empty());
    }
}
