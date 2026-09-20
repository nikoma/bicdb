use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::distribution::{ClusterNodeId, ClusterTopology};
use crate::distribution_routing::{
    ClusterRequestRouter, PointOperation, PointRoute, RouteDecision, RouteRetry, RouteTarget,
    RoutedRequestHeader, TopologyInstall,
};
use crate::error::{BicDbError, Result};

fn gateway_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub enum PointTransportReply<Response> {
    Success { response: Response },
    Retry { retry: RouteRetry },
}

/// One protocol-specific, reusable connection to a BicDB range endpoint.
pub trait ClusterPointConnection<Request, Response>: Send {
    fn execute(
        &mut self,
        header: &RoutedRequestHeader,
        request: &Request,
    ) -> Result<PointTransportReply<Response>>;
}

/// Creates pooled connections and refreshes routing topology after redirects.
///
/// Implementations may use BicDB's internal TLS RPC, an in-process endpoint,
/// or a test transport; routing and retry correctness is protocol-independent.
pub trait ClusterPointConnector<Request, Response>: Send + Sync + 'static {
    type Connection: ClusterPointConnection<Request, Response>;

    fn connect(&self, target: &RouteTarget) -> Result<Self::Connection>;

    fn refresh_topology(&self, minimum_generation: u64) -> Result<ClusterTopology>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterGatewayConfig {
    pub max_route_attempts: usize,
    pub max_idle_connections_per_node: usize,
}

impl Default for ClusterGatewayConfig {
    fn default() -> Self {
        Self {
            max_route_attempts: 3,
            max_idle_connections_per_node: 8,
        }
    }
}

impl ClusterGatewayConfig {
    pub fn validate(&self) -> Result<()> {
        if !(1..=16).contains(&self.max_route_attempts) {
            return Err(gateway_error(
                "cluster gateway max_route_attempts must be between 1 and 16",
            ));
        }
        if self.max_idle_connections_per_node > 1_024 {
            return Err(gateway_error(
                "cluster gateway max_idle_connections_per_node must not exceed 1024",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConnectionPoolStats {
    pub opened: u64,
    pub reused: u64,
    pub discarded: u64,
    pub idle: usize,
}

struct PoolCounters {
    opened: AtomicU64,
    reused: AtomicU64,
    discarded: AtomicU64,
}

impl Default for PoolCounters {
    fn default() -> Self {
        Self {
            opened: AtomicU64::new(0),
            reused: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
        }
    }
}

/// Bounded per-node connection pool used by gateway sessions.
pub struct BoundedClusterConnectionPool<Connector, Request, Response>
where
    Connector: ClusterPointConnector<Request, Response>,
{
    connector: Arc<Connector>,
    max_idle_per_node: usize,
    idle: Mutex<BTreeMap<ClusterNodeId, Vec<Connector::Connection>>>,
    counters: PoolCounters,
    marker: PhantomData<fn(Request) -> Response>,
}

impl<Connector, Request, Response> BoundedClusterConnectionPool<Connector, Request, Response>
where
    Connector: ClusterPointConnector<Request, Response>,
{
    pub fn new(connector: Arc<Connector>, max_idle_per_node: usize) -> Result<Self> {
        if max_idle_per_node > 1_024 {
            return Err(gateway_error(
                "cluster connection pool max idle per node must not exceed 1024",
            ));
        }
        Ok(Self {
            connector,
            max_idle_per_node,
            idle: Mutex::new(BTreeMap::new()),
            counters: PoolCounters::default(),
            marker: PhantomData,
        })
    }

    pub fn stats(&self) -> Result<ClusterConnectionPoolStats> {
        let idle = self
            .idle
            .lock()
            .map_err(|_| gateway_error("cluster connection pool lock is poisoned"))?
            .values()
            .map(Vec::len)
            .sum();
        Ok(ClusterConnectionPoolStats {
            opened: self.counters.opened.load(Ordering::Relaxed),
            reused: self.counters.reused.load(Ordering::Relaxed),
            discarded: self.counters.discarded.load(Ordering::Relaxed),
            idle,
        })
    }

    pub fn invalidate_node(&self, node_id: &ClusterNodeId) -> Result<usize> {
        let removed = self
            .idle
            .lock()
            .map_err(|_| gateway_error("cluster connection pool lock is poisoned"))?
            .remove(node_id)
            .map(|connections| connections.len())
            .unwrap_or(0);
        self.counters
            .discarded
            .fetch_add(removed as u64, Ordering::Relaxed);
        Ok(removed)
    }

    pub fn invalidate_all(&self) -> Result<usize> {
        let mut idle = self
            .idle
            .lock()
            .map_err(|_| gateway_error("cluster connection pool lock is poisoned"))?;
        let removed = idle.values().map(Vec::len).sum::<usize>();
        idle.clear();
        self.counters
            .discarded
            .fetch_add(removed as u64, Ordering::Relaxed);
        Ok(removed)
    }

    fn execute(
        &self,
        target: &RouteTarget,
        header: &RoutedRequestHeader,
        request: &Request,
    ) -> Result<PointTransportReply<Response>> {
        let mut connection = {
            let mut idle = self
                .idle
                .lock()
                .map_err(|_| gateway_error("cluster connection pool lock is poisoned"))?;
            idle.get_mut(&target.node_id).and_then(Vec::pop)
        };
        if connection.is_some() {
            self.counters.reused.fetch_add(1, Ordering::Relaxed);
        } else {
            connection = Some(self.connector.connect(target)?);
            self.counters.opened.fetch_add(1, Ordering::Relaxed);
        }
        let mut connection = connection.expect("connection opened or reused");
        let reply = connection.execute(header, request);
        match reply {
            Ok(reply) => {
                if self.max_idle_per_node > 0 {
                    let mut idle = self
                        .idle
                        .lock()
                        .map_err(|_| gateway_error("cluster connection pool lock is poisoned"))?;
                    let node_idle = idle.entry(target.node_id.clone()).or_default();
                    if node_idle.len() < self.max_idle_per_node {
                        node_idle.push(connection);
                    } else {
                        self.counters.discarded.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    self.counters.discarded.fetch_add(1, Ordering::Relaxed);
                }
                Ok(reply)
            }
            Err(error) => {
                self.counters.discarded.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }
}

/// Point-operation gateway used by server sessions. It routes, checks out a
/// pooled connection, handles structured redirects, refreshes topology, and
/// retries within an explicit bound.
pub struct ClusterPointGateway<Connector, Request, Response>
where
    Connector: ClusterPointConnector<Request, Response>,
{
    router: ClusterRequestRouter,
    connector: Arc<Connector>,
    pool: BoundedClusterConnectionPool<Connector, Request, Response>,
    config: ClusterGatewayConfig,
}

impl<Connector, Request, Response> ClusterPointGateway<Connector, Request, Response>
where
    Connector: ClusterPointConnector<Request, Response>,
{
    pub fn new(
        router: ClusterRequestRouter,
        connector: Arc<Connector>,
        config: ClusterGatewayConfig,
    ) -> Result<Self> {
        config.validate()?;
        let pool = BoundedClusterConnectionPool::new(
            Arc::clone(&connector),
            config.max_idle_connections_per_node,
        )?;
        Ok(Self {
            router,
            connector,
            pool,
            config,
        })
    }

    pub fn router(&self) -> &ClusterRequestRouter {
        &self.router
    }

    pub fn pool_stats(&self) -> Result<ClusterConnectionPoolStats> {
        self.pool.stats()
    }

    pub fn execute(
        &self,
        namespace: &str,
        key: &str,
        operation: PointOperation,
        requester: Option<&ClusterNodeId>,
        now_ms: u64,
        request: &Request,
    ) -> Result<Response> {
        let mut last_retry = None;
        let mut last_transport_error = None;
        for _ in 0..self.config.max_route_attempts {
            let decision = self
                .router
                .route_point(namespace, key, operation, requester, now_ms)?;
            let route = match decision {
                RouteDecision::Routed { route } => route,
                RouteDecision::Retry { retry } => {
                    return Err(gateway_error(format!(
                        "cluster point request cannot be routed: {retry:?}"
                    )));
                }
            };
            match self.execute_route(&route, operation, request) {
                Ok(PointTransportReply::Success { response }) => return Ok(response),
                Ok(PointTransportReply::Retry { retry }) => {
                    if retry.refresh_topology {
                        let minimum = retry
                            .topology_generation
                            .max(self.router.topology_generation()?.saturating_add(1));
                        let topology = self.connector.refresh_topology(minimum)?;
                        match self.router.install_topology(topology)? {
                            TopologyInstall::Installed => {
                                // Node addresses and certificates may have
                                // changed with the catalog generation.
                                self.pool.invalidate_all()?;
                            }
                            TopologyInstall::Unchanged => {}
                            TopologyInstall::StaleIgnored => {
                                return Err(gateway_error(format!(
                                    "topology refresh did not reach required generation {minimum}"
                                )));
                            }
                        }
                    }
                    last_retry = Some(retry);
                }
                Err(error) => {
                    last_transport_error = Some(error.to_string());
                    // A fresh catalog may have moved the leader while the old
                    // address was unreachable. Refresh before the next route.
                    let minimum = self.router.topology_generation()?.saturating_add(1);
                    if let Ok(topology) = self.connector.refresh_topology(minimum) {
                        let _ = self.router.install_topology(topology)?;
                    }
                }
            }
        }
        Err(gateway_error(format!(
            "cluster point request exhausted {} route attempts; last_retry={last_retry:?}; last_transport_error={last_transport_error:?}",
            self.config.max_route_attempts
        )))
    }

    fn execute_route(
        &self,
        route: &PointRoute,
        operation: PointOperation,
        request: &Request,
    ) -> Result<PointTransportReply<Response>> {
        let mut targets = Vec::with_capacity(1 + route.fallbacks.len());
        targets.push(&route.primary);
        if !matches!(operation, PointOperation::Write) {
            targets.extend(route.fallbacks.iter());
        }
        let mut last_error = None;
        for target in targets {
            match self.pool.execute(target, &route.header, request) {
                Ok(reply @ PointTransportReply::Success { .. })
                | Ok(reply @ PointTransportReply::Retry { .. }) => return Ok(reply),
                Err(error) => {
                    last_error = Some(error);
                    self.pool.invalidate_node(&target.node_id)?;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| gateway_error("route has no connection targets")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, VecDeque};

    use crate::distribution::{
        ClusterId, ClusterNode, ClusterNodeLifecycle, DistributionConfig, PlacementPolicy,
        RangeDescriptor, RangeId, RangeReplica, RangeReplicaRole, ReplicaId, TopologyChange,
        DISTRIBUTION_FORMAT_VERSION, DISTRIBUTION_HASH_VERSION,
    };
    use crate::{PointReadPolicy, ReplicaRoutingProgress, RouteRetryCode};

    fn config() -> DistributionConfig {
        DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("gateway-cluster").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "10.0.0.1:9444".to_string(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 10_000,
            replication_factor: 2,
            initial_ranges: 1,
            default_placement: PlacementPolicy::default(),
            suspect_after_ms: 1_000,
            dead_after_ms: 2_000,
            topology_history_limit: 64,
            metadata_election_timeout_ms: 1_500,
            metadata_heartbeat_interval_ms: 300,
            transport: Default::default(),
        }
    }

    fn topology(generation: u64, leader: &str) -> ClusterTopology {
        let n1 = ClusterNodeId::new("n1").unwrap();
        let n2 = ClusterNodeId::new("n2").unwrap();
        let mut node1 = ClusterNode::new(n1.clone(), "10.0.0.1:9444", 1, 10_000, 100).unwrap();
        let mut node2 = ClusterNode::new(n2.clone(), "10.0.0.2:9444", 1, 10_000, 100).unwrap();
        node1.lifecycle = ClusterNodeLifecycle::Active;
        node2.lifecycle = ClusterNodeLifecycle::Active;
        let topology = ClusterTopology {
            format_version: DISTRIBUTION_FORMAT_VERSION,
            hash_version: DISTRIBUTION_HASH_VERSION,
            cluster_id: ClusterId::new("gateway-cluster").unwrap(),
            generation,
            replication_factor: 2,
            nodes: BTreeMap::from([(n1.clone(), node1), (n2.clone(), node2)]),
            ranges: BTreeMap::from([(
                0,
                RangeDescriptor {
                    id: RangeId::new(1).unwrap(),
                    start_token: 0,
                    end_token: None,
                    epoch: generation,
                    replicas: vec![
                        RangeReplica {
                            id: ReplicaId::new(1).unwrap(),
                            node_id: n1.clone(),
                            role: RangeReplicaRole::Voter,
                        },
                        RangeReplica {
                            id: ReplicaId::new(2).unwrap(),
                            node_id: n2.clone(),
                            role: RangeReplicaRole::Voter,
                        },
                    ],
                    leader: ClusterNodeId::new(leader).unwrap(),
                    approximate_bytes: 10,
                    approximate_qps: 1,
                    placement: PlacementPolicy::default(),
                },
            )]),
            next_range_id: 2,
            next_replica_id: 3,
            next_relocation_id: 1,
            relocations: BTreeMap::new(),
            applied_rebalance_plans: VecDeque::new(),
            history: VecDeque::from([TopologyChange {
                generation,
                at_ms: 100,
                actor: n1,
                summary: "test topology".to_string(),
            }]),
        };
        topology.validate().unwrap();
        topology
    }

    #[derive(Default)]
    struct FakeState {
        topology: Mutex<Option<ClusterTopology>>,
        fail_nodes: Mutex<BTreeSet<ClusterNodeId>>,
        redirect_n1: AtomicU64,
    }

    struct FakeConnection {
        node_id: ClusterNodeId,
        state: Arc<FakeState>,
    }

    impl ClusterPointConnection<String, String> for FakeConnection {
        fn execute(
            &mut self,
            header: &RoutedRequestHeader,
            request: &String,
        ) -> Result<PointTransportReply<String>> {
            if self
                .state
                .fail_nodes
                .lock()
                .unwrap()
                .contains(&self.node_id)
            {
                return Err(gateway_error(format!("{} unavailable", self.node_id)));
            }
            if self.node_id == ClusterNodeId::new("n1").unwrap()
                && self.state.redirect_n1.load(Ordering::SeqCst) > 0
            {
                self.state.redirect_n1.fetch_sub(1, Ordering::SeqCst);
                return Ok(PointTransportReply::Retry {
                    retry: RouteRetry {
                        code: RouteRetryCode::NotLeader,
                        message: "leader moved".to_string(),
                        refresh_topology: true,
                        topology_generation: 2,
                        range_id: Some(header.range_id),
                        current_range_epoch: Some(2),
                        redirect: Some(RouteTarget {
                            node_id: ClusterNodeId::new("n2").unwrap(),
                            address: "10.0.0.2:9444".to_string(),
                            replica_id: ReplicaId::new(2).unwrap(),
                            leader: true,
                            locality_matches: 0,
                            observed_staleness_ms: 0,
                        }),
                    },
                });
            }
            Ok(PointTransportReply::Success {
                response: format!("{}:{request}", self.node_id),
            })
        }
    }

    struct FakeConnector {
        state: Arc<FakeState>,
    }

    impl ClusterPointConnector<String, String> for FakeConnector {
        type Connection = FakeConnection;

        fn connect(&self, target: &RouteTarget) -> Result<Self::Connection> {
            Ok(FakeConnection {
                node_id: target.node_id.clone(),
                state: Arc::clone(&self.state),
            })
        }

        fn refresh_topology(&self, minimum_generation: u64) -> Result<ClusterTopology> {
            let topology = self.state.topology.lock().unwrap().clone().unwrap();
            if topology.generation < minimum_generation {
                return Err(gateway_error(format!(
                    "topology {} is below requested {minimum_generation}",
                    topology.generation
                )));
            }
            Ok(topology)
        }
    }

    #[test]
    fn structured_redirect_refreshes_topology_and_reuses_new_leader_connection() {
        let state = Arc::new(FakeState {
            topology: Mutex::new(Some(topology(2, "n2"))),
            fail_nodes: Mutex::new(BTreeSet::new()),
            redirect_n1: AtomicU64::new(1),
        });
        let connector = Arc::new(FakeConnector {
            state: Arc::clone(&state),
        });
        let router = ClusterRequestRouter::new(config(), topology(1, "n1")).unwrap();
        let gateway = ClusterPointGateway::new(
            router,
            connector,
            ClusterGatewayConfig {
                max_route_attempts: 3,
                max_idle_connections_per_node: 2,
            },
        )
        .unwrap();

        let first = gateway
            .execute(
                "documents",
                "doc-1",
                PointOperation::Write,
                None,
                150,
                &"put".to_string(),
            )
            .unwrap();
        assert_eq!(first, "n2:put");
        assert_eq!(gateway.router().topology_generation().unwrap(), 2);
        let second = gateway
            .execute(
                "documents",
                "doc-2",
                PointOperation::Write,
                None,
                151,
                &"put2".to_string(),
            )
            .unwrap();
        assert_eq!(second, "n2:put2");
        let stats = gateway.pool_stats().unwrap();
        assert_eq!(stats.opened, 2);
        assert_eq!(stats.reused, 1);
        assert_eq!(stats.discarded, 1);
        assert_eq!(stats.idle, 1);
    }

    #[test]
    fn read_transport_failure_uses_bounded_fallback_without_write_failover() {
        let state = Arc::new(FakeState {
            topology: Mutex::new(Some(topology(1, "n1"))),
            fail_nodes: Mutex::new(BTreeSet::from([ClusterNodeId::new("n1").unwrap()])),
            redirect_n1: AtomicU64::new(0),
        });
        let connector = Arc::new(FakeConnector {
            state: Arc::clone(&state),
        });
        let router = ClusterRequestRouter::new(config(), topology(1, "n1")).unwrap();
        router
            .update_replica_progress(ReplicaRoutingProgress {
                replica_id: ReplicaId::new(1).unwrap(),
                durable_commit_sequence: 10,
                last_applied_commit_timestamp_ms: 149,
                observed_at_ms: 149,
            })
            .unwrap();
        router
            .update_replica_progress(ReplicaRoutingProgress {
                replica_id: ReplicaId::new(2).unwrap(),
                durable_commit_sequence: 10,
                last_applied_commit_timestamp_ms: 149,
                observed_at_ms: 149,
            })
            .unwrap();
        let gateway = ClusterPointGateway::new(
            router,
            connector,
            ClusterGatewayConfig {
                max_route_attempts: 1,
                max_idle_connections_per_node: 1,
            },
        )
        .unwrap();
        let response = gateway
            .execute(
                "documents",
                "doc-1",
                PointOperation::Read {
                    policy: PointReadPolicy::BoundedStaleness {
                        max_staleness_ms: 10,
                    },
                },
                None,
                150,
                &"get".to_string(),
            )
            .unwrap();
        assert_eq!(response, "n2:get");
        assert_eq!(gateway.pool_stats().unwrap().discarded, 1);
    }
}
