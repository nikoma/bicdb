//! Fresh born-v2 cluster only: actual host metadata and all installed writer
//! policies precede the FIRST application ACK. The consuming read facade is
//! internal; this does not qualify a public endpoint, operator or physical HA.
use super::*;
use crate::db::InstalledNativeWriterHostGuard;
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_native_read::{
    NativeStrongPointReadOutcome, NativeStrongPointReadRequest, NativeStrongPointReadValue,
    NativeStrongReadStage,
};
use crate::distribution_range_consensus::RangeWriteCoordinator;

type Coordinator = RangeWriteCoordinator<TcpClusterRelocationTransport>;
struct EnforcedNetwork {
    native: ActualNativeRf3Fixture,
    clients: BTreeMap<ClusterNodeId, Arc<TcpClusterRelocationTransport>>,
    metadata: BTreeMap<ClusterNodeId, Arc<Mutex<MetadataConsensusStore>>>,
    providers: BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    runtimes: BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    coordinators: BTreeMap<ClusterNodeId, Arc<Coordinator>>,
    hosts: BTreeMap<ClusterNodeId, Arc<InstalledNativeWriterHostGuard>>,
    servers: Vec<ClusterDataServerHandle>,
    _keys: tempfile::TempDir,
}
impl EnforcedNetwork {
    fn new() -> Self {
        let keys = tempfile::tempdir().unwrap();
        let ids = identities(keys.path());
        let sockets = ["n0", "n1", "n2"]
            .into_iter()
            .map(|name| (node(name), TcpListener::bind("127.0.0.1:0").unwrap()))
            .collect::<BTreeMap<_, _>>();
        let network = sockets
            .iter()
            .map(|(id, socket)| {
                (
                    id.clone(),
                    (socket.local_addr().unwrap().to_string(), ids[id].pin()),
                )
            })
            .collect();
        // This constructor does NOT recover or install a generic hook and
        // does NOT write an application row. Legacy fixtures stay unchanged.
        let native = ActualNativeRf3Fixture::new_with_enforced_network_and_ranges(&network, 2);
        let topology = native.topology.read().clone();
        for id in topology.nodes.keys() {
            assert_eq!(
                native.databases[id]
                    .read()
                    .actual_native_writer_enforcement_version()
                    .unwrap(),
                1
            );
            assert!(!native.databases[id].read().commit_admission_ready());
            assert_eq!(
                topology.nodes[id]
                    .native_store_birth_v1
                    .as_ref()
                    .unwrap()
                    .writer_enforcement_version(),
                1
            );
        }
        let config = |id: &ClusterNodeId| ClusterNetworkTransportConfig {
            connect_timeout_ms: 50,
            io_timeout_ms: 4000,
            max_frame_bytes: 4 * 1024 * 1024,
            max_inbound_connections: 16,
            dev_localhost_plaintext: false,
            tls: Some(ids[id].tls()),
        };
        let clients = topology
            .nodes
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    Arc::new(
                        TcpClusterRelocationTransport::new(
                            topology.cluster_id.clone(),
                            id.clone(),
                            Arc::clone(&native.topology),
                            config(id),
                        )
                        .unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let open = |id: &ClusterNodeId| {
            MetadataConsensusStore::open(
                keys.path().join(format!("strong-meta-{id}")),
                topology.cluster_id.clone(),
                id.clone(),
                topology.clone(),
                true,
            )
            .unwrap()
        };
        // Declared fixture election, actual persisted read-index probes; the
        // metadata leader n1 is distinct from the data leader n0.
        let mut leader = open(&node("n1"));
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([node("n0"), node("n1")]))
            .unwrap();
        let mut metadata = BTreeMap::new();
        for id in [node("n0"), node("n2")] {
            let mut follower = open(&id);
            let response = follower
                .handle_append_request(leader.append_request_from(1, 16).unwrap())
                .unwrap();
            leader.record_append_response(&id, &response).unwrap();
            metadata.insert(id, Arc::new(Mutex::new(follower)));
        }
        metadata.insert(node("n1"), Arc::new(Mutex::new(leader)));
        let providers = metadata
            .iter()
            .map(|(id, store)| {
                (
                    id.clone(),
                    Arc::new(
                        MetadataReadProvider::new(Arc::clone(store), clients[id].clone()).unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let runtimes = topology
            .nodes
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    Arc::new(
                        RangeRecoveryRuntime::new(
                            Arc::clone(&native.services[id]),
                            Arc::clone(&providers[id]),
                        )
                        .with_participant_transport(clients[id].clone()),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        drop(sockets);
        let servers = topology
            .nodes
            .keys()
            .map(|id| {
                start_cluster_data_server_with_recovery_runtime(
                    &topology.nodes[id].address,
                    config(id),
                    Arc::clone(&native.topology),
                    Arc::clone(&native.services[id]),
                    None,
                    Some(Arc::clone(&metadata[id])),
                    None,
                    Some(Arc::clone(&providers[id])),
                    Some(Arc::clone(&runtimes[id])),
                )
                .unwrap()
            })
            .collect();
        let coordinators = topology
            .nodes
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    Arc::new(
                        RangeWriteCoordinator::new_schema_fenced(
                            topology.cluster_id.clone(),
                            id.clone(),
                            Arc::clone(&native.topology),
                            Arc::clone(&native.services[id]),
                            Arc::clone(&clients[id]),
                        )
                        .unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut hosts = BTreeMap::new();
        for id in topology.nodes.keys() {
            let business = Uuid::now_v7();
            let observation = clients[id]
                .acquire_host_metadata_observation(
                    &node("n1"),
                    business,
                    Duration::from_secs(5),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            let policy = {
                let service = native.services[id].lock();
                let db = native.databases[id].read();
                service
                    .prepare_native_host_writer_enrollment(&db, observation, business)
                    .unwrap()
            };
            let host = native.databases[id]
                .read()
                .install_enrolled_native_coordinator(Arc::clone(&coordinators[id]), policy)
                .unwrap();
            hosts.insert(id.clone(), Arc::new(host));
        }
        // ALL current voter policies now exist. Only now may the installed
        // actual TCP coordinator establish its ordinary recovery prerequisite.
        coordinators[&node("n0")]
            .recover_local_leadership(8)
            .unwrap();
        Self {
            native,
            clients,
            metadata,
            providers,
            runtimes,
            coordinators,
            hosts,
            servers,
            _keys: keys,
        }
    }
    fn key(&self, range: RangeId, prefix: &str) -> String {
        (0..100_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|key| {
                self.native
                    .topology
                    .read()
                    .range_for_key("items", key)
                    .unwrap()
                    .id
                    == range
            })
            .unwrap()
    }
    fn ranges(&self) -> Vec<RangeDescriptor> {
        self.native
            .topology
            .read()
            .ranges
            .values()
            .cloned()
            .collect()
    }
    fn seed(&self) -> (String, RangeDescriptor) {
        let ranges = self.ranges();
        self.native
            .append(&self.key(ranges[1].id, "other-a"))
            .unwrap();
        self.native
            .append(&self.key(ranges[1].id, "other-b"))
            .unwrap();
        let selected = self.key(ranges[0].id, "selected-ack");
        self.native.append(&selected).unwrap();
        assert!(self.native.databases[&node("n0")]
            .read()
            .get("items", &selected)
            .unwrap()
            .is_some());
        (selected, ranges[0].clone())
    }
    fn first(
        &self,
        business: Uuid,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> MetadataReadObservation {
        self.clients[&node("n0")]
            .acquire_metadata_read_observation(
                &node("n1"),
                business,
                range.id,
                range.epoch,
                Duration::from_secs(5),
                parent,
            )
            .unwrap()
    }
    fn request(&self, key: &str, range: &RangeDescriptor) -> NativeStrongPointReadRequest {
        NativeStrongPointReadRequest {
            business_request_id: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            range_id: range.id,
            range_epoch: range.epoch,
            collection: "items".into(),
            id: key.into(),
            budget_ms: 5000,
        }
    }
    fn read(
        &self,
        request: NativeStrongPointReadRequest,
        parent: &CancellationToken,
    ) -> NativeStrongPointReadOutcome {
        let range = self
            .native
            .topology
            .read()
            .range_by_id(request.range_id)
            .unwrap()
            .clone();
        let first = self.first(request.business_request_id, &range, parent);
        self.coordinators[&node("n0")].strong_point_read(
            &self.runtimes[&node("n0")],
            Some(Arc::clone(&self.hosts[&node("n0")])),
            Arc::clone(&self.clients[&node("n0")]),
            first,
            request,
            parent,
        )
    }
    fn idle(&self) {
        let until = Instant::now() + Duration::from_secs(5);
        while self
            .runtimes
            .values()
            .any(|r| r.test_active_context_count() != 0)
        {
            assert!(
                Instant::now() < until,
                "actual Strong workers did not drain"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn change_committed_leader(&self, range: RangeId) {
        let mut topology = self.native.topology.read().clone();
        topology.generation += 1;
        let changed = topology
            .ranges
            .values_mut()
            .find(|r| r.id == range)
            .unwrap();
        changed.epoch += 1;
        changed.leader = node("n2");
        let mut leader = self.metadata[&node("n1")].lock();
        let entry = leader.propose_topology(topology.clone()).unwrap();
        for id in [node("n0"), node("n2")] {
            let response = self.metadata[&id]
                .lock()
                .handle_append_request(leader.append_request_from(entry.index, 16).unwrap())
                .unwrap();
            leader.record_append_response(&id, &response).unwrap();
        }
        *self.native.topology.write() = topology;
    }
}
impl Drop for EnforcedNetwork {
    fn drop(&mut self) {
        while let Some(server) = self.servers.pop() {
            server.join().unwrap();
        }
    }
}
fn observed(outcome: NativeStrongPointReadOutcome) -> NativeStrongPointReadValue {
    match outcome {
        NativeStrongPointReadOutcome::Observed(value) => value,
        NativeStrongPointReadOutcome::Unavailable(error) => panic!(
            "actual read unavailable at {:?}: {}",
            error.stage, error.detail
        ),
    }
}
fn unavailable(
    outcome: NativeStrongPointReadOutcome,
) -> crate::distribution_native_read::NativeStrongPointReadUnavailable {
    match outcome {
        NativeStrongPointReadOutcome::Unavailable(error) => error,
        NativeStrongPointReadOutcome::Observed(value) => {
            panic!("unexpected successful row/absence: {:?}", value.audit())
        }
    }
}

#[test]
fn native_strong_point_fresh_born2_actual_tls_ack_value_and_absence_with_local_b() {
    let fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    let parent = CancellationToken::uncancelable();
    let value = observed(fixture.read(fixture.request(&key, &range), &parent));
    assert_eq!(value.record().unwrap().id, key);
    assert_eq!(value.audit().range_index, 1);
    assert!(value.audit().target_local_visibility_db_sequence >= 3);
    assert!(value.audit().snapshot_sequence >= value.audit().target_local_visibility_db_sequence);
    assert_ne!(
        value.audit().first_metadata_context_id,
        value.audit().final_metadata_context_id
    );
    let absent = fixture.key(range.id, "never-written");
    let absence = observed(fixture.read(fixture.request(&absent, &range), &parent));
    assert!(absence.record().is_none());
    assert_eq!(absence.audit().range_index, 1);
    fixture.idle();
}

#[test]
fn native_strong_point_stale_route_and_missing_or_foreign_installed_hook_refuse() {
    let fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    let parent = CancellationToken::uncancelable();
    let mut stale = fixture.request(&key, &range);
    stale.range_epoch += 1;
    assert_eq!(
        unavailable(fixture.read(stale, &parent)).stage,
        NativeStrongReadStage::Route
    );
    fixture.idle();
    let request = fixture.request(&key, &range);
    let first = fixture.first(request.business_request_id, &range, &parent);
    let foreign = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            fixture.native.topology.read().cluster_id.clone(),
            node("n0"),
            Arc::clone(&fixture.native.topology),
            Arc::clone(&fixture.native.services[&node("n0")]),
            Arc::clone(&fixture.clients[&node("n0")]),
        )
        .unwrap(),
    );
    let result = foreign.strong_point_read(
        &fixture.runtimes[&node("n0")],
        Some(Arc::clone(&fixture.hosts[&node("n0")])),
        Arc::clone(&fixture.clients[&node("n0")]),
        first,
        request,
        &parent,
    );
    assert_eq!(
        unavailable(result).stage,
        NativeStrongReadStage::WriterAdmission
    );
    fixture.idle();
    // A genuine enrolled guard for n1's different physical native birth/open
    // cannot be transplanted into n0's read; no caller-selected identity is a seal.
    for host in [None, Some(Arc::clone(&fixture.hosts[&node("n1")]))] {
        let request = fixture.request(&key, &range);
        let first = fixture.first(request.business_request_id, &range, &parent);
        let result = fixture.coordinators[&node("n0")].strong_point_read(
            &fixture.runtimes[&node("n0")],
            host,
            Arc::clone(&fixture.clients[&node("n0")]),
            first,
            request,
            &parent,
        );
        assert_eq!(
            unavailable(result).stage,
            NativeStrongReadStage::WriterAdmission
        );
        fixture.idle();
    }
    // Even reinstalling the SAME Arc through the legacy hook changes the real
    // registration generation. The old enrolled guard must not bless it.
    fixture.native.databases[&node("n0")]
        .read()
        .install_commit_admission(fixture.coordinators[&node("n0")].clone());
    let result = fixture.read(fixture.request(&key, &range), &parent);
    assert_eq!(
        unavailable(result).stage,
        NativeStrongReadStage::WriterAdmission
    );
    fixture.idle();
}

#[test]
fn native_strong_point_raw_cq_or_pending_native_cannot_return_old_row_or_absence() {
    let fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    BicDb::test_reject_next_native_after_admission();
    let unresolved = fixture.key(range.id, "unknown-cq");
    let error = fixture.native.append(&unresolved).unwrap_err();
    assert!(
        error.to_string().contains("unknown") || error.to_string().contains("Unknown"),
        "{error}"
    );
    let error = unavailable(fixture.read(
        fixture.request(&key, &range),
        &CancellationToken::uncancelable(),
    ));
    assert!(matches!(
        error.stage,
        NativeStrongReadStage::DataQuorum | NativeStrongReadStage::Snapshot
    ));
    fixture.idle();
    assert!(fixture.native.databases[&node("n0")]
        .read()
        .get("items", &key)
        .unwrap()
        .is_some());
}

#[test]
fn native_strong_point_reservation_deadline_and_native_writer_do_not_invert_stamps() {
    let fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let held = fixture.coordinators[&node("n0")]
        .reserve_native_point_read(range.id, &parent)
        .unwrap();
    let mut request = fixture.request(&key, &range);
    request.budget_ms = 80;
    let start = Instant::now();
    let error = unavailable(fixture.read(request, &parent));
    assert!(matches!(
        error.stage,
        NativeStrongReadStage::Reservation | NativeStrongReadStage::Canceled
    ));
    assert!(start.elapsed() < Duration::from_secs(2));
    fixture.idle();
    let business = Uuid::now_v7();
    let round = crate::distribution_native_read::NativeReadQuorumRound::begin(
        fixture.first(business, &range, &parent),
        business,
        Uuid::now_v7(),
        &node("n0"),
        &parent,
    )
    .unwrap();
    let local = fixture.native.services[&node("n0")]
        .lock()
        .observe_current_native_range(&round.request().observation, &parent)
        .unwrap();
    // Outer DB.write may be owned by native origin. Its actual logical-range
    // admission MUST fail before a mutation permit, never wait on this reader.
    let next = fixture.key(range.id, "blocked-before-prepare");
    let error = fixture.native.append(&next).unwrap_err();
    assert!(
        !error.to_string().contains("unknown") && !error.to_string().contains("Unknown"),
        "{error}"
    );
    assert_eq!(
        fixture.native.databases[&node("n0")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        1
    );
    // This EXISTING actual proof must remain current: checking only a new
    // native R would miss a failed writer that rotated the reader stamp first.
    local
        .require_current(&fixture.native.services[&node("n0")].lock())
        .unwrap();
    drop(held);
    assert!(
        observed(fixture.read(fixture.request(&key, &range), &parent))
            .record()
            .is_some()
    );
    fixture.idle();
}

#[test]
fn native_strong_point_canceled_actual_local_io_retains_owned_context_until_drain() {
    let fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    let barrier = fixture.runtimes[&node("n0")].test_hold_native_read_io();
    let request = fixture.request(&key, &range);
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let first = fixture.first(request.business_request_id, &range, &parent);
    let (coordinator, runtime, host, client) = (
        Arc::clone(&fixture.coordinators[&node("n0")]),
        Arc::clone(&fixture.runtimes[&node("n0")]),
        Arc::clone(&fixture.hosts[&node("n0")]),
        Arc::clone(&fixture.clients[&node("n0")]),
    );
    let worker_parent = parent.clone();
    let worker = std::thread::spawn(move || {
        unavailable(coordinator.strong_point_read(
            &runtime,
            Some(host),
            client,
            first,
            request,
            &worker_parent,
        ))
        .stage
    });
    let until = Instant::now() + Duration::from_secs(3);
    while !barrier.entered() {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(fixture.native.services[&node("n0")].try_lock().is_some());
    assert!(fixture.native.databases[&node("n0")].try_write().is_some());
    parent.cancel();
    assert!(matches!(
        worker.join().unwrap(),
        NativeStrongReadStage::Canceled | NativeStrongReadStage::DataQuorum
    ));
    assert!(fixture.runtimes[&node("n0")].test_active_context_count() > 0);
    barrier.release();
    fixture.idle();
}

#[test]
fn native_strong_point_actual_final_metadata_leader_change_refuses_owned_snapshot() {
    let fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    let barrier = fixture.runtimes[&node("n0")].test_hold_point_after_actual_snapshot();
    let request = fixture.request(&key, &range);
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let first = fixture.first(request.business_request_id, &range, &parent);
    let (coordinator, runtime, host, client) = (
        Arc::clone(&fixture.coordinators[&node("n0")]),
        Arc::clone(&fixture.runtimes[&node("n0")]),
        Arc::clone(&fixture.hosts[&node("n0")]),
        Arc::clone(&fixture.clients[&node("n0")]),
    );
    let worker = std::thread::spawn(move || {
        unavailable(coordinator.strong_point_read(
            &runtime,
            Some(host),
            client,
            first,
            request,
            &parent,
        ))
        .stage
    });
    let until = Instant::now() + Duration::from_secs(3);
    while !barrier.entered() {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(fixture.native.services[&node("n0")].try_lock().is_some());
    assert!(fixture.native.databases[&node("n0")].try_write().is_some());
    fixture.change_committed_leader(range.id);
    barrier.release();
    assert!(matches!(
        worker.join().unwrap(),
        NativeStrongReadStage::FinalMetadata | NativeStrongReadStage::FinalCurrentness
    ));
    fixture.idle();
}

#[test]
fn native_strong_point_born2_legacy_event_get_never_falls_back_to_empty_database() {
    let fixture = EnforcedNetwork::new();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(2));
    let topology = fixture.native.topology.read();
    let range = topology
        .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, "tenant/missing")
        .unwrap()
        .clone();
    drop(topology);
    for destination in [node("n0"), node("n2")] {
        let result = fixture.clients[&node("n0")].application_event_cancellable(
            &parent,
            &destination,
            crate::ClusterEventRequest {
                range_id: range.id,
                range_epoch: range.epoch,
                operation: crate::ClusterEventOperation::Get {
                    namespace: "tenant".into(),
                    key: "missing".into(),
                },
            },
        );
        let error = result.expect_err("born-v2 Get must not become a bare successful absence");
        assert!(
            error.to_string().contains("strong_point_read_unavailable"),
            "{error}"
        );
    }
    fixture.idle();
    // This tests the explicit refusal until the separate host dispatcher is
    // installed; it makes no typed routing or incoming Strong endpoint claim.
}

#[test]
fn native_strong_point_policy_native_wal_publication_failure_fences_old_guard_and_prepare() {
    let mut fixture = EnforcedNetwork::new();
    let (key, range) = fixture.seed();
    let command = fixture.native.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n0"), 1)
        .unwrap()
        .entry
        .unwrap()
        .command;
    let business = Uuid::now_v7();
    let observation = fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(
            &node("n1"),
            business,
            Duration::from_secs(5),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let policy = {
        let service = fixture.native.services[&node("n0")].lock();
        let db = fixture.native.databases[&node("n0")].read();
        service
            .prepare_native_host_writer_enrollment(&db, observation, business)
            .unwrap()
    };
    {
        let db = fixture.native.databases[&node("n0")].read();
        db.test_fail_next_native_host_policy_publication();
        let error = db
            .install_enrolled_native_coordinator(fixture.coordinators[&node("n0")].clone(), policy)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("after actual native WAL publication"),
            "{error}"
        );
        assert!(fixture.hosts[&node("n0")]
            .require_current(&db, &range)
            .is_err());
    }
    let error = fixture.native.services[&node("n0")]
        .lock()
        .prepare_range_write(&range, &node("n0"), command)
        .unwrap_err();
    assert!(
        error.to_string().contains("writer") || error.to_string().contains("policy"),
        "{error}"
    );
    assert_eq!(
        unavailable(fixture.read(
            fixture.request(&key, &range),
            &CancellationToken::uncancelable()
        ))
        .stage,
        NativeStrongReadStage::WriterAdmission
    );
    fixture.idle();
    // Independently fresh HostMQ/install retry, never a flag/old receipt reset.
    let business = Uuid::now_v7();
    let observation = fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(
            &node("n1"),
            business,
            Duration::from_secs(5),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let policy = {
        let service = fixture.native.services[&node("n0")].lock();
        let db = fixture.native.databases[&node("n0")].read();
        service
            .prepare_native_host_writer_enrollment(&db, observation, business)
            .unwrap()
    };
    let host = fixture.native.databases[&node("n0")]
        .read()
        .install_enrolled_native_coordinator(fixture.coordinators[&node("n0")].clone(), policy)
        .unwrap();
    fixture.hosts.insert(node("n0"), Arc::new(host));
    assert!(observed(fixture.read(
        fixture.request(&key, &range),
        &CancellationToken::uncancelable()
    ))
    .record()
    .is_some());
    fixture.idle();
}

mod after_transfer;
