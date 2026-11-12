//! Actual loopback mTLS participant RPCs. Original ACKs use real native RF3
//! services; current epoch/leader and metadata election are explicit fixture
//! preconditions. This is not a physical failover or a serving-ready proof.
use super::*;
use crate::distribution_consensus::{
    MetadataConsensusStore, MetadataReadParticipant, MetadataRecoveryRolePurpose,
};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_consensus::{RangeWriteTransport, RecoveryFenceScope};
use crate::distribution_range_recovery::{
    RangeRecoveryRuntime, RecoveryEpochFenceRequest, RecoveryFenceQuorumRound,
    RecoveryFencedProbeRequest, RecoveryHistoricalCommandRequest, RecoveryHistoryObservation,
    RecoveryRoleAdmission,
};
use crate::distribution_transport::start_cluster_data_server_with_recovery_runtime;
use crate::{
    ClusterDataServerHandle, ClusterNetworkTransportConfig, ReplicationTlsConfig,
    TcpClusterRelocationTransport,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

fn node(value: &str) -> ClusterNodeId {
    ClusterNodeId::new(value).unwrap()
}
struct Identity {
    cert: PathBuf,
    key: PathBuf,
    ca: PathBuf,
}
impl Identity {
    fn tls(&self) -> ReplicationTlsConfig {
        ReplicationTlsConfig {
            cert_path: self.cert.clone(),
            key_path: self.key.clone(),
            ca_path: self.ca.clone(),
            require_client_cert: true,
            dev_localhost_plaintext: false,
        }
    }
    fn pin(&self) -> String {
        crate::replication_transport::replication_certificate_sha256(&self.cert).unwrap()
    }
}
fn identities(root: &Path) -> BTreeMap<ClusterNodeId, Identity> {
    identities_for_names(root, &["n0", "n1", "n2"])
}
fn identities_for_names(root: &Path, names: &[&str]) -> BTreeMap<ClusterNodeId, Identity> {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = params.self_signed(&key).unwrap();
    let ca_path = root.join("ca.pem");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    names
        .iter()
        .copied()
        .map(|name| {
            let leaf_key = KeyPair::generate().unwrap();
            let mut params =
                CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
            params.is_ca = IsCa::ExplicitNoCa;
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let cert = params.signed_by(&leaf_key, &ca, &key).unwrap();
            let identity = Identity {
                cert: root.join(format!("{name}.pem")),
                key: root.join(format!("{name}.key")),
                ca: ca_path.clone(),
            };
            std::fs::write(&identity.cert, cert.pem()).unwrap();
            std::fs::write(&identity.key, leaf_key.serialize_pem()).unwrap();
            (node(name), identity)
        })
        .collect()
}
#[derive(Debug, Default)]
struct ProbeHold {
    armed: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
}
impl ProbeHold {
    fn arm(&self) {
        self.entered.store(false, Ordering::SeqCst);
        self.released.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }
    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct ObservedTlsTransport {
    inner: Arc<TcpClusterRelocationTransport>,
    hold: Arc<ProbeHold>,
    ready_hold: Arc<ProbeHold>,
    read_hold: Arc<ProbeHold>,
    read_expired_once: Arc<AtomicBool>,
    read_n2_successful_calls: Arc<std::sync::atomic::AtomicUsize>,
    lag_status_seen: Arc<AtomicBool>,
}
impl RangeWriteTransport for ObservedTlsTransport {
    fn observe_current_native_range(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_native_read::NativeRangeReadProbeRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_read::NativeRangeReadObservation> {
        if destination == &node("n2") && self.read_hold.armed.load(Ordering::SeqCst) {
            while !self.read_hold.entered.load(Ordering::SeqCst) {
                parent.check()?;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let actual = self
            .inner
            .observe_current_native_range(destination, request, parent)?;
        if destination == &node("n2") {
            self.read_n2_successful_calls.fetch_add(1, Ordering::SeqCst);
        }

        if destination == &node("n2") && self.read_expired_once.swap(false, Ordering::SeqCst) {
            let until =
                parent.deadline().expect("bounded actual read attempt") + Duration::from_millis(5);
            while Instant::now() < until {
                std::thread::sleep(Duration::from_millis(1));
            }
            // Return a genuinely authenticated but expired observation once.
            // It must constrain history, never count as a vote or reset TTL.
            return Ok(actual);
        }

        if destination == &node("n0") && self.read_hold.armed.load(Ordering::SeqCst) {
            self.read_hold.entered.store(true, Ordering::SeqCst);
            // Test-only uncancellable return barrier AFTER the genuine TLS
            // response. Its held result is not counted in a candidate.
            let until = Instant::now() + Duration::from_secs(5);
            while !self.read_hold.released.load(Ordering::SeqCst) && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        parent.check()?;
        Ok(actual)
    }

    fn observe_native_completion_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeCompletionObservation> {
        self.inner
            .observe_native_completion_observation(destination, request, parent)
    }
    fn observe_native_ready_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeReadyObservation> {
        if destination == &node("n0") && self.ready_hold.armed.swap(false, Ordering::SeqCst) {
            self.ready_hold.entered.store(true, Ordering::SeqCst);
            while !self.ready_hold.released.load(Ordering::SeqCst) {
                parent.check()?;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let actual = self
            .inner
            .observe_native_ready_observation(destination, request, parent)?;
        if destination == &node("n2") && actual.reply().range_index == 0 {
            self.lag_status_seen.store(true, Ordering::SeqCst);
        }
        Ok(actual)
    }
    fn prepare_native_ready_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeReadyObservation> {
        self.inner
            .prepare_native_ready_observation(destination, request, parent)
    }
    fn recovery_fenced_history_observation(
        &self,
        destination: &ClusterNodeId,
        request: &RecoveryFencedProbeRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        if destination == &node("n2")
            && request.index.is_none()
            && self.hold.armed.swap(false, Ordering::SeqCst)
        {
            self.hold.entered.store(true, Ordering::SeqCst);
            while !self.hold.released.load(Ordering::SeqCst) {
                parent.check()?;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        // Every returned observation still originates from the actual mTLS RPC.
        self.inner
            .recovery_fenced_history_observation(destination, request, parent)
    }
    fn range_write_progress(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
    ) -> Result<RangeWriteProgress> {
        self.inner.range_write_progress(n, r, e)
    }
    fn apply_range_write_repair(
        &self,
        n: &ClusterNodeId,
        b: &RangeWriteRepairBatch,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.inner.apply_range_write_repair(n, b, l)
    }
    fn fetch_range_write_repair(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        i: u64,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.inner.fetch_range_write_repair(n, r, e, i, l)
    }
    fn probe_range_write(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        i: u64,
    ) -> Result<RangeWriteProbe> {
        self.inner.probe_range_write(n, r, e, i)
    }
    fn prepare_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.prepare_range_write(n, c)
    }
    fn commit_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.commit_range_write(n, c)
    }
    fn certify_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.certify_range_write(n, c)
    }
    fn apply_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.apply_range_write(n, c)
    }
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.abort_range_write(n, c)
    }
}
struct NetworkFixture {
    native: ActualNativeRf3Fixture,
    current: Arc<RwLock<ClusterTopology>>,
    command: RangeWriteCommand,
    clients: BTreeMap<ClusterNodeId, Arc<TcpClusterRelocationTransport>>,
    providers: BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    runtimes: BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    servers: Vec<ClusterDataServerHandle>,
    hold: Arc<ProbeHold>,
    ready_hold: Arc<ProbeHold>,
    read_hold: Arc<ProbeHold>,
    read_expired_once: Arc<AtomicBool>,
    read_n2_successful_calls: Arc<std::sync::atomic::AtomicUsize>,
    lag_status_seen: Arc<AtomicBool>,
    _keys: tempfile::TempDir,
}
impl NetworkFixture {
    fn new(wired: bool) -> Self {
        Self::new_ready_network(wired, false, false)
    }
    fn new_ready_network(wired: bool, all_nodes: bool, lagging_two: bool) -> Self {
        Self::new_ready_network_with_roles(wired, all_nodes, lagging_two, "n1", "n2")
    }
    fn new_ready_network_with_roles(
        wired: bool,
        all_nodes: bool,
        lagging_two: bool,
        data_leader: &str,
        metadata_leader: &str,
    ) -> Self {
        Self::new_ready_network_with_enrollment(
            wired,
            all_nodes,
            lagging_two,
            data_leader,
            metadata_leader,
            false,
        )
    }
    fn new_network_mode(
        wired: bool,
        all_nodes: bool,
        lagging_two: bool,
        data_leader: &str,
        metadata_leader: &str,
        historical_transition: bool,
    ) -> Self {
        Self::new_network_mode_with_enrollment(
            wired,
            all_nodes,
            lagging_two,
            data_leader,
            metadata_leader,
            historical_transition,
            false,
            None,
        )
    }
    fn new_ready_network_with_enrollment(
        wired: bool,
        all_nodes: bool,
        lagging_two: bool,
        data_leader: &str,
        metadata_leader: &str,
        enroll: bool,
    ) -> Self {
        Self::new_with_initial_metadata_edit(
            wired,
            all_nodes,
            lagging_two,
            data_leader,
            metadata_leader,
            enroll,
            None,
        )
    }
    fn new_with_initial_metadata_edit(
        wired: bool,
        all_nodes: bool,
        lagging_two: bool,
        data_leader: &str,
        metadata_leader: &str,
        enroll: bool,
        initial_edit: Option<fn(&mut ClusterTopology, &ActualNativeRf3Fixture, &Path)>,
    ) -> Self {
        Self::new_network_mode_with_enrollment(
            wired,
            all_nodes,
            lagging_two,
            data_leader,
            metadata_leader,
            true,
            enroll,
            initial_edit,
        )
    }
    fn new_network_mode_with_enrollment(
        wired: bool,
        all_nodes: bool,
        lagging_two: bool,
        data_leader: &str,
        metadata_leader: &str,
        historical_transition: bool,
        enroll: bool,
        initial_edit: Option<fn(&mut ClusterTopology, &ActualNativeRf3Fixture, &Path)>,
    ) -> Self {
        assert!((data_leader == "n1" && metadata_leader == "n2") || all_nodes);
        assert!(!lagging_two || all_nodes);
        let live_names = if all_nodes {
            vec!["n0", "n1", "n2"]
        } else {
            vec!["n1", "n2"]
        };
        let keys = tempfile::tempdir().unwrap();
        let identities = identities(keys.path());
        let listeners = ["n0", "n1", "n2"]
            .into_iter()
            .map(|name| (node(name), TcpListener::bind("127.0.0.1:0").unwrap()))
            .collect::<BTreeMap<_, _>>();
        let network = listeners
            .iter()
            .map(|(id, listener)| {
                (
                    id.clone(),
                    (
                        listener.local_addr().unwrap().to_string(),
                        identities[id].pin(),
                    ),
                )
            })
            .collect();
        let native = if enroll {
            ActualNativeRf3Fixture::new_with_enrolled_network_precondition(&network)
        } else {
            ActualNativeRf3Fixture::new_with_network_precondition(&network)
        };
        if lagging_two {
            native.transport.set_unavailable(&node("n2"), true);
        }
        native.append("actual-tls-historical").unwrap();
        if lagging_two {
            native.transport.set_unavailable(&node("n2"), false);
        }
        let command = {
            let topology = native.topology.read();
            let range = topology.ranges.values().next().unwrap();
            native.services[&node("n0")]
                .lock()
                .probe_range_write_for_peer(range, &node("n0"), 1)
                .unwrap()
                .entry
                .unwrap()
                .command
        };
        native.databases[&node("n0")]
            .read()
            .clear_commit_admission();
        let mut topology = native.topology.read().clone();
        if historical_transition {
            topology.generation += 1;
            let range = topology.ranges.values_mut().next().unwrap();
            range.epoch += 1;
            range.leader = node(data_leader);
        } else {
            assert_eq!(data_leader, "n0");
        }
        // INITIAL metadata fixture, before any consensus-store creation.
        if let Some(edit) = initial_edit {
            edit(&mut topology, &native, keys.path());
        }
        topology.validate().unwrap();
        let config = |id: &ClusterNodeId| ClusterNetworkTransportConfig {
            connect_timeout_ms: 50,
            io_timeout_ms: 4000,
            max_frame_bytes: 4 * 1024 * 1024,
            max_inbound_connections: 16,
            dev_localhost_plaintext: false,
            tls: Some(identities[id].tls()),
        };
        let current = Arc::new(RwLock::new(topology.clone()));
        let clients = live_names
            .iter()
            .copied()
            .map(|name| {
                let id = node(name);
                let client = Arc::new(
                    TcpClusterRelocationTransport::new(
                        topology.cluster_id.clone(),
                        id.clone(),
                        Arc::clone(&current),
                        config(&id),
                    )
                    .unwrap(),
                );
                (id, client)
            })
            .collect::<BTreeMap<_, _>>();
        // Actual persisted current-term metadata anchor. Defaults remain n2;
        // adversarial all-node fixtures may choose a distinct metadata leader.
        // Election membership is an explicit unit fixture precondition.
        let open = |id: &ClusterNodeId| {
            MetadataConsensusStore::open(
                keys.path().join(format!("meta-{id}")),
                topology.cluster_id.clone(),
                id.clone(),
                topology.clone(),
                true,
            )
            .unwrap()
        };
        let metadata_leader = node(metadata_leader);
        let follower_id = if metadata_leader == node("n2") {
            node("n1")
        } else {
            node("n0")
        };
        assert_ne!(metadata_leader, follower_id);
        let extra_id = [node("n0"), node("n1"), node("n2")]
            .into_iter()
            .find(|id| id != &metadata_leader && id != &follower_id)
            .unwrap();
        let mut leader = open(&metadata_leader);
        let mut follower = open(&follower_id);
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                follower_id.clone(),
                metadata_leader.clone(),
            ]))
            .unwrap();
        let response = follower
            .handle_append_request(leader.append_request_from(1, 16).unwrap())
            .unwrap();
        leader
            .record_append_response(&follower_id, &response)
            .unwrap();
        let extra = if all_nodes {
            let mut extra = open(&extra_id);
            let response = extra
                .handle_append_request(leader.append_request_from(1, 16).unwrap())
                .unwrap();
            leader.record_append_response(&extra_id, &response).unwrap();
            Some(Arc::new(Mutex::new(extra)))
        } else {
            None
        };
        let mut consensus = BTreeMap::from([
            (follower_id, Arc::new(Mutex::new(follower))),
            (metadata_leader, Arc::new(Mutex::new(leader))),
        ]);
        if let Some(extra) = extra {
            consensus.insert(extra_id, extra);
        }
        let providers = consensus
            .iter()
            .map(|(id, state)| {
                (
                    id.clone(),
                    Arc::new(
                        MetadataReadProvider::new(Arc::clone(state), clients[id].clone()).unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let hold = Arc::new(ProbeHold::default());
        let ready_hold = Arc::new(ProbeHold::default());
        let read_hold = Arc::new(ProbeHold::default());
        let read_expired_once = Arc::new(AtomicBool::new(false));
        let read_n2_successful_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let lag_status_seen = Arc::new(AtomicBool::new(false));
        let runtimes = live_names
            .iter()
            .copied()
            .map(|name| {
                let id = node(name);
                let runtime = RangeRecoveryRuntime::new(
                    Arc::clone(&native.services[&id]),
                    Arc::clone(&providers[&id]),
                );
                let runtime = if wired {
                    runtime.with_participant_transport(Arc::new(ObservedTlsTransport {
                        inner: Arc::clone(&clients[&id]),
                        hold: if name == "n1" {
                            Arc::clone(&hold)
                        } else {
                            Arc::new(ProbeHold::default())
                        },
                        ready_hold: if name == "n1" {
                            Arc::clone(&ready_hold)
                        } else {
                            Arc::new(ProbeHold::default())
                        },
                        read_hold: if name == "n1" {
                            Arc::clone(&read_hold)
                        } else {
                            Arc::new(ProbeHold::default())
                        },
                        read_expired_once: if name == "n1" {
                            Arc::clone(&read_expired_once)
                        } else {
                            Arc::new(AtomicBool::new(false))
                        },
                        read_n2_successful_calls: if name == "n1" {
                            Arc::clone(&read_n2_successful_calls)
                        } else {
                            Arc::new(std::sync::atomic::AtomicUsize::new(0))
                        },
                        lag_status_seen: Arc::clone(&lag_status_seen),
                    }))
                } else {
                    runtime
                };
                (id, Arc::new(runtime))
            })
            .collect::<BTreeMap<_, _>>();
        // Default: n0's endpoint stays closed. The readiness lag variant
        // opens all three actual mTLS endpoints; neither is a physical-host test.
        drop(listeners);
        let mut servers = Vec::new();
        for name in &live_names {
            let id = node(name);
            servers.push(
                start_cluster_data_server_with_recovery_runtime(
                    &topology.nodes[&id].address,
                    config(&id),
                    Arc::clone(&current),
                    Arc::clone(&native.services[&id]),
                    None,
                    Some(Arc::clone(&consensus[&id])),
                    None,
                    Some(Arc::clone(&providers[&id])),
                    Some(Arc::clone(&runtimes[&id])),
                )
                .unwrap(),
            );
        }
        Self {
            native,
            current,
            command,
            clients,
            providers,
            runtimes,
            servers,
            hold,
            ready_hold,
            read_hold,
            read_expired_once,
            read_n2_successful_calls,
            lag_status_seen,
            _keys: keys,
        }
    }
    fn actor(&self) -> MetadataReadParticipant {
        self.clients[&node("n1")].local_metadata_read_participant()
    }
    fn scope(&self) -> RecoveryFenceScope {
        let actor = self.actor();
        let range = self.current.read().ranges.values().next().unwrap().clone();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let nonce = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let witness = self.providers[&node("n1")]
            .witness_recovery_role(
                &actor,
                business,
                range.id,
                range.epoch,
                nonce,
                challenge,
                purpose.clone(),
                &parent,
            )
            .unwrap();
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            &actor,
            &node("n1"),
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            &purpose,
        )
        .unwrap();
        let mut round = RecoveryFenceQuorumRound::begin(role, &parent).unwrap();
        let scope = round.scope().clone();
        for destination in self.runtimes.keys() {
            let observation = self.clients[&node("n1")]
                .recovery_epoch_fence_observation(
                    destination,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 4000,
                    },
                    &parent,
                )
                .unwrap();
            round.record(Arc::new(observation)).unwrap();
        }
        round.finish().unwrap();
        scope
    }
    fn request(&self, scope: &RecoveryFenceScope) -> RecoveryHistoricalCommandRequest {
        RecoveryHistoricalCommandRequest {
            scope: scope.clone(),
            command: self.command.clone(),
            budget_ms: 3500,
        }
    }
    fn call(
        &self,
        name: &str,
        scope: &RecoveryFenceScope,
        apply: bool,
    ) -> Result<RecoveryHistoryObservation> {
        self.clients[&node("n1")].recovery_historical_command_observation(
            &node(name),
            &self.request(scope),
            apply,
            &CancellationToken::uncancelable(),
        )
    }
    fn wait_idle(&self) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while self
            .runtimes
            .values()
            .any(|r| r.test_active_context_count() != 0)
        {
            assert!(
                Instant::now() < deadline,
                "actual participant contexts failed to drain"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn ready_request(
        &self,
        scope: &RecoveryFenceScope,
    ) -> crate::distribution_range_recovery::NativeReadyRequest {
        crate::distribution_range_recovery::NativeReadyRequest {
            scope: scope.clone(),
            challenge: Uuid::now_v7(),
            budget_ms: 3500,
        }
    }
    fn prepare_ready(
        &self,
        name: &str,
        scope: &RecoveryFenceScope,
    ) -> Result<crate::distribution_range_recovery::NativeReadyObservation> {
        self.clients[&node("n1")].prepare_native_ready_observation(
            &node(name),
            &self.ready_request(scope),
            &CancellationToken::uncancelable(),
        )
    }
    fn status_ready(
        &self,
        name: &str,
        scope: &RecoveryFenceScope,
    ) -> Result<crate::distribution_range_recovery::NativeReadyObservation> {
        self.clients[&node("n1")].observe_native_ready_observation(
            &node(name),
            &self.ready_request(scope),
            &CancellationToken::uncancelable(),
        )
    }
    fn ready_role(&self, scope: &RecoveryFenceScope) -> RecoveryRoleAdmission {
        let actor = self.actor();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let witness = self.providers[&node("n1")]
            .witness_recovery_role(
                &actor,
                business,
                scope.range_id,
                scope.range_epoch,
                scope.recovery_nonce,
                challenge,
                scope.purpose.clone(),
                &parent,
            )
            .unwrap();
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            &actor,
            &node("n1"),
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            &scope.purpose,
        )
        .unwrap();
        scope.require_role(&role).unwrap();
        role
    }
    fn all_r1_prepared() -> (Self, RecoveryFenceScope) {
        let fixture = Self::new_ready_network(true, true, false);
        let scope = fixture.scope();
        for name in ["n0", "n1", "n2"] {
            fixture.call(name, &scope, false).unwrap();
            fixture.wait_idle();
        }
        for name in ["n0", "n1", "n2"] {
            fixture.call(name, &scope, true).unwrap();
            fixture.wait_idle();
        }
        for name in ["n0", "n1", "n2"] {
            fixture.prepare_ready(name, &scope).unwrap();
            fixture.wait_idle();
        }
        (fixture, scope)
    }
    fn completion_request(
        &self,
        scope: &RecoveryFenceScope,
    ) -> crate::distribution_range_recovery::NativeCompletionRequest {
        crate::distribution_range_recovery::NativeCompletionRequest {
            scope: scope.clone(),
            challenge: Uuid::now_v7(),
            budget_ms: 3500,
        }
    }
    fn complete(
        &self,
        name: &str,
        scope: &RecoveryFenceScope,
    ) -> Result<crate::distribution_range_recovery::NativeCompletionObservation> {
        self.clients[&node("n1")].complete_native_recovery_observation(
            &node(name),
            &self.completion_request(scope),
            &CancellationToken::uncancelable(),
        )
    }
    fn completion_status(
        &self,
        name: &str,
        scope: &RecoveryFenceScope,
    ) -> Result<crate::distribution_range_recovery::NativeCompletionObservation> {
        self.clients[&node("n1")].observe_native_completion_observation(
            &node(name),
            &self.completion_request(scope),
            &CancellationToken::uncancelable(),
        )
    }
    fn native_index(&self, name: &str) -> u64 {
        self.native.databases[&node(name)]
            .read()
            .current_native_range_source_lineage(self.command.range_id)
            .unwrap()
            .range_index()
    }
}
impl Drop for NetworkFixture {
    fn drop(&mut self) {
        self.hold.release();
        self.ready_hold.release();
        for server in self.servers.drain(..) {
            server.join().unwrap();
        }
    }
}

#[test]
fn actual_mtls_participant_current_quorum_recovers_without_old_data_leader_and_rejects_stale_scope()
{
    let fixture = NetworkFixture::new(true);
    let scope = fixture.scope();
    assert_ne!(
        fixture
            .current
            .read()
            .ranges
            .values()
            .next()
            .unwrap()
            .leader,
        node("n2")
    );
    let first = fixture.call("n1", &scope, false).unwrap();
    assert!(first.current_certification().is_some());
    let missing_quorum = fixture.call("n1", &scope, true).unwrap_err();
    assert!(
        fixture.native_index("n1") == 0,
        "one current marker must not grant native Apply: {missing_quorum}"
    );
    fixture.wait_idle();
    fixture.call("n2", &scope, false).unwrap();
    fixture.wait_idle();
    fixture.call("n1", &scope, true).unwrap();
    fixture.wait_idle();
    fixture.call("n2", &scope, true).unwrap();
    fixture.wait_idle();
    let before = fixture.native.databases[&node("n1")]
        .read()
        .current_commit_seq();
    fixture.call("n1", &scope, true).unwrap();
    fixture.wait_idle();
    assert_eq!(
        fixture.native.databases[&node("n1")]
            .read()
            .current_commit_seq(),
        before,
        "already-covered duplicate must not redo old final values"
    );
    for name in ["n1", "n2"] {
        assert_eq!(fixture.native_index(name), 1);
        assert!(fixture.native.databases[&node(name)]
            .read()
            .get("items", "actual-tls-historical")
            .unwrap()
            .is_some());
    }
    let mut stale = fixture.request(&scope);
    stale.scope.range_epoch -= 1;
    assert!(fixture.clients[&node("n1")]
        .recovery_historical_command_observation(
            &node("n1"),
            &stale,
            true,
            &CancellationToken::uncancelable()
        )
        .is_err());
    fixture.wait_idle();
    let newer = fixture.scope();
    assert_ne!(scope.recovery_nonce, newer.recovery_nonce);
    let old_status = fixture.clients[&node("n1")].recovery_fenced_history_observation(
        &node("n2"),
        &RecoveryFencedProbeRequest {
            scope,
            index: None,
            budget_ms: 1000,
        },
        &CancellationToken::uncancelable(),
    );
    assert!(
        old_status.is_err(),
        "old readonly nonce cannot rotate current recovery ownership"
    );
    fixture.wait_idle();
    // Rows only. Persistent recovery fencing remains active; no ordinary new
    // write, configuration activation, complete audit effects or StrongGet claim.
}

#[test]
fn actual_mtls_participant_collection_holds_no_recipient_service_or_database_latch() {
    let fixture = NetworkFixture::new(true);
    let scope = fixture.scope();
    fixture.call("n1", &scope, false).unwrap();
    fixture.wait_idle();
    fixture.call("n2", &scope, false).unwrap();
    fixture.wait_idle();
    fixture.hold.arm();
    let client = Arc::clone(&fixture.clients[&node("n1")]);
    let request = fixture.request(&scope);
    let worker = std::thread::spawn(move || {
        client.recovery_historical_command_observation(
            &node("n1"),
            &request,
            true,
            &CancellationToken::uncancelable(),
        )
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while !fixture.hold.entered.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "actual held peer probe was never reached"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        fixture.native.services[&node("n1")].try_lock().is_some(),
        "recipient service latch held across participant collection"
    );
    assert!(
        fixture.native.databases[&node("n1")].try_write().is_some(),
        "recipient DB latch held across participant collection"
    );
    fixture.hold.release();
    worker.join().unwrap().unwrap();
    fixture.wait_idle();
    assert_eq!(fixture.native_index("n1"), 1);
}

#[test]
fn actual_mtls_unwired_participant_receiver_refuses_without_materializing() {
    let fixture = NetworkFixture::new(false);
    let scope = fixture.scope();
    let error = fixture.call("n1", &scope, false).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("participant observation transport is not wired"),
        "{error}"
    );
    fixture.wait_idle();
    assert_eq!(fixture.native_index("n1"), 0);
    assert!(fixture.native.databases[&node("n1")]
        .read()
        .get("items", "actual-tls-historical")
        .unwrap()
        .is_none());
}

#[test]
fn native_ready_actual_tls_preparation_status_and_local_closed_quorum_remain_frozen() {
    let fixture = NetworkFixture::new(true);
    let scope = fixture.scope();
    assert!(
        fixture.prepare_ready("n1", &scope).is_err(),
        "raw CQ without physical native apply cannot prepare"
    );
    fixture.wait_idle();
    assert!(
        fixture.status_ready("n1", &scope).is_err(),
        "readonly status cannot manufacture a physical preparation"
    );
    fixture.wait_idle();
    for name in ["n1", "n2"] {
        fixture.call(name, &scope, false).unwrap();
        fixture.wait_idle();
    }
    for name in ["n1", "n2"] {
        fixture.call(name, &scope, true).unwrap();
        fixture.wait_idle();
    }
    let first = fixture.prepare_ready("n1", &scope).unwrap();
    fixture.wait_idle();
    assert_eq!(first.reply().range_index, 1);
    let request = fixture.ready_request(&scope);
    assert!(
        fixture.runtimes[&node("n1")]
            .acquire_closed_native_ready(
                &fixture.actor(),
                &request,
                &CancellationToken::uncancelable()
            )
            .is_err(),
        "one actual preparation cannot become a current majority"
    );
    fixture.wait_idle();
    let second = fixture.prepare_ready("n2", &scope).unwrap();
    fixture.wait_idle();
    assert_eq!(second.reply().range_index, 1);
    assert_ne!(
        first.reply().native_receipt_sha256,
        second.reply().native_receipt_sha256,
        "local physical receipts are not a common logical history"
    );
    let observed_request = fixture.ready_request(&scope);
    let observed = fixture.clients[&node("n1")]
        .observe_native_ready_observation(
            &node("n1"),
            &observed_request,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    fixture.wait_idle();
    assert_ne!(
        observed.reply().observed_metadata_context_id,
        scope.metadata_context_id,
        "wire provenance comes from the recipient's NEW MQ round, not echoed caller scope"
    );
    assert_ne!(
        observed.reply().observed_metadata_context_id,
        first.reply().observed_metadata_context_id
    );
    assert!(observed.reply().observed_metadata_term >= scope.metadata_term);
    assert!(observed.reply().observed_metadata_read_index >= scope.metadata_read_index);
    observed
        .reply()
        .test_validate_shape_only(&observed_request, observed.authenticated_peer())
        .unwrap();
    let mut missing_context = observed.reply().clone();
    missing_context.observed_metadata_context_id = Uuid::nil();
    assert!(missing_context
        .test_validate_shape_only(&observed_request, observed.authenticated_peer())
        .is_err());
    let mut missing_term = observed.reply().clone();
    missing_term.observed_metadata_term = 0;
    assert!(missing_term
        .test_validate_shape_only(&observed_request, observed.authenticated_peer())
        .is_err());
    let mut future_minimum = observed_request.clone();
    future_minimum.scope.metadata_read_index = observed
        .reply()
        .observed_metadata_read_index
        .checked_add(1)
        .unwrap();
    assert!(
        observed
            .reply()
            .test_validate_shape_only(&future_minimum, observed.authenticated_peer())
            .is_err(),
        "a reply cannot meet a committed anchor it has not actually observed"
    );
    assert_eq!(
        observed.reply().native_receipt_sha256,
        first.reply().native_receipt_sha256
    );
    assert_ne!(observed.reply().challenge, first.reply().challenge);
    let (local, quorum) = fixture.runtimes[&node("n1")]
        .acquire_closed_native_ready(
            &fixture.actor(),
            &fixture.ready_request(&scope),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    fixture.wait_idle();
    assert_eq!(local.range_index(), 1);
    assert_eq!(quorum.range_index(), 1);
    assert_eq!(
        quorum
            .distinct_member_ids()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([node("n1"), node("n2")])
    );
    assert!(fixture.native.databases[&node("n1")]
        .read()
        .range_mutation_gate()
        .try_enter(scope.range_id)
        .is_err());
    let mut wrong = scope.clone();
    wrong.recovery_nonce = Uuid::now_v7();
    assert!(fixture.status_ready("n2", &wrong).is_err());
    fixture.wait_idle();
    assert!(fixture.status_ready("n2", &scope).is_ok());
    fixture.wait_idle();
    // The collector is invoked locally with a trusted current-member actor.
    // All peer Prepare/Status evidence came from actual authenticated TLS RPCs.
    // No completion wire, serving activation, normal new write or StrongGet.
}

#[test]
fn native_ready_actual_tls_duplicate_voter_cannot_count_twice() {
    use crate::distribution_range_recovery::ClosedNativeReadyQuorumRound;
    let fixture = NetworkFixture::new(true);
    let scope = fixture.scope();
    for name in ["n1", "n2"] {
        fixture.call(name, &scope, false).unwrap();
        fixture.wait_idle();
    }
    for name in ["n1", "n2"] {
        fixture.call(name, &scope, true).unwrap();
        fixture.wait_idle();
    }
    for name in ["n1", "n2"] {
        fixture.prepare_ready(name, &scope).unwrap();
        fixture.wait_idle();
    }
    let role = fixture.ready_role(&scope);
    let mut round =
        ClosedNativeReadyQuorumRound::begin(role, &CancellationToken::uncancelable()).unwrap();
    round
        .record(fixture.status_ready("n1", &scope).unwrap())
        .unwrap();
    fixture.wait_idle();
    assert!(round
        .record(fixture.status_ready("n1", &scope).unwrap())
        .is_err());
    fixture.wait_idle();
    assert!(!round.is_ready());
    assert!(round.finish().is_err());
}

#[test]
fn native_ready_actual_tls_fast_clean_zero_minority_does_not_starve_matching_one_majority() {
    let fixture = NetworkFixture::new_ready_network(true, true, true);
    let scope = fixture.scope();
    let range = fixture
        .current
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(
        fixture.native.services[&node("n2")]
            .lock()
            .probe_range_write_for_peer(&range, &node("n1"), 1)
            .unwrap()
            .progress
            .last_index,
        0
    );
    for name in ["n0", "n1"] {
        fixture.call(name, &scope, false).unwrap();
        fixture.wait_idle();
    }
    fixture.call("n1", &scope, true).unwrap();
    fixture.wait_idle();
    for name in ["n0", "n1", "n2"] {
        fixture.prepare_ready(name, &scope).unwrap();
        fixture.wait_idle();
    }
    assert_eq!(
        fixture
            .status_ready("n2", &scope)
            .unwrap()
            .reply()
            .range_index,
        0
    );
    fixture.wait_idle();
    fixture.ready_hold.arm();
    fixture.lag_status_seen.store(false, Ordering::SeqCst);
    let runtime = Arc::clone(&fixture.runtimes[&node("n1")]);
    let actor = fixture.actor();
    let request = fixture.ready_request(&scope);
    let worker = std::thread::spawn(move || -> Result<(u64, u64, BTreeSet<ClusterNodeId>)> {
        let (local, quorum) = runtime.acquire_closed_native_ready(
            &actor,
            &request,
            &CancellationToken::uncancelable(),
        )?;
        // Keep the actual authority/physical proof on its producing thread;
        // return only diagnostic getters rather than imposing a Send contract.
        Ok((
            local.range_index(),
            quorum.range_index(),
            quorum.distinct_member_ids().cloned().collect(),
        ))
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while !fixture.ready_hold.entered.load(Ordering::SeqCst)
        || !fixture.lag_status_seen.load(Ordering::SeqCst)
    {
        assert!(
            Instant::now() < deadline,
            "actual clean R0 response/held R1 peer were not both reached"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(fixture.native.services[&node("n1")].try_lock().is_some());
    assert!(fixture.native.databases[&node("n1")].try_write().is_some());
    fixture.ready_hold.release();
    let (local_index, quorum_index, members) = worker.join().unwrap().unwrap();
    fixture.wait_idle();
    assert_eq!(local_index, 1);
    assert_eq!(quorum_index, 1);
    assert_eq!(members, BTreeSet::from([node("n0"), node("n1")]));
    assert_eq!(fixture.native_index("n2"), 0);
    assert_eq!(fixture.native_index("n0"), 1);
    assert_eq!(fixture.native_index("n1"), 1);
    // Genuine native/certified states, no caller-set floor or fake observation.
    // The lagging minority remains unavailable for serving until actual repair.
}

#[test]
fn native_completion_actual_tls_sequential_replicas_keep_frozen_pins_and_duplicate_sha() {
    use crate::distribution_range_recovery::{FrozenNativeReadyStage, NativeCompletionStage};
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    assert!(fixture.completion_status("n0", &scope).is_err());
    fixture.wait_idle();
    let mut hashes = BTreeMap::new();
    let before = fixture
        .native
        .databases
        .iter()
        .map(|(id, db)| (id.clone(), db.read().current_commit_seq()))
        .collect::<BTreeMap<_, _>>();
    let original_records = fixture
        .native
        .databases
        .iter()
        .map(|(id, db)| {
            let record = db
                .read()
                .get("items", "actual-tls-historical")
                .unwrap()
                .unwrap();
            (id.clone(), record)
        })
        .collect::<BTreeMap<_, _>>();
    for name in ["n0", "n1", "n2"] {
        let result = fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
        assert_eq!(result.reply().stage, NativeCompletionStage::CompletedFrozen);
        assert_eq!(result.reply().range_index, 1);
        hashes.insert(node(name), result.reply().native_serving_sha256.clone());
        let status = fixture.status_ready(name, &scope).unwrap();
        fixture.wait_idle();
        assert_eq!(
            status.reply().stage,
            FrozenNativeReadyStage::NativeCompletedFrozen
        );
    }
    for name in ["n0", "n1", "n2"] {
        let repeated = fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
        assert_eq!(repeated.reply().native_serving_sha256, hashes[&node(name)]);
        let status = fixture.completion_status(name, &scope).unwrap();
        fixture.wait_idle();
        assert_eq!(status.reply().native_serving_sha256, hashes[&node(name)]);
        assert_eq!(fixture.native_index(name), 1);
        let db = fixture.native.databases[&node(name)].read();
        assert_eq!(db.current_commit_seq(), before[&node(name)]);
        assert_eq!(
            db.get("items", "actual-tls-historical").unwrap().as_ref(),
            Some(&original_records[&node(name)])
        );
        assert!(db.range_mutation_gate().try_enter(scope.range_id).is_err());
    }
    // These are actual incoming Complete/readonly TLS RPCs with fresh peer QQ.
    // Completed-Frozen is not Activated, a normal new-write or StrongGet grant.
}

#[test]
fn native_completion_actual_tls_wrong_actor_cannot_complete_but_current_voter_can_observe() {
    use crate::distribution_range_recovery::{FrozenNativeReadyStage, NativeCompletionStage};
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    let wrong = fixture.clients[&node("n2")].complete_native_recovery_observation(
        &node("n0"),
        &fixture.completion_request(&scope),
        &CancellationToken::uncancelable(),
    );
    assert!(
        wrong.is_err(),
        "authenticated voter2 is not current data leader1"
    );
    fixture.wait_idle();
    assert!(fixture.completion_status("n0", &scope).is_err());
    fixture.wait_idle();
    let prepared = fixture.status_ready("n0", &scope).unwrap();
    fixture.wait_idle();
    assert_eq!(
        prepared.reply().stage,
        FrozenNativeReadyStage::NativePrepared
    );
    let complete = fixture.complete("n0", &scope).unwrap();
    fixture.wait_idle();
    let observed = fixture.clients[&node("n2")]
        .observe_native_completion_observation(
            &node("n0"),
            &fixture.completion_request(&scope),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    fixture.wait_idle();
    assert_eq!(
        observed.reply().native_serving_sha256,
        complete.reply().native_serving_sha256
    );
    let mut wrong_scope = scope.clone();
    wrong_scope.recovery_nonce = Uuid::now_v7();
    assert!(fixture.completion_status("n0", &wrong_scope).is_err());
    fixture.wait_idle();
    assert_eq!(
        fixture
            .completion_status("n0", &scope)
            .unwrap()
            .reply()
            .native_serving_sha256,
        complete.reply().native_serving_sha256
    );
    fixture.wait_idle();
}

#[test]
fn native_completion_actual_tls_canceled_socket_after_real_persistence_recovers_by_readonly_status()
{
    use crate::distribution_range_recovery::{FrozenNativeReadyStage, NativeCompletionStage};
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    for name in ["n0", "n1"] {
        fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
    }
    let barrier = fixture.runtimes[&node("n2")].test_install_completion_reply_barrier();
    let client = Arc::clone(&fixture.clients[&node("n1")]);
    let request = fixture.completion_request(&scope);
    let token = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let worker_token = token.clone();
    let worker = std::thread::spawn(move || {
        client
            .complete_native_recovery_observation(&node("n2"), &request, &worker_token)
            .map(|o| o.reply().native_serving_sha256.clone())
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while !barrier.entered() {
        if Instant::now() >= deadline {
            barrier.release();
            panic!(
                "actual completion producer never reached its post-persistence/pre-reply boundary"
            );
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // The real producer has completed and the TLS server has not sent its
    // reply. Cancel closes the ACTUAL client socket through its I/O watcher.
    let canceled_at = Instant::now();
    token.cancel();
    let result = worker.join().unwrap();
    barrier.release();
    assert!(
        result.is_err(),
        "the client must not receive a completion ACK"
    );
    assert!(
        canceled_at.elapsed() < Duration::from_secs(2),
        "client cancellation did not bound its real socket"
    );
    fixture.wait_idle();
    let observed = fixture.completion_status("n2", &scope).unwrap();
    fixture.wait_idle();
    assert_eq!(
        observed.reply().stage,
        NativeCompletionStage::CompletedFrozen
    );
    assert_eq!(observed.reply().range_index, 1);
    let retry = fixture.complete("n2", &scope).unwrap();
    fixture.wait_idle();
    assert_eq!(
        retry.reply().native_serving_sha256,
        observed.reply().native_serving_sha256
    );
    assert!(fixture.native.databases[&node("n2")]
        .read()
        .range_mutation_gate()
        .try_enter(scope.range_id)
        .is_err());
    // Real lost incoming reply, not a synthetic failure after parsing success.
    // This preserves an UNKNOWN client outcome until fresh readonly resolution;
    // no activation, new-write, StrongGet or physical-fault qualification.
}

#[cfg(test)]
mod activation_smoke;

#[cfg(test)]
mod activation_tests;

#[cfg(test)]
mod activation_rplusone;

mod native_normal_tls;

#[cfg(test)]
mod native_read_observation_tests;

mod host_metadata_tests;
#[cfg(test)]
mod native_read_rpc_tests;

#[cfg(test)]
mod native_strong_point_tests;
