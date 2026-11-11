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
    ["n0", "n1", "n2"]
        .into_iter()
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
}
impl RangeWriteTransport for ObservedTlsTransport {
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
    _keys: tempfile::TempDir,
}
impl NetworkFixture {
    fn new(wired: bool) -> Self {
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
        let native = ActualNativeRf3Fixture::new_with_network_precondition(&network);
        native.append("actual-tls-historical").unwrap();
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
        topology.generation += 1;
        let range = topology.ranges.values_mut().next().unwrap();
        range.epoch += 1;
        range.leader = node("n1");
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
        let clients = ["n1", "n2"]
            .into_iter()
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
        // Actual persisted current-term metadata anchor, with n2 the leader.
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
        let mut leader = open(&node("n2"));
        let mut follower = open(&node("n1"));
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([node("n1"), node("n2")]))
            .unwrap();
        let response = follower
            .handle_append_request(leader.append_request_from(1, 16).unwrap())
            .unwrap();
        leader
            .record_append_response(&node("n1"), &response)
            .unwrap();
        let consensus = BTreeMap::from([
            (node("n1"), Arc::new(Mutex::new(follower))),
            (node("n2"), Arc::new(Mutex::new(leader))),
        ]);
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
        let runtimes = ["n1", "n2"]
            .into_iter()
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
                    }))
                } else {
                    runtime
                };
                (id, Arc::new(runtime))
            })
            .collect::<BTreeMap<_, _>>();
        // n0's reserved endpoint is closed and NEVER hosted a recovery server.
        // A+B remain a live data and metadata majority; no physical-host claim.
        drop(listeners);
        let mut servers = Vec::new();
        for name in ["n1", "n2"] {
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
        for name in ["n1", "n2"] {
            let observation = self.clients[&node("n1")]
                .recovery_epoch_fence_observation(
                    &node(name),
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
