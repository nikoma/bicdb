//! Fresh metadata-leader acquisition, separate from range-image/read visibility.
//! No caller-supplied DTO can mint a local MetadataReadAuthority. Network work
//! runs without consensus/database/service latches. Limits bound outstanding
//! contexts, worker threads, queues, voter count, retries and serialized state.
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard};

use crate::distribution::ClusterNodeId;
use crate::distribution_consensus::{
    MetadataConsensusStore, MetadataReadAck, MetadataReadAcquireRequest,
    MetadataReadAcquireResponse, MetadataReadContext, MetadataReadParticipant,
    MetadataReadProbeRequest,
};
use crate::error::{BicDbError, Result};
use crate::CancellationToken;

fn acquisition_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("metadata read acquisition: {}", message.into()))
}

pub trait MetadataReadProbeTransport: Send + Sync + std::fmt::Debug + 'static {
    fn local_participant(&self) -> MetadataReadParticipant;
    fn probe(
        &self,
        destination: &ClusterNodeId,
        request: MetadataReadProbeRequest,
        cancellation: &CancellationToken,
    ) -> Result<MetadataReadAck>;
}

#[derive(Clone, Debug)]
pub struct MetadataReadProviderLimits {
    pub max_inflight_contexts: usize,
    pub workers_per_context: usize,
    pub max_voters: usize,
    pub max_attempts_per_voter: usize,
    pub max_payload_bytes: usize,
}

impl Default for MetadataReadProviderLimits {
    fn default() -> Self {
        Self {
            max_inflight_contexts: 4,
            workers_per_context: 4,
            max_voters: 64,
            max_attempts_per_voter: 4,
            max_payload_bytes: 2 * 1024 * 1024,
        }
    }
}

impl MetadataReadProviderLimits {
    fn validate(&self) -> Result<()> {
        if !(1..=4).contains(&self.max_inflight_contexts)
            || !(1..=4).contains(&self.workers_per_context)
            || !(1..=64).contains(&self.max_voters)
            || !(1..=8).contains(&self.max_attempts_per_voter)
            || !(256 * 1024..=2 * 1024 * 1024).contains(&self.max_payload_bytes)
        {
            return Err(acquisition_error("provider limits exceed hard bounds"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MetadataReadProvider {
    consensus: Arc<Mutex<MetadataConsensusStore>>,
    transport: Arc<dyn MetadataReadProbeTransport>,
    limits: MetadataReadProviderLimits,
    inflight: Arc<AtomicUsize>,
}

struct ContextPermit(Arc<AtomicUsize>);
impl Drop for ContextPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
struct CancelOnReturn(CancellationToken);
impl Drop for CancelOnReturn {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct ProbeJob {
    destination: ClusterNodeId,
    request: MetadataReadProbeRequest,
}
struct Candidate {
    destination: ClusterNodeId,
    next_index: u64,
    attempts: usize,
}

impl MetadataReadProvider {
    pub fn new(
        consensus: Arc<Mutex<MetadataConsensusStore>>,
        transport: Arc<dyn MetadataReadProbeTransport>,
    ) -> Result<Self> {
        Self::with_limits(consensus, transport, MetadataReadProviderLimits::default())
    }

    pub fn with_limits(
        consensus: Arc<Mutex<MetadataConsensusStore>>,
        transport: Arc<dyn MetadataReadProbeTransport>,
        limits: MetadataReadProviderLimits,
    ) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            consensus,
            transport,
            limits,
            inflight: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn inflight_contexts(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }

    fn lock_consensus(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<MutexGuard<'_, MetadataConsensusStore>> {
        loop {
            cancellation.check()?;
            if let Some(guard) = self.consensus.try_lock_for(Duration::from_millis(10)) {
                cancellation.check()?;
                return Ok(guard);
            }
        }
    }

    fn prepare_job(
        &self,
        context: &MetadataReadContext,
        candidate: &Candidate,
        cancellation: &CancellationToken,
    ) -> Result<ProbeJob> {
        let consensus = self.lock_consensus(cancellation)?;
        let request = consensus.bounded_read_authority_probe(
            context,
            &candidate.destination,
            candidate.next_index,
            self.limits.max_payload_bytes,
        )?;
        Ok(ProbeJob {
            destination: candidate.destination.clone(),
            request,
        })
    }

    /// Called only with the actual inbound authenticated member participant.
    /// Finite network deadlines do not make an OS metadata fsync preemptible.
    pub(crate) fn acquire(
        &self,
        caller: &MetadataReadParticipant,
        request: MetadataReadAcquireRequest,
        parent: &CancellationToken,
    ) -> Result<MetadataReadAcquireResponse> {
        request.validate()?;
        parent.check()?;
        self.inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limits.max_inflight_contexts).then_some(active + 1)
            })
            .map_err(|_| acquisition_error("bounded acquisition capacity is busy"))?;
        let permit = Arc::new(ContextPermit(Arc::clone(&self.inflight)));
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        let _cancel_on_return = CancelOnReturn(token.clone());
        let (mut context, range) = {
            let consensus = self.lock_consensus(&token)?;
            consensus.read_authority_payload_fits(self.limits.max_payload_bytes - 128 * 1024)?;
            let range = consensus.validate_read_acquisition_caller(&request, caller)?;
            let budget = token
                .deadline()
                .unwrap()
                .checked_duration_since(Instant::now())
                .ok_or_else(|| acquisition_error("deadline expired before begin"))?;
            let mut context = consensus.begin_read_authority(
                request.read_request_id,
                budget,
                &self.transport.local_participant(),
            )?;
            context.restrict_deadline(token.deadline().unwrap());
            (context, range)
        };
        let voters = context.remote_voters();
        if voters.len() + 1 > self.limits.max_voters {
            return Err(acquisition_error(
                "metadata voter set exceeds bounded fanout",
            ));
        }
        let token = token.child_with_deadline(context.deadline());
        let mut candidates = voters
            .into_iter()
            .map(|destination| Candidate {
                destination,
                next_index: context.read_index().saturating_add(1),
                attempts: 0,
            })
            .collect::<VecDeque<_>>();
        let workers = self.limits.workers_per_context.min(candidates.len());
        let (jobs_tx, jobs_rx) = mpsc::sync_channel::<ProbeJob>(workers.max(1));
        let jobs_rx = Arc::new(Mutex::new(jobs_rx));
        let (results_tx, results_rx) = mpsc::sync_channel(workers.max(1));
        for _ in 0..workers {
            let jobs = Arc::clone(&jobs_rx);
            let results = results_tx.clone();
            let transport = Arc::clone(&self.transport);
            let token = token.clone();
            let permit = Arc::clone(&permit);
            thread::Builder::new()
                .name("metadata-read-probe".to_string())
                .spawn(move || {
                    let _permit = permit;
                    while token.check().is_ok() {
                        let next = jobs.lock().recv_timeout(Duration::from_millis(10));
                        match next {
                            Ok(job) => {
                                if token.check().is_err() {
                                    break;
                                }
                                let result = transport.probe(&job.destination, job.request, &token);
                                if results.send((job.destination, result)).is_err() {
                                    break;
                                }
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                })?;
        }
        drop(results_tx);
        let mut outstanding = BTreeMap::new();
        loop {
            token.check()?;
            {
                let consensus = self.lock_consensus(&token)?;
                if consensus.read_authority_ready(&context)? {
                    if consensus.validate_read_acquisition_caller(&request, caller)? != range {
                        return Err(acquisition_error(
                            "range configuration changed during fresh quorum round",
                        ));
                    }
                    consensus
                        .read_authority_payload_fits(self.limits.max_payload_bytes - 128 * 1024)?;
                    let response = consensus
                        .finish_read_authority(context)?
                        .into_acquisition_response(request)?;
                    let mut counter = SizeBound(self.limits.max_payload_bytes);
                    serde_json::to_writer(&mut counter, &response)
                        .map_err(|_| acquisition_error("completed proof exceeds byte bound"))?;
                    token.check()?;
                    // Sibling cancellation happens on return, without joining a
                    // silent voter and allowing its timeout to expire this proof.
                    return Ok(response);
                }
            }
            while outstanding.len() < workers {
                let Some(mut candidate) = candidates.pop_front() else {
                    break;
                };
                let job = self.prepare_job(&context, &candidate, &token)?;
                candidate.attempts += 1;
                jobs_tx
                    .try_send(job)
                    .map_err(|_| acquisition_error("bounded probe queue invariant failed"))?;
                outstanding.insert(candidate.destination.clone(), candidate);
            }
            if outstanding.is_empty() {
                return Err(acquisition_error(
                    "fresh joint quorum unavailable within bounded probe attempts",
                ));
            }
            match results_rx.recv_timeout(Duration::from_millis(10)) {
                Ok((voter, result)) => {
                    let mut candidate = outstanding
                        .remove(&voter)
                        .ok_or_else(|| acquisition_error("unexpected probe worker result"))?;
                    if let Ok(ack) = result {
                        let accepted = self
                            .lock_consensus(&token)?
                            .record_read_authority_ack(&mut context, &ack)?;
                        if !accepted && candidate.attempts < self.limits.max_attempts_per_voter {
                            candidate.next_index = ack
                                .conflict_index()
                                .unwrap_or(1)
                                .max(1)
                                .min(context.read_index().saturating_add(1));
                            candidates.push_back(candidate);
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(acquisition_error("probe workers disconnected"))
                }
            }
        }
    }
}

struct SizeBound(usize);
impl Write for SizeBound {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metadata proof byte bound",
            ));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_consensus::METADATA_READ_ACQUISITION_PROTOCOL_VERSION;
    use crate::{
        ClusterId, ClusterNode, ClusterTopology, DistributionConfig, DistributionStore, RangeId,
    };
    use std::collections::BTreeSet;
    use uuid::Uuid;

    fn node(name: &str) -> ClusterNodeId {
        ClusterNodeId::new(name).unwrap()
    }
    fn participant(topology: &ClusterTopology, name: &str) -> MetadataReadParticipant {
        let id = node(name);
        MetadataReadParticipant::from_transport(
            id.clone(),
            topology.nodes[&id].incarnation,
            None,
            true,
        )
    }
    fn topology() -> ClusterTopology {
        let root = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("metadata-provider-test").unwrap(),
            node_id: node("n1"),
            node_address: "127.0.0.1:10001".into(),
            replication_factor: 1,
            initial_ranges: 1,
            ..DistributionConfig::default()
        };
        let mut store = DistributionStore::initialize_at(root.path(), config, false, 10).unwrap();
        for i in 2..=3 {
            store
                .join_node(
                    ClusterNode::new(
                        node(&format!("n{i}")),
                        format!("127.0.0.1:{}", 10000 + i),
                        1,
                        1_000_000,
                        10 + i,
                    )
                    .unwrap(),
                    &node("n1"),
                    10 + i,
                )
                .unwrap();
        }
        let mut topology = store.topology().clone();
        let range = topology.ranges.values_mut().next().unwrap();
        range.leader = node("n2");
        range.replicas[0].node_id = node("n2");
        topology.validate().unwrap();
        topology
    }
    fn open(
        root: &std::path::Path,
        name: &str,
        topology: &ClusterTopology,
    ) -> MetadataConsensusStore {
        MetadataConsensusStore::open(
            root,
            topology.cluster_id.clone(),
            node(name),
            topology.clone(),
            false,
        )
        .unwrap()
    }
    #[derive(Debug)]
    struct FakeTransport {
        topology: ClusterTopology,
        follower: Arc<Mutex<MetadataConsensusStore>>,
        leader: Arc<Mutex<MetadataConsensusStore>>,
        mode: AtomicUsize,
        stalled_started: AtomicUsize,
    }
    impl MetadataReadProbeTransport for FakeTransport {
        fn local_participant(&self) -> MetadataReadParticipant {
            participant(&self.topology, "n1")
        }
        fn probe(
            &self,
            destination: &ClusterNodeId,
            request: MetadataReadProbeRequest,
            token: &CancellationToken,
        ) -> Result<MetadataReadAck> {
            if *destination == node("n3") {
                self.stalled_started.fetch_add(1, Ordering::SeqCst);
                loop {
                    token.check()?;
                    thread::sleep(Duration::from_millis(2));
                }
            }
            match self.mode.load(Ordering::SeqCst) {
                1 => {
                    self.leader
                        .lock()
                        .observe_remote_term(request.append.term + 1)?;
                }
                2 => {
                    let mut leader = self.leader.lock();
                    let mut changed = leader.committed_topology().clone();
                    changed.generation += 1;
                    changed.nodes.get_mut(&node("n3")).unwrap().incarnation += 1;
                    leader.propose_topology(changed)?;
                }
                _ => {}
            }
            let response = self
                .follower
                .lock()
                .handle_read_authority_probe(request.clone())?;
            MetadataReadAck::from_transport(
                &request,
                destination,
                participant(&self.topology, "n2"),
                response,
            )
        }
    }
    struct Fixture {
        _root: tempfile::TempDir,
        topology: ClusterTopology,
        transport: Arc<FakeTransport>,
        provider: MetadataReadProvider,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let topology = topology();
            let mut leader = open(&root.path().join("n1"), "n1", &topology);
            let mut follower = open(&root.path().join("n2"), "n2", &topology);
            leader.start_election().unwrap();
            leader
                .become_leader(&BTreeSet::from([node("n1"), node("n2")]))
                .unwrap();
            let response = follower
                .handle_append_request(leader.append_request_from(1, 16).unwrap())
                .unwrap();
            leader
                .record_append_response(&node("n2"), &response)
                .unwrap();
            let leader = Arc::new(Mutex::new(leader));
            let transport = Arc::new(FakeTransport {
                topology: topology.clone(),
                follower: Arc::new(Mutex::new(follower)),
                leader: leader.clone(),
                mode: AtomicUsize::new(0),
                stalled_started: AtomicUsize::new(0),
            });
            let provider = MetadataReadProvider::new(leader, transport.clone()).unwrap();
            Self {
                _root: root,
                topology,
                transport,
                provider,
            }
        }
        fn request(&self) -> MetadataReadAcquireRequest {
            MetadataReadAcquireRequest {
                protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
                acquisition_id: Uuid::now_v7(),
                read_request_id: Uuid::now_v7(),
                requester_node_id: node("n2"),
                requester_incarnation: self.topology.nodes[&node("n2")].incarnation,
                range_id: RangeId::new(1).unwrap(),
                range_epoch: self
                    .topology
                    .range_by_id(RangeId::new(1).unwrap())
                    .unwrap()
                    .epoch,
                budget_ms: 1000,
            }
        }
        fn acquire(
            &self,
            request: MetadataReadAcquireRequest,
        ) -> Result<MetadataReadAcquireResponse> {
            self.provider.acquire(
                &participant(&self.topology, "n2"),
                request,
                &CancellationToken::uncancelable(),
            )
        }
    }
    #[test]
    fn fresh_quorum_returns_without_waiting_for_silent_voter() {
        let fixture = Fixture::new();
        let started = Instant::now();
        let request = fixture.request();
        let response = fixture.acquire(request.clone()).unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(response.metadata_leader_id, node("n1"));
        assert_eq!(request.requester_node_id, node("n2"));
        assert_eq!(
            response.fresh_old_voters,
            BTreeSet::from([node("n1"), node("n2")])
        );
        let observation = crate::MetadataReadObservation::from_transport(
            &request,
            &fixture.topology.cluster_id,
            &node("n1"),
            participant(&fixture.topology, "n1"),
            response,
            started + Duration::from_secs(1),
            started,
        )
        .unwrap();
        assert!(observation
            .committed_topology(request.read_request_id)
            .is_ok());
        assert!(observation.committed_topology(Uuid::now_v7()).is_err());
        let drain = Instant::now() + Duration::from_secs(1);
        while fixture.provider.inflight_contexts() != 0 && Instant::now() < drain {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(fixture.provider.inflight_contexts(), 0);
    }
    #[test]
    fn leadership_or_joint_configuration_change_invalidates_round() {
        for mode in [1, 2] {
            let fixture = Fixture::new();
            fixture.transport.mode.store(mode, Ordering::SeqCst);
            assert!(fixture.acquire(fixture.request()).is_err());
        }
    }
    #[test]
    fn stale_epoch_wrong_caller_and_expired_parent_refuse() {
        let fixture = Fixture::new();
        let mut request = fixture.request();
        request.range_epoch += 1;
        assert!(fixture.acquire(request).is_err());
        assert!(fixture
            .provider
            .acquire(
                &participant(&fixture.topology, "n3"),
                fixture.request(),
                &CancellationToken::uncancelable()
            )
            .is_err());
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() - Duration::from_millis(1));
        assert!(fixture
            .provider
            .acquire(
                &participant(&fixture.topology, "n2"),
                fixture.request(),
                &parent
            )
            .is_err());
        assert_eq!(fixture.provider.inflight_contexts(), 0);
    }
    #[test]
    fn response_nonce_quorum_identity_and_deadline_cannot_be_reused() {
        let fixture = Fixture::new();
        let request = fixture.request();
        let response = fixture.acquire(request.clone()).unwrap();
        let check = |request: &MetadataReadAcquireRequest,
                     response: MetadataReadAcquireResponse,
                     started: Instant| {
            crate::MetadataReadObservation::from_transport(
                request,
                &fixture.topology.cluster_id,
                &node("n1"),
                participant(&fixture.topology, "n1"),
                response,
                Instant::now() + Duration::from_secs(5),
                started,
            )
        };
        let mut changed = request.clone();
        changed.acquisition_id = Uuid::now_v7();
        assert!(check(&changed, response.clone(), Instant::now()).is_err());
        let mut changed = response.clone();
        changed.fresh_new_voters.clear();
        assert!(check(&request, changed, Instant::now()).is_err());
        let mut changed = response.clone();
        changed.metadata_leader_incarnation += 1;
        assert!(check(&request, changed, Instant::now()).is_err());
        assert!(check(&request, response, Instant::now() - Duration::from_secs(2)).is_err());
    }
    #[test]
    fn limits_and_busy_admission_fail_closed() {
        let fixture = Fixture::new();
        let mut limits = MetadataReadProviderLimits::default();
        limits.workers_per_context = 5;
        assert!(MetadataReadProvider::with_limits(
            fixture.transport.leader.clone(),
            fixture.transport.clone(),
            limits
        )
        .is_err());
        fixture.provider.inflight.store(4, Ordering::SeqCst);
        assert!(fixture.acquire(fixture.request()).is_err());
        assert_eq!(fixture.provider.inflight_contexts(), 4);
    }
    #[test]
    fn finished_local_authority_keeps_original_context_deadline() {
        let fixture = Fixture::new();
        let leader = fixture.transport.leader.lock();
        let mut context = leader
            .begin_read_authority(
                Uuid::now_v7(),
                Duration::from_millis(100),
                &participant(&fixture.topology, "n1"),
            )
            .unwrap();
        let deadline = context.deadline();
        let request = leader
            .read_authority_probe(&context, &node("n2"), 1, 16)
            .unwrap();
        drop(leader);
        let ack = fixture
            .transport
            .probe(&node("n2"), request, &CancellationToken::uncancelable())
            .unwrap();
        let mut leader = fixture.transport.leader.lock();
        leader
            .record_read_authority_ack(&mut context, &ack)
            .unwrap();
        let authority = leader.finish_read_authority(context).unwrap();
        assert_eq!(authority.deadline(), deadline);
        assert!(authority.remaining_budget().unwrap() <= Duration::from_millis(100));
        drop(leader);
        thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
        assert!(!authority.is_current_budget());
        assert!(authority.remaining_budget().is_none());
    }

    #[cfg(feature = "tls-replication")]
    mod tls {
        use super::*;
        use crate::{
            start_cluster_data_server, start_cluster_data_server_with_metadata_read_provider,
            BicDb, ClusterDataNodeService, ClusterDataServerHandle, ClusterNetworkTransportConfig,
            DbConfig, ReplicationTlsConfig, TcpClusterRelocationTransport,
        };
        use parking_lot::RwLock;
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose,
        };
        use std::net::TcpListener;
        use std::path::{Path, PathBuf};

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
            ["n1", "n2", "n3"]
                .into_iter()
                .map(|name| {
                    let leaf_key = KeyPair::generate().unwrap();
                    let mut params =
                        CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
                            .unwrap();
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
        struct NetworkFixture {
            _root: tempfile::TempDir,
            _silent: TcpListener,
            servers: Vec<ClusterDataServerHandle>,
            services: Vec<Arc<Mutex<ClusterDataNodeService>>>,
            client: TcpClusterRelocationTransport,
            leader_client: Arc<TcpClusterRelocationTransport>,
            range_epoch: u64,
        }
        impl NetworkFixture {
            fn new(wired: bool) -> Self {
                let root = tempfile::tempdir().unwrap();
                let identities = identities(root.path());
                let mut topology = topology();
                let one = TcpListener::bind("127.0.0.1:0").unwrap();
                let two = TcpListener::bind("127.0.0.1:0").unwrap();
                let silent = TcpListener::bind("127.0.0.1:0").unwrap();
                for (name, listener) in [("n1", &one), ("n2", &two), ("n3", &silent)] {
                    let member = topology.nodes.get_mut(&node(name)).unwrap();
                    member.address = listener.local_addr().unwrap().to_string();
                    member.tls_certificate_sha256 = Some(identities[&node(name)].pin());
                }
                topology.validate().unwrap();
                let range_epoch = topology
                    .range_by_id(RangeId::new(1).unwrap())
                    .unwrap()
                    .epoch;
                let mut leader = open(&root.path().join("consensus-n1"), "n1", &topology);
                let mut follower = open(&root.path().join("consensus-n2"), "n2", &topology);
                leader.start_election().unwrap();
                leader
                    .become_leader(&BTreeSet::from([node("n1"), node("n2")]))
                    .unwrap();
                let response = follower
                    .handle_append_request(leader.append_request_from(1, 16).unwrap())
                    .unwrap();
                leader
                    .record_append_response(&node("n2"), &response)
                    .unwrap();
                let consensus = [Arc::new(Mutex::new(leader)), Arc::new(Mutex::new(follower))];
                let shared = Arc::new(RwLock::new(topology.clone()));
                let network = |name: &str| ClusterNetworkTransportConfig {
                    connect_timeout_ms: 100,
                    io_timeout_ms: 2000,
                    max_frame_bytes: 4 * 1024 * 1024,
                    max_inbound_connections: 8,
                    dev_localhost_plaintext: false,
                    tls: Some(identities[&node(name)].tls()),
                };
                let leader_client = Arc::new(
                    TcpClusterRelocationTransport::new(
                        topology.cluster_id.clone(),
                        node("n1"),
                        shared.clone(),
                        network("n1"),
                    )
                    .unwrap(),
                );
                let provider = Arc::new(
                    MetadataReadProvider::new(consensus[0].clone(), leader_client.clone()).unwrap(),
                );
                let client = TcpClusterRelocationTransport::new(
                    topology.cluster_id.clone(),
                    node("n2"),
                    shared.clone(),
                    network("n2"),
                )
                .unwrap();
                let mut services = Vec::new();
                let mut servers = Vec::new();
                drop(one);
                drop(two);
                for (i, name) in ["n1", "n2"].into_iter().enumerate() {
                    let db = Arc::new(RwLock::new(
                        BicDb::open_with_config(
                            root.path().join(format!("db-{name}")),
                            DbConfig::default().with_fsync(false),
                        )
                        .unwrap(),
                    ));
                    let service = Arc::new(Mutex::new(
                        ClusterDataNodeService::open(
                            topology.cluster_id.clone(),
                            node(name),
                            root.path().join(format!("service-{name}")),
                            db,
                            false,
                        )
                        .unwrap(),
                    ));
                    let address = topology.nodes[&node(name)].address.clone();
                    let server = if wired && i == 0 {
                        start_cluster_data_server_with_metadata_read_provider(
                            &address,
                            network(name),
                            shared.clone(),
                            service.clone(),
                            None,
                            Some(consensus[i].clone()),
                            None,
                            Some(provider.clone()),
                        )
                        .unwrap()
                    } else {
                        start_cluster_data_server(
                            &address,
                            network(name),
                            shared.clone(),
                            service.clone(),
                            None,
                            Some(consensus[i].clone()),
                            None,
                        )
                        .unwrap()
                    };
                    services.push(service);
                    servers.push(server);
                }
                Self {
                    _root: root,
                    _silent: silent,
                    servers,
                    services,
                    client,
                    leader_client,
                    range_epoch,
                }
            }
            fn acquire(&self) -> Result<crate::MetadataReadObservation> {
                self.client.acquire_metadata_read_observation(
                    &node("n1"),
                    Uuid::now_v7(),
                    RangeId::new(1).unwrap(),
                    self.range_epoch,
                    Duration::from_millis(1800),
                    &CancellationToken::uncancelable(),
                )
            }
        }
        impl Drop for NetworkFixture {
            fn drop(&mut self) {
                for server in self.servers.drain(..) {
                    server.join().unwrap();
                }
            }
        }
        #[test]
        fn actual_mtls_range_leader_acquires_from_different_metadata_leader_without_silent_voter_or_data_latch(
        ) {
            let fixture = NetworkFixture::new(true);
            let _one = fixture.services[0].lock();
            let _two = fixture.services[1].lock();
            let started = Instant::now();
            let observation = fixture.acquire().unwrap();
            assert!(started.elapsed() < Duration::from_millis(1400));
            assert_eq!(observation.metadata_leader_id(), &node("n1"));
            assert_eq!(observation.request().requester_node_id, node("n2"));
            assert!(observation
                .committed_topology(observation.request().read_request_id)
                .is_ok());
        }
        #[test]
        fn unwired_server_and_non_range_leader_refuse_acquisition() {
            let fixture = NetworkFixture::new(false);
            assert!(fixture.acquire().is_err());
            drop(fixture);
            let fixture = NetworkFixture::new(true);
            assert!(fixture
                .leader_client
                .acquire_metadata_read_observation(
                    &node("n1"),
                    Uuid::now_v7(),
                    RangeId::new(1).unwrap(),
                    fixture.range_epoch,
                    Duration::from_millis(500),
                    &CancellationToken::uncancelable()
                )
                .is_err());
            assert!(fixture
                .client
                .acquire_metadata_read_observation(
                    &node("n1"),
                    Uuid::now_v7(),
                    RangeId::new(1).unwrap(),
                    fixture.range_epoch + 1,
                    Duration::from_millis(500),
                    &CancellationToken::uncancelable()
                )
                .is_err());
        }
    }
}
