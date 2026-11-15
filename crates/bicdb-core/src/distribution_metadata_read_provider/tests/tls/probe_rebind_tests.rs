//! Real mTLS probes only. The wrapper retains every actual TCP result; bounded
//! channel scheduling cannot generate an ACK, change a context or renew time.
use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cut {
    Rebind,
    Cancel,
}
#[derive(Clone, Debug)]
struct Seen {
    node: ClusterNodeId,
    attempt: usize,
    context: Uuid,
    read_request: Uuid,
    read_index: u64,
    started: Instant,
    socket_returned: Instant,
    delegated_returned: Instant,
    deadline: Option<Instant>,
    error: Option<io::ErrorKind>,
    error_class: Option<std::mem::Discriminant<BicDbError>>,
    ack_success: bool,
    gate_released: bool,
}
#[derive(Debug)]
struct Notice {
    node: ClusterNodeId,
    attempt: usize,
    error: Option<io::ErrorKind>,
    ack_success: bool,
    release: Option<mpsc::SyncSender<()>>,
}
#[derive(Debug)]
struct Recording {
    actual: Arc<TcpClusterRelocationTransport>,
    notices: mpsc::SyncSender<Notice>,
    completed: mpsc::SyncSender<()>,
    counts: Mutex<BTreeMap<ClusterNodeId, usize>>,
    seen: Mutex<Vec<Seen>>,
    overflow: AtomicBool,
    cut: Cut,
}
impl MetadataReadProbeTransport for Recording {
    fn local_participant(&self) -> MetadataReadParticipant {
        self.actual.local_metadata_read_participant()
    }
    fn probe(
        &self,
        destination: &ClusterNodeId,
        request: MetadataReadProbeRequest,
        token: &CancellationToken,
    ) -> Result<MetadataReadAck> {
        let attempt = {
            let mut counts = self.counts.lock();
            let count = counts.entry(destination.clone()).or_default();
            *count += 1;
            *count
        };
        let context = request.context_id;
        let read_request = request.read_request_id;
        let read_index = request.read_index;
        let started = Instant::now();
        let result = self.actual.probe(destination, request, token);
        let socket_returned = Instant::now();
        let error = match &result {
            Err(BicDbError::Io(error)) => Some(error.kind()),
            _ => None,
        };
        let ack_success = result
            .as_ref()
            .is_ok_and(|ack| ack.test_probe_response_data().append.success);
        let selected = destination == &node("n3") || destination == &node("n4");
        let held_attempts = if self.cut == Cut::Rebind { 3 } else { 2 };
        let gate = selected
            && attempt <= held_attempts
            && matches!(
                error,
                Some(io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused)
            );
        let mut gate_released = !gate;
        if gate {
            let (release, released) = mpsc::sync_channel::<()>(1);
            let sent = self
                .notices
                .try_send(Notice {
                    node: destination.clone(),
                    attempt,
                    error,
                    ack_success,
                    release: Some(release),
                })
                .is_ok();
            while sent && token.check().is_ok() {
                match released.recv_timeout(Duration::from_millis(2)) {
                    Ok(()) => {
                        gate_released = true;
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        } else {
            let _ = self.notices.try_send(Notice {
                node: destination.clone(),
                attempt,
                error,
                ack_success,
                release: None,
            });
        }
        // Pacing is measured from THIS return boundary, separately from the
        // earlier real socket return: a held channel cannot fake the delay.
        let delegated_returned = Instant::now();
        let mut seen = self.seen.lock();
        if seen.len() < 16 {
            seen.push(Seen {
                node: destination.clone(),
                attempt,
                context,
                read_request,
                read_index,
                started,
                socket_returned,
                delegated_returned,
                deadline: token.deadline(),
                error,
                error_class: result.as_ref().err().map(std::mem::discriminant),
                ack_success,
                gate_released,
            });
        } else {
            self.overflow.store(true, Ordering::SeqCst);
        }
        drop(seen);
        let _ = self.completed.try_send(());
        result
    }
}

struct Network {
    topology: ClusterTopology,
    identities: BTreeMap<ClusterNodeId, Identity>,
    consensus: BTreeMap<ClusterNodeId, Arc<Mutex<MetadataConsensusStore>>>,
    services: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    servers: BTreeMap<ClusterNodeId, ClusterDataServerHandle>,
    caller: TcpClusterRelocationTransport,
    provider: Arc<MetadataReadProvider>,
    recording: Arc<Recording>,
    notices: mpsc::Receiver<Notice>,
    completed: mpsc::Receiver<()>,
    // Keep files alive until every actual store/service/client field drops.
    _root: tempfile::TempDir,
}
fn four_identities(root: &Path) -> BTreeMap<ClusterNodeId, Identity> {
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = params.self_signed(&ca_key).unwrap();
    let ca_path = root.join("ca.pem");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    ["n1", "n2", "n3", "n4"]
        .into_iter()
        .map(|name| {
            let key = KeyPair::generate().unwrap();
            let mut params =
                CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
            params.is_ca = IsCa::ExplicitNoCa;
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
            let identity = Identity {
                cert: root.join(format!("{name}.pem")),
                key: root.join(format!("{name}.key")),
                ca: ca_path.clone(),
            };
            std::fs::write(&identity.cert, cert.pem()).unwrap();
            std::fs::write(&identity.key, key.serialize_pem()).unwrap();
            (node(name), identity)
        })
        .collect()
}
impl Network {
    fn new(cut: Cut) -> Self {
        let root = tempfile::tempdir().unwrap();
        let identities = four_identities(root.path());
        let mut topology = topology_with_count(4);
        let ports = ["n1", "n2", "n3", "n4"]
            .into_iter()
            .map(|name| (node(name), TcpListener::bind("127.0.0.1:0").unwrap()))
            .collect::<BTreeMap<_, _>>();
        for (id, listener) in &ports {
            let member = topology.nodes.get_mut(id).unwrap();
            member.address = listener.local_addr().unwrap().to_string();
            member.tls_certificate_sha256 = Some(identities[id].pin());
        }
        topology.validate().unwrap();
        assert_eq!(
            topology.metadata_voters().len(),
            4,
            "actual four-voter prerequisite"
        );
        let mut stores = ["n1", "n2", "n3", "n4"]
            .into_iter()
            .map(|name| {
                (
                    node(name),
                    open(
                        &root.path().join(format!("consensus-{name}")),
                        name,
                        &topology,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut leader = stores.remove(&node("n1")).unwrap();
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([node("n1"), node("n2"), node("n3")]))
            .unwrap();
        for name in ["n2", "n3", "n4"] {
            let response = stores
                .get_mut(&node(name))
                .unwrap()
                .handle_append_request(leader.append_request_from(1, 16).unwrap())
                .unwrap();
            leader
                .record_append_response(&node(name), &response)
                .unwrap();
        }
        let commit = leader.status().commit_index;
        assert!(commit > 0);
        for name in ["n2", "n3", "n4"] {
            let response = stores
                .get_mut(&node(name))
                .unwrap()
                .handle_append_request(leader.append_request_from(commit + 1, 16).unwrap())
                .unwrap();
            leader
                .record_append_response(&node(name), &response)
                .unwrap();
        }
        stores.insert(node("n1"), leader);
        let consensus = stores
            .into_iter()
            .map(|(id, store)| (id, Arc::new(Mutex::new(store))))
            .collect::<BTreeMap<_, _>>();
        let shared = Arc::new(RwLock::new(topology.clone()));
        let config = |id: &ClusterNodeId| ClusterNetworkTransportConfig {
            connect_timeout_ms: 100,
            io_timeout_ms: 1500,
            max_frame_bytes: 4 * 1024 * 1024,
            max_inbound_connections: 8,
            dev_localhost_plaintext: false,
            tls: Some(identities[id].tls()),
        };
        let actual = Arc::new(
            TcpClusterRelocationTransport::new(
                topology.cluster_id.clone(),
                node("n1"),
                shared.clone(),
                config(&node("n1")),
            )
            .unwrap(),
        );
        let caller = TcpClusterRelocationTransport::new(
            topology.cluster_id.clone(),
            node("n2"),
            shared,
            config(&node("n2")),
        )
        .unwrap();
        let (notices_tx, notices) = mpsc::sync_channel(16);
        let (completed_tx, completed) = mpsc::sync_channel(16);
        let recording = Arc::new(Recording {
            actual,
            notices: notices_tx,
            completed: completed_tx,
            counts: Mutex::new(BTreeMap::new()),
            seen: Mutex::new(Vec::new()),
            overflow: AtomicBool::new(false),
            cut,
        });
        let provider = Arc::new(
            MetadataReadProvider::new(consensus[&node("n1")].clone(), recording.clone()).unwrap(),
        );
        let services = ["n1", "n2", "n3", "n4"]
            .into_iter()
            .map(|name| {
                let db = Arc::new(RwLock::new(
                    BicDb::open_with_config(
                        root.path().join(format!("db-{name}")),
                        DbConfig::default().with_fsync(false),
                    )
                    .unwrap(),
                ));
                let service = ClusterDataNodeService::open(
                    topology.cluster_id.clone(),
                    node(name),
                    root.path().join(format!("service-{name}")),
                    db,
                    false,
                )
                .unwrap();
                (node(name), Arc::new(Mutex::new(service)))
            })
            .collect();
        drop(ports);
        let mut network = Self {
            _root: root,
            topology,
            identities,
            consensus,
            services,
            servers: BTreeMap::new(),
            caller,
            provider,
            recording,
            notices,
            completed,
        };
        for name in ["n1", "n2", "n3", "n4"] {
            network.start(&node(name)).unwrap();
        }
        // Actual listeners have existed. Close them while retaining SAME stores,
        // services, identities, addresses and committed configuration.
        for name in ["n3", "n4"] {
            network.servers.remove(&node(name)).unwrap().join().unwrap();
        }
        network
    }
    fn start(&mut self, id: &ClusterNodeId) -> Result<()> {
        let config = ClusterNetworkTransportConfig {
            connect_timeout_ms: 100,
            io_timeout_ms: 1500,
            max_frame_bytes: 4 * 1024 * 1024,
            max_inbound_connections: 8,
            dev_localhost_plaintext: false,
            tls: Some(self.identities[id].tls()),
        };
        let handle = start_cluster_data_server(
            &self.topology.nodes[id].address,
            config,
            Arc::new(RwLock::new(self.topology.clone())),
            self.services[id].clone(),
            None,
            Some(self.consensus[id].clone()),
            None,
        )?;
        self.servers.insert(id.clone(), handle);
        Ok(())
    }
}
impl Network {
    fn close(&mut self) -> Vec<BicDbError> {
        let mut errors = Vec::new();
        for (_, server) in std::mem::take(&mut self.servers) {
            if let Err(error) = server.join() {
                errors.push(error);
            }
        }
        errors
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        // Last-resort unwind cleanup cannot replace an original panic. Normal
        // execution retains and asserts every cleanup result below.
        for error in self.close() {
            eprintln!("ACTUAL_METADATA_REBIND_CLEANUP error={error:?}");
        }
    }
}
struct CancelOnExit(CancellationToken);
impl Drop for CancelOnExit {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[test]
fn actual_mtls_original_context_paces_socket_failures_then_rebinds_same_two_voters() {
    run(Cut::Rebind);
}
#[test]
fn actual_mtls_original_context_cancel_after_paced_socket_retry_refuses_quorum() {
    run(Cut::Cancel);
}

fn run(cut: Cut) {
    let mut network = Network::new(cut);
    assert_eq!(network.provider.limits.max_attempts_per_voter, 4);
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_millis(1500));
    let deadline = parent.deadline().unwrap();
    let request = MetadataReadAcquireRequest {
        protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
        acquisition_id: Uuid::now_v7(),
        read_request_id: Uuid::now_v7(),
        requester_node_id: node("n2"),
        requester_incarnation: network.topology.nodes[&node("n2")].incarnation,
        range_id: RangeId::new(1).unwrap(),
        range_epoch: network
            .topology
            .range_by_id(RangeId::new(1).unwrap())
            .unwrap()
            .epoch,
        budget_ms: 1500,
    };
    let provider = network.provider.clone();
    let caller = network.caller.local_metadata_read_participant();
    let original = request.clone();
    let actual_parent = parent.clone();
    let mut original_parent_live_before_cut = None;
    let (controller, joined) = thread::scope(|scope| {
        let acquisition = scope.spawn(move || provider.acquire(&caller, original, &actual_parent));
        // On early unwind, cancel before scope's automatic join. Normally retain
        // the original parent until the actual API has returned.
        let _cancel = CancelOnExit(parent.clone());
        let controller = (|| -> Result<()> {
            let hold = if cut == Cut::Rebind { 3 } else { 2 };
            let mut waiting = BTreeMap::new();
            let mut healthy_ack = false;
            while waiting.len() != 2 || !healthy_ack {
                parent.check()?;
                let notice = match network.notices.recv_timeout(Duration::from_millis(2)) {
                    Ok(notice) => notice,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(acquisition_error("actual probe notice channel ended"))
                    }
                };
                if notice.node == node("n2") {
                    healthy_ack |= notice.ack_success;
                }
                if let Some(release) = notice.release {
                    if notice.attempt == hold {
                        waiting.insert(notice.node, release);
                    } else {
                        let _ = release.send(());
                    }
                } else if notice.error.is_some() {
                    return Err(acquisition_error(
                        "unexpected actual socket result before selected rebind cut",
                    ));
                }
            }
            original_parent_live_before_cut = Some(parent.check().is_ok());
            parent.check()?;
            if cut == Cut::Rebind {
                network.start(&node("n3"))?;
                network.start(&node("n4"))?;
                parent.check()?;
                for (_, release) in waiting {
                    let _ = release.send(());
                }
            } else if cut == Cut::Cancel {
                parent.cancel();
            }
            Ok(())
        })();
        if controller.is_err() {
            parent.cancel();
        }
        (controller, acquisition.join())
    });
    // Preserve original outcome before any cleanup wait/read. Provider workers
    // are detached; sibling cancellation may be observed a little later.
    eprintln!("ACTUAL_METADATA_SOCKET_ORIGINAL_RETURN cut={cut:?} original_parent_live_before_cut={original_parent_live_before_cut:?} controller={controller:?} original={:?}", joined.as_ref().map(|result| result.as_ref().err()));
    let completion = (|| -> std::result::Result<(), &'static str> {
        let until = Instant::now() + Duration::from_secs(2);
        let required = if cut == Cut::Rebind { 3 } else { 2 };
        loop {
            let all_required_recorded = {
                let seen = network.recording.seen.lock();
                [node("n3"), node("n4")]
                    .iter()
                    .all(|id| seen.iter().filter(|event| event.node == *id).count() >= required)
            };
            if all_required_recorded {
                break Ok(());
            }
            let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                break Err("required original probe-result callbacks did not finish");
            };
            match network
                .completed
                .recv_timeout(remaining.min(Duration::from_millis(10)))
            {
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break Err("original probe-result channel ended")
                }
            }
        }
    })();
    let events = network.recording.seen.lock().clone();
    let cleanup_errors = network.close();
    eprintln!("ACTUAL_METADATA_REBIND_CLEANUP results={cleanup_errors:?}");
    eprintln!("ACTUAL_METADATA_SOCKET_REBIND cut={cut:?} request={request:?} deadline={deadline:?} controller={controller:?} original={:?} events={events:?}", joined.as_ref().map(|result| result.as_ref().err()));
    assert!(
        controller.is_ok(),
        "required actual failure/rebind cut not reached: {controller:?}"
    );
    assert_eq!(
        original_parent_live_before_cut,
        Some(true),
        "expiry is an unreached live rebind/cancel cut"
    );
    let result = joined.expect("actual provider worker panicked");
    assert!(
        completion.is_ok(),
        "required actual original results: {completion:?}"
    );
    assert!(!network.recording.overflow.load(Ordering::SeqCst));
    assert!(!events.is_empty());
    let context = events[0].context;
    let index = events[0].read_index;
    for event in &events {
        assert_eq!(event.context, context);
        assert_eq!(event.read_request, request.read_request_id);
        assert_eq!(event.read_index, index);
        assert!(event.deadline.is_some_and(|d| d <= deadline));
        assert!(
            event.started <= event.socket_returned
                && event.socket_returned <= event.delegated_returned
        );
        assert!(event.attempt <= 4, "original attempt cap unchanged");
        if event.error.is_some() {
            assert!(event.error_class.is_some());
        }
    }
    for id in [node("n3"), node("n4")] {
        let mut selected = events.iter().filter(|e| e.node == id).collect::<Vec<_>>();
        selected.sort_by_key(|e| e.attempt);
        let required = if cut == Cut::Rebind { 3 } else { 2 };
        assert!(selected.len() >= required);
        for pair in selected.windows(2) {
            assert!(matches!(
                pair[0].error,
                Some(io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused)
            ));
            let delay = match pair[0].attempt {
                1 => 5,
                2 => 10,
                3 => 20,
                _ => panic!("unexpected extra retry"),
            };
            assert!(
                pair[1].started.duration_since(pair[0].delegated_returned)
                    >= Duration::from_millis(delay),
                "actual retry must be paced AFTER delegate returned; channel hold is not the delay"
            );
            assert!(pair[0].gate_released);
        }
    }
    match cut {
        Cut::Rebind => {
            let response =
                result.expect("actual restored TLS majority must finish original context");
            response.validate(&request).unwrap();
            assert_eq!(response.context_id, context);
            assert_eq!(response.old_voters.len(), 4);
            assert_eq!(response.new_voters.len(), 4);
            assert!(response.fresh_old_voters.len() >= 3 && response.fresh_new_voters.len() >= 3);
            assert!(
                response.fresh_old_voters.contains(&node("n1"))
                    && response.fresh_old_voters.contains(&node("n2"))
            );
            assert!(events
                .iter()
                .any(|e| (e.node == node("n3") || e.node == node("n4"))
                    && e.attempt == 4
                    && e.ack_success));
        }
        Cut::Cancel => assert!(
            matches!(&result, Err(BicDbError::QueryCanceled)),
            "{result:?}"
        ),
    }
    assert!(
        cleanup_errors.is_empty(),
        "actual listener cleanup errors: {cleanup_errors:?}"
    );
}
