//! Actual mTLS + actual fsynced metadata decisions on one physical host.
//! An explicit endpoint redirection models delivery to the retained stale
//! process. No transport incarnation or quorum acknowledgement is fabricated.
use super::*;
use crate::distribution_transport::{start_cluster_data_server, TcpClusterRelocationTransport};
use crate::{ClusterNetworkTransportConfig, ReplicationTlsConfig};
use rcgen::{BasicConstraints, ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose};
use std::net::TcpListener;
use std::time::Instant;

fn network() -> (
    tempfile::TempDir,
    Vec<TcpListener>,
    BTreeMap<ClusterNodeId, (String, String)>,
    BTreeMap<ClusterNodeId, ClusterNetworkTransportConfig>,
) {
    let mut keys = tempfile::tempdir().unwrap();
    keys.disable_cleanup(true);
    eprintln!(
        "NATIVE_METADATA_DESTINATION_TLS_KEYS {}",
        keys.path().display()
    );
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = params.self_signed(&ca_key).unwrap();
    let ca_path = keys.path().join("ca.pem");
    fs::write(&ca_path, ca.pem()).unwrap();
    let sockets = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let mut facts = BTreeMap::new();
    let mut configs = BTreeMap::new();
    for i in 0..4 {
        let key = KeyPair::generate().unwrap();
        let mut params =
            CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let certificate = params.signed_by(&key, &ca, &ca_key).unwrap();
        let cert_path = keys.path().join(format!("n{i}.pem"));
        let key_path = keys.path().join(format!("n{i}.key"));
        fs::write(&cert_path, certificate.pem()).unwrap();
        fs::write(&key_path, key.serialize_pem()).unwrap();
        facts.insert(
            node(i),
            (
                sockets[i].local_addr().unwrap().to_string(),
                crate::replication_transport::replication_certificate_sha256(&cert_path).unwrap(),
            ),
        );
        configs.insert(
            node(i),
            ClusterNetworkTransportConfig {
                connect_timeout_ms: 100,
                io_timeout_ms: 1500,
                max_frame_bytes: 4 * 1024 * 1024,
                max_inbound_connections: 16,
                dev_localhost_plaintext: false,
                tls: Some(ReplicationTlsConfig {
                    cert_path,
                    key_path,
                    ca_path: ca_path.clone(),
                    require_client_cert: true,
                    dev_localhost_plaintext: false,
                }),
            },
        );
    }
    (keys, sockets, facts, configs)
}
fn replicate(
    leader: &Arc<Mutex<MetadataConsensusStore>>,
    client: &TcpClusterRelocationTransport,
    peers: &[usize],
) {
    for _ in 0..3 {
        for i in peers {
            let request = leader.lock().append_request_from(1, 32).unwrap();
            let reply = client.append_metadata(&node(*i), request).unwrap();
            assert!(reply.success);
            leader
                .lock()
                .record_append_response(&node(*i), &reply)
                .unwrap();
        }
    }
}

#[test]
fn native_metadata_tls_reused_pin_old_destination_refuses_before_held_latches() {
    let (_keys, sockets, network, configs) = network();
    let mut fixture = ActorFixture::new(&network);
    fixture.root.disable_cleanup(true);
    eprintln!(
        "NATIVE_METADATA_DESTINATION_ROOT {}",
        fixture.root.path().display()
    );
    let initial = fixture.topology.read().clone();
    let metadata = (0..4)
        .map(|i| {
            let actual = MetadataConsensusStore::initialize_native_owned(
                fixture.root.path().join(format!("tls-meta-n{i}")),
                Arc::clone(&fixture.databases[&node(i)]),
                &fixture.fresh_factories[&node(i)],
                initial.clone(),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
            (node(i), Arc::new(Mutex::new(actual)))
        })
        .collect::<BTreeMap<_, _>>();
    // A metadata backend from another real DB cannot attach merely because
    // the listener is given a matching topology declaration.
    assert!(start_cluster_data_server(
        "127.0.0.1:0",
        configs[&node(0)].clone(),
        Arc::new(RwLock::new(initial.clone())),
        Arc::clone(&fixture.services[&node(0)]),
        None,
        Some(Arc::clone(&metadata[&node(1)])),
        None,
    )
    .err()
    .unwrap()
    .to_string()
    .contains("actual"));
    let mut unenrolled = initial.clone();
    for member in unenrolled.nodes.values_mut() {
        member.native_store_birth_v1 = None;
    }
    let generic = Arc::new(Mutex::new(
        MetadataConsensusStore::open(
            fixture.root.path().join("generic-meta"),
            initial.cluster_id.clone(),
            node(0),
            unenrolled,
            true,
        )
        .unwrap(),
    ));
    assert!(start_cluster_data_server(
        "127.0.0.1:0",
        configs[&node(0)].clone(),
        Arc::new(RwLock::new(initial.clone())),
        Arc::clone(&fixture.services[&node(0)]),
        None,
        Some(generic),
        None,
    )
    .err()
    .unwrap()
    .to_string()
    .contains("owned store"));
    drop(sockets);
    let old_view = Arc::new(RwLock::new(initial.clone()));
    let mut servers = (0..4)
        .map(|i| {
            start_cluster_data_server(
                &initial.nodes[&node(i)].address,
                configs[&node(i)].clone(),
                if i == 0 {
                    Arc::clone(&old_view)
                } else {
                    Arc::new(RwLock::new(initial.clone()))
                },
                Arc::clone(&fixture.services[&node(i)]),
                None,
                Some(Arc::clone(&metadata[&node(i)])),
                None,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let leader = &metadata[&node(1)];
    let client = TcpClusterRelocationTransport::new(
        initial.cluster_id.clone(),
        node(1),
        Arc::new(RwLock::new(initial.clone())),
        configs[&node(1)].clone(),
    )
    .unwrap();
    let request = leader.lock().start_election().unwrap();
    // An unbound sender view cannot omit native identity and obtain a vote
    // from an actual enforced listener. Same real certificate and payload.
    let mut unspecified = initial.clone();
    unspecified
        .nodes
        .get_mut(&node(0))
        .unwrap()
        .native_store_birth_v1 = None;
    let unspecified = TcpClusterRelocationTransport::new(
        initial.cluster_id.clone(),
        node(1),
        Arc::new(RwLock::new(unspecified)),
        configs[&node(1)].clone(),
    )
    .unwrap();
    {
        let _service = fixture.services[&node(0)].lock();
        let _database = fixture.databases[&node(0)].write();
        let _metadata = metadata[&node(0)].lock();
        let started = Instant::now();
        let error = unspecified
            .request_metadata_vote(&node(0), request.clone())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("destination differs from actual native"),
            "{error}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    let mut votes = BTreeSet::from([node(1)]);
    for i in [0, 2, 3] {
        let reply = client
            .request_metadata_vote(&node(i), request.clone())
            .unwrap();
        assert!(reply.vote_granted);
        votes.insert(reply.voter_id);
    }
    leader.lock().become_leader(&votes).unwrap();
    replicate(leader, &client, &[0, 2, 3]);
    // A real original TLS/envelope exchange seals the receiver's actual
    // protected StoreUUID/birth, not a response reconstructed from JSON.
    let owned_request = leader.lock().append_request_from(1, 32).unwrap();
    let owned_reply = client.append_metadata(&node(0), owned_request).unwrap();
    assert!(owned_reply.success);
    assert!(owned_reply.native_receiver.is_some() && owned_reply.native_provenance.is_some());
    let bare: MetadataAppendResponse =
        serde_json::from_slice(&serde_json::to_vec(&owned_reply).unwrap()).unwrap();
    assert_eq!(bare, owned_reply);
    let progress_before = leader.lock().match_index.clone();
    let error = leader
        .lock()
        .record_append_response(&node(0), &bare)
        .unwrap_err();
    assert!(error.to_string().contains("not a DTO"), "{error}");
    assert_eq!(leader.lock().match_index, progress_before);
    leader
        .lock()
        .record_append_response(&node(0), &owned_reply)
        .unwrap();
    let mut retired = leader.lock().committed_topology().clone();
    retired.generation += 1;
    retired.nodes.get_mut(&node(0)).unwrap().lifecycle = ClusterNodeLifecycle::Decommissioned;
    assert_eq!(retired.ranges, initial.ranges);
    leader.lock().propose_topology(retired.clone()).unwrap();
    // Deliver ONLY to the healthy quorum. The original n0 really has not
    // learned its retirement, and keeps its original listener/topology/store.
    replicate(leader, &client, &[2, 3]);
    assert_eq!(leader.lock().committed_topology(), &retired);
    assert_eq!(metadata[&node(0)].lock().committed_topology(), &initial);
    let retired_progress = leader.lock().match_index.clone();
    let error = leader
        .lock()
        .record_append_response(&node(0), &owned_reply)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("current birth/owned store/body/pin mismatch"),
        "{error}"
    );
    assert_eq!(
        leader.lock().match_index,
        retired_progress,
        "an actual pre-retirement ACK cannot repopulate committed retired identity progress"
    );
    let replacement_socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut replacement = retired.clone();
    replacement.generation += 1;
    {
        let member = replacement.nodes.get_mut(&node(0)).unwrap();
        member.lifecycle = ClusterNodeLifecycle::Active;
        member.incarnation += 1;
        member.metadata_role = MetadataMemberRole::Learner;
        member.address = replacement_socket.local_addr().unwrap().to_string();
        member.native_store_birth_v1 = None;
    }
    let spec = RangeImageBirthSpec {
        cluster_id: initial.cluster_id.clone(),
        bootstrap_id: initial.range_image_bootstrap_id.unwrap(),
        node_id: node(0),
        member_incarnation: 2,
        restore_activation: None,
        initial_ranges: Vec::new(),
        initial_members: replacement
            .nodes
            .iter()
            .map(|(id, member)| {
                (
                    id.clone(),
                    RangeImageBirthMember {
                        incarnation: member.incarnation,
                        tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: member
                            .pending_tls_certificate_sha256
                            .clone(),
                    },
                )
            })
            .collect(),
    };
    let path = fixture.root.path().join("actual-replacement-n0");
    let (mut db, _) =
        BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone()).unwrap();
    db.create_collection("items").unwrap();
    drop(db);
    let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    crate::distribution_native_admission::ActualNativeStoreEnrollment::from_actual_factory(
        &factory,
        &db,
        &replacement,
    )
    .unwrap()
    .bind_initial_or_unassigned_learner(&mut replacement)
    .unwrap();
    let db = Arc::new(RwLock::new(db));
    let new_metadata = Arc::new(Mutex::new(
        MetadataConsensusStore::initialize_native_owned(
            fixture.root.path().join("replacement-meta-n0"),
            Arc::clone(&db),
            &factory,
            replacement.clone(),
            &CancellationToken::uncancelable(),
        )
        .unwrap(),
    ));
    let new_service = Arc::new(Mutex::new(
        crate::ClusterDataNodeService::open(
            initial.cluster_id.clone(),
            node(0),
            &path,
            Arc::clone(&db),
            true,
        )
        .unwrap(),
    ));
    drop(replacement_socket);
    servers.push(
        start_cluster_data_server(
            &replacement.nodes[&node(0)].address,
            configs[&node(0)].clone(),
            Arc::new(RwLock::new(replacement.clone())),
            Arc::clone(&new_service),
            None,
            Some(Arc::clone(&new_metadata)),
            None,
        )
        .unwrap(),
    );
    leader.lock().propose_topology(replacement.clone()).unwrap();
    replicate(leader, &client, &[2, 3]);
    client.install_topology(replacement.clone()).unwrap();
    replicate(leader, &client, &[0, 2, 3]);
    assert_eq!(new_metadata.lock().committed_topology(), &replacement);
    let mut promoted = replacement.clone();
    promoted.generation += 1;
    promoted.nodes.get_mut(&node(0)).unwrap().metadata_role = MetadataMemberRole::Voter;
    leader.lock().propose_topology(promoted.clone()).unwrap();
    replicate(leader, &client, &[0, 2, 3]);
    assert_eq!(leader.lock().committed_topology(), &promoted);
    assert_eq!(new_metadata.lock().committed_topology(), &promoted);
    assert_eq!(metadata[&node(0)].lock().committed_topology(), &initial);
    assert_eq!(*old_view.read(), initial);
    client.install_topology(promoted.clone()).unwrap();
    let request = leader.lock().start_election().unwrap();
    let healthy = client
        .request_metadata_vote(&node(0), request.clone())
        .unwrap();
    assert!(
        healthy.vote_granted,
        "actual replacement's request-bound reply must remain healthy"
    );
    // Explicit stale endpoint delivery. The expected identity is the genuine
    // committed replacement; ONLY its routing address points to old process0.
    let mut redirected = promoted.clone();
    redirected.nodes.get_mut(&node(0)).unwrap().address = initial.nodes[&node(0)].address.clone();
    let stale = TcpClusterRelocationTransport::new(
        initial.cluster_id.clone(),
        node(1),
        Arc::new(RwLock::new(redirected)),
        configs[&node(1)].clone(),
    )
    .unwrap();
    let state_path = fixture
        .root
        .path()
        .join("tls-meta-n0")
        .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let before = fs::read(&state_path).unwrap();
    let service_latch = fixture.services[&node(0)].lock();
    let db_latch = fixture.databases[&node(0)].write();
    let metadata_latch = metadata[&node(0)].lock();
    let started = Instant::now();
    let error = stale.request_metadata_vote(&node(0), request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("response identity or protocol mismatch"),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(fs::read(&state_path).unwrap(), before);
    assert_eq!(metadata_latch.committed_topology(), &initial);
    drop(metadata_latch);
    drop(db_latch);
    drop(service_latch);
    eprintln!("NATIVE_METADATA_STALE_DESTINATION_REFUSAL {error}");
    for server in servers {
        server.join().unwrap();
    }
}
