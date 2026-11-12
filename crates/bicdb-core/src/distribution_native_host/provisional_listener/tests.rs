//! Actual TLS listeners and actual from-birth paged factories. These tests
//! stop before metadata election; factory observations are not readiness.
use super::*;
use crate::{ClusterId, ClusterNode, DistributionConfig, Record, ReplicationTlsConfig};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};

fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn db_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
struct Fixture {
    // Handles stop before their actual databases and TLS files disappear.
    hosts: Vec<NativeProvisionalHost>,
    plan: NativeClusterBootstrapPlan,
    networks: Vec<ClusterNetworkTransportConfig>,
    _root: tempfile::TempDir,
}
struct PlannedNetwork {
    root: tempfile::TempDir,
    plan: NativeClusterBootstrapPlan,
    networks: Vec<ClusterNetworkTransportConfig>,
    ports: Vec<TcpListener>,
}
fn planned_network(count: usize) -> PlannedNetwork {
    planned_network_with_spare(count, 0)
}
fn planned_network_with_spare(count: usize, spare: usize) -> PlannedNetwork {
    assert!(matches!(count, 3 | 4));
    assert!(spare <= 1);
    let root = tempfile::tempdir().unwrap();
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = params.self_signed(&ca_key).unwrap();
    let ca_path = root.path().join("ca.pem");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    let ports = (0..count + spare)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let mut networks = Vec::new();
    let mut members = Vec::new();
    for (i, listener) in ports.iter().enumerate() {
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
        let cert_path = root.path().join(format!("n{i}.pem"));
        let key_path = root.path().join(format!("n{i}.key"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        let pin = crate::replication_transport::replication_certificate_sha256(&cert_path).unwrap();
        members.push(
            ClusterNode::new(
                node(i),
                listener.local_addr().unwrap().to_string(),
                1,
                1 << 30,
                10,
            )
            .unwrap()
            .with_tls_certificate_sha256(pin)
            .unwrap(),
        );
        networks.push(ClusterNetworkTransportConfig {
            connect_timeout_ms: 200,
            io_timeout_ms: 1500,
            max_frame_bytes: 4 * 1024 * 1024,
            max_inbound_connections: 8,
            dev_localhost_plaintext: false,
            tls: Some(ReplicationTlsConfig {
                cert_path,
                key_path,
                ca_path: ca_path.clone(),
                require_client_cert: true,
                dev_localhost_plaintext: false,
            }),
        });
    }
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("native-bootstrap-actual-tls").unwrap(),
        range_image_bootstrap_id: Some(Uuid::now_v7()),
        node_id: node(0),
        node_address: members[0].address.clone(),
        node_tls_certificate_sha256: members[0].tls_certificate_sha256.clone(),
        node_incarnation: 1,
        node_capacity_bytes: 1 << 30,
        replication_factor: 3,
        initial_ranges: 4,
        ..Default::default()
    };
    let token = CancellationToken::uncancelable();
    let plan = NativeClusterBootstrapPlan::new(
        &config,
        members.into_iter().take(count).collect(),
        vec![
            NativeBootstrapCollection {
                name: "items".into(),
                mode: CollectionMode::Standard,
            },
            NativeBootstrapCollection {
                name: crate::CLUSTER_EVENTS_COLLECTION.into(),
                mode: CollectionMode::Standard,
            },
        ],
        10,
        &token,
    )
    .unwrap();
    PlannedNetwork {
        root,
        plan,
        networks,
        ports,
    }
}
impl Fixture {
    fn new() -> Self {
        let PlannedNetwork {
            root,
            plan,
            networks,
            ports,
        } = planned_network(3);
        let token = CancellationToken::uncancelable();
        let seeds = (0..3)
            .map(|i| {
                Arc::new(
                    NativeProvisionalNode::create(
                        &root.path().join(format!("db{i}")),
                        db_config(),
                        plan.clone(),
                        &node(i),
                        &token,
                    )
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        drop(ports);
        let hosts = seeds
            .into_iter()
            .enumerate()
            .map(|(i, seed)| {
                NativeProvisionalHost::start(seed, networks[i].clone(), &token).unwrap()
            })
            .collect();
        Self {
            hosts,
            plan,
            networks,
            _root: root,
        }
    }
    fn offer(
        &self,
        i: usize,
        token: &CancellationToken,
    ) -> Result<NativeInitialBirthOfferObservation> {
        self.hosts[0]
            .transport
            .acquire_initial_birth_offer(&node(i), &self.plan, token)
    }
    fn assert_no_metadata(&self) {
        for host in &self.hosts {
            host.seed.require_unregistered().unwrap();
            assert!(!host
                .seed
                .root
                .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
                .exists());
        }
    }
}

#[test]
fn native_initial_birth_tls_all_actual_factories_bind_initial_config_without_admitting_writes() {
    let f = Fixture::new();
    let token = CancellationToken::uncancelable();
    let observed = f.hosts[0].observe_initial_configuration(&token).unwrap();
    let initial = observed
        .topology_for_actual_seed(&f.hosts[0].seed, &token)
        .unwrap();
    let mut births = BTreeSet::new();
    for host in &f.hosts {
        let member = &initial.nodes[&host.seed.spec.node_id];
        let birth = member.native_store_birth_v1.as_ref().unwrap();
        assert_eq!(birth.birth_id(), host.seed.manifest.birth_id);
        assert_eq!(birth.birth_sha256(), host.seed.manifest.birth_sha256);
        assert_eq!(birth.writer_enforcement_version(), 1);
        assert!(births.insert(birth.birth_id()));
        assert_eq!(
            member.labels[crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL],
            host.seed.manifest.schema_sha256
        );
        let db = host.seed.database.read();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("must-not-ack-from-birth-offer"))
            .unwrap();
        assert!(tx.commit().is_err());
        assert!(db
            .get("items", "must-not-ack-from-birth-offer")
            .unwrap()
            .is_none());
    }
    f.assert_no_metadata();
}

#[test]
fn native_initial_birth_tls_refuses_incomplete_duplicate_changed_plan_and_expired_attempt() {
    let f = Fixture::new();
    let token = CancellationToken::uncancelable();
    assert!(
        NativeInitialMetadataConfiguration::from_authenticated_offers(
            &f.plan,
            vec![f.offer(0, &token).unwrap(), f.offer(1, &token).unwrap()],
            &token
        )
        .is_err()
    );
    assert!(
        NativeInitialMetadataConfiguration::from_authenticated_offers(
            &f.plan,
            vec![
                f.offer(0, &token).unwrap(),
                f.offer(1, &token).unwrap(),
                f.offer(1, &token).unwrap()
            ],
            &token
        )
        .is_err()
    );
    let mut other = f.plan.clone();
    other.collections[0].name = "another_schema".into();
    assert!(f.hosts[0]
        .transport
        .acquire_initial_birth_offer(&node(1), &other, &token)
        .is_err());
    let canceled = Arc::new(AtomicBool::new(false));
    let parent = CancellationToken::new(Arc::clone(&canceled), None);
    let offers = (0..3).map(|i| f.offer(i, &parent).unwrap()).collect();
    canceled.store(true, Ordering::SeqCst);
    assert!(
        NativeInitialMetadataConfiguration::from_authenticated_offers(&f.plan, offers, &token)
            .is_err()
    );
    f.assert_no_metadata();
}

#[test]
fn native_initial_birth_tls_before_latch_but_busy_actual_factory_is_bounded_unavailable() {
    let f = Fixture::new();
    let token = CancellationToken::uncancelable();
    let service_latch = f.hosts[1]._service.lock();
    // Actual authenticated exchange must bypass the service mutex entirely.
    f.offer(1, &token).unwrap();
    let database_write = f.hosts[1].seed.database.write();
    let start = std::time::Instant::now();
    let error = f.offer(1, &token).err().unwrap().to_string();
    assert!(error.contains("factory is busy"), "{error}");
    assert!(start.elapsed() < Duration::from_secs(2));
    drop(database_write);
    drop(service_latch);
    f.offer(1, &token).unwrap();
    f.assert_no_metadata();
}

#[test]
fn native_initial_birth_tls_refuses_registered_root_stale_peer_and_plaintext_host() {
    let f = Fixture::new();
    let token = CancellationToken::uncancelable();
    // No request can relabel this actual transport's client identity.
    let request = NativeInitialBirthOfferRequest {
        version: 1,
        challenge: Uuid::now_v7(),
        requester: node(1),
        requester_incarnation: 1,
        plan_sha256: f.plan.checksum().unwrap(),
        budget_ms: 5000,
    };
    assert!(f.hosts[0]
        .transport
        .request_initial_birth_reply(&node(1), request, &token)
        .is_err());
    let mut changed = f.plan.topology.clone();
    changed
        .nodes
        .get_mut(&node(1))
        .unwrap()
        .tls_certificate_sha256 = Some("f".repeat(64));
    let wrong_pin = TcpClusterRelocationTransport::new(
        changed.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(changed)),
        f.networks[0].clone(),
    )
    .unwrap();
    assert!(wrong_pin
        .acquire_initial_birth_offer(&node(1), &f.plan, &token)
        .is_err());
    assert!(NativeProvisionalHost::start(
        Arc::clone(&f.hosts[0].seed),
        ClusterNetworkTransportConfig::default(),
        &token
    )
    .is_err());
    f.assert_no_metadata();
    std::fs::write(
        f.hosts[1]
            .seed
            .root
            .join(crate::distribution::DEFAULT_CLUSTER_TOPOLOGY),
        b"refusal-only sentinel",
    )
    .unwrap();
    let error = f.offer(1, &token).err().unwrap().to_string();
    assert!(error.contains("already registered"), "{error}");
    assert!(f.hosts[0].observe_initial_configuration(&token).is_err());
    assert!(!f.hosts[1]
        .seed
        .root
        .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        .exists());
}

#[test]
fn native_initial_birth_tls_provisional_surface_never_falls_through_even_after_factory_owner_ends()
{
    let mut f = Fixture::new();
    let range = f.plan.topology.ranges.values().next().unwrap().clone();
    let request = crate::ClusterEventRequest {
        range_id: range.id,
        range_epoch: range.epoch,
        operation: crate::ClusterEventOperation::Scan {
            namespace: "tenant".into(),
            after: None,
            limit: 1,
        },
    };
    let service = Arc::clone(&f.hosts[1]._service);
    {
        let _latch = service.lock();
        let start = std::time::Instant::now();
        let error = f.hosts[0]
            .transport
            .application_event(&node(1), request.clone())
            .unwrap_err();
        assert!(
            error.to_string().contains("provisional factory listener"),
            "{error}"
        );
        assert!(start.elapsed() < Duration::from_secs(2));
    }
    f.hosts.remove(1).shutdown().unwrap();
    assert!(service
        .lock()
        .initial_birth_provider()
        .unwrap()
        .upgrade()
        .is_none());
    // Keep the real DB/service alive while ending ONLY its factory owner.
    // Restart the same private listener with its expired provider option.
    let listener = start_cluster_data_server(
        &f.plan.topology.nodes[&node(1)].address,
        f.networks[1].clone(),
        Arc::new(RwLock::new(f.plan.topology.clone())),
        service,
        None,
        None,
        None,
    )
    .unwrap();
    let error = f.hosts[0]
        .transport
        .application_event(&node(1), request)
        .unwrap_err();
    assert!(
        error.to_string().contains("provisional factory listener"),
        "{error}"
    );
    assert!(f.offer(1, &CancellationToken::uncancelable()).is_err());
    listener.join().unwrap();
    f.assert_no_metadata();
}

mod process_tests;

mod initial_preparation_tests;
mod learner_factory_tests;
