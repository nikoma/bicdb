//! Restart of INITIAL members through the public host owner: four real
//! hosts with pinned mutual TLS in one test process, the public event client,
//! and a member that is dropped without a stop and reopened from its root.
use super::*;
use crate::{
    cluster_event_record_id, ClusterEventClient, ClusterId, ClusterNode, ClusterTopology,
    ReplicationTlsConfig, CLUSTER_EVENTS_COLLECTION,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use std::net::TcpListener;

fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn budget(ms: u64) -> CancellationToken {
    CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_millis(ms))
}
struct Cluster {
    hosts: Vec<Option<NativeMetadataHost>>,
    plan: NativeMetadataBootstrapPlan,
    networks: Vec<ClusterNetworkTransportConfig>,
    addresses: Vec<String>,
    process: CancellationToken,
    root: tempfile::TempDir,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.process.cancel();
        for host in self.hosts.iter_mut().flatten() {
            let _ = host.apply(NativeMetadataHostAction::Stop, &budget(5000));
        }
    }
}
impl Cluster {
    fn new() -> Self {
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
        let ports = (0..4)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let mut networks = Vec::new();
        let mut members = Vec::new();
        let mut addresses = Vec::new();
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
            let pin =
                crate::replication_transport::replication_certificate_sha256(&cert_path).unwrap();
            let address = listener.local_addr().unwrap().to_string();
            members.push(
                ClusterNode::new(node(i), address.clone(), 1, 1 << 30, 10)
                    .unwrap()
                    .with_tls_certificate_sha256(pin)
                    .unwrap(),
            );
            addresses.push(address);
            networks.push(ClusterNetworkTransportConfig {
                connect_timeout_ms: 200,
                io_timeout_ms: 2000,
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
            });
        }
        let mut configuration = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("native-initial-member-restart").unwrap(),
            range_image_bootstrap_id: Some(Uuid::now_v7()),
            node_id: node(0),
            node_address: addresses[0].clone(),
            node_tls_certificate_sha256: members[0].tls_certificate_sha256.clone(),
            node_incarnation: 1,
            node_capacity_bytes: 1 << 30,
            replication_factor: 3,
            initial_ranges: 4,
            ..Default::default()
        };
        configuration.transport = networks[0].clone();
        let plan = NativeMetadataBootstrapPlan {
            version: 1,
            configuration,
            members,
            collections: vec![
                NativeMetadataCollection {
                    name: "items".into(),
                    mode: CollectionMode::Standard,
                },
                NativeMetadataCollection {
                    name: CLUSTER_EVENTS_COLLECTION.into(),
                    mode: CollectionMode::Standard,
                },
            ],
            created_at_ms: 10,
        };
        drop(ports);
        let process = CancellationToken::uncancelable().child();
        let mut cluster = Self {
            hosts: Vec::new(),
            plan,
            networks,
            addresses,
            process,
            root,
        };
        for i in 0..4 {
            cluster.hosts.push(Some(
                NativeMetadataHost::start_fresh_metadata_only(
                    &cluster.db(i),
                    cluster.plan.clone(),
                    node(i),
                    &cluster.addresses[i],
                    cluster.networks[i].clone(),
                    8 * 1024 * 1024,
                    &budget(30_000),
                )
                .unwrap(),
            ));
        }
        for action in [
            NativeMetadataHostAction::Prepare,
            NativeMetadataHostAction::Launch,
        ] {
            for i in 0..4 {
                cluster
                    .host(i)
                    .apply(action.clone(), &budget(5000))
                    .unwrap();
            }
        }
        cluster
            .host(0)
            .apply(NativeMetadataHostAction::Elect, &budget(5000))
            .unwrap();
        for i in 0..4 {
            cluster
                .host(i)
                .apply(NativeMetadataHostAction::EnableControl, &budget(5000))
                .unwrap();
        }
        cluster.tick();
        cluster.open_data(&[0, 1, 2, 3]);
        cluster
    }
    fn db(&self, i: usize) -> PathBuf {
        self.root.path().join(format!("db{i}"))
    }
    fn host(&mut self, i: usize) -> &mut NativeMetadataHost {
        self.hosts[i].as_mut().unwrap()
    }
    /// What the serving process does every 500 ms, for every live host.
    fn tick(&mut self) {
        let process = self.process.clone();
        for host in self.hosts.iter_mut().flatten() {
            let _ = host.start_automatic_metadata(&process);
            let leads = host
                .status(&budget(250))
                .ok()
                .and_then(|status| status.metadata)
                .is_some_and(|m| m.role == crate::MetadataConsensusRole::Leader);
            if leads {
                let _ = host.maintain_metadata(&budget(1250));
            }
            let _ = host.refresh_initial_data_registry(&budget(5000));
        }
    }
    fn open_data(&mut self, members: &[usize]) {
        let until = Instant::now() + Duration::from_secs(60);
        let mut last = vec![String::new(); 4];
        loop {
            let mut ready = true;
            for &i in members {
                let serving = self
                    .host(i)
                    .status(&budget(250))
                    .is_ok_and(|status| status.data_serving);
                if serving {
                    continue;
                }
                ready = false;
                if let Err(error) = self
                    .host(i)
                    .apply(NativeMetadataHostAction::OpenInitialData, &budget(1500))
                {
                    last[i] = error.to_string();
                }
            }
            if ready {
                return;
            }
            self.tick();
            assert!(
                Instant::now() < until,
                "members {members:?} do not serve DATA: {last:?} statuses={:?}",
                members
                    .iter()
                    .map(|&i| self
                        .host(i)
                        .status(&budget(250))
                        .map(|s| serde_json::to_string(&s).unwrap()))
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn topology(&self, via: usize) -> ClusterTopology {
        crate::TcpClusterBootstrapClient::new(
            self.plan.configuration.cluster_id.clone(),
            node(0),
            self.networks[0].clone(),
        )
        .unwrap()
        .fetch_snapshot(&node(via), &self.addresses[via])
        .unwrap()
        .topology
    }
    fn client(&self, via: usize) -> ClusterEventClient {
        ClusterEventClient::new(&self.plan.configuration, self.topology(via))
            .unwrap()
            .with_request_timeout(Duration::from_secs(2))
            .unwrap()
    }
    fn metadata_leader(&mut self) -> Option<usize> {
        (0..4).find(|&i| {
            self.hosts[i].as_ref().is_some_and(|host| {
                host.status(&budget(250))
                    .ok()
                    .and_then(|status| status.metadata)
                    .is_some_and(|m| m.role == crate::MetadataConsensusRole::Leader)
            })
        })
    }
    /// End a member the way a crash does for its durable state: no stop
    /// action, no drain receipt. Returns once its root can be opened again.
    fn crash(&mut self, i: usize) {
        drop(self.hosts[i].take().unwrap());
    }
    fn reopen(&mut self, i: usize) {
        self.reopen_metadata(i);
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            self.tick();
            match self
                .host(i)
                .apply(NativeMetadataHostAction::EnableControl, &budget(5000))
            {
                Ok(_) => break,
                Err(error) => assert!(
                    Instant::now() < until,
                    "reopened member {i} cannot enable control: {error}"
                ),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        self.open_data(&[i]);
    }
    /// The constructor alone: owned metadata and its listener, nothing else.
    fn reopen_metadata(&mut self, i: usize) {
        let until = Instant::now() + Duration::from_secs(20);
        let host = loop {
            match NativeMetadataHost::reopen_metadata_only(
                &self.db(i),
                self.plan.clone(),
                node(i),
                &self.addresses[i],
                self.networks[i].clone(),
                8 * 1024 * 1024,
                &budget(30_000),
            ) {
                Ok(host) => break host,
                Err(error) => {
                    assert!(
                        Instant::now() < until,
                        "member {i} cannot be reopened: {error}"
                    );
                    self.tick();
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        };
        self.hosts[i] = Some(host);
    }
}
fn range_of(topology: &ClusterTopology, key: &str) -> RangeId {
    topology
        .range_for_key(
            CLUSTER_EVENTS_COLLECTION,
            &cluster_event_record_id("items", key).unwrap(),
        )
        .unwrap()
        .id
}
fn value(key: &str) -> serde_json::Value {
    serde_json::json!({"key": key, "payload": [1, null, {"kept": true}]})
}
/// Next unused key that falls into `range`.
fn key_in(topology: &ClusterTopology, range: RangeId, counter: &mut u64) -> String {
    loop {
        let key = format!("restart-{counter}");
        *counter += 1;
        if range_of(topology, &key) == range {
            return key;
        }
    }
}
/// An append repeated within `timeout`; the cluster is ticked meanwhile.
fn append(
    cluster: &mut Cluster,
    client: &ClusterEventClient,
    key: &str,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let begun = Instant::now();
    loop {
        match client.append("items", key, value(key)) {
            Ok(_) => return Ok(()),
            Err(error) if begun.elapsed() >= timeout => return Err(error.to_string()),
            Err(_) => {
                cluster.tick();
                let _ = client.refresh();
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}
fn read(
    cluster: &mut Cluster,
    client: &ClusterEventClient,
    key: &str,
    timeout: Duration,
) -> std::result::Result<Option<serde_json::Value>, String> {
    let begun = Instant::now();
    loop {
        match client.get_strong("items", key) {
            Ok(found) => return Ok(found),
            Err(error) if begun.elapsed() >= timeout => return Err(error.to_string()),
            Err(_) => {
                cluster.tick();
                let _ = client.refresh();
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// A follower of a range is dropped without a stop. Writes on the range
/// continue on the surviving quorum; the member is reopened from its own
/// root, serves again and catches up; then the OTHER follower is dropped and
/// the range still accepts writes, which requires the reopened member's
/// durable vote for every one of them. Every acknowledged row is readable.
#[test]
fn native_initial_member_restart_follower_rejoins_and_carries_the_quorum() {
    let mut cluster = Cluster::new();
    let topology = cluster.topology(0);
    let metadata_leader = cluster.metadata_leader().expect("a metadata leader");
    // A range and one of its followers that does not lead metadata.
    let (range, leader, victim, other) = topology
        .ranges
        .values()
        .find_map(|range| {
            let leader = (0..4).find(|&i| node(i) == range.leader)?;
            let followers = (0..4)
                .filter(|&i| i != leader && range.replicas.iter().any(|r| r.node_id == node(i)))
                .collect::<Vec<_>>();
            let victim = *followers.iter().find(|&&i| i != metadata_leader)?;
            let other = *followers.iter().find(|&&i| i != victim)?;
            (other != metadata_leader || leader != metadata_leader)
                .then_some((range.id, leader, victim, other))
        })
        .expect("a range with a follower that does not lead metadata");
    eprintln!("RESTART_TEST range={range} leader=n{leader} victim=n{victim} other=n{other} metadata_leader=n{metadata_leader}");
    let client = cluster.client(leader);
    let mut counter = 0u64;
    let mut acknowledged = Vec::new();
    // Rows on every range first, so that the victim's other ranges have data.
    for every in topology.ranges.values() {
        for _ in 0..2 {
            let key = key_in(&topology, every.id, &mut counter);
            append(&mut cluster, &client, &key, Duration::from_secs(20)).unwrap();
            acknowledged.push(key);
        }
    }
    cluster.crash(victim);
    for _ in 0..3 {
        let key = key_in(&topology, range, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("write with one follower down failed: {error}"));
        acknowledged.push(key);
    }
    eprintln!("RESTART_TEST writes continued with n{victim} down");
    cluster.reopen(victim);
    eprintln!("RESTART_TEST n{victim} reopened and serves DATA");
    for _ in 0..2 {
        let key = key_in(&topology, range, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("write after the follower returned failed: {error}"));
        acknowledged.push(key);
    }
    // The other follower goes away: leader + reopened member are the quorum.
    if other != cluster.metadata_leader().unwrap_or(usize::MAX) {
        cluster.crash(other);
        for _ in 0..2 {
            let key = key_in(&topology, range, &mut counter);
            append(&mut cluster, &client, &key, Duration::from_secs(30)).unwrap_or_else(|error| {
                panic!("write on the quorum of leader and reopened member failed: {error}")
            });
            acknowledged.push(key);
        }
        eprintln!("RESTART_TEST leader and reopened n{victim} carried the quorum");
        cluster.reopen(other);
    }
    for key in &acknowledged {
        assert_eq!(
            read(&mut cluster, &client, key, Duration::from_secs(30)).unwrap(),
            Some(value(key)),
            "acknowledged row {key} must be readable"
        );
    }
}

/// The leader of a range is dropped without a stop. That range refuses
/// writes while its leader is away (no other member takes it over); a range
/// led by a surviving member keeps accepting writes. The reopened leader
/// recovers its leadership through the ordinary leader recovery of the new
/// process and the range accepts writes again. Nothing acknowledged is lost.
#[test]
fn native_initial_member_restart_range_leader_recovers_its_leadership() {
    let mut cluster = Cluster::new();
    let topology = cluster.topology(0);
    let metadata_leader = cluster.metadata_leader().expect("a metadata leader");
    let led = topology
        .ranges
        .values()
        .find(|range| range.leader != node(metadata_leader))
        .expect("a range not led by the metadata leader")
        .clone();
    let victim = (0..4).find(|&i| node(i) == led.leader).unwrap();
    let surviving = topology
        .ranges
        .values()
        .find(|range| range.leader != led.leader)
        .unwrap()
        .clone();
    eprintln!(
        "RESTART_TEST led={} victim=n{victim} surviving={} led by {} metadata_leader=n{metadata_leader}",
        led.id, surviving.id, surviving.leader
    );
    let client = cluster.client(metadata_leader);
    let mut counter = 0u64;
    let mut acknowledged = Vec::new();
    for every in topology.ranges.values() {
        for _ in 0..2 {
            let key = key_in(&topology, every.id, &mut counter);
            append(&mut cluster, &client, &key, Duration::from_secs(20)).unwrap();
            acknowledged.push(key);
        }
    }
    cluster.crash(victim);
    let key = key_in(&topology, surviving.id, &mut counter);
    append(&mut cluster, &client, &key, Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("write on a range with a surviving leader failed: {error}"));
    acknowledged.push(key);
    // The leaderless range: every attempt within the window is refused.
    let refused = key_in(&topology, led.id, &mut counter);
    let leaderless = append(&mut cluster, &client, &refused, Duration::from_secs(4));
    eprintln!("RESTART_TEST write on the leaderless range: {leaderless:?}");
    assert!(
        leaderless.is_err(),
        "a range whose leader is down must not acknowledge a write"
    );
    cluster.reopen(victim);
    let status = cluster.host(victim).status(&budget(1000)).unwrap();
    assert_eq!(status.phase, NativeMetadataHostPhase::InitialData);
    assert!(status.data_serving);
    assert!(status
        .initial_data
        .as_ref()
        .unwrap()
        .ranges
        .iter()
        .any(|range| range.range_id == led.id
            && range.local_leader
            && range.leader_admission_observed));
    for _ in 0..2 {
        let key = key_in(&topology, led.id, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("write after the leader returned failed: {error}"));
        acknowledged.push(key);
    }
    for key in &acknowledged {
        assert_eq!(
            read(&mut cluster, &client, key, Duration::from_secs(30)).unwrap(),
            Some(value(key)),
            "acknowledged row {key} must be readable"
        );
    }
    // The refused write was never acknowledged; whether it exists is decided
    // by the cluster, and a later write of the same key must be accepted.
    append(&mut cluster, &client, &refused, Duration::from_secs(30)).unwrap();
}

/// The metadata leader is dropped without a stop. The remaining three elect
/// a leader by themselves, ranges led by survivors keep accepting writes, and
/// the reopened member rejoins metadata as a follower of the new leader with
/// its persisted term and log, then serves DATA again.
#[test]
fn native_initial_member_restart_metadata_leader_rejoins_as_follower() {
    let mut cluster = Cluster::new();
    let topology = cluster.topology(0);
    let victim = cluster.metadata_leader().expect("a metadata leader");
    let before = cluster.host(victim).status(&budget(1000)).unwrap();
    let term_before = before.metadata.as_ref().unwrap().current_term;
    let surviving = topology
        .ranges
        .values()
        .find(|range| range.leader != node(victim))
        .unwrap()
        .clone();
    let contact = (0..4).find(|&i| i != victim).unwrap();
    let client = cluster.client(contact);
    let mut counter = 0u64;
    let mut acknowledged = Vec::new();
    for every in topology.ranges.values() {
        let key = key_in(&topology, every.id, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(20)).unwrap();
        acknowledged.push(key);
    }
    cluster.crash(victim);
    let until = Instant::now() + Duration::from_secs(30);
    let successor = loop {
        cluster.tick();
        if let Some(leader) = cluster.metadata_leader() {
            break leader;
        }
        assert!(Instant::now() < until, "no metadata leader was elected");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_ne!(successor, victim);
    eprintln!("RESTART_TEST metadata leader n{victim} down, n{successor} elected");
    let key = key_in(&topology, surviving.id, &mut counter);
    append(&mut cluster, &client, &key, Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("write under the new metadata leader failed: {error}"));
    acknowledged.push(key);
    cluster.reopen(victim);
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        cluster.tick();
        let status = cluster.host(victim).status(&budget(1000)).unwrap();
        let metadata = status.metadata.as_ref().unwrap();
        if metadata.current_term > term_before && status.metadata_leader.is_some() {
            break;
        }
        assert!(
            Instant::now() < until,
            "reopened member did not learn the new metadata term: {metadata:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    for every in topology.ranges.values() {
        let key = key_in(&topology, every.id, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(30)).unwrap_or_else(|error| {
            panic!("write on {} after the restart failed: {error}", every.id)
        });
        acknowledged.push(key);
    }
    for key in &acknowledged {
        assert_eq!(
            read(&mut cluster, &client, key, Duration::from_secs(30)).unwrap(),
            Some(value(key)),
            "acknowledged row {key} must be readable"
        );
    }
}

/// The reopen never creates or adopts anything: a live root, a root that
/// never exposed consensus, a foreign plan and a foreign member are refused,
/// and a refusal leaves the root reopenable by its real owner.
#[test]
fn native_initial_member_restart_refuses_live_unexposed_and_foreign_roots() {
    let mut cluster = Cluster::new();
    let reopen =
        |cluster: &Cluster, root: usize, plan: NativeMetadataBootstrapPlan, as_node: usize| {
            NativeMetadataHost::reopen_metadata_only(
                &cluster.db(root),
                plan,
                node(as_node),
                &cluster.addresses[as_node],
                cluster.networks[as_node].clone(),
                8 * 1024 * 1024,
                &budget(10_000),
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
        };
    // The owner is alive: the root is held by its process.
    let live = reopen(&cluster, 3, cluster.plan.clone(), 3).unwrap_err();
    eprintln!("RESTART_TEST live root refusal: {live}");
    cluster.crash(3);
    // Another member's identity on this root.
    let until = Instant::now() + Duration::from_secs(20);
    let foreign = loop {
        let refusal = reopen(&cluster, 3, cluster.plan.clone(), 2).unwrap_err();
        if refusal.contains("another plan/member") {
            break refusal;
        }
        assert!(Instant::now() < until, "unexpected refusal: {refusal}");
        std::thread::sleep(Duration::from_millis(100));
    };
    eprintln!("RESTART_TEST foreign member refusal: {foreign}");
    // Another plan (a different creation time changes its checksum).
    let mut other_plan = cluster.plan.clone();
    other_plan.created_at_ms += 1;
    let refusal = reopen(&cluster, 3, other_plan, 3).unwrap_err();
    assert!(refusal.contains("another plan/member"), "{refusal}");
    // A fresh root that never exposed consensus.
    let unexposed = cluster.root.path().join("unexposed");
    let mut single = cluster.plan.clone();
    single.created_at_ms += 2;
    // Its own free endpoint is not needed: creation fails to bind only if the
    // crashed member's port is taken, and that member is down.
    drop(
        NativeMetadataHost::start_fresh_metadata_only(
            &unexposed,
            single.clone(),
            node(3),
            &cluster.addresses[3],
            cluster.networks[3].clone(),
            8 * 1024 * 1024,
            &budget(30_000),
        )
        .unwrap(),
    );
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        let refusal = NativeMetadataHost::reopen_metadata_only(
            &unexposed,
            single.clone(),
            node(3),
            &cluster.addresses[3],
            cluster.networks[3].clone(),
            8 * 1024 * 1024,
            &budget(10_000),
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
        if refusal.contains("never exposed consensus")
            || refusal.contains("no owned metadata state")
        {
            eprintln!("RESTART_TEST unexposed root refusal: {refusal}");
            break;
        }
        assert!(Instant::now() < until, "unexpected refusal: {refusal}");
        std::thread::sleep(Duration::from_millis(100));
    }
    // None of the refusals damaged the real root.
    cluster.reopen(3);
    assert!(cluster.host(3).status(&budget(1000)).unwrap().data_serving);
    // Nothing was ever written in this cluster: the reopened member leads a
    // range that is still at its genesis, recovered it without a genesis
    // proof, and that range accepts its first write like every other range.
    let topology = cluster.topology(0);
    assert!(topology
        .ranges
        .values()
        .any(|range| range.leader == node(3)));
    let client = cluster.client(0);
    let mut counter = 0u64;
    for every in topology.ranges.values() {
        let key = key_in(&topology, every.id, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(30)).unwrap_or_else(|error| {
            panic!(
                "first write on {} after the reopen failed: {error}",
                every.id
            )
        });
        assert_eq!(
            read(&mut cluster, &client, &key, Duration::from_secs(30)).unwrap(),
            Some(value(&key))
        );
    }
}

/// All four members are dropped without a stop and all four are reopened:
/// no member of the earlier processes is left to vouch for anything. They
/// elect a metadata leader from their persisted terms and logs, every member
/// enrolls a new writer policy, every range leader recovers its leadership
/// from a current voter quorum, and every acknowledged row is readable.
#[test]
fn native_initial_member_restart_whole_cluster_reopens_from_its_roots() {
    let mut cluster = Cluster::new();
    let topology = cluster.topology(0);
    let client = cluster.client(0);
    let mut counter = 0u64;
    let mut acknowledged = Vec::new();
    for every in topology.ranges.values() {
        for _ in 0..3 {
            let key = key_in(&topology, every.id, &mut counter);
            append(&mut cluster, &client, &key, Duration::from_secs(20)).unwrap();
            acknowledged.push(key);
        }
    }
    let terms = (0..4)
        .map(|i| {
            cluster
                .host(i)
                .status(&budget(1000))
                .unwrap()
                .metadata
                .unwrap()
                .current_term
        })
        .collect::<Vec<_>>();
    let term_before = *terms.iter().max().unwrap();
    drop(client);
    for i in 0..4 {
        cluster.crash(i);
    }
    for i in 0..4 {
        cluster.reopen_metadata(i);
        let status = cluster.host(i).status(&budget(1000)).unwrap();
        assert_eq!(status.phase, NativeMetadataHostPhase::OwnedMetadata);
        let metadata = status.metadata.unwrap();
        // Persisted term kept; no role or leader carried over.
        assert!(metadata.current_term >= terms[i]);
        assert_eq!(metadata.role, crate::MetadataConsensusRole::Follower);
        assert!(status.metadata_leader.is_none());
        assert!(!status.data_serving);
    }
    let until = Instant::now() + Duration::from_secs(90);
    let mut last = vec![String::new(); 4];
    loop {
        let mut serving = 0;
        for i in 0..4 {
            let status = cluster.host(i).status(&budget(250));
            let (phase, ready) = status
                .map(|status| (Some(status.phase), status.data_serving))
                .unwrap_or((None, false));
            if ready {
                serving += 1;
                continue;
            }
            let action = match phase {
                Some(
                    NativeMetadataHostPhase::OwnedMetadata
                    | NativeMetadataHostPhase::MetadataControlTransition,
                ) => NativeMetadataHostAction::EnableControl,
                _ => NativeMetadataHostAction::OpenInitialData,
            };
            if let Err(error) = cluster.host(i).apply(action, &budget(2500)) {
                last[i] = error.to_string();
            }
        }
        if serving == 4 {
            break;
        }
        cluster.tick();
        assert!(
            Instant::now() < until,
            "the reopened cluster does not serve DATA: {last:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let leader = cluster
        .metadata_leader()
        .expect("an elected metadata leader");
    assert!(
        cluster
            .host(leader)
            .status(&budget(1000))
            .unwrap()
            .metadata
            .unwrap()
            .current_term
            > term_before,
        "leadership of the reopened cluster needs a new elected term"
    );
    let client = cluster.client(leader);
    for key in &acknowledged {
        assert_eq!(
            read(&mut cluster, &client, key, Duration::from_secs(30)).unwrap(),
            Some(value(key)),
            "acknowledged row {key} must be readable after the whole cluster restarted"
        );
    }
    for every in topology.ranges.values() {
        let key = key_in(&topology, every.id, &mut counter);
        append(&mut cluster, &client, &key, Duration::from_secs(30)).unwrap_or_else(|error| {
            panic!(
                "write on {} after the whole cluster restarted failed: {error}",
                every.id
            )
        });
        assert_eq!(
            read(&mut cluster, &client, &key, Duration::from_secs(30)).unwrap(),
            Some(value(&key))
        );
    }
}
