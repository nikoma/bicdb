//! Client RPC deadline regressions over actual loopback mTLS.
//! Fresh temporary test certificates only; no production keys, HA or physical
//! qualification. The custom peer requires a CA-authenticated client and checks
//! its leaf pin. Topology is a local fixture, not a metadata-consensus exercise.
#![cfg(feature = "tls-replication")]

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use bicdb_core::replication_transport::{
    build_server_tls, peer_certificate_sha256, replication_certificate_sha256, server_tls_stream,
};
use bicdb_core::{
    ClusterId, ClusterNetworkTransportConfig, ClusterNode, ClusterNodeId, DistributionConfig,
    DistributionStore, ReplicationTlsConfig, TcpClusterRelocationTransport,
};
use parking_lot::RwLock;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::{ServerConnection, StreamOwned};
use serde_json::{json, Value};

const CLUSTER: &str = "mtls-rpc-deadline-regression-with-fresh-test-identities";
type TlsStream = StreamOwned<ServerConnection, TcpStream>;

#[derive(Clone)]
struct Identity {
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
}

impl Identity {
    fn tls(&self) -> ReplicationTlsConfig {
        ReplicationTlsConfig {
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            ca_path: self.ca_path.clone(),
            require_client_cert: true,
            dev_localhost_plaintext: false,
        }
    }

    fn fingerprint(&self) -> String {
        replication_certificate_sha256(&self.cert_path).unwrap()
    }
}

// Same rcgen/CA/SAN pattern as distribution_learner.rs; generated per fixture.
fn identities(root: &Path) -> BTreeMap<String, Identity> {
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = params.self_signed(&ca_key).unwrap();
    let ca_path = root.join("test-ca.pem");
    fs::write(&ca_path, ca.pem()).unwrap();
    ["client", "server", "other"]
        .into_iter()
        .map(|name| {
            let key = KeyPair::generate().unwrap();
            let mut params =
                CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                    .unwrap();
            params.is_ca = IsCa::ExplicitNoCa;
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let certificate = params.signed_by(&key, &ca, &ca_key).unwrap();
            let identity = Identity {
                cert_path: root.join(format!("{name}.pem")),
                key_path: root.join(format!("{name}.key")),
                ca_path: ca_path.clone(),
            };
            fs::write(&identity.cert_path, certificate.pem()).unwrap();
            fs::write(&identity.key_path, key.serialize_pem()).unwrap();
            (name.to_string(), identity)
        })
        .collect()
}

struct Fixture {
    _root: tempfile::TempDir,
    listener: TcpListener,
    transport: TcpClusterRelocationTransport,
    server_id: ClusterNodeId,
    server_identity: Identity,
    client_pin: String,
}

impl Fixture {
    fn new(io_timeout_ms: u64, wrong_server_pin: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let identities = identities(root.path());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client_id = ClusterNodeId::new("client").unwrap();
        let server_id = ClusterNodeId::new("server").unwrap();
        let network = ClusterNetworkTransportConfig {
            connect_timeout_ms: 1_000,
            io_timeout_ms,
            max_frame_bytes: 1024 * 1024,
            max_inbound_connections: 8,
            dev_localhost_plaintext: false,
            tls: Some(identities["client"].tls()),
        };
        let cluster_id = ClusterId::new(CLUSTER).unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: client_id.clone(),
            node_address: "127.0.0.1:9".to_string(),
            node_tls_certificate_sha256: Some(identities["client"].fingerprint()),
            initial_ranges: 1,
            replication_factor: 1,
            transport: network.clone(),
            ..Default::default()
        };
        let mut store =
            DistributionStore::initialize_at(root.path().join("topology"), config, false, 1)
                .unwrap();
        let expected_pin =
            identities[if wrong_server_pin { "other" } else { "server" }].fingerprint();
        store
            .join_node(
                ClusterNode::new(
                    server_id.clone(),
                    listener.local_addr().unwrap().to_string(),
                    1,
                    10_000,
                    2,
                )
                .unwrap()
                .with_tls_certificate_sha256(expected_pin)
                .unwrap(),
                &client_id,
                2,
            )
            .unwrap();
        let transport = TcpClusterRelocationTransport::new(
            cluster_id,
            client_id,
            Arc::new(RwLock::new(store.topology().clone())),
            network,
        )
        .unwrap();
        Self {
            _root: root,
            listener,
            transport,
            server_id,
            server_identity: identities["server"].clone(),
            client_pin: identities["client"].fingerprint(),
        }
    }
}

fn authenticated_peer(
    listener: TcpListener,
    identity: Identity,
    client_pin: String,
    handler: impl FnOnce(&mut TlsStream) + Send + 'static,
) -> thread::JoinHandle<()> {
    let config = build_server_tls(&identity.tls()).unwrap().config;
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        let mut stream = server_tls_stream(config, stream).unwrap();
        assert_eq!(peer_certificate_sha256(&mut stream).unwrap(), client_pin);
        handler(&mut stream);
    })
}

fn pong_frame(stream: &mut TlsStream) -> Vec<u8> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let len = u32::from_be_bytes(header) as usize;
    assert!(len > 0 && len < 64 * 1024);
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).unwrap();
    let request: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(request["cluster_id"], CLUSTER);
    assert_eq!(request["caller_node_id"], "client");
    assert_eq!(request["destination_node_id"], "server");
    assert_eq!(request["request"], "Ping");
    let body = serde_json::to_vec(&json!({
        "protocol_version": request["protocol_version"],
        "cluster_id": request["cluster_id"],
        "source_node_id": request["destination_node_id"],
        "request_id": request["request_id"],
        "response": {"Ok": "Pong"}
    }))
    .unwrap();
    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
    frame.extend(body);
    frame
}

fn assert_deadline_error(error: impl std::fmt::Display) {
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("deadline")
            || message.contains("timed out")
            || message.contains("timedout"),
        "expected timeout/deadline, got {message}"
    );
}

#[test]
fn mtls_healthy_authenticated_cluster_rpc_succeeds() {
    let fixture = Fixture::new(2_000, false);
    let peer = authenticated_peer(
        fixture.listener,
        fixture.server_identity,
        fixture.client_pin,
        |stream| {
            let frame = pong_frame(stream);
            stream.write_all(&frame).unwrap();
            stream.flush().unwrap();
        },
    );
    fixture.transport.ping(&fixture.server_id).unwrap();
    peer.join().unwrap();
}

#[test]
fn mtls_stalled_handshake_observes_cluster_rpc_deadline() {
    let fixture = Fixture::new(250, false);
    let (finished, wait) = mpsc::channel();
    let peer = thread::spawn(move || {
        let (mut stream, _) = fixture.listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut first = [0];
        stream.read_exact(&mut first).unwrap();
        assert_eq!(
            first[0], 0x16,
            "peer should receive a TLS ClientHello record"
        );
        // Keep the TCP connection open, but never send a ServerHello.
        let _ = wait.recv_timeout(Duration::from_secs(2));
    });
    let started = Instant::now();
    let error = fixture.transport.ping(&fixture.server_id).unwrap_err();
    let elapsed = started.elapsed();
    let _ = finished.send(());
    peer.join().unwrap();
    assert_deadline_error(error);
    assert!(elapsed < Duration::from_secs(2));
}

#[test]
fn mtls_encrypted_response_progress_does_not_extend_cluster_rpc_deadline() {
    let fixture = Fixture::new(250, false);
    let chunks = Arc::new(AtomicUsize::new(0));
    let sent = Arc::clone(&chunks);
    let peer = authenticated_peer(
        fixture.listener,
        fixture.server_identity,
        fixture.client_pin,
        move |stream| {
            let frame = pong_frame(stream);
            assert!(frame.len().div_ceil(4) * 15 > 500);
            for chunk in frame.chunks(4) {
                // Each flush emits encrypted TLS records, not raw JSON on TCP.
                if stream
                    .write_all(chunk)
                    .and_then(|_| stream.flush())
                    .is_err()
                {
                    break;
                }
                sent.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(15));
            }
        },
    );
    let started = Instant::now();
    let error = fixture.transport.ping(&fixture.server_id).unwrap_err();
    let elapsed = started.elapsed();
    peer.join().unwrap();
    assert_deadline_error(error);
    assert!(
        chunks.load(Ordering::SeqCst) >= 2,
        "must exercise continued encrypted progress"
    );
    println!(
        "{}",
        json!({"io_budget_ms":250,"elapsed_ms":elapsed.as_secs_f64()*1000.0,
        "encrypted_chunks_sent":chunks.load(Ordering::SeqCst),"physical_ha_qualification":false})
    );
    assert!(elapsed < Duration::from_secs(2));
}

#[test]
fn mtls_ca_valid_wrong_peer_pin_remains_rejected() {
    let fixture = Fixture::new(2_000, true);
    let config = build_server_tls(&fixture.server_identity.tls())
        .unwrap()
        .config;
    let peer = thread::spawn(move || {
        let (stream, _) = fixture.listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut stream = server_tls_stream(config, stream).unwrap();
        // The client may close immediately after checking the server leaf pin.
        if let Ok(client_pin) = peer_certificate_sha256(&mut stream) {
            assert_eq!(client_pin, fixture.client_pin);
            let mut byte = [0];
            assert!(
                !matches!(stream.read(&mut byte), Ok(n) if n > 0),
                "a rejected destination must not receive an application request"
            );
        }
    });
    let error = fixture.transport.ping(&fixture.server_id).unwrap_err();
    peer.join().unwrap();
    assert!(
        error
            .to_string()
            .contains("certificate fingerprint does not match membership"),
        "expected membership-pin rejection, got {error}"
    );
}
