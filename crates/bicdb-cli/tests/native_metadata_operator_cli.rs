//! Real executable/process commands. Metadata-only enrollment is not native
//! data Serving, a fifth backend, automatic election or a multi-host result.
use bicdb_core::{
    ClusterId, ClusterNetworkTransportConfig, ClusterNode, ClusterNodeId, CollectionMode,
    DistributionConfig, NativeMetadataBootstrapPlan, NativeMetadataCollection,
    ReplicationTlsConfig,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::Value;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_bicdb"))
}
struct Processes {
    children: Vec<Child>,
    root: Option<tempfile::TempDir>,
}
impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.children {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if std::thread::panicking() {
            if let Some(root) = self.root.take() {
                eprintln!(
                    "RETAINED_NATIVE_METADATA_OPERATOR_PRIVATE {}",
                    root.keep().display()
                );
            }
        }
    }
}
fn paths(root: &Path, i: usize) -> (PathBuf, PathBuf) {
    (
        root.join(format!("node{i}.command")),
        root.join(format!("node{i}.status")),
    )
}
fn step(root: &Path, i: usize, seq: u64, action: &str) -> Value {
    step_target(root, i, seq, action, None)
}
fn step_target(root: &Path, i: usize, seq: u64, action: &str, target: Option<&str>) -> Value {
    let (command, status) = paths(root, i);
    let mut call = binary();
    call.args(["cluster", "native-metadata", "step", "--command-file"])
        .arg(command)
        .arg("--status-file")
        .arg(&status)
        .arg("--sequence")
        .arg(seq.to_string())
        .arg("--action")
        .arg(action);
    if let Some(target) = target {
        call.arg("--target-node-id").arg(target);
    }
    let output = call.output().unwrap();
    assert!(
        output.status.success(),
        "operator {action} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(reply["sequence"], seq);
    assert_eq!(reply["data_serving"], false);
    assert_eq!(reply["host"]["data_serving"], false);
    reply
}
fn add_tls(command: &mut Command, root: &Path, i: usize) {
    command
        .arg("--cluster-tls-cert")
        .arg(root.join(format!("n{i}.pem")))
        .arg("--cluster-tls-key")
        .arg(root.join(format!("n{i}.key")))
        .arg("--cluster-tls-ca")
        .arg(root.join("ca.pem"));
}
#[test]
fn native_metadata_operator_actual_cli_four_process_startup_registration_and_data_refusal() {
    let root = tempfile::tempdir().unwrap();
    let mut processes = Processes {
        children: Vec::new(),
        root: Some(root),
    };
    let root = processes.root.as_ref().unwrap().path().to_path_buf();
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = ca_params.self_signed(&ca_key).unwrap();
    std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
    let ports = (0..5)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = ports
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect::<Vec<_>>();
    let mut members = Vec::new();
    for i in 0..5 {
        let key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let cert = p.signed_by(&key, &ca, &ca_key).unwrap();
        let cert_path = root.join(format!("n{i}.pem"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(root.join(format!("n{i}.key")), key.serialize_pem()).unwrap();
        members.push(
            ClusterNode::new(node(i), addresses[i].clone(), 1, 1 << 30, 10)
                .unwrap()
                .with_tls_certificate_sha256(
                    bicdb_core::replication_transport::replication_certificate_sha256(&cert_path)
                        .unwrap(),
                )
                .unwrap(),
        );
    }
    let mut config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("native-operator-cli").unwrap(),
        range_image_bootstrap_id: Some(uuid::Uuid::now_v7()),
        node_id: node(0),
        node_address: addresses[0].clone(),
        node_tls_certificate_sha256: members[0].tls_certificate_sha256.clone(),
        node_capacity_bytes: 1 << 30,
        replication_factor: 3,
        initial_ranges: 4,
        ..Default::default()
    };
    // Seed template paths are intentionally unusable on this host. Every
    // real process must consume its OWN explicit TLS, never those strings.
    config.transport.dev_localhost_plaintext = false;
    config.transport.tls = Some(ReplicationTlsConfig {
        cert_path: PathBuf::from("/seed-vm-only/seed.crt"),
        key_path: PathBuf::from("/seed-vm-only/seed.key"),
        ca_path: PathBuf::from("/seed-vm-only/ca.crt"),
        require_client_cert: true,
        dev_localhost_plaintext: false,
    });
    let plan = NativeMetadataBootstrapPlan {
        version: 1,
        configuration: config.clone(),
        members: members[..4].to_vec(),
        created_at_ms: 10,
        collections: vec![
            NativeMetadataCollection {
                name: "items".into(),
                mode: CollectionMode::Standard,
            },
            NativeMetadataCollection {
                name: bicdb_core::CLUSTER_EVENTS_COLLECTION.into(),
                mode: CollectionMode::Standard,
            },
        ],
    };
    let plan_path = root.join("common-plan.json");
    std::fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();
    drop(ports);
    for i in 0..4 {
        let (command, status) = paths(&root, i);
        let mut process = binary();
        process
            .args(["cluster", "native-metadata", "serve-fresh"])
            .arg(root.join(format!("db{i}")))
            .arg("--plan")
            .arg(&plan_path)
            .arg("--node-id")
            .arg(node(i).as_str())
            .arg("--address")
            .arg(&addresses[i])
            .arg("--command-file")
            .arg(command)
            .arg("--status-file")
            .arg(status)
            .arg("--run-seconds")
            .arg("120");
        add_tls(&mut process, &root, i);
        process
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(root.join(format!("node{i}.stdout"))).unwrap())
            .stderr(std::fs::File::create(root.join(format!("node{i}.stderr"))).unwrap());
        processes.children.push(process.spawn().unwrap());
    }
    let until = Instant::now() + Duration::from_secs(30);
    for i in 0..4 {
        while !paths(&root, i).1.exists() {
            assert!(
                processes.children[i].try_wait().unwrap().is_none(),
                "native metadata process{i} failed; retained private root has logs"
            );
            assert!(
                Instant::now() < until,
                "native metadata public constructor timeout"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let status: Value =
            serde_json::from_slice(&std::fs::read(paths(&root, i).1).unwrap()).unwrap();
        assert_eq!(status["process_id"], processes.children[i].id());
        assert_eq!(status["host"]["phase"], "provisional");
        assert_eq!(status["host"]["data_serving"], false);
    }
    let mut sequences = [0u64; 4];
    for action in ["prepare", "launch"] {
        for i in 0..4 {
            sequences[i] += 1;
            step(&root, i, sequences[i], action);
        }
    }
    sequences[0] += 1;
    step(&root, 0, sequences[0], "elect");
    for i in 0..4 {
        sequences[i] += 1;
        let reply = step(&root, i, sequences[i], "enable-control");
        assert_eq!(reply["host"]["phase"], "metadata_control");
    }
    let mut register = binary();
    register
        .args(["cluster", "native-metadata", "register", "--cluster-id"])
        .arg(config.cluster_id.as_str())
        .arg("--seed-node-id")
        .arg("n0")
        .arg("--seed-address")
        .arg(&addresses[0])
        .arg("--node-id")
        .arg("n4")
        .arg("--address")
        .arg(&addresses[4])
        .arg("--capacity-bytes")
        .arg((1u64 << 30).to_string());
    add_tls(&mut register, &root, 4);
    let output = register.output().unwrap();
    assert!(
        output.status.success(),
        "actual CLI registration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let registered: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(registered["metadata_identity_committed"], true);
    assert_eq!(registered["native_birth_bound"], false);
    assert_eq!(registered["data_serving"], false);
    assert!(
        !root.join("db4").exists(),
        "registration must not provision a generic fifth DB"
    );
    // Continue with a REAL fifth native factory/process, not generic member
    // provisioning. The common seed-only TLS paths are never used by it.
    let (learner_command, learner_status) = paths(&root, 4);
    let mut fifth = binary();
    fifth
        .args(["cluster", "native-metadata", "serve-learner-fresh"])
        .arg(root.join("db4"))
        .arg("--cluster-id")
        .arg(config.cluster_id.as_str())
        .arg("--seed-node-id")
        .arg("n0")
        .arg("--seed-address")
        .arg(&addresses[0])
        .arg("--node-id")
        .arg("n4")
        .arg("--address")
        .arg(&addresses[4])
        .arg("--command-file")
        .arg(learner_command)
        .arg("--status-file")
        .arg(&learner_status)
        .arg("--run-seconds")
        .arg("120");
    add_tls(&mut fifth, &root, 4);
    fifth
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("node4.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("node4.stderr")).unwrap());
    processes.children.push(fifth.spawn().unwrap());
    let until = Instant::now() + Duration::from_secs(15);
    while !learner_status.exists() {
        assert!(
            processes.children[4].try_wait().unwrap().is_none(),
            "fifth factory failed; retained node4 stderr contains exact producer refusal"
        );
        assert!(
            Instant::now() < until,
            "actual fifth factory/birth listener timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let provisional: Value =
        serde_json::from_slice(&std::fs::read(&learner_status).unwrap()).unwrap();
    assert_eq!(provisional["host"]["phase"], "provisional_birth");
    assert_eq!(provisional["host"]["birth_bound"], false);
    assert_eq!(provisional["host"]["data_serving"], false);
    assert!(root.join("db4").exists());
    let namespace_ids = provisional["host"]["namespace_ids"].clone();
    assert!(!namespace_ids.as_array().unwrap().is_empty());
    for entry in namespace_ids.as_array().unwrap() {
        assert!(!uuid::Uuid::parse_str(entry[1].as_str().unwrap())
            .unwrap()
            .is_nil());
    }
    sequences[0] += 1;
    let enrolled = step_target(&root, 0, sequences[0], "enroll-learner", Some("n4"));
    assert_eq!(enrolled["host"]["last_enrolled_learner"], "n4");
    let owned = step(&root, 4, 1, "start-learner-metadata");
    assert_eq!(owned["host"]["phase"], "owned_metadata_learner");
    assert_eq!(owned["host"]["birth_bound"], true);
    assert_eq!(owned["host"]["namespace_ids"], namespace_ids);
    assert_eq!(owned["host"]["birth_id"], provisional["host"]["birth_id"]);
    let store_id = owned["host"]["metadata_store_id"].clone();
    assert!(!uuid::Uuid::parse_str(store_id.as_str().unwrap())
        .unwrap()
        .is_nil());
    assert_eq!(owned["host"]["metadata_listener_owned"], true);
    sequences[0] += 1;
    let caught = step_target(&root, 0, sequences[0], "catch-up-learner", Some("n4"));
    assert_eq!(
        caught["host"]["last_caught_up_learner_store_id"], store_id,
        "actual native bound ACK must identify SAME protected hot backend"
    );
    let hot = step(&root, 4, 2, "status");
    assert_eq!(hot["host"]["metadata_store_id"], store_id);
    assert_eq!(hot["host"]["namespace_ids"], namespace_ids);
    assert!(
        hot["host"]["metadata"]["commit_index"].as_u64().unwrap()
            >= enrolled["host"]["metadata"]["commit_index"]
                .as_u64()
                .unwrap()
    );
    // Existing root misuse refuses BEFORE opening/resetting the real host.
    let existing = binary()
        .args(["cluster", "native-metadata", "serve-fresh"])
        .arg(root.join("db0"))
        .arg("--plan")
        .arg(&plan_path)
        .arg("--node-id")
        .arg("n0")
        .arg("--address")
        .arg(&addresses[0])
        .arg("--command-file")
        .arg(root.join("other.command"))
        .arg("--status-file")
        .arg(root.join("other.status"))
        .arg("--cluster-tls-cert")
        .arg(root.join("n0.pem"))
        .arg("--cluster-tls-key")
        .arg(root.join("n0.key"))
        .arg("--cluster-tls-ca")
        .arg(root.join("ca.pem"))
        .output()
        .unwrap();
    assert!(!existing.status.success());
    assert!(!root.join("other.status").exists());
    let network = ClusterNetworkTransportConfig {
        tls: Some(ReplicationTlsConfig {
            cert_path: root.join("n0.pem"),
            key_path: root.join("n0.key"),
            ca_path: root.join("ca.pem"),
            require_client_cert: true,
            dev_localhost_plaintext: false,
        }),
        dev_localhost_plaintext: false,
        max_frame_bytes: 4 * 1024 * 1024,
        ..Default::default()
    };
    let bootstrap = bicdb_core::TcpClusterBootstrapClient::new(
        config.cluster_id.clone(),
        node(0),
        network.clone(),
    )
    .unwrap();
    let snapshot = bootstrap.fetch_snapshot(&node(0), &addresses[0]).unwrap();
    assert_eq!(snapshot.topology.nodes[&node(4)].address, addresses[4]);
    let bound = snapshot.topology.nodes[&node(4)]
        .native_store_birth_v1
        .as_ref()
        .unwrap();
    assert_eq!(
        bound.birth_id().to_string(),
        provisional["host"]["birth_id"].as_str().unwrap()
    );
    assert_eq!(
        snapshot.topology.nodes[&node(4)].metadata_role,
        bicdb_core::MetadataMemberRole::Learner
    );
    assert_eq!(
        provisional["host"]["schema_sha256"].as_str().unwrap(),
        snapshot
            .topology
            .ranges
            .values()
            .next()
            .unwrap()
            .required_schema_sha256(&snapshot.topology.nodes)
            .unwrap()
    );
    // A committed actual birth cannot be replaced with a blank factory using
    // the same node/incarnation, even at another unused local path.
    let replacement_root = root.join("db4-replacement");
    let mut replacement = binary();
    replacement
        .args(["cluster", "native-metadata", "serve-learner-fresh"])
        .arg(&replacement_root)
        .arg("--cluster-id")
        .arg(config.cluster_id.as_str())
        .arg("--seed-node-id")
        .arg("n0")
        .arg("--seed-address")
        .arg(&addresses[0])
        .arg("--node-id")
        .arg("n4")
        .arg("--address")
        .arg(&addresses[4])
        .arg("--command-file")
        .arg(root.join("replacement.command"))
        .arg("--status-file")
        .arg(root.join("replacement.status"));
    add_tls(&mut replacement, &root, 4);
    assert!(!replacement.output().unwrap().status.success());
    assert!(!replacement_root.exists());
    let transport = bicdb_core::TcpClusterRelocationTransport::new(
        config.cluster_id,
        node(0),
        std::sync::Arc::new(parking_lot::RwLock::new(snapshot.topology.clone())),
        network,
    )
    .unwrap();
    let range = snapshot.topology.ranges.values().next().unwrap();
    for i in 0..5 {
        assert!(transport
            .application_event(
                &node(i),
                bicdb_core::ClusterEventRequest {
                    range_id: range.id,
                    range_epoch: range.epoch,
                    operation: bicdb_core::ClusterEventOperation::Scan {
                        namespace: "items".into(),
                        after: None,
                        limit: 1
                    }
                }
            )
            .is_err());
        if i == 4 {
            step(&root, 4, 3, "stop");
        } else {
            sequences[i] += 1;
            step(&root, i, sequences[i], "stop");
        }
    }
    let until = Instant::now() + Duration::from_secs(10);
    for child in &mut processes.children {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < until, "native public host failed to drain");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
