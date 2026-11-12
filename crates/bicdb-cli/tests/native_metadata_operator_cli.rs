//! Real executable/process commands. Metadata-only enrollment is not native
//! data Serving, automatic election or a multi-host result. Cold coverage kills
//! the fifth process only; the four existing metadata owners remain live.
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
#[path = "native_metadata_operator_cli/custody.rs"]
mod custody;
#[path = "native_metadata_operator_cli/database_custody.rs"]
mod database_custody;

struct Processes {
    children: Vec<Child>,
    root: Option<tempfile::TempDir>,
    spawn_records: Vec<Value>,
    prior_exits: Vec<Value>,
    owned_database_roots: std::collections::BTreeSet<PathBuf>,
}
impl Drop for Processes {
    fn drop(&mut self) {
        let exits = self.stop_children_for_drop();
        if std::thread::panicking() {
            self.capture_cli_failure(&exits);
            self.capture_cli_database_failure(&exits);
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
fn four_processes(
    manual_enrollment: bool,
) -> (
    Processes,
    PathBuf,
    DistributionConfig,
    Vec<String>,
    [u64; 4],
) {
    let root = tempfile::tempdir().unwrap();
    let mut processes = Processes {
        children: Vec::new(),
        root: Some(root),
        spawn_records: Vec::new(),
        prior_exits: Vec::new(),
        owned_database_roots: Default::default(),
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
    let ports = (0..6)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = ports
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect::<Vec<_>>();
    let mut members = Vec::new();
    for i in 0..6 {
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
        if manual_enrollment {
            process.arg("--manual-enrollment");
        }
        add_tls(&mut process, &root, i);
        process
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(root.join(format!("node{i}.stdout"))).unwrap())
            .stderr(std::fs::File::create(root.join(format!("node{i}.stderr"))).unwrap());
        let child = processes
            .spawn_recorded(&mut process, &root.join(format!("db{i}")))
            .unwrap();
        processes.children.push(child);
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
    (processes, root, config, addresses, sequences)
}
#[test]
fn native_metadata_operator_actual_cli_four_process_startup_registration_and_data_refusal() {
    let (mut processes, root, config, addresses, mut sequences) = four_processes(true);
    let plan_path = root.join("common-plan.json");
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
    // Own actual TLS is insufficient when the explicitly supplied endpoint
    // disagrees with the identity just committed through the real controller.
    // Use another valid numeric nonzero address so this reaches the actual
    // CONFIG comparison, not the earlier endpoint-syntax validation.
    let mut other_port = TcpListener::bind("127.0.0.1:0").unwrap();
    if other_port.local_addr().unwrap().to_string() == addresses[4] {
        // Keep that original socket held while obtaining another actual port.
        other_port = TcpListener::bind("127.0.0.1:0").unwrap();
    }
    let wrong_address = other_port.local_addr().unwrap().to_string();
    assert_ne!(wrong_address, addresses[4]);
    let wrong_root = root.join("db4-wrong-address");
    let wrong_command = root.join("wrong-address.command");
    let wrong_status = root.join("wrong-address.status");
    let mut wrong = binary();
    wrong
        .args(["cluster", "native-metadata", "serve-learner-fresh"])
        .arg(&wrong_root)
        .arg("--cluster-id")
        .arg(config.cluster_id.as_str())
        .arg("--seed-node-id")
        .arg("n0")
        .arg("--seed-address")
        .arg(&addresses[0])
        .arg("--node-id")
        .arg("n4")
        .arg("--address")
        .arg(&wrong_address)
        .arg("--command-file")
        .arg(&wrong_command)
        .arg("--status-file")
        .arg(&wrong_status);
    add_tls(&mut wrong, &root, 4);
    let refused = wrong.output().unwrap();
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr)
            .contains("fresh learner registration/address/incarnation/birth differs"),
        "wrong endpoint did not reach actual committed registration validation: {}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        !wrong_root.exists(),
        "address mismatch must refuse BEFORE any factory root creation"
    );
    assert!(!wrong_command.exists() && !wrong_status.exists());
    drop(other_port);
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
    let child = processes
        .spawn_recorded(&mut fifth, &root.join("db4"))
        .unwrap();
    processes.children.push(child);
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
    // A real process crash drops EVERY fifth-node owner. Preserve the killed
    // process outputs/operator files and exact durable state at the cut. The
    // four metadata voters remain live; this is not an all-cluster outage.
    let killed_pid = processes.children[4].id();
    processes.children[4].kill().unwrap();
    let killed = processes.children[4].wait().unwrap();
    processes.record_prior_exit(killed_pid, &killed);
    assert!(!killed.success());
    let metadata_path = root.join("db4/native-learner-metadata/cluster-metadata-consensus.json");
    let original_c_path =
        root.join("db4/native-learner-metadata/native-learner-metadata-original-c.json");
    let original_c_at_kill = std::fs::read(&original_c_path).unwrap();
    let persisted_at_kill = std::fs::read(&metadata_path).unwrap();
    let persisted_state: Value = serde_json::from_slice(&persisted_at_kill).unwrap();
    assert_eq!(persisted_state["state"]["native_store_id"], store_id);
    assert!(persisted_state["state"]["commit_index"].as_u64().unwrap() > 0);
    std::fs::write(root.join("node4-killed-metadata.json"), &persisted_at_kill).unwrap();
    std::fs::rename(paths(&root, 4).0, root.join("node4.pre-kill.command")).unwrap();
    std::fs::rename(&learner_status, root.join("node4.pre-kill.status")).unwrap();
    let learner_process = |mode: &str, command: &Path, status: &Path| {
        let mut call = binary();
        call.args(["cluster", "native-metadata", mode])
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
            .arg(command)
            .arg("--status-file")
            .arg(status)
            .arg("--run-seconds")
            .arg("120");
        add_tls(&mut call, &root, 4);
        call
    };
    // Fresh MUST still refuse the same existing root, before touching the
    // original protected metadata state or publishing a status/listener.
    let fresh_command = root.join("fresh-over-existing.command");
    let fresh_status = root.join("fresh-over-existing.status");
    let refused = learner_process("serve-learner-fresh", &fresh_command, &fresh_status)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(!fresh_command.exists() && !fresh_status.exists());
    assert_eq!(std::fs::read(&metadata_path).unwrap(), persisted_at_kill);
    let (cold_command, cold_status) = paths(&root, 4);
    let mut cold_process = learner_process("serve-learner-reopen", &cold_command, &cold_status);
    cold_process
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("node4-cold.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("node4-cold.stderr")).unwrap());
    let cold_child = processes
        .spawn_recorded(&mut cold_process, &root.join("db4"))
        .unwrap();
    processes.children[4] = cold_child;
    let until = Instant::now() + Duration::from_secs(15);
    while !cold_status.exists() {
        assert!(
            processes.children[4].try_wait().unwrap().is_none(),
            "cold fifth failed; retained node4-cold.stderr contains exact refusal"
        );
        assert!(
            Instant::now() < until,
            "strict existing fifth reopen timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let cold: Value = serde_json::from_slice(&std::fs::read(&cold_status).unwrap()).unwrap();
    assert_eq!(cold["process_id"], processes.children[4].id());
    assert_ne!(processes.children[4].id(), killed_pid);
    assert_eq!(cold["host"]["phase"], "metadata_transition");
    assert_eq!(cold["host"]["birth_bound"], true);
    assert_eq!(cold["host"]["birth_id"], provisional["host"]["birth_id"]);
    assert_eq!(
        cold["host"]["birth_sha256"],
        provisional["host"]["birth_sha256"]
    );
    assert_eq!(
        cold["host"]["schema_sha256"],
        provisional["host"]["schema_sha256"]
    );
    assert_eq!(cold["host"]["namespace_ids"], namespace_ids);
    assert_eq!(cold["host"]["metadata_store_id"], Value::Null);
    assert_eq!(cold["host"]["metadata_listener_owned"], false);
    assert_eq!(cold["host"]["data_serving"], false);
    // Reopening the actual factory alone neither recreates a metadata backend
    // nor resets its state. Starting it is a separate fresh OWN-MQ action.
    assert_eq!(std::fs::read(&metadata_path).unwrap(), persisted_at_kill);
    let resumed = step(&root, 4, 1, "start-learner-metadata");
    assert_eq!(resumed["host"]["phase"], "owned_metadata_learner");
    assert_eq!(resumed["host"]["metadata_store_id"], store_id);
    assert_eq!(resumed["host"]["metadata_listener_owned"], true);
    assert_eq!(
        resumed["host"]["birth_sha256"],
        provisional["host"]["birth_sha256"]
    );
    assert_eq!(resumed["host"]["namespace_ids"], namespace_ids);
    let resumed_state: Value =
        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
    let durable = |envelope: &Value| {
        let mut state = envelope["state"].clone();
        // These two labels are volatile on strict reopen. All term/vote/base/
        // log/commit/config/restore/native UUID fields must remain EXACT.
        state.as_object_mut().unwrap().remove("role");
        state.as_object_mut().unwrap().remove("leader_id");
        state
    };
    assert_eq!(durable(&resumed_state), durable(&persisted_state));
    assert_eq!(
        std::fs::read(&original_c_path).unwrap(),
        original_c_at_kill,
        "Registered reopen must preserve the exact original-C historical fact"
    );
    sequences[0] += 1;
    let caught_cold = step_target(&root, 0, sequences[0], "catch-up-learner", Some("n4"));
    assert_eq!(
        caught_cold["host"]["last_caught_up_learner_store_id"], store_id,
        "new TLS bound ACK must come from the SAME protected cold backend"
    );
    let after_cold_ack = step(&root, 4, 2, "status");
    assert_eq!(after_cold_ack["host"]["metadata_store_id"], store_id);
    assert_eq!(after_cold_ack["host"]["namespace_ids"], namespace_ids);
    assert_eq!(
        after_cold_ack["host"]["metadata"]["learners"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|id| id.as_str() == Some("n4"))
            .count(),
        1
    );
    assert!(!after_cold_ack["host"]["metadata"]["voters"]
        .as_array()
        .unwrap()
        .iter()
        .any(|id| id.as_str() == Some("n4")));
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

/// Four live native processes maintain metadata automatically. The fifth runs
/// exactly ONE public join command; no diagnostic registration/start/enroll/
/// catch-up step participates in this positive.
#[test]
fn native_metadata_operator_actual_cli_single_join_fresh_becomes_local_metadata_voter() {
    let (mut processes, root, config, addresses, _sequences) = four_processes(false);
    let (command, status) = paths(&root, 4);
    let mut fifth = binary();
    fifth
        .args(["cluster", "native-metadata", "join-fresh"])
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
        .arg("--capacity-bytes")
        .arg((1u64 << 30).to_string())
        .arg("--command-file")
        .arg(&command)
        .arg("--status-file")
        .arg(&status)
        .arg("--run-seconds")
        .arg("120");
    add_tls(&mut fifth, &root, 4);
    fifth
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("node4.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("node4.stderr")).unwrap());
    let child = processes
        .spawn_recorded(&mut fifth, &root.join("db4"))
        .unwrap();
    processes.children.push(child);
    let until = Instant::now() + Duration::from_secs(45);
    let (ready, source) = loop {
        for (i, child) in processes.children.iter_mut().enumerate() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "real native process{i} exited; logs retained"
            );
        }
        let ready: Option<Value> = std::fs::read(&status)
            .ok()
            .map(|b| serde_json::from_slice(&b).unwrap());
        let sources = (0..4)
            .map(|i| {
                serde_json::from_slice::<Value>(&std::fs::read(paths(&root, i).1).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        if let Some(ready) = ready {
            let host = &ready["host"];
            if ready["error"].is_null()
                && host["phase"] == "owned_metadata_voter"
                && host["metadata_control_owned"] == true
                && host["metadata"]["voters"]
                    .as_array()
                    .is_some_and(|v| v.contains(&serde_json::json!("n4")))
            {
                if let Some(source) = sources
                    .into_iter()
                    .find(|s| s["host"]["last_metadata_promotion"]["node"] == "n4")
                {
                    break (ready, source);
                }
            }
        }
        assert!(
            Instant::now() < until,
            "single join did not reach actual local installed voter state; retained logs"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(ready["error"].is_null(), "{ready}");
    assert_eq!(ready["host"]["birth_bound"], true);
    assert_eq!(ready["host"]["metadata_listener_owned"], true);
    assert_eq!(ready["host"]["data_serving"], false);
    let id = ready["host"]["metadata_store_id"].as_str().unwrap();
    assert!(!uuid::Uuid::parse_str(id).unwrap().is_nil());
    let receipt = &source["host"]["last_metadata_promotion"];
    assert_eq!(receipt["stage"], "promoted_and_installed");
    assert_eq!(receipt["receiver_store_id"], id);
    assert!(
        ready["host"]["metadata"]["commit_index"].as_u64().unwrap()
            >= receipt["installed_through_index"].as_u64().unwrap()
    );
    assert!(!ready["host"]["namespace_ids"]
        .as_array()
        .unwrap()
        .is_empty());
    // No diagnostic action was submitted to the fifth, even for polling.
    assert!(!command.exists());
    assert_eq!(ready["sequence"], 0);
    // Now exercise the lifecycle that a promotion-only result cannot prove:
    // the fifth campaigns using real authenticated votes/current-term barrier,
    // then exercises current-leader registration/control for a sixth backend.
    // The CLI still fetches schema from an actual original data leader; exact
    // fifth-node schema mirror handling is a separate deterministic Core case.
    let elected = step(&root, 4, 1, "elect");
    assert_eq!(elected["host"]["metadata"]["role"], "leader");
    assert_eq!(elected["host"]["metadata"]["node_id"], "n4");
    assert_eq!(elected["host"]["metadata_control_owned"], true);
    let (sixth_command, sixth_status) = paths(&root, 5);
    let mut sixth = binary();
    sixth
        .args(["cluster", "native-metadata", "join-fresh"])
        .arg(root.join("db5"))
        .arg("--cluster-id")
        .arg(config.cluster_id.as_str())
        // Keep the ORIGINAL seed hint. It must discover actual current leader
        // authority after the real fifth election, including later elections.
        .arg("--seed-node-id")
        .arg("n0")
        .arg("--seed-address")
        .arg(&addresses[0])
        .arg("--node-id")
        .arg("n5")
        .arg("--address")
        .arg(&addresses[5])
        .arg("--capacity-bytes")
        .arg((1u64 << 30).to_string())
        .arg("--command-file")
        .arg(&sixth_command)
        .arg("--status-file")
        .arg(&sixth_status)
        .arg("--run-seconds")
        .arg("120");
    add_tls(&mut sixth, &root, 5);
    sixth
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("node5.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("node5.stderr")).unwrap());
    let child = processes
        .spawn_recorded(&mut sixth, &root.join("db5"))
        .unwrap();
    processes.children.push(child);
    let until = Instant::now() + Duration::from_secs(45);
    let sixth = loop {
        for (i, child) in processes.children.iter_mut().enumerate() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "real native process{i} exited; logs retained"
            );
        }
        let actual: Option<Value> = std::fs::read(&sixth_status)
            .ok()
            .map(|b| serde_json::from_slice(&b).unwrap());
        if let Some(actual) = actual {
            if actual["error"].is_null()
                && actual["host"]["phase"] == "owned_metadata_voter"
                && actual["host"]["metadata_control_owned"] == true
                && actual["host"]["metadata"]["voters"]
                    .as_array()
                    .is_some_and(|v| v.contains(&serde_json::json!("n5")))
            {
                break actual;
            }
        }
        assert!(
            Instant::now() < until,
            "sixth join through actual promoted metadata leader did not finish; logs retained"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(sixth["error"].is_null(), "{sixth}");
    assert_eq!(sixth["host"]["birth_bound"], true);
    assert_eq!(sixth["host"]["metadata_listener_owned"], true);
    assert_eq!(sixth["host"]["data_serving"], false);
    let sixth_id = sixth["host"]["metadata_store_id"].as_str().unwrap();
    assert!(!uuid::Uuid::parse_str(sixth_id).unwrap().is_nil());
    assert_ne!(sixth_id, id);
    assert!(
        !sixth_command.exists(),
        "no hidden sixth diagnostic protocol actions"
    );
    assert_eq!(sixth["sequence"], 0);
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
    assert_eq!(
        snapshot.topology.nodes[&node(4)].metadata_role,
        bicdb_core::MetadataMemberRole::Voter
    );
    let current_leader = snapshot.metadata_leader_id.as_ref().unwrap();
    assert_eq!(
        snapshot.topology.nodes[current_leader].metadata_role,
        bicdb_core::MetadataMemberRole::Voter
    );
    let registered = snapshot
        .topology
        .history
        .iter()
        .filter(|event| event.summary == "node n5 joined as metadata Learner")
        .collect::<Vec<_>>();
    assert_eq!(registered.len(), 1);
    let event = registered[0];
    assert!(
        event.generation
            > elected["host"]["metadata"]["topology_generation"]
                .as_u64()
                .unwrap()
    );
    assert!(event.generation <= snapshot.topology.generation);
    assert_eq!(
        snapshot.topology.nodes[&event.actor].metadata_role,
        bicdb_core::MetadataMemberRole::Voter
    );
    assert!(snapshot.topology.nodes[&event.actor]
        .native_store_birth_v1
        .is_some());
    // This committed audit records the actual proposing store's actor under
    // fresh leader MQ. Do not require it remain leader after autonomous votes.
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(root.join("db5/native-unassigned-learner.json")).unwrap(),
    )
    .unwrap();
    assert!(
        !uuid::Uuid::parse_str(manifest["initial_metadata_context"].as_str().unwrap())
            .unwrap()
            .is_nil()
    );
    assert!(
        manifest["initial_metadata_term"].as_u64().unwrap()
            >= elected["host"]["metadata"]["current_term"]
                .as_u64()
                .unwrap()
    );
    assert!(
        manifest["initial_metadata_read_index"].as_u64().unwrap()
            > elected["host"]["metadata"]["commit_index"]
                .as_u64()
                .unwrap()
    );
    assert_eq!(manifest["schema_sha256"], sixth["host"]["schema_sha256"]);
    assert_eq!(elected["host"]["metadata_store_id"], id);
    eprintln!("NATIVE_SIXTH_REGISTRATION actor={} generation={} factory_mq_term={} factory_mq_index={} schema_source=initial_data_leader", event.actor, event.generation, manifest["initial_metadata_term"], manifest["initial_metadata_read_index"]);
    assert_eq!(
        snapshot.topology.nodes[&node(5)].metadata_role,
        bicdb_core::MetadataMemberRole::Voter
    );
    assert_eq!(
        snapshot.topology.nodes[&node(5)]
            .native_store_birth_v1
            .as_ref()
            .unwrap()
            .birth_id()
            .to_string(),
        sixth["host"]["birth_id"].as_str().unwrap()
    );
    assert!(snapshot.topology.ranges.values().all(|r| r
        .replicas
        .iter()
        .all(|p| p.node_id != node(4) && p.node_id != node(5))));
    let transport = bicdb_core::TcpClusterRelocationTransport::new(
        config.cluster_id,
        node(0),
        std::sync::Arc::new(parking_lot::RwLock::new(snapshot.topology.clone())),
        network,
    )
    .unwrap();
    let range = snapshot.topology.ranges.values().next().unwrap();
    for i in 0..6 {
        assert!(
            transport
                .application_event(
                    &node(i),
                    bicdb_core::ClusterEventRequest {
                        range_id: range.id,
                        range_epoch: range.epoch,
                        operation: bicdb_core::ClusterEventOperation::Scan {
                            namespace: "items".into(),
                            after: None,
                            limit: 1,
                        },
                    }
                )
                .is_err(),
            "node{i} must remain metadata-only"
        );
    }
}

#[path = "native_metadata_operator_cli/promoted_cold_tests.rs"]
mod promoted_cold_tests;

#[path = "native_metadata_operator_cli/initial_data_tests.rs"]
mod initial_data_tests;

#[path = "native_metadata_operator_cli/replica_receiver_tests.rs"]
mod replica_receiver_tests;
