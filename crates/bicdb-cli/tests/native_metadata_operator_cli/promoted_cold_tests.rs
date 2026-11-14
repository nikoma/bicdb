//! Real executable owners. First automatic promotion, then fifth-only SIGKILL
//! and same-root Registered cold reopen. No direct factory/metadata setter.
use super::*;
fn learner_command(
    root: &Path,
    config: &DistributionConfig,
    addresses: &[String],
    mode: &str,
    automatic: bool,
) -> Command {
    let (command, status) = paths(root, 4);
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
    if mode == "join-fresh" {
        call.arg("--capacity-bytes").arg((1u64 << 30).to_string());
    }
    if automatic {
        call.arg("--automatic-metadata");
    }
    add_tls(&mut call, root, 4);
    call.stdin(Stdio::null());
    call
}
fn wait_ready(processes: &mut Processes, root: &Path) -> Value {
    let until = Instant::now() + Duration::from_secs(45);
    loop {
        for (i, child) in processes.children.iter_mut().enumerate() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "actual process{i} exited; retained stdout/stderr contain phase"
            );
        }
        if let Ok(bytes) = std::fs::read(paths(root, 4).1) {
            let reply: Value = serde_json::from_slice(&bytes).unwrap();
            if reply["error"].is_null()
                && reply["host"]["phase"] == "owned_metadata_voter"
                && reply["host"]["metadata_control_owned"] == true
                && reply["host"]["metadata_listener_owned"] == true
            {
                return reply;
            }
        }
        assert!(Instant::now()<until,"actual promoted/cold fifth never reached strict installed-member control; retained files show phase");
        std::thread::sleep(Duration::from_millis(20));
    }
}
#[test]
fn native_metadata_operator_actual_cli_automatic_promoted_voter_kill_reopen_same_root() {
    let (mut processes, root, config, addresses, mut sequences) = four_processes(false);
    let mut fresh = learner_command(&root, &config, &addresses, "join-fresh", false);
    fresh
        .stdout(std::fs::File::create(root.join("node4.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("node4.stderr")).unwrap());
    let child = processes
        .spawn_recorded(&mut fresh, &root.join("db4"))
        .unwrap();
    processes.children.push(child);
    let ready = wait_ready(&mut processes, &root);
    let id = ready["host"]["metadata_store_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(!uuid::Uuid::parse_str(&id).unwrap().is_nil());
    assert_eq!(ready["host"]["birth_bound"], true);
    assert_eq!(ready["host"]["data_serving"], false);
    // Keep the genuine leader projection of the actual bound receiver ACK as
    // a pre-kill witness; no cached projection authorizes the cold constructor.
    let until = Instant::now() + Duration::from_secs(45);
    let receipt = loop {
        let sources = (0..4)
            .map(|i| {
                serde_json::from_slice::<Value>(&std::fs::read(paths(&root, i).1).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        if let Some(receipt) = sources.iter().find_map(|reply| {
            let receipt = &reply["host"]["last_metadata_promotion"];
            (receipt["node"] == "n4" && receipt["stage"] == "promoted_and_installed")
                .then(|| receipt.clone())
        }) {
            break receipt;
        }
        assert!(
            Instant::now() < until,
            "leader has not projected its genuine protected receiver ACK"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(receipt["receiver_store_id"], id);
    assert!(
        !paths(&root, 4).0.exists(),
        "join used no fifth diagnostic actions"
    );
    let old_pid = processes.children[4].id();
    processes.children[4].kill().unwrap();
    let killed = processes.children[4].wait().unwrap();
    processes.record_prior_exit(old_pid, &killed);
    assert!(!killed.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(killed.signal(), Some(libc::SIGKILL));
    }
    // Exact CRASH cut is read AFTER wait, not the earlier periodically refreshed
    // status. No DB engine opens it from this test process.
    let metadata = root.join("db4/native-learner-metadata/cluster-metadata-consensus.json");
    let original = root.join("db4/native-learner-metadata/native-learner-metadata-original-c.json");
    let at_kill = std::fs::read(&metadata).unwrap();
    let original_c = std::fs::read(&original).unwrap();
    let before: Value = serde_json::from_slice(&at_kill).unwrap();
    assert_eq!(before["state"]["native_store_id"], id);
    // Voter is the schema default and is deliberately omitted from serialized
    // ClusterNode values. Decode the actual persisted topology before checking.
    let at_kill_topology: bicdb_core::ClusterTopology =
        serde_json::from_value(before["state"]["committed_topology"].clone()).unwrap();
    assert_eq!(
        at_kill_topology.nodes[&node(4)].metadata_role,
        bicdb_core::MetadataMemberRole::Voter
    );
    std::fs::write(root.join("node4-promoted-kill-state.json"), &at_kill).unwrap();
    std::fs::write(
        root.join("node4-promoted-before.json"),
        serde_json::to_vec(&ready).unwrap(),
    )
    .unwrap();
    std::fs::rename(
        paths(&root, 4).1,
        root.join("node4.promoted-pre-kill.status"),
    )
    .unwrap();
    let refused = learner_command(&root, &config, &addresses, "join-fresh", false)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr)
        .contains("join-fresh refuses an existing native root"));
    assert_eq!(std::fs::read(&metadata).unwrap(), at_kill);
    assert!(!paths(&root, 4).0.exists() && !paths(&root, 4).1.exists());
    // This regression's intended supported boundary is a current leader in the
    // retained C. Wait for a REAL live old cohort leader if n4 itself was leader;
    // never install a leader hint/term or synthesize an MQ snapshot.
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let leaders = (0..4)
            .filter_map(|i| {
                std::fs::read(paths(&root, i).1)
                    .ok()
                    .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                    .filter(|v| v["host"]["metadata"]["role"] == "leader")
            })
            .count();
        if leaders == 1 {
            break;
        }
        assert!(
            Instant::now() < until,
            "remaining actual metadata cohort did not report a current leader"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut cold = learner_command(&root, &config, &addresses, "serve-learner-reopen", true);
    cold.stdout(std::fs::File::create(root.join("node4-promoted-cold.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("node4-promoted-cold.stderr")).unwrap());
    processes.children[4] = processes
        .spawn_recorded(&mut cold, &root.join("db4"))
        .unwrap();
    let reopened = wait_ready(&mut processes, &root);
    assert_ne!(processes.children[4].id(), old_pid);
    assert_eq!(reopened["process_id"], processes.children[4].id());
    for field in [
        "metadata_store_id",
        "birth_id",
        "birth_sha256",
        "schema_sha256",
        "namespace_ids",
    ] {
        assert_eq!(
            reopened["host"][field], ready["host"][field],
            "cold changed actual {field}"
        );
    }
    assert_eq!(reopened["sequence"], 0);
    assert!(
        !paths(&root, 4).0.exists(),
        "cold automatic mode used no diagnostic action"
    );
    assert_eq!(reopened["host"]["data_serving"], false);
    let after: Value = serde_json::from_slice(&std::fs::read(&metadata).unwrap()).unwrap();
    let durable = |value: &Value| {
        let mut state = value["state"].clone();
        state.as_object_mut().unwrap().remove("role");
        state.as_object_mut().unwrap().remove("leader_id");
        state
    };
    assert_eq!(durable(&after),durable(&before),"cold preserves actual term/vote/base/log/commit/config/restore/UUID; only volatile labels may change");
    assert_eq!(std::fs::read(&original).unwrap(), original_c);
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
    assert!(snapshot
        .topology
        .ranges
        .values()
        .all(|r| r.replicas.iter().all(|p| p.node_id != node(4))));
    let transport = bicdb_core::TcpClusterRelocationTransport::new(
        config.cluster_id,
        node(0),
        std::sync::Arc::new(parking_lot::RwLock::new(snapshot.topology.clone())),
        network,
    )
    .unwrap();
    let range = snapshot.topology.ranges.values().next().unwrap();
    for i in 0..5 {
        transport.ping(&node(i)).unwrap(); // TLS Ping, not application health.
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
                        }
                    }
                )
                .is_err(),
            "live node{i} must refuse ordinary data"
        );
    }
    // Force a NEW real 3/5 MQ to include the cold voter: gracefully stop two
    // NONleader founders only AFTER identity/state parity and all5 data refusals.
    // This is a distinct metadata-quorum check, not a claim those roots can
    // themselves cold-restart or that any native-data ACK survived an outage.
    //
    // A strict cold reopen clears the volatile leader label, and readiness
    // (`owned_metadata_voter` with owned control and listener) does not promise
    // leader contact: the first ready status can precede the retained leader's
    // first heartbeat to the reopened process. So read the leader from the
    // reopened process's own periodically refreshed status once it names one.
    // No label is installed and nothing is restarted; a leader that is really
    // absent, or a different one, still fails here.
    let until = Instant::now() + Duration::from_secs(15);
    let led = loop {
        assert!(
            processes.children[4].try_wait().unwrap().is_none(),
            "cold fifth exited before it learned its leader"
        );
        let status: Value =
            serde_json::from_slice(&std::fs::read(paths(&root, 4).1).unwrap()).unwrap();
        assert_eq!(status["process_id"], processes.children[4].id());
        if status["error"].is_null() && !status["host"]["metadata"]["leader_id"].is_null() {
            break status;
        }
        assert!(
            Instant::now() < until,
            "cold fifth never heard from a metadata leader: {status}"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        led["host"]["metadata"]["leader_id"], "n0",
        "this first promoted-cold control covers the healthy retained leader n0"
    );
    let stopped = [2usize, 3usize];
    for i in stopped {
        sequences[i] += 1;
        step(&root, i, sequences[i], "stop");
        let until = Instant::now() + Duration::from_secs(10);
        while processes.children[i].try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < until,
                "stopped founder{i} still owns an endpoint"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let live = step(&root, 4, 1, "status");
    assert_eq!(live["host"]["phase"], "owned_metadata_voter");
    assert_eq!(live["host"]["metadata_control_owned"], true);
    assert_eq!(live["host"]["metadata_store_id"], id);
    assert_eq!(live["host"]["data_serving"], false);
}
