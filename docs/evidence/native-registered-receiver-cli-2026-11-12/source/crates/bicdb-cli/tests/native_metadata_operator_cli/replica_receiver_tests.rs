//! Real first-four DATA ACK/Strong, then ONE fresh fifth process command.
//! Receiver-ready is NOT image installation, relocation or DATA serving.
use super::initial_data_tests::{actual_cli_identity_before_phase, journal, open_data};
use super::*;

#[test]
fn native_registered_replica_receiver_actual_cli_four_data_then_fifth_same_owner_no_data_grant() {
    let identity = actual_cli_identity_before_phase();
    let (mut processes, root, mut config, addresses, mut sequences) = four_processes(false);
    let mut ledger = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("registered-replica-external-ledger.jsonl"))
        .unwrap();
    journal(
        &mut ledger,
        serde_json::json!({"stage":"initial_four", "execution_identity":identity,
        "pids":processes.children.iter().map(Child::id).collect::<Vec<_>>()}),
    );
    let until = Instant::now() + Duration::from_secs(40);
    let mut ready = [false; 4];
    while !ready.iter().all(|ready| *ready) {
        for i in 0..4 {
            if !ready[i] {
                let reply = open_data(&root, i, &mut sequences[i], 1500);
                journal(
                    &mut ledger,
                    serde_json::json!({"stage":"initial_data_action","node":node(i),"reply":reply}),
                );
                ready[i] = reply["error"].is_null() && reply["host"]["data_serving"] == true;
            }
        }
        assert!(
            Instant::now() < until,
            "four DATA owners never became ready: {ready:?}"
        );
    }
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
    let initial = bootstrap.fetch_snapshot(&node(0), &addresses[0]).unwrap();
    config.transport = network.clone();
    let gateway = bicdb_core::ClusterEventClient::new(&config, initial.topology)
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let key = "ack-before-fifth-replica-receiver";
    let expected = serde_json::json!({"entire":"external ACK precedes fifth stage","nested":[17,null,{"retained":true}]});
    journal(
        &mut ledger,
        serde_json::json!({"stage":"attempt_before_fifth","namespace":"items","key":key,"value":expected}),
    );
    let ack = gateway.append("items", key, expected.clone());
    journal(
        &mut ledger,
        serde_json::json!({"stage":"reply_before_fifth","key":key,"value":expected,
        "acknowledged":ack.is_ok(),"error":ack.as_ref().err().map(ToString::to_string)}),
    );
    assert_eq!(ack.unwrap(), true);
    let value = gateway.get_strong("items", key).unwrap();
    journal(
        &mut ledger,
        serde_json::json!({"stage":"strong_before_fifth","key":key,"present":value.is_some(),"value":value}),
    );
    assert_eq!(value, Some(expected.clone()));
    let (command, status) = paths(&root, 4);
    let mut fifth = binary();
    fifth
        .args([
            "cluster",
            "native-metadata",
            "join-fresh",
            "--replica-receiver",
        ])
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
    let child = processes.spawn_recorded(&mut fifth).unwrap();
    let fifth_pid = child.id();
    processes.children.push(child);
    let until = Instant::now() + Duration::from_secs(45);
    let mut original_owner = None;
    let receiver = loop {
        for (i, child) in processes.children.iter_mut().enumerate() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "actual native process{i} exited; private diagnostic custody retained"
            );
        }
        if let Ok(bytes) = std::fs::read(&status) {
            let actual: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(actual["process_id"], fifth_pid);
            assert_eq!(actual["host"]["data_serving"], false);
            if let Some(id) = actual["host"]["metadata_store_id"].as_str() {
                let current = (
                    actual["host"]["birth_id"].clone(),
                    id.to_string(),
                    actual["host"]["namespace_ids"].clone(),
                );
                if let Some(saved) = &original_owner {
                    assert_eq!(
                        saved, &current,
                        "listener rotation must retain SAME birth/MStore/namespaces"
                    );
                } else {
                    original_owner = Some(current);
                }
            }
            if actual["error"].is_null()
                && actual["host"]["replica_receiver"]["receiver_ready"] == true
            {
                break actual;
            }
        }
        assert!(Instant::now()<until,"single fifth join never reached actual replica-only receiver; no image/move success implied");
        std::thread::sleep(Duration::from_millis(20));
    };
    journal(
        &mut ledger,
        serde_json::json!({"stage":"same_owned_fifth_receiver","status":receiver}),
    );
    assert_eq!(receiver["host"]["metadata_control_owned"], true);
    assert_eq!(receiver["host"]["replica_receiver"]["data_serving"], false);
    assert_eq!(
        receiver["host"]["replica_receiver"]["birth_id"],
        receiver["host"]["birth_id"]
    );
    assert_eq!(
        receiver["host"]["replica_receiver"]["metadata_store_id"],
        receiver["host"]["metadata_store_id"]
    );
    assert_eq!(receiver["sequence"], 0);
    assert!(
        !command.exists(),
        "single join command has no hidden step orchestration"
    );
    let current = bootstrap.fetch_snapshot(&node(0), &addresses[0]).unwrap();
    assert!(current
        .topology
        .ranges
        .values()
        .all(|r| r.replicas.iter().all(|p| p.node_id != node(4))));
    let transport = bicdb_core::TcpClusterRelocationTransport::new(
        config.cluster_id.clone(),
        node(0),
        std::sync::Arc::new(parking_lot::RwLock::new(current.topology.clone())),
        network,
    )
    .unwrap();
    let range = current.topology.ranges.values().next().unwrap();
    for operation in [
        bicdb_core::ClusterEventOperation::Append {
            namespace: "items".into(),
            key: "no-blank-app".into(),
            value: expected.clone(),
        },
        bicdb_core::ClusterEventOperation::Get {
            namespace: "items".into(),
            key: "not-a-blank-absence-proof".into(),
        },
        bicdb_core::ClusterEventOperation::Scan {
            namespace: "items".into(),
            after: None,
            limit: 1,
        },
    ] {
        // Actual public TLS requests using observed scope DATA, not a fake
        // assignment or a test-installed metadata/serving permission.
        let result = transport.application_event(
            &node(4),
            bicdb_core::ClusterEventRequest {
                range_id: range.id,
                range_epoch: range.epoch,
                operation,
            },
        );
        assert!(
            result.is_err(),
            "replica-only receiver cannot ACK APP or return blank absence"
        );
    }
    // SAME stale gateway: after membership changes it may require separately
    // fresh writer enrollment. Neither a transient refusal nor receiver-ready
    // becomes successful absence for the previously ACKed key.
    match gateway.get_strong("items", key) {
        Ok(value) => {
            assert_eq!(value, Some(expected));
            journal(
                &mut ledger,
                serde_json::json!({"stage":"strong_after_fifth","present":value.is_some(),"value":value}),
            );
        }
        Err(error) => journal(
            &mut ledger,
            serde_json::json!({"stage":"strong_after_fifth_retryable_unavailable","detail":error.to_string(),"serving_readiness":"not_claimed"}),
        ),
    }
    journal(
        &mut ledger,
        serde_json::json!({"stage":"replica_receiver_prerequisite_complete",
        "fifth_data_serving":false,"next_missing_gates":["native_placement_controller","authenticated_native_image_joint_wire","actual_image_joint_open_and_writer_admission"],
        "relocation":"not_attempted_or_claimed"}),
    );
}
