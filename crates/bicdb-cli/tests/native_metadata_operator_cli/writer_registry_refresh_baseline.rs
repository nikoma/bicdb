//! Strict separate baseline: metadata-only registry addition must not leave
//! unchanged DATA assignments permanently unavailable. Original receiver test
//! remains unchanged; this case requires both an old ACK read and a new ACK.
//! Expected RED on ca7. No writer-policy refresh is implemented by this test.
use super::initial_data_tests::{actual_cli_identity_before_phase, journal, open_data};
use super::*;

#[test]
fn native_metadata_registry_addition_actual_cli_requires_healthy_existing_read_and_fresh_ack() {
    let identity = actual_cli_identity_before_phase();
    let (mut processes, root, mut config, addresses, mut sequences) = four_processes(false);
    let rpc = super::rpc_evidence::DurableRpcJournal::open(&root).unwrap();
    let mut ledger = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("registry-refresh-external-ledger.jsonl"))
        .unwrap();
    // Persist the primary ledger inode and pathname before the first intent.
    ledger.sync_all().unwrap();
    std::fs::File::open(&root).unwrap().sync_all().unwrap();
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
    let setup = rpc.begin(serde_json::json!({"public_operation":"authenticated_bootstrap_setup","destination":"n0","address":addresses[0],"bootstrap_raw_frame_covered":false})).unwrap();
    let initial = bootstrap.fetch_snapshot(&node(0), &addresses[0]);
    setup.bootstrap(&initial).unwrap();
    let initial = initial.unwrap();
    config.transport = network.clone();
    let before_topology = serde_json::to_value(&initial.topology).unwrap();
    let before_bindings = bindings(&root, &mut sequences, &mut ledger, "before_fifth");
    let gateway = bicdb_core::ClusterEventClient::new(&config, initial.topology)
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap()
        .with_rpc_evidence_observer(
            rpc.observer("same_public_gateway"),
            bicdb_core::ClusterRpcEvidenceLimits::default(),
        )
        .unwrap();
    let key = "ack-before-fifth-registry-refresh";
    let expected = serde_json::json!({"entire":"external ACK precedes fifth stage","nested":[17,null,{"retained":true}]});
    let append_call = rpc.begin(serde_json::json!({"public_operation":"append","namespace":"items","key":key,"value":expected,"payload_sha256":super::rpc_evidence::payload_sha256(&expected),"phase":"before_fifth","request_timeout_ms":2000})).unwrap();
    journal(
        &mut ledger,
        serde_json::json!({"logical_operation_id":append_call.id,"stage":"attempt_before_fifth","namespace":"items","key":key,"value":expected}),
    );
    let ack = gateway.append("items", key, expected.clone());
    journal(
        &mut ledger,
        serde_json::json!({"logical_operation_id":append_call.id,"stage":"reply_before_fifth","key":key,"value":expected,
        "acknowledged":ack.is_ok(),"error":ack.as_ref().err().map(ToString::to_string)}),
    );
    append_call.append(&ack).unwrap();
    assert_eq!(ack.unwrap(), true);
    let read_call = rpc.begin(serde_json::json!({"public_operation":"get_strong","namespace":"items","key":key,"phase":"before_fifth","request_timeout_ms":2000})).unwrap();
    let value = gateway.get_strong("items", key);
    let read_id = read_call.id;
    read_call.strong(&value).unwrap();
    let value = value.unwrap();
    journal(
        &mut ledger,
        serde_json::json!({"logical_operation_id":read_id,"stage":"strong_before_fifth","key":key,"present":value.is_some(),"value":value}),
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
    let child = processes
        .spawn_recorded(&mut fifth, &root.join("db4"))
        .unwrap();
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
    let bootstrap_call = rpc.begin(serde_json::json!({"public_operation":"authenticated_bootstrap_after_fifth","destination":"n4","address":addresses[4],"bootstrap_raw_frame_covered":false})).unwrap();
    let current = bootstrap.fetch_snapshot(&node(4), &addresses[4]);
    bootstrap_call.bootstrap(&current).unwrap();
    let current = current.unwrap();
    // Bootstrap is observed DATA, never a policy publisher. The genuine fifth
    // owner reached its current owned voter/receiver phase before this read.
    assert!(current.topology.metadata_voters().contains(&node(4)));
    let after_topology = serde_json::to_value(&current.topology).unwrap();
    journal(
        &mut ledger,
        serde_json::json!({"stage":"actual_before_after_configuration",
        "before":before_topology,"after":after_topology}),
    );
    assert_eq!(
        data_assignments(&before_topology),
        data_assignments(&after_topology),
        "metadata addition must preserve every data range/epoch/leader/replica/placement"
    );
    for i in 0..4 {
        for field in [
            "incarnation",
            "native_store_birth_v1",
            "tls_certificate_sha256",
            "pending_tls_certificate_sha256",
            "address",
            "lifecycle",
            "labels",
        ] {
            assert_eq!(
                before_topology["nodes"][format!("n{i}")][field],
                after_topology["nodes"][format!("n{i}")][field],
                "existing identity/domain changed at node{i}/{field}"
            );
        }
    }
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
    .unwrap()
    .with_rpc_evidence_observer(
        rpc.observer("direct_unassigned_receiver_probe"),
        bicdb_core::ClusterRpcEvidenceLimits::default(),
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
        let receiver_call = rpc.begin(serde_json::json!({"public_operation":"replica_receiver_refusal_probe","destination":"n4","range_id":range.id,"range_epoch":range.epoch,"operation":operation})).unwrap();
        let result = transport.application_event(
            &node(4),
            bicdb_core::ClusterEventRequest {
                range_id: range.id,
                range_epoch: range.epoch,
                operation,
            },
        );
        receiver_call.receiver(&result).unwrap();
        assert!(
            result.is_err(),
            "replica-only receiver cannot ACK APP or return blank absence"
        );
    }
    // One explicit eventual-health gate, separate from each original public
    // request's unchanged 2s budget. No fresh schema/genesis/coordinator is
    // installed by the test, and no private command performs enrollment.
    const REQUEST_MS: u64 = 2000;
    let health_deadline = Instant::now() + Duration::from_secs(10);
    let fresh_key = "fresh-ack-after-fifth-registry-addition";
    let fresh_value = serde_json::json!({"whole":"new external ACK after registry addition",
        "nested":[null,{"unchanged_data_assignment":true}],"empty":{}});
    let mut old_ack_read = false;
    let mut fresh_ack = false;
    let mut fresh_ack_read = false;
    let mut observed_false_absence = false;
    let mut observed_wrong_value = false;
    let mut round = 0;
    // Leave enough original budget for BOTH required operations each round.
    while health_deadline.saturating_duration_since(Instant::now())
        >= Duration::from_millis(REQUEST_MS * 2)
    {
        round += 1;
        let old_call = rpc
            .begin(serde_json::json!({"public_operation":"get_strong",
            "namespace":"items","key":key,"phase":"strict_after_fifth","round":round,
            "request_timeout_ms":REQUEST_MS,"eventual_health_gate_ms":10000}))
            .unwrap();
        let old = gateway.get_strong("items", key);
        journal(
            &mut ledger,
            serde_json::json!({"logical_operation_id":old_call.id,
            "stage":"strict_old_ack_read","round":round,"expected":expected,
            "success":old.is_ok(),"present":old.as_ref().ok().map(Option::is_some),
            "value":old.as_ref().ok(),"error":old.as_ref().err().map(ToString::to_string)}),
        );
        old_ack_read |= old
            .as_ref()
            .is_ok_and(|value| value == &Some(expected.clone()));
        observed_false_absence |= matches!(&old, Ok(None));
        observed_wrong_value |= old
            .as_ref()
            .is_ok_and(|value| value != &Some(expected.clone()));
        old_call.strong(&old).unwrap();

        // Attempt this even when the old read failed. A missing reply is
        // explicitly NO_USER_ACK/UNKNOWN, never interpreted as rollback.
        let append_call = rpc
            .begin(serde_json::json!({"public_operation":"append",
            "namespace":"items","key":fresh_key,"value":fresh_value,
            "payload_sha256":super::rpc_evidence::payload_sha256(&fresh_value),
            "phase":"strict_after_fifth","round":round,"request_timeout_ms":REQUEST_MS}))
            .unwrap();
        journal(
            &mut ledger,
            serde_json::json!({"logical_operation_id":append_call.id,
            "stage":"strict_fresh_append_intent","round":round,"key":fresh_key,"value":fresh_value}),
        );
        let ack = gateway.append("items", fresh_key, fresh_value.clone());
        journal(
            &mut ledger,
            serde_json::json!({"logical_operation_id":append_call.id,
            "stage":"strict_fresh_append_reply","round":round,"key":fresh_key,"value":fresh_value,
            "acknowledged":ack.is_ok(),"inserted":ack.as_ref().ok(),
            "classification":if ack.is_ok(){"EXTERNAL_ACK"}else{"NO_USER_ACK_OUTCOME_UNKNOWN"},
            "error":ack.as_ref().err().map(ToString::to_string)}),
        );
        fresh_ack |= ack.is_ok();
        append_call.append(&ack).unwrap();
        if fresh_ack
            && health_deadline.saturating_duration_since(Instant::now())
                >= Duration::from_millis(REQUEST_MS)
        {
            let read_call = rpc
                .begin(serde_json::json!({"public_operation":"get_strong",
                "namespace":"items","key":fresh_key,"phase":"strict_fresh_ack_read",
                "round":round,"request_timeout_ms":REQUEST_MS}))
                .unwrap();
            let read = gateway.get_strong("items", fresh_key);
            journal(
                &mut ledger,
                serde_json::json!({"logical_operation_id":read_call.id,
                "stage":"strict_fresh_ack_read_reply","round":round,"expected":fresh_value,
                "success":read.is_ok(),"value":read.as_ref().ok(),
                "error":read.as_ref().err().map(ToString::to_string)}),
            );
            fresh_ack_read |= read
                .as_ref()
                .is_ok_and(|value| value == &Some(fresh_value.clone()));
            observed_false_absence |= matches!(&read, Ok(None));
            observed_wrong_value |= read
                .as_ref()
                .is_ok_and(|value| value != &Some(fresh_value.clone()));
            read_call.strong(&read).unwrap();
        }
        if old_ack_read && fresh_ack && fresh_ack_read {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let after_bindings = bindings(&root, &mut sequences, &mut ledger, "after_health_gate");
    // On this baseline there is no authorized writer-transition producer.
    // Diagnose any rotation rather than treating new DTO identities as proof.
    for i in 0..4 {
        for field in [
            "retained_owner_coordinator_id",
            "persisted_policy_coordinator_id",
            "persisted_policy_registration_generation",
            "persisted_policy_database_open_incarnation",
            "persisted_policy_range_store_open_id",
            "persisted_policy_checksum_sha256",
        ] {
            assert_eq!(before_bindings[i][field], after_bindings[i][field],
                "node{i}/{field}: changed binding requires an actual authorized transition, not status DATA");
        }
    }
    journal(
        &mut ledger,
        serde_json::json!({"stage":"strict_registry_addition_terminal",
        "rounds":round,"old_ack_read":old_ack_read,"fresh_ack":fresh_ack,
        "fresh_ack_read":fresh_ack_read,"observed_false_absence":observed_false_absence,"observed_wrong_value":observed_wrong_value,
        "fifth_data_serving":false,"data_move":"not_attempted",
        "scope":"unchanged assignments require actual bounded healthy existing read AND fresh ACK"}),
    );
    rpc.finish_case("strict_registry_health_probes_complete_no_success_implied")
        .unwrap();
    assert!(
        !observed_false_absence,
        "a previous external ACK must never become absent"
    );
    assert!(
        !observed_wrong_value,
        "Strong returned a value different from the full externally ACKed value"
    );
    assert!(
        round > 0,
        "the health gate must actually dispatch read AND write probes"
    );
    assert!(old_ack_read && fresh_ack && fresh_ack_read,
        "metadata-only registry addition left unchanged DATA unavailable: old_ack_read={old_ack_read}, fresh_ack={fresh_ack}, fresh_ack_read={fresh_ack_read}; all attempted outcomes retained");
}

fn data_assignments(topology: &Value) -> Value {
    let mut ranges = topology["ranges"].clone();
    for range in ranges.as_object_mut().unwrap().values_mut() {
        // Operational load estimates are not assignment authority. Preserve
        // ALL other fields, including epoch, leader, replicas and placement.
        range.as_object_mut().unwrap().remove("approximate_bytes");
        range.as_object_mut().unwrap().remove("approximate_qps");
    }
    ranges
}

fn bindings(
    root: &Path,
    sequences: &mut [u64; 4],
    ledger: &mut std::fs::File,
    phase: &str,
) -> Vec<Value> {
    (0..4)
        .map(|i| {
            sequences[i] += 1;
            let (command, status) = paths(root, i);
            let output = binary()
                .args(["cluster", "native-metadata", "step", "--command-file"])
                .arg(command)
                .arg("--status-file")
                .arg(&status)
                .arg("--sequence")
                .arg(sequences[i].to_string())
                .arg("--action")
                .arg("status")
                .arg("--timeout-ms")
                .arg("1500")
                .output()
                .unwrap();
            let actual: Value = serde_json::from_slice(&std::fs::read(&status).unwrap()).unwrap();
            journal(
                ledger,
                serde_json::json!({"stage":"readonly_retained_writer_binding",
            "phase":phase,"node":node(i),"command_success":output.status.success(),"reply":actual,
            "stderr":String::from_utf8_lossy(&output.stderr)}),
            );
            assert_eq!(actual["sequence"].as_u64(), Some(sequences[i]));
            assert!(
                output.status.success() && actual["error"].is_null(),
                "readonly actual owner diagnostic failed, not a healthy DATA verdict: {actual}"
            );
            let binding = actual["host"]["initial_data"]["writer_binding_diagnostic"].clone();
            for field in [
                "persisted_policy_coordinator_id",
                "persisted_policy_registration_generation",
                "persisted_policy_database_open_incarnation",
                "persisted_policy_range_store_open_id",
                "persisted_policy_checksum_sha256",
                "retained_owner_coordinator_id",
            ] {
                assert!(
                    binding[field].is_string(),
                    "actual binding identity missing: {field}: {actual}"
                );
            }
            binding
        })
        .collect()
}
