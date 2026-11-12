//! FOUR real initial processes, public command action and public event client.
//! The fifth data stage is deliberately not entered. Metadata registration,
//! automatic relocation, SQL and host/VM qualification are separate gates.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

fn actual_cli_identity_before_phase() -> Value {
    let runtime_source = std::env::var("BICDB_TEST_SOURCE_SHA").ok();
    if let Some(source) = &runtime_source {
        assert!(
            source.len() == 40 && source.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "runtime source evidence must be the exact workflow-provided source SHA"
        );
    }
    // This is the SAME actual executable path used by binary(). Stream the
    // whole regular file with bounded memory before spawning the four hosts;
    // no release identity or unprovided source revision is inferred from it.
    let mut file = std::fs::File::open(env!("CARGO_BIN_EXE_bicdb")).unwrap();
    let metadata = file.metadata().unwrap();
    assert!(metadata.is_file());
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes = bytes.checked_add(count as u64).unwrap();
    }
    assert_eq!(
        bytes,
        metadata.len(),
        "CLI executable changed while recording identity"
    );
    serde_json::json!({
        "source":runtime_source.as_deref().unwrap_or("UNAVAILABLE_RUNTIME_SOURCE"),
        "source_available":runtime_source.is_some(),
        "source_authority":"BICDB_TEST_SOURCE_SHA runtime environment; not option_env or runtime git lookup",
        "test_package_version":env!("CARGO_PKG_VERSION"),
        "binary_sha256":hex::encode(digest.finalize()),"binary_bytes":bytes,
        "binary_scope":"actual CARGO_BIN_EXE_bicdb bytes read before process/startup/ACK phase; not a published release"
    })
}

fn payload_sha256(value: &Value) -> String {
    hex::encode(Sha256::digest(serde_json::to_vec(value).unwrap()))
}

fn journal(file: &mut std::fs::File, value: Value) {
    serde_json::to_writer(&mut *file, &value).unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
    // CI retains full stdout; emit only the already-fsynced external record,
    // never TLS keys or a recursive database dump.
    println!(
        "NATIVE_INITIAL_DATA_EXTERNAL_LEDGER {}",
        serde_json::to_string(&value).unwrap()
    );
}

fn open_data(root: &Path, i: usize, sequence: &mut u64, budget_ms: u64) -> Value {
    *sequence += 1;
    let (command, status) = paths(root, i);
    let output = binary()
        .args(["cluster", "native-metadata", "step", "--command-file"])
        .arg(command)
        .arg("--status-file")
        .arg(&status)
        .arg("--sequence")
        .arg(sequence.to_string())
        .arg("--action")
        .arg("open-initial-data")
        .arg("--timeout-ms")
        .arg(budget_ms.to_string())
        .output()
        .unwrap();
    // A pending finite action may be refused while other live owners still
    // expose metadata-only listeners. Read only its actual command journal;
    // do not replace the failed action with a test-created proof or owner.
    let deadline = Instant::now() + Duration::from_secs(2);
    let reply = loop {
        let value: Value = serde_json::from_slice(&std::fs::read(&status).unwrap()).unwrap();
        if value["sequence"].as_u64() == Some(*sequence) {
            break value;
        }
        assert!(
            Instant::now() < deadline,
            "missing actual DATA action reply: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    if output.status.success() {
        assert!(reply["error"].is_null(), "{reply}");
    } else {
        assert!(
            reply["error"].is_string(),
            "refusal must retain actual diagnostic: {reply}"
        );
    }
    reply
}

#[test]
fn native_initial_data_actual_cli_four_process_first_ack_and_strong_full_value() {
    let identity = actual_cli_identity_before_phase();
    let (mut processes, root, mut config, addresses, mut sequences) = four_processes(false);
    assert_eq!(processes.children.len(), 4);
    let ids = processes
        .children
        .iter()
        .map(Child::id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 4);
    assert!(!ids.contains(&std::process::id()));
    let mut ledger = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("initial-data-external-ledger.jsonl"))
        .unwrap();
    journal(
        &mut ledger,
        serde_json::json!({
            "stage": "four_initial_owners", "source": identity["source"], "execution_identity":identity,
            "process_ids": processes.children.iter().map(Child::id).collect::<Vec<_>>(),
            "data_stage": "four_initial_only", "fifth_data_stage": "not_entered"
        }),
    );
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
    // Routing/bootstrap is actual authenticated DATA. The event operations
    // themselves must acquire the existing real native writer/Strong proofs.
    let snapshot = bootstrap.fetch_snapshot(&node(0), &addresses[0]).unwrap();
    config.transport = network;
    let gateway = bicdb_core::ClusterEventClient::new(&config, snapshot.topology)
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let pending = open_data(&root, 0, &mut sequences[0], 2000);
    journal(
        &mut ledger,
        serde_json::json!({"stage":"first_receiver_pending", "reply":pending}),
    );
    assert_eq!(pending["host"]["initial_data"]["receiver_listening"], true);
    assert_eq!(
        pending["host"]["initial_data"]["writer_policy_current"],
        true
    );
    assert_eq!(
        pending["host"]["initial_data"]["dispatcher_published"],
        false
    );
    assert_eq!(pending["host"]["data_serving"], false);
    assert!(pending["host"]["initial_data"]["ranges"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["local_leader"] == true));
    // Before the other three DATA receivers, no local+peer native QQ exists.
    // This is a no-user-ACK control; an error is not a global abort certificate.
    let blocked_key = "before-initial-data-quorum";
    let blocked_value = serde_json::json!({"phase":"must_not_ack_before_quorum"});
    journal(
        &mut ledger,
        serde_json::json!({"stage":"attempt", "namespace":"items", "key":blocked_key, "value":blocked_value, "payload_sha256":payload_sha256(&blocked_value)}),
    );
    let blocked = gateway.append("items", blocked_key, blocked_value);
    journal(
        &mut ledger,
        serde_json::json!({"stage":"reply", "key":blocked_key, "acknowledged":blocked.is_ok(), "error":blocked.as_ref().err().map(ToString::to_string)}),
    );
    assert!(
        blocked.is_err(),
        "partial initial startup must not acknowledge an APP"
    );
    // A typed Strong absence must not escape the constructed-but-unpublished
    // dispatcher during that same partial startup.
    assert!(gateway
        .get_strong("items", "not-written-before-open")
        .is_err());

    let mut ready = [false; 4];
    let until = Instant::now() + Duration::from_secs(40);
    while !ready.iter().all(|r| *r) {
        for i in 0..4 {
            assert!(
                processes.children[i].try_wait().unwrap().is_none(),
                "initial owner{i} ended"
            );
            if ready[i] {
                continue;
            }
            let reply = open_data(&root, i, &mut sequences[i], 1500);
            journal(
                &mut ledger,
                serde_json::json!({"stage":"data_open_action", "node":node(i), "reply":reply}),
            );
            ready[i] = reply["error"].is_null() && reply["host"]["data_serving"] == true;
            if ready[i] {
                assert_eq!(reply["host"]["initial_data"]["receiver_listening"], true);
                assert_eq!(reply["host"]["initial_data"]["writer_policy_current"], true);
                assert_eq!(reply["host"]["initial_data"]["dispatcher_published"], true);
                for range in reply["host"]["initial_data"]["ranges"].as_array().unwrap() {
                    if range["local_leader"] == true {
                        assert_eq!(range["leader_admission_observed"], true);
                    }
                }
            }
        }
        assert!(
            Instant::now() < until,
            "actual initial DATA actions did not reach ready: {ready:?}"
        );
    }
    // No range selection, shard-specific keys, private coordinator or root
    // reopening. SAME externally created public client for every operation.
    for (key, expected) in [
        (
            "external-first-ack",
            serde_json::json!({"full":{"array":[1,true,null,{"nested":"retained"}]},"count":17,"text":"entire acknowledged payload"}),
        ),
        ("external-json-null", Value::Null),
        ("external-empty-object", serde_json::json!({})),
    ] {
        journal(
            &mut ledger,
            serde_json::json!({"stage":"attempt", "namespace":"items", "key":key, "value":expected, "payload_sha256":payload_sha256(&expected)}),
        );
        let ack = gateway.append("items", key, expected.clone());
        journal(
            &mut ledger,
            serde_json::json!({"stage":"reply", "key":key, "value":expected, "payload_sha256":payload_sha256(&expected), "acknowledged":ack.is_ok(), "inserted":ack.as_ref().ok(), "error":ack.as_ref().err().map(ToString::to_string)}),
        );
        assert_eq!(
            ack.unwrap(),
            true,
            "a fresh key must produce a real new ACK"
        );
        let actual = gateway.get_strong("items", key).unwrap();
        journal(
            &mut ledger,
            serde_json::json!({"stage":"strong_reply", "key":key, "present":actual.is_some(), "value":actual}),
        );
        assert_eq!(
            actual,
            Some(expected),
            "Strong must return the ENTIRE externally ACKed value"
        );
    }
    let absent = gateway
        .get_strong("items", "genuine-unwritten-key")
        .unwrap();
    journal(
        &mut ledger,
        serde_json::json!({"stage":"strong_reply", "key":"genuine-unwritten-key", "present":absent.is_some(), "value":absent}),
    );
    assert_eq!(
        absent, None,
        "actual proved missing key remains a successful absence"
    );
    // Later finite retries do not re-create genesis or reset a nonempty range.
    for i in 0..4 {
        let reply = open_data(&root, i, &mut sequences[i], 1500);
        assert!(
            reply["error"].is_null(),
            "same retained owner retry must not reinstall/reset: {reply}"
        );
        assert_eq!(reply["host"]["data_serving"], true);
    }
    assert_eq!(
        gateway.get_strong("items", "external-first-ack").unwrap(),
        Some(
            serde_json::json!({"full":{"array":[1,true,null,{"nested":"retained"}]},"count":17,"text":"entire acknowledged payload"})
        )
    );
    journal(
        &mut ledger,
        serde_json::json!({"stage":"four_initial_data_complete", "externally_acknowledged":3, "fifth_data_stage":"not_entered", "automatic_range_move":"not_implemented"}),
    );
}
