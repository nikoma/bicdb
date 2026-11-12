//! Separate post-d77 actual lifecycle case. Old completion/activation decisions
//! stay at R1 after a genuine later ACK; this is not a StrongGet proof.
use super::activation_tests::{completion, data_zero_scope};
use super::*;
use crate::distribution_range_recovery::NativeCompletionStage;

#[test]
fn activated_anchor_keeps_r1_after_new_ack_and_late_prepared_follower_activates() {
    let mut fixture = NetworkFixture::new_ready_network_with_roles(true, true, false, "n0", "n1");
    fixture.native.root.disable_cleanup(true);
    eprintln!(
        "ACTIVATED_RPLUSONE_RETAINED_ROOT {}",
        fixture.native.root.path().display()
    );
    let scope = data_zero_scope(&fixture);
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let parent = CancellationToken::uncancelable();
    for apply in [false, true] {
        for destination in [node("n0"), node("n1"), node("n2")] {
            fixture.clients[&node("n0")]
                .recovery_historical_command_observation(
                    &destination,
                    &fixture.request(&scope),
                    apply,
                    &parent,
                )
                .unwrap();
            fixture.wait_idle();
        }
    }
    for destination in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .prepare_native_ready_observation(&destination, &fixture.ready_request(&scope), &parent)
            .unwrap();
        fixture.wait_idle();
    }
    for destination in [node("n0"), node("n2")] {
        fixture.clients[&node("n0")]
            .complete_native_recovery_observation(&destination, &completion(&scope), &parent)
            .unwrap();
        fixture.wait_idle();
    }
    let mut anchors = BTreeMap::new();
    for destination in [node("n0"), node("n2")] {
        let actual = fixture.runtimes[&destination]
            .activate_native_recovery(&actor, &completion(&scope), &parent)
            .unwrap();
        assert_eq!(actual.reply().stage, NativeCompletionStage::ActivatedAnchor);
        assert_eq!(actual.reply().range_index, 1);
        assert!(actual.reply().activation_decision_sha256.is_some());
        anchors.insert(destination, actual.reply().clone());
        fixture.wait_idle();
    }
    // Keep n1's actual Prepared pin. Only DATA deliveries for the new origin
    // call are unavailable; its metadata/TLS endpoint remains alive.
    fixture.native.transport.set_unavailable(&node("n1"), true);
    *fixture.native.topology.write() = fixture.current.read().clone();
    let recovered = fixture.native.coordinator.recover_local_leadership(8);
    let recover_error = recovered.as_ref().err().map(ToString::to_string);
    let recovery_report = serde_json::json!({"phase":"ACTUAL_COORDINATOR_RECOVERY_BEFORE_NEW_ACK","result":recovered.as_ref().ok(),"error":recover_error,"cached_epoch_seeded":false});
    std::fs::write(
        fixture
            .native
            .root
            .path()
            .join("rplusone-coordinator-recovery.json"),
        serde_json::to_vec_pretty(&recovery_report).unwrap(),
    )
    .unwrap();
    eprintln!("ACTIVATED_RPLUSONE_RECOVERY {recovery_report}");
    recovered.expect("actual current-epoch coordinator recovery must precede the new origin ACK");
    // Historical fixtures intentionally unhooked the original admission after
    // their seed ACK. Recovering the real coordinator does not install it.
    // Restore that ordinary host wiring only AFTER its actual recovery; this
    // readiness boolean means installed hook, never a physical/quorum proof.
    assert!(!fixture.native.databases[&node("n0")]
        .read()
        .commit_admission_ready());
    fixture.native.databases[&node("n0")]
        .read()
        .install_commit_admission(fixture.native.coordinator.clone());
    assert!(fixture.native.databases[&node("n0")]
        .read()
        .commit_admission_ready());
    let next = fixture.native.append("actual-rplusone-after-activation");
    let source_native = fixture.native.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(scope.range_id)
        .unwrap();
    let late_native = fixture.native.databases[&node("n1")]
        .read()
        .native_range_prefix_observation(scope.range_id)
        .unwrap();
    let late_raw = fixture.native.services[&node("n1")]
        .lock()
        .range_write_progress(scope.range_id);
    let origin_report = serde_json::json!({"phase":"NEW_ORIGIN_AFTER_ACTIVATION","acknowledged":next.is_ok(),"error":next.as_ref().err().map(ToString::to_string),"source_native":source_native,"late_node_native":late_native,"late_node_raw":late_raw,"physical_qualification":false,"strong_get_tested":false});
    std::fs::write(
        fixture
            .native
            .root
            .path()
            .join("rplusone-origin-result.json"),
        serde_json::to_vec_pretty(&origin_report).unwrap(),
    )
    .unwrap();
    eprintln!("ACTIVATED_RPLUSONE_ORIGIN {origin_report}");
    next.expect("a newly activated actual source must acknowledge a new origin transaction with actual0+2 quorum");
    let row2 = fixture.native.databases[&node("n0")]
        .read()
        .get("items", "actual-rplusone-after-activation")
        .unwrap()
        .unwrap();
    use std::io::Write;
    let mut ledger = std::fs::File::create(
        fixture
            .native
            .root
            .path()
            .join("controller-rplusone-ack.jsonl"),
    )
    .unwrap();
    writeln!(
        ledger,
        "{}",
        serde_json::json!({"key":"actual-rplusone-after-activation","record":row2,"range_index":2})
    )
    .unwrap();
    ledger.sync_all().unwrap();
    drop(ledger);
    assert_eq!(
        fixture.native.databases[&node("n0")]
            .read()
            .native_range_prefix_observation(scope.range_id)
            .unwrap()
            .native_applied_index,
        Some(2)
    );
    assert_eq!(
        fixture.native.databases[&node("n1")]
            .read()
            .native_range_prefix_observation(scope.range_id)
            .unwrap()
            .native_applied_index,
        Some(1)
    );
    assert_eq!(
        fixture.native.services[&node("n1")]
            .lock()
            .range_write_progress(scope.range_id)
            .last_index,
        1
    );
    assert!(fixture.native.databases[&node("n1")]
        .read()
        .get("items", "actual-rplusone-after-activation")
        .unwrap()
        .is_none());
    let after_ack = fixture.clients[&node("n0")]
        .observe_native_completion_observation(&node("n0"), &completion(&scope), &parent)
        .unwrap();
    assert_eq!(
        after_ack.reply().stage,
        NativeCompletionStage::ActivatedAnchor
    );
    assert_eq!(
        after_ack.reply().range_index,
        1,
        "immutable old decision is never current native head"
    );
    assert_eq!(
        after_ack.reply().activation_decision_sha256,
        anchors[&node("n0")].activation_decision_sha256
    );
    assert_eq!(
        after_ack.reply().logical_history_sha256,
        anchors[&node("n0")].logical_history_sha256
    );
    fixture.wait_idle();
    fixture.native.transport.set_unavailable(&node("n1"), false);
    assert_eq!(fixture.servers.len(), 3);
    fixture.servers.pop().unwrap().join().unwrap();
    fixture.native.transport.set_unavailable(&node("n2"), true);
    let complete = fixture.clients[&node("n0")]
        .complete_native_recovery_observation(&node("n1"), &completion(&scope), &parent)
        .unwrap();
    assert_eq!(
        complete.reply().stage,
        NativeCompletionStage::CompletedFrozen
    );
    assert_eq!(complete.reply().range_index, 1);
    fixture.wait_idle();
    let activate = fixture.runtimes[&node("n1")]
        .activate_native_recovery(&actor, &completion(&scope), &parent)
        .unwrap();
    assert_eq!(
        activate.reply().stage,
        NativeCompletionStage::ActivatedAnchor
    );
    assert_eq!(activate.reply().range_index, 1);
    fixture.wait_idle();
    assert!(fixture.clients[&node("n0")]
        .observe_native_ready_observation(&node("n1"), &fixture.ready_request(&scope), &parent)
        .is_err());
    fixture.wait_idle();
    let permit = fixture.native.databases[&node("n1")]
        .read()
        .range_mutation_gate()
        .try_enter(scope.range_id)
        .unwrap();
    drop(permit);
    assert_eq!(
        fixture.native.databases[&node("n1")]
            .read()
            .native_range_prefix_observation(scope.range_id)
            .unwrap()
            .native_applied_index,
        Some(1)
    );
    assert!(fixture.native.databases[&node("n1")]
        .read()
        .get("items", "actual-rplusone-after-activation")
        .unwrap()
        .is_none());
    let report = serde_json::json!({"classification":"NEW_ACK_R2_THEN_LATE_ACTIVATION_R1","source_actual_native":2,"source_activated_anchor":after_ack.reply(),"late_activated_anchor":activate.reply(),"late_actual_native":1,"late_row2_present":false,"actual_new_application_ack":true,"old_r1_decision_not_ready_or_current_head":true,"current_native_catchup_of_late_follower_proven":false,"physical_qualification":false,"strong_get_tested":false,"production_qualified":false});
    std::fs::write(
        fixture
            .native
            .root
            .path()
            .join("rplusone-late-activation-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("ACTIVATED_RPLUSONE_LATE_RESULT {report}");
}
