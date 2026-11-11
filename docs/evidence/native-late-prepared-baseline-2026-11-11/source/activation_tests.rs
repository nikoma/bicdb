//! Actual loopback TLS liveness characterization. Activation itself uses the
//! trusted local Runtime API, not an incoming Activate RPC or physical fault.
use super::*;
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeCompletionStage};

fn data_zero_scope(fixture: &NetworkFixture) -> RecoveryFenceScope {
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let range = fixture
        .current
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(range.leader, node("n0"));
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let nonce = Uuid::now_v7();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let witness = fixture.providers[&node("n0")]
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    let role = RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        &node("n0"),
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap();
    let mut round = RecoveryFenceQuorumRound::begin(role, &parent).unwrap();
    let scope = round.scope().clone();
    for destination in [node("n0"), node("n1"), node("n2")] {
        let actual = fixture.clients[&node("n0")]
            .recovery_epoch_fence_observation(
                &destination,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 4000,
                },
                &parent,
            )
            .unwrap();
        round.record(Arc::new(actual)).unwrap();
        fixture.wait_idle();
    }
    round.finish().unwrap();
    scope
}
fn completion(scope: &RecoveryFenceScope) -> NativeCompletionRequest {
    NativeCompletionRequest {
        scope: scope.clone(),
        challenge: Uuid::now_v7(),
        budget_ms: 3500,
    }
}

#[test]
fn actual_late_prepared_follower_after_two_activations_and_one_endpoint_loss_completes() {
    let mut fixture = NetworkFixture::new_ready_network_with_roles(true, true, false, "n0", "n1");
    fixture.native.root.disable_cleanup(true);
    eprintln!(
        "LATE_PREPARED_RETAINED_ROOT {}",
        fixture.native.root.path().display()
    );
    let parent = CancellationToken::uncancelable();
    let scope = data_zero_scope(&fixture);
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let original = fixture.native.databases[&node("n0")]
        .read()
        .get("items", "actual-tls-historical")
        .unwrap()
        .unwrap();
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
        let actual = fixture.clients[&node("n0")]
            .complete_native_recovery_observation(&destination, &completion(&scope), &parent)
            .unwrap();
        assert_eq!(actual.reply().stage, NativeCompletionStage::CompletedFrozen);
        fixture.wait_idle();
    }
    let mut activated = BTreeMap::new();
    for destination in [node("n0"), node("n2")] {
        let actual = fixture.runtimes[&destination]
            .activate_native_recovery(&actor, &completion(&scope), &parent)
            .unwrap();
        assert_eq!(actual.reply().stage, NativeCompletionStage::ActivatedAnchor);
        assert_eq!(actual.reply().range_index, 1);
        activated.insert(destination, actual.reply().clone());
        fixture.wait_idle();
    }
    // Recipient n1 is physically Prepared at the actual same R/H. Neither the
    // caller nor a synthesized reply is allowed to call this Completed.
    let prepared = fixture.clients[&node("n0")]
        .observe_native_ready_observation(&node("n1"), &fixture.ready_request(&scope), &parent)
        .unwrap();
    assert_eq!(prepared.reply().range_index, 1);
    assert!(fixture.native.databases[&node("n1")]
        .read()
        .range_mutation_gate()
        .try_enter(scope.range_id)
        .is_err());
    assert!(
        fixture.clients[&node("n0")]
            .observe_native_ready_observation(&node("n0"), &fixture.ready_request(&scope), &parent,)
            .is_err(),
        "Activated is never a Ready response"
    );
    fixture.wait_idle();
    // The last server is exactly task-owned n2 (all-node order n0,n1,n2).
    // n1 remains the real metadata leader, with live MQ n0+n1 available.
    assert_eq!(fixture.servers.len(), 3);
    fixture.servers.pop().unwrap().join().unwrap();
    let result = fixture.clients[&node("n0")].complete_native_recovery_observation(
        &node("n1"),
        &completion(&scope),
        &parent,
    );
    fixture.wait_idle();
    for destination in [node("n0"), node("n1")] {
        assert_eq!(
            fixture.native.databases[&destination]
                .read()
                .get("items", "actual-tls-historical")
                .unwrap(),
            Some(original.clone())
        );
    }
    let report = serde_json::json!({
        "classification": if result.is_ok() {"LATE_PREPARED_COMPLETION_SUCCEEDED"} else {"LATE_PREPARED_COMPLETION_REFUSED"},
        "scope":scope,"data_leader":"n0","metadata_leader":"n1","stopped_endpoint":"n2",
        "activated_before_loss":activated,"late_prepared_range_index":prepared.reply().range_index,
        "completion_error":result.as_ref().err().map(ToString::to_string),
        "acknowledged_record_preserved_on_available_nodes":true,"activation_wire_tested":false,
        "physical_qualification":false,"irretrievable_loss_proven":false,
    });
    std::fs::write(
        fixture
            .native
            .root
            .path()
            .join("late-prepared-completion-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("LATE_PREPARED_COMPLETION {report}");
    let actual = result.expect("a physically Prepared same-voter follower should complete from the durable activated decision after one peer endpoint is unavailable");
    assert_eq!(actual.reply().stage, NativeCompletionStage::CompletedFrozen);
}
