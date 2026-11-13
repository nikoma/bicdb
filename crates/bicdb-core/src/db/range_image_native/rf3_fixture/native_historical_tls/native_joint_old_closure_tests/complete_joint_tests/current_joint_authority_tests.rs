//! Actual authority refusals after the SAME genuine joint/ACK2/StrongR2 path.
//! Request purposes/nonces are claims, never locally fabricated role/quorum
//! objects. Every role below comes from the actual independent MQ producer.
use super::post_joint_no_data_tests::{
    diagnostic_cut, fresh_new_scope, report, require_inherited, snapshot,
};
use super::*;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

#[test]
fn native_current_joint_actual_authority_and_original_writer_cannot_rebase_recovery() {
    assert_eq!(
        run_complete_joint_with_continuation(
            0,
            JointFullContinuation::CurrentJointAuthorityRefusals
        ),
        JointTestOutcome::FullHandoffThenCurrentJointAuthorityRefusals,
        "a preliminary publication/open result cannot qualify these real authority controls"
    );
}
fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
fn actual_role(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    purpose: MetadataRecoveryRolePurpose,
    nonce: Uuid,
    parent: &CancellationToken,
) -> RecoveryRoleAdmission {
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let witness = fixture.providers[&range.leader]
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            parent,
        )
        .expect("genuine actual current MQ role prerequisite");
    RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        &range.leader,
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .expect("actual sealed role, not a purpose DTO as authority")
}
fn require_no_canonical_abort3(fixture: &EnforcedNetwork, range: crate::RangeId) {
    for id in [node("n1"), node("n2"), node("n3")] {
        let database = fixture.native.databases[&id].read();
        // Actual protected record read under ONE local native journal; this
        // decodes no authority and does not hold a service/network latch.
        let _journal = database.try_native_image_journal().unwrap();
        let record = database
            .read_native_range_image_record::<serde_json::Value>(&format!(
                "abort-decision:{range}:{:020}",
                3
            ))
            .unwrap();
        assert!(
            record.is_none(),
            "{id}: refusal cannot publish an actual canonical NoData decision"
        );
    }
}
fn reject_claim(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
    key: &str,
    label: &str,
    purpose: MetadataRecoveryRolePurpose,
    nonce: Uuid,
    expected: &str,
) {
    let before = diagnostic_cut(fixture, range, ack1, ack2, key);
    let parent = token();
    let role = actual_role(fixture, range, purpose, nonce, &parent);
    role.require_live_request().unwrap();
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    let request = RecoveryEpochFenceRequest {
        scope: scope.clone(),
        budget_ms: 4000,
    };
    let error = fixture.clients[&range.leader]
        .recovery_epoch_fence_observation(&range.leader, &request, &parent)
        .expect_err("actual protected receiver must reject the request-bound wrong claim");
    parent
        .check()
        .expect("expiry cannot satisfy a semantic authority refusal");
    role.require_live_request()
        .expect("same ORIGINAL real role remains live at semantic refusal");
    assert!(
        error.to_string().contains(expected),
        "{label}: intended semantic guard, not generic network/timeout: {error}"
    );
    fixture.idle();
    let after = diagnostic_cut(fixture, range, ack1, ack2, key);
    report(
        fixture,
        &format!("actual-current-joint-authority-{label}.json"),
        &serde_json::json!({
            "label":label,"actual_scope":scope,"actual_error":error.to_string(),
            "original_role_live_after_refusal":true,"before":before,"after":after,
        }),
    );
    assert_eq!(
        after, before,
        "{label}: no catalogue bytes, native point, rows, UID, promises or slot controls changed"
    );
    require_no_canonical_abort3(fixture, range.id);
    for id in [node("n1"), node("n2"), node("n3")] {
        assert!(fixture.native.databases[&id].read().range_mutation_gate().try_enter(range.id).is_ok(),
            "{label}: receiver preflight must refuse BEFORE rotating/freezing actual serving generation on {id}");
    }
}
pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
    actual_protected_handoff_nonce: Uuid,
) {
    assert_eq!(range.leader, node("n3"));
    let key = fixture.key(range.id, "actual-current-joint-authority-no-row");
    let points = fixture
        .native
        .databases
        .keys()
        .map(|id| (id.clone(), snapshot(fixture, range, id)))
        .collect::<BTreeMap<_, _>>();
    for id in [node("n1"), node("n2"), node("n3")] {
        assert_eq!(points[&id]["r"], 2);
    }
    require_inherited(fixture, range, &points, ack1, ack2);
    // Read-only altered CLAIM negatives against ACTUAL protected local closure.
    // This is not installation of a fabricated later promise or a new QQ.
    let before_claims = diagnostic_cut(fixture, range, ack1, ack2, &key);
    let global_cut = before_claims[&node("n1")]["joint"]["serving_activation"]["range_index"]
        .as_u64()
        .unwrap();
    let mut exact_local = None;
    fixture.native.services[&node("n1")]
        .lock()
        .test_current_joint_local_promise_claim(range.id, &token(), |local| {
            exact_local = local.clone();
            local
        })
        .expect("actual local resolved cut must be proved independently from request cut");
    assert_eq!(exact_local.as_ref().unwrap().resolved_through, 1);
    for (label, change) in [
        ("foreign-request", 0),
        ("lowered-local-floor", 1),
        ("raised-uncovered-floor", 2),
        ("missing-survivor-promise", 3),
    ] {
        let error = fixture.native.services[&node("n1")]
            .lock()
            .test_current_joint_local_promise_claim(range.id, &token(), |local| {
                let mut claim = local.unwrap();
                match change {
                    0 => claim.plan_id.push_str("-foreign-CLAIM"),
                    1 => claim.resolved_through = 0,
                    2 => claim.resolved_through = global_cut.checked_add(1).unwrap(),
                    3 => return None,
                    _ => unreachable!(),
                }
                Some(claim)
            })
            .expect_err(
                "a decoded/altered claim cannot replace actual protected local promise proof",
            );
        assert!(error.to_string().contains("promise"), "{label}: {error}");
    }
    fixture.native.services[&node("n3")]
        .lock()
        .test_current_joint_local_promise_claim(range.id, &token(), |local| {
            assert!(local.is_none());
            local
        })
        .expect("actual image target has genuine None, not a synthesized OLD voter promise");
    let error = fixture.native.services[&node("n3")]
        .lock()
        .test_current_joint_local_promise_claim(range.id, &token(), |_| exact_local.clone())
        .expect_err("actual target cannot acquire another member's original promise");
    assert!(
        error.to_string().contains("image target cannot synthesize"),
        "{error}"
    );
    let after_claims = diagnostic_cut(fixture, range, ack1, ack2, &key);
    report(
        fixture,
        "actual-current-joint-local-promise-proof-claims.json",
        &serde_json::json!({
            "boundary":"read-only CLAIM validation against actual opaque native closure fact; no persisted foreign promise controller",
            "global_cut":global_cut,"exact_actual_local_promise":exact_local,"before":before_claims,"after":after_claims,
        }),
    );
    assert_eq!(
        after_claims, before_claims,
        "read-only claim rejection cannot mutate either original promise or native/raw state"
    );
    let discovered = fixture.native.services[&range.leader]
        .lock()
        .observe_current_joint_recovery_purpose(range.id, &token())
        .unwrap();
    reject_claim(
        fixture,
        range,
        ack1,
        ack2,
        &key,
        "ordinary-purpose",
        MetadataRecoveryRolePurpose::NormalRecovery,
        Uuid::now_v7(),
        "normal recovery cannot supersede a handoff recovery",
    );
    let mut wrong_domain = discovered.clone();
    let MetadataRecoveryRolePurpose::CurrentJointRecovery {
        joint_decision_sha256,
    } = &mut wrong_domain
    else {
        panic!("discovery must read the actual protected joint domain");
    };
    // Deliberately wrong REQUEST DATA. Its fresh metadata role is genuine;
    // the receiver must still match the actual protected domain independently.
    let replacement = if joint_decision_sha256.starts_with('a') {
        "b"
    } else {
        "a"
    };
    joint_decision_sha256.replace_range(0..1, replacement);
    reject_claim(
        fixture,
        range,
        ack1,
        ack2,
        &key,
        "foreign-joint-domain",
        wrong_domain,
        Uuid::now_v7(),
        "current joint recovery needs exact activated epoch",
    );
    let original_nonce = points[&range.leader]["recovery_fence"]["scope"]["recovery_nonce"]
        .as_str()
        .unwrap()
        .parse::<Uuid>()
        .unwrap();
    assert_eq!(
        original_nonce, actual_protected_handoff_nonce,
        "decoded refusal nonce must equal the actual completed protected decision's original nonce"
    );
    reject_claim(
        fixture,
        range,
        ack1,
        ack2,
        &key,
        "original-handoff-nonce",
        discovered.clone(),
        original_nonce,
        "new joint recovery cannot reuse original handoff identity",
    );

    // Expire the SAME actual role/request. No later parent, new witness or
    // budget replaces its original absolute deadline to obtain a refusal.
    let before = diagnostic_cut(fixture, range, ack1, ack2, &key);
    let parent = token();
    let expired_role = actual_role(fixture, range, discovered.clone(), Uuid::now_v7(), &parent);
    expired_role.require_live_request().unwrap();
    let expired_scope = RecoveryFenceScope::from_role(&expired_role).unwrap();
    let deadline = parent.deadline().unwrap().max(expired_role.deadline());
    std::thread::sleep(
        deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
    );
    let error = fixture.clients[&range.leader]
        .recovery_epoch_fence_observation(
            &range.leader,
            &RecoveryEpochFenceRequest {
                scope: expired_scope.clone(),
                budget_ms: 4000,
            },
            &parent,
        )
        .expect_err("expired ORIGINAL operation cannot use still-valid protected historical facts");
    assert!(matches!(error, crate::BicDbError::QueryTimedOut), "{error}");
    assert!(expired_role.require_live_request().is_err());
    let after = diagnostic_cut(fixture, range, ack1, ack2, &key);
    report(
        fixture,
        "actual-current-joint-authority-original-expiry.json",
        &serde_json::json!({
            "scope":expired_scope,"actual_error":error.to_string(),"before":before,"after":after,
            "fresh_role_or_parent_after_expiry":false,
        "refusal_boundary":"original-caller-client-pre-IO", "receiver_late-role_rejection_observed":false,
        }),
    );
    assert_eq!(
        after, before,
        "original expiry must not publish native/raw refusal or alter physical generation"
    );
    assert!(fixture.native.databases[&range.leader]
        .read()
        .range_mutation_gate()
        .try_enter(range.id)
        .is_ok());

    require_no_canonical_abort3(fixture, range.id);

    // Now a DIFFERENT actual operation obtains its real full current-member
    // fence quorum, preserving the unmodified genuine positive producer.
    let scope = fresh_new_scope(fixture, range, &points, ack1, ack2, &key);
    let parent = token();
    let local = range.leader.clone();
    let business = Uuid::now_v7();
    let mq = fixture.clients[&local]
        .acquire_host_metadata_observation(&node("n1"), business, Duration::from_secs(5), &parent)
        .unwrap();
    let policy = {
        let service = fixture.native.services[&local].lock();
        let database = fixture.native.databases[&local].read();
        service
            .prepare_native_host_writer_enrollment(&database, mq, business)
            .unwrap()
    };
    let role = actual_role(fixture, range, discovered, scope.recovery_nonce, &parent);
    scope.require_role(&role).unwrap();
    let before = diagnostic_cut(fixture, range, ack1, ack2, &key);
    // Keep the OLD genuine host alive too: this tests pointer rebinding, not
    // merely disappearance of a weak owner or expired metadata observation.
    let _original_host = fixture.hosts[&local].clone();
    policy.require_live().unwrap();
    let _replacement = fixture.native.databases[&local]
        .read()
        .install_enrolled_native_coordinator(fixture.coordinators[&local].clone(), policy)
        .expect("genuine same-config fresh Host MQ writer reinstall prerequisite");
    parent.check().unwrap();
    role.require_live_request()
        .expect("ORIGINAL real mutation role remains live after actual reinstall");
    let error = fixture.clients[&local]
        .recovery_epoch_fence_observation(
            &local,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 4000,
            },
            &parent,
        )
        .expect_err("saved current-joint operation cannot adopt a replacement writer object");
    parent
        .check()
        .expect("ORIGINAL deadline must remain live at writer identity refusal");
    role.require_live_request()
        .expect("writer identity rejection cannot be satisfied by role expiry");
    assert!(
        error.to_string().contains("writer"),
        "actual writer-object refusal, not another failure: {error}"
    );
    let after = diagnostic_cut(fixture, range, ack1, ack2, &key);
    report(
        fixture,
        "actual-current-joint-authority-original-writer.json",
        &serde_json::json!({
            "scope":scope,"actual_error":error.to_string(),"original_host_retained":true,
            "original_role_live":true,"before":before,"after":after,
            "continued_old_registration_liveness_claim":false,
            "native_policy_reinstallation_is_expected_metadata_only":true,
        }),
    );
    assert_eq!(
        after, before,
        "reinstallation/refusal cannot rewrite saved operation or native/raw history"
    );
    for id in [node("n1"), node("n2"), node("n3")] {
        assert!(
            fixture.native.databases[&id]
                .read()
                .range_mutation_gate()
                .try_enter(range.id)
                .is_err(),
            "new operation remains durably/physically fenced on {id}"
        );
    }
    require_inherited(fixture, range, &points, ack1, ack2);
    require_no_canonical_abort3(fixture, range.id);
    eprintln!("ACTUAL_CURRENT_JOINT_AUTHORITY_TERMINAL real-joint-ACK2-StrongR2=true semantic-refusals=3 original-caller-client-expiry=true actual-full-current-fence=true original-writer-rebase-refused=true native-noData-not-published=true no-ACK4-claim=true");
}
