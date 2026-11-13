//! Separate genuine full-fixture idempotence regression. Never prepublish a
//! native activation decision before the unchanged acceptance's real first
//! Runtime activation: that would move WAL cost outside its original budget.
use super::*;
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeCompletionStage};

#[test]
fn native_post_joint_actual_activation_idempotence_after_fresh_activation_ack4_and_strong() {
    assert_eq!(
        run_complete_joint_with_continuation(
            0,
            JointFullContinuation::PostJointNativeActivationIdempotence
        ),
        JointTestOutcome::FullHandoffThenCurrentJointActivationIdempotence,
        "the original fresh activation/ACK4/Strong path must finish before idempotence can qualify",
    );
}

pub(super) fn after_actual_fresh_activation_and_ack4(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
) {
    use super::post_joint_no_data_tests::{diagnostic_cut, report};
    let unknown_key = fixture.key(range.id, "post-joint-new-domain-prepare-only-no-ACK3");
    let ledger: Vec<crate::Record> = serde_json::from_slice(
        &std::fs::read(
            fixture
                .native
                .root
                .path()
                .join("actual-joint-selected-range-ack-ledger.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(ledger.len(), 3, "actual business ACK1/ACK2/ACK4 ledger");
    assert_eq!(&ledger[0], ack1);
    assert_eq!(&ledger[1], ack2);
    let before = diagnostic_cut(fixture, range, ack1, ack2, &unknown_key);
    let recipient = node("n1");
    assert_eq!(
        before[&recipient]["native"]["r"], 4,
        "actual original acceptance produced ACK4"
    );
    // This decoded actual durable scope is REQUEST DATA only. The real Runtime
    // still independently collects actual Completion QQ and a fresh own role,
    // and validates native decision/current scope/guard/physical identity.
    let scope: RecoveryFenceScope =
        serde_json::from_value(before[&recipient]["recovery_fence"]["scope"].clone()).unwrap();
    let phase = &before[&recipient]["recovery_fence"]["phase"]["NativeActivated"];
    assert!(
        phase.is_object(),
        "actual completed/released activation prerequisite"
    );
    let original_decision_sha = phase["native_activation_sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let original_catalog = std::fs::read(
        fixture.native.databases[&recipient]
            .read()
            .data_path()
            .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
    )
    .unwrap();
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let observed = fixture.runtimes[&recipient]
        .activate_native_recovery(
            &actor,
            &NativeCompletionRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 4500,
            },
            &parent,
        )
        .expect("fresh actual QQ/role must support idempotent existing Runtime activation");
    parent.check().unwrap();
    let reply = observed.reply();
    assert_eq!(reply.stage, NativeCompletionStage::ActivatedAnchor);
    assert_eq!(
        reply.range_index, 3,
        "immutable recovery decision is an R3 anchor, not a fabricated current Ready4"
    );
    assert_eq!(
        reply.activation_decision_sha256.as_deref(),
        Some(original_decision_sha.as_str())
    );
    let after = diagnostic_cut(fixture, range, ack1, ack2, &unknown_key);
    assert_eq!(
        after, before,
        "idempotent historical decision cannot change actual R4/native/raw/UID/rows/obligations"
    );
    assert_eq!(
        std::fs::read(
            fixture.native.databases[&recipient]
                .read()
                .data_path()
                .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG,)
        )
        .unwrap(),
        original_catalog
    );
    for id in [node("n1"), node("n2"), node("n3")] {
        let db = fixture.native.databases[&id].read();
        let actual = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(actual.range_index(), 4);
        assert_eq!(
            actual.logical_history_sha256(),
            before[&id]["native"]["h"].as_str().unwrap()
        );
        assert_eq!(
            actual.actual_local_visibility_sequence(),
            before[&id]["native"]["local_data_b"].as_u64().unwrap()
        );
        for record in &ledger {
            assert_eq!(
                db.get("items", &record.id).unwrap().as_deref(),
                Some(record)
            );
        }
        assert!(db.get("items", &unknown_key).unwrap().is_none());
    }
    for record in &ledger {
        require_strong_r4(fixture, range, &record.id, Some(record));
    }
    require_strong_r4(fixture, range, &unknown_key, None);
    report(
        fixture,
        "actual-post-joint-activation-idempotence-after-ACK4.json",
        &serde_json::json!({
            "scope_request_claim":scope,"original_native_activation_sha256":original_decision_sha,
            "actual_runtime_observation":reply,"actual_current_native_index":4,"immutable_anchor_index":3,
            "first_fresh_runtime_activation_ack4_strong_prerequisite_completed":true,
            "ready_grant_from_anchor":false,"same_catalogue_bytes":true,
        }),
    );
}

fn require_strong_r4(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    key: &str,
    expected: Option<&crate::Record>,
) {
    let request = fixture.request(key, range);
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let mq = fixture.clients[&range.leader]
        .acquire_metadata_read_observation(
            &node("n1"),
            request.business_request_id,
            range.id,
            range.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let read = observed(fixture.coordinators[&range.leader].strong_point_read(
        &fixture.runtimes[&range.leader],
        Some(fixture.hosts[&range.leader].clone()),
        fixture.clients[&range.leader].clone(),
        mq,
        request,
        &parent,
    ));
    assert_eq!(read.record().map(|record| record.as_ref()), expected);
    assert_eq!(read.audit().range_index, 4);
}
