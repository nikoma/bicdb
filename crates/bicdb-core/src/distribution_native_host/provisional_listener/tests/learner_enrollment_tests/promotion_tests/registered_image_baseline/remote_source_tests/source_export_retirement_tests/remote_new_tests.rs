//! Genuine mTLS participant creation/callback/handoff, following the actual
//! S2 export retirement. No local OLD/NEW owner is passed to an RPC caller.
use super::*;
mod remote_decision_tests;
mod remote_old_decision_tests;
use crate::distribution_range_handoff::{
    NativeRemoteNewFactSetV1, NativeRemoteNewOperationV1 as NewOp, NativeRemoteNewRequestV1,
    NativeRemoteNewRound,
};
use crate::distribution_transport::AuthenticatedNativeRemoteNewFrame;

pub(super) fn is_case(case: SourceCase) -> bool {
    remote_old_decision_tests::is_case(case)
        || remote_decision_tests::is_case(case)
        || matches!(
            case,
            SourceCase::TargetRemoteNewPins
                | SourceCase::TargetRemoteNewWrongFact
                | SourceCase::TargetRemoteNewBeforeCancel
                | SourceCase::TargetRemoteNewAfterCancel
                | SourceCase::TargetRemoteNewChangedIntent
        )
}
#[test]
fn native_registered_fifth_remote_new_actual_materialization_independent_fetch_all_pin_owners() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewPins),
    );
}
#[test]
fn native_registered_fifth_remote_new_actual_wrong_fact_digest_refuses_before_pin_take() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewWrongFact),
    );
}
#[test]
fn native_registered_fifth_remote_new_actual_cancel_before_take_same_phase_fresh_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewBeforeCancel),
    );
}
#[test]
fn native_registered_fifth_remote_new_actual_cancel_after_take_retains_exact_progress() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewAfterCancel),
    );
}
#[test]
fn native_registered_fifth_remote_new_actual_changed_valid_quorum_cannot_replace_intent() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewChangedIntent),
    );
}

fn exchange(
    client: &TcpClusterRelocationTransport,
    recipient: &ClusterNodeId,
    request: &NativeRemoteNewRequestV1,
    ledger: &mut Vec<serde_json::Value>,
    output: &std::path::Path,
) -> Result<AuthenticatedNativeRemoteNewFrame> {
    let parent = budget(5000);
    let (mq, business) = own(client, &parent);
    let i = ledger.len();
    ledger.push(
        serde_json::json!({"recipient":recipient,"exact_request":request,"outcome":"not_started"}),
    );
    evidence(&output.join("remote-new-actual-rpc-ledger.json"), ledger)?;
    let result = client.request_remote_native_new(recipient, request, &mq, business, &parent);
    ledger[i]["outcome"] = if result.is_ok() {
        "returned"
    } else {
        "error_unknown_effect"
    }
    .into();
    ledger[i]["actual_error"] =
        serde_json::to_value(result.as_ref().err().map(|e| format!("{e:?}")))?;
    if let Ok(actual) = &result {
        ledger[i]["actual_reply"] = serde_json::to_value(actual.reply())?;
    }
    let saved = evidence(&output.join("remote-new-actual-rpc-ledger.json"), ledger);
    match result {
        Err(primary) => {
            let _ = saved;
            Err(primary)
        }
        Ok(actual) => {
            saved?;
            Ok(actual)
        }
    }
}
fn request(
    manifest: &NativeRemoteImageManifestV1,
    phase_id: Uuid,
    operation: NewOp,
) -> NativeRemoteNewRequestV1 {
    NativeRemoteNewRequestV1 {
        version: 1,
        manifest: manifest.clone(),
        phase_id,
        operation,
        business: Uuid::now_v7(),
        challenge: Uuid::now_v7(),
        budget_ms: 4000,
    }
}
fn gates(
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    range: crate::RangeId,
    mf: &NativeRemoteImageManifestV1,
) -> serde_json::Value {
    let mut observed = BTreeMap::new();
    for id in mf.old_members.keys() {
        let i = (0..4).find(|i| node(*i) == *id).unwrap();
        let db = services[i].lock().database();
        observed.insert(
            id.clone(),
            db.read()
                .range_mutation_gate()
                .test_source_capture_pin_state(range)
                .unwrap(),
        );
    }
    observed.insert(
        node(4),
        factory
            .database
            .read()
            .range_mutation_gate()
            .test_source_capture_pin_state(range)
            .unwrap(),
    );
    serde_json::to_value(observed).unwrap()
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    target_control: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    mf: &NativeRemoteImageManifestV1,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    journal: &[serde_json::Value],
) {
    assert!(is_case(case));
    let collection = budget(5000);
    let phase_id = Uuid::now_v7();
    let business = Uuid::now_v7();
    // A genuine fresh metadata read produces scope, never a local OLD quorum.
    let observed = clients[0]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            mf.capture_range.id,
            mf.capture_range.epoch,
            Duration::from_secs(5),
            &collection,
        )
        .unwrap();
    let read_round = NativeJointOldRound::begin(observed, business, mf.relocation_id).unwrap();
    let plan = read_round.plan();
    drop(read_round);
    let mut round =
        NativeRemoteNewRound::begin(plan.clone(), mf.clone(), phase_id, &collection).unwrap();
    let native_before = old_records(services, &plan);
    let target_before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let mut ledger = Vec::new();
    let mut participants = mf.old_members.keys().cloned().collect::<Vec<_>>();
    participants.push(mf.target_id.clone());
    for id in &participants {
        let actual = exchange(
            &clients[0],
            id,
            &request(mf, phase_id, NewOp::Materialize),
            &mut ledger,
            output,
        )
        .unwrap();
        assert_eq!(
            actual.reply().snapshot.binding.target_attempt,
            mf.target_attempt
        );
        if mf.new_members.contains_key(id) {
            round.record(actual).unwrap();
        } else {
            assert!(actual.reply().snapshot.new_materialized.is_none());
        }
    }
    let quorum = round.finish().unwrap();
    let facts = quorum.facts().clone();
    assert_eq!(facts.members.len(), mf.new_members.len());
    let gate_before = gates(services, factory, mf.capture_range.id, mf);
    let phase_before = target_control
        .test_remote_new_phase_evidence(mf.capture_range.id)
        .unwrap();
    if matches!(case, SourceCase::TargetRemoteNewWrongFact) {
        let mut wrong = facts.clone();
        wrong.members.insert(mf.target_id.clone(), "0".repeat(64));
        wrong.checksum_sha256 = wrong.checksum().unwrap();
        let result = exchange(
            &clients[0],
            &mf.target_id,
            &request(mf, phase_id, NewOp::PrepareDecision { facts: wrong }),
            &mut ledger,
            output,
        );
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        evidence(&output.join("remote-new-before-assertions.json"),&serde_json::json!({"case":format!("{case:?}"),"phase_before":phase_before,"phase_after":after,"gate_before":gate_before,"gate_after":gates(services,factory,mf.capture_range.id,mf),"actual_error":result.as_ref().err().map(|e|format!("{e:?}")),"acknowledged":acknowledged,"source_frame_journal":journal,"no_Ready_Serving_or_native_DECISION":true})).unwrap();
        assert!(result.is_err());
        assert_eq!(after["handoff_started"], false);
        assert!(after["exact_intent"].is_null());
        assert_eq!(after["attempts"].as_array().unwrap().len(), 0);
        assert_eq!(
            gates(services, factory, mf.capture_range.id, mf),
            gate_before
        );
        assert_eq!(old_records(services, &plan), native_before);
        assert_eq!(
            state(&factory.database, pending, &node(4), ranges).unwrap(),
            target_before
        );
        return;
    }
    if matches!(
        case,
        SourceCase::TargetRemoteNewBeforeCancel | SourceCase::TargetRemoteNewAfterCancel
    ) {
        target_control
            .test_arm_remote_new_cancel_at_handoff(
                mf.capture_range.id,
                matches!(case, SourceCase::TargetRemoteNewAfterCancel),
            )
            .unwrap();
        let first = exchange(
            &clients[0],
            &mf.target_id,
            &request(
                mf,
                phase_id,
                NewOp::PrepareDecision {
                    facts: facts.clone(),
                },
            ),
            &mut ledger,
            output,
        );
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        let gate_after = gates(services, factory, mf.capture_range.id, mf);
        evidence(&output.join("remote-new-cancel-before-assertions.json"),&serde_json::json!({"case":format!("{case:?}"),"actual_error":first.as_ref().err().map(|e|format!("{e:?}")),"phase_before":phase_before,"phase_after":after,"gate_before":gate_before,"gate_after":gate_after,"original_A":mf.target_attempt,"acknowledged":acknowledged,"source_frame_journal":journal})).unwrap();
        assert!(first.is_err());
        assert_eq!(after["attempts"].as_array().unwrap().len(), 1);
        assert_eq!(after["attempts"][0]["original_request_cancelled"], true);
        assert_eq!(after["exact_intent"], serde_json::to_value(&facts).unwrap());
        if matches!(case, SourceCase::TargetRemoteNewBeforeCancel) {
            assert_eq!(gate_after, gate_before);
            assert_eq!(after["target_materialization_reference_detached"], false);
        } else {
            assert_eq!(after["target_export_reference_detached"], true);
            assert_eq!(after["target_materialization_reference_detached"], true);
            assert!(gate_after[mf.target_id.as_str()]["capture_pin"].is_null());
        }
    }
    let mut replies = BTreeMap::new();
    for id in &participants {
        let actual = exchange(
            &clients[0],
            id,
            &request(
                mf,
                phase_id,
                NewOp::PrepareDecision {
                    facts: facts.clone(),
                },
            ),
            &mut ledger,
            output,
        )
        .unwrap();
        let handoff = actual.reply().handoff.as_ref().unwrap();
        assert_eq!(handoff.fact_set_sha256, facts.checksum_sha256);
        replies.insert(id.clone(), actual.reply().clone());
    }
    let first = target_control
        .test_remote_new_phase_evidence(mf.capture_range.id)
        .unwrap();
    if matches!(case, SourceCase::TargetRemoteNewChangedIntent) {
        let mut changed: NativeRemoteNewFactSetV1 = facts.clone();
        let optional = mf
            .new_members
            .keys()
            .find(|id| **id != mf.target_id && **id != mf.activation_range.leader)
            .unwrap();
        changed.members.remove(optional);
        changed.checksum_sha256 = changed.checksum().unwrap();
        changed.require_manifest(mf).unwrap();
        let result = exchange(
            &clients[0],
            &mf.target_id,
            &request(mf, phase_id, NewOp::PrepareDecision { facts: changed }),
            &mut ledger,
            output,
        );
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        evidence(&output.join("remote-new-changed-intent-before-assertions.json"),&serde_json::json!({"actual_error":result.as_ref().err().map(|e|format!("{e:?}")),"before":first,"after":after,"original_facts":facts,"acknowledged":acknowledged})).unwrap();
        assert!(result.is_err());
        assert_eq!(after["exact_intent"], first["exact_intent"]);
        let before_attempts = first["attempts"].as_array().unwrap();
        let after_attempts = after["attempts"].as_array().unwrap();
        assert_eq!(before_attempts.len(), after_attempts.len());
        for (before, after) in before_attempts.iter().zip(after_attempts) {
            // Actual old deadlines may pass during the refused request. Compare
            // immutable custody, not a later clock read of that old deadline.
            for key in [
                "actual_independently_fetched_facts",
                "actual_observer",
                "original_request_cancelled",
                "original_role_context",
                "original_own_context",
                "source_export_terminal_retained",
            ] {
                assert_eq!(before[key], after[key]);
            }
        }
    }
    let gate_after = gates(services, factory, mf.capture_range.id, mf);
    let native_after = old_records(services, &plan);
    let target_after = state(&factory.database, pending, &node(4), ranges).unwrap();
    // Retain the diagnostic failure separately; BicDbError is intentionally not
    // serializable and this observation must not replace the primary operation.
    let target_phase_result = target_control.test_remote_new_phase_evidence(mf.capture_range.id);
    evidence(&output.join("remote-new-before-assertions.json"),&serde_json::json!({"case":format!("{case:?}"),"original_MF":mf,"phase_id":phase_id,"actual_facts":facts,"actual_replies":replies,"target_phase":target_phase_result.as_ref().ok(),"target_phase_error":target_phase_result.as_ref().err().map(|error|format!("{error:?}")),"gate_before":gate_before,"gate_after":gate_after,"native_before":native_before,"native_after":native_after,"target_before":target_before,"target_after":target_after,"every_operation":ledger,"acknowledged":acknowledged,"source_frame_journal":journal,"RAM_handoff_not_native_DECISION":true,"Serving_OLD_cleanup_or_physical_qualification":false})).unwrap();
    for id in participants {
        let key = id.as_str();
        assert!(gate_after[key]["capture_pin"].is_null());
        assert_eq!(gate_after[key]["frozen"], gate_before[key]["frozen"]);
        assert_eq!(gate_after[key]["frozen_owner_dropped"], false);
        assert_eq!(gate_after[key]["poisoned"], false);
    }
    assert_eq!(native_before, native_after);
    assert_eq!(target_before, target_after);
    assert!(!factory.database.read().commit_admission_ready());
    if remote_old_decision_tests::is_case(case) {
        remote_old_decision_tests::exercise(
            case,
            controls,
            clients,
            services,
            target_control,
            factory,
            pending,
            ranges,
            mf,
            phase_id,
            &facts,
            &plan,
            acknowledged,
            output,
            journal,
            &mut ledger,
        );
    }
    if remote_decision_tests::is_case(case) {
        remote_decision_tests::exercise(
            case,
            clients,
            services,
            target_control,
            factory,
            pending,
            ranges,
            mf,
            phase_id,
            &facts,
            acknowledged,
            output,
            journal,
            &mut ledger,
        );
    }
}
