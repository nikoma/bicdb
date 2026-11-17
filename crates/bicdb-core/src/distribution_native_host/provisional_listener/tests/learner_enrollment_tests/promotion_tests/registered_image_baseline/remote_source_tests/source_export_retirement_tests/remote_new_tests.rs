//! Genuine mTLS participant creation/callback/handoff, following the actual
//! S2 export retirement. No local OLD/NEW owner is passed to an RPC caller.
use super::*;
mod decision_certificate_tests;
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
        || decision_certificate_tests::is_case(case)
        || matches!(
            case,
            SourceCase::TargetRemoteNewPins
                | SourceCase::TargetRemoteNewWrongFact
                | SourceCase::TargetRemoteNewBeforeCancel
                | SourceCase::TargetRemoteNewAfterCancel
                | SourceCase::TargetRemoteNewChangedIntent
                | SourceCase::TargetRemoteNewRepeatedPrepare
                | SourceCase::TargetRemoteNewSlowPrepare
                | SourceCase::TargetRemoteNewSpentAttempts
        )
}
/// A preparation that is repeated for the same fact set after it completed
/// takes no further attempt, however often; and the exact-request table
/// admits more requests over time than it holds at once.
#[test]
fn native_registered_fifth_remote_new_repeated_prepare_is_idempotent_and_takes_no_attempt() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewRepeatedPrepare),
    );
}
/// The caller's deadline expires while the member is still preparing. The
/// member completes inside the request's own budget; a repetition during
/// that time is refused as busy, and the repetition after it succeeds from
/// the retained outcome.
#[test]
fn native_registered_fifth_remote_new_deadline_expired_mid_prepare_then_repeat_succeeds() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewSlowPrepare),
    );
}
/// More refused preparations than the attempt cap, none of which detached
/// anything, do not use up the participant.
#[test]
fn native_registered_fifth_remote_new_refused_preparations_do_not_exhaust_attempts() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteNewSpentAttempts),
    );
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

/// The same retained attempts: their number and their immutable custody. A
/// later clock read of an old request's deadline is not compared.
fn same_attempts(after: &serde_json::Value, before: &serde_json::Value) {
    let after = after["attempts"].as_array().unwrap();
    let before = before["attempts"].as_array().unwrap();
    assert_eq!(after.len(), before.len());
    for (after, before) in after.iter().zip(before) {
        for key in [
            "actual_independently_fetched_facts",
            "actual_observer",
            "original_request_cancelled",
            "original_role_context",
            "original_own_context",
            "source_export_terminal_retained",
            "established_intent_or_detached_a_pin",
        ] {
            assert_eq!(after[key], before[key], "{key}");
        }
    }
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
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    template: &crate::DistributionConfig,
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
    // Metadata leader n0 is the destination, not necessarily the range
    // authority that must originate this actual range-scoped acquisition.
    let selected = pending.range_by_id(mf.capture_range.id).unwrap();
    assert_eq!(selected.epoch, mf.capture_range.epoch);
    assert_eq!(selected.leader, mf.capture_range.leader);
    let leader_index = clients
        .iter()
        .position(|client| client.local_metadata_read_participant().node_id() == &selected.leader)
        .expect("actual selected range leader has its original transport");
    let collection = budget(5000);
    let phase_id = Uuid::now_v7();
    let business = Uuid::now_v7();
    // A genuine fresh metadata read produces scope, never a local OLD quorum.
    let acquired = clients[leader_index].acquire_metadata_read_observation(
        &node(0),
        business,
        mf.capture_range.id,
        mf.capture_range.epoch,
        Duration::from_secs(5),
        &collection,
    );
    if let Err(primary) = &acquired {
        let mut diagnostic_errors = Vec::new();
        evidence_after(
            &output.join("remote-new-range-authority-acquisition-failure.json"),
            &serde_json::json!({
                "case":format!("{case:?}"),"metadata_destination":node(0),
                "actual_requester":clients[leader_index].local_metadata_read_participant().node_id(),
                "selected_range":selected,"source_capture_range":mf.capture_range,
                "business":business,"actual_error":primary.to_string(),
                "request_retry_count":0,"acknowledged":acknowledged,
                "source_frame_journal":journal,
                "participant_materialization_not_started":true,
            }),
            &mut diagnostic_errors,
        );
        for error in diagnostic_errors {
            eprintln!("REMOTE_NEW_ACQUISITION_DIAGNOSTIC_ERROR {error}; primary={primary}");
        }
    }
    let observed = acquired.unwrap();
    let read_round = NativeJointOldRound::begin(observed, business, mf.relocation_id).unwrap();
    let plan = read_round.plan();
    drop(read_round);
    if decision_certificate_tests::is_case(case) {
        // The coordinator initiator runs the complete remote round itself.
        decision_certificate_tests::exercise(
            case,
            controls,
            clients,
            metadata,
            template,
            target_control,
            mf,
            phase_id,
            &plan,
            output,
        );
        return;
    }
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
    if matches!(case, SourceCase::TargetRemoteNewSpentAttempts) {
        // Ten preparations in a row end inside the sink before any pin is
        // taken (more than the cap of eight). Only the first, which fixed
        // the fact-set intent, stays retained.
        for round in 0..10 {
            target_control
                .test_arm_remote_new_cancel_at_handoff(mf.capture_range.id, false)
                .unwrap();
            let refused = exchange(
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
            assert!(refused.is_err(), "round {round}");
            let after = target_control
                .test_remote_new_phase_evidence(mf.capture_range.id)
                .unwrap();
            let attempts = after["attempts"].as_array().unwrap();
            assert_eq!(attempts.len(), if round == 0 { 1 } else { 2 }, "{after}");
            assert_eq!(attempts[0]["established_intent_or_detached_a_pin"], true);
            assert_eq!(after["target_materialization_reference_detached"], false);
            assert_eq!(after["target_export_reference_detached"], false);
            assert_eq!(
                gates(services, factory, mf.capture_range.id, mf),
                gate_before
            );
        }
    }
    if matches!(case, SourceCase::TargetRemoteNewSlowPrepare) {
        target_control
            .test_arm_remote_new_slow_handoff(mf.capture_range.id)
            .unwrap();
        let parent = budget(5000);
        let (mq, own_business) = own(&clients[0], &parent);
        // The caller waits 1.2 s; the member's sink alone takes 2 s of the
        // request's 4 s budget.
        let caller = budget(1200);
        let slow = request(
            mf,
            phase_id,
            NewOp::PrepareDecision {
                facts: facts.clone(),
            },
        );
        let started = Instant::now();
        let lost =
            clients[0].request_remote_native_new(&mf.target_id, &slow, &mq, own_business, &caller);
        let lost = lost.err().map(|e| e.to_string());
        eprintln!(
            "REMOTE_NEW_SLOW_PREPARE caller={lost:?} after={:?}",
            started.elapsed()
        );
        assert!(lost.is_some(), "the caller's deadline expired first");
        assert!(started.elapsed() < Duration::from_millis(1900));
        // The member is still working: a repetition is refused as busy and
        // does not start the same work a second time.
        let busy = exchange(
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
        )
        .err()
        .map(|e| e.to_string())
        .expect("a repetition while the member still prepares is refused");
        assert!(
            busy.contains("busy: an earlier preparation of this phase is still running"),
            "{busy}"
        );
        std::thread::sleep(Duration::from_millis(3500).saturating_sub(started.elapsed()));
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        // Completed and retained, by the request whose caller had gone.
        let attempts = after["attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), 1, "{after}");
        assert_eq!(attempts[0]["original_request_cancelled"], false);
        assert_eq!(after["target_export_reference_detached"], true);
        assert_eq!(after["target_materialization_reference_detached"], true);
        let repeated = exchange(
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
        )
        .unwrap();
        assert_eq!(
            repeated.reply().handoff.as_ref().unwrap().fact_set_sha256,
            facts.checksum_sha256
        );
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        assert_eq!(after["attempts"].as_array().unwrap().len(), 1, "{after}");
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
    if matches!(case, SourceCase::TargetRemoteNewSpentAttempts) {
        // The refused ones are gone; the one that fixed the intent and the
        // one that detached the pins remain.
        assert_eq!(first["attempts"].as_array().unwrap().len(), 2, "{first}");
    }
    if matches!(case, SourceCase::TargetRemoteNewRepeatedPrepare) {
        // More repetitions than the attempt cap, on every participant.
        for round in 0..12 {
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
                .unwrap_or_else(|error| panic!("round {round} {id}: {error}"));
                assert_eq!(actual.reply().handoff, replies[id].handoff);
                assert_eq!(actual.reply().snapshot, replies[id].snapshot);
            }
        }
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        assert_eq!(first["attempts"].as_array().unwrap().len(), 1, "{first}");
        same_attempts(&after, &first);
        assert_eq!(after["exact_intent"], first["exact_intent"]);
        assert_eq!(
            after["retained_encoded_frame_bytes"],
            first["retained_encoded_frame_bytes"]
        );
        // A different fact set is still refused after the repetitions.
        let mut wrong = facts.clone();
        wrong.members.insert(mf.target_id.clone(), "0".repeat(64));
        wrong.checksum_sha256 = wrong.checksum().unwrap();
        assert!(exchange(
            &clients[0],
            &mf.target_id,
            &request(mf, phase_id, NewOp::PrepareDecision { facts: wrong }),
            &mut ledger,
            output,
        )
        .is_err());
        // The exact-request table: more requests than it holds at once are
        // admitted over time, because a request that is neither held by an
        // attempt nor still inside its own budget frees its slot.
        // (An observation is admitted from a participant, as in a callback.)
        let observer = (0..4)
            .find(|i| mf.new_members.contains_key(&node(*i)))
            .unwrap();
        for sent in 0..80 {
            let mut short = request(mf, phase_id, NewOp::Observe);
            short.budget_ms = 500;
            let parent = budget(5000);
            let (mq, own_business) = own(&clients[observer], &parent);
            clients[observer]
                .request_remote_native_new(&mf.target_id, &short, &mq, own_business, &parent)
                .unwrap_or_else(|error| panic!("observation {sent}: {error}"));
        }
        let after = target_control
            .test_remote_new_phase_evidence(mf.capture_range.id)
            .unwrap();
        assert!(
            after["retained_requests"].as_u64().unwrap() <= 64,
            "{after}"
        );
        same_attempts(&after, &first);
    }
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
