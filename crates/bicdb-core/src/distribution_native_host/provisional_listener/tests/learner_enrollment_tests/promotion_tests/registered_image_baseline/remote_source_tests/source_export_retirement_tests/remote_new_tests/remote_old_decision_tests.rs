//! Actual INITIAL OLD publishers. No target scope, local QQ or catalogue vote.
use super::*;
use crate::distribution_range_handoff::NativeRemoteDecisionSourceRequestV1;
pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetRemoteOldDecisionRetiree
            | SourceCase::TargetRemoteOldDecisionSurvivor
            | SourceCase::TargetRemoteOldDecisionDonor
            | SourceCase::TargetRemoteOldDecisionObserver
    )
}
#[test]
fn native_registered_fifth_remote_old_decision_actual_retiree_postcommit_cancel_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteOldDecisionRetiree),
    );
}
#[test]
fn native_registered_fifth_remote_old_decision_actual_survivor_native_body_and_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteOldDecisionSurvivor),
    );
}
#[test]
fn native_registered_fifth_remote_old_decision_actual_nonleader_donor_independent_self_callback() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteOldDecisionDonor),
    );
}
#[test]
fn native_registered_fifth_remote_old_decision_actual_observer_and_capture_ingress_refusals() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteOldDecisionObserver),
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    _target: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    topology: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    mf: &NativeRemoteImageManifestV1,
    phase: Uuid,
    facts: &NativeRemoteNewFactSetV1,
    plan: &NativeJointHandoffPlan,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    journal: &[serde_json::Value],
    ledger: &mut Vec<serde_json::Value>,
) {
    let gate_before = gates(services, factory, mf.capture_range.id, mf);
    if matches!(case, SourceCase::TargetRemoteOldDecisionObserver) {
        observer_refusals(clients, topology, mf, plan, acknowledged, output, journal);
        assert_eq!(
            gate_before,
            gates(services, factory, mf.capture_range.id, mf)
        );
        return;
    }
    let selected = match case {
        SourceCase::TargetRemoteOldDecisionRetiree => mf
            .old_members
            .keys()
            .find(|id| !mf.new_members.contains_key(*id))
            .unwrap(),
        SourceCase::TargetRemoteOldDecisionDonor => &mf.source_id,
        _ => mf
            .old_members
            .keys()
            .find(|id| mf.new_members.contains_key(*id) && **id != mf.source_id)
            .unwrap(),
    };
    let index = (0..4).find(|i| node(*i) == *selected).unwrap();
    assert_ne!(selected, &mf.target_id);
    if matches!(case, SourceCase::TargetRemoteOldDecisionDonor) {
        assert_ne!(selected, &mf.capture_range.leader);
    }
    let database = services[index].lock().database();
    let before = state(&database, topology, selected, ranges).unwrap();
    let deny_before = database
        .read()
        .range_mutation_gate()
        .try_enter(mf.capture_range.id)
        .err()
        .map(|e| e.to_string());
    let postcommit = matches!(case, SourceCase::TargetRemoteOldDecisionRetiree);
    if postcommit {
        controls[index]
            .test_arm_old_remote_native_decision_cut(mf.capture_range.id, 2)
            .unwrap();
    }
    let operation = NewOp::PublishDecision {
        facts: facts.clone(),
    };
    let first = exchange(
        &clients[0],
        selected,
        &request(mf, phase, operation.clone()),
        ledger,
        output,
    );
    let primary = first.as_ref().err().map(|e| format!("{e:?}"));
    let first_native =
        controls[index].test_old_remote_native_decision_evidence(mf.capture_range.id);
    evidence(&output.join("remote-old-decision-first-before-assertions.json"),&serde_json::json!({
        "case":format!("{case:?}"),"actual_recipient":selected,"retiree":!mf.new_members.contains_key(selected),
        "source_donor":mf.source_id,"original_MF":mf,"phase":phase,"before":before,
        "actual_error":primary,"actual_reply":first.as_ref().ok().map(|f|f.reply()),
        "native":first_native.as_ref().ok(),"diagnostic_error":first_native.as_ref().err().map(ToString::to_string),
        "ordinary_gate_error_not_end_to_end_APP":deny_before,"source_APP_ACKs":acknowledged,
        "every_actual_operation":ledger,"source_frame_journal":journal,"no_catalogue_vote_or_Serving":true})).unwrap();
    let first_native = first_native.unwrap();
    let actual = &first_native["native"]["actual_native_attempt"];
    let xid = actual["native_xid"].as_u64().unwrap();
    assert!(xid > 0);
    assert_eq!(actual["outcome"]["state"], "actual_commit_returned");
    assert!(deny_before.is_some());
    if postcommit {
        assert!(first.is_err());
    } else {
        assert_eq!(
            first
                .unwrap()
                .reply()
                .native_decision
                .as_ref()
                .unwrap()
                .native_page_xid,
            xid
        );
    }
    let retry = exchange(
        &clients[0],
        selected,
        &request(mf, phase, operation),
        ledger,
        output,
    );
    let retry_error = retry.as_ref().err().map(|e| format!("{e:?}"));
    let after_native =
        controls[index].test_old_remote_native_decision_evidence(mf.capture_range.id);
    let after = state(&database, topology, selected, ranges);
    let deny_after = database
        .read()
        .range_mutation_gate()
        .try_enter(mf.capture_range.id)
        .err()
        .map(|e| e.to_string());
    let gate_after = gates(services, factory, mf.capture_range.id, mf);
    evidence(&output.join("remote-old-decision-retry-before-assertions.json"),&serde_json::json!({
        "actual_recipient":selected,"original_native_xid":xid,"actual_error":retry_error,
        "actual_reply":retry.as_ref().ok().map(|f|f.reply()),"before":before,"after":after.as_ref().ok(),
        "after_error":after.as_ref().err().map(ToString::to_string),"first_native":first_native,
        "after_native":after_native.as_ref().ok(),"native_diagnostic_error":after_native.as_ref().err().map(ToString::to_string),
        "ordinary_gate_error_not_end_to_end_APP":deny_after,"gate_before":gate_before,"gate_after":gate_after,
        "source_APP_ACKs":acknowledged,"every_actual_operation":ledger,"source_frame_journal":journal,
        "no_catalogue_vote_activation_Open_or_Serving":true})).unwrap();
    let receipt = retry.unwrap().reply().native_decision.clone().unwrap();
    assert_eq!(receipt.node_id, *selected);
    assert_eq!(receipt.native_page_xid, xid);
    let after_native = after_native.unwrap();
    assert_eq!(first_native["native"], after_native["native"]);
    for observed in after_native["source_callback_actual_observers"]
        .as_array()
        .unwrap()
    {
        assert_eq!(observed["node"], selected.as_str());
    }
    assert_eq!(gate_before, gate_after);
    assert!(deny_after.is_some());
    let after = after.unwrap();
    for field in [
        "B",
        "birth",
        "schema",
        "namespace_ids",
        "protected_catalogue_uuid",
        "rows",
        "raw",
        "writer_installed",
    ] {
        assert_eq!(
            before[field], after[field],
            "native decision does not change {field}"
        );
    }
    let other = ranges[1].id.get().to_string();
    assert!(before["points"].get(&other).is_some());
    assert_eq!(before["points"][&other], after["points"][&other]);
}
fn observer_refusals(
    clients: &[Arc<TcpClusterRelocationTransport>],
    topology: &ClusterTopology,
    mf: &NativeRemoteImageManifestV1,
    plan: &NativeJointHandoffPlan,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    journal: &[serde_json::Value],
) {
    let caller = (0..4)
        .find(|i| mf.old_members.contains_key(&node(*i)))
        .unwrap();
    let budget = budget(5000);
    let (own, business) = own(&clients[caller], &budget);
    let source = crate::distribution_range_image::NativeSourceDiscoveryRequestV1::from_plan(
        plan,
        mf.source_id.clone(),
        mf.target_id.clone(),
        mf.target_attempt,
        crate::distribution_range_image::NativeSourceDiscoveryOperationV1::DecisionProvenance,
        &own,
        business,
        4000,
    )
    .unwrap();
    let valid = NativeRemoteDecisionSourceRequestV1 {
        source,
        observer_id: node(caller),
        observer_birth: plan.enrolled_birth(&node(caller)).unwrap().clone(),
    };
    let outside = (0..4)
        .find(|i| {
            !mf.old_members.contains_key(&node(*i)) && !mf.new_members.contains_key(&node(*i))
        })
        .unwrap();
    let other = mf
        .old_members
        .keys()
        .find(|id| **id != node(caller))
        .unwrap();
    let mut cases = Vec::new();
    let mut outside_request = valid.clone();
    outside_request.observer_id = node(outside);
    outside_request.observer_birth = topology.nodes[&node(outside)]
        .native_store_birth_v1
        .as_ref()
        .unwrap()
        .clone();
    cases.push((
        "authenticated_nonparticipant",
        outside,
        outside_request,
        false,
    ));
    let mut wrong = valid.clone();
    wrong.observer_id = other.clone();
    wrong.observer_birth = plan.enrolled_birth(other).unwrap().clone();
    cases.push((
        "actual_peer_differs_from_claimed_observer",
        caller,
        wrong,
        false,
    ));
    let mut birth = valid.clone();
    birth.observer_birth = plan.enrolled_birth(other).unwrap().clone();
    cases.push(("actual_observer_wrong_native_birth", caller, birth, false));
    for (name, op) in [
        (
            "forbidden_Start",
            crate::distribution_range_image::NativeSourceDiscoveryOperationV1::Start,
        ),
        (
            "forbidden_Describe",
            crate::distribution_range_image::NativeSourceDiscoveryOperationV1::Describe,
        ),
    ] {
        let mut request = valid.clone();
        request.source.operation = op;
        cases.push((name, caller, request, false));
    }
    cases.push(("genuine_old_readonly_observer", caller, valid, true));
    let mut observations = Vec::new();
    for (name, index, mut request, allowed) in cases {
        request.source.challenge = Uuid::now_v7();
        request.source.business = Uuid::now_v7();
        observations.push(serde_json::json!({"case":name,"actual_TLS_caller":node(index),"request":request,"outcome":"not_started"}));
        evidence(
            &output.join("remote-old-source-observer-ledger.json"),
            &observations,
        )
        .unwrap();
        let result = clients[index]
            .test_send_participant_decision_provenance(&request, &super::budget(5000));
        let row = observations.last_mut().unwrap();
        row["actual_error"] =
            serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
        row["returned"] = serde_json::json!(result.is_ok());
        if let Ok(reply) = &result {
            row["actual_reply"] = serde_json::to_value(reply).unwrap();
        }
        row["expected_allowed"] = serde_json::json!(allowed);
        let expected_refusal = match name {
            "authenticated_nonparticipant" => Some("not an original OLD/NEW participant"),
            "actual_peer_differs_from_claimed_observer" => {
                Some("differs from actual authenticated peer")
            }
            "actual_observer_wrong_native_birth" => Some("changed original enrolled birth"),
            "forbidden_Start" | "forbidden_Describe" => {
                Some("accepts only readonly decision observation")
            }
            _ => None,
        };
        row["expected_refusal"] = serde_json::json!(expected_refusal);
        if let Ok(reply) = &result {
            let validated = (|| -> Result<()> {
                if reply.request != request.source
                    || reply.checksum()? != reply.checksum_sha256
                    || reply.manifest.checksum_sha256 != mf.checksum_sha256
                {
                    return Err(BicDbError::Cluster(
                        "readonly provenance reply changed request/MF/checksum".into(),
                    ));
                }
                reply
                    .decision_provenance
                    .as_ref()
                    .ok_or_else(|| {
                        BicDbError::Cluster(
                            "readonly provenance reply missing actual certificate proof".into(),
                        )
                    })?
                    .require_manifest(mf)
            })();
            row["actual_reply_validation_error"] =
                serde_json::json!(validated.err().map(|e| e.to_string()));
        }
        evidence(
            &output.join("remote-old-source-observer-ledger.json"),
            &observations,
        )
        .unwrap();
    }
    evidence(&output.join("remote-old-source-observer-before-assertions.json"),&serde_json::json!({"observations":observations,"source_APP_ACKs":acknowledged,"source_frame_journal":journal,"readonly_only_no_native_publication":true})).unwrap();
    for row in &observations {
        assert_eq!(row["returned"], row["expected_allowed"], "{row}");
        if let Some(expected) = row["expected_refusal"].as_str() {
            assert!(
                row["actual_error"].as_str().unwrap().contains(expected),
                "actual intended source-identity refusal: {row}"
            );
        }
        if row["expected_allowed"] == true {
            assert!(row["actual_reply_validation_error"].is_null());
        }
    }
}
