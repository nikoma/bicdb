//! The Registered fifth is driven ONLY through the coordinator's bounded
//! TargetInstall step RPC. This test never hands the target a plan, manifest,
//! source fact, role or own observation and never calls its in-process
//! install methods; the host handle is used for memory-only observation.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointHandoffStepOperationV1 as StepOperation, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepRequestV1, NativeJointHandoffStepStageV1 as Stage,
};

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::NativeHandoffStepInstall | SourceCase::NativeHandoffStepRefusals
    )
}
// The coordinator is the metadata leader's process. A Registered target's
// CLOSED fence is bound to its initiator, so these cases fence from that
// same metadata leader instead of the original range leader.
#[test]
fn native_handoff_step_target_install_only_step_rpcs_retains_joint_target_and_identical_retry() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::MetadataLeader,
        Some(SourceCase::NativeHandoffStepInstall),
    );
}
#[test]
fn native_handoff_step_refuses_foreign_peer_changed_identity_and_second_attempt() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::MetadataLeader,
        Some(SourceCase::NativeHandoffStepRefusals),
    );
}

fn request(
    client: &TcpClusterRelocationTransport,
    plan: &NativeJointHandoffPlan,
    donor: &ClusterNodeId,
    attempt: Uuid,
    budget_ms: u64,
    parent: &CancellationToken,
) -> (
    NativeJointHandoffStepRequestV1,
    crate::distribution_consensus::HostMetadataReadObservation,
) {
    let business = Uuid::now_v7();
    let own = client
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), parent)
        .unwrap();
    let request = NativeJointHandoffStepRequestV1::from_plan(
        plan,
        StepOperation::TargetInstall {
            donor: donor.clone(),
            target_attempt: attempt,
        },
        business,
        budget_ms,
    )
    .unwrap();
    (request, own)
}
/// One genuine pinned step RPC from `client`. Returns only reply DATA.
fn step(
    client: &TcpClusterRelocationTransport,
    plan: &NativeJointHandoffPlan,
    donor: &ClusterNodeId,
    attempt: Uuid,
    budget_ms: u64,
    ledger: &mut Vec<serde_json::Value>,
    output: &std::path::Path,
) -> Result<NativeJointHandoffStepOutcomeV1> {
    let parent = budget(5000);
    let (request, own) = request(client, plan, donor, attempt, budget_ms, &parent);
    let started = Instant::now();
    let result = client.request_native_joint_handoff_step(&node(4), plan, &request, &own, &parent);
    ledger.push(serde_json::json!({
        "operation":"native_joint_handoff_step","request":request,
        "actual_initiator":client.local_metadata_read_participant().node_id(),
        "elapsed_ms":started.elapsed().as_millis() as u64,
        "outcome":result.as_ref().ok().map(|actual|actual.outcome()),
        "exact_error":result.as_ref().err().map(ToString::to_string),
        "reply_is_progress_DATA_not_authority":true,
    }));
    evidence(&output.join("native-handoff-step-ledger.json"), ledger).unwrap();
    let actual = result?;
    actual.require_request_live()?;
    assert_eq!(actual.reply().node_id, node(4));
    assert_eq!(
        actual.actual_observer(),
        client.local_metadata_read_participant().node_id()
    );
    Ok(actual.outcome().clone())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    target_control: &mut NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    process: &CancellationToken,
) {
    assert!(is_case(case));
    for control in controls.iter() {
        control.bind_initial_image_source_process(process).unwrap();
    }
    let selected = pending.range_by_id(ranges[0].id).unwrap();
    let range = selected.id;
    let relocation = pending
        .relocations
        .values()
        .find(|p| p.range_id == range)
        .unwrap();
    // Request identity only: the committed Pending plan as the fixture's
    // range leader reads it (a range read is bound to that leader). The
    // target never receives it and rebuilds its own from its Role/metadata.
    let leader_index = (0..4).find(|i| node(*i) == selected.leader).unwrap();
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let read = clients[leader_index]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            range,
            selected.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let round = NativeJointOldRound::begin(read, business, relocation.id).unwrap();
    let plan = round.plan();
    drop(round); // Never a local OLD certificate.
    let mut journal = Vec::new();
    // Existing product RPCs: the metadata leader closes every OLD member.
    // No donor source retainer or certificate is created by this test.
    assert!(remote_old_seed(
        SourceCase::TargetSourceExportRetire,
        target,
        controls,
        clients,
        services,
        metadata,
        &plan,
        process,
        output,
        &mut journal,
    )
    .is_none());
    let donor = plan
        .old_members()
        .keys()
        .find(|id| **id != selected.leader && *id != plan.retiring_source())
        .unwrap()
        .clone();
    let donor_index = (0..4).find(|i| node(*i) == donor).unwrap();
    assert!(controls[donor_index]
        .test_source_discovery_state(range)
        .unwrap()
        .is_null());
    let target_before = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert!(target_control
        .test_joint_handoff_step_state(range)
        .unwrap()
        .is_null());
    assert!(target_control.registered_remote_joint_owner(range).is_err());
    let mut ledger = Vec::new();
    let attempt = Uuid::now_v7();

    if matches!(case, SourceCase::NativeHandoffStepRefusals) {
        // (c) A real authenticated member that is neither the range leader
        // nor the metadata leader. Its own client-side checks pass; the
        // target's fresh Role witness for that peer is what refuses.
        let foreign = (1..4)
            .find(|i| node(*i) != selected.leader)
            .expect("a member that is neither range nor metadata leader");
        assert_ne!(node(foreign), node(0));
        let error = step(
            &clients[foreign],
            &plan,
            &donor,
            attempt,
            4000,
            &mut ledger,
            output,
        )
        .err()
        .expect("peer that is neither range leader nor metadata leader must be refused");
        let foreign_error = error.to_string();
        assert!(target_control
            .test_joint_handoff_step_state(range)
            .unwrap()
            .is_null());
        // (d) Exact identity DATA changed after the coordinator built it. The
        // strict client refuses to send these, so the fault sender is used;
        // every recipient check stays real.
        let mut changed_errors = serde_json::Map::new();
        for field in ["range_epoch", "relocation_id", "scope_sha256", "nonce"] {
            let parent = budget(5000);
            let (original, _own) = request(&clients[0], &plan, &donor, attempt, 4000, &parent);
            let mut value = serde_json::to_value(&original).unwrap();
            value[field] = match field {
                "range_epoch" => serde_json::json!(original.range_epoch + 1),
                "relocation_id" => {
                    serde_json::json!(value["relocation_id"].as_u64().unwrap() + 1000)
                }
                "scope_sha256" => serde_json::json!("0".repeat(64)),
                _ => serde_json::json!(Uuid::now_v7()),
            };
            let changed: NativeJointHandoffStepRequestV1 = serde_json::from_value(value).unwrap();
            assert_ne!(changed, original);
            let error = clients[0]
                .test_send_joint_handoff_step_request(&node(4), &changed, &parent)
                .err()
                .unwrap_or_else(|| panic!("changed {field} must be refused"));
            changed_errors.insert(field.into(), error.to_string().into());
            assert!(target_control
                .test_joint_handoff_step_state(range)
                .unwrap()
                .is_null());
        }
        assert!(
            changed_errors["scope_sha256"]
                .as_str()
                .unwrap()
                .contains("differs from actual Pending plan"),
            "{changed_errors:?}"
        );
        assert!(controls[donor_index]
            .test_source_discovery_state(range)
            .unwrap()
            .is_null());
        assert_eq!(
            state(&factory.database, pending, &node(4), ranges).unwrap(),
            target_before,
            "refused steps must not change the target"
        );
        // (e) The first attempt is retained by exactly one bounded sub-step
        // (the budget equals the minimum continuation budget).
        let first = step(
            &clients[0],
            &plan,
            &donor,
            attempt,
            1500,
            &mut ledger,
            output,
        );
        let retained = target_control.test_joint_handoff_step_state(range).unwrap();
        assert_eq!(retained["target_attempt"], attempt.to_string());
        assert!(retained["completed"].is_null());
        if let Ok(outcome) = &first {
            assert!(!outcome.complete);
        }
        let second_attempt = Uuid::now_v7();
        let error = step(
            &clients[0],
            &plan,
            &donor,
            second_attempt,
            4000,
            &mut ledger,
            output,
        )
        .err()
        .expect("second attempt while the first is incomplete must be refused");
        assert!(
            error
                .to_string()
                .contains("retains another exact target install attempt"),
            "{error}"
        );
        let after_refusal = target_control.test_joint_handoff_step_state(range).unwrap();
        assert_eq!(after_refusal, retained);
        // The original attempt is still the one that continues.
        let mut resumed = None;
        for _ in 0..8 {
            if let Ok(outcome) = step(
                &clients[0],
                &plan,
                &donor,
                attempt,
                4000,
                &mut ledger,
                output,
            ) {
                let advanced = outcome.stage > Stage::Discovering;
                resumed = Some(outcome);
                if advanced {
                    break;
                }
            }
        }
        let resumed = resumed.expect("original attempt must continue after the refusal");
        assert!(resumed.stage > Stage::Discovering, "{resumed:?}");
        assert!(resumed.manifest_sha256.is_some());
        let donor_state = controls[donor_index]
            .test_source_discovery_state(range)
            .unwrap();
        assert_eq!(
            donor_state["binding"]["target_attempt"],
            attempt.to_string()
        );
        evidence(
            &output.join("native-handoff-step-refusals.json"),
            &serde_json::json!({
                "foreign_peer":node(foreign),"foreign_peer_error":foreign_error,
                "changed_identity_errors":changed_errors,
                "first_attempt":attempt,"refused_second_attempt":second_attempt,
                "second_attempt_error":error.to_string(),"resumed":resumed,
                "retained_step":target_control.test_joint_handoff_step_state(range).unwrap(),
            }),
        )
        .unwrap();
        return;
    }

    // (a) Only step RPCs from the metadata leader's transport. The first
    // calls carry exactly the minimum continuation budget, so each performs
    // one bounded sub-step and the install is resumed across many requests.
    let mut completed = None;
    let mut errors = Vec::new();
    let mut lost_reply = None;
    let mut stages = Vec::new();
    for call in 0..96 {
        let budget_ms = if call < 8 { 1500 } else { 4000 };
        if call == 1 {
            // The target finishes this call's bounded work and retains it,
            // but the actual reply is never delivered.
            target_control
                .test_drop_next_joint_handoff_step_reply()
                .unwrap();
            let before = target_control.test_joint_handoff_step_state(range).unwrap();
            let lost = step(
                &clients[0],
                &plan,
                &donor,
                attempt,
                budget_ms,
                &mut ledger,
                output,
            );
            let after = target_control.test_joint_handoff_step_state(range).unwrap();
            assert!(lost.is_err(), "suppressed reply must not reach the client");
            lost_reply = Some(serde_json::json!({
                "error":lost.err().map(|e|e.to_string()),"before":before,"after":after,
            }));
            continue;
        }
        match step(
            &clients[0],
            &plan,
            &donor,
            attempt,
            budget_ms,
            &mut ledger,
            output,
        ) {
            Ok(outcome) => {
                stages.push(outcome.stage);
                if outcome.complete {
                    completed = Some(outcome);
                    break;
                }
            }
            Err(error) => {
                errors.push(error.to_string());
                assert!(
                    errors.len() <= 4,
                    "target install steps keep failing: {errors:?}"
                );
            }
        }
    }
    eprintln!(
        "NATIVE_HANDOFF_STEP_PROGRESS {}",
        serde_json::json!({"calls":ledger.len(),"stages":stages,"errors":errors,
            "elapsed_ms":ledger.iter().map(|entry|entry["elapsed_ms"].clone()).collect::<Vec<_>>()})
    );
    let completed = completed.unwrap_or_else(|| {
        panic!("target install did not complete; stages={stages:?} errors={errors:?}")
    });
    assert!(
        stages.windows(2).all(|pair| pair[0] <= pair[1]),
        "{stages:?}"
    );
    assert!(
        ledger.len() >= 4,
        "the install must have been resumed across several step requests"
    );
    let lost_reply = lost_reply.expect("lost reply control ran");
    assert_ne!(
        lost_reply["before"], lost_reply["after"],
        "the call whose reply was lost still retained its progress"
    );
    // The same completed state the in-process sequence reaches: the donor's
    // original published manifest, verified final image, native base,
    // adopted catalogue floor and the retained original joint target owner.
    let donor_state = controls[donor_index]
        .test_source_discovery_state(range)
        .unwrap();
    assert_eq!(
        donor_state["binding"]["target_attempt"],
        attempt.to_string()
    );
    assert_eq!(donor_state["source"]["published"], true);
    assert_eq!(
        donor_state["source"]["manifest"]["checksum_sha256"],
        serde_json::json!(completed.manifest_sha256)
    );
    assert_eq!(
        donor_state["source"]["manifest"]["source_session"],
        serde_json::json!(completed.source_session)
    );
    assert_eq!(completed.stage, Stage::Complete);
    let owner = target_control.registered_remote_joint_owner(range).unwrap();
    let retained_attempt = target_control
        .test_registered_remote_attempt(range)
        .unwrap();
    assert_eq!(retained_attempt["attempt"], attempt.to_string());
    assert_eq!(
        retained_attempt["source"],
        serde_json::json!(completed.manifest_sha256)
    );
    assert_eq!(retained_attempt["lease_present"], false);
    for field in ["proposal", "installing", "catalogue_floor"] {
        assert!(!retained_attempt[field].is_null(), "{retained_attempt}");
    }
    let image = target_control
        .test_registered_remote_image_state(range)
        .unwrap();
    assert_eq!(image["final_image"]["state"], "verified");
    assert!(!image["native_base"].is_null());
    let receipt = target_control
        .registered_remote_final_image_receipt(range)
        .unwrap();
    assert_eq!(
        Some(receipt.manifest().checksum_sha256.clone()),
        completed.manifest_sha256
    );
    let base = owner.original_target_arc().base();
    assert_eq!(
        Some(base.native_receipt_sha256().to_string()),
        completed.native_receipt_sha256
    );
    assert_eq!(Some(base.range_index()), completed.range_index);
    let record = factory
        .database
        .read()
        .get(
            crate::CLUSTER_EVENTS_COLLECTION,
            acknowledged[0]["record_id"].as_str().unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&record).unwrap(),
        acknowledged[0]["actual_full_record"]
    );
    let target_after = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert_eq!(target_after["writer_installed"], false);
    assert_ne!(target_after, target_before);
    // (b) Retries after completion return the identical retained outcome and
    // create neither a second owner nor further target mutation.
    let step_state = target_control.test_joint_handoff_step_state(range).unwrap();
    for _ in 0..2 {
        let again = step(
            &clients[0],
            &plan,
            &donor,
            attempt,
            4000,
            &mut ledger,
            output,
        )
        .unwrap();
        assert_eq!(again, completed);
        assert_eq!(
            serde_json::to_vec(&again).unwrap(),
            serde_json::to_vec(&completed).unwrap()
        );
    }
    assert!(Arc::ptr_eq(
        &owner,
        &target_control.registered_remote_joint_owner(range).unwrap()
    ));
    assert_eq!(
        target_control.test_joint_handoff_step_state(range).unwrap(),
        step_state
    );
    assert_eq!(
        state(&factory.database, pending, &node(4), ranges).unwrap(),
        target_after
    );
    // A different attempt after completion is refused as well.
    let other = step(
        &clients[0],
        &plan,
        &donor,
        Uuid::now_v7(),
        4000,
        &mut ledger,
        output,
    )
    .err()
    .expect("another attempt after completion must be refused");
    assert!(
        other
            .to_string()
            .contains("retains another exact target install attempt"),
        "{other}"
    );
    evidence(
        &output.join("native-handoff-step-result.json"),
        &serde_json::json!({
            "only_step_rpcs_from_metadata_leader":true,"step_calls":ledger.len(),
            "stages":stages,"tolerated_errors":errors,"lost_reply":lost_reply,
            "completed":completed,"donor":donor,"target_attempt":attempt,
            "retained_attempt":retained_attempt,"image":image,
            "joint_target_retained_in_host_not_on_wire":true,
            "serving_granted":false,"cold_recovery_or_physical_qualification":false,
        }),
    )
    .unwrap();
}
