//! Genuine constraints-first/Close continuation, using real original RPCs.
//! INITIAL/born profile only; no simulated Serving/Joined predecessor.
use super::*;
use crate::distribution_range_handoff::{
    NativeRemoteOldOperationV1 as OldOp, NativeRemoteOldReplyV1, NativeRemoteOldRequestV1,
};

pub(super) fn constrain(
    index: usize,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    plan: &Arc<NativeJointHandoffPlan>,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) -> serde_json::Value {
    let recipient = node(index);
    let db = services[index].lock().database();
    let native_before = db
        .read()
        .test_native_old_close_record(plan.old_range().id)
        .unwrap();
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let own = clients[0]
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let request =
        NativeRemoteOldRequestV1::from_plan(plan, OldOp::Constrain, business, 4000).unwrap();
    journal.push(serde_json::json!({"operation":"constraint_first_real_RPC","request":request,"recipient":recipient,"outcome":"not_started"}));
    let at = journal.len() - 1;
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
    let result = clients[0].request_remote_native_old(&recipient, plan, &request, &own, &parent);
    journal[at]["actual_reply"] =
        serde_json::to_value(result.as_ref().ok().map(|f| f.reply())).unwrap();
    journal[at]["exact_error"] =
        serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
    let custody = controls[index].test_constraint_close_custody(plan.old_range().id);
    let native_after = db.read().test_native_old_close_record(plan.old_range().id);
    let observation = serde_json::json!({"operation":"constraint_then_close_custody_boundary","phase":"constraint_only_before_Close","range_id":plan.old_range().id,"recipient":recipient,"request":request,
        "actual_reply":result.as_ref().ok().map(|f|f.reply()),"error":result.as_ref().err().map(ToString::to_string),
        "custody":custody.as_ref().ok(),"custody_error":custody.as_ref().err().map(ToString::to_string),
        "native_before":native_before,"native_after":native_after.as_ref().ok(),"native_error":native_after.as_ref().err().map(ToString::to_string),
        "scope":"actual INITIAL born constraints-first; not Joined/Serving qualification"});
    evidence(
        &output.join(format!(
            "constraint-first-r{}-{index}-before-assertions.json",
            plan.old_range().id.get()
        )),
        &observation,
    )
    .unwrap();
    journal.push(observation.clone());
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
    let frame = result.unwrap();
    frame.require_request_live().unwrap();
    assert!(frame.reply().closed.is_none());
    assert!(frame.reply().closed_native_record.is_none());
    assert!(native_before.is_none());
    assert!(native_after.unwrap().is_none());
    let custody = custody.unwrap();
    assert_eq!(custody["actual_close_present"], false);
    assert_eq!(custody["outcome_present"], false);
    assert_eq!(custody["predecessor"]["profile"], "actual-preclose-born");
    assert!(custody["constraint_owner_allocation_diagnostic_only"].is_string());
    assert_eq!(
        custody["constraints"],
        serde_json::to_value(&frame.reply().constraints).unwrap()
    );
    parent.check().unwrap();
    own.require_live_request(business).unwrap();
    observation
}

#[allow(clippy::too_many_arguments)]
pub(super) fn after_close(
    index: usize,
    before: &serde_json::Value,
    actual: &NativeRemoteOldReplyV1,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    plan: &Arc<NativeJointHandoffPlan>,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) {
    let recipient = node(index);
    let db = services[index].lock().database();
    let native = db
        .read()
        .test_native_old_close_record(plan.old_range().id)
        .unwrap();
    let custody = controls[index].test_constraint_close_custody(plan.old_range().id);
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let own = clients[0]
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let request = NativeRemoteOldRequestV1::from_plan(plan, OldOp::Close, business, 4000).unwrap();
    journal.push(serde_json::json!({"operation":"constraint_first_real_Close_replay","recipient":recipient,"request":request,"outcome":"not_started"}));
    let at = journal.len() - 1;
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
    let replay = clients[0].request_remote_native_old(&recipient, plan, &request, &own, &parent);
    journal[at]["actual_reply"] =
        serde_json::to_value(replay.as_ref().ok().map(|f| f.reply())).unwrap();
    journal[at]["exact_error"] =
        serde_json::to_value(replay.as_ref().err().map(ToString::to_string)).unwrap();
    let native_after = db.read().test_native_old_close_record(plan.old_range().id);
    let custody_after = controls[index].test_constraint_close_custody(plan.old_range().id);
    let report = serde_json::json!({"operation":"constraint_then_close_custody_boundary","phase":"real_Close_then_fresh_same_Close_replay","range_id":plan.old_range().id,"before_constraints":before,"actual_Close_reply":actual,
        "custody":custody.as_ref().ok(),"custody_error":custody.as_ref().err().map(ToString::to_string),"native":native,
        "replay_request":request,"replay_reply":replay.as_ref().ok().map(|f|f.reply()),"replay_error":replay.as_ref().err().map(ToString::to_string),
        "native_after_replay":native_after.as_ref().ok(),"native_after_error":native_after.as_ref().err().map(ToString::to_string),
        "custody_after_replay":custody_after.as_ref().ok(),"custody_after_error":custody_after.as_ref().err().map(ToString::to_string),
        "same_original_runtime_Frozen_predecessor_constraint_owner_required":true,"no_Joined_or_Serving_claim":true});
    evidence(
        &output.join(format!(
            "constraint-first-r{}-{index}-Close-before-assertions.json",
            plan.old_range().id.get()
        )),
        &report,
    )
    .unwrap();
    journal.push(report.clone());
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
    let custody = custody.unwrap();
    for key in [
        "scope_sha256",
        "runtime_allocation_diagnostic_only",
        "predecessor_allocation_diagnostic_only",
        "constraint_owner_allocation_diagnostic_only",
        "predecessor",
        "constraints",
        "constraint_frozen",
    ] {
        assert_eq!(
            before["custody"][key], custody[key],
            "original custody changed {key}"
        );
    }
    assert_eq!(
        custody["constraint_frozen"],
        custody["actual_closed_frozen"]
    );
    assert_eq!(custody["actual_close_present"], true);
    assert_eq!(
        serde_json::to_value(&actual.constraints).unwrap(),
        before["actual_reply"]["constraints"]
    );
    assert!(actual.closed.is_some());
    assert!(actual.closed_native_record.is_some());
    assert!(native.is_some());
    let replay = replay.unwrap();
    replay.require_request_live().unwrap();
    assert_eq!(
        actual.closed_native_record,
        replay.reply().closed_native_record
    );
    assert_eq!(native, native_after.unwrap());
    assert_eq!(custody, custody_after.unwrap());
    parent.check().unwrap();
    own.require_live_request(business).unwrap();
}
