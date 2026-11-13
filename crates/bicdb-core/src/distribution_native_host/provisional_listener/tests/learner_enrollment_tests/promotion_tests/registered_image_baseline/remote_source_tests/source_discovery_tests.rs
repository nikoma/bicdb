//! The donor session is created by the actual TCP recipient; this test never
//! constructs or hands a local source retainer/manifest to the client.
use super::*;
use crate::distribution_range_image::{
    NativeSourceDiscoveryOperationV1 as DiscoveryOp, NativeSourceDiscoveryRequestV1,
};

#[test]
fn native_registered_fifth_source_discovery_actual_tcp_start_describe_and_frames() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::SourceDiscovery));
}
#[test]
fn native_registered_fifth_source_discovery_actual_tcp_lost_reply_same_session() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::SourceDiscoveryReplyLost),
    );
}
#[test]
fn native_registered_fifth_source_discovery_actual_tcp_wrong_actor_before_capture() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::SourceDiscoveryWrongActor),
    );
}
#[test]
fn native_registered_fifth_source_discovery_actual_tcp_exact_request_conflict_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::SourceDiscoveryRequestConflict),
    );
}
#[test]
fn native_registered_fifth_source_discovery_actual_tcp_current_plan_revocation_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::SourceDiscoveryPlanRevoked),
    );
}

#[test]
fn native_registered_fifth_source_discovery_actual_tcp_original_process_revocation_before_capture()
{
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::SourceDiscoveryProcessRevoked),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    plan: &Arc<NativeJointHandoffPlan>,
    source: usize,
    target: &TcpClusterRelocationTransport,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
    native_before: &std::collections::BTreeMap<String, serde_json::Value>,
) {
    let attempt = Uuid::now_v7();
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let own = target
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let request = NativeSourceDiscoveryRequestV1::from_plan(
        plan,
        node(source),
        node(4),
        attempt,
        DiscoveryOp::Start,
        &own,
        business,
        4000,
    )
    .unwrap();
    let before = controls[source]
        .test_source_discovery_state(plan.old_range().id)
        .unwrap();
    assert!(before.is_null());
    let mut negative = serde_json::Value::Null;
    if matches!(case, SourceCase::SourceDiscoveryProcessRevoked) {
        controls[source]
            .test_cancel_source_discovery_original_process()
            .unwrap();
        journal.push(serde_json::json!({"operation":"native_source_discovery_revoked_process", "request":request,"outcome":"not_started"}));
        let index = journal.len() - 1;
        evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
        let result = target.test_send_source_discovery_request(&request, &parent);
        journal[index]["outcome"] = if result.is_err() {
            "refused"
        } else {
            "unexpected_reply"
        }
        .into();
        journal[index]["error"] =
            serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
        evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
        let error = result
            .err()
            .expect("actual bound process parent was canceled before capture");
        assert!(
            error.to_string().to_ascii_lowercase().contains("cancel"),
            "{error}"
        );
        assert!(controls[source]
            .test_source_discovery_state(plan.old_range().id)
            .unwrap()
            .is_null());
        evidence(&output.join("source-discovery-process-revocation.json"),&serde_json::json!({"request":request,
            "error":error.to_string(),"actual_original_process_parent_canceled":true,"capture_not_started":true,
            "physical_process_kill_or_power_loss":false})).unwrap();
        let after = native_records(services, plan);
        assert_eq!(
            &after, native_before,
            "revoked discovery cannot mutate actual OLD receipts"
        );
        evidence(
            &output.join("source-discovery-process-revocation-native.json"),
            &serde_json::json!({"before":native_before,"after":after}),
        )
        .unwrap();
        return;
    }

    if matches!(case, SourceCase::SourceDiscoveryWrongActor) {
        let error = clients[0]
            .test_send_source_discovery_request(&request, &parent)
            .err()
            .expect("actual metadata-leader peer is not the target");
        assert!(
            error
                .to_string()
                .contains("actual donor/target identity differs"),
            "{error}"
        );
        assert!(controls[source]
            .test_source_discovery_state(plan.old_range().id)
            .unwrap()
            .is_null());
        negative = serde_json::json!({"actual_wrong_actor":node(0),"error":error.to_string(),"no_candidate_created":true});
    }
    if matches!(case, SourceCase::SourceDiscoveryReplyLost) {
        controls[source]
            .test_drop_next_source_discovery_reply()
            .unwrap();
    }
    journal.push(serde_json::json!({"operation":"native_source_discovery_start","request":request,"outcome":"not_started"}));
    let entry = journal.len() - 1;
    evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
    let first = target.request_remote_image_source_discovery(plan, &request, &own, &parent);
    journal[entry]["outcome"] = if first.is_ok() {
        "returned_actual_manifest"
    } else {
        "error_unknown_effect"
    }
    .into();
    journal[entry]["error"] =
        serde_json::to_value(first.as_ref().err().map(ToString::to_string)).unwrap();
    if let Ok(actual) = &first {
        journal[entry]["actual_reply"] = serde_json::to_value(actual.reply()).unwrap();
    }
    evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
    let retained = controls[source]
        .test_source_discovery_state(plan.old_range().id)
        .unwrap();
    assert_eq!(retained["source"]["capture_attempted"], true);
    assert_eq!(retained["source"]["capture_retained"], true);
    assert_eq!(retained["source"]["published"], true);
    assert_eq!(retained["binding"]["target_attempt"], attempt.to_string());
    if matches!(case, SourceCase::SourceDiscoveryReplyLost) {
        assert!(
            first.is_err(),
            "actual server must close without issuing its completed reply"
        );
    } else {
        first.as_ref().unwrap().require_request_live().unwrap();
    }
    if matches!(case, SourceCase::SourceDiscoveryRequestConflict) {
        let mut changed = request.clone();
        changed.budget_ms -= 1;
        let p = budget(5000);
        let fresh = target
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &p)
            .unwrap();
        let error = target
            .request_remote_image_source_discovery(plan, &changed, &fresh, &p)
            .err()
            .expect("same challenge changed exact request bytes");
        assert!(
            error.to_string().contains("different exact request bytes"),
            "{error}"
        );
        negative = serde_json::json!({"changed_request":changed,"error":error.to_string()});
    }
    if matches!(case, SourceCase::SourceDiscoveryPlanRevoked) {
        let mut next = metadata[0].lock().committed_topology().clone();
        next.generation += 1;
        next.relocations
            .get_mut(&plan.promise().relocation_id)
            .unwrap()
            .plan_id
            .push_str("-revoked");
        next.validate().unwrap();
        let entry = metadata[0].lock().propose_topology(next).unwrap();
        let p = budget(5000);
        controls[0].heartbeat(&p).unwrap();
        controls[0].heartbeat(&p).unwrap();
        for actual in metadata {
            assert!(actual.lock().status().commit_index >= entry.index);
        }
        let p = budget(5000);
        let b = Uuid::now_v7();
        let fresh = target
            .acquire_host_metadata_observation(&node(0), b, Duration::from_secs(5), &p)
            .unwrap();
        let mut describe = request.clone();
        describe.operation = DiscoveryOp::Describe;
        describe.business = b;
        describe.challenge = Uuid::now_v7();
        // Bypass only client DATA prechecks to exercise the actual receiver's
        // own current MStore/plan refusal. This creates no typed source fact.
        let error = target
            .test_send_source_discovery_request(&describe, &p)
            .err()
            .expect("actual donor must refuse changed committed plan");
        fresh.require_live_request(b).unwrap();
        assert!(
            error.to_string().contains("immutable Pending image plan"),
            "{error}"
        );
        assert_eq!(
            controls[source]
                .test_source_discovery_state(plan.old_range().id)
                .unwrap(),
            retained
        );
        negative = serde_json::json!({"actual_committed_index":entry.index,"error":error.to_string(),"retained_candidate_unchanged":true});
    } else {
        let p = budget(5000);
        let b = Uuid::now_v7();
        let fresh = target
            .acquire_host_metadata_observation(&node(0), b, Duration::from_secs(5), &p)
            .unwrap();
        let describe = NativeSourceDiscoveryRequestV1::from_plan(
            plan,
            node(source),
            node(4),
            attempt,
            DiscoveryOp::Describe,
            &fresh,
            b,
            4000,
        )
        .unwrap();
        journal.push(serde_json::json!({"operation":"native_source_discovery_describe", "request":describe,"outcome":"not_started"}));
        let describe_index = journal.len() - 1;
        evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
        let result = target.request_remote_image_source_discovery(plan, &describe, &fresh, &p);
        journal[describe_index]["outcome"] = if result.is_ok() {
            "returned_actual_manifest"
        } else {
            "error_unknown_effect"
        }
        .into();
        journal[describe_index]["error"] =
            serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
        if let Ok(actual) = &result {
            journal[describe_index]["actual_reply"] = serde_json::to_value(actual.reply()).unwrap();
        }
        evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
        let actual = result.unwrap();
        let manifest = actual.reply().manifest.clone();
        assert_eq!(
            serde_json::to_value(&manifest).unwrap(),
            retained["source"]["manifest"]
        );
        let source = NativeRemoteImageFact::from_actual_discovery(actual).unwrap();
        assert_eq!(source.manifest().target_attempt, attempt);
        let returned = frame(
            target,
            &manifest,
            Op::History {
                cursor: RangeImageHistoryCursorV1::default(),
                max_entries: 64,
            },
            output,
            journal,
        )
        .unwrap();
        let bound = source.bind_step(returned).unwrap();
        bound.require_request_live().unwrap();
        journal.push(serde_json::json!({"operation":"native_source_discovery_describe","request":describe,"manifest":manifest,"same_retained_session":true}));
    }
    let native_after = native_records(services, plan);
    evidence(&output.join(format!("source-discovery-{}.json",plan.old_range().id.get())),&serde_json::json!({
        "case":format!("{case:?}"),"request":request,"retained_before":before,"retained_after_start":retained,
        "negative":negative,"native_old_before":native_before,"native_old_after":native_after,
        "actual_tls_discovery":true,"operator_controller_or_serving_proved":false
    })).unwrap();
    assert_eq!(
        &native_after, native_before,
        "discovery cannot re-close OLD with a new native xid"
    );
    evidence(&output.join("source-discovery-ledger.json"), journal).unwrap();
}

fn native_records(
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    plan: &NativeJointHandoffPlan,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    plan.old_members()
        .keys()
        .map(|recipient| {
            let index = (0..4).find(|i| node(*i) == *recipient).unwrap();
            let db = services[index].lock().database();
            let record = db
                .read()
                .test_native_old_close_record(plan.old_range().id)
                .unwrap()
                .unwrap();
            (recipient.to_string(), record)
        })
        .collect()
}
