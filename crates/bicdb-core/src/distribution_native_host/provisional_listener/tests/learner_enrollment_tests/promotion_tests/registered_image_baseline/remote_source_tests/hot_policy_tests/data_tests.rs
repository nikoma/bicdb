//! Unchanged native event SDK acceptance after the actual Joined adapter.
//! In-process controls are not independent-host/physical qualification, SQL
//! transparency, automatic controller convergence or capacity evidence.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointDecisionData, NativeJointReleaseContinuation, NativeJointServingPolicyAdmission,
};

pub(super) fn is_data_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetHotData
            | SourceCase::TargetHotDataReplyLost
            | SourceCase::TargetHotDataFirstCallerExpired
            | SourceCase::TargetHotDataFollower
    )
}
#[test]
fn native_registered_fifth_remote_hot_data_original_client_ack_and_full_strong() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetHotData));
}
#[test]
fn native_registered_fifth_remote_hot_data_lost_committed_decision_reply_keeps_same_attempt() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDataReplyLost),
    );
}
#[test]
fn native_registered_fifth_remote_hot_data_first_policy_real_caller_expiry_fresh_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDataFirstCallerExpired),
    );
}
#[test]
fn native_registered_fifth_remote_hot_data_follower_binds_without_fake_leader_ready() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDataFollower),
    );
}
fn fresh_policy(
    continuation: &Arc<NativeJointReleaseContinuation>,
    clients: &[Arc<TcpClusterRelocationTransport>],
    data: &NativeJointDecisionData,
) -> Arc<NativeJointServingPolicyAdmission> {
    let mut round = continuation
        .begin_fresh_release_phase(Duration::from_secs(4))
        .unwrap();
    for node_id in data.new_members().keys() {
        let i = (0..5).find(|i| node(*i) == *node_id).unwrap();
        round.acquire_actual_member(&clients[i], &node(0)).unwrap();
    }
    Arc::new(
        Arc::new(round.finish().unwrap())
            .acquire_local_writer_policy(&clients[4], &node(0))
            .unwrap(),
    )
}
#[allow(clippy::too_many_arguments)]
pub(super) fn install_first_policy(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    range: crate::RangeId,
    admission: Arc<NativeJointServingPolicyAdmission>,
    continuation: &Arc<NativeJointReleaseContinuation>,
    clients: &[Arc<TcpClusterRelocationTransport>],
    data: &NativeJointDecisionData,
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
) {
    if !matches!(case, SourceCase::TargetHotDataFirstCallerExpired) {
        operation(
            output,
            ledger,
            "actual_first_registered_data_policy",
            || control.install_registered_serving_data_policy(range, admission, &budget(5000)),
        )
        .unwrap();
        return;
    }
    let request = budget(5000);
    let deadline = request.deadline().unwrap();
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let marker = fired.clone();
    let cut = BicDb::test_registered_first_policy_after_native_commit(Box::new(move || {
        marker.store(true, std::sync::atomic::Ordering::Release);
        // Real original deadline, not synthetic cancellation, a manual tick,
        // replacement Role, extra budget or native-result/status fabrication.
        while Instant::now() <= deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
    }));
    let started = Instant::now();
    let original = operation(
        output,
        ledger,
        "first_policy_actual_commit_then_original_caller_expiry",
        || control.install_registered_serving_data_policy(range, admission, &request),
    );
    drop(cut);
    let before = control.test_registered_first_policy_evidence();
    evidence(&output.join("registered-hot-first-policy-expiry.json"),&serde_json::json!({
        "actual_native_commit_seam_reached":fired.load(std::sync::atomic::Ordering::Acquire),
        "first_elapsed_us":started.elapsed().as_micros(),"original_parent_budget_ms":5000,
        "original_clock_expired":Instant::now()>=deadline,"original_cancel_requested":request.is_cancel_requested(),
        "original_exact_error":original.as_ref().err().map(ToString::to_string),
        "before_retry":before.as_ref().ok(),"forensic_error":before.as_ref().err().map(ToString::to_string),
        "old_request_renewed":false,"reopened":false})).unwrap();
    assert!(
        fired.load(std::sync::atomic::Ordering::Acquire),
        "original commit prerequisite not reached"
    );
    assert!(original.is_err());
    assert!(!request.is_cancel_requested());
    assert!(request.check().is_err());
    let before = before.unwrap();
    assert!(before["record"]["native_page_xid"].as_u64().unwrap() > 0);
    let fresh = fresh_policy(continuation, clients, data);
    operation(
        output,
        ledger,
        "first_policy_new_request_same_original_native_candidate",
        || control.install_registered_serving_data_policy(range, fresh, &budget(5000)),
    )
    .unwrap();
    let after = control.test_registered_first_policy_evidence().unwrap();
    evidence(
        &output.join("registered-hot-first-policy-expiry-retry.json"),
        &serde_json::json!({
        "before_retry":before,"after_retry":after,"actual_native_commit_seam_reached":true,
        "genuine_new_all_released_phase":true,"old_request_renewed":false}),
    )
    .unwrap();
    assert_eq!(
        before["record"], after["record"],
        "same native xid/checksum/registration/coordinator"
    );
    assert_eq!(after["stage"], "Installed");
    assert_eq!(after["original_finite_caller_retained"], false);
}

fn client_call(
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
    name: &str,
    topology: &ClusterTopology,
    key: &str,
    expected: &serde_json::Value,
    call: impl FnOnce() -> Result<serde_json::Value>,
) -> Result<serde_json::Value> {
    let record_id = crate::cluster_event_record_id("items", key)?;
    let range = topology.range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &record_id)?;
    let index = ledger.len();
    ledger.push(
        serde_json::json!({"sequence":index,"logical_id":Uuid::now_v7(),"operation":name,
        "namespace":"items","key":key,"record_id":record_id,"expected_full_value":expected,
        "expected_payload_sha256":hex::encode(Sha256::digest(serde_json::to_vec(expected)?)),
        "range_id":range.id,"range_epoch":range.epoch,"range_leader":range.leader,
        "topology_generation":topology.generation,"configured_api_timeout_ms":2000,
        "original_prejoin_sdk_object":true,"manual_routing_refresh":false,
        "raw_wire_attempts_recorded":false,"internal_sdk_retry_count":null,
        "requested_consistency":if name.contains("strong") {"Strong"} else {"durable_append"},
        "initial_gateway_node":node(0),"initial_gateway_address":topology.nodes[&node(0)].address,
        "outcome":"not_started","acknowledged":false,"uncertain":false}),
    );
    evidence(
        &output.join("registered-hot-data-external-ledger.json"),
        ledger,
    )?;
    let start = Instant::now();
    let result = call();
    ledger[index]["elapsed_us"] = serde_json::to_value(start.elapsed().as_micros())?;
    ledger[index]["exact_error"] =
        serde_json::to_value(result.as_ref().err().map(ToString::to_string))?;
    ledger[index]["error_debug"] =
        serde_json::to_value(result.as_ref().err().map(|e| format!("{e:?}")))?;
    ledger[index]["actual_result"] = result
        .as_ref()
        .ok()
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let write = name.contains("append");
    // An identical append returning false is still an acknowledged success.
    // The fresh-key assertion below separately requires an actual new insert.
    ledger[index]["acknowledged"] = (write && result.is_ok()).into();
    ledger[index]["uncertain"] = (write && result.is_err()).into();
    ledger[index]["outcome"] = if result.is_ok() {
        "returned"
    } else if write {
        "uncertain_write_effect"
    } else {
        "read_unavailable"
    }
    .into();
    let saved = evidence(
        &output.join("registered-hot-data-external-ledger.json"),
        ledger,
    );
    match result {
        Err(primary) => {
            let _ = saved;
            Err(primary)
        }
        Ok(value) => {
            saved?;
            Ok(value)
        }
    }
}
fn new_key(topology: &ClusterTopology, range: crate::RangeId, label: &str) -> String {
    (0..100_000)
        .map(|i| format!("original-registered-joined-{label}-{i}"))
        .find(|key| {
            topology
                .range_for_key(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", key).unwrap(),
                )
                .unwrap()
                .id
                == range
        })
        .unwrap()
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    original: &crate::ClusterEventClient,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    _clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    topology: &ClusterTopology,
    data: &NativeJointDecisionData,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
) {
    let mut ledger = Vec::new();
    let key = acknowledged[0]["key"].as_str().unwrap();
    let expected = &acknowledged[0]["value"];
    let before = client_call(
        output,
        &mut ledger,
        "strong_before_joined_publication",
        topology,
        key,
        expected,
        || {
            original
                .get_strong("items", key)
                .map(|value| serde_json::to_value(value).unwrap())
        },
    );
    // A valid newer reader may answer; lack of a barrier can only refuse,
    // never translate this acknowledged record into successful absence.
    if let Ok(value) = before {
        assert_eq!(&value, expected, "no false absence before readiness");
    }
    let before_owner = control.test_registered_first_policy_evidence().unwrap();
    let mut phase_ledger = Vec::new();
    operation(
        output,
        &mut phase_ledger,
        "actual_registered_first_custody_to_joined_socket",
        || control.open_registered_joined_data(&budget(5000)),
    )
    .unwrap();
    let adopted = control.test_registered_first_policy_evidence().unwrap();
    assert_eq!(
        before_owner, adopted,
        "exact original attempt/guard/coordinator survived transfer"
    );
    let bound = control.joined_data_status().unwrap();
    evidence(
        &output.join("registered-hot-data-bound.json"),
        &serde_json::json!({
        "actual_status":bound,"before_owner":before_owner,"after_owner":adopted,
        "actual_factory_initial_ranges":factory.spec.initial_ranges,"new_initial_owner":false,
        "leader_ready_synthesized":false}),
    )
    .unwrap();
    assert!(!bound.dispatcher_published);
    operation(
        output,
        &mut phase_ledger,
        "actual_registered_joined_recovery_and_guarded_dispatch",
        || control.publish_joined_data(&budget(5000)),
    )
    .unwrap();
    let published = control.joined_data_status().unwrap();
    if matches!(case, SourceCase::TargetHotDataFollower) {
        assert_ne!(data.activation_range().leader, node(4));
        assert_eq!(
            published.registered_range.as_ref().unwrap()["leader_recovered_prefix_observed"],
            false
        );
    }
    assert!(published.dispatcher_published);
    for acknowledged in acknowledged {
        let key = acknowledged["key"].as_str().unwrap();
        let value = client_call(
            output,
            &mut ledger,
            "strong_original_ack_after_join",
            topology,
            key,
            &acknowledged["value"],
            || {
                original
                    .get_strong("items", key)
                    .map(|value| serde_json::to_value(value).unwrap())
            },
        )
        .unwrap();
        assert_eq!(value, acknowledged["value"]);
    }
    let next_key = new_key(topology, data.old_range().id, "next-ack");
    let next = serde_json::json!({"source":"same-original-client-after-join","nested":{"tenant":17,"rows":[null,true,4,{"v":"full"}]}});
    let appended = client_call(
        output,
        &mut ledger,
        "append_after_join",
        topology,
        &next_key,
        &next,
        || {
            original
                .append("items", &next_key, next.clone())
                .map(serde_json::Value::from)
        },
    );
    if appended.is_err() {
        // Reconcile the uncertain API before its assertion. Visibility is not
        // retroactively an acknowledgement and timeout never means rollback.
        let _ = client_call(
            output,
            &mut ledger,
            "strong_reconcile_uncertain_append",
            topology,
            &next_key,
            &next,
            || {
                original
                    .get_strong("items", &next_key)
                    .map(|value| serde_json::to_value(value).unwrap())
            },
        );
    }
    assert_eq!(appended.unwrap(), true);
    let read = client_call(
        output,
        &mut ledger,
        "strong_new_ack_after_join",
        topology,
        &next_key,
        &next,
        || {
            original
                .get_strong("items", &next_key)
                .map(|value| serde_json::to_value(value).unwrap())
        },
    )
    .unwrap();
    assert_eq!(read, next);
    let record_id = crate::cluster_event_record_id("items", &next_key).unwrap();
    let physical=services.iter().enumerate().map(|(index,service)| {
        let database=service.lock().database();
        let db=database.read();
        let row=db.get(crate::CLUSTER_EVENTS_COLLECTION,&record_id);
        let point=db.native_range_prefix_observation(data.old_range().id);
        serde_json::json!({"node":node(index),"full_record":row.as_ref().ok(),
            "record_error":row.as_ref().err().map(ToString::to_string),"native_watermarks":point.as_ref().ok(),
            "watermark_error":point.as_ref().err().map(ToString::to_string)})
    }).collect::<Vec<_>>();
    evidence(&output.join("registered-hot-data-result.json"),&serde_json::json!({
        "case":format!("{case:?}"),"actual_joined_status":published,"acknowledged_source_operations":acknowledged,
        "new_ack":{"key":next_key,"record_id":record_id,"value":next},"full_strong_value":read,
        "actual_physical_replicas":physical,"external_api_ledger":ledger,
        "ordinary_postgresql_sql_tested":false,"automatic_controller_tested":false,
        "in_process_functional_test":true,"independent_host_failure_tested":false,
        "physical_scale_or_production_qualification":false})).unwrap();
}
