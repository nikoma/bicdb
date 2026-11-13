//! A real expired MQ cannot count even live native close facts. A distinct
//! genuine MQ may count the SAME still-live facts under the identical scope.
use super::*;

#[test]
fn native_registered_fifth_old_round_expiry_then_fresh_mq_counts_same_live_closes() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::LocalOldExpiredRoundContinuation),
    );
}
fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
pub(super) fn observation_data(
    observation: &crate::MetadataReadObservation,
    business: Uuid,
) -> serde_json::Value {
    serde_json::json!({"business":business,"context":observation.context_id(),
        "leader":observation.metadata_leader_id(),"term":observation.term(),
        "read_index":observation.read_index(),"applied_index":observation.applied_index(),
        "observed_unix_ns":now_ns(),"remaining_ns":observation.remaining_budget().map(|d|d.as_nanos())})
}
fn fact_data(
    actual: &crate::distribution_range_handoff::NativeJointOldObservation,
) -> serde_json::Value {
    serde_json::json!({"node":actual.node_id(),"native_close_sha256":actual.receipt_sha256(),
        "scope_sha256":actual.plan().checksum_sha256().unwrap(),"range_index":actual.range_index(),
        "logical_genesis_sha256":actual.logical_genesis_sha256(),"logical_history_sha256":actual.logical_history_sha256()})
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    mut original: NativeJointOldRound,
    plan: Arc<NativeJointHandoffPlan>,
    original_mq: serde_json::Value,
    original_deadline: Instant,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    leader_index: usize,
    output: &std::path::Path,
) {
    let path = output.join("old-round-expiry-continuation.json");
    let origin = Instant::now();
    let scope = plan.checksum_sha256().unwrap();
    let old_indices = plan
        .old_members()
        .keys()
        .map(|id| (0..4).find(|i| node(*i) == *id).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        old_indices.len(),
        3,
        "control must cover ALL actual old voters, without an omitted constraint-only peer"
    );
    let mut journal = serde_json::json!({"original_mq":original_mq,"original_budget_ms":1000,
        "fresh_standard_budget_ms":5000,"scope_sha256":scope,"all_old_nodes":plan.old_members().keys().collect::<Vec<_>>(),
        "old_deadline_from_control_start_ns":original_deadline.saturating_duration_since(origin).as_nanos(),
        "control_origin_unix_ns":now_ns(),"no_caller_cancellation_claim":true,"component_only":true,"phase":"produce_all_actual_closes","closes":[],
        "no_partial_debt_or_unclosable_peer_in_this_control":true,"no_image_install_or_serving_claim":true});
    evidence(&path, &journal).unwrap();
    let mut actuals = Vec::new();
    let mut roles = Vec::new();
    let mut runtimes = Vec::new();
    let mut evidence_errors = Vec::new();
    for &i in &old_indices {
        journal["phase"] = "acquire_fresh_original_close_role".into();
        journal["current_node"] = serde_json::json!(node(i));
        journal["role_acquisition_intent_unix_ns"] = serde_json::json!(now_ns());
        evidence(&path, &journal).unwrap();
        let operation = budget(5000);
        let provider = MetadataReadProvider::new(metadata[i].clone(), clients[i].clone()).unwrap();
        let role = Arc::new(actual_role(&provider, actor, &plan, &node(i), &operation));
        journal["phase"] = "acquire_fresh_original_close_own".into();
        journal["current_role"] = serde_json::json!({"context":role.context_id(),"business":role.business_request_id(),
            "challenge":role.recipient_challenge(),"term":role.metadata_term(),"index":role.metadata_read_index(),
            "deadline_from_control_start_ns":role.deadline().saturating_duration_since(origin).as_nanos(),"observed_unix_ns":now_ns()});
        evidence(&path, &journal).unwrap();
        let business = Uuid::now_v7();
        let own = clients[i]
            .acquire_host_metadata_observation(
                &node(0),
                business,
                Duration::from_secs(5),
                &operation,
            )
            .unwrap();
        let runtime = Arc::new(NativeJointHandoffRuntime::new(services[i].clone()).unwrap());
        let mut row = serde_json::json!({"node":node(i),"phase":"close_intent","intent_unix_ns":now_ns(),
            "original_operation_budget_ms":5000,"role_context":role.context_id(),"role_business":role.business_request_id(),
            "role_challenge":role.recipient_challenge(),"role_term":role.metadata_term(),"role_index":role.metadata_read_index(),
            "role_deadline_from_control_start_ns":role.deadline().saturating_duration_since(origin).as_nanos(),
            "own_context":own.context_id(),"own_business":business,"own_term":own.term(),"own_index":own.read_index(),
            "own_deadline_from_control_start_ns":own.deadline().saturating_duration_since(origin).as_nanos()});
        journal["closes"].as_array_mut().unwrap().push(row.clone());
        evidence(&path, &journal).unwrap();
        let result = controls[i].close_initial_old_step(
            runtime.clone(),
            role.clone(),
            own,
            business,
            &operation,
        );
        row["phase"] = "close_reply".into();
        row["reply_unix_ns"] = serde_json::json!(now_ns());
        row["actual_error"] =
            serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
        row["actual_close"] = result
            .as_ref()
            .ok()
            .map(|a| fact_data(a))
            .unwrap_or(serde_json::Value::Null);
        *journal["closes"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap() = row;
        evidence_after(&path, &journal, &mut evidence_errors);
        let actual = result.unwrap();
        assert_eq!(actual.plan().checksum_sha256().unwrap(), scope);
        actuals.push(actual);
        roles.push(role);
        runtimes.push(runtime);
    }
    assert_eq!(actuals.len(), plan.old_members().len());
    // A real observed deadline, not a fabricated stale status or cancellation.
    while Instant::now() < original_deadline {
        std::thread::sleep(
            Duration::from_millis(2)
                .min(original_deadline.saturating_duration_since(Instant::now())),
        );
    }
    journal["phase"] = "original_round_must_reject".into();
    journal["after_real_deadline_unix_ns"] = serde_json::json!(now_ns());
    evidence(&path, &journal).unwrap();
    let refused = original.record_retained_closed_actual(actuals[0].clone());
    journal["original_record_error"] =
        serde_json::to_value(refused.as_ref().err().map(ToString::to_string)).unwrap();
    evidence_after(&path, &journal, &mut evidence_errors);
    let error = refused.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata observation request changed or expired"),
        "{error}"
    );
    drop(original);
    // Preconditions remain real and scoped. Expired close facts cannot be
    // retagged or renewed merely because another MQ succeeds.
    let live_checks=actuals.iter().zip(&roles).map(|(actual,role)| {
        serde_json::json!({"actual_close":fact_data(actual),"checked_unix_ns":now_ns(),
            "role_error":role.require_live_request().err().map(|e|e.to_string()),
            "plan_error":actual.plan().require_live().err().map(|e|e.to_string()),
            "native_error":actual.require_current().err().map(|e|e.to_string()),
            "role_deadline_from_control_start_ns":role.deadline().saturating_duration_since(origin).as_nanos()})
    }).collect::<Vec<_>>();
    journal["retained_fact_live_checks"] = serde_json::json!(live_checks);
    evidence(&path, &journal).unwrap();
    assert!(live_checks.iter().all(|c|c["role_error"].is_null() && c["plan_error"].is_null() && c["native_error"].is_null()),
        "retained fact no longer scoped/current; actual native reread producer required, no DTO renewal: {live_checks:?}");
    let before = old_indices
        .iter()
        .map(|&i| {
            let database = services[i].lock().database();
            state(&database, pending, &node(i), ranges).unwrap()
        })
        .collect::<Vec<_>>();
    journal["after_all_closes_before_fresh_count"] = serde_json::to_value(&before).unwrap();
    journal["phase"] = "acquire_distinct_genuine_fresh_mq".into();
    evidence(&path, &journal).unwrap();
    let fresh_parent = budget(5000);
    let business = Uuid::now_v7();
    let observed = clients[leader_index]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            Duration::from_secs(5),
            &fresh_parent,
        )
        .unwrap();
    let mut fresh_data = observation_data(&observed, business);
    fresh_data["deadline_from_control_start_ns"] = serde_json::json!(observed
        .deadline()
        .saturating_duration_since(origin)
        .as_nanos());
    assert_ne!(fresh_data["context"], journal["original_mq"]["context"]);
    assert_ne!(fresh_data["business"], journal["original_mq"]["business"]);
    journal["fresh_mq"] = fresh_data;
    let mut fresh =
        NativeJointOldRound::begin(observed, business, plan.promise().relocation_id).unwrap();
    assert_eq!(fresh.plan().checksum_sha256().unwrap(), scope);
    journal["phase"] = "count_same_still_live_actual_close_arcs".into();
    evidence(&path, &journal).unwrap();
    journal["fresh_count_attempts"] = serde_json::json!([]);
    for (actual, role) in actuals.iter().zip(&roles) {
        let live = role
            .require_live_request()
            .and_then(|_| actual.plan().require_live())
            .and_then(|_| actual.require_current());
        journal["current_retained_close"] = fact_data(actual);
        journal["current_retained_fact_error"] =
            serde_json::to_value(live.as_ref().err().map(ToString::to_string)).unwrap();
        evidence_after(&path, &journal, &mut evidence_errors);
        live.expect(
            "expired/changed actual close requires actual native reread, never DTO renewal",
        );
        let counted = fresh.record_retained_closed_actual(actual.clone());
        journal["fresh_count_attempts"].as_array_mut().unwrap().push(serde_json::json!({
            "actual_close":fact_data(actual),"observed_unix_ns":now_ns(),
            "actual_error":counted.as_ref().err().map(ToString::to_string),"same_actual_arc_passed":true}));
        evidence_after(&path, &journal, &mut evidence_errors);
        counted.unwrap();
    }
    let finished = fresh.finish();
    journal["fresh_finish_error"] =
        serde_json::to_value(finished.as_ref().err().map(ToString::to_string)).unwrap();
    evidence_after(&path, &journal, &mut evidence_errors);
    let completed = finished.unwrap();
    assert_eq!(completed.plan().checksum_sha256().unwrap(), scope);
    for actual in &actuals {
        assert_eq!(completed.range_index(), actual.range_index());
        assert_eq!(
            completed.logical_genesis_sha256(),
            actual.logical_genesis_sha256()
        );
        assert_eq!(
            completed.logical_history_sha256(),
            actual.logical_history_sha256()
        );
    }
    let after = old_indices
        .iter()
        .map(|&i| {
            let database = services[i].lock().database();
            state(&database, pending, &node(i), ranges).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        after, before,
        "fresh counting must not publish a second close or change actual native/catalogue/rows"
    );
    journal["phase"] = "complete_same_actual_close_facts_counted_under_distinct_mq".into();
    journal["after_fresh_count"] = serde_json::to_value(after).unwrap();
    journal["same_actual_close_facts"] =
        serde_json::to_value(actuals.iter().map(|a| fact_data(a)).collect::<Vec<_>>()).unwrap();
    journal["evidence_errors"] = serde_json::json!(evidence_errors);
    evidence(&path, &journal).unwrap();
    std::hint::black_box((&actuals, &roles, &runtimes, &completed));
}
