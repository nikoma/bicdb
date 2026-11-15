//! Known LOCAL MStore staleness controls, not a partition/new-leader proof.
//! Every mutation is the actual existing term-observation API and durable
//! state file. No cancellation, field assignment or fresh quorum is invented.
use super::*;

pub(super) fn is_decision_control(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetHotDecisionTermBeforePut | SourceCase::TargetHotDecisionTermAfterCommit
    )
}
#[test]
fn native_registered_fifth_remote_hot_local_term_before_decision_first_put_refuses_vote() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionTermBeforePut),
    );
}
#[test]
fn native_registered_fifth_remote_hot_local_term_after_decision_commit_retains_xid_refuses_vote() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionTermAfterCommit),
    );
}
#[test]
fn native_registered_fifth_remote_hot_local_term_after_policy_readback_refuses_hook() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotFirstPolicyTermBeforeHook),
    );
}
type Trace = Arc<Mutex<serde_json::Value>>;

pub(super) fn snapshot(
    control: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    topology: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
) -> serde_json::Value {
    // No metadata/network/engine guard is retained across these independent
    // diagnostic reads. Errors stay evidence; they never turn into empty data.
    let database = state(&factory.database, topology, &node(4), ranges);
    let hook = factory
        .database
        .try_read()
        .ok_or_else(|| BicDbError::Cluster("test actual DB busy for hook snapshot".into()))
        .and_then(|db| db.test_actual_registered_hook_evidence());
    let policy = control.test_registered_first_policy_evidence();
    let joined = control.joined_data_status();
    serde_json::json!({"database":database.as_ref().ok(),
        "database_error":database.as_ref().err().map(ToString::to_string),
        "actual_hook":hook.as_ref().ok(),"hook_evidence_error":hook.as_ref().err().map(ToString::to_string),
        "first_policy_attempt":policy.as_ref().ok(),
        "first_policy_evidence_error":policy.as_ref().err().map(ToString::to_string),
        "joined_data_status":joined,
        "observation_is_not_permission":true})
}
fn store_result(value: &Trace, path: &std::path::Path, mut result: serde_json::Value) {
    {
        let mut actual = value.lock();
        let mut events = actual["events"].as_array().cloned().unwrap_or_default();
        let mut event = result.clone();
        event.as_object_mut().unwrap().remove("events");
        event.as_object_mut().unwrap().remove("evidence_errors");
        events.push(event);
        result["events"] = events.into();
        result["evidence_errors"] = actual["evidence_errors"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into();
        *actual = result.clone();
    }
    if let Err(error) = evidence(path, &result) {
        value.lock()["evidence_errors"].as_array_mut().unwrap().push(
            serde_json::json!({"path":path,"exact_error":error.to_string(),"phase":result["phase"]}));
    }
}
/// Actor begins only when the actual native seam fires. Its mutation never
/// calls the service/DB/native journal, and the callback is outside the final
/// version guard. The actor's parent is the SAME original finite call; no
/// additional timeout or caller cancellation grants more time.
pub(super) fn mutation(
    metadata: Arc<Mutex<crate::MetadataConsensusStore>>,
    parent: CancellationToken,
    output: &std::path::Path,
) -> (Box<dyn FnOnce()>, Trace) {
    let trace = Arc::new(Mutex::new(
        serde_json::json!({"seam_reached":false,"mutation_proven":false}),
    ));
    let observed = trace.clone();
    let path = output.join("registered-hot-local-term-mutation.json");
    let callback = Box::new(move || {
        let started = Instant::now();
        store_result(
            &observed,
            &path,
            serde_json::json!({"seam_reached":true,
            "mutation_proven":false,"actor":"independent_thread","phase":"before_actor_start",
            "original_parent_remaining_us":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros())}),
        );
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let actual = metadata.clone();
        let actor_parent = parent.clone();
        let actor_trace = observed.clone();
        let actor_path = path.clone();
        let actor=std::thread::Builder::new().name("actual-hot-local-term".into()).spawn(move || {
            let attempt=(|| -> Result<serde_json::Value> {
                let mut store=loop {
                    actor_parent.check()?;
                    if let Some(store)=actual.try_lock_for(Duration::from_millis(5)) { break store; }
                };
                let before=store.test_actual_publication_version_evidence()?;
                let previous=store.status().current_term;
                let requested=previous.checked_add(1).ok_or_else(||BicDbError::Cluster("actual test term overflow".into()))?;
                let intent=serde_json::json!({"seam_reached":true,"phase":"actual_mutation_intent",
                    "mutation_proven":false,"api":"MetadataConsensusStore.observe_remote_term",
                    "before":before,"requested_term":requested});
                // Intent is retained before invoking the actual durable mutator.
                store_result(&actor_trace,&actor_path,intent);
                let result=store.observe_remote_term(requested);
                let after=store.test_actual_publication_version_evidence();
                let proven=result.as_ref().is_ok_and(|changed|*changed)
                    && after.as_ref().is_ok_and(|a|a["durable_state_term"]==requested
                        && a["status"]["current_term"]==requested
                        && a["version"]["actual_open"]==before["version"]["actual_open"]
                        && a["version"]["generation"].as_u64().zip(before["version"]["generation"].as_u64()).is_some_and(|(new,old)|new>old)
                        && a["version"]["ready"]==true && a["version"]["failed"]==false
                        && a["durable_state_checksum_valid"]==true);
                Ok(serde_json::json!({"seam_reached":true,"phase":"actual_mutation_returned",
                    "api":"MetadataConsensusStore.observe_remote_term","before":before,
                    "requested_term":requested,"actual_changed":result.as_ref().ok(),
                    "actual_error":result.as_ref().err().map(ToString::to_string),
                    "actual_error_debug":result.as_ref().err().map(|e|format!("{e:?}")),
                    "after":after.as_ref().ok(),"after_evidence_error":after.as_ref().err().map(ToString::to_string),
                    "original_parent_live_after_mutation":actor_parent.check().is_ok(),
                    "mutation_proven":proven,"no_fabricated_new_leader":true}))
            })();
            let report=match attempt {
                Ok(report)=>report,
                Err(error)=>serde_json::json!({"seam_reached":true,"phase":"actor_prerequisite_failed",
                    "exact_error":error.to_string(),"error_debug":format!("{error:?}"),"mutation_proven":false}),
            };
            store_result(&actor_trace,&actor_path,report);
            let _=tx.send(());
        });
        let actor = match actor {
            Ok(actor) => actor,
            Err(error) => {
                store_result(
                    &observed,
                    &path,
                    serde_json::json!({"seam_reached":true,
                    "phase":"actor_spawn_failed","exact_error":error.to_string(),"mutation_proven":false}),
                );
                return;
            }
        };
        loop {
            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(()) => {
                    let join = actor.join();
                    let mut report = observed.lock().clone();
                    report["actor_joined"] = join.is_ok().into();
                    report["callback_elapsed_us"] =
                        serde_json::to_value(started.elapsed().as_micros()).unwrap();
                    store_result(&observed, &path, report);
                    return;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    let mut report = observed.lock().clone();
                    report["actor_disconnected"] = true.into();
                    store_result(&observed, &path, report);
                    return;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = parent.check() {
                        let mut report = observed.lock().clone();
                        report["callback_parent_error"] = error.to_string().into();
                        report["callback_elapsed_us"] =
                            serde_json::to_value(started.elapsed().as_micros()).unwrap();
                        store_result(&observed, &path, report);
                        // A possibly still-running IO actor is not called success.
                        // Its exact original Arc/finite request remains retained.
                        return;
                    }
                }
            }
        }
    });
    (callback, trace)
}
fn require_mutation(report: &serde_json::Value, error: &BicDbError) {
    assert_eq!(
        report["seam_reached"], true,
        "actual native boundary prerequisite"
    );
    assert_eq!(
        report["actor_joined"], true,
        "actual mutation actor completion prerequisite"
    );
    assert_eq!(
        report["actual_changed"], true,
        "actual term mutation must have succeeded"
    );
    assert!(report["actual_error"].is_null());
    assert_eq!(
        report["original_parent_live_after_mutation"], true,
        "no widened or expired parent"
    );
    let old = &report["before"];
    let new = &report["after"];
    assert_eq!(new["status"]["current_term"], report["requested_term"]);
    assert_eq!(
        new["durable_state_term"], report["requested_term"],
        "actual persisted term"
    );
    assert_eq!(old["version"]["actual_open"], new["version"]["actual_open"]);
    assert!(
        new["version"]["generation"].as_u64().unwrap()
            > old["version"]["generation"].as_u64().unwrap()
    );
    assert_eq!(
        new["version"]["failed"], false,
        "this control is a successful term persist, not poison"
    );
    assert_eq!(new["version"]["ready"], true);
    assert_eq!(
        new["version"]["durable_generation"],
        new["version"]["generation"]
    );
    assert!(report["evidence_errors"].as_array().unwrap().is_empty());
    assert_eq!(report["mutation_proven"], true);
    assert_eq!(new["durable_state_checksum_valid"], true);
    // A timeout/cancellation may also refuse, but would not prove the intended
    // known-stale version boundary. Preserve it and fail this narrower gate.
    assert!(
        error
            .to_string()
            .contains("actual metadata authority version changed/dirty"),
        "expected exact stale version refusal, observed {error:?}"
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_decision(
    case: SourceCase,
    trace: Trace,
    result: Result<()>,
    attempt: Result<serde_json::Value>,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    selected: crate::RangeId,
    counted: Vec<ClusterNodeId>,
    before: serde_json::Value,
    after: serde_json::Value,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
) {
    let mutation = trace.lock().clone();
    let physical = (|| -> Result<serde_json::Value> {
        let candidate = attempt
            .as_ref()
            .map_err(|e| BicDbError::Cluster(e.to_string()))?["native_attempts"]
            .as_array()
            .and_then(|a| a.last())
            .and_then(|a| a["record"]["checksum_sha256"].as_str())
            .ok_or_else(|| {
                BicDbError::Cluster("exact retained native candidate hash absent".into())
            })?;
        let db = factory
            .database
            .try_read()
            .ok_or_else(|| BicDbError::Cluster("actual native decision readback DB busy".into()))?;
        db.test_registered_joint_decision_record_evidence(selected, candidate)
    })();
    evidence(&output.join("registered-hot-local-term-decision.json"),&serde_json::json!({
        "case":format!("{case:?}"),"mutation":mutation,"native_attempt":attempt.as_ref().ok(),
        "actual_native_readback":physical.as_ref().ok(),"native_readback_error":physical.as_ref().err().map(ToString::to_string),
        "attempt_evidence_error":attempt.as_ref().err().map(ToString::to_string),
        "api_returned_success":result.is_ok(),"exact_error":result.as_ref().err().map(ToString::to_string),
        "error_debug":result.as_ref().err().map(|e|format!("{e:?}")),
        "actual_counted_durable_members":counted,"before":before,"after":after,
        "source_app_ack_ledger":acknowledged,"new_sdk_operations":[],
        "local_known_staleness_only":true,"partition_or_new_leader_tested":false,
        "native_committed_outcome_is_not_rolled_back":true})).unwrap();
    let error =
        result.expect_err("known-stale local metadata cannot return a usable decision vote");
    require_mutation(&mutation, &error);
    assert!(
        !counted.contains(&node(4)),
        "no usable target vote counted after term change"
    );
    let attempt = attempt.unwrap();
    let actual = attempt["native_attempts"].as_array().unwrap();
    assert_eq!(
        actual.len(),
        1,
        "same exact candidate/xid, no replacement attempt"
    );
    assert!(actual[0]["native_xid"].as_u64().unwrap() > 0);
    assert_eq!(
        actual[0]["record"]["native_page_xid"],
        actual[0]["native_xid"]
    );
    let expected = if matches!(case, SourceCase::TargetHotDecisionTermBeforePut) {
        "actual_abort_returned"
    } else {
        "actual_commit_returned"
    };
    assert_eq!(actual[0]["outcome"]["state"], expected);
    assert_eq!(actual[0]["outcome"]["actual_xid"], actual[0]["native_xid"]);
    let bytes: Vec<u8> = serde_json::from_value(actual[0]["bytes"].clone()).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(&bytes)),
        actual[0]["bytes_sha256"]
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        actual[0]["record"]
    );
    let physical = physical.unwrap();
    if matches!(case, SourceCase::TargetHotDecisionTermBeforePut) {
        assert!(physical["actual_pointer"].is_null());
        assert!(physical["actual_candidate_archive"].is_null());
    } else {
        assert_eq!(physical["actual_pointer"], actual[0]["record"]);
        assert_eq!(physical["actual_candidate_archive"], actual[0]["record"]);
    }

    require_snapshots(&before, &after);
    assert_eq!(after["database"]["writer_installed"], false);
    assert_eq!(before["database"]["rows"], after["database"]["rows"]);
    assert!(after["joined_data_status"].is_null());
}
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_policy(
    trace: Trace,
    result: Result<()>,
    attempt: Result<serde_json::Value>,
    before: serde_json::Value,
    after: serde_json::Value,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
) {
    let mutation = trace.lock().clone();
    evidence(&output.join("registered-hot-local-term-policy.json"),&serde_json::json!({
        "mutation":mutation,"actual_first_policy_attempt":attempt.as_ref().ok(),
        "attempt_evidence_error":attempt.as_ref().err().map(ToString::to_string),
        "api_returned_success":result.is_ok(),"exact_error":result.as_ref().err().map(ToString::to_string),
        "error_debug":result.as_ref().err().map(|e|format!("{e:?}")),"before":before,"after":after,
        "source_app_ack_ledger":acknowledged,"new_sdk_operations":[],
        "local_known_staleness_only":true,"partition_or_new_leader_tested":false,
        "actual_native_policy_retained_no_new_attempt":true})).unwrap();
    let error = result.expect_err("known-stale metadata cannot publish FIRST hook");
    require_mutation(&mutation, &error);
    let attempt = attempt.unwrap();
    assert_eq!(
        attempt["stage"], "NativeReadBack",
        "exact native record retained before hook"
    );
    assert!(attempt["record"]["native_page_xid"].as_u64().unwrap() > 0);
    require_snapshots(&before, &after);
    assert_eq!(
        after["database"]["writer_installed"], false,
        "no actual writer hook installed"
    );
    assert_eq!(after["first_policy_attempt"], attempt);
    assert_eq!(before["database"]["rows"], after["database"]["rows"]);
    assert!(after["joined_data_status"].is_null());
}

fn require_snapshots(before: &serde_json::Value, after: &serde_json::Value) {
    assert_eq!(
        before["actual_hook"]["registration_generation"],
        after["actual_hook"]["registration_generation"]
    );
    assert!(before["actual_hook"]["registration_generation"].is_string());
    for snapshot in [before, after] {
        assert!(
            snapshot["database"].is_object(),
            "missing actual DB snapshot"
        );
        assert!(snapshot["database_error"].is_null());
        assert!(snapshot["hook_evidence_error"].is_null());
        assert_eq!(snapshot["actual_hook"]["authority_present"], false);
        assert_eq!(snapshot["actual_hook"]["registration_active"], false);
        assert!(snapshot["actual_hook"]["native_writer_record"].is_null());
        assert_eq!(snapshot["database"]["writer_installed"], false);
    }
}
