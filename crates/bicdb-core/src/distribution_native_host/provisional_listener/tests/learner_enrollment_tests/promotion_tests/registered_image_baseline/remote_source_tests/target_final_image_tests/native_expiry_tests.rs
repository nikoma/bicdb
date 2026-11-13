//! Actual-clock authority expiry, not a claim about the earlier CI failure.
use super::*;
use parking_lot::Mutex;

#[derive(Clone, Copy)]
enum Cut {
    BeforeFirstPut,
    AfterCommittedWal,
}
pub(super) struct ExpiryControl {
    cut: Cut,
    events: Arc<Mutex<Vec<serde_json::Value>>>,
}
impl ExpiryControl {
    fn callback(
        &self,
        role: &crate::distribution_range_recovery::HandoffRoleAdmission,
        parent: &CancellationToken,
    ) -> Box<dyn FnOnce()> {
        let original_role = role.retain_original();
        let original_parent = parent.clone();
        let deadline = original_role.deadline();
        let events = self.events.clone();
        let cut = match self.cut {
            Cut::BeforeFirstPut => "after_actual_xid_retention_before_owned_current_or_first_put",
            Cut::AfterCommittedWal => "after_actual_native_commit_return_before_postchecks",
        };
        Box::new(move || {
            let started = Instant::now();
            events.lock().push(serde_json::json!({"event":"actual_cut_entered","cut":cut,
                "original_role":diagnostic_window(Some(deadline),original_role.require_live_request()),
                "original_parent":diagnostic_window(original_parent.deadline(),original_parent.check()),
                "fake_cancellation_or_deadline_renewal":false}));
            // Wait only for this ACTUAL captured Role deadline. Parent remains
            // the existing5s bound; unexpected parent cancellation ends waiting.
            // No engine, metadata, network, file or admission operation occurs.
            while Instant::now() < deadline && original_parent.check().is_ok() {
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(5)),
                );
            }
            events.lock().push(serde_json::json!({"event":"actual_cut_wait_finished","cut":cut,"elapsed_us":started.elapsed().as_micros(),
                "original_role":diagnostic_window(Some(deadline),original_role.require_live_request()),
                "original_parent":diagnostic_window(original_parent.deadline(),original_parent.check()),
                "fake_cancellation_or_deadline_renewal":false}));
        })
    }
    pub(super) fn arm(
        &self,
        role: &crate::distribution_range_recovery::HandoffRoleAdmission,
        parent: &CancellationToken,
    ) -> (Option<impl Drop>, Option<impl Drop>) {
        let before = matches!(self.cut, Cut::BeforeFirstPut).then(|| {
            BicDb::test_registered_remote_base_before_first_put(self.callback(role, parent))
        });
        let after = matches!(self.cut, Cut::AfterCommittedWal)
            .then(|| crate::db::test_registered_remote_base_after_wal(self.callback(role, parent)));
        (before, after)
    }
}

#[test]
fn native_registered_fifth_remote_native_base_actual_expiry_before_put_proven_abort_same_a_resumes()
{
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseRoleExpiryBeforePut),
    );
}
#[test]
fn native_registered_fifth_remote_native_base_actual_expiry_after_commit_same_xid_resumes() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseRoleExpiryAfterCommit),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: &Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
    acknowledged: &serde_json::Value,
) {
    let range = plan.old_range().id;
    let attempt = source.manifest().target_attempt;
    let before = catalogue_tests::prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        attempt,
        acknowledged,
    );
    let cut = if matches!(case, SourceCase::TargetNativeBaseRoleExpiryBeforePut) {
        Cut::BeforeFirstPut
    } else {
        Cut::AfterCommittedWal
    };
    let expiry = ExpiryControl {
        cut,
        events: Arc::new(Mutex::new(Vec::new())),
    };
    let mut first_windows = Vec::new();
    let first_result = advance_with_diagnostics(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::PublishNativeBase,
        false,
        Some(&mut first_windows),
        Some(&expiry),
    );
    let first = catalogue_tests::prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        attempt,
        acknowledged,
    );
    let issued_xid = first["image"]["value"]["native_base"]["receipt"]["native_page_xid"].as_u64();
    let mut retry_windows = Vec::new();
    // A separate actual request acquires genuine fresh Role/OWN/Finished, but
    // consumes the SAME original A/MF/image owner. No preparation/recreated
    // process, source parent, target freeze or local-history conversion occurs.
    let retry_result = advance_with_diagnostics(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::PublishNativeBase,
        false,
        Some(&mut retry_windows),
        None,
    );
    let after = catalogue_tests::prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        attempt,
        acknowledged,
    );
    let old_xid_after_retry = match factory.database.try_read() {
        Some(db) => {
            match db.test_registered_native_base_forensic_status(range, attempt, issued_xid) {
                Ok(value) => serde_json::json!({"value":value,"diagnostic_error":null}),
                Err(error) => {
                    serde_json::json!({"value":null,"diagnostic_error":error.to_string()})
                }
            }
        }
        None => {
            serde_json::json!({"value":null,"diagnostic_error":"actual DB busy during original xid readback"})
        }
    };
    let events = expiry.events.lock().clone();
    let first_native = &first["native"]["value"];
    let native_status = issued_xid
        .and_then(|xid| first_native["native_tx_status_api_observations"][xid.to_string()].as_str())
        .map(str::to_string);
    let classification = match native_status.as_deref() {
        Some("Aborted")
            if first_native["base"].is_null() && first_native["attempt_archive"].is_null() =>
        {
            "same_open_native_abort_with_no_visible_base_or_archive"
        }
        Some("Committed")
            if !first_native["base"].is_null()
                && first_native["base"] == first_native["attempt_archive"] =>
        {
            "same_open_committed_base_and_archive"
        }
        _ => "unknown_native_outcome_stays_fenced",
    };
    let report = serde_json::json!({"version":1,"source_case":format!("{case:?}"),"before":before,"after_first_expiry":first,"after_fresh_request":after,
        "issued_native_xid":issued_xid,"original_xid_after_retry":old_xid_after_retry,"same_open_native_classification":classification,
        "actual_cut_events":events,"first_request_windows":first_windows,"retry_request_windows":retry_windows,
        "first_exact_error":first_result.as_ref().err().map(|e|e.to_string()),"first_error_debug":first_result.as_ref().err().map(|e|format!("{e:?}")),"first_progress":first_result.as_ref().ok(),
        "retry_exact_error":retry_result.as_ref().err().map(|e|e.to_string()),"retry_error_debug":retry_result.as_ref().err().map(|e|format!("{e:?}")),"retry_progress":retry_result.as_ref().ok(),
        "same_manifest_sha256":source.manifest().checksum_sha256,"actual_target_attempt":attempt,
        "parent_budget_ms_unchanged":5000,"request_order_unchanged":["Role","OWN","Finished","actual_host"],
        "real_original_role_deadline_wait_not_fake_cancel":true,"original_e1ec_failure_cause_claimed":false,
        "desired_acceptance_original_a_resumes_after_proven_abort_or_same_committed_xid":true,
        "no_cold_powerloss_or_serving_qualification":true});
    let write_error = evidence(
        &output.join("registered-native-base-real-role-expiry-outcome.json"),
        &report,
    )
    .err()
    .map(|e| e.to_string());
    if let Some(error) = &write_error {
        eprintln!("REGISTERED_NATIVE_BASE_EXPIRY_DIAGNOSTIC_ERROR {error}");
    }
    // Every observation, including an unsuccessful fresh retry, is retained
    // before asserting the desired recovery contract. This intentionally
    // exposes an unfixed proven-abort liveness defect instead of accepting it.
    assert!(
        write_error.is_none(),
        "expiry outcome persistence failed: {write_error:?}"
    );
    assert_eq!(
        events.len(),
        2,
        "actual intended native cut was not reached; no expiry proof"
    );
    assert_eq!(events[1]["original_role"]["expired_by_clock"], true);
    assert!(!events[1]["original_role"]["original_check_error"].is_null());
    assert_eq!(events[1]["original_parent"]["expired_by_clock"], false);
    assert!(events[1]["original_parent"]["original_check_error"].is_null());
    assert!(
        matches!(&first_result, Err(BicDbError::QueryTimedOut)),
        "expiry primary outcome: {first_result:?}"
    );
    for snapshot in [&before, &first, &after] {
        for key in ["owner", "image", "native", "logical"] {
            assert!(
                snapshot[key]["diagnostic_error"].is_null(),
                "diagnostic {key} unavailable: {}",
                snapshot[key]
            );
        }
        assert_eq!(
            snapshot["owner"]["value"]["actual_owner"],
            before["owner"]["value"]["actual_owner"]
        );
        assert_eq!(
            snapshot["exact_full_payload_physical_collections"],
            serde_json::json!(["__bic_cluster_events"])
        );
        for key in [
            "rows",
            "schema",
            "namespace_ids",
            "open",
            "birth",
            "B",
            "raw",
            "slots",
            "protected_catalogue_uuid",
            "writer_installed",
        ] {
            assert_eq!(
                snapshot["logical"]["value"][key], before["logical"]["value"][key],
                "expiry changed {key}"
            );
        }
    }
    let xid = issued_xid.expect("actual issued nonzero native xid must be retained");
    assert!(xid > 0);
    assert_eq!(first["image"]["value"]["native_base"]["attempted"], true);
    assert_eq!(first["image"]["value"]["native_base"]["verified"], false);
    match cut {
        Cut::BeforeFirstPut => {
            assert_eq!(
                native_status.as_deref(),
                Some("Aborted"),
                "absence alone is not native-abort proof"
            );
            assert!(first_native["base"].is_null() && first_native["attempt_archive"].is_null());
            assert!(first["image"]["value"]["native_base"]["actual_commit_returned"].is_null());
            assert_eq!(
                old_xid_after_retry["value"]["native_tx_status_api_observations"][xid.to_string()],
                "Aborted"
            );
        }
        Cut::AfterCommittedWal => {
            assert_eq!(native_status.as_deref(), Some("Committed"));
            assert_eq!(first_native["base"], first_native["attempt_archive"]);
            assert_eq!(
                first["image"]["value"]["native_base"]["actual_commit_returned"],
                xid
            );
            assert_eq!(first_native["base"], after["native"]["value"]["base"]);
        }
    }
    assert!(
        retry_result.is_ok(),
        "same original A must resume after the characterized native outcome; got {retry_result:?}"
    );
    let Progress::NativeBase {
        range_index,
        native_page_xid,
        ..
    } = retry_result.unwrap()
    else {
        panic!("actual native base retry progress required")
    };
    assert_eq!(range_index, source.manifest().range_index);
    if matches!(cut, Cut::AfterCommittedWal) {
        assert_eq!(
            native_page_xid, xid,
            "committed owner cannot allocate another native Tx"
        );
    }
    assert_eq!(after["image"]["value"]["native_base"]["verified"], true);
    assert_eq!(
        after["native"]["value"]["base"],
        after["native"]["value"]["attempt_archive"]
    );
}
