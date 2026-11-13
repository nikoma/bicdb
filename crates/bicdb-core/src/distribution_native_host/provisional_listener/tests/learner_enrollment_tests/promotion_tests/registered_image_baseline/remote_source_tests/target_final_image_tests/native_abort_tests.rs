//! Real native I/O/abort and finite-budget controls, never mocked Tx status.
use super::*;

#[test]
fn native_registered_fifth_remote_native_base_actual_abort_sync_failure_never_replaces_poisoned_owner(
) {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseAbortSyncFailed),
    );
}
#[test]
fn native_registered_fifth_remote_native_base_actual_commit_sync_unknown_never_replaces_owner() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseCommitSyncUnknown),
    );
}
#[test]
fn native_registered_fifth_remote_native_base_actual_proven_abort_budget_exhaustion_retains_every_owner(
) {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseAbortBudget),
    );
}
fn health(
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    xid: Option<u64>,
) -> serde_json::Value {
    match factory.database.try_read() {
        Some(db) => db.test_registered_native_health_status(xid),
        None => {
            serde_json::json!({"diagnostic_error":"actual DB busy during health/status observation"})
        }
    }
}
fn save(output: &std::path::Path, report: &serde_json::Value) {
    let error = evidence(
        &output.join("registered-native-base-abort-recovery-outcome.json"),
        report,
    )
    .err()
    .map(|e| e.to_string());
    if let Some(error) = &error {
        eprintln!("REGISTERED_NATIVE_BASE_ABORT_RECOVERY_EVIDENCE_ERROR {error}");
    }
    assert!(error.is_none(), "actual outcome report failed: {error:?}");
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
    if matches!(case, SourceCase::TargetNativeBaseAbortBudget) {
        let mut attempts = Vec::new();
        // Nine actual, separately requested healthy before-put aborts fill
        // eight retired slots and retain the ninth current attempted owner.
        for index in 0..=8 {
            let cut = native_expiry_tests::ExpiryControl::caller_cancel_before_put();
            let mut windows = Vec::new();
            let result = advance_with_diagnostics(
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
                Some(&mut windows),
                Some(&cut),
            );
            let snapshot = catalogue_tests::prerequisite_snapshot(
                control,
                factory,
                pending,
                ranges,
                range,
                attempt,
                acknowledged,
            );
            attempts.push(serde_json::json!({"index":index,"exact_error":result.as_ref().err().map(|e|e.to_string()),"progress":result.as_ref().ok(),"snapshot":snapshot,"events":cut.events(),"windows":windows}));
            save(
                output,
                &serde_json::json!({"version":1,"source_case":format!("{case:?}"),"before":before,"actual_aborted_requests":attempts,"final_budget_request_not_yet_made":true,"ordinary_sql_or_serving_claim":false}),
            );
            assert!(
                matches!(&result, Err(BicDbError::QueryCanceled)),
                "actual abort {index}: {result:?}"
            );
            let owner = &snapshot["image"]["value"]["native_base"];
            let xid = owner["receipt"]["native_page_xid"].as_u64().unwrap();
            assert_eq!(owner["max_retired_attempts"], 8);
            assert_eq!(owner["native_attempts_issued"], index + 1);
            assert_eq!(owner["retired"].as_array().unwrap().len(), index as usize);
            assert_eq!(
                snapshot["native"]["value"]["native_tx_status_api_observations"][xid.to_string()],
                "Aborted"
            );
            assert!(
                snapshot["native"]["value"]["base"].is_null()
                    && snapshot["native"]["value"]["attempt_archive"].is_null()
            );
            assert_eq!(
                snapshot["owner"]["value"]["actual_owner"],
                before["owner"]["value"]["actual_owner"]
            );
            assert_eq!(
                snapshot["exact_full_payload_physical_collections"],
                serde_json::json!(["__bic_cluster_events"])
            );
        }
        let full = catalogue_tests::prerequisite_snapshot(
            control,
            factory,
            pending,
            ranges,
            range,
            attempt,
            acknowledged,
        );
        let mut windows = Vec::new();
        let result = advance_with_diagnostics(
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
            Some(&mut windows),
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
        save(
            output,
            &serde_json::json!({"version":1,"source_case":format!("{case:?}"),"before":before,"actual_aborted_requests":attempts,"before_exhausted_request":full,"after_exhausted_request":after,"exact_budget_error":result.as_ref().err().map(|e|e.to_string()),"budget_request_windows":windows,"default_entry_and_byte_caps_unchanged":true,"no_eviction_or_new_native_attempt":true,"no_cold_or_serving_qualification":true}),
        );
        assert!(
            result.as_ref().err().is_some_and(|e| e
                .to_string()
                .contains("proven-abort retry budget exhausted")),
            "{result:?}"
        );
        assert_eq!(full["image"], after["image"]);
        assert_eq!(full["native"], after["native"]);
        assert_eq!(full["logical"], after["logical"]);
        assert_eq!(full["owner"], after["owner"]);
        let owner = &after["image"]["value"]["native_base"];
        assert_eq!(owner["native_attempts_issued"], 9);
        assert_eq!(owner["retired"].as_array().unwrap().len(), 8);
        assert!(
            owner["retired_canonical_bytes"].as_u64().unwrap()
                <= owner["max_retired_canonical_bytes"].as_u64().unwrap()
        );
        for old in owner["retired"].as_array().unwrap() {
            assert_eq!(old["same_open_terminal_status"], "Aborted");
            assert_eq!(old["core_page_wal_healthy_at_retirement"], true);
        }
        return;
    }
    let root = factory.database.read().data_path().to_owned();
    let abort_sync = matches!(case, SourceCase::TargetNativeBaseAbortSyncFailed);
    let cut = if abort_sync {
        native_expiry_tests::ExpiryControl::post_put_sync_failure(root)
    } else {
        native_expiry_tests::ExpiryControl::commit_sync_failure(root)
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
        Some(&cut),
    );
    let faults = cut.disarm_sync_fault();
    let first = catalogue_tests::prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        attempt,
        acknowledged,
    );
    let xid = first["image"]["value"]["native_base"]["receipt"]["native_page_xid"].as_u64();
    let first_health = health(factory, xid);
    let mut fresh_windows = Vec::new();
    let fresh_result = advance_with_diagnostics(
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
        Some(&mut fresh_windows),
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
    let after_health = health(factory, xid);
    let report = serde_json::json!({"version":1,"source_case":format!("{case:?}"),"before":before,"after_first_failure":first,"after_fresh_request":after,
        "actual_cut_events":cut.events(),"actual_io_fault":faults,"first_health_status":first_health,"after_health_status":after_health,
        "issued_native_xid":xid,"first_exact_error":first_result.as_ref().err().map(|e|e.to_string()),"first_error_debug":first_result.as_ref().err().map(|e|format!("{e:?}")),
        "fresh_exact_error":fresh_result.as_ref().err().map(|e|e.to_string()),"fresh_error_debug":fresh_result.as_ref().err().map(|e|format!("{e:?}")),
        "first_request_windows":first_windows,"fresh_request_windows":fresh_windows,
        "original_a_and_metadata_owner_retained":true,"no_health_reset_or_engine_reopen":true,
        "post_failure_payload_visibility_not_asserted_on_fenced_handle":true,"no_cold_or_serving_qualification":true});
    save(output, &report);
    assert_eq!(
        faults["fired"], 1,
        "actual native page_log.sync failure was not reached"
    );
    assert_eq!(
        cut.events().len(),
        2,
        "intended actual native cut not reached"
    );
    assert_eq!(first_health["wal_failure"]["site"], "page_log.sync");
    assert!(!first_health["core_recovery_error"].is_null());
    assert!(!after_health["core_recovery_error"].is_null());
    assert_eq!(
        first_health["wal_failure"], after_health["wal_failure"],
        "health must remain sticky after disarming actual fault"
    );
    let original = &first["image"]["value"]["native_base"];
    let current = &after["image"]["value"]["native_base"];
    for snapshot in [&first, &after] {
        assert!(snapshot["owner"]["diagnostic_error"].is_null());
        assert!(snapshot["image"]["diagnostic_error"].is_null());
    }
    assert_eq!(
        original, current,
        "fenced or uncertain native attempt must not be replaced or retired"
    );
    assert_eq!(current["native_attempts_issued"], 1);
    assert!(current["retired"].as_array().unwrap().is_empty());
    assert_eq!(
        first["owner"]["value"]["actual_owner"],
        after["owner"]["value"]["actual_owner"]
    );
    assert!(
        fresh_result.is_err(),
        "fresh authority must not reset a failed native WAL"
    );
    assert!(
        fresh_result
            .as_ref()
            .err()
            .is_some_and(|e| e.is_durability_failure() || e.to_string().contains("refuses writes")),
        "expected actual health refusal: {fresh_result:?}"
    );
    let before_invoke = fresh_windows
        .iter()
        .find(|e| e["stage"] == "before_actual_host_invoke")
        .expect("fresh role/OWN/frame actually observed");
    for key in ["role", "own", "source_original_token", "parent"] {
        assert!(before_invoke["observation"][key]["original_check_error"].is_null());
    }
    if abort_sync {
        assert!(
            matches!(&first_result, Err(BicDbError::QueryTimedOut)),
            "{first_result:?}"
        );
        assert_eq!(
            first_health["native_status_without_health_admission"],
            "Aborted"
        );
        assert_eq!(original["puts_completed"], 1);
        assert_eq!(cut.events()[1]["original_role"]["expired_by_clock"], true);
        assert!(cut.events()[1]["original_parent"]["original_check_error"].is_null());
    } else {
        assert!(
            matches!(&first_result, Err(BicDbError::CommitOutcomeUnknown { .. })),
            "must reach actual native commit sync uncertainty: {first_result:?}"
        );
        assert_eq!(original["puts_completed"], 2);
        assert_eq!(cut.events()[1]["original_role"]["expired_by_clock"], false);
    }
}
