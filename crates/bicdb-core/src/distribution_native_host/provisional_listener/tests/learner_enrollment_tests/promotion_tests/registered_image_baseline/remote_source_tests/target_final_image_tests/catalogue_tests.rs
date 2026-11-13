//! Actual catalogue adoption only: CLOSED remains and no writer is installed.
use super::*;

#[test]
fn native_registered_fifth_remote_catalogue_floor_actual_native_base_and_catalogue_match() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetCatalogueFloor),
    );
}
#[test]
fn native_registered_fifth_remote_catalogue_floor_actual_missing_native_base_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetCatalogueFloorMissingBase),
    );
}
#[test]
fn native_registered_fifth_remote_catalogue_floor_actual_repeated_directory_eio_same_candidate() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetCatalogueFloorDirSyncRetry),
    );
}
#[test]
fn native_registered_fifth_remote_catalogue_floor_actual_post_fsync_cancel_same_native_base_retry()
{
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetCatalogueFloorCanceled),
    );
}
fn catalogue(state: &serde_json::Value) -> serde_json::Value {
    let bytes = state["raw"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| u8::try_from(n.as_u64().unwrap()).unwrap())
        .collect::<Vec<_>>();
    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["catalog"].clone()
}
fn observed(result: Result<serde_json::Value>) -> serde_json::Value {
    match result {
        Ok(value) => serde_json::json!({"value":value,"diagnostic_error":null}),
        Err(error) => serde_json::json!({"value":null,"diagnostic_error":error.to_string()}),
    }
}
fn prerequisite_snapshot(
    control: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    range: crate::RangeId,
    attempt: Uuid,
    acknowledged: &serde_json::Value,
) -> serde_json::Value {
    let owner = observed(control.test_registered_remote_attempt(range));
    let image = observed(control.test_registered_remote_image_state(range));
    let attempted_xid = (image["value"]["native_base"]["attempted"].as_bool() == Some(true))
        .then(|| image["value"]["native_base"]["receipt"]["native_page_xid"].as_u64())
        .flatten()
        .filter(|xid| *xid > 0);
    let native = observed(match factory.database.try_read() {
        Some(db) => db.test_registered_native_base_forensic_status(range, attempt, attempted_xid),
        None => Err(BicDbError::Cluster(
            "prerequisite diagnostic actual DB busy".into(),
        )),
    });
    let logical = observed(state(&factory.database, pending, &node(4), ranges));
    let payload = &acknowledged["actual_full_record"];
    let physical_matches = logical["value"]["rows"].as_object().map(|collections| {
        collections
            .iter()
            .filter_map(|(name, records)| {
                records
                    .as_array()
                    .is_some_and(|records| records.contains(payload))
                    .then_some(name.clone())
            })
            .collect::<Vec<_>>()
    });
    serde_json::json!({"owner":owner,"image":image,"native":native,"logical":logical,
        "acknowledged_logical_api":acknowledged,"exact_full_payload_physical_collections":physical_matches,
        "logical_event_collection_is_not_a_sql_table_claim":true,
        "original_owners_retained":true,"engine_reopened":false,"read_only_diagnostic_not_admission":true})
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
    if matches!(case, SourceCase::TargetCatalogueFloorMissingBase) {
        let before = state(&factory.database, pending, &node(4), ranges).unwrap();
        let owner_before = control.test_registered_remote_attempt(range).unwrap();
        let image_before = control.test_registered_remote_image_state(range).unwrap();
        let error = advance(
            control,
            client,
            source,
            plan,
            provider,
            actor,
            output,
            journal,
            Operation::AdoptCatalogueFloor,
            false,
        )
        .unwrap_err();
        let after = state(&factory.database, pending, &node(4), ranges).unwrap();
        let owner_after = control.test_registered_remote_attempt(range).unwrap();
        let image_after = control.test_registered_remote_image_state(range).unwrap();
        evidence(&output.join("registered-catalogue-floor-missing-base.json"),&serde_json::json!({"before":before,"after":after,"owner_before":owner_before,"owner_after":owner_after,"image_before":image_before,"image_after":image_after,"exact_error":error.to_string(),"serving":false})).unwrap();
        assert!(
            error
                .to_string()
                .contains("requires original actual verified native base"),
            "{error}"
        );
        assert_eq!(before, after);
        assert_eq!(owner_before, owner_after);
        assert_eq!(image_before, image_after);
        return;
    }
    let prerequisite_before = prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        source.manifest().target_attempt,
        acknowledged,
    );
    let mut request_observations = Vec::new();
    let prerequisite_started = Instant::now();
    let prerequisite_result = advance_with_diagnostics(
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
        Some(&mut request_observations),
    );
    let prerequisite_elapsed_us = prerequisite_started.elapsed().as_micros();
    let prerequisite_after = prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        source.manifest().target_attempt,
        acknowledged,
    );
    let prerequisite_report = serde_json::json!({"version":1,"operation":"PublishNativeBase prerequisite before catalogue fault arming",
        "source_manifest_sha256":source.manifest().checksum_sha256,"actual_target_attempt":source.manifest().target_attempt,
        "before":prerequisite_before,"after":prerequisite_after,"request_observations":request_observations,
        "elapsed_us":prerequisite_elapsed_us,"exact_error":prerequisite_result.as_ref().err().map(|e|e.to_string()),
        "error_debug":prerequisite_result.as_ref().err().map(|e|format!("{e:?}")),"actual_progress":prerequisite_result.as_ref().ok(),
        "scope_budget_ms_unchanged":5000,"request_order_unchanged":["Role","OWN","Finished","actual_host"],
        "observations_are_local_attempt_diagnostics_not_source_frames":true,
        "catalogue_fault_not_yet_armed":true,"cold_reopen_or_serving_claim":false});
    let diagnostic_write_error = evidence(
        &output.join("registered-catalogue-prerequisite-outcome.json"),
        &prerequisite_report,
    )
    .err()
    .map(|e| e.to_string());
    if let Some(error) = &diagnostic_write_error {
        eprintln!("REGISTERED_CATALOGUE_PREREQUISITE_DIAGNOSTIC_ERROR {error}");
    }
    // Retain the exact primary API outcome even if a diagnostic read/write
    // failed. This assertion follows the outcome report, not the other way round.
    prerequisite_result.unwrap();
    assert!(
        diagnostic_write_error.is_none(),
        "diagnostic persistence failed: {diagnostic_write_error:?}"
    );
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let owner_before = control.test_registered_remote_attempt(range).unwrap();
    let image_before = control.test_registered_remote_image_state(range).unwrap();
    let native_before = factory
        .database
        .read()
        .test_registered_native_base_json(range)
        .unwrap();
    let root = factory.database.read().data_path().to_owned();
    let mut failures = Vec::new();
    let mut failure_outcomes = Vec::new();
    let mut candidate_bytes = None;
    if matches!(case, SourceCase::TargetCatalogueFloorDirSyncRetry) {
        let faults = bicdb_page::test_faults::arm_io_failure(
            "registered_remote_adoption.catalogue.dir_sync",
            &root,
            1,
            true,
        );
        for count in 1..=2 {
            let error = advance(
                control,
                client,
                source,
                plan,
                provider,
                actor,
                output,
                journal,
                Operation::AdoptCatalogueFloor,
                false,
            )
            .unwrap_err();
            let owner = control.test_registered_remote_attempt(range).unwrap();
            let observed = state(&factory.database, pending, &node(4), ranges).unwrap();
            evidence(&output.join(format!("registered-catalogue-floor-directory-error-{count}.json")),&serde_json::json!({"exact_error":error.to_string(),"actual_fault_count":faults.fired(),"owner":owner,"observed":observed})).unwrap();
            assert!(
                matches!(&error,BicDbError::Io(e) if e.raw_os_error()==Some(libc::EIO)),
                "{error}"
            );
            assert_eq!(faults.fired(), count);
            assert_eq!(owner["catalogue_floor"]["verified"], false);
            if let Some(bytes) = &candidate_bytes {
                assert_eq!(bytes, &observed["raw"]);
            } else {
                candidate_bytes = Some(observed["raw"].clone());
            }
            failures.push(error.to_string());
            failure_outcomes.push(serde_json::json!({"owner":owner,"observed":observed}));
        }
        drop(faults);
    } else if matches!(case, SourceCase::TargetCatalogueFloorCanceled) {
        let error = advance(
            control,
            client,
            source,
            plan,
            provider,
            actor,
            output,
            journal,
            Operation::AdoptCatalogueFloor,
            true,
        )
        .unwrap_err();
        let owner = control.test_registered_remote_attempt(range).unwrap();
        let observed = state(&factory.database, pending, &node(4), ranges).unwrap();
        evidence(
            &output.join("registered-catalogue-floor-post-fsync-cancel.json"),
            &serde_json::json!({"exact_error":error.to_string(),"owner":owner,"observed":observed}),
        )
        .unwrap();
        assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
        assert_eq!(owner["catalogue_floor"]["verified"], false);
        candidate_bytes = Some(observed["raw"].clone());
        failures.push(error.to_string());
        failure_outcomes.push(serde_json::json!({"owner":owner,"observed":observed}));
    }
    // Even after a visible candidate, ordinary image admission must not take
    // over the dedicated retry owner or clear its persistence uncertainty.
    let ordinary_error = if candidate_bytes.is_some() {
        Some(
            advance(
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
            )
            .unwrap_err()
            .to_string(),
        )
    } else {
        None
    };
    let first = advance(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::AdoptCatalogueFloor,
        false,
    )
    .unwrap();
    let second = advance(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::AdoptCatalogueFloor,
        false,
    )
    .unwrap();
    let ordinary_after_adoption_error = advance(
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
    )
    .unwrap_err()
    .to_string();
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    let owner_after = control.test_registered_remote_attempt(range).unwrap();
    let image_after = control.test_registered_remote_image_state(range).unwrap();
    let native_after = factory
        .database
        .read()
        .test_registered_native_base_json(range)
        .unwrap();
    let old_catalogue = catalogue(&before);
    let new_catalogue = catalogue(&after);
    evidence(&output.join("registered-catalogue-floor-before-assertions.json"),&serde_json::json!({"before":before,"after":after,"owner_before":owner_before,"owner_after":owner_after,"image_before":image_before,"image_after":image_after,"native_before":native_before,"native_after":native_after,"old_catalogue":old_catalogue,"new_catalogue":new_catalogue,"failures":failures,"failure_outcomes":failure_outcomes,"ordinary_image_retry_error":ordinary_error,"ordinary_after_adoption_error":ordinary_after_adoption_error,"first_progress":first,"second_progress":second,"catalogue_floor_adopted":true,"serving":false,"cold_reopen_or_physical_qualification":false})).unwrap();
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
    let Progress::CatalogueFloor {
        range_index,
        native_page_xid,
        native_receipt_sha256,
    } = first
    else {
        panic!("actual catalogue floor progress expected")
    };
    assert_eq!(range_index, source.manifest().range_index);
    assert_eq!(native_before, native_after);
    assert_eq!(image_before, image_after);
    assert_eq!(native_after["base"]["native_page_xid"], native_page_xid);
    assert_eq!(
        native_after["base"]["checksum_sha256"],
        native_receipt_sha256
    );
    assert_eq!(owner_after["catalogue_floor"]["verified"], true);
    assert_eq!(owner_before["actual_owner"], owner_after["actual_owner"]);
    assert_eq!(owner_before["installing"], owner_after["installing"]);
    assert_eq!(owner_before["proposal"], owner_after["proposal"]);
    for field in [
        "B",
        "birth",
        "open",
        "namespace_ids",
        "schema",
        "protected_catalogue_uuid",
        "points",
        "slots",
        "rows",
        "writer_installed",
    ] {
        assert_eq!(before[field], after[field], "changed {field}");
    }
    assert_eq!(after["writer_installed"], false);
    if let Some(bytes) = candidate_bytes {
        assert_eq!(bytes, after["raw"]);
    }
    let key = range.get().to_string();
    let log = &new_catalogue["ranges"][&key];
    for field in ["last_index", "resolved_through", "compacted_through"] {
        assert_eq!(log[field], range_index);
    }
    assert_eq!(log["entries"], serde_json::json!({}));
    let binding = &new_catalogue["native_image_bases"][&key];
    assert_eq!(binding["range_index"], range_index);
    assert_eq!(binding["native_receipt_sha256"], native_receipt_sha256);
    assert_eq!(
        binding["logical_history_sha256"],
        native_after["base"]["logical_history_sha256"]
    );
    let mut expected = old_catalogue.clone();
    expected["ranges"][&key] = log.clone();
    expected["native_image_bases"] = new_catalogue["native_image_bases"].clone();
    assert_eq!(
        expected, new_catalogue,
        "only selected floor and native binding may change"
    );
    assert_eq!(
        new_catalogue["image_install_intents"],
        old_catalogue["image_install_intents"]
    );
}
