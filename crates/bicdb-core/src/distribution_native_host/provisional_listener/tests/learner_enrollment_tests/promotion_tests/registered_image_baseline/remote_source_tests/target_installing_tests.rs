//! Real fifth target protected proposal -> durable catalogue Installing.
//! Still no source-history import, row copy, Ready, Serving or physical claim.
use super::*;
#[test]
fn native_registered_fifth_remote_installing_actual_catalogue_same_owner_preserves_rows() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetInstalling));
}
#[test]
fn native_registered_fifth_remote_installing_actual_after_fsync_cancel_fresh_same_attempt() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetInstallingPostCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_installing_actual_directory_sync_error_retries_exact_catalogue() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetInstallingDirSyncRetry),
    );
}
#[allow(clippy::too_many_arguments)]
fn publish_fresh(
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: &Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    output: &std::path::Path,
    cancel_after_fsync: bool,
) -> Result<Arc<crate::distribution_range_consensus::NativeRegisteredRemoteInstallingFence>> {
    let parent = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &parent).into_handoff()?;
    let business = Uuid::now_v7();
    let own = client.acquire_host_metadata_observation(
        &node(0),
        business,
        Duration::from_secs(5),
        &parent,
    )?;
    let mut frames = Vec::new();
    let fresh = source.bind_step(super::frame(
        client,
        source.manifest(),
        Op::Finish,
        output,
        &mut frames,
    )?)?;
    let cut = cancel_after_fsync.then(|| {
        let caller = parent.clone();
        crate::distribution_range_consensus::test_registered_remote_installing_after_fsync(
            Box::new(move || caller.cancel()),
        )
    });
    let result =
        control.publish_registered_remote_installing(source, &fresh, role, own, business, &parent);
    drop(cut);
    parent.cancel();
    result
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: Arc<NativeRemoteImageFact>,
    frame: &crate::distribution_range_image::NativeRemoteImageStep<'_>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
) {
    // Reuse the actual happy proposal operation, not a forged pointer or new
    // receiver. It verifies the same original A and protected WAL bytes first.
    target_proposal_tests::exercise(
        SourceCase::TargetProposal,
        control,
        client,
        source.clone(),
        frame,
        plan,
        provider,
        actor,
        factory,
        pending,
        ranges,
        output,
    );
    let range = plan.old_range().id;
    let root = factory.database.read().data_path().to_owned();
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let native_before = factory
        .database
        .read()
        .test_registered_remote_native_inventory()
        .unwrap();
    let retained_before = control.test_registered_remote_attempt(range).unwrap();
    assert_eq!(retained_before["installing"], serde_json::Value::Null);
    let proposal_before = factory
        .database
        .read()
        .test_registered_remote_proposal_record(source.manifest().target_attempt)
        .unwrap();
    let mut failures = Vec::new();
    let mut failed_outcomes = Vec::new();
    let mut failed_catalogue = None;
    if matches!(case, SourceCase::TargetInstallingDirSyncRetry) {
        let faults = bicdb_page::test_faults::arm_io_failure(
            "registered_remote_install.catalogue.dir_sync",
            &root,
            1,
            true,
        );
        for expected in 1..=2 {
            let error = publish_fresh(
                control, client, &source, plan, provider, actor, output, false,
            )
            .err()
            .expect("actual directory sync must fail");
            assert!(
                matches!(&error, BicDbError::Io(e) if e.raw_os_error()==Some(libc::EIO)),
                "{error}"
            );
            assert_eq!(faults.fired(), expected);
            let outcome = control.test_registered_remote_attempt(range).unwrap();
            assert_eq!(outcome["actual_owner"], retained_before["actual_owner"]);
            assert_eq!(outcome["installing"]["verified"], false);
            let raw = std::fs::read(
                root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
            )
            .unwrap();
            if let Some(previous) = &failed_catalogue {
                assert_eq!(previous, &raw);
            } else {
                failed_catalogue = Some(raw);
            }
            failures.push(error.to_string());
            failed_outcomes.push(outcome);
        }
        drop(faults);
    } else if matches!(case, SourceCase::TargetInstallingPostCanceled) {
        let error = publish_fresh(
            control, client, &source, plan, provider, actor, output, true,
        )
        .err()
        .expect("actual post-fsync caller cancel must be returned");
        assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
        let outcome = control.test_registered_remote_attempt(range).unwrap();
        assert_eq!(outcome["installing"]["verified"], false);
        failed_catalogue = Some(
            std::fs::read(root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG))
                .unwrap(),
        );
        failures.push(error.to_string());
        failed_outcomes.push(outcome);
    }
    let fence = publish_fresh(
        control, client, &source, plan, provider, actor, output, false,
    )
    .unwrap();
    assert_eq!(fence.range().id, range);
    assert_eq!(fence.attempt_id(), source.manifest().target_attempt);
    assert_eq!(
        fence.source_manifest_sha256(),
        source.manifest().checksum_sha256
    );
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    let retained_after = control.test_registered_remote_attempt(range).unwrap();
    assert_eq!(
        retained_after["actual_owner"],
        retained_before["actual_owner"]
    );
    assert_eq!(retained_after["proposal"], retained_before["proposal"]);
    assert_eq!(retained_after["installing"]["verified"], true);
    if let Some(raw) = failed_catalogue {
        assert_eq!(
            raw,
            std::fs::read(root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG))
                .unwrap(),
            "fresh retry must fsync exact retained catalogue"
        );
    }
    let before_raw: Vec<u8> = serde_json::from_value(before["raw"].clone()).unwrap();
    let after_raw: Vec<u8> = serde_json::from_value(after["raw"].clone()).unwrap();
    let previous: serde_json::Value = serde_json::from_slice(&before_raw).unwrap();
    let current: serde_json::Value = serde_json::from_slice(&after_raw).unwrap();
    assert_eq!(current["catalog"]["format_version"], 17);
    assert_eq!(current["catalog"]["ranges"], previous["catalog"]["ranges"]);
    assert_eq!(
        current["catalog"]["recovery_fences"],
        previous["catalog"]["recovery_fences"]
    );
    assert_eq!(
        current["catalog"]["native_abort_protocol"],
        previous["catalog"]["native_abort_protocol"]
    );
    assert_eq!(
        current["catalog"]["native_joint_handoffs"],
        previous["catalog"]["native_joint_handoffs"]
    );
    assert_eq!(
        current["catalog"]["current_joint_recoveries"],
        previous["catalog"]["current_joint_recoveries"]
    );
    let intents = current["catalog"]["image_install_intents"]
        .as_object()
        .unwrap();
    assert_eq!(intents.len(), 1);
    let intent = intents.values().next().unwrap();
    assert_eq!(
        intent["attempt_id"],
        serde_json::to_value(fence.attempt_id()).unwrap()
    );
    assert_eq!(
        intent["registered_proposal"],
        serde_json::to_value(fence.protected_proposal()).unwrap()
    );
    assert_eq!(intent["protected_proposal"], serde_json::Value::Null);
    assert!(crate::distribution_range_consensus::RangeWriteStore::require_no_native_image_install_on_disk(&root, range).is_err());
    // Repair outer checksums after each deliberate field erasure: format
    // validation itself must reject these, not only the old checksum.
    let validate = crate::distribution_range_consensus::RangeWriteStore::test_registered_installing_envelope_semantics;
    validate(&after_raw).unwrap();
    let mut erased = current.clone();
    erased["catalog"]["image_install_intents"][range.get().to_string()]["registered_proposal"] =
        serde_json::Value::Null;
    assert!(validate(&serde_json::to_vec(&erased).unwrap()).is_err());
    let mut downgraded = current.clone();
    downgraded["catalog"]["format_version"] = 15.into();
    assert!(validate(&serde_json::to_vec(&downgraded).unwrap()).is_err());
    let mut wrong_attempt = current.clone();
    wrong_attempt["catalog"]["image_install_intents"][range.get().to_string()]
        ["registered_proposal"]["attempt_id"] = serde_json::to_value(Uuid::now_v7()).unwrap();
    assert!(validate(&serde_json::to_vec(&wrong_attempt).unwrap()).is_err());
    let mut normalized_after = after.clone();
    normalized_after["raw"] = before["raw"].clone();
    assert_eq!(
        normalized_after, before,
        "only durable catalogue refusal changes: no rows/floors/namespace/writer/schema"
    );
    assert_eq!(
        factory
            .database
            .read()
            .test_registered_remote_native_inventory()
            .unwrap(),
        native_before
    );
    assert_eq!(
        factory
            .database
            .read()
            .test_registered_remote_proposal_record(source.manifest().target_attempt)
            .unwrap(),
        proposal_before
    );
    evidence(&output.join(format!("registered-installing-{}.json", range.get())), &serde_json::json!({
        "before":before,"after":after,"protected_proposal":proposal_before,"failed_outcomes":failed_outcomes,
        "errors":failures,"retained_before":retained_before,"retained_after":retained_after,
        "native_before":native_before,"no_rows_or_serving":true,"actual_catalogue_installing":true,
        "hardware_power_loss":false,"actual_injected_io":matches!(case,SourceCase::TargetInstallingDirSyncRetry)
    })).unwrap();
}
