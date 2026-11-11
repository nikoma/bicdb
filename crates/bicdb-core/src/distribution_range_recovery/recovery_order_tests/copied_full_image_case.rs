//! A separate continuation of the actual copied compacted fixture. All opens
//! remain on a copy; sparse constraints, real quorum/history producers and the
//! same native target service authorize every destructive transfer phase.
use super::*;
use crate::distribution_range_consensus::{ImageInstallPurpose, NativeImageInstallRole};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    materialize_actual_current_repair_image, NativeImageInstallFreeze,
};

#[test]
#[ignore = "requires explicit retained actual 1025-ACK root; copies it before full image recovery"]
fn copied_actual_1025_sparse_source_then_full_image_repairs_behind_voter() {
    super::sparse_current_case::run_sparse_current_case(true);
}

fn recipient_role(
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    initiator: &MetadataReadParticipant,
    range: &RangeDescriptor,
    nonce: Uuid,
) -> RecoveryRoleAdmission {
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let witness = providers[&node("n1")]
        .witness_recovery_role(
            initiator,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    RecoveryRoleAdmission::from_witness(
        witness,
        initiator,
        &node("n1"),
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap()
}

pub(super) fn continue_actual_compacted_recovery(
    fixture: &ActualNativeRf3Fixture,
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    runtimes: &BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    initiator: &MetadataReadParticipant,
    range: &RangeDescriptor,
    scope: &RecoveryFenceScope,
    formerly_unknown: &crate::distribution_range_consensus::RangeWriteCommand,
    parent: &CancellationToken,
) -> serde_json::Value {
    let source_id = node("n0");
    let target_id = node("n1");
    let source_service = &fixture.services[&source_id];
    let target_service = &fixture.services[&target_id];
    let source_db = Arc::clone(&fixture.databases[&source_id]);
    let target_db = Arc::clone(&fixture.databases[&target_id]);
    let original_uid = target_db
        .read()
        .local_namespace_identity("items")
        .unwrap()
        .unwrap();
    let original_target_b = target_db.read().current_commit_seq();
    let original_target_native = target_db
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(original_target_native.native_applied_index, Some(0));
    assert_eq!(
        target_service
            .lock()
            .range_write_progress(range.id)
            .last_index,
        0
    );
    let target_obligations = target_service
        .lock()
        .test_image_history_obligations(range.id)
        .unwrap();
    assert_eq!(
        target_obligations.irrevocable_commands(),
        &BTreeMap::from([(1026, formerly_unknown.checksum_sha256.clone())])
    );
    let initial_sparse = runtimes[&target_id]
        .observe_history(
            initiator,
            &RecoveryFencedProbeRequest {
                scope: scope.clone(),
                index: Some(1026),
                budget_ms: 5000,
            },
            parent,
        )
        .unwrap();
    let sparse_sha = initial_sparse
        .sparse_decision()
        .unwrap()
        .checksum_sha256()
        .to_string();

    // A NEW actual fence round observes source1026 and behind0/Sparse1026.
    // The original 1025/pending1026 observations are not relabelled current.
    let mut quorum = RecoveryFenceQuorumRound::begin(
        fresh_role(providers, initiator, range, scope.recovery_nonce),
        parent,
    )
    .unwrap();
    assert!(quorum.scope().same_session(scope));
    for id in [&source_id, &target_id] {
        quorum
            .record(
                runtimes[id]
                    .fence_epoch(
                        initiator,
                        &RecoveryEpochFenceRequest {
                            scope: scope.clone(),
                            budget_ms: 5000,
                        },
                        parent,
                    )
                    .unwrap(),
            )
            .unwrap();
    }
    let authority = Arc::new(quorum.finish().unwrap());
    assert_eq!(authority.observations().len(), 2);
    let frozen = runtimes[&source_id].source_capture_freeze(scope).unwrap();
    let source_ready = source_service
        .lock()
        .attest_native_recovery_ready(
            fresh_role(providers, initiator, range, scope.recovery_nonce),
            Arc::clone(&frozen),
            parent,
        )
        .unwrap();
    assert_eq!(source_ready.range_index(), 1026);
    let expected_h = source_ready.logical_history_sha256().to_string();
    let source_b = source_ready.visibility().get();
    let mut round = source_service
        .lock()
        .begin_native_recovery_source_history(authority, &source_ready, target_id.clone())
        .unwrap();
    let mut done = false;
    for _ in 0..128 {
        if source_service
            .lock()
            .advance_native_recovery_source_history(&mut round, &source_ready, parent)
            .unwrap()
        {
            done = true;
            break;
        }
    }
    assert!(
        done,
        "actual source history0..1026 must finish within its ORIGINAL authority deadline"
    );
    let cut = source_service
        .lock()
        .finish_native_recovery_source_history(round, &source_ready)
        .unwrap();
    let lineage = source_db
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap();
    assert_eq!(lineage.range_index(), 1026);
    assert_eq!(lineage.logical_history_sha256(), expected_h);
    drop(source_ready);
    let mut capture = Some(
        RangeImageCapture::begin(
            Arc::clone(&source_db),
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            parent,
        )
        .unwrap(),
    );
    let attempt = Uuid::now_v7();
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let source = capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    let lease = runtimes[&target_id]
        .transfer_image_install(
            scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            parent,
        )
        .unwrap();
    let install = target_service
        .lock()
        .begin_native_range_image_install(
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            lease,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            parent,
        )
        .unwrap();
    drop(source);
    drop(role);
    let verified = materialize_actual_current_repair_image(
        &mut capture,
        target_service,
        || recipient_role(providers, initiator, range, scope.recovery_nonce),
        || {
            // This executes AFTER complete staged spine/audit, BEFORE clearing
            // any target rows. Sparse1026 is an ACTUAL immutable obligation.
            let current = target_service
                .lock()
                .test_image_history_obligations(range.id)
                .unwrap();
            assert_eq!(
                current.irrevocable_commands(),
                target_obligations.irrevocable_commands()
            );
            target_service
                .lock()
                .test_require_image_history_obligations_current(&target_obligations)
                .unwrap();
            assert_eq!(
                target_db
                    .read()
                    .native_range_prefix_observation(range.id)
                    .unwrap()
                    .native_applied_index,
                Some(0)
            );
        },
        install,
    );
    let target_b = verified.base().target_snapshot_bound().get();
    assert!(target_b > original_target_b);
    assert_eq!(verified.base().range_index(), 1026);
    assert_eq!(verified.base().logical_history_sha256(), expected_h);
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let adopted = target_service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            parent,
        )
        .unwrap();
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let (freeze, finalization) = target_service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            parent,
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("same Runtime must own actual image lease")
    };
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let image = lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            parent,
        )
        .unwrap();
    let target_frozen = runtimes[&target_id].source_capture_freeze(scope).unwrap();
    let prepared = target_service
        .lock()
        .prepare_finalized_native_recovery_image(
            image,
            recipient_role(providers, initiator, range, scope.recovery_nonce),
            Arc::clone(&target_frozen),
            parent,
        )
        .unwrap();
    assert_eq!(prepared.range_index(), 1026);
    assert_eq!(prepared.logical_history_sha256(), expected_h);
    assert_eq!(prepared.local_visibility_sequence(), target_b);
    assert_eq!(
        target_db.read().local_namespace_identity("items").unwrap(),
        Some(original_uid)
    );
    assert_eq!(
        target_db
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        1026
    );
    let mut checked = 0;
    for line in std::fs::read_to_string(
        fixture
            .root
            .path()
            .join("controller-acknowledged-1025.jsonl"),
    )
    .unwrap()
    .lines()
    {
        let ledger: serde_json::Value = serde_json::from_str(line).unwrap();
        let actual = target_db
            .read()
            .get("items", ledger["acknowledged_key"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            ledger["record_sha256"],
            hex::encode(Sha256::digest(serde_json::to_vec(&actual).unwrap()))
        );
        checked += 1;
    }
    assert_eq!(checked, 1025);
    for mutation in &formerly_unknown.mutations {
        assert_eq!(
            target_db
                .read()
                .get(&mutation.collection, &mutation.record_id)
                .unwrap(),
            mutation.record,
            "unknown1026 was resolved by actual decision, never added to ACK ledger"
        );
    }
    // End the manually returned physical pin before the Runtime obtains its
    // own actual re-attestation of the durable Prepared phase; never set its
    // private session slots or downgrade phase to Active.
    drop(prepared);
    drop(target_frozen);
    let ready = runtimes[&target_id]
        .prepare_native_ready(
            initiator,
            &NativeReadyRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000,
            },
            parent,
        )
        .unwrap();
    assert_eq!(ready.reply().range_index, 1026);
    assert_eq!(ready.reply().logical_history_sha256, expected_h);
    assert_eq!(ready.reply().local_visibility_sequence, target_b);
    assert!(
        target_db
            .read()
            .range_mutation_gate()
            .try_enter(range.id)
            .is_err(),
        "Prepared/Ready does not release ordinary writes"
    );
    let after = runtimes[&target_id].observe_history(
        initiator,
        &RecoveryFencedProbeRequest {
            scope: scope.clone(),
            index: Some(1026),
            budget_ms: 5000,
        },
        parent,
    );
    // Generic historical mutation/status remains closed in NativePrepared.
    assert!(after.is_err());
    let report = serde_json::json!({"classification":"ACTUAL_COMPACTED_SPARSE_TO_COMPLETE_TARGET_IMAGE_AND_PREPARED_READY",
        "source_range_index":1026,"target_range_index":ready.reply().range_index,
        "history_sha256":expected_h,"source_local_visibility":source_b,"target_local_visibility":target_b,
        "target_uid_preserved":original_uid,"image_attempt":attempt,"sparse_decision_sha256":sparse_sha,
        "sparse_constraint_present_before_clear":true,"acknowledged_values_reconciled_on_target":checked,
        "unknown_tail_committed_by_actual_history":true,"unknown_tail_new_ack":false,
        "all_history_entries_0_through_1026_staged_and_audited":true,
        "physical_qualification":false,"activation_proven":false,"strong_get_tested":false});
    std::fs::write(
        fixture.root.path().join("full-compacted-image-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("FULL_COMPACTED_IMAGE_RESULT {report}");
    report
}
