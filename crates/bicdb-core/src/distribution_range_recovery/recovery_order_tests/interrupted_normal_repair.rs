//! Actual two-command interrupted NormalRepair. A passing characterization
//! records FAILED/unsupported recovery, not successful crash qualification.
use super::copied_full_image_case::recipient_role;
use super::small_sparse_cold::{fence_pair, make_runtimes};
use super::*;
use crate::distribution_range_consensus::{ImageInstallPurpose, NativeImageInstallRole};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::stage_and_clear_actual_current_repair_image;

fn save(root: &std::path::Path, name: &str, value: &serde_json::Value) {
    let path = root.join(name);
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    std::fs::File::open(root).unwrap().sync_all().unwrap();
}
fn catalogue(root: &std::path::Path, id: &ClusterNodeId) -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(root.join(id.as_str()).join("cluster-range-write-log.json")).unwrap(),
    )
    .unwrap()
}
fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Clone, Copy, Debug)]
enum ImageStop {
    CatalogueBeforeMarker,
    BeginBeforeStage,
    AfterClear,
}
#[test]
fn actual_interrupted_normal_image_cold_resume_is_characterized_without_forged_cut() {
    exercise_interrupted_normal_image(ImageStop::AfterClear, false);
}
#[test]
fn actual_protected_current_image_resumes_after_clear_with_fresh_installing_quorum() {
    exercise_interrupted_normal_image(ImageStop::AfterClear, true);
}
#[test]
fn actual_protected_current_image_resumes_begin_before_history_staging() {
    exercise_interrupted_normal_image(ImageStop::BeginBeforeStage, true);
}
#[test]
fn actual_protected_current_image_resumes_intent_before_native_marker() {
    exercise_interrupted_normal_image(ImageStop::CatalogueBeforeMarker, true);
}
fn exercise_interrupted_normal_image(stop: ImageStop, resume: bool) {
    let mut fixture = ActualNativeRf3Fixture::new();
    fixture.root.disable_cleanup(true);
    fixture.transport.enable_native_delivery();
    fixture.transport.drop_native_apply_once(node("n2"), 1);
    fixture.transport.set_unavailable(&node("n1"), true);
    eprintln!(
        "INTERRUPTED_NORMAL_ORIGINAL_ROOT {}",
        fixture.root.path().display()
    );
    let source_id = node("n0");
    let target_id = node("n1");
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let acknowledged = "interrupted-normal-ack-1";
    let unknown = "interrupted-normal-uncertain-2";
    fixture.append(acknowledged).unwrap();
    let acknowledged_record = fixture.databases[&source_id]
        .read()
        .get("items", acknowledged)
        .unwrap()
        .unwrap();
    let acknowledged_sha = sha(&serde_json::to_vec(&acknowledged_record).unwrap());
    let mut ledger =
        std::fs::File::create(fixture.root.path().join("controller-acknowledged.jsonl")).unwrap();
    writeln!(
        ledger,
        "{}",
        serde_json::json!({"acknowledged_key":acknowledged,"record_sha256":acknowledged_sha})
    )
    .unwrap();
    ledger.sync_all().unwrap();
    drop(ledger);
    std::fs::File::open(fixture.root.path())
        .unwrap()
        .sync_all()
        .unwrap();
    assert_eq!(
        fixture.services[&target_id]
            .lock()
            .range_write_progress(range.id)
            .last_index,
        0
    );
    BicDb::test_reject_next_native_after_admission();
    let unknown_error = fixture.append(unknown).unwrap_err();
    assert!(
        matches!(
            unknown_error,
            crate::BicDbError::CommitOutcomeUnknown { .. }
        ),
        "{unknown_error}"
    );
    let tail = fixture.services[&source_id]
        .lock()
        .probe_range_write_for_peer(&range, &source_id, 2)
        .unwrap()
        .entry
        .unwrap();
    assert_eq!(tail.state, RangeWriteState::QuorumCommitted);
    let pending = fixture.databases[&source_id]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(pending.native_applied_index, Some(1));
    assert_eq!(pending.pending_command_index, Some(2));
    fixture.transport.set_unavailable(&target_id, false);
    fixture.transport.set_unavailable(&node("n2"), true);
    let topology = fixture.topology.read().clone();
    let providers = test_recovery_metadata_cluster(
        &fixture.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let initiator = providers[&source_id].local_participant();
    let runtimes = make_runtimes(&fixture, &providers);
    let nonce = Uuid::now_v7();
    let role = fresh_role(&providers, &initiator, &range, nonce);
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    let parent = CancellationToken::uncancelable();
    fence_pair(&runtimes, &initiator, &scope);
    let request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command: tail.command.clone(),
        budget_ms: 5000,
    };
    runtimes[&source_id]
        .certify_historical(&initiator, &request, &parent)
        .unwrap();
    let sparse_observation = runtimes[&target_id]
        .certify_sparse_historical(&initiator, &request, &parent)
        .unwrap();
    assert_eq!(sparse_observation.fence().progress().last_index, 0);
    assert_eq!(
        sparse_observation.fence().native().native_applied_index,
        Some(0)
    );
    let sparse_sha = sparse_observation
        .sparse_decision()
        .unwrap()
        .checksum_sha256()
        .to_owned();
    let applied = runtimes[&source_id]
        .apply_historical(&initiator, &request, &parent)
        .unwrap();
    assert_eq!(applied.fence().native().native_applied_index, Some(2));

    let mut quorum =
        RecoveryFenceQuorumRound::begin(fresh_role(&providers, &initiator, &range, nonce), &parent)
            .unwrap();
    for id in [&source_id, &target_id] {
        quorum
            .record(
                runtimes[id]
                    .fence_epoch(
                        &initiator,
                        &RecoveryEpochFenceRequest {
                            scope: scope.clone(),
                            budget_ms: 5000,
                        },
                        &parent,
                    )
                    .unwrap(),
            )
            .unwrap();
    }
    let authority = Arc::new(quorum.finish().unwrap());
    let frozen = runtimes[&source_id].source_capture_freeze(&scope).unwrap();
    let source_ready = fixture.services[&source_id]
        .lock()
        .attest_native_recovery_ready(
            fresh_role(&providers, &initiator, &range, nonce),
            Arc::clone(&frozen),
            &parent,
        )
        .unwrap();
    assert_eq!(source_ready.range_index(), 2);
    let source_h = source_ready.logical_history_sha256().to_owned();
    let source_service = &fixture.services[&source_id];
    let target_service = &fixture.services[&target_id];
    let mut history = source_service
        .lock()
        .begin_native_recovery_source_history(authority, &source_ready, target_id.clone())
        .unwrap();
    let mut finished = false;
    for _ in 0..16 {
        if source_service
            .lock()
            .advance_native_recovery_source_history(&mut history, &source_ready, &parent)
            .unwrap()
        {
            finished = true;
            break;
        }
    }
    assert!(finished);
    let cut = source_service
        .lock()
        .finish_native_recovery_source_history(history, &source_ready)
        .unwrap();
    let source_db = Arc::clone(&fixture.databases[&source_id]);
    let target_db = Arc::clone(&fixture.databases[&target_id]);
    let lineage = source_db
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap();
    drop(source_ready);
    let mut capture = Some(
        RangeImageCapture::begin(
            Arc::clone(&source_db),
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            &parent,
        )
        .unwrap(),
    );
    let target_uid = target_db
        .read()
        .local_namespace_identity("items")
        .unwrap()
        .unwrap();
    let role = recipient_role(&providers, &initiator, &range, nonce);
    let source = capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    let attempt = Uuid::now_v7();
    let lease = runtimes[&target_id]
        .transfer_image_install(
            &scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &parent,
        )
        .unwrap();
    let mut lease = Some(lease);
    let mut install = if matches!(stop, ImageStop::CatalogueBeforeMarker) {
        let fence = target_service
            .lock()
            .test_current_image_catalogue_boundary(
                &source,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                lease.as_ref().unwrap().as_frozen(),
                attempt,
                false,
            )
            .unwrap();
        assert_eq!(fence.attempt_id(), attempt);
        None
    } else {
        Some(
            target_service
                .lock()
                .begin_native_range_image_install(
                    &source,
                    &NativeImageInstallRole::CurrentVoterRepair(&role),
                    lease.take().unwrap(),
                    ImageInstallPurpose::CurrentVoterRepair,
                    attempt,
                    &parent,
                )
                .unwrap(),
        )
    };
    drop(source);
    drop(role);
    let obligations = target_service
        .lock()
        .test_image_history_obligations(range.id)
        .unwrap();
    assert_eq!(
        obligations.irrevocable_commands(),
        &BTreeMap::from([(2, tail.command.checksum_sha256.clone())])
    );
    let stage_id = install.as_ref().map(|i| i.history_stage_id());
    if matches!(stop, ImageStop::AfterClear) {
        stage_and_clear_actual_current_repair_image(
            &mut capture,
            target_service,
            &|| recipient_role(&providers, &initiator, &range, nonce),
            || {
                target_service
                    .lock()
                    .test_require_image_history_obligations_current(&obligations)
                    .unwrap()
            },
            install.as_mut().unwrap(),
        );
    }
    // Stop at the EXACT requested boundary: stage0..2 audited, clear complete,
    // NO row copy, no completed source, no installed base or catalogue adoption.
    let partial = target_db
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let marker_expected = !matches!(stop, ImageStop::CatalogueBeforeMarker);
    assert_eq!(
        partial.native_applied_index,
        if marker_expected { None } else { Some(0) }
    );
    assert_eq!(partial.invalidated, marker_expected);
    assert!(target_db
        .read()
        .get("items", acknowledged)
        .unwrap()
        .is_none());
    assert_eq!(
        target_db.read().local_namespace_identity("items").unwrap(),
        Some(target_uid)
    );
    let stage_sha = target_db
        .read()
        .native_history_stage_state_commitment(range.id)
        .unwrap();
    let old_native = target_db
        .read()
        .native_recovery_admission(
            &topology.cluster_id,
            topology.range_image_bootstrap_id.unwrap(),
            &target_id,
            topology.nodes[&target_id].incarnation,
        )
        .unwrap();
    assert!(target_db
        .read()
        .native_installed_range_image_integrity(&old_native, range.id)
        .unwrap()
        .is_none());
    let protected = target_db
        .read()
        .read_native_current_image_proposal_by_id(&old_native, attempt)
        .unwrap()
        .unwrap();
    let protected_pointer = protected.pointer();
    drop(protected);
    let old_open = old_native.database_incarnation().to_string();
    let birth = old_native.birth_id();
    let birth_sha = old_native.birth_sha256().to_owned();
    let local_b = target_db.read().current_commit_seq();
    let source_identity = {
        let db = source_db.read();
        let native = db
            .native_recovery_admission(
                &topology.cluster_id,
                topology.range_image_bootstrap_id.unwrap(),
                &source_id,
                topology.nodes[&source_id].incarnation,
            )
            .unwrap();
        let lineage = db.current_native_range_source_lineage(range.id).unwrap();
        serde_json::json!({
            "birth":native.birth_id(),"birth_sha256":native.birth_sha256(),
            "db_open":native.database_incarnation(),"db_sequence_observed":db.current_commit_seq(),
            "items_local_uid":db.local_namespace_identity("items").unwrap(),
            "native_range_index":lineage.range_index(),"native_receipt_sha256":lineage.native_receipt_sha256(),
            "logical_history_sha256":lineage.logical_history_sha256()
        })
    };
    let phase = catalogue(fixture.root.path(), &target_id);
    let key = range.id.get().to_string();
    assert_eq!(
        phase["catalog"]["image_install_intents"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert_eq!(
        phase["catalog"]["sparse_committed_decisions"][&key]["2"]["checksum_sha256"],
        sparse_sha
    );
    assert_eq!(phase["catalog"]["ranges"][&key]["last_index"], 0);
    save(
        fixture.root.path(),
        "interrupted-before-close.json",
        &serde_json::json!({
            "stage":format!("{stop:?}"),"resume_requested":resume,"protected_proposal":protected_pointer, "source_native":2,"target_native":partial,
            "target_birth":birth,"target_birth_sha256":birth_sha,"target_open":old_open,
            "target_uid":target_uid,"target_local_db_sequence":local_b,"image_attempt":attempt,
            "history_stage_id":stage_id,"history_stage_sha256":stage_sha,"sparse_sha256":sparse_sha,
            "source_logical_history_sha256":source_h,"source_identity":source_identity,"ack_record_sha256":acknowledged_sha,
            "scope":scope,"catalogue":phase,"uncertain_call":unknown_error.to_string()
        }),
    );
    let original_root = fixture.root.path().to_path_buf();
    let original_catalogues = [source_id.clone(), target_id.clone(), node("n2")]
        .into_iter()
        .map(|id| {
            let bytes = std::fs::read(
                original_root
                    .join(id.as_str())
                    .join("cluster-range-write-log.json"),
            )
            .unwrap();
            (id, bytes)
        })
        .collect::<BTreeMap<_, _>>();
    let weak = fixture
        .databases
        .values()
        .map(Arc::downgrade)
        .collect::<Vec<_>>();
    drop(old_native);
    drop(install);
    drop(lease);
    drop(capture);
    drop(obligations);
    drop(sparse_observation);
    drop(applied);
    drop(source_db);
    drop(target_db);
    wait_for_contexts(&runtimes);
    drop(runtimes);
    drop(providers);
    drop(fixture);
    let deadline = Instant::now() + Duration::from_secs(5);
    while weak.iter().any(|db| db.upgrade().is_some()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        weak.iter().all(|db| db.upgrade().is_none()),
        "ALL owners must really close before copying"
    );
    let copied = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&original_root).unwrap();
    let mut cold = ActualNativeRf3Fixture::reopen_copied_recovery_state(copied).unwrap();
    cold.root.disable_cleanup(true);
    eprintln!(
        "INTERRUPTED_NORMAL_COPIED_ROOT {}",
        cold.root.path().display()
    );
    let cold_target = &cold.databases[&target_id];
    let native = cold_target
        .read()
        .native_recovery_admission(
            &topology.cluster_id,
            topology.range_image_bootstrap_id.unwrap(),
            &target_id,
            topology.nodes[&target_id].incarnation,
        )
        .unwrap();
    assert_eq!(native.birth_id(), birth);
    assert_eq!(native.birth_sha256(), birth_sha);
    assert_ne!(native.database_incarnation(), old_open);
    let new_open = native.database_incarnation().to_string();
    assert_eq!(
        cold_target
            .read()
            .local_namespace_identity("items")
            .unwrap(),
        Some(target_uid)
    );
    assert_eq!(cold_target.read().current_commit_seq(), local_b);
    assert_eq!(
        cold_target
            .read()
            .native_history_stage_state_commitment(range.id)
            .unwrap(),
        stage_sha.clone()
    );
    assert!(cold_target
        .read()
        .native_installed_range_image_integrity(&native, range.id)
        .unwrap()
        .is_none());
    let cold_native = cold_target
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(
        cold_native.native_applied_index,
        if marker_expected { None } else { Some(0) }
    );
    let cold_phase = catalogue(cold.root.path(), &target_id);
    assert_eq!(
        cold_phase["catalog"]["image_install_intents"][&key],
        phase["catalog"]["image_install_intents"][&key]
    );
    assert_eq!(
        cold_phase["catalog"]["sparse_committed_decisions"][&key]["2"]["checksum_sha256"],
        sparse_sha
    );
    cold.transport.set_unavailable(&node("n2"), true);
    let providers = test_recovery_metadata_cluster(
        &cold.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let initiator = providers[&source_id].local_participant();
    let runtimes = make_runtimes(&cold, &providers);
    // Actual independently fresh metadata quorum exists for installing n1.
    let mq_role = recipient_role(&providers, &initiator, &range, nonce);
    let metadata_term = mq_role.metadata_term();
    let metadata_index = mq_role.metadata_read_index();
    assert!(metadata_term > 0);
    assert_eq!(mq_role.current_range().id, range.id);
    drop(mq_role);
    let source_fence = runtimes[&source_id]
        .fence_epoch(
            &initiator,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    assert_eq!(source_fence.native().native_applied_index, Some(2));
    let target_error = runtimes[&target_id]
        .fence_epoch(
            &initiator,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap_err();
    assert!(
        target_error
            .to_string()
            .contains("durable native image installation is incomplete"),
        "{target_error}"
    );
    let mut round =
        RecoveryFenceQuorumRound::begin(fresh_role(&providers, &initiator, &range, nonce), &parent)
            .unwrap();
    let needed = round.required_quorum();
    assert_eq!(needed, 2);
    round.record(source_fence).unwrap();
    let quorum_error = round.finish().unwrap_err();
    assert!(
        quorum_error
            .to_string()
            .contains("no distinct current-data-voter epoch-fence majority"),
        "{quorum_error}"
    );
    let missing_session = runtimes[&target_id]
        .source_capture_freeze(&scope)
        .unwrap_err();
    assert!(
        missing_session
            .to_string()
            .contains("no retained recovery source freeze"),
        "{missing_session}"
    );
    let ready_error = runtimes[&target_id]
        .prepare_native_ready(
            &initiator,
            &NativeReadyRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap_err();
    assert!(
        ready_error.to_string().contains("image installation"),
        "{ready_error}"
    );
    assert!(crate::distribution_range_consensus::RangeWriteStore::require_no_native_image_install_on_disk(&cold.root.path().join(target_id.as_str()), range.id).is_err());
    let frozen = runtimes[&source_id].source_capture_freeze(&scope).unwrap();
    let source_ready = cold.services[&source_id]
        .lock()
        .attest_native_recovery_ready(
            fresh_role(&providers, &initiator, &range, nonce),
            frozen,
            &parent,
        )
        .unwrap();
    assert_eq!(source_ready.range_index(), 2);
    assert_eq!(source_ready.logical_history_sha256(), source_h);
    let cold_source_identity = {
        let db = cold.databases[&source_id].read();
        let native = db
            .native_recovery_admission(
                &topology.cluster_id,
                topology.range_image_bootstrap_id.unwrap(),
                &source_id,
                topology.nodes[&source_id].incarnation,
            )
            .unwrap();
        assert_ne!(
            Some(native.database_incarnation()),
            source_identity["db_open"].as_str()
        );
        assert_eq!(
            serde_json::to_value(native.birth_id()).unwrap(),
            source_identity["birth"]
        );
        assert_eq!(
            serde_json::to_value(db.local_namespace_identity("items").unwrap()).unwrap(),
            source_identity["items_local_uid"]
        );
        assert_eq!(
            source_ready.native_receipt_sha256(),
            source_identity["native_receipt_sha256"].as_str().unwrap()
        );
        serde_json::json!({"birth":native.birth_id(),"db_open":native.database_incarnation(),
            "db_sequence_observed":db.current_commit_seq(),"items_local_uid":db.local_namespace_identity("items").unwrap(),
            "actual_native_visibility_bound":source_ready.visibility().get(),"native_range_index":source_ready.range_index()})
    };
    let actual = cold.databases[&source_id]
        .read()
        .get("items", acknowledged)
        .unwrap()
        .unwrap();
    assert_eq!(sha(&serde_json::to_vec(&actual).unwrap()), acknowledged_sha);
    for mutation in &tail.command.mutations {
        assert_eq!(
            cold.databases[&source_id]
                .read()
                .get(&mutation.collection, &mutation.record_id)
                .unwrap()
                .as_deref(),
            mutation.record.as_ref()
        );
    }
    assert_eq!(
        std::fs::read_to_string(cold.root.path().join("controller-acknowledged.jsonl"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    for (id, bytes) in &original_catalogues {
        assert_eq!(
            &std::fs::read(
                original_root
                    .join(id.as_str())
                    .join("cluster-range-write-log.json")
            )
            .unwrap(),
            bytes
        );
    }
    let resumed = if resume {
        drop(source_ready);
        Some(
            super::protected_resume_positive::finish_actual_current_image_resume(
                &cold,
                &providers,
                &runtimes,
                &initiator,
                &range,
                &scope,
                protected_pointer.clone(),
                &tail.command,
                acknowledged,
                &acknowledged_sha,
                target_uid,
                &source_h,
                &parent,
            ),
        )
    } else {
        None
    };
    wait_for_contexts(&runtimes);
    let report = serde_json::json!({
        "characterization_test_passed":true,"recovery_scenario":if resume {"RECOVERED_TO_NATIVE_PREPARED_READY_USING_EXPLICIT_FRESH_INSTALLING_PROTOCOL"} else {"ORDINARY_PATH_REFUSES_INTERRUPTED_IMAGE_USE_EXPLICIT_INSTALLING_PROTOCOL"},
        "stop":format!("{stop:?}"),"purpose_specific_resume":resumed,"ordinary_path_still_refuses":true,
        "evidence_kind":"actual_in_process_protocol_storage_and_strict_copied_reopen",
        "original_root":original_root,"copied_root":cold.root.path(),"source_actual_native":2,
        "source_logical_history_sha256":source_h,"source_identity_before_close":source_identity,
        "source_identity_after_close":cold_source_identity,"target_actual_native":cold_native,
        "target_raw_last":0,"target_sparse_index":2,"sparse_sha256":sparse_sha,
        "target_uid":target_uid,"target_birth":birth,"target_birth_sha256":birth_sha,
        "original_target_open":old_open,"current_target_open":new_open,"target_local_db_sequence":local_b,
        "image_attempt":attempt,"stage_id":stage_id,"stage_sha256":stage_sha,"published_image_base_present":false,
        "fresh_metadata_term":metadata_term,"fresh_metadata_read_index":metadata_index,
        "fresh_data_fence_observations":1,"required_data_quorum":needed,
        "target_fence_error":target_error.to_string(),"data_quorum_error":quorum_error.to_string(),
        "runtime_session_error":missing_session.to_string(),"target_ready_error":ready_error.to_string(),
        "new_current_source_cut_constructed":resume,"original_catalogues_unchanged":true,
        "acknowledged_values_reconciled_on_source":1,"unknown2_resolved_on_source_but_not_new_ack":true,
        "intent_cleared":false,"new_genesis_created_after_reopen":false,"stale_authority_reused":false,
        "activation_proven":false,"physical_qualification":false,"strong_get_tested":false
    });
    save(
        cold.root.path(),
        "interrupted-normal-cold-result.json",
        &report,
    );
    eprintln!("INTERRUPTED_NORMAL_COLD_RESULT {report}");
}
