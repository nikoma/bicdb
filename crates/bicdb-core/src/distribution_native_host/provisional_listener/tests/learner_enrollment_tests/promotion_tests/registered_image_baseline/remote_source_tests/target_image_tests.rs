//! Genuine fifth: actual Installing -> stored audit -> private row WAL only.
use super::*;
use crate::db::{
    NativeRegisteredRemoteImageOperation as Operation,
    NativeRegisteredRemoteImageProgress as Progress,
};

#[test]
fn native_registered_fifth_remote_image_actual_stored_history_rows_beyond_b0_preserve_other_range()
{
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetImageRows));
}
#[test]
fn native_registered_fifth_remote_image_actual_rows_refuse_without_stored_history_audit() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetImageRowsWithoutAudit),
    );
}
#[test]
fn native_registered_fifth_remote_image_actual_pre_wal_caller_revoked_same_tx_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetImageRowsPreWalCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_image_actual_durable_row_cancel_same_tx_fresh_progress() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetImageRowsPostWalCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_image_actual_inner_durable_apply_failure_retains_pending() {
    const CHILD: &str = "BICDB_REGISTERED_REMOTE_ROW_APPLY_FAILURE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        run_registered_image_baseline_with_source(
            Case::Required,
            Some(SourceCase::TargetImageRowsApplyFailed),
        );
        return;
    }
    // Existing commit-apply env seam is process-global. Keep it in an isolated
    // child so concurrent HA tests cannot receive this intentional failure.
    let module = module_path!().split_once("::").unwrap().1;
    let exact=format!("{module}::native_registered_fifth_remote_image_actual_inner_durable_apply_failure_retains_pending");
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &exact, "--test-threads=1", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "isolated actual post-WAL apply failure test failed: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&result.stderr));
}
#[allow(clippy::too_many_arguments)]
fn advance(
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: &Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    frame: &crate::distribution_range_image::NativeRemoteImageStep<'_>,
    operation: Operation,
    cut: Option<bool>,
) -> Result<Progress> {
    let parent = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &parent).into_handoff()?;
    let business = Uuid::now_v7();
    let own = client.acquire_host_metadata_observation(
        &node(0),
        business,
        Duration::from_secs(5),
        &parent,
    )?;
    let guard = cut.map(|after| {
        let caller = parent.clone();
        crate::db::test_registered_remote_row_wal_cut(after, Box::new(move || caller.cancel()))
    });
    let result = control
        .advance_registered_remote_image(source, frame, role, own, business, &parent, operation);
    drop(guard);
    parent.cancel();
    result
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &serde_json::Value,
    output: &std::path::Path,
) {
    let range = plan.old_range().id;
    let manifest = source.manifest();
    let mut journal = Vec::new();
    let initial = RangeImageHistoryCursorV1::default();
    let frame = source
        .bind_step(
            super::frame(
                client,
                manifest,
                Op::History {
                    cursor: initial.clone(),
                    max_entries: 256,
                },
                output,
                &mut journal,
            )
            .unwrap(),
        )
        .unwrap();
    target_proposal_tests::exercise(
        SourceCase::TargetProposal,
        control,
        client,
        source.clone(),
        &frame,
        plan,
        provider,
        actor,
        factory,
        pending,
        ranges,
        output,
    );
    {
        let caller = budget(5000);
        let role = actual_role(provider, actor, plan, &node(4), &caller)
            .into_handoff()
            .unwrap();
        let business = Uuid::now_v7();
        let own = client
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &caller)
            .unwrap();
        let fresh = source
            .bind_step(
                super::frame(
                    client,
                    manifest,
                    Op::History {
                        cursor: initial.clone(),
                        max_entries: 256,
                    },
                    output,
                    &mut journal,
                )
                .unwrap(),
            )
            .unwrap();
        control
            .publish_registered_remote_installing(&source, &fresh, role, own, business, &caller)
            .unwrap();
        caller.cancel();
    }
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let readers =
        crate::format::required_reader_features(factory.database.read().data_path()).unwrap();
    let mut cursor = initial;
    {
        let fresh = source
            .bind_step(
                super::frame(
                    client,
                    manifest,
                    Op::History {
                        cursor: cursor.clone(),
                        max_entries: 256,
                    },
                    output,
                    &mut journal,
                )
                .unwrap(),
            )
            .unwrap();
        advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            &fresh,
            Operation::BeginHistory,
            None,
        )
        .unwrap();
    }
    if matches!(case, SourceCase::TargetImageRowsWithoutAudit) {
        let actual = source
            .bind_step(
                super::frame(
                    client,
                    manifest,
                    Op::Rows {
                        cursor: RangeImageCursorV1::default(),
                        options: Default::default(),
                    },
                    output,
                    &mut journal,
                )
                .unwrap(),
            )
            .unwrap();
        let error = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            &actual,
            Operation::ApplyRows,
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("full stored history not audited"),
            "{error}"
        );
        assert_eq!(
            state(&factory.database, pending, &node(4), ranges).unwrap(),
            before
        );
        evidence(&output.join("registered-image-missing-audit.json"),&serde_json::json!({"error":error.to_string(),"actual_owner":control.test_registered_remote_image_state(range).unwrap(),"before":before,"no_row_wal":true,"no_image_or_serving":true})).unwrap();
        return;
    }
    loop {
        let fresh = source
            .bind_step(
                super::frame(
                    client,
                    manifest,
                    Op::History {
                        cursor: cursor.clone(),
                        max_entries: 256,
                    },
                    output,
                    &mut journal,
                )
                .unwrap(),
            )
            .unwrap();
        let Progress::History(progress) = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            &fresh,
            Operation::AppendHistory,
            None,
        )
        .unwrap() else {
            panic!("actual history progress expected")
        };
        cursor = progress.cursor;
        if cursor.complete() {
            break;
        }
    }
    loop {
        let fresh = source
            .bind_step(
                super::frame(
                    client,
                    manifest,
                    Op::History {
                        cursor: RangeImageHistoryCursorV1::default(),
                        max_entries: 256,
                    },
                    output,
                    &mut journal,
                )
                .unwrap(),
            )
            .unwrap();
        let Progress::History(progress) = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            &fresh,
            Operation::AuditHistory { max_entries: 256 },
            None,
        )
        .unwrap() else {
            panic!("actual stored audit expected")
        };
        if progress.audited {
            break;
        }
    }
    assert_eq!(
        state(&factory.database, pending, &node(4), ranges).unwrap(),
        before,
        "stored history has no row/visibility authority alone"
    );
    let mut cursor = RangeImageCursorV1::default();
    let mut faulted = false;
    let mut last = None;
    let mut fault_outcomes = Vec::new();
    loop {
        let actual = source
            .bind_step(
                super::frame(
                    client,
                    manifest,
                    Op::Rows {
                        cursor: cursor.clone(),
                        options: Default::default(),
                    },
                    output,
                    &mut journal,
                )
                .unwrap(),
            )
            .unwrap();
        let Payload::Rows(step) = actual.payload().unwrap() else {
            panic!("actual Rows required")
        };
        let cut = if !faulted && step.batch.is_some() {
            match case {
                SourceCase::TargetImageRowsPreWalCanceled => Some(false),
                SourceCase::TargetImageRowsPostWalCanceled => Some(true),
                _ => None,
            }
        } else {
            None
        };
        let fail_apply =
            matches!(case, SourceCase::TargetImageRowsApplyFailed) && step.batch.is_some();
        let apply_failure_guard = fail_apply.then(|| {
            crate::db::test_registered_remote_row_wal_cut(
                false,
                Box::new(|| {
                    std::env::set_var("BICDB_TEST_FAIL_COMMIT_APPLY", "1");
                }),
            )
        });
        let result = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            &actual,
            Operation::ApplyRows,
            cut,
        );
        drop(apply_failure_guard);
        if fail_apply {
            std::env::remove_var("BICDB_TEST_FAIL_COMMIT_APPLY");
            let error = result.unwrap_err();
            assert!(
                matches!(&error, BicDbError::CommitOutcomeUnknown { .. }),
                "{error}"
            );
            assert!(
                error
                    .to_string()
                    .contains("injected post-decision materialization failure"),
                "{error}"
            );
            let retained = control.test_registered_remote_image_state(range).unwrap();
            assert_eq!(
                retained["rows"]["pending"]["logical_outcome"],
                "wal_durable_pending_apply"
            );
            assert!(
                retained["rows"]["pending"]["logical_sequence"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            assert!(!retained["rows"]["pending"]["intent"]["logical_txid"].is_null());
            assert_eq!(
                retained["rows"]["pending"]["progress_attempt"],
                serde_json::Value::Null
            );
            assert_eq!(
                retained["rows"]["progress"]["last_actual_target_b"],
                serde_json::Value::Null
            );
            let fenced = factory.database.read().check_recovery_state().unwrap_err();
            assert!(factory
                .database
                .read()
                .get(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    acknowledged["record_id"].as_str().unwrap()
                )
                .is_err());
            let fresh = source
                .bind_step(
                    super::frame(
                        client,
                        manifest,
                        Op::Rows {
                            cursor: cursor.clone(),
                            options: Default::default(),
                        },
                        output,
                        &mut journal,
                    )
                    .unwrap(),
                )
                .unwrap();
            let retry_error = advance(
                control,
                client,
                &source,
                plan,
                provider,
                actor,
                &fresh,
                Operation::ApplyRows,
                None,
            )
            .unwrap_err();
            assert_eq!(
                control.test_registered_remote_image_state(range).unwrap(),
                retained,
                "fenced retry must not replace original Tx/intent or advance rows"
            );
            evidence(&output.join("registered-image-inner-durable-apply-failure.json"),&serde_json::json!({
                "exact_error":error.to_string(),"fenced_error":fenced.to_string(),"fresh_retry_error":retry_error.to_string(),"retained":retained,
                "existing_failure_seam":"BICDB_TEST_FAIL_COMMIT_APPLY","durable_logical_wal_proven_at_actual_inner_boundary":true,"apply_complete":false,"row_progress_complete":false,
                "no_reopen_no_new_transaction_no_native_floor":true,"hardware_or_serving_claim":false})).unwrap();
            return;
        }
        let progress = if let Some(after) = cut {
            faulted = true;
            let error = result.unwrap_err();
            assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
            let retained = control.test_registered_remote_image_state(range).unwrap();
            let txid = retained["rows"]["pending"]["intent"]["logical_txid"].clone();
            assert!(!txid.is_null());
            let intent = retained["rows"]["pending"]["intent"].clone();
            if after {
                assert_eq!(
                    retained["rows"]["pending"]["logical_outcome"],
                    "applied_known"
                );
                assert!(!retained["rows"]["pending"]["logical_sequence"].is_null());
                assert_eq!(
                    retained["rows"]["pending"]["actual_transaction_retained"],
                    false
                );
            } else {
                assert_eq!(
                    retained["rows"]["pending"]["logical_outcome"],
                    "not_attempted"
                );
                assert_eq!(
                    retained["rows"]["pending"]["actual_transaction_retained"],
                    true
                );
                assert_eq!(factory.database.read().current_commit_seq(), 0);
            }
            let b_before = factory.database.read().current_commit_seq();
            let fresh = source
                .bind_step(
                    super::frame(
                        client,
                        manifest,
                        Op::Rows {
                            cursor: cursor.clone(),
                            options: Default::default(),
                        },
                        output,
                        &mut journal,
                    )
                    .unwrap(),
                )
                .unwrap();
            let progress = advance(
                control,
                client,
                &source,
                plan,
                provider,
                actor,
                &fresh,
                Operation::ApplyRows,
                None,
            )
            .unwrap();
            if after {
                assert_eq!(factory.database.read().current_commit_seq(),b_before,"reconcile known durable row commit must not issue a second logical transaction");
            }
            let completed = control.test_registered_remote_image_state(range).unwrap();
            assert_eq!(completed["rows"]["pending"], serde_json::Value::Null);
            fault_outcomes.push(serde_json::json!({"exact_error":error.to_string(),"retained":retained,"original_intent":intent,"original_txid":txid,"completed":completed,"known_durable_case":after}));
            progress
        } else {
            result.unwrap()
        };
        let Progress::Rows(progress) = progress else {
            panic!("actual row progress expected")
        };
        cursor = progress.cursor.clone();
        let complete = progress.complete;
        last = Some(progress);
        if complete {
            break;
        }
    }
    if matches!(
        case,
        SourceCase::TargetImageRowsPreWalCanceled | SourceCase::TargetImageRowsPostWalCanceled
    ) {
        assert!(faulted);
    }
    let final_frame = source
        .bind_step(super::frame(client, manifest, Op::Finish, output, &mut journal).unwrap())
        .unwrap();
    let Payload::Finished(seal) = final_frame.payload().unwrap() else {
        panic!("actual complete source seal required")
    };
    let rows = last.unwrap();
    assert!(rows.complete);
    assert_eq!(rows.records, seal.record_count);
    assert_eq!(rows.bytes, seal.serialized_record_bytes);
    assert_eq!(rows.content_sha256, seal.image_root_sha256);
    assert!(
        factory.database.read().current_commit_seq() > 0,
        "actual row WAL must move beyond B0"
    );
    let record = factory
        .database
        .read()
        .get(
            crate::CLUSTER_EVENTS_COLLECTION,
            acknowledged["record_id"].as_str().unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&record).unwrap(),
        acknowledged["actual_full_record"]
    );
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert_eq!(
        crate::format::required_reader_features(factory.database.read().data_path()).unwrap(),
        readers,
        "local image reader semantics must not change portable schema"
    );
    // The other range retains its exact raw refusal/native points and no
    // copied record outside this source range may appear on the fifth.
    assert_eq!(after["raw"], before["raw"]);
    let other = ranges
        .iter()
        .find(|descriptor| descriptor.id != range)
        .unwrap()
        .id
        .to_string();
    assert!(!before["points"][&other].is_null());
    assert_eq!(after["points"][&other], before["points"][&other]);
    for field in [
        "namespace_ids",
        "schema",
        "birth",
        "open",
        "slots",
        "protected_catalogue_uuid",
    ] {
        assert_eq!(after[field], before[field]);
    }
    assert_eq!(after["writer_installed"], false);
    for (collection, records) in after["rows"].as_object().unwrap() {
        for record in records.as_array().unwrap() {
            assert!(
                manifest
                    .capture_range
                    .contains_token(crate::distribution::distribution_key_token(
                        collection,
                        record["id"].as_str().unwrap()
                    )),
                "row escaped selected image range"
            );
        }
    }
    evidence(&output.join("registered-image-history-and-rows.json"),&serde_json::json!({"before":before,"after":after,"actual_owner":control.test_registered_remote_image_state(range).unwrap(),"rows":rows,"actual_source_seal":seal,"fault_outcomes":fault_outcomes,"acknowledged_record_verified_locally":record,"strong_read_or_serving_proven":false,"cold_recovery_or_physical_qualification":false})).unwrap();
}
