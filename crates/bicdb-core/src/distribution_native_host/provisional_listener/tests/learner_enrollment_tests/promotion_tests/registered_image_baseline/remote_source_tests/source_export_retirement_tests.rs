//! Actual S2 Start and TWO distinct pinned-mTLS retirement hops. No test hands
//! a source retainer, target owner, local NEW or certificate to the donor RPC.
use super::*;
mod callback_liveness_tests;
use crate::distribution_range_image::{
    NativeSourceDiscoveryOperationV1 as DiscoveryOp, NativeSourceDiscoveryRequestV1,
    NativeSourceExportOperationV1 as ExportOp, NativeSourceExportRequestV1,
};

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetSourceExportRetire
            | SourceCase::TargetSourceExportReplyLost
            | SourceCase::TargetSourceExportStartReplyLost
            | SourceCase::TargetSourceExportWrongActor
            | SourceCase::TargetSourceExportWrongManifest
            | SourceCase::TargetSourceExportWrongAttempt
            | SourceCase::TargetSourceExportWrongBirth
            | SourceCase::TargetSourceExportTargetStoreABA
            | SourceCase::TargetSourceExportOriginalSourceCanceled
            | SourceCase::TargetSourceExportNativeUnknown
            | SourceCase::TargetSourceExportTermAtDetach
            | SourceCase::TargetSourceExportExpiryAtDetach
            | SourceCase::TargetSourceExportProcessCancelAtDetach
            | SourceCase::TargetSourceExportCallbackCancelAtDetach
            | SourceCase::TargetSourceExportCallbackExpiryAtDetach
    )
}
macro_rules! actual_case {
    ($name:ident, $case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$case));
        }
    };
}
actual_case!(
    native_registered_fifth_source_export_retirement_actual_two_mtls_hops_nonleader_donor,
    TargetSourceExportRetire
);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_lost_retire_reply_same_terminal_custody,
    TargetSourceExportReplyLost
);
actual_case!(native_registered_fifth_source_export_retirement_actual_lost_start_reply_original_two_second_request, TargetSourceExportStartReplyLost);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_wrong_actor_refuses_before_drop,
    TargetSourceExportWrongActor
);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_wrong_manifest_refuses_before_drop,
    TargetSourceExportWrongManifest
);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_wrong_a_refuses_before_drop,
    TargetSourceExportWrongAttempt
);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_target_store_aba_refuses_before_drop,
    TargetSourceExportTargetStoreABA
);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_original_source_process_cancel_refuses,
    TargetSourceExportOriginalSourceCanceled
);
actual_case!(
    native_registered_fifth_source_export_retirement_actual_unknown_native_base_has_no_new_owner,
    TargetSourceExportNativeUnknown
);

actual_case!(native_registered_fifth_source_export_retirement_actual_local_term_change_at_final_detach_refuses, TargetSourceExportTermAtDetach);

actual_case!(
    native_registered_fifth_source_export_retirement_actual_wrong_native_birth_refuses_before_drop,
    TargetSourceExportWrongBirth
);

actual_case!(
    native_registered_fifth_source_export_retirement_actual_original_own_expires_at_final_detach,
    TargetSourceExportExpiryAtDetach
);
actual_case!(native_registered_fifth_source_export_retirement_actual_original_source_process_canceled_at_final_detach, TargetSourceExportProcessCancelAtDetach);

fn call<T: serde::Serialize>(
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
    operation: &str,
    request: &impl serde::Serialize,
    invoke: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let index = ledger.len();
    ledger
        .push(serde_json::json!({"operation":operation,"request":request,"outcome":"not_started"}));
    evidence(&output.join("source-export-retirement-ledger.json"), ledger)?;
    let result = invoke();
    ledger[index]["outcome"] = if result.is_ok() {
        "returned"
    } else {
        "error_unknown_effect"
    }
    .into();
    ledger[index]["exact_error"] =
        serde_json::to_value(result.as_ref().err().map(|e| format!("{e:?}")))?;
    if let Ok(reply) = &result {
        ledger[index]["actual_reply"] = serde_json::to_value(reply)?;
    }
    let saved = evidence(&output.join("source-export-retirement-ledger.json"), ledger);
    match result {
        Err(primary) => {
            let _ = saved;
            Err(primary)
        }
        Ok(reply) => {
            saved?;
            Ok(reply)
        }
    }
}
fn own(
    client: &TcpClusterRelocationTransport,
    parent: &CancellationToken,
) -> (
    crate::distribution_consensus::HostMetadataReadObservation,
    Uuid,
) {
    let business = Uuid::now_v7();
    (
        client
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), parent)
            .unwrap(),
        business,
    )
}
fn old_records(
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    plan: &NativeJointHandoffPlan,
) -> serde_json::Value {
    serde_json::to_value(
        plan.old_members()
            .keys()
            .map(|id| {
                let index = (0..4).find(|i| node(*i) == *id).unwrap();
                let db = services[index].lock().database();
                let record = db
                    .read()
                    .test_native_old_close_record(plan.old_range().id)
                    .unwrap()
                    .unwrap();
                (id.to_string(), record)
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .unwrap()
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    target_control: &mut NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    process: &CancellationToken,
) {
    assert!(is_case(case));
    assert_eq!(ranges.len(), 2);
    assert_eq!(acknowledged.len(), 2);
    for control in controls.iter() {
        control.bind_initial_image_source_process(process).unwrap();
    }
    let selected = pending.range_by_id(ranges[0].id).unwrap();
    let leader_index = (0..4).find(|i| node(*i) == selected.leader).unwrap();
    let leader_actor = clients[leader_index].local_metadata_read_participant();
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let read = clients[leader_index]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            selected.id,
            selected.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let relocation = pending
        .relocations
        .values()
        .find(|p| p.range_id == selected.id)
        .unwrap();
    let round = NativeJointOldRound::begin(read, business, relocation.id).unwrap();
    let plan = round.plan();
    drop(round); // Never finish a local OLD certificate.
    let mut journal = Vec::new();
    assert!(remote_old_seed(
        case,
        target,
        controls,
        clients,
        services,
        metadata,
        &plan,
        process,
        output,
        &mut journal
    )
    .is_none());
    let donor_id = plan
        .old_members()
        .keys()
        .find(|id| **id != selected.leader && *id != plan.retiring_source())
        .unwrap();
    let donor = (0..4).find(|i| node(*i) == *donor_id).unwrap();
    assert_ne!(node(donor), selected.leader);
    assert_ne!(&node(donor), plan.retiring_source());
    let native_before = old_records(services, &plan);
    let original_target = state(&factory.database, pending, &node(4), ranges).unwrap();
    let mut ledger = Vec::new();
    let attempt = Uuid::now_v7();
    let start_parent = budget(
        if matches!(case, SourceCase::TargetSourceExportStartReplyLost) {
            2000
        } else {
            5000
        },
    );
    let (start_own, business) = own(target, &start_parent);
    let start = NativeSourceDiscoveryRequestV1::from_plan(
        &plan,
        node(donor),
        node(4),
        attempt,
        DiscoveryOp::Start,
        &start_own,
        business,
        if matches!(case, SourceCase::TargetSourceExportStartReplyLost) {
            1800
        } else {
            4000
        },
    )
    .unwrap();
    if matches!(case, SourceCase::TargetSourceExportStartReplyLost) {
        controls[donor]
            .test_drop_next_source_discovery_reply()
            .unwrap();
    }
    let first = call(output, &mut ledger, "actual_s2_start", &start, || {
        target
            .request_remote_image_source_discovery(&plan, &start, &start_own, &start_parent)
            .map(|x| x.reply().clone())
    });
    let after_start = controls[donor]
        .test_source_discovery_state(selected.id)
        .unwrap();
    evidence(&output.join("source-export-start-outcome.json"), &serde_json::json!({"case":format!("{case:?}"),"first_error":first.as_ref().err().map(|e|format!("{e:?}")),"original_budget_ms":if matches!(case,SourceCase::TargetSourceExportStartReplyLost){2000}else{5000},"after":after_start,"acknowledged":acknowledged})).unwrap();
    if matches!(case, SourceCase::TargetSourceExportStartReplyLost) {
        assert!(first.is_err(), "actual Start reply must be suppressed");
        assert_eq!(after_start["source"]["capture_retained"], true);
        assert_eq!(
            after_start["source"]["manifest"]["target_attempt"],
            serde_json::to_value(attempt).unwrap()
        );
    } else {
        assert!(first.is_ok(), "actual S2 Start: {first:?}");
    }
    let describe_parent = budget(5000);
    let (describe_own, business) = own(target, &describe_parent);
    let describe = NativeSourceDiscoveryRequestV1::from_plan(
        &plan,
        node(donor),
        node(4),
        attempt,
        DiscoveryOp::Describe,
        &describe_own,
        business,
        4000,
    )
    .unwrap();
    let described = call(
        output,
        &mut ledger,
        "actual_s2_describe_original_session",
        &describe,
        || {
            target
                .request_remote_image_source_discovery(
                    &plan,
                    &describe,
                    &describe_own,
                    &describe_parent,
                )
                .map(|x| x.reply().clone())
        },
    )
    .unwrap();
    let manifest = described.manifest;
    assert_eq!(
        serde_json::to_value(&manifest).unwrap(),
        after_start["source"]["manifest"]
    );
    let source = Arc::new(
        NativeRemoteImageFact::from_actual_manifest(
            frame(target, &manifest, Op::Manifest, output, &mut journal).unwrap(),
        )
        .unwrap(),
    );
    let provider = MetadataReadProvider::new(metadata[4].clone(), target.clone()).unwrap();
    let native_unknown = matches!(case, SourceCase::TargetSourceExportNativeUnknown);
    target_final_image_tests::exercise(
        if native_unknown {
            SourceCase::TargetNativeBaseCommitSyncUnknown
        } else {
            SourceCase::TargetCatalogueFloor
        },
        target_control,
        target,
        source.clone(),
        &plan,
        &provider,
        &leader_actor,
        factory,
        pending,
        ranges,
        &acknowledged[0],
        output,
        &mut journal,
    );
    // Only the target takes its ORIGINAL production owner. No owner/certificate
    // is sent to the source; the source independently calls its mTLS endpoint.
    let original_owner = if native_unknown {
        None
    } else {
        let request = budget(5000);
        let role = actual_role(&provider, &leader_actor, &plan, &node(4), &request)
            .into_handoff()
            .unwrap();
        let (own, business) = own(target, &request);
        let finished = source
            .bind_step(frame(target, &manifest, Op::Finish, output, &mut journal).unwrap())
            .unwrap();
        Some(
            target_control
                .take_registered_remote_joint_target(
                    &source, &finished, role, own, business, &request,
                )
                .unwrap(),
        )
    };
    let before = controls[donor]
        .test_source_discovery_state(selected.id)
        .unwrap();
    assert_eq!(before["source"]["capture_retained"], true);
    assert_eq!(
        before["source"]["original_export_session_cancel_requested"],
        false
    );
    assert!(before["source"]["actual_gate"]["capture_pin"].is_string());
    assert!(before["source"]["export_retirement"]["completed"].is_null());
    let target_before_retire = if native_unknown {
        serde_json::json!({"native_health":factory.database.read().test_registered_native_health_status(None)})
    } else {
        state(&factory.database, pending, &node(4), ranges).unwrap()
    };
    let request_parent = budget(5000);
    // A poisoned target cannot acquire a new local OWN. The negative control
    // sends only DATA over its real TLS identity; the recipient must obtain
    // its own genuine authority and refuse the missing completed NEW owner.
    let request_own = (!native_unknown).then(|| own(target, &request_parent));
    let mut request = if let Some((own, business)) = &request_own {
        NativeSourceExportRequestV1::from_manifest(
            &manifest,
            ExportOp::Retire,
            own,
            *business,
            4000,
        )
        .unwrap()
    } else {
        NativeSourceExportRequestV1 {
            version: 1,
            binding: crate::distribution_range_image::NativeSourceExportBindingV1::from_manifest(
                &manifest,
            )
            .unwrap(),
            operation: ExportOp::Retire,
            business: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            budget_ms: 4000,
        }
    };
    if matches!(case, SourceCase::TargetSourceExportWrongManifest) {
        request.binding.manifest_sha256.replace_range(
            0..1,
            if manifest.checksum_sha256.starts_with('0') {
                "1"
            } else {
                "0"
            },
        );
    }
    if matches!(case, SourceCase::TargetSourceExportWrongAttempt) {
        request.binding.target_attempt = Uuid::now_v7();
    }
    if matches!(case, SourceCase::TargetSourceExportWrongBirth) {
        // A real enrolled OTHER node's immutable birth DATA, never a minted
        // capability or altered protected local identity.
        request.binding.source_birth = pending.nodes[&selected.leader]
            .native_store_birth_v1
            .clone()
            .unwrap();
        assert_ne!(request.binding.source_birth, manifest.source_birth);
    }
    if matches!(case, SourceCase::TargetSourceExportTargetStoreABA) {
        let replacement = crate::RangeWriteStore::open(
            output.join("export-target-replacement-store"),
            pending.cluster_id.clone(),
            node(4),
            true,
        )
        .unwrap();
        let old = services[4]
            .lock()
            .test_replace_native_range_write_store(replacement);
        drop(
            services[4]
                .lock()
                .test_replace_native_range_write_store(old),
        );
    }
    if matches!(case, SourceCase::TargetSourceExportOriginalSourceCanceled) {
        controls[donor]
            .test_cancel_source_discovery_original_process()
            .unwrap();
    }
    if matches!(case, SourceCase::TargetSourceExportReplyLost) {
        controls[donor]
            .test_drop_next_source_export_reply()
            .unwrap();
    }
    let detach_trace = Arc::new(Mutex::new(serde_json::Value::Null));
    if matches!(case, SourceCase::TargetSourceExportTermAtDetach) {
        let actual = metadata[donor].clone();
        let trace = detach_trace.clone();
        let caller = request_parent.clone();
        let path = output.join("source-export-final-detach-local-term.json");
        controls[donor].test_source_export_before_detach(Box::new(move |own_deadline, _original_process| {
            let result = (|| -> Result<serde_json::Value> {
                let mut store = loop {
                    caller.check()?;
                    if let Some(store) = actual.try_lock_for(Duration::from_millis(5)) { break store; }
                };
                let before = store.test_actual_publication_version_evidence()?;
                let previous = store.status().current_term;
                let requested = previous.checked_add(1).ok_or_else(|| BicDbError::Cluster("test actual term overflow".into()))?;
                evidence(&path,&serde_json::json!({"phase":"actual_mutation_intent","before":before,"requested_term":requested,"original_own_remaining_us":own_deadline.saturating_duration_since(Instant::now()).as_micros(),"local_term_only_not_new_leader":true}))?;
                let changed = store.observe_remote_term(requested)?;
                let after = store.test_actual_publication_version_evidence()?;
                Ok(serde_json::json!({"phase":"actual_mutation_returned","changed":changed,"before":before,"after":after,"previous_term":previous,"requested_term":requested,"actual_term":store.status().current_term,"original_own_remaining_us":own_deadline.saturating_duration_since(Instant::now()).as_micros(),"original_parent_live":caller.check().is_ok(),"local_term_only_not_new_leader":true}))
            })();
            let value = serde_json::json!({"observation":result.as_ref().ok(),"exact_error":result.as_ref().err().map(|e|format!("{e:?}"))});
            *trace.lock() = value.clone();
            evidence(&path,&value).unwrap();
        })).unwrap();
    }
    if matches!(
        case,
        SourceCase::TargetSourceExportExpiryAtDetach
            | SourceCase::TargetSourceExportProcessCancelAtDetach
    ) {
        let trace = detach_trace.clone();
        let caller = request_parent.clone();
        let path = output.join("source-export-final-detach-original-window.json");
        controls[donor].test_source_export_before_detach(Box::new(move |own_deadline, original_process| {
            let before = serde_json::json!({"seam_reached":true,"actual_source_process_cancelled":original_process.is_cancel_requested(),"original_own_remaining_us":own_deadline.saturating_duration_since(Instant::now()).as_micros(),"original_parent_live":caller.check().is_ok()});
            evidence(&path,&serde_json::json!({"before":before,"cut_not_yet_applied":true})).unwrap();
            if matches!(case,SourceCase::TargetSourceExportExpiryAtDetach) {
                while Instant::now() <= own_deadline { caller.check().unwrap(); std::thread::sleep(Duration::from_millis(5)); }
            } else { original_process.cancel(); }
            let after = serde_json::json!({"seam_reached":true,"actual_source_process_cancelled":original_process.is_cancel_requested(),"original_own_remaining_us":own_deadline.saturating_duration_since(Instant::now()).as_micros(),"original_parent_live":caller.check().is_ok()});
            let value=serde_json::json!({"before":before,"after":after,"no_clock_or_scope_replacement":true});
            *trace.lock()=value.clone();
            evidence(&path,&value).unwrap();
        })).unwrap();
    }
    if callback_liveness_tests::is_case(case) {
        controls[donor]
            .test_arm_source_export_callback_request(matches!(
                case,
                SourceCase::TargetSourceExportCallbackExpiryAtDetach
            ))
            .unwrap();
    }
    let sent_by = if matches!(case, SourceCase::TargetSourceExportWrongActor) {
        node(0)
    } else {
        node(4)
    };
    let raw = matches!(
        case,
        SourceCase::TargetSourceExportWrongActor
            | SourceCase::TargetSourceExportWrongManifest
            | SourceCase::TargetSourceExportWrongAttempt
            | SourceCase::TargetSourceExportWrongBirth
            | SourceCase::TargetSourceExportNativeUnknown
    );
    let result = call(
        output,
        &mut ledger,
        "actual_source_export_retire_two_hops",
        &serde_json::json!({"request":request,"actual_sender":sent_by}),
        || {
            if raw {
                (if matches!(case, SourceCase::TargetSourceExportWrongActor) {
                    clients[0].as_ref()
                } else {
                    target.as_ref()
                })
                .test_send_source_export_request(&request, &request_parent)
            } else {
                target.request_remote_source_export_retirement(
                    &manifest,
                    &request,
                    &request_own.as_ref().unwrap().0,
                    &request_parent,
                )
            }
        },
    );
    let target_gate_after = factory
        .database
        .read()
        .range_mutation_gate()
        .test_source_capture_pin_state(selected.id)
        .unwrap();
    let after = controls[donor]
        .test_source_discovery_state(selected.id)
        .unwrap();
    let native_after = old_records(services, &plan);
    let negative = matches!(
        case,
        SourceCase::TargetSourceExportWrongActor
            | SourceCase::TargetSourceExportWrongManifest
            | SourceCase::TargetSourceExportWrongAttempt
            | SourceCase::TargetSourceExportWrongBirth
            | SourceCase::TargetSourceExportTargetStoreABA
            | SourceCase::TargetSourceExportOriginalSourceCanceled
            | SourceCase::TargetSourceExportNativeUnknown
            | SourceCase::TargetSourceExportTermAtDetach
            | SourceCase::TargetSourceExportExpiryAtDetach
            | SourceCase::TargetSourceExportProcessCancelAtDetach
            | SourceCase::TargetSourceExportCallbackCancelAtDetach
            | SourceCase::TargetSourceExportCallbackExpiryAtDetach
    );
    evidence(&output.join("source-export-retirement-before-assertions.json"),&serde_json::json!({"version":1,"case":format!("{case:?}"),"actual_donor":node(donor),"range_leader":selected.leader,"retiring_replica":plan.retiring_source(),"request":request,"before":before,"after":after,"exact_error":result.as_ref().err().map(|e|format!("{e:?}")),"actual_reply":result.as_ref().ok(),"old_native_before":native_before,"old_native_after":native_after,"target_before":original_target,"target_before_retire":target_before_retire,"target_health":factory.database.read().test_registered_native_health_status(None),"actual_target_gate_after":target_gate_after,"source_manifest":manifest,"source_frame_journal":journal,"acknowledged":acknowledged,"old_retirement_tested":false,"serving_tested":false,"physical_qualification":false})).unwrap();
    if callback_liveness_tests::is_case(case) {
        callback_liveness_tests::finish(
            case,
            &controls[donor],
            factory,
            pending,
            ranges,
            &before,
            &after,
            &native_before,
            &native_after,
            &target_before_retire,
            &result,
            &manifest,
            &journal,
            acknowledged,
            output,
        );
    }
    assert_eq!(
        native_before, native_after,
        "export retirement must not rewrite native OLD receipts"
    );
    if matches!(case, SourceCase::TargetSourceExportTermAtDetach) {
        let trace = detach_trace.lock().clone();
        evidence(&output.join("source-export-final-detach-before-assertions.json"), &serde_json::json!({"trace":trace,"before":before,"after":after,"actual_request":request,"exact_error":result.as_ref().err().map(|e|format!("{e:?}")),"source_frame_journal":journal,"acknowledged":acknowledged})).unwrap();
        assert_eq!(trace["observation"]["changed"], true);
        assert_eq!(
            trace["observation"]["requested_term"],
            trace["observation"]["actual_term"]
        );
        assert_eq!(trace["observation"]["original_parent_live"], true);
        assert!(
            trace["observation"]["original_own_remaining_us"]
                .as_u64()
                .unwrap()
                > 0,
            "expiry is not this control's intended cut"
        );
        assert_eq!(
            after["source"]["export_retirement"]["observations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    if matches!(
        case,
        SourceCase::TargetSourceExportExpiryAtDetach
            | SourceCase::TargetSourceExportProcessCancelAtDetach
    ) {
        let trace = detach_trace.lock().clone();
        evidence(&output.join("source-export-final-detach-window-before-assertions.json"),&serde_json::json!({"case":format!("{case:?}"),"trace":trace,"before":before,"after":after,"exact_error":result.as_ref().err().map(|e|format!("{e:?}")),"acknowledged":acknowledged})).unwrap();
        assert_eq!(trace["before"]["seam_reached"], true);
        assert_eq!(trace["after"]["original_parent_live"], true);
        if matches!(case, SourceCase::TargetSourceExportExpiryAtDetach) {
            assert_eq!(trace["after"]["original_own_remaining_us"], 0);
        } else {
            assert_eq!(trace["after"]["actual_source_process_cancelled"], true);
            assert!(
                trace["after"]["original_own_remaining_us"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
        }
        assert_eq!(
            after["source"]["export_retirement"]["observations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    if negative {
        assert!(
            result.is_err(),
            "negative actual endpoint admitted retirement"
        );
        assert_eq!(after["source"]["capture_retained"], true);
        assert_eq!(
            after["source"]["actual_gate"]["capture_pin"],
            before["source"]["actual_gate"]["capture_pin"]
        );
        assert!(after["source"]["export_retirement"]["completed"].is_null());
        assert_eq!(
            before["source"]["source_session"],
            after["source"]["source_session"]
        );
        assert_eq!(before["source"]["manifest"], after["source"]["manifest"]);
        return;
    }
    if matches!(case, SourceCase::TargetSourceExportReplyLost) {
        assert!(result.is_err(), "actual retire reply must be suppressed");
    } else {
        assert!(result.is_ok(), "actual retirement: {result:?}");
    }
    assert_eq!(after["source"]["capture_retained"], false);
    assert_eq!(
        after["source"]["original_export_session_cancel_requested"],
        true
    );
    // OLD retains the shared native read pin until genuine decision/Open
    // continuation. ONLY this completed S2 export's actual Arc is retired.
    assert_eq!(
        after["source"]["actual_gate"]["capture_pin"],
        before["source"]["actual_gate"]["capture_pin"]
    );
    assert_eq!(
        after["source"]["actual_gate"]["frozen"],
        before["source"]["actual_gate"]["frozen"]
    );
    assert_eq!(after["source"]["actual_gate"]["poisoned"], false);
    let custody = &after["source"]["export_retirement"]["observations"][0];
    let before_count = custody["exact_export_pin_references_before"]
        .as_u64()
        .unwrap();
    let after_count = custody["exact_export_pin_references_after_drop"]
        .as_u64()
        .unwrap();
    assert!(
        after_count < before_count,
        "actual export read-pin reference did not retire"
    );
    assert!(
        after_count > 0,
        "original OLD read-pin obligation must remain owned"
    );
    assert_eq!(
        after["source"]["export_retirement"]["observations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let retry_parent = budget(5000);
    let (retry_own, business) = own(target, &retry_parent);
    let retry = NativeSourceExportRequestV1::from_manifest(
        &manifest,
        ExportOp::Describe,
        &retry_own,
        business,
        4000,
    )
    .unwrap();
    let reply = call(
        output,
        &mut ledger,
        "actual_source_export_describe_after_terminal_or_lost_reply",
        &retry,
        || {
            target.request_remote_source_export_retirement(
                &manifest,
                &retry,
                &retry_own,
                &retry_parent,
            )
        },
    )
    .unwrap();
    let final_state = controls[donor]
        .test_source_discovery_state(selected.id)
        .unwrap();
    let old_frame = frame(target, &manifest, Op::Finish, output, &mut journal);
    evidence(&output.join("source-export-retirement-reconciled.json"),&serde_json::json!({"case":format!("{case:?}"),"retired":reply.retired,"after":after,"final":final_state,"old_frame_exact_error":old_frame.as_ref().err().map(|e|format!("{e:?}")),"target_after":state(&factory.database,pending,&node(4),ranges).unwrap(),"old_native_after":old_records(services,&plan),"source_frame_journal":journal,"acknowledged":acknowledged,"terminal_is_historical_data_only":true,"old_fence_release_or_serving":false})).unwrap();
    assert_eq!(
        serde_json::to_value(reply.retired).unwrap(),
        after["source"]["export_retirement"]["completed"]
    );
    assert_eq!(
        final_state["source"]["export_retirement"]["observations"],
        after["source"]["export_retirement"]["observations"]
    );
    assert_eq!(
        final_state["source"]["source_session"],
        before["source"]["source_session"]
    );
    assert_eq!(
        final_state["source"]["manifest"],
        before["source"]["manifest"]
    );
    assert!(
        old_frame.is_err(),
        "retired exporter must not issue another frame"
    );
    assert_eq!(
        state(&factory.database, pending, &node(4), ranges).unwrap(),
        target_before_retire
    );
    assert!(
        target_gate_after["capture_pin"].is_string(),
        "actual target read pin must remain retained"
    );
    original_owner
        .unwrap()
        .retained_freeze()
        .unwrap()
        .try_as_frozen()
        .unwrap();
    assert!(!factory.database.read().commit_admission_ready());
}
