//! Five retained normal host OBJECTS in ONE Rust test process, with distinct
//! actual DB/TLS endpoints over real TCP; not independent processes or VMs.
//! No legacy enrollment fixture/local OLD quorum,
//! manufactured MF, backend substitute or native grant constructor is used.
use super::*;
mod cross_protocol_tests;
mod metadata_identity_tests;
use crate::distribution_native_host::remote_first::NativeFirstIngressFault as Fault;
use crate::distribution_native_host::{
    NativeFirstCoordinatorAction as Action, NativeFirstReceiveOperationV1 as Op,
    NativeMetadataBootstrapPlan, NativeMetadataCollection, NativeMetadataHost,
    NativeMetadataHostAction as HostAction, NativeMetadataLearnerHost,
};
use crate::distribution_range_image::{
    NativeRemoteImageSourceOperationV1 as Frame, RangeImageCursorV1, RangeImageHistoryCursorV1,
};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::mpsc;
use std::time::Instant;
mod bridge_retry_tests;
mod bridge_tests;
mod metadata_head_tests;
mod open_tests;
mod preparation_tests;
mod release_tests;
mod request_wait_tests;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    HotRequestWait,
    HotRequestCancel,
    HotRequestExpiry,
    StatusRequestWait,
    StatusRequestCancel,
    StatusRequestExpiry,
    OldRequestWait,
    OldRequestCancel,
    OldRequestExpiry,
    MetadataIdentityWait,
    MetadataIdentityCancel,
    MetadataIdentityExpiry,
    Normal,
    LostEnroll,
    CancelledQueued,
    ExpiredQueued,
    LostStart,
    UnknownFinal,
    Over512,
    LostClose,
    ConcurrentAdmission,
    NativeDecision,
    LostExport,
    LostNativeDecision,
    UnknownNativeDecision,
    Catalogue,
    CatalogueKnownNativeRead,
    CatalogueLostReply,
    CatalogueIoUnknown,
    CatalogueRenameUnknown,
    CatalogueForeignDisk,
    CataloguePreIoCancel,
    CatalogueInterveningWrite,
    CatalogueDuplicate,
    CatalogueWrongExpected,
    CatalogueStale,
    CatalogueScope,
    CatalogueMetadataHeadDrift,
    Preparation,
    PreparationCancelledAfterNative,
    PreparationCommitUnknown,
    PreparationHostPollCancelled,
    PreparationHostPollExpired,
    Bridge,
    BridgeMissingPrepared,
    BridgeCancelledAfterNative,
    BridgeCommitUnknown,
    Open,
    OpenR2Progress,
    OpenLaterIoFailed,
    OpenCancelledAfterIo,
    OpenIoUnknown,
    Release,
    ReleaseMissingOpen,
    ReleaseCancelledAfterDrop,
    Serving,
    ServingMissingReleased,
    ServingSuccessorRead,
    ServingDrainResume,
    ServingPartialDrainResume,
    ServingMetadataActivation,
    ServingActivationMissingReleased,
}
fn is_catalogue_case(case: Case) -> bool {
    matches!(
        case,
        Case::Catalogue
            | Case::CatalogueKnownNativeRead
            | Case::CatalogueLostReply
            | Case::CatalogueIoUnknown
            | Case::CatalogueRenameUnknown
            | Case::CatalogueForeignDisk
            | Case::CataloguePreIoCancel
            | Case::CatalogueInterveningWrite
            | Case::CatalogueDuplicate
            | Case::CatalogueWrongExpected
            | Case::CatalogueStale
            | Case::CatalogueScope
            | Case::CatalogueMetadataHeadDrift
            | Case::Preparation
            | Case::PreparationCancelledAfterNative
            | Case::PreparationCommitUnknown
            | Case::PreparationHostPollCancelled
            | Case::PreparationHostPollExpired
            | Case::Bridge
            | Case::BridgeMissingPrepared
            | Case::BridgeCancelledAfterNative
            | Case::BridgeCommitUnknown
            | Case::Open
            | Case::OpenR2Progress
            | Case::OpenLaterIoFailed
            | Case::OpenCancelledAfterIo
            | Case::OpenIoUnknown
            | Case::Release
            | Case::ReleaseMissingOpen
            | Case::ReleaseCancelledAfterDrop
            | Case::Serving
            | Case::ServingMissingReleased
            | Case::ServingSuccessorRead
            | Case::ServingDrainResume
            | Case::ServingPartialDrainResume
            | Case::ServingMetadataActivation
            | Case::ServingActivationMissingReleased
    )
}
#[derive(Clone, Copy, Debug)]
enum SetupCut {
    ReturnError,
    Panic,
}

enum OwnerCommand {
    LocalServingPartialDrain(
        crate::RangeId,
        Uuid,
        ClusterNodeId,
        Uuid,
        mpsc::SyncSender<Arc<crate::distribution_transport::InitialTransitionControl>>,
        mpsc::SyncSender<serde_json::Value>,
    ),
    LocalServingDrainCancel(crate::RangeId, Uuid, mpsc::SyncSender<serde_json::Value>),
    PolicySocketProfile(crate::RangeId, Uuid, mpsc::SyncSender<serde_json::Value>),
    LocalServing(
        crate::RangeId,
        Uuid,
        bool,
        mpsc::SyncSender<serde_json::Value>,
    ),
    RetireReferences(crate::RangeId, Uuid, mpsc::SyncSender<serde_json::Value>),
    ReleaseControl(
        crate::RangeId,
        Option<u8>,
        mpsc::SyncSender<serde_json::Value>,
    ),
    OpenControl(
        crate::RangeId,
        Option<u8>,
        mpsc::SyncSender<serde_json::Value>,
    ),
    BridgeControl(
        crate::RangeId,
        Option<u8>,
        mpsc::SyncSender<serde_json::Value>,
    ),
    Snapshot(mpsc::SyncSender<serde_json::Value>),
    PreparationControl(
        crate::RangeId,
        Option<u8>,
        mpsc::SyncSender<serde_json::Value>,
    ),
    NativeDecision(
        crate::RangeId,
        Option<u8>,
        mpsc::SyncSender<serde_json::Value>,
    ),
}
fn native_command(
    sender: &mpsc::Sender<OwnerCommand>,
    range: crate::RangeId,
    arm: Option<u8>,
) -> Result<serde_json::Value> {
    let (send, receive) = mpsc::sync_channel(1);
    sender
        .send(OwnerCommand::NativeDecision(range, arm, send))
        .map_err(|_| invalid("FIRST actual native owner command ended"))?;
    let data = receive
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("FIRST actual native owner diagnostic return unavailable"))?;
    if data["ok"] != true {
        return Err(invalid(&format!(
            "actual native owner diagnostic failed: {data}"
        )));
    }
    Ok(data["data"].clone())
}
fn snapshot(sender: &mpsc::Sender<OwnerCommand>) -> Result<serde_json::Value> {
    let (send, receive) = mpsc::sync_channel(1);
    sender
        .send(OwnerCommand::Snapshot(send))
        .map_err(|_| invalid("FIRST actual owner command ended"))?;
    let actual = receive
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("FIRST owner diagnostic return unavailable"))?;
    if actual["ok"] != true {
        return Err(invalid(&format!(
            "FIRST actual owner diagnostic error: {actual}"
        )));
    }
    Ok(actual["data"].clone())
}

struct Report {
    events: Vec<serde_json::Value>,
    omitted: u64,
    stderr_write_errors: u64,
}
impl Report {
    fn event(&mut self, value: serde_json::Value) {
        // Logging failure must never replace the original API/panic result.
        if writeln!(std::io::stderr().lock(), "NATIVE_FIRST_ACTUAL {}", value).is_err() {
            self.stderr_write_errors = self.stderr_write_errors.saturating_add(1);
        }
        if self.events.len() < 4096 {
            self.events.push(value);
        } else {
            self.omitted += 1;
        }
    }
    fn call<T>(&mut self, name: &str, call: impl FnOnce() -> Result<T>) -> Result<T> {
        self.event(serde_json::json!({"stage":name,"boundary":"before_actual_call"}));
        let entered = Instant::now();
        let result = call();
        // Preserve the actual primary return before any assertion. Report
        // serialization is diagnostic and never substitutes for that return.
        self.event(serde_json::json!({"stage":name,"boundary":"actual_call_returned",
            "elapsed_us":entered.elapsed().as_micros(),"ok":result.is_ok(),
            "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
        result
    }
    fn data_call<T: serde::Serialize>(
        &mut self,
        name: &str,
        call: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let result = self.call(name, call);
        self.event(
            serde_json::json!({"stage":name,"boundary":"actual_returned_DATA_only",
            "result_data":result.as_ref().ok().map(serde_json::to_value).and_then(|v|v.ok())}),
        );
        result
    }
}
struct Five {
    sources: Vec<NativeMetadataHost>,
    target: Option<NativeMetadataLearnerHost>,
    lifetime: CancellationToken,
    config: DistributionConfig,
    topology: ClusterTopology,
    root: tempfile::TempDir,
}
impl Five {
    fn start(report: &mut Report) -> Result<Self> {
        Self::start_with_cut(report, None)
    }
    fn start_with_cut(report: &mut Report, cut: Option<SetupCut>) -> Result<Self> {
        let PlannedNetwork {
            mut root,
            plan,
            networks,
            ports,
        } = planned_network_with_spare_and_io_timeout(4, 1, 1500);
        root.disable_cleanup(true);
        report.event(
            serde_json::json!({"stage":"normal_host_roots","root":root.path(),
            "scope":"in-process actual TLS; no physical qualification","preserved_on_failure":true}),
        );
        let lifetime = CancellationToken::uncancelable();
        let initial = &plan.topology;
        let config = DistributionConfig {
            enabled: true,
            cluster_id: initial.cluster_id.clone(),
            range_image_bootstrap_id: initial.range_image_bootstrap_id,
            node_id: node(0),
            node_address: initial.nodes[&node(0)].address.clone(),
            node_tls_certificate_sha256: initial.nodes[&node(0)].tls_certificate_sha256.clone(),
            node_incarnation: 1,
            node_capacity_bytes: 1 << 30,
            replication_factor: 3,
            initial_ranges: 4,
            transport: networks[0].clone(),
            ..Default::default()
        };
        let public = NativeMetadataBootstrapPlan {
            version: 1,
            configuration: config.clone(),
            members: initial.nodes.values().cloned().collect(),
            collections: plan
                .collections
                .iter()
                .map(|c| NativeMetadataCollection {
                    name: c.name.clone(),
                    mode: c.mode.clone(),
                })
                .collect(),
            created_at_ms: 10,
        };
        let mut reserved = ports.into_iter().map(Some).collect::<Vec<_>>();
        let fifth_address = reserved[4].as_ref().unwrap().local_addr()?.to_string();
        // Own every returned normal host immediately, before the next
        // fallible setup call. No live source/learner is stranded in locals.
        let mut five = Self {
            sources: Vec::new(),
            target: None,
            lifetime,
            config,
            topology: initial.clone(),
            root,
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            for i in 0..4 {
                // Every other endpoint remains actually reserved until its own
                // normal host is ready to bind. There is no free-port lottery.
                drop(reserved[i].take());
                five.sources
                    .push(report.call("normal_initial_host_start", || {
                        NativeMetadataHost::start_fresh_metadata_only(
                            &five.root.path().join(format!("normal-initial-{i}")),
                            public.clone(),
                            node(i),
                            &initial.nodes[&node(i)].address,
                            networks[i].clone(),
                            8 * 1024 * 1024,
                            &five.lifetime,
                        )
                    })?);
            }
            for action in [HostAction::Prepare, HostAction::Launch] {
                for host in &mut five.sources {
                    report.call("normal_initial_host_action", || {
                        host.apply(action.clone(), &five.lifetime)
                    })?;
                }
            }
            report.call("normal_actual_metadata_election", || {
                five.sources[0].apply(HostAction::Elect, &five.lifetime)
            })?;
            for host in &mut five.sources {
                report.call("normal_control_transition", || {
                    host.apply(HostAction::EnableControl, &five.lifetime)
                })?;
                report.call("normal_original_supervisor", || {
                    host.start_automatic_metadata(&five.lifetime)
                })?;
            }
            if let Some(cut) = cut {
                report.event(serde_json::json!({"stage":"actual_partial_setup_cut_after_four_supervisors",
                "cut":format!("{cut:?}"),"actual_host_count":five.sources.len(),"synthetic_native_failure":false}));
                match cut {
                    SetupCut::ReturnError => {
                        return Err(invalid("intentional partial FIRST fixture setup return"))
                    }
                    SetupCut::Panic => {
                        std::panic::panic_any("intentional partial FIRST fixture setup panic")
                    }
                }
            }
            // Same ordinary public opening API for every retained normal host.
            // Each invocation retains the API's original 5s bound.
            std::thread::scope(|scope| -> Result<()> {
                let handles = five
                    .sources
                    .iter_mut()
                    .enumerate()
                    .map(|(ordinal, host)| {
                        report.event(
                        serde_json::json!({"stage":"normal_initial_open_dispatch","ordinal":ordinal,
                        "original_action_budget_ms":5000}),
                    );
                        let parent = &five.lifetime;
                        scope.spawn(move || {
                            let started = Instant::now();
                            let actual = host.apply(HostAction::OpenInitialData, parent);
                            (started.elapsed().as_micros(), actual)
                        })
                    })
                    .collect::<Vec<_>>();
                enum OriginalFailure {
                    Error(BicDbError),
                    Panic(Box<dyn std::any::Any + Send>),
                }
                let expected_count = handles.len();
                let mut failures = Vec::new();
                for (ordinal, handle) in handles.into_iter().enumerate() {
                    match handle.join() {
                        Ok((elapsed_us, actual)) => {
                            report.event(serde_json::json!({
                                "stage":"normal_initial_data_joined_return","ordinal":ordinal,
                                "node":node(ordinal),"ok":actual.is_ok(),
                                "actual_call_elapsed_us":elapsed_us,
                                "original_action_budget_ms":5000,
                                "actual_status":actual.as_ref().ok(),
                                "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                            }));
                            if let Err(error) = report.call("normal_initial_data_return", || actual)
                            {
                                failures.push((ordinal, OriginalFailure::Error(error)));
                            }
                        }
                        Err(payload) => {
                            let data = if let Some(text) = payload.downcast_ref::<String>() {
                                serde_json::json!({"kind":"String","payload":text})
                            } else if let Some(text) = payload.downcast_ref::<&str>() {
                                serde_json::json!({"kind":"str","payload":text})
                            } else {
                                serde_json::json!({"kind":"opaque","type_id":format!("{:?}",payload.as_ref().type_id()),"original_payload_retained":true})
                            };
                            report.event(serde_json::json!({
                                "stage":"normal_initial_data_panic","ordinal":ordinal,
                                "node":node(ordinal),"actual_payload":data,
                                "original_action_budget_ms":5000,
                            }));
                            failures.push((ordinal, OriginalFailure::Panic(payload)));
                        }
                    }
                }
                report.event(serde_json::json!({
                    "stage":"normal_initial_data_all_returns_retained",
                    "joined":expected_count,"failed_or_panicked":failures.len(),
                    "no_new_DataOpen_attempts":true,
                }));
                // Retain every actual return before selecting the first
                // original failure in invocation order. No failed opening
                // is retried or relabeled successful during setup.
                if let Some((_, original)) = failures.into_iter().next() {
                    match original {
                        OriginalFailure::Error(error) => return Err(error),
                        OriginalFailure::Panic(payload) => std::panic::resume_unwind(payload),
                    }
                }
                Ok(())
            })?;
            let mut fifth =
                crate::ClusterNode::new(node(4), fifth_address.clone(), 1, 1 << 30, 10)?
                    .as_metadata_learner()
                    .with_tls_certificate_sha256(
                        crate::replication_transport::replication_certificate_sha256(
                            &networks[4].tls.as_ref().unwrap().cert_path,
                        )?,
                    )?;
            fifth.labels = initial.nodes[&node(0)].labels.clone();
            report.call("actual_tls_register_metadata_learner", || {
                five.sources[0].test_first_register_learner(fifth, &five.lifetime)
            })?;
            drop(reserved[4].take());
            let target = report.call("normal_join_fresh_host_start", || {
                NativeMetadataLearnerHost::start_fresh_metadata_only(
                    &five.root.path().join("normal-join-fresh"),
                    initial.cluster_id.clone(),
                    node(4),
                    &fifth_address,
                    1,
                    node(0),
                    &initial.nodes[&node(0)].address,
                    networks[4].clone(),
                    8 * 1024 * 1024,
                    &five.lifetime,
                )
            })?;
            five.target = Some(target);
            report.call("normal_native_birth_enrollment", || {
                five.sources[0].apply(HostAction::EnrollLearner { node: node(4) }, &five.lifetime)
            })?;
            report.call("normal_learner_owned_metadata", || {
                five.target.as_mut().unwrap().start_metadata(&five.lifetime)
            })?;
            report.call("normal_actual_learner_catchup", || {
                five.sources[0].apply(HostAction::CatchUpLearner { node: node(4) }, &five.lifetime)
            })?;
            let promotion = report.call("normal_actual_metadata_maintenance", || {
                five.sources[0].maintain_metadata(&five.lifetime)
            })?;
            report.event(serde_json::json!({"stage":"actual_maintenance_stage","value":promotion.last_metadata_maintenance}));
            report.call("normal_registered_control", || {
                five.target
                    .as_mut()
                    .unwrap()
                    .enable_metadata_control(&five.lifetime)
            })?;
            report.call("normal_target_original_supervisor", || {
                five.target
                    .as_mut()
                    .unwrap()
                    .start_automatic_metadata(&five.lifetime)
            })?;
            report.call("normal_sealed_fresh_receiver", || {
                five.target
                    .as_mut()
                    .unwrap()
                    .open_replica_receiver(&five.lifetime)
            })?;
            for host in &mut five.sources {
                report.call("normal_initial_registry_refresh", || {
                    host.refresh_initial_data_registry(&five.lifetime)
                })?;
            }
            let topology = report.call("actual_committed_topology", || {
                five.sources[0].test_first_topology(&five.lifetime)
            })?;
            five.topology = topology;
            Ok(())
        }));
        match outcome {
            Ok(Ok(())) => Ok(five),
            Ok(Err(error)) => {
                report.event(
                    serde_json::json!({"stage":"partial_setup_primary_error_before_cleanup",
                    "error":crate::distribution_native_host::remote_first::error_data(&error)}),
                );
                five.stop(report);
                Err(error)
            }
            Err(panic) => {
                report.event(
                    serde_json::json!({"stage":"partial_setup_primary_panic_before_cleanup"}),
                );
                five.stop(report);
                std::panic::resume_unwind(panic)
            }
        }
    }

    fn stop(&mut self, report: &mut Report) {
        if let Some(mut target) = self.target.take() {
            let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                report.call("actual_target_stop", || target.stop(&self.lifetime))
            }));
            report.event(serde_json::json!({"stage":"actual_target_cleanup_return",
                "returned_ok":cleanup.as_ref().is_ok_and(|r|r.is_ok()),"panicked":cleanup.is_err(),
                "full_drain_claim":false}));
        }
        for (ordinal, source) in self.sources.iter_mut().enumerate() {
            let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                report.data_call("actual_source_stop", || {
                    source.apply(HostAction::Stop, &self.lifetime)
                })
            }));
            report.event(serde_json::json!({"stage":"actual_source_cleanup_return","ordinal":ordinal,
                "returned_ok":cleanup.as_ref().is_ok_and(|r|r.is_ok()),"panicked":cleanup.is_err(),
                "actual_state_after":source.test_first_stop_state(),"new_completion_timeout":false}));
        }
        self.lifetime.cancel();
    }
}

#[test]
fn native_first_actual_metadata_owner_catalogue_scope_keeps_range_authority_and_refuses_stale_owner(
) {
    run(Case::CatalogueScope);
}
#[test]
fn native_first_remote_catalogue_actual_recipients_and_metadata_certified() {
    run(Case::Catalogue);
}
#[test]
fn native_first_metadata_selected_commit_survives_actual_other_range_head_changes() {
    run(Case::CatalogueMetadataHeadDrift);
}

#[test]
fn native_first_remote_catalogue_actual_known_commit_late_error_fresh_readback() {
    run(Case::CatalogueKnownNativeRead);
}

#[test]
fn native_first_remote_catalogue_actual_reply_loss_same_known_candidate() {
    run(Case::CatalogueLostReply);
}

#[test]
fn native_first_remote_catalogue_actual_dirsync_unknown_same_candidate_retry() {
    run(Case::CatalogueIoUnknown);
}

#[test]
fn native_first_remote_catalogue_actual_rename_unknown_original_disk_same_candidate_retry() {
    run(Case::CatalogueRenameUnknown);
}
#[test]
fn native_first_remote_catalogue_actual_foreign_disk_preimage_not_overwritten() {
    run(Case::CatalogueForeignDisk);
}
#[test]
fn native_first_remote_catalogue_actual_postclone_cancel_no_io_same_candidate_retry() {
    run(Case::CataloguePreIoCancel);
}

#[test]
fn native_first_remote_catalogue_actual_intervening_io_cannot_clear_old_uncertainty() {
    run(Case::CatalogueInterveningWrite);
}

#[test]
fn native_first_remote_catalogue_actual_duplicate_recipient_refused() {
    run(Case::CatalogueDuplicate);
}

#[test]
fn native_first_remote_catalogue_actual_frame_wrong_expected_body_refused() {
    run(Case::CatalogueWrongExpected);
}

#[test]
fn native_first_remote_catalogue_actual_expired_frame_new_collection_refused() {
    run(Case::CatalogueStale);
}

#[test]
fn native_first_normal_hosts_to_actual_recipient_native_decision() {
    run(Case::NativeDecision);
}
#[test]
fn native_first_actual_target_s2_reply_loss_describe_same_terminal_then_native_decision() {
    run(Case::LostExport);
}
#[test]
fn native_first_actual_native_decision_reply_loss_readonly_same_original_xid() {
    run(Case::LostNativeDecision);
}
#[test]
fn native_first_actual_native_sync_unknown_blocks_observation_and_new_mutation() {
    run(Case::UnknownNativeDecision);
}
#[test]
fn native_first_normal_hosts_actual_leader_nonleader_donor_to_materialized() {
    run(Case::Normal);
}
#[test]
fn native_first_actual_lost_enroll_reply_readonly_recovers_same_original_a() {
    run(Case::LostEnroll);
}
#[test]
fn native_first_actual_server_cancelled_queued_request_no_effect_fresh_step() {
    run(Case::CancelledQueued);
}
#[test]
fn native_first_actual_server_expired_queued_request_no_effect_fresh_step() {
    run(Case::ExpiredQueued);
}
#[test]
fn native_first_actual_lost_source_start_reply_describe_same_export_a() {
    run(Case::LostStart);
}
#[test]
fn native_first_actual_final_wal_cancel_retains_xid_and_blocks_next_mutation() {
    run(Case::UnknownFinal);
}
#[test]
fn native_first_over_512_real_row_steps_compact_and_refuse_consumed_sequence() {
    run(Case::Over512);
}
#[test]
fn native_first_actual_lost_close_readonly_resolves_original_record_without_replay() {
    run(Case::LostClose);
}
#[test]
fn native_first_actual_concurrent_new_sequence_cannot_replace_admitted_original() {
    run(Case::ConcurrentAdmission);
}
#[test]
fn native_first_partial_normal_setup_error_and_panic_stop_returned_supervisors() {
    for cut in [SetupCut::ReturnError, SetupCut::Panic] {
        let mut report = Report {
            events: Vec::new(),
            omitted: 0,
            stderr_write_errors: 0,
        };
        let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Five::start_with_cut(&mut report, Some(cut))
        }));
        let original = match &actual {
            Ok(Err(error)) => serde_json::json!({"returned_error":error.to_string()}),
            Err(payload) => serde_json::json!({"panic":payload.downcast_ref::<&str>().copied()}),
            _ => serde_json::json!({"unexpected_success":true}),
        };
        let cleanup = report
            .events
            .iter()
            .filter(|e| e["stage"] == "actual_source_cleanup_return")
            .cloned()
            .collect::<Vec<_>>();
        report.event(
            serde_json::json!({"actual_partial_setup_primary":original,"cleanup":cleanup,
            "original_stop_API_join_semantics":true,"independent_process_exit_claim":false}),
        );
        match cut {
            SetupCut::ReturnError => assert!(actual.as_ref().is_ok_and(|r| r
                .as_ref()
                .err()
                .is_some_and(|e| e
                    .to_string()
                    .contains("intentional partial FIRST fixture setup return")))),
            SetupCut::Panic => assert!(actual
                .as_ref()
                .err()
                .and_then(|p| p.downcast_ref::<&str>())
                .is_some_and(|p| *p == "intentional partial FIRST fixture setup panic")),
        }
        assert_eq!(
            cleanup.len(),
            4,
            "all returned actual source owners stopped"
        );
        for observed in cleanup {
            assert_eq!(
                observed["returned_ok"], true,
                "original stop failed; no drain claim"
            );
            assert_eq!(
                observed["actual_state_after"]["supervisor_slot_present"],
                false
            );
            assert_eq!(observed["actual_state_after"]["phase"], "stopped");
            assert_eq!(
                observed["actual_state_after"]["control_listener_running"],
                false
            );
        }
    }
}
fn run(case: Case) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
        stderr_write_errors: 0,
    };
    let mut five = match Five::start(&mut report) {
        Ok(five) => five,
        Err(error) => {
            report.event(
                serde_json::json!({"primary_setup_error":error.to_string(),"cut_reached":false}),
            );
            panic!("actual normal FIRST prerequisite: {error}");
        }
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise(&mut five, &mut report, case)
    }));
    five.stop(&mut report);
    let evidence = serde_json::json!({"events":report.events,"omitted":report.omitted,"stderr_write_errors":report.stderr_write_errors,
        "global_capture_complete":false,"automatic_operator":false,"serving_claim":false});
    let _ = std::fs::write(
        five.root.path().join("first-actual-report.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    );
    match result {
        Ok(actual) => actual.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn exercise(five: &mut Five, report: &mut Report, case: Case) -> Result<()> {
    let metadata_leader = five.sources[0]
        .status(&five.lifetime)?
        .metadata
        .and_then(|s| s.leader_id)
        .ok_or_else(|| invalid("FIRST actual metadata leader absent"))?;
    let selected = five
        .topology
        .ranges
        .values()
        .find(|r| r.leader != metadata_leader)
        .ok_or_else(|| invalid("FIRST actual event range absent"))?
        .clone();
    let untouched = five
        .topology
        .ranges
        .values()
        .find(|r| r.id != selected.id)
        .ok_or_else(|| invalid("FIRST untouched event range absent"))?
        .clone();
    let donor = selected
        .replicas
        .iter()
        .find(|r| {
            r.role == RangeReplicaRole::Voter
                && r.node_id != selected.leader
                && r.node_id != metadata_leader
        })
        .ok_or_else(|| invalid("FIRST donor neither actual leader unavailable"))?
        .node_id
        .clone();
    let coordinator = (0..4).find(|i| node(*i) == selected.leader).unwrap();
    let client = crate::ClusterEventClient::new(&five.config, five.topology.clone())?
        .with_request_timeout(Duration::from_secs(2))?;
    let selected_key = key(&five.topology, selected.id, "selected")?;
    let untouched_key = key(&five.topology, untouched.id, "untouched")?;
    sdk(
        report,
        &client,
        &selected_key,
        serde_json::json!({"actual":"selected-original-ACK"}),
    )?;
    sdk(
        report,
        &client,
        &untouched_key,
        serde_json::json!({"actual":"untouched-before"}),
    )?;
    if case == Case::Over512 {
        for i in 1..514 {
            sdk(
                report,
                &client,
                &key(&five.topology, selected.id, &format!("actual-over512-{i}"))?,
                serde_json::json!({"actual_selected_payload":i,"id":Uuid::now_v7()}),
            )?;
        }
    }
    if matches!(
        case,
        Case::HotRequestWait | Case::HotRequestCancel | Case::HotRequestExpiry
    ) {
        return request_wait_tests::hot(
            &five.sources[coordinator],
            selected.id,
            case,
            report,
            &five.lifetime,
        );
    }
    if matches!(
        case,
        Case::StatusRequestWait | Case::StatusRequestCancel | Case::StatusRequestExpiry
    ) {
        return request_wait_tests::status(
            &five.sources[coordinator],
            selected.id,
            case,
            report,
            &five.lifetime,
        );
    }
    let pending = report.call("actual_host_owned_allocation_quorum_commit", || {
        five.sources[0].test_first_commit_allocation(selected.id, &node(4), &five.lifetime)
    })?;
    for source in &mut five.sources {
        report.call("actual_post_placement_registry_refresh", || {
            source.refresh_initial_data_registry(&five.lifetime)
        })?;
    }
    let relocation = pending
        .relocations
        .values()
        .find(|r| r.range_id == selected.id)
        .ok_or_else(|| invalid("FIRST actual committed allocation absent"))?;
    report.event(serde_json::json!({"actual_range_leader":selected.leader,
        "actual_metadata_leader":metadata_leader,"actual_donor":donor,
        "committed_session":relocation.handoff_session,"target":node(4),
        "old_seed_used":false,"native_quorum_manufactured":false}));
    report.call("actual_range_leader_factory", || {
        five.sources[coordinator].begin_finite_remote_first(
            relocation.id,
            donor.clone(),
            &five.lifetime,
        )
    })?;
    if matches!(
        case,
        Case::MetadataIdentityWait | Case::MetadataIdentityCancel | Case::MetadataIdentityExpiry
    ) {
        return metadata_identity_tests::exercise(
            &five.sources[coordinator],
            selected.id,
            case,
            report,
            &five.lifetime,
        );
    }
    let mut target = five.target.take().unwrap();
    let fault = match case {
        Case::LostEnroll => Some(Fault::LoseSuccessfulReply(Op::Enroll)),
        Case::LostExport => Some(Fault::LoseSuccessfulReply(Op::RetireSourceExport)),
        Case::CancelledQueued => Some(Fault::CancelBeforeConsume(Op::Enroll)),
        Case::ExpiredQueued => Some(Fault::ExpireBeforeConsume(Op::Enroll)),
        Case::UnknownFinal => Some(Fault::CancelOriginalAfterFinalWal),
        _ => None,
    };
    if let Some(fault) = fault {
        report.call("arm_actual_original_target_ingress_cut", || {
            target.test_first_receiver_arm(fault)
        })?;
    }
    let donor_index = (0..4).find(|i| node(*i) == donor).unwrap();
    if case == Case::LostStart {
        report.call("arm_actual_donor_Start_reply_loss", || {
            five.sources[donor_index].test_first_drop_discovery_reply()
        })?;
    }
    let barrier = if case == Case::ConcurrentAdmission {
        Some(report.call("arm_actual_original_ingress_barrier", || {
            target.test_first_receiver_barrier(Op::Enroll)
        })?)
    } else {
        None
    };
    let close_peer = selected
        .replicas
        .iter()
        .filter(|r| r.role == RangeReplicaRole::Voter)
        .map(|r| r.node_id.clone())
        .min()
        .unwrap();
    let close_peer_index = (0..4).find(|i| node(*i) == close_peer).unwrap();
    let _close_loss_guard = if case == Case::LostClose {
        Some(report.call("arm_actual_original_OLD_Close_reply_loss", || {
            five.sources[close_peer_index].test_first_arm_close_loss(selected.id)
        })?)
    } else {
        None
    };
    let (owner_send, owner_receive) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let active = stop.clone();
    let parent = five.lifetime.child();
    let mut worker_errors = Vec::new();
    let mut worker_omitted = 0u64;
    let result = std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            while !active.load(Ordering::Acquire) && parent.check().is_ok() {
                while let Ok(command)=owner_receive.try_recv() {
                    match command {
                    OwnerCommand::PolicySocketProfile(range,phase,send)=> {
                        let request=parent.child_with_deadline(Instant::now()+Duration::from_secs(4));
                        let result=target.test_registered_policy_socket_profile(range,phase,&request);
                        let _=send.try_send(release_tests::policy_return_data(result));
                    },
                    OwnerCommand::LocalServingPartialDrain(range,phase,peer,business,ready,send)=> {
                        let request=parent.child_with_deadline(Instant::now()+Duration::from_secs(4));
                        let result=target.test_actual_registered_partial_drain_cancel(range,phase,peer,business,ready,&request);
                        let _=send.try_send(release_tests::policy_return_data(result));
                    },
                    OwnerCommand::LocalServingDrainCancel(range,phase,send)=> {
                        let request=parent.child_with_deadline(Instant::now()+Duration::from_secs(4));
                        let result=target.test_actual_registered_policy_drain_cancel(range,phase,&request);
                        let _=send.try_send(release_tests::policy_return_data(result));
                    },
                    OwnerCommand::LocalServing(range,phase,generic,send)=> {
                        let request=parent.child_with_deadline(Instant::now()+Duration::from_secs(4));
                        let result=if generic {target.test_local_policy_generic_refusal(range,phase,&request)}
                            else {target.advance_finite_remote_serving_policy(range,phase,&request)};
                        let _=send.try_send(release_tests::policy_return_data(result));
                    },
                    OwnerCommand::RetireReferences(range,phase,send)=> {
                        let request=parent.child_with_deadline(Instant::now()+Duration::from_secs(4));
                        let actual=target.retire_finite_remote_open_references(range,phase,&request);
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    },
                    OwnerCommand::ReleaseControl(range,mode,send)=> {
                        let actual=target.test_remote_release_control(range,mode);
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    },
                    OwnerCommand::OpenControl(range,mode,send)=> {
                        let actual=target.test_remote_open_control(range,mode);
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    },
                    OwnerCommand::BridgeControl(range,arm,send)=> {
                        let actual=target.test_remote_bridge_control(range,arm);
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    },
                    OwnerCommand::PreparationControl(range,arm,send)=> {
                        let actual=target.test_remote_preparation_control(range,arm);
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    },
                    OwnerCommand::NativeDecision(range,arm,send)=> {
                        let actual=target.test_first_native_decision(range,arm);
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    },
                    OwnerCommand::Snapshot(send)=> {
                        let actual=target.test_first_receiver_snapshot();
                        let _=send.try_send(serde_json::json!({"ok":actual.is_ok(),"data":actual.as_ref().ok(),
                            "error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    }}
                }
                if let Err(error) = target.poll_native_first_receiver(&parent) {
                    if worker_errors.len() < 64 {
                        worker_errors.push(
                            crate::distribution_native_host::remote_first::error_data(&error),
                        );
                    } else {
                        worker_omitted += 1;
                    }
                }
                if matches!(case,Case::Preparation|Case::PreparationCancelledAfterNative|Case::PreparationCommitUnknown|Case::PreparationHostPollCancelled|Case::PreparationHostPollExpired|Case::Bridge|Case::BridgeMissingPrepared|Case::BridgeCancelledAfterNative|Case::BridgeCommitUnknown|Case::Open|Case::OpenR2Progress|Case::OpenLaterIoFailed|Case::OpenCancelledAfterIo|Case::OpenIoUnknown|Case::Release|Case::ReleaseMissingOpen|Case::ReleaseCancelledAfterDrop|Case::Serving|Case::ServingMissingReleased|Case::ServingSuccessorRead|Case::ServingDrainResume|Case::ServingPartialDrainResume|Case::ServingMetadataActivation|Case::ServingActivationMissingReleased) {
                    // Every real poll has its own caller token; expiring or
                    // canceling it must not stop the retained owner/process.
                    let window=if case==Case::PreparationHostPollExpired {Duration::from_millis(100)} else {Duration::from_secs(5)};
                    let actor_poll=parent.child_with_deadline(Instant::now()+window);
                    if let Err(error)=target.poll_finite_remote_serving(&actor_poll) {
                        if worker_errors.len()<64 {worker_errors.push(crate::distribution_native_host::remote_first::error_data(&error));}
                        else {worker_omitted+=1;}
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            (target, worker_errors, worker_omitted)
        });
        let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let harness_seconds = if case == Case::Over512 { 900 } else { 120 };
            let harness_deadline = Instant::now() + Duration::from_secs(harness_seconds);
            report.event(
                serde_json::json!({"harness_next_step_admission_bound_seconds":harness_seconds,
                "per_step_original_budget_seconds":5,"network_io_ms":1500,"sdk_timeout_ms":2000}),
            );
            macro_rules! drive {
                ($name:expr,$action:expr $(,)?) => {{
                    if Instant::now() >= harness_deadline {
                        return Err(invalid(
                            "FIRST new component harness completion bound reached before next step",
                        ));
                    }
                    report.data_call($name, || {
                        five.sources[coordinator].drive_finite_remote_first(
                            selected.id,
                            $action,
                            &five.lifetime,
                        )
                    })
                }};
            }
            macro_rules! receive {
                ($name:expr,$operation:expr,$frame:expr $(,)?) => {
                    drive!(
                        $name,
                        Action::Receive {
                            operation: $operation,
                            frame: Some($frame)
                        }
                    )
                };
            }
            drive!("actual_target_N_fence", Action::FenceTarget)?;
            if case == Case::ConcurrentAdmission {
                let actual = report.data_call(
                    "actual_source_factory_two_concurrent_TLS_requests",
                    || {
                        five.sources[coordinator].test_first_concurrent_admission(
                            selected.id,
                            barrier.as_ref().unwrap(),
                            &five.lifetime,
                        )
                    },
                )?;
                let target_after = report
                    .data_call("actual_target_after_original_and_denied_concurrent", || {
                        snapshot(&owner_send)
                    })?;
                assert_eq!(actual["actual_returns"]["barrier"]["entered"], true);
                assert_eq!(
                    actual["actual_returns"]["barrier"]["event"]["original_window_live_after_hold"],
                    true,
                    "hold must not outlive original ingress"
                );
                assert_eq!(
                    actual["actual_returns"]["first_ok"], true,
                    "original request failed; preserve actual result"
                );
                assert_eq!(actual["original_postchecks_ok"], true);
                assert_eq!(actual["actual_returns"]["second_ok"], false);
                assert!(actual["actual_returns"]["second_error"]["display_prefix"]
                    .as_str()
                    .is_some_and(
                        |e| e.contains("FIRST original request is still pending/in flight")
                    ));
                assert_eq!(
                    target_after["attempts"].as_array().unwrap().len(),
                    1,
                    "only original A"
                );
                assert_eq!(
                    target_after["queue"]["admission"]["admitted_watermark"],
                    actual["actual_returns"]["first_request"]["sequence"]
                );
                assert_eq!(
                    target_after["attempts"][0]["attempt"],
                    actual["actual_returns"]["first_reply"]["target_attempt"]
                );
            } else if matches!(
                case,
                Case::LostEnroll | Case::CancelledQueued | Case::ExpiredQueued
            ) {
                let original = drive!(
                    "actual_enroll_with_original_server_cut",
                    Action::EnrollTarget
                );
                let source_before = report
                    .data_call("source_original_unknown_before_observation", || {
                        five.sources[coordinator].test_first_coordinator_snapshot(selected.id)
                    })?;
                let target_before = report
                    .data_call("target_actual_cut_before_observation", || {
                        snapshot(&owner_send)
                    })?;
                assert!(
                    original.is_err(),
                    "actual original cut must refuse client success"
                );
                assert_eq!(
                    target_before["queue"]["control"]["fired"], true,
                    "actual server seam required"
                );
                let old: crate::distribution_native_host::NativeFirstReceiveRequestV1 =
                    serde_json::from_value(source_before["pending_receive"].clone())?;
                if case == Case::LostEnroll {
                    assert_eq!(source_before["target_attempt"], serde_json::Value::Null);
                    assert_eq!(
                        target_before["queue"]["admission"]["last_return"]["classification"],
                        "returned_success_after_postchecks"
                    );
                    assert_eq!(target_before["attempts"].as_array().unwrap().len(), 1);
                } else {
                    assert!(
                        target_before["attempts"].as_array().unwrap().is_empty(),
                        "no A on original queued refusal"
                    );
                    if case == Case::CancelledQueued {
                        assert_eq!(
                            target_before["queue"]["control"]
                                ["original_server_cancelled_at_snapshot"],
                            true
                        );
                    }
                    if case == Case::ExpiredQueued {
                        assert_eq!(
                            target_before["queue"]["control"]
                                ["original_server_deadline_expired_at_snapshot"],
                            true
                        );
                    }
                }
                let observed = drive!(
                    "fresh_readonly_same_operation_original_sequence",
                    Action::ObservePending
                )?;
                let after = report
                    .data_call("source_actual_A_after_readonly_observation", || {
                        five.sources[coordinator].test_first_coordinator_snapshot(selected.id)
                    })?;
                if case == Case::LostEnroll {
                    assert_eq!(
                        after["target_attempt"], target_before["attempts"][0]["attempt"],
                        "same retained A, no enrollment replay"
                    );
                    assert_eq!(
                        observed["classification"],
                        "returned_success_after_postchecks"
                    );
                } else {
                    assert_eq!(
                        observed["classification"],
                        "rejected_before_first_image_or_capture_use"
                    );
                }
                let before_replay =
                    report.data_call("target_before_consumed_replay", || snapshot(&owner_send))?;
                let old_shape = old.clone();
                let replay = report.data_call("actual_TLS_consumed_original_request", || {
                    five.sources[coordinator].test_first_replay_request(
                        selected.id,
                        old,
                        &five.lifetime,
                    )
                });
                let after_replay =
                    report.data_call("target_after_consumed_replay", || snapshot(&owner_send))?;
                assert!(replay.as_ref().err().is_some_and(|e| e
                    .to_string()
                    .contains("FIRST request sequence already consumed")));
                assert_eq!(
                    before_replay["queue"]["actual_witness_attempts"],
                    after_replay["queue"]["actual_witness_attempts"],
                    "denied before extra FIRST witness"
                );
                assert_eq!(
                    before_replay["queue"]["admission"], after_replay["queue"]["admission"],
                    "no new effect/timer/admission for consumed sequence"
                );
                let mut new_shape = old_shape;
                new_shape.challenge = Uuid::now_v7();
                let changed =
                    report.data_call("actual_TLS_consumed_sequence_changed_shape", || {
                        five.sources[coordinator].test_first_replay_request(
                            selected.id,
                            new_shape,
                            &five.lifetime,
                        )
                    });
                let after_changed = report
                    .data_call("target_after_changed_shape_consumed_request", || {
                        snapshot(&owner_send)
                    })?;
                assert!(changed.as_ref().err().is_some_and(|e| e
                    .to_string()
                    .contains("FIRST request sequence already consumed")));
                assert_eq!(
                    after_replay["queue"], after_changed["queue"],
                    "changed body never renews consumed step or witness"
                );
                if case != Case::LostEnroll {
                    drive!(
                        "new_authenticated_enroll_after_actual_no_effect",
                        Action::EnrollTarget
                    )?;
                }
            } else {
                drive!("actual_first_enroll", Action::EnrollTarget)?;
            }
            let enrolled_request = if case == Case::Over512 {
                let actual = report
                    .data_call("original_enroll_before_history_compaction", || {
                        five.sources[coordinator].test_first_coordinator_snapshot(selected.id)
                    })?;
                Some(serde_json::from_value::<
                    crate::distribution_native_host::NativeFirstReceiveRequestV1,
                >(actual["last_return"]["intent"].clone())?)
            } else {
                None
            };
            drive!("actual_ALL_OLD_constrain", Action::ConstrainAll)?;
            if case == Case::LostClose {
                let first = drive!("actual_Close_then_wire_reply_loss", Action::CloseAll);
                let source_unknown = report.data_call("source_original_Close_unknown", || {
                    five.sources[coordinator].test_first_coordinator_snapshot(selected.id)
                })?;
                let before = report
                    .data_call("actual_OLD_original_close_before_resolution", || {
                        five.sources[close_peer_index].test_first_close_loss_snapshot(selected.id)
                    })?;
                assert!(first.is_err());
                assert_eq!(
                    before["event"]["fired"], true,
                    "real successful Close wire cut required"
                );
                assert_eq!(before["close_dispatches"], 1);
                assert_eq!(source_unknown["pending_control"]["kind"], "Close");
                let repeated = drive!(
                    "unresolved_Close_mutation_replay_must_refuse",
                    Action::CloseAll
                );
                let still = report.data_call("actual_OLD_after_refused_Close_replay", || {
                    five.sources[close_peer_index].test_first_close_loss_snapshot(selected.id)
                })?;
                assert!(repeated.as_ref().err().is_some_and(|e| e
                    .to_string()
                    .contains("unresolved Close requires readonly observation")));
                assert_eq!(before, still, "no second Close dispatched");
                let observed = drive!(
                    "fresh_actual_readonly_original_Close_resolution",
                    Action::ObservePendingClose
                )?;
                let after = report
                    .data_call("actual_OLD_after_readonly_Close_resolution", || {
                        five.sources[close_peer_index].test_first_close_loss_snapshot(selected.id)
                    })?;
                assert_eq!(after["close_dispatches"], 1);
                assert_eq!(after["observe_dispatches"], 1);
                assert_eq!(observed["new_Close_issued"], false);
                assert_eq!(observed["original_call_relabelled_success"], false);
                assert_eq!(
                    observed["original_unknown_return"],
                    source_unknown["pending_original_return"]
                );
                let old = &before["event"]["actual_reply"];
                assert_eq!(old["closed"], observed["readonly_actual_reply"]["closed"]);
                assert_eq!(
                    old["closed_native_record"],
                    observed["readonly_actual_reply"]["closed_native_record"]
                );
                let encoded: Vec<u8> =
                    serde_json::from_value(old["closed_native_record"]["encoded"].clone())?;
                let record: serde_json::Value = serde_json::from_slice(&encoded)?;
                assert!(record["native_page_xid"].as_u64().is_some_and(|n| n > 0));
                drive!(
                    "actual_remaining_OLD_Close_after_known_original",
                    Action::CloseAll
                )?;
            } else {
                drive!("actual_ALL_OLD_close", Action::CloseAll)?;
            }
            if matches!(
                case,
                Case::OldRequestWait | Case::OldRequestCancel | Case::OldRequestExpiry
            ) {
                return request_wait_tests::old(
                    &five.sources[donor_index],
                    selected.id,
                    case,
                    report,
                    &five.lifetime,
                );
            }
            let start = drive!(
                "actual_target_owned_SourceStart",
                Action::Receive {
                    operation: Op::Start,
                    frame: None
                }
            );
            let donor_after_start = if case == Case::LostStart {
                Some(
                    report.data_call("actual_donor_retained_export_after_lost_Start", || {
                        five.sources[donor_index].test_first_donor_snapshot(selected.id)
                    })?,
                )
            } else {
                None
            };
            if case == Case::LostStart {
                assert!(start.is_err(), "actual donor reply must be lost");
            }
            let describe = drive!(
                "actual_target_owned_SourceDescribe",
                Action::Receive {
                    operation: Op::Describe,
                    frame: None
                }
            )?;
            if let Ok(manifest) = start {
                assert_eq!(manifest, describe, "actual SAME source/MF/session/A");
            }
            if let Some(before) = donor_after_start {
                let after = report.data_call("actual_donor_same_export_after_Describe", || {
                    five.sources[donor_index].test_first_donor_snapshot(selected.id)
                })?;
                assert_eq!(
                    before, after,
                    "Describe must retain same actual donor export"
                );
                assert_eq!(
                    describe["target_attempt"],
                    five.sources[coordinator].test_first_coordinator_snapshot(selected.id)?
                        ["target_attempt"]
                );
            }
            for (name, operation) in [
                ("actual_transfer_N_to_A", Op::Transfer),
                ("actual_proposal", Op::Proposal),
                ("actual_Installing", Op::Installing),
            ] {
                // A live first History frame is sufficient for these MF-
                // bound target permissions. Finish is unavailable until the
                // real source history AND rows have actually been exported.
                receive!(
                    name,
                    operation,
                    history(RangeImageHistoryCursorV1::default())
                )?;
            }
            sdk(
                report,
                &client,
                &key(&five.topology, untouched.id, "during-Installing-A")?,
                serde_json::json!({"actual":"untouched-during-Installing-A"}),
            )?;
            let mut progress = receive!(
                "actual_begin_history",
                Op::BeginHistory,
                history(RangeImageHistoryCursorV1::default()),
            )?;
            let mut last_history_request = RangeImageHistoryCursorV1::default();
            loop {
                let h = progress
                    .get("History")
                    .ok_or_else(|| invalid("FIRST actual history return absent"))?;
                let cursor: RangeImageHistoryCursorV1 =
                    serde_json::from_value(h["cursor"].clone())?;
                if cursor.complete() {
                    break;
                }
                last_history_request = cursor.clone();
                progress = receive!("actual_append_history", Op::AppendHistory, history(cursor))?;
            }
            loop {
                progress = receive!(
                    "actual_audit_history",
                    Op::AuditHistory { max_entries: 256 },
                    history(last_history_request.clone()),
                )?;
                if progress["History"]["audited"] == true {
                    break;
                }
            }
            let mut cursor = RangeImageCursorV1::default();
            loop {
                let value = receive!(
                    "actual_apply_rows",
                    Op::ApplyRows,
                    Frame::Rows {
                        cursor,
                        options: if case == Case::Over512 {
                            crate::RangeSnapshotOptions {
                                max_records_per_batch: 1,
                                ..Default::default()
                            }
                        } else {
                            Default::default()
                        },
                    },
                )?;
                let rows = value
                    .get("Rows")
                    .ok_or_else(|| invalid("FIRST actual row return absent"))?;
                cursor = serde_json::from_value(rows["cursor"].clone())?;
                if rows["complete"] == true {
                    break;
                }
            }
            sdk(
                report,
                &client,
                &key(&five.topology, untouched.id, "after-Rows-before-scan")?,
                serde_json::json!({"actual":"untouched-after-Rows-before-scan"}),
            )?;
            if case == Case::Over512 {
                let actual = report.data_call("actual_over512_admission_and_rows", || {
                    snapshot(&owner_send)
                })?;
                assert!(actual["queue"]["admission"]["returned_count"]
                    .as_u64()
                    .is_some_and(|n| n > 512));
                assert!(
                    actual["native_states"][0]["state"]["rows"]["progress"]["records"]
                        .as_u64()
                        .is_some_and(|n| n >= 514)
                );
                let source = report
                    .data_call("actual_latest_row_request_after_compaction", || {
                        five.sources[coordinator].test_first_coordinator_snapshot(selected.id)
                    })?;
                let equal = serde_json::from_value::<
                    crate::distribution_native_host::NativeFirstReceiveRequestV1,
                >(source["last_return"]["intent"].clone())?;
                for old in [
                    enrolled_request.expect("retained original actual enrollment DATA"),
                    equal,
                ] {
                    let before = report.data_call("actual_before_old_or_equal_replay", || {
                        snapshot(&owner_send)
                    })?;
                    let denied = report.data_call("actual_TLS_old_or_equal_after_over512", || {
                        five.sources[coordinator].test_first_replay_request(
                            selected.id,
                            old,
                            &five.lifetime,
                        )
                    });
                    let after = report
                        .data_call("actual_after_old_or_equal_replay", || snapshot(&owner_send))?;
                    assert!(denied.as_ref().err().is_some_and(|e| e
                        .to_string()
                        .contains("FIRST request sequence already consumed")));
                    assert_eq!(
                        before["queue"], after["queue"],
                        "no witness, deadline/admission or effect for consumed sequence"
                    );
                    assert_eq!(
                        before["native_states"], after["native_states"],
                        "same actual native row XID/header"
                    );
                    assert_eq!(
                        before["custody"], after["custody"],
                        "same actual original A/Frozen"
                    );
                }
            }
            receive!(
                "actual_begin_verification",
                Op::BeginVerification,
                Frame::Finish,
            )?;
            loop {
                let value = receive!(
                    "actual_verify_target",
                    Op::VerifyTarget {
                        max_scanned_records: 256,
                    },
                    Frame::Finish,
                )?;
                if value["Verification"]["status"] == "Complete" {
                    break;
                }
            }
            if case == Case::UnknownFinal {
                let original = receive!(
                    "actual_final_WAL_then_original_server_cancel",
                    Op::PublishFinalImage,
                    Frame::Finish
                );
                let before = report.data_call("actual_native_final_unknown_custody", || {
                    snapshot(&owner_send)
                })?;
                assert!(original.is_err());
                assert_eq!(
                    before["queue"]["control"]["cut"],
                    "actual_native_final_return_then_original_server_cancel"
                );
                assert_eq!(
                    before["queue"]["control"]["fired"], true,
                    "genuine WAL seam required"
                );
                let record = &before["native_forensics"][0]["actual_native"]["current"];
                assert!(
                    record["native_page_xid"].as_u64().is_some_and(|x| x > 0),
                    "actual retained returned XID required"
                );
                assert_eq!(
                    record,
                    &before["native_forensics"][0]["actual_native"]["attempt_archive"]
                );
                let xid = record["native_page_xid"].as_u64().unwrap();
                assert_eq!(
                    before["native_forensics"][0]["actual_native"]
                        ["native_tx_status_api_observations"][xid.to_string()],
                    "Committed"
                );
                assert_eq!(
                    before["native_states"][0]["state"]["final_image"]["actual_commit_returned"]
                        .as_u64(),
                    Some(xid),
                    "actual final-image commit return must match the protected committed XID"
                );
                assert_eq!(
                    before["native_states"][0]["state"]["final_image"]["state"],
                    "native_commit_returned_before_postchecks"
                );
                assert!(record["source_seal"]["record_count"]
                    .as_u64()
                    .is_some_and(|n| n > 0));
                let forbidden =
                    report.data_call("actual_source_fresh_forbidden_next_mutation", || {
                        five.sources[coordinator]
                            .test_first_new_mutation_after_unknown(selected.id, &five.lifetime)
                    });
                let after = report
                    .data_call("actual_native_final_after_forbidden_mutation", || {
                        snapshot(&owner_send)
                    })?;
                assert!(forbidden.as_ref().err().is_some_and(|e| e
                    .to_string()
                    .contains("FIRST prior native outcome is unresolved")));
                assert_eq!(
                    before["native_forensics"], after["native_forensics"],
                    "same original final record/XID, no native replay"
                );
                assert_eq!(
                    before["custody"], after["custody"],
                    "original actual A/Frozen custody retained"
                );
                assert_eq!(
                    before["queue"]["admission"], after["queue"]["admission"],
                    "unknown gate denies before another owner-thread step"
                );
                return Ok(());
            }
            for (name, operation) in [
                ("actual_final_image", Op::PublishFinalImage),
                ("actual_native_base", Op::PublishNativeBase),
                ("actual_catalogue_floor", Op::AdoptCatalogueFloor),
                ("actual_materialize", Op::Materialize),
            ] {
                receive!(name, operation, Frame::Finish)?;
            }
            if matches!(
                case,
                Case::NativeDecision
                    | Case::LostExport
                    | Case::LostNativeDecision
                    | Case::UnknownNativeDecision
                    | Case::Catalogue
                    | Case::CatalogueKnownNativeRead
                    | Case::CatalogueLostReply
                    | Case::CatalogueIoUnknown
                    | Case::CatalogueRenameUnknown
                    | Case::CatalogueForeignDisk
                    | Case::CataloguePreIoCancel
                    | Case::CatalogueInterveningWrite
                    | Case::CatalogueDuplicate
                    | Case::CatalogueWrongExpected
                    | Case::CatalogueStale
                    | Case::CatalogueScope
                    | Case::CatalogueMetadataHeadDrift
                    | Case::Preparation
                    | Case::PreparationCancelledAfterNative
                    | Case::PreparationCommitUnknown
                    | Case::PreparationHostPollCancelled
                    | Case::PreparationHostPollExpired
                    | Case::Bridge
                    | Case::BridgeMissingPrepared
                    | Case::BridgeCancelledAfterNative
                    | Case::BridgeCommitUnknown
                    | Case::Open
                    | Case::OpenR2Progress
                    | Case::OpenLaterIoFailed
                    | Case::OpenCancelledAfterIo
                    | Case::OpenIoUnknown
                    | Case::Release
                    | Case::ReleaseMissingOpen
                    | Case::ReleaseCancelledAfterDrop
                    | Case::Serving
                    | Case::ServingMissingReleased
                    | Case::ServingSuccessorRead
                    | Case::ServingDrainResume
                    | Case::ServingPartialDrainResume
                    | Case::ServingMetadataActivation
                    | Case::ServingActivationMissingReleased
            ) {
                // Same five retained normal hosts; real range leader directly
                // observes OLD/survivor/Registered NEW, donor remains neither leader.
                let collected = drive!(
                    "actual_direct_coordinator_NEW_collection",
                    Action::CollectNativeNew
                )?;
                let phase = collected["phase"].clone();
                let retire = drive!(
                    "actual_target_owned_S2_retirement",
                    Action::Receive {
                        operation: Op::RetireSourceExport,
                        frame: None
                    }
                );
                if case == Case::LostExport {
                    report.event(serde_json::json!({"actual_S2_retire_return":retire.as_ref().ok(),
                        "actual_S2_retire_error":retire.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    assert!(
                        retire.is_err(),
                        "actual successful S2 reply must be suppressed"
                    );
                } else {
                    retire?;
                }
                let terminal = drive!(
                    "actual_target_owned_S2_Describe",
                    Action::Receive {
                        operation: Op::DescribeSourceExport,
                        frame: None
                    }
                )?;
                report.event(serde_json::json!({"actual_S2_terminal":terminal,"original_MF":describe,"phase":phase}));
                let mut participants = selected
                    .replicas
                    .iter()
                    .filter(|r| r.role == RangeReplicaRole::Voter)
                    .map(|r| r.node_id.clone())
                    .collect::<Vec<_>>();
                participants.push(node(4));
                for participant in &participants {
                    drive!(
                        "actual_recipient_prepare_original_pins",
                        Action::PrepareNativeMember {
                            node: participant.clone()
                        }
                    )?;
                }
                if matches!(
                    case,
                    Case::LostNativeDecision
                        | Case::UnknownNativeDecision
                        | Case::CatalogueKnownNativeRead
                ) {
                    report.data_call("arm_actual_original_target_native_cut", || {
                        native_command(
                            &owner_send,
                            selected.id,
                            Some(if case == Case::LostNativeDecision {
                                4
                            } else if case == Case::CatalogueKnownNativeRead {
                                2
                            } else {
                                3
                            }),
                        )
                    })?;
                }
                let target_result = drive!(
                    "actual_target_native_DECISION",
                    Action::PublishNativeMember { node: node(4) }
                );
                let before = report
                    .data_call("actual_target_native_return_before_assertions", || {
                        native_command(&owner_send, selected.id, None)
                    })?;
                report.event(serde_json::json!({"actual_target_D_return":target_result.as_ref().ok(),
                    "actual_target_D_error":target_result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                    "original_target_native":before,"phase":phase,"no_globalQQ_Open_release_Serving":true}));
                if case == Case::UnknownNativeDecision {
                    let replay = drive!(
                        "native_Unknown_mutation_refuses_without_replay",
                        Action::PublishNativeMember { node: node(4) }
                    );
                    let observe = drive!(
                        "native_Unknown_readonly_must_not_promote_visible_bytes",
                        Action::ObserveNativeDecision { node: node(4) }
                    );
                    let after = report
                        .data_call("native_Unknown_same_attempt_after_refusals", || {
                            native_command(&owner_send, selected.id, None)
                        })?;
                    report.event(serde_json::json!({"replay_error":replay.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                        "readonly_error":observe.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),"after":after}));
                    assert!(target_result.is_err());
                    assert!(replay.is_err());
                    assert!(observe.is_err());
                    assert_eq!(before["native"]["actual_commit_sync_fault_count"], 1);
                    assert_eq!(
                        before["native"]["actual_native_attempt"]["outcome"]["state"],
                        "unknown_effect"
                    );
                    assert_eq!(before["successful_sink_return_retained"], false);
                    assert_eq!(
                        before["native"], after["native"],
                        "no new native XID or outcome promotion"
                    );
                    return Ok(());
                }
                if case == Case::LostNativeDecision {
                    let observed = drive!(
                        "fresh_authenticated_same_original_native_D_observation",
                        Action::ObserveNativeDecision { node: node(4) }
                    )?;
                    let after = report
                        .data_call("same_native_attempt_after_readonly_resolution", || {
                            native_command(&owner_send, selected.id, None)
                        })?;
                    report.event(
                        serde_json::json!({"actual_readonly_resolution":observed,"after":after}),
                    );
                    assert!(
                        target_result.is_err(),
                        "actual successful native wire reply must be suppressed"
                    );
                    assert_eq!(before["successful_sink_return_retained"], true);
                    assert!(before["actual_success_reply_lost"]["native_page_xid"]
                        .as_u64()
                        .is_some_and(|x| x > 0));
                    assert_eq!(
                        before["native"], after["native"],
                        "readonly must not publish another XID"
                    );
                    assert_eq!(
                        before["actual_success_reply_lost"],
                        observed["actual_reply"]["native_decision"]
                    );
                } else if case == Case::CatalogueKnownNativeRead {
                    let recovered = drive!(
                        "actual_known_commit_late_error_fresh_readback",
                        Action::ObserveNativeDecision { node: node(4) }
                    )?;
                    let after = report
                        .data_call("actual_known_commit_same_attempt_after_readback", || {
                            native_command(&owner_send, selected.id, None)
                        })?;
                    report.event(serde_json::json!({"actual_fresh_readback":recovered,"before":before,"after":after,
                        "new_native_write_or_old_request_revival":false}));
                    assert!(
                        target_result.is_err(),
                        "actual post-commit cut must fail original helper"
                    );
                    assert_eq!(before["actual_test_cut"]["code"], 2);
                    assert_eq!(before["actual_test_cut"]["fired"], true);
                    assert_eq!(before["successful_sink_return_retained"], false);
                    assert_eq!(
                        before["native"]["actual_native_attempt"]["outcome"]["state"],
                        "actual_commit_returned"
                    );
                    assert_eq!(after["successful_sink_return_retained"], true);
                    assert_eq!(
                        before["native"], after["native"],
                        "fresh read must preserve original xid/body/provenance/outcome"
                    );
                    assert_eq!(
                        recovered["actual_reply"]["native_decision"]["native_page_xid"],
                        before["native"]["actual_issued_xid"]
                    );
                } else if is_catalogue_case(case) && target_result.is_err() {
                    // A NEW genuine protected KnownRead may resolve private
                    // KnownCommitted only; actual sync Unknown stays blocked.
                    drive!(
                        "fresh_actual_target_native_KnownRead",
                        Action::ObserveNativeDecision { node: node(4) }
                    )?;
                } else {
                    target_result?;
                }
                for participant in participants.iter().filter(|id| **id != node(4)) {
                    let original = drive!(
                        "actual_original_OLD_recipient_native_DECISION",
                        Action::PublishNativeMember {
                            node: participant.clone()
                        }
                    );
                    if is_catalogue_case(case) && original.is_err() {
                        drive!(
                            "fresh_actual_OLD_native_KnownRead",
                            Action::ObserveNativeDecision {
                                node: participant.clone()
                            }
                        )?;
                    } else {
                        original?;
                    }
                }
                if is_catalogue_case(case) {
                    // The SAME native operation may have returned Err only
                    // after real commit; an actual NEW KnownRead above is the
                    // only permitted resolution. No native re-commit here.
                    if matches!(
                        case,
                        Case::CatalogueLostReply
                            | Case::CatalogueIoUnknown
                            | Case::CatalogueRenameUnknown
                            | Case::CatalogueForeignDisk
                            | Case::CataloguePreIoCancel
                            | Case::CatalogueInterveningWrite
                    ) {
                        native_command(
                            &owner_send,
                            selected.id,
                            Some(if case == Case::CatalogueLostReply {
                                5
                            } else if case == Case::CatalogueRenameUnknown {
                                9
                            } else if case == Case::CataloguePreIoCancel {
                                10
                            } else {
                                6
                            }),
                        )?;
                    }
                    let before_catalogue = native_command(&owner_send, selected.id, None)?;
                    let target_catalogue = drive!(
                        "actual_original_target_catalogue",
                        Action::PublishCatalogueMember { node: node(4) }
                    );
                    let after_catalogue = native_command(&owner_send, selected.id, None)?;
                    report.event(serde_json::json!({"catalogue_call_ok":target_catalogue.as_ref().ok(),
                        "catalogue_call_error":target_catalogue.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                        "original_before":before_catalogue,"original_after":after_catalogue}));
                    assert_eq!(
                        before_catalogue["native"], after_catalogue["native"],
                        "catalogue must not begin or commit another native D"
                    );
                    if case == Case::CataloguePreIoCancel {
                        assert!(target_catalogue.is_err());
                        assert_eq!(after_catalogue["catalogue"]["actual_pre_io_cancel"], true);
                        assert_eq!(
                            after_catalogue["catalogue"]["pre_io_cancel_evidence"]
                                ["all_actual_contexts_live_before_cut"],
                            true
                        );
                        assert!(after_catalogue["catalogue"]["pre_io_cancel_evidence"]
                            ["role_remaining_us"]
                            .as_u64()
                            .is_some_and(|n| n > 0));
                        assert!(after_catalogue["catalogue"]["pre_io_cancel_evidence"]
                            ["own_remaining_us"]
                            .as_u64()
                            .is_some_and(|n| n > 0));
                        assert_eq!(after_catalogue["catalogue"]["owns_uncertainty"], true);
                        assert!(after_catalogue["catalogue"]["pending_generation"].is_null());
                        assert!(after_catalogue["catalogue"]["verified_generation"].is_null());
                        assert_eq!(
                            after_catalogue["catalogue"]["IO_attempts"]
                                .as_array()
                                .unwrap()
                                .len(),
                            0
                        );
                        let retry = drive!(
                            "actual_postclone_cancel_fresh_same_candidate_retry",
                            Action::ResumeCatalogueMember { node: node(4) }
                        );
                        let after = native_command(&owner_send, selected.id, None)?;
                        report.event(serde_json::json!({"fresh_retry":retry.as_ref().ok(),"fresh_error":retry.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),"after":after}));
                        retry?;
                        assert_eq!(after_catalogue["native"], after["native"]);
                        assert_eq!(
                            after_catalogue["catalogue"]["candidate_sha256"],
                            after["catalogue"]["candidate_sha256"]
                        );
                        assert_eq!(
                            after["catalogue"]["IO_attempts"].as_array().unwrap().len(),
                            1
                        );
                        assert_eq!(after["catalogue"]["owns_uncertainty"], false);
                    } else if case == Case::CatalogueLostReply {
                        assert!(target_catalogue.is_err());
                        assert!(
                            after_catalogue["actual_catalogue_success_reply_lost"].is_object(),
                            "cut requires actual successful returned catalogue receipt"
                        );
                        let observed = drive!(
                            "actual_readonly_same_known_catalogue",
                            Action::ObserveCatalogueMember { node: node(4) }
                        )?;
                        let after = native_command(&owner_send, selected.id, None)?;
                        assert_eq!(after_catalogue["native"], after["native"]);
                        assert_eq!(
                            after_catalogue["catalogue"], after["catalogue"],
                            "readonly known observation performs no IO rewrite"
                        );
                        assert_eq!(
                            after_catalogue["actual_catalogue_success_reply_lost"],
                            observed["actual_reply"]["catalogue_decision"]
                        );
                    } else if matches!(
                        case,
                        Case::CatalogueIoUnknown
                            | Case::CatalogueInterveningWrite
                            | Case::CatalogueRenameUnknown
                            | Case::CatalogueForeignDisk
                    ) {
                        assert!(target_catalogue.is_err());
                        if case == Case::CatalogueRenameUnknown {
                            assert_eq!(
                                after_catalogue["actual_catalogue_rename_fault_count"], 1,
                                "actual before-rename IO fault reached"
                            );
                        } else {
                            assert_eq!(
                                after_catalogue["actual_catalogue_dirsync_fault_count"], 1,
                                "real named storage dirsync fault reached"
                            );
                        }
                        assert_eq!(after_catalogue["catalogue"]["owns_uncertainty"], true);
                        assert!(after_catalogue["catalogue"]["verified_generation"].is_null());
                        let observe = drive!(
                            "uncertain_catalogue_visible_bytes_not_success",
                            Action::ObserveCatalogueMember { node: node(4) }
                        );
                        let foreign = participants
                            .iter()
                            .find(|n| **n != node(4))
                            .unwrap()
                            .clone();
                        let wrong_owner = drive!(
                            "uncertain_catalogue_wrong_original_recipient_refuses",
                            Action::ResumeCatalogueMember { node: foreign }
                        );
                        let refused = native_command(&owner_send, selected.id, None)?;
                        report.event(serde_json::json!({"observe_error":observe.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                            "wrong_original_recipient_error":wrong_owner.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),"after_refusals":refused}));
                        assert!(observe.is_err());
                        assert!(wrong_owner.is_err());
                        assert_eq!(after_catalogue["catalogue"],refused["catalogue"],"refused observation/foreign resolution cannot clear original uncertainty");
                        assert_eq!(after_catalogue["native"], refused["native"]);
                        if case == Case::CatalogueInterveningWrite {
                            native_command(&owner_send, selected.id, Some(7))?;
                        } else if case == Case::CatalogueForeignDisk {
                            native_command(&owner_send, selected.id, Some(8))?;
                        }
                        let retry = drive!(
                            "same_owned_catalogue_candidate_fresh_retry",
                            Action::ResumeCatalogueMember { node: node(4) }
                        );
                        let after_retry = native_command(&owner_send, selected.id, None)?;
                        report.event(serde_json::json!({"fresh_retry":retry.as_ref().ok(),"fresh_retry_error":retry.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),"after_retry":after_retry}));
                        assert_eq!(
                            after_catalogue["native"], after_retry["native"],
                            "same original native xid/body/provenance"
                        );
                        assert_eq!(
                            after_catalogue["catalogue"]["candidate_sha256"],
                            after_retry["catalogue"]["candidate_sha256"]
                        );
                        if case == Case::CatalogueForeignDisk {
                            assert!(retry
                                .as_ref()
                                .err()
                                .is_some_and(|e| e.to_string().contains("disk preimage")));
                            assert!(after_retry["catalogue"]["actual_injected_disk_sha256"]
                                .as_str()
                                .is_some());
                            assert_eq!(
                                after_retry["catalogue"]["actual_disk_after_preimage_check"],
                                after_retry["catalogue"]["actual_injected_disk_sha256"],
                                "actual different valid disk head preserved after refusal"
                            );
                            assert_eq!(
                                after_catalogue["catalogue"]["pending_generation"],
                                after_retry["catalogue"]["pending_generation"],
                                "explicit disk fault does not change RAM write generation"
                            );
                            assert_eq!(
                                after_catalogue["catalogue"]["IO_attempts"],
                                after_retry["catalogue"]["IO_attempts"],
                                "old owner never enters replacement over actual third head"
                            );
                            assert_eq!(after_retry["catalogue"]["owns_uncertainty"], true);
                            assert!(after_retry["catalogue"]["verified_generation"].is_null());
                            return Ok(()); // explicit real-disk fault remains closed
                        }
                        if case == Case::CatalogueInterveningWrite {
                            assert!(retry
                                .as_ref()
                                .err()
                                .is_some_and(|e| e.to_string().contains("superseded")));
                            assert_eq!(
                                after_retry["catalogue"]["actual_intervening_write_count"],
                                1
                            );
                            assert_eq!(after_retry["catalogue"]["owns_uncertainty"], true);
                            assert!(after_retry["catalogue"]["verified_generation"].is_null());
                            return Ok(()); // this real stale owner remains closed
                        }
                        retry?;
                        assert_eq!(after_retry["catalogue"]["owns_uncertainty"], false);
                        assert!(after_retry["catalogue"]["verified_generation"]
                            .as_u64()
                            .is_some());
                    } else {
                        target_catalogue?;
                    }
                    for participant in participants.iter().filter(|id| **id != node(4)) {
                        drive!(
                            "actual_original_OLD_catalogue_durable",
                            Action::PublishCatalogueMember {
                                node: participant.clone()
                            }
                        )?;
                    }
                    if matches!(
                        case,
                        Case::CatalogueDuplicate
                            | Case::CatalogueWrongExpected
                            | Case::CatalogueStale
                    ) {
                        let before = native_command(&owner_send, selected.id, None)?;
                        let mode = if case == Case::CatalogueDuplicate {
                            1
                        } else if case == Case::CatalogueWrongExpected {
                            2
                        } else {
                            3
                        };
                        let denied = report.data_call(
                            "actual_pinned_catalogue_collection_refusal",
                            || {
                                five.sources[coordinator].test_first_catalogue_collection_refusal(
                                    selected.id,
                                    mode,
                                    &five.lifetime,
                                )
                            },
                        )?;
                        let after = native_command(&owner_send, selected.id, None)?;
                        report.event(serde_json::json!({"actual_collection_control":denied,"target_before":before,"target_after":after}));
                        assert_eq!(denied["count_refused"], true);
                        assert_eq!(denied["raw_frame_fabricated"], false);
                        assert_eq!(before["native"], after["native"]);
                        assert_eq!(before["catalogue"], after["catalogue"]);
                    }
                    if case == Case::CatalogueScope {
                        let intent = five.sources[coordinator]
                            .first_catalogue_certification_intent(selected.id)?;
                        let actual_meta = (0..4).find(|i| node(*i) == metadata_leader).unwrap();
                        let before = native_command(&owner_send, selected.id, None)?;
                        let actual = report.data_call(
                            "actual_distinct_metadata_owner_scope_controls",
                            || {
                                five.sources[actual_meta].test_metadata_owner_catalogue_scope(
                                    &five.sources[coordinator],
                                    &intent,
                                    &five.lifetime,
                                )
                            },
                        )?;
                        let after = native_command(&owner_send, selected.id, None)?;
                        report.event(serde_json::json!({"actual_metadata_scope_controls":actual,"target_before":before,"target_after":after}));
                        assert_eq!(actual["all_actual_refusals_reached"], true);
                        assert_eq!(actual["actual_distinct_purpose_hash_match"], true);
                        assert_eq!(actual["metadata_scope_minted_native_grant"], false);
                        assert_eq!(before["native"], after["native"]);
                        assert_eq!(before["catalogue"], after["catalogue"]);
                    }
                    let intent = five.sources[coordinator]
                        .first_catalogue_certification_intent(selected.id)?;
                    let actual_meta = (0..4).find(|i| node(*i) == metadata_leader).unwrap();
                    if case == Case::CatalogueMetadataHeadDrift {
                        metadata_head_tests::exercise(
                            five,
                            actual_meta,
                            selected.id,
                            intent,
                            &owner_send,
                            report,
                        )?;
                    } else {
                        let preparation_scope = matches!(
                            case,
                            Case::Preparation
                                | Case::PreparationCancelledAfterNative
                                | Case::PreparationCommitUnknown
                                | Case::PreparationHostPollCancelled
                                | Case::PreparationHostPollExpired
                                | Case::Bridge
                                | Case::BridgeMissingPrepared
                                | Case::BridgeCancelledAfterNative
                                | Case::BridgeCommitUnknown
                                | Case::Open
                                | Case::OpenR2Progress
                                | Case::OpenLaterIoFailed
                                | Case::OpenCancelledAfterIo
                                | Case::OpenIoUnknown
                                | Case::Release
                                | Case::ReleaseMissingOpen
                                | Case::ReleaseCancelledAfterDrop
                                | Case::Serving
                                | Case::ServingMissingReleased
                                | Case::ServingSuccessorRead
                                | Case::ServingDrainResume
                                | Case::ServingPartialDrainResume
                                | Case::ServingMetadataActivation
                                | Case::ServingActivationMissingReleased
                        )
                        .then(|| {
                            (
                                intent.relocation_id(),
                                intent.phase_id(),
                                intent.final_member_ids(),
                            )
                        });
                        let certified = report.data_call(
                            "actual_metadata_owner_independent_dual_durability_certification",
                            || {
                                five.sources[actual_meta]
                                    .certify_finite_remote_catalogue(intent, &five.lifetime)
                            },
                        )?;
                        report.event(serde_json::json!({"actual_certified_only":certified,"automatic_initiation":false,
                        "global_Open_release_Serving":false,"later_epoch_producers_implemented":false}));
                        assert_eq!(certified["actual_committed_record"]["stage"], "Certified");
                        if let Some((relocation, phase, final_members)) = preparation_scope {
                            preparation_tests::exercise(
                                five,
                                actual_meta,
                                relocation,
                                phase,
                                selected.id,
                                &final_members,
                                &owner_send,
                                report,
                                case,
                                &client,
                                &selected_key,
                            )?;
                        }
                    }
                }
            }
            Ok(())
        }));
        stop.store(true, Ordering::Release);
        let (target, errors, omitted) = match worker.join() {
            Ok(actual) => actual,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        five.target = Some(target);
        let actual = match actual {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        report.event(serde_json::json!({"owner_thread_errors":errors,"owner_errors_omitted":omitted,
            "finite_case_result":actual.as_ref().ok(),"materialize_expected":case!=Case::UnknownFinal,
            "primary_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "global_qq_open_release_serving":false}));
        actual
    });
    result?;
    // No selected post-Close APP/Strong success is claimed without release.
    if !matches!(case, Case::UnknownFinal | Case::UnknownNativeDecision) {
        sdk(
            report,
            &client,
            &key(&five.topology, untouched.id, "after-materialize")?,
            serde_json::json!({"actual":"untouched-after-materialize"}),
        )?;
    }
    Ok(())
}
fn history(cursor: RangeImageHistoryCursorV1) -> Frame {
    Frame::History {
        cursor,
        max_entries: 256,
    }
}
fn key(topology: &ClusterTopology, range: crate::RangeId, label: &str) -> Result<String> {
    for i in 0..100_000 {
        let key = format!("normal-first-{label}-{i}");
        let record = crate::cluster_event_record_id("items", &key)?;
        if topology
            .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &record)?
            .id
            == range
        {
            return Ok(key);
        }
    }
    Err(invalid("FIRST deterministic key search exhausted"))
}
fn sdk(
    report: &mut Report,
    client: &crate::ClusterEventClient,
    key: &str,
    value: serde_json::Value,
) -> Result<()> {
    report.event(
        serde_json::json!({"stage":"original_sdk_attempt","namespace":"items","key":key,
        "record_id":crate::cluster_event_record_id("items",key)?,"expected_full_value":value,
        "request_timeout_ms":2000,"manual_route_refresh":false}),
    );
    let appended = report.data_call("original_sdk_actual_APPEND", || {
        client.append("items", key, value.clone())
    })?;
    report.event(
        serde_json::json!({"actual_successful_APP_return":true,"inserted":appended,
        "same_key_identical_internal_retry_permitted_by_stock_API":true}),
    );
    let actual = report.data_call("original_sdk_actual_full_Strong", || {
        client.get_strong("items", key)
    })?;
    assert_eq!(
        serde_json::to_value(actual)?,
        value,
        "actual original SDK full value"
    );
    Ok(())
}

#[test]
fn native_first_remote_registered_prepare_uses_actual_owner_and_tls_caller() {
    run(Case::Preparation);
}
#[test]
fn native_first_remote_prepare_actual_postcommit_server_cancel_known_read_no_replay() {
    run(Case::PreparationCancelledAfterNative);
}
#[test]
fn native_first_remote_prepare_actual_commit_sync_unknown_cannot_be_read_as_success() {
    run(Case::PreparationCommitUnknown);
}

#[test]
fn native_first_actual_host_poll_cancel_refuses_prepare_while_original_tls_request_is_live() {
    run(Case::PreparationHostPollCancelled);
}
#[test]
fn native_first_actual_host_poll_expiry_refuses_prepare_while_original_tls_request_is_live() {
    run(Case::PreparationHostPollExpired);
}

#[test]
fn native_first_remote_all_actual_preparations_then_target_native_bridge() {
    run(Case::Bridge);
}

#[test]
fn native_first_remote_bridge_refuses_actual_unprepared_final_member() {
    run(Case::BridgeMissingPrepared);
}

#[test]
fn native_first_remote_bridge_postcommit_server_cancel_resolves_same_native_xid() {
    run(Case::BridgeCancelledAfterNative);
}
#[test]
fn native_first_remote_bridge_commit_sync_unknown_remains_blocked() {
    run(Case::BridgeCommitUnknown);
}

#[test]
fn native_first_actual_remote_all_bridge_and_owned_open() {
    run(Case::Open);
}
#[test]
fn native_first_actual_open_observation_survives_stock_sdk_other_range_head_progress() {
    run(Case::OpenR2Progress);
}
#[test]
fn native_first_actual_later_same_byte_failed_io_refuses_readonly_open_projection() {
    run(Case::OpenLaterIoFailed);
}
#[test]
fn native_first_actual_open_post_io_server_cancel_resolves_owned_without_native_replay() {
    run(Case::OpenCancelledAfterIo);
}
#[test]
fn native_first_actual_open_directory_sync_unknown_requires_new_owned_fsync() {
    run(Case::OpenIoUnknown);
}

#[test]
fn native_first_actual_all_open_retirement_and_unique_release_component() {
    run(Case::Release);
}
#[test]
fn native_first_actual_release_refuses_missing_real_open_before_successor() {
    run(Case::ReleaseMissingOpen);
}
#[test]
fn native_first_actual_registered_drop_cancel_retains_owner_then_reconciles() {
    run(Case::ReleaseCancelledAfterDrop);
}

#[test]
fn native_first_actual_released_owners_to_serving_original_sdk_ack_full_strong() {
    run(Case::Serving);
}
#[test]
fn native_first_actual_local_policy_refuses_real_unreleased_member_without_native_write() {
    run(Case::ServingMissingReleased);
}

#[test]
fn native_first_actual_registered_replacement_socket_policy_read_and_original_denial() {
    run(Case::ServingSuccessorRead);
}

#[test]
fn native_first_actual_registered_cancelled_closed_drain_resumes_same_installed_owner() {
    run(Case::ServingDrainResume);
}

#[test]
fn native_first_actual_registered_shutdown_active_authenticated_peer_cancel_then_same_owner_resume()
{
    run(Case::ServingPartialDrainResume);
}

#[test]
fn native_first_actual_metadata_owner_outside_final_voters_records_released_activation() {
    run(Case::ServingMetadataActivation);
}

#[test]
fn native_first_actual_metadata_activation_refuses_original_unreleased_recipient_before_proposal() {
    run(Case::ServingActivationMissingReleased);
}
