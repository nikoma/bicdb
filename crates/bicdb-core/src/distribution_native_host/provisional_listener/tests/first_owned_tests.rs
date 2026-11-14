//! Five retained normal host OBJECTS in ONE Rust test process, with distinct
//! actual DB/TLS endpoints over real TCP; not independent processes or VMs.
//! No legacy enrollment fixture/local OLD quorum,
//! manufactured MF, backend substitute or native grant constructor is used.
use super::*;
use crate::distribution_native_host::remote_first::NativeFirstIngressFault as Fault;
use crate::distribution_native_host::{
    NativeFirstCoordinatorAction as Action, NativeFirstReceiveOperationV1 as Op,
    NativeMetadataBootstrapPlan, NativeMetadataCollection, NativeMetadataHost,
    NativeMetadataHostAction as HostAction, NativeMetadataLearnerHost,
};
use crate::distribution_range_image::{
    NativeRemoteImageSourceOperationV1 as Frame, RangeImageCursorV1, RangeImageHistoryCursorV1,
};
use std::sync::mpsc;
use std::time::Instant;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Normal,
    LostEnroll,
    CancelledQueued,
    ExpiredQueued,
    LostStart,
    UnknownFinal,
    Over512,
}
enum OwnerCommand {
    Snapshot(mpsc::SyncSender<serde_json::Value>),
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
        return Err(invalid(format!(
            "FIRST actual owner diagnostic error: {actual}"
        )));
    }
    Ok(actual["data"].clone())
}

struct Report {
    events: Vec<serde_json::Value>,
    omitted: u64,
}
impl Report {
    fn event(&mut self, value: serde_json::Value) {
        eprintln!("NATIVE_FIRST_ACTUAL {}", value);
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
        let mut sources = Vec::new();
        for i in 0..4 {
            // Every other endpoint remains actually reserved until its own
            // normal host is ready to bind. There is no free-port lottery.
            drop(reserved[i].take());
            sources.push(report.call("normal_initial_host_start", || {
                NativeMetadataHost::start_fresh_metadata_only(
                    &root.path().join(format!("normal-initial-{i}")),
                    public.clone(),
                    node(i),
                    &initial.nodes[&node(i)].address,
                    networks[i].clone(),
                    8 * 1024 * 1024,
                    &lifetime,
                )
            })?);
        }
        for action in [HostAction::Prepare, HostAction::Launch] {
            for host in &mut sources {
                report.call("normal_initial_host_action", || {
                    host.apply(action.clone(), &lifetime)
                })?;
            }
        }
        report.call("normal_actual_metadata_election", || {
            sources[0].apply(HostAction::Elect, &lifetime)
        })?;
        for host in &mut sources {
            report.call("normal_control_transition", || {
                host.apply(HostAction::EnableControl, &lifetime)
            })?;
            report.call("normal_original_supervisor", || {
                host.start_automatic_metadata(&lifetime)
            })?;
        }
        // Same ordinary public opening API for every retained normal host.
        // Each invocation retains the API's original 5s bound.
        std::thread::scope(|scope| -> Result<()> {
            let handles = sources
                .iter_mut()
                .enumerate()
                .map(|(ordinal, host)| {
                    report.event(
                        serde_json::json!({"stage":"normal_initial_open_dispatch","ordinal":ordinal,
                        "original_action_budget_ms":5000}),
                    );
                    let parent = &lifetime;
                    scope.spawn(move || host.apply(HostAction::OpenInitialData, parent))
                })
                .collect::<Vec<_>>();
            for handle in handles {
                let result = match handle.join() {
                    Ok(actual) => actual,
                    Err(panic) => std::panic::resume_unwind(panic),
                };
                report.call("normal_initial_data_return", || result)?;
            }
            Ok(())
        })?;
        let mut fifth = crate::ClusterNode::new(node(4), fifth_address.clone(), 1, 1 << 30, 10)?
            .as_metadata_learner()
            .with_tls_certificate_sha256(
                crate::replication_transport::replication_certificate_sha256(
                    &networks[4].tls.as_ref().unwrap().cert_path,
                )?,
            )?;
        fifth.labels = initial.nodes[&node(0)].labels.clone();
        report.call("actual_tls_register_metadata_learner", || {
            sources[0].test_first_register_learner(fifth, &lifetime)
        })?;
        drop(reserved[4].take());
        let mut target = report.call("normal_join_fresh_host_start", || {
            NativeMetadataLearnerHost::start_fresh_metadata_only(
                &root.path().join("normal-join-fresh"),
                initial.cluster_id.clone(),
                node(4),
                &fifth_address,
                1,
                node(0),
                &initial.nodes[&node(0)].address,
                networks[4].clone(),
                8 * 1024 * 1024,
                &lifetime,
            )
        })?;
        report.call("normal_native_birth_enrollment", || {
            sources[0].apply(HostAction::EnrollLearner { node: node(4) }, &lifetime)
        })?;
        report.call("normal_learner_owned_metadata", || {
            target.start_metadata(&lifetime)
        })?;
        report.call("normal_actual_learner_catchup", || {
            sources[0].apply(HostAction::CatchUpLearner { node: node(4) }, &lifetime)
        })?;
        let promotion = report.call("normal_actual_metadata_maintenance", || {
            sources[0].maintain_metadata(&lifetime)
        })?;
        report.event(serde_json::json!({"stage":"actual_maintenance_stage","value":promotion.last_metadata_maintenance}));
        report.call("normal_registered_control", || {
            target.enable_metadata_control(&lifetime)
        })?;
        report.call("normal_target_original_supervisor", || {
            target.start_automatic_metadata(&lifetime)
        })?;
        report.call("normal_sealed_fresh_receiver", || {
            target.open_replica_receiver(&lifetime)
        })?;
        for host in &mut sources {
            report.call("normal_initial_registry_refresh", || {
                host.refresh_initial_data_registry(&lifetime)
            })?;
        }
        let topology = report.call("actual_committed_topology", || {
            sources[0].test_first_topology(&lifetime)
        })?;
        Ok(Self {
            sources,
            target: Some(target),
            lifetime,
            config,
            topology,
            root,
        })
    }
    fn stop(&mut self, report: &mut Report) {
        if let Some(mut target) = self.target.take() {
            let _ = report.call("actual_target_stop", || target.stop(&self.lifetime));
        }
        for source in &mut self.sources {
            let _ = report.call("actual_source_stop", || {
                source.apply(HostAction::Stop, &self.lifetime)
            });
        }
        self.lifetime.cancel();
    }
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
fn run(case: Case) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
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
    let evidence = serde_json::json!({"events":report.events,"omitted":report.omitted,
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
    let mut target = five.target.take().unwrap();
    let fault = match case {
        Case::LostEnroll => Some(Fault::LoseSuccessfulReply(Op::Enroll)),
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
                    match command {OwnerCommand::Snapshot(send)=> {
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
            if matches!(
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
            for (name, action) in [
                ("actual_ALL_OLD_constrain", Action::ConstrainAll),
                ("actual_ALL_OLD_close", Action::CloseAll),
            ] {
                drive!(name, action)?;
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
                    before["native_states"][0]["state"]["final_image"]["actual_commit_returned"],
                    true
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
    if case != Case::UnknownFinal {
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
