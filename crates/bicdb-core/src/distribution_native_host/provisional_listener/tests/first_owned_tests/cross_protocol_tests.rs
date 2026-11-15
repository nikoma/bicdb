//! Actual normal-host FIRST/step arbitration over real pinned TCP. These are
//! in-process component controls, not controller/cold/Serving/physical proof.
//! Enrollment retains the real FIRST owner/N preflight BEFORE image Transfer
//! mints N->A. A discovery driver position is not a source capture or an A.
use super::*;

#[derive(Clone, Copy, Debug)]
enum Order {
    FirstThenStep,
    StepThenFirst,
}
enum Probe {
    Snapshot(crate::RangeId, mpsc::SyncSender<serde_json::Value>),
}
fn target_snapshot(
    sender: &mpsc::Sender<Probe>,
    range: crate::RangeId,
) -> Result<serde_json::Value> {
    let (send, receive) = mpsc::sync_channel(1);
    sender
        .send(Probe::Snapshot(range, send))
        .map_err(|_| invalid("cross-protocol original owner thread ended"))?;
    let data = receive
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("cross-protocol actual target diagnostic unavailable"))?;
    if data["ok"] != true {
        return Err(invalid(&format!(
            "actual cross-protocol target diagnostic failed: {data}"
        )));
    }
    Ok(data["data"].clone())
}
fn require_next(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(invalid(
            "cross-protocol120s component bound before next actual call; prerequisite incomplete",
        ));
    }
    Ok(())
}

#[test]
fn native_first_actual_enrollment_blocks_authenticated_other_step_before_effects() {
    run_order(Order::FirstThenStep);
}
#[test]
fn native_first_actual_retained_discovery_step_blocks_new_first_enrollment_before_effects() {
    run_order(Order::StepThenFirst);
}
fn run_order(order: Order) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
        stderr_write_errors: 0,
    };
    let mut five = Five::start(&mut report).unwrap_or_else(|error| {
        report.event(
            serde_json::json!({"cross_protocol_setup_error":error.to_string(),
            "requested_cut_reached":false}),
        );
        panic!("actual cross-protocol normal-host prerequisite: {error}")
    });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise_order(&mut five, &mut report, order)
    }));
    five.stop(&mut report);
    let evidence = serde_json::json!({"order":format!("{order:?}"),"events":report.events,
        "omitted":report.omitted,"stderr_write_errors":report.stderr_write_errors,
        "global_capture_complete":false,"same_process_normal_hosts":true,
        "physical_or_Serving_qualification":false});
    let _ = std::fs::write(
        five.root
            .path()
            .join("first-cross-protocol-actual-report.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    );
    match result {
        Ok(actual) => actual.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
fn exercise_order(five: &mut Five, report: &mut Report, order: Order) -> Result<()> {
    let metadata_leader = five.sources[0]
        .status(&five.lifetime)?
        .metadata
        .and_then(|s| s.leader_id)
        .ok_or_else(|| invalid("actual cross-protocol metadata leader absent"))?;
    let selected = five
        .topology
        .ranges
        .values()
        .find(|r| r.leader != metadata_leader)
        .ok_or_else(|| invalid("actual cross-protocol selected range absent"))?
        .clone();
    let untouched = five
        .topology
        .ranges
        .values()
        .find(|r| r.id != selected.id)
        .ok_or_else(|| invalid("actual cross-protocol untouched range absent"))?
        .clone();
    let donor = selected
        .replicas
        .iter()
        .find(|r| {
            r.role == RangeReplicaRole::Voter
                && r.node_id != selected.leader
                && r.node_id != metadata_leader
        })
        .ok_or_else(|| invalid("actual cross-protocol nonleader donor absent"))?
        .node_id
        .clone();
    let coordinator = (0..4)
        .find(|i| node(*i) == selected.leader)
        .ok_or_else(|| invalid("actual cross-protocol range leader host absent"))?;
    let donor_index = (0..4)
        .find(|i| node(*i) == donor)
        .ok_or_else(|| invalid("actual cross-protocol donor host absent"))?;
    let client = crate::ClusterEventClient::new(&five.config, five.topology.clone())?
        .with_request_timeout(Duration::from_secs(2))?;
    sdk(
        report,
        &client,
        &key(&five.topology, selected.id, "cross-before-selected")?,
        serde_json::json!({"actual":"selected-before-protocol-arbitration","present_null":null}),
    )?;
    sdk(
        report,
        &client,
        &key(&five.topology, untouched.id, "cross-before-untouched")?,
        serde_json::json!({"actual":"untouched-before-protocol-arbitration"}),
    )?;
    let pending = report.call("cross_actual_allocation", || {
        five.sources[0].test_first_commit_allocation(selected.id, &node(4), &five.lifetime)
    })?;
    for source in &mut five.sources {
        report.call("cross_actual_registry_refresh", || {
            source.refresh_initial_data_registry(&five.lifetime)
        })?;
    }
    for source in &five.sources {
        report.call("cross_observe_actual_same_worker_allocation_epoch", || {
            source.observe_initial_data_allocation(selected.id, &five.lifetime)
        })?;
    }
    let relocation = pending
        .relocations
        .values()
        .find(|r| r.range_id == selected.id)
        .ok_or_else(|| invalid("actual cross-protocol committed allocation absent"))?;
    report.call("cross_original_range_leader_factory", || {
        five.sources[coordinator].begin_finite_remote_first(
            relocation.id,
            donor.clone(),
            &five.lifetime,
        )
    })?;
    let mut target = five
        .target
        .take()
        .ok_or_else(|| invalid("actual normal target absent"))?;
    let (send, receive) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let active = stop.clone();
    let parent = five.lifetime.child();
    let result = std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            let mut errors = Vec::new();
            let mut omitted = 0u64;
            while !active.load(Ordering::Acquire) && parent.check().is_ok() {
                while let Ok(Probe::Snapshot(range, reply)) = receive.try_recv() {
                    let actual = target.test_first_cross_protocol_target_snapshot(range);
                    let _ = reply.try_send(serde_json::json!({"ok":actual.is_ok(),
                        "data":actual.as_ref().ok(),"error":actual.as_ref().err()
                            .map(crate::distribution_native_host::remote_first::error_data)}));
                }
                if let Err(error) = target.poll_native_first_receiver(&parent) {
                    if errors.len() < 64 {
                        errors.push(crate::distribution_native_host::remote_first::error_data(
                            &error,
                        ));
                    } else {
                        omitted += 1;
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            (target, errors, omitted)
        });
        // Catch assertion/primary errors BEFORE joining, then stop the actual
        // poller and restore its host to Five for the existing cleanup path.
        let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let deadline = Instant::now() + Duration::from_secs(120);
            report.event(serde_json::json!({"cross_protocol_original_windows":true,
                "component_next_call_bound_seconds":120,"per_step_seconds":5,
                "network_io_ms":1500,"original_sdk_timeout_ms":2000,
                "actual_range_leader":selected.leader,"actual_donor":donor,
                "actual_target":node(4),"order":format!("{order:?}")}));
            report.data_call("cross_actual_target_fence", || {
                five.sources[coordinator].drive_finite_remote_first(
                    selected.id,
                    Action::FenceTarget,
                    &five.lifetime,
                )
            })?;
            require_next(deadline)?;
            if matches!(order, Order::FirstThenStep) {
                report.data_call("cross_actual_FIRST_enroll", || {
                    five.sources[coordinator].drive_finite_remote_first(
                        selected.id,
                        Action::EnrollTarget,
                        &five.lifetime,
                    )
                })?;
                let before = report
                    .data_call("cross_actual_target_before_competing_step", || {
                        target_snapshot(&send, selected.id)
                    })?;
                let source_before =
                    report.data_call("cross_actual_source_before_competing_step", || {
                        five.sources[donor_index]
                            .test_first_cross_protocol_source_snapshot(selected.id)
                    })?;
                let coordinator_before =
                    five.sources[coordinator].test_first_coordinator_snapshot(selected.id)?;
                let first_attempt: Uuid =
                    serde_json::from_value(before["first"]["attempts"][0]["attempt"].clone())?;
                let competing_attempt = Uuid::now_v7();
                if competing_attempt == first_attempt {
                    return Err(invalid("actual correlation UUID collision"));
                }
                require_next(deadline)?;
                let rpc = report.data_call("cross_actual_authenticated_other_step", || {
                    five.sources[coordinator].test_first_competing_handoff_step(
                        selected.id,
                        competing_attempt,
                        &five.lifetime,
                    )
                });
                let after = report.data_call("cross_actual_target_after_competing_step", || {
                    target_snapshot(&send, selected.id)
                });
                let source_after =
                    report.data_call("cross_actual_source_after_competing_step", || {
                        five.sources[donor_index]
                            .test_first_cross_protocol_source_snapshot(selected.id)
                    });
                let coordinator_after =
                    five.sources[coordinator].test_first_coordinator_snapshot(selected.id);
                report.event(serde_json::json!({"cross_actual_before_assertions":true,
                    "target_before":before,"target_after":after.as_ref().ok(),
                    "source_before":source_before,"source_after":source_after.as_ref().ok(),
                    "coordinator_before":coordinator_before,"coordinator_after":coordinator_after.as_ref().ok(),
                    "rpc_facade":rpc.as_ref().ok(),"rpc_prerequisite_error":rpc.as_ref().err()
                        .map(crate::distribution_native_host::remote_first::error_data)}));
                let rpc = rpc?;
                let after = after?;
                let source_after = source_after?;
                let coordinator_after = coordinator_after?;
                assert_eq!(rpc["actual_rpc_returned_ok"], false);
                assert!(rpc["actual_rpc_error"]["display_prefix"].as_str().is_some_and(|s|
                    s.contains("joint handoff step cannot adopt FIRST-owned install custody")),
                    "requested claim cut not reached; actual exchange must remain prerequisite failure: {rpc}");
                assert!(before["step"].is_null());
                assert!(after["step"].is_null());
                assert_eq!(
                    before, after,
                    "competing step changed FIRST/native/install state"
                );
                assert_eq!(
                    source_before, source_after,
                    "claim refusal changed donor discovery/native lineage"
                );
                assert_eq!(
                    coordinator_before, coordinator_after,
                    "test sender changed original FIRST progress"
                );
                assert_eq!(before["first"]["attempts"].as_array().unwrap().len(), 1);
                assert_eq!(
                    before["first"]["attempts"][0]["original_process_live_at_snapshot"],
                    true
                );
                require_next(deadline)?;
                report.data_call("cross_actual_SAME_FIRST_owner_reobservation", || {
                    five.sources[coordinator].drive_finite_remote_first(
                        selected.id,
                        Action::EnrollTarget,
                        &five.lifetime,
                    )
                })?;
                let resumed = report.data_call("cross_actual_SAME_FIRST_owner_retained", || {
                    target_snapshot(&send, selected.id)
                })?;
                assert_eq!(resumed["first"]["attempts"].as_array().unwrap().len(), 1);
                assert_eq!(
                    resumed["first"]["attempts"][0]["attempt"],
                    before["first"]["attempts"][0]["attempt"]
                );
                assert_eq!(
                    resumed["first"]["attempts"][0]["operation"],
                    before["first"]["attempts"][0]["operation"]
                );
                assert_eq!(resumed["native"], before["native"]);
                assert!(resumed["step"].is_null());
            } else {
                let before = report
                    .data_call("cross_actual_target_before_step_position", || {
                        target_snapshot(&send, selected.id)
                    })?;
                let source_before =
                    report.data_call("cross_actual_source_before_step_position", || {
                        five.sources[donor_index]
                            .test_first_cross_protocol_source_snapshot(selected.id)
                    })?;
                let competing_attempt = Uuid::now_v7();
                let rpc = report.data_call("cross_actual_discovery_only_step", || {
                    five.sources[coordinator].test_first_competing_handoff_step(
                        selected.id,
                        competing_attempt,
                        &five.lifetime,
                    )
                });
                let retained = report.data_call("cross_actual_retained_step_position", || {
                    target_snapshot(&send, selected.id)
                });
                let source_retained =
                    report.data_call("cross_actual_source_after_discovery_attempt", || {
                        five.sources[donor_index]
                            .test_first_cross_protocol_source_snapshot(selected.id)
                    });
                report.event(
                    serde_json::json!({"reverse_actual_prerequisites_before_assertions":true,
                    "target_before":before,"target_retained":retained.as_ref().ok(),
                    "source_before":source_before,"source_retained":source_retained.as_ref().ok(),
                    "rpc_facade":rpc.as_ref().ok(),"rpc_prerequisite_error":rpc.as_ref().err()
                        .map(crate::distribution_native_host::remote_first::error_data),
                    "source_effect_not_inferred_from_error":true}),
                );
                let rpc = rpc?;
                let retained = retained?;
                let source_retained = source_retained?;
                // No OLD Close was issued. The REAL step must have retained
                // its discovery-only position, with no target native install.
                assert_eq!(rpc["actual_rpc_returned_ok"], false);
                assert_eq!(retained["step"]["stage"],"Discovering",
                    "real discovery-only step not reached; prerequisite failure is NOT a claim pass");
                assert_eq!(
                    retained["step"]["target_attempt"],
                    serde_json::to_value(competing_attempt)?
                );
                assert_eq!(retained["step"]["discovery_started"], false);
                assert!(retained["step"]["manifest_sha256"].is_null());
                assert_eq!(retained["native"], before["native"]);
                assert_eq!(source_before["native"], source_retained["native"]);
                assert!(retained["first"]["attempts"].as_array().unwrap().is_empty());
                require_next(deadline)?;
                let enroll = report.data_call("cross_actual_FIRST_after_retained_step", || {
                    five.sources[coordinator].drive_finite_remote_first(
                        selected.id,
                        Action::EnrollTarget,
                        &five.lifetime,
                    )
                });
                let after = report.data_call("cross_actual_target_after_refused_FIRST", || {
                    target_snapshot(&send, selected.id)
                });
                let source_after =
                    report.data_call("cross_actual_source_after_refused_FIRST", || {
                        five.sources[donor_index]
                            .test_first_cross_protocol_source_snapshot(selected.id)
                    });
                report.event(serde_json::json!({"reverse_actual_refusal_before_assertions":true,
                    "retained":retained,"after":after.as_ref().ok(),"source_retained":source_retained,
                    "source_after":source_after.as_ref().ok(),"actual_enroll_ok":enroll.is_ok(),
                    "actual_enroll_error":enroll.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                assert!(
                    enroll.as_ref().err().is_some_and(|e| e
                        .to_string()
                        .contains("FIRST cannot adopt a receiver-driven target install step")),
                    "FIRST must reach exact retained-step refusal; actual={enroll:?}"
                );
                let after = after?;
                let source_after = source_after?;
                assert_eq!(
                    retained, after,
                    "refused FIRST changed original step/native/install/queue state"
                );
                assert_eq!(
                    source_retained, source_after,
                    "refused FIRST changed source custody/native lineage"
                );
                assert!(after["first"]["attempts"].as_array().unwrap().is_empty());
            }
            Ok(())
        }));
        stop.store(true, Ordering::Release);
        let (target, errors, omitted) = match worker.join() {
            Ok(actual) => actual,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        five.target = Some(target);
        report.event(
            serde_json::json!({"cross_actual_owner_thread_errors":errors,
            "owner_errors_omitted":omitted,"hard_kernel_IO_teardown_bound":false}),
        );
        match actual {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    });
    result?;
    sdk(
        report,
        &client,
        &key(&five.topology, selected.id, "cross-after-selected")?,
        serde_json::json!({"actual":"selected-still-readable-writable-after-refusal","present_null":null}),
    )?;
    sdk(
        report,
        &client,
        &key(&five.topology, untouched.id, "cross-after-untouched")?,
        serde_json::json!({"actual":"untouched-still-readable-writable-after-refusal"}),
    )?;
    Ok(())
}
