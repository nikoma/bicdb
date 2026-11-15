//! Actual same-process normal hosts/TLS and original owner release component.
//! No selected-range Serving, APP/Strong, controller or cold completion claim.
use super::*;
use std::collections::BTreeSet;

pub(super) fn is_case(case: Case) -> bool {
    matches!(
        case,
        Case::Release | Case::ReleaseMissingOpen | Case::ReleaseCancelledAfterDrop
    )
}
enum Control {
    Retire(crate::RangeId, Uuid, mpsc::SyncSender<serde_json::Value>),
    Inspect(
        crate::RangeId,
        Option<u8>,
        mpsc::SyncSender<serde_json::Value>,
    ),
}
fn decode(actual: serde_json::Value) -> Result<serde_json::Value> {
    if actual["ok"] != true {
        return Err(invalid(&format!(
            "actual release owner control refused: {actual}"
        )));
    }
    Ok(actual["data"].clone())
}
fn source_control(
    sender: &mpsc::Sender<Control>,
    range: crate::RangeId,
    phase: Uuid,
    retire: bool,
    arm: Option<u8>,
) -> Result<serde_json::Value> {
    let (send, receive) = mpsc::sync_channel(1);
    sender
        .send(if retire {
            Control::Retire(range, phase, send)
        } else {
            Control::Inspect(range, arm, send)
        })
        .map_err(|_| invalid("actual release source owner ended"))?;
    decode(
        receive
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| invalid("actual release source owner return unavailable"))?,
    )
}
fn target_control(
    sender: &mpsc::Sender<OwnerCommand>,
    range: crate::RangeId,
    phase: Uuid,
    retire: bool,
    arm: Option<u8>,
) -> Result<serde_json::Value> {
    let (send, receive) = mpsc::sync_channel(1);
    sender
        .send(if retire {
            OwnerCommand::RetireReferences(range, phase, send)
        } else {
            OwnerCommand::ReleaseControl(range, arm, send)
        })
        .map_err(|_| invalid("actual release target owner ended"))?;
    decode(
        receive
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| invalid("actual release target owner return unavailable"))?,
    )
}
fn same_native(before: &serde_json::Value, after: &serde_json::Value) {
    for field in [
        "native_next_xid",
        "native_next_lsn",
        "native_durable_lsn",
        "current",
        "archive",
        "bridge_current",
        "bridge_archive",
    ] {
        assert!(
            !before["native_storage"][field].is_null(),
            "actual native forensic field {field} required"
        );
        assert_eq!(
            before["native_storage"][field], after["native_storage"][field],
            "release cannot replay native WAL: {field}"
        );
    }
}
fn same_point(before: &serde_json::Value, after: &serde_json::Value) {
    for field in [
        "decision_sha256",
        "native_decision_record_sha256",
        "remote_provenance_sha256",
        "catalogue_decision_sha256",
        "native_decision_xid",
        "native_database_open",
        "store_open",
        "original_frozen_owner",
        "native_bridge_sha256",
        "catalogue_activation_sha256",
    ] {
        assert_eq!(before[field], after[field], "same actual owner {field}");
    }
    assert_eq!(
        before["preparation"]["preparation_sha256"],
        after["preparation"]["preparation_sha256"]
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    five: &mut Five,
    metadata: usize,
    relocation: crate::distribution::RelocationId,
    phase: Uuid,
    range: crate::RangeId,
    members: &[ClusterNodeId],
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    case: Case,
) -> Result<()> {
    let lifetime = five.lifetime.child();
    let (before, rest) = five.sources.split_at_mut(metadata);
    let (coordinator, after) = rest
        .split_first_mut()
        .ok_or_else(|| invalid("actual release metadata owner absent"))?;
    let stop = Arc::new(AtomicBool::new(false));
    struct Stop(Arc<AtomicBool>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    std::thread::scope(|scope| {
        let _stop = Stop(stop.clone());
        let mut controls = BTreeMap::new();
        let mut workers = Vec::new();
        for (ordinal, host) in before.iter_mut().enumerate().chain(
            after
                .iter_mut()
                .enumerate()
                .map(|(i, h)| (metadata + 1 + i, h)),
        ) {
            let (send, receive) = mpsc::channel::<Control>();
            controls.insert(node(ordinal), send);
            let stop = stop.clone();
            let parent = lifetime.child();
            workers.push(scope.spawn(move || {
                let mut errors=Vec::new();let mut omitted=0u64;
                while !stop.load(Ordering::Acquire) && parent.check().is_ok() {
                    while let Ok(control)=receive.try_recv() {
                        let (send,result)=match control {
                            Control::Retire(range,phase,send)=> {
                                let request=parent.child_with_deadline(Instant::now()+Duration::from_secs(4));
                                let result=host.retire_finite_remote_open_references(range,phase,&request).map(|()|serde_json::json!({"actual_finite_reference_retirement_returned":true}));
                                (send,result)
                            },
                            Control::Inspect(range,mode,send)=>(send,host.test_remote_release_control(range,mode)),
                        };
                        let _=send.try_send(serde_json::json!({"ok":result.is_ok(),"data":result.as_ref().ok(),
                            "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                    }
                    if let Err(error)=host.poll_finite_remote_serving(&parent) {
                        if errors.len()<64 {errors.push(crate::distribution_native_host::remote_first::error_data(&error));}
                        else {omitted=omitted.saturating_add(1);}
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                (errors,omitted)
            }));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            assert!(
                members.contains(&node(4)),
                "genuine retained final configuration must include Registered target"
            );
            for member in members {
                report.data_call("actual_ALL_preparation_before_release", || {
                    coordinator.drive_finite_remote_preparation(
                        relocation, phase, member, false, &lifetime,
                    )
                })?;
            }
            for member in members {
                let bridge = report.data_call("actual_ALL_native_bridge_before_release", || {
                    coordinator.drive_finite_remote_bridge(relocation, phase, member, &lifetime)
                })?;
                assert!(bridge["point"]["native_bridge_sha256"].is_string());
            }
            let mut points = BTreeMap::new();
            let mut original_members = None;
            for member in members {
                if case == Case::ReleaseMissingOpen && *member == node(4) {
                    continue;
                }
                let opened =
                    report.data_call("actual_original_recipient_Open_before_release", || {
                        coordinator
                            .drive_finite_remote_open(relocation, phase, member, false, &lifetime)
                    })?;
                assert!(opened["point"]["catalogue_activation_sha256"].is_string());
                assert_eq!(opened["point"]["actual_dropped_open"], false);
                assert_eq!(opened["point"]["actual_writer_installed"], false);
                original_members =
                    Some(opened["original_request"]["manifest"]["old_members"].clone());
                points.insert(member.clone(), opened["point"].clone());
            }
            let before = report
                .data_call("actual_target_native_and_owner_before_release", || {
                    target_control(target, range, phase, false, None)
                })?;
            assert!(before["actual_successor"].is_null());
            if case == Case::ReleaseMissingOpen {
                let refused =
                    report.data_call("actual_Release_with_real_target_Open_absent", || {
                        coordinator.drive_finite_remote_release(
                            relocation,
                            phase,
                            &node(4),
                            false,
                            &lifetime,
                        )
                    });
                let after = report
                    .data_call("actual_missing_Open_owner_and_native_after_refusal", || {
                        target_control(target, range, phase, false, None)
                    });
                report.event(serde_json::json!({"actual_release_result":refused.as_ref().ok(),"actual_release_error":refused.as_ref().err().map(ToString::to_string),
                    "actual_after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string),"selected_Open_was_not_called":true}));
                let error = refused.expect_err("missing actual Open must refuse release");
                assert!(
                    error
                        .to_string()
                        .contains("actual remote Open catalogue activation absent"),
                    "actual protected Open absence must be the refusal: {error}"
                );
                let after = after?;
                assert!(
                    after["actual_successor"].is_null(),
                    "no successor before actual ALL Open proof"
                );
                assert!(
                    after["queue"]["unknown"].is_null(),
                    "pre-effect refusal cannot claim drop uncertainty"
                );
                same_native(&before, &after);
                return Ok(());
            }
            if case == Case::ReleaseCancelledAfterDrop {
                report.data_call(
                    "arm_actual_original_Registered_drop_before_arm_server_cancel",
                    || target_control(target, range, phase, false, Some(1)),
                )?;
            }
            // The old-members JSON selects destinations only. EACH production
            // actor independently requires its own retained participant and
            // a new authenticated ALL Open proof + attached native MStore.
            let old = original_members
                .ok_or_else(|| invalid("actual original request manifest absent"))?;
            let old = old
                .as_object()
                .ok_or_else(|| invalid("actual original OLD members not a map"))?;
            let mut union = members.iter().cloned().collect::<BTreeSet<_>>();
            for id in old.keys() {
                union.insert(ClusterNodeId::new(id.clone())?);
            }
            for member in &union {
                let actual = if *member == node(4) {
                    target_control(target, range, phase, true, None)
                } else if *member == node(metadata) {
                    let request =
                        lifetime.child_with_deadline(Instant::now() + Duration::from_secs(4));
                    coordinator.retire_finite_remote_open_references(range,phase,&request).map(|()|serde_json::json!({"actual_finite_reference_retirement_returned":true}))
                } else {
                    source_control(
                        controls
                            .get(member)
                            .ok_or_else(|| invalid("actual source actor control absent"))?,
                        range,
                        phase,
                        true,
                        None,
                    )
                };
                report.data_call("actual_original_host_finite_reference_retirement", || {
                    actual
                })?;
            }
            // Already retired but not yet released: direct protected Open
            // must use the retained successor, not reconstruct drained slots.
            for member in members {
                let observed = report.data_call(
                    "actual_Open_after_one_way_original_reference_retirement",
                    || coordinator.inspect_finite_remote_open(relocation, phase, member, &lifetime),
                )?;
                same_point(&points[member], &observed["point"]);
                assert_eq!(observed["point"]["actual_dropped_open"], false);
            }
            if case == Case::ReleaseCancelledAfterDrop {
                let first =
                    report.data_call("actual_Release_original_server_cancel_after_drop", || {
                        coordinator.drive_finite_remote_release(
                            relocation,
                            phase,
                            &node(4),
                            false,
                            &lifetime,
                        )
                    });
                let retained = report
                    .data_call("actual_original_drop_unknown_before_assert", || {
                        target_control(target, range, phase, false, None)
                    });
                report.event(serde_json::json!({"original_return":first.as_ref().ok(),"original_error":first.as_ref().err().map(ToString::to_string),
                    "actual_retained":retained.as_ref().ok(),"diagnostic_error":retained.as_ref().err().map(ToString::to_string)}));
                assert!(first.is_err());
                let retained = retained?;
                let successor = &retained["actual_successor"];
                assert_eq!(
                    successor["actual_owner_DATA"]["kind"],
                    "actual_Dropped_with_original_activation"
                );
                assert_eq!(
                    successor["memory"]["actual_lease_free_history_retained"],
                    true
                );
                let cut = &successor["actual_drop_cut"];
                assert_eq!(cut["actual_registered_after_drop_before_arm_fired"], 1);
                assert_eq!(cut["actual_original_server_cancelled"], true);
                assert!(cut["original_server_remaining_us_before_cancel"]
                    .as_u64()
                    .is_some_and(|n| n > 0));
                for field in [
                    "actual_request_remaining_us",
                    "actual_Role_remaining_us",
                    "actual_OWN_remaining_us",
                ] {
                    assert!(
                        cut["windows_before_cancel"][field]
                            .as_u64()
                            .is_some_and(|n| n > 0),
                        "real original window {field}"
                    );
                }
                assert_eq!(cut["windows_before_cancel"]["actual_process_live"], true);
                assert_eq!(
                    cut["windows_before_cancel"]["same_native_MStore_memory_live"],
                    true
                );
                assert!(!retained["queue"]["unknown"].is_null());
                same_native(&before, &retained);
                let read =
                    report.data_call("actual_ObserveReleased_refuses_dropped_but_unarmed", || {
                        coordinator.inspect_finite_remote_released(
                            relocation,
                            phase,
                            &node(4),
                            &lifetime,
                        )
                    });
                // Preserve BOTH the actual returned transport error and the
                // same owner's state after this readonly denial, before any
                // assertion/reconciliation. A busy/expired/transport refusal
                // is an unreached cut, never the expected negative result.
                let after_denial = report.data_call(
                    "actual_drop_owner_and_native_after_ObserveReleased_denial",
                    || target_control(target, range, phase, false, None),
                );
                report.event(serde_json::json!({
                    "actual_ObserveReleased_error":read.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                    "actual_after_denial":after_denial.as_ref().ok(),
                    "actual_after_denial_error":after_denial.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                    "separate_memory_storage_snapshots_non_atomic":true,
                }));
                let error = read.expect_err("Dropped alone is not armed released Open");
                // Exact real mapping: released_generation emits Cluster;
                // server response stores its Display as remote.message with
                // cluster_data_operation_failed, then the pinned RPC client
                // maps that envelope back into Cluster with this full body.
                let expected = format!(
                    "cluster node {} rejected RPC cluster_data_operation_failed: cluster error: joint observed owner has not released",
                    node(4)
                );
                assert!(
                    matches!(&error, BicDbError::Cluster(body) if body == &expected),
                    "required protected unarmed-continuity denial was not reached: {error:?}"
                );
                let after_denial = after_denial?;
                same_native(&retained, &after_denial);
                // Full originals were already logged above. Compare cloned
                // DATA without ONLY the time-dependent expiry observation;
                // the SAME stored absolute monotonic deadline remains in the
                // exact equality alongside request/op/sequence/error and
                // admitted/processing/returned. No deadline is renewed.
                let mut old_queue = retained["queue"].clone();
                let mut new_queue = after_denial["queue"].clone();
                let expired_before = old_queue["unknown"]
                    .as_object_mut()
                    .expect("original actual unknown required")
                    .remove("original_deadline_expired")
                    .expect("expiry observation required")
                    .as_bool()
                    .expect("expiry observation must be boolean");
                let expired_after = new_queue["unknown"]
                    .as_object_mut()
                    .expect("same actual unknown required after readonly denial")
                    .remove("original_deadline_expired")
                    .expect("expiry observation required")
                    .as_bool()
                    .expect("expiry observation must be boolean");
                assert!(
                    !expired_before || expired_after,
                    "same original deadline cannot go from expired to unexpired"
                );
                assert!(
                    old_queue["unknown"]["original_deadline_monotonic_debug"]
                        .as_str()
                        .is_some_and(|deadline| !deadline.is_empty()),
                    "actual stored absolute monotonic deadline identity required"
                );
                assert_eq!(
                    old_queue, new_queue,
                    "readonly denial cannot clear or replace original release uncertainty/deadline"
                );
                assert_eq!(retained["actual_successor"], after_denial["actual_successor"],
                    "readonly denial must retain the same dropped owner/history and original API errors");
                let resolved =
                    report.data_call("actual_same_operation_typed_ReconcileRelease", || {
                        coordinator.drive_finite_remote_release(
                            relocation,
                            phase,
                            &node(4),
                            true,
                            &lifetime,
                        )
                    });
                let after = report.data_call("actual_owner_after_fresh_release_resolution", || {
                    target_control(target, range, phase, false, None)
                });
                report.event(serde_json::json!({"fresh_return":resolved.as_ref().ok(),"fresh_error":resolved.as_ref().err().map(ToString::to_string),
                    "actual_after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string)}));
                let resolved = resolved?;
                let after = after?;
                assert_eq!(resolved["point"]["actual_dropped_open"], true);
                assert!(after["queue"]["unknown"].is_null());
                same_native(&before, &after);
                assert_eq!(
                    successor["actual_release_API_returns"]["first_original_error"],
                    after["actual_successor"]["actual_release_API_returns"]["first_original_error"]
                );
                assert!(!after["actual_successor"]["actual_release_API_returns"]
                    ["first_original_error"]
                    .is_null());
            }
            for member in members {
                if case == Case::ReleaseCancelledAfterDrop && *member == node(4) {
                    continue;
                }
                let released =
                    report.data_call("actual_authenticated_original_owner_Release", || {
                        coordinator.drive_finite_remote_release(
                            relocation, phase, member, false, &lifetime,
                        )
                    })?;
                same_point(&points[member], &released["point"]);
                assert_eq!(released["point"]["actual_dropped_open"], true);
                assert_eq!(released["point"]["actual_writer_installed"], false);
            }
            for member in members {
                let released = report.data_call("actual_ALL_protected_ObserveReleased", || {
                    coordinator.inspect_finite_remote_released(relocation, phase, member, &lifetime)
                })?;
                same_point(&points[member], &released["point"]);
                assert_eq!(released["point"]["actual_dropped_open"], true);
                assert_eq!(released["point"]["actual_writer_installed"], false);
            }
            let after = report.data_call("actual_target_native_after_ALL_release", || {
                target_control(target, range, phase, false, None)
            })?;
            same_native(&before, &after);
            assert_eq!(
                after["actual_successor"]["memory"]["actual_lease_free_history_retained"],
                true
            );
            assert_eq!(
                after["actual_successor"]["memory"]["actual_custody_retained"],
                false
            );
            report.event(serde_json::json!({"actual_ALL_original_owner_release_component":true,"Serving":false,
                "selected_APP_Strong_NOT_PROVEN":true,"automatic_controller_cold_cleanup_NOT_IMPLEMENTED":true}));
            Ok(())
        }));
        stop.store(true, Ordering::Release);
        for worker in workers {
            let (errors, omitted) = match worker.join() {
                Ok(v) => v,
                Err(p) => std::panic::resume_unwind(p),
            };
            report.event(
                serde_json::json!({"actual_release_source_poll_errors":errors,"omitted":omitted}),
            );
        }
        // RAII also releases on parent panic. Kernel/network calls and scoped
        // joins retain their original bounds; teardown is not preemptible.
        match result {
            Ok(actual) => actual,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}
