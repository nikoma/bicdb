//! Real retained normal hosts/TLS, same original budgets. These source
//! controls stop at Open; release/native Serving/physical qualification are
//! deliberately asserted absent until their actual producers are connected.
use super::*;
pub(super) fn is_case(case: Case) -> bool {
    matches!(
        case,
        Case::Open
            | Case::OpenR2Progress
            | Case::OpenLaterIoFailed
            | Case::OpenCancelledAfterIo
            | Case::OpenIoUnknown
    )
}
struct OpenControl {
    range: crate::RangeId,
    mode: Option<u8>,
    reply: mpsc::SyncSender<serde_json::Value>,
}
fn command(
    sender: &mpsc::Sender<OpenControl>,
    range: crate::RangeId,
    mode: Option<u8>,
) -> Result<serde_json::Value> {
    let (reply, receive) = mpsc::sync_channel(1);
    sender
        .send(OpenControl { range, mode, reply })
        .map_err(|_| invalid("actual source Open control ended"))?;
    let actual = receive
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("actual source Open diagnostic unavailable"))?;
    decode_control(actual)
}
fn target_command(
    sender: &mpsc::Sender<OwnerCommand>,
    range: crate::RangeId,
    mode: Option<u8>,
) -> Result<serde_json::Value> {
    let (send, receive) = mpsc::sync_channel(1);
    sender
        .send(OwnerCommand::OpenControl(range, mode, send))
        .map_err(|_| invalid("actual target Open control ended"))?;
    decode_control(
        receive
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| invalid("actual target Open diagnostic unavailable"))?,
    )
}
fn decode_control(actual: serde_json::Value) -> Result<serde_json::Value> {
    if actual["ok"] != true {
        return Err(invalid(&format!("actual Open diagnostic failed: {actual}")));
    }
    Ok(actual["data"].clone())
}
fn selected_same(before: &serde_json::Value, after: &serde_json::Value) {
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
        assert_eq!(
            before[field], after[field],
            "same actual selected protected {field}"
        );
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
    let client = crate::ClusterEventClient::new(&five.config, five.topology.clone())?
        .with_request_timeout(Duration::from_secs(2))?;
    let other = five
        .topology
        .ranges
        .values()
        .find(|r| r.id != range)
        .ok_or_else(|| invalid("actual untouched range absent"))?
        .clone();
    let other_key = key(&five.topology, other.id, "remote-open-real-unrelated-head")?;
    let (before, rest) = five.sources.split_at_mut(metadata);
    let (coordinator, after) = rest
        .split_first_mut()
        .ok_or_else(|| invalid("actual Open coordinator absent"))?;
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
            let (send, receive) = mpsc::channel::<OpenControl>();
            controls.insert(node(ordinal), send);
            let stop = stop.clone();
            let parent = lifetime.child();
            workers.push(scope.spawn(move || {
                let mut errors=Vec::new();let mut omitted=0u64;
                while !stop.load(Ordering::Acquire) && parent.check().is_ok() {
                    while let Ok(control)=receive.try_recv() {
                        let result=host.test_remote_open_control(control.range,control.mode);
                        let _=control.reply.try_send(serde_json::json!({"ok":result.is_ok(),"data":result.as_ref().ok(),
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
            for member in members {
                report.data_call("actual_ALL_new_native_preparation_for_Open", || {
                    coordinator.drive_finite_remote_preparation(
                        relocation, phase, member, false, &lifetime,
                    )
                })?;
            }
            for member in members {
                let bridge = report.data_call("actual_ALL_new_native_bridge_for_Open", || {
                    coordinator.drive_finite_remote_bridge(relocation, phase, member, &lifetime)
                })?;
                assert!(bridge["point"]["native_bridge_sha256"].is_string());
            }
            let retry = matches!(case, Case::OpenCancelledAfterIo | Case::OpenIoUnknown);
            if retry {
                let mode = if case == Case::OpenCancelledAfterIo {
                    1
                } else {
                    2
                };
                report.data_call("arm_actual_original_target_Open_boundary", || {
                    target_command(target, range, Some(mode))
                })?;
                let original = report.data_call("actual_target_Open_original_request", || {
                    coordinator.drive_finite_remote_open(
                        relocation,
                        phase,
                        &node(4),
                        false,
                        &lifetime,
                    )
                });
                let retained = report
                    .data_call("actual_target_Open_original_outcome_before_assert", || {
                        target_command(target, range, None)
                    });
                report.event(serde_json::json!({"original_error":original.as_ref().err().map(ToString::to_string),"retained":retained.as_ref().ok(),
                    "diagnostic_error":retained.as_ref().err().map(ToString::to_string),"source_controls_not_runtime_proof":true}));
                let retained = retained?;
                assert!(original.is_err());
                let cut = &retained["memory_before_storage"]["cut"];
                if case == Case::OpenIoUnknown {
                    assert_eq!(cut["actual_dirsync_faults"], 1);
                    assert_eq!(cut["actual_sink_returned_ok"], false);
                    assert_eq!(retained["actual_io"]["global_uncertain"], true);
                } else {
                    assert_eq!(cut["actual_original_server_cancelled"], true);
                    assert_eq!(cut["before_cancel"]["same_process_live"], true);
                    assert_eq!(cut["same_process_live_after_cancel"], true);
                    assert!(cut["before_cancel"]["original_server_remaining_us"]
                        .as_u64()
                        .is_some_and(|n| n > 0));
                    assert!(cut["before_cancel"]["original_OWN_remaining_us"]
                        .as_u64()
                        .is_some_and(|n| n > 0));
                    assert!(cut["before_cancel"]["original_Role_remaining_us"]
                        .as_u64()
                        .is_some_and(|n| n > 0));
                }
                let resolved =
                    report.data_call("fresh_authenticated_SAME_owned_Open_reconciliation", || {
                        coordinator.drive_finite_remote_open(
                            relocation,
                            phase,
                            &node(4),
                            true,
                            &lifetime,
                        )
                    });
                let after = report
                    .data_call("actual_target_Open_after_distinct_resolution", || {
                        target_command(target, range, None)
                    });
                report.event(serde_json::json!({"fresh_return":resolved.as_ref().ok(),"fresh_error":resolved.as_ref().err().map(ToString::to_string),
                    "after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string)}));
                let resolved = resolved?;
                let after = after?;
                assert!(resolved["point"]["catalogue_activation_sha256"].is_string());
                assert_eq!(
                    after["memory_before_storage"]["queue"]["unknown"],
                    serde_json::Value::Null
                );
                assert_eq!(after["actual_io"]["global_uncertain"], false);
                assert_eq!(
                    retained["memory_before_storage"]["native_preparation"],
                    after["memory_before_storage"]["native_preparation"]
                );
                assert_eq!(
                    retained["memory_before_storage"]["native_bridge"],
                    after["memory_before_storage"]["native_bridge"]
                );
                for field in [
                    "native_next_xid",
                    "native_next_lsn",
                    "current",
                    "archive",
                    "bridge_current",
                    "bridge_archive",
                ] {
                    assert_ne!(
                        retained["native_storage"][field],
                        serde_json::Value::Null,
                        "actual forensic cut must provide {field}"
                    );
                    assert_eq!(
                        retained["native_storage"][field], after["native_storage"][field],
                        "Open recovery must not replay native WAL {field}"
                    );
                }

                assert_eq!(
                    retained["actual_io"]["original"]["actual_io_returns"][0],
                    after["actual_io"]["original"]["actual_io_returns"][0]
                );
                assert_eq!(
                    retained["actual_io"]["original"]["native_bridge_sha256"],
                    after["actual_io"]["original"]["native_bridge_sha256"]
                );
                if case == Case::OpenIoUnknown {
                    assert!(
                        after["actual_io"]["entered"].as_u64().unwrap()
                            > retained["actual_io"]["entered"].as_u64().unwrap()
                    );
                } else {
                    assert_eq!(
                        after["actual_io"]["entered"],
                        retained["actual_io"]["entered"]
                    );
                }
                return Ok(());
            }
            let mut points = BTreeMap::new();
            for member in members {
                let opened = report.data_call("actual_recipient_owned_ALL_bridge_Open", || {
                    coordinator
                        .drive_finite_remote_open(relocation, phase, member, false, &lifetime)
                })?;
                assert!(opened["point"]["catalogue_activation_sha256"].is_string());
                assert_eq!(opened["point"]["actual_dropped_open"], false);
                assert_eq!(opened["point"]["actual_writer_installed"], false);
                points.insert(member.clone(), opened["point"].clone());
            }
            if case == Case::Open {
                return Ok(());
            }
            if case == Case::OpenLaterIoFailed {
                let member = members
                    .iter()
                    .find(|n| **n != node(4) && **n != node(metadata))
                    .ok_or_else(|| invalid("actual opened survivor worker absent"))?;
                let actual = report.data_call(
                    "genuine_Controlled_same_byte_later_directory_sync_error",
                    || command(&controls[member], range, Some(3)),
                )?;
                report.event(serde_json::json!({"actual_same_byte_failure":actual}));
                assert_eq!(actual["actual_dirsync_faults"], 1);
                assert_eq!(actual["IO_returned_ok"], false);
                assert_eq!(actual["after_IO"]["disk_matches_memory"], true);
                assert_eq!(actual["after_IO"]["global_uncertain"], false);
                assert_eq!(actual["after_IO"], actual["after_readonly_projection"]);
                assert!(actual["projection_error"]
                    .as_str()
                    .is_some_and(|e| e.contains("no known successful return")));
                let refused = report.data_call(
                    "fresh_real_TLS_Open_read_refuses_ambiguous_current_IO",
                    || coordinator.inspect_finite_remote_open(relocation, phase, member, &lifetime),
                );
                report.event(serde_json::json!({"actual_read_error":refused.as_ref().err().map(ToString::to_string)}));
                assert!(refused
                    .as_ref()
                    .err()
                    .is_some_and(|e| e.to_string().contains("no known successful return")));
                return Ok(());
            }
            let mut before_io = BTreeMap::new();
            for member in members.iter().filter(|n| **n != node(4)) {
                let actual = if *member == node(metadata) {
                    coordinator.test_remote_open_control(range, None)
                } else {
                    command(&controls[member], range, None)
                }?;
                before_io.insert(member.clone(), actual);
            }
            sdk(
                report,
                &client,
                &other_key,
                serde_json::json!({"actual":"stock-sdk-unrelated-range-after-selected-Open","range":other.id}),
            )?;
            let mut witnessed = false;
            for member in members.iter().filter(|n| **n != node(4)) {
                let read = report.data_call(
                    "actual_fresh_Open_read_after_stock_other_range_ACK_and_Strong",
                    || coordinator.inspect_finite_remote_open(relocation, phase, member, &lifetime),
                );
                let io = if *member == node(metadata) {
                    coordinator.test_remote_open_control(range, None)
                } else {
                    command(&controls[member], range, None)
                };
                report.event(serde_json::json!({"actual_member":member,"actual_read":read.as_ref().ok(),"read_error":read.as_ref().err().map(ToString::to_string),
                    "IO":io.as_ref().ok(),"IO_error":io.as_ref().err().map(ToString::to_string),"before_IO":before_io[member]}));
                let read = read?;
                let io = io?;
                selected_same(&points[member], &read["point"]);
                if points[member]["current_catalogue_sha256"]
                    != read["point"]["current_catalogue_sha256"]
                {
                    assert!(
                        io["actual_io"]["entered"].as_u64().unwrap()
                            > before_io[member]["actual_io"]["entered"].as_u64().unwrap()
                    );
                    assert_eq!(io["actual_io"]["entered"], io["actual_io"]["known_success"]);
                    assert_eq!(
                        io["actual_io"]["original"]["actual_io_returns"],
                        before_io[member]["actual_io"]["original"]["actual_io_returns"]
                    );
                    witnessed = true;
                }
            }
            assert!(witnessed,"actual stock R2 APP must reach a changed opened-survivor whole head; setup refusal is not a skipped pass");
            Ok(())
        }));
        stop.store(true, Ordering::Release);
        for worker in workers {
            let (errors, omitted) = match worker.join() {
                Ok(v) => v,
                Err(p) => std::panic::resume_unwind(p),
            };
            report.event(
                serde_json::json!({"actual_source_owner_poll_errors":errors,"omitted":omitted}),
            );
        }
        match result {
            Ok(actual) => actual,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}
