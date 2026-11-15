//! Actual original normal-host actors and TLS: ALL preparations before the
//! target's real native bridge. No decoded frame creates a local QQ or policy.
use super::*;
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
        .ok_or_else(|| invalid("actual metadata coordinator index absent"))?;
    let stop = Arc::new(AtomicBool::new(false));
    struct Stop(Arc<AtomicBool>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    std::thread::scope(|scope| {
        let _stop_on_panic = Stop(stop.clone());
        let mut workers = Vec::new();
        // Actual retained original host objects move only into these scoped
        // actor threads. No host is synthesized from request/MF selectors.
        for host in before.iter_mut().chain(after.iter_mut()) {
            let stop = stop.clone();
            let parent = lifetime.child();
            workers.push(scope.spawn(move || {
                let mut errors = Vec::new();
                let mut omitted = 0u64;
                while !stop.load(Ordering::Acquire) && parent.check().is_ok() {
                    if let Err(error) = host.poll_finite_remote_serving(&parent) {
                        if errors.len() < 64 {
                            errors.push(crate::distribution_native_host::remote_first::error_data(
                                &error,
                            ));
                        } else {
                            omitted = omitted.saturating_add(1);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                (errors, omitted)
            }));
        }
        let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let omitted_member = if case == Case::BridgeMissingPrepared {
                Some(
                    members
                        .iter()
                        .find(|n| *n != &node(4))
                        .ok_or_else(|| {
                            invalid("actual missing-member control requires real final survivor")
                        })?
                        .clone(),
                )
            } else {
                None
            };
            for node in members {
                if omitted_member.as_ref() == Some(node) {
                    continue;
                }
                let prepared =
                    report.data_call("actual_normal_owner_ALL_new_native_preparation", || {
                        coordinator.drive_finite_remote_preparation(
                            relocation, phase, node, false, &lifetime,
                        )
                    });
                report.event(serde_json::json!({"actual_NEW_node":node,"actual_prepare_return":prepared.as_ref().ok(),
                    "actual_prepare_error":prepared.as_ref().err().map(ToString::to_string),"no_full_Serving_claim":true}));
                let prepared = prepared?;
                assert_eq!(prepared["node"], serde_json::to_value(node).unwrap());
                assert!(prepared["point"]["preparation"].is_object());
                assert_eq!(prepared["local_quorum_constructed"], false);
            }
            if matches!(
                case,
                Case::BridgeCancelledAfterNative | Case::BridgeCommitUnknown
            ) {
                return bridge_retry_tests::exercise(
                    coordinator,
                    relocation,
                    phase,
                    range,
                    target,
                    report,
                    case,
                    &lifetime,
                );
            }
            let before = report.data_call("actual_target_bridge_before_request", || {
                preparation_tests::control(target, range, None)
            })?;
            // The actual recipient independently fetches ALL actual prepared
            // owners over authenticated TLS, including its direct self-read.
            let bridge = report
                .data_call("actual_target_recipient_ALL_prepared_native_bridge", || {
                    coordinator.drive_finite_remote_bridge(relocation, phase, &node(4), &lifetime)
                });
            report.event(
                serde_json::json!({"actual_bridge_return":bridge.as_ref().ok(),
                "actual_bridge_error":bridge.as_ref().err().map(ToString::to_string),
                "actual_final_member_count":members.len(),"stock_SDK_postrelease_not_reached":true,
                "objects_in_one_process_not_VMs":true}),
            );
            let after = report.data_call("actual_target_bridge_after_actual_return", || {
                preparation_tests::control(target, range, None)
            });
            report.event(serde_json::json!({"native_before":before,"native_after":after.as_ref().ok(),
                "diagnostic_error":after.as_ref().err().map(ToString::to_string),"actual_omitted_preparation_member":omitted_member}));
            let after = after?;
            if case == Case::BridgeMissingPrepared {
                let error = bridge
                    .as_ref()
                    .err()
                    .ok_or_else(|| invalid("actual unprepared member must refuse native bridge"))?;
                assert!(error
                    .to_string()
                    .contains("actual original host preparation owner unavailable"));
                assert_eq!(after["storage"]["bridge_current"], serde_json::Value::Null);
                assert_eq!(
                    after["memory_before_storage"]["bridge"]["actual_native_attempt"],
                    serde_json::Value::Null
                );
                assert_eq!(before["storage"]["current"], after["storage"]["current"]);
                return Ok(());
            }
            let bridge = bridge?;
            assert_eq!(
                after["memory_before_storage"]["bridge"]["completed_native_owner_retained"],
                true
            );
            assert_eq!(
                after["memory_before_storage"]["bridge"]["actual_native_attempt"]["actual_return"],
                "actual_native_returned_known"
            );
            assert!(
                after["memory_before_storage"]["bridge"]["actual_native_attempt"]
                    ["actual_issued_xid"]
                    .as_u64()
                    .is_some_and(|x| x > 0)
            );
            assert_eq!(
                after["storage"]["bridge_current"]["decoded"],
                after["storage"]["bridge_archive"]["decoded"]
            );
            assert_eq!(before["storage"]["current"], after["storage"]["current"]);
            assert!(bridge["point"]["native_bridge_sha256"]
                .as_str()
                .is_some_and(|s| s.len() == 64));
            assert_eq!(
                bridge["point"]["catalogue_activation_sha256"],
                serde_json::Value::Null
            );
            assert_eq!(bridge["point"]["actual_dropped_open"], false);
            assert_eq!(bridge["point"]["actual_writer_installed"], false);
            assert_eq!(bridge["local_quorum_constructed"], false);
            Ok(())
        }));
        stop.store(true, Ordering::Release);
        for worker in workers {
            let (errors, omitted) = match worker.join() {
                Ok(actual) => actual,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            report.event(serde_json::json!({"actual_source_owner_poll_errors":errors,"poll_errors_omitted":omitted}));
        }
        match actual {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
}
