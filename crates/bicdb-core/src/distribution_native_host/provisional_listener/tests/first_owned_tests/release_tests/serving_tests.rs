//! Real finite actor steps and the SAME stock SDK object created before Close.
//! These are same-process TCP/TLS controls, not VM/RPO/capacity qualification.
use super::*;
mod partial_drain_tests;
#[allow(clippy::too_many_arguments)]
fn actor_call(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    node_id: &ClusterNodeId,
    range: crate::RangeId,
    phase: Uuid,
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    parent: &CancellationToken,
    generic: bool,
) -> Result<serde_json::Value> {
    if *node_id == node(metadata) {
        let request = parent.child_with_deadline(Instant::now() + Duration::from_secs(4));
        let result = if generic {
            coordinator.test_local_policy_generic_refusal(range, phase, &request)
        } else {
            coordinator.advance_finite_remote_serving_policy(range, phase, &request)
        };
        return Ok(policy_return_data(result));
    }
    let (send, receive) = mpsc::sync_channel(1);
    if *node_id == node(4) {
        target
            .send(OwnerCommand::LocalServing(range, phase, generic, send))
            .map_err(|_| invalid("actual target Policy actor ended"))?;
    } else {
        controls
            .get(node_id)
            .ok_or_else(|| invalid("actual Policy source actor absent"))?
            .send(Control::LocalServing(range, phase, generic, send))
            .map_err(|_| invalid("actual source Policy actor ended"))?;
    }
    receive
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("actual Policy actor return unavailable"))
}
fn require_actual_ok(actual: serde_json::Value) -> Result<serde_json::Value> {
    if actual["actual_api_ok"] != true {
        return Err(invalid(&format!(
            "actual owner-local Policy API failed: {actual}"
        )));
    }
    Ok(actual["actual_result"].clone())
}
#[allow(clippy::too_many_arguments)]
fn actual_snapshot(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    node_id: &ClusterNodeId,
    range: crate::RangeId,
    phase: Uuid,
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
) -> Result<serde_json::Value> {
    if *node_id == node(metadata) {
        coordinator.test_remote_release_control(range, None)
    } else if *node_id == node(4) {
        target_control(target, range, phase, false, None)
    } else {
        source_control(
            controls
                .get(node_id)
                .ok_or_else(|| invalid("actual snapshot source absent"))?,
            range,
            phase,
            false,
            None,
        )
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) fn missing_released(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    range: crate::RangeId,
    phase: Uuid,
    members: &[ClusterNodeId],
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let local = members
        .iter()
        .find(|n| **n != node(4))
        .ok_or_else(|| invalid("genuine final survivor absent"))?;
    let before = report.data_call(
        "actual_policy_receiver_native_before_missing_real_release",
        || actual_snapshot(coordinator, metadata, local, range, phase, controls, target),
    )?;
    let actual = report.data_call(
        "actual_owner_local_Policy_missing_one_real_Released",
        || {
            actor_call(
                coordinator,
                metadata,
                local,
                range,
                phase,
                controls,
                target,
                parent,
                false,
            )
        },
    );
    let after = report.data_call(
        "actual_policy_receiver_native_after_missing_real_release",
        || actual_snapshot(coordinator, metadata, local, range, phase, controls, target),
    );
    report.event(serde_json::json!({"actual_policy_return":actual.as_ref().ok(),"delivery_error":actual.as_ref().err().map(ToString::to_string),
        "actual_after":after.as_ref().ok(),"snapshot_error":after.as_ref().err().map(ToString::to_string),
        "target_release_was_not_called":true}));
    let actual = actual?;
    let after = after?;
    assert_eq!(actual["actual_api_ok"], false);
    assert_eq!(actual["actual_cluster_body"],format!("cluster node {} rejected RPC cluster_data_operation_failed: cluster error: Serving writer policy still has an actual Frozen owner",node(4)),
        "wrong earlier refusal is an unreached actual ALL-Released cut");
    same_native(&before, &after);
    assert_eq!(before["queue"], after["queue"]);
    for field in [
        "memory",
        "actual_owner_DATA",
        "actual_owner_error",
        "actual_release_API_returns",
        "outer_API_returns",
        "policy_API_returns",
        "actual_drop_cut",
    ] {
        assert_eq!(
            before["actual_successor"][field], after["actual_successor"][field],
            "missing Released cannot alter actual native/owner effect {field}"
        );
    }
    let old_count = before["actual_successor"]["policy_outer_API_returns"]["returns"]
        .as_u64()
        .expect("actual policy outer history required");
    assert_eq!(
        after["actual_successor"]["policy_outer_API_returns"]["returns"],
        old_count + 1
    );
    assert_eq!(
        after["actual_successor"]["policy_outer_API_returns"]["last_actual_return_ok"],
        false
    );
    assert!(
        !after["actual_successor"]["policy_outer_API_returns"]["first_original_error"].is_null(),
        "original failed Policy return must remain attributed"
    );
    Ok(())
}
#[allow(clippy::too_many_arguments)]
pub(super) fn serve(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    range: crate::RangeId,
    phase: Uuid,
    members: &[ClusterNodeId],
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
    original: &crate::ClusterEventClient,
    old_key: &str,
    original_topology: &ClusterTopology,
    relocation: crate::distribution::RelocationId,
    case: Case,
) -> Result<()> {
    let old_value = serde_json::json!({"actual":"selected-original-ACK"});
    // All earlier sdk() attempts must have returned successfully for this
    // fixture to reach here. Preserve every original value, including the
    // untouched range; do not redefine preservation to only one selected row.
    assert_eq!(
        report.omitted, 0,
        "original SDK intent ledger must be complete"
    );
    let original_intents = report
        .events
        .iter()
        .filter(|event| event["stage"] == "original_sdk_attempt")
        .map(|event| {
            let namespace = event["namespace"]
                .as_str()
                .ok_or_else(|| invalid("original SDK namespace missing"))?
                .to_owned();
            let key = event["key"]
                .as_str()
                .ok_or_else(|| invalid("original SDK key missing"))?
                .to_owned();
            Ok((namespace, key, event["expected_full_value"].clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !original_intents.is_empty(),
        "actual original SDK corpus required"
    );
    let before = report.data_call("same_original_SDK_Strong_before_actual_policy", || {
        original.get_strong("items", old_key)
    });
    if let Ok(value) = before {
        assert_eq!(
            serde_json::to_value(value)?,
            old_value,
            "no successful false absence while readiness incomplete"
        );
    }
    let mut installed = BTreeMap::new();
    for member in members {
        if case == Case::ServingPartialDrainResume && *member == node(4) {
            return partial_drain_tests::run(
                coordinator,
                metadata,
                member,
                range,
                phase,
                controls,
                target,
                report,
                parent,
            );
        }
        if case == Case::ServingDrainResume && *member == node(4) {
            return verify_actual_cancelled_drain_resume(
                coordinator,
                metadata,
                member,
                range,
                phase,
                controls,
                target,
                report,
                parent,
            );
        }
        let raw = report.data_call(
            "actual_fresh_local_self_role_and_native_policy_install",
            || {
                actor_call(
                    coordinator,
                    metadata,
                    member,
                    range,
                    phase,
                    controls,
                    target,
                    parent,
                    false,
                )
            },
        )?;
        let actual = require_actual_ok(raw)?;
        assert_eq!(
            actual["transition"],
            "actual_policy_installed_readiness_still_request_bound"
        );
        assert_eq!(actual["incomplete"], true);
        assert_eq!(actual["Serving"], false);
        assert_eq!(actual["actual_local_node"], serde_json::to_value(member)?);
        assert_eq!(
            actual["actual_self_role_purpose"],
            "NativeJointLocalServingPolicy"
        );
        installed.insert(member.clone(), actual);
    }
    if case == Case::ServingSuccessorRead {
        return verify_registered_replacement_route(
            coordinator,
            relocation,
            range,
            phase,
            target,
            report,
            parent,
        );
    }
    // At least one genuine final member is neither data nor metadata leader.
    // Its actual production self-role/install already succeeded above.
    let nonleader = members
        .iter()
        .find(|n| {
            **n != node(metadata)
                && installed[*n]["selected_leader"] != serde_json::to_value(*n).unwrap()
        })
        .ok_or_else(|| invalid("real fixture has no nonleader final self-role recipient"))?;
    let before = report.data_call(
        "actual_installed_nonleader_before_generic_role_refusal",
        || {
            actual_snapshot(
                coordinator,
                metadata,
                nonleader,
                range,
                phase,
                controls,
                target,
            )
        },
    )?;
    let raw = report.data_call(
        "actual_live_self_Policy_witness_generic_mutation_factory_refusal",
        || {
            actor_call(
                coordinator,
                metadata,
                nonleader,
                range,
                phase,
                controls,
                target,
                parent,
                true,
            )
        },
    )?;
    let after = report.data_call(
        "actual_installed_nonleader_after_generic_role_refusal",
        || {
            actual_snapshot(
                coordinator,
                metadata,
                nonleader,
                range,
                phase,
                controls,
                target,
            )
        },
    )?;
    let refusal = require_actual_ok(raw)?;
    assert_eq!(refusal["actual_generic_factory_returned_ok"], false);
    assert_eq!(
        refusal["actual_cluster_body"],
        "range recovery: local-policy purpose cannot authorize generic recovery mutations"
    );
    assert_eq!(refusal["actual_self"], serde_json::to_value(nonleader)?);
    same_native(&before, &after);
    assert_eq!(before["queue"], after["queue"]);
    for member in members {
        let raw = report.data_call(
            "actual_native_majority_recovery_and_guarded_dispatch",
            || {
                actor_call(
                    coordinator,
                    metadata,
                    member,
                    range,
                    phase,
                    controls,
                    target,
                    parent,
                    false,
                )
            },
        )?;
        let actual = require_actual_ok(raw)?;
        assert_eq!(
            actual["transition"],
            "actual_native_recovery_and_guarded_DATA_publication_returned"
        );
        assert_eq!(actual["actual_local_node"], serde_json::to_value(member)?);
    }
    let old = report.data_call(
        "same_pre_move_SDK_full_Strong_of_original_ACK_after_real_serving",
        || original.get_strong("items", old_key),
    )?;
    assert_eq!(
        serde_json::to_value(old)?,
        old_value,
        "every original known ACK must survive; no false Strong404"
    );
    for (namespace, key, value) in original_intents {
        let actual = report
            .data_call("same_pre_move_SDK_full_Strong_of_every_prior_ACK", || {
                original.get_strong(&namespace, &key)
            })?;
        assert_eq!(
            serde_json::to_value(actual)?,
            value,
            "original SDK value changed or became false Strong absence: {namespace}/{key}"
        );
    }
    let new_key = key(original_topology, range, "after-real-serving-new-command")?;
    let value = serde_json::json!({"actual":"same-stock-client-after-owned-serving",
        "nested":{"all":[null,true,17,{"exact":"full value"}],"unicode":"λ"}});
    report.event(serde_json::json!({"stage":"same_pre_move_SDK_new_append_intent","namespace":"items","key":new_key,
        "expected_full_value":value,"request_timeout_ms":2000,"manual_routing_refresh":false,
        "SDK_initial_four_node_config_unchanged":true,"raw_wire_attempts_not_recorded":true}));
    let append = report.data_call("same_pre_move_SDK_real_new_append_ACK", || {
        original.append("items", &new_key, value.clone())
    });
    if append.is_err() {
        // Preserve possible success/uncertainty. Readback does not rewrite the
        // original error into an acknowledgement or a passing control.
        let _ = report.data_call(
            "actual_Strong_reconcile_uncertain_append_before_failure",
            || original.get_strong("items", &new_key),
        );
    }
    assert_eq!(
        append?, true,
        "fresh actual command required; all-existing corpus is not write acceptance"
    );
    let read = report.data_call("same_pre_move_SDK_full_Strong_of_new_real_ACK", || {
        original.get_strong("items", &new_key)
    })?;
    assert_eq!(serde_json::to_value(read)?, value);
    if case == Case::ServingMetadataActivation {
        metadata_activation_after_actual_serving(
            coordinator,
            metadata,
            relocation,
            range,
            phase,
            members,
            controls,
            target,
            report,
            parent,
        )?;
    }
    report.event(serde_json::json!({"actual_inprocess_original_SDK_ACK_and_full_Strong_controls_reached":true,
        "independent_process_VM_RPO_capacity_NOT_QUALIFIED":true,
        "final_metadata_controller_cold_repeated_relocation_cleanup_NOT_COMPLETE":true}));
    Ok(())
}

fn verify_registered_replacement_route(
    coordinator: &NativeMetadataHost,
    relocation: crate::distribution::RelocationId,
    range: crate::RangeId,
    phase: Uuid,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let (send, receive) = mpsc::sync_channel(1);
    target
        .send(OwnerCommand::PolicySocketProfile(range, phase, send))
        .map_err(|_| invalid("actual replacement listener owner ended"))?;
    let profile = receive
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("actual replacement listener profile unavailable"))?;
    let before = report.data_call(
        "actual_replacement_socket_native_before_readonly_dispatch",
        || target_control(target, range, phase, false, None),
    )?;
    let read = report.data_call(
        "actual_pinned_replacement_policy_read_and_nonselected_original_handler",
        || coordinator.test_registered_successor_read_dispatch(relocation, phase, &node(4), parent),
    );
    let after = report.data_call(
        "actual_replacement_socket_native_after_readonly_dispatch",
        || target_control(target, range, phase, false, None),
    );
    report.event(serde_json::json!({"actual_listener_profile":profile,
        "actual_read_and_denial":read.as_ref().ok(),"read_error":read.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        "actual_native_after":after.as_ref().ok(),"snapshot_error":after.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        "source_component_only_no_APP_Strong_acceptance":true}));
    let profile = require_actual_ok(profile)?;
    let read = read?;
    let after = after?;
    assert_eq!(profile["actual_Registered_Joined_owner"], true);
    assert_eq!(profile["actual_replacement_listener_bound"], true);
    assert_eq!(profile["actual_original_drained_owner_verified"], true);
    assert_eq!(
        profile["original_listeners"]["replica_listener_present"],
        false
    );
    assert_eq!(
        profile["original_listeners"]["old_replica_listener_present"],
        false
    );
    assert_eq!(profile["actual_APP_dispatcher_published"], false);
    assert_eq!(
        read["actual_policy_read_returned_ok"], true,
        "actual replacement protected source route was not reached: {read}"
    );
    let point = &read["actual_policy_read"]["point"];
    assert_eq!(point["actual_dropped_open"], true);
    assert_eq!(
        point["policy_released_audit"]["local_node_id"],
        serde_json::to_value(node(4))?
    );
    assert_eq!(
        read["actual_pinned_destination"],
        serde_json::to_value(node(4))?
    );
    assert_eq!(read["actual_nonselected_returned_ok"], false);
    assert_eq!(read["actual_nonselected_cluster_body"], format!(
        "cluster node {} rejected RPC cluster_data_operation_failed: cluster error: native host bootstrap: Joined/non-INITIAL owner has no source hosting authority", node(4)),
        "nonselected operation must preserve exact original INITIAL denial, not fallback on another error");
    same_native(&before, &after);
    assert_eq!(before["queue"], after["queue"]);
    assert_eq!(before["actual_successor"], after["actual_successor"]);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_actual_cancelled_drain_resume(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    member: &ClusterNodeId,
    range: crate::RangeId,
    phase: Uuid,
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let (send, receive) = mpsc::sync_channel(1);
    target
        .send(OwnerCommand::LocalServingDrainCancel(range, phase, send))
        .map_err(|_| invalid("actual target drain actor ended"))?;
    let cancelled = report.data_call(
        "actual_Registered_Closed_drain_API_cancel_return_and_retained_owner",
        || {
            receive
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| invalid("actual cancelled drain return unavailable"))
        },
    );
    let retained = report.data_call(
        "actual_same_native_and_successor_after_failed_Closed_drain",
        || target_control(target, range, phase, false, None),
    );
    report.event(
        serde_json::json!({"actual_cancelled_drain":cancelled.as_ref().ok(),
        "delivery_error":cancelled.as_ref().err().map(ToString::to_string),
        "actual_retained_native_successor":retained.as_ref().ok(),
        "snapshot_error":retained.as_ref().err().map(ToString::to_string),
        "snapshots_are_separate_nonatomic_DATA":true}),
    );
    let cancelled = require_actual_ok(cancelled?)?;
    let retained = retained?;
    assert_eq!(
        cancelled["actual_cut_after_original_Closed_life_ended"],
        true
    );
    assert!(
        cancelled["actual_original_remaining_us_before_cancel"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "expiry or an earlier failure did not reach the selected live cancellation cut"
    );
    assert_eq!(
        cancelled["actual_stop_and_drain_return"]["actual_returned_ok"],
        false
    );
    assert_eq!(
        cancelled["actual_stop_and_drain_return"]["actual_typed_QueryCanceled"], true,
        "the original actual drain API must return exact QueryCanceled, not another failure"
    );
    assert_eq!(cancelled["original_policy_API_returned_ok"], false);
    assert_eq!(cancelled["original_policy_typed_QueryCanceled"], true);
    assert!(!cancelled["original_policy_error"].is_null());
    assert!(!cancelled["actual_stop_and_drain_return"]["original_error"].is_null());
    assert_eq!(
        cancelled["actual_retained_Assigned"]["actual_original_Closed_life"],
        false
    );
    assert_eq!(
        cancelled["actual_retained_Assigned"]["actual_same_listener_retained"],
        true
    );
    assert_eq!(
        cancelled["actual_retained_Assigned"]["actual_stopping"],
        false
    );
    assert_eq!(cancelled["actual_Joined_owner_present"], false);
    assert_eq!(
        cancelled["actual_original_install_evidence"]["stage"],
        "Installed"
    );
    assert!(cancelled["install_evidence_error"].is_null());
    for history in ["policy_API_returns", "policy_outer_API_returns"] {
        assert_eq!(
            retained["actual_successor"][history]["last_actual_return_ok"],
            false
        );
        assert!(
            !retained["actual_successor"][history]["first_original_error"].is_null(),
            "original actual policy/drain error must be retained in {history}"
        );
    }
    // A distinct actual owner command, SAME retained Installed/native attempt.
    // Production repair must finish the real drain/transfer before obtaining
    // new self-TLS authority. It never revives the old Closed admission.
    let resumed = report.data_call(
        "distinct_actor_same_Installed_owner_actual_drain_transfer_Joined_bind",
        || {
            actor_call(
                coordinator,
                metadata,
                member,
                range,
                phase,
                controls,
                target,
                parent,
                false,
            )
        },
    );
    let (send, receive) = mpsc::sync_channel(1);
    target
        .send(OwnerCommand::PolicySocketProfile(range, phase, send))
        .map_err(|_| invalid("actual resumed Joined owner ended"))?;
    let profile = report.data_call(
        "actual_same_Installed_owner_after_resumed_Joined_bind",
        || {
            receive
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| invalid("actual resumed Joined profile unavailable"))
        },
    );
    let after = report.data_call(
        "actual_native_and_successor_after_distinct_drain_resume",
        || target_control(target, range, phase, false, None),
    );
    report.event(serde_json::json!({"actual_resume":resumed.as_ref().ok(),
        "actual_resume_error":resumed.as_ref().err().map(ToString::to_string),
        "actual_Joined_profile":profile.as_ref().ok(),
        "profile_error":profile.as_ref().err().map(ToString::to_string),
        "actual_native_after":after.as_ref().ok(),
        "native_snapshot_error":after.as_ref().err().map(ToString::to_string),
        "source_component_only_no_SDK_Strong_or_automatic_recovery_claim":true}));
    let resumed = require_actual_ok(resumed?)?;
    let profile = require_actual_ok(profile?)?;
    let after = after?;
    assert_eq!(
        resumed["transition"],
        "actual_policy_installed_readiness_still_request_bound"
    );
    assert_eq!(resumed["incomplete"], true);
    assert_eq!(resumed["Serving"], false);
    assert_eq!(profile["actual_Registered_Joined_owner"], true);
    assert_eq!(profile["actual_replacement_listener_bound"], true);
    assert_eq!(profile["actual_original_drained_owner_verified"], true);
    assert_eq!(profile["actual_APP_dispatcher_published"], false);
    assert_eq!(
        profile["original_listeners"]["replica_listener_present"],
        false
    );
    assert_eq!(
        profile["original_listeners"]["old_replica_listener_present"],
        false
    );
    assert_eq!(
        profile["actual_original_install_evidence"], cancelled["actual_original_install_evidence"],
        "drain recovery must consume the exact original Installed coordinator/open/native record"
    );
    same_native(&retained, &after);
    assert_eq!(retained["queue"], after["queue"]);
    for history in ["policy_API_returns", "policy_outer_API_returns"] {
        assert_eq!(
            after["actual_successor"][history]["first_original_error"],
            retained["actual_successor"][history]["first_original_error"],
            "a new actual continuation result must not rewrite the old error"
        );
        assert_eq!(
            after["actual_successor"][history]["last_actual_return_ok"],
            true
        );
        let original_count = retained["actual_successor"][history]["returns"]
            .as_u64()
            .expect("original actual policy history required");
        assert_eq!(
            after["actual_successor"][history]["returns"],
            original_count + 1
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn metadata_activation_missing_released(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    relocation: crate::distribution::RelocationId,
    range: crate::RangeId,
    phase: Uuid,
    members: &[ClusterNodeId],
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    assert!(
        !members.contains(&node(metadata)),
        "actual metadata observer must be outside final data voters"
    );
    let topology_before = coordinator.test_first_topology(parent)?;
    let before = actual_snapshot(
        coordinator,
        metadata,
        &node(4),
        range,
        phase,
        controls,
        target,
    )?;
    let actual = report.data_call(
        "actual_outside_NEW_metadata_owner_collects_unreleased_recipient_before_proposal",
        || coordinator.record_finite_remote_activation(relocation, phase, parent),
    );
    let topology_after = coordinator.test_first_topology(parent);
    let after = actual_snapshot(
        coordinator,
        metadata,
        &node(4),
        range,
        phase,
        controls,
        target,
    );
    let owner_state = coordinator.test_remote_activation_state(relocation, phase);
    report.event(serde_json::json!({"actual_activation_return":actual.as_ref().ok(),
        "original_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        "before":before,"after":after.as_ref().ok(),"after_error":after.as_ref().err().map(ToString::to_string),
        "topology_before":topology_before,"topology_after":topology_after.as_ref().ok(),
        "topology_error":topology_after.as_ref().err().map(ToString::to_string),
        "actual_target_Release_not_called":true,"metadata_observer_outside_final_NEW":true,
        "actual_metadata_owner_attempt":owner_state.as_ref().ok(),"owner_state_error":owner_state.as_ref().err().map(ToString::to_string)}));
    let error = actual.expect_err("actual unreleased recipient must refuse ALL activation proof");
    let expected=format!("cluster node {} rejected RPC cluster_data_operation_failed: cluster error: Serving writer policy still has an actual Frozen owner",node(4));
    assert!(
        matches!(&error,BicDbError::Cluster(body) if body==&expected),
        "earlier unrelated error is an unreached control: {error:?}"
    );
    let topology_after = topology_after?;
    let after = after?;
    let owner_state = owner_state?;
    assert!(!owner_state["first_original_API_error"].is_null());
    assert_eq!(owner_state["actual_attempt_present"], false);
    assert!(owner_state["actual_issued_entry"].is_null());
    assert_eq!(
        topology_before, topology_after,
        "failed proof cannot append a metadata transition"
    );
    same_native(&before, &after);
    assert_eq!(before["queue"], after["queue"]);
    assert_eq!(
        topology_after.relocations[&relocation]
            .native_joint_handoff_v1
            .as_ref()
            .unwrap()
            .stage(),
        crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn metadata_activation_after_actual_serving(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    relocation: crate::distribution::RelocationId,
    range: crate::RangeId,
    phase: Uuid,
    members: &[ClusterNodeId],
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    assert!(
        !members.contains(&node(metadata)),
        "actual metadata leader must be outside final data voters"
    );
    let topology_before = coordinator.test_first_topology(parent)?;
    let mut before = BTreeMap::new();
    for member in members {
        before.insert(
            member.clone(),
            actual_snapshot(
                coordinator,
                metadata,
                member,
                range,
                phase,
                controls,
                target,
            )?,
        );
    }
    let actual = report.data_call(
        "actual_metadata_owner_ALL_released_proof_and_protected_activation_commit",
        || coordinator.record_finite_remote_activation(relocation, phase, parent),
    );
    let topology_after = coordinator.test_first_topology(parent);
    let mut after = BTreeMap::new();
    for member in members {
        after.insert(
            member.clone(),
            actual_snapshot(
                coordinator,
                metadata,
                member,
                range,
                phase,
                controls,
                target,
            ),
        );
    }
    report.event(serde_json::json!({"actual_activation_return":actual.as_ref().ok(),
        "original_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        "topology_before":topology_before,"topology_after":topology_after.as_ref().ok(),
        "topology_error":topology_after.as_ref().err().map(ToString::to_string),
        "before":before,"after":after.iter().map(|(node,r)|(node,serde_json::json!({"actual":r.as_ref().ok(),"error":r.as_ref().err().map(ToString::to_string)}))).collect::<BTreeMap<_,_>>(),
        "earlier_original_SDK_ACK_and_full_Strong_assertions_reached":true,
        "metadata_observer_outside_final_NEW":true,"source_retirement_and_cold_cleanup_NOT_PROVEN":true}));
    let actual = actual?;
    let topology_after = topology_after?;
    assert_eq!(actual["known_selected_commit"], true);
    let record = topology_after.relocations[&relocation]
        .native_joint_handoff_v1
        .as_ref()
        .unwrap();
    assert_eq!(
        record.stage(),
        crate::distribution::NativeJointHandoffStageV1::AwaitingCleanup
    );
    assert_eq!(
        record.activated_quorum_sha256(),
        actual["actual_activation_sha256"].as_str()
    );
    assert_eq!(record.source_cleanup_sha256(), None);
    assert_eq!(
        topology_after.relocations[&relocation].phase,
        crate::distribution::RelocationPhase::CleaningUp
    );
    assert_eq!(
        topology_before.ranges, topology_after.ranges,
        "activation metadata changes no data membership"
    );
    for (member, observed) in after {
        let observed = observed?;
        same_native(&before[&member], &observed);
        assert_eq!(before[&member]["queue"], observed["queue"]);
    }
    // A distinct request observes the SAME known issued entry. No second
    // one-shot collection or topology mutation is used to infer old success.
    let repeated = report.data_call(
        "distinct_request_observes_same_actual_activation_commit_without_reproposal",
        || coordinator.record_finite_remote_activation(relocation, phase, parent),
    )?;
    assert_eq!(
        repeated["actual_issued_entry"],
        actual["actual_issued_entry"]
    );
    assert_eq!(coordinator.test_first_topology(parent)?, topology_after);
    Ok(())
}
