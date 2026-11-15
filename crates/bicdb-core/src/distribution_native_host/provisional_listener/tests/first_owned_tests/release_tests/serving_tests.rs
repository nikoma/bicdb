//! Real finite actor steps and the SAME stock SDK object created before Close.
//! These are same-process TCP/TLS controls, not VM/RPO/capacity qualification.
use super::*;
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
    report.event(serde_json::json!({"actual_inprocess_original_SDK_ACK_and_full_Strong_controls_reached":true,
        "independent_process_VM_RPO_capacity_NOT_QUALIFIED":true,
        "final_metadata_controller_cold_repeated_relocation_cleanup_NOT_COMPLETE":true}));
    Ok(())
}
