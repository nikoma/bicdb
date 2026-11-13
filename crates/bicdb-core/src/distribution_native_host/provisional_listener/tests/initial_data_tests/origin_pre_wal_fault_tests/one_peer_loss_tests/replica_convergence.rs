//! Required reachable follower convergence, separate from public availability.
//! The held value is the actual borrowed !Send debt-only scheduling guard from
//! the sole caller; it is dropped, never interpreted as permission or proof.
use super::*;
use std::collections::BTreeMap;

pub(super) fn snapshot(
    f: &Fixture,
    target: usize,
    range: &crate::RangeDescriptor,
    keys: &[&str],
) -> Result<serde_json::Value> {
    let open = {
        let service = f.hosts[target]._service.try_lock().ok_or_else(|| {
            BicDbError::Cluster("actual target service busy during diagnostic".into())
        })?;
        service.test_native_replica_open_state(range.id)?
    };
    let db = f.hosts[target]
        .seed
        .database
        .try_read()
        .ok_or_else(|| BicDbError::Cluster("actual target DB busy during diagnostic".into()))?;
    let prefix = db.native_range_prefix_observation(range.id)?;
    let lineage = db.current_native_range_source_lineage(range.id)?;
    check(
        prefix.native_applied_index == Some(lineage.range_index()) && !prefix.invalidated,
        "actual target native diagnostic changed between reads",
    )?;
    let mut records = BTreeMap::new();
    for key in keys {
        records.insert(
            (*key).to_owned(),
            db.get(
                crate::CLUSTER_EVENTS_COLLECTION,
                &crate::cluster_event_record_id("items", key)?,
            )?,
        );
    }
    Ok(
        serde_json::json!({"actual_open":open,"actual_prefix":prefix,
        "native_R":lineage.range_index(),"typed_local_B":lineage.actual_local_visibility_sequence(),
        "H0":lineage.logical_genesis_sha256(),"H":lineage.logical_history_sha256(),"records":records,
        "diagnostic_not_role_or_Ready":true}),
    )
}
#[allow(clippy::too_many_arguments)]
pub(super) fn run<
    ActualHeldDebtSchedule,
    ResumeActualTarget: FnOnce(&CancellationToken) -> Result<()>,
>(
    f: &Fixture,
    origin_control: &NativeMetadataControlHost,
    journal: &Arc<Journal>,
    client: &crate::ClusterEventClient,
    range: &crate::RangeDescriptor,
    topology: &ClusterTopology,
    values: &[(&str, &serde_json::Value)],
    original_unknown: &crate::RangeWriteCommand,
    original_ack: &crate::RangeWriteCommand,
    actual_reply_loss: serde_json::Value,
    actual_debt_schedule: ActualHeldDebtSchedule,
    first_public_receipt_ns: u128,
    expected_blocked_native: u64,
    expected_blocked_phase: &str,
    resume_actual_target: ResumeActualTarget,
    partial_repeat: Option<&[repeat_origin::OriginalWriter]>,
) -> Result<serde_json::Value> {
    check(
        first_public_receipt_ns <= 20_000_000_000,
        "original public availability receipt exceeded unchanged20s",
    )?;
    let fourth = key_in(topology, range.id, "origin-after-availability-ack");
    let fourth_value = serde_json::json!({"fourth":"must reach the previously frozen follower automatically",
        "full":[null,{},[4,5]],"nested":{"preserve":true}});
    let mut keys = values.iter().map(|(k, _)| *k).collect::<Vec<_>>();
    keys.push(&fourth);
    let before = snapshot(f, 2, range, &keys)?;
    journal.append(serde_json::json!({"kind":"replica_convergence_before_r4",
        "first_public_20s_receipt_ns":first_public_receipt_ns,"target":node(2),"actual":before,
        "actual_post_producer_reply_loss":actual_reply_loss,"actual_debt_schedule_still_held":true}))?;
    check(
        before["native_R"] == expected_blocked_native
            && before["actual_open"]["phase"] == expected_blocked_phase
            && before["actual_open"]["actual_current_open_valid"] == false,
        "follower did not retain its required actual blocked native/phase cut after publicR3",
    )?;
    check(
        before["actual_open"]["actual_store_open"]
            == actual_reply_loss["actual_reply"]["store_open_id"],
        "follower convergence must use SAME original Store open",
    )?;
    // New independent success clock, including R4 API work. Never restart it
    // after the scheduling unlock, activation, replay or an observation error.
    let start = Instant::now();
    let end = start + Duration::from_secs(20);
    let parent = CancellationToken::uncancelable().child_with_deadline(end);
    journal.append(
        serde_json::json!({"kind":"replica_convergence_clock_before_r4",
        "limit_ns":20_000_000_000u64,"first_public_clock_not_replaced":true,
        "target":node(2),"fourth_key":fourth,"full_fourth_value":fourth_value}),
    )?;
    let ack = append(journal, client, &fourth, &fourth_value);
    let strong = read(journal, client, &fourth);
    journal.append(serde_json::json!({"kind":"actual_r4_before_debt_release",
        "elapsed_ns":start.elapsed().as_nanos(),"actual_api_ack":ack.is_ok(),
        "append_error":ack.as_ref().err().map(ToString::to_string),
        "actual_Strong":strong.as_ref().ok(),"Strong_error":strong.as_ref().err().map(ToString::to_string)}))?;
    check(
        !ack.is_ok() || no_false_successful_value(&strong, &fourth_value),
        "false successful absence/wrong value after actualR4 ACK",
    )?;
    check(
        ack.is_ok()
            && strong
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&fourth_value)),
        "required actualR4 ACK/fullStrong before follower repair failed",
    )?;
    parent.check()?;
    let still_frozen = snapshot(f, 2, range, &keys)?;
    journal.append(
        serde_json::json!({"kind":"actual_target_still_frozen_after_real_r4",
        "elapsed_ns":start.elapsed().as_nanos(),"actual":still_frozen,
        "ordinary_APP_repair_did_not_unfreeze":true}),
    )?;
    check(
        still_frozen["native_R"] == expected_blocked_native
            && still_frozen["actual_open"]["phase"] == expected_blocked_phase
            && still_frozen["actual_open"]["actual_current_open_valid"] == false
            && still_frozen["records"][values[2].0].is_null()
            && still_frozen["records"][&fourth].is_null(),
        "ordinary APP unexpectedly materialized/unfroze target before actual debt producer",
    )?;
    // Every real ACK, including a same-value update whose API says
    // inserted=false, can consume a native command slot. Retain ALL actual
    // commands through the actual current source head, never assume four
    // logical value names imply exactly four native commands.
    let source_head = {
        let db = f.hosts[0].seed.database.read();
        db.current_native_range_source_lineage(range.id)?
            .range_index()
    };
    journal.append(
        serde_json::json!({"kind":"actual_complete_source_head_after_fourth_logical_ACK",
        "elapsed_ns":start.elapsed().as_nanos(),"actual_native_head":source_head,
        "same_value_ACKs_are_not_dropped_or_reclassified":true}),
    )?;
    check(
        source_head >= 4,
        "required actual native R3/R4 command coverage not established",
    )?;
    let mut commands = vec![original_ack.clone(), original_unknown.clone()];
    for index in 3..=source_head {
        let actual = f.hosts[0]
            ._service
            .lock()
            .probe_range_write_for_peer(range, &node(0), index)?
            .entry
            .ok_or_else(|| BicDbError::Cluster("actual ACK command payload not retained".into()))?;
        check(
            actual.state == crate::RangeWriteState::Applied,
            "source ACK must be actual Applied/native materialized",
        )?;
        commands.push(actual.command);
    }
    let source = {
        let db = f.hosts[0].seed.database.read();
        let lineage = db.current_native_range_source_lineage(range.id)?;
        check(
            lineage.range_index() == source_head,
            "actual source head changed during full retained-command proof",
        )?;
        for command in &commands {
            db.require_native_command_materialized(&lineage, command)?;
        }
        let mut records = BTreeMap::new();
        for key in &keys {
            records.insert(
                (*key).to_owned(),
                db.get(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", key)?,
                )?,
            );
        }
        serde_json::json!({"native_R":lineage.range_index(),"H0":lineage.logical_genesis_sha256(),
            "H":lineage.logical_history_sha256(),"typed_local_B":lineage.actual_local_visibility_sequence(),"records":records})
    };
    journal.append(
        serde_json::json!({"kind":"actual_origin_r4_full_commands_before_debt_release",
        "elapsed_ns":start.elapsed().as_nanos(),"source":source,"actual_commands":commands}),
    )?;
    if let Some(originals) = partial_repeat {
        // This distinct REQUIRED scenario intentionally injects the next
        // genuine origin fault BEFORE the old completed follower converges.
        // The retained actual debt guard remains held throughout that APP.
        parent.check()?;
        let partial = serde_json::json!({"actual_source":source,
            "actual_partial_target":still_frozen,"original_first_public_receipt_ns":first_public_receipt_ns,
            "first_replica_convergence_not_claimed":true});
        journal.append(
            serde_json::json!({"kind":"actual_partial_debt_before_second_unknown",
            "actual":partial,"all_original_owners_retained":true}),
        )?;
        return repeat_origin::run_partial(
            f,
            origin_control,
            journal,
            client,
            topology,
            range,
            values[0].0,
            values[0].1,
            originals,
            &partial,
            actual_debt_schedule,
        );
    }
    journal.append(serde_json::json!({"kind":"same_actual_target_network_resume_intent",
        "elapsed_ns":start.elapsed().as_nanos(),"actual_blocked_native":expected_blocked_native,
        "actual_blocked_phase":expected_blocked_phase,"no_protocol_or_permission_from_resume":true}))?;
    resume_actual_target(&parent)?;
    parent.check()?;
    drop(actual_debt_schedule);
    journal.append(
        serde_json::json!({"kind":"actual_debt_only_schedule_released",
        "elapsed_ns":start.elapsed().as_nanos(),"no_manual_tick_or_recovery_or_grant":true}),
    )?;
    let mut last = None;
    while Instant::now() < end {
        parent.check()?;
        let observation = snapshot(f, 2, range, &keys);
        let controller = origin_control
            .initial_data_status(&budget(50))
            .ok()
            .flatten();
        let controller_recorded = controller
            .as_ref()
            .and_then(|s| s.hot_recovery_last_attempt.as_ref())
            .is_some_and(|d| {
                d["actual_progress"]["n2"]["actual_remote_native_cut"] == source_head
                    && d["actual_progress"]["n2"]["actual_source_native_cut"] == source_head
                    && d["replica_debt_pending"]
                        .as_array()
                        .is_some_and(|nodes| !nodes.iter().any(|n| n == "n2"))
            });
        let elapsed = start.elapsed().as_nanos();
        journal.append(serde_json::json!({"kind":"actual_replica_convergence_sample",
            "elapsed_ns":elapsed,"actual":observation.as_ref().ok(),"error":observation.as_ref().err().map(ToString::to_string),
            "actual_controller":controller,"controller_recorded_physical_catchup":controller_recorded}))?;
        if let Ok(actual) = observation {
            let exact = actual["native_R"] == source_head
                && actual["H0"] == source["H0"]
                && actual["H"] == source["H"]
                && actual["records"] == source["records"]
                && actual["actual_open"]["phase"] == "NativeActivatedAnchor"
                && actual["actual_open"]["actual_current_open_valid"] == true;
            if exact && controller_recorded && elapsed <= 20_000_000_000 {
                // These immutable history leaves, not row equality/rawCQ,
                // establish R3, R4 and EVERY later actually committed no-op/
                // update command were physically materialized here.
                let db = f.hosts[2].seed.database.read();
                let lineage = db.current_native_range_source_lineage(range.id)?;
                for command in &commands {
                    db.require_native_command_materialized(&lineage, command)?;
                }
                if expected_blocked_native == 1 {
                    check(
                        crate::format::native_completed_decision_materialization_active(
                            db.data_path(),
                        )?,
                        "actual R4 target lost sticky completed-materialization reader fence",
                    )?;
                    let simulated =
                        crate::format::test_simulated_reader_without_completed_materialization(
                            db.data_path(),
                        );
                    check(simulated.as_ref().err().is_some_and(|e|e.to_string().contains(crate::format::NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG)),
                        "SIMULATED old-feature reader accepted actual converted root at later ordinaryR4")?;
                    let portable = crate::format::required_reader_features(db.data_path())?;
                    check(
                        !portable.iter().any(|f| {
                            f==crate::format::NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG
                        }),
                        "node-local completed receipt feature leaked into portable schema features",
                    )?;
                }
                drop(db);
                // The success clock includes the final full native-history
                // checks; an earlier diagnostic sample is not the receipt.
                parent.check()?;
                let receipt_elapsed = start.elapsed().as_nanos();
                check(
                    receipt_elapsed <= 20_000_000_000,
                    "full follower proof exceeded original convergence deadline",
                )?;
                let receipt = serde_json::json!({"required_actual_follower_R3_R4_convergence":true,
                    "separate_replica_clock_receipt_ns":receipt_elapsed,"first_public_20s_receipt_ns":first_public_receipt_ns,
                    "actual_complete_native_head":source_head,"all_materialized_command_indexes":commands.iter().map(|c|c.index).collect::<Vec<_>>(),
                    "actual":actual,"same_target_store_open":actual_reply_loss["actual_reply"]["store_open_id"],
                    "controller":origin_control.initial_data_status(&budget(50)).ok(),
                    "no_manual_tick_or_recovery":true,"not_fault_RTO_or_physical_qualification":true});
                journal.append(serde_json::json!({"kind":"required_actual_follower_convergence","receipt":receipt}))?;
                return Ok(receipt);
            }
            last = Some(actual);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    journal.append(serde_json::json!({"kind":"required_actual_follower_convergence_FAILED",
        "elapsed_ns":start.elapsed().as_nanos(),"first_public_receipt_ns":first_public_receipt_ns,"last":last,
        "leader_healthy_is_not_replica_repaired":true}))?;
    Err(BicDbError::Cluster("REQUIRED automatic same-follower Activate/open and native R3/R4 full-value convergence failed".into()))
}
