//! REQUIRED repeat-fault success, never pass on safe refusal/queued debt.
//! No new factory, manual maintenance, epoch/role setter or data authority.
use super::*;
type OriginalWriter = (
    Arc<
        crate::distribution_range_consensus::RangeWriteCoordinator<
            crate::TcpClusterRelocationTransport,
        >,
    >,
    Arc<crate::db::InstalledNativeWriterHostGuard>,
);
fn actual_all(
    f: &Fixture,
    originals: &[OriginalWriter],
    range: &crate::RangeDescriptor,
    expected: u64,
    keys: &[&str],
) -> Result<serde_json::Value> {
    check(
        originals.len() == 3,
        "all three original writer/guard pairs required",
    )?;
    let mut rows = Vec::new();
    for (i, (coordinator, guard)) in originals.iter().enumerate() {
        let open = {
            let service = f.hosts[i]
                ._service
                .try_lock()
                .ok_or_else(|| BicDbError::Cluster("actual repeat target service busy".into()))?;
            service.test_native_replica_open_state(range.id)?
        };
        let commands = {
            let service = f.hosts[0]
                ._service
                .try_lock()
                .ok_or_else(|| BicDbError::Cluster("actual repeat source service busy".into()))?;
            (1..=expected)
                .map(|index| {
                    service
                        .probe_range_write_for_peer(range, &node(0), index)?
                        .entry
                        .map(|e| e.command)
                        .ok_or_else(|| {
                            BicDbError::Cluster(
                                "actual repeat original command body not retained".into(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?
        };
        let db = f.hosts[i]
            .seed
            .database
            .try_read()
            .ok_or_else(|| BicDbError::Cluster("actual repeat DB busy".into()))?;
        guard.require_for_coordinator(&db, range, coordinator)?;
        let source = db.current_native_range_source_lineage(range.id)?;
        check(
            source.range_index() == expected,
            "required exact actual native R not established on all replicas",
        )?;
        check(
            open["phase"] == "NativeActivatedAnchor" && open["actual_current_open_valid"] == true,
            "actual original replica has not activated/opened",
        )?;
        for command in &commands {
            db.require_native_command_materialized(&source, command)?;
        }
        let mut records = std::collections::BTreeMap::new();
        for key in keys {
            let actual = db.get(
                crate::CLUSTER_EVENTS_COLLECTION,
                &crate::cluster_event_record_id("items", key)?,
            )?;
            check(
                actual.is_some(),
                "required previously ACKed full record disappeared from actual replica",
            )?;
            records.insert((*key).to_owned(), actual);
        }
        rows.push(serde_json::json!({"node":node(i),"native_R":source.range_index(),"H0":source.logical_genesis_sha256(),
            "H":source.logical_history_sha256(),"actual_local_B":source.actual_local_visibility_sequence(),
            "database_open_incarnation":db.incarnation_id(),"actual_current_open":open,"records":records,
            "original_coordinator":coordinator.native_writer_coordinator_id(),"actual_original_guard":guard.binding_diagnostic(),
            "all_exact_commands_physically_materialized":expected}));
    }
    check(
        rows.iter().all(|r| {
            r["H0"] == rows[0]["H0"] && r["H"] == rows[0]["H"] && r["records"] == rows[0]["records"]
        }),
        "actual repeat native histories/full records differ",
    )?;
    Ok(serde_json::json!(rows))
}
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    f: &Fixture,
    control: &NativeMetadataControlHost,
    journal: &Arc<Journal>,
    client: &crate::ClusterEventClient,
    topology: &ClusterTopology,
    range: &crate::RangeDescriptor,
    old: &str,
    old_value: &serde_json::Value,
    originals: &[OriginalWriter],
    first_convergence: &serde_json::Value,
) -> Result<serde_json::Value> {
    let fifth = key_in(topology, range.id, "same-origin-second-unknown5");
    let sixth = key_in(topology, range.id, "same-origin-second-next-ack6");
    let fifth_value =
        serde_json::json!({"second_unknown":5,"full":[null,{},[51,52]],"nested":{"preserve":true}});
    let sixth_value = serde_json::json!({"second_next_ACK":6,"full":[{},null,[61,62]],"nested":{"preserve":true}});
    let fourth = key_in(topology, range.id, "origin-after-availability-ack");
    let prior_unknown = key_in(topology, range.id, "origin-injected-unknown");
    let prior_next = key_in(topology, range.id, "origin-next-ack");
    let first_keys = [
        old,
        prior_unknown.as_str(),
        prior_next.as_str(),
        fourth.as_str(),
    ];
    // Every real ACK consumes a native command slot, including a same-value
    // retry acknowledged in a later health round (API inserted=false). The
    // actual origin head after the first complete convergence is therefore
    // >= 4 and is read, never assumed; every later index is relative to it.
    let base = {
        let db = f.hosts[0].seed.database.read();
        db.current_native_range_source_lineage(range.id)?
            .range_index()
    };
    check(
        base >= 4,
        "first recovery did not retain at least the four acknowledged native commands",
    )?;
    // The diagnostic snapshot uses non-blocking latches on live owners whose
    // process worker keeps tracking followers. A momentarily busy latch is
    // not an observation: retry only that, for a bounded time. Every actual
    // native/history/payload requirement of the snapshot is unchanged.
    let original_all4 = {
        let busy_until = Instant::now() + Duration::from_secs(2);
        loop {
            match actual_all(f, originals, range, base, &first_keys) {
                Err(BicDbError::Cluster(message))
                    if message.ends_with(" busy") && Instant::now() < busy_until =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                other => break other?,
            }
        }
    };
    journal.append(
        serde_json::json!({"kind":"second_fault_requires_first_complete_all_native4",
        "actual_all":original_all4,"first_convergence_receipt":first_convergence,
        "same_actual_original_guards_checked":true,"no_new_owner_or_genesis":true}),
    )?;
    // Scheduling only, acquired after actual first physical convergence. The
    // first callback/APP handler EXIT must be real, not a sender timeout.
    let held = control.test_hold_hot_controller_for_fault(&budget(1000))?;
    let db = f.hosts[0].seed.database.clone();
    let arm = db
        .read()
        .test_arm_native_origin_pre_wal_fault(range.id, "items", &fifth)?;
    let probe = arm.observation();
    journal.append(
        serde_json::json!({"kind":"second_actual_origin_fault_armed",
        "expected_previous_native_R":base,"full_fifth":fifth_value,"probe":probe.snapshot()}),
    )?;
    let api = append(journal, client, &fifth, &fifth_value);
    let wait_end = Instant::now() + Duration::from_secs(6);
    let fault = loop {
        let actual = probe.snapshot();
        if actual["app_span_ended"] == true {
            break actual;
        }
        check(
            Instant::now() < wait_end,
            "second actual normal APP handler did not exit",
        )?;
        std::thread::sleep(Duration::from_millis(2));
    };
    journal.append(serde_json::json!({"kind":"second_actual_origin_unknown_and_handler_EXIT",
        "actual_probe":fault,"actual_API_ACK":api.is_ok(),"API_error":api.as_ref().err().map(ToString::to_string),
        "no_ACK_inferred_from_probe":true}))?;
    check(
        api.is_err()
            && fault["fired"] == true
            && fault["core_result"] == "commit_outcome_unknown"
            && fault["unknown_sequence"].is_null()
            && fault["actual_transaction_id"] == fault["unknown_transaction_id"]
            && fault["release_callback_returned"] == true
            && fault["release_callback_ok"] == true
            && fault["normal_app_handler_exit"] == true
            && fault["panicked"] == false
            && fault["diagnostic_integrity_error"] == false,
        "second original Unknown/sequenceNone/actual release/normal EXIT prerequisite failed",
    )?;
    let command = f.hosts[0]
        ._service
        .lock()
        .probe_range_write_for_peer(range, &node(0), base + 1)?
        .entry
        .ok_or_else(|| BicDbError::Cluster("actual second CQ5 command missing".into()))?;
    check(
        command.command.command_id == fault["actual_ticket_command_id"].as_str().unwrap()
            && matches!(
                command.state,
                crate::RangeWriteState::QuorumCommitted | crate::RangeWriteState::Applied
            ),
        "second actual ticket/CQ command mismatch",
    )?;
    let prefix = db.read().native_range_prefix_observation(range.id)?;
    check(
        prefix.native_applied_index == Some(base)
            && prefix.pending_command_index == Some(base + 1)
            && prefix.pending_command_id.as_deref() == Some(command.command.command_id.as_str())
            && prefix.materialized_pending_index.is_none()
            && !prefix.invalidated,
        "second actual origin native4/Pending5 cut failed",
    )?;
    let mut actual_peers = Vec::new();
    for i in [1, 2] {
        let db = f.hosts[i].seed.database.read();
        originals[i]
            .1
            .require_for_coordinator(&db, range, &originals[i].0)?;
        let source = db.current_native_range_source_lineage(range.id)?;
        db.require_native_command_materialized(&source, &command.command)?;
        let full = db
            .get(
                crate::CLUSTER_EVENTS_COLLECTION,
                &crate::cluster_event_record_id("items", &fifth)?,
            )?
            .ok_or_else(|| {
                BicDbError::Cluster("actual peer second committed full record missing".into())
            })?;
        check(
            source.range_index() == base + 1 && full.metadata == fifth_value,
            "actual peer native5/full fifth mismatch",
        )?;
        actual_peers.push(serde_json::json!({"node":node(i),"native_R":source.range_index(),"H0":source.logical_genesis_sha256(),"H":source.logical_history_sha256(),"full_record":full}));
    }
    check(
        actual_peers[0]["H"] == actual_peers[1]["H"],
        "actual peer second histories conflict",
    )?;
    journal.append(
        serde_json::json!({"kind":"second_actual_origin4_pending5_peers_native5_before_release",
        "actual_origin":prefix,"actual_command":command,"actual_peers":actual_peers,
        "all_unknown_outcomes_remain_immutable":true}),
    )?;
    let start = Instant::now();
    let end = start + Duration::from_secs(20);
    let parent = CancellationToken::uncancelable().child_with_deadline(end);
    journal.append(
        serde_json::json!({"kind":"second_common_20s_gate_before_schedule_release",
        "no_manual_tick_or_recovery":true,"original_client_setting_ms":2000,"phase_limit_ms":5000}),
    )?;
    drop(held);
    let mut ever_ack = false;
    let mut round = 0;
    while Instant::now() < end {
        round += 1;
        let old_read = read(journal, client, old);
        let committed = read(journal, client, &fifth);
        let next = append(journal, client, &sixth, &sixth_value);
        ever_ack |= next.is_ok();
        let next_read = read(journal, client, &sixth);
        let elapsed = start.elapsed().as_nanos();
        journal.append(serde_json::json!({"kind":"second_origin_required_health_round","round":round,
            "receipt_elapsed_ns":elapsed,"old_exact":old_read.as_ref().is_ok_and(|v|v.as_ref()==Some(old_value)),
            "committed5_exact":committed.as_ref().is_ok_and(|v|v.as_ref()==Some(&fifth_value)),
            "actual_next_API_ACK":next.is_ok(),"ever_next_API_ACK":ever_ack,
            "next6_exact":next_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&sixth_value)),
            "controller_diagnostic":control.initial_data_status(&budget(50)).ok().flatten()}))?;
        check(no_false_successful_value(&old_read,old_value) && no_false_successful_value(&committed,&fifth_value)
            && (!ever_ack || no_false_successful_value(&next_read,&sixth_value)),"second recovery returned successful false absence/wrong value after actual ACK/decision")?;
        let healthy = elapsed <= 20_000_000_000
            && old_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(old_value))
            && committed
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&fifth_value))
            && next.is_ok()
            && next_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&sixth_value));
        if healthy {
            while Instant::now() < end {
                // A same-value sixth ACK repeated by a later health round
                // also consumes a slot: require the actual origin head, which
                // must cover at least the second Unknown and its next ACK.
                let final_all = f.hosts[0]
                    .seed
                    .database
                    .try_read()
                    .ok_or_else(|| BicDbError::Cluster("actual repeat origin DB busy".into()))
                    .and_then(|db| {
                        Ok(db
                            .current_native_range_source_lineage(range.id)?
                            .range_index())
                    })
                    .and_then(|head| {
                        check(
                            head >= base + 2,
                            "actual origin head does not cover second Unknown and next ACK",
                        )?;
                        actual_all(
                            f,
                            originals,
                            range,
                            head,
                            &[old, fifth.as_str(), sixth.as_str()],
                        )
                    });
                journal.append(serde_json::json!({"kind":"second_actual_all_replica6_sample",
                    "elapsed_ns":start.elapsed().as_nanos(),"actual_all":final_all.as_ref().ok(),"error":final_all.as_ref().err().map(ToString::to_string)}))?;
                if let Ok(all) = final_all {
                    parent.check()?;
                    let final_receipt_ns = start.elapsed().as_nanos();
                    check(
                        final_receipt_ns <= 20_000_000_000,
                        "second full replica proof exceeded original20s receipt gate",
                    )?;
                    let receipt = serde_json::json!({"required_second_unknown5_then_ACK6_all_native6":true,
                        "actual_second_Unknown":fault,"second_health_receipt_ns":elapsed,"all_replicas_receipt_ns":final_receipt_ns,
                        "actual_all":all,"original_all4":original_all4,"no_manual_recovery":true,
                        "first_Unknown_and_noACK_not_reclassified":true});
                    journal.append(serde_json::json!({"kind":"required_repeat_origin_success","receipt":receipt}))?;
                    return Ok(receipt);
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            break;
        }
        if end.saturating_duration_since(Instant::now()) < Duration::from_secs(8) {
            break;
        }
    }
    journal.append(serde_json::json!({"kind":"required_repeat_origin_FAILED","elapsed_ns":start.elapsed().as_nanos(),
        "actual_second_Unknown":fault,"ever_next_API_ACK":ever_ack,"controller":control.initial_data_status(&budget(100)).ok().flatten(),
        "safe_refusal_is_NOT_self_healing_success":true}))?;
    Err(BicDbError::Cluster("REQUIRED second actual origin Unknown5 through SAME original owners to ACK6/full Strong/all native6 failed".into()))
}
