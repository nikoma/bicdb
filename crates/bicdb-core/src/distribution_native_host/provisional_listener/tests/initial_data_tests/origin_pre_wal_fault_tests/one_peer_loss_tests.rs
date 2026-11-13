//! REQUIRED one-unavailable-peer success witness. Current all-recipient HOT
//! orchestration is expected to FAIL its terminal gate; refusal is not success.
//! No producer, role, quorum, counter, reservation or deadline is replaced.
use super::*;
#[derive(Clone, Copy, Debug)]
enum Scenario {
    Stopped,
    Blackhole,
    SocketEof,
    LostFollowerReply,
    LostOriginReply,
    SemanticThenStopped,
}
impl Scenario {
    fn stops_peer(self) -> bool {
        matches!(
            self,
            Self::Stopped | Self::Blackhole | Self::SocketEof | Self::SemanticThenStopped
        )
    }
}

fn run_one_peer_loss(
    f: &Fixture,
    controls: &mut [NativeMetadataControlHost],
    supervisors: &mut [crate::distribution_native_host::NativeMetadataSupervisor],
    configs: &[DistributionConfig],
    journal: &Arc<Journal>,
    output: &Path,
    stage: &mut &'static str,
    scenario: Scenario,
) -> Result<()> {
    check(
        controls.len() == 3 && supervisors.len() == 3,
        "all three actual automatic owners required",
    )?;
    check(
        controls.iter().all(|c| c.test_hot_worker_started()),
        "all actual process workers must already be started",
    )?;
    let (origin_controls, peers) = controls.split_at_mut(1);
    let origin_control = &origin_controls[0];
    let mut held = Some(origin_control.test_hold_hot_controller_for_fault(&budget(1000))?);
    *stage = "controller_scheduling_held_before_old_ack_and_fault";
    journal.append(
        serde_json::json!({"kind":"actual_controller_scheduling_hold",
        "automatic_process_owners_already_started":3,"held_actual_existing_state_mutex":true,
        "no_job_mutation_or_range_reservation_or_authority":true}),
    )?;
    // Bootstrap remains the genuine public authenticated call. Its exact raw
    // frame is not covered by EventClient's observer, and is labelled as such.
    journal.append(
        serde_json::json!({"kind":"bootstrap_intent","destination":node(0),
        "address":configs[0].node_address,"bootstrap_raw_frame_covered":false}),
    )?;
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        configs[0].cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )?;
    let snapshot = bootstrap.fetch_snapshot(&node(0), &configs[0].node_address)?;
    journal.append(
        serde_json::json!({"kind":"bootstrap_reply","topology":snapshot.topology,
        "bootstrap_raw_frame_covered":false,"data_not_authority":true}),
    )?;
    let topology = snapshot.topology;
    let client = crate::ClusterEventClient::new(&configs[0], topology.clone())?
        .with_request_timeout(Duration::from_secs(2))?
        .with_rpc_evidence_observer(journal.clone(), crate::ClusterRpcEvidenceLimits::default())?;
    let range = topology
        .ranges
        .values()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let old = key_in(&topology, range.id, "origin-old-ack");
    let failed = key_in(&topology, range.id, "origin-injected-unknown");
    let next = key_in(&topology, range.id, "origin-next-ack");
    let old_value =
        serde_json::json!({"old":"entire external ACK","nested":[17,null,{"keep":true}]});
    let failed_value =
        serde_json::json!({"committed":"forced before origin WAL","nested":[null,{},[2,3]]});
    let next_value =
        serde_json::json!({"next":"requires autonomous native tail recovery","nested":[true,null]});
    *stage = "actual_old_ack_and_strong_with_controller_held";
    check(
        append(journal, &client, &old, &old_value)?,
        "first external ACK missing",
    )?;
    check(
        read(journal, &client, &old)?.as_ref() == Some(&old_value),
        "first exact Strong missing",
    )?;
    let database = &f.hosts[0].seed.database;
    let before = {
        let db = database.read();
        let lineage = db.current_native_range_source_lineage(range.id)?;
        serde_json::json!({"native_R":lineage.range_index(),"H0":lineage.logical_genesis_sha256(),
            "H":lineage.logical_history_sha256(),"typed_local_B":lineage.actual_local_visibility_sequence(),
            "prefix":db.native_range_prefix_observation(range.id)?})
    };
    let old_r = before["native_R"].as_u64().unwrap();
    check(old_r == 1, "required old native R1 cut was not established")?;
    let old_command = f.hosts[0]
        ._service
        .lock()
        .probe_range_write_for_peer(&range, &node(0), old_r)?
        .entry
        .ok_or_else(|| BicDbError::Cluster("actual old ACK command missing".into()))?
        .command;
    {
        let db = database.read();
        let lineage = db.current_native_range_source_lineage(range.id)?;
        db.require_native_command_materialized(&lineage, &old_command)?;
    }
    journal.append(
        serde_json::json!({"kind":"old_ack_native_command_correlation",
        "event_id":crate::cluster_event_record_id("items",&old)?,"full_payload":old_value,
        "actual_command":old_command,"native_before":before}),
    )?;
    *stage = "actual_origin_unknown_release_exit_with_controller_held";
    let arm: crate::db::NativeOriginFaultArm = database
        .read()
        .test_arm_native_origin_pre_wal_fault(range.id, "items", &failed)?;
    let probe: Arc<crate::db::NativeOriginFaultProbe> = arm.observation();
    journal.append(
        serde_json::json!({"kind":"fault_armed","actual_before":before,
        "probe":probe.snapshot(),"full_target_payload":failed_value}),
    )?;
    // The public client may retry. The observer retains every physical attempt;
    // later failures cannot overwrite the first actual origin receipt.
    let faulted_append = append(journal, &client, &failed, &failed_value);
    let end = Instant::now() + Duration::from_secs(6);
    let fault = loop {
        let snapshot = probe.snapshot();
        if snapshot["app_span_ended"] == true {
            break snapshot;
        }
        check(
            Instant::now() < end,
            "actual origin APP handler EXIT was not observed",
        )?;
        std::thread::sleep(Duration::from_millis(2));
    };
    journal.append(
        serde_json::json!({"kind":"actual_origin_exit","probe":fault,
        "api_error":faulted_append.as_ref().err().map(ToString::to_string),
        "actual_api_user_ack":faulted_append.is_ok(),"api_inserted":faulted_append.as_ref().ok(),
        "no_ack_inferred_from_probe":true}),
    )?;
    // The FIRST actual origin attempt must fail below. A legitimate automatic
    // repair plus a later SDK retry may nevertheless produce a real public ACK.
    // The immutable probe never overwrites that first failure or supplies ACK.
    check(
        fault["fired"] == true
            && fault["core_result"] == "commit_outcome_unknown"
            && fault["release_callback_returned"] == true
            && fault["release_callback_ok"] == true
            && fault["normal_app_handler_exit"] == true
            && fault["panicked"] == false
            && fault["diagnostic_integrity_error"] == false
            && fault["unknown_sequence"].is_null()
            && fault["actual_transaction_id"] == fault["unknown_transaction_id"],
        "actual fault/Unknown/release/normal EXIT prerequisites failed",
    )?;
    let entry = f.hosts[0]
        ._service
        .lock()
        .probe_range_write_for_peer(&range, &node(0), old_r + 1)?
        .entry
        .ok_or_else(|| BicDbError::Cluster("actual admitted command not retained".into()))?;
    journal.append(
        serde_json::json!({"kind":"actual_admitted_command_correlation",
        "event_id":crate::cluster_event_record_id("items",&failed)?,"full_payload":failed_value,
        "actual_entry":entry,"probe":fault}),
    )?;
    check(
        entry.command.command_id == fault["actual_ticket_command_id"].as_str().unwrap(),
        "actual ticket/command identity differs",
    )?;
    check(
        matches!(
            entry.state,
            crate::RangeWriteState::QuorumCommitted | crate::RangeWriteState::Applied
        ),
        "actual CQ was not retained",
    )?;
    let origin = database.read().native_range_prefix_observation(range.id)?;
    journal.append(
        serde_json::json!({"kind":"actual_origin_prefix_after_handler_exit","origin":origin}),
    )?;
    check(
        origin.native_applied_index == Some(old_r)
            && origin.pending_command_index == Some(old_r + 1)
            && origin.pending_command_id.as_deref() == Some(entry.command.command_id.as_str())
            && origin.materialized_pending_index.is_none()
            && !origin.invalidated,
        "scheduling-held cut requires actual origin R1/Pending2 before peer loss",
    )?;
    check(faulted_append.is_err(), "required pre-loss cut not established: real SDK ACK arrived while controller scheduling was held")?;
    *stage = "actual_origin_pending_and_two_peer_native_proofs";
    let mut physical = Vec::new();
    let mut materialized = 0;
    for (i, host) in f.hosts.iter().enumerate() {
        if node(i) == range.leader {
            continue;
        }
        // Collect BOTH actual peers before asserting. The DB guard ends before
        // any diagnostic fsync; a failed prerequisite is retained, not skipped.
        let actual = (|| -> Result<serde_json::Value> {
            let db = host.seed.database.read();
            let lineage = db.current_native_range_source_lineage(range.id)?;
            db.require_native_command_materialized(&lineage, &entry.command)?;
            db.require_native_command_materialized(&lineage, &old_command)?;
            let record = db
                .get(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", &failed)?,
                )?
                .ok_or_else(|| {
                    BicDbError::Cluster("actual peer materialized row missing".into())
                })?;
            let previous = db
                .get(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", &old)?,
                )?
                .ok_or_else(|| BicDbError::Cluster("actual old ACK peer row missing".into()))?;
            check(
                record.metadata == failed_value && previous.metadata == old_value,
                "actual peer full committed/old ACK payload differs",
            )?;
            Ok(serde_json::json!({"native_R":lineage.range_index(),
                "H0":lineage.logical_genesis_sha256(),"H":lineage.logical_history_sha256(),
                "typed_local_B":lineage.actual_local_visibility_sequence(),
                "full_record":record,"old_ack_full_record":previous}))
        })();
        match actual {
            Ok(facts) => {materialized += 1;physical.push(serde_json::json!({"node":node(i),"verified_actual_native":facts}));}
            Err(error) => physical.push(serde_json::json!({"node":node(i),"actual_materialization_error":error.to_string()})),
        }
    }
    journal.append(serde_json::json!({"kind":"actual_native_cut","origin":origin,
        "actual_command":entry.command,"raw_state":entry.state,"peer_native_materialization":physical,
        "native_proof_not_inferred_from_raw_counters":true}))?;
    check(
        materialized == 2,
        "two actual peer native materialization prerequisites failed",
    )?;
    check(
        physical
            .iter()
            .all(|p| p["verified_actual_native"]["native_R"] == 2)
            && physical[0]["verified_actual_native"]["H0"] == before["H0"]
            && physical[1]["verified_actual_native"]["H0"] == before["H0"]
            && physical[0]["verified_actual_native"]["H"]
                == physical[1]["verified_actual_native"]["H"],
        "both actual peers must prove the same exact native R2 history before losing one",
    )?;

    // This common success clock includes peer stop/drain and the surviving
    // fresh metadata proof. It is never restarted after those prerequisites.
    *stage = "peer_stop_and_surviving_quorum_under_common_health_clock";
    let health_start = Instant::now();
    let health_end = health_start + Duration::from_secs(20);
    let health_parent = CancellationToken::uncancelable().child_with_deadline(health_end);
    journal.append(
        serde_json::json!({"kind":"health_gate_start_before_peer_stop",
        "health_limit_ns":20_000_000_000u64,"lost_peer":scenario.stops_peer().then(||node(1)),"scenario":format!("{scenario:?}"),
        "original_controller_hold_still_owned":true,"no_clock_restart_after_stop_or_mq":true}),
    )?;
    if matches!(scenario, Scenario::SemanticThenStopped) {
        // Genuine administrative revocation; keep this real receiver reachable
        // until the automatic worker observes its authenticated refusal.
        let original = peers[0].test_hot_recovery_bindings()?.1;
        let before = original.binding_diagnostic();
        f.hosts[1].seed.database.write().clear_commit_admission();
        journal.append(serde_json::json!({"kind":"actual_peer_hook_revocation",
            "node":node(1),"before":before,"after":original.binding_diagnostic(),
            "no_new_writer_or_permission":true,"health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
        drop(held.take());
        journal.append(serde_json::json!({"kind":"actual_controller_hold_released_for_semantic_negative",
            "only_mutex_unlock_no_authority":true,"health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
        let semantic_deadline = health_end.min(Instant::now() + Duration::from_secs(2));
        loop {
            health_parent.check()?;
            let status = origin_control
                .initial_data_status(&health_parent)?
                .ok_or_else(|| BicDbError::Cluster("actual origin status ended".into()))?;
            if status
                .hot_recovery_last_attempt
                .as_ref()
                .is_some_and(|d| d["semantic_veto"]["n1"] == "Fence")
            {
                journal.append(serde_json::json!({"kind":"actual_authenticated_semantic_veto_before_peer_loss",
                    "actual":status.hot_recovery_last_attempt,"health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
                break;
            }
            check(
                Instant::now() < semantic_deadline,
                "actual semantic refusal never reached retained job veto",
            )?;
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    let loss_runtime = match scenario {
        Scenario::LostFollowerReply => Some(peers[1].test_hot_recovery_bindings()?.2),
        Scenario::LostOriginReply => Some(origin_control.test_hot_recovery_bindings()?.2),
        _ => None,
    };
    let reply_loss = loss_runtime
        .as_ref()
        .map(|r| r.test_arm_hot_completion_reply_loss(range.id, entry.command.index))
        .transpose()?;
    let mut socket_fault = None;
    if scenario.stops_peer() {
        let stop_supervisor = supervisors[1].stop(
            &health_parent
                .child_with_deadline(health_end.min(Instant::now() + Duration::from_secs(5))),
        );
        journal.append(serde_json::json!({"kind":"actual_peer_supervisor_stop",
        "node":node(1),"ok":stop_supervisor.is_ok(),"error":stop_supervisor.as_ref().err().map(ToString::to_string),
        "health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
        // Attempt the actual listener stop even if supervisor drain failed. Both
        // results are retained before any precondition assertion.
        let stop_listener = peers[0].stop(
            &health_parent
                .child_with_deadline(health_end.min(Instant::now() + Duration::from_secs(5))),
        );
        journal.append(serde_json::json!({"kind":"actual_peer_data_metadata_listener_stop",
        "node":node(1),"ok":stop_listener.is_ok(),"error":stop_listener.as_ref().err().map(ToString::to_string),
        "health_elapsed_ns":health_start.elapsed().as_nanos(),"engine_files_not_removed":true}))?;
        stop_supervisor?;
        stop_listener?;
        let stopped = peers[0]
            .initial_data_status(&health_parent)?
            .ok_or_else(|| BicDbError::Cluster("actual stopped DATA owner disappeared".into()))?;
        journal.append(
            serde_json::json!({"kind":"actual_stopped_peer_status_diagnostic",
        "node":node(1),"status":stopped,"diagnostic_not_authority":true}),
        )?;
        check(
            !stopped.receiver_listening && !stopped.dispatcher_published,
            "actual peer listener did not drain/unpublish",
        )?;
        health_parent.check()?;
        let peer_address: std::net::SocketAddr = configs[1]
            .node_address
            .parse()
            .map_err(|e| BicDbError::Cluster(format!("actual peer address invalid: {e}")))?;
        journal.append(serde_json::json!({"kind":"peer_connect_probe_intent",
        "node":node(1),"address":peer_address,"purpose":"no authority; verify stopped listener unavailable"}))?;
        health_parent.check()?;
        let connect_budget = health_end
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(100));
        check(
            !connect_budget.is_zero(),
            "original health budget expired before peer probe",
        )?;
        let peer_connect = std::net::TcpStream::connect_timeout(&peer_address, connect_budget);
        let refused = peer_connect
            .as_ref()
            .err()
            .is_some_and(|e| e.kind() == io::ErrorKind::ConnectionRefused);
        journal.append(serde_json::json!({"kind":"peer_connect_probe_result",
        "node":node(1),"connected":peer_connect.is_ok(),"connection_refused":refused,
        "error_kind":peer_connect.as_ref().err().map(|e|format!("{:?}",e.kind())),
        "error":peer_connect.as_ref().err().map(ToString::to_string),
        "health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
        health_parent.check()?;
        check(refused,"actual stopped peer must refuse the connection; timeout or budget expiry is not the loss witness")?;

        if matches!(scenario, Scenario::Blackhole | Scenario::SocketEof) {
            socket_fault = Some(
                super::super::hot_recovery_tests::peer_transport_tests::SocketFault::bind(
                    &configs[1].node_address,
                    matches!(scenario, Scenario::SocketEof),
                ),
            );
            journal.append(serde_json::json!({"kind":"actual_stopped_peer_socket_fault",
                "scenario":format!("{scenario:?}"),"address":configs[1].node_address,
                "no_native_metadata_or_tls_authority":true,"health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
        }
    }
    // Current leader IDs are routing DATA. Only the following genuine fresh
    // sealed HostMQ proves the surviving cohort can still serve metadata.
    let hint = origin_control.metadata_status(&health_parent)?;
    let route = match hint.leader_id {
        Some(node) => node,
        None => peers[1]
            .metadata_status(&health_parent)?
            .leader_id
            .ok_or_else(|| {
                BicDbError::Cluster("surviving actual metadata leader unknown".into())
            })?,
    };
    check(
        route != node(1),
        "surviving metadata hint still routes only to stopped peer",
    )?;
    let metadata_transport = crate::TcpClusterRelocationTransport::new(
        configs[0].cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(topology.clone())),
        f.networks[0].clone(),
    )?
    .with_rpc_evidence_observer(journal.clone(), crate::ClusterRpcEvidenceLimits::default())?;
    let diagnostic_business = journal.start("surviving_host_metadata_qq", &failed, None)?;
    let business = Uuid::now_v7();
    let mq_started = Instant::now();
    let mq = metadata_transport.acquire_host_metadata_observation(
        &route,
        business,
        health_end
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(2)),
        &health_parent,
    );
    let mq_outcome = match &mq {
        Ok(observation) => match observation.committed_topology(business) {
            Ok(current) => serde_json::json!({"classification":"ACTUAL_FRESH_HOST_METADATA_QQ",
                "actual_business_id":business,"actual_context_id":observation.context_id(),"term":observation.term(),
                "read_index":observation.read_index(),"requester":observation.requester_node_id(),
                "requester_incarnation":observation.requester_incarnation(),"route":route,
                "actual_committed_topology":current,"fresh_responders_not_fabricated_from_topology":true}),
            Err(error) => {
                serde_json::json!({"classification":"ACTUAL_TRANSPORT_QQ_RETURNED_CURRENT_PROJECTION_REFUSED",
                "actual_business_id":business,"actual_context_id":observation.context_id(),"route":route,"error":error.to_string()})
            }
        },
        Err(error) => {
            serde_json::json!({"classification":"NO_SUCCESSFUL_HOST_METADATA_QQ","error":error.to_string(),"route":route})
        }
    };
    journal.finish(
        diagnostic_business,
        mq_outcome,
        mq_started.elapsed().as_nanos(),
    )?;
    let mq = mq?;
    mq.require_live_request(business)?;
    check(
        mq.committed_topology(business)? == &topology,
        "actual current membership/range changed during fault setup",
    )?;
    health_parent.check()?;
    let before_release = database.read().native_range_prefix_observation(range.id)?;
    check(
        before_release == origin,
        "origin progressed while actual controller serialization was held",
    )?;
    journal.append(serde_json::json!({"kind":"loss_and_surviving_mq_prerequisites_before_hold_release",
        "origin":before_release,"stopped_peer":scenario.stops_peer().then(||node(1)),"scenario":format!("{scenario:?}"),"actual_metadata_context":mq.context_id(),
        "health_elapsed_ns":health_start.elapsed().as_nanos(),"no_recovery_tick_or_mutating_protocol_invoked_by_fixture":true}))?;
    drop(held.take());
    journal.append(serde_json::json!({"kind":"actual_controller_scheduling_hold_released",
        "only_mutex_unlock_no_authority_refresh":true,"already_released_for_semantic_negative":matches!(scenario,Scenario::SemanticThenStopped),"health_elapsed_ns":health_start.elapsed().as_nanos()}))?;
    *stage = "required_autonomous_health_gate_with_peer_unavailable";
    let mut healthy = false;
    let mut ever_next_ack = false;
    let mut rounds = 0;
    let mut last_receipt = None;
    while Instant::now() < health_end {
        rounds += 1;
        // All four actual calls occur before checking outcomes. No manual
        // registry/metadata tick, direct recovery or readiness setter exists.
        let old_read = read(journal, &client, &old);
        let committed_read = read(journal, &client, &failed);
        let next_ack = append(journal, &client, &next, &next_value);
        ever_next_ack |= next_ack.is_ok();
        let next_read = read(journal, &client, &next);
        let receipt = health_start.elapsed().as_nanos();
        last_receipt = Some(receipt);
        journal.append(serde_json::json!({"kind":"health_round","round":rounds,
            "receipt_elapsed_ns":receipt,"health_limit_ns":20_000_000_000u64,
            "peer_stop_and_surviving_mq_included":true,
            "old_exact":old_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&old_value)),
            "committed_exact":committed_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&failed_value)),
            "next_user_ack":next_ack.is_ok(),"ever_next_user_ack":ever_next_ack,
            "next_exact":next_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&next_value))}))?;
        check(
            no_false_successful_value(&old_read, &old_value)
                && no_false_successful_value(&committed_read, &failed_value),
            "Strong returned false absence/wrong actual acknowledged or committed value",
        )?;
        check(
            !ever_next_ack || no_false_successful_value(&next_read, &next_value),
            "Strong returned false absence/wrong value after any actual next-event ACK",
        )?;
        healthy = receipt <= 20_000_000_000
            && old_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&old_value))
            && committed_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&failed_value))
            && next_ack.is_ok()
            && next_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&next_value));
        if healthy {
            break;
        }
        if health_end.saturating_duration_since(Instant::now()) < Duration::from_secs(8) {
            break;
        }
    }
    // Post-gate observations are diagnostics, never an extension of the
    // already-measured success receipt or a substitute for a user ACK.
    let final_prefix = (|| -> Result<serde_json::Value> {
        let db = database
            .try_read()
            .ok_or_else(|| BicDbError::Cluster("post-gate actual DB/callback busy".into()))?;
        Ok(serde_json::to_value(
            db.native_range_prefix_observation(range.id)?,
        )?)
    })();
    let final_native = (|| -> Result<serde_json::Value> {
        let db = database
            .try_read()
            .ok_or_else(|| BicDbError::Cluster("post-gate actual DB/callback busy".into()))?;
        let lineage = db.current_native_range_source_lineage(range.id)?;
        db.require_native_command_materialized(&lineage, &entry.command)?;
        db.require_native_command_materialized(&lineage, &old_command)?;
        let old_row = db.get(
            crate::CLUSTER_EVENTS_COLLECTION,
            &crate::cluster_event_record_id("items", &old)?,
        )?;
        let committed_row = db.get(
            crate::CLUSTER_EVENTS_COLLECTION,
            &crate::cluster_event_record_id("items", &failed)?,
        )?;
        let next_row = db.get(
            crate::CLUSTER_EVENTS_COLLECTION,
            &crate::cluster_event_record_id("items", &next)?,
        )?;
        Ok(
            serde_json::json!({"native_R":lineage.range_index(),"H0":lineage.logical_genesis_sha256(),
            "H":lineage.logical_history_sha256(),"typed_local_B":lineage.actual_local_visibility_sequence(),
            "actual_old_full_record":old_row,"actual_committed_full_record":committed_row,"actual_next_full_record":next_row}),
        )
    })();
    journal.append(serde_json::json!({"kind":"actual_post_gate_origin_state","prefix":final_prefix.as_ref().ok(),
        "prefix_error":final_prefix.as_ref().err().map(ToString::to_string),
        "actual_native":final_native.as_ref().ok(),"actual_native_error":final_native.as_ref().err().map(ToString::to_string),
        "origin_controller_diagnostic":origin_control.initial_data_status(&budget(500)).ok().flatten(),
        "after_gate_diagnostic_not_a_new_grant":true}))?;
    let reply_loss_snapshot = reply_loss.as_ref().map(|r| r.snapshot());
    if let Some(snapshot) = &reply_loss_snapshot {
        journal.append(serde_json::json!({"kind":"actual_post_producer_reply_loss",
            "scenario":format!("{scenario:?}"),"actual":snapshot,
            "no_ack_inferred_from_reply_loss":true}))?;
        check(
            snapshot["suppressed_after_actual_producer"] == true,
            "required actual durable producer reply-loss boundary never fired",
        )?;
    }
    // Socket owner is retained through all receipts; dropping it only ends the
    // test-owned nonblocking accept worker, never starts native work.
    drop(socket_fault);
    let terminal = serde_json::json!({"scenario":format!("{scenario:?}"),"acceptance_contract":if matches!(scenario,Scenario::SemanticThenStopped){"security-negative: retained actual semantic veto; NOT healthy-repair qualification"}else{"REQUIRED actual healthy repair and next ACK/full Strong"},"actual_reply_loss":reply_loss_snapshot,"required_one_peer_loss_repair_and_next_ack":healthy,
        "rounds":rounds,"ever_next_user_ack":ever_next_ack,
        "first_actual_origin_error":fault,"first_origin_unknown_is_not_rollback":true,
        "fault_api_user_ack":faulted_append.is_ok(),"terminal_elapsed_ns":health_start.elapsed().as_nanos(),
        "last_health_receipt_elapsed_ns":last_receipt,"health_success_receipt_elapsed_ns":if healthy {last_receipt}else{None},
        "health_limit_ns":20_000_000_000u64,"health_clock_started_before_peer_stop":true,
        "no_manual_maintenance_fallback":true,"no_genesis_or_counter_reset":true,
        "selected_peer_unavailable":scenario.stops_peer().then(||node(1)),"origin_final_prefix":final_prefix.as_ref().ok(),
        "origin_final_prefix_error":final_prefix.as_ref().err().map(ToString::to_string),
        "portable_native_db_custody":false,"physical_qualification":false});
    let terminal_write = serde_json::to_vec_pretty(&terminal)
        .map_err(BicDbError::from)
        .and_then(|bytes| {
            crate::storage::write_atomic(&output.join("terminal.json"), &bytes, true)
        });
    let terminal_journal = journal
        .append(serde_json::json!({"kind":"required_one_peer_loss_terminal","terminal":terminal}));
    if matches!(scenario, Scenario::SemanticThenStopped) {
        check(
            !healthy && !ever_next_ack,
            "known authenticated semantic ambiguity was silently skipped after peer loss",
        )?;
        let status = origin_control
            .initial_data_status(&budget(1000))?
            .ok_or_else(|| BicDbError::Cluster("actual origin status ended".into()))?;
        check(
            status
                .hot_recovery_last_attempt
                .as_ref()
                .is_some_and(|d| d["semantic_veto"]["n1"] == "Fence" && d["phase"] == "Fence"),
            "actual prior semantic veto did not survive peer unreachability",
        )?;
        check(
            database.read().native_range_prefix_observation(range.id)? == origin,
            "semantic-veto refusal changed actual native point/Pending",
        )?;
        terminal_write?;
        terminal_journal?;
        return Ok(());
    }
    if !healthy {
        // Preserve the required acceptance result even if supplemental
        // terminal persistence failed. Raw per-operation ledger failures are
        // still explicit and cannot qualify an incomplete witness as success.
        eprintln!(
            "NATIVE_HOT_ONE_PEER_LOSS_REQUIRED_FAILURE terminal_error={:?} journal_error={:?}",
            terminal_write.as_ref().err(),
            terminal_journal.as_ref().err()
        );
        return Err(BicDbError::Cluster("REQUIRED one-peer-unavailable autonomous committed-tail repair + next ACK/full Strong not achieved".into()));
    }
    terminal_write?;
    terminal_journal?;
    Ok(())
}

fn run_case(scenario: Scenario) {
    let (mut f, mut controls, configs) = actual_initial_metadata_controls();
    f._root.disable_cleanup(true);
    let output = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| f._root.path().join("public-diagnostics"))
        .join(format!("native-hot-one-peer-loss-{}", Uuid::now_v7()));
    let journal = Arc::new(Journal::create(&output.join("journal.jsonl")).unwrap());
    let exe = std::env::current_exe().unwrap();
    journal.append(serde_json::json!({"kind":"execution_identity",
        "source":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"test_executable_sha256":sha_file(&exe).unwrap(),
        "test_executable_bytes":std::fs::metadata(exe).unwrap().len(),
        "component":"three actual NativeHosts/RF3/TLS in one Core process; not CLI or VMs","scenario":format!("{scenario:?}"),
        "private_original_root":f._root.path(),"portable_native_db_custody":false,
        "no_direct_fixture_recovery_or_reopen":true})).unwrap();
    let process_parent = CancellationToken::uncancelable();
    let mut supervisors = Vec::new();
    let mut stage = "actual_initial_data_and_process_owner_setup";
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
        open_data(&mut controls)?;
        for control in &controls {
            supervisors.push(control.start_automatic_metadata(&process_parent)?);
        }
        run_one_peer_loss(
            &f,
            &mut controls,
            &mut supervisors,
            &configs,
            &journal,
            &output,
            &mut stage,
            scenario,
        )
    }));
    let primary_error = match &result {
        Ok(Err(error)) => Some(error.to_string()),
        Err(panic) => Some(
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string actual test panic".into()),
        ),
        _ => None,
    };
    let mut cleanup_errors = Vec::new();
    for supervisor in &mut supervisors {
        if let Err(error) = supervisor.stop(&budget(5000)) {
            cleanup_errors.push(error.to_string());
        }
    }
    for control in &mut controls {
        if let Err(error) = control.stop(&budget(5000)) {
            cleanup_errors.push(error.to_string());
        }
    }
    let final_report = serde_json::json!({"kind":"test_terminal_after_owned_stop_attempts",
        "stage":stage,"primary_error":primary_error,"cleanup_errors":cleanup_errors,
        "original_root_retained_path_only":f._root.path(),"portable_native_db_custody":false,
        "no_direct_fixture_recovery_or_reopen":true,"no_manual_maintenance_fallback":true});
    let journal_error = journal
        .append(final_report.clone())
        .err()
        .map(|e| e.to_string());
    let report_write = serde_json::to_vec_pretty(&final_report)
        .map_err(BicDbError::from)
        .and_then(|bytes| crate::storage::write_atomic(&output.join("cleanup.json"), &bytes, true));
    eprintln!(
        "NATIVE_HOT_ONE_PEER_LOSS_EVIDENCE {} {} journal_error={:?} report_error={:?}",
        output.display(),
        final_report,
        journal_error,
        report_write.as_ref().err()
    );
    // Evidence/cleanup errors cannot replace the first actual acceptance or
    // prerequisite error. A healthy case additionally requires clean retained evidence.
    match result {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => panic!("actual one-peer-loss witness failed at {stage}: {error}"),
        Ok(Ok(())) => {
            assert!(
                cleanup_errors.is_empty(),
                "actual cleanup errors: {cleanup_errors:?}"
            );
            assert!(
                journal_error.is_none(),
                "actual complete journal failure: {journal_error:?}"
            );
            report_write.unwrap();
        }
    }
}

#[test]
fn native_hot_one_peer_loss_actual_rpc_requires_autonomous_tail_recovery() {
    run_case(Scenario::Stopped);
}
#[test]
fn native_hot_peer_blackhole_actual_rpc_requires_autonomous_tail_recovery() {
    run_case(Scenario::Blackhole);
}
#[test]
fn native_hot_peer_socket_eof_actual_rpc_requires_autonomous_tail_recovery() {
    run_case(Scenario::SocketEof);
}
#[test]
fn native_hot_peer_lost_actual_follower_completion_reply_requires_fresh_recovery() {
    run_case(Scenario::LostFollowerReply);
}
#[test]
fn native_hot_peer_lost_actual_origin_completion_reply_requires_fresh_reattest() {
    run_case(Scenario::LostOriginReply);
}

#[test]
fn native_hot_peer_actual_semantic_rejection_then_socket_loss_retains_veto() {
    run_case(Scenario::SemanticThenStopped);
}
