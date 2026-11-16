//! One genuine CQ/pre-WAL receiver gap, a public ACK, the SAME stock gateway,
//! actual same-voter transfer and historical recovery. Source-only until run.
use super::*;
use sha2::{Digest, Sha256};

fn digest(value: &impl serde::Serialize) -> String {
    hex::encode(Sha256::digest(serde_json::to_vec(value).unwrap()))
}
fn outcome<T>(value: &Result<T>) -> Value {
    match value {
        Ok(_) => json!({"returned":"ok"}),
        Err(error) => {
            let full = error.to_string();
            json!({"returned":"error","detail":full.chars().take(4096).collect::<String>(),
                "detail_truncated":full.chars().count()>4096})
        }
    }
}
fn note(root: &Path, events: &mut Vec<Value>, stage: &str, facts: Value) {
    let event = json!({"stage":stage,"facts":facts});
    eprintln!("ACTUAL_STRONG_CATCHUP_TRANSFER {}", event);
    events.push(event);
    // Evidence failures never replace an already observed protocol error/panic.
    let written = serde_json::to_vec_pretty(events)
        .map_err(std::io::Error::other)
        .and_then(|bytes| std::fs::write(root.join("strong-catchup-transfer.json"), bytes));
    if let Err(error) = written {
        eprintln!("ACTUAL_STRONG_CATCHUP_EVIDENCE_UNAVAILABLE {error}");
    }
}
fn gap(f: &EnforcedNetwork, range: &RangeDescriptor, id: &str) -> Value {
    let entry = f.native.services[&node("n2")]
        .lock()
        .probe_range_write_for_peer(range, &range.leader, 1);
    let db = f.native.databases[&node("n2")].read();
    let native = db.native_range_prefix_observation(range.id);
    let row = db.get(crate::CLUSTER_EVENTS_COLLECTION, id);
    json!({"actual_node":"n2","actual_open":db.incarnation,"entry_result":outcome(&entry),
        "entry":entry.as_ref().ok().and_then(|p|p.entry.as_ref()),
        "actual_raw_progress":entry.as_ref().ok().map(|p|&p.progress),
        "native_result":outcome(&native),
        "native_applied":native.as_ref().ok().and_then(|p|p.native_applied_index),
        "native_pending":native.as_ref().ok().and_then(|p|p.pending_command_index),
        "native_materialized_pending":native.as_ref().ok().and_then(|p|p.materialized_pending_index),
        "row_result":outcome(&row),"row_present":row.as_ref().ok().map(|r|r.is_some()),
        "row_sha256":row.as_ref().ok().and_then(|r|r.as_ref()).map(digest)})
}

#[test]
fn native_event_endpoint_same_stale_gateway_cq_gap_transfer_recovers_original_acked_payload() {
    let mut f = EnforcedNetwork::new_endpoint_mode(true, true);
    let root = f.native.root.path().to_path_buf();
    let mut events = Vec::new();
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (old, key) = f.event_range_key("actual-cq-gap-acked");
        let id = crate::cluster_event_record_id("tenant", &key).unwrap();
        let expected = json!({"external_id":"actual-cq-gap-ack-1",
            "payload":"0123456789-abcdef".repeat(1024),
            "nested":{"none":null,"empty":{},"list":[true,7,"original",[1,2,3]]}});
        let initial = f.native.topology.read().clone();
        let gateway = f.event_client(initial.clone(), Duration::from_secs(20));
        note(
            &root,
            &mut events,
            "before_public_append",
            json!({
            "range":old,"key":key,"record_id":id,"expected_value_sha256":digest(&expected),
            "expected_value_bytes":serde_json::to_vec(&expected).unwrap().len(),
            "gateway_original_generation":initial.generation,"gateway_budget_ms":20000}),
        );
        let arm = f.native.databases[&node("n2")]
            .read()
            .test_arm_native_replica_cq_before_wal(old.id, "tenant", &key)
            .unwrap();
        let ack = gateway.append("tenant", &key, expected.clone());
        let cut = arm.snapshot();
        let after_ack = gap(&f, &old, &id);
        note(
            &root,
            &mut events,
            "actual_append_return_and_receiver_gap",
            json!({
            "append":outcome(&ack),"inserted":ack.as_ref().ok(),
            "cut":cut,"target":after_ack}),
        );
        assert!(ack.unwrap(), "the original public APP must actually ACK");
        assert!(serde_json::to_vec(&expected).unwrap().len() > 16 * 1024);
        let actual_cut = cut["actual_cut"]
            .as_object()
            .expect("actual selected receiver cut not reached");
        assert_eq!(actual_cut["actual_node"], json!(node("n2")));
        assert_eq!(actual_cut["actual_open"], after_ack["actual_open"]);
        assert_eq!(actual_cut["native_index_before"], 0);
        assert_eq!(
            after_ack["entry"]["state"],
            json!(RangeWriteState::QuorumCommitted)
        );
        assert_eq!(after_ack["native_applied"], 0);
        assert_eq!(after_ack["native_pending"], Value::Null);
        assert_eq!(after_ack["row_present"], false);
        let original_command = f.native.services[&node("n0")]
            .lock()
            .probe_range_write_for_peer(&old, &old.leader, 1)
            .unwrap()
            .entry
            .unwrap()
            .command;
        note(
            &root,
            &mut events,
            "actual_origin_command",
            json!({
            "command":original_command,"expected_value_sha256":digest(&expected)}),
        );
        assert_eq!(original_command.mutations.len(), 1);
        assert_eq!(
            original_command.mutations[0].collection,
            crate::CLUSTER_EVENTS_COLLECTION
        );
        assert_eq!(original_command.mutations[0].record_id, id);
        assert_eq!(
            original_command.mutations[0]
                .record
                .as_ref()
                .unwrap()
                .metadata,
            expected
        );
        assert_eq!(after_ack["entry"]["command"], json!(original_command));
        assert_eq!(actual_cut["command_id"], json!(original_command.command_id));
        assert_eq!(
            actual_cut["command_sha256"],
            json!(original_command.checksum_sha256)
        );
        let read = gateway.get_strong("tenant", &key);
        note(
            &root,
            &mut events,
            "actual_old_leader_read_of_acked_payload",
            json!({
            "read":outcome(&read),"actual_value_sha256":read.as_ref().ok().map(digest)}),
        );
        assert_eq!(read.unwrap(), Some(expected.clone()));
        f.idle();
        // No background repair/retry may silently close the intended gap.
        let unchanged_gap = gap(&f, &old, &id);
        note(
            &root,
            &mut events,
            "gap_still_actual_after_ack_and_strong",
            unchanged_gap.clone(),
        );
        assert_eq!(unchanged_gap, after_ack);
        drop(arm);

        // Genuine fsynced metadata quorum proposal; SAME voters, actual new
        // leader n2. This does not claim automatic placement/election.
        note(
            &root,
            &mut events,
            "before_actual_metadata_transfer",
            json!({"old_range":old}),
        );
        f.change_committed_leader(old.id);
        let range = f
            .native
            .topology
            .read()
            .range_by_id(old.id)
            .unwrap()
            .clone();
        note(
            &root,
            &mut events,
            "actual_metadata_transfer_return",
            json!({"current_range":range}),
        );
        assert_eq!(range.epoch, old.epoch + 1);
        assert_eq!(range.leader, node("n2"));
        assert_eq!(range.replicas, old.replicas);
        f.refresh_actual_writer_policies();
        for local in [node("n0"), node("n1"), node("n2")] {
            let published =
                f.dispatchers[&local].publish(&f.coordinators[&local], &f.hosts[&local]);
            note(
                &root,
                &mut events,
                "actual_dispatcher_publication",
                json!({
                "node":local,"result":outcome(&published),
                "note":"publication is not recovered-prefix/dataQQ/Strong readiness"}),
            );
            published.unwrap();
        }
        let stale = f.clients[&node("n1")].native_event_get_cancellable(
            &CancellationToken::uncancelable(),
            &node("n0"),
            &f.event_request(&key, &old),
        );
        note(
            &root,
            &mut events,
            "actual_old_route_reply",
            json!({
            "result":outcome(&stale),"reply":stale.as_ref().ok()}),
        );
        let stale = stale.unwrap();
        assert!(matches!(
            stale.result,
            NativeEventGetResult::Redirect { .. }
        ));
        let unready = f.clients[&node("n1")].native_event_get_cancellable(
            &CancellationToken::uncancelable(),
            &node("n2"),
            &f.event_request(&key, &range),
        );
        note(
            &root,
            &mut events,
            "actual_published_new_leader_catchup_refusal",
            json!({"result":outcome(&unready),"reply":unready.as_ref().ok()}),
        );
        let unready = unready.unwrap();
        assert!(
            matches!(
                unready.result,
                NativeEventGetResult::Unavailable {
                    code: NativeEventGetUnavailableCode::NativeRead {
                        stage: NativeStrongReadStage::DataQuorum | NativeStrongReadStage::Snapshot
                    },
                    ..
                }
            ),
            "the actual native catch-up barrier, not a missing dispatcher, must refuse"
        );
        // SAME stock SDK, original 20s request bound and its own real redirects
        // and retries. No metadata refresh/new gateway or synthetic route.
        let receiver_offset = f.dispatchers[&node("n2")].test_observed_requests().len();
        let gap_started = Instant::now();
        let gap_read = gateway.get_strong("tenant", &key);
        let gap_elapsed = gap_started.elapsed();
        let actual_sdk_attempts = f.dispatchers[&node("n2")]
            .test_observed_requests()
            .into_iter()
            .skip(receiver_offset)
            .collect::<Vec<_>>();
        note(
            &root,
            &mut events,
            "same_gateway_in_gap_return",
            json!({
            "result":outcome(&gap_read),"returned_value":gap_read.as_ref().ok(),
            "actual_elapsed_ms":gap_elapsed.as_millis(),"configured_client_budget_ms":20000,
            "actual_n2_SDK_requests":actual_sdk_attempts,
            "request_observation_scope":"existing bounded request ring, not complete RPC/outcome coverage",
            "scope":"endpoint component bound, not hot2s/OWN1.5s/phase5s or qualified RTO"}),
        );
        assert!(
            gap_read.is_err(),
            "unmaterialized selected leader must never return value/absence"
        );
        assert!(
            !actual_sdk_attempts.is_empty(),
            "SAME SDK must reach actual missing n2"
        );
        assert!(actual_sdk_attempts.iter().all(|r| r.2 == range.epoch));
        assert_eq!(
            actual_sdk_attempts
                .iter()
                .map(|r| r.0)
                .collect::<BTreeSet<_>>()
                .len(),
            1
        );
        f.idle();
        let still_missing = gap(&f, &range, &id);
        note(
            &root,
            &mut events,
            "gap_still_actual_before_recovery",
            still_missing.clone(),
        );
        assert_eq!(still_missing["entry"]["command"], json!(original_command));
        assert_eq!(
            still_missing["entry"]["state"],
            json!(RangeWriteState::QuorumCommitted)
        );
        assert_eq!(still_missing["native_applied"], 0);
        assert_eq!(still_missing["row_present"], false);

        note(
            &root,
            &mut events,
            "before_actual_fence_quorum",
            json!({"range":range}),
        );
        let scope = f.actual_current_recovery_scope(&range);
        note(
            &root,
            &mut events,
            "actual_fence_quorum_return",
            json!({"scope":scope}),
        );
        // Command comes from actual authenticated retained history, never the
        // external ACK/payload or a forged native floor/certificate.
        let mut discovered = None;
        for local in [node("n0"), node("n1"), node("n2")] {
            let history = f.clients[&node("n2")].recovery_fenced_history_observation(
                &local,
                &RecoveryFencedProbeRequest {
                    scope: scope.clone(),
                    index: Some(original_command.index),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            );
            note(
                &root,
                &mut events,
                "actual_retained_history_return",
                json!({
                "node":local,"result":outcome(&history),
                "entry":history.as_ref().ok().and_then(|h|h.entry())}),
            );
            let history = history.unwrap();
            let command = history
                .entry()
                .expect("original CQ/Applied command must remain")
                .command
                .clone();
            assert_eq!(command, original_command);
            if let Some(prior) = &discovered {
                assert_eq!(prior, &command);
            }
            discovered = Some(command);
            f.idle();
        }
        let request = RecoveryHistoricalCommandRequest {
            scope: scope.clone(),
            command: discovered.unwrap(),
            budget_ms: 3500,
        };
        for apply in [false, true] {
            for local in [node("n0"), node("n1"), node("n2")] {
                let result = f.clients[&node("n2")].recovery_historical_command_observation(
                    &local,
                    &request,
                    apply,
                    &CancellationToken::uncancelable(),
                );
                note(
                    &root,
                    &mut events,
                    "actual_same_command_historical_return",
                    json!({
                    "node":local,"apply":apply,"command_sha256":request.command.checksum_sha256,
                    "result":outcome(&result)}),
                );
                result.unwrap();
                f.idle();
            }
        }
        let (materialized, copied) = {
            let db = f.native.databases[&node("n2")].read();
            (
                db.native_range_prefix_observation(range.id).unwrap(),
                db.get(crate::CLUSTER_EVENTS_COLLECTION, &id).unwrap(),
            )
        };
        note(
            &root,
            &mut events,
            "actual_same_command_materialized",
            json!({
            "native_applied":materialized.native_applied_index,
            "pending":materialized.pending_command_index,"row_present":copied.is_some(),
            "actual_value_sha256":copied.as_ref().map(|r|digest(&r.metadata)),
            "original_command_sha256":original_command.checksum_sha256}),
        );
        assert_eq!(
            materialized.native_applied_index,
            Some(original_command.index)
        );
        assert_eq!(materialized.pending_command_index, None);
        assert_eq!(copied.unwrap().metadata, expected);
        for local in [node("n0"), node("n1"), node("n2")] {
            let ready = f.clients[&node("n2")].prepare_native_ready_observation(
                &local,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            );
            note(
                &root,
                &mut events,
                "actual_native_ready_return",
                json!({"node":local,"result":outcome(&ready)}),
            );
            assert_eq!(ready.unwrap().reply().range_index, original_command.index);
            f.idle();
        }
        for local in [node("n0"), node("n1"), node("n2")] {
            let complete = f.clients[&node("n2")].complete_native_recovery_observation(
                &local,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            );
            note(
                &root,
                &mut events,
                "actual_native_complete_return",
                json!({"node":local,"result":outcome(&complete)}),
            );
            assert_eq!(
                complete.unwrap().reply().range_index,
                original_command.index
            );
            f.idle();
        }
        let actor = f.clients[&node("n2")].local_metadata_read_participant();
        for local in [node("n0"), node("n1"), node("n2")] {
            // Existing trusted LOCAL activation API, not an incoming wire grant.
            let active = f.runtimes[&local].activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            );
            note(
                &root,
                &mut events,
                "actual_local_activation_return",
                json!({"node":local,"result":outcome(&active)}),
            );
            active.unwrap();
            f.idle();
        }
        let recovered = f.coordinators[&node("n2")].recover_local_leadership(8);
        note(
            &root,
            &mut events,
            "actual_ordinary_recovery_return",
            outcome(&recovered),
        );
        recovered.unwrap();
        let published =
            f.dispatchers[&node("n2")].publish(&f.coordinators[&node("n2")], &f.hosts[&node("n2")]);
        note(
            &root,
            &mut events,
            "actual_recovered_dispatcher_return",
            outcome(&published),
        );
        published.unwrap();
        let final_started = Instant::now();
        let final_value = gateway.get_strong("tenant", &key);
        let final_elapsed = final_started.elapsed();
        note(
            &root,
            &mut events,
            "same_gateway_final_strong_return",
            json!({
            "result":outcome(&final_value),"actual_value_sha256":final_value.as_ref().ok().map(digest),
            "expected_value_sha256":digest(&Some(expected.clone())),
            "actual_elapsed_ms":final_elapsed.as_millis(),"configured_client_budget_ms":20000}),
        );
        assert_eq!(final_value.unwrap(), Some(expected));
        f.idle();
    }));
    if let Err(primary) = run {
        let message = primary
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| primary.downcast_ref::<&str>().copied());
        note(
            &root,
            &mut events,
            "primary_panic_rethrown",
            json!({
            "qualification":"failed/unreached prerequisites are not successful catch-up proof",
            "primary_panic":message.map(|s|s.chars().take(4096).collect::<String>()),
            "primary_panic_truncated":message.is_some_and(|s|s.chars().count()>4096)}),
        );
        std::panic::resume_unwind(primary);
    }
}
