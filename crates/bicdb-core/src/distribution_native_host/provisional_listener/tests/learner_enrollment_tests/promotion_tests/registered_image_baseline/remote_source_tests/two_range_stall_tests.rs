//! Independent-range progress COMPONENT ONLY: actual selected Rows reply held
//! while ordinary SDK APP/Strong use another populated source range. This does
//! not receive a second range on Joined, install target rows, or qualify HA.
use super::*;
use crate::distribution_transport::native_image_response_stall::ActualRowsReplyHold;

#[test]
fn native_registered_fifth_remote_selected_rows_reply_stall_other_populated_range_original_sdk_progress(
) {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TwoRangeSourceReplyStall),
    );
}
fn call(
    gateway: &crate::ClusterEventClient,
    pending: &ClusterTopology,
    key: &str,
    value: &serde_json::Value,
    append: bool,
    hold: Option<&ActualRowsReplyHold>,
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
) -> Result<serde_json::Value> {
    let record_id = crate::cluster_event_record_id("items", key)?;
    let range = pending.range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &record_id)?;
    let at = ledger.len();
    ledger.push(serde_json::json!({"sequence":at,"logical_id":Uuid::now_v7(),
        "operation":if append {"ClusterEventClient.append"} else {"ClusterEventClient.get_strong"},
        "key":key,"namespace":"items","record_id":record_id,"expected_full_value":value,
        "payload_json_sha256":hex::encode(Sha256::digest(serde_json::to_vec(value)?)),
        // Expected metadata is DATA for comparison, never an observation of
        // the SDK's actual chosen route or permission to manually route it.
        "expected_range_id":range.id,"expected_range_epoch":range.epoch,"expected_leader":range.leader,
        "expected_topology_generation":pending.generation,
        "expected_metadata_source":"fixture pending committed descriptor, not actual SDK dispatch",
        "actual_dispatch_evidence":"existing original-sdk-rpc journal and index, separately retained",
        "logical_id_is_wire_attempt_id":false,"original_prejoin_gateway":true,
        "configured_sdk_request_ms":2000,"manual_shard_hint":false,"manual_sdk_refresh":false,
        "intent_unix_ns":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        "hold_at_invocation":hold.map(ActualRowsReplyHold::snapshot),"outcome":"not_started"}));
    evidence(&output.join("two-range-stall-external-ledger.json"), ledger)?;
    let start = Instant::now();
    let mut read_present = None;
    let result = if append {
        gateway
            .append("items", key, value.clone())
            .map(serde_json::Value::from)
    } else {
        gateway.get_strong("items", key).and_then(|v| {
            read_present = Some(v.is_some());
            serde_json::to_value(v).map_err(Into::into)
        })
    };
    ledger[at]["elapsed_us"] = serde_json::json!(start.elapsed().as_micros());
    ledger[at]["actual_result"] = result.as_ref().ok().cloned().unwrap_or_default();
    ledger[at]["actual_strong_present"] = serde_json::json!(read_present);
    ledger[at]["exact_error"] = serde_json::json!(result.as_ref().err().map(ToString::to_string));
    ledger[at]["debug_error"] = serde_json::json!(result.as_ref().err().map(|e| format!("{e:?}")));
    ledger[at]["acknowledged"] = (append && result.is_ok()).into();
    ledger[at]["uncertain_write_effect"] = (append && result.is_err()).into();
    ledger[at]["outcome"] = if result.is_ok() {
        "returned"
    } else if append {
        "uncertain_write_effect"
    } else {
        "read_unavailable"
    }
    .into();
    ledger[at]["hold_at_return"] = serde_json::json!(hold.map(ActualRowsReplyHold::snapshot));
    let saved = evidence(&output.join("two-range-stall-external-ledger.json"), ledger);
    match result {
        Err(primary) => {
            if let Err(error) = saved {
                eprintln!("two-range ledger error: {error}");
            }
            Err(primary)
        }
        Ok(value) => {
            saved?;
            Ok(value)
        }
    }
}
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn exercise(
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &[serde_json::Value],
    gateway: &crate::ClusterEventClient,
    output: &std::path::Path,
    process: &CancellationToken,
) {
    for control in controls {
        control.bind_initial_image_source_process(process).unwrap();
    }
    assert_eq!(ranges.len(), 2);
    let selected = pending.range_by_id(ranges[0].id).unwrap();
    let other = pending.range_by_id(ranges[1].id).unwrap();
    assert_ne!(selected.id, other.id);
    assert!(!pending.relocations.values().any(|p| p.range_id == other.id));
    let mut ledger = Vec::new();
    let prior = &acknowledged[1];
    let prior_key = prior["key"].as_str().unwrap();
    let prior_value = &prior["value"];
    // Genuine current ordinary Strong is a prerequisite, not a cached ready
    // bit or local DB read used as a serving permission.
    let prerequisite = call(
        gateway,
        pending,
        prior_key,
        prior_value,
        false,
        None,
        output,
        &mut ledger,
    );
    assert_eq!(prerequisite.unwrap(), *prior_value);
    let relocation = pending
        .relocations
        .values()
        .find(|p| p.range_id == selected.id)
        .unwrap();
    let leader = (0..4).find(|i| node(*i) == selected.leader).unwrap();
    let business = Uuid::now_v7();
    let request = budget(5000);
    let observed = clients[leader]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            selected.id,
            selected.epoch,
            Duration::from_secs(5),
            &request,
        )
        .unwrap();
    let round = NativeJointOldRound::begin(observed, business, relocation.id).unwrap();
    let plan = round.plan();
    drop(round); // purpose DATA only; never count a local OLD closure/quorum.
    let mut source_journal = Vec::new();
    let (source, retained, _) = remote_old_seed(
        SourceCase::TwoRangeSourceReplyStall,
        target,
        controls,
        clients,
        services,
        metadata,
        &plan,
        process,
        output,
        &mut source_journal,
    )
    .unwrap();
    let publish = budget(5000);
    let business = Uuid::now_v7();
    let own = clients[source]
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &publish)
        .unwrap();
    let manifest = retained.publish(&own, business, &publish).unwrap();
    assert_eq!(manifest.capture_range.id, selected.id);
    let actual = frame(target, &manifest, Op::Manifest, output, &mut source_journal).unwrap();
    let fact = NativeRemoteImageFact::from_actual_manifest(actual).unwrap();
    let hold = ActualRowsReplyHold::install(&manifest);
    let new_key = (0..100_000)
        .map(|i| format!("two-range-source-stall-{i}"))
        .find(|key| {
            let record = crate::cluster_event_record_id("items", key).unwrap();
            pending
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &record)
                .unwrap()
                .id
                == other.id
        })
        .unwrap();
    let value = serde_json::json!({"component":"independent-range-during-selected-response-hold",
        "nested":[null,17,{"full":"unaffected range payload"}]});
    // Pure logical DATA only. Move the original journal into its next producer
    // so frame() cannot overwrite prior actual OLD/Manifest evidence with a
    // new vector. No live source/request/authority object is copied here.
    let source_prefix = source_journal.clone();
    let source_prefix_len = source_prefix.len();
    let (reached, held_before, old_read, append, new_read, held_after, frame_result, wire_journal) =
        std::thread::scope(|scope| {
            let manifest = &manifest;
            let worker = scope.spawn(move || {
                let mut journal = source_journal;
                let result = frame(
                    target,
                    manifest,
                    Op::Rows {
                        cursor: RangeImageCursorV1::default(),
                        options: Default::default(),
                    },
                    output,
                    &mut journal,
                );
                (result, journal)
            });
            let reached = hold.wait_reached(&budget(5000));
            let before = hold.snapshot();
            // Retain every original2s outcome BEFORE any assertion/release.
            // Independent calls are intentionally sequential; if they consume
            // the original transport deadline this gate fails without renewal.
            let old_read = call(
                gateway,
                pending,
                prior_key,
                prior_value,
                false,
                Some(&hold),
                output,
                &mut ledger,
            );
            let append = call(
                gateway,
                pending,
                &new_key,
                &value,
                true,
                Some(&hold),
                output,
                &mut ledger,
            );
            let new_read = call(
                gateway,
                pending,
                &new_key,
                &value,
                false,
                Some(&hold),
                output,
                &mut ledger,
            );
            let after = hold.snapshot();
            hold.release();
            let (frame, journal) = worker.join().unwrap();
            (
                reached, before, old_read, append, new_read, after, frame, journal,
            )
        });
    let source_prefix_preserved =
        wire_journal.get(..source_prefix_len) == Some(source_prefix.as_slice());
    let actual_rows_entry_appended = wire_journal.len() == source_prefix_len + 1
        && wire_journal.last().is_some_and(|entry| {
            entry["operation"] == "source_image_frame"
                && entry["intent"]["operation"].get("Rows").is_some()
        });
    // Validate the actual returned observation while its original request is
    // still live, before unrelated reconciliation/evidence IO. Retain the
    // complete observed reply as DATA; this boolean is never a target grant.
    let wire = frame_result
        .as_ref()
        .ok()
        .map(|actual| serde_json::to_value(actual.reply()).unwrap());
    let frame_result = frame_result.and_then(|frame| {
        let actual = fact.bind_step(frame)?;
        if !matches!(actual.payload()?, Payload::Rows(_)) {
            return Err(BicDbError::Cluster(
                "actual resumed Rows reply missing".into(),
            ));
        }
        Ok(())
    });
    // The actual handler and client are now unblocked/joined, including all
    // failure paths. Refusal/timeout never means rollback; Strong below merely
    // reconciles the new write and cannot retroactively create its API ACK.
    let reconciled = call(
        gateway,
        pending,
        &new_key,
        &value,
        false,
        None,
        output,
        &mut ledger,
    );
    evidence(&output.join("two-range-stall-result.json"),&serde_json::json!({
        "scope":"independent-range progress component only","selected_range":selected,
        "unaffected_populated_range":other,"original_acknowledgments":acknowledged,
        "actual_manifest":manifest,"actual_held_before":held_before,"actual_held_after":held_after,
        "actual_after_release":hold.snapshot(),"reached_error":reached.as_ref().err().map(ToString::to_string),
        "actual_rows_reply":wire,"actual_frame_validated_before_reconciliation":frame_result.is_ok(),
        "actual_frame_error":frame_result.as_ref().err().map(ToString::to_string),
        "actual_frame_error_debug":frame_result.as_ref().err().map(|e|format!("{e:?}")),
        "pre_worker_source_journal_data_prefix":source_prefix,
        "pre_worker_source_journal_prefix_len":source_prefix_len,
        "original_source_journal_prefix_preserved":source_prefix_preserved,
        "actual_rows_entry_appended":actual_rows_entry_appended,
        "actual_request_journal":wire_journal,"external_api_ledger":ledger,
        "new_write_reconciliation":reconciled.as_ref().ok(),"reconciliation_error":reconciled.as_ref().err().map(ToString::to_string),
        "target_rows_installed":false,"second_range_moved_after_joined":false,
        "disk_stall_tested":false,"transport_hold_after_actual_export":true,
        "automatic_controller_or_physical_qualification":false})).unwrap();
    reached.unwrap();
    assert!(
        source_prefix_preserved,
        "original actual OLD/Manifest journal prefix must survive Rows worker"
    );
    assert!(
        actual_rows_entry_appended,
        "one actual Rows request must append to the same complete source journal"
    );
    assert_eq!(
        held_before["currently_held"], true,
        "actual selected reply hold prerequisite"
    );
    assert_eq!(
        held_after["currently_held"], true,
        "other range must progress while original selected transport remains held"
    );
    assert_eq!(
        old_read.unwrap(),
        *prior_value,
        "prior ACK full Strong while selected response held"
    );
    assert_eq!(
        append.unwrap(),
        true,
        "fresh other-range ordinary APP ACK while held"
    );
    assert_eq!(new_read.unwrap(), value, "new ACK full Strong while held");
    assert_eq!(reconciled.unwrap(), value);
    frame_result.unwrap();
    assert_eq!(hold.snapshot()["handler_resumed"], true);
    assert_eq!(hold.snapshot()["original_request_expired"], false);
}
