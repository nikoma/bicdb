//! Supplemental initial-APP diagnostics only. Original API outcome/assertions
//! and 2s budget are immutable; no extra APP, repair, lock clear or native open.
use super::*;
pub(super) fn append(
    f: &Fixture,
    journal: &Arc<Journal>,
    client: &crate::ClusterEventClient,
    range: &crate::RangeDescriptor,
    key: &str,
    value: &serde_json::Value,
) -> Result<bool> {
    let database = f.hosts[0].seed.database.clone();
    let arm = database
        .read()
        .test_observe_native_origin_attempt(range.id, "items", key);
    let result = super::append(journal, client, key, value);
    let supplemental = (|| -> Result<()> {
        let armed = arm.as_ref().map_err(|e| {
            BicDbError::Cluster(format!("initial APP observation unavailable: {e}"))
        })?;
        let probe = armed.observation();
        journal.append(serde_json::json!({"kind":"initial_APPEND_immutable_API_outcome_before_diagnostic_wait",
            "actual_API_ACK":result.is_ok(),"actual_API_inserted":result.as_ref().ok(),
            "actual_API_error":result.as_ref().err().map(ToString::to_string),"probe":probe.snapshot(),
            "supplemental_trace_cannot_erase_primary_ACK_or_reclassify_noACK":true}))?;
        let start = Instant::now();
        let end = start + Duration::from_secs(6);
        if result.is_err() {
            while !probe.app_span_ended() && Instant::now() < end {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        let actual = probe.snapshot();
        let physical = (|| -> Result<serde_json::Value> {
            let mut command = None;
            if let Some(ticket) = actual["actual_ticket_command_id"].as_str() {
                let service = f.hosts[0]._service.try_lock().ok_or_else(|| {
                    BicDbError::Cluster("initial diagnostic actual service still busy".into())
                })?;
                let progress = service.range_write_progress(range.id);
                for index in progress.compacted_through.saturating_add(1)..=progress.last_index {
                    if index > 64 {
                        return Err(BicDbError::Cluster(
                            "initial diagnostic command scan bound64 reached".into(),
                        ));
                    }
                    if let Some(entry) = service
                        .probe_range_write_for_peer(range, &node(0), index)?
                        .entry
                    {
                        if entry.command.command_id == ticket {
                            command = Some(entry);
                            break;
                        }
                    }
                }
            }
            let db = database.try_read().ok_or_else(|| {
                BicDbError::Cluster("initial diagnostic actual DB still busy".into())
            })?;
            let prefix = db.native_range_prefix_observation(range.id)?;
            let record = db.get(
                crate::CLUSTER_EVENTS_COLLECTION,
                &crate::cluster_event_record_id("items", key)?,
            )?;
            let materialized = command.as_ref().map(|entry| {
                let lineage = db.current_native_range_source_lineage(range.id)?;
                db.require_native_command_materialized(&lineage, &entry.command)
            });
            Ok(
                serde_json::json!({"actual_native_prefix":prefix,"actual_ticket_entry":command,"actual_full_record":record,
                "actual_native_command_materialization":materialized.as_ref().map(|r|r.is_ok()),
                "actual_materialization_error":materialized.and_then(|r|r.err()).map(|e|e.to_string()),
                "row_presence_or_catalogue_alone_is_not_materialization_proof":true}),
            )
        })();
        journal.append(serde_json::json!({"kind":"initial_APPEND_bounded_post_API_diagnostic",
            "diagnostic_wait_elapsed_ns":start.elapsed().as_nanos(),"diagnostic_wait_limit_ns":6_000_000_000u64,
            "not_request_budget_extension_or_retry":true,"actual_probe":actual,
            "actual_physical":physical.as_ref().ok(),"physical_diagnostic_error":physical.as_ref().err().map(ToString::to_string),
            "actual_API_ACK_remains":result.is_ok(),"no_engine_reopen_repair_or_authority":true}))?;
        Ok(())
    })();
    if let Err(error) = supplemental {
        // Never replace an original noACK or erase its established ACK because
        // an optional diagnostic could not complete. Existing full journal IO
        // errors remain explicit through the ordinary fixture paths.
        eprintln!("NATIVE_INITIAL_APPEND_SUPPLEMENTAL_DIAGNOSTIC_INCOMPLETE {error}");
    }
    drop(arm);
    result
}
