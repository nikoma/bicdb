//! Separate observed traffic-first and Strong-qualified setup diagnostics.
//! Ordinary Strong probes can qualify setup only; they never grant APP.
//! Original traffic-first tests do not invoke this module or change deadlines.
use super::*;
use std::io::{self, Write};

pub(super) struct Journal {
    file: Mutex<std::fs::File>,
    context: Mutex<serde_json::Value>,
    origin: Instant,
    counts: Mutex<(u64, u64)>,
}
impl Journal {
    pub(super) fn new(output: &Path) -> Arc<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("first-app-rpc.jsonl"))
            .unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(output).unwrap().sync_all().unwrap();
        Arc::new(Self {
            file: Mutex::new(file),
            context: Mutex::new(serde_json::Value::Null),
            origin: Instant::now(),
            counts: Mutex::new((0, 0)),
        })
    }
    fn write(&self, value: &serde_json::Value) -> io::Result<()> {
        let mut value = value.clone();
        let mut counts = self.counts.lock();
        counts.0 += 1;
        value["journal_sequence"] = counts.0.into();
        value["unix_ns"] = serde_json::json!(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos());
        let mut bytes = serde_json::to_vec(&value)?;
        bytes.push(b'\n');
        counts.1 = counts.1.saturating_add(bytes.len() as u64);
        if counts.0 > 10_000 || counts.1 > 96 * 1024 * 1024 {
            return Err(io::Error::other(
                "complete first-APP diagnostic exceeds10000 records/96MiB; no truncation",
            ));
        }
        let mut file = self.file.lock();
        file.write_all(&bytes)?;
        file.sync_data()
    }
    pub(super) fn start(&self, context: serde_json::Value) {
        self.write(
            &serde_json::json!({"kind":"intent","elapsed_ns":self.origin.elapsed().as_nanos(),
            "context":context}),
        )
        .unwrap();
        *self.context.lock() = context;
    }
    pub(super) fn finish<T: serde::Serialize>(&self, result: &Result<T>) -> io::Result<()> {
        let context = self.context.lock().clone();
        let recorded = self.write(&serde_json::json!({"kind":"api_reply",
            "elapsed_ns":self.origin.elapsed().as_nanos(),"context":context,
            "actual_api_value":result.as_ref().ok(),
            "actual_api_error":result.as_ref().err().map(|error|error.to_string()),
            "error_does_not_establish_rollback":result.is_err()}));
        *self.context.lock() = serde_json::Value::Null;
        recorded
    }
}
impl crate::ClusterRpcEvidenceObserver for Journal {
    fn record(&self, event: &crate::ClusterRpcEvidence) -> io::Result<()> {
        let context = self.context.lock().clone();
        // Actual existing TransportHook callback. Failure remains a real SDK
        // evidence failure; never claim durable capture after failed journal IO.
        self.write(&serde_json::json!({"kind":"actual_rpc","context":context,"caller_context_is_correlation_not_authority":true,"rpc":event}))
    }
}

/// Sequential observations, not an atomic snapshot or an admission grant.
/// No engine/metadata/service latch crosses an SDK or control-host call.
#[allow(clippy::too_many_arguments)]
pub(super) fn snapshot(
    label: &str,
    controls: &[NativeMetadataControlHost],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    original: &[Arc<crate::db::InstalledNativeWriterHostGuard>],
    ranges: &[crate::RangeDescriptor],
    deadline: Instant,
) -> serde_json::Value {
    let mut nodes = Vec::new();
    for (i, control) in controls.iter().enumerate() {
        let metadata_status = metadata[i].try_lock().map(|store| store.status());
        let status_deadline = deadline.min(Instant::now() + Duration::from_millis(50));
        let status = control.initial_data_status(
            &CancellationToken::uncancelable().child_with_deadline(status_deadline),
        );
        let service = services[i].try_lock().map(|service| {
            let slot = service
                .native_replica_listener_store_open_guard()
                .require_live();
            (
                service.native_initial_listener_store_open_id(),
                slot.as_ref().err().map(|error| error.to_string()),
                service.database(),
            )
        });
        let binding = control.test_hot_recovery_bindings();
        let native = match (&service, &binding) {
            (Some((_,_,database)), Ok((coordinator,_guard,_,_))) => database.try_read().map(|database| {
                ranges.iter().map(|range| {
                    let voter = range.replicas.iter().any(|replica|replica.node_id==node(i)
                        && replica.role==crate::RangeReplicaRole::Voter);
                    let checked = voter.then(||original[i].require_for_coordinator(&database,range,coordinator));
                    let prefix = database.native_range_prefix_observation(range.id);
                    serde_json::json!({"range":range.id,"range_epoch":range.epoch,
                        "expected_leader":range.leader,"is_actual_voter":voter,
                        "original_guard_coordinator_check_executed":voter,
                        "original_guard_coordinator_error":checked.and_then(|result|result.err()).map(|e|e.to_string()),
                        "native_prefix":prefix.as_ref().ok(),
                        "native_prefix_error":prefix.as_ref().err().map(|e|e.to_string()),
                        "cached_leader_publication_hint_not_permission":coordinator.initial_data_leader_admission_observed(range)})
                }).collect::<Vec<_>>()
            }),
            _ => None,
        };
        let actual_binding = binding.as_ref().ok().map(|(coordinator, guard, _, _)| {
            serde_json::json!({
            "same_original_guard_arc":Arc::ptr_eq(&original[i],guard),
            "actual_coordinator_id":coordinator.native_writer_coordinator_id(),
            "guard_binding":guard.binding_diagnostic()})
        });
        nodes.push(serde_json::json!({"node":node(i),"metadata_status":metadata_status,
            "metadata_lock_busy":metadata_status.is_none(),
            "initial_data_status":status.as_ref().ok(),"initial_data_status_error":status.as_ref().err().map(|e|e.to_string()),
            "actual_service_store_open":service.as_ref().map(|(open,_,_)|open),
            "actual_service_slot_error":service.as_ref().and_then(|(_,error,_)|error),
            "service_lock_busy":service.is_none(),"binding":actual_binding,
            "binding_error":binding.as_ref().err().map(|e|e.to_string()),
            "native_observations":native,"native_observation_not_obtained":native.is_none()}));
    }
    serde_json::json!({"label":label,"nodes":nodes,"sequential_observations_not_atomic":true,
        "original_open_absolute_budget_remaining_ns":deadline.saturating_duration_since(Instant::now()).as_nanos(),
        "cached_readiness_is_not_admission":true,"startup_availability_qualification":false})
}

pub(super) fn require_strong_absent(
    client: &crate::ClusterEventClient,
    journal: &Journal,
    topology: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    original_open_deadline: Instant,
) {
    let unique = Uuid::now_v7();
    let mut observations = Vec::new();
    for range in ranges {
        let key = (0..100_000)
            .map(|i| format!("never-written-strong-probe-{unique}-{i}"))
            .find(|key| {
                topology
                    .range_for_key(
                        crate::CLUSTER_EVENTS_COLLECTION,
                        &crate::cluster_event_record_id("items", key).unwrap(),
                    )
                    .unwrap()
                    .id
                    == range.id
            })
            .unwrap();
        loop {
            // SDK keeps its original two-second absolute per-call bound. Do
            // not start a call that could extend the original setup budget.
            assert!(original_open_deadline.saturating_duration_since(Instant::now())>=Duration::from_secs(2),
                "both ordinary Strong ABSENT probes unavailable within original INITIAL open20s; observations={observations:?}");
            journal.start(serde_json::json!({"logical_id":Uuid::now_v7(),"operation":"get_strong",
                "collection":"items","key":key,"record_id":crate::cluster_event_record_id("items",&key).unwrap(),
                "range":range.id,"range_epoch":range.epoch,"topology_generation":topology.generation,
                "never_written_probe":true,"configured_timeout_ms":2000}));
            let result = client.get_strong("items", &key);
            let captured = journal.finish(&result);
            let absent = matches!(&result, Ok(None));
            observations.push(
                serde_json::json!({"range":range.id,"actual_value":result.as_ref().ok(),
                "actual_error":result.as_ref().err().map(|e|e.to_string())}),
            );
            captured.unwrap();
            assert!(
                !matches!(&result, Ok(Some(_))),
                "fresh never-written probe unexpectedly present: {result:?}"
            );
            if absent {
                break;
            }
        }
    }
}
