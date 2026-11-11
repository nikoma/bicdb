//! Actual retained-prefix recovery with an explicitly injected delivery
//! schedule. This is one-process protocol/storage evidence, not a VM fault.
use super::*;
use crate::db::ActualNativeRf3Fixture;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_consensus::{
    RangeWriteAck, RangeWriteProbe, RangeWriteRepairBatch, RangeWriteRepairLimits, RangeWriteState,
    RangeWriteTransport,
};
use crate::{BicDb, CancellationToken};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::{Arc, Weak};

fn node(name: &str) -> ClusterNodeId {
    ClusterNodeId::new(name).unwrap()
}

/// Only delivery is injected. Every successful observation is returned by its
/// real Runtime/provider/service/native store. Weak references avoid a cycle.
#[derive(Debug)]
struct ObservedRecoveryBus<T: RangeWriteTransport> {
    inner: Arc<T>,
    caller: MetadataReadParticipant,
    receivers: Arc<Mutex<BTreeMap<ClusterNodeId, Weak<RangeRecoveryRuntime>>>>,
    unavailable: Arc<Mutex<BTreeSet<ClusterNodeId>>>,
}
impl<T: RangeWriteTransport> RangeWriteTransport for ObservedRecoveryBus<T> {
    fn recovery_fenced_history_observation(
        &self,
        destination: &ClusterNodeId,
        request: &RecoveryFencedProbeRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        parent.check()?;
        if self.unavailable.lock().contains(destination) {
            return Err(invalid("test data delivery is unavailable"));
        }
        let receiver = self
            .receivers
            .lock()
            .get(destination)
            .and_then(Weak::upgrade)
            .ok_or_else(|| invalid("test receiver is absent"))?;
        receiver.observe_history(&self.caller, request, parent)
    }
    fn range_write_progress(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
    ) -> Result<RangeWriteProgress> {
        self.inner.range_write_progress(n, r, e)
    }
    fn apply_range_write_repair(
        &self,
        n: &ClusterNodeId,
        b: &RangeWriteRepairBatch,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.inner.apply_range_write_repair(n, b, l)
    }
    fn fetch_range_write_repair(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        p: u64,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.inner.fetch_range_write_repair(n, r, e, p, l)
    }
    fn probe_range_write(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        i: u64,
    ) -> Result<RangeWriteProbe> {
        self.inner.probe_range_write(n, r, e, i)
    }
    fn prepare_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.prepare_range_write(n, c)
    }
    fn commit_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.commit_range_write(n, c)
    }
    fn certify_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.certify_range_write(n, c)
    }
    fn apply_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.apply_range_write(n, c)
    }
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.abort_range_write(n, c)
    }
}

fn fresh_role(
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    initiator: &MetadataReadParticipant,
    range: &RangeDescriptor,
    nonce: Uuid,
) -> RecoveryRoleAdmission {
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let observed = providers[initiator.node_id()]
        .witness_recovery_role(
            initiator,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    RecoveryRoleAdmission::from_witness(
        observed,
        initiator,
        initiator.node_id(),
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap()
}

fn wait_for_contexts(receivers: &BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>) {
    let limit = Instant::now() + Duration::from_secs(2);
    while receivers
        .values()
        .any(|r| r.test_active_context_count() != 0)
    {
        assert!(Instant::now() < limit, "counted recovery jobs must drain");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn actual_raw_behind_zero_recovers_retained_prefix_before_uncertain_tail_without_source_ready() {
    let fixture = ActualNativeRf3Fixture::new();
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    // Inject only message delivery: ACK1 has genuine old quorum n0+n2 while
    // n1 receives NONE of its raw prefix. No caller provides Applied labels.
    fixture.transport.set_unavailable(&node("n1"), true);
    let acknowledged = "retained-prefix-ack-1";
    fixture.append(acknowledged).unwrap();
    let expected = fixture.databases[&node("n0")]
        .read()
        .get("items", acknowledged)
        .unwrap()
        .unwrap();
    let initial_h = fixture.databases[&node("n0")]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .logical_history_sha256()
        .to_string();
    let ledger = fixture.root.path().join("controller-acknowledged.jsonl");
    let mut external = std::fs::File::create(&ledger).unwrap();
    writeln!(external,"{}",serde_json::json!({"acknowledged_key":acknowledged,"record_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&expected).unwrap())),"history_after_ack":initial_h})).unwrap();
    external.sync_all().unwrap();
    drop(external);
    assert_eq!(
        fixture.services[&node("n1")]
            .lock()
            .range_write_progress(range.id)
            .last_index,
        0
    );
    assert_eq!(
        fixture.services[&node("n2")]
            .lock()
            .probe_range_write_for_peer(&range, &node("n0"), 1)
            .unwrap()
            .entry
            .unwrap()
            .state,
        RangeWriteState::QuorumCommitted
    );

    BicDb::test_reject_next_native_after_admission();
    let unknown = "retained-prefix-unknown-2";
    let error = fixture.append(unknown).unwrap_err();
    assert!(
        matches!(error, crate::BicDbError::CommitOutcomeUnknown { .. }),
        "{error}"
    );
    let tail = fixture.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n0"), 2)
        .unwrap()
        .entry
        .unwrap();
    assert_eq!(tail.state, RangeWriteState::QuorumCommitted);
    let source_native = fixture.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(source_native.native_applied_index, Some(1));
    assert_eq!(source_native.pending_command_index, Some(2));
    assert_eq!(source_native.materialized_pending_index, None);
    // The old second certifier becomes unavailable only AFTER its real CQ2.
    fixture.transport.set_unavailable(&node("n1"), false);
    fixture.transport.set_unavailable(&node("n2"), true);
    let topology = fixture.topology.read().clone();
    let providers = test_recovery_metadata_cluster(
        &fixture.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let initiator = providers[&node("n0")].local_participant();
    let registry = Arc::new(Mutex::new(BTreeMap::new()));
    let unavailable = Arc::new(Mutex::new(BTreeSet::from([node("n2")])));
    let runtimes = fixture
        .services
        .iter()
        .map(|(id, service)| {
            let bus = Arc::new(ObservedRecoveryBus {
                inner: Arc::clone(&fixture.transport),
                caller: providers[id].local_participant(),
                receivers: Arc::clone(&registry),
                unavailable: Arc::clone(&unavailable),
            });
            let runtime = Arc::new(
                RangeRecoveryRuntime::new(Arc::clone(service), Arc::clone(&providers[id]))
                    .with_participant_transport(bus),
            );
            registry.lock().insert(id.clone(), Arc::downgrade(&runtime));
            (id.clone(), runtime)
        })
        .collect::<BTreeMap<_, _>>();
    let nonce = Uuid::now_v7();
    let parent = CancellationToken::uncancelable();
    let role = fresh_role(&providers, &initiator, &range, nonce);
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    for id in [node("n0"), node("n1")] {
        runtimes[&id]
            .fence_epoch(
                &initiator,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
    }
    let tail_request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command: tail.command.clone(),
        budget_ms: 5000,
    };
    runtimes[&node("n0")]
        .certify_historical(&initiator, &tail_request, &parent)
        .unwrap();
    let direct_gap = runtimes[&node("n1")]
        .certify_historical(&initiator, &tail_request, &parent)
        .unwrap_err();
    assert!(
        direct_gap.to_string().contains("compacted/unavailable gap"),
        "{direct_gap}"
    );
    let frozen = runtimes[&node("n0")].source_capture_freeze(&scope).unwrap();
    let role = fresh_role(&providers, &initiator, &range, nonce);
    assert!(
        fixture.services[&node("n0")]
            .lock()
            .attest_native_recovery_ready(role, frozen, &parent)
            .is_err(),
        "unfinished tail is never SourceReady"
    );

    let limits = RangeWriteRepairLimits::default();
    let retained = fixture.services[&node("n0")]
        .lock()
        .export_range_write_repair(&range, &node("n0"), 0, &limits)
        .unwrap();
    retained.validate(&limits).unwrap();
    assert_eq!(retained.entries.len(), 1);
    assert_eq!(retained.entries[0].command.index, 1);
    assert_eq!(retained.entries[0].state, RangeWriteState::Applied);
    // The generic path stays refused by the actual recovery freeze. Transport
    // the authentic command through the fresh historical protocol instead.
    assert!(fixture.services[&node("n1")]
        .lock()
        .apply_range_write_repair(&range, &node("n0"), &retained, &limits)
        .is_err());
    // Baseline conservatively marks the store uncertain even for the pure
    // contiguous-gap refusal above. Re-run the ACTUAL same-session fence:
    // it obtains a fresh metadata quorum and re-fsyncs the unchanged catalog
    // before any new observation/certificate. No flag or numeric state is
    // cleared by this test, and neither DB nor store is reopened.
    runtimes[&node("n1")]
        .fence_epoch(
            &initiator,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    let prefix_request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command: retained.entries[0].command.clone(),
        budget_ms: 5000,
    };
    for id in [node("n0"), node("n1")] {
        runtimes[&id]
            .certify_historical(&initiator, &prefix_request, &parent)
            .unwrap();
    }
    runtimes[&node("n1")]
        .apply_historical(&initiator, &prefix_request, &parent)
        .unwrap();
    assert_eq!(
        fixture.databases[&node("n1")]
            .read()
            .get("items", acknowledged)
            .unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        fixture.databases[&node("n1")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .logical_history_sha256(),
        initial_h
    );
    assert_eq!(
        fixture.services[&node("n1")]
            .lock()
            .range_write_progress(range.id)
            .resolved_through,
        1
    );

    runtimes[&node("n1")]
        .certify_historical(&initiator, &tail_request, &parent)
        .unwrap();
    for id in [node("n0"), node("n1")] {
        runtimes[&id]
            .apply_historical(&initiator, &tail_request, &parent)
            .unwrap();
    }
    for id in [node("n0"), node("n1")] {
        assert_eq!(
            fixture.services[&id]
                .lock()
                .range_write_progress(range.id)
                .resolved_through,
            2
        );
        assert_eq!(
            fixture.databases[&id]
                .read()
                .get("items", acknowledged)
                .unwrap(),
            Some(expected.clone())
        );
        assert!(fixture.databases[&id]
            .read()
            .get("items", unknown)
            .unwrap()
            .is_some());
        assert_eq!(
            fixture.databases[&id]
                .read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            Some(2)
        );
    }
    let frozen = runtimes[&node("n0")].source_capture_freeze(&scope).unwrap();
    let role = fresh_role(&providers, &initiator, &range, nonce);
    let ready = fixture.services[&node("n0")]
        .lock()
        .attest_native_recovery_ready(role, frozen, &parent)
        .unwrap();
    assert_eq!(ready.range_index(), 2);
    assert_eq!(
        ready.logical_history_sha256(),
        fixture.databases[&node("n1")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .logical_history_sha256()
    );
    assert_eq!(
        std::fs::read_to_string(&ledger).unwrap().lines().count(),
        1,
        "unknown call never enters ACK ledger"
    );
    drop(ready);
    wait_for_contexts(&runtimes);
    eprintln!(
        "RETAINED_PREFIX_RECOVERY_ORDER {}",
        serde_json::json!({"acknowledged_commits":1,"uncertain_calls":1,"initial_source_native":1,"initial_source_raw":2,"initial_available_peer_native":0,"initial_available_peer_raw":0,"direct_tail_gap_correctly_refused":true,"prefix_export_without_source_ready":true,"same_session_refsync_after_validation_refusal":true,"prefix_recertify_apply_then_tail":true,"final_available_native":2,"final_available_resolved":2,"activation_proven":false,"physical_qualification":false})
    );
}

/// Actual liveness boundary, NOT an assertion that data is lost: n2 still owns
/// old-quorum history but its data delivery remains unavailable throughout.
#[test]
fn actual_compacted_prefix_1025_and_uncertain_1026_refuse_raw_zero_repair() {
    let mut fixture = ActualNativeRf3Fixture::new();
    fixture.root.disable_cleanup(true);
    eprintln!(
        "LARGE_PREFIX_RETAINED_ROOT {}",
        fixture.root.path().display()
    );
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    fixture.transport.set_unavailable(&node("n1"), true);
    let ledger_path = fixture
        .root
        .path()
        .join("controller-acknowledged-1025.jsonl");
    let mut ledger = std::fs::File::create(&ledger_path).unwrap();
    for index in 1..=1025u64 {
        let key = format!("compacted-prefix-ack-{index}");
        fixture.append(&key).unwrap();
        let record = fixture.databases[&node("n0")]
            .read()
            .get("items", &key)
            .unwrap()
            .unwrap();
        writeln!(ledger,"{}",serde_json::json!({"index":index,"acknowledged_key":key,"record_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&record).unwrap()))})).unwrap();
        ledger.sync_all().unwrap();
    }
    drop(ledger);
    let before_tail = fixture.services[&node("n0")]
        .lock()
        .range_write_progress(range.id);
    assert_eq!(before_tail.resolved_through, 1025);
    assert_eq!(before_tail.compacted_through, 1);
    let source_native = fixture.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(source_native.native_applied_index, Some(1025));
    assert_eq!(
        fixture.services[&node("n1")]
            .lock()
            .range_write_progress(range.id)
            .last_index,
        0
    );
    // This old current voter actually owns CQ1, but no longer retains command1
    // on the source. Do not invent a payload from the immutable checksum spine.
    assert!(fixture.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n0"), 1)
        .unwrap()
        .entry
        .is_none());
    BicDb::test_reject_next_native_after_admission();
    let unknown_error = fixture.append("compacted-prefix-unknown-1026").unwrap_err();
    assert!(
        matches!(
            unknown_error,
            crate::BicDbError::CommitOutcomeUnknown { .. }
        ),
        "{unknown_error}"
    );
    let tail = fixture.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n0"), 1026)
        .unwrap()
        .entry
        .unwrap();
    assert_eq!(tail.state, RangeWriteState::QuorumCommitted);
    let source_native = fixture.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(source_native.native_applied_index, Some(1025));
    assert_eq!(source_native.pending_command_index, Some(1026));
    assert_eq!(source_native.materialized_pending_index, None);
    fixture.transport.set_unavailable(&node("n1"), false);
    fixture.transport.set_unavailable(&node("n2"), true);
    let preconditions = serde_json::json!({"classification":"ACTUAL_PRECONDITIONS_BEFORE_RECOVERY","acknowledged_commits":1025,"source_progress":fixture.services[&node("n0")].lock().range_write_progress(range.id),"behind_progress":fixture.services[&node("n1")].lock().range_write_progress(range.id),"source_native":source_native,"actual_tail":tail,"unavailable_data_node":"n2","physical_qualification":false});
    std::fs::write(
        fixture.root.path().join("large-prefix-preconditions.json"),
        serde_json::to_vec_pretty(&preconditions).unwrap(),
    )
    .unwrap();
    let topology = fixture.topology.read().clone();
    let providers = test_recovery_metadata_cluster(
        &fixture.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let initiator = providers[&node("n0")].local_participant();
    let registry = Arc::new(Mutex::new(BTreeMap::new()));
    let unavailable = Arc::new(Mutex::new(BTreeSet::from([node("n2")])));
    let runtimes = fixture
        .services
        .iter()
        .map(|(id, service)| {
            let bus = Arc::new(ObservedRecoveryBus {
                inner: Arc::clone(&fixture.transport),
                caller: providers[id].local_participant(),
                receivers: Arc::clone(&registry),
                unavailable: Arc::clone(&unavailable),
            });
            let runtime = Arc::new(
                RangeRecoveryRuntime::new(Arc::clone(service), Arc::clone(&providers[id]))
                    .with_participant_transport(bus),
            );
            registry.lock().insert(id.clone(), Arc::downgrade(&runtime));
            (id.clone(), runtime)
        })
        .collect::<BTreeMap<_, _>>();
    let nonce = Uuid::now_v7();
    let parent = CancellationToken::uncancelable();
    let role = fresh_role(&providers, &initiator, &range, nonce);
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    for id in [node("n0"), node("n1")] {
        let result = runtimes[&id].fence_epoch(
            &initiator,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        );
        if let Err(error) = &result {
            // Preserve the FIRST actual boundary, including an observation-size
            // refusal before the anticipated compacted-gap checks. Never trim
            // irreversible obligations or expand protocol limits to get past it.
            let report = serde_json::json!({"classification":"FIRST_ACTUAL_RECOVERY_REFUSAL","operation":"fence_epoch","recipient":id,"error":error.to_string(),"fixture_root":fixture.root.path(),"source_progress":fixture.services[&node("n0")].lock().range_write_progress(range.id),"behind_progress":fixture.services[&node("n1")].lock().range_write_progress(range.id),"irretrievable_loss_proven":false,"physical_qualification":false});
            std::fs::write(
                fixture.root.path().join("first-recovery-refusal.json"),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            eprintln!("COMPACTED_PREFIX_FIRST_REFUSAL {report}");
        }
        result.expect("actual epoch-fence reply must exist before later gap claims");
    }

    let tail_request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command: tail.command.clone(),
        budget_ms: 5000,
    };
    runtimes[&node("n0")]
        .certify_historical(&initiator, &tail_request, &parent)
        .unwrap();
    let direct_gap = runtimes[&node("n1")]
        .certify_historical(&initiator, &tail_request, &parent)
        .unwrap_err();
    assert!(
        direct_gap.to_string().contains("compacted/unavailable gap"),
        "{direct_gap}"
    );
    // Baseline has a conservative global uncertain refusal after validation;
    // re-fsync the exact actual session before observing the independent gaps.
    runtimes[&node("n1")]
        .fence_epoch(
            &initiator,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    let limits = RangeWriteRepairLimits::default();
    let prefix_gap = fixture.services[&node("n0")]
        .lock()
        .export_range_write_repair(&range, &node("n0"), 0, &limits)
        .unwrap_err();
    assert!(
        prefix_gap.to_string().contains("requires a range snapshot"),
        "{prefix_gap}"
    );
    let frozen = runtimes[&node("n0")].source_capture_freeze(&scope).unwrap();
    let role = fresh_role(&providers, &initiator, &range, nonce);
    let source_ready_error = fixture.services[&node("n0")]
        .lock()
        .attest_native_recovery_ready(role, frozen, &parent)
        .unwrap_err();
    assert!(
        source_ready_error
            .to_string()
            .contains("historical prefix/tail is not fully resolved"),
        "{source_ready_error}"
    );
    // Check final obligations instead of treating the expected errors as Ready.
    let source_progress = fixture.services[&node("n0")]
        .lock()
        .range_write_progress(range.id);
    let behind_progress = fixture.services[&node("n1")]
        .lock()
        .range_write_progress(range.id);
    let source_native = fixture.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let behind_native = fixture.databases[&node("n1")]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(
        (
            source_progress.last_index,
            source_progress.resolved_through,
            source_progress.compacted_through
        ),
        (1026, 1025, 1)
    );
    assert_eq!(behind_progress.last_index, 0);
    assert_eq!(source_native.native_applied_index, Some(1025));
    assert_eq!(source_native.pending_command_index, Some(1026));
    assert_eq!(behind_native.native_applied_index, Some(0));
    let mut acknowledged_checked = 0u64;
    for line in std::fs::read_to_string(&ledger_path).unwrap().lines() {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        let record = fixture.databases[&node("n0")]
            .read()
            .get("items", entry["acknowledged_key"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            entry["record_sha256"],
            hex::encode(Sha256::digest(serde_json::to_vec(&record).unwrap()))
        );
        acknowledged_checked += 1;
    }
    assert_eq!(acknowledged_checked, 1025);
    wait_for_contexts(&runtimes);
    let report = serde_json::json!({"classification":"FAIL_CLOSED_LIVENESS_BOUNDARY","scope":"in-process actual RF3 storage and protocol; injected data delivery only","acknowledged_commits_reconciled_on_source":acknowledged_checked,"uncertain_calls":1,"available_data_nodes":["n0","n1"],"unavailable_data_node":"n2","source_progress":source_progress,"behind_progress":behind_progress,"source_native":source_native,"behind_native":behind_native,"direct_tail_gap_error":direct_gap.to_string(),"prefix_export_error":prefix_gap.to_string(),"source_ready_error":source_ready_error.to_string(),"activation_proven":false,"physical_qualification":false,"irretrievable_loss_proven":false,"fixture_root":fixture.root.path()});
    std::fs::write(
        fixture
            .root
            .path()
            .join("compacted-prefix-refusal-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("COMPACTED_PREFIX_RECOVERY_REFUSAL {report}");
}
