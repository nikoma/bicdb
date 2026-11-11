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
fn actual_gap_refusal_preserves_durable_observations_and_retained_prefix_recovers() {
    let fixture = ActualNativeRf3Fixture::new();
    // Exercise authenticated native replay for reachable replicas. Drop only
    // Apply1 to n2 to retain the original actual CQ1/native0 precondition;
    // later admission must really repair it rather than project generic rows.
    fixture.transport.enable_native_delivery();
    fixture.transport.drop_native_apply_once(node("n2"), 1);
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
    let catalogue_path = fixture.databases[&node("n1")]
        .read()
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let catalogue_before = std::fs::read(&catalogue_path).unwrap();
    let direct_gap = runtimes[&node("n1")]
        .certify_historical(&initiator, &tail_request, &parent)
        .unwrap_err();
    assert!(
        direct_gap.to_string().contains("compacted/unavailable gap"),
        "{direct_gap}"
    );
    assert_eq!(
        std::fs::read(&catalogue_path).unwrap(),
        catalogue_before,
        "a pure validation refusal must not publish a changed catalogue"
    );
    // This is a real fresh readonly observation, not inspection or clearing of
    // the uncertainty flag. No fence re-fsync, store reopen or state reset is
    // performed between the rejected request and this observation.
    let after_gap = runtimes[&node("n1")]
        .observe_history(
            &initiator,
            &RecoveryFencedProbeRequest {
                scope: scope.clone(),
                index: None,
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    assert!(after_gap.fence().is_active_recovery());
    assert_eq!(after_gap.fence().progress().last_index, 0);
    assert_eq!(after_gap.fence().progress().resolved_through, 0);
    assert_eq!(
        fixture.databases[&node("n1")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
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
        serde_json::json!({"acknowledged_commits":1,"uncertain_calls":1,"initial_source_native":1,"initial_source_raw":2,"initial_available_peer_native":0,"initial_available_peer_raw":0,"direct_tail_gap_correctly_refused":true,"prefix_export_without_source_ready":true,"prefix_recertify_apply_then_tail":true,"final_available_native":2,"final_available_resolved":2,"activation_proven":false,"physical_qualification":false})
    );
}

mod copied_obligation_case;

#[path = "recovery_order_tests/sparse_current_case.rs"]
mod sparse_current_case;

mod small_sparse_cold;

mod copied_full_image_case;

mod interrupted_normal_repair;

mod protected_resume_positive;
