//! Real fsynced services, injected delivery only. Embedded caller identity is
//! explicit; this is not a TLS, VM-fault, StrongGet or production qualification.
use super::*;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;

#[derive(Clone, Debug, serde::Serialize)]
struct RepairResult {
    destination: ClusterNodeId,
    successful: bool,
    progress: Option<RangeWriteProgress>,
    error: Option<String>,
    native_immediately_after_repair: NativeRangePrefixObservation,
}
#[derive(Clone, Debug, serde::Serialize)]
struct PrepareTwoBoundary {
    native: NativeRangePrefixObservation,
    raw: RangeWriteProgress,
    first_record: Option<Record>,
}
#[derive(Debug)]
struct LoseOneApply {
    inner: Arc<ActualNativeRf3ServiceTransport>,
    dropped_apply_one: AtomicBool,
    repairs: Mutex<Vec<RepairResult>>,
    databases: BTreeMap<ClusterNodeId, Weak<RwLock<BicDb>>>,
    before_prepare_two: Mutex<Option<PrepareTwoBoundary>>,
}
impl RangeWriteTransport for LoseOneApply {
    fn observe_native_origin(
        &self,
        node: &ClusterNodeId,
        request: &crate::distribution_native_apply::NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        self.inner.observe_native_origin(node, request, parent)
    }
    fn recertify_native_origin(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<RangeWriteAck> {
        self.inner.recertify_native_origin(node, command, parent)
    }
    fn apply_native_origin(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
        certificate: &crate::distribution_range_consensus::NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        if node == &ClusterNodeId::new("n2")?
            && command.index == 1
            && !self.dropped_apply_one.swap(true, Ordering::SeqCst)
        {
            return Err(invalid(
                "test lost ONLY first actual native Apply1 delivery to n2 after genuine CQ1",
            ));
        }
        // No observation is fabricated: every successful response is produced
        // by the actual service with the opaque genuine origin certificate.
        self.inner
            .apply_native_origin(node, command, certificate, parent)
    }
    fn range_write_progress(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
    ) -> Result<RangeWriteProgress> {
        self.inner.range_write_progress(n, r, e)
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
    fn apply_range_write_repair(
        &self,
        n: &ClusterNodeId,
        b: &RangeWriteRepairBatch,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        let actual = self.inner.apply_range_write_repair(n, b, l);
        // Observe the actual local native state BEFORE returning to prepare2/
        // Apply2. Correct later replication may legitimately advance beyond1.
        let database = self.databases[n].upgrade().expect("fixture DB is live");
        let native_immediately_after_repair = database
            .read()
            .native_range_prefix_observation(b.range_id)
            .unwrap();
        self.repairs.lock().push(RepairResult {
            destination: n.clone(),
            successful: actual.is_ok(),
            progress: actual.as_ref().ok().cloned(),
            error: actual.as_ref().err().map(ToString::to_string),
            native_immediately_after_repair,
        });
        actual
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
        if n == &ClusterNodeId::new("n2")? && c.index == 2 {
            // Independent of legacy versus future qualified catch-up method.
            // Capture only; assert AFTER source returns its second real ACK.
            let database = self.databases[n].upgrade().expect("fixture DB is live");
            let (native, first_record) = {
                let db = database.read();
                (
                    db.native_range_prefix_observation(c.range_id).unwrap(),
                    db.get("items", "generic-repair-ack-1")?,
                )
            };
            let boundary = PrepareTwoBoundary {
                native,
                raw: self
                    .inner
                    .range_write_progress(n, c.range_id, c.range_epoch)?,
                first_record,
            };
            *self.before_prepare_two.lock() = Some(boundary);
        }
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
        // Legacy/bare Apply remains a separate unchanged path.
        self.inner.apply_range_write(n, c)
    }
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.abort_range_write(n, c)
    }
}

#[test]
fn actual_native_catchup_produces_prefix_one_before_next_prepare_and_ack() {
    let mut fixture = ActualNativeRf3Fixture::new();
    fixture.root.disable_cleanup(true);
    fixture.transport.enable_native_delivery();
    eprintln!(
        "NATIVE_GENERIC_REPAIR_RETAINED_ROOT {}",
        fixture.root.path().display()
    );
    let source = ClusterNodeId::new("n0").unwrap();
    let target = ClusterNodeId::new("n2").unwrap();
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let initial = fixture.databases[&target]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(initial.native_applied_index, Some(0));
    assert!(initial.factory_initialization_complete && !initial.invalidated);
    let transport = Arc::new(LoseOneApply {
        inner: Arc::clone(&fixture.transport),
        dropped_apply_one: AtomicBool::new(false),
        repairs: Mutex::new(Vec::new()),
        databases: fixture
            .databases
            .iter()
            .map(|(id, db)| (id.clone(), Arc::downgrade(db)))
            .collect(),
        before_prepare_two: Mutex::new(None),
    });
    // Replace the unused bootstrap admission before ANY transaction with a
    // real coordinator that independently recovers the actual same stores.
    // No admission/gate clearing or recovered-epoch/certificate seeding.
    let coordinator = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            fixture.topology.read().cluster_id.clone(),
            source.clone(),
            Arc::clone(&fixture.topology),
            Arc::clone(&fixture.services[&source]),
            Arc::clone(&transport),
        )
        .unwrap(),
    );
    coordinator.recover_local_leadership(8).unwrap();
    fixture.databases[&source]
        .read()
        .install_commit_admission(coordinator.clone());
    let ledger = fixture
        .root
        .path()
        .join("controller-two-acknowledged.jsonl");
    let mut ack = std::fs::File::create(&ledger).unwrap();
    use std::io::Write;
    fixture.append("generic-repair-ack-1").unwrap();
    let first_record = fixture.databases[&source]
        .read()
        .get("items", "generic-repair-ack-1")
        .unwrap()
        .unwrap();
    let first_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&first_record).unwrap()));
    writeln!(
        ack,
        "{}",
        serde_json::json!({"key":"generic-repair-ack-1","index":1,"record_sha256":first_sha256})
    )
    .unwrap();
    ack.sync_all().unwrap();
    assert!(transport.dropped_apply_one.load(Ordering::SeqCst));
    let before = fixture.databases[&target]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(before, initial);
    let before_raw = fixture.services[&target]
        .lock()
        .range_write_progress(range.id);
    assert_eq!((before_raw.last_index, before_raw.resolved_through), (1, 0));
    assert_eq!(
        fixture.services[&target]
            .lock()
            .probe_range_write_for_peer(&range, &source, 1)
            .unwrap()
            .entry
            .unwrap()
            .state,
        RangeWriteState::QuorumCommitted
    );
    assert!(fixture.databases[&target]
        .read()
        .get("items", "generic-repair-ack-1")
        .unwrap()
        .is_none());
    assert_eq!(
        fixture.databases[&source]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(1)
    );
    fixture.append("generic-repair-ack-2").unwrap();
    let second_record = fixture.databases[&source]
        .read()
        .get("items", "generic-repair-ack-2")
        .unwrap()
        .unwrap();
    let second_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&second_record).unwrap()));
    writeln!(
        ack,
        "{}",
        serde_json::json!({"key":"generic-repair-ack-2","index":2,"record_sha256":second_sha256})
    )
    .unwrap();
    ack.sync_all().unwrap();
    drop(ack);
    let after = fixture.databases[&target]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let after_raw = fixture.services[&target]
        .lock()
        .range_write_progress(range.id);
    let repair = transport
        .repairs
        .lock()
        .iter()
        .find(|r| r.destination == target)
        .cloned();
    let boundary = transport
        .before_prepare_two
        .lock()
        .clone()
        .expect("second prepare must expose actual post-catch-up state");
    let copied_row = fixture.databases[&target]
        .read()
        .get("items", "generic-repair-ack-1")
        .unwrap();
    for key in ["generic-repair-ack-1", "generic-repair-ack-2"] {
        assert!(fixture.databases[&source]
            .read()
            .get("items", key)
            .unwrap()
            .is_some());
    }
    assert_eq!(
        fixture.databases[&source]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(2)
    );
    let report = serde_json::json!({"classification":"ACTUAL_NATIVE_CATCHUP_BEFORE_NEXT_PREPARE","fault":"only Apply1 to n2 deliberately not delivered after genuine CQ1","authenticated_native_origin_instruction":true,"repair_result":repair,"before_prepare_two":boundary,"target_native_before":before,"target_native_after":after,"target_raw_before":before_raw,"target_raw_after":after_raw,"target_copied_row_present":copied_row.is_some(),"actual_origin_acks":2,"source_native_after":2,"transport":"in-process actual service replies; no TLS claim","physical_qualification":false,"new_leader_readiness_proven":false,"irretrievable_loss_proven":false});
    std::fs::write(
        fixture
            .root
            .path()
            .join("native-generic-repair-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("NATIVE_GENERIC_REPAIR_REPORT {report}");
    // Exact post-catch-up/pre-prepare state remains meaningful even when a
    // qualified later Apply2 correctly advances final native history to2.
    assert_eq!(
        boundary.native.native_applied_index,
        Some(1),
        "catch-up before prepare2 must establish actual native1; RawApplied alone is insufficient"
    );
    assert!(boundary.native.factory_initialization_complete && !boundary.native.invalidated);
    assert_eq!(boundary.raw.resolved_through, 1);
    assert_eq!(boundary.first_record.as_ref(), Some(&first_record));
    assert_eq!(copied_row.as_ref(), Some(&first_record));
    assert_eq!(after.native_applied_index, Some(2));
    assert_eq!(after_raw.resolved_through, 2);
    assert_eq!(
        fixture.databases[&target]
            .read()
            .get("items", "generic-repair-ack-2")
            .unwrap(),
        Some(second_record)
    );
    assert_eq!(std::fs::read_to_string(&ledger).unwrap().lines().count(), 2);
    assert!(after.factory_initialization_complete && !after.invalidated);
}
