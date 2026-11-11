//! Real fsynced services, injected delivery only. Embedded caller identity is
//! explicit; this is not a TLS, VM-fault, StrongGet or production qualification.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, serde::Serialize)]
struct RepairResult {
    destination: ClusterNodeId,
    successful: bool,
    progress: Option<RangeWriteProgress>,
    error: Option<String>,
}
#[derive(Debug)]
struct LoseOneApply {
    inner: Arc<ActualNativeRf3ServiceTransport>,
    dropped_apply_one: AtomicBool,
    repairs: Mutex<Vec<RepairResult>>,
}
impl RangeWriteTransport for LoseOneApply {
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
        self.repairs.lock().push(RepairResult {
            destination: n.clone(),
            successful: actual.is_ok(),
            progress: actual.as_ref().ok().cloned(),
            error: actual.as_ref().err().map(ToString::to_string),
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
        if n == &ClusterNodeId::new("n2")?
            && c.index == 1
            && !self.dropped_apply_one.swap(true, Ordering::SeqCst)
        {
            return Err(invalid(
                "test deliberately lost ONLY Apply1 delivery to n2 after actual CQ1",
            ));
        }
        // The existing embedded bare-Apply native limitation stays explicit;
        // no fabricated physical success is returned for any other request.
        self.inner.apply_range_write(n, c)
    }
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.abort_range_write(n, c)
    }
}

#[test]
fn native_generic_repair_after_lost_apply_never_reports_raw_applied_with_unknown_native_history() {
    let mut fixture = ActualNativeRf3Fixture::new();
    fixture.root.disable_cleanup(true);
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
    writeln!(
        ack,
        "{}",
        serde_json::json!({"key":"generic-repair-ack-1","index":1})
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
    writeln!(
        ack,
        "{}",
        serde_json::json!({"key":"generic-repair-ack-2","index":2})
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
        .cloned()
        .expect("second origin must exercise actual target catch-up");
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
    let report = serde_json::json!({"classification":"ACTUAL_NATIVE_GENERIC_REPAIR_CHARACTERIZATION","fault":"only Apply1 to n2 deliberately not delivered after genuine CQ1","embedded_bare_apply_native_limitation":true,"repair_result":repair,"target_native_before":before,"target_native_after":after,"target_raw_before":before_raw,"target_raw_after":after_raw,"target_copied_row_present":copied_row.is_some(),"actual_origin_acks":2,"source_native_after":2,"transport":"in-process actual service replies; no TLS claim","physical_qualification":false,"new_leader_readiness_proven":false,"irretrievable_loss_proven":false});
    std::fs::write(
        fixture
            .root
            .path()
            .join("native-generic-repair-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("NATIVE_GENERIC_REPAIR_REPORT {report}");
    if repair.successful {
        assert_eq!(repair.progress.as_ref().unwrap().resolved_through, 1);
        assert!(copied_row.is_some());
        assert_eq!(
            after.native_applied_index,
            Some(1),
            "a successful repair may not report RawApplied1 while actual native proof is unknown"
        );
        assert!(after.factory_initialization_complete && !after.invalidated);
    } else {
        // An explicit unsupported refusal is safe ONLY if it preserves the
        // actual native and raw prefix. This is not successful catch-up.
        assert_eq!(after, before);
        assert_eq!(after_raw, before_raw);
        assert!(copied_row.is_none());
    }
}
