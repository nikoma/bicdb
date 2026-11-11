//! Genuine loopback mTLS ordinary Apply. Metadata leader differs from data
//! leader. Initial membership/election are fixture preconditions, not HA.
use super::*;
use crate::distribution_native_apply::{NativeOriginApplyRequest, NativeReplicaApplyObservation};
use crate::distribution_range_consensus::{NativeRangeCommitCertificate, RangeWriteCoordinator};

#[derive(Debug)]
struct NormalTransport {
    inner: Arc<TcpClusterRelocationTransport>,
    captured: Mutex<Option<NativeOriginApplyRequest>>,
}
impl RangeWriteTransport for NormalTransport {
    fn apply_range_write_repair(
        &self,
        n: &ClusterNodeId,
        b: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.inner.apply_range_write_repair(n, b, limits)
    }
    fn fetch_range_write_repair(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        after: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.inner.fetch_range_write_repair(n, r, e, after, limits)
    }
    fn apply_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.apply_range_write(n, c)
    }
    fn apply_native_origin(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
        q: &NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        let wire = NativeOriginApplyRequest::from_certificate(c, q, parent)?;
        if c.index == 2 {
            *self.captured.lock() = Some(wire);
        }
        self.inner.apply_native_origin(n, c, q, parent)
    }
    fn observe_native_origin(
        &self,
        n: &ClusterNodeId,
        request: &crate::distribution_native_apply::NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        self.inner.observe_native_origin(n, request, parent)
    }
    fn recertify_native_origin(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<RangeWriteAck> {
        self.inner.recertify_native_origin(n, c, parent)
    }
    fn validate_native_command_pre_admission(
        &self,
        voters: &[ClusterNodeId],
        c: &RangeWriteCommand,
    ) -> Result<()> {
        self.inner.validate_native_command_pre_admission(voters, c)
    }
    fn validate_native_origin_pre_admission(
        &self,
        voters: &[ClusterNodeId],
        c: &RangeWriteCommand,
        intent: &crate::distribution_range_consensus::NativeRangeCommandIntent,
    ) -> Result<()> {
        self.inner
            .validate_native_origin_pre_admission(voters, c, intent)
    }
    fn range_write_progress(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
    ) -> Result<RangeWriteProgress> {
        self.inner.range_write_progress(n, r, e)
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
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.abort_range_write(n, c)
    }
}
fn run_actual_normal_tls() -> (NetworkFixture, Arc<NormalTransport>, Record) {
    // Actual command1 already ACKed through embedded real services, while
    // native instruction delivery was deliberately disabled. Both followers
    // therefore begin rawCQ1/native0. Every catch-up RPC below is actual mTLS.
    let fixture = NetworkFixture::new_network_mode(true, true, false, "n0", "n2", false);
    let transport = Arc::new(NormalTransport {
        inner: fixture.clients[&node("n0")].clone(),
        captured: Mutex::new(None),
    });
    let coor = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            fixture.current.read().cluster_id.clone(),
            node("n0"),
            fixture.current.clone(),
            fixture.native.services[&node("n0")].clone(),
            transport.clone(),
        )
        .unwrap(),
    );
    coor.recover_local_leadership(8).unwrap();
    fixture.native.databases[&node("n0")]
        .read()
        .install_commit_admission(coor);
    let mut expected =
        Record::new("actual-tls-historical").with_metadata(serde_json::json!({"revision": 2}));
    {
        let db = fixture.native.databases[&node("n0")].write();
        let mut tx = db.begin_transaction().unwrap();
        tx.update("items", expected.clone()).unwrap();
        tx.commit().unwrap();
    }
    let range = fixture.current.read().ranges.values().next().unwrap().id;
    for database in fixture.native.databases.values() {
        let database = database.read();
        assert_eq!(
            database
                .current_native_range_source_lineage(range)
                .unwrap()
                .range_index(),
            2
        );
        assert_eq!(
            *database.get("items", &expected.id).unwrap().unwrap(),
            expected
        );
    }
    expected.metadata = serde_json::json!({"revision": 3});
    {
        let db = fixture.native.databases[&node("n0")].write();
        let mut tx = db.begin_transaction().unwrap();
        tx.update("items", expected.clone()).unwrap();
        tx.commit().unwrap();
    }
    fixture.wait_idle();
    (fixture, transport, expected)
}
#[test]
fn native_normal_actual_mtls_catchup_then_two_acks_and_old_duplicate_preserves_new_value() {
    let (fixture, transport, expected) = run_actual_normal_tls();
    let request = transport.captured.lock().clone().unwrap();
    for name in ["n1", "n2"] {
        let result = fixture.clients[&node("n0")]
            .test_send_native_origin_request(
                &node(name),
                request.clone(),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(result.reply().native_index, 3);
        assert_eq!(result.reply().command_index, Some(2));
        let db = fixture.native.databases[&node(name)].read();
        assert_eq!(*db.get("items", &expected.id).unwrap().unwrap(), expected);
        assert_eq!(
            db.current_native_range_source_lineage(request.command.range_id)
                .unwrap()
                .range_index(),
            3
        );
    }
    fixture.wait_idle();
}
#[test]
fn native_normal_actual_mtls_wrong_actor_bootstrap_scope_epoch_refuse_before_native_mutation() {
    let (fixture, transport, expected) = run_actual_normal_tls();
    let request = transport.captured.lock().clone().unwrap();
    let target = node("n1");
    let before = fixture.native.databases[&target]
        .read()
        .current_native_range_source_lineage(request.command.range_id)
        .unwrap()
        .native_receipt_sha256()
        .to_owned();
    assert!(fixture.clients[&node("n2")]
        .test_send_native_origin_request(
            &target,
            request.clone(),
            &CancellationToken::uncancelable()
        )
        .is_err());
    let mut wrong_bootstrap = request.clone();
    wrong_bootstrap.bootstrap_id = Uuid::now_v7();
    assert!(fixture.clients[&node("n0")]
        .test_send_native_origin_request(
            &target,
            wrong_bootstrap,
            &CancellationToken::uncancelable()
        )
        .is_err());
    let mut wrong_scope = request.clone();
    wrong_scope.origin_scope_sha256 = "f".repeat(64);
    assert!(fixture.clients[&node("n0")]
        .test_send_native_origin_request(&target, wrong_scope, &CancellationToken::uncancelable())
        .is_err());
    let mut wrong_epoch = request.clone();
    wrong_epoch.command.range_epoch += 1;
    assert!(fixture.clients[&node("n0")]
        .test_send_native_origin_request(&target, wrong_epoch, &CancellationToken::uncancelable())
        .is_err());
    fixture.wait_idle();
    let db = fixture.native.databases[&target].read();
    assert_eq!(
        db.current_native_range_source_lineage(request.command.range_id)
            .unwrap()
            .native_receipt_sha256(),
        before
    );
    assert_eq!(*db.get("items", &expected.id).unwrap().unwrap(), expected);
}
