//! Genuine mTLS responses expire in their actual collector-child tokens.
//! No DTO/proof factory, artificial floor or modified scheduling policy.
use super::*;
use crate::distribution_range_recovery::{NativeReadyObservation, NativeReadyRequest};
use std::sync::mpsc;

#[derive(Debug)]
struct ExpiredReadyTransport {
    observed: ObservedTlsTransport,
    armed: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    entered: mpsc::Sender<usize>,
    attempts: Arc<Mutex<Vec<serde_json::Value>>>,
}
impl RangeWriteTransport for ExpiredReadyTransport {
    fn observe_native_ready_observation(
        &self,
        destination: &ClusterNodeId,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReadyObservation> {
        let actual =
            self.observed
                .observe_native_ready_observation(destination, request, parent)?;
        if destination != &node("n0") || !self.armed.load(Ordering::SeqCst) {
            return Ok(actual);
        }
        // Only AFTER the real server/transport/peer/challenge validator succeeds.
        parent.check()?;
        assert_eq!(actual.reply().range_index, 1);
        let attempt = {
            let mut attempts = self.attempts.lock();
            let attempt = attempts.len();
            attempts.push(serde_json::json!({
                "peer":actual.authenticated_peer().node_id(),
                "request":request,"actual_reply":actual.reply(),
                "remaining_ms_at_hold":parent.deadline().unwrap().saturating_duration_since(Instant::now()).as_millis(),
                "expired_return":false,
            }));
            attempt
        };
        self.entered.send(attempt).unwrap();
        // Both attempts retain THEIR SAME actual child token. Cleanup may
        // cancel that child on unwind; normal execution waits for expiry.
        loop {
            if self.abort.load(Ordering::SeqCst) {
                parent.cancel();
            }
            if let Err(error) = parent.check() {
                self.attempts.lock()[attempt]["expired_return"] =
                    serde_json::json!(matches!(error, BicDbError::QueryTimedOut));
                return Err(error);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn recovery_fenced_history_observation(
        &self,
        n: &ClusterNodeId,
        r: &RecoveryFencedProbeRequest,
        p: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        self.observed.recovery_fenced_history_observation(n, r, p)
    }
    fn range_write_progress(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
    ) -> Result<RangeWriteProgress> {
        self.observed.range_write_progress(n, r, e)
    }
    fn apply_range_write_repair(
        &self,
        n: &ClusterNodeId,
        b: &RangeWriteRepairBatch,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.observed.apply_range_write_repair(n, b, l)
    }
    fn fetch_range_write_repair(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        i: u64,
        l: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.observed.fetch_range_write_repair(n, r, e, i, l)
    }
    fn probe_range_write(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        i: u64,
    ) -> Result<RangeWriteProbe> {
        self.observed.probe_range_write(n, r, e, i)
    }
    fn prepare_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.observed.prepare_range_write(n, c)
    }
    fn commit_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.observed.commit_range_write(n, c)
    }
    fn certify_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.observed.certify_range_write(n, c)
    }
    fn apply_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.observed.apply_range_write(n, c)
    }
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.observed.abort_range_write(n, c)
    }
}
struct AbortHoldOnDrop(Arc<AtomicBool>);
impl Drop for AbortHoldOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

// Handler drain is required even if a new assertion unwinds. If bounded
// cleanup itself fails, retain this test-owned fixture rather than delete roots
// beneath live handlers; that is failure custody, not a successful cleanup.
struct DrainFixture(Option<NetworkFixture>);
impl std::ops::Deref for DrainFixture {
    type Target = NetworkFixture;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for DrainFixture {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}
impl Drop for DrainFixture {
    fn drop(&mut self) {
        let cleanup = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let mut drained = true;
        for server in &mut self.0.as_mut().unwrap().servers {
            if let Err(error) = server.stop_and_drain(&cleanup) {
                eprintln!("EXPIRED_READY_FIXTURE_DRAIN_FAILED {error}");
                drained = false;
            }
        }
        if !drained {
            std::mem::forget(self.0.take().unwrap());
        }
    }
}

// Actual sender-worker drain is separate from the business result/deadline.
// The fixture's wait_idle cannot see this independently owned local runtime.
fn wait_collector_idle(collector: &RangeRecoveryRuntime) {
    let until = Instant::now() + Duration::from_secs(2);
    while collector.test_active_context_count() != 0 {
        assert!(
            Instant::now() < until,
            "actual local collector operation failed to drain"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn snapshot(fixture: &NetworkFixture) -> BTreeMap<ClusterNodeId, serde_json::Value> {
    fixture.native.databases.iter().map(|(id, db)| {
        let db = db.read();
        let actual = db.current_native_range_source_lineage(fixture.command.range_id).unwrap();
        let mut rows = db.scan("items");
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        let value = serde_json::json!({
            "raw_catalogue":std::fs::read(db.data_path().join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG)).unwrap(),
            "R":actual.range_index(),"H0":actual.logical_genesis_sha256(),"H":actual.logical_history_sha256(),
            "B":actual.actual_local_visibility_sequence(),"namespace_uids":db.capture_range_image_bindings().unwrap().namespace_ids(),
            "database_incarnation":db.incarnation_id(),"commit_sequence":db.current_commit_seq(),"rows":rows,
        });
        (id.clone(), value)
    }).collect()
}

#[test]
fn native_ready_actual_tls_both_matching_peer_attempts_expire_without_counting_or_mutation() {
    let mut fixture = DrainFixture(Some(NetworkFixture::new_ready_network(true, true, true)));
    let (entered, holds) = mpsc::channel();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let armed = Arc::new(AtomicBool::new(false));
    let abort = Arc::new(AtomicBool::new(false));
    // This independent REAL local runtime starts BEFORE any recovery session.
    // The fixture n1 listener remains metadata-capable, with EMPTY recovery
    // sessions; only this runtime owns n1's actual local fence/Prepared/Frozen.
    let collector = Arc::new(
        RangeRecoveryRuntime::new(
            fixture.native.services[&node("n1")].clone(),
            fixture.providers[&node("n1")].clone(),
        )
        .with_participant_transport(Arc::new(ExpiredReadyTransport {
            observed: ObservedTlsTransport {
                inner: fixture.clients[&node("n1")].clone(),
                hold: Arc::new(ProbeHold::default()),
                ready_hold: Arc::new(ProbeHold::default()),
                read_hold: Arc::new(ProbeHold::default()),
                read_expired_once: Arc::new(AtomicBool::new(false)),
                read_n2_successful_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                lag_status_seen: fixture.lag_status_seen.clone(),
            },
            armed: armed.clone(),
            abort: abort.clone(),
            entered,
            attempts: attempts.clone(),
        })),
    );
    let actor = fixture.actor();
    let range = fixture
        .current
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let nonce = Uuid::now_v7();
    let setup = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let witness = fixture.providers[&node("n1")]
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &setup,
        )
        .unwrap();
    let role = RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        &node("n1"),
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap();
    let mut fences = RecoveryFenceQuorumRound::begin(role, &setup).unwrap();
    let scope = fences.scope().clone();
    let fence_request = RecoveryEpochFenceRequest {
        scope: scope.clone(),
        budget_ms: 4000,
    };
    for name in ["n0", "n2"] {
        fences
            .record(Arc::new(
                fixture.clients[&node("n1")]
                    .recovery_epoch_fence_observation(&node(name), &fence_request, &setup)
                    .unwrap(),
            ))
            .unwrap();
    }
    fences
        .record(Arc::new(
            collector
                .fence_epoch(&actor, &fence_request, &setup)
                .unwrap(),
        ))
        .unwrap();
    fences.finish().unwrap();
    fixture.wait_idle();
    fixture.call("n0", &scope, false).unwrap();
    collector
        .certify_historical(
            &actor,
            &fixture.request(&scope),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    wait_collector_idle(&collector);
    collector
        .apply_historical(
            &actor,
            &fixture.request(&scope),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    wait_collector_idle(&collector);
    for name in ["n0", "n2"] {
        fixture.prepare_ready(name, &scope).unwrap();
        fixture.wait_idle();
    }
    collector
        .prepare_native_ready(
            &actor,
            &fixture.ready_request(&scope),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    wait_collector_idle(&collector);
    assert_eq!(fixture.native_index("n0"), 1);
    assert_eq!(fixture.native_index("n1"), 1);
    assert_eq!(fixture.native_index("n2"), 0);
    assert_eq!(fixture.runtimes[&node("n1")].test_active_context_count(), 0);
    assert_eq!(collector.test_active_context_count(), 0);
    let before = snapshot(&fixture);
    assert_eq!(before[&node("n0")]["H0"], before[&node("n1")]["H0"]);
    assert_eq!(before[&node("n0")]["H"], before[&node("n1")]["H"]);
    assert_eq!(before[&node("n2")]["rows"], serde_json::json!([]));
    let lag = fixture.native.services[&node("n2")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n1"), 1)
        .unwrap();
    assert_eq!(lag.progress.last_index, 0);
    assert_eq!(lag.progress.resolved_through, 0);
    let freeze_owners = ["n0", "n2"].map(|name| {
        fixture.runtimes[&node(name)]
            .source_capture_freeze(&scope)
            .unwrap()
    });
    let local_freeze = collector.source_capture_freeze(&scope).unwrap();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    armed.store(true, Ordering::SeqCst);
    let request = fixture.ready_request(&scope);
    let error = std::thread::scope(|threads| {
        let _abort = AbortHoldOnDrop(abort.clone());
        let worker = threads.spawn(|| {
            let result = collector.acquire_closed_native_ready(&actor, &request, &parent);
            // The proof is inspected/dropped on its producing thread, never
            // returned as a fabricated Send authority across the test barrier.
            let error = match result {
                Ok(_) => panic!("expired R1 peer cannot form a closed majority with clean R0"),
                Err(error) => error,
            };
            parent
                .check()
                .expect("attempt exhaustion must leave original parent live");
            error.to_string()
        });
        for expected in 0..2 {
            assert_eq!(
                holds.recv_timeout(Duration::from_secs(3)).unwrap(),
                expected
            );
            parent.check().unwrap();
            assert_eq!(collector.test_active_context_count(), 1);
            // Matching peer has REALLY completed its authenticated receiver
            // operation; the delay holds only sender delivery, not engine latches.
            assert_eq!(fixture.runtimes[&node("n0")].test_active_context_count(), 0);
            for name in ["n0", "n1"] {
                assert!(fixture.native.services[&node(name)].try_lock().is_some());
                assert!(fixture.native.databases[&node(name)].try_write().is_some());
            }
        }
        worker.join().unwrap()
    });
    parent.check().unwrap();
    assert!(
        error.contains("closed current native-ready majority unavailable"),
        "{error}"
    );
    assert_eq!(
        attempts.lock().len(),
        2,
        "both ACTUAL matching peer responses must reach the hold"
    );
    assert!(attempts.lock().iter().all(|a| a["expired_return"] == true));
    assert!(
        fixture.lag_status_seen.load(Ordering::SeqCst),
        "real clean R0 minority must remain available"
    );
    fixture.wait_idle();
    wait_collector_idle(&collector);
    assert_eq!(
        snapshot(&fixture),
        before,
        "no raw/native/UID/B/row/floor mutation from unavailable collection"
    );
    for (name, original) in ["n0", "n2"].into_iter().zip(&freeze_owners) {
        let current = fixture.runtimes[&node(name)]
            .source_capture_freeze(&scope)
            .unwrap();
        assert!(Arc::ptr_eq(original, &current));
        fixture.native.databases[&node(name)]
            .read()
            .range_mutation_gate()
            .validate_quiescent_frozen(range.id, &current)
            .unwrap();
    }
    assert!(Arc::ptr_eq(
        &local_freeze,
        &collector.source_capture_freeze(&scope).unwrap()
    ));
    fixture.native.databases[&node("n1")]
        .read()
        .range_mutation_gate()
        .validate_quiescent_frozen(range.id, &local_freeze)
        .unwrap();
    eprintln!(
        "ACTUAL_EXPIRED_READY_DELIVERIES {}",
        serde_json::json!({"attempts":&*attempts.lock(),"error":error,"original_parent_live":true,"unchanged_state":true})
    );
    // Explicitly drain actual server handlers before fixture roots/keys end.
    let cleanup = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    for server in &mut fixture.servers {
        server.stop_and_drain(&cleanup).unwrap();
    }
}
