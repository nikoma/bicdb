//! PROCESS-owned hot DATA maintenance. No finite DataOpen/client call starts
//! this worker. Outside each bounded attempt the worker retains Weak only.
use super::*;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{
    RangeWriteCommand, RangeWriteCoordinator, RangeWriteState, RangeWriteTransport,
    RecoveryFenceScope,
};
use crate::distribution_range_recovery::{
    NativeCompletionRequest, NativeReadyRequest, RecoveryEpochFenceRequest,
    RecoveryHistoricalCommandRequest, RecoveryRoleAdmission,
};
use crate::distribution_transport::{HotRecoveryPeerFailure, HotRecoveryPhaseRequest};
use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::thread::JoinHandle;

const MAX_JOBS: usize = 64;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Fence,
    Certify,
    Apply,
    Prepare,
    Complete,
    Activate,
    Publish,
}
struct Job {
    scope: RecoveryFenceScope,
    command: RangeWriteCommand,
    range: crate::RangeDescriptor,
    members: Vec<ClusterNodeId>,
    phase: Phase,
    position: usize,
    // Durable successful per-recipient phase facts are scheduling only. Each
    // actual inner QQ and final recovered publisher still reprove authority.
    phase_observed: BTreeSet<ClusterNodeId>,
    semantic_veto: BTreeMap<ClusterNodeId, Phase>,
    peer_retry_after: BTreeMap<ClusterNodeId, Instant>,
}
#[derive(Default)]
struct State {
    jobs: BTreeMap<crate::RangeId, Job>,
    cursor: usize,
}
pub(super) struct NativeHotRecoveryMaintenance {
    initial: Arc<NativeInitialConsensusLaunch>,
    coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    host: Arc<crate::db::InstalledNativeWriterHostGuard>,
    runtime: Arc<RangeRecoveryRuntime>,
    admission: Arc<NativeInitialDataAdmission>,
    state: Mutex<State>,
    last: Mutex<Option<serde_json::Value>>,
    stopping: AtomicBool,
    process: OnceLock<CancellationToken>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for NativeHotRecoveryMaintenance {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}
impl NativeHotRecoveryMaintenance {
    pub(super) fn new(owner: &NativeInitialDataHost) -> Result<Arc<Self>> {
        let enrollment = owner
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("hot DATA owner has no original enrollment"))?;
        Ok(Arc::new(Self {
            initial: owner.initial.clone(),
            coordinator: enrollment.coordinator.clone(),
            host: enrollment.guard.clone(),
            runtime: owner
                .runtime
                .as_ref()
                .ok_or_else(|| invalid("hot DATA owner has no actual runtime"))?
                .clone(),
            admission: owner
                .admission
                .as_ref()
                .ok_or_else(|| invalid("hot DATA owner has no actual admission"))?
                .clone(),
            state: Mutex::new(State::default()),
            last: Mutex::new(None),
            stopping: AtomicBool::new(false),
            process: OnceLock::new(),
            worker: Mutex::new(None),
        }))
    }
    pub(super) fn note_start_error(&self, error: &crate::BicDbError) {
        let message = error.to_string();
        self.record(serde_json::json!({"phase":"start_refused","error":if message.len()<=4096{message}else{"diagnostic exceeds bound".into()},"diagnostic_not_authority":true}));
    }
    pub(super) fn request_shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(token) = self.process.get() {
            token.cancel();
        }
    }
    fn live(&self, token: &CancellationToken) -> Result<()> {
        token.check()?;
        if self.stopping.load(Ordering::Acquire) {
            return Err(invalid("hot DATA maintenance is stopping"));
        }
        self.admission
            .require_listener(&self.initial.service, &self.runtime)
    }
    pub(super) fn start(self: &Arc<Self>, process: &CancellationToken) -> Result<()> {
        self.live(process)?;
        let mut worker = self
            .worker
            .try_lock()
            .ok_or_else(|| invalid("hot DATA worker owner busy"))?;
        if worker.is_some() {
            return Ok(());
        }
        if self.process.get().is_none() {
            let _ = self.process.set(process.child());
        }
        let token = self.process.get().unwrap().clone();
        token.check()?;
        let weak = Arc::downgrade(self);
        *worker=Some(std::thread::Builder::new().name("native-hot-data-recovery".into()).spawn(move||{
            while token.check().is_ok() {
                let start=Instant::now();
                let Some(owner)=weak.upgrade() else {return;};
                if owner.stopping.load(Ordering::Acquire){return;}
                let action=token.child_with_deadline(Instant::now()+Duration::from_secs(5));
                let outcome=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||owner.tick(&action)));
                if outcome.is_err(){owner.record(serde_json::json!({"phase":"panic","scope":"owned exact job retained; no recovered grant","remaining_ms":0}));}
                drop(owner);
                while token.check().is_ok() && start.elapsed()<Duration::from_millis(500) {std::thread::sleep(Duration::from_millis(5));}
            }
        }).map_err(|e|invalid(&format!("hot DATA worker spawn failed: {e}")))?);
        self.live(process)
    }
    pub(super) fn try_join_finished(&self) -> bool {
        let Some(mut worker) = self.worker.try_lock() else {
            return false;
        };
        if worker.as_ref().is_some_and(|w| !w.is_finished()) {
            return false;
        }
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
        true
    }
    pub(super) fn stop(&self, parent: &CancellationToken) -> Result<()> {
        self.request_shutdown();
        while !self.try_join_finished() {
            parent.check()?;
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }
    pub(super) fn diagnostic(&self) -> Option<serde_json::Value> {
        self.last.try_lock().and_then(|v| v.clone())
    }
    fn record(&self, value: serde_json::Value) {
        #[cfg(test)]
        eprintln!("NATIVE_HOT_DATA_MAINTENANCE {value}");
        if let Some(mut last) = self.last.try_lock() {
            *last = Some(value);
        }
    }
    fn remaining(token: &CancellationToken) -> Result<u64> {
        token.check()?;
        token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .filter(|d| !d.is_zero())
            .map(|d| d.as_millis().clamp(1, 5000) as u64)
            .ok_or(crate::BicDbError::QueryTimedOut)
    }
    fn fresh_role(
        &self,
        range: &crate::RangeDescriptor,
        nonce: Uuid,
        token: &CancellationToken,
    ) -> Result<RecoveryRoleAdmission> {
        self.live(token)?;
        let actor = self.initial.transport.local_metadata_read_participant();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
        let witness = self.initial.provider.witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            token,
        )?;
        RecoveryRoleAdmission::from_witness(
            witness,
            &actor,
            &self.initial.seed.spec.node_id,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            &purpose,
        )
    }
    fn candidate(
        &self,
        range: &crate::RangeDescriptor,
        token: &CancellationToken,
    ) -> Result<Option<RangeWriteCommand>> {
        self.live(token)?;
        let service = self
            .initial
            .service
            .try_lock()
            .ok_or_else(|| invalid("hot candidate actual service busy"))?;
        let db = self
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("hot candidate actual DB busy"))?;
        self.host
            .require_for_coordinator(&db, range, &self.coordinator)?;
        let prefix = db.native_range_prefix_observation(range.id)?;
        let Some(index) = prefix.pending_command_index else {
            return Ok(None);
        };
        if prefix.invalidated
            || prefix.materialized_pending_index.is_some()
            || prefix.native_applied_index.and_then(|r| r.checked_add(1)) != Some(index)
        {
            return Err(invalid("hot original pre-WAL repair requires an actual next unmaterialized native Pending; other recovery remains unsupported"));
        }
        let entry = service
            .probe_range_write_for_peer(range, &self.initial.seed.spec.node_id, index)?
            .entry
            .ok_or_else(|| invalid("hot Pending has no retained actual command"))?;
        entry.command.validate()?;
        if !matches!(
            entry.state,
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied
        ) || prefix.pending_command_id.as_deref() != Some(entry.command.command_id.as_str())
            || entry.command.range_epoch != range.epoch
            || entry.command.leader_node_id != self.initial.seed.spec.node_id
        {
            return Err(invalid(
                "hot Pending is not this original same-epoch committed command",
            ));
        }
        self.live(token)?;
        Ok(Some(entry.command))
    }
    pub(super) fn tick(&self, parent: &CancellationToken) -> Result<()> {
        self.live(parent)?;
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("hot DATA maintenance attempt already active"))?;
        let ranges = self
            .initial
            .seed
            .spec
            .initial_ranges
            .iter()
            .filter(|r| r.leader == self.initial.seed.spec.node_id)
            .collect::<Vec<_>>();
        if ranges.is_empty() {
            return Ok(());
        }
        let range = (*ranges[state.cursor % ranges.len()]).clone();
        state.cursor = (state.cursor + 1) % ranges.len();
        if !state.jobs.contains_key(&range.id) {
            // SKIP ONLY: this cached scheduling hint never grants APP/Strong
            // or repairs a Pending. Actual Unknown release clears readiness;
            // retained jobs always continue through all fresh proof boundaries.
            if self
                .coordinator
                .initial_data_leader_admission_observed(&range)
            {
                return Ok(());
            }
            let Some(command) = self.candidate(&range, parent)? else {
                return Ok(());
            };
            if state.jobs.len() >= MAX_JOBS {
                return Err(invalid("hot DATA retained job bound exhausted"));
            }
            let role = self.fresh_role(&range, Uuid::now_v7(), parent)?;
            let scope = RecoveryFenceScope::from_role(&role)?;
            let mut members = role
                .member_credentials()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            // Actual original origin opens LAST; followers release first.
            members.sort_by_key(|n| *n == self.initial.seed.spec.node_id);
            state.jobs.insert(
                range.id,
                Job {
                    scope,
                    command,
                    range: range.clone(),
                    members,
                    phase: Phase::Fence,
                    position: 0,
                    phase_observed: BTreeSet::new(),
                    semantic_veto: BTreeMap::new(),
                    peer_retry_after: BTreeMap::new(),
                },
            );
        }
        let job = state.jobs.get_mut(&range.id).unwrap();
        let result = self.run_job(job, parent);
        let mut detail = serde_json::json!({"range_id":range.id,"epoch":job.range.epoch,"recovery_nonce":job.scope.recovery_nonce,
            "command_id":job.command.command_id,"command_sha256":job.command.checksum_sha256,"phase":format!("{:?}",job.phase),"position":job.position,
            "completed":result.is_ok(),"remaining_ms":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()),"diagnostic_not_authority":true});
        if let Err(error) = &result {
            let message = error.to_string();
            detail["error"] = serde_json::json!(if message.len() <= 4096 {
                message
            } else {
                "hot DATA error exceeds diagnostic bound".to_owned()
            });
        }
        self.record(detail);
        if result.is_ok() {
            state.jobs.remove(&range.id);
        }
        result
    }
    fn run_job(&self, job: &mut Job, parent: &CancellationToken) -> Result<()> {
        self.live(parent)?;
        let held = self.coordinator.reserve_hot_native_recovery(
            &self.runtime,
            &self.host,
            &job.range,
            parent,
        )?;
        // Reacquired actual role, never a cached topology/phase counter.
        let role = self.fresh_role(&job.range, job.scope.recovery_nonce, parent)?;
        job.scope.require_role(&role)?;
        let phase_parent = parent.child_with_deadline(role.deadline());
        loop {
            self.live(&phase_parent)?;
            role.require_live_request()?;
            held.require_actual_store_slot()?;
            if job.phase == Phase::Publish {
                if !job.semantic_veto.is_empty() {
                    return Err(invalid(
                        "hot recovery retains an unresolved actual peer semantic veto",
                    ));
                }
                let fresh = self.fresh_role(&job.range, job.scope.recovery_nonce, &phase_parent)?;
                self.coordinator.publish_hot_native_recovery(
                    &self.runtime,
                    &self.host,
                    &held,
                    &fresh,
                    &job.scope,
                    &job.command,
                    &phase_parent,
                )?;
                return Ok(());
            }
            if job.position == job.members.len() {
                // This threshold schedules the NEXT real producer. It is NOT
                // a QQ, role, recovered epoch or admission grant. Original
                // origin success and all known semantic vetoes are mandatory.
                let fresh = self.fresh_role(&job.range, job.scope.recovery_nonce, &phase_parent)?;
                job.scope.require_role(&fresh)?;
                held.require_actual_store_slot()?;
                if job.phase_observed.len() < job.members.len() / 2 + 1
                    || !job.phase_observed.contains(&self.initial.seed.spec.node_id)
                    || !job.semantic_veto.is_empty()
                {
                    job.position = 0;
                    return Err(invalid("hot native phase lacks actual original-origin and majority recipient observations"));
                }
                self.live(&phase_parent)?;
                fresh.require_live_request()?;
                job.position = 0;
                job.phase_observed.clear();
                job.phase = match job.phase {
                    Phase::Fence => Phase::Certify,
                    Phase::Certify => Phase::Apply,
                    Phase::Apply => Phase::Prepare,
                    Phase::Prepare => Phase::Complete,
                    Phase::Complete => Phase::Activate,
                    Phase::Activate => Phase::Publish,
                    Phase::Publish => unreachable!(),
                };
                continue;
            }
            let node = job.members[job.position].clone();
            let origin = node == self.initial.seed.spec.node_id;
            if job.phase_observed.contains(&node) && !job.semantic_veto.contains_key(&node) {
                job.position += 1;
                continue;
            }
            // A previous authenticated rejection cannot become skippable when
            // that same physical voter later disappears. Only a fresh actual
            // successful SAME-peer/SAME-phase observation clears this veto.
            if !origin
                && !job.semantic_veto.contains_key(&node)
                && job
                    .peer_retry_after
                    .get(&node)
                    .is_some_and(|d| Instant::now() < *d)
            {
                job.position += 1;
                continue;
            }
            let budget_ms = Self::remaining(&phase_parent)?;
            let io_ms = if origin {
                budget_ms
            } else if job.phase == Phase::Fence {
                budget_ms.min(500)
            } else {
                budget_ms.saturating_sub(250).max(1)
            };
            let request = match job.phase {
                Phase::Fence => HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                    scope: job.scope.clone(),
                    budget_ms,
                }),
                Phase::Certify | Phase::Apply => HotRecoveryPhaseRequest::Historical {
                    request: RecoveryHistoricalCommandRequest {
                        scope: job.scope.clone(),
                        command: job.command.clone(),
                        budget_ms,
                    },
                    apply: job.phase == Phase::Apply,
                },
                Phase::Prepare => HotRecoveryPhaseRequest::Prepare(NativeReadyRequest {
                    scope: job.scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms,
                }),
                Phase::Complete | Phase::Activate => HotRecoveryPhaseRequest::Complete {
                    request: NativeCompletionRequest {
                        scope: job.scope.clone(),
                        challenge: Uuid::now_v7(),
                        budget_ms,
                    },
                    activate: job.phase == Phase::Activate,
                },
                Phase::Publish => unreachable!(),
            };
            #[cfg(test)]
            eprintln!("NATIVE_HOT_DATA_PHASE node={} target={} phase={:?} command={} nonce={} remaining_ms={} subrequest_ms={}", self.initial.seed.spec.node_id,node,job.phase,job.command.command_id,job.scope.recovery_nonce,budget_ms,io_ms);
            let result = self.initial.transport.hot_recovery_phase_observation(
                &node,
                request,
                job.command.index,
                Duration::from_millis(io_ms),
                &phase_parent,
            );
            // Expired caller/role or changed original service slot aborts the
            // whole attempt regardless of any shorter peer socket timeout.
            self.live(&phase_parent)?;
            role.require_live_request()?;
            held.require_actual_store_slot()?;
            match result {
                Ok(_actual_receipt) => {
                    job.phase_observed.insert(node.clone());
                    job.semantic_veto.remove(&node);
                    job.peer_retry_after.remove(&node);
                }
                Err(error) => {
                    if let Some(peer) = HotRecoveryPeerFailure::from_error(&error) {
                        #[cfg(test)]
                        eprintln!("NATIVE_HOT_PEER_UNAVAILABLE node={} target={} phase={:?} kind={:?} may_have_dispatched={} count_success=false", self.initial.seed.spec.node_id,node,job.phase,peer.kind(),peer.may_have_dispatched());
                        if origin || job.semantic_veto.contains_key(&node) {
                            return Err(error);
                        }
                        // SKIP ONLY; never change current voters or a QQ. A
                        // persistent blackhole does not consume first budget
                        // again in every later phase. Bounded next probe.
                        job.peer_retry_after
                            .insert(node, Instant::now() + Duration::from_secs(10));
                    } else {
                        job.semantic_veto.insert(node, job.phase);
                        return Err(error);
                    }
                }
            }
            job.position += 1;
        }
    }
}
#[cfg(test)]
impl NativeHotRecoveryMaintenance {
    pub(super) fn started_for_test(&self) -> bool {
        self.process.get().is_some()
    }
}
#[cfg(test)]
impl NativeHotRecoveryMaintenance {
    pub(super) fn scope_for_test(
        &self,
        range: &crate::RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<RecoveryFenceScope> {
        RecoveryFenceScope::from_role(&self.fresh_role(range, Uuid::now_v7(), parent)?)
    }
}

/// Test scheduling hold ONLY. It owns the actual existing state mutex and
/// exposes no state, mutator, range reservation, role or permission. Rc marker
/// keeps it !Send/!Sync even if parking_lot feature selection changes.
#[cfg(test)]
pub(crate) struct NativeHotControllerTestHold<'a> {
    _held: parking_lot::MutexGuard<'a, State>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}
#[cfg(test)]
impl NativeHotRecoveryMaintenance {
    pub(super) fn hold_controller_for_test(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeHotControllerTestHold<'_>> {
        if parent.deadline().is_none() {
            return Err(invalid(
                "test controller hold needs a finite acquisition deadline",
            ));
        }
        loop {
            parent.check()?;
            if let Some(held) = self.state.try_lock() {
                parent.check()?;
                return Ok(NativeHotControllerTestHold {
                    _held: held,
                    _not_send: std::marker::PhantomData,
                });
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
