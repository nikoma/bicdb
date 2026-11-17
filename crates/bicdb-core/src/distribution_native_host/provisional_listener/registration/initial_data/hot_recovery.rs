//! PROCESS-owned hot DATA maintenance. No finite DataOpen/client call starts
//! this worker. Outside each bounded attempt the worker retains Weak only.
use super::*;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{
    NativeHotReplicaBinding, RangeWriteCommand, RangeWriteCoordinator, RangeWriteState,
    RangeWriteTransport, RecoveryFenceScope,
};
use crate::distribution_range_recovery::{
    NativeCompletionRequest, NativeReadyRequest, RecoveryEpochFenceRequest,
    RecoveryHistoricalCommandRequest, RecoveryRoleAdmission,
};
use crate::distribution_transport::{
    HotRecoveryPeerFailure, HotRecoveryPeerOpen, HotRecoveryPhaseRequest,
};
use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::thread::JoinHandle;

mod replica_debt;
#[cfg(test)]
pub(crate) use replica_debt::NativeHotDebtScheduleHold;
const MAX_JOBS: usize = 64;
/// Idle led ranges passed over by one attempt before it yields to pacing.
const MAX_IDLE_RANGE_SCAN: usize = 64;
/// A non-Fence peer subrequest shorter than this was truncated by THIS
/// sender's nearly exhausted attempt budget. Its socket timeout says nothing
/// about the peer, which answered the previous phase of the same job.
const MIN_UNSTARVED_PEER_IO_MS: u64 = 1000;
/// A phase producer is not started with less than this left in the attempt:
/// it could not finish, and the time it would burn is better spent by the
/// immediately following attempt, which resumes at the same retained phase.
const MIN_PHASE_START_MS: u64 = 1000;
/// Pacing between attempts. While every led range was last seen admitted and
/// nothing is retained, the attempt is a few memory reads, so a new actual
/// origin Pending is noticed quickly; any real work keeps the longer pacing.
const QUIET_PACING: Duration = Duration::from_millis(100);
const WORK_PACING: Duration = Duration::from_millis(500);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Fence,
    Certify,
    Apply,
    /// Abort of a Prepared-only slot: candidate, promises, resolution.
    AbortResolve,
    Prepare,
    Complete,
    Activate,
    Publish,
    Replica,
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
    recipient_next: BTreeMap<ClusterNodeId, Phase>,
    replica_binding: Option<NativeHotReplicaBinding>,
    peer_opens: BTreeMap<ClusterNodeId, HotRecoveryPeerOpen>,
    /// The command is aborted (Fence, AbortResolve, Prepare, ...) instead of
    /// recovered forward (Fence, Certify, Apply, Prepare, ...).
    abort: bool,
    /// Checksum of the origin's actual Core no-effect intent, once known.
    abort_candidate: Option<String>,
    /// A fresh session WITHOUT a command (Fence, Prepare, Complete, Activate)
    /// at the origin's fenced native point, for an admitted range one of
    /// whose members holds a durable activation that its current process
    /// never armed (`native_activation_dormant`): such a member refuses the
    /// range until a session activates it in its own open. Nothing is
    /// published: the leader's admission of the range is never withdrawn.
    rearm: bool,
    /// Re-arm only: each fenced member's clean native index (Fence reply).
    fenced: BTreeMap<ClusterNodeId, Option<u64>>,
    /// Diagnostic only: the first error each peer returned in each phase.
    first_peer_errors: BTreeMap<String, String>,
}
impl Job {
    fn note_peer_error(&mut self, node: &ClusterNodeId, error: &crate::BicDbError) {
        let key = format!("{node}:{:?}", self.phase);
        if self.first_peer_errors.len() < 16 && !self.first_peer_errors.contains_key(&key) {
            let message = error.to_string().chars().take(900).collect::<String>();
            self.first_peer_errors.insert(key, message);
        }
    }
}
#[derive(Default)]
struct State {
    jobs: BTreeMap<crate::RangeId, Job>,
    replica_debts: BTreeMap<crate::RangeId, replica_debt::ReplicaDebt>,
    cursor: usize,
}
pub(super) struct NativeHotRecoveryMaintenance {
    initial: Arc<NativeInitialConsensusLaunch>,
    coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    host: Arc<crate::db::InstalledNativeWriterHostGuard>,
    runtime: Arc<RangeRecoveryRuntime>,
    admission: Arc<NativeInitialDataAdmission>,
    state: Mutex<State>,
    #[cfg(test)]
    debt_schedule_test: Mutex<()>,
    last: Mutex<Option<serde_json::Value>>,
    // Scheduling hint only: the last attempt found every scanned led range
    // admitted and had no retained job, candidate or replica obligation.
    quiet: AtomicBool,
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
            #[cfg(test)]
            debt_schedule_test: Mutex::new(()),
            last: Mutex::new(None),
            quiet: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            process: OnceLock::new(),
            worker: Mutex::new(None),
        }))
    }
    pub(super) fn note_start_error(&self, error: &crate::BicDbError) {
        let message = error.to_string();
        self.record(serde_json::json!({"phase":"start_refused","error":if message.len()<=4096{message}else{"diagnostic exceeds bound".into()},"diagnostic_not_authority":true}));
    }
    /// True once a process token was bound; the worker may hold the original
    /// writer objects from then on.
    pub(super) fn process_bound(&self) -> bool {
        self.process.get().is_some()
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
                let pacing=if owner.quiet.load(Ordering::Acquire){QUIET_PACING}else{WORK_PACING};
                drop(owner);
                while token.check().is_ok() && start.elapsed()<pacing {std::thread::sleep(Duration::from_millis(5));}
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
    /// The command to recover and whether it is aborted (true) or recovered
    /// forward (false).
    fn candidate(
        &self,
        range: &crate::RangeDescriptor,
        token: &CancellationToken,
    ) -> Result<Option<(RangeWriteCommand, bool)>> {
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
            if let Some(command) = self.reopened_committed_candidate(&service, &prefix, range)? {
                return Ok(Some((command, false)));
            }
            return Ok(self
                .reopened_prepared_candidate(&service, &db, &prefix, range)?
                .map(|command| (command, true)));
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
        Ok(Some((entry.command, false)))
    }
    /// REOPENED owner only. The earlier process of this node ended after its
    /// own raw log recorded the next command with its own Commit vote or a
    /// commit certificate (Committed/QuorumCommitted/Applied) and before it
    /// wrote the native Pending, so
    /// the native prefix ends just below that command. That command is
    /// never aborted; it may only be recovered forward, which the session's
    /// Certify phase decides from fenced member votes or refuses. It is
    /// proposed to the SAME fenced session as a Pending one (Fence, Certify,
    /// Apply, Prepare, Complete, Activate, Publish), in which every member
    /// re-proves the command from its own durable state. Every other state
    /// (materialized or invalidated prefix, a gap) stays
    /// unsupported and is not proposed.
    fn reopened_committed_candidate(
        &self,
        service: &crate::ClusterDataNodeService,
        prefix: &crate::db::NativeRangePrefixObservation,
        range: &crate::RangeDescriptor,
    ) -> Result<Option<RangeWriteCommand>> {
        if !self.initial.reopened()
            || prefix.invalidated
            || prefix.materialized_pending_index.is_some()
        {
            return Ok(None);
        }
        let Some(index) = prefix.native_applied_index.and_then(|r| r.checked_add(1)) else {
            return Ok(None);
        };
        let progress = service.range_write_progress(range.id);
        if progress.resolved_through.checked_add(1) != Some(index) || progress.last_index < index {
            return Ok(None);
        }
        let Some(entry) = service
            .probe_range_write_for_peer(range, &self.initial.seed.spec.node_id, index)?
            .entry
        else {
            return Ok(None);
        };
        entry.command.validate()?;
        // A Committed (uncertified) own vote is proposed too: the session's
        // Certify phase decides it only from a fenced member quorum holding
        // a certificate or Commit votes (`HistoricalCommandDecisionRound`),
        // and refuses otherwise; it is never aborted (its slot is ever-Commit).
        if !matches!(
            entry.state,
            RangeWriteState::Committed
                | RangeWriteState::QuorumCommitted
                | RangeWriteState::Applied
        ) || entry.command.index != index
            || entry.command.range_id != range.id
            || entry.command.range_epoch != range.epoch
            || entry.command.leader_node_id != self.initial.seed.spec.node_id
        {
            return Ok(None);
        }
        Ok(Some(entry.command))
    }
    /// REOPENED owner only. The earlier process of this node ended after its
    /// own raw log recorded the next command as Prepared and before its own
    /// Commit vote: no member can have committed it (the origin durably marks
    /// its slot ever-Commit before sending Commit to anyone) and Core wrote
    /// nothing for it (application WAL follows the admission ticket, which
    /// follows the local Commit). It is aborted through the existing abort
    /// protocol, whose promise quorum re-proves the absence of any Commit at
    /// every member. Here only the raw shape is selected; the Core producer
    /// of the no-effect intent re-proves the protected slot itself.
    fn reopened_prepared_candidate(
        &self,
        service: &crate::ClusterDataNodeService,
        db: &crate::BicDb,
        prefix: &crate::db::NativeRangePrefixObservation,
        range: &crate::RangeDescriptor,
    ) -> Result<Option<RangeWriteCommand>> {
        if !self.initial.reopened()
            || prefix.invalidated
            || prefix.materialized_pending_index.is_some()
        {
            return Ok(None);
        }
        let Some(index) = prefix.native_applied_index.and_then(|r| r.checked_add(1)) else {
            return Ok(None);
        };
        let progress = service.range_write_progress(range.id);
        if progress.resolved_through.checked_add(1) != Some(index) {
            return Ok(None);
        }
        let command = if progress.last_index == index {
            let Some(entry) = service
                .probe_range_write_for_peer(range, &self.initial.seed.spec.node_id, index)?
                .entry
            else {
                return Ok(None);
            };
            if entry.state != RangeWriteState::Prepared {
                return Ok(None);
            }
            entry.command
        } else if progress.last_index.checked_add(1) == Some(index) {
            // Ended between the writer-start and the local Prepare: the
            // command exists only in the protected slot.
            let Some(command) = db.native_abort_slot_command(range.id, index)? else {
                return Ok(None);
            };
            command
        } else {
            return Ok(None);
        };
        command.validate()?;
        if command.index != index
            || command.range_id != range.id
            || command.range_epoch != range.epoch
            || command.leader_node_id != self.initial.seed.spec.node_id
        {
            return Ok(None);
        }
        Ok(Some(command))
    }
    /// An admitted led range needs a fresh re-arm session when one of its
    /// members holds a durable activation that its current process never
    /// armed: this node itself (`native_activation_dormant`, read locally),
    /// or a follower whose re-observation of the converged session refused
    /// for exactly that reason. A retained, not converged session is never
    /// replaced (its physical obligations stay with it). Scheduling only.
    fn rearm_wanted(&self, state: &State, range: &crate::RangeDescriptor) -> bool {
        match state.replica_debts.get(&range.id) {
            Some(debt) if !debt.converged() => return false,
            Some(debt) if debt.rearm_requested() => return true,
            _ => {}
        }
        self.initial
            .service
            .try_lock()
            .is_some_and(|service| service.native_activation_dormant(range.id))
    }
    /// The re-arm session's provisional anchor: this origin's own Applied
    /// command at its native point, when its raw log is resolved exactly
    /// there with nothing pending. The anchor is fixed again from the fenced
    /// origin (`anchor_rearm`); every phase reproves the point itself.
    fn rearm_candidate(
        &self,
        range: &crate::RangeDescriptor,
        token: &CancellationToken,
    ) -> Result<Option<RangeWriteCommand>> {
        self.live(token)?;
        let service = self
            .initial
            .service
            .try_lock()
            .ok_or_else(|| invalid("hot re-arm candidate actual service busy"))?;
        let db = self
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("hot re-arm candidate actual DB busy"))?;
        self.host
            .require_for_coordinator(&db, range, &self.coordinator)?;
        let prefix = db.native_range_prefix_observation(range.id)?;
        let Some(index) = prefix.native_applied_index else {
            return Ok(None);
        };
        let progress = service.range_write_progress(range.id);
        if prefix.invalidated
            || prefix.pending_command_index.is_some()
            || prefix.materialized_pending_index.is_some()
            || progress.resolved_through != index
            || progress.last_index != index
        {
            return Ok(None);
        }
        self.own_applied_command(&service, range, index)
    }
    fn own_applied_command(
        &self,
        service: &crate::ClusterDataNodeService,
        range: &crate::RangeDescriptor,
        index: u64,
    ) -> Result<Option<RangeWriteCommand>> {
        let Some(entry) = service
            .probe_range_write_for_peer(range, &self.initial.seed.spec.node_id, index)?
            .entry
        else {
            return Ok(None);
        };
        entry.command.validate()?;
        if entry.state != RangeWriteState::Applied
            || entry.command.index != index
            || entry.command.range_id != range.id
            || entry.command.range_epoch != range.epoch
        {
            return Ok(None);
        }
        Ok(Some(entry.command))
    }
    /// After the re-arm Fence: the session's point is the FENCED origin's
    /// clean native index (no admission can move it any more). A follower
    /// fenced at another point is not sent this session's Prepare (it would
    /// refuse it); it is continued as replica debt through the completed
    /// materialization of the anchor command, like any follower that missed
    /// the session's early phases.
    fn anchor_rearm(&self, job: &mut Job) -> Result<()> {
        let origin = &self.initial.seed.spec.node_id;
        let index = job.fenced.get(origin).copied().flatten().ok_or_else(|| {
            invalid("hot re-arm: the fenced origin is not at a clean native point")
        })?;
        if index != job.command.index {
            let service = self
                .initial
                .service
                .try_lock()
                .ok_or_else(|| invalid("hot re-arm anchor actual service busy"))?;
            job.command = self
                .own_applied_command(&service, &job.range, index)?
                .ok_or_else(|| {
                    invalid("hot re-arm: the fenced origin point is not its own Applied command")
                })?;
        }
        for (node, fenced) in &job.fenced {
            if node != origin
                && *fenced != Some(index)
                && job.recipient_next.get(node) == Some(&Phase::Prepare)
            {
                job.recipient_next.insert(node.clone(), Phase::Certify);
            }
        }
        Ok(())
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
        // Retained jobs and new actual origin Pendings come first: idle led
        // ranges are passed over (memory/local reads only) so such a range
        // is not delayed by one pacing interval per idle range. Retained
        // replica obligations keep their previous one-range-per-attempt
        // cadence below.
        let first = state.cursor % ranges.len();
        let mut selected = None;
        // Any path below that does or may do real work clears this first.
        self.quiet.store(false, Ordering::Release);
        let mut quiet = ranges.len() <= MAX_IDLE_RANGE_SCAN;
        for offset in 0..ranges.len().min(MAX_IDLE_RANGE_SCAN) {
            let at = (first + offset) % ranges.len();
            let range = (*ranges[at]).clone();
            if state.jobs.contains_key(&range.id) {
                state.cursor = (at + 1) % ranges.len();
                selected = Some(range);
                break;
            }
            // SKIP ONLY: this cached scheduling hint never grants APP/Strong
            // or repairs a Pending. Actual Unknown release clears readiness;
            // retained jobs always continue through all fresh proof boundaries.
            // An admitted range is passed over unless one of its members
            // holds a durable activation its current process never armed.
            let rearm = self
                .coordinator
                .initial_data_leader_admission_observed(&range);
            if rearm && !self.rearm_wanted(&state, &range) {
                continue;
            }
            // Do not discard an older session's frozen/uncaught-up physical
            // obligations when another origin failure appears. A separate
            // completed-decision/new-session dominance producer is required.
            // A session whose every follower was last observed activated and
            // at the actual source cut, with no retained veto, has no such
            // obligation left: only that one may be retired below.
            quiet = false;
            if state
                .replica_debts
                .get(&range.id)
                .is_some_and(|debt| !debt.converged())
            {
                state.cursor = (at + 1) % ranges.len();
                return Err(invalid("new origin recovery with retained replica session debt needs explicit continuation; old debt is preserved"));
            }
            let candidate = if rearm {
                self.rearm_candidate(&range, parent)
                    .map(|command| command.map(|command| (command, false)))
            } else {
                self.candidate(&range, parent)
            };
            let (command, abort) = match candidate {
                Ok(Some(candidate)) => candidate,
                Ok(None) => continue,
                Err(error) => {
                    state.cursor = (at + 1) % ranges.len();
                    self.record(serde_json::json!({"phase":"candidate_refused","range_id":range.id,"rearm":rearm,
                        "error":error.to_string().chars().take(4096).collect::<String>(),"diagnostic_not_authority":true}));
                    return Err(error);
                }
            };
            state.cursor = (at + 1) % ranges.len();
            if let Some(retired) = state.replica_debts.remove(&range.id) {
                self.record(
                    retired.diagnostic("converged_session_retired_for_new_actual_origin_pending"),
                );
            }
            if state.jobs.len() + state.replica_debts.len() >= MAX_JOBS {
                return Err(invalid("hot DATA retained job bound exhausted"));
            }
            // The abort candidate is produced BEFORE the fence: a recovery
            // fence freezes the range's mutation gate, and the producer's
            // write carries an ordinary counted lease. No job exists until
            // the actual durable intent does.
            let abort_candidate = if abort {
                let mut service = self
                    .initial
                    .service
                    .try_lock()
                    .ok_or_else(|| invalid("hot abort candidate actual service busy"))?;
                match service.reopened_native_abort_candidate(&range, &command) {
                    Ok(candidate) => Some(candidate),
                    Err(error) => {
                        drop(service);
                        self.record(serde_json::json!({"phase":"abort_candidate_refused","range_id":range.id,"abort":true,
                            "command_id":command.command_id,"error":error.to_string().chars().take(4096).collect::<String>(),"diagnostic_not_authority":true}));
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let role = self.fresh_role(&range, Uuid::now_v7(), parent)?;
            let scope = RecoveryFenceScope::from_role(&role)?;
            let mut members = role
                .member_credentials()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            // Actual original origin opens LAST; followers release first.
            members.sort_by_key(|n| *n == self.initial.seed.spec.node_id);
            let recipient_next = members.iter().cloned().map(|n| (n, Phase::Fence)).collect();
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
                    recipient_next,
                    replica_binding: None,
                    peer_opens: BTreeMap::new(),
                    abort,
                    abort_candidate,
                    rearm,
                    fenced: BTreeMap::new(),
                    first_peer_errors: BTreeMap::new(),
                },
            );
            selected = Some(range);
            break;
        }
        let Some(range) = selected else {
            let range = ranges[first];
            state.cursor = (first + 1) % ranges.len();
            if self
                .coordinator
                .initial_data_leader_admission_observed(range)
            {
                if let Some(debt) = state.replica_debts.get_mut(&range.id) {
                    return self.run_replica_debt(debt, parent);
                }
            }
            if quiet && state.replica_debts.is_empty() {
                self.quiet.store(true, Ordering::Release);
            }
            return Ok(());
        };
        let job = state.jobs.get_mut(&range.id).unwrap();
        let result = self.run_job(job, parent);
        let mut detail = serde_json::json!({"range_id":range.id,"epoch":job.range.epoch,"abort":job.abort,"rearm":job.rearm,"recovery_nonce":job.scope.recovery_nonce,
            "command_id":job.command.command_id,"command_sha256":job.command.checksum_sha256,"phase":format!("{:?}",job.phase),"position":job.position,
            "completed":result.is_ok(),"phase_observed":job.phase_observed,"semantic_veto":job.semantic_veto.iter().map(|(n,p)|(n.to_string(),format!("{p:?}"))).collect::<BTreeMap<_,_>>(),"remaining_ms":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()),"first_peer_errors":job.first_peer_errors,"diagnostic_not_authority":true});
        if let Err(error) = &result {
            let message = error.to_string();
            detail["error"] = serde_json::json!(if message.len() <= 4096 {
                message
            } else {
                "hot DATA error exceeds diagnostic bound".to_owned()
            });
        }
        self.record(detail);
        if result.is_ok() && job.abort {
            let mut done = state
                .jobs
                .remove(&range.id)
                .expect("actual abort job retained");
            let not_activated = done
                .recipient_next
                .keys()
                .filter(|n| **n != done.range.leader)
                .cloned()
                .collect::<Vec<_>>();
            self.record(serde_json::json!({"phase":"abort_activated_ordinary_leadership_recovery_next","range_id":range.id,"abort":true,
                "command_id":done.command.command_id,"followers_not_activated":not_activated,"diagnostic_not_authority":true}));
            // Tracking only: the activated members are re-observed like
            // those of a published session, so that one restarted later is
            // re-armed. The aborted command is never materialized at a
            // follower: no follower is left owing a phase of this session.
            // One that was not activated in it holds no activation of this
            // open either; it is re-armed by a fresh session once the range
            // is admitted again.
            done.recipient_next.clear();
            let mut debt = replica_debt::ReplicaDebt::from_published(done);
            if !not_activated.is_empty() {
                debt.request_rearm();
            }
            state.replica_debts.insert(range.id, debt);
            return Ok(());
        }
        if result.is_ok() {
            let published = state
                .jobs
                .remove(&range.id)
                .expect("actual published job retained");
            // Allocation/data transfer only, after actual quorum publication.
            // Never erase unfinished peer obligations with the ready job.
            state.replica_debts.insert(
                range.id,
                replica_debt::ReplicaDebt::from_published(published),
            );
            self.record(state.replica_debts[&range.id].diagnostic("queued_after_actual_publish"));
        }
        result
    }
    fn phase_request(job: &Job, budget_ms: u64) -> HotRecoveryPhaseRequest {
        match job.phase {
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
            Phase::AbortResolve | Phase::Publish | Phase::Replica => unreachable!(),
        }
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
        // Per attempt and phase: a transport cooldown may defer a peer only
        // while the origin plus a majority can be observed without it.
        let mut cooldown_probe = false;
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
                if Self::remaining(&phase_parent)? < MIN_PHASE_START_MS {
                    return Err(crate::BicDbError::QueryTimedOut);
                }
                let fresh = self.fresh_role(&job.range, job.scope.recovery_nonce, &phase_parent)?;
                if job.replica_binding.is_none() {
                    job.replica_binding = Some(self.coordinator.capture_hot_replica_binding(
                        &self.runtime,
                        &self.host,
                        &held,
                        &fresh,
                        &phase_parent,
                    )?);
                }
                self.coordinator.require_hot_replica_binding(
                    &self.runtime,
                    &self.host,
                    job.replica_binding.as_ref().unwrap(),
                    &fresh,
                    &phase_parent,
                )?;
                if job.rearm {
                    // The members are activated in their current opens; the
                    // range was admitted before and its admission is kept.
                    // The binding continues the session as replica debt.
                    return Ok(());
                }
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
            if job.phase == Phase::AbortResolve {
                self.run_abort_resolve(job, &phase_parent)?;
                let fresh = self.fresh_role(&job.range, job.scope.recovery_nonce, &phase_parent)?;
                job.scope.require_role(&fresh)?;
                held.require_actual_store_slot()?;
                fresh.require_live_request()?;
                job.position = 0;
                cooldown_probe = false;
                job.phase_observed.clear();
                job.phase = Phase::Prepare;
                continue;
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
                    // SKIP HINT ONLY: the cooldown exists to avoid paying a
                    // silent minority's timeout again. It must never hold back
                    // the very peer whose real answer the majority needs.
                    // Probe each still-eligible deferred peer once more now;
                    // only its actual successful observation is counted.
                    let now = Instant::now();
                    if !cooldown_probe
                        && job.semantic_veto.is_empty()
                        && job.members.iter().any(|n| {
                            *n != self.initial.seed.spec.node_id
                                && !job.phase_observed.contains(n)
                                && job.recipient_next.get(n) == Some(&job.phase)
                                && job.peer_retry_after.get(n).is_some_and(|d| now < *d)
                        })
                    {
                        cooldown_probe = true;
                        continue;
                    }
                    return Err(invalid("hot native phase lacks actual original-origin and majority recipient observations"));
                }
                self.live(&phase_parent)?;
                fresh.require_live_request()?;
                if job.abort && job.phase == Phase::Activate {
                    // The hot publication proves an APPLIED command; an
                    // aborted slot is published by the ordinary leadership
                    // recovery over the activated session, as after any
                    // completed abort. Nothing is granted here. The binding
                    // only lets the session's members be tracked afterwards.
                    if job.replica_binding.is_none() {
                        job.replica_binding = Some(self.coordinator.capture_hot_replica_binding(
                            &self.runtime,
                            &self.host,
                            &held,
                            &fresh,
                            &phase_parent,
                        )?);
                    }
                    return Ok(());
                }
                if job.rearm && job.phase == Phase::Fence {
                    self.anchor_rearm(job)?;
                }
                job.position = 0;
                cooldown_probe = false;
                job.phase_observed.clear();
                job.phase = match job.phase {
                    Phase::Fence if job.abort => Phase::AbortResolve,
                    Phase::Fence if job.rearm => Phase::Prepare,
                    Phase::Fence => Phase::Certify,
                    Phase::Certify => Phase::Apply,
                    Phase::Apply => Phase::Prepare,
                    Phase::Prepare => Phase::Complete,
                    Phase::Complete => Phase::Activate,
                    Phase::Activate => Phase::Publish,
                    Phase::AbortResolve | Phase::Publish | Phase::Replica => unreachable!(),
                };
                continue;
            }
            let node = job.members[job.position].clone();
            let origin = node == self.initial.seed.spec.node_id;
            if job.phase_observed.contains(&node) && !job.semantic_veto.contains_key(&node) {
                job.position += 1;
                continue;
            }
            // A follower without an actual successful observation of every
            // EARLIER phase of this session is never sent the current phase:
            // its receiver would refuse the out-of-order request, and that
            // refusal would become a permanent veto no later same-phase
            // success could clear. It is not counted here; its retained
            // per-recipient next phase is continued as replica debt after the
            // origin-plus-majority publication.
            if !origin
                && !job.semantic_veto.contains_key(&node)
                && job.recipient_next.get(&node) != Some(&job.phase)
            {
                job.position += 1;
                continue;
            }
            // A previous authenticated rejection cannot become skippable when
            // that same physical voter later disappears. Only a fresh actual
            // successful SAME-peer/SAME-phase observation clears this veto.
            let deferred = !origin
                && !job.semantic_veto.contains_key(&node)
                && job
                    .peer_retry_after
                    .get(&node)
                    .is_some_and(|d| Instant::now() < *d);
            if deferred && !cooldown_probe {
                job.position += 1;
                continue;
            }
            let budget_ms = Self::remaining(&phase_parent)?;
            if budget_ms < MIN_PHASE_START_MS {
                // Not a peer statement and not progress: resume this same
                // retained phase/recipient in the next attempt.
                return Err(crate::BicDbError::QueryTimedOut);
            }
            let io_ms = if origin {
                budget_ms
            } else if job.phase == Phase::Fence {
                budget_ms.min(500)
            } else if deferred {
                // Bounded re-probe: leave budget for the other deferred peer
                // and for the mandatory original origin.
                (budget_ms / 2).max(1)
            } else {
                budget_ms.saturating_sub(250).max(1)
            };
            let request = Self::phase_request(job, budget_ms);
            #[cfg(test)]
            eprintln!("NATIVE_HOT_DATA_PHASE node={} target={} phase={:?} command={} nonce={} remaining_ms={} subrequest_ms={}", self.initial.seed.spec.node_id,node,job.phase,job.command.command_id,job.scope.recovery_nonce,budget_ms,io_ms);
            let result = self
                .initial
                .transport
                .hot_recovery_phase_observation(
                    &node,
                    request,
                    job.command.index,
                    Duration::from_millis(io_ms),
                    &phase_parent,
                )
                .map(|receipt| {
                    (
                        receipt.observed_open().clone(),
                        receipt.fenced_clean_native_index(),
                    )
                });
            // Expired caller/role or changed original service slot aborts the
            // whole attempt regardless of any shorter peer socket timeout.
            self.live(&phase_parent)?;
            role.require_live_request()?;
            held.require_actual_store_slot()?;
            match result {
                Ok((actual_open, fenced_index)) => {
                    if let Some(original) = job.peer_opens.get(&node) {
                        if let Err(error) = original.require_same(&actual_open) {
                            job.semantic_veto.insert(node.clone(), job.phase);
                            return Err(error);
                        }
                    } else {
                        job.peer_opens.insert(node.clone(), actual_open);
                    }
                    job.phase_observed.insert(node.clone());
                    match job.phase {
                        Phase::Fence => {
                            let next = if job.abort {
                                Phase::AbortResolve
                            } else if job.rearm {
                                job.fenced.insert(node.clone(), fenced_index);
                                Phase::Prepare
                            } else {
                                Phase::Certify
                            };
                            job.recipient_next.insert(node.clone(), next);
                        }
                        Phase::Certify => {
                            job.recipient_next.insert(node.clone(), Phase::Apply);
                        }
                        Phase::Apply => {
                            job.recipient_next.insert(node.clone(), Phase::Prepare);
                        }
                        Phase::Prepare => {
                            job.recipient_next.insert(node.clone(), Phase::Complete);
                        }
                        Phase::Complete => {
                            job.recipient_next.insert(node.clone(), Phase::Activate);
                        }
                        Phase::Activate => {
                            job.recipient_next.remove(&node);
                        }
                        Phase::AbortResolve | Phase::Publish | Phase::Replica => unreachable!(),
                    }
                    job.semantic_veto.remove(&node);
                    job.peer_retry_after.remove(&node);
                }
                Err(error) => {
                    job.note_peer_error(&node, &error);
                    if let Some(peer) = HotRecoveryPeerFailure::from_error(&error) {
                        #[cfg(test)]
                        eprintln!("NATIVE_HOT_PEER_UNAVAILABLE node={} target={} phase={:?} kind={:?} may_have_dispatched={} count_success=false", self.initial.seed.spec.node_id,node,job.phase,peer.kind(),peer.may_have_dispatched());
                        if origin || job.semantic_veto.contains_key(&node) {
                            return Err(error);
                        }
                        let timed_out = matches!(
                            peer.kind(),
                            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                        );
                        // This sender's own nearly exhausted attempt budget
                        // truncated the subrequest. End the attempt without
                        // success, veto or cooldown; the next attempt asks the
                        // same peer again with a whole budget.
                        if timed_out
                            && job.phase != Phase::Fence
                            && io_ms < MIN_UNSTARVED_PEER_IO_MS
                        {
                            return Err(error);
                        }
                        // SKIP ONLY; never change current voters or a QQ. A
                        // persistent blackhole does not consume first budget
                        // again in every later phase. Bounded next probe.
                        let until = Instant::now() + Duration::from_secs(10);
                        if timed_out {
                            // Ordering hint for ordinary admission after the
                            // publication; it grants and counts nothing.
                            self.coordinator.note_native_voter_stalled(&node, until);
                        }
                        job.peer_retry_after.insert(node, until);
                    } else {
                        // Sender-local cancellation/expiry is not an
                        // authenticated semantic statement by this voter.
                        // It still aborts this attempt and never enables skip
                        // or successful phase progress. Remote timeout/busy
                        // replies remain generic authenticated Cluster errors
                        // and conservatively retain their actual-peer veto.
                        if !matches!(
                            error,
                            crate::BicDbError::QueryCanceled | crate::BicDbError::QueryTimedOut
                        ) {
                            job.semantic_veto.insert(node, job.phase);
                        }
                        return Err(error);
                    }
                }
            }
            job.position += 1;
        }
    }
}
impl NativeHotRecoveryMaintenance {
    /// The abort of a Prepared-only slot, with the existing requests
    /// unchanged: the origin's own Core no-effect intent (the candidate), then
    /// resolution at the origin FIRST, which creates the permanent promises of
    /// a member quorum (each member refuses with Irreversible on any Commit
    /// evidence), then resolution at each fenced follower, which collects
    /// those promises itself. Requires the origin plus a majority.
    fn run_abort_resolve(&self, job: &mut Job, parent: &CancellationToken) -> Result<()> {
        let origin = self.initial.seed.spec.node_id.clone();
        if job.abort_candidate.is_none() {
            self.live(parent)?;
            let mut service = self
                .initial
                .service
                .try_lock()
                .ok_or_else(|| invalid("hot abort candidate actual service busy"))?;
            job.abort_candidate =
                Some(service.reopened_native_abort_candidate(&job.range, &job.command)?);
        }
        let order = std::iter::once(origin.clone())
            .chain(
                job.members
                    .iter()
                    .filter(|n| {
                        **n != origin && job.recipient_next.get(*n) == Some(&Phase::AbortResolve)
                    })
                    .cloned(),
            )
            .collect::<Vec<_>>();
        let mut failures = BTreeMap::new();
        for node in order {
            if job.phase_observed.contains(&node) {
                continue;
            }
            let budget_ms = Self::remaining(parent)?;
            if budget_ms < MIN_PHASE_START_MS {
                return Err(crate::BicDbError::QueryTimedOut);
            }
            let request = crate::distribution_range_recovery::NativeAbortRequest {
                scope: job.scope.clone(),
                range_index: job.command.index,
                origin_node_id: origin.clone(),
                candidate_sha256: job.abort_candidate.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: budget_ms.min(4500),
            };
            #[cfg(test)]
            eprintln!("NATIVE_HOT_DATA_PHASE node={origin} target={node} phase=AbortResolve command={} nonce={} remaining_ms={budget_ms}", job.command.command_id, job.scope.recovery_nonce);
            match self
                .initial
                .transport
                .resolve_native_abort_at(&node, &request, parent)
            {
                Ok((index, _, _)) if index == job.command.index => {
                    job.phase_observed.insert(node.clone());
                    job.recipient_next.insert(node.clone(), Phase::Prepare);
                    job.semantic_veto.remove(&node);
                }
                Ok((index, _, _)) => {
                    return Err(invalid(&format!(
                        "hot abort resolution at {node} returned index {index}"
                    )));
                }
                Err(error) if node == origin => {
                    let message = error.to_string();
                    if message.contains("irreversible") {
                        // A member holds Commit evidence: never abort. Only
                        // the forward recovery may still decide this command.
                        job.abort = false;
                        job.phase = Phase::Certify;
                        job.position = 0;
                        job.phase_observed.clear();
                        for next in job.recipient_next.values_mut() {
                            if *next == Phase::AbortResolve {
                                *next = Phase::Certify;
                            }
                        }
                        return Err(invalid(&format!(
                            "abort of a reopened Prepared slot refused by Commit evidence; only forward recovery may decide it: {message}"
                        )));
                    }
                    return Err(error);
                }
                Err(error) => {
                    failures.insert(
                        node.to_string(),
                        error.to_string().chars().take(1024).collect::<String>(),
                    );
                }
            }
            self.live(parent)?;
        }
        if job.phase_observed.len() < job.members.len() / 2 + 1
            || !job.phase_observed.contains(&origin)
        {
            return Err(invalid(&format!(
                "hot abort resolution lacks the origin plus a member majority: {failures:?}"
            )));
        }
        Ok(())
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
