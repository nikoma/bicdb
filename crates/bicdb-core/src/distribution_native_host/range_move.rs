//! Production controller of ONE range move onto a registered member.
//!
//! The controller owns the handoff driver (`NativeJoinedDataHandoff`) and
//! advances it one stage per step. It adds no stage logic, fence or permission:
//! every stage and the learner allocation are the driver's, every member-side
//! check is the member's own.
//!
//! Precondition, checked at creation and before every step: this node is the
//! metadata leader AND the coordinator of the range. The coordinator of a range
//! is its committed leader; once a relocation of the range onto the target is
//! committed it is also that relocation's source (the leader the learner
//! allocation retires), because the membership stages name another leader
//! while the same process must finish the handoff. When the precondition does
//! not hold the controller pauses. When the committed metadata names another
//! node it drops the driver and its custody and keeps identity data only
//! (range, target); when it holds again a new driver is built and the backend
//! re-derives its state from the members. When this node merely could not
//! read its own metadata the driver is kept and nothing is driven.
//!
//! Steps. A step drives to the next stage only (`drive_until`). An error is
//! recorded as "blocked at <stage>: <error>" and the step is repeated after a
//! backoff that doubles per consecutive failure up to a cap; a timeout is
//! repeated on the next step without a backoff. Nothing here stops the host.
//!
//! Waiting. One part of a stage belongs to another member's own host loop
//! (the target's writer policy and its serving). While that loop has not
//! finished, the backend ends its call and names what it waits for. That is
//! progress of the member, not a failure: the status reports it as
//! "waiting at <stage>: <what>" with no error, the stage is unchanged, no
//! backoff applies and the next step repeats the stage.
//!
//! Scheduling. `NativeRangeMoveSlot` runs each step on its own owned worker
//! thread with its own budget, because one stage (the image transfer) cannot
//! be cut into supervisor-sized pieces: the process loop only starts a step
//! when none is in flight and never waits for it. The loop starts a step on
//! its supervisor tick; after a step that completed its stage it starts the
//! next one at once (`step_due`), because between the source cut and the
//! target's serving the range is unavailable and a tick per stage only adds
//! to that. A blocked, waiting or paused move keeps the tick and its backoff.
use super::joined_data_handoff::{
    plan_learner_allocation, JoinedHandoffBackend, JoinedHandoffMetadataHosts,
    NativeJoinedDataHandoff, NativeJoinedDataHandoffStage as Stage, RemoteJoinedHandoffBackend,
};
use super::provisional_listener::NativeLeaderRangeMoveHosts;
use super::*;
use crate::distribution::{RangeId, RelocationId, RelocationPhase};
use parking_lot::Mutex;
use serde::Serialize;
use std::thread::JoinHandle;
use std::time::Instant;

/// Budget of one step on its worker. A stage that does not finish inside it
/// is cancelled and repeated; the driver continues from committed metadata.
const STEP_BUDGET: Duration = Duration::from_secs(120);
/// First backoff after a failed step; doubled per consecutive failure.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_CAP: Duration = Duration::from_secs(30);

const STAGES: [Stage; 10] = [
    Stage::TargetAdmitted,
    Stage::LearnerAllocated,
    Stage::ImageVerified,
    Stage::DecisionPublished,
    Stage::Certified,
    Stage::PromotedFrozen,
    Stage::MembershipFinal,
    Stage::Serving,
    Stage::ActivationRecorded,
    Stage::SourceRetired,
];
/// The operator notes one move keeps. A note with a measured time differs
/// on every repetition; the bound keeps the status small.
const MAX_NOTES: usize = 8;

fn stage_name(stage: Stage) -> &'static str {
    match stage {
        Stage::TargetAdmitted => "target_admitted",
        Stage::LearnerAllocated => "learner_allocated",
        Stage::ImageVerified => "image_verified",
        Stage::DecisionPublished => "decision_published",
        Stage::Certified => "certified",
        Stage::PromotedFrozen => "promoted_frozen",
        Stage::MembershipFinal => "membership_final",
        Stage::Serving => "serving",
        Stage::ActivationRecorded => "activation_recorded",
        Stage::SourceRetired => "source_retired",
    }
}
fn successor(reached: Option<Stage>) -> Option<Stage> {
    match reached {
        None => Some(STAGES[0]),
        Some(stage) => STAGES.iter().copied().find(|next| *next > stage),
    }
}
fn move_error(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range move: {}", detail.into()))
}

/// Operator view of the host's range move. Diagnostic DATA only.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct NativeRangeMoveStatus {
    pub range: RangeId,
    pub target: ClusterNodeId,
    /// The last stage this controller completed since it was created or last
    /// resumed. `None` before the first one.
    pub stage: Option<&'static str>,
    /// The stage the next step drives to. `None` once finished.
    pub next_stage: Option<&'static str>,
    /// "blocked at <stage>: <error>" of the last failed step, or the last
    /// abort refusal. Cleared by the next successful step.
    pub last_error: Option<String>,
    /// "waiting at <stage>: <what>" when the last step ended only because a
    /// member has not yet finished work of its own host loop. Not an error:
    /// the next step repeats the stage. Cleared by the next step.
    pub waiting: Option<String>,
    /// Why the controller is paused: the precondition does not hold.
    pub paused: Option<String>,
    /// Conditions of this move that failed no step but that the operator
    /// must know: the learner allocation was proposed without the write
    /// hold, or the hold ended before this node published the allocation.
    /// Kept for the lifetime of the move; each text once.
    pub notes: Vec<String>,
    /// Steps that passed the precondition and the backoff.
    pub attempts: u64,
    pub completed: bool,
    pub aborted: bool,
    /// A step is running on its worker; the other fields are those of the
    /// last finished step.
    pub step_in_flight: bool,
}

/// The host surface of the controller: the metadata hosts of the stage logic
/// plus one bounded wait that the host may need before a step.
pub(crate) trait NativeRangeMoveHosts {
    fn metadata(&mut self) -> &mut (dyn JoinedHandoffMetadataHosts + 'static);
    /// Finish what an earlier commit left unconfirmed. An error blocks the
    /// step like a stage error.
    fn settle(&mut self, _parent: &CancellationToken) -> Result<()> {
        Ok(())
    }
    /// Learn, bounded, the fate of a configuration an earlier commit of
    /// these hosts proposed and did not see decided. `Ok` means that the
    /// committed configuration is the truth: nothing of these hosts is
    /// still in the log. An error means that such an entry may still commit.
    fn resolve_proposal(&mut self, _parent: &CancellationToken) -> Result<()> {
        Ok(())
    }
}
impl NativeRangeMoveHosts for NativeLeaderRangeMoveHosts {
    fn metadata(&mut self) -> &mut (dyn JoinedHandoffMetadataHosts + 'static) {
        self
    }
    fn settle(&mut self, parent: &CancellationToken) -> Result<()> {
        NativeLeaderRangeMoveHosts::resolve_proposal(self, parent)?;
        self.settle_routing(parent)
    }
    fn resolve_proposal(&mut self, parent: &CancellationToken) -> Result<()> {
        NativeLeaderRangeMoveHosts::resolve_proposal(self, parent)
    }
}

/// Why the precondition does not hold, as operator text.
struct NotCoordinating {
    reason: String,
    /// The committed metadata names another node. `false` when this node
    /// could not read its own metadata: nothing is known either way.
    lost: bool,
}

/// `Ok` when `local` is the metadata leader and the coordinator of `range`.
fn coordinating(
    hosts: &dyn JoinedHandoffMetadataHosts,
    local: &ClusterNodeId,
    range: RangeId,
    target: &ClusterNodeId,
) -> std::result::Result<ClusterTopology, NotCoordinating> {
    let unknown = |reason: String| NotCoordinating {
        reason,
        lost: false,
    };
    let lost = |reason: String| NotCoordinating { reason, lost: true };
    let leader = hosts
        .metadata_leader()
        .map_err(|error| unknown(format!("the metadata leader is unknown: {error}")))?;
    if &leader != local {
        // This node's own committed copy, to tell the operator what to do.
        let advice = hosts
            .committed_topology()
            .map(|topology| advice(&topology, range, target, &leader))
            .unwrap_or_default();
        return Err(lost(format!(
            "this node ({local}) is not the metadata leader ({leader}){advice}"
        )));
    }
    let topology = hosts
        .committed_topology()
        .map_err(|error| unknown(format!("the committed metadata is unreadable: {error}")))?;
    let descriptor = topology.range_by_id(range).ok_or_else(|| {
        lost(format!(
            "range {range} is absent from the committed metadata"
        ))
    })?;
    // Only an unfinished relocation keeps its source as the coordinator. A
    // completed one names a past source: it proves nothing about now.
    if &descriptor.leader == local
        || topology.relocations.values().any(|plan| {
            plan.range_id == range
                && &plan.target == target
                && plan.phase != RelocationPhase::Completed
                && plan.source.as_ref() == Some(local)
        })
    {
        return Ok(topology);
    }
    Err(lost(format!(
        "this node ({local}) is not the leader of range {range} ({}){}",
        descriptor.leader,
        advice(&topology, range, target, &leader)
    )))
}
/// What the operator does when the metadata leader and the coordinator of the
/// range differ: "; range R is led by nX; metadata is led by nY; run ...".
/// Empty when the range is unknown.
fn advice(
    topology: &ClusterTopology,
    range: RangeId,
    target: &ClusterNodeId,
    metadata_leader: &ClusterNodeId,
) -> String {
    let Some(descriptor) = topology.range_by_id(range) else {
        return String::new();
    };
    // A committed relocation onto the target keeps its source as coordinator
    // after the membership stages named another range leader.
    let source = topology
        .relocations
        .values()
        .find(|plan| {
            plan.range_id == range
                && &plan.target == target
                && plan.phase != RelocationPhase::Completed
        })
        .and_then(|plan| plan.source.clone())
        .filter(|source| source != &descriptor.leader);
    let (coordinator, role) = match &source {
        Some(source) => (
            source,
            format!("the committed move of range {range} onto {target} is coordinated by its source {source}"),
        ),
        None => (
            &descriptor.leader,
            format!("range {range} is led by {}", descriptor.leader),
        ),
    };
    let facts = format!("; {role}; metadata is led by {metadata_leader}");
    if coordinator == metadata_leader {
        return format!("{facts}; send move-range to {coordinator}");
    }
    let voter = topology.nodes.get(coordinator).is_some_and(|member| {
        member.lifecycle == ClusterNodeLifecycle::Active
            && member.metadata_role == MetadataMemberRole::Voter
    });
    if !voter {
        return format!(
            "{facts}; {coordinator} is not an active metadata voter, so metadata leadership cannot be transferred to it: this range cannot be moved until {coordinator} is a metadata voter"
        );
    }
    format!(
        "{facts}; run transfer-metadata-leadership --to {coordinator} on {metadata_leader}, then send move-range to {coordinator}"
    )
}
fn unfinished(topology: &ClusterTopology, range: RangeId, target: &ClusterNodeId) -> bool {
    unfinished_relocation(topology, range, target).is_some()
}
/// The committed relocation of the range onto the target that is not
/// `Completed`. The driver, `coordinating` and `advice` all mean this one.
fn unfinished_relocation(
    topology: &ClusterTopology,
    range: RangeId,
    target: &ClusterNodeId,
) -> Option<RelocationId> {
    topology
        .relocations
        .values()
        .find(|plan| {
            plan.range_id == range
                && &plan.target == target
                && plan.phase != RelocationPhase::Completed
        })
        .map(|plan| plan.id)
}

pub(crate) struct NativeRangeMoveController<B>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts>,
{
    local: ClusterNodeId,
    range: RangeId,
    target: ClusterNodeId,
    /// Builds the backend of a new driver: at creation and after each pause.
    backend: Box<dyn FnMut() -> B + Send>,
    handoff: Option<NativeJoinedDataHandoff<B>>,
    reached: Option<Stage>,
    last_error: Option<String>,
    waiting: Option<String>,
    paused: Option<String>,
    notes: Vec<String>,
    attempts: u64,
    failures: u32,
    retry_at: Option<Instant>,
    backoff: (Duration, Duration),
    completed: bool,
    aborted: bool,
    /// The unfinished relocation of this move, once this controller saw it
    /// committed. A completed relocation is this move's only by this id.
    relocation: Option<RelocationId>,
    /// An abort was requested and the fate of its commit is unknown. Nothing
    /// is driven until it is known.
    abort_pending: bool,
}

impl<B> NativeRangeMoveController<B>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts>,
{
    /// Refused unless the precondition holds now. Without a committed
    /// relocation of the range onto the target, the learner allocation is
    /// planned once here, unused, so that an unusable target is refused at
    /// the operator's request and not on a later step.
    pub(crate) fn new(
        local: ClusterNodeId,
        range: RangeId,
        target: ClusterNodeId,
        hosts: &dyn JoinedHandoffMetadataHosts,
        backend: Box<dyn FnMut() -> B + Send>,
    ) -> Result<Self> {
        let topology = coordinating(hosts, &local, range, &target).map_err(|refusal| {
            let reason = refusal.reason;
            move_error(format!(
                "refused: {reason}; the coordinator must be both the metadata leader and the leader of the range"
            ))
        })?;
        if !unfinished(&topology, range, &target) {
            plan_learner_allocation(&topology, range, &target)
                .map_err(|error| move_error(format!("refused: {error}")))?;
        }
        Ok(Self {
            local,
            range,
            target,
            backend,
            handoff: None,
            reached: None,
            last_error: None,
            waiting: None,
            paused: None,
            notes: Vec::new(),
            attempts: 0,
            failures: 0,
            retry_at: None,
            backoff: (BACKOFF_BASE, BACKOFF_CAP),
            completed: false,
            aborted: false,
            relocation: None,
            abort_pending: false,
        })
    }
    #[cfg(test)]
    pub(crate) fn with_backoff(mut self, base: Duration, cap: Duration) -> Self {
        self.backoff = (base, cap);
        self
    }
    pub(crate) fn finished(&self) -> bool {
        self.completed || self.aborted
    }
    pub(crate) fn status(&self) -> NativeRangeMoveStatus {
        NativeRangeMoveStatus {
            range: self.range,
            target: self.target.clone(),
            stage: self.reached.map(stage_name),
            next_stage: if self.finished() {
                None
            } else {
                successor(self.reached).map(stage_name)
            },
            last_error: self.last_error.clone(),
            waiting: self.waiting.clone(),
            paused: self.paused.clone(),
            notes: self.notes.clone(),
            attempts: self.attempts,
            completed: self.completed,
            aborted: self.aborted,
            step_in_flight: false,
        }
    }
    fn blocked(&mut self, stage: &str, error: BicDbError) {
        if matches!(error, BicDbError::QueryTimedOut) {
            // A timeout is a normal repetition on the next step.
            self.last_error = Some(format!(
                "blocked at {stage}: timed out; repeated on the next step"
            ));
            self.retry_at = None;
            return;
        }
        self.failures = self.failures.saturating_add(1);
        let (base, cap) = self.backoff;
        let delay = base
            .saturating_mul(1u32 << (self.failures - 1).min(16))
            .min(cap);
        self.retry_at = Some(Instant::now() + delay);
        self.last_error = Some(format!("blocked at {stage}: {error}"));
    }
    fn driver(&mut self) -> &mut NativeJoinedDataHandoff<B> {
        if self.handoff.is_none() {
            self.handoff = Some(NativeJoinedDataHandoff::with_backend(
                self.range,
                self.target.clone(),
                (self.backend)(),
            ));
        }
        self.handoff.as_mut().expect("driver was just built")
    }

    /// One step: at most one stage. Never returns an error; the outcome is
    /// the status.
    pub(crate) fn tick(
        &mut self,
        hosts: &mut dyn NativeRangeMoveHosts,
        parent: &CancellationToken,
    ) {
        if self.finished() {
            return;
        }
        if self.abort_pending {
            self.settle_abort(hosts, parent);
            return;
        }
        let coordinated = coordinating(hosts.metadata(), &self.local, self.range, &self.target);
        if let Ok(topology) = &coordinated {
            if let Some(id) = unfinished_relocation(topology, self.range, &self.target) {
                self.relocation = Some(id);
            }
        }
        if let Err(refusal) = coordinated {
            // The relocation this controller drove is completed in the
            // committed metadata: the move is complete, whoever committed
            // its last stage. No other completed relocation counts.
            if refusal.lost
                && self.relocation.is_some_and(|id| {
                    hosts.metadata().committed_topology().is_ok_and(|topology| {
                        topology
                            .relocations
                            .get(&id)
                            .is_some_and(|plan| plan.phase == RelocationPhase::Completed)
                    })
                })
            {
                self.completed = true;
                self.reached = Some(Stage::SourceRetired);
                self.handoff = None;
                self.paused = None;
                self.waiting = None;
                self.last_error = None;
                self.retry_at = None;
                return;
            }
            if refusal.lost {
                // Identity data only while another node is named: the driver
                // and whatever its backend retained are dropped, and the
                // stage is derived again on resumption.
                self.handoff = None;
                self.reached = None;
            }
            self.paused = Some(refusal.reason);
            self.waiting = None;
            return;
        }
        self.paused = None;
        if self.retry_at.is_some_and(|at| Instant::now() < at) {
            return;
        }
        let Some(next) = successor(self.reached) else {
            self.completed = true;
            return;
        };
        self.attempts += 1;
        self.waiting = None;
        if let Err(error) = hosts.settle(parent) {
            self.blocked("route confirmation", error);
            return;
        }
        let started = Instant::now();
        let driven = self
            .driver()
            .drive_until(hosts.metadata(), Some(next), parent);
        for note in self
            .handoff
            .as_mut()
            .map(NativeJoinedDataHandoff::take_notes)
            .unwrap_or_default()
        {
            if !self.notes.contains(&note) && self.notes.len() < MAX_NOTES {
                self.notes.push(note);
            }
        }
        super::joined_data_handoff::trace(started, || {
            format!(
                "step to {} -> {}",
                stage_name(next),
                match &driven {
                    Ok(stage) => format!("reached {}", stage_name(*stage)),
                    Err(error) => format!("error {error}"),
                }
            )
        });
        match driven {
            Ok(stage) => {
                self.reached = Some(self.reached.map_or(stage, |reached| reached.max(stage)));
                self.last_error = None;
                self.failures = 0;
                self.retry_at = None;
                if stage == Stage::SourceRetired
                    || self
                        .handoff
                        .as_ref()
                        .is_some_and(|handoff| handoff.completed())
                {
                    self.completed = true;
                }
            }
            Err(error) => {
                // The backend names a wait only for the call that just ended.
                let waiting = self
                    .handoff
                    .as_ref()
                    .and_then(|handoff| handoff.backend().waiting())
                    .filter(|_| !matches!(error, BicDbError::QueryTimedOut));
                match waiting {
                    Some(what) => {
                        // A member's own host loop has not finished: not a
                        // failure, no backoff, the stage is repeated.
                        self.waiting = Some(format!("waiting at {}: {what}", stage_name(next)));
                        self.last_error = None;
                        self.failures = 0;
                        self.retry_at = None;
                    }
                    None => self.blocked(stage_name(next), error),
                }
            }
        }
    }

    /// An abort whose commit was left undecided: learn its fate, then act
    /// on the committed configuration. Nothing is driven meanwhile.
    fn settle_abort(&mut self, hosts: &mut dyn NativeRangeMoveHosts, parent: &CancellationToken) {
        if let Err(error) = hosts.resolve_proposal(parent) {
            self.last_error = Some(format!("abort not decided: {error}"));
            return;
        }
        match hosts.metadata().committed_topology() {
            Ok(topology) if !unfinished(&topology, self.range, &self.target) => {
                self.abort_pending = false;
                self.finish_abort(Some(
                    "abort committed after its commit wait had ended".into(),
                ));
            }
            Ok(_) => {
                self.abort_pending = false;
                self.last_error = Some(
                    "the abort was not committed: its proposal was superseded. The move continues; request the abort again to abort it"
                        .into(),
                );
            }
            Err(error) => {
                self.last_error = Some(format!(
                    "abort not decided: the committed metadata is unreadable: {error}"
                ));
            }
        }
    }

    /// Abort before the source cut. First the fate of any configuration
    /// these hosts proposed and did not see decided is learned: while it is
    /// unknown the abort is refused as "unresolved proposal", never reported
    /// as done, because that entry may still commit. Then the committed
    /// configuration decides: a move without a committed relocation is
    /// cancelled without a commit; otherwise the driver's `abort_before_cut`
    /// decides, and its refusal is returned verbatim. An abort commit whose
    /// own fate stays unknown leaves the move neither aborted nor driven
    /// until a later step has learned it.
    pub(crate) fn abort(
        &mut self,
        hosts: &mut dyn NativeRangeMoveHosts,
        parent: &CancellationToken,
    ) -> Result<()> {
        if self.aborted {
            return Ok(());
        }
        if self.completed {
            return Err(move_error("the move is complete; nothing to abort"));
        }
        if let Err(error) = hosts.resolve_proposal(parent) {
            self.last_error = Some(format!("abort not decided: {error}"));
            return Err(move_error(format!("abort not decided: {error}")));
        }
        if self.abort_pending {
            // The earlier abort's commit is decided now.
            self.settle_abort(hosts, parent);
            if self.aborted {
                return Ok(());
            }
        }
        let topology = coordinating(hosts.metadata(), &self.local, self.range, &self.target)
            .map_err(|refusal| move_error(format!("abort refused: {}", refusal.reason)))?;
        if !unfinished(&topology, self.range, &self.target) {
            if self.relocation.is_some_and(|id| {
                topology
                    .relocations
                    .get(&id)
                    .is_some_and(|plan| plan.phase == RelocationPhase::Completed)
            }) {
                return Err(move_error(
                    "the committed relocation is complete; nothing to abort",
                ));
            }
            // Nothing is committed for this move, and nothing these hosts
            // proposed is still in the log.
            self.finish_abort(None);
            return Ok(());
        }
        let aborted = self.driver().abort_before_cut(hosts.metadata(), parent);
        match aborted {
            Ok(()) => {
                self.finish_abort(None);
                Ok(())
            }
            Err(error) => {
                // The abort commit may be in the log although its wait
                // failed: its fate first, then the committed configuration.
                if let Err(unresolved) = hosts.resolve_proposal(parent) {
                    self.abort_pending = true;
                    self.last_error = Some(format!("abort not decided: {unresolved}"));
                    return Err(move_error(format!(
                        "abort not decided: {unresolved}; the move is not driven until this is known"
                    )));
                }
                let committed = hosts
                    .metadata()
                    .committed_topology()
                    .is_ok_and(|current| !unfinished(&current, self.range, &self.target));
                if committed {
                    self.finish_abort(Some(format!("abort committed; {error}")));
                    return Ok(());
                }
                self.last_error = Some(format!("abort refused: {error}"));
                Err(error)
            }
        }
    }
    fn finish_abort(&mut self, note: Option<String>) {
        self.aborted = true;
        self.abort_pending = false;
        self.handoff = None;
        self.waiting = None;
        self.paused = None;
        self.retry_at = None;
        self.last_error = note;
    }
}

struct Driven<B>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts>,
{
    controller: NativeRangeMoveController<B>,
    hosts: NativeLeaderRangeMoveHosts,
}

/// The host's one range move: the controller, the leader's metadata hosts and
/// the owned worker of the step in flight.
pub(crate) struct NativeRangeMoveSlot<B = RemoteJoinedHandoffBackend>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts> + Send + 'static,
{
    driven: Arc<Mutex<Driven<B>>>,
    published: Arc<Mutex<NativeRangeMoveStatus>>,
    worker: Option<JoinHandle<()>>,
    step: Option<CancellationToken>,
}

impl NativeRangeMoveSlot<RemoteJoinedHandoffBackend> {
    /// The production move: the members are asked over the coordinator's own
    /// control transport.
    pub(crate) fn start(
        control: &NativeMetadataControlHost,
        range: RangeId,
        target: ClusterNodeId,
    ) -> Result<Self> {
        let hosts = control.range_move_hosts()?;
        let transport = hosts.transport();
        // This node's own data service: the move retires this node's own
        // replica, and a node never sends a step request to itself.
        let service = hosts.local_service();
        // This node's own write coordinator: the coordinator of a move leads
        // the moved range, and the learner allocation is proposed under its
        // admission hold of that range.
        let writer = control.initial_range_write_coordinator();
        let remote = target.clone();
        Self::start_with(
            hosts,
            range,
            target,
            Box::new(move || {
                let backend =
                    RemoteJoinedHandoffBackend::new(remote.clone(), Arc::clone(&transport))
                        .with_local_service(Arc::clone(&service));
                match &writer {
                    Some(writer) => backend.with_local_writer(Arc::clone(writer)),
                    None => backend,
                }
            }),
        )
    }
}

impl<B> NativeRangeMoveSlot<B>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts> + Send + 'static,
{
    pub(crate) fn start_with(
        hosts: NativeLeaderRangeMoveHosts,
        range: RangeId,
        target: ClusterNodeId,
        backend: Box<dyn FnMut() -> B + Send>,
    ) -> Result<Self> {
        let controller =
            NativeRangeMoveController::new(hosts.local(), range, target, &hosts, backend)?;
        let published = Arc::new(Mutex::new(controller.status()));
        Ok(Self {
            driven: Arc::new(Mutex::new(Driven { controller, hosts })),
            published,
            worker: None,
            step: None,
        })
    }
    fn reap(&mut self) -> Result<()> {
        if self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
            self.step.take();
            self.worker
                .take()
                .expect("finished worker present")
                .join()
                .map_err(|_| move_error("a step worker panicked"))?;
        }
        Ok(())
    }
    pub(crate) fn status(&self) -> NativeRangeMoveStatus {
        let mut status = self.published.lock().clone();
        status.step_in_flight = self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished());
        status
    }
    pub(crate) fn finished(&self) -> bool {
        let status = self.published.lock();
        status.completed || status.aborted
    }
    /// Start one step unless one is in flight. Never waits for the step.
    /// `process` is the lifetime of the owning process, not of the caller's
    /// request.
    pub(crate) fn advance(&mut self, process: &CancellationToken) -> Result<()> {
        process.check()?;
        self.reap()?;
        if self.worker.is_some() || self.finished() {
            return Ok(());
        }
        let token = process.child_with_deadline(Instant::now() + STEP_BUDGET);
        let step = token.clone();
        let driven = Arc::clone(&self.driven);
        let published = Arc::clone(&self.published);
        let worker = std::thread::Builder::new()
            .name("bicdb-native-range-move".into())
            .spawn(move || {
                let mut driven = driven.lock();
                let Driven { controller, hosts } = &mut *driven;
                controller.tick(hosts, &step);
                *published.lock() = controller.status();
            })
            .map_err(|error| move_error(format!("step worker spawn failed: {error}")))?;
        self.worker = Some(worker);
        self.step = Some(token);
        Ok(())
    }
    /// True when no step is in flight and the last one completed its stage:
    /// the next stage has nothing to wait for, so the process loop may start
    /// it at once instead of on its next supervisor tick. A blocked, waiting
    /// or paused move keeps the tick cadence and its backoff.
    pub(crate) fn step_due(&self) -> bool {
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            return false;
        }
        let status = self.published.lock();
        !status.completed
            && !status.aborted
            && status.stage.is_some()
            && status.last_error.is_none()
            && status.waiting.is_none()
            && status.paused.is_none()
    }
    /// Wait for the step in flight, bounded by `parent`.
    pub(crate) fn wait_step(&mut self, parent: &CancellationToken) -> Result<()> {
        while self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            parent.check()?;
            std::thread::sleep(Duration::from_millis(5));
        }
        self.reap()
    }
    /// Test only: the metadata hosts of this slot, between two steps.
    #[cfg(test)]
    pub(crate) fn test_hosts(&self, change: impl FnOnce(&mut NativeLeaderRangeMoveHosts)) {
        change(&mut self.driven.lock().hosts);
    }
    /// Abort before the source cut, after the step in flight has finished.
    pub(crate) fn abort(&mut self, parent: &CancellationToken) -> Result<()> {
        let mut driven = loop {
            parent.check()?;
            if let Some(driven) = self.driven.try_lock_for(Duration::from_millis(5)) {
                break driven;
            }
        };
        let Driven { controller, hosts } = &mut *driven;
        let outcome = controller.abort(hosts, parent);
        *self.published.lock() = controller.status();
        outcome
    }
    /// Cancel the step in flight and wait for its worker, bounded by `parent`.
    /// The worker keeps its owners until it has really exited.
    pub(crate) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        if let Some(step) = &self.step {
            step.cancel();
        }
        self.wait_step(parent)
    }
}

/// A metadata leadership transfer away from this host is refused while the
/// host's own range move is unfinished and running. The coordinator must be
/// the metadata leader; after a transfer its controller only pauses, and
/// nothing on the new leader continues the move. A move that is already
/// paused (this host does not coordinate it now) does not refuse: the
/// transfer may be the way to make the coordinator the leader again.
pub(crate) fn require_no_running_range_move<B>(slot: Option<&NativeRangeMoveSlot<B>>) -> Result<()>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts> + Send + 'static,
{
    let Some(status) = slot.map(NativeRangeMoveSlot::status) else {
        return Ok(());
    };
    if status.completed || status.aborted || status.paused.is_some() {
        return Ok(());
    }
    Err(move_error(format!(
        "metadata leadership transfer refused: this host coordinates the unfinished move of range {} onto {} (last completed stage: {}), and the coordinator must stay the metadata leader until the move is complete. Wait for the move to complete, or abort it first (move-range --abort, possible before the source cut)",
        status.range,
        status.target,
        status.stage.unwrap_or("none")
    )))
}

/// Operator request to move `range` onto `target`. One move at a time per
/// host: a different unfinished move refuses; the same move is confirmed; a
/// finished one (completed or aborted) is replaced.
pub(crate) fn request_range_move<B>(
    slot: &mut Option<NativeRangeMoveSlot<B>>,
    range: RangeId,
    target: &ClusterNodeId,
    parent: &CancellationToken,
    start: impl FnOnce() -> Result<NativeRangeMoveSlot<B>>,
) -> Result<()>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts> + Send + 'static,
{
    parent.check()?;
    if let Some(current) = slot.as_mut() {
        let status = current.status();
        if !status.completed && !status.aborted {
            if status.range == range && &status.target == target {
                return Ok(());
            }
            return Err(move_error(format!(
                "refused: the move of range {} onto {} is not finished; one move at a time per host",
                status.range, status.target
            )));
        }
        current.stop(parent)?;
    }
    *slot = Some(start()?);
    Ok(())
}

/// Operator request to abort the host's move of `range` before its cut.
pub(crate) fn request_range_move_abort<B>(
    slot: &mut Option<NativeRangeMoveSlot<B>>,
    range: RangeId,
    parent: &CancellationToken,
) -> Result<()>
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts> + Send + 'static,
{
    let current = slot
        .as_mut()
        .filter(|current| current.status().range == range)
        .ok_or_else(|| move_error(format!("this host has no move of range {range} to abort")))?;
    current.abort(parent)
}
