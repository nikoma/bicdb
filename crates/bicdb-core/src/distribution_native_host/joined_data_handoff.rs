//! Driver for moving ONE range onto a Registered member through the existing
//! joint handoff, with an abort before the source cut and a re-drivable
//! continuation after it.
//!
//! The driver sequences the existing joint objects only. It adds no fence,
//! quorum rule or permission, and it never revokes a durable promise.
//!
//! Boundary between abort and resume. A source close records a permanent
//! handoff promise on the old voter and advances that voter's durable range
//! epoch to the activation epoch. Once any old voter holds that promise the
//! only continuation is to finish this same handoff. Before it, the committed
//! learner allocation is the only change: no old voter, and not the target,
//! holds durable state of the new range epoch. `abort_before_cut` therefore
//! commits the original range descriptor again (the original voters at the
//! epoch the allocation expected, the relocation removed) through the real
//! metadata consensus. The original owners' installed policy, recovered
//! leadership and native configuration are those of exactly that descriptor,
//! so nothing is re-enrolled and no permission is created. The abort is
//! refused when this driver started a source round, when the committed
//! relocation is past its bare learner allocation, or when any old voter's
//! durable range log is already beyond the expected epoch. Metadata consensus
//! itself refuses to erase a relocation that carries a joint decision.
//!
//! Custody. The joint machinery issues live owners (range freezes, the closed
//! OLD quorum, the finalized target image, the durable decision, the serving
//! continuation) that cannot be decoded from disk. This driver retains them
//! between calls. A call that fails or stops between stages leaves them here
//! and a later call continues from the committed metadata stage plus this
//! custody. Losing the process that holds this object is a cold restart and
//! is not covered.
// No production caller yet: the driver is reached only from the in-process
// multi-node fixture. Remove this once a controller drives it.
#![allow(dead_code)]
use super::*;
use crate::distribution::{
    NativeJointHandoffStageV1, RangeDescriptor, RangeHandoffSession, RangeId, RangeRelocation,
    RangeReplica, RelocationId, RelocationPhase, ReplicaId, ReplicaMoveReason,
};
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataReadParticipant, MetadataRecoveryRolePurpose,
};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageInstallRole, NativeJointCatalogueActivationReceipt,
    RecoveryFenceScope,
};
use crate::distribution_range_handoff::{
    NativeJointDecisionPublicationRound, NativeJointDurableDecision, NativeJointHandoffPlan,
    NativeJointHandoffRuntime, NativeJointNewObservation, NativeJointNewRound,
    NativeJointOldClosedQuorum, NativeJointOldObservation, NativeJointOldRound,
    NativeJointPreparationPhaseQuorum, NativeJointPreparedServingObservation,
    NativeJointPreparedServingQuorum, NativeJointPreparedServingRound,
    NativeJointPublishedServingQuorum, NativeJointReleaseContinuation,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze, NativeRangeImageInstaller,
};
use crate::distribution_range_recovery::{
    RangeRecoveryRuntime, RecoveryEpochFenceRequest, RecoveryRoleAdmission,
};
use crate::distribution_transport::TcpClusterRelocationTransport;
use crate::{ClusterDataNodeService, DistributionStore, RangeSnapshotOptions};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::time::Instant;

/// Per-request network budget of one metadata or role acquisition.
const REQUEST: Duration = Duration::from_secs(5);
/// Budget of one fresh joint phase (all final members inside one window).
const PHASE: Duration = Duration::from_secs(4);
/// Budget of one bounded host operation (policy publication, socket change).
const HOST_OPERATION: Duration = Duration::from_secs(10);
/// Budget of the target's leadership recovery and ingress publication.
const LEADERSHIP: Duration = Duration::from_secs(20);
/// A timed-out fresh phase or publication is repeated this many times.
const PHASE_ATTEMPTS: usize = 4;

fn handoff_error(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("joined data handoff: {}", detail.into()))
}
fn stage_error(stage: &str, error: BicDbError) -> BicDbError {
    match error {
        // A timeout keeps its type: callers bound their own retries on it.
        BicDbError::QueryTimedOut => BicDbError::QueryTimedOut,
        other => handoff_error(format!("{stage}: {other}")),
    }
}
fn bounded(parent: &CancellationToken, budget: Duration) -> CancellationToken {
    parent.child_with_deadline(Instant::now() + budget)
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
fn retry_timed_out<T>(mut attempt: impl FnMut() -> Result<T>) -> Result<T> {
    let mut round = 0;
    loop {
        round += 1;
        match attempt() {
            Err(BicDbError::QueryTimedOut) if round < PHASE_ATTEMPTS => {}
            other => return other,
        }
    }
}

/// Completed stages, in order. `drive_until` returns after the named stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NativeJoinedDataHandoffStage {
    /// The target holds a writer policy and a DATA socket; no range changed.
    TargetAdmitted,
    /// The learner allocation is committed; no old voter holds a promise.
    LearnerAllocated,
    /// The source is cut and the verified image is finalized on the target.
    ImageVerified,
    /// The joint decision is durable on an old and a new data majority.
    DecisionPublished,
    /// Metadata stage: decision certified.
    Certified,
    /// Metadata stage: union membership, all writers frozen.
    PromotedFrozen,
    /// Metadata stage: final membership, activation pending.
    MembershipFinal,
    /// Open and release finished; the target leads and serves the range.
    Serving,
}

/// Actual handles of one participating member. They are the member's own
/// objects; the driver never constructs a service, store or transport.
#[derive(Clone)]
pub(crate) struct NativeJoinedDataMember {
    pub(crate) client: Arc<TcpClusterRelocationTransport>,
    pub(crate) provider: Arc<MetadataReadProvider>,
    pub(crate) service: Arc<Mutex<ClusterDataNodeService>>,
}

/// A planning view of the fresh committed configuration. It is never a
/// metadata store: only the real consensus commit publishes its result.
pub(crate) struct NativeJoinedDataPlanningView {
    pub(crate) view: DistributionStore,
    pub(crate) fresh: HostMetadataReadObservation,
    pub(crate) business: Uuid,
}

/// The hosts this driver acts on. Every method is an existing host action;
/// the driver holds the sequencing, the idempotence and the abort boundary.
pub(crate) trait NativeJoinedDataHandoffHosts {
    fn metadata_leader(&self) -> Result<ClusterNodeId>;
    fn committed_topology(&self) -> Result<ClusterTopology>;
    /// Commit through the real metadata consensus and return once the
    /// participating members route by the committed configuration.
    fn commit_topology(
        &mut self,
        candidate: ClusterTopology,
        parent: &CancellationToken,
    ) -> Result<()>;
    fn planning_view(&mut self, parent: &CancellationToken)
        -> Result<NativeJoinedDataPlanningView>;
    fn member(&self, node: &ClusterNodeId) -> Result<NativeJoinedDataMember>;
    /// Every original DATA owner whose installed policy follows the committed
    /// configuration, voters of this range or not.
    fn data_owners(&self) -> Result<Vec<ClusterNodeId>>;
    /// Install the target's writer policy and DATA socket. Repeatable.
    fn open_target(&mut self, parent: &CancellationToken) -> Result<()>;
    fn target_recovery_runtime(&self) -> Result<Arc<RangeRecoveryRuntime>>;
    fn refresh_target_policy(&mut self, parent: &CancellationToken) -> Result<()>;
    fn publish_target(&mut self, parent: &CancellationToken) -> Result<usize>;
    fn close_old(
        &self,
        node: &ClusterNodeId,
        runtime: Arc<NativeJointHandoffRuntime>,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJointOldObservation>>;
    fn forget_old_closure(&self, node: &ClusterNodeId, range: RangeId) -> Result<bool>;
    fn refresh_owner_policy(
        &mut self,
        node: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<()>;
    /// Called after every copied image step. An error stops this call and
    /// leaves the partial install on the target for a later restart.
    fn transfer_progress(&mut self, _copied_steps: usize) -> Result<()> {
        Ok(())
    }
}

struct Members {
    actor: MetadataReadParticipant,
    metadata_leader: ClusterNodeId,
    by_node: BTreeMap<ClusterNodeId, NativeJoinedDataMember>,
}
impl Members {
    fn get(&self, node: &ClusterNodeId) -> Result<&NativeJoinedDataMember> {
        self.by_node
            .get(node)
            .ok_or_else(|| handoff_error(format!("member {node} is not a participant")))
    }
    /// One fresh committed-handoff role of `recipient`, witnessed by the
    /// recipient's own metadata provider.
    fn role(
        &self,
        plan: &NativeJointHandoffPlan,
        recipient: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<RecoveryRoleAdmission> {
        let promise = plan.promise();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: promise.relocation_id,
            plan_id: promise.plan_id.clone(),
            session_id: promise.session_id,
            nonce: promise.nonce,
        };
        let token = bounded(parent, REQUEST);
        let observed = self.get(recipient)?.provider.witness_recovery_role(
            &self.actor,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            purpose.clone(),
            &token,
        )?;
        RecoveryRoleAdmission::from_witness(
            observed,
            &self.actor,
            recipient,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            &purpose,
        )
    }
}

/// One handoff of `range` onto `target`, with the custody described in the
/// module documentation.
pub(crate) struct NativeJoinedDataHandoff {
    range: RangeId,
    target: ClusterNodeId,
    /// Lifetime of the retained joint owners; never a request budget.
    session: CancellationToken,
    /// Freeze owners of the old members; survivors keep theirs until release.
    runtimes: BTreeMap<ClusterNodeId, Arc<NativeJointHandoffRuntime>>,
    /// A source round was started: a permanent promise may exist.
    cut_attempted: bool,
    target_fenced: bool,
    /// An image lease was taken from the target runtime at least once.
    target_leased: bool,
    plan: Option<Arc<NativeJointHandoffPlan>>,
    closed: Option<Arc<NativeJointOldClosedQuorum>>,
    image: Option<(NativeImageInstallFreeze, NativeImageCatalogueFinalization)>,
    publishing: Option<(NativeJointDecisionPublicationRound, BTreeSet<ClusterNodeId>)>,
    decision: Option<NativeJointDurableDecision>,
    policies_installed: bool,
    prepared: Option<Arc<NativeJointPreparedServingQuorum>>,
    published: Option<Arc<NativeJointPublishedServingQuorum>>,
    activations: BTreeMap<ClusterNodeId, Arc<NativeJointCatalogueActivationReceipt>>,
    image_owner: Option<crate::distribution_data::NativeJointImageReleaseOwner>,
    continuation: Option<Arc<NativeJointReleaseContinuation>>,
    released: BTreeSet<ClusterNodeId>,
    /// Ranges the target recovered when it opened ingress; set on completion.
    recovered: Option<usize>,
    /// Records per copied image step.
    image_batch_records: usize,
}

impl NativeJoinedDataHandoff {
    pub(crate) fn new(range: RangeId, target: ClusterNodeId) -> Self {
        Self {
            range,
            target,
            session: CancellationToken::uncancelable(),
            runtimes: BTreeMap::new(),
            cut_attempted: false,
            target_fenced: false,
            target_leased: false,
            plan: None,
            closed: None,
            image: None,
            publishing: None,
            decision: None,
            policies_installed: false,
            prepared: None,
            published: None,
            activations: BTreeMap::new(),
            image_owner: None,
            continuation: None,
            released: BTreeSet::new(),
            recovered: None,
            image_batch_records: 256,
        }
    }
    /// Bound the records of one copied image step (1..=256).
    pub(crate) fn with_image_batch_records(mut self, records: usize) -> Self {
        self.image_batch_records = records.clamp(1, 256);
        self
    }
    /// True once a source round was started. Abort is refused from then on.
    pub(crate) fn cut_attempted(&self) -> bool {
        self.cut_attempted
    }
    pub(crate) fn completed(&self) -> bool {
        self.recovered.is_some()
    }
    /// Run the handoff to completion. Repeating it after completion changes
    /// nothing and returns the same stage.
    pub(crate) fn drive(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        parent: &CancellationToken,
    ) -> Result<NativeJoinedDataHandoffStage> {
        self.drive_until(hosts, None, parent)
    }

    fn relocation(&self, topology: &ClusterTopology) -> Option<RangeRelocation> {
        topology
            .relocations
            .values()
            .find(|plan| {
                plan.range_id == self.range
                    && plan.target == self.target
                    && plan.phase != RelocationPhase::Completed
            })
            .cloned()
    }
    fn range(&self, topology: &ClusterTopology) -> Result<RangeDescriptor> {
        topology
            .range_by_id(self.range)
            .cloned()
            .ok_or_else(|| handoff_error("range absent from committed metadata"))
    }
    fn members(
        &self,
        hosts: &dyn NativeJoinedDataHandoffHosts,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
    ) -> Result<Members> {
        let mut nodes = BTreeSet::new();
        nodes.insert(self.target.clone());
        if let Some(plan) = &self.plan {
            nodes.extend(plan.old_members().keys().cloned());
            nodes.extend(plan.new_members().keys().cloned());
        } else {
            nodes.extend(range.replicas.iter().map(|replica| replica.node_id.clone()));
        }
        let source = relocation
            .source
            .clone()
            .ok_or_else(|| handoff_error("committed relocation names no source"))?;
        nodes.insert(source.clone());
        let mut by_node = BTreeMap::new();
        for node in nodes {
            let member = hosts.member(&node)?;
            by_node.insert(node, member);
        }
        let actor = by_node
            .get(&source)
            .ok_or_else(|| handoff_error("source member handle absent"))?
            .client
            .local_metadata_read_participant();
        Ok(Members {
            actor,
            metadata_leader: hosts.metadata_leader()?,
            by_node,
        })
    }

    /// Return the range to its original voters by committing the original
    /// range descriptor again. See the module documentation for the boundary.
    /// A later `drive` starts a new learner allocation.
    pub(crate) fn abort_before_cut(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.cut_attempted || !self.runtimes.is_empty() {
            return Err(handoff_error(
                "a source round was started and a permanent handoff promise may exist; the handoff can only be resumed",
            ));
        }
        let topology = hosts.committed_topology()?;
        let range = self.range(&topology)?;
        let relocation = self
            .relocation(&topology)
            .ok_or_else(|| handoff_error("no committed learner allocation to abort"))?;
        if relocation.native_joint_handoff_v1.is_some()
            || relocation.phase != RelocationPhase::LearnerAllocated
            || relocation.learner_epoch != range.epoch
            || relocation.expected_epoch.checked_add(1) != Some(range.epoch)
            || range
                .replicas
                .iter()
                .filter(|replica| {
                    replica.id == relocation.target_replica_id
                        && replica.node_id == relocation.target
                        && replica.role == RangeReplicaRole::Learner
                })
                .count()
                != 1
        {
            return Err(handoff_error(
                "the committed relocation is past its bare learner allocation; the handoff can only be resumed",
            ));
        }
        // Durable truth, independent of this object's memory: a source close
        // advances the old voter's range log to the activation epoch, and any
        // command of the allocation epoch advances it past the expected one.
        // Every old voter must be read; an unreadable voter refuses the abort.
        for replica in &range.replicas {
            if replica.role != RangeReplicaRole::Voter {
                continue;
            }
            let progress = hosts
                .member(&replica.node_id)?
                .service
                .try_lock_for(Duration::from_secs(1))
                .ok_or_else(|| handoff_error("old voter service busy during abort"))?
                .range_write_progress(range.id);
            if progress.current_epoch > relocation.expected_epoch {
                return Err(handoff_error(format!(
                    "old voter {} already holds durable state beyond the expected epoch; the handoff can only be resumed",
                    replica.node_id
                )));
            }
        }
        let mut candidate = topology.clone();
        candidate.relocations.remove(&relocation.id);
        let descriptor = candidate
            .ranges
            .values_mut()
            .find(|descriptor| descriptor.id == self.range)
            .ok_or_else(|| handoff_error("range absent from committed metadata"))?;
        descriptor
            .replicas
            .retain(|replica| replica.id != relocation.target_replica_id);
        descriptor.epoch = relocation.expected_epoch;
        candidate.generation += 1;
        candidate.validate()?;
        hosts
            .commit_topology(candidate, parent)
            .map_err(|error| stage_error("abort commit", error))?;
        self.plan = None;
        self.closed = None;
        Ok(())
    }

    /// Advance until `until` is complete, or to completion. Every stage is
    /// derived from the committed metadata and this object's custody, so the
    /// call may be repeated after any error or early return.
    pub(crate) fn drive_until(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        until: Option<NativeJoinedDataHandoffStage>,
        parent: &CancellationToken,
    ) -> Result<NativeJoinedDataHandoffStage> {
        use NativeJoinedDataHandoffStage as Stage;
        parent.check()?;
        if self.recovered.is_some() {
            return Ok(Stage::Serving);
        }
        // Step 1: target admission, before any range configuration changes.
        hosts
            .open_target(&bounded(parent, HOST_OPERATION))
            .map_err(|error| stage_error("target admission", error))?;
        if until == Some(Stage::TargetAdmitted) {
            return Ok(Stage::TargetAdmitted);
        }
        // Step 2: the committed learner allocation.
        let mut topology = hosts.committed_topology()?;
        if self.relocation(&topology).is_none() {
            let candidate = plan_learner_allocation(&topology, self.range, &self.target)?;
            hosts
                .commit_topology(candidate, parent)
                .map_err(|error| stage_error("learner allocation commit", error))?;
            topology = hosts.committed_topology()?;
        }
        let mut relocation = self
            .relocation(&topology)
            .ok_or_else(|| handoff_error("committed learner allocation absent"))?;
        if until == Some(Stage::LearnerAllocated) {
            return Ok(Stage::LearnerAllocated);
        }
        let range = self.range(&topology)?;
        let members = self.members(&*hosts, &range, &relocation)?;
        if relocation.native_joint_handoff_v1.is_some() {
            if let Some(stage @ (Stage::ImageVerified | Stage::DecisionPublished)) = until {
                return Ok(stage);
            }
        } else {
            // Steps 3 and 4: source cut, transfer and verification.
            if self.decision.is_none() && self.publishing.is_none() && self.image.is_none() {
                self.cut_and_ship(hosts, &members, &range, &relocation, parent)?;
            }
            if until == Some(Stage::ImageVerified) {
                return Ok(Stage::ImageVerified);
            }
            // Step 5: the joint decision on an old and a new data majority.
            if self.decision.is_none() {
                self.publish_decision(&members, parent)?;
            }
            if until == Some(Stage::DecisionPublished) {
                return Ok(Stage::DecisionPublished);
            }
        }
        // Step 6: the three staged metadata membership commits.
        for (stage, reached) in [
            (None, Stage::Certified),
            (
                Some(NativeJointHandoffStageV1::Certified),
                Stage::PromotedFrozen,
            ),
            (
                Some(NativeJointHandoffStageV1::PromotedFrozen),
                Stage::MembershipFinal,
            ),
        ] {
            let committed = relocation
                .native_joint_handoff_v1
                .as_ref()
                .map(|record| record.stage());
            if committed == stage {
                self.commit_membership_stage(hosts, stage, parent)?;
                topology = hosts.committed_topology()?;
                relocation = self
                    .relocation(&topology)
                    .ok_or_else(|| handoff_error("committed relocation disappeared"))?;
            }
            if until == Some(reached) {
                return Ok(reached);
            }
        }
        if relocation
            .native_joint_handoff_v1
            .as_ref()
            .map(|record| record.stage())
            != Some(NativeJointHandoffStageV1::AwaitingActivation)
        {
            return Err(handoff_error(
                "committed relocation is not awaiting activation after the membership stages",
            ));
        }
        // Steps 7 to 9: policy continuation, Open and release, serving.
        let recovered = self.open_and_serve(hosts, &members, parent)?;
        self.recovered = Some(recovered);
        Ok(Stage::Serving)
    }

    /// Close the old voters, capture the selected source and install the
    /// verified image on the target. A previous round's retained closures are
    /// bound to their original issuance window and cannot enter a new round,
    /// so every call closes again: the durable promise and native close record
    /// of the same session are confirmed, never replaced.
    fn cut_and_ship(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        members: &Members,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.cut_attempted = true;
        self.closed = None;
        self.plan = None;
        let token = bounded(parent, REQUEST);
        let business = Uuid::now_v7();
        let observation = members
            .get(&range.leader)?
            .client
            .acquire_metadata_read_observation(
                &members.metadata_leader,
                business,
                range.id,
                range.epoch,
                REQUEST,
                &token,
            )
            .map_err(|error| stage_error("source round metadata", error))?;
        let mut round = NativeJointOldRound::begin(observation, business, relocation.id)
            .map_err(|error| stage_error("source round", error))?;
        let plan = round.plan();
        let old = plan.old_members().keys().cloned().collect::<Vec<_>>();
        for node in &old {
            hosts.forget_old_closure(node, range.id)?;
            if !self.runtimes.contains_key(node) {
                let runtime = NativeJointHandoffRuntime::new(members.get(node)?.service.clone())?;
                self.runtimes.insert(node.clone(), Arc::new(runtime));
            }
        }
        for node in &old {
            let operation = bounded(parent, REQUEST);
            let role = members.role(&plan, node, parent)?;
            let constraints = self.runtimes[node]
                .observe_constraints(plan.clone(), role, &operation)
                .map_err(|error| stage_error("source constraints", error))?;
            round
                .record_constraints(constraints, &members.get(node)?.service)
                .map_err(|error| stage_error("source constraints", error))?;
        }
        for node in &old {
            let operation = bounded(parent, REQUEST);
            let role = members.role(&plan, node, parent)?;
            let own_business = Uuid::now_v7();
            let own = members
                .get(node)?
                .client
                .acquire_host_metadata_observation(
                    &members.metadata_leader,
                    own_business,
                    REQUEST,
                    &operation,
                )
                .map_err(|error| stage_error("source close metadata", error))?;
            let closed = hosts
                .close_old(
                    node,
                    self.runtimes[node].clone(),
                    Arc::new(role),
                    own,
                    own_business,
                    &operation,
                )
                .map_err(|error| stage_error("source close", error))?;
            round
                .record_retained_closed_actual(closed)
                .map_err(|error| stage_error("source close", error))?;
        }
        let mut closed = round
            .finish()
            .map_err(|error| stage_error("source cut", error))?;
        let capture = closed
            .begin_capture(Duration::from_secs(2), &self.session)
            .map_err(|error| stage_error("source capture", error))?;
        self.plan = Some(plan.clone());
        self.closed = Some(Arc::new(closed));
        let image = self
            .install_on_target(hosts, members, &plan, capture, parent)
            .map_err(|error| stage_error("image transfer", error))?;
        self.image = Some(image);
        Ok(())
    }

    /// Ship the frozen source image and its complete history into the
    /// target's Installing state, verify it there and finalize its catalogue.
    /// After an interrupted transfer the target's recovery session still owns
    /// the dropped attempt: the same session is restarted under a new attempt
    /// and the bounded clear and copy start again.
    fn install_on_target(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        members: &Members,
        plan: &NativeJointHandoffPlan,
        mut capture: RangeImageCapture,
        parent: &CancellationToken,
    ) -> Result<(NativeImageInstallFreeze, NativeImageCatalogueFinalization)> {
        let target = self.target.clone();
        let service = members.get(&target)?.service.clone();
        let runtime = hosts.target_recovery_runtime()?;
        let session = self.session.clone();
        let handoff_role = |parent: &CancellationToken| {
            members
                .role(plan, &target, parent)
                .and_then(|role| role.into_handoff())
        };
        let initial = members.role(plan, &target, parent)?;
        let scope = RecoveryFenceScope::from_role(&initial)?;
        if !self.target_fenced {
            runtime.fence_epoch(
                &members.actor,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &session,
            )?;
            self.target_fenced = true;
        }
        let handoff = handoff_role(parent)?;
        let image_role = NativeImageInstallRole::CommittedHandoff(&handoff);
        let attempt = Uuid::now_v7();
        let source = capture.authorize_target_install()?;
        let lease = if self.target_leased {
            runtime.restart_dropped_image_transfer(
                &scope,
                &source,
                &image_role,
                ImageInstallPurpose::RelocationLearner,
                attempt,
                &session,
            )?
        } else {
            runtime.transfer_image_install(
                &scope,
                &source,
                &image_role,
                ImageInstallPurpose::RelocationLearner,
                attempt,
                &session,
            )?
        };
        self.target_leased = true;
        let mut install: NativeRangeImageInstaller =
            service.lock().begin_native_range_image_install(
                &source,
                &image_role,
                lease,
                ImageInstallPurpose::RelocationLearner,
                attempt,
                &session,
            )?;
        drop(source);
        loop {
            let cursor = capture.history_cursor().clone();
            let step = capture.export_history_step(&cursor, 256, &session)?;
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            let stage_id = install.history_stage_id();
            let progress = service.lock().stage_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                stage_id,
                &step,
                &session,
            )?;
            if progress.cursor.complete() {
                break;
            }
        }
        loop {
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            if service
                .lock()
                .audit_native_range_image_history(
                    &mut install,
                    &NativeImageInstallRole::CommittedHandoff(&handoff),
                    &source,
                    256,
                    &session,
                )?
                .audit_complete
            {
                break;
            }
        }
        loop {
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            if service.lock().clear_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                &session,
            )? {
                break;
            }
        }
        let options = RangeSnapshotOptions {
            max_records_per_batch: self.image_batch_records,
            max_bytes_per_batch: 1024 * 1024,
            max_record_bytes: 1024 * 1024,
        };
        let mut copied = 0usize;
        loop {
            let cursor = capture.cursor().clone();
            let step = capture.export_step(&cursor, &options, &session)?;
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            service.lock().copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                attempt,
                &step,
                &session,
            )?;
            drop(source);
            copied += 1;
            hosts.transfer_progress(copied)?;
            if step.complete {
                break;
            }
        }
        let completed = capture.complete()?;
        let handoff = handoff_role(parent)?;
        let mut verification = service.lock().begin_native_range_image_verification(
            install,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            completed,
            &session,
        )?;
        let mut complete = false;
        for _ in 0..4096 {
            parent.check()?;
            let handoff = handoff_role(parent)?;
            if service.lock().advance_native_range_image_verification(
                &mut verification,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &session,
            )? == crate::db::NativeTargetImageVerificationProgress::Complete
            {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(handoff_error("bounded target verification did not finish"));
        }
        let handoff = handoff_role(parent)?;
        let verified = service.lock().finish_native_range_image_verification(
            verification,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &session,
        )?;
        let handoff = handoff_role(parent)?;
        let adopted = service.lock().adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &session,
        )?;
        let handoff = handoff_role(parent)?;
        let finalized = service.lock().finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &session,
        );
        finalized
    }

    /// Materialize the new set on the same cut and publish the complete
    /// decision on every old member and the target. Once the publication
    /// round exists it is retained, so a later call publishes only the
    /// members that are still missing.
    fn publish_decision(&mut self, members: &Members, parent: &CancellationToken) -> Result<()> {
        if self.publishing.is_none() {
            let closed = self
                .closed
                .clone()
                .ok_or_else(|| handoff_error("closed source custody absent before decision"))?;
            let plan = closed.plan();
            let survivors = plan
                .new_members()
                .keys()
                .filter(|node| **node != self.target)
                .cloned()
                .collect::<Vec<_>>();
            // Every fallible acquisition precedes the consumption of the
            // finalized image, which exists exactly once.
            let handoff = members
                .role(&plan, &self.target, parent)
                .and_then(|role| role.into_handoff())
                .map_err(|error| stage_error("target materialization role", error))?;
            let mut round = NativeJointNewRound::begin(closed.clone())
                .map_err(|error| stage_error("new round", error))?;
            for node in &survivors {
                let role = members.role(&plan, node, parent)?;
                round
                    .record_actual_survivor(node, role)
                    .map_err(|error| stage_error("survivor materialization", error))?;
            }
            let (frozen, image) = self
                .image
                .take()
                .ok_or_else(|| handoff_error("finalized image custody absent before decision"))?;
            let service = members.get(&self.target)?.service.clone();
            let target = service
                .lock()
                .observe_native_joint_new_target(&closed, handoff, image, frozen, &self.session)
                .map_err(|error| stage_error("target materialization", error))?;
            round
                .record(
                    NativeJointNewObservation::from_finalized_actual_target(target, service)
                        .map_err(|error| stage_error("target materialization", error))?,
                )
                .map_err(|error| stage_error("target materialization", error))?;
            let publication = round
                .finish()
                .and_then(|quorums| quorums.prepare_decision())
                .map_err(|error| stage_error("decision preparation", error))?
                .publication_round();
            self.publishing = Some((publication, BTreeSet::new()));
        }
        let plan = self
            .plan
            .clone()
            .ok_or_else(|| handoff_error("plan custody absent before decision publication"))?;
        let mut publishers = plan.old_members().keys().cloned().collect::<Vec<_>>();
        publishers.push(self.target.clone());
        let session = self.session.clone();
        let (publication, done) = self.publishing.as_mut().unwrap();
        for node in publishers {
            if done.contains(&node) {
                continue;
            }
            retry_timed_out(|| {
                let role = members.role(&plan, &node, parent)?;
                publication.publish_actual_member(role, &session)
            })
            .map_err(|error| stage_error("decision publication", error))?;
            done.insert(node);
        }
        let (publication, _) = self.publishing.take().unwrap();
        self.decision = Some(
            publication
                .finish()
                .map_err(|error| stage_error("decision quorum", error))?,
        );
        Ok(())
    }

    /// One staged metadata membership step, built from the retained durable
    /// decision on a planning view of the fresh committed configuration and
    /// committed by the real metadata consensus. `committed` is the stage the
    /// committed relocation records before this step.
    fn commit_membership_stage(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        committed: Option<NativeJointHandoffStageV1>,
        parent: &CancellationToken,
    ) -> Result<()> {
        let decision = self.decision.as_ref().ok_or_else(|| {
            handoff_error(
                "durable decision custody absent; the membership stages need the live decision",
            )
        })?;
        let actor = hosts.metadata_leader()?;
        let NativeJoinedDataPlanningView {
            mut view,
            fresh,
            business,
        } = hosts
            .planning_view(&bounded(parent, REQUEST))
            .map_err(|error| stage_error("membership planning view", error))?;
        let now = now_ms();
        match committed {
            None => {
                view.certify_native_joint_handoff(decision, &fresh, business, &actor, now)
                    .map_err(|error| stage_error("decision certification", error))?;
            }
            Some(NativeJointHandoffStageV1::Certified) => {
                view.promote_native_joint_handoff_frozen(decision, &fresh, business, &actor, now)
                    .map_err(|error| stage_error("union membership", error))?;
            }
            Some(NativeJointHandoffStageV1::PromotedFrozen) => {
                view.finalize_native_joint_membership_frozen(
                    decision, &fresh, business, &actor, now,
                )
                .map_err(|error| stage_error("final membership", error))?;
            }
            Some(_) => return Err(handoff_error("membership stage is already final")),
        }
        hosts
            .commit_topology(view.topology().clone(), parent)
            .map_err(|error| stage_error("membership commit", error))
    }

    /// Policy continuation, serving preparation, bridge publication, Open,
    /// release and target ingress. Every finished part is retained, so a
    /// later call continues after the last finished part.
    fn open_and_serve(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        members: &Members,
        parent: &CancellationToken,
    ) -> Result<usize> {
        let plan = self
            .plan
            .clone()
            .ok_or_else(|| handoff_error("plan custody absent before activation"))?;
        let range = plan.old_range().id;
        let leader = members.metadata_leader.clone();
        let new = plan.new_members().keys().cloned().collect::<Vec<_>>();
        let survivors = new
            .iter()
            .filter(|node| **node != self.target)
            .cloned()
            .collect::<Vec<_>>();
        let mut order = vec![self.target.clone()];
        order.extend(survivors.iter().cloned());
        let session = self.session.clone();
        if !self.policies_installed {
            for node in plan.old_members().keys() {
                hosts.forget_old_closure(node, range)?;
            }
            for node in hosts.data_owners()? {
                hosts
                    .refresh_owner_policy(&node, &bounded(parent, HOST_OPERATION))
                    .map_err(|error| stage_error("owner policy continuation", error))?;
            }
            hosts
                .refresh_target_policy(&bounded(parent, HOST_OPERATION))
                .map_err(|error| stage_error("target policy continuation", error))?;
            self.policies_installed = true;
        }
        if self.prepared.is_none() && self.published.is_none() && self.continuation.is_none() {
            let decision = self.decision.take().ok_or_else(|| {
                handoff_error("durable decision custody absent before serving preparation")
            })?;
            let mut round: Option<NativeJointPreparedServingRound> = None;
            for (local, (service, lease)) in decision
                .into_new_serving_owners()
                .map_err(|error| stage_error("serving owners", error))?
            {
                let business = Uuid::now_v7();
                let token = bounded(parent, REQUEST);
                let fresh = members
                    .get(&local)?
                    .client
                    .acquire_host_metadata_observation(&leader, business, REQUEST, &token)
                    .map_err(|error| stage_error("serving preparation metadata", error))?;
                let actual = {
                    let actual = service.lock();
                    let committed = actual
                        .observe_committed_native_joint_decision(fresh, business, range)
                        .map_err(|error| stage_error("committed decision", error))?;
                    actual
                        .prepare_local_native_joint_serving(committed, lease, &session)
                        .map_err(|error| stage_error("serving preparation", error))?
                };
                let observation =
                    NativeJointPreparedServingObservation::from_actual_service(actual, service)
                        .map_err(|error| stage_error("serving preparation", error))?;
                if let Some(round) = &mut round {
                    round
                        .record(observation)
                        .map_err(|error| stage_error("serving preparation", error))?;
                } else {
                    round = Some(
                        NativeJointPreparedServingRound::begin(observation, &session)
                            .map_err(|error| stage_error("serving preparation", error))?,
                    );
                }
            }
            let prepared = round
                .ok_or_else(|| handoff_error("no new serving owner"))?
                .finish()
                .map_err(|error| stage_error("serving preparation quorum", error))?;
            self.prepared = Some(Arc::new(prepared));
        }
        if self.published.is_none() && self.continuation.is_none() {
            let prepared = self
                .prepared
                .clone()
                .ok_or_else(|| handoff_error("prepared serving custody absent"))?;
            let fresh_phase = || -> Result<Arc<NativeJointPreparationPhaseQuorum>> {
                let mut fresh = prepared.begin_fresh_preparation_phase(PHASE)?;
                for node in &new {
                    fresh.acquire_actual_member(&members.get(node)?.client, &leader)?;
                }
                Ok(Arc::new(fresh.finish()?))
            };
            let mut publication = prepared
                .resume_native_publication()
                .map_err(|error| stage_error("serving publication", error))?;
            for local in &new {
                retry_timed_out(|| {
                    let phase = fresh_phase()?;
                    let admission =
                        phase.acquire_local_activation(&members.get(local)?.client, &leader)?;
                    publication.publish_actual_local_member(&admission, &session)
                })
                .map_err(|error| stage_error("serving publication", error))?;
            }
            let phase = retry_timed_out(fresh_phase)
                .map_err(|error| stage_error("serving publication aggregate", error))?;
            self.published = Some(Arc::new(
                publication
                    .finish_with_fresh_phase(phase)
                    .map_err(|error| stage_error("serving publication quorum", error))?,
            ));
        }
        if self.continuation.is_none() {
            let published = self
                .published
                .clone()
                .ok_or_else(|| handoff_error("published serving custody absent"))?;
            for local in &order {
                if self.activations.contains_key(local) {
                    continue;
                }
                let image_local = *local == self.target;
                let (receipt, owner) = retry_timed_out(|| {
                    let mut round = published.begin_fresh_open_phase(PHASE)?;
                    for node in &new {
                        round.acquire_actual_member(&members.get(node)?.client, &leader)?;
                    }
                    let round = Arc::new(round.finish()?);
                    let open = round.acquire_local_open(&members.get(local)?.client, &leader)?;
                    let receipt = Arc::new(open.persist_local_catalogue_activation(&session)?);
                    let owner = if image_local {
                        Some(open.retain_image_release_owner(receipt.clone())?)
                    } else {
                        None
                    };
                    Ok((receipt, owner))
                })
                .map_err(|error| stage_error("joint Open", error))?;
                if let Some(owner) = owner {
                    self.image_owner = Some(owner);
                }
                self.activations.insert(local.clone(), receipt);
            }
            let image = self
                .image_owner
                .take()
                .ok_or_else(|| handoff_error("target image release owner absent"))?;
            let mut runtimes = BTreeMap::new();
            for node in &survivors {
                let runtime = self
                    .runtimes
                    .get(node)
                    .cloned()
                    .ok_or_else(|| handoff_error("survivor freeze owner absent"))?;
                runtimes.insert(node.clone(), runtime);
            }
            let continuation = published
                .retain_release_continuation(self.activations.clone(), runtimes, image)
                .map_err(|error| stage_error("release continuation", error))?;
            self.continuation = Some(Arc::new(continuation));
            // The real freeze owners must drain before release.
            drop(published);
            self.published = None;
            self.prepared = None;
            self.closed = None;
        }
        let continuation = self
            .continuation
            .clone()
            .ok_or_else(|| handoff_error("release continuation custody absent"))?;
        for local in &order {
            if self.released.contains(local) {
                continue;
            }
            retry_timed_out(|| {
                let mut round = continuation.begin_fresh_release_phase(PHASE)?;
                for node in &new {
                    round.acquire_actual_member(&members.get(node)?.client, &leader)?;
                }
                Arc::new(round.finish()?)
                    .acquire_local_release(&members.get(local)?.client, &leader)?
                    .release_actual_local()
            })
            .map_err(|error| stage_error("joint release", error))?;
            self.released.insert(local.clone());
        }
        hosts
            .publish_target(&bounded(parent, LEADERSHIP))
            .map_err(|error| stage_error("target leadership and ingress", error))
    }
}

/// One committed learner allocation of `target` on the range, retiring the
/// current range leader. The target must be an enrolled member that is not a
/// replica of any range.
fn plan_learner_allocation(
    current: &ClusterTopology,
    range_id: RangeId,
    target: &ClusterNodeId,
) -> Result<ClusterTopology> {
    let mut proposed = current.clone();
    if proposed
        .nodes
        .get(target)
        .is_none_or(|node| node.native_store_birth_v1.is_none())
    {
        return Err(handoff_error(
            "target is not an enrolled member with a native store birth",
        ));
    }
    if proposed.ranges.values().any(|range| {
        range
            .replicas
            .iter()
            .any(|replica| &replica.node_id == target)
    }) {
        return Err(handoff_error("target already holds a range replica"));
    }
    if proposed
        .relocations
        .values()
        .any(|plan| plan.range_id == range_id && plan.phase != RelocationPhase::Completed)
    {
        return Err(handoff_error("range already has a committed relocation"));
    }
    let id = RelocationId::new(proposed.next_relocation_id)?;
    proposed.next_relocation_id += 1;
    let target_replica = ReplicaId::new(proposed.next_replica_id)?;
    proposed.next_replica_id += 1;
    let now = now_ms();
    let range = proposed
        .ranges
        .values_mut()
        .find(|range| range.id == range_id)
        .ok_or_else(|| handoff_error("range absent from committed metadata"))?;
    let expected_epoch = range.epoch;
    let source = range.leader.clone();
    range.epoch += 1;
    range.replicas.push(RangeReplica {
        id: target_replica,
        node_id: target.clone(),
        role: RangeReplicaRole::Learner,
    });
    proposed.relocations.insert(
        id,
        RangeRelocation {
            id,
            plan_id: format!("joined-data-{range_id}"),
            handoff_session: Some(RangeHandoffSession {
                session_id: Uuid::now_v7(),
                nonce: Uuid::now_v7(),
            }),
            native_joint_handoff_v1: None,
            range_id,
            expected_epoch,
            learner_epoch: expected_epoch + 1,
            promoted_epoch: None,
            source: Some(source),
            target: target.clone(),
            target_replica_id: target_replica,
            reason: ReplicaMoveReason::LoadBalance,
            phase: RelocationPhase::LearnerAllocated,
            resume_phase: None,
            snapshot_id: None,
            snapshot_sha256: None,
            snapshot_bytes_copied: 0,
            snapshot_resume_after_key: None,
            snapshot_commit_sequence: None,
            destination_durable_commit_sequence: None,
            source_commit_sequence: None,
            cleanup_resume_after_key: None,
            cleanup_records_deleted: 0,
            created_at_ms: now,
            updated_at_ms: now,
            last_error: None,
        },
    );
    proposed.generation += 1;
    proposed.validate()?;
    Ok(proposed)
}
