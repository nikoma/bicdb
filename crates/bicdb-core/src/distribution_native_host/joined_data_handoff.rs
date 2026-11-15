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
//!
//! Seam. This module holds the stage logic only: the stage derivation from the
//! committed metadata, the abort boundary, the resume rules and the metadata
//! commits. Everything that touches a member (its service, runtime, transport
//! client) and the custody above sit behind `JoinedHandoffBackend`.
//! `local_backend` is the in-process implementation. A remote implementation
//! supplies the same stages by asking the members to act; it must never build
//! a local quorum, role or freeze object from reply data. Those objects exist
//! only where the member's own service issued them.
// No production caller yet: the driver is reached only from the in-process
// multi-node fixture. Remove this once a controller drives it.
#![allow(dead_code)]
use super::*;
use crate::distribution::{
    NativeJointHandoffStageV1, RangeDescriptor, RangeHandoffSession, RangeId, RangeRelocation,
    RangeReplica, RelocationId, RelocationPhase, ReplicaId, ReplicaMoveReason,
};
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_handoff::{NativeJointHandoffRuntime, NativeJointOldObservation};
use crate::distribution_range_recovery::{RangeRecoveryRuntime, RecoveryRoleAdmission};
use crate::distribution_transport::TcpClusterRelocationTransport;
use crate::{ClusterDataNodeService, DistributionStore};
use parking_lot::Mutex;
use std::time::Instant;

mod local_backend;
pub(crate) use local_backend::LocalJoinedHandoffBackend;
#[cfg(test)]
pub(crate) use local_backend::TestHandoffCustodyCut;

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

/// The member-touching work of one handoff, at stage granularity. The stage
/// logic calls these in the order the committed metadata and the answers of
/// `cut_started`, `cut_required` and `decision_durable` dictate; an
/// implementation retains whatever a stage issued so that a repeated call
/// continues after the last finished part.
///
/// An implementation never commits metadata and never decides an abort.
pub(crate) trait JoinedHandoffBackend {
    /// The member handles of one `drive_until` call.
    type Members;
    /// True once a source round was started or a freeze owner exists: a
    /// permanent handoff promise may exist and the abort is refused. It must
    /// become true before the first member mutation of `cut_and_ship`.
    fn cut_started(&self) -> bool;
    /// The durable range-log epoch of the old voter `node`. An unreadable
    /// voter is an error.
    fn old_epoch(
        &self,
        hosts: &dyn NativeJoinedDataHandoffHosts,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
    ) -> Result<u64>;
    /// The abort was committed: drop what was retained for the aborted
    /// allocation.
    fn forget_aborted_allocation(&mut self);
    /// Install the target's writer policy and DATA socket. Repeatable.
    fn admit_target(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        parent: &CancellationToken,
    ) -> Result<()>;
    /// Resolve the participating members of the committed relocation.
    fn members(
        &self,
        hosts: &dyn NativeJoinedDataHandoffHosts,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
    ) -> Result<Self::Members>;
    /// No finalized image, publication round or durable decision is held.
    fn cut_required(&self) -> bool;
    /// Source cut, transfer and verification onto the target.
    fn cut_and_ship(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        members: &Self::Members,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()>;
    /// The durable joint decision is held.
    fn decision_durable(&self) -> bool;
    /// Publish the joint decision on an old and a new data majority.
    fn decide(&mut self, members: &Self::Members, parent: &CancellationToken) -> Result<()>;
    /// Refuse a membership stage without the live durable decision, before
    /// any metadata is read for it.
    fn require_decision(&self) -> Result<()>;
    /// Apply one staged membership step to the caller's planning `view`,
    /// which then holds the candidate configuration. `committed` is the stage
    /// the committed relocation records before this step.
    fn stage_candidate(
        &mut self,
        committed: Option<NativeJointHandoffStageV1>,
        view: &mut DistributionStore,
        fresh: &HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now: u64,
    ) -> Result<()>;
    /// Policy continuation through release.
    fn activate(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        members: &Self::Members,
        parent: &CancellationToken,
    ) -> Result<()>;
    /// Target leadership and ingress; the ranges the target recovered.
    fn serve(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        parent: &CancellationToken,
    ) -> Result<usize>;
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TestHandoffCustodyCut {
    AfterTargetNew,
    BeforePreparationOwn,
    AfterFirstNativePreparation,
}

/// One handoff of `range` onto `target`: the stage logic over a backend that
/// holds the custody described in the module documentation.
pub(crate) struct NativeJoinedDataHandoff<B: JoinedHandoffBackend = LocalJoinedHandoffBackend> {
    range: RangeId,
    target: ClusterNodeId,
    backend: B,
    /// Ranges the target recovered when it opened ingress; set on completion.
    recovered: Option<usize>,
}

impl NativeJoinedDataHandoff<LocalJoinedHandoffBackend> {
    pub(crate) fn new(range: RangeId, target: ClusterNodeId) -> Self {
        Self {
            range,
            backend: LocalJoinedHandoffBackend::new(target.clone()),
            target,
            recovered: None,
        }
    }
    #[cfg(test)]
    pub(crate) fn test_cancel_custody_once(&mut self, cut: TestHandoffCustodyCut) {
        self.backend.test_cancel_custody_once(cut);
    }
    #[cfg(test)]
    pub(crate) fn test_custody_snapshot(&self) -> serde_json::Value {
        self.backend.test_custody_snapshot()
    }
    /// Ask the target runtime to restart a dropped transfer in every state in
    /// which it must refuse, and record what it answered.
    #[cfg(test)]
    pub(crate) fn test_probe_restart_refusals(mut self) -> Self {
        self.backend.test_probe_restart_refusals();
        self
    }
    #[cfg(test)]
    pub(crate) fn test_restart_probes(&self) -> &[(&'static str, String)] {
        self.backend.test_restart_probes()
    }
    #[cfg(test)]
    pub(crate) fn test_cancel_custody_once(&mut self, cut: TestHandoffCustodyCut) {
        self.backend.test_cancel_custody_once(cut);
    }
    #[cfg(test)]
    pub(crate) fn test_custody_snapshot(&self) -> serde_json::Value {
        self.backend.test_custody_snapshot()
    }
    /// Bound the records of one copied image step (1..=256).
    pub(crate) fn with_image_batch_records(mut self, records: usize) -> Self {
        self.backend.set_image_batch_records(records);
        self
    }
    /// True once a source round was started. Abort is refused from then on.
    pub(crate) fn cut_attempted(&self) -> bool {
        self.backend.cut_attempted()
    }
}

impl<B: JoinedHandoffBackend> NativeJoinedDataHandoff<B> {
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

    /// Return the range to its original voters by committing the original
    /// range descriptor again. See the module documentation for the boundary.
    /// A later `drive` starts a new learner allocation.
    pub(crate) fn abort_before_cut(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.backend.cut_started() {
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
            let current_epoch = self.backend.old_epoch(&*hosts, &replica.node_id, &range)?;
            if current_epoch > relocation.expected_epoch {
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
        self.backend.forget_aborted_allocation();
        Ok(())
    }

    /// Advance until `until` is complete, or to completion. Every stage is
    /// derived from the committed metadata and the backend's custody, so the
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
        self.backend
            .admit_target(hosts, parent)
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
        let members = self.backend.members(&*hosts, &range, &relocation)?;
        if relocation.native_joint_handoff_v1.is_some() {
            if let Some(stage @ (Stage::ImageVerified | Stage::DecisionPublished)) = until {
                return Ok(stage);
            }
        } else {
            // Steps 3 and 4: source cut, transfer and verification.
            if self.backend.cut_required() {
                self.backend
                    .cut_and_ship(hosts, &members, &range, &relocation, parent)?;
            }
            if until == Some(Stage::ImageVerified) {
                return Ok(Stage::ImageVerified);
            }
            // Step 5: the joint decision on an old and a new data majority.
            if !self.backend.decision_durable() {
                self.backend.decide(&members, parent)?;
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
        self.backend.activate(hosts, &members, parent)?;
        let recovered = self.backend.serve(hosts, parent)?;
        self.recovered = Some(recovered);
        Ok(Stage::Serving)
    }

    /// One staged metadata membership step, built by the backend from its
    /// retained durable decision on a planning view of the fresh committed
    /// configuration and committed by the real metadata consensus.
    /// `committed` is the stage the committed relocation records before this
    /// step.
    fn commit_membership_stage(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        committed: Option<NativeJointHandoffStageV1>,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.backend.require_decision()?;
        let actor = hosts.metadata_leader()?;
        let NativeJoinedDataPlanningView {
            mut view,
            fresh,
            business,
        } = hosts
            .planning_view(&bounded(parent, REQUEST))
            .map_err(|error| stage_error("membership planning view", error))?;
        let now = now_ms();
        self.backend
            .stage_candidate(committed, &mut view, &fresh, business, &actor, now)?;
        hosts
            .commit_topology(view.topology().clone(), parent)
            .map_err(|error| stage_error("membership commit", error))
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

/// Existing pure allocation planner DATA for the actual-host acceptance setup.
/// This does not commit metadata or create a Role/OLD/native permission.
#[cfg(test)]
pub(in crate::distribution_native_host) fn test_first_allocation_candidate(
    current: &ClusterTopology,
    range: RangeId,
    target: &ClusterNodeId,
) -> Result<ClusterTopology> {
    plan_learner_allocation(current, range, target)
}
