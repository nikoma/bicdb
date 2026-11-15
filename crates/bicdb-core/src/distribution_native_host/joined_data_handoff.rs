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
//! is not covered before the activation record below is committed.
//!
//! Completion. Serving is not the end of a relocation. After the target
//! serves, a backend that records completion commits two further metadata
//! stages. The activation record (`AwaitingCleanup`) commits to the durable
//! catalogue activation of every member of the new set, each observed
//! released and open; from then on the committed metadata alone says that
//! the handoff is past Serving, and a driver without custody does not drive
//! it again. The source retirement (`Cleaned`, relocation `Completed`)
//! records the tombstone the retired source wrote on its own committed
//! metadata read. It needs no custody, so a new driver object finishes it.
//! No row is deleted on the retired source and its permanent promise stays;
//! the completed relocation only stops blocking a later handoff of the range.
//!
//! Seam. This module holds the stage logic only: the stage derivation from the
//! committed metadata, the abort boundary, the resume rules and the metadata
//! commits. Everything that touches a member (its service, runtime, transport
//! client) and the custody above sit behind `JoinedHandoffBackend`.
//! `local_backend` is the in-process implementation; `remote_backend` is the
//! beginning of the one that asks the members over RPC. The stage logic itself
//! reaches the hosts only through `JoinedHandoffMetadataHosts` (committed
//! metadata reads and commits); each backend names its own host surface as
//! `JoinedHandoffBackend::Hosts`, and a backend without in-process custody
//! re-derives its retained state in `resync`. A remote implementation
//! supplies the same stages by asking the members to act; it must never build
//! a local quorum, role or freeze object from reply data. Those objects exist
//! only where the member's own service issued them.
// The production caller is `range_move` (the remote backend). The in-process
// backend and its host surface are still reached only from the multi-node
// fixture.
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
mod remote_backend;
#[cfg(test)]
pub(crate) use remote_backend::RemoteTestStop;
pub(crate) use remote_backend::{
    RemoteJoinedHandoffBackend, RemoteMembers, RemoteOldCutObservation,
};

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
    /// The target is admitted to receive this range's data; no range changed.
    /// In-process backend: it holds a writer policy and a DATA socket. A
    /// backend whose target stays a registered replica receiver until release
    /// admits it as such.
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
    /// Metadata stage: the activation of every member of the new set is
    /// recorded (`AwaitingCleanup`). The committed metadata now says that the
    /// handoff is past Serving.
    ActivationRecorded,
    /// Metadata stage: the retired source recorded its retirement and the
    /// relocation is `Completed` (`Cleaned`). Its rows are retained.
    SourceRetired,
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

/// The metadata surface of the hosts: all that the stage logic itself needs,
/// whatever the backend. Every method is an existing host action.
pub(crate) trait JoinedHandoffMetadataHosts {
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
}

/// The hosts the in-process backend acts on. Every method is an existing host
/// action; the driver holds the sequencing, the idempotence and the abort
/// boundary.
pub(crate) trait NativeJoinedDataHandoffHosts: JoinedHandoffMetadataHosts {
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
    /// The host surface this backend acts on. The stage logic uses only its
    /// metadata part.
    type Hosts: ?Sized + JoinedHandoffMetadataHosts;
    /// True once a source round was started or a freeze owner exists: a
    /// permanent handoff promise may exist and the abort is refused. It must
    /// become true before the first member mutation of `cut_and_ship`.
    fn cut_started(&self) -> bool;
    /// The durable range-log epoch of the old voter `node`. An unreadable
    /// voter is an error.
    fn old_epoch(
        &self,
        hosts: &Self::Hosts,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
    ) -> Result<u64>;
    /// The abort was committed: drop what was retained for the aborted
    /// allocation.
    fn forget_aborted_allocation(&mut self);
    /// Admit the target to receive this range's data. Repeatable. The
    /// in-process backend installs the target's writer policy and DATA
    /// socket; a backend whose target stays a registered replica receiver
    /// until release admits it as such.
    fn admit_target(&mut self, hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<()>;
    /// Called after `admit_target` while no relocation of the range onto
    /// the target is committed, so always before the learner allocation is
    /// proposed: ask the members, read-only, whether the move can start. An
    /// error refuses the move with nothing committed. The in-process backend
    /// holds its members' handles and needs nothing.
    fn preflight(
        &mut self,
        _hosts: &mut Self::Hosts,
        _range: &RangeDescriptor,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Ok(())
    }
    /// Called immediately before the learner allocation of `range` is
    /// proposed, with nothing committed for the move. A backend whose
    /// coordinator leads the range makes sure here that no write command of
    /// the range straddles the commit; an error ends the call with nothing
    /// committed. `after_allocation` follows every successful call.
    fn before_allocation(
        &mut self,
        _hosts: &mut Self::Hosts,
        _range: &RangeDescriptor,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Ok(())
    }
    /// The commit attempt that followed `before_allocation` is over.
    /// `range` is the descriptor before the allocation; `committed` is false
    /// when the proposal failed or its outcome is not known.
    fn after_allocation(
        &mut self,
        _range: &RangeDescriptor,
        _committed: bool,
        _parent: &CancellationToken,
    ) {
    }
    /// Resolve the participating members of the committed relocation.
    fn members(
        &self,
        hosts: &Self::Hosts,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
    ) -> Result<Self::Members>;
    /// Re-derive retained state from the members' durable state before any
    /// stage decision of this call. The in-process backend retains everything
    /// in memory and needs nothing.
    fn resync(
        &mut self,
        _hosts: &mut Self::Hosts,
        _members: &Self::Members,
        _range: &RangeDescriptor,
        _relocation: &RangeRelocation,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Ok(())
    }
    /// Re-derive `cut_started` from the members before an abort is decided.
    /// An error refuses the abort. The in-process backend answers
    /// `cut_started` from memory and needs nothing.
    fn resync_before_abort(
        &mut self,
        _hosts: &mut Self::Hosts,
        _range: &RangeDescriptor,
        _relocation: &RangeRelocation,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Ok(())
    }
    /// No finalized image, publication round or durable decision is held.
    fn cut_required(&self) -> bool;
    /// Source cut, transfer and verification onto the target.
    fn cut_and_ship(
        &mut self,
        hosts: &mut Self::Hosts,
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
        hosts: &mut Self::Hosts,
        members: &Self::Members,
        parent: &CancellationToken,
    ) -> Result<()>;
    /// Target leadership and ingress; the ranges the target recovered.
    fn serve(&mut self, hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<usize>;
    /// True when this backend produces the proofs of the two completion
    /// stages. A backend that does not ends at `Serving`, as before, and its
    /// relocation stays at `AwaitingActivation`.
    fn records_completion(&self) -> bool {
        false
    }
    /// Observe every member of the new set released and open inside one
    /// fresh window and retain that proof for the activation record. Called
    /// after `serve`; `stage_candidate` then consumes it for the committed
    /// stage `AwaitingActivation`.
    fn prove_activation(
        &mut self,
        _members: &Self::Members,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Err(handoff_error(
            "this backend does not produce the activation record",
        ))
    }
    /// Ask the retired source to record its retirement from the range on its
    /// own committed metadata read, and retain its receipt. It must not
    /// delete or overwrite any data of the source. `stage_candidate` then
    /// consumes the receipt for the committed stage `AwaitingCleanup`.
    fn retire_source(
        &mut self,
        _members: &Self::Members,
        _range: &RangeDescriptor,
        _relocation: &RangeRelocation,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Err(handoff_error(
            "this backend does not produce the source retirement",
        ))
    }
    /// Set when the last call of this backend ended only because a member
    /// has not yet finished work that belongs to that member's own host
    /// loop: what is awaited. The call is repeated unchanged; it is progress
    /// of the member, not a failure of the handoff. `None` for every other
    /// outcome.
    fn waiting(&self) -> Option<String> {
        None
    }
}

/// One handoff of `range` onto `target`: the stage logic over a backend that
/// holds the custody described in the module documentation.
pub(crate) struct NativeJoinedDataHandoff<B: JoinedHandoffBackend = LocalJoinedHandoffBackend> {
    range: RangeId,
    target: ClusterNodeId,
    backend: B,
    /// Ranges the target recovered when it opened ingress; set once served.
    recovered: Option<usize>,
    /// The final stage, once this object reached or observed it.
    finished: Option<NativeJoinedDataHandoffStage>,
}

impl NativeJoinedDataHandoff<LocalJoinedHandoffBackend> {
    pub(crate) fn new(range: RangeId, target: ClusterNodeId) -> Self {
        let backend = LocalJoinedHandoffBackend::new(target.clone());
        Self::with_backend(range, target, backend)
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
    /// A handoff of `range` onto `target` over the given backend.
    pub(crate) fn with_backend(range: RangeId, target: ClusterNodeId, backend: B) -> Self {
        Self {
            range,
            target,
            backend,
            recovered: None,
            finished: None,
        }
    }
    /// The backend and what it retains, read-only.
    pub(crate) fn backend(&self) -> &B {
        &self.backend
    }
    /// True once the handoff reached the last stage its backend produces.
    pub(crate) fn completed(&self) -> bool {
        self.finished.is_some()
    }
    /// Run the handoff to completion. Repeating it after completion changes
    /// nothing and returns the same stage.
    pub(crate) fn drive(
        &mut self,
        hosts: &mut B::Hosts,
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
    fn recorded(relocation: &RangeRelocation) -> Option<NativeJointHandoffStageV1> {
        relocation
            .native_joint_handoff_v1
            .as_ref()
            .map(|record| record.stage())
    }
    /// The committed metadata holds a completed handoff of the range onto
    /// the target whose replica is still the target's voter replica there.
    fn committed_complete(&self, topology: &ClusterTopology) -> bool {
        let Some(range) = topology.range_by_id(self.range) else {
            return false;
        };
        topology.relocations.values().any(|plan| {
            plan.range_id == self.range
                && plan.target == self.target
                && plan.phase == RelocationPhase::Completed
                && Self::recorded(plan) == Some(NativeJointHandoffStageV1::Cleaned)
                && range.replicas.iter().any(|replica| {
                    replica.id == plan.target_replica_id
                        && replica.node_id == self.target
                        && replica.role == RangeReplicaRole::Voter
                })
        })
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
        hosts: &mut B::Hosts,
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
        // A backend without memory of its own asks the members first.
        self.backend
            .resync_before_abort(hosts, &range, &relocation, parent)?;
        if self.backend.cut_started() {
            return Err(handoff_error(
                "a source round was started and a permanent handoff promise may exist; the handoff can only be resumed",
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
        hosts: &mut B::Hosts,
        until: Option<NativeJoinedDataHandoffStage>,
        parent: &CancellationToken,
    ) -> Result<NativeJoinedDataHandoffStage> {
        use NativeJoinedDataHandoffStage as Stage;
        parent.check()?;
        if let Some(finished) = self.finished {
            return Ok(finished);
        }
        // The committed metadata decides alone whether the handoff is past
        // Serving. Such a handoff is never driven through its stages again,
        // with or without custody.
        let topology = hosts.committed_topology()?;
        let recorded = self.relocation(&topology).as_ref().and_then(Self::recorded);
        if recorded < Some(NativeJointHandoffStageV1::AwaitingCleanup) {
            if self.committed_complete(&topology) {
                self.finished = Some(Stage::SourceRetired);
                return Ok(Stage::SourceRetired);
            }
            if let Some(stopped) = self.drive_to_activation_record(hosts, until, parent)? {
                return Ok(stopped);
            }
        } else if let Some(earlier) = until.filter(|stage| *stage <= Stage::Serving) {
            return Ok(earlier);
        }
        if until == Some(Stage::ActivationRecorded) {
            return Ok(Stage::ActivationRecorded);
        }
        // Step 11: the retired source records its retirement; the relocation
        // completes. Nothing here needs the custody of the earlier stages.
        let topology = hosts.committed_topology()?;
        let relocation = self
            .relocation(&topology)
            .ok_or_else(|| handoff_error("committed relocation disappeared"))?;
        if Self::recorded(&relocation) != Some(NativeJointHandoffStageV1::AwaitingCleanup) {
            return Err(handoff_error(
                "committed relocation does not record its activation quorum",
            ));
        }
        let range = self.range(&topology)?;
        let members = self.backend.members(&*hosts, &range, &relocation)?;
        self.backend
            .retire_source(&members, &range, &relocation, parent)?;
        self.commit_membership_stage(
            hosts,
            Some(NativeJointHandoffStageV1::AwaitingCleanup),
            parent,
        )?;
        if !self.committed_complete(&hosts.committed_topology()?) {
            return Err(handoff_error(
                "committed relocation is not completed after the source retirement",
            ));
        }
        self.finished = Some(Stage::SourceRetired);
        Ok(Stage::SourceRetired)
    }

    /// Steps 1 to 10: everything up to the committed activation record.
    /// Returns the stage at which this call stops, or `None` when the
    /// activation record is committed and the completion continues.
    fn drive_to_activation_record(
        &mut self,
        hosts: &mut B::Hosts,
        until: Option<NativeJoinedDataHandoffStage>,
        parent: &CancellationToken,
    ) -> Result<Option<NativeJoinedDataHandoffStage>> {
        use NativeJoinedDataHandoffStage as Stage;
        // Step 1: target admission, before any range configuration changes.
        self.backend
            .admit_target(hosts, parent)
            .map_err(|error| stage_error("target admission", error))?;
        let mut topology = hosts.committed_topology()?;
        if self.relocation(&topology).is_none() {
            // Nothing is committed for this move yet: the members are asked
            // before the allocation below can be proposed.
            let range = self.range(&topology)?;
            self.backend
                .preflight(hosts, &range, parent)
                .map_err(|error| stage_error("target admission", error))?;
        }
        if until == Some(Stage::TargetAdmitted) {
            return Ok(Some(Stage::TargetAdmitted));
        }
        // Step 2: the committed learner allocation.
        if self.relocation(&topology).is_none() {
            let candidate = plan_learner_allocation(&topology, self.range, &self.target)?;
            let before = self.range(&topology)?;
            self.backend
                .before_allocation(hosts, &before, parent)
                .map_err(|error| stage_error("learner allocation", error))?;
            let committed = hosts.commit_topology(candidate, parent);
            self.backend
                .after_allocation(&before, committed.is_ok(), parent);
            committed.map_err(|error| stage_error("learner allocation commit", error))?;
            topology = hosts.committed_topology()?;
        }
        let mut relocation = self
            .relocation(&topology)
            .ok_or_else(|| handoff_error("committed learner allocation absent"))?;
        if until == Some(Stage::LearnerAllocated) {
            return Ok(Some(Stage::LearnerAllocated));
        }
        let range = self.range(&topology)?;
        let members = self.backend.members(&*hosts, &range, &relocation)?;
        self.backend
            .resync(hosts, &members, &range, &relocation, parent)?;
        if relocation.native_joint_handoff_v1.is_some() {
            if let Some(stage @ (Stage::ImageVerified | Stage::DecisionPublished)) = until {
                return Ok(Some(stage));
            }
        } else {
            // Steps 3 and 4: source cut, transfer and verification.
            if self.backend.cut_required() {
                self.backend
                    .cut_and_ship(hosts, &members, &range, &relocation, parent)?;
            }
            if until == Some(Stage::ImageVerified) {
                return Ok(Some(Stage::ImageVerified));
            }
            // Step 5: the joint decision on an old and a new data majority.
            if !self.backend.decision_durable() {
                self.backend.decide(&members, parent)?;
            }
            if until == Some(Stage::DecisionPublished) {
                return Ok(Some(Stage::DecisionPublished));
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
            if Self::recorded(&relocation) == stage {
                self.backend.require_decision()?;
                self.commit_membership_stage(hosts, stage, parent)?;
                topology = hosts.committed_topology()?;
                relocation = self
                    .relocation(&topology)
                    .ok_or_else(|| handoff_error("committed relocation disappeared"))?;
            }
            if until == Some(reached) {
                return Ok(Some(reached));
            }
        }
        if Self::recorded(&relocation) != Some(NativeJointHandoffStageV1::AwaitingActivation) {
            return Err(handoff_error(
                "committed relocation is not awaiting activation after the membership stages",
            ));
        }
        // Steps 7 to 9: policy continuation, Open and release, serving. A
        // served handoff is not activated or published a second time.
        if self.recovered.is_none() {
            self.backend.activate(hosts, &members, parent)?;
            let recovered = self.backend.serve(hosts, parent)?;
            self.recovered = Some(recovered);
        }
        if !self.backend.records_completion() {
            self.finished = Some(Stage::Serving);
            return Ok(Some(Stage::Serving));
        }
        if until == Some(Stage::Serving) {
            return Ok(Some(Stage::Serving));
        }
        // Step 10: the activation record. The proof lives inside one fresh
        // window, so the proof and its commit are repeated together.
        retry_timed_out(|| {
            self.backend
                .prove_activation(&members, parent)
                .map_err(|error| stage_error("activation quorum", error))?;
            self.commit_membership_stage(
                hosts,
                Some(NativeJointHandoffStageV1::AwaitingActivation),
                parent,
            )
        })?;
        Ok(None)
    }

    /// One staged metadata membership step, built by the backend from its
    /// retained durable decision on a planning view of the fresh committed
    /// configuration and committed by the real metadata consensus.
    /// `committed` is the stage the committed relocation records before this
    /// step. The two completion stages are built the same way from the
    /// backend's retained activation proof and source retirement receipt.
    fn commit_membership_stage(
        &mut self,
        hosts: &mut B::Hosts,
        committed: Option<NativeJointHandoffStageV1>,
        parent: &CancellationToken,
    ) -> Result<()> {
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

/// A relocation of the range is committed and not `Completed`. A joint
/// relocation is `Completed` only once its retired source recorded its
/// retirement, so an unfinished handoff refuses the next one.
fn range_has_unfinished_relocation(topology: &ClusterTopology, range_id: RangeId) -> bool {
    topology
        .relocations
        .values()
        .any(|plan| plan.range_id == range_id && plan.phase != RelocationPhase::Completed)
}
#[cfg(test)]
pub(crate) fn test_range_has_unfinished_relocation(
    topology: &ClusterTopology,
    range_id: RangeId,
) -> bool {
    range_has_unfinished_relocation(topology, range_id)
}
#[cfg(test)]
pub(crate) fn test_plan_learner_allocation(
    current: &ClusterTopology,
    range_id: RangeId,
    target: &ClusterNodeId,
) -> Result<ClusterTopology> {
    plan_learner_allocation(current, range_id, target)
}

/// One committed learner allocation of `target` on the range, retiring the
/// current range leader. The target must be an enrolled member that is not a
/// replica of any range.
pub(super) fn plan_learner_allocation(
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
    if range_has_unfinished_relocation(&proposed, range_id) {
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
