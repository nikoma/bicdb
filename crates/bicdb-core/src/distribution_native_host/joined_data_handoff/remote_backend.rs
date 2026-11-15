//! The remote backend of the joined data handoff: the coordinator asks the
//! members to act over the existing product RPCs. It holds identity DATA only
//! (node ids, the committed plan scope) plus the coordinator's own transport.
//!
//! Nothing here builds a role, quorum, freeze, lease or certificate from
//! reply data. The only retained reply objects are the authenticated frames
//! the pinned transport minted for this coordinator's own requests; every
//! member proves its own role and metadata again inside its receiver.
//!
//! Who may coordinate. Every request is sent as the node of the transport
//! this backend was given, and the existing checks decide what that node may
//! do; none is relaxed here:
//! - the pending plan comes from a range metadata observation, which the
//!   metadata leader grants to the current range leader only;
//! - a source Close is admitted from the range leader or the metadata leader;
//! - the read-only OLD observation is admitted from an old member only;
//! - the old voter epoch read is answered to the range leader only, and only
//!   while the voter's installed writer policy is the asked descriptor.
//! A coordinator in the range leader's process therefore passes the plan,
//! Close and observation checks. A coordinator in the metadata leader's
//! process that is not the range leader cannot obtain the plan and stops
//! there with the metadata leader's refusal.
//!
//! Implemented: the old voter epoch read, the member resolution, the source
//! Close round and the re-derivation of "a cut was started". Every other
//! stage answers a "not implemented" error.
use super::*;
use crate::distribution_range_consensus::RangeWriteTransport;
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeJointOldRound, NativeRemoteOldOperationV1,
    NativeRemoteOldRequestV1,
};
use crate::distribution_transport::AuthenticatedNativeRemoteOldFrame;
use std::collections::BTreeMap;

/// Receiver-side budget of one OLD request; the receiver bounds it to 5 s.
const OLD_REQUEST_MS: u64 = 4000;
/// The receiver's refusal of a read-only observation when its process retains
/// no operation of the range. The read-only observation has no typed "absent"
/// answer, so this text is the only way to tell absence from a failure.
const OBSERVE_ABSENT: &str = "OLD source has no retained actual operation";
/// Receiver refusals that prove a retained operation exists without a
/// completed closure: a Close is in flight or stopped after its promise.
const OBSERVE_IN_FLIGHT: [&str; 2] = [
    "OLD source native closure is not known complete",
    "OLD source operation still active",
];

fn remote_error(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("remote joined handoff backend: {}", detail.into()))
}
fn not_implemented(stage: &str) -> BicDbError {
    remote_error(format!("{stage} is not implemented"))
}

/// The participants of one committed relocation, as node ids only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemoteMembers {
    pub(crate) metadata_leader: ClusterNodeId,
    /// The voters of the range before the handoff, the retiring source
    /// included.
    pub(crate) old_voters: Vec<ClusterNodeId>,
    /// The surviving old voters and the target.
    pub(crate) new_members: Vec<ClusterNodeId>,
    pub(crate) target: ClusterNodeId,
    /// The retiring replica. It is placement data, not the image donor.
    pub(crate) source: ClusterNodeId,
}

/// What one old member answered to the read-only observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RemoteOldCutObservation {
    /// The member's process retains an operation of this exact plan scope.
    /// `closed` tells whether it reported a completed closure.
    Retained { closed: bool },
    /// The member's process retains an operation whose closure is not
    /// complete. A permanent promise may exist.
    InFlight,
    /// The member's process retains no operation of the range. This is the
    /// answer of that process's memory, not of the member's durable state.
    Absent,
    /// The member gave no usable answer.
    Inconclusive(String),
}
impl RemoteOldCutObservation {
    fn started(&self) -> bool {
        matches!(self, Self::Retained { .. } | Self::InFlight)
    }
}

pub(crate) struct RemoteJoinedHandoffBackend {
    target: ClusterNodeId,
    /// The coordinator's own transport; every request is sent as this node.
    transport: Arc<TcpClusterRelocationTransport>,
    /// A source round was started by this object, or a member reported a
    /// retained operation: a permanent promise may exist.
    cut_started: bool,
    /// Scope checksum of the plan the retained frames belong to.
    closed_scope_sha256: Option<String>,
    /// The authenticated Close frames of the last source round, as the
    /// transport minted them. They are evidence that each member answered a
    /// Close of this scope; they are never a quorum.
    closed: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteOldFrame>,
    /// The answers of the last read-only observation round.
    observed: Vec<(ClusterNodeId, RemoteOldCutObservation)>,
}

impl RemoteJoinedHandoffBackend {
    pub(crate) fn new(
        target: ClusterNodeId,
        transport: Arc<TcpClusterRelocationTransport>,
    ) -> Self {
        Self {
            target,
            transport,
            cut_started: false,
            closed_scope_sha256: None,
            closed: BTreeMap::new(),
            observed: Vec::new(),
        }
    }
    fn coordinator(&self) -> ClusterNodeId {
        self.transport
            .local_metadata_read_participant()
            .node_id()
            .clone()
    }
    /// The authenticated Close frames of the last source round.
    pub(crate) fn closed_frames(
        &self,
    ) -> &BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteOldFrame> {
        &self.closed
    }
    /// The answers of the last read-only observation round.
    pub(crate) fn cut_observations(&self) -> &[(ClusterNodeId, RemoteOldCutObservation)] {
        &self.observed
    }

    /// The pending plan of the committed relocation, from a fresh range
    /// metadata observation this coordinator acquires itself. It is purpose
    /// and scope data: no member accepts it in place of its own role and
    /// metadata observations.
    fn plan(
        &self,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJointHandoffPlan>> {
        let token = bounded(parent, REQUEST);
        let business = Uuid::now_v7();
        let observation = self.transport.acquire_metadata_read_observation(
            &members.metadata_leader,
            business,
            range.id,
            range.epoch,
            REQUEST,
            &token,
        )?;
        let round = NativeJointOldRound::begin(observation, business, relocation.id)?;
        let plan = round.plan();
        // The round is dropped unused: it never records a service observation
        // and never finishes a local quorum.
        drop(round);
        let planned = plan.old_members().keys().cloned().collect::<BTreeSet<_>>();
        let resolved = members.old_voters.iter().cloned().collect::<BTreeSet<_>>();
        if planned != resolved
            || plan.retiring_source() != &members.source
            || !plan.new_members().contains_key(&self.target)
        {
            return Err(remote_error(
                "the pending plan differs from the resolved members of the committed relocation",
            ));
        }
        Ok(plan)
    }

    /// Send the source Close to every old member, as this coordinator. Each
    /// member witnesses the coordinator's role itself and closes through its
    /// own receiver. A repeated call confirms the same durable promise and
    /// native close record; it never replaces them.
    pub(crate) fn close_old_members(
        &mut self,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        // Before the first member mutation, and before anything can fail.
        self.cut_started = true;
        self.closed.clear();
        self.closed_scope_sha256 = None;
        let plan = self
            .plan(members, range, relocation, parent)
            .map_err(|error| stage_error("source round metadata", error))?;
        self.closed_scope_sha256 = Some(plan.checksum_sha256()?);
        for recipient in plan.old_members().keys() {
            let operation = bounded(parent, REQUEST);
            let business = Uuid::now_v7();
            let own = self
                .transport
                .acquire_host_metadata_observation(
                    &members.metadata_leader,
                    business,
                    REQUEST,
                    &operation,
                )
                .map_err(|error| stage_error("source close metadata", error))?;
            let request = NativeRemoteOldRequestV1::from_plan(
                &plan,
                NativeRemoteOldOperationV1::Close,
                business,
                OLD_REQUEST_MS,
            )?;
            let frame = self
                .transport
                .request_remote_native_old(recipient, &plan, &request, &own, &operation)
                .map_err(|error| stage_error("source close", error))?;
            if frame.reply().closed.is_none() {
                return Err(handoff_error(format!(
                    "source close: old member {recipient} returned no closure"
                )));
            }
            self.closed.insert(recipient.clone(), frame);
        }
        Ok(())
    }

    /// Ask every old member, read-only, whether its process retains an
    /// operation of this relocation's plan scope. Nothing is mutated and no
    /// answer is turned into an owner.
    ///
    /// Limits of the existing read-only observation, none of which is worked
    /// around here: it admits old members only (the sender and the receiver
    /// both refuse another observer), it answers from the receiver process's
    /// retained table (a restarted member answers "absent" although its
    /// durable promise remains), and "absent" is a refusal text.
    pub(crate) fn observe_cut(
        &mut self,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.observed.clear();
        let coordinator = self.coordinator();
        let every = |reason: String| {
            members
                .old_voters
                .iter()
                .map(|node| {
                    (
                        node.clone(),
                        RemoteOldCutObservation::Inconclusive(reason.clone()),
                    )
                })
                .collect::<Vec<_>>()
        };
        let plan = match self.plan(members, range, relocation, parent) {
            Ok(plan) => plan,
            Err(error) => {
                parent.check()?;
                self.observed = every(format!("pending plan unavailable: {error}"));
                return Ok(());
            }
        };
        if !plan.old_members().contains_key(&coordinator) {
            self.observed = every(format!(
                "coordinator {coordinator} is not an old member of the range; the read-only OLD observation admits old members only"
            ));
            return Ok(());
        }
        for recipient in plan.old_members().keys() {
            let answer = self.observe_old_member(members, &plan, recipient, parent);
            parent.check()?;
            let observation = match answer {
                Ok(frame) => RemoteOldCutObservation::Retained {
                    closed: frame.reply().closed.is_some(),
                },
                Err(error) => {
                    let text = error.to_string();
                    if text.contains(OBSERVE_ABSENT) {
                        RemoteOldCutObservation::Absent
                    } else if OBSERVE_IN_FLIGHT.iter().any(|known| text.contains(known)) {
                        RemoteOldCutObservation::InFlight
                    } else {
                        RemoteOldCutObservation::Inconclusive(text)
                    }
                }
            };
            if observation.started() {
                self.cut_started = true;
            }
            self.observed.push((recipient.clone(), observation));
        }
        Ok(())
    }
    fn observe_old_member(
        &self,
        members: &RemoteMembers,
        plan: &NativeJointHandoffPlan,
        recipient: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteOldFrame> {
        let operation = bounded(parent, REQUEST);
        let business = Uuid::now_v7();
        let own = self.transport.acquire_host_metadata_observation(
            &members.metadata_leader,
            business,
            REQUEST,
            &operation,
        )?;
        let request = NativeRemoteOldRequestV1::from_plan(
            plan,
            NativeRemoteOldOperationV1::Observe,
            business,
            OLD_REQUEST_MS,
        )?;
        self.transport
            .request_remote_native_old(recipient, plan, &request, &own, &operation)
    }
}

impl JoinedHandoffBackend for RemoteJoinedHandoffBackend {
    type Members = RemoteMembers;
    type Hosts = dyn JoinedHandoffMetadataHosts;

    fn cut_started(&self) -> bool {
        self.cut_started
    }

    /// The durable range-log epoch the old voter reports over the existing
    /// range-write progress request. The voter answers its current range
    /// leader only, and only for the range epoch it has published; every
    /// refusal is an error here.
    fn old_epoch(
        &self,
        _hosts: &Self::Hosts,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
    ) -> Result<u64> {
        let progress = self
            .transport
            .range_write_progress(node, range.id, range.epoch)
            .map_err(|error| {
                remote_error(format!("old voter {node} epoch is unreadable: {error}"))
            })?;
        if &progress.node_id != node || progress.range_id != range.id {
            return Err(remote_error(format!(
                "old voter {node} answered the epoch read for another node or range"
            )));
        }
        Ok(progress.current_epoch)
    }

    fn forget_aborted_allocation(&mut self) {
        self.closed.clear();
        self.closed_scope_sha256 = None;
        self.observed.clear();
    }

    fn admit_target(
        &mut self,
        _hosts: &mut Self::Hosts,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Err(not_implemented("target admission"))
    }

    fn members(
        &self,
        hosts: &Self::Hosts,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
    ) -> Result<RemoteMembers> {
        let metadata_leader = hosts.metadata_leader()?;
        if relocation.target != self.target || relocation.range_id != range.id {
            return Err(remote_error(
                "the committed relocation names another target or range",
            ));
        }
        if relocation.native_joint_handoff_v1.is_some() {
            // The range descriptor no longer lists the old voters once the
            // membership stages begin.
            return Err(not_implemented(
                "member resolution after the certified decision",
            ));
        }
        let source = relocation
            .source
            .clone()
            .ok_or_else(|| remote_error("the committed relocation names no source"))?;
        let old_voters = range
            .replicas
            .iter()
            .filter(|replica| {
                replica.role == RangeReplicaRole::Voter && replica.node_id != self.target
            })
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        if !old_voters.contains(&source) {
            return Err(remote_error(
                "the retiring source is not a voter of the committed range",
            ));
        }
        let mut new_members = old_voters
            .iter()
            .filter(|node| **node != source)
            .cloned()
            .collect::<Vec<_>>();
        new_members.push(self.target.clone());
        Ok(RemoteMembers {
            metadata_leader,
            old_voters,
            new_members,
            target: self.target.clone(),
            source,
        })
    }

    /// Re-derive "a cut was started" for a coordinator without memory. A
    /// member that reports a retained operation sets it. An unusable answer
    /// does not stop the drive: the source round sets the flag itself before
    /// its first request. `resync_before_abort` is the strict form.
    fn resync(
        &mut self,
        _hosts: &mut Self::Hosts,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        if self.cut_started {
            return Ok(());
        }
        self.observe_cut(members, range, relocation, parent)
    }

    /// Before an abort is decided: every old member must answer the
    /// read-only observation. A member without a usable answer refuses the
    /// abort; so does a coordinator that may not observe.
    fn resync_before_abort(
        &mut self,
        hosts: &mut Self::Hosts,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        if self.cut_started {
            return Ok(());
        }
        let members = self.members(&*hosts, range, relocation)?;
        self.observe_cut(&members, range, relocation, parent)?;
        if self.cut_started {
            return Ok(());
        }
        if let Some((node, RemoteOldCutObservation::Inconclusive(reason))) = self
            .observed
            .iter()
            .find(|(_, answer)| matches!(answer, RemoteOldCutObservation::Inconclusive(_)))
        {
            return Err(remote_error(format!(
                "cannot establish that no source round was started: old member {node}: {reason}"
            )));
        }
        Ok(())
    }

    fn cut_required(&self) -> bool {
        // No finalized image, publication round or decision is ever held yet.
        true
    }

    /// First half only: the Close round. The donor's own collection of the
    /// closed set and the image transfer are not implemented.
    fn cut_and_ship(
        &mut self,
        _hosts: &mut Self::Hosts,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.close_old_members(members, range, relocation, parent)?;
        Err(not_implemented("source collection and image transfer"))
    }

    fn decision_durable(&self) -> bool {
        false
    }

    fn decide(&mut self, _members: &RemoteMembers, _parent: &CancellationToken) -> Result<()> {
        Err(not_implemented("joint decision publication"))
    }

    fn require_decision(&self) -> Result<()> {
        Err(not_implemented("durable decision custody"))
    }

    fn stage_candidate(
        &mut self,
        _committed: Option<NativeJointHandoffStageV1>,
        _view: &mut DistributionStore,
        _fresh: &HostMetadataReadObservation,
        _business: Uuid,
        _actor: &ClusterNodeId,
        _now: u64,
    ) -> Result<()> {
        Err(not_implemented("membership stage candidate"))
    }

    fn activate(
        &mut self,
        _hosts: &mut Self::Hosts,
        _members: &RemoteMembers,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Err(not_implemented("activation and release"))
    }

    fn serve(&mut self, _hosts: &mut Self::Hosts, _parent: &CancellationToken) -> Result<usize> {
        Err(not_implemented("target serving"))
    }
}
