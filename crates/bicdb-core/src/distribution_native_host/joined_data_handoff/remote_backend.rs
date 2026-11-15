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
//! - a source Close, the target fence and a target step are admitted from the
//!   range leader or the metadata leader, and the target's fence binds every
//!   later target step to the node that issued it;
//! - the read-only OLD observation is admitted from an old member only;
//! - the old voter epoch read is answered to the range leader only, and only
//!   while the voter's installed writer policy is the asked descriptor;
//! - metadata candidates are proposed in the metadata leader's process only.
//! One node satisfies all of them: the node that is both the metadata leader
//! and the leader of the moved range. `resync` and `cut_and_ship` refuse any
//! other coordinator before a request is sent. The promotion commit makes
//! the target the range leader; from then on only the metadata leader half
//! can be required.
//!
//! Identities that survive a coordinator restart. The target install attempt
//! and the decision phase are derived from the committed plan scope (and the
//! phase also from the source manifest), never drawn at random: a restarted
//! coordinator computes the same identities again and so cannot start a
//! second install attempt or a second decision phase beside one in flight.
//!
//! Implemented: target admission (committed metadata only), the source cut
//! with the target fence, the Close round, the target install and the export
//! retirement, the joint decision and the three membership candidates. The
//! bridge phases (activation, serving, activation record, source retirement)
//! answer a "not implemented" error.
use super::*;
use crate::distribution_consensus::{MetadataRecoveryRolePurpose, MetadataRecoveryRoleRequest};
use crate::distribution_range_consensus::{RangeWriteTransport, RecoveryFenceScope};
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeJointHandoffStepOperationV1, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepRequestV1, NativeJointOldRound, NativeRemoteJointDecisionCertificate,
    NativeRemoteOldOperationV1, NativeRemoteOldRequestV1,
};
use crate::distribution_range_image::NativeRemoteImageManifestV1;
use crate::distribution_range_recovery::RecoveryEpochFenceRequest;
use crate::distribution_transport::{
    AuthenticatedNativeJointHandoffStep, AuthenticatedNativeRemoteOldFrame,
};
use std::collections::BTreeMap;

/// Receiver-side budget of one target step or fence; receivers bound it to 5 s.
const TARGET_REQUEST_MS: u64 = 4000;
/// Bound of the step requests of one target install. A step performs at
/// least one bounded operation, so an install that needs more is not
/// progressing.
const INSTALL_STEP_CALLS: usize = 512;
/// A step that fails this many times in a row ends the call. The same
/// attempt continues on the next call.
const INSTALL_STEP_FAILURES: usize = 4;
/// Budget of one complete decision round (four requests per member).
const DECISION_ROUND: Duration = Duration::from_secs(60);

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
/// An identity derived from committed scope DATA, so that a coordinator
/// without memory computes the same one again.
fn derived_id(label: &str, parts: &[&str]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(label.as_bytes());
    for part in parts {
        hasher.update([0u8]);
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // Version 8 (custom), RFC variant: never nil and never a time-ordered id.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
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
    /// This object issued the target's handoff fence.
    target_fenced: bool,
    /// The pending plan the retained install and decision belong to. Scope
    /// DATA of this coordinator's own metadata observation.
    plan: Option<Arc<NativeJointHandoffPlan>>,
    /// The old member the target installed its image from.
    donor: Option<ClusterNodeId>,
    /// What the target answered for its completed install. Historical DATA.
    install: Option<NativeJointHandoffStepOutcomeV1>,
    /// The target's original source manifest, as the target returned it
    /// after the donor retired the export. Request DATA of the decision
    /// round; every member compares it with its own facts.
    manifest: Option<NativeRemoteImageManifestV1>,
    /// The decision certificate, built by the transport from authenticated
    /// durable observations only.
    certificate: Option<NativeRemoteJointDecisionCertificate>,
    /// Why no certificate can be held for the committed stage, when a
    /// coordinator without memory found the handoff past re-collection.
    certificate_unrecoverable: Option<String>,
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
            target_fenced: false,
            plan: None,
            donor: None,
            install: None,
            manifest: None,
            certificate: None,
            certificate_unrecoverable: None,
        }
    }
    /// The pending plan of the retained install and decision.
    pub(crate) fn retained_plan(&self) -> Option<&Arc<NativeJointHandoffPlan>> {
        self.plan.as_ref()
    }
    /// The old member the target installed its image from.
    pub(crate) fn donor(&self) -> Option<&ClusterNodeId> {
        self.donor.as_ref()
    }
    /// The target's completed install outcome (historical DATA).
    pub(crate) fn install_outcome(&self) -> Option<&NativeJointHandoffStepOutcomeV1> {
        self.install.as_ref()
    }
    /// The target's original source manifest.
    pub(crate) fn source_manifest(&self) -> Option<&NativeRemoteImageManifestV1> {
        self.manifest.as_ref()
    }
    /// The decision certificate.
    pub(crate) fn decision_certificate(&self) -> Option<&NativeRemoteJointDecisionCertificate> {
        self.certificate.as_ref()
    }
    /// The one install attempt of a plan scope.
    pub(crate) fn target_attempt(plan: &NativeJointHandoffPlan) -> Result<Uuid> {
        Ok(derived_id(
            "native-joint-remote-target-attempt-v1",
            &[&plan.checksum_sha256()?],
        ))
    }
    /// The one decision phase of a plan scope and source manifest.
    pub(crate) fn decision_phase(
        plan: &NativeJointHandoffPlan,
        manifest: &NativeRemoteImageManifestV1,
    ) -> Result<Uuid> {
        Ok(derived_id(
            "native-joint-remote-decision-phase-v1",
            &[&plan.checksum_sha256()?, &manifest.checksum_sha256],
        ))
    }
    /// The image donor: the first old member, in node order, that is neither
    /// the leader of the range nor the retiring source. The handoff retires
    /// the range leader by default, so both exclusions usually name the same
    /// node, which is also this coordinator; the donor is then a surviving
    /// follower. The rule depends on the plan alone, so every coordinator of
    /// this plan selects the same donor.
    pub(crate) fn select_donor(plan: &NativeJointHandoffPlan) -> Result<ClusterNodeId> {
        plan.old_members()
            .keys()
            .find(|node| **node != plan.old_range().leader && *node != plan.retiring_source())
            .cloned()
            .ok_or_else(|| {
                remote_error(
                    "no old member besides the range leader and the retiring source can donate the image",
                )
            })
    }

    /// The node that coordinates must be the metadata leader and, until the
    /// promotion commit hands the range to the target, the leader of the
    /// moved range. Checked on committed DATA before any request is sent.
    fn require_coordinator(
        &self,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        recorded: Option<NativeJointHandoffStageV1>,
    ) -> Result<()> {
        let coordinator = self.coordinator();
        if coordinator != members.metadata_leader {
            return Err(remote_error(format!(
                "coordinator {coordinator} is not the metadata leader {}; a handoff is coordinated by the node that is both the metadata leader and the leader of the moved range",
                members.metadata_leader
            )));
        }
        if recorded < Some(NativeJointHandoffStageV1::PromotedFrozen) && coordinator != range.leader
        {
            return Err(remote_error(format!(
                "coordinator {coordinator} is the metadata leader but not the leader {} of range {}; make the two coincide before moving the range",
                range.leader, range.id
            )));
        }
        Ok(())
    }

    /// Fence the target for this handoff session, as this coordinator. The
    /// fence scope comes from this coordinator's own fresh handoff role,
    /// witnessed by the metadata leader for this node; the target witnesses
    /// the same initiator again itself before it fences anything. The target
    /// binds its closed state to the initiator, so the node that fences must
    /// be the node that sends the install steps.
    fn fence_target(
        &self,
        members: &RemoteMembers,
        plan: &NativeJointHandoffPlan,
        parent: &CancellationToken,
    ) -> Result<()> {
        let actor = self.transport.local_metadata_read_participant();
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
        let request = MetadataRecoveryRoleRequest {
            protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            acquisition_id: Uuid::now_v7(),
            read_request_id: business,
            initiator_node_id: actor.node_id().clone(),
            initiator_incarnation: actor.member_incarnation(),
            initiator_certificate_sha256: actor.certificate_sha256().map(str::to_string),
            initiator_plaintext_loopback: actor.plaintext_loopback(),
            recipient_node_id: actor.node_id().clone(),
            recipient_incarnation: actor.member_incarnation(),
            range_id: plan.old_range().id,
            range_epoch: plan.old_range().epoch,
            recovery_nonce: promise.nonce,
            recipient_challenge: challenge,
            purpose: purpose.clone(),
            budget_ms: TARGET_REQUEST_MS,
        };
        let witness = self.transport.acquire_metadata_recovery_role_observation(
            &members.metadata_leader,
            request,
            &token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            &actor,
            actor.node_id(),
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            &purpose,
        )?;
        let scope = RecoveryFenceScope::from_role(&role)?;
        self.transport.recovery_epoch_fence_observation(
            &self.target,
            &RecoveryEpochFenceRequest {
                scope,
                budget_ms: TARGET_REQUEST_MS,
            },
            &token,
        )?;
        Ok(())
    }

    /// One bounded step request to the target, as this coordinator. The
    /// reply is progress DATA; the target proved its own role and metadata.
    fn target_step(
        &self,
        members: &RemoteMembers,
        plan: &NativeJointHandoffPlan,
        operation: NativeJointHandoffStepOperationV1,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointHandoffStep> {
        let token = bounded(parent, REQUEST);
        let business = Uuid::now_v7();
        let own = self.transport.acquire_host_metadata_observation(
            &members.metadata_leader,
            business,
            REQUEST,
            &token,
        )?;
        let request = NativeJointHandoffStepRequestV1::from_plan(
            plan,
            operation,
            business,
            TARGET_REQUEST_MS,
        )?;
        self.transport
            .request_native_joint_handoff_step(&self.target, plan, &request, &own, &token)
    }

    /// Drive the target's install of the donor's image to completion. Every
    /// request names the same attempt: a request that failed or whose reply
    /// was lost has an unknown effect, and the target continues exactly that
    /// attempt on the next request.
    fn install_on_target(
        &self,
        members: &RemoteMembers,
        plan: &NativeJointHandoffPlan,
        donor: &ClusterNodeId,
        target_attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeJointHandoffStepOutcomeV1> {
        let mut failures = 0;
        for _ in 0..INSTALL_STEP_CALLS {
            parent.check()?;
            let operation = NativeJointHandoffStepOperationV1::TargetInstall {
                donor: donor.clone(),
                target_attempt,
            };
            match self.target_step(members, plan, operation, parent) {
                Ok(frame) => {
                    failures = 0;
                    if frame.outcome().complete {
                        return Ok(frame.outcome().clone());
                    }
                }
                Err(error) => {
                    parent.check()?;
                    failures += 1;
                    if failures >= INSTALL_STEP_FAILURES {
                        return Err(error);
                    }
                }
            }
        }
        Err(remote_error(
            "the target install did not complete within its bounded number of steps",
        ))
    }

    /// Ask the target to retire the donor's completed export and to return
    /// its original source manifest. The target refuses unless it retains
    /// the completed install of exactly this donor and attempt, so this
    /// request never starts an install. Repeatable.
    fn retire_source_export(
        &self,
        members: &RemoteMembers,
        plan: &NativeJointHandoffPlan,
        donor: &ClusterNodeId,
        target_attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<(NativeJointHandoffStepOutcomeV1, NativeRemoteImageManifestV1)> {
        let frame = retry_timed_out(|| {
            self.target_step(
                members,
                plan,
                NativeJointHandoffStepOperationV1::SourceExportRetire {
                    donor: donor.clone(),
                    target_attempt,
                },
                parent,
            )
        })?;
        let manifest = frame.reply().source_manifest.clone().ok_or_else(|| {
            remote_error("the target returned no source manifest for its completed install")
        })?;
        Ok((frame.outcome().clone(), manifest))
    }

    /// Rebuild the retained decision for a coordinator without memory, at
    /// the committed stage `Certified`. Read-only towards every member: the
    /// target is asked only for the manifest of the install it already
    /// completed (it refuses when it retains none), the donor answers its
    /// retained export terminal, and the certificate is collected from
    /// observations of the members' already durable decision.
    fn recover_certified_decision(
        &mut self,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        let plan = self
            .plan(members, range, relocation, parent)
            .map_err(|error| stage_error("decision recovery metadata", error))?;
        let donor = Self::select_donor(&plan)?;
        let target_attempt = Self::target_attempt(&plan)?;
        let (outcome, manifest) = self
            .retire_source_export(members, &plan, &donor, target_attempt, parent)
            .map_err(|error| stage_error("decision recovery source manifest", error))?;
        let phase = Self::decision_phase(&plan, &manifest)?;
        let mut publishers = members.old_voters.clone();
        publishers.push(self.target.clone());
        let certificate = self
            .transport
            .recollect_remote_joint_decision_certificate(
                plan.clone(),
                &manifest,
                phase,
                &publishers,
                &members.metadata_leader,
                &bounded(parent, DECISION_ROUND),
            )
            .map_err(|error| stage_error("decision recovery certificate", error))?;
        self.plan = Some(plan);
        self.donor = Some(donor);
        self.install = Some(outcome);
        self.manifest = Some(manifest);
        self.certificate = Some(certificate);
        Ok(())
    }
    fn recorded(relocation: &RangeRelocation) -> Option<NativeJointHandoffStageV1> {
        relocation
            .native_joint_handoff_v1
            .as_ref()
            .map(|record| record.stage())
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
        self.plan = None;
    }

    /// Verify, on committed metadata only, that the target can be admitted;
    /// nothing is created and no member is asked. The target is admitted as
    /// a registered replica receiver by its own operator action, and no
    /// product request reads that receiver's state from another node. Its
    /// absence is therefore reported by the first request that needs it: the
    /// target fence and every target step are served by that receiver alone
    /// and are refused, changing nothing, when it is not open.
    ///
    /// This is also where a coordinator that is not the metadata leader is
    /// refused, before the learner allocation is proposed. The range leader
    /// half of the rule needs the committed relocation and is checked by
    /// `resync` and `cut_and_ship`.
    fn admit_target(&mut self, hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        let coordinator = self.coordinator();
        let metadata_leader = hosts.metadata_leader()?;
        if coordinator != metadata_leader {
            return Err(remote_error(format!(
                "coordinator {coordinator} is not the metadata leader {metadata_leader}; a handoff is coordinated by the node that is both the metadata leader and the leader of the moved range"
            )));
        }
        let topology = hosts.committed_topology()?;
        let member = topology.nodes.get(&self.target).ok_or_else(|| {
            remote_error(format!(
                "target {} is not a member of the committed configuration",
                self.target
            ))
        })?;
        if member.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
            || member.native_store_birth_v1.is_none()
        {
            return Err(remote_error(format!(
                "target {} is not an active enrolled member with a native store birth",
                self.target
            )));
        }
        Ok(())
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
        let source = relocation
            .source
            .clone()
            .ok_or_else(|| remote_error("the committed relocation names no source"))?;
        // The target is a learner until the promotion and a voter after it;
        // every other voter is an old voter. The final membership no longer
        // lists the retiring source, which the relocation still names.
        let mut old_voters = range
            .replicas
            .iter()
            .filter(|replica| {
                replica.role == RangeReplicaRole::Voter && replica.node_id != self.target
            })
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        if !old_voters.contains(&source) {
            if Self::recorded(relocation) < Some(NativeJointHandoffStageV1::AwaitingActivation) {
                return Err(remote_error(
                    "the retiring source is not a voter of the committed range",
                ));
            }
            old_voters.push(source.clone());
            old_voters.sort();
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

    /// Re-derive the retained state of a coordinator without memory, from
    /// the committed stage and the members' answers.
    ///
    /// Before the certified decision: a member that reports a retained
    /// operation sets "a cut was started". An unusable answer does not stop
    /// the drive: the source round sets the flag itself before its first
    /// request, and it repeats the fence, the Close round, the install (the
    /// same attempt) and the decision (the same phase), each of which
    /// confirms what is already durable. `resync_before_abort` is the strict
    /// form.
    ///
    /// At `Certified`: the certificate is collected again, read-only.
    ///
    /// From `PromotedFrozen` on the certificate cannot be collected again:
    /// its scope is the pending plan, which the metadata leader grants only
    /// to the leader of the range at its pre-promotion epoch, and the
    /// promotion made the target the leader at the next epoch. Nothing is
    /// guessed: `require_decision` refuses the final membership stage of
    /// such a coordinator. A handoff whose final membership is committed
    /// needs no certificate any more.
    fn resync(
        &mut self,
        _hosts: &mut Self::Hosts,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        let recorded = Self::recorded(relocation);
        self.require_coordinator(members, range, recorded)?;
        match recorded {
            None => {
                if !self.cut_started {
                    self.observe_cut(members, range, relocation, parent)?;
                }
            }
            Some(stage) => {
                // A certified decision exists only after a source cut.
                self.cut_started = true;
                if self.certificate.is_some() {
                    return Ok(());
                }
                match stage {
                    NativeJointHandoffStageV1::Certified => {
                        self.recover_certified_decision(members, range, relocation, parent)?;
                    }
                    NativeJointHandoffStageV1::PromotedFrozen => {
                        self.certificate_unrecoverable = Some(
                            "the committed handoff is at the frozen union membership and this coordinator does not retain its decision certificate; the certificate is scoped to the pending plan, which only the pre-promotion range leader can read, so it cannot be collected again and the final membership cannot be proposed by this coordinator"
                                .into(),
                        );
                    }
                    _ => {}
                }
            }
        }
        Ok(())
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
        // The manifest is retained once the image is installed and the
        // donor's export is retired; the certificate implies both.
        self.manifest.is_none() && self.certificate.is_none()
    }

    /// The source cut and the image, in this order:
    /// 1. the target fence, issued by this coordinator (the target binds its
    ///    install to the fence initiator);
    /// 2. the Close round on every old member;
    /// 3. the target install, driven by step requests: the target itself
    ///    asks the donor, whose own receiver collects the all-closed
    ///    certificate from every old member before it serves a frame;
    /// 4. the export retirement, which also returns the source manifest.
    /// No wait separates 2 from 3: the donor reads every old member's
    /// retained closure itself, read-only, and a member whose Close has not
    /// finished refuses that read, which fails the step; the step is then
    /// repeated with the same attempt.
    ///
    /// A repeated call repeats every part. Each confirms what is durable:
    /// the same promise and native close record, the same install attempt,
    /// the donor's retained export terminal.
    fn cut_and_ship(
        &mut self,
        _hosts: &mut Self::Hosts,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.require_coordinator(members, range, Self::recorded(relocation))?;
        // Before the first member mutation, and before anything can fail.
        self.cut_started = true;
        let plan = self
            .plan(members, range, relocation, parent)
            .map_err(|error| stage_error("source round metadata", error))?;
        let donor = Self::select_donor(&plan)?;
        let target_attempt = Self::target_attempt(&plan)?;
        if !self.target_fenced {
            self.fence_target(members, &plan, parent)
                .map_err(|error| stage_error("target fence", error))?;
            self.target_fenced = true;
        }
        self.close_old_members(members, range, relocation, parent)?;
        self.plan = Some(plan.clone());
        self.donor = Some(donor.clone());
        let installed = self
            .install_on_target(members, &plan, &donor, target_attempt, parent)
            .map_err(|error| stage_error("target install", error))?;
        let (outcome, manifest) = self
            .retire_source_export(members, &plan, &donor, target_attempt, parent)
            .map_err(|error| stage_error("source export retirement", error))?;
        if outcome.manifest_sha256 != installed.manifest_sha256
            || outcome.source_session != installed.source_session
            || outcome.range_index != installed.range_index
            || outcome.native_receipt_sha256 != installed.native_receipt_sha256
        {
            return Err(remote_error(
                "the target retired the export of another install than the one it completed",
            ));
        }
        self.install = Some(installed);
        self.manifest = Some(manifest);
        Ok(())
    }

    fn decision_durable(&self) -> bool {
        self.certificate.is_some()
    }

    /// Obtain the decision certificate. A decision that is already durable
    /// (a coordinator that lost its memory after the publication) is found
    /// by the read-only collection, which publishes nothing and creates no
    /// participant; otherwise the complete round runs: materialize, prepare,
    /// publish, then the same read-only collection.
    fn decide(&mut self, members: &RemoteMembers, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        let (Some(plan), Some(manifest)) = (self.plan.clone(), self.manifest.clone()) else {
            return Err(remote_error(
                "the joint decision needs the retained plan and source manifest of the installed image",
            ));
        };
        let phase = Self::decision_phase(&plan, &manifest)?;
        let mut publishers = members.old_voters.clone();
        publishers.push(self.target.clone());
        let token = bounded(parent, DECISION_ROUND);
        let certificate = match self.transport.recollect_remote_joint_decision_certificate(
            plan.clone(),
            &manifest,
            phase,
            &publishers,
            &members.metadata_leader,
            &token,
        ) {
            Ok(certificate) => certificate,
            Err(_) => {
                parent.check()?;
                self.transport
                    .run_remote_joint_decision_round(
                        plan,
                        &manifest,
                        phase,
                        &publishers,
                        &members.metadata_leader,
                        &token,
                    )
                    .map_err(|error| stage_error("joint decision", error))?
            }
        };
        self.certificate = Some(certificate);
        Ok(())
    }

    fn require_decision(&self) -> Result<()> {
        if self.certificate.is_some() {
            return Ok(());
        }
        Err(remote_error(match &self.certificate_unrecoverable {
            Some(reason) => reason.clone(),
            None => {
                "decision certificate absent; the membership stages need the certified decision"
                    .to_string()
            }
        }))
    }

    /// The three membership candidates, built from the retained certificate
    /// by the same stage functions the in-process backend uses. The two
    /// completion records belong to the bridge phases.
    fn stage_candidate(
        &mut self,
        committed: Option<NativeJointHandoffStageV1>,
        view: &mut DistributionStore,
        fresh: &HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now: u64,
    ) -> Result<()> {
        match committed {
            Some(NativeJointHandoffStageV1::AwaitingActivation) => {
                return Err(not_implemented("activation record"))
            }
            Some(NativeJointHandoffStageV1::AwaitingCleanup) => {
                return Err(not_implemented("source retirement record"))
            }
            _ => {}
        }
        self.require_decision()?;
        let Some(certificate) = self.certificate.as_ref() else {
            return Err(remote_error("decision certificate absent"));
        };
        match committed {
            None => {
                view.certify_native_joint_handoff(certificate, fresh, business, actor, now)
                    .map_err(|error| stage_error("decision certification", error))?;
            }
            Some(NativeJointHandoffStageV1::Certified) => {
                view.promote_native_joint_handoff_frozen(certificate, fresh, business, actor, now)
                    .map_err(|error| stage_error("union membership", error))?;
            }
            Some(NativeJointHandoffStageV1::PromotedFrozen) => {
                view.finalize_native_joint_membership_frozen(
                    certificate,
                    fresh,
                    business,
                    actor,
                    now,
                )
                .map_err(|error| stage_error("final membership", error))?;
            }
            Some(_) => return Err(remote_error("relocation is already completed")),
        }
        Ok(())
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
