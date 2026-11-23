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
//! - the read-only range move probe (the target's readiness before the
//!   learner allocation, an old voter's durable epoch before an abort) is
//!   answered to the leader of the range in the answering member's own
//!   committed configuration only;
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
//! A decision that metadata consensus certified is committed DATA. The
//! certified record carries the decision's scope, so a coordinator without
//! memory builds the union and the final membership from the committed
//! record alone and addresses the new members with the committed decision
//! identity. Only the certification itself needs the actual certificate.
//!
//! Implemented: target admission (committed metadata, then the read-only
//! pre-flight of the target and every voter), the source cut
//! with the target fence, the Close round, the target install and the export
//! retirement, the joint decision, the three membership candidates, the
//! activation (bridge preparation, bridge publication, catalogue activation,
//! OLD custody retirement, release and writer policy of every new member,
//! all by step requests), the target's serving, the activation record and
//! the source retirement.
//!
//! The target's host commands. The target's writer policy and its serving
//! replace the target's DATA owner and rebind its listener, so the target's
//! step receiver only records the command and the target's own host loop
//! performs it. Until that loop retained an outcome the target answers the
//! repeated step with "host command pending". The coordinator repeats the
//! step for a bounded time; when the time ends while the command is still
//! pending, the call ends with an error and `waiting()` names what is
//! awaited. The next call repeats the same step, and a completed command
//! answers with its retained outcome.
//!
//! What is not sent to a serving target. Once the target holds its writer
//! policy its host is a DATA owner and refuses every bridge step except the
//! phase observations and its two host commands. The activation therefore
//! decides by the read-only release observations what it sends: a target
//! that is observed released is sent neither a preparation, publication,
//! Open, custody retirement nor a release again.
//!
//! The leader of the final membership must be the target. A surviving member
//! that led the final membership would have to recover its leadership
//! itself, and no member step exists for that. The handoff retires the range
//! leader, which makes the target the leader; any other plan is refused at
//! the source stage, before the target is fenced or any old member is
//! closed, and again by `activate` before any bridge step.
//!
//! The coordinator's own data service. A handoff retires the coordinator's
//! own replica (the pre-handoff range leader), and a member never sends a
//! step request to itself. The backend is therefore optionally given this
//! node's own data service, for exactly two things this node does itself:
//! reading its own protected native decision record (the decision body of a
//! coordinator without memory) and recording its own source retirement. It
//! is never another member's service.
use super::*;
use crate::distribution_consensus::{MetadataRecoveryRolePurpose, MetadataRecoveryRoleRequest};
use crate::distribution_data::{
    NativeJointSourceRetired, NativeJointSourceRetirementProof,
    NativeRemoteJointSourceRetirementCertificate,
};
use crate::distribution_range_consensus::RecoveryFenceScope;
use crate::distribution_range_handoff::{
    NativeJointBridgeDecision, NativeJointBridgePhaseKindV1, NativeJointCommittedDecision,
    NativeJointDecisionData, NativeJointHandoffPlan, NativeJointHandoffStepOperationV1,
    NativeJointHandoffStepOutcomeV1, NativeJointHandoffStepRequestV1, NativeJointOldRound,
    NativeRemoteBridgePublicationCertificate, NativeRemoteBridgePublicationCertificateRound,
    NativeRemoteJointActivationCertificate, NativeRemoteJointDecisionCertificate,
    NativeRemoteNewFactSetV1, NativeRemoteOldOperationV1, NativeRemoteOldRequestV1,
};
use crate::distribution_range_image::NativeRemoteImageManifestV1;
use crate::distribution_range_recovery::RecoveryEpochFenceRequest;
use crate::distribution_transport::{
    AuthenticatedNativeJointBridgeStepFrame, AuthenticatedNativeJointHandoffStep,
    AuthenticatedNativeRemoteOldFrame, NativeRangeMoveProbeRoleV1,
};
use std::collections::BTreeMap;

mod complete_decision;

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
/// Receiver-side budget of one bridge step: the 5 s every receiver bounds it
/// to. A member performs several bounded parts inside one step, and a
/// publication, Open or release window must hold from the member's first
/// observation of another member until its own durable write: on a slow
/// host every part of that budget is used.
const BRIDGE_REQUEST_MS: u64 = 5000;
/// Budget of one bridge phase (preparation, publication or Open) over every
/// new member.
const BRIDGE_PHASE: Duration = Duration::from_secs(90);
/// A bridge step that is refused this many times in a row for another reason
/// than a busy member ends the call. The member continues the same step on
/// the next call.
const BRIDGE_STEP_FAILURES: usize = 4;
/// A member's step is repeated until the member reports it complete or its
/// phase budget (`BRIDGE_PHASE`) ends; there is no bound on the number of
/// requests. The first answers of a step that performs several bounded parts
/// are repeated at once; after `BRIDGE_REPEAT_FREE` repetitions the pause
/// between two requests doubles from the first to the second value.
const BRIDGE_REPEAT_FREE: u32 = 2;
const BRIDGE_REPEAT_PAUSE: (Duration, Duration) =
    (Duration::from_millis(25), Duration::from_millis(400));

/// How long one call waits for the target's host loop to finish a host
/// command before it reports the wait.
const HOST_COMMAND_WAIT: Duration = Duration::from_secs(20);
/// Pause between two repetitions of a pending host command step.
const HOST_COMMAND_POLL: Duration = Duration::from_millis(100);
/// A host command step that fails this many times in a row ends the call.
/// The target rebinds its listener while it runs a command, so a few
/// requests in a row may find no listener.
const HOST_COMMAND_FAILURES: usize = 20;
/// How long one call repeats the source Close of one old member while that
/// member answers that its own state is busy.
const CLOSE_BUSY_WAIT: Duration = Duration::from_secs(5);
/// How long one call repeats the activation observation while a member
/// answers that its own state is busy. The pause between two observations
/// doubles as for a bridge step.
const ACTIVATION_BUSY_WAIT: Duration = Duration::from_secs(30);
/// The target's refusal of a host command while its release is durable but
/// not confirmed by its second window. Such a target is not a DATA owner
/// yet: its release step is still served and confirms it.
const RELEASE_UNCONFIRMED: &str = "joint bridge member holds no confirmed release";

/// What the target answered to a repeated host command step.
enum HostCommandAnswer {
    /// The host loop retained this successful outcome.
    Complete(NativeJointHandoffStepOutcomeV1),
    /// The target holds no confirmed release; no command was recorded.
    ReleaseUnconfirmed,
}

/// Test only: where this object's activation ends, as a coordinator that
/// stops there would.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RemoteTestStop {
    Open,
    Release,
    WriterPolicy,
}

/// A member's refusal of the read-only RELEASE observation while it holds no
/// confirmed catalogue activation: its Open has not completed. As for the
/// OLD observation there is no typed "not yet" answer.
const RELEASE_NOT_OPEN: &str =
    "joint bridge phase observation of this kind has no retained member state yet";
/// The retired source's durable retirement, as the completing metadata stage
/// reads it.
enum RemoteSourceRetirement {
    /// This coordinator is the retired source: its own service's receipt.
    Local(NativeJointSourceRetired),
    /// Another node is: its authenticated reply to this coordinator.
    Remote(NativeRemoteJointSourceRetirementCertificate),
}
impl RemoteSourceRetirement {
    fn proof(&self) -> &dyn NativeJointSourceRetirementProof {
        match self {
            Self::Local(retired) => retired,
            Self::Remote(certificate) => certificate,
        }
    }
}

/// Receiver-side budget of one OLD request; the receiver bounds it to 5 s.
const OLD_REQUEST_MS: u64 = 4000;
/// How long the learner allocation waits for the moved range's write command
/// in flight on this node to finish before the step ends and is repeated.
const ALLOCATION_HOLD_WAIT: Duration = Duration::from_secs(5);
/// How long the admission hold is kept after the allocation committed, until
/// this node's own published configuration shows it.
const ALLOCATION_PUBLISH_WAIT: Duration = Duration::from_secs(10);
/// How long the hold is kept after a commit attempt whose outcome is not
/// known. The allocation may still commit; a hold that ended at once would
/// admit a command under the previous descriptor, and that command would
/// straddle the allocation if it then commits. The wait is bounded because
/// the hold refuses every write and point read of the range.
const ALLOCATION_UNKNOWN_WAIT: Duration = Duration::from_secs(10);
/// This node's write coordinator of the ranges it leads.
pub(crate) type LocalRangeWriter =
    crate::distribution_range_consensus::RangeWriteCoordinator<TcpClusterRelocationTransport>;
use crate::distribution_range_consensus::RangeAllocationHold;
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

/// A member's refusal because another request of the same member holds the
/// state it reads without waiting. It asks for a repetition and says nothing
/// about the step. There is no typed "busy" answer; every such refusal of a
/// member carries this word.
fn member_busy(error: &BicDbError) -> bool {
    !matches!(error, BicDbError::QueryTimedOut) && error.to_string().contains("busy")
}
/// The pause before repetition number `repeated` (from 1) of a member request.
fn repeat_pause(repeated: u32) -> Duration {
    let (first, last) = BRIDGE_REPEAT_PAUSE;
    match repeated.checked_sub(BRIDGE_REPEAT_FREE + 1) {
        None => Duration::ZERO,
        Some(doubled) => first.saturating_mul(1u32 << doubled.min(8)).min(last),
    }
}
/// How many members are asked at once for a step with two observation
/// windows around a durable write (publication, Open, release, writer
/// policy). Such a member reads every other member twice, and a member that
/// is busy with its own step answers "busy" and is asked again; so does the
/// metadata leader beyond four metadata reads at a time. Measured
/// between five processes: two members at once finish in about 1.1 s where
/// one after the other needs 1.5 s, but three members' publications at once
/// needed 2.2 to 2.3 s where one after the other needs 1.7 s, almost all of
/// it repetitions of busy members. So two.
const WINDOWED_STEPS_AT_ONCE: usize = 2;
/// How many members are asked at once for a preparation (the member's own
/// local work), a custody retirement (one read of every other member, no
/// durable write under observation) or the read-only release observation.
/// Three at once were measured at 0.33 s and 0.62 s where one after the
/// other needs 0.65 s and 1.0 s. Not more than the measured three: every
/// request takes metadata reads on the metadata leader, which admits four
/// at a time and refuses the rest as busy.
const LIGHT_STEPS_AT_ONCE: usize = 3;

/// One independent exchange per member, each on its own scoped thread; all
/// of them have returned before this function does and the results keep the
/// order of `nodes`. One member, or a thread that cannot be started, runs
/// on the calling thread. A worker that panicked is an error of its member.
fn member_fan_out<R: Send>(
    nodes: &[ClusterNodeId],
    exchange: &(dyn Fn(&ClusterNodeId) -> Result<R> + Sync),
) -> Vec<(ClusterNodeId, Result<R>)> {
    if nodes.len() < 2 {
        return nodes
            .iter()
            .map(|node| (node.clone(), exchange(node)))
            .collect();
    }
    std::thread::scope(|scope| {
        let started = nodes
            .iter()
            .map(|node| {
                let worker = std::thread::Builder::new()
                    .name("range-move-member-step".into())
                    .spawn_scoped(scope, move || exchange(node));
                (node, worker)
            })
            .collect::<Vec<_>>();
        started
            .into_iter()
            .map(|(node, worker)| {
                let answer = match worker {
                    Ok(worker) => worker.join().unwrap_or_else(|_| {
                        Err(remote_error(format!(
                            "the request worker of member {node} panicked"
                        )))
                    }),
                    Err(_) => exchange(node),
                };
                (node.clone(), answer)
            })
            .collect()
    })
}
/// The order of an Open or a release: the target alone, then the surviving
/// members together. `order` names the target first.
fn target_then_survivors(
    order: &[ClusterNodeId],
    target: &ClusterNodeId,
) -> Vec<Vec<ClusterNodeId>> {
    let (first, rest): (Vec<_>, Vec<_>) = order.iter().cloned().partition(|node| node == target);
    [first, rest]
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}
fn remote_error(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("remote joined handoff backend: {}", detail.into()))
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

/// The decision identity this coordinator addresses the new members with.
#[derive(Clone)]
pub(crate) enum RemoteBridgeDecision {
    /// The certified record of the committed metadata. Every coordinator
    /// can read it again.
    Committed(NativeJointCommittedDecision),
    /// The decision body of a retained certificate, for a record that was
    /// certified without its scope.
    Body(crate::distribution_range_handoff::NativeJointDecisionData),
}
impl RemoteBridgeDecision {
    fn identity(&self) -> &dyn NativeJointBridgeDecision {
        match self {
            Self::Committed(decision) => decision,
            Self::Body(decision) => decision,
        }
    }
}
impl NativeJointBridgeDecision for RemoteBridgeDecision {
    fn checksum_sha256(&self) -> &str {
        self.identity().checksum_sha256()
    }
    fn plan_sha256(&self) -> Result<String> {
        self.identity().plan_sha256()
    }
    fn promise(&self) -> &crate::distribution_range_handoff::RangeHandoffPromise {
        self.identity().promise()
    }
    fn old_range(&self) -> &RangeDescriptor {
        self.identity().old_range()
    }
    fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        self.identity().old_members()
    }
    fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        self.identity().new_members()
    }
    fn require_metadata_domain(
        &self,
        topology: &ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()> {
        self.identity().require_metadata_domain(topology, restore)
    }
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
    /// The committed range and relocation the retained plan was read for.
    /// A plan is a bounded metadata acquisition: new work is issued under a
    /// fresh one of the same scope.
    committed: Option<(RangeDescriptor, RangeRelocation)>,
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
    /// The committed relocation the last `resync` was given.
    relocation_id: Option<RelocationId>,
    /// The certified decision as the committed record names it, read by the
    /// last `resync`. Committed configuration DATA: it replaces the
    /// certificate for everything after the certification.
    committed_decision: Option<NativeJointCommittedDecision>,
    /// Every new member's confirmed bridge publication, counted from the
    /// authenticated frames of this coordinator's own requests. Aggregation
    /// evidence only; a new object collects it again from the members.
    bridge_publication: Option<NativeRemoteBridgePublicationCertificate<RemoteBridgeDecision>>,
    /// What every new member answered for its completed Open, in the order
    /// the members were asked. Historical DATA. Empty when this object found
    /// every member already open and sent no Open.
    bridge_opened: Vec<(ClusterNodeId, NativeJointHandoffStepOutcomeV1)>,
    /// What every new member answered to the read-only RELEASE observation
    /// at the start of the last activation: `None` while its Open is not
    /// complete, otherwise whether it is released. Historical DATA.
    bridge_observed: Vec<(ClusterNodeId, Option<bool>)>,
    /// The members this object asked to retire their OLD custody.
    bridge_retired: Vec<ClusterNodeId>,
    /// What every new member this object asked answered for its completed
    /// release, in the order the members were asked. Historical DATA. A
    /// target that was observed released is not asked.
    bridge_released: Vec<(ClusterNodeId, NativeJointHandoffStepOutcomeV1)>,
    /// What every new member answered for its completed writer policy: the
    /// surviving members in node order, then the target. Historical DATA.
    bridge_policies: Vec<(ClusterNodeId, NativeJointHandoffStepOutcomeV1)>,
    /// What the target answered for its completed serving. Historical DATA.
    served: Option<NativeJointHandoffStepOutcomeV1>,
    /// The decision identity and members of the activation this object
    /// completed; `serve` addresses the target with them.
    activated: Option<(RemoteBridgeDecision, RemoteMembers)>,
    /// What the last call was waiting for when it ended; see `waiting()`.
    waiting: Option<String>,
    /// How long one call repeats a pending host command of the target.
    host_command_wait: Duration,
    /// This node's own data service, when the caller supplied it. See the
    /// module documentation.
    local: Option<Arc<Mutex<ClusterDataNodeService>>>,
    /// This node's own write coordinator of the ranges it leads, when the
    /// caller supplied it: the learner allocation is proposed under its
    /// admission hold of the moved range.
    local_writer: Option<Arc<LocalRangeWriter>>,
    /// The admission hold of the allocation being proposed. Held only
    /// between `before_allocation` and `after_allocation` of one call.
    allocation_hold: Option<RangeAllocationHold<TcpClusterRelocationTransport>>,
    /// How long the hold waits for this node's published change after a
    /// committed allocation and after one of unknown outcome.
    allocation_waits: (Duration, Duration),
    /// Operator notes since the last `take_notes`.
    notes: Vec<String>,
    /// Range, relocation and decision digest of the committed handoff record
    /// the last `resync` read.
    committed_record: Option<(RangeId, RelocationId, String)>,
    /// The decision body, checked against the committed digest: of the
    /// retained certificate, or of this node's own native decision record.
    decision_body: Option<NativeJointDecisionData>,
    /// The all-released observation of the new set, for the activation
    /// record. It lives inside one bounded window.
    activation: Option<NativeRemoteJointActivationCertificate>,
    /// The retired source's retirement, for the completion record.
    retired: Option<RemoteSourceRetirement>,
    /// The fact set of the decision phase this object ran, with that phase.
    /// Request DATA: a repeated publication towards a member that missed the
    /// decision names exactly this set again.
    decision_facts: Option<(Uuid, NativeRemoteNewFactSetV1)>,
    /// Test only: end the activation after the named part, as a
    /// coordinator that stops there would.
    #[cfg(test)]
    stop_after: Option<RemoteTestStop>,
    /// Test only: while it names a member, the publication of the decision
    /// is not sent to that member, as a request that timed out would not
    /// have reached it.
    #[cfg(test)]
    decision_publish_fault: Option<Arc<Mutex<Option<ClusterNodeId>>>>,
    /// A step that reads the other members (every bridge step but the
    /// preparation) had to repeat a member alone because it failed beside
    /// its neighbours: the members are too slow to read each other while
    /// each is busy with its own step. Every later such step of this object
    /// then asks one member at a time. Measured at a quarter of one CPU, a
    /// two-member phase run together took 20 to 25 s and each member alone
    /// 3 s; a three-member custody retirement together 25 s.
    windowed_alone: std::sync::atomic::AtomicBool,
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
            committed: None,
            donor: None,
            install: None,
            manifest: None,
            certificate: None,
            certificate_unrecoverable: None,
            relocation_id: None,
            committed_decision: None,
            bridge_publication: None,
            bridge_opened: Vec::new(),
            bridge_observed: Vec::new(),
            bridge_retired: Vec::new(),
            bridge_released: Vec::new(),
            bridge_policies: Vec::new(),
            served: None,
            activated: None,
            waiting: None,
            host_command_wait: HOST_COMMAND_WAIT,
            local: None,
            local_writer: None,
            allocation_hold: None,
            allocation_waits: (ALLOCATION_PUBLISH_WAIT, ALLOCATION_UNKNOWN_WAIT),
            notes: Vec::new(),
            committed_record: None,
            decision_body: None,
            activation: None,
            retired: None,
            decision_facts: None,
            #[cfg(test)]
            stop_after: None,
            #[cfg(test)]
            decision_publish_fault: None,
            windowed_alone: std::sync::atomic::AtomicBool::new(false),
        }
    }
    /// Give the backend this coordinator's OWN data service: the service of
    /// the node whose transport it was built with. It is used only for what
    /// this node does itself (see the module documentation); a service of
    /// another node is refused by that service's own check of the metadata
    /// read, which is this transport's.
    pub(crate) fn with_local_service(
        mut self,
        service: Arc<Mutex<ClusterDataNodeService>>,
    ) -> Self {
        self.local = Some(service);
        self
    }
    /// Give the backend this coordinator's OWN write coordinator: the one
    /// that admits the writes of the ranges this node leads. With it the
    /// learner allocation is proposed only while no write command of the
    /// moved range is between its admission and its local completion.
    pub(crate) fn with_local_writer(mut self, writer: Arc<LocalRangeWriter>) -> Self {
        self.local_writer = Some(writer);
        self
    }
    /// Test only: shorter waits of the allocation hold for this node's
    /// published change (after a commit, after an unknown outcome).
    #[cfg(test)]
    pub(crate) fn test_allocation_waits(mut self, committed: Duration, unknown: Duration) -> Self {
        self.allocation_waits = (committed, unknown);
        self
    }
    /// Test only: a shorter wait for a pending host command of the target.
    #[cfg(test)]
    pub(crate) fn test_host_command_wait(mut self, wait: Duration) -> Self {
        self.host_command_wait = wait;
        self
    }
    /// Test only: this object's activation ends after the named part.
    #[cfg(test)]
    pub(crate) fn test_stop_after(mut self, stop: RemoteTestStop) -> Self {
        self.stop_after = Some(stop);
        self
    }
    #[cfg(test)]
    fn test_stop(&self, stop: RemoteTestStop) -> Result<()> {
        if self.stop_after == Some(stop) {
            return Err(remote_error(format!("test stop after {stop:?}")));
        }
        Ok(())
    }
    /// Every new member's completed writer policy outcome, in request order.
    pub(crate) fn bridge_policies(&self) -> &[(ClusterNodeId, NativeJointHandoffStepOutcomeV1)] {
        &self.bridge_policies
    }
    /// The target's completed serving outcome.
    pub(crate) fn served(&self) -> Option<&NativeJointHandoffStepOutcomeV1> {
        self.served.as_ref()
    }
    /// The leader of the final membership must be the target; see the
    /// module documentation. `at` names where the refusal is raised.
    fn require_target_leads(&self, activation: &RangeDescriptor, at: &str) -> Result<()> {
        if activation.leader == self.target {
            return Ok(());
        }
        Err(remote_error(format!(
            "refused {at}: the final membership of range {} is led by {}, not by the target {}; a surviving member that leads the final membership must recover its leadership itself and no member step exists for that. Only a move that retires the range leader, which makes the target the leader, is supported",
            activation.id, activation.leader, self.target
        )))
    }
    /// What every new member answered to the RELEASE observation at the
    /// start of the last activation.
    pub(crate) fn bridge_observed(&self) -> &[(ClusterNodeId, Option<bool>)] {
        &self.bridge_observed
    }
    /// The members this object asked to retire their OLD custody.
    pub(crate) fn bridge_retired(&self) -> &[ClusterNodeId] {
        &self.bridge_retired
    }
    /// The completed release outcome of every new member this object asked,
    /// in request order.
    pub(crate) fn bridge_released(&self) -> &[(ClusterNodeId, NativeJointHandoffStepOutcomeV1)] {
        &self.bridge_released
    }
    /// The certified decision as the committed record names it.
    pub(crate) fn committed_decision(&self) -> Option<&NativeJointCommittedDecision> {
        self.committed_decision.as_ref()
    }
    /// Every new member's confirmed bridge publication.
    pub(crate) fn bridge_publication(
        &self,
    ) -> Option<&NativeRemoteBridgePublicationCertificate<RemoteBridgeDecision>> {
        self.bridge_publication.as_ref()
    }
    /// Every new member's completed Open outcome, in request order.
    pub(crate) fn bridge_opened(&self) -> &[(ClusterNodeId, NativeJointHandoffStepOutcomeV1)] {
        &self.bridge_opened
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

    /// A fresh acquisition of the retained plan's scope. The retained plan
    /// expires with its metadata acquisition; a round that issues new work
    /// starts under a live one. A fresh plan of another scope is refused.
    fn renewed_plan(
        &mut self,
        members: &RemoteMembers,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJointHandoffPlan>> {
        let (Some(retained), Some((range, relocation))) =
            (self.plan.clone(), self.committed.clone())
        else {
            return Err(remote_error(
                "no plan of an installed image is retained for this handoff",
            ));
        };
        let fresh = self.plan(members, &range, &relocation, parent)?;
        if fresh.checksum_sha256()? != retained.checksum_sha256()? {
            return Err(remote_error(
                "the pending plan changed its scope after the image was installed",
            ));
        }
        self.plan = Some(fresh.clone());
        Ok(fresh)
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
        self.committed = Some((range.clone(), relocation.clone()));
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
    /// One read-only probe of `node` for `range`, bounded by `token`. A busy
    /// member is asked again inside the same bound.
    fn probe(
        &self,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
        token: &CancellationToken,
    ) -> std::result::Result<NativeRangeMoveProbeRoleV1, String> {
        self.probe_reply(node, range, token).map(|reply| reply.role)
    }
    /// The same probe with the member's whole reply.
    fn probe_reply(
        &self,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
        token: &CancellationToken,
    ) -> std::result::Result<crate::distribution_transport::NativeRangeMoveProbeReplyV1, String>
    {
        let mut round = 0;
        loop {
            round += 1;
            match self
                .transport
                .native_range_move_probe(node, range.id, range.epoch, token)
            {
                Ok(reply) => return Ok(reply),
                Err(error) => {
                    let text = error.to_string();
                    if round >= 3 || !text.contains("busy") || token.check().is_err() {
                        return Err(text);
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
    /// Every voter of the committed range answers the read-only probe and
    /// none holds an unfinished native command of the range.
    pub(crate) fn require_no_unfinished_command(
        &self,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.unfinished_command_voter(range, parent)?
            .map_or(Ok(()), |(node, what)| Err(remote_error(format!(
                "refused before the target fence and the source cut: old voter {node} holds an unfinished {what} of range {} (it was in flight when the learner allocation committed, and the range's changed configuration does not let it finish). Its source Close would be refused on every repetition. Nothing was sent, so the move can still be aborted: abort it (move-range --abort) to return the range to its original voters, let it finish, then start the move again",
                range.id
            ))))
    }
    /// The first voter of the committed range that holds an unfinished
    /// native command or an unfinished normal recovery of it, with which of
    /// the two, asked read-only. An unfinished recovery is named as the
    /// member reported its record (epoch, record, phase, who fenced it), so
    /// that an operator knows which record on which member blocks the move.
    /// A voter that does not answer as one is an error.
    fn unfinished_command_voter(
        &self,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<Option<(ClusterNodeId, String)>> {
        for replica in &range.replicas {
            if replica.role != RangeReplicaRole::Voter {
                continue;
            }
            let node = &replica.node_id;
            let reply = self.probe_reply(node, range, &bounded(parent, REQUEST));
            let record = reply
                .as_ref()
                .ok()
                .and_then(|reply| reply.unfinished_recovery_record.clone());
            match reply.map(|reply| reply.role) {
                Ok(NativeRangeMoveProbeRoleV1::Voter {
                    unfinished_command: false,
                    unfinished_recovery: false,
                    ..
                }) => {}
                Ok(NativeRangeMoveProbeRoleV1::Voter {
                    unfinished_command: true,
                    ..
                }) => return Ok(Some((node.clone(), "write command".into()))),
                Ok(NativeRangeMoveProbeRoleV1::Voter { .. }) => {
                    return Ok(Some((
                        node.clone(),
                        match record {
                            Some(record) => format!("recovery ({record})"),
                            None => "recovery (the member did not name its record)".into(),
                        },
                    )))
                }
                Ok(NativeRangeMoveProbeRoleV1::RegisteredReplicaReceiver) => {
                    return Err(remote_error(format!(
                        "refused before any member was changed: old voter {node} answered as a registered replica receiver, not as a data owner"
                    )))
                }
                Err(error) => {
                    parent.check()?;
                    return Err(remote_error(format!(
                        "refused before any member was changed: old voter {node} did not answer the read-only probe: {error}"
                    )));
                }
            }
        }
        Ok(None)
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
            // An old voter still answers client requests of the range (it
            // refuses them, and holds its own state while it does). A Close
            // that finds that state held is refused as "busy"; it is sent
            // again, as a new request under a new metadata read, for a
            // bounded time. A repeated Close continues the same operation
            // and confirms the same promise and close record.
            let until = Instant::now() + CLOSE_BUSY_WAIT;
            let mut busy = 0u32;
            let frame = loop {
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
                match self
                    .transport
                    .request_remote_native_old(recipient, &plan, &request, &own, &operation)
                {
                    Ok(frame) => break frame,
                    Err(error) if Instant::now() < until && member_busy(&error) => {
                        parent.check()?;
                        busy = busy.saturating_add(1);
                        std::thread::sleep(repeat_pause(busy.saturating_add(BRIDGE_REPEAT_FREE)));
                    }
                    Err(error) => return Err(stage_error("source close", error)),
                }
            };
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

    /// The durable range-log epoch the old voter reports to the read-only
    /// range move probe. The voter answers the leader of the range in its
    /// own committed configuration only, and only for the range epoch that
    /// configuration records; every refusal is an error here. The probe is
    /// not tied to the voter's installed writer policy, so it is answered
    /// after the learner allocation too, which is when an abort needs it.
    fn old_epoch(
        &self,
        _hosts: &Self::Hosts,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
    ) -> Result<u64> {
        let token = bounded(&CancellationToken::uncancelable(), REQUEST);
        let unreadable =
            |error: String| remote_error(format!("old voter {node} epoch is unreadable: {error}"));
        match self.probe(node, range, &token).map_err(unreadable)? {
            NativeRangeMoveProbeRoleV1::Voter { durable_epoch, .. } => Ok(durable_epoch),
            NativeRangeMoveProbeRoleV1::RegisteredReplicaReceiver => Err(unreadable(
                "it answered as a registered replica receiver, not as a voter of the range".into(),
            )),
        }
    }

    fn forget_aborted_allocation(&mut self) {
        self.closed.clear();
        self.closed_scope_sha256 = None;
        self.observed.clear();
        self.plan = None;
        self.committed = None;
    }

    /// Verify, on committed metadata, that the target can be admitted;
    /// nothing is created. The target is admitted as a registered replica
    /// receiver by its own operator action; whether that receiver is open
    /// is asked by `preflight`, which the stage logic calls after this
    /// while no relocation is committed.
    ///
    /// This is also where a coordinator that is not the metadata leader is
    /// refused, before the learner allocation is proposed. The range leader
    /// half of the rule needs the committed relocation and is checked by
    /// `resync` and `cut_and_ship`.
    fn admit_target(&mut self, hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        self.waiting = None;
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

    /// Immediately before the learner allocation is proposed. A write
    /// command that is admitted under the range's present descriptor and
    /// finishes after the allocation committed is refused by its own commit
    /// callback with its native pending record durable, and nothing settles
    /// it while the allocation stands. So the allocation is proposed only
    /// under this node's admission hold of the range (this node leads it):
    /// the hold is granted once no command of the range is between its
    /// admission and its local completion, waiting for one boundedly, and it
    /// refuses every new admission before it prepares anything. Under the
    /// hold every voter is asked, read-only, and must hold no unfinished
    /// command. A refusal here committed nothing: the command is settled by
    /// the range's recovery under its unchanged configuration and the step
    /// is repeated.
    fn before_allocation(
        &mut self,
        _hosts: &mut Self::Hosts,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.allocation_hold = None;
        let hold = match &self.local_writer {
            Some(writer) => Some(
                writer
                    .hold_range_for_allocation(range.id, ALLOCATION_HOLD_WAIT, parent)
                    .map_err(|error| {
                        self.waiting = Some(format!(
                            "waiting for a write command of range {} on this node to finish before the learner allocation is proposed",
                            range.id
                        ));
                        remote_error(format!(
                            "the learner allocation of range {} was not proposed: {error}. Nothing was committed",
                            range.id
                        ))
                    })?,
            ),
            None => None,
        };
        if let Some((node, what)) = self.unfinished_command_voter(range, parent)? {
            self.waiting = Some(format!(
                "waiting for the unfinished {what} of range {} on voter {node} to be settled before the learner allocation is proposed",
                range.id
            ));
            return Err(remote_error(format!(
                "the learner allocation of range {} was not proposed: voter {node} holds an unfinished {what} of the range, which is settled only while the range keeps its present configuration. Nothing was committed",
                range.id
            )));
        }
        if hold.is_none() {
            // The hold is this node's own write coordinator's; a host that
            // was given none proposes the allocation without it, and says so.
            self.notes.push(format!(
                "the learner allocation of range {} is proposed without a write hold: this node has no write coordinator of its own ranges (it is not an initial data owner). A write in flight on this node when the allocation commits is left unfinished; the source cut then refuses by name, and the move must be aborted and started again",
                range.id
            ));
        }
        self.allocation_hold = hold;
        Ok(())
    }

    /// The allocation commit attempt is over. After a commit the hold is
    /// kept until this node's own published configuration shows the changed
    /// range, so that no command is admitted under the previous descriptor.
    /// After an attempt whose outcome is not known the allocation may still
    /// commit, so the hold is kept the same way, bounded. Only an attempt
    /// that cannot commit any more releases the hold at once.
    ///
    /// The wait is bounded because the hold refuses the range's writes. When
    /// it ends without the published change, the hold is released and the
    /// fact is kept as an operator note: a command admitted from then on
    /// under the previous descriptor straddles the allocation if it commits,
    /// and the source cut refuses such a move by name.
    fn after_allocation(
        &mut self,
        range: &RangeDescriptor,
        outcome: AllocationCommitOutcome,
        parent: &CancellationToken,
    ) {
        let Some(hold) = self.allocation_hold.take() else {
            return;
        };
        let (wait, what) = match outcome {
            AllocationCommitOutcome::NotCommitted => return,
            AllocationCommitOutcome::Committed => (
                self.allocation_waits.0,
                "the learner allocation is committed",
            ),
            AllocationCommitOutcome::Unknown => (
                self.allocation_waits.1,
                "the outcome of the learner allocation commit is not known and it may still commit",
            ),
        };
        let started = Instant::now();
        let published = hold.wait_published_change(range, wait, parent);
        trace(started, || {
            format!("allocation hold: {what}; published change seen={published}")
        });
        if !published {
            self.notes.push(format!(
                "the write hold of range {} ended after {} ms although this node had not published the changed range: {what}. A write admitted on this node since then under the previous configuration is left unfinished by the allocation; the source cut then refuses by name, and the move must be aborted and started again",
                range.id,
                started.elapsed().as_millis()
            ));
        }
        drop(hold);
    }

    fn take_notes(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notes)
    }

    /// Before the learner allocation is proposed: ask the target and every
    /// voter of the range, read-only, whether the move can start. The target
    /// must answer as a live registered replica receiver and every voter
    /// must answer with its durable epoch; each answers this coordinator
    /// only as the leader of the range in its own committed configuration.
    /// A member that does not answer so refuses the move here, by name, and
    /// nothing is committed.
    fn preflight(
        &mut self,
        _hosts: &mut Self::Hosts,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let coordinator = self.coordinator();
        if coordinator != range.leader {
            return Err(remote_error(format!(
                "coordinator {coordinator} is not the leader {} of range {}; a move is coordinated by the node that is both the metadata leader and the leader of the range, and nothing was committed",
                range.leader, range.id
            )));
        }
        let refused = |member: &ClusterNodeId, what: &str, error: String| {
            remote_error(format!(
                "the move of range {} onto {} cannot start: {what} {member} is not ready: {error}. Nothing was committed",
                range.id, self.target
            ))
        };
        let token = bounded(parent, REQUEST);
        match self.probe(&self.target, range, &token) {
            Ok(NativeRangeMoveProbeRoleV1::RegisteredReplicaReceiver) => {}
            Ok(NativeRangeMoveProbeRoleV1::Voter { .. }) => {
                return Err(refused(
                    &self.target,
                    "target",
                    "it answered as a voter of the range, not as a registered replica receiver"
                        .into(),
                ))
            }
            Err(error) => {
                parent.check()?;
                return Err(refused(
                    &self.target,
                    "target",
                    format!(
                        "it did not answer as a registered replica receiver (open its replica receiver first): {error}"
                    ),
                ));
            }
        }
        for replica in &range.replicas {
            if replica.role != RangeReplicaRole::Voter {
                continue;
            }
            let token = bounded(parent, REQUEST);
            match self.probe(&replica.node_id, range, &token) {
                Ok(NativeRangeMoveProbeRoleV1::Voter {
                    writer_policy_refusal: Some(why),
                    ..
                }) => {
                    return Err(refused(
                        &replica.node_id,
                        "voter",
                        format!(
                            "its installed writer policy does not cover the committed membership ({why}). A voter publishes a member that joined by its own registry refresh (its status reports the last attempt); until it has, it would refuse {} as the target of the image, and a refused image capture cannot be started again",
                            self.target
                        ),
                    ))
                }
                Ok(NativeRangeMoveProbeRoleV1::Voter { durable_epoch, .. })
                    if durable_epoch <= range.epoch => {}
                Ok(NativeRangeMoveProbeRoleV1::Voter { durable_epoch, .. }) => {
                    return Err(refused(
                        &replica.node_id,
                        "voter",
                        format!(
                        "its range log is at epoch {durable_epoch}, beyond the committed epoch {}",
                        range.epoch
                    ),
                    ))
                }
                Ok(NativeRangeMoveProbeRoleV1::RegisteredReplicaReceiver) => {
                    return Err(refused(
                        &replica.node_id,
                        "voter",
                        "it answered as a registered replica receiver, not as a data owner".into(),
                    ))
                }
                Err(error) => {
                    parent.check()?;
                    return Err(refused(&replica.node_id, "voter", error.to_string()));
                }
            }
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
    /// At every certified stage the committed record is read: when it
    /// carries the certified scope, the committed decision replaces the
    /// certificate for the union and the final membership and for the
    /// bridge requests.
    ///
    /// From `PromotedFrozen` on the certificate cannot be collected again:
    /// its scope is the pending plan, which the metadata leader grants only
    /// to the leader of the range at its pre-promotion epoch, and the
    /// promotion made the target the leader at the next epoch. Nothing is
    /// guessed: for a record that was certified without its scope,
    /// `require_decision` refuses the final membership stage of a
    /// coordinator that does not retain the certificate.
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
        self.relocation_id = Some(relocation.id);
        self.committed_record = None;
        self.committed_decision = None;
        self.certificate_unrecoverable = None;
        match recorded {
            None => {
                if !self.cut_started {
                    self.observe_cut(members, range, relocation, parent)?;
                }
            }
            Some(stage) => {
                // A certified decision exists only after a source cut.
                self.cut_started = true;
                self.committed_record = relocation
                    .native_joint_handoff_v1
                    .as_ref()
                    .map(|record| (range.id, relocation.id, record.decision_sha256().to_owned()));
                self.committed_decision = NativeJointCommittedDecision::from_committed(relocation)
                    .map_err(|error| stage_error("committed decision record", error))?;
                if self.certificate.is_some() {
                    return Ok(());
                }
                match stage {
                    NativeJointHandoffStageV1::Certified => {
                        match self.recover_certified_decision(members, range, relocation, parent) {
                            Ok(()) => {}
                            // The committed record carries the certified
                            // scope: the union membership needs no
                            // certificate, so a member that cannot answer
                            // the read-only collection does not stop it.
                            Err(_) if self.committed_decision.is_some() => parent.check()?,
                            Err(error) => return Err(error),
                        }
                    }
                    NativeJointHandoffStageV1::PromotedFrozen
                        if self.committed_decision.is_none() =>
                    {
                        self.certificate_unrecoverable = Some(
                            "the committed handoff is at the frozen union membership, its record was certified without the decision scope, and this coordinator does not retain its decision certificate; the certificate is scoped to the pending plan, which only the pre-promotion range leader can read, so it cannot be collected again and the final membership cannot be proposed by this coordinator"
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
    /// the donor's retained export terminal. The fence is issued once per
    /// object; a new object finds it refused once the target holds install
    /// custody and relies on the install steps (see below).
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
        // An old member already retains an operation of this handoff: an
        // earlier coordinator object fenced the target and started the cut.
        let resumed = self.cut_started;
        // Before the first member mutation, and before anything can fail.
        self.cut_started = true;
        let started = Instant::now();
        let plan = self
            .plan(members, range, relocation, parent)
            .map_err(|error| stage_error("source round metadata", error))?;
        trace(started, || "cut: plan".into());
        // Before the target is fenced or any old member is closed. Nothing
        // was sent by this call, so it started no cut.
        if let Err(error) = self.require_target_leads(
            plan.activation_range(),
            "at the source stage, before the target fence and the source cut",
        ) {
            self.cut_started = resumed;
            return Err(error);
        }
        // Still before the target is fenced or any old member is closed: a
        // voter that holds an unfinished write command refuses its Close on
        // every repetition, and after a started source round the move could
        // not be aborted either. Asked read-only; nothing was sent by this
        // call, so it started no cut and the abort stays possible.
        if !resumed {
            let started = Instant::now();
            if let Err(error) = self.require_no_unfinished_command(range, parent) {
                self.cut_started = resumed;
                return Err(error);
            }
            trace(started, || "cut: unfinished command probe".into());
        }
        let donor = Self::select_donor(&plan)?;
        let target_attempt = Self::target_attempt(&plan)?;
        let mut fence_refusal = None;
        let started = Instant::now();
        if !self.target_fenced {
            match self.fence_target(members, &plan, parent) {
                Ok(()) => self.target_fenced = true,
                // A target that already holds install custody of this session
                // refuses another fence. When the cut is known to have been
                // started, the install steps below settle it: the target
                // serves them only under the fence this node issued, and
                // only for the one attempt of this plan. Without a started
                // cut a refused fence ends the call before any old member
                // is closed.
                Err(error) if resumed => {
                    parent.check()?;
                    fence_refusal = Some(error.to_string());
                }
                Err(error) => return Err(stage_error("target fence", error)),
            }
        }
        trace(started, || "cut: target fence".into());
        let started = Instant::now();
        self.close_old_members(members, range, relocation, parent)?;
        trace(started, || "cut: source close of every old member".into());
        let started = Instant::now();
        self.plan = Some(plan.clone());
        self.committed = Some((range.clone(), relocation.clone()));
        self.donor = Some(donor.clone());
        let installed = self
            .install_on_target(members, &plan, &donor, target_attempt, parent)
            .map_err(|error| match (error, fence_refusal) {
                (BicDbError::QueryTimedOut, _) => BicDbError::QueryTimedOut,
                (error, Some(fence)) => handoff_error(format!(
                    "target install: {error}; the target also refused a new fence: {fence}"
                )),
                (error, None) => stage_error("target install", error),
            })?;
        trace(started, || "cut: target install".into());
        let started = Instant::now();
        // The target served the install under this node's fence.
        self.target_fenced = true;
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
        trace(started, || "cut: source export retirement".into());
        self.install = Some(installed);
        self.manifest = Some(manifest);
        Ok(())
    }

    fn decision_durable(&self) -> bool {
        self.certificate.is_some() || self.committed_decision.is_some()
    }

    /// Obtain the decision certificate; see `complete_decision` for the rule
    /// that every member which later needs the decision must hold it before
    /// the certificate is retained.
    fn decide(&mut self, members: &RemoteMembers, parent: &CancellationToken) -> Result<()> {
        let started = Instant::now();
        let decided = self.decide_on_every_member(members, parent);
        trace(started, || format!("decision ok={}", decided.is_ok()));
        decided
    }

    /// The retained certificate, or the certified decision of the committed
    /// record. A relocation without a committed record has only the former.
    fn require_decision(&self) -> Result<()> {
        if self.certificate.is_some() || self.committed_decision.is_some() {
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
    /// by the same stage functions the in-process backend uses, and the two
    /// completion records, built from the proofs `prove_activation` and
    /// `retire_source` retained.
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
                let proof = self.activation.as_ref().ok_or_else(|| {
                    remote_error("activation proof absent before the activation record")
                })?;
                view.record_native_joint_activation(proof, fresh, business, actor, now)
                    .map_err(|error| stage_error("activation record", error))?;
                return Ok(());
            }
            Some(NativeJointHandoffStageV1::AwaitingCleanup) => {
                let retired = self.retired.as_ref().ok_or_else(|| {
                    remote_error("source retirement receipt absent before completion")
                })?;
                view.complete_native_joint_source_retirement(
                    retired.proof(),
                    fresh,
                    business,
                    actor,
                    now,
                )
                .map_err(|error| stage_error("source retirement record", error))?;
                return Ok(());
            }
            _ => {}
        }
        self.require_decision()?;
        // After the certification the committed record alone is enough; a
        // retained certificate is still used, and must equal that record.
        let certified = || -> Result<RelocationId> {
            match (&self.committed_decision, self.relocation_id) {
                (Some(_), Some(id)) => Ok(id),
                _ => Err(remote_error(
                    "decision certificate absent and the committed record carries no certified scope",
                )),
            }
        };
        match (committed, self.certificate.as_ref()) {
            (None, Some(certificate)) => {
                view.certify_native_joint_handoff(certificate, fresh, business, actor, now)
                    .map_err(|error| stage_error("decision certification", error))?;
            }
            (None, None) => {
                return Err(remote_error(
                    "decision certificate absent; the certification needs the actual decision certificate",
                ))
            }
            (Some(NativeJointHandoffStageV1::Certified), Some(certificate)) => {
                view.promote_native_joint_handoff_frozen(certificate, fresh, business, actor, now)
                    .map_err(|error| stage_error("union membership", error))?;
            }
            (Some(NativeJointHandoffStageV1::Certified), None) => {
                view.promote_certified_native_joint_handoff_frozen(
                    certified()?,
                    fresh,
                    business,
                    actor,
                    now,
                )
                .map_err(|error| stage_error("union membership", error))?;
            }
            (Some(NativeJointHandoffStageV1::PromotedFrozen), Some(certificate)) => {
                view.finalize_native_joint_membership_frozen(
                    certificate,
                    fresh,
                    business,
                    actor,
                    now,
                )
                .map_err(|error| stage_error("final membership", error))?;
            }
            (Some(NativeJointHandoffStageV1::PromotedFrozen), None) => {
                view.finalize_certified_native_joint_membership_frozen(
                    certified()?,
                    fresh,
                    business,
                    actor,
                    now,
                )
                .map_err(|error| stage_error("final membership", error))?;
            }
            (Some(_), _) => return Err(remote_error("relocation is already completed")),
        }
        Ok(())
    }

    /// The activation, by step requests to every new member, in the orders
    /// of the in-process driver (preparation and publication in node order;
    /// Open and release on the target first, then on the surviving members
    /// in node order; the writer policy on the surviving members in node
    /// order, then on the target):
    /// 1. `BridgePrepare`: the member prepares its own joint serving. A
    ///    surviving member closes its writer DATA domain over its own
    ///    retained source closure, under a policy it reads from its own
    ///    metadata, and takes the lease of that closure itself; the target
    ///    prepares over its own retained joint target. This is what the
    ///    in-process driver does with the hosts' policy continuation and by
    ///    dropping the retained source closures: here the closure stays with
    ///    the member, whose own step consumes it, so neither has a separate
    ///    request.
    /// 2. `BridgePublish`: the member publishes its own bridge after reading
    ///    every other member's preparation itself. The authenticated frames
    ///    are counted into the publication certificate.
    /// 3. `BridgeOpen`: the member persists its own catalogue activation
    ///    after reading every other member's bridge itself.
    /// 4. `RetireOldCustody`: the member retires the OLD custody it still
    ///    holds after proving, itself, every new member durably open.
    /// 5. `BridgeRelease`: the member releases its own freeze, repeated
    ///    while the member answers "not complete".
    /// 6. `BridgeWriterPolicy`: the member publishes its own writer DATA
    ///    policy for the final membership after reading every other new
    ///    member's audited release itself. A surviving member completes
    ///    inside its step, repeated while it answers "not complete". The
    ///    target records the command for its own host loop and answers
    ///    "host command pending" until that loop retained the outcome; the
    ///    step is repeated for a bounded time (see the module documentation).
    ///
    /// A repeated activation first asks every member, read-only, for its
    /// RELEASE observation. A member answers it only once its own Open is
    /// confirmed. When every member answers, parts 1 to 3 are not sent
    /// again: a released member has ended its preparation custody and
    /// refuses a repeated publication or Open. A member that is observed
    /// released is not asked to retire custody again either, and a target
    /// that is observed released is not sent its release again: it may hold
    /// its writer policy already, and its host then refuses that step. A
    /// surviving member answers a repeated release and a repeated writer
    /// policy from what it retained, and the target a repeated writer policy
    /// from its retained host command outcome; none repeats durable work.
    ///
    /// Nothing is retained that a new object could not obtain again: the
    /// decision identity comes from the committed record, and a member
    /// answers a repeated step from what it retained or durably holds.
    ///
    /// The members admit a bridge mutation from the metadata leader or from
    /// the committed range leader. After the promotion the target leads the
    /// range, so this coordinator is admitted as the metadata leader only;
    /// that is verified first. A coordinator that is itself a new member
    /// would have to perform its own steps locally, for which no entry
    /// exists; it is refused before any request.
    fn activate(
        &mut self,
        hosts: &mut Self::Hosts,
        members: &RemoteMembers,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.waiting = None;
        self.activated = None;
        let coordinator = self.coordinator();
        let metadata_leader = hosts.metadata_leader()?;
        if coordinator != metadata_leader || members.metadata_leader != metadata_leader {
            return Err(remote_error(format!(
                "coordinator {coordinator} is not the metadata leader {metadata_leader}; after the promotion the target leads the range, and the new members admit bridge steps from the metadata leader or the committed range leader only"
            )));
        }
        let topology = hosts.committed_topology()?;
        let relocation = self
            .relocation_id
            .and_then(|id| topology.relocations.get(&id))
            .filter(|relocation| relocation.target == self.target)
            .ok_or_else(|| {
                remote_error("the activation has no committed relocation of this handoff")
            })?;
        if Self::recorded(relocation) != Some(NativeJointHandoffStageV1::AwaitingActivation) {
            return Err(remote_error(
                "the activation needs the committed final membership of this handoff",
            ));
        }
        // The record this activation is made for, as committed now: the
        // last `resync` of an uninterrupted drive ran before any record of
        // this handoff was committed. The activation record is proven for
        // exactly this record.
        self.committed_record = relocation.native_joint_handoff_v1.as_ref().map(|record| {
            (
                relocation.range_id,
                relocation.id,
                record.decision_sha256().to_owned(),
            )
        });
        // The committed final membership names the leader of the activation.
        let activation = topology
            .range_by_id(relocation.range_id)
            .ok_or_else(|| remote_error("the activation has no committed range of this handoff"))?;
        self.require_target_leads(activation, "by the activation, before any bridge step")?;
        // Committed metadata first: it is what a coordinator without memory
        // has. A record certified without its scope leaves only the body of
        // a retained certificate.
        let decision = match NativeJointCommittedDecision::from_committed(relocation)
            .map_err(|error| stage_error("committed decision record", error))?
        {
            Some(decision) => RemoteBridgeDecision::Committed(decision),
            None => match self.certificate.as_ref() {
                Some(certificate) => RemoteBridgeDecision::Body(certificate.data().clone()),
                None => {
                    return Err(remote_error(
                        "the committed record carries no certified scope and no decision certificate is retained; the new members cannot be addressed",
                    ))
                }
            },
        };
        self.activate_new_members(decision, members, parent)
    }

    /// `TargetServe`: the target, holding its own confirmed writer policy,
    /// moves that policy to its ordinary DATA socket, recovers the ranges it
    /// leads and publishes its guarded ingress. The transition belongs to
    /// the target's own host loop: the step is repeated like the target's
    /// writer policy, and a target that already serves answers with its
    /// retained outcome. Returns the ranges the target recovered.
    fn serve(&mut self, _hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<usize> {
        parent.check()?;
        self.waiting = None;
        let (decision, members) = self.activated.clone().ok_or_else(|| {
            remote_error("target serve needs the activation this coordinator object completed")
        })?;
        let operation = NativeJointHandoffStepOperationV1::TargetServe {
            decision_sha256: decision.checksum_sha256().to_owned(),
        };
        let outcome =
            match self.complete_host_command(&members, &decision, &operation, "serve", parent)? {
                HostCommandAnswer::Complete(outcome) => outcome,
                HostCommandAnswer::ReleaseUnconfirmed => {
                    return Err(remote_error(format!(
                        "target {} holds no confirmed release at its serve step",
                        self.target
                    )))
                }
            };
        let recovered = outcome.target_recovered_ranges.ok_or_else(|| {
            remote_error("the target completed its serve step without its recovered ranges")
        })?;
        self.served = Some(outcome);
        Ok(recovered as usize)
    }

    fn waiting(&self) -> Option<String> {
        self.waiting.clone()
    }

    fn records_completion(&self) -> bool {
        true
    }

    /// Observe every member of the new set released and open, as the
    /// coordinator outside the new set: one authenticated RELEASE frame of
    /// every member inside one bounded window over this coordinator's own
    /// transport. The decision the observation is made for is the body
    /// whose checksum is the committed digest.
    fn prove_activation(
        &mut self,
        members: &RemoteMembers,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.waiting = None;
        self.activation = None;
        let (range, relocation, digest) = self.committed_record.clone().ok_or_else(|| {
            remote_error("the activation record has no committed handoff record of this handoff")
        })?;
        let body = self.committed_decision_body(members, range, relocation, &digest, parent)?;
        // This is the first observation made while the range serves client
        // requests again. A member reads its own state without waiting for
        // its data service and answers "busy" while a client request holds
        // it; that refusal asks for a repetition. The whole window is
        // observed again each time: nothing of a refused one is kept.
        let until = Instant::now() + ACTIVATION_BUSY_WAIT;
        let mut busy = 0u32;
        let certificate = loop {
            let started = Instant::now();
            let observed = NativeRemoteJointActivationCertificate::observe_as_outside_coordinator(
                &body,
                &self.transport,
                &members.metadata_leader,
                parent,
            );
            trace(started, || {
                format!(
                    "activation observation: {}",
                    match &observed {
                        Ok(_) => "ok".to_string(),
                        Err(error) => format!("error {error}"),
                    }
                )
            });
            match observed {
                Ok(certificate) => break certificate,
                Err(error) if Instant::now() < until && member_busy(&error) => {
                    parent.check()?;
                    busy = busy.saturating_add(1);
                    // Never at once: the member is asked again after a pause.
                    std::thread::sleep(repeat_pause(busy.saturating_add(BRIDGE_REPEAT_FREE)));
                }
                Err(error) => return Err(error),
            }
        };
        self.activation = Some(certificate);
        Ok(())
    }

    /// The retired source records its own retirement on its own committed
    /// metadata read. Nothing of the earlier stages is needed, so a new
    /// object finishes it.
    ///
    /// When this coordinator is the retired source (the handoff retires the
    /// pre-handoff range leader, which coordinates), its own data service
    /// records it: the same confirmation and retirement the step receiver
    /// runs, on this node's own metadata read. Otherwise the source is
    /// asked by a `SourceRetire` step and its authenticated reply is kept.
    fn retire_source(
        &mut self,
        members: &RemoteMembers,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.waiting = None;
        self.retired = None;
        let source = relocation
            .source
            .clone()
            .ok_or_else(|| remote_error("the committed relocation names no source"))?;
        if range
            .replicas
            .iter()
            .any(|replica| replica.node_id == source)
        {
            return Err(remote_error(
                "the source is still a replica of the committed range",
            ));
        }
        let coordinator = self.coordinator();
        if coordinator != members.metadata_leader {
            return Err(remote_error(format!(
                "coordinator {coordinator} is not the metadata leader {}; the retired source admits its retirement step from the metadata leader or the committed range leader only",
                members.metadata_leader
            )));
        }
        if source == coordinator {
            let local = self.local.clone().ok_or_else(|| {
                remote_error(format!(
                    "coordinator {coordinator} is the retired source and was not given its own data service; its own retirement cannot be recorded by a request to itself"
                ))
            })?;
            let retired = retry_timed_out(|| {
                let token = bounded(parent, REQUEST);
                let business = Uuid::now_v7();
                let own = self.transport.acquire_host_metadata_observation(
                    &members.metadata_leader,
                    business,
                    REQUEST,
                    &token,
                )?;
                ClusterDataNodeService::retire_confirmed_native_joint_source(
                    &local,
                    &own,
                    business,
                    relocation.id,
                    range.id,
                    &token,
                )
            })
            .map_err(|error| stage_error("source retirement", error))?;
            self.retired = Some(RemoteSourceRetirement::Local(retired));
            return Ok(());
        }
        let digest = relocation
            .native_joint_handoff_v1
            .as_ref()
            .map(|record| record.decision_sha256().to_owned())
            .ok_or_else(|| remote_error("the committed relocation records no joint decision"))?;
        let body =
            self.committed_decision_body(members, range.id, relocation.id, &digest, parent)?;
        let operation = NativeJointHandoffStepOperationV1::SourceRetire {
            decision_sha256: digest,
        };
        let phase = bounded(parent, BRIDGE_PHASE);
        let frame = self
            .complete_bridge_step(members, &body, &source, &operation, &phase)
            .map_err(|error| stage_error(&format!("source retirement of {source}"), error))?;
        self.retired = Some(RemoteSourceRetirement::Remote(
            NativeRemoteJointSourceRetirementCertificate::from_frame(&body, frame)
                .map_err(|error| stage_error("source retirement certificate", error))?,
        ));
        Ok(())
    }
}

impl RemoteJoinedHandoffBackend {
    /// One bounded bridge step request to one new member, as this
    /// coordinator. The frame is what the pinned exchange authenticated.
    fn bridge_step(
        &self,
        members: &RemoteMembers,
        decision: &dyn NativeJointBridgeDecision,
        recipient: &ClusterNodeId,
        operation: &NativeJointHandoffStepOperationV1,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        Self::bridge_step_over(
            &self.transport,
            members,
            decision,
            recipient,
            operation,
            parent,
        )
    }

    /// `bridge_step` over a transport alone: it reads nothing of this
    /// object, so the steps of one phase can run on scoped threads. Every
    /// call acquires its own fresh metadata observation and its own request
    /// identity; nothing is shared between two members' requests.
    fn bridge_step_over(
        transport: &TcpClusterRelocationTransport,
        members: &RemoteMembers,
        decision: &dyn NativeJointBridgeDecision,
        recipient: &ClusterNodeId,
        operation: &NativeJointHandoffStepOperationV1,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        let token = bounded(parent, REQUEST);
        let business = Uuid::now_v7();
        let started = Instant::now();
        let own = transport.acquire_host_metadata_observation(
            &members.metadata_leader,
            business,
            REQUEST,
            &token,
        )?;
        let observed = started.elapsed();
        let request = NativeJointHandoffStepRequestV1::from_decision(
            decision,
            operation.clone(),
            business,
            BRIDGE_REQUEST_MS,
        )?;
        let answer = Self::bridge_step_request(
            transport, recipient, decision, operation, &request, &own, &token,
        );
        trace(started, || {
            format!(
                "bridge step {} of {recipient}: own metadata {} ms, then {}",
                operation.trace_name(),
                observed.as_millis(),
                match &answer {
                    Ok(frame) => format!("complete={}", frame.outcome().complete),
                    Err(error) => format!("error {error}"),
                }
            )
        });
        answer
    }

    fn bridge_step_request(
        transport: &TcpClusterRelocationTransport,
        recipient: &ClusterNodeId,
        decision: &dyn NativeJointBridgeDecision,
        operation: &NativeJointHandoffStepOperationV1,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        token: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        match operation {
            NativeJointHandoffStepOperationV1::BridgePrepare { .. } => transport
                .request_native_joint_bridge_prepare(recipient, decision, request, own, token),
            NativeJointHandoffStepOperationV1::BridgePublish { .. } => transport
                .request_native_joint_bridge_publish(recipient, decision, request, own, token),
            NativeJointHandoffStepOperationV1::BridgeOpen { .. } => {
                transport.request_native_joint_bridge_open(recipient, decision, request, own, token)
            }
            NativeJointHandoffStepOperationV1::BridgePhaseObserve { kind, .. } => transport
                .request_native_joint_bridge_phase_observation(
                    recipient, decision, *kind, request, own, token,
                ),
            NativeJointHandoffStepOperationV1::RetireOldCustody { .. } => transport
                .request_native_joint_bridge_retire_old_custody(
                    recipient, decision, request, own, token,
                ),
            NativeJointHandoffStepOperationV1::BridgeRelease { .. } => transport
                .request_native_joint_bridge_release(recipient, decision, request, own, token),
            NativeJointHandoffStepOperationV1::BridgeWriterPolicy { .. } => transport
                .request_native_joint_bridge_writer_policy(
                    recipient, decision, request, own, token,
                ),
            NativeJointHandoffStepOperationV1::TargetServe { .. } => transport
                .request_native_joint_target_serve(recipient, decision, request, own, token),
            NativeJointHandoffStepOperationV1::SourceRetire { .. } => transport
                .request_native_joint_source_retire(recipient, decision, request, own, token),
            NativeJointHandoffStepOperationV1::TargetInstall { .. }
            | NativeJointHandoffStepOperationV1::SourceExportRetire { .. } => {
                Err(remote_error("not a bridge step of the activation"))
            }
        }
    }

    /// Repeat one member's bridge step until the member reports it complete.
    /// An incomplete answer is progress and a failed request has an unknown
    /// effect; either way the member continues the same step, which the
    /// request identity names, on the next request.
    ///
    /// The repetition is bounded by time, not by a number of requests: it
    /// ends when `phase` ends. While the range serves client requests a
    /// member answers "busy" whenever a client request holds the state the
    /// step reads, and a member's own confirmation window fails (the answer
    /// is then incomplete) whenever another member is busy; neither says
    /// anything about the step, so neither is counted. Any other refusal
    /// still ends the call after `BRIDGE_STEP_FAILURES` in a row. The pause
    /// between two requests grows as `repeat_pause` describes.
    fn complete_bridge_step(
        &self,
        members: &RemoteMembers,
        decision: &dyn NativeJointBridgeDecision,
        recipient: &ClusterNodeId,
        operation: &NativeJointHandoffStepOperationV1,
        phase: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        Self::complete_bridge_step_over(
            &self.transport,
            members,
            decision,
            recipient,
            operation,
            phase,
        )
    }

    /// `complete_bridge_step` over a transport alone; see `bridge_step_over`.
    fn complete_bridge_step_over(
        transport: &TcpClusterRelocationTransport,
        members: &RemoteMembers,
        decision: &dyn NativeJointBridgeDecision,
        recipient: &ClusterNodeId,
        operation: &NativeJointHandoffStepOperationV1,
        phase: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        let mut failures = 0;
        let mut repeated = 0u32;
        let mut last = String::from("no answer");
        loop {
            if phase.check().is_err() {
                break;
            }
            match Self::bridge_step_over(transport, members, decision, recipient, operation, phase)
            {
                Ok(frame) => {
                    failures = 0;
                    if frame.outcome().complete {
                        return Ok(frame);
                    }
                    last = "the member answered that the step is not complete".into();
                }
                Err(error) => {
                    if phase.check().is_err() {
                        break;
                    }
                    if !member_busy(&error) {
                        failures += 1;
                        if failures >= BRIDGE_STEP_FAILURES {
                            return Err(error);
                        }
                    }
                    last = error.to_string();
                }
            }
            repeated = repeated.saturating_add(1);
            let pause = repeat_pause(repeated);
            if !pause.is_zero() {
                std::thread::sleep(pause);
            }
        }
        // The phase budget ended, or the step of this controller was
        // cancelled: both are reported as what was last answered.
        Err(remote_error(format!(
            "member {recipient} did not complete its {} step within its phase budget after {repeated} requests; last answer: {last}",
            operation.trace_name()
        )))
    }

    /// One step of one phase on several members at once: every member's
    /// `complete_bridge_step` runs on its own scoped thread, and all of them
    /// have returned before this function does. The answers keep the order of
    /// `nodes`; what is returned is each member's completed outcome.
    ///
    /// What makes the members of one call independent. A member's step is
    /// the member's own work under its own fresh role and metadata; what it
    /// needs from the other members it reads itself, and it needs only what
    /// the PREVIOUS phase left on them (their preparation for a publication,
    /// their confirmed bridge for an Open, their confirmed Open for the
    /// custody retirement and the release, their confirmed release for the
    /// writer policy). The callers therefore keep every phase boundary: a
    /// phase starts only when every member completed the one before. Inside
    /// a phase nothing is skipped or merged; each member runs exactly the
    /// request sequence it ran before, with both of its observation windows.
    ///
    /// What concurrency can cost, and never more than time. A member that is
    /// asked for an observation while its own step holds its state answers
    /// "busy", and a member's second window refuses when another member's
    /// reported state changed between its two windows. Both leave the step
    /// incomplete (fail closed) and `complete_bridge_step` repeats it, as it
    /// always did. A member whose repetition ended with an error while its
    /// neighbours were still running is asked once more alone, after all of
    /// them returned, by the unchanged sequential rule; only that outcome is
    /// an error of the phase. So a refusal that only the concurrent
    /// neighbours caused cannot fail a move that the sequential order would
    /// have completed.
    ///
    /// `counted` is called with each completed frame on the thread that
    /// received it, at once, so that a frame is counted inside its own
    /// freshness window.
    ///
    /// The transport is shared by reference. It is `Sync` (the scoped spawn
    /// would not compile otherwise), it opens one connection per request and
    /// keeps no per-request state beyond an atomic request counter, and the
    /// write coordinator already calls the same transport type from scoped
    /// threads for its voter exchanges.
    ///
    /// `at_once` bounds how many members are asked together; the members
    /// are taken in the order of `nodes`, and a group starts when the group
    /// before it completed. See `WINDOWED_STEPS_AT_ONCE`.
    #[allow(clippy::too_many_arguments)]
    fn complete_bridge_steps(
        &self,
        members: &RemoteMembers,
        decision: &RemoteBridgeDecision,
        nodes: &[ClusterNodeId],
        at_once: usize,
        operation: &NativeJointHandoffStepOperationV1,
        phase: &CancellationToken,
        what: &str,
        counted: &(dyn Fn(AuthenticatedNativeJointBridgeStepFrame) -> Result<()> + Sync),
    ) -> Result<Vec<(ClusterNodeId, NativeJointHandoffStepOutcomeV1)>> {
        // Every step but the preparation reads the other members while they
        // serve their own step of the same phase.
        let windowed = !matches!(
            operation,
            NativeJointHandoffStepOperationV1::BridgePrepare { .. }
        );
        let mut outcomes = Vec::with_capacity(nodes.len());
        let mut start = 0;
        while start < nodes.len() {
            // Re-read per group: an earlier group of this very phase may
            // have shown that members cannot overlap here.
            let at_once = if windowed
                && self
                    .windowed_alone
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                1
            } else {
                at_once.max(1)
            };
            let group = &nodes[start..(start + at_once).min(nodes.len())];
            start += group.len();
            let (group_outcomes, repeated) = self.complete_bridge_step_group(
                members, decision, group, operation, phase, what, counted,
            )?;
            if windowed && repeated > 0 {
                self.windowed_alone
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            outcomes.extend(group_outcomes);
        }
        Ok(outcomes)
    }

    /// One group of `complete_bridge_steps`: all of `nodes` at once.
    #[allow(clippy::too_many_arguments)]
    fn complete_bridge_step_group(
        &self,
        members: &RemoteMembers,
        decision: &RemoteBridgeDecision,
        nodes: &[ClusterNodeId],
        operation: &NativeJointHandoffStepOperationV1,
        phase: &CancellationToken,
        what: &str,
        counted: &(dyn Fn(AuthenticatedNativeJointBridgeStepFrame) -> Result<()> + Sync),
    ) -> Result<(Vec<(ClusterNodeId, NativeJointHandoffStepOutcomeV1)>, usize)> {
        let started = Instant::now();
        let transport: &TcpClusterRelocationTransport = &self.transport;
        let step = |node: &ClusterNodeId| -> Result<NativeJointHandoffStepOutcomeV1> {
            let frame = Self::complete_bridge_step_over(
                transport, members, decision, node, operation, phase,
            )?;
            let outcome = frame.outcome().clone();
            counted(frame)?;
            Ok(outcome)
        };
        let answers = member_fan_out(nodes, &step);
        let mut outcomes = Vec::with_capacity(nodes.len());
        let mut repeated = 0;
        for (node, answer) in answers {
            let outcome = match answer {
                Ok(outcome) => outcome,
                Err(first) => {
                    if nodes.len() < 2 || phase.check().is_err() {
                        return Err(stage_error(&format!("{what} of {node}"), first));
                    }
                    // Alone now: every other member of the phase returned.
                    repeated += 1;
                    trace(started, || {
                        format!(
                            "{what} of {node} failed beside its neighbours and is repeated alone: {first}"
                        )
                    });
                    step(&node).map_err(|error| stage_error(&format!("{what} of {node}"), error))?
                }
            };
            outcomes.push((node, outcome));
        }
        trace(started, || {
            format!(
                "phase {what}: {} members at once, {repeated} repeated alone",
                nodes.len()
            )
        });
        Ok((outcomes, repeated))
    }

    /// Preparation, publication and Open on every new member of `decision`,
    /// then the custody retirement, the release and the writer policy.
    fn activate_new_members(
        &mut self,
        decision: RemoteBridgeDecision,
        members: &RemoteMembers,
        parent: &CancellationToken,
    ) -> Result<()> {
        let coordinator = self.coordinator();
        let new = decision.new_members().keys().cloned().collect::<Vec<_>>();
        let resolved = members.new_members.iter().collect::<BTreeSet<_>>();
        if new.iter().collect::<BTreeSet<_>>() != resolved || !new.contains(&self.target) {
            return Err(remote_error(
                "the new members of the committed decision differ from the committed final membership",
            ));
        }
        if new.contains(&coordinator) {
            return Err(remote_error(format!(
                "coordinator {coordinator} is a new member of the moved range; its own bridge steps would have to run locally, which is not implemented. A handoff that retires the coordinator's replica is the supported case"
            )));
        }
        // Open and release: the target first, then the surviving members in
        // node order. Preparation, publication and custody retirement: node
        // order.
        let mut order = vec![self.target.clone()];
        order.extend(new.iter().filter(|node| **node != self.target).cloned());
        let decision_sha256 = decision.checksum_sha256().to_owned();

        // What the members already hold decides what is sent.
        // Read-only, one request per member, three at a time.
        let phase = bounded(parent, BRIDGE_PHASE);
        let mut observed = Vec::new();
        {
            let transport: &TcpClusterRelocationTransport = &self.transport;
            let observe = |node: &ClusterNodeId| -> Result<Option<bool>> {
                Self::observe_member_release(
                    transport,
                    members,
                    &decision,
                    node,
                    &decision_sha256,
                    &phase,
                )
            };
            for group in new.chunks(LIGHT_STEPS_AT_ONCE) {
                for (node, answer) in member_fan_out(group, &observe) {
                    let answer = answer.map_err(|error| {
                        stage_error(&format!("release observation of {node}"), error)
                    })?;
                    observed.push((node, answer));
                }
            }
        }
        self.bridge_observed = observed.clone();
        let every_open = observed.iter().all(|(_, answer)| answer.is_some());
        if !every_open {
            if let Some((released, _)) = observed.iter().find(|(_, answer)| *answer == Some(true)) {
                let (behind, _) = observed
                    .iter()
                    .find(|(_, answer)| answer.is_none())
                    .expect("a member without a confirmed Open exists");
                // A member releases only after it observed every other
                // member durably open, so this member lost what it retained.
                return Err(remote_error(format!(
                    "new member {behind} reports no confirmed Open although {released} is already released; {behind} no longer retains its bridge state, and no bridge step is sent again"
                )));
            }
            self.prepare_publish_open(&decision, members, &new, &order, &decision_sha256, parent)?;
            #[cfg(test)]
            self.test_stop(RemoteTestStop::Open)?;
        }
        let released = |node: &ClusterNodeId| {
            every_open
                && observed
                    .iter()
                    .any(|(member, answer)| member == node && *answer == Some(true))
        };

        let operation = NativeJointHandoffStepOperationV1::RetireOldCustody {
            decision_sha256: decision_sha256.clone(),
        };
        let phase = bounded(parent, BRIDGE_PHASE);
        // Every member proves every new member durably open itself and
        // retires only its own custody: three at a time.
        let asked = new
            .iter()
            .filter(|node| !released(node))
            .cloned()
            .collect::<Vec<_>>();
        self.complete_bridge_steps(
            members,
            &decision,
            &asked,
            LIGHT_STEPS_AT_ONCE,
            &operation,
            &phase,
            "OLD custody retirement",
            &|_| Ok(()),
        )?;
        self.bridge_retired = asked;

        // A released surviving member answers the repeated request from
        // what it retained, re-read under its own lock. A target that is
        // observed released is not asked: once it holds its writer policy
        // its host is a DATA owner and refuses a release step.
        let target_released = released(&self.target);
        let operation = NativeJointHandoffStepOperationV1::BridgeRelease { decision_sha256 };
        let phase = bounded(parent, BRIDGE_PHASE);
        let mut outcomes = Vec::new();
        // The target first and alone, as before; then the surviving members
        // two at a time.
        for group in target_then_survivors(&order, &self.target) {
            let asked = group
                .iter()
                .filter(|node| !(**node == self.target && target_released))
                .cloned()
                .collect::<Vec<_>>();
            for (node, outcome) in self.complete_bridge_steps(
                members,
                &decision,
                &asked,
                WINDOWED_STEPS_AT_ONCE,
                &operation,
                &phase,
                "bridge release",
                &|_| Ok(()),
            )? {
                if outcome.released != Some(true) {
                    return Err(remote_error(format!(
                        "new member {node} completed its release step without reporting itself released"
                    )));
                }
                outcomes.push((node, outcome));
            }
        }
        self.bridge_released = outcomes;
        #[cfg(test)]
        self.test_stop(RemoteTestStop::Release)?;

        self.publish_writer_policies(&decision, members, &new, target_released, parent)?;
        #[cfg(test)]
        self.test_stop(RemoteTestStop::WriterPolicy)?;
        self.activated = Some((decision, members.clone()));
        Ok(())
    }

    /// The writer policy of every new member, after every new member is
    /// released: the surviving members in node order, then the target.
    ///
    /// A surviving member performs it inside its step. The target records
    /// the command for its own host loop; see `complete_host_command`.
    ///
    /// `target_release_skipped`: the target was observed released and was
    /// not sent its release. Such a target may hold a durable release that
    /// its second window never confirmed (the coordinator that sent the
    /// release stopped before its repetition). It then refuses its writer
    /// policy with "no confirmed release", which also proves that it is not
    /// a DATA owner yet: its release step is sent once, to confirm, and the
    /// writer policy again.
    fn publish_writer_policies(
        &mut self,
        decision: &RemoteBridgeDecision,
        members: &RemoteMembers,
        new: &[ClusterNodeId],
        target_release_skipped: bool,
        parent: &CancellationToken,
    ) -> Result<()> {
        let decision_sha256 = decision.checksum_sha256().to_owned();
        let operation = NativeJointHandoffStepOperationV1::BridgeWriterPolicy {
            decision_sha256: decision_sha256.clone(),
        };
        let phase = bounded(parent, BRIDGE_PHASE);
        let mut outcomes = Vec::new();
        // The surviving members two at a time: each reads every other
        // member's audited release, which the release phase left, and
        // installs only its own policy. The target follows when all of them completed: its
        // host replaces its DATA owner and rebinds its listener for the
        // command, and a surviving member that asked it meanwhile would be
        // refused.
        let survivors = new
            .iter()
            .filter(|node| **node != self.target)
            .cloned()
            .collect::<Vec<_>>();
        for (node, outcome) in self.complete_bridge_steps(
            members,
            decision,
            &survivors,
            WINDOWED_STEPS_AT_ONCE,
            &operation,
            &phase,
            "writer policy",
            &|_| Ok(()),
        )? {
            if outcome.writer_policy_installed != Some(true) {
                return Err(remote_error(format!(
                    "new member {node} completed its writer policy step without reporting the policy installed"
                )));
            }
            outcomes.push((node, outcome));
        }
        let target = self.target.clone();
        let mut answer =
            self.complete_host_command(members, decision, &operation, "writer policy", parent)?;
        if matches!(answer, HostCommandAnswer::ReleaseUnconfirmed) && target_release_skipped {
            let release = NativeJointHandoffStepOperationV1::BridgeRelease { decision_sha256 };
            let phase = bounded(parent, BRIDGE_PHASE);
            let frame = self
                .complete_bridge_step(members, decision, &target, &release, &phase)
                .map_err(|error| stage_error(&format!("bridge release of {target}"), error))?;
            if frame.outcome().released != Some(true) {
                return Err(remote_error(format!(
                    "new member {target} completed its release step without reporting itself released"
                )));
            }
            self.bridge_released
                .insert(0, (target.clone(), frame.outcome().clone()));
            answer =
                self.complete_host_command(members, decision, &operation, "writer policy", parent)?;
        }
        let HostCommandAnswer::Complete(outcome) = answer else {
            return Err(remote_error(format!(
                "target {target} holds no confirmed release at its writer policy step"
            )));
        };
        if outcome.writer_policy_installed != Some(true) {
            return Err(remote_error(format!(
                "target {target} completed its writer policy step without reporting the policy installed"
            )));
        }
        outcomes.push((target, outcome));
        self.bridge_policies = outcomes;
        Ok(())
    }

    /// Repeat one host command step of the target (`BridgeWriterPolicy` or
    /// `TargetServe`) until the target's own host loop retained its outcome.
    ///
    /// The target's step receiver only records the command; its host loop
    /// performs it and the next step reads the retained outcome. A failed
    /// attempt of that loop is retained too: the next step reports its text
    /// and arms the same command again. A command that already completed
    /// answers with its retained outcome, so a new coordinator object
    /// repeats nothing.
    ///
    /// The repetition is bounded by `HOST_COMMAND_WAIT`, and the command is
    /// asked at least once. When the bound ends while
    /// the command is still pending the call fails and `waiting()` reports
    /// what is awaited, with the text of the host's last failed attempt when
    /// it reported one; the next call continues the same command. A request
    /// that fails has an unknown effect and is repeated; the target rebinds
    /// its listener while it runs a command.
    fn complete_host_command(
        &mut self,
        members: &RemoteMembers,
        decision: &RemoteBridgeDecision,
        operation: &NativeJointHandoffStepOperationV1,
        what: &str,
        parent: &CancellationToken,
    ) -> Result<HostCommandAnswer> {
        let target = self.target.clone();
        let until = Instant::now() + self.host_command_wait;
        let mut failures = 0;
        let mut reported = None;
        // The last request failed: its effect is unknown, nothing is pending
        // as far as this call knows.
        let mut unanswered = None;
        loop {
            parent.check()?;
            match self.bridge_step(members, decision, &target, operation, parent) {
                Ok(frame) => {
                    failures = 0;
                    unanswered = None;
                    let outcome = frame.outcome();
                    if outcome.complete {
                        return Ok(HostCommandAnswer::Complete(outcome.clone()));
                    }
                    if outcome.stage
                        != crate::distribution_range_handoff::NativeJointHandoffStepStageV1::BridgeHostCommand
                    {
                        return Err(remote_error(format!(
                            "target {target} answered its {what} step with the stage {:?} of another step",
                            outcome.stage
                        )));
                    }
                    if let Some(text) = &outcome.host_command_error {
                        reported = Some(text.clone());
                    }
                }
                Err(error) => {
                    parent.check()?;
                    if error.to_string().contains(RELEASE_UNCONFIRMED) {
                        return Ok(HostCommandAnswer::ReleaseUnconfirmed);
                    }
                    failures += 1;
                    if failures >= HOST_COMMAND_FAILURES {
                        return Err(stage_error(&format!("{what} of {target}"), error));
                    }
                    unanswered = Some(error);
                }
            }
            if Instant::now() >= until {
                break;
            }
            std::thread::sleep(HOST_COMMAND_POLL);
        }
        // Only a command the target itself reported pending is a wait.
        if let Some(error) = unanswered {
            return Err(stage_error(&format!("{what} of {target}"), error));
        }
        let note = format!(
            "waiting for the host of target {target} to run its {what} command{}",
            match reported {
                Some(text) => format!("; its last attempt failed and is repeated: {text}"),
                None => String::new(),
            }
        );
        self.waiting = Some(note.clone());
        Err(remote_error(note))
    }

    /// One member's read-only RELEASE observation: `None` while the member
    /// holds no confirmed catalogue activation, otherwise whether it is
    /// released. Any other refusal is repeated a bounded number of times
    /// and then returned: an unknown answer never counts as "not open".
    fn observe_member_release(
        transport: &TcpClusterRelocationTransport,
        members: &RemoteMembers,
        decision: &RemoteBridgeDecision,
        node: &ClusterNodeId,
        decision_sha256: &str,
        phase: &CancellationToken,
    ) -> Result<Option<bool>> {
        let operation = NativeJointHandoffStepOperationV1::BridgePhaseObserve {
            decision_sha256: decision_sha256.to_owned(),
            kind: NativeJointBridgePhaseKindV1::Release,
        };
        let mut failures = 0;
        loop {
            phase.check()?;
            match Self::bridge_step_over(transport, members, decision, node, &operation, phase) {
                Ok(frame) => return Ok(Some(frame.outcome().released == Some(true))),
                Err(error) if error.to_string().contains(RELEASE_NOT_OPEN) => return Ok(None),
                Err(error) => {
                    phase.check()?;
                    // A busy member is asked again until the phase ends.
                    if !member_busy(&error) {
                        failures += 1;
                        if failures >= BRIDGE_STEP_FAILURES {
                            return Err(error);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    /// Preparation, publication and Open on every new member.
    fn prepare_publish_open(
        &mut self,
        decision: &RemoteBridgeDecision,
        members: &RemoteMembers,
        new: &[ClusterNodeId],
        order: &[ClusterNodeId],
        decision_sha256: &str,
        parent: &CancellationToken,
    ) -> Result<()> {
        let coordinator = self.coordinator();
        let operation = NativeJointHandoffStepOperationV1::BridgePrepare {
            decision_sha256: decision_sha256.to_owned(),
        };
        let phase = bounded(parent, BRIDGE_PHASE);
        // A preparation is the member's own local work: three at a time.
        self.complete_bridge_steps(
            members,
            decision,
            new,
            LIGHT_STEPS_AT_ONCE,
            &operation,
            &phase,
            "bridge preparation",
            &|_| Ok(()),
        )?;

        let operation = NativeJointHandoffStepOperationV1::BridgePublish {
            decision_sha256: decision_sha256.to_owned(),
        };
        let phase = bounded(parent, BRIDGE_PHASE);
        let round = Mutex::new(NativeRemoteBridgePublicationCertificateRound::begin(
            decision.clone(),
            coordinator,
            &phase,
        )?);
        // Every member reads every other member's preparation, which the
        // phase before left and a publication does not change: two at a
        // time, in node order. Each frame is counted inside its own
        // freshness window, on the thread that received it.
        self.complete_bridge_steps(
            members,
            decision,
            new,
            WINDOWED_STEPS_AT_ONCE,
            &operation,
            &phase,
            "bridge publication",
            &|frame| round.lock().record(frame),
        )?;
        self.bridge_publication = Some(
            round
                .into_inner()
                .finish()
                .map_err(|error| stage_error("bridge publication certificate", error))?,
        );

        let operation = NativeJointHandoffStepOperationV1::BridgeOpen {
            decision_sha256: decision_sha256.to_owned(),
        };
        let phase = bounded(parent, BRIDGE_PHASE);
        let mut opened = Vec::new();
        // The target first and alone, as before; then the surviving members
        // two at a time: each reads every other member's confirmed bridge,
        // which the publication phase left and an Open does not change.
        for group in target_then_survivors(order, &self.target) {
            opened.extend(self.complete_bridge_steps(
                members,
                decision,
                &group,
                WINDOWED_STEPS_AT_ONCE,
                &operation,
                &phase,
                "bridge open",
                &|_| Ok(()),
            )?);
        }
        self.bridge_opened = opened;
        Ok(())
    }

    /// The decision body whose checksum is the committed digest `digest`.
    ///
    /// The certified record of the committed metadata carries the decision's
    /// scope only. The body also holds every old member's closed facts, the
    /// observed irrevocable bounds, every new member's materialized facts,
    /// the selected source and the decided point; none of them is in the
    /// committed record, so the body cannot be rebuilt from it. It is taken
    /// from the retained certificate, or, for a coordinator without memory,
    /// read from this node's own protected native decision record (this
    /// node published the decision as an old member), which its own service
    /// returns only when the record's body checksum is the digest of this
    /// node's own committed metadata read. Otherwise it is refused.
    fn committed_decision_body(
        &mut self,
        members: &RemoteMembers,
        range: RangeId,
        relocation: RelocationId,
        digest: &str,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionData> {
        if let Some(body) = self
            .decision_body
            .as_ref()
            .filter(|body| body.checksum_sha256() == digest)
        {
            return Ok(body.clone());
        }
        self.decision_body = None;
        let body = match self
            .certificate
            .as_ref()
            .map(|certificate| certificate.data())
            .filter(|data| data.checksum_sha256() == digest)
        {
            Some(data) => data.clone(),
            None => {
                let local = self.local.clone().ok_or_else(|| {
                    remote_error(
                        "the decision body is needed and is held neither as a retained decision certificate nor readable from this node's own native decision record (no own data service was supplied); the committed record carries the decision's scope only",
                    )
                })?;
                let body = retry_timed_out(|| {
                    let token = bounded(parent, REQUEST);
                    let business = Uuid::now_v7();
                    let own = self.transport.acquire_host_metadata_observation(
                        &members.metadata_leader,
                        business,
                        REQUEST,
                        &token,
                    )?;
                    ClusterDataNodeService::committed_native_joint_decision_body(
                        &local, &own, business, relocation, range, &token,
                    )
                })
                .map_err(|error| stage_error("committed decision body", error))?;
                if body.checksum_sha256() != digest {
                    return Err(remote_error(
                        "this node's own native decision record is not the committed decision",
                    ));
                }
                body
            }
        };
        self.decision_body = Some(body.clone());
        Ok(body)
    }
}

#[cfg(test)]
mod member_fan_out_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn nodes(count: usize) -> Vec<ClusterNodeId> {
        (1..=count)
            .map(|i| ClusterNodeId::new(format!("n{i}")).unwrap())
            .collect()
    }

    /// Every member's exchange is in flight at the same time, each answer
    /// stays with its member in the order asked, and one member's error is
    /// that member's alone.
    #[test]
    fn native_remote_backend_member_fan_out_runs_members_at_once_and_keeps_their_order() {
        let nodes = nodes(4);
        let inside = AtomicUsize::new(0);
        let together = AtomicUsize::new(0);
        let exchange = |node: &ClusterNodeId| -> Result<String> {
            let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
            together.fetch_max(now, Ordering::SeqCst);
            // Stay until every member is inside, bounded.
            let until = Instant::now() + Duration::from_secs(10);
            while together.load(Ordering::SeqCst) < 4 && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(1));
            }
            inside.fetch_sub(1, Ordering::SeqCst);
            if node.as_str() == "n3" {
                return Err(remote_error("n3 refused"));
            }
            Ok(format!("answer of {node}"))
        };
        let answers = member_fan_out(&nodes, &exchange);
        assert_eq!(together.load(Ordering::SeqCst), 4, "all four at once");
        assert_eq!(
            answers
                .iter()
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>(),
            nodes
        );
        for (node, answer) in &answers {
            match node.as_str() {
                "n3" => assert!(answer
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("n3 refused")),
                _ => assert_eq!(answer.as_ref().unwrap(), &format!("answer of {node}")),
            }
        }
        // One member runs on the calling thread; none is asked for none.
        let caller = std::thread::current().id();
        let single = member_fan_out(&nodes[..1], &|_: &ClusterNodeId| {
            Ok(std::thread::current().id())
        });
        assert_eq!(single.len(), 1);
        assert_eq!(*single[0].1.as_ref().unwrap(), caller);
        assert!(member_fan_out(&[], &|_: &ClusterNodeId| Ok(())).is_empty());
    }

    /// An Open and a release keep the target first and alone.
    #[test]
    fn native_remote_backend_target_goes_first_and_alone_then_the_survivors_together() {
        let all = nodes(4);
        let target = all[3].clone();
        let order = vec![
            all[3].clone(),
            all[0].clone(),
            all[1].clone(),
            all[2].clone(),
        ];
        assert_eq!(
            target_then_survivors(&order, &target),
            vec![vec![all[3].clone()], all[..3].to_vec()]
        );
        // A set without the target (it was observed released) is one group.
        assert_eq!(
            target_then_survivors(&all[..3], &target),
            vec![all[..3].to_vec()]
        );
        assert_eq!(
            target_then_survivors(&order[..1], &target),
            vec![vec![target.clone()]]
        );
    }
}
