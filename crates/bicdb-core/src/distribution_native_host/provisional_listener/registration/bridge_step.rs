//! Member-local bridge steps after the joint decision is durable and the
//! membership is final. The authenticated peer supplies identity DATA only.
//! THIS member acquires its own fresh host metadata, compares the request
//! with the decision IT observes committed, and performs or re-proves only
//! its own local state. A reply is historical checksummed DATA: never a
//! quorum, phase, lease, publication or serving grant.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_handoff::{
    NativeJointBridgePhaseKindV1, NativeJointHandoffStepOutcomeV1, NativeJointHandoffStepReplyV1,
    NativeJointHandoffStepRequestV1, NativeJointHandoffStepStageV1 as Stage,
    NativeJointPreparationCustody, NativeJointPreparationPhaseFacts,
    NativeJointPreparationRetainedFacts,
};
use std::time::Duration;

/// A further sub-step starts only with this much of the request left. The
/// first sub-step of a call always runs so every call can make progress.
pub(super) const MIN_BRIDGE_SUBSTEP_BUDGET: Duration = Duration::from_millis(1500);

/// One member's retained bridge custody for one range. Created by the first
/// authorised `BridgePrepare` and kept by the member's own host: the later
/// publish/open/release steps of the same process continue from here.
pub(super) struct NativeBridgeMemberStep {
    binding_sha256: String,
    decision_sha256: String,
    /// Member-owned preparation session. It outlives every finite request and
    /// ends with this retained entry; no request can renew or replace it.
    session: CancellationToken,
    /// A survivor's real lease after it left the CLOSED writer transition,
    /// retained here BEFORE the fallible native preparation can consume it.
    pub(super) lease: Option<NativeJointServingLease>,
    custody: Option<Arc<NativeJointPreparationCustody>>,
}
impl Drop for NativeBridgeMemberStep {
    fn drop(&mut self) {
        // Custody cannot outlive the member host that retained it.
        self.session.cancel();
    }
}
impl NativeBridgeMemberStep {
    pub(super) fn session(&self) -> &CancellationToken {
        &self.session
    }
    pub(super) fn custody(&self) -> Option<&Arc<NativeJointPreparationCustody>> {
        self.custody.as_ref()
    }
    /// Infallible custody transfer immediately after the genuine producer.
    pub(super) fn retain_custody(&mut self, custody: Arc<NativeJointPreparationCustody>) {
        self.custody = Some(custody);
        // The native preparation now owns the same real lease.
        self.lease = None;
    }
    pub(super) fn prepare_outcome(&self, stage: Stage) -> NativeJointHandoffStepOutcomeV1 {
        match &self.custody {
            Some(custody) => {
                prepared_outcome(self.binding_sha256.clone(), custody.retained_facts(), None)
            }
            None => NativeJointHandoffStepOutcomeV1::bridge_progress(
                self.binding_sha256.clone(),
                stage,
                self.decision_sha256.clone(),
            ),
        }
    }
}
#[derive(Default)]
pub(super) struct NativeBridgeMemberSteps {
    steps: Mutex<BTreeMap<crate::RangeId, NativeBridgeMemberStep>>,
}
impl NativeBridgeMemberSteps {
    pub(super) fn hold(
        &self,
    ) -> Result<parking_lot::MutexGuard<'_, BTreeMap<crate::RangeId, NativeBridgeMemberStep>>> {
        self.steps
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step member custody busy"))
    }
}

/// The retained entry for a `BridgePrepare`, created only after the request
/// was authorised against this member's own committed metadata.
pub(super) fn retained_prepare_step<'a>(
    steps: &'a mut BTreeMap<crate::RangeId, NativeBridgeMemberStep>,
    request: &NativeJointHandoffStepRequestV1,
) -> Result<&'a mut NativeBridgeMemberStep> {
    let binding_sha256 = request.binding()?;
    let decision_sha256 = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    if let Some(retained) = steps.get(&request.range_id) {
        if retained.binding_sha256 != binding_sha256 || retained.decision_sha256 != decision_sha256
        {
            return Err(invalid(
                "joint bridge step retains another exact decision preparation",
            ));
        }
    } else {
        if steps.len() >= 8 {
            return Err(invalid(
                "joint bridge step member custody capacity; nothing is evicted",
            ));
        }
        steps.insert(
            request.range_id,
            NativeBridgeMemberStep {
                binding_sha256,
                decision_sha256,
                session: CancellationToken::new(Arc::new(AtomicBool::new(false)), None),
                lease: None,
                custody: None,
            },
        );
    }
    Ok(steps.get_mut(&request.range_id).unwrap())
}

fn prepared_outcome(
    binding_sha256: String,
    facts: NativeJointPreparationRetainedFacts,
    phase: Option<(NativeJointBridgePhaseKindV1, u64)>,
) -> NativeJointHandoffStepOutcomeV1 {
    let mut outcome = NativeJointHandoffStepOutcomeV1::bridge_progress(
        binding_sha256,
        Stage::Complete,
        facts.decision_sha256,
    );
    outcome.preparation_sha256 = Some(facts.preparation_sha256);
    outcome.native_database_incarnation = Some(facts.native_database_incarnation);
    outcome.local_visibility_sequence = Some(facts.local_visibility_sequence);
    outcome.decision_receipt_sha256 = Some(facts.decision_receipt_sha256);
    outcome.store_open_id = Some(facts.store_open_id);
    if let Some((kind, remaining_ms)) = phase {
        outcome.phase_kind = Some(kind);
        outcome.window_remaining_ms = Some(remaining_ms);
    }
    outcome
}

pub(super) fn bridge_remaining(token: &CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .filter(|left| !left.is_zero())
        .ok_or(BicDbError::QueryTimedOut)
}

/// This member's own fresh committed metadata for one bounded sub-step.
/// Never retained across sub-steps and never derived from request DATA.
pub(super) struct BridgeStepOwn {
    pub(super) own: HostMetadataReadObservation,
    pub(super) business: Uuid,
    pub(super) anchor: (u64, u64, Uuid),
    pub(super) birth: crate::distribution::NativeStoreBirthV1,
    /// The authenticated peer is the range leader or the metadata leader.
    pub(super) coordinator: bool,
}

/// Authenticate the peer and bind the request to the decision committed in
/// THIS member's own fresh metadata, before any retained state is created,
/// read or disclosed.
///
/// A mutation is accepted only from the coordinator: the same initiator
/// predicate every other handoff mutation uses (the range leader or the
/// metadata leader), evaluated against this member's own observation. A
/// read-only observation is also accepted from another data voter of the
/// final membership; the caller re-checks that peer against the NEW member
/// set of the committed decision it observes natively.
pub(super) fn acquire_bridge_step_own(
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    peer: &MetadataReadParticipant,
    request: &NativeJointHandoffStepRequestV1,
    token: &CancellationToken,
) -> Result<BridgeStepOwn> {
    token.check()?;
    request.require_shape()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?;
    let local = transport.local_metadata_read_participant();
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    let business = Uuid::now_v7();
    let own = transport.acquire_host_metadata_observation(
        &leader,
        business,
        bridge_remaining(token)?.min(Duration::from_secs(5)),
        token,
    )?;
    let topology = own.committed_topology(business)?;
    let actor = topology
        .nodes
        .get(peer.node_id())
        .ok_or_else(|| invalid("joint bridge step authenticated actor absent"))?;
    if peer.plaintext_loopback()
        || peer.node_id() == local.node_id()
        || actor.lifecycle != ClusterNodeLifecycle::Active
        || actor.incarnation != peer.member_incarnation()
        || peer
            .certificate_sha256()
            .is_none_or(|pin| !actor.accepts_tls_certificate_sha256(pin))
        || own.requester_node_id() != local.node_id()
        || own.requester_incarnation() != local.member_incarnation()
    {
        return Err(invalid(
            "joint bridge step actual peer/credential differs from committed member",
        ));
    }
    let relocation = topology
        .relocations
        .get(&request.relocation_id)
        .ok_or_else(|| invalid("joint bridge step relocation absent from own metadata"))?;
    let record = relocation
        .native_joint_handoff_v1
        .as_ref()
        .ok_or_else(|| invalid("joint bridge step decision is not committed in own metadata"))?;
    let range = topology
        .range_by_id(request.range_id)
        .ok_or_else(|| invalid("joint bridge step range absent from own metadata"))?;
    let voter = |node: &ClusterNodeId| {
        range.replicas.iter().any(|replica| {
            &replica.node_id == node && replica.role == crate::distribution::RangeReplicaRole::Voter
        })
    };
    if relocation.range_id != request.range_id
        || relocation.plan_id != request.plan_id
        || relocation.learner_epoch != request.range_epoch
        || relocation.handoff_session.as_ref().is_none_or(|session| {
            session.session_id != request.session_id || session.nonce != request.nonce
        })
        || record.stage() < crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
        || record.decision_sha256() != decision
        || record.session_id() != request.session_id
        || record.nonce() != request.nonce
        || !voter(local.node_id())
    {
        return Err(invalid(
            "joint bridge step differs from the decision committed in own metadata",
        ));
    }
    let coordinator = peer.node_id() == &leader || &range.leader == peer.node_id();
    if !(coordinator || (request.operation.read_only() && voter(peer.node_id()))) {
        return Err(invalid(
            "joint bridge step authenticated peer is not an admitted initiator",
        ));
    }
    let birth = topology
        .nodes
        .get(local.node_id())
        .and_then(|node| node.native_store_birth_v1.clone())
        .ok_or_else(|| invalid("joint bridge step local native birth absent"))?;
    let anchor = (own.term(), own.read_index(), own.context_id());
    token.check()?;
    Ok(BridgeStepOwn {
        own,
        business,
        anchor,
        birth,
        coordinator,
    })
}

/// Read-only PREPARATION phase observation of this member's retained custody.
/// A member that retained nothing has nothing to attest and refuses.
pub(super) fn observe_bridge_phase(
    steps: &NativeBridgeMemberSteps,
    peer: &MetadataReadParticipant,
    request: &NativeJointHandoffStepRequestV1,
    kind: NativeJointBridgePhaseKindV1,
    fresh: BridgeStepOwn,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    if kind != NativeJointBridgePhaseKindV1::Preparation {
        // The bridge and activation receipts these kinds attest are retained
        // by the publish/open steps; nothing here may stand in for them.
        return Err(invalid(
            "joint bridge phase observation of this kind has no retained member state yet",
        ));
    }
    let custody = {
        let held = steps.hold()?;
        held.get(&request.range_id)
            .and_then(|step| step.custody().cloned())
            .ok_or_else(|| invalid("joint bridge phase member retains no preparation"))?
    };
    // The NEW member set comes from the decision this member itself holds
    // natively, never from the request or from the peer.
    request.require_decision(custody.data())?;
    if custody
        .data()
        .new_members()
        .get(peer.node_id())
        .is_none_or(|member| member.incarnation != peer.member_incarnation())
        && !fresh.coordinator
    {
        return Err(invalid(
            "joint bridge phase observer is outside the committed NEW member set",
        ));
    }
    let BridgeStepOwn { own, business, .. } = fresh;
    let facts: NativeJointPreparationPhaseFacts =
        custody.observe_fresh_preparation_phase(own, business, token)?;
    let remaining = facts
        .deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or(BicDbError::QueryTimedOut)?;
    let anchor = (
        facts.metadata_term,
        facts.metadata_read_index,
        facts.metadata_context_id,
    );
    Ok((
        prepared_outcome(
            request.binding()?,
            facts.retained,
            Some((kind, remaining.as_millis().clamp(1, 5000) as u64)),
        ),
        anchor,
    ))
}

pub(super) fn bridge_reply(
    request: &NativeJointHandoffStepRequestV1,
    node_id: ClusterNodeId,
    birth: crate::distribution::NativeStoreBirthV1,
    outcome: NativeJointHandoffStepOutcomeV1,
    anchor: (u64, u64, Uuid),
    token: &CancellationToken,
) -> Result<NativeJointHandoffStepReplyV1> {
    let mut reply = NativeJointHandoffStepReplyV1 {
        request: request.clone(),
        node_id,
        birth,
        outcome,
        metadata_term: anchor.0,
        metadata_read_index: anchor.1,
        metadata_context_id: anchor.2,
        checksum_sha256: String::new(),
    };
    reply.checksum_sha256 = reply.checksum()?;
    reply.require_request(request)?;
    token.check()?;
    Ok(reply)
}
