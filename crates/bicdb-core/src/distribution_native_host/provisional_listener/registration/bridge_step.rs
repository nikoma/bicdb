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
    /// Set BEFORE this member's first native publication attempt and cleared
    /// only when a second observation window confirmed it. While set, the
    /// durable effect of an earlier attempt is unknown or unconfirmed.
    publication_pending: bool,
    /// One publication attempt of this member at a time.
    publishing: Arc<AtomicBool>,
    /// This member's own durable bridge receipt, retained as soon as the
    /// native publisher returned it and before any later check.
    bridge: Option<Arc<crate::db::NativeJointServingBridgeReceipt>>,
    bridge_confirmed: bool,
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
    /// Memory and durable evidence of this member's own publication; DATA.
    #[cfg(test)]
    pub(super) fn test_publication_evidence(&self) -> Result<serde_json::Value> {
        let custody = self.custody.as_ref();
        Ok(serde_json::json!({
            "publication_pending":self.publication_pending,
            "bridge_confirmed":self.bridge_confirmed,
            "retained_bridge_sha256":self.bridge.as_ref().map(|receipt|receipt.checksum_sha256().to_owned()),
            "read_pin_released":custody.map(|c|c.test_read_pin_released()).transpose()?,
            "durable_bridge":custody.map(|c|c.test_durable_bridge_record()).transpose()?.flatten(),
        }))
    }
    /// Infallible custody transfer immediately after the genuine producer.
    pub(super) fn retain_custody(&mut self, custody: Arc<NativeJointPreparationCustody>) {
        self.custody = Some(custody);
        // The native preparation now owns the same real lease.
        self.lease = None;
    }
    pub(super) fn prepare_outcome(&self, stage: Stage) -> Result<NativeJointHandoffStepOutcomeV1> {
        Ok(match &self.custody {
            Some(custody) => {
                prepared_outcome(self.binding_sha256.clone(), custody.retained_facts()?, None)
            }
            None => NativeJointHandoffStepOutcomeV1::bridge_progress(
                self.binding_sha256.clone(),
                stage,
                self.decision_sha256.clone(),
            ),
        })
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
    /// Retention of a produced outcome may not be skipped because another
    /// bounded request of this member is in the table; holders never wait on
    /// this member's own publication.
    fn hold_for_retention(
        &self,
    ) -> parking_lot::MutexGuard<'_, BTreeMap<crate::RangeId, NativeBridgeMemberStep>> {
        self.steps.lock()
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
                publication_pending: false,
                publishing: Arc::new(AtomicBool::new(false)),
                bridge: None,
                bridge_confirmed: false,
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
    outcome.writer_identity_sha256 = Some(facts.writer_identity_sha256);
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

/// Clears the single-attempt claim when one publication attempt ends.
struct PublishingClaim(Arc<AtomicBool>);
impl Drop for PublishingClaim {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// This member publishes its OWN native serving bridge.
///
/// The member itself observes every other NEW member over its own pinned
/// transport before the durable write and again after it. The reply reports
/// the durable receipt checksum only after that second window confirmed an
/// unchanged member set; otherwise the uncertain marker stays and the same
/// request identity resumes under a distinct fresh phase, where the native
/// publisher reconciles the exact existing bridge instead of rewriting it.
pub(super) fn publish_member_bridge(
    steps: &NativeBridgeMemberSteps,
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    request: &NativeJointHandoffStepRequestV1,
    fresh: BridgeStepOwn,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    let binding = request.binding()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    let published_outcome = |custody: &NativeJointPreparationCustody,
                             receipt: &crate::db::NativeJointServingBridgeReceipt|
     -> Result<NativeJointHandoffStepOutcomeV1> {
        let mut outcome = prepared_outcome(binding.clone(), custody.retained_facts()?, None);
        outcome.bridge_receipt_sha256 = Some(receipt.checksum_sha256().into());
        Ok(outcome)
    };
    let anchor = fresh.anchor;
    let (custody, confirmed, claim) = {
        let mut held = steps.hold()?;
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge publish member retains no preparation"))?;
        let custody = step
            .custody()
            .cloned()
            .ok_or_else(|| invalid("joint bridge publish member retains no preparation"))?;
        // The decision this member itself protected, never request DATA.
        request.require_decision(custody.data())?;
        if step.bridge_confirmed && !step.publication_pending {
            (custody, step.bridge.clone(), None)
        } else {
            if step.publishing.swap(true, Ordering::AcqRel) {
                return Err(invalid(
                    "joint bridge publish member attempt already running",
                ));
            }
            // Uncertain from here until a second window confirms.
            step.publication_pending = true;
            (
                custody,
                None,
                Some(PublishingClaim(step.publishing.clone())),
            )
        }
    };
    if let Some(receipt) = confirmed {
        // An identical retry: the SAME retained receipt, re-read as this
        // member's actual protected bridge under its own lock.
        custody.require_published_bridge_current(&receipt)?;
        token.check()?;
        return Ok((published_outcome(&custody, &receipt)?, anchor));
    }
    let _claim = claim;
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    let BridgeStepOwn { own, business, .. } = fresh;
    let published = custody.publish_remote_bridge(transport, &leader, &own, business, token)?;
    let receipt = published.receipt().clone();
    // Retained BEFORE the second window or any later check can fail.
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge publish member custody ended"))?;
        if step
            .bridge
            .as_ref()
            .is_some_and(|old| old.checksum_sha256() != receipt.checksum_sha256())
        {
            return Err(invalid(
                "joint bridge publish changed this member's retained native bridge",
            ));
        }
        step.bridge = Some(receipt.clone());
    }
    let incomplete = || {
        NativeJointHandoffStepOutcomeV1::bridge_progress(
            binding.clone(),
            Stage::BridgePublication,
            decision.clone(),
        )
    };
    if bridge_remaining(token).map_or(true, |left| left < MIN_BRIDGE_SUBSTEP_BUDGET) {
        return Ok((incomplete(), anchor));
    }
    if let Err(_error) =
        custody.confirm_remote_bridge_second_window(&published, transport, &leader, token)
    {
        // Fail closed: the durable bridge exists but is not reported. The
        // marker stays; the next call re-proves everything under a new phase.
        #[cfg(test)]
        eprintln!(
            "NATIVE_BRIDGE_PUBLISH_UNCONFIRMED node={} error={_error}",
            custody.local_node_id()
        );
        return Ok((incomplete(), anchor));
    }
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge publish member custody ended"))?;
        step.bridge_confirmed = true;
        step.publication_pending = false;
    }
    token.check()?;
    Ok((published_outcome(&custody, &receipt)?, anchor))
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
