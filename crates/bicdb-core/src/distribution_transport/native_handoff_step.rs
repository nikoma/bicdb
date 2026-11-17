//! Pinned mTLS joint handoff step. The initiator sends identity DATA only and
//! receives historical progress DATA. Only this actual exchange constructs the
//! opaque observation; a decoded reply is never a local owner, quorum or grant.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointBridgeDecision, NativeJointBridgePhaseKindV1, NativeJointHandoffPlan,
    NativeJointHandoffStepOperationV1, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepReplyV1, NativeJointHandoffStepRequestV1,
};

/// How long a member repeats its own read-only phase observation while its
/// own state is held by another request of the same member.
const MEMBER_OBSERVATION_BUSY_WAIT: Duration = Duration::from_millis(750);
/// Pause between two such repetitions.
const MEMBER_OBSERVATION_BUSY_PAUSE: Duration = Duration::from_millis(10);

/// Receiver side of a joint handoff step. A member reads its own state
/// without waiting (`try_lock`) and refuses with "busy" while another
/// request of the same member, usually an ordinary client request, holds
/// it. For the read-only phase observation that refusal fails the whole
/// window of whoever observes (the coordinator, or another member inside
/// its second window), although the holder releases shortly. The member
/// therefore repeats the observation itself, for a bounded time inside the
/// request's own deadline.
///
/// Every repetition is a complete new attempt of the same request: the peer
/// is authenticated again and the member takes its own fresh committed
/// metadata again, exactly as if the observer had sent the request again.
/// No lock is held between two attempts, so the repetition adds no lock
/// order. A step that changes member state is never repeated here: its
/// repetition stays with the coordinator.
pub(super) fn repeat_busy_phase_observation<T>(
    request: &NativeJointHandoffStepRequestV1,
    parent: &CancellationToken,
    attempt: impl Fn() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let mut repeated = 0u32;
    let answer = if !matches!(
        request.operation,
        NativeJointHandoffStepOperationV1::BridgePhaseObserve { .. }
    ) {
        attempt()
    } else {
        let until = started + MEMBER_OBSERVATION_BUSY_WAIT;
        loop {
            match attempt() {
                Err(error)
                    if Instant::now() < until
                        && parent.check().is_ok()
                        && error.to_string().contains("busy") =>
                {
                    repeated += 1;
                    std::thread::sleep(MEMBER_OBSERVATION_BUSY_PAUSE);
                }
                other => break other,
            }
        }
    };
    crate::distribution_native_host::range_move_trace(started, || {
        format!(
            "member served step {} busy_repetitions={repeated} {}",
            request.operation.trace_name(),
            match &answer {
                Ok(_) => "ok".to_string(),
                Err(error) => format!("error {error}"),
            }
        )
    });
    answer
}

/// One authenticated bridge step frame. Constructed only by the pinned
/// exchange below; a decoded reply is never one. It records what THIS
/// exchange's TLS/native handshake proved about the replying member and the
/// instant its freshness window closes. It is observation DATA: never a
/// quorum, phase, lease, publication or serving grant.
pub(crate) struct AuthenticatedNativeJointBridgeStepFrame {
    reply: NativeJointHandoffStepReplyV1,
    original_request: CancellationToken,
    actual_observer: ClusterNodeId,
    // Never copied from, or overridable by, reply DATA.
    actual_peer: ClusterNodeId,
    actual_peer_incarnation: u64,
    actual_peer_birth: crate::distribution::NativeStoreBirthV1,
    /// The earliest of the request budget, the observer's own metadata
    /// observation and what the member reported was left of its own.
    window_deadline: Instant,
}
impl AuthenticatedNativeJointBridgeStepFrame {
    /// Still inside the one bounded window this frame was observed in.
    pub(crate) fn require_live(&self) -> Result<()> {
        self.original_request.check()?;
        if Instant::now() >= self.window_deadline {
            return Err(transport_error(
                "joint bridge step frame freshness window expired",
            ));
        }
        Ok(())
    }
    pub(crate) fn reply(&self) -> &NativeJointHandoffStepReplyV1 {
        &self.reply
    }
    pub(crate) fn outcome(&self) -> &NativeJointHandoffStepOutcomeV1 {
        &self.reply.outcome
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        &self.actual_observer
    }
    pub(crate) fn actual_peer(&self) -> &ClusterNodeId {
        &self.actual_peer
    }
    pub(crate) fn actual_peer_incarnation(&self) -> u64 {
        self.actual_peer_incarnation
    }
    pub(crate) fn actual_peer_birth(&self) -> &crate::distribution::NativeStoreBirthV1 {
        &self.actual_peer_birth
    }
    pub(crate) fn window_deadline(&self) -> Instant {
        self.window_deadline
    }
    /// The member's own metadata anchor of the observation behind this frame.
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.reply.metadata_term,
            self.reply.metadata_read_index,
            self.reply.metadata_context_id,
        )
    }
}

pub(crate) struct AuthenticatedNativeJointHandoffStep {
    reply: NativeJointHandoffStepReplyV1,
    original_request: CancellationToken,
    actual_observer: ClusterNodeId,
}
impl AuthenticatedNativeJointHandoffStep {
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn reply(&self) -> &NativeJointHandoffStepReplyV1 {
        &self.reply
    }
    pub(crate) fn outcome(&self) -> &NativeJointHandoffStepOutcomeV1 {
        &self.reply.outcome
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        &self.actual_observer
    }
}
impl TcpClusterRelocationTransport {
    /// Fault-control sender only. Produces decoded DATA, never the strict
    /// authenticated wrapper. Every recipient check remains real.
    #[cfg(test)]
    pub(crate) fn test_send_joint_handoff_step_request(
        &self,
        recipient: &ClusterNodeId,
        request: &NativeJointHandoffStepRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeJointHandoffStepReplyV1> {
        let (response, _, _) = self.call_with_native_destination_cancellable(
            parent,
            recipient,
            ClusterDataRequest::NativeJointHandoffStep {
                request: request.clone(),
            },
        )?;
        match response {
            ClusterDataResponse::NativeJointHandoffStep(reply) => Ok(reply),
            other => Err(unexpected_response("test joint handoff step DATA", other)),
        }
    }

    /// One bounded step. The recipient performs the named operation itself
    /// under its own fresh Role and metadata; an error leaves an unknown
    /// effect that the same request identity resumes on the next call.
    pub(crate) fn request_native_joint_handoff_step(
        &self,
        recipient: &ClusterNodeId,
        plan: &NativeJointHandoffPlan,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointHandoffStep> {
        request.require_plan(plan)?;
        let token = request.cancellation(parent)?;
        own.require_live_request(request.business)?;
        let current = own.committed_topology(request.business)?;
        let member = current
            .nodes
            .get(recipient)
            .ok_or_else(|| transport_error("joint handoff step recipient missing"))?;
        let caller = current
            .nodes
            .get(&self.caller_node_id)
            .ok_or_else(|| transport_error("joint handoff step caller missing"))?;
        let native_birth = plan.enrolled_birth(recipient)?;
        if current.cluster_id != self.cluster_id
            || own.requester_node_id() != &self.caller_node_id
            || own.requester_incarnation() != caller.incarnation
            || member.lifecycle != ClusterNodeLifecycle::Active
            || caller.lifecycle != ClusterNodeLifecycle::Active
            || (!plan.old_members().contains_key(recipient)
                && !plan.new_members().contains_key(recipient))
            || member.native_store_birth_v1.as_ref() != Some(native_birth)
            || !current
                .range_by_id(request.range_id)
                .is_some_and(|r| plan.matches_capture_range(r))
            || self.config.dev_localhost_plaintext
        {
            return Err(transport_error(
                "joint handoff step original OWN/current actor/Pending binding differs",
            ));
        }
        self.require_numeric_metadata_destination(recipient, &token)?;
        let (response, actual, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            recipient,
            ClusterDataRequest::NativeJointHandoffStep {
                request: request.clone(),
            },
        )?;
        token.check()?;
        own.require_live_request(request.business)?;
        if actual.node_id() != recipient
            || actual.member_incarnation() != member.incarnation
            || actual.plaintext_loopback()
            || actual
                .certificate_sha256()
                .is_none_or(|pin| !member.accepts_tls_certificate_sha256(pin))
            || actual_birth.as_ref() != Some(native_birth)
        {
            return Err(transport_error(
                "joint handoff step actual TLS/native recipient differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeJointHandoffStep(reply) => reply,
            other => return Err(unexpected_response("native joint handoff step", other)),
        };
        reply.require_request(request)?;
        if &reply.node_id != recipient
            || &reply.birth != native_birth
            || reply.metadata_term < own.term()
            || reply.metadata_read_index < own.read_index()
        {
            return Err(transport_error(
                "joint handoff step actual response/metadata identity regressed",
            ));
        }
        token.check()?;
        own.require_live_request(request.business)?;
        Ok(AuthenticatedNativeJointHandoffStep {
            reply,
            original_request: token,
            actual_observer: self.caller_node_id.clone(),
        })
    }

    /// Fault-control sender for bridge steps. Produces decoded DATA, never
    /// the authenticated frame. Every recipient check remains real.
    #[cfg(test)]
    pub(crate) fn test_send_joint_bridge_step_request(
        &self,
        recipient: &ClusterNodeId,
        request: &NativeJointHandoffStepRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeJointHandoffStepReplyV1> {
        self.test_send_joint_handoff_step_request(recipient, request, parent)
    }

    /// Coordinator -> NEW member. The member prepares its own local joint
    /// serving and retains that custody itself; the frame carries checksums.
    pub(crate) fn request_native_joint_bridge_prepare(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgePrepare { .. }
        ) {
            return Err(transport_error(
                "joint bridge prepare exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> prepared NEW member. The member publishes its own
    /// native serving bridge after collecting, itself, fresh observations of
    /// every other NEW member before and after its durable write. The frame
    /// reports the member's own durable bridge receipt checksum once the
    /// member confirmed it; an incomplete frame is resumable progress DATA.
    pub(crate) fn request_native_joint_bridge_publish(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgePublish { .. }
        ) {
            return Err(transport_error(
                "joint bridge publish exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> NEW member holding its own confirmed bridge. The member
    /// persists its own catalogue activation after collecting, itself, fresh
    /// OPEN observations of every other NEW member before and after its
    /// durable write. The frame reports the member's own activation checksum
    /// once the member confirmed it.
    pub(crate) fn request_native_joint_bridge_open(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgeOpen { .. }
        ) {
            return Err(transport_error(
                "joint bridge open exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> durably open NEW member. The member retires its own
    /// obsolete OLD custody after proving, itself, every NEW member open.
    pub(crate) fn request_native_joint_bridge_retire_old_custody(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::RetireOldCustody { .. }
        ) {
            return Err(transport_error(
                "joint bridge custody retirement exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> durably open NEW member. The member releases its own
    /// retained freeze after collecting, itself, fresh RELEASE observations
    /// of every other NEW member before and after its release. The frame
    /// reports the armed generation once the member confirmed it.
    pub(crate) fn request_native_joint_bridge_release(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgeRelease { .. }
        ) {
            return Err(transport_error(
                "joint bridge release exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> released NEW member. The member publishes its own
    /// writer DATA policy for the final membership after collecting, itself,
    /// one validated WRITER AUDIT observation of every other NEW member
    /// before and a RELEASE observation of each after its durable write. A
    /// surviving member completes inside the step; the added member records
    /// the command for its host thread and reports `BridgeHostCommand` until
    /// that thread retained the outcome, so the coordinator repeats the step.
    pub(crate) fn request_native_joint_bridge_writer_policy(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgeWriterPolicy { .. }
        ) {
            return Err(transport_error(
                "joint bridge writer policy exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> the added member holding its own confirmed writer
    /// policy. Its host thread moves the retained first policy to its
    /// ordinary DATA socket and publishes its guarded ingress; the step
    /// reports `BridgeHostCommand` until that thread retained the outcome.
    /// The member's socket is rebound meanwhile: a repeated step reaches the
    /// same address and the same original receiver.
    pub(crate) fn request_native_joint_target_serve(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::TargetServe { .. }
        ) || data.old_members().contains_key(recipient)
        {
            return Err(transport_error(
                "joint target serve exchange carries another operation or addresses an original member",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator -> the RETIRED source of the committed decision. The
    /// source records its own retirement on its own committed metadata read;
    /// the frame reports that durable record.
    pub(crate) fn request_native_joint_source_retire(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::SourceRetire { .. }
        ) {
            return Err(transport_error(
                "joint source retirement exchange carries another operation",
            ));
        }
        self.request_native_joint_bridge_step(recipient, data, request, own, parent)
    }

    /// Coordinator or NEW member -> another NEW member. Read-only: the member
    /// re-proves its own retained state under its own fresh metadata.
    pub(crate) fn request_native_joint_bridge_phase_observation(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        kind: NativeJointBridgePhaseKindV1,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        if !matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgePhaseObserve { kind: actual, .. }
                if *actual == kind
        ) {
            return Err(transport_error(
                "joint bridge phase exchange carries another operation",
            ));
        }
        let frame = self.request_native_joint_bridge_step(recipient, data, request, own, parent)?;
        if frame.outcome().phase_kind != Some(kind) || !frame.outcome().complete {
            return Err(transport_error(
                "joint bridge phase reply is not the requested complete observation",
            ));
        }
        Ok(frame)
    }

    /// The identity comes from the committed decision the CALLER itself
    /// observed; `own` is the caller's fresh metadata the exchange is bound to.
    fn request_native_joint_bridge_step(
        &self,
        recipient: &ClusterNodeId,
        data: &dyn NativeJointBridgeDecision,
        request: &NativeJointHandoffStepRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
        request.require_decision(data)?;
        let token = request.cancellation(parent)?;
        own.require_live_request(request.business)?;
        let current = own.committed_topology(request.business)?;
        data.require_metadata_domain(current, own.restore_activation(request.business)?)?;
        let member = current
            .nodes
            .get(recipient)
            .ok_or_else(|| transport_error("joint bridge step recipient missing"))?;
        let caller = current
            .nodes
            .get(&self.caller_node_id)
            .ok_or_else(|| transport_error("joint bridge step caller missing"))?;
        // Every operation addresses a member of the new set, except the
        // source retirement, which addresses exactly the committed
        // relocation's source: an OLD member absent from the new set.
        let expected = if matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::SourceRetire { .. }
        ) {
            if data.new_members().contains_key(recipient)
                || current
                    .relocations
                    .get(&request.relocation_id)
                    .is_none_or(|relocation| relocation.source.as_ref() != Some(recipient))
            {
                return Err(transport_error(
                    "joint source retirement recipient is not the committed retired source",
                ));
            }
            data.old_members().get(recipient).ok_or_else(|| {
                transport_error("joint source retirement recipient is not an OLD member")
            })?
        } else {
            data.new_members()
                .get(recipient)
                .ok_or_else(|| transport_error("joint bridge step recipient is not a NEW member"))?
        };
        let native_birth = member
            .native_store_birth_v1
            .as_ref()
            .ok_or_else(|| transport_error("joint bridge step recipient has no native birth"))?;
        if current.cluster_id != self.cluster_id
            || recipient == &self.caller_node_id
            || own.requester_node_id() != &self.caller_node_id
            || own.requester_incarnation() != caller.incarnation
            || member.lifecycle != ClusterNodeLifecycle::Active
            || caller.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != expected.incarnation
            || self.config.dev_localhost_plaintext
        {
            return Err(transport_error(
                "joint bridge step original OWN/current actor/decision binding differs",
            ));
        }
        self.require_numeric_metadata_destination(recipient, &token)?;
        let sent = Instant::now();
        let (response, actual, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            recipient,
            ClusterDataRequest::NativeJointHandoffStep {
                request: request.clone(),
            },
        )?;
        token.check()?;
        own.require_live_request(request.business)?;
        if actual.node_id() != recipient
            || actual.member_incarnation() != member.incarnation
            || actual.plaintext_loopback()
            || actual
                .certificate_sha256()
                .is_none_or(|pin| !member.accepts_tls_certificate_sha256(pin))
            || actual_birth.as_ref() != Some(native_birth)
        {
            return Err(transport_error(
                "joint bridge step actual TLS/native recipient differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeJointHandoffStep(reply) => reply,
            other => return Err(unexpected_response("native joint bridge step", other)),
        };
        reply.require_request(request)?;
        if &reply.node_id != recipient
            || &reply.birth != native_birth
            || reply.metadata_term < own.term()
            || reply.metadata_read_index < own.read_index()
            || reply.metadata_context_id == own.context_id()
        {
            return Err(transport_error(
                "joint bridge step actual response/metadata identity regressed",
            ));
        }
        // The window can only shrink: the member's reported remainder counts
        // from the instant BEFORE this request left, which is earlier than
        // when the member measured it, so the member's own observation
        // cannot have expired while this frame still reads as live.
        let mut window_deadline = own.deadline();
        if let Some(deadline) = token.deadline() {
            window_deadline = window_deadline.min(deadline);
        }
        if let Some(left) = reply.outcome.window_remaining_ms {
            window_deadline = window_deadline.min(sent + Duration::from_millis(left));
        }
        let frame = AuthenticatedNativeJointBridgeStepFrame {
            reply,
            original_request: token,
            actual_observer: self.caller_node_id.clone(),
            actual_peer: actual.node_id().clone(),
            actual_peer_incarnation: actual.member_incarnation(),
            actual_peer_birth: native_birth.clone(),
            window_deadline,
        };
        frame.require_live()?;
        own.require_live_request(request.business)?;
        Ok(frame)
    }
}
