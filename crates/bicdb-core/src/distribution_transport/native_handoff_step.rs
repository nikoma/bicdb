//! Pinned mTLS joint handoff step. The initiator sends identity DATA only and
//! receives historical progress DATA. Only this actual exchange constructs the
//! opaque observation; a decoded reply is never a local owner, quorum or grant.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeJointHandoffStepOutcomeV1, NativeJointHandoffStepReplyV1,
    NativeJointHandoffStepRequestV1,
};

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
}
