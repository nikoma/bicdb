//! Actual pinned coordinator-to-target exchange. A reply is result DATA,
//! never an import permission or a local/remote decision quorum.
use super::*;
use crate::distribution_native_host::{NativeFirstReceiveReplyV1, NativeFirstReceiveRequestV1};
use crate::distribution_range_handoff::NativeJointHandoffPlan;

pub(crate) struct AuthenticatedNativeFirstReceive {
    reply: NativeFirstReceiveReplyV1,
    original_request: CancellationToken,
}
impl AuthenticatedNativeFirstReceive {
    pub(crate) fn reply(&self) -> &NativeFirstReceiveReplyV1 {
        &self.reply
    }
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn request_native_first_receive(
        &self,
        plan: &NativeJointHandoffPlan,
        request: &NativeFirstReceiveRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeFirstReceive> {
        let token = request.cancellation(parent)?;
        own.require_live_request(business)?;
        let topology = own.committed_topology(business)?;
        let p = plan.promise();
        let relocation = topology
            .relocations
            .get(&request.relocation)
            .ok_or_else(|| transport_error("FIRST committed relocation absent"))?;
        let caller = topology
            .nodes
            .get(&self.caller_node_id)
            .ok_or_else(|| transport_error("FIRST caller absent"))?;
        let target = topology
            .nodes
            .get(&relocation.target)
            .ok_or_else(|| transport_error("FIRST target absent"))?;
        let birth = plan.enrolled_birth(&target.id)?;
        if self.config.dev_localhost_plaintext
            || topology.cluster_id != self.cluster_id
            || self.caller_node_id != plan.old_range().leader
            || own.requester_node_id() != &self.caller_node_id
            || own.requester_incarnation() != caller.incarnation
            || caller.lifecycle != ClusterNodeLifecycle::Active
            || target.lifecycle != ClusterNodeLifecycle::Active
            || target.native_store_birth_v1.as_ref() != Some(birth)
            || !topology
                .range_by_id(request.range)
                .is_some_and(|r| plan.matches_capture_range(r))
            || request.relocation != p.relocation_id
            || request.session != p.session_id
            || request.nonce != p.nonce
            || request.epoch != plan.old_range().epoch
            || !plan.old_members().contains_key(&request.source)
        {
            return Err(transport_error(
                "FIRST actual range-leader OWN/Pending/target binding differs",
            ));
        }
        self.require_numeric_metadata_destination(&target.id, &token)?;
        let (response, peer, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            &target.id,
            ClusterDataRequest::NativeFirstReceive {
                request: Box::new(request.clone()),
            },
        )?;
        token.check()?;
        own.require_live_request(business)?;
        if peer.node_id() != &target.id
            || peer.member_incarnation() != target.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !target.accepts_tls_certificate_sha256(pin))
            || actual_birth.as_ref() != Some(birth)
        {
            return Err(transport_error("FIRST actual TLS/native target differs"));
        }
        let reply = match response {
            ClusterDataResponse::NativeFirstReceive(reply) => *reply,
            other => return Err(unexpected_response("FIRST receive", other)),
        };
        if reply.request != *request
            || reply.target != target.id
            || &reply.birth != birth
            || reply.target_attempt.is_some_and(|a| a.is_nil())
            || (reply.target_attempt.is_none()
                && !matches!(
                    request.operation,
                    crate::distribution_native_host::NativeFirstReceiveOperationV1::ObserveLast { .. }
                ))
            || request
                .target_attempt
                .is_some_and(|a| Some(a) != reply.target_attempt)
        {
            return Err(transport_error(
                "FIRST actual request/target/attempt response differs",
            ));
        }
        crate::distribution_range_image::bounded_json(&reply, 4 * 1024 * 1024)?;
        token.check()?;
        own.require_live_request(business)?;
        Ok(AuthenticatedNativeFirstReceive {
            reply,
            original_request: token,
        })
    }
}
