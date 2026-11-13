//! Pinned native OLD exchange. Only this real transport path constructs the
//! opaque historical observation; decoding a reply is never a local closure.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeRemoteOldOperationV1, NativeRemoteOldReplyV1,
    NativeRemoteOldRequestV1,
};

pub(crate) struct AuthenticatedNativeRemoteOldFrame {
    reply: NativeRemoteOldReplyV1,
    original_request: CancellationToken,
    actual_observer: ClusterNodeId,
}
impl AuthenticatedNativeRemoteOldFrame {
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn reply(&self) -> &NativeRemoteOldReplyV1 {
        &self.reply
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        &self.actual_observer
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn request_remote_native_old(
        &self,
        recipient: &ClusterNodeId,
        plan: &NativeJointHandoffPlan,
        request: &NativeRemoteOldRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteOldFrame> {
        request.require_plan(plan)?;
        let token = request.cancellation(parent)?;
        own.require_live_request(request.business)?;
        let current = own.committed_topology(request.business)?;
        let member = current
            .nodes
            .get(recipient)
            .ok_or_else(|| transport_error("remote OLD member missing"))?;
        let caller = current
            .nodes
            .get(&self.caller_node_id)
            .ok_or_else(|| transport_error("remote OLD caller missing"))?;
        let native_birth = plan.enrolled_birth(recipient)?;
        if current.cluster_id != self.cluster_id
            || own.requester_node_id() != &self.caller_node_id
            || own.requester_incarnation() != caller.incarnation
            || member.lifecycle != ClusterNodeLifecycle::Active
            || caller.lifecycle != ClusterNodeLifecycle::Active
            || !plan.old_members().contains_key(recipient)
            || (request.operation == NativeRemoteOldOperationV1::Observe
                && !plan.old_members().contains_key(&self.caller_node_id))
            || member.native_store_birth_v1.as_ref() != Some(native_birth)
            || !current
                .range_by_id(request.range_id)
                .is_some_and(|r| plan.matches_capture_range(r))
            || self.config.dev_localhost_plaintext
        {
            return Err(transport_error(
                "remote OLD original OWN/current actor/Pending binding differs",
            ));
        }
        self.require_numeric_metadata_destination(recipient, &token)?;
        let (response, actual, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            recipient,
            ClusterDataRequest::NativeRemoteOld {
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
                .is_none_or(|p| !member.accepts_tls_certificate_sha256(p))
            || actual_birth.as_ref() != Some(native_birth)
        {
            return Err(transport_error(
                "remote OLD actual TLS/native destination differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeRemoteOld(reply) => reply,
            other => return Err(unexpected_response("native OLD", other)),
        };
        if reply.request != *request
            || &reply.node_id != recipient
            || &reply.birth != native_birth
            || reply.database_open.is_empty()
            || reply.store_open.is_nil()
            || reply.metadata_store_id.is_nil()
            || reply.metadata_context_id.is_nil()
            || reply.metadata_term < own.term()
            || reply.metadata_read_index < own.read_index()
            || reply.checksum()? != reply.checksum_sha256
            || reply.constraints.node_id != *recipient
            || reply.constraints.scope_sha256 != request.scope_sha256
            || reply.constraints.birth_id != native_birth.birth_id()
            || reply.constraints.birth_sha256 != native_birth.birth_sha256()
            || reply.constraints.commands.len() > 4096
            || reply.constraints.unresolved_commands.len() > 4096
        {
            return Err(transport_error(
                "remote OLD actual response/request/identity/constraints differ",
            ));
        }
        if let Some(facts) = &reply.closed {
            if facts.node_id != *recipient
                || facts.scope_sha256 != request.scope_sha256
                || facts.birth_id != native_birth.birth_id()
                || facts.birth_sha256 != native_birth.birth_sha256()
                || facts.native_receipt_sha256.len() != 64
                || facts.logical_genesis_sha256.len() != 64
                || facts.logical_history_sha256.len() != 64
                || facts.metadata_context_id.is_nil()
                || facts.metadata_term > reply.metadata_term
                || facts.metadata_read_index > reply.metadata_read_index
                || facts.image_root_sha256.is_some()
                || facts.source_snapshot_db_sequence.is_some()
            {
                return Err(transport_error(
                    "remote OLD actual closure differs from original old native member",
                ));
            }
        } else if request.operation == NativeRemoteOldOperationV1::Close {
            return Err(transport_error(
                "remote OLD Close returned no actual closed fact",
            ));
        }
        token.check()?;
        own.require_live_request(request.business)?;
        Ok(AuthenticatedNativeRemoteOldFrame {
            reply,
            original_request: token,
            actual_observer: self.caller_node_id.clone(),
        })
    }
}
