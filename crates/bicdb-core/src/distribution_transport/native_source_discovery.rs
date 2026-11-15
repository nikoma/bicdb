//! Actual authenticated donor Start/Describe exchange. No expected manifest,
//! zero digest, local lineage, or serialized source capability is accepted.
use super::*;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_image::{
    NativeSourceDiscoveryReplyV1, NativeSourceDiscoveryRequestV1,
};

pub(crate) struct AuthenticatedNativeSourceDiscovery {
    reply: NativeSourceDiscoveryReplyV1,
    original_request: CancellationToken,
}
impl AuthenticatedNativeSourceDiscovery {
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn reply(&self) -> &NativeSourceDiscoveryReplyV1 {
        &self.reply
    }
}
impl TcpClusterRelocationTransport {
    /// Fault-control sender only. Produces decoded DATA, never the strict
    /// authenticated wrapper or a manifest fact. Server checks remain real.
    #[cfg(test)]
    pub(crate) fn test_send_source_discovery_request(
        &self,
        request: &NativeSourceDiscoveryRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeSourceDiscoveryReplyV1> {
        let (response, _, _) = self.call_with_native_destination_cancellable(
            parent,
            &request.source_id,
            ClusterDataRequest::NativeSourceDiscovery {
                request: request.clone(),
            },
        )?;
        match response {
            ClusterDataResponse::NativeSourceDiscovery(x) => Ok(x),
            other => Err(unexpected_response("test source discovery DATA", other)),
        }
    }

    pub(crate) fn request_remote_image_source_discovery(
        &self,
        plan: &NativeJointHandoffPlan,
        request: &NativeSourceDiscoveryRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeSourceDiscovery> {
        request.require_plan(plan)?;
        let token = request.cancellation(parent)?;
        own.require_live_request(request.business)?;
        let current = own.committed_topology(request.business)?;
        let source = current
            .nodes
            .get(&request.source_id)
            .ok_or_else(|| transport_error("discovery donor absent"))?;
        let target = current
            .nodes
            .get(&request.target_id)
            .ok_or_else(|| transport_error("discovery target absent"))?;
        let relocation = current
            .relocations
            .get(&request.relocation_id)
            .ok_or_else(|| transport_error("discovery relocation absent"))?;
        if self.config.dev_localhost_plaintext
            || self.cluster_id != request.cluster_id
            || current.cluster_id != request.cluster_id
            || current.range_image_bootstrap_id != Some(request.bootstrap_id)
            || self.caller_node_id != request.target_id
            || own.requester_node_id() != &request.target_id
            || own.requester_incarnation() != target.incarnation
            || source.lifecycle != ClusterNodeLifecycle::Active
            || target.lifecycle != ClusterNodeLifecycle::Active
            || source.native_store_birth_v1.as_ref() != Some(&request.source_birth)
            || target.native_store_birth_v1.as_ref() != Some(&request.target_birth)
            || !current
                .range_by_id(request.range_id)
                .is_some_and(|r| plan.matches_capture_range(r))
            || relocation.plan_id != request.plan_id
            || relocation
                .handoff_session
                .as_ref()
                .is_none_or(|s| s.session_id != request.handoff_session || s.nonce != request.nonce)
        {
            return Err(transport_error(
                "source discovery actual target OWN/current Pending binding differs",
            ));
        }
        self.require_numeric_metadata_destination(&request.source_id, &token)?;
        let (response, peer, birth) = self.call_with_native_destination_cancellable(
            &token,
            &request.source_id,
            ClusterDataRequest::NativeSourceDiscovery {
                request: request.clone(),
            },
        )?;
        token.check()?;
        own.require_live_request(request.business)?;
        if peer.node_id() != &request.source_id
            || peer.member_incarnation() != source.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !source.accepts_tls_certificate_sha256(pin))
            || birth.as_ref() != Some(&request.source_birth)
        {
            return Err(transport_error(
                "source discovery actual TLS/native donor differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeSourceDiscovery(x) => x,
            other => return Err(unexpected_response("source discovery", other)),
        };
        let mf = &reply.manifest;
        match (&reply.decision_provenance, request.operation) {
            (Some(proof), crate::distribution_range_image::NativeSourceDiscoveryOperationV1::DecisionProvenance) => proof.require_manifest(mf)?,
            (None, crate::distribution_range_image::NativeSourceDiscoveryOperationV1::Start | crate::distribution_range_image::NativeSourceDiscoveryOperationV1::Describe) => {},
            _ => return Err(transport_error("source decision provenance absent/unrequested")),
        }
        mf.require_shape()?;
        if reply.request != *request
            || reply.source_metadata_context.is_nil()
            || reply.source_metadata_term < own.term()
            || reply.source_metadata_read_index < own.read_index()
            || reply.checksum()? != reply.checksum_sha256
            || mf.cluster_id != request.cluster_id
            || mf.bootstrap_id != request.bootstrap_id
            || mf.source_id != request.source_id
            || mf.target_id != request.target_id
            || mf.source_birth != request.source_birth
            || mf.target_birth != request.target_birth
            || mf.target_attempt != request.target_attempt
            || mf.relocation_id != request.relocation_id
            || mf.plan_id != request.plan_id
            || mf.handoff_session != request.handoff_session
            || mf.nonce != request.nonce
            || mf.old_config_sha256 != plan.promise().old_config_sha256
            || mf.new_config_sha256 != plan.promise().new_config_sha256
            || !plan.matches_capture_range(&mf.capture_range)
            || crate::distribution_native_read::canonical_range(&mf.activation_range)
                != crate::distribution_native_read::canonical_range(plan.activation_range())
            || &mf.old_members != plan.old_members()
            || &mf.new_members != plan.new_members()
            || mf.schema_sha256 != plan.constraint_schema_sha256()
        {
            return Err(transport_error(
                "source discovery actual request/manifest/plan correlation differs",
            ));
        }
        token.check()?;
        own.require_live_request(request.business)?;
        Ok(AuthenticatedNativeSourceDiscovery {
            reply,
            original_request: token,
        })
    }
}
