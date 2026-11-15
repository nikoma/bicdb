//! Actual pinned TLS exchange only. Decoding a response never constructs this
//! opaque witness, a local quorum, or a recipient native-effect permission.
use super::*;
use crate::distribution_native_host::{
    NativeRemoteServingOperationV1, NativeRemoteServingPointV1, NativeRemoteServingRequestV1,
};
use crate::distribution_range_image::NativeRemoteImageManifestV1;

pub(crate) struct AuthenticatedNativeRemoteServingFrame {
    node: ClusterNodeId,
    incarnation: u64,
    observer: ClusterNodeId,
    manifest_sha256: String,
    phase: Uuid,
    decision_sha256: String,
    request_sha256: String,
    operation: NativeRemoteServingOperationV1,
    point: NativeRemoteServingPointV1,
    metadata: (u64, u64, Uuid),
    original_request: CancellationToken,
}
impl AuthenticatedNativeRemoteServingFrame {
    pub(crate) fn require_live_binding(
        &self,
        mf: &NativeRemoteImageManifestV1,
        phase: Uuid,
        decision: &str,
    ) -> Result<()> {
        self.original_request.check()?;
        if self.manifest_sha256 != mf.checksum_sha256
            || self.phase != phase
            || self.decision_sha256 != decision
            || mf
                .new_members
                .get(&self.node)
                .or_else(|| {
                    (self.operation == NativeRemoteServingOperationV1::ObserveDecision)
                        .then(|| mf.old_members.get(&self.node))
                        .flatten()
                })
                .is_none_or(|m| m.incarnation != self.incarnation)
        {
            return Err(transport_error(
                "serving authenticated observation changed original MF/phase/member/D",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        &self.node
    }
    pub(crate) fn member_incarnation(&self) -> u64 {
        self.incarnation
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        &self.observer
    }
    pub(crate) fn operation(&self) -> NativeRemoteServingOperationV1 {
        self.operation
    }
    pub(crate) fn point(&self) -> &NativeRemoteServingPointV1 {
        &self.point
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        self.metadata
    }
    pub(crate) fn retained_encoded_bytes(&self) -> Result<usize> {
        Ok(crate::distribution_range_image::bounded_json(
            &(
                &self.node,
                self.incarnation,
                &self.observer,
                &self.manifest_sha256,
                self.phase,
                &self.decision_sha256,
                &self.request_sha256,
                self.operation,
                &self.point,
                self.metadata,
            ),
            64 * 1024,
        )?
        .len())
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn request_remote_native_serving(
        &self,
        node: &ClusterNodeId,
        request: &NativeRemoteServingRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteServingFrame> {
        request.require_shape()?;
        let token = request.cancellation(parent)?;
        own.require_live_request(business)?;
        let topology = own.committed_topology(business)?;
        let mf = &request.manifest;
        let record = topology
            .relocations
            .get(&mf.relocation_id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| transport_error("serving client actual committed decision absent"))?;
        if own.requester_node_id() != &self.caller_node_id
            || topology.cluster_id != mf.cluster_id
            || topology.range_image_bootstrap_id != Some(mf.bootstrap_id)
            || own.restore_activation(business)? != mf.restore.as_ref()
            || topology
                .range_by_id(mf.capture_range.id)
                .map(crate::distribution_native_read::canonical_range)
                != Some(crate::distribution_native_read::canonical_range(
                    &mf.activation_range,
                ))
            || record.stage() != crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            || record.decision_sha256() != request.decision_sha256
            || record.session_id() != mf.handoff_session
            || record.nonce() != mf.nonce
        {
            return Err(transport_error(
                "serving actual client/current phase differs",
            ));
        }
        let member = topology
            .nodes
            .get(node)
            .ok_or_else(|| transport_error("serving destination absent"))?;
        let expected = mf
            .new_members
            .get(node)
            .or_else(|| {
                (request.operation == NativeRemoteServingOperationV1::ObserveDecision)
                    .then(|| mf.old_members.get(node))
                    .flatten()
            })
            .ok_or_else(|| transport_error("serving destination not final NEW member"))?;
        let birth = member
            .native_store_birth_v1
            .as_ref()
            .ok_or_else(|| transport_error("serving destination native birth absent"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != expected.incarnation
            || member.tls_certificate_sha256 != expected.tls_certificate_sha256
            || member.pending_tls_certificate_sha256 != expected.pending_tls_certificate_sha256
            || (node == &mf.target_id && birth != &mf.target_birth)
            || (node == &mf.source_id && birth != &mf.source_birth)
            || mf.actual_old_closures.get(node).is_some_and(|old| {
                old.birth_id != birth.birth_id() || old.birth_sha256 != birth.birth_sha256()
            })
        {
            return Err(transport_error(
                "serving destination substituted original native member",
            ));
        }
        self.require_numeric_metadata_destination(node, &token)?;
        let (response, peer, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            node,
            ClusterDataRequest::NativeRemoteServing {
                request: Box::new(request.clone()),
            },
        )?;
        token.check()?;
        own.require_live_request(business)?;
        if peer.node_id() != node
            || peer.member_incarnation() != member.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|p| !member.accepts_tls_certificate_sha256(p))
            || actual_birth.as_ref() != Some(birth)
        {
            return Err(transport_error(
                "serving actual pinned TLS/native destination differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeRemoteServing(reply) => reply,
            other => return Err(unexpected_response("native remote serving", other)),
        };
        reply.require_request(request)?;
        if reply.node_id != *node
            || reply.metadata_term < own.term()
            || reply.metadata_read_index < own.read_index()
        {
            return Err(transport_error(
                "serving authenticated response regressed actual member/MQ",
            ));
        }
        // Retain MF once per round, compact this bounded actual response only
        // AFTER exact request/authenticated native destination validation.
        Ok(AuthenticatedNativeRemoteServingFrame {
            node: node.clone(),
            incarnation: member.incarnation,
            observer: self.caller_node_id.clone(),
            manifest_sha256: mf.checksum_sha256.clone(),
            phase: request.phase_id,
            decision_sha256: request.decision_sha256.clone(),
            request_sha256: request.hash()?,
            operation: request.operation,
            point: reply.actual,
            metadata: (
                reply.metadata_term,
                reply.metadata_read_index,
                reply.metadata_context,
            ),
            original_request: token,
        })
    }
}
