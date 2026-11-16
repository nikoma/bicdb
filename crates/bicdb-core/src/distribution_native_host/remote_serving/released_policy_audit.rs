//! Historical audit DATA produced only by the actual protected release reader.
//! Deserialization never constructs a release/Policy admission or local QQ.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteReleasedPolicyAuditV1 {
    pub metadata: crate::distribution_consensus::HostMetadataReadAcquireResponse,
    pub native_bridge_sha256: String,
    pub catalogue_activation_sha256: String,
    pub released_generation: Uuid,
    pub local_node_id: ClusterNodeId,
}
impl NativeRemoteReleasedPolicyAuditV1 {
    pub(crate) fn from_protected_release(
        actual: &crate::distribution_data::ActualServiceNativeJointReleaseEvidence,
    ) -> Result<Self> {
        // Caller retains the genuine release-state/native historical scope
        // through this projection. No anchors are expanded into an OWN DTO.
        let projected = serde_json::from_value(actual.writer_domain_audit()?)?;
        Ok(projected)
    }
    pub(crate) fn require_reply(
        &self,
        request: &NativeRemoteServingRequestV1,
        reply: &NativeRemoteServingReplyV1,
    ) -> Result<()> {
        self.metadata.validate(&self.metadata.request)?;
        let point = &reply.actual;
        let member = request
            .manifest
            .new_members
            .get(&reply.node_id)
            .ok_or_else(|| invalid("policy audit omitted actual final member"))?;
        let topology = &self.metadata.evidence.committed_topology;
        let record = topology
            .relocations
            .get(&request.manifest.relocation_id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| invalid("policy audit original committed native phase absent"))?;
        if !point.actual_dropped_open
            || self.released_generation.is_nil()
            || self.local_node_id != reply.node_id
            || self.metadata.request.requester_node_id != reply.node_id
            || self.metadata.request.requester_incarnation != member.incarnation
            || point.native_bridge_sha256.as_deref() != Some(self.native_bridge_sha256.as_str())
            || point.catalogue_activation_sha256.as_deref()
                != Some(self.catalogue_activation_sha256.as_str())
            || self.metadata.evidence.term != reply.metadata_term
            || self.metadata.evidence.read_index != reply.metadata_read_index
            || self.metadata.evidence.context_id != reply.metadata_context
            || topology.cluster_id != request.manifest.cluster_id
            || topology.range_image_bootstrap_id != Some(request.manifest.bootstrap_id)
            || self.metadata.evidence.restore_activation.as_ref()
                != request.manifest.restore.as_ref()
            || topology
                .range_by_id(request.manifest.capture_range.id)
                .map(crate::distribution_native_read::canonical_range)
                != Some(crate::distribution_native_read::canonical_range(
                    &request.manifest.activation_range,
                ))
            || record.stage() != crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            || record.decision_sha256() != request.decision_sha256
            || record.session_id() != request.manifest.handoff_session
            || record.nonce() != request.manifest.nonce
        {
            return Err(invalid(
                "policy audit substituted actual source OWN/member/native/Open/generation/phase",
            ));
        }
        Ok(())
    }
}
