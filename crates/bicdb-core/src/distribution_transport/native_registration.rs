//! Request provenance owned by the actual mTLS receiver, never a decoded vote.
use super::*;

#[derive(Debug)]
pub(crate) struct NativeMetadataRegistrationIngress {
    caller: ClusterNodeId,
    incarnation: Option<u64>,
    certificate: String,
    node: ClusterNode,
    now_ms: u64,
    token: CancellationToken,
}
impl NativeMetadataRegistrationIngress {
    pub(super) fn from_authenticated_envelope(
        caller: ClusterNodeId,
        incarnation: Option<u64>,
        certificate: &str,
        node: ClusterNode,
        now_ms: u64,
        token: CancellationToken,
    ) -> Result<Self> {
        crate::distribution_range_consensus::validate_native_json_size(&node, 32 * 1024)?;
        token.check()?;
        Ok(Self {
            caller,
            incarnation,
            certificate: certificate.to_string(),
            node,
            now_ms,
            token,
        })
    }
    pub(crate) fn queue_bytes(&self) -> Result<usize> {
        let value = (
            &self.caller,
            self.incarnation,
            &self.certificate,
            &self.node,
            self.now_ms,
        );
        crate::distribution_range_consensus::validate_native_json_size(&value, 34 * 1024)?;
        Ok(serde_json::to_vec(&value)?.len())
    }
    pub(crate) fn node(&self) -> &ClusterNode {
        &self.node
    }
    pub(crate) fn now_ms(&self) -> u64 {
        self.now_ms
    }
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }
    pub(crate) fn require_current(&self, topology: &ClusterTopology) -> Result<()> {
        self.token.check()?;
        let node = &self.node;
        if node.lifecycle != crate::ClusterNodeLifecycle::Active
            || node.metadata_role != MetadataMemberRole::Learner
            || node.native_store_birth_v1.is_some()
            || node.pending_tls_certificate_sha256.is_some()
            || node.tls_certificate_sha256.is_none()
            || node.incarnation == 0
            || node.capacity_bytes == 0
            || node.used_bytes != 0
        {
            return Err(transport_error(
                "native registration requires an unbound Active Learner",
            ));
        }
        if let Some(caller) = topology.nodes.get(&self.caller) {
            if caller.lifecycle != crate::ClusterNodeLifecycle::Active
                || Some(caller.incarnation) != self.incarnation
                || !caller.accepts_tls_certificate_sha256(&self.certificate)
                || (self.caller != node.id && caller.metadata_role != MetadataMemberRole::Voter)
            {
                return Err(transport_error(
                    "native registration authenticated actor changed",
                ));
            }
        } else if self.incarnation.is_some()
            || self.caller != node.id
            || node.tls_certificate_sha256.as_deref() != Some(self.certificate.as_str())
        {
            return Err(transport_error(
                "unknown native caller may register only its exact TLS identity",
            ));
        }
        if topology
            .ranges
            .values()
            .any(|r| r.replicas.iter().any(|r| r.node_id == node.id))
        {
            // Existing assigned members can be observed idempotently, but never
            // re-enter the new-identity producer through this path.
            if !topology.nodes.contains_key(&node.id) {
                return Err(transport_error(
                    "native learner registration cannot assign data",
                ));
            }
        }
        Ok(())
    }
}
