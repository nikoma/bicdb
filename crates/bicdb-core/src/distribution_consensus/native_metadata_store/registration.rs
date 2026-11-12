//! CONFIG-only native registration under actual own-leader MQ.
use super::*;
use crate::distribution::ClusterNode;
use crate::distribution_transport::NativeMetadataRegistrationIngress;

pub(crate) fn same_registration(a: &ClusterNode, b: &ClusterNode) -> bool {
    a.id == b.id
        && a.address == b.address
        && a.incarnation == b.incarnation
        && a.tls_certificate_sha256 == b.tls_certificate_sha256
        && a.pending_tls_certificate_sha256 == b.pending_tls_certificate_sha256
        && a.capacity_bytes == b.capacity_bytes
        && a.labels == b.labels
        && a.lifecycle == b.lifecycle
}
impl MetadataConsensusStore {
    pub(crate) fn propose_native_metadata_registration(
        &mut self,
        ingress: &NativeMetadataRegistrationIngress,
        current: &HostMetadataReadObservation,
        business: Uuid,
        config: &crate::DistributionConfig,
    ) -> Result<Option<MetadataLogEntry>> {
        self.require_native_learner_enrollment_leader(current, business)?;
        ingress.require_current(&self.state.committed_topology)?;
        for member in self.state.committed_topology.nodes.values().filter(|n| {
            n.lifecycle == crate::ClusterNodeLifecycle::Active
                && n.metadata_role == MetadataMemberRole::Voter
        }) {
            let birth = member.native_store_birth_v1.as_ref().ok_or_else(|| {
                metadata_error("native registration needs actual enrolled old voters")
            })?;
            birth.validate_for(
                self.state.committed_topology.range_image_bootstrap_id,
                member.incarnation,
            )?;
            if birth.writer_enforcement_version() != 1 {
                return Err(metadata_error(
                    "native registration needs enforced old voters",
                ));
            }
        }
        let desired = ingress.node();
        if let Some(existing) = self.state.committed_topology.nodes.get(&desired.id) {
            if !same_registration(existing, desired) {
                return Err(metadata_error(
                    "native registration conflicts with committed identity",
                ));
            }
            // Never demote, erase a birth or remove a data assignment on retry.
            return Ok(None);
        }
        let pending = self
            .state
            .log
            .iter()
            .find(|entry| !entry.leadership_barrier)
            .cloned();
        let mut node = desired.clone();
        let mut now_ms = ingress.now_ms();
        if let Some(entry) = &pending {
            let saved = entry
                .topology
                .nodes
                .get(&desired.id)
                .ok_or_else(|| metadata_error("another native CONFIG is pending"))?;
            if !same_registration(saved, desired)
                || saved.metadata_role != MetadataMemberRole::Learner
                || saved.native_store_birth_v1.is_some()
                || entry.restore_activation.is_some()
                || entry.term != self.state.current_term
            {
                return Err(metadata_error("conflicting native registration is pending"));
            }
            node = saved.clone();
            now_ms = entry
                .topology
                .history
                .back()
                .ok_or_else(|| metadata_error("pending registration audit absent"))?
                .at_ms;
        }
        let mut plan = crate::DistributionStore::native_registration_plan(
            config.clone(),
            self.state.committed_topology.clone(),
        )?;
        plan.join_metadata_learner(node, &self.state.node_id, now_ms)?;
        let proposed = plan.topology().clone();
        current.require_live_request(business)?;
        ingress.token().check()?;
        let entry = if let Some(pending) = pending {
            if pending.topology != proposed {
                return Err(metadata_error(
                    "pending CONFIG is not this exact registration body",
                ));
            }
            self.refsync_identical_native_pending(&pending)?;
            pending
        } else {
            self.propose_topology(proposed)?
        };
        ingress.token().check()?;
        Ok(Some(entry))
    }
}
