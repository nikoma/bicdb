//! Actual protected catch-up ACK + independently fresh leader MQ precede a
//! normal old/new-quorum CONFIG. No installed counter/DTO creates this proof.
use super::*;

pub(crate) struct NativeLearnerPromotionReadiness {
    node: ClusterNodeId,
    reply: MetadataAppendResponse,
    topology: ClusterTopology,
    term: u64,
    index: u64,
    source_store: Uuid,
    metadata_context: Uuid,
    cancellation: crate::CancellationToken,
}
impl NativeLearnerPromotionReadiness {
    pub(crate) fn receiver_store_id(&self) -> Uuid {
        self.reply
            .native_receiver
            .as_ref()
            .unwrap()
            .metadata_store_id()
    }
}
impl MetadataConsensusStore {
    pub(crate) fn observe_native_learner_promotion_readiness(
        &self,
        current: &HostMetadataReadObservation,
        business: Uuid,
        target: &ClusterNodeId,
        reply: &MetadataAppendResponse,
        parent: &crate::CancellationToken,
    ) -> Result<NativeLearnerPromotionReadiness> {
        parent.check()?;
        self.require_native_learner_enrollment_leader(current, business)?;
        let member = self
            .state
            .committed_topology
            .nodes
            .get(target)
            .ok_or_else(|| metadata_error("promotion target absent"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || member.metadata_role != MetadataMemberRole::Learner
            || !known_born_v2(&self.state.committed_topology, target)
            || !reply.success
            || reply.term != self.state.current_term
            || reply.node_id != *target
            || self.state.commit_index == 0
            || reply.match_index < self.state.commit_index
            || self.match_index.get(target).copied().unwrap_or(0) < self.state.commit_index
        {
            return Err(metadata_error(
                "promotion requires actual caught-up protected learner ACK",
            ));
        }
        self.require_current_native_append_ack(target, reply)?;
        if reply
            .native_receiver
            .as_ref()
            .is_none_or(|r| r.metadata_store_id().is_nil())
        {
            return Err(metadata_error(
                "promotion requires actual protected receiver StoreUUID",
            ));
        }
        crate::distribution_range_consensus::validate_native_json_size(
            &self.state.committed_topology,
            2 * 1024 * 1024,
        )?;
        Ok(NativeLearnerPromotionReadiness {
            node: target.clone(),
            reply: reply.clone(),
            topology: self.state.committed_topology.clone(),
            term: self.state.current_term,
            index: self.state.commit_index,
            source_store: self.registered_native_metadata_member_store_id()?,
            metadata_context: current.context_id(),
            cancellation: parent.child_with_deadline(current.deadline()),
        })
    }
    pub(crate) fn propose_native_learner_promotion(
        &mut self,
        proof: &NativeLearnerPromotionReadiness,
        current: &HostMetadataReadObservation,
        business: Uuid,
        config: &crate::DistributionConfig,
        now_ms: u64,
        parent: &crate::CancellationToken,
    ) -> Result<Option<MetadataLogEntry>> {
        proof.cancellation.check()?;
        parent.check()?;
        self.require_native_learner_enrollment_leader(current, business)?;
        if current.context_id() == proof.metadata_context {
            return Err(metadata_error(
                "promotion needs independently fresh post-ACK metadata context",
            ));
        }
        if self.registered_native_metadata_member_store_id()? != proof.source_store
            || self.state.current_term != proof.term
        {
            return Err(metadata_error(
                "promotion catch-up belongs to another source owner/term",
            ));
        }
        let target = self
            .state
            .committed_topology
            .nodes
            .get(&proof.node)
            .ok_or_else(|| metadata_error("promotion target disappeared"))?;
        if target.metadata_role == MetadataMemberRole::Voter {
            if proof.topology.nodes.get(&proof.node).is_none_or(|old| {
                old.native_store_birth_v1 != target.native_store_birth_v1
                    || old.incarnation != target.incarnation
                    || old.tls_certificate_sha256 != target.tls_certificate_sha256
                    || old.pending_tls_certificate_sha256 != target.pending_tls_certificate_sha256
                    || target.lifecycle != ClusterNodeLifecycle::Active
            }) {
                return Err(metadata_error(
                    "already promoted member differs from actual protected catch-up identity",
                ));
            }
            return Ok(None); // Fresh committed status only; no new mutation.
        }
        if self.state.committed_topology != proof.topology
            || self.state.commit_index != proof.index
            || self.match_index.get(&proof.node).copied().unwrap_or(0) < proof.index
        {
            return Err(metadata_error(
                "promotion actual catch-up/configuration changed",
            ));
        }
        self.require_current_native_append_ack(&proof.node, &proof.reply)?;
        let pending = self
            .state
            .log
            .iter()
            .find(|e| !e.leadership_barrier)
            .cloned();
        let timestamp = if let Some(p) = &pending {
            if p.term != self.state.current_term || p.restore_activation.is_some() {
                return Err(metadata_error("conflicting pending metadata control"));
            }
            p.topology
                .history
                .back()
                .ok_or_else(|| metadata_error("pending promotion audit absent"))?
                .at_ms
        } else {
            now_ms
        };
        let mut plan = crate::DistributionStore::native_registration_plan(
            config.clone(),
            self.state.committed_topology.clone(),
        )?;
        plan.promote_metadata_learner(&proof.node, &self.state.node_id, timestamp)?;
        let proposed = plan.topology().clone();
        proof.cancellation.check()?;
        current.require_live_request(business)?;
        let entry = if let Some(p) = pending {
            if p.topology != proposed {
                return Err(metadata_error(
                    "pending CONFIG is not this exact learner promotion",
                ));
            }
            self.refsync_identical_native_pending(&p)?;
            p
        } else {
            self.propose_topology(proposed)?
        };
        parent.check()?;
        proof.cancellation.check()?;
        Ok(Some(entry))
    }
}
