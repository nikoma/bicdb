//! Actual native bridge publication on the NEW majority precedes raw/open
//! activation. These opaque facts cannot be decoded from quorum audit data.
use super::*;
mod open_phase;
mod release_phase;
pub(crate) use open_phase::{NativeJointOpenPhaseQuorum, NativeJointOpenPhaseRound};
pub(crate) use release_phase::{
    NativeJointReleaseAdmission, NativeJointReleaseContinuation, NativeJointReleasePhaseQuorum,
    NativeJointReleasePhaseRound, NativeJointServingPolicyAdmission,
};

pub(crate) struct NativeJointServingPublicationRound {
    fresh_required: bool,
    pending: std::collections::BTreeSet<ClusterNodeId>,
    phase: Option<Arc<NativeJointPreparationPhaseQuorum>>,
    prepared: Arc<NativeJointPreparedServingQuorum>,
    receipts: BTreeMap<ClusterNodeId, Arc<crate::db::NativeJointServingBridgeReceipt>>,
}
pub(crate) struct NativeJointPublishedServingQuorum {
    phase: Option<Arc<NativeJointPreparationPhaseQuorum>>,
    prepared: Arc<NativeJointPreparedServingQuorum>,
    receipts: BTreeMap<ClusterNodeId, Arc<crate::db::NativeJointServingBridgeReceipt>>,
}
impl NativeJointPreparedServingQuorum {
    pub(crate) fn serving_publication_round(
        self: &Arc<Self>,
    ) -> Result<NativeJointServingPublicationRound> {
        self.require_current()?;
        Ok(NativeJointServingPublicationRound {
            fresh_required: false,
            pending: std::collections::BTreeSet::new(),
            phase: None,
            prepared: self.clone(),
            receipts: BTreeMap::new(),
        })
    }
}
impl NativeJointPreparedServingQuorum {
    /// Aggregation only, not a permission to publish from expired facts.
    /// Every admitted member must carry a DISTINCT fresh full phase quorum.
    pub(crate) fn resume_native_publication(
        self: &Arc<Self>,
    ) -> Result<NativeJointServingPublicationRound> {
        self.require_original_caller()?;
        Ok(NativeJointServingPublicationRound {
            fresh_required: true,
            pending: std::collections::BTreeSet::new(),
            phase: None,
            prepared: self.clone(),
            receipts: BTreeMap::new(),
        })
    }
}
impl NativeJointServingPublicationRound {
    pub(crate) fn publish_actual_local_member(
        &mut self,
        admission: &NativeJointActivationAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.prepared, &admission.quorum) {
            return Err(invalid(
                "native bridge publication changed its actual prepared quorum",
            ));
        }
        if self.fresh_required && admission.phase.is_none() {
            return Err(invalid(
                "resumed native publication requires new actual full phase authority",
            ));
        }
        admission.claim_publication_attempt()?;
        let local = admission.local.local_node_id().clone();
        if admission.phase.is_some() && !self.receipts.contains_key(&local) {
            // ONE scoped native producer reconciles an actual old pointer or
            // publishes only if actually absent. Do not spend another complete
            // sweep on a separate absence probe after an unknown attempt.
            self.pending.insert(local);
            let receipt = admission.publish_local_native_bridge(parent)?;
            self.record_actual_receipt(admission, receipt)?;
            return Ok(());
        }
        // Unknown local completion is retained across phase expiration. A new
        // phase MUST check actual protected storage before another WAL attempt.
        if self.pending.contains(&local) || self.receipts.contains_key(&local) {
            if let Some(receipt) = admission.recover_local_native_bridge(parent)? {
                self.record_actual_receipt(admission, receipt)?;
                return Ok(());
            }
            if self.receipts.contains_key(&local) {
                return Err(invalid("previously counted native publication disappeared"));
            }
        }
        self.pending.insert(local);
        let receipt = admission.publish_local_native_bridge(parent)?;
        self.record_actual_receipt(admission, receipt)
    }
    fn record_actual_receipt(
        &mut self,
        admission: &NativeJointActivationAdmission,
        receipt: crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        let node = receipt.local_node_id().clone();
        if receipt.data() != self.prepared.data()
            || node != *admission.local.local_node_id()
            || self
                .receipts
                .get(&node)
                .is_some_and(|old| old.checksum_sha256() != receipt.checksum_sha256())
        {
            return Err(invalid(
                "native bridge publication changed decision or local native history",
            ));
        }
        let retired = {
            let _version = admission.committed().native_publication_guard()?;
            let old_phase = std::mem::replace(&mut self.phase, admission.phase.clone());
            self.pending.remove(&node);
            let old_receipt = self.receipts.insert(node, Arc::new(receipt));
            (old_phase, old_receipt)
        };
        drop(retired);
        Ok(())
    }
    /// Actual native producer succeeds, then the test loses its reply before
    /// aggregation. A subsequent distinct phase must recover the same audit.
    #[cfg(test)]
    pub(crate) fn test_lose_actual_publication_reply(
        &mut self,
        admission: &NativeJointActivationAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<String> {
        if !Arc::ptr_eq(&self.prepared, &admission.quorum) || admission.phase.is_none() {
            return Err(invalid("lost-reply control needs same actual fresh phase"));
        }
        admission.claim_publication_attempt()?;
        self.pending.insert(admission.local.local_node_id().clone());
        let actual = admission.publish_local_native_bridge(parent)?;
        let checksum = actual.checksum_sha256().to_owned();
        drop(actual);
        Ok(checksum)
    }
    /// One complete aggregation boundary under distinct current authority.
    /// Each SAME actual participant/preparation and protected native receipt is
    /// revalidated here; returning it does not rewrite any receipt or old TTL.
    pub(crate) fn finish_with_fresh_phase(
        self,
        phase: Arc<NativeJointPreparationPhaseQuorum>,
    ) -> Result<NativeJointPublishedServingQuorum> {
        if !self.pending.is_empty()
            || !Arc::ptr_eq(&self.prepared, &phase.original_quorum())
            || self
                .receipts
                .keys()
                .ne(self.prepared.data.new_members().keys())
        {
            return Err(invalid("fresh publication finish changed/omitted original NEW participants or unresolved local attempt"));
        }
        phase.require_live()?;
        for (node, receipt) in &self.receipts {
            let service = self.prepared.observations[node]
                .service
                .try_lock()
                .ok_or_else(|| invalid("publication finish service busy"))?;
            phase.require_member_with_service(node, &service)?;
            service.require_joint_bridge_with_writer_domain(
                receipt,
                self.prepared.observations[node].actual.writer_domain(),
            )?;
        }
        phase.require_live()?;
        Ok(NativeJointPublishedServingQuorum {
            phase: Some(phase),
            prepared: self.prepared,
            receipts: self.receipts,
        })
    }
    pub(crate) fn finish(self) -> Result<NativeJointPublishedServingQuorum> {
        if !self.pending.is_empty() {
            return Err(invalid(
                "native publication retains unresolved actual local attempts",
            ));
        }
        if let Some(phase) = &self.phase {
            phase.require_current()?;
        } else {
            self.prepared.require_current()?;
        }
        if self.fresh_required && self.receipts.len() != self.prepared.data.new_members().len() {
            return Err(invalid(
                "resumed joint publication cannot omit any final NEW replica",
            ));
        }
        if self.receipts.len() < self.prepared.data.new_members().len() / 2 + 1
            || !self
                .receipts
                .contains_key(&self.prepared.data.activation_range().leader)
        {
            return Err(invalid(
                "native serving bridge must actually persist on a NEW majority including leader",
            ));
        }
        let result = NativeJointPublishedServingQuorum {
            phase: self.phase,
            prepared: self.prepared,
            receipts: self.receipts,
        };
        result.require_current()?;
        Ok(result)
    }
}
impl NativeJointPublishedServingQuorum {
    #[cfg(test)]
    pub(crate) fn test_actual_receipt_checksums(&self) -> Result<BTreeMap<ClusterNodeId, String>> {
        // Historical diagnostics only; these exact actual receipts are NOT
        // current authority merely because their immutable hashes are readable.
        Ok(self
            .receipts
            .iter()
            .map(|(id, r)| (id.clone(), r.checksum_sha256().to_owned()))
            .collect())
    }
    #[cfg(test)]
    pub(crate) fn test_actual_published_members(&self) -> Result<Vec<ClusterNodeId>> {
        self.require_current()?;
        Ok(self.receipts.keys().cloned().collect())
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.prepared.data()
    }
    fn require_current(&self) -> Result<()> {
        if let Some(phase) = &self.phase {
            phase.require_current()?;
        } else {
            self.prepared.cancellation.check()?;
        }
        for (node, receipt) in &self.receipts {
            let service = self.prepared.observations[node]
                .service
                .try_lock()
                .ok_or_else(|| invalid("native bridge majority service busy"))?;
            service.require_joint_bridge_with_writer_domain(
                receipt,
                self.prepared.observations[node].actual.writer_domain(),
            )?;
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_local_open(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointOpenAdmission> {
        if self.phase.is_some() {
            return Err(invalid("fresh native publication remains frozen: distinct catalogue/open/release phase recovery is not yet supported"));
        }
        self.require_current()?;
        let actor = client.local_metadata_read_participant();
        let receipt = self
            .receipts
            .get(actor.node_id())
            .ok_or_else(|| {
                invalid("joint open requires an actual locally published native bridge")
            })?
            .clone();
        if self.data().new_members()[actor.node_id()].incarnation != actor.member_incarnation() {
            return Err(invalid("joint open actual transport incarnation changed"));
        }
        let local = self.prepared.observations[actor.node_id()].clone();
        let cancellation = parent.child_with_deadline(
            self.prepared
                .cancellation
                .deadline()
                .ok_or_else(|| invalid("native published quorum has no original deadline"))?,
        );
        cancellation.check()?;
        let remaining = cancellation
            .deadline()
            .and_then(|deadline| deadline.checked_duration_since(std::time::Instant::now()))
            .ok_or_else(|| invalid("native published quorum budget expired"))?
            .min(std::time::Duration::from_secs(5));
        let business = Uuid::now_v7();
        // Real post-publication MQ, with no service/database latch held.
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            remaining,
            &cancellation,
        )?;
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &local.service,
            &metadata,
            business,
            &cancellation,
        )?;
        let fresh = {
            let service = local
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint open metadata service busy"))?;
            local.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                self.data().old_range().id,
                metadata_point,
            )?
        };
        let cancellation = cancellation.child_with_deadline(fresh.deadline());
        let result = NativeJointOpenAdmission {
            open_phase: None,
            attempted: std::sync::atomic::AtomicBool::new(false),
            quorum: self.clone(),
            local,
            receipt,
            fresh,
            cancellation,
        };
        result.require_live()?;
        Ok(result)
    }
}

/// Actual fresh post-publication authority plus independently published NEW
/// native quorum. No metadata/counter payload can construct this admission.
pub(crate) struct NativeJointOpenAdmission {
    open_phase: Option<Arc<NativeJointOpenPhaseQuorum>>,
    attempted: std::sync::atomic::AtomicBool,
    quorum: Arc<NativeJointPublishedServingQuorum>,
    pub(super) local: Arc<NativeJointPreparedServingObservation>,
    receipt: Arc<crate::db::NativeJointServingBridgeReceipt>,
    fresh: CommittedNativeJointDecisionObservation,
    cancellation: crate::CancellationToken,
}
impl NativeJointOpenAdmission {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn retain_registered_release_owner(
        &self,
        activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    ) -> Result<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner> {
        self.require_live()?;
        if activation.native().data() != self.data()
            || activation.native_bridge_sha256() != self.receipt.checksum_sha256()
        {
            return Err(invalid(
                "Registered release changed original actual Open activation",
            ));
        }
        self.local
            .actual
            .retain_registered_release_owner(self.local.service.clone(), activation)
    }
    /// Retain only the actual image target lease for proof-authorized final
    /// release after all preparation/publication owners have drained.
    pub(crate) fn retain_image_release_owner(
        &self,
        activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    ) -> Result<crate::distribution_data::NativeJointImageReleaseOwner> {
        self.require_live()?;
        if activation.native().data() != self.data()
            || activation.native().checksum_sha256() != self.receipt.checksum_sha256()
        {
            return Err(invalid(
                "joint image release changed the original local activation",
            ));
        }
        self.local
            .actual
            .retain_image_release_owner(self.local.service.clone(), activation)
    }
    pub(crate) fn persist_local_catalogue_activation(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt> {
        parent.check()?;
        self.require_live()?;
        if self.open_phase.is_some()
            && self
                .attempted
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(invalid(
                "fresh open admission already attempted activation/recovery",
            ));
        }
        if self.open_phase.is_none() {
            self.require_external_publications_current()?;
        }
        let mut service = self
            .local
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint catalogue activation local service busy"))?;
        service.persist_local_native_joint_activation(self, &self.local.actual, parent)
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.quorum.data()
    }
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        &self.fresh
    }
    pub(crate) fn local_preparation(&self) -> &crate::db::NativeJointServingPreparation {
        self.local.actual.prepared()
    }
    pub(crate) fn native_receipt(&self) -> Arc<crate::db::NativeJointServingBridgeReceipt> {
        self.receipt.clone()
    }
    pub(crate) fn cancellation(&self) -> &crate::CancellationToken {
        &self.cancellation
    }
    pub(crate) fn original_writer_domain_for_activation(
        &self,
        database: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<crate::db::NativeJointWriterDomain> {
        self.require_live()?;
        self.local
            .actual
            .require_original_phase_owner(database, store)?;
        let domain = self.local.actual.writer_domain().clone();
        domain.require_current(database, store, self.data().activation_range())?;
        self.require_live()?;
        Ok(domain)
    }
    pub(crate) fn original_phase_owner_for_activation(
        &self,
        database: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<(crate::db::NativeJointPhaseOwner, std::path::PathBuf)> {
        self.require_live()?;
        let actual = self
            .local
            .actual
            .require_original_phase_owner(database, store)?;
        let (original, root) = self.local.actual.original_phase_owner_for_continuation()?;
        if !actual.same_actual(&original) {
            return Err(invalid("joint activation changed preparation writer"));
        }
        self.require_live()?;
        Ok((original, root))
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.fresh.require_metadata_current()?;
        if let Some(phase) = &self.open_phase {
            phase.require_post_quorum(&self.fresh)?;
        }
        if self.fresh.data() != self.data()
            || self.fresh.native_receipt().local_node_id() != self.receipt.local_node_id()
        {
            return Err(invalid(
                "joint open changed its actual committed/native local decision",
            ));
        }
        let (term, index, context) = self.fresh.metadata_anchor();
        for receipt in self.quorum.receipts.values() {
            let (observed_term, observed_index, observed_context) = receipt.metadata_anchor();
            if term < observed_term || index < observed_index || context == observed_context {
                return Err(invalid(
                    "joint open metadata was not acquired after actual native NEW publication",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn require_external_publications_current(&self) -> Result<()> {
        self.require_live()?;
        for (node, receipt) in &self.quorum.receipts {
            if node != self.receipt.local_node_id() {
                let service = self.quorum.prepared.observations[node]
                    .service
                    .try_lock()
                    .ok_or_else(|| invalid("joint open native peer service busy"))?;
                if let Some(phase) = &self.open_phase {
                    phase.require_member_with_service(node, &service)?;
                } else {
                    service.require_joint_bridge_with_writer_domain(
                        receipt,
                        self.quorum.prepared.observations[node]
                            .actual
                            .writer_domain(),
                    )?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn require_local_native_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_live()?;
        scope.require_native(&self.receipt)?;
        if let Some(phase) = &self.open_phase {
            phase.require_local_native_scope(self.receipt.local_node_id(), store, scope)?;
        }
        Ok(())
    }
    pub(crate) fn has_fresh_open_phase(&self) -> bool {
        self.open_phase.is_some()
    }
    pub(crate) fn require_local_open_preparation(
        &self,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        self.require_live()?;
        if let Some(phase) = &self.open_phase {
            phase.require_member_with_service(self.receipt.local_node_id(), service)?;
        }
        Ok(())
    }
}
