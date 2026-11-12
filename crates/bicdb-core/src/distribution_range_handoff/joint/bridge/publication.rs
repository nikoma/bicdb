//! Actual native bridge publication on the NEW majority precedes raw/open
//! activation. These opaque facts cannot be decoded from quorum audit data.
use super::*;

pub(crate) struct NativeJointServingPublicationRound {
    prepared: Arc<NativeJointPreparedServingQuorum>,
    receipts: BTreeMap<ClusterNodeId, Arc<crate::db::NativeJointServingBridgeReceipt>>,
}
pub(crate) struct NativeJointPublishedServingQuorum {
    prepared: Arc<NativeJointPreparedServingQuorum>,
    receipts: BTreeMap<ClusterNodeId, Arc<crate::db::NativeJointServingBridgeReceipt>>,
}
impl NativeJointPreparedServingQuorum {
    pub(crate) fn serving_publication_round(
        self: &Arc<Self>,
    ) -> Result<NativeJointServingPublicationRound> {
        self.require_current()?;
        Ok(NativeJointServingPublicationRound {
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
        let receipt = admission.publish_local_native_bridge(parent)?;
        let node = receipt.local_node_id().clone();
        if receipt.data() != self.prepared.data()
            || self
                .receipts
                .get(&node)
                .is_some_and(|old| old.checksum_sha256() != receipt.checksum_sha256())
        {
            return Err(invalid(
                "native bridge publication changed decision or local native history",
            ));
        }
        self.receipts.insert(node, Arc::new(receipt));
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointPublishedServingQuorum> {
        self.prepared.require_current()?;
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
            prepared: self.prepared,
            receipts: self.receipts,
        };
        result.require_current()?;
        Ok(result)
    }
}
impl NativeJointPublishedServingQuorum {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.prepared.data()
    }
    fn require_current(&self) -> Result<()> {
        self.prepared.cancellation.check()?;
        for (node, receipt) in &self.receipts {
            let service = self.prepared.observations[node]
                .service
                .try_lock()
                .ok_or_else(|| invalid("native bridge majority service busy"))?;
            service.require_actual_native_joint_bridge(receipt)?;
        }
        Ok(())
    }
    pub(crate) fn acquire_local_open(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointOpenAdmission> {
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
        let fresh = {
            let service = local
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint open metadata service busy"))?;
            service.observe_committed_native_joint_decision(
                metadata,
                business,
                self.data().old_range().id,
            )?
        };
        let cancellation = cancellation.child_with_deadline(fresh.deadline());
        let result = NativeJointOpenAdmission {
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
    quorum: Arc<NativeJointPublishedServingQuorum>,
    pub(super) local: Arc<NativeJointPreparedServingObservation>,
    receipt: Arc<crate::db::NativeJointServingBridgeReceipt>,
    fresh: CommittedNativeJointDecisionObservation,
    cancellation: crate::CancellationToken,
}
impl NativeJointOpenAdmission {
    pub(crate) fn persist_local_catalogue_activation(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt> {
        parent.check()?;
        self.require_external_publications_current()?;
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
    pub(crate) fn native_receipt(&self) -> Arc<crate::db::NativeJointServingBridgeReceipt> {
        self.receipt.clone()
    }
    pub(crate) fn cancellation(&self) -> &crate::CancellationToken {
        &self.cancellation
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
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
                service.require_actual_native_joint_bridge(receipt)?;
            }
        }
        Ok(())
    }
}
