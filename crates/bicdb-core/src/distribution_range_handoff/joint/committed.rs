//! Actual protected local decision plus an independently fresh committed MQ
//! publication. This is a historical joint decision, not local materialization,
//! current data quorum, serving permission or permission to drop a freeze.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::ActualServiceNativeJointCommittedEvidence;

pub(crate) struct CommittedNativeJointDecisionObservation {
    native: crate::db::NativeJointDecisionReceipt,
    metadata: HostMetadataReadObservation,
    business: Uuid,
    database_incarnation: String,
}
impl CommittedNativeJointDecisionObservation {
    pub(crate) fn from_actual_service(
        evidence: ActualServiceNativeJointCommittedEvidence,
    ) -> Result<Self> {
        let (native, metadata, business, database_incarnation) = evidence.into_parts();
        let result = Self {
            native,
            metadata,
            business,
            database_incarnation,
        };
        result.require_metadata_current()?;
        Ok(result)
    }
    pub(super) fn require_metadata_current(&self) -> Result<()> {
        self.metadata.require_live_request(self.business)?;
        let data = self.native.data();
        let topology = self.metadata.committed_topology(self.business)?;
        data.require_metadata_domain(topology, self.metadata.restore_activation(self.business)?)?;
        let record = topology
            .relocations
            .get(&data.promise().relocation_id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| invalid("joint decision is not committed in actual metadata"))?;
        if record.stage() < crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            || record.decision_sha256() != data.checksum_sha256()
            || record.session_id() != data.promise().session_id
            || record.nonce() != data.promise().nonce
            || record.range_index() != data.range_index()
            || record.logical_genesis_sha256() != data.logical_genesis_sha256()
            || record.logical_history_sha256() != data.logical_history_sha256()
            || record.old_configuration_sha256() != data.promise().old_config_sha256
            || record.new_configuration_sha256() != data.promise().new_config_sha256
            || topology
                .range_by_id(data.old_range().id)
                .is_none_or(|r| canonical_range(r) != *data.activation_range())
            || !data.new_members().contains_key(self.native.local_node_id())
            || self.metadata.requester_node_id() != self.native.local_node_id()
            || self.metadata.requester_incarnation()
                != data.new_members()[self.native.local_node_id()].incarnation
        {
            return Err(invalid(
                "actual committed joint observation changed point/new membership/requester",
            ));
        }
        let (term, index) = self.native.metadata_anchor();
        if self.metadata.term() < term || self.metadata.read_index() < index {
            return Err(invalid(
                "joint committed MQ regressed behind protected native decision",
            ));
        }
        Ok(())
    }
    /// Request-bound metadata/domain check only, never data visibility.
    pub(crate) fn require_current_metadata_only(&self) -> Result<()> {
        self.require_metadata_current()
    }
    pub(crate) fn require_current_with_historical_cut_scope(
        &self,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
    ) -> Result<()> {
        self.require_metadata_current()?;
        scope.require_committed_identity(self)
    }
    pub(crate) fn require_current_with_publication_scope(
        &self,
        scope: &crate::db::NativeJointPreparedPublicationScope<'_>,
    ) -> Result<()> {
        self.require_metadata_current()?;
        scope.require_committed_identity(self)
    }
    pub(crate) fn require_historical_audit_with_publication_scope(
        &self,
        scope: &crate::db::NativeJointPreparedPublicationScope<'_>,
    ) -> Result<()> {
        self.metadata
            .require_historical_request_not_canceled(self.business)?;
        scope.require_committed_identity(self)
    }
    pub(crate) fn require_current_with_native_read_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
    ) -> Result<()> {
        self.require_metadata_current()?;
        scope.require_committed_identity(self)
    }
    pub(crate) fn require_historical_audit_with_native_read_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
    ) -> Result<()> {
        self.metadata
            .require_historical_request_not_canceled(self.business)?;
        scope.require_committed_identity(self)
    }
    pub(crate) fn require_current(&self, database: &crate::BicDb) -> Result<()> {
        self.require_metadata_current()?;
        self.require_durable_fact_current(database)
    }
    /// Historical protected local decision/open fact ONLY. This deliberately
    /// grants no current metadata permission; a distinct fresh phase must
    /// validate its own actual MQ before combining this with physical facts.
    pub(crate) fn require_durable_fact_current(&self, database: &crate::BicDb) -> Result<()> {
        self.metadata
            .require_historical_request_not_canceled(self.business)?;
        if self.native.native_database_incarnation() != self.database_incarnation {
            return Err(invalid(
                "committed joint observation actual DB-open binding changed",
            ));
        }
        self.native.require_current(database)
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.native.data()
    }
    pub(crate) fn native_receipt(&self) -> &crate::db::NativeJointDecisionReceipt {
        &self.native
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.metadata.term(),
            self.metadata.read_index(),
            self.metadata.context_id(),
        )
    }
    pub(crate) fn deadline(&self) -> std::time::Instant {
        self.metadata.deadline()
    }
}
