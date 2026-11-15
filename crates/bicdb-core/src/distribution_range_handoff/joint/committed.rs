//! Actual protected local decision plus an independently fresh committed MQ
//! publication. This is a historical joint decision, not local materialization,
//! current data quorum, serving permission or permission to drop a freeze.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::ActualServiceNativeJointCommittedEvidence;
use std::sync::Arc;

pub(crate) struct CommittedNativeJointDecisionObservation {
    native: crate::db::NativeJointDecisionReceipt,
    metadata: HostMetadataReadObservation,
    business: Uuid,
    database_incarnation: String,
    metadata_point: Option<Arc<crate::distribution_data::ActualNativeJointMetadataPoint>>,
    closed_writer_domain: Option<crate::db::NativeJointWriterDomain>,
}
impl CommittedNativeJointDecisionObservation {
    pub(crate) fn from_actual_service(
        evidence: ActualServiceNativeJointCommittedEvidence,
    ) -> Result<Self> {
        let (
            native,
            metadata,
            business,
            database_incarnation,
            metadata_point,
            closed_writer_domain,
        ) = evidence.into_parts();
        let result = Self {
            native,
            metadata,
            business,
            database_incarnation,
            metadata_point,
            closed_writer_domain,
        };
        result.require_metadata_current()?;
        match &result.closed_writer_domain {
            None => {}
            #[cfg(feature = "tls-replication")]
            Some(crate::db::NativeJointWriterDomain::Closed(actual))
                if actual.data() == result.data() => {}
            _ => {
                return Err(invalid(
                    "committed CLOSED historical owner is not the original exact protected domain",
                ))
            }
        }
        Ok(result)
    }
    pub(super) fn require_metadata_current(&self) -> Result<()> {
        self.metadata.require_live_request(self.business)?;
        if let Some(point) = &self.metadata_point {
            point.require_request(&self.metadata, self.business)?;
        }
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
    /// Necessary captured-open equality only. This neither makes an old
    /// metadata request live nor validates a protected record/current grant.
    pub(crate) fn require_captured_native_open_identity(&self) -> Result<()> {
        if self.native.native_database_incarnation() != self.database_incarnation {
            return Err(invalid(
                "committed joint observation actual DB-open binding changed",
            ));
        }
        Ok(())
    }
    /// The actual service producer alone can retain this owner. Native/metadata
    /// records cannot construct it; ordinary/Installed observations keep None.
    pub(crate) fn closed_writer_domain(&self) -> Option<&crate::db::NativeJointWriterDomain> {
        self.closed_writer_domain.as_ref()
    }
    pub(crate) fn require_native_publication_metadata(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_metadata_current()?;
        match &self.metadata_point {
            Some(point) => point.require_native_store_point(db, store),
            None if self.native.native_metadata_initialization_version() == 0 => Ok(()),
            None => Err(invalid("protected native joint publication requires an actual current-MStore version captured outside engine locks")),
        }
    }
    pub(crate) fn require_publication_request_memory(&self) -> Result<()> {
        self.metadata.require_live_request(self.business)
    }
    pub(crate) fn native_publication_guard(
        &self,
    ) -> Result<Option<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>>>
    {
        match &self.metadata_point {
            Some(point) => point.try_publication_guard().map(Some),
            None if self.native.native_metadata_initialization_version() == 0 => Ok(None),
            None => Err(invalid(
                "protected native joint publication has no actual local metadata version gate",
            )),
        }
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
    #[cfg(test)]
    pub(crate) fn test_request_memory(&self) -> serde_json::Value {
        let now = std::time::Instant::now();
        serde_json::json!({"context_id":self.metadata.context_id(),
            "term":self.metadata.term(),"read_index":self.metadata.read_index(),
            "requester":self.metadata.requester_node_id(),
            "cancel_requested":self.metadata.test_original_request_cancellation().is_cancel_requested(),
            "remaining_us":self.deadline().saturating_duration_since(now).as_micros(),
            "past_deadline_us":now.saturating_duration_since(self.deadline()).as_micros(),
            "memory_observation_not_authority":true})
    }
    /// A cfg(test) read-only memory probe survives consumption, without
    /// exposing the actual cancelable request or any metadata permission.
    #[cfg(test)]
    pub(crate) fn test_request_memory_probe(&self) -> Box<dyn Fn() -> serde_json::Value> {
        let token = self.metadata.test_original_request_cancellation();
        let context = self.metadata.context_id();
        let term = self.metadata.term();
        let index = self.metadata.read_index();
        let deadline = self.deadline();
        Box::new(move || {
            let now = std::time::Instant::now();
            serde_json::json!({"context_id":context,"term":term,"read_index":index,
                "cancel_requested":token.is_cancel_requested(),
                "remaining_us":deadline.saturating_duration_since(now).as_micros(),
                "past_deadline_us":now.saturating_duration_since(deadline).as_micros(),
                "memory_observation_not_authority":true})
        })
    }
    pub(crate) fn writer_domain_metadata_audit(&self) -> Result<serde_json::Value> {
        self.require_metadata_current()?;
        self.metadata
            .joint_writer_domain_audit(self.business, self.data())
    }
}
