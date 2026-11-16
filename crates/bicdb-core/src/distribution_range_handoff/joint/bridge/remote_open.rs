//! Actual recipient preparation/native-bridge custody under a fresh request.
//! Neither a wire point nor a historical metadata record constructs this owner.
use super::*;

pub(crate) struct NativeRemoteOpenObservation {
    pub(super) local: Arc<NativeJointPreparedServingObservation>,
    pub(super) fresh: CommittedNativeJointDecisionObservation,
    pub(super) native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    vote: NativeJointPreparedServingVoteData,
}
impl NativeJointPreparationCustody {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn observe_remote_open(
        self: &Arc<Self>,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeRemoteOpenObservation> {
        let candidate = self.fresh_candidate(client, metadata_leader, parent)?;
        let fresh = candidate
            .fresh
            .ok_or_else(|| invalid("remote Open fresh actual observation absent"))?;
        let local = Arc::new(NativeJointPreparedServingObservation {
            actual: candidate.actual,
            service: candidate.service,
            fresh: None,
            custody: candidate.custody,
        });
        let prepared = local.actual.prepared();
        let (term, index, context) = fresh.metadata_anchor();
        let vote = NativeJointPreparedServingVoteData {
            preparation_sha256: prepared.checksum_sha256().into(),
            native_database_incarnation: prepared.native_database_incarnation().into(),
            actual_local_visibility_sequence: prepared.actual_local_visibility_sequence(),
            metadata_term: term,
            metadata_read_index: index,
            metadata_context_id: context,
        };
        let result = NativeRemoteOpenObservation {
            local,
            fresh,
            native,
            vote,
        };
        result.require_current()?;
        Ok(result)
    }
}
impl NativeRemoteOpenObservation {
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.fresh.require_publication_point_memory()?;
        self.local
            .custody
            .as_ref()
            .ok_or_else(|| invalid("remote Open original custody absent"))?
            .require_actual(&self.local.actual)
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_memory_current()?;
        let service = self
            .local
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote Open original service busy"))?;
        self.local
            .require_open_phase_current(&service, &self.fresh, self.native.clone())?;
        self.require_memory_current()
    }
    pub(crate) fn require_participant(&self, sha: &str) -> Result<()> {
        self.require_memory_current()?;
        self.local
            .actual
            .require_remote_preparation_participant(sha)
    }
    pub(crate) fn require_native_receipt(
        &self,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        self.require_memory_current()?;
        if self.fresh.native_receipt().checksum_sha256() != receipt.checksum_sha256()
            || self.fresh.data() != receipt.data()
        {
            return Err(invalid(
                "remote Open replaced exact original native decision",
            ));
        }
        Ok(())
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        self.fresh.metadata_anchor()
    }
    pub(crate) fn vote(&self) -> &NativeJointPreparedServingVoteData {
        &self.vote
    }
    /// The actual evidence performs all physical work and retains its scope
    /// through the final memory-only metadata publication boundary.
    pub(crate) fn inspect<T>(
        &self,
        inspect: impl FnOnce(
            &crate::db::NativeJointBridgeReadScope<'_>,
            &crate::distribution_range_consensus::RangeWriteStore,
            String,
            Option<String>,
            Uuid,
        ) -> Result<T>,
        final_memory: impl FnOnce() -> Result<()>,
    ) -> Result<T> {
        self.require_memory_current()?;
        let service = self
            .local
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote Open protected original service busy"))?;
        let custody = self
            .local
            .custody
            .as_ref()
            .ok_or_else(|| invalid("remote Open original custody absent"))?;
        custody.require_actual(&self.local.actual)?;
        custody.require_service(&self.local.service)?;
        self.local.actual.inspect_remote_open_with_custody(
            &service,
            &self.fresh,
            self.native.clone(),
            custody,
            inspect,
            final_memory,
        )
    }
}
