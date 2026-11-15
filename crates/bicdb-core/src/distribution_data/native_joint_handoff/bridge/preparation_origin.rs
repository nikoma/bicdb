//! Producer-sealed lifetime of an ACTUALLY completed native preparation.
//! It is distinct from a finite metadata request and grants no current authority.
use super::*;

pub(in crate::distribution_data::native_joint_handoff) struct PreparingOrigin {
    lifetime: ProducerLifetime,
}
enum ProducerLifetime {
    Original(CancellationToken),
    #[cfg(feature = "tls-replication")]
    Remote(Arc<crate::distribution_native_host::NativeRemotePreparationProcess>),
}
impl ProducerLifetime {
    fn require_memory(&self) -> Result<()> {
        match self {
            Self::Original(session) => session.check(),
            #[cfg(feature = "tls-replication")]
            Self::Remote(process) => process.require_memory(),
        }
    }
    fn bound_request(&self, parent: &CancellationToken) -> CancellationToken {
        match self {
            Self::Original(session) => parent.child_linked_to(session),
            #[cfg(feature = "tls-replication")]
            Self::Remote(process) => process.bound_request(parent),
        }
    }
    fn memory_lifetime(&self) -> crate::cancellation::CancellationLifetime {
        match self {
            Self::Original(session) => session.retain_memory_lifetime(),
            #[cfg(feature = "tls-replication")]
            Self::Remote(process) => process.memory_lifetime(),
        }
    }
}
pub(in crate::distribution_data::native_joint_handoff) struct CompletedPreparationOrigin {
    lifetime: ProducerLifetime,
    preparation_sha256: String,
    decision_sha256: String,
    local_decision_sha256: String,
    database_incarnation: String,
    node: ClusterNodeId,
}
impl PreparingOrigin {
    /// Only actual service producers call this BEFORE their native effects.
    pub(in crate::distribution_data::native_joint_handoff) fn begin(
        parent: &CancellationToken,
    ) -> Self {
        Self {
            lifetime: ProducerLifetime::Original(parent.child()),
        }
    }
    #[cfg(feature = "tls-replication")]
    pub(in crate::distribution_data::native_joint_handoff) fn begin_remote(
        process: Arc<crate::distribution_native_host::NativeRemotePreparationProcess>,
    ) -> Result<Self> {
        process.require_memory()?;
        Ok(Self {
            lifetime: ProducerLifetime::Remote(process),
        })
    }
    /// The actual native receipt is available only after successful native
    /// preparation/readback. No error/status/serialized body can seal this.
    pub(in crate::distribution_data::native_joint_handoff) fn seal(
        self,
        prepared: &crate::db::NativeJointServingPreparation,
        committed: &CommittedNativeJointDecisionObservation,
    ) -> CompletedPreparationOrigin {
        CompletedPreparationOrigin {
            lifetime: self.lifetime,
            preparation_sha256: prepared.checksum_sha256().into(),
            decision_sha256: committed.data().checksum_sha256().into(),
            local_decision_sha256: committed.native_receipt().checksum_sha256().into(),
            database_incarnation: prepared.native_database_incarnation().into(),
            node: prepared.local_node_id().clone(),
        }
    }
}
impl CompletedPreparationOrigin {
    pub(super) fn require_session(&self) -> Result<()> {
        self.lifetime.require_memory()
    }
    pub(super) fn bound_request(&self, parent: &CancellationToken) -> CancellationToken {
        self.lifetime.bound_request(parent)
    }
    pub(super) fn memory_lifetime(&self) -> crate::cancellation::CancellationLifetime {
        self.lifetime.memory_lifetime()
    }
    #[cfg(feature = "tls-replication")]
    pub(super) fn require_remote_participant(&self, sha: &str) -> Result<()> {
        match &self.lifetime {
            ProducerLifetime::Remote(process) => process.require_participant(sha),
            _ => Err(data_error(
                "remote preparation observation cannot cast a legacy/local producer",
            )),
        }
    }
    pub(super) fn require_actual(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
    ) -> Result<()> {
        self.require_session()?;
        #[cfg(feature = "tls-replication")]
        if let ProducerLifetime::Remote(process) = &self.lifetime {
            process.require_prepared(&actual.prepared)?;
        }
        actual.committed.require_captured_native_open_identity()?;
        if self.preparation_sha256 != actual.prepared.checksum_sha256()
            || self.decision_sha256 != actual.committed.data().checksum_sha256()
            || self.local_decision_sha256 != actual.committed.native_receipt().checksum_sha256()
            || self.database_incarnation != actual.prepared.native_database_incarnation()
            || self.database_incarnation
                != actual
                    .committed
                    .native_receipt()
                    .native_database_incarnation()
            || self.node != *actual.prepared.local_node_id()
        {
            return Err(data_error(
                "completed preparation changed original native receipt/open/point",
            ));
        }
        Ok(())
    }
}

/// The strict original-request path stays unchanged. Only an opaque custody
/// over this exact successfully returned actual owner selects the second path.
pub(super) enum PreparedHistory<'a> {
    OriginalRequest,
    Custody(&'a crate::distribution_range_handoff::NativeJointPreparationCustody),
}
impl PreparedHistory<'_> {
    pub(super) fn require_collection_origin(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
    ) -> Result<()> {
        match self {
            Self::OriginalRequest => actual.committed.require_historical_request_memory()?,
            Self::Custody(_) => self.require_origin(actual)?,
        }
        // This is the actual producer-sealed Session, not a supplied new parent.
        actual.origin.require_actual(actual)
    }
    fn require_origin(&self, actual: &ActualServiceNativeJointPreparedEvidence) -> Result<()> {
        match self {
            Self::OriginalRequest => Ok(()),
            Self::Custody(custody) => {
                custody.require_actual_evidence(actual)?;
                actual.origin.require_actual(actual)
            }
        }
    }
    pub(super) fn require_current(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
        db: &BicDb,
    ) -> Result<()> {
        match self {
            Self::OriginalRequest => actual.committed.require_durable_fact_current(db),
            Self::Custody(_) => {
                self.require_origin(actual)?;
                // Exact protected CURRENT/archive/native birth/open proof,
                // without treating the old canceled finite OWN as authority.
                actual.committed.native_receipt().require_current(db)
            }
        }
    }
    pub(super) fn require_publication_scope(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
        scope: &crate::db::NativeJointPreparedPublicationScope<'_>,
    ) -> Result<()> {
        match self {
            Self::OriginalRequest => actual
                .committed
                .require_historical_audit_with_publication_scope(scope),
            Self::Custody(_) => {
                self.require_origin(actual)?;
                scope.require_committed_identity(&actual.committed)
            }
        }
    }
    pub(super) fn require_open_scope(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
    ) -> Result<()> {
        match self {
            Self::OriginalRequest => actual
                .committed
                .require_historical_audit_with_native_read_scope(scope),
            Self::Custody(_) => {
                self.require_origin(actual)?;
                scope.require_committed_identity(&actual.committed)
            }
        }
    }
}
