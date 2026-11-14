//! Producer-sealed lifetime of an ACTUALLY completed native preparation.
//! It is distinct from a finite metadata request and grants no current authority.
use super::*;

pub(in crate::distribution_data::native_joint_handoff) struct PreparingOrigin {
    session: CancellationToken,
}
pub(in crate::distribution_data::native_joint_handoff) struct CompletedPreparationOrigin {
    session: CancellationToken,
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
            session: parent.child(),
        }
    }
    /// The actual native receipt is available only after successful native
    /// preparation/readback. No error/status/serialized body can seal this.
    pub(in crate::distribution_data::native_joint_handoff) fn seal(
        self,
        prepared: &crate::db::NativeJointServingPreparation,
        committed: &CommittedNativeJointDecisionObservation,
    ) -> CompletedPreparationOrigin {
        CompletedPreparationOrigin {
            session: self.session,
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
        self.session.check()
    }
    pub(super) fn bound_request(&self, parent: &CancellationToken) -> CancellationToken {
        parent.child_linked_to(&self.session)
    }
    pub(super) fn memory_lifetime(&self) -> crate::cancellation::CancellationLifetime {
        self.session.retain_memory_lifetime()
    }
    pub(super) fn require_actual(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
    ) -> Result<()> {
        self.require_session()?;
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
