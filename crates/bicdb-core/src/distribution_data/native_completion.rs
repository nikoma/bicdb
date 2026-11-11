//! Same actual service-owned store. All authority/network acquisition precedes
//! this latch; DB acquisition is nonwaiting and the owned preparation survives.
use super::*;
use crate::distribution_range_consensus::{
    LocalNativeCompletionEvidence, PreparedNativeRecoveryReady,
};
use crate::distribution_range_recovery::{
    ClosedNativeReadyQuorum, NativeCompletionRequest, RecoveryObservationRoleAdmission,
    RecoveryRoleAdmission,
};
impl ClusterDataNodeService {
    pub(crate) fn complete_local_native_recovery(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        quorum: ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        parent.check()?;
        request.scope.require_role(fresh)?;
        let db = try_database_read(&self.db)?;
        self.range_writes
            .complete_native_recovery_frozen(&db, prepared, quorum, fresh)?;
        // A canceled/lost response after durability never erases the completed
        // refusal. A fresh readonly request can attest the retained proof.
        parent.check()?;
        self.range_writes
            .observe_native_completion_for_role(&db, prepared, fresh, request)
    }
    pub(crate) fn observe_local_native_completion(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        parent.check()?;
        let db = try_database_read(&self.db)?;
        let evidence = self
            .range_writes
            .observe_native_completion_for_observer(&db, prepared, observer, request)?;
        parent.check()?;
        Ok(evidence)
    }
}
