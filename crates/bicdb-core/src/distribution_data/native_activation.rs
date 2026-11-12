//! Actual same-store local activation and final open-arm under nonwaiting DB
//! acquisition. Runtime owns physical pin/freeze release and does it LAST.
use super::*;
use crate::distribution_range_consensus::{NativeActivationReceipt, PreparedNativeRecoveryReady};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{
    NativeCompletionQuorum, NativeCompletionRequest, RecoveryRoleAdmission,
};
impl ClusterDataNodeService {
    pub(crate) fn activate_local_native_recovery(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        quorum: NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
        parent: &CancellationToken,
    ) -> Result<NativeActivationReceipt> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        if matches!(
            fresh.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            let slot = self.native_replica_listener_store_open_guard();
            self.range_writes
                .current_mut()
                .activate_native_recovery_owned(&db, prepared, quorum, fresh, &slot, parent)
        } else {
            self.range_writes
                .current_mut()
                .activate_native_recovery(&db, prepared, quorum, fresh)
        }
    }
    pub(crate) fn arm_local_native_activation(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        receipt: &NativeActivationReceipt,
        fresh: &RecoveryRoleAdmission,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        if matches!(
            fresh.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            let slot = self.native_replica_listener_store_open_guard();
            self.range_writes
                .current_mut()
                .arm_native_activation_open_owned(
                    &db, prepared, receipt, fresh, frozen, &slot, parent,
                )
        } else {
            self.range_writes
                .current_mut()
                .arm_native_activation_open(&db, prepared, receipt, fresh, frozen)
        }
    }
    pub(crate) fn require_current_native_activation(
        &self,
        request: &NativeCompletionRequest,
        fresh: &RecoveryRoleAdmission,
        quorum: &NativeCompletionQuorum,
    ) -> Result<()> {
        request.scope.require_role(fresh)?;
        if !self.native_completion_is_activated(request)? {
            return Err(data_error("no actual native activation"));
        }
        self.range_writes
            .require_current_native_activation_decision(quorum, fresh)
    }
}
