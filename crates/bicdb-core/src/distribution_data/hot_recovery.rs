//! Same-service/actual-store producer; no DTO or recovered bit can make a fact.
use super::*;
use crate::distribution_range_consensus::{
    NativeHotRecoveredOpen, RangeWriteCommand, RecoveryFenceScope,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
impl ClusterDataNodeService {
    pub(crate) fn observe_hot_recovered_open(
        &self,
        role: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<NativeHotRecoveredOpen> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        let fact = self
            .range_writes
            .observe_hot_recovered_open(&db, role, scope, command, parent)?;
        parent.check()?;
        Ok(fact)
    }
    pub(crate) fn revalidate_hot_recovered_open(
        &self,
        fact: &NativeHotRecoveredOpen,
        range: &RangeDescriptor,
    ) -> Result<()> {
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_writer_host_for_range(&db, range)?;
        fact.require_current(&db, &self.range_writes, range)
    }
}
