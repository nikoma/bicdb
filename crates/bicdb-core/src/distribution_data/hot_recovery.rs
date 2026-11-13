//! Same-service/actual-store producer; no DTO or recovered bit can make a fact.
use super::*;
use crate::distribution_range_consensus::{
    NativeHotRecoveredOpen, RangeWriteCommand, RecoveryFenceScope,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
impl ClusterDataNodeService {
    pub(crate) fn observe_hot_known_pending_replica_prefix(
        &self,
        db: &BicDb,
        role: &RecoveryRoleAdmission,
    ) -> Result<Option<crate::db::KnownPendingNativeReplicaPrefix>> {
        self.require_native_writer_host_for_range(db, role.current_range())?;
        let slot = self.range_writes.guard();
        slot.require_live()?;
        let known =
            db.observe_known_pending_native_replica_prefix_for_role(&self.range_writes, role)?;
        slot.require_live()?;
        role.require_live_request()?;
        Ok(known)
    }

    pub(crate) fn revalidate_hot_known_pending_replica_prefix(
        &self,
        db: &BicDb,
        known: &crate::db::KnownPendingNativeReplicaPrefix,
    ) -> Result<()> {
        let slot = self.range_writes.guard();
        slot.require_live()?;
        known.require_current(db, &self.range_writes)?;
        slot.require_live()
    }
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
        let slot = self.range_writes.guard();
        slot.require_live()?;
        let fact = self
            .range_writes
            .observe_hot_recovered_open(&db, role, scope, command, slot, parent)?;
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
