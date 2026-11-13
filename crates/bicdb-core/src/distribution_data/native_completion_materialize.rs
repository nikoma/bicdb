//! Local completed-decision checks, never a role/QQ built from a DTO.
use super::*;
use crate::distribution_range_recovery::CompletedNativeCommandAdmission;
impl ClusterDataNodeService {
    pub(crate) fn require_completed_native_command(
        &self,
        proof: &CompletedNativeCommandAdmission,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        proof.require_live()?;
        let role = proof.role();
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .require_completed_retained_command(&native, proof)?;
        proof.require_live()?;
        parent.check()
    }
}
