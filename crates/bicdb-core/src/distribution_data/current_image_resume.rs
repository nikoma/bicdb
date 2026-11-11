//! Local half of exact interrupted-image refusal observations. Fresh metadata
//! acquisition and range draining are Runtime work outside this service latch.
use super::*;
use crate::distribution_range_consensus::{
    CurrentImageInstallingAdmission, NativeCurrentImageProposalPointer, RecoveryFenceScope,
};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{CurrentImageInstallingEvidence, RecoveryRoleAdmission};
impl ClusterDataNodeService {
    pub(crate) fn preflight_current_image_installing(
        &mut self,
        role: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        pointer: &NativeCurrentImageProposalPointer,
        parent: &CancellationToken,
    ) -> Result<CurrentImageInstallingAdmission> {
        parent.check()?;
        role.require_live_request()?;
        let db = try_database_read(&self.db)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .admit_current_image_installing_observation(&db, &native, role, scope, pointer)
    }
    pub(crate) fn observe_current_image_installing(
        &self,
        admission: &CurrentImageInstallingAdmission,
        role: &RecoveryRoleAdmission,
        frozen: &FrozenRangeMutationPermit,
        challenge: uuid::Uuid,
        parent: &CancellationToken,
    ) -> Result<CurrentImageInstallingEvidence> {
        parent.check()?;
        role.require_live_request()?;
        let db = try_database_read(&self.db)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        let observed = self
            .range_writes
            .observe_current_image_installing(&db, &native, role, admission, frozen, challenge)?;
        parent.check()?;
        role.require_live_request()?;
        Ok(observed)
    }
}
