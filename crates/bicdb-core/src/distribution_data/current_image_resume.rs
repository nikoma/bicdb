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

impl ClusterDataNodeService {
    pub(crate) fn validate_current_image_installing(
        &self,
        admission: &CurrentImageInstallingAdmission,
        role: &RecoveryRoleAdmission,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let db = try_database_read(&self.db)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        admission.require_current(&self.range_writes, &db, &native, role)
    }
    pub(crate) fn begin_current_image_resume_history(
        &self,
        authority: Arc<crate::distribution_range_recovery::FreshCurrentImageResumeAuthority>,
        source: &crate::db::LocalNativeRecoveryReady,
    ) -> Result<crate::distribution_range_recovery::CurrentImageResumeHistoryRound> {
        let db = try_database_read(&self.db)?;
        crate::distribution_range_recovery::CurrentImageResumeHistoryRound::begin(
            authority,
            source,
            &db,
            &self.range_writes,
        )
    }
    pub(crate) fn advance_current_image_resume_history(
        &self,
        round: &mut crate::distribution_range_recovery::CurrentImageResumeHistoryRound,
        source: &crate::db::LocalNativeRecoveryReady,
        parent: &CancellationToken,
    ) -> Result<bool> {
        let db = try_database_read(&self.db)?;
        round.advance(source, &db, &self.range_writes, parent)
    }
    pub(crate) fn finish_current_image_resume_history(
        &self,
        round: crate::distribution_range_recovery::CurrentImageResumeHistoryRound,
        source: &crate::db::LocalNativeRecoveryReady,
    ) -> Result<crate::distribution_range_image::CertifiedRecoveryImageCut> {
        let db = try_database_read(&self.db)?;
        let verified = round.finish(source, &db, &self.range_writes)?;
        crate::distribution_range_image::CertifiedRecoveryImageCut::from_verified_resumed_source_history(verified,source,&db,&self.range_writes)
    }
}
