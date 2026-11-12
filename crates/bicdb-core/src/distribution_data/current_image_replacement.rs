//! Bounded SAME-STORE half of Installing existing-decision recovery. No RPC or
//! fresh metadata acquisition happens under these service/DB latches.
use super::*;
use crate::distribution_range_consensus::{
    InstallingCommittedAdmission, NativeCurrentImageProposalPointer,
};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{InstallingCommittedDecision, RecoveryRoleAdmission};
impl ClusterDataNodeService {
    pub(crate) fn preflight_installing_committed(
        &self,
        role: &RecoveryRoleAdmission,
        pointer: &NativeCurrentImageProposalPointer,
        parent: &CancellationToken,
    ) -> Result<InstallingCommittedAdmission> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .preflight_installing_committed_refusal(&db, &native, role, pointer)
    }
    pub(crate) fn observe_installing_committed(
        &self,
        role: &RecoveryRoleAdmission,
        admission: &InstallingCommittedAdmission,
        frozen: &FrozenRangeMutationPermit,
        challenge: Uuid,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::LocalInstallingCommittedFence> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        let out = self
            .range_writes
            .observe_installing_committed_fence(&db, &native, role, admission, frozen, challenge)?;
        parent.check()?;
        Ok(out)
    }
    pub(crate) fn certify_installing_existing_committed(
        &mut self,
        decision: &InstallingCommittedDecision,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::LocalInstallingCommittedCertification> {
        parent.check()?;
        decision.require_live()?;
        let role = decision.authority().role();
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_mutations
            .validate_controlled(role.current_range().id, &controlled)?;
        let out = self
            .range_writes
            .certify_existing_installing_decision(&db, &native, role, decision, frozen)?;
        parent.check()?;
        decision.require_live()?;
        drop(db);
        drop(controlled);
        Ok(out)
    }
}
impl ClusterDataNodeService {
    pub(crate) fn begin_current_image_replacement_history(
        &self,
        authority: Arc<crate::distribution_range_recovery::FreshInstallingCommittedAuthority>,
        source: &crate::db::LocalNativeRecoveryReady,
    ) -> Result<crate::distribution_range_recovery::CurrentImageReplacementHistoryRound> {
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        crate::distribution_range_recovery::CurrentImageReplacementHistoryRound::begin(
            authority,
            source,
            &db,
            &self.range_writes,
        )
    }
    pub(crate) fn advance_current_image_replacement_history(
        &self,
        round: &mut crate::distribution_range_recovery::CurrentImageReplacementHistoryRound,
        source: &crate::db::LocalNativeRecoveryReady,
        parent: &CancellationToken,
    ) -> Result<bool> {
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        round.advance(source, &db, &self.range_writes, parent)
    }
    pub(crate) fn finish_current_image_replacement_history(
        &self,
        round: crate::distribution_range_recovery::CurrentImageReplacementHistoryRound,
        source: &crate::db::LocalNativeRecoveryReady,
    ) -> Result<crate::distribution_range_image::CertifiedRecoveryImageCut> {
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let history = round.finish(source, &db, &self.range_writes)?;
        crate::distribution_range_image::CertifiedRecoveryImageCut::from_verified_replacement_source_history(history,source,&db,&self.range_writes)
    }
    pub(crate) fn handover_current_image_replacement(
        &mut self,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &crate::distribution_range_consensus::NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::InstallingRefusalHandoverReceipt> {
        parent.check()?;
        let r = role.role();
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            r.cluster_id(),
            r.bootstrap_id(),
            r.local_node_id(),
            r.local_member_incarnation(),
        )?;
        let out = self
            .range_writes
            .handover_installing_replacement_refusal(&db, &native, source, role, frozen, attempt)?;
        parent.check()?;
        Ok(out)
    }
    pub(crate) fn validate_current_image_replacement_handover(
        &self,
        receipt: &crate::distribution_range_consensus::InstallingRefusalHandoverReceipt,
        role: &RecoveryRoleAdmission,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        receipt.require_current(&self.range_writes, &db, &native, role)
    }
}

#[cfg(test)]
impl ClusterDataNodeService {
    pub(crate) fn test_replacement_proposal_before_handover(
        &mut self,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &crate::distribution_range_consensus::NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::InstallingRefusalHandoverReceipt> {
        parent.check()?;
        let r = role.role();
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            r.cluster_id(),
            r.bootstrap_id(),
            r.local_node_id(),
            r.local_member_incarnation(),
        )?;
        self.range_writes
            .test_replacement_proposal_before_handover(&db, &native, source, role, frozen, attempt)
    }
}

impl ClusterDataNodeService {
    pub(crate) fn resync_installing_committed_refusal(
        &mut self,
        role: &RecoveryRoleAdmission,
        admission: &InstallingCommittedAdmission,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .resync_installing_committed_refusal(&db, &native, role, admission, frozen)?;
        parent.check()
    }
}

#[cfg(test)]
impl ClusterDataNodeService {
    pub(crate) fn test_certify_installing_existing_committed_with(
        &mut self,
        decision: &InstallingCommittedDecision,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
        write: impl FnOnce(&std::path::Path, &[u8], bool) -> Result<()>,
    ) -> Result<crate::distribution_range_consensus::LocalInstallingCommittedCertification> {
        parent.check()?;
        decision.require_live()?;
        let role = decision.authority().role();
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_mutations
            .validate_controlled(role.current_range().id, &controlled)?;
        let out = self
            .range_writes
            .test_certify_existing_installing_decision_with(
                &db, &native, role, decision, frozen, write,
            )?;
        parent.check()?;
        decision.require_live()?;
        drop(db);
        drop(controlled);
        Ok(out)
    }
}
