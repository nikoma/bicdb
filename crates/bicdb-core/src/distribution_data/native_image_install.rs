//! Same-store local image operations. Callers acquire fresh metadata roles and
//! source authority BEFORE entering the outer data-service latch. These methods
//! perform one bounded local phase; they neither acquire network authority nor
//! sleep/retry a database latch, and never open another RangeWriteStore.
use super::*;
use crate::db::NativeTargetImageVerificationProgress;
use crate::distribution_range_consensus::{ImageInstallPurpose, NativeImageInstallRole};
use crate::distribution_range_image::{
    CompletedNativeRangeImageSource, RangeImageHistoryStepV1, RangeImageStepV1,
    TentativeNativeRangeImageSourceAuthorization,
};
use crate::distribution_range_image_install::{
    AdoptedNativeRangeImageInstall, NativeImageCatalogueFinalization, NativeImageInstallFreeze,
    NativeRangeImageInstaller, NativeRangeImageVerification, VerifiedNativeRangeImageInstall,
};

impl ClusterDataNodeService {
    #[cfg(test)]
    pub(crate) fn test_corrupt_adopted_image_marker(
        &mut self,
        scope: &crate::distribution_range_consensus::RecoveryFenceScope,
        index: u64,
        checksum: String,
    ) -> Result<()> {
        // Negative durable-corruption injection, NOT a legal authority path.
        self.range_writes
            .test_corrupt_compacted_marker(scope, index, checksum)
    }

    /// The sealed actual image return is consumed under this SAME catalogue.
    /// Its intent remains present throughout physical attestation; preparation
    /// atomically exchanges that exact refusal for the durable Prepared phase.
    pub(crate) fn prepare_finalized_native_recovery_image(
        &mut self,
        image: crate::distribution_range_image_install::FinalizedNativeRecoveryImage,
        role: crate::distribution_range_recovery::RecoveryRoleAdmission,
        frozen: Arc<crate::distribution_range_mutation::FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::PreparedNativeRecoveryReady> {
        parent.check()?;
        role.require_live_request()?;
        if role.cluster_id() != &self.cluster_id || role.local_node_id() != &self.node_id {
            return Err(data_error(
                "finalized image preparation belongs to another service",
            ));
        }
        let db = try_database_read(&self.db)?;
        let physical = db.attest_finalized_native_recovery_image(
            image,
            role,
            &self.range_writes,
            frozen,
            parent,
        )?;
        self.range_writes
            .prepare_native_recovery_ready(&db, physical)
    }

    /// Inspect this service's actual source catalogue without opening another
    /// writer. Fresh quorum authority is acquired by the caller beforehand.
    pub(crate) fn begin_native_recovery_source_history(
        &self,
        authority: Arc<crate::distribution_range_recovery::FreshRangeRecoveryAuthority>,
        source: &crate::db::LocalNativeRecoveryReady,
        target: ClusterNodeId,
    ) -> Result<crate::distribution_range_recovery::RecoverySourceHistoryRound> {
        let db = try_database_read(&self.db)?;
        crate::distribution_range_recovery::RecoverySourceHistoryRound::begin(
            authority,
            source,
            target,
            &db,
            &self.range_writes,
        )
    }

    pub(crate) fn advance_native_recovery_source_history(
        &self,
        round: &mut crate::distribution_range_recovery::RecoverySourceHistoryRound,
        source: &crate::db::LocalNativeRecoveryReady,
        parent: &CancellationToken,
    ) -> Result<bool> {
        let db = try_database_read(&self.db)?;
        round.advance(source, &db, &self.range_writes, parent)
    }

    pub(crate) fn finish_native_recovery_source_history(
        &self,
        round: crate::distribution_range_recovery::RecoverySourceHistoryRound,
        source: &crate::db::LocalNativeRecoveryReady,
    ) -> Result<crate::distribution_range_image::CertifiedRecoveryImageCut> {
        let db = try_database_read(&self.db)?;
        let history = round.finish(source, &db, &self.range_writes)?;
        crate::distribution_range_image::CertifiedRecoveryImageCut::from_verified_source_history(
            history,
            source,
            &db,
            &self.range_writes,
        )
    }

    /// Actual source physical proof from this service's already-persisted
    /// ACTIVE session. It grants no cluster quorum, serving, or caller floor.
    pub(crate) fn attest_native_recovery_ready(
        &self,
        role: crate::distribution_range_recovery::RecoveryRoleAdmission,
        frozen: Arc<crate::distribution_range_mutation::FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<crate::db::LocalNativeRecoveryReady> {
        parent.check()?;
        role.require_live_request()?;
        if role.cluster_id() != &self.cluster_id || role.local_node_id() != &self.node_id {
            return Err(data_error(
                "native source observation belongs to another service",
            ));
        }
        let db = try_database_read(&self.db)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        let scope = crate::distribution_range_consensus::RecoveryFenceScope::from_role(&role)?;
        let catalog = self
            .range_writes
            .observe_recovery_resolved_prefix(&role, &native, &scope)?;
        db.attest_local_native_recovery_ready(
            native,
            role,
            catalog,
            &self.range_writes,
            frozen,
            parent,
        )
    }

    fn require_native_image_local_role(&self, role: &NativeImageInstallRole<'_>) -> Result<()> {
        role.role().require_live_request()?;
        if role.role().cluster_id() != &self.cluster_id
            || role.role().local_node_id() != &self.node_id
        {
            return Err(data_error(
                "native image operation belongs to another actual data service",
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_native_range_image_install(
        &mut self,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: impl Into<NativeImageInstallFreeze>,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<NativeRangeImageInstaller> {
        cancellation.check()?;
        self.require_native_image_local_role(role)?;
        let native = {
            let db = try_database_read(&self.db)?;
            let actual = role.role();
            db.native_recovery_admission(
                actual.cluster_id(),
                actual.bootstrap_id(),
                actual.local_node_id(),
                actual.local_member_incarnation(),
            )?
        };
        let installer = NativeRangeImageInstaller::begin(
            Arc::clone(&self.db),
            &mut self.range_writes,
            native,
            source,
            role,
            frozen,
            purpose,
            attempt_id,
        )?;
        cancellation.check()?;
        Ok(installer)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_completed_native_range_image_install(
        &mut self,
        source: CompletedNativeRangeImageSource,
        role: &NativeImageInstallRole<'_>,
        frozen: impl Into<NativeImageInstallFreeze>,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<NativeRangeImageVerification> {
        cancellation.check()?;
        self.require_native_image_local_role(role)?;
        let native = {
            let db = try_database_read(&self.db)?;
            let actual = role.role();
            db.native_recovery_admission(
                actual.cluster_id(),
                actual.bootstrap_id(),
                actual.local_node_id(),
                actual.local_member_incarnation(),
            )?
        };
        NativeRangeImageInstaller::recover_completed(
            Arc::clone(&self.db),
            &mut self.range_writes,
            native,
            source,
            role,
            frozen,
            purpose,
            attempt_id,
            cancellation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn stage_native_range_image_history(
        &self,
        installer: &mut NativeRangeImageInstaller,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        stage_id: Uuid,
        step: &RangeImageHistoryStepV1,
        cancellation: &CancellationToken,
    ) -> Result<crate::db::NativeHistoryStageProgress> {
        self.require_native_image_local_role(role)?;
        installer.stage_history_step(
            &self.range_writes,
            role,
            source,
            stage_id,
            step,
            cancellation,
        )
    }

    pub(crate) fn audit_native_range_image_history(
        &self,
        installer: &mut NativeRangeImageInstaller,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        max_entries: usize,
        cancellation: &CancellationToken,
    ) -> Result<crate::db::NativeHistoryStageProgress> {
        self.require_native_image_local_role(role)?;
        installer.audit_history_step(&self.range_writes, role, source, max_entries, cancellation)
    }

    pub(crate) fn clear_native_range_image_step(
        &self,
        installer: &mut NativeRangeImageInstaller,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        self.require_native_image_local_role(role)?;
        installer.clear_step(&self.range_writes, role, source, cancellation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn copy_native_range_image_step(
        &self,
        installer: &mut NativeRangeImageInstaller,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        attempt_id: Uuid,
        step: &RangeImageStepV1,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.require_native_image_local_role(role)?;
        installer.install_step(
            attempt_id,
            &self.range_writes,
            role,
            source,
            step,
            cancellation,
        )
    }

    pub(crate) fn begin_native_range_image_verification(
        &self,
        installer: NativeRangeImageInstaller,
        role: &NativeImageInstallRole<'_>,
        source: CompletedNativeRangeImageSource,
        cancellation: &CancellationToken,
    ) -> Result<NativeRangeImageVerification> {
        self.require_native_image_local_role(role)?;
        installer.begin_verification(&self.range_writes, role, source, cancellation)
    }

    pub(crate) fn advance_native_range_image_verification(
        &self,
        verification: &mut NativeRangeImageVerification,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<NativeTargetImageVerificationProgress> {
        self.require_native_image_local_role(role)?;
        verification.advance(&self.range_writes, role, cancellation)
    }

    pub(crate) fn finish_native_range_image_verification(
        &self,
        verification: NativeRangeImageVerification,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedNativeRangeImageInstall> {
        self.require_native_image_local_role(role)?;
        verification.finish(&self.range_writes, role, cancellation)
    }

    pub(crate) fn adopt_native_range_image_catalogue(
        &mut self,
        verified: VerifiedNativeRangeImageInstall,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<AdoptedNativeRangeImageInstall> {
        self.require_native_image_local_role(role)?;
        verified.adopt_catalogue(&mut self.range_writes, role, cancellation)
    }

    pub(crate) fn finalize_native_range_image_catalogue(
        &self,
        adopted: AdoptedNativeRangeImageInstall,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NativeImageInstallFreeze, NativeImageCatalogueFinalization)> {
        self.require_native_image_local_role(role)?;
        adopted.into_finalization(&self.range_writes, role, cancellation)
    }
}
