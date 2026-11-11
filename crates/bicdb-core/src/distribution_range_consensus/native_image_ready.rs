//! Exact read-only image materialization observation while its install refusal
//! remains. Only the opaque actual Runtime finalization can enter this path.
use super::*;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_image_install::FinalizedNativeRecoveryImage;
use crate::distribution_range_image_scope::RangeImageSourcePurposeV1;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

/// An actual immutable catalogue observation, not a DTO/counter authority.
/// It includes irreversible markers and every refusal, not only the range R.
#[derive(Debug)]
pub(crate) struct FinalizedNativeImageCatalogueObservation {
    store_open_id: Uuid,
    range_id: RangeId,
    catalogue_sha256: String,
}
impl FinalizedNativeImageCatalogueObservation {
    pub(crate) fn require_current(&self, store: &RangeWriteStore, range_id: RangeId) -> Result<()> {
        store.require_durable_recovery_catalog()?;
        if self.store_open_id != store.open_id
            || self.range_id != range_id
            || self.catalogue_sha256 != store.recovery_range_catalog_sha256(range_id)?
        {
            return Err(range_write_error(
                "finalized image catalogue markers/refusals/open changed",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
    pub(crate) fn observe_finalizing_native_image_catalogue(
        &self,
        actual: &crate::distribution_range_image_install::NativeImageCatalogueFinalization,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<FinalizedNativeImageCatalogueObservation> {
        actual.require_current(self, role, frozen)?;
        self.require_durable_recovery_catalog()?;
        let range_id = actual.catalogue().range_id();
        Ok(FinalizedNativeImageCatalogueObservation {
            store_open_id: self.open_id,
            range_id,
            catalogue_sha256: self.recovery_range_catalog_sha256(range_id)?,
        })
    }
    /// No numeric observation constructor, owner override, intent removal or
    /// floor advancement. Ordinary NativeReady still requires NoImage.
    pub(crate) fn verify_finalized_native_image_catalogue(
        &self,
        image: &FinalizedNativeRecoveryImage,
        database: &crate::BicDb,
        role: &RecoveryRoleAdmission,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        role.require_live_request()?;
        let scope = image.scope();
        let native = image.native_admission();
        let fence = image.intent_fence();
        let physical = image.base();
        image
            .catalogue_observation()
            .require_current(self, scope.range_id)?;
        if !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || !matches!(scope.purpose, MetadataRecoveryRolePurpose::NormalRecovery)
            || fence.purpose() != ImageInstallPurpose::CurrentVoterRepair
            || !matches!(
                fence.source_purpose(),
                RangeImageSourcePurposeV1::CurrentVoterRepair
            )
        {
            return Err(range_write_error(
                "only an actual finalized Normal current-voter repair can prepare",
            ));
        }
        scope.require_role(role)?;
        native.require_current(database)?;
        physical.require_current(database, native)?;
        fence.require_live_role(&NativeImageInstallRole::CurrentVoterRepair(role))?;
        let saved = self.require_image_install_recovery_scope(role, native)?;
        database
            .range_mutation_gate()
            .validate_frozen(scope.range_id, frozen)?;
        let intent = self
            .catalog
            .image_install_intents
            .get(&scope.range_id)
            .ok_or_else(|| {
                range_write_error("finalized image observation lost its exact install refusal")
            })?;
        intent.validate()?;
        if !self.fsync
            || self.root.canonicalize()? != database.data_path().canonicalize()?
            || !saved.same_session(scope)
            || frozen.owner() != scope.freeze_owner()?
            || fence.database_incarnation() != native.database_incarnation()
            || native.cluster_id() != fence.cluster_id()
            || native.bootstrap_id() != fence.bootstrap_id()
            || native.local_node_id() != fence.target_node_id()
            || native.local_member_incarnation() != fence.target_member_incarnation()
            || native.birth_id() != fence.target_birth_id()
            || native.birth_sha256() != fence.target_birth_sha256()
            || intent.sha256()? != fence.intent_sha256()
            || database.capture_range_image_bindings()?.namespace_ids()
                != fence.target_namespace_ids()
            || self
                .catalog
                .ranges
                .get(&scope.range_id)
                .is_none_or(|log| log.current_epoch != scope.range_epoch)
        {
            return Err(range_write_error(
                "finalized image scope/store/native birth/UID/returned owner changed",
            ));
        }
        image
            .catalogue()
            .require_binding_current(self, physical, fence)
    }
}
