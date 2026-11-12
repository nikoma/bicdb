//! Actual image materialization retained across the Runtime's A→N owner return.
//! This proof never clears an image intent or grants serving/normal writes.
use super::*;
use crate::distribution_range_consensus::{
    FinalizedNativeImageCatalogueObservation, NativeImageCatalogueAdoptionReceipt,
    RecoveryFenceScope,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

pub(crate) struct FinalizedNativeRecoveryImage {
    actual: NativeImageCatalogueFinalization,
    scope: RecoveryFenceScope,
    catalogue_observation: FinalizedNativeImageCatalogueObservation,
}

impl std::fmt::Debug for FinalizedNativeRecoveryImage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FinalizedNativeRecoveryImage")
            .field("range_id", &self.scope.range_id)
            .field("attempt_id", &self.actual.fence.attempt_id())
            .finish_non_exhaustive()
    }
}

impl NativeImageCatalogueFinalization {
    /// The only constructor for the returned materialization proof. It first
    /// validates the ACTUAL A-owned image/fence/native catalogue and only then
    /// performs the real owner rekey. No caller-selected expected-owner bypass.
    pub(crate) fn return_owned_image_to_recovery(
        self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        scope: &RecoveryFenceScope,
        frozen: &mut FrozenRangeMutationPermit,
        cancellation: &CancellationToken,
    ) -> Result<FinalizedNativeRecoveryImage> {
        cancellation.check()?;
        scope.require_role(role.role())?;
        self.require_current(store, role, frozen)?;
        let catalogue_observation =
            store.observe_finalizing_native_image_catalogue(&self, role, frozen)?;
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy returning actual image to recovery"))?;
        self.admission.require_current(&db)?;
        let saved = store.require_image_install_recovery_scope(role.role(), &self.admission)?;
        if !saved.same_session(scope)
            || self.catalogue.native_database_incarnation() != self.admission.database_incarnation()
            || self.catalogue.range_id() != scope.range_id
            || self.catalogue.nonce() != scope.recovery_nonce
            || self.catalogue.session_id() != scope.freeze_owner()?
        {
            return Err(invalid(
                "actual image return differs from its persisted recovery session",
            ));
        }
        db.range_mutation_gate()
            .validate_quiescent_frozen(scope.range_id, frozen)?;
        cancellation.check()?;
        frozen.try_rotate_owner(scope.freeze_owner()?)?;
        // No fallible operation after the rekey: ownership is returned exactly
        // once while the service latch and unique Frozen remain held. Every
        // later consumer independently revalidates a fresh current role.
        drop(db);
        Ok(FinalizedNativeRecoveryImage {
            actual: self,
            scope: scope.clone(),
            catalogue_observation,
        })
    }
}

impl FinalizedNativeRecoveryImage {
    pub(crate) fn catalogue_observation(&self) -> &FinalizedNativeImageCatalogueObservation {
        &self.catalogue_observation
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn intent_fence(&self) -> &TargetImageInstallFence {
        &self.actual.fence
    }
    pub(crate) fn base(&self) -> &NativeInstalledRangeImageBase {
        &self.actual.base
    }
    pub(crate) fn catalogue(&self) -> &NativeImageCatalogueAdoptionReceipt {
        &self.actual.catalogue
    }
    pub(crate) fn native_admission(&self) -> &NativeRecoveryAdmission {
        &self.actual.admission
    }

    /// Read-only current materialization under an independently fresh role.
    /// The actual retained intent is REQUIRED here, never temporarily cleared.
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        role: &RecoveryRoleAdmission,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.actual.admission.require_current(database)?;
        self.actual
            .base
            .require_current(database, &self.actual.admission)?;
        store.verify_finalized_native_image_catalogue(self, database, role, frozen)
    }
}
