//! Actual-open admission lifetime/invalidation, not a quorum or read permit.
//! The Store can check this without reacquiring an outer database write guard.
use super::*;
use crate::distribution_range_consensus::{
    NativeActivationReceipt, PreparedNativeRecoveryReady, RangeWriteStore, RecoveryFenceScope,
};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeServingGeneration};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
#[derive(Debug)]
pub(crate) struct NativeActivatedOpenGuard {
    directory: std::sync::Weak<bicdb_page::DirectoryLock>,
    health: Arc<bicdb_page::DurabilityHealth>,
    generation: RangeServingGeneration,
    store_open_id: Uuid,
    scope: RecoveryFenceScope,
    activation_sha256: String,
}
impl NativeActivatedOpenGuard {
    pub(crate) fn require_current(
        &self,
        store_open: Uuid,
        scope: &RecoveryFenceScope,
        activation_sha: &str,
    ) -> Result<()> {
        if self.directory.upgrade().is_none()
            || self.store_open_id != store_open
            || !self.scope.same_session(scope)
            || self.activation_sha256 != activation_sha
        {
            return Err(BicDbError::Cluster(
                "native activation open/store/session changed".into(),
            ));
        }
        self.health
            .check()
            .map_err(crate::error::durability_error)?;
        self.generation.require_current()
    }
}
impl BicDb {
    pub(crate) fn native_activated_open_guard_in_scope(
        &self,
        scope: &crate::db::NativeActivationReadScope<'_>,
        receipt: &NativeActivationReceipt,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeActivatedOpenGuard> {
        scope.require_identity(self, scope.prepared(), scope.store(), scope.role())?;
        receipt.require_current_in_activation_scope(scope)?;
        self.check_recovery_state()?;
        let root = self._directory_lock.as_ref().ok_or_else(|| {
            BicDbError::Cluster("native activation has no actual DB directory lifetime".into())
        })?;
        let generation = self
            .range_mutations
            .bind_serving_generation(receipt.scope().range_id, frozen)?;
        // The existing generation guard is returned ONLY AFTER the callback's
        // actual scope exit checks. Error drops it without publishing an open.
        Ok(NativeActivatedOpenGuard {
            directory: Arc::downgrade(root),
            health: Arc::clone(&self.durability),
            generation,
            store_open_id: scope.store().recovery_store_open_id(),
            scope: receipt.scope().clone(),
            activation_sha256: receipt.activation_sha256()?,
        })
    }
    pub(crate) fn native_activated_open_guard(
        &self,
        store: &RangeWriteStore,
        prepared: &PreparedNativeRecoveryReady,
        receipt: &NativeActivationReceipt,
        fresh: &RecoveryRoleAdmission,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeActivatedOpenGuard> {
        receipt.require_current(self, store, prepared, fresh)?;
        self.check_recovery_state()?;
        let root = self._directory_lock.as_ref().ok_or_else(|| {
            BicDbError::Cluster("native activation has no actual DB directory lifetime".into())
        })?;
        let generation = self
            .range_mutations
            .bind_serving_generation(receipt.scope().range_id, frozen)?;
        Ok(NativeActivatedOpenGuard {
            directory: Arc::downgrade(root),
            health: Arc::clone(&self.durability),
            generation,
            store_open_id: store.recovery_store_open_id(),
            scope: receipt.scope().clone(),
            activation_sha256: receipt.activation_sha256()?,
        })
    }
}
