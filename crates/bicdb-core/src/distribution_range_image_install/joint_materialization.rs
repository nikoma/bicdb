//! Sealed heterogeneous materialization views for joint-only operations. A
//! Registered view retains its actual original proof; it never becomes the
//! legacy local fence/capture finalization type.
use super::*;
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::NativeImageCatalogueFinalization {}
    #[cfg(feature = "tls-replication")]
    impl Sealed for std::sync::Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget> {}
}
pub(crate) trait NativeJointImageMaterialization: sealed::Sealed {
    fn base(&self) -> &NativeInstalledRangeImageBase;
    fn native_admission(&self) -> &NativeRecoveryAdmission;
    fn source_seal(&self) -> &crate::distribution_range_image::LocalRangeImageSealV1;
    fn require_materialized_fact_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()>;
    #[cfg(feature = "tls-replication")]
    fn registered_target(
        &self,
    ) -> Option<&std::sync::Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>>
    {
        None
    }
}
impl NativeJointImageMaterialization for NativeImageCatalogueFinalization {
    fn base(&self) -> &NativeInstalledRangeImageBase {
        NativeImageCatalogueFinalization::base(self)
    }
    fn native_admission(&self) -> &NativeRecoveryAdmission {
        NativeImageCatalogueFinalization::native_admission(self)
    }
    fn source_seal(&self) -> &crate::distribution_range_image::LocalRangeImageSealV1 {
        NativeImageCatalogueFinalization::source_seal(self)
    }
    fn require_materialized_fact_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        NativeImageCatalogueFinalization::require_materialized_fact_current(self, db, store, frozen)
    }
}
#[cfg(feature = "tls-replication")]
impl NativeJointImageMaterialization
    for std::sync::Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>
{
    fn base(&self) -> &NativeInstalledRangeImageBase {
        self.as_ref().base()
    }
    fn native_admission(&self) -> &NativeRecoveryAdmission {
        self.as_ref().native()
    }
    fn source_seal(&self) -> &crate::distribution_range_image::LocalRangeImageSealV1 {
        self.as_ref().source_seal()
    }
    fn require_materialized_fact_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.original_hook().require_current(db)?;
        self.require_materialized_point(db, store, frozen)?;
        self.original_hook().require_current(db)
    }
    fn registered_target(
        &self,
    ) -> Option<&std::sync::Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>>
    {
        Some(self)
    }
}
