//! Actual sealed catalogue floor for an ORIGINAL Registered joint successor.
//! The constructor checks the original live point and complete durable candidate;
//! later checks permit only independently authorized joint metadata evolution.
use super::*;
pub(crate) struct NativeRegisteredRemoteCatalogueMaterialization {
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    base: NativeImageCatalogueBase,
    database_open: String,
    store_open: Uuid,
    catalogue_id: Uuid,
}
impl NativeRegisteredRemoteCataloguePublication {
    pub(crate) fn seal_joint_materialization(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteCataloguePoint<'_>,
    ) -> Result<NativeRegisteredRemoteCatalogueMaterialization> {
        self.require_point(store, db, point)?;
        self.require_disk(store)?;
        if !self.verified || store.recovery_persistence_uncertain {
            return Err(range_write_error(
                "Registered joint target requires actual completed catalogue publication",
            ));
        }
        store.require_durable_recovery_catalog()?;
        db.require_native_writer_catalogue_custody(store)?;
        let result = NativeRegisteredRemoteCatalogueMaterialization {
            preflight: self.preflight.clone(),
            base: self.base.clone(),
            database_open: self.database_open.clone(),
            store_open: self.store_open,
            catalogue_id: self.catalogue_id,
        };
        result.require_current(store, db, point.physical)?;
        Ok(result)
    }
}
impl NativeRegisteredRemoteCatalogueMaterialization {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        physical: &NativeInstalledRangeImageBase,
    ) -> Result<()> {
        self.preflight.require_original_process()?;
        self.preflight.native().require_current(db)?;
        physical.require_current(db, self.preflight.native())?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        store.require_actual_abort_catalogue(db)?;
        store.require_durable_recovery_catalog()?;
        let log = store
            .catalog
            .ranges
            .get(&self.base.range_id)
            .ok_or_else(|| {
                range_write_error("Registered joint materialized range floor missing")
            })?;
        if store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || store.catalog.native_image_bases.get(&self.base.range_id) != Some(&self.base)
            || log.last_index != self.base.range_index
            || log.resolved_through != self.base.range_index
            || log.compacted_through != self.base.range_index
            || !log.entries.is_empty()
            || physical.attempt_id() != self.base.attempt_id
            || physical.native_receipt_sha256() != self.base.native_receipt_sha256
            || physical.capture_range().id != self.base.range_id
            || physical.range_index() != self.base.range_index
            || physical.logical_genesis_sha256() != self.base.logical_genesis_sha256
            || physical.logical_history_sha256() != self.base.logical_history_sha256
        {
            return Err(range_write_error(
                "Registered joint materialization changed actual original floor/open/native proof",
            ));
        }
        self.preflight.require_original_process()
    }
}
