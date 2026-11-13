//! PRIVATE serving observation borrow. Never a role, snapshot or read permit.
use super::*;
use crate::distribution_data::RangeWriteServiceSlotGuard;
use crate::distribution_native_read::NativeRangeReadFacts;
use crate::distribution_range_consensus::RangeWriteStore;

pub(crate) struct NativeServingObservationScope<'a> {
    database: &'a BicDb,
    store: &'a RangeWriteStore,
    request: &'a NativeRangeReadScope,
    native: &'a NativeRecoveryAdmission,
    parent: &'a CancellationToken,
    slot: &'a RangeWriteServiceSlotGuard,
    writer: Arc<NativeWriterHostState>,
    journal: NativeImageJournalGuard<'a>,
    lineage: SourceRangeImageLineage,
    catalogue: NativeRangeReadCatalogue,
    point: PhysicalPoint,
    stamp: NativeRangeReadStamp,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
struct ObservationParts {
    catalogue: NativeRangeReadCatalogue,
    point: PhysicalPoint,
    stamp: NativeRangeReadStamp,
    writer: NativeReadWriterLifetime,
}
impl<'a> NativeServingObservationScope<'a> {
    fn enter(
        database: &'a BicDb,
        store: &'a RangeWriteStore,
        request: &'a NativeRangeReadScope,
        native: &'a NativeRecoveryAdmission,
        slot: &'a RangeWriteServiceSlotGuard,
        parent: &'a CancellationToken,
    ) -> Result<Self> {
        #[cfg(test)]
        let _entry =
            crate::distribution_native_read::trace::Span::current("read-serving-scope-entry");
        parent.check()?;
        request.validate()?;
        if native.writer_enforcement_version() != 1 {
            return Err(invalid(
                "serving read scope requires actual enforced native birth",
            ));
        }
        slot.require_live()?;
        if slot.id() != store.native_writer_store_open_id() {
            return Err(invalid(
                "serving read scope actual service Store slot differs",
            ));
        }
        store.require_native_catalog_durability(database.data_path())?;
        store.require_native_writer_host_base()?;
        let writer = store.native_writer_host_state()?;
        writer.require_read_database_lifetime(database, &request.range)?;
        let stamp = database
            .range_mutation_gate()
            .observe_native_read_stamp(request.range.id)?;
        let journal = database.try_native_image_journal()?;
        native.require_current(database)?;
        let fingerprint = database.schema_compatibility_fingerprint()?;
        let lineage = database
            .current_native_range_source_lineage_under_journal(request.range.id, &journal)?;
        let catalogue = store.observe_native_read_catalogue(
            request,
            lineage.range_index(),
            database.data_path(),
            native,
        )?;
        let point = database.native_read_point_from_lineage(
            request,
            &catalogue,
            parent,
            &lineage,
            &fingerprint.sha256,
        )?;
        if native.birth_id() != point.birth_id || native.birth_sha256() != point.birth_sha256 {
            return Err(invalid("serving read scope actual birth changed"));
        }
        let scope = Self {
            database,
            store,
            request,
            native,
            parent,
            slot,
            writer,
            journal,
            lineage,
            catalogue,
            point,
            stamp,
            _local: std::marker::PhantomData,
        };
        scope.writer.require_database(database, &request.range)?;
        scope.require_mutable()?;
        database.require_native_serving_read_archive(&scope)?;
        store.require_actual_abort_controls_in_read_scope(&scope)?;
        scope.require_mutable()?;
        Ok(scope)
    }
    pub(crate) fn database(&self) -> &BicDb {
        self.database
    }
    pub(crate) fn actual_range_point(&self) -> (RangeId, u64) {
        (self.request.range.id, self.lineage.range_index())
    }
    pub(crate) fn require_database(&self, database: &BicDb) -> Result<()> {
        self.require_database_store(database, self.store)
    }
    pub(crate) fn require_database_store(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        if !std::ptr::eq(database, self.database) || !std::ptr::eq(store, self.store) {
            return Err(invalid("serving read scope substituted actual DB/Store"));
        }
        self.journal.require_database(database)?;
        self.parent.check()?;
        self.slot.require_live()?;
        self.stamp.require_current()?;
        self.writer.require_read_lifetime()
    }
    pub(crate) fn require_range(&self, range: RangeId, native_r: u64) -> Result<()> {
        self.journal.require_database(self.database)?;
        self.parent.check()?;
        if range != self.request.range.id || native_r != self.lineage.range_index() {
            return Err(invalid(
                "serving read scope belongs to another actual range/point",
            ));
        }
        Ok(())
    }
    pub(in crate::db::range_image_native) fn lineage(
        &self,
        database: &BicDb,
    ) -> Result<&SourceRangeImageLineage> {
        self.require_database_store(database, self.store)?;
        Ok(&self.lineage)
    }
    fn require_mutable(&self) -> Result<()> {
        self.require_database_store(self.database, self.store)?;
        self.database.check_recovery_state()?;
        self.store
            .require_native_catalog_durability(self.database.data_path())?;
        self.store.require_native_writer_host_base()?;
        let actual = self.store.native_writer_host_state()?;
        if !Arc::ptr_eq(&actual, &self.writer) {
            return Err(invalid("serving read scope changed original writer object"));
        }
        self.writer
            .require_read_database_lifetime(self.database, &self.request.range)?;
        self.parent.check()
    }
    fn require_observation(
        &self,
        physical: &NativeRangeReadPhysical,
        catalogue: &NativeRangeReadCatalogue,
    ) -> Result<()> {
        physical.require_lifetime()?;
        physical.require_catalogue_binding(catalogue)?;
        if !std::ptr::eq(&physical.native, self.native)
            || physical.point != self.point
            || physical.scope.checksum_sha256()? != self.request.checksum_sha256()?
        {
            return Err(invalid(
                "serving read scope differs from original actual observation",
            ));
        }
        match &physical.writer {
            NativeReadWriterLifetime::Enforced {
                writer,
                slot,
                schema_generation,
                serving_record_sha256,
            } => {
                let original = writer
                    .upgrade()
                    .ok_or_else(|| invalid("native read original writer ended"))?;
                if !Arc::ptr_eq(&original, &self.writer)
                    || slot.id() != self.slot.id()
                    || *schema_generation != self.lineage.current_schema_generation
                    || serving_record_sha256.as_deref()
                        != self.lineage.serving.as_ref().map(|s| s.checksum_sha256())
                {
                    return Err(invalid("serving read scope replaced original writer/slot"));
                }
                slot.require_live()?;
            }
            NativeReadWriterLifetime::LegacyComponent => {
                return Err(invalid("legacy observation has no enforced serving scope"))
            }
        }
        catalogue.require_current(
            self.store,
            self.request,
            self.point.range_index,
            self.database.data_path(),
            self.native,
        )?;
        self.parent.check()
    }
    pub(crate) fn require_ancestor(&self, peer: &NativeRangeReadFacts) -> Result<()> {
        self.require_database_store(self.database, self.store)?;
        if peer.logical_genesis_sha256 != self.point.h0 || peer.range_index > self.point.range_index
        {
            return Err(invalid("peer point is outside actual local native history"));
        }
        let expected = if peer.range_index == 0 {
            self.point.h0.clone()
        } else {
            self.database
                .read_native_history_entry(
                    self.request.range.id,
                    self.lineage.history_generation(),
                    peer.range_index,
                )?
                .commitment_sha256
        };
        if expected != peer.logical_history_sha256 {
            return Err(invalid(
                "lower peer conflicts with actual local native history",
            ));
        }
        self.parent.check()
    }
    fn require_exit(&self) -> Result<()> {
        #[cfg(test)]
        let _exit =
            crate::distribution_native_read::trace::Span::current("read-serving-scope-exit");
        self.require_mutable()?;
        // A fresh full read of actual state, not acceptance of the borrowed
        // prestate. The one lineage remains bound by the continuously held journal.
        self.database.validate_source_range_image_lineage(
            &self.lineage,
            self.request.range.id,
            self.point.range_index,
        )?;
        self.database.require_native_serving_read_archive(self)?;
        self.store
            .require_actual_abort_controls_in_read_scope(self)?;
        self.catalogue.require_current(
            self.store,
            self.request,
            self.point.range_index,
            self.database.data_path(),
            self.native,
        )?;
        let fingerprint = self.database.schema_compatibility_fingerprint()?;
        let point = self.database.native_read_point_from_lineage(
            self.request,
            &self.catalogue,
            self.parent,
            &self.lineage,
            &fingerprint.sha256,
        )?;
        if point != self.point {
            return Err(invalid("serving read scope actual native point changed"));
        }
        self.native.require_current(self.database)?;
        self.writer
            .require_database(self.database, &self.request.range)?;
        self.require_mutable()
    }
    fn finish<R>(
        self,
        action: impl for<'scope> FnOnce(&NativeServingObservationScope<'scope>) -> Result<R>,
    ) -> Result<(R, ObservationParts)> {
        #[cfg(test)]
        tests::after_entry(&self)?;
        let result = action(&self)?;
        self.require_exit()?;
        let parts = ObservationParts {
            catalogue: self.catalogue,
            point: self.point,
            stamp: self.stamp,
            writer: NativeReadWriterLifetime::Enforced {
                writer: Arc::downgrade(&self.writer),
                slot: self.slot.clone(),
                schema_generation: self.lineage.current_schema_generation,
                serving_record_sha256: self
                    .lineage
                    .serving
                    .as_ref()
                    .map(|s| s.checksum_sha256().to_owned()),
            },
        };
        Ok((result, parts))
    }
}
impl BicDb {
    pub(crate) fn observe_enforced_native_range_read(
        &self,
        store: &RangeWriteStore,
        slot: &RangeWriteServiceSlotGuard,
        scope: &NativeRangeReadScope,
        parent: &CancellationToken,
    ) -> Result<(NativeRangeReadPhysical, NativeRangeReadCatalogue)> {
        parent.check()?;
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("serving read has no actual native birth"))?;
        let native = self.native_admission_for_actual_birth(&birth)?;
        let scope_read =
            NativeServingObservationScope::enter(self, store, scope, &native, slot, parent)?;
        let (_, parts) = scope_read.finish(|_| Ok(()))?;
        let physical = NativeRangeReadPhysical {
            native,
            scope: scope.clone(),
            local_b: NativeReadLocalDbSequence(parts.point.local_b),
            point: parts.point,
            stamp: parts.stamp,
            writer: parts.writer,
            directory: Arc::downgrade(
                self._directory_lock
                    .as_ref()
                    .ok_or_else(|| invalid("native read has no actual directory lifetime"))?,
            ),
            health: Arc::clone(&self.durability),
            store_open_id: parts.catalogue.store_open_id(),
            catalogue_sha256: parts.catalogue.checksum_sha256().into(),
            cancellation: parent.child(),
        };
        physical.require_lifetime()?;
        parent.check()?;
        Ok((physical, parts.catalogue))
    }
    pub(crate) fn with_existing_native_serving_read_scope<R>(
        &self,
        physical: &NativeRangeReadPhysical,
        catalogue: &NativeRangeReadCatalogue,
        store: &RangeWriteStore,
        slot: &RangeWriteServiceSlotGuard,
        parent: &CancellationToken,
        action: impl for<'scope> FnOnce(&NativeServingObservationScope<'scope>) -> Result<R>,
    ) -> Result<R> {
        physical.require_lifetime()?;
        let scope = NativeServingObservationScope::enter(
            self,
            store,
            &physical.scope,
            &physical.native,
            slot,
            parent,
        )?;
        scope.require_observation(physical, catalogue)?;
        let (result, _) = scope.finish(|actual| {
            let result = action(actual)?;
            actual.require_observation(physical, catalogue)?;
            Ok(result)
        })?;
        physical.require_lifetime()?;
        parent.check()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
