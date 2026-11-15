//! Actual original installed writer/hook/native journal for a single Joined
//! dispatcher publication. This local scope is never a distributed lease or
//! a replacement for ordinary per-request metadata/data read/write barriers.
use super::*;

pub(crate) struct NativeJoinedPublicationScope<'a> {
    database: &'a BicDb,
    metadata: &'a crate::distribution_data::ActualNativeJointMetadataPoint,
    journal: NativeImageJournalGuard<'a>,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    _hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    _writer: &'a Arc<NativeWriterHostState>,
}
impl InstalledNativeWriterHostGuard {
    pub(crate) fn joined_publication_scope<'a, T: RangeWriteTransport + 'static>(
        &'a self,
        db: &'a BicDb,
        range: &RangeDescriptor,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        metadata: &'a crate::distribution_data::ActualNativeJointMetadataPoint,
    ) -> Result<NativeJoinedPublicationScope<'a>> {
        self.require_for_coordinator(db, range, coordinator)?;
        db.require_joined_publication_storage_health()?;
        let admission = db
            .serializable_commit_admission
            .try_read()
            .ok_or_else(|| invalid("Joined publication actual admission busy"))?;
        let hook = db
            .commit_admission
            .try_read()
            .ok_or_else(|| invalid("Joined publication actual writer hook busy"))?;
        self.state.require_borrowed_original_hook(db, &hook)?;
        let journal = db.try_native_image_journal()?;
        metadata.require_native_point(db)?;
        let actual: NativeWriterPolicyRecord = db
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| {
            invalid("Joined publication original native writer receipt absent")
        })?;
        actual.validate()?;
        if actual != self.state.record {
            return Err(invalid(
                "Joined publication replaced original actual native writer",
            ));
        }
        Ok(NativeJoinedPublicationScope {
            database: db,
            metadata,
            journal,
            _admission: admission,
            _hook: hook,
            _writer: &self.state,
        })
    }
}
impl NativeJoinedPublicationScope<'_> {
    pub(crate) fn historical_cut<'a>(
        &'a self,
        native: &'a crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<crate::db::NativeJointHistoricalCutScope<'a>> {
        native.historical_cut_scope_under_journal(self.database, &self.journal)
    }
    pub(crate) fn require_actual_store(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.metadata
            .require_native_store_point(self.database, store)
    }
    pub(crate) fn publication_guard(
        &self,
    ) -> Result<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>> {
        self.metadata.try_publication_guard()
    }
}

impl BicDb {
    /// Necessary actual Core/page/WAL health only, no readiness or authority.
    pub(crate) fn require_joined_publication_storage_health(&self) -> Result<()> {
        self.check_recovery_state()?;
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Joined actual native page store absent"))?;
        paged
            .store()
            .durability_health()
            .check()
            .map_err(crate::error::durability_error)?;
        paged
            .store()
            .wal()
            .health()
            .check()
            .map_err(crate::error::durability_error)?;
        self.check_recovery_state()
    }
}
