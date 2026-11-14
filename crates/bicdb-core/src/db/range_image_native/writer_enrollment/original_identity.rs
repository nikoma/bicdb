//! Actual held original-writer hook identity. This is no selected-range,
//! metadata, image, mutation, CLOSED, Serving, APP or Strong authority.
use super::*;

/// Only actual DB hook acquisition constructs this lexical non-serde point.
/// It keeps the real read guard, not a memoized bool, raw hook or detached ID.
pub(crate) struct NativeOriginalWriterIdentityBorrow<'a> {
    database: &'a BicDb,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    writer: Arc<NativeWriterHostState>,
}
impl NativeOriginalWriterIdentityBorrow<'_> {
    pub(crate) fn writer(&self) -> &Arc<NativeWriterHostState> {
        &self.writer
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        if !std::ptr::eq(self.database, db) {
            return Err(invalid("borrowed original writer changed actual DB object"));
        }
        self.writer
            .require_original_identity_database_at_hook(db, &self.hook)?;
        self.writer.require_verified_registry_database(db)
    }
    /// Actual guard+coordinator identity under THIS actual borrowed hook. No
    /// recursive hook read, source/metadata proof or range admission is added.
    pub(crate) fn require_guard_origin<T: RangeWriteTransport + 'static>(
        &self,
        db: &BicDb,
        guard: &InstalledNativeWriterHostGuard,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        writer: &Arc<NativeWriterHostState>,
    ) -> Result<()> {
        self.require_database(db)?;
        if !Arc::ptr_eq(&guard.state, writer) || !Arc::ptr_eq(writer, &self.writer) {
            return Err(invalid("borrowed original writer replaced actual State"));
        }
        let expected: Arc<dyn CommitAdmission> = coordinator.clone();
        if self
            .writer
            .coordinator
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, &expected))
            || coordinator.native_writer_coordinator_id() != self.writer.record.coordinator_id
        {
            return Err(invalid(
                "borrowed original writer replaced actual coordinator",
            ));
        }
        self.require_database(db)
    }
}
impl BicDb {
    /// Read-only original identity point from a real installed hook. Callers
    /// holding another hook guard must reuse their existing point, not call
    /// this constructor again. It does not take native mutation admission.
    pub(crate) fn borrow_original_native_writer_identity<'a>(
        &'a self,
        writer: Arc<NativeWriterHostState>,
    ) -> Result<NativeOriginalWriterIdentityBorrow<'a>> {
        let hook = self
            .commit_admission
            .try_read()
            .ok_or_else(|| invalid("original native writer hook identity busy"))?;
        let point = NativeOriginalWriterIdentityBorrow {
            database: self,
            hook,
            writer,
        };
        point.require_database(self)?;
        Ok(point)
    }
}
impl NativeWriterHostState {
    /// Same strict original identity as the ordinary helper, using the
    /// ACTUAL caller-held hook. Ordinary blocking identity is left unchanged.
    fn require_original_identity_database_at_hook(
        &self,
        database: &BicDb,
        hook: &crate::db::commit_admission_hook::CommitAdmissionSlot,
    ) -> Result<()> {
        self.require_lifetime()?;
        database.check_recovery_state()?;
        if database.incarnation_id() != self.record.database_incarnation
            || database.data_path().canonicalize()? != self.root
        {
            return Err(invalid("native writer database/open changed"));
        }
        let facts = &self.record.facts;
        let native = database.native_recovery_admission(
            &facts.cluster_id,
            facts.bootstrap_id,
            &facts.local_node_id,
            facts.local_incarnation,
        )?;
        if native.writer_enforcement_version() != 1
            || native.birth_id() != facts.local_birth_id
            || native.birth_sha256() != facts.local_birth_sha256
        {
            return Err(invalid("native writer birth/enforcement changed"));
        }
        let registration = self
            .registration
            .upgrade()
            .ok_or_else(|| invalid("native writer registration ended"))?;
        if !Arc::ptr_eq(hook.registration(), &registration)
            || hook
                .native_writer()
                .is_none_or(|state| !std::ptr::eq(state.as_ref(), self))
        {
            return Err(invalid(
                "native writer policy is not the actual installed hook",
            ));
        }
        let stored: NativeWriterPolicyRecord = database
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| invalid("native host writer receipt missing"))?;
        stored.validate()?;
        if stored != self.record {
            return Err(invalid("native host writer receipt changed"));
        }
        Ok(())
    }
}
