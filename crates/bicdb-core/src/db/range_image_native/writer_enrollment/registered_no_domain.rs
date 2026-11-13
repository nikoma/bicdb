//! Scoped ORIGINAL receiver exclusion. Absence is never a zero history floor.
use super::*;

/// Created only while the actual original hook/admission and native journal
/// are borrowed. This scoped observation cannot be returned by the callback.
pub(crate) struct NativeRegisteredNoDomainScope<'a> {
    database: &'a BicDb,
    native: &'a NativeRecoveryAdmission,
    original: &'a NativeRegisteredReceiverHook,
    frozen: &'a FrozenRangeMutationPermit,
    range: RangeId,
    caller: &'a CancellationToken,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeRegisteredNoDomainScope<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        self.range
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.native
    }
    pub(crate) fn require_selected_rows_absent(
        &self,
        range: &crate::RangeDescriptor,
        bindings: &crate::db::RangeImageDbBindings,
    ) -> Result<()> {
        self.require_database(self.database)?;
        if range.id != self.range {
            return Err(invalid("NoDomain row scan changed actual selected range"));
        }
        self.database.validate_range_image_bindings(bindings)?;
        for collection in bindings.collections() {
            self.caller.check()?;
            let actual = self
                .database
                .for_each_range_snapshot_batch_with_cancellation(
                    &collection.name,
                    range,
                    None,
                    &RangeSnapshotOptions {
                        max_records_per_batch: 1,
                        max_bytes_per_batch: 8 * 1024 * 1024,
                        max_record_bytes: 8 * 1024 * 1024,
                    },
                    1024,
                    self.caller,
                    |_batch| {
                        Err(invalid(
                            "Registered NoDomain selected range has actual physical rows",
                        ))
                    },
                )?;
            if !actual.completed {
                return Err(invalid(
                    "Registered NoDomain bounded selected-row absence scan incomplete",
                ));
            }
            self.database.validate_range_image_bindings(bindings)?;
        }
        self.require_database(self.database)
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.require_original_exclusion(db)?;
        db.require_original_registered_receiver_domain_with_journal(self.native, &self.journal)?;
        self.caller.check()
    }
    /// Does not assert NoDomain or authorize rows. This is the exact original
    /// exclusion recheck after a future typed Installing-refusal producer has
    /// changed the native inventory. That producer must verify its own receipt.
    pub(crate) fn require_original_exclusion(&self, db: &BicDb) -> Result<()> {
        self.caller.check()?;
        if !std::ptr::eq(self.database, db) {
            return Err(invalid("Registered NoDomain changed actual borrowed DB"));
        }
        self.journal.require_database(db)?;
        self.original.require_borrowed(db, &self.hook)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.range, self.frozen)?;
        self.native.require_current(db)?;
        db.require_never_installed_receiver_writer_records_point()?;
        db.require_original_registered_receiver_birth_with_journal(self.native, &self.journal)?;
        self.caller.check()
    }
}
impl BicDb {
    #[cfg(test)]
    pub(crate) fn test_invalidate_registered_no_domain_schema(
        &self,
        scope: &NativeRegisteredNoDomainScope<'_>,
    ) -> Result<()> {
        scope.require_database(self)?;
        let before = self.schema_compatibility.generation();
        // Real generation invalidation, not invented DTO/UUID. This proves
        // revocation only; no DDL operation or DDL durability is exercised.
        self.schema_compatibility.invalidate();
        if self.schema_compatibility.generation() <= before {
            return Err(invalid("actual NoDomain schema generation did not advance"));
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_registered_selected_no_domain_exclusion<R>(
        &self,
        native: &NativeRecoveryAdmission,
        original: &NativeRegisteredReceiverHook,
        range: RangeId,
        frozen: &FrozenRangeMutationPermit,
        caller: &CancellationToken,
        operation: impl for<'scope> FnOnce(&'scope NativeRegisteredNoDomainScope<'_>) -> Result<R>,
    ) -> Result<R> {
        caller.check()?;
        let scope = NativeRegisteredNoDomainScope {
            database: self,
            native,
            original,
            frozen,
            range,
            caller,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered NoDomain writer admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered NoDomain original hook busy"))?,
            journal: self.try_native_image_journal()?,
        };
        scope.require_database(self)?;
        let result = operation(&scope);
        let post = scope.require_original_exclusion(self);
        result.and_then(|value| post.map(|()| value))
    }
}
