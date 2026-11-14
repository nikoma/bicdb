//! Actual A-owned original receiver exclusion; never NoDomain or a row grant.
use super::*;

pub(crate) struct NativeRegisteredRemoteAttemptScope<'a> {
    db: &'a BicDb,
    native: &'a NativeRecoveryAdmission,
    original: &'a NativeRegisteredReceiverHook,
    frozen: &'a FrozenRangeMutationPermit,
    attempt: Uuid,
    caller: &'a CancellationToken,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeRegisteredRemoteAttemptScope<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        self.frozen.range_id()
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.attempt
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.native
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.caller.check()?;
        if !std::ptr::eq(db, self.db)
            || self.attempt.is_nil()
            || self.frozen.owner() != self.attempt
        {
            return Err(invalid("Registered proposal changed actual DB/A owner"));
        }
        self.journal.require_database(db)?;
        self.original.require_borrowed(db, &self.hook)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.range_id(), self.frozen)?;
        db.require_never_installed_receiver_writer_records_point()?;
        db.require_original_registered_receiver_birth_with_journal(self.native, &self.journal)?;
        self.caller.check()
    }
    pub(crate) fn require_inventory(&self, db: &BicDb, keys: &[String]) -> Result<()> {
        self.require_database(db)?;
        let prefix = format!("registered-remote-image-proposal/{}/", self.attempt);
        if keys.iter().any(|key| !key.starts_with(&prefix)) {
            return Err(invalid(
                "Registered proposal inventory is outside exact retained attempt",
            ));
        }
        db.require_registered_receiver_inventory_with_extra_keys(keys)
    }
    pub(crate) fn require_install_safety(&self, safety: &NativeRangeInstallSafety) -> Result<()> {
        self.require_database(self.db)?;
        safety.require_current_under_journal(self.db, self.native, self.frozen, &self.journal)
    }
}
impl BicDb {
    pub(crate) fn with_registered_remote_attempt_exclusion<R>(
        &self,
        native: &NativeRecoveryAdmission,
        original: &NativeRegisteredReceiverHook,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
        caller: &CancellationToken,
        operation: impl for<'scope> FnOnce(&'scope NativeRegisteredRemoteAttemptScope<'_>) -> Result<R>,
    ) -> Result<R> {
        let scope = NativeRegisteredRemoteAttemptScope {
            db: self,
            native,
            original,
            frozen,
            attempt,
            caller,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered proposal admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered proposal original hook busy"))?,
            journal: self.try_native_image_journal()?,
        };
        scope.require_database(self)?;
        let result = operation(&scope);
        let post = scope.require_database(self);
        result.and_then(|value| post.map(|()| value))
    }
}
