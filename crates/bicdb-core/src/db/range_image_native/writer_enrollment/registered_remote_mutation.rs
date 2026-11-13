//! Original Registered A-owned mutation exclusion after Installing. Never R0.
use super::*;

pub(crate) struct NativeRegisteredRemoteMutationScope<'a> {
    db: &'a BicDb,
    native: &'a NativeRecoveryAdmission,
    original: &'a NativeRegisteredReceiverHook,
    frozen: &'a FrozenRangeMutationPermit,
    attempt: Uuid,
    caller: &'a CancellationToken,
    // Invalidate lexical liveness BEFORE dropping any actual lock guard.
    lifetime: Arc<()>,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
/// Only the lexical scope owns the strong reference. A transaction cannot
/// prolong the actual hook/admission/journal exclusion through this token.
pub(in crate::db::range_image_native) struct RegisteredRemoteMutationScopeLiveness {
    scope: std::sync::Weak<()>,
    database_incarnation: String,
    attempt: Uuid,
    range: RangeId,
}
impl RegisteredRemoteMutationScopeLiveness {
    pub(in crate::db::range_image_native) fn require_current(
        &self,
        db: &BicDb,
        attempt: Uuid,
        range: RangeId,
    ) -> Result<()> {
        if self.scope.upgrade().is_none()
            || self.database_incarnation != db.incarnation_id()
            || self.attempt != attempt
            || self.range != range
        {
            return Err(invalid(
                "Registered row transaction outlived original guarded scope",
            ));
        }
        db.check_recovery_state()
    }
}
impl NativeRegisteredRemoteMutationScope<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        self.frozen.range_id()
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.attempt
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.native
    }
    pub(in crate::db::range_image_native) fn frozen(&self) -> &FrozenRangeMutationPermit {
        self.frozen
    }
    pub(in crate::db::range_image_native) fn retained_scope_liveness(
        &self,
    ) -> RegisteredRemoteMutationScopeLiveness {
        RegisteredRemoteMutationScopeLiveness {
            scope: Arc::downgrade(&self.lifetime),
            database_incarnation: self.db.incarnation_id().into(),
            attempt: self.attempt,
            range: self.range_id(),
        }
    }
    /// Allows this scope's actual Controlled writer. The constructor and exit
    /// independently require quiescence; this never admits another owner.
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.caller.check()?;
        if !std::ptr::eq(self.db, db)
            || self.attempt.is_nil()
            || self.frozen.owner() != self.attempt
        {
            return Err(invalid("Registered mutation changed actual DB/A owner"));
        }
        self.journal.require_database(db)?;
        self.original.require_borrowed(db, &self.hook)?;
        db.range_mutation_gate()
            .validate_frozen(self.range_id(), self.frozen)?;
        db.require_never_installed_receiver_writer_records_point()?;
        db.require_original_registered_receiver_birth_open_with_journal(
            self.native,
            &self.journal,
        )?;
        self.caller.check()
    }
    pub(crate) fn require_irreversible_install_safety(
        &self,
        safety: &NativeRangeInstallSafety,
    ) -> Result<()> {
        self.require_database(self.db)?;
        let current = self.db.native_range_install_safety_record_point(
            self.native,
            self.range_id(),
            &self.journal,
        )?;
        if safety.range_id() != self.range_id()
            || safety.irreversible_records_sha256() != current.irreversible_records_sha256()
        {
            return Err(invalid(
                "Registered mutation changed immutable native history/pending preimage",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn with_registered_remote_mutation_exclusion<R>(
        &self,
        native: &NativeRecoveryAdmission,
        original: &NativeRegisteredReceiverHook,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
        caller: &CancellationToken,
        operation: impl for<'scope> FnOnce(&'scope NativeRegisteredRemoteMutationScope<'_>) -> Result<R>,
    ) -> Result<R> {
        let scope = NativeRegisteredRemoteMutationScope {
            db: self,
            native,
            original,
            frozen,
            attempt,
            caller,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered mutation admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered mutation original hook busy"))?,
            journal: self.try_native_image_journal()?,
            lifetime: Arc::new(()),
        };
        scope.require_database(self)?;
        self.range_mutation_gate()
            .validate_quiescent_frozen(scope.range_id(), frozen)?;
        let result = operation(&scope);
        let post = scope.require_database(self).and_then(|()| {
            self.range_mutation_gate()
                .validate_quiescent_frozen(scope.range_id(), frozen)
        });
        result.and_then(|value| post.map(|()| value))
    }
}
