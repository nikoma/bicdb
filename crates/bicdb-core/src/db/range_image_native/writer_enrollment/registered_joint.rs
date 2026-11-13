//! A distinct ORIGINAL receiver-only joint metadata scope. Never an ordinary
//! writer, a legacy install fence, a local OLD quorum, or a row transaction.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteJointTarget;
use crate::distribution_range_consensus::RangeWriteStore;
pub(crate) struct NativeRegisteredJointScope<'a> {
    db: &'a BicDb,
    target: &'a NativeRegisteredRemoteJointTarget,
    frozen: &'a FrozenRangeMutationPermit,
    caller: &'a CancellationToken,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeRegisteredJointScope<'_> {
    pub(crate) fn target(&self) -> &NativeRegisteredRemoteJointTarget {
        self.target
    }
    pub(crate) fn frozen(&self) -> &FrozenRangeMutationPermit {
        self.frozen
    }
    fn require_original(&self, db: &BicDb) -> Result<()> {
        self.caller.check()?;
        if !std::ptr::eq(db, self.db) {
            return Err(invalid(
                "Registered joint scope changed actual borrowed database",
            ));
        }
        self.journal.require_database(db)?;
        self.target
            .original_hook()
            .require_borrowed(db, &self.hook)?;
        db.require_never_installed_receiver_writer_records_point()?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.target.base().capture_range().id, self.frozen)?;
        self.target.require_original_point(db, self.frozen)?;
        self.caller.check()
    }
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_original(db)?;
        self.target
            .require_materialized_point(db, store, self.frozen)?;
        self.require_original(db)
    }
}
impl BicDb {
    pub(crate) fn with_registered_joint_scope<R>(
        &self,
        target: &NativeRegisteredRemoteJointTarget,
        frozen: &FrozenRangeMutationPermit,
        caller: &CancellationToken,
        operation: impl for<'scope> FnOnce(&'scope NativeRegisteredJointScope<'_>) -> Result<R>,
    ) -> Result<R> {
        let scope = NativeRegisteredJointScope {
            db: self,
            target,
            frozen,
            caller,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered joint admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered joint original hook busy"))?,
            journal: self.try_native_image_journal()?,
        };
        scope.require_original(self)?;
        let result = operation(&scope);
        let post = scope.require_original(self);
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(value) => {
                post?;
                Ok(value)
            }
        }
    }
}
