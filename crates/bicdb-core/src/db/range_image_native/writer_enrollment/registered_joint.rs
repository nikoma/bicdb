//! ORIGINAL receiver identity and mutation exclusion only. This scope supplies
//! NO fresh Role/OWN/metadata authority. Each phase mutator must separately
//! retain and revalidate an actual fresh typed metadata point before and after
//! its native operation. Never an ordinary
//! writer, a legacy install fence, a local OLD quorum, or a row transaction.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteJointTarget;
use crate::distribution_range_consensus::RangeWriteStore;
pub(crate) struct NativeRegisteredJointExclusionScope<'a> {
    db: &'a BicDb,
    target: &'a NativeRegisteredRemoteJointTarget,
    frozen: &'a FrozenRangeMutationPermit,
    caller: &'a CancellationToken,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeRegisteredJointExclusionScope<'_> {
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
            .validate_frozen(self.target.base().capture_range().id, self.frozen)?;
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
    pub(crate) fn with_registered_joint_exclusion_scope<R>(
        &self,
        target: &NativeRegisteredRemoteJointTarget,
        frozen: &FrozenRangeMutationPermit,
        caller: &CancellationToken,
        operation: impl for<'scope> FnOnce(&'scope NativeRegisteredJointExclusionScope<'_>) -> Result<R>,
    ) -> Result<R> {
        let scope = NativeRegisteredJointExclusionScope {
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
        self.range_mutation_gate()
            .validate_quiescent_frozen(target.base().capture_range().id, frozen)?;
        let result = operation(&scope);
        let post = scope.require_original(self).and_then(|()| {
            self.range_mutation_gate()
                .validate_quiescent_frozen(target.base().capture_range().id, frozen)
        });
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

/// Necessary original identity/admission exclusion WITHOUT a journal or
/// Frozen/metadata grant. Joint producers retain this through native/cat WAL;
/// their distinct typed phase admission and physical ownership stay mandatory.
pub(crate) struct NativeRegisteredJointIdentityBorrow<'a> {
    db: &'a BicDb,
    target: Arc<NativeRegisteredRemoteJointTarget>,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
}
impl NativeRegisteredJointIdentityBorrow<'_> {
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        if !std::ptr::eq(db, self.db) {
            return Err(invalid(
                "Registered joint identity borrow changed actual DB",
            ));
        }
        self.target
            .original_hook()
            .require_borrowed(db, &self.hook)?;
        db.require_never_installed_receiver_writer_records_point()?;
        self.target.require_joint_identity(db, store)?;
        self.target.original_hook().require_borrowed(db, &self.hook)
    }
}
impl BicDb {
    pub(crate) fn borrow_registered_joint_identity(
        &self,
        target: Arc<NativeRegisteredRemoteJointTarget>,
    ) -> Result<NativeRegisteredJointIdentityBorrow<'_>> {
        let original = NativeRegisteredJointIdentityBorrow {
            db: self,
            target,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered joint identity admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Registered joint identity original hook busy"))?,
        };
        original
            .target
            .original_hook()
            .require_borrowed(self, &original.hook)?;
        self.require_never_installed_receiver_writer_records_point()?;
        Ok(original)
    }
}
