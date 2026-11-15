//! Lexical original Joined image/A exclusion for later joint mutations.
//! No Role/OWN, NEW quorum, DECISION, Serving, APP or Strong authority is
//! constructed here. A concrete mutation must supply its own fresh admission.
use super::*;
use crate::distribution_native_host::NativeJoinedRemoteMaterializedOwner;
use crate::distribution_range_consensus::RangeWriteStore;

pub(crate) struct NativeJoinedJointExclusionScope<'a> {
    database: &'a BicDb,
    target: &'a NativeJoinedRemoteMaterializedOwner,
    frozen: &'a FrozenRangeMutationPermit,
    caller: &'a CancellationToken,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    writer: NativeOriginalWriterIdentityBorrow<'a>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeJoinedJointExclusionScope<'_> {
    pub(crate) fn target(&self) -> &NativeJoinedRemoteMaterializedOwner {
        self.target
    }
    pub(crate) fn frozen(&self) -> &FrozenRangeMutationPermit {
        self.frozen
    }
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.caller.check()?;
        if !std::ptr::eq(self.database, db) {
            return Err(invalid("Joined joint exclusion changed actual borrowed DB"));
        }
        self.journal.require_database(db)?;
        self.target
            .require_joint_point_with_writer_borrow(db, store, self.frozen, &self.writer)?;
        self.caller.check()
    }
}
impl BicDb {
    /// Only the actual DB can obtain this non-serde point. The target must be
    /// the original host-produced materialization and the permit its exact A.
    /// The already-held hook is reused through the sealed writer borrow.
    pub(crate) fn joined_joint_exclusion_scope<'a>(
        &'a self,
        target: &'a NativeJoinedRemoteMaterializedOwner,
        frozen: &'a FrozenRangeMutationPermit,
        store: &RangeWriteStore,
        caller: &'a CancellationToken,
    ) -> Result<NativeJoinedJointExclusionScope<'a>> {
        caller.check()?;
        let scope = NativeJoinedJointExclusionScope {
            database: self,
            target,
            frozen,
            caller,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Joined joint admission busy"))?,
            writer: self
                .borrow_original_native_writer_identity(target.original_writer().clone())?,
            journal: self.try_native_image_journal()?,
        };
        scope.require_current(self, store)?;
        self.range_mutation_gate()
            .validate_quiescent_frozen(target.base().capture_range().id, frozen)?;
        Ok(scope)
    }
}
