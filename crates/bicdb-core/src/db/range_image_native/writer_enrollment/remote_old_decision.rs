//! Actual original installed writer/OLD exclusion. This is identity plus a
//! CLOSED freeze, never selected ordinary Serving or a Registered receiver.
use super::*;
use crate::db::NativeJointOldCloseReceipt;
use crate::distribution_range_consensus::{NativeJointOldPromiseReceipt, RangeWriteStore};
use crate::distribution_range_handoff::NativeRemoteDecisionAdmission;
pub(crate) struct NativeRemoteOldDecisionWriterScope<'a> {
    db: &'a BicDb,
    writer: &'a NativeWriterHostState,
    native: &'a NativeJointOldCloseReceipt,
    promise: &'a NativeJointOldPromiseReceipt,
    frozen: &'a FrozenRangeMutationPermit,
    caller: &'a CancellationToken,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeRemoteOldDecisionWriterScope<'_> {
    pub(crate) fn native_close(&self) -> &NativeJointOldCloseReceipt {
        self.native
    }
    pub(crate) fn frozen(&self) -> &FrozenRangeMutationPermit {
        self.frozen
    }

    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
    ) -> Result<()> {
        self.caller.check()?;
        admission.require_old_parts(self.native, self.promise, self.frozen, self.writer)?;
        admission.require_current()?;
        if self.frozen.owner() != admission.plan().promise().session_id {
            return Err(invalid(
                "remote OLD decision original CLOSED owner/session changed",
            ));
        }
        if !std::ptr::eq(db, self.db) {
            return Err(invalid("remote OLD decision changed borrowed database"));
        }
        self.journal.require_database(db)?;
        let registration = self.writer.require_lifetime()?;
        if !Arc::ptr_eq(self.hook.registration(), &registration)
            || self
                .hook
                .native_writer()
                .is_none_or(|actual| !std::ptr::eq(actual.as_ref(), self.writer))
            || db.incarnation_id() != self.writer.record.database_incarnation
            || db.data_path().canonicalize()? != self.writer.root
        {
            return Err(invalid(
                "remote OLD decision original installed hook/DB/open changed",
            ));
        }
        // Consume only the proof retained by THIS original OLD observation
        // before closure. Never derive effective Serving from current DATA.
        admission.require_old_predecessor(db, self.writer)?;
        let stored: NativeWriterPolicyRecord =
            db.read_native_range_image_record(POLICY_POINTER)?
                .ok_or_else(|| invalid("remote OLD decision installed policy absent"))?;
        stored.validate()?;
        if stored != self.writer.record {
            return Err(invalid("remote OLD decision installed policy replaced"));
        }
        self.writer.require_verified_registry_database(db)?;
        self.writer
            .require_store(db.data_path(), store.native_writer_store_open_id())?;
        let actual = store.native_writer_host_state()?;
        if !std::ptr::eq(actual.as_ref(), self.writer) {
            return Err(invalid("remote OLD decision original Store writer differs"));
        }
        self.native
            .require_current(db, admission.plan(), admission.role(), self.promise, store)?;
        db.range_mutation_gate()
            .validate_frozen(admission.data().old_range().id, self.frozen)?;
        db.check_recovery_state()?;
        admission.require_memory_current()?;
        self.caller.check()
    }
}
impl BicDb {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn remote_old_decision_writer_scope<'a>(
        &'a self,
        writer: &'a NativeWriterHostState,
        native: &'a NativeJointOldCloseReceipt,
        promise: &'a NativeJointOldPromiseReceipt,
        frozen: &'a FrozenRangeMutationPermit,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        caller: &'a CancellationToken,
    ) -> Result<NativeRemoteOldDecisionWriterScope<'a>> {
        admission.require_old_parts(native, promise, frozen, writer)?;
        let scope = NativeRemoteOldDecisionWriterScope {
            db: self,
            writer,
            native,
            promise,
            frozen,
            caller,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("remote OLD decision admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("remote OLD decision hook busy"))?,
            journal: self.try_native_image_journal()?,
        };
        scope.require_current(self, store, admission)?;
        self.range_mutation_gate()
            .validate_quiescent_frozen(admission.data().old_range().id, frozen)?;
        Ok(scope)
    }
}
