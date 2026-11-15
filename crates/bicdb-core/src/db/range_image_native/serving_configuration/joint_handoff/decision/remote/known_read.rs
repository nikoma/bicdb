//! Fresh protected read of SAME private KnownCommitted attempt. No format,
//! native begin/put/commit/Controlled acquisition or original-token revival.
use super::*;
impl BicDb {
    pub(crate) fn recover_known_remote_registered_decision(
        &self,
        target: &crate::distribution_native_host::NativeRegisteredRemoteJointTarget,
        frozen: &FrozenRangeMutationPermit,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.known_read_attempt()?;
        admission.require_target(target)?;
        let physical = self.registered_joint_exclusion_scope(target, frozen, parent)?;
        self.read_original_known_remote_decision(
            RemotePhysicalScope::Registered(physical),
            store,
            admission,
            parent,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_known_remote_old_decision(
        &self,
        writer: &crate::db::NativeWriterHostState,
        native: &NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.known_read_attempt()?;
        let physical = self.remote_old_decision_writer_scope(
            writer, native, promise, frozen, store, admission, parent,
        )?;
        self.read_original_known_remote_decision(
            RemotePhysicalScope::Old(physical),
            store,
            admission,
            parent,
        )
    }
    fn read_original_known_remote_decision(
        &self,
        physical: RemotePhysicalScope<'_>,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionReceipt> {
        let original = admission.known_read_attempt()?;
        self.require_registered_joint_decision_storage_healthy()?;
        physical.require_current(self, store, admission)?;
        admission
            .metadata()
            .require_native_store_point(self, store)?;
        let point = physical.local_point();
        let receipt = self.read_native_joint_decision(
            point.native,
            admission.data().old_range().id,
            admission.data().checksum_sha256(),
        )?;
        // Requires private KnownCommitted + exact original xid/canonical
        // record/provenance bytes and CURRENT/archive/status, not status alone.
        self.observe_known_remote_native_decision(&receipt, original)?;
        if receipt.data() != admission.data()
            || admission.role().context_id() == receipt.metadata_context_id()
            || admission.role().metadata_term() < receipt.metadata_anchor().0
            || admission.role().metadata_read_index() < receipt.metadata_anchor().1
        {
            return Err(invalid(
                "fresh known native read changed body/metadata floor",
            ));
        }
        self.require_registered_joint_decision_storage_healthy()?;
        physical.require_current(self, store, admission)?;
        admission
            .metadata()
            .require_native_store_point(self, store)?;
        self.range_mutation_gate()
            .validate_quiescent_frozen(admission.data().old_range().id, point.frozen)?;
        parent.check()?;
        let _version = admission.publication_guard()?;
        admission.require_memory_current()?;
        Ok(receipt)
    }
}
