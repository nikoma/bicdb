//! Original recipient physical ownership retained across catalogue IO. No
//! native WAL replay and no reconstructed local prepared decision.
use super::*;

/// Sealed lexical proof: every physical owner/hook/journal is still borrowed.
/// The range Store remains separately borrowed by the actual catalogue sink.
pub(crate) struct NativeRemoteCataloguePhysicalPoint<'a> {
    physical: RemotePhysicalScope<'a>,
}
impl NativeRemoteCataloguePhysicalPoint<'_> {
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
    ) -> Result<()> {
        admission.require_current()?;
        admission.require_known_catalogue_native(db)?;
        match &self.physical {
            RemotePhysicalScope::Registered(scope) => {
                admission.require_target(scope.target())?;
                // A SAME retained candidate must remain repairable while the
                // generic catalogue durability predicate correctly refuses.
                scope.require_identity_point(db, store)?;
            }
            RemotePhysicalScope::Old(scope) => scope.require_current(db, store, admission)?,
        }
        admission.metadata().require_native_store_point(db, store)?;
        admission.require_memory_current()
    }
}
impl BicDb {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn confirm_remote_registered_decision_catalogue(
        &self,
        target: &crate::distribution_native_host::NativeRegisteredRemoteJointTarget,
        frozen: &FrozenRangeMutationPermit,
        store: &mut RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        retained: &mut Option<
            crate::distribution_range_consensus::NativeRemoteDecisionCataloguePublication,
        >,
        observe_only: bool,
        parent: &CancellationToken,
    ) -> Result<String> {
        admission.require_target(target)?;
        let physical = self.registered_joint_exclusion_scope(target, frozen, parent)?;
        self.remote_decision_catalogue(
            store,
            admission,
            retained,
            NativeRemoteCataloguePhysicalPoint {
                physical: RemotePhysicalScope::Registered(physical),
            },
            observe_only,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn confirm_remote_old_decision_catalogue(
        &self,
        writer: &crate::db::NativeWriterHostState,
        native: &NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        store: &mut RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        retained: &mut Option<
            crate::distribution_range_consensus::NativeRemoteDecisionCataloguePublication,
        >,
        observe_only: bool,
        parent: &CancellationToken,
    ) -> Result<String> {
        let physical = self.remote_old_decision_writer_scope(
            writer, native, promise, frozen, store, admission, parent,
        )?;
        self.remote_decision_catalogue(
            store,
            admission,
            retained,
            NativeRemoteCataloguePhysicalPoint {
                physical: RemotePhysicalScope::Old(physical),
            },
            observe_only,
        )
    }
    fn remote_decision_catalogue(
        &self,
        store: &mut RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        retained: &mut Option<
            crate::distribution_range_consensus::NativeRemoteDecisionCataloguePublication,
        >,
        physical: NativeRemoteCataloguePhysicalPoint<'_>,
        observe_only: bool,
    ) -> Result<String> {
        physical.require_current(self, store, admission)?;
        if retained.is_none() {
            if observe_only {
                return Err(invalid("remote catalogue original owned candidate absent"));
            }
            // Retain exact bytes and original preimage BEFORE memory/IO.
            *retained = Some(crate::distribution_range_consensus::NativeRemoteDecisionCataloguePublication::begin(
                store, self, admission, &physical,
            )?);
        }
        let actual = retained.as_mut().unwrap();
        if observe_only {
            actual.observe(self, store, admission, &physical)
        } else {
            actual.advance(self, store, admission, &physical)
        }
    }
}
