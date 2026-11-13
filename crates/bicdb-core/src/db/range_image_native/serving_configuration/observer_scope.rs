//! PRIVATE same-call observer scope. No mutation-role conversion or cached grant.
use super::*;
use crate::distribution_data::RangeWriteServiceSlotGuard;
#[cfg(test)]
use crate::distribution_range_recovery::NativeProbeTraceSpan;
use crate::distribution_range_recovery::{NativeReadyRequest, RecoveryObservationRoleAdmission};

pub(crate) struct NativeReadyObserverReadScope<'a> {
    database: &'a BicDb,
    store: &'a RangeWriteStore,
    prepared: &'a PreparedNativeRecoveryReady,
    observer: &'a RecoveryObservationRoleAdmission,
    request: &'a NativeReadyRequest,
    parent: &'a CancellationToken,
    slot: &'a RangeWriteServiceSlotGuard,
    writer: Arc<crate::db::NativeWriterHostState>,
    journal: NativeImageJournalGuard<'a>,
    lineage: SourceRangeImageLineage,
    joint: Option<NativeCurrentJointRecoveryDomain>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeReadyObserverReadScope<'_> {
    pub(crate) fn database(&self) -> &BicDb {
        self.database
    }
    pub(crate) fn store(&self) -> &RangeWriteStore {
        self.store
    }
    pub(crate) fn prepared(&self) -> &PreparedNativeRecoveryReady {
        self.prepared
    }
    pub(crate) fn observer(&self) -> &RecoveryObservationRoleAdmission {
        self.observer
    }
    pub(crate) fn request(&self) -> &NativeReadyRequest {
        self.request
    }
    pub(crate) fn parent(&self) -> &CancellationToken {
        self.parent
    }
    pub(in crate::db::range_image_native) fn journal(&self) -> &NativeImageJournalGuard<'_> {
        &self.journal
    }
    pub(in crate::db::range_image_native) fn lineage(&self) -> &SourceRangeImageLineage {
        &self.lineage
    }
    pub(crate) fn joint(&self) -> Result<&NativeCurrentJointRecoveryDomain> {
        self.joint
            .as_ref()
            .ok_or_else(|| invalid("observer scope actual joint fact not installed"))
    }
    pub(crate) fn require_identity(
        &self,
        database: &BicDb,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeReadyRequest,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        if !std::ptr::eq(database, self.database)
            || !std::ptr::eq(prepared, self.prepared)
            || !std::ptr::eq(store, self.store)
            || !std::ptr::eq(observer, self.observer)
            || !std::ptr::eq(request, self.request)
        {
            return Err(invalid(
                "observer scope substituted actual DB/Prepared/Store/observer/request",
            ));
        }
        self.parent.check()?;
        observer.require_scope(&request.scope)?;
        self.slot.require_live()?;
        if self.slot.id() != store.native_writer_store_open_id() {
            return Err(invalid("observer scope actual service Store slot differs"));
        }
        let actual = store.native_writer_host_state()?;
        if !Arc::ptr_eq(&actual, &self.writer) {
            return Err(invalid("observer scope changed original writer object"));
        }
        #[cfg(test)]
        let writer_trace = NativeProbeTraceSpan::current("observer-identity-full-writer");
        self.writer
            .require_database(database, observer.current_range())?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(writer_trace);
        #[cfg(test)]
        let pin_trace = NativeProbeTraceSpan::current("observer-identity-owned-native-pin");
        prepared
            .physical_for_completion_scope()
            .require_owned_pin_live(database)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(pin_trace);
        #[cfg(test)]
        let identity_trace = NativeProbeTraceSpan::current("observer-identity-native-fields");
        observer
            .require_native_identity(prepared.physical_for_completion_scope().native_admission())?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(identity_trace);
        self.parent.check()
    }
    pub(crate) fn require_serving(&self, expected_sha: &str) -> Result<()> {
        self.parent.check()?;
        let actual = &self.lineage;
        #[cfg(test)]
        let current_trace = NativeProbeTraceSpan::current("observer-serving-current-readback");
        let record = self
            .database
            .read_native_recovered_serving_configuration(
                &actual.birth,
                &actual.base,
                actual.prefix.as_ref(),
            )?
            .ok_or_else(|| invalid("observer scope actual Completed serving pointer missing"))?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(current_trace);
        #[cfg(test)]
        let archive_trace = NativeProbeTraceSpan::current("observer-serving-archive-readback");
        let archive: NativeRecoveredServingConfiguration = self
            .database
            .read_native_range_image_record(&history_key(record.range.id, &record.checksum_sha256))?
            .ok_or_else(|| invalid("observer scope immutable serving archive missing"))?;
        archive.validate_historical_floor(&actual.birth, &actual.base)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(archive_trace);
        let physical = self.prepared.physical_for_completion_scope();
        if archive != record
            || actual.serving.as_ref() != Some(&record)
            || !record.binds_current_base(&actual.base)
            || record.checksum_sha256 != expected_sha
            || !record.scope.same_session(self.prepared.scope())
            || record.activation_index != physical.range_index()
            || record.activation_history_sha256 != physical.logical_history_sha256()
            || record.logical_genesis_sha256 != physical.logical_genesis_sha256()
            || record.native_receipt_at_activation_sha256 != physical.native_receipt_sha256()
            || record.local_visibility_at_activation != physical.visibility().get()
        {
            return Err(invalid(
                "observer scope serving current/archive changed actual R/H/B/native binding",
            ));
        }
        self.observer.require_scope(&self.request.scope)?;
        self.parent.check()
    }
    fn require_exit(&self) -> Result<()> {
        #[cfg(test)]
        let identity_trace = NativeProbeTraceSpan::current("observer-exit-initial-identity");
        self.require_identity(
            self.database,
            self.prepared,
            self.store,
            self.observer,
            self.request,
        )?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(identity_trace);
        // Actual current physical/generation/schema/native comparison, not a
        // borrowed prestate used as poststate. No second lineage construction.
        #[cfg(test)]
        let lineage_trace = NativeProbeTraceSpan::current("observer-exit-actual-lineage");
        self.database.validate_source_range_image_lineage(
            &self.lineage,
            self.request.scope.range_id,
            self.prepared.range_index(),
        )?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(lineage_trace);
        let joint = self.joint()?;
        #[cfg(test)]
        let joint_trace = NativeProbeTraceSpan::current("observer-exit-full-joint-saved-binding");
        joint.require_current_in_observer_scope(self)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(joint_trace);
        #[cfg(test)]
        let prepared_trace = NativeProbeTraceSpan::current("observer-exit-prepared-binding");
        self.prepared.require_current_in_observer_scope(self)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(prepared_trace);
        self.require_identity(
            self.database,
            self.prepared,
            self.store,
            self.observer,
            self.request,
        )
    }
}
impl BicDb {
    pub(crate) fn with_native_ready_observer_scope<R>(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeReadyRequest,
        slot: &RangeWriteServiceSlotGuard,
        parent: &CancellationToken,
        action: impl for<'scope> FnOnce(&NativeReadyObserverReadScope<'scope>) -> Result<R>,
    ) -> Result<R> {
        parent.check()?;
        #[cfg(test)]
        let entry_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
            "observer-scoped-entry",
        );
        observer.require_scope(&request.scope)?;
        if !matches!(
            request.scope.purpose,
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            return Err(invalid(
                "observer scope requires actual CurrentJoint observer purpose",
            ));
        }
        slot.require_live()?;
        let writer = store.native_writer_host_state()?;
        #[cfg(test)]
        let writer_trace = NativeProbeTraceSpan::current("observer-constructor-full-writer");
        writer.require_database(self, observer.current_range())?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(writer_trace);
        #[cfg(test)]
        let journal_trace =
            NativeProbeTraceSpan::current("observer-constructor-journal-acquisition");
        let journal = self.try_native_image_journal()?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(journal_trace);
        #[cfg(test)]
        let physical_trace = NativeProbeTraceSpan::current("observer-constructor-full-physical");
        prepared
            .physical_for_completion_scope()
            .require_owned_materialization_current(self)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(physical_trace);
        #[cfg(test)]
        let lineage_trace = NativeProbeTraceSpan::current("observer-constructor-actual-lineage");
        let lineage = self
            .current_native_range_source_lineage_under_journal(request.scope.range_id, &journal)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(lineage_trace);
        if lineage.range_index() != prepared.range_index()
            || lineage.logical_genesis_sha256() != prepared.logical_genesis_sha256()
            || lineage.logical_history_sha256() != prepared.logical_history_sha256()
            || lineage.native_receipt_sha256() != prepared.native_receipt_sha256()
            || lineage.actual_local_visibility_sequence() != prepared.local_visibility_sequence()
        {
            return Err(invalid("observer scope changed actual owned native point"));
        }
        let mut scope = NativeReadyObserverReadScope {
            database: self,
            prepared,
            store,
            observer,
            request,
            slot,
            parent,
            writer,
            journal,
            lineage,
            joint: None,
            _local: std::marker::PhantomData,
        };
        #[cfg(test)]
        let identity_trace = NativeProbeTraceSpan::current("observer-constructor-initial-identity");
        scope.require_identity(self, prepared, store, observer, request)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(identity_trace);
        #[cfg(test)]
        let joint_trace = NativeProbeTraceSpan::current("observer-constructor-joint-fact");
        scope.joint = Some(self.observe_current_joint_domain_in_observer_scope(&scope)?);
        #[cfg(test)]
        NativeProbeTraceSpan::finish(joint_trace);
        #[cfg(test)]
        let prepared_trace = NativeProbeTraceSpan::current("observer-constructor-prepared-binding");
        prepared.require_current_in_observer_scope(&scope)?;
        #[cfg(test)]
        NativeProbeTraceSpan::finish(prepared_trace);
        #[cfg(test)]
        crate::distribution_range_recovery::NativeProbeTraceSpan::finish(entry_trace);
        #[cfg(test)]
        observer_scope_tests::after_entry(&scope)?;
        let result = action(&scope)?;
        #[cfg(test)]
        let exit_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
            "observer-scoped-exit",
        );
        scope.require_exit()?;
        #[cfg(test)]
        crate::distribution_range_recovery::NativeProbeTraceSpan::finish(exit_trace);
        Ok(result)
    }
}
#[cfg(test)]
mod observer_scope_tests;
