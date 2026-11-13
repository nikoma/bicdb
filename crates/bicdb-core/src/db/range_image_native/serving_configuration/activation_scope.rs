//! Same-call activation validation, never a reusable currentness grant.
//! Catalogue fsync is OUTSIDE this actual native journal borrow.
use super::*;
use crate::distribution_data::RangeWriteServiceSlotGuard;
use crate::distribution_range_recovery::NativeCompletionQuorum;

pub(crate) struct NativeActivationReadScope<'a> {
    database: &'a BicDb,
    store: &'a RangeWriteStore,
    prepared: &'a PreparedNativeRecoveryReady,
    quorum: &'a NativeCompletionQuorum,
    fresh: &'a RecoveryRoleAdmission,
    parent: &'a CancellationToken,
    slot: &'a RangeWriteServiceSlotGuard,
    writer: Arc<crate::db::NativeWriterHostState>,
    journal: NativeImageJournalGuard<'a>,
    lineage: SourceRangeImageLineage,
    joint: Option<NativeCurrentJointRecoveryDomain>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeActivationReadScope<'_> {
    pub(crate) fn database(&self) -> &BicDb {
        self.database
    }
    pub(crate) fn store(&self) -> &RangeWriteStore {
        self.store
    }
    pub(crate) fn prepared(&self) -> &PreparedNativeRecoveryReady {
        self.prepared
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        self.fresh
    }
    pub(crate) fn quorum(&self) -> &NativeCompletionQuorum {
        self.quorum
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
            .ok_or_else(|| invalid("activation scope actual joint fact missing"))
    }
    pub(crate) fn require_identity(
        &self,
        database: &BicDb,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        if !std::ptr::eq(database, self.database)
            || !std::ptr::eq(prepared, self.prepared)
            || !std::ptr::eq(store, self.store)
            || !std::ptr::eq(fresh, self.fresh)
        {
            return Err(invalid(
                "activation scope substituted actual DB/Prepared/Store/role",
            ));
        }
        self.parent.check()?;
        self.quorum
            .require_prepared_identity_point(prepared, fresh)?;
        prepared.scope().require_role(fresh)?;
        self.slot.require_live()?;
        if self.slot.id() != store.native_writer_store_open_id() {
            return Err(invalid(
                "activation scope actual service Store slot differs",
            ));
        }
        let writer = store.native_writer_host_state()?;
        if !Arc::ptr_eq(&writer, &self.writer) {
            return Err(invalid("activation scope changed original writer object"));
        }
        self.writer
            .require_activation_lifetime(database, fresh.current_range())?;
        prepared
            .physical_for_completion_scope()
            .require_owned_pin_gate(database)?;
        self.parent.check()
    }
    fn serving_record(&self, expected_sha: &str) -> Result<NativeRecoveredServingConfiguration> {
        self.require_identity(self.database, self.prepared, self.store, self.fresh)?;
        let actual = &self.lineage;
        let record = self
            .database
            .read_native_recovered_serving_configuration(
                &actual.birth,
                &actual.base,
                actual.prefix.as_ref(),
            )?
            .ok_or_else(|| invalid("activation scope actual serving pointer missing"))?;
        let archive: NativeRecoveredServingConfiguration = self
            .database
            .read_native_range_image_record(&history_key(record.range.id, &record.checksum_sha256))?
            .ok_or_else(|| invalid("activation scope immutable serving archive missing"))?;
        archive.validate_historical_floor(&actual.birth, &actual.base)?;
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
                "activation scope serving current/archive differs from actual R/H/B/native point",
            ));
        }
        self.parent.check()?;
        Ok(record)
    }
    pub(crate) fn require_serving(&self, expected_sha: &str) -> Result<()> {
        self.serving_record(expected_sha)?;
        Ok(())
    }
    pub(crate) fn require_serving_receipt(
        &self,
        receipt: &NativeRecoveredServingReceipt,
    ) -> Result<()> {
        self.require_native_admission(&receipt.native)?;
        if self.serving_record(receipt.checksum_sha256())? != receipt.record {
            return Err(invalid(
                "activation scope substituted actual native serving receipt",
            ));
        }
        Ok(())
    }
    pub(crate) fn observe_serving(
        &self,
        expected_sha: &str,
    ) -> Result<NativeRecoveredServingReceipt> {
        let record = self.serving_record(expected_sha)?;
        let native = self.database.native_recovery_admission(
            &record.scope.cluster_id,
            record.scope.bootstrap_id,
            &record.local_node_id,
            record.local_member_incarnation,
        )?;
        self.parent.check()?;
        Ok(NativeRecoveredServingReceipt { native, record })
    }
    pub(in crate::db::range_image_native) fn require_native_admission(
        &self,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        self.require_identity(self.database, self.prepared, self.store, self.fresh)?;
        let actual = self
            .prepared
            .physical_for_completion_scope()
            .native_admission();
        if native.database_incarnation != actual.database_incarnation
            || native.birth_id != actual.birth_id
            || native.birth_sha256 != actual.birth_sha256
            || native.writer_enforcement_version != actual.writer_enforcement_version
            || native.metadata_initialization_version != actual.metadata_initialization_version
            || native.abort_protocol_version != actual.abort_protocol_version
            || native.cluster_id != actual.cluster_id
            || native.bootstrap_id != actual.bootstrap_id
            || native.local_node_id != actual.local_node_id
            || native.local_member_incarnation != actual.local_member_incarnation
            || native.restore_activation != actual.restore_activation
        {
            return Err(invalid(
                "activation scope substituted actual native admission",
            ));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_after_catalogue_entry(&self) -> Result<()> {
        activation_scope_tests::after_entry(self)
    }
    fn require_exit(&self) -> Result<()> {
        self.require_identity(self.database, self.prepared, self.store, self.fresh)?;
        // A decision WAL write changed metadata: reread its current/archive
        // through its fixed consumer, and independently check actual physical
        // state here. The retained prewrite lineage alone is never poststate.
        self.database.validate_source_range_image_lineage(
            &self.lineage,
            self.prepared.scope().range_id,
            self.prepared.range_index(),
        )?;
        self.prepared
            .physical_for_completion_scope()
            .require_owned_pin_live(self.database)?;
        self.writer
            .require_database(self.database, self.fresh.current_range())?;
        self.joint()?.require_current_in_activation_scope(self)?;
        self.prepared.require_current_in_activation_scope(self)?;
        self.require_identity(self.database, self.prepared, self.store, self.fresh)
    }
}
impl BicDb {
    pub(crate) fn with_native_activation_read_scope<R>(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        quorum: &NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
        slot: &RangeWriteServiceSlotGuard,
        parent: &CancellationToken,
        action: impl for<'scope> FnOnce(&NativeActivationReadScope<'scope>) -> Result<R>,
    ) -> Result<R> {
        #[cfg(test)]
        let mut entry_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-activate-scoped-entry",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        parent.check()?;
        quorum.require_prepared_identity_point(prepared, fresh)?;
        if !matches!(
            fresh.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            return Err(invalid(
                "activation scope requires actual CurrentJoint mutation role",
            ));
        }
        slot.require_live()?;
        let writer = store.native_writer_host_state()?;
        writer.require_database(self, fresh.current_range())?;
        let journal = self.try_native_image_journal()?;
        prepared
            .physical_for_completion_scope()
            .require_owned_materialization_current(self)?;
        let lineage = self.current_native_range_source_lineage_under_journal(
            prepared.scope().range_id,
            &journal,
        )?;
        if lineage.range_index() != prepared.range_index()
            || lineage.logical_genesis_sha256() != prepared.logical_genesis_sha256()
            || lineage.logical_history_sha256() != prepared.logical_history_sha256()
            || lineage.native_receipt_sha256() != prepared.native_receipt_sha256()
            || lineage.actual_local_visibility_sequence() != prepared.local_visibility_sequence()
        {
            return Err(invalid(
                "activation scope changed actual owned physical point",
            ));
        }
        let mut scope = NativeActivationReadScope {
            database: self,
            prepared,
            store,
            quorum,
            fresh,
            slot,
            parent,
            writer,
            journal,
            lineage,
            joint: None,
            _local: std::marker::PhantomData,
        };
        scope.require_identity(self, prepared, store, fresh)?;
        scope.joint = Some(self.observe_current_joint_domain_in_activation_scope(&scope)?);
        prepared.require_current_in_activation_scope(&scope)?;
        #[cfg(test)]
        {
            entry_trace.complete();
            drop(entry_trace);
        }
        let result = action(&scope)?;
        #[cfg(test)]
        let mut exit_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-activate-scoped-exit",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        scope.require_exit()?;
        #[cfg(test)]
        exit_trace.complete();
        Ok(result)
    }
}
#[cfg(test)]
mod activation_scope_tests;
