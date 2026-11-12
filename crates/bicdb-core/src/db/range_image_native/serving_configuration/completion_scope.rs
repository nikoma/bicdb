//! One nonescaping actual completion read/write borrow. This is not a durable
//! currentness token: no constructor, Clone, Serde or scope escapes the fixed
//! callback. Catalogue fsync is deliberately outside the native journal hold.
use super::*;

pub(crate) struct NativeCompletionReadScope<'a> {
    database: &'a BicDb,
    store: &'a RangeWriteStore,
    prepared: &'a PreparedNativeRecoveryReady,
    fresh: &'a RecoveryRoleAdmission,
    journal: NativeImageJournalGuard<'a>,
    lineage: SourceRangeImageLineage,
    // Keep !Send independently of the underlying mutex guard implementation.
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeCompletionReadScope<'_> {
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
    pub(in crate::db::range_image_native) fn journal(&self) -> &NativeImageJournalGuard<'_> {
        &self.journal
    }
    pub(in crate::db::range_image_native) fn lineage(&self) -> &SourceRangeImageLineage {
        &self.lineage
    }
    pub(crate) fn observe_current_joint_domain(&self) -> Result<NativeCurrentJointRecoveryDomain> {
        self.database
            .observe_current_joint_domain_in_completion_scope(self)
    }
    pub(crate) fn require_identity(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        if !std::ptr::eq(prepared, self.prepared)
            || !std::ptr::eq(store, self.store)
            || !std::ptr::eq(fresh, self.fresh)
        {
            return Err(invalid(
                "completion scope substituted actual Prepared/Store/role",
            ));
        }
        fresh.require_live_request()?;
        prepared.scope().require_role(fresh)
    }
    // This re-reads the full physical contract after our own WAL write. The
    // journal excludes another protected native publication, not hook clearing,
    // cancellation, schema invalidation or health poison: all remain checked.
    pub(crate) fn require_physical_domain_current(&self) -> Result<()> {
        self.require_identity(self.prepared, self.store, self.fresh)?;
        self.database.validate_source_range_image_lineage(
            &self.lineage,
            self.prepared.scope().range_id,
            self.prepared.range_index(),
        )?;
        self.prepared
            .physical_for_completion_scope()
            .require_owned_pin_live(self.database)?;
        // Refresh deliberately precedes the cached binding update. Validate
        // actual physical/domain/original writer and persisted scope here;
        // this grants no cached binding or completion response.
        self.prepared.require_domain_for_completion_scope(self)?;
        self.fresh.require_live_request()
    }
    pub(crate) fn require_unchanged_native_point(&self) -> Result<()> {
        self.require_physical_domain_current()?;
        // Always required after the fixed callback has installed the ACTUAL
        // updated binding (and on ordinary native producer readback).
        self.prepared
            .require_produced_completion_boundary(self.database, self.store, self.fresh)
    }
    pub(crate) fn observe_serving(
        &self,
        expected_checksum: &str,
    ) -> Result<NativeRecoveredServingReceipt> {
        self.require_identity(self.prepared, self.store, self.fresh)?;
        let actual = &self.lineage;
        let record = self
            .database
            .read_native_recovered_serving_configuration(
                &actual.birth,
                &actual.base,
                actual.prefix.as_ref(),
            )?
            .ok_or_else(|| invalid("completion scope has no actual native serving pointer"))?;
        let archive: NativeRecoveredServingConfiguration = self
            .database
            .read_native_range_image_record(&history_key(record.range.id, &record.checksum_sha256))?
            .ok_or_else(|| invalid("completion scope lost actual immutable serving archive"))?;
        archive.validate_historical_floor(&actual.birth, &actual.base)?;
        let physical = self.prepared.physical_for_completion_scope();
        if archive != record
            || !record.binds_current_base(&actual.base)
            || record.checksum_sha256 != expected_checksum
            || !record.scope.same_session(self.prepared.scope())
            || record.activation_index != physical.range_index()
            || record.activation_history_sha256 != physical.logical_history_sha256()
            || record.logical_genesis_sha256 != physical.logical_genesis_sha256()
            || record.native_receipt_at_activation_sha256 != physical.native_receipt_sha256()
            || record.local_visibility_at_activation != physical.visibility().get()
        {
            return Err(invalid(
                "completion scope current/archive differs from exact owned native point",
            ));
        }
        let native = self.database.native_recovery_admission(
            &record.scope.cluster_id,
            record.scope.bootstrap_id,
            &record.local_node_id,
            record.local_member_incarnation,
        )?;
        // The actual record + immutable archive were read inside THIS journal;
        // no receipt.current() relocks or independently reconstructs lineage.
        self.fresh.require_live_request()?;
        Ok(NativeRecoveredServingReceipt { native, record })
    }
}
impl BicDb {
    pub(crate) fn with_native_completion_read_scope<R>(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
        action: impl for<'scope> FnOnce(&NativeCompletionReadScope<'scope>) -> Result<R>,
    ) -> Result<R> {
        #[cfg(test)]
        let mut entry_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-scoped-native-read-entry",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        fresh.require_live_request()?;
        prepared.scope().require_role(fresh)?;
        let journal = self.try_native_image_journal()?;
        // Full actual physical proof, not the Prepared DTO or a phase flag.
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
                "completion scope changed actual owned physical point",
            ));
        }
        let scope = NativeCompletionReadScope {
            database: self,
            store,
            prepared,
            fresh,
            journal,
            lineage,
            _local: std::marker::PhantomData,
        };
        // Includes exact current joint facts, saved floor/archive constraints
        // and original writer under this real scope. NativePrepared/current
        // binding or the deliberately NEW postCAT binding is validated by its
        // fixed consumer; this read scope itself grants neither phase.
        prepared.require_domain_for_completion_scope(&scope)?;
        #[cfg(test)]
        {
            entry_trace.complete();
            drop(entry_trace);
        }
        let result = action(&scope)?;
        #[cfg(test)]
        let mut exit_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-scoped-native-read-exit",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        scope.require_unchanged_native_point()?;
        #[cfg(test)]
        exit_trace.complete();
        Ok(result)
    }
}
