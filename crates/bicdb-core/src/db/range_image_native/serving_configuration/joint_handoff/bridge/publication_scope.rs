//! A borrowed LOCAL preparation proof under the actual native journal. It
//! cannot escape the database/owner lifetime or cross a network operation.
use super::*;

pub(crate) struct NativeJointPreparedPublicationScope<'a> {
    pub(super) database: &'a BicDb,
    pub(super) prepared: &'a NativeJointServingPreparation,
    pub(super) store: &'a RangeWriteStore,
    pub(super) committed: &'a CommittedNativeJointDecisionObservation,
    pub(super) catalogue: &'a NativeJointBridgeCatalogue,
    pub(super) image: Option<&'a dyn NativeJointImageMaterialization>,
    pub(super) frozen: &'a FrozenRangeMutationPermit,
    pub(super) journal: crate::db::range_image_native::NativeImageJournalGuard<'a>,
    lineage: SourceRangeImageLineage,
    #[cfg(feature = "tls-replication")]
    registered_identity: Option<crate::db::NativeRegisteredJointIdentityBorrow<'a>>,
}
impl NativeJointServingPreparation {
    pub(crate) fn publication_read_scope<'a>(
        &'a self,
        database: &'a BicDb,
        store: &'a RangeWriteStore,
        committed: &'a CommittedNativeJointDecisionObservation,
        catalogue: &'a NativeJointBridgeCatalogue,
        image: Option<&'a dyn NativeJointImageMaterialization>,
        frozen: &'a FrozenRangeMutationPermit,
    ) -> Result<NativeJointPreparedPublicationScope<'a>> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
            "publication-native-journal-preflight",
            self.local_node_id(),
            committed.deadline(),
        );
        #[cfg(feature = "tls-replication")]
        let registered_identity = image
            .and_then(|i| i.registered_target())
            .map(|t| database.borrow_registered_joint_identity(t.clone()))
            .transpose()?;
        let journal = database.try_native_image_journal()?;
        self.native.require_current(database)?;
        let lineage = database.current_native_range_source_lineage(self.record.range_id)?;
        // No Controlled permit is held yet. In particular this preflight must
        // never count its own metadata callback as an undrained foreign writer.
        catalogue.require_current(store, database, committed, image, frozen)?;
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(self.record.range_id, frozen)?;
        self.record.validate()?;
        self.record.require_binding_with_actual(
            database,
            &self.native,
            committed,
            catalogue,
            &lineage,
        )?;
        for key in [
            preparation_key(self.record.range_id),
            preparation_history_key(self.record.range_id, &self.record.checksum_sha256),
        ] {
            let actual: NativeJointServingPreparationRecord = database
                .read_native_range_image_record(&key)?
                .ok_or_else(|| {
                    invalid("publication scope lost actual preparation pointer/archive")
                })?;
            actual.validate()?;
            if actual != self.record {
                return Err(invalid(
                    "publication scope actual preparation pointer/archive changed",
                ));
            }
        }
        let scope = NativeJointPreparedPublicationScope {
            database,
            prepared: self,
            store,
            committed,
            catalogue,
            image,
            frozen,
            journal,
            lineage,
            #[cfg(feature = "tls-replication")]
            registered_identity,
        };
        scope.require_retained_history(&scope.lineage)?;
        #[cfg(test)]
        trace.complete();
        Ok(scope)
    }
}
impl NativeJointPreparedPublicationScope<'_> {
    pub(crate) fn database(&self) -> &BicDb {
        self.database
    }
    pub(crate) fn require_committed_identity(
        &self,
        committed: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        #[cfg(feature = "tls-replication")]
        if let Some(original) = &self.registered_identity {
            original.require_current(self.database, self.store)?;
        }
        if committed.data() != self.committed.data()
            || committed.native_receipt().checksum_sha256()
                != self.prepared.record.local_decision_sha256
            || committed.native_receipt().native_database_incarnation()
                != self.prepared.native_database_incarnation()
        {
            return Err(invalid(
                "publication scope changed actual local protected decision/open",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_owned_preparation(
        &self,
        prepared: &NativeJointServingPreparation,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        store: &RangeWriteStore,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        #[cfg(feature = "tls-replication")]
        if let Some(original) = &self.registered_identity {
            original.require_current(self.database, self.store)?;
        }
        if !std::ptr::eq(prepared, self.prepared)
            || !std::ptr::eq(catalogue, self.catalogue)
            || !std::ptr::eq(store, self.store)
            || !std::ptr::eq(frozen, self.frozen)
            || match (self.image, image) {
                (None, None) => false,
                (Some(a), Some(b)) => !std::ptr::eq(a, b),
                _ => true,
            }
        {
            return Err(invalid(
                "publication scope substituted actual store/preparation/image/frozen owner",
            ));
        }
        Ok(())
    }
    /// Re-read EVERY captured retained resolution, even if the native head
    /// and catalogue digest are unchanged. No numeric head can substitute for
    /// a missing or different middle history entry. This is a bounded retained
    /// map check, not a full 0..R export/ancestry proof.
    pub(super) fn require_retained_history(&self, actual: &SourceRangeImageLineage) -> Result<()> {
        self.journal.require_database(self.database)?;
        #[cfg(feature = "tls-replication")]
        if let Some(original) = &self.registered_identity {
            original.require_current(self.database, self.store)?;
        }
        for (index, expected) in self.catalogue.decisions() {
            self.committed.require_current_metadata_only()?;
            if *index == 0 || *index > actual.range_index() {
                return Err(invalid(
                    "publication retained resolution exceeds actual native materialized cut",
                ));
            }
            let entry = self.database.read_native_history_entry(
                self.prepared.record.range_id,
                actual.history_generation(),
                *index,
            )?;
            let matches = match (expected, &entry.outcome) {
                (
                    crate::distribution_range_consensus::NativeReadCatalogueDecision::Applied(
                        expected,
                    ),
                    NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => expected == command_checksum_sha256,
                (
                    crate::distribution_range_consensus::NativeReadCatalogueDecision::Aborted,
                    NativeLogicalHistoryOutcomeV1::Aborted,
                ) => true,
                _ => false,
            };
            if entry.index != *index || !matches {
                return Err(invalid(
                    "publication retained resolution contradicts actual immutable native history",
                ));
            }
        }
        Ok(())
    }
    /// Called ONLY after explicitly reading the current source contract again
    /// after the native write. The old lineage is a comparison baseline, not
    /// a post-write proof. The intended bridge changes effective membership;
    /// it must leave every actual physical/history identity unchanged.
    pub(super) fn require_unchanged_post_lineage(
        &self,
        actual: &SourceRangeImageLineage,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        #[cfg(feature = "tls-replication")]
        if let Some(original) = &self.registered_identity {
            original.require_current(self.database, self.store)?;
        }
        let before = &self.lineage;
        if actual.birth.birth_id != before.birth.birth_id
            || actual.birth.checksum_sha256 != before.birth.checksum_sha256
            || actual.base != before.base
            || actual.current_database_incarnation != before.current_database_incarnation
            || actual.current_schema_generation != before.current_schema_generation
            || actual.native_receipt_sha256() != before.native_receipt_sha256()
            || actual.range_index() != before.range_index()
            || actual.logical_genesis_sha256() != before.logical_genesis_sha256()
            || actual.logical_history_sha256() != before.logical_history_sha256()
            || actual.history_generation() != before.history_generation()
            || actual.actual_local_visibility_sequence()
                != before.actual_local_visibility_sequence()
            || actual.native_write_range()? != self.committed.data().activation_range()
            || &actual.native_write_members()? != self.committed.data().new_members()
        {
            return Err(invalid("native bridge write changed physical/prefix/history/UID/schema/B or selected wrong effective domain"));
        }
        Ok(())
    }
}
