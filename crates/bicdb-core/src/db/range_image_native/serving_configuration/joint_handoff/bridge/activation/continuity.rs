//! Historical decided-cut continuity ONLY, even while legal newer commands
//! are pending. This type cannot become SourceLineage/Ready/row-read evidence.
use super::*;
pub(crate) struct NativeJointHistoricalCutScope<'a> {
    database: &'a BicDb,
    native: &'a NativeJointServingBridgeReceipt,
    journal: NativeJointScopeJournal<'a>,
    contract: SourceRangeImageLineage,
    pending: Option<(u64, String)>,
}
impl NativeJointServingBridgeReceipt {
    pub(crate) fn historical_cut_scope<'a>(
        &'a self,
        database: &'a BicDb,
    ) -> Result<NativeJointHistoricalCutScope<'a>> {
        self.historical_cut_scope_with_journal(
            database,
            NativeJointScopeJournal::Owned(database.try_native_image_journal()?),
        )
    }
    pub(in crate::db::range_image_native) fn historical_cut_scope_under_journal<'a>(
        &'a self,
        database: &'a BicDb,
        journal: &'a crate::db::range_image_native::NativeImageJournalGuard<'a>,
    ) -> Result<NativeJointHistoricalCutScope<'a>> {
        self.historical_cut_scope_with_journal(database, NativeJointScopeJournal::Borrowed(journal))
    }
    fn historical_cut_scope_with_journal<'a>(
        &'a self,
        database: &'a BicDb,
        journal: NativeJointScopeJournal<'a>,
    ) -> Result<NativeJointHistoricalCutScope<'a>> {
        journal.require_database(database)?;
        self.record.validate()?;
        self.native.require_current(database)?;
        // Contract reader deliberately does NOT assert source visibility while
        // pending. The immutable old-cut membership/anchor is still verified.
        let contract =
            database.native_range_source_lineage_contract(self.record.prepared.range_id)?;
        self.record.require_actual_anchor(
            database,
            &self.native,
            &contract.birth,
            &contract.base,
        )?;
        if contract.range_index() < self.record.prepared.range_index
            || contract.actual_local_visibility_sequence()
                < self.record.prepared.actual_local_visibility_sequence
        {
            return Err(invalid(
                "historical joint cut exceeds actual certified native contract",
            ));
        }
        let current: NativeJointServingBridgeRecord = database
            .read_native_range_image_record(&key(self.record.prepared.range_id))?
            .ok_or_else(|| invalid("historical joint bridge current record missing"))?;
        let archive: NativeJointServingBridgeRecord = database
            .read_native_range_image_record(&archive_key(
                self.record.prepared.range_id,
                &self.record.checksum_sha256,
            ))?
            .ok_or_else(|| invalid("historical joint bridge archive missing"))?;
        current.validate()?;
        archive.validate()?;
        if current != self.record || archive != self.record {
            return Err(invalid("historical joint bridge pointer/archive changed"));
        }
        let pending = database.joint_pending_obligation_under_journal(&contract)?;
        Ok(NativeJointHistoricalCutScope {
            database,
            native: self,
            journal,
            contract,
            pending,
        })
    }
}
impl NativeJointHistoricalCutScope<'_> {
    /// Actual certified CONTRACT position only; pending physical data can be
    /// ahead, so this is never a current row-read/visibility permission.
    pub(crate) fn certified_contract_index(&self) -> u64 {
        self.contract.range_index()
    }
    pub(crate) fn covers_actual_resolution(
        &self,
        index: u64,
        decision: &crate::distribution_range_consensus::NativeReadCatalogueDecision,
    ) -> Result<bool> {
        self.journal.require_database(self.database)?;
        if index > self.contract.range_index() {
            return Ok(false);
        }
        let entry = self.database.read_native_history_entry(
            self.native.data().old_range().id,
            self.contract.history_generation(),
            index,
        )?;
        Ok(entry.index == index
            && match (decision, &entry.outcome) {
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
            })
    }
    pub(crate) fn database(&self) -> &BicDb {
        self.database
    }
    pub(crate) fn native(&self) -> &NativeJointServingBridgeReceipt {
        self.native
    }
    pub(crate) fn pending_obligation(&self) -> Option<&(u64, String)> {
        self.pending.as_ref()
    }
    pub(crate) fn require_native(&self, native: &NativeJointServingBridgeReceipt) -> Result<()> {
        self.journal.require_database(self.database)?;
        if !std::ptr::eq(native, self.native) {
            return Err(invalid("historical cut scope substituted actual receipt"));
        }
        Ok(())
    }
    pub(crate) fn require_committed_identity(
        &self,
        committed: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        if committed.data() != self.native.data()
            || committed.native_receipt().checksum_sha256()
                != self.native.record.prepared.local_decision_sha256
            || committed.native_receipt().native_database_incarnation()
                != self.native.native_database_incarnation()
        {
            return Err(invalid(
                "historical cut scope changed actual local decision/open",
            ));
        }
        Ok(())
    }
    pub(crate) fn covers_actual_command(&self, index: u64, checksum: &str) -> Result<bool> {
        self.journal.require_database(self.database)?;
        if index > self.contract.range_index() {
            return Ok(false);
        }
        let entry = self.database.read_native_history_entry(
            self.native.data().old_range().id,
            self.contract.history_generation(),
            index,
        )?;
        Ok(entry.index == index
            && matches!(entry.outcome,NativeLogicalHistoryOutcomeV1::Applied {command_checksum_sha256} if command_checksum_sha256==checksum))
    }
}
