//! Current-certificate-quorum historical materialization. A known immutable
//! prefix remains a lower bound when pending rows exist; it is NOT a source
//! image or serving authority. Original command identity and current recovery
//! authority are deliberately separate domains.
use super::*;
use crate::distribution_range_consensus::{
    RangeWriteStore, RecoveryLocalHistoricalApplyObservation,
};
use crate::distribution_range_mutation::{
    ControlledFrozenMutationPermit, FrozenRangeMutationPermit,
};
use crate::distribution_range_recovery::{RecoveryApplyAdmission, RecoveryRoleAdmission};

/// Actual local immutable prefix + exact pending observation, bound to current
/// native DB/store opens. No serde/Clone/numeric constructor. In particular this
/// cannot be passed to SourceCapture or replace NativeRecoveryReady.
#[derive(Debug)]
pub(crate) struct KnownNativeRecoveryPrefix {
    database_incarnation: String,
    lineage: SourceRangeImageLineage,
    pending: Option<PendingNativeRangeCommand>,
    catalog: RecoveryLocalHistoricalApplyObservation,
}
impl KnownNativeRecoveryPrefix {
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        self.lineage.cluster_id()
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.lineage.bootstrap_id()
    }
    pub(crate) fn base_range(&self) -> &RangeDescriptor {
        self.lineage.base_range()
    }
    pub(crate) fn base_members(&self) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        self.lineage.base_members()
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        self.lineage.schema_sha256()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.lineage.range_index()
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.lineage.local_node_id()
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.lineage.local_member_incarnation()
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.lineage.restore_activation()
    }
}

#[allow(clippy::too_many_arguments)]
fn require_current_historical_apply(
    database: &BicDb,
    store: &RangeWriteStore,
    native: &NativeRecoveryAdmission,
    role: &RecoveryRoleAdmission,
    admission: &RecoveryApplyAdmission,
    frozen: &FrozenRangeMutationPermit,
    controlled: &ControlledFrozenMutationPermit,
) -> Result<RecoveryLocalHistoricalApplyObservation> {
    admission.require_live_request()?;
    admission.require_recipient_role(role)?;
    native.require_current(database)?;
    if native.cluster_id() != admission.cluster_id()
        || native.bootstrap_id() != admission.bootstrap_id()
        || native.local_node_id() != admission.local_node_id()
        || native.local_member_incarnation() != admission.local_member_incarnation()
        || native.restore_activation() != admission.restore_activation()
        || frozen.owner() != admission.scope().freeze_owner()?
    {
        return Err(invalid(
            "historical native admission/birth/owned freeze differ",
        ));
    }
    database
        .range_mutations
        .validate_frozen(admission.command().range_id, frozen)?;
    database
        .range_mutations
        .validate_controlled(admission.command().range_id, controlled)?;
    let observation = store.observe_local_historical_apply(database, native, role, admission)?;
    observation.require_current(store, database, native, role, admission)?;
    Ok(observation)
}

impl BicDb {
    /// No network, drain or global sequence wait occurs under these borrowed
    /// guards. Runtime acquires/drains the same-session freeze beforehand.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_known_native_recovery_prefix(
        &self,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<KnownNativeRecoveryPrefix> {
        let catalog = require_current_historical_apply(
            self, store, native, role, admission, frozen, controlled,
        )?;
        let _journal = self.try_native_image_journal()?;
        // This private contract producer checks actual metadata/history/UIDs,
        // but intentionally retains pending for reconciliation. Its result is
        // never returned as a public SourceLineage while pending exists.
        let lineage = self.native_range_source_lineage_contract(admission.command().range_id)?;
        let pending: Option<PendingNativeRangeCommand> =
            self.read_native_range_image_record(&pending_key(admission.command().range_id))?;
        if let Some(pending) = &pending {
            pending.validate()?;
            let command = admission.command();
            if pending.birth_id != lineage.birth.birth_id
                || pending.range_id != command.range_id
                || pending.command_id != command.command_id
                || pending.command_checksum_sha256 != command.checksum_sha256
                || pending.mutations_sha256
                    != crate::db::admission_mutations_sha256(&command.mutations)?
                || pending.range_index != command.index
                || pending.previous_index != lineage.range_index()
                || pending.previous_receipt_sha256 != lineage.native_receipt_sha256()
            {
                return Err(invalid(
                    "historical native pending contradicts actual selected command/prefix",
                ));
            }
            if self
                .native_range_command_scopes
                .try_lock()
                .ok_or_else(|| invalid("historical native scope registry busy"))?
                .entries
                .contains_key(&pending.expected_local_transaction_id)
            {
                return Err(invalid(
                    "historical native previous local attempt still owns its scope",
                ));
            }
        }
        let command = admission.command();
        if command.index
            > lineage
                .range_index()
                .checked_add(1)
                .ok_or_else(|| invalid("historical native prefix index overflow"))?
        {
            return Err(invalid(
                "historical native prefix gap requires certified image repair",
            ));
        }
        if command.index <= lineage.range_index() {
            let entry = self.read_native_history_entry(
                command.range_id,
                lineage.history_generation(),
                command.index,
            )?;
            if entry.outcome
                != (history::NativeLogicalHistoryOutcomeV1::Applied {
                    command_checksum_sha256: command.checksum_sha256.clone(),
                })
            {
                return Err(invalid(
                    "historical native completed history contradicts selected command",
                ));
            }
        }
        catalog.require_current(store, self, native, role, admission)?;
        Ok(KnownNativeRecoveryPrefix {
            database_incarnation: self.incarnation.clone(),
            lineage,
            pending,
            catalog,
        })
    }
}

const MAX_RECOVERY_LOCAL_ATTEMPTS: u16 = 32;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct RecoveryNativeAttemptBinding {
    format_version: u32,
    attempt_id: Uuid,
    attempt_number: u16,
    previous_pending_sha256: Option<String>,
    local_database_incarnation: String,
    command_sha256: String,
    recovery_nonce: Uuid,
    configuration_sha256: String,
    metadata_context_id: Uuid,
}
impl RecoveryNativeAttemptBinding {
    pub(super) fn validate_prefix(
        &self,
        command_sha256: &str,
        materialized: &MaterializedNativeCommand,
        evidence: &NativeRangeCertificateEvidence,
    ) -> Result<()> {
        if self.command_sha256 != command_sha256
            || self.local_database_incarnation != materialized.database_incarnation
            || !matches!(evidence,NativeRangeCertificateEvidence::HistoricalCurrentCertificateQuorum {
                recovery_nonce,configuration_sha256,metadata_context_id,..}
                if *recovery_nonce==self.recovery_nonce && *configuration_sha256==self.configuration_sha256
                    && *metadata_context_id==self.metadata_context_id)
        {
            return Err(invalid(
                "native recovered prefix actual attempt/certificate/WAL provenance differs",
            ));
        }
        Ok(())
    }
    pub(super) fn validate(&self, pending: &PendingNativeRangeCommand) -> Result<()> {
        if self.format_version != 1
            || self.attempt_id.is_nil()
            || self.recovery_nonce.is_nil()
            || self.metadata_context_id.is_nil()
            || self.attempt_number == 0
            || self.attempt_number > MAX_RECOVERY_LOCAL_ATTEMPTS
            || self.local_database_incarnation.is_empty()
            || self.local_database_incarnation.len() > 128
            || self.command_sha256 != pending.command_checksum_sha256
            || self.configuration_sha256.len() != 64
            || self
                .previous_pending_sha256
                .as_ref()
                .is_some_and(|sha| sha.len() != 64)
        {
            return Err(invalid("damaged/bounded native recovery attempt binding"));
        }
        Ok(())
    }
}
impl KnownNativeRecoveryPrefix {
    #[allow(clippy::too_many_arguments)]
    fn require_current(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<()> {
        if self.database_incarnation != database.incarnation {
            return Err(invalid(
                "known native recovery prefix belongs to another DB open",
            ));
        }
        require_current_historical_apply(
            database, store, native, role, admission, frozen, controlled,
        )?;
        self.catalog
            .require_current(store, database, native, role, admission)?;
        database.validate_source_range_image_lineage_contract(
            &self.lineage,
            admission.command().range_id,
            self.lineage.range_index(),
        )?;
        let actual: Option<PendingNativeRangeCommand> =
            database.read_native_range_image_record(&pending_key(admission.command().range_id))?;
        if actual != self.pending {
            return Err(invalid(
                "actual native pending changed after recovery observation",
            ));
        }
        if let Some(previous) = &self.pending {
            if database
                .native_range_command_scopes
                .try_lock()
                .ok_or_else(|| invalid("native recovery scope registry busy before adoption"))?
                .entries
                .contains_key(&previous.expected_local_transaction_id)
            {
                return Err(invalid(
                    "native recovery previous local scope became active",
                ));
            }
        }
        Ok(())
    }
}
impl BicDb {
    /// Persist a NEW actual local transaction attempt before data replay.
    /// The previous pending record remains immutable forensic evidence. This
    /// never treats a missing old AppWAL frame or a same-value row as a floor.
    #[allow(clippy::too_many_arguments)]
    fn prepare_historical_native_pending(
        &self,
        known: &KnownNativeRecoveryPrefix,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
        certificate: &NativeRangeCommitCertificate,
        local_transaction_id: u64,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<NativeRangePendingApply> {
        let _journal = self.try_native_image_journal()?;
        known.require_current(self, store, native, role, admission, frozen, controlled)?;
        let intent = certificate.intent();
        let command = admission.command();
        if !matches!(
            certificate.evidence(),
            NativeRangeCertificateEvidence::HistoricalCurrentCertificateQuorum { .. }
        ) || intent.command_id() != command.command_id
            || intent.command_checksum_sha256() != command.checksum_sha256
            || intent.range_index()
                != known
                    .range_index()
                    .checked_add(1)
                    .ok_or_else(|| invalid("native recovery index overflow"))?
            || command.index != intent.range_index()
        {
            return Err(invalid(
                "native recovery intent/certificate differs from exact next command",
            ));
        }
        let previous_attempt = known
            .pending
            .as_ref()
            .and_then(|pending| pending.recovery_attempt.as_ref())
            .map_or(0, |attempt| attempt.attempt_number);
        let attempt_number=previous_attempt.checked_add(1)
            .filter(|attempt|*attempt<=MAX_RECOVERY_LOCAL_ATTEMPTS)
            .ok_or_else(||invalid("bounded native recovery attempts exhausted; certified repair/diagnosis required"))?;
        let (global_image_generation, range_image_generation) =
            self.native_range_image_generations(&known.lineage.birth, command.range_id)?;
        let mut pending = PendingNativeRangeCommand {
            format_version: 1,
            birth_id: known.lineage.birth.birth_id,
            range_id: command.range_id,
            range_index: command.index,
            previous_index: known.range_index(),
            previous_receipt_sha256: known.lineage.native_receipt_sha256().to_string(),
            command_id: command.command_id.clone(),
            command_checksum_sha256: command.checksum_sha256.clone(),
            mutations_sha256: intent.mutations_sha256().to_string(),
            scope_sha256: intent.scope_sha256().to_string(),
            expected_local_transaction_id: local_transaction_id,
            global_image_generation,
            range_image_generation,
            materialized: None,
            recovery_attempt: Some(RecoveryNativeAttemptBinding {
                format_version: 1,
                attempt_id: Uuid::now_v7(),
                attempt_number,
                previous_pending_sha256: known
                    .pending
                    .as_ref()
                    .map(|old| old.checksum_sha256.clone()),
                local_database_incarnation: self.incarnation.clone(),
                command_sha256: command.checksum_sha256.clone(),
                recovery_nonce: admission.recovery_nonce(),
                configuration_sha256: admission.scope().configuration_sha256.clone(),
                metadata_context_id: admission.context_id(),
            }),
            checksum_sha256: String::new(),
        };
        pending.checksum_sha256 = pending.checksum()?;
        pending.validate()?;
        let scope =
            self.reserve_native_command_scope(local_transaction_id, intent.mutations_sha256())?;
        let encoded = bounded_json(&pending, MAX_NATIVE_RECEIPT_BYTES)?;
        self.in_paged_transaction(|paged, xid| {
            if let Some(previous) = &known.pending {
                let key = native_key(&format!(
                    "pending-attempt:{}:{}",
                    command.range_id.get(),
                    previous.checksum_sha256
                ));
                let previous_encoded = bounded_json(previous, MAX_NATIVE_RECEIPT_BYTES)?;
                if let Some(existing) = paged
                    .store()
                    .get_as_of(&paged.latest_snapshot(), &key)
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
                {
                    if existing != previous_encoded {
                        return Err(invalid(
                            "native previous attempt immutable archive conflicts",
                        ));
                    }
                } else {
                    paged
                        .store()
                        .put(xid, &key, &previous_encoded)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
            }
            paged
                .store()
                .put(xid, &native_key(&pending_key(command.range_id)), &encoded)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native pending adoption has no page store"))?;
        // No row operation can use this scope before its actual native commit.
        scope.arm()?;
        Ok(NativeRangePendingApply {
            database_incarnation: self.incarnation.clone(),
            intent: intent.clone(),
            pending,
            _scope: scope,
        })
    }
}

/// Actual row/WAL/native-prefix outcome for the exact original command. This
/// is not a serving/configuration authority and does not reconstruct omitted
/// original audit/sync/queue effects. No serde/Clone/counter constructor.
#[derive(Debug)]
pub(crate) struct NativeRecoveryAppliedReceipt {
    database_incarnation: String,
    birth_sha256: String,
    command_id: String,
    command_sha256: String,
    command_index: u64,
    recovery_nonce: Uuid,
    configuration_sha256: String,
    role_context_id: Uuid,
    lineage: SourceRangeImageLineage,
}
impl NativeRecoveryAppliedReceipt {
    pub(crate) fn native_database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.lineage.base_range().id
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.command_index
    }
    pub(crate) fn command_id(&self) -> &str {
        &self.command_id
    }
    pub(crate) fn command_sha256(&self) -> &str {
        &self.command_sha256
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        self.lineage.native_receipt_sha256()
    }
    pub(crate) fn actual_native_applied_through(&self) -> u64 {
        self.lineage.range_index()
    }
    pub(crate) fn local_visibility_db_sequence(&self) -> u64 {
        self.lineage.actual_local_visibility_sequence()
    }
    pub(crate) fn recovery_nonce(&self) -> Uuid {
        self.recovery_nonce
    }
    pub(crate) fn configuration_sha256(&self) -> &str {
        &self.configuration_sha256
    }
    pub(crate) fn role_context_id(&self) -> Uuid {
        self.role_context_id
    }
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
    ) -> Result<()> {
        admission.require_live_request()?;
        admission.require_recipient_role(role)?;
        native.require_current(database)?;
        let command = admission.command();
        if self.database_incarnation != database.incarnation
            || self.birth_sha256 != native.birth_sha256()
            || self.command_id != command.command_id
            || self.command_sha256 != command.checksum_sha256
            || self.command_index != command.index
            || self.recovery_nonce != admission.recovery_nonce()
            || self.configuration_sha256 != admission.scope().configuration_sha256
            || self.role_context_id != role.context_id()
            || self.range_id() != command.range_id
        {
            return Err(invalid(
                "actual historical native receipt scope/open/command changed",
            ));
        }
        database.validate_source_range_image_lineage(
            &self.lineage,
            command.range_id,
            self.lineage.range_index(),
        )?;
        let entry = database.read_native_history_entry(
            command.range_id,
            self.lineage.history_generation(),
            command.index,
        )?;
        if entry.outcome
            != (history::NativeLogicalHistoryOutcomeV1::Applied {
                command_checksum_sha256: command.checksum_sha256.clone(),
            })
        {
            return Err(invalid("actual historical native receipt history differs"));
        }
        Ok(())
    }
}
impl BicDb {
    fn historical_native_applied_receipt(
        &self,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
    ) -> Result<NativeRecoveryAppliedReceipt> {
        let lineage = self.current_native_range_source_lineage(admission.command().range_id)?;
        let receipt = NativeRecoveryAppliedReceipt {
            database_incarnation: self.incarnation.clone(),
            birth_sha256: native.birth_sha256().to_string(),
            command_id: admission.command().command_id.clone(),
            command_sha256: admission.command().checksum_sha256.clone(),
            command_index: admission.command().index,
            recovery_nonce: admission.recovery_nonce(),
            configuration_sha256: admission.scope().configuration_sha256.clone(),
            role_context_id: role.context_id(),
            lineage,
        };
        receipt.require_current(self, native, role, admission)?;
        Ok(receipt)
    }
    /// Reconcile the original certified record mutations using a NEW physical
    /// local transaction/WAL receipt. Runtime owns the freeze/controlled lease
    /// through return; no network/drain/DB-lock reentry occurs here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_historical_native_command(
        &self,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
        cancellation: &CancellationToken,
    ) -> Result<NativeRecoveryAppliedReceipt> {
        cancellation.check()?;
        let known = self.observe_known_native_recovery_prefix(
            store, native, role, admission, frozen, controlled,
        )?;
        let command = admission.command();
        if command.index <= known.range_index() {
            // Actual immutable native history, not raw Applied/row equality,
            // proved coverage. Do not replay old values over later commands.
            known.require_current(self, store, native, role, admission, frozen, controlled)?;
            return self.historical_native_applied_receipt(native, role, admission);
        }
        let certificate = NativeRangeCommandIntent::for_historical_apply(admission, &known)?;
        let authority = RangeMutationAuthority::Controlled(controlled);
        let mut transaction = self.begin_transaction()?;
        let result = (|| {
            let pending = self.prepare_historical_native_pending(
                &known,
                store,
                native,
                role,
                admission,
                &certificate,
                transaction.id().0,
                frozen,
                controlled,
            )?;
            transaction.bypass_commit_admission = true;
            transaction.imported_projection = true;
            transaction.range_mutation_claim = Some(authority.claim());
            transaction.set_snapshot_isolation(false, 0)?;
            for mutation in &command.mutations {
                cancellation.check()?;
                let reserved = transaction.read_committed_update_record_nowait(
                    &mutation.collection,
                    &mutation.record_id,
                )?;
                let snapshot = reserved.map_or(0, |(_, sequence)| sequence);
                match &mutation.record {
                    Some(record) => transaction.write_upserts_with_statement_snapshots(
                        &mutation.collection,
                        [(record.clone(), snapshot)],
                    )?,
                    None => transaction.delete_many_with_statement_snapshots(
                        &mutation.collection,
                        [(&mutation.record_id, snapshot)],
                    )?,
                }
            }
            cancellation.check()?;
            let sequence = self.commit_transaction(&mut transaction)?;
            self.tx_log.write_durable(sequence)?;
            let receipt = transaction.mint_native_wal_apply_receipt()?;
            known
                .catalog
                .require_current(store, self, native, role, admission)?;
            self.record_native_range_materialized(&pending, &receipt, &authority)?;
            transaction.finalize_commit_admission()?;
            transaction.finalize_committed_broker_outbox();
            transaction.finalize_committed_memory_jobs()?;
            self.complete_native_range_command(&pending, &certificate, &receipt, &authority)?;
            known
                .catalog
                .require_current(store, self, native, role, admission)?;
            self.historical_native_applied_receipt(native, role, admission)
        })();
        match result {
            Ok(receipt) => {
                self.auto_checkpoint_if_due();
                Ok(receipt)
            }
            Err(error) => {
                // No distributed abort is published. The actual local cleanup
                // finishes before the borrowed owned controlled lease can drop.
                self.finish_failed_native_local_attempt(&mut transaction);
                Err(BicDbError::CommitOutcomeUnknown {sequence:transaction.committed_seq,
                    transaction_id:Some(transaction.id().0),incarnation:self.incarnation.clone(),
                    detail:format!("original certified command {} requires native historical recovery: {error}",command.command_id)})
            }
        }
    }
}
