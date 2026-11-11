//! Durable local pending/materialized/certified boundaries. A pending record
//! never raises the source floor. Only the actual WAL callback AND the opaque
//! coordinator certificate quorum can produce the completed prefix below.
use super::*;
use crate::distribution_range_consensus::{NativeRangeCommandIntent, NativeRangeCommitCertificate};
use crate::distribution_range_mutation::RangeMutationAuthority;

fn pending_key(range: RangeId) -> String {
    format!("pending:{}", range.get())
}
fn prefix_key(range: RangeId) -> String {
    format!("prefix:{}", range.get())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MaterializedNativeCommand {
    local_transaction_id: u64,
    local_wal_sequence: u64,
    actual_mutations_sha256: String,
    database_incarnation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PendingNativeRangeCommand {
    format_version: u32,
    birth_id: Uuid,
    range_id: RangeId,
    range_index: u64,
    previous_index: u64,
    previous_receipt_sha256: String,
    command_id: String,
    command_checksum_sha256: String,
    mutations_sha256: String,
    scope_sha256: String,
    expected_local_transaction_id: u64,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    materialized: Option<MaterializedNativeCommand>,
    checksum_sha256: String,
}
impl PendingNativeRangeCommand {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        if self.format_version != 1
            || self.birth_id.is_nil()
            || self.previous_index.checked_add(1) != Some(self.range_index)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged pending native command"));
        }
        Ok(())
    }
}

/// Current-open handle for one actual durable pending record. Not a commit
/// decision, certificate, visibility permit or serializable recovery token.
#[derive(Debug)]
pub(crate) struct NativeRangePendingApply {
    database_incarnation: String,
    intent: NativeRangeCommandIntent,
    pending: PendingNativeRangeCommand,
}

/// Constant-size durable prefix extension; its native transaction follows the
/// actual data/WAL apply. Previous native receipts form an ordered local
/// history, not portable voter signatures or metadata authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct NativeCertifiedRangePrefix {
    format_version: u32,
    birth_id: Uuid,
    base_receipt_sha256: String,
    range_id: RangeId,
    range_index: u64,
    previous_index: u64,
    previous_receipt_sha256: String,
    command_id: String,
    command_checksum_sha256: String,
    mutations_sha256: String,
    scope_sha256: String,
    materialized: MaterializedNativeCommand,
    certified_voters: BTreeSet<ClusterNodeId>,
    required_quorum: usize,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    checksum_sha256: String,
}
impl NativeCertifiedRangePrefix {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    pub(super) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(super) fn checksum_sha256(&self) -> &str {
        &self.checksum_sha256
    }
    pub(super) fn validate(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeGenesisReceipt,
    ) -> Result<()> {
        if self.format_version != 1
            || self.birth_id != birth.birth_id
            || self.base_receipt_sha256 != base.checksum_sha256
            || self.range_id != base.range.id
            || self.range_index <= base.range_index
            || self.previous_index.checked_add(1) != Some(self.range_index)
            || self.global_image_generation != base.global_image_generation
            || self.range_image_generation != base.range_image_generation
            || self.certified_voters.len() < self.required_quorum
            || self.required_quorum == 0
            || self.certified_voters.len() > 64
            || self.materialized.actual_mutations_sha256 != self.mutations_sha256
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged/mismatched native certified prefix"));
        }
        Ok(())
    }
}

impl BicDb {
    /// Narrow local replica materialization: a concrete coordinator certificate
    /// and exact final mutations are mandatory. Remote/recovery admission is a
    /// separate producer; raw decoded CQ/Applied records cannot call this API.
    pub(crate) fn apply_native_locally_certified_command(
        &self,
        certificate: &NativeRangeCommitCertificate,
        mutations: &[CommitAdmissionMutation],
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let intent = certificate.intent();
        if admission_mutations_sha256(mutations)? != intent.mutations_sha256() {
            return Err(invalid(
                "certified native apply differs from the actual final mutations",
            ));
        }
        let lineage = self.current_native_range_source_lineage(intent.range().id)?;
        if lineage.range_index() == intent.range_index() {
            if lineage.prefix.as_ref().is_some_and(|prefix| {
                prefix.command_id == intent.command_id()
                    && prefix.command_checksum_sha256 == intent.command_checksum_sha256()
                    && prefix.scope_sha256 == intent.scope_sha256()
                    && prefix.mutations_sha256 == intent.mutations_sha256()
            }) {
                return Ok(());
            }
            return Err(invalid(
                "native duplicate index differs from completed command",
            ));
        }
        let mut transaction = self.begin_transaction()?;
        let pending = self.prepare_native_range_command(intent, transaction.id().0, authority)?;
        transaction.bypass_commit_admission = true;
        transaction.range_mutation_claim = Some(authority.claim());
        transaction.set_snapshot_isolation(false, 0)?;
        let result = (|| {
            for mutation in mutations {
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
            let sequence = self.commit_transaction(&mut transaction)?;
            self.tx_log.write_durable(sequence)?;
            let receipt = transaction.mint_native_wal_apply_receipt()?;
            self.record_native_range_materialized(&pending, &receipt, authority)?;
            transaction.finalize_commit_admission()?;
            transaction.finalize_committed_broker_outbox();
            transaction.finalize_committed_memory_jobs()?;
            self.complete_native_range_command(&pending, certificate, &receipt, authority)
        })();
        if let Err(error) = result {
            return Err(BicDbError::CommitOutcomeUnknown {
                sequence: transaction.committed_seq,
                transaction_id: Some(transaction.id().0),
                incarnation: self.incarnation.clone(),
                detail: format!("certified native command {} is irrevocable; local prefix completion requires recovery: {error}", intent.command_id()),
            });
        }
        self.auto_checkpoint_if_due();
        Ok(())
    }

    pub(super) fn read_native_certified_range_prefix(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeGenesisReceipt,
    ) -> Result<Option<NativeCertifiedRangePrefix>> {
        let prefix: Option<NativeCertifiedRangePrefix> =
            self.read_native_range_image_record(&prefix_key(base.range.id))?;
        if let Some(prefix) = &prefix {
            prefix.validate(birth, base)?;
        }
        Ok(prefix)
    }
    pub(super) fn native_range_pending_exists(&self, range: RangeId) -> Result<bool> {
        Ok(self
            .read_native_range_image_record::<PendingNativeRangeCommand>(&pending_key(range))?
            .is_some())
    }
    pub(super) fn observe_native_pending(
        &self,
        observation: &mut NativeRangePrefixObservation,
    ) -> Result<()> {
        let pending: Option<PendingNativeRangeCommand> =
            self.read_native_range_image_record(&pending_key(observation.range_id))?;
        if let Some(pending) = pending {
            pending.validate()?;
            observation.pending_command_index = Some(pending.range_index);
            observation.pending_command_id = Some(pending.command_id);
            observation.materialized_pending_index =
                pending.materialized.map(|_| pending.range_index);
            observation.diagnostic =
                Some("unfinished native command; source image unavailable".into());
        }
        Ok(())
    }
    pub(crate) fn current_native_range_source_lineage(
        &self,
        range_id: RangeId,
    ) -> Result<SourceRangeImageLineage> {
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("no native birth"))?;
        let receipt: NativeGenesisReceipt = self
            .read_native_range_image_record(&genesis_key(range_id))?
            .ok_or_else(|| invalid("no trusted native base; image repair required"))?;
        receipt.validate()?;
        let prefix = self.read_native_certified_range_prefix(&birth, &receipt)?;
        let lineage = SourceRangeImageLineage {
            birth,
            receipt,
            prefix,
            current_database_incarnation: self.incarnation.clone(),
            current_schema_generation: self.schema_compatibility.generation(),
        };
        self.validate_source_range_image_lineage(&lineage, range_id, lineage.range_index())?;
        Ok(lineage)
    }
    /// The coordinator must persist this before the actual native row apply.
    /// A caller-supplied index, transaction ID or mutation permit cannot mint
    /// the command/configuration intent accepted by this adapter.
    pub(crate) fn prepare_native_range_command(
        &self,
        intent: &NativeRangeCommandIntent,
        local_transaction_id: u64,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<NativeRangePendingApply> {
        self.range_mutations
            .validate(intent.range().id, authority)?;
        let _journal = self
            .native_range_image_journal
            .try_lock()
            .ok_or_else(|| invalid("native journal busy; retry before decision"))?;
        let lineage = self.current_native_range_source_lineage(intent.range().id)?;
        intent.validate_native_base(&lineage)?;
        let (global_image_generation, range_image_generation) =
            self.native_range_image_generations(&lineage.birth, intent.range().id)?;
        let mut pending = PendingNativeRangeCommand {
            format_version: 1,
            birth_id: lineage.birth.birth_id,
            range_id: intent.range().id,
            range_index: intent.range_index(),
            previous_index: lineage.range_index(),
            previous_receipt_sha256: lineage.native_receipt_sha256().to_string(),
            command_id: intent.command_id().to_string(),
            command_checksum_sha256: intent.command_checksum_sha256().to_string(),
            mutations_sha256: intent.mutations_sha256().to_string(),
            scope_sha256: intent.scope_sha256().to_string(),
            expected_local_transaction_id: local_transaction_id,
            global_image_generation,
            range_image_generation,
            materialized: None,
            checksum_sha256: String::new(),
        };
        pending.checksum_sha256 = pending.checksum()?;
        self.write_native_range_image_record(&pending_key(pending.range_id), &pending)?;
        Ok(NativeRangePendingApply {
            database_incarnation: self.incarnation.clone(),
            intent: intent.clone(),
            pending,
        })
    }
    fn require_native_pending(
        &self,
        pending: &NativeRangePendingApply,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<PendingNativeRangeCommand> {
        self.range_mutations
            .validate(pending.pending.range_id, authority)?;
        self.validate_native_wal_apply_receipt(receipt)?;
        if pending.database_incarnation != self.incarnation
            || receipt.transaction_id() != pending.pending.expected_local_transaction_id
            || receipt.mutations_sha256() != pending.intent.mutations_sha256()
            || self.native_range_image_installing(pending.pending.range_id)?
        {
            return Err(invalid(
                "pending native apply differs from actual DB/transaction/mutations",
            ));
        }
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("native birth disappeared"))?;
        let current: PendingNativeRangeCommand = self
            .read_native_range_image_record(&pending_key(pending.pending.range_id))?
            .ok_or_else(|| invalid("pending native command disappeared"))?;
        current.validate()?;
        let mut unsigned_current = current.clone();
        unsigned_current.materialized = None;
        unsigned_current.checksum_sha256 = unsigned_current.checksum()?;
        if unsigned_current != pending.pending
            || current.birth_id != birth.birth_id
            || self.native_range_image_generations(&birth, current.range_id)?
                != (
                    current.global_image_generation,
                    current.range_image_generation,
                )
        {
            return Err(invalid(
                "native pending identity/base generation changed; repair required",
            ));
        }
        Ok(current)
    }
    /// Materialized pending is diagnostic state, not a certified floor. The
    /// actual private WAL receipt is validated under this current DB/open.
    pub(crate) fn record_native_range_materialized(
        &self,
        pending: &NativeRangePendingApply,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let _journal = self
            .native_range_image_journal
            .try_lock()
            .ok_or_else(|| invalid("native journal busy after apply"))?;
        let mut record = self.require_native_pending(pending, receipt, authority)?;
        let materialized = MaterializedNativeCommand {
            local_transaction_id: receipt.transaction_id(),
            local_wal_sequence: receipt.sequence(),
            actual_mutations_sha256: receipt.mutations_sha256().to_string(),
            database_incarnation: receipt.database_incarnation().to_string(),
        };
        if record
            .materialized
            .as_ref()
            .is_some_and(|previous| previous != &materialized)
        {
            return Err(invalid("pending native application receipt changed"));
        }
        record.materialized = Some(materialized);
        record.checksum_sha256 = record.checksum()?;
        self.write_native_range_image_record(&pending_key(record.range_id), &record)
    }
    /// Atomic prefix + pending deletion inside one real native page-WAL txn,
    /// AFTER separate durable data/index/WAL apply and actual certificate QQ.
    /// This does not pretend that those earlier transactions were one txn.
    pub(crate) fn complete_native_range_command(
        &self,
        pending: &NativeRangePendingApply,
        certificate: &NativeRangeCommitCertificate,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let _journal = self
            .native_range_image_journal
            .try_lock()
            .ok_or_else(|| invalid("native journal busy after certification"))?;
        let record = self.require_native_pending(pending, receipt, authority)?;
        if certificate.intent().scope_sha256() != pending.intent.scope_sha256()
            || certificate.intent().command_id() != pending.intent.command_id()
            || certificate.intent().command_checksum_sha256()
                != pending.intent.command_checksum_sha256()
            || certificate.intent().mutations_sha256() != pending.intent.mutations_sha256()
        {
            return Err(invalid(
                "native completion certificate does not match pending command",
            ));
        }
        let materialized = record
            .materialized
            .clone()
            .ok_or_else(|| invalid("native completion has no actual materialization receipt"))?;
        if materialized.local_transaction_id != receipt.transaction_id()
            || materialized.local_wal_sequence != receipt.sequence()
        {
            return Err(invalid(
                "native completion changed its actual WAL application",
            ));
        }
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("no native birth"))?;
        let base: NativeGenesisReceipt = self
            .read_native_range_image_record(&genesis_key(record.range_id))?
            .ok_or_else(|| invalid("native completion base disappeared"))?;
        let old = self.read_native_certified_range_prefix(&birth, &base)?;
        let previous_index = old
            .as_ref()
            .map_or(base.range_index, |previous| previous.range_index);
        let previous_sha = old
            .as_ref()
            .map_or(base.checksum_sha256.as_str(), |previous| {
                previous.checksum_sha256.as_str()
            });
        if previous_index != record.previous_index || previous_sha != record.previous_receipt_sha256
        {
            return Err(invalid(
                "native completion would skip or overwrite another certified prefix",
            ));
        }
        let mut prefix = NativeCertifiedRangePrefix {
            format_version: 1,
            birth_id: record.birth_id,
            base_receipt_sha256: base.checksum_sha256.clone(),
            range_id: record.range_id,
            range_index: record.range_index,
            previous_index: record.previous_index,
            previous_receipt_sha256: record.previous_receipt_sha256,
            command_id: record.command_id,
            command_checksum_sha256: record.command_checksum_sha256,
            mutations_sha256: record.mutations_sha256,
            scope_sha256: record.scope_sha256,
            materialized,
            certified_voters: certificate.certified_voters().clone(),
            required_quorum: certificate.required_quorum(),
            global_image_generation: record.global_image_generation,
            range_image_generation: record.range_image_generation,
            checksum_sha256: String::new(),
        };
        prefix.checksum_sha256 = prefix.checksum()?;
        prefix.validate(&birth, &base)?;
        let encoded = bounded_json(&prefix, MAX_NATIVE_RECEIPT_BYTES)?;
        self.in_paged_transaction(|paged, xid| {
            paged
                .store()
                .put(xid, &native_key(&prefix_key(prefix.range_id)), &encoded)
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            paged
                .store()
                .delete(xid, &native_key(&pending_key(prefix.range_id)))
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native completion has no page store"))?;
        Ok(())
    }
}
