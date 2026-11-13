//! Retained original row transaction and exact imported-image WAL grant.
use super::*;
use crate::db::registered_row_retention::{self, RegisteredRowIntent as RowIntent};
use crate::distribution_range_image::{RangeImageCursorV1, RangeImageStepV1};
use crate::distribution_range_mutation::{RangeMutationAuthority, RangeMutationClaim};
use writer_enrollment::RegisteredRemoteMutationScopeLiveness;

const MAX_ROW_STEP: usize = 9 * 1024 * 1024;
fn row_key(attempt: Uuid) -> String {
    format!("registered-remote-image/{attempt}/rows")
}
fn intent_key(attempt: Uuid, step: u64) -> String {
    format!("registered-remote-image/{attempt}/row-intent/{step:020}")
}
fn frame_hash(hash: &mut sha2::Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RowHeader {
    version: u32,
    attempt: Uuid,
    history_generation: Uuid,
    manifest_sha256: String,
    intent_sha256: String,
    cursor: RangeImageCursorV1,
    chain: String,
    records: u64,
    bytes: u64,
    complete: bool,
    last_sha256: Option<String>,
    last_previous: Option<RangeImageCursorV1>,
    source_min_b: Option<u64>,
    source_max_b: Option<u64>,
    last_actual_target_b: Option<u64>,
    native_xid: u64,
    checksum_sha256: String,
}
impl RowHeader {
    fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("registered-remote-rows-v1", value),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.attempt.is_nil()
            || self.history_generation.is_nil()
            || self.native_xid == 0
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged Registered remote row progress"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum LogicalOutcome {
    NotAttempted,
    AttemptedUnknown,
    WalDurablePendingApply(u64),
    AppliedKnown(u64),
    NoRows,
}
struct RowPending {
    // One shared actual lease outlives Transaction consumption/finalization.
    // Release only when committed protected progress and postchecks permit
    // removing this original pending step; errors retain it with the debt.
    _wal_retention: super::super::super::native_wal_apply::NativeWalRetentionPin,
    intent: RowIntent,
    step: RangeImageStepV1,
    transaction: Option<Transaction>,
    logical: Arc<parking_lot::Mutex<LogicalOutcome>>,
    content: sha2::Sha256,
    next: RowHeader,
    intent_commit_returned: Option<u64>,
    progress_attempt: Option<(RowHeader, Option<u64>)>,
}
pub(super) struct RetainedRemoteRows {
    header: RowHeader,
    content: sha2::Sha256,
    pending: Option<RowPending>,
}
/// Actual complete protected row progress, never a final-image/read grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RegisteredCompletedRows {
    pub(super) native_xid: u64,
    pub(super) header_sha256: String,
    pub(super) records: u64,
    pub(super) bytes: u64,
    pub(super) content_sha256: String,
    pub(super) last_actual_target_b: Option<u64>,
}
/// Immutable publication DATA copied from the actual committed row header.
/// Deserialization alone creates no complete-row capability or writer grant.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArchivedRegisteredRows {
    header: RowHeader,
}
impl ArchivedRegisteredRows {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn require_historical(
        &self,
        db: &BicDb,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
        seal: &crate::distribution_range_image::LocalRangeImageSealV1,
        intent: &str,
        generation: Uuid,
        xid: u64,
        sha: &str,
        last_b: Option<u64>,
    ) -> Result<()> {
        let h = &self.header;
        h.validate()?;
        if !h.complete
            || h.attempt != manifest.target_attempt
            || h.history_generation != generation
            || h.manifest_sha256 != manifest.checksum_sha256
            || h.intent_sha256 != intent
            || h.native_xid != xid
            || h.checksum_sha256 != sha
            || h.last_actual_target_b != last_b
            || h.cursor.collection_index() != manifest.collections.len()
            || h.cursor.after_key().is_some()
            || h.cursor.step() != seal.steps
            || h.chain != seal.wire_chain_sha256
            || h.records != seal.record_count
            || h.bytes != seal.serialized_record_bytes
            || h.source_min_b.unwrap_or(manifest.source_reserved_b) != seal.min_snapshot_db_sequence
            || h.source_max_b.unwrap_or(manifest.source_reserved_b) != seal.max_snapshot_db_sequence
            || (h.records > 0 && last_b.is_none())
            || db
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Registered archived rows page store absent"))?
                .store()
                .transactions()
                .status(xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "Registered immutable published row header differs from final binding",
            ));
        }
        Ok(())
    }
}
impl NativeRegisteredRemoteImageInstallation {
    pub(super) fn completed_rows(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        seal: &crate::distribution_range_image::LocalRangeImageSealV1,
    ) -> Result<RegisteredCompletedRows> {
        let history = self.finish_history(db, context)?;
        history.require_current(db, context)?;
        let rows = self
            .rows
            .as_ref()
            .ok_or_else(|| invalid("Registered final image needs actual complete rows"))?;
        rows.header.validate()?;
        let manifest = context.point.preflight().source().manifest();
        let content_sha256 = hex::encode(rows.content.clone().finalize());
        let actual: RowHeader = db
            .read_native_range_image_record(&row_key(rows.header.attempt))?
            .ok_or_else(|| invalid("Registered completed row header absent"))?;
        let committed = db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Registered completed rows page store absent"))?
            .store()
            .transactions()
            .status(rows.header.native_xid)
            == bicdb_page::TxStatus::Committed;
        if rows.pending.is_some()
            || !rows.header.complete
            || actual != rows.header
            || !committed
            || rows.header.history_generation != history.header.generation
            || history.header.wire_chain != seal.history_wire_chain_sha256
            || history.header.cursor.step() != seal.history_steps
            || rows.header.manifest_sha256 != manifest.checksum_sha256
            || rows.header.intent_sha256 != context.fence.intent_sha256()?
            || rows.header.cursor.collection_index() != manifest.collections.len()
            || rows.header.cursor.after_key().is_some()
            || rows.header.records != seal.record_count
            || rows.header.bytes != seal.serialized_record_bytes
            || content_sha256 != seal.image_root_sha256
            || rows.header.chain != seal.wire_chain_sha256
            || rows.header.cursor.step() != seal.steps
            || rows
                .header
                .source_min_b
                .unwrap_or(manifest.source_reserved_b)
                != seal.min_snapshot_db_sequence
            || rows
                .header
                .source_max_b
                .unwrap_or(manifest.source_reserved_b)
                != seal.max_snapshot_db_sequence
            || (rows.header.records > 0 && rows.header.last_actual_target_b.is_none())
        {
            return Err(invalid(
                "Registered final image needs exact complete committed rows and source seal",
            ));
        }
        context.require_current(db)?;
        Ok(RegisteredCompletedRows {
            native_xid: rows.header.native_xid,
            header_sha256: rows.header.checksum_sha256.clone(),
            records: rows.header.records,
            bytes: rows.header.bytes,
            content_sha256,
            last_actual_target_b: rows.header.last_actual_target_b,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeRegisteredRemoteRowProgress {
    pub cursor: RangeImageCursorV1,
    pub complete: bool,
    pub records: u64,
    pub bytes: u64,
    pub target_actual_visibility_b: Option<u64>,
    pub content_sha256: String,
}
/// No public constructor/serde. The weak lexical lease cannot keep borrowed
/// hook/admission/journal guards alive. Every actual WAL boundary checks it.
pub(crate) struct NativeRegisteredRemoteRowTransactionGrant {
    request: NativeRegisteredRemoteTransactionRequest,
    lexical: RegisteredRemoteMutationScopeLiveness,
    fence: Arc<NativeRegisteredRemoteInstallingFence>,
    proposal: Arc<NativeRegisteredRemoteProposalReceipt>,
    history: HistoryHeader,
    intent: RowIntent,
    range: RangeDescriptor,
    claim: RangeMutationClaim,
    database_open: String,
    transaction_id: u64,
    mutations_sha256: String,
    logical: Arc<parking_lot::Mutex<LogicalOutcome>>,
}
impl std::fmt::Debug for NativeRegisteredRemoteRowTransactionGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRegisteredRemoteRowTransactionGrant")
            .field("range", &self.range.id)
            .field("attempt", &self.intent.attempt)
            .field("transaction_id", &self.transaction_id)
            .finish_non_exhaustive()
    }
}
impl NativeRegisteredRemoteRowTransactionGrant {
    /// Actual inner logical-WAL success, before crash/apply/materialization.
    /// Outcome custody only: this cannot admit row progress or Serving.
    pub(in crate::db) fn retain_wal_durable(&self, sequence: u64) {
        *self.logical.lock() = LogicalOutcome::WalDurablePendingApply(sequence);
    }
    /// Actual inner applied/visible state, before outbox finalizers. It does
    /// not clear a durability fence or replace fresh scope validation.
    pub(in crate::db) fn retain_applied_known(&self, sequence: u64) {
        *self.logical.lock() = LogicalOutcome::AppliedKnown(sequence);
        #[cfg(test)]
        cuts::fire(true);
    }
    pub(in crate::db) fn require_current(&self, db: &BicDb, tx: &Transaction) -> Result<()> {
        self.lexical
            .require_current(db, self.intent.attempt, self.range.id)?;
        if self.database_open != db.incarnation_id()
            || self.transaction_id != tx.id().0
            || self.intent.logical_txid != Some(tx.id().0)
            || super::super::super::native_wal_apply::actual_transaction_mutations_sha256(tx)?
                != self.mutations_sha256
            || self.intent.mutations_sha256.as_deref() != Some(self.mutations_sha256.as_str())
            || !tx.bypass_commit_admission
            || !tx.imported_projection
            || tx.enqueue_memory_jobs_on_commit
            || tx.isolation != TransactionIsolation::ReadCommitted
        {
            return Err(invalid(
                "Registered row transaction changed exact actual Tx/mutations/scope",
            ));
        }
        db.range_mutations.validate_claim(&self.claim)?;
        self.request.require_current(db)?;
        self.proposal.require_guarded_transaction_current(
            db,
            self.request.preflight().native(),
            &self.lexical,
        )?;
        self.fence
            .require_database_binding(db, self.request.preflight(), &self.proposal)?;
        if db.read_registered_remote_history(self.intent.attempt)? != self.history
            || !self.history.audited
            || db.read_registered_remote_row_intent(&self.intent)? != self.intent
        {
            return Err(invalid(
                "Registered row transaction changed protected intent or audited history",
            ));
        }
        for write in &tx.writes {
            if !self
                .range
                .contains_token(crate::distribution::distribution_key_token(
                    &write.collection,
                    &write.record_id,
                ))
            {
                return Err(invalid(
                    "Registered row transaction left exact selected range",
                ));
            }
        }
        self.request.require_current(db)?;
        self.lexical
            .require_current(db, self.intent.attempt, self.range.id)
    }
}
impl NativeRegisteredRemoteImageInstallation {
    pub(crate) fn apply_rows(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteRowProgress> {
        let proof = self.finish_history(db, context)?;
        proof.require_current(db, context)?;
        let NativeRemoteImageSourcePayloadV1::Rows(step) = context.point.frame().payload()? else {
            return Err(invalid(
                "Registered row apply needs actual authenticated Rows frame",
            ));
        };
        if self.rows.is_none() {
            if db
                .read_native_range_image_record::<RowHeader>(&row_key(self.header.attempt))?
                .is_some()
            {
                return Err(invalid(
                    "Registered row progress has no original retained owner",
                ));
            }
            let manifest = context.point.preflight().source().manifest();
            let catalog = crate::distribution_range_image::canonical_range_image_catalog(
                &manifest.cluster_id,
                &manifest.capture_range,
                manifest.range_index,
                context.point.preflight().bindings(),
            )?;
            let mut content = sha2::Sha256::new();
            content.update(b"bicdb.range-image.content.v1\0");
            frame_hash(&mut content, &catalog);
            self.rows = Some(RetainedRemoteRows {
                header: RowHeader {
                    version: 1,
                    attempt: self.header.attempt,
                    history_generation: self.header.generation,
                    manifest_sha256: self.header.manifest_sha256.clone(),
                    intent_sha256: self.header.intent_sha256.clone(),
                    cursor: RangeImageCursorV1::default(),
                    chain: hex::encode(sha2::Sha256::digest(b"bicdb.range-image.wire.v1\0")),
                    records: 0,
                    bytes: 0,
                    complete: false,
                    last_sha256: None,
                    last_previous: None,
                    source_min_b: None,
                    source_max_b: None,
                    last_actual_target_b: None,
                    native_xid: 0,
                    checksum_sha256: String::new(),
                },
                content,
                pending: None,
            });
        }
        let rows = self.rows.as_mut().unwrap();
        rows.apply(db, context, &self.header, step)?;
        Ok(NativeRegisteredRemoteRowProgress {
            cursor: rows.header.cursor.clone(),
            complete: rows.header.complete,
            records: rows.header.records,
            bytes: rows.header.bytes,
            target_actual_visibility_b: rows.header.last_actual_target_b,
            content_sha256: hex::encode(rows.content.clone().finalize()),
        })
    }
}
impl RetainedRemoteRows {
    #[cfg(test)]
    pub(super) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"progress":self.header,"pending":self.pending.as_ref().map(|pending|{
            let logical=*pending.logical.lock();
            serde_json::json!({
            "intent":pending.intent,"actual_transaction_retained":pending.transaction.is_some(),"intent_commit_returned":pending.intent_commit_returned,
            "logical_outcome":match logical { LogicalOutcome::NotAttempted=>"not_attempted",LogicalOutcome::AttemptedUnknown=>"unknown",LogicalOutcome::WalDurablePendingApply(_)=>"wal_durable_pending_apply",LogicalOutcome::AppliedKnown(_)=>"applied_known",LogicalOutcome::NoRows=>"no_rows_in_actual_step"},
            "logical_sequence":match logical { LogicalOutcome::WalDurablePendingApply(sequence)|LogicalOutcome::AppliedKnown(sequence)=>Some(sequence),_=>None },"progress_attempt":pending.progress_attempt})})})
    }
    fn apply(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        history: &HistoryHeader,
        step: &RangeImageStepV1,
    ) -> Result<()> {
        context.require_current(db)?;
        if self.pending.is_none() {
            if self.header.native_xid != 0 {
                self.header.validate()?;
                if db
                    .read_native_range_image_record::<RowHeader>(&row_key(self.header.attempt))?
                    .as_ref()
                    != Some(&self.header)
                {
                    return Err(invalid("Registered row owner/protected progress differ"));
                }
            }
            if self.header.last_previous.as_ref() == Some(&step.previous_cursor)
                && self.header.last_sha256.as_deref() == Some(step.chain_sha256.as_str())
            {
                return Ok(());
            }
            validate_step(step, &self.header, context)?;
            let mut next = self.header.clone();
            let mut content = self.content.clone();
            let mut transaction = None;
            if let Some(batch) = &step.batch {
                let meta = &context.point.preflight().bindings().collections()
                    [self.header.cursor.collection_index()];
                BicDb::validate_range_snapshot_fields(context.fence.range(), meta, batch)?;
                if db
                    .collection_state(&batch.collection)?
                    .meta
                    .portable_contract()
                    != meta.portable_contract()
                {
                    return Err(invalid("Registered row preinstalled collection differs"));
                }
                for record in &batch.records {
                    let bytes = bounded_json(record, 8 * 1024 * 1024)?;
                    frame_hash(&mut content, meta.name.as_bytes());
                    frame_hash(&mut content, &bytes);
                    next.records = next
                        .records
                        .checked_add(1)
                        .ok_or_else(|| invalid("Registered row count overflow"))?;
                    next.bytes = next
                        .bytes
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| invalid("Registered row bytes overflow"))?;
                }
                let mut tx = db.begin_transaction()?;
                tx.bypass_commit_admission = true;
                tx.imported_projection = true;
                // Physical projection must not enqueue fresh model/embedding
                // or application memory jobs while copying source records.
                tx.enqueue_memory_jobs_on_commit = false;
                tx.write_upserts_unchecked(&batch.collection, batch.records.clone())?;
                transaction = Some(tx);
            }
            // Acquire before the first native intent WAL. The SAME actual
            // transaction shares this lease; commit finalization may clear its
            // clone, but cannot release the pending owner's retention.
            let wal_retention = if let Some(tx) = transaction.as_mut() {
                if tx.native_wal_retention.is_none() {
                    tx.native_wal_retention = Some(db.tx_log.pin_native_wal_proof()?);
                }
                tx.native_wal_retention.as_ref().unwrap().clone()
            } else {
                db.tx_log.pin_native_wal_proof()?
            };
            let intent = RowIntent {
                version: 1,
                attempt: self.header.attempt,
                range: context.fence.range().id,
                manifest_sha256: self.header.manifest_sha256.clone(),
                history_generation: history.generation,
                history_header_sha256: history.checksum_sha256.clone(),
                intent_sha256: self.header.intent_sha256.clone(),
                step: step.next_cursor.step(),
                step_sha256: step.chain_sha256.clone(),
                previous_progress_sha256: (self.header.native_xid != 0)
                    .then(|| self.header.checksum_sha256.clone()),
                logical_txid: transaction.as_ref().map(|tx| tx.id().0),
                mutations_sha256: transaction
                    .as_ref()
                    .map(super::super::super::native_wal_apply::actual_transaction_mutations_sha256)
                    .transpose()?,
                native_xid: 0,
                checksum_sha256: String::new(),
            };
            next.cursor = step.next_cursor.clone();
            next.chain = step.chain_sha256.clone();
            next.complete = step.complete;
            next.last_sha256 = Some(step.chain_sha256.clone());
            next.last_previous = Some(step.previous_cursor.clone());
            next.source_min_b = Some(next.source_min_b.map_or(step.snapshot_db_sequence, |b| {
                b.min(step.snapshot_db_sequence)
            }));
            next.source_max_b = Some(next.source_max_b.map_or(step.snapshot_db_sequence, |b| {
                b.max(step.snapshot_db_sequence)
            }));
            let logical = if transaction.is_some() {
                LogicalOutcome::NotAttempted
            } else {
                LogicalOutcome::NoRows
            };
            // Original host owns exact pending Tx/payload/progress BEFORE any
            // intent page WAL or row WAL. No canceled call recreates it.
            self.pending = Some(RowPending {
                _wal_retention: wal_retention,
                intent,
                step: step.clone(),
                transaction,
                logical: Arc::new(parking_lot::Mutex::new(logical)),
                content,
                next,
                intent_commit_returned: None,
                progress_attempt: None,
            });
        }
        let pending = self.pending.as_mut().unwrap();
        if &pending.step != step {
            return Err(invalid(
                "Registered pending row operation requires exact original source step",
            ));
        }
        crate::format::enable_native_registered_remote_image(db.data_path())?;
        let controlled = db
            .range_mutation_gate()
            .try_controlled_mutation(context.scope.frozen())?;
        let authority = RangeMutationAuthority::Controlled(&controlled);
        if pending.intent.native_xid == 0 {
            let result = db.in_paged_transaction(|paged, xid| {
                context.require_current(db)?;
                pending.intent.native_xid = xid;
                pending.intent.checksum_sha256 = pending.intent.checksum()?;
                pending.intent.validate()?;
                let bytes = bounded_json(&pending.intent, MAX_NATIVE_RECEIPT_BYTES)?;
                registered_row_retention::insert(
                    paged,
                    xid,
                    db.incarnation_id(),
                    context.point.preflight().native().birth_sha256(),
                    &pending.intent,
                )?;
                paged
                    .store()
                    .put(
                        xid,
                        &native_key(&intent_key(pending.intent.attempt, pending.intent.step)),
                        &bytes,
                    )
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                context.require_current(db)
            });
            if let Ok(Some(xid)) = &result {
                pending.intent_commit_returned = Some(*xid);
            }
            result?.ok_or_else(|| invalid("Registered row intent has no page store"))?;
        }
        if db.read_registered_remote_row_intent(&pending.intent)? != pending.intent {
            return Err(invalid(
                "Registered row native intent exact outcome unproven",
            ));
        }
        // The intent and its restrictive index must have the SAME committed
        // page XID before any actual logical-WAL attempt can proceed.
        if pending.progress_attempt.is_none() {
            registered_row_retention::require_exact(
                db.paged_records
                    .as_deref()
                    .ok_or_else(|| invalid("Registered index page store absent"))?,
                db.incarnation_id(),
                &pending.intent,
            )?;
        }
        let logical = *pending.logical.lock();
        if matches!(logical, LogicalOutcome::NotAttempted) {
            context.require_current(db)?;
            db.invalidate_native_range_images_before_mutation(
                Some(context.fence.range().id),
                "Registered remote image row apply",
            )?;
            let tx = pending
                .transaction
                .as_mut()
                .ok_or_else(|| invalid("Registered pending actual row transaction absent"))?;
            let actual_hash =
                super::super::super::native_wal_apply::actual_transaction_mutations_sha256(tx)?;
            if pending.intent.logical_txid != Some(tx.id().0)
                || pending.intent.mutations_sha256.as_deref() != Some(actual_hash.as_str())
            {
                return Err(invalid("Registered actual retained row Tx changed"));
            }
            let claim = authority.claim();
            tx.range_mutation_claim = Some(authority.claim());
            tx.native_registered_remote_row_grant =
                Some(NativeRegisteredRemoteRowTransactionGrant {
                    request: context.point.retain_transaction_request()?,
                    lexical: context.scope.retained_scope_liveness(),
                    fence: context.fence.clone(),
                    proposal: context.proposal.clone(),
                    history: history.clone(),
                    intent: pending.intent.clone(),
                    range: context.fence.range().clone(),
                    claim,
                    database_open: db.incarnation_id().into(),
                    transaction_id: tx.id().0,
                    mutations_sha256: actual_hash,
                    logical: pending.logical.clone(),
                });
            #[cfg(test)]
            cuts::fire(false);
            context.require_current(db)?;
            *pending.logical.lock() = LogicalOutcome::AttemptedUnknown;
            let tx = pending
                .transaction
                .take()
                .expect("actual pending transaction checked before WAL");
            let result = tx.commit_registered_remote_image_with_sequence();
            result?;
        }
        let logical = *pending.logical.lock();
        match logical {
            LogicalOutcome::AttemptedUnknown=>return Err(invalid("Registered row logical WAL outcome unknown; original Tx/payload retained, no new transaction permitted")),
            LogicalOutcome::WalDurablePendingApply(_)=>return Err(invalid("Registered row logical WAL is durable but apply/materialization is incomplete; original Tx retained without row progress")),
            LogicalOutcome::AppliedKnown(sequence)=>pending.next.last_actual_target_b=Some(sequence),
            LogicalOutcome::NoRows=>{},LogicalOutcome::NotAttempted=>return Err(invalid("Registered row operation not attempted")),
        }
        context.require_current(db)?;
        if pending.progress_attempt.is_none() {
            let result = db.in_paged_transaction(|paged, xid| {
                context.require_current(db)?;
                let mut next = pending.next.clone();
                next.native_xid = xid;
                next.checksum_sha256 = next.checksum()?;
                next.validate()?;
                pending.progress_attempt = Some((next.clone(), None));
                registered_row_retention::require_exact(
                    paged,
                    db.incarnation_id(),
                    &pending.intent,
                )?;
                registered_row_retention::clear(paged, xid, &pending.intent)?;
                paged
                    .store()
                    .put(
                        xid,
                        &native_key(&row_key(next.attempt)),
                        &bounded_json(&next, MAX_NATIVE_RECEIPT_BYTES)?,
                    )
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                context.require_current(db)
            });
            if let Ok(Some(xid)) = &result {
                pending.progress_attempt.as_mut().unwrap().1 = Some(*xid);
            }
            result?.ok_or_else(|| invalid("Registered row progress has no page store"))?;
        }
        let (attempted, _) = pending.progress_attempt.as_ref().unwrap();
        let actual: RowHeader = db
            .read_native_range_image_record(&row_key(attempted.attempt))?
            .ok_or_else(|| invalid("Registered row progress attempted result absent"))?;
        actual.validate()?;
        if &actual != attempted
            || db
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Registered row page store absent"))?
                .store()
                .transactions()
                .status(actual.native_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid("Registered row progress outcome not proven"));
        }
        registered_row_retention::require_cleared(
            db.paged_records
                .as_deref()
                .ok_or_else(|| invalid("Registered index page store absent"))?,
            &pending.intent,
        )?;
        context.require_current(db)?;
        self.header = actual;
        self.content = pending.content.clone();
        // Exact committed header readback and fresh context checks above are
        // the release boundary. Unknown/pending-apply/error paths never drop
        // this step merely because the consuming logical commit returned.
        self.pending = None;
        drop(controlled);
        Ok(())
    }
}

#[cfg(test)]
mod cuts {
    use std::cell::RefCell;
    type Cut = Box<dyn FnOnce()>;
    thread_local! {static CUT:RefCell<Option<(bool,Cut)>>=const{RefCell::new(None)};}
    pub(crate) struct Guard(Option<(bool, Cut)>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }
    pub(crate) fn install(after: bool, cut: Cut) -> Guard {
        Guard(CUT.with(|slot| slot.replace(Some((after, cut)))))
    }
    pub(super) fn fire(after: bool) {
        let callback = CUT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.as_ref().is_some_and(|(point, _)| *point == after) {
                slot.take().map(|(_, cut)| cut)
            } else {
                None
            }
        });
        if let Some(callback) = callback {
            callback();
        }
    }
}
#[cfg(test)]
pub(crate) use cuts::install as test_registered_remote_row_wal_cut;
impl BicDb {
    pub(super) fn archive_registered_completed_rows(
        &self,
        attempt: Uuid,
        xid: u64,
        sha: &str,
    ) -> Result<ArchivedRegisteredRows> {
        self.require_registered_completed_rows_point(attempt, xid, sha)?;
        let header = self
            .read_native_range_image_record(&row_key(attempt))?
            .ok_or_else(|| {
                invalid("Registered actual completed rows disappeared before archive")
            })?;
        Ok(ArchivedRegisteredRows { header })
    }
    pub(super) fn require_registered_completed_rows_point(
        &self,
        attempt: Uuid,
        xid: u64,
        hash: &str,
    ) -> Result<()> {
        let header: RowHeader = self
            .read_native_range_image_record(&row_key(attempt))?
            .ok_or_else(|| invalid("Registered final row progress absent"))?;
        header.validate()?;
        if !header.complete
            || header.attempt != attempt
            || header.native_xid != xid
            || header.checksum_sha256 != hash
            || self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Registered final rows page store absent"))?
                .store()
                .transactions()
                .status(xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "Registered final row progress changed or uncommitted",
            ));
        }
        Ok(())
    }
    fn read_registered_remote_row_intent(&self, expected: &RowIntent) -> Result<RowIntent> {
        let actual: RowIntent = self
            .read_native_range_image_record(&intent_key(expected.attempt, expected.step))?
            .ok_or_else(|| invalid("Registered exact row intent absent"))?;
        actual.validate()?;
        if &actual != expected
            || self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Registered row intent page store absent"))?
                .store()
                .transactions()
                .status(actual.native_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid("Registered exact row intent not proven committed"));
        }
        Ok(actual)
    }
}
fn validate_step(
    step: &RangeImageStepV1,
    current: &RowHeader,
    context: &NativeRegisteredRemoteImageContext<'_>,
) -> Result<()> {
    bounded_json(step, MAX_ROW_STEP)?;
    let next = &step.next_cursor;
    let cursor = &current.cursor;
    let manifest = context.point.preflight().source().manifest();
    let collections = &manifest.collections;
    if current.complete
        || step.format_version != 1
        || step.session_id != manifest.handoff_session
        || step.nonce != manifest.nonce
        || step.previous_cursor != *cursor
        || step.previous_chain_sha256 != current.chain
        || next.step()
            != cursor
                .step()
                .checked_add(1)
                .ok_or_else(|| invalid("Registered row cursor overflow"))?
        || cursor.collection_index() >= collections.len()
        || step.snapshot_db_sequence < manifest.source_reserved_b
        || next.collection_index() > collections.len()
        || next.collection_index() < cursor.collection_index()
        || next.collection_index() > cursor.collection_index() + 1
        || step.complete != (next.collection_index() == collections.len())
    {
        return Err(invalid("Registered row source/cursor/cut mismatch"));
    }
    if next.collection_index() == cursor.collection_index() {
        if next.after_key() <= cursor.after_key() {
            return Err(invalid("Registered row cursor did not advance"));
        }
    } else if next.after_key().is_some() {
        return Err(invalid("Registered row next collection did not reset key"));
    }
    let mut unsigned = step.clone();
    unsigned.chain_sha256.clear();
    if hex::encode(sha2::Sha256::digest(bounded_json(&unsigned, MAX_ROW_STEP)?))
        != step.chain_sha256
    {
        return Err(invalid("Registered row step checksum differs"));
    }
    if let Some(batch) = &step.batch {
        let name = &collections[cursor.collection_index()].name;
        if batch.collection != *name
            || batch.range_id != manifest.capture_range.id
            || batch.range_epoch != manifest.capture_range.epoch
            || batch.snapshot_commit_sequence != step.snapshot_db_sequence
            || batch.records.is_empty()
            || batch.records.len() > 16384
            || batch.serialized_record_bytes > 8 * 1024 * 1024
            || batch.resume_after_key.as_str() <= cursor.after_key().unwrap_or("")
        {
            return Err(invalid("Registered row batch bounds/scope differ"));
        }
        let mut previous = cursor.after_key();
        let mut bytes = 0usize;
        for record in &batch.records {
            if previous.is_some_and(|last| record.id.as_str() <= last)
                || record.id > batch.resume_after_key
                || !manifest.capture_range.contains_token(
                    crate::distribution::distribution_key_token(name, &record.id),
                )
            {
                return Err(invalid(
                    "Registered row records unordered/outside selected range",
                ));
            }
            bytes = bytes
                .checked_add(bounded_json(record, 8 * 1024 * 1024)?.len())
                .ok_or_else(|| invalid("Registered row batch bytes overflow"))?;
            previous = Some(record.id.as_str());
        }
        if bytes != batch.serialized_record_bytes
            || bytes > 8 * 1024 * 1024
            || (next.collection_index() == cursor.collection_index()
                && next.after_key() != Some(batch.resume_after_key.as_str()))
        {
            return Err(invalid("Registered row actual bytes/resume differ"));
        }
    }
    Ok(())
}
