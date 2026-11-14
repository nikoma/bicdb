//! Actual additional Joined history/rows. SAME original State + A; no FIRST,
//! source-lineage cast, native base, ordinary write domain or Serving.
use super::*;
use crate::distribution_native_host::{
    NativeJoinedRemoteImagePoint, NativeJoinedRemoteTransactionRequest,
};
use crate::distribution_range_consensus::{NativeJoinedRemoteInstallingFence, RangeWriteStore};
use crate::distribution_range_image::{
    NativeRemoteImageSourcePayloadV1, RangeImageHistoryCursorV1,
};
mod final_publication;
mod rows;
mod verification;
pub(crate) use final_publication::{
    NativeJoinedRemoteFinalImagePointer, NativeJoinedRemoteFinalImageReceipt,
};
pub(crate) use rows::{NativeJoinedRemoteRowProgress, NativeJoinedRemoteRowTransactionGrant};
pub(crate) use verification::NativeJoinedRemoteVerificationProgress;

fn header_key(attempt: Uuid) -> String {
    format!("joined-remote-image/{attempt}/history")
}
fn initial_chain() -> String {
    hex::encode(sha2::Sha256::digest(b"bicdb.range-image.history-wire.v1\0"))
}

pub(crate) struct NativeJoinedRemoteImageContext<'a> {
    store: &'a RangeWriteStore,
    point: &'a NativeJoinedRemoteImagePoint<'a>,
    scope: &'a NativeJoinedRemoteMutationScope<'a>,
    fence: &'a Arc<NativeJoinedRemoteInstallingFence>,
    proposal: Arc<NativeJoinedRemoteProposalReceipt>,
}
impl<'a> NativeJoinedRemoteImageContext<'a> {
    pub(crate) fn new(
        db: &BicDb,
        store: &'a RangeWriteStore,
        point: &'a NativeJoinedRemoteImagePoint<'a>,
        scope: &'a NativeJoinedRemoteMutationScope<'a>,
        fence: &'a Arc<NativeJoinedRemoteInstallingFence>,
    ) -> Result<Self> {
        point.require_live()?;
        scope.require_store(db, store)?;
        if !Arc::ptr_eq(scope.preflight(), point.preflight_arc()) {
            return Err(invalid(
                "Joined image context changed original scope/preflight",
            ));
        }
        let proposal = Arc::new(db.read_native_joined_remote_proposal_for_mutation(
            store,
            scope,
            fence.protected_proposal(),
        )?);
        let result = Self {
            store,
            point,
            scope,
            fence,
            proposal,
        };
        result.require_current(db)?;
        Ok(result)
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.point.require_live()?;
        self.scope.require_store(db, self.store)?;
        self.proposal
            .require_mutation_current(db, self.store, self.scope)?;
        self.fence
            .require_mutation_current(self.store, db, self.scope, &self.proposal)?;
        if !Arc::ptr_eq(self.scope.preflight(), self.point.preflight_arc())
            || self.scope.attempt_id() != self.fence.attempt_id()
            || self.scope.range_id() != self.fence.range().id
        {
            return Err(invalid("Joined image original A/range/Installing changed"));
        }
        self.point.require_live()
    }
}
pub(crate) enum NativeJoinedRemoteImageOperation {
    BeginHistory,
    AppendHistory,
    AuditHistory { max_entries: usize },
    ApplyRows,
    BeginVerification,
    VerifyTarget { max_scanned_records: usize },
    PublishFinal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum NativeJoinedRemoteImageProgress {
    History(NativeJoinedRemoteHistoryProgress),
    Rows(NativeJoinedRemoteRowProgress),
    Verification(NativeJoinedRemoteVerificationProgress),
    FinalImage(NativeJoinedRemoteFinalImagePointer),
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HistoryHeader {
    version: u32,
    attempt: Uuid,
    range: RangeId,
    birth: Uuid,
    generation: Uuid,
    manifest_sha256: String,
    intent_sha256: String,
    proposal: NativeJoinedRemoteProposalPointer,
    range_index: u64,
    h0: String,
    h: String,
    source_receipt: String,
    session: Uuid,
    nonce: Uuid,
    cursor: RangeImageHistoryCursorV1,
    wire_chain: String,
    verified: u64,
    head: Option<String>,
    last_previous: Option<RangeImageHistoryCursorV1>,
    last_sha256: Option<String>,
    audit_next: u64,
    audit_head: Option<String>,
    audited: bool,
    native_xid: u64,
    checksum_sha256: String,
}
impl HistoryHeader {
    fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("joined-remote-history-v1", value),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.attempt.is_nil()
            || self.birth.is_nil()
            || self.generation.is_nil()
            || self.native_xid == 0
            || self.proposal.attempt_id != self.attempt
            || self.session.is_nil()
            || self.nonce.is_nil()
            || self.verified
                > self
                    .range_index
                    .checked_add(1)
                    .ok_or_else(|| invalid("remote history count overflow"))?
            || self.audit_next > self.verified
            || self.cursor.next_index() != self.verified
            || (self.audited
                && (!self.cursor.complete()
                    || self.audit_next != self.verified
                    || self.audit_head.as_deref() != Some(self.h.as_str())))
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged Joined remote history header"));
        }
        Ok(())
    }
    fn require_context(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        context.require_current(db)?;
        self.validate()?;
        let manifest = context.point.preflight().source().manifest();
        if self.attempt != context.scope.attempt_id()
            || self.range != manifest.capture_range.id
            || self.birth != context.scope.native().birth_id()
            || self.manifest_sha256 != manifest.checksum_sha256
            || self.intent_sha256 != context.fence.intent_sha256()?
            || self.proposal != *context.fence.protected_proposal()
            || self.range_index != manifest.range_index
            || self.h0 != manifest.h0
            || self.h != manifest.h
            || self.source_receipt != manifest.source_native_receipt
            || self.session != manifest.handoff_session
            || self.nonce != manifest.nonce
        {
            return Err(invalid(
                "Joined remote stored history differs from actual original context",
            ));
        }
        Ok(())
    }
}
struct HistoryPending {
    header: HistoryHeader,
    entries: Vec<NativeLogicalHistoryEntryV1>,
    native_begin: Option<u64>,
    bytes: Option<Vec<u8>>,
    commit_returned: Option<u64>,
}
/// Original host retains this exact generation and every attempted native
/// outcome BEFORE its first put. A retry cannot silently create another spine.
pub(crate) struct NativeJoinedRemoteImageInstallation {
    preflight: Arc<crate::distribution_native_host::NativeJoinedRemoteInstallPreflight>,
    header: HistoryHeader,
    pending: Option<HistoryPending>,
    native_history_commit_returned: Option<u64>,
    rows: Option<rows::RetainedRemoteRows>,
    verification: Option<verification::JoinedTargetVerifier>,
    scan_pending: Option<verification::JoinedTargetScanReadPoint>,
    final_publication: Option<final_publication::JoinedFinalPublication>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeJoinedRemoteHistoryProgress {
    pub cursor: RangeImageHistoryCursorV1,
    pub verified_entries: u64,
    pub audited: bool,
}
/// Actual fully stored and separately audited source spine, not a range floor,
/// row success, local SourceLineage, activated configuration or read grant.
pub(crate) struct NativeJoinedRemoteHistoryProof {
    header: HistoryHeader,
    database_open: String,
}
impl NativeJoinedRemoteHistoryProof {
    pub(crate) fn history_generation(&self) -> Uuid {
        self.header.generation
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.header.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.header.h0
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.header.h
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        self.header.require_context(db, context)?;
        if !self.header.audited
            || self.database_open != db.incarnation_id()
            || db.read_joined_remote_history(self.header.attempt)? != self.header
        {
            return Err(invalid(
                "Joined remote history proof stale, incomplete or reopened",
            ));
        }
        Ok(())
    }
}
impl NativeJoinedRemoteImageInstallation {
    pub(crate) fn advance(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        operation: NativeJoinedRemoteImageOperation,
    ) -> Result<NativeJoinedRemoteImageProgress> {
        match operation {
            NativeJoinedRemoteImageOperation::BeginHistory => self
                .begin_history(db, context)
                .map(NativeJoinedRemoteImageProgress::History),
            NativeJoinedRemoteImageOperation::AppendHistory => self
                .append_history(db, context)
                .map(NativeJoinedRemoteImageProgress::History),
            NativeJoinedRemoteImageOperation::AuditHistory { max_entries } => self
                .audit_history(db, context, max_entries)
                .map(NativeJoinedRemoteImageProgress::History),
            NativeJoinedRemoteImageOperation::ApplyRows => self
                .apply_rows(db, context)
                .map(NativeJoinedRemoteImageProgress::Rows),
            NativeJoinedRemoteImageOperation::BeginVerification => self
                .begin_verification(db, context)
                .map(NativeJoinedRemoteImageProgress::Verification),
            NativeJoinedRemoteImageOperation::PublishFinal => self
                .publish_final_image(db, context)
                .map(NativeJoinedRemoteImageProgress::FinalImage),
            NativeJoinedRemoteImageOperation::VerifyTarget { .. } => Err(invalid(
                "Joined physical target scan requires actual unlocked host read-point path",
            )),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"history":self.header,"pending":self.pending.as_ref().map(|p|serde_json::json!({
            "header":p.header,"issued_native_xid":p.native_begin,"commit_returned":p.commit_returned,
            "candidate_framed":p.bytes.is_some()})),"rows":self.rows.as_ref().map(|r|r.test_state()),
            "verification":self.verification.as_ref().map(|v|v.test_state()),
            "pending_read_point":self.scan_pending.as_ref().map(|p|p.test_state()),
            "final_publication":self.final_publication.as_ref().map(|p|p.test_state()),
            "native_base_published":false,"Serving":false})
    }
}
impl NativeJoinedRemoteImageInstallation {
    pub(crate) fn retain(context: &NativeJoinedRemoteImageContext<'_>) -> Result<Self> {
        context.point.require_live()?;
        let manifest = context.point.preflight().source().manifest();
        Ok(Self {
            preflight: context.point.preflight_arc().clone(),
            header: HistoryHeader {
                version: 1,
                attempt: manifest.target_attempt,
                range: manifest.capture_range.id,
                birth: context.scope.native().birth_id(),
                generation: Uuid::now_v7(),
                manifest_sha256: manifest.checksum_sha256.clone(),
                intent_sha256: context.fence.intent_sha256()?,
                proposal: context.fence.protected_proposal().clone(),
                range_index: manifest.range_index,
                h0: manifest.h0.clone(),
                h: manifest.h.clone(),
                source_receipt: manifest.source_native_receipt.clone(),
                session: manifest.handoff_session,
                nonce: manifest.nonce,
                cursor: RangeImageHistoryCursorV1::default(),
                wire_chain: initial_chain(),
                verified: 0,
                head: None,
                last_previous: None,
                last_sha256: None,
                audit_next: 0,
                audit_head: None,
                audited: false,
                native_xid: 0,
                checksum_sha256: String::new(),
            },
            pending: None,
            native_history_commit_returned: None,
            rows: None,
            verification: None,
            scan_pending: None,
            final_publication: None,
        })
    }
    fn require_owner(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        if !std::ptr::eq(Arc::as_ref(&self.preflight), context.point.preflight()) {
            return Err(invalid("Joined image owner changed original host attempt"));
        }
        context.require_current(db)
    }
    fn reconcile_history(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        self.require_owner(db, context)?;
        if let Some(pending) = &self.pending {
            if let Some(xid) = pending.native_begin {
                let bytes = pending.bytes.as_ref().ok_or_else(|| invalid("Joined history issued native xid before framing; original attempt retained"))?;
                let actual = db.read_joined_remote_history(pending.header.attempt)?;
                if actual != pending.header
                    || bounded_json(&actual, MAX_NATIVE_RECEIPT_BYTES)?.as_slice()
                        != bytes.as_slice()
                    || actual.native_xid != xid
                    || db
                        .paged_records
                        .as_ref()
                        .ok_or_else(|| invalid("Joined history page store absent"))?
                        .store()
                        .transactions()
                        .status(xid)
                        != bicdb_page::TxStatus::Committed
                {
                    return Err(invalid(
                        "Joined original history native outcome remains unproven",
                    ));
                }
                self.header = actual;
                self.pending = None;
            }
        }
        if self.header.native_xid != 0 {
            self.header.require_context(db, context)?;
            if db.read_joined_remote_history(self.header.attempt)? != self.header {
                return Err(invalid("Joined original history owner/header differ"));
            }
        }
        Ok(())
    }
    fn persist_history(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        next: HistoryHeader,
        entries: &[NativeLogicalHistoryEntryV1],
    ) -> Result<()> {
        self.require_owner(db, context)?;
        crate::format::enable_native_joined_remote_image(db.data_path())?;
        if let Some(pending) = &self.pending {
            if pending.native_begin.is_some()
                || pending.header != next
                || pending.entries.as_slice() != entries
            {
                return Err(invalid(
                    "Joined history retry cannot replace original pending native attempt",
                ));
            }
        } else {
            self.pending = Some(HistoryPending {
                header: next,
                entries: entries.to_vec(),
                native_begin: None,
                bytes: None,
                commit_returned: None,
            });
        }
        let pending = self.pending.as_mut().unwrap();
        let controlled = db
            .range_mutation_gate()
            .try_controlled_mutation(context.scope.frozen()?)?;
        let result = db.in_paged_transaction(|paged, xid| {
            pending.native_begin = Some(xid);
            pending.header.native_xid = xid;
            context.require_current(db)?;
            pending.header.checksum_sha256 = pending.header.checksum()?;
            pending.header.require_context(db, context)?;
            let bytes = bounded_json(&pending.header, MAX_NATIVE_RECEIPT_BYTES)?;
            pending.bytes = Some(bytes);
            for entry in &pending.entries {
                history::put_immutable_history_entry(paged, xid, pending.header.generation, entry)?;
            }
            paged
                .store()
                .put(
                    xid,
                    &native_key(&header_key(pending.header.attempt)),
                    pending.bytes.as_ref().unwrap(),
                )
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            context.require_current(db)
        });
        if let Ok(Some(xid)) = &result {
            self.native_history_commit_returned = Some(*xid);
            pending.commit_returned = Some(*xid);
        }
        drop(controlled);
        result?.ok_or_else(|| invalid("Joined history requires native page store"))?;
        self.reconcile_history(db, context)
    }
    pub(crate) fn begin_history(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<NativeJoinedRemoteHistoryProgress> {
        self.reconcile_history(db, context)?;
        if self.header.native_xid == 0 {
            if db
                .read_native_range_image_record::<HistoryHeader>(&header_key(self.header.attempt))?
                .is_some()
            {
                return Err(invalid(
                    "Joined history exists without original retained owner",
                ));
            }
            self.persist_history(db, context, self.header.clone(), &[])?;
        }
        Ok(self.history_progress())
    }
    fn history_progress(&self) -> NativeJoinedRemoteHistoryProgress {
        NativeJoinedRemoteHistoryProgress {
            cursor: self.header.cursor.clone(),
            verified_entries: self.header.verified,
            audited: self.header.audited,
        }
    }
    pub(crate) fn append_history(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<NativeJoinedRemoteHistoryProgress> {
        self.reconcile_history(db, context)?;
        self.header.require_context(db, context)?;
        let NativeRemoteImageSourcePayloadV1::History(step) = context.point.frame().payload()?
        else {
            return Err(invalid(
                "Joined history append requires actual authenticated History frame",
            ));
        };
        let mut unsigned = step.clone();
        unsigned.chain_sha256.clear();
        if step.format_version != 1
            || step.chain_sha256
                != hex::encode(sha2::Sha256::digest(bounded_json(&unsigned, 272 * 1024)?))
            || step.session_id != self.header.session
            || step.nonce != self.header.nonce
        {
            return Err(invalid("Joined history frame hash/session changed"));
        }
        if self.header.last_previous.as_ref() == Some(&step.previous_cursor)
            && self.header.last_sha256.as_deref() == Some(step.chain_sha256.as_str())
        {
            return Ok(self.history_progress());
        }
        let mut next = self.header.clone();
        let source = &step.history;
        if next.cursor.complete()
            || step.previous_cursor != next.cursor
            || step.previous_chain_sha256 != next.wire_chain
            || source.format_version != 1
            || source.range_id != next.range
            || source.source_range_index != next.range_index
            || source.source_native_receipt_sha256 != next.source_receipt
            || source.logical_genesis_sha256 != next.h0
            || source.logical_history_sha256 != next.h
            || source.first_index != next.verified
            || source.entries.is_empty()
            || source.entries.len() > history::MAX_HISTORY_STEP_ENTRIES
        {
            return Err(invalid("Joined history frame cursor or source cut changed"));
        }
        for entry in &source.entries {
            context.point.require_live()?;
            entry.validate_shape()?;
            if entry.range_id != next.range || entry.index != next.verified {
                return Err(invalid("Joined history missing/reordered entry"));
            }
            if entry.index == 0 {
                if entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis
                    || entry.commitment_sha256 != next.h0
                {
                    return Err(invalid("Joined history genesis differs"));
                }
            } else if next.head.as_deref() != Some(entry.previous_sha256.as_str()) {
                return Err(invalid("Joined history forked source chain"));
            }
            next.head = Some(entry.commitment_sha256.clone());
            next.verified = next
                .verified
                .checked_add(1)
                .ok_or_else(|| invalid("remote history count overflow"))?;
        }
        let total = next
            .range_index
            .checked_add(1)
            .ok_or_else(|| invalid("remote history total overflow"))?;
        let complete = next.verified == total;
        if next.verified > total
            || source.next_index != if complete { None } else { Some(next.verified) }
            || step.next_cursor.next_index() != next.verified
            || step.next_cursor.complete() != complete
            || step.next_cursor.step()
                != next
                    .cursor
                    .step()
                    .checked_add(1)
                    .ok_or_else(|| invalid("remote history step overflow"))?
            || (complete && next.head.as_deref() != Some(next.h.as_str()))
        {
            return Err(invalid("Joined full source history completion differs"));
        }
        next.cursor = step.next_cursor.clone();
        next.wire_chain = step.chain_sha256.clone();
        next.last_previous = Some(step.previous_cursor.clone());
        next.last_sha256 = Some(step.chain_sha256.clone());
        self.persist_history(db, context, next, &source.entries)?;
        Ok(self.history_progress())
    }
    pub(crate) fn audit_history(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        max_entries: usize,
    ) -> Result<NativeJoinedRemoteHistoryProgress> {
        if !(1..=history::MAX_HISTORY_STEP_ENTRIES).contains(&max_entries) {
            return Err(invalid("Joined history audit bound invalid"));
        }
        self.reconcile_history(db, context)?;
        self.header.require_context(db, context)?;
        if !self.header.cursor.complete() {
            return Err(invalid("Joined cannot audit incomplete history"));
        }
        if !self.header.audited {
            let mut next = self.header.clone();
            for _ in 0..max_entries {
                context.point.require_live()?;
                let entry =
                    db.read_native_history_entry(next.range, next.generation, next.audit_next)?;
                if (entry.index == 0
                    && (entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis
                        || entry.commitment_sha256 != next.h0))
                    || (entry.index > 0
                        && next.audit_head.as_deref() != Some(entry.previous_sha256.as_str()))
                {
                    return Err(invalid("Joined stored history audit missing/forked"));
                }
                next.audit_head = Some(entry.commitment_sha256);
                next.audit_next = next
                    .audit_next
                    .checked_add(1)
                    .ok_or_else(|| invalid("remote history audit overflow"))?;
                if entry.index == next.range_index {
                    next.audited = true;
                    break;
                }
            }
            if next.audited && next.audit_head.as_deref() != Some(next.h.as_str()) {
                return Err(invalid("Joined stored history final head differs"));
            }
            self.persist_history(db, context, next, &[])?;
        }
        Ok(self.history_progress())
    }
    pub(crate) fn finish_history(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<NativeJoinedRemoteHistoryProof> {
        self.reconcile_history(db, context)?;
        self.header.require_context(db, context)?;
        if !self.header.audited {
            return Err(invalid("Joined full stored history not audited"));
        }
        let first = db.read_native_history_entry(self.header.range, self.header.generation, 0)?;
        let last = db.read_native_history_entry(
            self.header.range,
            self.header.generation,
            self.header.range_index,
        )?;
        if first.commitment_sha256 != self.header.h0 || last.commitment_sha256 != self.header.h {
            return Err(invalid("Joined history proof endpoints changed"));
        }
        Ok(NativeJoinedRemoteHistoryProof {
            header: self.header.clone(),
            database_open: db.incarnation_id().into(),
        })
    }
}
impl BicDb {
    fn read_joined_remote_history(&self, attempt: Uuid) -> Result<HistoryHeader> {
        if !crate::format::native_joined_remote_image_active(self.data_path())? {
            return Err(invalid("Joined history sticky local reader fence missing"));
        }
        let header: HistoryHeader = self
            .read_native_range_image_record(&header_key(attempt))?
            .ok_or_else(|| invalid("Joined remote history header absent"))?;
        header.validate()?;
        if header.attempt != attempt {
            return Err(invalid("Joined history key/A mismatch"));
        }
        Ok(header)
    }
}
