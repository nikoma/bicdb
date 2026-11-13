//! Actual Registered remote history and rows. Never local source lineage.
use super::*;
use crate::distribution_native_host::{
    NativeRegisteredRemoteProposalPoint, NativeRegisteredRemoteTransactionRequest,
};
use crate::distribution_range_consensus::{NativeRegisteredRemoteInstallingFence, RangeWriteStore};
use crate::distribution_range_image::{
    NativeRemoteImageSourcePayloadV1, RangeImageHistoryCursorV1, RangeImageHistoryStepV1,
};

mod final_publication;
mod rows;
mod verification;
#[cfg(test)]
pub(crate) use final_publication::test_registered_remote_final_image_after_wal;
pub(crate) use final_publication::{
    HistoricalRegisteredFinalImage, NativeRegisteredRemoteFinalImagePointer,
    NativeRegisteredRemoteFinalImageReceipt,
};
#[cfg(test)]
pub(crate) use rows::test_registered_remote_row_wal_cut;
pub(crate) use rows::{
    NativeRegisteredRemoteRowProgress, NativeRegisteredRemoteRowTransactionGrant,
};
pub(crate) use verification::NativeRegisteredRemoteVerificationProgress;

fn header_key(attempt: Uuid) -> String {
    format!("registered-remote-image/{attempt}/history")
}
fn initial_chain() -> String {
    hex::encode(sha2::Sha256::digest(b"bicdb.range-image.history-wire.v1\0"))
}

/// Every input is actual, borrowed and independently current. A manifest or
/// persistent Installing record alone cannot construct this guarded context.
pub(crate) struct NativeRegisteredRemoteImageContext<'a> {
    store: &'a RangeWriteStore,
    point: &'a NativeRegisteredRemoteProposalPoint<'a>,
    scope: &'a NativeRegisteredRemoteMutationScope<'a>,
    fence: &'a Arc<NativeRegisteredRemoteInstallingFence>,
    proposal: Arc<NativeRegisteredRemoteProposalReceipt>,
    base: Option<Arc<crate::db::RegisteredRemoteBaseBoundary>>,
}
impl<'a> NativeRegisteredRemoteImageContext<'a> {
    pub(crate) fn new(
        db: &BicDb,
        store: &'a RangeWriteStore,
        point: &'a NativeRegisteredRemoteProposalPoint<'a>,
        scope: &'a NativeRegisteredRemoteMutationScope<'a>,
        fence: &'a Arc<NativeRegisteredRemoteInstallingFence>,
        base: Option<Arc<crate::db::RegisteredRemoteBaseBoundary>>,
    ) -> Result<Self> {
        if let Some(boundary) = &base {
            point.require_original_mutation_identity(db, store, scope)?;
            boundary.require_boundary(db, scope, point.preflight().safety())?;
        } else {
            point.require_mutation_boundary(db, store, scope)?;
        }
        let proposal = Arc::new(db.read_native_registered_remote_proposal_for_mutation(
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
            base,
        };
        result.require_current(db)?;
        Ok(result)
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        if let Some(boundary) = &self.base {
            self.point
                .require_original_mutation_identity(db, self.store, self.scope)?;
            boundary.require_boundary(db, self.scope, self.point.preflight().safety())?;
        } else {
            self.point
                .require_mutation_boundary(db, self.store, self.scope)?;
        }
        self.proposal
            .require_mutation_current(db, self.store, self.scope)?;
        self.fence.require_catalogue_binding(
            self.store,
            db,
            self.point.preflight(),
            &self.proposal,
        )?;
        if self.fence.attempt_id() != self.scope.attempt_id()
            || self.fence.range().id != self.scope.range_id()
            || self.fence.source_manifest_sha256()
                != self.point.preflight().source().manifest().checksum_sha256
        {
            return Err(invalid(
                "Registered remote image changed actual source/A/Installing",
            ));
        }
        self.point.require_live()
    }
    pub(in crate::db) fn native_scope(&self) -> &NativeRegisteredRemoteMutationScope<'_> {
        self.scope
    }
    pub(in crate::db) fn actual_preflight(
        &self,
    ) -> &crate::distribution_native_host::NativeRegisteredRemoteInstallPreflight {
        self.point.preflight()
    }
    /// Identity constraints from the actual current request, never authority
    /// reconstructed from numeric IDs. Role/OWN/source checks remain mandatory.
    pub(in crate::db) fn actual_publication_request_ids(&self) -> (Uuid, Uuid, Uuid, Uuid) {
        (
            self.point.request().role().role().context_id(),
            self.point.request().own().context_id(),
            self.point.request().business(),
            self.point.request().member().store_id(),
        )
    }
    pub(in crate::db) fn with_owned_base(
        &self,
        base: Arc<crate::db::RegisteredRemoteBaseBoundary>,
    ) -> Self {
        Self {
            store: self.store,
            point: self.point,
            scope: self.scope,
            fence: self.fence,
            proposal: self.proposal.clone(),
            base: Some(base),
        }
    }
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
    proposal: NativeRegisteredRemoteProposalPointer,
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
            &("registered-remote-history-v1", value),
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
            return Err(invalid("damaged Registered remote history header"));
        }
        Ok(())
    }
    fn require_context(
        &self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
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
                "Registered remote stored history differs from actual original context",
            ));
        }
        Ok(())
    }
}
struct HistoryPending {
    header: HistoryHeader,
    bytes: Vec<u8>,
    commit_returned: Option<u64>,
}
/// Original host retains this exact generation and every attempted native
/// outcome BEFORE its first put. A retry cannot silently create another spine.
pub(crate) struct NativeRegisteredRemoteImageInstallation {
    preflight: Arc<crate::distribution_native_host::NativeRegisteredRemoteInstallPreflight>,
    header: HistoryHeader,
    pending: Option<HistoryPending>,
    native_history_commit_returned: Option<u64>,
    rows: Option<rows::RetainedRemoteRows>,
    verification: Option<verification::RegisteredTargetVerifier>,
    final_publication: Option<final_publication::RegisteredFinalPublication>,
    base_publication: Option<crate::db::RegisteredRemoteBasePublication>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeRegisteredRemoteHistoryProgress {
    pub cursor: RangeImageHistoryCursorV1,
    pub verified_entries: u64,
    pub audited: bool,
}
pub(crate) enum NativeRegisteredRemoteImageOperation {
    BeginHistory,
    AppendHistory,
    AuditHistory { max_entries: usize },
    ApplyRows,
    BeginVerification,
    VerifyTarget { max_scanned_records: usize },
    PublishFinalImage,
    PublishNativeBase,
    AdoptCatalogueFloor,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum NativeRegisteredRemoteImageProgress {
    History(NativeRegisteredRemoteHistoryProgress),
    Rows(NativeRegisteredRemoteRowProgress),
    Verification(NativeRegisteredRemoteVerificationProgress),
    FinalImage(NativeRegisteredRemoteFinalImagePointer),
    NativeBase {
        range_index: u64,
        native_page_xid: u64,
        checksum_sha256: String,
    },
    CatalogueFloor {
        range_index: u64,
        native_page_xid: u64,
        native_receipt_sha256: String,
    },
}
/// Actual fully stored and separately audited source spine, not a range floor,
/// row success, local SourceLineage, activated configuration or read grant.
pub(crate) struct NativeRegisteredRemoteHistoryProof {
    header: HistoryHeader,
    database_open: String,
}
impl NativeRegisteredRemoteHistoryProof {
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
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<()> {
        self.header.require_context(db, context)?;
        if !self.header.audited
            || self.database_open != db.incarnation_id()
            || db.read_registered_remote_history(self.header.attempt)? != self.header
        {
            return Err(invalid(
                "Registered remote history proof stale, incomplete or reopened",
            ));
        }
        Ok(())
    }
}
impl NativeRegisteredRemoteImageInstallation {
    pub(crate) fn advance(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        operation: NativeRegisteredRemoteImageOperation,
    ) -> Result<NativeRegisteredRemoteImageProgress> {
        if self.base_publication.is_some()
            && !matches!(
                operation,
                NativeRegisteredRemoteImageOperation::PublishNativeBase
            )
        {
            return Err(invalid(
                "Registered native base publication forbids further image mutation",
            ));
        }
        match operation {
            NativeRegisteredRemoteImageOperation::AdoptCatalogueFloor => Err(invalid(
                "Registered catalogue adoption requires the original host mutable service sink",
            )),
            NativeRegisteredRemoteImageOperation::BeginHistory => self
                .begin_history(db, context)
                .map(NativeRegisteredRemoteImageProgress::History),
            NativeRegisteredRemoteImageOperation::AppendHistory => self
                .append_history(db, context)
                .map(NativeRegisteredRemoteImageProgress::History),
            NativeRegisteredRemoteImageOperation::AuditHistory { max_entries } => self
                .audit_history(db, context, max_entries)
                .map(NativeRegisteredRemoteImageProgress::History),
            NativeRegisteredRemoteImageOperation::ApplyRows => self
                .apply_rows(db, context)
                .map(NativeRegisteredRemoteImageProgress::Rows),
            NativeRegisteredRemoteImageOperation::BeginVerification => self
                .begin_verification(db, context)
                .map(NativeRegisteredRemoteImageProgress::Verification),
            NativeRegisteredRemoteImageOperation::VerifyTarget {
                max_scanned_records,
            } => self
                .verify_target(db, context, max_scanned_records)
                .map(NativeRegisteredRemoteImageProgress::Verification),
            NativeRegisteredRemoteImageOperation::PublishFinalImage => self
                .publish_final_image(db, context)
                .map(NativeRegisteredRemoteImageProgress::FinalImage),
            NativeRegisteredRemoteImageOperation::PublishNativeBase => {
                if self.base_publication.is_none() {
                    let final_image = self.final_image_receipt().cloned().ok_or_else(|| {
                        invalid("Registered native base requires actual final image receipt")
                    })?;
                    self.base_publication =
                        Some(crate::db::RegisteredRemoteBasePublication::retain(
                            db,
                            context,
                            final_image,
                        )?);
                }
                self.base_publication.as_mut().unwrap().publish(db, context)
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"history":self.header,"history_pending":self.pending.as_ref().map(|p|serde_json::json!({"header":p.header,"native_commit_returned":p.commit_returned})),
            "rows":self.rows.as_ref().map(|rows|rows.test_state()),
            "verification":self.verification.as_ref().map(|v|v.test_state()),
            "final_image":self.final_publication.as_ref().map(|p|p.test_state()),
            "native_base":self.base_publication.as_ref().map(|p|p.test_state()),
            "actual_original_host_retained":true,"no_floor_or_serving":self.base_publication.is_none(),"serving":false})
    }
    pub(crate) fn retain(context: &NativeRegisteredRemoteImageContext<'_>) -> Result<Self> {
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
            final_publication: None,
            base_publication: None,
        })
    }
    pub(crate) fn base_boundary(&self) -> Option<Arc<crate::db::RegisteredRemoteBaseBoundary>> {
        self.base_publication.as_ref().and_then(|p| p.boundary())
    }
    pub(crate) fn verified_base(&self) -> Option<&NativeInstalledRangeImageBase> {
        self.base_publication
            .as_ref()
            .and_then(|p| p.verified_base())
    }
    fn require_owner(
        &self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<()> {
        if !std::ptr::eq(Arc::as_ref(&self.preflight), context.point.preflight()) {
            return Err(invalid(
                "Registered image owner changed original host attempt",
            ));
        }
        context.require_current(db)
    }
    fn reconcile_history(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<()> {
        self.require_owner(db, context)?;
        if let Some(pending) = &self.pending {
            let actual = db.read_registered_remote_history(pending.header.attempt)?;
            if actual != pending.header
                || bounded_json(&actual, MAX_NATIVE_RECEIPT_BYTES)? != pending.bytes
                || db
                    .paged_records
                    .as_ref()
                    .ok_or_else(|| invalid("remote history page store absent"))?
                    .store()
                    .transactions()
                    .status(actual.native_xid)
                    != bicdb_page::TxStatus::Committed
            {
                return Err(invalid(
                    "Registered history attempted native outcome remains unproven",
                ));
            }
            self.header = actual;
            self.pending = None;
        }
        if self.header.native_xid != 0 {
            self.header.require_context(db, context)?;
            if db.read_registered_remote_history(self.header.attempt)? != self.header {
                return Err(invalid("Registered actual history owner/header differ"));
            }
        }
        Ok(())
    }
    fn persist_history(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        mut next: HistoryHeader,
        entries: &[NativeLogicalHistoryEntryV1],
    ) -> Result<()> {
        self.require_owner(db, context)?;
        crate::format::enable_native_registered_remote_image(db.data_path())?;
        let controlled = db
            .range_mutation_gate()
            .try_controlled_mutation(context.scope.frozen())?;
        let result = db.in_paged_transaction(|paged, xid| {
            context.require_current(db)?;
            next.native_xid = xid;
            next.checksum_sha256 = next.checksum()?;
            next.require_context(db, context)?;
            let bytes = bounded_json(&next, MAX_NATIVE_RECEIPT_BYTES)?;
            self.pending = Some(HistoryPending {
                header: next.clone(),
                bytes: bytes.clone(),
                commit_returned: None,
            });
            for entry in entries {
                history::put_immutable_history_entry(paged, xid, next.generation, entry)?;
            }
            paged
                .store()
                .put(xid, &native_key(&header_key(next.attempt)), &bytes)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            context.require_current(db)
        });
        if let Ok(Some(xid)) = &result {
            self.native_history_commit_returned = Some(*xid);
            self.pending
                .as_mut()
                .ok_or_else(|| invalid("remote history attempted owner absent"))?
                .commit_returned = Some(*xid);
        }
        drop(controlled);
        result?.ok_or_else(|| invalid("remote history has no native page store"))?;
        self.reconcile_history(db, context)
    }
    pub(crate) fn begin_history(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteHistoryProgress> {
        self.reconcile_history(db, context)?;
        if self.header.native_xid == 0 {
            if db
                .read_native_range_image_record::<HistoryHeader>(&header_key(self.header.attempt))?
                .is_some()
            {
                return Err(invalid(
                    "Registered history exists without original retained owner",
                ));
            }
            self.persist_history(db, context, self.header.clone(), &[])?;
        }
        Ok(self.history_progress())
    }
    fn history_progress(&self) -> NativeRegisteredRemoteHistoryProgress {
        NativeRegisteredRemoteHistoryProgress {
            cursor: self.header.cursor.clone(),
            verified_entries: self.header.verified,
            audited: self.header.audited,
        }
    }
    pub(crate) fn append_history(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteHistoryProgress> {
        self.reconcile_history(db, context)?;
        self.header.require_context(db, context)?;
        let NativeRemoteImageSourcePayloadV1::History(step) = context.point.frame().payload()?
        else {
            return Err(invalid(
                "Registered history append requires actual authenticated History frame",
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
            return Err(invalid("Registered history frame hash/session changed"));
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
            return Err(invalid(
                "Registered history frame cursor or source cut changed",
            ));
        }
        for entry in &source.entries {
            context.point.require_live()?;
            entry.validate_shape()?;
            if entry.range_id != next.range || entry.index != next.verified {
                return Err(invalid("Registered history missing/reordered entry"));
            }
            if entry.index == 0 {
                if entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis
                    || entry.commitment_sha256 != next.h0
                {
                    return Err(invalid("Registered history genesis differs"));
                }
            } else if next.head.as_deref() != Some(entry.previous_sha256.as_str()) {
                return Err(invalid("Registered history forked source chain"));
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
            return Err(invalid("Registered full source history completion differs"));
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
        context: &NativeRegisteredRemoteImageContext<'_>,
        max_entries: usize,
    ) -> Result<NativeRegisteredRemoteHistoryProgress> {
        if !(1..=history::MAX_HISTORY_STEP_ENTRIES).contains(&max_entries) {
            return Err(invalid("Registered history audit bound invalid"));
        }
        self.reconcile_history(db, context)?;
        self.header.require_context(db, context)?;
        if !self.header.cursor.complete() {
            return Err(invalid("Registered cannot audit incomplete history"));
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
                    return Err(invalid("Registered stored history audit missing/forked"));
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
                return Err(invalid("Registered stored history final head differs"));
            }
            self.persist_history(db, context, next, &[])?;
        }
        Ok(self.history_progress())
    }
    pub(crate) fn finish_history(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteHistoryProof> {
        self.reconcile_history(db, context)?;
        self.header.require_context(db, context)?;
        if !self.header.audited {
            return Err(invalid("Registered full stored history not audited"));
        }
        let first = db.read_native_history_entry(self.header.range, self.header.generation, 0)?;
        let last = db.read_native_history_entry(
            self.header.range,
            self.header.generation,
            self.header.range_index,
        )?;
        if first.commitment_sha256 != self.header.h0 || last.commitment_sha256 != self.header.h {
            return Err(invalid("Registered history proof endpoints changed"));
        }
        Ok(NativeRegisteredRemoteHistoryProof {
            header: self.header.clone(),
            database_open: db.incarnation_id().into(),
        })
    }
}
impl BicDb {
    fn read_registered_remote_history(&self, attempt: Uuid) -> Result<HistoryHeader> {
        if !crate::format::native_registered_remote_image_active(self.data_path())? {
            return Err(invalid(
                "Registered history sticky local reader fence missing",
            ));
        }
        let header: HistoryHeader = self
            .read_native_range_image_record(&header_key(attempt))?
            .ok_or_else(|| invalid("Registered remote history header absent"))?;
        header.validate()?;
        if header.attempt != attempt {
            return Err(invalid("Registered history key/A mismatch"));
        }
        Ok(header)
    }
}
