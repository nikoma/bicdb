//! Non-destructive, bounded target history staging. A completed spine proves
//! history compatibility only; native data/visibility/config activation remain
//! separate mandatory gates. No DTO or caller watermark constructs the proof.
use super::*;
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageInstallRole, RangeWriteStore,
    TargetImageCatalogHistoryObligations, TargetImageInstallFence,
};
use crate::distribution_range_image::{
    RangeImageHistoryCursorV1, RangeImageHistoryStepV1,
    TentativeNativeRangeImageSourceAuthorization,
};
use crate::distribution_range_mutation::RangeMutationAuthority;

const STAGE_FORMAT: u32 = 1;
const MAX_WIRE: usize = 272 * 1024;
fn stage_key(range: RangeId) -> String {
    format!("history-stage:active:{}", range.get())
}
fn published_stage_key(range: RangeId, generation: Uuid) -> String {
    format!("history-stage:published:{}:{generation}", range.get())
}
fn initial_chain() -> String {
    hex::encode(sha2::Sha256::digest(b"bicdb.range-image.history-wire.v1\0"))
}

/// Borrowed actual target inputs. The durable fence is refusal, not a grant to
/// clear data. Every bounded operation revalidates these opaque observations.
pub(crate) struct NativeHistoryTargetContext<'a> {
    store: &'a RangeWriteStore,
    fence: &'a TargetImageInstallFence,
    admission: &'a NativeRecoveryAdmission,
    safety: &'a NativeRangeInstallSafety,
    catalog: &'a TargetImageCatalogHistoryObligations,
    role: &'a NativeImageInstallRole<'a>,
    frozen: &'a FrozenRangeMutationPermit,
}
impl<'a> NativeHistoryTargetContext<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        database: &BicDb,
        store: &'a RangeWriteStore,
        fence: &'a TargetImageInstallFence,
        admission: &'a NativeRecoveryAdmission,
        safety: &'a NativeRangeInstallSafety,
        catalog: &'a TargetImageCatalogHistoryObligations,
        role: &'a NativeImageInstallRole<'a>,
        frozen: &'a FrozenRangeMutationPermit,
    ) -> Result<Self> {
        let context = Self {
            store,
            fence,
            admission,
            safety,
            catalog,
            role,
            frozen,
        };
        context.require_current(database)?;
        Ok(context)
    }
    fn require_current(&self, database: &BicDb) -> Result<()> {
        let journal = database.try_native_image_journal()?;
        self.require_current_under_journal(database, &journal)
    }
    fn require_current_under_journal(
        &self,
        database: &BicDb,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<()> {
        journal.require_database(database)?;
        self.store.validate_native_image_install_step(
            self.fence,
            database,
            self.admission,
            self.role,
            self.frozen,
        )?;
        self.catalog.require_current(self.store)?;
        self.safety.require_irreversible_current_under_journal(
            database,
            self.admission,
            self.frozen,
            journal,
        )?;
        if self.catalog.cluster_id() != self.admission.cluster_id()
            || self.catalog.local_node_id() != self.admission.local_node_id()
            || self.catalog.range_id() != self.fence.range().id
            || self.safety.range_id() != self.fence.range().id
        {
            return Err(invalid(
                "history target catalog/native/fence identities differ",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct HistorySourceScope {
    scope_sha256: String,
    range_id: RangeId,
    range_index: u64,
    native_receipt_sha256: String,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    session_id: Uuid,
    nonce: Uuid,
}
impl HistorySourceScope {
    fn actual(source: &TentativeNativeRangeImageSourceAuthorization<'_>) -> Result<Self> {
        source.require_current()?;
        let cut = source.cut();
        let lineage = source.lineage();
        if lineage.range_index() != cut.range_index() {
            return Err(invalid("history source lineage/cut index differ"));
        }
        let scope_sha256 = hex::encode(sha2::Sha256::digest(bounded_json(
            &(
                "bicdb.native.history-stage.source.v1",
                (
                    cut.cluster_id(),
                    source.bootstrap_id(),
                    cut.range().id,
                    cut.range().start_token,
                    cut.range().end_token,
                    cut.range().epoch,
                ),
                (
                    cut.session_id(),
                    cut.nonce(),
                    cut.purpose(),
                    cut.old_config_sha256(),
                    cut.new_config_sha256(),
                ),
                (
                    cut.source_id(),
                    cut.source_incarnation(),
                    cut.target_id(),
                    cut.target_incarnation(),
                ),
                (
                    lineage.range_index(),
                    source.native_receipt_sha256(),
                    lineage.logical_genesis_sha256(),
                    lineage.logical_history_sha256(),
                ),
            ),
            64 * 1024,
        )?));
        Ok(Self {
            scope_sha256,
            range_id: cut.range().id,
            range_index: lineage.range_index(),
            native_receipt_sha256: source.native_receipt_sha256().to_string(),
            logical_genesis_sha256: lineage.logical_genesis_sha256().to_string(),
            logical_history_sha256: lineage.logical_history_sha256().to_string(),
            session_id: cut.session_id(),
            nonce: cut.nonce(),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct StageHeader {
    format_version: u32,
    stage_id: Uuid,
    generation: Uuid,
    target_birth_id: Uuid,
    target_birth_sha256: String,
    attempt_id: Uuid,
    intent_sha256: String,
    native_irreversible_sha256: String,
    catalog_sha256: String,
    source: HistorySourceScope,
    target_point: Option<(u64, String)>,
    target_point_matched: bool,
    remaining_commands: BTreeMap<u64, String>,
    cursor: RangeImageHistoryCursorV1,
    verified_entries: u64,
    current_head: Option<String>,
    wire_chain: String,
    last_previous_cursor: Option<RangeImageHistoryCursorV1>,
    last_step_sha256: Option<String>,
    audit_next_index: u64,
    audit_head: Option<String>,
    audit_complete: bool,
    #[serde(default)]
    published_native_page_xid: Option<u64>,
    checksum_sha256: String,
}
impl StageHeader {
    fn checksum(&self) -> Result<String> {
        let mut copy = self.clone();
        copy.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &copy,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        let total = self
            .source
            .range_index
            .checked_add(1)
            .ok_or_else(|| invalid("history total overflow"))?;
        if self.format_version != STAGE_FORMAT
            || self.stage_id.is_nil()
            || self.generation.is_nil()
            || self.target_birth_id.is_nil()
            || self.attempt_id.is_nil()
            || self.checksum()? != self.checksum_sha256
            || self.verified_entries > total
            || self.cursor.next_index() != self.verified_entries
            || self.cursor.complete() != (self.verified_entries == total)
            || self.audit_next_index > total
            || self.audit_complete != (self.audit_next_index == total)
            || self
                .published_native_page_xid
                .is_some_and(|xid| xid == 0 || !self.audit_complete)
        {
            return Err(invalid("damaged/inconsistent native history stage header"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct NativeHistoryStage {
    database_incarnation: String,
    stage_id: Uuid,
    generation: Uuid,
    range_id: RangeId,
}
impl NativeHistoryStage {
    pub(crate) fn stage_id(&self) -> Uuid {
        self.stage_id
    }
    pub(crate) fn generation(&self) -> Uuid {
        self.generation
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NativeHistoryStageProgress {
    pub verified_entries: u64,
    pub cursor: RangeImageHistoryCursorV1,
    pub audit_complete: bool,
}

/// Actual full stored-spine audit and every native/catalog compatibility
/// obligation passed. This is not data installation or range configuration.
#[derive(Debug)]
pub(crate) struct StagedNativeHistoryProof {
    database_incarnation: String,
    stage_id: Uuid,
    header_sha256: String,
    header: StageHeader,
}
impl StagedNativeHistoryProof {
    pub(crate) fn history_generation(&self) -> Uuid {
        self.header.generation
    }
    pub(crate) fn verified_through(&self) -> u64 {
        self.header.source.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.header.source.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.header.source.logical_history_sha256
    }
    pub(crate) fn source_native_receipt_sha256(&self) -> &str {
        &self.header.source.native_receipt_sha256
    }
    pub(crate) fn target_birth_id(&self) -> Uuid {
        self.header.target_birth_id
    }
    pub(crate) fn target_birth_sha256(&self) -> &str {
        &self.header.target_birth_sha256
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.header.attempt_id
    }
    pub(crate) fn source_session_id(&self) -> Uuid {
        self.header.source.session_id
    }
    pub(crate) fn source_nonce(&self) -> Uuid {
        self.header.source.nonce
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.header.source.range_id
    }
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
    ) -> Result<()> {
        let journal = database.try_native_image_journal()?;
        context.require_current_under_journal(database, &journal)?;
        let actual = database.read_history_stage(self.range_id())?;
        if database.incarnation != self.database_incarnation
            || actual.stage_id != self.stage_id
            || actual.checksum_sha256 != self.header_sha256
            || actual != self.header
            || HistorySourceScope::actual(source)? != actual.source
            || !actual.audit_complete
        {
            return Err(invalid("staged history proof is stale/reopened/incomplete"));
        }
        Ok(())
    }
}

/// Values derived from an actual fully audited staged spine. Only the private
/// atomic native publisher creates this binding inside its page transaction.
/// A consumer writes these values into the completed native base receipt;
/// they grant no committed range configuration or read readiness.
pub(crate) struct NativeHistoryPublicationBinding<'a> {
    proof: &'a StagedNativeHistoryProof,
}
impl NativeHistoryPublicationBinding<'_> {
    pub(crate) fn history_generation(&self) -> Uuid {
        self.proof.history_generation()
    }
    pub(crate) fn verified_through(&self) -> u64 {
        self.proof.verified_through()
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        self.proof.logical_genesis_sha256()
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.proof.logical_history_sha256()
    }
    pub(crate) fn source_native_receipt_sha256(&self) -> &str {
        self.proof.source_native_receipt_sha256()
    }
    pub(crate) fn target_birth_id(&self) -> Uuid {
        self.proof.target_birth_id()
    }
    pub(crate) fn target_birth_sha256(&self) -> &str {
        self.proof.target_birth_sha256()
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.proof.attempt_id()
    }
    pub(crate) fn source_session_id(&self) -> Uuid {
        self.proof.source_session_id()
    }
    pub(crate) fn source_nonce(&self) -> Uuid {
        self.proof.source_nonce()
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.proof.range_id()
    }
}

impl BicDb {
    /// Couple the actual completed native base pointer to old-prefix retirement
    /// in ONE ordered native page-WAL transaction. Source completion, full
    /// stored history audit, actual target data/root/UID/visibility proof and
    /// current immutable catalog/native obligations are independently required.
    /// No physical data copy or O(R) history copy occurs in this transaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_staged_native_history_publication<F>(
        &self,
        proof: &StagedNativeHistoryProof,
        context: &NativeHistoryTargetContext<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        contents: &super::super::range_image_install::VerifiedNativeTargetImageContents,
        bindings: &RangeImageDbBindings,
        cancellation: &CancellationToken,
        publish_base: F,
    ) -> Result<bicdb_page::Xid>
    where
        F: FnOnce(
            &crate::paged_collection::PagedRecords,
            bicdb_page::Xid,
            &NativeHistoryPublicationBinding<'_>,
        ) -> Result<()>,
    {
        cancellation.check()?;
        source.require_current()?;
        let authorization = source.authorize_target_install()?;
        let journal = self.try_native_image_journal()?;
        context.require_current_under_journal(self, &journal)?;
        let mut header = self.read_history_stage(proof.range_id())?;
        if self.incarnation != proof.database_incarnation
            || header.stage_id != proof.stage_id
            || header != proof.header
            || header.checksum_sha256 != proof.header_sha256
            || header.source != HistorySourceScope::actual(&authorization)?
            || !header.audit_complete
            || !header.cursor.complete()
            || !header.target_point_matched
            || !header.remaining_commands.is_empty()
            || header.published_native_page_xid.is_some()
            || source.seal().logical_genesis_sha256 != proof.logical_genesis_sha256()
            || source.seal().logical_history_sha256 != proof.logical_history_sha256()
            || source.seal().history_entries
                != proof
                    .verified_through()
                    .checked_add(1)
                    .ok_or_else(|| invalid("native publication history length overflow"))?
            || !self.native_range_image_installing(proof.range_id())?
        {
            return Err(invalid(
                "native history publication proof/source/dirty scope changed",
            ));
        }
        // Re-read immutable stored endpoints after actual persisted full audit;
        // no active history keys can be overwritten by the stager.
        for (index, expected) in [
            (0, proof.logical_genesis_sha256()),
            (proof.verified_through(), proof.logical_history_sha256()),
        ] {
            if self
                .read_native_history_entry(proof.range_id(), proof.history_generation(), index)?
                .commitment_sha256
                != expected
            {
                return Err(invalid("native publication staged endpoint changed"));
            }
        }
        contents.require_current(self, context.fence, context.admission, bindings, source)?;
        cancellation.check()?;
        let (actual_global_generation, actual_range_generation) =
            self.native_range_image_generation_pair(proof.range_id())?;
        let controlled = self
            .range_mutations
            .try_controlled_mutation(context.frozen)?;
        let binding = NativeHistoryPublicationBinding { proof };
        let xid = self
            .in_paged_transaction(|paged, xid| {
                cancellation.check()?;
                publish_base(paged, xid, &binding)?;
                // Read our OWN actual page transaction. A no-op callback or
                // an old receipt must never retire native lower bounds.
                let mut own_snapshot = paged.latest_snapshot();
                own_snapshot.xid = xid;
                let encoded = paged
                    .store()
                    .get_as_of(
                        &own_snapshot,
                        &native_key(&format!("install:base:{}", proof.range_id().get())),
                    )
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
                    .ok_or_else(|| invalid("native publisher wrote no completed base"))?;
                if encoded.len() > MAX_NATIVE_RECEIPT_BYTES {
                    return Err(invalid("native published base exceeds byte bound"));
                }
                let base: serde_json::Value = serde_json::from_slice(&encoded)?;
                let matches =
                    |name: &str, expected: serde_json::Value| base.get(name) == Some(&expected);
                if !matches(
                    "cluster_id",
                    serde_json::json!(context.admission.cluster_id()),
                ) || !matches(
                    "bootstrap_id",
                    serde_json::json!(context.admission.bootstrap_id()),
                ) || !matches(
                    "target_node_id",
                    serde_json::json!(context.admission.local_node_id()),
                ) || !matches(
                    "target_member_incarnation",
                    serde_json::json!(context.admission.local_member_incarnation()),
                ) || !matches(
                    "restore_activation",
                    serde_json::json!(context.admission.restore_activation()),
                ) || !matches("purpose", serde_json::json!(context.fence.purpose()))
                    || !matches("capture_range", serde_json::json!(context.fence.range()))
                    || !matches(
                        "intended_activation_range",
                        serde_json::json!(context.fence.intended_activation_range()),
                    )
                    || !matches(
                        "base_members",
                        serde_json::json!(source.cut().capture_members()),
                    )
                    || !matches(
                        "intended_activation_members",
                        serde_json::json!(context.fence.intended_activation_members()),
                    )
                    || !matches(
                        "source_purpose",
                        serde_json::json!(context.fence.source_purpose()),
                    )
                    || !matches(
                        "intent_sha256",
                        serde_json::json!(context.fence.intent_sha256()),
                    )
                    || !matches(
                        "old_config_sha256",
                        serde_json::json!(context.fence.old_config_sha256()),
                    )
                    || !matches(
                        "intended_config_sha256",
                        serde_json::json!(context.fence.intended_config_sha256()),
                    )
                    || !matches("schema_sha256", serde_json::json!(bindings.schema_sha256()))
                    || !matches(
                        "reader_features",
                        serde_json::json!(bindings.reader_features()),
                    )
                    || !matches(
                        "local_namespace_ids",
                        serde_json::json!(bindings.namespace_ids()),
                    )
                    || !matches(
                        "portable_catalog_sha256",
                        serde_json::json!(source.seal().portable_catalog_sha256),
                    )
                    || !matches(
                        "image_root_sha256",
                        serde_json::json!(source.seal().image_root_sha256),
                    )
                    || !matches(
                        "record_count",
                        serde_json::json!(source.seal().record_count),
                    )
                    || !matches(
                        "serialized_record_bytes",
                        serde_json::json!(source.seal().serialized_record_bytes),
                    )
                    || !matches(
                        "global_image_generation",
                        serde_json::json!(actual_global_generation),
                    )
                    || !matches(
                        "range_image_generation",
                        serde_json::json!(actual_range_generation),
                    )
                    || !matches(
                        "reserved_db_upper_bound",
                        serde_json::json!(contents.actual_local_visibility_sequence()),
                    )
                    || !matches(
                        "min_snapshot_db_sequence",
                        serde_json::json!(contents.actual_observed_min_snapshot_sequence()),
                    )
                    || !matches("native_page_xid", serde_json::json!(xid))
                    || !matches("birth_id", serde_json::json!(binding.target_birth_id()))
                    || !matches(
                        "birth_sha256",
                        serde_json::json!(binding.target_birth_sha256()),
                    )
                    || !matches("attempt_id", serde_json::json!(binding.attempt_id()))
                    || !matches("session_id", serde_json::json!(binding.source_session_id()))
                    || !matches("nonce", serde_json::json!(binding.source_nonce()))
                    || !matches("range_index", serde_json::json!(binding.verified_through()))
                    || !matches(
                        "history_verified_through",
                        serde_json::json!(binding.verified_through()),
                    )
                    || !matches(
                        "history_generation",
                        serde_json::json!(binding.history_generation()),
                    )
                    || !matches(
                        "logical_genesis_sha256",
                        serde_json::json!(binding.logical_genesis_sha256()),
                    )
                    || !matches(
                        "logical_history_sha256",
                        serde_json::json!(binding.logical_history_sha256()),
                    )
                    || !matches(
                        "source_native_receipt_sha256",
                        serde_json::json!(binding.source_native_receipt_sha256()),
                    )
                    || paged
                        .store()
                        .get_as_of(
                            &own_snapshot,
                            &native_key(&format!("install:active:{}", proof.range_id().get())),
                        )
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
                        .is_some()
                {
                    return Err(invalid(
                        "native publisher base/active/history transaction binding differs",
                    ));
                }
                // The completed receipt's new local history generation is written
                // by the trusted publisher closure in this same transaction.
                // Retire only exact old range journals already covered by the
                // actual immutable target compatibility observation.
                for key in [
                    format!("pending:{}", proof.range_id().get()),
                    format!("prefix:{}", proof.range_id().get()),
                ] {
                    paged
                        .store()
                        .delete(xid, &native_key(&key))
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                header.published_native_page_xid = Some(xid);
                header.checksum_sha256 = header.checksum()?;
                header.validate()?;
                // Preserve the prior base's publication evidence when a later
                // install starts a new active staging generation.
                let published_key = native_key(&published_stage_key(
                    proof.range_id(),
                    proof.history_generation(),
                ));
                if paged
                    .store()
                    .get_as_of(&own_snapshot, &published_key)
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
                    .is_some()
                {
                    return Err(invalid("native history generation was already published"));
                }
                paged
                    .store()
                    .put(
                        xid,
                        &published_key,
                        &bounded_json(&header, MAX_NATIVE_RECEIPT_BYTES)?,
                    )
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                paged
                    .store()
                    .put(
                        xid,
                        &native_key(&stage_key(proof.range_id())),
                        &bounded_json(&header, MAX_NATIVE_RECEIPT_BYTES)?,
                    )
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                Ok(())
            })?
            .ok_or_else(|| invalid("native history publication has no page store"))?;
        drop(controlled);
        Ok(xid)
    }

    /// Read-only coupling check for an actual opaque installed receipt. This
    /// does not recreate a StageProof or confer mutation/config authority.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn validate_published_native_history_binding(
        &self,
        range: RangeId,
        generation: Uuid,
        index: u64,
        genesis: &str,
        head: &str,
        page_xid: u64,
        birth_id: Uuid,
        birth_sha256: &str,
        attempt: Uuid,
        session: Uuid,
        nonce: Uuid,
        source_receipt_sha256: &str,
    ) -> Result<()> {
        let header: StageHeader = self
            .read_native_range_image_record(&published_stage_key(range, generation))?
            .ok_or_else(|| {
                invalid("installed history has no actual immutable publication record")
            })?;
        header.validate()?;
        if header.source.range_id != range
            || header.generation != generation
            || header.source.range_index != index
            || header.source.logical_genesis_sha256 != genesis
            || header.source.logical_history_sha256 != head
            || header.published_native_page_xid != Some(page_xid)
            || header.target_birth_id != birth_id
            || header.target_birth_sha256 != birth_sha256
            || header.attempt_id != attempt
            || header.source.session_id != session
            || header.source.nonce != nonce
            || header.source.native_receipt_sha256 != source_receipt_sha256
            || !header.cursor.complete()
            || !header.audit_complete
            || !header.target_point_matched
            || !header.remaining_commands.is_empty()
        {
            return Err(invalid(
                "installed receipt and actual history publication differ",
            ));
        }
        Ok(())
    }

    /// Read-only custody commitment; no mutation, floor or serving authority.
    pub(crate) fn native_history_stage_state_commitment(
        &self,
        range: RangeId,
    ) -> Result<Option<String>> {
        let header: Option<StageHeader> = self.read_native_range_image_record(&stage_key(range))?;
        header
            .map(|header| {
                header.validate()?;
                if header.source.range_id != range {
                    return Err(invalid("native history stage key/range differ"));
                }
                Ok(header.checksum_sha256)
            })
            .transpose()
    }
    /// An unrelated preexisting completed/partial stage is not this attempt.
    /// Malformed state remains an error; absence is never a readiness proof.
    pub(crate) fn require_no_native_history_stage_for_attempt(
        &self,
        range: RangeId,
        attempt: Uuid,
    ) -> Result<()> {
        if attempt.is_nil() {
            return Err(invalid(
                "native history absence check requires an actual attempt",
            ));
        }
        let header: Option<StageHeader> = self.read_native_range_image_record(&stage_key(range))?;
        if let Some(header) = header {
            header.validate()?;
            if header.source.range_id != range || header.attempt_id == attempt {
                return Err(invalid(
                    "actual native history stage exists for this attempt or range differs",
                ));
            }
        }
        Ok(())
    }

    fn read_history_stage(&self, range: RangeId) -> Result<StageHeader> {
        let header: StageHeader = self
            .read_native_range_image_record(&stage_key(range))?
            .ok_or_else(|| invalid("native history stage is absent"))?;
        header.validate()?;
        Ok(header)
    }
    fn validate_history_stage(
        &self,
        stage: &NativeHistoryStage,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<StageHeader> {
        context.require_current_under_journal(self, journal)?;
        let header = self.read_history_stage(stage.range_id)?;
        if stage.database_incarnation != self.incarnation
            || stage.range_id != context.fence.range().id
            || stage.stage_id != header.stage_id
            || stage.generation != header.generation
            || header.target_birth_id != context.admission.birth_id()
            || header.target_birth_sha256 != context.admission.birth_sha256()
            || header.attempt_id != context.fence.attempt_id()
            || header.intent_sha256 != context.fence.intent_sha256()
            || header.native_irreversible_sha256 != context.safety.irreversible_records_sha256()
            || header.catalog_sha256 != context.catalog.range_log_sha256()
            || header.source != HistorySourceScope::actual(source)?
            || header.published_native_page_xid.is_some()
        {
            return Err(invalid(
                "native history stage/attempt/source/target binding changed",
            ));
        }
        Ok(header)
    }
    pub(crate) fn begin_native_history_stage(
        &self,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
    ) -> Result<NativeHistoryStage> {
        let journal = self.try_native_image_journal()?;
        context.require_current_under_journal(self, &journal)?;
        let scope = HistorySourceScope::actual(source)?;
        let must_match_planned_target = context.fence.purpose()
            == ImageInstallPurpose::RelocationLearner
            || matches!(
                source.cut().purpose(),
                crate::distribution_range_image::RangeImageSourcePurposeV1::CurrentVoterRepair
            );
        if !must_match_planned_target {
            let exact_handoff_old_voter = context.fence.purpose()
                == ImageInstallPurpose::CurrentVoterRepair
                && matches!(context.role, NativeImageInstallRole::CommittedHandoff(_))
                && context
                    .role
                    .role()
                    .current_range()
                    .replicas
                    .iter()
                    .any(|replica| {
                        replica.node_id == *context.admission.local_node_id()
                            && replica.role == RangeReplicaRole::Voter
                    });
            if !exact_handoff_old_voter {
                return Err(invalid(
                    "history destination is not the exact scoped handoff voter repair",
                ));
            }
        }
        if scope.range_id != context.fence.range().id
            || scope.range_index != context.fence.source_range_index()
            || source.bootstrap_id() != context.admission.bootstrap_id()
            || source.cut().cluster_id() != context.admission.cluster_id()
            || (must_match_planned_target
                && (source.cut().target_id() != context.admission.local_node_id()
                    || source.cut().target_incarnation()
                        != context.admission.local_member_incarnation()))
            || source.native_receipt_sha256() != context.fence.source_native_receipt_sha256()
            || context.catalog.last_index() > scope.range_index
            || context
                .safety
                .maximum_irreversible_index()
                .is_some_and(|index| index > scope.range_index)
            || context
                .safety
                .maximum_pending_index()
                .is_some_and(|index| index > scope.range_index)
        {
            return Err(invalid(
                "history source cannot cover the actual target scope/floor/tail",
            ));
        }
        scope
            .range_index
            .checked_add(1)
            .ok_or_else(|| invalid("history total overflow"))?;
        let target_point = context
            .safety
            .history_point()
            .map(|point| {
                if point.cluster_id() != context.admission.cluster_id()
                    || point.bootstrap_id() != context.admission.bootstrap_id()
                    || point.range_id() != scope.range_id
                    || point.logical_genesis_sha256() != scope.logical_genesis_sha256
                    || point.range_index() > scope.range_index
                    || (point.range_index() == scope.range_index
                        && point.logical_history_sha256() != scope.logical_history_sha256)
                {
                    return Err(invalid(
                        "actual target native history is incompatible with source",
                    ));
                }
                Ok((
                    point.range_index(),
                    point.logical_history_sha256().to_string(),
                ))
            })
            .transpose()?;
        let mut commands = context.catalog.irrevocable_commands().clone();
        if let (Some(index), Some(checksum)) = (
            context.safety.pending_index(),
            context.safety.pending_command_checksum_sha256(),
        ) {
            if commands
                .insert(index, checksum.to_string())
                .is_some_and(|previous| previous != checksum)
            {
                return Err(invalid(
                    "native pending contradicts actual target committed catalog history",
                ));
            }
        }
        let mut header = StageHeader {
            format_version: STAGE_FORMAT,
            stage_id: Uuid::now_v7(),
            generation: Uuid::now_v7(),
            target_birth_id: context.admission.birth_id(),
            target_birth_sha256: context.admission.birth_sha256().to_string(),
            attempt_id: context.fence.attempt_id(),
            intent_sha256: context.fence.intent_sha256().to_string(),
            native_irreversible_sha256: context.safety.irreversible_records_sha256().to_string(),
            catalog_sha256: context.catalog.range_log_sha256().to_string(),
            source: scope,
            target_point_matched: target_point.is_none(),
            target_point,
            remaining_commands: commands,
            cursor: Default::default(),
            verified_entries: 0,
            current_head: None,
            wire_chain: initial_chain(),
            last_previous_cursor: None,
            last_step_sha256: None,
            audit_next_index: 0,
            audit_head: None,
            audit_complete: false,
            published_native_page_xid: None,
            checksum_sha256: String::new(),
        };
        header.checksum_sha256 = header.checksum()?;
        header.validate()?;
        if let Some(previous) =
            self.read_native_range_image_record::<StageHeader>(&stage_key(header.source.range_id))?
        {
            previous.validate()?;
            if previous.target_birth_id != header.target_birth_id
                || previous.target_birth_sha256 != header.target_birth_sha256
                || previous.source.logical_genesis_sha256 != header.source.logical_genesis_sha256
                || previous.source.range_index > header.source.range_index
                || (previous.source.range_index == header.source.range_index
                    && previous.source.logical_history_sha256
                        != header.source.logical_history_sha256)
            {
                return Err(invalid(
                    "new history attempt regresses/forks known native source history",
                ));
            }
            if previous.attempt_id == header.attempt_id {
                if previous.intent_sha256 != header.intent_sha256
                    || previous.source != header.source
                    || previous.catalog_sha256 != header.catalog_sha256
                    || previous.native_irreversible_sha256 != header.native_irreversible_sha256
                {
                    return Err(invalid(
                        "existing history attempt changed its exact source/target obligations",
                    ));
                }
                return Ok(NativeHistoryStage {
                    database_incarnation: self.incarnation.clone(),
                    stage_id: previous.stage_id,
                    generation: previous.generation,
                    range_id: previous.source.range_id,
                });
            }
            if previous.source != header.source {
                let point=context.safety.history_point().filter(|point|point.range_index()>=previous.source.range_index)
                    .ok_or_else(||invalid("alternate source requires an explicit current-quorum substitution proof"))?;
                let retained = self.read_native_history_entry(
                    point.range_id(),
                    point.history_generation(),
                    previous.source.range_index,
                )?;
                if !previous.audit_complete
                    || retained.commitment_sha256 != previous.source.logical_history_sha256
                {
                    return Err(invalid(
                        "prior observed source history is not covered by actual native history",
                    ));
                }
            }
        }
        let controlled = self
            .range_mutations
            .try_controlled_mutation(context.frozen)?;
        self.write_native_range_image_record(&stage_key(header.source.range_id), &header)?;
        drop(controlled);
        Ok(NativeHistoryStage {
            database_incarnation: self.incarnation.clone(),
            stage_id: header.stage_id,
            generation: header.generation,
            range_id: header.source.range_id,
        })
    }
    pub(crate) fn append_native_history_stage(
        &self,
        stage: &NativeHistoryStage,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        target_stage_id: Uuid,
        step: &RangeImageHistoryStepV1,
        cancellation: &CancellationToken,
    ) -> Result<NativeHistoryStageProgress> {
        cancellation.check()?;
        let journal = self.try_native_image_journal()?;
        let mut header = self.validate_history_stage(stage, context, source, &journal)?;
        if target_stage_id != stage.stage_id {
            return Err(invalid("late native history stage attempt"));
        }
        if step.history.entries.is_empty()
            || step.history.entries.len() > history::MAX_HISTORY_STEP_ENTRIES
        {
            return Err(invalid("native history wire entry bound exceeded"));
        }
        bounded_json(step, MAX_WIRE)?;
        let mut unsigned = step.clone();
        unsigned.chain_sha256.clear();
        let digest = hex::encode(sha2::Sha256::digest(bounded_json(&unsigned, MAX_WIRE)?));
        if step.format_version != 1
            || step.chain_sha256 != digest
            || step.session_id != header.source.session_id
            || step.nonce != header.source.nonce
        {
            return Err(invalid("native history wire hash/session mismatch"));
        }
        if header.last_previous_cursor.as_ref() == Some(&step.previous_cursor)
            && header.last_step_sha256.as_deref() == Some(step.chain_sha256.as_str())
        {
            return Ok(NativeHistoryStageProgress {
                verified_entries: header.verified_entries,
                cursor: header.cursor,
                audit_complete: header.audit_complete,
            });
        }
        let history = &step.history;
        let total = header
            .source
            .range_index
            .checked_add(1)
            .ok_or_else(|| invalid("history total overflow"))?;
        if header.cursor.complete()
            || step.previous_cursor != header.cursor
            || step.previous_chain_sha256 != header.wire_chain
            || history.range_id != header.source.range_id
            || history.source_range_index != header.source.range_index
            || history.source_native_receipt_sha256 != header.source.native_receipt_sha256
            || history.logical_genesis_sha256 != header.source.logical_genesis_sha256
            || history.logical_history_sha256 != header.source.logical_history_sha256
            || history.first_index != header.verified_entries
            || history.entries.is_empty()
            || history.entries.len() > history::MAX_HISTORY_STEP_ENTRIES
        {
            return Err(invalid("native history stage cursor/source/head changed"));
        }
        for entry in &history.entries {
            cancellation.check()?;
            entry.validate_shape()?;
            if entry.range_id != header.source.range_id || entry.index != header.verified_entries {
                return Err(invalid("missing/reordered native history entry"));
            }
            if entry.index == 0 {
                if entry.outcome != history::NativeLogicalHistoryOutcomeV1::Genesis
                    || entry.commitment_sha256 != header.source.logical_genesis_sha256
                {
                    return Err(invalid("native stage genesis differs"));
                }
            } else if header.current_head.as_deref() != Some(entry.previous_sha256.as_str()) {
                return Err(invalid("native history stage link differs"));
            }
            if let Some((index, expected)) = &header.target_point {
                if *index == entry.index {
                    if expected != &entry.commitment_sha256 {
                        return Err(invalid("source forks actual target native history"));
                    }
                    header.target_point_matched = true;
                }
            }
            if let Some(expected) = header.remaining_commands.get(&entry.index) {
                if entry.outcome
                    != (history::NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256: expected.clone(),
                    })
                {
                    return Err(invalid(
                        "source history contradicts actual CQ/Applied/native-pending command",
                    ));
                }
                header.remaining_commands.remove(&entry.index);
            }
            header.current_head = Some(entry.commitment_sha256.clone());
            header.verified_entries = header
                .verified_entries
                .checked_add(1)
                .ok_or_else(|| invalid("native history count overflow"))?;
        }
        let complete = header.verified_entries == total;
        if header.verified_entries > total
            || history.next_index
                != if complete {
                    None
                } else {
                    Some(header.verified_entries)
                }
            || step.next_cursor.next_index() != header.verified_entries
            || step.next_cursor.complete() != complete
            || step.next_cursor.step()
                != header
                    .cursor
                    .step()
                    .checked_add(1)
                    .ok_or_else(|| invalid("native history step overflow"))?
            || (complete
                && (header.current_head.as_deref()
                    != Some(header.source.logical_history_sha256.as_str())
                    || !header.target_point_matched
                    || !header.remaining_commands.is_empty()))
        {
            return Err(invalid(
                "native history completion did not verify full source/target obligations",
            ));
        }
        header.last_previous_cursor = Some(step.previous_cursor.clone());
        header.last_step_sha256 = Some(step.chain_sha256.clone());
        header.cursor = step.next_cursor.clone();
        header.wire_chain = step.chain_sha256.clone();
        header.checksum_sha256 = header.checksum()?;
        header.validate()?;
        let controlled = self
            .range_mutations
            .try_controlled_mutation(context.frozen)?;
        self.in_paged_transaction(|paged, xid| {
            for entry in &history.entries {
                history::put_immutable_history_entry(paged, xid, header.generation, entry)?;
            }
            let bytes = bounded_json(&header, MAX_NATIVE_RECEIPT_BYTES)?;
            paged
                .store()
                .put(xid, &native_key(&stage_key(header.source.range_id)), &bytes)
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native staged history has no page store"))?;
        drop(controlled);
        Ok(NativeHistoryStageProgress {
            verified_entries: header.verified_entries,
            cursor: header.cursor,
            audit_complete: header.audit_complete,
        })
    }
    pub(crate) fn audit_native_history_stage_step(
        &self,
        stage: &NativeHistoryStage,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        max_entries: usize,
        cancellation: &CancellationToken,
    ) -> Result<NativeHistoryStageProgress> {
        if !(1..=history::MAX_HISTORY_STEP_ENTRIES).contains(&max_entries) {
            return Err(invalid("invalid native history audit bound"));
        }
        let journal = self.try_native_image_journal()?;
        let mut header = self.validate_history_stage(stage, context, source, &journal)?;
        if !header.cursor.complete() {
            return Err(invalid("cannot audit partial native history stream"));
        }
        if !header.audit_complete {
            for _ in 0..max_entries {
                cancellation.check()?;
                let entry = self.read_native_history_entry(
                    header.source.range_id,
                    header.generation,
                    header.audit_next_index,
                )?;
                if entry.index == 0 {
                    if entry.commitment_sha256 != header.source.logical_genesis_sha256
                        || entry.outcome != history::NativeLogicalHistoryOutcomeV1::Genesis
                    {
                        return Err(invalid("stored stage genesis differs"));
                    }
                } else if header.audit_head.as_deref() != Some(entry.previous_sha256.as_str()) {
                    return Err(invalid("stored stage history is incomplete/forked"));
                }
                header.audit_head = Some(entry.commitment_sha256);
                header.audit_next_index = header
                    .audit_next_index
                    .checked_add(1)
                    .ok_or_else(|| invalid("history audit overflow"))?;
                if entry.index == header.source.range_index {
                    header.audit_complete = true;
                    break;
                }
            }
            if header.audit_complete
                && header.audit_head.as_deref()
                    != Some(header.source.logical_history_sha256.as_str())
            {
                return Err(invalid("stored full native spine head mismatch"));
            }
            header.checksum_sha256 = header.checksum()?;
            header.validate()?;
            let controlled = self
                .range_mutations
                .try_controlled_mutation(context.frozen)?;
            self.write_native_range_image_record(&stage_key(header.source.range_id), &header)?;
            drop(controlled);
        }
        Ok(NativeHistoryStageProgress {
            verified_entries: header.verified_entries,
            cursor: header.cursor,
            audit_complete: header.audit_complete,
        })
    }
    pub(crate) fn finish_native_history_stage(
        &self,
        stage: &NativeHistoryStage,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
    ) -> Result<StagedNativeHistoryProof> {
        let journal = self.try_native_image_journal()?;
        let header = self.validate_history_stage(stage, context, source, &journal)?;
        if !header.cursor.complete()
            || !header.audit_complete
            || !header.target_point_matched
            || !header.remaining_commands.is_empty()
        {
            return Err(invalid(
                "actual full native history compatibility/audit is incomplete",
            ));
        }
        let first = self.read_native_history_entry(header.source.range_id, header.generation, 0)?;
        let last = self.read_native_history_entry(
            header.source.range_id,
            header.generation,
            header.source.range_index,
        )?;
        if first.commitment_sha256 != header.source.logical_genesis_sha256
            || last.commitment_sha256 != header.source.logical_history_sha256
        {
            return Err(invalid("stored verified native history endpoints changed"));
        }
        Ok(StagedNativeHistoryProof {
            database_incarnation: self.incarnation.clone(),
            stage_id: header.stage_id,
            header_sha256: header.checksum_sha256.clone(),
            header,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_range_image_install::tests::Fixture;

    /// This fixture uses actual fresh native schema/genesis and, when requested,
    /// ordinary RF1 origin transactions before the permanent handoff promise.
    /// Staging never clears or installs data rows and grants no membership.
    fn exercise_stage(
        command_count: usize,
        tamper: bool,
        tear_stored_entry: bool,
        conflict_catalog: bool,
    ) {
        let mut fixture =
            Fixture::new_with_ranges_and_before_handoff(2, |db, topology, spec, _| {
                let selected = topology.ranges.values().next().unwrap().id;
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    selected,
                    command_count,
                );
            });
        let target = fixture.target();
        let range_id = fixture.range().id;
        let attempt = Uuid::now_v7();
        let admission = target
            .read()
            .native_recovery_admission(
                &fixture.topology.cluster_id,
                fixture.topology.range_image_bootstrap_id.unwrap(),
                &fixture.target_spec.node_id,
                fixture.target_spec.member_incarnation,
            )
            .unwrap();
        let frozen = target
            .read()
            .range_mutation_gate()
            .try_freeze(range_id, attempt)
            .unwrap();
        let safety = target
            .read()
            .native_range_install_safety(&admission, range_id, &frozen)
            .unwrap();
        assert_eq!(
            safety.maximum_irreversible_index(),
            None,
            "fresh learner has no native floor"
        );
        let mut store = fixture.store();
        if conflict_catalog {
            assert_eq!(command_count, 1);
            let key = (0..)
                .map(|i| format!("stage-conflicting-catalog-{i}"))
                .find(|key| fixture.topology.range_for_key("items", key).unwrap().id == range_id)
                .unwrap();
            let command = crate::distribution_range_consensus::test_catalog_command(
                fixture.topology.cluster_id.clone(),
                fixture.range(),
                1,
                &crate::CommitAdmissionIntent {
                    transaction_id: 90001,
                    mutations: vec![crate::CommitAdmissionMutation {
                        collection: "items".into(),
                        record_id: key.clone(),
                        record: Some(crate::Record::new(key)),
                    }],
                },
            )
            .unwrap();
            // Actual local catalog transitions, not a hand-edited JSON log or
            // caller-filtered obligation list. This intentionally incompatible
            // replica fixture is not a demonstrated distributed fork schedule.
            store.prepare(command.clone()).unwrap();
            store.commit(&command).unwrap();
            store.certify(&command).unwrap();
        }
        let role = fixture.role();
        let install_role = NativeImageInstallRole::CommittedHandoff(&role);
        let fence = {
            let auth = fixture
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            store
                .begin_native_image_install(
                    &target.read(),
                    &admission,
                    &safety,
                    &auth,
                    &install_role,
                    &frozen,
                    ImageInstallPurpose::RelocationLearner,
                    attempt,
                )
                .unwrap()
        };
        let catalog = store
            .image_install_catalog_history_obligations(range_id)
            .unwrap();
        let context = NativeHistoryTargetContext::new(
            &target.read(),
            &store,
            &fence,
            &admission,
            &safety,
            &catalog,
            &install_role,
            &frozen,
        )
        .unwrap();
        let stage = {
            let auth = fixture
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            let stage = target
                .read()
                .begin_native_history_stage(&context, &auth)
                .unwrap();
            let retry = target
                .read()
                .begin_native_history_stage(&context, &auth)
                .unwrap();
            assert_eq!(
                retry.stage_id(),
                stage.stage_id(),
                "same attempt must not discard progress"
            );
            assert_eq!(retry.generation(), stage.generation());
            assert!(target
                .read()
                .finish_native_history_stage(&stage, &context, &auth)
                .is_err());
            assert_ne!(
                stage.generation(),
                auth.lineage().history_generation(),
                "target generation remains local"
            );
            stage
        };
        loop {
            let step = {
                let capture = fixture.capture.as_mut().unwrap();
                let cursor = capture.history_cursor().clone();
                capture
                    .export_history_step(&cursor, 1, &CancellationToken::uncancelable())
                    .unwrap()
            };
            let auth = fixture
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            if tamper {
                let mut corrupt = step.clone();
                corrupt.history.entries[0].commitment_sha256 = "f".repeat(64);
                assert!(target
                    .read()
                    .append_native_history_stage(
                        &stage,
                        &context,
                        &auth,
                        stage.stage_id(),
                        &corrupt,
                        &CancellationToken::uncancelable()
                    )
                    .is_err());
                assert!(target
                    .read()
                    .append_native_history_stage(
                        &stage,
                        &context,
                        &auth,
                        Uuid::now_v7(),
                        &step,
                        &CancellationToken::uncancelable()
                    )
                    .is_err());
            }
            if conflict_catalog && step.history.first_index == 1 {
                let error = target
                    .read()
                    .append_native_history_stage(
                        &stage,
                        &context,
                        &auth,
                        stage.stage_id(),
                        &step,
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("contradicts actual CQ/Applied/native-pending"));
                assert!(target
                    .read()
                    .finish_native_history_stage(&stage, &context, &auth)
                    .is_err());
                assert!(target
                    .read()
                    .audit_native_history_stage_step(
                        &stage,
                        &context,
                        &auth,
                        1,
                        &CancellationToken::uncancelable()
                    )
                    .is_err());
                return;
            }
            let progress = target
                .read()
                .append_native_history_stage(
                    &stage,
                    &context,
                    &auth,
                    stage.stage_id(),
                    &step,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            let retry = target
                .read()
                .append_native_history_stage(
                    &stage,
                    &context,
                    &auth,
                    stage.stage_id(),
                    &step,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            assert_eq!(
                progress, retry,
                "identical last-frame retry must be idempotent"
            );
            if progress.cursor.complete() {
                break;
            }
        }
        let auth = fixture
            .capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        assert!(
            target
                .read()
                .finish_native_history_stage(&stage, &context, &auth)
                .is_err(),
            "wire checks do not replace actual stored-spine audit"
        );
        if tear_stored_entry {
            // Deliberately remove an inactive staged page key, never active data
            // or a command catalog: a stored-spine hole must fail closed.
            let db = target.read();
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(&frozen)
                .unwrap();
            db.in_paged_transaction(|paged, xid| {
                paged
                    .store()
                    .delete(
                        xid,
                        &native_key(&history::history_key(range_id, stage.generation(), 0)),
                    )
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                Ok(())
            })
            .unwrap()
            .unwrap();
            drop(controlled);
            assert!(db
                .audit_native_history_stage_step(
                    &stage,
                    &context,
                    &auth,
                    1,
                    &CancellationToken::uncancelable()
                )
                .is_err());
            assert!(db
                .finish_native_history_stage(&stage, &context, &auth)
                .is_err());
            return;
        }
        loop {
            let progress = target
                .read()
                .audit_native_history_stage_step(
                    &stage,
                    &context,
                    &auth,
                    1,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            if progress.audit_complete {
                break;
            }
        }
        let proof = target
            .read()
            .finish_native_history_stage(&stage, &context, &auth)
            .unwrap();
        assert_eq!(proof.verified_through(), command_count as u64);
        assert_eq!(
            proof.logical_genesis_sha256(),
            auth.lineage().logical_genesis_sha256()
        );
        assert_eq!(
            proof.logical_history_sha256(),
            auth.lineage().logical_history_sha256()
        );
        proof
            .require_current(&target.read(), &context, &auth)
            .unwrap();
        for key in (0..)
            .map(|i| format!("stage-actual-origin-{i}"))
            .filter(|key| fixture.topology.range_for_key("items", key).unwrap().id == range_id)
            .take(command_count)
        {
            assert!(
                fixture.source.read().get("items", &key).unwrap().is_some(),
                "fixture genuinely committed source payload"
            );
            assert!(
                target.read().get("items", &key).unwrap().is_none(),
                "history staging copies no data rows"
            );
        }
        assert!(
            target
                .read()
                .current_native_range_source_lineage(range_id)
                .is_err(),
            "staged history is not an installed/activated source anchor"
        );
    }
    #[test]
    fn native_history_stage_actual_empty_spine_requires_stored_audit_and_is_idempotent() {
        exercise_stage(0, false, false, false)
    }
    #[test]
    fn native_history_stage_actual_origin_spine_rejects_corrupt_and_late_frames() {
        exercise_stage(1, true, false, false)
    }
    #[test]
    fn native_history_stage_actual_retained_catalog_command_conflict_refuses_before_data() {
        exercise_stage(1, false, false, true)
    }
    #[test]
    fn native_history_stage_missing_stored_entry_refuses_proof() {
        exercise_stage(1, false, true, false)
    }
}

/// Scoped image-only row capability from an actual fully audited history and
/// target/source fence. A mutation permit or serialized payload cannot mint it.
pub(crate) struct NativeImageRowScope<'a> {
    context: &'a NativeHistoryTargetContext<'a>,
    proof: &'a StagedNativeHistoryProof,
    source: &'a TentativeNativeRangeImageSourceAuthorization<'a>,
    marker_sha256: String,
}
#[derive(Debug)]
pub(crate) struct NativeImageTransactionGrant {
    database_incarnation: String,
    transaction_id: u64,
    mutations_sha256: String,
    range: crate::RangeDescriptor,
    attempt: Uuid,
    marker_sha256: String,
    deadline: std::time::Instant,
    claim: crate::distribution_range_mutation::RangeMutationClaim,
}
impl NativeImageTransactionGrant {
    pub(in crate::db) fn require_current(
        &self,
        database: &BicDb,
        transaction: &Transaction,
    ) -> Result<()> {
        database.check_recovery_state()?;
        if self.database_incarnation != database.incarnation_id()
            || self.transaction_id != transaction.id().0
            || std::time::Instant::now() >= self.deadline
            || super::super::native_wal_apply::actual_transaction_mutations_sha256(transaction)?
                != self.mutations_sha256
            || database.native_image_row_marker_commitment(self.range.id, self.attempt)?
                != self.marker_sha256
        {
            return Err(invalid(
                "native image transaction capability changed/expired",
            ));
        }
        database.range_mutations.validate_claim(&self.claim)?;
        for mutation in &transaction.writes {
            if !self
                .range
                .contains_token(crate::distribution::distribution_key_token(
                    &mutation.collection,
                    &mutation.record_id,
                ))
            {
                return Err(invalid("native image mutation left its authorized range"));
            }
        }
        database.require_installed_native_writer_database_for_range(&self.range)
    }
}
impl NativeImageRowScope<'_> {
    pub(crate) fn range(&self) -> &crate::RangeDescriptor {
        self.context.fence.range()
    }
    pub(in crate::db) fn authorize_transaction(
        &self,
        database: &BicDb,
        transaction: &mut Transaction,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        self.source.require_current()?;
        self.context.role.role().require_live_request()?;
        database
            .range_mutations
            .validate(self.range().id, authority)?;
        if database
            .native_image_row_marker_commitment(self.range().id, self.context.fence.attempt_id())?
            != self.marker_sha256
            || self.proof.attempt_id() != self.context.fence.attempt_id()
        {
            return Err(invalid("native image scope marker/attempt changed"));
        }
        transaction.native_image_row_grant = Some(NativeImageTransactionGrant {
            database_incarnation: database.incarnation_id().into(),
            transaction_id: transaction.id().0,
            mutations_sha256: super::super::native_wal_apply::actual_transaction_mutations_sha256(
                transaction,
            )?,
            range: self.range().clone(),
            attempt: self.context.fence.attempt_id(),
            marker_sha256: self.marker_sha256.clone(),
            deadline: self.context.role.role().deadline(),
            claim: authority.claim(),
        });
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn native_image_row_scope<'a>(
        &self,
        context: &'a NativeHistoryTargetContext<'a>,
        proof: &'a StagedNativeHistoryProof,
        source: &'a TentativeNativeRangeImageSourceAuthorization<'a>,
    ) -> Result<NativeImageRowScope<'a>> {
        proof.require_current(self, context, source)?;
        self.require_installed_native_writer_database_for_range(context.fence.range())?;
        let marker_sha256 = self.native_image_row_marker_commitment(
            context.fence.range().id,
            context.fence.attempt_id(),
        )?;
        Ok(NativeImageRowScope {
            context,
            proof,
            source,
            marker_sha256,
        })
    }
}
