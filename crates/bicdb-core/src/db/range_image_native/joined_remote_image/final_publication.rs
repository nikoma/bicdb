//! Original Joined final physical publication. Separate protected provenance;
//! no installed native base/floor, catalogue adoption, writer or Serving.
use super::verification::VerifiedJoinedTargetContents;
use super::*;
use crate::distribution_range_image::{LocalRangeImageSealV1, NativeRemoteImageManifestV1};

pub(crate) use crate::db::NativeJoinedRemoteFinalImagePointer;
fn current(attempt: Uuid) -> String {
    format!("joined-remote-image/{attempt}/final/current")
}
fn archive(attempt: Uuid, xid: u64) -> String {
    format!("joined-remote-image/{attempt}/final/archive/{xid:020}")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    manifest: NativeRemoteImageManifestV1,
    source_seal: LocalRangeImageSealV1,
    attempt: Uuid,
    range: RangeId,
    database_open: String,
    target_birth: crate::distribution::NativeStoreBirthV1,
    store_open: Uuid,
    catalogue_id: Uuid,
    namespaces: Vec<(String, Uuid)>,
    intent_sha256: String,
    proposal: NativeJoinedRemoteProposalPointer,
    history_generation: Uuid,
    history_header_sha256: String,
    history_header: HistoryHeader,
    rows_native_xid: u64,
    rows_header_sha256: String,
    rows_header: rows::ArchivedJoinedRows,
    last_applied_target_b: Option<u64>,
    verification_generation: Uuid,
    target_reserved_b: u64,
    target_min_scan_b: u64,
    image_generations: (Uuid, Uuid),
    index_definitions: Vec<IndexDefinition>,
    original_writer_policy_sha256: String,
    metadata_store: Uuid,
    metadata_context: Uuid,
    metadata_term: u64,
    metadata_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl Record {
    fn checksum(&self) -> Result<String> {
        let mut r = self.clone();
        r.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("joined-remote-final-image-v3", r),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn pointer(&self) -> NativeJoinedRemoteFinalImagePointer {
        NativeJoinedRemoteFinalImagePointer {
            attempt_id: self.attempt,
            native_page_xid: self.native_page_xid,
            checksum_sha256: self.checksum_sha256.clone(),
        }
    }
    fn validate(&self) -> Result<()> {
        self.manifest.require_shape()?;
        self.proposal.validate()?;
        self.pointer().validate()?;
        self.history_header.validate()?;
        if self.version != 3
            || self.attempt.is_nil()
            || self.attempt != self.manifest.target_attempt
            || self.range != self.manifest.capture_range.id
            || self.database_open.is_empty()
            || self.target_birth != self.manifest.target_birth
            || self.store_open.is_nil()
            || self.catalogue_id.is_nil()
            || self.proposal.attempt_id != self.attempt
            || self.history_generation.is_nil()
            || self.verification_generation.is_nil()
            || self.rows_native_xid == 0
            || self.native_page_xid == 0
            || self.target_min_scan_b < self.target_reserved_b
            || self
                .last_applied_target_b
                .is_some_and(|b| b > self.target_reserved_b)
            || self.image_generations.0.is_nil()
            || self.image_generations.1.is_nil()
            || self.original_writer_policy_sha256.len() != 64
            || !self
                .original_writer_policy_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self.metadata_store.is_nil()
            || self.metadata_context.is_nil()
            || self.metadata_term == 0
            || self
                .index_definitions
                .windows(2)
                .any(|p| p[0].name >= p[1].name)
            || self.namespaces.windows(2).any(|p| p[0].0 >= p[1].0)
            || self
                .namespaces
                .iter()
                .any(|(n, id)| n.is_empty() || id.is_nil())
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged Joined protected final image"));
        }
        Ok(())
    }
    fn require_current(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        self.validate()?;
        self.require_published_history(db)?;
        context.require_current(db)?;
        let preflight = context.point.preflight();
        let history = db.read_joined_remote_history(self.attempt)?;
        if self.manifest.checksum_sha256 != preflight.source().manifest().checksum_sha256
            || bounded_json(&self.manifest, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(preflight.source().manifest(), MAX_NATIVE_RECEIPT_BYTES)?
            || self.source_seal != verification::finished_seal(context)?
            || self.attempt != context.scope.attempt_id()
            || self.range != context.scope.range_id()
            || self.database_open != db.incarnation_id()
            || self.target_birth
                != crate::distribution::NativeStoreBirthV1::from_actual_native(
                    context.scope.native(),
                )?
            || self.store_open != context.store.native_writer_store_open_id()
            || self.catalogue_id != preflight.selected().catalogue().catalogue_id()
            || self.namespaces != preflight.bindings().namespace_ids()
            || self.intent_sha256 != context.fence.intent_sha256()?
            || self.proposal != *context.fence.protected_proposal()
            || self.history_generation != history.generation
            || self.history_header_sha256 != history.checksum_sha256
            || !history.audited
            || self.image_generations != db.native_range_image_generation_pair(self.range)?
            || bounded_json(&self.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&verification::actual_indexes(db)?, MAX_NATIVE_RECEIPT_BYTES)?
            || self.original_writer_policy_sha256 != context.scope.original_writer_sha256()
            || self.metadata_store != context.point.request().member().store_id()
            || context.point.request().own().term() < self.metadata_term
            || context.point.request().own().read_index() < self.metadata_index
            || db.replication_shippable_seq() < self.target_reserved_b
            || db.current_visibility_watermark() < self.target_reserved_b
        {
            return Err(invalid(
                "Joined final image original source/target/protected point changed",
            ));
        }
        db.require_joined_completed_rows_point(
            self.attempt,
            self.rows_native_xid,
            &self.rows_header_sha256,
        )?;
        context.require_current(db)
    }
    /// Immutable publication constraints. This deliberately does not consult
    /// mutable current stage/row headers, current namespace/schema/image
    /// generations, original DB-open or any request/Serving grant.
    fn require_published_history(&self, db: &BicDb) -> Result<()> {
        self.validate()?;
        verification::require_seal_manifest(&self.source_seal, &self.manifest)?;
        let h = &self.history_header;
        if h.attempt != self.attempt
            || h.range != self.range
            || h.birth != self.target_birth.birth_id()
            || h.generation != self.history_generation
            || h.checksum_sha256 != self.history_header_sha256
            || h.manifest_sha256 != self.manifest.checksum_sha256
            || h.intent_sha256 != self.intent_sha256
            || h.proposal != self.proposal
            || h.range_index != self.manifest.range_index
            || h.h0 != self.manifest.h0
            || h.h != self.manifest.h
            || h.source_receipt != self.manifest.source_native_receipt
            || h.session != self.manifest.handoff_session
            || h.nonce != self.manifest.nonce
            || !h.audited
            || !h.cursor.complete()
            || h.verified != self.source_seal.history_entries
            || h.cursor.step() != self.source_seal.history_steps
            || h.wire_chain != self.source_seal.history_wire_chain_sha256
            || h.head.as_deref() != Some(self.manifest.h.as_str())
            || h.audit_head.as_deref() != Some(self.manifest.h.as_str())
            || db
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Joined published history page store absent"))?
                .store()
                .transactions()
                .status(h.native_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "Joined immutable published history differs from final binding",
            ));
        }
        for (index, hash) in [
            (0, self.manifest.h0.as_str()),
            (self.manifest.range_index, self.manifest.h.as_str()),
        ] {
            let entry = db.read_native_history_entry(self.range, self.history_generation, index)?;
            entry.validate_shape()?;
            if entry.range_id != self.range
                || entry.index != index
                || entry.commitment_sha256 != hash
                || (index == 0 && entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis)
            {
                return Err(invalid(
                    "Joined immutable published history endpoint changed",
                ));
            }
        }
        self.rows_header.require_historical(
            db,
            &self.manifest,
            &self.source_seal,
            &self.intent_sha256,
            self.history_generation,
            self.rows_native_xid,
            &self.rows_header_sha256,
            self.last_applied_target_b,
        )
    }
    fn require_verified(&self, proof: &VerifiedJoinedTargetContents) -> Result<()> {
        if self.verification_generation != proof.generation
            || self.target_reserved_b != proof.target_reserved_b
            || self.target_min_scan_b != proof.min_snapshot_b
            || self.image_generations != proof.image_generations
            || self.source_seal != proof.seal
            || self.history_generation != proof.history_generation
            || self.history_header_sha256 != proof.history_header_sha256
            || self.rows_native_xid != proof.rows.native_xid
            || self.rows_header_sha256 != proof.rows.header_sha256
            || self.last_applied_target_b != proof.rows.last_actual_target_b
            || bounded_json(&self.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&proof.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
        {
            return Err(invalid(
                "Joined final publication changed original independent scan",
            ));
        }
        Ok(())
    }
}
/// Produced only from actual committed CURRENT/archive readback while the
/// original host/A remains owned. No serde or decoded-pointer constructor.
pub(crate) struct NativeJoinedRemoteFinalImageReceipt {
    record: Record,
}
impl NativeJoinedRemoteFinalImageReceipt {
    /// Complete physical proof for an ORIGINAL retained catalogue point. This
    /// checks real native publication and actual current identities, without
    /// falsely calling uncertain whole-catalogue durability "current".
    pub(crate) fn require_catalogue_current(
        &self,
        db: &BicDb,
        scope: &NativeJoinedRemoteMutationScope<'_>,
        point: &NativeJoinedRemoteImagePoint<'_>,
        physical: &NativeInstalledRangeImageBase,
    ) -> Result<()> {
        point.require_live()?;
        scope.require_database(db)?;
        physical.require_current(db, scope.native())?;
        let actual = db.read_joined_final_image_integrity(scope.native(), &self.pointer())?;
        let r = &self.record;
        let pf = point.preflight();
        if bounded_json(&actual.record, MAX_NATIVE_RECEIPT_BYTES)?
            != bounded_json(r, MAX_NATIVE_RECEIPT_BYTES)?
            || physical.joined_final_image_pointer() != Some(&self.pointer())
            || !std::ptr::eq(pf, scope.preflight().as_ref())
            || r.database_open != db.incarnation_id()
            || r.attempt != scope.attempt_id()
            || r.manifest.checksum_sha256 != pf.source().manifest().checksum_sha256
            || r.namespaces != pf.bindings().namespace_ids()
            || r.manifest.schema_sha256 != pf.bindings().schema_sha256()
            || r.manifest.readers != pf.bindings().reader_features()
            || r.store_open != pf.selected().catalogue().store_open_id()
            || r.catalogue_id != pf.selected().catalogue().catalogue_id()
            || r.original_writer_policy_sha256 != scope.original_writer_sha256()
            || r.image_generations != db.native_range_image_generation_pair(r.range)?
            || bounded_json(&r.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&verification::actual_indexes(db)?, MAX_NATIVE_RECEIPT_BYTES)?
            || r.metadata_store != point.request().member().store_id()
            || point.request().own().term() < r.metadata_term
            || point.request().own().read_index() < r.metadata_index
            || db.replication_shippable_seq() < r.target_reserved_b
            || db.current_visibility_watermark() < r.target_reserved_b
        {
            return Err(invalid(
                "Joined catalogue final original physical identity changed",
            ));
        }
        let NativeRemoteImageSourcePayloadV1::Finished(seal) = point.frame().payload()? else {
            return Err(invalid(
                "Joined catalogue final requires genuine Finished source frame",
            ));
        };
        if seal != &r.source_seal {
            return Err(invalid(
                "Joined catalogue changed original full source seal",
            ));
        }
        point.require_live()?;
        scope.require_database(db)
    }
    pub(crate) fn pointer(&self) -> NativeJoinedRemoteFinalImagePointer {
        self.record.pointer()
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.record.manifest
    }
    pub(crate) fn source_seal(&self) -> &LocalRangeImageSealV1 {
        &self.record.source_seal
    }
    pub(crate) fn history_generation(&self) -> Uuid {
        self.record.history_generation
    }
    pub(crate) fn target_reserved_b(&self) -> u64 {
        self.record.target_reserved_b
    }
    pub(crate) fn target_min_scan_b(&self) -> u64 {
        self.record.target_min_scan_b
    }
    pub(crate) fn image_generations(&self) -> (Uuid, Uuid) {
        self.record.image_generations
    }
    pub(crate) fn target_birth(&self) -> &crate::distribution::NativeStoreBirthV1 {
        &self.record.target_birth
    }
    pub(crate) fn target_database_open(&self) -> &str {
        &self.record.database_open
    }
    pub(crate) fn target_store_open(&self) -> Uuid {
        self.record.store_open
    }
    pub(crate) fn target_catalogue_id(&self) -> Uuid {
        self.record.catalogue_id
    }
    pub(crate) fn target_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.record.namespaces
    }
    pub(crate) fn original_writer_policy_sha256(&self) -> &str {
        &self.record.original_writer_policy_sha256
    }
    pub(crate) fn proposal(&self) -> &NativeJoinedRemoteProposalPointer {
        &self.record.proposal
    }
    pub(crate) fn history_header_sha256(&self) -> &str {
        &self.record.history_header_sha256
    }
    pub(crate) fn rows_header_sha256(&self) -> &str {
        &self.record.rows_header_sha256
    }
    pub(crate) fn rows_native_xid(&self) -> u64 {
        self.record.rows_native_xid
    }
    pub(crate) fn index_definitions(&self) -> &[IndexDefinition] {
        &self.record.index_definitions
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        let actual = read_actual(db, context, &self.pointer())?;
        if bounded_json(&actual.record, MAX_NATIVE_RECEIPT_BYTES)?
            != bounded_json(&self.record, MAX_NATIVE_RECEIPT_BYTES)?
        {
            return Err(invalid(
                "Joined final receipt changed exact original native bytes",
            ));
        }
        Ok(())
    }
}
impl std::fmt::Debug for NativeJoinedRemoteFinalImageReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeJoinedRemoteFinalImageReceipt")
            .field("pointer", &self.pointer())
            .finish_non_exhaustive()
    }
}
enum State {
    Prepared(Record),
    NativeBegun {
        xid: u64,
    },
    Attempted {
        record: Record,
        bytes: Vec<u8>,
    },
    Committed {
        record: Record,
        bytes: Vec<u8>,
    },
    Verified {
        receipt: Arc<NativeJoinedRemoteFinalImageReceipt>,
        bytes: Vec<u8>,
    },
}
pub(super) struct JoinedFinalPublication {
    state: State,
    issued_xid: Option<u64>,
    commit_returned: Option<u64>,
    last_error: Option<String>,
}
impl JoinedFinalPublication {
    fn begin(
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        proof: &VerifiedJoinedTargetContents,
    ) -> Result<Self> {
        context.require_current(db)?;
        let pf = context.point.preflight();
        if db
            .read_native_range_image_record::<serde_json::Value>(&current(
                context.scope.attempt_id(),
            ))?
            .is_some()
        {
            return Err(invalid(
                "Joined final image exists without original publication owner",
            ));
        }
        let record = Record {
            version: 3,
            manifest: pf.source().manifest().clone(),
            source_seal: proof.seal.clone(),
            attempt: context.scope.attempt_id(),
            range: context.scope.range_id(),
            database_open: db.incarnation_id().into(),
            target_birth: crate::distribution::NativeStoreBirthV1::from_actual_native(
                context.scope.native(),
            )?,
            store_open: context.store.native_writer_store_open_id(),
            catalogue_id: pf.selected().catalogue().catalogue_id(),
            namespaces: pf.bindings().namespace_ids(),
            intent_sha256: context.fence.intent_sha256()?,
            proposal: context.fence.protected_proposal().clone(),
            history_generation: proof.history_generation,
            history_header_sha256: proof.history_header_sha256.clone(),
            history_header: db.read_joined_remote_history(context.scope.attempt_id())?,
            rows_native_xid: proof.rows.native_xid,
            rows_header_sha256: proof.rows.header_sha256.clone(),
            rows_header: db.archive_joined_completed_rows(
                context.scope.attempt_id(),
                proof.rows.native_xid,
                &proof.rows.header_sha256,
            )?,
            last_applied_target_b: proof.rows.last_actual_target_b,
            verification_generation: proof.generation,
            target_reserved_b: proof.target_reserved_b,
            target_min_scan_b: proof.min_snapshot_b,
            image_generations: proof.image_generations,
            index_definitions: proof.index_definitions.clone(),
            original_writer_policy_sha256: context.scope.original_writer_sha256().to_owned(),
            metadata_store: context.point.request().member().store_id(),
            metadata_context: context.point.request().own().context_id(),
            metadata_term: context.point.request().own().term(),
            metadata_index: context.point.request().own().read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        let mut sized = record.clone();
        sized.native_page_xid = u64::MAX;
        sized.checksum_sha256 = sized.checksum()?;
        sized.require_current(db, context)?;
        Ok(Self {
            state: State::Prepared(record),
            commit_returned: None,
            issued_xid: None,
            last_error: None,
        })
    }
    pub(super) fn receipt(&self) -> Option<&Arc<NativeJoinedRemoteFinalImageReceipt>> {
        match &self.state {
            State::Verified { receipt, .. } => Some(receipt),
            _ => None,
        }
    }
    fn advance(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        proof: &VerifiedJoinedTargetContents,
    ) -> Result<NativeJoinedRemoteFinalImagePointer> {
        context.require_current(db)?;
        crate::format::enable_native_joined_remote_image(db.data_path())?;
        if let State::Prepared(template) = &self.state {
            if self.issued_xid.is_some() {
                return Err(invalid(
                    "Joined final issued original xid lost framing; no replacement",
                ));
            }
            let mut candidate = template.clone();
            candidate.require_verified(proof)?;
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(context.scope.frozen()?)?;
            let result = db.in_paged_transaction(|paged, xid| {
                self.issued_xid = Some(xid); // FIRST actual native closure operation.
                self.state = State::NativeBegun { xid }; // Before any fallible scope/framing.
                context.require_current(db)?;
                candidate.native_page_xid = xid;
                candidate.checksum_sha256 = candidate.checksum()?;
                candidate.require_current(db, context)?;
                let bytes = bounded_json(&candidate, MAX_NATIVE_RECEIPT_BYTES)?;
                self.state = State::Attempted {
                    record: candidate.clone(),
                    bytes: bytes.clone(),
                };
                for name in [archive(candidate.attempt, xid), current(candidate.attempt)] {
                    paged
                        .store()
                        .put(xid, &native_key(&name), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                context.require_current(db)
            });
            if let Ok(Some(xid)) = &result {
                self.commit_returned = Some(*xid); // Actual native return BEFORE postchecks.
                let State::Attempted { record, bytes } = &self.state else {
                    return Err(invalid("Joined final committed original candidate absent"));
                };
                if record.native_page_xid != *xid || self.issued_xid != Some(*xid) {
                    return Err(invalid("Joined final actual committed xid differs"));
                }
                self.state = State::Committed {
                    record: record.clone(),
                    bytes: bytes.clone(),
                };
            }
            drop(controlled);
            if let Err(error) = &result {
                self.last_error = Some(format!("{error:?}").chars().take(4096).collect());
            }
            result?.ok_or_else(|| invalid("Joined final native page store absent"))?;
        }
        let (record,bytes)=match &self.state {
            State::Prepared(_)=>return Err(invalid("Joined final original native attempt absent")),
            State::NativeBegun{xid}=>return Err(invalid(format!("Joined final issued native xid {xid} before framing; original outcome retained; no replacement"))),
            State::Attempted{record,bytes}|State::Committed{record,bytes}=>(record.clone(),bytes.clone()),
            State::Verified{receipt,bytes}=>(receipt.record.clone(),bytes.clone()),
        };
        record.require_verified(proof)?;
        record.require_current(db, context)?;
        let receipt = Arc::new(read_actual(db, context, &record.pointer())?);
        if self.issued_xid != Some(record.native_page_xid)
            || bounded_json(&receipt.record, MAX_NATIVE_RECEIPT_BYTES)? != bytes
        {
            return Err(invalid(
                "Joined final readback differs from original issued xid/bytes",
            ));
        }
        context.require_current(db)?;
        let pointer = receipt.pointer();
        let next = State::Verified { receipt, bytes }; // Allocate before final short guard.
        let identity = context.scope.frozen()?.observe_identity()?;
        let slot = &mut self.state;
        let gate = context.scope.frozen()?.try_current_publication()?;
        let version = context
            .point
            .request()
            .member()
            .publication_version()
            .try_publication_guard()?;
        context.point.request().require_request_memory()?;
        context.point.frame().require_request_live()?;
        identity.require_in_current_publication(&gate)?;
        db.require_joined_publication_storage_health()?;
        let old = std::mem::replace(slot, next);
        drop(version);
        drop(gate);
        drop(old);
        Ok(pointer)
    }
    #[cfg(test)]
    pub(super) fn test_state(&self) -> serde_json::Value {
        let (state, record, bytes) = match &self.state {
            State::Prepared(r) => ("prepared", Some(r), None),
            State::NativeBegun { .. } => ("issued_before_framing", None, None),
            State::Attempted { record, bytes } => ("attempted_unknown", Some(record), Some(bytes)),
            State::Committed { record, bytes } => {
                ("actual_native_commit_returned", Some(record), Some(bytes))
            }
            State::Verified { receipt, bytes } => (
                "verified_current_and_archive",
                Some(&receipt.record),
                Some(bytes),
            ),
        };
        serde_json::json!({"state":state,"record":record,"issued_xid":self.issued_xid,"actual_commit_returned":self.commit_returned,"last_error":self.last_error,"candidate_bytes_sha256":bytes.map(|b|hex::encode(sha2::Sha256::digest(b))),"no_native_base_catalogue_or_Serving":true})
    }
}
fn read_actual(
    db: &BicDb,
    context: &NativeJoinedRemoteImageContext<'_>,
    pointer: &NativeJoinedRemoteFinalImagePointer,
) -> Result<NativeJoinedRemoteFinalImageReceipt> {
    context.require_current(db)?;
    pointer.validate()?;
    let record: Record = db
        .read_native_range_image_record(&current(pointer.attempt_id))?
        .ok_or_else(|| invalid("Joined final CURRENT absent; original outcome unproven"))?;
    let archived: Record = db
        .read_native_range_image_record(&archive(pointer.attempt_id, pointer.native_page_xid))?
        .ok_or_else(|| invalid("Joined final archive absent; original outcome unproven"))?;
    record.require_current(db, context)?;
    archived.validate()?;
    if record.pointer() != *pointer
        || archived.pointer() != *pointer
        || bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?
            != bounded_json(&archived, MAX_NATIVE_RECEIPT_BYTES)?
        || db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Joined final native page store absent"))?
            .store()
            .transactions()
            .status(record.native_page_xid)
            != bicdb_page::TxStatus::Committed
    {
        return Err(invalid(
            "Joined final exact committed CURRENT/archive not proven",
        ));
    }
    context.require_current(db)?;
    Ok(NativeJoinedRemoteFinalImageReceipt { record })
}
impl NativeJoinedRemoteImageInstallation {
    pub(crate) fn final_image_receipt(&self) -> Option<&Arc<NativeJoinedRemoteFinalImageReceipt>> {
        self.final_publication
            .as_ref()
            .and_then(JoinedFinalPublication::receipt)
    }
    pub(crate) fn publish_final_image(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<NativeJoinedRemoteFinalImagePointer> {
        self.require_owner(db, context)?;
        if self.scan_pending.is_some() {
            return Err(invalid(
                "Joined final requires publication of retained target scan first",
            ));
        }
        let proof = self.verified_contents(db, context)?;
        if self.final_publication.is_none() {
            self.final_publication = Some(JoinedFinalPublication::begin(db, context, &proof)?);
        }
        self.final_publication
            .as_mut()
            .unwrap()
            .advance(db, context, &proof)
    }
}

/// Lower-bound publication integrity from a real DB-root read, never a
/// current image, resumed owner, row mutation, adoption or Serving capability.
pub(crate) struct HistoricalJoinedFinalImage {
    record: Record,
}
impl std::fmt::Debug for HistoricalJoinedFinalImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalJoinedFinalImage")
            .field("pointer", &self.pointer())
            .finish_non_exhaustive()
    }
}
impl HistoricalJoinedFinalImage {
    pub(crate) fn pointer(&self) -> NativeJoinedRemoteFinalImagePointer {
        self.record.pointer()
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.record.manifest
    }
    pub(crate) fn source_seal(&self) -> &LocalRangeImageSealV1 {
        &self.record.source_seal
    }
    pub(crate) fn history_generation(&self) -> Uuid {
        self.record.history_generation
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.record.manifest.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.record.manifest.h0
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.record.manifest.h
    }
    pub(crate) fn image_root_sha256(&self) -> &str {
        &self.record.source_seal.image_root_sha256
    }
    pub(crate) fn record_count(&self) -> u64 {
        self.record.source_seal.record_count
    }
    pub(crate) fn serialized_record_bytes(&self) -> u64 {
        self.record.source_seal.serialized_record_bytes
    }
    pub(crate) fn target_reserved_b(&self) -> u64 {
        self.record.target_reserved_b
    }
    pub(crate) fn target_min_scan_b(&self) -> u64 {
        self.record.target_min_scan_b
    }
    pub(crate) fn image_generations(&self) -> (Uuid, Uuid) {
        self.record.image_generations
    }
    pub(crate) fn target_birth(&self) -> &crate::distribution::NativeStoreBirthV1 {
        &self.record.target_birth
    }
    pub(crate) fn target_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.record.namespaces
    }
    pub(crate) fn target_catalogue_id(&self) -> Uuid {
        self.record.catalogue_id
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.record.intent_sha256
    }
    pub(crate) fn proposal(&self) -> &NativeJoinedRemoteProposalPointer {
        &self.record.proposal
    }
    pub(crate) fn history_header_sha256(&self) -> &str {
        &self.record.history_header_sha256
    }
    pub(crate) fn rows_header_sha256(&self) -> &str {
        &self.record.rows_header_sha256
    }
    pub(crate) fn rows_native_xid(&self) -> u64 {
        self.record.rows_native_xid
    }
    pub(crate) fn index_definitions(&self) -> &[IndexDefinition] {
        &self.record.index_definitions
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        let actual = db.read_joined_final_image_integrity(native, &self.pointer())?;
        if bounded_json(&actual.record, MAX_NATIVE_RECEIPT_BYTES)?
            != bounded_json(&self.record, MAX_NATIVE_RECEIPT_BYTES)?
        {
            return Err(invalid(
                "Joined historical final image protected binding changed",
            ));
        }
        Ok(())
    }
}

impl BicDb {
    pub(crate) fn read_joined_final_image_integrity(
        &self,
        native: &NativeRecoveryAdmission,
        pointer: &NativeJoinedRemoteFinalImagePointer,
    ) -> Result<HistoricalJoinedFinalImage> {
        native.require_current(self)?;
        pointer.validate()?;
        self.check_recovery_state()?;
        if !crate::format::native_joined_remote_image_active(self.data_path())? {
            return Err(invalid("Joined historical final-image reader fence absent"));
        }
        let record: Record = self
            .read_native_range_image_record(&current(pointer.attempt_id))?
            .ok_or_else(|| invalid("Joined historical final CURRENT absent"))?;
        let archived: Record = self
            .read_native_range_image_record(&archive(pointer.attempt_id, pointer.native_page_xid))?
            .ok_or_else(|| invalid("Joined historical final immutable archive absent"))?;
        record.validate()?;
        archived.validate()?;
        if record.pointer() != *pointer
            || archived.pointer() != *pointer
            || bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&archived, MAX_NATIVE_RECEIPT_BYTES)?
            || record.target_birth
                != crate::distribution::NativeStoreBirthV1::from_actual_native(native)?
            || record.manifest.cluster_id != *native.cluster_id()
            || record.manifest.bootstrap_id != native.bootstrap_id()
            || record.manifest.target_id != *native.local_node_id()
            || record.manifest.target_birth.node_incarnation() != native.local_member_incarnation()
            || record.manifest.restore.as_ref() != native.restore_activation()
            || self.native_abort_catalogue_store_id()? != Some(record.catalogue_id)
            || self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Joined historical final page store absent"))?
                .store()
                .transactions()
                .status(record.native_page_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid("Joined historical final-image native identity or exact committed publication differs"));
        }
        record.require_published_history(self)?;
        native.require_current(self)?;
        self.check_recovery_state()?;
        Ok(HistoricalJoinedFinalImage { record })
    }
}
