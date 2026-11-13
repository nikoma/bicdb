//! Distinct actual Registered final physical image. It publishes neither an
//! install:base/native floor nor catalogue adoption, writer or Serving state.
use super::verification::VerifiedRegisteredTargetContents;
use super::*;
use crate::distribution_range_image::{LocalRangeImageSealV1, NativeRemoteImageManifestV1};

fn current(attempt: Uuid) -> String {
    format!("registered-remote-image/{attempt}/final/current")
}
fn archive(attempt: Uuid, xid: u64) -> String {
    format!("registered-remote-image/{attempt}/final/archive/{xid:020}")
}
pub(crate) use crate::db::NativeRegisteredRemoteFinalImagePointer;
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
    proposal: NativeRegisteredRemoteProposalPointer,
    history_generation: Uuid,
    history_header_sha256: String,
    rows_native_xid: u64,
    rows_header_sha256: String,
    last_applied_target_b: Option<u64>,
    verification_generation: Uuid,
    target_reserved_b: u64,
    target_min_scan_b: u64,
    image_generations: (Uuid, Uuid),
    index_definitions: Vec<IndexDefinition>,
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
            &("registered-remote-final-image-v1", r),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn pointer(&self) -> NativeRegisteredRemoteFinalImagePointer {
        NativeRegisteredRemoteFinalImagePointer {
            attempt_id: self.attempt,
            native_page_xid: self.native_page_xid,
            checksum_sha256: self.checksum_sha256.clone(),
        }
    }
    fn validate(&self) -> Result<()> {
        self.manifest.require_shape()?;
        self.proposal.validate()?;
        self.pointer().validate()?;
        if self.version != 1
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
            return Err(invalid("damaged Registered protected final image"));
        }
        Ok(())
    }
    fn require_current(
        &self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<()> {
        self.validate()?;
        context.require_current(db)?;
        let preflight = context.point.preflight();
        let history = db.read_registered_remote_history(self.attempt)?;
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
            || self.catalogue_id != preflight.catalogue().catalogue_id()
            || self.namespaces != preflight.bindings().namespace_ids()
            || self.intent_sha256 != context.fence.intent_sha256()?
            || self.proposal != *context.fence.protected_proposal()
            || self.history_generation != history.generation
            || self.history_header_sha256 != history.checksum_sha256
            || !history.audited
            || self.image_generations != db.native_range_image_generation_pair(self.range)?
            || bounded_json(&self.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&verification::actual_indexes(db)?, MAX_NATIVE_RECEIPT_BYTES)?
            || self.metadata_store != context.point.request().member().store_id()
            || context.point.request().own().term() < self.metadata_term
            || context.point.request().own().read_index() < self.metadata_index
            || db.replication_shippable_seq() < self.target_reserved_b
            || db.current_visibility_watermark() < self.target_reserved_b
        {
            return Err(invalid(
                "Registered final image original source/target/protected point changed",
            ));
        }
        db.require_registered_completed_rows_point(
            self.attempt,
            self.rows_native_xid,
            &self.rows_header_sha256,
        )?;
        context.require_current(db)
    }
    fn require_verified(&self, proof: &VerifiedRegisteredTargetContents) -> Result<()> {
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
                "Registered final publication changed original independent scan",
            ));
        }
        Ok(())
    }
}
/// Actual committed protected receipt, not deserializable and not a range
/// floor. Later adoption must consume this alongside actual original custody.
pub(crate) struct NativeRegisteredRemoteFinalImageReceipt {
    record: Record,
}
impl std::fmt::Debug for NativeRegisteredRemoteFinalImageReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRegisteredRemoteFinalImageReceipt")
            .field("pointer", &self.pointer())
            .finish_non_exhaustive()
    }
}
impl NativeRegisteredRemoteFinalImageReceipt {
    pub(crate) fn pointer(&self) -> NativeRegisteredRemoteFinalImagePointer {
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
    pub(crate) fn target_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.record.namespaces
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
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.record.intent_sha256
    }
    pub(crate) fn proposal(&self) -> &NativeRegisteredRemoteProposalPointer {
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
    pub(crate) fn image_generations(&self) -> (Uuid, Uuid) {
        self.record.image_generations
    }
    pub(crate) fn index_definitions(&self) -> &[IndexDefinition] {
        &self.record.index_definitions
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<()> {
        let actual = read_actual(db, context, &self.pointer())?;
        if bounded_json(&actual.record, MAX_NATIVE_RECEIPT_BYTES)?
            != bounded_json(&self.record, MAX_NATIVE_RECEIPT_BYTES)?
        {
            return Err(invalid("Registered actual final receipt changed"));
        }
        Ok(())
    }
}
enum State {
    Prepared(Record),
    Attempted {
        record: Record,
        bytes: Vec<u8>,
    },
    Committed {
        record: Record,
        bytes: Vec<u8>,
    },
    Verified {
        receipt: Arc<NativeRegisteredRemoteFinalImageReceipt>,
        bytes: Vec<u8>,
    },
}
pub(super) struct RegisteredFinalPublication {
    state: State,
    commit_returned: Option<u64>,
}
impl RegisteredFinalPublication {
    fn begin(
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        proof: &VerifiedRegisteredTargetContents,
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
                "Registered final image exists without original publication owner",
            ));
        }
        let record = Record {
            version: 1,
            manifest: pf.source().manifest().clone(),
            source_seal: proof.seal.clone(),
            attempt: context.scope.attempt_id(),
            range: context.scope.range_id(),
            database_open: db.incarnation_id().into(),
            target_birth: crate::distribution::NativeStoreBirthV1::from_actual_native(
                context.scope.native(),
            )?,
            store_open: context.store.native_writer_store_open_id(),
            catalogue_id: pf.catalogue().catalogue_id(),
            namespaces: pf.bindings().namespace_ids(),
            intent_sha256: context.fence.intent_sha256()?,
            proposal: context.fence.protected_proposal().clone(),
            history_generation: proof.history_generation,
            history_header_sha256: proof.history_header_sha256.clone(),
            rows_native_xid: proof.rows.native_xid,
            rows_header_sha256: proof.rows.header_sha256.clone(),
            last_applied_target_b: proof.rows.last_actual_target_b,
            verification_generation: proof.generation,
            target_reserved_b: proof.target_reserved_b,
            target_min_scan_b: proof.min_snapshot_b,
            image_generations: proof.image_generations,
            index_definitions: proof.index_definitions.clone(),
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
        })
    }
    pub(super) fn receipt(&self) -> Option<&Arc<NativeRegisteredRemoteFinalImageReceipt>> {
        if let State::Verified { receipt, .. } = &self.state {
            Some(receipt)
        } else {
            None
        }
    }
    fn advance(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        proof: &VerifiedRegisteredTargetContents,
    ) -> Result<NativeRegisteredRemoteFinalImagePointer> {
        context.require_current(db)?;
        crate::format::enable_native_registered_remote_image(db.data_path())?;
        if let State::Prepared(record) = &self.state {
            let mut record = record.clone();
            record.require_verified(proof)?;
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(context.scope.frozen())?;
            let result = db.in_paged_transaction(|paged, xid| {
                context.require_current(db)?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.require_current(db, context)?;
                let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
                self.state = State::Attempted {
                    record: record.clone(),
                    bytes: bytes.clone(),
                };
                for key in [archive(record.attempt, xid), current(record.attempt)] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                context.require_current(db)
            });
            if let Ok(Some(xid)) = &result {
                self.commit_returned = Some(*xid);
                let State::Attempted { record, bytes } = &self.state else {
                    return Err(invalid("Registered final attempted owner absent"));
                };
                if record.native_page_xid != *xid {
                    return Err(invalid("Registered final actual returned xid differs"));
                }
                self.state = State::Committed {
                    record: record.clone(),
                    bytes: bytes.clone(),
                };
            }
            drop(controlled);
            result?.ok_or_else(|| invalid("Registered final page store absent"))?;
            #[cfg(test)]
            cuts::fire();
        }
        let (record, bytes) = match &self.state {
            State::Prepared(_) => return Err(invalid("Registered final native attempt absent")),
            State::Attempted { record, bytes } | State::Committed { record, bytes } => {
                (record.clone(), bytes.clone())
            }
            State::Verified { receipt, bytes } => (receipt.record.clone(), bytes.clone()),
        };
        record.require_verified(proof)?;
        record.require_current(db, context)?;
        let receipt = read_actual(db, context, &record.pointer())?;
        if bounded_json(&receipt.record, MAX_NATIVE_RECEIPT_BYTES)? != bytes {
            return Err(invalid(
                "Registered final actual bytes differ from retained original attempt",
            ));
        }
        context.require_current(db)?;
        let pointer = receipt.pointer();
        self.state = State::Verified {
            receipt: Arc::new(receipt),
            bytes,
        };
        Ok(pointer)
    }
    #[cfg(test)]
    pub(super) fn test_state(&self) -> serde_json::Value {
        let (name, record) = match &self.state {
            State::Prepared(r) => ("prepared", r),
            State::Attempted { record, .. } => ("attempted_unknown", record),
            State::Committed { record, .. } => ("native_commit_returned_before_postchecks", record),
            State::Verified { receipt, .. } => ("verified", &receipt.record),
        };
        serde_json::json!({"state":name,"record":record,"actual_commit_returned":self.commit_returned,"no_native_floor_or_serving":true,"independent_index_parity":false})
    }
}
fn read_actual(
    db: &BicDb,
    context: &NativeRegisteredRemoteImageContext<'_>,
    pointer: &NativeRegisteredRemoteFinalImagePointer,
) -> Result<NativeRegisteredRemoteFinalImageReceipt> {
    context.require_current(db)?;
    let record: Record = db
        .read_native_range_image_record(&current(pointer.attempt_id))?
        .ok_or_else(|| invalid("Registered final CURRENT absent; outcome unproven"))?;
    let archived: Record = db
        .read_native_range_image_record(&archive(pointer.attempt_id, pointer.native_page_xid))?
        .ok_or_else(|| invalid("Registered final archive absent; outcome unproven"))?;
    record.require_current(db, context)?;
    archived.validate()?;
    if record.pointer() != *pointer
        || archived.pointer() != *pointer
        || bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?
            != bounded_json(&archived, MAX_NATIVE_RECEIPT_BYTES)?
        || db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Registered final page store absent"))?
            .store()
            .transactions()
            .status(record.native_page_xid)
            != bicdb_page::TxStatus::Committed
    {
        return Err(invalid(
            "Registered final exact committed CURRENT/archive outcome unproven",
        ));
    }
    context.require_current(db)?;
    Ok(NativeRegisteredRemoteFinalImageReceipt { record })
}
impl NativeRegisteredRemoteImageInstallation {
    pub(crate) fn final_image_receipt(
        &self,
    ) -> Option<&Arc<NativeRegisteredRemoteFinalImageReceipt>> {
        self.final_publication.as_ref().and_then(|p| p.receipt())
    }
    pub(crate) fn publish_final_image(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteFinalImagePointer> {
        let proof = self.verified_contents(db, context)?;
        if self.final_publication.is_none() {
            self.final_publication = Some(RegisteredFinalPublication::begin(db, context, &proof)?);
        }
        self.final_publication
            .as_mut()
            .unwrap()
            .advance(db, context, &proof)
    }
}
#[cfg(test)]
mod cuts {
    use std::cell::RefCell;
    thread_local! {static CUT:RefCell<Option<Box<dyn FnOnce()>>>=const{RefCell::new(None)};}
    pub(crate) struct Guard(Option<Box<dyn FnOnce()>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|s| {
                s.replace(self.0.take());
            });
        }
    }
    pub(crate) fn arm(cut: Box<dyn FnOnce()>) -> Guard {
        Guard(CUT.with(|s| s.replace(Some(cut))))
    }
    pub(super) fn fire() {
        let cut = CUT.with(|s| s.borrow_mut().take());
        if let Some(cut) = cut {
            cut();
        }
    }
}
#[cfg(test)]
pub(crate) use cuts::arm as test_registered_remote_final_image_after_wal;
