//! Bounded native target installation. Staging and native receipt creation are
//! deliberately separate from metadata membership and range-log activation.
use std::sync::Arc;

use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::{
    NativeHistoryStage, NativeHistoryStageProgress, NativeHistoryTargetContext,
    NativeInstalledRangeImageBase, NativeRangeInstallSafety, NativeRecoveryAdmission,
    NativeTargetImageVerificationProgress, NativeTargetImageVerifier, RangeImageDbBindings,
    StagedNativeHistoryProof,
};
use crate::distribution::{distribution_key_token, RangeSnapshotOptions};
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageCatalogueAdoptionReceipt, NativeImageInstallRole,
    RangeWriteStore, TargetImageCatalogHistoryObligations, TargetImageInstallFence,
};
use crate::distribution_range_image::{
    bounded_json, canonical_range_image_catalog, content_hasher, CompletedNativeRangeImageSource,
    RangeImageCursorV1, RangeImageHistoryStepV1, RangeImageStepV1,
    TentativeNativeRangeImageSourceAuthorization,
};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationAuthority};
use crate::{BicDb, BicDbError, CancellationToken, CollectionMeta, Result};

const MAX_RECORDS: usize = 16_384;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_STEP_BYTES: usize = 9 * 1024 * 1024;

fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("native image installer: {}", detail.into()))
}
fn frame(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}
fn options() -> RangeSnapshotOptions {
    RangeSnapshotOptions {
        max_records_per_batch: 1024,
        max_bytes_per_batch: MAX_BYTES,
        max_record_bytes: MAX_BYTES,
    }
}

/// One owned attempt. Dropping this releases only local exclusion; its catalog
/// intent/native dirty marker remain durable refusals. Restart requires a new
/// authenticated attempt and starts the bounded clear/copy again.
pub(crate) struct NativeRangeImageInstaller {
    database: Arc<RwLock<BicDb>>,
    admission: NativeRecoveryAdmission,
    frozen: FrozenRangeMutationPermit,
    fence: TargetImageInstallFence,
    bindings: RangeImageDbBindings,
    source_reserved_db_upper_bound: u64,
    safety: NativeRangeInstallSafety,
    catalog: TargetImageCatalogHistoryObligations,
    history_stage: NativeHistoryStage,
    history_proof: Option<StagedNativeHistoryProof>,
    destructive_started: bool,
    clear_collection: usize,
    clear_after: Option<String>,
    cleared: bool,
    cursor: RangeImageCursorV1,
    chain: String,
    stream_content: Sha256,
    records: u64,
    bytes: u64,
    complete: bool,
    previous_step: Option<RangeImageStepV1>,
}

/// Owns the unchanged target image and actual source certificate across bounded
/// verification steps. Callers release service/DB locks and renew fresh roles
/// between steps; neither waiting nor a partial scan manufactures readiness.
pub(crate) struct NativeRangeImageVerification {
    database: Arc<RwLock<BicDb>>,
    admission: NativeRecoveryAdmission,
    frozen: FrozenRangeMutationPermit,
    fence: TargetImageInstallFence,
    bindings: RangeImageDbBindings,
    source: CompletedNativeRangeImageSource,
    verifier: NativeTargetImageVerifier,
    history: Option<(
        NativeRangeInstallSafety,
        TargetImageCatalogHistoryObligations,
        StagedNativeHistoryProof,
    )>,
    recover_completed: bool,
}
impl NativeRangeImageVerification {
    pub(crate) fn advance(
        &mut self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<NativeTargetImageVerificationProgress> {
        self.source.require_current()?;
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy during bounded verification"))?;
        store.validate_native_image_install_step(
            &self.fence,
            &db,
            &self.admission,
            role,
            &self.frozen,
        )?;
        if let Some((safety, catalog, proof)) = &self.history {
            let context = NativeHistoryTargetContext::new(
                &db,
                store,
                &self.fence,
                &self.admission,
                safety,
                catalog,
                role,
                &self.frozen,
            )?;
            proof.require_current(&db, &context, &self.source.authorize_target_install()?)?;
        } else if !self.recover_completed {
            return Err(invalid("bounded verification has no actual staged history"));
        }
        db.advance_native_target_image_verification(
            &mut self.verifier,
            &self.fence,
            &self.admission,
            &self.frozen,
            &self.bindings,
            role,
            &self.source,
            cancellation,
        )
    }
    pub(crate) fn finish(
        self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedNativeRangeImageInstall> {
        self.source.require_current()?;
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy before verified native publication"))?;
        store.validate_native_image_install_step(
            &self.fence,
            &db,
            &self.admission,
            role,
            &self.frozen,
        )?;
        let contents = db.finish_native_target_image_verification(
            self.verifier,
            &self.fence,
            &self.admission,
            &self.frozen,
            &self.bindings,
            role,
            &self.source,
            cancellation,
        )?;
        let history_context = self
            .history
            .as_ref()
            .map(|(safety, catalog, _)| {
                NativeHistoryTargetContext::new(
                    &db,
                    store,
                    &self.fence,
                    &self.admission,
                    safety,
                    catalog,
                    role,
                    &self.frozen,
                )
            })
            .transpose()?;
        let history = match (&self.history, &history_context) {
            (Some((_, _, proof)), Some(context)) => Some((proof, context)),
            _ => None,
        };
        let base = db.finish_native_target_image_install(
            &self.fence,
            &self.admission,
            &self.frozen,
            &self.bindings,
            role,
            &self.source,
            self.recover_completed,
            contents,
            history,
            cancellation,
        )?;
        drop(db);
        let verified = VerifiedNativeRangeImageInstall {
            database: self.database,
            admission: self.admission,
            frozen: self.frozen,
            fence: self.fence,
            source: self.source,
            base,
        };
        verified.require_current(store, role)?;
        Ok(verified)
    }
}

/// Not deserializable. Its construction consumed the actual complete source
/// capture and actual target scan/page-WAL receipt. It retains both freezes
/// until the range catalog consumes it; a native receipt alone grants no floor.
pub(crate) struct VerifiedNativeRangeImageInstall {
    database: Arc<RwLock<BicDb>>,
    admission: NativeRecoveryAdmission,
    frozen: FrozenRangeMutationPermit,
    fence: TargetImageInstallFence,
    source: CompletedNativeRangeImageSource,
    base: NativeInstalledRangeImageBase,
}

/// The actual physical image and its fsynced catalogue snapshot, with all
/// refusals/freezes still owned. This is not an activated/serving image.
pub(crate) struct AdoptedNativeRangeImageInstall {
    verified: VerifiedNativeRangeImageInstall,
    catalogue: NativeImageCatalogueAdoptionReceipt,
}
impl AdoptedNativeRangeImageInstall {
    pub(crate) fn catalogue(&self) -> &NativeImageCatalogueAdoptionReceipt {
        &self.catalogue
    }
    pub(crate) fn base(&self) -> &NativeInstalledRangeImageBase {
        self.verified.base()
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
    ) -> Result<()> {
        self.catalogue.require_current(store, &self.verified, role)
    }
}
impl VerifiedNativeRangeImageInstall {
    pub(crate) fn adopt_catalogue(
        self,
        store: &mut RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        cancellation: &CancellationToken,
    ) -> Result<AdoptedNativeRangeImageInstall> {
        cancellation.check()?;
        let catalogue = store.adopt_verified_native_image_catalogue(&self, role)?;
        cancellation.check()?;
        let adopted = AdoptedNativeRangeImageInstall {
            verified: self,
            catalogue,
        };
        adopted.require_current(store, role)?;
        Ok(adopted)
    }
    pub(crate) fn base(&self) -> &NativeInstalledRangeImageBase {
        &self.base
    }
    pub(crate) fn fence(&self) -> &TargetImageInstallFence {
        &self.fence
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
    ) -> Result<()> {
        self.source.require_current()?;
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy during verified install validation"))?;
        store.validate_native_image_install_step(
            &self.fence,
            &db,
            &self.admission,
            role,
            &self.frozen,
        )?;
        self.base.require_current(&db, &self.admission)
    }
}

impl NativeRangeImageInstaller {
    /// A process may have died after native receipt commit but before catalog
    /// activation. Recompute the actual complete image and mint a new opaque
    /// token for the SAME durable attempt, without clearing or copying rows.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_completed(
        database: Arc<RwLock<BicDb>>,
        store: &mut RangeWriteStore,
        admission: NativeRecoveryAdmission,
        source: CompletedNativeRangeImageSource,
        role: &NativeImageInstallRole<'_>,
        frozen: FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<NativeRangeImageVerification> {
        cancellation.check()?;
        source.require_current()?;
        let db = database
            .try_read()
            .ok_or_else(|| invalid("target DB busy before completed image recovery"))?;
        let bindings = db.capture_range_image_bindings()?;
        let prior = db
            .native_installed_range_image_base(&admission, source.cut().range().id)?
            .ok_or_else(|| invalid("completed recovery has no actual native receipt"))?;
        let fence = store.reacquire_completed_native_image_install(
            &db, &admission, &source, role, &frozen, purpose, attempt_id, &prior,
        )?;
        let verifier = db.begin_native_target_image_verification(
            &fence,
            &admission,
            &frozen,
            &bindings,
            role,
            &source,
            true,
            cancellation,
        )?;
        drop(db);
        Ok(NativeRangeImageVerification {
            database,
            admission,
            frozen,
            fence,
            bindings,
            source,
            verifier,
            history: None,
            recover_completed: true,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin(
        database: Arc<RwLock<BicDb>>,
        store: &mut RangeWriteStore,
        admission: NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
    ) -> Result<Self> {
        source.require_current()?;
        let db = database
            .try_read()
            .ok_or_else(|| invalid("target DB busy before install"))?;
        let bindings = db.capture_range_image_bindings()?;
        if source.bindings().collections() != bindings.collections()
            || source.bindings().schema_sha256() != bindings.schema_sha256()
            || source.bindings().reader_features() != bindings.reader_features()
        {
            return Err(invalid(
                "target must have exact preinstalled portable schema/catalog/reader contract",
            ));
        }
        for meta in source
            .bindings()
            .collections()
            .iter()
            .chain(bindings.collections())
        {
            if meta.storage_name.is_some() || meta.renamed_from.is_some() {
                return Err(invalid("physical namespace aliases/rename recovery are unsupported by this image codec"));
            }
        }
        let safety =
            db.native_range_install_safety(&admission, source.cut().range().id, &frozen)?;
        let fence = store.begin_native_image_install(
            &db, &admission, &safety, source, role, &frozen, purpose, attempt_id,
        )?;
        store.validate_native_image_install_step(&fence, &db, &admission, role, &frozen)?;
        // The catalog refusal is already durably confirmed. From this point
        // any crash/timeout is dirty/unready until a fresh authorized restart.
        db.begin_native_target_image_install(&fence, &admission, &frozen, &bindings)?;
        let catalog_obligations =
            store.image_install_catalog_history_obligations(fence.range().id)?;
        let context = NativeHistoryTargetContext::new(
            &db,
            store,
            &fence,
            &admission,
            &safety,
            &catalog_obligations,
            role,
            &frozen,
        )?;
        let history_stage = db.begin_native_history_stage(&context, source)?;
        let catalog = canonical_range_image_catalog(
            source.cut().cluster_id(),
            source.cut().range(),
            source.cut().range_index(),
            &bindings,
        )?;
        if hex::encode(Sha256::digest(&catalog)) != source.catalog_sha256() {
            return Err(invalid(
                "target canonical catalog differs from actual source",
            ));
        }
        let complete = bindings.collections().is_empty();
        let source_reserved_db_upper_bound = source.reserved_db_upper_bound().get();
        drop(db);
        Ok(Self {
            database,
            admission,
            frozen,
            fence,
            bindings,
            source_reserved_db_upper_bound,
            safety,
            catalog: catalog_obligations,
            history_stage,
            history_proof: None,
            destructive_started: false,
            clear_collection: 0,
            clear_after: None,
            cleared: complete,
            cursor: RangeImageCursorV1::default(),
            chain: hex::encode(Sha256::digest(b"bicdb.range-image.wire.v1\0")),
            stream_content: content_hasher(&catalog),
            records: 0,
            bytes: 0,
            complete,
            previous_step: None,
        })
    }

    pub(crate) fn fence(&self) -> &TargetImageInstallFence {
        &self.fence
    }
    pub(crate) fn history_stage_id(&self) -> Uuid {
        self.history_stage.stage_id()
    }
    pub(crate) fn stage_history_step(
        &mut self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        stage_id: Uuid,
        step: &RangeImageHistoryStepV1,
        cancellation: &CancellationToken,
    ) -> Result<NativeHistoryStageProgress> {
        if self.destructive_started || self.history_proof.is_some() {
            return Err(invalid(
                "history staging is sealed before target data changes",
            ));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy during history stage"))?;
        let context = NativeHistoryTargetContext::new(
            &db,
            store,
            &self.fence,
            &self.admission,
            &self.safety,
            &self.catalog,
            role,
            &self.frozen,
        )?;
        db.append_native_history_stage(
            &self.history_stage,
            &context,
            source,
            stage_id,
            step,
            cancellation,
        )
    }
    pub(crate) fn audit_history_step(
        &mut self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        max_entries: usize,
        cancellation: &CancellationToken,
    ) -> Result<NativeHistoryStageProgress> {
        if self.destructive_started {
            return Err(invalid("cannot restage after target data changes"));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy during history audit"))?;
        let context = NativeHistoryTargetContext::new(
            &db,
            store,
            &self.fence,
            &self.admission,
            &self.safety,
            &self.catalog,
            role,
            &self.frozen,
        )?;
        let progress = db.audit_native_history_stage_step(
            &self.history_stage,
            &context,
            source,
            max_entries,
            cancellation,
        )?;
        if progress.audit_complete {
            self.history_proof =
                Some(db.finish_native_history_stage(&self.history_stage, &context, source)?);
        }
        Ok(progress)
    }
    fn require_history_before_rows(
        &self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
    ) -> Result<()> {
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy checking history"))?;
        let context = NativeHistoryTargetContext::new(
            &db,
            store,
            &self.fence,
            &self.admission,
            &self.safety,
            &self.catalog,
            role,
            &self.frozen,
        )?;
        self.history_proof
            .as_ref()
            .ok_or_else(|| invalid("target data changes require full actual staged history"))?
            .require_current(&db, &context, source)?;
        if !self.destructive_started {
            self.safety
                .require_current(&db, &self.admission, &self.frozen)?;
        }
        Ok(())
    }
    fn validate(&self, store: &RangeWriteStore, role: &NativeImageInstallRole<'_>) -> Result<()> {
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy"))?;
        store.validate_native_image_install_step(
            &self.fence,
            &db,
            &self.admission,
            role,
            &self.frozen,
        )?;
        db.validate_range_image_bindings(&self.bindings)
    }

    /// Remove stale extras in bounded batches before accepting source rows.
    /// Only the authorized range is cleared; physical namespace identities and
    /// every other range's records remain in their existing backing.
    pub(crate) fn clear_step(
        &mut self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        cancellation.check()?;
        self.validate(store, role)?;
        self.require_history_before_rows(store, role, source)?;
        if self.cleared {
            return Ok(true);
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy before clear"))?;
        let meta = &self.bindings.collections()[self.clear_collection];
        let mut ids = Vec::new();
        let export = db.for_each_range_snapshot_batch_with_cancellation(
            &meta.name,
            self.fence.range(),
            self.clear_after.as_deref(),
            &options(),
            MAX_RECORDS,
            cancellation,
            |batch| {
                ids = batch.records.into_iter().map(|record| record.id).collect();
                Ok(false)
            },
        )?;
        let controlled = db
            .range_mutation_gate()
            .try_controlled_mutation(&self.frozen)?;
        self.destructive_started = true;
        db.delete_range_records_as_local_with_mutation(
            self.fence.range(),
            &meta.name,
            &ids,
            &RangeMutationAuthority::Controlled(&controlled),
        )?;
        drop(controlled);
        role.role().require_live_request()?;
        cancellation.check()?;
        if export.completed {
            self.clear_collection += 1;
            self.clear_after = None;
        } else {
            if export.resume_after_key <= self.clear_after {
                return Err(invalid("target clear cursor did not advance"));
            }
            self.clear_after = export.resume_after_key;
        }
        self.cleared = self.clear_collection == self.bindings.collections().len();
        Ok(self.cleared)
    }

    pub(crate) fn install_step(
        &mut self,
        attempt_id: Uuid,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        step: &RangeImageStepV1,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        self.validate(store, role)?;
        self.require_history_before_rows(store, role, source)?;
        if attempt_id != self.fence.attempt_id() || !self.cleared {
            return Err(invalid(
                "late attempt or target stale-key clear is incomplete",
            ));
        }
        if self.previous_step.as_ref() == Some(step) {
            return Ok(());
        }
        validate_step(
            step,
            &self.cursor,
            &self.chain,
            &self.bindings.collections(),
            &self.fence,
            self.source_reserved_db_upper_bound,
        )?;
        if self.complete {
            return Err(invalid("image stream is already complete"));
        }
        let meta = &self.bindings.collections()[self.cursor.collection_index()];
        let mut content = self.stream_content.clone();
        let mut records = self.records;
        let mut bytes = self.bytes;
        if let Some(batch) = &step.batch {
            for record in &batch.records {
                let encoded = bounded_json(record, MAX_BYTES)?;
                frame(&mut content, meta.name.as_bytes());
                frame(&mut content, &encoded);
                records = records
                    .checked_add(1)
                    .ok_or_else(|| invalid("record count overflow"))?;
                bytes = bytes
                    .checked_add(encoded.len() as u64)
                    .ok_or_else(|| invalid("record bytes overflow"))?;
            }
            let db = self
                .database
                .try_read()
                .ok_or_else(|| invalid("target DB busy before image batch"))?;
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(&self.frozen)?;
            self.destructive_started = true;
            db.apply_range_snapshot_batch_preinstalled_with_mutation(
                self.fence.range(),
                meta,
                batch,
                &RangeMutationAuthority::Controlled(&controlled),
            )?;
            drop(controlled);
        }
        self.validate(store, role)?;
        cancellation.check()?;
        self.stream_content = content;
        self.records = records;
        self.bytes = bytes;
        self.cursor = step.next_cursor.clone();
        self.chain.clone_from(&step.chain_sha256);
        self.complete = step.complete;
        self.previous_step = Some(step.clone());
        Ok(())
    }

    pub(crate) fn begin_verification(
        self,
        store: &RangeWriteStore,
        role: &NativeImageInstallRole<'_>,
        source: CompletedNativeRangeImageSource,
        cancellation: &CancellationToken,
    ) -> Result<NativeRangeImageVerification> {
        source.require_current()?;
        self.validate(store, role)?;
        self.require_history_before_rows(store, role, &source.authorize_target_install()?)?;
        let seal = source.seal();
        if !self.cleared
            || !self.complete
            || self.records != seal.record_count
            || self.bytes != seal.serialized_record_bytes
            || self.cursor.step() != seal.steps
            || self.chain != seal.wire_chain_sha256
            || hex::encode(self.stream_content.clone().finalize()) != seal.image_root_sha256
            || seal.session_id != self.fence.session_id()
            || seal.nonce != self.fence.nonce()
            || seal.range_index != self.fence.source_range_index()
            || seal.source_native_receipt_sha256 != self.fence.source_native_receipt_sha256()
            || seal.portable_catalog_sha256 != self.fence.source_catalog_sha256()
        {
            return Err(invalid(
                "complete source certificate does not match the installed ordered stream",
            ));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("target DB busy before native image verification"))?;
        let verifier = db.begin_native_target_image_verification(
            &self.fence,
            &self.admission,
            &self.frozen,
            &self.bindings,
            role,
            &source,
            false,
            cancellation,
        )?;
        drop(db);
        Ok(NativeRangeImageVerification {
            database: self.database,
            admission: self.admission,
            frozen: self.frozen,
            fence: self.fence,
            bindings: self.bindings,
            source,
            verifier,
            history: Some((
                self.safety,
                self.catalog,
                self.history_proof
                    .ok_or_else(|| invalid("complete data has no full history proof"))?,
            )),
            recover_completed: false,
        })
    }
}

fn validate_step(
    step: &RangeImageStepV1,
    cursor: &RangeImageCursorV1,
    chain: &str,
    collections: &[CollectionMeta],
    fence: &TargetImageInstallFence,
    reserved: u64,
) -> Result<()> {
    bounded_json(step, MAX_STEP_BYTES)?;
    let next = &step.next_cursor;
    if step.format_version != 1
        || step.session_id != fence.session_id()
        || step.nonce != fence.nonce()
        || &step.previous_cursor != cursor
        || step.previous_chain_sha256 != chain
        || next.step()
            != cursor
                .step()
                .checked_add(1)
                .ok_or_else(|| invalid("cursor overflow"))?
        || cursor.collection_index() >= collections.len()
        || step.snapshot_db_sequence < reserved
        || next.collection_index() > collections.len()
        || next.collection_index() < cursor.collection_index()
        || next.collection_index() > cursor.collection_index() + 1
        || step.complete != (next.collection_index() == collections.len())
    {
        return Err(invalid(
            "source step/cursor/session/snapshot scope mismatch",
        ));
    }
    if next.collection_index() == cursor.collection_index() {
        if next.after_key() <= cursor.after_key() {
            return Err(invalid("source cursor failed to advance"));
        }
    } else if next.after_key().is_some() {
        return Err(invalid("next collection must begin at its first key"));
    }
    let mut unsigned = step.clone();
    unsigned.chain_sha256.clear();
    if hex::encode(Sha256::digest(bounded_json(&unsigned, MAX_STEP_BYTES)?)) != step.chain_sha256 {
        return Err(invalid("source step checksum mismatch"));
    }
    if let Some(batch) = &step.batch {
        let name = &collections[cursor.collection_index()].name;
        if &batch.collection != name
            || batch.range_id != fence.range().id
            || batch.range_epoch != fence.range().epoch
            || batch.snapshot_commit_sequence != step.snapshot_db_sequence
            || batch.records.is_empty()
            || batch.records.len() > MAX_RECORDS
            || batch.serialized_record_bytes > MAX_BYTES
            || batch.resume_after_key.as_str() <= cursor.after_key().unwrap_or("")
        {
            return Err(invalid("source batch scope/bounds mismatch"));
        }
        let mut previous = cursor.after_key();
        let mut bytes = 0usize;
        for record in &batch.records {
            if previous.is_some_and(|last| record.id.as_str() <= last)
                || record.id.as_str() > batch.resume_after_key.as_str()
                || !fence
                    .range()
                    .contains_token(distribution_key_token(name, &record.id))
            {
                return Err(invalid(
                    "source records unordered or outside authorized range/cursor",
                ));
            }
            bytes = bytes
                .checked_add(bounded_json(record, MAX_BYTES)?.len())
                .ok_or_else(|| invalid("batch byte overflow"))?;
            previous = Some(&record.id);
        }
        if bytes != batch.serialized_record_bytes || bytes > MAX_BYTES {
            return Err(invalid("source batch actual byte bound mismatch"));
        }
        if next.collection_index() == cursor.collection_index()
            && next.after_key() != Some(batch.resume_after_key.as_str())
        {
            return Err(invalid("source batch/cursor resume mismatch"));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::{RangeImageBirthMember, RangeImageBirthSpec};
    use crate::distribution::*;
    use crate::distribution_range_handoff::{HandoffCutRound, HandoffPromiseAck};
    use crate::distribution_range_image::RangeImageCapture;
    use crate::distribution_range_recovery::{test_handoff_role_admission, HandoffRoleAdmission};
    use crate::{DbConfig, StorageMode};
    use std::time::Duration;

    fn node(name: &str) -> ClusterNodeId {
        ClusterNodeId::new(name).unwrap()
    }
    fn config() -> DbConfig {
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }
    pub(crate) struct Fixture {
        pub(crate) root: tempfile::TempDir,
        pub(crate) topology: ClusterTopology,
        pub(crate) source: Arc<RwLock<BicDb>>,
        pub(crate) target: Option<Arc<RwLock<BicDb>>>,
        pub(crate) target_spec: RangeImageBirthSpec,
        pub(crate) source_spec: RangeImageBirthSpec,
        pub(crate) capture: Option<RangeImageCapture>,
    }
    impl Fixture {
        pub(crate) fn new() -> Self {
            Self::new_with_ranges_and_before_handoff(1, |_, _, _, _| {})
        }
        pub(crate) fn new_with_ranges_and_before_handoff(
            range_count: u32,
            before_handoff: impl FnOnce(
                &Arc<RwLock<BicDb>>,
                &ClusterTopology,
                &RangeImageBirthSpec,
                &std::path::Path,
            ),
        ) -> Self {
            let root = tempfile::tempdir().unwrap();
            let mut metadata = DistributionStore::initialize_at(
                root.path().join("topology"),
                DistributionConfig {
                    enabled: true,
                    cluster_id: ClusterId::new("actual-native-r0-install-test").unwrap(),
                    node_id: node("n1"),
                    node_address: "127.0.0.1:10001".into(),
                    initial_ranges: range_count,
                    replication_factor: 1,
                    ..Default::default()
                },
                true,
                1,
            )
            .unwrap();
            for (id, port) in [("n2", 10002), ("n3", 10003)] {
                metadata
                    .join_node(
                        ClusterNode::new(node(id), format!("127.0.0.1:{port}"), 1, 1_000_000, 2)
                            .unwrap(),
                        &node("n1"),
                        2,
                    )
                    .unwrap();
            }
            let original_topology = metadata.topology().clone();
            let originals = original_topology
                .ranges
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let original = originals[0].clone();
            metadata
                .apply_rebalance_plan(
                    &RebalancePlan {
                        id: "native-r0-genuine-plan".into(),
                        topology_generation: metadata.topology().generation,
                        created_at_ms: 3,
                        replica_moves: vec![PlannedReplicaMove {
                            range_id: original.id,
                            expected_epoch: original.epoch,
                            source: Some(node("n1")),
                            target: node("n2"),
                            estimated_bytes: 0,
                            reason: ReplicaMoveReason::LoadBalance,
                        }],
                        leader_transfers: vec![],
                        unplaced_ranges: vec![],
                        estimated_bytes_in_flight: 0,
                    },
                    &node("n1"),
                    3,
                )
                .unwrap();
            let mut topology = metadata.topology().clone();
            topology.range_image_bootstrap_id = Some(Uuid::now_v7());
            topology.validate().unwrap();
            let spec = |id: &str, initial_ranges| RangeImageBirthSpec {
                cluster_id: topology.cluster_id.clone(),
                bootstrap_id: topology.range_image_bootstrap_id.unwrap(),
                node_id: node(id),
                member_incarnation: topology.nodes[&node(id)].incarnation,
                initial_members: topology
                    .nodes
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.clone(),
                            RangeImageBirthMember {
                                incarnation: member.incarnation,
                                tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                                pending_tls_certificate_sha256: member
                                    .pending_tls_certificate_sha256
                                    .clone(),
                            },
                        )
                    })
                    .collect(),
                restore_activation: None,
                initial_ranges,
            };
            let source_spec = spec("n1", originals.clone());
            let (mut source, _) = BicDb::create_new_range_image_database(
                &root.path().join("source"),
                config(),
                source_spec.clone(),
            )
            .unwrap();
            source.create_collection("items").unwrap();
            // As in the existing actual SourceCapture fixture, provision the
            // fresh schema, then reopen its real paged namespace before minting
            // any native genesis/image proof. No paged_lazy flag is forged.
            drop(source);
            let (source, genesis) = BicDb::open_range_image_database(
                &root.path().join("source"),
                config(),
                &source_spec,
            )
            .unwrap();
            let target_spec = spec("n2", vec![]);
            let (mut target, _) = BicDb::create_new_range_image_database(
                &root.path().join("target"),
                config(),
                target_spec.clone(),
            )
            .unwrap();
            target.create_collection("items").unwrap();
            drop(target);
            let (target, _) = BicDb::open_range_image_database(
                &root.path().join("target"),
                config(),
                &target_spec,
            )
            .unwrap();
            let schema = source.schema_compatibility_fingerprint().unwrap().sha256;
            assert_eq!(
                schema,
                target.schema_compatibility_fingerprint().unwrap().sha256
            );
            for member in topology.nodes.values_mut() {
                member
                    .labels
                    .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.into(), schema.clone());
            }
            let source = Arc::new(RwLock::new(source));
            for initial_range in &originals {
                let frozen = source
                    .read()
                    .range_mutation_gate()
                    .try_freeze(initial_range.id, Uuid::now_v7())
                    .unwrap();
                BicDb::initialize_range_image_genesis(
                    &source,
                    &genesis,
                    initial_range,
                    &frozen,
                    Duration::from_secs(3),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            }
            // Actual native schema/genesis exists, but no permanent handoff
            // promise has been installed: genuine ordinary admission is legal.
            let mut origin_topology = topology.clone();
            origin_topology.ranges = original_topology.ranges.clone();
            origin_topology.relocations.clear();
            before_handoff(&source, &origin_topology, &source_spec, root.path());
            let mut fixture = Self {
                root,
                topology,
                source,
                target: Some(Arc::new(RwLock::new(target))),
                target_spec,
                source_spec,
                capture: None,
            };
            fixture.recapture();
            fixture
        }
        pub(crate) fn range(&self) -> &RangeDescriptor {
            self.topology.ranges.values().next().unwrap()
        }
        fn relocation(&self) -> crate::RelocationId {
            *self.topology.relocations.keys().next().unwrap()
        }
        pub(crate) fn target(&self) -> Arc<RwLock<BicDb>> {
            Arc::clone(self.target.as_ref().unwrap())
        }
        pub(crate) fn role(&self) -> HandoffRoleAdmission {
            test_handoff_role_admission(
                &self
                    .root
                    .path()
                    .join(format!("fresh-role-{}", Uuid::now_v7())),
                &self.topology,
                &node("n1"),
                &node("n1"),
                &node("n2"),
                self.relocation(),
                Duration::from_secs(10),
            )
            .unwrap()
        }
        fn recapture(&mut self) {
            assert!(self.capture.is_none());
            let mut round = HandoffCutRound::begin(
                crate::distribution_range_handoff::tests::authority(
                    &self
                        .root
                        .path()
                        .join(format!("source-metadata-{}", Uuid::now_v7())),
                    &self.topology,
                ),
                self.relocation(),
            )
            .unwrap();
            let mut store = RangeWriteStore::open(
                self.source.read().data_path(),
                self.topology.cluster_id.clone(),
                node("n1"),
                true,
            )
            .unwrap();
            let promise = store.install_handoff_promise(round.request()).unwrap();
            round
                .record(
                    HandoffPromiseAck::from_transport(
                        round.request(),
                        &self.topology.nodes[&node("n1")],
                        None,
                        true,
                        promise,
                    )
                    .unwrap(),
                )
                .unwrap();
            let cut = round.finish().unwrap();
            let lineage = self
                .source
                .read()
                .current_native_range_source_lineage(self.range().id)
                .unwrap();
            assert_eq!(
                cut.range_index(),
                lineage.range_index(),
                "actual native and range-store cut must agree"
            );
            let frozen = self
                .source
                .read()
                .range_mutation_gate()
                .try_freeze(self.range().id, cut.session_id())
                .unwrap();
            self.capture = Some(
                RangeImageCapture::begin(
                    Arc::clone(&self.source),
                    cut,
                    frozen,
                    lineage,
                    Duration::from_secs(3),
                    &CancellationToken::uncancelable(),
                )
                .unwrap(),
            );
        }
        pub(crate) fn store(&self) -> RangeWriteStore {
            RangeWriteStore::open(
                self.root.path().join("target"),
                self.topology.cluster_id.clone(),
                node("n2"),
                true,
            )
            .unwrap()
        }
        fn begin(&self, store: &mut RangeWriteStore, attempt: Uuid) -> NativeRangeImageInstaller {
            self.try_begin(store, attempt).unwrap()
        }
        fn try_begin(
            &self,
            store: &mut RangeWriteStore,
            attempt: Uuid,
        ) -> Result<NativeRangeImageInstaller> {
            let target = self.target();
            let admission = target
                .read()
                .native_recovery_admission(
                    &self.topology.cluster_id,
                    self.topology.range_image_bootstrap_id.unwrap(),
                    &node("n2"),
                    self.target_spec.member_incarnation,
                )
                .unwrap();
            let frozen = target
                .read()
                .range_mutation_gate()
                .try_freeze(self.range().id, attempt)
                .unwrap();
            let role = self.role();
            let authorization = self
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            NativeRangeImageInstaller::begin(
                target,
                store,
                admission,
                &authorization,
                &NativeImageInstallRole::CommittedHandoff(&role),
                frozen,
                ImageInstallPurpose::RelocationLearner,
                attempt,
            )
        }
        fn clear(&mut self, store: &RangeWriteStore, install: &mut NativeRangeImageInstaller) {
            // Complete and audit the ACTUAL inactive target history before
            // any stale row removal, even for empty R0 sources.
            loop {
                let step = {
                    let capture = self.capture.as_mut().unwrap();
                    let cursor = capture.history_cursor().clone();
                    capture
                        .export_history_step(&cursor, 256, &CancellationToken::uncancelable())
                        .unwrap()
                };
                let role = self.role();
                let auth = self
                    .capture
                    .as_ref()
                    .unwrap()
                    .authorize_target_install()
                    .unwrap();
                let progress = install
                    .stage_history_step(
                        store,
                        &NativeImageInstallRole::CommittedHandoff(&role),
                        &auth,
                        install.history_stage_id(),
                        &step,
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap();
                if progress.cursor.complete() {
                    break;
                }
            }
            loop {
                let role = self.role();
                let auth = self
                    .capture
                    .as_ref()
                    .unwrap()
                    .authorize_target_install()
                    .unwrap();
                if install
                    .audit_history_step(
                        store,
                        &NativeImageInstallRole::CommittedHandoff(&role),
                        &auth,
                        256,
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap()
                    .audit_complete
                {
                    break;
                }
            }
            loop {
                let role = self.role();
                let auth = self
                    .capture
                    .as_ref()
                    .unwrap()
                    .authorize_target_install()
                    .unwrap();
                if install
                    .clear_step(
                        store,
                        &NativeImageInstallRole::CommittedHandoff(&role),
                        &auth,
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap()
                {
                    break;
                }
            }
        }
        fn copy(&mut self, store: &RangeWriteStore, install: &mut NativeRangeImageInstaller) {
            loop {
                let capture = self.capture.as_mut().unwrap();
                let cursor = capture.cursor().clone();
                let step = capture
                    .export_step(&cursor, &options(), &CancellationToken::uncancelable())
                    .unwrap();
                let role = self.role();
                let auth = self
                    .capture
                    .as_ref()
                    .unwrap()
                    .authorize_target_install()
                    .unwrap();
                install
                    .install_step(
                        install.fence().attempt_id(),
                        store,
                        &NativeImageInstallRole::CommittedHandoff(&role),
                        &auth,
                        &step,
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap();
                if step.complete {
                    break;
                }
            }
        }
        fn export_history(&mut self) {
            loop {
                let capture = self.capture.as_mut().unwrap();
                let cursor = capture.history_cursor().clone();
                if capture
                    .export_history_step(&cursor, 256, &CancellationToken::uncancelable())
                    .unwrap()
                    .next_cursor
                    .complete()
                {
                    break;
                }
            }
        }
        fn reopen_source(&mut self) {
            assert!(self.capture.is_none());
            let temporary = self.target();
            let old = std::mem::replace(&mut self.source, temporary);
            assert_eq!(Arc::strong_count(&old), 1);
            drop(
                Arc::try_unwrap(old)
                    .unwrap_or_else(|_| panic!("source still owned"))
                    .into_inner(),
            );
            let (db, _) = BicDb::open_range_image_database(
                &self.root.path().join("source"),
                config(),
                &self.source_spec,
            )
            .unwrap();
            db.schema_compatibility_fingerprint().unwrap();
            self.source = Arc::new(RwLock::new(db));
        }
        fn reopen_target(&mut self) {
            let old = self.target.take().unwrap();
            assert_eq!(Arc::strong_count(&old), 1);
            drop(
                Arc::try_unwrap(old)
                    .unwrap_or_else(|_| panic!("target still owned"))
                    .into_inner(),
            );
            let (db, _) = BicDb::open_range_image_database(
                &self.root.path().join("target"),
                config(),
                &self.target_spec,
            )
            .unwrap();
            db.schema_compatibility_fingerprint().unwrap();
            self.target = Some(Arc::new(RwLock::new(db)));
        }
    }

    fn finish_verification(
        fixture: &Fixture,
        store: &RangeWriteStore,
        mut verification: NativeRangeImageVerification,
    ) -> VerifiedNativeRangeImageInstall {
        let mut complete = false;
        for _ in 0..128 {
            let role = fixture.role();
            if verification
                .advance(
                    store,
                    &NativeImageInstallRole::CommittedHandoff(&role),
                    &CancellationToken::uncancelable(),
                )
                .unwrap()
                == NativeTargetImageVerificationProgress::Complete
            {
                complete = true;
                break;
            }
        }
        assert!(complete, "bounded R0 fixture verification did not complete");
        let role = fixture.role();
        verification
            .finish(
                store,
                &NativeImageInstallRole::CommittedHandoff(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap()
    }
    fn verify_install(
        fixture: &Fixture,
        store: &RangeWriteStore,
        install: NativeRangeImageInstaller,
        source: CompletedNativeRangeImageSource,
    ) -> VerifiedNativeRangeImageInstall {
        let role = fixture.role();
        let verification = install
            .begin_verification(
                store,
                &NativeImageInstallRole::CommittedHandoff(&role),
                source,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        finish_verification(fixture, store, verification)
    }

    fn actual_catalogue_adoption_fixture(
    ) -> (Fixture, RangeWriteStore, VerifiedNativeRangeImageInstall) {
        let mut fixture =
            Fixture::new_with_ranges_and_before_handoff(1, |db, topology, spec, _| {
                let range = topology.ranges.values().next().unwrap().id;
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    range,
                    1,
                );
            });
        let mut store = fixture.store();
        let mut install = fixture.begin(&mut store, Uuid::now_v7());
        fixture.clear(&store, &mut install);
        fixture.copy(&store, &mut install);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, install, source);
        assert_eq!(verified.base().range_index(), 1);
        assert_eq!(store.progress(fixture.range().id).resolved_through, 0);
        (fixture, store, verified)
    }

    #[test]
    fn actual_native_image_catalogue_adoption_folds_real_rows_but_keeps_every_admission_refused() {
        let (fixture, mut store, verified) = actual_catalogue_adoption_fixture();
        let physical_receipt = verified.base().native_receipt_sha256().to_string();
        let role = fixture.role();
        let image_role = NativeImageInstallRole::CommittedHandoff(&role);
        let adopted = verified
            .adopt_catalogue(&mut store, &image_role, &CancellationToken::uncancelable())
            .unwrap();
        adopted.require_current(&store, &image_role).unwrap();
        let progress = store.progress(fixture.range().id);
        assert_eq!(
            (
                progress.last_index,
                progress.resolved_through,
                progress.compacted_through
            ),
            (1, 1, 1)
        );
        assert_eq!(adopted.catalogue().range_index(), 1);
        assert_eq!(adopted.base().native_receipt_sha256(), physical_receipt);
        assert!(fixture
            .target()
            .read()
            .get("items", "stage-actual-origin-0")
            .unwrap()
            .is_some());
        assert!(store.require_no_image_install(fixture.range().id).is_err());
        assert!(fixture
            .target()
            .read()
            .range_mutation_gate()
            .try_enter(fixture.range().id)
            .is_err());
        let command = crate::distribution_range_consensus::native_ready_test_command(
            fixture.topology.cluster_id.clone(),
            fixture.range(),
            2,
            fixture.range().leader.clone(),
            &crate::CommitAdmissionIntent {
                transaction_id: 1234567,
                mutations: vec![crate::CommitAdmissionMutation {
                    collection: "items".into(),
                    record_id: "still-refused".into(),
                    record: Some(crate::Record::new("still-refused")),
                }],
            },
        )
        .unwrap();
        assert!(
            store.prepare(command).is_err(),
            "adopting a real image must not enable ordinary writes"
        );
        let inspection = RangeWriteStore::inspect(
            fixture.root.path().join("target"),
            fixture.topology.cluster_id.clone(),
            node("n2"),
            64 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(inspection.format_version, 6);
    }

    #[test]
    fn actual_native_image_catalogue_adoption_retry_is_idempotent_without_reapplying_rows() {
        let (fixture, mut store, verified) = actual_catalogue_adoption_fixture();
        let role = fixture.role();
        let image_role = NativeImageInstallRole::CommittedHandoff(&role);
        let before_sequence = fixture.target().read().current_commit_seq();
        let first = store
            .adopt_verified_native_image_catalogue(&verified, &image_role)
            .unwrap();
        let second = store
            .adopt_verified_native_image_catalogue(&verified, &image_role)
            .unwrap();
        first
            .require_current(&store, &verified, &image_role)
            .unwrap();
        second
            .require_current(&store, &verified, &image_role)
            .unwrap();
        assert_eq!(
            first.native_receipt_sha256(),
            second.native_receipt_sha256()
        );
        assert_eq!(
            fixture.target().read().current_commit_seq(),
            before_sequence
        );
        drop(store);
        let reopened = fixture.store();
        assert!(
            first
                .require_current(&reopened, &verified, &image_role)
                .is_err(),
            "a new store-open must reissue actual proof"
        );
        assert_eq!(reopened.progress(fixture.range().id).resolved_through, 1);
        assert!(reopened
            .require_no_image_install(fixture.range().id)
            .is_err());
    }

    #[test]
    fn actual_native_image_catalogue_adoption_ambiguous_persist_needs_real_refsync_before_receipt()
    {
        let (fixture, mut store, verified) = actual_catalogue_adoption_fixture();
        let role = fixture.role();
        let image_role = NativeImageInstallRole::CommittedHandoff(&role);
        let physical_receipt = verified.base().native_receipt_sha256().to_string();
        let error = store
            .test_adopt_native_image_catalogue_after_durable_rename_failure(&verified, &image_role)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected catalogue acknowledgment error"),
            "{error}"
        );
        assert_eq!(store.progress(fixture.range().id).resolved_through, 1);
        assert!(store.require_no_image_install(fixture.range().id).is_err());
        let receipt = store
            .adopt_verified_native_image_catalogue(&verified, &image_role)
            .unwrap();
        receipt
            .require_current(&store, &verified, &image_role)
            .unwrap();
        assert_eq!(receipt.native_receipt_sha256(), physical_receipt);
        assert!(store.require_no_image_install(fixture.range().id).is_err());
    }

    #[test]
    fn actual_native_image_catalogue_adoption_reopens_and_reissues_same_materialized_snapshot() {
        let (mut fixture, mut store, verified) = actual_catalogue_adoption_fixture();
        let attempt = verified.fence().attempt_id();
        let role = fixture.role();
        let image_role = NativeImageInstallRole::CommittedHandoff(&role);
        let adopted = verified
            .adopt_catalogue(&mut store, &image_role, &CancellationToken::uncancelable())
            .unwrap();
        let original = adopted.base().native_receipt_sha256().to_string();
        drop(adopted);
        drop(store);
        fixture.reopen_target();
        fixture.reopen_source();
        fixture.recapture();
        loop {
            let capture = fixture.capture.as_mut().unwrap();
            let cursor = capture.cursor().clone();
            if capture
                .export_step(&cursor, &options(), &CancellationToken::uncancelable())
                .unwrap()
                .complete
            {
                break;
            }
        }
        fixture.export_history();
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let target = fixture.target();
        let admission = target
            .read()
            .native_recovery_admission(
                &fixture.topology.cluster_id,
                fixture.topology.range_image_bootstrap_id.unwrap(),
                &node("n2"),
                fixture.target_spec.member_incarnation,
            )
            .unwrap();
        let frozen = target
            .read()
            .range_mutation_gate()
            .try_freeze(fixture.range().id, attempt)
            .unwrap();
        let role = fixture.role();
        let image_role = NativeImageInstallRole::CommittedHandoff(&role);
        let mut store = fixture.store();
        let verification = NativeRangeImageInstaller::recover_completed(
            target,
            &mut store,
            admission,
            source,
            &image_role,
            frozen,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        let verified = finish_verification(&fixture, &store, verification);
        let role = fixture.role();
        let image_role = NativeImageInstallRole::CommittedHandoff(&role);
        let reissued = verified
            .adopt_catalogue(&mut store, &image_role, &CancellationToken::uncancelable())
            .unwrap();
        reissued.require_current(&store, &image_role).unwrap();
        assert_eq!(reissued.base().native_receipt_sha256(), original);
        assert_eq!(store.progress(fixture.range().id).resolved_through, 1);
        assert!(fixture
            .target()
            .read()
            .get("items", "stage-actual-origin-0")
            .unwrap()
            .is_some());
        assert!(store.require_no_image_install(fixture.range().id).is_err());
    }

    #[test]
    fn actual_native_r0_copy_removes_extra_rows_and_preserves_target_namespace_identity() {
        let mut fixture = Fixture::new();
        let target = fixture.target();
        let target_uid = target
            .read()
            .local_namespace_identity("items")
            .unwrap()
            .unwrap();
        let source_uid = fixture
            .source
            .read()
            .local_namespace_identity("items")
            .unwrap()
            .unwrap();
        assert_ne!(target_uid, source_uid);
        // An explicitly unacknowledged stale physical learner record. It is
        // not a client commit or a manufactured native history certificate.
        let stale = crate::Record::new("extra-stale");
        target
            .read()
            .apply_range_snapshot_batch_preinstalled(
                fixture.range(),
                &CollectionMeta::standard("items"),
                &RangeSnapshotBatch {
                    collection: "items".into(),
                    range_id: fixture.range().id,
                    range_epoch: fixture.range().epoch,
                    snapshot_commit_sequence: 0,
                    resume_after_key: stale.id.clone(),
                    serialized_record_bytes: serde_json::to_vec(&stale).unwrap().len(),
                    records: vec![stale],
                },
            )
            .unwrap();
        drop(target);
        let mut store = fixture.store();
        let mut install = fixture.begin(&mut store, Uuid::now_v7());
        assert!(
            fixture
                .target()
                .read()
                .native_range_prefix_observation(fixture.range().id)
                .unwrap()
                .invalidated
        );
        fixture.clear(&store, &mut install);
        fixture.copy(&store, &mut install);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, install, source);
        assert_eq!(verified.base().range_index(), 0);
        assert_eq!(
            verified.base().local_namespace_ids(),
            &[("items".to_string(), target_uid)]
        );
        assert!(fixture
            .target()
            .read()
            .get("items", "extra-stale")
            .unwrap()
            .is_none());
        assert!(
            store.require_no_image_install(fixture.range().id).is_err(),
            "physical receipt grants neither activation nor range catalog readiness"
        );
        assert!(fixture
            .target()
            .read()
            .range_mutation_gate()
            .try_enter(fixture.range().id)
            .is_err());
        drop(verified);
        fixture.reopen_target();
        assert_eq!(
            fixture
                .target()
                .read()
                .local_namespace_identity("items")
                .unwrap(),
            Some(target_uid)
        );
        assert!(fixture
            .store()
            .require_no_image_install(fixture.range().id)
            .is_err());
    }
    #[test]
    fn actual_native_r1_install_uses_target_local_visibility_and_preserves_other_range_rows() {
        let mut fixture =
            Fixture::new_with_ranges_and_before_handoff(2, |db, topology, spec, _| {
                let ranges = topology
                    .ranges
                    .values()
                    .map(|range| range.id)
                    .collect::<Vec<_>>();
                // Genuine independent range commits distinguish global DB sequence
                // from the selected range's one command. No sequence is assigned.
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    ranges[1],
                    4,
                );
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    ranges[0],
                    1,
                );
            });
        let target = fixture.target();
        let source_r = fixture
            .capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap()
            .cut()
            .range_index();
        assert_eq!(source_r, 1);
        let source_b = fixture.source.read().current_commit_seq();
        assert_ne!(source_b, source_r);
        let selected = fixture.range().clone();
        let other = fixture
            .topology
            .ranges
            .values()
            .find(|r| r.id != selected.id)
            .unwrap()
            .clone();
        let source_key = (0..)
            .map(|i| format!("stage-actual-origin-{i}"))
            .find(|key| selected.contains_token(distribution_key_token("items", key)))
            .unwrap();
        let expected = fixture
            .source
            .read()
            .get("items", &source_key)
            .unwrap()
            .unwrap();
        let target_uid = target.read().local_namespace_identity("items").unwrap();
        let mut retained = Vec::new();
        // These are explicitly unacknowledged learner physical writes. Each
        // real local transaction advances the target's own MVCC sequence.
        for key in (0..)
            .map(|i| format!("other-target-{i}"))
            .filter(|key| other.contains_token(distribution_key_token("items", key)))
            .take(17)
        {
            let row =
                crate::Record::new(key.clone()).with_metadata(serde_json::json!({"retained":true}));
            target
                .read()
                .apply_range_snapshot_batch_preinstalled(
                    &other,
                    &CollectionMeta::standard("items"),
                    &RangeSnapshotBatch {
                        collection: "items".into(),
                        range_id: other.id,
                        range_epoch: other.epoch,
                        snapshot_commit_sequence: 0,
                        resume_after_key: key.clone(),
                        serialized_record_bytes: serde_json::to_vec(&row).unwrap().len(),
                        records: vec![row],
                    },
                )
                .unwrap();
            retained.push(key);
        }
        let stale =
            crate::Record::new(source_key.clone()).with_metadata(serde_json::json!({"stale":true}));
        target
            .read()
            .apply_range_snapshot_batch_preinstalled(
                &selected,
                &CollectionMeta::standard("items"),
                &RangeSnapshotBatch {
                    collection: "items".into(),
                    range_id: selected.id,
                    range_epoch: selected.epoch,
                    snapshot_commit_sequence: 0,
                    resume_after_key: source_key.clone(),
                    serialized_record_bytes: serde_json::to_vec(&stale).unwrap().len(),
                    records: vec![stale],
                },
            )
            .unwrap();
        let target_before = target.read().current_commit_seq();
        assert!(target_before > source_b && target_before > source_r);
        drop(target);
        let mut store = fixture.store();
        let mut install = fixture.begin(&mut store, Uuid::now_v7());
        {
            let role = fixture.role();
            let auth = fixture
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            assert!(
                install
                    .clear_step(
                        &store,
                        &NativeImageInstallRole::CommittedHandoff(&role),
                        &auth,
                        &CancellationToken::uncancelable()
                    )
                    .is_err(),
                "no destructive clear before actual full history audit"
            );
        }
        fixture.clear(&store, &mut install);
        fixture.copy(&store, &mut install);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, install, source);
        assert_eq!(verified.base().range_index(), source_r);
        assert!(verified.base().target_snapshot_bound().get() > target_before);
        assert_ne!(verified.base().target_snapshot_bound().get(), source_b);
        assert_eq!(verified.base().history_verified_through(), source_r);
        {
            let db = fixture.target();
            let db = db.read();
            assert_eq!(
                db.get("items", &source_key).unwrap().unwrap().as_ref(),
                expected.as_ref(),
                "ordinary target MVCC get must see the exact copied value"
            );
            for key in &retained {
                assert_eq!(
                    db.get("items", key).unwrap().unwrap().metadata,
                    serde_json::json!({"retained":true})
                );
            }
            assert_eq!(db.local_namespace_identity("items").unwrap(), target_uid);
        }
        drop(verified);
        fixture.reopen_target();
        assert_eq!(
            fixture
                .target()
                .read()
                .get("items", &source_key)
                .unwrap()
                .unwrap()
                .as_ref(),
            expected.as_ref()
        );
        for key in retained {
            assert!(fixture
                .target()
                .read()
                .get("items", &key)
                .unwrap()
                .is_some());
        }
        assert!(
            fixture
                .store()
                .require_no_image_install(selected.id)
                .is_err(),
            "native image is not membership/catalog activation"
        );
    }

    #[test]
    fn actual_native_r1025_install_preserves_full_history_after_command_payload_compaction() {
        let mut fixture =
            Fixture::new_with_ranges_and_before_handoff(2, |db, topology, spec, _| {
                let ranges = topology.ranges.values().map(|r| r.id).collect::<Vec<_>>();
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    ranges[1],
                    3,
                );
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    ranges[0],
                    1025,
                );
            });
        let range_id = fixture.range().id;
        let source_b = fixture.source.read().current_commit_seq();
        assert!(
            source_b > 1025,
            "actual other-range commits make DBseq distinct from R"
        );
        let source_store = RangeWriteStore::open(
            fixture.source.read().data_path(),
            fixture.topology.cluster_id.clone(),
            node("n1"),
            true,
        )
        .unwrap();
        let progress = source_store.progress(range_id);
        assert_eq!(progress.resolved_through, 1025);
        assert_eq!(
            progress.compacted_through, 1,
            "actual first command payload has passed normal retention"
        );
        let obligations = source_store
            .image_install_catalog_history_obligations(range_id)
            .unwrap();
        assert_eq!(obligations.irrevocable_commands().len(), 1024);
        drop(obligations);
        drop(source_store);
        let keys = (0..)
            .map(|i| format!("stage-actual-origin-{i}"))
            .filter(|key| {
                fixture
                    .range()
                    .contains_token(distribution_key_token("items", key))
            })
            .take(1025)
            .collect::<Vec<_>>();
        for key in &keys {
            assert_eq!(
                fixture
                    .source
                    .read()
                    .get("items", key)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &crate::Record::new(key)
            );
        }
        let target_uid = fixture
            .target()
            .read()
            .local_namespace_identity("items")
            .unwrap();
        let mut store = fixture.store();
        let mut install = fixture.begin(&mut store, Uuid::now_v7());
        fixture.clear(&store, &mut install);
        fixture.copy(&store, &mut install);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        assert_eq!(source.seal().range_index, 1025);
        assert_eq!(source.seal().history_entries, 1026);
        assert!(
            source.seal().history_steps >= 5,
            "bounded history includes genesis and all commands"
        );
        let verified = verify_install(&fixture, &store, install, source);
        assert_eq!(verified.base().range_index(), 1025);
        assert_eq!(verified.base().history_verified_through(), 1025);
        assert!(verified.base().target_snapshot_bound().get() < 1025,
            "target uses its own actual small local MVCC bound, never source range index or source DBseq");
        {
            let db = fixture.target();
            let db = db.read();
            for key in &keys {
                assert_eq!(
                    db.get("items", key).unwrap().unwrap().as_ref(),
                    &crate::Record::new(key),
                    "ordinary target MVCC get must see every copied row after payload retention"
                );
            }
            assert_eq!(db.local_namespace_identity("items").unwrap(), target_uid);
        }
        drop(verified);
        fixture.reopen_target();
        for key in keys {
            assert_eq!(
                fixture
                    .target()
                    .read()
                    .get("items", &key)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &crate::Record::new(key)
            );
        }
        assert!(
            fixture.store().require_no_image_install(range_id).is_err(),
            "physical copy and complete history do not activate membership or catalog floor"
        );
    }

    #[test]
    fn actual_installed_r1_floor_survives_physical_invalidation_and_reopen_without_readiness() {
        let mut fixture =
            Fixture::new_with_ranges_and_before_handoff(2, |db, topology, spec, _| {
                let selected = topology.ranges.values().next().unwrap().id;
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    selected,
                    1,
                );
            });
        let mut store = fixture.store();
        let mut install = fixture.begin(&mut store, Uuid::now_v7());
        fixture.clear(&store, &mut install);
        fixture.copy(&store, &mut install);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, install, source);
        let receipt = verified.base().native_receipt_sha256().to_string();
        let history = verified.base().logical_history_sha256().to_string();
        drop(verified);
        {
            let target = fixture.target();
            let db = target.read();
            let extra = (0..)
                .map(|i| format!("after-image-unacknowledged-{i}"))
                .find(|key| {
                    fixture
                        .range()
                        .contains_token(distribution_key_token("items", key))
                })
                .unwrap();
            let row = crate::Record::new(extra.clone());
            // Trusted physical learner mutation, explicitly NOT an acknowledged
            // quorum commit or a new native certified prefix. It invalidates
            // current source contents without erasing the prior durable floor.
            db.apply_range_snapshot_batch_preinstalled(
                fixture.range(),
                &CollectionMeta::standard("items"),
                &RangeSnapshotBatch {
                    collection: "items".into(),
                    range_id: fixture.range().id,
                    range_epoch: fixture.range().epoch,
                    snapshot_commit_sequence: 0,
                    resume_after_key: extra,
                    serialized_record_bytes: serde_json::to_vec(&row).unwrap().len(),
                    records: vec![row],
                },
            )
            .unwrap();
        }
        for reopen in [false, true] {
            if reopen {
                fixture.reopen_target();
            }
            let target = fixture.target();
            let db = target.read();
            let native = db
                .native_recovery_admission(
                    &fixture.topology.cluster_id,
                    fixture.topology.range_image_bootstrap_id.unwrap(),
                    &node("n2"),
                    fixture.target_spec.member_incarnation,
                )
                .unwrap();
            assert!(
                db.native_installed_range_image_base(&native, fixture.range().id)
                    .is_err(),
                "invalidated image must not remain a source/ready anchor"
            );
            let integrity = db
                .native_installed_range_image_integrity(&native, fixture.range().id)
                .unwrap()
                .unwrap();
            assert_eq!(integrity.range_index(), 1);
            assert_eq!(integrity.native_receipt_sha256(), receipt);
            assert_eq!(integrity.logical_history_sha256(), history);
            let frozen = db
                .range_mutation_gate()
                .try_freeze(fixture.range().id, Uuid::now_v7())
                .unwrap();
            let safety = db
                .native_range_install_safety(&native, fixture.range().id, &frozen)
                .unwrap();
            assert!(safety.source_invalidated());
            assert_eq!(safety.maximum_irreversible_index(), Some(1));
            assert_eq!(safety.history_point().unwrap().range_index(), 1);
            assert_eq!(
                safety.history_point().unwrap().logical_history_sha256(),
                history
            );
            safety.require_current(&db, &native, &frozen).unwrap();
            assert!(
                db.native_range_prefix_observation(fixture.range().id)
                    .unwrap()
                    .invalidated
            );
            assert!(store.require_no_image_install(fixture.range().id).is_err());
        }
    }

    #[test]
    fn native_partial_attempt_refuses_recreated_namespace_until_fresh_authorized_attempt() {
        let mut fixture = Fixture::new();
        let original_uid = fixture
            .target()
            .read()
            .local_namespace_identity("items")
            .unwrap();
        let attempt = Uuid::now_v7();
        let mut store = fixture.store();
        let partial = fixture.begin(&mut store, attempt);
        drop(partial);
        drop(store);
        {
            let target = fixture.target();
            let mut db = target.write();
            assert!(db.drop_collection("items").unwrap());
            db.create_collection("items").unwrap();
            assert_ne!(
                db.local_namespace_identity("items").unwrap(),
                original_uid,
                "actual hard drop/recreate has a fresh local namespace lineage"
            );
        }
        fixture.reopen_target();
        let recreated_uid = fixture
            .target()
            .read()
            .local_namespace_identity("items")
            .unwrap();
        let mut store = fixture.store();
        let error = match fixture.try_begin(&mut store, attempt) {
            Err(error) => error,
            Ok(_) => panic!("old attempt must not silently bind the recreated physical backing"),
        };
        assert!(error.to_string().contains("recreated namespace"), "{error}");
        assert!(store.require_no_image_install(fixture.range().id).is_err());
        assert!(
            fixture
                .target()
                .read()
                .native_range_prefix_observation(fixture.range().id)
                .unwrap()
                .invalidated
        );
        // Explicitly restart from a fresh actual role/attempt. No prior receipt,
        // numeric floor or target UUID is transplanted onto the new backing.
        let mut restarted = fixture.begin(&mut store, Uuid::now_v7());
        fixture.clear(&store, &mut restarted);
        fixture.copy(&store, &mut restarted);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, restarted, source);
        assert_eq!(
            fixture
                .target()
                .read()
                .local_namespace_identity("items")
                .unwrap(),
            recreated_uid
        );
        assert_ne!(recreated_uid, original_uid);
        assert!(store.require_no_image_install(fixture.range().id).is_err());
        drop(verified);
        fixture.reopen_target();
        assert_eq!(
            fixture
                .target()
                .read()
                .local_namespace_identity("items")
                .unwrap(),
            recreated_uid
        );
    }

    #[test]
    fn partial_native_attempt_reopen_stays_unready_and_new_attempt_rejects_late_chunks() {
        let mut fixture = Fixture::new();
        let mut store = fixture.store();
        let old_attempt = Uuid::now_v7();
        let mut old = fixture.begin(&mut store, old_attempt);
        fixture.clear(&store, &mut old);
        let capture = fixture.capture.as_mut().unwrap();
        let cursor = capture.cursor().clone();
        let step = capture
            .export_step(&cursor, &options(), &CancellationToken::uncancelable())
            .unwrap();
        drop(old);
        drop(store);
        fixture.reopen_target();
        assert!(
            fixture
                .target()
                .read()
                .native_range_prefix_observation(fixture.range().id)
                .unwrap()
                .invalidated
        );
        let admission = fixture
            .target()
            .read()
            .native_recovery_admission(
                &fixture.topology.cluster_id,
                fixture.topology.range_image_bootstrap_id.unwrap(),
                &node("n2"),
                fixture.target_spec.member_incarnation,
            )
            .unwrap();
        assert!(fixture
            .target()
            .read()
            .native_installed_range_image_base(&admission, fixture.range().id)
            .is_err());
        drop(fixture.capture.take());
        // A real fresh metadata snapshot may carry changed load estimates;
        // these do not change the handoff configuration/session or data proof.
        let range_id = fixture.range().id;
        fixture.topology.generation += 1;
        fixture
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
            .unwrap()
            .approximate_bytes = 999;
        fixture
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
            .unwrap()
            .approximate_qps = 777;
        fixture.topology.validate().unwrap();
        fixture.recapture();
        let mut store = fixture.store();
        let new_attempt = Uuid::now_v7();
        let mut new = fixture.begin(&mut store, new_attempt);
        fixture.clear(&store, &mut new);
        let role = fixture.role();
        assert!(new
            .install_step(
                old_attempt,
                &store,
                &NativeImageInstallRole::CommittedHandoff(&role),
                &fixture
                    .capture
                    .as_ref()
                    .unwrap()
                    .authorize_target_install()
                    .unwrap(),
                &step,
                &CancellationToken::uncancelable()
            )
            .is_err());
        fixture.copy(&store, &mut new);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, new, source);
        assert_eq!(verified.fence().attempt_id(), new_attempt);
    }
    #[test]
    fn native_completed_crash_window_reopens_source_and_target_and_reissues_only_exact_attempt() {
        exercise_completed_handoff_window(0);
    }
    #[test]
    fn native_completed_nonzero_handoff_window_reaudits_history_and_reissues_exact_anchor() {
        exercise_completed_handoff_window(1);
    }
    fn exercise_completed_handoff_window(source_commands: usize) {
        let mut fixture = if source_commands == 0 {
            Fixture::new()
        } else {
            Fixture::new_with_ranges_and_before_handoff(2, |db, topology, spec, _| {
                let ranges = topology
                    .ranges
                    .values()
                    .map(|range| range.id)
                    .collect::<Vec<_>>();
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    ranges[1],
                    2,
                );
                crate::distribution_range_consensus::seed_actual_origin(
                    db,
                    topology.clone(),
                    spec,
                    ranges[0],
                    source_commands,
                );
            })
        };
        let source_b = fixture.source.read().current_commit_seq();
        let expected_key = (0..)
            .map(|index| format!("stage-actual-origin-{index}"))
            .find(|key| {
                fixture
                    .range()
                    .contains_token(distribution_key_token("items", key))
            })
            .unwrap();
        let mut store = fixture.store();
        let attempt = Uuid::now_v7();
        let mut install = fixture.begin(&mut store, attempt);
        fixture.clear(&store, &mut install);
        fixture.copy(&store, &mut install);
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let verified = verify_install(&fixture, &store, install, source);
        let first_receipt = verified.base().native_receipt_sha256().to_string();
        assert_eq!(verified.base().range_index(), source_commands as u64);
        let source_open = fixture
            .source
            .read()
            .capture_range_image_bindings()
            .unwrap()
            .database_incarnation()
            .to_string();
        drop(verified);
        drop(store);
        fixture.reopen_target();
        fixture.reopen_source();
        assert_ne!(
            source_open,
            fixture
                .source
                .read()
                .capture_range_image_bindings()
                .unwrap()
                .database_incarnation()
        );
        fixture.recapture();
        loop {
            let capture = fixture.capture.as_mut().unwrap();
            let cursor = capture.cursor().clone();
            if capture
                .export_step(&cursor, &options(), &CancellationToken::uncancelable())
                .unwrap()
                .complete
            {
                break;
            }
        }
        fixture.export_history();
        let source = fixture.capture.take().unwrap().complete().unwrap();
        let target = fixture.target();
        let admission = target
            .read()
            .native_recovery_admission(
                &fixture.topology.cluster_id,
                fixture.topology.range_image_bootstrap_id.unwrap(),
                &node("n2"),
                fixture.target_spec.member_incarnation,
            )
            .unwrap();
        let frozen = target
            .read()
            .range_mutation_gate()
            .try_freeze(fixture.range().id, attempt)
            .unwrap();
        let role = fixture.role();
        let mut store = fixture.store();
        let recovered = NativeRangeImageInstaller::recover_completed(
            target,
            &mut store,
            admission,
            source,
            &NativeImageInstallRole::CommittedHandoff(&role),
            frozen,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        let recovered = finish_verification(&fixture, &store, recovered);
        assert_eq!(recovered.fence().attempt_id(), attempt);
        assert_eq!(recovered.base().range_index(), source_commands as u64);
        if source_commands > 0 {
            assert!(source_b > source_commands as u64);
            assert_ne!(recovered.base().target_snapshot_bound().get(), source_b);
            assert_eq!(
                fixture
                    .target()
                    .read()
                    .get("items", &expected_key)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &crate::Record::new(expected_key)
            );
        }
        assert_eq!(
            recovered.base().native_receipt_sha256(),
            first_receipt,
            "recovery revalidates the exact already-published native anchor without retiring a second prefix"
        );
        assert!(store.require_no_image_install(fixture.range().id).is_err());
    }

    #[test]
    fn actual_capture_refuses_outstanding_controlled_callback_then_succeeds_after_drain() {
        let mut fixture = Fixture::new();
        drop(fixture.capture.take());
        let mut round = HandoffCutRound::begin(
            crate::distribution_range_handoff::tests::authority(
                &fixture.root.path().join("controlled-source-metadata"),
                &fixture.topology,
            ),
            fixture.relocation(),
        )
        .unwrap();
        let mut store = RangeWriteStore::open(
            fixture.source.read().data_path(),
            fixture.topology.cluster_id.clone(),
            node("n1"),
            true,
        )
        .unwrap();
        let promise = store.install_handoff_promise(round.request()).unwrap();
        round
            .record(
                HandoffPromiseAck::from_transport(
                    round.request(),
                    &fixture.topology.nodes[&node("n1")],
                    None,
                    true,
                    promise,
                )
                .unwrap(),
            )
            .unwrap();
        let cut = round.finish().unwrap();
        let lineage = fixture
            .source
            .read()
            .current_native_range_source_lineage(fixture.range().id)
            .unwrap();
        let gate = fixture.source.read().range_mutation_gate();
        let frozen = gate
            .try_freeze(fixture.range().id, cut.session_id())
            .unwrap();
        let callback = gate.try_controlled_mutation(&frozen).unwrap();
        let result = RangeImageCapture::begin(
            Arc::clone(&fixture.source),
            cut,
            frozen,
            lineage,
            Duration::from_secs(3),
            &CancellationToken::uncancelable(),
        );
        assert!(
            result.is_err(),
            "a frozen owner alone cannot prove its controlled callback drained"
        );
        assert!(gate.try_enter(fixture.range().id).is_err());
        drop(callback);
        fixture.recapture();
        fixture
            .capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap()
            .require_current()
            .unwrap();
    }
}
