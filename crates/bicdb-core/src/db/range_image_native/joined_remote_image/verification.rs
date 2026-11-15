//! Independent bounded target-primary scan. No index parity, floor or Ready.
use super::*;
mod scan_point;
use crate::distribution_range_image::{
    canonical_range_image_catalog, content_hasher, LocalRangeImageSealV1,
};
pub(in crate::db::range_image_native::joined_remote_image) use scan_point::JoinedTargetScanReadPoint;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum JoinedTargetVerificationStatus {
    WaitingForDurability,
    WaitingForVisibility,
    Scanning,
    Complete,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeJoinedRemoteVerificationProgress {
    pub status: JoinedTargetVerificationStatus,
    pub collection: usize,
    pub after_key: Option<String>,
    pub records: u64,
    pub bytes: u64,
    pub target_reserved_b: u64,
}
/// Original-owner scan state, never reconstructed from a source DTO.
#[derive(Clone)]
pub(super) struct JoinedTargetVerifier {
    generation: Uuid,
    database_open: String,
    target_reserved_b: u64,
    image_generations: (Uuid, Uuid),
    index_definitions: Vec<IndexDefinition>,
    seal: LocalRangeImageSealV1,
    rows: rows::JoinedCompletedRows,
    history_header_sha256: String,
    history_generation: Uuid,
    content: sha2::Sha256,
    collection: usize,
    after: Option<String>,
    records: u64,
    bytes: u64,
    min_snapshot_b: Option<u64>,
    complete: bool,
    invalid: bool,
}
/// Produced only by a complete actual target scan and protected source/row
/// checks. Fields describe physical contents, never membership or read grants.
pub(super) struct VerifiedJoinedTargetContents {
    pub(super) generation: Uuid,
    pub(super) target_reserved_b: u64,
    pub(super) min_snapshot_b: u64,
    pub(super) image_generations: (Uuid, Uuid),
    pub(super) index_definitions: Vec<IndexDefinition>,
    pub(super) seal: LocalRangeImageSealV1,
    pub(super) rows: rows::JoinedCompletedRows,
    pub(super) history_header_sha256: String,
    pub(super) history_generation: Uuid,
}
pub(super) fn finished_seal(
    context: &NativeJoinedRemoteImageContext<'_>,
) -> Result<LocalRangeImageSealV1> {
    let NativeRemoteImageSourcePayloadV1::Finished(seal) = context.point.frame().payload()? else {
        return Err(invalid(
            "Joined target verification needs actual authenticated Finished frame",
        ));
    };
    require_seal_manifest(seal, context.point.preflight().source().manifest())?;
    context.point.require_live()?;
    Ok(seal.clone())
}
/// Integrity constraints on immutable published DATA only. Live producers
/// separately require the actual authenticated frame and original A context.
pub(super) fn require_seal_manifest(
    seal: &LocalRangeImageSealV1,
    mf: &crate::distribution_range_image::NativeRemoteImageManifestV1,
) -> Result<()> {
    let history_count = mf
        .range_index
        .checked_add(1)
        .ok_or_else(|| invalid("Joined final history count overflow"))?;
    if seal.format_version != 1
        || seal.session_id != mf.handoff_session
        || seal.nonce != mf.nonce
        || seal.cluster_id != mf.cluster_id
        || seal.bootstrap_id != mf.bootstrap_id
        || seal.range_id != mf.capture_range.id
        || seal.capture_epoch != mf.capture_range.epoch
        || seal.activation_epoch != mf.activation_range.epoch
        || seal.range_index != mf.range_index
        || seal.source_id != mf.source_id
        || seal.source_member_incarnation != mf.source_birth.node_incarnation()
        || seal.source_database_incarnation != mf.source_database_open
        || seal.source_native_receipt_sha256 != mf.source_native_receipt
        || seal.old_config_sha256 != mf.old_config_sha256
        || seal.new_config_sha256 != mf.new_config_sha256
        || seal.schema_sha256 != mf.schema_sha256
        || seal.reader_features != mf.readers
        || seal.portable_catalog_sha256 != mf.portable_catalogue_sha256
        || seal.logical_genesis_sha256 != mf.h0
        || seal.logical_history_sha256 != mf.h
        || seal.reserved_db_upper_bound != mf.source_reserved_b
        || seal.min_snapshot_db_sequence < mf.source_reserved_b
        || seal.max_snapshot_db_sequence < seal.min_snapshot_db_sequence
        || seal.history_entries != history_count
        || seal.local_namespace_ids != mf.source_namespace_ids
        || !matches!(&seal.purpose, crate::distribution_range_image::RangeImageSourcePurposeV1::CommittedHandoff { relocation_id, plan_id }
            if *relocation_id==mf.relocation_id && *plan_id==mf.plan_id)
    {
        return Err(invalid(
            "Joined authenticated Finished seal differs from actual original manifest",
        ));
    }
    Ok(())
}
pub(super) fn actual_indexes(db: &BicDb) -> Result<Vec<IndexDefinition>> {
    let mut definitions = db.index_definitions();
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    bounded_json(&definitions, MAX_NATIVE_RECEIPT_BYTES)?;
    Ok(definitions)
}
impl JoinedTargetVerifier {
    fn require_current(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        seal: &LocalRangeImageSealV1,
        rows: &rows::JoinedCompletedRows,
        history: &HistoryHeader,
    ) -> Result<()> {
        context.require_current(db)?;
        if self.invalid
            || self.database_open != db.incarnation_id()
            || &self.seal != seal
            || &self.rows != rows
            || self.history_header_sha256 != history.checksum_sha256
            || self.history_generation != history.generation
            || self.image_generations
                != db.native_range_image_generation_pair(context.fence.range().id)?
            || bounded_json(&self.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&actual_indexes(db)?, MAX_NATIVE_RECEIPT_BYTES)?
        {
            return Err(invalid("Joined independent target scan changed original image/rows/history/index definitions"));
        }
        Ok(())
    }
    fn progress(
        &self,
        status: JoinedTargetVerificationStatus,
    ) -> NativeJoinedRemoteVerificationProgress {
        NativeJoinedRemoteVerificationProgress {
            status,
            collection: self.collection,
            after_key: self.after.clone(),
            records: self.records,
            bytes: self.bytes,
            target_reserved_b: self.target_reserved_b,
        }
    }
    #[cfg(test)]
    pub(super) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"generation":self.generation,"target_reserved_b":self.target_reserved_b,"image_generations":self.image_generations,
            "collection":self.collection,"after_key":self.after,"records":self.records,"bytes":self.bytes,"complete":self.complete,"invalid":self.invalid,
            "content_sha256":hex::encode(self.content.clone().finalize()),"index_definitions":self.index_definitions,"independent_index_parity":false})
    }
}
impl NativeJoinedRemoteImageInstallation {
    pub(crate) fn begin_verification(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<NativeJoinedRemoteVerificationProgress> {
        self.require_owner(db, context)?;
        let seal = finished_seal(context)?;
        let rows = self.completed_rows(db, context, &seal)?;
        if self.verification.is_none() {
            let mf = context.point.preflight().source().manifest();
            let catalog = canonical_range_image_catalog(
                &mf.cluster_id,
                &mf.capture_range,
                mf.range_index,
                context.point.preflight().bindings(),
            )?;
            if hex::encode(sha2::Sha256::digest(&catalog)) != seal.portable_catalog_sha256 {
                return Err(invalid(
                    "Joined target canonical catalogue differs from Finished seal",
                ));
            }
            self.verification = Some(JoinedTargetVerifier {
                generation: Uuid::now_v7(),
                database_open: db.incarnation_id().into(),
                target_reserved_b: db.current_commit_seq(),
                image_generations: db.native_range_image_generation_pair(mf.capture_range.id)?,
                index_definitions: actual_indexes(db)?,
                seal: seal.clone(),
                rows: rows.clone(),
                history_header_sha256: self.header.checksum_sha256.clone(),
                history_generation: self.header.generation,
                content: content_hasher(&catalog),
                collection: 0,
                after: None,
                records: 0,
                bytes: 0,
                min_snapshot_b: None,
                complete: mf.collections.is_empty(),
                invalid: false,
            });
        }
        let state = self.verification.as_ref().unwrap();
        state.require_current(db, context, &seal, &rows, &self.header)?;
        Ok(state.progress(if state.complete {
            JoinedTargetVerificationStatus::Complete
        } else {
            JoinedTargetVerificationStatus::Scanning
        }))
    }
    // Physical verification is intentionally absent from this guarded context.
    // The host uses the retained unlocked read-point producer instead.
    pub(super) fn verified_contents(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<VerifiedJoinedTargetContents> {
        self.require_owner(db, context)?;
        let seal = finished_seal(context)?;
        let rows = self.completed_rows(db, context, &seal)?;
        let state = self
            .verification
            .as_ref()
            .ok_or_else(|| invalid("Joined final image requires independent target scan"))?;
        state.require_current(db, context, &seal, &rows, &self.header)?;
        if !state.complete
            || db.replication_shippable_seq() < state.target_reserved_b
            || db.current_visibility_watermark() < state.target_reserved_b
            || state.records != seal.record_count
            || state.bytes != seal.serialized_record_bytes
            || hex::encode(state.content.clone().finalize()) != seal.image_root_sha256
        {
            return Err(invalid(
                "Joined independent target verification incomplete or not durable/visible",
            ));
        }
        Ok(VerifiedJoinedTargetContents {
            generation: state.generation,
            target_reserved_b: state.target_reserved_b,
            min_snapshot_b: state.min_snapshot_b.unwrap_or(state.target_reserved_b),
            image_generations: state.image_generations,
            index_definitions: state.index_definitions.clone(),
            seal,
            rows,
            history_header_sha256: state.history_header_sha256.clone(),
            history_generation: state.history_generation,
        })
    }
}
