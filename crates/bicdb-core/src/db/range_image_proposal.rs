//! Protected pre-BEGIN historical source acceptance and target preimage.
//! Orphans do not invalidate the ordinary native point. These receipts never
//! create current metadata/data quorum, target compatibility or native Ready.
use super::range_image_native::{native_key, MAX_NATIVE_RECEIPT_BYTES};
use super::*;
use crate::distribution::{ClusterNodeId, RangeId};
use crate::distribution_range_consensus::{
    NativeCurrentImageProposalPointer, NativeImageInstallRole, NativeImageProposalIntentBinding,
    RangeWriteStore, TargetImageCatalogHistoryObligations,
};
use crate::distribution_range_image::{bounded_json, TentativeNativeRangeImageSourceAuthorization};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{
    HistoricalCurrentImageAcceptanceV1, RecoveryNativeHistoryPointV1,
};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

const VERSION: u32 = 1;
fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(format!("native current image proposal: {message}"))
}
fn key(id: Uuid) -> String {
    format!("current-image-proposal/{id}")
}
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Preserved irreversible point, not a claim about dirty current rows. Every
/// field is initially supplied by actual NativeRangeInstallSafety/catalogue.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProtectedCurrentImagePreimageV1 {
    pub base_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub pending_index: Option<u64>,
    pub materialized_pending_index: Option<u64>,
    pub pending_command_sha256: Option<String>,
    pub native_point: Option<RecoveryNativeHistoryPointV1>,
    pub history_generation: Option<Uuid>,
    pub native_records_sha256: String,
    pub catalogue_sha256: String,
    pub irreversible_commands: BTreeMap<u64, String>,
    pub observed_db_sequence: u64,
}
impl ProtectedCurrentImagePreimageV1 {
    pub(crate) fn maximum_index(&self) -> Option<u64> {
        self.base_index
            .into_iter()
            .chain(self.applied_index)
            .chain(self.pending_index)
            .chain(self.materialized_pending_index)
            .chain(self.irreversible_commands.keys().next_back().copied())
            .max()
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if !digest(&self.native_records_sha256)
            || !digest(&self.catalogue_sha256)
            || self.irreversible_commands.len() > 65_536
            || self
                .irreversible_commands
                .iter()
                .any(|(i, s)| *i == 0 || !digest(s))
            || self.pending_index.is_some() != self.pending_command_sha256.is_some()
            || self
                .pending_command_sha256
                .as_deref()
                .is_some_and(|s| !digest(s))
            || self.native_point.is_some() != self.history_generation.is_some()
            || self.history_generation.is_some_and(|id| id.is_nil())
            || self.native_point.as_ref().is_some_and(|p| {
                !digest(&p.logical_genesis_sha256)
                    || !digest(&p.logical_history_sha256)
                    || !digest(&p.native_receipt_sha256)
            })
        {
            return Err(invalid("damaged preserved native/catalogue preimage"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtectedCurrentImageProposalRecord {
    format_version: u32,
    proposal_id: Uuid,
    parent: Option<NativeCurrentImageProposalPointer>,
    intent_core_sha256: String,
    target_birth_id: Uuid,
    target_birth_sha256: String,
    target_node: ClusterNodeId,
    target_member_incarnation: u64,
    target_namespace_ids: Vec<(String, Uuid)>,
    target_observed_database_incarnation: String,
    original_target_metadata_context: Uuid,
    original_target_metadata_term: u64,
    original_target_metadata_read_index: u64,
    source_acceptance: HistoricalCurrentImageAcceptanceV1,
    preimage: ProtectedCurrentImagePreimageV1,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl ProtectedCurrentImageProposalRecord {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        self.source_acceptance.validate()?;
        self.preimage.validate()?;
        if let Some(parent) = &self.parent {
            parent.validate()?;
        }
        if self.format_version != VERSION
            || self.proposal_id.is_nil()
            || self.target_birth_id.is_nil()
            || self.native_page_xid == 0
            || !digest(&self.target_birth_sha256)
            || !digest(&self.intent_core_sha256)
            || self.target_node != self.source_acceptance.target_id
            || self
                .source_acceptance
                .members
                .get(&self.target_node)
                .is_none_or(|m| m.incarnation != self.target_member_incarnation)
            || self.target_namespace_ids.len() > 65_536
            || self
                .target_namespace_ids
                .windows(2)
                .any(|p| p[0].0 >= p[1].0)
            || self
                .target_namespace_ids
                .iter()
                .any(|(name, id)| name.is_empty() || id.is_nil())
            || self.target_observed_database_incarnation.is_empty()
            || self.target_observed_database_incarnation.len() > 128
            || self.original_target_metadata_context.is_nil()
            || self.original_target_metadata_term < self.source_acceptance.scope.metadata_term
            || self.original_target_metadata_read_index
                < self.source_acceptance.scope.metadata_read_index
            || self
                .preimage
                .maximum_index()
                .is_some_and(|r| r > self.source_acceptance.source_range_index)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected historical proposal"));
        }
        Ok(())
    }
}

/// Actual native-WAL receipt bound to current physical birth/open. This is
/// historical acceptance only. Loading it cannot reconstruct fresh authority.
#[derive(Debug)]
pub(crate) struct NativeCurrentImageProposalReceipt {
    root: PathBuf,
    database_incarnation: String,
    record: ProtectedCurrentImageProposalRecord,
}
impl NativeCurrentImageProposalReceipt {
    pub(crate) fn pointer(&self) -> NativeCurrentImageProposalPointer {
        NativeCurrentImageProposalPointer {
            proposal_id: self.record.proposal_id,
            checksum_sha256: self.record.checksum_sha256.clone(),
        }
    }
    pub(crate) fn intent_core_sha256(&self) -> &str {
        &self.record.intent_core_sha256
    }
    pub(crate) fn preimage(&self) -> &ProtectedCurrentImagePreimageV1 {
        &self.record.preimage
    }
    pub(crate) fn historical_source(&self) -> &HistoricalCurrentImageAcceptanceV1 {
        &self.record.source_acceptance
    }
    pub(crate) fn target_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.record.target_namespace_ids
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        native.require_current(db)?;
        self.record.validate()?;
        if self.database_incarnation != native.database_incarnation()
            || self.root != db.data_path().canonicalize()?
            || self.record.target_birth_id != native.birth_id()
            || self.record.target_birth_sha256 != native.birth_sha256()
            || self.record.target_node != *native.local_node_id()
            || self.record.target_member_incarnation != native.local_member_incarnation()
            || self.record.source_acceptance.scope.cluster_id != *native.cluster_id()
            || self.record.source_acceptance.scope.bootstrap_id != native.bootstrap_id()
            || self.record.source_acceptance.restore_activation.as_ref()
                != native.restore_activation()
            || db.capture_range_image_bindings()?.namespace_ids()
                != self.record.target_namespace_ids
            || db.schema_compatibility_fingerprint()?.sha256
                != self.record.source_acceptance.schema_sha256
        {
            return Err(invalid(
                "proposal belongs to another current physical birth/UID/schema/restore/open",
            ));
        }
        let stored: ProtectedCurrentImageProposalRecord = db
            .read_native_range_image_record(&key(self.record.proposal_id))?
            .ok_or_else(|| invalid("referenced protected proposal is missing"))?;
        stored.validate()?;
        if stored.checksum_sha256 != self.record.checksum_sha256 {
            return Err(invalid("protected proposal changed"));
        }
        if let (Some(point), Some(generation)) = (
            &self.record.preimage.native_point,
            self.record.preimage.history_generation,
        ) {
            let head = db.read_native_history_entry(
                point_range(&self.record),
                generation,
                point.range_index,
            )?;
            let genesis = db.read_native_history_entry(point_range(&self.record), generation, 0)?;
            if head.commitment_sha256 != point.logical_history_sha256
                || genesis.commitment_sha256 != point.logical_genesis_sha256
            {
                return Err(invalid("proposal lost immutable preimage history"));
            }
        }
        Ok(())
    }
}
fn point_range(record: &ProtectedCurrentImageProposalRecord) -> RangeId {
    record.source_acceptance.scope.range_id
}

impl BicDb {
    pub(crate) fn read_native_current_image_proposal(
        &self,
        native: &NativeRecoveryAdmission,
        pointer: &NativeCurrentImageProposalPointer,
    ) -> Result<NativeCurrentImageProposalReceipt> {
        pointer.validate()?;
        native.require_current(self)?;
        if !crate::format::current_image_proposal_active(&self.path)? {
            return Err(invalid("protected proposal reader fence is absent"));
        }
        let record: ProtectedCurrentImageProposalRecord = self
            .read_native_range_image_record(&key(pointer.proposal_id))?
            .ok_or_else(|| invalid("referenced protected proposal is absent"))?;
        record.validate()?;
        if record.proposal_id != pointer.proposal_id
            || record.checksum_sha256 != pointer.checksum_sha256
        {
            return Err(invalid("protected proposal pointer mismatch"));
        }
        let receipt = NativeCurrentImageProposalReceipt {
            root: self.path.canonicalize()?,
            database_incarnation: native.database_incarnation().to_string(),
            record,
        };
        receipt.require_current(self, native)?;
        Ok(receipt)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_native_current_image_proposal(
        &self,
        store: &RangeWriteStore,
        binding: &NativeImageProposalIntentBinding,
        native: &NativeRecoveryAdmission,
        safety: &NativeRangeInstallSafety,
        catalogue: &TargetImageCatalogHistoryObligations,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeCurrentImageProposalReceipt> {
        let NativeImageInstallRole::CurrentVoterRepair(current) = role else {
            return Err(invalid("proposal requires actual NormalRepair role"));
        };
        binding.require_current(store, self, native, source, role, frozen)?;
        safety.require_current(self, native, frozen)?;
        catalogue.require_current(store)?;
        let accepted = source
            .cut()
            .historical_current_acceptance()
            .ok_or_else(|| invalid("source cut has no actual historical source QQ acceptance"))?;
        let audit = accepted.audit();
        audit.validate()?;
        if audit.target_id != *native.local_node_id()
            || audit.source_range_index != source.cut().range_index()
            || audit.source_native_receipt_sha256 != source.native_receipt_sha256()
            || audit.logical_genesis_sha256 != source.lineage().logical_genesis_sha256()
            || audit.logical_history_sha256 != source.lineage().logical_history_sha256()
            || !audit.scope.same_session(
                &crate::distribution_range_consensus::RecoveryFenceScope::from_role(current)?,
            )
        {
            return Err(invalid("actual source acceptance differs from candidate"));
        }
        crate::format::enable_current_image_proposal(&self.path)?;
        let _journal = self.try_native_image_journal()?;
        binding.require_current(store, self, native, source, role, frozen)?;
        safety.require_current_under_journal(self, native, frozen, &_journal)?;
        catalogue.require_current(store)?;
        let parent = binding.parent_proposal(store)?;
        let prior = parent
            .as_ref()
            .map(|p| self.read_native_current_image_proposal(native, p))
            .transpose()?;
        if let Some(prior) = &prior {
            if prior.historical_source().scope.same_session(&audit.scope) == false
                || prior.historical_source().source_range_index != audit.source_range_index
                || prior.historical_source().logical_history_sha256 != audit.logical_history_sha256
                || prior.historical_source().logical_genesis_sha256 != audit.logical_genesis_sha256
                || prior.target_namespace_ids() != binding.namespace_ids()
            {
                return Err(invalid(
                    "fresh proposal cannot replace another historical cut/preimage/UID",
                ));
            }
        }
        let preimage = if let Some(prior) = &prior {
            prior.preimage().clone()
        } else {
            let point = safety.history_point();
            ProtectedCurrentImagePreimageV1 {
                base_index: safety.base_index(),
                applied_index: safety.applied_index(),
                pending_index: safety.pending_index(),
                materialized_pending_index: safety.materialized_pending_index(),
                pending_command_sha256: safety
                    .pending_command_checksum_sha256()
                    .map(str::to_string),
                native_point: point.map(|p| RecoveryNativeHistoryPointV1 {
                    range_index: p.range_index(),
                    logical_genesis_sha256: p.logical_genesis_sha256().to_string(),
                    logical_history_sha256: p.logical_history_sha256().to_string(),
                    native_receipt_sha256: p.native_receipt_sha256().to_string(),
                }),
                history_generation: point.map(|p| p.history_generation()),
                native_records_sha256: safety.irreversible_records_sha256().to_string(),
                catalogue_sha256: catalogue.range_log_sha256().to_string(),
                irreversible_commands: catalogue.irrevocable_commands().clone(),
                observed_db_sequence: self.current_commit_seq(),
            }
        };
        let existing: Option<ProtectedCurrentImageProposalRecord> =
            self.read_native_range_image_record(&key(binding.attempt_id()))?;
        let mut record = if let Some(existing) = existing {
            existing.validate()?;
            if existing.intent_core_sha256 != binding.core_sha256()
                || existing.target_birth_id != native.birth_id()
                || existing.target_namespace_ids != binding.namespace_ids()
                || existing.source_acceptance.source_range_index != audit.source_range_index
                || existing.source_acceptance.logical_history_sha256 != audit.logical_history_sha256
            {
                return Err(invalid(
                    "proposal attempt already names another actual scope",
                ));
            }
            existing
        } else {
            ProtectedCurrentImageProposalRecord {
                format_version: VERSION,
                proposal_id: binding.attempt_id(),
                parent,
                intent_core_sha256: binding.core_sha256().to_string(),
                target_birth_id: native.birth_id(),
                target_birth_sha256: native.birth_sha256().to_string(),
                target_node: native.local_node_id().clone(),
                target_member_incarnation: native.local_member_incarnation(),
                target_namespace_ids: binding.namespace_ids().to_vec(),
                target_observed_database_incarnation: native.database_incarnation().to_string(),
                original_target_metadata_context: current.context_id(),
                original_target_metadata_term: current.metadata_term(),
                original_target_metadata_read_index: current.metadata_read_index(),
                source_acceptance: audit.clone(),
                preimage,
                native_page_xid: 0,
                checksum_sha256: String::new(),
            }
        };
        // Enforce COMPLETE bounds before any native page mutation; XID is a
        // fixed-width integral field and not a range or MVCC visibility bound.
        let mut sized = record.clone();
        sized.native_page_xid = u64::MAX;
        sized.checksum_sha256 = "0".repeat(64);
        bounded_json(&sized, MAX_NATIVE_RECEIPT_BYTES)?;
        let controlled = self.range_mutation_gate().try_controlled_mutation(frozen)?;
        self.in_paged_transaction(|paged, xid| {
            if record.native_page_xid == 0 {
                record.native_page_xid = xid.get();
                record.checksum_sha256 = record.checksum()?;
            }
            record.validate()?;
            paged
                .store()
                .put(
                    xid,
                    &native_key(&key(record.proposal_id)),
                    &bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?,
                )
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("protected proposal requires actual native page store"))?;
        drop(controlled);
        binding.require_current(store, self, native, source, role, frozen)?;
        safety.require_current_under_journal(self, native, frozen, &_journal)?;
        catalogue.require_current(store)?;
        current.require_live_request()?;
        let pointer = NativeCurrentImageProposalPointer {
            proposal_id: record.proposal_id,
            checksum_sha256: record.checksum_sha256.clone(),
        };
        self.read_native_current_image_proposal(native, &pointer)
    }
}
