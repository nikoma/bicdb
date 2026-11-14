//! Normalize actual native genesis/installed anchors for local suffix checks.
//! The private normalized record is not a proof constructor: only actual typed
//! native producers below create it. Capture and intended future configuration
//! remain distinct; this module does not activate membership.
use super::*;
use crate::db::{NativeInstalledRangeImageBase, NativeInstalledRangeImageIntegrity};

#[derive(Debug)]
pub(super) enum NativeRangeImageAnchor {
    Genesis(NativeGenesisReceipt),
    Installed(NativeInstalledRangeImageBase),
}

/// Private derived binding for checksums/prefix linking. It cannot be decoded
/// from a DTO or supplied by a caller to establish a native floor.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(super) struct NativeRangeBaseBinding {
    pub(super) installed: bool,
    pub(super) birth_id: Uuid,
    pub(super) birth_sha256: String,
    pub(super) checksum_sha256: String,
    pub(super) range: RangeDescriptor,
    pub(super) base_members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    pub(super) local_member_incarnation: u64,
    pub(super) restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    pub(super) range_index: u64,
    pub(super) actual_local_visibility_sequence: u64,
    pub(super) schema_sha256: String,
    pub(super) global_image_generation: Uuid,
    pub(super) range_image_generation: Uuid,
    pub(super) portable_catalog_sha256: String,
    pub(super) image_root_sha256: String,
    pub(super) reader_features: Vec<String>,
    pub(super) local_namespace_ids: Vec<(String, Uuid)>,
    pub(super) native_page_xid: u64,
    pub(super) history_generation: Uuid,
    pub(super) logical_genesis_sha256: String,
    pub(super) logical_history_sha256: String,
}
impl NativeRangeBaseBinding {
    pub(super) fn genesis(receipt: &NativeGenesisReceipt) -> Self {
        Self {
            installed: false,
            birth_id: receipt.birth_id,
            birth_sha256: receipt.birth_sha256.clone(),
            checksum_sha256: receipt.checksum_sha256.clone(),
            range: receipt.range.clone(),
            base_members: receipt.base_members.clone(),
            local_member_incarnation: receipt.local_member_incarnation,
            restore_activation: receipt.restore_activation.clone(),
            range_index: receipt.range_index,
            actual_local_visibility_sequence: receipt.actual_snapshot_db_sequence,
            schema_sha256: receipt.schema_sha256.clone(),
            global_image_generation: receipt.global_image_generation,
            range_image_generation: receipt.range_image_generation,
            portable_catalog_sha256: receipt.portable_catalog_sha256.clone(),
            image_root_sha256: receipt.empty_image_root_sha256.clone(),
            reader_features: receipt.reader_features.clone(),
            local_namespace_ids: receipt.local_namespace_ids.clone(),
            native_page_xid: receipt.native_page_xid,
            history_generation: receipt.history_generation,
            logical_genesis_sha256: receipt.logical_genesis_sha256.clone(),
            logical_history_sha256: receipt.logical_genesis_sha256.clone(),
        }
    }
}
macro_rules! actual_installed_binding {
    ($base:expr) => {{
        let base = $base;
        let (global_image_generation, range_image_generation) = base.image_generations();
        NativeRangeBaseBinding {
            installed: true,
            birth_id: base.birth_id(),
            birth_sha256: base.birth_sha256().to_string(),
            checksum_sha256: base.native_receipt_sha256().to_string(),
            range: base.capture_range().clone(),
            base_members: base.base_members().clone(),
            local_member_incarnation: base.local_member_incarnation(),
            restore_activation: base.restore_activation().cloned(),
            range_index: base.range_index(),
            actual_local_visibility_sequence: base.target_snapshot_bound().get(),
            schema_sha256: base.schema_sha256().to_string(),
            global_image_generation,
            range_image_generation,
            portable_catalog_sha256: base.portable_catalog_sha256().to_string(),
            image_root_sha256: base.image_root_sha256().to_string(),
            reader_features: base.reader_features().to_vec(),
            local_namespace_ids: base.local_namespace_ids().to_vec(),
            native_page_xid: base.native_page_xid(),
            history_generation: base.history_generation(),
            logical_genesis_sha256: base.logical_genesis_sha256().to_string(),
            logical_history_sha256: base.logical_history_sha256().to_string(),
        }
    }};
}
impl NativeRangeImageAnchor {
    pub(super) fn binding(&self) -> NativeRangeBaseBinding {
        match self {
            Self::Genesis(base) => NativeRangeBaseBinding::genesis(base),
            Self::Installed(base) => actual_installed_binding!(base),
        }
    }
}
impl BicDb {
    pub(super) fn native_admission_for_actual_birth(
        &self,
        birth: &NativeDatabaseBirth,
    ) -> Result<NativeRecoveryAdmission> {
        self.native_recovery_admission(
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            &birth.spec.node_id,
            birth.spec.member_incarnation,
        )
    }
    pub(super) fn validate_native_range_base_history(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<()> {
        if base.birth_id != birth.birth_id
            || base.birth_sha256 != birth.checksum_sha256
            || base.local_member_incarnation != birth.spec.member_incarnation
        {
            return Err(invalid(
                "native range anchor belongs to another physical birth",
            ));
        }
        let first = self.read_native_history_entry(base.range.id, base.history_generation, 0)?;
        let last = self.read_native_history_entry(
            base.range.id,
            base.history_generation,
            base.range_index,
        )?;
        if first.outcome != history::NativeLogicalHistoryOutcomeV1::Genesis
            || first.commitment_sha256 != base.logical_genesis_sha256
            || last.commitment_sha256 != base.logical_history_sha256
        {
            return Err(invalid(
                "native range anchor logical history endpoints differ",
            ));
        }
        Ok(())
    }
    pub(super) fn actual_native_range_anchor(
        &self,
        birth: &NativeDatabaseBirth,
        range_id: RangeId,
    ) -> Result<NativeRangeImageAnchor> {
        if self.native_range_image_installing(range_id)? {
            return Err(invalid(
                "native image replacement incomplete; no source anchor",
            ));
        }
        if self
            .read_native_range_image_record::<serde_json::Value>(&format!(
                "install:base:{}",
                range_id.get()
            ))?
            .is_some()
        {
            let admission = self.native_admission_for_actual_birth(birth)?;
            let base = self
                .native_installed_range_image_base(&admission, range_id)?
                .ok_or_else(|| invalid("known installed anchor disappeared"))?;
            self.validate_actual_installed_history_publication(&base)?;
            return Ok(NativeRangeImageAnchor::Installed(base));
        }
        let base: NativeGenesisReceipt = self
            .read_native_range_image_record(&genesis_key(range_id))?
            .ok_or_else(|| invalid("no trusted native anchor; image repair required"))?;
        base.validate()?;
        self.validate_native_genesis_history(birth, &base)?;
        Ok(NativeRangeImageAnchor::Genesis(base))
    }
    pub(super) fn actual_native_range_integrity_binding(
        &self,
        birth: &NativeDatabaseBirth,
        range_id: RangeId,
    ) -> Result<Option<NativeRangeBaseBinding>> {
        let admission = self.native_admission_for_actual_birth(birth)?;
        if let Some(base) = self.native_installed_range_image_integrity(&admission, range_id)? {
            self.validate_actual_installed_integrity_history_publication(&base)?;
            let binding = actual_installed_binding!(&base);
            self.validate_native_range_base_history(birth, &binding)?;
            return Ok(Some(binding));
        }
        let base: Option<NativeGenesisReceipt> =
            self.read_native_range_image_record(&genesis_key(range_id))?;
        base.map(|base| {
            base.validate()?;
            self.validate_native_genesis_history(birth, &base)?;
            Ok(NativeRangeBaseBinding::genesis(&base))
        })
        .transpose()
    }
    fn validate_actual_installed_history_publication(
        &self,
        base: &NativeInstalledRangeImageBase,
    ) -> Result<()> {
        if base.has_registered_history_publication() || base.has_joined_history_publication() {
            return self.validate_registered_installed_base_history(base);
        }
        self.validate_published_native_history_binding(
            base.capture_range().id,
            base.history_generation(),
            base.range_index(),
            base.logical_genesis_sha256(),
            base.logical_history_sha256(),
            base.native_page_xid(),
            base.birth_id(),
            base.birth_sha256(),
            base.attempt_id(),
            base.session_id(),
            base.nonce(),
            base.source_native_receipt_sha256(),
        )
    }
    fn validate_actual_installed_integrity_history_publication(
        &self,
        base: &NativeInstalledRangeImageIntegrity,
    ) -> Result<()> {
        if base.has_registered_history_publication() || base.has_joined_history_publication() {
            return self.validate_registered_installed_integrity_history(base);
        }
        self.validate_published_native_history_binding(
            base.capture_range().id,
            base.history_generation(),
            base.range_index(),
            base.logical_genesis_sha256(),
            base.logical_history_sha256(),
            base.native_page_xid(),
            base.birth_id(),
            base.birth_sha256(),
            base.attempt_id(),
            base.session_id(),
            base.nonce(),
            base.source_native_receipt_sha256(),
        )
    }
}
