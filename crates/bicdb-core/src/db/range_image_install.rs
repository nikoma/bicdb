//! Native target receipts. A completed physical anchor is not an activated
//! range configuration, data-catalog floor, or strong-read permission.
use super::range_image_native::{native_key, MAX_NATIVE_RECEIPT_BYTES};
use super::*;
use crate::distribution::{ClusterId, ClusterNodeId, RangeId};
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageInstallRole, TargetImageInstallFence,
};
use crate::distribution_range_image::{bounded_json, canonical_range_image_catalog};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

const INSTALL_FORMAT: u32 = 1;
const REGISTERED_INSTALL_FORMAT: u32 = 2;
#[cfg(feature = "tls-replication")]
mod registered_remote;
#[cfg(all(test, feature = "tls-replication"))]
pub(crate) use registered_remote::test_registered_remote_base_after_wal;
#[cfg(feature = "tls-replication")]
pub(crate) use registered_remote::{RegisteredRemoteBaseBoundary, RegisteredRemoteBasePublication};

fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("native target image: {}", detail.into()))
}
fn active_key(range_id: RangeId) -> String {
    format!("install:active:{}", range_id.get())
}
fn base_key(range_id: RangeId) -> String {
    format!("install:base:{}", range_id.get())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeInstalling {
    format_version: u32,
    attempt_id: Uuid,
    session_id: Uuid,
    birth_id: Uuid,
    birth_sha256: String,
    intent_sha256: String,
    source_range_index: u64,
    local_namespace_ids: Vec<(String, Uuid)>,
}

/// Private persistence DTO. Only the actual native finalization path writes
/// it. Its checksum is integrity, never a substitute for image verification.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeInstalledRangeImageReceipt {
    format_version: u32,
    // Unconditional persistence DATA. A reader without the actual Registered
    // history validator must refuse this profile, never interpret it as local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registered_final_image: Option<crate::db::NativeRegisteredRemoteFinalImagePointer>,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    target_node_id: ClusterNodeId,
    target_member_incarnation: u64,
    birth_id: Uuid,
    birth_sha256: String,
    purpose: ImageInstallPurpose,
    capture_range: RangeDescriptor,
    intended_activation_range: RangeDescriptor,
    base_members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    intended_activation_members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    range_index: u64,
    history_generation: Uuid,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    history_verified_through: u64,
    attempt_id: Uuid,
    session_id: Uuid,
    nonce: Uuid,
    source_purpose: crate::distribution_range_image::RangeImageSourcePurposeV1,
    intent_sha256: String,
    source_native_receipt_sha256: String,
    old_config_sha256: String,
    intended_config_sha256: String,
    schema_sha256: String,
    reader_features: Vec<String>,
    portable_catalog_sha256: String,
    image_root_sha256: String,
    local_namespace_ids: Vec<(String, Uuid)>,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    reserved_db_upper_bound: u64,
    min_snapshot_db_sequence: u64,
    record_count: u64,
    serialized_record_bytes: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl NativeInstalledRangeImageReceipt {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        let digest =
            |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
        if !matches!(
            (self.format_version, self.registered_final_image.as_ref()),
            (INSTALL_FORMAT, None) | (REGISTERED_INSTALL_FORMAT, Some(_))
        ) || self.bootstrap_id.is_nil()
            || self.birth_id.is_nil()
            || self.attempt_id.is_nil()
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.target_member_incarnation == 0
            || self.native_page_xid == 0
            || self.history_generation.is_nil()
            || self.history_verified_through != self.range_index
            || !digest(&self.logical_genesis_sha256)
            || !digest(&self.logical_history_sha256)
            || self.global_image_generation.is_nil()
            || self.range_image_generation.is_nil()
            || self.capture_range.id != self.intended_activation_range.id
            || self.capture_range.start_token != self.intended_activation_range.start_token
            || self.capture_range.end_token != self.intended_activation_range.end_token
            || self.capture_range.epoch == 0
            || self.intended_activation_range.epoch < self.capture_range.epoch
            || self.min_snapshot_db_sequence < self.reserved_db_upper_bound
            || self
                .local_namespace_ids
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
            || self.local_namespace_ids.iter().any(|(_, id)| id.is_nil())
            || self.base_members.is_empty()
            || self.base_members.len() > 64
            || !digest(&self.birth_sha256)
            || !digest(&self.intent_sha256)
            || !digest(&self.source_native_receipt_sha256)
            || !digest(&self.old_config_sha256)
            || !digest(&self.intended_config_sha256)
            || !digest(&self.schema_sha256)
            || !digest(&self.portable_catalog_sha256)
            || !digest(&self.image_root_sha256)
            || self.checksum_sha256 != self.checksum()?
        {
            return Err(invalid("damaged installed image receipt"));
        }
        if let Some(pointer) = &self.registered_final_image {
            pointer.validate()?;
            if pointer.attempt_id != self.attempt_id
                || self.purpose != ImageInstallPurpose::RelocationLearner
            {
                return Err(invalid("Registered installed profile/purpose mismatch"));
            }
        }
        Ok(())
    }
}

/// An actual native receipt bound to this DB open. The two configurations are
/// observations: neither is selected as active by this type. Root's distinct
/// durable catalog/configuration proof must authorize any later source use.
#[derive(Debug)]
pub(crate) struct NativeInstalledRangeImageBase {
    receipt: NativeInstalledRangeImageReceipt,
    database_incarnation: String,
    schema_generation: u64,
}

/// An actual persisted lower-bound receipt. This intentionally does not prove
/// current row/schema/UID generations, complete data, visibility, or readiness.
/// It remains available during Installing and after image invalidation so an
/// irreversible range floor cannot disappear behind a source-read refusal.
#[derive(Debug)]
pub(crate) struct NativeInstalledRangeImageIntegrity {
    receipt: NativeInstalledRangeImageReceipt,
    database_incarnation: String,
}
impl NativeInstalledRangeImageIntegrity {
    pub(super) fn has_registered_history_publication(&self) -> bool {
        self.receipt.registered_final_image.is_some()
    }
    pub(crate) fn source_native_receipt_sha256(&self) -> &str {
        &self.receipt.source_native_receipt_sha256
    }
    pub(crate) fn history_generation(&self) -> Uuid {
        self.receipt.history_generation
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.receipt.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.receipt.logical_history_sha256
    }
    pub(crate) fn history_verified_through(&self) -> u64 {
        self.receipt.history_verified_through
    }
    pub(crate) fn target_snapshot_bound(&self) -> TargetNativeImageSnapshotBound {
        TargetNativeImageSnapshotBound(self.receipt.reserved_db_upper_bound)
    }
    pub(crate) fn target_observed_min_snapshot_sequence(&self) -> u64 {
        self.receipt.min_snapshot_db_sequence
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.receipt.attempt_id
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.receipt.intent_sha256
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.receipt.session_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.receipt.nonce
    }
    pub(crate) fn source_purpose(
        &self,
    ) -> &crate::distribution_range_image::RangeImageSourcePurposeV1 {
        &self.receipt.source_purpose
    }
    pub(crate) fn capture_range(&self) -> &RangeDescriptor {
        &self.receipt.capture_range
    }
    pub(crate) fn intended_activation_range(&self) -> &RangeDescriptor {
        &self.receipt.intended_activation_range
    }
    pub(crate) fn purpose(&self) -> ImageInstallPurpose {
        self.receipt.purpose
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.receipt.range_index
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.receipt.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.receipt.bootstrap_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.receipt.target_node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.receipt.target_member_incarnation
    }
    pub(crate) fn intended_activation_members(
        &self,
    ) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        &self.receipt.intended_activation_members
    }
    pub(crate) fn base_members(&self) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        &self.receipt.base_members
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.receipt.restore_activation.as_ref()
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        &self.receipt.checksum_sha256
    }
    pub(crate) fn native_page_xid(&self) -> u64 {
        self.receipt.native_page_xid
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.receipt.schema_sha256
    }
    pub(crate) fn reader_features(&self) -> &[String] {
        &self.receipt.reader_features
    }
    pub(crate) fn portable_catalog_sha256(&self) -> &str {
        &self.receipt.portable_catalog_sha256
    }
    pub(crate) fn image_root_sha256(&self) -> &str {
        &self.receipt.image_root_sha256
    }
    pub(crate) fn local_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.receipt.local_namespace_ids
    }
    pub(crate) fn image_generations(&self) -> (Uuid, Uuid) {
        (
            self.receipt.global_image_generation,
            self.receipt.range_image_generation,
        )
    }
    pub(crate) fn birth_id(&self) -> Uuid {
        self.receipt.birth_id
    }
    pub(crate) fn birth_sha256(&self) -> &str {
        &self.receipt.birth_sha256
    }

    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        admission: &NativeRecoveryAdmission,
    ) -> Result<()> {
        admission.require_current(database)?;
        self.receipt.validate()?;
        let receipt = &self.receipt;
        if self.database_incarnation != database.incarnation
            || receipt.birth_id != admission.birth_id()
            || receipt.birth_sha256 != admission.birth_sha256()
            || receipt.cluster_id != *admission.cluster_id()
            || receipt.bootstrap_id != admission.bootstrap_id()
            || receipt.target_node_id != *admission.local_node_id()
            || receipt.target_member_incarnation != admission.local_member_incarnation()
            || receipt.restore_activation.as_ref() != admission.restore_activation()
        {
            return Err(invalid(
                "installed integrity receipt native birth/open mismatch",
            ));
        }
        let actual: NativeInstalledRangeImageReceipt = database
            .read_native_range_image_record(&base_key(receipt.capture_range.id))?
            .ok_or_else(|| invalid("installed integrity receipt disappeared"))?;
        actual.validate()?;
        if actual != self.receipt {
            return Err(invalid("installed integrity receipt changed"));
        }
        database.validate_registered_installed_history_if_present(receipt)?;
        Ok(())
    }
}

/// Owned progress from actual native target scans; no serde/numeric constructor.
pub(crate) struct NativeTargetImageVerifier {
    database_incarnation: String,
    birth_id: Uuid,
    intent_sha256: String,
    reserved: u64,
    generations: (Uuid, Uuid),
    catalog_sha256: String,
    content: sha2::Sha256,
    expected_root: String,
    expected_records: u64,
    expected_bytes: u64,
    collection: usize,
    after: Option<String>,
    min_snapshot: Option<u64>,
    records: u64,
    bytes: u64,
    complete: bool,
    invalid: bool,
    recover_completed: bool,
    recovered_base: Option<NativeInstalledRangeImageBase>,
    history_audit_next: u64,
    history_audit_head: Option<String>,
    history_audit_complete: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeTargetImageVerificationProgress {
    WaitingForDurability,
    WaitingForVisibility,
    Scanning,
    Complete,
}
pub(crate) struct VerifiedNativeTargetImageContents {
    state: NativeTargetImageVerifier,
}
impl NativeTargetImageVerifier {
    fn require_current(
        &self,
        database: &BicDb,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        bindings: &RangeImageDbBindings,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
    ) -> Result<()> {
        if self.invalid
            || self.database_incarnation != database.incarnation
            || self.birth_id != admission.birth_id()
            || self.intent_sha256 != fence.intent_sha256()
            || self.expected_root != source.seal().image_root_sha256
            || self.expected_records != source.seal().record_count
            || self.expected_bytes != source.seal().serialized_record_bytes
            || self.generations != database.native_range_image_generation_pair(fence.range().id)?
        {
            return Err(invalid(
                "bounded target verification scope/physical image changed",
            ));
        }
        if let Some(base) = &self.recovered_base {
            base.require_current(database, admission)?;
        }
        database.validate_range_image_bindings(bindings)
    }
}
impl VerifiedNativeTargetImageContents {
    pub(in crate::db) fn actual_local_visibility_sequence(&self) -> u64 {
        self.state.reserved
    }
    pub(in crate::db) fn actual_observed_min_snapshot_sequence(&self) -> u64 {
        self.state.min_snapshot.unwrap_or(self.state.reserved)
    }
    pub(in crate::db) fn require_current(
        &self,
        database: &BicDb,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        bindings: &RangeImageDbBindings,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
    ) -> Result<()> {
        self.state
            .require_current(database, fence, admission, bindings, source)?;
        if !self.state.complete
            || !self.state.history_audit_complete
            || database.replication_shippable_seq() < self.state.reserved
            || database.current_visibility_watermark() < self.state.reserved
        {
            return Err(invalid(
                "complete target proof lost its actual local durable/visible bound",
            ));
        }
        Ok(())
    }
}

/// Actual target-global snapshot/durability domain, distinct from consensus R
/// and from the source's reservation B. Only a native installed base issues it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetNativeImageSnapshotBound(u64);
impl TargetNativeImageSnapshotBound {
    pub(crate) fn get(self) -> u64 {
        self.0
    }
}
impl NativeInstalledRangeImageBase {
    pub(super) fn has_registered_history_publication(&self) -> bool {
        self.receipt.registered_final_image.is_some()
    }
    pub(crate) fn source_native_receipt_sha256(&self) -> &str {
        &self.receipt.source_native_receipt_sha256
    }
    pub(crate) fn history_generation(&self) -> Uuid {
        self.receipt.history_generation
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.receipt.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.receipt.logical_history_sha256
    }
    pub(crate) fn history_verified_through(&self) -> u64 {
        self.receipt.history_verified_through
    }
    pub(crate) fn target_snapshot_bound(&self) -> TargetNativeImageSnapshotBound {
        TargetNativeImageSnapshotBound(self.receipt.reserved_db_upper_bound)
    }
    pub(crate) fn target_observed_min_snapshot_sequence(&self) -> u64 {
        self.receipt.min_snapshot_db_sequence
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.receipt.attempt_id
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.receipt.intent_sha256
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.receipt.session_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.receipt.nonce
    }
    pub(crate) fn source_purpose(
        &self,
    ) -> &crate::distribution_range_image::RangeImageSourcePurposeV1 {
        &self.receipt.source_purpose
    }
    pub(crate) fn capture_range(&self) -> &RangeDescriptor {
        &self.receipt.capture_range
    }
    pub(crate) fn intended_activation_range(&self) -> &RangeDescriptor {
        &self.receipt.intended_activation_range
    }
    pub(crate) fn purpose(&self) -> ImageInstallPurpose {
        self.receipt.purpose
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.receipt.range_index
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.receipt.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.receipt.bootstrap_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.receipt.target_node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.receipt.target_member_incarnation
    }
    pub(crate) fn intended_activation_members(
        &self,
    ) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        &self.receipt.intended_activation_members
    }
    pub(crate) fn base_members(&self) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        &self.receipt.base_members
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.receipt.restore_activation.as_ref()
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        &self.receipt.checksum_sha256
    }
    pub(crate) fn native_page_xid(&self) -> u64 {
        self.receipt.native_page_xid
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.receipt.schema_sha256
    }
    pub(crate) fn reader_features(&self) -> &[String] {
        &self.receipt.reader_features
    }
    pub(crate) fn portable_catalog_sha256(&self) -> &str {
        &self.receipt.portable_catalog_sha256
    }
    pub(crate) fn image_root_sha256(&self) -> &str {
        &self.receipt.image_root_sha256
    }
    pub(crate) fn local_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.receipt.local_namespace_ids
    }
    pub(crate) fn image_generations(&self) -> (Uuid, Uuid) {
        (
            self.receipt.global_image_generation,
            self.receipt.range_image_generation,
        )
    }
    pub(crate) fn birth_id(&self) -> Uuid {
        self.receipt.birth_id
    }
    pub(crate) fn birth_sha256(&self) -> &str {
        &self.receipt.birth_sha256
    }

    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        admission: &NativeRecoveryAdmission,
    ) -> Result<()> {
        admission.require_current(database)?;
        let receipt = &self.receipt;
        let bindings = database.capture_range_image_bindings()?;
        if self.database_incarnation != database.incarnation
            || self.schema_generation != database.schema_compatibility.generation()
            || database.native_range_image_installing(receipt.capture_range.id)?
            || receipt.birth_id != admission.birth_id()
            || receipt.birth_sha256 != admission.birth_sha256()
            || receipt.cluster_id != *admission.cluster_id()
            || receipt.bootstrap_id != admission.bootstrap_id()
            || receipt.target_node_id != *admission.local_node_id()
            || receipt.target_member_incarnation != admission.local_member_incarnation()
            || receipt.restore_activation.as_ref() != admission.restore_activation()
            || receipt.schema_sha256 != bindings.schema_sha256()
            || receipt.reader_features != bindings.reader_features()
            || receipt.local_namespace_ids != bindings.namespace_ids()
            || self.image_generations()
                != database.native_range_image_generation_pair(receipt.capture_range.id)?
        {
            return Err(invalid(
                "installed image local/native namespace contract changed",
            ));
        }
        let current: NativeInstalledRangeImageReceipt = database
            .read_native_range_image_record(&base_key(receipt.capture_range.id))?
            .ok_or_else(|| invalid("installed native receipt disappeared"))?;
        current.validate()?;
        if &current != receipt {
            return Err(invalid("installed native receipt was superseded"));
        }
        database.validate_registered_installed_history_if_present(receipt)?;
        let catalog = canonical_range_image_catalog(
            &receipt.cluster_id,
            &receipt.capture_range,
            receipt.range_index,
            &bindings,
        )?;
        if hex::encode(sha2::Sha256::digest(catalog)) != receipt.portable_catalog_sha256 {
            return Err(invalid("installed image portable catalog changed"));
        }
        Ok(())
    }
}

impl BicDb {
    fn validate_registered_installed_history_if_present(
        &self,
        receipt: &NativeInstalledRangeImageReceipt,
    ) -> Result<()> {
        if receipt.registered_final_image.is_none() {
            return Ok(());
        }
        #[cfg(feature = "tls-replication")]
        return self.validate_registered_installed_history(receipt);
        #[cfg(not(feature = "tls-replication"))]
        Err(invalid(
            "Registered installed history reader unavailable in this build",
        ))
    }

    pub(in crate::db) fn validate_registered_installed_base_history(
        &self,
        base: &NativeInstalledRangeImageBase,
    ) -> Result<()> {
        self.validate_registered_installed_history_if_present(&base.receipt)
    }

    pub(in crate::db) fn validate_registered_installed_integrity_history(
        &self,
        base: &NativeInstalledRangeImageIntegrity,
    ) -> Result<()> {
        self.validate_registered_installed_history_if_present(&base.receipt)
    }

    #[cfg(test)]
    pub(crate) fn test_native_serving_history_json(
        &self,
        range_id: RangeId,
        checksum: &str,
    ) -> Result<Option<serde_json::Value>> {
        self.read_native_range_image_record(&format!(
            "serving-configuration-history/{range_id}/{checksum}"
        ))
    }
    #[cfg(test)]
    pub(crate) fn test_native_serving_record_json(
        &self,
        range_id: RangeId,
    ) -> Result<Option<serde_json::Value>> {
        // Read-only forensic view. JSON cannot construct serving authority and
        // this test method never writes/deletes/relabels the reserved record.
        self.read_native_range_image_record(&format!("serving-configuration/{range_id}"))
    }

    /// Actual install-phase records, under the native journal and the owned
    /// quiescent range freeze. This is a read-only commitment, not readiness.
    pub(crate) fn native_image_install_phase_commitment(
        &self,
        range_id: RangeId,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<String> {
        let _journal = self.try_native_image_journal()?;
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)?;
        let marker: Option<NativeInstalling> =
            self.read_native_range_image_record(&active_key(range_id))?;
        if let Some(marker) = &marker {
            if marker.format_version != INSTALL_FORMAT
                || marker.attempt_id.is_nil()
                || marker.session_id.is_nil()
                || marker.birth_id.is_nil()
            {
                return Err(invalid(
                    "unknown native Installing contract during ownership preflight",
                ));
            }
        }
        let base: Option<NativeInstalledRangeImageReceipt> =
            self.read_native_range_image_record(&base_key(range_id))?;
        if let Some(base) = &base {
            base.validate()?;
        }
        let stage = self.native_history_stage_state_commitment(range_id)?;
        let bytes = bounded_json(
            &("bicdb.native.image-phase.v1", range_id, marker, base, stage),
            MAX_NATIVE_RECEIPT_BYTES
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)?;
        Ok(hex::encode(sha2::Sha256::digest(bytes)))
    }

    /// A previous other-attempt image may exist unchanged. This attempt must
    /// have created no Installing, completed native base, or staged history.
    pub(crate) fn require_no_native_image_install_state_for_attempt(
        &self,
        range_id: RangeId,
        attempt_id: Uuid,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        if attempt_id.is_nil() {
            return Err(invalid("nil image ownership-return attempt"));
        }
        let _journal = self.try_native_image_journal()?;
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)?;
        if let Some(marker) =
            self.read_native_range_image_record::<NativeInstalling>(&active_key(range_id))?
        {
            if marker.format_version != INSTALL_FORMAT || marker.attempt_id == attempt_id {
                return Err(invalid("this image attempt has persisted Installing state"));
            }
        }
        if let Some(base) = self
            .read_native_range_image_record::<NativeInstalledRangeImageReceipt>(&base_key(
                range_id,
            ))?
        {
            base.validate()?;
            if base.attempt_id == attempt_id {
                return Err(invalid("this image attempt has persisted a native base"));
            }
        }
        self.require_no_native_history_stage_for_attempt(range_id, attempt_id)?;
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)
    }

    pub(crate) fn require_normal_native_range_mutation_allowed(
        &self,
        range_id: RangeId,
    ) -> Result<()> {
        self.check_recovery_state()?;
        if self.native_range_image_installing(range_id)? {
            return Err(invalid(
                "normal physical mutation conflicts with native Installing",
            ));
        }
        // The caller must hold its actual normal mutation permit and check
        // the live RangeWriteStore intent before physical apply. Initial
        // installer freeze+drain closes the catalog-intent/marker interval;
        // rereading a complete JSON catalog on every row is not that proof.
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn require_native_target_image_scope(
        &self,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
        role: &NativeImageInstallRole<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        recover_completed: bool,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let seal = source.seal();
        cancellation.check()?;
        source.require_current()?;
        fence.require_live_role(role)?;
        admission.require_current(self)?;
        self.range_mutations
            .validate_quiescent_frozen(fence.range().id, frozen)?;
        self.validate_range_image_bindings(bindings)?;
        if frozen.owner() != fence.attempt_id()
            || admission.database_incarnation() != fence.database_incarnation()
            || admission.birth_id() != fence.target_birth_id()
            || admission.birth_sha256() != fence.target_birth_sha256()
            || seal.cluster_id != *fence.cluster_id()
            || seal.bootstrap_id != fence.bootstrap_id()
            || seal.range_id != fence.range().id
            || seal.capture_epoch != fence.range().epoch
            || seal.activation_epoch != fence.activation_epoch()
            || seal.session_id != fence.session_id()
            || seal.nonce != fence.nonce()
            || &seal.purpose != fence.source_purpose()
            || seal.old_config_sha256 != fence.old_config_sha256()
            || seal.new_config_sha256 != fence.intended_config_sha256()
            || seal.range_index != fence.source_range_index()
            || &seal.source_id != fence.source_id()
            || seal.source_member_incarnation != fence.source_incarnation()
            || seal.source_native_receipt_sha256 != fence.source_native_receipt_sha256()
            || seal.schema_sha256 != bindings.schema_sha256()
            || seal.reader_features != bindings.reader_features()
            || source.bindings().collections() != bindings.collections()
        {
            return Err(invalid(
                "completed source/native target/fresh-role scope mismatch",
            ));
        }
        let marker: Option<NativeInstalling> =
            self.read_native_range_image_record(&active_key(fence.range().id))?;
        if let Some(marker) = marker {
            if marker.format_version != INSTALL_FORMAT
                || marker.attempt_id != fence.attempt_id()
                || marker.session_id != fence.session_id()
                || marker.birth_id != admission.birth_id()
                || marker.birth_sha256 != admission.birth_sha256()
                || marker.intent_sha256 != fence.intent_sha256()
                || marker.local_namespace_ids != bindings.namespace_ids()
                || marker.source_range_index != fence.source_range_index()
            {
                return Err(invalid("native Installing attempt was replaced or damaged"));
            }
        } else if recover_completed {
            let base = self
                .native_installed_range_image_base(admission, fence.range().id)?
                .ok_or_else(|| invalid("no native completed receipt to recover"))?;
            if base.attempt_id() != fence.attempt_id()
                || base.intent_sha256() != fence.intent_sha256()
                || base.range_index() != fence.source_range_index()
                || base.session_id() != fence.session_id()
                || base.nonce() != fence.nonce()
                || base.source_purpose() != fence.source_purpose()
            {
                return Err(invalid(
                    "completed native image is not this exact persisted attempt",
                ));
            }
        } else {
            return Err(invalid("no actual native Installing record"));
        }
        Ok(())
    }

    /// Capture actual target B exactly once. Waiting/scan progress is returned
    /// by bounded steps, so callers release database/service locks between them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_native_target_image_verification(
        &self,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
        role: &NativeImageInstallRole<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        recover_completed: bool,
        cancellation: &CancellationToken,
    ) -> Result<NativeTargetImageVerifier> {
        self.require_native_target_image_scope(
            fence,
            admission,
            frozen,
            bindings,
            role,
            source,
            recover_completed,
            cancellation,
        )?;
        let seal = source.seal();
        let catalog = canonical_range_image_catalog(
            fence.cluster_id(),
            fence.range(),
            fence.source_range_index(),
            bindings,
        )?;
        let catalog_sha256 = hex::encode(sha2::Sha256::digest(&catalog));
        if catalog_sha256 != seal.portable_catalog_sha256
            || catalog_sha256 != fence.source_catalog_sha256()
        {
            return Err(invalid(
                "complete target portable catalog differs from completed source",
            ));
        }
        let recovered_base = if recover_completed {
            let base = self
                .native_installed_range_image_base(admission, fence.range().id)?
                .ok_or_else(|| invalid("completed verification lost installed anchor"))?;
            if base.range_index() != seal.range_index
                || base.logical_genesis_sha256() != seal.logical_genesis_sha256
                || base.logical_history_sha256() != seal.logical_history_sha256
                || base.source_native_receipt_sha256() != seal.source_native_receipt_sha256
            {
                return Err(invalid("completed target history/source anchor differs"));
            }
            Some(base)
        } else {
            None
        };
        Ok(NativeTargetImageVerifier {
            database_incarnation: self.incarnation.clone(),
            birth_id: admission.birth_id(),
            intent_sha256: fence.intent_sha256().to_string(),
            reserved: self.current_commit_seq(),
            generations: self.native_range_image_generation_pair(fence.range().id)?,
            catalog_sha256,
            content: crate::distribution_range_image::content_hasher(&catalog),
            expected_root: seal.image_root_sha256.clone(),
            expected_records: seal.record_count,
            expected_bytes: seal.serialized_record_bytes,
            collection: 0,
            after: None,
            min_snapshot: None,
            records: 0,
            bytes: 0,
            complete: bindings.collections().is_empty(),
            invalid: false,
            recover_completed,
            recovered_base,
            history_audit_next: 0,
            history_audit_head: None,
            history_audit_complete: !recover_completed,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn advance_native_target_image_verification(
        &self,
        state: &mut NativeTargetImageVerifier,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
        role: &NativeImageInstallRole<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        cancellation: &CancellationToken,
    ) -> Result<NativeTargetImageVerificationProgress> {
        self.require_native_target_image_scope(
            fence,
            admission,
            frozen,
            bindings,
            role,
            source,
            state.recover_completed,
            cancellation,
        )?;
        state.require_current(self, fence, admission, bindings, source)?;
        if self.replication_shippable_seq() < state.reserved {
            return Ok(NativeTargetImageVerificationProgress::WaitingForDurability);
        }
        if self.current_visibility_watermark() < state.reserved {
            return Ok(NativeTargetImageVerificationProgress::WaitingForVisibility);
        }
        if !state.history_audit_complete {
            let base = state
                .recovered_base
                .as_ref()
                .ok_or_else(|| invalid("completed history audit has no actual anchor"))?;
            let mut next = state.history_audit_next;
            let mut head = state.history_audit_head.clone();
            let mut complete = false;
            for _ in 0..256 {
                cancellation.check()?;
                let entry = self.read_native_history_entry(
                    fence.range().id,
                    base.history_generation(),
                    next,
                )?;
                entry.validate_shape()?;
                if entry.range_id != fence.range().id
                    || entry.index != next
                    || (next == 0
                        && (entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis
                            || entry.commitment_sha256 != base.logical_genesis_sha256()))
                    || (next > 0 && head.as_deref() != Some(entry.previous_sha256.as_str()))
                {
                    return Err(invalid(
                        "actual installed history is missing/reordered/forked",
                    ));
                }
                head = Some(entry.commitment_sha256);
                next = next
                    .checked_add(1)
                    .ok_or_else(|| invalid("installed history audit overflow"))?;
                if entry.index == base.range_index() {
                    if head.as_deref() != Some(base.logical_history_sha256()) {
                        return Err(invalid("actual installed complete history head differs"));
                    }
                    complete = true;
                    break;
                }
            }
            state.require_current(self, fence, admission, bindings, source)?;
            fence.require_live_role(role)?;
            cancellation.check()?;
            state.history_audit_next = next;
            state.history_audit_head = head;
            state.history_audit_complete = complete;
            return Ok(NativeTargetImageVerificationProgress::Scanning);
        }
        if state.complete {
            return Ok(NativeTargetImageVerificationProgress::Complete);
        }
        let meta = &bindings.collections()[state.collection];
        let mut content = state.content.clone();
        let mut records = state.records;
        let mut bytes = state.bytes;
        let exported = self.for_each_range_snapshot_batch_with_cancellation(
            &meta.name,
            fence.range(),
            state.after.as_deref(),
            &crate::distribution::RangeSnapshotOptions::default(),
            16_384,
            cancellation,
            |batch| {
                for record in batch.records {
                    let encoded = bounded_json(&record, 8 * 1024 * 1024)?;
                    for frame in [meta.name.as_bytes(), encoded.as_slice()] {
                        content.update((frame.len() as u64).to_be_bytes());
                        content.update(frame);
                    }
                    records = records
                        .checked_add(1)
                        .ok_or_else(|| invalid("target record count overflow"))?;
                    bytes = bytes
                        .checked_add(encoded.len() as u64)
                        .ok_or_else(|| invalid("target byte count overflow"))?;
                }
                Ok(false)
            },
        )?;
        if exported.snapshot_commit_sequence < state.reserved {
            return Ok(NativeTargetImageVerificationProgress::WaitingForVisibility);
        }
        let (collection, after) = if exported.completed {
            (state.collection + 1, None)
        } else {
            if exported.resume_after_key <= state.after {
                return Err(invalid(
                    "bounded target verification cursor failed to advance",
                ));
            }
            (state.collection, exported.resume_after_key)
        };
        let complete = collection == bindings.collections().len();
        if complete
            && (records != state.expected_records
                || bytes != state.expected_bytes
                || hex::encode(content.clone().finalize()) != state.expected_root)
        {
            state.invalid = true;
            return Err(invalid(
                "actual complete target contents differ from completed source (including extras)",
            ));
        }
        state.require_current(self, fence, admission, bindings, source)?;
        fence.require_live_role(role)?;
        cancellation.check()?;
        state.content = content;
        state.records = records;
        state.bytes = bytes;
        state.min_snapshot = Some(
            state
                .min_snapshot
                .map_or(exported.snapshot_commit_sequence, |old| {
                    old.min(exported.snapshot_commit_sequence)
                }),
        );
        state.collection = collection;
        state.after = after;
        state.complete = complete;
        Ok(if complete {
            NativeTargetImageVerificationProgress::Complete
        } else {
            NativeTargetImageVerificationProgress::Scanning
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finish_native_target_image_verification(
        &self,
        state: NativeTargetImageVerifier,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
        role: &NativeImageInstallRole<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedNativeTargetImageContents> {
        self.require_native_target_image_scope(
            fence,
            admission,
            frozen,
            bindings,
            role,
            source,
            state.recover_completed,
            cancellation,
        )?;
        state.require_current(self, fence, admission, bindings, source)?;
        if !state.complete
            || !state.history_audit_complete
            || self.replication_shippable_seq() < state.reserved
            || self.current_visibility_watermark() < state.reserved
            || state.records != state.expected_records
            || state.bytes != state.expected_bytes
            || hex::encode(state.content.clone().finalize()) != state.expected_root
        {
            return Err(invalid(
                "target complete-content proof is incomplete/not locally visible and durable",
            ));
        }
        Ok(VerifiedNativeTargetImageContents { state })
    }

    /// Publish only after the actual bounded complete-image producer finished.
    pub(crate) fn finish_native_target_image_install(
        &self,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
        role: &NativeImageInstallRole<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        recover_completed: bool,
        contents: VerifiedNativeTargetImageContents,
        history: Option<(&StagedNativeHistoryProof, &NativeHistoryTargetContext<'_>)>,
        cancellation: &CancellationToken,
    ) -> Result<NativeInstalledRangeImageBase> {
        let seal = source.seal();
        self.require_native_target_image_scope(
            fence,
            admission,
            frozen,
            bindings,
            role,
            source,
            recover_completed,
            cancellation,
        )?;
        contents.require_current(self, fence, admission, bindings, source)?;
        if recover_completed {
            let prior = self
                .native_installed_range_image_base(admission, fence.range().id)?
                .ok_or_else(|| invalid("completed recovery lost its actual native base"))?;
            if prior.receipt.intent_sha256 != fence.intent_sha256()
                || prior.receipt.attempt_id != fence.attempt_id()
                || prior.receipt.source_native_receipt_sha256 != seal.source_native_receipt_sha256
                || prior.receipt.logical_genesis_sha256 != seal.logical_genesis_sha256
                || prior.receipt.logical_history_sha256 != seal.logical_history_sha256
                || prior.receipt.image_root_sha256 != seal.image_root_sha256
            {
                return Err(invalid("completed recovery base/source scope differs"));
            }
            return Ok(prior);
        }
        let (history, history_context) = history
            .ok_or_else(|| invalid("native publication requires actual full staged history"))?;
        let reserved = contents.state.reserved;
        let min_snapshot = contents.state.min_snapshot;
        let records = contents.state.records;
        let bytes = contents.state.bytes;
        let catalog_sha256 = contents.state.catalog_sha256.clone();
        self.validate_range_image_bindings(bindings)?;
        self.range_mutations
            .validate_quiescent_frozen(fence.range().id, frozen)?;
        source.require_current()?;
        fence.require_live_role(role)?;
        cancellation.check()?;
        let (global_image_generation, range_image_generation) =
            self.native_range_image_generation_pair(fence.range().id)?;
        let intended = fence.intended_activation_range().clone();
        let intended_activation_members = fence.intended_activation_members().clone();
        // This is the captured old-data membership only. Intended future
        // credentials/activation still require Root's separate durable proof.
        let base_members = role
            .role()
            .member_credentials()
            .iter()
            .map(|(id, member)| {
                (
                    id.clone(),
                    RangeImageBirthMember {
                        incarnation: member.incarnation,
                        tls_certificate_sha256: member.active_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: member.pending_certificate_sha256.clone(),
                    },
                )
            })
            .collect();
        let mut published_receipt = None;
        self.with_staged_native_history_publication(
            history,
            history_context,
            source,
            &contents,
            bindings,
            cancellation,
            |paged, xid, history_binding| {
                let mut receipt = NativeInstalledRangeImageReceipt {
                    format_version: INSTALL_FORMAT,
                    registered_final_image: None,
                    cluster_id: fence.cluster_id().clone(),
                    bootstrap_id: fence.bootstrap_id(),
                    target_node_id: admission.local_node_id().clone(),
                    target_member_incarnation: admission.local_member_incarnation(),
                    birth_id: admission.birth_id(),
                    birth_sha256: admission.birth_sha256().to_string(),
                    purpose: fence.purpose(),
                    capture_range: fence.range().clone(),
                    intended_activation_range: intended,
                    base_members,
                    intended_activation_members,
                    restore_activation: admission.restore_activation().cloned(),
                    range_index: fence.source_range_index(),
                    history_generation: history_binding.history_generation(),
                    logical_genesis_sha256: history_binding.logical_genesis_sha256().to_string(),
                    logical_history_sha256: history_binding.logical_history_sha256().to_string(),
                    history_verified_through: history_binding.verified_through(),
                    attempt_id: fence.attempt_id(),
                    session_id: fence.session_id(),
                    nonce: fence.nonce(),
                    source_purpose: fence.source_purpose().clone(),
                    intent_sha256: fence.intent_sha256().to_string(),
                    source_native_receipt_sha256: seal.source_native_receipt_sha256.clone(),
                    old_config_sha256: fence.old_config_sha256().to_string(),
                    intended_config_sha256: fence.intended_config_sha256().to_string(),
                    schema_sha256: bindings.schema_sha256().to_string(),
                    reader_features: bindings.reader_features().to_vec(),
                    portable_catalog_sha256: catalog_sha256,
                    image_root_sha256: seal.image_root_sha256.clone(),
                    local_namespace_ids: bindings.namespace_ids(),
                    global_image_generation,
                    range_image_generation,
                    reserved_db_upper_bound: reserved,
                    min_snapshot_db_sequence: min_snapshot.unwrap_or(reserved),
                    record_count: records,
                    serialized_record_bytes: bytes,
                    native_page_xid: 0,
                    checksum_sha256: String::new(),
                };
                receipt.native_page_xid = xid;
                receipt.checksum_sha256 = receipt.checksum()?;
                let encoded = bounded_json(&receipt, MAX_NATIVE_RECEIPT_BYTES)?;
                paged
                    .store()
                    .put(xid, &native_key(&base_key(fence.range().id)), &encoded)
                    .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
                paged
                    .store()
                    .delete(xid, &native_key(&active_key(fence.range().id)))
                    .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
                published_receipt = Some(receipt);
                Ok(())
            },
        )?;
        let receipt =
            published_receipt.ok_or_else(|| invalid("native publisher produced no receipt"))?;
        receipt.validate()?;
        let base = NativeInstalledRangeImageBase {
            receipt,
            database_incarnation: self.incarnation.clone(),
            schema_generation: self.schema_compatibility.generation(),
        };
        base.require_current(self, admission)?;
        Ok(base)
    }

    pub(crate) fn native_installed_range_image_integrity(
        &self,
        admission: &NativeRecoveryAdmission,
        range_id: RangeId,
    ) -> Result<Option<NativeInstalledRangeImageIntegrity>> {
        admission.require_current(self)?;
        let Some(receipt) = self
            .read_native_range_image_record::<NativeInstalledRangeImageReceipt>(&base_key(
                range_id,
            ))?
        else {
            return Ok(None);
        };
        receipt.validate()?;
        if receipt.capture_range.id != range_id {
            return Err(invalid("installed integrity range mismatch"));
        }
        let integrity = NativeInstalledRangeImageIntegrity {
            receipt,
            database_incarnation: self.incarnation.clone(),
        };
        integrity.require_current(self, admission)?;
        Ok(Some(integrity))
    }

    pub(crate) fn native_installed_range_image_base(
        &self,
        admission: &NativeRecoveryAdmission,
        range_id: RangeId,
    ) -> Result<Option<NativeInstalledRangeImageBase>> {
        admission.require_current(self)?;
        if self.native_range_image_installing(range_id)? {
            return Err(invalid(
                "unfinished native image install is quarantined; no completed anchor",
            ));
        }
        let Some(receipt) = self
            .read_native_range_image_record::<NativeInstalledRangeImageReceipt>(&base_key(
                range_id,
            ))?
        else {
            return Ok(None);
        };
        receipt.validate()?;
        if receipt.capture_range.id != range_id {
            return Err(invalid("installed receipt range mismatch"));
        }
        let base = NativeInstalledRangeImageBase {
            receipt,
            database_incarnation: self.incarnation.clone(),
            schema_generation: self.schema_compatibility.generation(),
        };
        base.require_current(self, admission)?;
        Ok(Some(base))
    }

    pub(crate) fn require_native_image_install_restartable(
        &self,
        range_id: RangeId,
        attempt_id: Uuid,
        intent_sha256: &str,
    ) -> Result<()> {
        if let Some(completed) = self
            .read_native_range_image_record::<NativeInstalledRangeImageReceipt>(&base_key(
                range_id,
            ))?
        {
            completed.validate()?;
            if completed.attempt_id == attempt_id || completed.intent_sha256 == intent_sha256 {
                return Err(invalid(
                    "completed native image cannot be superseded as an incomplete attempt",
                ));
            }
        }
        Ok(())
    }

    /// Native marker before any row mutation. A previous partial attempt is
    /// restarted only by a replacement durable catalog fence; JSON progress
    /// never authorizes resume or completion.
    pub(crate) fn begin_native_target_image_install(
        &self,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
    ) -> Result<()> {
        admission.require_current(self)?;
        self.range_mutations
            .validate_frozen(fence.range().id, frozen)?;
        self.validate_range_image_bindings(bindings)?;
        if frozen.owner() != fence.attempt_id()
            || admission.database_incarnation() != fence.database_incarnation()
            || admission.birth_id() != fence.target_birth_id()
            || admission.birth_sha256() != fence.target_birth_sha256()
        {
            return Err(invalid(
                "stale native install token or previous partial attempt; quarantine required",
            ));
        }
        if let Some(previous) =
            self.read_native_range_image_record::<NativeInstalling>(&active_key(fence.range().id))?
        {
            if previous.format_version != INSTALL_FORMAT
                || (previous.session_id != fence.session_id()
                    && !fence.matches_replaced_installing_marker(
                        previous.session_id,
                        previous.attempt_id,
                        &previous.intent_sha256,
                        previous.source_range_index,
                    ))
                || previous.birth_id != admission.birth_id()
                || previous.birth_sha256 != admission.birth_sha256()
                || (previous.source_range_index != fence.source_range_index()
                    && !fence.matches_replaced_installing_marker(
                        previous.session_id,
                        previous.attempt_id,
                        &previous.intent_sha256,
                        previous.source_range_index,
                    ))
            {
                return Err(invalid(
                    "previous native Installing belongs to another lineage",
                ));
            }
            if previous.attempt_id == fence.attempt_id()
                && (previous.local_namespace_ids != bindings.namespace_ids()
                    || previous.intent_sha256 != fence.intent_sha256())
            {
                return Err(invalid("same native install attempt cannot retarget a recreated namespace or changed intent"));
            }
            self.require_native_image_install_restartable(
                fence.range().id,
                previous.attempt_id,
                &previous.intent_sha256,
            )?;
        }
        let _controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        self.write_native_range_image_record(
            &active_key(fence.range().id),
            &NativeInstalling {
                format_version: INSTALL_FORMAT,
                attempt_id: fence.attempt_id(),
                session_id: fence.session_id(),
                birth_id: admission.birth_id(),
                birth_sha256: admission.birth_sha256().to_string(),
                intent_sha256: fence.intent_sha256().to_string(),
                source_range_index: fence.source_range_index(),
                local_namespace_ids: bindings.namespace_ids(),
            },
        )
    }
}

impl BicDb {
    pub(in crate::db) fn native_image_row_marker_commitment(
        &self,
        range: RangeId,
        attempt: Uuid,
    ) -> Result<String> {
        let marker: NativeInstalling = self
            .read_native_range_image_record(&active_key(range))?
            .ok_or_else(|| invalid("native image row capability has no active installation"))?;
        if marker.format_version != INSTALL_FORMAT || marker.attempt_id != attempt {
            return Err(invalid("native image row installation attempt changed"));
        }
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &marker,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
}
