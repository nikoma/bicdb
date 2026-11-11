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
        if self.format_version != INSTALL_FORMAT
            || self.bootstrap_id.is_nil()
            || self.birth_id.is_nil()
            || self.attempt_id.is_nil()
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.target_member_incarnation == 0
            || self.native_page_xid == 0
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

    /// Recompute the complete actual target image under the still-owned
    /// freeze, then order its native receipt after every data/index page txn.
    /// Source wire receipts and native row counts alone cannot call this path.
    pub(crate) fn finish_native_target_image_install(
        &self,
        fence: &TargetImageInstallFence,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        bindings: &RangeImageDbBindings,
        role: &NativeImageInstallRole<'_>,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        recover_completed: bool,
        cancellation: &CancellationToken,
    ) -> Result<NativeInstalledRangeImageBase> {
        use crate::distribution_range_image::content_hasher;
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
        let reserved = self.current_commit_seq();
        if self.replication_shippable_seq() < reserved {
            return Err(invalid(
                "target actual durable prefix has not reached captured B; retry outside DB locks",
            ));
        }
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
        let mut content = content_hasher(&catalog);
        let mut records = 0u64;
        let mut bytes = 0u64;
        let mut min_snapshot = None::<u64>;
        let options = crate::distribution::RangeSnapshotOptions::default();
        // The scan includes every target key, including keys never mentioned
        // by a copied batch. Thus extras cannot hide behind the source cursor.
        for meta in bindings.collections() {
            let mut after = None::<String>;
            loop {
                cancellation.check()?;
                fence.require_live_role(role)?;
                self.validate_range_image_bindings(bindings)?;
                let export = self.for_each_range_snapshot_batch_with_cancellation(
                    &meta.name,
                    fence.range(),
                    after.as_deref(),
                    &options,
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
                if export.snapshot_commit_sequence < reserved {
                    return Err(invalid("actual target snapshot below captured target B"));
                }
                min_snapshot = Some(min_snapshot.map_or(export.snapshot_commit_sequence, |old| {
                    old.min(export.snapshot_commit_sequence)
                }));
                if export.completed {
                    break;
                }
                if export.resume_after_key <= after {
                    return Err(invalid("target complete scan failed to advance"));
                }
                after = export.resume_after_key;
            }
        }
        if records != seal.record_count
            || bytes != seal.serialized_record_bytes
            || hex::encode(content.finalize()) != seal.image_root_sha256
        {
            return Err(invalid(
                "actual complete target contents differ from completed source (including extras)",
            ));
        }
        self.validate_range_image_bindings(bindings)?;
        self.range_mutations
            .validate_quiescent_frozen(fence.range().id, frozen)?;
        source.require_current()?;
        fence.require_live_role(role)?;
        cancellation.check()?;
        let (global_image_generation, range_image_generation) =
            self.native_range_image_generation_pair(fence.range().id)?;
        let intended = source.cut().intended_activation_range().clone();
        let intended_activation_members = source.cut().intended_activation_members().clone();
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
        let mut receipt = NativeInstalledRangeImageReceipt {
            format_version: INSTALL_FORMAT,
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
        // The receipt's own page transaction orders/fsyncs the prior actual
        // image/index page prefix. This is not one all-image atomic SQL txn.
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        self.in_paged_transaction(|paged, xid| {
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
            Ok(())
        })?
        .ok_or_else(|| invalid("installed receipt has no actual native page transaction"))?;
        drop(controlled);
        receipt.validate()?;
        let base = NativeInstalledRangeImageBase {
            receipt,
            database_incarnation: self.incarnation.clone(),
            schema_generation: self.schema_compatibility.generation(),
        };
        base.require_current(self, admission)?;
        Ok(base)
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
                || previous.session_id != fence.session_id()
                || previous.birth_id != admission.birth_id()
                || previous.birth_sha256 != admission.birth_sha256()
                || previous.source_range_index != fence.source_range_index()
            {
                return Err(invalid(
                    "previous native Installing belongs to another lineage",
                ));
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
