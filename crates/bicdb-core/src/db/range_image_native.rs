//! Native birth and empty-genesis receipts for explicitly new image-aware DBs.
//!
//! Hidden records use the page store's own WAL/root. They are not public
//! collections and cannot be supplied as JSON files to manufacture lineage.
mod learner_schema;
mod listener_identity;
use super::*;
use crate::distribution::{ClusterId, ClusterNodeId, RangeId, RangeReplicaRole};
use crate::distribution_range_image::{
    bounded_json, canonical_range_image_catalog, content_hasher,
};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

mod abort_catalogue;
pub(crate) use abort_catalogue::NativeAbortCatalogueBootstrap;
pub(crate) mod abort_history;
mod certified_prefix;
#[cfg(test)]
pub(super) use certified_prefix::test_record_origin_release;
#[cfg(test)]
pub(crate) use certified_prefix::{
    NativeOriginAppSpan, NativeOriginFaultArm, NativeOriginFaultProbe,
};
mod history;
mod initial_data;
pub(crate) use initial_data::NativeInitialDataGenesis;
mod history_stage;
pub(crate) use history_stage::ClosedImageOriginalWriter;
mod installed_anchor;
mod native_read;
mod registered_receiver_domain;
mod writer_enrollment;
#[cfg(feature = "tls-replication")]
pub(crate) use writer_enrollment::NativeRegisteredClosedPersistScope;
pub(crate) use writer_enrollment::{
    InstalledNativeWriterHostGuard, NativeRegisteredReceiverHook, NativeWriterHostState,
};
mod serving_configuration;
pub(crate) use native_read::{
    NativeRangeReadPhysical, NativeReadLocalDbSequence, NativeServingObservationScope,
};
pub(crate) use serving_configuration::{
    HistoricalPendingNativeActivationFacts, NativeActivationDecisionReceipt,
    NativeActivationReadScope, NativeCompletionReadScope, NativeCurrentJointBinding,
    NativeCurrentJointRecoveryDomain, NativeJointBridgeReadScope, NativeJointDecisionReceipt,
    NativeJointHistoricalCutScope, NativeJointOldCloseReceipt, NativeJointOldPhysicalProof,
    NativeJointPreparedPublicationScope, NativeJointServingBridgeReceipt,
    NativeJointServingPreparation, NativeReadyObserverReadScope, NativeRecoveredServingReceipt,
};
use serving_configuration::{NativeJointServingDomain, NativeRecoveredServingConfiguration};
#[cfg(test)]
pub(crate) mod rf3_fixture;
use certified_prefix::NativeCertifiedRangePrefix;
pub(crate) use certified_prefix::NativeRecoveryAbortedReceipt;
pub(crate) use history::{
    NativeLogicalHistoryEntryV1, NativeLogicalHistoryOutcomeV1, NativeLogicalHistoryPoint,
    NativeLogicalHistoryStepV1,
};
pub(crate) use history_stage::{
    NativeHistoryPublicationBinding, NativeHistoryStage, NativeHistoryStageProgress,
    NativeHistoryTargetContext, NativeImageRowScope, NativeImageTransactionGrant,
    StagedNativeHistoryProof,
};
use installed_anchor::{NativeRangeBaseBinding, NativeRangeImageAnchor};

#[cfg(test)]
thread_local! {
    static TEST_REJECT_NATIVE_AFTER_ADMISSION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static TEST_REJECT_NATIVE_AFTER_LOCAL_WAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) use certified_prefix::NativeRangeScopeRegistry;
pub(crate) use certified_prefix::{
    KnownNativeRecoveryPrefix, KnownPendingNativeReplicaPrefix, NativeRangeInstallSafety,
    NativeRangePendingApply, NativeRecoveryAppliedReceipt, NativeReplicaMaterialization,
};

pub(super) fn native_key(key: &str) -> Vec<u8> {
    let mut encoded = crate::paged_collection::RANGE_IMAGE_NATIVE_NS_V1.to_vec();
    encoded.extend_from_slice(key.as_bytes());
    encoded
}
const BIRTH_KEY: &str = "birth";
const READY_KEY: &str = "initialized";
const INVALIDATION_KEY: &str = "unscoped-invalidation";
const FACTORY_PREFLIGHT: &str = "range-image-factory.json";
pub(super) const MAX_NATIVE_RECEIPT_BYTES: usize = 8 * 1024 * 1024;
const MAX_GENESIS_RANGES: usize = 65_536;
const MAX_WAIT: Duration = Duration::from_secs(300);

/// Native journal serialization bound to the actual current DB open. It is
/// not a mutation, quorum or source-lineage authority.
pub(super) struct NativeImageJournalGuard<'a> {
    database_incarnation: &'a str,
    _guard: parking_lot::MutexGuard<'a, ()>,
}
impl NativeImageJournalGuard<'_> {
    fn require_database(&self, database: &BicDb) -> Result<()> {
        if self.database_incarnation != database.incarnation {
            return Err(invalid("native journal belongs to another DB open"));
        }
        Ok(())
    }
}

fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("native range image: {}", detail.into()))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeImageBirthMember {
    pub incarnation: u64,
    pub tls_certificate_sha256: Option<String>,
    pub pending_tls_certificate_sha256: Option<String>,
}

/// Trusted bootstrap input, not an authority. Only successful atomic creation
/// and native birth persistence produce the opaque factory authority below.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeImageBirthSpec {
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub initial_members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    pub restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    pub initial_ranges: Vec<RangeDescriptor>,
}
impl RangeImageBirthSpec {
    fn validate(&self) -> Result<()> {
        if self.bootstrap_id.is_nil()
            || self.member_incarnation == 0
            || self.initial_ranges.len() > MAX_GENESIS_RANGES
        {
            return Err(invalid("invalid fresh bootstrap identity/range bound"));
        }
        if self.initial_members.len() > MAX_GENESIS_RANGES
            || self
                .initial_members
                .get(&self.node_id)
                .map(|member| member.incarnation)
                != Some(self.member_incarnation)
        {
            return Err(invalid(
                "fresh bootstrap local incarnation/member map mismatch",
            ));
        }
        for member in self.initial_members.values() {
            let valid_pin = |pin: &Option<String>| {
                pin.as_ref().is_none_or(|pin| {
                    pin.len() == 64 && pin.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
            };
            if member.incarnation == 0
                || !valid_pin(&member.tls_certificate_sha256)
                || !valid_pin(&member.pending_tls_certificate_sha256)
            {
                return Err(invalid(
                    "fresh bootstrap member identity or TLS pin is invalid",
                ));
            }
        }
        if let Some(restore) = &self.restore_activation {
            restore.validate()?;
            if restore.cluster_id != self.cluster_id || !self.initial_ranges.is_empty() {
                return Err(invalid(
                    "restored cluster can add a fresh learner, but cannot initialize old data as empty genesis",
                ));
            }
        }
        let mut ids = BTreeSet::new();
        for range in &self.initial_ranges {
            for replica in &range.replicas {
                if !self.initial_members.contains_key(&replica.node_id) {
                    return Err(invalid(
                        "initial range refers to an unbound member identity",
                    ));
                }
            }
            if range.epoch != 1
                || !ids.insert(range.id)
                || range.voter_count() == 0
                || !range
                    .replicas
                    .iter()
                    .any(|r| r.node_id == self.node_id && r.role == RangeReplicaRole::Voter)
                || range.end_token.is_some_and(|end| end <= range.start_token)
            {
                return Err(invalid(
                    "fresh bootstrap needs unique initial epoch-1 local voter ranges",
                ));
            }
        }
        bounded_json(self, MAX_NATIVE_RECEIPT_BYTES)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeDatabaseBirth {
    format_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata_initialization_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    abort_protocol_version: Option<u32>,
    birth_id: Uuid,
    spec: RangeImageBirthSpec,
    image_generation: Uuid,
    checksum_sha256: String,
}
impl NativeDatabaseBirth {
    fn new(spec: RangeImageBirthSpec) -> Result<Self> {
        Self::new_with_writer_enforcement(spec, false, false)
    }
    fn new_with_writer_enforcement(
        spec: RangeImageBirthSpec,
        enforce: bool,
        metadata_initialization: bool,
    ) -> Result<Self> {
        spec.validate()?;
        let mut birth = Self {
            format_version: if enforce { 2 } else { 1 },
            metadata_initialization_version: metadata_initialization.then_some(1),
            abort_protocol_version: None,
            birth_id: Uuid::now_v7(),
            spec,
            image_generation: Uuid::now_v7(),
            checksum_sha256: String::new(),
        };
        birth.checksum_sha256 = birth.checksum()?;
        Ok(birth)
    }
    fn checksum(&self) -> Result<String> {
        if let Some(version) = self.abort_protocol_version {
            return Ok(hex::encode(sha2::Sha256::digest(bounded_json(
                &(
                    "NativeAbortProtocolFromBirth-v1",
                    self.format_version,
                    self.birth_id,
                    &self.spec,
                    self.image_generation,
                    self.metadata_initialization_version,
                    version,
                ),
                MAX_NATIVE_RECEIPT_BYTES,
            )?)));
        }
        if let Some(version) = self.metadata_initialization_version {
            return Ok(hex::encode(sha2::Sha256::digest(bounded_json(
                &(
                    "NativeMetadataInitializationFromBirth-v1",
                    self.format_version,
                    self.birth_id,
                    &self.spec,
                    self.image_generation,
                    version,
                ),
                MAX_NATIVE_RECEIPT_BYTES,
            )?)));
        }
        // Exact legacy birth-v1 and pre-capability birth-v2 checksum domain.
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &(
                self.format_version,
                self.birth_id,
                &self.spec,
                self.image_generation,
            ),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if !matches!(self.format_version, 1 | 2)
            || self.abort_protocol_version.is_some_and(|v| {
                v != 1
                    || self.format_version != 2
                    || self.metadata_initialization_version != Some(1)
            })
            || self
                .metadata_initialization_version
                .is_some_and(|v| v != 1 || self.format_version != 2)
            || self.birth_id.is_nil()
            || self.image_generation.is_nil()
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged native birth record"));
        }
        Ok(())
    }
}

/// A preflight refusal fence only. This sidecar never grants native lineage;
/// its exact hash is checked against the actual page-WAL birth after opening.
#[derive(Debug, Serialize, Deserialize)]
struct FactoryPreflight {
    format_version: u32,
    spec: RangeImageBirthSpec,
    native_birth_sha256: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
struct NativeImageInvalidation {
    birth_id: Uuid,
    image_generation: Uuid,
    reason: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct NativeInitializationComplete {
    birth_id: Uuid,
    birth_sha256: String,
    genesis_sha256: BTreeMap<RangeId, String>,
}

/// Not deserializable and not obtainable by opening an arbitrary empty DB.
/// Strict reopen reissues it only from the existing native birth and exact
/// expected cluster/bootstrap/member/config bindings.
#[derive(Debug)]
pub(crate) struct FreshRangeGenesisAuthority {
    birth: NativeDatabaseBirth,
    root: PathBuf,
}

/// Local database admission only. A fresh authenticated recovery-role witness
/// is additionally required by the recovery RPC. This value proves neither a
/// data quorum nor a readable range floor, and cannot be decoded from a DTO.
#[derive(Debug)]
pub(crate) struct NativeRecoveryAdmission {
    database_incarnation: String,
    writer_enforcement_version: u32,
    metadata_initialization_version: u32,
    abort_protocol_version: u32,
    birth_id: Uuid,
    birth_sha256: String,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    local_node_id: ClusterNodeId,
    local_member_incarnation: u64,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
}
impl NativeRecoveryAdmission {
    pub(crate) fn require_current(&self, database: &BicDb) -> Result<()> {
        let current = database.native_recovery_admission(
            &self.cluster_id,
            self.bootstrap_id,
            &self.local_node_id,
            self.local_member_incarnation,
        )?;
        if self.database_incarnation != current.database_incarnation
            || self.birth_id != current.birth_id
            || self.birth_sha256 != current.birth_sha256
            || self.writer_enforcement_version != current.writer_enforcement_version
            || self.metadata_initialization_version != current.metadata_initialization_version
            || self.abort_protocol_version != current.abort_protocol_version
        {
            return Err(invalid(
                "recovery admission belongs to another DB/open or native birth",
            ));
        }
        Ok(())
    }
    pub(crate) fn abort_protocol_version(&self) -> u32 {
        self.abort_protocol_version
    }
    pub(crate) fn metadata_initialization_version(&self) -> u32 {
        self.metadata_initialization_version
    }
    pub(crate) fn writer_enforcement_version(&self) -> u32 {
        self.writer_enforcement_version
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.local_node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.local_member_incarnation
    }
    pub(crate) fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.restore_activation.as_ref()
    }
    pub(crate) fn birth_id(&self) -> Uuid {
        self.birth_id
    }
    pub(crate) fn birth_sha256(&self) -> &str {
        &self.birth_sha256
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeGenesisReceipt {
    format_version: u32,
    birth_id: Uuid,
    birth_sha256: String,
    range: RangeDescriptor,
    base_members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    local_member_incarnation: u64,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    range_index: u64,
    source_database_incarnation: String,
    reserved_db_upper_bound: u64,
    actual_snapshot_db_sequence: u64,
    schema_sha256: String,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    portable_catalog_sha256: String,
    empty_image_root_sha256: String,
    reader_features: Vec<String>,
    local_namespace_ids: Vec<(String, Uuid)>,
    native_page_xid: u64,
    history_generation: Uuid,
    logical_genesis_sha256: String,
    checksum_sha256: String,
}
impl NativeGenesisReceipt {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        if self.format_version != 2
            || self.range.epoch != 1
            || self.local_member_incarnation == 0
            || self
                .local_namespace_ids
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
            || self.local_namespace_ids.iter().any(|(_, id)| id.is_nil())
            || self.range_index != 0
            || self.birth_id.is_nil()
            || self.history_generation.is_nil()
            || self.logical_genesis_sha256.len() != 64
            || self.actual_snapshot_db_sequence < self.reserved_db_upper_bound
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged native genesis receipt"));
        }
        Ok(())
    }
}

/// Read-only observation, never an authority/barrier. Unknown or invalid
/// native lineage has None indices; verified empty genesis is Some(0).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NativeRangePrefixObservation {
    pub range_id: RangeId,
    pub native_base_index: Option<u64>,
    pub native_applied_index: Option<u64>,
    pub pending_command_index: Option<u64>,
    pub materialized_pending_index: Option<u64>,
    pub pending_command_id: Option<String>,
    pub native_receipt_sha256: Option<String>,
    pub factory_initialization_complete: bool,
    pub invalidated: bool,
    pub diagnostic: Option<String>,
}
impl NativeRangePrefixObservation {
    fn unknown(range_id: RangeId, diagnostic: &str) -> Self {
        Self {
            range_id,
            native_base_index: None,
            native_applied_index: None,
            pending_command_index: None,
            materialized_pending_index: None,
            pending_command_id: None,
            native_receipt_sha256: None,
            factory_initialization_complete: false,
            invalidated: false,
            diagnostic: Some(diagnostic.to_string()),
        }
    }
}

/// Actual native genesis/base lineage, not merely a range-log counter. The
/// initial producer below verifies a fresh native birth and scans the range
/// empty under its owned freeze, then commits this receipt in the page WAL.
/// Suffix extension/installed-image producers are a separate required layer.
#[derive(Debug)]
pub(crate) struct SourceRangeImageLineage {
    birth: NativeDatabaseBirth,
    _anchor: NativeRangeImageAnchor,
    base: NativeRangeBaseBinding,
    prefix: Option<NativeCertifiedRangePrefix>,
    serving: Option<NativeRecoveredServingConfiguration>,
    joint: Option<NativeJointServingDomain>,
    current_database_incarnation: String,
    current_schema_generation: u64,
}

/// Only the atomic NEW factory constructs this short-lived capability. Strict
/// reopen returns no such token, even for a zero-index/no-policy prototype.
pub(crate) struct FreshNativeCatalogueBootstrap<'a> {
    database: &'a BicDb,
    birth: &'a NativeDatabaseBirth,
}
impl FreshNativeCatalogueBootstrap<'_> {
    pub(crate) fn require_non_abort_profile(&self) -> Result<()> {
        if self.birth.abort_protocol_version.is_some() {
            return Err(invalid(
                "abort-aware fresh factory must use protected catalogue StoreUUID",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_current(&self) -> Result<(PathBuf, ClusterId, ClusterNodeId)> {
        self.database.check_recovery_state()?;
        let actual = self
            .database
            .read_native_birth()?
            .ok_or_else(|| invalid("fresh catalogue has no actual native birth"))?;
        if actual != *self.birth
            || actual.format_version != 2
            || !format::native_catalogue_custody_active(self.database.data_path())?
        {
            return Err(invalid(
                "fresh catalogue capability changed native factory/birth",
            ));
        }
        Ok((
            self.database.data_path().to_path_buf(),
            actual.spec.cluster_id,
            actual.spec.node_id,
        ))
    }
}

impl FreshRangeGenesisAuthority {
    pub(crate) fn birth_id(&self) -> Uuid {
        self.birth.birth_id
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.birth.spec.cluster_id
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        &self.birth.spec.node_id
    }

    /// Actual NEW factory fact only. Missing old-disk enrollment is never
    /// repaired by this check. The metadata publication seam separately
    /// requires initial generation one or an unassigned learner.
    pub(crate) fn require_unwritten_enrollment_database(
        &self,
        database: &BicDb,
        topology: &crate::distribution::ClusterTopology,
    ) -> Result<NativeRecoveryAdmission> {
        self.birth.validate()?;
        topology.validate()?;
        let spec = &self.birth.spec;
        let actual = database.native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )?;
        if database.path.canonicalize()? != self.root
            || actual.birth_id != self.birth.birth_id
            || actual.birth_sha256 != self.birth.checksum_sha256
            || topology.cluster_id != spec.cluster_id
            || topology.range_image_bootstrap_id != Some(spec.bootstrap_id)
            || actual.restore_activation.as_ref() != spec.restore_activation.as_ref()
        {
            return Err(invalid(
                "enrollment requires the actual completed factory birth and initial domain",
            ));
        }
        for (id, member) in &spec.initial_members {
            let node = topology
                .nodes
                .get(id)
                .ok_or_else(|| invalid("initial enrollment member is absent"))?;
            if node.incarnation != member.incarnation
                || node.tls_certificate_sha256 != member.tls_certificate_sha256
                || node.pending_tls_certificate_sha256 != member.pending_tls_certificate_sha256
            {
                return Err(invalid("initial enrollment member identity differs"));
            }
        }
        let expected: BTreeMap<_, _> = spec
            .initial_ranges
            .iter()
            .map(|r| (r.id, crate::distribution_native_read::canonical_range(r)))
            .collect();
        let current: BTreeMap<_, _> = topology
            .ranges
            .values()
            .filter(|r| {
                r.replicas.iter().any(|replica| {
                    replica.node_id == spec.node_id && replica.role == RangeReplicaRole::Voter
                })
            })
            .map(|r| (r.id, crate::distribution_native_read::canonical_range(r)))
            .collect();
        if expected != current {
            return Err(invalid(
                "factory initial voter ranges differ from enrollment topology",
            ));
        }
        for range in &spec.initial_ranges {
            let point = database.native_range_prefix_observation(range.id)?;
            if point.native_applied_index != Some(0)
                || point.pending_command_index.is_some()
                || point.materialized_pending_index.is_some()
                || point.invalidated
            {
                return Err(invalid(
                    "bootstrap enrollment requires actual unwritten native genesis",
                ));
            }
        }
        Ok(actual)
    }
}
impl SourceRangeImageLineage {
    /// Target-local MVCC visibility bound from actual native materialization.
    /// This is neither range R nor a source-node sequence. Installed anchors
    /// must use their own actual target bound when that producer is composed.
    pub(crate) fn actual_local_visibility_sequence(&self) -> u64 {
        self.prefix
            .as_ref()
            .map_or(self.base.actual_local_visibility_sequence, |prefix| {
                prefix.actual_local_visibility_sequence()
            })
    }
    /// Identity from the actual protected native bridge, never metadata intent.
    pub(crate) fn current_joint_decision_sha256(&self) -> Option<&str> {
        self.joint.as_ref().map(|joint| joint.decision_sha256())
    }
    pub(crate) fn base_range(&self) -> &RangeDescriptor {
        &self.base.range
    }
    /// The physical capture anchor stays immutable. New ordinary writes use
    /// the independently native-WAL-persisted recovered serving configuration.
    pub(crate) fn native_write_range(&self) -> Result<&RangeDescriptor> {
        if let Some(joint) = &self.joint {
            return Ok(joint.range());
        }
        match &self.serving {
            Some(serving) if serving.binds_current_base(&self.base) => Ok(serving.range()),
            Some(_) => Err(invalid(
                "historical native serving floor has no current physical-base grant",
            )),
            None => Ok(&self.base.range),
        }
    }
    /// Physical voter credentials derived from the immutable descriptor,
    /// excluding unrelated metadata-only birth members. The birth map stays
    /// intact and no caller-selected member list can enter this projection.
    pub(crate) fn physical_range_members(
        &self,
    ) -> Result<BTreeMap<ClusterNodeId, RangeImageBirthMember>> {
        self.members_for_native_range(&self.base.range)
    }
    fn members_for_native_range(
        &self,
        range: &RangeDescriptor,
    ) -> Result<BTreeMap<ClusterNodeId, RangeImageBirthMember>> {
        let mut members = BTreeMap::new();
        for replica in range
            .replicas
            .iter()
            .filter(|r| r.role == RangeReplicaRole::Voter)
        {
            let member = self
                .base
                .base_members
                .get(&replica.node_id)
                .ok_or_else(|| {
                    invalid("actual native range voter has no bound birth credential")
                })?;
            if members
                .insert(replica.node_id.clone(), member.clone())
                .is_some()
            {
                return Err(invalid("actual native range has duplicate voter identity"));
            }
        }
        Ok(members)
    }
    /// Current ordinary data-voter domain. A joint override will be selected
    /// only from the independently protected native handoff bridge.
    pub(crate) fn native_write_members(
        &self,
    ) -> Result<BTreeMap<ClusterNodeId, RangeImageBirthMember>> {
        if let Some(joint) = &self.joint {
            return Ok(joint.members().clone());
        }
        self.members_for_native_range(self.native_write_range()?)
    }
    pub(crate) fn base_range_index(&self) -> u64 {
        self.base.range_index
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.prefix
            .as_ref()
            .map_or(self.base.range_index, |prefix| prefix.range_index())
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.birth.spec.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.birth.spec.bootstrap_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.birth.spec.node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.base.local_member_incarnation
    }
    pub(crate) fn base_members(&self) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        &self.base.base_members
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.base.restore_activation.as_ref()
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        self.prefix
            .as_ref()
            .map_or(self.base.checksum_sha256.as_str(), |prefix| {
                prefix.checksum_sha256()
            })
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.base.schema_sha256
    }
    pub(crate) fn history_generation(&self) -> Uuid {
        self.base.history_generation
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.base.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.prefix
            .as_ref()
            .map_or(self.base.logical_history_sha256.as_str(), |prefix| {
                prefix.logical_history_sha256()
            })
    }
}

fn genesis_key(range_id: RangeId) -> String {
    format!("genesis:{}", range_id.get())
}

impl BicDb {
    /// Until a qualified NEW-voter AbortQQ is composed, refuse NoData BEFORE
    /// its native WAL write whenever an actual protected joint domain exists.
    /// A matching configuration/SHA must not bypass this support boundary.
    pub(crate) fn require_native_no_data_without_joint_domain(&self, range: RangeId) -> Result<()> {
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("NoData support check has no actual native birth"))?;
        let anchor = self.actual_native_range_anchor(&birth, range)?;
        if self
            .read_native_joint_serving_domain(&birth, &anchor.binding())?
            .is_some()
        {
            return Err(invalid(
                "NoData after native joint activation requires a qualified NEW-voter abort domain",
            ));
        }
        Ok(())
    }
    pub(super) fn try_native_image_journal(&self) -> Result<NativeImageJournalGuard<'_>> {
        Ok(NativeImageJournalGuard {
            database_incarnation: &self.incarnation,
            _guard: self
                .native_range_image_journal
                .try_lock()
                .ok_or_else(|| invalid("native image journal busy"))?,
        })
    }
    #[cfg(test)]
    pub(crate) fn test_reject_next_native_after_admission() {
        TEST_REJECT_NATIVE_AFTER_ADMISSION.with(|reject| reject.set(true));
    }
    #[cfg(test)]
    pub(crate) fn test_reject_next_native_after_local_wal() {
        TEST_REJECT_NATIVE_AFTER_LOCAL_WAL.with(|reject| reject.set(true));
    }
    #[cfg(test)]
    pub(super) fn test_native_after_local_wal_rejected(&self) -> bool {
        TEST_REJECT_NATIVE_AFTER_LOCAL_WAL.with(|reject| reject.replace(false))
    }
    #[cfg(test)]
    pub(super) fn test_native_admission_rejected(&self, transaction: &Transaction) -> bool {
        TEST_REJECT_NATIVE_AFTER_ADMISSION.with(|reject| reject.replace(false))
            || self.test_consume_receiver_origin_fault(transaction)
    }
    pub(super) fn finish_failed_native_local_attempt(&self, transaction: &mut Transaction) {
        if transaction.state == TxState::Pending {
            self.tx_states.remove(&transaction.id.0);
            let locked_keys = transaction.locked_keys.get_mut().take_all();
            release_owned_write_locks(&self.write_locks, transaction.id, &locked_keys);
            transaction.state = TxState::Aborted;
        }
    }

    /// Mode selection only, never a birth, range or recovery authority.
    pub(crate) fn native_range_image_birth_present(&self) -> bool {
        self.range_image_native_birth_active
            .load(AtomicOrdering::Acquire)
    }

    /// Inputs are claimed identity bindings, not authority. The native birth,
    /// current-open identity and sticky fsync fence must independently match.
    pub(crate) fn native_recovery_admission(
        &self,
        cluster_id: &ClusterId,
        bootstrap_id: Uuid,
        local_node_id: &ClusterNodeId,
        local_member_incarnation: u64,
    ) -> Result<NativeRecoveryAdmission> {
        #[cfg(test)]
        let total_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-admission-total",
        );
        #[cfg(test)]
        let flags_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-admission-health-storage-reader-flags",
        );
        self.check_recovery_state()?;
        if !self.config.fsync
            || self.config.storage_mode != StorageMode::ServerPaged
            || !format::range_image_install_active(&self.path)?
            || self.range_image_initializing.load(AtomicOrdering::Acquire)
        {
            return Err(invalid(
                "native recovery requires completed fsync image-aware factory",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(flags_trace);
        #[cfg(test)]
        let birth_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-admission-actual-birth",
        );
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("native recovery has no actual database birth"))?;
        if bootstrap_id.is_nil()
            || &birth.spec.cluster_id != cluster_id
            || birth.spec.bootstrap_id != bootstrap_id
            || &birth.spec.node_id != local_node_id
            || birth.spec.member_incarnation != local_member_incarnation
        {
            return Err(invalid(
                "native recovery birth/bootstrap/local member binding mismatch",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(birth_trace);
        // An in-memory boolean is insufficient. Revalidate the actual ordered
        // completion marker and its immutable genesis inventory on every mint.
        #[cfg(test)]
        let ready_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-admission-ready-marker-and-all-initial-ranges",
        );
        let ready: NativeInitializationComplete =
            self.read_native_range_image_record(READY_KEY)?
                .ok_or_else(|| invalid("native recovery factory completion is missing"))?;
        if ready.birth_id != birth.birth_id
            || ready.birth_sha256 != birth.checksum_sha256
            || ready.genesis_sha256 != self.native_genesis_receipts(&birth)?
        {
            return Err(invalid(
                "native recovery factory completion does not match birth",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(ready_trace);
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(total_trace);
        Ok(NativeRecoveryAdmission {
            database_incarnation: self.incarnation.clone(),
            writer_enforcement_version: u32::from(birth.format_version == 2),
            metadata_initialization_version: birth.metadata_initialization_version.unwrap_or(0),
            abort_protocol_version: birth.abort_protocol_version.unwrap_or(0),
            birth_id: birth.birth_id,
            birth_sha256: birth.checksum_sha256,
            cluster_id: cluster_id.clone(),
            bootstrap_id,
            local_node_id: local_node_id.clone(),
            local_member_incarnation,
            restore_activation: birth.spec.restore_activation,
        })
    }

    pub(crate) fn create_new_range_image_database(
        root: &Path,
        config: DbConfig,
        spec: RangeImageBirthSpec,
    ) -> Result<(Self, FreshRangeGenesisAuthority)> {
        Self::create_new_range_image_database_with(root, config, spec, |_| Ok(()))
    }
    /// Fresh enforced-writer domain. Existing roots and birth-v1 upgrades are refused.
    pub(crate) fn create_new_enrolled_range_image_database(
        root: &Path,
        config: DbConfig,
        spec: RangeImageBirthSpec,
    ) -> Result<(Self, FreshRangeGenesisAuthority)> {
        Self::create_new_range_image_database_with_mode(
            root,
            config,
            spec,
            true,
            true,
            true,
            |_| Ok(()),
        )
    }
    /// Reproduces the ACTUAL pre-capability enforced factory ordering for
    /// compatibility negatives. It never creates fresh metadata authority.
    #[cfg(test)]
    pub(crate) fn test_create_pre_metadata_capability_enrolled_database(
        root: &Path,
        config: DbConfig,
        spec: RangeImageBirthSpec,
    ) -> Result<(Self, FreshRangeGenesisAuthority)> {
        Self::create_new_range_image_database_with_mode(
            root,
            config,
            spec,
            true,
            false,
            false,
            |_| Ok(()),
        )
    }
    fn create_new_range_image_database_with(
        root: &Path,
        config: DbConfig,
        spec: RangeImageBirthSpec,
        after_flags: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<(Self, FreshRangeGenesisAuthority)> {
        Self::create_new_range_image_database_with_mode(
            root,
            config,
            spec,
            false,
            false,
            false,
            after_flags,
        )
    }
    fn create_new_range_image_database_with_mode(
        root: &Path,
        config: DbConfig,
        spec: RangeImageBirthSpec,
        enforce: bool,
        metadata_initialization: bool,
        abort_protocol: bool,
        after_flags: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<(Self, FreshRangeGenesisAuthority)> {
        spec.validate()?;
        if !config.fsync || config.storage_mode != StorageMode::ServerPaged {
            return Err(invalid(
                "fresh image-aware database requires fsync server_paged",
            ));
        }
        // Atomic refusal is essential: emptiness/epoch1 does not establish
        // freshness. Never delete or overwrite an existing/partial directory.
        fs::create_dir(root).map_err(|error| {
            invalid(format!("atomic fresh-directory creation refused: {error}"))
        })?;
        let parent = root
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        crate::storage::write_atomic(
            &root.join(FACTORY_PREFLIGHT),
            &bounded_json(
                &FactoryPreflight {
                    format_version: 1,
                    spec: spec.clone(),
                    native_birth_sha256: None,
                },
                MAX_NATIVE_RECEIPT_BYTES,
            )?,
            true,
        )?;
        let mut db = Self::open_with_config(root, config.with_required_commit_admission(true))?;
        db.activate_local_namespace_identity_v1()?;
        format::enable_range_image_install(root)?;
        if enforce {
            format::enable_current_image_proposal(root)?;
            format::enable_native_writer_enrollment(root)?;
            format::enable_native_catalogue_custody(root)?;
            if metadata_initialization {
                format::enable_native_metadata_store(root)?;
            }
        }
        if abort_protocol {
            format::enable_native_abort_history(root)?;
        }
        after_flags(&mut db)?;
        let mut birth = NativeDatabaseBirth::new_with_writer_enforcement(
            spec,
            enforce,
            metadata_initialization,
        )?;
        birth.abort_protocol_version = abort_protocol.then_some(1);
        birth.checksum_sha256 = birth.checksum()?;
        db.write_native_range_image_record(BIRTH_KEY, &birth)?;
        crate::storage::write_atomic(
            &root.join(FACTORY_PREFLIGHT),
            &bounded_json(
                &FactoryPreflight {
                    format_version: 1,
                    spec: birth.spec.clone(),
                    native_birth_sha256: Some(birth.checksum_sha256.clone()),
                },
                MAX_NATIVE_RECEIPT_BYTES,
            )?,
            true,
        )?;
        if enforce {
            let fresh = FreshNativeCatalogueBootstrap {
                database: &db,
                birth: &birth,
            };
            fresh.require_current()?;
            if birth.abort_protocol_version == Some(1) {
                // One actual catalogue owner: protected Pending→exact15→
                // Registered BEFORE factory exposure, never an empty12 retrofit.
                db.initialize_native_abort_catalogue(&birth)?;
            } else {
                crate::distribution_range_consensus::RangeWriteStore::initialize_fresh_native_catalogue(&fresh)?;
            }
        }
        db.finish_native_genesis_initialization(&birth)?;
        let authority = FreshRangeGenesisAuthority {
            birth,
            root: root.canonicalize()?,
        };
        Ok((db, authority))
    }

    pub(crate) fn open_range_image_database(
        root: &Path,
        config: DbConfig,
        expected: &RangeImageBirthSpec,
    ) -> Result<(Self, FreshRangeGenesisAuthority)> {
        expected.validate()?;
        if !config.fsync || config.storage_mode != StorageMode::ServerPaged {
            return Err(invalid(
                "strict image-aware reopen requires fsync server_paged",
            ));
        }
        // This JSON can only refuse an open, never attest a base. Native
        // page-WAL birth verification below is still mandatory.
        let file = File::open(root.join(FACTORY_PREFLIGHT)).map_err(|_| {
            invalid("existing/partial root has no completed factory preflight; do not reuse it")
        })?;
        let mut bytes = Vec::new();
        file.take((MAX_NATIVE_RECEIPT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_NATIVE_RECEIPT_BYTES {
            return Err(invalid("factory preflight bound exceeded"));
        }
        let preflight: FactoryPreflight = serde_json::from_slice(&bytes)?;
        if preflight.format_version != 1
            || &preflight.spec != expected
            || preflight.native_birth_sha256.is_none()
            || !root.is_dir()
            || !format::range_image_install_active(root)?
        {
            return Err(invalid(
                "incomplete/mismatched factory; strict reopen refuses before engine recovery",
            ));
        }
        let db = Self::open_with_config(root, config.with_required_commit_admission(true))?;
        let birth = db
            .read_native_birth()?
            .ok_or_else(|| invalid("incomplete factory: native birth is missing"))?;
        if &birth.spec != expected
            || Some(&birth.checksum_sha256) != preflight.native_birth_sha256.as_ref()
        {
            return Err(invalid(
                "native birth cluster/bootstrap/member/config differs; authorized restore required",
            ));
        }
        let authority = FreshRangeGenesisAuthority {
            birth,
            root: root.canonicalize()?,
        };
        Ok((db, authority))
    }

    pub(super) fn write_native_range_image_record(
        &self,
        key: &str,
        value: &impl Serialize,
    ) -> Result<()> {
        self.check_recovery_state()?;
        if !self.config.fsync || !format::range_image_install_active(&self.path)? {
            return Err(invalid(
                "native receipt requires the sticky fsync reader fence",
            ));
        }
        let encoded = bounded_json(value, MAX_NATIVE_RECEIPT_BYTES)?;
        let key = native_key(key);
        self.in_paged_transaction(|paged, xid| {
            paged
                .store()
                .put(xid, &key, &encoded)
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native receipt has no page store"))?;
        Ok(())
    }
    pub(super) fn read_native_range_image_record<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        self.check_recovery_state()?;
        if !format::range_image_install_active(&self.path)? {
            return Ok(None);
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("native image reader has no page store"))?;
        let encoded = paged
            .store()
            .get_as_of(&paged.latest_snapshot(), &native_key(key))
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
        encoded
            .map(|encoded| {
                if encoded.len() > MAX_NATIVE_RECEIPT_BYTES {
                    return Err(invalid("native receipt byte bound exceeded"));
                }
                Ok(serde_json::from_slice(&encoded)?)
            })
            .transpose()
    }
    fn read_native_birth(&self) -> Result<Option<NativeDatabaseBirth>> {
        let birth: Option<NativeDatabaseBirth> = self.read_native_range_image_record(BIRTH_KEY)?;
        if let Some(birth) = &birth {
            birth.validate()?;
        }
        let enforced = format::native_writer_enrollment_active(&self.path)?;
        if birth
            .as_ref()
            .is_some_and(|birth| birth.format_version == 2)
            != enforced
        {
            return Err(invalid("native writer enrollment must be fenced from birth; partial factory or legacy upgrade refused"));
        }
        let abort_fenced = format::native_abort_history_active(&self.path)?;
        if birth
            .as_ref()
            .is_some_and(|b| b.abort_protocol_version == Some(1))
            != abort_fenced
        {
            return Err(invalid(
                "abort protocol capability must be fenced from birth; retrofit refused",
            ));
        }
        let metadata_fenced = format::native_metadata_store_active(&self.path)?;
        if birth
            .as_ref()
            .is_some_and(|b| b.metadata_initialization_version == Some(1))
            != metadata_fenced
        {
            return Err(invalid(
                "metadata initialization capability must be fenced from birth; retrofit refused",
            ));
        }
        Ok(birth)
    }
    fn native_range_image_generations(
        &self,
        birth: &NativeDatabaseBirth,
        range_id: RangeId,
    ) -> Result<(Uuid, Uuid)> {
        let read = |key: &str| -> Result<Uuid> {
            let invalidation: Option<NativeImageInvalidation> =
                self.read_native_range_image_record(key)?;
            if let Some(invalidation) = invalidation {
                if invalidation.birth_id != birth.birth_id || invalidation.image_generation.is_nil()
                {
                    return Err(invalid(
                        "native invalidation belongs to another database birth",
                    ));
                }
                Ok(invalidation.image_generation)
            } else {
                Ok(birth.image_generation)
            }
        };
        Ok((
            read(INVALIDATION_KEY)?,
            read(&format!("range-invalidation:{}", range_id.get()))?,
        ))
    }
    pub(super) fn native_range_image_generation_pair(
        &self,
        range_id: RangeId,
    ) -> Result<(Uuid, Uuid)> {
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("image generation has no native birth"))?;
        self.native_range_image_generations(&birth, range_id)
    }

    /// The installer writes this marker durably BEFORE altering any image
    /// row. Mere marker presence (even an unfamiliar version) closes lineage
    /// until the actual completed native base transaction removes it.
    pub(super) fn native_range_image_installing(&self, range_id: RangeId) -> Result<bool> {
        Ok(self
            .read_native_range_image_record::<serde_json::Value>(&format!(
                "install:active:{}",
                range_id.get()
            ))?
            .is_some())
    }
    /// Must finish durably BEFORE physical mutation. A known-range operation
    /// invalidates only its own base; unscoped imports/DDL invalidate all. No
    /// local mutation permit, raw DTO or counter re-establishes the floor.
    pub(super) fn invalidate_native_range_images_before_mutation(
        &self,
        range_id: Option<RangeId>,
        reason: &str,
    ) -> Result<()> {
        if !self
            .range_image_native_birth_active
            .load(AtomicOrdering::Acquire)
        {
            return Ok(());
        }
        let Some(birth) = self.read_native_birth()? else {
            return Ok(());
        };
        if reason.len() > 256 {
            return Err(invalid("invalidation reason bound exceeded"));
        }
        // Invalidate actual-open native serving before the first persistent
        // native-generation change, including unscoped imports/DDL. The token
        // never survives a generation replacement even if the DB stays open.
        match range_id {
            Some(range) => self.range_mutations.invalidate_serving_generation(range),
            None => self.range_mutations.invalidate_all_serving_generations(),
        }
        let key = range_id
            .map(|range| format!("range-invalidation:{}", range.get()))
            .unwrap_or_else(|| INVALIDATION_KEY.to_string());
        self.write_native_range_image_record(
            &key,
            &NativeImageInvalidation {
                birth_id: birth.birth_id,
                image_generation: Uuid::now_v7(),
                reason: reason.to_string(),
            },
        )
    }

    fn native_genesis_receipts(
        &self,
        birth: &NativeDatabaseBirth,
    ) -> Result<BTreeMap<RangeId, String>> {
        #[cfg(test)]
        let inventory_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-admission-all-genesis-inventory",
            );
        let mut receipts = BTreeMap::new();
        for range in &birth.spec.initial_ranges {
            #[cfg(test)]
            let range_trace =
                crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                    "refresh-admission-one-genesis-validation",
                );
            #[cfg(test)]
            if range_trace.is_some() {
                eprintln!(
                    "ACTUAL_REFRESH_GENESIS_RANGE range={} initial_ranges={}",
                    range.id,
                    birth.spec.initial_ranges.len()
                );
            }
            let receipt: Option<NativeGenesisReceipt> =
                self.read_native_range_image_record(&genesis_key(range.id))?;
            if let Some(receipt) = receipt {
                receipt.validate()?;
                self.validate_native_genesis_history(birth, &receipt)?;
                if receipt.birth_id != birth.birth_id
                    || receipt.birth_sha256 != birth.checksum_sha256
                    || receipt.range != *range
                    || receipt.base_members != birth.spec.initial_members
                    || receipt.local_member_incarnation != birth.spec.member_incarnation
                    || receipt.restore_activation != birth.spec.restore_activation
                {
                    return Err(invalid(
                        "genesis receipt does not match its native birth/config",
                    ));
                }
                receipts.insert(range.id, receipt.checksum_sha256);
            }
            #[cfg(test)]
            crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(range_trace);
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(inventory_trace);
        Ok(receipts)
    }
    pub(super) fn load_native_range_image_admission(&self) -> Result<()> {
        let Some(birth) = self.read_native_birth()? else {
            self.range_image_native_birth_active
                .store(false, AtomicOrdering::Release);
            return Ok(());
        };
        self.range_image_native_birth_active
            .store(true, AtomicOrdering::Release);
        let ready: Option<NativeInitializationComplete> =
            self.read_native_range_image_record(READY_KEY)?;
        if ready.is_some() && birth.abort_protocol_version == Some(1) {
            self.native_abort_catalogue_store_id()?;
        }
        if let Some(ready) = ready {
            if ready.birth_id != birth.birth_id
                || ready.birth_sha256 != birth.checksum_sha256
                || ready.genesis_sha256.len() != birth.spec.initial_ranges.len()
                || self.native_genesis_receipts(&birth)? != ready.genesis_sha256
            {
                return Err(invalid(
                    "completed native genesis is damaged; never reinitialize from emptiness",
                ));
            }
            self.range_image_initializing
                .store(false, AtomicOrdering::Release);
        } else {
            // Even all receipts without the final ordered native marker stay
            // closed until the idempotent initialization retry seals it.
            self.range_image_initializing
                .store(true, AtomicOrdering::Release);
        }
        Ok(())
    }
    fn finish_native_genesis_initialization(&self, birth: &NativeDatabaseBirth) -> Result<()> {
        let receipts = self.native_genesis_receipts(birth)?;
        if receipts.len() == birth.spec.initial_ranges.len() {
            self.initialize_native_abort_catalogue(birth)?;
            self.write_native_range_image_record(
                READY_KEY,
                &NativeInitializationComplete {
                    birth_id: birth.birth_id,
                    birth_sha256: birth.checksum_sha256.clone(),
                    genesis_sha256: receipts,
                },
            )?;
        }
        self.load_native_range_image_admission()
    }

    pub(crate) fn native_range_prefix_observation(
        &self,
        range_id: RangeId,
    ) -> Result<NativeRangePrefixObservation> {
        self.check_recovery_state()?;
        if self.native_range_image_installing(range_id)? {
            let mut observation = NativeRangePrefixObservation::unknown(
                range_id,
                "native image installation is incomplete",
            );
            observation.invalidated = true;
            return Ok(observation);
        }
        let lineage = match self.native_range_source_lineage_contract(range_id) {
            Ok(lineage) => lineage,
            Err(_error) => {
                let mut unknown = NativeRangePrefixObservation::unknown(
                    range_id,
                    "native base unknown/invalid; repair or re-attestation required",
                );
                unknown.invalidated = true;
                return Ok(unknown);
            }
        };
        let mut observation = NativeRangePrefixObservation {
            range_id,
            native_base_index: Some(lineage.base.range_index),
            native_applied_index: Some(lineage.range_index()),
            pending_command_index: None,
            materialized_pending_index: None,
            pending_command_id: None,
            native_receipt_sha256: Some(lineage.native_receipt_sha256().to_string()),
            factory_initialization_complete: !self
                .range_image_initializing
                .load(AtomicOrdering::Acquire),
            invalidated: false,
            diagnostic: None,
        };
        self.observe_native_pending(&mut observation)?;
        Ok(observation)
    }

    pub(crate) fn validate_source_range_image_lineage(
        &self,
        lineage: &SourceRangeImageLineage,
        range_id: RangeId,
        range_index: u64,
    ) -> Result<()> {
        self.validate_source_range_image_lineage_contract(lineage, range_id, range_index)?;
        if self.native_range_pending_exists(range_id)? {
            return Err(invalid(
                "unfinished native command; source range image unavailable",
            ));
        }
        Ok(())
    }

    fn validate_source_range_image_lineage_contract(
        &self,
        lineage: &SourceRangeImageLineage,
        range_id: RangeId,
        range_index: u64,
    ) -> Result<()> {
        #[cfg(test)]
        let native_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-lineage-native-birth-anchor-prefix-joint",
        );
        self.check_recovery_state()?;
        if self.native_range_image_installing(range_id)? {
            return Err(invalid(
                "native image installation is incomplete; no source lineage",
            ));
        }
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("source has no trusted native database birth"))?;
        let actual_anchor = self.actual_native_range_anchor(&birth, range_id)?;
        let receipt = actual_anchor.binding();
        self.validate_native_range_base_history(&birth, &receipt)?;
        let prefix = self.read_native_certified_range_prefix(&birth, &receipt)?;
        let joint = self.read_native_joint_serving_domain(&birth, &receipt)?;
        if let Some(joint) = &joint {
            joint.require_materialized(&receipt, prefix.as_ref())?;
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(native_trace);
        #[cfg(test)]
        let catalog_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-lineage-all-namespace-catalog-and-generations",
            );
        let bindings = self.capture_range_image_bindings()?;
        let catalog = canonical_range_image_catalog(
            &birth.spec.cluster_id,
            &receipt.range,
            receipt.range_index,
            &bindings,
        )?;
        if self.incarnation != lineage.current_database_incarnation
            || self.schema_compatibility.generation() != lineage.current_schema_generation
            || self.native_range_image_generations(&birth, range_id)?
                != (
                    receipt.global_image_generation,
                    receipt.range_image_generation,
                )
            || hex::encode(sha2::Sha256::digest(&catalog)) != receipt.portable_catalog_sha256
            || (!receipt.installed
                && hex::encode(content_hasher(&catalog).finalize()) != receipt.image_root_sha256)
            || birth != lineage.birth
            || receipt != lineage.base
            || prefix != lineage.prefix
            || joint != lineage.joint
            || lineage.range_index() != range_index
            || receipt.range.id != range_id
            || !self.config.fsync
        {
            return Err(invalid(
                "source lineage/base/floor changed; re-establish actual native proof",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(catalog_trace);
        #[cfg(test)]
        let namespace_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-lineage-namespace-UID-schema-readers",
            );
        for (name, id) in &receipt.local_namespace_ids {
            if self.local_namespace_identity(name)? != Some(*id) {
                return Err(invalid("source native base namespace lineage changed"));
            }
        }
        if self.collections.len() != receipt.local_namespace_ids.len()
            || self.schema_compatibility.verified_sha256().as_deref()
                != Some(receipt.schema_sha256.as_str())
            || format::required_reader_features(&self.path)? != receipt.reader_features
        {
            return Err(invalid(
                "source native base schema/reader contract changed; re-attestation required",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(namespace_trace);
        Ok(())
    }

    /// No DB/service latch may be held by the caller. This initial producer
    /// establishes only actual empty genesis; it never backfills legacy R>0.
    pub(crate) fn initialize_range_image_genesis(
        db: &Arc<RwLock<BicDb>>,
        authority: &FreshRangeGenesisAuthority,
        range: &RangeDescriptor,
        frozen: &FrozenRangeMutationPermit,
        budget: Duration,
        cancellation: &CancellationToken,
    ) -> Result<SourceRangeImageLineage> {
        if budget.is_zero() || budget > MAX_WAIT {
            return Err(invalid("invalid genesis wait budget"));
        }
        let cancellation = cancellation.child_with_deadline(Instant::now() + budget);
        let (reserved, bindings, source_pin) = {
            let database = db
                .try_read()
                .ok_or_else(|| invalid("database busy; retry genesis outside locks"))?;
            database.range_mutations.validate_frozen(range.id, frozen)?;
            let birth = database
                .read_native_birth()?
                .ok_or_else(|| invalid("empty legacy database has no factory birth"))?;
            if birth != authority.birth
                || database.path.canonicalize()? != authority.root
                || !birth
                    .spec
                    .initial_ranges
                    .iter()
                    .any(|initial| initial == range)
            {
                return Err(invalid(
                    "factory authority does not bind this physical database/range/config",
                ));
            }
            if let Some(receipt) = database
                .read_native_range_image_record::<NativeGenesisReceipt>(&genesis_key(range.id))?
            {
                // Once published, empty genesis is immutable. This cannot
                // reset a later/invalidated base or erase an acknowledged cut.
                if database
                    .read_native_range_image_record::<NativeInitializationComplete>(READY_KEY)?
                    .is_some()
                {
                    let base = NativeRangeBaseBinding::genesis(&receipt);
                    let serving = database
                        .read_native_recovered_serving_configuration(&birth, &base, None)?;
                    let joint = database.read_native_joint_serving_domain(&birth, &base)?;
                    let lineage = SourceRangeImageLineage {
                        birth,
                        base,
                        serving,
                        joint,
                        _anchor: NativeRangeImageAnchor::Genesis(receipt),
                        prefix: None,
                        current_database_incarnation: database.incarnation.clone(),
                        current_schema_generation: database.schema_compatibility.generation(),
                    };
                    database.validate_source_range_image_lineage(&lineage, range.id, 0)?;
                    return Ok(lineage);
                }
            }
            (
                database.current_commit_seq(),
                database.capture_range_image_bindings()?,
                database
                    .range_mutations
                    .try_source_capture_read_pin(range.id, frozen)?,
            )
        };
        loop {
            cancellation.check()?;
            let reached = {
                let database = db
                    .try_read()
                    .ok_or_else(|| invalid("database busy; retry genesis outside locks"))?;
                database.range_mutations.validate_frozen(range.id, frozen)?;
                database
                    .range_mutations
                    .validate_source_capture_read_pin(range.id, &source_pin)?;
                database.validate_range_image_bindings(&bindings)?;
                database.replication_shippable_seq() >= reserved
            };
            if reached {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut actual_snapshot = reserved;
        for meta in bindings.collections() {
            let mut after = None;
            loop {
                cancellation.check()?;
                let database = db
                    .try_read()
                    .ok_or_else(|| invalid("database busy during genesis scan"))?;
                database.range_mutations.validate_frozen(range.id, frozen)?;
                database
                    .range_mutations
                    .validate_source_capture_read_pin(range.id, &source_pin)?;
                database.validate_range_image_bindings(&bindings)?;
                let exported = database.for_each_range_snapshot_batch_with_cancellation(
                    &meta.name,
                    range,
                    after.as_deref(),
                    &RangeSnapshotOptions {
                        max_records_per_batch: 128,
                        max_bytes_per_batch: 8 * 1024 * 1024,
                        max_record_bytes: 8 * 1024 * 1024,
                    },
                    16_384,
                    &cancellation,
                    |_batch| Err(invalid("fresh genesis range is not actually empty")),
                )?;
                if exported.snapshot_commit_sequence < reserved {
                    return Err(invalid(
                        "actual genesis snapshot is below the captured reservation bound",
                    ));
                }
                actual_snapshot = actual_snapshot.max(exported.snapshot_commit_sequence);
                if exported.completed {
                    break;
                }
                if exported.resume_after_key <= after {
                    return Err(invalid("genesis empty scan failed to advance"));
                }
                after = exported.resume_after_key;
            }
        }
        let database = db
            .try_write()
            .ok_or_else(|| invalid("database busy before native genesis receipt"))?;
        database.range_mutations.validate_frozen(range.id, frozen)?;
        database
            .range_mutations
            .validate_source_capture_read_pin(range.id, &source_pin)?;
        database.validate_range_image_bindings(&bindings)?;
        cancellation.check()?;
        let key = genesis_key(range.id);
        let catalog =
            canonical_range_image_catalog(&authority.birth.spec.cluster_id, range, 0, &bindings)?;
        let (global_image_generation, range_image_generation) =
            database.native_range_image_generations(&authority.birth, range.id)?;
        let mut receipt = NativeGenesisReceipt {
            format_version: 2,
            birth_id: authority.birth.birth_id,
            birth_sha256: authority.birth.checksum_sha256.clone(),
            range: range.clone(),
            base_members: authority.birth.spec.initial_members.clone(),
            local_member_incarnation: authority.birth.spec.member_incarnation,
            restore_activation: authority.birth.spec.restore_activation.clone(),
            range_index: 0,
            source_database_incarnation: database.incarnation.clone(),
            reserved_db_upper_bound: reserved,
            actual_snapshot_db_sequence: actual_snapshot,
            schema_sha256: bindings.schema_sha256().to_string(),
            global_image_generation,
            range_image_generation,
            portable_catalog_sha256: hex::encode(sha2::Sha256::digest(&catalog)),
            empty_image_root_sha256: hex::encode(content_hasher(&catalog).finalize()),
            reader_features: bindings.reader_features().to_vec(),
            local_namespace_ids: bindings.namespace_ids(),
            native_page_xid: 0,
            history_generation: Uuid::now_v7(),
            logical_genesis_sha256: String::new(),
            checksum_sha256: String::new(),
        };
        receipt.logical_genesis_sha256 = history::logical_genesis_hash(&authority.birth, &receipt)?;
        // Frozen is read-only. This actual unpublished factory producer owns
        // a counted native writer until BOTH receipt and initialization marker
        // have finished their real native page-WAL commits.
        // The exclusive DB guard bridges pin release to writer admission:
        // no controlled writer can alter the scanned image in that gap.
        drop(source_pin);
        let controlled = database.range_mutations.try_controlled_mutation(frozen)?;
        database.range_mutations.validate(
            range.id,
            &crate::distribution_range_mutation::RangeMutationAuthority::Controlled(&controlled),
        )?;
        // The page commit orders/syncs the earlier prefix; this is not a claim
        // that separate catalog/record transactions became one atomic txn.
        database
            .in_paged_transaction(|paged, xid| {
                receipt.native_page_xid = xid;
                receipt.checksum_sha256 = receipt.checksum()?;
                history::put_immutable_history_entry(
                    paged,
                    xid,
                    receipt.history_generation,
                    &history::NativeLogicalHistoryEntryV1::genesis(
                        range.id,
                        &receipt.logical_genesis_sha256,
                    )?,
                )?;
                let encoded = bounded_json(&receipt, MAX_NATIVE_RECEIPT_BYTES)?;
                paged
                    .store()
                    .put(xid, &native_key(&key), &encoded)
                    .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
                Ok(())
            })?
            .ok_or_else(|| invalid("genesis receipt has no native WAL transaction"))?;
        database.finish_native_genesis_initialization(&authority.birth)?;
        drop(controlled);
        database
            .range_mutations
            .validate_quiescent_frozen(range.id, frozen)?;
        let base = NativeRangeBaseBinding::genesis(&receipt);
        let serving =
            database.read_native_recovered_serving_configuration(&authority.birth, &base, None)?;
        let joint = database.read_native_joint_serving_domain(&authority.birth, &base)?;
        let lineage = SourceRangeImageLineage {
            birth: authority.birth.clone(),
            base,
            serving,
            joint,
            _anchor: NativeRangeImageAnchor::Genesis(receipt),
            prefix: None,
            current_database_incarnation: database.incarnation.clone(),
            current_schema_generation: database.schema_compatibility.generation(),
        };
        database.validate_source_range_image_lineage(&lineage, range.id, 0)?;
        Ok(lineage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_range_mutation::RangeMutationAuthority;
    use crate::{DistributionConfig, DistributionStore};

    pub(super) fn config() -> DbConfig {
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }
    pub(super) fn spec() -> RangeImageBirthSpec {
        let metadata = tempfile::tempdir().unwrap();
        let topology = DistributionStore::initialize_at(
            metadata.path(),
            DistributionConfig {
                enabled: true,
                initial_ranges: 2,
                replication_factor: 1,
                ..Default::default()
            },
            false,
            1,
        )
        .unwrap()
        .topology()
        .clone();
        RangeImageBirthSpec {
            cluster_id: topology.cluster_id,
            bootstrap_id: Uuid::now_v7(),
            node_id: topology.nodes.keys().next().unwrap().clone(),
            member_incarnation: 1,
            restore_activation: None,
            initial_members: topology
                .nodes
                .into_iter()
                .map(|(id, node)| {
                    (
                        id,
                        RangeImageBirthMember {
                            incarnation: node.incarnation,
                            tls_certificate_sha256: node.tls_certificate_sha256,
                            pending_tls_certificate_sha256: node.pending_tls_certificate_sha256,
                        },
                    )
                })
                .collect(),
            initial_ranges: topology.ranges.into_values().collect(),
        }
    }
    fn tree(root: &Path) -> BTreeMap<String, String> {
        fn visit(root: &Path, directory: &Path, result: &mut BTreeMap<String, String>) {
            for entry in fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, result);
                } else {
                    result.insert(
                        path.strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .to_string(),
                        hex::encode(sha2::Sha256::digest(fs::read(path).unwrap())),
                    );
                }
            }
        }
        let mut result = BTreeMap::new();
        visit(root, root, &mut result);
        result
    }
    pub(super) fn schema_ready_database(
        root: &Path,
        expected: &RangeImageBirthSpec,
    ) -> (Arc<RwLock<BicDb>>, FreshRangeGenesisAuthority) {
        let (mut db, _authority) =
            BicDb::create_new_range_image_database(root, config(), expected.clone()).unwrap();
        db.create_collection("items").unwrap();
        drop(db);
        let (db, authority) = BicDb::open_range_image_database(root, config(), expected).unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        (Arc::new(RwLock::new(db)), authority)
    }
    pub(super) fn initialize(
        db: &Arc<RwLock<BicDb>>,
        authority: &FreshRangeGenesisAuthority,
        range: &RangeDescriptor,
    ) -> SourceRangeImageLineage {
        let gate = db.read().range_mutation_gate();
        let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
        BicDb::initialize_range_image_genesis(
            db,
            authority,
            range,
            &frozen,
            Duration::from_secs(3),
            &CancellationToken::uncancelable(),
        )
        .unwrap()
    }

    #[test]
    fn native_recovery_admission_requires_actual_completed_birth_not_flags_or_empty_counters() {
        let parent = tempfile::tempdir().unwrap();
        let expected = spec();
        let legacy = parent.path().join("legacy");
        let mut db = BicDb::open_with_config(&legacy, config()).unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        format::enable_range_image_install(&legacy).unwrap();
        assert!(db
            .native_recovery_admission(
                &expected.cluster_id,
                expected.bootstrap_id,
                &expected.node_id,
                expected.member_incarnation
            )
            .is_err());
        drop(db);

        let fresh = parent.path().join("fresh");
        let (db, authority) = schema_ready_database(&fresh, &expected);
        assert!(db
            .read()
            .native_recovery_admission(
                &expected.cluster_id,
                expected.bootstrap_id,
                &expected.node_id,
                expected.member_incarnation
            )
            .is_err());
        for range in &expected.initial_ranges {
            initialize(&db, &authority, range);
        }
        let admission = db
            .read()
            .native_recovery_admission(
                &expected.cluster_id,
                expected.bootstrap_id,
                &expected.node_id,
                expected.member_incarnation,
            )
            .unwrap();
        admission.require_current(&db.read()).unwrap();
        assert_eq!(admission.cluster_id(), &expected.cluster_id);
        assert_eq!(admission.bootstrap_id(), expected.bootstrap_id);
        assert_eq!(admission.local_node_id(), &expected.node_id);
        assert_eq!(
            admission.local_member_incarnation(),
            expected.member_incarnation
        );
        assert_eq!(
            admission.restore_activation(),
            expected.restore_activation.as_ref()
        );
        assert!(db
            .read()
            .native_recovery_admission(
                &expected.cluster_id,
                Uuid::now_v7(),
                &expected.node_id,
                expected.member_incarnation
            )
            .is_err());
        assert!(db
            .read()
            .native_recovery_admission(
                &expected.cluster_id,
                expected.bootstrap_id,
                &expected.node_id,
                expected.member_incarnation + 1
            )
            .is_err());
        drop(db);
        let (reopened, _) = BicDb::open_range_image_database(&fresh, config(), &expected).unwrap();
        assert!(admission.require_current(&reopened).is_err());
        reopened
            .native_recovery_admission(
                &expected.cluster_id,
                expected.bootstrap_id,
                &expected.node_id,
                expected.member_incarnation,
            )
            .unwrap()
            .require_current(&reopened)
            .unwrap();
    }

    #[test]
    fn native_recovery_admission_empty_fresh_learner_has_no_range_floor() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("learner");
        let mut expected = spec();
        expected.initial_ranges.clear();
        let (db, _) =
            BicDb::create_new_range_image_database(&root, config(), expected.clone()).unwrap();
        db.native_recovery_admission(
            &expected.cluster_id,
            expected.bootstrap_id,
            &expected.node_id,
            expected.member_incarnation,
        )
        .unwrap();
        let observation = db
            .native_range_prefix_observation(RangeId::new(1).unwrap())
            .unwrap();
        assert_eq!(observation.native_base_index, None);
        assert_eq!(observation.native_applied_index, None);
        assert_eq!(observation.materialized_pending_index, None);
    }

    #[test]
    fn native_range_prefix_observation_distinguishes_verified_zero_and_invalidation() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let expected = spec();
        let (db, authority) = schema_ready_database(&root, &expected);
        let range = &expected.initial_ranges[0];
        assert_eq!(
            db.read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            None
        );
        for range in &expected.initial_ranges {
            initialize(&db, &authority, range);
        }
        let observation = db.read().native_range_prefix_observation(range.id).unwrap();
        assert_eq!(observation.native_base_index, Some(0));
        assert_eq!(observation.native_applied_index, Some(0));
        assert!(observation.factory_initialization_complete);
        assert!(!observation.invalidated);
        db.read()
            .invalidate_native_range_images_before_mutation(Some(range.id), "raw mutation begins")
            .unwrap();
        let invalidated = db.read().native_range_prefix_observation(range.id).unwrap();
        assert_eq!(invalidated.native_applied_index, None);
        assert!(invalidated.invalidated);
    }

    #[test]
    fn native_installing_marker_closes_source_and_observation_after_reopen() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let expected = spec();
        let (db, authority) = schema_ready_database(&root, &expected);
        let range = &expected.initial_ranges[0];
        let lineage = initialize(&db, &authority, range);
        for other in expected.initial_ranges.iter().skip(1) {
            initialize(&db, &authority, other);
        }
        db.read()
            .write_native_range_image_record(
                &format!("install:active:{}", range.id.get()),
                &json!({"incomplete":true,"session":Uuid::now_v7()}),
            )
            .unwrap();
        assert!(db
            .read()
            .validate_source_range_image_lineage(&lineage, range.id, 0)
            .is_err());
        let observation = db.read().native_range_prefix_observation(range.id).unwrap();
        assert!(observation.invalidated);
        assert_eq!(observation.native_applied_index, None);
        drop(db);
        let (reopened, _) = BicDb::open_range_image_database(&root, config(), &expected).unwrap();
        assert!(reopened.native_range_image_installing(range.id).unwrap());
        let observation = reopened.native_range_prefix_observation(range.id).unwrap();
        assert!(observation.invalidated);
        assert_eq!(observation.native_applied_index, None);
    }

    #[test]
    fn native_range_image_factory_refuses_existing_legacy_and_binding_mismatch_without_mutation() {
        let parent = tempfile::tempdir().unwrap();
        let legacy = parent.path().join("legacy");
        let db = BicDb::open_with_config(&legacy, config()).unwrap();
        drop(db);
        let expected = spec();
        let before = tree(&legacy);
        assert!(
            BicDb::create_new_range_image_database(&legacy, config(), expected.clone()).is_err()
        );
        assert!(BicDb::open_range_image_database(&legacy, config(), &expected).is_err());
        assert_eq!(tree(&legacy), before);
        let fresh = parent.path().join("fresh");
        let (db, _) =
            BicDb::create_new_range_image_database(&fresh, config(), expected.clone()).unwrap();
        drop(db);
        let before = tree(&fresh);
        let mut wrong = expected;
        wrong.bootstrap_id = Uuid::now_v7();
        assert!(BicDb::open_range_image_database(&fresh, config(), &wrong).is_err());
        assert_eq!(tree(&fresh), before);
    }

    #[test]
    fn native_range_image_factory_after_flags_failure_preserves_partial_root_and_refuses_strict_reopen(
    ) {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("partial");
        let expected = spec();
        let error =
            BicDb::create_new_range_image_database_with(&root, config(), expected.clone(), |_db| {
                Err(invalid(
                    "injected partial initialization after sticky flags, before native birth",
                ))
            })
            .unwrap_err();
        assert!(error.to_string().contains("injected partial"));
        assert!(format::range_image_install_active(&root).unwrap());
        let before = tree(&root);
        assert!(BicDb::open_range_image_database(&root, config(), &expected)
            .unwrap_err()
            .to_string()
            .contains("incomplete"));
        assert!(BicDb::create_new_range_image_database(&root, config(), expected).is_err());
        assert_eq!(tree(&root), before);
    }

    #[test]
    fn native_range_image_genesis_is_actual_empty_native_durable_and_reopens_without_row_counter_shortcut(
    ) {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let expected = spec();
        let (db, authority) = schema_ready_database(&root, &expected);
        {
            let mut database = db.write();
            assert!(database
                .insert("items", Record::new("early"))
                .unwrap_err()
                .to_string()
                .contains("genesis is incomplete"));
        }
        let first = initialize(&db, &authority, &expected.initial_ranges[0]);
        assert!(db
            .read()
            .range_image_initializing
            .load(AtomicOrdering::Acquire));
        let second = initialize(&db, &authority, &expected.initial_ranges[1]);
        assert!(!db
            .read()
            .range_image_initializing
            .load(AtomicOrdering::Acquire));
        assert_eq!(first.range_index(), 0);
        let NativeRangeImageAnchor::Genesis(genesis) = &first._anchor else {
            panic!("fresh factory must produce actual Genesis, never an installed surrogate")
        };
        assert_eq!(genesis.reserved_db_upper_bound, 0);
        assert!(genesis.native_page_xid > 0);
        let first_hash = first.native_receipt_sha256().to_string();
        assert_eq!(
            initialize(&db, &authority, &expected.initial_ranges[0]).native_receipt_sha256(),
            first_hash
        );
        db.read()
            .validate_source_range_image_lineage(&second, expected.initial_ranges[1].id, 0)
            .unwrap();
        drop(first);
        drop(second);
        drop(authority);
        drop(db);
        let (db, authority) = BicDb::open_range_image_database(&root, config(), &expected).unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        assert!(!db.range_image_initializing.load(AtomicOrdering::Acquire));
        let db = Arc::new(RwLock::new(db));
        let recovered = initialize(&db, &authority, &expected.initial_ranges[0]);
        assert_eq!(recovered.native_receipt_sha256(), first_hash);
        assert!(db
            .read()
            .collections()
            .iter()
            .all(|meta| !meta.name.contains('\0')));
    }

    #[test]
    fn native_range_image_scoped_import_invalidates_only_affected_floor_and_ddl_invalidates_before_recreate(
    ) {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let expected = spec();
        let (db, authority) = schema_ready_database(&root, &expected);
        let a = initialize(&db, &authority, &expected.initial_ranges[0]);
        let b = initialize(&db, &authority, &expected.initial_ranges[1]);
        let range = &expected.initial_ranges[1];
        let key = (0..10_000)
            .map(|i| format!("k{i}"))
            .find(|key| range.contains_token(distribution_key_token("items", key)))
            .unwrap();
        let record = Record::new(key.clone()).with_metadata(json!({"from":"raw-copy"}));
        let batch = RangeSnapshotBatch {
            collection: "items".into(),
            range_id: range.id,
            range_epoch: range.epoch,
            snapshot_commit_sequence: 0,
            resume_after_key: key.clone(),
            serialized_record_bytes: serde_json::to_vec(&record).unwrap().len(),
            records: vec![record],
        };
        let gate = db.read().range_mutation_gate();
        let permit = gate.try_enter(range.id).unwrap();
        {
            let database = db.read();
            let meta = database.collections().into_iter().next().unwrap();
            database
                .apply_range_snapshot_batch_preinstalled_with_mutation(
                    range,
                    &meta,
                    &batch,
                    &RangeMutationAuthority::Mutation(&permit),
                )
                .unwrap();
            assert!(database
                .validate_source_range_image_lineage(&b, range.id, 0)
                .is_err());
            database
                .validate_source_range_image_lineage(&a, expected.initial_ranges[0].id, 0)
                .unwrap();
            assert!(database.current_commit_seq() > a.range_index()); // DBseq != range index
            assert!(database.get("items", &key).unwrap().is_some());
        }
        drop(permit);
        let before = db.read().local_namespace_identity("items").unwrap();
        db.write().drop_collection("items").unwrap();
        db.write().create_collection("items").unwrap();
        db.read().schema_compatibility_fingerprint().unwrap();
        assert_ne!(db.read().local_namespace_identity("items").unwrap(), before);
        assert!(db
            .read()
            .validate_source_range_image_lineage(&a, expected.initial_ranges[0].id, 0)
            .is_err());
        // Completed native genesis cannot reset an invalidated base merely
        // because the hard-recreated relation happens to be empty again.
        let frozen = gate
            .try_freeze(expected.initial_ranges[0].id, Uuid::now_v7())
            .unwrap();
        assert!(BicDb::initialize_range_image_genesis(
            &db,
            &authority,
            &expected.initial_ranges[0],
            &frozen,
            Duration::from_secs(1),
            &CancellationToken::uncancelable()
        )
        .is_err());
    }

    #[test]
    fn native_range_image_keyspace_cannot_be_chosen_by_collection_or_snapshot_storage_alias() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let expected = spec();
        let (mut db, _) =
            BicDb::create_new_range_image_database(&root, config(), expected.clone()).unwrap();
        assert!(db
            .create_collection("\0bicdb.range-image.native.v1")
            .is_err());
        let mut meta = CollectionMeta::standard("items");
        meta.storage_name = Some("\0receipt".into());
        let batch = RangeSnapshotBatch {
            collection: "items".into(),
            range_id: expected.initial_ranges[0].id,
            range_epoch: 1,
            snapshot_commit_sequence: 0,
            resume_after_key: "birth".into(),
            serialized_record_bytes: 0,
            records: vec![],
        };
        assert!(matches!(
            db.apply_range_snapshot_batch(&expected.initial_ranges[0], &meta, &batch),
            Err(BicDbError::InvalidCollectionName(_))
        ));
        meta.storage_name = Some(String::new());
        assert!(matches!(
            db.apply_range_snapshot_batch(&expected.initial_ranges[0], &meta, &batch),
            Err(BicDbError::InvalidCollectionName(_))
        ));
        let paged = db.paged_records.as_ref().unwrap();
        let (xid, _) = paged.begin();
        assert!(paged.put(xid, "", &Record::new("\u{10}birth")).is_err());
        paged.abort(xid).unwrap();
        assert!(db.read_native_birth().unwrap().is_some());
    }
    #[test]
    fn native_range_image_fresh_learner_empty_initial_set_grants_no_range_genesis() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("learner");
        let mut expected = spec();
        let unrelated_range = expected.initial_ranges[0].clone();
        expected.initial_ranges.clear();
        let (db, authority) =
            BicDb::create_new_range_image_database(&root, config(), expected).unwrap();
        assert!(!db.range_image_initializing.load(AtomicOrdering::Acquire));
        db.schema_compatibility_fingerprint().unwrap();
        let gate = db.range_mutation_gate();
        let frozen = gate.try_freeze(unrelated_range.id, Uuid::now_v7()).unwrap();
        let db = Arc::new(RwLock::new(db));
        assert!(BicDb::initialize_range_image_genesis(
            &db,
            &authority,
            &unrelated_range,
            &frozen,
            Duration::from_secs(1),
            &CancellationToken::uncancelable()
        )
        .is_err());
        assert!(db
            .read()
            .read_native_range_image_record::<NativeGenesisReceipt>(&genesis_key(
                unrelated_range.id
            ))
            .unwrap()
            .is_none());
    }
}
