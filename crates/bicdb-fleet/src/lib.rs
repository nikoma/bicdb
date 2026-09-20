//! Industry-neutral App Root and fleet authorization primitives.
//!
//! This crate deliberately has no dependency on the BicDB database, SQL,
//! application runtime, cell key provider, or network servers. A fleet
//! controller may distribute immutable bytes and signed transition metadata;
//! it has no type capable of opening a database or unwrapping a Cell key.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use semver::{Version, VersionReq};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub const FLEET_TRUST_POLICY_FORMAT: &str = "bicdb.fleet-trust-policy/v1";
pub const FLEET_RELEASE_FORMAT: &str = "bicdb.fleet-release/v1";
pub const TRANSPARENCY_ENTRY_FORMAT: &str = "bicdb.transparency-entry/v1";
pub const TRANSPARENCY_CHECKPOINT_FORMAT: &str = "bicdb.transparency-checkpoint/v1";
pub const ROLLOUT_PLAN_FORMAT: &str = "bicdb.rollout-plan/v1";
pub const COHORT_GATE_FORMAT: &str = "bicdb.cohort-gate/v1";
pub const ACTIVATION_TICKET_FORMAT: &str = "bicdb.activation-ticket/v1";
pub const ACTIVATION_BUNDLE_FORMAT: &str = "bicdb.activation-bundle/v1";
pub const CONVERGENCE_RECEIPT_FORMAT: &str = "bicdb.convergence-receipt/v1";

const RELEASE_DOMAIN: &[u8] = b"BICDB-FLEET-RELEASE-V1\0";
const CHECKPOINT_DOMAIN: &[u8] = b"BICDB-FLEET-TRANSPARENCY-CHECKPOINT-V1\0";
const ROLLOUT_DOMAIN: &[u8] = b"BICDB-FLEET-ROLLOUT-V1\0";
const COHORT_GATE_DOMAIN: &[u8] = b"BICDB-FLEET-COHORT-GATE-V1\0";
const ACTIVATION_DOMAIN: &[u8] = b"BICDB-FLEET-ACTIVATION-V1\0";
const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_DOCUMENT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_APPLICATIONS: usize = 1024;
const MAX_COMPONENTS_PER_APPLICATION: usize = 128;
const MAX_COHORTS: usize = 1024;
const MAX_CELLS_PER_PLAN: usize = 1_000_000;
const MAX_TRANSPARENCY_PROOF_ENTRIES: usize = 4096;
const MAX_CONVERGENCE_RECEIPTS: usize = 1_000_000;
static ARTIFACT_STAGING_NONCE: AtomicU64 = AtomicU64::new(1);

pub type Result<T> = std::result::Result<T, FleetError>;

#[derive(Debug, thiserror::Error)]
pub enum FleetError {
    #[error("FLEET_DOCUMENT_INVALID: {0}")]
    Invalid(String),
    #[error("FLEET_SIGNATURE_INVALID: {0}")]
    Signature(String),
    #[error("FLEET_AUTHORITY_INSUFFICIENT: {0}")]
    Authority(String),
    #[error("FLEET_ACTIVATION_REFUSED: {0}")]
    Activation(String),
    #[error("FLEET_ARTIFACT_INVALID: {0}")]
    Artifact(String),
    #[error("FLEET_CONVERGENCE_INVALID: {0}")]
    Convergence(String),
    #[error("FLEET_IO: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct FleetDigest(String);

impl FleetDigest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let Some(hex_value) = value.strip_prefix("sha256:") else {
            return Err(FleetError::Invalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        };
        if value != value.to_ascii_lowercase()
            || hex_value.len() != 64
            || !hex_value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(FleetError::Invalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn hex(&self) -> &str {
        self.0.strip_prefix("sha256:").unwrap_or_default()
    }
}

impl std::fmt::Display for FleetDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for FleetDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct FleetCellId(String);

impl FleetCellId {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let parsed = Uuid::parse_str(&value)
            .map_err(|_| FleetError::Invalid("cell_id must be a UUID".to_string()))?;
        if parsed.is_nil() {
            return Err(FleetError::Invalid(
                "cell_id must not be the nil UUID".to_string(),
            ));
        }
        Ok(Self(parsed.hyphenated().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FleetCellId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for FleetCellId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn canonical_cbor<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|error| FleetError::Invalid(format!("encode deterministic CBOR: {error}")))?;
    Ok(bytes)
}

fn decode_canonical_cbor<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let value: T = ciborium::de::from_reader(bytes)
        .map_err(|error| FleetError::Invalid(format!("decode CBOR: {error}")))?;
    if canonical_cbor(&value)? != bytes {
        return Err(FleetError::Invalid(
            "document is not in BicDB deterministic CBOR encoding".to_string(),
        ));
    }
    Ok(value)
}

pub fn document_digest<T: Serialize>(value: &T) -> Result<FleetDigest> {
    Ok(FleetDigest::of_bytes(&canonical_cbor(value)?))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn require_identifier(value: &str, label: &str) -> Result<()> {
    if valid_identifier(value) {
        Ok(())
    } else {
        Err(FleetError::Invalid(format!(
            "{label} must be a 1..128 byte ASCII identifier"
        )))
    }
}

fn valid_media_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.contains('/')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityRole {
    Publisher,
    Security,
    Builder,
    Transparency,
    Rollout,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FleetAuthorityKey {
    pub key_id: String,
    pub role: AuthorityRole,
    /// Exactly 64 lowercase hexadecimal digits encoding an Ed25519 key.
    pub public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FleetTrustPolicy {
    pub format: String,
    pub policy_id: String,
    pub generation: u64,
    pub authorities: Vec<FleetAuthorityKey>,
    pub thresholds: BTreeMap<AuthorityRole, u8>,
    pub minimum_reproducible_builds: u8,
    pub maximum_clock_skew_seconds: i64,
    pub maximum_ticket_lifetime_seconds: i64,
    pub maximum_dormant_seconds: i64,
    /// Keyed by `<root>/<application>`.
    pub minimum_safe_release_sequences: BTreeMap<String, u64>,
}

impl FleetTrustPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.format != FLEET_TRUST_POLICY_FORMAT
            || self.generation == 0
            || self.minimum_reproducible_builds < 2
            || self.maximum_clock_skew_seconds < 0
            || self.maximum_clock_skew_seconds > 300
            || self.maximum_ticket_lifetime_seconds <= 0
            || self.maximum_ticket_lifetime_seconds > 86_400
            || self.maximum_dormant_seconds <= 0
        {
            return Err(FleetError::Invalid(
                "fleet policy has an invalid format, generation, or safety bound".to_string(),
            ));
        }
        require_identifier(&self.policy_id, "policy_id")?;
        let mut ids = BTreeSet::new();
        let mut key_material = BTreeSet::new();
        let mut available = BTreeMap::<AuthorityRole, usize>::new();
        for authority in &self.authorities {
            require_identifier(&authority.key_id, "authority key_id")?;
            if !ids.insert(authority.key_id.clone()) {
                return Err(FleetError::Invalid(format!(
                    "duplicate authority key id {}",
                    authority.key_id
                )));
            }
            if authority.public_key != authority.public_key.to_ascii_lowercase()
                || authority.public_key.len() != 64
            {
                return Err(FleetError::Invalid(format!(
                    "authority {} is not a lowercase Ed25519 public key",
                    authority.key_id
                )));
            }
            let decoded = hex::decode(&authority.public_key)
                .map_err(|_| FleetError::Invalid("authority key is not hexadecimal".to_string()))?;
            let bytes: [u8; 32] = decoded.try_into().map_err(|_| {
                FleetError::Invalid("authority key must contain 32 bytes".to_string())
            })?;
            VerifyingKey::from_bytes(&bytes).map_err(|_| {
                FleetError::Invalid(format!("authority {} is invalid", authority.key_id))
            })?;
            if !key_material.insert(authority.public_key.clone()) {
                return Err(FleetError::Invalid(
                    "one public key cannot occupy more than one fleet trust domain".to_string(),
                ));
            }
            *available.entry(authority.role).or_default() += 1;
        }
        for role in [
            AuthorityRole::Publisher,
            AuthorityRole::Security,
            AuthorityRole::Builder,
            AuthorityRole::Transparency,
            AuthorityRole::Rollout,
        ] {
            let threshold = usize::from(*self.thresholds.get(&role).unwrap_or(&0));
            let minimum = if role == AuthorityRole::Builder { 2 } else { 1 };
            if threshold < minimum || threshold > available.get(&role).copied().unwrap_or(0) {
                return Err(FleetError::Invalid(format!(
                    "authority threshold for {role:?} is not satisfiable"
                )));
            }
        }
        if usize::from(self.minimum_reproducible_builds)
            > available.get(&AuthorityRole::Builder).copied().unwrap_or(0)
        {
            return Err(FleetError::Invalid(
                "reproducible-build threshold exceeds independent builders".to_string(),
            ));
        }
        for (application, sequence) in &self.minimum_safe_release_sequences {
            if application.split('/').count() != 2
                || application.split('/').any(|part| !valid_identifier(part))
                || *sequence == 0
            {
                return Err(FleetError::Invalid(
                    "minimum-safe releases require <root>/<application> and a positive sequence"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    fn authority(&self, key_id: &str) -> Result<(&FleetAuthorityKey, VerifyingKey)> {
        let authority = self
            .authorities
            .iter()
            .find(|authority| authority.key_id == key_id)
            .ok_or_else(|| FleetError::Signature(format!("untrusted signer {key_id}")))?;
        let bytes: [u8; 32] = hex::decode(&authority.public_key)
            .map_err(|_| FleetError::Signature("invalid trusted key encoding".to_string()))?
            .try_into()
            .map_err(|_| FleetError::Signature("invalid trusted key length".to_string()))?;
        let key = VerifyingKey::from_bytes(&bytes)
            .map_err(|_| FleetError::Signature("invalid trusted Ed25519 key".to_string()))?;
        Ok((authority, key))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSignature {
    pub key_id: String,
    pub role: AuthorityRole,
    pub signature: Vec<u8>,
}

fn approval_message<T: Serialize>(
    domain: &[u8],
    role: AuthorityRole,
    key_id: &str,
    payload: &T,
) -> Result<Vec<u8>> {
    let encoded = canonical_cbor(payload)?;
    let role = format!("{role:?}").to_ascii_lowercase();
    let mut message =
        Vec::with_capacity(domain.len() + key_id.len() + role.len() + encoded.len() + 24);
    message.extend_from_slice(domain);
    message.extend_from_slice(&(key_id.len() as u64).to_be_bytes());
    message.extend_from_slice(key_id.as_bytes());
    message.extend_from_slice(&(role.len() as u64).to_be_bytes());
    message.extend_from_slice(role.as_bytes());
    message.extend_from_slice(&encoded);
    Ok(message)
}

fn sign_approval<T: Serialize>(
    domain: &[u8],
    role: AuthorityRole,
    key_id: &str,
    payload: &T,
    key: &SigningKey,
) -> Result<ApprovalSignature> {
    require_identifier(key_id, "signing key id")?;
    Ok(ApprovalSignature {
        key_id: key_id.to_string(),
        role,
        signature: key
            .sign(&approval_message(domain, role, key_id, payload)?)
            .to_bytes()
            .to_vec(),
    })
}

fn verify_approvals<T: Serialize>(
    policy: &FleetTrustPolicy,
    domain: &[u8],
    payload: &T,
    signatures: &[ApprovalSignature],
    required: &[AuthorityRole],
) -> Result<BTreeSet<String>> {
    let mut signers = BTreeSet::new();
    let mut counts = BTreeMap::<AuthorityRole, usize>::new();
    for approval in signatures {
        if !signers.insert(approval.key_id.clone()) {
            return Err(FleetError::Signature(format!(
                "signer {} appears more than once",
                approval.key_id
            )));
        }
        let (authority, key) = policy.authority(&approval.key_id)?;
        if authority.role != approval.role {
            return Err(FleetError::Signature(format!(
                "signer {} asserted the wrong authority role",
                approval.key_id
            )));
        }
        let signature = Signature::from_slice(&approval.signature)
            .map_err(|_| FleetError::Signature("invalid Ed25519 signature length".to_string()))?;
        key.verify_strict(
            &approval_message(domain, approval.role, &approval.key_id, payload)?,
            &signature,
        )
        .map_err(|_| FleetError::Signature(format!("signature {} failed", approval.key_id)))?;
        *counts.entry(approval.role).or_default() += 1;
    }
    for role in required {
        let required_count = usize::from(*policy.thresholds.get(role).unwrap_or(&0));
        if counts.get(role).copied().unwrap_or(0) < required_count {
            return Err(FleetError::Authority(format!(
                "{role:?} threshold requires {required_count} distinct approvals"
            )));
        }
    }
    Ok(signers)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReference {
    pub digest: FleetDigest,
    pub size_bytes: u64,
    pub media_type: String,
}

impl ArtifactReference {
    fn validate(&self) -> Result<()> {
        if self.size_bytes == 0
            || self.size_bytes > MAX_ARTIFACT_BYTES
            || !valid_media_type(&self.media_type)
        {
            return Err(FleetError::Invalid(
                "artifact has an invalid size or media type".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseComponentKind {
    Backend,
    Frontend,
    Migration,
    Policy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseComponent {
    pub name: String,
    pub kind: ReleaseComponentKind,
    pub artifact: ArtifactReference,
    pub provenance_digest: FleetDigest,
    pub sbom_digest: FleetDigest,
    pub build_recipe_digest: FleetDigest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseCompatibility {
    pub bicdb_version_requirement: String,
    pub from_schema_generation_min: u64,
    pub from_schema_generation_max: u64,
    pub target_schema_generation: u64,
    pub record_format_min: u64,
    pub record_format_max: u64,
    pub rollback_until: i64,
}

impl ReleaseCompatibility {
    fn validate(&self) -> Result<()> {
        VersionReq::parse(&self.bicdb_version_requirement).map_err(|error| {
            FleetError::Invalid(format!("invalid BicDB version requirement: {error}"))
        })?;
        if self.from_schema_generation_min > self.from_schema_generation_max
            || self.target_schema_generation == 0
            || self.record_format_min == 0
            || self.record_format_min > self.record_format_max
            || self.rollback_until <= 0
        {
            return Err(FleetError::Invalid(
                "release compatibility generations or rollback window are invalid".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTarget {
    pub root: String,
    pub name: String,
    pub version: String,
    pub artifact_digest: FleetDigest,
    pub schema_generation: u64,
    pub execution_scope: String,
    pub data_class: String,
}

impl ApplicationTarget {
    pub fn application_id(&self) -> String {
        format!("{}/{}", self.root, self.name)
    }

    fn validate(&self) -> Result<()> {
        require_identifier(&self.root, "application root")?;
        require_identifier(&self.name, "application name")?;
        Version::parse(&self.version).map_err(|error| {
            FleetError::Invalid(format!("invalid application semantic version: {error}"))
        })?;
        if self.schema_generation == 0 {
            return Err(FleetError::Invalid(
                "application schema generation must be positive".to_string(),
            ));
        }
        require_identifier(&self.execution_scope, "execution scope")?;
        require_identifier(&self.data_class, "data class")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRelease {
    pub root: String,
    pub name: String,
    pub version: String,
    pub release_sequence: u64,
    pub package: ArtifactReference,
    pub schema_generation: u64,
    pub execution_scope: String,
    pub data_class: String,
    pub compatibility: ReleaseCompatibility,
    pub components: Vec<ReleaseComponent>,
}

impl ApplicationRelease {
    pub fn application_id(&self) -> String {
        format!("{}/{}", self.root, self.name)
    }

    pub fn target(&self) -> ApplicationTarget {
        ApplicationTarget {
            root: self.root.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            artifact_digest: self.package.digest.clone(),
            schema_generation: self.schema_generation,
            execution_scope: self.execution_scope.clone(),
            data_class: self.data_class.clone(),
        }
    }

    fn validate(&self) -> Result<()> {
        self.target().validate()?;
        self.package.validate()?;
        self.compatibility.validate()?;
        require_identifier(&self.execution_scope, "execution scope")?;
        require_identifier(&self.data_class, "data class")?;
        if self.release_sequence == 0
            || self.compatibility.target_schema_generation != self.schema_generation
            || self.components.is_empty()
            || self.components.len() > MAX_COMPONENTS_PER_APPLICATION
        {
            return Err(FleetError::Invalid(
                "application release has an invalid sequence, schema, or component set".to_string(),
            ));
        }
        let mut components = BTreeSet::new();
        for component in &self.components {
            require_identifier(&component.name, "component name")?;
            component.artifact.validate()?;
            if !components.insert((component.kind, component.name.to_ascii_lowercase())) {
                return Err(FleetError::Invalid(
                    "application release repeats a component identity".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReproducibleBuildWitness {
    pub builder_key_id: String,
    pub application_id: String,
    pub package_digest: FleetDigest,
    pub source_tree_digest: FleetDigest,
    pub build_recipe_digest: FleetDigest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FleetRelease {
    pub format: String,
    pub release_id: String,
    pub published_at: i64,
    pub source_tree_digest: FleetDigest,
    pub dependency_lock_digest: FleetDigest,
    pub sbom_digest: FleetDigest,
    pub provenance_digest: FleetDigest,
    pub applications: Vec<ApplicationRelease>,
    pub reproducible_builds: Vec<ReproducibleBuildWitness>,
}

impl FleetRelease {
    pub fn validate(&self, policy: &FleetTrustPolicy) -> Result<()> {
        if self.format != FLEET_RELEASE_FORMAT
            || Uuid::parse_str(&self.release_id).is_err()
            || self.published_at <= 0
            || self.applications.is_empty()
            || self.applications.len() > MAX_APPLICATIONS
        {
            return Err(FleetError::Invalid(
                "fleet release has an invalid identity, time, or application count".to_string(),
            ));
        }
        let mut applications = BTreeMap::new();
        for application in &self.applications {
            application.validate()?;
            if applications
                .insert(application.application_id(), application)
                .is_some()
            {
                return Err(FleetError::Invalid(
                    "fleet release repeats an application identity".to_string(),
                ));
            }
        }
        let mut witnesses = BTreeSet::new();
        let mut per_application = BTreeMap::<String, BTreeSet<String>>::new();
        for witness in &self.reproducible_builds {
            require_identifier(&witness.builder_key_id, "builder key id")?;
            if !witnesses.insert((
                witness.application_id.clone(),
                witness.builder_key_id.clone(),
            )) {
                return Err(FleetError::Invalid(
                    "reproducible-build witness is duplicated".to_string(),
                ));
            }
            let application = applications.get(&witness.application_id).ok_or_else(|| {
                FleetError::Invalid("build witness names an unknown application".to_string())
            })?;
            let (authority, _) = policy.authority(&witness.builder_key_id)?;
            if authority.role != AuthorityRole::Builder
                || witness.package_digest != application.package.digest
                || witness.source_tree_digest != self.source_tree_digest
                || !application
                    .components
                    .iter()
                    .any(|component| component.build_recipe_digest == witness.build_recipe_digest)
            {
                return Err(FleetError::Invalid(
                    "build witness does not bind an independent builder to exact release inputs/output"
                        .to_string(),
                ));
            }
            per_application
                .entry(witness.application_id.clone())
                .or_default()
                .insert(witness.builder_key_id.clone());
        }
        for application in applications.keys() {
            if per_application
                .get(application)
                .map(BTreeSet::len)
                .unwrap_or(0)
                < usize::from(policy.minimum_reproducible_builds)
            {
                return Err(FleetError::Invalid(format!(
                    "application {application} lacks independently reproduced builds"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedFleetRelease {
    pub release: FleetRelease,
    pub approvals: Vec<ApprovalSignature>,
}

pub fn sign_release_approval(
    release: &FleetRelease,
    role: AuthorityRole,
    key_id: &str,
    key: &SigningKey,
) -> Result<ApprovalSignature> {
    sign_approval(RELEASE_DOMAIN, role, key_id, release, key)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransparencyEntry {
    pub format: String,
    pub log_id: String,
    pub index: u64,
    pub previous_entry_digest: Option<FleetDigest>,
    pub release_digest: FleetDigest,
    pub recorded_at: i64,
}

impl TransparencyEntry {
    fn validate(&self) -> Result<()> {
        if self.format != TRANSPARENCY_ENTRY_FORMAT
            || self.recorded_at <= 0
            || (self.index == 0) != self.previous_entry_digest.is_none()
        {
            return Err(FleetError::Invalid(
                "transparency entry has invalid format, time, or chain root".to_string(),
            ));
        }
        require_identifier(&self.log_id, "transparency log id")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransparencyCheckpoint {
    pub format: String,
    pub log_id: String,
    pub size: u64,
    pub head_entry_digest: FleetDigest,
    pub issued_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedTransparencyCheckpoint {
    pub checkpoint: TransparencyCheckpoint,
    pub approvals: Vec<ApprovalSignature>,
}

pub fn sign_checkpoint_approval(
    checkpoint: &TransparencyCheckpoint,
    role: AuthorityRole,
    key_id: &str,
    key: &SigningKey,
) -> Result<ApprovalSignature> {
    sign_approval(CHECKPOINT_DOMAIN, role, key_id, checkpoint, key)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransparencyInclusionProof {
    /// The first entry contains the release; following entries prove its path
    /// through the hash chain to the independently signed checkpoint head.
    pub entries: Vec<TransparencyEntry>,
    pub checkpoint: SignedTransparencyCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RolloutCohort {
    pub name: String,
    pub cells: Vec<FleetCellId>,
    pub not_before: i64,
    pub observation_seconds: i64,
    pub max_parallel: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RolloutPlan {
    pub format: String,
    pub rollout_id: String,
    pub release_digest: FleetDigest,
    pub created_at: i64,
    pub expires_at: i64,
    pub cohorts: Vec<RolloutCohort>,
}

impl RolloutPlan {
    fn validate(&self) -> Result<()> {
        if self.format != ROLLOUT_PLAN_FORMAT
            || Uuid::parse_str(&self.rollout_id).is_err()
            || self.created_at <= 0
            || self.expires_at <= self.created_at
            || self.cohorts.is_empty()
            || self.cohorts.len() > MAX_COHORTS
        {
            return Err(FleetError::Invalid(
                "rollout plan has an invalid identity, time window, or cohort count".to_string(),
            ));
        }
        let mut names = BTreeSet::new();
        let mut cells = BTreeSet::new();
        for cohort in &self.cohorts {
            require_identifier(&cohort.name, "cohort name")?;
            if !names.insert(cohort.name.to_ascii_lowercase())
                || cohort.cells.is_empty()
                || cohort.not_before < self.created_at
                || cohort.not_before >= self.expires_at
                || cohort.observation_seconds <= 0
                || cohort.observation_seconds > 7 * 86_400
                || cohort.max_parallel == 0
                || usize::try_from(cohort.max_parallel).unwrap_or(usize::MAX) > cohort.cells.len()
            {
                return Err(FleetError::Invalid(
                    "rollout cohort has invalid identity, bounds, or concurrency".to_string(),
                ));
            }
            for cell in &cohort.cells {
                if !cells.insert(cell.clone()) {
                    return Err(FleetError::Invalid(
                        "a cell may occur in exactly one bounded rollout cohort".to_string(),
                    ));
                }
            }
        }
        if cells.len() > MAX_CELLS_PER_PLAN {
            return Err(FleetError::Invalid(
                "rollout plan exceeds the bounded cell count".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedRolloutPlan {
    pub plan: RolloutPlan,
    pub approvals: Vec<ApprovalSignature>,
}

pub fn sign_rollout_approval(
    plan: &RolloutPlan,
    role: AuthorityRole,
    key_id: &str,
    key: &SigningKey,
) -> Result<ApprovalSignature> {
    sign_approval(ROLLOUT_DOMAIN, role, key_id, plan, key)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CohortDecision {
    Proceed,
    Freeze,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CohortGate {
    pub format: String,
    pub rollout_id: String,
    pub release_digest: FleetDigest,
    pub cohort_index: u32,
    pub observed_from: i64,
    pub observed_until: i64,
    pub maximum_error_rate_basis_points: u16,
    pub observed_error_rate_basis_points: u16,
    pub metrics_digest: FleetDigest,
    pub decision: CohortDecision,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedCohortGate {
    pub gate: CohortGate,
    pub approvals: Vec<ApprovalSignature>,
}

pub fn sign_cohort_gate_approval(
    gate: &CohortGate,
    role: AuthorityRole,
    key_id: &str,
    key: &SigningKey,
) -> Result<ApprovalSignature> {
    sign_approval(COHORT_GATE_DOMAIN, role, key_id, gate, key)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActivationTicket {
    pub format: String,
    pub ticket_id: String,
    pub cell_id: FleetCellId,
    pub release_digest: FleetDigest,
    pub rollout_id: String,
    pub cohort_index: u32,
    pub previous_manifest_generation: u64,
    pub previous_manifest_digest: Option<FleetDigest>,
    pub next_manifest_generation: u64,
    pub next_manifest_digest: FleetDigest,
    pub issued_at: i64,
    pub not_before: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedActivationTicket {
    pub ticket: ActivationTicket,
    pub approvals: Vec<ApprovalSignature>,
}

pub fn sign_activation_approval(
    ticket: &ActivationTicket,
    role: AuthorityRole,
    key_id: &str,
    key: &SigningKey,
) -> Result<ApprovalSignature> {
    sign_approval(ACTIVATION_DOMAIN, role, key_id, ticket, key)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FleetActivationBundle {
    pub format: String,
    pub release: SignedFleetRelease,
    pub transparency: TransparencyInclusionProof,
    pub rollout: SignedRolloutPlan,
    pub prior_cohort_gates: Vec<SignedCohortGate>,
    pub ticket: SignedActivationTicket,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationContext {
    pub cell_id: FleetCellId,
    pub previous_manifest_generation: u64,
    pub previous_manifest_digest: Option<FleetDigest>,
    pub previous_applications: Vec<ApplicationTarget>,
    pub next_manifest_generation: u64,
    pub next_manifest_digest: FleetDigest,
    pub next_applications: Vec<ApplicationTarget>,
    pub bicdb_version: String,
    pub now: i64,
    pub dormant_since: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedFleetActivation {
    pub release_digest: FleetDigest,
    pub rollout_id: String,
    pub cohort_index: u32,
    pub cohort_name: String,
    pub rollback_until: i64,
    pub applications: Vec<ApplicationTarget>,
}

pub fn verify_activation_bundle(
    policy: &FleetTrustPolicy,
    bundle: &FleetActivationBundle,
    context: &ActivationContext,
) -> Result<VerifiedFleetActivation> {
    policy.validate()?;
    if bundle.format != ACTIVATION_BUNDLE_FORMAT {
        return Err(FleetError::Invalid(
            "unsupported activation bundle format".to_string(),
        ));
    }
    bundle.release.release.validate(policy)?;
    let release_signers = verify_approvals(
        policy,
        RELEASE_DOMAIN,
        &bundle.release.release,
        &bundle.release.approvals,
        &[
            AuthorityRole::Publisher,
            AuthorityRole::Security,
            AuthorityRole::Builder,
        ],
    )?;
    for witness in &bundle.release.release.reproducible_builds {
        if !release_signers.contains(&witness.builder_key_id) {
            return Err(FleetError::Authority(format!(
                "reproducible builder {} did not approve the exact release",
                witness.builder_key_id
            )));
        }
    }
    let release_digest = document_digest(&bundle.release.release)?;
    verify_transparency(policy, &bundle.transparency, &release_digest)?;

    bundle.rollout.plan.validate()?;
    verify_approvals(
        policy,
        ROLLOUT_DOMAIN,
        &bundle.rollout.plan,
        &bundle.rollout.approvals,
        &[AuthorityRole::Rollout, AuthorityRole::Security],
    )?;
    let plan = &bundle.rollout.plan;
    if plan.release_digest != release_digest
        || context.now < plan.created_at - policy.maximum_clock_skew_seconds
        || context.now > plan.expires_at + policy.maximum_clock_skew_seconds
    {
        return Err(FleetError::Activation(
            "rollout does not authorize this release at the current time".to_string(),
        ));
    }

    let ticket = &bundle.ticket.ticket;
    if ticket.format != ACTIVATION_TICKET_FORMAT
        || Uuid::parse_str(&ticket.ticket_id).is_err()
        || ticket.cell_id != context.cell_id
        || ticket.release_digest != release_digest
        || ticket.rollout_id != plan.rollout_id
        || ticket.previous_manifest_generation != context.previous_manifest_generation
        || ticket.previous_manifest_digest != context.previous_manifest_digest
        || ticket.next_manifest_generation != context.next_manifest_generation
        || ticket.next_manifest_digest != context.next_manifest_digest
        || ticket.next_manifest_generation != ticket.previous_manifest_generation.saturating_add(1)
        || ticket.issued_at <= 0
        || ticket.not_before < ticket.issued_at
        || ticket.expires_at <= ticket.not_before
        || ticket.expires_at - ticket.issued_at > policy.maximum_ticket_lifetime_seconds
        || context.now < ticket.not_before - policy.maximum_clock_skew_seconds
        || context.now > ticket.expires_at + policy.maximum_clock_skew_seconds
    {
        return Err(FleetError::Activation(
            "activation ticket does not exactly bind this Cell transition and time window"
                .to_string(),
        ));
    }
    verify_approvals(
        policy,
        ACTIVATION_DOMAIN,
        ticket,
        &bundle.ticket.approvals,
        &[AuthorityRole::Rollout, AuthorityRole::Security],
    )?;

    let cohort_index = usize::try_from(ticket.cohort_index)
        .map_err(|_| FleetError::Activation("cohort index is out of range".to_string()))?;
    let cohort = plan.cohorts.get(cohort_index).ok_or_else(|| {
        FleetError::Activation("activation ticket names an unknown cohort".to_string())
    })?;
    if !cohort.cells.contains(&context.cell_id)
        || context.now < cohort.not_before - policy.maximum_clock_skew_seconds
    {
        return Err(FleetError::Activation(
            "Cell is absent from the exact cohort or its activation window has not opened"
                .to_string(),
        ));
    }
    verify_prior_cohort_gates(policy, bundle, cohort_index)?;

    let mut expected = context.next_applications.clone();
    let mut released = bundle
        .release
        .release
        .applications
        .iter()
        .map(ApplicationRelease::target)
        .collect::<Vec<_>>();
    expected.sort();
    released.sort();
    if expected != released {
        return Err(FleetError::Activation(
            "fleet release applications differ from the exact next CellManifest pins".to_string(),
        ));
    }
    let runtime_version = Version::parse(&context.bicdb_version).map_err(|error| {
        FleetError::Activation(format!("running BicDB version is not semantic: {error}"))
    })?;
    let previous = context
        .previous_applications
        .iter()
        .map(|application| (application.application_id(), application))
        .collect::<BTreeMap<_, _>>();
    let mut rollback_until = i64::MAX;
    for application in &bundle.release.release.applications {
        let requirement = VersionReq::parse(&application.compatibility.bicdb_version_requirement)
            .map_err(|error| FleetError::Activation(error.to_string()))?;
        let previous_schema = previous
            .get(&application.application_id())
            .map(|application| application.schema_generation)
            .unwrap_or(0);
        if !requirement.matches(&runtime_version)
            || previous_schema < application.compatibility.from_schema_generation_min
            || previous_schema > application.compatibility.from_schema_generation_max
            || context.now > application.compatibility.rollback_until
        {
            return Err(FleetError::Activation(format!(
                "application {} is outside its runtime/schema/rollback compatibility window",
                application.application_id()
            )));
        }
        let minimum_safe = policy
            .minimum_safe_release_sequences
            .get(&application.application_id())
            .copied()
            .unwrap_or(1);
        if application.release_sequence < minimum_safe {
            return Err(FleetError::Activation(format!(
                "application {} is below the policy minimum-safe release",
                application.application_id()
            )));
        }
        rollback_until = rollback_until.min(application.compatibility.rollback_until);
    }
    if let Some(dormant_since) = context.dormant_since {
        if dormant_since <= 0 || dormant_since > context.now {
            return Err(FleetError::Activation(
                "dormant Cell timestamp is invalid".to_string(),
            ));
        }
        if context.now - dormant_since > policy.maximum_dormant_seconds {
            for application in &bundle.release.release.applications {
                if !policy
                    .minimum_safe_release_sequences
                    .contains_key(&application.application_id())
                {
                    return Err(FleetError::Activation(format!(
                        "long-dormant Cell lacks a minimum-safe release for {}",
                        application.application_id()
                    )));
                }
            }
        }
    }
    Ok(VerifiedFleetActivation {
        release_digest,
        rollout_id: plan.rollout_id.clone(),
        cohort_index: ticket.cohort_index,
        cohort_name: cohort.name.clone(),
        rollback_until,
        applications: released,
    })
}

fn verify_transparency(
    policy: &FleetTrustPolicy,
    proof: &TransparencyInclusionProof,
    release_digest: &FleetDigest,
) -> Result<()> {
    if proof.entries.is_empty() || proof.entries.len() > MAX_TRANSPARENCY_PROOF_ENTRIES {
        return Err(FleetError::Invalid(
            "transparency proof has an invalid bounded path".to_string(),
        ));
    }
    for entry in &proof.entries {
        entry.validate()?;
    }
    if proof.entries[0].release_digest != *release_digest {
        return Err(FleetError::Activation(
            "transparency proof is for another release".to_string(),
        ));
    }
    for pair in proof.entries.windows(2) {
        if pair[1].log_id != pair[0].log_id
            || pair[1].index != pair[0].index.saturating_add(1)
            || pair[1].previous_entry_digest.as_ref() != Some(&document_digest(&pair[0])?)
        {
            return Err(FleetError::Invalid(
                "transparency inclusion path is not an exact hash chain".to_string(),
            ));
        }
    }
    let last = proof.entries.last().expect("nonempty proof");
    let checkpoint = &proof.checkpoint.checkpoint;
    if checkpoint.format != TRANSPARENCY_CHECKPOINT_FORMAT
        || checkpoint.log_id != last.log_id
        || checkpoint.size != last.index.saturating_add(1)
        || checkpoint.head_entry_digest != document_digest(last)?
        || checkpoint.issued_at < last.recorded_at
    {
        return Err(FleetError::Invalid(
            "transparency checkpoint does not bind the proof head".to_string(),
        ));
    }
    verify_approvals(
        policy,
        CHECKPOINT_DOMAIN,
        checkpoint,
        &proof.checkpoint.approvals,
        &[AuthorityRole::Transparency, AuthorityRole::Security],
    )?;
    Ok(())
}

fn verify_prior_cohort_gates(
    policy: &FleetTrustPolicy,
    bundle: &FleetActivationBundle,
    cohort_index: usize,
) -> Result<()> {
    if bundle.prior_cohort_gates.len() != cohort_index {
        return Err(FleetError::Activation(
            "activation lacks an exact gate for every prior cohort".to_string(),
        ));
    }
    for (index, signed) in bundle.prior_cohort_gates.iter().enumerate() {
        let gate = &signed.gate;
        let cohort = &bundle.rollout.plan.cohorts[index];
        if gate.format != COHORT_GATE_FORMAT
            || gate.rollout_id != bundle.rollout.plan.rollout_id
            || gate.release_digest != bundle.rollout.plan.release_digest
            || usize::try_from(gate.cohort_index).ok() != Some(index)
            || gate.observed_from < cohort.not_before
            || gate.observed_until
                < gate
                    .observed_from
                    .saturating_add(cohort.observation_seconds)
            || gate.maximum_error_rate_basis_points > 10_000
            || gate.observed_error_rate_basis_points > gate.maximum_error_rate_basis_points
            || gate.decision != CohortDecision::Proceed
        {
            return Err(FleetError::Activation(format!(
                "prior cohort {index} has no successful bounded observation gate"
            )));
        }
        verify_approvals(
            policy,
            COHORT_GATE_DOMAIN,
            gate,
            &signed.approvals,
            &[AuthorityRole::Rollout, AuthorityRole::Security],
        )?;
    }
    Ok(())
}

/// Content-addressed storage with no mutation or alias operation. The
/// registry stores opaque bytes only; signed releases carry all authority.
#[derive(Clone, Debug)]
pub struct ImmutableArtifactRegistry {
    root: PathBuf,
}

impl ImmutableArtifactRegistry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(path)?;
        }
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(FleetError::Artifact(
                "artifact registry root must be a real directory".to_string(),
            ));
        }
        Ok(Self {
            root: fs::canonicalize(path)?,
        })
    }

    pub fn publish(
        &self,
        bytes: &[u8],
        media_type: impl Into<String>,
    ) -> Result<ArtifactReference> {
        let media_type = media_type.into();
        if bytes.is_empty()
            || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_ARTIFACT_BYTES
            || !valid_media_type(&media_type)
        {
            return Err(FleetError::Artifact(
                "artifact bytes or media type exceed registry policy".to_string(),
            ));
        }
        let digest = FleetDigest::of_bytes(bytes);
        let path = self.artifact_path(&digest)?;
        let parent = path.parent().expect("artifact path has shard");
        if !parent.exists() {
            fs::create_dir(parent)?;
        }
        let canonical_parent = fs::canonicalize(parent)?;
        if !canonical_parent.starts_with(&self.root) {
            return Err(FleetError::Artifact(
                "artifact shard escapes the registry root".to_string(),
            ));
        }
        if path.exists() {
            self.verify_existing(&path, &digest, bytes)?;
            return Ok(ArtifactReference {
                digest,
                size_bytes: bytes.len() as u64,
                media_type,
            });
        }
        let staging = parent.join(format!(
            ".{}.{}.{}.new",
            &digest.hex()[2..],
            std::process::id(),
            ARTIFACT_STAGING_NONCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&staging)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        match fs::hard_link(&staging, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.verify_existing(&path, &digest, bytes)?;
            }
            Err(error) => {
                let _ = fs::remove_file(&staging);
                return Err(error.into());
            }
        }
        fs::remove_file(&staging)?;
        File::open(parent)?.sync_all()?;
        Ok(ArtifactReference {
            digest,
            size_bytes: bytes.len() as u64,
            media_type,
        })
    }

    pub fn read(&self, reference: &ArtifactReference) -> Result<Vec<u8>> {
        reference.validate()?;
        let path = self.artifact_path(&reference.digest)?;
        let bytes = read_bounded_regular_file(&path, reference.size_bytes)?;
        if bytes.len() as u64 != reference.size_bytes
            || FleetDigest::of_bytes(&bytes) != reference.digest
        {
            return Err(FleetError::Artifact(
                "registry artifact differs from its immutable reference".to_string(),
            ));
        }
        Ok(bytes)
    }

    fn artifact_path(&self, digest: &FleetDigest) -> Result<PathBuf> {
        let hex_value = digest.hex();
        if hex_value.len() != 64 {
            return Err(FleetError::Artifact("invalid artifact digest".to_string()));
        }
        Ok(self.root.join(&hex_value[..2]).join(&hex_value[2..]))
    }

    fn verify_existing(&self, path: &Path, digest: &FleetDigest, expected: &[u8]) -> Result<()> {
        let bytes = read_bounded_regular_file(path, MAX_ARTIFACT_BYTES)?;
        if bytes != expected || FleetDigest::of_bytes(&bytes) != *digest {
            return Err(FleetError::Artifact(
                "immutable digest path already contains different bytes".to_string(),
            ));
        }
        Ok(())
    }
}

fn read_bounded_regular_file(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > maximum
    {
        return Err(FleetError::Artifact(format!(
            "{} is not a bounded regular file",
            path.display()
        )));
    }
    let file = File::open(path)?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or_default());
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(FleetError::Artifact(
            "file grew beyond its declared bound while reading".to_string(),
        ));
    }
    Ok(bytes)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConvergence {
    pub application_id: String,
    pub previous: Option<ApplicationTarget>,
    pub active: ApplicationTarget,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConvergenceReceipt {
    pub format: String,
    pub cell_id: FleetCellId,
    pub sequence: u64,
    pub previous_receipt_digest: Option<FleetDigest>,
    pub previous_manifest_digest: Option<FleetDigest>,
    pub active_manifest_digest: FleetDigest,
    pub release_digest: FleetDigest,
    pub rollout_id: String,
    pub cohort_index: u32,
    pub started_at: i64,
    pub completed_at: i64,
    pub rollback_until: i64,
    pub applications: Vec<ApplicationConvergence>,
}

impl ConvergenceReceipt {
    fn validate(&self) -> Result<()> {
        if self.format != CONVERGENCE_RECEIPT_FORMAT
            || self.sequence == 0
            || (self.sequence == 1) != self.previous_receipt_digest.is_none()
            || Uuid::parse_str(&self.rollout_id).is_err()
            || self.started_at <= 0
            || self.completed_at < self.started_at
            || self.rollback_until < self.completed_at
            || self.applications.is_empty()
            || self.applications.len() > MAX_APPLICATIONS
        {
            return Err(FleetError::Convergence(
                "convergence receipt has invalid identity, chain, timing, or applications"
                    .to_string(),
            ));
        }
        let mut ids = BTreeSet::new();
        for application in &self.applications {
            application.active.validate()?;
            if application.application_id != application.active.application_id()
                || !ids.insert(application.application_id.clone())
            {
                return Err(FleetError::Convergence(
                    "convergence receipt repeats or misbinds an application".to_string(),
                ));
            }
            if let Some(previous) = &application.previous {
                previous.validate()?;
                if previous.application_id() != application.application_id {
                    return Err(FleetError::Convergence(
                        "previous application identity differs from active identity".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ConvergenceLedger {
    root: PathBuf,
}

impl ConvergenceLedger {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(path)?;
        }
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(FleetError::Convergence(
                "convergence ledger must be a real directory".to_string(),
            ));
        }
        Ok(Self {
            root: fs::canonicalize(path)?,
        })
    }

    pub fn verify(&self) -> Result<Vec<(FleetDigest, ConvergenceReceipt)>> {
        let mut entries = fs::read_dir(&self.root)?.collect::<std::result::Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        if entries.len() > MAX_CONVERGENCE_RECEIPTS {
            return Err(FleetError::Convergence(
                "convergence ledger exceeds receipt bound".to_string(),
            ));
        }
        let mut receipts: Vec<(FleetDigest, ConvergenceReceipt)> =
            Vec::with_capacity(entries.len());
        let mut prior = None;
        for (index, entry) in entries.into_iter().enumerate() {
            let expected_sequence = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
            let expected_name = format!("{expected_sequence:020}.cbor");
            if entry.file_name().to_string_lossy() != expected_name {
                return Err(FleetError::Convergence(
                    "convergence ledger contains a gap or foreign file".to_string(),
                ));
            }
            let bytes = read_bounded_regular_file(&entry.path(), MAX_DOCUMENT_BYTES)?;
            let receipt: ConvergenceReceipt = decode_canonical_cbor(&bytes)?;
            receipt.validate()?;
            if receipt.sequence != expected_sequence || receipt.previous_receipt_digest != prior {
                return Err(FleetError::Convergence(
                    "convergence receipt chain is not contiguous".to_string(),
                ));
            }
            if let Some((_, first)) = receipts.first() {
                if receipt.cell_id != first.cell_id {
                    return Err(FleetError::Convergence(
                        "convergence ledger mixes Cell identities".to_string(),
                    ));
                }
            }
            let digest = FleetDigest::of_bytes(&bytes);
            prior = Some(digest.clone());
            receipts.push((digest, receipt));
        }
        Ok(receipts)
    }

    pub fn latest(&self) -> Result<Option<(FleetDigest, ConvergenceReceipt)>> {
        Ok(self.verify()?.pop())
    }

    pub fn append(&self, receipt: &ConvergenceReceipt) -> Result<FleetDigest> {
        receipt.validate()?;
        let latest = self.latest()?;
        let expected_sequence = latest
            .as_ref()
            .map(|(_, prior)| prior.sequence.saturating_add(1))
            .unwrap_or(1);
        let expected_prior = latest.as_ref().map(|(digest, _)| digest.clone());
        if receipt.sequence != expected_sequence
            || receipt.previous_receipt_digest != expected_prior
            || latest
                .as_ref()
                .is_some_and(|(_, prior)| prior.cell_id != receipt.cell_id)
        {
            return Err(FleetError::Convergence(
                "new convergence receipt does not extend the exact Cell-local head".to_string(),
            ));
        }
        let bytes = canonical_cbor(receipt)?;
        if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            return Err(FleetError::Convergence(
                "convergence receipt exceeds document bound".to_string(),
            ));
        }
        let path = self.root.join(format!("{:020}.cbor", receipt.sequence));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(FleetDigest::of_bytes(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Keys {
        publisher: SigningKey,
        security: SigningKey,
        builder_a: SigningKey,
        builder_b: SigningKey,
        transparency: SigningKey,
        rollout: SigningKey,
    }

    fn keys() -> Keys {
        Keys {
            publisher: SigningKey::from_bytes(&[1; 32]),
            security: SigningKey::from_bytes(&[2; 32]),
            builder_a: SigningKey::from_bytes(&[3; 32]),
            builder_b: SigningKey::from_bytes(&[4; 32]),
            transparency: SigningKey::from_bytes(&[5; 32]),
            rollout: SigningKey::from_bytes(&[6; 32]),
        }
    }

    fn policy(keys: &Keys) -> FleetTrustPolicy {
        let authority = |key_id: &str, role, key: &SigningKey| FleetAuthorityKey {
            key_id: key_id.to_string(),
            role,
            public_key: hex::encode(key.verifying_key().as_bytes()),
        };
        FleetTrustPolicy {
            format: FLEET_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "production-fleet".to_string(),
            generation: 1,
            authorities: vec![
                authority("publisher-a", AuthorityRole::Publisher, &keys.publisher),
                authority("security-a", AuthorityRole::Security, &keys.security),
                authority("builder-a", AuthorityRole::Builder, &keys.builder_a),
                authority("builder-b", AuthorityRole::Builder, &keys.builder_b),
                authority(
                    "transparency-a",
                    AuthorityRole::Transparency,
                    &keys.transparency,
                ),
                authority("rollout-a", AuthorityRole::Rollout, &keys.rollout),
            ],
            thresholds: BTreeMap::from([
                (AuthorityRole::Publisher, 1),
                (AuthorityRole::Security, 1),
                (AuthorityRole::Builder, 2),
                (AuthorityRole::Transparency, 1),
                (AuthorityRole::Rollout, 1),
            ]),
            minimum_reproducible_builds: 2,
            maximum_clock_skew_seconds: 30,
            maximum_ticket_lifetime_seconds: 3600,
            maximum_dormant_seconds: 86_400,
            minimum_safe_release_sequences: BTreeMap::from([(
                "generic-suite/generic-ledger".to_string(),
                7,
            )]),
        }
    }

    fn digest(label: &str) -> FleetDigest {
        FleetDigest::of_bytes(label.as_bytes())
    }

    fn target() -> ApplicationTarget {
        ApplicationTarget {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "2.0.0".to_string(),
            artifact_digest: digest("package"),
            schema_generation: 8,
            execution_scope: "cell".to_string(),
            data_class: "sensitive".to_string(),
        }
    }

    fn release(keys: &Keys) -> SignedFleetRelease {
        let component = ReleaseComponent {
            name: "backend".to_string(),
            kind: ReleaseComponentKind::Backend,
            artifact: ArtifactReference {
                digest: digest("backend"),
                size_bytes: 7,
                media_type: "application/wasm".to_string(),
            },
            provenance_digest: digest("component-provenance"),
            sbom_digest: digest("component-sbom"),
            build_recipe_digest: digest("recipe"),
        };
        let release = FleetRelease {
            format: FLEET_RELEASE_FORMAT.to_string(),
            release_id: "018f7b30-4f4d-7b5c-a1f6-a183663e1241".to_string(),
            published_at: 1_800_000_000,
            source_tree_digest: digest("source"),
            dependency_lock_digest: digest("lock"),
            sbom_digest: digest("sbom"),
            provenance_digest: digest("provenance"),
            applications: vec![ApplicationRelease {
                root: "generic-suite".to_string(),
                name: "generic-ledger".to_string(),
                version: "2.0.0".to_string(),
                release_sequence: 7,
                package: ArtifactReference {
                    digest: digest("package"),
                    size_bytes: 11,
                    media_type: "application/vnd.bicdb.app+json".to_string(),
                },
                schema_generation: 8,
                execution_scope: "cell".to_string(),
                data_class: "sensitive".to_string(),
                compatibility: ReleaseCompatibility {
                    bicdb_version_requirement: ">=1.0.321-beta, <2.0.0".to_string(),
                    from_schema_generation_min: 7,
                    from_schema_generation_max: 7,
                    target_schema_generation: 8,
                    record_format_min: 7,
                    record_format_max: 8,
                    rollback_until: 1_800_003_600,
                },
                components: vec![component],
            }],
            reproducible_builds: vec![
                ReproducibleBuildWitness {
                    builder_key_id: "builder-a".to_string(),
                    application_id: "generic-suite/generic-ledger".to_string(),
                    package_digest: digest("package"),
                    source_tree_digest: digest("source"),
                    build_recipe_digest: digest("recipe"),
                },
                ReproducibleBuildWitness {
                    builder_key_id: "builder-b".to_string(),
                    application_id: "generic-suite/generic-ledger".to_string(),
                    package_digest: digest("package"),
                    source_tree_digest: digest("source"),
                    build_recipe_digest: digest("recipe"),
                },
            ],
        };
        let approvals = vec![
            sign_release_approval(
                &release,
                AuthorityRole::Publisher,
                "publisher-a",
                &keys.publisher,
            )
            .unwrap(),
            sign_release_approval(
                &release,
                AuthorityRole::Security,
                "security-a",
                &keys.security,
            )
            .unwrap(),
            sign_release_approval(
                &release,
                AuthorityRole::Builder,
                "builder-a",
                &keys.builder_a,
            )
            .unwrap(),
            sign_release_approval(
                &release,
                AuthorityRole::Builder,
                "builder-b",
                &keys.builder_b,
            )
            .unwrap(),
        ];
        SignedFleetRelease { release, approvals }
    }

    fn activation_fixture() -> (FleetTrustPolicy, FleetActivationBundle, ActivationContext) {
        let keys = keys();
        let policy = policy(&keys);
        let release = release(&keys);
        let release_digest = document_digest(&release.release).unwrap();
        let entry = TransparencyEntry {
            format: TRANSPARENCY_ENTRY_FORMAT.to_string(),
            log_id: "primary-log".to_string(),
            index: 0,
            previous_entry_digest: None,
            release_digest: release_digest.clone(),
            recorded_at: 1_800_000_010,
        };
        let checkpoint = TransparencyCheckpoint {
            format: TRANSPARENCY_CHECKPOINT_FORMAT.to_string(),
            log_id: "primary-log".to_string(),
            size: 1,
            head_entry_digest: document_digest(&entry).unwrap(),
            issued_at: 1_800_000_020,
        };
        let transparency = TransparencyInclusionProof {
            entries: vec![entry],
            checkpoint: SignedTransparencyCheckpoint {
                approvals: vec![
                    sign_checkpoint_approval(
                        &checkpoint,
                        AuthorityRole::Transparency,
                        "transparency-a",
                        &keys.transparency,
                    )
                    .unwrap(),
                    sign_checkpoint_approval(
                        &checkpoint,
                        AuthorityRole::Security,
                        "security-a",
                        &keys.security,
                    )
                    .unwrap(),
                ],
                checkpoint,
            },
        };
        let cell = FleetCellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1240").unwrap();
        let plan = RolloutPlan {
            format: ROLLOUT_PLAN_FORMAT.to_string(),
            rollout_id: "018f7b30-4f4d-7b5c-a1f6-a183663e1242".to_string(),
            release_digest: release_digest.clone(),
            created_at: 1_800_000_000,
            expires_at: 1_800_003_600,
            cohorts: vec![RolloutCohort {
                name: "canary".to_string(),
                cells: vec![cell.clone()],
                not_before: 1_800_000_030,
                observation_seconds: 300,
                max_parallel: 1,
            }],
        };
        let rollout = SignedRolloutPlan {
            approvals: vec![
                sign_rollout_approval(&plan, AuthorityRole::Rollout, "rollout-a", &keys.rollout)
                    .unwrap(),
                sign_rollout_approval(&plan, AuthorityRole::Security, "security-a", &keys.security)
                    .unwrap(),
            ],
            plan,
        };
        let previous_manifest = digest("manifest-7");
        let next_manifest = digest("manifest-8");
        let ticket = ActivationTicket {
            format: ACTIVATION_TICKET_FORMAT.to_string(),
            ticket_id: "018f7b30-4f4d-7b5c-a1f6-a183663e1243".to_string(),
            cell_id: cell.clone(),
            release_digest,
            rollout_id: rollout.plan.rollout_id.clone(),
            cohort_index: 0,
            previous_manifest_generation: 7,
            previous_manifest_digest: Some(previous_manifest.clone()),
            next_manifest_generation: 8,
            next_manifest_digest: next_manifest.clone(),
            issued_at: 1_800_000_020,
            not_before: 1_800_000_030,
            expires_at: 1_800_003_000,
        };
        let signed_ticket = SignedActivationTicket {
            approvals: vec![
                sign_activation_approval(
                    &ticket,
                    AuthorityRole::Rollout,
                    "rollout-a",
                    &keys.rollout,
                )
                .unwrap(),
                sign_activation_approval(
                    &ticket,
                    AuthorityRole::Security,
                    "security-a",
                    &keys.security,
                )
                .unwrap(),
            ],
            ticket,
        };
        let context = ActivationContext {
            cell_id: cell,
            previous_manifest_generation: 7,
            previous_manifest_digest: Some(previous_manifest),
            previous_applications: vec![ApplicationTarget {
                root: "generic-suite".to_string(),
                name: "generic-ledger".to_string(),
                version: "1.9.0".to_string(),
                artifact_digest: digest("old-package"),
                schema_generation: 7,
                execution_scope: "cell".to_string(),
                data_class: "sensitive".to_string(),
            }],
            next_manifest_generation: 8,
            next_manifest_digest: next_manifest,
            next_applications: vec![target()],
            bicdb_version: "1.0.321-beta".to_string(),
            now: 1_800_000_040,
            dormant_since: Some(1_799_900_000),
        };
        (
            policy,
            FleetActivationBundle {
                format: ACTIVATION_BUNDLE_FORMAT.to_string(),
                release,
                transparency,
                rollout,
                prior_cohort_gates: vec![],
                ticket: signed_ticket,
            },
            context,
        )
    }

    #[test]
    fn exact_threshold_activation_is_accepted() {
        let (policy, bundle, context) = activation_fixture();
        let verified = verify_activation_bundle(&policy, &bundle, &context).unwrap();
        assert_eq!(verified.cohort_name, "canary");
        assert_eq!(verified.applications, vec![target()]);
    }

    #[test]
    fn one_compromised_release_signer_cannot_activate() {
        let (policy, mut bundle, context) = activation_fixture();
        bundle
            .release
            .approvals
            .retain(|approval| approval.role == AuthorityRole::Publisher);
        let error = verify_activation_bundle(&policy, &bundle, &context).unwrap_err();
        assert!(matches!(error, FleetError::Authority(_)));
    }

    #[test]
    fn bundle_cannot_move_to_another_cell_or_manifest() {
        let (policy, bundle, mut context) = activation_fixture();
        context.cell_id = FleetCellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e9999").unwrap();
        let error = verify_activation_bundle(&policy, &bundle, &context).unwrap_err();
        assert!(matches!(error, FleetError::Activation(_)));
    }

    #[test]
    fn immutable_registry_is_content_addressed_and_symlink_safe() {
        let directory = tempfile::tempdir().unwrap();
        let registry = ImmutableArtifactRegistry::open(directory.path()).unwrap();
        let reference = registry
            .publish(b"signed application", "application/octet-stream")
            .unwrap();
        assert_eq!(registry.read(&reference).unwrap(), b"signed application");
        assert_eq!(
            registry
                .publish(b"signed application", "application/octet-stream")
                .unwrap()
                .digest,
            reference.digest
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let escaped = tempfile::tempdir().unwrap();
            let root = directory.path().join("linked");
            symlink(escaped.path(), &root).unwrap();
            assert!(matches!(
                ImmutableArtifactRegistry::open(root),
                Err(FleetError::Artifact(_))
            ));
        }
    }

    #[test]
    fn convergence_receipts_are_fsynced_and_hash_chained() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = ConvergenceLedger::open(directory.path()).unwrap();
        let cell = FleetCellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1240").unwrap();
        let first = ConvergenceReceipt {
            format: CONVERGENCE_RECEIPT_FORMAT.to_string(),
            cell_id: cell.clone(),
            sequence: 1,
            previous_receipt_digest: None,
            previous_manifest_digest: Some(digest("manifest-7")),
            active_manifest_digest: digest("manifest-8"),
            release_digest: digest("release"),
            rollout_id: "018f7b30-4f4d-7b5c-a1f6-a183663e1242".to_string(),
            cohort_index: 0,
            started_at: 100,
            completed_at: 110,
            rollback_until: 200,
            applications: vec![ApplicationConvergence {
                application_id: target().application_id(),
                previous: None,
                active: target(),
            }],
        };
        let first_digest = ledger.append(&first).unwrap();
        let mut second = first.clone();
        second.sequence = 2;
        second.previous_receipt_digest = Some(first_digest);
        second.previous_manifest_digest = Some(first.active_manifest_digest.clone());
        second.active_manifest_digest = digest("manifest-9");
        ledger.append(&second).unwrap();
        assert_eq!(ledger.verify().unwrap().len(), 2);
        // Altering a historical receipt is detected by its successor's exact
        // predecessor digest. The cell layer anchors the current head beside
        // its monotonic manifest state.
        let first_path = directory.path().join("00000000000000000001.cbor");
        let mut bytes = fs::read(&first_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(first_path, bytes).unwrap();
        assert!(ledger.verify().is_err());
    }

    #[test]
    fn policy_forbids_one_key_in_multiple_trust_domains() {
        let keys = keys();
        let mut policy = policy(&keys);
        policy.authorities[1].public_key = policy.authorities[0].public_key.clone();
        assert!(policy.validate().is_err());
    }
}
