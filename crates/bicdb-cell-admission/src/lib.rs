//! Industry-neutral hardened-fleet admission evidence.
//!
//! This crate verifies build-specific, Cell-specific evidence for every gate in
//! the BicDB Cell architecture. It deliberately has no database, SQL, network,
//! application-runtime, orchestration, or key-provider dependency. Evidence
//! authorities can certify immutable facts, but this crate has no capability
//! that can open a Cell or unwrap a Cell key.

use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub const ADMISSION_TRUST_POLICY_FORMAT: &str = "bicdb.admission-trust-policy/v1";
pub const GATE_EVIDENCE_FORMAT: &str = "bicdb.admission-gate-evidence/v1";
pub const DEPLOYMENT_ATTESTATION_FORMAT: &str = "bicdb.deployment-attestation/v1";
pub const EVIDENCE_CHECKPOINT_FORMAT: &str = "bicdb.admission-evidence-checkpoint/v1";
pub const ADMISSION_AUTHORIZATION_FORMAT: &str = "bicdb.admission-authorization/v1";
pub const ADMISSION_BUNDLE_FORMAT: &str = "bicdb.admission-bundle/v1";

/// This release permits regulated-data admission only after the complete
/// Phase-8 evidence bundle has passed every check below. The trust policy,
/// threshold authorities, exact-build evidence, deployment attestation,
/// transparency checkpoint, and short-lived activation authorization remain
/// the authority; this release flag grants nothing on its own.
pub const REGULATED_ADMISSION_ENABLED: bool = true;

const GATE_EVIDENCE_DOMAIN: &[u8] = b"BICDB-ADMISSION-GATE-EVIDENCE-V1\0";
const DEPLOYMENT_ATTESTATION_DOMAIN: &[u8] = b"BICDB-DEPLOYMENT-ATTESTATION-V1\0";
const EVIDENCE_CHECKPOINT_DOMAIN: &[u8] = b"BICDB-ADMISSION-CHECKPOINT-V1\0";
const ADMISSION_AUTHORIZATION_DOMAIN: &[u8] = b"BICDB-ADMISSION-AUTHORIZATION-V1\0";
const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_APPLICATIONS: usize = 1024;
const MAX_EVIDENCE_ARTIFACTS: usize = 128;
const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_AUTHORITIES_PER_ROLE: usize = 16;
const MAX_AUTHORITIES: usize = MAX_AUTHORITIES_PER_ROLE * 4;

pub type Result<T> = std::result::Result<T, AdmissionError>;

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("ADMISSION_DOCUMENT_INVALID: {0}")]
    Invalid(String),
    #[error("ADMISSION_SIGNATURE_INVALID: {0}")]
    Signature(String),
    #[error("ADMISSION_AUTHORITY_INSUFFICIENT: {0}")]
    Authority(String),
    #[error("ADMISSION_SUBJECT_MISMATCH: {0}")]
    Subject(String),
    #[error("ADMISSION_EVIDENCE_INCOMPLETE: {0}")]
    Evidence(String),
    #[error("REGULATED_ADMISSION_INCOMPLETE: {0}")]
    NotEnabled(String),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct AdmissionDigest(String);

impl AdmissionDigest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let Some(hex_value) = value.strip_prefix("sha256:") else {
            return Err(AdmissionError::Invalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        };
        if value != value.to_ascii_lowercase()
            || hex_value.len() != 64
            || !hex_value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(AdmissionError::Invalid(
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
}

impl std::fmt::Display for AdmissionDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AdmissionDigest {
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
        .map_err(|error| AdmissionError::Invalid(format!("encode deterministic CBOR: {error}")))?;
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(AdmissionError::Invalid(
            "admission document exceeds the absolute 16 MiB bound".to_string(),
        ));
    }
    Ok(bytes)
}

pub fn encode_document<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_cbor(value)
}

pub fn decode_document<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(AdmissionError::Invalid(
            "admission document exceeds the absolute 16 MiB bound".to_string(),
        ));
    }
    let value: T = ciborium::de::from_reader(bytes)
        .map_err(|error| AdmissionError::Invalid(format!("decode CBOR: {error}")))?;
    if canonical_cbor(&value)? != bytes {
        return Err(AdmissionError::Invalid(
            "document is not in BicDB deterministic CBOR encoding".to_string(),
        ));
    }
    Ok(value)
}

pub fn document_digest<T: Serialize>(value: &T) -> Result<AdmissionDigest> {
    Ok(AdmissionDigest::of_bytes(&canonical_cbor(value)?))
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
        Err(AdmissionError::Invalid(format!(
            "{label} must be a 1..128 byte ASCII identifier"
        )))
    }
}

fn require_uuid(value: &str, label: &str) -> Result<()> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| AdmissionError::Invalid(format!("{label} must be a UUID")))?;
    if parsed.is_nil() {
        return Err(AdmissionError::Invalid(format!(
            "{label} must not be the nil UUID"
        )));
    }
    Ok(())
}

fn valid_media_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.contains('/')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionGate {
    RuntimeGraph,
    Identity,
    Encryption,
    Kms,
    Authorization,
    ApplicationSupplyChain,
    Isolation,
    ReplicationHa,
    BackupRecovery,
    Device,
    Sharing,
    IndependentReview,
}

impl AdmissionGate {
    pub const ALL: [Self; 12] = [
        Self::RuntimeGraph,
        Self::Identity,
        Self::Encryption,
        Self::Kms,
        Self::Authorization,
        Self::ApplicationSupplyChain,
        Self::Isolation,
        Self::ReplicationHa,
        Self::BackupRecovery,
        Self::Device,
        Self::Sharing,
        Self::IndependentReview,
    ];
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDiscipline {
    Database,
    Cryptography,
    Platform,
    RegulatedDataSafety,
    Privacy,
    Operations,
}

impl ReviewDiscipline {
    pub const ALL: [Self; 6] = [
        Self::Database,
        Self::Cryptography,
        Self::Platform,
        Self::RegulatedDataSafety,
        Self::Privacy,
        Self::Operations,
    ];
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionAuthorityRole {
    EvidenceReviewer,
    DeploymentAttestor,
    TransparencyWitness,
    AdmissionAuthority,
}

impl AdmissionAuthorityRole {
    pub const ALL: [Self; 4] = [
        Self::EvidenceReviewer,
        Self::DeploymentAttestor,
        Self::TransparencyWitness,
        Self::AdmissionAuthority,
    ];
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionAuthorityKey {
    pub key_id: String,
    pub role: AdmissionAuthorityRole,
    /// Exactly 64 lowercase hexadecimal digits encoding an Ed25519 public key.
    pub public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionTrustPolicy {
    pub format: String,
    pub policy_id: String,
    pub generation: u64,
    pub authorities: Vec<AdmissionAuthorityKey>,
    pub thresholds: BTreeMap<AdmissionAuthorityRole, u8>,
    pub maximum_clock_skew_seconds: i64,
    pub maximum_evidence_lifetime_seconds: i64,
    pub maximum_attestation_lifetime_seconds: i64,
    pub maximum_authorization_lifetime_seconds: i64,
    pub allowed_isolation_tiers: BTreeSet<String>,
    pub trusted_attestation_verifier_ids: BTreeSet<String>,
}

impl AdmissionTrustPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.format != ADMISSION_TRUST_POLICY_FORMAT
            || self.generation == 0
            || self.maximum_clock_skew_seconds < 0
            || self.maximum_clock_skew_seconds > 300
            || self.maximum_evidence_lifetime_seconds <= 0
            || self.maximum_evidence_lifetime_seconds > 366 * 24 * 60 * 60
            || self.maximum_attestation_lifetime_seconds <= 0
            || self.maximum_attestation_lifetime_seconds > 24 * 60 * 60
            || self.maximum_authorization_lifetime_seconds <= 0
            || self.maximum_authorization_lifetime_seconds > 24 * 60 * 60
        {
            return Err(AdmissionError::Invalid(
                "admission policy has an invalid format, generation, or lifetime bound".to_string(),
            ));
        }
        require_identifier(&self.policy_id, "policy_id")?;
        if self.allowed_isolation_tiers.is_empty()
            || self.trusted_attestation_verifier_ids.is_empty()
            || self.allowed_isolation_tiers.len() > 32
            || self.trusted_attestation_verifier_ids.len() > 32
        {
            return Err(AdmissionError::Invalid(
                "admission policy requires bounded isolation-tier and attestation-verifier allowlists"
                    .to_string(),
            ));
        }
        for tier in &self.allowed_isolation_tiers {
            require_identifier(tier, "isolation tier")?;
        }
        for verifier in &self.trusted_attestation_verifier_ids {
            require_identifier(verifier, "attestation verifier id")?;
        }

        let mut ids = BTreeSet::new();
        let mut material = BTreeSet::new();
        let mut available = BTreeMap::<AdmissionAuthorityRole, usize>::new();
        if self.authorities.len() > MAX_AUTHORITIES {
            return Err(AdmissionError::Invalid(format!(
                "admission policy cannot contain more than {MAX_AUTHORITIES} authorities"
            )));
        }
        let mut previous_id: Option<&str> = None;
        for authority in &self.authorities {
            require_identifier(&authority.key_id, "authority key_id")?;
            if previous_id.is_some_and(|previous| previous >= authority.key_id.as_str()) {
                return Err(AdmissionError::Invalid(
                    "authority keys must be strictly sorted by key_id".to_string(),
                ));
            }
            previous_id = Some(&authority.key_id);
            if !ids.insert(authority.key_id.clone()) {
                return Err(AdmissionError::Invalid(format!(
                    "duplicate authority key id {}",
                    authority.key_id
                )));
            }
            if authority.public_key != authority.public_key.to_ascii_lowercase()
                || authority.public_key.len() != 64
            {
                return Err(AdmissionError::Invalid(format!(
                    "authority {} is not a lowercase Ed25519 public key",
                    authority.key_id
                )));
            }
            let decoded = hex::decode(&authority.public_key).map_err(|_| {
                AdmissionError::Invalid("authority key is not hexadecimal".to_string())
            })?;
            let bytes: [u8; 32] = decoded.try_into().map_err(|_| {
                AdmissionError::Invalid("authority key must contain 32 bytes".to_string())
            })?;
            VerifyingKey::from_bytes(&bytes).map_err(|_| {
                AdmissionError::Invalid(format!("authority {} is invalid", authority.key_id))
            })?;
            if !material.insert(authority.public_key.clone()) {
                return Err(AdmissionError::Invalid(
                    "one public key cannot occupy more than one admission trust domain".to_string(),
                ));
            }
            *available.entry(authority.role).or_default() += 1;
        }
        for role in AdmissionAuthorityRole::ALL {
            let threshold = usize::from(*self.thresholds.get(&role).unwrap_or(&0));
            let role_authorities = available.get(&role).copied().unwrap_or(0);
            if threshold < 2
                || threshold > role_authorities
                || role_authorities > MAX_AUTHORITIES_PER_ROLE
            {
                return Err(AdmissionError::Invalid(format!(
                    "authority threshold for {role:?} must require 2..={MAX_AUTHORITIES_PER_ROLE} independent keys and be satisfiable"
                )));
            }
        }
        if self.thresholds.len() != AdmissionAuthorityRole::ALL.len() {
            return Err(AdmissionError::Invalid(
                "admission policy must define exactly the four authority thresholds".to_string(),
            ));
        }
        Ok(())
    }

    fn authority(&self, key_id: &str) -> Result<(&AdmissionAuthorityKey, VerifyingKey)> {
        let authority = self
            .authorities
            .iter()
            .find(|authority| authority.key_id == key_id)
            .ok_or_else(|| AdmissionError::Signature(format!("untrusted signer {key_id}")))?;
        let bytes: [u8; 32] = hex::decode(&authority.public_key)
            .map_err(|_| AdmissionError::Signature("invalid trusted key encoding".to_string()))?
            .try_into()
            .map_err(|_| AdmissionError::Signature("invalid trusted key length".to_string()))?;
        let key = VerifyingKey::from_bytes(&bytes)
            .map_err(|_| AdmissionError::Signature("invalid trusted Ed25519 key".to_string()))?;
        Ok((authority, key))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionApproval {
    pub key_id: String,
    pub role: AdmissionAuthorityRole,
    pub signature: Vec<u8>,
}

fn approval_message<T: Serialize>(
    domain: &[u8],
    role: AdmissionAuthorityRole,
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

pub fn sign_approval<T: Serialize>(
    domain: AdmissionSignatureDomain,
    role: AdmissionAuthorityRole,
    key_id: &str,
    payload: &T,
    key: &SigningKey,
) -> Result<AdmissionApproval> {
    require_identifier(key_id, "signing key id")?;
    let domain = domain.bytes();
    Ok(AdmissionApproval {
        key_id: key_id.to_string(),
        role,
        signature: key
            .sign(&approval_message(domain, role, key_id, payload)?)
            .to_bytes()
            .to_vec(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionSignatureDomain {
    GateEvidence,
    DeploymentAttestation,
    EvidenceCheckpoint,
    AdmissionAuthorization,
}

impl AdmissionSignatureDomain {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::GateEvidence => GATE_EVIDENCE_DOMAIN,
            Self::DeploymentAttestation => DEPLOYMENT_ATTESTATION_DOMAIN,
            Self::EvidenceCheckpoint => EVIDENCE_CHECKPOINT_DOMAIN,
            Self::AdmissionAuthorization => ADMISSION_AUTHORIZATION_DOMAIN,
        }
    }
}

fn verify_approvals<T: Serialize>(
    policy: &AdmissionTrustPolicy,
    domain: AdmissionSignatureDomain,
    payload: &T,
    approvals: &[AdmissionApproval],
    required_role: AdmissionAuthorityRole,
) -> Result<BTreeSet<String>> {
    let mut signers = BTreeSet::new();
    let threshold = usize::from(*policy.thresholds.get(&required_role).unwrap_or(&0));
    if approvals.len() != threshold {
        return Err(AdmissionError::Authority(format!(
            "{required_role:?} requires exactly {threshold} distinct approvals"
        )));
    }
    let mut previous_id: Option<&str> = None;
    for approval in approvals {
        if previous_id.is_some_and(|previous| previous >= approval.key_id.as_str()) {
            return Err(AdmissionError::Signature(
                "approval signatures must be strictly sorted by key_id".to_string(),
            ));
        }
        previous_id = Some(&approval.key_id);
        if !signers.insert(approval.key_id.clone()) {
            return Err(AdmissionError::Signature(format!(
                "signer {} appears more than once",
                approval.key_id
            )));
        }
        let (authority, key) = policy.authority(&approval.key_id)?;
        if authority.role != required_role || approval.role != required_role {
            return Err(AdmissionError::Signature(format!(
                "signer {} asserted or occupied the wrong authority role",
                approval.key_id
            )));
        }
        let signature = Signature::from_slice(&approval.signature).map_err(|_| {
            AdmissionError::Signature("invalid Ed25519 signature length".to_string())
        })?;
        key.verify_strict(
            &approval_message(domain.bytes(), approval.role, &approval.key_id, payload)?,
            &signature,
        )
        .map_err(|_| AdmissionError::Signature(format!("signature {} failed", approval.key_id)))?;
    }
    if signers.len() != threshold {
        return Err(AdmissionError::Authority(format!(
            "{required_role:?} requires exactly {threshold} distinct approvals"
        )));
    }
    Ok(signers)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ApplicationMeasurement {
    pub application_root: String,
    pub application_name: String,
    pub digest: AdmissionDigest,
    pub schema_generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionSubject {
    pub cell_id: String,
    pub manifest_digest: AdmissionDigest,
    pub bicdb_binary_digest: AdmissionDigest,
    pub guest_image_digest: AdmissionDigest,
    pub applications: Vec<ApplicationMeasurement>,
    pub security_profile: String,
    pub isolation_profile: String,
    pub deployment_isolation_tier: String,
}

impl AdmissionSubject {
    pub fn validate(&self) -> Result<()> {
        require_uuid(&self.cell_id, "cell_id")?;
        require_identifier(&self.security_profile, "security_profile")?;
        require_identifier(&self.isolation_profile, "isolation_profile")?;
        require_identifier(&self.deployment_isolation_tier, "deployment_isolation_tier")?;
        if self.applications.is_empty() || self.applications.len() > MAX_APPLICATIONS {
            return Err(AdmissionError::Invalid(
                "admission subject requires 1..1024 application measurements".to_string(),
            ));
        }
        let mut previous: Option<(&str, &str)> = None;
        for application in &self.applications {
            require_identifier(&application.application_root, "application_root")?;
            require_identifier(&application.application_name, "application_name")?;
            let identity = (
                application.application_root.as_str(),
                application.application_name.as_str(),
            );
            if application.schema_generation == 0 || previous.is_some_and(|value| value >= identity)
            {
                return Err(AdmissionError::Invalid(
                    "application measurements require positive generations and strict root/name order"
                        .to_string(),
                ));
            }
            previous = Some(identity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceTestSummary {
    pub executed: u64,
    pub passed: u64,
    pub failed: u64,
    pub skipped: u64,
    pub fixture_digest: AdmissionDigest,
    pub result_digest: AdmissionDigest,
}

impl EvidenceTestSummary {
    fn validate(&self) -> Result<()> {
        if self.executed == 0
            || self.passed == 0
            || self.failed != 0
            || self.skipped != 0
            || self.executed
                != self
                    .passed
                    .saturating_add(self.failed)
                    .saturating_add(self.skipped)
        {
            return Err(AdmissionError::Evidence(
                "gate evidence requires a non-empty, failure-free, skip-free exact test summary"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceArtifact {
    pub artifact_digest: AdmissionDigest,
    pub provenance_digest: AdmissionDigest,
    pub media_type: String,
    pub size_bytes: u64,
    pub produced_at: i64,
}

impl EvidenceArtifact {
    fn validate(&self) -> Result<()> {
        if !valid_media_type(&self.media_type)
            || self.size_bytes == 0
            || self.size_bytes > MAX_ARTIFACT_BYTES
            || self.produced_at <= 0
        {
            return Err(AdmissionError::Evidence(
                "evidence artifact has an invalid media type, size, or timestamp".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GateEvidence {
    pub format: String,
    pub evidence_id: String,
    pub gate: AdmissionGate,
    pub subject: AdmissionSubject,
    pub tests: EvidenceTestSummary,
    pub artifacts: Vec<EvidenceArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub review_disciplines: Vec<ReviewDiscipline>,
    pub issued_at: i64,
    pub expires_at: i64,
}

impl GateEvidence {
    fn validate(&self, policy: &AdmissionTrustPolicy, now: i64) -> Result<()> {
        if self.format != GATE_EVIDENCE_FORMAT {
            return Err(AdmissionError::Invalid(
                "unsupported gate evidence format".to_string(),
            ));
        }
        require_uuid(&self.evidence_id, "evidence_id")?;
        self.subject.validate()?;
        self.tests.validate()?;
        if self.artifacts.is_empty() || self.artifacts.len() > MAX_EVIDENCE_ARTIFACTS {
            return Err(AdmissionError::Evidence(
                "gate evidence requires 1..128 immutable artifacts".to_string(),
            ));
        }
        let mut artifact_digests = BTreeSet::new();
        for artifact in &self.artifacts {
            artifact.validate()?;
            if artifact.produced_at > self.issued_at
                || !artifact_digests.insert(artifact.artifact_digest.clone())
            {
                return Err(AdmissionError::Evidence(
                    "gate evidence artifacts must be unique and predate certification".to_string(),
                ));
            }
        }
        if self.issued_at <= 0
            || self.expires_at <= self.issued_at
            || self.expires_at - self.issued_at > policy.maximum_evidence_lifetime_seconds
            || now
                < self
                    .issued_at
                    .saturating_sub(policy.maximum_clock_skew_seconds)
            || now
                > self
                    .expires_at
                    .saturating_add(policy.maximum_clock_skew_seconds)
        {
            return Err(AdmissionError::Evidence(
                "gate evidence is outside its bounded validity window".to_string(),
            ));
        }
        let expected_disciplines = ReviewDiscipline::ALL.to_vec();
        if (self.gate == AdmissionGate::IndependentReview
            && self.review_disciplines != expected_disciplines)
            || (self.gate != AdmissionGate::IndependentReview
                && !self.review_disciplines.is_empty())
        {
            return Err(AdmissionError::Evidence(
                "only the independent-review gate must carry the exact six review disciplines"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedGateEvidence {
    pub evidence: GateEvidence,
    pub approvals: Vec<AdmissionApproval>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeploymentAttestation {
    pub format: String,
    pub attestation_id: String,
    pub subject: AdmissionSubject,
    pub attestation_verifier_id: String,
    pub attestation_report_digest: AdmissionDigest,
    pub workload_identity_digest: AdmissionDigest,
    pub kms_scope_digest: AdmissionDigest,
    pub capability_graph_digest: AdmissionDigest,
    pub storage_mount_digest: AdmissionDigest,
    pub network_policy_digest: AdmissionDigest,
    pub crash_dump_policy_digest: AdmissionDigest,
    pub observability_policy_digest: AdmissionDigest,
    /// Exactly 64 lowercase hexadecimal digits supplied by the activation
    /// authority and covered by the attestation.
    pub nonce: String,
    pub issued_at: i64,
    pub expires_at: i64,
}

impl DeploymentAttestation {
    fn validate(&self, policy: &AdmissionTrustPolicy, now: i64) -> Result<()> {
        if self.format != DEPLOYMENT_ATTESTATION_FORMAT {
            return Err(AdmissionError::Invalid(
                "unsupported deployment attestation format".to_string(),
            ));
        }
        require_uuid(&self.attestation_id, "attestation_id")?;
        self.subject.validate()?;
        require_identifier(&self.attestation_verifier_id, "attestation_verifier_id")?;
        if !policy
            .trusted_attestation_verifier_ids
            .contains(&self.attestation_verifier_id)
            || !policy
                .allowed_isolation_tiers
                .contains(&self.subject.deployment_isolation_tier)
        {
            return Err(AdmissionError::Evidence(
                "deployment attestation verifier or isolation tier is not policy-trusted"
                    .to_string(),
            ));
        }
        if self.nonce.len() != 64
            || self.nonce != self.nonce.to_ascii_lowercase()
            || !self.nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(AdmissionError::Evidence(
                "deployment attestation nonce must be 32 lowercase hexadecimal bytes".to_string(),
            ));
        }
        if self.issued_at <= 0
            || self.expires_at <= self.issued_at
            || self.expires_at - self.issued_at > policy.maximum_attestation_lifetime_seconds
            || now
                < self
                    .issued_at
                    .saturating_sub(policy.maximum_clock_skew_seconds)
            || now
                > self
                    .expires_at
                    .saturating_add(policy.maximum_clock_skew_seconds)
        {
            return Err(AdmissionError::Evidence(
                "deployment attestation is outside its bounded validity window".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedDeploymentAttestation {
    pub attestation: DeploymentAttestation,
    pub approvals: Vec<AdmissionApproval>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceCheckpoint {
    pub format: String,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_checkpoint_digest: Option<AdmissionDigest>,
    pub gate_evidence_digests: Vec<AdmissionDigest>,
    pub deployment_attestation_digest: AdmissionDigest,
    pub issued_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedEvidenceCheckpoint {
    pub checkpoint: EvidenceCheckpoint,
    pub approvals: Vec<AdmissionApproval>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionAuthorization {
    pub format: String,
    pub authorization_id: String,
    pub policy_digest: AdmissionDigest,
    pub subject: AdmissionSubject,
    pub gate_evidence_digests: Vec<AdmissionDigest>,
    pub deployment_attestation_digest: AdmissionDigest,
    pub evidence_checkpoint_digest: AdmissionDigest,
    pub nonce: String,
    pub issued_at: i64,
    pub not_before: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedAdmissionAuthorization {
    pub authorization: AdmissionAuthorization,
    pub approvals: Vec<AdmissionApproval>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionBundle {
    pub format: String,
    pub gate_evidence: Vec<CertifiedGateEvidence>,
    pub deployment: CertifiedDeploymentAttestation,
    pub checkpoint: CertifiedEvidenceCheckpoint,
    pub authorization: CertifiedAdmissionAuthorization,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedAdmissionEvidence {
    pub policy_digest: AdmissionDigest,
    pub bundle_digest: AdmissionDigest,
    pub authorization_id: String,
    pub checkpoint_sequence: u64,
    pub evidence_expires_at: i64,
    pub deployment_expires_at: i64,
    pub authorization_expires_at: i64,
    pub verified_gates: BTreeSet<AdmissionGate>,
    pub evidence_complete: bool,
    pub regulated_admission_enabled: bool,
}

impl VerifiedAdmissionEvidence {
    /// Check the current signed admission window. Expiry is exclusive and
    /// runtime use receives no startup clock-skew allowance.
    pub fn regulated_admission_result_at(&self, now: i64) -> Result<()> {
        self.check_enabled()?;
        if now < 0
            || now >= self.evidence_expires_at
            || now >= self.deployment_expires_at
            || now >= self.authorization_expires_at
        {
            return Err(AdmissionError::Evidence(
                "regulated-data admission window has expired or the clock is invalid".to_string(),
            ));
        }
        Ok(())
    }

    pub fn regulated_admission_result(&self) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_secs()).ok())
            .unwrap_or(i64::MAX);
        self.regulated_admission_result_at(now)
    }

    fn check_enabled(&self) -> Result<()> {
        if self.evidence_complete && self.regulated_admission_enabled {
            Ok(())
        } else {
            Err(AdmissionError::NotEnabled(
                "regulated-data admission requires a complete signed Phase-8 evidence bundle and an admission-enabled release"
                    .to_string(),
            ))
        }
    }
}

pub fn verify_admission_bundle(
    policy: &AdmissionTrustPolicy,
    bundle: &AdmissionBundle,
    expected_subject: &AdmissionSubject,
    now: i64,
) -> Result<VerifiedAdmissionEvidence> {
    policy.validate()?;
    expected_subject.validate()?;
    if bundle.format != ADMISSION_BUNDLE_FORMAT {
        return Err(AdmissionError::Invalid(
            "unsupported admission bundle format".to_string(),
        ));
    }
    if !policy
        .allowed_isolation_tiers
        .contains(&expected_subject.deployment_isolation_tier)
    {
        return Err(AdmissionError::Subject(
            "expected deployment isolation tier is not policy-trusted".to_string(),
        ));
    }
    if bundle.gate_evidence.len() != AdmissionGate::ALL.len() {
        return Err(AdmissionError::Evidence(
            "admission bundle must contain exactly all twelve gate certificates".to_string(),
        ));
    }

    let mut gates = BTreeSet::new();
    let mut evidence_ids = BTreeSet::new();
    let mut gate_digests = Vec::with_capacity(AdmissionGate::ALL.len());
    let mut evidence_expires_at = i64::MAX;
    let mut latest_evidence_issued_at = 0_i64;
    for (index, certified) in bundle.gate_evidence.iter().enumerate() {
        let expected_gate = AdmissionGate::ALL[index];
        if certified.evidence.gate != expected_gate
            || !gates.insert(certified.evidence.gate)
            || !evidence_ids.insert(certified.evidence.evidence_id.clone())
        {
            return Err(AdmissionError::Evidence(
                "gate certificates must contain the exact twelve gates in canonical order with unique evidence ids"
                    .to_string(),
            ));
        }
        certified.evidence.validate(policy, now)?;
        if &certified.evidence.subject != expected_subject {
            return Err(AdmissionError::Subject(format!(
                "{:?} evidence belongs to another exact build, Cell, manifest, application set, or deployment profile",
                certified.evidence.gate
            )));
        }
        verify_approvals(
            policy,
            AdmissionSignatureDomain::GateEvidence,
            &certified.evidence,
            &certified.approvals,
            AdmissionAuthorityRole::EvidenceReviewer,
        )?;
        gate_digests.push(document_digest(certified)?);
        evidence_expires_at = evidence_expires_at.min(certified.evidence.expires_at);
        latest_evidence_issued_at = latest_evidence_issued_at.max(certified.evidence.issued_at);
    }

    let deployment = &bundle.deployment.attestation;
    deployment.validate(policy, now)?;
    if &deployment.subject != expected_subject {
        return Err(AdmissionError::Subject(
            "deployment attestation belongs to another exact admission subject".to_string(),
        ));
    }
    verify_approvals(
        policy,
        AdmissionSignatureDomain::DeploymentAttestation,
        deployment,
        &bundle.deployment.approvals,
        AdmissionAuthorityRole::DeploymentAttestor,
    )?;
    let deployment_digest = document_digest(&bundle.deployment)?;

    let checkpoint = &bundle.checkpoint.checkpoint;
    if checkpoint.format != EVIDENCE_CHECKPOINT_FORMAT
        || checkpoint.sequence == 0
        || (checkpoint.sequence == 1) != checkpoint.previous_checkpoint_digest.is_none()
        || checkpoint.gate_evidence_digests != gate_digests
        || checkpoint.deployment_attestation_digest != deployment_digest
        || checkpoint.issued_at <= 0
        || checkpoint.issued_at < latest_evidence_issued_at
        || checkpoint.issued_at < deployment.issued_at
        || checkpoint.issued_at > evidence_expires_at
        || checkpoint.issued_at > deployment.expires_at
        || checkpoint.issued_at > now.saturating_add(policy.maximum_clock_skew_seconds)
    {
        return Err(AdmissionError::Evidence(
            "transparency checkpoint does not exactly commit the canonical evidence set and chain position"
                .to_string(),
        ));
    }
    verify_approvals(
        policy,
        AdmissionSignatureDomain::EvidenceCheckpoint,
        checkpoint,
        &bundle.checkpoint.approvals,
        AdmissionAuthorityRole::TransparencyWitness,
    )?;
    let checkpoint_digest = document_digest(&bundle.checkpoint)?;

    let authorization = &bundle.authorization.authorization;
    let policy_digest = document_digest(policy)?;
    if authorization.format != ADMISSION_AUTHORIZATION_FORMAT
        || authorization.policy_digest != policy_digest
        || &authorization.subject != expected_subject
        || authorization.gate_evidence_digests != gate_digests
        || authorization.deployment_attestation_digest != deployment_digest
        || authorization.evidence_checkpoint_digest != checkpoint_digest
    {
        return Err(AdmissionError::Subject(
            "admission authorization does not bind the exact policy, subject, evidence, attestation, and checkpoint"
                .to_string(),
        ));
    }
    require_uuid(&authorization.authorization_id, "authorization_id")?;
    if authorization.nonce != deployment.nonce
        || authorization.nonce.len() != 64
        || authorization.issued_at <= 0
        || authorization.issued_at < checkpoint.issued_at
        || authorization.not_before < authorization.issued_at
        || authorization.expires_at <= authorization.not_before
        || authorization.expires_at - authorization.issued_at
            > policy.maximum_authorization_lifetime_seconds
        || now
            < authorization
                .not_before
                .saturating_sub(policy.maximum_clock_skew_seconds)
        || now
            > authorization
                .expires_at
                .saturating_add(policy.maximum_clock_skew_seconds)
        || authorization.expires_at > deployment.expires_at
        || authorization.expires_at > evidence_expires_at
    {
        return Err(AdmissionError::Evidence(
            "admission authorization has an invalid nonce or bounded validity window".to_string(),
        ));
    }
    verify_approvals(
        policy,
        AdmissionSignatureDomain::AdmissionAuthorization,
        authorization,
        &bundle.authorization.approvals,
        AdmissionAuthorityRole::AdmissionAuthority,
    )?;

    Ok(VerifiedAdmissionEvidence {
        policy_digest,
        bundle_digest: document_digest(bundle)?,
        authorization_id: authorization.authorization_id.clone(),
        checkpoint_sequence: checkpoint.sequence,
        evidence_expires_at,
        deployment_expires_at: deployment.expires_at,
        authorization_expires_at: authorization.expires_at,
        verified_gates: gates,
        evidence_complete: true,
        regulated_admission_enabled: REGULATED_ADMISSION_ENABLED,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 2_000_000_000;

    struct Keys {
        evidence: Vec<(String, SigningKey)>,
        deployment: Vec<(String, SigningKey)>,
        transparency: Vec<(String, SigningKey)>,
        admission: Vec<(String, SigningKey)>,
    }

    fn signing_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn keys() -> Keys {
        Keys {
            evidence: vec![
                ("evidence-1".to_string(), signing_key(1)),
                ("evidence-2".to_string(), signing_key(2)),
            ],
            deployment: vec![
                ("deployment-1".to_string(), signing_key(3)),
                ("deployment-2".to_string(), signing_key(4)),
            ],
            transparency: vec![
                ("transparency-1".to_string(), signing_key(5)),
                ("transparency-2".to_string(), signing_key(6)),
            ],
            admission: vec![
                ("admission-1".to_string(), signing_key(7)),
                ("admission-2".to_string(), signing_key(8)),
            ],
        }
    }

    fn authorities(keys: &Keys) -> Vec<AdmissionAuthorityKey> {
        let mut authorities = Vec::new();
        for (role, entries) in [
            (AdmissionAuthorityRole::EvidenceReviewer, &keys.evidence),
            (AdmissionAuthorityRole::DeploymentAttestor, &keys.deployment),
            (
                AdmissionAuthorityRole::TransparencyWitness,
                &keys.transparency,
            ),
            (AdmissionAuthorityRole::AdmissionAuthority, &keys.admission),
        ] {
            for (key_id, key) in entries {
                authorities.push(AdmissionAuthorityKey {
                    key_id: key_id.clone(),
                    role,
                    public_key: hex::encode(key.verifying_key().as_bytes()),
                });
            }
        }
        authorities.sort_by(|left, right| left.key_id.cmp(&right.key_id));
        authorities
    }

    fn policy(keys: &Keys) -> AdmissionTrustPolicy {
        AdmissionTrustPolicy {
            format: ADMISSION_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "production-admission-2026".to_string(),
            generation: 1,
            authorities: authorities(keys),
            thresholds: AdmissionAuthorityRole::ALL
                .into_iter()
                .map(|role| (role, 2))
                .collect(),
            maximum_clock_skew_seconds: 30,
            maximum_evidence_lifetime_seconds: 30 * 24 * 60 * 60,
            maximum_attestation_lifetime_seconds: 600,
            maximum_authorization_lifetime_seconds: 300,
            allowed_isolation_tiers: ["confidential-microvm".to_string()].into_iter().collect(),
            trusted_attestation_verifier_ids: ["verifier-prod-1".to_string()].into_iter().collect(),
        }
    }

    fn digest(label: &str) -> AdmissionDigest {
        AdmissionDigest::of_bytes(label.as_bytes())
    }

    fn subject() -> AdmissionSubject {
        AdmissionSubject {
            cell_id: "018f0000-0000-7000-8000-000000000001".to_string(),
            manifest_digest: digest("manifest"),
            bicdb_binary_digest: digest("binary"),
            guest_image_digest: digest("guest"),
            applications: vec![ApplicationMeasurement {
                application_root: "example-root".to_string(),
                application_name: "example-app".to_string(),
                digest: digest("application"),
                schema_generation: 17,
            }],
            security_profile: "phase8-hardened-fleet-deny-regulated".to_string(),
            isolation_profile: "phase8-attested-confidential-microvm".to_string(),
            deployment_isolation_tier: "confidential-microvm".to_string(),
        }
    }

    fn approvals<T: Serialize>(
        domain: AdmissionSignatureDomain,
        role: AdmissionAuthorityRole,
        payload: &T,
        keys: &[(String, SigningKey)],
    ) -> Vec<AdmissionApproval> {
        let mut signatures = keys
            .iter()
            .map(|(key_id, key)| sign_approval(domain, role, key_id, payload, key).unwrap())
            .collect::<Vec<_>>();
        signatures.sort_by(|left, right| left.key_id.cmp(&right.key_id));
        signatures
    }

    fn fixture() -> (
        AdmissionTrustPolicy,
        AdmissionBundle,
        AdmissionSubject,
        Keys,
    ) {
        let keys = keys();
        let policy = policy(&keys);
        let subject = subject();
        let mut gate_evidence = Vec::new();
        for (index, gate) in AdmissionGate::ALL.into_iter().enumerate() {
            let evidence = GateEvidence {
                format: GATE_EVIDENCE_FORMAT.to_string(),
                evidence_id: Uuid::from_u128(100 + index as u128)
                    .hyphenated()
                    .to_string(),
                gate,
                subject: subject.clone(),
                tests: EvidenceTestSummary {
                    executed: 10,
                    passed: 10,
                    failed: 0,
                    skipped: 0,
                    fixture_digest: digest(&format!("fixture-{index}")),
                    result_digest: digest(&format!("result-{index}")),
                },
                artifacts: vec![EvidenceArtifact {
                    artifact_digest: digest(&format!("artifact-{index}")),
                    provenance_digest: digest(&format!("provenance-{index}")),
                    media_type: "application/vnd.bicdb.evidence+cbor".to_string(),
                    size_bytes: 4096,
                    produced_at: NOW - 100,
                }],
                review_disciplines: if gate == AdmissionGate::IndependentReview {
                    ReviewDiscipline::ALL.to_vec()
                } else {
                    Vec::new()
                },
                issued_at: NOW - 60,
                expires_at: NOW + 3600,
            };
            let approvals = approvals(
                AdmissionSignatureDomain::GateEvidence,
                AdmissionAuthorityRole::EvidenceReviewer,
                &evidence,
                &keys.evidence,
            );
            gate_evidence.push(CertifiedGateEvidence {
                evidence,
                approvals,
            });
        }

        let deployment = DeploymentAttestation {
            format: DEPLOYMENT_ATTESTATION_FORMAT.to_string(),
            attestation_id: Uuid::from_u128(1000).hyphenated().to_string(),
            subject: subject.clone(),
            attestation_verifier_id: "verifier-prod-1".to_string(),
            attestation_report_digest: digest("attestation-report"),
            workload_identity_digest: digest("workload-identity"),
            kms_scope_digest: digest("kms-scope"),
            capability_graph_digest: digest("capability-graph"),
            storage_mount_digest: digest("storage-mount"),
            network_policy_digest: digest("network-policy"),
            crash_dump_policy_digest: digest("crash-dump-policy"),
            observability_policy_digest: digest("observability-policy"),
            nonce: "ab".repeat(32),
            issued_at: NOW - 30,
            expires_at: NOW + 300,
        };
        let deployment = CertifiedDeploymentAttestation {
            approvals: approvals(
                AdmissionSignatureDomain::DeploymentAttestation,
                AdmissionAuthorityRole::DeploymentAttestor,
                &deployment,
                &keys.deployment,
            ),
            attestation: deployment,
        };
        let gate_evidence_digests = gate_evidence
            .iter()
            .map(|evidence| document_digest(evidence).unwrap())
            .collect::<Vec<_>>();
        let deployment_attestation_digest = document_digest(&deployment).unwrap();
        let checkpoint = EvidenceCheckpoint {
            format: EVIDENCE_CHECKPOINT_FORMAT.to_string(),
            sequence: 1,
            previous_checkpoint_digest: None,
            gate_evidence_digests: gate_evidence_digests.clone(),
            deployment_attestation_digest: deployment_attestation_digest.clone(),
            issued_at: NOW - 20,
        };
        let checkpoint = CertifiedEvidenceCheckpoint {
            approvals: approvals(
                AdmissionSignatureDomain::EvidenceCheckpoint,
                AdmissionAuthorityRole::TransparencyWitness,
                &checkpoint,
                &keys.transparency,
            ),
            checkpoint,
        };
        let authorization = AdmissionAuthorization {
            format: ADMISSION_AUTHORIZATION_FORMAT.to_string(),
            authorization_id: Uuid::from_u128(2000).hyphenated().to_string(),
            policy_digest: document_digest(&policy).unwrap(),
            subject: subject.clone(),
            gate_evidence_digests,
            deployment_attestation_digest,
            evidence_checkpoint_digest: document_digest(&checkpoint).unwrap(),
            nonce: "ab".repeat(32),
            issued_at: NOW - 10,
            not_before: NOW - 5,
            expires_at: NOW + 240,
        };
        let authorization = CertifiedAdmissionAuthorization {
            approvals: approvals(
                AdmissionSignatureDomain::AdmissionAuthorization,
                AdmissionAuthorityRole::AdmissionAuthority,
                &authorization,
                &keys.admission,
            ),
            authorization,
        };
        (
            policy,
            AdmissionBundle {
                format: ADMISSION_BUNDLE_FORMAT.to_string(),
                gate_evidence,
                deployment,
                checkpoint,
                authorization,
            },
            subject,
            keys,
        )
    }

    #[test]
    fn exact_twelve_gate_bundle_enables_regulated_admission() {
        let (policy, bundle, subject, _) = fixture();
        let verified = verify_admission_bundle(&policy, &bundle, &subject, NOW).unwrap();
        assert_eq!(verified.verified_gates.len(), 12);
        assert!(verified.evidence_complete);
        assert!(verified.regulated_admission_enabled);
        verified.regulated_admission_result_at(NOW).unwrap();

        let mut incomplete = verified.clone();
        incomplete.evidence_complete = false;
        assert!(matches!(
            incomplete.regulated_admission_result_at(NOW),
            Err(AdmissionError::NotEnabled(_))
        ));
    }

    #[test]
    fn continuous_admission_checks_each_deadline_without_reverification() {
        let (policy, bundle, subject, _) = fixture();
        let verified = verify_admission_bundle(&policy, &bundle, &subject, NOW).unwrap();
        for deadline in 0..3 {
            let mut evidence = verified.clone();
            evidence.evidence_expires_at = NOW + 100;
            evidence.deployment_expires_at = NOW + 100;
            evidence.authorization_expires_at = NOW + 100;
            match deadline {
                0 => evidence.evidence_expires_at = NOW + 1,
                1 => evidence.deployment_expires_at = NOW + 1,
                _ => evidence.authorization_expires_at = NOW + 1,
            }
            evidence.regulated_admission_result_at(NOW).unwrap();
            assert!(evidence.regulated_admission_result_at(NOW + 1).is_err());
            assert!(evidence.regulated_admission_result_at(NOW + 101).is_err());
            assert!(evidence.regulated_admission_result_at(i64::MIN).is_err());
            assert!(evidence.regulated_admission_result_at(i64::MAX).is_err());
        }
    }

    #[test]
    fn missing_duplicate_wrong_role_and_build_substitution_fail_closed() {
        let (policy, bundle, subject, keys) = fixture();

        let mut missing = bundle.clone();
        missing.gate_evidence.pop();
        assert!(matches!(
            verify_admission_bundle(&policy, &missing, &subject, NOW),
            Err(AdmissionError::Evidence(_))
        ));

        let mut duplicate = bundle.clone();
        duplicate.gate_evidence[1] = duplicate.gate_evidence[0].clone();
        assert!(matches!(
            verify_admission_bundle(&policy, &duplicate, &subject, NOW),
            Err(AdmissionError::Evidence(_))
        ));

        let mut wrong_role = bundle.clone();
        let evidence = &wrong_role.gate_evidence[0].evidence;
        wrong_role.gate_evidence[0].approvals = approvals(
            AdmissionSignatureDomain::GateEvidence,
            AdmissionAuthorityRole::DeploymentAttestor,
            evidence,
            &keys.deployment,
        );
        assert!(matches!(
            verify_admission_bundle(&policy, &wrong_role, &subject, NOW),
            Err(AdmissionError::Signature(_))
        ));

        let mut another_build = subject.clone();
        another_build.bicdb_binary_digest = digest("another-binary");
        assert!(matches!(
            verify_admission_bundle(&policy, &bundle, &another_build, NOW),
            Err(AdmissionError::Subject(_))
        ));
    }

    #[test]
    fn stale_tampered_or_untrusted_attestation_fails_closed() {
        let (policy, bundle, subject, keys) = fixture();
        assert!(matches!(
            verify_admission_bundle(&policy, &bundle, &subject, NOW + 4000),
            Err(AdmissionError::Evidence(_))
        ));
        assert!(matches!(
            verify_admission_bundle(&policy, &bundle, &subject, i64::MIN),
            Err(AdmissionError::Evidence(_))
        ));
        assert!(matches!(
            verify_admission_bundle(&policy, &bundle, &subject, i64::MAX),
            Err(AdmissionError::Evidence(_))
        ));

        let mut tampered = bundle.clone();
        tampered.gate_evidence[0].evidence.tests.result_digest = digest("tampered");
        assert!(matches!(
            verify_admission_bundle(&policy, &tampered, &subject, NOW),
            Err(AdmissionError::Signature(_))
        ));

        let mut untrusted = bundle.clone();
        untrusted.deployment.attestation.attestation_verifier_id = "self-attested".to_string();
        assert!(matches!(
            verify_admission_bundle(&policy, &untrusted, &subject, NOW),
            Err(AdmissionError::Evidence(_))
        ));

        let mut checkpoint_before_evidence = bundle.clone();
        checkpoint_before_evidence.checkpoint.checkpoint.issued_at = NOW - 120;
        checkpoint_before_evidence.checkpoint.approvals = approvals(
            AdmissionSignatureDomain::EvidenceCheckpoint,
            AdmissionAuthorityRole::TransparencyWitness,
            &checkpoint_before_evidence.checkpoint.checkpoint,
            &keys.transparency,
        );
        checkpoint_before_evidence
            .authorization
            .authorization
            .evidence_checkpoint_digest =
            document_digest(&checkpoint_before_evidence.checkpoint).unwrap();
        checkpoint_before_evidence.authorization.approvals = approvals(
            AdmissionSignatureDomain::AdmissionAuthorization,
            AdmissionAuthorityRole::AdmissionAuthority,
            &checkpoint_before_evidence.authorization.authorization,
            &keys.admission,
        );
        assert!(matches!(
            verify_admission_bundle(&policy, &checkpoint_before_evidence, &subject, NOW),
            Err(AdmissionError::Evidence(_))
        ));

        let mut authorization_before_checkpoint = bundle.clone();
        authorization_before_checkpoint
            .authorization
            .authorization
            .issued_at = NOW - 25;
        authorization_before_checkpoint
            .authorization
            .authorization
            .not_before = NOW - 24;
        authorization_before_checkpoint.authorization.approvals = approvals(
            AdmissionSignatureDomain::AdmissionAuthorization,
            AdmissionAuthorityRole::AdmissionAuthority,
            &authorization_before_checkpoint.authorization.authorization,
            &keys.admission,
        );
        assert!(matches!(
            verify_admission_bundle(&policy, &authorization_before_checkpoint, &subject, NOW),
            Err(AdmissionError::Evidence(_))
        ));
    }

    #[test]
    fn canonical_documents_and_disjoint_threshold_roles_are_mandatory() {
        let (policy, bundle, _, keys) = fixture();
        let mut bytes = encode_document(&bundle).unwrap();
        bytes.push(0);
        assert!(matches!(
            decode_document::<AdmissionBundle>(&bytes),
            Err(AdmissionError::Invalid(_))
        ));

        let mut reused = policy.clone();
        reused.authorities[1].public_key = reused.authorities[0].public_key.clone();
        assert!(matches!(reused.validate(), Err(AdmissionError::Invalid(_))));

        let mut unilateral = policy;
        unilateral
            .thresholds
            .insert(AdmissionAuthorityRole::AdmissionAuthority, 1);
        assert!(matches!(
            unilateral.validate(),
            Err(AdmissionError::Invalid(_))
        ));

        assert_ne!(
            keys.evidence[0].1.verifying_key(),
            keys.admission[0].1.verifying_key()
        );
    }
}
