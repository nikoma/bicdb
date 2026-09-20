//! Fail-closed construction boundary for one industry-agnostic BicDB cell.
//!
//! This crate intentionally has no dependency on the general BicDB CLI,
//! cluster orchestration, RESP, browser sync, extension loading, or the
//! application package installer. It implements the Phase 0/1/2 cell boundary
//! and the Phase-3/4/5/6/7/8 cell-native application, fleet lifecycle,
//! cell-scoped HA/recovery, filtered device-replica, cross-cell grant, and
//! hardened-fleet evidence boundary from
//! `docs/bicdb-cell-application-architecture.md`; it does **not** claim the
//! regulated-workload production admission gates are complete.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};

mod admission;
use admission::CellAdmissionLease;
pub use admission::CellAdmissionWatchdog;

use base64::Engine;
use bicdb_app_runtime::{
    AppRuntimeError, ApplicationCapability, ApplicationComponentKind, ApplicationDataClass,
    ApplicationDatabaseFeature, ApplicationExecutionScope, ApplicationHostConfig,
    ApplicationHttpRequest, ApplicationHttpResponse, ApplicationPackage, ApplicationReadiness,
    ApplicationRuntime, BoundedObservability, DenyBlobProvider, DenyEgressProvider, EgressProvider,
    FrontendAsset, HttpHostPolicy, HttpServerHandle, InMemorySecretProvider, InvocationServices,
    JwtAudience, JwtAuthenticator, JwtClaims, JwtConfiguration, LlmProvider, LlmProviderRequest,
    LlmProviderResponse, PackageVerifier, SecretRecord, TokenizerProvider, TrustedHttpHandler,
    TrustedSigningKeys,
};
use bicdb_cell_admission::{
    verify_admission_bundle, AdmissionBundle, AdmissionDigest, AdmissionSubject,
    AdmissionTrustPolicy, ApplicationMeasurement, VerifiedAdmissionEvidence,
};
use bicdb_cell_device::{
    AmendmentEvaluation, CertifiedAmendmentResolution, CertifiedDeviceAuthorization,
    CertifiedDeviceEnrollment, CertifiedDeviceRetirement, DeviceDigest, DeviceExporter, DeviceId,
    DeviceParentLedger, DeviceTrustPolicy, DeviceWorkingSetObject, ProvisionedDeviceDatabaseKey,
    SignedDeviceAmendment, SignedDeviceWorkingSet,
};
#[cfg(test)]
use bicdb_cell_device::{
    DeviceAuthorityKey, DeviceAuthorityRole, DeviceExportKey, DEVICE_TRUST_POLICY_FORMAT,
};
use bicdb_cell_grant::{
    CertifiedCrossCellGrant, CertifiedGrantRevocation, CertifiedImportReview,
    CertifiedRecipientKey, GrantDigest, GrantExporter, GrantId, GrantPlaintextObject,
    GrantRecipientLedger, GrantSourceLedger, GrantTrustPolicy, ImportedGrantEvidence,
    RecipientPrivateKey, SignedGrantPackage, SoftwareRecipientPrivateKey,
};
use bicdb_cell_ha::{
    decode_document as decode_ha_document, document_digest as ha_document_digest,
    encode_document as encode_ha_document, sign_fence_ack, sign_replication_object,
    verify_cell_backup, verify_ha_activation, verify_replica_lease, verify_replication_object,
    verify_restore_authorization, CellBackupManifest, CellHaCommitFence, CellHaState,
    CellHaStateStore, CellReplicationObject, CertifiedCellBackup, CertifiedReplicaLease,
    CertifiedRestoreAuthorization, CertifiedWriterEpoch, HaActivationContext, HaDigest, HaId,
    HaTrustPolicy, ReplicaRole, ReplicationObjectHeader, ReplicationObjectKind, SignedFenceAck,
    SystemHaClock, WriterFenceStatus, HA_STATE_FORMAT, REPLICATION_OBJECT_FORMAT,
};
use bicdb_core::{
    restore_backup, rotate_bound_database_encryption, BackupCreateOptions, BackupRestoreOptions,
    BackupRestoreReport, BicDb, BoundEncryptionRotationOptions, BoundEncryptionRotationReport,
    CommitFrame, DatabaseObjectCipher, DbConfig, EncryptionBinding, EncryptionConfig,
    EncryptionObjectPurpose, ReplicationConfig, ReplicationMode, CURRENT_FORMAT_VERSION,
    REPLICATION_PROTOCOL_VERSION,
};
use bicdb_extension::abi_v2::{
    ApplicationAuthKindV1, ApplicationLlmClientV1, EgressDeclaration, EgressResponse,
    HttpRequestBodyV2, HttpResponseBodyV2,
};
use bicdb_extension::HttpMethod;
use bicdb_fleet::{
    verify_activation_bundle, ActivationContext, ApplicationConvergence, ApplicationTarget,
    ConvergenceLedger, ConvergenceReceipt, FleetActivationBundle, FleetCellId, FleetDigest,
    FleetTrustPolicy, VerifiedFleetActivation, CONVERGENCE_RECEIPT_FORMAT,
    FLEET_TRUST_POLICY_FORMAT,
};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use parking_lot::{Mutex, RwLock};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

const MAX_SIGNED_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_FLEET_ACTIVATION_BUNDLE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SIGNED_KEY_LEASE_BYTES: usize = 64 * 1024;
const MAX_KEY_LEASE_LIFETIME_SECONDS: i64 = 300;
const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_APPLICATION_PACKAGE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CELL_ACCESS_TOKEN_LIFETIME_SECONDS: i64 = 3_600;
const MAX_CELL_IDENTITY_CLOCK_SKEW_SECONDS: i64 = 120;
const MAX_CELL_HANDOFF_LIFETIME_SECONDS: i64 = 120;
const MAX_CELL_SESSION_LIFETIME_SECONDS: i64 = 3_600;
const MANIFEST_DOMAIN: &[u8] = b"BICDB-CELL-MANIFEST-V1\0";
const VOLUME_DOMAIN: &[u8] = b"BICDB-CELL-VOLUME-V1\0";
const KEY_LEASE_DOMAIN: &[u8] = b"BICDB-CELL-KEY-LEASE-V1\0";
const CELL_STATE_FILE: &str = ".bicdb-cell-state.cbor";
const CELL_ROLLBACK_WITNESS_FILE: &str = ".bicdb-cell-rollback-witness.cbor";
const CELL_APP_RUNTIME_DIRECTORY: &str = ".bicdb-cell-app-runtime";
const CELL_HANDOFF_REPLAY_JOURNAL: &str = "handoff-replay.jsonl";
const CELL_CONVERGENCE_LEDGER_DIRECTORY: &str = ".bicdb-cell-convergence";
const CELL_HA_STATE_FILE: &str = ".bicdb-cell-ha-state.cbor";
const CELL_RUNTIME_LOCK_FILE: &str = ".bicdb-cell-runtime.lock";
const CELL_HA_BACKUP_DIRECTORY: &str = ".bicdb-cell-backups";
const CELL_HA_RESTORE_DIRECTORY: &str = ".bicdb-cell-restores";
const CELL_HA_BACKUP_KEY_FORMAT: &str = "bicdb.cell-ha-backup-key/v1";
const CELL_HA_BACKUP_PAYLOAD_FORMAT: &str = "bicdb.cell-ha-backup-payload/v1";
const MAX_CELL_BACKUP_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MAX_HANDOFF_REPLAY_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;
pub const CELL_IDENTITY_FILE: &str = ".bicdb-cell-identity.cbor";
pub const CELL_MANIFEST_FORMAT: &str = "bicdb.cell-manifest/v1";
pub const CELL_VOLUME_FORMAT: &str = "bicdb.cell-volume/v1";
pub const CELL_RELEASE_POLICY_FORMAT: &str = "bicdb.cell-release-policy/v1";
pub const CELL_IDENTITY_POLICY_FORMAT: &str = "bicdb.cell-identity-policy/v1";
pub const CELL_EGRESS_POLICY_FORMAT: &str = "bicdb.cell-egress-policy/v1";
pub const CELL_AUTHORIZATION_POLICY_FORMAT: &str = "bicdb.cell-authorization-policy/v1";
pub const CELL_FEATURE_CERTIFICATION_FORMAT: &str = "bicdb.cell-feature-certification/v1";
pub const CELL_KEY_LEASE_FORMAT: &str = "bicdb.cell-key-lease/v1";
pub const CELL_HA_CONFIGURATION_FORMAT: &str = "bicdb.cell-ha-configuration/v1";
pub const PHASE1_ENCRYPTION_PROFILE: &str = "phase1-xchacha20poly1305";
pub const PHASE2_ENCRYPTION_PROFILE: &str = "cell-bound-xchacha20poly1305-v2";
pub const PHASE1_ISOLATION_PROFILE: &str = "phase1-process-isolated";
pub const PHASE2_ISOLATION_PROFILE: &str = "phase2-process-isolated";
pub const PHASE3_ISOLATION_PROFILE: &str = "phase3-cell-native-process-isolated";
pub const PHASE4_ISOLATION_PROFILE: &str = "phase4-fleet-authorized-process-isolated";
pub const PHASE5_ISOLATION_PROFILE: &str = "phase5-cell-ha-process-isolated";
pub const PHASE6_ISOLATION_PROFILE: &str = "phase6-device-edge-process-isolated";
pub const PHASE7_ISOLATION_PROFILE: &str = "phase7-cross-cell-grants-process-isolated";
pub const PHASE8_ISOLATION_PROFILE: &str = "phase8-attested-hardened-fleet";
pub const PHASE1_SECURITY_PROFILE: &str = "phase1-development-deny-regulated";
pub const PHASE2_SECURITY_PROFILE: &str = "phase2-cryptographic-deny-regulated";
const LEGACY_PHASE1_SECURITY_PROFILE: &str = "phase1-development-deny-phi";
const LEGACY_PHASE2_SECURITY_PROFILE: &str = "phase2-cryptographic-deny-phi";
pub const PHASE3_SECURITY_PROFILE: &str = "phase3-cell-native-deny-regulated";
pub const PHASE4_SECURITY_PROFILE: &str = "phase4-fleet-lifecycle-deny-regulated";
pub const PHASE5_SECURITY_PROFILE: &str = "phase5-cell-ha-deny-regulated";
pub const PHASE6_SECURITY_PROFILE: &str = "phase6-device-edge-deny-regulated";
pub const PHASE7_SECURITY_PROFILE: &str = "phase7-cross-cell-grants-deny-regulated";
pub const PHASE8_SECURITY_PROFILE: &str = "phase8-hardened-fleet-deny-regulated";

pub type Result<T> = std::result::Result<T, CellError>;

#[derive(Debug, thiserror::Error)]
pub enum CellError {
    #[error("CELL_DOCUMENT_INVALID: {0}")]
    DocumentInvalid(String),
    #[error("CELL_SIGNATURE_INVALID: {0}")]
    SignatureInvalid(String),
    #[error("CELL_IDENTITY_MISMATCH: {0}")]
    IdentityMismatch(String),
    #[error("CELL_MANIFEST_ROLLBACK: {0}")]
    ManifestRollback(String),
    #[error("CELL_RUNTIME_MISMATCH: {0}")]
    RuntimeMismatch(String),
    #[error("CELL_KEY_SCOPE_MISMATCH: {0}")]
    KeyScopeMismatch(String),
    #[error("CELL_KEY_LEASE_INVALID: {0}")]
    KeyLeaseInvalid(String),
    #[error("CELL_ARTIFACT_MISMATCH: {0}")]
    ArtifactMismatch(String),
    #[error("CELL_STORAGE_UNSAFE: {0}")]
    StorageUnsafe(String),
    #[error("REGULATED_ADMISSION_INCOMPLETE: {0}")]
    RegulatedAdmissionIncomplete(String),
    #[error("CELL_APPLICATION: {0}")]
    Application(#[from] bicdb_app_runtime::AppRuntimeError),
    #[error("CELL_FLEET: {0}")]
    Fleet(#[from] bicdb_fleet::FleetError),
    #[error("CELL_HA: {0}")]
    Ha(#[from] bicdb_cell_ha::HaError),
    #[error("CELL_DEVICE: {0}")]
    Device(#[from] bicdb_cell_device::DeviceError),
    #[error("CELL_GRANT: {0}")]
    Grant(#[from] bicdb_cell_grant::GrantError),
    #[error("CELL_ADMISSION: {0}")]
    Admission(#[from] bicdb_cell_admission::AdmissionError),
    #[error("CELL_IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("CELL_DATABASE: {0}")]
    Database(#[from] bicdb_core::BicDbError),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct CellId(String);

impl CellId {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let parsed = Uuid::parse_str(&value)
            .map_err(|_| CellError::DocumentInvalid("cell_id must be a UUID".to_string()))?;
        if parsed.is_nil() {
            return Err(CellError::DocumentInvalid(
                "cell_id must not be the nil UUID".to_string(),
            ));
        }
        Ok(Self(parsed.hyphenated().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CellId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CellId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value != value.to_ascii_lowercase() {
            return Err(CellError::DocumentInvalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        }
        let Some(hex) = value.strip_prefix("sha256:") else {
            return Err(CellError::DocumentInvalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        };
        if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CellError::DocumentInvalid(
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

impl std::fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

pub type ExecutionScope = ApplicationExecutionScope;
pub type DataClass = ApplicationDataClass;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellStorageManifest {
    pub volume_id: String,
    pub lineage_id: String,
    pub database_relative_path: String,
    pub database_format: u32,
    pub encryption_profile: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellRuntimeManifest {
    pub bicdb_binary_digest: Sha256Digest,
    pub guest_image_digest: Sha256Digest,
    pub isolation_profile: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellApplicationPin {
    pub root: String,
    pub name: String,
    pub version: String,
    pub digest: Sha256Digest,
    pub schema_generation: u64,
    pub scope: ExecutionScope,
    pub data_class: DataClass,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellKeyManifest {
    pub authority: String,
    pub cell_kek_id: String,
    pub key_epoch: u64,
    pub key_fingerprint: Sha256Digest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellReplicationManifest {
    pub group_id: String,
    pub replica_id: String,
    pub writer_epoch: u64,
    #[serde(default, skip_serializing_if = "cell_replica_role_is_primary")]
    pub role: CellReplicaRole,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CellReplicaRole {
    #[default]
    Primary,
    Standby,
    Recovery,
}

fn cell_replica_role_is_primary(role: &CellReplicaRole) -> bool {
    *role == CellReplicaRole::Primary
}

impl From<CellReplicaRole> for ReplicaRole {
    fn from(role: CellReplicaRole) -> Self {
        match role {
            CellReplicaRole::Primary => Self::Primary,
            CellReplicaRole::Standby => Self::Standby,
            CellReplicaRole::Recovery => Self::Recovery,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellPolicyManifest {
    pub security_profile: String,
    pub egress_policy_digest: Sha256Digest,
    pub identity_policy_digest: Sha256Digest,
    pub trusted_release_policy_digest: Sha256Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization_policy_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature_certification_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fleet_trust_policy_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ha_trust_policy_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_trust_policy_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_trust_policy_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_trust_policy_digest: Option<Sha256Digest>,
}

/// Root-scoped application release keys. This policy is independently pinned
/// by the CellManifest, so package publication authority and cell activation
/// authority remain separate credentials.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellReleaseRoot {
    pub root: String,
    /// Ed25519 public keys encoded as exactly 64 lowercase hexadecimal digits.
    pub signing_keys: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellReleasePolicy {
    pub format: String,
    /// The current package format carries one release signature. Requiring one
    /// signature is explicit rather than silently pretending threshold signing
    /// exists; the separately signed CellManifest authorizes activation.
    pub required_package_signatures: u8,
    pub roots: Vec<CellReleaseRoot>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellIdentityPolicy {
    pub format: String,
    pub issuer: String,
    pub audience: String,
    pub authentication_method: String,
    pub maximum_lifetime_seconds: i64,
    pub clock_skew_seconds: i64,
    /// OIDC Ed25519 JWKS embedded in the manifest-pinned policy. Applications
    /// receive verified actor claims, never verifier keys or bearer tokens.
    pub oidc_ed25519_jwks: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CellEgressMode {
    DenyAll,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellEgressPolicy {
    pub format: String,
    pub mode: CellEgressMode,
    /// Explicit Phase-1-only bindings that let a signed application boot for
    /// integration testing while external effects remain denied. This field is
    /// rejected by every production-capable Cell profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase1_development_providers: Option<CellPhase1DevelopmentProviders>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CellDevelopmentProviderBinding {
    pub application: String,
    pub provider: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellPhase1DevelopmentProviders {
    /// Cell-derived keys available only to signed encrypt/decrypt declarations.
    #[serde(default)]
    pub derived_secrets: BTreeSet<String>,
    /// Signed HTTP providers that are present only as deny-all test bindings.
    #[serde(default)]
    pub deny_egress: BTreeSet<CellDevelopmentProviderBinding>,
    /// Deterministic local token counters used by test-only LLM flows.
    #[serde(default)]
    pub deterministic_tokenizers: BTreeSet<CellDevelopmentProviderBinding>,
    /// Signed LLM providers whose calls fail explicitly instead of preventing
    /// the rest of the application from starting.
    #[serde(default)]
    pub deny_llm: BTreeSet<CellDevelopmentProviderBinding>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellAuthorizedDevice {
    pub device_id: String,
    /// Ed25519 public key encoded as 64 lowercase hexadecimal digits.
    pub public_key: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellMemberAuthorization {
    pub user_id: String,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    pub devices: Vec<CellAuthorizedDevice>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellAuthorizationPolicy {
    pub format: String,
    pub cell_id: CellId,
    pub authorization_epoch: u64,
    pub session_lifetime_seconds: i64,
    pub minimum_assurance: String,
    pub members: Vec<CellMemberAuthorization>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellFeatureCertification {
    pub format: String,
    pub bicdb_binary_digest: Sha256Digest,
    pub database_format: u32,
    pub conformance_suite_digest: Sha256Digest,
    pub certified_features: BTreeSet<ApplicationDatabaseFeature>,
}

fn default_true() -> bool {
    true
}

pub struct CellApplicationHostConfig {
    pub release_policy_path: PathBuf,
    pub identity_policy_path: PathBuf,
    pub egress_policy_path: PathBuf,
    pub authorization_policy_path: Option<PathBuf>,
    pub feature_certification_path: Option<PathBuf>,
    /// Phase-4 trust policy and independently authorized activation bundle.
    pub fleet_trust_policy_path: Option<PathBuf>,
    pub fleet_activation_bundle_path: Option<PathBuf>,
    /// The signed predecessor is required for non-initial Phase-4 transitions
    /// so schema compatibility derives from Cell authority, not the fleet.
    pub previous_manifest_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellManifest {
    pub format: String,
    pub cell_id: CellId,
    pub manifest_generation: u64,
    pub previous_manifest_digest: Option<Sha256Digest>,
    pub jurisdiction: String,
    pub storage: CellStorageManifest,
    pub runtime: CellRuntimeManifest,
    pub applications: Vec<CellApplicationPin>,
    pub keys: CellKeyManifest,
    pub replication: CellReplicationManifest,
    pub policy: CellPolicyManifest,
}

/// The Cell-wide configuration certified by one writer epoch and shared by
/// every replica. Replica-local volume identity, replica identity/role, and
/// the local manifest predecessor chain are deliberately excluded: those are
/// independently authenticated by each signed CellManifest and key lease.
/// Everything which may affect decrypted execution or durable compatibility
/// remains in this projection, so two replicas cannot join one epoch while
/// running different applications, policies, binaries, keys, or storage
/// lineage.
#[derive(Serialize)]
struct CellHaConfiguration<'a> {
    format: &'static str,
    cell_id: &'a CellId,
    configuration_generation: u64,
    jurisdiction: &'a str,
    storage_lineage_id: &'a str,
    database_relative_path: &'a str,
    database_format: u32,
    encryption_profile: &'a str,
    runtime: &'a CellRuntimeManifest,
    applications: &'a [CellApplicationPin],
    keys: &'a CellKeyManifest,
    replication_group_id: &'a str,
    writer_epoch: u64,
    policy: &'a CellPolicyManifest,
}

fn cell_ha_configuration_digest(manifest: &CellManifest) -> Result<HaDigest> {
    let projection = CellHaConfiguration {
        format: CELL_HA_CONFIGURATION_FORMAT,
        cell_id: &manifest.cell_id,
        configuration_generation: manifest.manifest_generation,
        jurisdiction: &manifest.jurisdiction,
        storage_lineage_id: &manifest.storage.lineage_id,
        database_relative_path: &manifest.storage.database_relative_path,
        database_format: manifest.storage.database_format,
        encryption_profile: &manifest.storage.encryption_profile,
        runtime: &manifest.runtime,
        applications: &manifest.applications,
        keys: &manifest.keys,
        replication_group_id: &manifest.replication.group_id,
        writer_epoch: manifest.replication.writer_epoch,
        policy: &manifest.policy,
    };
    Ok(HaDigest::of_bytes(&canonical_cbor(&projection)?))
}

impl CellManifest {
    pub fn uses_phase2_cryptography(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE2_ISOLATION_PROFILE
            && matches!(
                self.policy.security_profile.as_str(),
                PHASE2_SECURITY_PROFILE | LEGACY_PHASE2_SECURITY_PROFILE
            )
    }

    pub fn uses_phase3_cell_native(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE3_ISOLATION_PROFILE
            && self.policy.security_profile == PHASE3_SECURITY_PROFILE
    }

    pub fn uses_phase4_fleet_lifecycle(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE4_ISOLATION_PROFILE
            && self.policy.security_profile == PHASE4_SECURITY_PROFILE
    }

    pub fn uses_phase5_cell_ha(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE5_ISOLATION_PROFILE
            && self.policy.security_profile == PHASE5_SECURITY_PROFILE
    }

    pub fn uses_phase6_device_edge(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE6_ISOLATION_PROFILE
            && self.policy.security_profile == PHASE6_SECURITY_PROFILE
    }

    pub fn uses_phase7_cross_cell_grants(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE7_ISOLATION_PROFILE
            && self.policy.security_profile == PHASE7_SECURITY_PROFILE
    }

    pub fn uses_phase8_hardened_fleet(&self) -> bool {
        self.storage.encryption_profile == PHASE2_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE8_ISOLATION_PROFILE
            && self.policy.security_profile == PHASE8_SECURITY_PROFILE
    }

    pub fn uses_cross_cell_grants(&self) -> bool {
        self.uses_phase7_cross_cell_grants() || self.uses_phase8_hardened_fleet()
    }

    pub fn uses_device_edge(&self) -> bool {
        self.uses_phase6_device_edge() || self.uses_cross_cell_grants()
    }

    pub fn uses_fleet_lifecycle(&self) -> bool {
        self.uses_phase4_fleet_lifecycle()
            || self.uses_phase5_cell_ha()
            || self.uses_phase6_device_edge()
            || self.uses_cross_cell_grants()
    }

    pub fn uses_cell_ha(&self) -> bool {
        self.uses_phase5_cell_ha()
            || self.uses_phase6_device_edge()
            || self.uses_cross_cell_grants()
    }

    pub fn uses_cell_native(&self) -> bool {
        self.uses_phase3_cell_native()
            || self.uses_phase4_fleet_lifecycle()
            || self.uses_phase5_cell_ha()
            || self.uses_phase6_device_edge()
            || self.uses_cross_cell_grants()
    }

    pub fn uses_bound_cryptography(&self) -> bool {
        self.uses_phase2_cryptography() || self.uses_cell_native()
    }

    pub fn validate(&self) -> Result<()> {
        if self.format != CELL_MANIFEST_FORMAT {
            return Err(CellError::DocumentInvalid(format!(
                "unsupported manifest format {}",
                self.format
            )));
        }
        CellId::parse(self.cell_id.0.clone())?;
        if self.manifest_generation == 0 {
            return Err(CellError::DocumentInvalid(
                "manifest_generation must be positive".to_string(),
            ));
        }
        if self.manifest_generation > 1 && self.previous_manifest_digest.is_none() {
            return Err(CellError::DocumentInvalid(
                "non-initial manifest must link previous_manifest_digest".to_string(),
            ));
        }
        if self.manifest_generation == 1 && self.previous_manifest_digest.is_some() {
            return Err(CellError::DocumentInvalid(
                "initial manifest cannot link a previous manifest".to_string(),
            ));
        }
        if self.storage.volume_id.trim().is_empty()
            || self.storage.lineage_id.trim().is_empty()
            || self.jurisdiction.trim().is_empty()
        {
            return Err(CellError::DocumentInvalid(
                "cell, volume, lineage, and jurisdiction identities must be non-empty".to_string(),
            ));
        }
        validate_database_relative_path(&self.storage.database_relative_path)?;
        if self.storage.database_format != CURRENT_FORMAT_VERSION {
            return Err(CellError::DocumentInvalid(format!(
                "Phase 1 requires BicDB database format {CURRENT_FORMAT_VERSION}"
            )));
        }
        let phase1_profile = self.storage.encryption_profile == PHASE1_ENCRYPTION_PROFILE
            && self.runtime.isolation_profile == PHASE1_ISOLATION_PROFILE
            && matches!(
                self.policy.security_profile.as_str(),
                PHASE1_SECURITY_PROFILE | LEGACY_PHASE1_SECURITY_PROFILE
            );
        if !phase1_profile && !self.uses_bound_cryptography() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the encryption, isolation, and security profiles must be one coherent supported generation"
                    .to_string(),
            ));
        }
        if self.keys.key_epoch == 0 || self.replication.writer_epoch == 0 {
            return Err(CellError::DocumentInvalid(
                "key_epoch and writer_epoch must be positive".to_string(),
            ));
        }
        if self.keys.authority.trim().is_empty()
            || self.keys.cell_kek_id.trim().is_empty()
            || self.replication.group_id.trim().is_empty()
            || self.replication.replica_id.trim().is_empty()
        {
            return Err(CellError::DocumentInvalid(
                "key and replication identities must be non-empty".to_string(),
            ));
        }
        if self.uses_bound_cryptography() && !self.keys.authority.starts_with("kms+attested://") {
            return Err(CellError::DocumentInvalid(
                "bound-cryptography key authority must use kms+attested:// and cannot name a development file provider"
                    .to_string(),
            ));
        }
        let mut app_names = BTreeSet::new();
        for application in &self.applications {
            if application.scope != ExecutionScope::Cell {
                return Err(CellError::DocumentInvalid(format!(
                    "cell manifest application {} must have cell scope",
                    application.name
                )));
            }
            if application.root.trim().is_empty()
                || application.name.trim().is_empty()
                || application.version.trim().is_empty()
                || application.schema_generation == 0
                || !app_names.insert(application.name.to_ascii_lowercase())
            {
                return Err(CellError::DocumentInvalid(
                    "application pins require unique non-empty identities and a positive schema generation"
                        .to_string(),
                ));
            }
            if application.data_class.is_regulated() && !self.uses_phase8_hardened_fleet() {
                return Err(CellError::RegulatedAdmissionIncomplete(format!(
                    "application {} requests a regulated data class outside the Phase 8 admission profile",
                    application.name
                )));
            }
        }
        if self.uses_cell_native()
            && (self.applications.is_empty()
                || self.policy.authorization_policy_digest.is_none()
                || self.policy.feature_certification_digest.is_none())
        {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "cell-native profiles require pinned applications, cell-local authorization, and binary feature certification"
                    .to_string(),
            ));
        }
        if self.uses_fleet_lifecycle() != self.policy.fleet_trust_policy_digest.is_some() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the Phase 4/5/6 profile and fleet trust-policy pin must be selected together"
                    .to_string(),
            ));
        }
        if self.uses_cell_ha() != self.policy.ha_trust_policy_digest.is_some() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the Phase 5/6 profile and HA trust-policy pin must be selected together"
                    .to_string(),
            ));
        }
        if self.uses_device_edge() != self.policy.device_trust_policy_digest.is_some() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the Phase 6 profile and device trust-policy pin must be selected together"
                    .to_string(),
            ));
        }
        if self.uses_cross_cell_grants() != self.policy.grant_trust_policy_digest.is_some() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the Phase 7/8 profile and grant trust-policy pin must be selected together"
                    .to_string(),
            ));
        }
        if self.uses_cross_cell_grants() && self.policy.device_trust_policy_digest.is_none() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the cumulative Phase 7/8 profile requires the Phase 6 device policy pin"
                    .to_string(),
            ));
        }
        if self.uses_phase8_hardened_fleet() != self.policy.admission_trust_policy_digest.is_some()
        {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the Phase 8 profile and admission trust-policy pin must be selected together"
                    .to_string(),
            ));
        }
        if self.uses_phase8_hardened_fleet() && self.policy.grant_trust_policy_digest.is_none() {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "the cumulative Phase 8 profile requires the Phase 7 grant policy pin".to_string(),
            ));
        }
        if self.uses_cell_ha() {
            HaId::parse(self.replication.replica_id.clone(), "replica_id")?;
        } else if self.replication.role != CellReplicaRole::Primary {
            return Err(CellError::DocumentInvalid(
                "standby/recovery replica roles require the Phase 5 HA profile".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedCellDocument {
    pub envelope_format: String,
    pub signer_key_id: String,
    pub payload_sha256: Sha256Digest,
    pub payload_cbor: Vec<u8>,
    pub signature: Vec<u8>,
}

impl Drop for SignedCellDocument {
    fn drop(&mut self) {
        // Most signed payloads are public metadata, but the same strict
        // envelope codec carries the one-shot key lease. Always erase the
        // decoded payload buffer so an error path cannot leave key bytes in a
        // reusable allocation.
        self.payload_cbor.zeroize();
    }
}

#[derive(Clone, Debug)]
pub struct VerifiedCellManifest {
    pub manifest: CellManifest,
    pub digest: Sha256Digest,
    pub signer_key_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellVolumeIdentity {
    pub format: String,
    pub cell_id: CellId,
    pub volume_id: String,
    pub lineage_id: String,
    pub database_relative_path: String,
    pub initial_manifest_digest: Sha256Digest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CellLocalState {
    format: String,
    cell_id: CellId,
    volume_id: String,
    highest_manifest_generation: u64,
    current_manifest_digest: Sha256Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    convergence_receipt_digest: Option<Sha256Digest>,
}

pub type TrustedManifestKeys = BTreeMap<String, VerifyingKey>;

fn canonical_cbor<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes).map_err(|error| {
        CellError::DocumentInvalid(format!("encode deterministic CBOR: {error}"))
    })?;
    Ok(bytes)
}

fn decode_canonical_cbor<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let value: T = ciborium::de::from_reader(bytes)
        .map_err(|error| CellError::DocumentInvalid(format!("decode CBOR: {error}")))?;
    let encoded = canonical_cbor(&value)?;
    if encoded != bytes {
        return Err(CellError::DocumentInvalid(
            "CBOR document is not in BicDB deterministic encoding".to_string(),
        ));
    }
    Ok(value)
}

fn signature_message(domain: &[u8], signer_key_id: &str, payload: &[u8]) -> Vec<u8> {
    let signer_key_id = signer_key_id.as_bytes();
    let signer_key_id_len = u64::try_from(signer_key_id.len()).unwrap_or(u64::MAX);
    let mut message = Vec::with_capacity(domain.len() + 8 + signer_key_id.len() + payload.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(&signer_key_id_len.to_be_bytes());
    message.extend_from_slice(signer_key_id);
    message.extend_from_slice(payload);
    message
}

fn validate_signer_key_id(signer_key_id: &str) -> Result<()> {
    if signer_key_id.is_empty()
        || signer_key_id.len() > 128
        || !signer_key_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(CellError::DocumentInvalid(
            "signer_key_id must be 1..128 ASCII identifier characters".to_string(),
        ));
    }
    Ok(())
}

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn sign_payload<T: Serialize>(
    envelope_format: &str,
    domain: &[u8],
    payload: &T,
    signer_key_id: &str,
    signing_key: &SigningKey,
) -> Result<Vec<u8>> {
    validate_signer_key_id(signer_key_id)?;
    let payload_cbor = canonical_cbor(payload)?;
    let signature = signing_key
        .sign(&signature_message(domain, signer_key_id, &payload_cbor))
        .to_bytes()
        .to_vec();
    canonical_cbor(&SignedCellDocument {
        envelope_format: envelope_format.to_string(),
        signer_key_id: signer_key_id.to_string(),
        payload_sha256: Sha256Digest::of_bytes(&payload_cbor),
        payload_cbor,
        signature,
    })
}

fn verify_payload<T: Serialize + DeserializeOwned>(
    bytes: &[u8],
    envelope_format: &str,
    domain: &[u8],
    trusted_keys: &TrustedManifestKeys,
) -> Result<(T, Sha256Digest, String)> {
    let envelope: SignedCellDocument = decode_canonical_cbor(bytes)?;
    if envelope.envelope_format != envelope_format {
        return Err(CellError::DocumentInvalid(format!(
            "unexpected signed document format {}",
            envelope.envelope_format
        )));
    }
    validate_signer_key_id(&envelope.signer_key_id)?;
    if Sha256Digest::of_bytes(&envelope.payload_cbor) != envelope.payload_sha256 {
        return Err(CellError::SignatureInvalid(
            "payload digest does not match envelope".to_string(),
        ));
    }
    let key = trusted_keys.get(&envelope.signer_key_id).ok_or_else(|| {
        CellError::SignatureInvalid(format!("signer {} is not trusted", envelope.signer_key_id))
    })?;
    let signature = Signature::from_slice(&envelope.signature)
        .map_err(|_| CellError::SignatureInvalid("invalid Ed25519 signature length".to_string()))?;
    key.verify(
        &signature_message(domain, &envelope.signer_key_id, &envelope.payload_cbor),
        &signature,
    )
    .map_err(|_| CellError::SignatureInvalid("Ed25519 verification failed".to_string()))?;
    let payload = decode_canonical_cbor(&envelope.payload_cbor)?;
    Ok((
        payload,
        envelope.payload_sha256.clone(),
        envelope.signer_key_id.clone(),
    ))
}

pub fn sign_manifest(
    manifest: &CellManifest,
    signer_key_id: &str,
    signing_key: &SigningKey,
) -> Result<Vec<u8>> {
    manifest.validate()?;
    sign_payload(
        CELL_MANIFEST_FORMAT,
        MANIFEST_DOMAIN,
        manifest,
        signer_key_id,
        signing_key,
    )
}

pub fn verify_manifest_bytes(
    bytes: &[u8],
    trusted_keys: &TrustedManifestKeys,
) -> Result<VerifiedCellManifest> {
    if bytes.len() > MAX_SIGNED_DOCUMENT_BYTES {
        return Err(CellError::DocumentInvalid(
            "signed manifest exceeds 1 MiB".to_string(),
        ));
    }
    let (manifest, digest, signer_key_id) =
        verify_payload(bytes, CELL_MANIFEST_FORMAT, MANIFEST_DOMAIN, trusted_keys)?;
    let manifest: CellManifest = manifest;
    manifest.validate()?;
    Ok(VerifiedCellManifest {
        manifest,
        digest,
        signer_key_id,
    })
}

pub fn load_verified_manifest(
    path: &Path,
    trusted_keys: &TrustedManifestKeys,
) -> Result<VerifiedCellManifest> {
    let bytes = read_bounded_regular_file(path, MAX_SIGNED_DOCUMENT_BYTES as u64, false)?;
    verify_manifest_bytes(&bytes, trusted_keys)
}

pub fn load_manifest_json(path: &Path) -> Result<CellManifest> {
    let bytes = read_bounded_regular_file(path, MAX_SIGNED_DOCUMENT_BYTES as u64, false)?;
    let manifest: CellManifest = serde_json::from_slice(&bytes)
        .map_err(|error| CellError::DocumentInvalid(format!("parse manifest JSON: {error}")))?;
    manifest.validate()?;
    Ok(manifest)
}

#[derive(Clone, Debug)]
pub struct CellKeyRequest<'a> {
    pub cell_id: &'a CellId,
    pub manifest_digest: &'a Sha256Digest,
    pub manifest_generation: u64,
    pub volume_id: &'a str,
    pub lineage_id: &'a str,
    pub bicdb_binary_digest: &'a Sha256Digest,
    pub guest_image_digest: &'a Sha256Digest,
    pub encryption_profile: &'a str,
    pub security_profile: &'a str,
    pub authority: &'a str,
    pub cell_kek_id: &'a str,
    pub key_epoch: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellKeyLease {
    pub format: String,
    pub lease_id: String,
    pub issued_at: i64,
    pub not_before: i64,
    pub expires_at: i64,
    /// Strictly increasing value maintained by an external, non-volume
    /// authority. A restored volume cannot make this counter go backwards at
    /// the signer that authorizes a fresh lease.
    pub previous_rollback_counter: u64,
    pub rollback_counter: u64,
    pub attestation_nonce: String,
    pub cell_id: CellId,
    pub manifest_digest: Sha256Digest,
    pub manifest_generation: u64,
    pub volume_id: String,
    pub lineage_id: String,
    pub bicdb_binary_digest: Sha256Digest,
    pub guest_image_digest: Sha256Digest,
    pub encryption_profile: String,
    pub security_profile: String,
    pub authority: String,
    pub cell_kek_id: String,
    pub key_epoch: u64,
    pub key_fingerprint: Sha256Digest,
    pub key: [u8; 32],
}

impl std::fmt::Debug for CellKeyLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CellKeyLease")
            .field("lease_id", &self.lease_id)
            .field("cell_id", &self.cell_id)
            .field("manifest_digest", &self.manifest_digest)
            .field("manifest_generation", &self.manifest_generation)
            .field("key_epoch", &self.key_epoch)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl Drop for CellKeyLease {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl CellKeyLease {
    pub fn validate(&self) -> Result<()> {
        if self.format != CELL_KEY_LEASE_FORMAT {
            return Err(CellError::KeyLeaseInvalid(format!(
                "unsupported key lease format {}",
                self.format
            )));
        }
        let lease_id = Uuid::parse_str(&self.lease_id)
            .map_err(|_| CellError::KeyLeaseInvalid("lease_id must be a UUID".to_string()))?;
        if lease_id.is_nil() {
            return Err(CellError::KeyLeaseInvalid(
                "lease_id must not be nil".to_string(),
            ));
        }
        if self.manifest_generation == 0 || self.key_epoch == 0 {
            return Err(CellError::KeyLeaseInvalid(
                "manifest_generation and key_epoch must be positive".to_string(),
            ));
        }
        if self.rollback_counter == 0 {
            return Err(CellError::KeyLeaseInvalid(
                "rollback_counter must be positive".to_string(),
            ));
        }
        if self.previous_rollback_counter.checked_add(1) != Some(self.rollback_counter) {
            return Err(CellError::KeyLeaseInvalid(
                "rollback_counter must be the exact successor of previous_rollback_counter"
                    .to_string(),
            ));
        }
        if self.not_before < self.issued_at
            || self.expires_at <= self.not_before
            || self.expires_at.saturating_sub(self.not_before) > MAX_KEY_LEASE_LIFETIME_SECONDS
        {
            return Err(CellError::KeyLeaseInvalid(
                "key lease lifetime is invalid or exceeds five minutes".to_string(),
            ));
        }
        if self.attestation_nonce.len() != 64
            || !self
                .attestation_nonce
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(CellError::KeyLeaseInvalid(
                "attestation_nonce must be 64 lowercase hexadecimal digits".to_string(),
            ));
        }
        if !self.authority.starts_with("kms+attested://")
            || self.cell_kek_id.trim().is_empty()
            || self.volume_id.trim().is_empty()
            || self.lineage_id.trim().is_empty()
            || self.encryption_profile.trim().is_empty()
            || self.security_profile.trim().is_empty()
        {
            return Err(CellError::KeyLeaseInvalid(
                "key lease scope contains an empty or non-attested authority".to_string(),
            ));
        }
        if Sha256Digest::of_bytes(&self.key) != self.key_fingerprint {
            return Err(CellError::KeyLeaseInvalid(
                "key lease fingerprint does not match key material".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn sign_cell_key_lease(
    lease: &CellKeyLease,
    signer_key_id: &str,
    signing_key: &SigningKey,
) -> Result<Vec<u8>> {
    lease.validate()?;
    sign_payload(
        CELL_KEY_LEASE_FORMAT,
        KEY_LEASE_DOMAIN,
        lease,
        signer_key_id,
        signing_key,
    )
}

/// One-shot, vendor-neutral KMS/HSM key delivery. The provider owns an
/// inherited anonymous pipe/socket reader, consumes it only after all manifest,
/// volume, runtime, and artifact checks have succeeded, and verifies a short-
/// lived signed lease against the exact workload identity before returning one
/// cell key. It carries no KMS credential and cannot request another cell.
pub struct AttestedKeyLeaseCellKeyProvider {
    reader: Mutex<Option<Box<dyn Read + Send>>>,
    trusted_kms_keys: TrustedManifestKeys,
    expected_attestation_nonce: String,
}

/// Open a pre-authorized key-lease stream inherited from the workload
/// launcher. BicDB duplicates the descriptor atomically with close-on-exec and
/// validates that duplicate, avoiding a `/proc` reopen race. The descriptor
/// must name a pipe, not a regular file, terminal, or device. The signed lease
/// remains the authorization boundary, while this restriction prevents the
/// production construction path from quietly becoming another key-file option.
#[cfg(target_os = "linux")]
pub fn open_inherited_key_lease_reader(fd: i32) -> Result<File> {
    use std::os::fd::FromRawFd;
    use std::os::unix::fs::FileTypeExt;

    if fd <= libc::STDERR_FILENO {
        return Err(CellError::KeyLeaseInvalid(
            "key lease descriptor must be greater than stderr".to_string(),
        ));
    }
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
    if duplicated < 0 {
        return Err(CellError::KeyLeaseInvalid(format!(
            "key lease descriptor {fd} is unavailable: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: F_DUPFD_CLOEXEC returned a new owned descriptor on success.
    let file = unsafe { File::from_raw_fd(duplicated) };
    let metadata = file.metadata()?;
    if !metadata.file_type().is_fifo() {
        return Err(CellError::KeyLeaseInvalid(
            "key lease descriptor must be an inherited pipe, never a regular file or terminal"
                .to_string(),
        ));
    }
    Ok(file)
}

#[cfg(not(target_os = "linux"))]
pub fn open_inherited_key_lease_reader(_fd: i32) -> Result<File> {
    Err(CellError::KeyLeaseInvalid(
        "attested inherited key leases currently require Linux".to_string(),
    ))
}

impl AttestedKeyLeaseCellKeyProvider {
    pub fn new(
        reader: impl Read + Send + 'static,
        trusted_kms_keys: TrustedManifestKeys,
        expected_attestation_nonce: impl Into<String>,
    ) -> Result<Self> {
        if trusted_kms_keys.is_empty() {
            return Err(CellError::KeyLeaseInvalid(
                "at least one independent KMS lease signing key is required".to_string(),
            ));
        }
        let expected_attestation_nonce = expected_attestation_nonce.into();
        if expected_attestation_nonce.len() != 64
            || !expected_attestation_nonce
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(CellError::KeyLeaseInvalid(
                "expected attestation nonce must be 64 lowercase hexadecimal digits".to_string(),
            ));
        }
        Ok(Self {
            reader: Mutex::new(Some(Box::new(reader))),
            trusted_kms_keys,
            expected_attestation_nonce,
        })
    }

    fn consume_verified_lease(
        &self,
        request: &CellKeyRequest<'_>,
    ) -> Result<(CellKeyLease, Sha256Digest, String)> {
        let mut reader = self.reader.lock().take().ok_or_else(|| {
            CellError::KeyLeaseInvalid(
                "key lease stream was already consumed; leases are one-shot".to_string(),
            )
        })?;
        let mut bytes = Zeroizing::new(Vec::new());
        reader
            .by_ref()
            .take((MAX_SIGNED_KEY_LEASE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.is_empty() || bytes.len() > MAX_SIGNED_KEY_LEASE_BYTES {
            return Err(CellError::KeyLeaseInvalid(
                "signed key lease is empty or exceeds 64 KiB".to_string(),
            ));
        }
        let (lease, lease_digest, signer_key_id): (CellKeyLease, _, _) = verify_payload(
            &bytes,
            CELL_KEY_LEASE_FORMAT,
            KEY_LEASE_DOMAIN,
            &self.trusted_kms_keys,
        )?;
        lease.validate()?;
        let now = current_unix_timestamp();
        if now < lease.not_before || now > lease.expires_at {
            return Err(CellError::KeyLeaseInvalid(
                "key lease is not currently valid".to_string(),
            ));
        }
        let exact_scope = lease.attestation_nonce == self.expected_attestation_nonce
            && &lease.cell_id == request.cell_id
            && &lease.manifest_digest == request.manifest_digest
            && lease.manifest_generation == request.manifest_generation
            && lease.volume_id == request.volume_id
            && lease.lineage_id == request.lineage_id
            && &lease.bicdb_binary_digest == request.bicdb_binary_digest
            && &lease.guest_image_digest == request.guest_image_digest
            && lease.encryption_profile == request.encryption_profile
            && lease.security_profile == request.security_profile
            && lease.authority == request.authority
            && lease.cell_kek_id == request.cell_kek_id
            && lease.key_epoch == request.key_epoch;
        if !exact_scope {
            return Err(CellError::KeyScopeMismatch(
                "signed key lease does not match the exact cell workload identity".to_string(),
            ));
        }
        Ok((lease, lease_digest, signer_key_id))
    }
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct CellKeyMaterial {
    key: [u8; 32],
    #[zeroize(skip)]
    rollback_evidence: Option<CellRollbackEvidence>,
}

impl CellKeyMaterial {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            key: bytes,
            rollback_evidence: None,
        }
    }

    fn fingerprint(&self) -> Sha256Digest {
        Sha256Digest::of_bytes(&self.key)
    }

    fn clone_bytes(&self) -> Vec<u8> {
        self.key.to_vec()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CellRollbackEvidence {
    cell_id: CellId,
    volume_id: String,
    lineage_id: String,
    lease_id: String,
    lease_digest: Sha256Digest,
    signer_key_id: String,
    previous_rollback_counter: u64,
    rollback_counter: u64,
    manifest_digest: Sha256Digest,
    manifest_generation: u64,
    key_epoch: u64,
    issued_at: i64,
    expires_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellKeyProviderAssurance {
    DevelopmentFile,
    AttestedLease,
}

pub trait CellKeyProvider: Send + Sync {
    fn unwrap_cell_key(&self, request: CellKeyRequest<'_>) -> Result<CellKeyMaterial>;
    fn provider_kind(&self) -> &'static str;
    fn assurance(&self) -> CellKeyProviderAssurance;
}

impl CellKeyProvider for AttestedKeyLeaseCellKeyProvider {
    fn unwrap_cell_key(&self, request: CellKeyRequest<'_>) -> Result<CellKeyMaterial> {
        let (lease, lease_digest, signer_key_id) = self.consume_verified_lease(&request)?;
        Ok(CellKeyMaterial {
            key: lease.key,
            rollback_evidence: Some(CellRollbackEvidence {
                cell_id: lease.cell_id.clone(),
                volume_id: lease.volume_id.clone(),
                lineage_id: lease.lineage_id.clone(),
                lease_id: lease.lease_id.clone(),
                lease_digest,
                signer_key_id,
                previous_rollback_counter: lease.previous_rollback_counter,
                rollback_counter: lease.rollback_counter,
                manifest_digest: lease.manifest_digest.clone(),
                manifest_generation: lease.manifest_generation,
                key_epoch: lease.key_epoch,
                issued_at: lease.issued_at,
                expires_at: lease.expires_at,
            }),
        })
    }

    fn provider_kind(&self) -> &'static str {
        "attested-signed-inherited-fd"
    }

    fn assurance(&self) -> CellKeyProviderAssurance {
        CellKeyProviderAssurance::AttestedLease
    }
}

/// Development-only fixed-file provider. It deliberately has no cell-id
/// parameter under caller control: one provider instance can return one key.
pub struct DevelopmentFileCellKeyProvider {
    path: PathBuf,
    bound_cell_id: CellId,
    bound_kek_id: String,
}

impl DevelopmentFileCellKeyProvider {
    pub fn new(path: PathBuf, bound_cell_id: CellId, bound_kek_id: String) -> Self {
        Self {
            path,
            bound_cell_id,
            bound_kek_id,
        }
    }
}

impl CellKeyProvider for DevelopmentFileCellKeyProvider {
    fn unwrap_cell_key(&self, request: CellKeyRequest<'_>) -> Result<CellKeyMaterial> {
        if request.cell_id != &self.bound_cell_id
            || request.authority != "file://development-fixed"
            || request.cell_kek_id != self.bound_kek_id
        {
            return Err(CellError::KeyScopeMismatch(
                "provider is not bound to requested cell/key identity".to_string(),
            ));
        }
        let bytes = Zeroizing::new(read_bounded_regular_file(&self.path, 128, true)?);
        let mut key = [0_u8; 32];
        if bytes.len() == 32 {
            key.copy_from_slice(&bytes);
        } else {
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| CellError::KeyScopeMismatch("key file is not raw or hex".to_string()))?
                .trim();
            let decoded = Zeroizing::new(hex::decode(text).map_err(|_| {
                CellError::KeyScopeMismatch("key file is not raw or hex".to_string())
            })?);
            if decoded.len() != 32 {
                return Err(CellError::KeyScopeMismatch(
                    "cell key must contain exactly 32 bytes".to_string(),
                ));
            }
            key.copy_from_slice(&decoded);
        }
        let material = CellKeyMaterial::from_bytes(key);
        key.zeroize();
        Ok(material)
    }

    fn provider_kind(&self) -> &'static str {
        "development-fixed-file"
    }

    fn assurance(&self) -> CellKeyProviderAssurance {
        CellKeyProviderAssurance::DevelopmentFile
    }
}

pub struct CellRuntimeConfig {
    pub manifest_path: PathBuf,
    pub volume_path: PathBuf,
    pub expected_cell_id: CellId,
    pub expected_volume_id: String,
    pub expected_guest_image_digest: Sha256Digest,
    pub artifact_root: PathBuf,
    pub trusted_manifest_keys: TrustedManifestKeys,
    pub key_provider: Box<dyn CellKeyProvider>,
    /// Required when the signed manifest pins one or more cell applications.
    /// The three policy files are content-bound by the CellManifest.
    pub application_host: Option<CellApplicationHostConfig>,
    /// Phase-5 quorum policy and the exact short-lived replica authority.
    /// These documents are ciphertext-free and are verified before Cell key
    /// release; database state is matched immediately after encrypted open.
    pub ha: Option<CellHaRuntimeConfig>,
    /// Phase-6 device trust policy and Cell-local export signer. The signer
    /// can encrypt filtered working sets to certified devices but has no Cell
    /// key-unwrapping authority.
    pub device: Option<CellDeviceRuntimeConfig>,
    /// Phase-7 grant policy plus workload-local export and recipient keys.
    /// These keys can sign bounded ciphertext or open envelopes addressed to
    /// this Cell; neither can unwrap a Cell database key.
    pub grant: Option<CellGrantRuntimeConfig>,
    /// Phase-8 independently signed evidence and exact deployment
    /// attestation. Both are verified against the manifest-pinned policy and
    /// running subject before the one-shot Cell key lease is consumed.
    pub admission: Option<CellAdmissionRuntimeConfig>,
}

pub struct CellHaRuntimeConfig {
    pub trust_policy_path: PathBuf,
    pub writer_epoch_path: PathBuf,
    pub replica_lease_path: PathBuf,
    pub previous_writer_epoch_path: Option<PathBuf>,
    pub previous_primary_lease_path: Option<PathBuf>,
    /// Workload-local key matching `replica_public_key` in the certified
    /// short-lived lease. It authenticates replication objects and graceful
    /// fence receipts; it has no Cell decryption authority.
    pub replica_signing_key_path: PathBuf,
}

pub struct CellDeviceRuntimeConfig {
    pub trust_policy_path: PathBuf,
    pub exporter_key_id: String,
    pub exporter_signing_key_path: PathBuf,
}

pub struct CellGrantRuntimeConfig {
    pub trust_policy_path: PathBuf,
    pub exporter_key_id: String,
    pub exporter_signing_key_path: PathBuf,
    pub recipient_key_id: String,
    pub recipient_private_key_path: PathBuf,
}

pub struct CellAdmissionRuntimeConfig {
    pub trust_policy_path: PathBuf,
    pub evidence_bundle_path: PathBuf,
    /// The actual deployment tier selected by the launcher. The attestation,
    /// policy allowlist, and every gate certificate must all bind this exact
    /// value; the environment/CLI string alone grants no authority.
    pub expected_deployment_isolation_tier: String,
}

pub struct CellKeyRotationConfig {
    pub current_manifest_path: PathBuf,
    pub next_manifest_path: PathBuf,
    pub volume_path: PathBuf,
    pub expected_cell_id: CellId,
    pub expected_volume_id: String,
    pub expected_guest_image_digest: Sha256Digest,
    pub trusted_manifest_keys: TrustedManifestKeys,
    pub current_key_provider: Box<dyn CellKeyProvider>,
    pub next_key_provider: Box<dyn CellKeyProvider>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CellKeyRotationReport {
    pub cell_id: CellId,
    pub volume_id: String,
    pub previous_manifest_digest: Sha256Digest,
    pub active_manifest_digest: Sha256Digest,
    pub previous_manifest_generation: u64,
    pub active_manifest_generation: u64,
    pub storage: BoundEncryptionRotationReport,
    pub regulated_data_admitted: bool,
}

fn key_request_for_manifest(manifest: &VerifiedCellManifest) -> CellKeyRequest<'_> {
    CellKeyRequest {
        cell_id: &manifest.manifest.cell_id,
        manifest_digest: &manifest.digest,
        manifest_generation: manifest.manifest.manifest_generation,
        volume_id: &manifest.manifest.storage.volume_id,
        lineage_id: &manifest.manifest.storage.lineage_id,
        bicdb_binary_digest: &manifest.manifest.runtime.bicdb_binary_digest,
        guest_image_digest: &manifest.manifest.runtime.guest_image_digest,
        encryption_profile: &manifest.manifest.storage.encryption_profile,
        security_profile: &manifest.manifest.policy.security_profile,
        authority: &manifest.manifest.keys.authority,
        cell_kek_id: &manifest.manifest.keys.cell_kek_id,
        key_epoch: manifest.manifest.keys.key_epoch,
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CellAdmissionReport {
    pub phase: &'static str,
    pub regulated_data_admitted: bool,
    pub enforced: Vec<&'static str>,
    pub missing: Vec<&'static str>,
}

impl CellAdmissionReport {
    pub fn phase1() -> Self {
        Self {
            phase: "phase1-single-cell",
            regulated_data_admitted: false,
            enforced: vec![
                "signed deterministic manifest",
                "one expected cell and volume identity",
                "runtime and application digest pins",
                "fixed cell-scoped key-provider request",
                "encrypted single-database open",
                "monotonic local manifest transition",
                "no public listener or database selector",
            ],
            missing: vec![
                "attested KMS/HSM key release",
                "complete durable and transient encryption proof",
                "external rollback root",
                "attested microVM/process isolation",
                "cell-native signed application execution",
                "cell-scoped HA and replication fencing",
                "independent regulated-workload security certification",
            ],
        }
    }

    pub fn application_host_preview() -> Self {
        Self {
            phase: "phase1-plus-cell-application-preview",
            regulated_data_admitted: false,
            enforced: vec![
                "signed deterministic manifest",
                "one expected cell and volume identity",
                "runtime and application digest pins",
                "separate manifest and application release trust",
                "signed package identity, version, scope, and schema generation",
                "manifest-pinned OIDC identity policy",
                "manifest-pinned deny-all egress policy",
                "cell-local application activation and migrations",
                "authenticated cell-local HTTP application host",
                "fixed cell-scoped key-provider request",
                "encrypted single-database open",
                "monotonic local manifest transition",
                "no pgwire, database selector, or cross-cell handle",
            ],
            missing: vec![
                "attested KMS/HSM key release",
                "complete durable and transient encryption proof",
                "encrypted cell blob provider",
                "externally authorized egress providers",
                "external rollback root",
                "attested microVM/process isolation",
                "threshold release signatures and fleet activation receipts",
                "cell-scoped HA and replication fencing",
                "independent regulated-workload security certification",
            ],
        }
    }

    pub fn phase2_cryptographic_cell() -> Self {
        Self {
            phase: "phase2-cryptographic-cell",
            regulated_data_admitted: false,
            enforced: vec![
                "signed deterministic manifest",
                "one expected cell and volume identity",
                "runtime and application digest pins",
                "attested signed one-cell key lease",
                "cell/profile/key-epoch bound encryption metadata",
                "HKDF-separated purpose and per-object keys",
                "cell, purpose, epoch, and object identity in AEAD context",
                "encrypted single-database open",
                "encrypted records, WAL, sync, audit, spills, blobs, catalogs, indexes, and search sidecars",
                "exact-predecessor external rollback witness",
                "crash-resumable two-lease offline key rotation",
                "monotonic local manifest transition",
                "no public pgwire listener or database selector",
            ],
            missing: vec![
                "production KMS/HSM IAM and attestation evidence",
                "data-commit external rollback root",
                "deployment-level durable and transient path certification",
                "attested microVM/process isolation",
                "regulated-workload authorization certification",
                "cell-scoped HA, backup, restore, and replication fencing",
                "independent cryptographic and security review",
            ],
        }
    }

    pub fn phase3_cell_native() -> Self {
        Self {
            phase: "phase3-cell-native-application",
            regulated_data_admitted: false,
            enforced: vec![
                "Phase 2 cell-bound cryptography and identity",
                "signed backend modules and same-origin frontend bytes",
                "signed cell scope, data class, capability, and egress contracts",
                "manifest-pinned cell-local membership and device keys",
                "device-proof-bound one-time identity handoff",
                "cell-key and authorization-epoch bound host-only sessions",
                "raw bearer and cookie credentials withheld from guest code",
                "exact-binary RLS/function/trigger/job feature certification",
                "no ambient raw SQL, blobs, egress, secrets, pgwire, or cross-cell handles",
            ],
            missing: vec![
                "threshold fleet release activation and transparency",
                "cell-scoped HA, backup, restore, and replication fencing",
                "hardware-bound offline device replicas",
                "recipient-encrypted cross-cell grants",
                "attested microVM enforcement and independent regulated-workload review",
            ],
        }
    }

    pub fn phase4_fleet_lifecycle() -> Self {
        Self {
            phase: "phase4-app-root-fleet-lifecycle",
            regulated_data_admitted: false,
            enforced: vec![
                "Phase 3 cell-native application and authorization boundary",
                "fleet layer has no database, SQL, runtime, or key-provider dependency",
                "immutable content-addressed App Root artifacts",
                "distinct publisher, security, builder, transparency, and rollout keys",
                "publisher, security, and two-builder release threshold",
                "signed hash-chain transparency checkpoint",
                "bounded explicit cohorts with independently approved observation gates",
                "per-Cell activation ticket bound to exact manifest transition",
                "runtime and schema compatibility plus minimum-safe wake admission",
                "fsynced Cell-local convergence receipts anchored in manifest state",
            ],
            missing: vec![
                "cell-scoped HA, backup, restore, and replication fencing",
                "hardware-bound offline device replicas",
                "recipient-encrypted cross-cell grants",
                "attested microVM enforcement and independent regulated-workload review",
            ],
        }
    }

    pub fn phase5_cell_ha() -> Self {
        Self {
            phase: "phase5-cell-scoped-ha-dr",
            regulated_data_admitted: false,
            enforced: vec![
                "Phase 4 Cell application and fleet lifecycle boundary",
                "independently pinned Lease, Recovery, and Auditor authorities",
                "short-lived quorum-certified replica leases",
                "writer fencing at the durable database commit boundary",
                "fsynced Cell/replica/writer-epoch/commit anti-rollback state",
                "non-overlapping writer epochs with graceful fence or lease expiry",
                "asynchronous replication has an explicit declared RPO and auditor-measured drills; continuous commit-time RPO enforcement and unsupported zero-RPO claims are refused",
                "cell, group, source, destination, key epoch, and sequence bound replication AEAD",
                "keyless failover ordering before route publication",
                "Cell-scoped encrypted backup lineage and quorum-authorized restore",
                "auditor verification for signed topology-specific RPO/RTO drill evidence",
            ],
            missing: vec![
                "zero-RPO synchronous Cell commit replication",
                "continuous commit-time RPO enforcement, production transport and placement, and externally archived HA drills",
                "hardware-bound offline device replicas",
                "recipient-encrypted cross-cell grants",
                "attested microVM enforcement and independent regulated-workload review",
            ],
        }
    }

    pub fn phase6_device_edge() -> Self {
        Self {
            phase: "phase6-hardware-bound-device-edge",
            regulated_data_admitted: false,
            enforced: vec![
                "Phase 5 Cell application, fleet, HA, backup, and recovery boundary",
                "disjoint threshold Enrollment, Authorization, Resolution, and Retirement authorities",
                "certified hardware-bound encryption/signing keys, user presence, and rollback-resistant clock contract",
                "device-unique encrypted database key; the parent Cell key is never exported",
                "exact bounded object working sets with signed tombstones and no wildcard filter language",
                "encrypted device database bound to Cell, device, storage profile, and key epoch",
                "bounded offline authorization and local reauthentication expiry",
                "immutable hardware-signed causal amendment chains without wall-clock last-writer-wins",
                "parent clean/conflict classification and threshold-certified resolution",
                "retirement refusal at the parent and cooperative hardware-key destruction",
                "parent device ledger inherits Cell encryption, commit fencing, replication, backup, and recovery",
            ],
            missing: vec![
                "production TPM/Secure Enclave/StrongBox implementation and attestation evidence",
                "proof that previously decrypted plaintext was not copied before device retirement",
                "recipient-encrypted cross-cell grants",
                "attested microVM enforcement and independent regulated-workload review",
            ],
        }
    }

    pub fn phase7_cross_cell_grants() -> Self {
        Self {
            phase: "phase7-cross-cell-object-grants",
            regulated_data_admitted: false,
            enforced: vec![
                "Phase 6 Cell, fleet, HA, recovery, and device boundary",
                "disjoint threshold Issue, Revocation, RecipientKey, and ImportAcceptance authorities",
                "mutually pinned source and recipient Cell trust anchors",
                "exact object, purpose, duration, application, schema, category, media, and redisclosure scope",
                "independent random per-object DEKs; no source Cell key crosses the boundary",
                "RFC 9180 X25519/HKDF-SHA256/ChaCha20-Poly1305 recipient key envelopes",
                "signed deterministic bounded non-executable ciphertext packages",
                "strict package and grant predecessor chains with replay refusal",
                "recipient content-inspection evidence and threshold import acceptance",
                "immutable external-source provenance in the recipient encrypted Cell database",
                "honest revocation: future delivery stops, previously decrypted copies remain",
                "no source database handle, SQL, WASM, migration, provider, filesystem, or network reference in the import format",
            ],
            missing: vec![
                "independent review of the HPKE implementation and complete protocol fuzzing",
                "production HSM/attested recipient-key implementation and opaque relay evidence",
                "attested microVM enforcement and independent regulated-workload review",
            ],
        }
    }

    pub fn phase8_hardened_fleet(regulated_data_admitted: bool) -> Self {
        Self {
            phase: "phase8-hardened-fleet-admission-evidence",
            regulated_data_admitted,
            enforced: vec![
                "Phase 7 Cell, fleet, HA, device, and object-grant boundary",
                "database-free admission verifier with no Cell or key-provider capability",
                "exact twelve production gates in canonical order with no omission or duplication",
                "build, guest image, Cell, manifest, application, schema, security profile, and isolation-tier binding",
                "disjoint threshold EvidenceReviewer, DeploymentAttestor, TransparencyWitness, and AdmissionAuthority roles",
                "failure-free, skip-free test summaries plus immutable artifact and provenance digests",
                "database, cryptography, platform, regulated-data safety, privacy, and operations review disciplines",
                "short-lived attestation bound to workload identity, KMS scope, capability graph, mounts, network, crash dumps, and observability",
                "hash-chain evidence checkpoint plus separate short-lived activation authorization",
                "manifest-pinned policy and evidence verification before Cell key release",
                "no self-attested, stale, cross-build, cross-Cell, cross-manifest, or role-substitution shortcut",
            ],
            missing: if regulated_data_admitted {
                Vec::new()
            } else {
                vec![
                    "complete threshold-signed Phase 8 evidence for the exact runtime subject",
                    "trusted deployment attestation for the selected isolation tier",
                    "short-lived threshold admission authorization",
                ]
            },
        }
    }
}

#[derive(Clone)]
struct PreparedFrontend {
    package_sha256: String,
    assets: BTreeMap<String, FrontendAsset>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceHandoffRequest {
    assertion: String,
    device_id: String,
    nonce: String,
    proof: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HandoffReplayRecord {
    digest: String,
    expires_at: i64,
    mac: String,
}

struct HandoffReplayJournal {
    path: PathBuf,
    key: Zeroizing<Vec<u8>>,
    active: BTreeMap<String, i64>,
}

impl HandoffReplayJournal {
    fn open(path: PathBuf, key: &[u8]) -> Result<Self> {
        let mut journal = Self {
            path,
            key: Zeroizing::new(key.to_vec()),
            active: BTreeMap::new(),
        };
        match fs::symlink_metadata(&journal.path) {
            Ok(metadata)
                if !metadata.file_type().is_symlink()
                    && metadata.is_file()
                    && metadata.len() <= MAX_HANDOFF_REPLAY_JOURNAL_BYTES =>
            {
                let bytes = read_bounded_regular_file(
                    &journal.path,
                    MAX_HANDOFF_REPLAY_JOURNAL_BYTES,
                    true,
                )?;
                let now = unix_seconds();
                for line in bytes
                    .split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                {
                    let record: HandoffReplayRecord =
                        serde_json::from_slice(line).map_err(|_| {
                            CellError::StorageUnsafe(
                                "handoff replay journal contains an invalid record".to_string(),
                            )
                        })?;
                    journal.verify_record(&record)?;
                    if record.expires_at >= now {
                        journal.active.insert(record.digest, record.expires_at);
                    }
                }
            }
            Ok(_) => {
                return Err(CellError::StorageUnsafe(
                    "handoff replay journal is not a bounded regular file".to_string(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(journal)
    }

    fn verify_record(&self, record: &HandoffReplayRecord) -> Result<()> {
        let expected = replay_record_mac(&self.key, &record.digest, record.expires_at)?;
        if expected != record.mac {
            return Err(CellError::StorageUnsafe(
                "handoff replay journal authentication failed".to_string(),
            ));
        }
        Ok(())
    }

    fn consume(&mut self, identity: &str, expires_at: i64) -> Result<()> {
        let now = unix_seconds();
        self.active.retain(|_, expiry| *expiry >= now);
        let digest = hex::encode(Sha256::digest(identity.as_bytes()));
        if self.active.contains_key(&digest) {
            return Err(CellError::Application(
                bicdb_app_runtime::AppRuntimeError::Authentication(
                    "one-time handoff assertion was already consumed".to_string(),
                ),
            ));
        }
        let record = HandoffReplayRecord {
            mac: replay_record_mac(&self.key, &digest, expires_at)?,
            digest: digest.clone(),
            expires_at,
        };
        let mut encoded = serde_json::to_vec(&record).map_err(|error| {
            CellError::StorageUnsafe(format!("encode handoff replay record: {error}"))
        })?;
        encoded.push(b'\n');
        let existing = fs::symlink_metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        if existing.saturating_add(encoded.len() as u64) > MAX_HANDOFF_REPLAY_JOURNAL_BYTES {
            return Err(CellError::StorageUnsafe(
                "handoff replay journal reached its fail-closed bound".to_string(),
            ));
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        }
        let mut file = options.open(&self.path)?;
        file.write_all(&encoded)?;
        file.sync_data()?;
        self.active.insert(digest, expires_at);
        Ok(())
    }
}

fn replay_record_mac(key: &[u8], digest: &str, expires_at: i64) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|error| CellError::StorageUnsafe(error.to_string()))?;
    mac.update(b"BICDB-CELL-HANDOFF-REPLAY-V1\0");
    mac.update(digest.as_bytes());
    mac.update(&expires_at.to_be_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

struct CellHttpBoundary {
    cell_id: CellId,
    external_authenticator: JwtAuthenticator,
    authorization: CellAuthorizationPolicy,
    session_key: Zeroizing<Vec<u8>>,
    frontends: BTreeMap<String, PreparedFrontend>,
    replay: Mutex<HandoffReplayJournal>,
}

impl CellHttpBoundary {
    fn handoff(&self, request: &ApplicationHttpRequest) -> Result<ApplicationHttpResponse> {
        if request.method != HttpMethod::Post {
            return Err(CellError::Application(
                bicdb_app_runtime::AppRuntimeError::InvalidRequest(
                    "cell handoff requires POST".to_string(),
                ),
            ));
        }
        let HttpRequestBodyV2::Json(body) = &request.body else {
            return Err(CellError::Application(
                bicdb_app_runtime::AppRuntimeError::InvalidRequest(
                    "cell handoff requires a JSON body".to_string(),
                ),
            ));
        };
        let handoff: DeviceHandoffRequest =
            serde_json::from_value(body.clone()).map_err(|error| {
                CellError::Application(bicdb_app_runtime::AppRuntimeError::InvalidRequest(format!(
                    "invalid cell handoff body: {error}"
                )))
            })?;
        if handoff.nonce.len() != 64
            || !handoff
                .nonce
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(CellError::Application(
                bicdb_app_runtime::AppRuntimeError::Authentication(
                    "handoff nonce must be 32 bytes of lowercase hexadecimal".to_string(),
                ),
            ));
        }
        let request_id = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-request-id"))
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let actor = self.external_authenticator.authenticate(
            &handoff.assertion,
            request_id,
            None,
            None,
            unix_seconds().saturating_add(MAX_CELL_HANDOFF_LIFETIME_SECONDS) * 1_000,
        )?;
        let user_id = actor.user_id.as_deref().ok_or_else(|| {
            CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
                "handoff assertion has no user identity".to_string(),
            ))
        })?;
        let assertion_id = actor.session_id.as_deref().ok_or_else(|| {
            CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
                "handoff assertion has no one-time session id".to_string(),
            ))
        })?;
        if actor.client_id.as_deref() != Some(handoff.device_id.as_str())
            || actor.assurance_level.as_deref()
                != Some(self.authorization.minimum_assurance.as_str())
            || actor.policy_attributes.get("cell_id").map(String::as_str)
                != Some(self.cell_id.as_str())
            || actor
                .policy_attributes
                .get("handoff_nonce")
                .map(String::as_str)
                != Some(handoff.nonce.as_str())
        {
            return Err(CellError::Application(
                bicdb_app_runtime::AppRuntimeError::Authentication(
                    "handoff assertion is not bound to this cell, device, nonce, or assurance policy"
                        .to_string(),
                ),
            ));
        }
        let member = self
            .authorization
            .members
            .iter()
            .find(|member| member.enabled && member.user_id == user_id)
            .ok_or_else(|| {
                CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
                    "user is not an active cell member".to_string(),
                ))
            })?;
        let device = member
            .devices
            .iter()
            .find(|device| device.enabled && device.device_id == handoff.device_id)
            .ok_or_else(|| {
                CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
                    "device is not authorized for this cell member".to_string(),
                ))
            })?;
        verify_device_handoff_proof(&self.cell_id, &handoff, &device.public_key)?;
        let now = unix_seconds();
        let assertion_expiry = actor.deadline_unix_ms / 1_000;
        self.replay
            .lock()
            .consume(assertion_id, assertion_expiry.max(now))?;
        let session_id = Uuid::new_v4().to_string();
        let expires_at = now.saturating_add(self.authorization.session_lifetime_seconds);
        let token = issue_cell_session(
            &self.session_key,
            &self.cell_id,
            &self.authorization,
            member,
            device,
            &session_id,
            expires_at,
        )?;
        let cookie_name = cell_session_cookie_name();
        Ok(ApplicationHttpResponse {
            status: 204,
            headers: vec![
                (
                    "set-cookie".to_string(),
                    format!(
                        "{cookie_name}={token}; Path=/; Max-Age={}; Secure; HttpOnly; SameSite=Strict",
                        self.authorization.session_lifetime_seconds
                    ),
                ),
                ("cache-control".to_string(), "no-store".to_string()),
            ],
            body: HttpResponseBodyV2::Empty,
            trailers: Vec::new(),
            retry_after_ms: None,
        })
    }

    fn frontend(&self, request: &ApplicationHttpRequest) -> Option<ApplicationHttpResponse> {
        if !matches!(request.method, HttpMethod::Get | HttpMethod::Head) {
            return None;
        }
        let (frontend, asset, immutable) = if self.frontends.len() == 1 {
            let frontend = self.frontends.values().next()?;
            let path = request.path.strip_prefix('/').unwrap_or(&request.path);
            let path = if path.is_empty() { "index.html" } else { path };
            if let Some(asset) = frontend.assets.get(path) {
                (frontend, asset, false)
            } else {
                frontend_asset_by_digest(&self.frontends, &request.path)?
            }
        } else {
            frontend_asset_by_digest(&self.frontends, &request.path)?
        };
        let body = if request.method == HttpMethod::Head {
            HttpResponseBodyV2::Empty
        } else {
            HttpResponseBodyV2::Binary(asset.bytes.clone())
        };
        Some(ApplicationHttpResponse {
            status: 200,
            headers: cell_frontend_headers(
                &asset.content_type,
                immutable,
                &frontend.package_sha256,
            ),
            body,
            trailers: Vec::new(),
            retry_after_ms: None,
        })
    }
}

impl TrustedHttpHandler for CellHttpBoundary {
    fn handle(
        &self,
        request: &ApplicationHttpRequest,
    ) -> bicdb_app_runtime::Result<Option<ApplicationHttpResponse>> {
        let result = if request.path == "/_bicdb/session/handoff" {
            Some(self.handoff(request))
        } else if request.path == "/_bicdb/session/logout" {
            if request.method != HttpMethod::Post {
                return Err(bicdb_app_runtime::AppRuntimeError::InvalidRequest(
                    "cell logout requires POST".to_string(),
                ));
            }
            Some(Ok(ApplicationHttpResponse {
                status: 204,
                headers: vec![(
                    "set-cookie".to_string(),
                    format!(
                        "{}=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Strict",
                        cell_session_cookie_name()
                    ),
                )],
                body: HttpResponseBodyV2::Empty,
                trailers: Vec::new(),
                retry_after_ms: None,
            }))
        } else {
            return Ok(self.frontend(request));
        };
        result
            .expect("reserved cell path has a response")
            .map(Some)
            .map_err(|error| match error {
                CellError::Application(error) => error,
                other => bicdb_app_runtime::AppRuntimeError::Provider(other.to_string()),
            })
    }
}

fn cell_session_cookie_name() -> &'static str {
    "__Host-bicdb-session"
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn verify_device_handoff_proof(
    cell_id: &CellId,
    handoff: &DeviceHandoffRequest,
    encoded_public_key: &str,
) -> Result<()> {
    let public_key: [u8; 32] = hex::decode(encoded_public_key)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| CellError::ArtifactMismatch("invalid device public key".to_string()))?;
    let key = VerifyingKey::from_bytes(&public_key)
        .map_err(|error| CellError::ArtifactMismatch(error.to_string()))?;
    let signature_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&handoff.proof)
        .map_err(|_| {
            CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
                "device handoff proof is not base64url".to_string(),
            ))
        })?;
    let signature = Signature::from_slice(&signature_bytes).map_err(|_| {
        CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
            "device handoff proof is not an Ed25519 signature".to_string(),
        ))
    })?;
    let mut message = b"BICDB-CELL-DEVICE-HANDOFF-V1\0".to_vec();
    message.extend_from_slice(cell_id.as_str().as_bytes());
    message.push(0);
    message.extend_from_slice(handoff.device_id.as_bytes());
    message.push(0);
    message.extend_from_slice(&Sha256::digest(handoff.assertion.as_bytes()));
    message.push(0);
    message.extend_from_slice(handoff.nonce.as_bytes());
    key.verify_strict(&message, &signature).map_err(|_| {
        CellError::Application(bicdb_app_runtime::AppRuntimeError::Authentication(
            "device handoff proof is invalid".to_string(),
        ))
    })
}

fn issue_cell_session(
    key: &[u8],
    cell_id: &CellId,
    policy: &CellAuthorizationPolicy,
    member: &CellMemberAuthorization,
    device: &CellAuthorizedDevice,
    session_id: &str,
    expires_at: i64,
) -> Result<String> {
    let header = serde_json::json!({"alg":"HS256","typ":"JWT"});
    let claims = JwtClaims {
        sub: member.user_id.clone(),
        email: None,
        name: None,
        service_id: None,
        client_id: Some(device.device_id.clone()),
        acting_client_id: None,
        tenant_id: None,
        workspace_id: None,
        organization_id: None,
        session_id: Some(session_id.to_string()),
        roles: member.roles.clone(),
        scopes: member.scopes.clone(),
        assurance_level: Some(policy.minimum_assurance.clone()),
        iss: format!("bicdb-cell://{cell_id}"),
        aud: JwtAudience::One(format!("bicdb-cell:{cell_id}")),
        exp: expires_at,
        token_type: Some("access".to_string()),
        nbf: Some(unix_seconds()),
        policy: BTreeMap::from([
            ("cell_id".to_string(), cell_id.to_string()),
            (
                "authorization_epoch".to_string(),
                policy.authorization_epoch.to_string(),
            ),
        ]),
    };
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&header).map_err(|error| {
            CellError::Application(bicdb_app_runtime::AppRuntimeError::Json(error))
        })?,
    );
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&claims).map_err(|error| {
            CellError::Application(bicdb_app_runtime::AppRuntimeError::Json(error))
        })?,
    );
    let signing_input = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|error| CellError::StorageUnsafe(error.to_string()))?;
    mac.update(signing_input.as_bytes());
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{signature}"))
}

fn derive_cell_session_key(
    cell_key: &CellKeyMaterial,
    cell_id: &CellId,
    manifest_digest: &Sha256Digest,
    authorization_epoch: u64,
) -> Result<Zeroizing<Vec<u8>>> {
    let input = Zeroizing::new(cell_key.clone_bytes());
    let hkdf = Hkdf::<Sha256>::new(Some(cell_id.as_str().as_bytes()), input.as_slice());
    let mut output = Zeroizing::new(vec![0_u8; 32]);
    let mut info = b"BICDB-CELL-SESSION-KEY-V1\0".to_vec();
    info.extend_from_slice(manifest_digest.as_str().as_bytes());
    info.extend_from_slice(&authorization_epoch.to_be_bytes());
    hkdf.expand(&info, output.as_mut_slice())
        .map_err(|_| CellError::StorageUnsafe("derive cell session key".to_string()))?;
    Ok(output)
}

fn frontend_asset_by_digest<'a>(
    frontends: &'a BTreeMap<String, PreparedFrontend>,
    path: &str,
) -> Option<(&'a PreparedFrontend, &'a FrontendAsset, bool)> {
    let path = path.strip_prefix("/_bicdb/apps/")?;
    let mut segments = path.splitn(3, '/');
    let application = segments.next()?;
    let digest = segments.next()?;
    let asset_path = segments.next()?;
    let frontend = frontends.get(application)?;
    if digest != frontend.package_sha256 {
        return None;
    }
    Some((frontend, frontend.assets.get(asset_path)?, true))
}

fn cell_frontend_headers(
    content_type: &str,
    immutable: bool,
    package_sha256: &str,
) -> Vec<(String, String)> {
    vec![
        ("content-type".to_string(), content_type.to_string()),
        (
            "cache-control".to_string(),
            if immutable {
                "public, max-age=31536000, immutable"
            } else {
                "no-store"
            }
            .to_string(),
        ),
        (
            "etag".to_string(),
            format!("\"sha256:{package_sha256}\""),
        ),
        (
            "content-security-policy".to_string(),
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"
                .to_string(),
        ),
        ("x-content-type-options".to_string(), "nosniff".to_string()),
        ("x-frame-options".to_string(), "DENY".to_string()),
        ("referrer-policy".to_string(), "no-referrer".to_string()),
        (
            "permissions-policy".to_string(),
            "camera=(), microphone=(), geolocation=(), payment=(), usb=()".to_string(),
        ),
        (
            "cross-origin-opener-policy".to_string(),
            "same-origin".to_string(),
        ),
        (
            "cross-origin-resource-policy".to_string(),
            "same-origin".to_string(),
        ),
    ]
}

/// The only serving capability constructed by a CellRuntime. It deliberately
/// exposes neither the database nor package lifecycle mutation methods.
pub struct CellApplicationHost {
    runtime: Arc<ApplicationRuntime>,
    authenticator: Arc<JwtAuthenticator>,
    trusted_handler: Option<Arc<dyn TrustedHttpHandler>>,
    admission: Option<Arc<CellAdmissionLease>>,
}

impl CellApplicationHost {
    pub fn readiness(&self) -> ApplicationReadiness {
        self.runtime.readiness()
    }

    pub async fn serve_http(
        &self,
        listener: tokio::net::TcpListener,
        mut policy: HttpHostPolicy,
    ) -> Result<HttpServerHandle> {
        if let Some(admission) = &self.admission {
            admission.check()?;
            policy.admission_check = Some(admission.clone());
        }
        if self.trusted_handler.is_some() {
            policy.session_cookie_name = Some(cell_session_cookie_name().to_string());
        }
        Ok(self
            .runtime
            .clone()
            .serve_http_with_handler(
                listener,
                self.authenticator.clone(),
                policy,
                self.trusted_handler.clone(),
            )
            .await?)
    }

    pub async fn serve_http_tls(
        &self,
        address: SocketAddr,
        certificate: impl AsRef<Path>,
        private_key: impl AsRef<Path>,
        mut policy: HttpHostPolicy,
    ) -> Result<HttpServerHandle> {
        if let Some(admission) = &self.admission {
            admission.check()?;
            policy.admission_check = Some(admission.clone());
        }
        if self.trusted_handler.is_some() {
            policy.session_cookie_name = Some(cell_session_cookie_name().to_string());
        }
        Ok(self
            .runtime
            .clone()
            .serve_http_tls_with_handler(
                address,
                certificate,
                private_key,
                self.authenticator.clone(),
                policy,
                self.trusted_handler.clone(),
            )
            .await?)
    }
}

pub struct CellRuntime {
    manifest: VerifiedCellManifest,
    volume_path: PathBuf,
    // The open file carries the OS-enforced exclusive lease for this exact
    // volume. It intentionally has no public accessor and lives until every
    // database/application capability has been torn down.
    _runtime_lock: File,
    database: Option<Arc<RwLock<BicDb>>>,
    application_host: Option<CellApplicationHost>,
    ha: Option<CellHaRuntime>,
    device: Option<CellDeviceRuntime>,
    grant: Option<CellGrantRuntime>,
    admission: Option<Arc<CellAdmissionLease>>,
    key_provider_kind: &'static str,
}

struct CellHaRuntime {
    policy: Arc<HaTrustPolicy>,
    fence: Arc<CellHaCommitFence>,
    object_cipher: DatabaseObjectCipher,
    database_path: PathBuf,
    replica_signing_key: SigningKey,
}

struct PreparedCellHa {
    policy: Arc<HaTrustPolicy>,
    epoch: CertifiedWriterEpoch,
    lease: CertifiedReplicaLease,
    previous_epoch: Option<CertifiedWriterEpoch>,
    previous_primary_lease: Option<CertifiedReplicaLease>,
    state_store: CellHaStateStore,
    previous_state: Option<CellHaState>,
    replica_signing_key: SigningKey,
}

struct PreparedCellDevice {
    policy: Arc<DeviceTrustPolicy>,
    exporter: DeviceExporter,
}

struct CellDeviceRuntime {
    policy: Arc<DeviceTrustPolicy>,
    exporter: DeviceExporter,
    ledger: DeviceParentLedger,
}

struct PreparedCellGrant {
    policy: Arc<GrantTrustPolicy>,
    exporter: GrantExporter,
    recipient_private_key: SoftwareRecipientPrivateKey,
}

struct CellGrantRuntime {
    policy: Arc<GrantTrustPolicy>,
    exporter: GrantExporter,
    recipient_private_key: SoftwareRecipientPrivateKey,
    source_ledger: GrantSourceLedger,
    recipient_ledger: GrantRecipientLedger,
}

struct PreparedCellAdmission {
    lease: Arc<CellAdmissionLease>,
}

/// An encrypted backup awaiting independent Recovery-authority certification.
/// The candidate has no authority to restore itself; callers must obtain a
/// `CertifiedCellBackup` whose manifest is byte-for-byte identical.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CellBackupCandidate {
    pub manifest: CellBackupManifest,
    pub archive_path: PathBuf,
    pub wrapped_key_path: PathBuf,
    pub archive_digest: Sha256Digest,
    pub wrapped_key_digest: Sha256Digest,
}

#[derive(Serialize)]
struct CellBackupKeyBinding<'a> {
    format: &'static str,
    backup_id: &'a HaId,
    cell_id: &'a HaId,
    group_id: &'a str,
    lineage_id: &'a str,
    key_epoch: u64,
    manifest_generation: u64,
    manifest_digest: &'a HaDigest,
    archive_digest: &'a Sha256Digest,
}

#[derive(Serialize)]
struct CellBackupPayload<'a> {
    format: &'static str,
    backup_id: &'a HaId,
    archive_digest: &'a Sha256Digest,
    wrapped_key_digest: &'a Sha256Digest,
}

/// Rotate one Phase-2 cell through an exact signed manifest transition. Both
/// keys arrive through independently verified, one-shot attested leases. The
/// generic storage rotator never sees manifest authority, while this boundary
/// never obtains a credential capable of requesting arbitrary cell keys.
pub fn rotate_cell_key(config: CellKeyRotationConfig) -> Result<CellKeyRotationReport> {
    let current =
        load_verified_manifest(&config.current_manifest_path, &config.trusted_manifest_keys)?;
    let next = load_verified_manifest(&config.next_manifest_path, &config.trusted_manifest_keys)?;
    validate_key_rotation_transition(&current, &next)?;
    if current.manifest.cell_id != config.expected_cell_id
        || next.manifest.cell_id != config.expected_cell_id
        || current.manifest.storage.volume_id != config.expected_volume_id
        || next.manifest.storage.volume_id != config.expected_volume_id
    {
        return Err(CellError::IdentityMismatch(
            "key rotation manifests do not match the requested cell and volume".to_string(),
        ));
    }
    if current.manifest.runtime.guest_image_digest != config.expected_guest_image_digest
        || next.manifest.runtime.guest_image_digest != config.expected_guest_image_digest
    {
        return Err(CellError::RuntimeMismatch(
            "key rotation guest image measurement does not match both manifests".to_string(),
        ));
    }
    let running_digest = running_binary_digest()?;
    if current.manifest.runtime.bicdb_binary_digest != running_digest
        || next.manifest.runtime.bicdb_binary_digest != running_digest
    {
        return Err(CellError::RuntimeMismatch(
            "running BicDB binary does not match the signed key-rotation transition".to_string(),
        ));
    }
    if config.current_key_provider.assurance() != CellKeyProviderAssurance::AttestedLease
        || config.next_key_provider.assurance() != CellKeyProviderAssurance::AttestedLease
    {
        return Err(CellError::KeyScopeMismatch(
            "Phase 2 key rotation accepts only two independently attested signed leases"
                .to_string(),
        ));
    }

    let volume_path = canonical_directory_without_symlink(&config.volume_path)?;
    verify_volume_identity(&volume_path, &current, &config.trusted_manifest_keys)?;
    verify_volume_identity(&volume_path, &next, &config.trusted_manifest_keys)?;
    verify_local_manifest_state(&volume_path, &current)?;
    verify_local_manifest_state(&volume_path, &next)?;

    let database_path = volume_path.join(&current.manifest.storage.database_relative_path);
    verify_cell_storage_tree_safety(&database_path)?;

    let current_key = config
        .current_key_provider
        .unwrap_cell_key(key_request_for_manifest(&current))?;
    let next_key = config
        .next_key_provider
        .unwrap_cell_key(key_request_for_manifest(&next))?;
    if current_key.fingerprint() != current.manifest.keys.key_fingerprint
        || next_key.fingerprint() != next.manifest.keys.key_fingerprint
    {
        return Err(CellError::KeyScopeMismatch(
            "key-rotation lease fingerprint does not match its signed manifest".to_string(),
        ));
    }
    let current_evidence = current_key.rollback_evidence.as_ref().ok_or_else(|| {
        CellError::KeyLeaseInvalid(
            "retiring key lease supplied no external rollback evidence".to_string(),
        )
    })?;
    let next_evidence = next_key.rollback_evidence.as_ref().ok_or_else(|| {
        CellError::KeyLeaseInvalid(
            "next key lease supplied no external rollback evidence".to_string(),
        )
    })?;
    verify_external_rollback_evidence(&volume_path, &current, current_evidence)?;
    verify_external_rollback_scope(&next, next_evidence)?;
    if next_evidence.previous_rollback_counter != current_evidence.rollback_counter {
        return Err(CellError::ManifestRollback(
            "next key lease must name the retiring lease counter as its exact predecessor"
                .to_string(),
        ));
    }

    let storage = rotate_bound_database_encryption(
        &database_path,
        EncryptionConfig::with_raw_key(current_key.clone_bytes()).with_binding(
            EncryptionBinding::new(
                current.manifest.cell_id.as_str(),
                &current.manifest.storage.encryption_profile,
                current.manifest.keys.key_epoch,
            )?,
        ),
        EncryptionConfig::with_raw_key(next_key.clone_bytes()).with_binding(
            EncryptionBinding::new(
                next.manifest.cell_id.as_str(),
                &next.manifest.storage.encryption_profile,
                next.manifest.keys.key_epoch,
            )?,
        ),
        BoundEncryptionRotationOptions::default(),
    )?;

    // External anti-rollback evidence is committed first. A crash between
    // these two atomic files is recovered by opening the already-activated
    // next manifest with a fresh, higher external lease; the inverse order
    // could strand the old manifest as active without its external witness.
    persist_external_rollback_evidence(&volume_path, next_evidence)?;
    persist_local_manifest_state(&volume_path, &next)?;
    Ok(CellKeyRotationReport {
        cell_id: next.manifest.cell_id.clone(),
        volume_id: next.manifest.storage.volume_id.clone(),
        previous_manifest_digest: current.digest.clone(),
        active_manifest_digest: next.digest.clone(),
        previous_manifest_generation: current.manifest.manifest_generation,
        active_manifest_generation: next.manifest.manifest_generation,
        storage,
        regulated_data_admitted: false,
    })
}

fn validate_key_rotation_transition(
    current: &VerifiedCellManifest,
    next: &VerifiedCellManifest,
) -> Result<()> {
    if !current.manifest.uses_bound_cryptography() || !next.manifest.uses_bound_cryptography() {
        return Err(CellError::RegulatedAdmissionIncomplete(
            "key rotation is callable only for coherent bound-cryptography manifests".to_string(),
        ));
    }
    if next.manifest.manifest_generation != current.manifest.manifest_generation.saturating_add(1)
        || next.manifest.previous_manifest_digest.as_ref() != Some(&current.digest)
    {
        return Err(CellError::ManifestRollback(
            "key rotation requires the immediately linked next manifest generation".to_string(),
        ));
    }
    if next.manifest.keys.authority != current.manifest.keys.authority
        || next.manifest.keys.cell_kek_id != current.manifest.keys.cell_kek_id
        || current.manifest.keys.key_epoch.checked_add(1) != Some(next.manifest.keys.key_epoch)
        || next.manifest.keys.key_fingerprint == current.manifest.keys.key_fingerprint
    {
        return Err(CellError::KeyScopeMismatch(
            "rotation must preserve key authority/KEK identity, advance key epoch by exactly one, and change key material"
                .to_string(),
        ));
    }
    let mut expected = current.manifest.clone();
    expected.manifest_generation = next.manifest.manifest_generation;
    expected.previous_manifest_digest = next.manifest.previous_manifest_digest.clone();
    expected.keys = next.manifest.keys.clone();
    if expected != next.manifest {
        return Err(CellError::DocumentInvalid(
            "key-rotation transition may change only generation linkage and key epoch/fingerprint"
                .to_string(),
        ));
    }
    Ok(())
}

fn load_ha_document<T>(path: &Path) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let bytes = read_bounded_regular_file(path, 4 * 1024 * 1024, false)?;
    Ok(decode_ha_document(&bytes)?)
}

fn ha_activation_context(
    manifest: &VerifiedCellManifest,
    durable_commit_seq: u64,
    now: i64,
) -> Result<HaActivationContext> {
    Ok(HaActivationContext {
        cell_id: HaId::parse(manifest.manifest.cell_id.as_str(), "cell_id")?,
        group_id: manifest.manifest.replication.group_id.clone(),
        replica_id: HaId::parse(
            manifest.manifest.replication.replica_id.clone(),
            "replica_id",
        )?,
        role: manifest.manifest.replication.role.into(),
        writer_epoch: manifest.manifest.replication.writer_epoch,
        key_epoch: manifest.manifest.keys.key_epoch,
        manifest_generation: manifest.manifest.manifest_generation,
        // HA epochs bind the canonical Cell-wide configuration, not the
        // replica-local manifest envelope. The latter contains unique volume
        // and replica identities and therefore cannot be identical on a real
        // primary and standby.
        manifest_digest: cell_ha_configuration_digest(&manifest.manifest)?,
        local_durable_commit_seq: durable_commit_seq,
        now,
    })
}

fn prepare_cell_ha(
    manifest: &VerifiedCellManifest,
    volume_path: &Path,
    config: Option<&CellHaRuntimeConfig>,
) -> Result<Option<PreparedCellHa>> {
    if manifest.manifest.uses_cell_ha() != config.is_some() {
        return Err(CellError::RegulatedAdmissionIncomplete(
            "the Phase 5/6 profile and HA runtime documents must be supplied together".to_string(),
        ));
    }
    let Some(config) = config else {
        return Ok(None);
    };
    if config.previous_writer_epoch_path.is_some() != config.previous_primary_lease_path.is_some() {
        return Err(CellError::DocumentInvalid(
            "previous writer epoch and primary lease must be supplied together".to_string(),
        ));
    }
    let policy_bytes =
        read_bounded_regular_file(&config.trust_policy_path, 4 * 1024 * 1024, false)?;
    let expected_policy_digest = manifest
        .manifest
        .policy
        .ha_trust_policy_digest
        .as_ref()
        .ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Phase 5 manifest has no HA trust-policy digest".to_string(),
            )
        })?;
    if Sha256Digest::of_bytes(&policy_bytes) != *expected_policy_digest {
        return Err(CellError::ArtifactMismatch(
            "HA trust policy does not match the signed CellManifest pin".to_string(),
        ));
    }
    let policy: HaTrustPolicy = decode_ha_document(&policy_bytes)?;
    policy.validate()?;
    let epoch: CertifiedWriterEpoch = load_ha_document(&config.writer_epoch_path)?;
    let lease: CertifiedReplicaLease = load_ha_document(&config.replica_lease_path)?;
    let previous_epoch = config
        .previous_writer_epoch_path
        .as_deref()
        .map(load_ha_document)
        .transpose()?;
    let previous_primary_lease = config
        .previous_primary_lease_path
        .as_deref()
        .map(load_ha_document)
        .transpose()?;
    let replica_signing_key = load_signing_key(&config.replica_signing_key_path)?;
    if hex::encode(replica_signing_key.verifying_key().as_bytes())
        != lease.statement.replica_public_key
    {
        return Err(CellError::SignatureInvalid(
            "replica signing key does not match the certified short-lived lease".to_string(),
        ));
    }

    // Verify signatures, topology, exact scope, lease time, and epoch
    // non-overlap before consuming the one-shot Cell key lease. The signed
    // lease carries the durable sequence expected at encrypted open; it is
    // checked against the actual database immediately afterwards.
    verify_ha_activation(
        &policy,
        &epoch,
        &lease,
        previous_epoch.as_ref(),
        previous_primary_lease.as_ref(),
        &ha_activation_context(
            manifest,
            lease.statement.local_durable_commit_seq,
            current_unix_timestamp(),
        )?,
    )?;

    let state_store = CellHaStateStore::new(volume_path.join(CELL_HA_STATE_FILE));
    let previous_state = state_store.load()?;
    if let Some(state) = previous_state.as_ref() {
        let epoch_digest = ha_document_digest(&epoch.statement)?;
        let expected = ha_activation_context(
            manifest,
            lease.statement.local_durable_commit_seq,
            current_unix_timestamp(),
        )?;
        if state.cell_id != expected.cell_id
            || state.group_id != expected.group_id
            || state.replica_id != expected.replica_id
            || state.writer_epoch != expected.writer_epoch
            || state.writer_epoch_digest != epoch_digest
            || state.last_applied_commit_seq != lease.statement.local_durable_commit_seq
        {
            return Err(CellError::IdentityMismatch(
                "durable HA witness does not match the exact certified replica activation"
                    .to_string(),
            ));
        }
    }
    Ok(Some(PreparedCellHa {
        policy: Arc::new(policy),
        epoch,
        lease,
        previous_epoch,
        previous_primary_lease,
        state_store,
        previous_state,
        replica_signing_key,
    }))
}

fn prepare_cell_device(
    manifest: &VerifiedCellManifest,
    config: Option<&CellDeviceRuntimeConfig>,
) -> Result<Option<PreparedCellDevice>> {
    if manifest.manifest.uses_device_edge() != config.is_some() {
        return Err(CellError::RegulatedAdmissionIncomplete(
            "the Phase 6 profile and device runtime documents must be supplied together"
                .to_string(),
        ));
    }
    let Some(config) = config else {
        return Ok(None);
    };
    let policy_bytes =
        read_bounded_regular_file(&config.trust_policy_path, 4 * 1024 * 1024, false)?;
    let expected_digest = manifest
        .manifest
        .policy
        .device_trust_policy_digest
        .as_ref()
        .ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Phase 6 manifest has no device trust-policy digest".to_string(),
            )
        })?;
    if Sha256Digest::of_bytes(&policy_bytes) != *expected_digest {
        return Err(CellError::ArtifactMismatch(
            "device trust policy does not match the signed CellManifest pin".to_string(),
        ));
    }
    let policy: DeviceTrustPolicy = bicdb_cell_device::decode_document(&policy_bytes)?;
    policy.validate()?;
    let signing_key = load_signing_key(&config.exporter_signing_key_path)?;
    let policy = Arc::new(policy);
    let exporter = DeviceExporter::new(
        Arc::clone(&policy),
        config.exporter_key_id.clone(),
        signing_key,
    )?;
    Ok(Some(PreparedCellDevice { policy, exporter }))
}

fn prepare_cell_grant(
    manifest: &VerifiedCellManifest,
    config: Option<&CellGrantRuntimeConfig>,
) -> Result<Option<PreparedCellGrant>> {
    if manifest.manifest.uses_cross_cell_grants() != config.is_some() {
        return Err(CellError::RegulatedAdmissionIncomplete(
            "the Phase 7/8 profile and grant runtime documents must be supplied together"
                .to_string(),
        ));
    }
    let Some(config) = config else {
        return Ok(None);
    };
    let policy_bytes =
        read_bounded_regular_file(&config.trust_policy_path, 4 * 1024 * 1024, false)?;
    let expected_digest = manifest
        .manifest
        .policy
        .grant_trust_policy_digest
        .as_ref()
        .ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Phase 7 manifest has no grant trust-policy digest".to_string(),
            )
        })?;
    if Sha256Digest::of_bytes(&policy_bytes) != *expected_digest {
        return Err(CellError::ArtifactMismatch(
            "grant trust policy does not match the signed CellManifest pin".to_string(),
        ));
    }
    let policy: GrantTrustPolicy = bicdb_cell_grant::decode_document(&policy_bytes)?;
    policy.validate()?;
    if policy.cell_id.as_str() != manifest.manifest.cell_id.as_str() {
        return Err(CellError::IdentityMismatch(
            "grant trust policy belongs to another Cell".to_string(),
        ));
    }
    let exporter_signing_key = load_signing_key(&config.exporter_signing_key_path)?;
    let exporter = GrantExporter::new(
        &policy,
        GrantId::parse(manifest.manifest.cell_id.as_str(), "cell_id")?,
        config.exporter_key_id.clone(),
        exporter_signing_key,
    )?;
    let private_bytes = Zeroizing::new(read_bounded_regular_file(
        &config.recipient_private_key_path,
        128,
        true,
    )?);
    let decoded = if private_bytes.len() == 32 {
        Zeroizing::new(private_bytes.to_vec())
    } else {
        let text = std::str::from_utf8(private_bytes.as_ref()).map_err(|_| {
            CellError::DocumentInvalid("recipient private key is not raw or hex".to_string())
        })?;
        Zeroizing::new(hex::decode(text.trim()).map_err(|_| {
            CellError::DocumentInvalid("recipient private key is not raw or hex".to_string())
        })?)
    };
    if decoded.len() != 32 {
        return Err(CellError::DocumentInvalid(
            "recipient private key must contain exactly 32 bytes".to_string(),
        ));
    }
    let recipient_private_key =
        SoftwareRecipientPrivateKey::from_bytes(config.recipient_key_id.clone(), decoded.to_vec())?;
    let pinned = policy
        .recipient_encryption_keys
        .iter()
        .find(|key| key.key_id == config.recipient_key_id)
        .ok_or_else(|| {
            CellError::ArtifactMismatch(
                "recipient private key id is not pinned by the grant policy".to_string(),
            )
        })?;
    if pinned.public_key != recipient_private_key.public_key() {
        return Err(CellError::ArtifactMismatch(
            "recipient private key does not match the grant policy public key".to_string(),
        ));
    }
    Ok(Some(PreparedCellGrant {
        policy: Arc::new(policy),
        exporter,
        recipient_private_key,
    }))
}

fn admission_digest(digest: &Sha256Digest) -> Result<AdmissionDigest> {
    Ok(AdmissionDigest::parse(digest.as_str())?)
}

fn admission_subject(
    manifest: &VerifiedCellManifest,
    deployment_isolation_tier: &str,
) -> Result<AdmissionSubject> {
    let mut applications = manifest
        .manifest
        .applications
        .iter()
        .map(|application| {
            Ok(ApplicationMeasurement {
                application_root: application.root.clone(),
                application_name: application.name.clone(),
                digest: admission_digest(&application.digest)?,
                schema_generation: application.schema_generation,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    applications.sort_by(|left, right| {
        (&left.application_root, &left.application_name)
            .cmp(&(&right.application_root, &right.application_name))
    });
    let subject = AdmissionSubject {
        cell_id: manifest.manifest.cell_id.as_str().to_string(),
        manifest_digest: admission_digest(&manifest.digest)?,
        bicdb_binary_digest: admission_digest(&manifest.manifest.runtime.bicdb_binary_digest)?,
        guest_image_digest: admission_digest(&manifest.manifest.runtime.guest_image_digest)?,
        applications,
        security_profile: manifest.manifest.policy.security_profile.clone(),
        isolation_profile: manifest.manifest.runtime.isolation_profile.clone(),
        deployment_isolation_tier: deployment_isolation_tier.to_string(),
    };
    subject.validate()?;
    Ok(subject)
}

fn prepare_cell_admission(
    manifest: &VerifiedCellManifest,
    config: Option<&CellAdmissionRuntimeConfig>,
) -> Result<Option<PreparedCellAdmission>> {
    if manifest.manifest.uses_phase8_hardened_fleet() != config.is_some() {
        return Err(CellError::RegulatedAdmissionIncomplete(
            "the Phase 8 profile and admission evidence documents must be supplied together"
                .to_string(),
        ));
    }
    let Some(config) = config else {
        return Ok(None);
    };
    let policy_bytes =
        read_bounded_regular_file(&config.trust_policy_path, 16 * 1024 * 1024, false)?;
    let expected_digest = manifest
        .manifest
        .policy
        .admission_trust_policy_digest
        .as_ref()
        .ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Phase 8 manifest has no admission trust-policy digest".to_string(),
            )
        })?;
    if Sha256Digest::of_bytes(&policy_bytes) != *expected_digest {
        return Err(CellError::ArtifactMismatch(
            "admission trust policy does not match the signed CellManifest pin".to_string(),
        ));
    }
    let policy: AdmissionTrustPolicy = bicdb_cell_admission::decode_document(&policy_bytes)?;
    policy.validate()?;
    let bundle_bytes =
        read_bounded_regular_file(&config.evidence_bundle_path, 16 * 1024 * 1024, false)?;
    let bundle: AdmissionBundle = bicdb_cell_admission::decode_document(&bundle_bytes)?;
    let subject = admission_subject(manifest, &config.expected_deployment_isolation_tier)?;
    let verified = verify_admission_bundle(&policy, &bundle, &subject, current_unix_timestamp())?;
    let lease = Arc::new(CellAdmissionLease::new(verified)?);
    Ok(Some(PreparedCellAdmission { lease }))
}

impl CellRuntime {
    /// Validate every Phase-1 identity before the database is allowed to open.
    /// The first storage mutation can occur only after all signed bindings,
    /// artifact pins, and the cell-scoped key fingerprint have matched.
    pub fn open(config: CellRuntimeConfig) -> Result<Self> {
        let convergence_started_at = current_unix_timestamp();
        let verified =
            load_verified_manifest(&config.manifest_path, &config.trusted_manifest_keys)?;
        let manifest = &verified.manifest;
        if manifest.cell_id != config.expected_cell_id {
            return Err(CellError::IdentityMismatch(
                "requested cell does not match signed manifest".to_string(),
            ));
        }
        if manifest.storage.volume_id != config.expected_volume_id {
            return Err(CellError::IdentityMismatch(
                "requested volume does not match signed manifest".to_string(),
            ));
        }
        if manifest.runtime.guest_image_digest != config.expected_guest_image_digest {
            return Err(CellError::RuntimeMismatch(
                "guest image measurement does not match manifest".to_string(),
            ));
        }

        let volume_path = canonical_directory_without_symlink(&config.volume_path)?;
        verify_volume_identity(&volume_path, &verified, &config.trusted_manifest_keys)?;
        verify_local_manifest_state(&volume_path, &verified)?;

        if running_binary_digest()? != manifest.runtime.bicdb_binary_digest {
            return Err(CellError::RuntimeMismatch(
                "running BicDB binary digest does not match manifest".to_string(),
            ));
        }
        let prepared_admission = prepare_cell_admission(&verified, config.admission.as_ref())?;
        let regulated_data_admitted = prepared_admission
            .as_ref()
            .is_some_and(|prepared| prepared.lease.check().is_ok());
        let prepared_ha = prepare_cell_ha(&verified, &volume_path, config.ha.as_ref())?;
        let prepared_device = prepare_cell_device(&verified, config.device.as_ref())?;
        let prepared_grant = prepare_cell_grant(&verified, config.grant.as_ref())?;
        let prepared_fleet = prepare_fleet_activation(
            &verified,
            &volume_path,
            config.application_host.as_ref(),
            &config.trusted_manifest_keys,
        )?;
        let prepared_applications = prepare_cell_applications(
            &config.artifact_root,
            manifest,
            config.application_host.as_ref(),
            regulated_data_admitted,
        )?;

        // All signed identity, HA, fleet, and application inputs have now
        // been verified. Acquire a kernel-enforced single-runtime lease before
        // key release or database open so two processes can never mutate one
        // Cell volume through independent in-memory fences.
        let runtime_lock = acquire_cell_runtime_lock(&volume_path)?;

        let database_path = volume_path.join(&manifest.storage.database_relative_path);
        verify_cell_storage_tree_safety(&database_path)?;

        if manifest.uses_bound_cryptography()
            && config.key_provider.assurance() != CellKeyProviderAssurance::AttestedLease
        {
            return Err(CellError::KeyScopeMismatch(
                "bound-cryptography cells require an attested signed key lease; development file keys are refused"
                    .to_string(),
            ));
        }
        if let Some(admission) = &prepared_admission {
            admission.lease.check()?;
        }
        let key = config
            .key_provider
            .unwrap_cell_key(key_request_for_manifest(&verified))?;
        if let Some(admission) = &prepared_admission {
            admission.lease.check()?;
        }
        if key.fingerprint() != manifest.keys.key_fingerprint {
            return Err(CellError::KeyScopeMismatch(
                "unwrapped key fingerprint does not match manifest".to_string(),
            ));
        }
        let rollback_evidence = if manifest.uses_bound_cryptography() {
            let evidence = key.rollback_evidence.as_ref().ok_or_else(|| {
                CellError::KeyLeaseInvalid(
                    "Phase 2 key provider returned no external rollback evidence".to_string(),
                )
            })?;
            verify_external_rollback_evidence(&volume_path, &verified, evidence)?;
            Some(evidence.clone())
        } else {
            None
        };
        let key_provider_kind = config.key_provider.provider_kind();
        // Phase 1 deliberately uses the encrypted log store. BicDB's
        // server-paged engine currently refuses database-level encryption
        // because its data pages are not yet encrypted; selecting it here
        // would turn a truthful admission failure into a misleading claim.
        let encryption = if manifest.uses_bound_cryptography() {
            EncryptionConfig::with_raw_key(key.clone_bytes()).with_binding(EncryptionBinding::new(
                manifest.cell_id.as_str(),
                &manifest.storage.encryption_profile,
                manifest.keys.key_epoch,
            )?)
        } else {
            EncryptionConfig::with_raw_key(key.clone_bytes())
        };
        let database_config = if manifest.uses_cell_ha() {
            let mode = match manifest.replication.role {
                CellReplicaRole::Primary => ReplicationMode::Primary,
                CellReplicaRole::Standby | CellReplicaRole::Recovery => ReplicationMode::Standby,
            };
            let replication = ReplicationConfig {
                enabled: false,
                mode,
                cluster_id: manifest.replication.group_id.clone(),
                node_id: manifest.replication.replica_id.clone(),
                ..ReplicationConfig::default()
            };
            DbConfig::default()
                .with_fsync(true)
                .with_replication(replication)
                .with_required_commit_admission(true)
        } else {
            DbConfig::default().with_fsync(true)
        };
        let database = BicDb::open_with_encryption(&database_path, database_config, encryption)?;
        let ha = prepared_ha
            .map(|prepared| {
                let durable_commit_seq = database.last_applied_commit_seq();
                let activation = verify_ha_activation(
                    &prepared.policy,
                    &prepared.epoch,
                    &prepared.lease,
                    prepared.previous_epoch.as_ref(),
                    prepared.previous_primary_lease.as_ref(),
                    &ha_activation_context(
                        &verified,
                        durable_commit_seq,
                        current_unix_timestamp(),
                    )?,
                )?;
                let initial_state = CellHaState {
                    format: HA_STATE_FORMAT.to_string(),
                    cell_id: activation.lease.cell_id.clone(),
                    group_id: activation.lease.group_id.clone(),
                    replica_id: activation.lease.replica_id.clone(),
                    writer_epoch: activation.lease.writer_epoch,
                    writer_epoch_digest: activation.epoch_digest.clone(),
                    last_applied_commit_seq: durable_commit_seq,
                    last_replication_object_digest: prepared
                        .previous_state
                        .as_ref()
                        .and_then(|state| state.last_replication_object_digest.clone()),
                    backup_head_digest: prepared
                        .previous_state
                        .as_ref()
                        .and_then(|state| state.backup_head_digest.clone()),
                };
                let fence = Arc::new(
                    CellHaCommitFence::new(
                        Arc::clone(&prepared.policy),
                        activation,
                        Arc::new(SystemHaClock),
                    )?
                    .with_durable_state(prepared.state_store.clone(), initial_state)?,
                );
                if manifest.replication.role == CellReplicaRole::Primary {
                    database.install_commit_admission(fence.clone());
                }
                let object_cipher = database.bound_object_cipher().ok_or_else(|| {
                    CellError::RegulatedAdmissionIncomplete(
                        "Phase 5 HA requires a Cell-bound replication/backup cipher".to_string(),
                    )
                })?;
                Ok::<_, CellError>(CellHaRuntime {
                    policy: prepared.policy,
                    fence,
                    object_cipher,
                    database_path: database_path.clone(),
                    replica_signing_key: prepared.replica_signing_key,
                })
            })
            .transpose()?;
        let database = Arc::new(RwLock::new(database));
        let executes_applications = ha
            .as_ref()
            .is_none_or(|_| manifest.replication.role == CellReplicaRole::Primary);
        let mut application_host = if executes_applications {
            prepared_applications
                .map(|prepared| {
                    construct_application_host(
                        &volume_path,
                        &manifest.cell_id,
                        &verified.digest,
                        &key,
                        database.clone(),
                        prepared,
                    )
                })
                .transpose()?
        } else {
            None
        };
        if let Some(host) = &mut application_host {
            host.admission = prepared_admission
                .as_ref()
                .map(|prepared| prepared.lease.clone());
        }
        let device = if executes_applications {
            prepared_device
                .map(|prepared| {
                    let cell_id = DeviceId::parse(manifest.cell_id.as_str(), "cell_id")?;
                    let ledger =
                        DeviceParentLedger::open_cell_database(Arc::clone(&database), cell_id)?;
                    Ok::<_, CellError>(CellDeviceRuntime {
                        policy: prepared.policy,
                        exporter: prepared.exporter,
                        ledger,
                    })
                })
                .transpose()?
        } else {
            None
        };
        let grant = if executes_applications {
            prepared_grant
                .map(|prepared| {
                    let cell_id = GrantId::parse(manifest.cell_id.as_str(), "cell_id")?;
                    let source_ledger = GrantSourceLedger::open_cell_database(
                        Arc::clone(&database),
                        cell_id.clone(),
                    )?;
                    let recipient_ledger =
                        GrantRecipientLedger::open_cell_database(Arc::clone(&database), cell_id)?;
                    Ok::<_, CellError>(CellGrantRuntime {
                        policy: prepared.policy,
                        exporter: prepared.exporter,
                        recipient_private_key: prepared.recipient_private_key,
                        source_ledger,
                        recipient_ledger,
                    })
                })
                .transpose()?
        } else {
            None
        };
        if executes_applications {
            if let Some(prepared) = prepared_fleet.as_ref() {
                database.read().flush()?;
                record_fleet_convergence(
                    &volume_path,
                    &verified,
                    prepared,
                    convergence_started_at,
                    current_unix_timestamp(),
                )?;
            }
        }
        // The manifest becomes the active monotonic state only after every
        // pinned application has staged, activated, and passed readiness.
        // Persisting earlier could make an activation failure strand the cell
        // at a release which never actually became runnable.
        if let Some(admission) = &prepared_admission {
            admission.lease.check()?;
        }
        persist_local_manifest_state(&volume_path, &verified)?;
        if let Some(evidence) = rollback_evidence.as_ref() {
            persist_external_rollback_evidence(&volume_path, evidence)?;
        }
        Ok(Self {
            manifest: verified,
            volume_path,
            _runtime_lock: runtime_lock,
            database: Some(database),
            application_host,
            ha,
            device,
            grant,
            admission: prepared_admission.map(|prepared| prepared.lease),
            key_provider_kind,
        })
    }

    pub fn manifest(&self) -> &CellManifest {
        &self.manifest.manifest
    }

    pub fn manifest_digest(&self) -> &Sha256Digest {
        &self.manifest.digest
    }

    pub fn volume_path(&self) -> &Path {
        &self.volume_path
    }

    pub fn key_provider_kind(&self) -> &'static str {
        self.key_provider_kind
    }

    pub fn admission_report(&self) -> CellAdmissionReport {
        if self.manifest.manifest.uses_phase8_hardened_fleet() {
            let regulated_data_admitted = self
                .admission
                .as_ref()
                .is_some_and(|lease| lease.check().is_ok());
            CellAdmissionReport::phase8_hardened_fleet(regulated_data_admitted)
        } else if self.manifest.manifest.uses_phase7_cross_cell_grants() {
            CellAdmissionReport::phase7_cross_cell_grants()
        } else if self.manifest.manifest.uses_phase6_device_edge() {
            CellAdmissionReport::phase6_device_edge()
        } else if self.manifest.manifest.uses_phase5_cell_ha() {
            CellAdmissionReport::phase5_cell_ha()
        } else if self.manifest.manifest.uses_phase4_fleet_lifecycle() {
            CellAdmissionReport::phase4_fleet_lifecycle()
        } else if self.manifest.manifest.uses_phase3_cell_native() {
            CellAdmissionReport::phase3_cell_native()
        } else if self.manifest.manifest.uses_phase2_cryptography() {
            CellAdmissionReport::phase2_cryptographic_cell()
        } else if self.application_host.is_some() {
            CellAdmissionReport::application_host_preview()
        } else {
            CellAdmissionReport::phase1()
        }
    }

    pub fn verified_admission_evidence(&self) -> Option<&VerifiedAdmissionEvidence> {
        self.admission.as_ref().map(|lease| &lease.evidence)
    }

    /// Enforce continuous Phase-8 admission across all process activity. Keep
    /// this guard alive until after `close`; expiry terminates the process.
    /// Earlier Cell profiles return an inert guard.
    pub fn start_admission_watchdog(&self) -> Result<CellAdmissionWatchdog> {
        CellAdmissionWatchdog::start(self.admission.clone())
    }

    pub fn application_host(&self) -> Option<&CellApplicationHost> {
        self.application_host.as_ref()
    }

    pub fn ha_status(&self) -> Option<WriterFenceStatus> {
        self.ha.as_ref().map(|ha| ha.fence.status())
    }

    pub fn device_trust_policy(&self) -> Option<&DeviceTrustPolicy> {
        self.device.as_ref().map(|device| device.policy.as_ref())
    }

    fn device_runtime(&self) -> Result<&CellDeviceRuntime> {
        self.device.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "device-edge authority is unavailable on this profile or standby".to_string(),
            )
        })
    }

    fn verify_device_application_scope(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: Option<&CertifiedDeviceAuthorization>,
    ) -> Result<()> {
        if enrollment.statement.cell_id.as_str() != self.manifest.manifest.cell_id.as_str() {
            return Err(CellError::IdentityMismatch(
                "device enrollment belongs to another Cell".to_string(),
            ));
        }
        let application = self
            .manifest
            .manifest
            .applications
            .iter()
            .find(|application| {
                application.digest.as_str() == enrollment.statement.application_digest.as_str()
            })
            .ok_or_else(|| {
                CellError::ArtifactMismatch(
                    "device enrollment application is not pinned by this CellManifest".to_string(),
                )
            })?;
        if authorization.is_some_and(|authorization| {
            authorization.statement.schema_generation != application.schema_generation
        }) {
            return Err(CellError::ArtifactMismatch(
                "device authorization schema generation differs from its Cell application pin"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub fn provision_device_database_key(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        provisioning_id: DeviceId,
        key_epoch: u64,
        issued_at: i64,
    ) -> Result<ProvisionedDeviceDatabaseKey> {
        self.verify_device_application_scope(enrollment, None)?;
        Ok(self.device_runtime()?.exporter.provision_database_key(
            enrollment,
            provisioning_id,
            key_epoch,
            issued_at,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn export_device_working_set(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        package_id: DeviceId,
        package_sequence: u64,
        previous_package_digest: Option<DeviceDigest>,
        source_commit_seq: u64,
        objects: Vec<DeviceWorkingSetObject>,
        issued_at: i64,
        expires_at: i64,
    ) -> Result<SignedDeviceWorkingSet> {
        self.verify_device_application_scope(enrollment, Some(authorization))?;
        let device = self.device_runtime()?;
        let package = device.exporter.export_working_set(
            enrollment,
            authorization,
            package_id,
            package_sequence,
            previous_package_digest,
            source_commit_seq,
            objects,
            issued_at,
            expires_at,
        )?;
        device.ledger.record_working_set(
            device.policy.as_ref(),
            enrollment,
            authorization,
            &package,
            current_unix_timestamp(),
        )?;
        Ok(package)
    }

    pub fn activate_device_authorization(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        now: i64,
    ) -> Result<DeviceDigest> {
        self.verify_device_application_scope(enrollment, Some(authorization))?;
        Ok(self.device_runtime()?.ledger.activate_authorization(
            self.device_runtime()?.policy.as_ref(),
            enrollment,
            authorization,
            now,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn admit_device_amendment(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        amendment: &SignedDeviceAmendment,
        received_at: i64,
        current_object_digest: Option<DeviceDigest>,
    ) -> Result<AmendmentEvaluation> {
        self.verify_device_application_scope(enrollment, Some(authorization))?;
        let device = self.device_runtime()?;
        Ok(device.ledger.admit_amendment(
            device.policy.as_ref(),
            enrollment,
            authorization,
            amendment,
            received_at,
            current_object_digest,
        )?)
    }

    pub fn record_device_resolution(
        &self,
        resolution: &CertifiedAmendmentResolution,
    ) -> Result<DeviceDigest> {
        let device = self.device_runtime()?;
        Ok(device.ledger.record_resolution(
            device.policy.as_ref(),
            resolution,
            current_unix_timestamp(),
        )?)
    }

    pub fn retire_device(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        retirement: &CertifiedDeviceRetirement,
    ) -> Result<DeviceDigest> {
        self.verify_device_application_scope(enrollment, Some(authorization))?;
        let device = self.device_runtime()?;
        Ok(device.ledger.retire(
            device.policy.as_ref(),
            enrollment,
            authorization,
            retirement,
            current_unix_timestamp(),
        )?)
    }

    pub fn grant_trust_policy(&self) -> Option<&GrantTrustPolicy> {
        self.grant.as_ref().map(|grant| grant.policy.as_ref())
    }

    fn grant_runtime(&self) -> Result<&CellGrantRuntime> {
        self.grant.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "cross-cell grant authority is unavailable on this profile or standby".to_string(),
            )
        })
    }

    fn verify_grant_application_scope(
        &self,
        grant: &CertifiedCrossCellGrant,
        local_is_source: bool,
    ) -> Result<()> {
        let (cell_id, application_digest, schema_generation) = if local_is_source {
            (
                &grant.statement.source_cell_id,
                &grant.statement.source_application_digest,
                grant.statement.source_schema_generation,
            )
        } else {
            (
                &grant.statement.recipient_cell_id,
                &grant.statement.recipient_application_digest,
                grant.statement.recipient_schema_generation,
            )
        };
        if cell_id.as_str() != self.manifest.manifest.cell_id.as_str() {
            return Err(CellError::IdentityMismatch(
                "grant local role belongs to another Cell".to_string(),
            ));
        }
        let application = self
            .manifest
            .manifest
            .applications
            .iter()
            .find(|application| application.digest.as_str() == application_digest.as_str())
            .ok_or_else(|| {
                CellError::ArtifactMismatch(
                    "grant application is not pinned by this CellManifest".to_string(),
                )
            })?;
        if application.schema_generation != schema_generation {
            return Err(CellError::ArtifactMismatch(
                "grant schema generation differs from the local Cell application pin".to_string(),
            ));
        }
        Ok(())
    }

    pub fn activate_cross_cell_grant(
        &self,
        recipient_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        now: i64,
    ) -> Result<GrantDigest> {
        self.verify_grant_application_scope(grant, true)?;
        let runtime = self.grant_runtime()?;
        Ok(runtime.source_ledger.activate_grant(
            runtime.policy.as_ref(),
            recipient_policy,
            recipient_key,
            grant,
            now,
        )?)
    }

    pub fn accept_cross_cell_grant(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        now: i64,
    ) -> Result<GrantDigest> {
        self.verify_grant_application_scope(grant, false)?;
        let runtime = self.grant_runtime()?;
        Ok(runtime.recipient_ledger.accept_grant(
            source_policy,
            runtime.policy.as_ref(),
            recipient_key,
            grant,
            now,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn export_cross_cell_package(
        &self,
        recipient_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        package_id: GrantId,
        package_sequence: u64,
        previous_package_digest: Option<GrantDigest>,
        issued_at: i64,
        expires_at: i64,
        objects: Vec<GrantPlaintextObject>,
    ) -> Result<SignedGrantPackage> {
        self.verify_grant_application_scope(grant, true)?;
        let runtime = self.grant_runtime()?;
        let package = runtime.exporter.export(
            runtime.policy.as_ref(),
            recipient_policy,
            recipient_key,
            grant,
            package_id,
            package_sequence,
            previous_package_digest,
            issued_at,
            expires_at,
            objects,
        )?;
        runtime.source_ledger.record_package(
            runtime.policy.as_ref(),
            recipient_policy,
            recipient_key,
            grant,
            &package,
            issued_at,
        )?;
        Ok(package)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn import_cross_cell_package(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        package: &SignedGrantPackage,
        review: &CertifiedImportReview,
        now: i64,
    ) -> Result<Vec<ImportedGrantEvidence>> {
        self.verify_grant_application_scope(grant, false)?;
        let runtime = self.grant_runtime()?;
        Ok(runtime.recipient_ledger.import_package(
            source_policy,
            runtime.policy.as_ref(),
            recipient_key,
            &runtime.recipient_private_key,
            grant,
            package,
            review,
            now,
        )?)
    }

    pub fn revoke_outbound_cross_cell_grant(
        &self,
        grant: &CertifiedCrossCellGrant,
        revocation: &CertifiedGrantRevocation,
        now: i64,
    ) -> Result<GrantDigest> {
        self.verify_grant_application_scope(grant, true)?;
        let runtime = self.grant_runtime()?;
        Ok(runtime.source_ledger.record_revocation(
            runtime.policy.as_ref(),
            grant,
            revocation,
            now,
        )?)
    }

    pub fn record_inbound_cross_cell_revocation(
        &self,
        source_policy: &GrantTrustPolicy,
        grant: &CertifiedCrossCellGrant,
        revocation: &CertifiedGrantRevocation,
        now: i64,
    ) -> Result<GrantDigest> {
        self.verify_grant_application_scope(grant, false)?;
        Ok(self.grant_runtime()?.recipient_ledger.record_revocation(
            source_policy,
            grant,
            revocation,
            now,
        )?)
    }

    pub fn renew_ha_lease(&self, lease: &CertifiedReplicaLease) -> Result<()> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "HA lease renewal requires the Phase 5 runtime".to_string(),
            )
        })?;
        ha.fence.renew(lease)?;
        Ok(())
    }

    /// Quiesce the real database commit boundary, revoke the writer, flush all
    /// commits which passed admission, advance the external HA witness, then
    /// sign the exact final durable sequence. New commits cannot enter between
    /// these steps because the installed admission authority stays revoked.
    pub fn fence_primary(&self, nonce: String) -> Result<SignedFenceAck> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "writer fencing requires the Phase 5 runtime".to_string(),
            )
        })?;
        ha.fence.assert_live(ReplicaRole::Primary)?;
        let database = self
            .database
            .as_ref()
            .ok_or_else(|| CellError::StorageUnsafe("cell database is closed".to_string()))?;
        let database = database.read();
        database.fence_commit_admission()?;
        database.flush()?;
        let final_durable_commit_seq = database.last_commit_seq();
        ha.fence
            .checkpoint_durable_commit(final_durable_commit_seq)?;
        let statement = ha
            .fence
            .fence_ack_statement(final_durable_commit_seq, nonce)?;
        Ok(sign_fence_ack(statement, &ha.replica_signing_key)?)
    }

    /// Export Cell-bound, source-signed commit objects for one certified
    /// destination replica. Transport/orchestration receives ciphertext only.
    pub fn export_replication_commits(
        &self,
        destination_lease: &CertifiedReplicaLease,
        after_commit_seq: u64,
        predecessor_object_digest: Option<HaDigest>,
        limit: usize,
    ) -> Result<Vec<CellReplicationObject>> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Cell replication export requires the Phase 5 runtime".to_string(),
            )
        })?;
        ha.fence.assert_live(ReplicaRole::Primary)?;
        verify_replica_lease(&ha.policy, destination_lease, current_unix_timestamp())?;
        let local = ha.fence.status();
        let destination = &destination_lease.statement;
        if destination.cell_id != local.cell_id
            || destination.group_id != local.group_id
            || destination.replica_id == local.replica_id
            || destination.role != ReplicaRole::Standby
            || destination.writer_epoch != local.writer_epoch
            || destination.key_epoch != self.manifest.manifest.keys.key_epoch
            || destination.manifest_digest != cell_ha_configuration_digest(&self.manifest.manifest)?
        {
            return Err(CellError::IdentityMismatch(
                "replication destination lease is outside this exact Cell writer epoch".to_string(),
            ));
        }
        let frames = self
            .database
            .as_ref()
            .ok_or_else(|| CellError::StorageUnsafe("cell database is closed".to_string()))?
            .read()
            .export_replication_frames_since(after_commit_seq, limit)?;
        let mut predecessor = predecessor_object_digest;
        let mut objects = Vec::with_capacity(frames.len());
        for frame in frames {
            let expected_previous = after_commit_seq
                .checked_add(objects.len() as u64)
                .ok_or_else(|| {
                    CellError::Database(bicdb_core::BicDbError::HighAvailability(
                        "replication sequence overflowed".to_string(),
                    ))
                })?;
            if frame.previous_commit_seq != expected_previous
                || frame.commit_seq != expected_previous.saturating_add(1)
            {
                return Err(CellError::Database(
                    bicdb_core::BicDbError::HighAvailability(
                        "exported replication frames are not a contiguous suffix".to_string(),
                    ),
                ));
            }
            let plaintext = encode_ha_document(&frame)?;
            let mut header = ReplicationObjectHeader {
                format: REPLICATION_OBJECT_FORMAT.to_string(),
                kind: ReplicationObjectKind::Commit,
                cell_id: local.cell_id.clone(),
                group_id: local.group_id.clone(),
                source_replica_id: local.replica_id.clone(),
                destination_replica_id: destination.replica_id.clone(),
                writer_epoch: local.writer_epoch,
                key_epoch: self.manifest.manifest.keys.key_epoch,
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                commit_seq: frame.commit_seq,
                previous_commit_seq: frame.previous_commit_seq,
                predecessor_object_digest: predecessor.clone(),
                plaintext_digest: HaDigest::of_bytes(&plaintext),
                ciphertext_digest: HaDigest::of_bytes(&[]),
            };
            let aad = header.authenticated_data()?;
            let object_path = replication_object_logical_path(&ha.database_path, &header);
            let ciphertext = ha.object_cipher.seal_for(
                EncryptionObjectPurpose::Replication,
                &object_path,
                &plaintext,
                &aad,
            )?;
            header.ciphertext_digest = HaDigest::of_bytes(&ciphertext);
            let object = sign_replication_object(
                CellReplicationObject {
                    header,
                    source_lease_digest: local.lease_digest.clone(),
                    ciphertext,
                    signature: String::new(),
                },
                &ha.replica_signing_key,
            )?;
            predecessor = Some(object.digest()?);
            objects.push(object);
        }
        Ok(objects)
    }

    /// Authenticate, decrypt, continuity-check, and durably apply one commit
    /// object on its exact destination standby. A duplicate database apply can
    /// still repair a missing HA witness after a crash.
    pub fn apply_replication_commit(
        &self,
        source_lease: &CertifiedReplicaLease,
        object: &CellReplicationObject,
    ) -> Result<()> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Cell replication apply requires the Phase 5 runtime".to_string(),
            )
        })?;
        ha.fence.assert_live(ReplicaRole::Standby)?;
        verify_replication_object(&ha.policy, object, source_lease, current_unix_timestamp())?;
        let local = ha.fence.status();
        object.header.assert_scope(
            &local.cell_id,
            &local.group_id,
            &source_lease.statement.replica_id,
            &local.replica_id,
            local.writer_epoch,
            self.manifest.manifest.keys.key_epoch,
        )?;
        if object.header.kind != ReplicationObjectKind::Commit {
            return Err(CellError::DocumentInvalid(
                "commit apply refuses a non-commit replication object".to_string(),
            ));
        }
        let previous = ha.fence.durable_state().ok_or_else(|| {
            CellError::StorageUnsafe("standby has no durable HA witness".to_string())
        })?;
        if object.header.previous_commit_seq != previous.last_applied_commit_seq
            || object.header.commit_seq != previous.last_applied_commit_seq.saturating_add(1)
            || object.header.predecessor_object_digest != previous.last_replication_object_digest
        {
            return Err(CellError::ManifestRollback(
                "replication object is not the exact durable predecessor continuation".to_string(),
            ));
        }
        let aad = object.header.authenticated_data()?;
        let object_path = replication_object_logical_path(&ha.database_path, &object.header);
        let plaintext = ha.object_cipher.open_for(
            EncryptionObjectPurpose::Replication,
            &object_path,
            &object.ciphertext,
            &aad,
        )?;
        if HaDigest::of_bytes(&plaintext) != object.header.plaintext_digest {
            return Err(CellError::DocumentInvalid(
                "replication plaintext digest mismatch".to_string(),
            ));
        }
        let frame: CommitFrame = decode_ha_document(&plaintext)?;
        if frame.cluster_id != local.group_id
            || frame.source_node_id != source_lease.statement.replica_id.as_str()
            || frame.protocol_version != object.header.protocol_version
            || frame.commit_seq != object.header.commit_seq
            || frame.previous_commit_seq != object.header.previous_commit_seq
        {
            return Err(CellError::IdentityMismatch(
                "decrypted commit frame differs from its Cell replication header".to_string(),
            ));
        }
        self.database
            .as_ref()
            .ok_or_else(|| CellError::StorageUnsafe("cell database is closed".to_string()))?
            .write()
            .apply_replication_frame(&frame)?;
        ha.fence.checkpoint_replication_apply(
            frame.commit_seq,
            frame.previous_commit_seq,
            object.header.predecessor_object_digest.as_ref(),
            object.digest()?,
        )?;
        Ok(())
    }

    /// Materialize a full, encrypted Cell backup and separately wrap its
    /// one-time archive key under the Cell's Backup-purpose key. The result is
    /// only a candidate: Recovery quorum must certify the exact payload digest
    /// before it can advance the backup lineage or be restored.
    pub fn create_cell_backup(&self, backup_id: HaId) -> Result<CellBackupCandidate> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Cell backup requires the Phase 5 runtime".to_string(),
            )
        })?;
        let status = ha.fence.status();
        if status.role == ReplicaRole::Recovery {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "a recovery-only replica cannot originate a backup".to_string(),
            ));
        }
        ha.fence.assert_live(status.role)?;
        let backup_root =
            ensure_restricted_directory(&self.volume_path.join(CELL_HA_BACKUP_DIRECTORY))?;
        let (archive_path, wrapped_key_path) = cell_backup_artifact_paths(&backup_root, &backup_id);
        if archive_path.exists() || wrapped_key_path.exists() {
            return Err(CellError::ManifestRollback(
                "Cell backup identifier has already been used".to_string(),
            ));
        }

        let mut random = Zeroizing::new([0_u8; 32]);
        getrandom::fill(random.as_mut()).map_err(|error| {
            CellError::StorageUnsafe(format!("generate Cell backup key: {error}"))
        })?;
        let passphrase = Zeroizing::new(hex::encode(random.as_ref()));
        let database = self
            .database
            .as_ref()
            .ok_or_else(|| CellError::StorageUnsafe("cell database is closed".to_string()))?;
        let (report, backup_commit_seq) = database.read().create_consistent_backup(
            &archive_path,
            BackupCreateOptions {
                passphrase: passphrase.to_string(),
                base_backup: None,
            },
        )?;
        if !report.full {
            let _ = fs::remove_file(&archive_path);
            return Err(CellError::StorageUnsafe(
                "Cell backup unexpectedly produced a non-full archive".to_string(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&archive_path, fs::Permissions::from_mode(0o600))?;
        }
        let archive_digest = sha256_cell_backup_file(&archive_path)?;
        let source_manifest_digest = HaDigest::parse(self.manifest.digest.as_str())?;
        let binding = CellBackupKeyBinding {
            format: CELL_HA_BACKUP_KEY_FORMAT,
            backup_id: &backup_id,
            cell_id: &status.cell_id,
            group_id: &status.group_id,
            lineage_id: &self.manifest.manifest.storage.lineage_id,
            key_epoch: self.manifest.manifest.keys.key_epoch,
            manifest_generation: self.manifest.manifest.manifest_generation,
            manifest_digest: &source_manifest_digest,
            archive_digest: &archive_digest,
        };
        let aad = canonical_cbor(&binding)?;
        let logical_path = backup_key_logical_path(&ha.database_path, &backup_id);
        let wrapped_key = match ha.object_cipher.seal_for(
            EncryptionObjectPurpose::Backup,
            &logical_path,
            passphrase.as_bytes(),
            &aad,
        ) {
            Ok(value) => value,
            Err(error) => {
                let _ = fs::remove_file(&archive_path);
                return Err(error.into());
            }
        };
        if let Err(error) = write_new_restricted(&wrapped_key_path, &wrapped_key) {
            let _ = fs::remove_file(&archive_path);
            return Err(error);
        }
        sync_directory(&backup_root)?;
        let wrapped_key_digest = sha256_cell_backup_file(&wrapped_key_path)?;
        let payload_digest =
            cell_backup_payload_digest(&backup_id, &archive_digest, &wrapped_key_digest)?;
        let durable = ha.fence.durable_state().ok_or_else(|| {
            CellError::StorageUnsafe("Cell backup has no durable HA witness".to_string())
        })?;
        if durable.last_applied_commit_seq < backup_commit_seq {
            let _ = fs::remove_file(&wrapped_key_path);
            let _ = fs::remove_file(&archive_path);
            return Err(CellError::StorageUnsafe(
                "Cell backup sequence is ahead of the durable HA witness".to_string(),
            ));
        }
        Ok(CellBackupCandidate {
            manifest: CellBackupManifest {
                format: bicdb_cell_ha::BACKUP_MANIFEST_FORMAT.to_string(),
                backup_id,
                cell_id: status.cell_id,
                group_id: status.group_id,
                lineage_id: self.manifest.manifest.storage.lineage_id.clone(),
                source_replica_id: status.replica_id,
                writer_epoch: status.writer_epoch,
                key_epoch: self.manifest.manifest.keys.key_epoch,
                manifest_generation: self.manifest.manifest.manifest_generation,
                manifest_digest: source_manifest_digest,
                // `create_consistent_backup` returns the exact boundary held
                // while the archive was materialized. A later commit may
                // legitimately advance the witness before this method builds
                // its manifest; never mislabel that older archive as newer.
                durable_commit_seq: backup_commit_seq,
                predecessor_backup_digest: durable.backup_head_digest,
                encrypted_payload_digest: payload_digest,
                created_at: current_unix_timestamp(),
            },
            archive_path,
            wrapped_key_path,
            archive_digest,
            wrapped_key_digest,
        })
    }

    /// Verify Recovery quorum over the exact candidate and atomically advance
    /// the local anti-rollback backup lineage witness.
    pub fn finalize_cell_backup(
        &self,
        candidate: &CellBackupCandidate,
        certified: &CertifiedCellBackup,
    ) -> Result<HaDigest> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Cell backup certification requires the Phase 5 runtime".to_string(),
            )
        })?;
        if certified.manifest != candidate.manifest {
            return Err(CellError::ArtifactMismatch(
                "certified Cell backup differs from the local candidate".to_string(),
            ));
        }
        let backup_root =
            ensure_restricted_directory(&self.volume_path.join(CELL_HA_BACKUP_DIRECTORY))?;
        let expected_paths =
            cell_backup_artifact_paths(&backup_root, &candidate.manifest.backup_id);
        if candidate.archive_path != expected_paths.0
            || candidate.wrapped_key_path != expected_paths.1
            || sha256_cell_backup_file(&candidate.archive_path)? != candidate.archive_digest
            || sha256_cell_backup_file(&candidate.wrapped_key_path)? != candidate.wrapped_key_digest
            || cell_backup_payload_digest(
                &candidate.manifest.backup_id,
                &candidate.archive_digest,
                &candidate.wrapped_key_digest,
            )? != candidate.manifest.encrypted_payload_digest
        {
            return Err(CellError::ArtifactMismatch(
                "Cell backup payload changed before certification".to_string(),
            ));
        }
        let current = ha.fence.durable_state().ok_or_else(|| {
            CellError::StorageUnsafe("Cell backup has no durable HA witness".to_string())
        })?;
        if candidate.manifest.predecessor_backup_digest != current.backup_head_digest
            || candidate.manifest.cell_id != current.cell_id
            || candidate.manifest.group_id != current.group_id
            || candidate.manifest.source_replica_id != current.replica_id
            || candidate.manifest.writer_epoch != current.writer_epoch
            || candidate.manifest.durable_commit_seq > current.last_applied_commit_seq
            || candidate.manifest.key_epoch != self.manifest.manifest.keys.key_epoch
            || candidate.manifest.manifest_generation != self.manifest.manifest.manifest_generation
            || candidate.manifest.manifest_digest != HaDigest::parse(self.manifest.digest.as_str())?
            || candidate.manifest.lineage_id != self.manifest.manifest.storage.lineage_id
        {
            return Err(CellError::ManifestRollback(
                "Cell backup does not extend the exact current durable lineage".to_string(),
            ));
        }
        let digest = verify_cell_backup(&ha.policy, certified)?;
        ha.fence.checkpoint_backup(
            candidate.manifest.predecessor_backup_digest.as_ref(),
            digest.clone(),
        )?;
        Ok(digest)
    }

    /// Restore a Recovery-certified backup into a new, inert database
    /// directory. This never replaces the active database and never promotes
    /// the restored copy; a separately certified next writer epoch is still
    /// required before it can serve or accept writes.
    pub fn restore_cell_backup(
        &self,
        certified: &CertifiedCellBackup,
        authorization: &CertifiedRestoreAuthorization,
        target_relative: &Path,
    ) -> Result<BackupRestoreReport> {
        let ha = self.ha.as_ref().ok_or_else(|| {
            CellError::RegulatedAdmissionIncomplete(
                "Cell restore requires the Phase 5 runtime".to_string(),
            )
        })?;
        let status = ha.fence.status();
        if status.role != ReplicaRole::Recovery {
            return Err(CellError::RegulatedAdmissionIncomplete(
                "Cell restore requires a recovery-only replica lease".to_string(),
            ));
        }
        ha.fence.assert_live(ReplicaRole::Recovery)?;
        verify_restore_authorization(
            &ha.policy,
            certified,
            authorization,
            current_unix_timestamp(),
        )?;
        let backup = &certified.manifest;
        if backup.cell_id != status.cell_id
            || backup.group_id != status.group_id
            || backup.lineage_id != self.manifest.manifest.storage.lineage_id
            || backup.key_epoch != self.manifest.manifest.keys.key_epoch
            || authorization.authorization.target_replica_id != status.replica_id
            || authorization.authorization.next_writer_epoch != status.writer_epoch
            || backup.durable_commit_seq != ha.fence.writer_epoch().accepted_durable_commit_seq
        {
            return Err(CellError::IdentityMismatch(
                "restore is outside the exact Cell, lineage, key, replica, next epoch, or accepted durable history"
                    .to_string(),
            ));
        }
        let backup_root =
            ensure_restricted_directory(&self.volume_path.join(CELL_HA_BACKUP_DIRECTORY))?;
        let (archive_path, wrapped_key_path) =
            cell_backup_artifact_paths(&backup_root, &backup.backup_id);
        let archive_digest = sha256_cell_backup_file(&archive_path)?;
        let wrapped_key_digest = sha256_cell_backup_file(&wrapped_key_path)?;
        if cell_backup_payload_digest(&backup.backup_id, &archive_digest, &wrapped_key_digest)?
            != backup.encrypted_payload_digest
        {
            return Err(CellError::ArtifactMismatch(
                "restore backup payload does not match its Recovery-certified digest".to_string(),
            ));
        }
        let binding = CellBackupKeyBinding {
            format: CELL_HA_BACKUP_KEY_FORMAT,
            backup_id: &backup.backup_id,
            cell_id: &backup.cell_id,
            group_id: &backup.group_id,
            lineage_id: &backup.lineage_id,
            key_epoch: backup.key_epoch,
            manifest_generation: backup.manifest_generation,
            manifest_digest: &backup.manifest_digest,
            archive_digest: &archive_digest,
        };
        let aad = canonical_cbor(&binding)?;
        let wrapped_key = read_bounded_regular_file(&wrapped_key_path, 4096, true)?;
        let passphrase_bytes = Zeroizing::new(ha.object_cipher.open_for(
            EncryptionObjectPurpose::Backup,
            &backup_key_logical_path(&ha.database_path, &backup.backup_id),
            &wrapped_key,
            &aad,
        )?);
        let passphrase = Zeroizing::new(
            std::str::from_utf8(passphrase_bytes.as_ref())
                .map_err(|_| {
                    CellError::ArtifactMismatch(
                        "unwrapped Cell backup key is not the expected encoding".to_string(),
                    )
                })?
                .to_string(),
        );
        if passphrase.len() != 64
            || !passphrase
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(CellError::ArtifactMismatch(
                "unwrapped Cell backup key has the wrong shape".to_string(),
            ));
        }
        let restore_root =
            ensure_restricted_directory(&self.volume_path.join(CELL_HA_RESTORE_DIRECTORY))?;
        let target = safe_new_restore_target(&restore_root, target_relative)?;
        restore_backup(
            &archive_path,
            &target,
            BackupRestoreOptions {
                passphrase: passphrase.to_string(),
                force: false,
            },
        )
        .map_err(Into::into)
    }

    pub fn flush(&self) -> Result<()> {
        self.database
            .as_ref()
            .ok_or_else(|| CellError::StorageUnsafe("cell database is closed".to_string()))?
            .read()
            .flush()?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        drop(self.application_host.take());
        drop(self.device.take());
        drop(self.ha.take());
        if let Some(database) = self.database.take() {
            let database = Arc::try_unwrap(database).map_err(|_| {
                CellError::StorageUnsafe(
                    "cell application listener still holds the database; shut it down first"
                        .to_string(),
                )
            })?;
            database.into_inner().close()?;
        }
        Ok(())
    }
}

struct PreparedCellApplications {
    packages: Vec<ApplicationPackage>,
    verifier: PackageVerifier,
    authenticator: JwtAuthenticator,
    phase3: Option<PreparedPhase3Boundary>,
    phase1_development_providers: Option<CellPhase1DevelopmentProviders>,
}

struct PreparedPhase3Boundary {
    authorization: CellAuthorizationPolicy,
    frontend_assets: BTreeMap<String, PreparedFrontend>,
}

struct PreparedFleetActivation {
    verified: VerifiedFleetActivation,
    previous_applications: Vec<ApplicationTarget>,
}

fn fleet_digest(digest: &Sha256Digest) -> Result<FleetDigest> {
    Ok(FleetDigest::parse(digest.as_str())?)
}

fn cell_digest(digest: &FleetDigest) -> Result<Sha256Digest> {
    Sha256Digest::parse(digest.as_str())
}

fn application_scope_name(scope: ApplicationExecutionScope) -> &'static str {
    match scope {
        ApplicationExecutionScope::Global => "global",
        ApplicationExecutionScope::Cell => "cell",
        ApplicationExecutionScope::Device => "device",
    }
}

fn application_data_class_name(data_class: ApplicationDataClass) -> &'static str {
    match data_class {
        ApplicationDataClass::Public => "public",
        ApplicationDataClass::Operational => "operational",
        ApplicationDataClass::Sensitive => "sensitive",
        ApplicationDataClass::Regulated => "regulated",
        ApplicationDataClass::RegulatedLocal => "regulated_local",
    }
}

fn fleet_application_targets(manifest: &CellManifest) -> Result<Vec<ApplicationTarget>> {
    manifest
        .applications
        .iter()
        .map(|application| {
            Ok(ApplicationTarget {
                root: application.root.clone(),
                name: application.name.clone(),
                version: application.version.clone(),
                artifact_digest: fleet_digest(&application.digest)?,
                schema_generation: application.schema_generation,
                execution_scope: application_scope_name(application.scope).to_string(),
                data_class: application_data_class_name(application.data_class).to_string(),
            })
        })
        .collect()
}

fn prepare_fleet_activation(
    manifest: &VerifiedCellManifest,
    volume_path: &Path,
    host_config: Option<&CellApplicationHostConfig>,
    trusted_manifest_keys: &TrustedManifestKeys,
) -> Result<Option<PreparedFleetActivation>> {
    if !manifest.manifest.uses_fleet_lifecycle() {
        if host_config.is_some_and(|config| {
            config.fleet_trust_policy_path.is_some()
                || config.fleet_activation_bundle_path.is_some()
                || config.previous_manifest_path.is_some()
        }) {
            return Err(CellError::ArtifactMismatch(
                "fleet lifecycle inputs require a coherent Phase 4-or-later profile".to_string(),
            ));
        }
        return Ok(None);
    }
    let host_config = host_config.ok_or_else(|| {
        CellError::ArtifactMismatch(
            "Phase 4-or-later runtime requires application and fleet inputs".to_string(),
        )
    })?;
    let policy_path = host_config
        .fleet_trust_policy_path
        .as_ref()
        .ok_or_else(|| {
            CellError::ArtifactMismatch(
                "Phase 4-or-later runtime requires --fleet-trust-policy".to_string(),
            )
        })?;
    let bundle_path = host_config
        .fleet_activation_bundle_path
        .as_ref()
        .ok_or_else(|| {
            CellError::ArtifactMismatch(
                "Phase 4-or-later runtime requires --fleet-activation-bundle".to_string(),
            )
        })?;
    let policy_digest = manifest
        .manifest
        .policy
        .fleet_trust_policy_digest
        .as_ref()
        .expect("Phase 4 manifest validation requires fleet policy pin");
    let policy: FleetTrustPolicy =
        load_pinned_json(policy_path, policy_digest, "fleet trust policy")?;
    if policy.format != FLEET_TRUST_POLICY_FORMAT {
        return Err(CellError::ArtifactMismatch(
            "fleet trust policy has an unsupported format".to_string(),
        ));
    }
    let bundle_bytes =
        read_bounded_regular_file(bundle_path, MAX_FLEET_ACTIVATION_BUNDLE_BYTES, false)?;
    let bundle: FleetActivationBundle = serde_json::from_slice(&bundle_bytes).map_err(|error| {
        CellError::ArtifactMismatch(format!("decode fleet activation bundle: {error}"))
    })?;

    let (previous_manifest_generation, previous_manifest_digest, previous_applications) =
        if manifest.manifest.manifest_generation == 1 {
            if host_config.previous_manifest_path.is_some() {
                return Err(CellError::ArtifactMismatch(
                    "initial Phase 4 manifest cannot name a predecessor manifest".to_string(),
                ));
            }
            (0, None, Vec::new())
        } else {
            let previous_path = host_config.previous_manifest_path.as_ref().ok_or_else(|| {
                CellError::ArtifactMismatch(
                    "non-initial Phase 4 activation requires the signed predecessor manifest"
                        .to_string(),
                )
            })?;
            let previous = load_verified_manifest(previous_path, trusted_manifest_keys)?;
            if previous.manifest.cell_id != manifest.manifest.cell_id
                || previous.manifest.manifest_generation.saturating_add(1)
                    != manifest.manifest.manifest_generation
                || manifest.manifest.previous_manifest_digest.as_ref() != Some(&previous.digest)
            {
                return Err(CellError::ManifestRollback(
                    "Phase 4 predecessor does not exactly link this Cell manifest transition"
                        .to_string(),
                ));
            }
            (
                previous.manifest.manifest_generation,
                Some(fleet_digest(&previous.digest)?),
                fleet_application_targets(&previous.manifest)?,
            )
        };
    let context = ActivationContext {
        cell_id: FleetCellId::parse(manifest.manifest.cell_id.as_str())?,
        previous_manifest_generation,
        previous_manifest_digest,
        previous_applications: previous_applications.clone(),
        next_manifest_generation: manifest.manifest.manifest_generation,
        next_manifest_digest: fleet_digest(&manifest.digest)?,
        next_applications: fleet_application_targets(&manifest.manifest)?,
        bicdb_version: env!("CARGO_PKG_VERSION").to_string(),
        now: current_unix_timestamp(),
        // Never trust a fleet-supplied dormancy claim. Derive it from the
        // Cell-local, hash-chained convergence ledger instead.
        dormant_since: latest_convergence_completed_at(volume_path)?,
    };
    Ok(Some(PreparedFleetActivation {
        verified: verify_activation_bundle(&policy, &bundle, &context)?,
        previous_applications,
    }))
}

fn record_fleet_convergence(
    volume_path: &Path,
    manifest: &VerifiedCellManifest,
    prepared: &PreparedFleetActivation,
    started_at: i64,
    completed_at: i64,
) -> Result<()> {
    let ledger = ConvergenceLedger::open(volume_path.join(CELL_CONVERGENCE_LEDGER_DIRECTORY))?;
    let latest = ledger.latest()?;
    let active_manifest_digest = fleet_digest(&manifest.digest)?;
    if latest.as_ref().is_some_and(|(_, receipt)| {
        receipt.active_manifest_digest == active_manifest_digest
            && receipt.release_digest == prepared.verified.release_digest
    }) {
        return Ok(());
    }
    let previous = prepared
        .previous_applications
        .iter()
        .map(|application| (application.application_id(), application.clone()))
        .collect::<BTreeMap<_, _>>();
    let applications = prepared
        .verified
        .applications
        .iter()
        .map(|active| ApplicationConvergence {
            application_id: active.application_id(),
            previous: previous.get(&active.application_id()).cloned(),
            active: active.clone(),
        })
        .collect();
    let receipt = ConvergenceReceipt {
        format: CONVERGENCE_RECEIPT_FORMAT.to_string(),
        cell_id: FleetCellId::parse(manifest.manifest.cell_id.as_str())?,
        sequence: latest
            .as_ref()
            .map(|(_, receipt)| receipt.sequence.saturating_add(1))
            .unwrap_or(1),
        previous_receipt_digest: latest.as_ref().map(|(digest, _)| digest.clone()),
        previous_manifest_digest: manifest
            .manifest
            .previous_manifest_digest
            .as_ref()
            .map(fleet_digest)
            .transpose()?,
        active_manifest_digest,
        release_digest: prepared.verified.release_digest.clone(),
        rollout_id: prepared.verified.rollout_id.clone(),
        cohort_index: prepared.verified.cohort_index,
        started_at,
        completed_at: completed_at.max(started_at),
        rollback_until: prepared.verified.rollback_until,
        applications,
    };
    ledger.append(&receipt)?;
    Ok(())
}

fn prepare_cell_applications(
    artifact_root: &Path,
    manifest: &CellManifest,
    host_config: Option<&CellApplicationHostConfig>,
    regulated_data_admitted: bool,
) -> Result<Option<PreparedCellApplications>> {
    if manifest.applications.is_empty() {
        if host_config.is_some() {
            return Err(CellError::DocumentInvalid(
                "application policies were supplied but the manifest pins no applications"
                    .to_string(),
            ));
        }
        // Phase 1 still validates that the operator supplied a real artifact
        // directory even though there is nothing to open from it.
        canonical_directory_without_symlink(artifact_root)?;
        return Ok(None);
    }
    let host_config = host_config.ok_or_else(|| {
        CellError::ArtifactMismatch(
            "signed application pins require release, identity, and egress policies".to_string(),
        )
    })?;

    let release_policy: CellReleasePolicy = load_pinned_json(
        &host_config.release_policy_path,
        &manifest.policy.trusted_release_policy_digest,
        "release policy",
    )?;
    if release_policy.format != CELL_RELEASE_POLICY_FORMAT
        || release_policy.required_package_signatures != 1
        || release_policy.roots.is_empty()
    {
        return Err(CellError::ArtifactMismatch(
            "release policy must use v1, require one package signature, and declare roots"
                .to_string(),
        ));
    }

    let identity_policy: CellIdentityPolicy = load_pinned_json(
        &host_config.identity_policy_path,
        &manifest.policy.identity_policy_digest,
        "identity policy",
    )?;
    if identity_policy.format != CELL_IDENTITY_POLICY_FORMAT
        || !identity_policy.issuer.starts_with("https://")
        || identity_policy.issuer.chars().any(char::is_whitespace)
        || identity_policy.audience.trim().is_empty()
        || identity_policy.authentication_method != "oidc-ed25519"
        || identity_policy.maximum_lifetime_seconds <= 0
        || identity_policy.maximum_lifetime_seconds > MAX_CELL_ACCESS_TOKEN_LIFETIME_SECONDS
        || identity_policy.clock_skew_seconds < 0
        || identity_policy.clock_skew_seconds > MAX_CELL_IDENTITY_CLOCK_SKEW_SECONDS
    {
        return Err(CellError::ArtifactMismatch(
            "identity policy has an invalid format or verifier bounds".to_string(),
        ));
    }
    let authenticator = JwtAuthenticator::oidc_ed25519(
        JwtConfiguration {
            issuer: identity_policy.issuer.clone(),
            audience: identity_policy.audience.clone(),
            authentication_method: identity_policy.authentication_method.clone(),
            maximum_lifetime_seconds: identity_policy.maximum_lifetime_seconds,
            clock_skew_seconds: identity_policy.clock_skew_seconds,
        },
        &serde_json::to_vec(&identity_policy.oidc_ed25519_jwks).map_err(|error| {
            CellError::ArtifactMismatch(format!("encode identity-policy JWKS: {error}"))
        })?,
    )?;

    let (authorization_policy, feature_certification) = if manifest.uses_cell_native() {
        if identity_policy.maximum_lifetime_seconds > MAX_CELL_HANDOFF_LIFETIME_SECONDS {
            return Err(CellError::ArtifactMismatch(format!(
                "Phase 3 handoff assertions may live for at most {MAX_CELL_HANDOFF_LIFETIME_SECONDS} seconds"
            )));
        }
        let authorization_digest = manifest
            .policy
            .authorization_policy_digest
            .as_ref()
            .expect("Phase 3 manifest validation requires authorization digest");
        let authorization_path =
            host_config
                .authorization_policy_path
                .as_ref()
                .ok_or_else(|| {
                    CellError::ArtifactMismatch(
                        "cell-native profiles require the manifest-pinned cell authorization policy"
                            .to_string(),
                    )
                })?;
        let authorization: CellAuthorizationPolicy = load_pinned_json(
            authorization_path,
            authorization_digest,
            "authorization policy",
        )?;
        validate_authorization_policy(&authorization, &manifest.cell_id)?;

        let certification_digest = manifest
            .policy
            .feature_certification_digest
            .as_ref()
            .expect("Phase 3 manifest validation requires feature certification digest");
        let certification_path =
            host_config
                .feature_certification_path
                .as_ref()
                .ok_or_else(|| {
                    CellError::ArtifactMismatch(
                        "cell-native profiles require manifest-pinned binary feature certification"
                            .to_string(),
                    )
                })?;
        let certification: CellFeatureCertification = load_pinned_json(
            certification_path,
            certification_digest,
            "feature certification",
        )?;
        validate_feature_certification(&certification, manifest)?;
        (Some(authorization), Some(certification))
    } else {
        if host_config.authorization_policy_path.is_some()
            || host_config.feature_certification_path.is_some()
        {
            return Err(CellError::ArtifactMismatch(
                "authorization and feature certification policies require the Phase 3 profile"
                    .to_string(),
            ));
        }
        (None, None)
    };

    let egress_policy: CellEgressPolicy = load_pinned_json(
        &host_config.egress_policy_path,
        &manifest.policy.egress_policy_digest,
        "egress policy",
    )?;
    if egress_policy.format != CELL_EGRESS_POLICY_FORMAT
        || egress_policy.mode != CellEgressMode::DenyAll
    {
        return Err(CellError::ArtifactMismatch(
            "this cell runtime supports only the manifest-pinned deny_all egress policy"
                .to_string(),
        ));
    }
    if egress_policy.phase1_development_providers.is_some()
        && (manifest.uses_bound_cryptography()
            || manifest.applications.iter().any(|application| {
                matches!(
                    application.data_class,
                    ApplicationDataClass::Regulated | ApplicationDataClass::RegulatedLocal
                )
            }))
    {
        return Err(CellError::ArtifactMismatch(
            "development provider bindings are restricted to non-regulated Phase-1 Cells"
                .to_string(),
        ));
    }

    let mut roots = BTreeMap::<String, TrustedSigningKeys>::new();
    let mut all_keys = TrustedSigningKeys::default();
    let mut global_key_ids = BTreeSet::new();
    for root in release_policy.roots {
        if root.root.trim().is_empty()
            || root.signing_keys.is_empty()
            || roots.contains_key(&root.root)
        {
            return Err(CellError::ArtifactMismatch(
                "release roots must have unique non-empty names and signing keys".to_string(),
            ));
        }
        let mut root_keys = TrustedSigningKeys::default();
        for (key_id, encoded_key) in root.signing_keys {
            validate_signer_key_id(&key_id)?;
            if !global_key_ids.insert(key_id.clone()) {
                return Err(CellError::ArtifactMismatch(format!(
                    "application signing key id {key_id} is ambiguous across release roots"
                )));
            }
            if encoded_key != encoded_key.to_ascii_lowercase() || encoded_key.len() != 64 {
                return Err(CellError::ArtifactMismatch(format!(
                    "application signing key {key_id} must be 64 lowercase hex digits"
                )));
            }
            let bytes = hex::decode(&encoded_key).map_err(|_| {
                CellError::ArtifactMismatch(format!(
                    "application signing key {key_id} is not hexadecimal"
                ))
            })?;
            root_keys.insert_ed25519(key_id.clone(), &bytes)?;
            all_keys.insert_ed25519(key_id, &bytes)?;
        }
        roots.insert(root.root, root_keys);
    }

    let artifact_root = canonical_directory_without_symlink(artifact_root)?;
    let mut packages = Vec::with_capacity(manifest.applications.len());
    let mut frontend_assets = BTreeMap::new();
    for pin in &manifest.applications {
        let path = artifact_root.join(format!("{}.bicdb-app", pin.digest.hex()));
        let bytes = read_bounded_regular_file(&path, MAX_APPLICATION_PACKAGE_BYTES, false)?;
        if Sha256Digest::of_bytes(&bytes) != pin.digest {
            return Err(CellError::ArtifactMismatch(format!(
                "application {} digest mismatch",
                pin.name
            )));
        }
        let package: ApplicationPackage = serde_json::from_slice(&bytes).map_err(|error| {
            CellError::ArtifactMismatch(format!(
                "application {} package is invalid JSON: {error}",
                pin.name
            ))
        })?;
        let root_keys = roots.get(&pin.root).ok_or_else(|| {
            CellError::ArtifactMismatch(format!(
                "application {} references unknown release root {}",
                pin.name, pin.root
            ))
        })?;
        let verification =
            PackageVerifier::new(root_keys.clone(), MAX_APPLICATION_PACKAGE_BYTES as usize)?
                .verify(&package)?;
        let application = package.manifest.application.as_deref().ok_or_else(|| {
            CellError::ArtifactMismatch(format!(
                "application {} has no ABI-v2 application contract",
                pin.name
            ))
        })?;
        if let Some(route) = application.routes.iter().find(|route| route.public) {
            return Err(CellError::ArtifactMismatch(format!(
                "cell application {} declares public route {}; cell application routes must authenticate",
                pin.name, route.name
            )));
        }
        for (scheme_name, scheme) in &application.auth_schemes {
            if scheme.kind != ApplicationAuthKindV1::OidcEd25519
                || scheme.issuer != identity_policy.issuer
                || scheme.audience != identity_policy.audience
            {
                return Err(CellError::ArtifactMismatch(format!(
                    "cell application {} authentication scheme {} does not match the pinned identity policy",
                    pin.name, scheme_name
                )));
            }
        }
        if package.manifest.identity.name != pin.name
            || package.manifest.identity.version != pin.version
            || application.package.application != pin.name
            || application.package.version != pin.version
        {
            return Err(CellError::ArtifactMismatch(format!(
                "application {} identity/version does not match its cell pin",
                pin.name
            )));
        }
        let schema_generation = application
            .migrations
            .iter()
            .map(|migration| migration.schema_version)
            .chain(
                application
                    .resources
                    .iter()
                    .map(|resource| resource.schema_version),
            )
            .max()
            .unwrap_or(1);
        if schema_generation != pin.schema_generation {
            return Err(CellError::ArtifactMismatch(format!(
                "application {} schema generation is {}, pin requires {}",
                pin.name, schema_generation, pin.schema_generation
            )));
        }
        if verification.signing_key_id.trim().is_empty() {
            return Err(CellError::ArtifactMismatch(format!(
                "application {} has no release signing identity",
                pin.name
            )));
        }
        if let Some(certification) = feature_certification.as_ref() {
            validate_phase3_package(pin, &package, certification, regulated_data_admitted)?;
            frontend_assets.insert(
                pin.name.clone(),
                PreparedFrontend {
                    package_sha256: application.package.package_sha256.clone(),
                    assets: package.frontend_assets.clone(),
                },
            );
        }
        packages.push(package);
    }

    if let Some(development) = egress_policy.phase1_development_providers.as_ref() {
        validate_phase1_development_providers(development, &packages)?;
    }

    if authorization_policy.is_some() && frontend_assets.is_empty() {
        return Err(CellError::ArtifactMismatch(
            "Phase 3 requires at least one signed same-origin frontend".to_string(),
        ));
    }

    Ok(Some(PreparedCellApplications {
        packages,
        verifier: PackageVerifier::new(all_keys, MAX_APPLICATION_PACKAGE_BYTES as usize)?,
        authenticator,
        phase3: authorization_policy.map(|authorization| PreparedPhase3Boundary {
            authorization,
            frontend_assets,
        }),
        phase1_development_providers: egress_policy.phase1_development_providers,
    }))
}

fn validate_phase1_development_providers(
    policy: &CellPhase1DevelopmentProviders,
    packages: &[ApplicationPackage],
) -> Result<()> {
    let applications = packages
        .iter()
        .filter_map(|package| {
            package
                .manifest
                .application
                .as_deref()
                .map(|application| (package.manifest.identity.name.as_str(), application))
        })
        .collect::<Vec<_>>();
    for secret_name in &policy.derived_secrets {
        if secret_name.trim().is_empty() {
            return Err(CellError::ArtifactMismatch(
                "development derived-secret names must be non-empty".to_string(),
            ));
        }
        let mut matched = false;
        for (_, application) in &applications {
            for declaration in application
                .secrets
                .iter()
                .filter(|declaration| &declaration.name == secret_name)
            {
                matched = true;
                let allowed = declaration.operations.iter().all(|operation| {
                    matches!(
                        operation,
                        bicdb_extension::abi_v2::CryptoOperation::Metadata
                            | bicdb_extension::abi_v2::CryptoOperation::Encrypt
                            | bicdb_extension::abi_v2::CryptoOperation::Decrypt
                    )
                });
                if declaration.allow_plaintext_read || !allowed {
                    return Err(CellError::ArtifactMismatch(format!(
                        "development derived secret `{secret_name}` may authorize only metadata/encrypt/decrypt operations"
                    )));
                }
            }
        }
        if !matched {
            return Err(CellError::ArtifactMismatch(format!(
                "development derived secret `{secret_name}` is not declared by a pinned application"
            )));
        }
    }
    validate_development_bindings("egress", &policy.deny_egress, |application, provider| {
        applications.iter().any(|(name, contract)| {
            *name == application
                && contract
                    .egress
                    .iter()
                    .any(|declaration| declaration.provider.as_deref() == Some(provider))
        })
    })?;
    validate_development_bindings(
        "tokenizer",
        &policy.deterministic_tokenizers,
        |application, provider| {
            applications.iter().any(|(name, contract)| {
                *name == application
                    && contract
                        .application_program
                        .as_ref()
                        .and_then(|program| program.tokenizer.as_ref())
                        .is_some_and(|tokenizer| tokenizer.providers.contains_key(provider))
            })
        },
    )?;
    validate_development_bindings("LLM", &policy.deny_llm, |application, provider| {
        applications.iter().any(|(name, contract)| {
            *name == application
                && contract
                    .application_program
                    .as_ref()
                    .and_then(|program| program.llm.as_ref())
                    .is_some_and(|llm| {
                        llm.clients
                            .values()
                            .any(|client| client.provider == provider)
                    })
        })
    })?;
    Ok(())
}

fn validate_development_bindings(
    label: &str,
    bindings: &BTreeSet<CellDevelopmentProviderBinding>,
    declared: impl Fn(&str, &str) -> bool,
) -> Result<()> {
    for binding in bindings {
        if binding.application.trim().is_empty()
            || binding.provider.trim().is_empty()
            || !declared(&binding.application, &binding.provider)
        {
            return Err(CellError::ArtifactMismatch(format!(
                "development {label} binding `{}:{}` is not declared by a pinned application",
                binding.application, binding.provider
            )));
        }
    }
    Ok(())
}

fn validate_authorization_policy(
    policy: &CellAuthorizationPolicy,
    expected_cell_id: &CellId,
) -> Result<()> {
    if policy.format != CELL_AUTHORIZATION_POLICY_FORMAT
        || &policy.cell_id != expected_cell_id
        || policy.authorization_epoch == 0
        || policy.session_lifetime_seconds <= 0
        || policy.session_lifetime_seconds > MAX_CELL_SESSION_LIFETIME_SECONDS
        || policy.minimum_assurance.trim().is_empty()
        || policy.members.is_empty()
    {
        return Err(CellError::ArtifactMismatch(
            "cell authorization policy has an invalid identity, epoch, assurance, or session lifetime"
                .to_string(),
        ));
    }
    let mut users = BTreeSet::new();
    let mut devices = BTreeSet::new();
    for member in &policy.members {
        if member.user_id.trim().is_empty()
            || !users.insert(member.user_id.clone())
            || member.roles.iter().any(|role| role.trim().is_empty())
            || member.scopes.iter().any(|scope| scope.trim().is_empty())
            || member.devices.is_empty()
        {
            return Err(CellError::ArtifactMismatch(
                "cell authorization members require unique identities, valid authority, and at least one device"
                    .to_string(),
            ));
        }
        for device in &member.devices {
            if device.device_id.trim().is_empty()
                || !devices.insert(device.device_id.clone())
                || device.public_key != device.public_key.to_ascii_lowercase()
                || device.public_key.len() != 64
                || hex::decode(&device.public_key)
                    .ok()
                    .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                    .and_then(|bytes| VerifyingKey::from_bytes(&bytes).ok())
                    .is_none()
            {
                return Err(CellError::ArtifactMismatch(
                    "cell authorization devices require globally unique ids and valid Ed25519 public keys"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_feature_certification(
    certification: &CellFeatureCertification,
    manifest: &CellManifest,
) -> Result<()> {
    if certification.format != CELL_FEATURE_CERTIFICATION_FORMAT
        || certification.bicdb_binary_digest != manifest.runtime.bicdb_binary_digest
        || certification.database_format != manifest.storage.database_format
        || certification.certified_features.is_empty()
    {
        return Err(CellError::ArtifactMismatch(
            "feature certification must bind this exact BicDB binary/database format and certify at least one feature"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_phase3_package(
    pin: &CellApplicationPin,
    package: &ApplicationPackage,
    certification: &CellFeatureCertification,
    regulated_data_admitted: bool,
) -> Result<()> {
    let application = package
        .manifest
        .application
        .as_deref()
        .expect("package verifier requires an application contract");
    if package.components.is_empty() {
        return Err(CellError::ArtifactMismatch(format!(
            "Phase 3 application {} has no signed component contracts",
            pin.name
        )));
    }
    if !application.auth_schemes.is_empty() {
        return Err(CellError::ArtifactMismatch(format!(
            "Phase 3 application {} must use cell-local host authentication, not a package-selected verifier",
            pin.name
        )));
    }
    let mut capabilities = BTreeSet::new();
    let mut database_features = BTreeSet::new();
    for component in &package.components {
        if component.scope != ApplicationExecutionScope::Cell
            || component.scope != pin.scope
            || component.data_class != pin.data_class
            || !component.egress.is_empty()
            || (component.data_class.is_regulated() && !regulated_data_admitted)
        {
            return Err(CellError::ArtifactMismatch(format!(
                "application {} component {} differs from its cell scope/data-class pin, requests egress, or requests regulated data without verified admission",
                pin.name, component.name
            )));
        }
        capabilities.extend(component.capabilities.iter().copied());
        database_features.extend(component.database_features.iter().copied());
    }
    let mut required = BTreeSet::new();
    let mut required_database_features = BTreeSet::new();
    if !application.resources.is_empty()
        || !application.relation_permissions.is_empty()
        || !application.migrations.is_empty()
    {
        required.insert(ApplicationCapability::Database);
    }
    if !application.routes.is_empty() {
        required.insert(ApplicationCapability::HttpRoutes);
    }
    if !package.frontend_assets.is_empty() {
        required.insert(ApplicationCapability::FrontendAssets);
    }
    if !application.workers.is_empty() || !application.schedules.is_empty() {
        required.insert(ApplicationCapability::Jobs);
        required_database_features.insert(ApplicationDatabaseFeature::Jobs);
    }
    if !application.secrets.is_empty() {
        required.insert(ApplicationCapability::Secrets);
    }
    if !application.blobs.is_empty() {
        required.insert(ApplicationCapability::Blobs);
    }
    if !application.egress.is_empty() {
        required.insert(ApplicationCapability::Egress);
    }
    if !application.raw_sql.is_empty() {
        required.insert(ApplicationCapability::RawSql);
        if application
            .raw_sql
            .iter()
            .any(|statement| !statement.routines.is_empty())
        {
            required_database_features.insert(ApplicationDatabaseFeature::StoredFunctions);
        }
    }
    if application
        .resources
        .iter()
        .any(|resource| resource.policy.is_some())
    {
        required_database_features.insert(ApplicationDatabaseFeature::RowLevelSecurity);
    }
    if !application.invariants.is_empty()
        || application
            .application_program
            .as_ref()
            .is_some_and(|program| !program.mutation_bindings.is_empty())
    {
        required_database_features.insert(ApplicationDatabaseFeature::Triggers);
    }
    if capabilities != required {
        return Err(CellError::ArtifactMismatch(format!(
            "application {} component capability union differs from the executable package surface",
            pin.name
        )));
    }
    if capabilities.iter().any(|capability| {
        matches!(
            capability,
            ApplicationCapability::Secrets
                | ApplicationCapability::Blobs
                | ApplicationCapability::Egress
                | ApplicationCapability::RawSql
        )
    }) {
        return Err(CellError::ArtifactMismatch(format!(
            "application {} requests an ambient or incomplete Phase 3 provider",
            pin.name
        )));
    }
    if database_features != required_database_features {
        return Err(CellError::ArtifactMismatch(format!(
            "application {} database feature contracts differ from the executable package surface",
            pin.name
        )));
    }
    if capabilities.contains(&ApplicationCapability::FrontendAssets)
        && (!package.frontend_assets.contains_key("index.html")
            || !package.components.iter().any(|component| {
                component.kind == ApplicationComponentKind::Frontend
                    && component
                        .capabilities
                        .contains(&ApplicationCapability::FrontendAssets)
            }))
    {
        return Err(CellError::ArtifactMismatch(format!(
            "application {} frontend requires a signed frontend component and index.html",
            pin.name
        )));
    }
    if !package.components.iter().any(|component| {
        component.kind == ApplicationComponentKind::Backend
            && component
                .capabilities
                .contains(&ApplicationCapability::HttpRoutes)
    }) && required.contains(&ApplicationCapability::HttpRoutes)
    {
        return Err(CellError::ArtifactMismatch(format!(
            "application {} HTTP routes have no signed backend component",
            pin.name
        )));
    }
    if !database_features.is_subset(&certification.certified_features) {
        return Err(CellError::ArtifactMismatch(format!(
            "application {} requires database features absent from the exact-binary certification",
            pin.name
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct CellDevelopmentDenyEgressProvider {
    bindings: BTreeSet<CellDevelopmentProviderBinding>,
}

impl EgressProvider for CellDevelopmentDenyEgressProvider {
    fn available(
        &self,
        application: &str,
        policy: &EgressDeclaration,
    ) -> bicdb_app_runtime::Result<()> {
        let provider = policy.provider.as_deref().unwrap_or_default();
        if self.bindings.contains(&CellDevelopmentProviderBinding {
            application: application.to_string(),
            provider: provider.to_string(),
        }) {
            Ok(())
        } else {
            Err(AppRuntimeError::Provider(format!(
                "Cell development egress provider `{provider}` is unavailable for application `{application}`"
            )))
        }
    }

    fn execute(
        &self,
        application: &str,
        policy: &EgressDeclaration,
        _mtls_secret: Option<&SecretRecord>,
        _method: &str,
        _url: &str,
        _headers: &[(String, String)],
        _body: &[u8],
        _timeout_ms: u64,
    ) -> bicdb_app_runtime::Result<EgressResponse> {
        self.available(application, policy)?;
        Err(AppRuntimeError::CapabilityDenied(format!(
            "network egress `{}` is disabled by the Cell's signed Phase-1 development policy",
            policy.name
        )))
    }
}

#[derive(Clone, Debug)]
struct CellDevelopmentTokenizerProvider {
    bindings: BTreeSet<CellDevelopmentProviderBinding>,
}

impl TokenizerProvider for CellDevelopmentTokenizerProvider {
    fn available(&self, application: &str, provider: &str) -> bicdb_app_runtime::Result<()> {
        if self.bindings.contains(&CellDevelopmentProviderBinding {
            application: application.to_string(),
            provider: provider.to_string(),
        }) {
            Ok(())
        } else {
            Err(AppRuntimeError::Provider(format!(
                "Cell development tokenizer `{provider}` is unavailable for application `{application}`"
            )))
        }
    }

    fn count(
        &self,
        application: &str,
        provider: &str,
        text: &str,
    ) -> bicdb_app_runtime::Result<u64> {
        self.available(application, provider)?;
        // Deliberately deterministic and conservative for local integration
        // tests; production Cells must bind a manifest-attested tokenizer.
        Ok(text.len().div_ceil(4) as u64)
    }
}

#[derive(Clone, Debug)]
struct CellDevelopmentDenyLlmProvider {
    bindings: BTreeSet<CellDevelopmentProviderBinding>,
}

impl LlmProvider for CellDevelopmentDenyLlmProvider {
    fn available(&self, application: &str, provider: &str) -> bicdb_app_runtime::Result<()> {
        if self.bindings.contains(&CellDevelopmentProviderBinding {
            application: application.to_string(),
            provider: provider.to_string(),
        }) {
            Ok(())
        } else {
            Err(AppRuntimeError::Provider(format!(
                "Cell development LLM provider `{provider}` is unavailable for application `{application}`"
            )))
        }
    }

    fn complete(
        &self,
        application: &str,
        provider: &str,
        _contract: &ApplicationLlmClientV1,
        _request: LlmProviderRequest,
    ) -> bicdb_app_runtime::Result<LlmProviderResponse> {
        self.available(application, provider)?;
        Err(AppRuntimeError::CapabilityDenied(format!(
            "LLM provider `{provider}` is disabled by the Cell's signed Phase-1 development policy"
        )))
    }

    fn estimate_microusd(
        &self,
        application: &str,
        provider: &str,
        _input_tokens: u64,
        _output_tokens: u64,
    ) -> bicdb_app_runtime::Result<u64> {
        self.available(application, provider)?;
        Ok(0)
    }
}

fn construct_application_host(
    volume_path: &Path,
    cell_id: &CellId,
    manifest_digest: &Sha256Digest,
    cell_key: &CellKeyMaterial,
    database: Arc<RwLock<BicDb>>,
    prepared: PreparedCellApplications,
) -> Result<CellApplicationHost> {
    let package_root = ensure_restricted_directory(&volume_path.join(CELL_APP_RUNTIME_DIRECTORY))?;
    let PreparedCellApplications {
        packages,
        verifier,
        authenticator,
        phase3,
        phase1_development_providers,
    } = prepared;
    let mut config = ApplicationHostConfig::new(&package_root, format!("cell-{cell_id}"));
    config.max_package_bytes = MAX_APPLICATION_PACKAGE_BYTES as usize;
    config.required_packages = packages
        .iter()
        .map(|package| package.manifest.identity.name.clone())
        .collect();
    let secret_provider = InMemorySecretProvider::default();
    if let Some(development) = phase1_development_providers.as_ref() {
        for secret_name in &development.derived_secrets {
            let material = derive_phase1_application_secret(cell_id, cell_key, secret_name)?;
            secret_provider.insert(
                secret_name,
                "cell-key-epoch",
                format!("cell:{cell_id}:{secret_name}"),
                "aes-256-gcm",
                material.to_vec(),
                true,
            )?;
        }
    }
    let mut services = InvocationServices::new(
        Arc::new(secret_provider),
        phase1_development_providers
            .as_ref()
            .map(|development| {
                Arc::new(CellDevelopmentDenyEgressProvider {
                    bindings: development.deny_egress.clone(),
                }) as Arc<dyn EgressProvider>
            })
            .unwrap_or_else(|| Arc::new(DenyEgressProvider)),
        Arc::new(DenyBlobProvider),
        Arc::new(BoundedObservability::new(100_000)?),
    );
    if let Some(development) = phase1_development_providers {
        services = services
            .with_tokenizer_provider(Arc::new(CellDevelopmentTokenizerProvider {
                bindings: development.deterministic_tokenizers,
            }))
            .with_llm_provider(Arc::new(CellDevelopmentDenyLlmProvider {
                bindings: development.deny_llm,
            }));
    }
    let runtime = ApplicationRuntime::new_shared(database, config, verifier, services)?;
    let applications = packages
        .iter()
        .map(|package| package.manifest.identity.name.clone())
        .collect::<Vec<_>>();
    for package in packages {
        runtime.stage(package)?;
    }
    runtime.activate_batch(&applications)?;
    let readiness = runtime.readiness();
    if !readiness.ready {
        return Err(CellError::Application(
            bicdb_app_runtime::AppRuntimeError::NotReady(
                serde_json::to_string(&readiness).unwrap_or_else(|_| {
                    "cell application host failed its readiness gate".to_string()
                }),
            ),
        ));
    }
    let (authenticator, trusted_handler): (_, Option<Arc<dyn TrustedHttpHandler>>) =
        if let Some(phase3) = phase3 {
            let session_key = derive_cell_session_key(
                cell_key,
                cell_id,
                manifest_digest,
                phase3.authorization.authorization_epoch,
            )?;
            let session_authenticator = JwtAuthenticator::hs256(
                JwtConfiguration {
                    issuer: format!("bicdb-cell://{cell_id}"),
                    audience: format!("bicdb-cell:{cell_id}"),
                    authentication_method: "cell-device-session".to_string(),
                    maximum_lifetime_seconds: phase3.authorization.session_lifetime_seconds,
                    clock_skew_seconds: 0,
                },
                session_key.to_vec(),
            )?;
            let replay = HandoffReplayJournal::open(
                package_root.join(CELL_HANDOFF_REPLAY_JOURNAL),
                &session_key,
            )?;
            let boundary = CellHttpBoundary {
                cell_id: cell_id.clone(),
                external_authenticator: authenticator,
                authorization: phase3.authorization,
                session_key,
                frontends: phase3.frontend_assets,
                replay: Mutex::new(replay),
            };
            (
                session_authenticator,
                Some(Arc::new(boundary) as Arc<dyn TrustedHttpHandler>),
            )
        } else {
            (authenticator, None)
        };
    Ok(CellApplicationHost {
        runtime,
        authenticator: Arc::new(authenticator),
        trusted_handler,
        admission: None,
    })
}

fn derive_phase1_application_secret(
    cell_id: &CellId,
    cell_key: &CellKeyMaterial,
    secret_name: &str,
) -> Result<[u8; 32]> {
    // Persisted application ciphertext must survive routine signed-manifest
    // transitions. The Cell identity and key material are volume-bound and
    // stable across those transitions; the manifest digest is intentionally
    // not part of this derivation because it changes every generation.
    let hkdf = Hkdf::<Sha256>::new(Some(cell_id.as_str().as_bytes()), &cell_key.clone_bytes());
    let mut material = [0_u8; 32];
    let info = format!("bicdb-cell/application-secret/v1\0{secret_name}");
    hkdf.expand(info.as_bytes(), &mut material).map_err(|_| {
        CellError::Application(AppRuntimeError::Provider(format!(
            "derive Cell-local secret `{secret_name}`"
        )))
    })?;
    Ok(material)
}

fn load_pinned_json<T: DeserializeOwned>(
    path: &Path,
    expected_digest: &Sha256Digest,
    label: &str,
) -> Result<T> {
    let bytes = read_bounded_regular_file(path, MAX_SIGNED_DOCUMENT_BYTES as u64, false)?;
    if &Sha256Digest::of_bytes(&bytes) != expected_digest {
        return Err(CellError::ArtifactMismatch(format!(
            "{label} digest does not match the CellManifest"
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| CellError::ArtifactMismatch(format!("decode {label}: {error}")))
}

fn ensure_restricted_directory(path: &Path) -> Result<PathBuf> {
    if !path.exists() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path)?;
    }
    canonical_directory_without_symlink(path)
}

pub fn bind_volume(
    volume_path: &Path,
    verified_manifest: &VerifiedCellManifest,
    signer_key_id: &str,
    signing_key: &SigningKey,
) -> Result<()> {
    verified_manifest.manifest.validate()?;
    if verified_manifest.manifest.manifest_generation != 1 {
        return Err(CellError::ManifestRollback(
            "a new volume can be bound only to a generation-1 manifest".to_string(),
        ));
    }
    let volume_path = canonical_directory_without_symlink(volume_path)?;
    let database_path =
        volume_path.join(&verified_manifest.manifest.storage.database_relative_path);
    match fs::symlink_metadata(&database_path) {
        Ok(_) => {
            return Err(CellError::StorageUnsafe(
                "refusing to bind a volume whose database path already exists".to_string(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let identity = CellVolumeIdentity {
        format: CELL_VOLUME_FORMAT.to_string(),
        cell_id: verified_manifest.manifest.cell_id.clone(),
        volume_id: verified_manifest.manifest.storage.volume_id.clone(),
        lineage_id: verified_manifest.manifest.storage.lineage_id.clone(),
        database_relative_path: verified_manifest
            .manifest
            .storage
            .database_relative_path
            .clone(),
        initial_manifest_digest: verified_manifest.digest.clone(),
    };
    let bytes = sign_payload(
        CELL_VOLUME_FORMAT,
        VOLUME_DOMAIN,
        &identity,
        signer_key_id,
        signing_key,
    )?;
    write_new_restricted(&volume_path.join(CELL_IDENTITY_FILE), &bytes)
}

fn verify_volume_identity(
    volume_path: &Path,
    manifest: &VerifiedCellManifest,
    trusted_keys: &TrustedManifestKeys,
) -> Result<()> {
    let bytes = read_bounded_regular_file(
        &volume_path.join(CELL_IDENTITY_FILE),
        MAX_SIGNED_DOCUMENT_BYTES as u64,
        false,
    )?;
    let (identity, _, _): (CellVolumeIdentity, _, _) =
        verify_payload(&bytes, CELL_VOLUME_FORMAT, VOLUME_DOMAIN, trusted_keys)?;
    if identity.format != CELL_VOLUME_FORMAT
        || identity.cell_id != manifest.manifest.cell_id
        || identity.volume_id != manifest.manifest.storage.volume_id
        || identity.lineage_id != manifest.manifest.storage.lineage_id
        || identity.database_relative_path != manifest.manifest.storage.database_relative_path
    {
        return Err(CellError::IdentityMismatch(
            "signed volume identity does not match manifest".to_string(),
        ));
    }
    if manifest.manifest.manifest_generation == 1
        && identity.initial_manifest_digest != manifest.digest
    {
        return Err(CellError::IdentityMismatch(
            "initial manifest does not match signed volume lineage root".to_string(),
        ));
    }
    Ok(())
}

fn verify_local_manifest_state(volume_path: &Path, manifest: &VerifiedCellManifest) -> Result<()> {
    let path = volume_path.join(CELL_STATE_FILE);
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if manifest.manifest.manifest_generation != 1 {
                return Err(CellError::ManifestRollback(
                    "a volume with no local state may activate only its signed generation-1 lineage root"
                        .to_string(),
                ));
            }
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded_regular_file(&path, MAX_SIGNED_DOCUMENT_BYTES as u64, false)?;
    let state: CellLocalState = decode_canonical_cbor(&bytes)?;
    if state.format != "bicdb.cell-local-state/v1"
        || state.cell_id != manifest.manifest.cell_id
        || state.volume_id != manifest.manifest.storage.volume_id
    {
        return Err(CellError::IdentityMismatch(
            "local manifest state belongs to another cell or volume".to_string(),
        ));
    }
    if manifest.manifest.manifest_generation < state.highest_manifest_generation {
        return Err(CellError::ManifestRollback(
            "manifest generation is older than local monotonic state".to_string(),
        ));
    }
    if manifest.manifest.manifest_generation == state.highest_manifest_generation
        && manifest.digest != state.current_manifest_digest
    {
        return Err(CellError::ManifestRollback(
            "manifest digest changed without generation advance".to_string(),
        ));
    }
    if manifest.manifest.manifest_generation == state.highest_manifest_generation
        && state.convergence_receipt_digest != convergence_ledger_head(volume_path)?
    {
        return Err(CellError::ManifestRollback(
            "Cell convergence receipt head differs from monotonic manifest state".to_string(),
        ));
    }
    if manifest.manifest.manifest_generation > state.highest_manifest_generation {
        if state.highest_manifest_generation.checked_add(1)
            != Some(manifest.manifest.manifest_generation)
        {
            return Err(CellError::ManifestRollback(
                "manifest transition must advance by exactly one generation".to_string(),
            ));
        }
        if manifest.manifest.previous_manifest_digest.as_ref()
            != Some(&state.current_manifest_digest)
        {
            return Err(CellError::ManifestRollback(
                "manifest transition does not link the active digest".to_string(),
            ));
        }
    }
    Ok(())
}

fn persist_local_manifest_state(volume_path: &Path, manifest: &VerifiedCellManifest) -> Result<()> {
    let convergence_receipt_digest = convergence_ledger_head(volume_path)?;
    let state = CellLocalState {
        format: "bicdb.cell-local-state/v1".to_string(),
        cell_id: manifest.manifest.cell_id.clone(),
        volume_id: manifest.manifest.storage.volume_id.clone(),
        highest_manifest_generation: manifest.manifest.manifest_generation,
        current_manifest_digest: manifest.digest.clone(),
        convergence_receipt_digest,
    };
    let bytes = canonical_cbor(&state)?;
    let final_path = volume_path.join(CELL_STATE_FILE);
    let temporary_path = volume_path.join(format!("{CELL_STATE_FILE}.new"));
    if temporary_path.exists() {
        return Err(CellError::StorageUnsafe(
            "stale cell-state staging file requires operator inspection".to_string(),
        ));
    }
    write_new_restricted(&temporary_path, &bytes)?;
    fs::rename(&temporary_path, &final_path)?;
    sync_directory(volume_path)?;
    Ok(())
}

fn convergence_ledger_head(volume_path: &Path) -> Result<Option<Sha256Digest>> {
    let path = volume_path.join(CELL_CONVERGENCE_LEDGER_DIRECTORY);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(CellError::StorageUnsafe(
                "Cell convergence ledger is not a real directory".to_string(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    ConvergenceLedger::open(path)?
        .latest()?
        .map(|(digest, _)| cell_digest(&digest))
        .transpose()
}

fn latest_convergence_completed_at(volume_path: &Path) -> Result<Option<i64>> {
    let path = volume_path.join(CELL_CONVERGENCE_LEDGER_DIRECTORY);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(CellError::StorageUnsafe(
                "Cell convergence ledger is not a real directory".to_string(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    Ok(ConvergenceLedger::open(path)?
        .latest()?
        .map(|(_, receipt)| receipt.completed_at))
}

fn verify_external_rollback_evidence(
    volume_path: &Path,
    manifest: &VerifiedCellManifest,
    evidence: &CellRollbackEvidence,
) -> Result<()> {
    verify_external_rollback_scope(manifest, evidence)?;
    let path = volume_path.join(CELL_ROLLBACK_WITNESS_FILE);
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if evidence.previous_rollback_counter != 0 {
                return Err(CellError::ManifestRollback(format!(
                    "first external rollback lease names predecessor {}, expected 0",
                    evidence.previous_rollback_counter
                )));
            }
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded_regular_file(&path, MAX_SIGNED_KEY_LEASE_BYTES as u64, false)?;
    let previous: CellRollbackEvidence = decode_canonical_cbor(&bytes)?;
    if previous.cell_id != evidence.cell_id
        || previous.volume_id != evidence.volume_id
        || previous.lineage_id != evidence.lineage_id
    {
        return Err(CellError::IdentityMismatch(
            "rollback witness belongs to another cell or volume lineage".to_string(),
        ));
    }
    if evidence.previous_rollback_counter != previous.rollback_counter {
        return Err(CellError::ManifestRollback(format!(
            "external rollback lease names predecessor {}, but the durable witness is {}",
            evidence.previous_rollback_counter, previous.rollback_counter
        )));
    }
    if evidence.manifest_generation < previous.manifest_generation
        || evidence.key_epoch < previous.key_epoch
    {
        return Err(CellError::ManifestRollback(
            "external rollback witness would decrease manifest generation or key epoch".to_string(),
        ));
    }
    Ok(())
}

fn verify_external_rollback_scope(
    manifest: &VerifiedCellManifest,
    evidence: &CellRollbackEvidence,
) -> Result<()> {
    if evidence.cell_id != manifest.manifest.cell_id
        || evidence.volume_id != manifest.manifest.storage.volume_id
        || evidence.lineage_id != manifest.manifest.storage.lineage_id
        || evidence.manifest_digest != manifest.digest
        || evidence.manifest_generation != manifest.manifest.manifest_generation
        || evidence.key_epoch != manifest.manifest.keys.key_epoch
    {
        return Err(CellError::KeyScopeMismatch(
            "external rollback evidence does not match the exact manifest and volume lineage"
                .to_string(),
        ));
    }
    Ok(())
}

fn persist_external_rollback_evidence(
    volume_path: &Path,
    evidence: &CellRollbackEvidence,
) -> Result<()> {
    let bytes = canonical_cbor(evidence)?;
    let final_path = volume_path.join(CELL_ROLLBACK_WITNESS_FILE);
    let temporary_path = volume_path.join(format!("{CELL_ROLLBACK_WITNESS_FILE}.new"));
    if temporary_path.exists() {
        return Err(CellError::StorageUnsafe(
            "stale rollback-witness staging file requires operator inspection".to_string(),
        ));
    }
    write_new_restricted(&temporary_path, &bytes)?;
    fs::rename(&temporary_path, &final_path)?;
    sync_directory(volume_path)?;
    Ok(())
}

pub fn load_signing_key(path: &Path) -> Result<SigningKey> {
    let bytes = Zeroizing::new(read_bounded_regular_file(path, 128, true)?);
    let mut key = [0_u8; 32];
    if bytes.len() == 32 {
        key.copy_from_slice(&bytes);
    } else {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| CellError::DocumentInvalid("signing key is not raw or hex".to_string()))?
            .trim();
        let decoded = Zeroizing::new(hex::decode(text).map_err(|_| {
            CellError::DocumentInvalid("signing key is not raw or hex".to_string())
        })?);
        if decoded.len() != 32 {
            return Err(CellError::DocumentInvalid(
                "signing key must contain exactly 32 bytes".to_string(),
            ));
        }
        key.copy_from_slice(&decoded);
    }
    let signing_key = SigningKey::from_bytes(&key);
    key.zeroize();
    Ok(signing_key)
}

pub fn load_verifying_key(path: &Path) -> Result<VerifyingKey> {
    let bytes = read_bounded_regular_file(path, 128, false)?;
    let decoded = if bytes.len() == 32 {
        bytes
    } else {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| CellError::DocumentInvalid("verifying key is not raw or hex".to_string()))?
            .trim();
        hex::decode(text).map_err(|_| {
            CellError::DocumentInvalid("verifying key is not raw or hex".to_string())
        })?
    };
    let key: [u8; 32] = decoded.try_into().map_err(|_| {
        CellError::DocumentInvalid("verifying key must contain exactly 32 bytes".to_string())
    })?;
    VerifyingKey::from_bytes(&key)
        .map_err(|_| CellError::DocumentInvalid("invalid Ed25519 verifying key".to_string()))
}

pub fn parse_trusted_key_specs(specs: &[String]) -> Result<TrustedManifestKeys> {
    let mut keys = BTreeMap::new();
    for spec in specs {
        let (key_id, path) = spec.split_once('=').ok_or_else(|| {
            CellError::DocumentInvalid("trusted key must be KEY_ID=FILE".to_string())
        })?;
        validate_signer_key_id(key_id)?;
        if keys.contains_key(key_id) {
            return Err(CellError::DocumentInvalid(
                "trusted key ids must be unique and non-empty".to_string(),
            ));
        }
        keys.insert(key_id.to_string(), load_verifying_key(Path::new(path))?);
    }
    if keys.is_empty() {
        return Err(CellError::SignatureInvalid(
            "at least one trusted manifest key is required".to_string(),
        ));
    }
    Ok(keys)
}

fn validate_database_relative_path(path: &str) -> Result<()> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CellError::DocumentInvalid(
            "database_relative_path must contain only relative normal components".to_string(),
        ));
    }
    if path != Path::new("db") {
        return Err(CellError::DocumentInvalid(
            "Phase 1 fixes database_relative_path to db".to_string(),
        ));
    }
    Ok(())
}

fn replication_object_logical_path(
    database_path: &Path,
    header: &ReplicationObjectHeader,
) -> PathBuf {
    database_path
        .join(".bicdb-cell-ha")
        .join("replication")
        .join(header.source_replica_id.as_str())
        .join(header.destination_replica_id.as_str())
        .join(header.writer_epoch.to_string())
        .join(format!("{}.object", header.commit_seq))
}

fn backup_key_logical_path(database_path: &Path, backup_id: &HaId) -> PathBuf {
    database_path
        .join(".bicdb-cell-ha")
        .join("backup-key")
        .join(format!("{}.wrapped", backup_id.as_str()))
}

fn cell_backup_artifact_paths(backup_root: &Path, backup_id: &HaId) -> (PathBuf, PathBuf) {
    (
        backup_root.join(format!("{}.bicbackup", backup_id.as_str())),
        backup_root.join(format!("{}.keywrap", backup_id.as_str())),
    )
}

fn cell_backup_payload_digest(
    backup_id: &HaId,
    archive_digest: &Sha256Digest,
    wrapped_key_digest: &Sha256Digest,
) -> Result<HaDigest> {
    Ok(HaDigest::of_bytes(&canonical_cbor(&CellBackupPayload {
        format: CELL_HA_BACKUP_PAYLOAD_FORMAT,
        backup_id,
        archive_digest,
        wrapped_key_digest,
    })?))
}

fn sha256_cell_backup_file(path: &Path) -> Result<Sha256Digest> {
    sha256_bounded_regular_file(path, MAX_CELL_BACKUP_BYTES, true)
}

fn safe_new_restore_target(root: &Path, relative: &Path) -> Result<PathBuf> {
    let mut components = relative.components();
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(CellError::StorageUnsafe(
            "Cell restore target must be one relative normal directory name".to_string(),
        ));
    }
    let target = root.join(relative);
    if fs::symlink_metadata(&target).is_ok() {
        return Err(CellError::StorageUnsafe(
            "Cell restore target already exists".to_string(),
        ));
    }
    Ok(target)
}

fn canonical_directory_without_symlink(path: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CellError::StorageUnsafe(
            "cell path must be an existing non-symlink directory".to_string(),
        ));
    }
    let canonical = fs::canonicalize(path)?;
    let canonical_metadata = fs::symlink_metadata(&canonical)?;
    if canonical_metadata.file_type().is_symlink() || !canonical_metadata.is_dir() {
        return Err(CellError::StorageUnsafe(
            "canonical cell path is not a directory".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: `geteuid` has no arguments and no memory-safety preconditions.
        let effective_uid = unsafe { libc::geteuid() };
        if canonical_metadata.uid() != effective_uid || canonical_metadata.mode() & 0o022 != 0 {
            return Err(CellError::StorageUnsafe(format!(
                "{} must be owned by the runtime user and not group/world writable",
                canonical.display()
            )));
        }
    }
    Ok(canonical)
}

/// Hold a kernel-enforced, process-lifetime lease for one canonical Cell
/// volume. The lock lives outside the encrypted database tree so backup and
/// restore never copy it. A stale file is harmless: authority is carried by
/// the open file description, and the kernel releases it on process death.
#[cfg(unix)]
fn acquire_cell_runtime_lock(volume_path: &Path) -> Result<File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let path = volume_path.join(CELL_RUNTIME_LOCK_FILE);
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(&path)?;
    let metadata = file.metadata()?;
    // SAFETY: `geteuid` has no arguments and no memory-safety preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != effective_uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(CellError::StorageUnsafe(
            "Cell runtime lock must be a single-link, owner-only regular file".to_string(),
        ));
    }
    // SAFETY: `file` owns a valid descriptor for the lifetime of this call.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EWOULDBLOCK)
            || error.raw_os_error() == Some(libc::EAGAIN)
        {
            return Err(CellError::StorageUnsafe(
                "another runtime already holds this exact Cell volume".to_string(),
            ));
        }
        return Err(error.into());
    }
    Ok(file)
}

#[cfg(windows)]
fn acquire_cell_runtime_lock(volume_path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    let path = volume_path.join(CELL_RUNTIME_LOCK_FILE);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).share_mode(0);
    options.open(path).map_err(|error| {
        if matches!(
            error.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::WouldBlock
        ) {
            CellError::StorageUnsafe(
                "another runtime already holds this exact Cell volume".to_string(),
            )
        } else {
            error.into()
        }
    })
}

#[cfg(not(any(unix, windows)))]
fn acquire_cell_runtime_lock(_volume_path: &Path) -> Result<File> {
    Err(CellError::RegulatedAdmissionIncomplete(
        "this platform has no implemented kernel Cell-volume lease".to_string(),
    ))
}

/// Inspect an existing cell database tree without following links before any
/// key lease is consumed. Strict file readers protect the final component;
/// this closes the complementary parent-directory substitution path and also
/// prevents devices, sockets, or multiply-linked files from entering the
/// constructed cell storage graph.
fn verify_cell_storage_tree_safety(database_path: &Path) -> Result<()> {
    const MAX_STORAGE_TREE_DEPTH: usize = 128;
    const MAX_STORAGE_TREE_OBJECTS: usize = 1_000_000;

    let root_metadata = match fs::symlink_metadata(database_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(CellError::StorageUnsafe(
            "cell database root must be a non-symlink directory".to_string(),
        ));
    }

    let mut pending = vec![(database_path.to_path_buf(), 0_usize)];
    let mut objects = 0_usize;
    while let Some((directory, depth)) = pending.pop() {
        if depth > MAX_STORAGE_TREE_DEPTH {
            return Err(CellError::StorageUnsafe(format!(
                "cell storage tree exceeds {MAX_STORAGE_TREE_DEPTH} directory levels"
            )));
        }
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            objects = objects.checked_add(1).ok_or_else(|| {
                CellError::StorageUnsafe("cell storage object counter overflowed".to_string())
            })?;
            if objects > MAX_STORAGE_TREE_OBJECTS {
                return Err(CellError::StorageUnsafe(format!(
                    "cell storage tree exceeds {MAX_STORAGE_TREE_OBJECTS} objects"
                )));
            }
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(CellError::StorageUnsafe(format!(
                    "cell storage tree contains a symbolic link: {}",
                    path.display()
                )));
            }
            if metadata.is_dir() {
                pending.push((path, depth.saturating_add(1)));
                continue;
            }
            if !metadata.is_file() {
                return Err(CellError::StorageUnsafe(format!(
                    "cell storage tree contains a non-regular object: {}",
                    path.display()
                )));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(CellError::StorageUnsafe(format!(
                        "cell storage tree contains a multiply-linked file: {}",
                        path.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn read_bounded_regular_file(path: &Path, max_bytes: u64, secret: bool) -> Result<Vec<u8>> {
    let (file, opened_len) = open_bounded_regular_file(path, max_bytes, secret)?;
    let mut bytes = Vec::with_capacity(opened_len as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(CellError::StorageUnsafe(format!(
            "{} changed beyond its bounded size while reading",
            path.display()
        )));
    }
    Ok(bytes)
}

fn open_bounded_regular_file(path: &Path, max_bytes: u64, secret: bool) -> Result<(File, u64)> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CellError::StorageUnsafe(format!(
            "{} must be a regular non-symlink file",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file() || opened_metadata.len() > max_bytes {
        return Err(CellError::StorageUnsafe(format!(
            "{} is not a bounded regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev() != opened_metadata.dev() || metadata.ino() != opened_metadata.ino() {
            return Err(CellError::StorageUnsafe(format!(
                "{} changed while it was being opened",
                path.display()
            )));
        }
        if opened_metadata.nlink() != 1 {
            return Err(CellError::StorageUnsafe(format!(
                "{} must not have hard links",
                path.display()
            )));
        }
        let forbidden = if secret { 0o077 } else { 0o022 };
        if opened_metadata.mode() & forbidden != 0 {
            return Err(CellError::StorageUnsafe(format!(
                "{} has unsafe permissions",
                path.display()
            )));
        }
        // SAFETY: `geteuid` has no arguments and no memory-safety preconditions.
        let effective_uid = unsafe { libc::geteuid() };
        if secret && opened_metadata.uid() != effective_uid {
            return Err(CellError::StorageUnsafe(format!(
                "{} secret is not owned by the runtime user",
                path.display()
            )));
        }
    }
    let opened_len = opened_metadata.len();
    Ok((file, opened_len))
}

fn sha256_bounded_regular_file(path: &Path, max_bytes: u64, secret: bool) -> Result<Sha256Digest> {
    let (file, _) = open_bounded_regular_file(path, max_bytes, secret)?;
    sha256_bounded_open_file(file, path, max_bytes)
}

fn sha256_bounded_open_file(
    file: File,
    display_path: &Path,
    max_bytes: u64,
) -> Result<Sha256Digest> {
    let mut limited = file.take(max_bytes.saturating_add(1));
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = limited.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > max_bytes {
            return Err(CellError::StorageUnsafe(format!(
                "{} changed beyond its bounded size while hashing",
                display_path.display()
            )));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(Sha256Digest(format!(
        "sha256:{}",
        hex::encode(hasher.finalize())
    )))
}

fn write_new_restricted(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub fn sha256_file(path: &Path) -> Result<Sha256Digest> {
    sha256_bounded_regular_file(path, MAX_ARTIFACT_BYTES, false)
}

fn running_binary_digest() -> Result<Sha256Digest> {
    static RUNNING_BINARY_DIGEST: OnceLock<Sha256Digest> = OnceLock::new();
    if let Some(digest) = RUNNING_BINARY_DIGEST.get() {
        return Ok(digest.clone());
    }
    let digest = running_binary_digest_uncached()?;
    let _ = RUNNING_BINARY_DIGEST.set(digest.clone());
    Ok(digest)
}

fn running_binary_digest_uncached() -> Result<Sha256Digest> {
    #[cfg(target_os = "linux")]
    {
        let path = Path::new("/proc/self/exe");
        let mut options = OpenOptions::new();
        options.read(true);
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC);
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_ARTIFACT_BYTES {
            return Err(CellError::StorageUnsafe(
                "running executable is not a bounded regular file".to_string(),
            ));
        }
        sha256_bounded_open_file(file, path, MAX_ARTIFACT_BYTES)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let path = std::env::current_exe().map_err(CellError::Io)?;
        sha256_bounded_regular_file(&path, MAX_ARTIFACT_BYTES, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use bicdb_app_runtime::canonical_signing_payload;
    use bicdb_cell_admission::{
        document_digest as admission_document_digest, encode_document as encode_admission_document,
        sign_approval as sign_admission_approval, AdmissionApproval, AdmissionAuthorityKey,
        AdmissionAuthorityRole, AdmissionAuthorization, AdmissionBundle, AdmissionGate,
        AdmissionSignatureDomain, AdmissionTrustPolicy, CertifiedAdmissionAuthorization,
        CertifiedDeploymentAttestation, CertifiedEvidenceCheckpoint, CertifiedGateEvidence,
        DeploymentAttestation, EvidenceArtifact, EvidenceCheckpoint, EvidenceTestSummary,
        GateEvidence, ReviewDiscipline, ADMISSION_AUTHORIZATION_FORMAT, ADMISSION_BUNDLE_FORMAT,
        ADMISSION_TRUST_POLICY_FORMAT, DEPLOYMENT_ATTESTATION_FORMAT, EVIDENCE_CHECKPOINT_FORMAT,
        GATE_EVIDENCE_FORMAT,
    };
    use bicdb_cell_ha::{
        sign_cell_backup_approval, sign_replica_lease_approval,
        sign_restore_authorization_approval, sign_writer_epoch_approval, FenceEvidence, HaApproval,
        HaAuthorityKey, HaAuthorityRole, HaDurabilityMode, HaTopologyPolicy, ReplicaLeaseStatement,
        RestoreAuthorization, VerifiedHaActivation, WriterEpochStatement, HA_TRUST_POLICY_FORMAT,
        REPLICA_LEASE_FORMAT, RESTORE_AUTHORIZATION_FORMAT, WRITER_EPOCH_FORMAT,
    };
    use bicdb_core::{
        EncryptionMetadata, GraphProjection, HnswIndexConfig, MemoryIndexMode, ModelRegistryEntry,
        Record, ENCRYPTION_METADATA_FILE,
    };
    use bicdb_extension::abi_v2::{
        ApplicationAuthKindV1, ApplicationAuthSchemeV1, ApplicationManifestV2, PackageMetadata,
        RouteV2, APPLICATION_COMPATIBILITY_PROFILE,
    };
    use bicdb_extension::{
        ExtensionIdentity, ExtensionLimits, ExtensionManifest, ExtensionPermissions, HttpMethod,
    };
    use bicdb_fleet::{
        document_digest as fleet_document_digest, sign_activation_approval,
        sign_checkpoint_approval, sign_release_approval, sign_rollout_approval, ActivationTicket,
        ApplicationRelease, ApprovalSignature, ArtifactReference, AuthorityRole, FleetAuthorityKey,
        FleetRelease, ReleaseCompatibility, ReleaseComponent, ReleaseComponentKind,
        ReproducibleBuildWitness, RolloutCohort, RolloutPlan, SignedActivationTicket,
        SignedFleetRelease, SignedRolloutPlan, SignedTransparencyCheckpoint,
        TransparencyCheckpoint, TransparencyEntry, TransparencyInclusionProof,
        ACTIVATION_BUNDLE_FORMAT, ACTIVATION_TICKET_FORMAT, FLEET_RELEASE_FORMAT,
        ROLLOUT_PLAN_FORMAT, TRANSPARENCY_CHECKPOINT_FORMAT, TRANSPARENCY_ENTRY_FORMAT,
    };
    use std::io::Cursor;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fn digest(label: &str) -> Sha256Digest {
        Sha256Digest::of_bytes(label.as_bytes())
    }

    fn current_runtime_digest() -> Sha256Digest {
        running_binary_digest().unwrap()
    }

    fn hex_digest(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn wasm_module(manifest: &ExtensionManifest) -> Vec<u8> {
        let manifest = serde_json::to_string(manifest).unwrap();
        let escaped = manifest
            .as_bytes()
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let result = br#"{"status":200,"body":{"status":200,"body":{"kind":"json","value":{"ok":true}}},"ack":true}"#;
        let result_escaped = result
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let result_pointer = 16_384_u32;
        wat::parse_str(format!(
            r#"(module
                (import "bicdb:app/host" "call"
                    (func $host_call (param i32 i32 i32 i32) (result i64)))
                (memory (export "memory") 2 1024)
                (global $next (mut i32) (i32.const 32768))
                (data (i32.const 1024) "{escaped}")
                (data (i32.const {result_pointer}) "{result_escaped}")
                (func (export "bicdb_extension_abi_version") (result i32)
                    i32.const 2)
                (func (export "bicdb_extension_manifest_ptr") (result i32)
                    i32.const 1024)
                (func (export "bicdb_extension_manifest_len") (result i32)
                    i32.const {manifest_len})
                (func (export "bicdb_extension_alloc") (param $len i32) (result i32)
                    (local $ptr i32)
                    global.get $next
                    local.tee $ptr
                    local.get $len
                    i32.add
                    global.set $next
                    local.get $ptr)
                (func (export "bicdb_extension_dealloc") (param i32 i32))
                (func (export "bicdb_extension_invoke") (param i32 i32) (result i64)
                    i64.const {packed}))"#,
            manifest_len = manifest.len(),
            packed = ((result_pointer as u64) << 32) | result.len() as u64,
        ))
        .unwrap()
    }

    fn signed_generic_package(signing_key: &SigningKey) -> ApplicationPackage {
        let dependency_lock = br#"{"format":1,"packages":[]}"#.to_vec();
        let sbom = br#"{"bomFormat":"CycloneDX","specVersion":"1.5"}"#.to_vec();
        let provenance = br#"{"builder":"bicdb-cell-test"}"#.to_vec();
        let application = ApplicationManifestV2 {
            abi_version: 2,
            application_profile: APPLICATION_COMPATIBILITY_PROFILE.to_string(),
            package: PackageMetadata {
                application: "generic-ledger".to_string(),
                version: "1.0.0".to_string(),
                package_sha256: "0".repeat(64),
                dependency_lock_sha256: hex_digest(&dependency_lock),
                sbom_sha256: hex_digest(&sbom),
                provenance_sha256: hex_digest(&provenance),
                signature_key_id: "application-release-a".to_string(),
                signature_algorithm: "ed25519".to_string(),
                signature: "unsigned-placeholder".to_string(),
            },
            relation_permissions: vec![],
            raw_sql: vec![],
            service_imports: vec![],
            service_exports: vec![],
            secrets: vec![],
            egress: vec![],
            blobs: vec![],
            routes: vec![RouteV2 {
                name: "status".to_string(),
                method: HttpMethod::Get,
                template: "/status".to_string(),
                export: "status".to_string(),
                resource: None,
                operation: None,
                service_call: None,
                application_request: None,
                application_response: None,
                idempotency: None,
                cache: None,
                telemetry: None,
                response_headers: BTreeMap::new(),
                public: false,
                auth_scheme: Some("cell_identity".to_string()),
                roles: BTreeSet::new(),
                scopes: BTreeSet::new(),
                roles_any: false,
                scopes_any: false,
                max_request_bytes: 1024,
                max_response_bytes: 4096,
                streaming_request: false,
                streaming_response: false,
                sse: false,
                websocket: false,
            }],
            response_headers: BTreeMap::new(),
            auth_schemes: BTreeMap::from([(
                "cell_identity".to_string(),
                ApplicationAuthSchemeV1 {
                    kind: ApplicationAuthKindV1::OidcEd25519,
                    issuer: "https://identity.example.test".to_string(),
                    audience: "generic-cell".to_string(),
                },
            )]),
            realtime: vec![],
            resources: vec![],
            invariants: vec![],
            migrations: vec![],
            workers: vec![],
            schedules: vec![],
            application_program: None,
            required_features: BTreeSet::new(),
            max_call_depth: 16,
        };
        let embedded = ExtensionManifest {
            identity: ExtensionIdentity {
                name: "generic-ledger".to_string(),
                version: "1.0.0".to_string(),
                abi_version: 2,
                description: "industry-agnostic cell conformance package".to_string(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::new(),
            permissions: ExtensionPermissions::default(),
            limits: ExtensionLimits::default(),
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![],
            subscriptions: vec![],
            observability: vec![],
            application: Some(Box::new(application)),
        };
        let module = wasm_module(&embedded);
        let mut package = ApplicationPackage {
            manifest: embedded,
            modules: BTreeMap::from([("generic-ledger".to_string(), module)]),
            frontend_assets: BTreeMap::new(),
            components: Vec::new(),
            dependency_lock,
            sbom,
            provenance,
        };
        let payload = canonical_signing_payload(&package).unwrap();
        let package_sha256 = hex_digest(&payload);
        let signature = signing_key.sign(package_sha256.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_sha256;
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        package
    }

    fn resign_package(package: &mut ApplicationPackage, signing_key: &SigningKey) {
        let payload = canonical_signing_payload(package).unwrap();
        let package_sha256 = hex_digest(&payload);
        let signature = signing_key.sign(package_sha256.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_sha256;
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    }

    fn replace_fixture_package(
        fixture: &ApplicationFixture,
        manifest: &mut CellManifest,
        package: &mut ApplicationPackage,
        signing_key: &SigningKey,
    ) {
        resign_package(package, signing_key);
        let bytes = serde_json::to_vec(package).unwrap();
        let digest = Sha256Digest::of_bytes(&bytes);
        write_restricted(
            &fixture
                .artifact_root
                .join(format!("{}.bicdb-app", digest.hex())),
            &bytes,
        );
        manifest.applications[0].digest = digest;
    }

    fn identity_token() -> String {
        let signing = SigningKey::from_bytes(&[6; 32]);
        let encode = |value: &serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(value).unwrap())
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header = encode(&serde_json::json!({
            "alg": "EdDSA",
            "typ": "JWT",
            "kid": "identity-a"
        }));
        let payload = encode(&serde_json::json!({
            "sub": "operator-1",
            "iss": "https://identity.example.test",
            "aud": "generic-cell",
            "iat": now,
            "exp": now + 300
        }));
        let signature = signing.sign(format!("{header}.{payload}").as_bytes());
        format!(
            "{header}.{payload}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes())
        )
    }

    struct ApplicationFixture {
        artifact_root: PathBuf,
        release_policy: PathBuf,
        identity_policy: PathBuf,
        egress_policy: PathBuf,
    }

    fn write_application_fixture(
        directory: &Path,
        manifest: &mut CellManifest,
        application_signing_key: &SigningKey,
    ) -> ApplicationFixture {
        let artifact_root = directory.join("artifacts");
        fs::create_dir(&artifact_root).unwrap();
        let package = signed_generic_package(application_signing_key);
        let package_bytes = serde_json::to_vec(&package).unwrap();
        let package_digest = Sha256Digest::of_bytes(&package_bytes);
        write_restricted(
            &artifact_root.join(format!("{}.bicdb-app", package_digest.hex())),
            &package_bytes,
        );

        let release_policy_bytes = serde_json::to_vec(&CellReleasePolicy {
            format: CELL_RELEASE_POLICY_FORMAT.to_string(),
            required_package_signatures: 1,
            roots: vec![CellReleaseRoot {
                root: "generic-suite".to_string(),
                signing_keys: BTreeMap::from([(
                    "application-release-a".to_string(),
                    hex::encode(application_signing_key.verifying_key().as_bytes()),
                )]),
            }],
        })
        .unwrap();
        let release_policy = directory.join("release-policy.json");
        write_restricted(&release_policy, &release_policy_bytes);

        let identity_signing_key = SigningKey::from_bytes(&[6; 32]);
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "OKP",
                "crv": "Ed25519",
                "alg": "EdDSA",
                "use": "sig",
                "kid": "identity-a",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(identity_signing_key.verifying_key().as_bytes()),
            }]
        });
        let identity_policy_bytes = serde_json::to_vec(&CellIdentityPolicy {
            format: CELL_IDENTITY_POLICY_FORMAT.to_string(),
            issuer: "https://identity.example.test".to_string(),
            audience: "generic-cell".to_string(),
            authentication_method: "oidc-ed25519".to_string(),
            maximum_lifetime_seconds: 3600,
            clock_skew_seconds: 30,
            oidc_ed25519_jwks: jwks,
        })
        .unwrap();
        let identity_policy = directory.join("identity-policy.json");
        write_restricted(&identity_policy, &identity_policy_bytes);

        let egress_policy_bytes = serde_json::to_vec(&CellEgressPolicy {
            format: CELL_EGRESS_POLICY_FORMAT.to_string(),
            mode: CellEgressMode::DenyAll,
            phase1_development_providers: None,
        })
        .unwrap();
        let egress_policy = directory.join("egress-policy.json");
        write_restricted(&egress_policy, &egress_policy_bytes);

        manifest.applications.push(CellApplicationPin {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "1.0.0".to_string(),
            digest: package_digest,
            schema_generation: 1,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Sensitive,
        });
        manifest.policy.trusted_release_policy_digest =
            Sha256Digest::of_bytes(&release_policy_bytes);
        manifest.policy.identity_policy_digest = Sha256Digest::of_bytes(&identity_policy_bytes);
        manifest.policy.egress_policy_digest = Sha256Digest::of_bytes(&egress_policy_bytes);

        ApplicationFixture {
            artifact_root,
            release_policy,
            identity_policy,
            egress_policy,
        }
    }

    fn manifest(binary: Sha256Digest, key: Sha256Digest) -> CellManifest {
        CellManifest {
            format: CELL_MANIFEST_FORMAT.to_string(),
            cell_id: CellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1240").unwrap(),
            manifest_generation: 1,
            previous_manifest_digest: None,
            jurisdiction: "IN".to_string(),
            storage: CellStorageManifest {
                volume_id: "vol-a".to_string(),
                lineage_id: "lineage-a".to_string(),
                database_relative_path: "db".to_string(),
                database_format: CURRENT_FORMAT_VERSION,
                encryption_profile: "phase1-xchacha20poly1305".to_string(),
            },
            runtime: CellRuntimeManifest {
                bicdb_binary_digest: binary,
                guest_image_digest: digest("guest-a"),
                isolation_profile: "phase1-process-isolated".to_string(),
            },
            applications: Vec::new(),
            keys: CellKeyManifest {
                authority: "file://development-fixed".to_string(),
                cell_kek_id: "kek-a".to_string(),
                key_epoch: 1,
                key_fingerprint: key,
            },
            replication: CellReplicationManifest {
                group_id: "rg-a".to_string(),
                replica_id: "replica-a".to_string(),
                writer_epoch: 1,
                role: CellReplicaRole::Primary,
            },
            policy: CellPolicyManifest {
                security_profile: PHASE1_SECURITY_PROFILE.to_string(),
                egress_policy_digest: digest("egress"),
                identity_policy_digest: digest("identity"),
                trusted_release_policy_digest: digest("release"),
                authorization_policy_digest: None,
                feature_certification_digest: None,
                fleet_trust_policy_digest: None,
                ha_trust_policy_digest: None,
                device_trust_policy_digest: None,
                grant_trust_policy_digest: None,
                admission_trust_policy_digest: None,
            },
        }
    }

    fn phase4_trust_policy(keys: &[(&str, AuthorityRole, &SigningKey)]) -> FleetTrustPolicy {
        FleetTrustPolicy {
            format: FLEET_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "generic-fleet".to_string(),
            generation: 1,
            authorities: keys
                .iter()
                .map(|(key_id, role, key)| FleetAuthorityKey {
                    key_id: (*key_id).to_string(),
                    role: *role,
                    public_key: hex::encode(key.verifying_key().as_bytes()),
                })
                .collect(),
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
                1,
            )]),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn phase4_activation_bundle(
        manifest: &VerifiedCellManifest,
        publisher: &SigningKey,
        security: &SigningKey,
        builder_a: &SigningKey,
        builder_b: &SigningKey,
        transparency: &SigningKey,
        rollout_key: &SigningKey,
        now: i64,
    ) -> FleetActivationBundle {
        let pin = &manifest.manifest.applications[0];
        let recipe = FleetDigest::of_bytes(b"recipe");
        let source = FleetDigest::of_bytes(b"source");
        let package_digest = FleetDigest::parse(pin.digest.as_str()).unwrap();
        let application = ApplicationRelease {
            root: pin.root.clone(),
            name: pin.name.clone(),
            version: pin.version.clone(),
            release_sequence: 1,
            package: ArtifactReference {
                digest: package_digest.clone(),
                size_bytes: 1,
                media_type: "application/vnd.bicdb.app+json".to_string(),
            },
            schema_generation: pin.schema_generation,
            execution_scope: "cell".to_string(),
            data_class: "sensitive".to_string(),
            compatibility: ReleaseCompatibility {
                bicdb_version_requirement: format!(">={}, <2.0.0", env!("CARGO_PKG_VERSION")),
                from_schema_generation_min: 0,
                from_schema_generation_max: 0,
                target_schema_generation: pin.schema_generation,
                record_format_min: 1,
                record_format_max: 1,
                rollback_until: now + 3600,
            },
            components: vec![ReleaseComponent {
                name: "package".to_string(),
                kind: ReleaseComponentKind::Backend,
                artifact: ArtifactReference {
                    digest: package_digest.clone(),
                    size_bytes: 1,
                    media_type: "application/vnd.bicdb.app+json".to_string(),
                },
                provenance_digest: FleetDigest::of_bytes(b"component-provenance"),
                sbom_digest: FleetDigest::of_bytes(b"component-sbom"),
                build_recipe_digest: recipe.clone(),
            }],
        };
        let release = FleetRelease {
            format: FLEET_RELEASE_FORMAT.to_string(),
            release_id: "018f7b30-4f4d-7b5c-a1f6-a183663e2201".to_string(),
            published_at: now - 60,
            source_tree_digest: source.clone(),
            dependency_lock_digest: FleetDigest::of_bytes(b"lock"),
            sbom_digest: FleetDigest::of_bytes(b"sbom"),
            provenance_digest: FleetDigest::of_bytes(b"provenance"),
            applications: vec![application],
            reproducible_builds: vec![
                ReproducibleBuildWitness {
                    builder_key_id: "builder-a".to_string(),
                    application_id: "generic-suite/generic-ledger".to_string(),
                    package_digest: package_digest.clone(),
                    source_tree_digest: source.clone(),
                    build_recipe_digest: recipe.clone(),
                },
                ReproducibleBuildWitness {
                    builder_key_id: "builder-b".to_string(),
                    application_id: "generic-suite/generic-ledger".to_string(),
                    package_digest,
                    source_tree_digest: source,
                    build_recipe_digest: recipe,
                },
            ],
        };
        let release_approval =
            |role, key_id, key| sign_release_approval(&release, role, key_id, key).unwrap();
        let signed_release = SignedFleetRelease {
            approvals: vec![
                release_approval(AuthorityRole::Publisher, "publisher-a", publisher),
                release_approval(AuthorityRole::Security, "security-a", security),
                release_approval(AuthorityRole::Builder, "builder-a", builder_a),
                release_approval(AuthorityRole::Builder, "builder-b", builder_b),
            ],
            release,
        };
        let release_digest = fleet_document_digest(&signed_release.release).unwrap();
        let entry = TransparencyEntry {
            format: TRANSPARENCY_ENTRY_FORMAT.to_string(),
            log_id: "generic-log".to_string(),
            index: 0,
            previous_entry_digest: None,
            release_digest: release_digest.clone(),
            recorded_at: now - 50,
        };
        let checkpoint = TransparencyCheckpoint {
            format: TRANSPARENCY_CHECKPOINT_FORMAT.to_string(),
            log_id: "generic-log".to_string(),
            size: 1,
            head_entry_digest: fleet_document_digest(&entry).unwrap(),
            issued_at: now - 40,
        };
        let checkpoint_approvals: Vec<ApprovalSignature> = vec![
            sign_checkpoint_approval(
                &checkpoint,
                AuthorityRole::Transparency,
                "transparency-a",
                transparency,
            )
            .unwrap(),
            sign_checkpoint_approval(&checkpoint, AuthorityRole::Security, "security-a", security)
                .unwrap(),
        ];
        let cell_id = FleetCellId::parse(manifest.manifest.cell_id.as_str()).unwrap();
        let plan = RolloutPlan {
            format: ROLLOUT_PLAN_FORMAT.to_string(),
            rollout_id: "018f7b30-4f4d-7b5c-a1f6-a183663e2202".to_string(),
            release_digest: release_digest.clone(),
            created_at: now - 60,
            expires_at: now + 3600,
            cohorts: vec![RolloutCohort {
                name: "internal".to_string(),
                cells: vec![cell_id.clone()],
                not_before: now - 30,
                observation_seconds: 60,
                max_parallel: 1,
            }],
        };
        let signed_plan = SignedRolloutPlan {
            approvals: vec![
                sign_rollout_approval(&plan, AuthorityRole::Rollout, "rollout-a", rollout_key)
                    .unwrap(),
                sign_rollout_approval(&plan, AuthorityRole::Security, "security-a", security)
                    .unwrap(),
            ],
            plan,
        };
        let ticket = ActivationTicket {
            format: ACTIVATION_TICKET_FORMAT.to_string(),
            ticket_id: "018f7b30-4f4d-7b5c-a1f6-a183663e2203".to_string(),
            cell_id,
            release_digest,
            rollout_id: signed_plan.plan.rollout_id.clone(),
            cohort_index: 0,
            previous_manifest_generation: 0,
            previous_manifest_digest: None,
            next_manifest_generation: 1,
            next_manifest_digest: FleetDigest::parse(manifest.digest.as_str()).unwrap(),
            issued_at: now - 20,
            not_before: now - 10,
            expires_at: now + 300,
        };
        FleetActivationBundle {
            format: ACTIVATION_BUNDLE_FORMAT.to_string(),
            release: signed_release,
            transparency: TransparencyInclusionProof {
                entries: vec![entry],
                checkpoint: SignedTransparencyCheckpoint {
                    checkpoint,
                    approvals: checkpoint_approvals,
                },
            },
            rollout: signed_plan,
            prior_cohort_gates: vec![],
            ticket: SignedActivationTicket {
                approvals: vec![
                    sign_activation_approval(
                        &ticket,
                        AuthorityRole::Rollout,
                        "rollout-a",
                        rollout_key,
                    )
                    .unwrap(),
                    sign_activation_approval(
                        &ticket,
                        AuthorityRole::Security,
                        "security-a",
                        security,
                    )
                    .unwrap(),
                ],
                ticket,
            },
        }
    }

    struct NeverKeyProvider;

    impl CellKeyProvider for NeverKeyProvider {
        fn unwrap_cell_key(&self, _request: CellKeyRequest<'_>) -> Result<CellKeyMaterial> {
            panic!("fleet activation must be verified before key release")
        }

        fn provider_kind(&self) -> &'static str {
            "never-key"
        }

        fn assurance(&self) -> CellKeyProviderAssurance {
            CellKeyProviderAssurance::AttestedLease
        }
    }

    fn phase2_manifest(binary: Sha256Digest, key: Sha256Digest) -> CellManifest {
        let mut manifest = manifest(binary, key);
        manifest.storage.encryption_profile = PHASE2_ENCRYPTION_PROFILE.to_string();
        manifest.runtime.isolation_profile = PHASE2_ISOLATION_PROFILE.to_string();
        manifest.keys.authority = "kms+attested://test/key-service".to_string();
        manifest.policy.security_profile = PHASE2_SECURITY_PROFILE.to_string();
        manifest
    }

    #[test]
    fn new_cell_profiles_are_neutral_and_legacy_profiles_remain_readable() {
        assert_eq!(PHASE1_SECURITY_PROFILE, "phase1-development-deny-regulated");
        assert_eq!(
            PHASE2_SECURITY_PROFILE,
            "phase2-cryptographic-deny-regulated"
        );
        assert_ne!(PHASE1_SECURITY_PROFILE, LEGACY_PHASE1_SECURITY_PROFILE);
        assert_ne!(PHASE2_SECURITY_PROFILE, LEGACY_PHASE2_SECURITY_PROFILE);

        let binary = current_runtime_digest();
        let key = Sha256Digest::of_bytes(&[9; 32]);
        let canonical = manifest(binary.clone(), key.clone());
        canonical.validate().unwrap();
        assert_eq!(canonical.policy.security_profile, PHASE1_SECURITY_PROFILE);

        let mut legacy_phase1 = canonical;
        legacy_phase1.policy.security_profile = LEGACY_PHASE1_SECURITY_PROFILE.to_string();
        legacy_phase1.validate().unwrap();

        let mut legacy_phase2 = phase2_manifest(binary, key);
        legacy_phase2.policy.security_profile = LEGACY_PHASE2_SECURITY_PROFILE.to_string();
        legacy_phase2.validate().unwrap();
        assert!(legacy_phase2.uses_phase2_cryptography());
    }

    struct CellHaTestKeys {
        lease_a: SigningKey,
        lease_b: SigningKey,
        lease_c: SigningKey,
        recovery_a: SigningKey,
        recovery_b: SigningKey,
        auditor: SigningKey,
        auditor_b: SigningKey,
        primary: SigningKey,
        standby: SigningKey,
        recovery: SigningKey,
    }

    fn cell_ha_test_keys() -> CellHaTestKeys {
        CellHaTestKeys {
            lease_a: SigningKey::from_bytes(&[31; 32]),
            lease_b: SigningKey::from_bytes(&[32; 32]),
            lease_c: SigningKey::from_bytes(&[33; 32]),
            recovery_a: SigningKey::from_bytes(&[34; 32]),
            recovery_b: SigningKey::from_bytes(&[35; 32]),
            auditor: SigningKey::from_bytes(&[36; 32]),
            auditor_b: SigningKey::from_bytes(&[37; 32]),
            primary: SigningKey::from_bytes(&[38; 32]),
            standby: SigningKey::from_bytes(&[39; 32]),
            recovery: SigningKey::from_bytes(&[40; 32]),
        }
    }

    fn cell_ha_test_policy(keys: &CellHaTestKeys) -> HaTrustPolicy {
        let authority = |key_id: &str, role, key: &SigningKey| HaAuthorityKey {
            key_id: key_id.to_string(),
            role,
            public_key: hex::encode(key.verifying_key().as_bytes()),
        };
        HaTrustPolicy {
            format: HA_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "generic-cell-ha".to_string(),
            generation: 1,
            authorities: vec![
                authority("lease-a", HaAuthorityRole::Lease, &keys.lease_a),
                authority("lease-b", HaAuthorityRole::Lease, &keys.lease_b),
                authority("lease-c", HaAuthorityRole::Lease, &keys.lease_c),
                authority("recovery-a", HaAuthorityRole::Recovery, &keys.recovery_a),
                authority("recovery-b", HaAuthorityRole::Recovery, &keys.recovery_b),
                authority("auditor-a", HaAuthorityRole::Auditor, &keys.auditor),
                authority("auditor-b", HaAuthorityRole::Auditor, &keys.auditor_b),
            ],
            thresholds: BTreeMap::from([
                (HaAuthorityRole::Lease, 2),
                (HaAuthorityRole::Recovery, 2),
                (HaAuthorityRole::Auditor, 1),
            ]),
            maximum_clock_skew_seconds: 5,
            maximum_replica_lease_seconds: 300,
            topology: HaTopologyPolicy {
                mode: HaDurabilityMode::LocalSynchronousRemoteAsynchronous,
                replica_count: 3,
                voting_replicas: 3,
                maximum_rpo_commits: 64,
                maximum_rto_millis: 30_000,
            },
        }
    }

    fn lease_approvals(
        statement: &ReplicaLeaseStatement,
        keys: &CellHaTestKeys,
    ) -> Vec<HaApproval> {
        vec![
            sign_replica_lease_approval("lease-a", statement, &keys.lease_a).unwrap(),
            sign_replica_lease_approval("lease-b", statement, &keys.lease_b).unwrap(),
        ]
    }

    fn key_lease(
        manifest: &CellManifest,
        manifest_digest: Sha256Digest,
        key: [u8; 32],
        nonce: &str,
    ) -> CellKeyLease {
        let now = current_unix_timestamp();
        CellKeyLease {
            format: CELL_KEY_LEASE_FORMAT.to_string(),
            lease_id: Uuid::new_v4().to_string(),
            issued_at: now - 1,
            not_before: now - 1,
            expires_at: now + 120,
            previous_rollback_counter: 0,
            rollback_counter: 1,
            attestation_nonce: nonce.to_string(),
            cell_id: manifest.cell_id.clone(),
            manifest_digest,
            manifest_generation: manifest.manifest_generation,
            volume_id: manifest.storage.volume_id.clone(),
            lineage_id: manifest.storage.lineage_id.clone(),
            bicdb_binary_digest: manifest.runtime.bicdb_binary_digest.clone(),
            guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            encryption_profile: manifest.storage.encryption_profile.clone(),
            security_profile: manifest.policy.security_profile.clone(),
            authority: manifest.keys.authority.clone(),
            cell_kek_id: manifest.keys.cell_kek_id.clone(),
            key_epoch: manifest.keys.key_epoch,
            key_fingerprint: Sha256Digest::of_bytes(&key),
            key,
        }
    }

    fn all_tree_bytes(root: &Path) -> Vec<u8> {
        fn collect(path: &Path, output: &mut Vec<u8>) {
            let mut entries = fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            entries.sort();
            for path in entries {
                if path.is_dir() {
                    collect(&path, output);
                } else {
                    output.extend_from_slice(&fs::read(path).unwrap());
                }
            }
        }

        let mut output = Vec::new();
        collect(root, &mut output);
        output
    }

    fn write_restricted(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn expect_cell_error(result: Result<CellRuntime>) -> CellError {
        match result {
            Err(error) => error,
            Ok(runtime) => {
                runtime.close().unwrap();
                panic!("invalid cell configuration unexpectedly opened")
            }
        }
    }

    #[test]
    fn signed_attested_key_lease_is_one_shot_and_exactly_workload_scoped() {
        let key = [9_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let kms_signing = SigningKey::from_bytes(&[8; 32]);
        let manifest = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&key));
        let signed_manifest = sign_manifest(&manifest, "release-a", &manifest_signing).unwrap();
        let verified = verify_manifest_bytes(
            &signed_manifest,
            &BTreeMap::from([("release-a".to_string(), manifest_signing.verifying_key())]),
        )
        .unwrap();
        let nonce = "1a".repeat(32);
        let lease = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        let signed_lease = sign_cell_key_lease(&lease, "kms-a", &kms_signing).unwrap();
        let provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(signed_lease),
            BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]),
            nonce,
        )
        .unwrap();
        let request = CellKeyRequest {
            cell_id: &manifest.cell_id,
            manifest_digest: &verified.digest,
            manifest_generation: manifest.manifest_generation,
            volume_id: &manifest.storage.volume_id,
            lineage_id: &manifest.storage.lineage_id,
            bicdb_binary_digest: &manifest.runtime.bicdb_binary_digest,
            guest_image_digest: &manifest.runtime.guest_image_digest,
            encryption_profile: &manifest.storage.encryption_profile,
            security_profile: &manifest.policy.security_profile,
            authority: &manifest.keys.authority,
            cell_kek_id: &manifest.keys.cell_kek_id,
            key_epoch: manifest.keys.key_epoch,
        };
        assert_eq!(provider.unwrap_cell_key(request.clone()).unwrap().key, key);
        assert!(matches!(
            provider.unwrap_cell_key(request),
            Err(CellError::KeyLeaseInvalid(message)) if message.contains("one-shot")
        ));
    }

    #[test]
    fn attested_key_lease_rejects_every_workload_scope_substitution() {
        let key = [9_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let kms_signing = SigningKey::from_bytes(&[8; 32]);
        let manifest = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&key));
        let signed_manifest = sign_manifest(&manifest, "release-a", &manifest_signing).unwrap();
        let verified = verify_manifest_bytes(
            &signed_manifest,
            &BTreeMap::from([("release-a".to_string(), manifest_signing.verifying_key())]),
        )
        .unwrap();
        let nonce = "4d".repeat(32);
        let trusted = BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]);
        let cases = [
            "cell_id",
            "manifest_digest",
            "manifest_generation",
            "volume_id",
            "lineage_id",
            "bicdb_binary_digest",
            "guest_image_digest",
            "encryption_profile",
            "security_profile",
            "authority",
            "cell_kek_id",
            "key_epoch",
        ];

        for field in cases {
            let mut lease = key_lease(&manifest, verified.digest.clone(), key, &nonce);
            match field {
                "cell_id" => {
                    lease.cell_id = CellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1241").unwrap()
                }
                "manifest_digest" => lease.manifest_digest = digest("another-manifest"),
                "manifest_generation" => lease.manifest_generation += 1,
                "volume_id" => lease.volume_id = "volume-substituted".to_string(),
                "lineage_id" => lease.lineage_id = "lineage-substituted".to_string(),
                "bicdb_binary_digest" => lease.bicdb_binary_digest = digest("another-binary"),
                "guest_image_digest" => lease.guest_image_digest = digest("another-guest"),
                "encryption_profile" => {
                    lease.encryption_profile = "another-encryption-profile".to_string()
                }
                "security_profile" => {
                    lease.security_profile = "another-security-profile".to_string()
                }
                "authority" => lease.authority = "kms+attested://other/authority".to_string(),
                "cell_kek_id" => lease.cell_kek_id = "another-kek".to_string(),
                "key_epoch" => lease.key_epoch += 1,
                _ => unreachable!(),
            }
            let signed = sign_cell_key_lease(&lease, "kms-a", &kms_signing).unwrap();
            let provider = AttestedKeyLeaseCellKeyProvider::new(
                Cursor::new(signed),
                trusted.clone(),
                nonce.clone(),
            )
            .unwrap();
            let result = provider.unwrap_cell_key(CellKeyRequest {
                cell_id: &manifest.cell_id,
                manifest_digest: &verified.digest,
                manifest_generation: manifest.manifest_generation,
                volume_id: &manifest.storage.volume_id,
                lineage_id: &manifest.storage.lineage_id,
                bicdb_binary_digest: &manifest.runtime.bicdb_binary_digest,
                guest_image_digest: &manifest.runtime.guest_image_digest,
                encryption_profile: &manifest.storage.encryption_profile,
                security_profile: &manifest.policy.security_profile,
                authority: &manifest.keys.authority,
                cell_kek_id: &manifest.keys.cell_kek_id,
                key_epoch: manifest.keys.key_epoch,
            });
            assert!(
                matches!(result, Err(CellError::KeyScopeMismatch(_))),
                "substitution of {field} was not rejected"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn inherited_key_lease_reader_accepts_only_a_pipe_descriptor() {
        use std::os::fd::{AsRawFd, FromRawFd};

        let mut descriptors = [-1_i32; 2];
        assert_eq!(
            unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) },
            0
        );
        let read_owner = unsafe { File::from_raw_fd(descriptors[0]) };
        let mut write_owner = unsafe { File::from_raw_fd(descriptors[1]) };
        let mut inherited = open_inherited_key_lease_reader(read_owner.as_raw_fd()).unwrap();
        write_owner.write_all(b"signed-lease").unwrap();
        drop(write_owner);
        drop(read_owner);
        let mut payload = Vec::new();
        inherited.read_to_end(&mut payload).unwrap();
        assert_eq!(payload, b"signed-lease");

        let regular = tempfile::tempfile().unwrap();
        assert!(matches!(
            open_inherited_key_lease_reader(regular.as_raw_fd()),
            Err(CellError::KeyLeaseInvalid(message)) if message.contains("inherited pipe")
        ));
        assert!(matches!(
            open_inherited_key_lease_reader(libc::STDIN_FILENO),
            Err(CellError::KeyLeaseInvalid(message)) if message.contains("greater than stderr")
        ));
    }

    #[test]
    fn attested_key_lease_refuses_wrong_scope_signature_time_and_nonce() {
        let key = [9_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let kms_signing = SigningKey::from_bytes(&[8; 32]);
        let wrong_kms_signing = SigningKey::from_bytes(&[10; 32]);
        let manifest = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&key));
        let signed_manifest = sign_manifest(&manifest, "release-a", &manifest_signing).unwrap();
        let verified = verify_manifest_bytes(
            &signed_manifest,
            &BTreeMap::from([("release-a".to_string(), manifest_signing.verifying_key())]),
        )
        .unwrap();
        let nonce = "2b".repeat(32);
        let request = || CellKeyRequest {
            cell_id: &manifest.cell_id,
            manifest_digest: &verified.digest,
            manifest_generation: manifest.manifest_generation,
            volume_id: &manifest.storage.volume_id,
            lineage_id: &manifest.storage.lineage_id,
            bicdb_binary_digest: &manifest.runtime.bicdb_binary_digest,
            guest_image_digest: &manifest.runtime.guest_image_digest,
            encryption_profile: &manifest.storage.encryption_profile,
            security_profile: &manifest.policy.security_profile,
            authority: &manifest.keys.authority,
            cell_kek_id: &manifest.keys.cell_kek_id,
            key_epoch: manifest.keys.key_epoch,
        };
        let trusted = BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]);
        let attempt = |lease: &CellKeyLease,
                       signer: &SigningKey,
                       expected_nonce: String|
         -> Result<CellKeyMaterial> {
            let signed = sign_cell_key_lease(lease, "kms-a", signer)?;
            AttestedKeyLeaseCellKeyProvider::new(
                Cursor::new(signed),
                trusted.clone(),
                expected_nonce,
            )?
            .unwrap_cell_key(request())
        };

        let wrong_nonce = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        assert!(matches!(
            attempt(&wrong_nonce, &kms_signing, "3c".repeat(32)),
            Err(CellError::KeyScopeMismatch(_))
        ));

        let mut wrong_cell = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        wrong_cell.cell_id = CellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1241").unwrap();
        assert!(matches!(
            attempt(&wrong_cell, &kms_signing, nonce.clone()),
            Err(CellError::KeyScopeMismatch(_))
        ));

        let wrong_signature = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        assert!(matches!(
            attempt(&wrong_signature, &wrong_kms_signing, nonce.clone()),
            Err(CellError::SignatureInvalid(_))
        ));

        let mut expired = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        let now = current_unix_timestamp();
        expired.issued_at = now - 121;
        expired.not_before = now - 120;
        expired.expires_at = now - 1;
        assert!(matches!(
            attempt(&expired, &kms_signing, nonce),
            Err(CellError::KeyLeaseInvalid(message)) if message.contains("currently valid")
        ));

        let mut skipped_predecessor =
            key_lease(&manifest, verified.digest.clone(), key, &"2b".repeat(32));
        skipped_predecessor.rollback_counter = 3;
        assert!(matches!(
            sign_cell_key_lease(&skipped_predecessor, "kms-a", &kms_signing),
            Err(CellError::KeyLeaseInvalid(message)) if message.contains("exact successor")
        ));
    }

    #[test]
    fn external_rollback_witness_requires_a_strictly_newer_authority_counter() {
        let directory = tempfile::tempdir().unwrap();
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let manifest = phase2_manifest(
            current_runtime_digest(),
            Sha256Digest::of_bytes(&[9_u8; 32]),
        );
        let signed = sign_manifest(&manifest, "release-a", &manifest_signing).unwrap();
        let verified = verify_manifest_bytes(
            &signed,
            &BTreeMap::from([("release-a".to_string(), manifest_signing.verifying_key())]),
        )
        .unwrap();
        let evidence = |previous_counter, counter| CellRollbackEvidence {
            cell_id: manifest.cell_id.clone(),
            volume_id: manifest.storage.volume_id.clone(),
            lineage_id: manifest.storage.lineage_id.clone(),
            lease_id: Uuid::new_v4().to_string(),
            lease_digest: digest(&format!("lease-{counter}")),
            signer_key_id: "kms-a".to_string(),
            previous_rollback_counter: previous_counter,
            rollback_counter: counter,
            manifest_digest: verified.digest.clone(),
            manifest_generation: manifest.manifest_generation,
            key_epoch: manifest.keys.key_epoch,
            issued_at: current_unix_timestamp(),
            expires_at: current_unix_timestamp() + 120,
        };
        let first = evidence(0, 1);
        verify_external_rollback_evidence(directory.path(), &verified, &first).unwrap();
        persist_external_rollback_evidence(directory.path(), &first).unwrap();
        assert!(matches!(
            verify_external_rollback_evidence(directory.path(), &verified, &evidence(0, 1)),
            Err(CellError::ManifestRollback(_))
        ));
        assert!(matches!(
            verify_external_rollback_evidence(directory.path(), &verified, &evidence(2, 3)),
            Err(CellError::ManifestRollback(_))
        ));
        verify_external_rollback_evidence(directory.path(), &verified, &evidence(1, 2)).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let witness = directory.path().join(CELL_ROLLBACK_WITNESS_FILE);
            fs::remove_file(&witness).unwrap();
            symlink("missing-rollback-witness", &witness).unwrap();
            assert!(matches!(
                verify_external_rollback_evidence(directory.path(), &verified, &evidence(0, 1)),
                Err(CellError::StorageUnsafe(_))
            ));
        }
    }

    #[test]
    fn phase2_cell_uses_bound_ciphertext_and_refuses_development_key_provider() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key = [9_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let kms_signing = SigningKey::from_bytes(&[8; 32]);
        let manifest = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&key));
        let signed_manifest = sign_manifest(&manifest, "release-a", &manifest_signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed_manifest);
        let trusted = BTreeMap::from([("release-a".to_string(), manifest_signing.verifying_key())]);
        let verified = verify_manifest_bytes(&signed_manifest, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &manifest_signing).unwrap();

        let refused = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path: manifest_path.clone(),
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted.clone(),
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                directory.path().join("must-not-be-read.key"),
                manifest.cell_id.clone(),
                manifest.keys.cell_kek_id.clone(),
            )),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(refused, CellError::KeyScopeMismatch(_)));
        assert!(!volume.join("db").exists());

        let nonce = "4d".repeat(32);
        let lease = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        let lease_bytes = sign_cell_key_lease(&lease, "kms-a", &kms_signing).unwrap();
        let provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(lease_bytes),
            BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]),
            nonce,
        )
        .unwrap();
        let runtime = CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted,
            key_provider: Box::new(provider),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        })
        .unwrap();
        let canary = "phase2-secret-canary-6af3c4f0";
        {
            let database = runtime.database.as_ref().unwrap();
            let mut database = database.write();
            database.create_collection("confidential_records").unwrap();
            database
                .register_embedding_model(ModelRegistryEntry::local_test("generic-model", 8))
                .unwrap();
            database
                .create_memory_index(
                    "generic_memory_index",
                    "confidential_records",
                    "payload",
                    "generic-model",
                    MemoryIndexMode::Async,
                )
                .unwrap();
            database
                .insert(
                    "confidential_records",
                    Record::new(canary)
                        .with_metadata(serde_json::json!({"payload": canary}))
                        .with_payload(canary.as_bytes().to_vec())
                        .with_vector(vec![1.0, 0.0]),
                )
                .unwrap();
            database
                .create_vector_index("confidential_records", HnswIndexConfig::default())
                .unwrap();
            database
                .build_graph_projection(
                    GraphProjection::new("generic_graph")
                        .nodes_from("confidential_records", "Object"),
                )
                .unwrap();
            database
                .write_attachment_from_reader(
                    "confidential_records",
                    canary,
                    "opaque",
                    Some("application/octet-stream"),
                    Cursor::new(canary.as_bytes()),
                )
                .unwrap();
        }
        runtime.flush().unwrap();
        let report = runtime.admission_report();
        assert_eq!(report.phase, "phase2-cryptographic-cell");
        assert!(!report.regulated_data_admitted);
        runtime.close().unwrap();

        let metadata: EncryptionMetadata = serde_json::from_slice(
            &fs::read(volume.join("db").join(ENCRYPTION_METADATA_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.version, 2);
        assert_eq!(
            metadata.binding,
            Some(
                EncryptionBinding::new(
                    manifest.cell_id.as_str(),
                    PHASE2_ENCRYPTION_PROFILE,
                    manifest.keys.key_epoch,
                )
                .unwrap()
            )
        );
        for relative in [
            "collections.json",
            "model-registry.json",
            "memory-indexes.json",
            "memory-index-jobs.json",
            "vector_indexes/confidential_records.hnsw.json",
            "graphs/generic_graph.graph.json",
        ] {
            let path = volume.join("db").join(relative);
            let bytes = fs::read(&path).unwrap_or_else(|error| {
                panic!("read encrypted sidecar {}: {error}", path.display())
            });
            assert_eq!(
                bytes.get(..4),
                Some(b"BICF".as_slice()),
                "{} is not a cell-bound encrypted frame",
                path.display()
            );
        }
        assert!(
            !all_tree_bytes(&volume)
                .windows(canary.len())
                .any(|window| window == canary.as_bytes()),
            "confidential canary leaked into the cell tree"
        );
    }

    #[test]
    fn signed_cell_key_rotation_uses_two_attested_leases_and_activates_next_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let old_key = [0x41_u8; 32];
        let new_key = [0x9c_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let kms_signing = SigningKey::from_bytes(&[8; 32]);
        let trusted_manifest =
            BTreeMap::from([("release-a".to_string(), manifest_signing.verifying_key())]);
        let trusted_kms = BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]);

        let current = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&old_key));
        let current_bytes = sign_manifest(&current, "release-a", &manifest_signing).unwrap();
        let current_verified = verify_manifest_bytes(&current_bytes, &trusted_manifest).unwrap();
        let current_path = directory.path().join("current.cbor");
        write_restricted(&current_path, &current_bytes);
        bind_volume(&volume, &current_verified, "release-a", &manifest_signing).unwrap();

        let startup_nonce = "5e".repeat(32);
        let mut startup_lease = key_lease(
            &current,
            current_verified.digest.clone(),
            old_key,
            &startup_nonce,
        );
        startup_lease.previous_rollback_counter = 0;
        startup_lease.rollback_counter = 1;
        let startup_provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(sign_cell_key_lease(&startup_lease, "kms-a", &kms_signing).unwrap()),
            trusted_kms.clone(),
            startup_nonce,
        )
        .unwrap();
        let runtime = CellRuntime::open(CellRuntimeConfig {
            manifest_path: current_path.clone(),
            volume_path: volume.clone(),
            expected_cell_id: current.cell_id.clone(),
            expected_volume_id: current.storage.volume_id.clone(),
            expected_guest_image_digest: current.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted_manifest.clone(),
            key_provider: Box::new(startup_provider),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        })
        .unwrap();
        let canary = "cell-rotation-canary-827e9f";
        {
            let mut database = runtime.database.as_ref().unwrap().write();
            database.create_collection("generic_records").unwrap();
            database
                .insert(
                    "generic_records",
                    Record::new("r-1").with_payload(canary.as_bytes().to_vec()),
                )
                .unwrap();
        }
        runtime.close().unwrap();

        let mut next = current.clone();
        next.manifest_generation = 2;
        next.previous_manifest_digest = Some(current_verified.digest.clone());
        next.keys.key_epoch = 2;
        next.keys.key_fingerprint = Sha256Digest::of_bytes(&new_key);
        let next_bytes = sign_manifest(&next, "release-a", &manifest_signing).unwrap();
        let next_verified = verify_manifest_bytes(&next_bytes, &trusted_manifest).unwrap();
        let next_path = directory.path().join("next.cbor");
        write_restricted(&next_path, &next_bytes);

        let current_nonce = "6f".repeat(32);
        let next_nonce = "70".repeat(32);
        let mut current_lease = key_lease(
            &current,
            current_verified.digest.clone(),
            old_key,
            &current_nonce,
        );
        current_lease.previous_rollback_counter = 1;
        current_lease.rollback_counter = 2;
        let mut next_lease = key_lease(&next, next_verified.digest.clone(), new_key, &next_nonce);
        next_lease.previous_rollback_counter = 2;
        next_lease.rollback_counter = 3;
        let current_provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(sign_cell_key_lease(&current_lease, "kms-a", &kms_signing).unwrap()),
            trusted_kms.clone(),
            current_nonce,
        )
        .unwrap();
        let next_provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(sign_cell_key_lease(&next_lease, "kms-a", &kms_signing).unwrap()),
            trusted_kms.clone(),
            next_nonce,
        )
        .unwrap();
        let report = rotate_cell_key(CellKeyRotationConfig {
            current_manifest_path: current_path,
            next_manifest_path: next_path.clone(),
            volume_path: volume.clone(),
            expected_cell_id: current.cell_id.clone(),
            expected_volume_id: current.storage.volume_id.clone(),
            expected_guest_image_digest: current.runtime.guest_image_digest.clone(),
            trusted_manifest_keys: trusted_manifest.clone(),
            current_key_provider: Box::new(current_provider),
            next_key_provider: Box::new(next_provider),
        })
        .unwrap();
        assert!(report.storage.activated);
        assert_eq!(report.previous_manifest_generation, 1);
        assert_eq!(report.active_manifest_generation, 2);
        assert!(!report.regulated_data_admitted);
        assert!(report.storage.retired_path.is_dir());

        let reopen_nonce = "81".repeat(32);
        let mut reopen_lease =
            key_lease(&next, next_verified.digest.clone(), new_key, &reopen_nonce);
        reopen_lease.previous_rollback_counter = 3;
        reopen_lease.rollback_counter = 4;
        let reopen_provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(sign_cell_key_lease(&reopen_lease, "kms-a", &kms_signing).unwrap()),
            trusted_kms,
            reopen_nonce,
        )
        .unwrap();
        let runtime = CellRuntime::open(CellRuntimeConfig {
            manifest_path: next_path,
            volume_path: volume.clone(),
            expected_cell_id: next.cell_id.clone(),
            expected_volume_id: next.storage.volume_id.clone(),
            expected_guest_image_digest: next.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted_manifest,
            key_provider: Box::new(reopen_provider),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        })
        .unwrap();
        assert_eq!(
            runtime
                .database
                .as_ref()
                .unwrap()
                .read()
                .get("generic_records", "r-1")
                .unwrap()
                .unwrap()
                .payload
                .as_deref(),
            Some(canary.as_bytes())
        );
        runtime.close().unwrap();
        assert!(
            !all_tree_bytes(&volume)
                .windows(canary.len())
                .any(|window| window == canary.as_bytes()),
            "rotation exposed plaintext anywhere in the cell volume"
        );
    }

    #[test]
    fn key_rotation_transition_is_key_only_and_epoch_contiguous() {
        let signing = SigningKey::from_bytes(&[7; 32]);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let current = phase2_manifest(
            current_runtime_digest(),
            Sha256Digest::of_bytes(&[0x11_u8; 32]),
        );
        let current_bytes = sign_manifest(&current, "release-a", &signing).unwrap();
        let current_verified = verify_manifest_bytes(&current_bytes, &trusted).unwrap();

        let next_manifest = |epoch, mutate_replication: bool| {
            let mut next = current.clone();
            next.manifest_generation = 2;
            next.previous_manifest_digest = Some(current_verified.digest.clone());
            next.keys.key_epoch = epoch;
            next.keys.key_fingerprint = Sha256Digest::of_bytes(&[0x22_u8; 32]);
            if mutate_replication {
                next.replication.writer_epoch += 1;
            }
            let bytes = sign_manifest(&next, "release-a", &signing).unwrap();
            verify_manifest_bytes(&bytes, &trusted).unwrap()
        };

        validate_key_rotation_transition(&current_verified, &next_manifest(2, false)).unwrap();
        assert!(matches!(
            validate_key_rotation_transition(&current_verified, &next_manifest(3, false)),
            Err(CellError::KeyScopeMismatch(_))
        ));
        assert!(matches!(
            validate_key_rotation_transition(&current_verified, &next_manifest(2, true)),
            Err(CellError::DocumentInvalid(_))
        ));
    }

    #[test]
    fn signed_manifest_rejects_tampering_and_unknown_signers() {
        let signing = SigningKey::from_bytes(&[7; 32]);
        let manifest = manifest(digest("binary"), Sha256Digest::of_bytes(&[9; 32]));
        let bytes = sign_manifest(&manifest, "release-a", &signing).unwrap();
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        assert_eq!(
            verify_manifest_bytes(&bytes, &trusted).unwrap().manifest,
            manifest
        );
        let mut relabeled: SignedCellDocument = decode_canonical_cbor(&bytes).unwrap();
        relabeled.signer_key_id = "release-alias".to_string();
        let relabeled = canonical_cbor(&relabeled).unwrap();
        let aliased_trust = BTreeMap::from([
            ("release-a".to_string(), signing.verifying_key()),
            ("release-alias".to_string(), signing.verifying_key()),
        ]);
        assert!(matches!(
            verify_manifest_bytes(&relabeled, &aliased_trust),
            Err(CellError::SignatureInvalid(_))
        ));

        let mut tampered = bytes;
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(verify_manifest_bytes(&tampered, &trusted).is_err());
        assert!(verify_manifest_bytes(
            &sign_manifest(&manifest, "other", &signing).unwrap(),
            &trusted
        )
        .is_err());
        assert!(
            serde_json::from_str::<Sha256Digest>(&format!("\"SHA256:{}\"", "a".repeat(64)))
                .is_err()
        );
    }

    #[test]
    fn wrong_volume_is_refused_before_database_creation() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let signed = sign_manifest(&manifest, "release-a", &signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &signing).unwrap();

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: "vol-b".to_string(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::IdentityMismatch(_)));
        assert!(!volume.join("db").exists());
    }

    #[test]
    fn substituted_runtime_digest_is_refused_before_database_creation() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let manifest = manifest(
            digest("not-the-running-binary"),
            Sha256Digest::of_bytes(&[9; 32]),
        );
        let signed = sign_manifest(&manifest, "release-a", &signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &signing).unwrap();

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::RuntimeMismatch(_)));
        assert!(!volume.join("db").exists());
    }

    #[test]
    fn wrong_key_is_refused_before_database_creation() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("wrong-cell.key");
        write_restricted(&key_path, &[8; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let signed = sign_manifest(&manifest, "release-a", &signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &signing).unwrap();

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::KeyScopeMismatch(_)));
        assert!(!volume.join("db").exists());
    }

    #[test]
    fn resigned_initial_manifest_cannot_replace_the_volume_lineage_root() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let original = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let original_signed = sign_manifest(&original, "release-a", &signing).unwrap();
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified = verify_manifest_bytes(&original_signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &signing).unwrap();

        let mut replacement = original.clone();
        replacement.jurisdiction = "US".to_string();
        let replacement_signed = sign_manifest(&replacement, "release-a", &signing).unwrap();
        let manifest_path = directory.path().join("replacement.cbor");
        write_restricted(&manifest_path, &replacement_signed);

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: replacement.cell_id.clone(),
            expected_volume_id: replacement.storage.volume_id.clone(),
            expected_guest_image_digest: replacement.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                replacement.cell_id,
                "kek-a".to_string(),
            )),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::IdentityMismatch(_)));
        assert!(!volume.join("db").exists());
    }

    #[cfg(unix)]
    #[test]
    fn cell_storage_tree_safety_rejects_nested_links_before_key_use() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("db");
        let nested = database.join("indexes");
        fs::create_dir_all(&nested).unwrap();
        let object = nested.join("catalog.bicf");
        write_restricted(&object, b"ciphertext fixture");

        let alias = nested.join("catalog-alias.bicf");
        fs::hard_link(&object, &alias).unwrap();
        assert!(matches!(
            verify_cell_storage_tree_safety(&database),
            Err(CellError::StorageUnsafe(_))
        ));
        fs::remove_file(&alias).unwrap();

        let linked_directory = database.join("search");
        symlink("missing-search-tree", &linked_directory).unwrap();
        assert!(matches!(
            verify_cell_storage_tree_safety(&database),
            Err(CellError::StorageUnsafe(_))
        ));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn cell_volume_runtime_lock_is_kernel_exclusive_and_crash_recoverable() {
        let directory = tempfile::tempdir().unwrap();
        let first = acquire_cell_runtime_lock(directory.path()).unwrap();
        assert!(matches!(
            acquire_cell_runtime_lock(directory.path()),
            Err(CellError::StorageUnsafe(_))
        ));
        drop(first);
        acquire_cell_runtime_lock(directory.path()).unwrap();
    }

    #[test]
    fn mismatched_application_artifact_is_refused_before_key_or_database_open() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let application_digest = manifest.applications[0].digest.clone();
        write_restricted(
            &fixture
                .artifact_root
                .join(format!("{}.bicdb-app", application_digest.hex())),
            b"substituted-application",
        );
        let signed = sign_manifest(&manifest, "release-a", &signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &signing).unwrap();

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root,
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: Some(CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: None,
                feature_certification_path: None,
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::ArtifactMismatch(_)));
        assert!(!volume.join("db").exists());
    }

    #[test]
    fn generic_signed_application_activates_and_serves_only_from_its_cell_host() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let signed = sign_manifest(&manifest, "manifest-release-a", &manifest_signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([(
            "manifest-release-a".to_string(),
            manifest_signing.verifying_key(),
        )]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "manifest-release-a", &manifest_signing).unwrap();

        let runtime = CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root,
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: Some(CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: None,
                feature_certification_path: None,
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            ha: None,
            device: None,
            grant: None,
            admission: None,
        })
        .unwrap();
        let host = runtime.application_host().expect("application host");
        assert!(host.readiness().ready);
        let report = runtime.admission_report();
        assert_eq!(report.phase, "phase1-plus-cell-application-preview");
        assert!(!report.regulated_data_admitted);

        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        async_runtime.block_on(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = host
                .serve_http(listener, HttpHostPolicy::default())
                .await
                .unwrap();
            let mut client = tokio::net::TcpStream::connect(server.address)
                .await
                .unwrap();
            client
                .write_all(
                    b"GET /status HTTP/1.1\r\nHost: cell.test\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 401"));

            let token = identity_token();
            let mut client = tokio::net::TcpStream::connect(server.address)
                .await
                .unwrap();
            client
                .write_all(
                    format!(
                        "GET /status HTTP/1.1\r\nHost: cell.test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8_lossy(&response);
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(response.contains("{\"ok\":true}"), "{response}");
            server.shutdown().await.unwrap();
        });
        runtime.close().unwrap();
    }

    #[test]
    fn phase4_activation_is_threshold_verified_before_cell_key_release() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let manifest_signing = SigningKey::from_bytes(&[17; 32]);
        let application_signing = SigningKey::from_bytes(&[18; 32]);
        let publisher = SigningKey::from_bytes(&[21; 32]);
        let security = SigningKey::from_bytes(&[22; 32]);
        let builder_a = SigningKey::from_bytes(&[23; 32]);
        let builder_b = SigningKey::from_bytes(&[24; 32]);
        let transparency = SigningKey::from_bytes(&[25; 32]);
        let rollout = SigningKey::from_bytes(&[26; 32]);
        let key = [9_u8; 32];
        let mut manifest = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&key));
        manifest.runtime.isolation_profile = PHASE4_ISOLATION_PROFILE.to_string();
        manifest.policy.security_profile = PHASE4_SECURITY_PROFILE.to_string();
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        manifest.policy.authorization_policy_digest = Some(digest("phase4-authorization"));
        manifest.policy.feature_certification_digest = Some(digest("phase4-features"));

        let authority_keys = [
            ("publisher-a", AuthorityRole::Publisher, &publisher),
            ("security-a", AuthorityRole::Security, &security),
            ("builder-a", AuthorityRole::Builder, &builder_a),
            ("builder-b", AuthorityRole::Builder, &builder_b),
            ("transparency-a", AuthorityRole::Transparency, &transparency),
            ("rollout-a", AuthorityRole::Rollout, &rollout),
        ];
        let fleet_policy = phase4_trust_policy(&authority_keys);
        let fleet_policy_bytes = serde_json::to_vec(&fleet_policy).unwrap();
        let fleet_policy_path = directory.path().join("fleet-policy.json");
        write_restricted(&fleet_policy_path, &fleet_policy_bytes);
        manifest.policy.fleet_trust_policy_digest =
            Some(Sha256Digest::of_bytes(&fleet_policy_bytes));

        let signed_manifest =
            sign_manifest(&manifest, "manifest-release-a", &manifest_signing).unwrap();
        let trusted = BTreeMap::from([(
            "manifest-release-a".to_string(),
            manifest_signing.verifying_key(),
        )]);
        let verified = verify_manifest_bytes(&signed_manifest, &trusted).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed_manifest);
        bind_volume(&volume, &verified, "manifest-release-a", &manifest_signing).unwrap();

        let now = current_unix_timestamp();
        let bundle = phase4_activation_bundle(
            &verified,
            &publisher,
            &security,
            &builder_a,
            &builder_b,
            &transparency,
            &rollout,
            now,
        );
        let bundle_path = directory.path().join("activation.json");
        write_restricted(&bundle_path, &serde_json::to_vec(&bundle).unwrap());
        let host = CellApplicationHostConfig {
            release_policy_path: fixture.release_policy.clone(),
            identity_policy_path: fixture.identity_policy.clone(),
            egress_policy_path: fixture.egress_policy.clone(),
            authorization_policy_path: Some(directory.path().join("authorization.json")),
            feature_certification_path: Some(directory.path().join("features.json")),
            fleet_trust_policy_path: Some(fleet_policy_path),
            fleet_activation_bundle_path: Some(bundle_path.clone()),
            previous_manifest_path: None,
        };
        let prepared = prepare_fleet_activation(&verified, &volume, Some(&host), &trusted)
            .unwrap()
            .expect("Phase 4 activation");
        assert_eq!(prepared.verified.cohort_name, "internal");
        assert_eq!(prepared.verified.applications.len(), 1);
        record_fleet_convergence(&volume, &verified, &prepared, now, now + 1).unwrap();
        persist_local_manifest_state(&volume, &verified).unwrap();
        verify_local_manifest_state(&volume, &verified).unwrap();
        assert_eq!(
            ConvergenceLedger::open(volume.join(CELL_CONVERGENCE_LEDGER_DIRECTORY))
                .unwrap()
                .verify()
                .unwrap()
                .len(),
            1
        );

        write_restricted(&bundle_path, b"{}");
        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root,
            trusted_manifest_keys: trusted,
            key_provider: Box::new(NeverKeyProvider),
            application_host: Some(host),
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::ArtifactMismatch(_)));
        assert!(!volume.join("db").exists());
    }

    #[test]
    fn phase5_real_stores_replicate_fence_backup_and_restore_under_exact_authority() {
        let directory = tempfile::tempdir().unwrap();
        let primary_volume = directory.path().join("primary");
        let standby_volume = directory.path().join("standby");
        fs::create_dir(&primary_volume).unwrap();
        fs::create_dir(&standby_volume).unwrap();
        let raw_key = [49_u8; 32];
        let keys = cell_ha_test_keys();
        let policy = cell_ha_test_policy(&keys);
        let policy_bytes = encode_ha_document(&policy).unwrap();
        let primary_id =
            HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3101", "primary replica").unwrap();
        let standby_id =
            HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3102", "standby replica").unwrap();
        let recovery_id =
            HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3103", "recovery replica").unwrap();

        let mut primary_manifest =
            phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&raw_key));
        primary_manifest.runtime.isolation_profile = PHASE5_ISOLATION_PROFILE.to_string();
        primary_manifest.policy.security_profile = PHASE5_SECURITY_PROFILE.to_string();
        primary_manifest.policy.authorization_policy_digest = Some(digest("phase5-authz"));
        primary_manifest.policy.feature_certification_digest = Some(digest("phase5-features"));
        primary_manifest.policy.fleet_trust_policy_digest = Some(digest("phase5-fleet"));
        primary_manifest.policy.ha_trust_policy_digest =
            Some(Sha256Digest::of_bytes(&policy_bytes));
        primary_manifest.applications = vec![CellApplicationPin {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "1.0.0".to_string(),
            digest: digest("phase5-generic-ledger"),
            schema_generation: 1,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Sensitive,
        }];
        primary_manifest.storage.volume_id = "phase5-primary-volume".to_string();
        primary_manifest.replication.group_id = "phase5-group".to_string();
        primary_manifest.replication.replica_id = primary_id.as_str().to_string();
        primary_manifest.replication.role = CellReplicaRole::Primary;

        let mut standby_manifest = primary_manifest.clone();
        standby_manifest.storage.volume_id = "phase5-standby-volume".to_string();
        standby_manifest.replication.replica_id = standby_id.as_str().to_string();
        standby_manifest.replication.role = CellReplicaRole::Standby;
        let configuration_digest = cell_ha_configuration_digest(&primary_manifest).unwrap();
        assert_eq!(
            configuration_digest,
            cell_ha_configuration_digest(&standby_manifest).unwrap(),
            "replica-local identities must not fork the certified Cell configuration"
        );

        let now = current_unix_timestamp();
        let epoch_statement = WriterEpochStatement {
            format: WRITER_EPOCH_FORMAT.to_string(),
            cell_id: HaId::parse(primary_manifest.cell_id.as_str(), "cell_id").unwrap(),
            group_id: primary_manifest.replication.group_id.clone(),
            writer_epoch: 1,
            primary_replica_id: primary_id.clone(),
            primary_fence_public_key: hex::encode(keys.primary.verifying_key().as_bytes()),
            key_epoch: primary_manifest.keys.key_epoch,
            manifest_generation: primary_manifest.manifest_generation,
            manifest_digest: configuration_digest.clone(),
            accepted_durable_commit_seq: 0,
            activated_at: now - 1,
            predecessor_epoch_digest: None,
            predecessor_primary_lease_digest: None,
            fence_evidence: None,
        };
        let epoch = CertifiedWriterEpoch {
            approvals: vec![
                sign_writer_epoch_approval("lease-a", &epoch_statement, &keys.lease_a).unwrap(),
                sign_writer_epoch_approval("lease-b", &epoch_statement, &keys.lease_b).unwrap(),
            ],
            statement: epoch_statement,
        };
        let epoch_digest = ha_document_digest(&epoch.statement).unwrap();
        let lease =
            |lease_id: &str, replica_id: HaId, role: ReplicaRole, signing_key: &SigningKey| {
                let statement = ReplicaLeaseStatement {
                    format: REPLICA_LEASE_FORMAT.to_string(),
                    lease_id: HaId::parse(lease_id, "lease_id").unwrap(),
                    lease_sequence: 1,
                    predecessor_lease_digest: None,
                    cell_id: epoch.statement.cell_id.clone(),
                    group_id: epoch.statement.group_id.clone(),
                    replica_id,
                    replica_public_key: hex::encode(signing_key.verifying_key().as_bytes()),
                    role,
                    writer_epoch: 1,
                    writer_epoch_digest: epoch_digest.clone(),
                    key_epoch: primary_manifest.keys.key_epoch,
                    manifest_generation: primary_manifest.manifest_generation,
                    manifest_digest: configuration_digest.clone(),
                    local_durable_commit_seq: 0,
                    issued_at: now - 1,
                    not_before: now - 1,
                    expires_at: now + 240,
                };
                CertifiedReplicaLease {
                    approvals: lease_approvals(&statement, &keys),
                    statement,
                }
            };
        let primary_lease = lease(
            "018f7b30-4f4d-7b5c-a1f6-a183663e3201",
            primary_id.clone(),
            ReplicaRole::Primary,
            &keys.primary,
        );
        let standby_lease = lease(
            "018f7b30-4f4d-7b5c-a1f6-a183663e3202",
            standby_id.clone(),
            ReplicaRole::Standby,
            &keys.standby,
        );
        let activation = |manifest: &CellManifest, lease: &CertifiedReplicaLease| {
            verify_ha_activation(
                &policy,
                &epoch,
                lease,
                None,
                None,
                &HaActivationContext {
                    cell_id: epoch.statement.cell_id.clone(),
                    group_id: epoch.statement.group_id.clone(),
                    replica_id: lease.statement.replica_id.clone(),
                    role: lease.statement.role,
                    writer_epoch: 1,
                    key_epoch: manifest.keys.key_epoch,
                    manifest_generation: manifest.manifest_generation,
                    manifest_digest: cell_ha_configuration_digest(manifest).unwrap(),
                    local_durable_commit_seq: 0,
                    now,
                },
            )
            .unwrap()
        };
        let primary_activation = activation(&primary_manifest, &primary_lease);
        let standby_activation = activation(&standby_manifest, &standby_lease);

        let open_database = |volume: &Path, manifest: &CellManifest, mode| {
            let replication = ReplicationConfig {
                enabled: false,
                mode,
                cluster_id: manifest.replication.group_id.clone(),
                node_id: manifest.replication.replica_id.clone(),
                ..ReplicationConfig::default()
            };
            BicDb::open_with_encryption(
                volume.join("db"),
                DbConfig::default()
                    .with_fsync(true)
                    .with_replication(replication)
                    .with_required_commit_admission(true),
                EncryptionConfig::with_raw_key(raw_key).with_binding(
                    EncryptionBinding::new(
                        manifest.cell_id.as_str(),
                        &manifest.storage.encryption_profile,
                        manifest.keys.key_epoch,
                    )
                    .unwrap(),
                ),
            )
            .unwrap()
        };
        let primary_db =
            open_database(&primary_volume, &primary_manifest, ReplicationMode::Primary);
        let standby_db =
            open_database(&standby_volume, &standby_manifest, ReplicationMode::Standby);
        let make_fence = |volume: &Path, activation: VerifiedHaActivation, replica_id: HaId| {
            let state = CellHaState {
                format: HA_STATE_FORMAT.to_string(),
                cell_id: epoch.statement.cell_id.clone(),
                group_id: epoch.statement.group_id.clone(),
                replica_id,
                writer_epoch: 1,
                writer_epoch_digest: epoch_digest.clone(),
                last_applied_commit_seq: 0,
                last_replication_object_digest: None,
                backup_head_digest: None,
            };
            Arc::new(
                CellHaCommitFence::new(
                    Arc::new(policy.clone()),
                    activation,
                    Arc::new(SystemHaClock),
                )
                .unwrap()
                .with_durable_state(
                    CellHaStateStore::new(volume.join(CELL_HA_STATE_FILE)),
                    state,
                )
                .unwrap(),
            )
        };
        let primary_fence = make_fence(&primary_volume, primary_activation, primary_id.clone());
        let standby_fence = make_fence(&standby_volume, standby_activation, standby_id.clone());
        primary_db.install_commit_admission(primary_fence.clone());
        let primary_cipher = primary_db.bound_object_cipher().unwrap();
        let standby_cipher = standby_db.bound_object_cipher().unwrap();

        let verified = |manifest: CellManifest| VerifiedCellManifest {
            digest: Sha256Digest::of_bytes(&canonical_cbor(&manifest).unwrap()),
            manifest,
            signer_key_id: "test-manifest-authority".to_string(),
        };
        let primary_runtime = CellRuntime {
            manifest: verified(primary_manifest.clone()),
            volume_path: primary_volume.clone(),
            _runtime_lock: tempfile::tempfile().unwrap(),
            database: Some(Arc::new(RwLock::new(primary_db))),
            application_host: None,
            ha: Some(CellHaRuntime {
                policy: Arc::new(policy.clone()),
                fence: primary_fence,
                object_cipher: primary_cipher,
                database_path: primary_volume.join("db"),
                replica_signing_key: keys.primary.clone(),
            }),
            device: None,
            grant: None,
            admission: None,
            key_provider_kind: "test-attested-lease",
        };
        let standby_runtime = CellRuntime {
            manifest: verified(standby_manifest.clone()),
            volume_path: standby_volume.clone(),
            _runtime_lock: tempfile::tempfile().unwrap(),
            database: Some(Arc::new(RwLock::new(standby_db))),
            application_host: None,
            ha: Some(CellHaRuntime {
                policy: Arc::new(policy.clone()),
                fence: standby_fence,
                object_cipher: standby_cipher.clone(),
                database_path: standby_volume.join("db"),
                replica_signing_key: keys.standby.clone(),
            }),
            device: None,
            grant: None,
            admission: None,
            key_provider_kind: "test-attested-lease",
        };

        {
            let mut database = primary_runtime.database.as_ref().unwrap().write();
            database.create_collection("generic_records").unwrap();
            database
                .insert(
                    "generic_records",
                    Record::new("record-a").with_payload(b"phase5-replicated".to_vec()),
                )
                .unwrap();
        }
        let objects = primary_runtime
            .export_replication_commits(&standby_lease, 0, None, 100)
            .unwrap();
        assert!(!objects.is_empty());
        let replicated_commit_seq = objects.last().unwrap().header.commit_seq;
        for object in &objects {
            standby_runtime
                .apply_replication_commit(&primary_lease, object)
                .unwrap();
        }
        assert_eq!(
            standby_runtime
                .database
                .as_ref()
                .unwrap()
                .read()
                .get("generic_records", "record-a")
                .unwrap()
                .unwrap()
                .payload,
            Some(b"phase5-replicated".to_vec())
        );
        assert!(standby_runtime.application_host().is_none());

        // A later recovery authorization is only valid after the real primary
        // commit boundary has been quiesced and its exact durable sequence has
        // been signed. The resulting acknowledgement is then part of the
        // threshold-certified epoch transition; a fabricated epoch cannot be
        // used as restore authority.
        let fence_ack = primary_runtime.fence_primary("ab".repeat(32)).unwrap();
        assert_eq!(
            fence_ack.statement.final_durable_commit_seq,
            replicated_commit_seq
        );
        assert!(primary_runtime
            .database
            .as_ref()
            .unwrap()
            .write()
            .insert(
                "generic_records",
                Record::new("must-be-fenced").with_payload(Vec::new()),
            )
            .is_err());

        let candidate = standby_runtime
            .create_cell_backup(
                HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3301", "backup_id").unwrap(),
            )
            .unwrap();
        let certified = CertifiedCellBackup {
            approvals: vec![
                sign_cell_backup_approval("recovery-a", &candidate.manifest, &keys.recovery_a)
                    .unwrap(),
                sign_cell_backup_approval("recovery-b", &candidate.manifest, &keys.recovery_b)
                    .unwrap(),
            ],
            manifest: candidate.manifest.clone(),
        };
        let backup_digest = standby_runtime
            .finalize_cell_backup(&candidate, &certified)
            .unwrap();

        let mut recovery_manifest = standby_manifest.clone();
        recovery_manifest.replication.replica_id = recovery_id.as_str().to_string();
        recovery_manifest.replication.role = CellReplicaRole::Recovery;
        recovery_manifest.replication.writer_epoch = 2;
        let promoted_at = current_unix_timestamp().max(fence_ack.statement.fenced_at);
        let recovery_epoch_statement = WriterEpochStatement {
            format: WRITER_EPOCH_FORMAT.to_string(),
            cell_id: epoch.statement.cell_id.clone(),
            group_id: epoch.statement.group_id.clone(),
            writer_epoch: 2,
            primary_replica_id: standby_id.clone(),
            primary_fence_public_key: hex::encode(keys.standby.verifying_key().as_bytes()),
            key_epoch: recovery_manifest.keys.key_epoch,
            manifest_generation: recovery_manifest.manifest_generation,
            manifest_digest: cell_ha_configuration_digest(&recovery_manifest).unwrap(),
            accepted_durable_commit_seq: certified.manifest.durable_commit_seq,
            activated_at: promoted_at,
            predecessor_epoch_digest: Some(epoch_digest.clone()),
            predecessor_primary_lease_digest: Some(
                ha_document_digest(&primary_lease.statement).unwrap(),
            ),
            fence_evidence: Some(FenceEvidence::Graceful {
                acknowledgement: fence_ack,
            }),
        };
        let recovery_epoch = CertifiedWriterEpoch {
            approvals: vec![
                sign_writer_epoch_approval("lease-a", &recovery_epoch_statement, &keys.lease_a)
                    .unwrap(),
                sign_writer_epoch_approval("lease-b", &recovery_epoch_statement, &keys.lease_b)
                    .unwrap(),
            ],
            statement: recovery_epoch_statement,
        };
        let recovery_lease_statement = ReplicaLeaseStatement {
            format: REPLICA_LEASE_FORMAT.to_string(),
            lease_id: HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3203", "lease_id").unwrap(),
            lease_sequence: 1,
            predecessor_lease_digest: None,
            cell_id: recovery_epoch.statement.cell_id.clone(),
            group_id: recovery_epoch.statement.group_id.clone(),
            replica_id: recovery_id.clone(),
            replica_public_key: hex::encode(keys.recovery.verifying_key().as_bytes()),
            role: ReplicaRole::Recovery,
            writer_epoch: 2,
            writer_epoch_digest: ha_document_digest(&recovery_epoch.statement).unwrap(),
            key_epoch: recovery_manifest.keys.key_epoch,
            manifest_generation: recovery_manifest.manifest_generation,
            manifest_digest: recovery_epoch.statement.manifest_digest.clone(),
            local_durable_commit_seq: certified.manifest.durable_commit_seq,
            issued_at: promoted_at,
            not_before: promoted_at,
            expires_at: promoted_at + 240,
        };
        let recovery_lease = CertifiedReplicaLease {
            approvals: lease_approvals(&recovery_lease_statement, &keys),
            statement: recovery_lease_statement,
        };
        let recovery_activation = verify_ha_activation(
            &policy,
            &recovery_epoch,
            &recovery_lease,
            Some(&epoch),
            Some(&primary_lease),
            &HaActivationContext {
                cell_id: recovery_epoch.statement.cell_id.clone(),
                group_id: recovery_epoch.statement.group_id.clone(),
                replica_id: recovery_id.clone(),
                role: ReplicaRole::Recovery,
                writer_epoch: 2,
                key_epoch: recovery_manifest.keys.key_epoch,
                manifest_generation: recovery_manifest.manifest_generation,
                manifest_digest: recovery_epoch.statement.manifest_digest.clone(),
                local_durable_commit_seq: certified.manifest.durable_commit_seq,
                now: current_unix_timestamp(),
            },
        )
        .unwrap();
        let recovery_fence = Arc::new(
            CellHaCommitFence::new(
                Arc::new(policy.clone()),
                recovery_activation,
                Arc::new(SystemHaClock),
            )
            .unwrap(),
        );
        let recovery_runtime = CellRuntime {
            manifest: verified(recovery_manifest),
            volume_path: standby_volume.clone(),
            _runtime_lock: tempfile::tempfile().unwrap(),
            database: None,
            application_host: None,
            ha: Some(CellHaRuntime {
                policy: Arc::new(policy.clone()),
                fence: recovery_fence,
                object_cipher: standby_cipher,
                database_path: standby_volume.join("db"),
                replica_signing_key: keys.recovery.clone(),
            }),
            device: None,
            grant: None,
            admission: None,
            key_provider_kind: "test-attested-lease",
        };
        let restore_statement = RestoreAuthorization {
            format: RESTORE_AUTHORIZATION_FORMAT.to_string(),
            authorization_id: HaId::parse(
                "018f7b30-4f4d-7b5c-a1f6-a183663e3401",
                "authorization_id",
            )
            .unwrap(),
            backup_digest,
            cell_id: certified.manifest.cell_id.clone(),
            group_id: certified.manifest.group_id.clone(),
            lineage_id: certified.manifest.lineage_id.clone(),
            target_replica_id: recovery_id,
            next_writer_epoch: 2,
            issued_at: now - 1,
            expires_at: now + 120,
        };
        let restore = CertifiedRestoreAuthorization {
            approvals: vec![
                sign_restore_authorization_approval(
                    "recovery-a",
                    &restore_statement,
                    &keys.recovery_a,
                )
                .unwrap(),
                sign_restore_authorization_approval(
                    "recovery-b",
                    &restore_statement,
                    &keys.recovery_b,
                )
                .unwrap(),
            ],
            authorization: restore_statement,
        };
        let restore_report = recovery_runtime
            .restore_cell_backup(&certified, &restore, Path::new("drill-a"))
            .unwrap();
        assert!(restore_report.files_restored > 0);
        assert!(standby_volume
            .join(CELL_HA_RESTORE_DIRECTORY)
            .join("drill-a")
            .join(ENCRYPTION_METADATA_FILE)
            .is_file());
        let restored_db = BicDb::open_with_encryption(
            &restore_report.target_path,
            DbConfig::default()
                .with_fsync(true)
                .with_replication(ReplicationConfig {
                    enabled: false,
                    mode: ReplicationMode::Standby,
                    cluster_id: standby_manifest.replication.group_id.clone(),
                    node_id: standby_manifest.replication.replica_id.clone(),
                    ..ReplicationConfig::default()
                })
                .with_required_commit_admission(true),
            EncryptionConfig::with_raw_key(raw_key).with_binding(
                EncryptionBinding::new(
                    standby_manifest.cell_id.as_str(),
                    &standby_manifest.storage.encryption_profile,
                    standby_manifest.keys.key_epoch,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(
            restored_db
                .get("generic_records", "record-a")
                .unwrap()
                .unwrap()
                .payload,
            Some(b"phase5-replicated".to_vec()),
            "Recovery-certified restore must reopen as the exact encrypted database cut"
        );
        restored_db.close().unwrap();

        drop(recovery_runtime);
        primary_runtime.close().unwrap();
        standby_runtime.close().unwrap();
    }

    #[test]
    fn phase6_public_open_inherits_ha_and_constructs_encrypted_device_parent_ledger() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();

        let cell_key = [51_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[41; 32]);
        let kms_signing = SigningKey::from_bytes(&[42; 32]);
        let application_signing = SigningKey::from_bytes(&[43; 32]);
        let device_signing = SigningKey::from_bytes(&[44; 32]);
        let publisher = SigningKey::from_bytes(&[45; 32]);
        let security = SigningKey::from_bytes(&[46; 32]);
        let builder_a = SigningKey::from_bytes(&[47; 32]);
        let builder_b = SigningKey::from_bytes(&[48; 32]);
        let transparency = SigningKey::from_bytes(&[49; 32]);
        let rollout = SigningKey::from_bytes(&[50; 32]);
        let device_exporter = SigningKey::from_bytes(&[70; 32]);
        let ha_keys = cell_ha_test_keys();

        let mut manifest =
            phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&cell_key));
        manifest.runtime.isolation_profile = PHASE6_ISOLATION_PROFILE.to_string();
        manifest.policy.security_profile = PHASE6_SECURITY_PROFILE.to_string();
        manifest.storage.volume_id = "phase6-public-open-volume".to_string();
        manifest.replication.group_id = "phase6-public-open-group".to_string();
        let replica_id = HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3501", "replica_id").unwrap();
        manifest.replication.replica_id = replica_id.as_str().to_string();
        manifest.replication.role = CellReplicaRole::Primary;

        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let package_path = fixture.artifact_root.join(format!(
            "{}.bicdb-app",
            manifest.applications[0].digest.hex()
        ));
        let mut package: ApplicationPackage =
            serde_json::from_slice(&fs::read(package_path).unwrap()).unwrap();
        {
            let application = package.manifest.application.as_deref_mut().unwrap();
            application.auth_schemes.clear();
            application.routes[0].auth_scheme = None;
        }
        package.components = vec![bicdb_app_runtime::ApplicationComponent {
            name: "api".to_string(),
            kind: ApplicationComponentKind::Backend,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Sensitive,
            capabilities: BTreeSet::from([ApplicationCapability::HttpRoutes]),
            egress: BTreeSet::new(),
            database_features: BTreeSet::new(),
        }];
        package
            .modules
            .insert("generic-ledger".to_string(), wasm_module(&package.manifest));
        replace_fixture_package(&fixture, &mut manifest, &mut package, &application_signing);

        let identity_signing = SigningKey::from_bytes(&[6; 32]);
        let identity_policy_bytes = serde_json::to_vec(&CellIdentityPolicy {
            format: CELL_IDENTITY_POLICY_FORMAT.to_string(),
            issuer: "https://identity.example.test".to_string(),
            audience: "generic-cell".to_string(),
            authentication_method: "oidc-ed25519".to_string(),
            maximum_lifetime_seconds: MAX_CELL_HANDOFF_LIFETIME_SECONDS,
            clock_skew_seconds: 5,
            oidc_ed25519_jwks: serde_json::json!({
                "keys": [{
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "alg": "EdDSA",
                    "use": "sig",
                    "kid": "identity-a",
                    "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(identity_signing.verifying_key().as_bytes()),
                }]
            }),
        })
        .unwrap();
        write_restricted(&fixture.identity_policy, &identity_policy_bytes);
        manifest.policy.identity_policy_digest = Sha256Digest::of_bytes(&identity_policy_bytes);

        let authorization_policy_path = directory.path().join("authorization-policy.json");
        let authorization_policy_bytes = serde_json::to_vec(&CellAuthorizationPolicy {
            format: CELL_AUTHORIZATION_POLICY_FORMAT.to_string(),
            cell_id: manifest.cell_id.clone(),
            authorization_epoch: 1,
            session_lifetime_seconds: 300,
            minimum_assurance: "hardware-bound".to_string(),
            members: vec![CellMemberAuthorization {
                user_id: "member-a".to_string(),
                roles: BTreeSet::from(["cell-reader".to_string()]),
                scopes: BTreeSet::from(["records.read".to_string()]),
                devices: vec![CellAuthorizedDevice {
                    device_id: "device-a".to_string(),
                    public_key: hex::encode(device_signing.verifying_key().as_bytes()),
                    enabled: true,
                }],
                enabled: true,
            }],
        })
        .unwrap();
        write_restricted(&authorization_policy_path, &authorization_policy_bytes);
        manifest.policy.authorization_policy_digest =
            Some(Sha256Digest::of_bytes(&authorization_policy_bytes));

        let feature_certification_path = directory.path().join("feature-certification.json");
        let feature_certification_bytes = serde_json::to_vec(&CellFeatureCertification {
            format: CELL_FEATURE_CERTIFICATION_FORMAT.to_string(),
            bicdb_binary_digest: manifest.runtime.bicdb_binary_digest.clone(),
            database_format: manifest.storage.database_format,
            conformance_suite_digest: digest("phase6-public-open-suite"),
            certified_features: BTreeSet::from([ApplicationDatabaseFeature::RowLevelSecurity]),
        })
        .unwrap();
        write_restricted(&feature_certification_path, &feature_certification_bytes);
        manifest.policy.feature_certification_digest =
            Some(Sha256Digest::of_bytes(&feature_certification_bytes));

        let fleet_authorities = [
            ("publisher-a", AuthorityRole::Publisher, &publisher),
            ("security-a", AuthorityRole::Security, &security),
            ("builder-a", AuthorityRole::Builder, &builder_a),
            ("builder-b", AuthorityRole::Builder, &builder_b),
            ("transparency-a", AuthorityRole::Transparency, &transparency),
            ("rollout-a", AuthorityRole::Rollout, &rollout),
        ];
        let fleet_policy = phase4_trust_policy(&fleet_authorities);
        let fleet_policy_bytes = serde_json::to_vec(&fleet_policy).unwrap();
        let fleet_policy_path = directory.path().join("fleet-policy.json");
        write_restricted(&fleet_policy_path, &fleet_policy_bytes);
        manifest.policy.fleet_trust_policy_digest =
            Some(Sha256Digest::of_bytes(&fleet_policy_bytes));

        let ha_policy = cell_ha_test_policy(&ha_keys);
        let ha_policy_bytes = encode_ha_document(&ha_policy).unwrap();
        let ha_policy_path = directory.path().join("ha-policy.cbor");
        write_restricted(&ha_policy_path, &ha_policy_bytes);
        manifest.policy.ha_trust_policy_digest = Some(Sha256Digest::of_bytes(&ha_policy_bytes));

        let device_authority_keys = (0..8)
            .map(|index| SigningKey::from_bytes(&[80 + index as u8; 32]))
            .collect::<Vec<_>>();
        let device_roles = [
            DeviceAuthorityRole::Enrollment,
            DeviceAuthorityRole::Enrollment,
            DeviceAuthorityRole::Authorization,
            DeviceAuthorityRole::Authorization,
            DeviceAuthorityRole::Resolution,
            DeviceAuthorityRole::Resolution,
            DeviceAuthorityRole::Retirement,
            DeviceAuthorityRole::Retirement,
        ];
        let device_policy = DeviceTrustPolicy {
            format: DEVICE_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "generic-device-edge".to_string(),
            authorities: device_authority_keys
                .iter()
                .zip(device_roles)
                .enumerate()
                .map(|(index, (key, role))| DeviceAuthorityKey {
                    key_id: format!("device-authority-{index}"),
                    role,
                    public_key: hex::encode(key.verifying_key().as_bytes()),
                })
                .collect(),
            enrollment_threshold: 2,
            authorization_threshold: 2,
            resolution_threshold: 2,
            retirement_threshold: 2,
            export_keys: vec![DeviceExportKey {
                key_id: "device-export-a".to_string(),
                public_key: hex::encode(device_exporter.verifying_key().as_bytes()),
            }],
            allowed_hardware_profiles: BTreeSet::from(["certified-hardware-v1".to_string()]),
            maximum_offline_seconds: 3_600,
            maximum_local_reauthentication_seconds: 300,
            maximum_upload_grace_seconds: 600,
            maximum_working_set_objects: 1_000,
            maximum_working_set_bytes: 64 * 1024 * 1024,
            maximum_pending_amendments: 1_000,
            maximum_pending_amendment_bytes: 64 * 1024 * 1024,
        };
        let device_policy_bytes = bicdb_cell_device::encode_document(&device_policy).unwrap();
        let device_policy_path = directory.path().join("device-policy.cbor");
        let device_exporter_key_path = directory.path().join("device-exporter.key");
        write_restricted(&device_policy_path, &device_policy_bytes);
        write_restricted(&device_exporter_key_path, &device_exporter.to_bytes());
        manifest.policy.device_trust_policy_digest =
            Some(Sha256Digest::of_bytes(&device_policy_bytes));

        let signed_manifest =
            sign_manifest(&manifest, "manifest-release-a", &manifest_signing).unwrap();
        let trusted = BTreeMap::from([(
            "manifest-release-a".to_string(),
            manifest_signing.verifying_key(),
        )]);
        let verified = verify_manifest_bytes(&signed_manifest, &trusted).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed_manifest);
        bind_volume(&volume, &verified, "manifest-release-a", &manifest_signing).unwrap();

        let now = current_unix_timestamp();
        let fleet_bundle = phase4_activation_bundle(
            &verified,
            &publisher,
            &security,
            &builder_a,
            &builder_b,
            &transparency,
            &rollout,
            now,
        );
        let fleet_bundle_path = directory.path().join("fleet-activation.json");
        write_restricted(
            &fleet_bundle_path,
            &serde_json::to_vec(&fleet_bundle).unwrap(),
        );

        let epoch_statement = WriterEpochStatement {
            format: WRITER_EPOCH_FORMAT.to_string(),
            cell_id: HaId::parse(manifest.cell_id.as_str(), "cell_id").unwrap(),
            group_id: manifest.replication.group_id.clone(),
            writer_epoch: 1,
            primary_replica_id: replica_id.clone(),
            primary_fence_public_key: hex::encode(ha_keys.primary.verifying_key().as_bytes()),
            key_epoch: manifest.keys.key_epoch,
            manifest_generation: manifest.manifest_generation,
            manifest_digest: cell_ha_configuration_digest(&manifest).unwrap(),
            accepted_durable_commit_seq: 0,
            activated_at: now - 1,
            predecessor_epoch_digest: None,
            predecessor_primary_lease_digest: None,
            fence_evidence: None,
        };
        let epoch = CertifiedWriterEpoch {
            approvals: vec![
                sign_writer_epoch_approval("lease-a", &epoch_statement, &ha_keys.lease_a).unwrap(),
                sign_writer_epoch_approval("lease-b", &epoch_statement, &ha_keys.lease_b).unwrap(),
            ],
            statement: epoch_statement,
        };
        let lease_statement = ReplicaLeaseStatement {
            format: REPLICA_LEASE_FORMAT.to_string(),
            lease_id: HaId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e3502", "lease_id").unwrap(),
            lease_sequence: 1,
            predecessor_lease_digest: None,
            cell_id: epoch.statement.cell_id.clone(),
            group_id: epoch.statement.group_id.clone(),
            replica_id: replica_id.clone(),
            replica_public_key: hex::encode(ha_keys.primary.verifying_key().as_bytes()),
            role: ReplicaRole::Primary,
            writer_epoch: 1,
            writer_epoch_digest: ha_document_digest(&epoch.statement).unwrap(),
            key_epoch: manifest.keys.key_epoch,
            manifest_generation: manifest.manifest_generation,
            manifest_digest: epoch.statement.manifest_digest.clone(),
            local_durable_commit_seq: 0,
            issued_at: now - 1,
            not_before: now - 1,
            expires_at: now + 240,
        };
        let lease = CertifiedReplicaLease {
            approvals: lease_approvals(&lease_statement, &ha_keys),
            statement: lease_statement,
        };
        let epoch_path = directory.path().join("writer-epoch.cbor");
        let lease_path = directory.path().join("replica-lease.cbor");
        let replica_key_path = directory.path().join("replica-signing.key");
        write_restricted(&epoch_path, &encode_ha_document(&epoch).unwrap());
        write_restricted(&lease_path, &encode_ha_document(&lease).unwrap());

        let runtime_config = |key_provider: Box<dyn CellKeyProvider>| CellRuntimeConfig {
            manifest_path: manifest_path.clone(),
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root.clone(),
            trusted_manifest_keys: trusted.clone(),
            key_provider,
            application_host: Some(CellApplicationHostConfig {
                release_policy_path: fixture.release_policy.clone(),
                identity_policy_path: fixture.identity_policy.clone(),
                egress_policy_path: fixture.egress_policy.clone(),
                authorization_policy_path: Some(authorization_policy_path.clone()),
                feature_certification_path: Some(feature_certification_path.clone()),
                fleet_trust_policy_path: Some(fleet_policy_path.clone()),
                fleet_activation_bundle_path: Some(fleet_bundle_path.clone()),
                previous_manifest_path: None,
            }),
            ha: Some(CellHaRuntimeConfig {
                trust_policy_path: ha_policy_path.clone(),
                writer_epoch_path: epoch_path.clone(),
                replica_lease_path: lease_path.clone(),
                previous_writer_epoch_path: None,
                previous_primary_lease_path: None,
                replica_signing_key_path: replica_key_path.clone(),
            }),
            device: Some(CellDeviceRuntimeConfig {
                trust_policy_path: device_policy_path.clone(),
                exporter_key_id: "device-export-a".to_string(),
                exporter_signing_key_path: device_exporter_key_path.clone(),
            }),
            grant: None,
            admission: None,
        };

        write_restricted(&replica_key_path, &[99; 32]);
        let error = expect_cell_error(CellRuntime::open(runtime_config(Box::new(
            NeverKeyProvider,
        ))));
        assert!(matches!(error, CellError::SignatureInvalid(_)));
        assert!(!volume.join("db").exists());
        assert!(!volume.join(CELL_RUNTIME_LOCK_FILE).exists());
        write_restricted(&replica_key_path, &ha_keys.primary.to_bytes());

        let nonce = "6e".repeat(32);
        let key_lease = key_lease(&manifest, verified.digest.clone(), cell_key, &nonce);
        let key_provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(sign_cell_key_lease(&key_lease, "kms-a", &kms_signing).unwrap()),
            BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]),
            nonce,
        )
        .unwrap();

        let runtime = CellRuntime::open(runtime_config(Box::new(key_provider))).unwrap();

        assert!(runtime.application_host().unwrap().readiness().ready);
        assert_eq!(
            runtime.admission_report().phase,
            "phase6-hardware-bound-device-edge"
        );
        assert!(!runtime.admission_report().regulated_data_admitted);
        assert_eq!(
            runtime.device_trust_policy().unwrap().policy_id,
            "generic-device-edge"
        );
        let status = runtime.ha_status().unwrap();
        assert_eq!(status.replica_id, replica_id);
        assert_eq!(status.role, ReplicaRole::Primary);
        {
            let mut database = runtime.database.as_ref().unwrap().write();
            database.create_collection("phase5_open_records").unwrap();
            database
                .insert("phase5_open_records", Record::new("record-a"))
                .unwrap();
        }
        let database_commit_seq = runtime
            .database
            .as_ref()
            .unwrap()
            .read()
            .last_applied_commit_seq();
        assert_eq!(
            runtime
                .ha
                .as_ref()
                .unwrap()
                .fence
                .durable_state()
                .unwrap()
                .last_applied_commit_seq,
            database_commit_seq
        );
        runtime.close().unwrap();
    }

    #[test]
    fn phase3_runtime_construction_opens_one_cell_native_application_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key = [9_u8; 32];
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let kms_signing = SigningKey::from_bytes(&[8; 32]);
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let identity_signing = SigningKey::from_bytes(&[6; 32]);
        let device_signing = SigningKey::from_bytes(&[4; 32]);
        let mut manifest = phase2_manifest(current_runtime_digest(), Sha256Digest::of_bytes(&key));
        manifest.runtime.isolation_profile = PHASE3_ISOLATION_PROFILE.to_string();
        manifest.policy.security_profile = PHASE3_SECURITY_PROFILE.to_string();
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);

        let package_path = fixture.artifact_root.join(format!(
            "{}.bicdb-app",
            manifest.applications[0].digest.hex()
        ));
        let mut package: ApplicationPackage =
            serde_json::from_slice(&fs::read(package_path).unwrap()).unwrap();
        {
            let application = package.manifest.application.as_deref_mut().unwrap();
            application.auth_schemes.clear();
            application.routes[0].auth_scheme = None;
        }
        package.frontend_assets.insert(
            "index.html".to_string(),
            FrontendAsset {
                content_type: "text/html; charset=utf-8".to_string(),
                bytes: b"<!doctype html><title>Generic Cell</title>".to_vec(),
            },
        );
        package.components = vec![
            bicdb_app_runtime::ApplicationComponent {
                name: "api".to_string(),
                kind: ApplicationComponentKind::Backend,
                scope: ExecutionScope::Cell,
                data_class: DataClass::Sensitive,
                capabilities: BTreeSet::from([ApplicationCapability::HttpRoutes]),
                egress: BTreeSet::new(),
                database_features: BTreeSet::new(),
            },
            bicdb_app_runtime::ApplicationComponent {
                name: "web".to_string(),
                kind: ApplicationComponentKind::Frontend,
                scope: ExecutionScope::Cell,
                data_class: DataClass::Sensitive,
                capabilities: BTreeSet::from([ApplicationCapability::FrontendAssets]),
                egress: BTreeSet::new(),
                database_features: BTreeSet::new(),
            },
        ];
        package
            .modules
            .insert("generic-ledger".to_string(), wasm_module(&package.manifest));
        replace_fixture_package(&fixture, &mut manifest, &mut package, &application_signing);

        let identity_jwks = serde_json::json!({
            "keys": [{
                "kty": "OKP",
                "crv": "Ed25519",
                "alg": "EdDSA",
                "use": "sig",
                "kid": "identity-a",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(identity_signing.verifying_key().as_bytes()),
            }]
        });
        let identity_policy_bytes = serde_json::to_vec(&CellIdentityPolicy {
            format: CELL_IDENTITY_POLICY_FORMAT.to_string(),
            issuer: "https://identity.example.test".to_string(),
            audience: "generic-cell".to_string(),
            authentication_method: "oidc-ed25519".to_string(),
            maximum_lifetime_seconds: MAX_CELL_HANDOFF_LIFETIME_SECONDS,
            clock_skew_seconds: 5,
            oidc_ed25519_jwks: identity_jwks,
        })
        .unwrap();
        write_restricted(&fixture.identity_policy, &identity_policy_bytes);
        manifest.policy.identity_policy_digest = Sha256Digest::of_bytes(&identity_policy_bytes);

        let authorization_policy = directory.path().join("authorization-policy.json");
        let authorization_policy_bytes = serde_json::to_vec(&CellAuthorizationPolicy {
            format: CELL_AUTHORIZATION_POLICY_FORMAT.to_string(),
            cell_id: manifest.cell_id.clone(),
            authorization_epoch: 1,
            session_lifetime_seconds: 300,
            minimum_assurance: "hardware-bound".to_string(),
            members: vec![CellMemberAuthorization {
                user_id: "member-a".to_string(),
                roles: BTreeSet::from(["cell-reader".to_string()]),
                scopes: BTreeSet::from(["records.read".to_string()]),
                devices: vec![CellAuthorizedDevice {
                    device_id: "device-a".to_string(),
                    public_key: hex::encode(device_signing.verifying_key().as_bytes()),
                    enabled: true,
                }],
                enabled: true,
            }],
        })
        .unwrap();
        write_restricted(&authorization_policy, &authorization_policy_bytes);
        manifest.policy.authorization_policy_digest =
            Some(Sha256Digest::of_bytes(&authorization_policy_bytes));

        let feature_certification = directory.path().join("feature-certification.json");
        let feature_certification_bytes = serde_json::to_vec(&CellFeatureCertification {
            format: CELL_FEATURE_CERTIFICATION_FORMAT.to_string(),
            bicdb_binary_digest: manifest.runtime.bicdb_binary_digest.clone(),
            database_format: manifest.storage.database_format,
            conformance_suite_digest: digest("phase3-exact-binary-suite"),
            certified_features: BTreeSet::from([ApplicationDatabaseFeature::RowLevelSecurity]),
        })
        .unwrap();
        write_restricted(&feature_certification, &feature_certification_bytes);
        manifest.policy.feature_certification_digest =
            Some(Sha256Digest::of_bytes(&feature_certification_bytes));

        let signed_manifest =
            sign_manifest(&manifest, "manifest-release-a", &manifest_signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed_manifest);
        let trusted = BTreeMap::from([(
            "manifest-release-a".to_string(),
            manifest_signing.verifying_key(),
        )]);
        let verified = verify_manifest_bytes(&signed_manifest, &trusted).unwrap();
        bind_volume(&volume, &verified, "manifest-release-a", &manifest_signing).unwrap();
        let nonce = "5e".repeat(32);
        let lease = key_lease(&manifest, verified.digest.clone(), key, &nonce);
        let provider = AttestedKeyLeaseCellKeyProvider::new(
            Cursor::new(sign_cell_key_lease(&lease, "kms-a", &kms_signing).unwrap()),
            BTreeMap::from([("kms-a".to_string(), kms_signing.verifying_key())]),
            nonce,
        )
        .unwrap();

        let runtime = CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume,
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root,
            trusted_manifest_keys: trusted,
            key_provider: Box::new(provider),
            application_host: Some(CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: Some(authorization_policy),
                feature_certification_path: Some(feature_certification),
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            ha: None,
            device: None,
            grant: None,
            admission: None,
        })
        .unwrap();
        let host = runtime
            .application_host()
            .expect("Phase 3 application host");
        assert!(host.readiness().ready);
        assert_eq!(
            runtime.admission_report().phase,
            "phase3-cell-native-application"
        );
        assert!(!runtime.admission_report().regulated_data_admitted);

        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        async_runtime.block_on(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = host
                .serve_http(listener, HttpHostPolicy::default())
                .await
                .unwrap();
            let mut client = tokio::net::TcpStream::connect(server.address)
                .await
                .unwrap();
            client
                .write_all(b"GET / HTTP/1.1\r\nHost: cell.test\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8_lossy(&response);
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(response.contains("Generic Cell"), "{response}");
            assert!(response
                .to_ascii_lowercase()
                .contains("content-security-policy"));
            server.shutdown().await.unwrap();
        });
        runtime.close().unwrap();
    }

    #[test]
    fn regulated_data_classes_remain_fail_closed_before_regulated_admission() {
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        manifest.applications.push(CellApplicationPin {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "1.0.0".to_string(),
            digest: digest("generic-ledger"),
            schema_generation: 1,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Regulated,
        });
        assert!(matches!(
            manifest.validate(),
            Err(CellError::RegulatedAdmissionIncomplete(_))
        ));
        manifest.applications[0].data_class = DataClass::RegulatedLocal;
        assert!(matches!(
            manifest.validate(),
            Err(CellError::RegulatedAdmissionIncomplete(_))
        ));
    }

    fn phase8_admission_keys() -> Vec<(String, AdmissionAuthorityRole, SigningKey)> {
        [
            (
                "admission-a",
                AdmissionAuthorityRole::AdmissionAuthority,
                71,
            ),
            (
                "admission-b",
                AdmissionAuthorityRole::AdmissionAuthority,
                72,
            ),
            (
                "deployment-a",
                AdmissionAuthorityRole::DeploymentAttestor,
                73,
            ),
            (
                "deployment-b",
                AdmissionAuthorityRole::DeploymentAttestor,
                74,
            ),
            ("evidence-a", AdmissionAuthorityRole::EvidenceReviewer, 75),
            ("evidence-b", AdmissionAuthorityRole::EvidenceReviewer, 76),
            (
                "transparency-a",
                AdmissionAuthorityRole::TransparencyWitness,
                77,
            ),
            (
                "transparency-b",
                AdmissionAuthorityRole::TransparencyWitness,
                78,
            ),
        ]
        .into_iter()
        .map(|(key_id, role, seed)| {
            (
                key_id.to_string(),
                role,
                SigningKey::from_bytes(&[seed; 32]),
            )
        })
        .collect()
    }

    fn phase8_admission_policy(
        keys: &[(String, AdmissionAuthorityRole, SigningKey)],
    ) -> AdmissionTrustPolicy {
        AdmissionTrustPolicy {
            format: ADMISSION_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "phase8-integration-policy".to_string(),
            generation: 1,
            authorities: keys
                .iter()
                .map(|(key_id, role, key)| AdmissionAuthorityKey {
                    key_id: key_id.clone(),
                    role: *role,
                    public_key: hex::encode(key.verifying_key().as_bytes()),
                })
                .collect(),
            thresholds: AdmissionAuthorityRole::ALL
                .into_iter()
                .map(|role| (role, 2))
                .collect(),
            maximum_clock_skew_seconds: 30,
            maximum_evidence_lifetime_seconds: 7200,
            maximum_attestation_lifetime_seconds: 600,
            maximum_authorization_lifetime_seconds: 300,
            allowed_isolation_tiers: BTreeSet::from(["confidential-microvm".to_string()]),
            trusted_attestation_verifier_ids: BTreeSet::from([
                "integration-attestation-verifier".to_string()
            ]),
        }
    }

    fn phase8_approvals<T: Serialize>(
        domain: AdmissionSignatureDomain,
        role: AdmissionAuthorityRole,
        payload: &T,
        keys: &[(String, AdmissionAuthorityRole, SigningKey)],
    ) -> Vec<AdmissionApproval> {
        keys.iter()
            .filter(|(_, key_role, _)| *key_role == role)
            .map(|(key_id, _, key)| {
                sign_admission_approval(domain, role, key_id, payload, key).unwrap()
            })
            .collect()
    }

    fn phase8_admission_bundle(
        policy: &AdmissionTrustPolicy,
        subject: &AdmissionSubject,
        keys: &[(String, AdmissionAuthorityRole, SigningKey)],
        now: i64,
    ) -> AdmissionBundle {
        let gate_evidence = AdmissionGate::ALL
            .into_iter()
            .enumerate()
            .map(|(index, gate)| {
                let evidence = GateEvidence {
                    format: GATE_EVIDENCE_FORMAT.to_string(),
                    evidence_id: Uuid::from_u128(10_000 + index as u128)
                        .hyphenated()
                        .to_string(),
                    gate,
                    subject: subject.clone(),
                    tests: EvidenceTestSummary {
                        executed: 4,
                        passed: 4,
                        failed: 0,
                        skipped: 0,
                        fixture_digest: AdmissionDigest::of_bytes(
                            format!("fixture-{index}").as_bytes(),
                        ),
                        result_digest: AdmissionDigest::of_bytes(
                            format!("result-{index}").as_bytes(),
                        ),
                    },
                    artifacts: vec![EvidenceArtifact {
                        artifact_digest: AdmissionDigest::of_bytes(
                            format!("artifact-{index}").as_bytes(),
                        ),
                        provenance_digest: AdmissionDigest::of_bytes(
                            format!("provenance-{index}").as_bytes(),
                        ),
                        media_type: "application/vnd.bicdb.evidence+cbor".to_string(),
                        size_bytes: 4096,
                        produced_at: now - 120,
                    }],
                    review_disciplines: if gate == AdmissionGate::IndependentReview {
                        ReviewDiscipline::ALL.to_vec()
                    } else {
                        Vec::new()
                    },
                    issued_at: now - 60,
                    expires_at: now + 3600,
                };
                CertifiedGateEvidence {
                    approvals: phase8_approvals(
                        AdmissionSignatureDomain::GateEvidence,
                        AdmissionAuthorityRole::EvidenceReviewer,
                        &evidence,
                        keys,
                    ),
                    evidence,
                }
            })
            .collect::<Vec<_>>();

        let attestation = DeploymentAttestation {
            format: DEPLOYMENT_ATTESTATION_FORMAT.to_string(),
            attestation_id: Uuid::from_u128(20_000).hyphenated().to_string(),
            subject: subject.clone(),
            attestation_verifier_id: "integration-attestation-verifier".to_string(),
            attestation_report_digest: AdmissionDigest::of_bytes(b"attestation-report"),
            workload_identity_digest: AdmissionDigest::of_bytes(b"workload-identity"),
            kms_scope_digest: AdmissionDigest::of_bytes(b"kms-scope"),
            capability_graph_digest: AdmissionDigest::of_bytes(b"capability-graph"),
            storage_mount_digest: AdmissionDigest::of_bytes(b"storage-mount"),
            network_policy_digest: AdmissionDigest::of_bytes(b"network-policy"),
            crash_dump_policy_digest: AdmissionDigest::of_bytes(b"crash-dump-policy"),
            observability_policy_digest: AdmissionDigest::of_bytes(b"observability-policy"),
            nonce: "8a".repeat(32),
            issued_at: now - 30,
            expires_at: now + 300,
        };
        let deployment = CertifiedDeploymentAttestation {
            approvals: phase8_approvals(
                AdmissionSignatureDomain::DeploymentAttestation,
                AdmissionAuthorityRole::DeploymentAttestor,
                &attestation,
                keys,
            ),
            attestation,
        };
        let gate_evidence_digests = gate_evidence
            .iter()
            .map(|evidence| admission_document_digest(evidence).unwrap())
            .collect::<Vec<_>>();
        let deployment_attestation_digest = admission_document_digest(&deployment).unwrap();
        let checkpoint = EvidenceCheckpoint {
            format: EVIDENCE_CHECKPOINT_FORMAT.to_string(),
            sequence: 1,
            previous_checkpoint_digest: None,
            gate_evidence_digests: gate_evidence_digests.clone(),
            deployment_attestation_digest: deployment_attestation_digest.clone(),
            issued_at: now - 20,
        };
        let checkpoint = CertifiedEvidenceCheckpoint {
            approvals: phase8_approvals(
                AdmissionSignatureDomain::EvidenceCheckpoint,
                AdmissionAuthorityRole::TransparencyWitness,
                &checkpoint,
                keys,
            ),
            checkpoint,
        };
        let authorization = AdmissionAuthorization {
            format: ADMISSION_AUTHORIZATION_FORMAT.to_string(),
            authorization_id: Uuid::from_u128(30_000).hyphenated().to_string(),
            policy_digest: admission_document_digest(policy).unwrap(),
            subject: subject.clone(),
            gate_evidence_digests,
            deployment_attestation_digest,
            evidence_checkpoint_digest: admission_document_digest(&checkpoint).unwrap(),
            nonce: "8a".repeat(32),
            issued_at: now - 10,
            not_before: now - 5,
            expires_at: now + 240,
        };
        let authorization = CertifiedAdmissionAuthorization {
            approvals: phase8_approvals(
                AdmissionSignatureDomain::AdmissionAuthorization,
                AdmissionAuthorityRole::AdmissionAuthority,
                &authorization,
                keys,
            ),
            authorization,
        };
        AdmissionBundle {
            format: ADMISSION_BUNDLE_FORMAT.to_string(),
            gate_evidence,
            deployment,
            checkpoint,
            authorization,
        }
    }

    #[test]
    fn phase8_profile_verifies_exact_evidence_and_enables_regulated_admission() {
        let directory = tempfile::tempdir().unwrap();
        let keys = phase8_admission_keys();
        let policy = phase8_admission_policy(&keys);
        let policy_bytes = encode_admission_document(&policy).unwrap();
        let policy_path = directory.path().join("admission-policy.cbor");
        write_restricted(&policy_path, &policy_bytes);

        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        manifest.storage.encryption_profile = PHASE2_ENCRYPTION_PROFILE.to_string();
        manifest.runtime.isolation_profile = PHASE8_ISOLATION_PROFILE.to_string();
        manifest.policy.security_profile = PHASE8_SECURITY_PROFILE.to_string();
        manifest.keys.authority = "kms+attested://cell-a".to_string();
        manifest.replication.replica_id = "018f7b30-4f4d-7b5c-a1f6-a183663e1241".to_string();
        manifest.applications.push(CellApplicationPin {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "1.0.0".to_string(),
            digest: digest("generic-ledger"),
            schema_generation: 1,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Sensitive,
        });
        manifest.policy.authorization_policy_digest = Some(digest("authorization"));
        manifest.policy.feature_certification_digest = Some(digest("feature-certification"));
        manifest.policy.fleet_trust_policy_digest = Some(digest("fleet"));
        manifest.policy.ha_trust_policy_digest = Some(digest("ha"));
        manifest.policy.device_trust_policy_digest = Some(digest("device"));
        manifest.policy.grant_trust_policy_digest = Some(digest("grant"));
        manifest.policy.admission_trust_policy_digest = Some(Sha256Digest::of_bytes(&policy_bytes));
        manifest.applications[0].data_class = DataClass::Regulated;
        manifest.validate().unwrap();
        assert!(manifest.uses_phase8_hardened_fleet());
        assert!(manifest.uses_cross_cell_grants());
        assert!(manifest.uses_device_edge());
        assert!(manifest.uses_cell_ha());
        assert!(manifest.uses_fleet_lifecycle());

        let verified_manifest = VerifiedCellManifest {
            manifest,
            digest: digest("exact-phase8-manifest"),
            signer_key_id: "manifest-release-a".to_string(),
        };
        let subject = admission_subject(&verified_manifest, "confidential-microvm").unwrap();
        let now = current_unix_timestamp();
        let bundle = phase8_admission_bundle(&policy, &subject, &keys, now);
        let bundle_path = directory.path().join("admission-bundle.cbor");
        write_restricted(&bundle_path, &encode_admission_document(&bundle).unwrap());
        let config = CellAdmissionRuntimeConfig {
            trust_policy_path: policy_path,
            evidence_bundle_path: bundle_path,
            expected_deployment_isolation_tier: "confidential-microvm".to_string(),
        };

        let prepared = prepare_cell_admission(&verified_manifest, Some(&config))
            .unwrap()
            .expect("Phase 8 evidence");
        assert_eq!(prepared.lease.evidence.verified_gates.len(), 12);
        assert!(prepared.lease.evidence.evidence_complete);
        assert!(prepared.lease.evidence.regulated_admission_enabled);
        prepared.lease.check().unwrap();

        let admitted_report = CellAdmissionReport::phase8_hardened_fleet(true);
        assert!(admitted_report.regulated_data_admitted);
        assert!(admitted_report.missing.is_empty());
        let denied_report = CellAdmissionReport::phase8_hardened_fleet(false);
        assert!(!denied_report.regulated_data_admitted);
        assert!(!denied_report.missing.is_empty());

        let wrong_tier = CellAdmissionRuntimeConfig {
            expected_deployment_isolation_tier: "ordinary-process".to_string(),
            ..config
        };
        assert!(matches!(
            prepare_cell_admission(&verified_manifest, Some(&wrong_tier)),
            Err(CellError::Admission(_))
        ));

        // Exercise the actual runtime status accessor with verified evidence
        // and a fake clock, without restarting or re-verifying the bundle.
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(
            prepared.lease.evidence.authorization_expires_at - 1,
        ));
        let lease =
            CellAdmissionLease::with_test_clock(prepared.lease.evidence.clone(), clock.clone());
        let runtime = CellRuntime {
            manifest: verified_manifest,
            volume_path: directory.path().to_path_buf(),
            _runtime_lock: tempfile::tempfile().unwrap(),
            database: None,
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: Some(Arc::new(lease)),
            key_provider_kind: "test",
        };
        assert!(runtime.admission_report().regulated_data_admitted);
        clock.store(
            prepared
                .lease
                .evidence
                .evidence_expires_at
                .max(prepared.lease.evidence.deployment_expires_at)
                .max(prepared.lease.evidence.authorization_expires_at)
                + 1,
            std::sync::atomic::Ordering::SeqCst,
        );
        assert!(!runtime.admission_report().regulated_data_admitted);
        clock.store(now, std::sync::atomic::Ordering::SeqCst);
        assert!(!runtime.admission_report().regulated_data_admitted);
    }

    #[test]
    fn phase7_profile_is_cumulative_callable_and_not_regulated_admitted() {
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        manifest.storage.encryption_profile = PHASE2_ENCRYPTION_PROFILE.to_string();
        manifest.runtime.isolation_profile = PHASE7_ISOLATION_PROFILE.to_string();
        manifest.policy.security_profile = PHASE7_SECURITY_PROFILE.to_string();
        manifest.keys.authority = "kms+attested://cell-a".to_string();
        manifest.replication.replica_id = "018f7b30-4f4d-7b5c-a1f6-a183663e1241".to_string();
        manifest.applications.push(CellApplicationPin {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "1.0.0".to_string(),
            digest: digest("generic-ledger"),
            schema_generation: 1,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Sensitive,
        });
        manifest.policy.authorization_policy_digest = Some(digest("authorization"));
        manifest.policy.feature_certification_digest = Some(digest("feature-certification"));
        manifest.policy.fleet_trust_policy_digest = Some(digest("fleet"));
        manifest.policy.ha_trust_policy_digest = Some(digest("ha"));
        manifest.policy.device_trust_policy_digest = Some(digest("device"));
        manifest.policy.grant_trust_policy_digest = Some(digest("grant"));

        manifest.validate().unwrap();
        assert!(manifest.uses_phase7_cross_cell_grants());
        assert!(manifest.uses_device_edge());
        assert!(manifest.uses_cell_ha());
        assert!(manifest.uses_fleet_lifecycle());
        assert!(manifest.uses_cell_native());
        let report = CellAdmissionReport::phase7_cross_cell_grants();
        assert_eq!(report.phase, "phase7-cross-cell-object-grants");
        assert!(!report.regulated_data_admitted);

        manifest.policy.grant_trust_policy_digest = None;
        assert!(matches!(
            manifest.validate(),
            Err(CellError::RegulatedAdmissionIncomplete(_))
        ));
    }

    #[test]
    fn cell_application_routes_cannot_be_public() {
        let directory = tempfile::tempdir().unwrap();
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let path = fixture.artifact_root.join(format!(
            "{}.bicdb-app",
            manifest.applications[0].digest.hex()
        ));
        let mut package: ApplicationPackage =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let route = &mut package.manifest.application.as_deref_mut().unwrap().routes[0];
        route.public = true;
        route.auth_scheme = None;
        replace_fixture_package(&fixture, &mut manifest, &mut package, &application_signing);

        let error = match prepare_cell_applications(
            &fixture.artifact_root,
            &manifest,
            Some(&CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: None,
                feature_certification_path: None,
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            false,
        ) {
            Err(error) => error,
            Ok(_) => panic!("public cell route was accepted"),
        };
        assert!(matches!(error, CellError::ArtifactMismatch(_)));
        assert!(error.to_string().contains("must authenticate"));
    }

    #[test]
    fn application_authentication_contract_must_match_pinned_identity_policy() {
        let directory = tempfile::tempdir().unwrap();
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let path = fixture.artifact_root.join(format!(
            "{}.bicdb-app",
            manifest.applications[0].digest.hex()
        ));
        let mut package: ApplicationPackage =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        package
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .auth_schemes
            .get_mut("cell_identity")
            .unwrap()
            .audience = "another-cell".to_string();
        replace_fixture_package(&fixture, &mut manifest, &mut package, &application_signing);

        let error = match prepare_cell_applications(
            &fixture.artifact_root,
            &manifest,
            Some(&CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: None,
                feature_certification_path: None,
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            false,
        ) {
            Err(error) => error,
            Ok(_) => panic!("mismatched application authentication contract was accepted"),
        };
        assert!(matches!(error, CellError::ArtifactMismatch(_)));
        assert!(error.to_string().contains("pinned identity policy"));
    }

    #[test]
    fn failed_application_activation_does_not_advance_manifest_state() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let package_path = fixture.artifact_root.join(format!(
            "{}.bicdb-app",
            manifest.applications[0].digest.hex()
        ));
        let mut package: ApplicationPackage =
            serde_json::from_slice(&fs::read(package_path).unwrap()).unwrap();
        package
            .modules
            .insert("generic-ledger".to_string(), b"not-wasm".to_vec());
        replace_fixture_package(&fixture, &mut manifest, &mut package, &application_signing);

        let signed = sign_manifest(&manifest, "manifest-release-a", &manifest_signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([(
            "manifest-release-a".to_string(),
            manifest_signing.verifying_key(),
        )]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "manifest-release-a", &manifest_signing).unwrap();

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root,
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: Some(CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: None,
                feature_certification_path: None,
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::Application(_)));
        assert!(
            !volume.join(CELL_STATE_FILE).exists(),
            "failed activation must not become the cell's monotonic active release"
        );
    }

    #[test]
    fn identity_policy_cannot_weaken_cell_token_bounds() {
        let directory = tempfile::tempdir().unwrap();
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);
        let identity_signing_key = SigningKey::from_bytes(&[6; 32]);
        let weak_policy = serde_json::to_vec(&CellIdentityPolicy {
            format: CELL_IDENTITY_POLICY_FORMAT.to_string(),
            issuer: "https://identity.example.test".to_string(),
            audience: "generic-cell".to_string(),
            authentication_method: "oidc-ed25519".to_string(),
            maximum_lifetime_seconds: MAX_CELL_ACCESS_TOKEN_LIFETIME_SECONDS + 1,
            clock_skew_seconds: 30,
            oidc_ed25519_jwks: serde_json::json!({
                "keys": [{
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "alg": "EdDSA",
                    "use": "sig",
                    "kid": "identity-a",
                    "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(identity_signing_key.verifying_key().as_bytes()),
                }]
            }),
        })
        .unwrap();
        write_restricted(&fixture.identity_policy, &weak_policy);
        manifest.policy.identity_policy_digest = Sha256Digest::of_bytes(&weak_policy);

        assert!(matches!(
            prepare_cell_applications(
                &fixture.artifact_root,
                &manifest,
                Some(&CellApplicationHostConfig {
                    release_policy_path: fixture.release_policy,
                    identity_policy_path: fixture.identity_policy,
                    egress_policy_path: fixture.egress_policy,
                    authorization_policy_path: None,
                    feature_certification_path: None,
                    fleet_trust_policy_path: None,
                    fleet_activation_bundle_path: None,
                    previous_manifest_path: None,
                }),
                false,
            ),
            Err(CellError::ArtifactMismatch(_))
        ));
    }

    #[test]
    fn application_release_key_cannot_substitute_for_manifest_activation_authority() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let manifest_signing = SigningKey::from_bytes(&[7; 32]);
        let application_signing = SigningKey::from_bytes(&[5; 32]);
        let untrusted_application_signing = SigningKey::from_bytes(&[4; 32]);
        let mut manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let fixture =
            write_application_fixture(directory.path(), &mut manifest, &application_signing);

        // The activation authority signs an internally coherent manifest and
        // policy, but that policy does not trust the key which signed the
        // package. Outer digest pinning alone must not make the package valid.
        let replacement_release_policy = serde_json::to_vec(&CellReleasePolicy {
            format: CELL_RELEASE_POLICY_FORMAT.to_string(),
            required_package_signatures: 1,
            roots: vec![CellReleaseRoot {
                root: "generic-suite".to_string(),
                signing_keys: BTreeMap::from([(
                    "application-release-a".to_string(),
                    hex::encode(untrusted_application_signing.verifying_key().as_bytes()),
                )]),
            }],
        })
        .unwrap();
        write_restricted(&fixture.release_policy, &replacement_release_policy);
        manifest.policy.trusted_release_policy_digest =
            Sha256Digest::of_bytes(&replacement_release_policy);

        let signed = sign_manifest(&manifest, "manifest-release-a", &manifest_signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([(
            "manifest-release-a".to_string(),
            manifest_signing.verifying_key(),
        )]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "manifest-release-a", &manifest_signing).unwrap();

        let error = expect_cell_error(CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: fixture.artifact_root,
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: Some(CellApplicationHostConfig {
                release_policy_path: fixture.release_policy,
                identity_policy_path: fixture.identity_policy,
                egress_policy_path: fixture.egress_policy,
                authorization_policy_path: None,
                feature_certification_path: None,
                fleet_trust_policy_path: None,
                fleet_activation_bundle_path: None,
                previous_manifest_path: None,
            }),
            ha: None,
            device: None,
            grant: None,
            admission: None,
        }));
        assert!(matches!(error, CellError::Application(_)));
        assert!(!volume.join("db").exists());
    }

    #[test]
    fn phase1_runtime_opens_one_bound_encrypted_database_and_denies_regulated_admission() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let manifest = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let signed = sign_manifest(&manifest, "release-a", &signing).unwrap();
        let manifest_path = directory.path().join("cell.cbor");
        write_restricted(&manifest_path, &signed);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified = verify_manifest_bytes(&signed, &trusted).unwrap();
        bind_volume(&volume, &verified, "release-a", &signing).unwrap();

        let runtime = CellRuntime::open(CellRuntimeConfig {
            manifest_path,
            volume_path: volume.clone(),
            expected_cell_id: manifest.cell_id.clone(),
            expected_volume_id: manifest.storage.volume_id.clone(),
            expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
            artifact_root: directory.path().to_path_buf(),
            trusted_manifest_keys: trusted,
            key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                key_path,
                manifest.cell_id,
                "kek-a".to_string(),
            )),
            application_host: None,
            ha: None,
            device: None,
            grant: None,
            admission: None,
        })
        .unwrap();
        assert!(volume.join("db").exists());
        assert!(!runtime.admission_report().regulated_data_admitted);
        runtime.close().unwrap();
    }

    #[test]
    fn signed_manifest_transition_advances_once_and_rejects_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let volume = directory.path().join("volume");
        fs::create_dir(&volume).unwrap();
        let key_path = directory.path().join("cell.key");
        write_restricted(&key_path, &[9; 32]);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let generation_one = manifest(current_runtime_digest(), Sha256Digest::of_bytes(&[9; 32]));
        let signed_one = sign_manifest(&generation_one, "release-a", &signing).unwrap();
        let path_one = directory.path().join("generation-one.cbor");
        write_restricted(&path_one, &signed_one);
        let trusted = BTreeMap::from([("release-a".to_string(), signing.verifying_key())]);
        let verified_one = verify_manifest_bytes(&signed_one, &trusted).unwrap();

        let unbound_volume = directory.path().join("unbound-volume");
        fs::create_dir(&unbound_volume).unwrap();
        let mut generation_two_without_root = generation_one.clone();
        generation_two_without_root.manifest_generation = 2;
        generation_two_without_root.previous_manifest_digest = Some(verified_one.digest.clone());
        let signed_two_without_root =
            sign_manifest(&generation_two_without_root, "release-a", &signing).unwrap();
        let verified_two_without_root =
            verify_manifest_bytes(&signed_two_without_root, &trusted).unwrap();
        let bind_error = bind_volume(
            &unbound_volume,
            &verified_two_without_root,
            "release-a",
            &signing,
        )
        .unwrap_err();
        assert!(matches!(bind_error, CellError::ManifestRollback(_)));
        assert!(!unbound_volume.join(CELL_IDENTITY_FILE).exists());

        bind_volume(&volume, &verified_one, "release-a", &signing).unwrap();

        let open = |manifest_path: PathBuf, manifest: &CellManifest| {
            CellRuntime::open(CellRuntimeConfig {
                manifest_path,
                volume_path: volume.clone(),
                expected_cell_id: manifest.cell_id.clone(),
                expected_volume_id: manifest.storage.volume_id.clone(),
                expected_guest_image_digest: manifest.runtime.guest_image_digest.clone(),
                artifact_root: directory.path().to_path_buf(),
                trusted_manifest_keys: trusted.clone(),
                key_provider: Box::new(DevelopmentFileCellKeyProvider::new(
                    key_path.clone(),
                    manifest.cell_id.clone(),
                    "kek-a".to_string(),
                )),
                application_host: None,
                ha: None,
                device: None,
                grant: None,
                admission: None,
            })
        };

        open(path_one.clone(), &generation_one)
            .unwrap()
            .close()
            .unwrap();

        let mut generation_two = generation_one.clone();
        generation_two.manifest_generation = 2;
        generation_two.previous_manifest_digest = Some(verified_one.digest.clone());
        let signed_two = sign_manifest(&generation_two, "release-a", &signing).unwrap();
        let path_two = directory.path().join("generation-two.cbor");
        write_restricted(&path_two, &signed_two);
        open(path_two, &generation_two).unwrap().close().unwrap();

        let state_before = fs::read(volume.join(CELL_STATE_FILE)).unwrap();

        let mut generation_four = generation_two.clone();
        generation_four.manifest_generation = 4;
        generation_four.previous_manifest_digest =
            Some(verify_manifest_bytes(&signed_two, &trusted).unwrap().digest);
        let signed_four = sign_manifest(&generation_four, "release-a", &signing).unwrap();
        let path_four = directory.path().join("generation-four.cbor");
        write_restricted(&path_four, &signed_four);
        let gap_error = expect_cell_error(open(path_four, &generation_four));
        assert!(matches!(gap_error, CellError::ManifestRollback(_)));
        assert_eq!(
            fs::read(volume.join(CELL_STATE_FILE)).unwrap(),
            state_before,
            "generation-gap refusal must not mutate monotonic state"
        );

        let error = expect_cell_error(open(path_one, &generation_one));
        assert!(matches!(error, CellError::ManifestRollback(_)));
        assert_eq!(
            fs::read(volume.join(CELL_STATE_FILE)).unwrap(),
            state_before,
            "rollback refusal must not mutate monotonic state"
        );
    }

    fn phase3_external_authenticator(signing: &SigningKey) -> JwtAuthenticator {
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "OKP",
                "crv": "Ed25519",
                "alg": "EdDSA",
                "use": "sig",
                "kid": "identity-a",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(signing.verifying_key().as_bytes()),
            }]
        });
        JwtAuthenticator::oidc_ed25519(
            JwtConfiguration {
                issuer: "https://identity.example.test".to_string(),
                audience: "generic-cell".to_string(),
                authentication_method: "oidc-device-handoff".to_string(),
                maximum_lifetime_seconds: MAX_CELL_HANDOFF_LIFETIME_SECONDS,
                clock_skew_seconds: 5,
            },
            &serde_json::to_vec(&jwks).unwrap(),
        )
        .unwrap()
    }

    fn phase3_handoff_assertion(
        signing: &SigningKey,
        cell_id: &CellId,
        device_id: &str,
        nonce: &str,
        assertion_id: &str,
    ) -> String {
        let encode = |value: &serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(value).unwrap())
        };
        let now = unix_seconds();
        let header = encode(&serde_json::json!({
            "alg": "EdDSA",
            "typ": "JWT",
            "kid": "identity-a"
        }));
        let payload = encode(&serde_json::json!({
            "sub": "member-a",
            "client_id": device_id,
            "session_id": assertion_id,
            "roles": ["external-global-admin"],
            "scopes": ["external-all"],
            "assurance_level": "hardware-bound",
            "iss": "https://identity.example.test",
            "aud": "generic-cell",
            "iat": now,
            "nbf": now - 1,
            "exp": now + 60,
            "token_type": "access",
            "policy": {
                "cell_id": cell_id.as_str(),
                "handoff_nonce": nonce
            }
        }));
        let signature = signing.sign(format!("{header}.{payload}").as_bytes());
        format!(
            "{header}.{payload}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes())
        )
    }

    fn phase3_device_proof(
        signing: &SigningKey,
        cell_id: &CellId,
        device_id: &str,
        assertion: &str,
        nonce: &str,
    ) -> String {
        let mut message = b"BICDB-CELL-DEVICE-HANDOFF-V1\0".to_vec();
        message.extend_from_slice(cell_id.as_str().as_bytes());
        message.push(0);
        message.extend_from_slice(device_id.as_bytes());
        message.push(0);
        message.extend_from_slice(&Sha256::digest(assertion.as_bytes()));
        message.push(0);
        message.extend_from_slice(nonce.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing.sign(&message).to_bytes())
    }

    fn phase3_request(
        method: HttpMethod,
        path: &str,
        body: HttpRequestBodyV2,
    ) -> ApplicationHttpRequest {
        ApplicationHttpRequest {
            method,
            path: path.to_string(),
            query: Vec::new(),
            headers: Vec::new(),
            body,
            peer_address: None,
        }
    }

    #[test]
    fn phase3_boundary_serves_only_signed_frontend_and_issues_device_bound_local_authority() {
        let directory = tempfile::tempdir().unwrap();
        let cell_id = CellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1240").unwrap();
        let identity_signing = SigningKey::from_bytes(&[6; 32]);
        let device_signing = SigningKey::from_bytes(&[8; 32]);
        let device_id = "device-a";
        let package_sha256 = "a".repeat(64);
        let session_key = [9_u8; 32];
        let authorization = CellAuthorizationPolicy {
            format: CELL_AUTHORIZATION_POLICY_FORMAT.to_string(),
            cell_id: cell_id.clone(),
            authorization_epoch: 7,
            session_lifetime_seconds: 300,
            minimum_assurance: "hardware-bound".to_string(),
            members: vec![CellMemberAuthorization {
                user_id: "member-a".to_string(),
                roles: BTreeSet::from(["cell-reader".to_string()]),
                scopes: BTreeSet::from(["records.read".to_string()]),
                devices: vec![CellAuthorizedDevice {
                    device_id: device_id.to_string(),
                    public_key: hex::encode(device_signing.verifying_key().as_bytes()),
                    enabled: true,
                }],
                enabled: true,
            }],
        };
        let replay_path = directory.path().join("handoff-replay.jsonl");
        let boundary = CellHttpBoundary {
            cell_id: cell_id.clone(),
            external_authenticator: phase3_external_authenticator(&identity_signing),
            authorization: authorization.clone(),
            session_key: Zeroizing::new(session_key.to_vec()),
            frontends: BTreeMap::from([(
                "generic-ledger".to_string(),
                PreparedFrontend {
                    package_sha256: package_sha256.clone(),
                    assets: BTreeMap::from([(
                        "index.html".to_string(),
                        FrontendAsset {
                            content_type: "text/html; charset=utf-8".to_string(),
                            bytes: b"<!doctype html><title>Cell</title>".to_vec(),
                        },
                    )]),
                },
            )]),
            replay: Mutex::new(
                HandoffReplayJournal::open(replay_path.clone(), &session_key).unwrap(),
            ),
        };

        let index = boundary
            .handle(&phase3_request(
                HttpMethod::Get,
                "/",
                HttpRequestBodyV2::Empty,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(index.status, 200);
        assert_eq!(
            index.body,
            HttpResponseBodyV2::Binary(b"<!doctype html><title>Cell</title>".to_vec())
        );
        assert!(index.headers.iter().any(|(name, value)| {
            name == "content-security-policy" && value.contains("default-src 'self'")
        }));
        let immutable = boundary
            .handle(&phase3_request(
                HttpMethod::Get,
                &format!("/_bicdb/apps/generic-ledger/{package_sha256}/index.html"),
                HttpRequestBodyV2::Empty,
            ))
            .unwrap()
            .unwrap();
        assert!(immutable
            .headers
            .iter()
            .any(|(name, value)| { name == "cache-control" && value.contains("immutable") }));
        assert!(boundary
            .handle(&phase3_request(
                HttpMethod::Get,
                "/ambient.js",
                HttpRequestBodyV2::Empty,
            ))
            .unwrap()
            .is_none());

        let nonce = "ab".repeat(32);
        let assertion_id = "one-time-assertion-a";
        let assertion =
            phase3_handoff_assertion(&identity_signing, &cell_id, device_id, &nonce, assertion_id);
        let handoff = DeviceHandoffRequest {
            proof: phase3_device_proof(&device_signing, &cell_id, device_id, &assertion, &nonce),
            assertion,
            device_id: device_id.to_string(),
            nonce,
        };
        let request = phase3_request(
            HttpMethod::Post,
            "/_bicdb/session/handoff",
            HttpRequestBodyV2::Json(serde_json::to_value(&handoff).unwrap()),
        );
        let response = boundary.handle(&request).unwrap().unwrap();
        assert_eq!(response.status, 204);
        let cookie = response
            .headers
            .iter()
            .find(|(name, _)| name == "set-cookie")
            .map(|(_, value)| value)
            .unwrap();
        assert!(cookie.contains("Secure; HttpOnly; SameSite=Strict"));
        let token = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        let actor = JwtAuthenticator::hs256(
            JwtConfiguration {
                issuer: format!("bicdb-cell://{cell_id}"),
                audience: format!("bicdb-cell:{cell_id}"),
                authentication_method: "cell-device-session".to_string(),
                maximum_lifetime_seconds: authorization.session_lifetime_seconds,
                clock_skew_seconds: 0,
            },
            session_key.to_vec(),
        )
        .unwrap()
        .authenticate(
            token,
            "trace-a".to_string(),
            None,
            None,
            (unix_seconds() + 30) * 1_000,
        )
        .unwrap();
        assert_eq!(actor.user_id.as_deref(), Some("member-a"));
        assert_eq!(actor.client_id.as_deref(), Some(device_id));
        assert_eq!(actor.roles, BTreeSet::from(["cell-reader".to_string()]));
        assert_eq!(actor.scopes, BTreeSet::from(["records.read".to_string()]));
        assert!(!actor.roles.contains("external-global-admin"));

        let replay_error = boundary.handle(&request).unwrap_err();
        assert!(replay_error.to_string().contains("already consumed"));
        drop(boundary);
        let mut reopened = HandoffReplayJournal::open(replay_path, &session_key).unwrap();
        assert!(reopened
            .consume(assertion_id, unix_seconds() + 60)
            .unwrap_err()
            .to_string()
            .contains("already consumed"));
    }

    #[test]
    fn phase3_package_contract_must_exactly_match_executable_authority() {
        let signing = SigningKey::from_bytes(&[5; 32]);
        let mut package = signed_generic_package(&signing);
        let application = package.manifest.application.as_deref_mut().unwrap();
        application.auth_schemes.clear();
        application.routes[0].auth_scheme = None;
        package.frontend_assets.insert(
            "index.html".to_string(),
            FrontendAsset {
                content_type: "text/html; charset=utf-8".to_string(),
                bytes: b"<!doctype html>".to_vec(),
            },
        );
        package.components = vec![
            bicdb_app_runtime::ApplicationComponent {
                name: "api".to_string(),
                kind: ApplicationComponentKind::Backend,
                scope: ExecutionScope::Cell,
                data_class: DataClass::Sensitive,
                capabilities: BTreeSet::from([ApplicationCapability::HttpRoutes]),
                egress: BTreeSet::new(),
                database_features: BTreeSet::new(),
            },
            bicdb_app_runtime::ApplicationComponent {
                name: "web".to_string(),
                kind: ApplicationComponentKind::Frontend,
                scope: ExecutionScope::Cell,
                data_class: DataClass::Sensitive,
                capabilities: BTreeSet::from([ApplicationCapability::FrontendAssets]),
                egress: BTreeSet::new(),
                database_features: BTreeSet::new(),
            },
        ];
        let pin = CellApplicationPin {
            root: "generic-suite".to_string(),
            name: "generic-ledger".to_string(),
            version: "1.0.0".to_string(),
            digest: digest("package"),
            schema_generation: 1,
            scope: ExecutionScope::Cell,
            data_class: DataClass::Sensitive,
        };
        let certification = CellFeatureCertification {
            format: CELL_FEATURE_CERTIFICATION_FORMAT.to_string(),
            bicdb_binary_digest: current_runtime_digest(),
            database_format: CURRENT_FORMAT_VERSION,
            conformance_suite_digest: digest("suite"),
            certified_features: BTreeSet::from([ApplicationDatabaseFeature::RowLevelSecurity]),
        };
        validate_phase3_package(&pin, &package, &certification, false).unwrap();

        let mut forged = package.clone();
        forged.components[0]
            .capabilities
            .insert(ApplicationCapability::Secrets);
        assert!(validate_phase3_package(&pin, &forged, &certification, false).is_err());

        let mut forged_feature = package.clone();
        forged_feature.components[0]
            .database_features
            .insert(ApplicationDatabaseFeature::RowLevelSecurity);
        assert!(validate_phase3_package(&pin, &forged_feature, &certification, false).is_err());

        let mut regulated = package;
        for component in &mut regulated.components {
            component.data_class = DataClass::Regulated;
        }
        let mut regulated_pin = pin;
        regulated_pin.data_class = DataClass::Regulated;
        assert!(
            validate_phase3_package(&regulated_pin, &regulated, &certification, false).is_err()
        );
        validate_phase3_package(&regulated_pin, &regulated, &certification, true).unwrap();
    }

    #[test]
    fn phase1_application_secrets_are_stable_and_cell_scoped() {
        let cell = CellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1240").unwrap();
        let other_cell = CellId::parse("018f7b30-4f4d-7b5c-a1f6-a183663e1241").unwrap();
        let key = CellKeyMaterial::from_bytes([42; 32]);

        let first = derive_phase1_application_secret(&cell, &key, "PATIENT_KEY").unwrap();
        let after_manifest_transition =
            derive_phase1_application_secret(&cell, &key, "PATIENT_KEY").unwrap();
        let other_secret = derive_phase1_application_secret(&cell, &key, "DOCUMENT_KEY").unwrap();
        let other_cell_secret =
            derive_phase1_application_secret(&other_cell, &key, "PATIENT_KEY").unwrap();

        assert_eq!(first, after_manifest_transition);
        assert_ne!(first, other_secret);
        assert_ne!(first, other_cell_secret);
    }
}
