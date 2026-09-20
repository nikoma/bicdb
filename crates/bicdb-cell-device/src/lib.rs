//! Cell-subordinate, filtered device replicas for bounded offline work.
//!
//! This crate is deliberately industry neutral. A device receives exact
//! objects selected by a Cell-local authorization decision, never the parent
//! Cell key or an unrestricted database. Offline writes are immutable signed
//! amendment proposals. The parent classifies them against causal base
//! digests and never resolves a conflict by wall-clock last-writer-wins.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bicdb_core::{BicDb, BicDbError, DbConfig, EncryptionBinding, EncryptionConfig, Record};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use curve25519_dalek::montgomery::MontgomeryPoint;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hkdf::Hkdf;
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

pub const DEVICE_TRUST_POLICY_FORMAT: &str = "bicdb.cell-device-trust-policy/v1";
pub const DEVICE_ENROLLMENT_FORMAT: &str = "bicdb.cell-device-enrollment/v1";
pub const DEVICE_AUTHORIZATION_FORMAT: &str = "bicdb.cell-device-authorization/v1";
pub const DEVICE_DATABASE_KEY_FORMAT: &str = "bicdb.cell-device-database-key/v1";
pub const DEVICE_WORKING_SET_FORMAT: &str = "bicdb.cell-device-working-set/v1";
pub const DEVICE_AMENDMENT_FORMAT: &str = "bicdb.cell-device-amendment/v1";
pub const DEVICE_RESOLUTION_FORMAT: &str = "bicdb.cell-device-resolution/v1";
pub const DEVICE_RETIREMENT_FORMAT: &str = "bicdb.cell-device-retirement/v1";
pub const DEVICE_LOCAL_STATE_FORMAT: &str = "bicdb.cell-device-local-state/v1";
pub const DEVICE_PARENT_STATE_FORMAT: &str = "bicdb.cell-device-parent-state/v1";
pub const DEVICE_STORAGE_PROFILE: &str = "bicdb-device-replica-bound-v1";

const ENROLLMENT_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-ENROLLMENT-V1\0";
const AUTHORIZATION_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-AUTHORIZATION-V1\0";
const DATABASE_KEY_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-DATABASE-KEY-V1\0";
const WORKING_SET_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-WORKING-SET-V1\0";
const AMENDMENT_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-AMENDMENT-V1\0";
const RESOLUTION_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-RESOLUTION-V1\0";
const RETIREMENT_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-RETIREMENT-V1\0";
const USER_PRESENCE_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-USER-PRESENCE-V1\0";
const SEALED_PAYLOAD_DOMAIN: &[u8] = b"BICDB-CELL-DEVICE-X25519-XCHACHA20-V1\0";
const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_AUTHORITIES: usize = 127;
const MAX_EXACT_OBJECTS: usize = 100_000;
const MAX_AUTHORIZED_PACKAGE_HISTORY: usize = 4_096;
const MAX_OFFLINE_SECONDS: i64 = 30 * 24 * 60 * 60;
const MAX_UPLOAD_GRACE_SECONDS: i64 = 7 * 24 * 60 * 60;
const DEVICE_OBJECTS_COLLECTION: &str = "bicdb_device_objects";
const DEVICE_AMENDMENTS_COLLECTION: &str = "bicdb_device_amendments";
const DEVICE_STATE_COLLECTION: &str = "bicdb_device_state";
const DEVICE_STATE_RECORD_ID: &str = "state";
const DEVICE_PARENT_STATE_COLLECTION: &str = "bicdb_cell_device_parent_state";
const DEVICE_PARENT_STATE_RECORD_ID: &str = "state";

pub type Result<T> = std::result::Result<T, DeviceError>;

#[derive(Debug, thiserror::Error)]
pub enum DeviceError {
    #[error("CELL_DEVICE_DOCUMENT_INVALID: {0}")]
    Invalid(String),
    #[error("CELL_DEVICE_SIGNATURE_INVALID: {0}")]
    Signature(String),
    #[error("CELL_DEVICE_QUORUM_INSUFFICIENT: {0}")]
    Quorum(String),
    #[error("CELL_DEVICE_SCOPE_MISMATCH: {0}")]
    Scope(String),
    #[error("CELL_DEVICE_OFFLINE_EXPIRED: {0}")]
    OfflineExpired(String),
    #[error("CELL_DEVICE_RETIRED: {0}")]
    Retired(String),
    #[error("CELL_DEVICE_CONFLICT: {0}")]
    Conflict(String),
    #[error("CELL_DEVICE_LIMIT_EXCEEDED: {0}")]
    Limit(String),
    #[error("CELL_DEVICE_CRYPTOGRAPHY: {0}")]
    Cryptography(String),
    #[error("CELL_DEVICE_STORAGE: {0}")]
    Storage(String),
    #[error("CELL_DEVICE_IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("CELL_DEVICE_DATABASE: {0}")]
    Database(#[from] BicDbError),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct DeviceDigest(String);

impl DeviceDigest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let Some(hex_value) = value.strip_prefix("sha256:") else {
            return Err(DeviceError::Invalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        };
        if value != value.to_ascii_lowercase()
            || hex_value.len() != 64
            || !hex_value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(DeviceError::Invalid(
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

impl std::fmt::Display for DeviceDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DeviceDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct DeviceId(String);

impl DeviceId {
    pub fn parse(value: impl Into<String>, label: &str) -> Result<Self> {
        let value = value.into();
        let parsed = Uuid::parse_str(&value)
            .map_err(|_| DeviceError::Invalid(format!("{label} must be a UUID")))?;
        if parsed.is_nil() {
            return Err(DeviceError::Invalid(format!("{label} must not be nil")));
        }
        Ok(Self(parsed.hyphenated().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DeviceId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?, "identifier")
            .map_err(serde::de::Error::custom)
    }
}

fn canonical_cbor<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|error| DeviceError::Invalid(format!("encode deterministic CBOR: {error}")))?;
    Ok(bytes)
}

pub fn encode_document<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_cbor(value)
}

pub fn decode_document<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(DeviceError::Invalid(
            "device document is empty or exceeds 16 MiB".to_string(),
        ));
    }
    let value: T = ciborium::de::from_reader(bytes)
        .map_err(|error| DeviceError::Invalid(format!("decode CBOR: {error}")))?;
    if canonical_cbor(&value)? != bytes {
        return Err(DeviceError::Invalid(
            "device document is not in BicDB deterministic CBOR encoding".to_string(),
        ));
    }
    Ok(value)
}

pub fn document_digest<T: Serialize>(value: &T) -> Result<DeviceDigest> {
    Ok(DeviceDigest::of_bytes(&canonical_cbor(value)?))
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn require_label(value: &str, label: &str) -> Result<()> {
    if valid_label(value) {
        Ok(())
    } else {
        Err(DeviceError::Invalid(format!(
            "{label} must be a 1..256 byte portable ASCII identifier"
        )))
    }
}

fn prepare_device_database_path(path: &Path) -> Result<()> {
    if !path.is_absolute() || path.parent().is_none() {
        return Err(DeviceError::Storage(
            "device database path must be absolute and have a parent".to_string(),
        ));
    }
    let parent = path.parent().expect("parent checked");
    for ancestor in parent.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(DeviceError::Storage(format!(
                    "device database ancestor {} is not a real directory",
                    ancestor.display()
                )))
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DeviceError::Storage(format!(
                    "device database parent {} must already exist",
                    parent.display()
                )))
            }
            Err(error) => return Err(error.into()),
        }
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            DeviceError::Storage("device database path is not a real directory".to_string()),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn create_device_database_directory(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).is_ok() {
        return prepare_device_database_path(path);
    }
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    File::open(path.parent().expect("absolute path parent checked"))?.sync_all()?;
    prepare_device_database_path(path)
}

fn verifying_key(value: &str, label: &str) -> Result<VerifyingKey> {
    if value != value.to_ascii_lowercase() {
        return Err(DeviceError::Invalid(format!(
            "{label} must be lowercase hexadecimal"
        )));
    }
    let bytes = hex::decode(value)
        .map_err(|_| DeviceError::Invalid(format!("{label} is not hexadecimal")))?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| DeviceError::Invalid(format!("{label} must contain 32 bytes")))?;
    VerifyingKey::from_bytes(&array)
        .map_err(|_| DeviceError::Invalid(format!("{label} is not an Ed25519 public key")))
}

fn x25519_public(value: &str, label: &str) -> Result<MontgomeryPoint> {
    if value != value.to_ascii_lowercase() {
        return Err(DeviceError::Invalid(format!(
            "{label} must be lowercase hexadecimal"
        )));
    }
    let bytes = hex::decode(value)
        .map_err(|_| DeviceError::Invalid(format!("{label} is not hexadecimal")))?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| DeviceError::Invalid(format!("{label} must contain 32 bytes")))?;
    if array == [0; 32] {
        return Err(DeviceError::Invalid(format!("{label} must not be zero")));
    }
    Ok(MontgomeryPoint(array))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAuthorityRole {
    Enrollment,
    Authorization,
    Resolution,
    Retirement,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceAuthorityKey {
    pub key_id: String,
    pub role: DeviceAuthorityRole,
    pub public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceExportKey {
    pub key_id: String,
    pub public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceTrustPolicy {
    pub format: String,
    pub policy_id: String,
    pub authorities: Vec<DeviceAuthorityKey>,
    pub enrollment_threshold: u16,
    pub authorization_threshold: u16,
    pub resolution_threshold: u16,
    pub retirement_threshold: u16,
    pub export_keys: Vec<DeviceExportKey>,
    pub allowed_hardware_profiles: BTreeSet<String>,
    pub maximum_offline_seconds: i64,
    pub maximum_local_reauthentication_seconds: i64,
    pub maximum_upload_grace_seconds: i64,
    pub maximum_working_set_objects: u32,
    pub maximum_working_set_bytes: u64,
    pub maximum_pending_amendments: u32,
    pub maximum_pending_amendment_bytes: u64,
}

impl DeviceTrustPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.format != DEVICE_TRUST_POLICY_FORMAT {
            return Err(DeviceError::Invalid(format!(
                "unsupported device trust policy format {}",
                self.format
            )));
        }
        require_label(&self.policy_id, "policy_id")?;
        if self.authorities.len() < 8 || self.authorities.len() > MAX_AUTHORITIES {
            return Err(DeviceError::Invalid(
                "device trust policy requires 8..127 authority keys".to_string(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut public_keys = BTreeSet::new();
        let mut role_counts = BTreeMap::<DeviceAuthorityRole, usize>::new();
        for authority in &self.authorities {
            require_label(&authority.key_id, "authority key_id")?;
            let key = verifying_key(&authority.public_key, "authority public_key")?;
            if !ids.insert(authority.key_id.clone()) || !public_keys.insert(key.to_bytes()) {
                return Err(DeviceError::Invalid(
                    "authority key ids and public keys must be globally unique".to_string(),
                ));
            }
            *role_counts.entry(authority.role).or_default() += 1;
        }
        let thresholds = [
            (DeviceAuthorityRole::Enrollment, self.enrollment_threshold),
            (
                DeviceAuthorityRole::Authorization,
                self.authorization_threshold,
            ),
            (DeviceAuthorityRole::Resolution, self.resolution_threshold),
            (DeviceAuthorityRole::Retirement, self.retirement_threshold),
        ];
        for (role, threshold) in thresholds {
            let available = role_counts.get(&role).copied().unwrap_or_default();
            if threshold < 2 || usize::from(threshold) > available {
                return Err(DeviceError::Invalid(format!(
                    "{role:?} requires a threshold of at least two within its own disjoint keys"
                )));
            }
        }
        if self.export_keys.is_empty() || self.export_keys.len() > 32 {
            return Err(DeviceError::Invalid(
                "device policy requires 1..32 parent export keys".to_string(),
            ));
        }
        for export in &self.export_keys {
            require_label(&export.key_id, "export key_id")?;
            let key = verifying_key(&export.public_key, "export public_key")?;
            if !ids.insert(export.key_id.clone()) || !public_keys.insert(key.to_bytes()) {
                return Err(DeviceError::Invalid(
                    "export keys cannot reuse authority ids or key material".to_string(),
                ));
            }
        }
        if self.allowed_hardware_profiles.is_empty()
            || self
                .allowed_hardware_profiles
                .iter()
                .any(|profile| !valid_label(profile))
        {
            return Err(DeviceError::Invalid(
                "allowed hardware profiles must contain portable identifiers".to_string(),
            ));
        }
        if !(1..=MAX_OFFLINE_SECONDS).contains(&self.maximum_offline_seconds)
            || !(1..=self.maximum_offline_seconds)
                .contains(&self.maximum_local_reauthentication_seconds)
            || !(0..=MAX_UPLOAD_GRACE_SECONDS).contains(&self.maximum_upload_grace_seconds)
            || self.maximum_working_set_objects == 0
            || usize::try_from(self.maximum_working_set_objects).unwrap_or(usize::MAX)
                > MAX_EXACT_OBJECTS
            || self.maximum_working_set_bytes == 0
            || self.maximum_pending_amendments == 0
            || self.maximum_pending_amendment_bytes == 0
        {
            return Err(DeviceError::Invalid(
                "device policy limits are zero, unbounded, or internally inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn threshold(&self, role: DeviceAuthorityRole) -> u16 {
        match role {
            DeviceAuthorityRole::Enrollment => self.enrollment_threshold,
            DeviceAuthorityRole::Authorization => self.authorization_threshold,
            DeviceAuthorityRole::Resolution => self.resolution_threshold,
            DeviceAuthorityRole::Retirement => self.retirement_threshold,
        }
    }

    fn authority(&self, key_id: &str, role: DeviceAuthorityRole) -> Result<VerifyingKey> {
        let authority = self
            .authorities
            .iter()
            .find(|candidate| candidate.key_id == key_id && candidate.role == role)
            .ok_or_else(|| {
                DeviceError::Signature(format!("key {key_id} is not authorized for {role:?}"))
            })?;
        verifying_key(&authority.public_key, "authority public_key")
    }

    fn export_key(&self, key_id: &str) -> Result<VerifyingKey> {
        let key = self
            .export_keys
            .iter()
            .find(|candidate| candidate.key_id == key_id)
            .ok_or_else(|| DeviceError::Signature("unknown parent export key".to_string()))?;
        verifying_key(&key.public_key, "export public_key")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceAuthorityApproval {
    pub key_id: String,
    pub signature: Vec<u8>,
}

fn signing_message<T: Serialize>(domain: &[u8], statement: &T) -> Result<Vec<u8>> {
    let payload = canonical_cbor(statement)?;
    let mut message = Vec::with_capacity(domain.len() + payload.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(&payload);
    Ok(message)
}

pub fn sign_authority_approval<T: Serialize>(
    domain: &[u8],
    key_id: impl Into<String>,
    statement: &T,
    signing_key: &SigningKey,
) -> Result<DeviceAuthorityApproval> {
    Ok(DeviceAuthorityApproval {
        key_id: key_id.into(),
        signature: signing_key
            .sign(&signing_message(domain, statement)?)
            .to_bytes()
            .to_vec(),
    })
}

pub fn sign_enrollment_approval(
    key_id: impl Into<String>,
    statement: &DeviceEnrollmentStatement,
    signing_key: &SigningKey,
) -> Result<DeviceAuthorityApproval> {
    sign_authority_approval(ENROLLMENT_DOMAIN, key_id, statement, signing_key)
}

pub fn sign_authorization_approval(
    key_id: impl Into<String>,
    statement: &DeviceAuthorizationStatement,
    signing_key: &SigningKey,
) -> Result<DeviceAuthorityApproval> {
    sign_authority_approval(AUTHORIZATION_DOMAIN, key_id, statement, signing_key)
}

pub fn sign_resolution_approval(
    key_id: impl Into<String>,
    statement: &AmendmentResolutionStatement,
    signing_key: &SigningKey,
) -> Result<DeviceAuthorityApproval> {
    sign_authority_approval(RESOLUTION_DOMAIN, key_id, statement, signing_key)
}

pub fn sign_retirement_approval(
    key_id: impl Into<String>,
    statement: &DeviceRetirementStatement,
    signing_key: &SigningKey,
) -> Result<DeviceAuthorityApproval> {
    sign_authority_approval(RETIREMENT_DOMAIN, key_id, statement, signing_key)
}

fn verify_approvals<T: Serialize>(
    policy: &DeviceTrustPolicy,
    role: DeviceAuthorityRole,
    domain: &[u8],
    statement: &T,
    approvals: &[DeviceAuthorityApproval],
) -> Result<()> {
    policy.validate()?;
    let message = signing_message(domain, statement)?;
    let mut accepted = BTreeSet::new();
    for approval in approvals {
        if !accepted.insert(approval.key_id.clone()) {
            return Err(DeviceError::Signature(
                "duplicate authority approval".to_string(),
            ));
        }
        let key = policy.authority(&approval.key_id, role)?;
        let signature = Signature::from_slice(&approval.signature)
            .map_err(|_| DeviceError::Signature("malformed authority signature".to_string()))?;
        key.verify_strict(&message, &signature).map_err(|_| {
            DeviceError::Signature("authority signature verification failed".to_string())
        })?;
    }
    if accepted.len() < usize::from(policy.threshold(role)) {
        return Err(DeviceError::Quorum(format!(
            "{role:?} has {} approvals, needs {}",
            accepted.len(),
            policy.threshold(role)
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HardwareKeyDescriptor {
    pub key_id: String,
    pub hardware_profile: String,
    pub platform_attestation_digest: DeviceDigest,
    pub encryption_public_key: String,
    pub signing_public_key: String,
    pub signing_requires_user_presence: bool,
    pub rollback_resistant_clock: bool,
}

impl HardwareKeyDescriptor {
    fn validate(&self, policy: &DeviceTrustPolicy) -> Result<()> {
        require_label(&self.key_id, "hardware key_id")?;
        if !policy
            .allowed_hardware_profiles
            .contains(&self.hardware_profile)
            || !self.signing_requires_user_presence
            || !self.rollback_resistant_clock
        {
            return Err(DeviceError::Invalid(
                "device key lacks an allowed hardware profile, user-presence signing, or rollback-resistant clock"
                    .to_string(),
            ));
        }
        x25519_public(&self.encryption_public_key, "device encryption_public_key")?;
        verifying_key(&self.signing_public_key, "device signing_public_key")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceEnrollmentStatement {
    pub format: String,
    pub enrollment_id: DeviceId,
    pub trust_policy_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub principal_id: String,
    pub application_digest: DeviceDigest,
    pub hardware_key: HardwareKeyDescriptor,
    pub enrolled_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedDeviceEnrollment {
    pub statement: DeviceEnrollmentStatement,
    pub approvals: Vec<DeviceAuthorityApproval>,
}

pub fn verify_enrollment(
    policy: &DeviceTrustPolicy,
    enrollment: &CertifiedDeviceEnrollment,
) -> Result<DeviceDigest> {
    let statement = &enrollment.statement;
    if statement.format != DEVICE_ENROLLMENT_FORMAT
        || statement.trust_policy_digest != document_digest(policy)?
        || statement.enrolled_at <= 0
        || statement.principal_id.is_empty()
    {
        return Err(DeviceError::Invalid(
            "device enrollment identity or time is invalid".to_string(),
        ));
    }
    require_label(&statement.principal_id, "principal_id")?;
    statement.hardware_key.validate(policy)?;
    verify_approvals(
        policy,
        DeviceAuthorityRole::Enrollment,
        ENROLLMENT_DOMAIN,
        statement,
        &enrollment.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct DeviceObjectRef {
    pub namespace: String,
    pub object_id: String,
    pub data_category: String,
}

impl DeviceObjectRef {
    fn validate(&self) -> Result<()> {
        require_label(&self.namespace, "object namespace")?;
        require_label(&self.data_category, "object data_category")?;
        if self.object_id.is_empty() || self.object_id.len() > 1024 || self.object_id.contains('\0')
        {
            return Err(DeviceError::Invalid(
                "object_id must be 1..1024 bytes and contain no NUL".to_string(),
            ));
        }
        Ok(())
    }

    fn storage_id(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.namespace.as_bytes());
        hasher.update([0]);
        hasher.update(self.object_id.as_bytes());
        format!("object-{}", hex::encode(hasher.finalize()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceOfflinePolicy {
    pub maximum_offline_seconds: i64,
    pub local_reauthentication_seconds: i64,
    pub upload_grace_seconds: i64,
    pub maximum_pending_amendments: u32,
    pub maximum_pending_amendment_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceAuthorizationStatement {
    pub format: String,
    pub authorization_id: DeviceId,
    pub enrollment_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub principal_id: String,
    pub authorization_epoch: u64,
    pub previous_authorization_digest: Option<DeviceDigest>,
    pub application_digest: DeviceDigest,
    pub schema_generation: u64,
    pub exact_working_set: BTreeSet<DeviceObjectRef>,
    pub maximum_working_set_bytes: u64,
    pub offline: DeviceOfflinePolicy,
    pub issued_at: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedDeviceAuthorization {
    pub statement: DeviceAuthorizationStatement,
    pub approvals: Vec<DeviceAuthorityApproval>,
}

pub fn verify_authorization(
    policy: &DeviceTrustPolicy,
    enrollment: &CertifiedDeviceEnrollment,
    authorization: &CertifiedDeviceAuthorization,
    now: i64,
) -> Result<DeviceDigest> {
    let enrollment_digest = verify_enrollment(policy, enrollment)?;
    let statement = &authorization.statement;
    if statement.format != DEVICE_AUTHORIZATION_FORMAT
        || statement.enrollment_digest != enrollment_digest
        || statement.cell_id != enrollment.statement.cell_id
        || statement.device_replica_id != enrollment.statement.device_replica_id
        || statement.principal_id != enrollment.statement.principal_id
        || statement.application_digest != enrollment.statement.application_digest
        || statement.authorization_epoch == 0
        || statement.schema_generation == 0
        || statement.issued_at <= 0
        || statement.expires_at <= statement.issued_at
        || now < statement.issued_at
        || now > statement.expires_at
    {
        return Err(DeviceError::Scope(
            "device authorization does not match the enrollment or active time".to_string(),
        ));
    }
    if statement.authorization_epoch == 1 && statement.previous_authorization_digest.is_some()
        || statement.authorization_epoch > 1 && statement.previous_authorization_digest.is_none()
    {
        return Err(DeviceError::Invalid(
            "authorization epoch predecessor shape is invalid".to_string(),
        ));
    }
    if statement.exact_working_set.is_empty()
        || statement.exact_working_set.len()
            > usize::try_from(policy.maximum_working_set_objects).unwrap_or(usize::MAX)
        || statement.maximum_working_set_bytes == 0
        || statement.maximum_working_set_bytes > policy.maximum_working_set_bytes
    {
        return Err(DeviceError::Limit(
            "working-set object or byte bounds exceed policy".to_string(),
        ));
    }
    for object in &statement.exact_working_set {
        object.validate()?;
    }
    if statement.offline.maximum_offline_seconds <= 0
        || statement.offline.maximum_offline_seconds > policy.maximum_offline_seconds
        || statement.offline.local_reauthentication_seconds <= 0
        || statement.offline.local_reauthentication_seconds
            > policy.maximum_local_reauthentication_seconds
        || statement.offline.local_reauthentication_seconds
            > statement.offline.maximum_offline_seconds
        || statement.offline.upload_grace_seconds < 0
        || statement.offline.upload_grace_seconds > policy.maximum_upload_grace_seconds
        || statement.offline.maximum_pending_amendments == 0
        || statement.offline.maximum_pending_amendments > policy.maximum_pending_amendments
        || statement.offline.maximum_pending_amendment_bytes == 0
        || statement.offline.maximum_pending_amendment_bytes
            > policy.maximum_pending_amendment_bytes
    {
        return Err(DeviceError::Limit(
            "offline or amendment bounds exceed policy".to_string(),
        ));
    }
    verify_approvals(
        policy,
        DeviceAuthorityRole::Authorization,
        AUTHORIZATION_DOMAIN,
        statement,
        &authorization.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SealedDevicePayload {
    pub algorithm: String,
    pub ephemeral_public_key: String,
    pub nonce: Vec<u8>,
    pub aad_digest: DeviceDigest,
    pub ciphertext: Vec<u8>,
}

fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0_u8; N];
    getrandom::fill(&mut bytes)
        .map_err(|error| DeviceError::Cryptography(format!("random source failed: {error}")))?;
    Ok(bytes)
}

fn derive_sealed_key(shared: &[u8; 32], aad: &[u8]) -> Result<[u8; 32]> {
    let aad_digest = Sha256::digest(aad);
    let hkdf = Hkdf::<Sha256>::new(Some(SEALED_PAYLOAD_DOMAIN), shared);
    let mut key = [0_u8; 32];
    hkdf.expand(&aad_digest, &mut key)
        .map_err(|_| DeviceError::Cryptography("sealed-payload HKDF failed".to_string()))?;
    Ok(key)
}

pub fn seal_for_device(
    encryption_public_key: &str,
    aad: &[u8],
    plaintext: &[u8],
) -> Result<SealedDevicePayload> {
    let recipient = x25519_public(encryption_public_key, "device encryption_public_key")?;
    let mut ephemeral_secret = random_bytes::<32>()?;
    let ephemeral_public = MontgomeryPoint::mul_base_clamped(ephemeral_secret);
    let mut shared = recipient.mul_clamped(ephemeral_secret).to_bytes();
    ephemeral_secret.zeroize();
    if shared == [0; 32] {
        shared.zeroize();
        return Err(DeviceError::Cryptography(
            "X25519 produced the all-zero shared secret".to_string(),
        ));
    }
    let mut key = derive_sealed_key(&shared, aad)?;
    shared.zeroize();
    let nonce = random_bytes::<24>()?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| DeviceError::Cryptography("seal for device failed".to_string()))?;
    key.zeroize();
    Ok(SealedDevicePayload {
        algorithm: "x25519-hkdf-sha256-xchacha20poly1305".to_string(),
        ephemeral_public_key: hex::encode(ephemeral_public.as_bytes()),
        nonce: nonce.to_vec(),
        aad_digest: DeviceDigest::of_bytes(aad),
        ciphertext,
    })
}

/// Software reference for a platform keystore implementation. Production
/// device code should perform this scalar operation inside TPM/Secure
/// Enclave/StrongBox APIs and implement [`HardwareBoundDeviceKey`] without
/// exporting the private scalar.
pub fn open_sealed_with_private_key(
    mut private_key: [u8; 32],
    sealed: &SealedDevicePayload,
    aad: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    if sealed.algorithm != "x25519-hkdf-sha256-xchacha20poly1305"
        || sealed.nonce.len() != 24
        || sealed.aad_digest != DeviceDigest::of_bytes(aad)
    {
        private_key.zeroize();
        return Err(DeviceError::Cryptography(
            "sealed payload algorithm, nonce, or AAD is invalid".to_string(),
        ));
    }
    let ephemeral = match x25519_public(&sealed.ephemeral_public_key, "ephemeral public key") {
        Ok(ephemeral) => ephemeral,
        Err(error) => {
            private_key.zeroize();
            return Err(error);
        }
    };
    let mut shared = ephemeral.mul_clamped(private_key).to_bytes();
    private_key.zeroize();
    if shared == [0; 32] {
        shared.zeroize();
        return Err(DeviceError::Cryptography(
            "X25519 produced the all-zero shared secret".to_string(),
        ));
    }
    let mut key = derive_sealed_key(&shared, aad)?;
    shared.zeroize();
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher
        .decrypt(
            XNonce::from_slice(&sealed.nonce),
            chacha20poly1305::aead::Payload {
                msg: &sealed.ciphertext,
                aad,
            },
        )
        .map_err(|_| DeviceError::Cryptography("open sealed device payload failed".to_string()))?;
    key.zeroize();
    Ok(Zeroizing::new(plaintext))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecureClockReading {
    pub unix_timestamp: i64,
    pub monotonic_counter: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserPresenceStatement {
    pub device_replica_id: DeviceId,
    pub authorization_epoch: u64,
    pub session_nonce: DeviceId,
    pub clock: SecureClockReading,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HardwareUserPresenceEvidence {
    pub statement: UserPresenceStatement,
    pub signature: Vec<u8>,
}

pub trait HardwareBoundDeviceKey: std::fmt::Debug + Send {
    fn descriptor(&self) -> HardwareKeyDescriptor;
    fn secure_clock(&mut self) -> Result<SecureClockReading>;
    fn open_sealed(
        &mut self,
        sealed: &SealedDevicePayload,
        aad: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>>;
    fn sign(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    /// Must require local biometric/PIN/user-presence policy before returning.
    fn sign_with_user_presence(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    /// Best-effort cooperative key destruction. It cannot prove deletion of
    /// plaintext previously copied by malware or a user.
    fn destroy(&mut self) -> Result<()>;
}

fn verify_hardware_descriptor(
    enrollment: &CertifiedDeviceEnrollment,
    hardware: &dyn HardwareBoundDeviceKey,
) -> Result<()> {
    if hardware.descriptor() != enrollment.statement.hardware_key {
        return Err(DeviceError::Scope(
            "hardware key does not match the certified enrollment".to_string(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceDatabaseKeyStatement {
    pub format: String,
    pub provisioning_id: DeviceId,
    pub enrollment_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub application_digest: DeviceDigest,
    pub key_epoch: u64,
    pub key_fingerprint: DeviceDigest,
    pub sealed_key_digest: DeviceDigest,
    pub issued_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProvisionedDeviceDatabaseKey {
    pub statement: DeviceDatabaseKeyStatement,
    pub sealed_key: SealedDevicePayload,
    pub exporter_key_id: String,
    pub signature: Vec<u8>,
}

#[derive(Serialize)]
struct DeviceDatabaseKeyAad<'a> {
    format: &'static str,
    provisioning_id: &'a DeviceId,
    enrollment_digest: &'a DeviceDigest,
    cell_id: &'a DeviceId,
    device_replica_id: &'a DeviceId,
    application_digest: &'a DeviceDigest,
    key_epoch: u64,
    issued_at: i64,
}

fn database_key_aad(statement: &DeviceDatabaseKeyStatement) -> Result<Vec<u8>> {
    canonical_cbor(&DeviceDatabaseKeyAad {
        format: DEVICE_DATABASE_KEY_FORMAT,
        provisioning_id: &statement.provisioning_id,
        enrollment_digest: &statement.enrollment_digest,
        cell_id: &statement.cell_id,
        device_replica_id: &statement.device_replica_id,
        application_digest: &statement.application_digest,
        key_epoch: statement.key_epoch,
        issued_at: statement.issued_at,
    })
}

fn verify_export_signature<T: Serialize>(
    policy: &DeviceTrustPolicy,
    domain: &[u8],
    key_id: &str,
    statement: &T,
    signature: &[u8],
) -> Result<()> {
    let key = policy.export_key(key_id)?;
    let signature = Signature::from_slice(signature)
        .map_err(|_| DeviceError::Signature("malformed parent export signature".to_string()))?;
    key.verify_strict(&signing_message(domain, statement)?, &signature)
        .map_err(|_| {
            DeviceError::Signature("parent export signature verification failed".to_string())
        })
}

pub fn verify_database_key_provisioning(
    policy: &DeviceTrustPolicy,
    enrollment: &CertifiedDeviceEnrollment,
    provisioning: &ProvisionedDeviceDatabaseKey,
) -> Result<()> {
    let enrollment_digest = verify_enrollment(policy, enrollment)?;
    let statement = &provisioning.statement;
    if statement.format != DEVICE_DATABASE_KEY_FORMAT
        || statement.enrollment_digest != enrollment_digest
        || statement.cell_id != enrollment.statement.cell_id
        || statement.device_replica_id != enrollment.statement.device_replica_id
        || statement.application_digest != enrollment.statement.application_digest
        || statement.key_epoch == 0
        || statement.issued_at < enrollment.statement.enrolled_at
        || statement.sealed_key_digest != document_digest(&provisioning.sealed_key)?
    {
        return Err(DeviceError::Scope(
            "device database key provisioning is outside its certified enrollment".to_string(),
        ));
    }
    verify_export_signature(
        policy,
        DATABASE_KEY_DOMAIN,
        &provisioning.exporter_key_id,
        statement,
        &provisioning.signature,
    )
}

#[derive(Clone, Debug)]
pub struct DeviceExporter {
    policy: Arc<DeviceTrustPolicy>,
    key_id: String,
    signing_key: SigningKey,
}

impl DeviceExporter {
    pub fn new(
        policy: Arc<DeviceTrustPolicy>,
        key_id: impl Into<String>,
        signing_key: SigningKey,
    ) -> Result<Self> {
        policy.validate()?;
        let key_id = key_id.into();
        if policy.export_key(&key_id)?.to_bytes() != signing_key.verifying_key().to_bytes() {
            return Err(DeviceError::Signature(
                "local parent export key does not match the trust policy".to_string(),
            ));
        }
        Ok(Self {
            policy,
            key_id,
            signing_key,
        })
    }

    pub fn provision_database_key(
        &self,
        enrollment: &CertifiedDeviceEnrollment,
        provisioning_id: DeviceId,
        key_epoch: u64,
        issued_at: i64,
    ) -> Result<ProvisionedDeviceDatabaseKey> {
        let enrollment_digest = verify_enrollment(&self.policy, enrollment)?;
        if key_epoch == 0 || issued_at < enrollment.statement.enrolled_at {
            return Err(DeviceError::Invalid(
                "device key epoch or issue time is invalid".to_string(),
            ));
        }
        let mut database_key = random_bytes::<32>()?;
        let mut statement = DeviceDatabaseKeyStatement {
            format: DEVICE_DATABASE_KEY_FORMAT.to_string(),
            provisioning_id,
            enrollment_digest,
            cell_id: enrollment.statement.cell_id.clone(),
            device_replica_id: enrollment.statement.device_replica_id.clone(),
            application_digest: enrollment.statement.application_digest.clone(),
            key_epoch,
            key_fingerprint: DeviceDigest::of_bytes(&database_key),
            sealed_key_digest: DeviceDigest::of_bytes(b"pending"),
            issued_at,
        };
        let aad = database_key_aad(&statement)?;
        let sealed_key = seal_for_device(
            &enrollment.statement.hardware_key.encryption_public_key,
            &aad,
            &database_key,
        )?;
        database_key.zeroize();
        statement.sealed_key_digest = document_digest(&sealed_key)?;
        let signature = self
            .signing_key
            .sign(&signing_message(DATABASE_KEY_DOMAIN, &statement)?)
            .to_bytes()
            .to_vec();
        Ok(ProvisionedDeviceDatabaseKey {
            statement,
            sealed_key,
            exporter_key_id: self.key_id.clone(),
            signature,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex as StdMutex;

    #[derive(Debug)]
    struct TestHardwareKey {
        descriptor: HardwareKeyDescriptor,
        encryption_private_key: [u8; 32],
        signing_key: SigningKey,
        clock: Arc<StdMutex<SecureClockReading>>,
        destroyed: Arc<AtomicBool>,
    }

    impl HardwareBoundDeviceKey for TestHardwareKey {
        fn descriptor(&self) -> HardwareKeyDescriptor {
            self.descriptor.clone()
        }

        fn secure_clock(&mut self) -> Result<SecureClockReading> {
            if self.destroyed.load(Ordering::SeqCst) {
                return Err(DeviceError::Retired(
                    "hardware key was destroyed".to_string(),
                ));
            }
            Ok(self.clock.lock().unwrap().clone())
        }

        fn open_sealed(
            &mut self,
            sealed: &SealedDevicePayload,
            aad: &[u8],
        ) -> Result<Zeroizing<Vec<u8>>> {
            if self.destroyed.load(Ordering::SeqCst) {
                return Err(DeviceError::Retired(
                    "hardware key was destroyed".to_string(),
                ));
            }
            open_sealed_with_private_key(self.encryption_private_key, sealed, aad)
        }

        fn sign(&mut self, message: &[u8]) -> Result<Vec<u8>> {
            if self.destroyed.load(Ordering::SeqCst) {
                return Err(DeviceError::Retired(
                    "hardware key was destroyed".to_string(),
                ));
            }
            Ok(self.signing_key.sign(message).to_bytes().to_vec())
        }

        fn sign_with_user_presence(&mut self, message: &[u8]) -> Result<Vec<u8>> {
            self.sign(message)
        }

        fn destroy(&mut self) -> Result<()> {
            self.encryption_private_key.zeroize();
            self.destroyed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    struct Fixture {
        policy: Arc<DeviceTrustPolicy>,
        authority_keys: BTreeMap<DeviceAuthorityRole, Vec<(String, SigningKey)>>,
        exporter: DeviceExporter,
        enrollment: CertifiedDeviceEnrollment,
        authorization: CertifiedDeviceAuthorization,
        encryption_private_key: [u8; 32],
        hardware_signing_key: SigningKey,
        descriptor: HardwareKeyDescriptor,
        clock: Arc<StdMutex<SecureClockReading>>,
        destroyed: Arc<AtomicBool>,
        existing_ref: DeviceObjectRef,
        absent_ref: DeviceObjectRef,
    }

    fn id(value: u128) -> DeviceId {
        DeviceId::parse(Uuid::from_u128(value).hyphenated().to_string(), "test id").unwrap()
    }

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn approve<T: Serialize>(
        domain: &[u8],
        keys: &[(String, SigningKey)],
        statement: &T,
    ) -> Vec<DeviceAuthorityApproval> {
        keys.iter()
            .take(2)
            .map(|(key_id, key)| {
                sign_authority_approval(domain, key_id.clone(), statement, key).unwrap()
            })
            .collect()
    }

    fn fixture() -> Fixture {
        let roles = [
            DeviceAuthorityRole::Enrollment,
            DeviceAuthorityRole::Authorization,
            DeviceAuthorityRole::Resolution,
            DeviceAuthorityRole::Retirement,
        ];
        let mut authority_keys = BTreeMap::new();
        let mut authorities = Vec::new();
        for (role_index, role) in roles.into_iter().enumerate() {
            let keys = (0..2)
                .map(|key_index| {
                    let seed = 1 + (role_index * 2 + key_index) as u8;
                    let key_id = format!("{role:?}-{key_index}").to_ascii_lowercase();
                    let signing_key = key(seed);
                    authorities.push(DeviceAuthorityKey {
                        key_id: key_id.clone(),
                        role,
                        public_key: hex::encode(signing_key.verifying_key().as_bytes()),
                    });
                    (key_id, signing_key)
                })
                .collect::<Vec<_>>();
            authority_keys.insert(role, keys);
        }
        let export_signing_key = key(20);
        let policy = Arc::new(DeviceTrustPolicy {
            format: DEVICE_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "test-device-policy".to_string(),
            authorities,
            enrollment_threshold: 2,
            authorization_threshold: 2,
            resolution_threshold: 2,
            retirement_threshold: 2,
            export_keys: vec![DeviceExportKey {
                key_id: "export-a".to_string(),
                public_key: hex::encode(export_signing_key.verifying_key().as_bytes()),
            }],
            allowed_hardware_profiles: BTreeSet::from(["test-secure-hardware".to_string()]),
            maximum_offline_seconds: 3_600,
            maximum_local_reauthentication_seconds: 300,
            maximum_upload_grace_seconds: 600,
            maximum_working_set_objects: 10,
            maximum_working_set_bytes: 1_000_000,
            maximum_pending_amendments: 10,
            maximum_pending_amendment_bytes: 1_000_000,
        });
        policy.validate().unwrap();
        let exporter =
            DeviceExporter::new(Arc::clone(&policy), "export-a", export_signing_key).unwrap();
        let encryption_private_key = [90; 32];
        let encryption_public_key =
            MontgomeryPoint::mul_base_clamped(encryption_private_key).to_bytes();
        let hardware_signing_key = key(21);
        let descriptor = HardwareKeyDescriptor {
            key_id: "hardware-a".to_string(),
            hardware_profile: "test-secure-hardware".to_string(),
            platform_attestation_digest: DeviceDigest::of_bytes(b"platform-attestation"),
            encryption_public_key: hex::encode(encryption_public_key),
            signing_public_key: hex::encode(hardware_signing_key.verifying_key().as_bytes()),
            signing_requires_user_presence: true,
            rollback_resistant_clock: true,
        };
        let enrollment_statement = DeviceEnrollmentStatement {
            format: DEVICE_ENROLLMENT_FORMAT.to_string(),
            enrollment_id: id(1),
            trust_policy_digest: document_digest(policy.as_ref()).unwrap(),
            cell_id: id(2),
            device_replica_id: id(3),
            principal_id: "principal-a".to_string(),
            application_digest: DeviceDigest::of_bytes(b"generic-application"),
            hardware_key: descriptor.clone(),
            enrolled_at: 1_800_000_000,
        };
        let enrollment = CertifiedDeviceEnrollment {
            approvals: approve(
                ENROLLMENT_DOMAIN,
                &authority_keys[&DeviceAuthorityRole::Enrollment],
                &enrollment_statement,
            ),
            statement: enrollment_statement,
        };
        let existing_ref = DeviceObjectRef {
            namespace: "documents".to_string(),
            object_id: "object-a".to_string(),
            data_category: "confidential".to_string(),
        };
        let absent_ref = DeviceObjectRef {
            namespace: "documents".to_string(),
            object_id: "object-b".to_string(),
            data_category: "confidential".to_string(),
        };
        let authorization_statement = DeviceAuthorizationStatement {
            format: DEVICE_AUTHORIZATION_FORMAT.to_string(),
            authorization_id: id(4),
            enrollment_digest: document_digest(&enrollment.statement).unwrap(),
            cell_id: enrollment.statement.cell_id.clone(),
            device_replica_id: enrollment.statement.device_replica_id.clone(),
            principal_id: enrollment.statement.principal_id.clone(),
            authorization_epoch: 1,
            previous_authorization_digest: None,
            application_digest: enrollment.statement.application_digest.clone(),
            schema_generation: 7,
            exact_working_set: BTreeSet::from([existing_ref.clone(), absent_ref.clone()]),
            maximum_working_set_bytes: 1_000_000,
            offline: DeviceOfflinePolicy {
                maximum_offline_seconds: 3_600,
                local_reauthentication_seconds: 300,
                upload_grace_seconds: 600,
                maximum_pending_amendments: 10,
                maximum_pending_amendment_bytes: 1_000_000,
            },
            issued_at: 1_800_000_000,
            expires_at: 1_800_003_600,
        };
        let authorization = CertifiedDeviceAuthorization {
            approvals: approve(
                AUTHORIZATION_DOMAIN,
                &authority_keys[&DeviceAuthorityRole::Authorization],
                &authorization_statement,
            ),
            statement: authorization_statement,
        };
        Fixture {
            policy,
            authority_keys,
            exporter,
            enrollment,
            authorization,
            encryption_private_key,
            hardware_signing_key,
            descriptor,
            clock: Arc::new(StdMutex::new(SecureClockReading {
                unix_timestamp: 1_800_000_100,
                monotonic_counter: 100,
            })),
            destroyed: Arc::new(AtomicBool::new(false)),
            existing_ref,
            absent_ref,
        }
    }

    fn hardware(fixture: &Fixture) -> TestHardwareKey {
        TestHardwareKey {
            descriptor: fixture.descriptor.clone(),
            encryption_private_key: fixture.encryption_private_key,
            signing_key: fixture.hardware_signing_key.clone(),
            clock: Arc::clone(&fixture.clock),
            destroyed: Arc::clone(&fixture.destroyed),
        }
    }

    fn resolution(
        fixture: &Fixture,
        amendment: &SignedDeviceAmendment,
    ) -> CertifiedAmendmentResolution {
        let statement = AmendmentResolutionStatement {
            format: DEVICE_RESOLUTION_FORMAT.to_string(),
            resolution_id: id(50 + amendment.statement.amendment_sequence as u128),
            amendment_digest: document_digest(&amendment.statement).unwrap(),
            cell_id: fixture.enrollment.statement.cell_id.clone(),
            device_replica_id: fixture.enrollment.statement.device_replica_id.clone(),
            authorization_epoch: 1,
            disposition: AmendmentResolutionDisposition::Reject,
            resulting_object_digest: None,
            immutable_provenance_digest: DeviceDigest::of_bytes(b"resolution-evidence"),
            resolved_at: 1_800_000_100,
        };
        CertifiedAmendmentResolution {
            approvals: approve(
                RESOLUTION_DOMAIN,
                &fixture.authority_keys[&DeviceAuthorityRole::Resolution],
                &statement,
            ),
            statement,
        }
    }

    fn collect_file_bytes(path: &Path, output: &mut Vec<u8>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                collect_file_bytes(&entry.path(), output);
            } else if metadata.is_file() {
                output.extend(fs::read(entry.path()).unwrap());
            }
        }
    }

    #[test]
    fn encrypted_filtered_replica_amendment_conflict_resolution_and_retirement() {
        let fixture = fixture();
        let temporary = tempfile::tempdir().unwrap();
        let provisioning = fixture
            .exporter
            .provision_database_key(&fixture.enrollment, id(5), 1, 1_800_000_050)
            .unwrap();
        let existing = Record::new("object-a").with_payload(b"DEVICE_PLAINTEXT_SENTINEL".to_vec());
        let existing_digest = document_digest(&existing).unwrap();
        let objects = vec![
            DeviceWorkingSetObject {
                object: fixture.existing_ref.clone(),
                source_version_digest: Some(existing_digest.clone()),
                record: Some(existing.clone()),
            },
            DeviceWorkingSetObject {
                object: fixture.absent_ref.clone(),
                source_version_digest: None,
                record: None,
            },
        ];
        let package = fixture
            .exporter
            .export_working_set(
                &fixture.enrollment,
                &fixture.authorization,
                id(6),
                1,
                None,
                42,
                objects,
                1_800_000_050,
                1_800_000_700,
            )
            .unwrap();
        let unauthorized = fixture.exporter.export_working_set(
            &fixture.enrollment,
            &fixture.authorization,
            id(7),
            1,
            None,
            42,
            vec![DeviceWorkingSetObject {
                object: fixture.existing_ref.clone(),
                source_version_digest: Some(existing_digest.clone()),
                record: Some(existing.clone()),
            }],
            1_800_000_050,
            1_800_000_700,
        );
        assert!(matches!(unauthorized, Err(DeviceError::Limit(_))));

        let database_path = temporary.path().join("device-db");
        let wrong_path = temporary.path().join("wrong-device-db");
        let mut wrong_descriptor = fixture.descriptor.clone();
        wrong_descriptor.key_id = "hardware-b".to_string();
        let wrong = TestHardwareKey {
            descriptor: wrong_descriptor,
            encryption_private_key: [91; 32],
            signing_key: key(22),
            clock: Arc::clone(&fixture.clock),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        assert!(matches!(
            DeviceReplica::open(
                &wrong_path,
                Arc::clone(&fixture.policy),
                fixture.enrollment.clone(),
                fixture.authorization.clone(),
                &provisioning,
                Box::new(wrong),
            ),
            Err(DeviceError::Scope(_))
        ));
        assert!(!wrong_path.exists());

        let mut replica = DeviceReplica::open(
            &database_path,
            Arc::clone(&fixture.policy),
            fixture.enrollment.clone(),
            fixture.authorization.clone(),
            &provisioning,
            Box::new(hardware(&fixture)),
        )
        .unwrap();
        replica.import_working_set(&package).unwrap();
        assert!(matches!(
            replica.import_working_set(&package),
            Err(DeviceError::Scope(_))
        ));
        let session = replica.begin_local_session(id(8)).unwrap();
        assert_eq!(
            replica.get_object(&session, &fixture.existing_ref).unwrap(),
            Some(existing)
        );
        assert_eq!(
            replica.get_object(&session, &fixture.absent_ref).unwrap(),
            None
        );

        let ledger = DeviceParentLedger::open(
            temporary.path().join("parent/device-ledger.cbor"),
            fixture.enrollment.statement.cell_id.clone(),
        )
        .unwrap();
        ledger
            .activate_authorization(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                1_800_000_100,
            )
            .unwrap();
        ledger
            .record_working_set(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                &package,
                1_800_000_100,
            )
            .unwrap();
        let replacement = Record::new("object-a").with_payload(b"replacement".to_vec());
        let amendment_one = replica
            .queue_amendment(
                &session,
                id(9),
                fixture.existing_ref.clone(),
                DeviceAmendmentOperation::Replace {
                    record: replacement,
                },
            )
            .unwrap();
        let clean = ledger
            .admit_amendment(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                &amendment_one,
                1_800_000_150,
                Some(existing_digest),
            )
            .unwrap();
        assert_eq!(clean.kind, AmendmentEvaluationKind::Clean);
        assert!(ledger
            .admit_amendment(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                &amendment_one,
                1_800_000_150,
                clean.current_object_digest.clone(),
            )
            .is_err());
        let resolution_one = resolution(&fixture, &amendment_one);
        let mut future_resolution_statement = resolution_one.statement.clone();
        future_resolution_statement.resolved_at = 1_800_000_101;
        let future_resolution = CertifiedAmendmentResolution {
            approvals: approve(
                RESOLUTION_DOMAIN,
                &fixture.authority_keys[&DeviceAuthorityRole::Resolution],
                &future_resolution_statement,
            ),
            statement: future_resolution_statement,
        };
        assert!(matches!(
            ledger.record_resolution(&fixture.policy, &future_resolution, 1_800_000_100),
            Err(DeviceError::Scope(_))
        ));
        assert!(matches!(
            replica.apply_resolution(&future_resolution),
            Err(DeviceError::Scope(_))
        ));
        ledger
            .record_resolution(&fixture.policy, &resolution_one, 1_800_000_100)
            .unwrap();
        replica.apply_resolution(&resolution_one).unwrap();

        assert!(matches!(
            replica.queue_amendment(
                &session,
                id(10),
                fixture.absent_ref.clone(),
                DeviceAmendmentOperation::Create {
                    record: Record::new("object-b").with_payload(b"new object".to_vec()),
                },
            ),
            Err(DeviceError::Scope(_))
        ));
        {
            let mut clock = fixture.clock.lock().unwrap();
            clock.monotonic_counter = 101;
        }
        let amendment_two = replica
            .queue_amendment(
                &session,
                id(10),
                fixture.absent_ref.clone(),
                DeviceAmendmentOperation::Create {
                    record: Record::new("object-b").with_payload(b"new object".to_vec()),
                },
            )
            .unwrap();
        let conflict = ledger
            .admit_amendment(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                &amendment_two,
                1_800_000_150,
                Some(DeviceDigest::of_bytes(b"concurrent-parent-object")),
            )
            .unwrap();
        assert_eq!(conflict.kind, AmendmentEvaluationKind::Conflict);
        let resolution_two = resolution(&fixture, &amendment_two);
        ledger
            .record_resolution(&fixture.policy, &resolution_two, 1_800_000_100)
            .unwrap();
        replica.apply_resolution(&resolution_two).unwrap();
        assert!(replica.pending_amendments(&session).unwrap().is_empty());

        {
            let mut clock = fixture.clock.lock().unwrap();
            clock.monotonic_counter = 99;
        }
        assert!(matches!(
            replica.pending_amendments(&session),
            Err(DeviceError::Scope(_))
        ));
        {
            let mut clock = fixture.clock.lock().unwrap();
            clock.monotonic_counter = 102;
        }

        let retirement_statement = DeviceRetirementStatement {
            format: DEVICE_RETIREMENT_FORMAT.to_string(),
            retirement_id: id(11),
            enrollment_digest: document_digest(&fixture.enrollment.statement).unwrap(),
            cell_id: fixture.enrollment.statement.cell_id.clone(),
            device_replica_id: fixture.enrollment.statement.device_replica_id.clone(),
            final_authorization_epoch: 1,
            final_authorization_digest: document_digest(&fixture.authorization.statement).unwrap(),
            reason_digest: DeviceDigest::of_bytes(b"device-retired"),
            effective_at: 1_800_000_100,
        };
        let retirement = CertifiedDeviceRetirement {
            approvals: approve(
                RETIREMENT_DOMAIN,
                &fixture.authority_keys[&DeviceAuthorityRole::Retirement],
                &retirement_statement,
            ),
            statement: retirement_statement,
        };
        ledger
            .retire(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                &retirement,
                1_800_000_300,
            )
            .unwrap();
        replica.retire(&retirement).unwrap();
        assert!(fixture.destroyed.load(Ordering::SeqCst));
        assert!(ledger
            .admit_amendment(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                &amendment_two,
                1_800_000_350,
                None,
            )
            .is_err());

        let mut bytes = Vec::new();
        collect_file_bytes(&database_path, &mut bytes);
        assert!(!bytes
            .windows(b"DEVICE_PLAINTEXT_SENTINEL".len())
            .any(|window| window == b"DEVICE_PLAINTEXT_SENTINEL"));
    }

    #[test]
    fn role_substitution_and_unadmitted_resolution_fail_closed() {
        let fixture = fixture();
        let mut substituted_policy = fixture.policy.as_ref().clone();
        substituted_policy.maximum_offline_seconds -= 1;
        assert!(matches!(
            verify_enrollment(&substituted_policy, &fixture.enrollment),
            Err(DeviceError::Invalid(_))
        ));
        let mut enrollment = fixture.enrollment.clone();
        enrollment.approvals = approve(
            AUTHORIZATION_DOMAIN,
            &fixture.authority_keys[&DeviceAuthorityRole::Authorization],
            &enrollment.statement,
        );
        assert!(matches!(
            verify_enrollment(&fixture.policy, &enrollment),
            Err(DeviceError::Signature(_))
        ));

        let temporary = tempfile::tempdir().unwrap();
        let ledger = DeviceParentLedger::open(
            temporary.path().join("ledger.cbor"),
            fixture.enrollment.statement.cell_id.clone(),
        )
        .unwrap();
        ledger
            .activate_authorization(
                &fixture.policy,
                &fixture.enrollment,
                &fixture.authorization,
                1_800_000_100,
            )
            .unwrap();
        let unknown = SignedDeviceAmendment {
            statement: DeviceAmendmentStatement {
                format: DEVICE_AMENDMENT_FORMAT.to_string(),
                amendment_id: id(70),
                enrollment_digest: document_digest(&fixture.enrollment.statement).unwrap(),
                authorization_digest: document_digest(&fixture.authorization.statement).unwrap(),
                cell_id: fixture.enrollment.statement.cell_id.clone(),
                device_replica_id: fixture.enrollment.statement.device_replica_id.clone(),
                principal_id: fixture.enrollment.statement.principal_id.clone(),
                authorization_epoch: 1,
                application_digest: fixture.enrollment.statement.application_digest.clone(),
                schema_generation: 7,
                amendment_sequence: 1,
                previous_amendment_digest: None,
                causal_package_digest: DeviceDigest::of_bytes(b"package"),
                object: fixture.existing_ref.clone(),
                base_version_digest: Some(DeviceDigest::of_bytes(b"base")),
                operation: DeviceAmendmentOperation::Delete,
                authored_at: 1_800_000_100,
                secure_clock_counter: 1,
            },
            signature: vec![],
        };
        let resolution = resolution(&fixture, &unknown);
        assert!(matches!(
            ledger.record_resolution(&fixture.policy, &resolution, 1_800_000_100),
            Err(DeviceError::Scope(_))
        ));
    }
}

impl DeviceReplica {
    pub fn import_working_set(&mut self, package: &SignedDeviceWorkingSet) -> Result<DeviceDigest> {
        let clock = self.observe_clock()?;
        let header = &package.header;
        let enrollment_digest = document_digest(&self.enrollment.statement)?;
        if header.format != DEVICE_WORKING_SET_FORMAT
            || header.enrollment_digest != enrollment_digest
            || header.authorization_digest != self.state.authorization_digest
            || header.cell_id != self.state.cell_id
            || header.device_replica_id != self.state.device_replica_id
            || header.authorization_epoch != self.state.authorization_epoch
            || header.application_digest != self.authorization.statement.application_digest
            || header.schema_generation != self.authorization.statement.schema_generation
            || header.package_sequence != self.state.last_package_sequence.saturating_add(1)
            || header.previous_package_digest != self.state.last_package_digest
            || clock.unix_timestamp < header.issued_at
            || clock.unix_timestamp > header.expires_at
            || header.expires_at > self.authorization.statement.expires_at
            || header.expires_at - header.issued_at
                > self.authorization.statement.offline.maximum_offline_seconds
        {
            return Err(DeviceError::Scope(
                "working-set package is stale, replayed, expired, or outside active device scope"
                    .to_string(),
            ));
        }
        let binding = WorkingSetSignatureBinding {
            header,
            sealed_payload_digest: document_digest(&package.sealed_payload)?,
        };
        verify_export_signature(
            &self.policy,
            WORKING_SET_DOMAIN,
            &package.exporter_key_id,
            &binding,
            &package.signature,
        )?;
        let aad = canonical_cbor(header)?;
        let plaintext = self.hardware.open_sealed(&package.sealed_payload, &aad)?;
        if u64::try_from(plaintext.len()).unwrap_or(u64::MAX) != header.plaintext_bytes
            || DeviceDigest::of_bytes(plaintext.as_ref()) != header.content_digest
        {
            return Err(DeviceError::Cryptography(
                "working-set plaintext length or digest is invalid".to_string(),
            ));
        }
        let payload: DeviceWorkingSetPayload = decode_document(plaintext.as_ref())?;
        if payload.format != DEVICE_WORKING_SET_FORMAT
            || payload.objects.len() != header.object_count as usize
            || payload.objects.len() != self.authorization.statement.exact_working_set.len()
        {
            return Err(DeviceError::Invalid(
                "working-set payload count is not the exact authorization".to_string(),
            ));
        }
        let package_digest = document_digest(&binding)?;
        let mut installed_ids = BTreeSet::new();
        let mut object_records = Vec::with_capacity(payload.objects.len());
        for object in payload.objects {
            object.validate()?;
            if !self
                .authorization
                .statement
                .exact_working_set
                .contains(&object.object)
                || !installed_ids.insert(object.object.storage_id())
            {
                return Err(DeviceError::Scope(
                    "working set contains an unauthorized or duplicate object".to_string(),
                ));
            }
            let storage_id = object.object.storage_id();
            let stored = StoredDeviceObject {
                source: object,
                package_digest: package_digest.clone(),
                authorization_epoch: self.state.authorization_epoch,
            };
            object_records.push(Record::new(storage_id).with_payload(canonical_cbor(&stored)?));
        }
        let existing = self
            .database()?
            .scan_collection(DEVICE_OBJECTS_COLLECTION)?;
        let stale = existing
            .into_iter()
            .map(|record| record.id)
            .filter(|id| !installed_ids.contains(id))
            .collect::<Vec<_>>();
        let mut next = self.state.clone();
        next.last_package_sequence = header.package_sequence;
        next.last_package_digest = Some(package_digest.clone());
        next.last_package_expires_at = Some(header.expires_at);
        let state_record = device_state_record(&next)?;
        let mut transaction = self.database()?.begin_transaction()?;
        transaction.batch_insert(DEVICE_OBJECTS_COLLECTION, object_records)?;
        transaction.delete_many(DEVICE_OBJECTS_COLLECTION, stale)?;
        transaction.insert(DEVICE_STATE_COLLECTION, state_record)?;
        transaction.commit()?;
        self.state = next;
        Ok(package_digest)
    }

    pub fn begin_local_session(&mut self, session_nonce: DeviceId) -> Result<DeviceLocalSession> {
        let clock = self.observe_clock()?;
        self.ensure_live_offline_window(clock.unix_timestamp)?;
        let statement = UserPresenceStatement {
            device_replica_id: self.state.device_replica_id.clone(),
            authorization_epoch: self.state.authorization_epoch,
            session_nonce: session_nonce.clone(),
            clock: clock.clone(),
        };
        let message = signing_message(USER_PRESENCE_DOMAIN, &statement)?;
        let evidence = HardwareUserPresenceEvidence {
            statement,
            signature: self.hardware.sign_with_user_presence(&message)?,
        };
        let key = verifying_key(
            &self.enrollment.statement.hardware_key.signing_public_key,
            "device signing_public_key",
        )?;
        let signature = Signature::from_slice(&evidence.signature)
            .map_err(|_| DeviceError::Signature("malformed user-presence signature".to_string()))?;
        key.verify_strict(&message, &signature).map_err(|_| {
            DeviceError::Signature("hardware user-presence signature failed".to_string())
        })?;
        let package_expiry = self.state.last_package_expires_at.unwrap_or_default();
        let expires_at = clock
            .unix_timestamp
            .saturating_add(
                self.authorization
                    .statement
                    .offline
                    .local_reauthentication_seconds,
            )
            .min(package_expiry)
            .min(self.authorization.statement.expires_at);
        Ok(DeviceLocalSession {
            device_replica_id: self.state.device_replica_id.clone(),
            authorization_epoch: self.state.authorization_epoch,
            session_nonce,
            issued_at: clock.unix_timestamp,
            expires_at,
            user_presence_evidence_digest: document_digest(&evidence)?,
        })
    }

    fn validate_session(&mut self, session: &DeviceLocalSession) -> Result<SecureClockReading> {
        let clock = self.observe_clock()?;
        self.ensure_live_offline_window(clock.unix_timestamp)?;
        if session.device_replica_id != self.state.device_replica_id
            || session.authorization_epoch != self.state.authorization_epoch
            || clock.unix_timestamp < session.issued_at
            || clock.unix_timestamp > session.expires_at
        {
            return Err(DeviceError::OfflineExpired(
                "device-local session is stale, expired, or outside the active epoch".to_string(),
            ));
        }
        Ok(clock)
    }

    pub fn get_object(
        &mut self,
        session: &DeviceLocalSession,
        object: &DeviceObjectRef,
    ) -> Result<Option<Record>> {
        self.validate_session(session)?;
        if !self
            .authorization
            .statement
            .exact_working_set
            .contains(object)
        {
            return Err(DeviceError::Scope(
                "object is outside the exact device working set".to_string(),
            ));
        }
        let stored = self
            .database()?
            .get(DEVICE_OBJECTS_COLLECTION, &object.storage_id())?;
        stored
            .map(|record| {
                let payload = record.payload.as_ref().ok_or_else(|| {
                    DeviceError::Storage("device object has no encrypted payload".to_string())
                })?;
                let stored: StoredDeviceObject = decode_document(payload)?;
                if &stored.source.object != object
                    || stored.authorization_epoch != self.state.authorization_epoch
                {
                    return Err(DeviceError::Scope(
                        "stored device object identity or authorization epoch is invalid"
                            .to_string(),
                    ));
                }
                Ok(stored.source.record)
            })
            .transpose()
            .map(Option::flatten)
    }

    pub fn queue_amendment(
        &mut self,
        session: &DeviceLocalSession,
        amendment_id: DeviceId,
        object: DeviceObjectRef,
        operation: DeviceAmendmentOperation,
    ) -> Result<SignedDeviceAmendment> {
        let clock = self.validate_session(session)?;
        if clock.monotonic_counter <= self.state.last_amendment_secure_clock_counter {
            return Err(DeviceError::Scope(
                "each offline amendment requires a fresh rollback-resistant clock counter"
                    .to_string(),
            ));
        }
        if !self
            .authorization
            .statement
            .exact_working_set
            .contains(&object)
        {
            return Err(DeviceError::Scope(
                "amendment object is outside the exact device working set".to_string(),
            ));
        }
        let current = self
            .database()?
            .get(DEVICE_OBJECTS_COLLECTION, &object.storage_id())?
            .map(|record| {
                let payload = record.payload.as_ref().ok_or_else(|| {
                    DeviceError::Storage("device object has no payload".to_string())
                })?;
                decode_document::<StoredDeviceObject>(payload)
            })
            .transpose()?;
        let base_version_digest = current
            .as_ref()
            .and_then(|stored| stored.source.source_version_digest.clone());
        match &operation {
            DeviceAmendmentOperation::Create { record }
                if current
                    .as_ref()
                    .is_some_and(|stored| stored.source.record.is_none())
                    && record.id == object.object_id => {}
            DeviceAmendmentOperation::Replace { record }
                if current
                    .as_ref()
                    .is_some_and(|stored| stored.source.record.is_some())
                    && record.id == object.object_id => {}
            DeviceAmendmentOperation::Delete
                if current
                    .as_ref()
                    .is_some_and(|stored| stored.source.record.is_some()) => {}
            _ => {
                return Err(DeviceError::Conflict(
                    "local amendment operation does not match the replicated base object"
                        .to_string(),
                ))
            }
        }
        let pending = self
            .database()?
            .scan_collection(DEVICE_AMENDMENTS_COLLECTION)?;
        if pending.len()
            >= self
                .authorization
                .statement
                .offline
                .maximum_pending_amendments as usize
        {
            return Err(DeviceError::Limit(
                "pending amendment count is full".to_string(),
            ));
        }
        let statement = DeviceAmendmentStatement {
            format: DEVICE_AMENDMENT_FORMAT.to_string(),
            amendment_id,
            enrollment_digest: self.state.enrollment_digest.clone(),
            authorization_digest: self.state.authorization_digest.clone(),
            cell_id: self.state.cell_id.clone(),
            device_replica_id: self.state.device_replica_id.clone(),
            principal_id: self.authorization.statement.principal_id.clone(),
            authorization_epoch: self.state.authorization_epoch,
            application_digest: self.authorization.statement.application_digest.clone(),
            schema_generation: self.authorization.statement.schema_generation,
            amendment_sequence: self.state.last_amendment_sequence.saturating_add(1),
            previous_amendment_digest: self.state.last_amendment_digest.clone(),
            causal_package_digest: self.state.last_package_digest.clone().ok_or_else(|| {
                DeviceError::OfflineExpired("no causal working-set package".to_string())
            })?,
            object,
            base_version_digest,
            operation,
            authored_at: clock.unix_timestamp,
            secure_clock_counter: clock.monotonic_counter,
        };
        let message = signing_message(AMENDMENT_DOMAIN, &statement)?;
        let signed = SignedDeviceAmendment {
            statement,
            signature: self.hardware.sign(&message)?,
        };
        let amendment_bytes = canonical_cbor(&signed)?;
        let existing_bytes = pending.iter().try_fold(0_u64, |sum, record| {
            let length = record.payload.as_ref().map_or(0, Vec::len) as u64;
            sum.checked_add(length)
                .ok_or_else(|| DeviceError::Limit("pending byte count overflow".to_string()))
        })?;
        if existing_bytes.saturating_add(amendment_bytes.len() as u64)
            > self
                .authorization
                .statement
                .offline
                .maximum_pending_amendment_bytes
        {
            return Err(DeviceError::Limit(
                "pending amendment byte queue is full".to_string(),
            ));
        }
        let digest = document_digest(&signed.statement)?;
        let mut next = self.state.clone();
        next.last_amendment_sequence = signed.statement.amendment_sequence;
        next.last_amendment_digest = Some(digest);
        next.last_amendment_secure_clock_counter = signed.statement.secure_clock_counter;
        let amendment_record = Record::new(format!(
            "amendment-{:020}",
            signed.statement.amendment_sequence
        ))
        .with_payload(amendment_bytes);
        let mut transaction = self.database()?.begin_transaction()?;
        transaction.insert(DEVICE_AMENDMENTS_COLLECTION, amendment_record)?;
        transaction.insert(DEVICE_STATE_COLLECTION, device_state_record(&next)?)?;
        transaction.commit()?;
        self.state = next;
        Ok(signed)
    }

    pub fn pending_amendments(
        &mut self,
        session: &DeviceLocalSession,
    ) -> Result<Vec<SignedDeviceAmendment>> {
        self.validate_session(session)?;
        self.database()?
            .scan_collection(DEVICE_AMENDMENTS_COLLECTION)?
            .into_iter()
            .map(|record| {
                let payload = record.payload.ok_or_else(|| {
                    DeviceError::Storage("pending amendment has no payload".to_string())
                })?;
                decode_document(&payload)
            })
            .collect()
    }

    pub fn apply_resolution(
        &mut self,
        resolution: &CertifiedAmendmentResolution,
    ) -> Result<DeviceDigest> {
        let digest = verify_amendment_resolution(&self.policy, resolution)?;
        let clock = self.observe_clock()?;
        if resolution.statement.cell_id != self.state.cell_id
            || resolution.statement.device_replica_id != self.state.device_replica_id
            || resolution.statement.authorization_epoch != self.state.authorization_epoch
            || resolution.statement.resolved_at > clock.unix_timestamp
        {
            return Err(DeviceError::Scope(
                "resolution belongs to another device replica".to_string(),
            ));
        }
        let pending = self
            .database()?
            .scan_collection(DEVICE_AMENDMENTS_COLLECTION)?;
        let mut matched = None;
        for record in pending {
            let payload = record.payload.as_ref().ok_or_else(|| {
                DeviceError::Storage("pending amendment has no payload".to_string())
            })?;
            let amendment: SignedDeviceAmendment = decode_document(payload)?;
            if document_digest(&amendment.statement)? == resolution.statement.amendment_digest {
                matched = Some(record);
                break;
            }
        }
        let record = matched.ok_or_else(|| {
            DeviceError::Scope("resolution has no exact pending amendment".to_string())
        })?;
        self.database_mut()?
            .delete(DEVICE_AMENDMENTS_COLLECTION, &record.id)?;
        Ok(digest)
    }

    pub fn retire(&mut self, retirement: &CertifiedDeviceRetirement) -> Result<DeviceDigest> {
        let digest = verify_retirement(
            &self.policy,
            &self.enrollment,
            &self.authorization,
            retirement,
        )?;
        let clock = self.observe_clock()?;
        if retirement.statement.effective_at > clock.unix_timestamp {
            return Err(DeviceError::Scope(
                "future-scheduled retirement is not yet effective on this device".to_string(),
            ));
        }
        let mut state = self.state.clone();
        state.retirement_digest = Some(digest.clone());
        persist_device_state(self.database_mut()?, &state)?;
        self.state = state;
        if let Some(database) = self.database.take() {
            database.close()?;
        }
        self.hardware.destroy()?;
        Ok(digest)
    }

    pub fn close(mut self) -> Result<()> {
        if let Some(database) = self.database.take() {
            database.close()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DeviceLocalState {
    format: String,
    enrollment_digest: DeviceDigest,
    cell_id: DeviceId,
    device_replica_id: DeviceId,
    database_key_epoch: u64,
    authorization_epoch: u64,
    authorization_digest: DeviceDigest,
    last_package_sequence: u64,
    last_package_digest: Option<DeviceDigest>,
    last_package_expires_at: Option<i64>,
    highest_secure_clock_counter: u64,
    highest_secure_clock_timestamp: i64,
    last_amendment_sequence: u64,
    last_amendment_digest: Option<DeviceDigest>,
    last_amendment_secure_clock_counter: u64,
    retirement_digest: Option<DeviceDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct StoredDeviceObject {
    source: DeviceWorkingSetObject,
    package_digest: DeviceDigest,
    authorization_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceLocalSession {
    pub device_replica_id: DeviceId,
    pub authorization_epoch: u64,
    pub session_nonce: DeviceId,
    pub issued_at: i64,
    pub expires_at: i64,
    pub user_presence_evidence_digest: DeviceDigest,
}

pub struct DeviceReplica {
    policy: Arc<DeviceTrustPolicy>,
    enrollment: CertifiedDeviceEnrollment,
    authorization: CertifiedDeviceAuthorization,
    hardware: Box<dyn HardwareBoundDeviceKey>,
    database: Option<BicDb>,
    state: DeviceLocalState,
}

impl std::fmt::Debug for DeviceReplica {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeviceReplica")
            .field("device_replica_id", &self.state.device_replica_id)
            .field("authorization_epoch", &self.state.authorization_epoch)
            .field("retired", &self.state.retirement_digest.is_some())
            .finish_non_exhaustive()
    }
}

impl DeviceReplica {
    pub fn open(
        database_path: impl AsRef<Path>,
        policy: Arc<DeviceTrustPolicy>,
        enrollment: CertifiedDeviceEnrollment,
        authorization: CertifiedDeviceAuthorization,
        provisioning: &ProvisionedDeviceDatabaseKey,
        mut hardware: Box<dyn HardwareBoundDeviceKey>,
    ) -> Result<Self> {
        policy.validate()?;
        verify_hardware_descriptor(&enrollment, hardware.as_ref())?;
        verify_database_key_provisioning(&policy, &enrollment, provisioning)?;
        let clock = hardware.secure_clock()?;
        if clock.unix_timestamp <= 0 || clock.monotonic_counter == 0 {
            return Err(DeviceError::Scope(
                "hardware secure clock returned an invalid initial reading".to_string(),
            ));
        }
        let enrollment_digest = verify_enrollment(&policy, &enrollment)?;
        let authorization_digest =
            verify_authorization(&policy, &enrollment, &authorization, clock.unix_timestamp)?;
        prepare_device_database_path(database_path.as_ref())?;
        let aad = database_key_aad(&provisioning.statement)?;
        let key_bytes = hardware.open_sealed(&provisioning.sealed_key, &aad)?;
        if key_bytes.len() != 32
            || DeviceDigest::of_bytes(key_bytes.as_ref()) != provisioning.statement.key_fingerprint
        {
            return Err(DeviceError::Cryptography(
                "unsealed device database key has the wrong length or fingerprint".to_string(),
            ));
        }
        create_device_database_directory(database_path.as_ref())?;
        let binding = EncryptionBinding::new(
            format!(
                "device:{}:{}",
                enrollment.statement.cell_id, enrollment.statement.device_replica_id
            ),
            DEVICE_STORAGE_PROFILE,
            provisioning.statement.key_epoch,
        )?;
        let mut database = BicDb::open_with_encryption(
            database_path,
            DbConfig::default().with_fsync(true),
            EncryptionConfig::with_raw_key(key_bytes.to_vec()).with_binding(binding),
        )?;
        database.create_collection(DEVICE_OBJECTS_COLLECTION)?;
        database.create_collection(DEVICE_AMENDMENTS_COLLECTION)?;
        database.create_collection(DEVICE_STATE_COLLECTION)?;

        let existing_state = database
            .get(DEVICE_STATE_COLLECTION, DEVICE_STATE_RECORD_ID)?
            .map(|record| {
                let payload = record.payload.as_ref().ok_or_else(|| {
                    DeviceError::Storage("device state record has no payload".to_string())
                })?;
                decode_document::<DeviceLocalState>(payload)
            })
            .transpose()?;
        let state = if let Some(state) = existing_state {
            if state.format != DEVICE_LOCAL_STATE_FORMAT
                || state.enrollment_digest != enrollment_digest
                || state.cell_id != enrollment.statement.cell_id
                || state.device_replica_id != enrollment.statement.device_replica_id
                || state.database_key_epoch != provisioning.statement.key_epoch
                || state.authorization_epoch != authorization.statement.authorization_epoch
                || state.authorization_digest != authorization_digest
                || state.retirement_digest.is_some()
            {
                return Err(DeviceError::Scope(
                    "device database state does not match the exact active enrollment, key, and authorization"
                        .to_string(),
                ));
            }
            if clock.monotonic_counter < state.highest_secure_clock_counter
                || clock.unix_timestamp < state.highest_secure_clock_timestamp
            {
                return Err(DeviceError::Scope(
                    "hardware secure clock moved backward relative to encrypted device state"
                        .to_string(),
                ));
            }
            state
        } else {
            if authorization.statement.authorization_epoch != 1
                || authorization
                    .statement
                    .previous_authorization_digest
                    .is_some()
            {
                return Err(DeviceError::Scope(
                    "a new device database must begin at authorization epoch 1".to_string(),
                ));
            }
            let state = DeviceLocalState {
                format: DEVICE_LOCAL_STATE_FORMAT.to_string(),
                enrollment_digest,
                cell_id: enrollment.statement.cell_id.clone(),
                device_replica_id: enrollment.statement.device_replica_id.clone(),
                database_key_epoch: provisioning.statement.key_epoch,
                authorization_epoch: authorization.statement.authorization_epoch,
                authorization_digest,
                last_package_sequence: 0,
                last_package_digest: None,
                last_package_expires_at: None,
                highest_secure_clock_counter: clock.monotonic_counter,
                highest_secure_clock_timestamp: clock.unix_timestamp,
                last_amendment_sequence: 0,
                last_amendment_digest: None,
                last_amendment_secure_clock_counter: 0,
                retirement_digest: None,
            };
            persist_device_state(&mut database, &state)?;
            state
        };
        Ok(Self {
            policy,
            enrollment,
            authorization,
            hardware,
            database: Some(database),
            state,
        })
    }

    fn database(&self) -> Result<&BicDb> {
        self.database
            .as_ref()
            .ok_or_else(|| DeviceError::Retired("device database is closed or retired".to_string()))
    }

    fn database_mut(&mut self) -> Result<&mut BicDb> {
        self.database
            .as_mut()
            .ok_or_else(|| DeviceError::Retired("device database is closed or retired".to_string()))
    }

    fn observe_clock(&mut self) -> Result<SecureClockReading> {
        let clock = self.hardware.secure_clock()?;
        if clock.monotonic_counter < self.state.highest_secure_clock_counter
            || clock.unix_timestamp < self.state.highest_secure_clock_timestamp
        {
            return Err(DeviceError::Scope(
                "hardware secure clock moved backward".to_string(),
            ));
        }
        if clock.monotonic_counter > self.state.highest_secure_clock_counter
            || clock.unix_timestamp > self.state.highest_secure_clock_timestamp
        {
            let mut next = self.state.clone();
            next.highest_secure_clock_counter = clock.monotonic_counter;
            next.highest_secure_clock_timestamp = clock.unix_timestamp;
            persist_device_state(self.database_mut()?, &next)?;
            self.state = next;
        }
        Ok(clock)
    }

    fn ensure_live_offline_window(&self, now: i64) -> Result<()> {
        if self.state.retirement_digest.is_some() {
            return Err(DeviceError::Retired("device is retired".to_string()));
        }
        let package_expiry = self.state.last_package_expires_at.ok_or_else(|| {
            DeviceError::OfflineExpired("no authorized working set is installed".to_string())
        })?;
        if now > package_expiry || now > self.authorization.statement.expires_at {
            return Err(DeviceError::OfflineExpired(
                "offline working set or authorization has expired".to_string(),
            ));
        }
        Ok(())
    }

    pub fn activate_authorization(
        &mut self,
        next: CertifiedDeviceAuthorization,
    ) -> Result<DeviceDigest> {
        let clock = self.observe_clock()?;
        let digest =
            verify_authorization(&self.policy, &self.enrollment, &next, clock.unix_timestamp)?;
        if next.statement.authorization_epoch != self.state.authorization_epoch.saturating_add(1)
            || next.statement.previous_authorization_digest.as_ref()
                != Some(&self.state.authorization_digest)
        {
            return Err(DeviceError::Scope(
                "device authorization must advance by one exact predecessor".to_string(),
            ));
        }
        let mut state = self.state.clone();
        state.authorization_epoch = next.statement.authorization_epoch;
        state.authorization_digest = digest.clone();
        state.last_package_sequence = 0;
        state.last_package_digest = None;
        state.last_package_expires_at = None;
        persist_device_state(self.database_mut()?, &state)?;
        self.authorization = next;
        self.state = state;
        Ok(digest)
    }
}

fn device_state_record(state: &DeviceLocalState) -> Result<Record> {
    Ok(Record::new(DEVICE_STATE_RECORD_ID).with_payload(canonical_cbor(state)?))
}

fn persist_device_state(database: &mut BicDb, state: &DeviceLocalState) -> Result<()> {
    database.insert(DEVICE_STATE_COLLECTION, device_state_record(state)?)?;
    database.flush()?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeviceAmendmentOperation {
    Create { record: Record },
    Replace { record: Record },
    Delete,
}

impl DeviceAmendmentOperation {
    fn resulting_digest(&self) -> Result<Option<DeviceDigest>> {
        match self {
            Self::Create { record } | Self::Replace { record } => {
                Ok(Some(document_digest(record)?))
            }
            Self::Delete => Ok(None),
        }
    }

    fn record_id(&self) -> Option<&str> {
        match self {
            Self::Create { record } | Self::Replace { record } => Some(&record.id),
            Self::Delete => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeviceAmendmentStatement {
    pub format: String,
    pub amendment_id: DeviceId,
    pub enrollment_digest: DeviceDigest,
    pub authorization_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub principal_id: String,
    pub authorization_epoch: u64,
    pub application_digest: DeviceDigest,
    pub schema_generation: u64,
    pub amendment_sequence: u64,
    pub previous_amendment_digest: Option<DeviceDigest>,
    pub causal_package_digest: DeviceDigest,
    pub object: DeviceObjectRef,
    pub base_version_digest: Option<DeviceDigest>,
    pub operation: DeviceAmendmentOperation,
    pub authored_at: i64,
    pub secure_clock_counter: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SignedDeviceAmendment {
    pub statement: DeviceAmendmentStatement,
    pub signature: Vec<u8>,
}

pub fn verify_device_amendment_signature(
    enrollment: &CertifiedDeviceEnrollment,
    amendment: &SignedDeviceAmendment,
) -> Result<DeviceDigest> {
    let key = verifying_key(
        &enrollment.statement.hardware_key.signing_public_key,
        "device signing_public_key",
    )?;
    let signature = Signature::from_slice(&amendment.signature)
        .map_err(|_| DeviceError::Signature("malformed device amendment signature".to_string()))?;
    key.verify_strict(
        &signing_message(AMENDMENT_DOMAIN, &amendment.statement)?,
        &signature,
    )
    .map_err(|_| DeviceError::Signature("device amendment signature failed".to_string()))?;
    document_digest(&amendment.statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AmendmentEvaluationKind {
    Clean,
    Conflict,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AmendmentEvaluation {
    pub amendment_digest: DeviceDigest,
    pub kind: AmendmentEvaluationKind,
    pub proposed_base_digest: Option<DeviceDigest>,
    pub current_object_digest: Option<DeviceDigest>,
    pub proposed_result_digest: Option<DeviceDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AmendmentResolutionDisposition {
    Accept,
    Reject,
    Merge,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AmendmentResolutionStatement {
    pub format: String,
    pub resolution_id: DeviceId,
    pub amendment_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub authorization_epoch: u64,
    pub disposition: AmendmentResolutionDisposition,
    pub resulting_object_digest: Option<DeviceDigest>,
    pub immutable_provenance_digest: DeviceDigest,
    pub resolved_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedAmendmentResolution {
    pub statement: AmendmentResolutionStatement,
    pub approvals: Vec<DeviceAuthorityApproval>,
}

pub fn verify_amendment_resolution(
    policy: &DeviceTrustPolicy,
    resolution: &CertifiedAmendmentResolution,
) -> Result<DeviceDigest> {
    let statement = &resolution.statement;
    if statement.format != DEVICE_RESOLUTION_FORMAT
        || statement.authorization_epoch == 0
        || statement.resolved_at <= 0
        || matches!(
            statement.disposition,
            AmendmentResolutionDisposition::Accept | AmendmentResolutionDisposition::Merge
        ) && statement.resulting_object_digest.is_none()
        || statement.disposition == AmendmentResolutionDisposition::Reject
            && statement.resulting_object_digest.is_some()
    {
        return Err(DeviceError::Invalid(
            "amendment resolution shape is invalid".to_string(),
        ));
    }
    verify_approvals(
        policy,
        DeviceAuthorityRole::Resolution,
        RESOLUTION_DOMAIN,
        statement,
        &resolution.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceRetirementStatement {
    pub format: String,
    pub retirement_id: DeviceId,
    pub enrollment_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub final_authorization_epoch: u64,
    pub final_authorization_digest: DeviceDigest,
    pub reason_digest: DeviceDigest,
    pub effective_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedDeviceRetirement {
    pub statement: DeviceRetirementStatement,
    pub approvals: Vec<DeviceAuthorityApproval>,
}

pub fn verify_retirement(
    policy: &DeviceTrustPolicy,
    enrollment: &CertifiedDeviceEnrollment,
    authorization: &CertifiedDeviceAuthorization,
    retirement: &CertifiedDeviceRetirement,
) -> Result<DeviceDigest> {
    let enrollment_digest = verify_enrollment(policy, enrollment)?;
    let authorization_digest = document_digest(&authorization.statement)?;
    let statement = &retirement.statement;
    if statement.format != DEVICE_RETIREMENT_FORMAT
        || statement.enrollment_digest != enrollment_digest
        || statement.cell_id != enrollment.statement.cell_id
        || statement.device_replica_id != enrollment.statement.device_replica_id
        || statement.final_authorization_epoch != authorization.statement.authorization_epoch
        || statement.final_authorization_digest != authorization_digest
        || statement.effective_at < enrollment.statement.enrolled_at
    {
        return Err(DeviceError::Scope(
            "device retirement is outside the exact enrollment and authorization".to_string(),
        ));
    }
    verify_approvals(
        policy,
        DeviceAuthorityRole::Retirement,
        RETIREMENT_DOMAIN,
        statement,
        &retirement.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ParentReplicaState {
    enrollment_digest: DeviceDigest,
    authorization_epoch: u64,
    authorization_digest: DeviceDigest,
    last_package_sequence: u64,
    last_package_digest: Option<DeviceDigest>,
    authorized_packages: BTreeSet<DeviceDigest>,
    last_amendment_sequence: u64,
    last_amendment_digest: Option<DeviceDigest>,
    highest_amendment_secure_clock_counter: u64,
    pending_amendments: BTreeSet<DeviceDigest>,
    resolved_amendments: BTreeSet<DeviceDigest>,
    retirement_digest: Option<DeviceDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ParentLedgerState {
    format: String,
    cell_id: DeviceId,
    devices: BTreeMap<DeviceId, ParentReplicaState>,
}

pub struct DeviceParentLedger {
    storage: ParentLedgerStorage,
    state: RwLock<ParentLedgerState>,
}

enum ParentLedgerStorage {
    File(PathBuf),
    /// The Cell integration uses the parent Cell database so ledger updates
    /// inherit its encryption, commit fencing, replication, backup, and
    /// recovery lineage instead of becoming a host-local sidecar.
    Database(Arc<RwLock<BicDb>>),
}

impl std::fmt::Debug for DeviceParentLedger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeviceParentLedger")
            .field(
                "storage",
                &match self.storage {
                    ParentLedgerStorage::File(_) => "file",
                    ParentLedgerStorage::Database(_) => "cell-database",
                },
            )
            .finish_non_exhaustive()
    }
}

impl DeviceParentLedger {
    pub fn open(path: impl Into<PathBuf>, cell_id: DeviceId) -> Result<Self> {
        let path = path.into();
        let state = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                    return Err(DeviceError::Storage(
                        "parent device ledger must be a regular file".to_string(),
                    ));
                }
                if metadata.len() as usize > MAX_DOCUMENT_BYTES {
                    return Err(DeviceError::Storage(
                        "parent device ledger exceeds 16 MiB".to_string(),
                    ));
                }
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                let mut options = OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
                }
                options.open(&path)?.read_to_end(&mut bytes)?;
                let state: ParentLedgerState = decode_document(&bytes)?;
                if state.format != DEVICE_PARENT_STATE_FORMAT || state.cell_id != cell_id {
                    return Err(DeviceError::Scope(
                        "parent device ledger belongs to another Cell".to_string(),
                    ));
                }
                state
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ParentLedgerState {
                format: DEVICE_PARENT_STATE_FORMAT.to_string(),
                cell_id,
                devices: BTreeMap::new(),
            },
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            storage: ParentLedgerStorage::File(path),
            state: RwLock::new(state),
        })
    }

    pub fn open_cell_database(database: Arc<RwLock<BicDb>>, cell_id: DeviceId) -> Result<Self> {
        let state = {
            let mut database = database.write();
            database.create_collection(DEVICE_PARENT_STATE_COLLECTION)?;
            database
                .get(
                    DEVICE_PARENT_STATE_COLLECTION,
                    DEVICE_PARENT_STATE_RECORD_ID,
                )?
                .map(|record| {
                    let payload = record.payload.as_ref().ok_or_else(|| {
                        DeviceError::Storage(
                            "parent device ledger record has no encrypted payload".to_string(),
                        )
                    })?;
                    let state: ParentLedgerState = decode_document(payload)?;
                    if state.format != DEVICE_PARENT_STATE_FORMAT || state.cell_id != cell_id {
                        return Err(DeviceError::Scope(
                            "parent device ledger belongs to another Cell".to_string(),
                        ));
                    }
                    Ok(state)
                })
                .transpose()?
                .unwrap_or(ParentLedgerState {
                    format: DEVICE_PARENT_STATE_FORMAT.to_string(),
                    cell_id,
                    devices: BTreeMap::new(),
                })
        };
        Ok(Self {
            storage: ParentLedgerStorage::Database(database),
            state: RwLock::new(state),
        })
    }

    fn persist(&self, state: &ParentLedgerState) -> Result<()> {
        match &self.storage {
            ParentLedgerStorage::File(path) => Self::persist_file(path, state),
            ParentLedgerStorage::Database(database) => {
                let record =
                    Record::new(DEVICE_PARENT_STATE_RECORD_ID).with_payload(canonical_cbor(state)?);
                let mut database = database.write();
                database.insert(DEVICE_PARENT_STATE_COLLECTION, record)?;
                database.flush()?;
                Ok(())
            }
        }
    }

    fn persist_file(path: &Path, state: &ParentLedgerState) -> Result<()> {
        let parent = path.parent().ok_or_else(|| {
            DeviceError::Storage("parent device ledger has no parent directory".to_string())
        })?;
        fs::create_dir_all(parent)?;
        let staging = parent.join(format!(
            ".{}.{}.new",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("device-ledger"),
            Uuid::new_v4()
        ));
        let bytes = canonical_cbor(state)?;
        let result = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
            }
            let mut file = options.open(&staging)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&staging, path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&staging);
        }
        result
    }

    pub fn activate_authorization(
        &self,
        policy: &DeviceTrustPolicy,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        now: i64,
    ) -> Result<DeviceDigest> {
        let enrollment_digest = verify_enrollment(policy, enrollment)?;
        let authorization_digest = verify_authorization(policy, enrollment, authorization, now)?;
        let mut state = self.state.write();
        let current = state.devices.get(&enrollment.statement.device_replica_id);
        match current {
            None if authorization.statement.authorization_epoch == 1
                && authorization
                    .statement
                    .previous_authorization_digest
                    .is_none() => {}
            Some(current)
                if current.retirement_digest.is_none()
                    && authorization.statement.authorization_epoch
                        == current.authorization_epoch.saturating_add(1)
                    && authorization
                        .statement
                        .previous_authorization_digest
                        .as_ref()
                        == Some(&current.authorization_digest) => {}
            Some(current)
                if current.retirement_digest.is_none()
                    && authorization.statement.authorization_epoch
                        == current.authorization_epoch
                    && authorization_digest == current.authorization_digest =>
            {
                return Ok(authorization_digest)
            }
            Some(current) if current.retirement_digest.is_some() => {
                return Err(DeviceError::Retired(
                    "retired device authorization cannot advance".to_string(),
                ))
            }
            _ => {
                return Err(DeviceError::Scope(
                    "authorization is not the exact next parent-ledger epoch".to_string(),
                ))
            }
        }
        let previous = current.cloned();
        let mut next = state.clone();
        next.devices.insert(
            enrollment.statement.device_replica_id.clone(),
            ParentReplicaState {
                enrollment_digest,
                authorization_epoch: authorization.statement.authorization_epoch,
                authorization_digest: authorization_digest.clone(),
                last_package_sequence: 0,
                last_package_digest: None,
                authorized_packages: BTreeSet::new(),
                last_amendment_sequence: previous
                    .as_ref()
                    .map_or(0, |entry| entry.last_amendment_sequence),
                last_amendment_digest: previous
                    .as_ref()
                    .and_then(|entry| entry.last_amendment_digest.clone()),
                highest_amendment_secure_clock_counter: previous
                    .as_ref()
                    .map_or(0, |entry| entry.highest_amendment_secure_clock_counter),
                pending_amendments: previous
                    .as_ref()
                    .map_or_else(BTreeSet::new, |entry| entry.pending_amendments.clone()),
                resolved_amendments: previous
                    .map_or_else(BTreeSet::new, |entry| entry.resolved_amendments),
                retirement_digest: None,
            },
        );
        self.persist(&next)?;
        *state = next;
        Ok(authorization_digest)
    }

    pub fn record_working_set(
        &self,
        policy: &DeviceTrustPolicy,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        package: &SignedDeviceWorkingSet,
        recorded_at: i64,
    ) -> Result<DeviceDigest> {
        let authorization_digest = document_digest(&authorization.statement)?;
        let package_digest =
            verify_working_set_envelope(policy, enrollment, authorization, package)?;
        let mut state = self.state.write();
        let mut next = state.clone();
        let device = next
            .devices
            .get_mut(&authorization.statement.device_replica_id)
            .ok_or_else(|| DeviceError::Scope("device authorization is not active".to_string()))?;
        if device.retirement_digest.is_some()
            || device.authorization_epoch != authorization.statement.authorization_epoch
            || device.authorization_digest != authorization_digest
            || package.header.package_sequence != device.last_package_sequence.saturating_add(1)
            || package.header.previous_package_digest != device.last_package_digest
            || device.authorized_packages.len() >= MAX_AUTHORIZED_PACKAGE_HISTORY
            || package.header.issued_at > recorded_at
            || recorded_at > package.header.expires_at
        {
            return Err(DeviceError::Scope(
                "working-set package is out of order, over history limits, or outside active authority"
                    .to_string(),
            ));
        }
        device.last_package_sequence = package.header.package_sequence;
        device.last_package_digest = Some(package_digest.clone());
        device.authorized_packages.insert(package_digest.clone());
        self.persist(&next)?;
        *state = next;
        Ok(package_digest)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn admit_amendment(
        &self,
        policy: &DeviceTrustPolicy,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        amendment: &SignedDeviceAmendment,
        received_at: i64,
        current_object_digest: Option<DeviceDigest>,
    ) -> Result<AmendmentEvaluation> {
        let enrollment_digest = verify_enrollment(policy, enrollment)?;
        let authorization_digest = document_digest(&authorization.statement)?;
        let statement = &amendment.statement;
        statement.object.validate()?;
        let parent_deadline = authorization
            .statement
            .expires_at
            .checked_add(authorization.statement.offline.upload_grace_seconds)
            .ok_or_else(|| DeviceError::Invalid("amendment deadline overflow".to_string()))?;
        if statement.format != DEVICE_AMENDMENT_FORMAT
            || statement.enrollment_digest != enrollment_digest
            || statement.authorization_digest != authorization_digest
            || statement.cell_id != authorization.statement.cell_id
            || statement.device_replica_id != authorization.statement.device_replica_id
            || statement.principal_id != authorization.statement.principal_id
            || statement.authorization_epoch != authorization.statement.authorization_epoch
            || statement.application_digest != authorization.statement.application_digest
            || statement.schema_generation != authorization.statement.schema_generation
            || !authorization
                .statement
                .exact_working_set
                .contains(&statement.object)
            || statement.authored_at < authorization.statement.issued_at
            || statement.authored_at > authorization.statement.expires_at
            || received_at < statement.authored_at
            || received_at > parent_deadline
            || statement.secure_clock_counter == 0
        {
            return Err(DeviceError::Scope(
                "device amendment is outside current authorization, working set, or time bounds"
                    .to_string(),
            ));
        }
        match &statement.operation {
            DeviceAmendmentOperation::Create { .. } if statement.base_version_digest.is_none() => {}
            DeviceAmendmentOperation::Replace { .. } | DeviceAmendmentOperation::Delete
                if statement.base_version_digest.is_some() => {}
            _ => {
                return Err(DeviceError::Invalid(
                    "create must have no base; replace/delete must name a base digest".to_string(),
                ))
            }
        }
        if statement
            .operation
            .record_id()
            .is_some_and(|id| id != statement.object.object_id)
        {
            return Err(DeviceError::Scope(
                "amendment record id differs from its authorized object".to_string(),
            ));
        }
        let amendment_digest = verify_device_amendment_signature(enrollment, amendment)?;
        let mut state = self.state.write();
        let mut next = state.clone();
        let device = next
            .devices
            .get_mut(&statement.device_replica_id)
            .ok_or_else(|| DeviceError::Scope("device authorization is not active".to_string()))?;
        if device.retirement_digest.is_some() {
            return Err(DeviceError::Retired(
                "parent refuses amendments from a retired device".to_string(),
            ));
        }
        if device.enrollment_digest != enrollment_digest
            || device.authorization_epoch != statement.authorization_epoch
            || device.authorization_digest != authorization_digest
            || statement.amendment_sequence != device.last_amendment_sequence.saturating_add(1)
            || statement.previous_amendment_digest != device.last_amendment_digest
            || statement.secure_clock_counter <= device.highest_amendment_secure_clock_counter
            || !device
                .authorized_packages
                .contains(&statement.causal_package_digest)
        {
            return Err(DeviceError::Scope(
                "device amendment is replayed, out of order, or belongs to another epoch"
                    .to_string(),
            ));
        }
        let kind = if statement.base_version_digest == current_object_digest {
            AmendmentEvaluationKind::Clean
        } else {
            AmendmentEvaluationKind::Conflict
        };
        let evaluation = AmendmentEvaluation {
            amendment_digest: amendment_digest.clone(),
            kind,
            proposed_base_digest: statement.base_version_digest.clone(),
            current_object_digest,
            proposed_result_digest: statement.operation.resulting_digest()?,
        };
        device.last_amendment_sequence = statement.amendment_sequence;
        device.last_amendment_digest = Some(amendment_digest.clone());
        device.highest_amendment_secure_clock_counter = statement.secure_clock_counter;
        device.pending_amendments.insert(amendment_digest);
        self.persist(&next)?;
        *state = next;
        Ok(evaluation)
    }

    pub fn record_resolution(
        &self,
        policy: &DeviceTrustPolicy,
        resolution: &CertifiedAmendmentResolution,
        now: i64,
    ) -> Result<DeviceDigest> {
        let digest = verify_amendment_resolution(policy, resolution)?;
        let mut state = self.state.write();
        if resolution.statement.cell_id != state.cell_id || resolution.statement.resolved_at > now {
            return Err(DeviceError::Scope(
                "resolution belongs to another Cell".to_string(),
            ));
        }
        let mut next = state.clone();
        let device = next
            .devices
            .get_mut(&resolution.statement.device_replica_id)
            .ok_or_else(|| DeviceError::Scope("resolution names an unknown device".to_string()))?;
        if device.retirement_digest.is_some()
            || device.authorization_epoch != resolution.statement.authorization_epoch
            || !device
                .pending_amendments
                .contains(&resolution.statement.amendment_digest)
            || device
                .resolved_amendments
                .contains(&resolution.statement.amendment_digest)
        {
            return Err(DeviceError::Scope(
                "resolution is stale, replayed, or names a retired device".to_string(),
            ));
        }
        device
            .pending_amendments
            .remove(&resolution.statement.amendment_digest);
        device
            .resolved_amendments
            .insert(resolution.statement.amendment_digest.clone());
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }

    pub fn retire(
        &self,
        policy: &DeviceTrustPolicy,
        enrollment: &CertifiedDeviceEnrollment,
        authorization: &CertifiedDeviceAuthorization,
        retirement: &CertifiedDeviceRetirement,
        now: i64,
    ) -> Result<DeviceDigest> {
        let digest = verify_retirement(policy, enrollment, authorization, retirement)?;
        if retirement.statement.effective_at > now {
            return Err(DeviceError::Scope(
                "future-scheduled retirement is not yet effective at the parent".to_string(),
            ));
        }
        let mut state = self.state.write();
        let mut next = state.clone();
        let device = next
            .devices
            .get_mut(&retirement.statement.device_replica_id)
            .ok_or_else(|| DeviceError::Scope("retirement names an unknown device".to_string()))?;
        if device.retirement_digest.is_some()
            || device.authorization_epoch != retirement.statement.final_authorization_epoch
            || device.authorization_digest != retirement.statement.final_authorization_digest
        {
            return Err(DeviceError::Retired(
                "device is already retired or retirement is stale".to_string(),
            ));
        }
        device.retirement_digest = Some(digest.clone());
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeviceWorkingSetObject {
    pub object: DeviceObjectRef,
    /// `None` is a signed tombstone for an authorized object that does not yet
    /// exist. This permits a later offline `Create` without granting wildcard
    /// create authority.
    pub source_version_digest: Option<DeviceDigest>,
    pub record: Option<Record>,
}

impl DeviceWorkingSetObject {
    fn validate(&self) -> Result<()> {
        self.object.validate()?;
        match (&self.source_version_digest, &self.record) {
            (Some(source_version_digest), Some(record))
                if record.id == self.object.object_id
                    && source_version_digest == &document_digest(record)? => {}
            (None, None) => {}
            _ => {
                return Err(DeviceError::Invalid(
                    "working-set record identity, tombstone, or source digest is invalid"
                        .to_string(),
                ))
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct DeviceWorkingSetPayload {
    format: String,
    objects: Vec<DeviceWorkingSetObject>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceWorkingSetHeader {
    pub format: String,
    pub package_id: DeviceId,
    pub enrollment_digest: DeviceDigest,
    pub authorization_digest: DeviceDigest,
    pub cell_id: DeviceId,
    pub device_replica_id: DeviceId,
    pub authorization_epoch: u64,
    pub application_digest: DeviceDigest,
    pub schema_generation: u64,
    pub package_sequence: u64,
    pub previous_package_digest: Option<DeviceDigest>,
    pub source_commit_seq: u64,
    pub object_count: u32,
    pub plaintext_bytes: u64,
    pub content_digest: DeviceDigest,
    pub issued_at: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedDeviceWorkingSet {
    pub header: DeviceWorkingSetHeader,
    pub sealed_payload: SealedDevicePayload,
    pub exporter_key_id: String,
    pub signature: Vec<u8>,
}

#[derive(Serialize)]
struct WorkingSetSignatureBinding<'a> {
    header: &'a DeviceWorkingSetHeader,
    sealed_payload_digest: DeviceDigest,
}

pub fn verify_working_set_envelope(
    policy: &DeviceTrustPolicy,
    enrollment: &CertifiedDeviceEnrollment,
    authorization: &CertifiedDeviceAuthorization,
    package: &SignedDeviceWorkingSet,
) -> Result<DeviceDigest> {
    let enrollment_digest = verify_enrollment(policy, enrollment)?;
    let authorization_digest =
        verify_authorization(policy, enrollment, authorization, package.header.issued_at)?;
    let header = &package.header;
    if header.format != DEVICE_WORKING_SET_FORMAT
        || header.enrollment_digest != enrollment_digest
        || header.authorization_digest != authorization_digest
        || header.cell_id != authorization.statement.cell_id
        || header.device_replica_id != authorization.statement.device_replica_id
        || header.authorization_epoch != authorization.statement.authorization_epoch
        || header.application_digest != authorization.statement.application_digest
        || header.schema_generation != authorization.statement.schema_generation
        || header.package_sequence == 0
        || header.package_sequence == 1 && header.previous_package_digest.is_some()
        || header.package_sequence > 1 && header.previous_package_digest.is_none()
        || header.object_count as usize != authorization.statement.exact_working_set.len()
        || header.plaintext_bytes == 0
        || header.plaintext_bytes > authorization.statement.maximum_working_set_bytes
        || header.expires_at <= header.issued_at
        || header.expires_at > authorization.statement.expires_at
        || header.expires_at - header.issued_at
            > authorization.statement.offline.maximum_offline_seconds
    {
        return Err(DeviceError::Scope(
            "working-set envelope is outside its exact authorization".to_string(),
        ));
    }
    let binding = WorkingSetSignatureBinding {
        header,
        sealed_payload_digest: document_digest(&package.sealed_payload)?,
    };
    verify_export_signature(
        policy,
        WORKING_SET_DOMAIN,
        &package.exporter_key_id,
        &binding,
        &package.signature,
    )?;
    document_digest(&binding)
}

impl DeviceExporter {
    #[allow(clippy::too_many_arguments)]
    pub fn export_working_set(
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
        let enrollment_digest = verify_enrollment(&self.policy, enrollment)?;
        let authorization_digest =
            verify_authorization(&self.policy, enrollment, authorization, issued_at)?;
        if package_sequence == 0
            || package_sequence == 1 && previous_package_digest.is_some()
            || package_sequence > 1 && previous_package_digest.is_none()
            || expires_at <= issued_at
            || expires_at > authorization.statement.expires_at
            || expires_at - issued_at > authorization.statement.offline.maximum_offline_seconds
        {
            return Err(DeviceError::Invalid(
                "working-set sequence, predecessor, or offline expiry is invalid".to_string(),
            ));
        }
        if objects.is_empty()
            || objects.len() != authorization.statement.exact_working_set.len()
            || objects.len()
                > usize::try_from(self.policy.maximum_working_set_objects).unwrap_or(usize::MAX)
        {
            return Err(DeviceError::Limit(
                "working set must contain every and only exact authorized object".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for object in &objects {
            object.validate()?;
            if !authorization
                .statement
                .exact_working_set
                .contains(&object.object)
                || !seen.insert(object.object.clone())
            {
                return Err(DeviceError::Scope(
                    "working set contains an unauthorized or duplicate object".to_string(),
                ));
            }
        }
        let payload = DeviceWorkingSetPayload {
            format: DEVICE_WORKING_SET_FORMAT.to_string(),
            objects,
        };
        let payload_bytes = canonical_cbor(&payload)?;
        let plaintext_bytes = u64::try_from(payload_bytes.len())
            .map_err(|_| DeviceError::Limit("working set is too large".to_string()))?;
        if plaintext_bytes > authorization.statement.maximum_working_set_bytes
            || plaintext_bytes > self.policy.maximum_working_set_bytes
        {
            return Err(DeviceError::Limit(
                "working-set plaintext exceeds the signed byte bound".to_string(),
            ));
        }
        let header = DeviceWorkingSetHeader {
            format: DEVICE_WORKING_SET_FORMAT.to_string(),
            package_id,
            enrollment_digest,
            authorization_digest,
            cell_id: authorization.statement.cell_id.clone(),
            device_replica_id: authorization.statement.device_replica_id.clone(),
            authorization_epoch: authorization.statement.authorization_epoch,
            application_digest: authorization.statement.application_digest.clone(),
            schema_generation: authorization.statement.schema_generation,
            package_sequence,
            previous_package_digest,
            source_commit_seq,
            object_count: u32::try_from(payload.objects.len())
                .map_err(|_| DeviceError::Limit("too many working-set objects".to_string()))?,
            plaintext_bytes,
            content_digest: DeviceDigest::of_bytes(&payload_bytes),
            issued_at,
            expires_at,
        };
        let aad = canonical_cbor(&header)?;
        let sealed_payload = seal_for_device(
            &enrollment.statement.hardware_key.encryption_public_key,
            &aad,
            &payload_bytes,
        )?;
        let binding = WorkingSetSignatureBinding {
            header: &header,
            sealed_payload_digest: document_digest(&sealed_payload)?,
        };
        let signature = self
            .signing_key
            .sign(&signing_message(WORKING_SET_DOMAIN, &binding)?)
            .to_bytes()
            .to_vec();
        Ok(SignedDeviceWorkingSet {
            header,
            sealed_payload,
            exporter_key_id: self.key_id.clone(),
            signature,
        })
    }
}
