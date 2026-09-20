//! Bounded, encrypted object grants between independently isolated Cells.
//!
//! A recipient never receives a source Cell key and never opens a source
//! database. The only cross-boundary artifact is deterministic, signed,
//! bounded, non-executable CBOR carrying ciphertext. Each object uses an
//! independent random DEK, and RFC 9180 HPKE wraps that DEK to one certified
//! recipient key. Source and recipient ledgers live inside their respective
//! encrypted Cell databases.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bicdb_core::{BicDb, BicDbError, Record};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hpke::{
    aead::ChaCha20Poly1305 as HpkeChaCha20Poly1305, kdf::HkdfSha256, kem::X25519HkdfSha256,
    Deserializable, Kem as KemTrait, OpModeR, OpModeS, Serializable,
};
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

pub const GRANT_TRUST_POLICY_FORMAT: &str = "bicdb.cell-grant-trust-policy/v1";
pub const RECIPIENT_KEY_FORMAT: &str = "bicdb.cell-grant-recipient-key/v1";
pub const GRANT_FORMAT: &str = "bicdb.cell-grant/v1";
pub const GRANT_PACKAGE_FORMAT: &str = "bicdb.cell-grant-package/v1";
pub const GRANT_REVOCATION_FORMAT: &str = "bicdb.cell-grant-revocation/v1";
pub const IMPORT_REVIEW_FORMAT: &str = "bicdb.cell-grant-import-review/v1";
pub const IMPORTED_EVIDENCE_FORMAT: &str = "bicdb.cell-grant-imported-evidence/v1";
pub const SOURCE_LEDGER_FORMAT: &str = "bicdb.cell-grant-source-ledger/v1";
pub const RECIPIENT_LEDGER_FORMAT: &str = "bicdb.cell-grant-recipient-ledger/v1";
pub const HPKE_SUITE: &str = "DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/ChaCha20Poly1305";

const RECIPIENT_KEY_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-RECIPIENT-KEY-V1\0";
const GRANT_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-V1\0";
const PACKAGE_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-PACKAGE-V1\0";
const REVOCATION_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-REVOCATION-V1\0";
const IMPORT_REVIEW_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-IMPORT-REVIEW-V1\0";
const HPKE_INFO_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-HPKE-INFO-V1\0";
const OBJECT_PAYLOAD_AAD_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-OBJECT-AAD-V1\0";
const OBJECT_WRAP_AAD_DOMAIN: &[u8] = b"BICDB-CELL-GRANT-WRAP-AAD-V1\0";
const MAX_DOCUMENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_AUTHORITIES: usize = 127;
const MAX_KEYS: usize = 32;
const ABSOLUTE_MAX_OBJECTS: usize = 100_000;
const ABSOLUTE_MAX_OBJECT_BYTES: u64 = 32 * 1024 * 1024;
const ABSOLUTE_MAX_PACKAGE_BYTES: u64 = 60 * 1024 * 1024;
const ABSOLUTE_MAX_GRANT_SECONDS: i64 = 366 * 24 * 60 * 60;
const SOURCE_STATE_COLLECTION: &str = "bicdb_cell_grant_source_state";
const SOURCE_STATE_RECORD_ID: &str = "state";
const RECIPIENT_STATE_COLLECTION: &str = "bicdb_cell_grant_recipient_state";
const RECIPIENT_STATE_RECORD_ID: &str = "state";
const IMPORTED_EVIDENCE_COLLECTION: &str = "bicdb_cell_grant_imported_evidence";

type HpkeKem = X25519HkdfSha256;
type HpkeAead = HpkeChaCha20Poly1305;
type HpkeKdf = HkdfSha256;

pub type Result<T> = std::result::Result<T, GrantError>;

#[derive(Debug, thiserror::Error)]
pub enum GrantError {
    #[error("CELL_GRANT_DOCUMENT_INVALID: {0}")]
    Invalid(String),
    #[error("CELL_GRANT_SIGNATURE_INVALID: {0}")]
    Signature(String),
    #[error("CELL_GRANT_QUORUM_INSUFFICIENT: {0}")]
    Quorum(String),
    #[error("CELL_GRANT_SCOPE_MISMATCH: {0}")]
    Scope(String),
    #[error("CELL_GRANT_EXPIRED: {0}")]
    Expired(String),
    #[error("CELL_GRANT_REVOKED: {0}")]
    Revoked(String),
    #[error("CELL_GRANT_REPLAY: {0}")]
    Replay(String),
    #[error("CELL_GRANT_LIMIT_EXCEEDED: {0}")]
    Limit(String),
    #[error("CELL_GRANT_CRYPTOGRAPHY: {0}")]
    Cryptography(String),
    #[error("CELL_GRANT_STORAGE: {0}")]
    Storage(String),
    #[error("CELL_GRANT_DATABASE: {0}")]
    Database(#[from] BicDbError),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct GrantDigest(String);

impl GrantDigest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let Some(hex_value) = value.strip_prefix("sha256:") else {
            return Err(GrantError::Invalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        };
        if value != value.to_ascii_lowercase()
            || hex_value.len() != 64
            || !hex_value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(GrantError::Invalid(
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

impl std::fmt::Display for GrantDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GrantDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct GrantId(String);

impl GrantId {
    pub fn parse(value: impl Into<String>, label: &str) -> Result<Self> {
        let value = value.into();
        let parsed = Uuid::parse_str(&value)
            .map_err(|_| GrantError::Invalid(format!("{label} must be a UUID")))?;
        if parsed.is_nil() {
            return Err(GrantError::Invalid(format!("{label} must not be nil")));
        }
        Ok(Self(parsed.hyphenated().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for GrantId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GrantId {
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
        .map_err(|error| GrantError::Invalid(format!("encode deterministic CBOR: {error}")))?;
    Ok(bytes)
}

pub fn encode_document<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_cbor(value)
}

pub fn decode_document<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(GrantError::Invalid(
            "grant document is empty or exceeds 64 MiB".to_string(),
        ));
    }
    let value: T = ciborium::de::from_reader(bytes)
        .map_err(|error| GrantError::Invalid(format!("decode CBOR: {error}")))?;
    if canonical_cbor(&value)? != bytes {
        return Err(GrantError::Invalid(
            "grant document is not in BicDB deterministic CBOR encoding".to_string(),
        ));
    }
    Ok(value)
}

pub fn document_digest<T: Serialize>(value: &T) -> Result<GrantDigest> {
    Ok(GrantDigest::of_bytes(&canonical_cbor(value)?))
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
        Err(GrantError::Invalid(format!(
            "{label} must be a 1..256 byte portable ASCII identifier"
        )))
    }
}

fn verifying_key(value: &str, label: &str) -> Result<VerifyingKey> {
    if value != value.to_ascii_lowercase() {
        return Err(GrantError::Invalid(format!(
            "{label} must be lowercase hexadecimal"
        )));
    }
    let bytes = hex::decode(value)
        .map_err(|_| GrantError::Invalid(format!("{label} is not hexadecimal")))?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| GrantError::Invalid(format!("{label} must contain 32 bytes")))?;
    VerifyingKey::from_bytes(&array)
        .map_err(|_| GrantError::Invalid(format!("{label} is not an Ed25519 public key")))
}

fn hpke_public_key(value: &str, label: &str) -> Result<<HpkeKem as KemTrait>::PublicKey> {
    if value != value.to_ascii_lowercase() {
        return Err(GrantError::Invalid(format!(
            "{label} must be lowercase hexadecimal"
        )));
    }
    let bytes = hex::decode(value)
        .map_err(|_| GrantError::Invalid(format!("{label} is not hexadecimal")))?;
    if bytes.len() != 32 || bytes.iter().all(|byte| *byte == 0) {
        return Err(GrantError::Invalid(format!(
            "{label} must contain a non-zero 32-byte X25519 public key"
        )));
    }
    <HpkeKem as KemTrait>::PublicKey::from_bytes(&bytes)
        .map_err(|_| GrantError::Invalid(format!("{label} is not an X25519 public key")))
}

fn signing_message<T: Serialize>(domain: &[u8], statement: &T) -> Result<Vec<u8>> {
    let payload = canonical_cbor(statement)?;
    let mut message = Vec::with_capacity(domain.len() + payload.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(&payload);
    Ok(message)
}

fn domain_document<T: Serialize>(domain: &[u8], value: &T) -> Result<Vec<u8>> {
    signing_message(domain, value)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum GrantAuthorityRole {
    Issue,
    Revocation,
    RecipientKey,
    ImportAcceptance,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantAuthorityKey {
    pub key_id: String,
    pub role: GrantAuthorityRole,
    pub public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantExportKey {
    pub key_id: String,
    pub public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecipientEncryptionKey {
    pub key_id: String,
    pub public_key: String,
    pub key_epoch: u64,
    pub hardware_attestation_digest: GrantDigest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantTrustPolicy {
    pub format: String,
    pub policy_id: String,
    pub cell_id: GrantId,
    pub authorities: Vec<GrantAuthorityKey>,
    pub issue_threshold: u16,
    pub revocation_threshold: u16,
    pub recipient_key_threshold: u16,
    pub import_acceptance_threshold: u16,
    pub export_keys: Vec<GrantExportKey>,
    pub recipient_encryption_keys: Vec<RecipientEncryptionKey>,
    pub trusted_remote_anchor_digests: BTreeMap<GrantId, GrantDigest>,
    pub allowed_data_categories: BTreeSet<String>,
    pub allowed_media_types: BTreeSet<String>,
    pub allowed_purpose_digests: BTreeSet<GrantDigest>,
    pub maximum_grant_seconds: i64,
    pub maximum_objects_per_grant: u32,
    pub maximum_object_bytes: u64,
    pub maximum_package_bytes: u64,
    pub maximum_packages_per_grant: u32,
}

impl GrantTrustPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.format != GRANT_TRUST_POLICY_FORMAT {
            return Err(GrantError::Invalid(format!(
                "unsupported grant trust policy format {}",
                self.format
            )));
        }
        require_label(&self.policy_id, "policy_id")?;
        if self.authorities.len() < 8 || self.authorities.len() > MAX_AUTHORITIES {
            return Err(GrantError::Invalid(
                "grant trust policy requires 8..127 authority keys".to_string(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut public_keys = BTreeSet::new();
        let mut role_counts = BTreeMap::<GrantAuthorityRole, usize>::new();
        for authority in &self.authorities {
            require_label(&authority.key_id, "authority key_id")?;
            let key = verifying_key(&authority.public_key, "authority public_key")?;
            if !ids.insert(authority.key_id.clone()) || !public_keys.insert(key.to_bytes()) {
                return Err(GrantError::Invalid(
                    "authority key ids and public keys must be globally unique".to_string(),
                ));
            }
            *role_counts.entry(authority.role).or_default() += 1;
        }
        for (role, threshold) in [
            (GrantAuthorityRole::Issue, self.issue_threshold),
            (GrantAuthorityRole::Revocation, self.revocation_threshold),
            (
                GrantAuthorityRole::RecipientKey,
                self.recipient_key_threshold,
            ),
            (
                GrantAuthorityRole::ImportAcceptance,
                self.import_acceptance_threshold,
            ),
        ] {
            let available = role_counts.get(&role).copied().unwrap_or_default();
            if threshold < 2 || usize::from(threshold) > available {
                return Err(GrantError::Invalid(format!(
                    "{role:?} requires a threshold of at least two within its own disjoint keys"
                )));
            }
        }
        if self.export_keys.is_empty() || self.export_keys.len() > MAX_KEYS {
            return Err(GrantError::Invalid(
                "grant policy requires 1..32 export keys".to_string(),
            ));
        }
        for export in &self.export_keys {
            require_label(&export.key_id, "export key_id")?;
            let key = verifying_key(&export.public_key, "export public_key")?;
            if !ids.insert(export.key_id.clone()) || !public_keys.insert(key.to_bytes()) {
                return Err(GrantError::Invalid(
                    "export keys cannot reuse authority ids or key material".to_string(),
                ));
            }
        }
        if self.recipient_encryption_keys.is_empty()
            || self.recipient_encryption_keys.len() > MAX_KEYS
        {
            return Err(GrantError::Invalid(
                "grant policy requires 1..32 recipient encryption keys".to_string(),
            ));
        }
        let mut recipient_material = BTreeSet::new();
        for key in &self.recipient_encryption_keys {
            require_label(&key.key_id, "recipient key_id")?;
            if !ids.insert(key.key_id.clone()) || key.key_epoch == 0 {
                return Err(GrantError::Invalid(
                    "recipient key ids must be unique and epochs positive".to_string(),
                ));
            }
            let public = hpke_public_key(&key.public_key, "recipient public_key")?;
            if !recipient_material.insert(public.to_bytes().as_slice().to_vec()) {
                return Err(GrantError::Invalid(
                    "recipient public keys must be unique".to_string(),
                ));
            }
        }
        if self.trusted_remote_anchor_digests.is_empty()
            || self
                .trusted_remote_anchor_digests
                .contains_key(&self.cell_id)
        {
            return Err(GrantError::Invalid(
                "trusted remote policy pins must be non-empty and exclude the local Cell"
                    .to_string(),
            ));
        }
        if self.allowed_data_categories.is_empty()
            || self.allowed_media_types.is_empty()
            || self.allowed_purpose_digests.is_empty()
            || self
                .allowed_data_categories
                .iter()
                .chain(self.allowed_media_types.iter())
                .any(|value| !valid_label(value))
        {
            return Err(GrantError::Invalid(
                "grant policy allowlists must be non-empty portable identifiers".to_string(),
            ));
        }
        if !(1..=ABSOLUTE_MAX_GRANT_SECONDS).contains(&self.maximum_grant_seconds)
            || self.maximum_objects_per_grant == 0
            || usize::try_from(self.maximum_objects_per_grant).unwrap_or(usize::MAX)
                > ABSOLUTE_MAX_OBJECTS
            || self.maximum_object_bytes == 0
            || self.maximum_object_bytes > ABSOLUTE_MAX_OBJECT_BYTES
            || self.maximum_package_bytes == 0
            || self.maximum_package_bytes > ABSOLUTE_MAX_PACKAGE_BYTES
            || self.maximum_object_bytes > self.maximum_package_bytes
            || self.maximum_packages_per_grant == 0
        {
            return Err(GrantError::Invalid(
                "grant policy limits are zero, unbounded, or internally inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn threshold(&self, role: GrantAuthorityRole) -> u16 {
        match role {
            GrantAuthorityRole::Issue => self.issue_threshold,
            GrantAuthorityRole::Revocation => self.revocation_threshold,
            GrantAuthorityRole::RecipientKey => self.recipient_key_threshold,
            GrantAuthorityRole::ImportAcceptance => self.import_acceptance_threshold,
        }
    }

    fn authority(&self, key_id: &str, role: GrantAuthorityRole) -> Result<VerifyingKey> {
        let authority = self
            .authorities
            .iter()
            .find(|candidate| candidate.key_id == key_id && candidate.role == role)
            .ok_or_else(|| {
                GrantError::Signature(format!("key {key_id} is not authorized for {role:?}"))
            })?;
        verifying_key(&authority.public_key, "authority public_key")
    }

    fn export_key(&self, key_id: &str) -> Result<VerifyingKey> {
        let key = self
            .export_keys
            .iter()
            .find(|candidate| candidate.key_id == key_id)
            .ok_or_else(|| GrantError::Signature("unknown source export key".to_string()))?;
        verifying_key(&key.public_key, "export public_key")
    }

    fn recipient_key(&self, key_id: &str) -> Result<&RecipientEncryptionKey> {
        self.recipient_encryption_keys
            .iter()
            .find(|candidate| candidate.key_id == key_id)
            .ok_or_else(|| GrantError::Scope("unknown recipient encryption key".to_string()))
    }

    /// Digest of every local security decision while deliberately excluding
    /// outbound remote pins. Excluding only that map makes two Cells able to
    /// pin each other without an impossible recursive hash cycle.
    pub fn trust_anchor_digest(&self) -> Result<GrantDigest> {
        #[derive(Serialize)]
        struct Anchor<'a> {
            format: &'a str,
            policy_id: &'a str,
            cell_id: &'a GrantId,
            authorities: &'a [GrantAuthorityKey],
            issue_threshold: u16,
            revocation_threshold: u16,
            recipient_key_threshold: u16,
            import_acceptance_threshold: u16,
            export_keys: &'a [GrantExportKey],
            recipient_encryption_keys: &'a [RecipientEncryptionKey],
            allowed_data_categories: &'a BTreeSet<String>,
            allowed_media_types: &'a BTreeSet<String>,
            allowed_purpose_digests: &'a BTreeSet<GrantDigest>,
            maximum_grant_seconds: i64,
            maximum_objects_per_grant: u32,
            maximum_object_bytes: u64,
            maximum_package_bytes: u64,
            maximum_packages_per_grant: u32,
        }
        document_digest(&Anchor {
            format: &self.format,
            policy_id: &self.policy_id,
            cell_id: &self.cell_id,
            authorities: &self.authorities,
            issue_threshold: self.issue_threshold,
            revocation_threshold: self.revocation_threshold,
            recipient_key_threshold: self.recipient_key_threshold,
            import_acceptance_threshold: self.import_acceptance_threshold,
            export_keys: &self.export_keys,
            recipient_encryption_keys: &self.recipient_encryption_keys,
            allowed_data_categories: &self.allowed_data_categories,
            allowed_media_types: &self.allowed_media_types,
            allowed_purpose_digests: &self.allowed_purpose_digests,
            maximum_grant_seconds: self.maximum_grant_seconds,
            maximum_objects_per_grant: self.maximum_objects_per_grant,
            maximum_object_bytes: self.maximum_object_bytes,
            maximum_package_bytes: self.maximum_package_bytes,
            maximum_packages_per_grant: self.maximum_packages_per_grant,
        })
    }

    fn trusts(&self, remote: &GrantTrustPolicy) -> Result<()> {
        let expected = self
            .trusted_remote_anchor_digests
            .get(&remote.cell_id)
            .ok_or_else(|| GrantError::Scope("remote Cell policy is not trusted".to_string()))?;
        if expected != &remote.trust_anchor_digest()? {
            return Err(GrantError::Scope(
                "remote Cell trust-anchor digest does not match the pinned policy".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantAuthorityApproval {
    pub key_id: String,
    pub signature: Vec<u8>,
}

fn sign_approval<T: Serialize>(
    domain: &[u8],
    key_id: impl Into<String>,
    statement: &T,
    signing_key: &SigningKey,
) -> Result<GrantAuthorityApproval> {
    Ok(GrantAuthorityApproval {
        key_id: key_id.into(),
        signature: signing_key
            .sign(&signing_message(domain, statement)?)
            .to_bytes()
            .to_vec(),
    })
}

pub fn sign_recipient_key_approval(
    key_id: impl Into<String>,
    statement: &RecipientKeyStatement,
    signing_key: &SigningKey,
) -> Result<GrantAuthorityApproval> {
    sign_approval(RECIPIENT_KEY_DOMAIN, key_id, statement, signing_key)
}

pub fn sign_grant_approval(
    key_id: impl Into<String>,
    statement: &GrantStatement,
    signing_key: &SigningKey,
) -> Result<GrantAuthorityApproval> {
    sign_approval(GRANT_DOMAIN, key_id, statement, signing_key)
}

pub fn sign_revocation_approval(
    key_id: impl Into<String>,
    statement: &GrantRevocationStatement,
    signing_key: &SigningKey,
) -> Result<GrantAuthorityApproval> {
    sign_approval(REVOCATION_DOMAIN, key_id, statement, signing_key)
}

pub fn sign_import_review_approval(
    key_id: impl Into<String>,
    statement: &ImportReviewStatement,
    signing_key: &SigningKey,
) -> Result<GrantAuthorityApproval> {
    sign_approval(IMPORT_REVIEW_DOMAIN, key_id, statement, signing_key)
}

fn verify_approvals<T: Serialize>(
    policy: &GrantTrustPolicy,
    role: GrantAuthorityRole,
    domain: &[u8],
    statement: &T,
    approvals: &[GrantAuthorityApproval],
) -> Result<()> {
    policy.validate()?;
    let message = signing_message(domain, statement)?;
    let mut accepted = BTreeSet::new();
    for approval in approvals {
        if !accepted.insert(approval.key_id.clone()) {
            return Err(GrantError::Signature(
                "duplicate authority approval".to_string(),
            ));
        }
        let key = policy.authority(&approval.key_id, role)?;
        let signature = Signature::from_slice(&approval.signature)
            .map_err(|_| GrantError::Signature("malformed authority signature".to_string()))?;
        key.verify_strict(&message, &signature).map_err(|_| {
            GrantError::Signature("authority signature verification failed".to_string())
        })?;
    }
    if accepted.len() < usize::from(policy.threshold(role)) {
        return Err(GrantError::Quorum(format!(
            "{role:?} has {} approvals, needs {}",
            accepted.len(),
            policy.threshold(role)
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecipientKeyStatement {
    pub format: String,
    pub certificate_id: GrantId,
    pub trust_policy_digest: GrantDigest,
    pub recipient_cell_id: GrantId,
    pub application_digest: GrantDigest,
    pub schema_generation: u64,
    pub key_id: String,
    pub key_epoch: u64,
    pub hpke_suite: String,
    pub public_key: String,
    pub hardware_attestation_digest: GrantDigest,
    pub valid_from: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedRecipientKey {
    pub statement: RecipientKeyStatement,
    pub approvals: Vec<GrantAuthorityApproval>,
}

pub fn verify_recipient_key(
    policy: &GrantTrustPolicy,
    certificate: &CertifiedRecipientKey,
    now: i64,
) -> Result<GrantDigest> {
    policy.validate()?;
    let statement = &certificate.statement;
    let pinned = policy.recipient_key(&statement.key_id)?;
    if statement.format != RECIPIENT_KEY_FORMAT
        || statement.trust_policy_digest != document_digest(policy)?
        || statement.recipient_cell_id != policy.cell_id
        || statement.schema_generation == 0
        || statement.key_epoch != pinned.key_epoch
        || statement.hpke_suite != HPKE_SUITE
        || statement.public_key != pinned.public_key
        || statement.hardware_attestation_digest != pinned.hardware_attestation_digest
        || statement.valid_from <= 0
        || statement.expires_at < statement.valid_from
    {
        return Err(GrantError::Scope(
            "recipient key certificate is not bound to the exact policy, Cell, application, schema, key, and suite"
                .to_string(),
        ));
    }
    if now < statement.valid_from || now > statement.expires_at {
        return Err(GrantError::Expired(
            "recipient key certificate is not currently valid".to_string(),
        ));
    }
    hpke_public_key(&statement.public_key, "recipient certificate public_key")?;
    verify_approvals(
        policy,
        GrantAuthorityRole::RecipientKey,
        RECIPIENT_KEY_DOMAIN,
        statement,
        &certificate.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct GrantObjectRef {
    pub namespace: String,
    pub object_id: String,
    pub source_version_digest: GrantDigest,
    pub data_category: String,
    pub media_type: String,
}

impl GrantObjectRef {
    fn validate(&self) -> Result<()> {
        require_label(&self.namespace, "object namespace")?;
        require_label(&self.data_category, "object data_category")?;
        require_label(&self.media_type, "object media_type")?;
        if self.object_id.is_empty() || self.object_id.len() > 1024 || self.object_id.contains('\0')
        {
            return Err(GrantError::Invalid(
                "object_id must be 1..1024 bytes and contain no NUL".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RedisclosurePolicy {
    Prohibited,
    NewExplicitGrantRequired,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantStatement {
    pub format: String,
    pub grant_id: GrantId,
    pub grant_epoch: u64,
    pub previous_grant_digest: Option<GrantDigest>,
    pub source_policy_digest: GrantDigest,
    pub recipient_policy_digest: GrantDigest,
    pub source_cell_id: GrantId,
    pub recipient_cell_id: GrantId,
    pub source_application_digest: GrantDigest,
    pub recipient_application_digest: GrantDigest,
    pub source_schema_generation: u64,
    pub recipient_schema_generation: u64,
    pub recipient_key_certificate_digest: GrantDigest,
    pub exact_objects: BTreeSet<GrantObjectRef>,
    pub purpose_digest: GrantDigest,
    pub redisclosure: RedisclosurePolicy,
    pub valid_from: i64,
    pub expires_at: i64,
    pub maximum_object_bytes: u64,
    pub maximum_total_bytes: u64,
    pub maximum_packages: u32,
    pub anti_replay_nonce: GrantId,
    pub issued_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedCrossCellGrant {
    pub statement: GrantStatement,
    pub approvals: Vec<GrantAuthorityApproval>,
}

pub fn verify_grant(
    source_policy: &GrantTrustPolicy,
    recipient_policy: &GrantTrustPolicy,
    recipient_key: &CertifiedRecipientKey,
    grant: &CertifiedCrossCellGrant,
    now: i64,
) -> Result<GrantDigest> {
    source_policy.validate()?;
    recipient_policy.validate()?;
    source_policy.trusts(recipient_policy)?;
    recipient_policy.trusts(source_policy)?;
    let recipient_key_digest = verify_recipient_key(recipient_policy, recipient_key, now)?;
    let statement = &grant.statement;
    if statement.format != GRANT_FORMAT
        || statement.source_policy_digest != document_digest(source_policy)?
        || statement.recipient_policy_digest != document_digest(recipient_policy)?
        || statement.source_cell_id != source_policy.cell_id
        || statement.recipient_cell_id != recipient_policy.cell_id
        || statement.source_cell_id == statement.recipient_cell_id
        || statement.source_schema_generation == 0
        || statement.recipient_schema_generation == 0
        || statement.recipient_key_certificate_digest != recipient_key_digest
        || statement.recipient_application_digest != recipient_key.statement.application_digest
        || statement.recipient_schema_generation != recipient_key.statement.schema_generation
        || statement.grant_epoch == 0
        || statement.issued_at <= 0
        || statement.valid_from < statement.issued_at
        || statement.expires_at < statement.valid_from
        || recipient_key.statement.valid_from > statement.valid_from
        || recipient_key.statement.expires_at < statement.expires_at
    {
        return Err(GrantError::Scope(
            "grant is not bound to the exact mutually trusted Cells, policies, applications, schema generations, and recipient key"
                .to_string(),
        ));
    }
    if now < statement.valid_from || now > statement.expires_at {
        return Err(GrantError::Expired(
            "grant is not currently valid".to_string(),
        ));
    }
    let lifetime = statement.expires_at.saturating_sub(statement.valid_from);
    if lifetime > source_policy.maximum_grant_seconds
        || lifetime > recipient_policy.maximum_grant_seconds
    {
        return Err(GrantError::Limit(
            "grant lifetime exceeds a Cell policy".to_string(),
        ));
    }
    if statement.exact_objects.is_empty()
        || statement.exact_objects.len()
            > usize::try_from(source_policy.maximum_objects_per_grant).unwrap_or(usize::MAX)
        || statement.exact_objects.len()
            > usize::try_from(recipient_policy.maximum_objects_per_grant).unwrap_or(usize::MAX)
        || statement.maximum_object_bytes == 0
        || statement.maximum_object_bytes > source_policy.maximum_object_bytes
        || statement.maximum_object_bytes > recipient_policy.maximum_object_bytes
        || statement.maximum_total_bytes == 0
        || statement.maximum_total_bytes > source_policy.maximum_package_bytes
        || statement.maximum_total_bytes > recipient_policy.maximum_package_bytes
        || statement.maximum_object_bytes > statement.maximum_total_bytes
        || statement.maximum_packages == 0
        || statement.maximum_packages > source_policy.maximum_packages_per_grant
        || statement.maximum_packages > recipient_policy.maximum_packages_per_grant
    {
        return Err(GrantError::Limit(
            "grant object/package bounds exceed a Cell policy".to_string(),
        ));
    }
    if !source_policy
        .allowed_purpose_digests
        .contains(&statement.purpose_digest)
        || !recipient_policy
            .allowed_purpose_digests
            .contains(&statement.purpose_digest)
    {
        return Err(GrantError::Scope(
            "grant purpose is not allowed by both Cells".to_string(),
        ));
    }
    for object in &statement.exact_objects {
        object.validate()?;
        if !source_policy
            .allowed_data_categories
            .contains(&object.data_category)
            || !recipient_policy
                .allowed_data_categories
                .contains(&object.data_category)
            || !source_policy
                .allowed_media_types
                .contains(&object.media_type)
            || !recipient_policy
                .allowed_media_types
                .contains(&object.media_type)
        {
            return Err(GrantError::Scope(
                "grant object category or media type is not allowed by both Cells".to_string(),
            ));
        }
    }
    verify_approvals(
        source_policy,
        GrantAuthorityRole::Issue,
        GRANT_DOMAIN,
        statement,
        &grant.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantPlaintextObject {
    pub object: GrantObjectRef,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HpkeWrappedObjectKey {
    pub suite: String,
    pub recipient_key_id: String,
    pub encapsulated_key: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EncryptedGrantObject {
    pub object: GrantObjectRef,
    pub plaintext_digest: GrantDigest,
    pub plaintext_bytes: u64,
    pub nonce: Vec<u8>,
    pub ciphertext_digest: GrantDigest,
    pub ciphertext: Vec<u8>,
    pub wrapped_dek: HpkeWrappedObjectKey,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantPackageStatement {
    pub format: String,
    pub package_id: GrantId,
    pub grant_id: GrantId,
    pub grant_epoch: u64,
    pub grant_digest: GrantDigest,
    pub source_cell_id: GrantId,
    pub recipient_cell_id: GrantId,
    pub source_application_digest: GrantDigest,
    pub recipient_application_digest: GrantDigest,
    pub source_schema_generation: u64,
    pub recipient_schema_generation: u64,
    pub package_sequence: u64,
    pub previous_package_digest: Option<GrantDigest>,
    pub issued_at: i64,
    pub expires_at: i64,
    pub total_plaintext_bytes: u64,
    pub total_ciphertext_bytes: u64,
    pub objects: Vec<EncryptedGrantObject>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedGrantPackage {
    pub statement: GrantPackageStatement,
    pub exporter_key_id: String,
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectAad<'a> {
    grant_digest: &'a GrantDigest,
    package_id: &'a GrantId,
    package_sequence: u64,
    recipient_key_certificate_digest: &'a GrantDigest,
    object: &'a GrantObjectRef,
    plaintext_digest: &'a GrantDigest,
    plaintext_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectWrapAad<'a> {
    payload_aad_digest: GrantDigest,
    ciphertext_digest: &'a GrantDigest,
    nonce: &'a [u8],
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct HpkeInfo<'a> {
    grant_digest: &'a GrantDigest,
    package_id: &'a GrantId,
    recipient_cell_id: &'a GrantId,
    recipient_key_id: &'a str,
}

fn hpke_seal(
    public_key: &str,
    info: &[u8],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<(Vec<u8>, Vec<u8>)> {
    let public_key = hpke_public_key(public_key, "HPKE recipient public_key")?;
    let (encapped, mut sender) =
        hpke::setup_sender::<HpkeAead, HpkeKdf, HpkeKem>(&OpModeS::Base, &public_key, info)
            .map_err(|_| GrantError::Cryptography("HPKE sender setup failed".to_string()))?;
    let ciphertext = sender
        .seal(plaintext, aad)
        .map_err(|_| GrantError::Cryptography("HPKE seal failed".to_string()))?;
    Ok((encapped.to_bytes().as_slice().to_vec(), ciphertext))
}

fn hpke_open(
    private_key: &[u8],
    encapsulated_key: &[u8],
    info: &[u8],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let private = <HpkeKem as KemTrait>::PrivateKey::from_bytes(private_key)
        .map_err(|_| GrantError::Cryptography("invalid HPKE private key".to_string()))?;
    let encapped = <HpkeKem as KemTrait>::EncappedKey::from_bytes(encapsulated_key)
        .map_err(|_| GrantError::Cryptography("invalid HPKE encapsulated key".to_string()))?;
    let mut receiver = hpke::setup_receiver::<HpkeAead, HpkeKdf, HpkeKem>(
        &OpModeR::Base,
        &private,
        &encapped,
        info,
    )
    .map_err(|_| GrantError::Cryptography("HPKE receiver setup failed".to_string()))?;
    receiver
        .open(ciphertext, aad)
        .map(Zeroizing::new)
        .map_err(|_| GrantError::Cryptography("HPKE open failed".to_string()))
}

pub struct GrantExporter {
    cell_id: GrantId,
    key_id: String,
    signing_key: SigningKey,
}

impl std::fmt::Debug for GrantExporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrantExporter")
            .field("cell_id", &self.cell_id)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl GrantExporter {
    pub fn new(
        policy: &GrantTrustPolicy,
        cell_id: GrantId,
        key_id: impl Into<String>,
        signing_key: SigningKey,
    ) -> Result<Self> {
        policy.validate()?;
        let key_id = key_id.into();
        if policy.cell_id != cell_id
            || policy.export_key(&key_id)?.to_bytes() != signing_key.verifying_key().to_bytes()
        {
            return Err(GrantError::Scope(
                "export signer does not match the exact Cell trust policy".to_string(),
            ));
        }
        Ok(Self {
            cell_id,
            key_id,
            signing_key,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn export(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        package_id: GrantId,
        package_sequence: u64,
        previous_package_digest: Option<GrantDigest>,
        issued_at: i64,
        expires_at: i64,
        mut objects: Vec<GrantPlaintextObject>,
    ) -> Result<SignedGrantPackage> {
        let grant_digest = verify_grant(
            source_policy,
            recipient_policy,
            recipient_key,
            grant,
            issued_at,
        )?;
        if self.cell_id != source_policy.cell_id
            || package_sequence == 0
            || (package_sequence == 1) != previous_package_digest.is_none()
            || issued_at < grant.statement.valid_from
            || expires_at < issued_at
            || expires_at > grant.statement.expires_at
            || objects.is_empty()
        {
            return Err(GrantError::Scope(
                "package identity, sequence, time, or source scope is invalid".to_string(),
            ));
        }
        objects.sort_by(|left, right| left.object.cmp(&right.object));
        if objects
            .windows(2)
            .any(|pair| pair[0].object >= pair[1].object)
        {
            return Err(GrantError::Scope(
                "package objects must be unique exact references".to_string(),
            ));
        }
        let mut total_plaintext_bytes = 0_u64;
        let mut total_ciphertext_bytes = 0_u64;
        let mut encrypted_objects = Vec::with_capacity(objects.len());
        let recipient_key_digest = document_digest(&recipient_key.statement)?;
        let hpke_info = domain_document(
            HPKE_INFO_DOMAIN,
            &HpkeInfo {
                grant_digest: &grant_digest,
                package_id: &package_id,
                recipient_cell_id: &grant.statement.recipient_cell_id,
                recipient_key_id: &recipient_key.statement.key_id,
            },
        )?;
        for object in objects {
            let GrantPlaintextObject { object, bytes } = object;
            let bytes = Zeroizing::new(bytes);
            if !grant.statement.exact_objects.contains(&object) {
                return Err(GrantError::Scope(
                    "package contains an object outside the exact grant".to_string(),
                ));
            }
            let plaintext_bytes = u64::try_from(bytes.len())
                .map_err(|_| GrantError::Limit("object length overflow".to_string()))?;
            if plaintext_bytes == 0 || plaintext_bytes > grant.statement.maximum_object_bytes {
                return Err(GrantError::Limit(
                    "object is empty or exceeds the grant bound".to_string(),
                ));
            }
            total_plaintext_bytes = total_plaintext_bytes
                .checked_add(plaintext_bytes)
                .ok_or_else(|| GrantError::Limit("package byte count overflow".to_string()))?;
            if total_plaintext_bytes > grant.statement.maximum_total_bytes {
                return Err(GrantError::Limit(
                    "package plaintext exceeds the grant bound".to_string(),
                ));
            }
            let plaintext_digest = GrantDigest::of_bytes(bytes.as_ref());
            let payload_aad = domain_document(
                OBJECT_PAYLOAD_AAD_DOMAIN,
                &ObjectAad {
                    grant_digest: &grant_digest,
                    package_id: &package_id,
                    package_sequence,
                    recipient_key_certificate_digest: &recipient_key_digest,
                    object: &object,
                    plaintext_digest: &plaintext_digest,
                    plaintext_bytes,
                },
            )?;
            let mut dek = Zeroizing::new([0_u8; 32]);
            getrandom::fill(dek.as_mut()).map_err(|error| {
                GrantError::Cryptography(format!("generate object DEK: {error}"))
            })?;
            let mut nonce = [0_u8; 24];
            getrandom::fill(&mut nonce).map_err(|error| {
                GrantError::Cryptography(format!("generate object nonce: {error}"))
            })?;
            let cipher = XChaCha20Poly1305::new(Key::from_slice(dek.as_ref()));
            let ciphertext = cipher
                .encrypt(
                    XNonce::from_slice(&nonce),
                    chacha20poly1305::aead::Payload {
                        msg: bytes.as_ref(),
                        aad: &payload_aad,
                    },
                )
                .map_err(|_| GrantError::Cryptography("object encryption failed".to_string()))?;
            let ciphertext_digest = GrantDigest::of_bytes(&ciphertext);
            let wrap_aad = domain_document(
                OBJECT_WRAP_AAD_DOMAIN,
                &ObjectWrapAad {
                    payload_aad_digest: GrantDigest::of_bytes(&payload_aad),
                    ciphertext_digest: &ciphertext_digest,
                    nonce: &nonce,
                },
            )?;
            let (encapsulated_key, wrapped_ciphertext) = hpke_seal(
                &recipient_key.statement.public_key,
                &hpke_info,
                &wrap_aad,
                dek.as_ref(),
            )?;
            total_ciphertext_bytes = total_ciphertext_bytes
                .checked_add(u64::try_from(ciphertext.len()).unwrap_or(u64::MAX))
                .and_then(|sum| {
                    sum.checked_add(u64::try_from(wrapped_ciphertext.len()).unwrap_or(u64::MAX))
                })
                .ok_or_else(|| GrantError::Limit("ciphertext byte count overflow".to_string()))?;
            encrypted_objects.push(EncryptedGrantObject {
                object,
                plaintext_digest,
                plaintext_bytes,
                nonce: nonce.to_vec(),
                ciphertext_digest,
                ciphertext,
                wrapped_dek: HpkeWrappedObjectKey {
                    suite: HPKE_SUITE.to_string(),
                    recipient_key_id: recipient_key.statement.key_id.clone(),
                    encapsulated_key,
                    ciphertext: wrapped_ciphertext,
                },
            });
        }
        if total_ciphertext_bytes
            > grant
                .statement
                .maximum_total_bytes
                .saturating_add(u64::try_from(encrypted_objects.len()).unwrap_or(u64::MAX) * 256)
        {
            return Err(GrantError::Limit(
                "package ciphertext overhead exceeds its bounded allowance".to_string(),
            ));
        }
        let statement = GrantPackageStatement {
            format: GRANT_PACKAGE_FORMAT.to_string(),
            package_id,
            grant_id: grant.statement.grant_id.clone(),
            grant_epoch: grant.statement.grant_epoch,
            grant_digest,
            source_cell_id: grant.statement.source_cell_id.clone(),
            recipient_cell_id: grant.statement.recipient_cell_id.clone(),
            source_application_digest: grant.statement.source_application_digest.clone(),
            recipient_application_digest: grant.statement.recipient_application_digest.clone(),
            source_schema_generation: grant.statement.source_schema_generation,
            recipient_schema_generation: grant.statement.recipient_schema_generation,
            package_sequence,
            previous_package_digest,
            issued_at,
            expires_at,
            total_plaintext_bytes,
            total_ciphertext_bytes,
            objects: encrypted_objects,
        };
        let signature = self
            .signing_key
            .sign(&signing_message(PACKAGE_DOMAIN, &statement)?)
            .to_bytes()
            .to_vec();
        let package = SignedGrantPackage {
            statement,
            exporter_key_id: self.key_id.clone(),
            signature,
        };
        if canonical_cbor(&package)?.len() > MAX_DOCUMENT_BYTES {
            return Err(GrantError::Limit(
                "encoded package exceeds the absolute parser bound".to_string(),
            ));
        }
        Ok(package)
    }
}

pub fn verify_package(
    source_policy: &GrantTrustPolicy,
    recipient_policy: &GrantTrustPolicy,
    recipient_key: &CertifiedRecipientKey,
    grant: &CertifiedCrossCellGrant,
    package: &SignedGrantPackage,
    now: i64,
) -> Result<GrantDigest> {
    if canonical_cbor(package)?.len() > MAX_DOCUMENT_BYTES {
        return Err(GrantError::Limit(
            "encoded package exceeds the absolute parser bound".to_string(),
        ));
    }
    let grant_digest = verify_grant(source_policy, recipient_policy, recipient_key, grant, now)?;
    let statement = &package.statement;
    if statement.format != GRANT_PACKAGE_FORMAT
        || statement.grant_id != grant.statement.grant_id
        || statement.grant_epoch != grant.statement.grant_epoch
        || statement.grant_digest != grant_digest
        || statement.source_cell_id != grant.statement.source_cell_id
        || statement.recipient_cell_id != grant.statement.recipient_cell_id
        || statement.source_application_digest != grant.statement.source_application_digest
        || statement.recipient_application_digest != grant.statement.recipient_application_digest
        || statement.source_schema_generation != grant.statement.source_schema_generation
        || statement.recipient_schema_generation != grant.statement.recipient_schema_generation
        || statement.package_sequence == 0
        || (statement.package_sequence == 1) != statement.previous_package_digest.is_none()
        || statement.issued_at < grant.statement.valid_from
        || statement.expires_at < statement.issued_at
        || statement.expires_at > grant.statement.expires_at
        || statement.objects.is_empty()
        || statement.objects.len() > grant.statement.exact_objects.len()
    {
        return Err(GrantError::Scope(
            "package is not bound to the exact active grant and applications".to_string(),
        ));
    }
    if now < statement.issued_at || now > statement.expires_at {
        return Err(GrantError::Expired(
            "package is future-dated or expired".to_string(),
        ));
    }
    let mut previous = None;
    let mut plaintext_total = 0_u64;
    let mut ciphertext_total = 0_u64;
    for object in &statement.objects {
        if !grant.statement.exact_objects.contains(&object.object)
            || previous
                .as_ref()
                .is_some_and(|value| value >= &object.object)
            || object.plaintext_bytes == 0
            || object.plaintext_bytes > grant.statement.maximum_object_bytes
            || object.nonce.len() != 24
            || object.ciphertext.is_empty()
            || object.ciphertext_digest != GrantDigest::of_bytes(&object.ciphertext)
            || object.wrapped_dek.suite != HPKE_SUITE
            || object.wrapped_dek.recipient_key_id != recipient_key.statement.key_id
            || object.wrapped_dek.encapsulated_key.len() != 32
            || object.wrapped_dek.ciphertext.is_empty()
        {
            return Err(GrantError::Scope(
                "package object order, scope, size, digest, nonce, or recipient envelope is invalid"
                    .to_string(),
            ));
        }
        plaintext_total = plaintext_total
            .checked_add(object.plaintext_bytes)
            .ok_or_else(|| GrantError::Limit("plaintext total overflow".to_string()))?;
        ciphertext_total = ciphertext_total
            .checked_add(u64::try_from(object.ciphertext.len()).unwrap_or(u64::MAX))
            .and_then(|sum| {
                sum.checked_add(
                    u64::try_from(object.wrapped_dek.ciphertext.len()).unwrap_or(u64::MAX),
                )
            })
            .ok_or_else(|| GrantError::Limit("ciphertext total overflow".to_string()))?;
        previous = Some(object.object.clone());
    }
    if plaintext_total != statement.total_plaintext_bytes
        || ciphertext_total != statement.total_ciphertext_bytes
        || plaintext_total > grant.statement.maximum_total_bytes
        || ciphertext_total
            > grant
                .statement
                .maximum_total_bytes
                .saturating_add(u64::try_from(statement.objects.len()).unwrap_or(u64::MAX) * 256)
    {
        return Err(GrantError::Limit(
            "package aggregate byte accounting is invalid".to_string(),
        ));
    }
    let signature = Signature::from_slice(&package.signature)
        .map_err(|_| GrantError::Signature("malformed package signature".to_string()))?;
    source_policy
        .export_key(&package.exporter_key_id)?
        .verify_strict(&signing_message(PACKAGE_DOMAIN, statement)?, &signature)
        .map_err(|_| GrantError::Signature("package signature failed".to_string()))?;
    document_digest(statement)
}

pub trait RecipientPrivateKey: Send + Sync {
    fn key_id(&self) -> &str;
    fn public_key(&self) -> &str;
    fn open(
        &self,
        envelope: &HpkeWrappedObjectKey,
        info: &[u8],
        aad: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>>;
}

pub struct SoftwareRecipientPrivateKey {
    key_id: String,
    public_key: String,
    private_key: Zeroizing<Vec<u8>>,
}

impl std::fmt::Debug for SoftwareRecipientPrivateKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SoftwareRecipientPrivateKey")
            .field("key_id", &self.key_id)
            .field("public_key", &self.public_key)
            .finish_non_exhaustive()
    }
}

impl SoftwareRecipientPrivateKey {
    pub fn from_bytes(key_id: impl Into<String>, bytes: Vec<u8>) -> Result<Self> {
        let key_id = key_id.into();
        require_label(&key_id, "recipient private key_id")?;
        let private = <HpkeKem as KemTrait>::PrivateKey::from_bytes(&bytes)
            .map_err(|_| GrantError::Cryptography("invalid HPKE private key".to_string()))?;
        let public = HpkeKem::sk_to_pk(&private);
        Ok(Self {
            key_id,
            public_key: hex::encode(public.to_bytes().as_slice()),
            private_key: Zeroizing::new(bytes),
        })
    }

    pub fn generate(key_id: impl Into<String>) -> Result<Self> {
        let (private, _) = HpkeKem::gen_keypair();
        Self::from_bytes(key_id, private.to_bytes().as_slice().to_vec())
    }

    pub fn private_key_bytes_for_provisioning(&self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(self.private_key.to_vec())
    }
}

impl RecipientPrivateKey for SoftwareRecipientPrivateKey {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn public_key(&self) -> &str {
        &self.public_key
    }

    fn open(
        &self,
        envelope: &HpkeWrappedObjectKey,
        info: &[u8],
        aad: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        if envelope.recipient_key_id != self.key_id || envelope.suite != HPKE_SUITE {
            return Err(GrantError::Scope(
                "HPKE envelope is for another recipient key or suite".to_string(),
            ));
        }
        hpke_open(
            self.private_key.as_ref(),
            &envelope.encapsulated_key,
            info,
            aad,
            &envelope.ciphertext,
        )
    }
}

fn decrypt_package_objects(
    recipient_key_certificate: &CertifiedRecipientKey,
    package: &SignedGrantPackage,
    recipient_private_key: &dyn RecipientPrivateKey,
) -> Result<Vec<(EncryptedGrantObject, Zeroizing<Vec<u8>>)>> {
    if recipient_private_key.key_id() != recipient_key_certificate.statement.key_id
        || recipient_private_key.public_key() != recipient_key_certificate.statement.public_key
    {
        return Err(GrantError::Scope(
            "recipient private key does not match the certified public key".to_string(),
        ));
    }
    let recipient_key_digest = document_digest(&recipient_key_certificate.statement)?;
    let hpke_info = domain_document(
        HPKE_INFO_DOMAIN,
        &HpkeInfo {
            grant_digest: &package.statement.grant_digest,
            package_id: &package.statement.package_id,
            recipient_cell_id: &package.statement.recipient_cell_id,
            recipient_key_id: &recipient_key_certificate.statement.key_id,
        },
    )?;
    let mut opened = Vec::with_capacity(package.statement.objects.len());
    for encrypted in &package.statement.objects {
        let payload_aad = domain_document(
            OBJECT_PAYLOAD_AAD_DOMAIN,
            &ObjectAad {
                grant_digest: &package.statement.grant_digest,
                package_id: &package.statement.package_id,
                package_sequence: package.statement.package_sequence,
                recipient_key_certificate_digest: &recipient_key_digest,
                object: &encrypted.object,
                plaintext_digest: &encrypted.plaintext_digest,
                plaintext_bytes: encrypted.plaintext_bytes,
            },
        )?;
        let wrap_aad = domain_document(
            OBJECT_WRAP_AAD_DOMAIN,
            &ObjectWrapAad {
                payload_aad_digest: GrantDigest::of_bytes(&payload_aad),
                ciphertext_digest: &encrypted.ciphertext_digest,
                nonce: &encrypted.nonce,
            },
        )?;
        let mut dek = recipient_private_key.open(&encrypted.wrapped_dek, &hpke_info, &wrap_aad)?;
        if dek.len() != 32 {
            return Err(GrantError::Cryptography(
                "unwrapped object DEK has the wrong length".to_string(),
            ));
        }
        let cipher = XChaCha20Poly1305::new(Key::from_slice(dek.as_ref()));
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&encrypted.nonce),
                chacha20poly1305::aead::Payload {
                    msg: &encrypted.ciphertext,
                    aad: &payload_aad,
                },
            )
            .map(Zeroizing::new)
            .map_err(|_| GrantError::Cryptography("object decryption failed".to_string()))?;
        dek.zeroize();
        if u64::try_from(plaintext.len()).unwrap_or(u64::MAX) != encrypted.plaintext_bytes
            || GrantDigest::of_bytes(plaintext.as_ref()) != encrypted.plaintext_digest
        {
            return Err(GrantError::Cryptography(
                "decrypted object length or digest mismatch".to_string(),
            ));
        }
        opened.push((encrypted.clone(), plaintext));
    }
    Ok(opened)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ImportReviewStatement {
    pub format: String,
    pub review_id: GrantId,
    pub recipient_policy_digest: GrantDigest,
    pub recipient_cell_id: GrantId,
    pub grant_digest: GrantDigest,
    pub package_digest: GrantDigest,
    pub content_policy_digest: GrantDigest,
    pub scanner_profile_digest: GrantDigest,
    pub scanner_result_digest: GrantDigest,
    pub reviewed_at: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedImportReview {
    pub statement: ImportReviewStatement,
    pub approvals: Vec<GrantAuthorityApproval>,
}

pub fn verify_import_review(
    recipient_policy: &GrantTrustPolicy,
    grant_digest: &GrantDigest,
    package_digest: &GrantDigest,
    review: &CertifiedImportReview,
    now: i64,
) -> Result<GrantDigest> {
    let statement = &review.statement;
    if statement.format != IMPORT_REVIEW_FORMAT
        || statement.recipient_policy_digest != document_digest(recipient_policy)?
        || statement.recipient_cell_id != recipient_policy.cell_id
        || &statement.grant_digest != grant_digest
        || &statement.package_digest != package_digest
        || statement.reviewed_at <= 0
        || statement.expires_at < statement.reviewed_at
        || now < statement.reviewed_at
        || now > statement.expires_at
    {
        return Err(GrantError::Scope(
            "import review is not current or bound to the exact Cell, grant, package, and policy"
                .to_string(),
        ));
    }
    verify_approvals(
        recipient_policy,
        GrantAuthorityRole::ImportAcceptance,
        IMPORT_REVIEW_DOMAIN,
        statement,
        &review.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantRevocationStatement {
    pub format: String,
    pub revocation_id: GrantId,
    pub source_policy_digest: GrantDigest,
    pub grant_id: GrantId,
    pub grant_epoch: u64,
    pub grant_digest: GrantDigest,
    pub source_cell_id: GrantId,
    pub recipient_cell_id: GrantId,
    pub reason_digest: GrantDigest,
    pub revoked_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedGrantRevocation {
    pub statement: GrantRevocationStatement,
    pub approvals: Vec<GrantAuthorityApproval>,
}

pub fn verify_revocation(
    source_policy: &GrantTrustPolicy,
    grant: &CertifiedCrossCellGrant,
    revocation: &CertifiedGrantRevocation,
    now: i64,
) -> Result<GrantDigest> {
    let statement = &revocation.statement;
    let grant_digest = document_digest(&grant.statement)?;
    if statement.format != GRANT_REVOCATION_FORMAT
        || statement.source_policy_digest != document_digest(source_policy)?
        || statement.grant_id != grant.statement.grant_id
        || statement.grant_epoch != grant.statement.grant_epoch
        || statement.grant_digest != grant_digest
        || statement.source_cell_id != grant.statement.source_cell_id
        || statement.recipient_cell_id != grant.statement.recipient_cell_id
        || statement.revoked_at < grant.statement.issued_at
        || statement.revoked_at > now
    {
        return Err(GrantError::Scope(
            "revocation is future-dated or outside the exact grant".to_string(),
        ));
    }
    verify_approvals(
        source_policy,
        GrantAuthorityRole::Revocation,
        REVOCATION_DOMAIN,
        statement,
        &revocation.approvals,
    )?;
    document_digest(statement)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ImportedGrantEvidence {
    pub format: String,
    pub evidence_id: GrantDigest,
    pub source_cell_id: GrantId,
    pub recipient_cell_id: GrantId,
    pub source_application_digest: GrantDigest,
    pub recipient_application_digest: GrantDigest,
    pub source_schema_generation: u64,
    pub recipient_schema_generation: u64,
    pub grant_id: GrantId,
    pub grant_epoch: u64,
    pub grant_digest: GrantDigest,
    pub package_id: GrantId,
    pub package_sequence: u64,
    pub package_digest: GrantDigest,
    pub import_review_digest: GrantDigest,
    pub object: GrantObjectRef,
    pub source_plaintext_digest: GrantDigest,
    pub imported_at: i64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct GrantLedgerEntry {
    active_epoch: u64,
    active_grant_digest: GrantDigest,
    last_package_sequence: u64,
    last_package_digest: Option<GrantDigest>,
    package_count: u32,
    total_plaintext_bytes: u64,
    revocation_digest: Option<GrantDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceLedgerState {
    format: String,
    cell_id: GrantId,
    grants: BTreeMap<GrantId, GrantLedgerEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RecipientLedgerState {
    format: String,
    cell_id: GrantId,
    grants: BTreeMap<GrantId, GrantLedgerEntry>,
    imported_package_digests: BTreeSet<GrantDigest>,
}

fn load_state<T: Serialize + DeserializeOwned>(
    database: &mut BicDb,
    collection: &str,
    record_id: &str,
) -> Result<Option<T>> {
    database.create_collection(collection)?;
    database
        .get(collection, record_id)?
        .map(|record| {
            let payload = record.payload.as_ref().ok_or_else(|| {
                GrantError::Storage(format!("{collection} state record has no payload"))
            })?;
            decode_document(payload)
        })
        .transpose()
}

fn state_record<T: Serialize>(record_id: &str, state: &T) -> Result<Record> {
    Ok(Record::new(record_id).with_payload(canonical_cbor(state)?))
}

fn activate_entry(
    entries: &mut BTreeMap<GrantId, GrantLedgerEntry>,
    grant: &CertifiedCrossCellGrant,
    digest: GrantDigest,
) -> Result<()> {
    match entries.get_mut(&grant.statement.grant_id) {
        None if grant.statement.grant_epoch == 1
            && grant.statement.previous_grant_digest.is_none() =>
        {
            entries.insert(
                grant.statement.grant_id.clone(),
                GrantLedgerEntry {
                    active_epoch: 1,
                    active_grant_digest: digest,
                    last_package_sequence: 0,
                    last_package_digest: None,
                    package_count: 0,
                    total_plaintext_bytes: 0,
                    revocation_digest: None,
                },
            );
            Ok(())
        }
        Some(current)
            if current.revocation_digest.is_none()
                && grant.statement.grant_epoch == current.active_epoch.saturating_add(1)
                && grant.statement.previous_grant_digest.as_ref()
                    == Some(&current.active_grant_digest) =>
        {
            current.active_epoch = grant.statement.grant_epoch;
            current.active_grant_digest = digest;
            Ok(())
        }
        _ => Err(GrantError::Replay(
            "grant activation must be the initial epoch or exact next predecessor".to_string(),
        )),
    }
}

fn record_package_entry(
    entry: &mut GrantLedgerEntry,
    grant: &CertifiedCrossCellGrant,
    package: &SignedGrantPackage,
    digest: GrantDigest,
) -> Result<()> {
    if entry.revocation_digest.is_some() {
        return Err(GrantError::Revoked("grant is revoked".to_string()));
    }
    if entry.active_epoch != grant.statement.grant_epoch
        || entry.active_grant_digest != package.statement.grant_digest
        || package.statement.package_sequence != entry.last_package_sequence.saturating_add(1)
        || package.statement.previous_package_digest != entry.last_package_digest
        || entry.package_count >= grant.statement.maximum_packages
        || entry
            .total_plaintext_bytes
            .saturating_add(package.statement.total_plaintext_bytes)
            > grant
                .statement
                .maximum_total_bytes
                .saturating_mul(u64::from(grant.statement.maximum_packages))
    {
        return Err(GrantError::Replay(
            "package does not advance the exact active grant chain within its cumulative bounds"
                .to_string(),
        ));
    }
    entry.last_package_sequence = package.statement.package_sequence;
    entry.last_package_digest = Some(digest);
    entry.package_count = entry.package_count.saturating_add(1);
    entry.total_plaintext_bytes = entry
        .total_plaintext_bytes
        .saturating_add(package.statement.total_plaintext_bytes);
    Ok(())
}

pub struct GrantSourceLedger {
    database: Arc<RwLock<BicDb>>,
    state: RwLock<SourceLedgerState>,
}

impl GrantSourceLedger {
    pub fn open_cell_database(database: Arc<RwLock<BicDb>>, cell_id: GrantId) -> Result<Self> {
        let state = {
            let mut database = database.write();
            load_state::<SourceLedgerState>(
                &mut database,
                SOURCE_STATE_COLLECTION,
                SOURCE_STATE_RECORD_ID,
            )?
            .unwrap_or(SourceLedgerState {
                format: SOURCE_LEDGER_FORMAT.to_string(),
                cell_id: cell_id.clone(),
                grants: BTreeMap::new(),
            })
        };
        if state.format != SOURCE_LEDGER_FORMAT || state.cell_id != cell_id {
            return Err(GrantError::Scope(
                "source grant ledger belongs to another Cell".to_string(),
            ));
        }
        Ok(Self {
            database,
            state: RwLock::new(state),
        })
    }

    fn persist(&self, state: &SourceLedgerState) -> Result<()> {
        let mut database = self.database.write();
        database.insert(
            SOURCE_STATE_COLLECTION,
            state_record(SOURCE_STATE_RECORD_ID, state)?,
        )?;
        database.flush()?;
        Ok(())
    }

    pub fn activate_grant(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        now: i64,
    ) -> Result<GrantDigest> {
        let digest = verify_grant(source_policy, recipient_policy, recipient_key, grant, now)?;
        if self.state.read().cell_id != source_policy.cell_id {
            return Err(GrantError::Scope(
                "source ledger is bound to another Cell".to_string(),
            ));
        }
        let mut state = self.state.write();
        let mut next = state.clone();
        activate_entry(&mut next.grants, grant, digest.clone())?;
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }

    pub fn record_package(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        package: &SignedGrantPackage,
        now: i64,
    ) -> Result<GrantDigest> {
        let digest = verify_package(
            source_policy,
            recipient_policy,
            recipient_key,
            grant,
            package,
            now,
        )?;
        let mut state = self.state.write();
        let mut next = state.clone();
        let entry = next
            .grants
            .get_mut(&grant.statement.grant_id)
            .ok_or_else(|| GrantError::Scope("grant is not active at source".to_string()))?;
        record_package_entry(entry, grant, package, digest.clone())?;
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }

    pub fn record_revocation(
        &self,
        source_policy: &GrantTrustPolicy,
        grant: &CertifiedCrossCellGrant,
        revocation: &CertifiedGrantRevocation,
        now: i64,
    ) -> Result<GrantDigest> {
        let digest = verify_revocation(source_policy, grant, revocation, now)?;
        let mut state = self.state.write();
        let mut next = state.clone();
        let entry = next
            .grants
            .get_mut(&grant.statement.grant_id)
            .ok_or_else(|| GrantError::Scope("grant is not active at source".to_string()))?;
        if entry.active_grant_digest != revocation.statement.grant_digest
            || entry.revocation_digest.is_some()
        {
            return Err(GrantError::Replay(
                "revocation does not match the active unrevoked grant".to_string(),
            ));
        }
        entry.revocation_digest = Some(digest.clone());
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }
}

pub struct GrantRecipientLedger {
    database: Arc<RwLock<BicDb>>,
    state: RwLock<RecipientLedgerState>,
}

impl GrantRecipientLedger {
    pub fn open_cell_database(database: Arc<RwLock<BicDb>>, cell_id: GrantId) -> Result<Self> {
        let state = {
            let mut database = database.write();
            database.create_collection(IMPORTED_EVIDENCE_COLLECTION)?;
            load_state::<RecipientLedgerState>(
                &mut database,
                RECIPIENT_STATE_COLLECTION,
                RECIPIENT_STATE_RECORD_ID,
            )?
            .unwrap_or(RecipientLedgerState {
                format: RECIPIENT_LEDGER_FORMAT.to_string(),
                cell_id: cell_id.clone(),
                grants: BTreeMap::new(),
                imported_package_digests: BTreeSet::new(),
            })
        };
        if state.format != RECIPIENT_LEDGER_FORMAT || state.cell_id != cell_id {
            return Err(GrantError::Scope(
                "recipient grant ledger belongs to another Cell".to_string(),
            ));
        }
        Ok(Self {
            database,
            state: RwLock::new(state),
        })
    }

    fn persist(&self, state: &RecipientLedgerState) -> Result<()> {
        let mut database = self.database.write();
        database.insert(
            RECIPIENT_STATE_COLLECTION,
            state_record(RECIPIENT_STATE_RECORD_ID, state)?,
        )?;
        database.flush()?;
        Ok(())
    }

    pub fn accept_grant(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_policy: &GrantTrustPolicy,
        recipient_key: &CertifiedRecipientKey,
        grant: &CertifiedCrossCellGrant,
        now: i64,
    ) -> Result<GrantDigest> {
        let digest = verify_grant(source_policy, recipient_policy, recipient_key, grant, now)?;
        if self.state.read().cell_id != recipient_policy.cell_id {
            return Err(GrantError::Scope(
                "recipient ledger is bound to another Cell".to_string(),
            ));
        }
        let mut state = self.state.write();
        let mut next = state.clone();
        activate_entry(&mut next.grants, grant, digest.clone())?;
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn import_package(
        &self,
        source_policy: &GrantTrustPolicy,
        recipient_policy: &GrantTrustPolicy,
        recipient_key_certificate: &CertifiedRecipientKey,
        recipient_private_key: &dyn RecipientPrivateKey,
        grant: &CertifiedCrossCellGrant,
        package: &SignedGrantPackage,
        review: &CertifiedImportReview,
        now: i64,
    ) -> Result<Vec<ImportedGrantEvidence>> {
        let package_digest = verify_package(
            source_policy,
            recipient_policy,
            recipient_key_certificate,
            grant,
            package,
            now,
        )?;
        let review_digest = verify_import_review(
            recipient_policy,
            &package.statement.grant_digest,
            &package_digest,
            review,
            now,
        )?;
        let opened =
            decrypt_package_objects(recipient_key_certificate, package, recipient_private_key)?;
        let mut state = self.state.write();
        let mut next = state.clone();
        if !next.imported_package_digests.insert(package_digest.clone()) {
            return Err(GrantError::Replay(
                "package digest was already imported".to_string(),
            ));
        }
        let entry = next
            .grants
            .get_mut(&grant.statement.grant_id)
            .ok_or_else(|| GrantError::Scope("grant is not accepted at recipient".to_string()))?;
        record_package_entry(entry, grant, package, package_digest.clone())?;
        let evidence = opened
            .into_iter()
            .map(|(encrypted, plaintext)| {
                let evidence_id = document_digest(&(
                    &package_digest,
                    &encrypted.object,
                    &encrypted.plaintext_digest,
                ))?;
                Ok(ImportedGrantEvidence {
                    format: IMPORTED_EVIDENCE_FORMAT.to_string(),
                    evidence_id,
                    source_cell_id: package.statement.source_cell_id.clone(),
                    recipient_cell_id: package.statement.recipient_cell_id.clone(),
                    source_application_digest: package.statement.source_application_digest.clone(),
                    recipient_application_digest: package
                        .statement
                        .recipient_application_digest
                        .clone(),
                    source_schema_generation: package.statement.source_schema_generation,
                    recipient_schema_generation: package.statement.recipient_schema_generation,
                    grant_id: package.statement.grant_id.clone(),
                    grant_epoch: package.statement.grant_epoch,
                    grant_digest: package.statement.grant_digest.clone(),
                    package_id: package.statement.package_id.clone(),
                    package_sequence: package.statement.package_sequence,
                    package_digest: package_digest.clone(),
                    import_review_digest: review_digest.clone(),
                    object: encrypted.object,
                    source_plaintext_digest: encrypted.plaintext_digest,
                    imported_at: now,
                    bytes: plaintext.to_vec(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let database = self.database.write();
        let mut transaction = database.begin_transaction()?;
        for item in &evidence {
            transaction.insert(
                IMPORTED_EVIDENCE_COLLECTION,
                Record::new(item.evidence_id.as_str()).with_payload(canonical_cbor(item)?),
            )?;
        }
        transaction.insert(
            RECIPIENT_STATE_COLLECTION,
            state_record(RECIPIENT_STATE_RECORD_ID, &next)?,
        )?;
        transaction.commit()?;
        database.flush()?;
        drop(database);
        *state = next;
        Ok(evidence)
    }

    pub fn record_revocation(
        &self,
        source_policy: &GrantTrustPolicy,
        grant: &CertifiedCrossCellGrant,
        revocation: &CertifiedGrantRevocation,
        now: i64,
    ) -> Result<GrantDigest> {
        let digest = verify_revocation(source_policy, grant, revocation, now)?;
        let mut state = self.state.write();
        let mut next = state.clone();
        let entry = next
            .grants
            .get_mut(&grant.statement.grant_id)
            .ok_or_else(|| GrantError::Scope("grant is not accepted at recipient".to_string()))?;
        if entry.active_grant_digest != revocation.statement.grant_digest
            || entry.revocation_digest.is_some()
        {
            return Err(GrantError::Replay(
                "revocation does not match the active unrevoked grant".to_string(),
            ));
        }
        entry.revocation_digest = Some(digest.clone());
        self.persist(&next)?;
        *state = next;
        Ok(digest)
    }

    pub fn imported_evidence(&self, evidence_id: &GrantDigest) -> Result<ImportedGrantEvidence> {
        let database = self.database.read();
        let record = database
            .get(IMPORTED_EVIDENCE_COLLECTION, evidence_id.as_str())?
            .ok_or_else(|| GrantError::Scope("imported evidence does not exist".to_string()))?;
        let payload = record.payload.as_ref().ok_or_else(|| {
            GrantError::Storage("imported evidence record has no payload".to_string())
        })?;
        decode_document(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, EncryptionBinding, EncryptionConfig};
    use std::fs;
    use std::path::Path;

    const NOW: i64 = 1_800_000_000;

    fn id(value: u128) -> GrantId {
        GrantId::parse(Uuid::from_u128(value).hyphenated().to_string(), "test id").unwrap()
    }

    fn digest(value: &str) -> GrantDigest {
        GrantDigest::of_bytes(value.as_bytes())
    }

    fn authority_keys(seed: u8) -> Vec<SigningKey> {
        (0..8)
            .map(|offset| SigningKey::from_bytes(&[seed.saturating_add(offset); 32]))
            .collect()
    }

    fn make_policy(
        cell_id: GrantId,
        policy_id: &str,
        authorities: &[SigningKey],
        exporter: &SigningKey,
        recipient: &SoftwareRecipientPrivateKey,
    ) -> GrantTrustPolicy {
        let roles = [
            GrantAuthorityRole::Issue,
            GrantAuthorityRole::Issue,
            GrantAuthorityRole::Revocation,
            GrantAuthorityRole::Revocation,
            GrantAuthorityRole::RecipientKey,
            GrantAuthorityRole::RecipientKey,
            GrantAuthorityRole::ImportAcceptance,
            GrantAuthorityRole::ImportAcceptance,
        ];
        GrantTrustPolicy {
            format: GRANT_TRUST_POLICY_FORMAT.to_string(),
            policy_id: policy_id.to_string(),
            cell_id,
            authorities: authorities
                .iter()
                .zip(roles)
                .enumerate()
                .map(|(index, (key, role))| GrantAuthorityKey {
                    key_id: format!("authority-{index}"),
                    role,
                    public_key: hex::encode(key.verifying_key().as_bytes()),
                })
                .collect(),
            issue_threshold: 2,
            revocation_threshold: 2,
            recipient_key_threshold: 2,
            import_acceptance_threshold: 2,
            export_keys: vec![GrantExportKey {
                key_id: "export-1".to_string(),
                public_key: hex::encode(exporter.verifying_key().as_bytes()),
            }],
            recipient_encryption_keys: vec![RecipientEncryptionKey {
                key_id: recipient.key_id().to_string(),
                public_key: recipient.public_key().to_string(),
                key_epoch: 1,
                hardware_attestation_digest: digest(&format!("{policy_id}-attestation")),
            }],
            trusted_remote_anchor_digests: BTreeMap::new(),
            allowed_data_categories: ["regulated-record".to_string()].into_iter().collect(),
            allowed_media_types: ["application-cbor".to_string()].into_iter().collect(),
            allowed_purpose_digests: [digest("treatment-purpose")].into_iter().collect(),
            maximum_grant_seconds: 86_400,
            maximum_objects_per_grant: 8,
            maximum_object_bytes: 1_000_000,
            maximum_package_bytes: 4_000_000,
            maximum_packages_per_grant: 8,
        }
    }

    fn approvals<T: Serialize>(
        domain: &[u8],
        statement: &T,
        keys: &[SigningKey],
        indexes: [usize; 2],
    ) -> Vec<GrantAuthorityApproval> {
        indexes
            .into_iter()
            .map(|index| {
                sign_approval(
                    domain,
                    format!("authority-{index}"),
                    statement,
                    &keys[index],
                )
                .unwrap()
            })
            .collect()
    }

    struct Fixture {
        source_policy: GrantTrustPolicy,
        recipient_policy: GrantTrustPolicy,
        source_authorities: Vec<SigningKey>,
        recipient_authorities: Vec<SigningKey>,
        source_exporter_key: SigningKey,
        source_exporter: GrantExporter,
        recipient_private_key: SoftwareRecipientPrivateKey,
        recipient_key: CertifiedRecipientKey,
        grant: CertifiedCrossCellGrant,
        object: GrantObjectRef,
    }

    fn fixture() -> Fixture {
        let source_authorities = authority_keys(10);
        let recipient_authorities = authority_keys(30);
        let source_exporter_key = SigningKey::from_bytes(&[70; 32]);
        let recipient_exporter_key = SigningKey::from_bytes(&[71; 32]);
        let source_private_key = SoftwareRecipientPrivateKey::generate("source-hpke-1").unwrap();
        let recipient_private_key =
            SoftwareRecipientPrivateKey::generate("recipient-hpke-1").unwrap();
        let mut source_policy = make_policy(
            id(1),
            "source-policy",
            &source_authorities,
            &source_exporter_key,
            &source_private_key,
        );
        let mut recipient_policy = make_policy(
            id(2),
            "recipient-policy",
            &recipient_authorities,
            &recipient_exporter_key,
            &recipient_private_key,
        );
        let source_anchor = source_policy.trust_anchor_digest().unwrap();
        let recipient_anchor = recipient_policy.trust_anchor_digest().unwrap();
        source_policy
            .trusted_remote_anchor_digests
            .insert(recipient_policy.cell_id.clone(), recipient_anchor);
        recipient_policy
            .trusted_remote_anchor_digests
            .insert(source_policy.cell_id.clone(), source_anchor);
        source_policy.validate().unwrap();
        recipient_policy.validate().unwrap();

        let recipient_key_statement = RecipientKeyStatement {
            format: RECIPIENT_KEY_FORMAT.to_string(),
            certificate_id: id(3),
            trust_policy_digest: document_digest(&recipient_policy).unwrap(),
            recipient_cell_id: recipient_policy.cell_id.clone(),
            application_digest: digest("recipient-application"),
            schema_generation: 9,
            key_id: recipient_private_key.key_id().to_string(),
            key_epoch: 1,
            hpke_suite: HPKE_SUITE.to_string(),
            public_key: recipient_private_key.public_key().to_string(),
            hardware_attestation_digest: digest("recipient-policy-attestation"),
            valid_from: NOW - 100,
            expires_at: NOW + 20_000,
        };
        let recipient_key = CertifiedRecipientKey {
            approvals: approvals(
                RECIPIENT_KEY_DOMAIN,
                &recipient_key_statement,
                &recipient_authorities,
                [4, 5],
            ),
            statement: recipient_key_statement,
        };
        let object = GrantObjectRef {
            namespace: "records".to_string(),
            object_id: "object-7".to_string(),
            source_version_digest: digest("source-version-7"),
            data_category: "regulated-record".to_string(),
            media_type: "application-cbor".to_string(),
        };
        let grant_statement = GrantStatement {
            format: GRANT_FORMAT.to_string(),
            grant_id: id(4),
            grant_epoch: 1,
            previous_grant_digest: None,
            source_policy_digest: document_digest(&source_policy).unwrap(),
            recipient_policy_digest: document_digest(&recipient_policy).unwrap(),
            source_cell_id: source_policy.cell_id.clone(),
            recipient_cell_id: recipient_policy.cell_id.clone(),
            source_application_digest: digest("source-application"),
            recipient_application_digest: recipient_key.statement.application_digest.clone(),
            source_schema_generation: 12,
            recipient_schema_generation: recipient_key.statement.schema_generation,
            recipient_key_certificate_digest: document_digest(&recipient_key.statement).unwrap(),
            exact_objects: [object.clone()].into_iter().collect(),
            purpose_digest: digest("treatment-purpose"),
            redisclosure: RedisclosurePolicy::NewExplicitGrantRequired,
            valid_from: NOW,
            expires_at: NOW + 10_000,
            maximum_object_bytes: 100_000,
            maximum_total_bytes: 500_000,
            maximum_packages: 3,
            anti_replay_nonce: id(5),
            issued_at: NOW - 10,
        };
        let grant = CertifiedCrossCellGrant {
            approvals: approvals(GRANT_DOMAIN, &grant_statement, &source_authorities, [0, 1]),
            statement: grant_statement,
        };
        let source_exporter = GrantExporter::new(
            &source_policy,
            source_policy.cell_id.clone(),
            "export-1",
            source_exporter_key.clone(),
        )
        .unwrap();
        Fixture {
            source_policy,
            recipient_policy,
            source_authorities,
            recipient_authorities,
            source_exporter_key,
            source_exporter,
            recipient_private_key,
            recipient_key,
            grant,
            object,
        }
    }

    fn encrypted_database(path: &Path, cell: &GrantId, key_byte: u8) -> Arc<RwLock<BicDb>> {
        fs::create_dir_all(path).unwrap();
        let binding = EncryptionBinding::new(
            format!("grant-ledger:{cell}"),
            "bicdb-cell-grant-ledger-v1",
            1,
        )
        .unwrap();
        Arc::new(RwLock::new(
            BicDb::open_with_encryption(
                path,
                DbConfig::default().with_fsync(true),
                EncryptionConfig::with_raw_key(vec![key_byte; 32]).with_binding(binding),
            )
            .unwrap(),
        ))
    }

    fn import_review(fixture: &Fixture, package: &SignedGrantPackage) -> CertifiedImportReview {
        let statement = ImportReviewStatement {
            format: IMPORT_REVIEW_FORMAT.to_string(),
            review_id: id(20 + u128::from(package.statement.package_sequence)),
            recipient_policy_digest: document_digest(&fixture.recipient_policy).unwrap(),
            recipient_cell_id: fixture.recipient_policy.cell_id.clone(),
            grant_digest: package.statement.grant_digest.clone(),
            package_digest: document_digest(&package.statement).unwrap(),
            content_policy_digest: digest("content-policy-v1"),
            scanner_profile_digest: digest("scanner-profile-v1"),
            scanner_result_digest: digest("clean-result-v1"),
            reviewed_at: NOW + 10,
            expires_at: NOW + 500,
        };
        CertifiedImportReview {
            approvals: approvals(
                IMPORT_REVIEW_DOMAIN,
                &statement,
                &fixture.recipient_authorities,
                [6, 7],
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
    fn hpke_object_grant_round_trip_persists_encrypted_immutable_evidence() {
        let fixture = fixture();
        let temporary = tempfile::tempdir().unwrap();
        let source_path = temporary.path().join("source");
        let recipient_path = temporary.path().join("recipient");
        let source_database = encrypted_database(&source_path, &fixture.source_policy.cell_id, 91);
        let recipient_database =
            encrypted_database(&recipient_path, &fixture.recipient_policy.cell_id, 92);
        let source = GrantSourceLedger::open_cell_database(
            Arc::clone(&source_database),
            fixture.source_policy.cell_id.clone(),
        )
        .unwrap();
        let recipient = GrantRecipientLedger::open_cell_database(
            Arc::clone(&recipient_database),
            fixture.recipient_policy.cell_id.clone(),
        )
        .unwrap();
        source
            .activate_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                NOW,
            )
            .unwrap();
        recipient
            .accept_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                NOW,
            )
            .unwrap();
        let sentinel = b"CROSS_CELL_PLAINTEXT_SENTINEL".to_vec();
        let package = fixture
            .source_exporter
            .export(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                id(6),
                1,
                None,
                NOW + 1,
                NOW + 1_000,
                vec![GrantPlaintextObject {
                    object: fixture.object.clone(),
                    bytes: sentinel.clone(),
                }],
            )
            .unwrap();
        assert!(!encode_document(&package)
            .unwrap()
            .windows(sentinel.len())
            .any(|window| window == sentinel));
        source
            .record_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                &package,
                NOW + 10,
            )
            .unwrap();
        let review = import_review(&fixture, &package);
        let imported = recipient
            .import_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.recipient_private_key,
                &fixture.grant,
                &package,
                &review,
                NOW + 10,
            )
            .unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].bytes, sentinel);
        assert_eq!(
            recipient
                .imported_evidence(&imported[0].evidence_id)
                .unwrap(),
            imported[0]
        );
        let mut durable = Vec::new();
        collect_file_bytes(&recipient_path, &mut durable);
        assert!(!durable
            .windows(sentinel.len())
            .any(|window| window == sentinel));
    }

    #[test]
    fn wrong_recipient_tamper_replay_and_policy_substitution_fail_closed() {
        let fixture = fixture();
        let package = fixture
            .source_exporter
            .export(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                id(7),
                1,
                None,
                NOW + 1,
                NOW + 1_000,
                vec![GrantPlaintextObject {
                    object: fixture.object.clone(),
                    bytes: b"sensitive object".to_vec(),
                }],
            )
            .unwrap();
        let mut tampered = package.clone();
        tampered.statement.objects[0].ciphertext[0] ^= 1;
        assert!(matches!(
            verify_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                &tampered,
                NOW + 10
            ),
            Err(GrantError::Scope(_))
        ));
        let wrong = SoftwareRecipientPrivateKey::generate("recipient-hpke-1").unwrap();
        assert!(matches!(
            decrypt_package_objects(&fixture.recipient_key, &package, &wrong),
            Err(GrantError::Scope(_))
        ));
        let mut substituted = fixture.recipient_policy.clone();
        substituted.maximum_package_bytes -= 1;
        assert!(matches!(
            verify_grant(
                &fixture.source_policy,
                &substituted,
                &fixture.recipient_key,
                &fixture.grant,
                NOW
            ),
            Err(GrantError::Scope(_))
        ));

        let temporary = tempfile::tempdir().unwrap();
        let database = encrypted_database(
            &temporary.path().join("recipient"),
            &fixture.recipient_policy.cell_id,
            93,
        );
        let recipient = GrantRecipientLedger::open_cell_database(
            database,
            fixture.recipient_policy.cell_id.clone(),
        )
        .unwrap();
        recipient
            .accept_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                NOW,
            )
            .unwrap();
        let review = import_review(&fixture, &package);
        recipient
            .import_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.recipient_private_key,
                &fixture.grant,
                &package,
                &review,
                NOW + 10,
            )
            .unwrap();
        assert!(matches!(
            recipient.import_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.recipient_private_key,
                &fixture.grant,
                &package,
                &review,
                NOW + 10,
            ),
            Err(GrantError::Replay(_))
        ));
    }

    #[test]
    fn threshold_revocation_stops_future_delivery_without_false_erasure_claim() {
        let fixture = fixture();
        let temporary = tempfile::tempdir().unwrap();
        let source_database = encrypted_database(
            &temporary.path().join("source"),
            &fixture.source_policy.cell_id,
            94,
        );
        let recipient_database = encrypted_database(
            &temporary.path().join("recipient"),
            &fixture.recipient_policy.cell_id,
            95,
        );
        let source = GrantSourceLedger::open_cell_database(
            source_database,
            fixture.source_policy.cell_id.clone(),
        )
        .unwrap();
        let recipient = GrantRecipientLedger::open_cell_database(
            recipient_database,
            fixture.recipient_policy.cell_id.clone(),
        )
        .unwrap();
        source
            .activate_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                NOW,
            )
            .unwrap();
        recipient
            .accept_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                NOW,
            )
            .unwrap();
        let first = fixture
            .source_exporter
            .export(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                id(8),
                1,
                None,
                NOW + 1,
                NOW + 1_000,
                vec![GrantPlaintextObject {
                    object: fixture.object.clone(),
                    bytes: b"already delivered".to_vec(),
                }],
            )
            .unwrap();
        let first_digest = source
            .record_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                &first,
                NOW + 10,
            )
            .unwrap();
        let imported = recipient
            .import_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.recipient_private_key,
                &fixture.grant,
                &first,
                &import_review(&fixture, &first),
                NOW + 10,
            )
            .unwrap();
        let revocation_statement = GrantRevocationStatement {
            format: GRANT_REVOCATION_FORMAT.to_string(),
            revocation_id: id(9),
            source_policy_digest: document_digest(&fixture.source_policy).unwrap(),
            grant_id: fixture.grant.statement.grant_id.clone(),
            grant_epoch: 1,
            grant_digest: document_digest(&fixture.grant.statement).unwrap(),
            source_cell_id: fixture.source_policy.cell_id.clone(),
            recipient_cell_id: fixture.recipient_policy.cell_id.clone(),
            reason_digest: digest("consent-withdrawn"),
            revoked_at: NOW + 20,
        };
        let revocation = CertifiedGrantRevocation {
            approvals: approvals(
                REVOCATION_DOMAIN,
                &revocation_statement,
                &fixture.source_authorities,
                [2, 3],
            ),
            statement: revocation_statement,
        };
        source
            .record_revocation(
                &fixture.source_policy,
                &fixture.grant,
                &revocation,
                NOW + 20,
            )
            .unwrap();
        recipient
            .record_revocation(
                &fixture.source_policy,
                &fixture.grant,
                &revocation,
                NOW + 20,
            )
            .unwrap();
        let second = fixture
            .source_exporter
            .export(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                id(10),
                2,
                Some(first_digest),
                NOW + 21,
                NOW + 1_000,
                vec![GrantPlaintextObject {
                    object: fixture.object.clone(),
                    bytes: b"future update".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            source.record_package(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                &second,
                NOW + 21,
            ),
            Err(GrantError::Revoked(_))
        ));
        assert_eq!(
            recipient
                .imported_evidence(&imported[0].evidence_id)
                .unwrap()
                .bytes,
            b"already delivered"
        );
    }

    #[test]
    fn concurrent_package_recording_has_one_durable_winner() {
        let fixture = fixture();
        let temporary = tempfile::tempdir().unwrap();
        let database = encrypted_database(
            &temporary.path().join("source"),
            &fixture.source_policy.cell_id,
            96,
        );
        let source = Arc::new(
            GrantSourceLedger::open_cell_database(database, fixture.source_policy.cell_id.clone())
                .unwrap(),
        );
        source
            .activate_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                NOW,
            )
            .unwrap();
        let package = fixture
            .source_exporter
            .export(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &fixture.grant,
                id(11),
                1,
                None,
                NOW + 1,
                NOW + 1_000,
                vec![GrantPlaintextObject {
                    object: fixture.object.clone(),
                    bytes: b"single durable winner".to_vec(),
                }],
            )
            .unwrap();
        let source_policy = Arc::new(fixture.source_policy);
        let recipient_policy = Arc::new(fixture.recipient_policy);
        let recipient_key = Arc::new(fixture.recipient_key);
        let grant = Arc::new(fixture.grant);
        let package = Arc::new(package);
        let handles = (0..8)
            .map(|_| {
                let source = Arc::clone(&source);
                let source_policy = Arc::clone(&source_policy);
                let recipient_policy = Arc::clone(&recipient_policy);
                let recipient_key = Arc::clone(&recipient_key);
                let grant = Arc::clone(&grant);
                let package = Arc::clone(&package);
                std::thread::spawn(move || {
                    source
                        .record_package(
                            &source_policy,
                            &recipient_policy,
                            &recipient_key,
                            &grant,
                            &package,
                            NOW + 10,
                        )
                        .is_ok()
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|succeeded| *succeeded)
                .count(),
            1
        );
        let state = source.state.read();
        let entry = state.grants.get(&grant.statement.grant_id).unwrap();
        assert_eq!(entry.package_count, 1);
        assert_eq!(entry.last_package_sequence, 1);
    }

    #[test]
    fn canonical_parser_and_role_separation_reject_ambiguous_or_wrong_authority() {
        let fixture = fixture();
        let mut bytes = encode_document(&fixture.grant).unwrap();
        bytes.push(0);
        assert!(matches!(
            decode_document::<CertifiedCrossCellGrant>(&bytes),
            Err(GrantError::Invalid(_))
        ));
        let mut wrong_role = fixture.grant.clone();
        wrong_role.approvals = approvals(
            GRANT_DOMAIN,
            &wrong_role.statement,
            &fixture.source_authorities,
            [2, 3],
        );
        assert!(matches!(
            verify_grant(
                &fixture.source_policy,
                &fixture.recipient_policy,
                &fixture.recipient_key,
                &wrong_role,
                NOW
            ),
            Err(GrantError::Signature(_))
        ));
        assert_ne!(
            fixture.source_exporter_key.verifying_key(),
            fixture.source_authorities[0].verifying_key()
        );
    }
}
