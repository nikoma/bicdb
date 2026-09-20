//! Cell-scoped high-availability control and fencing primitives.
//!
//! This crate deliberately has no Cell key-provider or application-runtime
//! dependency.  Quorum authorities can select a writer and authorize recovery,
//! but cannot decrypt a Cell.  The only database integration is BicDB's
//! commit-admission capability, used locally to refuse every write after the
//! exact writer lease is fenced or expires.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDbError, CommitAdmission, CommitAdmissionIntent, CommitAdmissionTicket};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use parking_lot::{Mutex, RwLock};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub const HA_TRUST_POLICY_FORMAT: &str = "bicdb.cell-ha-trust-policy/v1";
pub const WRITER_EPOCH_FORMAT: &str = "bicdb.cell-writer-epoch/v1";
pub const REPLICA_LEASE_FORMAT: &str = "bicdb.cell-replica-lease/v1";
pub const FENCE_ACK_FORMAT: &str = "bicdb.cell-writer-fence-ack/v1";
pub const REPLICATION_OBJECT_FORMAT: &str = "bicdb.cell-replication-object/v1";
pub const HA_STATE_FORMAT: &str = "bicdb.cell-ha-state/v1";
pub const BACKUP_MANIFEST_FORMAT: &str = "bicdb.cell-backup-manifest/v1";
pub const RESTORE_AUTHORIZATION_FORMAT: &str = "bicdb.cell-restore-authorization/v1";
pub const HA_DRILL_EVIDENCE_FORMAT: &str = "bicdb.cell-ha-drill-evidence/v1";

const WRITER_EPOCH_DOMAIN: &[u8] = b"BICDB-CELL-WRITER-EPOCH-V1\0";
const REPLICA_LEASE_DOMAIN: &[u8] = b"BICDB-CELL-REPLICA-LEASE-V1\0";
const FENCE_ACK_DOMAIN: &[u8] = b"BICDB-CELL-WRITER-FENCE-ACK-V1\0";
const BACKUP_DOMAIN: &[u8] = b"BICDB-CELL-BACKUP-MANIFEST-V1\0";
const RESTORE_DOMAIN: &[u8] = b"BICDB-CELL-RESTORE-AUTHORIZATION-V1\0";
const DRILL_DOMAIN: &[u8] = b"BICDB-CELL-HA-DRILL-EVIDENCE-V1\0";
const REPLICATION_OBJECT_DOMAIN: &[u8] = b"BICDB-CELL-REPLICATION-OBJECT-V1\0";
const COMMIT_TICKET_DOMAIN: &[u8] = b"BICDB-CELL-HA-COMMIT-TICKET-V1\0";
const MAX_DOCUMENT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_REPLICAS: usize = 127;
const MAX_AUTHORITIES: usize = 127;
const MAX_RESTORE_AUTHORIZATION_SECONDS: i64 = 15 * 60;

pub type Result<T> = std::result::Result<T, HaError>;

#[derive(Debug, thiserror::Error)]
pub enum HaError {
    #[error("CELL_HA_DOCUMENT_INVALID: {0}")]
    Invalid(String),
    #[error("CELL_HA_SIGNATURE_INVALID: {0}")]
    Signature(String),
    #[error("CELL_HA_QUORUM_INSUFFICIENT: {0}")]
    Quorum(String),
    #[error("CELL_HA_SCOPE_MISMATCH: {0}")]
    Scope(String),
    #[error("CELL_HA_FENCED: {0}")]
    Fenced(String),
    #[error("CELL_HA_REPLICATION_INVALID: {0}")]
    Replication(String),
    #[error("CELL_HA_BACKUP_INVALID: {0}")]
    Backup(String),
    #[error("CELL_HA_SUPERVISOR_INVALID: {0}")]
    Supervisor(String),
    #[error("CELL_HA_IO: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct HaDigest(String);

impl HaDigest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let Some(hex_value) = value.strip_prefix("sha256:") else {
            return Err(HaError::Invalid(
                "digest must use sha256:<64 lowercase hex>".to_string(),
            ));
        };
        if value != value.to_ascii_lowercase()
            || hex_value.len() != 64
            || !hex_value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(HaError::Invalid(
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

impl std::fmt::Display for HaDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for HaDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct HaId(String);

impl HaId {
    pub fn parse(value: impl Into<String>, label: &str) -> Result<Self> {
        let value = value.into();
        let parsed = Uuid::parse_str(&value)
            .map_err(|_| HaError::Invalid(format!("{label} must be a UUID")))?;
        if parsed.is_nil() {
            return Err(HaError::Invalid(format!("{label} must not be nil")));
        }
        Ok(Self(parsed.hyphenated().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for HaId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for HaId {
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
        .map_err(|error| HaError::Invalid(format!("encode deterministic CBOR: {error}")))?;
    Ok(bytes)
}

pub fn encode_document<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_cbor(value)
}

pub fn decode_document<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_DOCUMENT_BYTES {
        return Err(HaError::Invalid(
            "HA document is empty or exceeds 4 MiB".to_string(),
        ));
    }
    let value: T = ciborium::de::from_reader(bytes)
        .map_err(|error| HaError::Invalid(format!("decode CBOR: {error}")))?;
    if canonical_cbor(&value)? != bytes {
        return Err(HaError::Invalid(
            "HA document is not in BicDB deterministic CBOR encoding".to_string(),
        ));
    }
    Ok(value)
}

pub fn document_digest<T: Serialize>(value: &T) -> Result<HaDigest> {
    Ok(HaDigest::of_bytes(&canonical_cbor(value)?))
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn require_label(value: &str, label: &str) -> Result<()> {
    if valid_label(value) {
        Ok(())
    } else {
        Err(HaError::Invalid(format!(
            "{label} must be a 1..128 byte ASCII identifier"
        )))
    }
}

fn public_key(value: &str, label: &str) -> Result<VerifyingKey> {
    let bytes = hex::decode(value)
        .map_err(|_| HaError::Invalid(format!("{label} is not lowercase hexadecimal")))?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| HaError::Invalid(format!("{label} must contain 32 bytes")))?;
    VerifyingKey::from_bytes(&array)
        .map_err(|_| HaError::Invalid(format!("{label} is not an Ed25519 public key")))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum HaAuthorityRole {
    Lease,
    Recovery,
    Auditor,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HaAuthorityKey {
    pub key_id: String,
    pub role: HaAuthorityRole,
    pub public_key: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HaDurabilityMode {
    SynchronousQuorum,
    LocalSynchronousRemoteAsynchronous,
    Asynchronous,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HaTopologyPolicy {
    pub mode: HaDurabilityMode,
    pub replica_count: u16,
    pub voting_replicas: u16,
    pub maximum_rpo_commits: u64,
    pub maximum_rto_millis: u64,
}

impl HaTopologyPolicy {
    fn validate(&self) -> Result<()> {
        if self.replica_count < 3
            || usize::from(self.replica_count) > MAX_REPLICAS
            || self.voting_replicas < 3
            || self.voting_replicas > self.replica_count
            || self.voting_replicas % 2 == 0
            || self.maximum_rto_millis == 0
        {
            return Err(HaError::Invalid(
                "HA topology requires 3..127 replicas, an odd quorum of at least three, and a positive RTO"
                    .to_string(),
            ));
        }
        if self.mode == HaDurabilityMode::SynchronousQuorum {
            return Err(HaError::Invalid(
                "synchronous-quorum Cell commit replication is not implemented; refusing a false zero-RPO claim"
                    .to_string(),
            ));
        }
        if self.maximum_rpo_commits == 0 || self.maximum_rpo_commits > 1_000_000 {
            return Err(HaError::Invalid(
                "asynchronous Cell replication requires an explicit 1..1000000 commit RPO"
                    .to_string(),
            ));
        }
        if self.maximum_rto_millis > 86_400_000 {
            return Err(HaError::Invalid(
                "Cell HA RTO must not exceed 24 hours".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HaTrustPolicy {
    pub format: String,
    pub policy_id: String,
    pub generation: u64,
    pub authorities: Vec<HaAuthorityKey>,
    pub thresholds: BTreeMap<HaAuthorityRole, u8>,
    pub maximum_clock_skew_seconds: i64,
    pub maximum_replica_lease_seconds: i64,
    pub topology: HaTopologyPolicy,
}

impl HaTrustPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.format != HA_TRUST_POLICY_FORMAT || self.generation == 0 {
            return Err(HaError::Invalid(
                "unsupported or non-monotonic HA trust policy".to_string(),
            ));
        }
        require_label(&self.policy_id, "policy_id")?;
        if self.authorities.len() < 7 || self.authorities.len() > MAX_AUTHORITIES {
            return Err(HaError::Invalid(
                "HA policy requires bounded, independently assigned authorities".to_string(),
            ));
        }
        if !(0..=120).contains(&self.maximum_clock_skew_seconds)
            || !(5..=300).contains(&self.maximum_replica_lease_seconds)
        {
            return Err(HaError::Invalid(
                "HA clock skew or replica lease lifetime exceeds safe bounds".to_string(),
            ));
        }
        self.topology.validate()?;
        let mut key_ids = BTreeSet::new();
        let mut public_keys = BTreeSet::new();
        let mut counts = BTreeMap::<HaAuthorityRole, usize>::new();
        for authority in &self.authorities {
            require_label(&authority.key_id, "authority key_id")?;
            if !key_ids.insert(authority.key_id.clone()) {
                return Err(HaError::Invalid(
                    "duplicate HA authority key_id".to_string(),
                ));
            }
            public_key(&authority.public_key, "HA authority public key")?;
            if !public_keys.insert(authority.public_key.clone()) {
                return Err(HaError::Invalid(
                    "one public key cannot occupy multiple HA trust positions".to_string(),
                ));
            }
            *counts.entry(authority.role).or_default() += 1;
        }
        for (role, safe_minimum) in [
            (HaAuthorityRole::Lease, 2_u8),
            (HaAuthorityRole::Recovery, 2_u8),
            (HaAuthorityRole::Auditor, 1_u8),
        ] {
            let threshold = self.thresholds.get(&role).copied().unwrap_or_default();
            let count = counts.get(&role).copied().unwrap_or_default();
            if threshold < safe_minimum || usize::from(threshold) > count {
                return Err(HaError::Invalid(format!(
                    "unsafe or impossible {role:?} threshold"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HaApproval {
    pub key_id: String,
    pub signature: String,
}

pub fn sign_approval<T: Serialize>(
    domain: &[u8],
    key_id: impl Into<String>,
    document: &T,
    signing_key: &SigningKey,
) -> Result<HaApproval> {
    let key_id = key_id.into();
    require_label(&key_id, "approval key_id")?;
    let mut message = Vec::new();
    message.extend_from_slice(domain);
    message.extend_from_slice(&(key_id.len() as u64).to_be_bytes());
    message.extend_from_slice(key_id.as_bytes());
    message.extend_from_slice(&canonical_cbor(document)?);
    Ok(HaApproval {
        key_id,
        signature: hex::encode(signing_key.sign(&message).to_bytes()),
    })
}

fn verify_quorum<T: Serialize>(
    policy: &HaTrustPolicy,
    role: HaAuthorityRole,
    domain: &[u8],
    document: &T,
    approvals: &[HaApproval],
) -> Result<()> {
    let threshold = usize::from(policy.thresholds.get(&role).copied().unwrap_or_default());
    let encoded = canonical_cbor(document)?;
    let authorities = policy
        .authorities
        .iter()
        .filter(|authority| authority.role == role)
        .map(|authority| (authority.key_id.as_str(), authority))
        .collect::<BTreeMap<_, _>>();
    let mut accepted = BTreeSet::new();
    for approval in approvals {
        if !accepted.insert(approval.key_id.as_str()) {
            return Err(HaError::Signature(
                "duplicate HA approval key_id".to_string(),
            ));
        }
        let authority = authorities.get(approval.key_id.as_str()).ok_or_else(|| {
            HaError::Signature(format!(
                "{} is not authorized for {role:?}",
                approval.key_id
            ))
        })?;
        let verifying_key = public_key(&authority.public_key, "HA authority public key")?;
        let signature_bytes = hex::decode(&approval.signature)
            .map_err(|_| HaError::Signature("approval signature is not hex".to_string()))?;
        let signature = Signature::from_slice(&signature_bytes)
            .map_err(|_| HaError::Signature("approval signature has wrong length".to_string()))?;
        let mut message = Vec::new();
        message.extend_from_slice(domain);
        message.extend_from_slice(&(approval.key_id.len() as u64).to_be_bytes());
        message.extend_from_slice(approval.key_id.as_bytes());
        message.extend_from_slice(&encoded);
        verifying_key
            .verify_strict(&message, &signature)
            .map_err(|_| HaError::Signature("HA approval signature failed".to_string()))?;
    }
    if accepted.len() < threshold {
        return Err(HaError::Quorum(format!(
            "{role:?} approvals {} are below threshold {threshold}",
            accepted.len()
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    Primary,
    Standby,
    Recovery,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FenceAckStatement {
    pub format: String,
    pub cell_id: HaId,
    pub group_id: String,
    pub primary_replica_id: HaId,
    pub writer_epoch: u64,
    pub writer_epoch_digest: HaDigest,
    pub primary_lease_digest: HaDigest,
    pub final_durable_commit_seq: u64,
    pub fenced_at: i64,
    pub nonce: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedFenceAck {
    pub statement: FenceAckStatement,
    pub signature: String,
}

pub fn sign_fence_ack(
    statement: FenceAckStatement,
    signing_key: &SigningKey,
) -> Result<SignedFenceAck> {
    validate_fence_statement(&statement)?;
    let mut message = FENCE_ACK_DOMAIN.to_vec();
    message.extend_from_slice(&canonical_cbor(&statement)?);
    Ok(SignedFenceAck {
        statement,
        signature: hex::encode(signing_key.sign(&message).to_bytes()),
    })
}

fn validate_fence_statement(statement: &FenceAckStatement) -> Result<()> {
    if statement.format != FENCE_ACK_FORMAT || statement.writer_epoch == 0 {
        return Err(HaError::Invalid(
            "invalid writer fence statement".to_string(),
        ));
    }
    require_label(&statement.group_id, "replication group_id")?;
    if statement.nonce.len() != 64
        || !statement
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(HaError::Invalid(
            "fence nonce must be 64 lowercase hexadecimal digits".to_string(),
        ));
    }
    Ok(())
}

fn verify_fence_ack(ack: &SignedFenceAck, public_key_hex: &str) -> Result<()> {
    validate_fence_statement(&ack.statement)?;
    let key = public_key(public_key_hex, "primary fence public key")?;
    let signature_bytes = hex::decode(&ack.signature)
        .map_err(|_| HaError::Signature("fence signature is not hex".to_string()))?;
    let signature = Signature::from_slice(&signature_bytes)
        .map_err(|_| HaError::Signature("fence signature has wrong length".to_string()))?;
    let mut message = FENCE_ACK_DOMAIN.to_vec();
    message.extend_from_slice(&canonical_cbor(&ack.statement)?);
    key.verify_strict(&message, &signature)
        .map_err(|_| HaError::Signature("old-primary fence signature failed".to_string()))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum FenceEvidence {
    Graceful { acknowledgement: SignedFenceAck },
    PreviousLeaseExpired { observed_at: i64 },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WriterEpochStatement {
    pub format: String,
    pub cell_id: HaId,
    pub group_id: String,
    pub writer_epoch: u64,
    pub primary_replica_id: HaId,
    pub primary_fence_public_key: String,
    pub key_epoch: u64,
    pub manifest_generation: u64,
    pub manifest_digest: HaDigest,
    pub accepted_durable_commit_seq: u64,
    pub activated_at: i64,
    pub predecessor_epoch_digest: Option<HaDigest>,
    pub predecessor_primary_lease_digest: Option<HaDigest>,
    pub fence_evidence: Option<FenceEvidence>,
}

impl WriterEpochStatement {
    fn validate(&self) -> Result<()> {
        if self.format != WRITER_EPOCH_FORMAT
            || self.writer_epoch == 0
            || self.key_epoch == 0
            || self.manifest_generation == 0
        {
            return Err(HaError::Invalid(
                "invalid writer epoch statement".to_string(),
            ));
        }
        require_label(&self.group_id, "replication group_id")?;
        public_key(&self.primary_fence_public_key, "primary fence public key")?;
        let initial = self.writer_epoch == 1;
        if initial
            != (self.predecessor_epoch_digest.is_none()
                && self.predecessor_primary_lease_digest.is_none()
                && self.fence_evidence.is_none())
        {
            return Err(HaError::Invalid(
                "initial writer epoch alone omits predecessor and fence evidence".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedWriterEpoch {
    pub statement: WriterEpochStatement,
    pub approvals: Vec<HaApproval>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicaLeaseStatement {
    pub format: String,
    pub lease_id: HaId,
    pub lease_sequence: u64,
    pub predecessor_lease_digest: Option<HaDigest>,
    pub cell_id: HaId,
    pub group_id: String,
    pub replica_id: HaId,
    /// Workload-held Ed25519 key for authenticating every object emitted by
    /// this exact short-lived replica lease. Sharing the Cell data key does
    /// not grant authority to impersonate the elected writer.
    pub replica_public_key: String,
    pub role: ReplicaRole,
    pub writer_epoch: u64,
    pub writer_epoch_digest: HaDigest,
    pub key_epoch: u64,
    pub manifest_generation: u64,
    pub manifest_digest: HaDigest,
    pub local_durable_commit_seq: u64,
    pub issued_at: i64,
    pub not_before: i64,
    pub expires_at: i64,
}

impl ReplicaLeaseStatement {
    fn validate(&self, policy: &HaTrustPolicy) -> Result<()> {
        if self.format != REPLICA_LEASE_FORMAT
            || self.lease_sequence == 0
            || self.writer_epoch == 0
            || self.key_epoch == 0
            || self.manifest_generation == 0
        {
            return Err(HaError::Invalid(
                "invalid replica lease statement".to_string(),
            ));
        }
        require_label(&self.group_id, "replication group_id")?;
        public_key(&self.replica_public_key, "replica transport public key")?;
        if self.not_before < self.issued_at
            || self.expires_at <= self.not_before
            || self.expires_at.saturating_sub(self.not_before)
                > policy.maximum_replica_lease_seconds
        {
            return Err(HaError::Invalid(
                "replica lease lifetime is invalid or exceeds policy".to_string(),
            ));
        }
        if (self.lease_sequence == 1) != self.predecessor_lease_digest.is_none() {
            return Err(HaError::Invalid(
                "only the first replica lease omits its predecessor digest".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn verify_replica_lease(
    policy: &HaTrustPolicy,
    lease: &CertifiedReplicaLease,
    now: i64,
) -> Result<HaDigest> {
    policy.validate()?;
    let digest = verify_lease_approval(policy, lease)?;
    let skew = policy.maximum_clock_skew_seconds;
    if now.saturating_add(skew) < lease.statement.not_before
        || now.saturating_sub(skew) > lease.statement.expires_at
    {
        return Err(HaError::Fenced(
            "certified replica lease is not currently usable".to_string(),
        ));
    }
    Ok(digest)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedReplicaLease {
    pub statement: ReplicaLeaseStatement,
    pub approvals: Vec<HaApproval>,
}

#[derive(Clone, Debug)]
pub struct HaActivationContext {
    pub cell_id: HaId,
    pub group_id: String,
    pub replica_id: HaId,
    pub role: ReplicaRole,
    pub writer_epoch: u64,
    pub key_epoch: u64,
    pub manifest_generation: u64,
    pub manifest_digest: HaDigest,
    pub local_durable_commit_seq: u64,
    pub now: i64,
}

#[derive(Clone, Debug)]
pub struct VerifiedHaActivation {
    pub epoch: WriterEpochStatement,
    pub epoch_digest: HaDigest,
    pub lease: ReplicaLeaseStatement,
    pub lease_digest: HaDigest,
}

fn verify_epoch_approval(policy: &HaTrustPolicy, epoch: &CertifiedWriterEpoch) -> Result<HaDigest> {
    epoch.statement.validate()?;
    verify_quorum(
        policy,
        HaAuthorityRole::Lease,
        WRITER_EPOCH_DOMAIN,
        &epoch.statement,
        &epoch.approvals,
    )?;
    document_digest(&epoch.statement)
}

fn verify_lease_approval(
    policy: &HaTrustPolicy,
    lease: &CertifiedReplicaLease,
) -> Result<HaDigest> {
    lease.statement.validate(policy)?;
    verify_quorum(
        policy,
        HaAuthorityRole::Lease,
        REPLICA_LEASE_DOMAIN,
        &lease.statement,
        &lease.approvals,
    )?;
    document_digest(&lease.statement)
}

fn verify_epoch_transition(
    policy: &HaTrustPolicy,
    next: &WriterEpochStatement,
    previous_epoch: Option<&CertifiedWriterEpoch>,
    previous_primary_lease: Option<&CertifiedReplicaLease>,
) -> Result<()> {
    if next.writer_epoch == 1 {
        if previous_epoch.is_some() || previous_primary_lease.is_some() {
            return Err(HaError::Scope(
                "initial writer epoch cannot accept predecessor documents".to_string(),
            ));
        }
        return Ok(());
    }
    let previous_epoch = previous_epoch.ok_or_else(|| {
        HaError::Scope("writer promotion requires the certified predecessor epoch".to_string())
    })?;
    let previous_epoch_digest = verify_epoch_approval(policy, previous_epoch)?;
    let previous_lease = previous_primary_lease.ok_or_else(|| {
        HaError::Scope("writer promotion requires the predecessor primary lease".to_string())
    })?;
    let previous_lease_digest = verify_lease_approval(policy, previous_lease)?;
    if next.cell_id != previous_epoch.statement.cell_id
        || next.group_id != previous_epoch.statement.group_id
        || next.writer_epoch != previous_epoch.statement.writer_epoch.saturating_add(1)
        || next.predecessor_epoch_digest.as_ref() != Some(&previous_epoch_digest)
        || next.predecessor_primary_lease_digest.as_ref() != Some(&previous_lease_digest)
        || previous_lease.statement.cell_id != previous_epoch.statement.cell_id
        || previous_lease.statement.group_id != previous_epoch.statement.group_id
        || previous_lease.statement.replica_id != previous_epoch.statement.primary_replica_id
        || previous_lease.statement.role != ReplicaRole::Primary
        || previous_lease.statement.writer_epoch != previous_epoch.statement.writer_epoch
        || previous_lease.statement.writer_epoch_digest != previous_epoch_digest
        || next.key_epoch < previous_epoch.statement.key_epoch
        || next.manifest_generation < previous_epoch.statement.manifest_generation
    {
        return Err(HaError::Scope(
            "writer epoch predecessor, primary lease, or monotonic scope mismatch".to_string(),
        ));
    }
    match next.fence_evidence.as_ref().ok_or_else(|| {
        HaError::Fenced("writer promotion has no old-primary fence evidence".to_string())
    })? {
        FenceEvidence::PreviousLeaseExpired { observed_at } => {
            let safe_after = previous_lease
                .statement
                .expires_at
                .saturating_add(policy.maximum_clock_skew_seconds);
            if *observed_at < safe_after || next.activated_at < safe_after {
                return Err(HaError::Fenced(
                    "new epoch overlaps the predecessor primary lease".to_string(),
                ));
            }
        }
        FenceEvidence::Graceful { acknowledgement } => {
            verify_fence_ack(
                acknowledgement,
                &previous_epoch.statement.primary_fence_public_key,
            )?;
            let statement = &acknowledgement.statement;
            if statement.cell_id != next.cell_id
                || statement.group_id != next.group_id
                || statement.primary_replica_id != previous_epoch.statement.primary_replica_id
                || statement.writer_epoch != previous_epoch.statement.writer_epoch
                || statement.writer_epoch_digest != previous_epoch_digest
                || statement.primary_lease_digest != previous_lease_digest
                || next.accepted_durable_commit_seq < statement.final_durable_commit_seq
                || next.activated_at < statement.fenced_at
            {
                return Err(HaError::Fenced(
                    "graceful fence acknowledgement does not bind the exact predecessor"
                        .to_string(),
                ));
            }
        }
    }
    if next.accepted_durable_commit_seq < previous_lease.statement.local_durable_commit_seq {
        return Err(HaError::Fenced(
            "promotion would move durable commit state backwards".to_string(),
        ));
    }
    Ok(())
}

pub fn verify_ha_activation(
    policy: &HaTrustPolicy,
    epoch: &CertifiedWriterEpoch,
    lease: &CertifiedReplicaLease,
    previous_epoch: Option<&CertifiedWriterEpoch>,
    previous_primary_lease: Option<&CertifiedReplicaLease>,
    context: &HaActivationContext,
) -> Result<VerifiedHaActivation> {
    policy.validate()?;
    let epoch_digest = verify_epoch_approval(policy, epoch)?;
    verify_epoch_transition(
        policy,
        &epoch.statement,
        previous_epoch,
        previous_primary_lease,
    )?;
    let lease_digest = verify_lease_approval(policy, lease)?;
    let expected_primary = context.role == ReplicaRole::Primary;
    if epoch.statement.cell_id != context.cell_id
        || epoch.statement.group_id != context.group_id
        || epoch.statement.writer_epoch != context.writer_epoch
        || epoch.statement.key_epoch != context.key_epoch
        || epoch.statement.manifest_generation != context.manifest_generation
        || epoch.statement.manifest_digest != context.manifest_digest
        || lease.statement.cell_id != context.cell_id
        || lease.statement.group_id != context.group_id
        || lease.statement.replica_id != context.replica_id
        || lease.statement.role != context.role
        || lease.statement.writer_epoch != context.writer_epoch
        || lease.statement.writer_epoch_digest != epoch_digest
        || lease.statement.key_epoch != context.key_epoch
        || lease.statement.manifest_generation != context.manifest_generation
        || lease.statement.manifest_digest != context.manifest_digest
        || lease.statement.local_durable_commit_seq != context.local_durable_commit_seq
        || (expected_primary && epoch.statement.primary_replica_id != context.replica_id)
        || (expected_primary
            && lease.statement.replica_public_key != epoch.statement.primary_fence_public_key)
        || (!expected_primary
            && context.role == ReplicaRole::Standby
            && epoch.statement.primary_replica_id == context.replica_id)
    {
        return Err(HaError::Scope(
            "HA activation does not match exact Cell/replica/epoch/manifest state".to_string(),
        ));
    }
    let skew = policy.maximum_clock_skew_seconds;
    if context.now.saturating_add(skew) < lease.statement.not_before
        || context.now.saturating_sub(skew) > lease.statement.expires_at
        || context.now.saturating_add(skew) < epoch.statement.activated_at
    {
        return Err(HaError::Fenced(
            "HA replica lease or writer epoch is not currently valid".to_string(),
        ));
    }
    if expected_primary
        && lease.statement.local_durable_commit_seq != epoch.statement.accepted_durable_commit_seq
    {
        return Err(HaError::Scope(
            "primary durable sequence differs from accepted writer history".to_string(),
        ));
    }
    Ok(VerifiedHaActivation {
        epoch: epoch.statement.clone(),
        epoch_digest,
        lease: lease.statement.clone(),
        lease_digest,
    })
}

pub trait HaClock: std::fmt::Debug + Send + Sync {
    fn unix_timestamp(&self) -> i64;
}

#[derive(Debug)]
pub struct SystemHaClock;

impl HaClock for SystemHaClock {
    fn unix_timestamp(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WriterFenceStatus {
    pub cell_id: HaId,
    pub group_id: String,
    pub replica_id: HaId,
    pub role: ReplicaRole,
    pub writer_epoch: u64,
    pub lease_sequence: u64,
    pub lease_digest: HaDigest,
    pub expires_at: i64,
    pub revoked: bool,
}

#[derive(Clone)]
pub struct CellHaCommitFence {
    policy: Arc<HaTrustPolicy>,
    clock: Arc<dyn HaClock>,
    epoch: WriterEpochStatement,
    epoch_digest: HaDigest,
    lease: Arc<RwLock<(ReplicaLeaseStatement, HaDigest)>>,
    revoked: Arc<AtomicBool>,
    durable_state: Option<Arc<Mutex<(CellHaStateStore, CellHaState)>>>,
}

impl std::fmt::Debug for CellHaCommitFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = self.status();
        formatter
            .debug_struct("CellHaCommitFence")
            .field("cell_id", &status.cell_id)
            .field("replica_id", &status.replica_id)
            .field("writer_epoch", &status.writer_epoch)
            .field("revoked", &status.revoked)
            .finish()
    }
}

impl CellHaCommitFence {
    pub fn new(
        policy: Arc<HaTrustPolicy>,
        activation: VerifiedHaActivation,
        clock: Arc<dyn HaClock>,
    ) -> Result<Self> {
        policy.validate()?;
        Ok(Self {
            policy,
            clock,
            epoch: activation.epoch,
            epoch_digest: activation.epoch_digest,
            lease: Arc::new(RwLock::new((activation.lease, activation.lease_digest))),
            revoked: Arc::new(AtomicBool::new(false)),
            durable_state: None,
        })
    }

    /// Attach the ciphertext-free, fsynced HA rollback witness. The witness
    /// is advanced from the post-WAL commit callback, never by SQL or the
    /// orchestration layer. Concurrent group-commit callbacks may complete out
    /// of order; an already-higher local watermark is therefore idempotent.
    pub fn with_durable_state(
        mut self,
        store: CellHaStateStore,
        initial: CellHaState,
    ) -> Result<Self> {
        let status = self.status();
        if initial.cell_id != status.cell_id
            || initial.group_id != status.group_id
            || initial.replica_id != status.replica_id
            || initial.writer_epoch != status.writer_epoch
            || initial.writer_epoch_digest != self.epoch_digest
        {
            return Err(HaError::Scope(
                "durable HA witness does not match the active writer fence".to_string(),
            ));
        }
        store.advance(&initial)?;
        self.durable_state = Some(Arc::new(Mutex::new((store, initial))));
        Ok(self)
    }

    pub fn writer_epoch(&self) -> &WriterEpochStatement {
        &self.epoch
    }

    pub fn status(&self) -> WriterFenceStatus {
        let lease = self.lease.read();
        WriterFenceStatus {
            cell_id: lease.0.cell_id.clone(),
            group_id: lease.0.group_id.clone(),
            replica_id: lease.0.replica_id.clone(),
            role: lease.0.role,
            writer_epoch: lease.0.writer_epoch,
            lease_sequence: lease.0.lease_sequence,
            lease_digest: lease.1.clone(),
            expires_at: lease.0.expires_at,
            revoked: self.revoked.load(Ordering::SeqCst),
        }
    }

    pub fn assert_live(&self, required_role: ReplicaRole) -> Result<()> {
        if self.revoked.load(Ordering::SeqCst) {
            return Err(HaError::Fenced(
                "replica lease was locally fenced".to_string(),
            ));
        }
        let lease = self.lease.read();
        let now = self.clock.unix_timestamp();
        if lease.0.role != required_role || now < lease.0.not_before || now > lease.0.expires_at {
            return Err(HaError::Fenced(
                "replica role or short-lived lease is not active".to_string(),
            ));
        }
        Ok(())
    }

    pub fn renew(&self, next: &CertifiedReplicaLease) -> Result<()> {
        let next_digest = verify_lease_approval(&self.policy, next)?;
        // Serialize comparison and replacement under one write guard. Two
        // independently delivered, same-sequence quorum documents must never
        // race and let an older/shorter lease overwrite the winner.
        let mut current = self.lease.write();
        if next.statement.cell_id != current.0.cell_id
            || next.statement.group_id != current.0.group_id
            || next.statement.replica_id != current.0.replica_id
            || next.statement.replica_public_key != current.0.replica_public_key
            || next.statement.role != current.0.role
            || next.statement.writer_epoch != current.0.writer_epoch
            || next.statement.writer_epoch_digest != self.epoch_digest
            || next.statement.key_epoch != current.0.key_epoch
            || next.statement.manifest_generation != current.0.manifest_generation
            || next.statement.manifest_digest != current.0.manifest_digest
            || next.statement.lease_sequence != current.0.lease_sequence.saturating_add(1)
            || next.statement.predecessor_lease_digest.as_ref() != Some(&current.1)
            || next.statement.not_before
                > current
                    .0
                    .expires_at
                    .saturating_add(self.policy.maximum_clock_skew_seconds)
        {
            return Err(HaError::Scope(
                "replica lease renewal does not exactly continue current authority".to_string(),
            ));
        }
        let now = self.clock.unix_timestamp();
        if now.saturating_add(self.policy.maximum_clock_skew_seconds) < next.statement.not_before
            || now.saturating_sub(self.policy.maximum_clock_skew_seconds)
                > next.statement.expires_at
        {
            return Err(HaError::Fenced(
                "replica lease renewal is not currently usable".to_string(),
            ));
        }
        *current = (next.statement.clone(), next_digest);
        Ok(())
    }

    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }

    pub fn fence_ack_statement(
        &self,
        final_durable_commit_seq: u64,
        nonce: String,
    ) -> Result<FenceAckStatement> {
        self.revoke();
        let status = self.status();
        Ok(FenceAckStatement {
            format: FENCE_ACK_FORMAT.to_string(),
            cell_id: status.cell_id,
            group_id: status.group_id,
            primary_replica_id: status.replica_id,
            writer_epoch: status.writer_epoch,
            writer_epoch_digest: self.epoch_digest.clone(),
            primary_lease_digest: status.lease_digest,
            final_durable_commit_seq,
            fenced_at: self.clock.unix_timestamp(),
            nonce,
        })
    }

    pub fn checkpoint_durable_commit(&self, commit_seq: u64) -> Result<()> {
        if let Some(durable_state) = self.durable_state.as_ref() {
            let mut durable_state = durable_state.lock();
            if commit_seq > durable_state.1.last_applied_commit_seq {
                let mut next = durable_state.1.clone();
                next.last_applied_commit_seq = commit_seq;
                durable_state.0.advance(&next)?;
                durable_state.1 = next;
            }
        }
        Ok(())
    }

    pub fn checkpoint_replication_apply(
        &self,
        commit_seq: u64,
        expected_previous_commit_seq: u64,
        expected_predecessor_digest: Option<&HaDigest>,
        object_digest: HaDigest,
    ) -> Result<()> {
        let durable_state = self.durable_state.as_ref().ok_or_else(|| {
            HaError::Invalid("replication checkpoint has no durable HA witness".to_string())
        })?;
        let mut durable_state = durable_state.lock();
        if durable_state.1.last_applied_commit_seq == commit_seq
            && durable_state.1.last_replication_object_digest.as_ref() == Some(&object_digest)
        {
            return Ok(());
        }
        if commit_seq != expected_previous_commit_seq.saturating_add(1)
            || durable_state.1.last_applied_commit_seq != expected_previous_commit_seq
            || durable_state.1.last_replication_object_digest.as_ref()
                != expected_predecessor_digest
        {
            return Err(HaError::Fenced(
                "replication checkpoint is not the exact durable predecessor continuation"
                    .to_string(),
            ));
        }
        let mut next = durable_state.1.clone();
        next.last_applied_commit_seq = commit_seq;
        next.last_replication_object_digest = Some(object_digest);
        durable_state.0.advance(&next)?;
        durable_state.1 = next;
        Ok(())
    }

    pub fn checkpoint_backup(
        &self,
        expected_predecessor: Option<&HaDigest>,
        backup_digest: HaDigest,
    ) -> Result<()> {
        let durable_state = self.durable_state.as_ref().ok_or_else(|| {
            HaError::Invalid("backup checkpoint has no durable HA witness".to_string())
        })?;
        let mut durable_state = durable_state.lock();
        if durable_state.1.backup_head_digest.as_ref() != expected_predecessor {
            return Err(HaError::Fenced(
                "backup certification raced or does not extend the durable backup head".to_string(),
            ));
        }
        let mut next = durable_state.1.clone();
        next.backup_head_digest = Some(backup_digest);
        durable_state.0.advance(&next)?;
        durable_state.1 = next;
        Ok(())
    }

    pub fn durable_state(&self) -> Option<CellHaState> {
        self.durable_state
            .as_ref()
            .map(|state| state.lock().1.clone())
    }
}

impl CommitAdmission for CellHaCommitFence {
    fn admit(
        &self,
        intent: &CommitAdmissionIntent,
    ) -> std::result::Result<CommitAdmissionTicket, BicDbError> {
        self.assert_live(ReplicaRole::Primary)
            .map_err(|error| BicDbError::HighAvailability(error.to_string()))?;
        let status = self.status();
        let mut hasher = Sha256::new();
        hasher.update(COMMIT_TICKET_DOMAIN);
        hasher.update(self.epoch_digest.as_str().as_bytes());
        hasher.update(status.lease_digest.as_str().as_bytes());
        let intent_bytes = canonical_cbor(intent)
            .map_err(|error| BicDbError::HighAvailability(error.to_string()))?;
        hasher.update(intent_bytes);
        Ok(CommitAdmissionTicket {
            authority: format!(
                "cell-ha:{}/{}/{}",
                status.cell_id, status.writer_epoch, status.replica_id
            ),
            command_id: hex::encode(hasher.finalize()),
        })
    }

    fn local_applied(
        &self,
        ticket: &CommitAdmissionTicket,
        commit_seq: u64,
    ) -> std::result::Result<(), BicDbError> {
        // A commit may have crossed admission immediately before fencing. It
        // must still finish local durability and advance the exact witness;
        // rejecting this callback after revocation would manufacture an
        // apparent gap between the database and the HA control plane.
        let status = self.status();
        let expected = format!(
            "cell-ha:{}/{}/{}",
            status.cell_id, status.writer_epoch, status.replica_id
        );
        if ticket.authority != expected
            || ticket.command_id.len() != 64
            || !ticket
                .command_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(BicDbError::HighAvailability(
                "commit ticket is outside the active Cell writer fence".to_string(),
            ));
        }
        self.checkpoint_durable_commit(commit_seq)
            .map_err(|error| BicDbError::HighAvailability(error.to_string()))?;
        Ok(())
    }

    fn fence(&self) -> std::result::Result<(), BicDbError> {
        self.revoke();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationObjectKind {
    Handshake,
    SnapshotManifest,
    SnapshotChunk,
    Commit,
    Acknowledgement,
    DurableApply,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationObjectHeader {
    pub format: String,
    pub kind: ReplicationObjectKind,
    pub cell_id: HaId,
    pub group_id: String,
    pub source_replica_id: HaId,
    pub destination_replica_id: HaId,
    pub writer_epoch: u64,
    pub key_epoch: u64,
    pub protocol_version: u32,
    pub commit_seq: u64,
    pub previous_commit_seq: u64,
    pub predecessor_object_digest: Option<HaDigest>,
    pub plaintext_digest: HaDigest,
    pub ciphertext_digest: HaDigest,
}

impl ReplicationObjectHeader {
    pub fn validate(&self) -> Result<()> {
        if self.format != REPLICATION_OBJECT_FORMAT
            || self.source_replica_id == self.destination_replica_id
            || self.writer_epoch == 0
            || self.key_epoch == 0
            || self.protocol_version == 0
            || self.previous_commit_seq > self.commit_seq
        {
            return Err(HaError::Replication(
                "invalid Cell replication object header".to_string(),
            ));
        }
        require_label(&self.group_id, "replication group_id")
    }

    pub fn assert_scope(
        &self,
        cell_id: &HaId,
        group_id: &str,
        source: &HaId,
        destination: &HaId,
        writer_epoch: u64,
        key_epoch: u64,
    ) -> Result<()> {
        self.validate()?;
        if &self.cell_id != cell_id
            || self.group_id != group_id
            || &self.source_replica_id != source
            || &self.destination_replica_id != destination
            || self.writer_epoch != writer_epoch
            || self.key_epoch != key_epoch
        {
            return Err(HaError::Replication(
                "replication object crossed Cell/replica/epoch scope".to_string(),
            ));
        }
        Ok(())
    }

    /// Canonical authenticated metadata for the cell-bound payload cipher.
    /// The ciphertext digest is deliberately excluded because it is only
    /// known after sealing; every routing, epoch, sequence, predecessor, kind,
    /// and plaintext-integrity field remains inside AEAD authentication.
    pub fn authenticated_data(&self) -> Result<Vec<u8>> {
        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct AuthenticatedHeader<'a> {
            format: &'a str,
            kind: ReplicationObjectKind,
            cell_id: &'a HaId,
            group_id: &'a str,
            source_replica_id: &'a HaId,
            destination_replica_id: &'a HaId,
            writer_epoch: u64,
            key_epoch: u64,
            protocol_version: u32,
            commit_seq: u64,
            previous_commit_seq: u64,
            predecessor_object_digest: &'a Option<HaDigest>,
            plaintext_digest: &'a HaDigest,
        }
        self.validate()?;
        canonical_cbor(&AuthenticatedHeader {
            format: &self.format,
            kind: self.kind,
            cell_id: &self.cell_id,
            group_id: &self.group_id,
            source_replica_id: &self.source_replica_id,
            destination_replica_id: &self.destination_replica_id,
            writer_epoch: self.writer_epoch,
            key_epoch: self.key_epoch,
            protocol_version: self.protocol_version,
            commit_seq: self.commit_seq,
            previous_commit_seq: self.previous_commit_seq,
            predecessor_object_digest: &self.predecessor_object_digest,
            plaintext_digest: &self.plaintext_digest,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellReplicationObject {
    pub header: ReplicationObjectHeader,
    pub source_lease_digest: HaDigest,
    pub ciphertext: Vec<u8>,
    pub signature: String,
}

impl CellReplicationObject {
    pub fn validate_ciphertext(&self) -> Result<()> {
        self.header.validate()?;
        if self.ciphertext.is_empty()
            || HaDigest::of_bytes(&self.ciphertext) != self.header.ciphertext_digest
        {
            return Err(HaError::Replication(
                "replication ciphertext is empty or has the wrong digest".to_string(),
            ));
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<HaDigest> {
        document_digest(self)
    }
}

fn replication_object_signing_message(object: &CellReplicationObject) -> Result<Vec<u8>> {
    let mut unsigned = object.clone();
    unsigned.signature.clear();
    let mut message = REPLICATION_OBJECT_DOMAIN.to_vec();
    message.extend_from_slice(&canonical_cbor(&unsigned)?);
    Ok(message)
}

pub fn sign_replication_object(
    mut object: CellReplicationObject,
    signing_key: &SigningKey,
) -> Result<CellReplicationObject> {
    object.validate_ciphertext()?;
    object.signature = hex::encode(
        signing_key
            .sign(&replication_object_signing_message(&object)?)
            .to_bytes(),
    );
    Ok(object)
}

pub fn verify_replication_object(
    policy: &HaTrustPolicy,
    object: &CellReplicationObject,
    source_lease: &CertifiedReplicaLease,
    now: i64,
) -> Result<()> {
    object.validate_ciphertext()?;
    let lease_digest = verify_replica_lease(policy, source_lease, now)?;
    let statement = &source_lease.statement;
    if object.source_lease_digest != lease_digest
        || object.header.cell_id != statement.cell_id
        || object.header.group_id != statement.group_id
        || object.header.source_replica_id != statement.replica_id
        || object.header.writer_epoch != statement.writer_epoch
        || object.header.key_epoch != statement.key_epoch
        || (matches!(
            object.header.kind,
            ReplicationObjectKind::SnapshotManifest
                | ReplicationObjectKind::SnapshotChunk
                | ReplicationObjectKind::Commit
        ) && statement.role != ReplicaRole::Primary)
    {
        return Err(HaError::Replication(
            "replication object is outside its certified source lease".to_string(),
        ));
    }
    let key = public_key(
        &statement.replica_public_key,
        "replica transport public key",
    )?;
    let signature_bytes = hex::decode(&object.signature)
        .map_err(|_| HaError::Signature("replication signature is not hex".to_string()))?;
    let signature = Signature::from_slice(&signature_bytes)
        .map_err(|_| HaError::Signature("replication signature has wrong length".to_string()))?;
    key.verify_strict(&replication_object_signing_message(object)?, &signature)
        .map_err(|_| HaError::Signature("replication source signature failed".to_string()))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellHaState {
    pub format: String,
    pub cell_id: HaId,
    pub group_id: String,
    pub replica_id: HaId,
    pub writer_epoch: u64,
    pub writer_epoch_digest: HaDigest,
    pub last_applied_commit_seq: u64,
    pub last_replication_object_digest: Option<HaDigest>,
    pub backup_head_digest: Option<HaDigest>,
}

impl CellHaState {
    fn validate(&self) -> Result<()> {
        if self.format != HA_STATE_FORMAT || self.writer_epoch == 0 {
            return Err(HaError::Invalid(
                "invalid durable Cell HA state".to_string(),
            ));
        }
        require_label(&self.group_id, "replication group_id")
    }
}

#[derive(Clone, Debug)]
pub struct CellHaStateStore {
    path: PathBuf,
}

impl CellHaStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<CellHaState>> {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(HaError::Invalid(
                    "Cell HA state is not a regular non-symlink file".to_string(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let bytes = read_bounded(&self.path, MAX_DOCUMENT_BYTES)?;
        let state: CellHaState = decode_document(&bytes)?;
        state.validate()?;
        Ok(Some(state))
    }

    pub fn advance(&self, next: &CellHaState) -> Result<()> {
        next.validate()?;
        if let Some(current) = self.load()? {
            if next.cell_id != current.cell_id
                || next.group_id != current.group_id
                || next.replica_id != current.replica_id
                || next.writer_epoch < current.writer_epoch
                || (next.writer_epoch == current.writer_epoch
                    && next.writer_epoch_digest != current.writer_epoch_digest)
                || next.last_applied_commit_seq < current.last_applied_commit_seq
            {
                return Err(HaError::Fenced(
                    "durable Cell HA state would roll back or change identity".to_string(),
                ));
            }
            if next.writer_epoch > current.writer_epoch
                && next.writer_epoch != current.writer_epoch.saturating_add(1)
            {
                return Err(HaError::Fenced(
                    "durable Cell HA writer epoch must advance exactly once".to_string(),
                ));
            }
        }
        let parent = self.path.parent().ok_or_else(|| {
            HaError::Invalid("Cell HA state path has no parent directory".to_string())
        })?;
        let parent_metadata = fs::symlink_metadata(parent)?;
        if !parent_metadata.file_type().is_dir() || parent_metadata.file_type().is_symlink() {
            return Err(HaError::Invalid(
                "Cell HA state parent must be a real directory".to_string(),
            ));
        }
        let target_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| HaError::Invalid("Cell HA state filename is invalid".to_string()))?;
        // A unique create-new sibling cannot be confused with debris from a
        // process crash between fsync and rename. The kernel-generated UUID
        // also prevents two accidental runtimes from sharing a staging path;
        // the Cell volume lease remains the primary cross-process exclusion.
        let staging = parent.join(format!(".{target_name}.{}.new", Uuid::new_v4()));
        let bytes = canonical_cbor(next)?;
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&staging)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&staging, &self.path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&staging);
        }
        result
    }
}

fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() || metadata.len() > max
    {
        return Err(HaError::Invalid(
            "HA file is not a bounded regular file".to_string(),
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() > max {
        return Err(HaError::Invalid(
            "opened HA file is not a bounded regular file".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev() != opened.dev() || metadata.ino() != opened.ino() {
            return Err(HaError::Invalid(
                "HA file changed while it was being opened".to_string(),
            ));
        }
        if opened.nlink() != 1 || opened.mode() & 0o022 != 0 {
            return Err(HaError::Invalid(
                "HA file must have one link and must not be group/world writable".to_string(),
            ));
        }
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    std::io::Read::by_ref(&mut file)
        .take(max.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(HaError::Invalid("HA file exceeds its bound".to_string()));
    }
    Ok(bytes)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CellBackupManifest {
    pub format: String,
    pub backup_id: HaId,
    pub cell_id: HaId,
    pub group_id: String,
    pub lineage_id: String,
    pub source_replica_id: HaId,
    pub writer_epoch: u64,
    pub key_epoch: u64,
    pub manifest_generation: u64,
    pub manifest_digest: HaDigest,
    pub durable_commit_seq: u64,
    pub predecessor_backup_digest: Option<HaDigest>,
    pub encrypted_payload_digest: HaDigest,
    pub created_at: i64,
}

impl CellBackupManifest {
    fn validate(&self) -> Result<()> {
        if self.format != BACKUP_MANIFEST_FORMAT
            || self.writer_epoch == 0
            || self.key_epoch == 0
            || self.manifest_generation == 0
        {
            return Err(HaError::Backup("invalid Cell backup manifest".to_string()));
        }
        require_label(&self.group_id, "backup group_id")?;
        require_label(&self.lineage_id, "backup lineage_id")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedCellBackup {
    pub manifest: CellBackupManifest,
    pub approvals: Vec<HaApproval>,
}

pub fn verify_cell_backup(
    policy: &HaTrustPolicy,
    backup: &CertifiedCellBackup,
) -> Result<HaDigest> {
    policy.validate()?;
    backup.manifest.validate()?;
    verify_quorum(
        policy,
        HaAuthorityRole::Recovery,
        BACKUP_DOMAIN,
        &backup.manifest,
        &backup.approvals,
    )?;
    document_digest(&backup.manifest)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreAuthorization {
    pub format: String,
    pub authorization_id: HaId,
    pub backup_digest: HaDigest,
    pub cell_id: HaId,
    pub group_id: String,
    pub lineage_id: String,
    pub target_replica_id: HaId,
    pub next_writer_epoch: u64,
    pub issued_at: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedRestoreAuthorization {
    pub authorization: RestoreAuthorization,
    pub approvals: Vec<HaApproval>,
}

pub fn verify_restore_authorization(
    policy: &HaTrustPolicy,
    backup: &CertifiedCellBackup,
    restore: &CertifiedRestoreAuthorization,
    now: i64,
) -> Result<()> {
    let backup_digest = verify_cell_backup(policy, backup)?;
    let authorization = &restore.authorization;
    if authorization.format != RESTORE_AUTHORIZATION_FORMAT
        || authorization.backup_digest != backup_digest
        || authorization.cell_id != backup.manifest.cell_id
        || authorization.group_id != backup.manifest.group_id
        || authorization.lineage_id != backup.manifest.lineage_id
        || authorization.next_writer_epoch < backup.manifest.writer_epoch.saturating_add(1)
        || authorization.expires_at <= authorization.issued_at
        || authorization
            .expires_at
            .saturating_sub(authorization.issued_at)
            > MAX_RESTORE_AUTHORIZATION_SECONDS
        || now < authorization.issued_at
        || now > authorization.expires_at
    {
        return Err(HaError::Backup(
            "restore authorization is expired or outside exact backup lineage".to_string(),
        ));
    }
    verify_quorum(
        policy,
        HaAuthorityRole::Recovery,
        RESTORE_DOMAIN,
        authorization,
        &restore.approvals,
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HaDrillEvidence {
    pub format: String,
    pub evidence_id: HaId,
    pub cell_id: HaId,
    pub group_id: String,
    pub topology_policy_digest: HaDigest,
    pub started_at: i64,
    pub completed_at: i64,
    pub observed_rpo_commits: u64,
    pub observed_rto_millis: u64,
    pub old_primary_rebuilt: bool,
    pub backup_restore_verified: bool,
    pub evidence_artifact_digest: HaDigest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CertifiedHaDrillEvidence {
    pub evidence: HaDrillEvidence,
    pub approvals: Vec<HaApproval>,
}

pub fn verify_ha_drill(policy: &HaTrustPolicy, drill: &CertifiedHaDrillEvidence) -> Result<()> {
    policy.validate()?;
    let evidence = &drill.evidence;
    if evidence.format != HA_DRILL_EVIDENCE_FORMAT
        || evidence.completed_at < evidence.started_at
        || evidence.observed_rpo_commits > policy.topology.maximum_rpo_commits
        || evidence.observed_rto_millis > policy.topology.maximum_rto_millis
        || !evidence.old_primary_rebuilt
        || !evidence.backup_restore_verified
        || evidence.topology_policy_digest != document_digest(&policy.topology)?
    {
        return Err(HaError::Invalid(
            "HA drill does not meet the signed topology RPO/RTO and recovery policy".to_string(),
        ));
    }
    verify_quorum(
        policy,
        HaAuthorityRole::Auditor,
        DRILL_DOMAIN,
        evidence,
        &drill.approvals,
    )
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailoverPhase {
    FenceOldWriter,
    EstablishDurableSequence,
    ActivateTarget,
    PublishRoute,
    RebuildOldPrimary,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum FailoverAction {
    ObtainFence {
        source_replica_id: HaId,
        writer_epoch: u64,
    },
    SelectDurableSequence {
        minimum_commit_seq: u64,
    },
    Activate {
        target_replica_id: HaId,
        writer_epoch: u64,
    },
    PublishRoute {
        target_replica_id: HaId,
        writer_epoch: u64,
    },
    Rebuild {
        old_primary_replica_id: HaId,
        from_replica_id: HaId,
        writer_epoch: u64,
    },
    None,
}

#[derive(Clone, Debug)]
pub struct CellFailoverSupervisor {
    cell_id: HaId,
    group_id: String,
    old_primary: HaId,
    target: HaId,
    previous_epoch: u64,
    previous_durable_commit_seq: u64,
    accepted_durable_commit_seq: Option<u64>,
    next_epoch_digest: Option<HaDigest>,
    phase: FailoverPhase,
}

impl CellFailoverSupervisor {
    pub fn begin(
        cell_id: HaId,
        group_id: String,
        old_primary: HaId,
        target: HaId,
        previous_epoch: u64,
        previous_durable_commit_seq: u64,
    ) -> Result<Self> {
        require_label(&group_id, "failover group_id")?;
        if old_primary == target || previous_epoch == 0 {
            return Err(HaError::Supervisor(
                "failover target must differ from a positive-epoch primary".to_string(),
            ));
        }
        Ok(Self {
            cell_id,
            group_id,
            old_primary,
            target,
            previous_epoch,
            previous_durable_commit_seq,
            accepted_durable_commit_seq: None,
            next_epoch_digest: None,
            phase: FailoverPhase::FenceOldWriter,
        })
    }

    pub fn phase(&self) -> FailoverPhase {
        self.phase
    }

    pub fn next_action(&self) -> FailoverAction {
        match self.phase {
            FailoverPhase::FenceOldWriter => FailoverAction::ObtainFence {
                source_replica_id: self.old_primary.clone(),
                writer_epoch: self.previous_epoch,
            },
            FailoverPhase::EstablishDurableSequence => FailoverAction::SelectDurableSequence {
                minimum_commit_seq: self.previous_durable_commit_seq,
            },
            FailoverPhase::ActivateTarget => FailoverAction::Activate {
                target_replica_id: self.target.clone(),
                writer_epoch: self.previous_epoch.saturating_add(1),
            },
            FailoverPhase::PublishRoute => FailoverAction::PublishRoute {
                target_replica_id: self.target.clone(),
                writer_epoch: self.previous_epoch.saturating_add(1),
            },
            FailoverPhase::RebuildOldPrimary => FailoverAction::Rebuild {
                old_primary_replica_id: self.old_primary.clone(),
                from_replica_id: self.target.clone(),
                writer_epoch: self.previous_epoch.saturating_add(1),
            },
            FailoverPhase::Complete => FailoverAction::None,
        }
    }

    pub fn old_writer_fenced(&mut self) -> Result<()> {
        self.require_phase(FailoverPhase::FenceOldWriter)?;
        self.phase = FailoverPhase::EstablishDurableSequence;
        Ok(())
    }

    pub fn durable_sequence_selected(&mut self, commit_seq: u64) -> Result<()> {
        self.require_phase(FailoverPhase::EstablishDurableSequence)?;
        if commit_seq < self.previous_durable_commit_seq {
            return Err(HaError::Supervisor(
                "failover durable sequence would move backwards".to_string(),
            ));
        }
        self.accepted_durable_commit_seq = Some(commit_seq);
        self.phase = FailoverPhase::ActivateTarget;
        Ok(())
    }

    pub fn target_activated(&mut self, activation: &VerifiedHaActivation) -> Result<()> {
        self.require_phase(FailoverPhase::ActivateTarget)?;
        if activation.epoch.cell_id != self.cell_id
            || activation.epoch.group_id != self.group_id
            || activation.epoch.primary_replica_id != self.target
            || activation.epoch.writer_epoch != self.previous_epoch.saturating_add(1)
            || activation.epoch.accepted_durable_commit_seq
                != self.accepted_durable_commit_seq.unwrap_or(u64::MAX)
            || activation.lease.role != ReplicaRole::Primary
        {
            return Err(HaError::Supervisor(
                "target activation does not match the failover decision".to_string(),
            ));
        }
        self.next_epoch_digest = Some(activation.epoch_digest.clone());
        self.phase = FailoverPhase::PublishRoute;
        Ok(())
    }

    pub fn route_published(&mut self, epoch_digest: &HaDigest) -> Result<()> {
        self.require_phase(FailoverPhase::PublishRoute)?;
        if self.next_epoch_digest.as_ref() != Some(epoch_digest) {
            return Err(HaError::Supervisor(
                "route publication is not bound to the activated writer epoch".to_string(),
            ));
        }
        self.phase = FailoverPhase::RebuildOldPrimary;
        Ok(())
    }

    pub fn old_primary_rebuilt(
        &mut self,
        replica_id: &HaId,
        writer_epoch: u64,
        durable_commit_seq: u64,
    ) -> Result<()> {
        self.require_phase(FailoverPhase::RebuildOldPrimary)?;
        if replica_id != &self.old_primary
            || writer_epoch != self.previous_epoch.saturating_add(1)
            || durable_commit_seq < self.accepted_durable_commit_seq.unwrap_or(u64::MAX)
        {
            return Err(HaError::Supervisor(
                "old-primary rebuild is not from the winning writer history".to_string(),
            ));
        }
        self.phase = FailoverPhase::Complete;
        Ok(())
    }

    fn require_phase(&self, expected: FailoverPhase) -> Result<()> {
        if self.phase != expected {
            return Err(HaError::Supervisor(format!(
                "failover event requires {expected:?}, current phase is {:?}",
                self.phase
            )));
        }
        Ok(())
    }
}

/// Create a Lease-authority approval for a writer epoch. Verification still
/// checks that `key_id` occupies a Lease role in the pinned trust policy.
pub fn sign_writer_epoch_approval(
    key_id: impl Into<String>,
    statement: &WriterEpochStatement,
    signing_key: &SigningKey,
) -> Result<HaApproval> {
    sign_approval(WRITER_EPOCH_DOMAIN, key_id, statement, signing_key)
}

/// Create a Lease-authority approval for one short-lived replica lease.
pub fn sign_replica_lease_approval(
    key_id: impl Into<String>,
    statement: &ReplicaLeaseStatement,
    signing_key: &SigningKey,
) -> Result<HaApproval> {
    sign_approval(REPLICA_LEASE_DOMAIN, key_id, statement, signing_key)
}

/// Create a Recovery-authority approval for an immutable Cell backup.
pub fn sign_cell_backup_approval(
    key_id: impl Into<String>,
    manifest: &CellBackupManifest,
    signing_key: &SigningKey,
) -> Result<HaApproval> {
    sign_approval(BACKUP_DOMAIN, key_id, manifest, signing_key)
}

/// Create a Recovery-authority approval for a bounded restore operation.
pub fn sign_restore_authorization_approval(
    key_id: impl Into<String>,
    authorization: &RestoreAuthorization,
    signing_key: &SigningKey,
) -> Result<HaApproval> {
    sign_approval(RESTORE_DOMAIN, key_id, authorization, signing_key)
}

/// Create an Auditor-authority approval for measured HA drill evidence.
pub fn sign_ha_drill_approval(
    key_id: impl Into<String>,
    evidence: &HaDrillEvidence,
    signing_key: &SigningKey,
) -> Result<HaApproval> {
    sign_approval(DRILL_DOMAIN, key_id, evidence, signing_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

    #[derive(Debug)]
    struct ManualClock(AtomicI64);

    impl HaClock for ManualClock {
        fn unix_timestamp(&self) -> i64 {
            self.0.load(AtomicOrdering::SeqCst)
        }
    }

    struct Keys {
        lease_a: SigningKey,
        lease_b: SigningKey,
        lease_c: SigningKey,
        recovery_a: SigningKey,
        recovery_b: SigningKey,
        auditor: SigningKey,
        auditor_b: SigningKey,
        fence: SigningKey,
    }

    fn keys() -> Keys {
        Keys {
            lease_a: SigningKey::from_bytes(&[1; 32]),
            lease_b: SigningKey::from_bytes(&[2; 32]),
            lease_c: SigningKey::from_bytes(&[3; 32]),
            recovery_a: SigningKey::from_bytes(&[4; 32]),
            recovery_b: SigningKey::from_bytes(&[5; 32]),
            auditor: SigningKey::from_bytes(&[6; 32]),
            auditor_b: SigningKey::from_bytes(&[7; 32]),
            fence: SigningKey::from_bytes(&[8; 32]),
        }
    }

    fn id(byte: u8) -> HaId {
        HaId::parse(Uuid::from_bytes([byte; 16]).to_string(), "fixture").unwrap()
    }

    fn policy(keys: &Keys) -> HaTrustPolicy {
        let authority = |key_id: &str, role, key: &SigningKey| HaAuthorityKey {
            key_id: key_id.to_string(),
            role,
            public_key: hex::encode(key.verifying_key().as_bytes()),
        };
        HaTrustPolicy {
            format: HA_TRUST_POLICY_FORMAT.to_string(),
            policy_id: "cell-ha-production".to_string(),
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
            maximum_replica_lease_seconds: 60,
            topology: HaTopologyPolicy {
                mode: HaDurabilityMode::LocalSynchronousRemoteAsynchronous,
                replica_count: 3,
                voting_replicas: 3,
                maximum_rpo_commits: 64,
                maximum_rto_millis: 30_000,
            },
        }
    }

    fn approvals<T: Serialize>(
        domain: &[u8],
        document: &T,
        pairs: &[(&str, &SigningKey)],
    ) -> Vec<HaApproval> {
        pairs
            .iter()
            .map(|(id, key)| sign_approval(domain, *id, document, key).unwrap())
            .collect()
    }

    fn initial_activation(
        keys: &Keys,
    ) -> (
        HaTrustPolicy,
        CertifiedWriterEpoch,
        CertifiedReplicaLease,
        HaActivationContext,
    ) {
        let policy = policy(keys);
        let epoch_statement = WriterEpochStatement {
            format: WRITER_EPOCH_FORMAT.to_string(),
            cell_id: id(10),
            group_id: "group-a".to_string(),
            writer_epoch: 1,
            primary_replica_id: id(11),
            primary_fence_public_key: hex::encode(keys.fence.verifying_key().as_bytes()),
            key_epoch: 7,
            manifest_generation: 9,
            manifest_digest: HaDigest::of_bytes(b"manifest"),
            accepted_durable_commit_seq: 42,
            activated_at: 100,
            predecessor_epoch_digest: None,
            predecessor_primary_lease_digest: None,
            fence_evidence: None,
        };
        let epoch = CertifiedWriterEpoch {
            approvals: approvals(
                WRITER_EPOCH_DOMAIN,
                &epoch_statement,
                &[("lease-a", &keys.lease_a), ("lease-b", &keys.lease_b)],
            ),
            statement: epoch_statement,
        };
        let epoch_digest = document_digest(&epoch.statement).unwrap();
        let lease_statement = ReplicaLeaseStatement {
            format: REPLICA_LEASE_FORMAT.to_string(),
            lease_id: id(12),
            lease_sequence: 1,
            predecessor_lease_digest: None,
            cell_id: id(10),
            group_id: "group-a".to_string(),
            replica_id: id(11),
            replica_public_key: hex::encode(keys.fence.verifying_key().as_bytes()),
            role: ReplicaRole::Primary,
            writer_epoch: 1,
            writer_epoch_digest: epoch_digest,
            key_epoch: 7,
            manifest_generation: 9,
            manifest_digest: HaDigest::of_bytes(b"manifest"),
            local_durable_commit_seq: 42,
            issued_at: 100,
            not_before: 100,
            expires_at: 150,
        };
        let lease = CertifiedReplicaLease {
            approvals: approvals(
                REPLICA_LEASE_DOMAIN,
                &lease_statement,
                &[("lease-a", &keys.lease_a), ("lease-b", &keys.lease_b)],
            ),
            statement: lease_statement,
        };
        let context = HaActivationContext {
            cell_id: id(10),
            group_id: "group-a".to_string(),
            replica_id: id(11),
            role: ReplicaRole::Primary,
            writer_epoch: 1,
            key_epoch: 7,
            manifest_generation: 9,
            manifest_digest: HaDigest::of_bytes(b"manifest"),
            local_durable_commit_seq: 42,
            now: 110,
        };
        (policy, epoch, lease, context)
    }

    #[test]
    fn quorum_certified_primary_is_fenced_at_the_commit_boundary() {
        let keys = keys();
        let (policy, epoch, lease, context) = initial_activation(&keys);
        let activation =
            verify_ha_activation(&policy, &epoch, &lease, None, None, &context).unwrap();
        let clock = Arc::new(ManualClock(AtomicI64::new(110)));
        let fence = CellHaCommitFence::new(Arc::new(policy), activation, clock.clone()).unwrap();
        let ticket = fence
            .admit(&CommitAdmissionIntent {
                transaction_id: 7,
                mutations: Vec::new(),
            })
            .unwrap();
        fence.local_applied(&ticket, 1).unwrap();
        clock.0.store(151, AtomicOrdering::SeqCst);
        assert!(fence
            .admit(&CommitAdmissionIntent {
                transaction_id: 8,
                mutations: Vec::new(),
            })
            .is_err());
    }

    #[test]
    fn one_lease_authority_cannot_select_a_writer() {
        let keys = keys();
        let (policy, epoch, mut lease, context) = initial_activation(&keys);
        lease.approvals.truncate(1);
        assert!(matches!(
            verify_ha_activation(&policy, &epoch, &lease, None, None, &context),
            Err(HaError::Quorum(_))
        ));
    }

    #[test]
    fn topology_cannot_claim_zero_rpo_without_synchronous_commit_replication() {
        let keys = keys();
        let mut policy = policy(&keys);
        policy.topology.mode = HaDurabilityMode::SynchronousQuorum;
        policy.topology.maximum_rpo_commits = 0;
        assert!(matches!(policy.validate(), Err(HaError::Invalid(_))));

        policy.topology.mode = HaDurabilityMode::Asynchronous;
        assert!(matches!(policy.validate(), Err(HaError::Invalid(_))));
    }

    #[test]
    fn promotion_waits_for_expiry_or_exact_old_primary_ack() {
        let keys = keys();
        let (policy, previous_epoch, previous_lease, _) = initial_activation(&keys);
        let previous_epoch_digest = document_digest(&previous_epoch.statement).unwrap();
        let previous_lease_digest = document_digest(&previous_lease.statement).unwrap();
        let next_statement = WriterEpochStatement {
            format: WRITER_EPOCH_FORMAT.to_string(),
            cell_id: id(10),
            group_id: "group-a".to_string(),
            writer_epoch: 2,
            primary_replica_id: id(13),
            primary_fence_public_key: hex::encode(
                SigningKey::from_bytes(&[9; 32]).verifying_key().as_bytes(),
            ),
            key_epoch: 7,
            manifest_generation: 10,
            manifest_digest: HaDigest::of_bytes(b"manifest-2"),
            accepted_durable_commit_seq: 42,
            activated_at: 151,
            predecessor_epoch_digest: Some(previous_epoch_digest),
            predecessor_primary_lease_digest: Some(previous_lease_digest),
            fence_evidence: Some(FenceEvidence::PreviousLeaseExpired { observed_at: 151 }),
        };
        assert!(matches!(
            verify_epoch_transition(
                &policy,
                &next_statement,
                Some(&previous_epoch),
                Some(&previous_lease)
            ),
            Err(HaError::Fenced(_))
        ));
        let safe = WriterEpochStatement {
            activated_at: 155,
            fence_evidence: Some(FenceEvidence::PreviousLeaseExpired { observed_at: 155 }),
            ..next_statement
        };
        verify_epoch_transition(&policy, &safe, Some(&previous_epoch), Some(&previous_lease))
            .unwrap();
    }

    #[test]
    fn replication_objects_are_bound_to_both_replicas_and_epochs() {
        let keys = keys();
        let (policy, _epoch, source_lease, _context) = initial_activation(&keys);
        let ciphertext = b"opaque ciphertext".to_vec();
        let object = CellReplicationObject {
            header: ReplicationObjectHeader {
                format: REPLICATION_OBJECT_FORMAT.to_string(),
                kind: ReplicationObjectKind::Commit,
                cell_id: id(10),
                group_id: "group-a".to_string(),
                source_replica_id: id(11),
                destination_replica_id: id(12),
                writer_epoch: 1,
                key_epoch: 7,
                protocol_version: 1,
                commit_seq: 99,
                previous_commit_seq: 98,
                predecessor_object_digest: None,
                plaintext_digest: HaDigest::of_bytes(b"frame"),
                ciphertext_digest: HaDigest::of_bytes(&ciphertext),
            },
            source_lease_digest: document_digest(&source_lease.statement).unwrap(),
            ciphertext,
            signature: String::new(),
        };
        let object = sign_replication_object(object, &keys.fence).unwrap();
        verify_replication_object(&policy, &object, &source_lease, 110).unwrap();
        assert!(object
            .header
            .assert_scope(&id(10), "group-a", &id(11), &id(13), 1, 7)
            .is_err());

        // Possessing another replica key (or the shared Cell data key) cannot
        // impersonate the elected writer's exact short-lived lease.
        let forged = sign_replication_object(
            CellReplicationObject {
                signature: String::new(),
                ..object.clone()
            },
            &keys.auditor,
        )
        .unwrap();
        assert!(matches!(
            verify_replication_object(&policy, &forged, &source_lease, 110),
            Err(HaError::Signature(_))
        ));
    }

    #[test]
    fn durable_state_refuses_epoch_and_commit_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let store = CellHaStateStore::new(directory.path().join("state.cbor"));
        let state = CellHaState {
            format: HA_STATE_FORMAT.to_string(),
            cell_id: id(10),
            group_id: "group-a".to_string(),
            replica_id: id(11),
            writer_epoch: 7,
            writer_epoch_digest: HaDigest::of_bytes(b"epoch-7"),
            last_applied_commit_seq: 100,
            last_replication_object_digest: None,
            backup_head_digest: None,
        };
        store.advance(&state).unwrap();
        // Debris at the fixed staging name used by an older implementation
        // must not permanently strand a healthy witness after a crash.
        fs::write(directory.path().join("state.cbor.new"), b"stale").unwrap();
        store
            .advance(&CellHaState {
                last_applied_commit_seq: 101,
                ..state.clone()
            })
            .unwrap();
        let stale = CellHaState {
            last_applied_commit_seq: 99,
            ..state
        };
        assert!(matches!(store.advance(&stale), Err(HaError::Fenced(_))));
    }

    #[test]
    fn auditor_certifies_only_drills_inside_the_signed_rpo_rto() {
        let keys = keys();
        let policy = policy(&keys);
        let evidence = HaDrillEvidence {
            format: HA_DRILL_EVIDENCE_FORMAT.to_string(),
            evidence_id: id(20),
            cell_id: id(10),
            group_id: "group-a".to_string(),
            topology_policy_digest: document_digest(&policy.topology).unwrap(),
            started_at: 100,
            completed_at: 110,
            observed_rpo_commits: 10,
            observed_rto_millis: 2_000,
            old_primary_rebuilt: true,
            backup_restore_verified: true,
            evidence_artifact_digest: HaDigest::of_bytes(b"drill-artifact"),
        };
        let certified = CertifiedHaDrillEvidence {
            approvals: vec![sign_ha_drill_approval("auditor-a", &evidence, &keys.auditor).unwrap()],
            evidence: evidence.clone(),
        };
        verify_ha_drill(&policy, &certified).unwrap();

        let outside_rpo = HaDrillEvidence {
            observed_rpo_commits: policy.topology.maximum_rpo_commits + 1,
            ..evidence
        };
        let outside_rpo = CertifiedHaDrillEvidence {
            approvals: vec![
                sign_ha_drill_approval("auditor-a", &outside_rpo, &keys.auditor).unwrap(),
            ],
            evidence: outside_rpo,
        };
        assert!(matches!(
            verify_ha_drill(&policy, &outside_rpo),
            Err(HaError::Invalid(_))
        ));
    }

    #[test]
    fn failover_routes_only_after_fence_durable_selection_and_activation() {
        let mut supervisor =
            CellFailoverSupervisor::begin(id(10), "group-a".to_string(), id(11), id(12), 4, 90)
                .unwrap();
        assert_eq!(supervisor.phase(), FailoverPhase::FenceOldWriter);
        supervisor.old_writer_fenced().unwrap();
        assert!(supervisor.durable_sequence_selected(89).is_err());
        supervisor.durable_sequence_selected(90).unwrap();
        assert_eq!(supervisor.phase(), FailoverPhase::ActivateTarget);
    }
}
