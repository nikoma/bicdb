//! Destination-owned admission and progress for bounded anti-entropy repair.
//!
//! Repair batches are derived from a completed quorum digest report, confined
//! to one divergent token bucket, and applied idempotently under the original
//! range write fence. This module intentionally does not declare a replica
//! healthy: a later fresh digest must certify the repaired root.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::CommitAdmissionMutation;
use crate::distribution::{distribution_key_token, ClusterId, ClusterNodeId, RangeId};
use crate::distribution_anti_entropy::{RangeDigestLimits, RangeDigestTransport};
use crate::distribution_anti_entropy_run::{RangeDigestRunOutcome, RangeDigestRunReport};
use crate::error::{BicDbError, Result};

pub const RANGE_DIGEST_REPAIR_FORMAT_VERSION: u32 = 1;

fn repair_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range anti-entropy repair: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairLimits {
    pub digest: RangeDigestLimits,
    pub max_mutations_per_batch: usize,
    pub max_bytes_per_batch: usize,
    pub max_record_bytes: usize,
    pub max_state_bytes: u64,
    pub max_retained_sessions: usize,
}

impl Default for RangeDigestRepairLimits {
    fn default() -> Self {
        Self {
            digest: RangeDigestLimits::default(),
            max_mutations_per_batch: 1_024,
            max_bytes_per_batch: 8 * 1024 * 1024,
            max_record_bytes: 8 * 1024 * 1024,
            max_state_bytes: 1024 * 1024,
            max_retained_sessions: 4_096,
        }
    }
}

impl RangeDigestRepairLimits {
    pub fn validate(&self) -> Result<()> {
        self.digest.validate()?;
        if !(1..=1_000_000).contains(&self.max_mutations_per_batch)
            || !(4 * 1024..=256 * 1024 * 1024).contains(&self.max_bytes_per_batch)
            || self.max_record_bytes == 0
            || self.max_record_bytes > self.max_bytes_per_batch
            || !(4 * 1024..=16 * 1024 * 1024).contains(&self.max_state_bytes)
            || !(1..=4_096).contains(&self.max_retained_sessions)
        {
            return Err(repair_error(
                "mutation, byte, state, or retained-session limit is outside bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeDigestRepairBatch {
    pub format_version: u32,
    pub repair_id: Uuid,
    pub digest_session_id: Uuid,
    pub cluster_id: ClusterId,
    pub source_node_id: ClusterNodeId,
    pub destination_node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub bucket: u32,
    pub source_root_sha256: String,
    pub destination_root_sha256: String,
    pub authority: RangeDigestRunReport,
    pub sequence: u64,
    pub previous_batch_sha256: Option<String>,
    pub mutations: Vec<CommitAdmissionMutation>,
    pub serialized_mutation_bytes: usize,
    /// The external merge has emitted every mutation for this bucket. This is
    /// not a health certificate; it moves the destination into an explicit
    /// awaiting-verification state.
    pub input_complete: bool,
    pub checksum_sha256: String,
}

impl RangeDigestRepairBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        repair_id: Uuid,
        source_node_id: ClusterNodeId,
        destination_node_id: ClusterNodeId,
        bucket: u32,
        sequence: u64,
        previous_batch_sha256: Option<String>,
        mutations: Vec<CommitAdmissionMutation>,
        serialized_mutation_bytes: usize,
        input_complete: bool,
        authority: RangeDigestRunReport,
        limits: &RangeDigestRepairLimits,
    ) -> Result<Self> {
        let source_root_sha256 = authority
            .certified_source_root_sha256
            .clone()
            .ok_or_else(|| repair_error("digest report has no certified source root"))?;
        let destination_root_sha256 = authority
            .root_evidence
            .iter()
            .find(|evidence| evidence.node_ids.contains(&destination_node_id))
            .map(|evidence| evidence.root_sha256.clone())
            .ok_or_else(|| repair_error("destination has no digest root evidence"))?;
        let mut batch = Self {
            format_version: RANGE_DIGEST_REPAIR_FORMAT_VERSION,
            repair_id,
            digest_session_id: authority.session_id,
            cluster_id: authority.cluster_id.clone(),
            source_node_id,
            destination_node_id,
            range_id: authority.range_id,
            range_epoch: authority.range_epoch,
            resolved_through: authority.resolved_through,
            bucket,
            source_root_sha256,
            destination_root_sha256,
            authority,
            sequence,
            previous_batch_sha256,
            mutations,
            serialized_mutation_bytes,
            input_complete,
            checksum_sha256: String::new(),
        };
        batch.checksum_sha256 = batch.calculate_checksum()?;
        batch.validate(limits)?;
        Ok(batch)
    }

    pub fn validate(&self, limits: &RangeDigestRepairLimits) -> Result<()> {
        limits.validate()?;
        self.authority.validate()?;
        validate_sha256(&self.source_root_sha256)?;
        validate_sha256(&self.destination_root_sha256)?;
        if self.format_version != RANGE_DIGEST_REPAIR_FORMAT_VERSION
            || self.repair_id.is_nil()
            || self.digest_session_id != self.authority.session_id
            || self.cluster_id != self.authority.cluster_id
            || self.source_node_id == self.destination_node_id
            || self.range_id != self.authority.range_id
            || self.range_epoch != self.authority.range_epoch
            || self.resolved_through != self.authority.resolved_through
            || self.bucket as usize >= limits.digest.bucket_count
            || !self.authority.divergent_buckets.contains(&self.bucket)
            || self.sequence == 0
            || (self.sequence == 1) != self.previous_batch_sha256.is_none()
            || self.mutations.len() > limits.max_mutations_per_batch
            || (self.mutations.is_empty() && !self.input_complete)
        {
            return Err(repair_error(
                "batch identity, authority, sequence, or bounds are invalid",
            ));
        }
        if self.authority.outcome != RangeDigestRunOutcome::DivergentCertifiedSource
            || self.authority.certified_source_root_sha256.as_deref()
                != Some(self.source_root_sha256.as_str())
            || self.destination_root_sha256 == self.source_root_sha256
        {
            return Err(repair_error(
                "batch does not repair a divergent root from certified evidence",
            ));
        }
        let source_is_certified = self.authority.root_evidence.iter().any(|evidence| {
            evidence.root_sha256 == self.source_root_sha256
                && evidence.node_ids.contains(&self.source_node_id)
                && evidence.node_ids.len() >= self.authority.required_quorum
        });
        let destination_is_divergent = self.authority.root_evidence.iter().any(|evidence| {
            evidence.root_sha256 == self.destination_root_sha256
                && evidence.node_ids.contains(&self.destination_node_id)
        });
        if !source_is_certified || !destination_is_divergent {
            return Err(repair_error(
                "source or destination is absent from the required root evidence",
            ));
        }
        if let Some(previous) = &self.previous_batch_sha256 {
            validate_sha256(previous)?;
        }

        let mut actual_bytes = 0_usize;
        let mut previous_key = None::<(&str, &str)>;
        for mutation in &self.mutations {
            if mutation.collection.is_empty()
                || mutation.collection.len() > 1024
                || mutation.record_id.is_empty()
                || mutation.record_id.len() > 16 * 1024
                || mutation
                    .record
                    .as_ref()
                    .is_some_and(|record| record.id != mutation.record_id)
            {
                return Err(repair_error("repair mutation identity is invalid"));
            }
            let key = (mutation.collection.as_str(), mutation.record_id.as_str());
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(repair_error(
                    "repair mutations are duplicated or not canonically ordered",
                ));
            }
            if token_bucket(
                distribution_key_token(&mutation.collection, &mutation.record_id),
                limits.digest.bucket_count,
            ) != self.bucket as usize
            {
                return Err(repair_error("repair mutation belongs to another bucket"));
            }
            let bytes = serde_json::to_vec(mutation)?.len();
            if bytes > limits.max_record_bytes {
                return Err(repair_error("repair mutation exceeds its byte bound"));
            }
            actual_bytes = actual_bytes
                .checked_add(bytes)
                .ok_or_else(|| repair_error("repair mutation byte total overflow"))?;
            if actual_bytes > limits.max_bytes_per_batch {
                return Err(repair_error("repair batch exceeds its byte bound"));
            }
            previous_key = Some(key);
        }
        if actual_bytes != self.serialized_mutation_bytes {
            return Err(repair_error(
                "repair batch declared mutation byte total is incorrect",
            ));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(repair_error("repair batch checksum mismatch"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            repair_id: Uuid,
            digest_session_id: Uuid,
            cluster_id: &'a ClusterId,
            source_node_id: &'a ClusterNodeId,
            destination_node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            resolved_through: u64,
            bucket: u32,
            source_root_sha256: &'a str,
            destination_root_sha256: &'a str,
            authority: &'a RangeDigestRunReport,
            sequence: u64,
            previous_batch_sha256: &'a Option<String>,
            mutations: &'a [CommitAdmissionMutation],
            serialized_mutation_bytes: usize,
            input_complete: bool,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            repair_id: self.repair_id,
            digest_session_id: self.digest_session_id,
            cluster_id: &self.cluster_id,
            source_node_id: &self.source_node_id,
            destination_node_id: &self.destination_node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            resolved_through: self.resolved_through,
            bucket: self.bucket,
            source_root_sha256: &self.source_root_sha256,
            destination_root_sha256: &self.destination_root_sha256,
            authority: &self.authority,
            sequence: self.sequence,
            previous_batch_sha256: &self.previous_batch_sha256,
            mutations: &self.mutations,
            serialized_mutation_bytes: self.serialized_mutation_bytes,
            input_complete: self.input_complete,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairState {
    pub format_version: u32,
    pub repair_id: Uuid,
    pub digest_session_id: Uuid,
    pub cluster_id: ClusterId,
    pub source_node_id: ClusterNodeId,
    pub destination_node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub bucket: u32,
    pub source_root_sha256: String,
    pub destination_root_sha256: String,
    pub authority_checksum_sha256: String,
    pub applied_batches: u64,
    pub applied_mutations: u64,
    pub last_batch_sha256: Option<String>,
    pub input_complete: bool,
    pub checksum_sha256: String,
}

impl RangeDigestRepairState {
    pub(crate) fn create(batch: &RangeDigestRepairBatch) -> Result<Self> {
        let mut state = Self {
            format_version: RANGE_DIGEST_REPAIR_FORMAT_VERSION,
            repair_id: batch.repair_id,
            digest_session_id: batch.digest_session_id,
            cluster_id: batch.cluster_id.clone(),
            source_node_id: batch.source_node_id.clone(),
            destination_node_id: batch.destination_node_id.clone(),
            range_id: batch.range_id,
            range_epoch: batch.range_epoch,
            resolved_through: batch.resolved_through,
            bucket: batch.bucket,
            source_root_sha256: batch.source_root_sha256.clone(),
            destination_root_sha256: batch.destination_root_sha256.clone(),
            authority_checksum_sha256: batch.authority.checksum_sha256.clone(),
            applied_batches: 0,
            applied_mutations: 0,
            last_batch_sha256: None,
            input_complete: false,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        Ok(state)
    }

    pub(crate) fn validate_batch_identity(&self, batch: &RangeDigestRepairBatch) -> Result<()> {
        if self.repair_id != batch.repair_id
            || self.digest_session_id != batch.digest_session_id
            || self.cluster_id != batch.cluster_id
            || self.source_node_id != batch.source_node_id
            || self.destination_node_id != batch.destination_node_id
            || self.range_id != batch.range_id
            || self.range_epoch != batch.range_epoch
            || self.resolved_through != batch.resolved_through
            || self.bucket != batch.bucket
            || self.source_root_sha256 != batch.source_root_sha256
            || self.destination_root_sha256 != batch.destination_root_sha256
            || self.authority_checksum_sha256 != batch.authority.checksum_sha256
        {
            return Err(repair_error(
                "repair batch identity changed within a session",
            ));
        }
        Ok(())
    }

    pub(crate) fn record_applied(
        &mut self,
        batch: &RangeDigestRepairBatch,
        limits: &RangeDigestRepairLimits,
    ) -> Result<()> {
        self.validate_batch_identity(batch)?;
        if self.input_complete
            || batch.sequence != self.applied_batches.saturating_add(1)
            || batch.previous_batch_sha256 != self.last_batch_sha256
        {
            return Err(repair_error(
                "repair batch is out of order or follows completed input",
            ));
        }
        self.applied_batches = batch.sequence;
        self.applied_mutations = self
            .applied_mutations
            .checked_add(batch.mutations.len() as u64)
            .ok_or_else(|| repair_error("repair mutation total overflow"))?;
        self.last_batch_sha256 = Some(batch.checksum_sha256.clone());
        self.input_complete = batch.input_complete;
        self.refresh_checksum()?;
        self.validate(limits)
    }

    pub fn validate(&self, limits: &RangeDigestRepairLimits) -> Result<()> {
        limits.validate()?;
        if self.format_version != RANGE_DIGEST_REPAIR_FORMAT_VERSION
            || self.repair_id.is_nil()
            || self.digest_session_id.is_nil()
            || self.source_node_id == self.destination_node_id
            || self.range_epoch == 0
            || self.bucket as usize >= limits.digest.bucket_count
            || self.applied_batches == 0 && self.applied_mutations != 0
            || self.applied_batches == 0 && self.last_batch_sha256.is_some()
            || self.applied_batches > 0 && self.last_batch_sha256.is_none()
            || self.input_complete && self.applied_batches == 0
        {
            return Err(repair_error("repair state identity or progress is invalid"));
        }
        validate_sha256(&self.source_root_sha256)?;
        validate_sha256(&self.destination_root_sha256)?;
        validate_sha256(&self.authority_checksum_sha256)?;
        if self.source_root_sha256 == self.destination_root_sha256 {
            return Err(repair_error(
                "repair state source and destination roots match",
            ));
        }
        if let Some(last) = &self.last_batch_sha256 {
            validate_sha256(last)?;
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(repair_error("repair state checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > limits.max_state_bytes {
            return Err(repair_error("repair state exceeds its byte bound"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            repair_id: Uuid,
            digest_session_id: Uuid,
            cluster_id: &'a ClusterId,
            source_node_id: &'a ClusterNodeId,
            destination_node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            resolved_through: u64,
            bucket: u32,
            source_root_sha256: &'a str,
            destination_root_sha256: &'a str,
            authority_checksum_sha256: &'a str,
            applied_batches: u64,
            applied_mutations: u64,
            last_batch_sha256: &'a Option<String>,
            input_complete: bool,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            repair_id: self.repair_id,
            digest_session_id: self.digest_session_id,
            cluster_id: &self.cluster_id,
            source_node_id: &self.source_node_id,
            destination_node_id: &self.destination_node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            resolved_through: self.resolved_through,
            bucket: self.bucket,
            source_root_sha256: &self.source_root_sha256,
            destination_root_sha256: &self.destination_root_sha256,
            authority_checksum_sha256: &self.authority_checksum_sha256,
            applied_batches: self.applied_batches,
            applied_mutations: self.applied_mutations,
            last_batch_sha256: &self.last_batch_sha256,
            input_complete: self.input_complete,
        })
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }
}

pub fn save_range_digest_repair_state(
    path: impl AsRef<Path>,
    state: &RangeDigestRepairState,
    limits: &RangeDigestRepairLimits,
    fsync: bool,
) -> Result<()> {
    state.validate(limits)?;
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() as u64 > limits.max_state_bytes {
        return Err(repair_error("repair state exceeds its write bound"));
    }
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_range_digest_repair_state(
    path: impl AsRef<Path>,
    limits: &RangeDigestRepairLimits,
) -> Result<RangeDigestRepairState> {
    limits.validate()?;
    let path = path.as_ref();
    let file = open_repair_state_no_follow(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limits.max_state_bytes {
        return Err(repair_error(
            "repair state file is unsafe or outside its bound",
        ));
    }
    let length = metadata.len();
    let mut bytes = Vec::with_capacity(
        usize::try_from(length).map_err(|_| repair_error("repair state is too large"))?,
    );
    file.take(limits.max_state_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > limits.max_state_bytes {
        return Err(repair_error("repair state changed or grew while reading"));
    }
    let state: RangeDigestRepairState = serde_json::from_slice(&bytes)?;
    state.validate(limits)?;
    Ok(state)
}

fn open_repair_state_no_follow(path: &Path) -> Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(Into::into)
    }
    #[cfg(not(unix))]
    {
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(repair_error(
                "repair state file must not be a symbolic link",
            ));
        }
        OpenOptions::new().read(true).open(path).map_err(Into::into)
    }
}

#[allow(clippy::too_many_arguments)]
pub trait RangeDigestRepairTransport: RangeDigestTransport {
    fn apply_range_digest_repair(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        batch: &RangeDigestRepairBatch,
        limits: &RangeDigestRepairLimits,
        now_ms: u64,
    ) -> Result<RangeDigestRepairState>;
}

fn token_bucket(token: u64, bucket_count: usize) -> usize {
    let shift = u64::BITS - bucket_count.trailing_zeros();
    (token >> shift) as usize
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(repair_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::distribution_anti_entropy_run::RangeDigestRootEvidence;
    use crate::Record;

    fn authority(
        session_id: Uuid,
        cluster_id: ClusterId,
        range_id: RangeId,
        source: &ClusterNodeId,
        source_peer: &ClusterNodeId,
        destination: &ClusterNodeId,
        bucket: u32,
    ) -> RangeDigestRunReport {
        let source_root = "1".repeat(64);
        let destination_root = "e".repeat(64);
        RangeDigestRunReport::create(
            session_id,
            cluster_id,
            range_id,
            3,
            9,
            2,
            RangeDigestRunOutcome::DivergentCertifiedSource,
            vec![
                RangeDigestRootEvidence {
                    root_sha256: source_root.clone(),
                    node_ids: vec![source.clone(), source_peer.clone()],
                },
                RangeDigestRootEvidence {
                    root_sha256: destination_root,
                    node_ids: vec![destination.clone()],
                },
            ],
            Some(source_root),
            vec![bucket],
        )
        .unwrap()
    }

    #[test]
    fn certified_batches_are_bucket_bounded_chained_and_restart_durable() {
        let limits = RangeDigestRepairLimits {
            digest: RangeDigestLimits {
                bucket_count: 16,
                ..RangeDigestLimits::default()
            },
            ..RangeDigestRepairLimits::default()
        };
        let cluster_id = ClusterId::new("repair-cluster").unwrap();
        let range_id = RangeId::new(4).unwrap();
        let source = ClusterNodeId::new("node-a").unwrap();
        let source_peer = ClusterNodeId::new("node-b").unwrap();
        let destination = ClusterNodeId::new("node-c").unwrap();
        let record_id = "item-007";
        let bucket = token_bucket(
            distribution_key_token("items", record_id),
            limits.digest.bucket_count,
        ) as u32;
        let authority = authority(
            Uuid::new_v4(),
            cluster_id,
            range_id,
            &source,
            &source_peer,
            &destination,
            bucket,
        );
        let mutation = CommitAdmissionMutation {
            collection: "items".to_string(),
            record_id: record_id.to_string(),
            record: Some(Record::new(record_id).with_metadata(json!({"value": 7}))),
        };
        let mutation_bytes = serde_json::to_vec(&mutation).unwrap().len();
        let first = RangeDigestRepairBatch::create(
            Uuid::new_v4(),
            source,
            destination,
            bucket,
            1,
            None,
            vec![mutation],
            mutation_bytes,
            false,
            authority.clone(),
            &limits,
        )
        .unwrap();
        let other_id = (0..10_000)
            .map(|value| format!("other-{value}"))
            .find(|candidate| {
                token_bucket(
                    distribution_key_token("items", candidate),
                    limits.digest.bucket_count,
                ) != bucket as usize
            })
            .unwrap();
        let mut cross_bucket = first.clone();
        cross_bucket.mutations[0].record_id = other_id.clone();
        cross_bucket.mutations[0].record.as_mut().unwrap().id = other_id;
        cross_bucket.serialized_mutation_bytes = serde_json::to_vec(&cross_bucket.mutations[0])
            .unwrap()
            .len();
        cross_bucket.checksum_sha256 = cross_bucket.calculate_checksum().unwrap();
        assert!(cross_bucket.validate(&limits).is_err());

        let mut state = RangeDigestRepairState::create(&first).unwrap();
        state.validate(&limits).unwrap();
        state.record_applied(&first, &limits).unwrap();
        assert_eq!(state.applied_batches, 1);
        assert_eq!(state.applied_mutations, 1);

        let final_batch = RangeDigestRepairBatch::create(
            first.repair_id,
            first.source_node_id.clone(),
            first.destination_node_id.clone(),
            bucket,
            2,
            Some(first.checksum_sha256.clone()),
            Vec::new(),
            0,
            true,
            authority,
            &limits,
        )
        .unwrap();
        state.record_applied(&final_batch, &limits).unwrap();
        assert!(state.input_complete);
        assert_eq!(state.applied_mutations, 1);
        assert!(state.record_applied(&final_batch, &limits).is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repair.json");
        save_range_digest_repair_state(&path, &state, &limits, false).unwrap();
        assert_eq!(
            load_range_digest_repair_state(&path, &limits).unwrap(),
            state
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let link = directory.path().join("repair-link.json");
            symlink(&path, &link).unwrap();
            assert!(load_range_digest_repair_state(&link, &limits).is_err());
        }
        let mut damaged = std::fs::read(&path).unwrap();
        let middle = damaged.len() / 2;
        damaged[middle] ^= 1;
        std::fs::write(&path, damaged).unwrap();
        assert!(load_range_digest_repair_state(&path, &limits).is_err());
    }
}
