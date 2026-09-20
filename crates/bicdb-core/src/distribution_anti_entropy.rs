//! Bounded, crash-resumable replica data digests.
//!
//! The digest is a fixed-fanout Merkle summary. Record hashes are chained into
//! token buckets in deterministic collection/primary-key scan order, and a
//! root binds every bucket plus the range epoch and resolved consensus index.
//! State is checksummed and atomically replaceable between bounded scan calls;
//! neither memory nor the final comparison grows with row count.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{distribution_key_token, ClusterId, ClusterNodeId, RangeId};
use crate::error::{BicDbError, Result};
use crate::record::Record;

pub const RANGE_DIGEST_FORMAT_VERSION: u32 = 1;
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const RECORD_HASH_DOMAIN: &[u8] = b"BICDB-RANGE-DIGEST-RECORD-V1";
const ROOT_HASH_DOMAIN: &[u8] = b"BICDB-RANGE-DIGEST-ROOT-V1";
const CURSOR_ADVANCE_HASH_DOMAIN: &[u8] = b"BICDB-RANGE-DIGEST-CURSOR-ADVANCE-V1";

fn digest_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range anti-entropy: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestLimits {
    /// Fixed Merkle leaf count. A power of two permits direct token-prefix
    /// routing and keeps comparison cost independent of row count.
    pub bucket_count: usize,
    pub max_records_per_batch: usize,
    pub max_bytes_per_batch: usize,
    pub max_record_bytes: usize,
    pub max_state_bytes: u64,
}

impl Default for RangeDigestLimits {
    fn default() -> Self {
        Self {
            bucket_count: 4_096,
            max_records_per_batch: 1_024,
            max_bytes_per_batch: 8 * 1024 * 1024,
            max_record_bytes: 8 * 1024 * 1024,
            max_state_bytes: 16 * 1024 * 1024,
        }
    }
}

impl RangeDigestLimits {
    pub fn validate(&self) -> Result<()> {
        if !self.bucket_count.is_power_of_two()
            || !(16..=65_536).contains(&self.bucket_count)
            || !(1..=1_000_000).contains(&self.max_records_per_batch)
            || !(4 * 1024..=256 * 1024 * 1024).contains(&self.max_bytes_per_batch)
            || self.max_record_bytes == 0
            || self.max_record_bytes > self.max_bytes_per_batch
            || !(4 * 1024..=64 * 1024 * 1024).contains(&self.max_state_bytes)
        {
            return Err(digest_error(
                "bucket, record, byte, or state limits are outside supported bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestBucket {
    pub bucket: u32,
    pub records: u64,
    pub serialized_bytes: u64,
    pub sha256: String,
}

impl RangeDigestBucket {
    pub fn empty(bucket: u32, limits: &RangeDigestLimits) -> Result<Self> {
        limits.validate()?;
        if bucket as usize >= limits.bucket_count {
            return Err(digest_error("digest bucket is outside its fixed fanout"));
        }
        Ok(Self {
            bucket,
            records: 0,
            serialized_bytes: 0,
            sha256: EMPTY_SHA256.to_string(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestState {
    pub format_version: u32,
    pub session_id: Uuid,
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub start_token: u64,
    pub end_token: Option<u64>,
    /// Consensus log prefix that must remain fixed while this digest scans.
    pub resolved_through: u64,
    pub bucket_count: usize,
    pub resume_after_key: Option<String>,
    pub last_collection: Option<String>,
    pub last_record_id: Option<String>,
    pub last_batch_sha256: Option<String>,
    pub scanned_records: u64,
    pub serialized_bytes: u64,
    pub buckets: Vec<RangeDigestBucket>,
    pub completed: bool,
    pub root_sha256: Option<String>,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestManifest {
    pub format_version: u32,
    pub session_id: Uuid,
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub start_token: u64,
    pub end_token: Option<u64>,
    pub resolved_through: u64,
    pub bucket_count: usize,
    pub scanned_records: u64,
    pub serialized_bytes: u64,
    pub buckets: Vec<RangeDigestBucket>,
    pub root_sha256: String,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestDiff {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub local_node_id: ClusterNodeId,
    pub remote_node_id: ClusterNodeId,
    pub local_root_sha256: String,
    pub remote_root_sha256: String,
    pub divergent_buckets: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeDigestBucketScanStep {
    pub session_id: Uuid,
    pub source_node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub bucket: u32,
    pub expected_previous_resume: Option<String>,
    pub resume_after_key: Option<String>,
    pub collection: Option<String>,
    pub records: Vec<Record>,
    pub serialized_record_bytes: usize,
    pub completed: bool,
    pub checksum_sha256: String,
}

impl RangeDigestBucketScanStep {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        session_id: Uuid,
        source_node_id: ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        resolved_through: u64,
        bucket: u32,
        expected_previous_resume: Option<String>,
        resume_after_key: Option<String>,
        collection: Option<String>,
        records: Vec<Record>,
        serialized_record_bytes: usize,
        completed: bool,
        limits: &RangeDigestLimits,
    ) -> Result<Self> {
        let mut step = Self {
            session_id,
            source_node_id,
            range_id,
            range_epoch,
            resolved_through,
            bucket,
            expected_previous_resume,
            resume_after_key,
            collection,
            records,
            serialized_record_bytes,
            completed,
            checksum_sha256: String::new(),
        };
        step.checksum_sha256 = step.calculate_checksum()?;
        step.validate(limits)?;
        Ok(step)
    }

    pub fn validate(&self, limits: &RangeDigestLimits) -> Result<()> {
        limits.validate()?;
        if self.session_id.is_nil()
            || self.range_epoch == 0
            || self.bucket as usize >= limits.bucket_count
            || self.records.len() > limits.max_records_per_batch
            || self
                .expected_previous_resume
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 16 * 1024)
            || self
                .resume_after_key
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 16 * 1024)
            || self
                .collection
                .as_ref()
                .is_some_and(|collection| collection.is_empty() || collection.len() > 1024)
        {
            return Err(digest_error(
                "bucket scan step identity or bounds are invalid",
            ));
        }
        if self.completed {
            if self.resume_after_key.is_some()
                || self.collection.is_some()
                || !self.records.is_empty()
                || self.serialized_record_bytes != 0
            {
                return Err(digest_error("completed bucket scan step carries row data"));
            }
        } else if self.resume_after_key.is_none()
            || self.resume_after_key == self.expected_previous_resume
            || (!self.records.is_empty() && self.collection.is_none())
        {
            return Err(digest_error("bucket scan progress has no forward cursor"));
        }
        let mut actual_bytes = 0_usize;
        let mut previous_id = None::<&str>;
        for record in &self.records {
            if record.id.is_empty()
                || record.id.len() > 16 * 1024
                || previous_id.is_some_and(|previous| previous >= record.id.as_str())
            {
                return Err(digest_error("bucket scan record IDs are not canonical"));
            }
            let bytes = serde_json::to_vec(record)?.len();
            if bytes > limits.max_record_bytes {
                return Err(digest_error("bucket scan record exceeds its byte bound"));
            }
            actual_bytes = actual_bytes
                .checked_add(bytes)
                .ok_or_else(|| digest_error("bucket scan byte total overflow"))?;
            if actual_bytes > limits.max_bytes_per_batch {
                return Err(digest_error("bucket scan step exceeds its byte bound"));
            }
            let collection = self
                .collection
                .as_deref()
                .ok_or_else(|| digest_error("bucket scan records have no collection"))?;
            if range_digest_bucket_for_record(collection, &record.id, limits)? != self.bucket {
                return Err(digest_error(
                    "bucket scan step contains a record from another bucket",
                ));
            }
            previous_id = Some(&record.id);
        }
        if actual_bytes != self.serialized_record_bytes {
            return Err(digest_error("bucket scan declared byte total is incorrect"));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(digest_error("bucket scan step checksum mismatch"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            session_id: Uuid,
            source_node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            resolved_through: u64,
            bucket: u32,
            expected_previous_resume: &'a Option<String>,
            resume_after_key: &'a Option<String>,
            collection: &'a Option<String>,
            records: &'a [Record],
            serialized_record_bytes: usize,
            completed: bool,
        }
        sha256_json(&Payload {
            session_id: self.session_id,
            source_node_id: &self.source_node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            resolved_through: self.resolved_through,
            bucket: self.bucket,
            expected_previous_resume: &self.expected_previous_resume,
            resume_after_key: &self.resume_after_key,
            collection: &self.collection,
            records: &self.records,
            serialized_record_bytes: self.serialized_record_bytes,
            completed: self.completed,
        })
    }
}

pub fn range_digest_bucket_for_record(
    collection: &str,
    record_id: &str,
    limits: &RangeDigestLimits,
) -> Result<u32> {
    limits.validate()?;
    if collection.is_empty() || record_id.is_empty() {
        return Err(digest_error(
            "bucket routing requires collection and record ID",
        ));
    }
    Ok(token_bucket(
        distribution_key_token(collection, record_id),
        limits.bucket_count,
    ) as u32)
}

/// Append one bounded canonical record slice to an independently accumulated
/// digest bucket. Repair certification uses the exact same hash chain as a
/// full range digest so verified bucket evidence can reconstruct a full root.
pub fn append_range_digest_bucket_records(
    bucket: &mut RangeDigestBucket,
    collection: &str,
    records: &[Record],
    limits: &RangeDigestLimits,
) -> Result<()> {
    limits.validate()?;
    if bucket.bucket as usize >= limits.bucket_count
        || collection.is_empty()
        || collection.len() > 1024
        || records.len() > limits.max_records_per_batch
    {
        return Err(digest_error(
            "independent bucket append is outside its bounds",
        ));
    }
    validate_sha256(&bucket.sha256)?;
    let mut bytes = 0_usize;
    let mut previous = None::<&str>;
    for record in records {
        if record.id.is_empty()
            || record.id.len() > 16 * 1024
            || previous.is_some_and(|previous| previous >= record.id.as_str())
            || range_digest_bucket_for_record(collection, &record.id, limits)? != bucket.bucket
        {
            return Err(digest_error(
                "independent bucket records are not canonical or bucket-local",
            ));
        }
        let encoded = serde_json::to_vec(record)?;
        if encoded.len() > limits.max_record_bytes {
            return Err(digest_error(
                "independent bucket record exceeds its byte bound",
            ));
        }
        bytes = bytes
            .checked_add(encoded.len())
            .ok_or_else(|| digest_error("independent bucket byte total overflow"))?;
        if bytes > limits.max_bytes_per_batch {
            return Err(digest_error(
                "independent bucket slice exceeds its byte bound",
            ));
        }
        append_encoded_digest_record(bucket, collection, &encoded)?;
        previous = Some(&record.id);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn calculate_range_digest_root(
    cluster_id: &ClusterId,
    range_id: RangeId,
    range_epoch: u64,
    start_token: u64,
    end_token: Option<u64>,
    resolved_through: u64,
    buckets: &[RangeDigestBucket],
) -> Result<String> {
    validate_buckets(buckets)?;
    calculate_root(
        cluster_id,
        range_id,
        range_epoch,
        start_token,
        end_token,
        resolved_through,
        buckets,
    )
}

/// Authenticated node-to-node transport for node-owned digest checkpoints.
/// The caller supplies only a compare-and-advance checksum, never bucket data.
pub trait RangeDigestTransport: std::fmt::Debug + Send + Sync + 'static {
    #[allow(clippy::too_many_arguments)]
    fn advance_range_digest(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        session_id: Uuid,
        expected_checksum_sha256: Option<&str>,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestState>;

    #[allow(clippy::too_many_arguments)]
    fn export_range_digest_bucket(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        session_id: Uuid,
        bucket: u32,
        resume_after_key: Option<&str>,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestBucketScanStep> {
        let _ = (
            destination,
            range_id,
            range_epoch,
            session_id,
            bucket,
            resume_after_key,
            limits,
            now_ms,
        );
        Err(digest_error(
            "range digest transport does not support bucket export",
        ))
    }
}

impl RangeDigestState {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        session_id: Uuid,
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        start_token: u64,
        end_token: Option<u64>,
        resolved_through: u64,
        limits: &RangeDigestLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if session_id.is_nil()
            || range_epoch == 0
            || end_token.is_some_and(|end| end <= start_token)
        {
            return Err(digest_error(
                "session ID, range epoch, or token interval is invalid",
            ));
        }
        let buckets = (0..limits.bucket_count)
            .map(|bucket| RangeDigestBucket {
                bucket: bucket as u32,
                records: 0,
                serialized_bytes: 0,
                sha256: EMPTY_SHA256.to_string(),
            })
            .collect();
        let mut state = Self {
            format_version: RANGE_DIGEST_FORMAT_VERSION,
            session_id,
            cluster_id,
            node_id,
            range_id,
            range_epoch,
            start_token,
            end_token,
            resolved_through,
            bucket_count: limits.bucket_count,
            resume_after_key: None,
            last_collection: None,
            last_record_id: None,
            last_batch_sha256: None,
            scanned_records: 0,
            serialized_bytes: 0,
            buckets,
            completed: false,
            root_sha256: None,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        state.validate(limits)?;
        Ok(state)
    }

    /// Apply one deterministic scan batch. Returns `false` when the exact batch
    /// was already durably reflected, making retry after an uncertain response
    /// idempotent.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_batch(
        &mut self,
        expected_previous_resume: Option<&str>,
        resume_after_key: impl Into<String>,
        collection: &str,
        range_id: RangeId,
        range_epoch: u64,
        records: &[Record],
        declared_serialized_bytes: usize,
        limits: &RangeDigestLimits,
    ) -> Result<bool> {
        self.validate(limits)?;
        if self.completed {
            return Err(digest_error("cannot append to a completed range digest"));
        }
        if range_id != self.range_id || range_epoch != self.range_epoch {
            return Err(digest_error(
                "digest batch crossed its range or epoch fence",
            ));
        }
        if collection.is_empty()
            || collection.len() > 1024
            || collection.chars().any(char::is_control)
        {
            return Err(digest_error("digest collection identity is invalid"));
        }
        if records.is_empty() || records.len() > limits.max_records_per_batch {
            return Err(digest_error(
                "digest batch record count is outside its bound",
            ));
        }
        let resume_after_key = resume_after_key.into();
        if resume_after_key.is_empty() || resume_after_key.len() > 16 * 1024 {
            return Err(digest_error("digest resume cursor is outside its bound"));
        }
        let fingerprint = batch_sha256(
            expected_previous_resume,
            &resume_after_key,
            collection,
            range_id,
            range_epoch,
            records,
            declared_serialized_bytes,
        )?;
        if self.resume_after_key.as_deref() == Some(resume_after_key.as_str()) {
            if self.last_batch_sha256.as_deref() == Some(fingerprint.as_str()) {
                return Ok(false);
            }
            return Err(digest_error(
                "replayed digest cursor carries different batch contents",
            ));
        }
        if self.resume_after_key.as_deref() != expected_previous_resume {
            return Err(digest_error(
                "digest batch resume cursor is stale or has a gap",
            ));
        }

        let mut encoded = Vec::with_capacity(records.len());
        let mut actual_bytes = 0_usize;
        let mut previous_id = self
            .last_collection
            .as_deref()
            .filter(|previous| *previous == collection)
            .and(self.last_record_id.as_deref());
        if self
            .last_collection
            .as_deref()
            .is_some_and(|previous| previous > collection)
        {
            return Err(digest_error(
                "digest collections are not monotonically ordered",
            ));
        }
        for record in records {
            if record.id.is_empty()
                || record.id.len() > 16 * 1024
                || previous_id.is_some_and(|previous| previous >= record.id.as_str())
            {
                return Err(digest_error(
                    "digest record IDs are empty, oversized, duplicated, or out of order",
                ));
            }
            let bytes = serde_json::to_vec(record)?;
            if bytes.len() > limits.max_record_bytes {
                return Err(digest_error(format!(
                    "record {collection}/{} exceeds the digest record-byte bound",
                    record.id
                )));
            }
            actual_bytes = actual_bytes
                .checked_add(bytes.len())
                .ok_or_else(|| digest_error("digest batch byte count overflow"))?;
            if actual_bytes > limits.max_bytes_per_batch {
                return Err(digest_error("digest batch exceeds its byte bound"));
            }
            previous_id = Some(record.id.as_str());
            encoded.push(bytes);
        }
        if actual_bytes != declared_serialized_bytes {
            return Err(digest_error(format!(
                "digest batch declared {declared_serialized_bytes} bytes but contains {actual_bytes}"
            )));
        }

        for (record, bytes) in records.iter().zip(encoded) {
            let bucket_index = token_bucket(
                distribution_key_token(collection, &record.id),
                self.bucket_count,
            );
            let bucket = self
                .buckets
                .get_mut(bucket_index)
                .ok_or_else(|| digest_error("digest bucket routing escaped its fixed fanout"))?;
            append_encoded_digest_record(bucket, collection, &bytes)?;
        }
        self.scanned_records = self
            .scanned_records
            .checked_add(records.len() as u64)
            .ok_or_else(|| digest_error("digest record count overflow"))?;
        self.serialized_bytes = self
            .serialized_bytes
            .checked_add(actual_bytes as u64)
            .ok_or_else(|| digest_error("digest byte count overflow"))?;
        self.last_collection = Some(collection.to_string());
        self.last_record_id = records.last().map(|record| record.id.clone());
        self.resume_after_key = Some(resume_after_key);
        self.last_batch_sha256 = Some(fingerprint);
        self.refresh_checksum()?;
        self.validate(limits)?;
        Ok(true)
    }

    /// Advance across a collection boundary that emitted no records. This is
    /// a separately fingerprinted checkpoint so retry after an uncertain
    /// durable save is idempotent, while a conflicting cursor at the same
    /// position fails closed.
    pub fn advance_cursor(
        &mut self,
        expected_previous_resume: Option<&str>,
        resume_after_key: impl Into<String>,
        limits: &RangeDigestLimits,
    ) -> Result<bool> {
        self.validate(limits)?;
        if self.completed {
            return Err(digest_error("cannot advance a completed range digest"));
        }
        let resume_after_key = resume_after_key.into();
        if resume_after_key.is_empty() || resume_after_key.len() > 16 * 1024 {
            return Err(digest_error("digest resume cursor is outside its bound"));
        }
        let fingerprint = cursor_advance_sha256(
            expected_previous_resume,
            &resume_after_key,
            self.range_id,
            self.range_epoch,
        );
        if self.resume_after_key.as_deref() == Some(resume_after_key.as_str()) {
            if self.last_batch_sha256.as_deref() == Some(fingerprint.as_str()) {
                return Ok(false);
            }
            return Err(digest_error(
                "replayed digest cursor carries different advance contents",
            ));
        }
        if self.resume_after_key.as_deref() != expected_previous_resume {
            return Err(digest_error("digest cursor advance is stale or has a gap"));
        }
        self.resume_after_key = Some(resume_after_key);
        self.last_batch_sha256 = Some(fingerprint);
        self.refresh_checksum()?;
        self.validate(limits)?;
        Ok(true)
    }

    pub fn finish(
        &mut self,
        expected_resume_after_key: Option<&str>,
        limits: &RangeDigestLimits,
    ) -> Result<RangeDigestManifest> {
        self.validate(limits)?;
        if self.resume_after_key.as_deref() != expected_resume_after_key {
            return Err(digest_error(
                "digest completion cursor does not match state",
            ));
        }
        if !self.completed {
            self.root_sha256 = Some(calculate_root(
                &self.cluster_id,
                self.range_id,
                self.range_epoch,
                self.start_token,
                self.end_token,
                self.resolved_through,
                &self.buckets,
            )?);
            self.completed = true;
            self.refresh_checksum()?;
        }
        self.manifest(limits)
    }

    pub fn manifest(&self, limits: &RangeDigestLimits) -> Result<RangeDigestManifest> {
        self.validate(limits)?;
        if !self.completed {
            return Err(digest_error("range digest is not complete"));
        }
        let root_sha256 = self
            .root_sha256
            .clone()
            .ok_or_else(|| digest_error("completed range digest has no root"))?;
        let mut manifest = RangeDigestManifest {
            format_version: self.format_version,
            session_id: self.session_id,
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            start_token: self.start_token,
            end_token: self.end_token,
            resolved_through: self.resolved_through,
            bucket_count: self.bucket_count,
            scanned_records: self.scanned_records,
            serialized_bytes: self.serialized_bytes,
            buckets: self.buckets.clone(),
            root_sha256,
            checksum_sha256: String::new(),
        };
        manifest.checksum_sha256 = manifest.calculate_checksum()?;
        manifest.validate(limits)?;
        Ok(manifest)
    }

    pub fn validate(&self, limits: &RangeDigestLimits) -> Result<()> {
        limits.validate()?;
        if self.format_version != RANGE_DIGEST_FORMAT_VERSION
            || self.session_id.is_nil()
            || self.range_epoch == 0
            || self.end_token.is_some_and(|end| end <= self.start_token)
            || self.bucket_count != limits.bucket_count
            || self.buckets.len() != self.bucket_count
            || self
                .resume_after_key
                .as_ref()
                .is_some_and(|value| value.len() > 16 * 1024)
            || self
                .last_collection
                .as_ref()
                .is_some_and(|value| value.len() > 1024)
            || self
                .last_record_id
                .as_ref()
                .is_some_and(|value| value.len() > 16 * 1024)
        {
            return Err(digest_error(
                "range digest state identity or bounds are invalid",
            ));
        }
        validate_buckets(&self.buckets)?;
        if let Some(batch) = &self.last_batch_sha256 {
            validate_sha256(batch)?;
        }
        let records = self.buckets.iter().try_fold(0_u64, |total, bucket| {
            total
                .checked_add(bucket.records)
                .ok_or_else(|| digest_error("digest bucket record total overflow"))
        })?;
        let bytes = self.buckets.iter().try_fold(0_u64, |total, bucket| {
            total
                .checked_add(bucket.serialized_bytes)
                .ok_or_else(|| digest_error("digest bucket byte total overflow"))
        })?;
        if records != self.scanned_records || bytes != self.serialized_bytes {
            return Err(digest_error("digest bucket totals disagree with state"));
        }
        if self.completed != self.root_sha256.is_some() {
            return Err(digest_error("digest completion and root disagree"));
        }
        if let Some(root) = &self.root_sha256 {
            validate_sha256(root)?;
            if *root
                != calculate_root(
                    &self.cluster_id,
                    self.range_id,
                    self.range_epoch,
                    self.start_token,
                    self.end_token,
                    self.resolved_through,
                    &self.buckets,
                )?
            {
                return Err(digest_error("range digest root mismatch"));
            }
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(digest_error("range digest state checksum mismatch"));
        }
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() as u64 > limits.max_state_bytes {
            return Err(digest_error("range digest state exceeds its byte bound"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            session_id: Uuid,
            cluster_id: &'a ClusterId,
            node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            start_token: u64,
            end_token: Option<u64>,
            resolved_through: u64,
            bucket_count: usize,
            resume_after_key: &'a Option<String>,
            last_collection: &'a Option<String>,
            last_record_id: &'a Option<String>,
            last_batch_sha256: &'a Option<String>,
            scanned_records: u64,
            serialized_bytes: u64,
            buckets: &'a [RangeDigestBucket],
            completed: bool,
            root_sha256: &'a Option<String>,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            session_id: self.session_id,
            cluster_id: &self.cluster_id,
            node_id: &self.node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            start_token: self.start_token,
            end_token: self.end_token,
            resolved_through: self.resolved_through,
            bucket_count: self.bucket_count,
            resume_after_key: &self.resume_after_key,
            last_collection: &self.last_collection,
            last_record_id: &self.last_record_id,
            last_batch_sha256: &self.last_batch_sha256,
            scanned_records: self.scanned_records,
            serialized_bytes: self.serialized_bytes,
            buckets: &self.buckets,
            completed: self.completed,
            root_sha256: &self.root_sha256,
        })
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }
}

fn cursor_advance_sha256(
    previous_resume: Option<&str>,
    resume_after_key: &str,
    range_id: RangeId,
    range_epoch: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(CURSOR_ADVANCE_HASH_DOMAIN);
    hasher.update(range_id.get().to_le_bytes());
    hasher.update(range_epoch.to_le_bytes());
    if let Some(previous) = previous_resume {
        hasher.update(1_u8.to_le_bytes());
        hasher.update((previous.len() as u64).to_le_bytes());
        hasher.update(previous.as_bytes());
    } else {
        hasher.update(0_u8.to_le_bytes());
    }
    hasher.update((resume_after_key.len() as u64).to_le_bytes());
    hasher.update(resume_after_key.as_bytes());
    hex::encode(hasher.finalize())
}

impl RangeDigestManifest {
    pub fn validate(&self, limits: &RangeDigestLimits) -> Result<()> {
        limits.validate()?;
        if self.format_version != RANGE_DIGEST_FORMAT_VERSION
            || self.session_id.is_nil()
            || self.range_epoch == 0
            || self.end_token.is_some_and(|end| end <= self.start_token)
            || self.bucket_count != limits.bucket_count
            || self.buckets.len() != self.bucket_count
        {
            return Err(digest_error(
                "range digest manifest identity or bounds are invalid",
            ));
        }
        validate_buckets(&self.buckets)?;
        let records = self.buckets.iter().try_fold(0_u64, |total, bucket| {
            total
                .checked_add(bucket.records)
                .ok_or_else(|| digest_error("digest manifest record total overflow"))
        })?;
        let bytes = self.buckets.iter().try_fold(0_u64, |total, bucket| {
            total
                .checked_add(bucket.serialized_bytes)
                .ok_or_else(|| digest_error("digest manifest byte total overflow"))
        })?;
        if records != self.scanned_records || bytes != self.serialized_bytes {
            return Err(digest_error("digest manifest bucket totals disagree"));
        }
        validate_sha256(&self.root_sha256)?;
        if self.root_sha256
            != calculate_root(
                &self.cluster_id,
                self.range_id,
                self.range_epoch,
                self.start_token,
                self.end_token,
                self.resolved_through,
                &self.buckets,
            )?
        {
            return Err(digest_error("digest manifest root mismatch"));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(digest_error("digest manifest checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > limits.max_state_bytes {
            return Err(digest_error("range digest manifest exceeds its byte bound"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            session_id: Uuid,
            cluster_id: &'a ClusterId,
            node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            start_token: u64,
            end_token: Option<u64>,
            resolved_through: u64,
            bucket_count: usize,
            scanned_records: u64,
            serialized_bytes: u64,
            buckets: &'a [RangeDigestBucket],
            root_sha256: &'a str,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            session_id: self.session_id,
            cluster_id: &self.cluster_id,
            node_id: &self.node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            start_token: self.start_token,
            end_token: self.end_token,
            resolved_through: self.resolved_through,
            bucket_count: self.bucket_count,
            scanned_records: self.scanned_records,
            serialized_bytes: self.serialized_bytes,
            buckets: &self.buckets,
            root_sha256: &self.root_sha256,
        })
    }
}

pub fn compare_range_digest_manifests(
    local: &RangeDigestManifest,
    remote: &RangeDigestManifest,
    limits: &RangeDigestLimits,
) -> Result<RangeDigestDiff> {
    local.validate(limits)?;
    remote.validate(limits)?;
    if local.cluster_id != remote.cluster_id
        || local.range_id != remote.range_id
        || local.range_epoch != remote.range_epoch
        || local.start_token != remote.start_token
        || local.end_token != remote.end_token
        || local.resolved_through != remote.resolved_through
        || local.bucket_count != remote.bucket_count
    {
        return Err(digest_error(
            "cannot compare digests from different cluster/range/epoch/consensus prefixes",
        ));
    }
    let divergent_buckets = local
        .buckets
        .iter()
        .zip(&remote.buckets)
        .filter_map(|(left, right)| (left != right).then_some(left.bucket))
        .collect();
    Ok(RangeDigestDiff {
        range_id: local.range_id,
        range_epoch: local.range_epoch,
        resolved_through: local.resolved_through,
        local_node_id: local.node_id.clone(),
        remote_node_id: remote.node_id.clone(),
        local_root_sha256: local.root_sha256.clone(),
        remote_root_sha256: remote.root_sha256.clone(),
        divergent_buckets,
    })
}

pub fn save_range_digest_state(
    path: impl AsRef<Path>,
    state: &RangeDigestState,
    limits: &RangeDigestLimits,
    fsync: bool,
) -> Result<()> {
    state.validate(limits)?;
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() as u64 > limits.max_state_bytes {
        return Err(digest_error("range digest state exceeds its write bound"));
    }
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_range_digest_state(
    path: impl AsRef<Path>,
    limits: &RangeDigestLimits,
) -> Result<RangeDigestState> {
    limits.validate()?;
    let path = path.as_ref();
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length == 0 || length > limits.max_state_bytes {
        return Err(digest_error(
            "range digest state file is outside its byte bound",
        ));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(limits.max_state_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > limits.max_state_bytes {
        return Err(digest_error(
            "range digest state changed or grew while reading",
        ));
    }
    let state: RangeDigestState = serde_json::from_slice(&bytes)?;
    state.validate(limits)?;
    Ok(state)
}

fn token_bucket(token: u64, bucket_count: usize) -> usize {
    let shift = 64 - bucket_count.trailing_zeros();
    (token >> shift) as usize
}

fn validate_buckets(buckets: &[RangeDigestBucket]) -> Result<()> {
    for (index, bucket) in buckets.iter().enumerate() {
        if bucket.bucket as usize != index {
            return Err(digest_error(
                "digest buckets are not canonical and contiguous",
            ));
        }
        validate_sha256(&bucket.sha256)?;
        if bucket.records == 0 && (bucket.serialized_bytes != 0 || bucket.sha256 != EMPTY_SHA256) {
            return Err(digest_error("empty digest bucket carries non-empty state"));
        }
    }
    Ok(())
}

fn append_encoded_digest_record(
    bucket: &mut RangeDigestBucket,
    collection: &str,
    bytes: &[u8],
) -> Result<()> {
    let previous = decode_sha256(&bucket.sha256)?;
    let mut hasher = Sha256::new();
    hasher.update(RECORD_HASH_DOMAIN);
    hasher.update(previous);
    hasher.update((collection.len() as u64).to_le_bytes());
    hasher.update(collection.as_bytes());
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    bucket.sha256 = hex::encode(hasher.finalize());
    bucket.records = bucket
        .records
        .checked_add(1)
        .ok_or_else(|| digest_error("digest bucket record count overflow"))?;
    bucket.serialized_bytes = bucket
        .serialized_bytes
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| digest_error("digest bucket byte count overflow"))?;
    Ok(())
}

fn calculate_root(
    cluster_id: &ClusterId,
    range_id: RangeId,
    range_epoch: u64,
    start_token: u64,
    end_token: Option<u64>,
    resolved_through: u64,
    buckets: &[RangeDigestBucket],
) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(ROOT_HASH_DOMAIN);
    hasher.update((cluster_id.as_str().len() as u64).to_le_bytes());
    hasher.update(cluster_id.as_str().as_bytes());
    hasher.update(range_id.get().to_le_bytes());
    hasher.update(range_epoch.to_le_bytes());
    hasher.update(start_token.to_le_bytes());
    hasher.update([u8::from(end_token.is_some())]);
    hasher.update(end_token.unwrap_or(0).to_le_bytes());
    hasher.update(resolved_through.to_le_bytes());
    hasher.update((buckets.len() as u64).to_le_bytes());
    for bucket in buckets {
        hasher.update(bucket.bucket.to_le_bytes());
        hasher.update(bucket.records.to_le_bytes());
        hasher.update(bucket.serialized_bytes.to_le_bytes());
        hasher.update(decode_sha256(&bucket.sha256)?);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[allow(clippy::too_many_arguments)]
fn batch_sha256(
    expected_previous_resume: Option<&str>,
    resume_after_key: &str,
    collection: &str,
    range_id: RangeId,
    range_epoch: u64,
    records: &[Record],
    declared_serialized_bytes: usize,
) -> Result<String> {
    #[derive(Serialize)]
    struct Batch<'a> {
        expected_previous_resume: Option<&'a str>,
        resume_after_key: &'a str,
        collection: &'a str,
        range_id: RangeId,
        range_epoch: u64,
        records: &'a [Record],
        declared_serialized_bytes: usize,
    }
    sha256_json(&Batch {
        expected_previous_resume,
        resume_after_key,
        collection,
        range_id,
        range_epoch,
        records,
        declared_serialized_bytes,
    })
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(digest_error(
            "digest SHA-256 is not canonical lowercase hex",
        ));
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    validate_sha256(value)?;
    let mut decoded = [0_u8; 32];
    hex::decode_to_slice(value, &mut decoded)
        .map_err(|error| digest_error(format!("invalid digest SHA-256: {error}")))?;
    Ok(decoded)
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ids() -> (ClusterId, ClusterNodeId, ClusterNodeId, RangeId) {
        (
            ClusterId::new("cluster-a").unwrap(),
            ClusterNodeId::new("node-a").unwrap(),
            ClusterNodeId::new("node-b").unwrap(),
            RangeId::new(7).unwrap(),
        )
    }

    fn record(id: usize, value: usize) -> Record {
        Record::new(format!("record-{id:05}"))
            .with_metadata(json!({"value": value, "stable": true}))
    }

    fn add_batches(
        state: &mut RangeDigestState,
        records: &[Record],
        batch_size: usize,
        limits: &RangeDigestLimits,
    ) {
        let mut previous = None::<String>;
        for (index, batch) in records.chunks(batch_size).enumerate() {
            let cursor = format!("cursor-{index:05}");
            let bytes = batch
                .iter()
                .map(|record| serde_json::to_vec(record).unwrap().len())
                .sum();
            state
                .apply_batch(
                    previous.as_deref(),
                    &cursor,
                    "patients",
                    RangeId::new(7).unwrap(),
                    3,
                    batch,
                    bytes,
                    limits,
                )
                .unwrap();
            previous = Some(cursor);
        }
    }

    #[test]
    fn digest_is_independent_of_bounded_batch_boundaries() {
        let (cluster, node_a, node_b, range) = ids();
        let limits = RangeDigestLimits {
            bucket_count: 64,
            max_records_per_batch: 200,
            ..RangeDigestLimits::default()
        };
        let records: Vec<_> = (0..1_000).map(|id| record(id, id)).collect();
        let session = Uuid::new_v4();
        let mut first = RangeDigestState::create(
            session,
            cluster.clone(),
            node_a,
            range,
            3,
            0,
            None,
            91,
            &limits,
        )
        .unwrap();
        let mut second =
            RangeDigestState::create(session, cluster, node_b, range, 3, 0, None, 91, &limits)
                .unwrap();
        add_batches(&mut first, &records, 100, &limits);
        add_batches(&mut second, &records, 137, &limits);
        let first_cursor = first.resume_after_key.clone();
        let second_cursor = second.resume_after_key.clone();
        let first = first.finish(first_cursor.as_deref(), &limits).unwrap();
        let second = second.finish(second_cursor.as_deref(), &limits).unwrap();
        assert_eq!(first.root_sha256, second.root_sha256);
        assert!(compare_range_digest_manifests(&first, &second, &limits)
            .unwrap()
            .divergent_buckets
            .is_empty());
    }

    #[test]
    fn one_changed_record_identifies_a_bounded_repair_bucket() {
        let (cluster, node_a, node_b, range) = ids();
        let limits = RangeDigestLimits {
            bucket_count: 64,
            max_records_per_batch: 200,
            ..RangeDigestLimits::default()
        };
        let records: Vec<_> = (0..500).map(|id| record(id, id)).collect();
        let mut changed = records.clone();
        changed[243] = record(243, 999_999);
        let session = Uuid::new_v4();
        let mut first = RangeDigestState::create(
            session,
            cluster.clone(),
            node_a,
            range,
            3,
            0,
            None,
            91,
            &limits,
        )
        .unwrap();
        let mut second =
            RangeDigestState::create(session, cluster, node_b, range, 3, 0, None, 91, &limits)
                .unwrap();
        add_batches(&mut first, &records, 100, &limits);
        add_batches(&mut second, &changed, 100, &limits);
        let first_cursor = first.resume_after_key.clone();
        let second_cursor = second.resume_after_key.clone();
        let first = first.finish(first_cursor.as_deref(), &limits).unwrap();
        let second = second.finish(second_cursor.as_deref(), &limits).unwrap();
        let diff = compare_range_digest_manifests(&first, &second, &limits).unwrap();
        assert_eq!(diff.divergent_buckets.len(), 1);
    }

    #[test]
    fn checkpoint_reopen_and_uncertain_batch_retry_are_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let (cluster, node_a, _, range) = ids();
        let limits = RangeDigestLimits {
            bucket_count: 16,
            ..RangeDigestLimits::default()
        };
        let mut state = RangeDigestState::create(
            Uuid::new_v4(),
            cluster,
            node_a,
            range,
            3,
            0,
            None,
            91,
            &limits,
        )
        .unwrap();
        let records: Vec<_> = (0..10).map(|id| record(id, id)).collect();
        let bytes = records
            .iter()
            .map(|record| serde_json::to_vec(record).unwrap().len())
            .sum();
        assert!(state
            .apply_batch(None, "cursor-1", "patients", range, 3, &records, bytes, &limits,)
            .unwrap());
        let path = directory.path().join("digest-state.json");
        save_range_digest_state(&path, &state, &limits, false).unwrap();
        let mut reopened = load_range_digest_state(&path, &limits).unwrap();
        assert!(!reopened
            .apply_batch(None, "cursor-1", "patients", range, 3, &records, bytes, &limits,)
            .unwrap());
        assert_eq!(reopened.scanned_records, 10);

        let mut corrupted = std::fs::read(&path).unwrap();
        let position = corrupted.len() / 2;
        corrupted[position] ^= 1;
        std::fs::write(&path, corrupted).unwrap();
        assert!(load_range_digest_state(&path, &limits).is_err());
    }

    #[test]
    fn empty_collection_cursor_advance_is_checked_and_idempotent() {
        let (cluster, node_a, _, range) = ids();
        let limits = RangeDigestLimits {
            bucket_count: 16,
            ..RangeDigestLimits::default()
        };
        let mut state = RangeDigestState::create(
            Uuid::new_v4(),
            cluster,
            node_a,
            range,
            3,
            0,
            None,
            91,
            &limits,
        )
        .unwrap();
        assert!(state.advance_cursor(None, "collection-b", &limits).unwrap());
        assert!(!state.advance_cursor(None, "collection-b", &limits).unwrap());
        assert_eq!(state.scanned_records, 0);
        assert!(state
            .advance_cursor(Some("collection-a"), "collection-c", &limits)
            .is_err());
    }

    #[test]
    fn comparison_refuses_different_consensus_prefixes() {
        let (cluster, node_a, node_b, range) = ids();
        let limits = RangeDigestLimits {
            bucket_count: 16,
            ..RangeDigestLimits::default()
        };
        let mut first = RangeDigestState::create(
            Uuid::new_v4(),
            cluster.clone(),
            node_a,
            range,
            3,
            0,
            None,
            91,
            &limits,
        )
        .unwrap();
        let mut second = RangeDigestState::create(
            Uuid::new_v4(),
            cluster,
            node_b,
            range,
            3,
            0,
            None,
            92,
            &limits,
        )
        .unwrap();
        let first = first.finish(None, &limits).unwrap();
        let second = second.finish(None, &limits).unwrap();
        assert!(compare_range_digest_manifests(&first, &second, &limits).is_err());
    }
}
