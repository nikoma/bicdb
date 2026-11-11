//! Legacy readers require `records`. Omit that field for protected snapshots
//! so a reader predating capability checks cannot silently ignore new flags.
//! Unprotected batches retain the original serialized shape and field order.
use super::RangeSnapshotBatch;
use crate::{RangeId, Record};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize)]
struct BorrowedBatch<'a> {
    collection: &'a str,
    range_id: RangeId,
    range_epoch: u64,
    snapshot_commit_sequence: u64,
    resume_after_key: &'a str,
    serialized_record_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    records: Option<&'a [Record]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_protected_records_v1: Option<&'a [Record]>,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    required_reader_features: &'a [String],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedBatch {
    collection: String,
    range_id: RangeId,
    range_epoch: u64,
    snapshot_commit_sequence: u64,
    resume_after_key: String,
    serialized_record_bytes: usize,
    records: Option<Vec<Record>>,
    reader_protected_records_v1: Option<Vec<Record>>,
    #[serde(default)]
    required_reader_features: Vec<String>,
}

impl Serialize for RangeSnapshotBatch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.verify_reader_requirements()
            .map_err(serde::ser::Error::custom)?;
        let protected = !self.required_reader_features.is_empty();
        BorrowedBatch {
            collection: &self.collection,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            snapshot_commit_sequence: self.snapshot_commit_sequence,
            resume_after_key: &self.resume_after_key,
            serialized_record_bytes: self.serialized_record_bytes,
            records: (!protected).then_some(self.records.as_slice()),
            reader_protected_records_v1: protected.then_some(self.records.as_slice()),
            required_reader_features: &self.required_reader_features,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RangeSnapshotBatch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OwnedBatch::deserialize(deserializer)?;
        let records = match (
            wire.required_reader_features.is_empty(),
            wire.records,
            wire.reader_protected_records_v1,
        ) {
            (true, Some(records), None) | (false, None, Some(records)) => records,
            _ => {
                return Err(serde::de::Error::custom(
                    "snapshot rows do not match the required-reader wire envelope",
                ));
            }
        };
        let batch = Self {
            collection: wire.collection,
            range_id: wire.range_id,
            range_epoch: wire.range_epoch,
            snapshot_commit_sequence: wire.snapshot_commit_sequence,
            resume_after_key: wire.resume_after_key,
            serialized_record_bytes: wire.serialized_record_bytes,
            records,
            required_reader_features: wire.required_reader_features,
        };
        batch
            .verify_reader_requirements()
            .map_err(serde::de::Error::custom)?;
        Ok(batch)
    }
}
