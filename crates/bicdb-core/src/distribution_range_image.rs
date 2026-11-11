//! Bounded capture of an already-fenced physical range image.
//!
//! A local seal is an observation, not a membership/quorum certificate. The
//! handoff coordinator must verify independent authenticated observations and
//! actual installed native receipts before it authorizes cleanup/activation.
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::{RangeImageDbBindings, SourceRangeImageLineage};
use crate::distribution::{
    ClusterId, ClusterNodeId, RangeId, RangeSnapshotBatch, RangeSnapshotOptions, RelocationId,
};
use crate::distribution_range_handoff::CertifiedRangeImageCut;
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationGate};
use crate::{BicDb, BicDbError, CancellationToken, CollectionMeta, Result};

const IMAGE_FORMAT: u32 = 1;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_WIRE_STEP_BYTES: usize = 9 * 1024 * 1024;
const MAX_SCAN_RECORDS: usize = 16_384;
const MAX_CAPTURE_WAIT: Duration = Duration::from_secs(300);
const WAIT_POLL: Duration = Duration::from_millis(10);

fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range image: {}", detail.into()))
}

/// A consensus command cut. Its only production constructor takes the opaque
/// coordinator proof; a number received from a wire message is never enough.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RangeIndexCut(u64);
impl RangeIndexCut {
    fn certified(cut: &CertifiedRangeImageCut) -> Self {
        Self(cut.range_index())
    }
    pub(crate) fn get(self) -> u64 {
        self.0
    }
}

/// A captured global reservation bound, not an applied snapshot or range index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReservedDbUpperBound(u64);
impl ReservedDbUpperBound {
    pub(crate) fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeImageCursorV1 {
    collection_index: usize,
    after_key: Option<String>,
    step: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct RangeImageStepV1 {
    pub format_version: u32,
    pub session_id: Uuid,
    pub nonce: Uuid,
    pub previous_cursor: RangeImageCursorV1,
    pub next_cursor: RangeImageCursorV1,
    pub batch: Option<RangeSnapshotBatch>,
    pub snapshot_db_sequence: u64,
    pub complete: bool,
    pub previous_chain_sha256: String,
    pub chain_sha256: String,
}

/// This DTO is deliberately deserializable: it is an authenticated transport
/// observation to be checked by the handoff protocol, not proof by construction.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LocalRangeImageSealV1 {
    pub format_version: u32,
    pub session_id: Uuid,
    pub nonce: Uuid,
    pub plan_id: String,
    pub relocation_id: RelocationId,
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub source_native_receipt_sha256: String,
    pub range_id: RangeId,
    pub capture_epoch: u64,
    pub activation_epoch: u64,
    pub range_index: u64,
    pub old_config_sha256: String,
    pub new_config_sha256: String,
    pub source_id: ClusterNodeId,
    pub source_member_incarnation: u64,
    pub source_database_incarnation: String,
    pub reserved_db_upper_bound: u64,
    pub min_snapshot_db_sequence: u64,
    pub max_snapshot_db_sequence: u64,
    pub schema_sha256: String,
    pub reader_features: Vec<String>,
    pub portable_catalog_sha256: String,
    pub image_root_sha256: String,
    pub record_count: u64,
    pub serialized_record_bytes: u64,
    pub steps: u64,
    pub wire_chain_sha256: String,
    pub local_namespace_ids: Vec<(String, Uuid)>,
}

/// At most one bounded retry step is retained. The exclusive local permit is
/// owned until this session is dropped; durable remote promises survive that
/// drop and must be resolved by the handoff coordinator, never by a timer.
pub(crate) struct RangeImageCapture {
    db: Arc<RwLock<BicDb>>,
    gate: RangeMutationGate,
    frozen: FrozenRangeMutationPermit,
    cut: CertifiedRangeImageCut,
    range_index: RangeIndexCut,
    reserved: ReservedDbUpperBound,
    bindings: RangeImageDbBindings,
    lineage: SourceRangeImageLineage,
    catalog_sha256: String,
    content: Sha256,
    cursor: RangeImageCursorV1,
    previous_step: Option<RangeImageStepV1>,
    wire_chain_sha256: String,
    record_count: u64,
    serialized_record_bytes: u64,
    min_snapshot_db_sequence: Option<u64>,
    max_snapshot_db_sequence: u64,
    complete: bool,
    invalid: bool,
}

fn read_database(db: &Arc<RwLock<BicDb>>) -> Result<RwLockReadGuard<'_, BicDb>> {
    db.try_read()
        .ok_or_else(|| invalid("database is busy; retry outside database/service locks"))
}

/// Write JSON with a hard allocation bound, including framing/escaping bytes.
struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("range image serialization bound exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn bounded_json(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut writer = BoundedJson {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}
fn hash_frame(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

pub(crate) fn canonical_range_image_catalog(
    cluster_id: &ClusterId,
    range: &crate::distribution::RangeDescriptor,
    range_index: u64,
    bindings: &RangeImageDbBindings,
) -> Result<Vec<u8>> {
    bounded_json(
        &(
            IMAGE_FORMAT,
            cluster_id,
            range.id,
            range.start_token,
            range.end_token,
            range_index,
            bindings.schema_sha256(),
            bindings.reader_features(),
            bindings.collections(),
        ),
        MAX_BATCH_BYTES,
    )
}
pub(crate) fn content_hasher(catalog: &[u8]) -> Sha256 {
    let mut content = Sha256::new();
    content.update(b"bicdb.range-image.content.v1\0");
    hash_frame(&mut content, catalog);
    content
}

impl RangeImageCapture {
    pub(crate) fn begin(
        db: Arc<RwLock<BicDb>>,
        cut: CertifiedRangeImageCut,
        frozen: FrozenRangeMutationPermit,
        lineage: SourceRangeImageLineage,
        wait_budget: Duration,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        if wait_budget.is_zero()
            || wait_budget > MAX_CAPTURE_WAIT
            || frozen.owner() != cut.session_id()
        {
            return Err(invalid("invalid capture budget or freeze-session binding"));
        }
        let wait = cancellation.child_with_deadline(Instant::now() + wait_budget);
        wait.check()?;
        let (gate, reserved, bindings) = {
            let database = read_database(&db)?;
            let gate = database.range_mutation_gate();
            gate.validate_frozen(cut.range().id, &frozen)?;
            database.validate_source_range_image_lineage(
                &lineage,
                cut.range().id,
                cut.range_index(),
            )?;
            let bindings = database.capture_range_image_bindings()?;
            // R and B belong to different domains. Never use B as a snapshot
            // override or treat a local reservation as quorum-certified R.
            (
                gate,
                ReservedDbUpperBound(database.current_commit_seq()),
                bindings,
            )
        };
        loop {
            wait.check()?;
            let reached = {
                let database = read_database(&db)?;
                gate.validate_frozen(cut.range().id, &frozen)?;
                database.validate_range_image_bindings(&bindings)?;
                database.replication_shippable_seq() >= reserved.0
            };
            if reached {
                break;
            }
            // No database/service latch is held across this bounded wait.
            std::thread::sleep(WAIT_POLL);
        }
        let catalog_bytes = canonical_range_image_catalog(
            cut.cluster_id(),
            cut.range(),
            cut.range_index(),
            &bindings,
        )?;
        let catalog_sha256 = hex::encode(Sha256::digest(&catalog_bytes));
        let content = content_hasher(&catalog_bytes);
        let wire_chain_sha256 = hex::encode(Sha256::digest(b"bicdb.range-image.wire.v1\0"));
        let range_index = RangeIndexCut::certified(&cut);
        let complete = bindings.collections().is_empty();
        Ok(Self {
            db,
            gate,
            frozen,
            cut,
            range_index,
            reserved,
            bindings,
            lineage,
            catalog_sha256,
            content,
            cursor: RangeImageCursorV1::default(),
            previous_step: None,
            wire_chain_sha256,
            record_count: 0,
            serialized_record_bytes: 0,
            min_snapshot_db_sequence: None,
            max_snapshot_db_sequence: 0,
            complete,
            invalid: false,
        })
    }

    pub(crate) fn cursor(&self) -> &RangeImageCursorV1 {
        &self.cursor
    }
    pub(crate) fn collections(&self) -> &[CollectionMeta] {
        self.bindings.collections()
    }
    pub(crate) fn range_index(&self) -> RangeIndexCut {
        self.range_index
    }
    pub(crate) fn reserved_upper_bound(&self) -> ReservedDbUpperBound {
        self.reserved
    }

    fn validate(&self, database: &BicDb) -> Result<()> {
        if self.invalid {
            return Err(invalid(
                "capture was invalidated; begin a new authorized session",
            ));
        }
        self.gate
            .validate_frozen(self.cut.range().id, &self.frozen)?;
        database.validate_range_image_bindings(&self.bindings)?;
        database.validate_source_range_image_lineage(
            &self.lineage,
            self.cut.range().id,
            self.range_index.0,
        )?;
        if database.replication_shippable_seq() < self.reserved.0 {
            return Err(invalid(
                "actual durable/applied prefix regressed below captured reservation bound",
            ));
        }
        Ok(())
    }

    /// The caller controls a deadline token for each RPC. A successful cursor
    /// always includes every matching row scanned before that cursor, even if
    /// most raw rows belonged to other ranges. Timeouts return no new cursor.
    pub(crate) fn export_step(
        &mut self,
        expected_cursor: &RangeImageCursorV1,
        options: &RangeSnapshotOptions,
        cancellation: &CancellationToken,
    ) -> Result<RangeImageStepV1> {
        cancellation.check()?;
        options.validate()?;
        if options.max_bytes_per_batch > MAX_BATCH_BYTES
            || options.max_record_bytes > MAX_BATCH_BYTES
            || options.max_records_per_batch > MAX_SCAN_RECORDS
        {
            return Err(invalid("copy step exceeds the fixed memory/scan budget"));
        }
        let database = read_database(&self.db)?;
        if let Err(error) = self.validate(&database) {
            self.invalid = true;
            return Err(error);
        }
        if let Some(previous) = &self.previous_step {
            if &previous.previous_cursor == expected_cursor {
                return Ok(previous.clone());
            }
        }
        if expected_cursor != &self.cursor || self.complete {
            return Err(invalid("copy cursor is stale, forged, or already complete"));
        }
        let meta = &self.bindings.collections()[self.cursor.collection_index];
        let mut batch = None;
        let export = database.for_each_range_snapshot_batch_with_cancellation(
            &meta.name,
            self.cut.range(),
            self.cursor.after_key.as_deref(),
            options,
            MAX_SCAN_RECORDS,
            cancellation,
            |value| {
                batch = Some(value);
                Ok(false)
            },
        )?;
        if export.snapshot_commit_sequence < self.reserved.0 {
            self.invalid = true;
            return Err(invalid(
                "actual MVCC snapshot is below the captured durable reservation bound",
            ));
        }
        if let Err(error) = self.validate(&database) {
            self.invalid = true;
            return Err(error);
        }
        cancellation.check()?;
        let mut next = self.cursor.clone();
        next.step = next
            .step
            .checked_add(1)
            .ok_or_else(|| invalid("copy step overflow"))?;
        if export.completed {
            next.collection_index += 1;
            next.after_key = None;
        } else {
            if export.resume_after_key.as_ref() <= self.cursor.after_key.as_ref() {
                return Err(invalid("bounded scan did not advance its cursor"));
            }
            next.after_key = export.resume_after_key;
        }
        let complete = next.collection_index == self.bindings.collections().len();
        let mut step = RangeImageStepV1 {
            format_version: IMAGE_FORMAT,
            session_id: self.cut.session_id(),
            nonce: self.cut.nonce(),
            previous_cursor: self.cursor.clone(),
            next_cursor: next.clone(),
            batch,
            snapshot_db_sequence: export.snapshot_commit_sequence,
            complete,
            previous_chain_sha256: self.wire_chain_sha256.clone(),
            chain_sha256: String::new(),
        };
        let encoded = bounded_json(&step, MAX_WIRE_STEP_BYTES)?;
        step.chain_sha256 = hex::encode(Sha256::digest(&encoded));
        // Publish session progress only after the entire step is checked. Any
        // unsuccessful attempt may be retried from the previous cursor.
        let mut content = self.content.clone();
        let mut record_count = self.record_count;
        let mut serialized_record_bytes = self.serialized_record_bytes;
        if let Some(batch) = &step.batch {
            for record in &batch.records {
                let bytes = bounded_json(record, options.max_record_bytes)?;
                hash_frame(&mut content, meta.name.as_bytes());
                hash_frame(&mut content, &bytes);
                record_count = record_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("image record count overflow"))?;
                serialized_record_bytes = serialized_record_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("image byte count overflow"))?;
            }
        }
        self.content = content;
        self.record_count = record_count;
        self.serialized_record_bytes = serialized_record_bytes;
        self.min_snapshot_db_sequence = Some(
            self.min_snapshot_db_sequence
                .map_or(export.snapshot_commit_sequence, |seq| {
                    seq.min(export.snapshot_commit_sequence)
                }),
        );
        self.max_snapshot_db_sequence = self
            .max_snapshot_db_sequence
            .max(export.snapshot_commit_sequence);
        self.cursor = next;
        self.complete = complete;
        self.wire_chain_sha256.clone_from(&step.chain_sha256);
        self.previous_step = Some(step.clone());
        Ok(step)
    }

    pub(crate) fn completed_seal(&self) -> Result<LocalRangeImageSealV1> {
        if !self.complete {
            return Err(invalid("cannot seal an incomplete image"));
        }
        let database = read_database(&self.db)?;
        self.validate(&database)?;
        let min_snapshot = self.min_snapshot_db_sequence.unwrap_or(self.reserved.0);
        let max_snapshot = self.max_snapshot_db_sequence.max(self.reserved.0);
        Ok(LocalRangeImageSealV1 {
            format_version: IMAGE_FORMAT,
            session_id: self.cut.session_id(),
            nonce: self.cut.nonce(),
            plan_id: self.cut.plan_id().to_string(),
            relocation_id: self.cut.relocation_id(),
            cluster_id: self.cut.cluster_id().clone(),
            bootstrap_id: self.lineage.bootstrap_id(),
            source_native_receipt_sha256: self.lineage.native_receipt_sha256().to_string(),
            range_id: self.cut.range().id,
            capture_epoch: self.cut.range().epoch,
            activation_epoch: self.cut.activation_epoch(),
            range_index: self.range_index.0,
            old_config_sha256: self.cut.old_config_sha256().to_string(),
            new_config_sha256: self.cut.new_config_sha256().to_string(),
            source_id: self.cut.source_id().clone(),
            source_member_incarnation: self.cut.source_incarnation(),
            source_database_incarnation: self.bindings.database_incarnation().to_string(),
            reserved_db_upper_bound: self.reserved.0,
            min_snapshot_db_sequence: min_snapshot,
            max_snapshot_db_sequence: max_snapshot,
            schema_sha256: self.bindings.schema_sha256().to_string(),
            reader_features: self.bindings.reader_features().to_vec(),
            portable_catalog_sha256: self.catalog_sha256.clone(),
            image_root_sha256: hex::encode(self.content.clone().finalize()),
            record_count: self.record_count,
            serialized_record_bytes: self.serialized_record_bytes,
            steps: self.cursor.step,
            wire_chain_sha256: self.wire_chain_sha256.clone(),
            local_namespace_ids: self.bindings.namespace_ids(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn json_bound_counts_framing_and_escaping_before_allocation() {
        let text = "\\".repeat(1024);
        assert!(bounded_json(&text, 1024).is_err());
        assert_eq!(bounded_json(&vec![1, 2], 5).unwrap(), b"[1,2]");
        assert!(bounded_json(&vec![1, 2], 4).is_err());
    }
    #[test]
    fn content_framing_cannot_confuse_namespace_record_boundaries() {
        let mut one = Sha256::new();
        hash_frame(&mut one, b"a");
        hash_frame(&mut one, b"bc");
        let mut two = Sha256::new();
        hash_frame(&mut two, b"ab");
        hash_frame(&mut two, b"c");
        assert_ne!(one.finalize(), two.finalize());
    }
}
