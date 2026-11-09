use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::encryption::EncryptionRuntime;
use crate::error::Result;
use crate::storage::{self, FrameKind, SegmentReadMode};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpType {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncOp {
    pub op_id: Uuid,
    pub collection: String,
    pub record_id: String,
    pub op_type: OpType,
    pub timestamp: i64,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum SyncLogEvent {
    Add {
        op: SyncOp,
    },
    MarkSynced {
        op_ids: Vec<Uuid>,
    },
    /// Written first by a compaction, which runs with no commit in flight:
    /// every commit at or below `commit_seq` had its ops appended (or the
    /// database was fenced, and compaction refused). Compaction forgets the
    /// acknowledged op ids, so recovery must not republish the transaction
    /// log's copy of any commit at or below this sequence.
    CompactedThrough {
        commit_seq: u64,
    },
}

#[derive(Debug)]
pub(crate) struct SyncLog {
    path: PathBuf,
    fsync: bool,
    encryption: EncryptionRuntime,
    pending: BTreeMap<Uuid, SyncOp>,
    synced: BTreeSet<Uuid>,
    /// See `SyncLogEvent::CompactedThrough`.
    compacted_through: u64,
    /// The database's fail-closed state: a failed tail repair of this log
    /// fences the database (`storage::fence_on_frame_log_failure`).
    durability: Option<Arc<bicdb_page::DurabilityHealth>>,
}

impl SyncLog {
    pub fn open(
        path: impl AsRef<Path>,
        fsync: bool,
        read_mode: SegmentReadMode,
        encryption: EncryptionRuntime,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let recovered = storage::read_frames(&path, FrameKind::Sync, read_mode, &encryption)?;
        let _truncated_bytes = recovered.truncated_bytes;
        let mut pending = BTreeMap::new();
        let mut synced = BTreeSet::new();
        let mut compacted_through = 0_u64;

        // Parse the sync-log frames (JSON) across all cores; the order-sensitive
        // fold below (Add then MarkSynced) still replays them in original order.
        let events = crate::db::parallel_map_ordered(&recovered.frames, |frame| {
            let event: SyncLogEvent = serde_json::from_slice(&frame.payload)?;
            Ok(event)
        })?;
        for event in events {
            match event {
                SyncLogEvent::Add { op } => {
                    if !synced.contains(&op.op_id) {
                        pending.insert(op.op_id, op);
                    }
                }
                SyncLogEvent::MarkSynced { op_ids } => {
                    for op_id in op_ids {
                        synced.insert(op_id);
                        pending.remove(&op_id);
                    }
                }
                SyncLogEvent::CompactedThrough { commit_seq } => {
                    compacted_through = compacted_through.max(commit_seq);
                }
            }
        }

        Ok(Self {
            path,
            fsync,
            encryption,
            pending,
            synced,
            compacted_through,
            durability: None,
        })
    }

    /// Construct a sync log WITHOUT reading the on-disk frames. Used when the CDC
    /// outbox is disabled (`DbConfig::sync_outbox == false`): no consumer is polling
    /// it, so re-reading and replaying a possibly multi-GB never-drained outbox at
    /// open would be pure cost. Starts with empty pending/synced state.
    pub fn disabled(path: impl AsRef<Path>, fsync: bool, encryption: EncryptionRuntime) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            fsync,
            encryption,
            pending: BTreeMap::new(),
            synced: BTreeSet::new(),
            compacted_through: 0,
            durability: None,
        }
    }

    /// Recovery's copy of committed change-capture ops into this log.
    ///
    /// A commit's ops are written into its own transaction-log batch, so
    /// they are durable exactly when the commit decision is, and appended
    /// here only after the commit is durable and visible. A crash (or a
    /// failed append, which fences the database) between the two leaves
    /// committed ops only in the transaction log. `committed` holds every
    /// committed transaction's ops still in the retained log, in commit
    /// order. An op already here, pending or acknowledged, is skipped, so
    /// replaying the same log any number of times appends each op once. A
    /// commit at or below the compaction watermark is skipped too: its ops
    /// were appended before that compaction, which may have dropped them as
    /// acknowledged. Returns the number of ops appended.
    pub(crate) fn republish_committed(
        &mut self,
        committed: &[(u64, Vec<SyncOp>)],
    ) -> Result<usize> {
        let mut seen = BTreeSet::new();
        let missing = committed
            .iter()
            .filter(|(commit_seq, _)| *commit_seq > self.compacted_through)
            .flat_map(|(_, ops)| ops.iter())
            .filter(|op| {
                !self.pending.contains_key(&op.op_id)
                    && !self.synced.contains(&op.op_id)
                    && seen.insert(op.op_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        self.append_ops(&missing)?;
        Ok(missing.len())
    }

    pub(crate) fn attach_durability(&mut self, health: Arc<bicdb_page::DurabilityHealth>) {
        self.durability = Some(health);
    }

    pub fn append_ops(&mut self, ops: &[SyncOp]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut payloads = Vec::with_capacity(ops.len());
        for op in ops {
            let event = SyncLogEvent::Add { op: op.clone() };
            payloads.push(serde_json::to_vec(&event)?);
        }

        // A failed append cuts its bytes back before returning (or, when that
        // fails too, fences the database), so the commit that called this
        // aborts with nothing of it left in the log.
        storage::append_frames(
            &self.path,
            FrameKind::Sync,
            &payloads,
            self.fsync,
            &storage::CompressionConfig::disabled(),
            &self.encryption,
        )
        .map_err(|error| storage::fence_on_frame_log_failure(self.durability.as_ref(), error))?;

        for op in ops {
            if !self.synced.contains(&op.op_id) {
                self.pending.insert(op.op_id, op.clone());
            }
        }
        Ok(())
    }

    pub fn pending_ops(&self) -> Vec<SyncOp> {
        let mut ops = self.pending.values().cloned().collect::<Vec<_>>();
        ops.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.collection.cmp(&right.collection))
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        ops
    }

    pub fn mark_synced(&mut self, op_ids: &[Uuid]) -> Result<()> {
        if op_ids.is_empty() {
            return Ok(());
        }

        let event = SyncLogEvent::MarkSynced {
            op_ids: op_ids.to_vec(),
        };
        let payload = serde_json::to_vec(&event)?;
        storage::append_frame(
            &self.path,
            FrameKind::Sync,
            &payload,
            self.fsync,
            &self.encryption,
        )
        .map_err(|error| storage::fence_on_frame_log_failure(self.durability.as_ref(), error))?;

        for op_id in op_ids {
            self.synced.insert(*op_id);
            self.pending.remove(op_id);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        storage::sync_file(&self.path)
    }

    /// `compacted_through` is the newest commit sequence; the caller holds
    /// the database exclusively, so no commit is between its decision and
    /// its append here.
    pub(crate) fn compact(&mut self, compacted_through: u64) -> Result<(u64, u64)> {
        let bytes_before = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };

        let ops = self.pending_ops();
        // The watermark matters only once compaction forgets acknowledged
        // ids (or an earlier compaction did): otherwise every op is still
        // known by id, and the log stays as small as before.
        let needs_watermark = !self.synced.is_empty() || self.compacted_through > 0;
        let compacted_through = if needs_watermark {
            self.compacted_through.max(compacted_through)
        } else {
            0
        };
        let mut payloads = Vec::with_capacity(ops.len() + 1);
        if needs_watermark {
            payloads.push(serde_json::to_vec(&SyncLogEvent::CompactedThrough {
                commit_seq: compacted_through,
            })?);
        }
        for op in &ops {
            payloads.push(serde_json::to_vec(&SyncLogEvent::Add { op: op.clone() })?);
        }
        crate::db::rewrite_frame_file(
            &self.path,
            FrameKind::Sync,
            &payloads,
            self.fsync,
            &storage::CompressionConfig::disabled(),
            &self.encryption,
        )?;
        self.synced.clear();
        self.compacted_through = compacted_through;
        self.pending = ops.into_iter().map(|op| (op.op_id, op)).collect();

        let bytes_after = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        Ok((bytes_before, bytes_after))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(record: &str) -> SyncOp {
        SyncOp {
            op_id: Uuid::new_v4(),
            collection: "items".into(),
            record_id: record.into(),
            op_type: OpType::Insert,
            timestamp: 1,
            hash: String::new(),
        }
    }

    fn open(path: &Path) -> SyncLog {
        SyncLog::open(
            path,
            true,
            SegmentReadMode::Buffered,
            EncryptionRuntime::disabled(),
        )
        .unwrap()
    }

    /// Republication appends each committed op once, never an op that is
    /// pending or acknowledged, and never a commit at or below the
    /// compaction watermark (whose acknowledged ops compaction forgot).
    #[test]
    fn republication_is_idempotent_and_respects_the_compaction_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sync.log");
        let (a, b, c) = (op("a"), op("b"), op("c"));
        {
            let mut log = open(&path);
            // Commit 3 published a and b; the consumer acknowledged a.
            log.append_ops(&[a.clone(), b.clone()]).unwrap();
            log.mark_synced(&[a.op_id]).unwrap();
            // The log still holds commit 3: nothing to republish.
            assert_eq!(
                log.republish_committed(&[(3, vec![a.clone(), b.clone()])])
                    .unwrap(),
                0
            );
            // Compaction at sequence 5 forgets the acknowledgment of a.
            log.compact(5).unwrap();
        }
        let mut log = open(&path);
        assert_eq!(log.pending_ops(), vec![b.clone()]);
        // A retained log still holding commit 3 must not bring a back.
        assert_eq!(
            log.republish_committed(&[(3, vec![a.clone(), b.clone()])])
                .unwrap(),
            0
        );
        // Commit 6 is past the watermark and missing: appended once.
        assert_eq!(log.republish_committed(&[(6, vec![c.clone()])]).unwrap(), 1);
        assert_eq!(log.republish_committed(&[(6, vec![c.clone()])]).unwrap(), 0);
        drop(log);
        let log = open(&path);
        let mut pending = log.pending_ops();
        pending.sort_by(|x, y| x.record_id.cmp(&y.record_id));
        assert_eq!(pending, vec![b, c]);
    }
}
