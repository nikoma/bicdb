use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

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
    Add { op: SyncOp },
    MarkSynced { op_ids: Vec<Uuid> },
}

#[derive(Debug)]
pub(crate) struct SyncLog {
    path: PathBuf,
    fsync: bool,
    encryption: EncryptionRuntime,
    pending: BTreeMap<Uuid, SyncOp>,
    synced: BTreeSet<Uuid>,
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
            }
        }

        Ok(Self {
            path,
            fsync,
            encryption,
            pending,
            synced,
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
        }
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

        storage::append_frames(
            &self.path,
            FrameKind::Sync,
            &payloads,
            self.fsync,
            &storage::CompressionConfig::disabled(),
            &self.encryption,
        )?;

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
        )?;

        for op_id in op_ids {
            self.synced.insert(*op_id);
            self.pending.remove(op_id);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        storage::sync_file(&self.path)
    }

    pub(crate) fn compact(&mut self) -> Result<(u64, u64)> {
        let bytes_before = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };

        let ops = self.pending_ops();
        let payloads = ops
            .iter()
            .map(|op| serde_json::to_vec(&SyncLogEvent::Add { op: op.clone() }))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        crate::db::rewrite_frame_file(
            &self.path,
            FrameKind::Sync,
            &payloads,
            self.fsync,
            &storage::CompressionConfig::disabled(),
            &self.encryption,
        )?;
        self.synced.clear();
        self.pending = ops.into_iter().map(|op| (op.op_id, op)).collect();

        let bytes_after = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        Ok((bytes_before, bytes_after))
    }
}
