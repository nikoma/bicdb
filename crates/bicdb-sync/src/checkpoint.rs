use std::collections::BTreeMap;

use bicdb_core::{BicDbError, NodeId, Result, SyncCheckpoint};
use serde::{Deserialize, Serialize};

pub const SYNC_CHECKPOINT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientSyncCheckpoint {
    pub format_version: u32,
    pub local_export: SyncCheckpoint,
    pub remote_imports: BTreeMap<String, SyncCheckpoint>,
}

impl Default for ClientSyncCheckpoint {
    fn default() -> Self {
        Self {
            format_version: SYNC_CHECKPOINT_FORMAT_VERSION,
            local_export: SyncCheckpoint::default(),
            remote_imports: BTreeMap::new(),
        }
    }
}

impl ClientSyncCheckpoint {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != SYNC_CHECKPOINT_FORMAT_VERSION {
            return Err(BicDbError::SyncBundle(format!(
                "unsupported sync checkpoint format version {}",
                self.format_version
            )));
        }
        Ok(())
    }

    pub fn remote_checkpoint(&self, node_id: &NodeId) -> SyncCheckpoint {
        self.remote_imports
            .get(&node_id.to_string())
            .copied()
            .unwrap_or_default()
    }

    pub fn record_local_export(&mut self, checkpoint: SyncCheckpoint) {
        if checkpoint.event_offset > self.local_export.event_offset {
            self.local_export = checkpoint;
        }
    }

    pub fn record_remote_import(&mut self, node_id: &NodeId, checkpoint: SyncCheckpoint) {
        let entry = self.remote_imports.entry(node_id.to_string()).or_default();
        if checkpoint.event_offset > entry.event_offset {
            *entry = checkpoint;
        }
    }
}
