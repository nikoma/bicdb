use bicdb_core::{NodeId, Result, SyncBundle, SyncCheckpoint};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ClientSyncCheckpoint;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushBundleReport {
    pub bundle_id: Uuid,
    pub source_node_id: NodeId,
    pub event_count: usize,
    pub from_checkpoint: SyncCheckpoint,
    pub next_checkpoint: SyncCheckpoint,
}

impl From<&SyncBundle> for PushBundleReport {
    fn from(bundle: &SyncBundle) -> Self {
        Self {
            bundle_id: bundle.bundle_id,
            source_node_id: bundle.source_node_id.clone(),
            event_count: bundle.event_count,
            from_checkpoint: bundle.from_checkpoint,
            next_checkpoint: bundle.next_checkpoint,
        }
    }
}

pub trait SyncEndpoint {
    fn load_checkpoint(&self, client_node_id: &NodeId) -> Result<ClientSyncCheckpoint>;

    fn save_checkpoint(
        &mut self,
        client_node_id: &NodeId,
        checkpoint: &ClientSyncCheckpoint,
    ) -> Result<()>;

    fn push_bundle(&mut self, bundle: &SyncBundle) -> Result<PushBundleReport>;

    fn pull_bundles(
        &self,
        client_node_id: &NodeId,
        checkpoint: &ClientSyncCheckpoint,
    ) -> Result<Vec<SyncBundle>>;
}
