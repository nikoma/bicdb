use bicdb_core::{BicDb, NodeId, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ClientSyncCheckpoint, SyncEndpoint};

#[derive(Clone, Debug, Default)]
pub struct SyncCoordinator;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncRunReport {
    pub node_id: NodeId,
    pub pushed_bundle_id: Option<Uuid>,
    pub pushed_events: usize,
    pub pulled_bundles: usize,
    pub imported_events: usize,
    pub duplicate_events: usize,
    pub records_merged: usize,
    pub conflicts_resolved: usize,
    pub starting_checkpoint: ClientSyncCheckpoint,
    pub ending_checkpoint: ClientSyncCheckpoint,
}

impl SyncCoordinator {
    pub fn new() -> Self {
        Self
    }

    pub fn sync_once<E>(&mut self, db: &mut BicDb, endpoint: &mut E) -> Result<SyncRunReport>
    where
        E: SyncEndpoint,
    {
        let node_id = db.node_id();
        let mut checkpoint = endpoint.load_checkpoint(&node_id)?;
        checkpoint.validate()?;
        let starting_checkpoint = checkpoint.clone();

        let mut report = SyncRunReport {
            node_id: node_id.clone(),
            pushed_bundle_id: None,
            pushed_events: 0,
            pulled_bundles: 0,
            imported_events: 0,
            duplicate_events: 0,
            records_merged: 0,
            conflicts_resolved: 0,
            starting_checkpoint,
            ending_checkpoint: checkpoint.clone(),
        };

        let local_bundle = db.export_sync_bundle_since(checkpoint.local_export)?;
        if local_bundle.event_count > 0 {
            let push = endpoint.push_bundle(&local_bundle)?;
            checkpoint.record_local_export(push.next_checkpoint);
            report.pushed_bundle_id = Some(push.bundle_id);
            report.pushed_events = push.event_count;
        }

        for remote_bundle in endpoint.pull_bundles(&node_id, &checkpoint)? {
            let source_node_id = remote_bundle.source_node_id.clone();
            let next_checkpoint = remote_bundle.next_checkpoint;
            let import = db.import_sync_bundle(remote_bundle)?;
            checkpoint.record_remote_import(&source_node_id, next_checkpoint);

            report.pulled_bundles += 1;
            report.imported_events += import.imported_events;
            report.duplicate_events += import.duplicate_events;
            report.records_merged += import.records_merged;
            report.conflicts_resolved += import.conflicts_resolved;
        }

        let imported_events = db.export_sync_bundle_since(checkpoint.local_export)?;
        checkpoint.record_local_export(imported_events.next_checkpoint);

        endpoint.save_checkpoint(&node_id, &checkpoint)?;
        report.ending_checkpoint = checkpoint;
        Ok(report)
    }
}
