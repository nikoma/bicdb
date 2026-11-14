//! Actual post-collection faults and DATA-only forensic reads.
use super::*;

impl NativeJointPreparationPhaseQuorum {
    pub(crate) fn test_prepublication_snapshot(&self) -> Result<serde_json::Value> {
        // ALL authority clocks are copied BEFORE any forensic storage read.
        // Later diagnostic work cannot masquerade as operation-time expiry.
        let started = Instant::now();
        let phase_remaining_us = self
            .cancellation
            .deadline()
            .map(|d| d.saturating_duration_since(started).as_micros());
        let phase_cancel_requested = self.cancellation.is_cancel_requested();
        let aggregate_caller_cancel_requested = self.original.caller.is_cancel_requested();
        let aggregate_caller_live = self.original.caller.check().is_ok();
        let clocks: BTreeMap<_, _> = self
            .observations
            .iter()
            .map(|(node, observation)| (node.clone(), observation.committed.test_request_memory()))
            .collect();
        let mut members = BTreeMap::new();
        for (node, old) in &self.original.observations {
            let service = old
                .service
                .try_lock()
                .ok_or_else(|| invalid("prepublication diagnostic service busy"))?;
            let mut value = old.actual.test_prepublication_owner_snapshot(&service)?;
            value["request"] = clocks[node].clone();
            members.insert(node.clone(), value);
        }
        Ok(serde_json::json!({"members":members,
            "phase_cancel_requested":phase_cancel_requested,
            "phase_remaining_us":phase_remaining_us,
            "aggregate_caller_cancel_requested":aggregate_caller_cancel_requested,
            "aggregate_caller_live":aggregate_caller_live,
            "clocks_captured_at_snapshot_entry_before_storage":true,
            "diagnostic_elapsed_us":started.elapsed().as_micros(),
            "memory_observation_not_authority":true}))
    }
    pub(crate) fn test_remove_post_collection_archive(
        &self,
        member: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<serde_json::Value> {
        let old = self
            .original
            .observations
            .get(member)
            .ok_or_else(|| invalid("fault member missing"))?;
        let service = old
            .service
            .try_lock()
            .ok_or_else(|| invalid("archive fault service busy"))?;
        old.actual
            .test_post_collection_archive_fault(&service, parent)
    }
    /// Opaque actual controlled admission; its type exposes no mutation API.
    /// The caller can only retain/drop it while exercising the real publisher.
    pub(crate) fn test_hold_post_collection_controlled(
        &self,
        member: &ClusterNodeId,
    ) -> Result<impl Send> {
        let old = self
            .original
            .observations
            .get(member)
            .ok_or_else(|| invalid("controlled fault member missing"))?;
        let service = old
            .service
            .try_lock()
            .ok_or_else(|| invalid("controlled fault service busy"))?;
        old.actual.test_post_collection_controlled_hold(&service)
    }
}
