//! Actual post-collection faults and DATA-only forensic reads.
use super::*;

impl NativeJointPreparationPhaseQuorum {
    pub(crate) fn test_prepublication_snapshot(&self) -> Result<serde_json::Value> {
        let mut members = BTreeMap::new();
        for (node, old) in &self.original.observations {
            let service = old
                .service
                .try_lock()
                .ok_or_else(|| invalid("prepublication diagnostic service busy"))?;
            let mut value = old.actual.test_prepublication_owner_snapshot(&service)?;
            value["request"] = self.observations[node].committed.test_request_memory();
            members.insert(node.clone(), value);
        }
        Ok(serde_json::json!({"members":members,
            "phase_cancel_requested":self.cancellation.is_cancel_requested(),
            "phase_remaining_us":self.cancellation.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
            "aggregate_caller_cancel_requested":self.original.caller.is_cancel_requested(),
            "aggregate_caller_live":self.original.caller.check().is_ok(),
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
