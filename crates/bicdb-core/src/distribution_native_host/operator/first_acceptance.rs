//! Test-only wrappers on actual normal public hosts. These do not expose a
//! service, Role, OWN, Frozen or native quorum or enable an automatic planner.
use super::*;
impl NativeMetadataHost {
    pub(in crate::distribution_native_host) fn test_committed_publication_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_committed_publication_request_wait(range, mode, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_metadata_identity_own_cancel(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_first_metadata_identity_own_cancel(range, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_hot_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_hot_request_wait(range, mode, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_status_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_status_request_wait(range, mode, parent, |original| self.status(original))
    }
    pub(in crate::distribution_native_host) fn test_first_old_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_old_request_wait(range, mode, parent)
    }
}
impl NativeMetadataHost {
    pub(in crate::distribution_native_host) fn test_first_metadata_identity_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_first_metadata_identity_wait(range, mode, parent)
    }
    pub(in crate::distribution_native_host) fn test_actual_partial_drain_peer_call(
        &self,
        destination: &ClusterNodeId,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<impl FnOnce() -> Result<serde_json::Value> + Send + 'static> {
        self.test_first_control()?
            .test_actual_partial_drain_peer_call(destination, business, parent)
    }
    fn test_first_control(&self) -> Result<&NativeMetadataControlHost> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("FIRST actual test host control absent"))
    }
    pub(in crate::distribution_native_host) fn test_first_topology(
        &self,
        parent: &CancellationToken,
    ) -> Result<ClusterTopology> {
        self.test_first_control()?.test_first_topology(parent)
    }
    pub(in crate::distribution_native_host) fn test_first_register_learner(
        &self,
        learner: crate::ClusterNode,
        parent: &CancellationToken,
    ) -> Result<ClusterTopology> {
        self.test_first_control()?
            .test_first_register_learner(learner, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_commit_allocation(
        &self,
        range: crate::RangeId,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<ClusterTopology> {
        self.test_first_control()?
            .test_first_commit_allocation(range, target, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_coordinator_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_first_coordinator_snapshot(range)
    }
    pub(in crate::distribution_native_host) fn test_first_replay_request(
        &mut self,
        range: crate::RangeId,
        request: crate::distribution_native_host::NativeFirstReceiveRequestV1,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.control
            .as_mut()
            .ok_or_else(|| invalid("actual FIRST test source absent"))?
            .test_first_replay_request(range, request, parent)
    }

    pub(in crate::distribution_native_host) fn test_first_drop_discovery_reply(
        &self,
    ) -> Result<()> {
        self.test_first_control()?
            .test_drop_next_source_discovery_reply()
    }
    pub(in crate::distribution_native_host) fn test_first_donor_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_source_discovery_state(range)
    }
    pub(in crate::distribution_native_host) fn test_first_new_mutation_after_unknown(
        &mut self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.control
            .as_mut()
            .ok_or_else(|| invalid("actual FIRST source owner absent"))?
            .test_first_new_mutation_after_unknown(range, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_arm_close_loss(
        &self,
        range: crate::RangeId,
    ) -> Result<impl Drop + Send + 'static> {
        self.test_first_control()?.test_first_arm_close_loss(range)
    }
    pub(in crate::distribution_native_host) fn test_first_close_loss_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_first_close_loss_snapshot(range)
    }

    pub(in crate::distribution_native_host) fn test_first_concurrent_admission(
        &mut self,
        range: crate::RangeId,
        barrier: &crate::distribution_native_host::remote_first::NativeFirstIngressBarrier,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.control
            .as_mut()
            .ok_or_else(|| invalid("actual FIRST source absent"))?
            .test_first_concurrent_admission(range, barrier, parent)
    }

    pub(in crate::distribution_native_host) fn test_first_stop_state(&self) -> serde_json::Value {
        serde_json::json!({"phase":self.phase,"supervisor_slot_present":self.supervisor.is_some(),
            "control_listener_running":self.control.as_ref().is_some_and(|c|c.is_running()),"memory_only":true})
    }
}

// Cross-protocol controls execute on the SAME actual normal host. Only DATA
// leaves these facades; no retained plan/backend/transport/owner is exposed.
impl NativeMetadataHost {
    pub(in crate::distribution_native_host) fn test_first_competing_handoff_step(
        &self,
        range: crate::RangeId,
        target_attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_first_competing_handoff_step(range, target_attempt, parent)
    }
    pub(in crate::distribution_native_host) fn test_first_cross_protocol_source_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.test_first_control()?
            .test_first_cross_protocol_source_snapshot(range)
    }
}
