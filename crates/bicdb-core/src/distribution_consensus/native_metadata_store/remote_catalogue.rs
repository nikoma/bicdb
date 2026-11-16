//! Necessary actual pending-entry comparison ONLY. This neither invents a
//! dual-data proof nor retries failed metadata persistence.
use super::*;
impl MetadataConsensusStore {
    /// Necessary selected-effect comparison against this ACTUAL healthy native
    /// committed store. Historical log entries are compacted; this deliberately
    /// does NOT claim the earlier issued entry itself committed. Callers must
    /// retain the genuine original known proposal return and data-quorum proof.
    pub(crate) fn require_native_remote_selected_commit(
        &self,
        issued: &MetadataLogEntry,
        relocation: crate::distribution::RelocationId,
        current: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        self.require_native_control_leader(current, business)?;
        if issued.leadership_barrier
            || issued.restore_activation.is_some()
            || issued.index > self.state.commit_index
            || issued.term > self.state.current_term
            || issued.topology.generation > self.state.committed_topology.generation
        {
            return Err(metadata_error(
                "FIRST known selected proposal is beyond actual committed native metadata",
            ));
        }
        let expected = issued
            .topology
            .relocations
            .get(&relocation)
            .ok_or_else(|| metadata_error("FIRST original selected proposal absent"))?;
        let observed = self
            .state
            .committed_topology
            .relocations
            .get(&relocation)
            .ok_or_else(|| metadata_error("FIRST actual committed selected relocation absent"))?;
        if expected.native_joint_handoff_v1.is_none()
            || expected.native_joint_handoff_v1 != observed.native_joint_handoff_v1
            || expected.id != observed.id
            || expected.plan_id != observed.plan_id
            || expected.handoff_session != observed.handoff_session
            || expected.range_id != observed.range_id
            || expected.expected_epoch != observed.expected_epoch
            || expected.learner_epoch != observed.learner_epoch
            || expected.promoted_epoch != observed.promoted_epoch
            || expected.source != observed.source
            || expected.target != observed.target
            || expected.target_replica_id != observed.target_replica_id
            || expected.phase != observed.phase
            || expected.resume_phase != observed.resume_phase
            || issued.topology.cluster_id != self.state.committed_topology.cluster_id
            || issued.topology.range_image_bootstrap_id
                != self.state.committed_topology.range_image_bootstrap_id
        {
            return Err(metadata_error("FIRST actual committed selected native binding differs from original known proposal"));
        }
        let canonical = |r: &crate::RangeDescriptor| {
            let mut r = r.clone();
            r.approximate_bytes = 0;
            r.approximate_qps = 0;
            r.replicas.sort_by_key(|x| x.id);
            r
        };
        let old = issued
            .topology
            .range_by_id(expected.range_id)
            .ok_or_else(|| metadata_error("FIRST original selected range absent"))?;
        let actual = self
            .state
            .committed_topology
            .range_by_id(expected.range_id)
            .ok_or_else(|| metadata_error("FIRST actual selected range absent"))?;
        if canonical(old) != canonical(actual) {
            return Err(metadata_error(
                "FIRST actual selected configuration changed",
            ));
        }
        for node in old
            .replicas
            .iter()
            .map(|r| &r.node_id)
            .chain(expected.source.iter())
        {
            let a = issued
                .topology
                .nodes
                .get(node)
                .ok_or_else(|| metadata_error("FIRST original selected member absent"))?;
            let b = self
                .state
                .committed_topology
                .nodes
                .get(node)
                .ok_or_else(|| metadata_error("FIRST actual selected member absent"))?;
            if a.incarnation != b.incarnation
                || a.native_store_birth_v1 != b.native_store_birth_v1
                || a.tls_certificate_sha256 != b.tls_certificate_sha256
                || a.pending_tls_certificate_sha256 != b.pending_tls_certificate_sha256
                || a.labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != b.labels
                        .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                || b.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
            {
                return Err(metadata_error(
                    "FIRST actual selected member birth/credential/schema changed",
                ));
            }
        }
        current.require_live_request(business)
    }
    /// Actual owner-built membership-only proposal. Generic topology DATA is
    /// never used to create the private historical certification scope.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn propose_native_remote_membership(
        &mut self,
        topology: crate::ClusterTopology,
        scope: &crate::distribution_native_host::NativeRemoteMembershipScope<'_>,
    ) -> Result<MetadataLogEntry> {
        scope.require_proposal(
            &self.state.committed_topology,
            &topology,
            &self.state.node_id,
        )?;
        self.authority_operation(|store| {
            store.propose_topology_checked_inner(topology, || scope.require_request())
        })
    }
    pub(crate) fn require_native_remote_catalogue_pending(
        &self,
        entry: &MetadataLogEntry,
        current: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        self.require_native_control_leader(current, business)?;
        if entry.leadership_barrier
            || entry.restore_activation.is_some()
            || entry.term != self.state.current_term
            || self
                .state
                .log
                .iter()
                .filter(|e| !e.leadership_barrier)
                .count()
                != 1
            || self.state.log.iter().find(|e| e.index == entry.index) != Some(entry)
        {
            return Err(metadata_error(
                "FIRST original native catalogue proposal no longer matches actual pending entry",
            ));
        }
        Ok(())
    }

    #[cfg(feature = "tls-replication")]
    pub(crate) fn propose_native_remote_activation(
        &mut self,
        topology: crate::ClusterTopology,
        scope: &crate::distribution_native_host::NativeRemoteActivationScope<'_>,
    ) -> Result<MetadataLogEntry> {
        scope.require_control_leader(self)?;
        scope.require_proposal(
            &self.state.committed_topology,
            &topology,
            &self.state.node_id,
        )?;
        self.authority_operation(|store| {
            store.propose_topology_checked_inner(topology, || scope.require_request())
        })
    }
}
