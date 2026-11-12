//! Service-owned ordinary native materialization. Store refusal checks and an
//! owned normal mutation permit span the real local WAL/native callback.
use super::*;
use crate::distribution_native_apply::{
    NativeLeaderInstructionAdmission, NativeReplicaApplyObservation,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

impl ClusterDataNodeService {
    pub(crate) fn observe_native_ordinary_for_role(
        &self,
        role: &RecoveryRoleAdmission,
        challenge: Uuid,
    ) -> Result<NativeReplicaApplyObservation> {
        role.require_live_request()?;
        if role.local_node_id() != &self.node_id || role.cluster_id() != &self.cluster_id {
            return Err(data_error(
                "native ordinary observer belongs to another service",
            ));
        }
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, role.current_range())?;
        let actual = database.observe_native_replica_for_role(role)?;
        let progress = self.range_writes.progress(role.current_range().id);
        let observation = NativeReplicaApplyObservation::from_native_materialization(
            &actual, progress, challenge,
        )?;
        actual.require_current(&database)?;
        role.require_live_request()?;
        Ok(observation)
    }
    pub(crate) fn apply_native_ordinary_instruction(
        &mut self,
        admission: &NativeLeaderInstructionAdmission,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        parent.check()?;
        admission.require_live()?;
        let range = admission.role().current_range();
        let command = admission.command();
        if admission.role().local_node_id() != &self.node_id
            || admission.role().cluster_id() != &self.cluster_id
        {
            return Err(data_error("native instruction belongs to another service"));
        }
        let gate = self.range_mutations.clone();
        let permit = gate.try_enter(range.id)?;
        let authority = RangeMutationAuthority::Mutation(&permit);
        self.validate_range_write(range, admission.peer_node_id(), command)?;
        self.range_writes.normal_native_write_allowed(command)?;
        if !matches!(
            self.range_writes.state(command)?,
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied
        ) {
            return Err(data_error(
                "native ordinary instruction has no actual local durable CQ",
            ));
        }
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, range)?;
        // Raw Applied is intentionally not a physical fast path. The native
        // adapter verifies immutable history or performs a real new WAL apply.
        let actual = database.apply_native_origin_instruction(admission, &authority, parent)?;
        self.range_writes.normal_native_write_allowed(command)?;
        actual.require_current(&database)?;
        self.range_writes.mark_applied(command)?;
        let result = NativeReplicaApplyObservation::from_native_materialization(
            &actual,
            self.range_writes.progress(range.id),
            admission.challenge(),
        )?;
        admission.require_live()?;
        parent.check()?;
        Ok(result)
    }
}

// Explicit trusted embedded fixture seam: a real opaque origin certificate,
// never a decoded certificate/voter list, authorizes this lower-level control.
#[cfg(test)]
impl ClusterDataNodeService {
    pub(crate) fn observe_native_embedded_ordinary(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        challenge: Uuid,
    ) -> Result<NativeReplicaApplyObservation> {
        self.validate_range_write_peer(range, caller)?;
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        let actual = database.observe_native_embedded_replica(range, None)?;
        NativeReplicaApplyObservation::from_native_materialization(
            &actual,
            self.range_writes.progress(range.id),
            challenge,
        )
    }
    pub(crate) fn apply_native_embedded_origin(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
        certificate: &crate::distribution_range_consensus::NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        parent.check()?;
        self.validate_range_write(range, caller, command)?;
        let gate = self.range_mutations.clone();
        let permit = gate.try_enter(range.id)?;
        let authority = RangeMutationAuthority::Mutation(&permit);
        self.range_writes.normal_native_write_allowed(command)?;
        if certificate.intent().command_checksum_sha256() != command.checksum_sha256
            || certificate.intent().command_id() != command.command_id
            || !certificate.evidence().is_origin()
            || !matches!(
                self.range_writes.state(command)?,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            )
        {
            return Err(data_error(
                "embedded native apply requires exact actual certificate and local CQ",
            ));
        }
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        let before = database.current_native_range_source_lineage(range.id)?;
        certificate
            .intent()
            .validate_native_configuration(&before)?;
        if command.index <= before.range_index() {
            database.require_native_command_materialized(&before, command)?;
        } else {
            database.apply_native_origin_certificate(
                certificate,
                &command.mutations,
                &authority,
                true,
            )?;
        }
        self.range_writes.normal_native_write_allowed(command)?;
        self.range_writes.mark_applied(command)?;
        let actual = database.observe_native_embedded_replica(range, Some(command))?;
        NativeReplicaApplyObservation::from_native_materialization(
            &actual,
            self.range_writes.progress(range.id),
            Uuid::now_v7(),
        )
    }
}
