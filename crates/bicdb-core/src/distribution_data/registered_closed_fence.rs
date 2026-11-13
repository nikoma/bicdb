//! Sealed registered receiver refusal only. Ordinary recovery stays strict.
use super::*;
use crate::distribution_native_host::NativeAssignedRegisteredClosedFence;
impl ClusterDataNodeService {
    pub(crate) fn preflight_registered_closed_fence(
        &self,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let role = capability.role();
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        native.require_current(&db)?;
        db.with_assigned_closed_writer_exclusion(capability, || {
            self.range_writes
                .preflight_registered_closed_fence(&db, &native, capability)?;
            parent.check()?;
            capability.require_live()
        })
    }
    pub(crate) fn fence_registered_closed_receiver(
        &mut self,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
        parent: &crate::CancellationToken,
    ) -> Result<crate::distribution_range_recovery::RecoveryEpochFenceReply> {
        parent.check()?;
        let role = capability.role();
        let scope = capability.scope();
        self.range_mutations
            .validate_quiescent_frozen(scope.range_id, frozen)?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        native.require_current(&db)?;
        let store = self.range_writes.current_mut();
        db.with_assigned_closed_writer_exclusion(capability, || {
            store.persist_registered_closed_fence(&db, &native, capability, frozen)?;
            parent.check()?;
            native.require_current(&db)?;
            capability.require_database_store(&db, store)?;
            let reply = crate::distribution_range_recovery::RecoveryEpochFenceReply {
                phase: store.recovery_observation_phase(scope)?,
                image_history: store.observe_registered_closed_fence_obligations(
                    &db, &native, capability, frozen,
                )?,
                scope: scope.clone(),
                node_id: self.node_id.clone(),
                member_incarnation: role.local_member_incarnation(),
                store_open_id: store.recovery_store_open_id(),
                native_database_incarnation: native.database_incarnation().to_string(),
                birth_initialization_complete: true,
                progress: store.progress(scope.range_id),
                native: db.native_range_prefix_observation(scope.range_id)?,
            };
            self.range_mutations
                .validate_quiescent_frozen(scope.range_id, frozen)?;
            parent.check()?;
            capability.require_live()?;
            Ok(reply)
        })
    }
}
