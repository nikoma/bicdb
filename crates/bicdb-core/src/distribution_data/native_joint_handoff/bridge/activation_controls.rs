//! Test DATA only from the original actual preparation/service. No raw lease,
//! Frozen, writer, request or replay owner leaves this module.
use super::*;
impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn test_owned_activation_state(
        &self,
        service: &ClusterDataNodeService,
    ) -> Result<serde_json::Value> {
        let session_live = self.require_original_preparation_session().is_ok();
        let frozen_owner = self.lease.try_frozen()?.owner();
        let state = service.range_writes.test_owned_activation_snapshot()?;
        let database = service
            .db
            .try_read()
            .ok_or_else(|| data_error("owned activation diagnostic DB busy"))?;
        let storage =
            database.test_joint_prepublication_storage(self.committed.data().old_range().id)?;
        Ok(
            serde_json::json!({"catalogue":state,"storage":storage,"native_preparation_sha256":self.prepared.checksum_sha256(),"original_session_live_at_snapshot_entry":session_live,"frozen_owner":frozen_owner}),
        )
    }
    pub(crate) fn test_owned_activation_later_io(
        &self,
        service: &mut ClusterDataNodeService,
    ) -> Result<serde_json::Value> {
        self.require_original_preparation_session()?;
        self.require_original_writer_lifetime()?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service
            .range_writes
            .current_mut()
            .test_later_same_bytes_activation_io(&database, self.lease.try_frozen()?)
    }
    pub(crate) fn test_owned_activation_foreign(
        &self,
        service: &ClusterDataNodeService,
        native: &crate::db::NativeJointServingBridgeReceipt,
        foreign: &Self,
    ) -> Result<serde_json::Value> {
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.range_writes.test_owned_activation_foreign_bindings(
            &database,
            native,
            &foreign.writer_domain,
            foreign.lease.try_frozen()?,
        )
    }
}

impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn test_stale_activation_comparison(
        &self,
        service: &ClusterDataNodeService,
        admission: &crate::distribution_range_handoff::NativeJointOpenAdmission,
    ) -> Result<serde_json::Value> {
        self.require_original_preparation_session()?;
        self.require_original_writer_lifetime()?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service
            .range_writes
            .test_completed_activation_owner_against_current(
                &database,
                admission,
                &self.writer_domain,
            )
    }
}
