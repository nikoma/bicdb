//! Original service/DB/Store consumer for the DISTINCT remote native publisher.
use super::*;
impl ClusterDataNodeService {
    pub(crate) fn publish_remote_registered_decision(
        service: &Arc<Mutex<Self>>,
        target: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>,
        frozen: &FrozenRangeMutationPermit,
        admission: &crate::distribution_range_handoff::NativeRemoteDecisionAdmission<'_>,
        attempt: &mut crate::db::NativeRemoteJointDecisionAttempt,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        admission.require_current()?;
        admission.metadata().require_external_current()?;
        let mut service = service
            .try_lock()
            .ok_or_else(|| data_error("remote native DECISION original service busy"))?;
        let database_arc = service.db.clone();
        let database = checked_database_read(&database_arc, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        database.publish_remote_registered_native_decision(
            target,
            frozen,
            service.range_writes.current_mut(),
            admission,
            attempt,
            parent,
        )
    }
}
