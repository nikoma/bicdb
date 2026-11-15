//! Same actual preparation owner publishes its native bridge. The host retains
//! this original attempt before entry; no reply DATA is a local QQ or receipt.
use super::*;

#[derive(Default)]
pub(crate) struct NativeRemoteServiceBridge {
    native: Option<crate::db::NativeRemoteJointBridgeAttempt>,
    completed: Option<Arc<crate::db::NativeJointServingBridgeReceipt>>,
    #[cfg(test)]
    mode: u8,
}
impl NativeRemoteServiceBridge {
    pub(crate) fn completed(&self) -> Option<&Arc<crate::db::NativeJointServingBridgeReceipt>> {
        self.completed.as_ref()
    }
}
impl ClusterDataNodeService {
    pub(crate) fn publish_remote_joint_bridge(
        original: &Arc<Mutex<Self>>,
        admission: &crate::distribution_range_handoff::NativeRemoteBridgeAdmission<'_>,
        retained: &mut NativeRemoteServiceBridge,
    ) -> Result<()> {
        admission.require_live()?;
        let request = admission.bound_request();
        request.check()?;
        if retained.completed.is_some() {
            return Err(data_error(
                "remote bridge already has its actual completed owner; no repeated mutation",
            ));
        }
        let service = original
            .try_lock()
            .ok_or_else(|| data_error("remote bridge original service busy"))?;
        admission.require_service(&service)?;
        let actual = admission.actual();
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        let committed = admission.committed();
        committed.require_current(&database)?;
        let frozen = actual.lease.try_frozen()?;
        if retained.native.is_none() {
            retained.native = Some(crate::db::NativeRemoteJointBridgeAttempt::retain_original(
                &database,
                &service.range_writes,
                &actual.prepared,
                frozen,
                admission,
            )?);
        }
        #[cfg(test)]
        if retained.mode != 0 {
            let mode = std::mem::take(&mut retained.mode);
            retained.native.as_mut().unwrap().test_arm(mode)?;
        }
        let native = database.publish_remote_native_joint_bridge(
            admission,
            &actual.prepared,
            &service.range_writes,
            &actual.catalogue,
            actual.lease.image(),
            frozen,
            &request,
            retained.native.as_mut().unwrap(),
        )?;
        // Retain the genuine return before process/metadata/request postchecks.
        // The underlying attempt already retains the exact native API outcome
        // even when construction/readback above returns an outer Err.
        retained.completed = Some(Arc::new(native));
        admission.require_live()?;
        request.check()?;
        Ok(())
    }
}

impl ClusterDataNodeService {
    pub(crate) fn observe_remote_joint_bridge_point(
        original: &Arc<Mutex<Self>>,
        admission: &crate::distribution_range_handoff::NativeRemoteBridgeAdmission<'_>,
        native: &crate::db::NativeJointServingBridgeReceipt,
        point: &mut crate::distribution_native_host::NativeRemoteServingPointV1,
    ) -> Result<()> {
        admission.require_live()?;
        let service = original
            .try_lock()
            .ok_or_else(|| data_error("remote bridge observation original service busy"))?;
        admission.require_service(&service)?;
        let actual = admission.actual();
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        let scope = actual.prepared.publication_read_scope(
            &database,
            &service.range_writes,
            admission.committed(),
            &actual.catalogue,
            actual.lease.image(),
            actual.lease.try_frozen()?,
        )?;
        admission.require_scope(&scope, &service.range_writes)?;
        scope.require_remote_bridge_current(native)?;
        if native.data() != admission.committed().data()
            || native.native_preparation_sha256() != actual.prepared.checksum_sha256()
            || point
                .preparation
                .as_ref()
                .is_none_or(|p| p.preparation_sha256 != native.native_preparation_sha256())
            || point.native_database_open != native.native_database_incarnation()
        {
            return Err(data_error(
                "remote bridge observation changed exact actual preparation/D/open",
            ));
        }
        point.native_bridge_sha256 = Some(native.checksum_sha256().into());
        admission
            .committed()
            .require_native_publication_metadata(&database, &service.range_writes)?;
        let version = admission.committed().native_publication_guard()?;
        admission.require_live()?;
        drop(version);
        drop(scope);
        Ok(())
    }
}

#[cfg(test)]
impl NativeRemoteServiceBridge {
    pub(crate) fn test_outcome(&self) -> serde_json::Value {
        serde_json::json!({"actual_native_attempt":self.native.as_ref().map(|n|n.test_outcome()),
            "completed_native_owner_retained":self.completed.is_some()})
    }
}

impl ClusterDataNodeService {
    pub(crate) fn observe_remote_joint_bridge_read_point(
        original: &Arc<Mutex<Self>>,
        admission: &crate::distribution_range_handoff::NativeRemoteBridgeReadAdmission<'_>,
        native: &crate::db::NativeJointServingBridgeReceipt,
        point: &mut crate::distribution_native_host::NativeRemoteServingPointV1,
    ) -> Result<()> {
        admission.require_live()?;
        let service = original
            .try_lock()
            .ok_or_else(|| data_error("remote bridge observation original service busy"))?;
        admission.require_service(&service)?;
        let actual = admission.actual();
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        let scope = actual.prepared.publication_read_scope(
            &database,
            &service.range_writes,
            admission.committed(),
            &actual.catalogue,
            actual.lease.image(),
            actual.lease.try_frozen()?,
        )?;
        admission.require_scope(&scope, &service.range_writes)?;
        scope.require_remote_bridge_current(native)?;
        if native.data() != admission.committed().data()
            || native.native_preparation_sha256() != actual.prepared.checksum_sha256()
            || point
                .preparation
                .as_ref()
                .is_none_or(|p| p.preparation_sha256 != native.native_preparation_sha256())
            || point.native_database_open != native.native_database_incarnation()
        {
            return Err(data_error(
                "remote bridge observation changed exact actual preparation/D/open",
            ));
        }
        point.native_bridge_sha256 = Some(native.checksum_sha256().into());
        admission
            .committed()
            .require_native_publication_metadata(&database, &service.range_writes)?;
        let version = admission.committed().native_publication_guard()?;
        admission.require_live()?;
        drop(version);
        drop(scope);
        Ok(())
    }
}

impl ClusterDataNodeService {
    pub(crate) fn read_known_remote_joint_bridge(
        original: &Arc<Mutex<Self>>,
        admission: &crate::distribution_range_handoff::NativeRemoteBridgeReadAdmission<'_>,
        retained: &mut NativeRemoteServiceBridge,
    ) -> Result<()> {
        admission.require_live()?;
        let request = admission.bound_request();
        request.check()?;
        let service = original
            .try_lock()
            .ok_or_else(|| data_error("known bridge read original service busy"))?;
        admission.require_service(&service)?;
        let actual = admission.actual();
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        let attempt = retained.native.as_ref().ok_or_else(|| {
            data_error("known bridge read missing original native attempt; no reconstruction")
        })?;
        let receipt = database.read_known_remote_native_joint_bridge(
            admission,
            &actual.prepared,
            &service.range_writes,
            &actual.catalogue,
            actual.lease.image(),
            actual.lease.try_frozen()?,
            &request,
            attempt,
        )?;
        attempt.require_known_receipt(&receipt)?;
        if let Some(original) = &retained.completed {
            if original.checksum_sha256() != receipt.checksum_sha256()
                || original.native_database_incarnation() != receipt.native_database_incarnation()
            {
                return Err(data_error(
                    "known bridge read changed original completed receipt/open",
                ));
            }
        } else {
            // A NEW successful protected read is retained before outer checks;
            // it never changes the recorded original native API outcome.
            retained.completed = Some(Arc::new(receipt));
        }
        admission.require_live()?;
        request.check()?;
        Ok(())
    }
}
impl NativeRemoteServiceBridge {
    pub(crate) fn require_resolution(&self) -> Result<()> {
        let native = self
            .native
            .as_ref()
            .ok_or_else(|| data_error("bridge resolution original attempt absent"))?;
        let completed = self
            .completed
            .as_ref()
            .ok_or_else(|| data_error("bridge resolution verified completed owner absent"))?;
        native.require_known_receipt(completed)
    }
    #[cfg(test)]
    pub(crate) fn test_arm(&mut self, mode: u8) -> Result<()> {
        if !matches!(mode, 1 | 2)
            || self.mode != 0
            || self.native.is_some()
            || self.completed.is_some()
        {
            return Err(data_error(
                "bridge fault requires untouched SAME actual original host attempt",
            ));
        }
        self.mode = mode;
        Ok(())
    }
}
