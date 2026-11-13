//! Actual service seam for from-birth writer enrollment. No network under latches.
use super::*;
use crate::db::NativeWriterHostState;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use std::sync::Weak;
impl ClusterDataNodeService {
    pub(crate) fn native_writer_enforcement_version(&self) -> Result<u32> {
        self.storage_policy.native_writer_enforcement_version()
    }
    pub(crate) fn prepare_native_host_writer_enrollment(
        &self,
        database: &BicDb,
        metadata: HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<SealedHostWriterEnrollment> {
        self.require_native_catalog_database(database)?;
        if self.native_writer_enforcement_version()? != 1 {
            return Err(data_error(
                "host enrollment requires an actual from-birth enforced service",
            ));
        }
        let policy = SealedHostWriterEnrollment::from_actual(
            metadata,
            business,
            database,
            &self.range_writes,
        )?;
        self.validate_native_writer_enrollment(database, &policy)?;
        Ok(policy)
    }
    pub(crate) fn validate_native_writer_enrollment(
        &self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.require_native_catalog_database(database)?;
        policy.require_live()?;
        database.require_native_writer_catalogue_custody(&self.range_writes)?;
        if self.native_writer_enforcement_version()? != 1
            || database.actual_native_writer_enforcement_version()? != 1
            || policy.database_incarnation() != database.incarnation_id()
            || policy.store_open_id() != self.range_writes.native_writer_store_open_id()
            || policy.facts().cluster_id != self.cluster_id
            || policy.facts().local_node_id != self.node_id
            || database
                .schema_compatibility_authority()
                .verified_sha256()
                .as_deref()
                != Some(policy.facts().schema_sha256.as_str())
        {
            return Err(data_error(
                "host enrollment is not for this actual service/store/database",
            ));
        }
        Ok(())
    }
    pub(crate) fn install_native_writer_host_state(
        &mut self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
        state: Weak<NativeWriterHostState>,
    ) -> Result<()> {
        self.validate_native_writer_enrollment(database, policy)?;
        self.range_writes
            .current_mut()
            .install_native_writer_host_state(database, policy, state)
    }
    pub(crate) fn require_native_writer_host_for_range(
        &self,
        database: &BicDb,
        range: &RangeDescriptor,
    ) -> Result<()> {
        if self.native_writer_enforcement_version()? == 0 {
            return Ok(());
        }
        self.require_native_catalog_database(database)?;
        self.range_writes.require_native_writer_host_base()?;
        self.range_writes
            .native_writer_host_state()?
            .require_database(database, range)
    }
    pub(super) fn require_native_writer_scope_without_database(
        &self,
        range: &RangeDescriptor,
    ) -> Result<()> {
        if self.native_writer_enforcement_version()? == 0 {
            return Ok(());
        }
        self.range_writes.require_native_writer_host_base()?;
        self.range_writes
            .native_writer_host_state()?
            .require_range(range)
    }
}

impl ClusterDataNodeService {
    pub(crate) fn retain_initial_data_writer<
        T: crate::distribution_range_consensus::RangeWriteTransport,
    >(
        &self,
        coordinator: &Arc<crate::distribution_range_consensus::RangeWriteCoordinator<T>>,
        guard: &crate::db::InstalledNativeWriterHostGuard,
        range: &RangeDescriptor,
    ) -> Result<(Arc<crate::db::NativeWriterHostState>, std::path::PathBuf)> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        guard.require_for_coordinator(&database, range, coordinator)?;
        let writer = self.range_writes.native_writer_host_state()?;
        writer.require_database(&database, range)?;
        writer.require_store(
            database.data_path(),
            self.range_writes.native_writer_store_open_id(),
        )?;
        Ok((writer, database.data_path().to_path_buf()))
    }
}

/// Single actual-service production. No DTO/parts/Clone constructor; contains
/// the original live policy, actual current slot and original writer object.
pub(crate) struct NativeWriterRegistryRefresh {
    policy: SealedHostWriterEnrollment,
    audit: serde_json::Value,
    writer: Arc<NativeWriterHostState>,
    service: Weak<Mutex<ClusterDataNodeService>>,
    slot: super::RangeWriteServiceSlotGuard,
}
impl NativeWriterRegistryRefresh {
    pub(crate) fn require_request_and_slot_lifetime(&self) -> Result<()> {
        self.slot.require_live()?;
        self.policy.require_live()
    }
    pub(crate) fn policy(&self) -> &SealedHostWriterEnrollment {
        &self.policy
    }
    pub(crate) fn audit(&self) -> &serde_json::Value {
        &self.audit
    }
    pub(crate) fn require_current<
        T: crate::distribution_range_consensus::RangeWriteTransport + 'static,
    >(
        &self,
        database: &BicDb,
        coordinator: &Arc<crate::distribution_range_consensus::RangeWriteCoordinator<T>>,
        guard: &crate::db::InstalledNativeWriterHostGuard,
    ) -> Result<()> {
        self.with_current_for_publication(database, coordinator, guard, || Ok(()))
    }
    pub(crate) fn with_current_for_publication<
        T: crate::distribution_range_consensus::RangeWriteTransport + 'static,
    >(
        &self,
        database: &BicDb,
        coordinator: &Arc<crate::distribution_range_consensus::RangeWriteCoordinator<T>>,
        guard: &crate::db::InstalledNativeWriterHostGuard,
        publish: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.policy.require_live()?;
        self.slot.require_live()?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| data_error("registry refresh actual service ended"))?;
        coordinator.require_registry_refresh_service(&service)?;
        let service = service
            .try_lock()
            .ok_or_else(|| data_error("registry refresh actual service busy"))?;
        service.validate_native_writer_enrollment(database, &self.policy)?;
        let current = service.range_writes.native_writer_host_state()?;
        if !Arc::ptr_eq(&current, &self.writer)
            || service.range_writes.native_writer_store_open_id() != self.slot.id()
            || self.policy.store_open_id() != self.slot.id()
        {
            return Err(data_error(
                "registry refresh current Store/writer object changed",
            ));
        }
        guard.require_registry_refresh_origin(database, coordinator, &current)?;
        self.slot.require_live()?;
        self.policy.require_live()?;
        publish()
    }
}
impl ClusterDataNodeService {
    pub(crate) fn prepare_native_writer_registry_refresh<
        T: crate::distribution_range_consensus::RangeWriteTransport + 'static,
    >(
        actual_service: &Arc<Mutex<Self>>,
        database: &BicDb,
        coordinator: &Arc<crate::distribution_range_consensus::RangeWriteCoordinator<T>>,
        guard: &crate::db::InstalledNativeWriterHostGuard,
        metadata: HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<NativeWriterRegistryRefresh> {
        coordinator.require_registry_refresh_service(actual_service)?;
        // Validate actual old/new quorum stability before native/schema work.
        metadata.stable_writer_registry_audit(business)?;
        let service = actual_service
            .try_lock()
            .ok_or_else(|| data_error("registry refresh service busy before preparation"))?;
        let writer = service.range_writes.native_writer_host_state()?;
        guard.require_registry_refresh_origin(database, coordinator, &writer)?;
        let slot = service.native_replica_listener_store_open_guard();
        slot.require_live()?;
        let policy = service.prepare_native_host_writer_enrollment(database, metadata, business)?;
        let audit = policy.stable_registry_audit()?;
        policy.require_live()?;
        Ok(NativeWriterRegistryRefresh {
            policy,
            audit,
            writer,
            service: Arc::downgrade(actual_service),
            slot,
        })
    }
}
