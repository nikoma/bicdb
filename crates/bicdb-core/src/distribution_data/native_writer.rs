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
