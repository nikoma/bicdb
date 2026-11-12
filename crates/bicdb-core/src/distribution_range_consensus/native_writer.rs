//! Installed native host policy gates. These never replace command/role fences.
use super::*;
use crate::db::NativeWriterHostState;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use std::sync::Weak;

impl RangeWriteStore {
    pub(crate) fn native_writer_store_open_id(&self) -> Uuid {
        self.open_id
    }
    pub(crate) fn native_writer_host_state(&self) -> Result<Arc<NativeWriterHostState>> {
        self.native_writer_host
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| range_write_error("native host writer policy is not installed"))
    }
    pub(crate) fn require_native_writer_host_base(&self) -> Result<()> {
        if !crate::format::native_writer_enrollment_active(&self.root)? {
            return Ok(());
        }
        if !self.fsync {
            return Err(range_write_error(
                "enforced native writer catalog requires fsync",
            ));
        }
        let state = self
            .native_writer_host
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                range_write_error("enforced native writer host policy is not installed")
            })?;
        state.require_store(&self.root, self.open_id)
    }
    pub(crate) fn require_native_writer_host_range_id(&self, range_id: RangeId) -> Result<()> {
        self.require_native_writer_host_base()?;
        if crate::format::native_writer_enrollment_active(&self.root)? {
            let state = self
                .native_writer_host
                .as_ref()
                .and_then(Weak::upgrade)
                .ok_or_else(|| range_write_error("native writer host policy ended"))?;
            state.require_range_id(range_id)?;
        }
        Ok(())
    }
    fn require_native_writer_command(&self, command: &RangeWriteCommand) -> Result<()> {
        self.require_native_writer_host_base()?;
        if crate::format::native_writer_enrollment_active(&self.root)? {
            self.native_writer_host
                .as_ref()
                .and_then(Weak::upgrade)
                .ok_or_else(|| range_write_error("native writer host policy ended"))?
                .require_command(command)?;
        }
        Ok(())
    }
    pub(crate) fn install_native_writer_host_state(
        &mut self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
        state: Weak<NativeWriterHostState>,
    ) -> Result<()> {
        self.require_native_catalog_durability(database.data_path())?;
        if self.open_id != policy.store_open_id()
            || self.catalog.cluster_id != policy.facts().cluster_id
            || self.catalog.node_id != policy.facts().local_node_id
        {
            return Err(range_write_error(
                "host policy targets another actual catalog/open",
            ));
        }
        self.native_writer_host = Some(state);
        Ok(())
    }
    // Only open() calls this for already-validated, unchanged refusal data.
    // It cannot manufacture an enforced v1→QC migration or expose a data ACK.
    pub(super) fn persist_verified_open_catalog(&self) -> Result<()> {
        let envelope = RangeWriteEnvelope::new(self.catalog.clone())?;
        crate::storage::write_atomic(
            &self.root.join(DEFAULT_RANGE_WRITE_LOG),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )
    }
}
impl<T: RangeWriteTransport> RangeWriteCoordinator<T> {
    pub(crate) fn native_writer_coordinator_id(&self) -> Uuid {
        self.native_coordinator_id
    }
    pub(crate) fn require_native_writer_host_context(
        &self,
        database: &BicDb,
        range: &RangeDescriptor,
    ) -> Result<()> {
        if database.actual_native_writer_enforcement_version()? == 0 {
            return Ok(());
        }
        let state = self
            .native_writer_host
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| range_write_error("native coordinator host policy is not installed"))?;
        if database.incarnation_id() != self.local_incarnation
            || state.coordinator_id() != self.native_coordinator_id
        {
            return Err(range_write_error(
                "native coordinator DB/open identity changed",
            ));
        }
        state.require_database(database, range)?;
        state.require_topology(&self.topology.read())
    }
    pub(crate) fn validate_native_writer_enrollment(
        &self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        policy.require_live()?;
        if !self.native_image_mode
            || database.actual_native_writer_enforcement_version()? != 1
            || database.incarnation_id() != self.local_incarnation
            || policy.database_incarnation() != self.local_incarnation
            || policy.facts().cluster_id != self.cluster_id
            || policy.facts().local_node_id != self.local_node_id
        {
            return Err(range_write_error(
                "host policy and actual native coordinator differ",
            ));
        }
        {
            let topology = self.topology.read();
            policy.facts().require_topology(&topology)?;
            for range in topology.ranges.values() {
                policy.facts().require_range(range)?;
            }
        }
        let service = self
            .local_service
            .try_lock()
            .ok_or_else(|| range_write_error("service busy while installing host policy"))?;
        service.validate_native_writer_enrollment(database, policy)
    }
    pub(crate) fn install_native_writer_host_state(
        &self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
        state: Weak<NativeWriterHostState>,
    ) -> Result<()> {
        self.validate_native_writer_enrollment(database, policy)?;
        self.local_service
            .try_lock()
            .ok_or_else(|| range_write_error("service busy while publishing host policy"))?
            .install_native_writer_host_state(database, policy, state.clone())?;
        *self.native_writer_host.lock() = Some(state);
        Ok(())
    }
}
