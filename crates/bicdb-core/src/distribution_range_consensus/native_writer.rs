//! Installed native host policy gates. These never replace command/role fences.
use super::*;
use crate::db::NativeWriterHostState;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use crate::BicDb;
use std::sync::Weak;
pub(super) const NATIVE_WRITER_CATALOG_FORMAT: u32 = 12;

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
        let enforced = crate::format::native_writer_enrollment_active(&self.root)?;
        if !enforced
            && self.catalog.native_writer_enforcement_version.is_none()
            && self.catalog.format_version != NATIVE_WRITER_CATALOG_FORMAT
        {
            return Ok(());
        }
        if !enforced
            || self.catalog.native_writer_enforcement_version != Some(1)
            || self.catalog.format_version < NATIVE_WRITER_CATALOG_FORMAT
        {
            return Err(range_write_error(
                "native writer catalog lacks its from-birth reader fence",
            ));
        }
        if !self.fsync {
            return Err(range_write_error(
                "enforced native writer catalog requires fsync",
            ));
        }
        if !self.catalogue_loaded_at_open {
            return Err(BicDbError::NativeRangeCatalogueCustodyMissing);
        }
        if !crate::format::native_catalogue_custody_active(&self.root)? {
            return Err(range_write_error(
                "enforced native writer lacks catalogue custody reader fence",
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
    pub(super) fn require_native_writer_command(&self, command: &RangeWriteCommand) -> Result<()> {
        self.require_native_writer_host_base()?;
        self.native_joint_command_allowed(command)?;
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
        if !self.catalogue_loaded_at_open {
            return Err(BicDbError::NativeRangeCatalogueCustodyMissing);
        }
        let envelope = RangeWriteEnvelope::new(self.catalog.clone())?;
        crate::storage::write_atomic(
            &self.root.join(DEFAULT_RANGE_WRITE_LOG),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )
    }
}
impl<T: RangeWriteTransport> RangeWriteCoordinator<T> {
    fn expected_enforced_native_store(&self) -> bool {
        self.topology
            .read()
            .nodes
            .get(&self.local_node_id)
            .and_then(|node| node.native_store_birth_v1.as_ref())
            .is_some_and(|birth| birth.writer_enforcement_version() == 1)
    }
    // Metadata may require a store, but cannot manufacture that store's birth.
    pub(super) fn require_expected_native_database(&self, database: Option<&BicDb>) -> Result<()> {
        if self.expected_enforced_native_store()
            && database
                .map(BicDb::actual_native_writer_enforcement_version)
                .transpose()?
                != Some(1)
        {
            return Err(range_write_error(
                "registered enforced native coordinator requires its actual born-v2 database",
            ));
        }
        Ok(())
    }
    pub(super) fn require_expected_native_recovery_store(&self) -> Result<()> {
        if self.expected_enforced_native_store() {
            let service = self.local_service.try_lock().ok_or_else(|| {
                range_write_error("service busy before native recovery store check")
            })?;
            if service.native_writer_enforcement_version()? != 1 {
                return Err(range_write_error(
                    "registered enforced native coordinator cannot recover a legacy store",
                ));
            }
        }
        Ok(())
    }
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
