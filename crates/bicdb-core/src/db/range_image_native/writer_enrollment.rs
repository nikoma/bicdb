//! Actual from-birth host-policy installation; no DTO/read permit constructor.
use super::*;
use crate::db::commit_admission_hook::CommitAdmissionRegistration;
use crate::distribution_native_writer::{HostWriterPolicyFacts, SealedHostWriterEnrollment};
use crate::distribution_range_consensus::{RangeWriteCoordinator, RangeWriteTransport};
use std::sync::Weak;

const POLICY_POINTER: &str = "writer-enrollment/current";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeWriterPolicyRecord {
    version: u32,
    generation: Uuid,
    coordinator_id: Uuid,
    database_incarnation: String,
    store_open_id: Uuid,
    facts: HostWriterPolicyFacts,
    metadata_context_id: Uuid,
    metadata_term: u64,
    metadata_read_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl NativeWriterPolicyRecord {
    fn checksum(&self) -> Result<String> {
        let mut copy = self.clone();
        copy.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &copy,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.generation.is_nil()
            || self.coordinator_id.is_nil()
            || self.store_open_id.is_nil()
            || self.database_incarnation.is_empty()
            || self.metadata_context_id.is_nil()
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || self.native_page_xid == 0
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged native host writer policy receipt"));
        }
        Ok(())
    }
}

/// Weak lifetime bindings prevent coor→service→policy→coor/DB cycles. This
/// state has no public constructor and is published only with the actual hook.
#[derive(Debug)]
pub(crate) struct NativeWriterHostState {
    record: NativeWriterPolicyRecord,
    root: PathBuf,
    directory: Weak<bicdb_page::DirectoryLock>,
    health: Weak<bicdb_page::DurabilityHealth>,
    schema: SchemaCompatibilityAuthority,
    schema_generation: u64,
    registration: Weak<CommitAdmissionRegistration>,
    coordinator: Weak<dyn CommitAdmission>,
}
impl NativeWriterHostState {
    fn require_lifetime(&self) -> Result<Arc<CommitAdmissionRegistration>> {
        let registration = self
            .registration
            .upgrade()
            .ok_or_else(|| invalid("native writer hook lifetime ended"))?;
        if !registration.is_active()
            || registration.generation() != self.record.generation
            || self.directory.upgrade().is_none()
        {
            return Err(invalid("native writer hook/open is no longer current"));
        }
        let actual = registration
            .authority()
            .ok_or_else(|| invalid("native writer authority disappeared"))?;
        let expected = self
            .coordinator
            .upgrade()
            .ok_or_else(|| invalid("native writer coordinator ended"))?;
        if !Arc::ptr_eq(&actual, &expected) {
            return Err(invalid("native writer coordinator registration changed"));
        }
        if self.schema.generation() != self.schema_generation
            || self.schema.verified_sha256().as_deref()
                != Some(self.record.facts.schema_sha256.as_str())
        {
            return Err(invalid(
                "native writer schema changed; refresh actual host policy",
            ));
        }
        self.health
            .upgrade()
            .ok_or_else(|| invalid("native database lifetime ended"))?
            .check()
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
        Ok(registration)
    }
    pub(crate) fn require_store(&self, root: &Path, open_id: Uuid) -> Result<()> {
        self.require_lifetime()?;
        if open_id != self.record.store_open_id
            || root.canonicalize()? != self.root
            || !format::native_writer_enrollment_active(root)?
        {
            return Err(invalid("native writer store/root/open changed"));
        }
        Ok(())
    }
    pub(crate) fn coordinator_id(&self) -> Uuid {
        self.record.coordinator_id
    }
    pub(crate) fn require_range_id(&self, range: RangeId) -> Result<()> {
        if !self.record.facts.ranges.contains_key(&range) {
            return Err(invalid("range is outside actual host writer policy"));
        }
        Ok(())
    }
    pub(crate) fn require_range(&self, range: &RangeDescriptor) -> Result<()> {
        self.record.facts.require_range(range)
    }
    pub(crate) fn require_command(
        &self,
        command: &crate::distribution_range_consensus::RangeWriteCommand,
    ) -> Result<()> {
        let range = self
            .record
            .facts
            .ranges
            .get(&command.range_id)
            .ok_or_else(|| invalid("native command range absent from host policy"))?;
        if range.epoch != command.range_epoch || range.leader != command.leader_node_id {
            return Err(invalid(
                "native command configuration differs from installed host policy",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_topology(&self, topology: &crate::ClusterTopology) -> Result<()> {
        self.record.facts.require_topology(topology)
    }
    pub(crate) fn require_database(&self, database: &BicDb, range: &RangeDescriptor) -> Result<()> {
        self.require_lifetime()?;
        database.check_recovery_state()?;
        if database.incarnation_id() != self.record.database_incarnation
            || database.data_path().canonicalize()? != self.root
        {
            return Err(invalid("native writer database/open changed"));
        }
        let facts = &self.record.facts;
        let native = database.native_recovery_admission(
            &facts.cluster_id,
            facts.bootstrap_id,
            &facts.local_node_id,
            facts.local_incarnation,
        )?;
        if native.writer_enforcement_version() != 1
            || native.birth_id() != facts.local_birth_id
            || native.birth_sha256() != facts.local_birth_sha256
        {
            return Err(invalid("native writer birth/enforcement changed"));
        }
        let hook = database.commit_admission.read();
        let registration = self
            .registration
            .upgrade()
            .ok_or_else(|| invalid("native writer registration ended"))?;
        if !Arc::ptr_eq(hook.registration(), &registration)
            || hook
                .native_writer()
                .is_none_or(|state| !std::ptr::eq(state.as_ref(), self))
        {
            return Err(invalid(
                "native writer policy is not the actual installed hook",
            ));
        }
        let stored: NativeWriterPolicyRecord = database
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| invalid("native host writer receipt missing"))?;
        stored.validate()?;
        if stored != self.record {
            return Err(invalid("native host writer receipt changed"));
        }
        self.require_range(range)
    }
}

/// Necessary installed-host continuity proof. It grants no current role,
/// snapshot, data quorum or row/absence permission.
#[derive(Debug)]
pub(crate) struct InstalledNativeWriterHostGuard {
    state: Arc<NativeWriterHostState>,
}
impl InstalledNativeWriterHostGuard {
    pub(crate) fn writer_enforcement_version(&self) -> u32 {
        1
    }
    pub(crate) fn require_current(&self, database: &BicDb, range: &RangeDescriptor) -> Result<()> {
        self.state.require_database(database, range)
    }
    pub(crate) fn require_for_coordinator<T: RangeWriteTransport + 'static>(
        &self,
        database: &BicDb,
        range: &RangeDescriptor,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
    ) -> Result<()> {
        self.require_current(database, range)?;
        let actual: Arc<dyn CommitAdmission> = coordinator.clone();
        let installed = self
            .state
            .coordinator
            .upgrade()
            .ok_or_else(|| invalid("native writer coordinator ended"))?;
        if !Arc::ptr_eq(&actual, &installed)
            || coordinator.native_writer_coordinator_id() != self.state.record.coordinator_id
        {
            return Err(invalid(
                "read reservation coordinator is not the actual installed writer",
            ));
        }
        coordinator.require_native_writer_host_context(database, range)
    }
}

impl BicDb {
    pub(crate) fn actual_native_writer_enforcement_version(&self) -> Result<u32> {
        self.check_recovery_state()?;
        Ok(self
            .read_native_birth()?
            .map_or(0, |birth| u32::from(birth.format_version == 2)))
    }
    pub(in crate::db) fn require_installed_native_writer_database_for_range(
        &self,
        range: &RangeDescriptor,
    ) -> Result<()> {
        if self.actual_native_writer_enforcement_version()? == 0 {
            return Ok(());
        }
        let state = self
            .commit_admission
            .read()
            .native_writer()
            .cloned()
            .ok_or_else(|| invalid("born-v2 database has no actual installed writer policy"))?;
        state.require_database(self, range)
    }
    pub(in crate::db) fn require_enforced_transaction_preflight(
        &self,
        transaction: &Transaction,
    ) -> Result<()> {
        if transaction.writes.is_empty() || self.actual_native_writer_enforcement_version()? == 0 {
            return Ok(());
        }
        if transaction.bypass_commit_admission {
            return self.require_native_transaction_mutation_scope(transaction);
        }
        let state = self
            .commit_admission
            .read()
            .native_writer()
            .cloned()
            .ok_or_else(|| {
                invalid("born-v2 application commit has no actual installed writer policy")
            })?;
        for write in &transaction.writes {
            let range = state
                .record
                .facts
                .ranges
                .values()
                .find(|r| {
                    r.contains_token(crate::distribution::distribution_key_token(
                        &write.collection,
                        &write.record_id,
                    ))
                })
                .ok_or_else(|| invalid("application mutation has no registered native range"))?;
            state.require_database(self, range)?;
        }
        Ok(())
    }
    pub(in crate::db) fn require_native_generic_import_allowed(&self) -> Result<()> {
        if self.actual_native_writer_enforcement_version()? != 0 {
            return Err(invalid(
                "born-v2 generic import requires an actual native Apply/image capability",
            ));
        }
        Ok(())
    }
    /// Call outside service locks. All network/MQ work has already completed.
    pub(crate) fn install_enrolled_native_coordinator<T: RangeWriteTransport + 'static>(
        &self,
        coordinator: Arc<RangeWriteCoordinator<T>>,
        policy: SealedHostWriterEnrollment,
    ) -> Result<InstalledNativeWriterHostGuard> {
        let _admission = self.serializable_commit_admission.write();
        policy.require_live()?;
        if self.actual_native_writer_enforcement_version()? != 1
            || self.incarnation_id() != policy.database_incarnation()
        {
            return Err(invalid(
                "host policy needs the same actual born-v2 database/open",
            ));
        }
        coordinator.validate_native_writer_enrollment(self, &policy)?;
        let authority: Arc<dyn CommitAdmission> = coordinator.clone();
        let registration = CommitAdmissionRegistration::new(Some(&authority));
        let mut record = NativeWriterPolicyRecord {
            version: 1,
            generation: registration.generation(),
            coordinator_id: coordinator.native_writer_coordinator_id(),
            database_incarnation: self.incarnation_id().into(),
            store_open_id: policy.store_open_id(),
            facts: policy.facts().clone(),
            metadata_context_id: policy.metadata_context_id(),
            metadata_term: policy.metadata_term(),
            metadata_read_index: policy.metadata_read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        let _journal = self.try_native_image_journal()?;
        let previous: Option<NativeWriterPolicyRecord> =
            self.read_native_range_image_record(POLICY_POINTER)?;
        if let Some(previous) = &previous {
            previous.validate()?;
            if previous.facts.local_birth_id != record.facts.local_birth_id
                || previous.facts.local_birth_sha256 != record.facts.local_birth_sha256
            {
                return Err(invalid(
                    "native writer policy history belongs to another physical birth",
                ));
            }
        }
        // Native publication can become durable before later hook publication
        // fails. Never leave the previous raw-store grant active in that gap.
        self.commit_admission.write().deactivate_native_writer();
        self.in_paged_transaction(|paged, xid| {
            policy.require_live()?;
            record.native_page_xid = xid;
            record.checksum_sha256 = record.checksum()?;
            record.validate()?;
            if let Some(previous) = &previous {
                paged
                    .store()
                    .put(
                        xid,
                        &native_key("writer-enrollment/previous"),
                        &bounded_json(previous, MAX_NATIVE_RECEIPT_BYTES)?,
                    )
                    .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            }
            paged
                .store()
                .put(
                    xid,
                    &native_key(POLICY_POINTER),
                    &bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?,
                )
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("host writer policy has no native page store"))?;
        policy.require_live()?;
        let stored: NativeWriterPolicyRecord = self
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| invalid("host writer receipt missing after native WAL"))?;
        stored.validate()?;
        if stored != record {
            return Err(invalid("host writer receipt changed after publication"));
        }
        let state = Arc::new(NativeWriterHostState {
            record,
            root: self.path.canonicalize()?,
            directory: self
                .native_service_directory_lifetime()
                .ok_or_else(|| invalid("native writer DB has no directory lifetime"))?,
            health: Arc::downgrade(&self.durability),
            schema: self.schema_compatibility_authority(),
            schema_generation: self.schema_compatibility.generation(),
            registration: Arc::downgrade(&registration),
            coordinator: Arc::downgrade(&authority),
        });
        #[cfg(test)]
        if TEST_FAIL_HOST_POLICY_PUBLICATION.with(|flag| flag.replace(false)) {
            return Err(invalid(
                "injected host policy failure after actual native WAL publication",
            ));
        }
        coordinator.install_native_writer_host_state(self, &policy, Arc::downgrade(&state))?;
        policy.require_live()?;
        // The registration remains inactive until policy, authority and hook
        // are atomically published. Earlier weak store bindings cannot ACK.
        self.commit_admission
            .write()
            .install_enrolled(authority, registration, Arc::clone(&state));
        Ok(InstalledNativeWriterHostGuard { state })
    }
}

#[cfg(test)]
thread_local! { static TEST_FAIL_HOST_POLICY_PUBLICATION:std::cell::Cell<bool>=const { std::cell::Cell::new(false) }; }
#[cfg(test)]
impl BicDb {
    pub(crate) fn test_fail_next_native_host_policy_publication(&self) {
        TEST_FAIL_HOST_POLICY_PUBLICATION.with(|flag| flag.set(true));
    }
}

#[cfg(test)]
mod tests;
