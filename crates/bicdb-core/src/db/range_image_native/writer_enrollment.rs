//! Actual from-birth host-policy installation; no DTO/read permit constructor.
use super::*;
use crate::db::commit_admission_hook::CommitAdmissionRegistration;
use crate::distribution_native_writer::{HostWriterPolicyFacts, SealedHostWriterEnrollment};
use crate::distribution_range_consensus::{RangeWriteCoordinator, RangeWriteTransport};
use std::sync::Weak;

const POLICY_POINTER: &str = "writer-enrollment/current";
mod registry_refresh;

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
    registry_extension:
        parking_lot::RwLock<Option<Arc<registry_refresh::VerifiedRegistryExtension>>>,
    registry_publication_pending: std::sync::atomic::AtomicBool,
}
impl NativeWriterHostState {
    pub(crate) fn registration_generation(&self) -> Uuid {
        self.record.generation
    }
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
    /// Necessary original writer/registration lifetime refusal for an owned
    /// read observation. Full actual DB/hook/registry checks remain scoped.
    pub(in crate::db::range_image_native) fn require_read_lifetime(&self) -> Result<()> {
        self.require_lifetime().map(|_| ())
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
        self.require_registry_topology(topology)
    }
    /// Necessary mutable-boundary refusal inside an actual activation journal
    /// scope. Full policy/birth/registry validation remains at scope entry/exit.
    pub(in crate::db::range_image_native) fn require_activation_lifetime(
        &self,
        database: &BicDb,
        range: &RangeDescriptor,
    ) -> Result<()> {
        let registration = self.require_lifetime()?;
        database.check_recovery_state()?;
        if database.incarnation_id() != self.record.database_incarnation
            || database.data_path().canonicalize()? != self.root
        {
            return Err(invalid("native writer database/open changed"));
        }
        let hook = database.commit_admission.read();
        if !Arc::ptr_eq(hook.registration(), &registration)
            || hook
                .native_writer()
                .is_none_or(|state| !std::ptr::eq(state.as_ref(), self))
        {
            return Err(invalid(
                "native writer policy is not the actual installed hook",
            ));
        }
        self.require_range(range)
    }
    /// Necessary read boundary only: same existing original registration,
    /// DB/open, hook, schema/health and immutable DATA policy checks. This
    /// unit preflight grants neither activation nor a serving observation.
    pub(in crate::db::range_image_native) fn require_read_database_lifetime(
        &self,
        database: &BicDb,
        range: &RangeDescriptor,
    ) -> Result<()> {
        self.require_activation_lifetime(database, range)
    }
    pub(crate) fn require_database(&self, database: &BicDb, range: &RangeDescriptor) -> Result<()> {
        self.require_original_database(database, range)?;
        self.require_verified_registry_database(database)
    }
    // Only registry publication may reconcile a durable-but-unpublished audit.
    // This still proves the actual original DB/hook/policy/open, not a role.
    fn require_original_database(&self, database: &BicDb, range: &RangeDescriptor) -> Result<()> {
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
    /// Read-only diagnostic DATA from the retained actual installation. No
    /// DB/service/native-journal reacquisition or role/readiness authority.
    /// The bound registration may have been revoked; its ID is not a grant.
    pub(crate) fn binding_diagnostic(&self) -> serde_json::Value {
        let observed = self.state.registration.upgrade();
        let actual = observed
            .as_ref()
            .and_then(|registration| registration.authority());
        let expected = self.state.coordinator.upgrade();
        let matches = actual
            .as_ref()
            .zip(expected.as_ref())
            .map(|(actual, expected)| Arc::ptr_eq(actual, expected));
        let lifetime = self.state.require_lifetime();
        serde_json::json!({
            "scope":"diagnostic retained binding only; not linearizable Ready/role or a permission",
            "persisted_policy_coordinator_id":self.state.record.coordinator_id,
            "persisted_policy_registration_generation":self.state.record.generation,
            "persisted_policy_database_open_incarnation":self.state.record.database_incarnation,
            "persisted_policy_range_store_open_id":self.state.record.store_open_id,
            "persisted_policy_checksum_sha256":self.state.record.checksum_sha256,
            "observed_bound_registration_generation":observed.as_ref().map(|registration| registration.generation()),
            "observed_bound_registration_active":observed.as_ref().map(|registration| registration.is_active()),
            "observed_bound_authority_matches_retained_coordinator":matches,
            "observed_weak_directory_alive":self.state.directory.upgrade().is_some(),
            "observed_binding_lifetime_live":lifetime.is_ok(),
            "observed_binding_liveness_error":lifetime.err().map(|error|error.to_string()),
            "actual_verified_registry_extension":self.state.registry_diagnostic(),
            "current_database_hook_slot_not_relocked":true,
            "identity_from_status_dto_is_not_authority":true,
        })
    }
    pub(crate) fn writer_enforcement_version(&self) -> u32 {
        1
    }
    /// Retire only THIS actual installation after its host has drained. The
    /// commit gate and hook lock serialize comparison and clearing with new
    /// installs, including the SAME coordinator Arc reinstalled under a new
    /// registration. No native journal, range permit or network is acquired.
    pub(crate) fn retire_if_current<T: RangeWriteTransport>(
        &self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
    ) -> Result<bool> {
        // Revocation must work even after a durability/recovery failure. No
        // native reads, health/ready check or filesystem lookup belongs here:
        // the immutable actual-open ID plus exact private slot identities
        // below suffice to revoke THIS hook, and cannot grant any authority.
        if database.incarnation_id() != self.state.record.database_incarnation
            || coordinator.native_writer_coordinator_id() != self.state.record.coordinator_id
        {
            return Err(invalid(
                "cannot retire a writer from another actual database/coordinator",
            ));
        }
        let expected: Arc<dyn CommitAdmission> = coordinator.clone();
        if self
            .state
            .coordinator
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, &expected))
        {
            return Err(invalid("cannot retire another actual writer Arc"));
        }
        let Some(registration) = self.state.registration.upgrade() else {
            return Ok(false);
        };
        let _quiesced = database
            .serializable_commit_admission
            .try_write()
            .ok_or_else(|| {
                invalid("actual transactions still draining before writer retirement")
            })?;
        let mut hook = database
            .commit_admission
            .try_write()
            .ok_or_else(|| invalid("actual writer installation busy during retirement"))?;
        if !Arc::ptr_eq(hook.registration(), &registration)
            || hook
                .native_writer()
                .is_none_or(|actual| !Arc::ptr_eq(actual, &self.state))
            || hook
                .as_ref()
                .is_none_or(|actual| !Arc::ptr_eq(actual, &expected))
        {
            return Ok(false);
        }
        hook.install(None);
        Ok(true)
    }
    /// Cleanup-only liveness: no registration means no slot can still own
    /// this installation. This cannot become a writer/read permission.
    pub(crate) fn registration_ended(&self) -> bool {
        self.state.registration.upgrade().is_none()
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
    /// Existing native WAL policy is an irreversible ever-enrolled refusal fact,
    /// not permission to replace missing decisions with an empty catalogue.
    /// Uses only native record reads: safe under the caller's existing journal.
    pub(crate) fn require_native_writer_catalogue_custody(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        if self.actual_native_writer_enforcement_version()? != 1 {
            return Err(invalid("catalogue custody requires actual enforced birth"));
        }
        if !format::native_catalogue_custody_active(self.data_path())? {
            return Err(BicDbError::FormatCompatibility("native writer requires catalogue-custody reader fence; older prototype roots require authorized recovery".into()));
        }
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("catalogue custody lost actual birth"))?;
        if let Some(previous) =
            self.read_native_range_image_record::<NativeWriterPolicyRecord>(POLICY_POINTER)?
        {
            previous.validate()?;
            if previous.facts.local_birth_id != birth.birth_id
                || previous.facts.local_birth_sha256 != birth.checksum_sha256
            {
                return Err(invalid("prior host policy has another actual native birth"));
            }
        }
        store.require_verified_native_catalogue_custody(self.data_path())
    }
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
        if self.actual_native_writer_enforcement_version()? == 0 {
            return Ok(());
        }
        // A queue-only transaction has no record routing key and must not
        // bypass the range protocol merely because writes is empty. Mixed
        // record/broker transactions likewise need effects in the replicated
        // command before they can promise a durable distributed outcome.
        if !transaction.pending_broker_publishes.lock().is_empty() {
            return Err(BicDbError::Unsupported(
                "native range commands do not yet replicate broker effects; transaction refused before range Prepare or application WAL".into(),
            ));
        }
        if transaction.writes.is_empty() {
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
            registry_extension: parking_lot::RwLock::new(None),
            registry_publication_pending: std::sync::atomic::AtomicBool::new(false),
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
