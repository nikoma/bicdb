//! Retained SAME-State CLOSED publication. No ordinary range permission.
use super::*;
use crate::distribution_data::{
    ClusterDataNodeService, ClusterServiceOpenGuard, NativeJointServingLease,
    RangeWriteServiceSlotGuard,
};
use crate::distribution_native_host::NativeDataOwnerAdmission;
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use parking_lot::{Mutex, MutexGuard};

pub(crate) struct NativeWriterDataDomainTransition {
    writer: Arc<NativeWriterHostState>,
    coordinator: Weak<dyn CommitAdmission>,
    service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    metadata: Option<Weak<Mutex<crate::distribution_consensus::MetadataConsensusStore>>>,
    owner: NativeDataOwnerAdmission,
    slot: RangeWriteServiceSlotGuard,
    open: ClusterServiceOpenGuard,
    caller: CancellationToken,
    range: RangeId,
    lease: Option<NativeJointServingLease>,
    publication: ClosedPublication,
}
enum ClosedPublication {
    Unwritten,
    Prepared(ClosedDomainRecord),
    Uncertain(ClosedDomainRecord),
    NativeVerified(Arc<NativeClosedWriterDomain>),
    Verified(Arc<NativeClosedWriterDomain>),
}
/// Facts on the ORIGINAL State, not a fresh MQ or serving/readiness grant.
/// No Deserialize/parts constructor or strong DB/coordinator/Frozen ownership.
pub(crate) struct NativeClosedWriterDomain {
    pub(super) writer: Arc<NativeWriterHostState>,
    pub(super) record: ClosedDomainRecord,
    owner: NativeDataOwnerAdmission,
    pub(super) service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    pub(super) metadata: Weak<Mutex<crate::distribution_consensus::MetadataConsensusStore>>,
    caller: CancellationToken,
    pub(super) slot: RangeWriteServiceSlotGuard,
    open: ClusterServiceOpenGuard,
}
impl NativeClosedWriterDomain {
    pub(crate) fn actual_writer(&self) -> &Arc<NativeWriterHostState> {
        &self.writer
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        self.record.decision.activation_range()
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.record.decision
    }
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.caller.check()?;
        if self.metadata.upgrade().is_none() {
            return Err(invalid("CLOSED writer original metadata owner ended"));
        }
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("CLOSED writer actual service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("CLOSED writer actual runtime ended"))?;
        self.owner.require_listener(&service, &runtime)?;
        self.slot.require_live()?;
        self.open.require_live()?;
        self.writer
            .require_store(&self.writer.root, self.slot.id())?;
        self.record.validate(&self.writer.record)
    }
    pub(crate) fn require_current_for_store(
        &self,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        range: &RangeDescriptor,
    ) -> Result<()> {
        if crate::distribution_native_read::canonical_range(range) != *self.range() {
            return Err(invalid("CLOSED writer changed its selected range"));
        }
        self.require_current(db)?;
        store.require_closed_writer_promise(
            self.data(),
            &self.record.local.local_promise,
            self.record.local.old_close_sha256.as_deref(),
        )
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.require_lifetime()?;
        self.open.require_database(db)?;
        self.writer.require_original_identity_database(db)?;
        self.writer.require_verified_registry_database(db)?;
        self.record.validate(&self.writer.record)?;
        require_closed_record_current(db, &self.writer, &self.record)?;
        self.require_lifetime()
    }
}
fn actual_service<'a>(
    expected: &Arc<Mutex<ClusterDataNodeService>>,
    held: &MutexGuard<'a, ClusterDataNodeService>,
) -> Result<()> {
    if !std::ptr::eq(parking_lot::MutexGuard::mutex(held), expected.as_ref()) {
        return Err(invalid(
            "writer DATA transition changed actual borrowed service mutex",
        ));
    }
    Ok(())
}
impl NativeWriterDataDomainTransition {
    /// Capture the original real owners before any fallible audit publication.
    /// The caller retains this attempt BEFORE calling advance_closed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin<T: RangeWriteTransport + 'static>(
        database: &BicDb,
        expected: &Arc<Mutex<ClusterDataNodeService>>,
        service: &MutexGuard<'_, ClusterDataNodeService>,
        runtime: &Arc<RangeRecoveryRuntime>,
        owner: NativeDataOwnerAdmission,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        guard: &InstalledNativeWriterHostGuard,
        range: RangeId,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        actual_service(expected, service)?;
        owner.require_listener(expected, runtime)?;
        coordinator.require_registry_refresh_service(expected)?;
        service.require_native_catalog_database(database)?;
        let writer = service
            .with_writer_data_domain_store(database, |store| store.native_writer_host_state())?;
        guard.require_data_domain_origin(database, coordinator, &writer)?;
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(range, lease.try_frozen()?)?;
        let slot = service.native_replica_listener_store_open_guard();
        slot.require_live()?;
        writer.require_store(database.data_path(), slot.id())?;
        let actual: Arc<dyn CommitAdmission> = coordinator.clone();
        Ok(Self {
            writer,
            coordinator: Arc::downgrade(&actual),
            service: Arc::downgrade(expected),
            runtime: Arc::downgrade(runtime),
            metadata: None,
            owner,
            slot,
            open: service.listener_open_guard(),
            caller: parent.clone(),
            range,
            lease: Some(lease),
            publication: ClosedPublication::Unwritten,
        })
    }
    fn require_owner<T: RangeWriteTransport + 'static>(
        &self,
        db: &BicDb,
        service: &MutexGuard<'_, ClusterDataNodeService>,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
    ) -> Result<()> {
        self.caller.check()?;
        self.open.require_database(db)?;
        self.slot.require_live()?;
        let actual = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("writer DATA actual service ended"))?;
        actual_service(&actual, service)?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("writer DATA original runtime ended"))?;
        self.owner.require_listener(&actual, &runtime)?;
        coordinator.require_registry_refresh_service(&actual)?;
        let expected: Arc<dyn CommitAdmission> = coordinator.clone();
        if self
            .coordinator
            .upgrade()
            .is_none_or(|old| !Arc::ptr_eq(&old, &expected))
            || !Arc::ptr_eq(
                &self.writer,
                &service
                    .with_writer_data_domain_store(db, |store| store.native_writer_host_state())?,
            )
            || self.slot.id() != service.native_initial_listener_store_open_id()
        {
            return Err(invalid(
                "writer DATA transition replaced original writer/coordinator/Store",
            ));
        }
        self.writer.require_original_identity_database(db)?;
        self.writer.require_verified_registry_database(db)?;
        self.caller.check()
    }
    pub(crate) fn verified(&self) -> Option<&Arc<NativeClosedWriterDomain>> {
        match &self.publication {
            ClosedPublication::Verified(v) => Some(v),
            _ => None,
        }
    }
    pub(crate) fn publication_unresolved(&self) -> bool {
        matches!(&self.publication, ClosedPublication::Uncertain(_))
    }
    /// Fresh protected MStore checks precede/follow the engine scope. The
    /// caller retains this attempt before any possibly durable publication.
    pub(crate) fn advance_closed<T: RangeWriteTransport + 'static>(
        &mut self,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        policy.require_owned_metadata_current()?;
        let actual = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("writer DATA actual service ended"))?;
        let result = {
            let service = actual
                .try_lock()
                .ok_or_else(|| invalid("writer DATA service busy at publication"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("writer DATA database busy at publication"))?;
            policy.require_local(&db, &actual, self.slot.id())?;
            self.advance_closed_local(&db, &service, coordinator, policy)
        };
        let post = policy.require_owned_metadata_current();
        result?;
        post?;
        if let ClosedPublication::NativeVerified(actual) = &self.publication {
            actual.require_lifetime()?;
            self.publication = ClosedPublication::Verified(actual.clone());
        }
        Ok(())
    }
    /// No MStore/network operation under engine/journal/admission guards.
    fn advance_closed_local<T: RangeWriteTransport + 'static>(
        &mut self,
        db: &BicDb,
        service: &MutexGuard<'_, ClusterDataNodeService>,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        service.with_writer_data_domain_store(db, |store| {
            self.advance_closed_with_store(db, service, coordinator, policy, store)
        })
    }
    fn advance_closed_with_store<T: RangeWriteTransport + 'static>(
        &mut self,
        db: &BicDb,
        service: &MutexGuard<'_, ClusterDataNodeService>,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        policy: &NativeWriterDataDomainPolicy,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_owner(db, service, coordinator)?;
        let actual_service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("writer DATA actual service ended"))?;
        policy.require_local(db, &actual_service, self.slot.id())?;
        let metadata = policy.metadata_owner();
        match &self.metadata {
            Some(original) if !Weak::ptr_eq(original, &metadata) => {
                return Err(invalid(
                    "writer DATA transition changed original actual metadata instance",
                ))
            }
            None => self.metadata = Some(metadata),
            _ => {}
        }
        policy.require_live()?;
        if policy.database_incarnation() != db.incarnation_id()
            || policy.store_open_id() != self.slot.id()
            || policy.facts().local_birth_id != self.writer.record.facts.local_birth_id
            || policy.facts().local_birth_sha256 != self.writer.record.facts.local_birth_sha256
            || policy.facts().schema_sha256 != self.writer.record.facts.schema_sha256
            || policy.facts().restore_activation != self.writer.record.facts.restore_activation
        {
            return Err(invalid(
                "writer DATA OWN proof changed original physical policy domain",
            ));
        }
        if let ClosedPublication::NativeVerified(v) | ClosedPublication::Verified(v) =
            &self.publication
        {
            policy.facts().require_range(v.range())?;
            policy.joint_domain_audit(v.data())?;
            if policy.metadata_term() < v.record.metadata_term
                || policy.metadata_read_index() < v.record.metadata_index
            {
                return Err(invalid(
                    "writer DATA fresh reconciliation regressed behind actual native audit",
                ));
            }
            return v.require_current(db);
        }
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| invalid("writer DATA original image/OLD custody transferred"))?;
        let facts = &self.writer.record.facts;
        let native = db.native_recovery_admission(
            &facts.cluster_id,
            facts.bootstrap_id,
            &facts.local_node_id,
            facts.local_incarnation,
        )?;
        let selected = db.read_current_native_joint_writer_decision(&native, self.range)?;
        policy
            .facts()
            .require_range(selected.data().activation_range())?;
        let audit = policy.joint_domain_audit(selected.data())?;
        let actual = db.observe_closed_writer_joint_fact(
            &native,
            selected.data(),
            store,
            lease.image(),
            lease.try_frozen()?,
        )?;
        let _admission = db
            .serializable_commit_admission
            .try_write()
            .ok_or_else(|| invalid("writer DATA admission busy"))?;
        self.require_owner(db, service, coordinator)?;
        let _journal = db.try_native_image_journal()?;
        actual.require_current(db, store, lease.image(), lease.try_frozen()?)?;
        if matches!(&self.publication, ClosedPublication::Unwritten) {
            let sequence = next_sequence(db, &self.writer.record)?;
            let previous = load_serving(db, &self.writer, self.range)?;
            self.publication = ClosedPublication::Prepared(ClosedDomainRecord {
                version: 1,
                sequence,
                original: OriginalWriterBinding::from_actual(&self.writer.record),
                previous_serving_sha256: previous.map(|p| p.checksum_sha256),
                decision: selected.data().clone(),
                local: actual.point().clone(),
                metadata_audit: audit,
                metadata_context: policy.metadata_context_id(),
                metadata_store_id: policy.metadata_store_id(),
                metadata_term: policy.metadata_term(),
                metadata_index: policy.metadata_read_index(),
                native_page_xid: 0,
                checksum_sha256: String::new(),
            });
        }
        let record = match &self.publication {
            ClosedPublication::Prepared(r) | ClosedPublication::Uncertain(r) => r.clone(),
            _ => return Err(invalid("writer DATA progress changed")),
        };
        if record.decision != *selected.data()
            || record.local != *actual.point()
            || policy.metadata_term() < record.metadata_term
            || policy.metadata_read_index() < record.metadata_index
        {
            return Err(invalid(
                "fresh writer DATA reconciliation changed exact attempted fact",
            ));
        }
        crate::format::enable_native_writer_data_domain(db.data_path())?;
        policy.require_live()?;
        self.caller.check()?;
        self.writer.data_domains.write().pending.insert(self.range);
        if matches!(&self.publication, ClosedPublication::Prepared(_)) {
            let mut record = record;
            // No native put has happened in Prepared. Another RANGE may have
            // published while this prewrite attempt was retained; allocate
            // accounting from the actual counter under this SAME journal.
            // NativeUncertain never changes its original sequence/audit.
            record.sequence = next_sequence(db, &self.writer.record)?;
            // Hook guard is acquired AFTER full native preflight. No nested
            // full hook acquisition occurs until this actual write guard drops.
            let hook = db
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("writer DATA hook busy"))?;
            self.writer.require_borrowed_original_hook(db, &hook)?;
            db.in_paged_transaction(|paged, xid| {
                policy.require_live()?;
                policy.require_local(db, &actual_service, self.slot.id())?;
                self.caller.check()?;
                self.slot.require_live()?;
                self.writer.require_borrowed_original_hook(db, &hook)?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate(&self.writer.record)?;
                self.publication = ClosedPublication::Uncertain(record.clone());
                let mut counter = AuditCounter {
                    version: 1,
                    original: record.original.clone(),
                    sequence: record.sequence,
                    checksum_sha256: String::new(),
                };
                counter.checksum_sha256 = counter.checksum()?;
                for (key, bytes) in [
                    (
                        counter_key(self.writer.record.generation),
                        bounded_json(&counter, 16 * 1024)?,
                    ),
                    (
                        archive(self.writer.record.generation, record.sequence),
                        bounded_json(&record, MAX_AUDIT_BYTES)?,
                    ),
                    (
                        closed_current(self.writer.record.generation, self.range),
                        bounded_json(&record, MAX_AUDIT_BYTES)?,
                    ),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                policy.require_local(db, &actual_service, self.slot.id())?;
                Ok(())
            })?
            .ok_or_else(|| invalid("writer DATA has no actual native page store"))?;
            drop(hook);
        }
        let record = match &self.publication {
            ClosedPublication::Uncertain(r) => r.clone(),
            _ => return Err(invalid("writer DATA lost attempted native receipt")),
        };
        record.validate(&self.writer.record)?;
        require_closed_record_current(db, &self.writer, &record)?;
        actual.require_current(db, store, lease.image(), lease.try_frozen()?)?;
        self.require_owner(db, service, coordinator)?;
        policy.require_live()?;
        // Actual same-open verified CLOSED fact only, never ordinary admission.
        self.publication = ClosedPublication::NativeVerified(Arc::new(NativeClosedWriterDomain {
            writer: self.writer.clone(),
            record,
            owner: self.owner.clone(),
            service: self.service.clone(),
            runtime: self.runtime.clone(),
            metadata: self.metadata.as_ref().unwrap().clone(),
            caller: self.caller.clone(),
            slot: self.slot.clone(),
            open: self.open.clone(),
        }));
        Ok(())
    }
    pub(crate) fn take_joint_lease(&mut self, db: &BicDb) -> Result<NativeJointServingLease> {
        self.verified()
            .ok_or_else(|| invalid("writer DATA CLOSED audit not verified"))?
            .require_current(db)?;
        self.caller.check()?;
        self.lease
            .take()
            .ok_or_else(|| invalid("writer DATA real lease already transferred"))
    }
}
