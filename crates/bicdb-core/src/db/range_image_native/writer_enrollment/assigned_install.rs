//! Retained first-policy publication for an actual formerly Registered image
//! target. Generic policy installation is unchanged. Unknown never restores
//! NoWriter or discards the actual attempted receipt/registration.
use super::*;
use crate::distribution_native_host::{
    NativeAssignedDataPolicyOrigin, NativeRegisteredServingDataPolicyOrigin,
};
/// Sealed actual native-readback borrow, minted only by this retained
/// installer while its real DB/journal/hook/admission scope remains held.
/// No path/hash/Weak<State>/policy DTO can construct this candidate.
pub(crate) struct NativeFirstPolicyPublicationCandidate<'a> {
    database: &'a BicDb,
    journal: &'a NativeImageJournalGuard<'a>,
    state: &'a Arc<NativeWriterHostState>,
    registration: &'a Arc<CommitAdmissionRegistration>,
}
impl NativeFirstPolicyPublicationCandidate<'_> {
    pub(crate) fn borrow_historical_cut<'a>(
        &'a self,
        native: &'a crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<crate::db::NativeJointHistoricalCutScope<'a>> {
        native.historical_cut_scope_under_journal(self.database, self.journal)
    }
    pub(crate) fn state(&self) -> &Arc<NativeWriterHostState> {
        self.state
    }
    pub(crate) fn require_actual(
        &self,
        db: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        if !std::ptr::eq(db, self.database)
            || self.state.record.facts != *policy.facts()
            || self.state.record.store_open_id != policy.store_open_id()
            || self.state.record.database_incarnation != db.incarnation_id()
            || self.registration.generation() != self.state.record.generation
        {
            return Err(invalid(
                "FIRST publication candidate changed actual record/registration/open",
            ));
        }
        self.journal.require_database(db)?;
        let actual: NativeWriterPolicyRecord =
            db.read_native_range_image_record(POLICY_POINTER)?
                .ok_or_else(|| invalid("FIRST actual native candidate disappeared"))?;
        actual.validate()?;
        if actual != self.state.record {
            return Err(invalid(
                "FIRST actual native candidate differs from exact retained readback",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum FirstPolicyOrigin<'a> {
    Legacy(&'a NativeAssignedDataPolicyOrigin),
    Registered(&'a NativeRegisteredServingDataPolicyOrigin),
}
enum FirstPolicyOriginIdentity {
    Legacy,
    Registered(std::sync::Weak<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>),
}
impl FirstPolicyOrigin<'_> {
    fn identity(&self) -> FirstPolicyOriginIdentity {
        match self {
            Self::Legacy(_) => FirstPolicyOriginIdentity::Legacy,
            Self::Registered(o) => {
                FirstPolicyOriginIdentity::Registered(Arc::downgrade(o.original_joint_owner()))
            }
        }
    }
    fn require_identity(&self, expected: &FirstPolicyOriginIdentity) -> Result<()> {
        match (self, expected) {
            (Self::Legacy(_), FirstPolicyOriginIdentity::Legacy) => Ok(()),
            (Self::Registered(o), FirstPolicyOriginIdentity::Registered(old))
                if std::sync::Weak::ptr_eq(old, &Arc::downgrade(o.original_joint_owner())) =>
            {
                Ok(())
            }
            _ => Err(invalid(
                "first policy retry changed its actual original provenance owner",
            )),
        }
    }
    fn range(&self) -> &RangeDescriptor {
        match self {
            Self::Legacy(o) => o.range(),
            Self::Registered(o) => o.range(),
        }
    }
    fn require_live(&self) -> Result<()> {
        match self {
            Self::Legacy(o) => o.require_live(),
            Self::Registered(o) => o.require_live(),
        }
    }
    fn require_original_empty_hook(&self, db: &BicDb) -> Result<()> {
        match self {
            Self::Legacy(o) => o.require_original_empty_hook(db),
            Self::Registered(o) => o.require_original_empty_hook(db),
        }
    }
    fn require_current(&self, db: &BicDb, policy: &SealedHostWriterEnrollment) -> Result<()> {
        match self {
            Self::Legacy(o) => o.require_current(db, policy),
            Self::Registered(o) => o.require_current(db, policy),
        }
    }
    fn with_publication_service<R>(
        &self,
        db: &BicDb,
        policy: &SealedHostWriterEnrollment,
        candidate: &NativeFirstPolicyPublicationCandidate<'_>,
        operation: impl for<'scope> FnOnce(
            &'scope Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
            &'scope mut crate::distribution_data::ClusterDataNodeService,
        ) -> Result<R>,
    ) -> Result<R> {
        match self {
            Self::Legacy(o) => o.with_publication_service(db, policy, operation),
            Self::Registered(o) => o.with_publication_service(db, policy, candidate, operation),
        }
    }
    fn require_native_metadata_point(&self, db: &BicDb) -> Result<()> {
        match self {
            Self::Legacy(_) => Ok(()),
            Self::Registered(o) => o.require_native_metadata_point(db),
        }
    }
    fn publication_version_guard(
        &self,
    ) -> Result<Option<crate::distribution_consensus::NativeMetadataPublicationGuard<'_>>> {
        match self {
            Self::Legacy(_) => Ok(None),
            Self::Registered(o) => o.publication_version_guard().map(Some),
        }
    }
    fn actual_released_publication_point(
        &self,
    ) -> Result<Option<(Arc<crate::db::NativeJointActivatedOpenGuard>, Uuid, String)>> {
        match self {
            Self::Legacy(_) => Ok(None),
            Self::Registered(origin) => origin.actual_released_publication_point().map(Some),
        }
    }
    fn retain_installed_writer(&self, state: Arc<NativeWriterHostState>) -> Result<()> {
        match self {
            Self::Legacy(_) => Ok(()),
            Self::Registered(o) => o.retain_installed_writer(state),
        }
    }
}

enum Publication {
    AwaitingNative,
    Prepared {
        record: NativeWriterPolicyRecord,
        registration: Arc<CommitAdmissionRegistration>,
    },
    NativeUncertain {
        record: NativeWriterPolicyRecord,
        registration: Arc<CommitAdmissionRegistration>,
    },
    NativeReadBack {
        record: NativeWriterPolicyRecord,
        registration: Arc<CommitAdmissionRegistration>,
        state: Arc<NativeWriterHostState>,
    },
    Installed {
        guard: Arc<InstalledNativeWriterHostGuard>,
    },
}
pub(crate) struct NativeAssignedWriterInstallAttempt {
    origin_identity: FirstPolicyOriginIdentity,
    coordinator: Weak<dyn CommitAdmission>,
    coordinator_id: Uuid,
    database_incarnation: String,
    // Legacy keeps its original caller contract. Registered lifetime belongs
    // to the exact retained receiver, never the first finite policy RPC.
    original_caller: Option<CancellationToken>,
    publication: Publication,
}
impl NativeAssignedWriterInstallAttempt {
    pub(crate) fn begin<T: RangeWriteTransport + 'static>(
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: &NativeAssignedDataPolicyOrigin,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<Self> {
        Self::begin_from_origin(
            database,
            coordinator,
            FirstPolicyOrigin::Legacy(origin),
            policy,
        )
    }
    pub(crate) fn begin_registered<T: RangeWriteTransport + 'static>(
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: &NativeRegisteredServingDataPolicyOrigin,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<Self> {
        Self::begin_from_origin(
            database,
            coordinator,
            FirstPolicyOrigin::Registered(origin),
            policy,
        )
    }
    fn begin_from_origin<T: RangeWriteTransport + 'static>(
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: FirstPolicyOrigin<'_>,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<Self> {
        origin.require_current(database, policy)?;
        origin.require_original_empty_hook(database)?;
        coordinator.validate_native_writer_enrollment(database, policy)?;
        // A new operation cannot adopt an old unknown policy from JSON. Only
        // this already-retained attempt may reconcile its exact own audit.
        database.require_never_installed_receiver_writer_records()?;
        let authority: Arc<dyn CommitAdmission> = coordinator.clone();
        Ok(Self {
            origin_identity: origin.identity(),
            coordinator: Arc::downgrade(&authority),
            coordinator_id: coordinator.native_writer_coordinator_id(),
            database_incarnation: database.incarnation_id().into(),
            original_caller: match origin {
                FirstPolicyOrigin::Legacy(old) => Some(old.original_caller().clone()),
                FirstPolicyOrigin::Registered(_) => None,
            },
            publication: Publication::AwaitingNative,
        })
    }
    fn require_original_attempt_lifetime(&self) -> Result<()> {
        match (&self.origin_identity, &self.original_caller) {
            (FirstPolicyOriginIdentity::Legacy, Some(caller)) => caller.check(),
            (FirstPolicyOriginIdentity::Registered(owner), None) => owner
                .upgrade()
                .ok_or_else(|| invalid("FIRST Registered original retained owner ended"))?
                .require_first_policy_attempt_lifetime(),
            _ => Err(invalid("FIRST policy lifetime changed original provenance")),
        }
    }
    pub(crate) fn installed_guard(&self) -> Option<&Arc<InstalledNativeWriterHostGuard>> {
        match &self.publication {
            Publication::Installed { guard } => Some(guard),
            _ => None,
        }
    }
    pub(crate) fn unresolved_native_publication(&self) -> bool {
        matches!(
            &self.publication,
            Publication::NativeUncertain { .. } | Publication::NativeReadBack { .. }
        )
    }
    fn require_request<T: RangeWriteTransport + 'static>(
        &self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: FirstPolicyOrigin<'_>,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<Arc<dyn CommitAdmission>> {
        self.require_original_attempt_lifetime()?;
        origin.require_identity(&self.origin_identity)?;
        origin.require_live()?;
        policy.require_live()?;
        let actual: Arc<dyn CommitAdmission> = coordinator.clone();
        if self.coordinator_id != coordinator.native_writer_coordinator_id()
            || self.database_incarnation != database.incarnation_id()
            || self
                .coordinator
                .upgrade()
                .is_none_or(|old| !Arc::ptr_eq(&old, &actual))
        {
            return Err(invalid(
                "assigned install changed its original actual coordinator/DB",
            ));
        }
        Ok(actual)
    }
    /// Called outside service/MStore/journal locks. The host owns this attempt
    /// AND its coordinator before calling; post-fsync failure cannot lose them.
    pub(crate) fn advance<T: RangeWriteTransport + 'static>(
        &mut self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: &NativeAssignedDataPolicyOrigin,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.advance_from_origin(
            database,
            coordinator,
            FirstPolicyOrigin::Legacy(origin),
            policy,
        )
    }
    pub(crate) fn advance_registered<T: RangeWriteTransport + 'static>(
        &mut self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: &NativeRegisteredServingDataPolicyOrigin,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.advance_from_origin(
            database,
            coordinator,
            FirstPolicyOrigin::Registered(origin),
            policy,
        )
    }
    fn advance_from_origin<T: RangeWriteTransport + 'static>(
        &mut self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: FirstPolicyOrigin<'_>,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        if self.installed_guard().is_some() {
            return Err(invalid(
                "assigned policy already installed; use retained current guard",
            ));
        }
        let authority = self.require_request(database, coordinator, origin, policy)?;
        let _admission = database
            .serializable_commit_admission
            .try_write()
            .ok_or_else(|| invalid("assigned policy admission busy"))?;
        origin.require_current(database, policy)?;
        origin.require_original_empty_hook(database)?;
        coordinator.validate_native_writer_enrollment(database, policy)?;
        if matches!(&self.publication, Publication::AwaitingNative) {
            let registration = CommitAdmissionRegistration::new(Some(&authority));
            let record = NativeWriterPolicyRecord {
                version: 1,
                generation: registration.generation(),
                coordinator_id: self.coordinator_id,
                database_incarnation: self.database_incarnation.clone(),
                store_open_id: policy.store_open_id(),
                facts: policy.facts().clone(),
                metadata_context_id: policy.metadata_context_id(),
                metadata_term: policy.metadata_term(),
                metadata_read_index: policy.metadata_read_index(),
                native_page_xid: 0,
                checksum_sha256: String::new(),
            };
            self.publication = Publication::Prepared {
                record,
                registration,
            };
        }
        if let Publication::Prepared {
            record,
            registration,
        } = &self.publication
        {
            let (mut record, registration) = (record.clone(), registration.clone());
            if &record.facts != policy.facts()
                || policy.metadata_term() < record.metadata_term
                || policy.metadata_read_index() < record.metadata_read_index
            {
                return Err(invalid(
                    "assigned prewrite retry changed original facts/audit",
                ));
            }
            database.require_never_installed_receiver_writer_records()?;
            let _journal = database.try_native_image_journal()?;
            database
                .in_paged_transaction(|paged, xid| {
                    self.require_original_attempt_lifetime()?;
                    origin.require_live()?;
                    policy.require_live()?;
                    origin.require_native_metadata_point(database)?;
                    record.native_page_xid = xid;
                    record.checksum_sha256 = record.checksum()?;
                    record.validate()?;
                    // Actual record/xid/registration custody BEFORE the first
                    // policy put. Earlier cancellation stays Prepared; any
                    // put/fsync ambiguity never returns to Prepared/NoWriter.
                    self.publication = Publication::NativeUncertain {
                        record: record.clone(),
                        registration: registration.clone(),
                    };
                    paged
                        .store()
                        .put(
                            xid,
                            &native_key(POLICY_POINTER),
                            &bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?,
                        )
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                    Ok(())
                })?
                .ok_or_else(|| invalid("assigned policy has no native page store"))?;
        }
        self.require_request(database, coordinator, origin, policy)?;
        // Independently read the attempted actual audit. An absent/foreign
        // receipt is not a zero/abort result and cannot mint a fresh attempt.
        {
            let _journal = database.try_native_image_journal()?;
            let (record, registration) = match &self.publication {
                Publication::NativeUncertain {
                    record,
                    registration,
                } => (record, registration),
                Publication::NativeReadBack {
                    record,
                    registration,
                    ..
                } => (record, registration),
                _ => return Err(invalid("assigned install lost retained native progress")),
            };
            record.validate()?;
            if record.facts != *policy.facts()
                || record.store_open_id != policy.store_open_id()
                || policy.metadata_term() < record.metadata_term
                || policy.metadata_read_index() < record.metadata_read_index
            {
                return Err(invalid(
                    "fresh assigned policy differs from original attempted audit",
                ));
            }
            let actual: NativeWriterPolicyRecord = database
                .read_native_range_image_record(POLICY_POINTER)?
                .ok_or_else(|| {
                    invalid("assigned policy outcome unresolved; exact attempted receipt absent")
                })?;
            actual.validate()?;
            if actual != *record || registration.generation() != record.generation {
                return Err(invalid(
                    "assigned policy current differs from exact retained attempted receipt",
                ));
            }
            if matches!(&self.publication, Publication::NativeUncertain { .. }) {
                let state = Arc::new(NativeWriterHostState {
                    record: record.clone(),
                    root: database.path.canonicalize()?,
                    directory: database
                        .native_service_directory_lifetime()
                        .ok_or_else(|| invalid("assigned policy lost actual directory lifetime"))?,
                    health: Arc::downgrade(&database.durability),
                    schema: database.schema_compatibility_authority(),
                    schema_generation: database.schema_compatibility.generation(),
                    registration: Arc::downgrade(registration),
                    coordinator: Arc::downgrade(&authority),
                    registry_extension: parking_lot::RwLock::new(None),
                    registry_publication_pending: std::sync::atomic::AtomicBool::new(false),
                    data_domains: parking_lot::RwLock::new(Default::default()),
                });
                self.publication = Publication::NativeReadBack {
                    record: record.clone(),
                    registration: registration.clone(),
                    state,
                };
            }
        }
        // No native journal spans full image/decision validation. Its actual
        // read pin and admission exclusion still cover this installation.
        origin.require_current(database, policy)?;
        self.require_request(database, coordinator, origin, policy)?;
        let _journal = database.try_native_image_journal()?;
        let (record, registration, state) = match &self.publication {
            Publication::NativeReadBack {
                record,
                registration,
                state,
            } => (record.clone(), registration.clone(), state.clone()),
            _ => return Err(invalid("assigned install has no exact native readback")),
        };
        let actual: NativeWriterPolicyRecord = database
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| invalid("assigned native policy disappeared before hook publication"))?;
        actual.validate()?;
        if actual != record
            || state.schema.generation() != state.schema_generation
            || state.schema.verified_sha256().as_deref()
                != Some(record.facts.schema_sha256.as_str())
        {
            return Err(invalid(
                "assigned actual policy/schema changed before hook publication",
            ));
        }
        origin.require_original_empty_hook(database)?;
        let candidate = NativeFirstPolicyPublicationCandidate {
            database,
            journal: &_journal,
            state: &state,
            registration: &registration,
        };
        let released = origin.actual_released_publication_point()?;
        origin.with_publication_service(
            database,
            policy,
            &candidate,
            |actual_service, service| {
                origin.require_original_empty_hook(database)?;
                let mut hook = database
                    .commit_admission
                    .try_write()
                    .ok_or_else(|| invalid("assigned actual hook busy before publication"))?;
                // All I/O, native identity reads and full admission validation
                // precede the short actual-version gate. The returned concrete
                // publication borrow retains the actual service Store/cache and
                // held routing topology; the surrounding journal/admission/hook
                // and original release scope remain lexical through publication.
                self.require_request(database, coordinator, origin, policy)?;
                origin.require_native_metadata_point(database)?;
                let range_publication = released
                    .as_ref()
                    .map(|(guard, store, sha)| guard.borrow_arm_publication(*store, sha))
                    .transpose()?;
                let mut publication = coordinator.prepare_assigned_native_writer_publication(
                    database,
                    actual_service,
                    service,
                    policy,
                    &candidate,
                )?;
                let retired;
                {
                    let version = origin.publication_version_guard()?;
                    if let Some(version) = &version {
                        version.require_current_memory()?;
                    }
                    // Concrete memory publication only: no callback/full-live/native
                    // read/fsync under this guard. Hook and both caches agree.
                    let old_caches = publication.publish();
                    let old_hook = hook.replace_enrolled_retaining_old(
                        authority,
                        registration.clone(),
                        state.clone(),
                    );
                    let old_publication = std::mem::replace(
                        &mut self.publication,
                        Publication::Installed {
                            guard: Arc::new(InstalledNativeWriterHostGuard {
                                state: state.clone(),
                            }),
                        },
                    );
                    retired = (old_caches, old_hook, old_publication);
                }
                // Retired owners and lexical cache/topology guards leave only
                // AFTER the short version guard; no destructor/callback inside it.
                drop(retired);
                drop(publication);
                drop(range_publication);
                // Exact Installed custody is already retained on every error.
                origin.retain_installed_writer(state.clone())
            },
        )?;
        drop(_journal);
        origin.require_current(database, policy)?;
        self.require_original_attempt_lifetime()?;
        self.installed_guard().unwrap().require_for_coordinator(
            database,
            origin.range(),
            coordinator,
        )
    }
}
