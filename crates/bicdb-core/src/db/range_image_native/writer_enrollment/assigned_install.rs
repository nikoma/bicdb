//! Retained first-policy publication for an actual formerly Registered image
//! target. Generic policy installation is unchanged. Unknown never restores
//! NoWriter or discards the actual attempted receipt/registration.
use super::*;
use crate::distribution_native_host::NativeAssignedDataPolicyOrigin;

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
    coordinator: Weak<dyn CommitAdmission>,
    coordinator_id: Uuid,
    database_incarnation: String,
    original_caller: CancellationToken,
    publication: Publication,
}
impl NativeAssignedWriterInstallAttempt {
    pub(crate) fn begin<T: RangeWriteTransport + 'static>(
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        origin: &NativeAssignedDataPolicyOrigin,
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
            coordinator: Arc::downgrade(&authority),
            coordinator_id: coordinator.native_writer_coordinator_id(),
            database_incarnation: database.incarnation_id().into(),
            original_caller: origin.original_caller().clone(),
            publication: Publication::AwaitingNative,
        })
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
        origin: &NativeAssignedDataPolicyOrigin,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<Arc<dyn CommitAdmission>> {
        self.original_caller.check()?;
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
                    self.original_caller.check()?;
                    origin.require_live()?;
                    policy.require_live()?;
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
        origin.with_publication_service(database, policy, |actual_service, service| {
            origin.require_original_empty_hook(database)?;
            let mut hook = database
                .commit_admission
                .try_write()
                .ok_or_else(|| invalid("assigned actual hook busy before publication"))?;
            coordinator.install_assigned_native_writer_host_state(
                database,
                actual_service,
                service,
                policy,
                Arc::downgrade(&state),
            )?;
            self.require_request(database, coordinator, origin, policy)?;
            hook.install_enrolled(authority, registration, state.clone());
            // Retain BEFORE the scope's fallible postchecks. Service mutex
            // excludes Slot replacement through this exact hook publication.
            self.publication = Publication::Installed {
                guard: Arc::new(InstalledNativeWriterHostGuard {
                    state: state.clone(),
                }),
            };
            Ok(())
        })?;
        drop(_journal);
        origin.require_current(database, policy)?;
        self.original_caller.check()?;
        self.installed_guard().unwrap().require_for_coordinator(
            database,
            origin.range(),
            coordinator,
        )
    }
}
