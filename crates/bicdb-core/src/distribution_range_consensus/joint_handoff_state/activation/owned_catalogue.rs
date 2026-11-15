//! Original full-head activation owner. This comparison path NEVER changes
//! generic catalogue health or grants Serving/APP/Strong permission.
use super::*;
use crate::db::{NativeJointPhaseOwner, NativeJointServingBridgeReceipt, NativeJointWriterDomain};
use crate::distribution_range_mutation::FrozenRangeMutationIdentity;
const MAX_RETAINED: usize = 8 * 1024 * 1024;
const MAX_ATTEMPTS: usize = 8;
#[derive(Debug, Default)]
pub(in crate::distribution_range_consensus) struct ActivationIoHistory {
    chain: [u8; 32],
    events: u64,
    count_overflow: bool,
}
impl ActivationIoHistory {
    fn record(&mut self, native: &str, index: usize, kind: &str, error: Option<&str>) {
        let bytes = serde_json::to_vec(&(native, index, kind, error))
            .expect("bounded scalar activation history");
        let mut hash = sha2::Sha256::new();
        hash.update(self.chain);
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        self.chain = hash.finalize().into();
        match self.events.checked_add(1) {
            Some(n) => self.events = n,
            None => self.count_overflow = true,
        }
    }
}
#[derive(Debug)]
enum IoReturn {
    Entered,
    ReturnedOk,
    ReturnedErr { sha256: String },
}
pub(in crate::distribution_range_consensus) struct NativeJointActivationPublication {
    original: RangeWriteCatalog,
    candidate: RangeWriteCatalog,
    bytes: Vec<u8>,
    root: PathBuf,
    store: Uuid,
    database_open: String,
    catalogue: Uuid,
    native: Arc<NativeJointServingBridgeReceipt>,
    phase_owner: NativeJointPhaseOwner,
    frozen: FrozenRangeMutationIdentity,
    session: crate::cancellation::CancellationLifetime,
    outcomes: Vec<IoReturn>,
    // Produced only by THIS actual memory publication/entered write. Neither
    // equal bytes nor global uncertainty can acquire this pending ownership.
    owns_uncertainty: bool,
    write_generation: u64,
    verified: bool,
}
impl std::fmt::Debug for NativeJointActivationPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeJointActivationPublication")
            .field("range", &self.native.data().old_range().id)
            .field("store", &self.store)
            .field("attempts", &self.outcomes.len())
            .field("verified", &self.verified)
            .finish_non_exhaustive()
    }
}
/// Necessary comparison of the SAME original/candidate head and actual owner.
/// Fields/constructor stay private. This is not catalogue durability evidence.
pub(crate) struct NativeJointActivationComparison<'a> {
    actual: &'a NativeJointActivationPublication,
    store: &'a RangeWriteStore,
    frozen: &'a FrozenRangeMutationPermit,
    controlled: Option<&'a crate::distribution_range_mutation::ControlledFrozenMutationPermit>,
}
impl NativeJointActivationComparison<'_> {
    fn require_original_gate(&self) -> Result<()> {
        match self.controlled {
            Some(controlled) => self
                .actual
                .frozen
                .require_same_controlled(self.frozen, controlled),
            None => self.actual.frozen.require_same_current(self.frozen),
        }
    }
    pub(crate) fn require_store(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        domain: &NativeJointWriterDomain,
    ) -> Result<()> {
        if !std::ptr::eq(self.store, store) {
            return Err(range_write_error(
                "activation comparison substituted actual Store",
            ));
        }
        self.actual
            .require_binding_identity(db, store, &self.actual.native, domain)?;
        self.require_original_gate()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_registered_target(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        target: &crate::distribution_native_host::NativeRegisteredRemoteJointTarget,
    ) -> Result<()> {
        if !std::ptr::eq(self.store, store) {
            return Err(range_write_error(
                "activation comparison substituted actual Store",
            ));
        }
        let NativeJointPhaseOwner::Registered(original) = &self.actual.phase_owner else {
            return Err(range_write_error(
                "activation comparison has no original Registered owner",
            ));
        };
        let original = original.upgrade().ok_or_else(|| {
            range_write_error("activation comparison original Registered owner ended")
        })?;
        if !std::ptr::eq(original.target().as_ref(), target)
            || db.incarnation_id() != self.actual.database_open
        {
            return Err(range_write_error(
                "activation comparison substituted original Registered target/open",
            ));
        }
        self.actual.require_binding_identity(
            db,
            store,
            &self.actual.native,
            &NativeJointWriterDomain::Registered(Arc::downgrade(&original)),
        )?;
        self.require_original_gate()
    }
}
enum FreshAuthority<'a> {
    Open(&'a NativeJointOpenAdmission),
    Host {
        host: &'a crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        point: Option<&'a crate::distribution_data::ActualNativeJointMetadataPoint>,
    },
}
impl FreshAuthority<'_> {
    fn require_current(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        native: &NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        match self {
            Self::Open(a) => {
                a.require_live()?;
                if a.native_receipt().checksum_sha256() != native.checksum_sha256() {
                    return Err(range_write_error(
                        "owned activation fresh Open changed native bridge",
                    ));
                }
                a.committed().require_native_publication_metadata(db, store)
            }
            Self::Host {
                host,
                business,
                point,
            } => {
                host.require_live_request(*business)?;
                native.data().require_metadata_domain(
                    host.committed_topology(*business)?,
                    host.restore_activation(*business)?,
                )?;
                let (term, index, _) = native.metadata_anchor();
                if host.requester_node_id() != &store.catalog.node_id
                    || native
                        .data()
                        .new_members()
                        .get(&store.catalog.node_id)
                        .is_none_or(|m| m.incarnation != host.requester_incarnation())
                    || host.term() < term
                    || host.read_index() < index
                {
                    return Err(range_write_error(
                        "owned activation retry changed actual local HostMQ/domain",
                    ));
                }
                match point {
                    Some(p) => {
                        p.require_request(host, *business)?;
                        p.require_native_store_point(db, store)
                    }
                    None if native.native_metadata_initialization_version() == 0 => Ok(()),
                    None => Err(range_write_error(
                        "owned activation requires actual current MStore point",
                    )),
                }
            }
        }
    }
    fn publication_guard(
        &self,
        native: &NativeJointServingBridgeReceipt,
    ) -> Result<Option<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>>>
    {
        match self {
            Self::Open(a) => a.committed().native_publication_guard(),
            Self::Host { point: Some(p), .. } => p.try_publication_guard().map(Some),
            Self::Host { point: None, .. }
                if native.native_metadata_initialization_version() == 0 =>
            {
                Ok(None)
            }
            _ => Err(range_write_error(
                "owned activation missing native publication point",
            )),
        }
    }
    fn require_request_memory(&self) -> Result<()> {
        match self {
            Self::Open(a) => {
                a.cancellation().check()?;
                a.committed().require_publication_request_memory()
            }
            Self::Host { host, business, .. } => host.require_live_request(*business),
        }
    }
}
impl NativeJointActivationPublication {
    fn read_head(store: &RangeWriteStore) -> Result<RangeWriteCatalog> {
        let mut bytes = Vec::new();
        open_range_catalog_no_follow(&store.root.join(DEFAULT_RANGE_WRITE_LOG))?
            .take(MAX_RETAINED as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_RETAINED {
            return Err(range_write_error(
                "activation catalogue head exceeds bounded read",
            ));
        }
        serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()
    }
    fn require_disk(&self, store: &RangeWriteStore) -> Result<()> {
        let actual = Self::read_head(store)?;
        if actual != self.original && actual != self.candidate {
            return Err(range_write_error(
                "activation owned original/candidate disk head changed",
            ));
        }
        Ok(())
    }
    fn require_binding(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        native: &NativeJointServingBridgeReceipt,
        domain: &NativeJointWriterDomain,
    ) -> Result<()> {
        self.require_binding_identity(db, store, native, domain)?;
        native.require_current(db)
    }
    // Necessary identity only inside this same lexical comparison. The full
    // native CURRENT/archive proof remains in validate_domain before and after
    // IO; comparison recursion cannot turn this helper into a current grant.
    fn require_binding_identity(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        native: &NativeJointServingBridgeReceipt,
        domain: &NativeJointWriterDomain,
    ) -> Result<()> {
        self.session.check()?;
        if self.write_generation != store.current_catalogue_write_generation() {
            return Err(range_write_error(
                "activation original owner superseded by later catalogue IO",
            ));
        }
        if !store.activation_uses_owned_profile(db)?
            || !store.fsync
            || !crate::format::native_joint_data_handoff_active(db.data_path())?
            || store.open_id != self.store
            || store.root.canonicalize()? != self.root
            || db.incarnation_id() != self.database_open
            || db.data_path().canonicalize()? != self.root
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue)
            || native.checksum_sha256() != self.native.checksum_sha256()
            || !self.phase_owner.same_actual(&domain.phase_owner())
            || (store.catalog != self.original && store.catalog != self.candidate)
        {
            return Err(range_write_error(
                "activation retry lost same owner/Store/open/native/whole-head binding",
            ));
        }
        self.phase_owner.require_store(&store.root, store.open_id)
    }
    fn validate_domain(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        domain: &NativeJointWriterDomain,
        frozen: &FrozenRangeMutationPermit,
        controlled: Option<&crate::distribution_range_mutation::ControlledFrozenMutationPermit>,
    ) -> Result<()> {
        self.require_binding(db, store, &self.native, domain)?;
        self.require_disk(store)?;
        let comparison = NativeJointActivationComparison {
            actual: self,
            store,
            frozen,
            controlled,
        };
        comparison.require_original_gate()?;
        domain.require_owned_activation_comparison(
            db,
            store,
            &comparison,
            self.native.data().activation_range(),
        )
    }
    fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: Arc<NativeJointServingBridgeReceipt>,
        domain: &NativeJointWriterDomain,
        frozen: &FrozenRangeMutationPermit,
        image: Option<&dyn NativeJointImageMaterialization>,
        session: crate::cancellation::CancellationLifetime,
    ) -> Result<Self> {
        store.require_native_catalog_durability(db.data_path())?;
        store.require_native_joint_host_base(db, native.data().old_range().id)?;
        domain.require_current(db, store, native.data().activation_range())?;
        native.require_current(db)?;
        session.check()?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(native.data().old_range().id, frozen)?;
        let data = native.data();
        let range = data.old_range().id;
        let activation = NativeJointCatalogueActivation::from_actual(&native)?;
        let original_bytes =
            crate::distribution_range_image::bounded_json(&store.catalog, MAX_RETAINED)?;
        if Self::read_head(store)? != store.catalog {
            return Err(range_write_error(
                "activation original disk/memory head differs",
            ));
        }
        let mut candidate = store.catalog.clone();
        let state = candidate
            .native_joint_handoffs
            .as_mut()
            .and_then(|s| s.get_mut(&range))
            .ok_or_else(|| range_write_error("activation original protected decision absent"))?;
        if state.global_decision_sha256.as_deref() != Some(data.checksum_sha256())
            || state.session_id != data.promise().session_id
            || state.nonce != data.promise().nonce
            || state
                .serving_activation
                .as_ref()
                .is_some_and(|old| old != &activation)
        {
            return Err(range_write_error(
                "activation original decision/attempt differs",
            ));
        }
        let first = state.serving_activation.is_none();
        state.serving_activation = Some(activation);
        candidate
            .ranges
            .entry(range)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: data.activation_range().epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
                entries: BTreeMap::new(),
            })
            .current_epoch = data.activation_range().epoch;
        if first && image.is_some() {
            candidate.image_install_intents.remove(&range);
        }
        let candidate_bytes =
            crate::distribution_range_image::bounded_json(&candidate, MAX_RETAINED)?;
        let envelope = RangeWriteEnvelope::new(candidate.clone())?;
        let bytes = crate::distribution_range_image::bounded_json(&envelope, MAX_RETAINED)?;
        if original_bytes
            .len()
            .checked_add(candidate_bytes.len())
            .and_then(|n| n.checked_add(bytes.len()))
            .is_none_or(|n| n > MAX_RETAINED)
        {
            return Err(range_write_error(
                "activation original/candidate retention exceeds 8MiB",
            ));
        }
        Ok(Self {
            original: store.catalog.clone(),
            candidate,
            bytes,
            root: store.root.canonicalize()?,
            store: store.open_id,
            database_open: db.incarnation_id().into(),
            catalogue: db
                .native_abort_catalogue_store_id()?
                .ok_or_else(|| range_write_error("activation actual catalogue identity absent"))?,
            native,
            phase_owner: domain.phase_owner(),
            frozen: frozen.observe_identity()?,
            session,
            outcomes: Vec::new(),
            owns_uncertainty: false,
            write_generation: store.current_catalogue_write_generation(),
            verified: false,
        })
    }
    fn write(
        &mut self,
        store: &mut RangeWriteStore,
        db: &crate::BicDb,
        domain: &NativeJointWriterDomain,
        frozen: &FrozenRangeMutationPermit,
        controlled: &crate::distribution_range_mutation::ControlledFrozenMutationPermit,
        authority: FreshAuthority<'_>,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.require_pending_ownership(store)?;
        self.validate_domain(db, store, domain, frozen, Some(controlled))?;
        parent.check()?;
        authority.require_current(db, store, &self.native)?;
        db.range_mutation_gate().validate(
            self.native.data().old_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        if self.outcomes.len() >= MAX_ATTEMPTS {
            return Err(range_write_error(
                "activation owned IO attempt bound; original custody retained",
            ));
        }
        self.verified = false;
        self.owns_uncertainty = true;
        store.recovery_persistence_uncertain = true;
        store.catalog = self.candidate.clone();
        store.activation_io_history.record(
            self.native.checksum_sha256(),
            self.outcomes.len(),
            "memory_candidate_published",
            None,
        );
        // Cloning/serialization never extends the original fresh window.
        // Recheck immediately before the synchronous storage effect.
        authority.require_current(db, store, &self.native)?;
        parent.check()?;
        self.session.check()?;
        // This actual IO entry supersedes ALL older owners, including for
        // identical bytes and for an error returned before/after rename.
        self.write_generation = store.note_catalogue_write_entered()?;
        self.outcomes.push(IoReturn::Entered);
        let index = self.outcomes.len() - 1;
        store
            .activation_io_history
            .record(self.native.checksum_sha256(), index, "entered", None);
        let result = crate::storage::replace_durably(
            &store.root.join(DEFAULT_RANGE_WRITE_LOG),
            crate::storage::ReplaceContents::Plain(&self.bytes),
            true,
            "native_joint_activation.catalogue",
            crate::storage::FailClosed::No,
        );
        // Record the ACTUAL return before readback, clocks or current checks.
        match result {
            Ok(()) => {
                self.outcomes[index] = IoReturn::ReturnedOk;
                store.activation_io_history.record(
                    self.native.checksum_sha256(),
                    index,
                    "returned_ok",
                    None,
                );
            }
            Err(error) => {
                // Hash without retaining Error or allocating its full Display.
                struct Hash(sha2::Sha256);
                impl std::fmt::Write for Hash {
                    fn write_str(&mut self, s: &str) -> std::fmt::Result {
                        self.0.update(s.as_bytes());
                        Ok(())
                    }
                }
                let mut hash = Hash(sha2::Sha256::new());
                let _ = std::fmt::write(&mut hash, format_args!("{error}"));
                let sha256 = hex::encode(hash.0.finalize());
                store.activation_io_history.record(
                    self.native.checksum_sha256(),
                    index,
                    "returned_error",
                    Some(&sha256),
                );
                self.outcomes[index] = IoReturn::ReturnedErr { sha256 };
                return Err(error);
            }
        }
        let mut actual = Vec::new();
        open_range_catalog_no_follow(&store.root.join(DEFAULT_RANGE_WRITE_LOG))?
            .take(MAX_RETAINED as u64 + 1)
            .read_to_end(&mut actual)?;
        if actual != self.bytes
            || serde_json::from_slice::<RangeWriteEnvelope>(&actual)?.verify()? != self.candidate
        {
            return Err(range_write_error(
                "activation exact fsynced candidate readback differs",
            ));
        }
        self.validate_domain(db, store, domain, frozen, Some(controlled))?;
        parent.check()?;
        self.session.check()?;
        self.require_pending_ownership(store)?;
        authority.require_current(db, store, &self.native)?;
        db.range_mutation_gate().validate(
            self.native.data().old_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        let version = authority.publication_guard(&self.native)?;
        if let Some(version) = &version {
            version.require_current_memory()?;
        }
        authority.require_request_memory()?;
        parent.check()?;
        self.session.check()?;
        // This publishes NEW known durability of THIS complete candidate only.
        // It never changes earlier IO history or grants a Serving observation.
        store.recovery_persistence_uncertain = false;
        self.owns_uncertainty = false;
        self.verified = true;
        Ok(())
    }

    fn require_pending_ownership(&self, store: &RangeWriteStore) -> Result<()> {
        if self.verified || self.owns_uncertainty != store.recovery_persistence_uncertain {
            return Err(range_write_error(
                "activation uncertainty is not owned by original pending publication",
            ));
        }
        if self.write_generation != store.current_catalogue_write_generation() {
            return Err(range_write_error(
                "activation original owner superseded by later catalogue IO",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
    /// Positive profile selection, never a fallback from a failed constructor.
    /// A protected/enforced birth cannot downgrade when its owner/flag is lost.
    fn activation_uses_owned_profile(&self, db: &crate::BicDb) -> Result<bool> {
        let writer_flag = crate::format::native_writer_enrollment_active(&self.root)?;
        let abort_flag = crate::format::native_abort_history_active(&self.root)?;
        let writer_version = db.actual_native_writer_enforcement_version()?;
        if writer_version == 0
            && !writer_flag
            && !abort_flag
            && self.catalog.native_writer_enforcement_version.is_none()
            && self.catalog.native_abort_protocol.is_none()
            && self._native_abort_owner.is_none()
        {
            return Ok(false);
        }
        if writer_version != 1
            || !writer_flag
            || !abort_flag
            || self.catalog.native_writer_enforcement_version != Some(1)
            || self._native_abort_owner.is_none()
            || self
                .catalog
                .native_abort_protocol
                .as_ref()
                .map(|p| p.store_id)
                != db.native_abort_catalogue_store_id()?
            || self.catalog.native_abort_protocol.is_none()
        {
            return Err(range_write_error(
                "protected activation lost original catalogue directory owner/birth/reader profile",
            ));
        }
        Ok(true)
    }
    /// Routing only. A true flag grants nothing; the actual retained owner,
    /// head/gate/native and fresh admission must all pass inside retry.
    pub(crate) fn needs_owned_activation_recovery(&self) -> bool {
        self.recovery_persistence_uncertain
    }

    /// Retain the actual full-head intent BEFORE memory or IO. No unknown
    /// head can create another owner; known completed custody can be replaced
    /// only after its actual durable readback, never from a visible file.
    pub(in crate::distribution_range_consensus::joint_handoff_state::activation) fn retain_owned_activation(
        &mut self,
        db: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        domain: &NativeJointWriterDomain,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        admission.require_live()?;
        if !self.activation_uses_owned_profile(db)? {
            if self.recovery_persistence_uncertain || self.owned_joint_activation.is_some() {
                return Err(range_write_error(
                    "unprotected activation cannot acquire or reconstruct recovery custody",
                ));
            }
            return Ok(());
        }
        if self.recovery_persistence_uncertain {
            return Err(range_write_error(
                "owned activation unresolved head requires original retry entry",
            ));
        }
        if let Some(old) = &self.owned_joint_activation {
            let old = old
                .try_lock()
                .ok_or_else(|| range_write_error("original owned activation active"))?;
            if !old.verified {
                // The exact intent can outlive a refused pre-effect gate or
                // request. Reuse it without replacing origin/candidate; this
                // is not ambiguous IO resolution or a native re-prepare.
                if !old.owns_uncertainty && old.outcomes.is_empty() {
                    old.require_binding(db, self, &admission.native_receipt(), domain)?;
                    old.require_disk(self)?;
                    old.validate_domain(db, self, domain, frozen, None)?;
                    return Ok(());
                }
                return Err(range_write_error(
                    "original owned activation not completed; no replacement",
                ));
            }
        }
        let actual = NativeJointActivationPublication::begin(
            self,
            db,
            admission.native_receipt(),
            domain,
            frozen,
            image,
            admission.original_preparation_memory_lifetime()?,
        )?;
        self.owned_joint_activation = Some(Arc::new(Mutex::new(actual)));
        Ok(())
    }
    pub(in crate::distribution_range_consensus::joint_handoff_state::activation) fn write_owned_activation_from_admission(
        &mut self,
        db: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        domain: &NativeJointWriterDomain,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        controlled: &crate::distribution_range_mutation::ControlledFrozenMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        if !self.activation_uses_owned_profile(db)? {
            return self.write_initial_unprotected_activation(
                db, admission, domain, image, controlled, parent,
            );
        }
        let actual = self
            .owned_joint_activation
            .clone()
            .ok_or_else(|| range_write_error("actual original activation intent absent"))?;
        let mut actual = actual
            .try_lock()
            .ok_or_else(|| range_write_error("actual original activation IO active"))?;
        actual.write(
            self,
            db,
            domain,
            frozen,
            controlled,
            FreshAuthority::Open(admission),
            parent,
        )
    }
    fn write_initial_unprotected_activation(
        &mut self,
        db: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        domain: &NativeJointWriterDomain,
        image: Option<&dyn NativeJointImageMaterialization>,
        controlled: &crate::distribution_range_mutation::ControlledFrozenMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        if self.activation_uses_owned_profile(db)?
            || self.recovery_persistence_uncertain
            || self.owned_joint_activation.is_some()
        {
            return Err(range_write_error(
                "legacy initial activation is not a positively unprotected healthy profile",
            ));
        }
        admission.require_live()?;
        parent.check()?;
        domain.require_current(db, self, admission.data().activation_range())?;
        admission
            .committed()
            .require_native_publication_metadata(db, self)?;
        let range = admission.data().old_range().id;
        db.range_mutation_gate()
            .validate(range, &RangeMutationAuthority::Controlled(controlled))?;
        let candidate = NativeJointCatalogueActivation::from_actual(&admission.native_receipt())?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_mut()
            .and_then(|states| states.get_mut(&range))
            .ok_or_else(|| {
                range_write_error("legacy initial activation lost permanent joint decision")
            })?;
        if state.global_decision_sha256.as_deref() != Some(admission.data().checksum_sha256())
            || state.session_id != admission.data().promise().session_id
            || state.nonce != admission.data().promise().nonce
        {
            return Err(range_write_error(
                "legacy initial activation changed permanent plan/session",
            ));
        }
        let first = state.serving_activation.is_none();
        self.recovery_persistence_uncertain = true;
        state.serving_activation = Some(candidate);
        self.catalog
            .ranges
            .entry(range)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: admission.data().activation_range().epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
                entries: BTreeMap::new(),
            })
            .current_epoch = admission.data().activation_range().epoch;
        if first && image.is_some() {
            self.catalog.image_install_intents.remove(&range);
        }
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        Ok(())
    }
    pub(crate) fn retry_owned_activation_from_actual_host(
        &mut self,
        db: &crate::BicDb,
        host: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        native: &NativeJointServingBridgeReceipt,
        domain: &NativeJointWriterDomain,
        frozen: &FrozenRangeMutationPermit,
        point: Option<&crate::distribution_data::ActualNativeJointMetadataPoint>,
        parent: &CancellationToken,
    ) -> Result<bool> {
        if !self.recovery_persistence_uncertain {
            return Ok(false);
        }
        let actual = self.owned_joint_activation.clone().ok_or_else(|| {
            range_write_error(
                "uncertain activation has no original owned intent; no reconstruction",
            )
        })?;
        let mut actual = actual
            .try_lock()
            .ok_or_else(|| range_write_error("original activation retry active"))?;
        actual.require_pending_ownership(self)?;
        actual.require_binding(db, self, native, domain)?;
        actual.require_disk(self)?;
        actual.validate_domain(db, self, domain, frozen, None)?;
        let authority = FreshAuthority::Host {
            host,
            business,
            point,
        };
        authority.require_current(db, self, native)?;
        parent.check()?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(native.data().old_range().id, frozen)?;
        let controlled = db.range_mutation_gate().try_controlled_mutation(frozen)?;
        actual.write(self, db, domain, frozen, &controlled, authority, parent)?;
        drop(controlled);
        parent.check()?;
        Ok(true)
    }
    pub(crate) fn retry_owned_activation_from_admission(
        &mut self,
        db: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        domain: &NativeJointWriterDomain,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<bool> {
        if !self.recovery_persistence_uncertain {
            return Ok(false);
        }
        let actual = self.owned_joint_activation.clone().ok_or_else(|| {
            range_write_error(
                "uncertain activation has no original owned intent; no reconstruction",
            )
        })?;
        let mut actual = actual
            .try_lock()
            .ok_or_else(|| range_write_error("original activation retry active"))?;
        actual.require_pending_ownership(self)?;
        actual.require_binding(db, self, &admission.native_receipt(), domain)?;
        actual.require_disk(self)?;
        actual.validate_domain(db, self, domain, frozen, None)?;
        admission.require_live()?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(admission.data().old_range().id, frozen)?;
        let controlled = db.range_mutation_gate().try_controlled_mutation(frozen)?;
        actual.write(
            self,
            db,
            domain,
            frozen,
            &controlled,
            FreshAuthority::Open(admission),
            parent,
        )?;
        drop(controlled);
        parent.check()?;
        Ok(true)
    }
}

#[cfg(test)]
mod controls;
