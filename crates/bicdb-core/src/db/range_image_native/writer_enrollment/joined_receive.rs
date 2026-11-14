//! Borrowed actual installed writer for a selected Joined image receiver.
//! Identity and storage constraints only: no NoDomain/R0, remote source fact,
//! row transaction, native install, decision, Open or ordinary range grant.
use super::*;
use crate::distribution_native_host::NativeJoinedRangeReceiveRequest;
use crate::distribution_range_consensus::RangeWriteStore;

pub(crate) struct NativeJoinedReceiveStorageScope<'a> {
    db: &'a BicDb,
    request: &'a NativeJoinedRangeReceiveRequest,
    native: &'a NativeRecoveryAdmission,
    frozen: &'a FrozenRangeMutationPermit,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeJoinedReceiveStorageScope<'_> {
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.native
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.request.origin().range_id()
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.request.require_live()?;
        if !std::ptr::eq(db, self.db) {
            return Err(invalid("Joined receive scope changed actual borrowed DB"));
        }
        self.journal.require_database(db)?;
        db.require_joined_publication_storage_health()?;
        self.native.require_current(db)?;
        // Original Registered birth/open remains, but B0 and an empty whole
        // native namespace would incorrectly forbid the existing served range.
        db.require_original_registered_receiver_birth_open_with_journal(
            self.native,
            &self.journal,
        )?;
        self.request.member().require_database_point(db)?;
        let guard = self.request.origin().guard();
        guard.state.require_borrowed_original_hook(db, &self.hook)?;
        let stored: NativeWriterPolicyRecord =
            db.read_native_range_image_record(POLICY_POINTER)?
                .ok_or_else(|| invalid("Joined receive original writer policy disappeared"))?;
        stored.validate()?;
        if stored != guard.state.record {
            return Err(invalid(
                "Joined receive replaced original installed native policy",
            ));
        }
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.range_id(), self.frozen)?;
        self.request
            .member()
            .publication_version()
            .require_current_memory()?;
        self.request.require_live()
    }
    pub(crate) fn require_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_database(db)?;
        let writer = store.native_writer_host_state()?;
        if !Arc::ptr_eq(&writer, &self.request.origin().guard().state) {
            return Err(invalid(
                "Joined receive Store carries another installed State",
            ));
        }
        writer.require_store(db.data_path(), store.native_writer_store_open_id())?;
        db.require_native_writer_catalogue_custody(store)?;
        self.require_database(db)
    }
    /// Actual lower bounds, including pending/invalidated state, are DATA.
    /// A caller must not interpret all-None as a certified zero history.
    pub(crate) fn observe_selected_install_safety(&self) -> Result<NativeRangeInstallSafety> {
        self.require_database(self.db)?;
        self.db.native_range_install_safety_under_journal(
            self.native,
            self.range_id(),
            self.frozen,
            &self.journal,
        )
    }
}
impl InstalledNativeWriterHostGuard {
    /// Full same-State hook/native-policy check for each row-scan chunk. This
    /// is outside any final version publication guard and holds no hook or
    /// journal guard across the subsequent actual paged scan.
    pub(crate) fn require_joined_receive_identity(&self, db: &BicDb) -> Result<()> {
        db.require_joined_publication_storage_health()?;
        self.state.require_original_identity_database(db)?;
        self.state.require_verified_registry_database(db)
    }
}
impl BicDb {
    /// This request was captured outside engine locks. The caller must perform
    /// its current-MStore pre/post checks there as well; the local version
    /// point below rejects known stale state, never replaces a quorum lease.
    pub(crate) fn with_joined_receive_storage_scope<R>(
        &self,
        request: &NativeJoinedRangeReceiveRequest,
        native: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        operation: impl for<'scope> FnOnce(&'scope NativeJoinedReceiveStorageScope<'_>) -> Result<R>,
    ) -> Result<R> {
        request.require_live()?;
        let role = request.role().role();
        if native.local_node_id() != role.local_node_id()
            || native.local_member_incarnation() != role.local_member_incarnation()
            || native.cluster_id() != role.cluster_id()
            || native.bootstrap_id() != role.bootstrap_id()
            || request.origin().range_id() != role.current_range().id
            || frozen.owner()
                != crate::distribution_range_consensus::RecoveryFenceScope::from_role(role)?
                    .freeze_owner()?
        {
            return Err(invalid(
                "Joined receive storage request changed native/selected freeze identity",
            ));
        }
        let scope = NativeJoinedReceiveStorageScope {
            db: self,
            request,
            native,
            frozen,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Joined receive actual writer admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Joined receive original installed hook busy"))?,
            journal: self.try_native_image_journal()?,
        };
        scope.require_database(self)?;
        let result = operation(&scope);
        let post = scope.require_database(self);
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(value) => {
                post?;
                Ok(value)
            }
        }
    }
}
