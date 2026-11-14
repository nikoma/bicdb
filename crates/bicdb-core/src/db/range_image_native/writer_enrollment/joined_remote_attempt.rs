//! Original Joined A and SAME installed writer, not FIRST/NoDomain. This
//! lexical scope grants no ordinary selected-range writes or activation.
use super::*;
use crate::distribution_native_host::{
    NativeJoinedRangeReceiveRequest, NativeJoinedRemoteInstallPreflight,
};
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_image::NativeRemoteImageStep;
use crate::distribution_range_recovery::JoinedRemoteImageInstallFreeze;

pub(crate) struct NativeJoinedRemoteAttemptScope<'a> {
    db: &'a BicDb,
    preflight: &'a Arc<NativeJoinedRemoteInstallPreflight>,
    request: &'a NativeJoinedRangeReceiveRequest,
    frame: &'a NativeRemoteImageStep<'a>,
    lease: &'a JoinedRemoteImageInstallFreeze,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    journal: NativeImageJournalGuard<'a>,
}
impl NativeJoinedRemoteAttemptScope<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        self.preflight.scope().range_id
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.preflight.attempt_id()
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.preflight.native()
    }
    pub(crate) fn preflight(&self) -> &Arc<NativeJoinedRemoteInstallPreflight> {
        self.preflight
    }
    pub(crate) fn request(&self) -> &NativeJoinedRangeReceiveRequest {
        self.request
    }
    pub(crate) fn frozen(&self) -> Result<&FrozenRangeMutationPermit> {
        self.lease.try_as_frozen()
    }
    pub(crate) fn original_writer_sha256(&self) -> &str {
        &self.request.origin().guard().state.record.checksum_sha256
    }
    fn require_database_identity(&self, db: &BicDb) -> Result<()> {
        self.preflight.require_request(self.request, self.frame)?;
        if !std::ptr::eq(db, self.db)
            || !Arc::ptr_eq(self.preflight, self.lease.preflight())
            || self.frozen()?.owner() != self.attempt_id()
            || self.frozen()?.range_id() != self.range_id()
        {
            return Err(invalid(
                "Joined native attempt changed actual original DB/A",
            ));
        }
        self.journal.require_database(db)?;
        db.require_joined_publication_storage_health()?;
        db.require_original_registered_receiver_birth_open_with_journal(
            self.native(),
            &self.journal,
        )?;
        self.request.member().require_database_point(db)?;
        let original = &self.request.origin().guard().state;
        original.require_borrowed_original_hook(db, &self.hook)?;
        let stored: NativeWriterPolicyRecord =
            db.read_native_range_image_record(POLICY_POINTER)?
                .ok_or_else(|| invalid("Joined original writer policy disappeared"))?;
        stored.validate()?;
        if stored != original.record {
            return Err(invalid("Joined native attempt replaced original writer"));
        }
        db.range_mutation_gate()
            .validate_frozen(self.range_id(), self.frozen()?)?;
        self.request
            .member()
            .publication_version()
            .require_current_memory()?;
        self.preflight.require_request(self.request, self.frame)
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.require_database_identity(db)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.range_id(), self.frozen()?)
    }
    /// Exact actual owners only. A retained catalogue continuation separately
    /// proves/refsyncs its own original candidate before clearing uncertainty.
    pub(crate) fn require_store_identity(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_database(db)?;
        self.preflight.require_original_writer(db, store)?;
        self.require_database(db)
    }
    pub(crate) fn require_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_store_identity(db, store)?;
        db.require_native_writer_catalogue_custody(store)?;
        self.require_database(db)
    }
    /// This exact selected lower bound is still unknown/empty, not R=0. The
    /// completed original row-absence scan stays owned by preflight; N -> A
    /// preserves the SAME frozen lane and never admits an intervening writer.
    pub(crate) fn require_initial_preimage(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        self.require_store(db, store)?;
        self.preflight.safety().require_current_under_journal(
            db,
            self.native(),
            self.frozen()?,
            &self.journal,
        )?;
        self.request
            .origin()
            .guard()
            .require_new_joined_data_range_point(self.range_id())?;
        self.preflight
            .selected()
            .catalogue()
            .require_attempt_current(store, db, self)?;
        self.require_store(db, store)
    }
}
impl BicDb {
    /// The host has checked actual runtime retention BEFORE obtaining any
    /// service/DB lock. Only the opaque actual A lease can construct this
    /// scope; a raw Frozen or serialized proposal is insufficient.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_joined_remote_attempt_scope<'a, R>(
        &'a self,
        preflight: &'a Arc<NativeJoinedRemoteInstallPreflight>,
        request: &'a NativeJoinedRangeReceiveRequest,
        frame: &'a NativeRemoteImageStep<'a>,
        lease: &'a JoinedRemoteImageInstallFreeze,
        operation: impl for<'scope> FnOnce(&'scope NativeJoinedRemoteAttemptScope<'a>) -> Result<R>,
    ) -> Result<R> {
        let scope = NativeJoinedRemoteAttemptScope {
            db: self,
            preflight,
            request,
            frame,
            lease,
            _admission: self
                .serializable_commit_admission
                .try_read()
                .ok_or_else(|| invalid("Joined native attempt admission busy"))?,
            hook: self
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Joined native attempt original hook busy"))?,
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

/// Same original A physical guards, distinct controlled-row context. Only its
/// lexical constructor can produce the weak row-transaction lifetime witness.
pub(crate) struct NativeJoinedRemoteMutationScope<'a> {
    // Drop this lifetime BEFORE the actual admission/hook/journal guards.
    lifetime: Arc<()>,
    original: NativeJoinedRemoteAttemptScope<'a>,
}
pub(in crate::db::range_image_native) struct JoinedRemoteMutationScopeLiveness {
    scope: std::sync::Weak<()>,
    database_open: String,
    attempt: Uuid,
    range: RangeId,
}
impl JoinedRemoteMutationScopeLiveness {
    pub(in crate::db::range_image_native) fn require_current(
        &self,
        db: &BicDb,
        attempt: Uuid,
        range: RangeId,
    ) -> Result<()> {
        if self.scope.upgrade().is_none()
            || self.database_open != db.incarnation_id()
            || self.attempt != attempt
            || self.range != range
        {
            return Err(invalid(
                "Joined row transaction outlived actual guarded scope",
            ));
        }
        db.require_joined_publication_storage_health()
    }
}
impl NativeJoinedRemoteMutationScope<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        self.original.range_id()
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.original.attempt_id()
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.original.native()
    }
    pub(crate) fn preflight(&self) -> &Arc<NativeJoinedRemoteInstallPreflight> {
        self.original.preflight()
    }
    pub(crate) fn request(&self) -> &NativeJoinedRangeReceiveRequest {
        self.original.request()
    }
    pub(crate) fn frozen(&self) -> Result<&FrozenRangeMutationPermit> {
        self.original.frozen()
    }
    pub(crate) fn original_writer_sha256(&self) -> &str {
        self.original.original_writer_sha256()
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.original.require_database_identity(db)
    }
    /// Actual original service/Store/State only. This is NOT a durability
    /// exception for image/row callers. Only the retained catalogue sink uses
    /// it to reconcile its original exact uncertain file publication.
    pub(crate) fn require_store_identity(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_database(db)?;
        self.preflight().require_original_writer(db, store)?;
        self.require_database(db)
    }
    pub(crate) fn require_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_store_identity(db, store)?;
        db.require_native_writer_catalogue_custody(store)?;
        self.require_database(db)
    }
    pub(crate) fn require_original_install_safety(&self, db: &BicDb) -> Result<()> {
        self.require_database(db)?;
        self.preflight().safety().require_current_under_journal(
            db,
            self.native(),
            self.frozen()?,
            &self.original.journal,
        )?;
        self.require_database(db)
    }
    pub(in crate::db::range_image_native) fn retained_scope_liveness(
        &self,
    ) -> JoinedRemoteMutationScopeLiveness {
        JoinedRemoteMutationScopeLiveness {
            scope: Arc::downgrade(&self.lifetime),
            database_open: self.original.db.incarnation_id().into(),
            attempt: self.attempt_id(),
            range: self.range_id(),
        }
    }
}
impl BicDb {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_joined_remote_mutation_scope<'a, R>(
        &'a self,
        preflight: &'a Arc<NativeJoinedRemoteInstallPreflight>,
        request: &'a NativeJoinedRangeReceiveRequest,
        frame: &'a NativeRemoteImageStep<'a>,
        lease: &'a JoinedRemoteImageInstallFreeze,
        operation: impl for<'scope> FnOnce(&'scope NativeJoinedRemoteMutationScope<'a>) -> Result<R>,
    ) -> Result<R> {
        let scope = NativeJoinedRemoteMutationScope {
            lifetime: Arc::new(()),
            original: NativeJoinedRemoteAttemptScope {
                db: self,
                preflight,
                request,
                frame,
                lease,
                _admission: self
                    .serializable_commit_admission
                    .try_read()
                    .ok_or_else(|| invalid("Joined mutation original admission busy"))?,
                hook: self
                    .commit_admission
                    .try_read()
                    .ok_or_else(|| invalid("Joined mutation original hook busy"))?,
                journal: self.try_native_image_journal()?,
            },
        };
        scope.original.require_database(self)?;
        let result = operation(&scope);
        let post = scope.original.require_database(self);
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
