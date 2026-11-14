//! Actual lexical physical rotation point, not a detached precheck or lease.
//! The temporary N Arc is consumed and dropped BEFORE the runtime callback.
use super::*;
use crate::distribution_native_host::NativeJoinedRemoteInstallPreflight;
use crate::distribution_range_image::NativeRemoteImageStep;

pub(crate) struct NativeJoinedRemoteRotationPoint<'a> {
    db: &'a BicDb,
    preflight: &'a Arc<NativeJoinedRemoteInstallPreflight>,
    request: &'a NativeJoinedRangeReceiveRequest,
    frame: &'a NativeRemoteImageStep<'a>,
    store: &'a RangeWriteStore,
    frozen: crate::distribution_range_mutation::FrozenRangeMutationIdentity,
    registration: Arc<crate::db::commit_admission_hook::CommitAdmissionRegistration>,
    writer: Arc<NativeWriterHostState>,
    _admission: parking_lot::RwLockReadGuard<'a, ()>,
    _hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    _journal: NativeImageJournalGuard<'a>,
}
impl NativeJoinedRemoteRotationPoint<'_> {
    /// Only atomics/deadlines/private current gate identity. Actual native,
    /// policy/schema/canonical/hash checks are tied to the still-held physical
    /// guards. No MStore lock, filesystem/native read, serialization or callback.
    pub(crate) fn require_current_memory(
        &self,
        actual_n: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.request.require_request_memory()?;
        self.frame.require_request_live()?;
        self.request
            .member()
            .publication_version()
            .require_current_memory()?;
        self.frozen.require_same_current(actual_n)?;
        if !std::ptr::eq(self.preflight.source().as_ref(), self.frame.source())
            || !self.registration.is_active()
            || self.registration.generation() != self.writer.record.generation
            || !Arc::ptr_eq(&self.writer, &self.store.native_writer_host_state()?)
        {
            return Err(invalid(
                "Joined prepared rotation lost exact original State/registration/N",
            ));
        }
        // These methods only inspect sticky Core/page/WAL health. Their error
        // reporting cannot make a failed health check into publication.
        self.db.require_joined_publication_storage_health()?;
        self.request.require_request_memory()
    }
    pub(crate) fn publication_guard(
        &self,
    ) -> Result<crate::distribution_consensus::NativeMetadataPublicationGuard<'_>> {
        self.request
            .member()
            .publication_version()
            .try_publication_guard()
    }
}
impl BicDb {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_joined_remote_rotation_point<'a, R>(
        &'a self,
        store: &'a RangeWriteStore,
        preflight: &'a Arc<NativeJoinedRemoteInstallPreflight>,
        request: &'a NativeJoinedRangeReceiveRequest,
        frame: &'a NativeRemoteImageStep<'a>,
        temporary_n: Arc<FrozenRangeMutationPermit>,
        operation: impl for<'point> FnOnce(&'point NativeJoinedRemoteRotationPoint<'a>) -> Result<R>,
    ) -> Result<R> {
        // Physical prechecks. The host wrapper already owns the exact actual
        // service/DB borrow and made current-MStore checks before entering it.
        preflight.require_request(request, frame)?;
        preflight
            .selected()
            .origin()
            .require_database_store(self, store)?;
        let writer = store.native_writer_host_state()?;
        let admission = self
            .serializable_commit_admission
            .try_read()
            .ok_or_else(|| invalid("Joined rotation actual admission busy"))?;
        let hook = self
            .commit_admission
            .try_read()
            .ok_or_else(|| invalid("Joined rotation actual installed hook busy"))?;
        writer.require_borrowed_original_hook(self, &hook)?;
        let registration = hook.registration().clone();
        let journal = self.try_native_image_journal()?;
        let frozen = temporary_n.observe_identity()?;
        {
            // This transient view borrows the owned guard bundle. It does not
            // make the eventual point or callback borrow the temporary N Arc.
            let point = NativeJoinedReceiveStorageScope {
                db: self,
                request,
                native: preflight.native(),
                frozen: &temporary_n,
                _admission: JoinedReceiveGuard::Borrowed(&admission),
                hook: JoinedReceiveGuard::Borrowed(&hook),
                journal: JoinedReceiveGuard::Borrowed(&journal),
            };
            point.require_store(self, store)?;
            preflight.intent().require_current(
                store,
                self,
                preflight.selected(),
                request,
                frame,
                &point,
            )?;
        }
        let point = NativeJoinedRemoteRotationPoint {
            db: self,
            preflight,
            request,
            frame,
            store,
            frozen,
            registration,
            writer,
            _admission: admission,
            _hook: hook,
            _journal: journal,
        };
        // A readonly identity retains no permit Weak/strong Arc. The runtime
        // callback can now prove its real unique N and consume/rotate it.
        drop(temporary_n);
        operation(&point)
        // No postcheck may demand the old N identity after a successful A take.
    }
}
