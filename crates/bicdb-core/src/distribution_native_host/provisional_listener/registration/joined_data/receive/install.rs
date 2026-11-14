//! Sealed original prepared Joined A. Constructor consumes actual prepared
//! source/selected constraints only; it performs no transfer/WAL/native apply.
use super::*;
use crate::distribution_range_consensus::{NativeJoinedRemoteImageIntent, RecoveryFenceScope};
use crate::distribution_range_image::{NativeRemoteImageFact, NativeRemoteImageStep};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

pub(crate) struct NativeJoinedRemoteInstallPreflight {
    selected: Arc<NativeJoinedRangeReceivePreflight>,
    intent: NativeJoinedRemoteImageIntent,
    plan_sha256: String,
    readonly_plan: crate::distribution_range_handoff::NativeJointReadOnlyPlanIdentity,
}
impl NativeJoinedRemoteInstallPreflight {
    pub(crate) fn service(&self) -> &Arc<Mutex<ClusterDataNodeService>> {
        &self.selected.origin().parts.backend.service
    }
    pub(crate) fn readonly_plan(
        &self,
    ) -> &crate::distribution_range_handoff::NativeJointReadOnlyPlanIdentity {
        &self.readonly_plan
    }
    pub(crate) fn require_original_writer(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<Arc<crate::db::NativeWriterHostState>> {
        self.selected.origin().require_database_store(db, store)?;
        store.native_writer_host_state()
    }
    pub(crate) fn selected(&self) -> &Arc<NativeJoinedRangeReceivePreflight> {
        &self.selected
    }
    pub(crate) fn source(&self) -> &Arc<NativeRemoteImageFact> {
        self.intent.source()
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.selected.scope()
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id()
    }
    pub(crate) fn intent(&self) -> &NativeJoinedRemoteImageIntent {
        &self.intent
    }
    pub(crate) fn native(&self) -> &crate::db::NativeRecoveryAdmission {
        self.selected.native()
    }
    pub(crate) fn bindings(&self) -> &crate::db::RangeImageDbBindings {
        self.selected.bindings()
    }
    pub(crate) fn safety(&self) -> &crate::db::NativeRangeInstallSafety {
        self.selected.safety()
    }
    pub(crate) fn require_original_process(&self) -> Result<()> {
        self.selected.origin().require_live()
    }
    pub(crate) fn require_runtime(&self, runtime: &Arc<RangeRecoveryRuntime>) -> Result<()> {
        let origin = self.selected.origin();
        origin.require_live()?;
        if !Arc::ptr_eq(&origin.parts.runtime, runtime) {
            return Err(invalid(
                "Joined install preflight changed actual original runtime",
            ));
        }
        origin
            .admission
            .require_listener(&origin.parts.backend.service, runtime)
    }
    pub(crate) fn require_request(
        &self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
    ) -> Result<()> {
        self.require_original_process()?;
        request.require_live()?;
        frame.require_request_live()?;
        self.scope().require_role(request.role().role())?;
        if !Arc::ptr_eq(self.selected.origin(), request.origin_arc())
            || !std::ptr::eq(self.source().as_ref(), frame.source())
            || self.intent.range_id() != self.scope().range_id
            || self.intent.original_freeze_owner() != self.scope().freeze_owner()?
            || self.source().manifest().target_attempt != self.attempt_id()
            || request.plan()?.checksum_sha256()? != self.plan_sha256
        {
            return Err(invalid(
                "Joined original install changed source/MF/A/plan/request",
            ));
        }
        request.require_live()
    }
    /// Outside engine locks, including after an uncertain or post-WAL error.
    pub(crate) fn require_metadata_current(
        &self,
        request: &NativeJoinedRangeReceiveRequest,
    ) -> Result<()> {
        self.require_original_process()?;
        if !Arc::ptr_eq(self.selected.origin(), request.origin_arc()) {
            return Err(invalid(
                "Joined install metadata request changed original actual owner",
            ));
        }
        request.require_metadata_current()
    }
    /// Necessary actual PRE-rotation point. No Runtime/MStore lock is acquired
    /// here; the full physical precheck MUST occur before Runtime.sessions,
    /// then the final separately prepared memory-only point is used under
    /// the real runtime operation/session. The caller separately supplies
    /// supplies fresh metadata/version proof before the unique N->A transfer.
    pub(crate) fn require_before_rotation(
        &self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_request(request, frame)?;
        let origin = self.selected.origin();
        let service = origin
            .parts
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("Joined install original service busy before rotation"))?;
        let database = service.database();
        if !Arc::ptr_eq(&database, &origin.parts.factory.database) {
            return Err(invalid("Joined install changed actual DB before rotation"));
        }
        let db = database
            .try_read()
            .ok_or_else(|| invalid("Joined install original DB busy before rotation"))?;
        service.with_writer_data_domain_store(&db, |store| {
            origin.require_database_store(&db, store)
        })?;
        db.with_joined_receive_storage_scope(request, self.native(), frozen, |point| {
            service.with_writer_data_domain_store(&db, |store| {
                self.intent
                    .require_current(store, &db, &self.selected, request, frame, point)
            })
        })?;
        self.require_request(request, frame)
    }
}
impl NativeJoinedRangeReceiveRequest {
    pub(crate) fn prepare_remote_install_preflight(
        &self,
        selected: Arc<NativeJoinedRangeReceivePreflight>,
        source: Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        frozen: &Arc<FrozenRangeMutationPermit>,
    ) -> Result<Arc<NativeJoinedRemoteInstallPreflight>> {
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        if !Arc::ptr_eq(selected.origin(), &self.origin)
            || !std::ptr::eq(source.as_ref(), frame.source())
        {
            return Err(invalid(
                "Joined prepared install requires original receiver and actual MF owner",
            ));
        }
        let plan = self.plan()?;
        let plan_sha256 = plan.checksum_sha256()?;
        let readonly_plan = plan.into_readonly_identity()?;
        let attempt = source.manifest().target_attempt;
        let intent = selected.with_current(self, frozen, |db, store, point| {
            let intent = store.prepare_joined_remote_image_intent(
                db,
                &selected,
                self,
                source.clone(),
                frame,
                point,
                attempt,
            )?;
            intent.require_current(store, db, &selected, self, frame, point)?;
            Ok(intent)
        })?;
        let prepared = Arc::new(NativeJoinedRemoteInstallPreflight {
            selected,
            intent,
            plan_sha256,
            readonly_plan,
        });
        prepared.require_request(self, frame)?;
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        Ok(prepared)
    }
}
