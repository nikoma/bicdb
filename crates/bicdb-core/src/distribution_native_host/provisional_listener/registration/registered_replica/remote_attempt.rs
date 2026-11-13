//! Original Registered retained attempt and read-only transfer preflight.
//! No Installing WAL, rows, floor or serving authority is produced here.
use super::no_domain::RegisteredRemoteNativePreflight;
use super::remote_image::NativeRegisteredRemoteTransferRequest;
use super::*;
use crate::distribution_range_consensus::{RangeWriteStore, RecoveryFenceScope};
use crate::distribution_range_image::{NativeRemoteImageFact, NativeRemoteImageStep};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{
    HandoffRoleAdmission, RegisteredRemoteImageInstallFreeze,
};
mod remote_mutation;
pub(crate) use remote_mutation::NativeRegisteredRemoteTransactionRequest;
mod remote_installing;

pub(crate) struct NativeRegisteredRemoteInstallPreflight {
    original: Arc<NativeRegisteredReplicaAdmission>,
    source: Arc<NativeRemoteImageFact>,
    actual: RegisteredRemoteNativePreflight,
    scope: RecoveryFenceScope,
    plan_sha256: String,
    attempt: Uuid,
    intent: crate::distribution_range_consensus::NativeRegisteredRemoteImageIntent,
}
pub(super) struct NativeRegisteredRemoteInstallAttempt {
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    lease: Option<RegisteredRemoteImageInstallFreeze>,
    proposal: Option<crate::db::NativeRegisteredRemoteProposalPublication>,
    installing:
        Option<crate::distribution_range_consensus::NativeRegisteredRemoteInstallingPublication>,
}
/// Original host-only construction, borrowing its actual A lease and fresh
/// request. Neither a DTO nor a missing native prefix constructs this point.
pub(crate) struct NativeRegisteredRemoteProposalPoint<'a> {
    preflight: &'a Arc<NativeRegisteredRemoteInstallPreflight>,
    lease: &'a RegisteredRemoteImageInstallFreeze,
    request: &'a NativeRegisteredRemoteTransferRequest,
    frame: &'a NativeRemoteImageStep<'a>,
}
impl NativeRegisteredRemoteProposalPoint<'_> {
    pub(crate) fn preflight_arc(&self) -> &Arc<NativeRegisteredRemoteInstallPreflight> {
        self.preflight
    }
    /// Necessary actual original boundary after the separate sink changes its
    /// own catalogue candidate. The sink must verify that exact new state.
    pub(crate) fn require_original_boundary(
        &self,
        db: &BicDb,
        scope: &crate::db::NativeRegisteredRemoteAttemptScope<'_>,
    ) -> Result<()> {
        self.require_live()?;
        scope.require_database(db)?;
        self.preflight.original.database.require_database(db)?;
        if scope.attempt_id() != self.preflight.attempt_id()
            || scope.range_id() != self.preflight.scope().range_id
        {
            return Err(invalid(
                "Registered original mutation boundary changed A/range",
            ));
        }
        self.preflight
            .actual
            .require_original_boundary(db, scope, &self.request.member)?;
        self.require_live()
    }
    pub(crate) fn preflight(&self) -> &NativeRegisteredRemoteInstallPreflight {
        self.preflight
    }
    pub(crate) fn request(&self) -> &NativeRegisteredRemoteTransferRequest {
        self.request
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.preflight.require_request(self.request, self.frame)?;
        if !Arc::ptr_eq(self.preflight, self.lease.preflight())
            || self.lease.try_as_frozen()?.owner() != self.preflight.attempt_id()
        {
            return Err(invalid(
                "Registered proposal does not borrow exact retained A lease",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_database_store(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &crate::db::NativeRegisteredRemoteAttemptScope<'_>,
    ) -> Result<()> {
        self.require_original_boundary(db, scope)?;
        if scope.attempt_id() != self.preflight.attempt_id()
            || scope.range_id() != self.preflight.scope().range_id
            || store.native_writer_store_open_id() != self.preflight.original.store.id()
        {
            return Err(invalid(
                "Registered proposal changed original range/Store/A point",
            ));
        }
        self.preflight.actual.require_attempt_current(
            db,
            store,
            scope,
            self.request.role.role(),
            &self.request.member,
        )?;
        self.require_live()
    }
}
impl NativeRegisteredRemoteInstallPreflight {
    pub(crate) fn source(&self) -> &Arc<NativeRemoteImageFact> {
        &self.source
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.attempt
    }
    pub(crate) fn intent(
        &self,
    ) -> &crate::distribution_range_consensus::NativeRegisteredRemoteImageIntent {
        &self.intent
    }
    pub(crate) fn native(&self) -> &crate::db::NativeRecoveryAdmission {
        &self.actual.native
    }
    pub(crate) fn bindings(&self) -> &crate::db::RangeImageDbBindings {
        &self.actual.bindings
    }
    pub(crate) fn safety(&self) -> &crate::db::NativeRangeInstallSafety {
        self.actual.safety()
    }
    pub(crate) fn catalogue(
        &self,
    ) -> &crate::distribution_range_consensus::NativeRegisteredRangeCatalogueAbsence {
        self.actual.catalogue()
    }
    pub(crate) fn require_original_process(&self) -> Result<()> {
        self.original.require_live()
    }
    pub(crate) fn require_runtime(&self, runtime: &Arc<RangeRecoveryRuntime>) -> Result<()> {
        let service = self
            .original
            .service
            .upgrade()
            .ok_or_else(|| invalid("remote attempt actual service ended"))?;
        self.original.require_listener(&service, runtime)
    }
    pub(crate) fn require_request(
        &self,
        request: &NativeRegisteredRemoteTransferRequest,
        frame: &NativeRemoteImageStep<'_>,
    ) -> Result<()> {
        self.require_original_process()?;
        request.require_live(&self.source, frame)?;
        self.scope.require_role(request.role.role())?;
        if request.plan.checksum_sha256()? != self.plan_sha256
            || self.source.manifest().target_attempt != self.attempt
            || self.intent.attempt_id() != self.attempt
            || self.intent.range_id() != self.scope.range_id
            || self.intent.original_freeze_owner() != self.scope.freeze_owner()?
        {
            return Err(invalid(
                "registered remote attempt changed exact original source/plan",
            ));
        }
        Ok(())
    }
    /// Outside service/DB/journal guards, using this actual receiver's MStore.
    pub(crate) fn require_metadata_current(
        &self,
        request: &NativeRegisteredRemoteTransferRequest,
    ) -> Result<()> {
        self.require_original_process()?;
        let backend = self
            .original
            .backend
            .upgrade()
            .ok_or_else(|| invalid("remote attempt actual MStore ended"))?;
        let parent = &request.own;
        parent.require_live_request(request.business)?;
        let metadata = backend.lock_metadata(request.cancellation())?;
        metadata.require_listener_database(&self.original.database)?;
        request.member.require_current(&metadata)?;
        self.require_original_process()
    }
    /// Nonwaiting actual point recheck. Called under the Runtime's exclusive
    /// selected-range operation; no metadata/network lock is taken here.
    pub(crate) fn require_before_rotation(
        &self,
        request: &NativeRegisteredRemoteTransferRequest,
        frame: &NativeRemoteImageStep<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.with_current_preflight(request, frame, frozen, |_db, _store, _scope| Ok(()))
    }
    pub(crate) fn with_current_preflight<R>(
        &self,
        request: &NativeRegisteredRemoteTransferRequest,
        frame: &NativeRemoteImageStep<'_>,
        frozen: &FrozenRangeMutationPermit,
        operation: impl for<'scope> FnOnce(
            &BicDb,
            &RangeWriteStore,
            &'scope crate::db::NativeRegisteredNoDomainScope<'_>,
        ) -> Result<R>,
    ) -> Result<R> {
        self.require_request(request, frame)?;
        if frozen.owner() != self.scope.freeze_owner()? || frozen.range_id() != self.scope.range_id
        {
            return Err(invalid("registered transfer lost original N-owned Frozen"));
        }
        let service = self
            .original
            .service
            .upgrade()
            .ok_or_else(|| invalid("remote attempt actual service ended"))?;
        let actual = service
            .try_lock()
            .ok_or_else(|| invalid("remote transfer actual service busy"))?;
        let database = actual.database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("remote transfer actual DB busy"))?;
        self.original.database.require_database(&db)?;
        db.with_registered_selected_no_domain_exclusion(
            &self.actual.native,
            &self.original.original_hook,
            self.scope.range_id,
            frozen,
            request.cancellation(),
            |scope| {
                actual.with_writer_data_domain_store(&db, |store| {
                    if store.native_writer_store_open_id() != self.original.store.id() {
                        return Err(invalid("registered transfer original Store slot changed"));
                    }
                    self.actual.require_current(
                        &db,
                        store,
                        scope,
                        request.role.role(),
                        &request.member,
                    )?;
                    self.intent.require_prepared_constraints(
                        store,
                        &db,
                        &self.actual.native,
                        &self.actual.bindings,
                        frame,
                        &request.role,
                        frozen,
                    )?;
                    let result = operation(&db, store, scope);
                    let post = self.require_request(request, frame).and_then(|()| {
                        self.actual.require_current(
                            &db,
                            store,
                            scope,
                            request.role.role(),
                            &request.member,
                        )?;
                        self.intent.require_prepared_constraints(
                            store,
                            &db,
                            &self.actual.native,
                            &self.actual.bindings,
                            frame,
                            &request.role,
                            frozen,
                        )
                    });
                    result.and_then(|value| post.map(|()| value))
                })
            },
        )
    }
}
impl NativeMetadataControlHost {
    #[cfg(test)]
    pub(crate) fn test_registered_remote_attempt(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let owner = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let actual = owner
            .remote_attempts
            .get(&range)
            .ok_or_else(|| invalid("retained actual remote attempt absent"))?;
        if let Some(lease) = &actual.lease {
            lease.require_retained()?;
        }
        Ok(serde_json::json!({"attempt":actual.preflight.attempt_id(),
            "original_owner":actual.preflight.scope().freeze_owner()?,
            "actual_owner":actual.lease.as_ref().map(|l|l.as_frozen().owner()),
            "source":actual.preflight.source().manifest().checksum_sha256,
            "proposal":actual.proposal.as_ref().map(|publication|publication.test_outcome()),
            "installing":actual.installing.as_ref().map(|publication|publication.test_outcome()),
            "retained":true,"lease_present":actual.lease.is_some()}))
    }
    /// Capture real readonly facts, retain this exact attempt, then transfer
    /// the unique actual runtime permit N→A. No durable writes occur here.
    pub(crate) fn prepare_registered_remote_install_attempt(
        &mut self,
        source: Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        frame.require_request_live()?;
        if !std::ptr::eq(Arc::as_ref(&source), frame.source()) {
            return Err(invalid(
                "registered attempt requires identical actual owned MF source",
            ));
        }
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid(
                "Registered transfer cannot replace an existing DATA host",
            ));
        }
        let receiver = self
            .replica
            .as_mut()
            .ok_or_else(|| invalid("original Registered receiver absent"))?;
        let scope = RecoveryFenceScope::from_role(role.role())?;
        let range = scope.range_id;
        let attempt = source.manifest().target_attempt;
        if attempt.is_nil() || attempt == scope.freeze_owner()? {
            return Err(invalid(
                "Registered target attempt must differ from original handoff session",
            ));
        }
        if receiver
            .remote_attempts
            .get(&range)
            .is_some_and(|retained| retained.lease.is_some())
        {
            return Err(invalid(
                "original Registered attempt already owns transferred freeze",
            ));
        }
        let request;
        let actual;
        let intent;
        {
            // Only a lexical shared view. Every such Arc/borrow ends BEFORE
            // Arc::get_mut and rotation in the real runtime below.
            let frozen = receiver.runtime.source_capture_freeze(&scope)?;
            let step = receiver
                .admission
                .observe_remote_image_step(frame, role, own, business, &frozen, parent)?;
            (actual, intent) = step
                .observe_selected_no_domain()?
                .into_remote_native_preflight_with(|target, db, store, scope| {
                    let intent = store.prepare_registered_remote_image_intent(
                        db, target, frame, scope, attempt,
                    )?;
                    intent.require_preflight_current(store, db, target, frame, scope)?;
                    Ok(intent)
                })?;
            request = step.into_transfer_request();
        }
        request.require_live(&source, frame)?;
        let candidate = Arc::new(NativeRegisteredRemoteInstallPreflight {
            original: receiver.admission.clone(),
            source,
            actual,
            scope,
            plan_sha256: request.plan.checksum_sha256()?,
            attempt,
            intent,
        });
        let preflight = if let Some(retained) = receiver.remote_attempts.get(&range) {
            if !Arc::ptr_eq(retained.preflight.source(), candidate.source()) {
                return Err(invalid(
                    "retained Registered attempt requires original MF Arc",
                ));
            }
            if retained.preflight.intent().sha256()? != candidate.intent().sha256()? {
                return Err(invalid(
                    "retained Registered prepared intent changed during fresh retry",
                ));
            }
            retained.preflight.require_request(&request, frame)?;
            retained.preflight.clone()
        } else {
            candidate
        };
        preflight.require_metadata_current(&request)?;
        // Original host retains the attempt BEFORE rotation or any future WAL.
        receiver.remote_attempts.entry(range).or_insert_with(|| {
            NativeRegisteredRemoteInstallAttempt {
                preflight: preflight.clone(),
                lease: None,
                proposal: None,
                installing: None,
            }
        });
        let lease = receiver.runtime.transfer_registered_remote_install(
            preflight.clone(),
            &request,
            frame,
            parent,
        )?;
        receiver.remote_attempts.get_mut(&range).unwrap().lease = Some(lease);
        preflight.require_metadata_current(&request)?;
        preflight.require_request(&request, frame)?;
        parent.check()
    }
    pub(crate) fn publish_registered_remote_proposal(
        &mut self,
        source: &Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeRegisteredRemoteProposalPointer> {
        parent.check()?;
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid("Registered proposal cannot replace DATA owner"));
        }
        let receiver = self
            .replica
            .as_mut()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let range = role.role().current_range().id;
        let attempt = receiver
            .remote_attempts
            .get_mut(&range)
            .ok_or_else(|| invalid("actual retained remote attempt absent"))?;
        if !Arc::ptr_eq(source, attempt.preflight.source()) {
            return Err(invalid(
                "Registered proposal requires same actual retained MF Arc",
            ));
        }
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Registered proposal has no actual A lease"))?;
        lease.require_retained()?;
        // Fresh A request. N's source_capture_freeze path remains unavailable.
        let step = receiver.admission.observe_remote_image_step(
            frame,
            role,
            own,
            business,
            lease.try_as_frozen()?,
            parent,
        )?;
        let request = step.into_transfer_request();
        attempt.preflight.require_metadata_current(&request)?;
        let point = NativeRegisteredRemoteProposalPoint {
            preflight: &attempt.preflight,
            lease,
            request: &request,
            frame,
        };
        point.require_live()?;
        if attempt.proposal.is_none() {
            attempt.proposal = Some(crate::db::NativeRegisteredRemoteProposalPublication::begin(
                &point,
            )?);
        }
        // Fault/control boundary is outside every DB/service/journal/hook
        // guard. An actual hook/slot revocation cannot deadlock its own reader.
        #[cfg(test)]
        crate::db::test_fire_before_registered_remote_proposal_scope();
        // Retained exact publication owner BEFORE page transaction assignment.
        let result = {
            let actual = receiver
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Registered proposal service busy"))?;
            let database = actual.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered proposal DB busy"))?;
            db.with_registered_remote_attempt_exclusion(
                attempt.preflight.native(),
                &receiver.admission.original_hook,
                lease.try_as_frozen()?,
                attempt.preflight.attempt_id(),
                request.cancellation(),
                |scope| {
                    actual.with_writer_data_domain_store(&db, |store| {
                        attempt
                            .proposal
                            .as_mut()
                            .unwrap()
                            .advance(&db, store, &point, scope)
                    })
                },
            )
        };
        let post = attempt
            .preflight
            .require_metadata_current(&request)
            .and_then(|()| point.require_live());
        let pointer = result?;
        post?;
        parent.check()?;
        Ok(pointer)
    }
}
