//! Distinct owned original Release ingress. It cannot enter preparation,
//! bridge or catalogue Open sinks, and never renews an old request.
use super::*;

pub(crate) struct NativeRemoteReleaseRequestAdmission {
    role: Arc<RecoveryRoleAdmission>,
    own: Arc<HostMetadataReadObservation>,
    point: Arc<ActualNativeJointMetadataPoint>,
    business: Uuid,
    phase: Uuid,
    decision: String,
    session: Uuid,
    nonce: Uuid,
    retry: bool,
    process: Arc<NativeRemotePreparationProcess>,
    token: CancellationToken,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn admit_native_release(
        &self,
        process: Arc<NativeRemotePreparationProcess>,
    ) -> Result<NativeRemoteReleaseRequestAdmission> {
        self.require_request()?;
        process.require_memory()?;
        if !matches!(
            self.request.operation,
            NativeRemoteServingOperationV1::Release
                | NativeRemoteServingOperationV1::ReconcileRelease
        ) {
            return Err(invalid(
                "native release requires distinct actual mutating Release ingress",
            ));
        }
        Ok(NativeRemoteReleaseRequestAdmission {
            role: self.role.clone(),
            own: self.own.clone(),
            point: self.point.clone(),
            business: self.business,
            phase: self.request.phase_id,
            decision: self.request.decision_sha256.clone(),
            session: self.request.manifest.handoff_session,
            nonce: self.request.manifest.nonce,
            retry: self.request.operation == NativeRemoteServingOperationV1::ReconcileRelease,
            token: process
                .bound_request(&self.bound_actor_request())
                .child_linked_to(&self.own.original_request_cancellation())
                .child_with_deadline(self.role.deadline().min(self.own.deadline())),
            process,
        })
    }
}
impl NativeRemoteReleaseRequestAdmission {
    #[cfg(test)]
    pub(crate) fn test_original_windows(&self) -> serde_json::Value {
        let now = Instant::now();
        serde_json::json!({
            "actual_request_remaining_us":self.token.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
            "actual_Role_remaining_us":self.role.deadline().saturating_duration_since(now).as_micros(),
            "actual_OWN_remaining_us":self.own.deadline().saturating_duration_since(now).as_micros(),
            "actual_process_live":self.process.require_memory().is_ok(),
            "same_native_MStore_memory_live":self.point.require_current_memory().is_ok(),
        })
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.token.check()?;
        self.process.require_memory()?;
        self.role.require_live_request()?;
        self.own.require_live_request(self.business)?;
        self.point.require_current_memory()
    }
    pub(crate) fn require_current(
        &self,
        fresh: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_memory_current()?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.point.require_request(&self.own, self.business)?;
        fresh.require_current_metadata_only()?;
        if self.phase.is_nil()
            || self.decision != fresh.data().checksum_sha256()
            || self.session != fresh.data().promise().session_id
            || self.nonce != fresh.data().promise().nonce
            || self.role.local_node_id() != fresh.native_receipt().local_node_id()
        {
            return Err(invalid(
                "Release changed original authenticated recipient/phase/decision",
            ));
        }
        self.require_memory_current()
    }
    pub(crate) fn require_scope(
        &self,
        fresh: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_current(fresh)?;
        self.point
            .require_native_store_point(scope.database(), store)?;
        self.process
            .require_original_native(scope.database(), fresh)?;
        self.require_memory_current()
    }
    pub(crate) fn require_successor(
        &self,
        original: &crate::distribution_data::NativeRemoteReleaseOwner,
    ) -> Result<()> {
        self.require_memory_current()?;
        original.require_process(&self.process)
    }
    pub(crate) fn require_service(
        &self,
        service: &crate::distribution_data::ClusterDataNodeService,
    ) -> Result<()> {
        self.require_memory_current()?;
        self.process.require_service(service)
    }
    pub(crate) fn bound_request(&self) -> CancellationToken {
        self.token.child()
    }
    pub(crate) fn retry(&self) -> bool {
        self.retry
    }
}

impl NativeRemoteServingQueue {
    pub(in crate::distribution_native_host) fn resolve_original_release(
        &self,
        work: &NativeRemoteServingIngress,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        observed: &crate::distribution_data::NativeRemoteReleaseObservation,
    ) -> Result<()> {
        if work.request.operation != NativeRemoteServingOperationV1::ReconcileRelease {
            return Err(invalid(
                "release resolution requires distinct same-owner ReconcileRelease",
            ));
        }
        work.require_request()?;
        admission.require_remote_successor(observed.owner())?;
        observed.require_released_open()?;
        let expected = admission.activation().activation_sha256()?;
        let native = admission.native().checksum_sha256().to_string();
        let binding = (
            work.peer.node_id().clone(),
            work.peer.member_incarnation(),
            work.peer
                .certificate_sha256()
                .ok_or_else(|| invalid("release resolution actual TLS peer absent"))?
                .to_string(),
            work.request.original_operation,
            work.request.manifest.checksum_sha256.clone(),
            work.request.phase_id,
        );
        let mut removed = None;
        observed.inspect(
            |scope, _store, _head, activation, _frozen, released| {
                if !released || activation != expected || scope.native().checksum_sha256() != native
                {
                    return Err(invalid(
                        "release resolution changed actual same-owner dropped/armed Open",
                    ));
                }
                Ok(())
            },
            || {
                work.require_request_memory()?;
                admission.require_remote_successor(observed.owner())?;
                let mut state = self
                    .state
                    .try_lock()
                    .ok_or_else(|| invalid("release resolution original queue busy"))?;
                if state.binding.as_ref() != Some(&binding)
                    || work.request.sequence != state.admitted
                {
                    return Err(invalid(
                        "release resolution replaced original actor/admitted request",
                    ));
                }
                let Some(old) = state.unknown.as_ref() else {
                    return Ok(());
                };
                if !matches!(
                    old.request.operation,
                    NativeRemoteServingOperationV1::Release
                        | NativeRemoteServingOperationV1::ReconcileRelease
                ) || old.request.original_operation != work.request.original_operation
                    || old.request.phase_id != work.request.phase_id
                    || old.request.manifest.checksum_sha256 != work.request.manifest.checksum_sha256
                    || old.request.decision_sha256 != work.request.decision_sha256
                {
                    return Err(invalid(
                        "release resolution cannot replace another original unknown",
                    ));
                }
                // The original return remains attributed in returned_chain;
                // this is a NEW protected outcome, never a rewritten old Ok.
                removed = state.unknown.take();
                Ok(())
            },
        )?;
        drop(removed);
        Ok(())
    }
}
