//! Receiver-owned prepared proof lifetime. Status never creates a freeze or
//! substitutes a caller counter for an actual retained physical read pin.
use super::*;
use crate::distribution_range_consensus::PreparedNativeRecoveryReady;

impl RangeRecoveryRuntime {
    pub(super) fn retained_native_prepared(
        &self,
        scope: &RecoveryFenceScope,
    ) -> Result<Option<Arc<PreparedNativeRecoveryReady>>> {
        let expected_owner = scope.freeze_owner()?;
        let sessions = self.sessions.lock();
        let session = sessions
            .current
            .get(&scope.range_id)
            .ok_or_else(|| invalid("no actual recovery session for native readiness"))?;
        if !session.scope.same_session(scope)
            || session
                .frozen
                .as_ref()
                .is_none_or(|f| f.owner() != expected_owner)
        {
            return Err(invalid(
                "native-ready session/freeze has changed or is leased",
            ));
        }
        Ok(session.native_prepared.clone())
    }
    pub(crate) fn prepare_native_ready(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReadyObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let scope = &request.scope;
        let witness = self.provider.witness_recovery_role(
            initiator,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            scope.purpose.clone(),
            &token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            initiator,
            &self.local_node_id,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            &scope.purpose,
        )?;
        scope.require_role(&role)?;
        let token = token.child_with_deadline(role.deadline());
        {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.preflight_native_ready(&role, request)?;
        }
        // The preflight found an actual Active/Prepared refusal. No completed
        // session may create a permanent freeze through this endpoint.
        let frozen = self.frozen(scope, &token, false)?;
        let evidence = if let Some(prepared) = self.retained_native_prepared(scope)? {
            let mut service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.reattest_local_native_ready(&prepared, &role, request, &frozen, &token)?
        } else {
            let (prepared, evidence) = {
                let mut service = loop {
                    token.check()?;
                    if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                        break s;
                    }
                };
                service.prepare_local_native_ready(role, request, frozen, &token)?
            };
            let mut sessions = self.sessions.lock();
            let session = sessions
                .current
                .get_mut(&scope.range_id)
                .ok_or_else(|| invalid("prepared receiver session disappeared"))?;
            if !session.scope.same_session(scope) || session.native_prepared.is_some() {
                return Err(invalid("native preparation raced another actual session"));
            }
            session.native_prepared = Some(Arc::new(prepared));
            evidence
        };
        token.check()?;
        NativeReadyObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
    pub(crate) fn observe_native_ready(
        &self,
        observer: &MetadataReadParticipant,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReadyObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve_observer(request.scope.range_id)?;
        self.observe_native_ready_inner(observer, request, &token)
    }
    pub(super) fn observe_native_ready_inner(
        &self,
        observer: &MetadataReadParticipant,
        request: &NativeReadyRequest,
        token: &CancellationToken,
    ) -> Result<NativeReadyObservation> {
        let scope = &request.scope;
        let prepared = self.retained_native_prepared(scope)?.ok_or_else(|| {
            invalid("no actual retained native-prepared pin; leader must prepare this replica")
        })?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = self.provider.witness_recovery_role(
            observer,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            RecoveryObservationRoleAdmission::purpose(scope)?,
            token,
        )?;
        let role = RecoveryObservationRoleAdmission::from_witness(
            witness,
            observer,
            &self.local_node_id,
            business,
            challenge,
            scope,
        )?;
        let token = token.child_with_deadline(role.deadline());
        let service = loop {
            token.check()?;
            if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                break s;
            }
        };
        let evidence = service.observe_local_native_ready(&prepared, &role, request, &token)?;
        token.check()?;
        NativeReadyObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
}
