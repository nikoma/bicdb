//! Same-voter activation driver. Actual completion decisions are collected over
//! bounded authenticated channels; no caller list or decoded stage unlocks data.
use super::*;
impl RangeRecoveryRuntime {
    pub(crate) fn activate_native_recovery(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<NativeCompletionObservation> {
        let token = request.cancellation(parent)?;
        let operation = self.reserve(request.scope.range_id)?;
        let quorum = self.acquire_native_completion_quorum_with_operation(
            initiator,
            request,
            &token,
            Arc::clone(&operation),
        )?;
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
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
        let fresh = RecoveryRoleAdmission::from_witness(
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
        scope.require_role(&fresh)?;
        quorum.require_recipient_role(&fresh)?;
        let token = token.child_with_deadline(fresh.deadline());
        let has_session = {
            let sessions = self.sessions.lock();
            sessions.current.contains_key(&scope.range_id)
        };
        if !has_session {
            // A durable anchor after cold open is useful decision evidence,
            // but cannot arm a new DB open or synthesize its physical pin.
            {
                let service = loop {
                    token.check()?;
                    if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                        break s;
                    }
                };
                service.require_current_native_activation(request, &fresh, &quorum)?;
            }
            return self.observe_native_completion_inner(initiator, request, &token);
        }
        let prepared = self
            .retained_native_prepared(scope)?
            .ok_or_else(|| invalid("activation requires actual owned completed materialization"))?;
        let mut service = loop {
            token.check()?;
            if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                break s;
            }
        };
        let receipt = service.activate_local_native_recovery(&prepared, quorum, &fresh, &token)?;
        // Stop admission of new readonly owners while checking uniqueness and
        // removing this exact session. Existing owners cause a bounded retry.
        let mut sessions = self.sessions.lock();
        let session = sessions
            .current
            .get(&scope.range_id)
            .ok_or_else(|| invalid("activation receiver session disappeared"))?;
        let saved = session
            .native_prepared
            .as_ref()
            .ok_or_else(|| invalid("activation lost its owned preparation"))?;
        let frozen = session
            .frozen
            .as_ref()
            .ok_or_else(|| invalid("activation freeze is image-leased"))?;
        if !session.scope.same_session(scope)
            || !Arc::ptr_eq(saved, &prepared)
            || Arc::strong_count(saved) != 2
            || Arc::weak_count(saved) != 0
            || Arc::strong_count(frozen) != 2
            || Arc::weak_count(frozen) != 0
            || session.image_preflight.is_some()
        {
            return Err(invalid(
                "activation remains fenced until actual source/status/freeze owners drain",
            ));
        }
        self.gate
            .validate_quiescent_frozen(scope.range_id, frozen)?;
        let mut owned = sessions
            .current
            .remove(&scope.range_id)
            .expect("checked session");
        drop(prepared);
        let physical = match Arc::try_unwrap(owned.native_prepared.take().expect("checked proof")) {
            Ok(p) => p,
            Err(p) => {
                owned.native_prepared = Some(p);
                sessions.current.insert(scope.range_id, owned);
                return Err(invalid("activation physical owner raced final release"));
            }
        };
        // Arm is the LAST fallible step. On failure put BOTH owned guards back;
        // no Drop/error may silently release this durable recovery refusal.
        if let Err(error) = service.arm_local_native_activation(
            &physical,
            &receipt,
            &fresh,
            owned.frozen.as_ref().unwrap(),
            &token,
        ) {
            owned.native_prepared = Some(Arc::new(physical));
            sessions.current.insert(scope.range_id, owned);
            return Err(error);
        }
        // No network, filesystem, cancellation or other fallible work between
        // these drops. LocalReady declares pin BEFORE frozen; the session owns
        // the only remaining frozen Arc and it is released LAST.
        drop(physical);
        drop(owned.frozen.take());
        drop(sessions);
        drop(service);
        self.observe_native_completion_inner(initiator, request, &token)
    }
}
