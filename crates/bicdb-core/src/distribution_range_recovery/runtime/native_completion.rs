//! Incoming Complete independently collects its own actual closed native QQ.
//! Only the resulting persistence is observed; the session keeps its pin/freeze.
use super::*;
enum IncomingCompletionDecision {
    Closed(ClosedNativeReadyQuorum),
    Completed(NativeCompletionQuorum),
}
impl IncomingCompletionDecision {
    fn require_recipient_role(&self, f: &RecoveryRoleAdmission) -> Result<()> {
        match self {
            Self::Closed(q) => q.require_recipient_role(f),
            Self::Completed(q) => q.require_recipient_role(f),
        }
    }
}

impl RangeRecoveryRuntime {
    pub(crate) fn complete_native_recovery(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<NativeCompletionObservation> {
        let token = request.cancellation(parent)?;
        let operation = self.reserve(request.scope.range_id)?;
        // This collection obtains actual fresh leader-only metadata authority
        // and authentic peer evidence without any service/DB latch held. The
        // SAME operation remains owned through durable completion and workers
        // retain their counted contexts until canceled work really drains.
        let remaining = token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or_else(|| invalid("completion request expired"))?;
        let selection_budget = token.child_with_deadline(Instant::now() + remaining / 2);
        let selection = self.select_native_completion_protocol(
            initiator,
            request,
            &selection_budget,
            &token,
            Arc::clone(&operation),
        )?;
        let (prepared, quorum) = match selection {
            NativeCompletionSelection::Quorum(q) => {
                let has = {
                    let sessions = self.sessions.lock();
                    sessions.current.contains_key(&request.scope.range_id)
                };
                (
                    if has {
                        self.retained_native_prepared(&request.scope)?
                    } else {
                        None
                    },
                    IncomingCompletionDecision::Completed(q),
                )
            }
            NativeCompletionSelection::Constraint(constraint) => {
                while Arc::strong_count(&operation) != 1 {
                    token.check()?;
                    std::thread::sleep(LATCH_POLL);
                }
                let (prepared, q) = self.acquire_closed_native_ready_with_operation(
                    initiator,
                    &request.ready_request(),
                    &token,
                    Arc::clone(&operation),
                )?;
                constraint.require_closed_quorum(&q)?;
                (Some(prepared), IncomingCompletionDecision::Closed(q))
            }
        };
        token.check()?;
        // Revalidate metadata AFTER quorum collection. This fresh context may
        // not renew the caller budget or the original opaque QQ lifetime.
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
        let already_activated = {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.native_completion_is_activated(request)?
        };
        if already_activated {
            // Reissue only the independently checked immutable decision. This
            // never restores an open-admission map or releases a pending pin.
            return self.observe_native_completion_inner(initiator, request, &token);
        }
        let prepared = prepared
            .ok_or_else(|| invalid("completion has no actual local prepared materialization"))?;
        let evidence =
            {
                let mut service = loop {
                    token.check()?;
                    if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                        break s;
                    }
                };
                match quorum {
                    IncomingCompletionDecision::Closed(q) => service
                        .complete_local_native_recovery(&prepared, q, &fresh, request, &token)?,
                    IncomingCompletionDecision::Completed(q) => service
                        .adopt_local_native_completion(&prepared, q, &fresh, request, &token)?,
                }
            };
        token.check()?;
        NativeCompletionObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
    pub(crate) fn observe_native_completion(
        &self,
        observer: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<NativeCompletionObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve_observer(request.scope.range_id)?;
        self.observe_native_completion_inner(observer, request, &token)
    }
    pub(super) fn observe_native_completion_inner(
        &self,
        observer: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        token: &CancellationToken,
    ) -> Result<NativeCompletionObservation> {
        let scope = &request.scope;
        let has_session = {
            let sessions = self.sessions.lock();
            sessions.current.contains_key(&scope.range_id)
        };
        let prepared = if has_session {
            self.retained_native_prepared(scope)?
        } else {
            None
        };
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
            &token,
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
        let evidence =
            service.observe_local_native_completion(prepared.as_deref(), &role, request, &token)?;
        token.check()?;
        NativeCompletionObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
}
