//! Incoming Complete independently collects its own actual closed native QQ.
//! Only the resulting persistence is observed; the session keeps its pin/freeze.
use super::*;
enum IncomingCompletionDecision {
    Closed(ClosedNativeReadyQuorum),
    Completed(NativeCompletionQuorum),
    Activated(ActivatedNativeCompletionDecision),
}
impl IncomingCompletionDecision {
    fn require_recipient_role(&self, f: &RecoveryRoleAdmission) -> Result<()> {
        match self {
            Self::Closed(q) => q.require_recipient_role(f),
            Self::Completed(q) => q.require_recipient_role(f),
            Self::Activated(q) => q.require_recipient_role(f),
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
        #[cfg(test)]
        let mut complete_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-complete-total",
            &self.local_node_id,
            &token,
        );
        #[cfg(test)]
        eprintln!("ACTUAL_RECOVERY_COMPLETE_REQUEST node={} challenge={} session={} scope={:?} parent_remaining_ms={:?}", self.local_node_id, request.challenge, request.scope.recovery_nonce, request.scope, parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()));
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
        #[cfg(test)]
        let mut selection_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-complete-protocol-selection",
            &self.local_node_id,
            &selection_budget,
        );
        let selection = self.select_native_completion_protocol(
            initiator,
            request,
            &selection_budget,
            &token,
            Arc::clone(&operation),
        )?;
        #[cfg(test)]
        {
            selection_trace.complete();
            drop(selection_trace);
        }
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
            NativeCompletionSelection::Decision(d) => {
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
                    IncomingCompletionDecision::Activated(d),
                )
            }
            NativeCompletionSelection::Constraint(constraint) => {
                #[cfg(test)]
                let mut drain_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
                    "recovery-complete-selection-worker-drain",
                    &self.local_node_id,
                    &token,
                );
                while Arc::strong_count(&operation) != 1 {
                    token.check()?;
                    std::thread::sleep(LATCH_POLL);
                }
                #[cfg(test)]
                {
                    drain_trace.complete();
                    drop(drain_trace);
                }
                #[cfg(test)]
                let mut ready_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
                    "recovery-complete-closed-ready-QQ",
                    &self.local_node_id,
                    &token,
                );
                let (prepared, q) = self.acquire_closed_native_ready_with_operation(
                    initiator,
                    &request.ready_request(),
                    &token,
                    Arc::clone(&operation),
                )?;
                constraint.require_closed_quorum(&q)?;
                #[cfg(test)]
                {
                    ready_trace.complete();
                    drop(ready_trace);
                }
                (Some(prepared), IncomingCompletionDecision::Closed(q))
            }
        };
        token.check()?;
        // Revalidate metadata AFTER quorum collection. This fresh context may
        // not renew the caller budget or the original opaque QQ lifetime.
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        #[cfg(test)]
        let mut post_qq_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-complete-post-QQ-metadata-role",
            &self.local_node_id,
            &token,
        );
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
        #[cfg(test)]
        {
            post_qq_trace.complete();
            drop(post_qq_trace);
        }
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
        let evidence = {
            #[cfg(test)]
            let mut service_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
                "recovery-complete-service-lock-acquisition",
                &self.local_node_id,
                &token,
            );
            let mut service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            #[cfg(test)]
            {
                service_trace.complete();
                drop(service_trace);
            }
            match quorum {
                IncomingCompletionDecision::Closed(q) => {
                    service.complete_local_native_recovery(&prepared, q, &fresh, request, &token)?
                }
                IncomingCompletionDecision::Completed(q) => {
                    service.adopt_local_native_completion(&prepared, q, &fresh, request, &token)?
                }
                IncomingCompletionDecision::Activated(d) => service
                    .adopt_local_native_activated_decision(&prepared, d, &fresh, request, &token)?,
            }
        };
        token.check()?;
        #[cfg(test)]
        complete_trace.complete();
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
        #[cfg(test)]
        NativeProbeTraceSpan::event(
            "completion-status",
            "receiver-request-entry",
            &self.local_node_id,
            &request.scope,
            request.challenge,
            parent.deadline(),
            format!("request_budget_ms={}", request.budget_ms),
        );
        let token = request.cancellation(parent)?;
        #[cfg(test)]
        let mut operation_trace = NativeProbeOperationExit::new(
            "completion-status",
            &self.local_node_id,
            &request.scope,
            request.challenge,
            token.deadline(),
        );
        let reserved = self.reserve_observer(request.scope.range_id);
        #[cfg(test)]
        operation_trace.detail(format!(
            "reservation_ok={} reservation_error={:?}",
            reserved.is_ok(),
            reserved.as_ref().err().map(ToString::to_string)
        ));
        let _operation = reserved?;
        #[cfg(test)]
        operation_trace.bind(&_operation);
        let result = self.observe_native_completion_inner(observer, request, &token);
        #[cfg(test)]
        operation_trace.outcome(&result);
        result
    }
    pub(super) fn observe_native_completion_inner(
        &self,
        observer: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        token: &CancellationToken,
    ) -> Result<NativeCompletionObservation> {
        #[cfg(test)]
        let mut handler_trace = NativeProbeTraceSpan::new(
            "completion-status",
            "receiver-handler",
            &self.local_node_id,
            &request.scope,
            request.challenge,
            token.deadline(),
        );
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
        #[cfg(test)]
        let mut role_trace = NativeProbeTraceSpan::new(
            "completion-status",
            "receiver-readonly-role",
            &self.local_node_id,
            scope,
            request.challenge,
            token.deadline(),
        );
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
        #[cfg(test)]
        {
            role_trace.detail(format!("role_context={} witness_challenge={} actual_role_remaining_ms={} effective_request_remaining_ms={:?}", role.metadata_context_id(), challenge, role.deadline().saturating_duration_since(Instant::now()).as_millis(), token.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis())));
            role_trace.complete();
            drop(role_trace);
        }
        #[cfg(test)]
        let mut service_trace = NativeProbeTraceSpan::new(
            "completion-status",
            "receiver-service-acquisition",
            &self.local_node_id,
            scope,
            request.challenge,
            token.deadline(),
        );
        let service = loop {
            token.check()?;
            if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                break s;
            }
        };
        #[cfg(test)]
        {
            service_trace.complete();
            drop(service_trace);
        }
        #[cfg(test)]
        let mut validation_trace = NativeProbeTraceSpan::new(
            "completion-status",
            "receiver-local-validation",
            &self.local_node_id,
            scope,
            request.challenge,
            token.deadline(),
        );
        let evidence =
            service.observe_local_native_completion(prepared.as_deref(), &role, request, &token)?;
        #[cfg(test)]
        {
            validation_trace.complete();
            drop(validation_trace);
        }
        token.check()?;
        #[cfg(test)]
        let mut projection_trace = NativeProbeTraceSpan::new(
            "completion-status",
            "receiver-final-observation",
            &self.local_node_id,
            scope,
            request.challenge,
            token.deadline(),
        );
        let result = NativeCompletionObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        );
        #[cfg(test)]
        {
            projection_trace.detail(format!(
                "result_ok={} error={:?}",
                result.is_ok(),
                result.as_ref().err().map(ToString::to_string)
            ));
            if result.is_ok() {
                projection_trace.complete();
                handler_trace.complete();
            }
        }
        result
    }
}
