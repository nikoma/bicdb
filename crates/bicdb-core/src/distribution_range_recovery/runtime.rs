//! Receiver-owned recovery sessions. Metadata/network waits and actual range
//! drain happen outside service/DB latches. A durable fence retains its freeze
//! even when its response is lost or the caller's one-shot budget expires.
use super::*;
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationGate};
use crate::CancellationToken;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::Arc;

const MAX_CONTEXTS: usize = 4;
const MAX_SESSIONS: usize = 64;
const LATCH_POLL: Duration = Duration::from_millis(5);

#[derive(Debug)]
struct Session {
    scope: RecoveryFenceScope,
    frozen: Arc<FrozenRangeMutationPermit>,
}
#[derive(Debug, Default)]
struct Sessions {
    current: BTreeMap<RangeId, Session>,
    active: BTreeSet<RangeId>,
}

#[derive(Debug)]
pub(crate) struct RangeRecoveryRuntime {
    service: Arc<Mutex<ClusterDataNodeService>>,
    provider: Arc<MetadataReadProvider>,
    local_node_id: ClusterNodeId,
    gate: RangeMutationGate,
    sessions: Mutex<Sessions>,
}
struct Operation<'a>(&'a RangeRecoveryRuntime, RangeId);
impl Drop for Operation<'_> {
    fn drop(&mut self) {
        self.0.sessions.lock().active.remove(&self.1);
    }
}
impl RangeRecoveryRuntime {
    pub(crate) fn observe_history(
        &self,
        observer: &MetadataReadParticipant,
        request: &RecoveryFencedProbeRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let scope = &request.scope;
        let purpose = RecoveryObservationRoleAdmission::purpose(scope)?;
        let witness = self.provider.witness_recovery_role(
            observer,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            purpose,
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
        // This cannot create/refresh a durable fence; the service below must
        // find the exact already-persisted session and native DB incarnation.
        {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.require_persisted_recovery_scope(scope)?;
        }
        let frozen = self.frozen(scope, &token, false)?;
        let service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        let reply = service.observe_native_recovery_history(&role, request, &frozen, &token)?;
        token.check()?;
        RecoveryHistoryObservation::from_local(
            reply,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
    pub(crate) fn new(
        service: Arc<Mutex<ClusterDataNodeService>>,
        provider: Arc<MetadataReadProvider>,
    ) -> Self {
        let (local_node_id, gate) = {
            let service = service.lock();
            (service.node_id().clone(), service.range_mutation_gate())
        };
        Self {
            service,
            provider,
            local_node_id,
            gate,
            sessions: Mutex::new(Sessions::default()),
        }
    }
    fn reserve(&self, range: RangeId) -> Result<Operation<'_>> {
        let mut sessions = self.sessions.lock();
        if sessions.active.len() >= MAX_CONTEXTS || !sessions.active.insert(range) {
            return Err(invalid("receiver recovery context/range is busy"));
        }
        Ok(Operation(self, range))
    }
    fn frozen(
        &self,
        scope: &RecoveryFenceScope,
        token: &CancellationToken,
        replace: bool,
    ) -> Result<Arc<FrozenRangeMutationPermit>> {
        token.check()?;
        {
            let mut sessions = self.sessions.lock();
            if let Some(session) = sessions.current.get_mut(&scope.range_id) {
                if session.scope.same_session(scope) {
                    self.gate
                        .validate_quiescent_frozen(scope.range_id, &session.frozen)?;
                    return Ok(Arc::clone(&session.frozen));
                }
                if !replace {
                    return Err(invalid(
                        "read-only observation cannot rotate a recovery freeze",
                    ));
                }
                if matches!(
                    session.scope.purpose,
                    MetadataRecoveryRolePurpose::CommittedHandoff { .. }
                ) {
                    return Err(invalid(
                        "normal recovery cannot take a retained handoff freeze",
                    ));
                }
                let frozen = Arc::get_mut(&mut session.frozen).ok_or_else(|| {
                    invalid("old recovery freeze still has a source/callback owner")
                })?;
                frozen.try_rotate_owner(scope.recovery_nonce)?;
                session.scope = scope.clone();
                return Ok(Arc::clone(&session.frozen));
            }
            if sessions.current.len() >= MAX_SESSIONS {
                return Err(invalid("receiver recovery session bound reached"));
            }
        }
        let remaining = token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or_else(|| invalid("recovery drain deadline expired"))?;
        // No service/database/session-map lock while waiting for WAL callbacks.
        let frozen =
            Arc::new(
                self.gate
                    .freeze(scope.range_id, scope.recovery_nonce, remaining, token)?,
            );
        token.check()?;
        let mut sessions = self.sessions.lock();
        sessions.current.insert(
            scope.range_id,
            Session {
                scope: scope.clone(),
                frozen: Arc::clone(&frozen),
            },
        );
        Ok(frozen)
    }
    pub(crate) fn fence_epoch(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryEpochFenceRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryFenceObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let scope = &request.scope;
        // Actual initiating peer and fresh recipient challenge enter a new
        // metadata leader begin/probe/record/finish; no cached role fallback.
        let observation = self.provider.witness_recovery_role(
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
            observation,
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
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.preflight_recovery_nonce(scope)?;
        }
        let frozen = self.frozen(scope, &token, true)?;
        token.check()?;
        let mut service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        let reply = service.fence_native_recovery(&role, scope, &frozen, &token)?;
        token.check()?;
        RecoveryFenceObservation::from_local(
            reply,
            self.provider.local_participant(),
            scope,
            token.deadline().unwrap(),
        )
    }
}
