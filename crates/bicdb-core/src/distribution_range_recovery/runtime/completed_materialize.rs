//! Actual closed-decision target repair. No Activated-to-Active authority cast.
use super::*;
use crate::distribution_range_recovery::{
    CompletedNativeCommandAdmission, NativeCompletedMaterializeRequest, RecoveryApplyAdmission,
};
impl RangeRecoveryRuntime {
    pub(crate) fn materialize_completed_native_command(
        self: &Arc<Self>,
        original_host: &crate::distribution_native_host::NativeInitialDataAdmission,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletedMaterializeRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReadyObservation> {
        let token = request.cancellation(parent)?;
        let scope = &request.historical.scope;
        original_host.require_listener(&self.service, self)?;
        let operation = self.reserve(scope.range_id)?;
        let (slot, original_db) = {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            (
                service.native_replica_listener_store_open_guard(),
                service.listener_open_guard(),
            )
        };
        let current = || -> Result<()> {
            original_host.require_listener(&self.service, self)?;
            slot.require_live()?;
            original_db.require_live()
        };
        current()?;
        // RequireQuorum mode: a lone activated decision fallback is NOT a
        // distinct physical completed quorum for this new materialization.
        let completion = NativeCompletionRequest {
            scope: scope.clone(),
            challenge: Uuid::now_v7(),
            budget_ms: crate::distribution_native_apply::remaining_budget(&token)?,
        };
        let quorum = self.acquire_native_completion_quorum_with_operation(
            initiator,
            &completion,
            &token,
            Arc::clone(&operation),
        )?;
        token.check()?;
        current()?;
        // All real observer workers retain their context until actual exit.
        while Arc::strong_count(&operation) != 1 {
            token.check()?;
            std::thread::sleep(LATCH_POLL);
        }
        let role = self.normal_role(
            initiator,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            &token,
        )?;
        scope.require_role(&role)?;
        let token = token.child_with_deadline(role.deadline());
        let proof = CompletedNativeCommandAdmission::from_actual_quorum(
            quorum,
            role,
            request.historical.command.clone(),
            &token,
        )?;
        {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            current()?;
            service.require_completed_native_command(&proof, &token)?;
        }
        let admission = RecoveryApplyAdmission::from_completed_decision(proof)?;
        let retained = {
            let sessions = self.sessions.lock();
            sessions
                .current
                .get(&scope.range_id)
                .is_some_and(|s| s.native_prepared.is_some())
        };
        // A same-open prepared target needs actual Prepare reattestation, not
        // re-fencing or replaying an already immutable native history leaf.
        if !retained {
            {
                let service = loop {
                    token.check()?;
                    if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                        break s;
                    }
                };
                service.preflight_native_recovery(
                    admission.completed_decision().unwrap().role(),
                    scope,
                    &token,
                )?;
            }
            let frozen = self.frozen(scope, &token, false)?;
            {
                let mut service = loop {
                    token.check()?;
                    if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                        break s;
                    }
                };
                current()?;
                let role = admission
                    .completed_decision()
                    .expect("sealed disjoint proof")
                    .role();
                service.require_completed_native_command(
                    admission.completed_decision().unwrap(),
                    &token,
                )?;
                service.fence_native_recovery(role, scope, &frozen, &token)?;
                service.apply_native_historical_command(role, &admission, &frozen, &token)?;
                current()?;
                admission.require_live_request()?;
            }
        } else {
            // Existing actual prepared pin is revalidated by the ordinary
            // prepare endpoint below. No DTO or durable phase creates it.
            admission.require_live_request()?;
        }
        current()?;
        token.check()?;
        drop(operation);
        // Exact same finite request parent; this separately reproves actual
        // local native preparation and retains the real pin/Frozen owner.
        let mut ready_request = request.ready_request();
        ready_request.budget_ms = crate::distribution_native_apply::remaining_budget(&token)?;
        let observed = self.prepare_native_ready(initiator, &ready_request, &token)?;
        token.check()?;
        current()?;
        admission.require_live_request()?;
        let completed = admission.completed_decision().unwrap();
        if observed.reply().range_index != request.historical.command.index
            || observed.reply().logical_genesis_sha256 != completed.logical_genesis_sha256()
            || observed.reply().logical_history_sha256 != completed.logical_history_sha256()
        {
            return Err(invalid(
                "completed materialization prepared another actual cut",
            ));
        }
        Ok(observed)
    }
}
