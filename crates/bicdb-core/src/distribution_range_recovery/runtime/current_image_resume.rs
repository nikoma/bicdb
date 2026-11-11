//! A separate cold Installing session. It is refusal-only until a sealed fresh
//! mixed quorum/source-history producer authorizes an exact resumed installer.
use super::*;
use crate::distribution_range_consensus::{
    CurrentImageInstallingAdmission, NativeCurrentImageProposalPointer,
};
use crate::distribution_range_recovery::{
    CurrentImageInstallingObservation, CurrentImageInstallingRequest,
};

#[derive(Debug)]
pub(super) struct InstallingResumeSession {
    pub(super) scope: RecoveryFenceScope,
    pub(super) proposal: NativeCurrentImageProposalPointer,
    pub(super) frozen: Option<Arc<FrozenRangeMutationPermit>>,
    pub(super) admission: CurrentImageInstallingAdmission,
}
impl RangeRecoveryRuntime {
    pub(crate) fn acquire_current_image_installing_fence(
        &self,
        initiator: &MetadataReadParticipant,
        request: &CurrentImageInstallingRequest,
        parent: &CancellationToken,
    ) -> Result<CurrentImageInstallingObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
        let business = Uuid::now_v7();
        let recipient_challenge = Uuid::now_v7();
        let scope = &request.scope;
        if !matches!(scope.purpose, MetadataRecoveryRolePurpose::NormalRecovery) {
            return Err(invalid("installing fence requires NormalRecovery"));
        }
        let witness = self.provider.witness_recovery_role(
            initiator,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            recipient_challenge,
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
            recipient_challenge,
            &scope.purpose,
        )?;
        scope.require_role(&role)?;
        let token = token.child_with_deadline(role.deadline());
        let admission = {
            let mut service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.preflight_current_image_installing(&role, scope, &request.proposal, &token)?
        };
        let existing = {
            let sessions = self.sessions.lock();
            if sessions.current.contains_key(&scope.range_id) {
                return Err(invalid("hot ordinary/image session still owns this range; cold Installing admission cannot steal it"));
            }
            if let Some(session) = sessions.installing.get(&scope.range_id) {
                if !session.scope.same_session(scope) || session.proposal != request.proposal {
                    return Err(invalid("another exact Installing session is retained"));
                }
                Some(Arc::clone(
                    session
                        .frozen
                        .as_ref()
                        .ok_or_else(|| invalid("Installing freeze is leased"))?,
                ))
            } else {
                if sessions.current.len() + sessions.installing.len() >= MAX_SESSIONS {
                    return Err(invalid("recovery session bound reached"));
                }
                None
            }
        };
        let frozen = if let Some(frozen) = existing {
            frozen
        } else {
            let remaining = token
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .ok_or_else(|| invalid("Installing drain deadline expired"))?;
            let frozen = Arc::new(self.gate.freeze(
                scope.range_id,
                scope.freeze_owner()?,
                remaining,
                &token,
            )?);
            let mut sessions = self.sessions.lock();
            if sessions.current.contains_key(&scope.range_id)
                || sessions.installing.contains_key(&scope.range_id)
            {
                return Err(invalid("Installing session raced another owner"));
            }
            sessions.installing.insert(
                scope.range_id,
                InstallingResumeSession {
                    scope: scope.clone(),
                    proposal: request.proposal.clone(),
                    frozen: Some(Arc::clone(&frozen)),
                    admission,
                },
            );
            frozen
        };
        self.gate
            .validate_quiescent_frozen(scope.range_id, &frozen)?;
        let evidence = {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            // Use a new actual preflight on every call; an old stored metadata
            // context never extends the caller's original acquisition budget.
            let sessions = self.sessions.lock();
            let session = sessions
                .installing
                .get(&scope.range_id)
                .ok_or_else(|| invalid("Installing session disappeared"))?;
            service.observe_current_image_installing(
                &session.admission,
                &role,
                &frozen,
                request.challenge,
                &token,
            )?
        };
        token.check()?;
        CurrentImageInstallingObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
}
