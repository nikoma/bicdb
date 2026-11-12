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

impl RangeRecoveryRuntime {
    /// Only a live capture from the distinct actual mixed-quorum/full-history
    /// factory may convert this refusal-only cold session into an owned image
    /// lease. No ordinary recovery authority, caller owner number or DTO works.
    pub(crate) fn transfer_current_image_resume(
        self: &Arc<Self>,
        scope: &RecoveryFenceScope,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &crate::distribution_range_consensus::NativeImageInstallRole<'_>,
        attempt_id: Uuid,
        parent: &CancellationToken,
    ) -> Result<RecoveryImageInstallFreeze> {
        let _operation = self.reserve(scope.range_id)?;
        scope.require_role(role.role())?;
        let pointer = source
            .cut()
            .resume_proposal()
            .ok_or_else(|| invalid("source is not an actual verified interrupted-image cut"))?;
        if attempt_id == scope.freeze_owner()? || attempt_id == pointer.proposal_id {
            return Err(invalid("resumed install requires a fresh distinct attempt"));
        }
        let token = parent.child_with_deadline(role.role().deadline());
        token.check()?;
        let original = {
            let sessions = self.sessions.lock();
            if sessions.current.contains_key(&scope.range_id) {
                return Err(invalid(
                    "ordinary recovery session already owns resumed range",
                ));
            }
            let session = sessions
                .installing
                .get(&scope.range_id)
                .ok_or_else(|| invalid("no actual cold Installing session"))?;
            if !session.scope.same_session(scope) || &session.proposal != pointer {
                return Err(invalid(
                    "resumed source differs from exact Installing session",
                ));
            }
            Arc::clone(
                session
                    .frozen
                    .as_ref()
                    .ok_or_else(|| invalid("Installing freeze is already leased"))?,
            )
        };
        let preflight = {
            let mut service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            {
                let sessions = self.sessions.lock();
                let session = sessions
                    .installing
                    .get(&scope.range_id)
                    .ok_or_else(|| invalid("Installing session disappeared"))?;
                service.validate_current_image_installing(
                    &session.admission,
                    role.role(),
                    &token,
                )?;
            }
            service.preflight_recovery_image_transfer(
                source,
                role,
                &original,
                crate::distribution_range_consensus::ImageInstallPurpose::CurrentVoterRepair,
                attempt_id,
                &token,
            )?
        };
        if preflight.original_freeze_owner() != scope.freeze_owner()?
            || preflight.nonce() != scope.recovery_nonce
            || preflight.range_id() != scope.range_id
            || preflight.attempt_id() != attempt_id
        {
            return Err(invalid(
                "actual resumed transfer preflight has another scope",
            ));
        }
        drop(original);
        token.check()?;
        let mut sessions = self.sessions.lock();
        if sessions.current.contains_key(&scope.range_id) {
            return Err(invalid("another normal session raced resume transfer"));
        }
        let session = sessions
            .installing
            .get_mut(&scope.range_id)
            .ok_or_else(|| invalid("Installing ownership disappeared"))?;
        if &session.proposal != pointer || !session.scope.same_session(scope) {
            return Err(invalid("Installing proposal changed before transfer"));
        }
        let frozen = Arc::get_mut(
            session
                .frozen
                .as_mut()
                .ok_or_else(|| invalid("Installing ownership is already leased"))?,
        )
        .ok_or_else(|| invalid("another actual source/callback holds Installing freeze"))?;
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("service busy before resumed owner rotation"))?;
        service.validate_current_image_installing(&session.admission, role.role(), &token)?;
        service.rotate_recovery_image_transfer(&preflight, source, role, frozen, &token)?;
        drop(service);
        let mut session = sessions.installing.remove(&scope.range_id).unwrap();
        let arc = session.frozen.take().unwrap();
        let frozen =
            Arc::try_unwrap(arc).expect("unique resumed ownership checked under sessions latch");
        sessions.current.insert(
            scope.range_id,
            Session {
                scope: scope.clone(),
                frozen: None,
                image_preflight: None,
                native_prepared: None,
            },
        );
        Ok(RecoveryImageInstallFreeze {
            runtime: Arc::clone(self),
            scope: scope.clone(),
            frozen: Some(frozen),
            preflight: Some(preflight),
            returned: false,
        })
    }
}
