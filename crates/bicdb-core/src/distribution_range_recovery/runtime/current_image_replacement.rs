//! Cold Installing refusal under a new current MQ/same-voter data domain. The
//! old persisted intent/recovery fence is untouched by observation/certification.
use super::*;
use crate::distribution_range_consensus::InstallingCommittedAdmission;
use crate::distribution_range_recovery::{
    InstallingCommittedCertificationObservation, InstallingCommittedDecision,
    InstallingCommittedFenceObservation, InstallingCommittedRequest,
};
#[derive(Debug)]
pub(super) struct InstallingReplacementSession {
    pub(super) scope: RecoveryFenceScope,
    pub(super) pointer: crate::distribution_range_consensus::NativeCurrentImageProposalPointer,
    pub(super) frozen: Option<Arc<FrozenRangeMutationPermit>>,
    pub(super) admission: InstallingCommittedAdmission,
}
impl RangeRecoveryRuntime {
    pub(crate) fn acquire_installing_committed_fence(
        &self,
        initiator: &MetadataReadParticipant,
        request: &InstallingCommittedRequest,
        parent: &CancellationToken,
    ) -> Result<InstallingCommittedFenceObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
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
        let admission = {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.preflight_installing_committed(&role, &request.proposal, &token)?
        };
        let existing = {
            let sessions = self.sessions.lock();
            if sessions.current.contains_key(&scope.range_id)
                || sessions.installing.contains_key(&scope.range_id)
            {
                return Err(invalid(
                    "Installing replacement cannot steal another retained owner",
                ));
            }
            if let Some(s) = sessions.replacing.get(&scope.range_id) {
                if !s.scope.same_session(scope) || s.pointer != request.proposal {
                    return Err(invalid(
                        "another replacement current scope owns Installing freeze",
                    ));
                }
                Some(Arc::clone(
                    s.frozen
                        .as_ref()
                        .ok_or_else(|| invalid("replacement freeze is leased"))?,
                ))
            } else {
                if sessions.current.len() + sessions.installing.len() + sessions.replacing.len()
                    >= MAX_SESSIONS
                {
                    return Err(invalid("replacement session bound reached"));
                }
                None
            }
        };
        let frozen = if let Some(f) = existing {
            f
        } else {
            let remaining = token
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .ok_or_else(|| invalid("replacement drain expired"))?;
            let f = Arc::new(self.gate.freeze(
                scope.range_id,
                scope.freeze_owner()?,
                remaining,
                &token,
            )?);
            let mut sessions = self.sessions.lock();
            if sessions.current.contains_key(&scope.range_id)
                || sessions.installing.contains_key(&scope.range_id)
                || sessions.replacing.contains_key(&scope.range_id)
            {
                return Err(invalid("replacement owner raced another session"));
            }
            sessions.replacing.insert(
                scope.range_id,
                InstallingReplacementSession {
                    scope: scope.clone(),
                    pointer: request.proposal.clone(),
                    frozen: Some(Arc::clone(&f)),
                    admission,
                },
            );
            f
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
            let sessions = self.sessions.lock();
            let s = sessions
                .replacing
                .get(&scope.range_id)
                .ok_or_else(|| invalid("replacement session disappeared"))?;
            service.observe_installing_committed(
                &role,
                &s.admission,
                &frozen,
                request.challenge,
                &token,
            )?
        };
        token.check()?;
        InstallingCommittedFenceObservation::from_local(
            evidence,
            self.provider.local_participant(),
            request,
            &token,
        )
    }
    /// Trusted local orchestration only: actual independent role and mixed QQ
    /// are already owned by the decision. A wire/controller producer is separate.
    pub(crate) fn certify_installing_existing_committed(
        &self,
        decision: &InstallingCommittedDecision,
        parent: &CancellationToken,
    ) -> Result<InstallingCommittedCertificationObservation> {
        decision.require_live()?;
        let a = decision.authority();
        let role = a.role();
        if role.local_node_id() != &self.local_node_id {
            return Err(invalid("existing-decision recipient changed"));
        }
        let token = parent.child_with_deadline(a.deadline());
        token.check()?;
        let _operation = self.reserve(a.scope().range_id)?;
        let frozen = {
            let sessions = self.sessions.lock();
            if role.local_node_id() == a.target() {
                let s = sessions
                    .replacing
                    .get(&a.scope().range_id)
                    .ok_or_else(|| invalid("no actual Installing replacement session"))?;
                if !s.scope.same_session(a.scope()) || &s.pointer != a.pointer() {
                    return Err(invalid(
                        "Installing certification is another session/proposal",
                    ));
                }
                Arc::clone(
                    s.frozen
                        .as_ref()
                        .ok_or_else(|| invalid("Installing freeze leased"))?,
                )
            } else {
                let s = sessions
                    .current
                    .get(&a.scope().range_id)
                    .ok_or_else(|| invalid("no actual ordinary recovery session"))?;
                if !s.scope.same_session(a.scope()) {
                    return Err(invalid(
                        "ordinary certifier is another current recovery session",
                    ));
                }
                Arc::clone(
                    s.frozen
                        .as_ref()
                        .ok_or_else(|| invalid("ordinary freeze leased"))?,
                )
            }
        };
        let evidence = {
            let mut service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.certify_installing_existing_committed(decision, &frozen, &token)?
        };
        token.check()?;
        InstallingCommittedCertificationObservation::from_local(
            evidence,
            self.provider.local_participant(),
            decision,
            &token,
        )
    }
    /// Existing physical Apply only on an ordinary recipient, with an ACTUAL
    /// distinct current certification QQ. Installing can never obtain this path.
    pub(crate) fn apply_installing_existing_committed(
        &self,
        admission: crate::distribution_range_recovery::InstallingCommittedApplyAdmission,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeRecoveryAppliedReceipt> {
        admission.require_live()?;
        let token = parent.child_with_deadline(admission.deadline());
        let _operation = self.reserve(admission.scope().range_id)?;
        if admission.role().local_node_id() != &self.local_node_id {
            return Err(invalid("existing-decision Apply recipient changed"));
        }
        let frozen = self.frozen(admission.scope(), &token, false)?;
        let admission=crate::distribution_range_recovery::RecoveryApplyAdmission::from_installing_existing_committed(admission)?;
        let role = admission
            .installing_role()
            .expect("constructor preserves mixed proof");
        let mut service = loop {
            token.check()?;
            if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                break s;
            }
        };
        service.apply_native_historical_command(role, &admission, &frozen, &token)
    }
}
impl RangeRecoveryRuntime {
    /// Final-only transition consumes a real full-history replacement source.
    /// Its durable handover still refuses all writes and leaves old provenance.
    pub(crate) fn transfer_current_image_replacement(
        self: &Arc<Self>,
        scope: &RecoveryFenceScope,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &crate::distribution_range_consensus::NativeImageInstallRole<'_>,
        attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<RecoveryImageInstallFreeze> {
        source.require_current()?;
        scope.require_role(role.role())?;
        let token = parent.child_with_deadline(role.role().deadline());
        token.check()?;
        let operation = self.reserve(scope.range_id)?;
        if !source.cut().is_actual_replacement_source() {
            return Err(invalid(
                "replacement transfer requires actual completed source-history cut",
            ));
        }
        let original = {
            let sessions = self.sessions.lock();
            let s = sessions
                .replacing
                .get(&scope.range_id)
                .ok_or_else(|| invalid("no actual replacement refusal session"))?;
            if !s.scope.same_session(scope) || source.cut().resume_proposal() != Some(&s.pointer) {
                return Err(invalid(
                    "replacement transfer changed current scope/protected parent",
                ));
            }
            Arc::clone(
                s.frozen
                    .as_ref()
                    .ok_or_else(|| invalid("replacement freeze leased"))?,
            )
        };
        self.gate
            .validate_quiescent_frozen(scope.range_id, &original)?;
        let handover = {
            let mut service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.handover_current_image_replacement(source, role, &original, attempt, &token)?
        };
        {
            let service = loop {
                token.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.validate_current_image_replacement_handover(&handover, role.role(), &token)?;
            let mut sessions = self.sessions.lock();
            if sessions.current.contains_key(&scope.range_id)
                || sessions.installing.contains_key(&scope.range_id)
            {
                return Err(invalid(
                    "replacement handover cannot steal another runtime owner",
                ));
            }
            let s = sessions
                .replacing
                .remove(&scope.range_id)
                .ok_or_else(|| invalid("replacement session disappeared after durable handover"))?;
            if !s.scope.same_session(scope)
                || s.frozen.as_ref().is_none_or(|f| !Arc::ptr_eq(f, &original))
            {
                sessions.replacing.insert(scope.range_id, s);
                return Err(invalid("replacement freeze changed after durable handover"));
            }
            sessions.current.insert(
                scope.range_id,
                Session {
                    scope: scope.clone(),
                    frozen: s.frozen,
                    image_preflight: None,
                    native_prepared: None,
                },
            );
        }
        // No normal operation was enabled: durable intent and ActiveRecovery stayed
        // present throughout. Existing owned transfer now validates the new SAME
        // actual store/session/birth, and rotates N2 -> attempt under its normal proof.
        drop(original);
        drop(operation);
        self.transfer_image_install(
            scope,
            source,
            role,
            crate::distribution_range_consensus::ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &token,
        )
    }
}

#[cfg(test)]
impl RangeRecoveryRuntime {
    pub(crate) fn test_replacement_handover_cut(
        &self,
        scope: &RecoveryFenceScope,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &crate::distribution_range_consensus::NativeImageInstallRole<'_>,
        attempt: Uuid,
        before_catalogue: bool,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::InstallingRefusalHandoverReceipt> {
        let token = parent.child_with_deadline(role.role().deadline());
        token.check()?;
        let _operation = self.reserve(scope.range_id)?;
        scope.require_role(role.role())?;
        let frozen = {
            let sessions = self.sessions.lock();
            let s = sessions
                .replacing
                .get(&scope.range_id)
                .ok_or_else(|| invalid("actual test replacement session missing"))?;
            if !s.scope.same_session(scope) || source.cut().resume_proposal() != Some(&s.pointer) {
                return Err(invalid("test replacement cut changed actual scope"));
            }
            Arc::clone(
                s.frozen
                    .as_ref()
                    .ok_or_else(|| invalid("test replacement freeze leased"))?,
            )
        };
        let mut service = loop {
            token.check()?;
            if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                break s;
            }
        };
        if before_catalogue {
            service
                .test_replacement_proposal_before_handover(source, role, &frozen, attempt, &token)
        } else {
            service.handover_current_image_replacement(source, role, &frozen, attempt, &token)
        }
    }
}
