//! Projection requires the actual retained physical proof AND durable completed
//! catalogue/native-WAL binding. It cannot create readiness or release a fence.
use super::*;
use crate::distribution_range_recovery::{
    FrozenNativeReadyStage, NativeCompletionReply, NativeCompletionRequest,
    RecoveryObservationRoleAdmission, RecoveryRoleAdmission,
};
#[derive(Debug)]
pub(crate) struct LocalNativeCompletionEvidence {
    reply: NativeCompletionReply,
}
impl LocalNativeCompletionEvidence {
    pub(crate) fn into_reply(self) -> NativeCompletionReply {
        self.reply
    }
}
impl RangeWriteStore {
    pub(crate) fn complete_and_observe_native_recovery_frozen(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: crate::distribution_range_recovery::ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        self.produce_and_observe_completion(
            db,
            prepared,
            super::native_ready_completion::NativeCompletionDecision::Closed(quorum),
            fresh,
            request,
            parent,
        )
    }
    pub(crate) fn adopt_and_observe_native_completion_decision(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: crate::distribution_range_recovery::NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        self.produce_and_observe_completion(
            db,
            prepared,
            super::native_ready_completion::NativeCompletionDecision::Completed(quorum),
            fresh,
            request,
            parent,
        )
    }
    fn produce_and_observe_completion(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        decision: super::native_ready_completion::NativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        self.produce_completion_and_project_ready(
            db,
            prepared,
            decision,
            fresh,
            &request.ready_request(),
            parent,
            |store, ready, receipt| store.project_new_completion_evidence(ready, prepared, receipt),
        )
    }
    #[cfg(test)]
    pub(crate) fn test_complete_and_observe_native_recovery_frozen_with(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: crate::distribution_range_recovery::ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<LocalNativeCompletionEvidence> {
        self.test_produce_completion_and_project_ready_with(
            db,
            prepared,
            super::native_ready_completion::NativeCompletionDecision::Closed(quorum),
            fresh,
            &request.ready_request(),
            parent,
            write,
            |store, ready, receipt| store.project_new_completion_evidence(ready, prepared, receipt),
        )
    }
    fn project_new_completion_evidence(
        &self,
        ready: super::native_ready_observation::LocalNativeReadyEvidence,
        prepared: &PreparedNativeRecoveryReady,
        receipt: &super::native_ready_completion::NativeRecoveryCompletedFrozenReceipt,
    ) -> Result<LocalNativeCompletionEvidence> {
        let evidence = self.project_native_completion(ready.into_reply(), prepared)?;
        if evidence.reply.native_serving_sha256 != receipt.native_serving_sha256() {
            return Err(range_write_error(
                "completion projection differs from actual native producer receipt",
            ));
        }
        Ok(evidence)
    }
    pub(crate) fn observe_native_completion_for_role(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        role: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
    ) -> Result<LocalNativeCompletionEvidence> {
        request.scope.require_role(role)?;
        self.refuse_unpersisted_completion(&request.scope)?;
        let ready = self
            .observe_prepared_native_for_role(db, prepared, role, &request.ready_request())?
            .into_reply();
        self.project_native_completion(ready, prepared)
    }
    pub(crate) fn observe_native_completion_for_observer_scoped(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeCompletionRequest,
        slot: &crate::distribution_data::RangeWriteServiceSlotGuard,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        observer.require_scope(&request.scope)?;
        self.refuse_unpersisted_completion(&request.scope)?;
        let ready = self
            .observe_prepared_native_for_observer_scoped(
                db,
                prepared,
                observer,
                &request.ready_request(),
                slot,
                parent,
            )?
            .into_reply();
        self.project_native_completion(ready, prepared)
    }
    pub(crate) fn observe_native_completion_for_observer(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeCompletionRequest,
    ) -> Result<LocalNativeCompletionEvidence> {
        observer.require_scope(&request.scope)?;
        self.refuse_unpersisted_completion(&request.scope)?;
        let ready = self
            .observe_prepared_native_for_observer(db, prepared, observer, &request.ready_request())?
            .into_reply();
        self.project_native_completion(ready, prepared)
    }
    /// Pure refusal before deep physical validation. An actual uncompleted
    /// catalogue phase cannot produce completion evidence, absence, readiness
    /// or a quorum vote. Completed replies still take the full observer path.
    fn refuse_unpersisted_completion(&self, scope: &RecoveryFenceScope) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        self.require_native_phase_format(scope.range_id)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("completion observation lost its actual refusal"))?;
        if !saved.scope.same_session(scope) {
            return Err(range_write_error(
                "completion observation has another actual saved session",
            ));
        }
        if matches!(
            saved.phase,
            recovery_state::RecoveryFencePhase::NativePrepared(_)
        ) {
            return Err(range_write_error(
                "native completion has not actually persisted",
            ));
        }
        Ok(())
    }
    pub(crate) fn observe_activated_completion_for_observer(
        &self,
        db: &crate::BicDb,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeCompletionRequest,
    ) -> Result<LocalNativeCompletionEvidence> {
        observer.require_scope(&request.scope)?;
        self.require_durable_recovery_catalog()?;
        self.require_native_phase_format(request.scope.range_id)?;
        self.require_no_image_install(request.scope.range_id)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&request.scope.range_id)
            .ok_or_else(|| range_write_error("no actual activated completion record"))?;
        if !saved.scope.same_session(&request.scope)
            || saved.local_node_id != *observer.local_node_id()
            || saved.local_member_incarnation != observer.local_member_incarnation()
            || self.catalog.node_id != *observer.local_node_id()
            || &self.catalog.cluster_id != observer.cluster_id()
            || db.data_path().canonicalize()? != self.root.canonicalize()?
        {
            return Err(range_write_error(
                "activated anchor actual store/member/scope mismatch",
            ));
        }
        let recovery_state::RecoveryFencePhase::NativeActivated(activated) = &saved.phase else {
            return Err(range_write_error("no durable activated decision"));
        };
        activated.validate()?;
        let completed = &activated.completed;
        let point = &completed.point;
        let native = db.native_recovery_admission(
            observer.cluster_id(),
            observer.bootstrap_id(),
            observer.local_node_id(),
            observer.local_member_incarnation(),
        )?;
        observer.require_native_identity(&native)?;
        let receipt = db.observe_native_activated_completion_anchor(
            observer,
            &request.scope,
            &completed.native_serving_sha256,
            point.range_index(),
            point.logical_genesis_sha256(),
            point.logical_history_sha256(),
        )?;
        let activation = db.observe_native_activation_decision_for_observer(
            observer,
            &request.scope,
            &activated.native_activation_sha256,
            &completed.native_serving_sha256,
            point.range_index(),
            point.logical_genesis_sha256(),
            point.logical_history_sha256(),
        )?;
        if activation.completion_quorum_sha256() != activated.completion_quorum_sha256 {
            return Err(range_write_error(
                "actual protected activation/cataloque commitments disagree",
            ));
        }
        if receipt.native_receipt_at_activation_sha256() != point.native_receipt_sha256()
            || receipt.local_visibility_at_activation() != point.local_visibility_sequence()
        {
            return Err(range_write_error(
                "activated anchor differs from actual native completion receipt",
            ));
        }
        Ok(LocalNativeCompletionEvidence {
            reply: NativeCompletionReply {
                stage: crate::distribution_range_recovery::NativeCompletionStage::ActivatedAnchor,
                evidence_version: 1,
                scope: request.scope.clone(),
                challenge: request.challenge,
                observed_metadata_context_id: observer.metadata_context_id(),
                observed_metadata_term: observer.metadata_term(),
                observed_metadata_read_index: observer.metadata_read_index(),
                node_id: self.catalog.node_id.clone(),
                member_incarnation: observer.local_member_incarnation(),
                store_open_id: self.open_id,
                native_database_incarnation: native.database_incarnation().to_owned(),
                range_index: point.range_index(),
                logical_genesis_sha256: point.logical_genesis_sha256().to_owned(),
                logical_history_sha256: point.logical_history_sha256().to_owned(),
                native_receipt_sha256: point.native_receipt_sha256().to_owned(),
                local_visibility_sequence_at_completion: point.local_visibility_sequence(),
                native_serving_sha256: completed.native_serving_sha256.clone(),
                activation_decision_sha256: Some(activation.checksum_sha256().to_owned()),
            },
        })
    }
    fn project_native_completion(
        &self,
        ready: crate::distribution_range_recovery::NativeReadyReply,
        prepared: &PreparedNativeRecoveryReady,
    ) -> Result<LocalNativeCompletionEvidence> {
        if ready.stage != FrozenNativeReadyStage::NativeCompletedFrozen {
            return Err(range_write_error(
                "native completion has not actually persisted",
            ));
        }
        let record = self
            .catalog
            .recovery_fences
            .get(&ready.scope.range_id)
            .ok_or_else(|| range_write_error("native completion has no actual saved refusal"))?;
        let recovery_state::RecoveryFencePhase::NativeCompleted(completed) = &record.phase else {
            return Err(range_write_error(
                "native completion projection requires a frozen completed phase",
            ));
        };
        completed.validate()?;
        if completed.point != prepared.actual_prepared_point() {
            return Err(range_write_error(
                "native completion projection changed its actual pinned point",
            ));
        }
        Ok(LocalNativeCompletionEvidence {
            reply: NativeCompletionReply::from_frozen_projection(
                ready,
                completed.native_serving_sha256.clone(),
            )?,
        })
    }
}
