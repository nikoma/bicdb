//! Bounded raw projection from an ACTUAL owned native preparation. This module
//! cannot create a physical proof, change a catalogue floor or open admission.
use super::*;
use crate::distribution_range_recovery::{
    FrozenNativeReadyStage, NativeReadyPhysicalDomain, NativeReadyReply, NativeReadyRequest,
    RecoveryObservationRoleAdmission, RecoveryRoleAdmission,
};

#[derive(Debug)]
pub(crate) struct LocalNativeReadyEvidence {
    reply: NativeReadyReply,
}
impl LocalNativeReadyEvidence {
    pub(crate) fn into_reply(self) -> NativeReadyReply {
        self.reply
    }
}
impl RangeWriteStore {
    // Module-private producer continuation. It accepts only real opaque quorum
    // decisions, performs their actual persistence/refresh, then immediately
    // projects under the SAME exclusive Store/actual DB/Frozen/read-pin borrow.
    // No callback or "already checked" capability is exposed outside consensus.
    pub(super) fn produce_completion_and_project_ready<T>(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        decision: super::native_ready_completion::NativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
        project: impl FnOnce(
            &Self,
            LocalNativeReadyEvidence,
            &super::native_ready_completion::NativeRecoveryCompletedFrozenReceipt,
        ) -> Result<T>,
    ) -> Result<T> {
        parent.check()?;
        request.scope.require_role(fresh)?;
        let receipt = self.persist_native_completion_decision(db, prepared, decision, fresh)?;
        self.project_just_produced_completion(
            db, prepared, fresh, request, parent, receipt, project,
        )
    }
    #[cfg(test)]
    pub(super) fn test_produce_completion_and_project_ready_with<T>(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        decision: super::native_ready_completion::NativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
        project: impl FnOnce(
            &Self,
            LocalNativeReadyEvidence,
            &super::native_ready_completion::NativeRecoveryCompletedFrozenReceipt,
        ) -> Result<T>,
    ) -> Result<T> {
        parent.check()?;
        request.scope.require_role(fresh)?;
        let receipt = self
            .test_persist_native_completion_decision_with(db, prepared, decision, fresh, write)?;
        self.project_just_produced_completion(
            db, prepared, fresh, request, parent, receipt, project,
        )
    }
    // Strictly private: both call sites above have just run the actual durable
    // producer under this uninterrupted exclusive borrow. No stored receipt
    // can select this path from another module or request.
    fn project_just_produced_completion<T>(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        fresh: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
        receipt: super::native_ready_completion::NativeRecoveryCompletedFrozenReceipt,
        project: impl FnOnce(
            &Self,
            LocalNativeReadyEvidence,
            &super::native_ready_completion::NativeRecoveryCompletedFrozenReceipt,
        ) -> Result<T>,
    ) -> Result<T> {
        parent.check()?;
        #[cfg(test)]
        let mut projection_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-complete-produced-response-projection",
            fresh.local_node_id(),
            parent,
        );
        let ready = self.project_prepared_native(
            prepared,
            request,
            fresh.context_id(),
            fresh.metadata_term(),
            fresh.metadata_read_index(),
        )?;
        let result = project(self, ready, &receipt)?;
        prepared.require_produced_completion_boundary(db, self, fresh)?;
        fresh.require_live_request()?;
        parent.check()?;
        #[cfg(test)]
        projection_trace.complete();
        Ok(result)
    }
    /// Produce and project in one exclusive store/same-DB borrow. The producer
    /// owns the actual Frozen/read pin and performs its full physical checks
    /// before and after durable preparation. No caller-supplied Prepared proof
    /// or reusable "already checked" token can enter this path.
    pub(crate) fn prepare_and_observe_native_recovery_ready(
        &mut self,
        db: &crate::BicDb,
        physical: crate::db::LocalNativeRecoveryReady,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<(PreparedNativeRecoveryReady, LocalNativeReadyEvidence)> {
        parent.check()?;
        request.scope.require_role(physical.role())?;
        let prepared = self.prepare_native_recovery_ready(db, physical)?;
        // This is immediately after the producer's full post-fsync validation,
        // within the SAME exclusive catalogue borrow and owned physical pin.
        parent.check()?;
        let role = prepared.role();
        role.require_live_request()?;
        #[cfg(test)]
        let mut projection_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-prepare-produced-response-projection",
            role.local_node_id(),
            parent,
        );
        let evidence = self.project_prepared_native(
            &prepared,
            request,
            role.context_id(),
            role.metadata_term(),
            role.metadata_read_index(),
        )?;
        // Necessary refusal at the return boundary; this is not a renewed
        // physical/current-role grant or a cache surviving the local call.
        if matches!(
            role.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) && self
            .selected_current_joint_scope(request.scope.range_id)?
            .is_none_or(|saved| !saved.same_session(&request.scope))
        {
            return Err(range_write_error(
                "produced ready response changed its original current-joint writer/session",
            ));
        }
        role.require_live_request()?;
        parent.check()?;
        #[cfg(test)]
        projection_trace.complete();
        Ok((prepared, evidence))
    }
    /// Only preflight for retaining a restrictive freeze. This deliberately
    /// accepts an uncertain exact Prepared refusal so its actual fsync can be
    /// retried; it grants no observation, floor or mutation authority.
    pub(crate) fn preflight_native_ready_freeze(
        &self,
        role: &RecoveryRoleAdmission,
        native: &crate::db::NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<()> {
        scope.require_role(role)?;
        self.require_no_image_install(scope.range_id)?;
        self.require_native_recovery_identity(role, native)?;
        let record = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| {
                range_write_error("native preparation requires an existing actual recovery fence")
            })?;
        if !record.scope.same_session(scope)
            || record.native_database_incarnation != native.database_incarnation()
            || record.local_node_id != *role.local_node_id()
            || record.local_member_incarnation != role.local_member_incarnation()
            || !matches!(
                record.phase,
                recovery_state::RecoveryFencePhase::Active
                    | recovery_state::RecoveryFencePhase::NativePrepared(_)
                    | recovery_state::RecoveryFencePhase::NativeCompleted(_)
            )
        {
            return Err(range_write_error(
                "native preparation cannot freeze an activated or different session/open",
            ));
        }
        Ok(())
    }
    pub(crate) fn observe_prepared_native_for_role(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        role: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
    ) -> Result<LocalNativeReadyEvidence> {
        prepared.require_current_under_role(db, self, role)?;
        request.scope.require_role(role)?;
        self.project_prepared_native(
            prepared,
            request,
            role.context_id(),
            role.metadata_term(),
            role.metadata_read_index(),
        )
    }
    pub(crate) fn observe_prepared_native_for_observer(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeReadyRequest,
    ) -> Result<LocalNativeReadyEvidence> {
        prepared.require_current_for_observer(db, self, observer, &request.scope)?;
        self.project_prepared_native(
            prepared,
            request,
            observer.metadata_context_id(),
            observer.metadata_term(),
            observer.metadata_read_index(),
        )
    }
    fn project_prepared_native(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        request: &NativeReadyRequest,
        observed_metadata_context_id: Uuid,
        observed_metadata_term: u64,
        observed_metadata_read_index: u64,
    ) -> Result<LocalNativeReadyEvidence> {
        if observed_metadata_context_id.is_nil()
            || observed_metadata_term < request.scope.metadata_term
            || observed_metadata_read_index < request.scope.metadata_read_index
            || request.challenge.is_nil()
            || !request.scope.same_session(prepared.scope())
            || !matches!(
                request.scope.purpose,
                crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
                    | crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
            )
        {
            return Err(range_write_error(
                "native-ready projection differs from exact prepared session",
            ));
        }
        let record = self
            .catalog
            .recovery_fences
            .get(&request.scope.range_id)
            .ok_or_else(|| range_write_error("native-ready projection has no actual record"))?;
        let log = self
            .catalog
            .ranges
            .get(&request.scope.range_id)
            .ok_or_else(|| {
                range_write_error("native-ready projection has no actual resolved log")
            })?;
        if !matches!(
            record.phase,
            recovery_state::RecoveryFencePhase::NativePrepared(_)
                | recovery_state::RecoveryFencePhase::NativeCompleted(_)
        ) || log.last_index != prepared.range_index()
            || log.resolved_through != prepared.range_index()
            || log
                .entries
                .values()
                .any(|e| !matches!(e.state, RangeWriteState::Applied | RangeWriteState::Aborted))
        {
            return Err(range_write_error(
                "native-ready projection is not an irreversible closed preparation",
            ));
        }
        let mut obligations = BTreeMap::new();
        for entry in log
            .entries
            .values()
            .filter(|e| e.state == RangeWriteState::Applied)
        {
            obligations.insert(entry.command.index, entry.command.checksum_sha256.clone());
        }
        for (index, marker) in &record.historical_certifications {
            if *index > prepared.range_index()
                || obligations
                    .insert(*index, marker.command_sha256.clone())
                    .is_some_and(|old| old != marker.command_sha256)
            {
                return Err(range_write_error(
                    "native-prepared irreversible obligations conflict",
                ));
            }
        }
        for (index, checksum) in self.sparse_irreversible_commands(prepared.scope().range_id)? {
            if index > prepared.range_index()
                || obligations
                    .insert(index, checksum.clone())
                    .is_some_and(|old| old != checksum)
            {
                return Err(range_write_error(
                    "native-prepared sparse committed history conflicts or is uncovered",
                ));
            }
        }
        if obligations.len() > 2048 {
            return Err(range_write_error(
                "native-ready obligation projection exceeds bounded retention",
            ));
        }
        Ok(LocalNativeReadyEvidence {
            reply: NativeReadyReply {
                stage: prepared.frozen_stage()?,
                evidence_version: 1,
                physical_domain: NativeReadyPhysicalDomain::for_scope(&request.scope)?,
                scope: request.scope.clone(),
                challenge: request.challenge,
                observed_metadata_context_id,
                observed_metadata_term,
                observed_metadata_read_index,
                node_id: self.catalog.node_id.clone(),
                member_incarnation: prepared.role().local_member_incarnation(),
                store_open_id: self.open_id,
                native_database_incarnation: prepared.native_database_incarnation().to_owned(),
                range_index: prepared.range_index(),
                logical_genesis_sha256: prepared.logical_genesis_sha256().to_owned(),
                logical_history_sha256: prepared.logical_history_sha256().to_owned(),
                native_receipt_sha256: prepared.native_receipt_sha256().to_owned(),
                local_visibility_sequence: prepared.local_visibility_sequence(),
                progress: self.progress(request.scope.range_id),
                irreversible_command_sha256: obligations,
            },
        })
    }
}
