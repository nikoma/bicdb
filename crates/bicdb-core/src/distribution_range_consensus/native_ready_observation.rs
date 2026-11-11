//! Bounded raw projection from an ACTUAL owned native preparation. This module
//! cannot create a physical proof, change a catalogue floor or open admission.
use super::*;
use crate::distribution_range_recovery::{
    NativeReadyPhysicalDomain, NativeReadyReply, NativeReadyRequest,
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
    pub(crate) fn observe_prepared_native_for_role(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        role: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
    ) -> Result<LocalNativeReadyEvidence> {
        prepared.require_current_under_role(db, self, role)?;
        request.scope.require_role(role)?;
        self.project_prepared_native(prepared, request)
    }
    pub(crate) fn observe_prepared_native_for_observer(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeReadyRequest,
    ) -> Result<LocalNativeReadyEvidence> {
        prepared.require_current_for_observer(db, self, observer, &request.scope)?;
        self.project_prepared_native(prepared, request)
    }
    fn project_prepared_native(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        request: &NativeReadyRequest,
    ) -> Result<LocalNativeReadyEvidence> {
        if request.challenge.is_nil()
            || !request.scope.same_session(prepared.scope())
            || !matches!(
                request.scope.purpose,
                crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
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
        if obligations.len() > 2048 {
            return Err(range_write_error(
                "native-ready obligation projection exceeds bounded retention",
            ));
        }
        Ok(LocalNativeReadyEvidence {
            reply: NativeReadyReply {
                evidence_version: 1,
                physical_domain: NativeReadyPhysicalDomain::SameVoterNativeAnchorV1,
                scope: request.scope.clone(),
                challenge: request.challenge,
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
