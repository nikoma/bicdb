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
    pub(crate) fn observe_native_completion_for_role(
        &self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        role: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
    ) -> Result<LocalNativeCompletionEvidence> {
        let ready = self
            .observe_prepared_native_for_role(db, prepared, role, &request.ready_request())?
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
        let ready = self
            .observe_prepared_native_for_observer(db, prepared, observer, &request.ready_request())?
            .into_reply();
        self.project_native_completion(ready, prepared)
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
            reply: NativeCompletionReply {
                ready,
                native_serving_sha256: completed.native_serving_sha256.clone(),
            },
        })
    }
}
