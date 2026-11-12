//! Same-open activated state is a necessary fact, not current authority. Only
//! the fresh enrolled readQQ + distinct final MQ producer may use this fact.
use super::*;
use crate::distribution_consensus::{MetadataReadObservation, MetadataRecoveryRolePurpose};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use recovery_state::RecoveryFencePhase;

pub(crate) struct NativeHotRecoveredOpen {
    store_open: Uuid,
    database_open: String,
    scope: RecoveryFenceScope,
    range: RangeDescriptor,
    command: RangeWriteCommand,
    decision: crate::db::NativeActivationDecisionReceipt,
    point: (u64, String, String),
    min_metadata: (u64, u64),
    cancellation: CancellationToken,
}
impl NativeHotRecoveredOpen {
    pub(crate) fn require_metadata(
        &self,
        metadata: &MetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        metadata.require_live_request(business)?;
        if metadata.term() < self.min_metadata.0 || metadata.read_index() < self.min_metadata.1 {
            return Err(range_write_error(
                "hot recovery final metadata precedes actual activation check",
            ));
        }
        Ok(())
    }
    pub(crate) fn matches_point(&self, point: (u64, &str, &str)) -> bool {
        point.0 == self.point.0 && point.1 == self.point.1 && point.2 == self.point.2
    }
    pub(crate) fn require_current(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        range: &RangeDescriptor,
    ) -> Result<()> {
        self.cancellation.check()?;
        if self.database_open != db.incarnation_id()
            || self.store_open != store.open_id
            || crate::distribution_native_read::canonical_range(range) != self.range
        {
            return Err(range_write_error(
                "hot recovered-open fact changed actual DB/Store/range",
            ));
        }
        store.require_actual_native_activated_open(range.id)?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&range.id)
            .ok_or_else(|| range_write_error("hot recovered-open actual refusal disappeared"))?;
        let RecoveryFencePhase::NativeActivated(a) = &saved.phase else {
            return Err(range_write_error(
                "hot recovery has no actual local activated state",
            ));
        };
        if !saved.scope.same_session(&self.scope)
            || a.native_activation_sha256 != self.decision.checksum_sha256()
            || a.completed.point.range_index() != self.point.0
            || a.completed.point.logical_genesis_sha256() != self.point.1
            || a.completed.point.logical_history_sha256() != self.point.2
        {
            return Err(range_write_error(
                "hot recovered-open exact activation changed",
            ));
        }
        self.decision.require_current(db)?;
        let lineage = db.current_native_range_source_lineage(range.id)?;
        db.require_native_command_materialized(&lineage, &self.command)?;
        store.require_actual_abort_controls_covered(db, range.id, lineage.range_index())?;
        if lineage.range_index() != self.point.0
            || lineage.logical_genesis_sha256() != self.point.1
            || lineage.logical_history_sha256() != self.point.2
        {
            return Err(range_write_error(
                "hot recovered-open current native history changed",
            ));
        }
        self.cancellation.check()
    }
}
impl RangeWriteStore {
    pub(crate) fn observe_hot_recovered_open(
        &self,
        db: &crate::BicDb,
        fresh: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<NativeHotRecoveredOpen> {
        parent.check()?;
        fresh.require_live_request()?;
        scope.require_role(fresh)?;
        if !matches!(fresh.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || command.range_id != scope.range_id
            || command.range_epoch != scope.range_epoch
            || command.leader_node_id != *fresh.local_node_id()
        {
            return Err(range_write_error(
                "hot repair only supports actual original same-epoch NormalRecovery leader",
            ));
        }
        self.require_actual_native_activated_open(scope.range_id)?;
        let native = db.native_recovery_admission(
            fresh.cluster_id(),
            fresh.bootstrap_id(),
            fresh.local_node_id(),
            fresh.local_member_incarnation(),
        )?;
        self.require_native_recovery_identity(fresh, &native)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("hot repair actual activation absent"))?;
        let RecoveryFencePhase::NativeActivated(a) = &saved.phase else {
            return Err(range_write_error("hot repair is not activated"));
        };
        if !saved.scope.same_session(scope) {
            return Err(range_write_error(
                "hot repair actual activation belongs to another session",
            ));
        }
        let p = &a.completed.point;
        let decision = db.observe_native_activation_decision_for_role(
            fresh,
            scope,
            &a.native_activation_sha256,
            &a.completed.native_serving_sha256,
            p.range_index(),
            p.logical_genesis_sha256(),
            p.logical_history_sha256(),
        )?;
        let fact = NativeHotRecoveredOpen {
            store_open: self.open_id,
            database_open: db.incarnation_id().to_owned(),
            scope: scope.clone(),
            range: crate::distribution_native_read::canonical_range(fresh.current_range()),
            command: command.clone(),
            decision,
            point: (
                p.range_index(),
                p.logical_genesis_sha256().to_owned(),
                p.logical_history_sha256().to_owned(),
            ),
            min_metadata: (fresh.metadata_term(), fresh.metadata_read_index()),
            cancellation: parent.child_with_deadline(fresh.deadline()),
        };
        if fact.point.0 != command.index {
            return Err(range_write_error(
                "hot recovered-open point differs from selected committed command",
            ));
        }
        fact.require_current(db, self, fresh.current_range())?;
        fresh.require_live_request()?;
        Ok(fact)
    }
}
