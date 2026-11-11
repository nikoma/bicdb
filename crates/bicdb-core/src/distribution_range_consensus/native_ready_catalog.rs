//! Read-only bridge from the actual resolved catalogue to native history.
use super::*;
use crate::db::NativeRecoveryAdmission;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use crate::distribution_range_recovery::{
    RecoveryImageHistoryObligationsV1, RecoveryNativeHistoryPointV1, RecoveryNativePendingHistoryV1,
};

#[cfg(test)]
pub(crate) fn native_ready_test_command(
    cluster: ClusterId,
    range: &RangeDescriptor,
    index: u64,
    leader: ClusterNodeId,
    intent: &CommitAdmissionIntent,
) -> Result<RangeWriteCommand> {
    RangeWriteCommand::new(cluster, range, index, leader, intent)
}

impl RangeWriteStore {
    /// Called only after actual fresh role verification and persisted fencing.
    /// This method reads real native safety and logical records; caller-supplied
    /// watermarks, a serialized voter list, or an empty DTO cannot fill it in.
    pub(crate) fn observe_recovery_image_obligations(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<RecoveryImageHistoryObligationsV1> {
        self.require_persisted_recovery_scope(scope)?;
        native.require_current(db)?;
        let saved = &self.catalog.recovery_fences[&scope.range_id];
        if !self.fsync
            || self.root.canonicalize()? != db.data_path().canonicalize()?
            || native.cluster_id() != &self.catalog.cluster_id
            || native.cluster_id() != &scope.cluster_id
            || native.bootstrap_id() != scope.bootstrap_id
            || native.local_node_id() != &self.catalog.node_id
            || saved.native_database_incarnation != native.database_incarnation()
            || saved.local_member_incarnation != native.local_member_incarnation()
        {
            return Err(range_write_error(
                "fenced history observation has another actual native/store identity",
            ));
        }
        let safety = db.native_range_install_safety(native, scope.range_id, frozen)?;
        let log = self
            .catalog
            .ranges
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("fenced history range is absent"))?;
        let mut irreversible_command_sha256 = BTreeMap::new();
        for entry in log.entries.values().filter(|entry| {
            matches!(
                entry.state,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            )
        }) {
            irreversible_command_sha256
                .insert(entry.command.index, entry.command.checksum_sha256.clone());
        }
        for (index, marker) in &saved.historical_certifications {
            if irreversible_command_sha256
                .insert(*index, marker.command_sha256.clone())
                .is_some_and(|prior| prior != marker.command_sha256)
            {
                return Err(range_write_error(
                    "current certification conflicts with retained irreversible history",
                ));
            }
        }
        let native_point = safety
            .history_point()
            .map(|point| RecoveryNativeHistoryPointV1 {
                range_index: point.range_index(),
                logical_genesis_sha256: point.logical_genesis_sha256().to_string(),
                logical_history_sha256: point.logical_history_sha256().to_string(),
                native_receipt_sha256: point.native_receipt_sha256().to_string(),
            });
        let pending_native = match (
            safety.pending_index(),
            safety.pending_command_checksum_sha256(),
        ) {
            (Some(index), Some(checksum)) => Some(RecoveryNativePendingHistoryV1 {
                index,
                command_checksum_sha256: checksum.to_string(),
                materialized: safety.materialized_pending_index() == Some(index),
            }),
            (None, None) => None,
            _ => {
                return Err(range_write_error(
                    "actual native pending evidence is incomplete",
                ))
            }
        };
        let observed = RecoveryImageHistoryObligationsV1 {
            format_version: 1,
            node_id: self.catalog.node_id.clone(),
            range_id: scope.range_id,
            last_index: log.last_index,
            compacted_through: log.compacted_through,
            native_point,
            pending_native,
            irreversible_command_sha256,
        };
        observed.require_shape(scope, &self.progress(scope.range_id))?;
        self.require_persisted_recovery_scope(scope)?;
        Ok(observed)
    }
    /// The callback observes only actual retained resolved entries, while this
    /// store is immutably borrowed. A numeric resolved watermark is insufficient:
    /// every retained decision must agree with the corresponding native history.
    pub(crate) fn verify_native_ready_catalog(
        &self,
        observation: &RecoveryResolvedCatalogObservation,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        actual_database_path: &Path,
        mut verify: impl FnMut(&RangeWriteLogEntry) -> Result<()>,
    ) -> Result<()> {
        self.revalidate_recovery_resolved_observation(observation, role, native)?;
        if !self.fsync || self.root.canonicalize()? != actual_database_path.canonicalize()? {
            return Err(range_write_error(
                "native readiness catalogue belongs to another physical database",
            ));
        }
        let log = self
            .catalog
            .ranges
            .get(&observation.range_id())
            .ok_or_else(|| range_write_error("native readiness range is absent"))?;
        if log.entries.len() > RETAIN_APPLIED_COMMANDS_PER_RANGE as usize {
            return Err(range_write_error(
                "native readiness retained decision scan exceeds its bound",
            ));
        }
        for entry in log.entries.values() {
            if entry.command.index > observation.resolved_index()
                || !matches!(
                    entry.state,
                    RangeWriteState::Applied | RangeWriteState::Aborted
                )
            {
                return Err(range_write_error(
                    "native readiness contains an unresolved decision",
                ));
            }
            verify(entry)?;
        }
        self.revalidate_recovery_resolved_observation(observation, role, native)
    }
}
