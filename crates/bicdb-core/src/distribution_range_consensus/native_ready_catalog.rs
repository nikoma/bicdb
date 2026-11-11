//! Read-only bridge from the actual resolved catalogue to native history.
use super::*;
use crate::db::NativeRecoveryAdmission;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

impl RangeWriteStore {
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
