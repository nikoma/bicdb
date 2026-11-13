//! Read-only bridge from the actual resolved catalogue to native history.
use super::*;
use crate::db::NativeRecoveryAdmission;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use crate::distribution_range_recovery::{
    RecoveryImageHistoryObligationsV1, RecoveryNativeHistoryPointV1, RecoveryNativePendingHistoryV1,
};

fn insert_complete_fenced_obligation(
    values: &mut BTreeMap<u64, String>,
    index: u64,
    checksum: &str,
) -> Result<()> {
    use crate::distribution_range_recovery::{
        fenced_obligation_limit_error, MAX_FENCED_IRREVERSIBLE_COMMANDS,
    };
    if let Some(previous) = values.get(&index) {
        if previous != checksum {
            return Err(range_write_error(
                "current certification conflicts with retained irreversible history",
            ));
        }
    } else {
        if values.len() >= MAX_FENCED_IRREVERSIBLE_COMMANDS {
            return Err(fenced_obligation_limit_error());
        }
        values.insert(index, checksum.to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod obligation_bound_tests {
    use super::*;
    use crate::distribution_range_recovery::MAX_FENCED_IRREVERSIBLE_COMMANDS;

    #[test]
    fn complete_fenced_obligation_producer_deduplicates_without_dropping_conflicts_or_overflow() {
        let mut values = BTreeMap::new();
        for index in 1..=MAX_FENCED_IRREVERSIBLE_COMMANDS as u64 {
            insert_complete_fenced_obligation(&mut values, index, &"a".repeat(64)).unwrap();
        }
        let before = values.clone();
        insert_complete_fenced_obligation(&mut values, 1, &"a".repeat(64)).unwrap();
        assert_eq!(values, before);
        let conflict =
            insert_complete_fenced_obligation(&mut values, 1, &"b".repeat(64)).unwrap_err();
        assert!(conflict.to_string().contains("conflicts"));
        assert_eq!(values, before);
        let overflow = insert_complete_fenced_obligation(
            &mut values,
            MAX_FENCED_IRREVERSIBLE_COMMANDS as u64 + 1,
            &"a".repeat(64),
        )
        .unwrap_err();
        assert!(overflow
            .to_string()
            .contains("complete 2049-command obligation reply bound"));
        assert_eq!(values, before, "no partial reply or mutated accepted map");
    }
}

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
        let result = self.observe_recovery_image_obligations_body(db, native, scope, frozen)?;
        self.require_persisted_recovery_scope(scope)?;

        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark("response-image-obligations-ok");
        // FENCE_TRACE_END
        Ok(result)
    }
    pub(super) fn observe_recovery_image_obligations_body(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<RecoveryImageHistoryObligationsV1> {

        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark("response-native-current-begin");
        // FENCE_TRACE_END
        native.require_current(db)?;
        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark("response-native-current-ok");
        // FENCE_TRACE_END

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

        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark(
            "response-native-install-safety-begin",
        );
        // FENCE_TRACE_END
        let safety = db.native_range_install_safety(native, scope.range_id, frozen)?;
        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark("response-native-install-safety-ok");
        // FENCE_TRACE_END

        let log = self
            .catalog
            .ranges
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("fenced history range is absent"))?;

        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark(
            "response-irreversible-inventory-begin",
        );
        // FENCE_TRACE_END
        let mut irreversible_command_sha256 = BTreeMap::new();
        for entry in log.entries.values().filter(|entry| {
            matches!(
                entry.state,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            )
        }) {
            insert_complete_fenced_obligation(
                &mut irreversible_command_sha256,
                entry.command.index,
                &entry.command.checksum_sha256,
            )?;
        }
        for (index, marker) in &saved.historical_certifications {
            insert_complete_fenced_obligation(
                &mut irreversible_command_sha256,
                *index,
                &marker.command_sha256,
            )?;
        }
        for (index, checksum) in self.archived_joint_certifications(scope.range_id)? {
            insert_complete_fenced_obligation(&mut irreversible_command_sha256, index, &checksum)?;
        }
        let sparse_commands = self.sparse_irreversible_commands(scope.range_id)?;
        for (index, checksum) in &sparse_commands {
            insert_complete_fenced_obligation(&mut irreversible_command_sha256, *index, checksum)?;
        }
        let sparse_committed_command_sha256 =
            (!sparse_commands.is_empty()).then_some(sparse_commands);
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
            format_version: if sparse_committed_command_sha256.is_some() {
                2
            } else {
                1
            },
            node_id: self.catalog.node_id.clone(),
            range_id: scope.range_id,
            last_index: log.last_index,
            compacted_through: log.compacted_through,
            native_point,
            pending_native,
            irreversible_command_sha256,
            sparse_committed_command_sha256,
        };

        // FENCE_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_range_recovery::fence_trace::mark("response-irreversible-inventory-ok");
        // FENCE_TRACE_END
        observed.require_shape(scope, &self.progress(scope.range_id))?;
        Ok(observed)
    }
    /// Compaction may remove a command payload while the durable current-
    /// session certification marker remains. Every such irreversible checksum
    /// must also match ACTUAL native history before a ready proof is minted.
    pub(crate) fn verify_native_ready_historical_markers(
        &self,
        observation: &RecoveryResolvedCatalogObservation,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        mut verify: impl FnMut(u64, &str) -> Result<()>,
    ) -> Result<()> {
        self.revalidate_recovery_resolved_observation(observation, role, native)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&observation.range_id())
            .ok_or_else(|| range_write_error("native marker readiness has no saved fence"))?;
        if saved.historical_certifications.len() > 1024 {
            return Err(range_write_error(
                "native marker readiness exceeds its bounded scan",
            ));
        }
        for (index, marker) in &saved.historical_certifications {
            if *index > observation.resolved_index() {
                return Err(range_write_error(
                    "native readiness omits an irreversible certification marker",
                ));
            }
            verify(*index, &marker.command_sha256)?;
        }
        for (index, checksum) in self.archived_joint_certifications(observation.range_id())? {
            if index > observation.resolved_index() {
                return Err(range_write_error(
                    "native readiness omits archived irreversible history",
                ));
            }
            verify(index, &checksum)?;
        }
        for (index, checksum) in self.sparse_irreversible_commands(observation.range_id())? {
            if index > observation.resolved_index() {
                return Err(range_write_error(
                    "native readiness omits permanent sparse committed history",
                ));
            }
            verify(index, &checksum)?;
        }
        self.revalidate_recovery_resolved_observation(observation, role, native)
    }
    #[cfg(test)]
    pub(crate) fn test_corrupt_compacted_marker(
        &mut self,
        scope: &RecoveryFenceScope,
        index: u64,
        checksum: String,
    ) -> Result<()> {
        // Deliberately corrupt durable logical metadata while leaving genuine
        // native rows/WAL/history intact. This is a NEGATIVE fault-injection
        // fixture, not a legal compaction/authority producer or Azure cause.
        let saved = self
            .catalog
            .recovery_fences
            .get_mut(&scope.range_id)
            .unwrap();
        saved.historical_certifications.insert(
            index,
            HistoricalRangeCertification {
                recovery_nonce: scope.recovery_nonce,
                configuration_sha256: scope.configuration_sha256.clone(),
                range_epoch: scope.range_epoch,
                index,
                command_sha256: checksum,
            },
        );
        let log = self.catalog.ranges.get_mut(&scope.range_id).unwrap();
        assert_eq!(log.resolved_through, index);
        assert_eq!(log.last_index, index);
        log.entries.retain(|position, _| *position > index);
        log.compacted_through = index;
        self.persist_recovery_catalog()
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
