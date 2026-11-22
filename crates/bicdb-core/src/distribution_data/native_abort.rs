//! Local abort-origin bookkeeping uses the SAME actual service-owned Store.
use super::*;
use crate::distribution_range_consensus::NativePrepareOnlyRejection;
impl ClusterDataNodeService {
    pub(crate) fn begin_native_abort_attempt(
        &mut self,
        database: &BicDb,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<NativePrepareOnlyRejection> {
        self.range_mutations.validate(range.id, authority)?;
        self.require_native_catalog_database(database)?;
        self.require_native_writer_host_for_range(database, range)?;
        let source = database.current_native_range_source_lineage(range.id)?;
        let native = database.native_recovery_admission(
            &self.cluster_id,
            source.bootstrap_id(),
            &self.node_id,
            source.local_member_incarnation(),
        )?;
        self.range_writes
            .current_mut()
            .start_native_abort_attempt(database, &native, range, command, authority)
    }
    pub(crate) fn commit_native_range_write_with_context(
        &mut self,
        database: &BicDb,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<RangeWriteAck> {
        self.range_mutations.validate(range.id, authority)?;
        self.require_native_catalog_database(database)?;
        self.require_native_writer_host_for_range(database, range)?;
        self.validate_range_write(range, caller, command)?;
        self.range_writes
            .preflight_actual_native_commit_entry(command)?;
        database.native_abort_mark_ever_commit(command, range, authority)?;
        #[cfg(test)]
        if command.leader_node_id == self.node_id
            && crate::distribution_range_consensus::native_abort::test_take_native_admission_stop(
                range.id,
                crate::distribution_range_consensus::native_abort::TestNativeAdmissionStop::AfterEverCommitMark,
            )
        {
            return Err(data_error(
                "injected admission stop AfterEverCommitMark: raw Commit vote not persisted",
            ));
        }
        let state = self.range_writes.current_mut().commit(command)?;
        self.range_write_ack(command, state)
    }
    pub(crate) fn finish_native_prepare_only_rejection(
        &mut self,
        database: &BicDb,
        rejected: NativePrepareOnlyRejection,
        cleaned: &crate::db::NativeTransactionNoEffect,
    ) -> Result<()> {
        self.require_native_catalog_database(database)?;
        self.range_writes
            .current_mut()
            .finish_native_prepare_only_rejection(database, rejected, cleaned)
    }
    /// REOPENED origin only: the candidate checksum for this node's own
    /// Prepared-only slot left by an earlier process. See
    /// `RangeWriteStore::reopened_native_abort_candidate`.
    pub(crate) fn reopened_native_abort_candidate(
        &mut self,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
    ) -> Result<String> {
        let database = Arc::clone(&self.db);
        let db = try_database_read(&database)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, range)?;
        // An existing intent needs no new write (and no lease: a recovery
        // fence freezes the range's mutation gate).
        if let Some(existing) = self
            .range_writes
            .current_mut()
            .reopened_native_abort_slot(&db, range, command)?
        {
            return Ok(existing);
        }
        // A counted mutation lease, as every slot write carries. Admissions
        // are excluded by the caller's range reservation and the unrecovered
        // epoch, not by this lease.
        let mutation = self.range_mutations.try_enter(range.id)?;
        let result = self
            .range_writes
            .current_mut()
            .reopened_native_abort_candidate(
                &db,
                range,
                command,
                &RangeMutationAuthority::Mutation(&mutation),
            );
        drop(mutation);
        result
    }
    /// REOPENED origin only: re-prove that this node's own slot
    /// `command.index` was left by an EARLIER open inside its local Commit:
    /// the protected slot is ever-Commit for exactly this command (the
    /// irrevocable commit choice of the coordinator; it can no longer be
    /// aborted), with no no-effect intent and no abort promise, and the raw
    /// entry is still exactly this Prepared command as the only unresolved
    /// index. Read-only.
    pub(crate) fn require_reopened_native_commit_slot(
        &self,
        db: &BicDb,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        self.require_native_catalog_database(db)?;
        self.require_native_writer_host_for_range(db, range)?;
        command.validate()?;
        if command.leader_node_id != self.node_id
            || command.range_id != range.id
            || command.range_epoch != range.epoch
        {
            return Err(data_error(
                "reopened Commit vote: only this node's own same-epoch command",
            ));
        }
        if db.native_abort_slot_started_this_open(range.id, command.index) {
            return Err(data_error(
                "reopened Commit vote: the slot was started by this open; only its own admission may decide it",
            ));
        }
        let ever_commit = db.native_abort_slot_inventory()?.into_iter().any(|s| {
            s.command == *command
                && s.ever_commit
                && s.no_effect_sha256.is_none()
                && s.abort_promise_sha256.is_none()
        });
        if !ever_commit {
            return Err(data_error(
                "reopened Commit vote: the protected slot is not ever-Commit for exactly this command",
            ));
        }
        let progress = self.range_write_progress(range.id);
        let raw = self
            .probe_range_write_for_peer(range, &self.node_id.clone(), command.index)?
            .entry;
        if progress.resolved_through.checked_add(1) != Some(command.index)
            || progress.last_index != command.index
            || !raw.is_some_and(|e| e.state == RangeWriteState::Prepared && e.command == *command)
        {
            return Err(data_error(
                "reopened Commit vote: the raw log does not hold exactly this Prepared command as its only unresolved entry",
            ));
        }
        Ok(())
    }
    /// REOPENED origin only: cast this node's own raw Commit vote for the
    /// slot proven by `require_reopened_native_commit_slot`. The vote goes
    /// through the unchanged local Commit path
    /// (`commit_native_range_write_with_context`), which re-proves the slot,
    /// the raw entry, the absence of any promise/no-effect intent and of an
    /// active recovery context, under a counted mutation lease.
    pub(crate) fn reopened_native_commit_vote(
        &mut self,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let database = Arc::clone(&self.db);
        let db = try_database_read(&database)?;
        self.require_reopened_native_commit_slot(&db, range, command)?;
        let gate = self.range_mutations.clone();
        let mutation = gate.try_enter(range.id)?;
        let node = self.node_id.clone();
        let result = self.commit_native_range_write_with_context(
            &db,
            range,
            &node,
            command,
            &RangeMutationAuthority::Mutation(&mutation),
        );
        drop(mutation);
        result
    }
    pub(super) fn ensure_native_abort_prepare_start(
        &mut self,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        if !self.range_writes.requires_native_abort_tracking()?
            || self.range_writes.has_native_abort_attempt(command)?
        {
            return Ok(());
        }
        let database = Arc::clone(&self.db);
        let borrowed = try_database_read(&database)?;
        self.begin_native_abort_attempt(&borrowed, range, command, authority)?;
        Ok(())
    }
}

impl ClusterDataNodeService {
    pub(crate) fn observe_existing_native_abort_promise(
        &self,
        role: &crate::distribution_range_recovery::RecoveryObservationRoleAdmission,
        request: &crate::distribution_range_recovery::NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::native_abort::LocalAbortPromise> {
        let db = try_database_read(&self.db)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            &self.node_id,
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .observe_existing_native_abort_promise(&db, &native, role, request, parent)
    }
    pub(crate) fn observe_native_abort_candidate(
        &self,
        role: &crate::distribution_range_recovery::RecoveryObservationRoleAdmission,
        request: &crate::distribution_range_recovery::NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::native_abort::LocalAbortCandidate> {
        let db = try_database_read(&self.db)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            &self.node_id,
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .observe_actual_abort_candidate(&db, &native, role, request, parent)
    }
    pub(crate) fn promise_native_abort(
        &mut self,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        request: &crate::distribution_range_recovery::NativeAbortRequest,
        candidate: &crate::distribution_range_recovery::NativeAbortCandidateObservation,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::native_abort::LocalAbortPromise> {
        let db = try_database_read(&self.db)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        self.range_mutations
            .validate_quiescent_frozen(request.scope.range_id, frozen)?;
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            &self.node_id,
            role.local_member_incarnation(),
        )?;
        let result = self.range_writes.current_mut().promise_actual_native_abort(
            &db,
            &native,
            role,
            request,
            candidate,
            &RangeMutationAuthority::Controlled(&controlled),
            parent,
        );
        drop(controlled);
        result
    }
}
impl ClusterDataNodeService {
    pub(crate) fn apply_native_abort_quorum(
        &mut self,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        quorum: &crate::distribution_range_recovery::NativeAbortQuorum,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeRecoveryAbortedReceipt> {
        let db = try_database_read(&self.db)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        self.range_mutations
            .validate_quiescent_frozen(role.current_range().id, frozen)?;
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            &self.node_id,
            role.local_member_incarnation(),
        )?;
        let receipt = db.apply_native_abort_quorum(
            &self.range_writes,
            &native,
            role,
            quorum,
            frozen,
            &controlled,
            parent,
        )?;
        #[cfg(test)]
        if std::mem::take(&mut self.test_fail_abort_after_native_publication) {
            return Err(data_error("injected recovery response loss after actual canonical Abort native WAL before raw mark"));
        }
        self.range_writes
            .current_mut()
            .mark_actual_native_abort_resolved(&db, &native, role, quorum, &receipt)?;
        parent.check()?;
        drop(controlled);
        Ok(receipt)
    }
}

#[cfg(test)]
impl ClusterDataNodeService {
    pub(crate) fn test_fail_abort_after_native_publication(&mut self) {
        self.test_fail_abort_after_native_publication = true;
    }
    pub(crate) fn test_refuse_native_prepare_after_start(&mut self, refuse: bool) {
        self.test_refuse_native_prepare_after_start = refuse;
    }
}
