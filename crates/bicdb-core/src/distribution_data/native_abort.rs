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
        database.native_abort_mark_ever_commit(command, range, authority)?;
        let state = self.range_writes.commit(command)?;
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
            .finish_native_prepare_only_rejection(database, rejected, cleaned)
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
    pub(crate) fn observe_native_abort_candidate(
        &self,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
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
        let result = self.range_writes.promise_actual_native_abort(
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
