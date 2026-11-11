//! Actual native ordinary replica row/WAL materialization. Authenticated leader
//! instructions are not manufactured local quorum certificates.
use super::*;
use crate::distribution_native_apply::NativeLeaderInstructionAdmission;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

#[derive(Debug)]
pub(crate) struct NativeReplicaMaterialization {
    native: NativeRecoveryAdmission,
    range: RangeDescriptor,
    index: u64,
    receipt_sha256: String,
    genesis: String,
    history: String,
    command_index: Option<u64>,
    command_sha256: Option<String>,
}
impl NativeReplicaMaterialization {
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.native.bootstrap_id()
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.native.local_node_id()
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.native.local_member_incarnation()
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn native_index(&self) -> u64 {
        self.index
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        &self.receipt_sha256
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.genesis
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.history
    }
    pub(crate) fn command_index(&self) -> Option<u64> {
        self.command_index
    }
    pub(crate) fn command_checksum_sha256(&self) -> Option<&str> {
        self.command_sha256.as_deref()
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.native.require_current(db)?;
        let actual = db.current_native_range_source_lineage(self.range.id)?;
        if actual.range_index() != self.index
            || actual.native_receipt_sha256() != self.receipt_sha256
            || actual.logical_genesis_sha256() != self.genesis
            || actual.logical_history_sha256() != self.history
        {
            return Err(invalid(
                "native replica materialization observation changed",
            ));
        }
        Ok(())
    }
}
fn same_native_range(a: &RangeDescriptor, b: &RangeDescriptor) -> bool {
    let canonical = |r: &RangeDescriptor| {
        let mut r = r.clone();
        r.approximate_bytes = 0;
        r.approximate_qps = 0;
        r.replicas.sort_by_key(|replica| replica.id);
        r
    };
    canonical(a) == canonical(b)
}
impl BicDb {
    pub(crate) fn require_native_replica_prefix_compatible(
        &self,
        source: &SourceRangeImageLineage,
        remote: &crate::distribution_native_apply::NativeReplicaProgressReply,
    ) -> Result<()> {
        self.validate_source_range_image_lineage(
            source,
            source.base_range().id,
            source.range_index(),
        )?;
        if source.bootstrap_id() != remote.bootstrap_id
            || source.base_range().id != remote.range_id
            || remote.native_index > source.range_index()
            || remote.logical_genesis_sha256 != source.logical_genesis_sha256()
            || !source
                .base_members()
                .get(&remote.node_id)
                .is_some_and(|m| m.incarnation == remote.member_incarnation)
        {
            return Err(invalid(
                "native replica progress differs from actual source domain",
            ));
        }
        let entry = self.read_native_history_entry(
            remote.range_id,
            source.history_generation(),
            remote.native_index,
        )?;
        if entry.commitment_sha256 != remote.logical_history_sha256 {
            return Err(invalid(
                "native replica immutable history is incompatible with source",
            ));
        }
        Ok(())
    }
    /// The native history leaf, not raw Applied/row equality, proves coverage.
    pub(crate) fn require_native_command_materialized(
        &self,
        lineage: &SourceRangeImageLineage,
        command: &crate::RangeWriteCommand,
    ) -> Result<()> {
        self.validate_source_range_image_lineage(lineage, command.range_id, lineage.range_index())?;
        command.validate()?;
        if command.cluster_id != *lineage.cluster_id()
            || command.range_id != lineage.base_range().id
            || command.index > lineage.range_index()
        {
            return Err(invalid(
                "native command is not covered by actual materialization",
            ));
        }
        let entry = self.read_native_history_entry(
            command.range_id,
            lineage.history_generation(),
            command.index,
        )?;
        if entry.outcome
            != (history::NativeLogicalHistoryOutcomeV1::Applied {
                command_checksum_sha256: command.checksum_sha256.clone(),
            })
        {
            return Err(invalid(
                "native immutable history disagrees with the original command",
            ));
        }
        Ok(())
    }
    fn native_replica_materialization(
        &self,
        actual: &SourceRangeImageLineage,
        advertised: &RangeDescriptor,
        command: Option<&crate::RangeWriteCommand>,
    ) -> Result<NativeReplicaMaterialization> {
        if let Some(command) = command {
            self.require_native_command_materialized(actual, command)?;
        }
        let native = self.native_recovery_admission(
            actual.cluster_id(),
            actual.bootstrap_id(),
            actual.local_node_id(),
            actual.local_member_incarnation(),
        )?;
        let result = NativeReplicaMaterialization {
            native,
            range: advertised.clone(),
            index: actual.range_index(),
            receipt_sha256: actual.native_receipt_sha256().into(),
            genesis: actual.logical_genesis_sha256().into(),
            history: actual.logical_history_sha256().into(),
            command_index: command.map(|c| c.index),
            command_sha256: command.map(|c| c.checksum_sha256.clone()),
        };
        result.require_current(self)?;
        Ok(result)
    }
    #[cfg(test)]
    pub(crate) fn observe_native_embedded_replica(
        &self,
        range: &RangeDescriptor,
        command: Option<&crate::RangeWriteCommand>,
    ) -> Result<NativeReplicaMaterialization> {
        self.schema_compatibility_fingerprint()?;
        let actual = self.current_native_range_source_lineage(range.id)?;
        if !same_native_range(actual.native_write_range()?, range) {
            return Err(invalid("embedded ordinary status configuration differs"));
        }
        self.native_replica_materialization(&actual, range, command)
    }
    pub(crate) fn observe_native_replica_for_role(
        &self,
        role: &RecoveryRoleAdmission,
    ) -> Result<NativeReplicaMaterialization> {
        role.require_live_request()?;
        let fingerprint = self.schema_compatibility_fingerprint()?;
        let actual = self.current_native_range_source_lineage(role.current_range().id)?;
        let mut predecessor = role.current_range().clone();
        if let Some(plan) = role.committed_relocation() {
            if plan.learner_epoch == predecessor.epoch {
                if plan.expected_epoch.checked_add(1) != Some(plan.learner_epoch)
                    || predecessor
                        .replicas
                        .iter()
                        .filter(|r| {
                            r.id == plan.target_replica_id
                                && r.node_id == plan.target
                                && r.role == RangeReplicaRole::Learner
                        })
                        .count()
                        != 1
                {
                    return Err(invalid(
                        "native ordinary status has no exact learner predecessor",
                    ));
                }
                predecessor.epoch = plan.expected_epoch;
                predecessor
                    .replicas
                    .retain(|r| r.id != plan.target_replica_id);
            }
        }
        if actual.cluster_id() != role.cluster_id()
            || actual.bootstrap_id() != role.bootstrap_id()
            || actual.local_node_id() != role.local_node_id()
            || actual.local_member_incarnation() != role.local_member_incarnation()
            || actual.restore_activation() != role.restore_activation()
            || role.expected_schema_sha256() != Some(fingerprint.sha256.as_str())
            || fingerprint.sha256 != actual.schema_sha256()
            || (!same_native_range(actual.native_write_range()?, role.current_range())
                && !same_native_range(actual.native_write_range()?, &predecessor))
            || actual.base_members().len() != role.member_credentials().len()
            || role.member_credentials().iter().any(|(id, m)| {
                actual.base_members().get(id).is_none_or(|a| {
                    a.incarnation != m.incarnation
                        || a.tls_certificate_sha256 != m.active_certificate_sha256
                        || a.pending_tls_certificate_sha256 != m.pending_certificate_sha256
                })
            })
        {
            return Err(invalid(
                "native ordinary status needs actual current same-voter/configuration proof",
            ));
        }
        let result = self.native_replica_materialization(&actual, role.current_range(), None)?;
        role.require_live_request()?;
        Ok(result)
    }
    pub(crate) fn apply_native_origin_instruction(
        &self,
        admission: &NativeLeaderInstructionAdmission,
        authority: &RangeMutationAuthority<'_>,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaMaterialization> {
        admission.require_live()?;
        parent.check()?;
        let intent = admission.intent();
        let command = admission.command();
        self.range_mutations.validate(command.range_id, authority)?;
        self.schema_compatibility_fingerprint()?;
        self.require_normal_native_range_mutation_allowed(command.range_id)?;
        let actual = self.current_native_range_source_lineage(command.range_id)?;
        intent.validate_native_configuration(&actual)?;
        if command.index <= actual.range_index() {
            return self.native_replica_materialization(&actual, intent.range(), Some(command));
        }
        intent.validate_native_base(&actual)?;
        let mut transaction = self.begin_transaction()?;
        let result = (|| {
            let pending =
                self.prepare_native_range_command(intent, transaction.id().0, authority)?;
            transaction.bypass_commit_admission = true;
            transaction.imported_projection = true;
            transaction.range_mutation_claim = Some(authority.claim());
            transaction.set_snapshot_isolation(false, 0)?;
            for mutation in &command.mutations {
                parent.check()?;
                admission.require_live()?;
                let reserved = transaction.read_committed_update_record_nowait(
                    &mutation.collection,
                    &mutation.record_id,
                )?;
                let snapshot = reserved.map_or(0, |(_, seq)| seq);
                match &mutation.record {
                    Some(record) => transaction.write_upserts_with_statement_snapshots(
                        &mutation.collection,
                        [(record.clone(), snapshot)],
                    )?,
                    None => transaction.delete_many_with_statement_snapshots(
                        &mutation.collection,
                        [(&mutation.record_id, snapshot)],
                    )?,
                }
            }
            parent.check()?;
            admission.require_live()?;
            let sequence = self.commit_transaction(&mut transaction)?;
            self.tx_log.write_durable(sequence)?;
            let receipt = transaction.mint_native_wal_apply_receipt()?;
            self.record_native_range_materialized(&pending, &receipt, authority)?;
            transaction.finalize_commit_admission()?;
            transaction.finalize_committed_broker_outbox();
            transaction.finalize_committed_memory_jobs()?;
            self.complete_native_range_command_with_evidence(
                &pending,
                NativeCompletionMaterializationEvidence::Leader(admission),
                &receipt,
                authority,
            )?;
            parent.check()?;
            admission.require_live()?;
            let actual = self.current_native_range_source_lineage(command.range_id)?;
            self.native_replica_materialization(&actual, intent.range(), Some(command))
        })();
        match result {
            Ok(proof) => {
                self.auto_checkpoint_if_due();
                Ok(proof)
            }
            Err(error) => {
                self.finish_failed_native_local_attempt(&mut transaction);
                Err(BicDbError::CommitOutcomeUnknown { sequence: transaction.committed_seq,
                    transaction_id: Some(transaction.id().0), incarnation: self.incarnation.clone(),
                    detail: format!("native replica command {} is irrevocable; materialization requires recovery: {error}", command.command_id) })
            }
        }
    }
}
