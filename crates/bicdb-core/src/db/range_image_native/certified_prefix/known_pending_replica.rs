//! A materialized lower bound while the NEXT committed origin is Pending.
//! This object cannot be a source image, current serving receipt, or Ready.
//! It authorizes no mutation: suffix replay still needs actual recipient CQ.
use super::*;
use crate::distribution_range_consensus::{RangeWriteCommand, RangeWriteStore};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

#[derive(Debug)]
pub(crate) struct KnownPendingNativeReplicaPrefix {
    database_open: String,
    store_open: Uuid,
    lineage: SourceRangeImageLineage,
    pending: PendingNativeRangeCommand,
    command: RangeWriteCommand,
}
impl KnownPendingNativeReplicaPrefix {
    pub(crate) fn range_index(&self) -> u64 {
        self.lineage.range_index()
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.lineage.restore_activation()
    }
    pub(crate) fn validate_plan(&self, plan: &NativeRangeCommandIntent) -> Result<()> {
        plan.validate_native_configuration(&self.lineage)
    }
    pub(crate) fn require_materialized(
        &self,
        db: &BicDb,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        command.validate()?;
        if command.cluster_id != *self.lineage.cluster_id()
            || command.range_id != self.lineage.base_range().id
            || command.index > self.range_index()
        {
            return Err(invalid(
                "KnownPending suffix exceeds actual materialized lower bound",
            ));
        }
        let entry = db.read_native_history_entry(
            command.range_id,
            self.lineage.history_generation(),
            command.index,
        )?;
        if entry.outcome
            != (history::NativeLogicalHistoryOutcomeV1::Applied {
                command_checksum_sha256: command.checksum_sha256.clone(),
            })
        {
            return Err(invalid(
                "KnownPending suffix lacks immutable Applied history",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_remote(
        &self,
        db: &BicDb,
        remote: &crate::distribution_native_apply::NativeReplicaProgressReply,
    ) -> Result<()> {
        let source = &self.lineage;
        if source.bootstrap_id() != remote.bootstrap_id
            || source.base_range().id != remote.range_id
            || remote.native_index > self.command.index
            || remote.logical_genesis_sha256 != source.logical_genesis_sha256()
            || !source
                .base_members()
                .get(&remote.node_id)
                .is_some_and(|m| m.incarnation == remote.member_incarnation)
        {
            return Err(invalid(
                "KnownPending remote differs from actual original native domain",
            ));
        }
        let expected_history = if remote.native_index == self.command.index {
            // The only allowed remote tail is the exact actual retained next
            // committed command. Compute its canonical ancestry from actual
            // immutable local H(R), then compare the authenticated physical
            // remote H(R+1). This is no local materialization/serving claim.
            history::NativeLogicalHistoryEntryV1::applied(
                remote.range_id,
                self.command.index,
                source.logical_history_sha256(),
                &self.command.checksum_sha256,
            )?
            .commitment_sha256
        } else {
            db.read_native_history_entry(
                remote.range_id,
                source.history_generation(),
                remote.native_index,
            )?
            .commitment_sha256
        };
        if expected_history != remote.logical_history_sha256 {
            return Err(invalid(
                "KnownPending remote is not an actual canonical ancestor",
            ));
        }
        Ok(())
    }
    pub(in crate::db::range_image_native) fn lineage_for_historical_activation(
        &self,
    ) -> &SourceRangeImageLineage {
        // Private to the native DB producer; never exported as SourceLineage.
        &self.lineage
    }
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        let _journal = db.try_native_image_journal()?;
        let actual = db
            .observe_known_pending_native_replica_prefix_under_journal(
                store,
                self.lineage.native_write_range()?,
            )?
            .ok_or_else(|| invalid("KnownPending origin obligation disappeared"))?;
        if self.database_open != actual.database_open
            || self.store_open != actual.store_open
            || self.pending != actual.pending
            || self.command != actual.command
            || self.range_index() != actual.range_index()
            || self.lineage.native_receipt_sha256() != actual.lineage.native_receipt_sha256()
            || self.lineage.logical_genesis_sha256() != actual.lineage.logical_genesis_sha256()
            || self.lineage.logical_history_sha256() != actual.lineage.logical_history_sha256()
        {
            return Err(invalid("KnownPending actual open/prefix/pending changed"));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn observe_known_pending_native_replica_prefix_for_role(
        &self,
        store: &RangeWriteStore,
        role: &RecoveryRoleAdmission,
    ) -> Result<Option<KnownPendingNativeReplicaPrefix>> {
        role.require_live_request()?;
        if !matches!(
            role.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
        ) {
            return Err(invalid(
                "KnownPending replica continuation requires NormalRecovery",
            ));
        }
        let _journal = self.try_native_image_journal()?;
        let known = self.observe_known_pending_native_replica_prefix_under_journal(
            store,
            role.current_range(),
        )?;
        if let Some(known) = &known {
            let actual = &known.lineage;
            let native = self.native_recovery_admission(
                role.cluster_id(),
                role.bootstrap_id(),
                role.local_node_id(),
                role.local_member_incarnation(),
            )?;
            native.require_current(self)?;
            if actual.cluster_id() != role.cluster_id()
                || actual.bootstrap_id() != role.bootstrap_id()
                || actual.local_node_id() != role.local_node_id()
                || actual.local_member_incarnation() != role.local_member_incarnation()
                || actual.restore_activation() != role.restore_activation()
                || role.expected_schema_sha256() != Some(actual.schema_sha256())
            {
                return Err(invalid(
                    "KnownPending actual native birth differs from fresh role",
                ));
            }
            for slot in self
                .native_abort_slot_inventory()?
                .iter()
                .filter(|s| s.command.range_id == role.current_range().id)
            {
                slot.domain.require_recovery_source(actual, role)?;
            }
        }
        role.require_live_request()?;
        Ok(known)
    }
    /// Caller holds the actual native journal. Never returns a serving grant.
    pub(in crate::db::range_image_native) fn observe_known_pending_native_replica_prefix_under_journal(
        &self,
        store: &RangeWriteStore,
        range: &RangeDescriptor,
    ) -> Result<Option<KnownPendingNativeReplicaPrefix>> {
        self.check_recovery_state()?;
        let Some(pending) = self
            .read_native_range_image_record::<PendingNativeRangeCommand>(&pending_key(range.id))?
        else {
            return Ok(None);
        };
        self.require_native_writer_catalogue_custody(store)?;
        store.require_native_catalog_durability(self.data_path())?;
        store.require_native_writer_host_range_id(range.id)?;
        pending.validate()?;
        let lineage = self.native_range_source_lineage_contract(range.id)?;
        let probe = store.probe(range.id, range.epoch, pending.range_index)?;
        let entry = probe
            .entry
            .ok_or_else(|| invalid("KnownPending has no retained actual command"))?;
        let command = entry.command;
        command.validate()?;
        if crate::distribution_native_read::canonical_range(lineage.native_write_range()?)
            != crate::distribution_native_read::canonical_range(range)
            || probe.progress.node_id != *lineage.local_node_id()
            || probe.progress.current_epoch != range.epoch
            || probe.progress.last_index != pending.range_index
            || !matches!(
                entry.state,
                crate::RangeWriteState::QuorumCommitted | crate::RangeWriteState::Applied
            )
            || command.range_id != range.id
            || command.range_epoch != range.epoch
            || command.cluster_id != *lineage.cluster_id()
            || command.leader_node_id != range.leader
            || pending.birth_id != lineage.birth.birth_id
            || pending.range_id != range.id
            || pending.previous_index != lineage.range_index()
            || pending.previous_receipt_sha256 != lineage.native_receipt_sha256()
            || pending.range_index != command.index
            || pending.command_id != command.command_id
            || pending.command_checksum_sha256 != command.checksum_sha256
            || pending.mutations_sha256
                != crate::db::admission_mutations_sha256(&command.mutations)?
            || pending.expected_local_transaction_id != command.transaction_id
            || self.native_range_image_generations(&lineage.birth, range.id)?
                != (
                    pending.global_image_generation,
                    pending.range_image_generation,
                )
            || pending.materialized.is_some()
            || pending.recovery_attempt.is_some()
        {
            return Err(invalid(
                "KnownPending requires exact next unmaterialized same-epoch committed origin",
            ));
        }
        NativeRangeCommandIntent::require_retained_materialized_prefix_origin_scope(
            &command,
            &lineage,
            &pending.scope_sha256,
        )?;
        if self
            .native_range_command_scopes
            .try_lock()
            .ok_or_else(|| invalid("KnownPending original scope registry busy"))?
            .entries
            .contains_key(&pending.expected_local_transaction_id)
        {
            return Err(invalid(
                "KnownPending original local transaction still owns its scope",
            ));
        }
        let known = KnownPendingNativeReplicaPrefix {
            database_open: self.incarnation.clone(),
            store_open: store.native_writer_store_open_id(),
            lineage,
            pending,
            command,
        };
        store.require_known_pending_replica_controls(self, &known, &known.command)?;
        Ok(Some(known))
    }
}
