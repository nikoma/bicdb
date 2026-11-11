//! Current-certificate-quorum historical materialization. A known immutable
//! prefix remains a lower bound when pending rows exist; it is NOT a source
//! image or serving authority. Original command identity and current recovery
//! authority are deliberately separate domains.
use super::*;
use crate::distribution_range_consensus::{RangeWriteStore, TargetImageCatalogHistoryObligations};
use crate::distribution_range_mutation::{
    ControlledFrozenMutationPermit, FrozenRangeMutationPermit,
};
use crate::distribution_range_recovery::{RecoveryApplyAdmission, RecoveryRoleAdmission};

/// Actual local immutable prefix + exact pending observation, bound to current
/// native DB/store opens. No serde/Clone/numeric constructor. In particular this
/// cannot be passed to SourceCapture or replace NativeRecoveryReady.
#[derive(Debug)]
pub(crate) struct KnownNativeRecoveryPrefix {
    database_incarnation: String,
    lineage: SourceRangeImageLineage,
    pending: Option<PendingNativeRangeCommand>,
    catalog: TargetImageCatalogHistoryObligations,
}
impl KnownNativeRecoveryPrefix {
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        self.lineage.cluster_id()
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.lineage.bootstrap_id()
    }
    pub(crate) fn base_range(&self) -> &RangeDescriptor {
        self.lineage.base_range()
    }
    pub(crate) fn base_members(&self) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        self.lineage.base_members()
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        self.lineage.schema_sha256()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.lineage.range_index()
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.lineage.local_node_id()
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.lineage.local_member_incarnation()
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.lineage.restore_activation()
    }
}

#[allow(clippy::too_many_arguments)]
fn require_current_historical_apply(
    database: &BicDb,
    store: &RangeWriteStore,
    native: &NativeRecoveryAdmission,
    role: &RecoveryRoleAdmission,
    admission: &RecoveryApplyAdmission,
    frozen: &FrozenRangeMutationPermit,
    controlled: &ControlledFrozenMutationPermit,
) -> Result<()> {
    admission.require_live_request()?;
    admission.require_recipient_role(role)?;
    native.require_current(database)?;
    store.require_recovery_scope(role, native, admission.scope())?;
    let command = admission.command();
    role.require_historical_command_scope(command)?;
    if native.cluster_id() != admission.cluster_id()
        || native.bootstrap_id() != admission.bootstrap_id()
        || native.local_node_id() != admission.local_node_id()
        || native.local_member_incarnation() != admission.local_member_incarnation()
        || native.restore_activation() != admission.restore_activation()
        || frozen.owner() != admission.scope().freeze_owner()?
    {
        return Err(invalid(
            "historical native admission/birth/owned freeze differ",
        ));
    }
    database
        .range_mutations
        .validate_frozen(command.range_id, frozen)?;
    database
        .range_mutations
        .validate_controlled(command.range_id, controlled)?;
    let marker = store
        .historical_certification_observation(admission.scope(), command.index)?
        .ok_or_else(|| {
            invalid("historical native Apply lacks ACTUAL local current-session certification")
        })?;
    if marker.recovery_nonce != admission.recovery_nonce()
        || marker.configuration_sha256 != admission.scope().configuration_sha256
        || marker.range_epoch != admission.scope().range_epoch
        || marker.index != command.index
        || marker.command_sha256 != command.checksum_sha256
    {
        return Err(invalid(
            "historical native local certify marker differs from admission",
        ));
    }
    let probe = store.probe(
        command.range_id,
        admission.current_range().epoch,
        command.index,
    )?;
    if let Some(entry) = probe.entry {
        if entry.command != *command
            || !matches!(
                entry.state,
                crate::RangeWriteState::QuorumCommitted | crate::RangeWriteState::Applied
            )
        {
            return Err(invalid(
                "historical native actual local command/state differs",
            ));
        }
    } else if command.index > probe.progress.compacted_through {
        return Err(invalid("historical native actual local command is missing"));
    }
    Ok(())
}

impl BicDb {
    /// No network, drain or global sequence wait occurs under these borrowed
    /// guards. Runtime acquires/drains the same-session freeze beforehand.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_known_native_recovery_prefix(
        &self,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &RecoveryApplyAdmission,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<KnownNativeRecoveryPrefix> {
        require_current_historical_apply(self, store, native, role, admission, frozen, controlled)?;
        let _journal = self.try_native_image_journal()?;
        // This private contract producer checks actual metadata/history/UIDs,
        // but intentionally retains pending for reconciliation. Its result is
        // never returned as a public SourceLineage while pending exists.
        let lineage = self.native_range_source_lineage_contract(admission.command().range_id)?;
        let pending: Option<PendingNativeRangeCommand> =
            self.read_native_range_image_record(&pending_key(admission.command().range_id))?;
        if let Some(pending) = &pending {
            pending.validate()?;
            let command = admission.command();
            if pending.birth_id != lineage.birth.birth_id
                || pending.range_id != command.range_id
                || pending.command_id != command.command_id
                || pending.command_checksum_sha256 != command.checksum_sha256
                || pending.mutations_sha256
                    != crate::db::admission_mutations_sha256(&command.mutations)?
                || pending.range_index != command.index
                || pending.previous_index != lineage.range_index()
                || pending.previous_receipt_sha256 != lineage.native_receipt_sha256()
            {
                return Err(invalid(
                    "historical native pending contradicts actual selected command/prefix",
                ));
            }
            if self
                .native_range_command_scopes
                .try_lock()
                .ok_or_else(|| invalid("historical native scope registry busy"))?
                .entries
                .contains_key(&pending.expected_local_transaction_id)
            {
                return Err(invalid(
                    "historical native previous local attempt still owns its scope",
                ));
            }
        }
        let command = admission.command();
        if command.index
            > lineage
                .range_index()
                .checked_add(1)
                .ok_or_else(|| invalid("historical native prefix index overflow"))?
        {
            return Err(invalid(
                "historical native prefix gap requires certified image repair",
            ));
        }
        if command.index <= lineage.range_index() {
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
                    "historical native completed history contradicts selected command",
                ));
            }
        }
        let catalog = store.image_install_catalog_history_obligations(command.range_id)?;
        require_current_historical_apply(self, store, native, role, admission, frozen, controlled)?;
        Ok(KnownNativeRecoveryPrefix {
            database_incarnation: self.incarnation.clone(),
            lineage,
            pending,
            catalog,
        })
    }
}
