//! Durable, unordered committed-decision storage. A sparse promise is NOT a
//! range-log entry, applied watermark, replica-ready fact or native permission.
//! Only a live independently selected historical committed decision may create
//! one; old serialized audit data can never create a new quorum.
use super::*;
use crate::distribution_range_recovery::{
    HistoricalCommandDecision, RecoveryMemberIdentity, RecoveryRoleAdmission,
};

pub(super) const SPARSE_HISTORY_CATALOG_FORMAT: u32 = 10;
const MAX_SPARSE_DECISIONS_PER_RANGE: usize = 1024;
const MAX_SPARSE_DECISIONS_TOTAL: usize = 65_536;
const MAX_SPARSE_CATALOG_BYTES: usize = 32 * 1024 * 1024;
const MAX_SPARSE_RECORD_BYTES: usize = MAX_NATIVE_RECOVERY_ENTRY_BYTES + 128 * 1024;

/// Persisted provenance ONLY. It is never deserialized into decision admission,
/// authenticated votes or a current certificate quorum.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SparseDecisionVoteAudit {
    pub(crate) member_incarnation: u64,
    pub(crate) observed_raw_state: Option<RangeWriteState>,
    pub(crate) prior_sparse_decision_sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SparseDecisionAudit {
    pub(crate) scope: RecoveryFenceScope,
    pub(crate) members: BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    pub(crate) observations: BTreeMap<ClusterNodeId, SparseDecisionVoteAudit>,
}
impl SparseDecisionAudit {
    fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        let digest =
            |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
        if self.members.is_empty()
            || self.members.len() > 64
            || self.observations.len() < self.members.len() / 2 + 1
            || self.observations.len() > self.members.len()
        {
            return Err(range_write_error(
                "damaged sparse decision audit membership",
            ));
        }
        let mut irreversible = false;
        let mut committed = 0;
        for (id, vote) in &self.observations {
            let member = self
                .members
                .get(id)
                .ok_or_else(|| range_write_error("sparse decision audit has a foreign voter"))?;
            if member.incarnation == 0
                || vote.member_incarnation != member.incarnation
                || vote.observed_raw_state == Some(RangeWriteState::Aborted)
                || vote
                    .prior_sparse_decision_sha256
                    .as_deref()
                    .is_some_and(|sha| !digest(sha))
            {
                return Err(range_write_error("damaged sparse decision audit vote"));
            }
            irreversible |= matches!(
                vote.observed_raw_state,
                Some(RangeWriteState::QuorumCommitted | RangeWriteState::Applied)
            ) || vote.prior_sparse_decision_sha256.is_some();
            committed += usize::from(vote.observed_raw_state == Some(RangeWriteState::Committed));
        }
        if !irreversible && committed < self.members.len() / 2 + 1 {
            return Err(range_write_error(
                "sparse audit does not describe a committed decision",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct SparseHistoricalDecision {
    format_version: u32,
    local_node_id: ClusterNodeId,
    local_member_incarnation: u64,
    native_database_incarnation: String,
    native_birth_id: Uuid,
    native_birth_sha256: String,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    command: RangeWriteCommand,
    audit: SparseDecisionAudit,
    checksum_sha256: String,
}
impl SparseHistoricalDecision {
    pub(super) fn command_checksum_sha256(&self) -> &str {
        &self.command.checksum_sha256
    }
    fn from_actual_decision(
        decision: &HistoricalCommandDecision,
        role: &RecoveryRoleAdmission,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<Self> {
        decision.require_recipient_role(role)?;
        let mut record = Self {
            format_version: 1,
            local_node_id: role.local_node_id().clone(),
            local_member_incarnation: role.local_member_incarnation(),
            native_database_incarnation: native.database_incarnation().to_owned(),
            native_birth_id: native.birth_id(),
            native_birth_sha256: native.birth_sha256().to_owned(),
            restore_activation: native.restore_activation().cloned(),
            command: decision.command().clone(),
            audit: decision.sparse_persistence_audit()?,
            checksum_sha256: String::new(),
        };
        record.checksum_sha256 = record.calculate_checksum()?;
        record.validate()?;
        Ok(record)
    }
    fn calculate_checksum(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        validate_native_json_size(self, MAX_SPARSE_RECORD_BYTES)?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            "bicdb-sparse-committed-history-v1",
            self.format_version,
            &self.local_node_id,
            self.local_member_incarnation,
            &self.native_database_incarnation,
            self.native_birth_id,
            &self.native_birth_sha256,
            &self.restore_activation,
            &self.command,
            &self.audit,
        ))?)))
    }
    fn validate(&self) -> Result<()> {
        validate_native_recovery_command_size(&self.command)?;
        self.command.validate()?;
        self.audit.validate()?;
        if self.format_version != 1
            || !matches!(
                self.audit.scope.purpose,
                crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.local_member_incarnation == 0
            || self.native_database_incarnation.is_empty()
            || self.native_database_incarnation.len() > 16 * 1024
            || self.native_birth_id.is_nil()
            || self.native_birth_sha256.len() != 64
            || !self
                .native_birth_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self
                .audit
                .members
                .get(&self.local_node_id)
                .is_none_or(|member| member.incarnation != self.local_member_incarnation)
            || self.command.cluster_id != self.audit.scope.cluster_id
            || self.command.range_id != self.audit.scope.range_id
            || self.command.range_epoch > self.audit.scope.range_epoch
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(range_write_error(
                "damaged sparse historical committed decision",
            ));
        }
        Ok(())
    }
}

/// Wire/persistent projection ONLY. Transport authentication and actual store
/// production are required before the enclosing historical observation exists.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SparseHistoricalDecisionProjection {
    record: SparseHistoricalDecision,
    observed_native_database_incarnation: String,
}
impl SparseHistoricalDecisionProjection {
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.record.local_node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.record.local_member_incarnation
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        &self.observed_native_database_incarnation
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        &self.record.command
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn validate_shape(&self, scope: &RecoveryFenceScope, index: u64) -> Result<()> {
        self.record.validate()?;
        if self.command().index != index
            || self.command().cluster_id != scope.cluster_id
            || self.record.audit.scope.bootstrap_id != scope.bootstrap_id
            || self.command().range_id != scope.range_id
            || self.command().range_epoch > scope.range_epoch
        {
            return Err(range_write_error(
                "sparse decision projection has another request domain",
            ));
        }
        Ok(())
    }
}

pub(super) type SparseDecisions = BTreeMap<RangeId, BTreeMap<u64, SparseHistoricalDecision>>;

pub(super) fn validate_sparse_catalog(catalog: &RangeWriteCatalog) -> Result<()> {
    if catalog.format_version < SPARSE_HISTORY_CATALOG_FORMAT {
        if catalog.sparse_committed_decisions.is_some() {
            return Err(range_write_error(
                "sparse decision field lacks its required reader fence",
            ));
        }
        return Ok(());
    }
    let values = catalog.sparse_committed_decisions.as_ref().ok_or_else(|| {
        range_write_error("sparse-capable catalog is missing its mandatory decision field")
    })?;
    validate_native_json_size(values, MAX_SPARSE_CATALOG_BYTES)?;
    let mut total = 0;
    for (range, decisions) in values {
        if decisions.len() > MAX_SPARSE_DECISIONS_PER_RANGE || !catalog.ranges.contains_key(range) {
            return Err(range_write_error(
                "sparse committed decision range/count is invalid",
            ));
        }
        total += decisions.len();
        for (index, decision) in decisions {
            decision.validate()?;
            if *index != decision.command.index
                || *range != decision.command.range_id
                || decision.command.cluster_id != catalog.cluster_id
                || decision.local_node_id != catalog.node_id
            {
                return Err(range_write_error(
                    "sparse committed decision key/domain mismatch",
                ));
            }
            if catalog.ranges[range]
                .entries
                .get(index)
                .is_some_and(|entry| {
                    entry.command != decision.command || entry.state == RangeWriteState::Aborted
                })
            {
                return Err(range_write_error(
                    "sparse committed decision conflicts with ordered history",
                ));
            }
        }
    }
    if total > MAX_SPARSE_DECISIONS_TOTAL {
        return Err(range_write_error(
            "sparse committed decision catalog exceeds count bound",
        ));
    }
    Ok(())
}

impl RangeWriteStore {
    /// Separate protocol operation: persist a previously selected COMMITTED
    /// command across an ordered-history gap. This does not relax contiguous
    /// certification, publish an ordered log entry, or apply any native row.
    pub(crate) fn certify_sparse_historical_command(
        &mut self,
        role: &RecoveryRoleAdmission,
        native: &crate::db::NativeRecoveryAdmission,
        decision: &HistoricalCommandDecision,
    ) -> Result<HistoricalRangeCertification> {
        self.certify_sparse_historical_command_with(
            role,
            native,
            decision,
            crate::storage::write_atomic,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_certify_sparse_historical_command_with(
        &mut self,
        role: &RecoveryRoleAdmission,
        native: &crate::db::NativeRecoveryAdmission,
        decision: &HistoricalCommandDecision,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<HistoricalRangeCertification> {
        self.certify_sparse_historical_command_with(role, native, decision, write)
    }
    fn certify_sparse_historical_command_with(
        &mut self,
        role: &RecoveryRoleAdmission,
        native: &crate::db::NativeRecoveryAdmission,
        decision: &HistoricalCommandDecision,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<HistoricalRangeCertification> {
        decision.require_recipient_role(role)?;
        if !matches!(
            role.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
        ) || !role.member_credentials().contains_key(role.local_node_id())
        {
            return Err(range_write_error(
                "sparse certification requires a current normal data voter",
            ));
        }
        let scope = decision.scope();
        self.require_recovery_scope(role, native, scope)?;
        let command = decision.command();
        role.require_historical_command_scope(command)?;
        if self
            .catalog
            .handoff_promises
            .get(&scope.range_id)
            .is_some_and(|promise| command.index > promise.resolved_through)
        {
            return Err(range_write_error(
                "sparse certification exceeds permanent handoff cut",
            ));
        }
        let log = self
            .catalog
            .ranges
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("sparse certification has no fenced range"))?;
        if log.entries.contains_key(&command.index)
            || (command.index > log.compacted_through
                && command.index == log.last_index.saturating_add(1))
        {
            return Err(range_write_error(
                "available ordered history requires normal certification",
            ));
        }
        let marker = HistoricalRangeCertification {
            recovery_nonce: scope.recovery_nonce,
            configuration_sha256: scope.configuration_sha256.clone(),
            range_epoch: scope.range_epoch,
            index: command.index,
            command_sha256: command.checksum_sha256.clone(),
        };
        let saved = &self.catalog.recovery_fences[&scope.range_id];
        if saved
            .historical_certifications
            .get(&command.index)
            .is_some_and(|old| old != &marker)
            || (!saved.historical_certifications.contains_key(&command.index)
                && saved.historical_certifications.len() >= 1024)
        {
            return Err(range_write_error(
                "sparse current marker conflicts or exceeds bound",
            ));
        }
        // Preflight the COMPLETE candidate before the first mutation. In
        // particular, oversized sparse payloads cannot poison durable status.
        let mut candidate = self.catalog.clone();
        let entries = candidate
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new)
            .entry(scope.range_id)
            .or_default();
        if let Some(old) = entries.get(&command.index) {
            old.validate()?;
            if old.command != *command {
                return Err(range_write_error(
                    "sparse committed decision cannot change command",
                ));
            }
            // Preserve original irreversible decision provenance on retries,
            // even when a later request has a fresh metadata context/nonce.
        } else {
            if entries.len() >= MAX_SPARSE_DECISIONS_PER_RANGE {
                return Err(range_write_error(
                    "sparse committed decision range bound reached",
                ));
            }
            entries.insert(
                command.index,
                SparseHistoricalDecision::from_actual_decision(decision, role, native)?,
            );
        }
        candidate
            .recovery_fences
            .get_mut(&scope.range_id)
            .unwrap()
            .historical_certifications
            .insert(command.index, marker.clone());
        candidate.format_version = candidate.format_version.max(SPARSE_HISTORY_CATALOG_FORMAT);
        validate_sparse_catalog(&candidate)?;
        // Full command, old committed provenance and the CURRENT marker become
        // durable in ONE catalogue publication. Retain all state/refusals on
        // either side of an ambiguous rename/fsync; never roll back a promise.
        self.recovery_persistence_uncertain = true;
        self.catalog = candidate;
        self.persist_recovery_catalog_with(write)?;
        Ok(marker)
    }
    pub(crate) fn sparse_historical_projection(
        &self,
        scope: &RecoveryFenceScope,
        index: u64,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<Option<SparseHistoricalDecisionProjection>> {
        self.require_persisted_recovery_scope(scope)?;
        validate_sparse_catalog(&self.catalog)?;
        let Some(record) = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .and_then(|ranges| ranges.get(&scope.range_id))
            .and_then(|entries| entries.get(&index))
        else {
            return Ok(None);
        };
        self.require_sparse_native_binding(scope.range_id, native)?;
        let value = SparseHistoricalDecisionProjection {
            record: record.clone(),
            observed_native_database_incarnation: native.database_incarnation().to_owned(),
        };
        value.validate_shape(scope, index)?;
        Ok(Some(value))
    }
    pub(crate) fn sparse_irreversible_commands(
        &self,
        range: RangeId,
    ) -> Result<BTreeMap<u64, String>> {
        validate_sparse_catalog(&self.catalog)?;
        Ok(self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .and_then(|ranges| ranges.get(&range))
            .into_iter()
            .flat_map(|entries| entries.iter())
            .map(|(index, record)| (*index, record.command.checksum_sha256.clone()))
            .collect())
    }
    pub(super) fn require_sparse_native_binding(
        &self,
        range: RangeId,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        if let Some(decisions) = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .and_then(|ranges| ranges.get(&range))
        {
            for record in decisions.values() {
                record.validate()?;
                if record.local_node_id != *native.local_node_id()
                    || record.local_member_incarnation != native.local_member_incarnation()
                    || record.native_birth_id != native.birth_id()
                    || record.native_birth_sha256 != native.birth_sha256()
                    || record.restore_activation.as_ref() != native.restore_activation()
                {
                    return Err(range_write_error(
                        "sparse history belongs to another actual native birth/restore",
                    ));
                }
            }
        }
        Ok(())
    }
    pub(super) fn require_no_uncovered_sparse(&self, range: RangeId, resolved: u64) -> Result<()> {
        if self
            .sparse_irreversible_commands(range)?
            .keys()
            .any(|index| *index > resolved)
        {
            return Err(range_write_error(
                "committed sparse history is not fully materialized",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn sparse_catalog_codec_requires_explicit_new_format_field_and_preserves_legacy_shape() {
        // Codec-only checks: no native birth, selected decision, sparse
        // promise, authenticated observation or recovery grant is constructed.
        let root = tempfile::tempdir().unwrap();
        let store = RangeWriteStore::open(
            root.path(),
            ClusterId::new("sparse-codec-only").unwrap(),
            ClusterNodeId::new("node").unwrap(),
            false,
        )
        .unwrap();
        let legacy = store.catalog.clone();
        assert!(!serde_json::to_value(&legacy)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("sparse_committed_decisions"));
        RangeWriteEnvelope::new(legacy.clone())
            .unwrap()
            .verify()
            .unwrap();
        let mut new = legacy.clone();
        new.format_version = SPARSE_HISTORY_CATALOG_FORMAT;
        new.sparse_committed_decisions = Some(BTreeMap::new());
        RangeWriteEnvelope::new(new.clone())
            .unwrap()
            .verify()
            .unwrap();
        let mut encoded = serde_json::to_value(&new).unwrap();
        encoded
            .as_object_mut()
            .unwrap()
            .remove("sparse_committed_decisions");
        let missing: RangeWriteCatalog = serde_json::from_value(encoded).unwrap();
        let error = RangeWriteEnvelope::new(missing)
            .unwrap()
            .verify()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("missing its mandatory decision field"));
        new.format_version = SPARSE_HISTORY_CATALOG_FORMAT - 1;
        let error = RangeWriteEnvelope::new(new).unwrap().verify().unwrap_err();
        assert!(error.to_string().contains("required reader fence"));
    }
}
