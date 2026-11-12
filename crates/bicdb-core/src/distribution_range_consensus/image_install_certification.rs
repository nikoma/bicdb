//! Certification of an EXISTING committed decision while a protected image is
//! Installing. This never prepares/commits/applies a command or advances a log.
//! Historical intent and ordinary recovery refusal stay intact.
use super::recovery_state::RecoveryFencePhase;
use super::*;
use crate::db::{NativeCurrentImageProposalReceipt, NativeRecoveryAdmission};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{
    HistoricalCurrentImageAcceptanceV1, RecoveryRoleAdmission,
};

/// Actual local producer result, not a deserialized evidence grant.
#[derive(Debug)]
pub(crate) struct LocalInstallingCommittedFence {
    evidence: crate::distribution_range_recovery::InstallingCommittedFenceEvidence,
}
impl LocalInstallingCommittedFence {
    pub(crate) fn into_evidence(
        self,
    ) -> crate::distribution_range_recovery::InstallingCommittedFenceEvidence {
        self.evidence
    }
}
#[derive(Debug)]
pub(crate) struct LocalInstallingCommittedCertification {
    evidence: crate::distribution_range_recovery::InstallingCommittedCertificationEvidence,
}
impl LocalInstallingCommittedCertification {
    pub(crate) fn into_evidence(
        self,
    ) -> crate::distribution_range_recovery::InstallingCommittedCertificationEvidence {
        self.evidence
    }
}

pub(super) const INSTALLING_CERTIFICATION_FORMAT: u32 = 13;
/// Shape-only upgrade of a validated candidate catalogue. This does not
/// change its format, writer mode, native birth, rows, floor or any proof.
pub(super) fn ensure_installing_certification_maps(catalog: &mut RangeWriteCatalog) {
    catalog
        .installing_committed_certifications
        .get_or_insert_with(BTreeMap::new);
    catalog
        .installing_recovery_handovers
        .get_or_insert_with(BTreeMap::new);
}

const MAX_RECORDS_PER_RANGE: usize = 128;
const MAX_RECORD_BYTES: usize = MAX_NATIVE_RECOVERY_ENTRY_BYTES + 256 * 1024;
const MAX_CATALOG_BYTES: usize = 32 * 1024 * 1024;

fn canonical(mut r: RangeDescriptor) -> RangeDescriptor {
    r.approximate_bytes = 0;
    r.approximate_qps = 0;
    r.replicas.sort_by_key(|p| p.id);
    r
}
/// Only the leader and data epoch may change. Changed membership/pins, span,
/// schema, restore, placement or bootstrap needs another protocol, not this one.
pub(crate) fn require_installing_same_voter_transition(
    historical: &HistoricalCurrentImageAcceptanceV1,
    role: &RecoveryRoleAdmission,
) -> Result<()> {
    role.require_live_request()?;
    historical.validate()?;
    let mut current = canonical(role.current_range().clone());
    let old = canonical(historical.range.clone());
    if current.epoch < old.epoch
        || (current.epoch == old.epoch && current.leader != old.leader)
        || !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
        || role.cluster_id() != &historical.scope.cluster_id
        || role.bootstrap_id() != historical.scope.bootstrap_id
        || role.member_credentials() != &historical.members
        || role.restore_activation() != historical.restore_activation.as_ref()
        || role.expected_schema_sha256() != Some(historical.schema_sha256.as_str())
        || role.metadata_term() < historical.metadata_term
        || role.metadata_read_index() < historical.metadata_read_index
    {
        return Err(range_write_error(
            "Installing transition changes protected identity/domain or regresses metadata",
        ));
    }
    current.epoch = old.epoch;
    current.leader = old.leader.clone();
    if current != old {
        return Err(range_write_error(
            "Installing transition needs unchanged same-voter physical/configuration domain",
        ));
    }
    Ok(())
}

/// Actual protected target admission under an independently fresh CURRENT role.
/// Its saved intent/old recovery scope are historical refusals, never replaced.
#[derive(Debug)]
pub(crate) struct InstallingCommittedAdmission {
    store_open_id: Uuid,
    native_open: String,
    intent: ImageInstallIntent,
    intent_sha256: String,
    proposal: NativeCurrentImageProposalReceipt,
    current_scope: RecoveryFenceScope,
}
impl InstallingCommittedAdmission {
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.current_scope
    }
    pub(crate) fn proposal(&self) -> &NativeCurrentImageProposalReceipt {
        &self.proposal
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.intent_sha256
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        store.require_durable_recovery_catalog()?;
        self.require_refusal_identity_current(store, db, native, role)
    }
    fn require_refusal_identity_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        // Complete immutable candidate validation, but no claim of fsync.
        // Only the owned-freeze resync method may make an uncertain store
        // observable again; this preflight cannot mint a quorum vote.
        // Bound the complete serialized candidate before cloning/checksum
        // buffers, including an uncertainty retry with unrelated ranges.
        validate_native_json_size(&store.catalog, 64 * 1024 * 1024)?;
        RangeWriteEnvelope::new(store.catalog.clone())?.verify()?;
        store.require_native_recovery_identity(role, native)?;
        self.current_scope.require_role(role)?;
        native.require_current(db)?;
        self.proposal.require_current(db, native)?;
        require_installing_same_voter_transition(self.proposal.historical_source(), role)?;
        let current = store
            .catalog
            .image_install_intents
            .get(&self.intent.range.id)
            .ok_or_else(|| range_write_error("protected Installing intent disappeared"))?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.intent.range.id)
            .ok_or_else(|| {
                range_write_error("protected Installing recovery refusal disappeared")
            })?;
        if self.store_open_id != store.open_id
            || self.native_open != native.database_incarnation()
            || store.root.canonicalize()? != db.data_path().canonicalize()?
            || current.sha256()? != self.intent_sha256
            || current.protected_proposal.as_ref() != Some(&self.proposal.pointer())
            || current.core_sha256()? != self.proposal.intent_core_sha256()
            || current.target_node_id != *native.local_node_id()
            || current.target_member_incarnation != native.local_member_incarnation()
            || current.target_birth_id != native.birth_id()
            || current.target_birth_sha256 != native.birth_sha256()
            || current.target_namespace_ids.as_slice() != self.proposal.target_namespace_ids()
            || saved.scope.recovery_nonce != current.nonce
            || saved.scope.range_epoch != current.range.epoch
            || saved.scope.configuration_sha256
                != self.proposal.historical_source().scope.configuration_sha256
            || !matches!(saved.phase, RecoveryFencePhase::Active)
            || current.purpose != ImageInstallPurpose::CurrentVoterRepair
            || !matches!(
                current.source_purpose,
                crate::distribution_range_image::RangeImageSourcePurposeV1::CurrentVoterRepair
            )
            || current.session_id != current.nonce
        {
            return Err(range_write_error(
                "Installing committed admission lost exact historical intent/birth/scope",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
    pub(crate) fn admit_installing_committed_decision(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        pointer: &NativeCurrentImageProposalPointer,
    ) -> Result<InstallingCommittedAdmission> {
        // No mutation, reader-fence upgrade or ordinary NoImage bypass occurs.
        self.require_durable_recovery_catalog()?;
        self.preflight_installing_committed_refusal(db, native, role, pointer)
    }
    pub(crate) fn preflight_installing_committed_refusal(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        pointer: &NativeCurrentImageProposalPointer,
    ) -> Result<InstallingCommittedAdmission> {
        // No mutation, reader-fence upgrade or ordinary NoImage bypass occurs.
        self.require_native_recovery_identity(role, native)?;
        let intent = self
            .catalog
            .image_install_intents
            .get(&role.current_range().id)
            .ok_or_else(|| {
                range_write_error("existing-COMMITTED Installing operation needs actual intent")
            })?
            .clone();
        if self.catalog.format_version < IMAGE_PROPOSAL_CATALOG_FORMAT
            || intent.protected_proposal.as_ref() != Some(pointer)
        {
            return Err(range_write_error(
                "existing-COMMITTED Installing operation needs exact protected proposal",
            ));
        }
        let proposal = db.read_native_current_image_proposal(native, pointer)?;
        let result = InstallingCommittedAdmission {
            store_open_id: self.open_id,
            native_open: native.database_incarnation().to_owned(),
            intent_sha256: intent.sha256()?,
            intent,
            proposal,
            current_scope: RecoveryFenceScope::from_role(role)?,
        };
        result.require_refusal_identity_current(self, db, native, role)?;
        Ok(result)
    }
    pub(crate) fn resync_installing_committed_refusal(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &InstallingCommittedAdmission,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        admission.require_refusal_identity_current(self, db, native, role)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(admission.scope().range_id, frozen)?;
        if frozen.owner() != admission.scope().freeze_owner()? {
            return Err(range_write_error(
                "Installing refusal resync changed actual owner",
            ));
        }
        if self.recovery_persistence_uncertain {
            let controlled = db.range_mutation_gate().try_controlled_mutation(frozen)?;
            // Exact complete in-memory candidate is re-fsynced unchanged.
            // No old/new nonce, intent, raw/native index or marker is reset.
            self.persist_recovery_catalog()?;
            drop(controlled);
        }
        admission.require_current(self, db, native, role)
    }
    pub(crate) fn observe_installing_committed_fence(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &InstallingCommittedAdmission,
        frozen: &FrozenRangeMutationPermit,
        challenge: Uuid,
    ) -> Result<LocalInstallingCommittedFence> {
        admission.require_current(self, db, native, role)?;
        if challenge.is_nil() || frozen.owner() != admission.scope().freeze_owner()? {
            return Err(range_write_error(
                "Installing existing-decision challenge/freeze differs",
            ));
        }
        db.range_mutation_gate()
            .validate_quiescent_frozen(admission.scope().range_id, frozen)?;
        let safety = db.native_range_install_safety(native, admission.scope().range_id, frozen)?;
        let obligations =
            self.image_install_catalog_history_obligations(admission.scope().range_id)?;
        let result = crate::distribution_range_recovery::InstallingCommittedFenceEvidence {
            current_scope: admission.scope().clone(),
            historical_source: admission.proposal.historical_source().clone(),
            proposal: admission.proposal.pointer(),
            intent_sha256: admission.intent_sha256.clone(),
            preimage: admission.proposal.preimage().clone(),
            target_namespace_ids: admission.proposal.target_namespace_ids().to_vec(),
            node_id: self.catalog.node_id.clone(),
            member_incarnation: native.local_member_incarnation(),
            native_birth_id: native.birth_id(),
            native_birth_sha256: native.birth_sha256().to_owned(),
            native_database_incarnation: native.database_incarnation().to_owned(),
            store_open_id: self.open_id,
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            challenge,
            native_point: safety.history_point().map(|p| {
                crate::distribution_range_recovery::RecoveryNativeHistoryPointV1 {
                    range_index: p.range_index(),
                    logical_genesis_sha256: p.logical_genesis_sha256().to_owned(),
                    logical_history_sha256: p.logical_history_sha256().to_owned(),
                    native_receipt_sha256: p.native_receipt_sha256().to_owned(),
                }
            }),
            pending: match (
                safety.pending_index(),
                safety.pending_command_checksum_sha256(),
            ) {
                (Some(index), Some(h)) => Some(
                    crate::distribution_range_recovery::RecoveryNativePendingHistoryV1 {
                        index,
                        command_checksum_sha256: h.to_owned(),
                        materialized: safety.materialized_pending_index() == Some(index),
                    },
                ),
                (None, None) => None,
                _ => {
                    return Err(range_write_error(
                        "incomplete Installing pending obligation",
                    ))
                }
            },
            progress: self.progress(admission.scope().range_id),
            native: db.native_range_prefix_observation(admission.scope().range_id)?,
            irreversible_commands: obligations.irrevocable_commands().clone(),
            catalogue_sha256: obligations.range_log_sha256().to_owned(),
        };
        result.validate_shape()?;
        safety.require_current(db, native, frozen)?;
        obligations.require_current(self)?;
        admission.require_current(self, db, native, role)?;
        Ok(LocalInstallingCommittedFence { evidence: result })
    }
}

/// Durable provenance, not an authority reconstructed on open. Original
/// command epoch/checksum and old intent are never rewritten to the current
/// certification scope. Native/open labels are recorded separately.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct InstallingCommittedCertification {
    format_version: u32,
    pub(super) scope: RecoveryFenceScope,
    members: BTreeMap<ClusterNodeId, crate::distribution_range_recovery::RecoveryMemberIdentity>,
    pub(super) command: RangeWriteCommand,
    proposal: NativeCurrentImageProposalPointer,
    protected_intent_sha256: String,
    protected_target: ClusterNodeId,
    protected_target_birth: Uuid,
    protected_target_birth_sha256: String,
    protected_target_namespace_ids: Vec<(String, Uuid)>,
    local_node: ClusterNodeId,
    local_member_incarnation: u64,
    native_birth: Uuid,
    native_birth_sha256: String,
    audit_native_open: String,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    metadata_context_id: Uuid,
    metadata_term: u64,
    metadata_read_index: u64,
    votes: BTreeMap<ClusterNodeId, crate::distribution_range_recovery::InstallingDecisionVoteAudit>,
    pub(super) marker: HistoricalRangeCertification,
    checksum_sha256: String,
}
impl InstallingCommittedCertification {
    fn key(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    "Installing-current-cert-key-v1",
                    &self.scope.cluster_id,
                    self.scope.bootstrap_id,
                    self.scope.range_id,
                    self.scope.range_epoch,
                    &self.scope.configuration_sha256,
                    self.scope.recovery_nonce,
                    self.command.index,
                ),
                256 * 1024,
            )?,
        )))
    }
    fn checksum(&self) -> Result<String> {
        let mut c = self.clone();
        c.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("Installing-existing-committed-cert-v1", c),
                MAX_RECORD_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.proposal.validate()?;
        validate_native_recovery_command_size(&self.command)?;
        self.command.validate()?;
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.format_version != 1
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.command.cluster_id != self.scope.cluster_id
            || self.command.range_id != self.scope.range_id
            || self.command.range_epoch > self.scope.range_epoch
            || self.members.is_empty()
            || self.members.len() > 64
            || self.votes.len() < self.members.len() / 2 + 1
            || self.votes.len() > self.members.len()
            || !self.members.contains_key(&self.protected_target)
            || self
                .members
                .get(&self.local_node)
                .is_none_or(|m| m.incarnation != self.local_member_incarnation)
            || self.native_birth.is_nil()
            || self.protected_target_birth.is_nil()
            || self.metadata_context_id.is_nil()
            || self.audit_native_open.is_empty()
            || self.audit_native_open.len() > 128
            || self.metadata_term < self.scope.metadata_term
            || self.metadata_read_index < self.scope.metadata_read_index
            || !digest(&self.protected_intent_sha256)
            || !digest(&self.native_birth_sha256)
            || !digest(&self.protected_target_birth_sha256)
            || self.protected_target_namespace_ids.len() > 65_536
            || self
                .protected_target_namespace_ids
                .windows(2)
                .any(|p| p[0].0 >= p[1].0)
            || self
                .protected_target_namespace_ids
                .iter()
                .any(|(n, id)| n.is_empty() || id.is_nil())
            || self.marker.recovery_nonce != self.scope.recovery_nonce
            || self.marker.configuration_sha256 != self.scope.configuration_sha256
            || self.marker.range_epoch != self.scope.range_epoch
            || self.marker.index != self.command.index
            || self.marker.command_sha256 != self.command.checksum_sha256
            || self.checksum()? != self.checksum_sha256
        {
            return Err(range_write_error(
                "damaged Installing existing-committed certification",
            ));
        }
        let mut irreversible = false;
        let mut protected = false;
        for (id, v) in &self.votes {
            if self
                .members
                .get(id)
                .is_none_or(|m| m.incarnation == 0 || m.incarnation != v.member_incarnation)
                || v.raw_state == Some(RangeWriteState::Aborted)
                || v.prior_sparse_sha256.as_deref().is_some_and(|h| !digest(h))
            {
                return Err(range_write_error(
                    "damaged Installing certification decision voter",
                ));
            }
            match (&v.installing_proposal, &v.installing_intent_sha256) {
                (Some(p), Some(h))
                    if id == &self.protected_target
                        && p == &self.proposal
                        && h == &self.protected_intent_sha256
                        && v.raw_state.is_none()
                        && v.prior_sparse_sha256.is_none() =>
                {
                    protected = true
                }
                (None, None) if id != &self.protected_target => {
                    irreversible |= matches!(
                        v.raw_state,
                        Some(RangeWriteState::QuorumCommitted | RangeWriteState::Applied)
                    ) || v.prior_sparse_sha256.is_some()
                }
                _ => {
                    return Err(range_write_error(
                        "Installing certification audit conflates protected and ordinary voters",
                    ))
                }
            }
        }
        if !protected || !irreversible {
            return Err(range_write_error(
                "Installing certification lacks historical committed evidence and protected target",
            ));
        }
        Ok(())
    }
}
pub(super) type InstallingCertifications =
    BTreeMap<RangeId, BTreeMap<String, InstallingCommittedCertification>>;
pub(super) fn validate_installing_certifications(catalog: &RangeWriteCatalog) -> Result<()> {
    if catalog.format_version < INSTALLING_CERTIFICATION_FORMAT {
        if catalog.installing_committed_certifications.is_some() {
            return Err(range_write_error(
                "Installing certification map lacks reader fence",
            ));
        }
        return Ok(());
    }
    let all = catalog
        .installing_committed_certifications
        .as_ref()
        .ok_or_else(|| {
            range_write_error("Installing certification reader fence requires explicit map")
        })?;
    validate_native_json_size(all, MAX_CATALOG_BYTES)?;
    let mut total = 0;
    for (range, records) in all {
        if !catalog.ranges.contains_key(range) || records.len() > MAX_RECORDS_PER_RANGE {
            return Err(range_write_error("Installing certification range bound"));
        }
        total += records.len();
        let mut obligations = BTreeMap::new();
        for (key, r) in records {
            r.validate()?;
            if key != &r.key()?
                || *range != r.scope.range_id
                || r.scope.cluster_id != catalog.cluster_id
                || r.local_node != catalog.node_id
            {
                return Err(range_write_error(
                    "Installing certification map/key/domain mismatch",
                ));
            }
            if obligations
                .insert(r.command.index, r.command.checksum_sha256.clone())
                .is_some_and(|h| h != r.command.checksum_sha256)
                || catalog.ranges[range]
                    .entries
                    .get(&r.command.index)
                    .is_some_and(|e| e.command != r.command || e.state == RangeWriteState::Aborted)
            {
                return Err(range_write_error(
                    "Installing certification conflicts with retained decision",
                ));
            }
        }
    }
    if total > 4096 {
        return Err(range_write_error(
            "Installing certification catalogue count bound",
        ));
    }
    Ok(())
}
impl RangeWriteStore {
    pub(super) fn require_installing_certification_native_binding(
        &self,
        range: RangeId,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        validate_installing_certifications(&self.catalog)?;
        for r in self
            .catalog
            .installing_committed_certifications
            .as_ref()
            .and_then(|m| m.get(&range))
            .into_iter()
            .flat_map(|r| r.values())
        {
            if r.local_node != *native.local_node_id()
                || r.local_member_incarnation != native.local_member_incarnation()
                || r.native_birth != native.birth_id()
                || r.native_birth_sha256 != native.birth_sha256()
                || r.restore_activation.as_ref() != native.restore_activation()
            {
                return Err(range_write_error(
                    "Installing certification belongs to another native birth/restore",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn installing_certification_obligations(
        &self,
        range: RangeId,
    ) -> Result<BTreeMap<u64, String>> {
        validate_installing_certifications(&self.catalog)?;
        let mut out = BTreeMap::new();
        for r in self
            .catalog
            .installing_committed_certifications
            .as_ref()
            .and_then(|m| m.get(&range))
            .into_iter()
            .flat_map(|r| r.values())
        {
            if out
                .insert(r.command.index, r.command.checksum_sha256.clone())
                .is_some_and(|h| h != r.command.checksum_sha256)
            {
                return Err(range_write_error(
                    "Installing certification permanent decision conflict",
                ));
            }
        }
        Ok(out)
    }
    pub(crate) fn certify_existing_installing_decision(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        decision: &crate::distribution_range_recovery::InstallingCommittedDecision,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<LocalInstallingCommittedCertification> {
        self.certify_existing_installing_decision_with(
            db,
            native,
            role,
            decision,
            frozen,
            crate::storage::write_atomic,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_certify_existing_installing_decision_with(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        decision: &crate::distribution_range_recovery::InstallingCommittedDecision,
        frozen: &FrozenRangeMutationPermit,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<LocalInstallingCommittedCertification> {
        self.certify_existing_installing_decision_with(db, native, role, decision, frozen, write)
    }
    fn certify_existing_installing_decision_with(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        decision: &crate::distribution_range_recovery::InstallingCommittedDecision,
        frozen: &FrozenRangeMutationPermit,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<LocalInstallingCommittedCertification> {
        decision.require_recipient_role(role)?;
        let a = decision.authority();
        let scope = a.scope();
        let command = decision.command();
        self.require_native_recovery_identity(role, native)?;
        native.require_current(db)?;
        db.range_mutation_gate()
            .validate_frozen(scope.range_id, frozen)?;
        if frozen.owner() != scope.freeze_owner()? {
            return Err(range_write_error(
                "Installing current certification freeze changed",
            ));
        }
        if let Some(p) = self.catalog.handoff_promises.get(&scope.range_id) {
            if command.index > p.resolved_through {
                return Err(range_write_error(
                    "Installing existing decision exceeds permanent handoff promise",
                ));
            }
        }
        let is_target = role.local_node_id() == a.target();
        if is_target {
            let local = self.admit_installing_committed_decision(db, native, role, a.pointer())?;
            if local.intent_sha256() != a.installing().intent_sha256
                || local.proposal().historical_source().source_range_index < command.index
            {
                return Err(range_write_error(
                    "Installing certification protected intent changed",
                ));
            }
        } else {
            // An ordinary recipient remains behind the UNCHANGED NoImage guard.
            self.require_recovery_scope(role, native, scope)?;
            let entry = self
                .catalog
                .ranges
                .get(&scope.range_id)
                .and_then(|l| l.entries.get(&command.index))
                .ok_or_else(|| {
                    range_write_error(
                        "ordinary existing-decision certifier lacks retained full command",
                    )
                })?;
            if entry.command != *command
                || !matches!(
                    entry.state,
                    RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                )
            {
                return Err(range_write_error(
                    "ordinary existing-decision certifier must retain actual CQ/Applied command",
                ));
            }
        }
        let marker = HistoricalRangeCertification {
            recovery_nonce: scope.recovery_nonce,
            configuration_sha256: scope.configuration_sha256.clone(),
            range_epoch: scope.range_epoch,
            index: command.index,
            command_sha256: command.checksum_sha256.clone(),
        };
        let e = a.installing();
        let mut record = InstallingCommittedCertification {
            format_version: 1,
            scope: scope.clone(),
            members: role.member_credentials().clone(),
            command: command.clone(),
            proposal: a.pointer().clone(),
            protected_intent_sha256: e.intent_sha256.clone(),
            protected_target: a.target().clone(),
            protected_target_birth: e.native_birth_id,
            protected_target_birth_sha256: e.native_birth_sha256.clone(),
            protected_target_namespace_ids: e.target_namespace_ids.clone(),
            local_node: role.local_node_id().clone(),
            local_member_incarnation: role.local_member_incarnation(),
            native_birth: native.birth_id(),
            native_birth_sha256: native.birth_sha256().to_owned(),
            audit_native_open: native.database_incarnation().to_owned(),
            restore_activation: native.restore_activation().cloned(),
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            votes: decision.votes().clone(),
            marker: marker.clone(),
            checksum_sha256: String::new(),
        };
        record.checksum_sha256 = record.checksum()?;
        record.validate()?;
        let key = record.key()?;
        let mut candidate = self.catalog.clone();
        let entries = candidate
            .installing_committed_certifications
            .get_or_insert_with(BTreeMap::new)
            .entry(scope.range_id)
            .or_default();
        if let Some(old) = entries.get(&key) {
            // Preserve original provenance/open on an exact-session retry.
            if old.command != record.command
                || !old.scope.same_session(&record.scope)
                || old.proposal != record.proposal
                || old.protected_intent_sha256 != record.protected_intent_sha256
                || old.native_birth != record.native_birth
            {
                return Err(range_write_error(
                    "Installing certification retry changes original decision/domain",
                ));
            }
            record = old.clone();
        } else {
            if entries.len() >= MAX_RECORDS_PER_RANGE {
                return Err(range_write_error(
                    "Installing certification range bound reached",
                ));
            }
            entries.insert(key, record.clone());
        }
        if !is_target {
            let saved = candidate
                .recovery_fences
                .get_mut(&scope.range_id)
                .expect("ordinary scope checked");
            if saved
                .historical_certifications
                .get(&command.index)
                .is_some_and(|old| old != &marker)
                || (!saved.historical_certifications.contains_key(&command.index)
                    && saved.historical_certifications.len() >= 1024)
            {
                return Err(range_write_error(
                    "ordinary current certification marker conflict/bound",
                ));
            }
            saved
                .historical_certifications
                .insert(command.index, marker.clone());
        }
        // Retain a permanent full-body historical projection as well as the
        // detailed Installing audit. Future ordered compaction must not hide
        // an irrevocable decision from existing historical lookup/repair.
        let sparse = candidate
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new)
            .entry(scope.range_id)
            .or_default();
        if let Some(old) = sparse.get(&command.index) {
            old.validate()?;
            if old.command() != command {
                return Err(range_write_error(
                    "Installing decision conflicts with original permanent sparse body",
                ));
            }
        } else {
            if sparse.len() >= 1024 {
                return Err(range_write_error("Installing sparse retention bound"));
            }
            sparse.insert(command.index,super::sparse_certification::SparseHistoricalDecision::from_actual_installing_decision(decision,role,native)?);
        }
        // Populate and reader-fence every new field before validating the
        // candidate. An ordinary certifier may still have a pre-sparse
        // catalogue, unlike the Installing target's already upgraded one.
        // Validation must remain before any self.catalog mutation/publication.
        // Existing sparse mandatory field remains explicit for the newer format.
        candidate
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        candidate
            .installing_recovery_handovers
            .get_or_insert_with(BTreeMap::new);
        candidate.format_version = candidate
            .format_version
            .max(INSTALLING_CERTIFICATION_FORMAT);
        super::sparse_certification::validate_sparse_catalog(&candidate)?;
        validate_installing_certifications(&candidate)?;
        super::image_install_state::validate_catalog_install_intents(&candidate)?;
        crate::format::enable_current_image_proposal(&self.root)?;
        // Candidate validation occurs before mutation; ambiguity keeps refusal.
        self.recovery_persistence_uncertain = true;
        self.catalog = candidate;
        self.persist_recovery_catalog_with(write)?;
        decision.require_live()?;
        native.require_current(db)?;
        Ok(LocalInstallingCommittedCertification {
            evidence:
                crate::distribution_range_recovery::InstallingCommittedCertificationEvidence {
                    scope: scope.clone(),
                    node_id: role.local_node_id().clone(),
                    member_incarnation: role.local_member_incarnation(),
                    native_birth_id: native.birth_id(),
                    native_birth_sha256: native.birth_sha256().to_owned(),
                    native_database_incarnation: native.database_incarnation().to_owned(),
                    store_open_id: self.open_id,
                    proposal: record.proposal,
                    intent_sha256: record.protected_intent_sha256,
                    command: command.clone(),
                    marker,
                    record_sha256: record.checksum_sha256,
                    metadata_context_id: role.context_id(),
                    metadata_term: role.metadata_term(),
                    metadata_read_index: role.metadata_read_index(),
                    installing: is_target,
                },
        })
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    #[test]
    fn installing_certification_reader_fence_requires_both_complete_maps_and_preserves_legacy_bytes(
    ) {
        // Codec-only control, deliberately no native/quorum authority claim.
        let root = tempfile::tempdir().unwrap();
        let store = RangeWriteStore::open(
            root.path(),
            ClusterId::new("installing-codec").unwrap(),
            ClusterNodeId::new("n0").unwrap(),
            true,
        )
        .unwrap();
        let original = store.catalog.clone();
        let old = serde_json::to_value(&original).unwrap();
        assert!(old.get("installing_committed_certifications").is_none());
        assert!(old.get("installing_recovery_handovers").is_none());
        let roundtrip: RangeWriteCatalog = serde_json::from_value(old).unwrap();
        assert_eq!(
            catalog_checksum(&original).unwrap(),
            catalog_checksum(&roundtrip).unwrap()
        );
        let mut candidate = original.clone();
        candidate.format_version = INSTALLING_CERTIFICATION_FORMAT;
        candidate.sparse_committed_decisions = Some(BTreeMap::new());
        assert!(RangeWriteEnvelope::new(candidate.clone())
            .unwrap()
            .verify()
            .is_err());
        candidate.installing_committed_certifications = Some(BTreeMap::new());
        assert!(RangeWriteEnvelope::new(candidate.clone())
            .unwrap()
            .verify()
            .is_err());
        candidate.installing_recovery_handovers = Some(BTreeMap::new());
        RangeWriteEnvelope::new(candidate.clone())
            .unwrap()
            .verify()
            .unwrap();
        candidate.installing_committed_certifications = None;
        assert!(RangeWriteEnvelope::new(candidate)
            .unwrap()
            .verify()
            .is_err());
        let mut unfenced = original;
        unfenced.installing_recovery_handovers = Some(BTreeMap::new());
        assert!(RangeWriteEnvelope::new(unfenced).unwrap().verify().is_err());
    }
}
