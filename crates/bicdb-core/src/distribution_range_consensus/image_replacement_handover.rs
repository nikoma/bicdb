//! Final-only refusal transition after the ACTUAL full replacement source cut.
//! New native proposal is durable first; one catalogue publication archives
//! old refusals and installs new refusals. No rows/floors/serving are released.
use super::*;
use crate::db::NativeRecoveryAdmission;
use crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
const MAX_HANDOVERS: usize = 128;
const MAX_HANDOVER_BYTES: usize = 512 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct InstallingRecoveryHandover {
    range_id: RangeId,
    old_fence: RecoveryFenceRecord,
    old_intent: ImageInstallIntent,
    new_scope: RecoveryFenceScope,
    new_intent: ImageInstallIntent,
    old_intent_sha256: String,
    new_intent_sha256: String,
    source_acceptance_sha256: String,
    checksum_sha256: String,
}
impl InstallingRecoveryHandover {
    fn checksum(&self) -> Result<String> {
        let mut c = self.clone();
        c.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("Installing-refusal-handover-v1", c),
                MAX_HANDOVER_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.old_fence.validate()?;
        self.old_intent.validate()?;
        self.new_scope.validate()?;
        self.new_intent.validate()?;
        let mut old_domain = self.old_intent.range.clone();
        let mut new_domain = self.new_intent.range.clone();
        old_domain.approximate_bytes = 0;
        old_domain.approximate_qps = 0;
        new_domain.approximate_bytes = 0;
        new_domain.approximate_qps = 0;
        new_domain.epoch = old_domain.epoch;
        new_domain.leader = old_domain.leader.clone();
        old_domain.replicas.sort_by_key(|r| r.id);
        new_domain.replicas.sort_by_key(|r| r.id);
        if old_domain != new_domain
            || self.old_intent.current_voter_credentials
                != self.new_intent.current_voter_credentials
            || self.old_intent.schema_sha256 != self.new_intent.schema_sha256
            || self.old_intent.bootstrap_id != self.new_intent.bootstrap_id
            || self.old_intent.restore_activation != self.new_intent.restore_activation
        {
            return Err(range_write_error(
                "Installing handover cannot change same-voter physical domain",
            ));
        }
        if self.range_id != self.old_intent.range.id
            || self.range_id != self.new_intent.range.id
            || self.old_fence.scope.range_id != self.range_id
            || self.new_scope.range_id != self.range_id
            || !matches!(self.old_fence.phase, RecoveryFencePhase::Active)
            || !matches!(
                self.new_scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.old_fence.scope.recovery_nonce != self.old_intent.nonce
            || self.new_scope.recovery_nonce != self.new_intent.nonce
            || self.new_scope.range_epoch < self.old_fence.scope.range_epoch
            || self.new_scope.recovery_nonce == self.old_fence.scope.recovery_nonce
            || self.old_intent.protected_proposal.is_none()
            || self.new_intent.protected_proposal.is_none()
            || self.old_intent.target_node_id != self.new_intent.target_node_id
            || self.old_intent.target_birth_id != self.new_intent.target_birth_id
            || self.old_intent.target_birth_sha256 != self.new_intent.target_birth_sha256
            || self.old_intent.target_namespace_ids != self.new_intent.target_namespace_ids
            || self.new_intent.source_range_index < self.old_intent.source_range_index
            || self.old_intent.sha256()? != self.old_intent_sha256
            || self.new_intent.sha256()? != self.new_intent_sha256
            || self.source_acceptance_sha256.len() != 64
            || !self
                .source_acceptance_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self.checksum()? != self.checksum_sha256
        {
            return Err(range_write_error("damaged Installing refusal handover"));
        }
        Ok(())
    }
}
pub(super) type InstallingHandovers = BTreeMap<RangeId, BTreeMap<Uuid, InstallingRecoveryHandover>>;
pub(super) fn validate_installing_handovers(c: &RangeWriteCatalog) -> Result<()> {
    if c.format_version < INSTALLING_CERTIFICATION_FORMAT {
        if c.installing_recovery_handovers.is_some() {
            return Err(range_write_error("Installing handover lacks reader fence"));
        }
        return Ok(());
    }
    let maps = c
        .installing_recovery_handovers
        .as_ref()
        .ok_or_else(|| range_write_error("Installing handover format requires explicit map"))?;
    validate_native_json_size(maps, 32 * 1024 * 1024)?;
    let mut total = 0;
    for (r, records) in maps {
        if records.len() > MAX_HANDOVERS || !c.ranges.contains_key(r) {
            return Err(range_write_error("Installing handover count/range bound"));
        }
        total += records.len();
        for (a, v) in records {
            v.validate()?;
            if *r != v.range_id
                || *a != v.new_intent.attempt_id
                || v.old_intent.cluster_id != c.cluster_id
                || v.old_intent.target_node_id != c.node_id
            {
                return Err(range_write_error(
                    "Installing handover catalogue identity differs",
                ));
            }
        }
    }
    if total > 4096 {
        return Err(range_write_error("Installing handover total bound"));
    }
    Ok(())
}
#[derive(Debug)]
pub(crate) struct InstallingRefusalHandoverReceipt {
    store_open_id: Uuid,
    native_open: String,
    range: RangeId,
    attempt: Uuid,
    checksum: String,
}
impl InstallingRefusalHandoverReceipt {
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        store.require_durable_recovery_catalog()?;
        native.require_current(db)?;
        let h = store.installing_handover(self.range, self.attempt)?;
        h.new_scope.require_role(role)?;
        if store.open_id != self.store_open_id
            || native.database_incarnation() != self.native_open
            || h.checksum_sha256 != self.checksum
            || store
                .catalog
                .image_install_intents
                .get(&self.range)
                .is_none_or(|i| i.sha256().ok().as_deref() != Some(h.new_intent_sha256.as_str()))
            || store
                .catalog
                .recovery_fences
                .get(&self.range)
                .is_none_or(|f| {
                    !f.scope.same_session(&h.new_scope)
                        || !matches!(f.phase, RecoveryFencePhase::Active)
                        || f.native_database_incarnation != native.database_incarnation()
                })
        {
            return Err(range_write_error(
                "actual Installing handover/open/refusal changed",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub(super) struct ReplacedInstallingIdentity {
    session: Uuid,
    attempt: Uuid,
    intent_sha256: String,
    source_r: u64,
}
impl ReplacedInstallingIdentity {
    pub(crate) fn matches(&self, session: Uuid, attempt: Uuid, sha: &str, r: u64) -> bool {
        self.session == session
            && self.attempt == attempt
            && self.intent_sha256 == sha
            && self.source_r == r
    }
}
impl RangeWriteStore {
    fn installing_handover(&self, r: RangeId, a: Uuid) -> Result<&InstallingRecoveryHandover> {
        validate_installing_handovers(&self.catalog)?;
        self.catalog
            .installing_recovery_handovers
            .as_ref()
            .and_then(|m| m.get(&r))
            .and_then(|m| m.get(&a))
            .ok_or_else(|| range_write_error("actual Installing handover is absent"))
    }
    pub(super) fn replaced_installing_identity(
        &self,
        intent: &ImageInstallIntent,
    ) -> Result<Option<ReplacedInstallingIdentity>> {
        validate_installing_handovers(&self.catalog)?;
        let Some(records) = self
            .catalog
            .installing_recovery_handovers
            .as_ref()
            .and_then(|m| m.get(&intent.range.id))
        else {
            return Ok(None);
        };
        let mut found = None;
        for h in records.values() {
            let mut expected = h.new_intent.clone();
            expected.attempt_id = intent.attempt_id;
            expected.protected_proposal = intent.protected_proposal.clone();
            if expected == *intent {
                if found.is_some() {
                    return Err(range_write_error(
                        "ambiguous retained Installing handover marker lineage",
                    ));
                }
                found = Some(h);
            }
        }
        let Some(h) = found else { return Ok(None) };
        Ok(Some(ReplacedInstallingIdentity {
            session: h.old_intent.session_id,
            attempt: h.old_intent.attempt_id,
            intent_sha256: h.old_intent_sha256.clone(),
            source_r: h.old_intent.source_range_index,
        }))
    }
    pub(crate) fn handover_installing_replacement_refusal(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
    ) -> Result<InstallingRefusalHandoverReceipt> {
        self.handover_installing_replacement_refusal_with(
            db,
            native,
            source,
            role,
            frozen,
            attempt,
            || Ok(()),
        )
    }
    #[cfg(test)]
    pub(crate) fn test_replacement_proposal_before_handover(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
    ) -> Result<InstallingRefusalHandoverReceipt> {
        self.handover_installing_replacement_refusal_with(
            db,
            native,
            source,
            role,
            frozen,
            attempt,
            || {
                Err(range_write_error(
                    "injected after actual replacement native proposal, BEFORE catalogue handover",
                ))
            },
        )
    }
    fn handover_installing_replacement_refusal_with(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        attempt: Uuid,
        prepublish: impl FnOnce() -> Result<()>,
    ) -> Result<InstallingRefusalHandoverReceipt> {
        source.require_current()?;
        let cut = source.cut();
        let current = role.role();
        current.require_live_request()?;
        if !cut.is_actual_replacement_source()
            || !matches!(role, NativeImageInstallRole::CurrentVoterRepair(_))
            || attempt.is_nil()
            || attempt == current.recovery_nonce()
        {
            return Err(range_write_error(
                "handover requires actual replacement source and fresh attempt",
            ));
        }
        let pointer = cut
            .resume_proposal()
            .ok_or_else(|| range_write_error("replacement cut lacks exact protected parent"))?;
        let old_admission =
            self.admit_installing_committed_decision(db, native, current, pointer)?;
        old_admission.require_current(self, db, native, current)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(cut.range().id, frozen)?;
        if frozen.owner() != current.recovery_nonce() {
            return Err(range_write_error(
                "handover must retain actual current recovery freeze",
            ));
        }
        let old = self.catalog.image_install_intents[&cut.range().id].clone();
        let old_fence = self.catalog.recovery_fences[&cut.range().id].clone();
        if old.attempt_id == attempt || old_fence.scope.recovery_nonce == current.recovery_nonce() {
            return Err(range_write_error(
                "replacement handover requires another attempt/current recovery session",
            ));
        }
        let mut intent = self.candidate_native_image_install_intent(
            db,
            native,
            source,
            role,
            frozen,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            current.recovery_nonce(),
        )?;
        self.validate_install_local_log(cut.range(), cut.range_index())?;
        let binding = NativeImageProposalIntentBinding::from_actual_replacement_candidate(
            self, native, &intent, source, role, frozen,
        )?;
        let safety = db.native_range_install_safety(native, cut.range().id, frozen)?;
        let catalogue = self.image_install_catalog_history_obligations(cut.range().id)?;
        // Durable native proposal first. A crash here leaves an orphan and old
        // durable refusals intact; no target row has been changed.
        let protected = db.persist_native_current_image_proposal(
            self, &binding, native, &safety, &catalogue, source, role, frozen,
        )?;
        intent.protected_proposal = Some(protected.pointer());
        intent.validate()?;
        let mut h = InstallingRecoveryHandover {
            range_id: cut.range().id,
            old_fence: old_fence.clone(),
            old_intent: old.clone(),
            new_scope: RecoveryFenceScope::from_role(current)?,
            old_intent_sha256: old.sha256()?,
            new_intent_sha256: intent.sha256()?,
            new_intent: intent.clone(),
            source_acceptance_sha256: hex::encode(Sha256::digest(
                crate::distribution_range_image::bounded_json(
                    cut.historical_current_acceptance()
                        .ok_or_else(|| {
                            range_write_error("replacement source lacks historical acceptance")
                        })?
                        .audit(),
                    8 * 1024 * 1024,
                )?,
            )),
            checksum_sha256: String::new(),
        };
        h.checksum_sha256 = h.checksum()?;
        h.validate()?;
        let mut new_fence = old_fence.clone();
        new_fence.scope = h.new_scope.clone();
        new_fence.native_database_incarnation = native.database_incarnation().to_owned();
        new_fence.historical_certifications.clear();
        new_fence
            .retired_nonces
            .insert(old_fence.scope.recovery_nonce);
        if new_fence.retired_nonces.len() > 256
            || new_fence
                .retired_nonces
                .contains(&h.new_scope.recovery_nonce)
        {
            return Err(range_write_error(
                "replacement handover nonce retirement bound/replay",
            ));
        }
        if let Some(records) = self
            .catalog
            .installing_committed_certifications
            .as_ref()
            .and_then(|m| m.get(&cut.range().id))
        {
            for r in records.values() {
                if r.scope.same_session(&h.new_scope) {
                    new_fence
                        .historical_certifications
                        .insert(r.command.index, r.marker.clone());
                }
            }
        }
        new_fence.validate()?;
        let mut candidate = self.catalog.clone();
        let archive = candidate
            .installing_recovery_handovers
            .get_or_insert_with(BTreeMap::new)
            .entry(cut.range().id)
            .or_default();
        if archive.len() >= MAX_HANDOVERS {
            return Err(range_write_error("replacement handover archive bound"));
        }
        archive.insert(attempt, h.clone());
        candidate
            .image_install_intents
            .insert(cut.range().id, intent);
        candidate.recovery_fences.insert(cut.range().id, new_fence);
        candidate
            .ranges
            .get_mut(&cut.range().id)
            .ok_or_else(|| range_write_error("handover range absent"))?
            .current_epoch = current.current_range().epoch;
        candidate.format_version = candidate
            .format_version
            .max(INSTALLING_CERTIFICATION_FORMAT);
        candidate
            .installing_committed_certifications
            .get_or_insert_with(BTreeMap::new);
        candidate
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        validate_installing_handovers(&candidate)?;
        super::image_install_certification::validate_installing_certifications(&candidate)?;
        super::image_install_state::validate_catalog_install_intents(&candidate)?;
        // Test adapter may stop after REAL native page-WAL publication,
        // before any catalogue mutation. Production continues immediately.
        prepublish()?;
        let controlled = db.range_mutation_gate().try_controlled_mutation(frozen)?;
        self.recovery_persistence_uncertain = true;
        self.catalog = candidate;
        self.persist_recovery_catalog()?;
        drop(controlled);
        let out = InstallingRefusalHandoverReceipt {
            store_open_id: self.open_id,
            native_open: native.database_incarnation().to_owned(),
            range: cut.range().id,
            attempt,
            checksum: h.checksum_sha256,
        };
        out.require_current(self, db, native, current)?;
        source.require_current()?;
        Ok(out)
    }
}
