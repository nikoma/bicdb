//! Exact historical-proposal admission for a REFUSAL-only cold observation.
//! This never relaxes ordinary NoImage guards or advances a native/catalogue
//! floor. The new observation remains distinct from ordinary recovery votes.
use super::*;
use crate::db::{NativeCurrentImageProposalReceipt, NativeRecoveryAdmission};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{CurrentImageInstallingEvidence, RecoveryRoleAdmission};

#[derive(Debug)]
pub(crate) struct CurrentImageInstallingAdmission {
    store_open_id: Uuid,
    intent: ImageInstallIntent,
    intent_sha256: String,
    proposal: NativeCurrentImageProposalReceipt,
    saved: RecoveryImageInstallSessionObservation,
}
impl CurrentImageInstallingAdmission {
    pub(crate) fn proposal_pointer(&self) -> NativeCurrentImageProposalPointer {
        self.proposal.pointer()
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.saved.saved_scope()
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        role.require_live_request()?;
        self.saved.require_current(store, db, native, role)?;
        if self.store_open_id != store.open_id
            || store.root.canonicalize()? != db.data_path().canonicalize()?
            || !store.fsync
            || !crate::format::current_image_proposal_active(&store.root)?
        {
            return Err(range_write_error(
                "installing admission belongs to another store/open",
            ));
        }
        let current = store
            .catalog
            .image_install_intents
            .get(&self.intent.range.id)
            .ok_or_else(|| range_write_error("installing admission lost exact intent"))?;
        if current.sha256()? != self.intent_sha256
            || current.protected_proposal.as_ref() != Some(&self.proposal.pointer())
        {
            return Err(range_write_error(
                "installing admission intent/proposal changed",
            ));
        }
        super::image_install_fence::require_intent_live_role(
            current,
            &NativeImageInstallRole::CurrentVoterRepair(role),
        )?;
        self.proposal.require_current(db, native)?;
        if self.proposal.intent_core_sha256() != current.core_sha256()? {
            return Err(range_write_error(
                "installing admission proposal/core differs",
            ));
        }
        let accepted = self.proposal.historical_source();
        if role.metadata_term() < accepted.metadata_term
            || role.metadata_read_index() < accepted.metadata_read_index
        {
            return Err(range_write_error(
                "installing fresh metadata is behind historical acceptance",
            ));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    pub(crate) fn admit_current_image_installing_observation(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        pointer: &NativeCurrentImageProposalPointer,
    ) -> Result<CurrentImageInstallingAdmission> {
        scope.require_role(role)?;
        let saved = self.observe_image_install_recovery_session(db, native, role)?;
        let intent = self
            .catalog
            .image_install_intents
            .get(&scope.range_id)
            .ok_or_else(|| {
                range_write_error("installing observation requires actual existing intent")
            })?
            .clone();
        if self.catalog.format_version < IMAGE_PROPOSAL_CATALOG_FORMAT
            || intent.protected_proposal.as_ref() != Some(pointer)
            || intent.purpose != ImageInstallPurpose::CurrentVoterRepair
            || !matches!(
                intent.source_purpose,
                crate::distribution_range_image::RangeImageSourcePurposeV1::CurrentVoterRepair
            )
            || intent.session_id != scope.recovery_nonce
            || intent.nonce != scope.recovery_nonce
        {
            return Err(range_write_error(
                "installing observation has no exact protected NormalRepair intent",
            ));
        }
        let proposal = db.read_native_current_image_proposal(native, pointer)?;
        let result = CurrentImageInstallingAdmission {
            store_open_id: self.open_id,
            intent_sha256: intent.sha256()?,
            intent,
            proposal,
            saved,
        };
        result.require_current(self, db, native, role)?;
        Ok(result)
    }
    pub(crate) fn observe_current_image_installing(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &CurrentImageInstallingAdmission,
        frozen: &FrozenRangeMutationPermit,
        challenge: Uuid,
    ) -> Result<CurrentImageInstallingEvidence> {
        admission.require_current(self, db, native, role)?;
        if challenge.is_nil() || frozen.owner() != admission.scope().freeze_owner()? {
            return Err(range_write_error(
                "installing observation has wrong challenge/freeze",
            ));
        }
        db.range_mutation_gate()
            .validate_quiescent_frozen(admission.scope().range_id, frozen)?;
        let safety = db.native_range_install_safety(native, admission.scope().range_id, frozen)?;
        let catalogue =
            self.image_install_catalog_history_obligations(admission.scope().range_id)?;
        let point = safety.history_point().map(|p| {
            crate::distribution_range_recovery::RecoveryNativeHistoryPointV1 {
                range_index: p.range_index(),
                logical_genesis_sha256: p.logical_genesis_sha256().to_string(),
                logical_history_sha256: p.logical_history_sha256().to_string(),
                native_receipt_sha256: p.native_receipt_sha256().to_string(),
            }
        });
        let current_pending = match (
            safety.pending_index(),
            safety.pending_command_checksum_sha256(),
        ) {
            (Some(index), Some(checksum)) => Some(
                crate::distribution_range_recovery::RecoveryNativePendingHistoryV1 {
                    index,
                    command_checksum_sha256: checksum.to_string(),
                    materialized: safety.materialized_pending_index() == Some(index),
                },
            ),
            (None, None) => None,
            _ => {
                return Err(range_write_error(
                    "installing observation native pending proof incomplete",
                ))
            }
        };
        let result = CurrentImageInstallingEvidence {
            scope: admission.scope().clone(),
            proposal: admission.proposal.pointer(),
            intent_sha256: admission.intent_sha256.clone(),
            source_acceptance: admission.proposal.historical_source().clone(),
            preimage: admission.proposal.preimage().clone(),
            target_birth_id: native.birth_id(),
            target_birth_sha256: native.birth_sha256().to_string(),
            target_namespace_ids: admission.proposal.target_namespace_ids().to_vec(),
            node_id: self.catalog.node_id.clone(),
            member_incarnation: native.local_member_incarnation(),
            store_open_id: self.open_id,
            native_database_incarnation: native.database_incarnation().to_string(),
            observed_metadata_context_id: role.context_id(),
            observed_metadata_term: role.metadata_term(),
            observed_metadata_read_index: role.metadata_read_index(),
            challenge,
            progress: self.progress(admission.scope().range_id),
            current_native: db.native_range_prefix_observation(admission.scope().range_id)?,
            current_native_point: point,
            current_pending,
            current_catalogue_sha256: catalogue.range_log_sha256().to_string(),
            current_irreversible_commands: catalogue.irrevocable_commands().clone(),
        };
        result.validate_shape()?;
        safety.require_current(db, native, frozen)?;
        catalogue.require_current(self)?;
        admission.require_current(self, db, native, role)?;
        Ok(result)
    }
}
