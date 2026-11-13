//! Native proposal pointers are integrity references in a REFUSAL catalogue.
//! They cannot create source quorum, current metadata or native write authority.
use super::*;
use crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

pub(super) const IMAGE_PROPOSAL_CATALOG_FORMAT: u32 = 11;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCurrentImageProposalPointer {
    pub(crate) proposal_id: Uuid,
    pub(crate) checksum_sha256: String,
}
impl NativeCurrentImageProposalPointer {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.proposal_id.is_nil()
            || self.checksum_sha256.len() != 64
            || !self.checksum_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(range_write_error(
                "invalid protected current image proposal pointer",
            ));
        }
        Ok(())
    }
}

/// Actual store/open-bound intent-core preflight, before publishing BEGIN.
/// No DTO/Clone constructor; it authorizes only proposal persistence under the
/// SAME live source/role/native/freeze and actual target history observation.
#[derive(Debug)]
pub(crate) struct NativeImageProposalIntentBinding {
    store_open_id: Uuid,
    database_incarnation: String,
    intent: ImageInstallIntent,
    core_sha256: String,
    expected_freeze_owner: Uuid,
}
impl NativeImageProposalIntentBinding {
    pub(super) fn from_actual_candidate(
        store: &RangeWriteStore,
        native: &crate::db::NativeRecoveryAdmission,
        intent: &ImageInstallIntent,
    ) -> Result<Self> {
        Ok(Self {
            store_open_id: store.open_id,
            database_incarnation: native.database_incarnation().to_string(),
            intent: intent.clone(),
            core_sha256: intent.core_sha256()?,
            expected_freeze_owner: intent.attempt_id,
        })
    }
    pub(super) fn from_actual_replacement_candidate(
        store: &RangeWriteStore,
        native: &crate::db::NativeRecoveryAdmission,
        intent: &ImageInstallIntent,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<Self> {
        source.require_current()?;
        role.role().require_live_request()?;
        if !source.cut().is_actual_replacement_source()
            || source.cut().resume_proposal().is_none()
            || frozen.owner() != role.role().recovery_nonce()
            || intent.session_id != role.role().recovery_nonce()
        {
            return Err(range_write_error("replacement proposal requires actual verified replacement cut and current recovery freeze"));
        }
        let mut out = Self::from_actual_candidate(store, native, intent)?;
        out.expected_freeze_owner = role.role().recovery_nonce();
        Ok(out)
    }
    pub(crate) fn core_sha256(&self) -> &str {
        &self.core_sha256
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn namespace_ids(&self) -> &[(String, Uuid)] {
        &self.intent.target_namespace_ids
    }
    pub(crate) fn parent_proposal(
        &self,
        store: &RangeWriteStore,
    ) -> Result<Option<NativeCurrentImageProposalPointer>> {
        if store.open_id != self.store_open_id {
            return Err(range_write_error("proposal parent store reopened"));
        }
        Ok(store
            .catalog
            .image_install_intents
            .get(&self.intent.range.id)
            .and_then(|i| i.protected_proposal.clone()))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &crate::db::NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        if self.store_open_id != store.open_id
            || self.database_incarnation != native.database_incarnation()
        {
            return Err(range_write_error(
                "protected proposal preflight store/native open changed",
            ));
        }
        let current = store.candidate_native_image_install_intent(
            db,
            native,
            source,
            role,
            frozen,
            self.intent.purpose,
            self.intent.attempt_id,
            self.expected_freeze_owner,
        )?;
        if current.core_sha256()? != self.core_sha256 {
            return Err(range_write_error(
                "actual protected proposal intent core changed",
            ));
        }
        store.validate_install_local_log(&current.range, current.source_range_index)?;
        Ok(())
    }
}
