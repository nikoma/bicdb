//! Durable image-install refusal state. DTO labels/records never authorize
//! row mutation, a range floor, membership activation or strong reads.
use super::*;
use crate::distribution_range_image_scope::RangeImageSourcePurposeV1;

pub(super) const IMAGE_INSTALL_RANGE_LOG_FORMAT: u32 = 5;
pub(super) const MAX_INSTALL_INTENTS: usize = 65_536;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ImageInstallPurpose {
    RelocationLearner,
    CurrentVoterRepair,
}

/// A persisted refusal record, never an authority reconstructed from JSON.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ImageInstallIntent {
    pub(super) cluster_id: ClusterId,
    pub(super) bootstrap_id: Uuid,
    pub(super) range: RangeDescriptor,
    pub(super) purpose: ImageInstallPurpose,
    pub(super) attempt_id: Uuid,
    pub(super) session_id: Uuid,
    pub(super) nonce: Uuid,
    pub(super) source_purpose: RangeImageSourcePurposeV1,
    pub(super) target_node_id: ClusterNodeId,
    pub(super) target_member_incarnation: u64,
    pub(super) target_birth_id: Uuid,
    pub(super) target_birth_sha256: String,
    // Actual target-local physical namespace lineage, durable even in the
    // catalog-intent-before-native-marker crash window. Never source UUIDs.
    pub(super) target_namespace_ids: Vec<(String, Uuid)>,
    pub(super) source_id: ClusterNodeId,
    pub(super) source_incarnation: u64,
    pub(super) source_range_index: u64,
    pub(super) source_native_receipt_sha256: String,
    pub(super) source_catalog_sha256: String,
    pub(super) old_config_sha256: String,
    pub(super) intended_config_sha256: String,
    pub(super) activation_epoch: u64,
    pub(super) intended_range: RangeDescriptor,
    pub(super) intended_members: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub(super) current_voter_credentials: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub(super) target_credentials: crate::db::RangeImageBirthMember,
    pub(super) restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    pub(super) schema_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) protected_proposal: Option<super::NativeCurrentImageProposalPointer>,
}
impl ImageInstallIntent {
    pub(super) fn validate(&self) -> Result<()> {
        if let Some(pointer) = &self.protected_proposal {
            pointer.validate()?;
        }
        let digest =
            |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
        if self.bootstrap_id.is_nil()
            || self.attempt_id.is_nil()
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.target_birth_id.is_nil()
            || self.target_namespace_ids.len() > 65_536
            || self
                .target_namespace_ids
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
            || self
                .target_namespace_ids
                .iter()
                .any(|(name, id)| name.is_empty() || id.is_nil())
            || self.target_member_incarnation == 0
            || self.source_incarnation == 0
            || self.range.epoch == 0
            || self.activation_epoch < self.range.epoch
            || !digest(&self.target_birth_sha256)
            || !digest(&self.source_native_receipt_sha256)
            || !digest(&self.source_catalog_sha256)
            || !digest(&self.old_config_sha256)
            || !digest(&self.intended_config_sha256)
            || !digest(&self.schema_sha256)
            || self.current_voter_credentials.is_empty()
            || self.current_voter_credentials.len() > 64
            || self.target_credentials.incarnation != self.target_member_incarnation
            || self.intended_range.id != self.range.id
            || self.intended_range.start_token != self.range.start_token
            || self.intended_range.end_token != self.range.end_token
            || self.intended_range.epoch != self.activation_epoch
            || self.intended_members.is_empty()
            || self.intended_members.len() > 64
        {
            return Err(range_write_error("damaged image install refusal record"));
        }
        match &self.source_purpose {
            RangeImageSourcePurposeV1::CommittedHandoff {
                relocation_id,
                plan_id,
            } if relocation_id.get() > 0
                && !plan_id.is_empty()
                && plan_id.len() <= 1024
                && self.activation_epoch > self.range.epoch => {}
            RangeImageSourcePurposeV1::CurrentVoterRepair
                if self.purpose == ImageInstallPurpose::CurrentVoterRepair
                    && self.activation_epoch == self.range.epoch
                    && self.old_config_sha256 == self.intended_config_sha256 => {}
            _ => {
                return Err(range_write_error(
                    "invalid image install source purpose/configuration",
                ))
            }
        }
        crate::distribution_range_image::bounded_json(self, 128 * 1024)?;
        Ok(())
    }
    pub(super) fn core_sha256(&self) -> Result<String> {
        let mut core = self.clone();
        core.protected_proposal = None;
        core.sha256()
    }
    pub(super) fn sha256(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(self, 128 * 1024)?,
        )))
    }
}

pub(super) fn validate_catalog_install_intents(catalog: &RangeWriteCatalog) -> Result<()> {
    if catalog.image_install_intents.len() > MAX_INSTALL_INTENTS {
        return Err(range_write_error(
            "image install intent count bound exceeded",
        ));
    }
    for (range_id, intent) in &catalog.image_install_intents {
        intent.validate()?;
        if catalog.format_version >= super::IMAGE_PROPOSAL_CATALOG_FORMAT
            && matches!(
                intent.source_purpose,
                crate::distribution_range_image::RangeImageSourcePurposeV1::CurrentVoterRepair
            )
            && intent.protected_proposal.is_none()
        {
            return Err(range_write_error(
                "current image intent lacks protected pre-BEGIN proposal",
            ));
        }
        if catalog.format_version < IMAGE_INSTALL_RANGE_LOG_FORMAT
            || *range_id != intent.range.id
            || catalog.cluster_id != intent.cluster_id
            || catalog.node_id != intent.target_node_id
        {
            return Err(range_write_error(
                "image install intent catalog binding mismatch",
            ));
        }
    }
    Ok(())
}

impl RangeWriteStore {
    /// Defense for physical native writers. The live store check is still
    /// mandatory because a failed fsync may leave a conservative memory-only
    /// intent. This read never creates/migrates a catalog or grants authority.
    pub(crate) fn require_no_native_image_install_on_disk(
        root: &Path,
        range_id: RangeId,
    ) -> Result<()> {
        const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
        let file = match open_range_catalog_no_follow(&root.join(DEFAULT_RANGE_WRITE_LOG)) {
            Ok(file) => file,
            Err(BicDbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(())
            }
            Err(error) => return Err(error),
        };
        if file.metadata()?.len() > MAX_CATALOG_BYTES {
            return Err(range_write_error(
                "native install guard catalog byte bound exceeded",
            ));
        }
        let mut bytes = Vec::new();
        file.take(MAX_CATALOG_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CATALOG_BYTES {
            return Err(range_write_error(
                "native install guard catalog grew beyond bound",
            ));
        }
        let catalog = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        if catalog.image_install_intents.contains_key(&range_id) {
            return Err(range_write_error(
                "physical native mutation conflicts with durable catalog image install",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_no_image_install(&self, range_id: RangeId) -> Result<()> {
        if self.catalog.image_install_intents.contains_key(&range_id) {
            return Err(range_write_error(format!(
                "range {range_id} is unavailable while durable native image installation is incomplete"
            )));
        }
        Ok(())
    }
}
