//! Durable refusal while a physical range image is being replaced.
//!
//! An intent grants neither a data floor nor membership/read readiness. Its
//! presence survives a failed writer, dropped local freeze and process reopen.
use super::*;
use crate::db::{NativeRangeInstallSafety, NativeRecoveryAdmission};
use crate::distribution_range_image::{
    RangeImageSourcePurposeV1, TentativeNativeRangeImageSourceAuthorization,
};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{HandoffRoleAdmission, RecoveryRoleAdmission};

use super::image_install_state::{ImageInstallIntent, MAX_INSTALL_INTENTS};

fn canonical_range(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|replica| replica.id);
    range
}
fn semantic_range(left: &RangeDescriptor, right: &RangeDescriptor) -> bool {
    canonical_range(left) == canonical_range(right)
}

fn same_install_lineage_for_new_attempt(
    existing: &ImageInstallIntent,
    candidate: &ImageInstallIntent,
) -> bool {
    let mut normalized = existing.clone();
    normalized.range = candidate.range.clone();
    normalized.attempt_id = candidate.attempt_id;
    // The protected producer validates the old pointer and immutable preimage;
    // pointer identity itself changes on a fresh explicitly authorized attempt.
    normalized.protected_proposal = candidate.protected_proposal.clone();
    if existing.attempt_id != candidate.attempt_id {
        normalized.target_namespace_ids = candidate.target_namespace_ids.clone();
    }
    semantic_range(&existing.range, &candidate.range) && normalized == *candidate
}

fn credentials(
    identity: &crate::distribution_range_recovery::RecoveryMemberIdentity,
) -> crate::db::RangeImageBirthMember {
    crate::db::RangeImageBirthMember {
        incarnation: identity.incarnation,
        tls_certificate_sha256: identity.active_certificate_sha256.clone(),
        pending_tls_certificate_sha256: identity.pending_certificate_sha256.clone(),
    }
}

/// The fresh metadata role is separate from the source/data-quorum proof.
/// Normal recovery cannot masquerade as a committed plan or learner role.
#[derive(Clone, Copy)]
pub(crate) enum NativeImageInstallRole<'a> {
    CommittedHandoff(&'a HandoffRoleAdmission),
    CurrentVoterRepair(&'a RecoveryRoleAdmission),
}
impl NativeImageInstallRole<'_> {
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        match self {
            Self::CommittedHandoff(role) => role.role(),
            Self::CurrentVoterRepair(role) => role,
        }
    }
    fn intended_configuration(
        &self,
    ) -> Result<(
        RangeDescriptor,
        BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    )> {
        let current = self.role();
        let mut range = current.current_range().clone();
        let mut members = current
            .member_credentials()
            .iter()
            .map(|(id, member)| (id.clone(), credentials(member)))
            .collect::<BTreeMap<_, _>>();
        if let Self::CommittedHandoff(role) = self {
            let plan = role.committed_relocation();
            let mut found = false;
            for replica in &mut range.replicas {
                if replica.id == plan.target_replica_id
                    && replica.node_id == plan.target
                    && replica.role == RangeReplicaRole::Learner
                {
                    replica.role = RangeReplicaRole::Voter;
                    found = true;
                }
            }
            if !found {
                return Err(range_write_error(
                    "fresh handoff has no exact target learner",
                ));
            }
            members.insert(
                plan.target.clone(),
                credentials(role.target_member_identity()),
            );
            if let Some(source) = &plan.source {
                range.replicas.retain(|replica| &replica.node_id != source);
                members.remove(source);
                if &range.leader == source {
                    range.leader = plan.target.clone();
                }
            }
            range.epoch = range
                .epoch
                .checked_add(2)
                .ok_or_else(|| range_write_error("activation epoch overflow"))?;
        }
        Ok((range, members))
    }
    fn require_scope(
        &self,
        purpose: &RangeImageSourcePurposeV1,
        session: Uuid,
        nonce: Uuid,
    ) -> Result<()> {
        match (self, purpose) {
            (
                Self::CommittedHandoff(role),
                RangeImageSourcePurposeV1::CommittedHandoff {
                    relocation_id,
                    plan_id,
                },
            ) => role.require_live_scope(*relocation_id, plan_id, session, nonce),
            (Self::CurrentVoterRepair(role), RangeImageSourcePurposeV1::CurrentVoterRepair) => {
                role.require_live_request()?;
                if !matches!(
                    role.purpose(),
                    crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
                ) || role.recovery_nonce() != nonce
                    || role.recovery_nonce() != session
                {
                    return Err(range_write_error(
                        "current-voter repair is not for this fresh recovery session",
                    ));
                }
                Ok(())
            }
            _ => Err(range_write_error(
                "image source purpose and fresh metadata role differ",
            )),
        }
    }
}

pub(super) fn require_intent_live_role(
    intent: &ImageInstallIntent,
    role: &NativeImageInstallRole<'_>,
) -> Result<()> {
    role.require_scope(&intent.source_purpose, intent.session_id, intent.nonce)?;
    let actual = role.role();
    let (intended_range, intended_members) = role.intended_configuration()?;
    if !semantic_range(&intended_range, &intent.intended_range)
        || intended_members != intent.intended_members
        || actual.cluster_id() != &intent.cluster_id
        || actual.bootstrap_id() != intent.bootstrap_id
        || !semantic_range(actual.current_range(), &intent.range)
        || actual
            .member_credentials()
            .iter()
            .map(|(id, member)| (id.clone(), credentials(member)))
            .collect::<BTreeMap<_, _>>()
            != intent.current_voter_credentials
        || credentials(actual.local_member_identity()) != intent.target_credentials
        || actual.restore_activation() != intent.restore_activation.as_ref()
        || actual.expected_schema_sha256() != Some(intent.schema_sha256.as_str())
        || actual.local_node_id() != &intent.target_node_id
        || actual.local_member_incarnation() != intent.target_member_incarnation
    {
        return Err(range_write_error(
            "image install fresh role/configuration changed",
        ));
    }
    Ok(())
}

/// Actual read-only transfer preflight. It grants no row mutation, catalog
/// floor or user admission, and is neither cloneable nor deserializable.
#[derive(Debug)]
pub(crate) struct NativeImageInstallTransferPreflight {
    store_open_id: Uuid,
    database_incarnation: String,
    original_freeze_owner: Uuid,
    intent: ImageInstallIntent,
    source_database_incarnation: String,
    original_control_commitment: String,
    original_native_phase_commitment: String,
    target_reader_features: Vec<String>,
    native_safety: NativeRangeInstallSafety,
    catalog: super::TargetImageCatalogHistoryObligations,
    saved_session: Option<super::RecoveryImageInstallSessionObservation>,
    cancellation: crate::CancellationToken,
}
impl NativeImageInstallTransferPreflight {
    pub(crate) fn range_id(&self) -> RangeId {
        self.intent.range.id
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.intent.nonce
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.intent.session_id
    }
    pub(crate) fn original_freeze_owner(&self) -> Uuid {
        self.original_freeze_owner
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_transfer_current(
        &self,
        store: &RangeWriteStore,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_current(
            store,
            database,
            native,
            source,
            role,
            frozen,
            self.original_freeze_owner,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_install_start(
        &self,
        store: &RangeWriteStore,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_current(
            store,
            database,
            native,
            source,
            role,
            frozen,
            self.intent.attempt_id,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn require_current(
        &self,
        store: &RangeWriteStore,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        expected_owner: Uuid,
    ) -> Result<()> {
        self.cancellation.check()?;
        source.require_current()?;
        self.native_safety
            .require_current(database, native, frozen)?;
        if self.original_control_commitment
            != store.image_transfer_control_commitment(self.range_id(), &self.cancellation)?
            || self.original_native_phase_commitment
                != database.native_image_install_phase_commitment(self.range_id(), frozen)?
            || self.target_reader_features
                != database.capture_range_image_bindings()?.reader_features()
        {
            return Err(range_write_error(
                "image transfer original target/control phase changed",
            ));
        }

        self.catalog.require_current(store)?;
        if let Some(saved) = &self.saved_session {
            saved.require_current(store, database, native, role.role())?;
        }
        let actual = store.candidate_native_image_install_intent(
            database,
            native,
            source,
            role,
            frozen,
            self.intent.purpose,
            self.intent.attempt_id,
            expected_owner,
        )?;
        if self.store_open_id != store.open_id
            || self.database_incarnation != native.database_incarnation()
            || self.source_database_incarnation != source.bindings().database_incarnation()
            || actual != self.intent
        {
            return Err(range_write_error(
                "image transfer preflight source/target/open/attempt changed",
            ));
        }
        store.validate_install_local_log(&self.intent.range, self.intent.source_range_index)?;
        self.cancellation.check()
    }
}

/// Freshly authorized return of an untouched transfer to its original frozen
/// owner. This never clears recovery/image refusal or grants rows/serving.
#[derive(Debug)]
pub(crate) struct UntouchedNativeImageTransferReturn {
    store_open_id: Uuid,
    database_incarnation: String,
    original_freeze_owner: Uuid,
    intent: ImageInstallIntent,
    control_commitment: String,
    native_phase_commitment: String,
    target_reader_features: Vec<String>,
    native_safety: NativeRangeInstallSafety,
    catalog: super::TargetImageCatalogHistoryObligations,
    saved_session: Option<super::RecoveryImageInstallSessionObservation>,
    cancellation: crate::CancellationToken,
}
impl UntouchedNativeImageTransferReturn {
    pub(crate) fn range_id(&self) -> RangeId {
        self.intent.range.id
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.intent.nonce
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.intent.session_id
    }
    pub(crate) fn original_freeze_owner(&self) -> Uuid {
        self.original_freeze_owner
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.cancellation.check()?;
        require_intent_live_role(&self.intent, role)?;
        native.require_current(database)?;
        self.native_safety
            .require_current(database, native, frozen)?;
        self.catalog.require_current(store)?;
        if let Some(saved) = &self.saved_session {
            saved.require_current(store, database, native, role.role())?;
        }
        store.require_no_native_image_catalogue_adoption_for_attempt(
            self.range_id(),
            self.attempt_id(),
        )?;
        if store
            .catalog
            .image_install_intents
            .get(&self.range_id())
            .is_some_and(|intent| intent.attempt_id == self.attempt_id())
        {
            return Err(range_write_error(
                "this attempt has persisted an image intent",
            ));
        }
        database.require_no_native_image_install_state_for_attempt(
            self.range_id(),
            self.attempt_id(),
            frozen,
        )?;
        let bindings = database.capture_range_image_bindings()?;
        if self.store_open_id != store.open_id
            || self.database_incarnation != native.database_incarnation()
            || self.intent.target_birth_id != native.birth_id()
            || self.intent.target_birth_sha256 != native.birth_sha256()
            || frozen.owner() != self.attempt_id()
            || bindings.namespace_ids() != self.intent.target_namespace_ids
            || bindings.schema_sha256() != self.intent.schema_sha256
            || bindings.reader_features() != self.target_reader_features
            || self.control_commitment
                != store.image_transfer_control_commitment(self.range_id(), &self.cancellation)?
            || self.native_phase_commitment
                != database.native_image_install_phase_commitment(self.range_id(), frozen)?
        {
            return Err(range_write_error(
                "untouched transfer target/control facts changed",
            ));
        }
        self.cancellation.check()
    }
}

/// A token for this exact store open, native DB open and durable attempt.
/// Reopening requires a fresh role observation and a new persistence proof.
#[derive(Debug)]
pub(crate) struct TargetImageInstallFence {
    store_open_id: Uuid,
    database_incarnation: String,
    intent: ImageInstallIntent,
    intent_sha256: String,
    replaced_installing: Option<ReplacedInstallingIdentity>,
}
impl TargetImageInstallFence {
    pub(crate) fn matches_replaced_installing_marker(
        &self,
        session: Uuid,
        attempt: Uuid,
        sha: &str,
        r: u64,
    ) -> bool {
        self.replaced_installing
            .as_ref()
            .is_some_and(|p| p.matches(session, attempt, sha, r))
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.intent.range
    }
    pub(crate) fn purpose(&self) -> ImageInstallPurpose {
        self.intent.purpose
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.intent.session_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.intent.nonce
    }
    pub(crate) fn source_purpose(&self) -> &RangeImageSourcePurposeV1 {
        &self.intent.source_purpose
    }
    pub(crate) fn source_range_index(&self) -> u64 {
        self.intent.source_range_index
    }
    pub(crate) fn source_id(&self) -> &ClusterNodeId {
        &self.intent.source_id
    }
    pub(crate) fn source_incarnation(&self) -> u64 {
        self.intent.source_incarnation
    }
    pub(crate) fn protected_proposal(&self) -> Option<&super::NativeCurrentImageProposalPointer> {
        self.intent.protected_proposal.as_ref()
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.intent_sha256
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.intent.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.intent.bootstrap_id
    }
    pub(crate) fn target_node_id(&self) -> &ClusterNodeId {
        &self.intent.target_node_id
    }
    pub(crate) fn target_member_incarnation(&self) -> u64 {
        self.intent.target_member_incarnation
    }
    pub(crate) fn target_birth_id(&self) -> Uuid {
        self.intent.target_birth_id
    }
    pub(crate) fn target_birth_sha256(&self) -> &str {
        &self.intent.target_birth_sha256
    }
    pub(crate) fn target_namespace_ids(&self) -> &[(String, Uuid)] {
        &self.intent.target_namespace_ids
    }
    pub(crate) fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn old_config_sha256(&self) -> &str {
        &self.intent.old_config_sha256
    }
    pub(crate) fn intended_config_sha256(&self) -> &str {
        &self.intent.intended_config_sha256
    }
    pub(crate) fn intended_activation_range(&self) -> &RangeDescriptor {
        &self.intent.intended_range
    }
    pub(crate) fn intended_activation_members(
        &self,
    ) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.intent.intended_members
    }
    pub(crate) fn activation_epoch(&self) -> u64 {
        self.intent.activation_epoch
    }
    pub(crate) fn source_catalog_sha256(&self) -> &str {
        &self.intent.source_catalog_sha256
    }
    pub(crate) fn source_native_receipt_sha256(&self) -> &str {
        &self.intent.source_native_receipt_sha256
    }

    /// This check is deliberately not a lease: each caller supplies a live,
    /// purpose-bound fresh observation before its next bounded operation.
    pub(crate) fn require_live_role(&self, role: &NativeImageInstallRole<'_>) -> Result<()> {
        require_intent_live_role(&self.intent, role)
    }
}

impl RangeWriteStore {
    fn image_transfer_control_commitment(
        &self,
        range_id: RangeId,
        cancellation: &crate::CancellationToken,
    ) -> Result<String> {
        struct HashWriter<'a> {
            hash: Sha256,
            bytes: usize,
            cancellation: &'a crate::CancellationToken,
        }
        impl std::io::Write for HashWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.cancellation
                    .check()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "image control commitment overflow",
                    )
                })?;
                if self.bytes > 32 * 1024 * 1024 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "image control commitment exceeds bound",
                    ));
                }
                self.hash.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        cancellation.check()?;
        let mut writer = HashWriter {
            hash: Sha256::new(),
            bytes: 0,
            cancellation,
        };
        let encoded = serde_json::to_writer(
            &mut writer,
            &(
                "bicdb.image-transfer-control.v1",
                &self.catalog.cluster_id,
                &self.catalog.node_id,
                range_id,
                self.catalog.image_install_intents.get(&range_id),
                self.catalog.native_image_bases.get(&range_id),
                self.catalog.recovery_fences.get(&range_id),
                self.catalog.handoff_promises.get(&range_id),
            ),
        );
        cancellation.check()?;
        encoded?;
        if let Some(records) = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .and_then(|ranges| ranges.get(&range_id))
            .filter(|records| !records.is_empty())
        {
            serde_json::to_writer(&mut writer, &("sparse-committed-history-v1", records))?;
        }
        cancellation.check()?;
        Ok(hex::encode(writer.hash.finalize()))
    }

    /// Capture real facts under Runtime's existing frozen owner. A later
    /// unique-owner lease rotates that SAME drained permit to this attempt;
    /// no caller-selected owner is accepted as a write authority.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn preflight_recovery_image_install(
        &mut self,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeImageInstallTransferPreflight> {
        let cancellation = parent.child_with_deadline(role.role().deadline());
        cancellation.check()?;
        let bindings = database.capture_range_image_bindings()?;
        if bindings.collections() != source.bindings().collections()
            || bindings.schema_sha256() != source.bindings().schema_sha256()
            || bindings.reader_features() != source.bindings().reader_features()
            || bindings
                .collections()
                .iter()
                .chain(source.bindings().collections())
                .any(|meta| meta.storage_name.is_some() || meta.renamed_from.is_some())
        {
            return Err(range_write_error(
                "image transfer requires exact supported preinstalled target schema/readers",
            ));
        }
        let candidate = self.candidate_native_image_install_intent(
            database,
            native,
            source,
            role,
            frozen,
            purpose,
            attempt_id,
            frozen.owner(),
        )?;
        let saved_session =
            self.require_native_install_recovery_preflight(database, native, role, &candidate)?;
        let native_safety =
            database.native_range_install_safety(native, candidate.range.id, frozen)?;
        if native_safety
            .maximum_irreversible_index()
            .is_some_and(|r| r > candidate.source_range_index)
            || native_safety
                .maximum_pending_index()
                .is_some_and(|r| r > candidate.source_range_index)
        {
            return Err(range_write_error(
                "image transfer source is below actual native irreversible/pending history",
            ));
        }
        self.validate_install_local_log(&candidate.range, candidate.source_range_index)?;
        let catalog = self.image_install_catalog_history_obligations(candidate.range.id)?;
        let token = NativeImageInstallTransferPreflight {
            store_open_id: self.open_id,
            database_incarnation: native.database_incarnation().to_string(),
            original_freeze_owner: frozen.owner(),
            source_database_incarnation: source.bindings().database_incarnation().to_string(),
            original_control_commitment: self
                .image_transfer_control_commitment(candidate.range.id, &cancellation)?,
            original_native_phase_commitment: database
                .native_image_install_phase_commitment(candidate.range.id, frozen)?,
            target_reader_features: bindings.reader_features().to_vec(),
            intent: candidate,
            native_safety,
            catalog,
            saved_session,
            cancellation,
        };
        token.require_transfer_current(self, database, native, source, role, frozen)?;
        Ok(token)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prove_untouched_image_transfer_return(
        &mut self,
        original: &NativeImageInstallTransferPreflight,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        parent: &crate::CancellationToken,
    ) -> Result<UntouchedNativeImageTransferReturn> {
        let cancellation = parent.child_with_deadline(role.role().deadline());
        cancellation.check()?;
        require_intent_live_role(&original.intent, role)?;
        native.require_current(database)?;
        // Fresh observation may re-fsync an unchanged prior refusal. It never
        // changes its nonce, native open, scope, floor, intent or physical data.
        let saved_session = match role {
            NativeImageInstallRole::CurrentVoterRepair(recovery) => {
                Some(self.observe_image_install_recovery_session(database, native, recovery)?)
            }
            NativeImageInstallRole::CommittedHandoff(_) => None,
        };
        if let Some(saved) = &original.saved_session {
            saved.require_current(self, database, native, role.role())?;
        }
        original
            .native_safety
            .require_current(database, native, frozen)?;
        original.catalog.require_current(self)?;
        if original.store_open_id != self.open_id
            || original.database_incarnation != native.database_incarnation()
            || original.original_control_commitment
                != self.image_transfer_control_commitment(original.range_id(), &cancellation)?
            || original.original_native_phase_commitment
                != database.native_image_install_phase_commitment(original.range_id(), frozen)?
        {
            return Err(range_write_error(
                "original transfer was changed; actual resume/adoption is required",
            ));
        }
        let proof = UntouchedNativeImageTransferReturn {
            store_open_id: self.open_id,
            database_incarnation: native.database_incarnation().to_string(),
            original_freeze_owner: original.original_freeze_owner,
            intent: original.intent.clone(),
            control_commitment: original.original_control_commitment.clone(),
            native_phase_commitment: original.original_native_phase_commitment.clone(),
            target_reader_features: original.target_reader_features.clone(),
            native_safety: database.native_range_install_safety(
                native,
                original.range_id(),
                frozen,
            )?,
            catalog: self.image_install_catalog_history_obligations(original.range_id())?,
            saved_session,
            cancellation,
        };
        proof.require_current(self, database, native, role, frozen)?;
        Ok(proof)
    }

    /// Reissue an exact unfinished catalog token after the native image has
    /// completed. This never clears/replaces rows or advances a numeric floor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reacquire_completed_native_image_install(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        source: &crate::distribution_range_image::CompletedNativeRangeImageSource,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        base: &crate::db::NativeInstalledRangeImageBase,
    ) -> Result<TargetImageInstallFence> {
        source.require_current()?;
        base.require_current(database, admission)?;
        let staging = source.authorize_target_install()?;
        let candidate = self.candidate_native_image_install_intent(
            database, admission, &staging, role, frozen, purpose, attempt_id, attempt_id,
        )?;
        self.require_native_install_recovery_preflight(database, admission, role, &candidate)?;
        let existing = self
            .catalog
            .image_install_intents
            .get(&candidate.range.id)
            .ok_or_else(|| range_write_error("completed image has no unfinished catalog intent"))?;
        let mut normalized = existing.clone();
        normalized.range = candidate.range.clone();
        if !semantic_range(&existing.range, &candidate.range)
            || normalized != candidate
            || base.attempt_id() != existing.attempt_id
            || base.intent_sha256() != existing.sha256()?
            || base.session_id() != existing.session_id
            || base.nonce() != existing.nonce
            || base.source_purpose() != &existing.source_purpose
            || base.range_index() != existing.source_range_index
            || base.image_root_sha256() != source.seal().image_root_sha256
            || base.portable_catalog_sha256() != source.seal().portable_catalog_sha256
        {
            return Err(range_write_error(
                "completed image does not match the exact retained source/catalog attempt",
            ));
        }
        self.validate_install_local_log(&existing.range, existing.source_range_index)?;
        let intent = existing.clone();
        self.persist()?;
        role.role().require_live_request()?;
        let fence = TargetImageInstallFence {
            replaced_installing: self.replaced_installing_identity(&intent)?,
            store_open_id: self.open_id,
            database_incarnation: admission.database_incarnation().to_string(),
            intent_sha256: intent.sha256()?,
            intent,
        };
        if matches!(role, NativeImageInstallRole::CurrentVoterRepair(_)) {
            self.rebind_recovery_open_after_image_fence(database, admission, role, &fence, frozen)?;
        }
        Ok(fence)
    }

    /// Refusal-only preflight before ANY new intent/marker. A fresh role cannot
    /// replace a saved nonce. Cold reopen may change only the ephemeral DB open
    /// and only when an actual pre-existing intent already binds this physical
    /// birth, unchanged local namespace IDs and exact saved recovery session.
    fn require_native_install_recovery_preflight(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        role: &NativeImageInstallRole<'_>,
        candidate: &ImageInstallIntent,
    ) -> Result<Option<super::RecoveryImageInstallSessionObservation>> {
        let NativeImageInstallRole::CurrentVoterRepair(recovery_role) = role else {
            return Ok(None);
        };
        let saved =
            self.observe_image_install_recovery_session(database, admission, recovery_role)?;
        saved.require_current(self, database, admission, recovery_role)?;
        if saved.saved_native_database_incarnation() != saved.current_native_database_incarnation()
        {
            let existing = self
                .catalog
                .image_install_intents
                .get(&candidate.range.id)
                .ok_or_else(|| {
                    range_write_error(
                        "cold image recovery cannot create an intent over a previous DB open",
                    )
                })?;
            if existing.target_namespace_ids != candidate.target_namespace_ids
                || existing.target_birth_id != saved.native_birth_id()
                || existing.target_birth_sha256 != saved.native_birth_sha256()
                || !same_install_lineage_for_new_attempt(existing, candidate)
            {
                return Err(range_write_error("cold image recovery requires the exact existing intent and unchanged native namespace lineage"));
            }
        }
        Ok(Some(saved))
    }

    /// All bounded physical install operations use the strict saved session.
    /// The lower token validator stays separate for the one controlled cold
    /// rebind, which changes only its saved ephemeral native DB incarnation.
    pub(crate) fn validate_native_image_install_step(
        &self,
        fence: &TargetImageInstallFence,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.validate_native_image_install_fence(fence, database, admission, role, frozen)?;
        if let NativeImageInstallRole::CurrentVoterRepair(recovery_role) = role {
            self.require_image_install_recovery_scope(recovery_role, admission)?;
        }
        Ok(())
    }

    pub(super) fn validate_install_local_log(
        &self,
        range: &RangeDescriptor,
        source_r: u64,
    ) -> Result<()> {
        if self.catalog.backup_fences.contains_key(&range.id) {
            return Err(range_write_error(
                "image install conflicts with a backup fence",
            ));
        }
        if self
            .sparse_irreversible_commands(range.id)?
            .keys()
            .any(|index| *index > source_r)
        {
            return Err(range_write_error(
                "image source is below permanent sparse committed history",
            ));
        }
        if let Some(log) = self.catalog.ranges.get(&range.id) {
            if log.current_epoch > range.epoch
                && self
                    .catalog
                    .handoff_promises
                    .get(&range.id)
                    .is_none_or(|promise| promise.old_epoch != range.epoch)
            {
                return Err(range_write_error("image source is below local range epoch"));
            }
            if source_r < log.resolved_through || source_r < log.compacted_through {
                return Err(range_write_error(
                    "image source is below locally resolved history",
                ));
            }
            // A local unresolved higher prepare might have acquired quorum on
            // unavailable peers. Do not guess that it may safely be erased.
            if log
                .entries
                .values()
                .any(|entry| entry.command.index > source_r)
            {
                return Err(range_write_error(
                    "image source is below a local certified or unresolved tail",
                ));
            }
            if log.last_index > source_r {
                return Err(range_write_error("image source is below local last index"));
            }
        }
        Ok(())
    }

    /// Catalogue format is global. A new protected range must not publish
    /// format11 while another legacy CurrentRepair intent lacks its proposal.
    /// Such a legacy range needs independent qualified recovery first.
    fn require_protected_proposal_upgrade_possible(&self, replacing: RangeId) -> Result<()> {
        if self
            .catalog
            .image_install_intents
            .iter()
            .any(|(range, intent)| {
                *range != replacing
                    && matches!(
                        intent.source_purpose,
                        RangeImageSourcePurposeV1::CurrentVoterRepair
                    )
                    && intent.protected_proposal.is_none()
            })
        {
            return Err(range_write_error("protected proposal upgrade would strand another legacy current-image intent; recover it before format11"));
        }
        Ok(())
    }

    /// Persist the catalog refusal before the native Installing marker or rows.
    /// The independently minted native safety observation is checked by the
    /// target installer under this same freeze immediately before this call.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_native_image_install(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        native_safety: &NativeRangeInstallSafety,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
    ) -> Result<TargetImageInstallFence> {
        self.begin_native_image_install_with(
            database,
            admission,
            native_safety,
            source,
            role,
            frozen,
            purpose,
            attempt_id,
            Self::persist,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn begin_native_image_install_with(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        native_safety: &NativeRangeInstallSafety,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        persist: impl FnOnce(&Self) -> Result<()>,
    ) -> Result<TargetImageInstallFence> {
        let mut intent = self.candidate_native_image_install_intent(
            database, admission, source, role, frozen, purpose, attempt_id, attempt_id,
        )?;
        if matches!(
            intent.source_purpose,
            RangeImageSourcePurposeV1::CurrentVoterRepair
        ) {
            self.require_protected_proposal_upgrade_possible(intent.range.id)?;
        }
        self.require_native_install_recovery_preflight(database, admission, role, &intent)?;
        let cut = source.cut();
        native_safety.require_current(database, admission, frozen)?;
        if native_safety.range_id() != cut.range().id
            || native_safety
                .maximum_irreversible_index()
                .is_some_and(|index| index > cut.range_index())
            || native_safety
                .maximum_pending_index()
                .is_some_and(|index| index > cut.range_index())
        {
            return Err(range_write_error(
                "image source would roll back actual native floor or pending mutation",
            ));
        }
        // This token is a durable REFUSAL only. It grants no row mutation:
        // full staged logical-history compatibility is a separate mandatory
        // prerequisite of the target row/final-publication state machine.
        self.validate_install_local_log(cut.range(), cut.range_index())?;
        if matches!(cut.purpose(), RangeImageSourcePurposeV1::CurrentVoterRepair) {
            let binding = super::NativeImageProposalIntentBinding::from_actual_candidate(
                self, admission, &intent,
            )?;
            let catalogue = self.image_install_catalog_history_obligations(intent.range.id)?;
            let proposal = database.persist_native_current_image_proposal(
                self,
                &binding,
                admission,
                native_safety,
                &catalogue,
                source,
                role,
                frozen,
            )?;
            if proposal.intent_core_sha256() != intent.core_sha256()? {
                return Err(range_write_error(
                    "protected proposal differs from exact BEGIN core",
                ));
            }
            intent.protected_proposal = Some(proposal.pointer());
        }
        self.invalidate_native_activation_open(intent.range.id);
        if let Some(existing) = self.catalog.image_install_intents.get(&intent.range.id) {
            if existing.attempt_id == intent.attempt_id
                && existing.target_namespace_ids != intent.target_namespace_ids
            {
                return Err(range_write_error(
                    "same native install attempt cannot retarget a recreated namespace",
                ));
            }
            if !same_install_lineage_for_new_attempt(existing, &intent) {
                return Err(range_write_error(
                    "new image attempt cannot replace another handoff lineage",
                ));
            }
            if existing.attempt_id == intent.attempt_id {
                intent = existing.clone();
            } else {
                // A completed physical image must be recovered/finalized via
                // its native proof, never destructively reclassified dirty.
                database.require_native_image_install_restartable(
                    existing.range.id,
                    existing.attempt_id,
                    &existing.sha256()?,
                )?;
                self.catalog
                    .image_install_intents
                    .insert(intent.range.id, intent.clone());
            }
        } else {
            if self.catalog.image_install_intents.len() >= MAX_INSTALL_INTENTS {
                return Err(range_write_error(
                    "image install intent count bound exceeded",
                ));
            }
            self.catalog
                .image_install_intents
                .insert(intent.range.id, intent.clone());
        }
        self.persist_native_install_intent_with(intent.clone(), persist)?;
        role.role().require_live_request()?;
        let fence = TargetImageInstallFence {
            replaced_installing: self.replaced_installing_identity(&intent)?,
            store_open_id: self.open_id,
            database_incarnation: admission.database_incarnation().to_string(),
            intent_sha256: intent.sha256()?,
            intent,
        };
        if matches!(role, NativeImageInstallRole::CurrentVoterRepair(_)) {
            self.rebind_recovery_open_after_image_fence(database, admission, role, &fence, frozen)?;
        }
        Ok(fence)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn test_current_proposal_without_intent_publication(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        native_safety: &NativeRangeInstallSafety,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        attempt_id: Uuid,
    ) -> Result<TargetImageInstallFence> {
        self.begin_native_image_install_with(
            database,
            admission,
            native_safety,
            source,
            role,
            frozen,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt_id,
            |_| {
                Err(range_write_error(
                    "injected before ANY catalogue intent publication",
                ))
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn candidate_native_image_install_intent(
        &self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
        expected_freeze_owner: Uuid,
    ) -> Result<ImageInstallIntent> {
        source.require_current()?;
        let cut = source.cut();
        role.require_scope(&cut.purpose(), cut.session_id(), cut.nonce())?;
        admission.require_current(database)?;
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(cut.range().id, frozen)?;
        if frozen.owner() != expected_freeze_owner
            || database.data_path().canonicalize()? != self.root.canonicalize()?
            || !self.fsync
            || !crate::format::range_image_install_active(&self.root)?
            || self.catalog.cluster_id != *cut.cluster_id()
            || self.catalog.node_id != *admission.local_node_id()
            || admission.cluster_id() != cut.cluster_id()
            || admission.bootstrap_id() != source.bootstrap_id()
        {
            return Err(range_write_error(
                "image install DB/store/freeze/native birth mismatch",
            ));
        }
        let current = role.role();
        let (intended_range, intended_members) = role.intended_configuration()?;
        if !semantic_range(&intended_range, cut.intended_activation_range())
            || &intended_members != cut.intended_activation_members()
            || !semantic_range(current.current_range(), cut.range())
            || current.expected_schema_sha256() != Some(source.bindings().schema_sha256())
            || current.restore_activation() != source.lineage().restore_activation()
            || current
                .member_credentials()
                .iter()
                .map(|(id, member)| (id.clone(), credentials(member)))
                .collect::<BTreeMap<_, _>>()
                != cut.capture_members()
            || current.cluster_id() != cut.cluster_id()
            || current.bootstrap_id() != source.bootstrap_id()
            || current.local_node_id() != admission.local_node_id()
            || current.local_member_incarnation() != admission.local_member_incarnation()
            || current.restore_activation() != admission.restore_activation()
        {
            return Err(range_write_error(
                "image install role is not for this exact native target/configuration",
            ));
        }
        let local_role = cut
            .range()
            .replicas
            .iter()
            .find(|replica| &replica.node_id == admission.local_node_id());
        let valid_purpose = match purpose {
            ImageInstallPurpose::RelocationLearner => {
                admission.local_node_id() == cut.target_id()
                    && admission.local_member_incarnation() == cut.target_incarnation()
                    && local_role.is_some_and(|replica| replica.role == RangeReplicaRole::Learner)
            }
            ImageInstallPurpose::CurrentVoterRepair => {
                local_role.is_some_and(|replica| replica.role == RangeReplicaRole::Voter)
                    && (!matches!(cut.purpose(), RangeImageSourcePurposeV1::CurrentVoterRepair)
                        || (admission.local_node_id() == cut.target_id()
                            && admission.local_member_incarnation() == cut.target_incarnation()))
            }
        };
        if !valid_purpose
            || (matches!(cut.purpose(), RangeImageSourcePurposeV1::CurrentVoterRepair)
                && purpose != ImageInstallPurpose::CurrentVoterRepair)
        {
            return Err(range_write_error(
                "image install target role/purpose mismatch",
            ));
        }
        let intent = ImageInstallIntent {
            cluster_id: cut.cluster_id().clone(),
            bootstrap_id: source.bootstrap_id(),
            range: canonical_range(cut.range()),
            purpose,
            attempt_id,
            session_id: cut.session_id(),
            nonce: cut.nonce(),
            source_purpose: cut.purpose(),
            target_node_id: admission.local_node_id().clone(),
            target_member_incarnation: admission.local_member_incarnation(),
            target_birth_id: admission.birth_id(),
            target_birth_sha256: admission.birth_sha256().to_string(),
            target_namespace_ids: database.capture_range_image_bindings()?.namespace_ids(),
            source_id: cut.source_id().clone(),
            source_incarnation: cut.source_incarnation(),
            source_range_index: cut.range_index(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_catalog_sha256: source.catalog_sha256().to_string(),
            old_config_sha256: cut.old_config_sha256().to_string(),
            intended_config_sha256: cut.new_config_sha256().to_string(),
            activation_epoch: cut.activation_epoch(),
            intended_range: canonical_range(&intended_range),
            intended_members,
            current_voter_credentials: current
                .member_credentials()
                .iter()
                .map(|(id, member)| (id.clone(), credentials(member)))
                .collect(),
            target_credentials: credentials(current.local_member_identity()),
            restore_activation: current.restore_activation().cloned(),
            schema_sha256: source.bindings().schema_sha256().to_string(),
            protected_proposal: None,
        };
        intent.validate()?;
        Ok(intent)
    }

    fn persist_native_install_intent_with(
        &mut self,
        intent: ImageInstallIntent,
        persist: impl FnOnce(&Self) -> Result<()>,
    ) -> Result<()> {
        intent.validate()?;
        let protected = intent.protected_proposal.is_some();
        self.catalog
            .image_install_intents
            .insert(intent.range.id, intent);
        self.catalog.format_version = self.catalog.format_version.max(if protected {
            super::IMAGE_PROPOSAL_CATALOG_FORMAT
        } else {
            IMAGE_INSTALL_RANGE_LOG_FORMAT
        });
        if protected {
            // Format10+ requires the explicit sparse field even when empty.
            self.catalog
                .sparse_committed_decisions
                .get_or_insert_with(BTreeMap::new);
        }
        // write_atomic may rename before returning a directory-sync error.
        // Never restore the previous permissive in-memory catalog on failure.
        persist(self)
    }

    pub(crate) fn validate_native_image_install_fence(
        &self,
        fence: &TargetImageInstallFence,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        fence.require_live_role(role)?;
        admission.require_current(database)?;
        if let Some(pointer) = &fence.intent.protected_proposal {
            let protected = database.read_native_current_image_proposal(admission, pointer)?;
            if protected.intent_core_sha256() != fence.intent.core_sha256()? {
                return Err(range_write_error(
                    "image fence protected proposal/core mismatch",
                ));
            }
        }
        database
            .range_mutation_gate()
            .validate_frozen(fence.range().id, frozen)?;
        if fence.store_open_id != self.open_id
            || fence.database_incarnation != admission.database_incarnation()
            || database.data_path().canonicalize()? != self.root.canonicalize()?
            || admission.cluster_id() != fence.cluster_id()
            || admission.bootstrap_id() != fence.bootstrap_id()
            || admission.local_node_id() != fence.target_node_id()
            || admission.local_member_incarnation() != fence.target_member_incarnation()
            || admission.birth_id() != fence.target_birth_id()
            || admission.birth_sha256() != fence.target_birth_sha256()
            || database.capture_range_image_bindings()?.namespace_ids()
                != fence.intent.target_namespace_ids
            || frozen.owner() != fence.attempt_id()
            || self.catalog.image_install_intents.get(&fence.range().id) != Some(&fence.intent)
        {
            return Err(range_write_error(
                "stale image install store/DB/open/attempt token",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{PlacementPolicy, RangeReplica};

    fn node(name: &str) -> ClusterNodeId {
        ClusterNodeId::new(name).unwrap()
    }
    fn range() -> RangeDescriptor {
        RangeDescriptor {
            id: RangeId::new(1).unwrap(),
            start_token: 0,
            end_token: None,
            epoch: 1,
            replicas: vec![
                RangeReplica {
                    id: crate::ReplicaId::new(1).unwrap(),
                    node_id: node("n1"),
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: crate::ReplicaId::new(2).unwrap(),
                    node_id: node("n2"),
                    role: RangeReplicaRole::Learner,
                },
            ],
            leader: node("n1"),
            approximate_bytes: 0,
            approximate_qps: 0,
            placement: PlacementPolicy::default(),
        }
    }
    fn command(index: u64) -> RangeWriteCommand {
        let mut command = RangeWriteCommand {
            protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
            cluster_id: ClusterId::new("image-intent-storage-unit").unwrap(),
            range_id: range().id,
            range_epoch: 1,
            index,
            command_id: format!("command-{index}"),
            leader_node_id: node("n1"),
            transaction_id: index,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: format!("k{index}"),
                record: Some(crate::Record::new(format!("k{index}"))),
            }],
            checksum_sha256: String::new(),
        };
        command.checksum_sha256 = command.calculate_checksum().unwrap();
        command
    }
    // Synthetic persistence DTO only: these tests NEVER construct a live
    // SourceAuthorization, role, fence token, Verified install or data floor.
    fn intent(source_r: u64) -> ImageInstallIntent {
        let member = crate::db::RangeImageBirthMember {
            incarnation: 1,
            tls_certificate_sha256: None,
            pending_tls_certificate_sha256: None,
        };
        ImageInstallIntent {
            cluster_id: command(1).cluster_id,
            bootstrap_id: Uuid::now_v7(),
            range: range(),
            purpose: ImageInstallPurpose::RelocationLearner,
            attempt_id: Uuid::now_v7(),
            session_id: Uuid::now_v7(),
            nonce: Uuid::now_v7(),
            source_purpose: RangeImageSourcePurposeV1::CommittedHandoff {
                relocation_id: crate::RelocationId::new(1).unwrap(),
                plan_id: "storage-unit-not-authority".into(),
            },
            target_node_id: node("n2"),
            target_member_incarnation: 1,
            target_birth_id: Uuid::now_v7(),
            target_birth_sha256: "a".repeat(64),
            target_namespace_ids: vec![("items".to_string(), Uuid::now_v7())],
            source_id: node("n1"),
            source_incarnation: 1,
            source_range_index: source_r,
            source_native_receipt_sha256: "b".repeat(64),
            source_catalog_sha256: "c".repeat(64),
            old_config_sha256: "d".repeat(64),
            intended_config_sha256: "e".repeat(64),
            activation_epoch: 3,
            intended_range: {
                let mut future = range();
                future.epoch = 3;
                future
                    .replicas
                    .retain(|replica| replica.node_id == node("n2"));
                future.replicas[0].role = RangeReplicaRole::Voter;
                future.leader = node("n2");
                future
            },
            intended_members: BTreeMap::from([(node("n2"), member.clone())]),
            current_voter_credentials: BTreeMap::from([(node("n1"), member.clone())]),
            target_credentials: member,
            restore_activation: None,
            schema_sha256: "f".repeat(64),
            protected_proposal: None,
        }
    }
    fn store(root: &Path) -> RangeWriteStore {
        crate::format::enable_range_image_install(root).unwrap();
        RangeWriteStore::open(root, command(1).cluster_id, node("n2"), true).unwrap()
    }
    #[test]
    fn protected_proposal_global_upgrade_refuses_other_legacy_range_without_writing() {
        // Lower storage compatibility fixture only: this is a valid historical
        // refusal DTO, not a source-cut/role/replication authority constructor.
        let root = tempfile::tempdir().unwrap();
        let mut local = store(root.path());
        let legacy_id = RangeId::new(2).unwrap();
        let mut legacy = intent(0);
        legacy.range.id = legacy_id;
        legacy.range.replicas[1].role = RangeReplicaRole::Voter;
        legacy.purpose = ImageInstallPurpose::CurrentVoterRepair;
        legacy.source_purpose = RangeImageSourcePurposeV1::CurrentVoterRepair;
        legacy.intended_range = legacy.range.clone();
        legacy.activation_epoch = legacy.range.epoch;
        legacy.intended_config_sha256 = legacy.old_config_sha256.clone();
        legacy.protected_proposal = None;
        legacy.validate().unwrap();
        local
            .catalog
            .image_install_intents
            .insert(legacy_id, legacy);
        local.catalog.format_version = super::super::SPARSE_HISTORY_CATALOG_FORMAT;
        local.catalog.sparse_committed_decisions = Some(BTreeMap::new());
        local.persist().unwrap();
        let before = std::fs::read(root.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
        let error = local
            .require_protected_proposal_upgrade_possible(RangeId::new(1).unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("strand another legacy"));
        assert_eq!(
            std::fs::read(root.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
            before
        );
        assert!(!crate::format::current_image_proposal_active(root.path()).unwrap());
        assert_eq!(
            local.catalog.format_version,
            super::super::SPARSE_HISTORY_CATALOG_FORMAT
        );
        drop(local);
        let reopened = store(root.path());
        assert!(reopened.catalog.image_install_intents[&legacy_id]
            .protected_proposal
            .is_none());
        // Same-range replacement still requires the real actual source/native
        // producer; passing THIS read-only preflight publishes nothing.
        reopened
            .require_protected_proposal_upgrade_possible(legacy_id)
            .unwrap();
        assert_eq!(
            std::fs::read(root.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
            before
        );
    }

    #[test]
    fn install_intent_reopen_blocks_all_ordinary_decisions_without_changing_watermarks() {
        let root = tempfile::tempdir().unwrap();
        let mut local = store(root.path());
        let first = command(1);
        local.prepare(first.clone()).unwrap();
        local.commit(&first).unwrap();
        local.certify(&first).unwrap();
        let before = local.progress(range().id);
        local
            .persist_native_install_intent_with(intent(1), RangeWriteStore::persist)
            .unwrap();
        drop(local);
        let mut local = store(root.path());
        assert_eq!(local.catalog.format_version, IMAGE_INSTALL_RANGE_LOG_FORMAT);
        assert_eq!(local.progress(range().id), before);
        assert!(local.prepare(command(2)).is_err());
        assert!(local.commit(&first).is_err());
        assert!(local.certify(&first).is_err());
        assert!(local.mark_applied(&first).is_err());
        assert!(local.abort(&first).is_err());
        assert_eq!(
            local.probe(range().id, 1, 1).unwrap().entry.unwrap().state,
            RangeWriteState::QuorumCommitted
        );
        assert!(
            RangeWriteStore::require_no_native_image_install_on_disk(root.path(), range().id)
                .is_err()
        );
        assert!(
            RangeWriteStore::open(root.path(), command(1).cluster_id, node("n2"), false).is_err()
        );
    }
    #[test]
    fn image_cut_cannot_erase_any_local_unresolved_certified_or_resolved_tail() {
        for state in [
            RangeWriteState::Prepared,
            RangeWriteState::Committed,
            RangeWriteState::QuorumCommitted,
            RangeWriteState::Applied,
            RangeWriteState::Aborted,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut local = store(root.path());
            let first = command(1);
            local.prepare(first.clone()).unwrap();
            if matches!(
                state,
                RangeWriteState::Committed
                    | RangeWriteState::QuorumCommitted
                    | RangeWriteState::Applied
            ) {
                local.commit(&first).unwrap();
            }
            if matches!(
                state,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            ) {
                local.certify(&first).unwrap();
            }
            if state == RangeWriteState::Applied {
                local.mark_applied(&first).unwrap();
            }
            if state == RangeWriteState::Aborted {
                local.abort(&first).unwrap();
            }
            assert!(
                local.validate_install_local_log(&range(), 0).is_err(),
                "state={state:?}"
            );
            local.validate_install_local_log(&range(), 1).unwrap();
        }
    }
    #[test]
    fn post_rename_install_error_retains_refusal_and_retry_must_fsync_again() {
        let root = tempfile::tempdir().unwrap();
        let mut local = store(root.path());
        let candidate = intent(0);
        let error = local.persist_native_install_intent_with(candidate.clone(), |store| {
            store.persist_with(|path, bytes, fsync| {
                assert!(fsync);
                let stage = path.with_extension("unit-post-rename");
                let mut file = File::create(&stage)?;
                std::io::Write::write_all(&mut file, bytes)?;
                file.sync_all()?;
                fs::rename(stage, path)?;
                Err(std::io::Error::other("injected after rename before directory fsync").into())
            })
        });
        assert!(error.is_err());
        assert!(local.require_no_image_install(range().id).is_err());
        assert!(store(root.path())
            .require_no_image_install(range().id)
            .is_err());
        let failed = local.persist_native_install_intent_with(candidate.clone(), |_| {
            Err(std::io::Error::other("retry failed").into())
        });
        assert!(failed.is_err());
        assert!(local.require_no_image_install(range().id).is_err());
        let mut called = false;
        local
            .persist_native_install_intent_with(candidate, |store| {
                assert!(store.fsync);
                called = true;
                store.persist()
            })
            .unwrap();
        assert!(called);
        assert!(store(root.path())
            .require_no_image_install(range().id)
            .is_err());
    }
}
