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

fn semantic_range(left: &RangeDescriptor, right: &RangeDescriptor) -> bool {
    let mut a = left.clone();
    let mut b = right.clone();
    a.approximate_bytes = 0;
    a.approximate_qps = 0;
    b.approximate_bytes = 0;
    b.approximate_qps = 0;
    a.replicas.sort_by_key(|replica| replica.id);
    b.replicas.sort_by_key(|replica| replica.id);
    a == b
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
                    || role.business_request_id() != session
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

/// A token for this exact store open, native DB open and durable attempt.
/// Reopening requires a fresh role observation and a new persistence proof.
#[derive(Debug)]
pub(crate) struct TargetImageInstallFence {
    store_open_id: Uuid,
    database_incarnation: String,
    intent: ImageInstallIntent,
    intent_sha256: String,
}
impl TargetImageInstallFence {
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
    pub(crate) fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn old_config_sha256(&self) -> &str {
        &self.intent.old_config_sha256
    }
    pub(crate) fn intended_config_sha256(&self) -> &str {
        &self.intent.intended_config_sha256
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
        role.require_scope(self.source_purpose(), self.session_id(), self.nonce())?;
        let actual = role.role();
        let (intended_range, intended_members) = role.intended_configuration()?;
        if !semantic_range(&intended_range, &self.intent.intended_range)
            || intended_members != self.intent.intended_members
            || actual.cluster_id() != self.cluster_id()
            || actual.bootstrap_id() != self.bootstrap_id()
            || !semantic_range(actual.current_range(), self.range())
            || actual
                .member_credentials()
                .iter()
                .map(|(id, member)| (id.clone(), credentials(member)))
                .collect::<BTreeMap<_, _>>()
                != self.intent.current_voter_credentials
            || credentials(actual.local_member_identity()) != self.intent.target_credentials
            || actual.restore_activation() != self.intent.restore_activation.as_ref()
            || actual.expected_schema_sha256() != Some(self.intent.schema_sha256.as_str())
            || actual.local_node_id() != self.target_node_id()
            || actual.local_member_incarnation() != self.target_member_incarnation()
        {
            return Err(range_write_error(
                "image install fresh role/configuration changed",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
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
            database, admission, &staging, role, frozen, purpose, attempt_id,
        )?;
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
        if existing.source_range_index != 0 {
            return Err(range_write_error(
                "nonzero completed image recovery requires typed logical-history compatibility",
            ));
        }
        self.validate_install_local_log(&existing.range, existing.source_range_index)?;
        let intent = existing.clone();
        self.persist()?;
        role.role().require_live_request()?;
        Ok(TargetImageInstallFence {
            store_open_id: self.open_id,
            database_incarnation: admission.database_incarnation().to_string(),
            intent_sha256: intent.sha256()?,
            intent,
        })
    }

    fn validate_install_local_log(&self, range: &RangeDescriptor, source_r: u64) -> Result<()> {
        if self.catalog.backup_fences.contains_key(&range.id) {
            return Err(range_write_error(
                "image install conflicts with a backup fence",
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
        let mut intent = self.candidate_native_image_install_intent(
            database, admission, source, role, frozen, purpose, attempt_id,
        )?;
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
        // R parity is not a history-compatibility proof. Until the opaque
        // shared logical-history adapter is integrated, refuse nonempty
        // certified prefixes rather than erasing an equal-index fork.
        if cut.range_index() != 0
            || native_safety
                .maximum_irreversible_index()
                .is_some_and(|index| index > 0)
        {
            return Err(range_write_error("nonzero native image install requires the typed logical-history compatibility adapter"));
        }
        self.validate_install_local_log(cut.range(), cut.range_index())?;
        if let Some(existing) = self.catalog.image_install_intents.get(&intent.range.id) {
            let mut normalized = existing.clone();
            normalized.range = intent.range.clone();
            normalized.attempt_id = intent.attempt_id;
            if !semantic_range(&existing.range, &intent.range) || normalized != intent {
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
        self.persist_native_install_intent_with(intent.clone(), Self::persist)?;
        role.role().require_live_request()?;
        Ok(TargetImageInstallFence {
            store_open_id: self.open_id,
            database_incarnation: admission.database_incarnation().to_string(),
            intent_sha256: intent.sha256()?,
            intent,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn candidate_native_image_install_intent(
        &self,
        database: &crate::BicDb,
        admission: &NativeRecoveryAdmission,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        frozen: &FrozenRangeMutationPermit,
        purpose: ImageInstallPurpose,
        attempt_id: Uuid,
    ) -> Result<ImageInstallIntent> {
        source.require_current()?;
        let cut = source.cut();
        role.require_scope(&cut.purpose(), cut.session_id(), cut.nonce())?;
        admission.require_current(database)?;
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(cut.range().id, frozen)?;
        if frozen.owner() != attempt_id
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
            range: cut.range().clone(),
            purpose,
            attempt_id,
            session_id: cut.session_id(),
            nonce: cut.nonce(),
            source_purpose: cut.purpose(),
            target_node_id: admission.local_node_id().clone(),
            target_member_incarnation: admission.local_member_incarnation(),
            target_birth_id: admission.birth_id(),
            target_birth_sha256: admission.birth_sha256().to_string(),
            source_id: cut.source_id().clone(),
            source_incarnation: cut.source_incarnation(),
            source_range_index: cut.range_index(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_catalog_sha256: source.catalog_sha256().to_string(),
            old_config_sha256: cut.old_config_sha256().to_string(),
            intended_config_sha256: cut.new_config_sha256().to_string(),
            activation_epoch: cut.activation_epoch(),
            intended_range,
            intended_members,
            current_voter_credentials: current
                .member_credentials()
                .iter()
                .map(|(id, member)| (id.clone(), credentials(member)))
                .collect(),
            target_credentials: credentials(current.local_member_identity()),
            restore_activation: current.restore_activation().cloned(),
            schema_sha256: source.bindings().schema_sha256().to_string(),
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
        self.catalog
            .image_install_intents
            .insert(intent.range.id, intent);
        self.catalog.format_version = self
            .catalog
            .format_version
            .max(IMAGE_INSTALL_RANGE_LOG_FORMAT);
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
        }
    }
    fn store(root: &Path) -> RangeWriteStore {
        crate::format::enable_range_image_install(root).unwrap();
        RangeWriteStore::open(root, command(1).cluster_id, node("n2"), true).unwrap()
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
