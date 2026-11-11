//! Durable recovery refusals and non-serializable observations of actual local
//! catalogue state. No record/number here grants native visibility or readiness.
use super::*;
use crate::db::NativeRecoveryAdmission;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

pub(super) const MAX_RECOVERY_FENCES: usize = 65_536;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryFenceScope {
    pub protocol_version: u32,
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub initiator_node_id: ClusterNodeId,
    pub initiator_incarnation: u64,
    pub recovery_nonce: Uuid,
    pub business_request_id: Uuid,
    pub metadata_context_id: Uuid,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub configuration_sha256: String,
    pub range_configuration_sha256: String,
    pub purpose: MetadataRecoveryRolePurpose,
}
impl RecoveryFenceScope {
    pub(crate) fn freeze_owner(&self) -> Result<Uuid> {
        self.validate()?;
        match self.purpose {
            MetadataRecoveryRolePurpose::NormalRecovery => Ok(self.recovery_nonce),
            MetadataRecoveryRolePurpose::CommittedHandoff { session_id, .. }
                if !session_id.is_nil() =>
            {
                Ok(session_id)
            }
            _ => Err(range_write_error(
                "invalid read-only or missing handoff freeze owner",
            )),
        }
    }
    pub(crate) fn from_role(role: &RecoveryRoleAdmission) -> Result<Self> {
        role.require_live_request()?;
        Ok(Self {
            protocol_version: 1,
            cluster_id: role.cluster_id().clone(),
            bootstrap_id: role.bootstrap_id(),
            range_id: role.current_range().id,
            range_epoch: role.current_range().epoch,
            initiator_node_id: role.initiator_node_id().clone(),
            initiator_incarnation: role.initiator_incarnation(),
            recovery_nonce: role.recovery_nonce(),
            business_request_id: role.business_request_id(),
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            configuration_sha256: role.configuration_sha256()?,
            range_configuration_sha256: role.range_configuration_sha256()?,
            purpose: role.purpose().clone(),
        })
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != 1
            || self.bootstrap_id.is_nil()
            || self.range_epoch == 0
            || self.initiator_incarnation == 0
            || self.recovery_nonce.is_nil()
            || self.business_request_id.is_nil()
            || self.metadata_context_id.is_nil()
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || matches!(
                self.purpose,
                MetadataRecoveryRolePurpose::ObserveFencedRecovery { .. }
            )
            || self.configuration_sha256.len() != 64
            || self.range_configuration_sha256.len() != 64
            || !self
                .range_configuration_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || !self
                .configuration_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(range_write_error("invalid recovery fence scope"));
        }
        Ok(())
    }
    pub(crate) fn require_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.validate()?;
        role.require_live_request()?;
        if &self.cluster_id != role.cluster_id()
            || self.bootstrap_id != role.bootstrap_id()
            || self.range_id != role.current_range().id
            || self.range_epoch != role.current_range().epoch
            || &self.initiator_node_id != role.initiator_node_id()
            || self.initiator_incarnation != role.initiator_incarnation()
            || self.recovery_nonce != role.recovery_nonce()
            || &self.purpose != role.purpose()
            || self.configuration_sha256 != role.configuration_sha256()?
            || self.range_configuration_sha256 != role.range_configuration_sha256()?
            || self.metadata_term > role.metadata_term()
            || self.metadata_read_index > role.metadata_read_index()
        {
            return Err(range_write_error(
                "recovery fence does not bind the independently fresh role",
            ));
        }
        Ok(())
    }
    /// A durable recovery session outlives one fresh metadata observation.
    /// Request UUIDs/contexts are provenance, never a renewable time lease.
    pub(crate) fn same_session(&self, other: &Self) -> bool {
        self.cluster_id == other.cluster_id
            && self.bootstrap_id == other.bootstrap_id
            && self.range_id == other.range_id
            && self.range_epoch == other.range_epoch
            && self.initiator_node_id == other.initiator_node_id
            && self.initiator_incarnation == other.initiator_incarnation
            && self.recovery_nonce == other.recovery_nonce
            && self.configuration_sha256 == other.configuration_sha256
            && self.range_configuration_sha256 == other.range_configuration_sha256
            && self.purpose == other.purpose
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum RecoveryFencePhase {
    Active,
    /// Durable promise to stop historical mutation at an ACTUALLY materialized
    /// point. This is neither a serving configuration nor quorum completion.
    NativePrepared(super::native_ready_prepare::NativePreparedPoint),
    Completed {
        resolved_index: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct RecoveryFenceRecord {
    pub(super) scope: RecoveryFenceScope,
    pub(super) phase: RecoveryFencePhase,
    pub(super) local_node_id: ClusterNodeId,
    pub(super) local_member_incarnation: u64,
    pub(super) native_database_incarnation: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(super) historical_certifications: BTreeMap<u64, HistoricalRangeCertification>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(super) retired_nonces: BTreeSet<Uuid>,
}
/// Persisted observation, not a transferable certificate/Apply admission.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalRangeCertification {
    pub recovery_nonce: Uuid,
    pub configuration_sha256: String,
    pub range_epoch: u64,
    pub index: u64,
    pub command_sha256: String,
}
impl RecoveryFenceRecord {
    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if let RecoveryFencePhase::NativePrepared(point) = &self.phase {
            point.validate()?;
        }
        if self.retired_nonces.len() > 256
            || self.retired_nonces.contains(&self.scope.recovery_nonce)
            || self.retired_nonces.iter().any(Uuid::is_nil)
        {
            return Err(range_write_error("damaged/bounded retired recovery nonces"));
        }
        if self.local_member_incarnation == 0 || self.native_database_incarnation.is_empty() {
            return Err(range_write_error("damaged recovery refusal record"));
        }
        if self.historical_certifications.len() > 1024
            || self
                .historical_certifications
                .iter()
                .any(|(index, marker)| {
                    *index == 0
                        || marker.index != *index
                        || marker.recovery_nonce != self.scope.recovery_nonce
                        || marker.configuration_sha256 != self.scope.configuration_sha256
                        || marker.range_epoch != self.scope.range_epoch
                        || marker.command_sha256.len() != 64
                        || !marker.command_sha256.bytes().all(|b| b.is_ascii_hexdigit())
                })
        {
            return Err(range_write_error(
                "damaged/bounded historical certification markers",
            ));
        }
        Ok(())
    }
}

/// Real store-open-bound prefix observation. There is deliberately no numeric
/// constructor, Deserialize, Clone or conversion from a repair DTO.
#[derive(Debug)]
pub(crate) struct RecoveryResolvedCatalogObservation {
    store_open_id: Uuid,
    node_id: ClusterNodeId,
    scope: RecoveryFenceScope,
    resolved_index: u64,
    compacted_through: u64,
    catalog_sha256: String,
    local_role_context_id: Uuid,
}

/// Actual saved-session state for target-install preflight, NOT permission to
/// bypass a DB-open mismatch or install rows. Only an independently validated
/// existing target intent can justify a cold-open rebind.
#[derive(Debug)]
pub(crate) struct RecoveryImageInstallSessionObservation {
    store_open_id: Uuid,
    scope: RecoveryFenceScope,
    saved_native_database_incarnation: String,
    current_native_database_incarnation: String,
    native_birth_id: Uuid,
    native_birth_sha256: String,
    range_catalog_sha256: String,
}

/// Exact actual local catalogue preflight for a physical historical Apply.
/// It carries no native floor and cannot be constructed from remote cert DTOs.
#[derive(Debug)]
pub(crate) struct RecoveryLocalHistoricalApplyObservation {
    store_open_id: Uuid,
    native_database_incarnation: String,
    native_birth_id: Uuid,
    role_context_id: Uuid,
    scope: RecoveryFenceScope,
    command_id: String,
    command_sha256: String,
    index: u64,
    catalog_sha256: String,
}
impl RecoveryLocalHistoricalApplyObservation {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        &self.native_database_incarnation
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.scope.range_id
    }
    pub(crate) fn command_id(&self) -> &str {
        &self.command_id
    }
    pub(crate) fn command_sha256(&self) -> &str {
        &self.command_sha256
    }
    pub(crate) fn command_index(&self) -> u64 {
        self.index
    }
    pub(crate) fn role_context_id(&self) -> Uuid {
        self.role_context_id
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &crate::distribution_range_recovery::RecoveryApplyAdmission,
    ) -> Result<()> {
        native.require_current(db)?;
        admission.require_recipient_role(role)?;
        store.require_local_historical_apply_state(role, native, admission)?;
        if self.store_open_id != store.open_id
            || self.native_database_incarnation != native.database_incarnation()
            || self.native_birth_id != native.birth_id()
            || self.role_context_id != role.context_id()
            || !self.scope.same_session(admission.scope())
            || self.command_id != admission.command().command_id
            || self.command_sha256 != admission.command().checksum_sha256
            || self.index != admission.command().index
            || self.catalog_sha256 != store.recovery_range_catalog_sha256(self.scope.range_id)?
            || store.root.canonicalize()? != db.data_path().canonicalize()?
        {
            return Err(range_write_error(
                "actual historical Apply catalogue/open/context changed",
            ));
        }
        Ok(())
    }
}
impl RecoveryImageInstallSessionObservation {
    pub(crate) fn saved_scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn saved_native_database_incarnation(&self) -> &str {
        &self.saved_native_database_incarnation
    }
    pub(crate) fn current_native_database_incarnation(&self) -> &str {
        &self.current_native_database_incarnation
    }
    pub(crate) fn native_birth_id(&self) -> Uuid {
        self.native_birth_id
    }
    pub(crate) fn native_birth_sha256(&self) -> &str {
        &self.native_birth_sha256
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        native.require_current(db)?;
        store.require_native_recovery_identity(role, native)?;
        store.require_durable_recovery_catalog()?;
        self.scope.require_role(role)?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("target preflight recovery session disappeared"))?;
        if self.store_open_id != store.open_id
            || !saved.scope.same_session(&self.scope)
            || !matches!(saved.phase, RecoveryFencePhase::Active)
            || saved.native_database_incarnation != self.saved_native_database_incarnation
            || native.database_incarnation() != self.current_native_database_incarnation
            || native.birth_id() != self.native_birth_id
            || native.birth_sha256() != self.native_birth_sha256
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || self.range_catalog_sha256
                != store.recovery_range_catalog_sha256(self.scope.range_id)?
        {
            return Err(range_write_error(
                "actual target preflight session/store/native state changed",
            ));
        }
        Ok(())
    }
}
impl RecoveryResolvedCatalogObservation {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        &self.node_id
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.scope.cluster_id
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.scope.range_id
    }
    pub(crate) fn range_epoch(&self) -> u64 {
        self.scope.range_epoch
    }
    pub(crate) fn recovery_nonce(&self) -> Uuid {
        self.scope.recovery_nonce
    }
    pub(crate) fn configuration_sha256(&self) -> &str {
        &self.scope.configuration_sha256
    }
    pub(crate) fn resolved_index(&self) -> u64 {
        self.resolved_index
    }
    pub(crate) fn compacted_through(&self) -> u64 {
        self.compacted_through
    }
    pub(crate) fn local_role_context_id(&self) -> Uuid {
        self.local_role_context_id
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn require_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.scope.require_role(role)?;
        if &self.node_id != role.local_node_id() || self.local_role_context_id != role.context_id()
        {
            return Err(range_write_error(
                "resolved observation belongs to another local fresh role",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
    pub(crate) fn mark_historical_applied(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &crate::distribution_range_recovery::RecoveryApplyAdmission,
        receipt: &crate::db::NativeRecoveryAppliedReceipt,
    ) -> Result<RangeWriteProgress> {
        receipt.require_current(db, native, role, admission)?;
        self.require_local_historical_apply_state(role, native, admission)?;
        let command = admission.command();
        if receipt.range_id() != command.range_id
            || receipt.range_index() != command.index
            || receipt.command_id() != command.command_id
            || receipt.command_sha256() != command.checksum_sha256
            || receipt.native_database_incarnation() != native.database_incarnation()
            || receipt.actual_native_applied_through() < command.index
            || receipt.recovery_nonce() != admission.recovery_nonce()
            || receipt.configuration_sha256() != admission.scope().configuration_sha256
            || receipt.role_context_id() != role.context_id()
        {
            return Err(range_write_error(
                "actual native historical receipt does not match local certified command",
            ));
        }
        self.recovery_persistence_uncertain = true;
        let log = self
            .catalog
            .ranges
            .get_mut(&command.range_id)
            .expect("actual local preflight checked range");
        if let Some(entry) = log.entries.get_mut(&command.index) {
            // Never infer physical application from this old label: the
            // opaque actual WAL/history receipt above is mandatory each time.
            entry.state = RangeWriteState::Applied;
            advance_resolved(log);
            compact_resolved(log);
        } else if command.index > log.compacted_through {
            return Err(range_write_error(
                "physical historical receipt has a new catalogue gap",
            ));
        }
        // Even exact already-covered retries re-fsync before acknowledgment.
        self.persist_recovery_catalog()?;
        receipt.require_current(db, native, role, admission)?;
        Ok(self.progress(command.range_id))
    }
    fn require_local_historical_apply_state(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        admission: &crate::distribution_range_recovery::RecoveryApplyAdmission,
    ) -> Result<()> {
        admission.require_recipient_role(role)?;
        let scope = admission.scope();
        self.require_recovery_scope(role, native, scope)?;
        let command = admission.command();
        role.require_historical_command_scope(command)?;
        if self
            .catalog
            .handoff_promises
            .get(&scope.range_id)
            .is_some_and(|promise| command.index > promise.resolved_through)
        {
            return Err(range_write_error(
                "physical historical Apply exceeds permanent promised prefix",
            ));
        }
        let log = self
            .catalog
            .ranges
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("historical Apply actual log is absent"))?;
        let extra = if matches!(
            scope.purpose,
            MetadataRecoveryRolePurpose::CommittedHandoff { .. }
        ) {
            scope.range_epoch.checked_add(2)
        } else {
            None
        };
        if log.current_epoch != scope.range_epoch && Some(log.current_epoch) != extra {
            return Err(range_write_error(
                "physical historical Apply actual log epoch changed",
            ));
        }
        match log.entries.get(&command.index) {
            Some(entry)
                if entry.command == *command
                    && matches!(
                        entry.state,
                        RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                    ) => {}
            None if command.index <= log.compacted_through => {}
            _ => {
                return Err(range_write_error(
                    "physical historical Apply contradicts actual retained command state",
                ))
            }
        }
        let marker=self.catalog.recovery_fences[&scope.range_id].historical_certifications.get(&command.index)
            .ok_or_else(||range_write_error("physical historical Apply lacks an actual local current-session certify marker"))?;
        if marker.recovery_nonce != scope.recovery_nonce
            || marker.configuration_sha256 != scope.configuration_sha256
            || marker.range_epoch != scope.range_epoch
            || marker.index != command.index
            || marker.command_sha256 != command.checksum_sha256
        {
            return Err(range_write_error(
                "physical historical Apply local certify marker mismatch",
            ));
        }
        Ok(())
    }

    pub(crate) fn observe_local_historical_apply(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
        admission: &crate::distribution_range_recovery::RecoveryApplyAdmission,
    ) -> Result<RecoveryLocalHistoricalApplyObservation> {
        native.require_current(db)?;
        self.require_local_historical_apply_state(role, native, admission)?;
        let command = admission.command();
        let observed = RecoveryLocalHistoricalApplyObservation {
            store_open_id: self.open_id,
            native_database_incarnation: native.database_incarnation().to_string(),
            native_birth_id: native.birth_id(),
            role_context_id: role.context_id(),
            scope: admission.scope().clone(),
            command_id: command.command_id.clone(),
            command_sha256: command.checksum_sha256.clone(),
            index: command.index,
            catalog_sha256: self.recovery_range_catalog_sha256(command.range_id)?,
        };
        observed.require_current(self, db, native, role, admission)?;
        Ok(observed)
    }
    pub(crate) fn observe_image_install_recovery_session(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &RecoveryRoleAdmission,
    ) -> Result<RecoveryImageInstallSessionObservation> {
        native.require_current(db)?;
        self.require_native_recovery_identity(role, native)?;
        if !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || db.data_path().canonicalize()? != self.root.canonicalize()?
        {
            return Err(range_write_error(
                "current-voter image preflight requires its actual normal-recovery DB/store",
            ));
        }
        let saved = self
            .catalog
            .recovery_fences
            .get(&role.current_range().id)
            .ok_or_else(|| {
                range_write_error("current-voter image target has no saved recovery session")
            })?;
        saved.scope.require_role(role)?;
        if !matches!(saved.phase, RecoveryFencePhase::Active)
            || &saved.local_node_id != role.local_node_id()
            || saved.local_member_incarnation != role.local_member_incarnation()
        {
            return Err(range_write_error(
                "current-voter image target has another recovery session",
            ));
        }
        // Resolve an ambiguous prior refusal-only sync, without modifying any
        // nonce, config, native open, intent, history or physical row state.
        if self.recovery_persistence_uncertain {
            self.persist_recovery_catalog()?;
        }
        role.require_live_request()?;
        let saved = &self.catalog.recovery_fences[&role.current_range().id];
        let observed = RecoveryImageInstallSessionObservation {
            store_open_id: self.open_id,
            scope: saved.scope.clone(),
            saved_native_database_incarnation: saved.native_database_incarnation.clone(),
            current_native_database_incarnation: native.database_incarnation().to_string(),
            native_birth_id: native.birth_id(),
            native_birth_sha256: native.birth_sha256().to_string(),
            range_catalog_sha256: self.recovery_range_catalog_sha256(role.current_range().id)?,
        };
        observed.require_current(self, db, native, role)?;
        Ok(observed)
    }

    pub(crate) fn rebind_recovery_open_after_image_fence(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        role: &super::NativeImageInstallRole<'_>,
        fence: &super::TargetImageInstallFence,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<RecoveryFenceScope> {
        if !matches!(role, super::NativeImageInstallRole::CurrentVoterRepair(_)) {
            return Err(range_write_error(
                "only actual current-voter repair can rebind a normal recovery open",
            ));
        }
        self.validate_native_image_install_fence(fence, db, native, role, frozen)?;
        let actual = role.role();
        self.require_native_recovery_identity(actual, native)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&fence.range().id)
            .ok_or_else(|| {
                range_write_error("image repair cannot create a missing recovery session")
            })?;
        saved.scope.require_role(actual)?;
        if !matches!(saved.phase, RecoveryFencePhase::Active)
            || !matches!(
                saved.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || fence.session_id() != saved.scope.recovery_nonce
            || fence.nonce() != saved.scope.recovery_nonce
            || saved.local_node_id != *fence.target_node_id()
            || saved.local_member_incarnation != fence.target_member_incarnation()
        {
            return Err(range_write_error(
                "actual image intent cannot replace another recovery session",
            ));
        }
        let gate = db.range_mutation_gate();
        let controlled = gate.try_controlled_mutation(frozen)?;
        let scope = saved.scope.clone();
        self.catalog
            .recovery_fences
            .get_mut(&scope.range_id)
            .expect("matched recovery record")
            .native_database_incarnation = native.database_incarnation().to_string();
        // Always re-fsync even after an ambiguous previous same-open attempt.
        self.persist_recovery_catalog()?;
        native.require_current(db)?;
        actual.require_live_request()?;
        drop(controlled);
        Ok(scope)
    }
    pub(super) fn recovery_range_catalog_sha256(&self, range_id: RangeId) -> Result<String> {
        let saved = self
            .catalog
            .recovery_fences
            .get(&range_id)
            .ok_or_else(|| range_write_error("range commitment has no recovery record"))?;
        let scope = &saved.scope;
        // Request/context/term/read-index are one-shot provenance validated by
        // the live role, not range-state mutations or renewable leases.
        let stable_scope = (
            scope.protocol_version,
            &scope.cluster_id,
            scope.bootstrap_id,
            scope.range_id,
            scope.range_epoch,
            &scope.initiator_node_id,
            scope.initiator_incarnation,
            scope.recovery_nonce,
            &scope.configuration_sha256,
            &scope.range_configuration_sha256,
            &scope.purpose,
        );
        let refusal = (
            &saved.phase,
            &saved.local_node_id,
            saved.local_member_incarnation,
            &saved.native_database_incarnation,
            &saved.historical_certifications,
            &saved.retired_nonces,
        );
        struct HashWriter(Sha256);
        impl std::io::Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut hash = HashWriter(Sha256::new());
        serde_json::to_writer(
            &mut hash,
            &(
                "bicdb-recovery-range-catalog-v1",
                &self.catalog.cluster_id,
                &self.catalog.node_id,
                self.catalog.format_version,
                self.catalog.ranges.get(&range_id),
                stable_scope,
                refusal,
                self.catalog.handoff_promises.get(&range_id),
                self.catalog.image_install_intents.get(&range_id),
                self.catalog.native_image_bases.get(&range_id),
                self.catalog.backup_fences.get(&range_id),
            ),
        )?;
        Ok(hex::encode(hash.0.finalize()))
    }
    /// No row/WAL Apply occurs here. Only an independently observed committed
    /// historical decision can install the current-session certify marker.
    pub(crate) fn certify_historical_command(
        &mut self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        decision: &crate::distribution_range_recovery::HistoricalCommandDecision,
    ) -> Result<HistoricalRangeCertification> {
        decision.require_recipient_role(role)?;
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
                "historical certification exceeds permanent promised prefix",
            ));
        }
        // Every subsequent error retains a conservative no-proof refusal;
        // partially changed in-memory labels are never durable observations.
        self.recovery_persistence_uncertain = true;
        let log = self
            .catalog
            .ranges
            .get_mut(&scope.range_id)
            .ok_or_else(|| range_write_error("fenced range is absent"))?;
        if let Some(existing) = log.entries.get(&command.index) {
            if existing.command != *command || existing.state == RangeWriteState::Aborted {
                return Err(range_write_error(
                    "historical certification conflicts with actual retained history",
                ));
            }
        } else {
            if command.index <= log.compacted_through
                || command.index != log.last_index.saturating_add(1)
            {
                return Err(range_write_error("historical certification has a compacted/unavailable gap; native image/history repair is required"));
            }
            log.entries.insert(
                command.index,
                RangeWriteLogEntry {
                    command: command.clone(),
                    state: RangeWriteState::Committed,
                },
            );
            log.last_index = command.index;
        }
        let entry = log
            .entries
            .get_mut(&command.index)
            .expect("validated/inserted historical entry");
        if entry.state != RangeWriteState::Applied {
            entry.state = RangeWriteState::QuorumCommitted;
        }
        let marker = HistoricalRangeCertification {
            recovery_nonce: scope.recovery_nonce,
            configuration_sha256: scope.configuration_sha256.clone(),
            range_epoch: scope.range_epoch,
            index: command.index,
            command_sha256: command.checksum_sha256.clone(),
        };
        let record = self
            .catalog
            .recovery_fences
            .get_mut(&scope.range_id)
            .expect("scope checked active");
        if !record
            .historical_certifications
            .contains_key(&command.index)
            && record.historical_certifications.len() >= 1024
        {
            return Err(range_write_error(
                "current-session historical certification bound reached",
            ));
        }
        if record
            .historical_certifications
            .get(&command.index)
            .is_some_and(|old| old != &marker)
        {
            return Err(range_write_error(
                "conflicting current-session historical certification",
            ));
        }
        record
            .historical_certifications
            .insert(command.index, marker.clone());
        // Retain ambiguous decisions/refusals but never expose them as synced
        // observations until a subsequent successful whole-catalog fsync.
        self.persist_recovery_catalog()?;
        Ok(marker)
    }
    pub(crate) fn recovery_observation_phase(
        &self,
        scope: &RecoveryFenceScope,
    ) -> Result<crate::distribution_range_recovery::RecoveryFenceObservationPhase> {
        self.require_durable_recovery_catalog()?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("no actual recovery phase record"))?;
        if !saved.scope.same_session(scope) {
            return Err(range_write_error(
                "actual recovery phase belongs to another session",
            ));
        }
        Ok(match saved.phase {
            RecoveryFencePhase::Active=>crate::distribution_range_recovery::RecoveryFenceObservationPhase::ActiveRecovery,
            RecoveryFencePhase::NativePrepared(_) => crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared,
            RecoveryFencePhase::Completed {..}=>crate::distribution_range_recovery::RecoveryFenceObservationPhase::CompletedEpochAuthority,
        })
    }
    pub(crate) fn preflight_recovery_nonce(&self, scope: &RecoveryFenceScope) -> Result<()> {
        self.require_no_image_install(scope.range_id)?;
        scope.validate()?;
        if let Some(old) = self.catalog.recovery_fences.get(&scope.range_id) {
            if scope.range_epoch < old.scope.range_epoch
                || (scope.range_epoch == old.scope.range_epoch
                    && (old.retired_nonces.contains(&scope.recovery_nonce)
                        || (old.scope.recovery_nonce != scope.recovery_nonce
                            && old.retired_nonces.len() >= 256)))
            {
                return Err(range_write_error(
                    "replayed/retired recovery nonce or same-epoch refresh bound reached",
                ));
            }
        }
        Ok(())
    }
    /// Refinement guard for a current-voter image installer. It never installs
    /// or supersedes a session: that requires a prior actual epoch-fence round.
    pub(crate) fn require_image_install_recovery_scope(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
    ) -> Result<RecoveryFenceScope> {
        self.require_durable_recovery_catalog()?;
        self.require_native_recovery_identity(role, native)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&role.current_range().id)
            .ok_or_else(|| {
                range_write_error("current-voter repair target has no active recovery fence")
            })?;
        saved.scope.require_role(role)?;
        if !matches!(saved.phase, RecoveryFencePhase::Active)
            || saved.native_database_incarnation != native.database_incarnation()
            || saved.local_member_incarnation != role.local_member_incarnation()
            || &saved.local_node_id != role.local_node_id()
        {
            return Err(range_write_error(
                "image installer recovery scope/native open changed",
            ));
        }
        Ok(saved.scope.clone())
    }
    pub(crate) fn historical_certification_observation(
        &self,
        scope: &RecoveryFenceScope,
        index: u64,
    ) -> Result<Option<HistoricalRangeCertification>> {
        self.require_persisted_recovery_scope(scope)?;
        Ok(self.catalog.recovery_fences[&scope.range_id]
            .historical_certifications
            .get(&index)
            .cloned())
    }
    pub(crate) fn require_persisted_recovery_scope(
        &self,
        scope: &RecoveryFenceScope,
    ) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        self.require_no_image_install(scope.range_id)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("no actual persisted recovery fence"))?;
        if !saved.scope.same_session(scope) || !matches!(saved.phase, RecoveryFencePhase::Active) {
            return Err(range_write_error(
                "actual persisted recovery fence scope changed",
            ));
        }
        Ok(())
    }
    pub(super) fn require_durable_recovery_catalog(&self) -> Result<()> {
        if self.recovery_persistence_uncertain {
            return Err(range_write_error("recovery catalog persistence is uncertain; re-fsync the exact fenced session before claiming durability"));
        }
        Ok(())
    }
    pub(super) fn persist_recovery_catalog(&mut self) -> Result<()> {
        self.persist_recovery_catalog_with(crate::storage::write_atomic)
    }
    pub(super) fn persist_recovery_catalog_with(
        &mut self,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<()> {
        self.recovery_persistence_uncertain = true;
        self.persist_with(write)?;
        self.recovery_persistence_uncertain = false;
        Ok(())
    }
    pub(crate) fn observe_fenced_recovery_history(
        &self,
        role: &crate::distribution_range_recovery::RecoveryObservationRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        index: Option<u64>,
    ) -> Result<Option<RangeWriteLogEntry>> {
        self.require_durable_recovery_catalog()?;
        self.require_no_image_install(scope.range_id)?;
        role.require_scope(scope)?;
        role.require_native_identity(native)?;
        if !self.fsync
            || !crate::format::range_image_install_active(&self.root)?
            || self.catalog.cluster_id != *role.cluster_id()
            || self.catalog.node_id != *role.local_node_id()
        {
            return Err(range_write_error(
                "read-only recovery requires actual local admitted native birth",
            ));
        }
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| {
                range_write_error("read-only recovery observation has no persisted fence")
            })?;
        if !saved.scope.same_session(scope)
            || !matches!(saved.phase, RecoveryFencePhase::Active)
            || saved.native_database_incarnation != native.database_incarnation()
        {
            return Err(range_write_error(
                "read-only recovery observation fence/open/session changed",
            ));
        }
        let entry = index.and_then(|index| {
            self.catalog
                .ranges
                .get(&scope.range_id)
                .and_then(|log| log.entries.get(&index))
        });
        // Shared complete-entry bound, before cloning a large command body.
        if let Some(entry) = entry {
            super::validate_native_recovery_entry_size(entry)?;
        }
        Ok(entry.cloned())
    }
    pub(crate) fn recovery_store_open_id(&self) -> Uuid {
        self.open_id
    }
    pub(crate) fn require_no_active_recovery(&self, range_id: RangeId) -> Result<()> {
        if self
            .catalog
            .recovery_fences
            .get(&range_id)
            .is_some_and(|f| {
                !matches!(f.phase, RecoveryFencePhase::Completed { .. })
                    || matches!(
                        f.scope.purpose,
                        MetadataRecoveryRolePurpose::CommittedHandoff { .. }
                    )
            })
        {
            return Err(range_write_error(
                "range is blocked by a durable active recovery/handoff context",
            ));
        }
        Ok(())
    }
    pub(crate) fn normal_native_write_allowed(&self, command: &RangeWriteCommand) -> Result<()> {
        self.require_no_active_recovery(command.range_id)?;
        self.require_no_image_install(command.range_id)?;
        if self
            .catalog
            .handoff_promises
            .contains_key(&command.range_id)
        {
            return Err(range_write_error(
                "ordinary native write conflicts with permanent handoff promise",
            ));
        }
        if self
            .catalog
            .ranges
            .get(&command.range_id)
            .is_some_and(|log| log.current_epoch != command.range_epoch)
        {
            return Err(range_write_error(
                "ordinary native command is fenced by current epoch",
            ));
        }
        if self
            .catalog
            .backup_fences
            .get(&command.range_id)
            .is_some_and(|f| command.index > f.resolved_through)
        {
            return Err(range_write_error(
                "ordinary native command exceeds backup fence",
            ));
        }
        Ok(())
    }
    pub(super) fn require_native_recovery_identity(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        role.require_live_request()?;
        if !self.fsync
            || !crate::format::range_image_install_active(&self.root)?
            || &self.catalog.cluster_id != role.cluster_id()
            || &self.catalog.node_id != role.local_node_id()
            || native.cluster_id() != role.cluster_id()
            || native.bootstrap_id() != role.bootstrap_id()
            || native.local_node_id() != role.local_node_id()
            || native.local_member_incarnation() != role.local_member_incarnation()
            || native.restore_activation() != role.restore_activation()
        {
            return Err(range_write_error(
                "recovery requires actual admitted native birth, fsync and exact fresh role",
            ));
        }
        Ok(())
    }
    /// Read-only full candidate validation before changing the actual freeze
    /// owner; the mutating persist path repeats it after drain under latches.
    pub(crate) fn preflight_recovery_fence(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<()> {
        self.require_no_image_install(scope.range_id)?;
        self.preflight_recovery_nonce(scope)?;
        self.require_native_recovery_identity(role, native)?;
        scope.require_role(role)?;
        if self.catalog.backup_fences.contains_key(&scope.range_id) {
            return Err(range_write_error(
                "recovery cannot supersede an active backup fence",
            ));
        }
        let promised = self.catalog.handoff_promises.get(&scope.range_id);
        let obsolete_promise_current_config = promised.is_some_and(|promise| {
            scope.range_epoch == promise.activation_epoch
                && role.expected_handoff_old_config_sha256().ok().as_deref()
                    == Some(promise.new_config_sha256.as_str())
        });
        if let Some(promise) = promised {
            match &scope.purpose {
                MetadataRecoveryRolePurpose::CommittedHandoff {
                    relocation_id,
                    plan_id,
                    session_id,
                    nonce,
                } if promise.relocation_id == *relocation_id
                    && promise.plan_id == *plan_id
                    && promise.session_id == *session_id
                    && promise.nonce == *nonce
                    && promise.old_epoch == scope.range_epoch
                    && promise.old_config_sha256
                        == role.expected_handoff_old_config_sha256()? => {}
                MetadataRecoveryRolePurpose::NormalRecovery if obsolete_promise_current_config => {}
                _ => {
                    return Err(range_write_error(
                        "recovery cannot supersede a permanent handoff promise",
                    ))
                }
            }
        }
        if let Some(old) = self.catalog.recovery_fences.get(&scope.range_id) {
            let exact_normal_to_committed_handoff = matches!(
                old.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            ) && matches!(
                scope.purpose,
                MetadataRecoveryRolePurpose::CommittedHandoff { .. }
            ) && old.scope.range_epoch == scope.range_epoch
                && old.scope.range_configuration_sha256 == scope.range_configuration_sha256
                && role.committed_relocation().is_some();
            if matches!(
                old.scope.purpose,
                MetadataRecoveryRolePurpose::CommittedHandoff { .. }
            ) && old.scope.purpose != scope.purpose
                && !obsolete_promise_current_config
            {
                return Err(range_write_error(
                    "normal recovery cannot supersede a handoff recovery",
                ));
            }
            if old.scope.range_epoch > scope.range_epoch
                || (old.scope.range_epoch == scope.range_epoch
                    && old.scope.configuration_sha256 != scope.configuration_sha256
                    && !exact_normal_to_committed_handoff)
            {
                return Err(range_write_error(
                    "recovery scope regressed or changed configuration without epoch",
                ));
            }
            if old.scope.same_session(scope) && !matches!(old.phase, RecoveryFencePhase::Active) {
                return Err(range_write_error(
                    "prepared/completed recovery cannot be reset by a repeated epoch fence",
                ));
            }
        } else if self.catalog.recovery_fences.len() >= MAX_RECOVERY_FENCES {
            return Err(range_write_error("recovery refusal catalog bound reached"));
        }
        if let Some(log) = self.catalog.ranges.get(&scope.range_id) {
            if log.current_epoch > scope.range_epoch
                && !promised.is_some_and(|p| p.activation_epoch == log.current_epoch)
            {
                return Err(range_write_error(
                    "recovery cannot lower a fenced data epoch",
                ));
            }
        }
        Ok(())
    }
    /// Caller owns and has quiescently drained the actual DB range freeze,
    /// then rechecked NativeRecoveryAdmission::require_current under DB guard.
    /// No old prepared/committed entry is changed, and no native floor is set.
    pub(crate) fn fence_recovery_epoch(
        &mut self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<()> {
        self.fence_recovery_epoch_with(role, native, scope, crate::storage::write_atomic)
    }
    fn fence_recovery_epoch_with(
        &mut self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<()> {
        self.preflight_recovery_fence(role, native, scope)?;
        let log = self
            .catalog
            .ranges
            .entry(scope.range_id)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: scope.range_epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
                entries: BTreeMap::new(),
            });
        log.current_epoch = log.current_epoch.max(scope.range_epoch);
        self.catalog.format_version = self.catalog.format_version.max(5);
        let historical_certifications = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .filter(|old| old.scope.same_session(scope))
            .map(|old| old.historical_certifications.clone())
            .unwrap_or_default();
        let mut retired_nonces = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .filter(|old| old.scope.range_epoch == scope.range_epoch)
            .map(|old| old.retired_nonces.clone())
            .unwrap_or_default();
        if let Some(old) = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .filter(|old| {
                old.scope.range_epoch == scope.range_epoch
                    && old.scope.recovery_nonce != scope.recovery_nonce
            })
        {
            retired_nonces.insert(old.scope.recovery_nonce);
        }
        self.catalog.recovery_fences.insert(
            scope.range_id,
            RecoveryFenceRecord {
                scope: scope.clone(),
                phase: RecoveryFencePhase::Active,
                local_node_id: role.local_node_id().clone(),
                local_member_incarnation: role.local_member_incarnation(),
                native_database_incarnation: native.database_incarnation().to_string(),
                historical_certifications,
                retired_nonces,
            },
        );
        // Retain this refusal on any rename/directory-sync ambiguity. Retrying
        // always re-fsyncs before any fresh quorum acknowledgment is issued.
        self.persist_recovery_catalog_with(write)
    }
    pub(crate) fn require_recovery_scope(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        self.require_no_image_install(scope.range_id)?;
        self.require_native_recovery_identity(role, native)?;
        scope.require_role(role)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("no durable recovery epoch fence"))?;
        if !saved.scope.same_session(scope)
            || !matches!(saved.phase, RecoveryFencePhase::Active)
            || saved.native_database_incarnation != native.database_incarnation()
        {
            return Err(range_write_error(
                "recovery nonce/context or native DB open changed",
            ));
        }
        Ok(())
    }
    pub(crate) fn observe_recovery_resolved_prefix(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<RecoveryResolvedCatalogObservation> {
        self.require_recovery_scope(role, native, scope)?;
        self.observe_native_ready_resolved_prefix(role, native, scope)
    }
    /// Read-only re-attestation may inspect a prepared point. It grants no
    /// historical mutation permission and cannot reset the preparation promise.
    pub(crate) fn observe_native_ready_resolved_prefix(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<RecoveryResolvedCatalogObservation> {
        self.require_native_ready_scope(role, native, scope)?;
        let log = self
            .catalog
            .ranges
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("fenced range absent"))?;
        if log.last_index != log.resolved_through
            || log.entries.values().any(|entry| {
                matches!(
                    entry.state,
                    RangeWriteState::Prepared
                        | RangeWriteState::Committed
                        | RangeWriteState::QuorumCommitted
                )
            })
        {
            return Err(range_write_error(
                "historical prefix/tail is not fully resolved",
            ));
        }
        Ok(RecoveryResolvedCatalogObservation {
            store_open_id: self.open_id,
            node_id: self.catalog.node_id.clone(),
            scope: scope.clone(),
            resolved_index: log.resolved_through,
            compacted_through: log.compacted_through,
            catalog_sha256: self.recovery_range_catalog_sha256(scope.range_id)?,
            local_role_context_id: role.context_id(),
        })
    }
    pub(crate) fn revalidate_recovery_resolved_observation(
        &self,
        observation: &RecoveryResolvedCatalogObservation,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        observation.require_role(role)?;
        self.require_native_ready_scope(role, native, &observation.scope)?;
        if observation.store_open_id != self.open_id
            || observation.catalog_sha256
                != self.recovery_range_catalog_sha256(observation.range_id())?
        {
            return Err(range_write_error(
                "actual recovery catalogue/open changed after observation",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{RangeImageBirthMember, RangeImageBirthSpec};
    use crate::distribution_range_recovery::test_role_admission;
    use crate::{
        BicDb, CommitAdmissionMutation, DbConfig, DistributionConfig, DistributionStore,
        StorageMode,
    };
    use std::time::Duration;

    struct Fixture {
        root: tempfile::TempDir,
        db: BicDb,
        topology: ClusterTopology,
        role: RecoveryRoleAdmission,
        native: NativeRecoveryAdmission,
        scope: RecoveryFenceScope,
        store: RangeWriteStore,
        range: RangeDescriptor,
    }
    impl Fixture {
        fn new(epoch: u64) -> Self {
            Self::with_ranges(epoch, 1)
        }
        fn with_ranges(epoch: u64, initial_ranges: u32) -> Self {
            let root = tempfile::tempdir().unwrap();
            let node = ClusterNodeId::new("n1").unwrap();
            let bootstrap = Uuid::now_v7();
            let config = DistributionConfig {
                enabled: true,
                cluster_id: ClusterId::new("native-recovery-state").unwrap(),
                node_id: node.clone(),
                node_address: "127.0.0.1:10001".into(),
                initial_ranges,
                replication_factor: 1,
                range_image_bootstrap_id: Some(bootstrap),
                ..DistributionConfig::default()
            };
            let topology =
                DistributionStore::initialize_at(root.path().join("control"), config, true, 1)
                    .unwrap()
                    .topology()
                    .clone();
            let range = topology.ranges.values().next().unwrap().clone();
            let spec = RangeImageBirthSpec {
                cluster_id: topology.cluster_id.clone(),
                bootstrap_id: bootstrap,
                node_id: node.clone(),
                member_incarnation: topology.nodes[&node].incarnation,
                initial_members: topology
                    .nodes
                    .iter()
                    .map(|(id, m)| {
                        (
                            id.clone(),
                            RangeImageBirthMember {
                                incarnation: m.incarnation,
                                tls_certificate_sha256: m.tls_certificate_sha256.clone(),
                                pending_tls_certificate_sha256: m
                                    .pending_tls_certificate_sha256
                                    .clone(),
                            },
                        )
                    })
                    .collect(),
                // A genuinely completed fresh learner birth has no native
                // range base until an image/genesis is actually installed.
                initial_ranges: vec![],
                restore_activation: None,
            };
            let dbconfig = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
            let (mut db, _) =
                BicDb::create_new_range_image_database(&root.path().join("db"), dbconfig, spec)
                    .unwrap();
            db.create_collection("items").unwrap();
            let fingerprint = db.schema_compatibility_fingerprint().unwrap().sha256;
            let mut topology = topology;
            topology.nodes.get_mut(&node).unwrap().labels.insert(
                crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
                fingerprint,
            );
            topology.ranges.values_mut().next().unwrap().epoch = epoch;
            let role = test_role_admission(
                &root.path().join("metadata"),
                &topology,
                &node,
                &node,
                &node,
                range.id,
                epoch,
                Uuid::now_v7(),
                MetadataRecoveryRolePurpose::NormalRecovery,
                Duration::from_secs(3),
            )
            .unwrap();
            let native = db
                .native_recovery_admission(
                    &topology.cluster_id,
                    bootstrap,
                    &node,
                    topology.nodes[&node].incarnation,
                )
                .unwrap();
            native.require_current(&db).unwrap();
            let scope = RecoveryFenceScope::from_role(&role).unwrap();
            let store = RangeWriteStore::open(
                root.path().join("db"),
                topology.cluster_id.clone(),
                node,
                true,
            )
            .unwrap();
            Self {
                root,
                db,
                topology,
                role,
                native,
                scope,
                store,
                range,
            }
        }
        fn command(&self, index: u64) -> RangeWriteCommand {
            RangeWriteCommand::new(
                self.topology.cluster_id.clone(),
                &self.range,
                index,
                self.range.leader.clone(),
                &CommitAdmissionIntent {
                    transaction_id: index,
                    mutations: vec![CommitAdmissionMutation {
                        collection: "items".into(),
                        record_id: format!("row-{index}"),
                        record: Some(crate::Record::new(format!("row-{index}"))),
                    }],
                },
            )
            .unwrap()
        }
    }
    #[test]
    fn actual_native_epoch_fence_preserves_unresolved_tail_and_never_grants_zero_floor() {
        let mut fixture = Fixture::new(2);
        let one = fixture.command(1);
        let two = fixture.command(2);
        let three = fixture.command(3);
        fixture.store.prepare(one.clone()).unwrap();
        fixture.store.prepare(two.clone()).unwrap();
        fixture.store.commit(&two).unwrap();
        fixture.store.prepare(three.clone()).unwrap();
        fixture.store.commit(&three).unwrap();
        fixture.store.certify(&three).unwrap();
        let gate = fixture.db.range_mutation_gate();
        let frozen = gate
            .try_freeze(one.range_id, fixture.scope.recovery_nonce)
            .unwrap();
        gate.validate_quiescent_frozen(one.range_id, &frozen)
            .unwrap();
        fixture.native.require_current(&fixture.db).unwrap();
        fixture
            .store
            .fence_recovery_epoch(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        let log = &fixture.store.catalog.ranges[&one.range_id];
        assert_eq!(log.current_epoch, 2);
        assert_eq!(log.last_index, 3);
        assert_eq!(log.resolved_through, 0);
        assert_eq!(log.entries[&1].state, RangeWriteState::Prepared);
        assert_eq!(log.entries[&2].state, RangeWriteState::Committed);
        assert_eq!(log.entries[&3].state, RangeWriteState::QuorumCommitted);
        assert!(fixture.store.prepare(fixture.command(4)).is_err());
        assert!(fixture.store.commit(&one).is_err());
        assert!(fixture.store.normal_native_write_allowed(&three).is_err());
        assert!(fixture
            .store
            .observe_recovery_resolved_prefix(&fixture.role, &fixture.native, &fixture.scope)
            .is_err());
        assert_eq!(
            fixture
                .db
                .native_range_prefix_observation(one.range_id)
                .unwrap()
                .native_applied_index,
            None,
            "logical zero is NOT verified native genesis"
        );
        let catalog = fixture.store.catalog.clone();
        drop(fixture.store);
        let reopened = RangeWriteStore::open(
            fixture.root.path().join("db"),
            fixture.topology.cluster_id.clone(),
            fixture.range.leader.clone(),
            true,
        )
        .unwrap();
        assert_eq!(reopened.catalog, catalog);
        assert!(reopened.normal_native_write_allowed(&three).is_err());
        assert!(fixture.root.path().exists());
    }
    #[test]
    fn actual_other_range_decisions_and_same_session_refresh_preserve_range_observation() {
        let mut fixture = Fixture::with_ranges(1, 2);
        let gate = fixture.db.range_mutation_gate();
        let _frozen = gate
            .try_freeze(fixture.range.id, fixture.scope.freeze_owner().unwrap())
            .unwrap();
        fixture
            .store
            .fence_recovery_epoch(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        let observation = fixture
            .store
            .observe_recovery_resolved_prefix(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        let other = fixture
            .topology
            .ranges
            .values()
            .find(|r| r.id != fixture.range.id)
            .unwrap();
        let key = (0..1000)
            .map(|n| format!("other-{n}"))
            .find(|id| fixture.topology.range_for_key("items", id).unwrap().id == other.id)
            .unwrap();
        let command = RangeWriteCommand::new(
            fixture.topology.cluster_id.clone(),
            other,
            1,
            other.leader.clone(),
            &CommitAdmissionIntent {
                transaction_id: 123,
                mutations: vec![CommitAdmissionMutation {
                    collection: "items".into(),
                    record_id: key.clone(),
                    record: Some(crate::Record::new(key)),
                }],
            },
        )
        .unwrap();
        fixture.store.prepare(command.clone()).unwrap();
        fixture.store.commit(&command).unwrap();
        fixture.store.certify(&command).unwrap();
        fixture.store.mark_applied(&command).unwrap();
        fixture
            .store
            .revalidate_recovery_resolved_observation(&observation, &fixture.role, &fixture.native)
            .unwrap();
        let renewed = test_role_admission(
            &fixture.root.path().join("metadata"),
            &fixture.topology,
            &fixture.range.leader,
            &fixture.range.leader,
            &fixture.range.leader,
            fixture.range.id,
            fixture.scope.range_epoch,
            fixture.scope.recovery_nonce,
            MetadataRecoveryRolePurpose::NormalRecovery,
            Duration::from_secs(3),
        )
        .unwrap();
        let refreshed_scope = RecoveryFenceScope::from_role(&renewed).unwrap();
        assert_ne!(
            refreshed_scope.metadata_context_id,
            fixture.scope.metadata_context_id
        );
        fixture
            .store
            .fence_recovery_epoch(&renewed, &fixture.native, &refreshed_scope)
            .unwrap();
        fixture
            .store
            .revalidate_recovery_resolved_observation(&observation, &fixture.role, &fixture.native)
            .unwrap();
        assert!(
            observation.require_role(&renewed).is_err(),
            "stable range state does not renew original role provenance"
        );
        assert_eq!(
            fixture
                .db
                .native_range_prefix_observation(fixture.range.id)
                .unwrap()
                .native_applied_index,
            None
        );
    }

    #[test]
    fn actual_rename_then_directory_sync_error_cannot_issue_durability_observations() {
        let mut fixture = Fixture::new(1);
        let gate = fixture.db.range_mutation_gate();
        let _frozen = gate
            .try_freeze(fixture.range.id, fixture.scope.freeze_owner().unwrap())
            .unwrap();
        let error = fixture
            .store
            .fence_recovery_epoch_with(
                &fixture.role,
                &fixture.native,
                &fixture.scope,
                |path, bytes, fsync| {
                    assert!(fsync);
                    let stage = path.with_extension("recovery-injected-stage");
                    fs::write(&stage, bytes)?;
                    File::open(&stage)?.sync_all()?;
                    fs::rename(&stage, path)?;
                    Err(std::io::Error::other(
                        "injected failure after rename before directory sync",
                    )
                    .into())
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("after rename"));
        assert!(
            fixture
                .store
                .catalog
                .recovery_fences
                .contains_key(&fixture.range.id),
            "stricter refusal is retained"
        );
        assert!(fixture
            .store
            .require_persisted_recovery_scope(&fixture.scope)
            .is_err());
        assert!(fixture
            .store
            .recovery_observation_phase(&fixture.scope)
            .is_err());
        assert!(fixture
            .store
            .historical_certification_observation(&fixture.scope, 1)
            .is_err());
        assert!(fixture
            .store
            .observe_recovery_resolved_prefix(&fixture.role, &fixture.native, &fixture.scope)
            .is_err());
        // The same exact session re-fsyncs before reporting any durability.
        fixture
            .store
            .fence_recovery_epoch(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        fixture
            .store
            .require_persisted_recovery_scope(&fixture.scope)
            .unwrap();
        assert_eq!(
            fixture
                .store
                .recovery_observation_phase(&fixture.scope)
                .unwrap(),
            crate::distribution_range_recovery::RecoveryFenceObservationPhase::ActiveRecovery
        );
        assert_eq!(
            fixture
                .db
                .native_range_prefix_observation(fixture.range.id)
                .unwrap()
                .native_applied_index,
            None
        );
    }

    #[test]
    fn actual_same_epoch_nonce_refresh_durably_refuses_delayed_old_fence() {
        let mut fixture = Fixture::new(1);
        let gate = fixture.db.range_mutation_gate();
        let mut frozen = gate
            .try_freeze(fixture.range.id, fixture.scope.recovery_nonce)
            .unwrap();
        fixture
            .store
            .fence_recovery_epoch(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        let role_b = test_role_admission(
            &fixture.root.path().join("metadata"),
            &fixture.topology,
            &fixture.range.leader,
            &fixture.range.leader,
            &fixture.range.leader,
            fixture.range.id,
            fixture.scope.range_epoch,
            Uuid::now_v7(),
            MetadataRecoveryRolePurpose::NormalRecovery,
            Duration::from_secs(3),
        )
        .unwrap();
        let scope_b = RecoveryFenceScope::from_role(&role_b).unwrap();
        fixture.store.preflight_recovery_nonce(&scope_b).unwrap();
        frozen.try_rotate_owner(scope_b.recovery_nonce).unwrap();
        fixture
            .store
            .fence_recovery_epoch(&role_b, &fixture.native, &scope_b)
            .unwrap();
        let before = fixture.store.catalog.clone();
        assert!(fixture
            .store
            .preflight_recovery_nonce(&fixture.scope)
            .is_err());
        assert!(fixture
            .store
            .fence_recovery_epoch(&fixture.role, &fixture.native, &fixture.scope)
            .is_err());
        assert_eq!(frozen.owner(), scope_b.recovery_nonce);
        assert_eq!(fixture.store.catalog, before);
        let reopened = RangeWriteStore::open(
            fixture.root.path().join("db"),
            fixture.topology.cluster_id.clone(),
            fixture.range.leader.clone(),
            true,
        )
        .unwrap();
        assert!(reopened.preflight_recovery_nonce(&fixture.scope).is_err());
        assert!(reopened.preflight_recovery_nonce(&scope_b).is_ok());
        assert_eq!(
            reopened.catalog.recovery_fences[&fixture.range.id].retired_nonces,
            BTreeSet::from([fixture.scope.recovery_nonce])
        );
    }

    #[test]
    fn actual_store_open_and_nonce_bind_resolved_observation() {
        let mut fixture = Fixture::new(1);
        let gate = fixture.db.range_mutation_gate();
        let frozen = gate
            .try_freeze(fixture.range.id, fixture.scope.recovery_nonce)
            .unwrap();
        gate.validate_quiescent_frozen(fixture.range.id, &frozen)
            .unwrap();
        fixture
            .store
            .fence_recovery_epoch(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        let observed = fixture
            .store
            .observe_recovery_resolved_prefix(&fixture.role, &fixture.native, &fixture.scope)
            .unwrap();
        assert_eq!(observed.resolved_index(), 0);
        fixture
            .store
            .revalidate_recovery_resolved_observation(&observed, &fixture.role, &fixture.native)
            .unwrap();
        let renewed = test_role_admission(
            &fixture.root.path().join("metadata"),
            &fixture.topology,
            &fixture.range.leader,
            &fixture.range.leader,
            &fixture.range.leader,
            fixture.range.id,
            fixture.scope.range_epoch,
            fixture.scope.recovery_nonce,
            MetadataRecoveryRolePurpose::NormalRecovery,
            Duration::from_secs(3),
        )
        .unwrap();
        assert_ne!(
            renewed.business_request_id(),
            fixture.role.business_request_id()
        );
        // The durable session remains; each operation still supplies a new
        // independently fresh witness. An old observation cannot become new.
        fixture
            .store
            .require_recovery_scope(&renewed, &fixture.native, &fixture.scope)
            .unwrap();
        assert!(observed.require_role(&renewed).is_err());
        let mut old = fixture.scope.clone();
        old.recovery_nonce = Uuid::now_v7();
        assert!(fixture
            .store
            .require_recovery_scope(&fixture.role, &fixture.native, &old)
            .is_err());
        let reopened = RangeWriteStore::open(
            fixture.root.path().join("db"),
            fixture.topology.cluster_id.clone(),
            fixture.range.leader.clone(),
            true,
        )
        .unwrap();
        assert!(reopened
            .revalidate_recovery_resolved_observation(&observed, &fixture.role, &fixture.native)
            .is_err());
        assert_eq!(
            fixture
                .db
                .native_range_prefix_observation(fixture.range.id)
                .unwrap()
                .native_applied_index,
            None
        );
    }
}
