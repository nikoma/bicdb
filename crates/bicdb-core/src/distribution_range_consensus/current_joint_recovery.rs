//! A new refusal in an ACTUALLY completed joint domain. Persisted bindings and
//! archived old refusals are constraints; only the actual native fact plus a
//! fresh specific metadata role may enter this path.
use super::*;
use crate::db::{
    NativeCurrentJointBinding, NativeCurrentJointRecoveryDomain, NativeRecoveryAdmission,
};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use recovery_state::{RecoveryFencePhase, RecoveryFenceRecord};

pub(super) const CURRENT_JOINT_CATALOG_FORMAT: u32 = 16;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct CurrentJointRecoveryState {
    binding: NativeCurrentJointBinding,
    scope: RecoveryFenceScope,
    original_handoff_promise: RangeHandoffPromise,
    archived: BTreeMap<Uuid, ArchivedJointRefusal>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArchivedJointRefusal {
    fence: RecoveryFenceRecord,
    // The exact previous operation's floor survives a subsequent independent
    // recovery. None is only for the original non-CurrentJoint refusal.
    binding: Option<NativeCurrentJointBinding>,
}
pub(super) fn validate_current_joint_catalog(catalog: &RangeWriteCatalog) -> Result<()> {
    if (catalog.format_version >= CURRENT_JOINT_CATALOG_FORMAT)
        != catalog.current_joint_recoveries.is_some()
    {
        return Err(range_write_error(
            "current joint recovery format/map is missing or unsupported",
        ));
    }
    if let Some(states) = &catalog.current_joint_recoveries {
        if states.len() > recovery_state::MAX_RECOVERY_FENCES
            || catalog.native_writer_enforcement_version != Some(1)
            || catalog.native_abort_protocol.is_none()
            || catalog.native_joint_handoffs.is_none()
        {
            return Err(range_write_error(
                "current joint recovery lacks enforced native prerequisite maps",
            ));
        }
        for (range, state) in states {
            state.scope.validate()?;
            state.original_handoff_promise.validate()?;
            if state.original_handoff_promise.range_id != *range {
                return Err(range_write_error(
                    "current joint archive changed original promised range",
                ));
            }
            if state.scope.range_id != *range
                || state.binding.range.id != *range
                || state.archived.len() > 256
                || state.binding.store_id
                    != catalog.native_abort_protocol.as_ref().unwrap().store_id()
                || !matches!(&state.scope.purpose, MetadataRecoveryRolePurpose::CurrentJointRecovery { joint_decision_sha256 } if joint_decision_sha256 == &state.binding.joint_decision_sha256)
                || catalog
                    .recovery_fences
                    .get(range)
                    .is_none_or(|saved| !saved.scope.same_session(&state.scope))
            {
                return Err(range_write_error(
                    "damaged current joint recovery constraints",
                ));
            }
            for (nonce, archived) in &state.archived {
                let old = &archived.fence;
                old.validate()?;
                if matches!(
                    old.scope.purpose,
                    MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
                ) != archived.binding.is_some()
                {
                    return Err(range_write_error(
                        "archived joint operation lost exact prior native floor binding",
                    ));
                }
                if nonce.is_nil()
                    || *nonce != old.scope.recovery_nonce
                    || old.scope.range_id != *range
                    || old.scope.cluster_id != catalog.cluster_id
                    || old.local_node_id != catalog.node_id
                    || *nonce == state.scope.recovery_nonce
                {
                    return Err(range_write_error("damaged archived joint recovery refusal"));
                }
            }
        }
    }
    Ok(())
}
impl RangeWriteStore {
    pub(crate) fn require_current_joint_handoff_constraints(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
    ) -> Result<()> {
        let range = data.activation_range().id;
        self.require_no_image_install(range)?;
        if self.catalog.backup_fences.contains_key(&range)
            || self
                .catalog
                .handoff_promises
                .get(&range)
                .is_some_and(|promise| promise != data.promise())
        {
            return Err(range_write_error(
                "current joint recovery cannot bypass a DIFFERENT permanent handoff/backup refusal",
            ));
        }
        Ok(())
    }
    fn require_original_joint_recovery_writer(&self, range: RangeId) -> Result<()> {
        let original=self.current_joint_recovery_writers.get(&range).and_then(std::sync::Weak::upgrade)
            .ok_or_else(||range_write_error("original current-joint recovery writer owner unavailable; cold/new-operation rebind is unsupported"))?;
        let actual = self.native_writer_host_state()?;
        if !Arc::ptr_eq(&original, &actual) {
            return Err(range_write_error(
                "current-joint recovery cannot rebase original writer object",
            ));
        }
        original.require_store(&self.root, self.open_id)
    }
    pub(super) fn selected_current_joint_scope(
        &self,
        range: RangeId,
    ) -> Result<Option<&RecoveryFenceScope>> {
        let Some(saved) = self.catalog.recovery_fences.get(&range) else {
            return Ok(None);
        };
        if !matches!(
            saved.scope.purpose,
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            return Ok(None);
        }
        self.current_joint_scope_constraint(&saved.scope)?;
        let original = &self.catalog.current_joint_recoveries.as_ref().unwrap()[&range]
            .original_handoff_promise;
        if self
            .catalog
            .handoff_promises
            .get(&range)
            .is_some_and(|promise| promise != original)
        {
            return Err(range_write_error(
                "current joint recovered open has a different permanent handoff refusal",
            ));
        }
        self.require_original_joint_recovery_writer(range)?;
        Ok(Some(&saved.scope))
    }
    pub(super) fn current_joint_recovered_command(
        &self,
        command: &RangeWriteCommand,
    ) -> Result<bool> {
        let Some(scope) = self.selected_current_joint_scope(command.range_id)? else {
            return Ok(false);
        };
        self.native_activated_open_allowed(command.range_id)?;
        let RecoveryFencePhase::NativeActivated(activated) =
            &self.catalog.recovery_fences[&command.range_id].phase
        else {
            unreachable!("checked actual activation")
        };
        if command.range_epoch != scope.range_epoch
            || command.leader_node_id != scope.initiator_node_id
            || command.index <= activated.completed.point.range_index()
        {
            return Err(range_write_error("current joint recovered writer cannot replay prefix or change current leader/epoch"));
        }
        Ok(true)
    }
    pub(super) fn current_joint_recovered_read(
        &self,
        scope: &crate::distribution_native_read::NativeRangeReadScope,
    ) -> Result<bool> {
        if self.selected_current_joint_scope(scope.range.id)?.is_none() {
            return Ok(false);
        }
        self.native_activated_open_allowed(scope.range.id)?;
        let binding =
            &self.catalog.current_joint_recoveries.as_ref().unwrap()[&scope.range.id].binding;
        if crate::distribution_native_read::canonical_range(&scope.range) != binding.range
            || scope.schema_sha256 != binding.schema_sha256
            || scope.restore_activation != binding.restore
            || scope.members.len() != binding.members.len()
            || scope.members.iter().any(|(id, m)| {
                binding.members.get(id).is_none_or(|b| {
                    b.incarnation != m.incarnation
                        || b.tls_certificate_sha256 != m.active_certificate_sha256
                        || b.pending_tls_certificate_sha256 != m.pending_certificate_sha256
                })
            })
        {
            return Err(range_write_error(
                "current joint recovered read changed protected current domain",
            ));
        }
        Ok(true)
    }
    pub(super) fn archived_joint_certifications(
        &self,
        range: RangeId,
    ) -> Result<BTreeMap<u64, String>> {
        let mut markers = BTreeMap::new();
        if let Some(state) = self
            .catalog
            .current_joint_recoveries
            .as_ref()
            .and_then(|m| m.get(&range))
        {
            for old in state.archived.values() {
                for (index, marker) in &old.fence.historical_certifications {
                    if markers
                        .insert(*index, marker.command_sha256.clone())
                        .is_some_and(|sha| sha != marker.command_sha256)
                    {
                        return Err(range_write_error(
                            "archived joint irreversible certifications conflict",
                        ));
                    }
                }
            }
        }
        Ok(markers)
    }
    pub(super) fn current_joint_scope_constraint(&self, scope: &RecoveryFenceScope) -> Result<()> {
        if matches!(
            scope.purpose,
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            validate_current_joint_catalog(&self.catalog)?;
            if !crate::format::native_current_joint_recovery_active(&self.root)?
                || self
                    .catalog
                    .current_joint_recoveries
                    .as_ref()
                    .and_then(|m| m.get(&scope.range_id))
                    .is_none_or(|s| !s.scope.same_session(scope))
            {
                return Err(range_write_error(
                    "current joint recovery has no exact durable qualified scope",
                ));
            }
        }
        Ok(())
    }
    /// Historical local constraint check only. The caller independently owns a
    /// fresh mutation/observer role and its ORIGINAL operation cancellation.
    pub(super) fn require_current_joint_scope_domain(
        &self,
        db: &crate::BicDb,
        scope: &RecoveryFenceScope,
        parent: &CancellationToken,
    ) -> Result<NativeCurrentJointRecoveryDomain> {
        self.current_joint_scope_constraint(scope)?;
        let fact = db.observe_current_joint_recovery_domain(self, scope.range_id, parent)?;
        if scope.purpose != fact.purpose() {
            return Err(range_write_error(
                "saved current joint scope changed actual protected decision",
            ));
        }
        let saved = &self.catalog.current_joint_recoveries.as_ref().unwrap()[&scope.range_id];
        if &saved.original_handoff_promise != fact.data().promise() {
            return Err(range_write_error(
                "saved current joint operation changed actual original permanent promise",
            ));
        }
        let original = self
            .current_joint_recovery_writers
            .get(&scope.range_id)
            .ok_or_else(|| {
                range_write_error(
                    "current-joint recovery original writer owner missing on this open",
                )
            })?;
        fact.require_original_writer(original)?;
        fact.require_saved_binding(db, &saved.binding)?;
        for old in saved.archived.values() {
            if let Some(binding) = &old.binding {
                fact.require_saved_binding(db, binding)?;
            }
        }
        parent.check()?;
        Ok(fact)
    }
    pub(crate) fn require_current_joint_recovery_domain(
        &self,
        db: &crate::BicDb,
        role: &RecoveryRoleAdmission,
    ) -> Result<NativeCurrentJointRecoveryDomain> {
        role.require_live_request()?;
        let scope = RecoveryFenceScope::from_role(role)?;
        // This local sub-step never renews the role. Outer callers retain and
        // check their original cancellation; the exact role deadline caps IO.
        let token = CancellationToken::new().child_with_deadline(role.deadline());
        let fact = self.require_current_joint_scope_domain(db, &scope, &token)?;
        fact.require_role(role)?;
        role.require_live_request()?;
        Ok(fact)
    }
    pub(crate) fn preflight_current_joint_recovery(
        &self,
        db: &crate::BicDb,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        fact: &NativeCurrentJointRecoveryDomain,
    ) -> Result<()> {
        self.require_no_image_install(scope.range_id)?;
        self.require_native_recovery_identity(role, native)?;
        scope.require_role(role)?;
        fact.require_role(role)?;
        fact.require_current(db, self)?;
        if self.catalog.backup_fences.contains_key(&scope.range_id)
            || scope.recovery_nonce == fact.data().promise().session_id
            || scope.recovery_nonce == fact.data().promise().nonce
        {
            return Err(range_write_error(
                "new joint recovery cannot reuse original handoff identity or backup refusal",
            ));
        }
        if let Some(promise) = self.catalog.handoff_promises.get(&scope.range_id) {
            if promise != fact.data().promise() {
                return Err(range_write_error(
                    "new joint recovery changed an irreversible original handoff promise",
                ));
            }
        }
        let state = self
            .catalog
            .current_joint_recoveries
            .as_ref()
            .and_then(|m| m.get(&scope.range_id));
        if let Some(state) = state {
            self.require_original_joint_recovery_writer(scope.range_id)?;
            if &state.original_handoff_promise != fact.data().promise() {
                return Err(range_write_error(
                    "prior joint operation changed original permanent promise",
                ));
            }
            if state.archived.contains_key(&scope.recovery_nonce) {
                return Err(range_write_error(
                    "retired cross-epoch joint recovery nonce",
                ));
            }
            fact.require_saved_binding(db, &state.binding)?;
        }
        let old = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| {
                range_write_error("completed joint domain lost its original recovery refusal")
            })?;
        old.validate()?;
        if old.scope.cluster_id != scope.cluster_id
            || old.scope.bootstrap_id != scope.bootstrap_id
            || old.scope.range_id != scope.range_id
            || old.local_node_id != *role.local_node_id()
            || old.local_member_incarnation != native.local_member_incarnation()
            || old.scope.metadata_term > scope.metadata_term
            || old.scope.metadata_read_index > scope.metadata_read_index
        {
            return Err(range_write_error("joint recovery changed actual old refusal identity/bootstrap or regressed metadata"));
        }
        if matches!(
            old.scope.purpose,
            MetadataRecoveryRolePurpose::CommittedHandoff { .. }
        ) && old.scope.range_configuration_sha256 != fact.data().old_recovery_range_sha256()?
        {
            return Err(range_write_error(
                "old handoff refusal changed protected OLD semantic domain",
            ));
        }
        if old.scope.range_epoch > scope.range_epoch
            || old.retired_nonces.contains(&scope.recovery_nonce)
            || (!old.scope.same_session(scope) && old.retired_nonces.len() >= 256)
        {
            return Err(range_write_error(
                "joint recovery regressed epoch/replayed nonce/exceeded refusal bound",
            ));
        }
        match &old.scope.purpose {
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id,
                plan_id,
                session_id,
                nonce,
            } if *relocation_id == fact.data().promise().relocation_id
                && *plan_id == fact.data().promise().plan_id
                && *session_id == fact.data().promise().session_id
                && *nonce == fact.data().promise().nonce
                && old.scope.range_epoch == fact.data().old_range().epoch => {}
            MetadataRecoveryRolePurpose::NormalRecovery
                if old.scope.range_epoch == scope.range_epoch
                    && old.scope.range_configuration_sha256 == scope.range_configuration_sha256 => {
            }
            MetadataRecoveryRolePurpose::CurrentJointRecovery {
                joint_decision_sha256,
            } if joint_decision_sha256 == &fact.binding().joint_decision_sha256
                && state.is_some() => {}
            _ => {
                return Err(range_write_error(
                    "old refusal is outside actual protected original/current joint domain",
                ))
            }
        }
        if old.scope.same_session(scope) && !matches!(old.phase, RecoveryFencePhase::Active) {
            return Err(range_write_error(
                "joint prepared/completed operation cannot reset itself",
            ));
        }
        if self
            .catalog
            .ranges
            .get(&scope.range_id)
            .is_none_or(|log| log.current_epoch > scope.range_epoch)
        {
            return Err(range_write_error(
                "joint recovery has absent/regressed raw data epoch",
            ));
        }
        Ok(())
    }
    pub(crate) fn fence_current_joint_recovery(
        &mut self,
        db: &crate::BicDb,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        fact: &NativeCurrentJointRecoveryDomain,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.preflight_current_joint_recovery(db, role, native, scope, fact)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(scope.range_id, frozen)?;
        if frozen.owner() != scope.freeze_owner()? {
            return Err(range_write_error(
                "joint recovery does not own new drained freeze",
            ));
        }
        let old = self.catalog.recovery_fences[&scope.range_id].clone();
        if old.scope.same_session(scope) {
            // Re-fsync EXACT existing refusal; never manufacture a new native
            // point, archive or scope on an exact durable retry. An unresolved
            // catalogue ambiguity remains closed at outer native-fact preflight.
            return self.persist_recovery_catalog();
        }
        let mut archived = self
            .catalog
            .current_joint_recoveries
            .as_ref()
            .and_then(|m| m.get(&scope.range_id))
            .map(|s| s.archived.clone())
            .unwrap_or_default();
        if archived.len() >= 256 {
            return Err(range_write_error("joint recovery archival bound reached"));
        }
        let old_binding = self
            .catalog
            .current_joint_recoveries
            .as_ref()
            .and_then(|m| m.get(&scope.range_id))
            .map(|s| s.binding.clone());
        archived.insert(
            old.scope.recovery_nonce,
            ArchivedJointRefusal {
                fence: old.clone(),
                binding: old_binding,
            },
        );
        let mut retired = old.retired_nonces.clone();
        retired.insert(old.scope.recovery_nonce);
        let state = CurrentJointRecoveryState {
            binding: fact.binding().clone(),
            scope: scope.clone(),
            original_handoff_promise: fact.data().promise().clone(),
            archived,
        };
        if serde_json::to_vec(&state)?.len() > 8 * 1024 * 1024 {
            return Err(range_write_error(
                "joint recovery refusal archive exceeds explicit 8MiB bound",
            ));
        }
        role.require_live_request()?;
        crate::format::enable_native_current_joint_recovery(&self.root)?;
        role.require_live_request()?;
        // From this point every ambiguity remains restrictive. The one atomic
        // catalogue transaction retains raw log/CQ/sparse/abort/promise state.
        self.recovery_persistence_uncertain = true;
        self.native_activated_opens.remove(&scope.range_id);
        self.current_joint_recovery_writers
            .insert(scope.range_id, fact.original_writer());
        self.catalog.format_version = CURRENT_JOINT_CATALOG_FORMAT;
        self.catalog
            .current_joint_recoveries
            .get_or_insert_with(BTreeMap::new)
            .insert(scope.range_id, state);
        self.catalog
            .ranges
            .get_mut(&scope.range_id)
            .unwrap()
            .current_epoch = scope.range_epoch;
        self.catalog.recovery_fences.insert(
            scope.range_id,
            RecoveryFenceRecord {
                scope: scope.clone(),
                phase: RecoveryFencePhase::Active,
                local_node_id: role.local_node_id().clone(),
                local_member_incarnation: role.local_member_incarnation(),
                native_database_incarnation: native.database_incarnation().into(),
                historical_certifications: BTreeMap::new(),
                retired_nonces: retired,
            },
        );
        self.persist_recovery_catalog()
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    // Shape tests only: these values can never construct a native fact, role,
    // fence acknowledgment, NoData quorum, physical ready proof or serving.
    #[test]
    fn native_current_joint_catalogue_requires_present_map_and_native_prerequisites() {
        let root = tempfile::tempdir().unwrap();
        let store = RangeWriteStore::open(
            root.path(),
            ClusterId::new("joint-reader-contract").unwrap(),
            ClusterNodeId::new("n0").unwrap(),
            true,
        )
        .unwrap();
        let legacy = store.catalog.clone();
        for absent in [None, Some(BTreeMap::new())] {
            let mut candidate = legacy.clone();
            candidate.format_version = CURRENT_JOINT_CATALOG_FORMAT;
            candidate.current_joint_recoveries = absent;
            let error = RangeWriteEnvelope::new(candidate)
                .unwrap()
                .verify()
                .unwrap_err();
            assert!(
                error.to_string().contains("current joint recovery"),
                "{error}"
            );
        }
        let mut old = legacy;
        old.current_joint_recoveries = Some(BTreeMap::new());
        assert!(
            RangeWriteEnvelope::new(old).unwrap().verify().is_err(),
            "old format cannot silently acquire current-joint semantics"
        );
    }
    #[test]
    fn native_current_joint_legacy_omission_preserves_verified_catalogue_bytes() {
        let root = tempfile::tempdir().unwrap();
        let store = RangeWriteStore::open(
            root.path(),
            ClusterId::new("joint-legacy-reader").unwrap(),
            ClusterNodeId::new("n0").unwrap(),
            true,
        )
        .unwrap();
        let envelope = RangeWriteEnvelope::new(store.catalog.clone()).unwrap();
        let bytes = serde_json::to_vec(&envelope).unwrap();
        let mut old_view: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(old_view["catalog"]
            .as_object_mut()
            .unwrap()
            .remove("current_joint_recoveries")
            .is_none());
        let reopened: RangeWriteEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(serde_json::to_vec(&reopened).unwrap(), bytes);
        assert_eq!(reopened.verify().unwrap(), store.catalog);
    }
}
