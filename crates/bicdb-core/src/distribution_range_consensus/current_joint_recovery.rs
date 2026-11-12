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
    // Survivors originally promise through protected joint old-close, which
    // need not create a generic RecoveryFenceRecord. Preserve actual absence.
    fence: Option<RecoveryFenceRecord>,
    // Every archived representation keeps its actual original physical floor,
    // exact local Option promise and protected closure identity.
    binding: NativeCurrentJointBinding,
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
            if let Some(local) = &state.binding.local_handoff_promise {
                local.validate()?;
            }
            if state.binding.local_handoff_promise.as_ref() != catalog.handoff_promises.get(range)
                || state.binding.local_handoff_promise.is_some()
                    != state.binding.local_old_close_sha256.is_some()
                || state
                    .binding
                    .local_old_close_sha256
                    .as_ref()
                    .is_some_and(|sha| {
                        sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit())
                    })
            {
                return Err(range_write_error(
                    "current joint archive lost exact original LOCAL promise/closure constraint",
                ));
            }
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
                if nonce.is_nil()
                    || *nonce == state.scope.recovery_nonce
                    || archived.binding.joint_decision_sha256 != state.binding.joint_decision_sha256
                    || archived.binding.birth_id != state.binding.birth_id
                    || archived.binding.store_id != state.binding.store_id
                {
                    return Err(range_write_error("damaged archived joint recovery binding"));
                }
                if let Some(old) = &archived.fence {
                    old.validate()?;
                    if *nonce != old.scope.recovery_nonce
                        || old.scope.range_id != *range
                        || old.scope.cluster_id != catalog.cluster_id
                        || old.local_node_id != catalog.node_id
                    {
                        return Err(range_write_error(
                            "damaged archived optional generic recovery refusal",
                        ));
                    }
                } else if *nonce != state.original_handoff_promise.nonce
                    || archived.binding.local_handoff_promise.is_none()
                    || archived.binding.local_old_close_sha256.is_none()
                {
                    return Err(range_write_error("missing generic refusal lacks exact original permanent native joint closure"));
                }
            }
        }
    }
    Ok(())
}
impl RangeWriteStore {
    /// Constraint DATA only. Semantic request equality does NOT establish the
    /// local floor: the DB producer separately verifies the protected old-close
    /// record/current+archive and global OLD physical materialization facts.
    pub(crate) fn current_joint_local_handoff_promise(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
    ) -> Result<Option<RangeHandoffPromise>> {
        let range = data.activation_range().id;
        self.require_no_image_install(range)?;
        let local = self.catalog.handoff_promises.get(&range);
        if self.catalog.backup_fences.contains_key(&range)
            || local.is_some_and(|promise| {
                !promise.same_request(data.promise())
                    || promise.resolved_through > data.range_index()
            })
        {
            return Err(range_write_error("current joint recovery cannot bypass a DIFFERENT or uncovered permanent handoff/backup refusal"));
        }
        Ok(local.cloned())
    }
    pub(crate) fn require_current_joint_local_closure_pointer(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        old_close: Option<&str>,
    ) -> Result<()> {
        let state = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&data.activation_range().id))
            .ok_or_else(|| {
                range_write_error("current joint recovery lost actual local joint state")
            })?;
        state.require_current_joint_closure_pointer(data, old_close)
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
            .binding
            .local_handoff_promise;
        if self.catalog.handoff_promises.get(&range) != original.as_ref() {
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
                for (index, marker) in old
                    .fence
                    .iter()
                    .flat_map(|fence| fence.historical_certifications.iter())
                {
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
        #[cfg(test)]
        let fact_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-joint-fact-constructor",
        );
        let fact = db.observe_current_joint_recovery_domain(self, scope.range_id, parent)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(fact_trace);
        #[cfg(test)]
        let archive_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-joint-original-writer-and-all-archived-floors",
            );
        self.require_current_joint_scope_against_fact(db, scope, parent, &fact)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(archive_trace);
        Ok(fact)
    }
    fn require_current_joint_scope_against_fact(
        &self,
        db: &crate::BicDb,
        scope: &RecoveryFenceScope,
        parent: &CancellationToken,
        fact: &NativeCurrentJointRecoveryDomain,
    ) -> Result<()> {
        self.current_joint_scope_constraint(scope)?;
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
            fact.require_saved_binding(db, &old.binding)?;
        }
        parent.check()
    }
    pub(crate) fn require_current_joint_domain_in_completion_scope(
        &self,
        completion: &crate::db::NativeCompletionReadScope<'_>,
    ) -> Result<NativeCurrentJointRecoveryDomain> {
        completion.require_identity(completion.prepared(), self, completion.role())?;
        let role = completion.role();
        let scope = RecoveryFenceScope::from_role(role)?;
        let fact = completion.observe_current_joint_domain()?;
        let token = CancellationToken::uncancelable().child_with_deadline(role.deadline());
        self.require_current_joint_scope_against_fact(
            completion.database(),
            &scope,
            &token,
            &fact,
        )?;
        fact.require_role(role)?;
        role.require_live_request()?;
        Ok(fact)
    }
    pub(crate) fn require_current_joint_domain_in_observer_scope(
        &self,
        observer: &crate::db::NativeReadyObserverReadScope<'_>,
    ) -> Result<()> {
        observer.require_identity(
            observer.database(),
            observer.prepared(),
            self,
            observer.observer(),
            observer.request(),
        )?;
        self.require_current_joint_scope_against_fact(
            observer.database(),
            &observer.request().scope,
            observer.parent(),
            observer.joint()?,
        )
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
        let token = CancellationToken::uncancelable().child_with_deadline(role.deadline());
        let fact = self.require_current_joint_scope_domain(db, &scope, &token)?;
        fact.require_role(role)?;
        role.require_live_request()?;
        Ok(fact)
    }
    /// Decoder-only negative derived from the actual produced catalogue. No
    /// live state is removed and this returns no admission/role/proof.
    #[cfg(test)]
    pub(crate) fn test_missing_current_joint_generic_custody(&self, range: RangeId) -> Result<()> {
        let saved = self
            .catalog
            .recovery_fences
            .get(&range)
            .ok_or_else(|| range_write_error("actual current fence prerequisite missing"))?;
        self.current_joint_scope_constraint(&saved.scope)?;
        let mut damaged = self.catalog.clone();
        damaged.recovery_fences.remove(&range);
        validate_current_joint_catalog(&damaged)
    }
    fn next_joint_retired_nonces(
        &self,
        scope: &RecoveryFenceScope,
        fact: &NativeCurrentJointRecoveryDomain,
    ) -> Result<BTreeSet<Uuid>> {
        let old = self.catalog.recovery_fences.get(&scope.range_id);
        let mut retired = old
            .map(|old| old.retired_nonces.clone())
            .unwrap_or_default();
        if let Some(old) = old.filter(|old| !old.scope.same_session(scope)) {
            retired.insert(old.scope.recovery_nonce);
        }
        retired.insert(fact.data().promise().nonce);
        retired.insert(fact.data().promise().session_id);
        if retired.len() > 256 || retired.contains(&scope.recovery_nonce) {
            return Err(range_write_error(
                "joint recovery retired nonce bound/replay including original native promise",
            ));
        }
        Ok(retired)
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
        if self.catalog.handoff_promises.get(&scope.range_id)
            != fact.binding().local_handoff_promise.as_ref()
        {
            return Err(range_write_error(
                "new joint recovery changed the exact actual LOCAL handoff promise",
            ));
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
        self.next_joint_retired_nonces(scope, fact)?;
        if let Some(old) = self.catalog.recovery_fences.get(&scope.range_id) {
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
            ) && old.scope.range_configuration_sha256
                != fact.data().old_recovery_range_sha256()?
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
                        && old.scope.range_configuration_sha256
                            == scope.range_configuration_sha256 => {}
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
        } else {
            // True original refusal on a surviving OLD member is the exact
            // local permanent promise + protected OLD closure already checked
            // by the actual fact producer. Do not invent a prior generic scope.
            if state.is_some()
                || fact.binding().local_handoff_promise.is_none()
                || fact.binding().local_old_close_sha256.is_none()
                || !fact.data().old_members().contains_key(role.local_node_id())
            {
                return Err(range_write_error("missing original generic refusal has no exact protected surviving-member custody"));
            }
            if self.catalog.recovery_fences.len() >= recovery_state::MAX_RECOVERY_FENCES {
                return Err(range_write_error("recovery refusal catalog bound reached"));
            }
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
        let old = self.catalog.recovery_fences.get(&scope.range_id).cloned();
        if old
            .as_ref()
            .is_some_and(|old| old.scope.same_session(scope))
        {
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
            .map(|state| state.binding.clone())
            .unwrap_or_else(|| fact.binding().clone());
        let archived_nonce = old
            .as_ref()
            .map(|old| old.scope.recovery_nonce)
            .unwrap_or(fact.data().promise().nonce);
        archived.insert(
            archived_nonce,
            ArchivedJointRefusal {
                fence: old,
                binding: old_binding,
            },
        );
        let retired = self.next_joint_retired_nonces(scope, fact)?;
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
