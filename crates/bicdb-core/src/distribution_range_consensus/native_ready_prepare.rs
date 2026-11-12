//! Actual physical prepare for recovery completion. Persisting this promise
//! closes historical mutation; it does not activate membership, serving, or
//! strong reads. A later, independently verified data-quorum decision is required.
use super::*;
use crate::db::{LocalNativeRecoveryReady, NativeRecoveryAdmission};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use recovery_state::RecoveryFencePhase;

pub(super) const NATIVE_READY_CATALOG_FORMAT: u32 = 7;

/// Durable observation only. Neither decoding this record nor reporting its
/// counters constructs the owned physical proof below.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativePreparedPoint {
    format_version: u32,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    native_receipt_sha256: String,
    local_visibility_sequence: u64,
}
impl NativePreparedPoint {
    pub(super) fn validate(&self) -> Result<()> {
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.format_version != 1
            || !digest(&self.logical_genesis_sha256)
            || !digest(&self.logical_history_sha256)
            || !digest(&self.native_receipt_sha256)
            || (self.range_index == 0 && self.logical_history_sha256 != self.logical_genesis_sha256)
        {
            return Err(range_write_error("damaged native-prepared physical point"));
        }
        Ok(())
    }
    pub(super) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(super) fn logical_genesis_sha256(&self) -> &str {
        &self.logical_genesis_sha256
    }
    pub(super) fn logical_history_sha256(&self) -> &str {
        &self.logical_history_sha256
    }
    pub(super) fn native_receipt_sha256(&self) -> &str {
        &self.native_receipt_sha256
    }
    pub(super) fn local_visibility_sequence(&self) -> u64 {
        self.local_visibility_sequence
    }
    fn from_actual(physical: &LocalNativeRecoveryReady) -> Self {
        Self {
            format_version: 1,
            range_index: physical.range_index(),
            logical_genesis_sha256: physical.logical_genesis_sha256().to_owned(),
            logical_history_sha256: physical.logical_history_sha256().to_owned(),
            native_receipt_sha256: physical.native_receipt_sha256().to_owned(),
            local_visibility_sequence: physical.visibility().get(),
        }
    }
}

pub(super) fn validate_native_prepared_catalog(catalog: &RangeWriteCatalog) -> Result<()> {
    if !super::range_write_catalog_format_supported(catalog.format_version) {
        return Err(range_write_error("unknown native catalogue format"));
    }
    for (range_id, record) in &catalog.recovery_fences {
        let (point, minimum_format) = match &record.phase {
            RecoveryFencePhase::NativePrepared(point) => (point, NATIVE_READY_CATALOG_FORMAT),
            RecoveryFencePhase::NativeCompleted(completed) => {
                completed.validate()?;
                (
                    &completed.point,
                    super::native_ready_completion::NATIVE_COMPLETED_CATALOG_FORMAT,
                )
            }
            RecoveryFencePhase::NativeActivated(activated) => {
                activated.validate()?;
                (
                    &activated.completed.point,
                    super::native_activation::NATIVE_ACTIVATED_FORMAT,
                )
            }
            _ => continue,
        };
        {
            point.validate()?;
            let log = catalog
                .ranges
                .get(range_id)
                .ok_or_else(|| range_write_error("prepared range has no actual log"))?;
            if catalog.format_version < minimum_format
                || !matches!(
                    record.scope.purpose,
                    MetadataRecoveryRolePurpose::NormalRecovery
                        | MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
                )
                || if matches!(record.phase, RecoveryFencePhase::NativeActivated(_)) {
                    log.last_index < point.range_index || log.resolved_through < point.range_index
                } else {
                    log.last_index != point.range_index || log.resolved_through != point.range_index
                }
                || log.current_epoch != record.scope.range_epoch
                || catalog.image_install_intents.contains_key(range_id)
                || log
                    .entries
                    .values()
                    .filter(|entry| {
                        !matches!(record.phase, RecoveryFencePhase::NativeActivated(_))
                            || entry.command.index <= point.range_index
                    })
                    .any(|entry| {
                        !matches!(
                            entry.state,
                            RangeWriteState::Applied | RangeWriteState::Aborted
                        )
                    })
            {
                return Err(range_write_error(
                    "native preparation disagrees with actual resolved catalogue",
                ));
            }
        }
    }
    Ok(())
}

/// Owns the SAME physical read pin and freeze used before preparation. No
/// serde, Clone, public fields, watermark constructor, or release-fence method.
/// This attests one local replica; it is not a native-ready quorum.
#[derive(Debug)]
pub(crate) struct PreparedNativeRecoveryReady {
    physical: LocalNativeRecoveryReady,
    scope: RecoveryFenceScope,
    point: NativePreparedPoint,
    store_open_id: Uuid,
    binding: Mutex<NativeReadyCatalogueBinding>,
}
#[derive(Debug)]
struct NativeReadyCatalogueBinding {
    catalog_sha256: String,
    native_completed_serving_sha256: Option<String>,
}
impl PreparedNativeRecoveryReady {
    pub(crate) fn require_completed_for_activation(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<crate::db::NativeRecoveredServingReceipt> {
        self.require_current_under_role(db, store, fresh)?;
        let saved = &store.catalog.recovery_fences[&self.scope.range_id];
        let RecoveryFencePhase::NativeCompleted(point) = &saved.phase else {
            return Err(range_write_error(
                "activation requires actual completed-frozen state",
            ));
        };
        db.observe_native_recovered_serving_for_physical(
            &self.physical,
            &self.scope,
            &point.native_serving_sha256,
        )
    }
    pub(crate) fn require_activated_materialization(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<crate::db::NativeRecoveredServingReceipt> {
        self.validate_activated_refsync(db, store, fresh)?;
        store.require_durable_recovery_catalog()?;
        let RecoveryFencePhase::NativeActivated(activated) =
            &store.catalog.recovery_fences[&self.scope.range_id].phase
        else {
            unreachable!("validated phase")
        };
        db.observe_native_recovered_serving_for_physical(
            &self.physical,
            &self.scope,
            &activated.completed.native_serving_sha256,
        )
    }
    pub(super) fn validate_activated_refsync(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.scope.require_role(fresh)?;
        if matches!(
            fresh.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            store.require_current_joint_recovery_domain(db, fresh)?;
        }
        self.physical.require_owned_materialization_current(db)?;
        store.require_no_image_install(self.scope.range_id)?;
        validate_native_prepared_catalog(&store.catalog)?;
        store.require_native_recovery_identity(fresh, self.physical.native_admission())?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("activation lost its actual saved record"))?;
        let RecoveryFencePhase::NativeActivated(activated) = &saved.phase else {
            return Err(range_write_error("no actual durable native activation"));
        };
        let binding = self
            .binding
            .try_lock()
            .ok_or_else(|| range_write_error("owned native binding busy at activation release"))?;
        let completed_phase = RecoveryFencePhase::NativeCompleted(activated.completed.clone());
        if self.store_open_id != store.open_id
            || !saved.scope.same_session(&self.scope)
            || saved.native_database_incarnation != self.native_database_incarnation()
            || saved.local_node_id != *fresh.local_node_id()
            || saved.local_member_incarnation != fresh.local_member_incarnation()
            || activated.completed.point != self.point
            || self.point != NativePreparedPoint::from_actual(&self.physical)
            || binding.native_completed_serving_sha256.as_deref()
                != Some(activated.completed.native_serving_sha256.as_str())
            || binding.catalog_sha256 != activated.completed_catalog_sha256
            || store.recovery_range_catalog_sha256_with_phase(
                self.scope.range_id,
                Some(&completed_phase),
            )? != activated.completed_catalog_sha256
        {
            return Err(range_write_error(
                "activation changed state beyond the exact saved completed phase",
            ));
        }
        db.observe_native_recovered_serving_for_physical(
            &self.physical,
            &self.scope,
            &activated.completed.native_serving_sha256,
        )?;
        Ok(())
    }
    pub(super) fn actual_prepared_point(&self) -> NativePreparedPoint {
        self.point.clone()
    }
    /// A stage report is meaningful only after the caller has revalidated this
    /// owned proof against the actual store, native materialization and role.
    pub(crate) fn frozen_stage(
        &self,
    ) -> Result<crate::distribution_range_recovery::FrozenNativeReadyStage> {
        let binding = self
            .binding
            .try_lock()
            .ok_or_else(|| range_write_error("owned native readiness binding is busy"))?;
        Ok(if binding.native_completed_serving_sha256.is_some() {
            crate::distribution_range_recovery::FrozenNativeReadyStage::NativeCompletedFrozen
        } else {
            crate::distribution_range_recovery::FrozenNativeReadyStage::NativePrepared
        })
    }
    fn require_binding_current(&self, db: &crate::BicDb, store: &RangeWriteStore) -> Result<()> {
        let binding = self
            .binding
            .try_lock()
            .ok_or_else(|| range_write_error("owned native readiness binding is busy"))?;
        if let Some(sha) = self.require_binding_shape(store, &binding)? {
            db.observe_native_recovered_serving_for_physical(&self.physical, &self.scope, &sha)?;
        }
        Ok(())
    }
    // Catalogue data only. Never a physical validation/current-proof producer.
    fn require_binding_shape_current(&self, store: &RangeWriteStore) -> Result<Option<String>> {
        let binding = self
            .binding
            .try_lock()
            .ok_or_else(|| range_write_error("owned native readiness binding is busy"))?;
        self.require_binding_shape(store, &binding)
    }
    fn require_binding_shape(
        &self,
        store: &RangeWriteStore,
        binding: &NativeReadyCatalogueBinding,
    ) -> Result<Option<String>> {
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("owned native readiness lost its actual refusal"))?;
        if self.store_open_id != store.open_id
            || binding.catalog_sha256 != store.recovery_range_catalog_sha256(self.scope.range_id)?
            || self.point != NativePreparedPoint::from_actual(&self.physical)
        {
            return Err(range_write_error(
                "owned native readiness has stale actual catalogue/materialization",
            ));
        }
        match (&saved.phase, &binding.native_completed_serving_sha256) {
            (RecoveryFencePhase::NativePrepared(point), None) if point == &self.point => {}
            (RecoveryFencePhase::NativeCompleted(completed), Some(sha))
                if completed.point == self.point && &completed.native_serving_sha256 == sha =>
            {
                return Ok(Some(sha.clone()));
            }
            _ => {
                return Err(range_write_error(
                    "owned native readiness phase/binding changed",
                ))
            }
        }
        Ok(None)
    }
    /// Refresh only after actual native-WAL and catalogue completion have BOTH
    /// persisted. It keeps the identical owned pin/freeze/R/H. On ambiguity the
    /// caller must first re-fsync the exact refusal; this grants no activation.
    pub(crate) fn refresh_after_durable_native_completion(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.refresh_completed_native_binding(db, store, fresh)?;
        // A later standalone refresh has no exclusive producer continuation;
        // preserve its full current validation rather than reuse a readback.
        self.require_current_under_role(db, store, fresh)
    }
    /// Full post-catalogue native/physical check, plus exact updated binding.
    /// The returned native receipt is a historical fact, not a cached current
    /// grant; only the uninterrupted actual completion producer reuses it.
    pub(super) fn refresh_completed_native_binding(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<crate::db::NativeRecoveredServingReceipt> {
        #[cfg(test)]
        let _detail_context = crate::distribution_range_handoff::NativeRefreshTraceContext::enter(
            fresh.local_node_id(),
            fresh.deadline(),
        );
        db.with_native_completion_read_scope(self, store, fresh, |completion| {
            self.refresh_completed_native_binding_in_scope(completion)
        })
    }
    fn refresh_completed_native_binding_in_scope(
        &self,
        completion: &crate::db::NativeCompletionReadScope<'_>,
    ) -> Result<crate::db::NativeRecoveredServingReceipt> {
        let db = completion.database();
        let store = completion.store();
        let fresh = completion.role();
        completion.require_identity(self, store, fresh)?;
        #[cfg(test)]
        let mut actual_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-refresh-actual-domain-physical",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        self.scope.require_role(fresh)?;
        #[cfg(test)]
        let fact_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-current-joint-fact-and-original-archives",
        );
        if matches!(
            fresh.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            store.require_current_joint_domain_in_completion_scope(completion)?;
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(fact_trace);
        #[cfg(test)]
        let physical_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-owned-native-pin-and-physical-lineage",
            );
        self.physical.require_owned_pin_live(db)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(physical_trace);
        #[cfg(test)]
        let scope_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-current-role-durable-scope-point",
        );
        store.require_native_ready_scope(fresh, self.physical.native_admission(), &self.scope)?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("completion refresh lost actual refusal"))?;
        let RecoveryFencePhase::NativeCompleted(completed) = &saved.phase else {
            return Err(range_write_error(
                "completion refresh has no actual completed-frozen record",
            ));
        };
        if self.store_open_id != store.open_id
            || completed.point != self.point
            || !saved.scope.same_session(&self.scope)
        {
            return Err(range_write_error(
                "completion refresh changed the actual pinned point/session",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(scope_trace);
        #[cfg(test)]
        let serving_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-exact-native-serving-readback-total",
            );
        let native = completion.observe_serving(&completed.native_serving_sha256)?;
        completion.require_physical_domain_current()?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(serving_trace);
        #[cfg(test)]
        {
            actual_trace.complete();
            drop(actual_trace);
        }
        let updated = NativeReadyCatalogueBinding {
            catalog_sha256: store.recovery_range_catalog_sha256(self.scope.range_id)?,
            native_completed_serving_sha256: Some(completed.native_serving_sha256.clone()),
        };
        *self
            .binding
            .try_lock()
            .ok_or_else(|| range_write_error("completion binding is busy"))? = updated;
        #[cfg(test)]
        let mut binding_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-refresh-current-updated-binding",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        if self.require_binding_shape_current(store)?.as_deref() != Some(native.checksum_sha256()) {
            return Err(range_write_error(
                "refreshed completion binding differs from actual native readback",
            ));
        }
        self.require_produced_completion_boundary(db, store, fresh)?;
        #[cfg(test)]
        binding_trace.complete();
        Ok(native)
    }
    /// Necessary live boundary after a full same-borrow producer check. This
    /// never substitutes for physical/history validation or authorizes a stored
    /// Prepared value on a later call.
    pub(crate) fn require_produced_completion_boundary(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.scope.require_role(fresh)?;
        #[cfg(test)]
        let pin_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-return-boundary-actual-native-pin",
        );
        self.physical.require_owned_pin_live(db)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(pin_trace);
        #[cfg(test)]
        let boundary_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-return-boundary-scope-binding-original-writer",
            );
        store.require_native_ready_scope(fresh, self.physical.native_admission(), &self.scope)?;
        store.require_prepared_native_point_matches(&self.physical)?;
        self.require_binding_shape_current(store)?;
        if matches!(
            fresh.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) && store
            .selected_current_joint_scope(self.scope.range_id)?
            .is_none_or(|scope| !scope.same_session(&self.scope))
        {
            return Err(range_write_error(
                "completed producer changed original writer/session",
            ));
        }
        let result = fresh.require_live_request();
        #[cfg(test)]
        if result.is_ok() {
            crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(boundary_trace);
        }
        result
    }
    // Only an actual journal-held completion scope can use the already read
    // materialization. Later/standalone observers retain the full path below.
    pub(crate) fn require_domain_for_completion_scope(
        &self,
        completion: &crate::db::NativeCompletionReadScope<'_>,
    ) -> Result<()> {
        let db = completion.database();
        let store = completion.store();
        let fresh = completion.role();
        completion.require_identity(self, store, fresh)?;
        if matches!(
            fresh.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            store.require_current_joint_domain_in_completion_scope(completion)?;
        }
        self.physical.require_owned_pin_live(db)?;
        store.require_native_ready_scope(fresh, self.physical.native_admission(), &self.scope)?;
        store.require_prepared_native_point_matches(&self.physical)?;
        if self.store_open_id != store.open_id
            || self.point != NativePreparedPoint::from_actual(&self.physical)
        {
            return Err(range_write_error(
                "prepared proof has stale actual store/physical state",
            ));
        }
        fresh.require_live_request()
    }
    pub(crate) fn require_current_for_completion_scope(
        &self,
        completion: &crate::db::NativeCompletionReadScope<'_>,
    ) -> Result<()> {
        self.require_domain_for_completion_scope(completion)?;
        if let Some(sha) = self.require_binding_shape_current(completion.store())? {
            completion.observe_serving(&sha)?;
        }
        completion.role().require_live_request()
    }
    pub(crate) fn physical_for_completion_scope(&self) -> &LocalNativeRecoveryReady {
        &self.physical
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        self.physical.role()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.point.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.point.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.point.logical_history_sha256
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        &self.point.native_receipt_sha256
    }
    pub(crate) fn local_visibility_sequence(&self) -> u64 {
        self.point.local_visibility_sequence
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        self.physical.native_admission().database_incarnation()
    }
    pub(crate) fn completed_serving_sha_for_activation(
        &self,
        store: &RangeWriteStore,
    ) -> Result<String> {
        match &store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("activation lost actual completed record"))?
            .phase
        {
            RecoveryFencePhase::NativeCompleted(completed) => {
                Ok(completed.native_serving_sha256.clone())
            }
            RecoveryFencePhase::NativeActivated(activated) => {
                Ok(activated.completed.native_serving_sha256.clone())
            }
            _ => Err(range_write_error(
                "activation requires actual completed serving commitment",
            )),
        }
    }
    /// One actual journal-held activation read: exact durable phase and the
    /// unchanged Completed binding, never reinterpret Activated as Ready.
    pub(crate) fn require_current_in_activation_scope(
        &self,
        scope: &crate::db::NativeActivationReadScope<'_>,
    ) -> Result<()> {
        let db = scope.database();
        let store = scope.store();
        let fresh = scope.role();
        scope.require_identity(db, self, store, fresh)?;
        store.require_current_joint_domain_in_activation_scope(scope)?;
        store.require_durable_recovery_catalog()?;
        store.require_no_image_install(self.scope.range_id)?;
        validate_native_prepared_catalog(&store.catalog)?;
        store.require_native_recovery_identity(fresh, self.physical.native_admission())?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("activation scope lost actual refusal"))?;
        let binding = self
            .binding
            .try_lock()
            .ok_or_else(|| range_write_error("activation scope owned binding busy"))?;
        let (completed, completed_digest) = match &saved.phase {
            RecoveryFencePhase::NativeCompleted(point) => (
                point,
                store.recovery_range_catalog_sha256(self.scope.range_id)?,
            ),
            RecoveryFencePhase::NativeActivated(activated) => {
                let completed_phase =
                    RecoveryFencePhase::NativeCompleted(activated.completed.clone());
                let actual = store.recovery_range_catalog_sha256_with_phase(
                    self.scope.range_id,
                    Some(&completed_phase),
                )?;
                if actual != activated.completed_catalog_sha256 {
                    return Err(range_write_error(
                        "activation scope changed original Completed catalogue commitment",
                    ));
                }
                (&activated.completed, actual)
            }
            _ => {
                return Err(range_write_error(
                    "activation scope needs actual Completed or Activated phase",
                ))
            }
        };
        if self.store_open_id != store.open_id
            || !saved.scope.same_session(&self.scope)
            || saved.native_database_incarnation != self.native_database_incarnation()
            || saved.local_node_id != *fresh.local_node_id()
            || saved.local_member_incarnation != fresh.local_member_incarnation()
            || completed.point != self.point
            || self.point != NativePreparedPoint::from_actual(&self.physical)
            || binding.catalog_sha256 != completed_digest
            || binding.native_completed_serving_sha256.as_deref()
                != Some(completed.native_serving_sha256.as_str())
        {
            return Err(range_write_error(
                "activation scope changed actual Completed materialization/binding/session",
            ));
        }
        scope.require_serving(&completed.native_serving_sha256)?;
        scope.parent().check()?;
        fresh.require_live_request()
    }
    /// Only the actual nonescaping read-only journal scope may reuse lineage.
    pub(crate) fn require_current_in_observer_scope(
        &self,
        scope: &crate::db::NativeReadyObserverReadScope<'_>,
    ) -> Result<()> {
        let db = scope.database();
        let store = scope.store();
        let observer = scope.observer();
        let requested_scope = &scope.request().scope;
        scope.require_identity(db, self, store, observer, scope.request())?;
        store.require_current_joint_domain_in_observer_scope(scope)?;
        store.require_durable_recovery_catalog()?;
        store.require_native_phase_format(self.scope.range_id)?;
        store.require_no_image_install(self.scope.range_id)?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("observer scope lost actual saved recovery fence"))?;
        if !self.scope.same_session(requested_scope)
            || !saved.scope.same_session(requested_scope)
            || saved.native_database_incarnation != self.native_database_incarnation()
            || saved.local_node_id != *observer.local_node_id()
            || saved.local_member_incarnation != observer.local_member_incarnation()
            || store.catalog.node_id != *observer.local_node_id()
            || store.catalog.cluster_id != *observer.cluster_id()
            || self.store_open_id != store.open_id
            || self.point != NativePreparedPoint::from_actual(&self.physical)
        {
            return Err(range_write_error(
                "observer scope changed actual Store/native/session identity",
            ));
        }
        if let Some(sha) = self.require_binding_shape_current(store)? {
            scope.require_serving(&sha)?;
        }
        observer.require_scope(requested_scope)?;
        scope.parent().check()
    }
    /// A peer's independently challenged READ-ONLY role can inspect the exact
    /// prepared state. It cannot become the coordinator's mutation admission.
    pub(crate) fn require_current_for_observer(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        observer: &crate::distribution_range_recovery::RecoveryObservationRoleAdmission,
        requested_scope: &RecoveryFenceScope,
    ) -> Result<()> {
        observer.require_scope(requested_scope)?;
        if matches!(
            requested_scope.purpose,
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            #[cfg(test)]
            let domain_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
                "observer-current-joint-domain",
            );
            store.require_current_joint_scope_domain(
                db,
                requested_scope,
                &CancellationToken::uncancelable().child_with_deadline(observer.deadline()),
            )?;
            #[cfg(test)]
            crate::distribution_range_recovery::NativeProbeTraceSpan::finish(domain_trace);
        }
        #[cfg(test)]
        let physical_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
            "observer-owned-physical",
        );
        observer.require_native_identity(self.physical.native_admission())?;
        self.physical.require_owned_materialization_current(db)?;
        #[cfg(test)]
        crate::distribution_range_recovery::NativeProbeTraceSpan::finish(physical_trace);
        #[cfg(test)]
        let catalog_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
            "observer-catalogue-identity",
        );
        store.require_durable_recovery_catalog()?;
        store.require_native_phase_format(self.scope.range_id)?;
        store.require_no_image_install(self.scope.range_id)?;
        let saved = store
            .catalog
            .recovery_fences
            .get(&self.scope.range_id)
            .ok_or_else(|| range_write_error("prepared observation lost its actual saved fence"))?;
        if !self.scope.same_session(requested_scope)
            || !saved.scope.same_session(requested_scope)
            || saved.native_database_incarnation != self.native_database_incarnation()
            || saved.local_node_id != *observer.local_node_id()
            || saved.local_member_incarnation != observer.local_member_incarnation()
            || store.catalog.node_id != *observer.local_node_id()
            || store.catalog.cluster_id != *observer.cluster_id()
            || self.store_open_id != store.open_id
            || self.point != NativePreparedPoint::from_actual(&self.physical)
        {
            return Err(range_write_error(
                "read-only prepared observation has stale native/store/session identity",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_recovery::NativeProbeTraceSpan::finish(catalog_trace);
        #[cfg(test)]
        let mut binding_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
            "observer-full-binding-current",
        );
        let result = self.require_binding_current(db, store);
        #[cfg(test)]
        if result.is_ok() {
            if let Some(trace) = &mut binding_trace {
                trace.complete();
            }
        }
        result
    }
    pub(crate) fn require_current(&self, db: &crate::BicDb, store: &RangeWriteStore) -> Result<()> {
        self.physical.require_physical_current(db)?;
        self.require_current_under_role(db, store, self.role())
    }
    /// A separately challenged fresh role may use the persisted preparation;
    /// this never renews the old request or treats its context as a time lease.
    pub(crate) fn require_current_under_role(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        fresh.require_live_request()?;
        self.scope.require_role(fresh)?;
        if matches!(
            fresh.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            store.require_current_joint_recovery_domain(db, fresh)?;
        }
        self.physical.require_owned_materialization_current(db)?;
        store.require_native_ready_scope(fresh, self.physical.native_admission(), &self.scope)?;
        store.require_prepared_native_point_matches(&self.physical)?;
        if self.store_open_id != store.open_id
            || self.point != NativePreparedPoint::from_actual(&self.physical)
        {
            return Err(range_write_error(
                "prepared proof has stale actual store/physical state",
            ));
        }
        self.require_binding_current(db, store)
    }
}

impl RangeWriteStore {
    /// Common read-only validation, deliberately separate from the Active-only
    /// authority used by historical certification and Apply.
    pub(super) fn require_native_ready_scope(
        &self,
        role: &RecoveryRoleAdmission,
        native: &NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
    ) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        self.require_native_phase_format(scope.range_id)?;
        self.current_joint_scope_constraint(scope)?;
        self.require_no_image_install(scope.range_id)?;
        self.require_native_recovery_identity(role, native)?;
        scope.require_role(role)?;
        let record = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("native-ready observation has no actual fence"))?;
        if !record.scope.same_session(scope)
            || record.native_database_incarnation != native.database_incarnation()
            || record.local_node_id != *role.local_node_id()
            || record.local_member_incarnation != role.local_member_incarnation()
            || !matches!(
                record.phase,
                RecoveryFencePhase::Active
                    | RecoveryFencePhase::NativePrepared(_)
                    | RecoveryFencePhase::NativeCompleted(_)
            )
        {
            return Err(range_write_error(
                "native-ready scope is completed, superseded, or from another native open",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_prepared_native_point_matches(
        &self,
        physical: &LocalNativeRecoveryReady,
    ) -> Result<()> {
        let record = self
            .catalog
            .recovery_fences
            .get(&physical.role().current_range().id)
            .ok_or_else(|| range_write_error("actual physical readiness has no recovery fence"))?;
        let point = match &record.phase {
            RecoveryFencePhase::NativePrepared(point) => Some(point),
            RecoveryFencePhase::NativeCompleted(completed) => Some(&completed.point),
            _ => None,
        };
        if let Some(point) = point {
            if point != &NativePreparedPoint::from_actual(physical) {
                return Err(range_write_error(
                    "actual physical state changed from native-prepared promise",
                ));
            }
        }
        Ok(())
    }
    /// Re-fsync ONLY the exact saved prepared refusal after a lost proof or
    /// ambiguous write. This returns no physical/quorum/serving certificate and
    /// does not change phase, point, nonce, native open, or membership. A later
    /// physical producer must independently re-attest the actual materialization.
    pub(crate) fn refsync_native_prepared_refusal(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        fresh: &RecoveryRoleAdmission,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        let token = parent.child_with_deadline(fresh.deadline());
        token.check()?;
        fresh.require_live_request()?;
        native.require_current(db)?;
        self.require_native_recovery_identity(fresh, native)?;
        let scope = RecoveryFenceScope::from_role(fresh)?;
        self.require_no_image_install(scope.range_id)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&scope.range_id)
            .ok_or_else(|| range_write_error("prepared re-fsync has no saved refusal"))?;
        saved.scope.require_role(fresh)?;
        if !saved.scope.same_session(&scope)
            || !matches!(
                saved.phase,
                RecoveryFencePhase::NativePrepared(_) | RecoveryFencePhase::NativeCompleted(_)
            )
            || saved.native_database_incarnation != native.database_incarnation()
            || saved.local_node_id != *fresh.local_node_id()
            || saved.local_member_incarnation != fresh.local_member_incarnation()
            || frozen.owner() != scope.freeze_owner()?
        {
            return Err(range_write_error(
                "prepared re-fsync would change the actual saved session/native identity",
            ));
        }
        db.range_mutation_gate()
            .validate_frozen(scope.range_id, frozen)?;
        validate_native_prepared_catalog(&self.catalog)?;
        self.persist_recovery_catalog()?;
        token.check()?;
        fresh.require_live_request()
    }
    #[cfg(test)]
    pub(crate) fn test_prepare_native_recovery_ready_with(
        &mut self,
        db: &crate::BicDb,
        physical: LocalNativeRecoveryReady,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<PreparedNativeRecoveryReady> {
        self.prepare_native_recovery_ready_with(db, physical, write)
    }
    pub(crate) fn prepare_native_recovery_ready(
        &mut self,
        db: &crate::BicDb,
        physical: LocalNativeRecoveryReady,
    ) -> Result<PreparedNativeRecoveryReady> {
        self.prepare_native_recovery_ready_with(db, physical, crate::storage::write_atomic)
    }
    fn prepare_native_recovery_ready_with(
        &mut self,
        db: &crate::BicDb,
        physical: LocalNativeRecoveryReady,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<PreparedNativeRecoveryReady> {
        #[cfg(test)]
        let mut physical_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-prepare-catalogue-preflight-physical-current",
                physical.role().local_node_id(),
                physical.role().deadline(),
            );
        physical.require_current(db, self)?;
        #[cfg(test)]
        {
            physical_trace.complete();
            drop(physical_trace);
        }
        if !matches!(
            physical.role().purpose(),
            MetadataRecoveryRolePurpose::NormalRecovery
                | MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            return Err(range_write_error(
                "normal native preparation cannot activate a handoff configuration",
            ));
        }
        self.require_prepared_native_point_matches(&physical)?;
        let scope = RecoveryFenceScope::from_role(physical.role())?;
        let point = NativePreparedPoint::from_actual(&physical);
        point.validate()?;
        let finalized_image_range = if let Some(image) = physical.finalized_image() {
            // The physical producer has JUST validated this actual finalized
            // native image under its N-owned read pin while the install intent
            // is still present. Replacing that exact refusal is one catalogue
            // transition; there is no NoImage gap for historical/user writes.
            let fence = image.intent_fence();
            let saved = &self.catalog.recovery_fences[&scope.range_id];
            let intent = self
                .catalog
                .image_install_intents
                .get(&scope.range_id)
                .ok_or_else(|| {
                    range_write_error("finalized preparation lost its exact image intent")
                })?;
            if !image.scope().same_session(&scope)
                || !matches!(saved.phase, RecoveryFencePhase::Active)
                || intent.attempt_id != fence.attempt_id()
                || intent.session_id != fence.session_id()
                || intent.nonce != fence.nonce()
                || intent.sha256()? != fence.intent_sha256()
            {
                return Err(range_write_error(
                    "finalized preparation would retire another image/session",
                ));
            }
            Some(scope.range_id)
        } else {
            None
        };
        // Even a later structural validation failure must never expose a
        // changed in-memory refusal as if it had already been fsynced.
        self.recovery_persistence_uncertain = true;
        let native_completed_serving_sha256 = match &self.catalog.recovery_fences[&scope.range_id]
            .phase
        {
            RecoveryFencePhase::NativeCompleted(completed) => {
                db.observe_native_recovered_serving_for_physical(
                    &physical,
                    &scope,
                    &completed.native_serving_sha256,
                )?;
                Some(completed.native_serving_sha256.clone())
            }
            _ => {
                self.catalog
                    .recovery_fences
                    .get_mut(&scope.range_id)
                    .ok_or_else(|| range_write_error("native preparation lost its actual fence"))?
                    .phase = RecoveryFencePhase::NativePrepared(point.clone());
                None
            }
        };
        if let Some(range_id) = finalized_image_range {
            self.catalog.image_install_intents.remove(&range_id);
        }
        self.catalog.format_version = self.catalog.format_version.max(NATIVE_READY_CATALOG_FORMAT);
        validate_native_prepared_catalog(&self.catalog)?;
        // On any rename/directory-sync ambiguity retain this refusal in memory
        // and on disk. No proof exists until actual persistence succeeds.
        #[cfg(test)]
        let mut persist_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-prepare-catalogue-fsync",
                physical.role().local_node_id(),
                physical.role().deadline(),
            );
        self.persist_recovery_catalog_with(write)?;
        #[cfg(test)]
        {
            persist_trace.complete();
            drop(persist_trace);
            eprintln!("ACTUAL_RECOVERY_PREPARE_CATALOGUE_PERSISTED node={} nonce={} point={:?} uncertain={} remaining_ms={}", physical.role().local_node_id(), scope.recovery_nonce, point, self.recovery_persistence_uncertain, physical.role().deadline().saturating_duration_since(std::time::Instant::now()).as_millis());
        }
        let prepared = PreparedNativeRecoveryReady {
            physical,
            scope: scope.clone(),
            point,
            store_open_id: self.open_id,
            binding: Mutex::new(NativeReadyCatalogueBinding {
                catalog_sha256: self.recovery_range_catalog_sha256(scope.range_id)?,
                native_completed_serving_sha256,
            }),
        };
        #[cfg(test)]
        let mut current_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-prepare-after-persist-current",
                prepared.role().local_node_id(),
                prepared.role().deadline(),
            );
        prepared.require_current(db, self)?;
        #[cfg(test)]
        current_trace.complete();
        Ok(prepared)
    }
}
