//! Exact real Registered receiver refusal path. No ordinary writer exception.
use super::*;
use crate::distribution_native_host::NativeAssignedRegisteredClosedFence;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
impl RangeWriteStore {
    pub(crate) fn preflight_registered_closed_fence(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
    ) -> Result<()> {
        capability.require_database_store(db, self)?;
        self.preflight_registered_closed_constraints(db, native, capability)?;
        capability.require_database_store(db, self)
    }
    fn preflight_registered_closed_constraints(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
    ) -> Result<()> {
        let scope = capability.scope();
        // All absence checks below are necessary constraints, NOT the
        // producer: only the actual sealed receiver capability enters here.
        if !self.fsync
            || !self.catalogue_loaded_at_open
            || self.catalog.native_writer_enforcement_version != Some(1)
            || self.catalog.format_version < NATIVE_ABORT_CATALOG_FORMAT
            || !range_write_catalog_format_supported(self.catalog.format_version)
            || !crate::format::native_writer_enrollment_active(&self.root)?
            || !crate::format::native_catalogue_custody_active(&self.root)?
            || !crate::format::native_abort_history_active(&self.root)?
            || self.root.canonicalize()? != db.data_path().canonicalize()?
        {
            return Err(range_write_error("assigned CLOSED receiver needs actual existing native catalogue custody/reader fences"));
        }
        self.require_actual_abort_catalogue(db)?;
        self.catalog
            .native_abort_protocol
            .as_ref()
            .ok_or_else(|| {
                range_write_error("assigned receiver lacks actual protected catalogue identity")
            })?
            .require_unstarted_registered_receiver()?;
        if !db.native_abort_slot_inventory()?.is_empty()
            || !self.catalog.backup_fences.is_empty()
            || !self.catalog.handoff_promises.is_empty()
            || !self.catalog.image_install_intents.is_empty()
            || !self.catalog.native_image_bases.is_empty()
            || self
                .catalog
                .sparse_committed_decisions
                .as_ref()
                .is_some_and(|m| !m.is_empty())
            || self
                .catalog
                .installing_committed_certifications
                .as_ref()
                .is_some_and(|m| !m.is_empty())
            || self
                .catalog
                .installing_recovery_handovers
                .as_ref()
                .is_some_and(|m| !m.is_empty())
            || self
                .catalog
                .native_joint_handoffs
                .as_ref()
                .is_some_and(|m| !m.is_empty())
            || self
                .catalog
                .current_joint_recoveries
                .as_ref()
                .is_some_and(|m| !m.is_empty())
            || self.catalog.ranges.values().any(|r| {
                r.last_index != 0
                    || r.resolved_through != 0
                    || r.compacted_through != 0
                    || !r.entries.is_empty()
            })
            || self.catalog.recovery_fences.values().any(|r| {
                !matches!(r.phase, RecoveryFencePhase::Active)
                    || !matches!(
                        r.scope.purpose,
                        MetadataRecoveryRolePurpose::CommittedHandoff { .. }
                    )
                    || !r.historical_certifications.is_empty()
                    || !r.retired_nonces.is_empty()
                    || r.local_node_id != *native.local_node_id()
                    || r.local_member_incarnation != native.local_member_incarnation()
                    || r.native_database_incarnation != native.database_incarnation()
            })
        {
            return Err(range_write_error("assigned CLOSED receiver nonempty irreversible/image/native domain unsupported; obligations retained"));
        }
        // Explicit initial lane only. A later second-range image on this same
        // populated receiver needs an obligation-preserving continuation.
        if db.current_commit_seq() != 0 {
            return Err(range_write_error("assigned CLOSED initial receiver has business history; populated continuation unsupported"));
        }
        for range in capability
            .role()
            .current_range()
            .replicas
            .iter()
            .filter(|r| r.node_id == *capability.role().local_node_id())
        {
            if range.role != RangeReplicaRole::Learner {
                return Err(range_write_error("assigned receiver is not a data Learner"));
            }
        }
        db.require_original_registered_receiver_domain(native)?;
        self.preflight_recovery_nonce_fields(scope)?;
        self.preflight_recovery_fence_constraints(capability.role(), native, scope)?;
        capability.require_live()
    }
    pub(crate) fn persist_registered_closed_fence(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.persist_registered_closed_fence_inner(
            db,
            native,
            capability,
            frozen,
            crate::storage::write_atomic,
        )
    }
    fn persist_registered_closed_fence_inner(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &FrozenRangeMutationPermit,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<()> {
        db.range_mutation_gate()
            .validate_quiescent_frozen(capability.scope().range_id, frozen)?;
        if frozen.owner() != capability.scope().freeze_owner()? {
            return Err(range_write_error(
                "assigned CLOSED fence lost original actual freeze owner",
            ));
        }
        self.preflight_registered_closed_fence(db, native, capability)?;
        self.install_recovery_fence_record(capability.role(), native, capability.scope());
        self.recovery_persistence_uncertain = true;
        capability.with_original_hook_persist(db, |scope| {
            self.persist_registered_closed_catalog(db, native, capability, frozen, scope, write)
        })?;
        self.recovery_persistence_uncertain = false;
        self.require_registered_closed_fence_persisted(db, native, capability)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(capability.scope().range_id, frozen)?;
        capability.require_live()
    }
    /// Refusal-only sink: no call to the ordinary writer-based persist path.
    /// Requires the exact actual borrowed hook/admission exclusion at both sides
    /// of the real file+directory fsync; never returns a durable/live grant.
    fn persist_registered_closed_catalog(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &FrozenRangeMutationPermit,
        scope: &crate::db::NativeRegisteredClosedPersistScope<'_, '_>,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<()> {
        scope.require_current(db, self)?;
        native.require_current(db)?;
        self.preflight_registered_closed_constraints(db, native, capability)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(capability.scope().range_id, frozen)?;
        self.require_active_recovery_scope_record(capability.scope())?;
        let envelope = RangeWriteEnvelope::new(self.catalog.clone())?;
        write(
            &self.root.join(DEFAULT_RANGE_WRITE_LOG),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )?;
        // Real post-write readback verifies checksum and every current field.
        // A post-write failure retains both the memory refusal and uncertain flag.
        let file = open_range_catalog_no_follow(&self.root.join(DEFAULT_RANGE_WRITE_LOG))?;
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "registered CLOSED catalogue readback exceeds bound",
            ));
        }
        let actual = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        if actual != self.catalog {
            return Err(range_write_error(
                "registered CLOSED actual persisted refusal differs",
            ));
        }
        scope.require_current(db, self)?;
        native.require_current(db)?;
        self.preflight_registered_closed_constraints(db, native, capability)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(capability.scope().range_id, frozen)?;
        self.require_active_recovery_scope_record(capability.scope())?;
        capability.require_live()
    }
    fn require_registered_closed_fence_persisted(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
    ) -> Result<()> {
        self.preflight_registered_closed_fence(db, native, capability)?;
        self.require_durable_recovery_catalog()?;
        self.require_active_recovery_scope_record(capability.scope())
    }
    pub(crate) fn observe_registered_closed_fence_obligations(
        &self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<crate::distribution_range_recovery::RecoveryImageHistoryObligationsV1> {
        self.require_registered_closed_fence_persisted(db, native, capability)?;
        let observed =
            self.observe_recovery_image_obligations_body(db, native, capability.scope(), frozen)?;
        self.require_registered_closed_fence_persisted(db, native, capability)?;
        capability.require_live()?;
        Ok(observed)
    }
}

#[cfg(test)]
impl RangeWriteStore {
    pub(crate) fn test_registered_closed_sink(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &FrozenRangeMutationPermit,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<()> {
        self.persist_registered_closed_fence_inner(db, native, capability, frozen, write)
    }
    pub(crate) fn test_registered_closed_persistence_state(&self) -> Result<serde_json::Value> {
        let file = open_range_catalog_no_follow(&self.root.join(DEFAULT_RANGE_WRITE_LOG))?;
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error("test catalogue readback bound"));
        }
        let actual = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        Ok(
            serde_json::json!({"uncertain":self.recovery_persistence_uncertain,
            "actual_checksum_verified":true,"actual_equals_memory":actual==self.catalog,
            "actual_catalogue":actual,"memory_catalogue":self.catalog}),
        )
    }
}
