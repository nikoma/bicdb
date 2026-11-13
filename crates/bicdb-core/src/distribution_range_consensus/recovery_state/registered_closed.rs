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
        let point = db.native_range_prefix_observation(scope.range_id)?;
        if point.native_applied_index.is_some()
            || point.pending_command_index.is_some()
            || point.materialized_pending_index.is_some()
            || point.invalidated
        {
            return Err(range_write_error("assigned CLOSED initial target is not UNKNOWN/without pending; no floor may be reset"));
        }
        self.preflight_recovery_nonce_fields(scope)?;
        self.preflight_recovery_fence_constraints(capability.role(), native, scope)?;
        capability.require_database_store(db, self)
    }
    pub(crate) fn persist_registered_closed_fence(
        &mut self,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        capability: &NativeAssignedRegisteredClosedFence<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        db.range_mutation_gate()
            .validate_quiescent_frozen(capability.scope().range_id, frozen)?;
        if frozen.owner() != capability.scope().freeze_owner()? {
            return Err(range_write_error(
                "assigned CLOSED fence lost original actual freeze owner",
            ));
        }
        self.preflight_registered_closed_fence(db, native, capability)?;
        capability.with_original_hook_persist(db, || {
            self.persist_recovery_fence_record(
                capability.role(),
                native,
                capability.scope(),
                crate::storage::write_atomic,
            )
        })?;
        self.require_registered_closed_fence_persisted(db, native, capability)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(capability.scope().range_id, frozen)?;
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
