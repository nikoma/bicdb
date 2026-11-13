//! Exact actual selected catalogue absence; never a synthetic R=0 progress.
use super::*;

pub(crate) struct NativeRegisteredRangeCatalogueAbsence {
    open: RangeWriteStoreOpenGuard,
    range: RangeId,
    catalogue: Uuid,
    birth: Uuid,
    birth_sha256: String,
    refusal: RecoveryFenceRecord,
    raw: RangeWriteLog,
}
impl NativeRegisteredRangeCatalogueAbsence {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.open.id()
    }
    pub(crate) fn catalogue_id(&self) -> Uuid {
        self.catalogue
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        scope: &crate::db::NativeRegisteredNoDomainScope<'_>,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
    ) -> Result<()> {
        self.open.require_live()?;
        if store.open_id != self.open.id()
            || !std::sync::Weak::ptr_eq(&self.open.open, &Arc::downgrade(&store.open_lifetime))
            || scope.range_id() != self.range
            || scope.native().birth_id() != self.birth
            || scope.native().birth_sha256() != self.birth_sha256
        {
            return Err(range_write_error(
                "Registered NoDomain changed actual Store/native birth",
            ));
        }
        store.require_registered_selected_catalogue_absence(db, scope, role)?;
        if store.catalog.recovery_fences.get(&self.range) != Some(&self.refusal)
            || store.catalog.ranges.get(&self.range) != Some(&self.raw)
        {
            return Err(range_write_error(
                "Registered NoDomain actual CLOSED refusal/raw pair changed",
            ));
        }
        if db.native_abort_catalogue_store_id()? != Some(self.catalogue) {
            return Err(range_write_error(
                "Registered NoDomain protected catalogue UUID changed",
            ));
        }
        self.open.require_live()
    }
}
impl RangeWriteStore {
    pub(crate) fn observe_registered_selected_catalogue_absence(
        &self,
        db: &crate::BicDb,
        scope: &crate::db::NativeRegisteredNoDomainScope<'_>,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
    ) -> Result<NativeRegisteredRangeCatalogueAbsence> {
        self.require_registered_selected_catalogue_absence(db, scope, role)?;
        let catalogue = db
            .native_abort_catalogue_store_id()?
            .ok_or_else(|| range_write_error("Registered NoDomain protected catalogue absent"))?;
        Ok(NativeRegisteredRangeCatalogueAbsence {
            open: RangeWriteStoreOpenGuard {
                open: Arc::downgrade(&self.open_lifetime),
                id: self.open_id,
            },
            range: scope.range_id(),
            catalogue,
            birth: scope.native().birth_id(),
            birth_sha256: scope.native().birth_sha256().into(),
            refusal: self.catalog.recovery_fences[&scope.range_id()].clone(),
            raw: self.catalog.ranges[&scope.range_id()].clone(),
        })
    }
    fn require_registered_selected_catalogue_absence(
        &self,
        db: &crate::BicDb,
        scope: &crate::db::NativeRegisteredNoDomainScope<'_>,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
    ) -> Result<()> {
        scope.require_database(db)?;
        self.require_native_catalog_durability(db.data_path())?;
        db.require_native_writer_catalogue_custody(self)?;
        self.require_actual_abort_catalogue(db)?;
        let native = scope.native();
        let range = scope.range_id();
        let sparse = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .ok_or_else(|| {
                range_write_error("Registered NoDomain mandatory sparse field absent")
            })?;
        let installing = self
            .catalog
            .installing_committed_certifications
            .as_ref()
            .ok_or_else(|| {
                range_write_error("Registered NoDomain mandatory Installing field absent")
            })?;
        let handovers = self
            .catalog
            .installing_recovery_handovers
            .as_ref()
            .ok_or_else(|| {
                range_write_error("Registered NoDomain mandatory handover field absent")
            })?;
        let joint =
            self.catalog.native_joint_handoffs.as_ref().ok_or_else(|| {
                range_write_error("Registered NoDomain mandatory joint field absent")
            })?;
        let current = self
            .catalog
            .current_joint_recoveries
            .as_ref()
            .ok_or_else(|| {
                range_write_error("Registered NoDomain mandatory current-joint field absent")
            })?;
        let abort =
            self.catalog.native_abort_protocol.as_ref().ok_or_else(|| {
                range_write_error("Registered NoDomain protected abort field absent")
            })?;
        let refusal = self.catalog.recovery_fences.get(&range).ok_or_else(|| {
            range_write_error("Registered NoDomain actual CLOSED refusal missing")
        })?;
        let raw = self.catalog.ranges.get(&range).ok_or_else(|| {
            range_write_error("Registered NoDomain actual CLOSED raw pair missing")
        })?;
        refusal.scope.require_role(role)?;
        self.require_persisted_recovery_scope(&refusal.scope)?;
        if !matches!(refusal.phase, RecoveryFencePhase::Active)
            || !matches!(
                refusal.scope.purpose,
                crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff { .. }
            )
            || !refusal.historical_certifications.is_empty()
            || !refusal.retired_nonces.is_empty()
            || refusal.local_node_id != *native.local_node_id()
            || refusal.local_member_incarnation != native.local_member_incarnation()
            || refusal.native_database_incarnation != native.database_incarnation()
            || raw.current_epoch != refusal.scope.range_epoch
            || raw.last_index != 0
            || raw.resolved_through != 0
            || raw.compacted_through != 0
            || !raw.entries.is_empty()
        {
            return Err(range_write_error("Registered NoDomain exact CLOSED refusal contains DATA/obligations or changed epoch"));
        }
        if self.catalog.cluster_id != *native.cluster_id()
            || self.catalog.node_id != *native.local_node_id()
            || self.catalog.native_writer_enforcement_version != Some(1)
            || self.native_writer_host.is_some()
            || self.catalog.backup_fences.contains_key(&range)
            || self.catalog.handoff_promises.contains_key(&range)
            || self.catalog.image_install_intents.contains_key(&range)
            || self.catalog.native_image_bases.contains_key(&range)
            || sparse.contains_key(&range)
            || installing.contains_key(&range)
            || handovers.contains_key(&range)
            || joint.contains_key(&range)
            || current.contains_key(&range)
            || abort.range(range).is_some()
            || self.native_activated_opens.contains_key(&range)
            || self.native_joint_activated_opens.contains_key(&range)
            || self.current_joint_recovery_writers.contains_key(&range)
            || db
                .native_abort_slot_inventory()?
                .iter()
                .any(|r| r.command.range_id == range)
        {
            return Err(range_write_error("Registered selected range has actual raw/catalogue/slot/domain state; never infer R=0"));
        }
        scope.require_database(db)
    }
}
