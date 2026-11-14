//! Selected existing-Joined constraints. FIRST never-writer absence remains
//! unchanged. An actual generic epoch fence supplies the refusal; this type
//! neither synthesizes one nor interprets unknown native state as R=0.
use super::*;
use crate::db::NativeJoinedReceiveStorageScope;

pub(crate) struct NativeJoinedRangeCatalogueAbsence {
    // Integrity/preimage DATA shared with FIRST, not its scoped constructor or
    // absence authority. No accessor converts this into the FIRST capability.
    selected: NativeRegisteredRangeCatalogueAbsence,
}
impl NativeJoinedRangeCatalogueAbsence {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.selected.store_open_id()
    }
    pub(crate) fn constraint_sha256(&self) -> Result<String> {
        self.selected.constraint_sha256()
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        scope: &NativeJoinedReceiveStorageScope<'_>,
    ) -> Result<()> {
        store.require_joined_selected_catalogue_absence(db, scope)?;
        let s = &self.selected;
        s.open.require_live()?;
        if store.open_id != s.open.id()
            || !std::sync::Weak::ptr_eq(&s.open.open, &Arc::downgrade(&store.open_lifetime))
            || scope.range_id() != s.range
            || scope.native().birth_id() != s.birth
            || scope.native().birth_sha256() != s.birth_sha256
            || db.native_abort_catalogue_store_id()? != Some(s.catalogue)
            || store.catalog.recovery_fences.get(&s.range) != Some(&s.refusal)
            || store.catalog.ranges.get(&s.range) != Some(&s.raw)
        {
            return Err(range_write_error(
                "Joined selected original refusal/raw/Store preimage changed",
            ));
        }
        scope.require_store(db, store)
    }
}
impl RangeWriteStore {
    pub(crate) fn observe_joined_selected_catalogue_absence(
        &self,
        db: &crate::BicDb,
        scope: &NativeJoinedReceiveStorageScope<'_>,
    ) -> Result<NativeJoinedRangeCatalogueAbsence> {
        self.require_joined_selected_catalogue_absence(db, scope)?;
        Ok(NativeJoinedRangeCatalogueAbsence {
            selected: NativeRegisteredRangeCatalogueAbsence {
                open: RangeWriteStoreOpenGuard {
                    open: Arc::downgrade(&self.open_lifetime),
                    id: self.open_id,
                },
                range: scope.range_id(),
                catalogue: db.native_abort_catalogue_store_id()?.ok_or_else(|| {
                    range_write_error("Joined protected catalogue identity absent")
                })?,
                birth: scope.native().birth_id(),
                birth_sha256: scope.native().birth_sha256().into(),
                refusal: self.catalog.recovery_fences[&scope.range_id()].clone(),
                raw: self.catalog.ranges[&scope.range_id()].clone(),
            },
        })
    }
    fn require_joined_selected_catalogue_absence(
        &self,
        db: &crate::BicDb,
        scope: &NativeJoinedReceiveStorageScope<'_>,
    ) -> Result<()> {
        scope.require_store(db, self)?;
        self.require_native_catalog_durability(db.data_path())?;
        self.require_actual_abort_catalogue(db)?;
        self.require_durable_recovery_catalog()?;
        let sparse = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .ok_or_else(|| range_write_error("Joined selected sparse map absent/unknown"))?;
        let installing = self
            .catalog
            .installing_committed_certifications
            .as_ref()
            .ok_or_else(|| range_write_error("Joined selected Installing map absent/unknown"))?;
        let handovers = self
            .catalog
            .installing_recovery_handovers
            .as_ref()
            .ok_or_else(|| range_write_error("Joined selected handover map absent/unknown"))?;
        let joint = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .ok_or_else(|| range_write_error("Joined selected joint map absent/unknown"))?;
        let current = self
            .catalog
            .current_joint_recoveries
            .as_ref()
            .ok_or_else(|| range_write_error("Joined selected current-joint map absent/unknown"))?;
        let abort = self
            .catalog
            .native_abort_protocol
            .as_ref()
            .ok_or_else(|| range_write_error("Joined selected abort map absent/unknown"))?;
        let range = scope.range_id();
        let refusal = self.catalog.recovery_fences.get(&range).ok_or_else(|| {
            range_write_error("Joined requires actual durable selected epoch refusal")
        })?;
        let raw =
            self.catalog.ranges.get(&range).ok_or_else(|| {
                range_write_error("Joined selected epoch refusal/raw pair absent")
            })?;
        refusal.scope.require_role(scope.role())?;
        if !matches!(refusal.phase, RecoveryFencePhase::Active)
            || !matches!(
                refusal.scope.purpose,
                crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff { .. }
            )
            || !refusal.historical_certifications.is_empty()
            || refusal.local_node_id != *scope.native().local_node_id()
            || refusal.local_member_incarnation != scope.native().local_member_incarnation()
            || refusal.native_database_incarnation != scope.native().database_incarnation()
            || raw.current_epoch != refusal.scope.range_epoch
            || raw.last_index != 0
            || raw.resolved_through != 0
            || raw.compacted_through != 0
            || !raw.entries.is_empty()
        {
            return Err(range_write_error(
                "Joined selected refusal contains DATA/history or changed owner/epoch",
            ));
        }
        if self.catalog.cluster_id != *scope.native().cluster_id()
            || self.catalog.node_id != *scope.native().local_node_id()
            || self.catalog.native_writer_enforcement_version != Some(1)
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
            return Err(range_write_error(
                "Joined selected native/catalogue/slot obligation retained; receive refused",
            ));
        }
        // We do not compare other ranges' progress or whole-database B, and
        // never clear them. Existing retired nonce constraints stay captured
        // verbatim in the actual selected preimage, not silently discarded.
        scope.require_unmaterialized_selected()?;
        scope.require_store(db, self)
    }
}
