//! Complete current LOCAL catalogue for a decided joint release continuation.
//! Does not certify a new head or grant ordinary reads/writes.
use super::*;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

impl RangeWriteStore {
    pub(crate) fn observe_owned_frozen_joint_generation(
        &self,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<crate::db::NativeJointOpenContinuity> {
        let range = fact.native().data().old_range().id;
        let guard = self
            .native_joint_activated_opens
            .get(&range)
            .ok_or_else(|| {
                range_write_error("frozen joint member lacks actual activation generation")
            })?;
        guard.observe_owned_frozen_generation(self.open_id, &fact.activation_sha256()?)
    }
    pub(crate) fn require_frozen_joint_release_complete(
        &self,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        let range = scope.native().data().old_range().id;
        scope
            .database()
            .range_mutation_gate()
            .validate_quiescent_frozen(range, frozen)?;
        let actual = self.progress(range);
        let native = scope.certified_contract_index();
        if actual.current_epoch != scope.native().data().activation_range().epoch
            || actual.last_index != native
            || actual.resolved_through != native
            || actual.compacted_through > native
            || scope.pending_obligation().is_some()
        {
            return Err(range_write_error(
                "frozen joint release needs actual local unresolved/higher-tail replay first",
            ));
        }
        self.require_actual_abort_controls_covered(scope.database(), range, native)?;
        for (index, decision) in self.collect_native_resolved_decisions(range, native)? {
            if !scope.covers_actual_resolution(index, &decision)? {
                return Err(range_write_error(
                    "frozen joint release history differs from actual retained decision",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn observe_native_joint_open_continuity(
        &self,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<crate::db::NativeJointOpenContinuity> {
        let range = fact.native().data().old_range().id;
        let guard = self
            .native_joint_activated_opens
            .get(&range)
            .ok_or_else(|| range_write_error("joint release lacks same-open actual guard"))?;
        guard.observe_actual_continuity(self.open_id, &fact.activation_sha256()?)
    }
    pub(crate) fn require_native_joint_open_continuity(
        &self,
        fact: &NativeJointCatalogueActivationFact,
        continuity: &crate::db::NativeJointOpenContinuity,
    ) -> Result<()> {
        let range = fact.native().data().old_range().id;
        let guard = self
            .native_joint_activated_opens
            .get(&range)
            .ok_or_else(|| range_write_error("joint actual open guard disappeared"))?;
        continuity.require_current(guard, self.open_id, &fact.activation_sha256()?)
    }
}

/// Historical continuation constraints, NOT a vote for current application R.
/// Every retained raw command, irreversible map and native pending identity is
/// captured from actual local storage, never from a filtered caller list.
pub(crate) struct NativeJointReleaseObservedObligations {
    store_open_id: Uuid,
    root: PathBuf,
    range: RangeId,
    commands: BTreeMap<u64, String>,
}
fn insert_observed(commands: &mut BTreeMap<u64, String>, index: u64, sha: &str) -> Result<()> {
    if index == 0 || sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(range_write_error(
            "malformed actual joint continuation obligation",
        ));
    }
    if let Some(old) = commands.get(&index) {
        if old != sha {
            return Err(range_write_error(
                "joint continuation observed conflicting command identities",
            ));
        }
    } else {
        if commands.len() >= 65_536 {
            return Err(range_write_error(
                "joint continuation obligation bound exceeded",
            ));
        }
        commands.insert(index, sha.into());
    }
    Ok(())
}
impl RangeWriteStore {
    fn actual_joint_release_commands(
        &self,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
    ) -> Result<BTreeMap<u64, String>> {
        self.require_native_catalog_durability(scope.database().data_path())?;
        self.require_native_joint_host_base(
            scope.database(),
            scope.native().data().old_range().id,
        )?;
        let range = scope.native().data().old_range().id;
        if self.progress(range).current_epoch != scope.native().data().activation_range().epoch {
            return Err(range_write_error(
                "joint continuation local catalogue epoch left protected activation domain",
            ));
        }
        let mut commands = BTreeMap::new();
        let obligations = self.image_install_catalog_history_obligations(range)?;
        for (index, sha) in obligations.irrevocable_commands() {
            insert_observed(&mut commands, *index, sha)?;
        }
        if let Some(log) = self.catalog.ranges.get(&range) {
            for (index, entry) in &log.entries {
                insert_observed(&mut commands, *index, &entry.command.checksum_sha256)?;
            }
        }
        self.require_actual_abort_catalogue(scope.database())?;
        if scope
            .database()
            .native_abort_catalogue_store_id()?
            .is_some()
        {
            for record in scope.database().native_abort_slot_inventory()? {
                if record.command.range_id == range {
                    insert_observed(
                        &mut commands,
                        record.command.index,
                        &record.command.checksum_sha256,
                    )?;
                }
            }
        }
        if let Some((index, sha)) = scope.pending_obligation() {
            insert_observed(&mut commands, *index, sha)?;
        }
        Ok(commands)
    }
    pub(crate) fn observe_joint_release_obligations(
        &self,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
    ) -> Result<NativeJointReleaseObservedObligations> {
        Ok(NativeJointReleaseObservedObligations {
            store_open_id: self.open_id,
            root: self.root.canonicalize()?,
            range: scope.native().data().old_range().id,
            commands: self.actual_joint_release_commands(scope)?,
        })
    }
}
impl NativeJointReleaseObservedObligations {
    pub(crate) fn require_preserved_current(
        &self,
        store: &RangeWriteStore,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
    ) -> Result<()> {
        if self.store_open_id != store.open_id
            || self.root != store.root.canonicalize()?
            || self.range != scope.native().data().old_range().id
        {
            return Err(range_write_error(
                "joint continuation actual store/root/open changed",
            ));
        }
        let current = store.actual_joint_release_commands(scope)?;
        for (index, sha) in &self.commands {
            match current.get(index) {
                Some(actual) if actual==sha=>{},
                Some(_)=>return Err(range_write_error("joint continuation prior command identity forked")),
                None if scope.covers_actual_command(*index,sha)?=>{},
                None=>return Err(range_write_error("joint continuation lost observed command; actual native application/retained proof required")),
            }
        }
        Ok(())
    }
}

impl RangeWriteStore {
    pub(crate) fn arm_native_joint_from_fresh_release(
        &self,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        original: &NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        let owner = crate::distribution_data::NativeJointReleaseOwnerView::Dropped(original);
        admission.require_local_with_store_scope(self, scope, owner)?;
        let fact = admission.activation();
        let guard = self
            .native_joint_activated_opens
            .get(&scope.native().data().old_range().id)
            .ok_or_else(|| {
                range_write_error("fresh joint release lost actual installed generation")
            })?;
        let activation_sha256 = fact.activation_sha256()?;
        // Full native/history/catalogue checks share actual historical scope.
        // Both local mutable range/generation slots are borrowed BEFORE the
        // metadata gate. No lock/I/O/callback occurs during final arm itself.
        let publication = guard.borrow_arm_publication(self.open_id, &activation_sha256)?;
        let version = admission.native_publication_guard()?;
        publication.publish();
        drop(version);
        drop(publication);
        Ok(())
    }
}
