//! Read-only actual catalogue closure, including permanent sparse obligations.
use super::*;
use crate::distribution_native_read::{invalid, NativeRangeReadScope};
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) enum NativeReadCatalogueDecision {
    Applied(String),
    Aborted,
}
#[derive(Debug)]
pub(crate) struct NativeRangeReadCatalogue {
    store_open_id: Uuid,
    range_id: RangeId,
    native_r: u64,
    root: PathBuf,
    checksum: String,
    decisions: BTreeMap<u64, NativeReadCatalogueDecision>,
    recovered: bool,
}
impl NativeRangeReadCatalogue {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.native_r
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.checksum
    }
    pub(crate) fn decisions(&self) -> &BTreeMap<u64, NativeReadCatalogueDecision> {
        &self.decisions
    }
    pub(crate) fn is_recovered(&self) -> bool {
        self.recovered
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        scope: &NativeRangeReadScope,
        native_r: u64,
        data_path: &Path,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        if self.store_open_id != store.open_id
            || self.range_id != scope.range.id
            || self.native_r != native_r
            || self.root != data_path
        {
            return Err(invalid("actual native read store/open/path changed"));
        }
        let current = store.observe_native_read_catalogue(scope, native_r, data_path, native)?;
        if self.checksum != current.checksum
            || self.decisions != current.decisions
            || self.recovered != current.recovered
        {
            return Err(invalid("native read catalogue changed"));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    pub(crate) fn observe_native_read_catalogue(
        &self,
        scope: &NativeRangeReadScope,
        native_r: u64,
        data_path: &Path,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<NativeRangeReadCatalogue> {
        scope.validate()?;
        self.require_native_catalog_durability(data_path)?;
        let range = scope.range.id;
        self.require_sparse_native_binding(range, native)?;
        self.require_no_image_install(range)?;
        if self.root != data_path
            || self.catalog.cluster_id != scope.cluster_id
            || !scope.members.contains_key(&self.catalog.node_id)
            || self.catalog.handoff_promises.contains_key(&range)
            || self.catalog.backup_fences.contains_key(&range)
        {
            return Err(invalid(
                "read catalogue has another store/scope or retained refusal",
            ));
        }
        let recovered = if let Some(saved) = self.catalog.recovery_fences.get(&range) {
            if !matches!(
                saved.phase,
                recovery_state::RecoveryFencePhase::NativeActivated(_)
            ) || saved.scope.range_epoch != scope.range.epoch
                || saved.local_node_id != self.catalog.node_id
                || saved.local_member_incarnation
                    != scope.members[&self.catalog.node_id].incarnation
            {
                return Err(invalid(
                    "native read requires actually activated current recovery",
                ));
            }
            self.require_actual_native_activated_open(range)?;
            true
        } else {
            false
        };
        let progress = self.progress(range);
        if progress.last_index != native_r
            || progress.resolved_through != native_r
            || progress.compacted_through > native_r
            || progress.current_epoch > scope.range.epoch
        {
            return Err(invalid("native read has unresolved/ahead raw history"));
        }
        let decisions = self.collect_native_resolved_decisions(range, native_r)?;
        let sparse_record_commitments = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .and_then(|ranges| ranges.get(&range))
            .into_iter()
            .flat_map(|entries| entries.iter())
            .map(|(index, record)| (*index, record.record_checksum_sha256()))
            .collect::<BTreeMap<_, _>>();
        let commitment = (
            "bicdb-native-read-catalogue-v1",
            &self.catalog.cluster_id,
            &self.catalog.node_id,
            &progress,
            self.catalog.recovery_fences.get(&range),
            self.catalog.native_image_bases.get(&range),
            &decisions,
            &sparse_record_commitments,
        );
        validate_native_json_size(&commitment, 4 * 1024 * 1024)?;
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
        serde_json::to_writer(&mut hash, &commitment)?;
        Ok(NativeRangeReadCatalogue {
            store_open_id: self.open_id,
            range_id: range,
            native_r,
            root: self.root.clone(),
            checksum: hex::encode(hash.0.finalize()),
            decisions,
            recovered,
        })
    }
    /// Data observation only, shared with permanent joint closure. It does not
    /// bypass serving/lifecycle checks or manufacture a native floor.
    pub(super) fn collect_native_resolved_decisions(
        &self,
        range: RangeId,
        native_r: u64,
    ) -> Result<BTreeMap<u64, NativeReadCatalogueDecision>> {
        let mut decisions = BTreeMap::new();
        let mut insert = |index, decision| -> Result<()> {
            if index == 0 || index > native_r {
                return Err(invalid("native read omits irreversible logical history"));
            }
            if let Some(old) = decisions.get(&index) {
                if old != &decision {
                    return Err(invalid("conflicting actual read catalogue outcomes"));
                }
            } else {
                if decisions.len()
                    >= crate::distribution_range_recovery::MAX_FENCED_IRREVERSIBLE_COMMANDS
                {
                    return Err(invalid("native read catalogue obligation bound reached"));
                }
                decisions.insert(index, decision);
            }
            Ok(())
        };
        if let Some(log) = self.catalog.ranges.get(&range) {
            if log.entries.len()
                > crate::distribution_range_recovery::MAX_FENCED_IRREVERSIBLE_COMMANDS
            {
                return Err(invalid("native read retained-entry bound reached"));
            }
            for (index, entry) in &log.entries {
                let decision = match entry.state {
                    RangeWriteState::Applied => {
                        NativeReadCatalogueDecision::Applied(entry.command.checksum_sha256.clone())
                    }
                    RangeWriteState::Aborted => NativeReadCatalogueDecision::Aborted,
                    _ => {
                        return Err(invalid(
                            "native read has unresolved Prepared/Committed/CQ entry",
                        ))
                    }
                };
                insert(*index, decision)?;
            }
        }
        if let Some(saved) = self.catalog.recovery_fences.get(&range) {
            for (index, marker) in &saved.historical_certifications {
                insert(
                    *index,
                    NativeReadCatalogueDecision::Applied(marker.command_sha256.clone()),
                )?;
            }
        }
        for (index, checksum) in self.sparse_irreversible_commands(range)? {
            insert(index, NativeReadCatalogueDecision::Applied(checksum))?;
        }
        Ok(decisions)
    }
}
