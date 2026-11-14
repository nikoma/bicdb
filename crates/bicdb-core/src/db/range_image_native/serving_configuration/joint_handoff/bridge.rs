//! Actual NEW-voter physical preparation at the protected joint cut. A native
//! preparation record does not activate a membership or release its freeze.
use super::*;
use crate::distribution_range_consensus::NativeJointBridgeCatalogue;
use crate::distribution_range_handoff::CommittedNativeJointDecisionObservation;
use crate::distribution_range_image_install::NativeJointImageMaterialization;

mod activation;
mod publication_scope;
pub(in crate::db::range_image_native) use activation::NativeJointServingDomain;
pub(crate) use activation::{
    NativeCurrentJointBinding, NativeCurrentJointRecoveryDomain, NativeJointBridgeReadScope,
    NativeJointHistoricalCutScope, NativeJointServingBridgeReceipt,
};
pub(crate) use publication_scope::NativeJointPreparedPublicationScope;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeJointServingPreparationRecord {
    version: u32,
    decision_sha256: String,
    local_decision_sha256: String,
    birth_id: Uuid,
    birth_sha256: String,
    local_node_id: ClusterNodeId,
    local_incarnation: u64,
    physical_base_sha256: String,
    native_receipt_sha256: String,
    range_id: RangeId,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    actual_local_visibility_sequence: u64,
    history_generation: Uuid,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    local_namespace_ids: Vec<(String, Uuid)>,
    catalogue_sha256: String,
    metadata_context_id: Uuid,
    metadata_term: u64,
    metadata_read_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
fn preparation_key(range: RangeId) -> String {
    format!("joint-handoff/serving-prepared/current/{range}")
}
fn preparation_history_key(range: RangeId, sha: &str) -> String {
    format!("joint-handoff/serving-prepared/history/{range}/{sha}")
}
impl NativeJointServingPreparationRecord {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.version != 1
            || self.birth_id.is_nil()
            || self.local_incarnation == 0
            || self.history_generation.is_nil()
            || self.global_image_generation.is_nil()
            || self.range_image_generation.is_nil()
            || self.metadata_context_id.is_nil()
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || self.native_page_xid == 0
            || self
                .local_namespace_ids
                .windows(2)
                .any(|w| w[0].0 >= w[1].0)
            || self.local_namespace_ids.iter().any(|(_, id)| id.is_nil())
            || [
                &self.decision_sha256,
                &self.local_decision_sha256,
                &self.birth_sha256,
                &self.physical_base_sha256,
                &self.native_receipt_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
                &self.catalogue_sha256,
            ]
            .iter()
            .any(|s| !digest(s))
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected NEW joint serving preparation"));
        }
        Ok(())
    }
    fn same_fact(&self, other: &Self) -> bool {
        let mut old = self.clone();
        let mut new = other.clone();
        for fact in [&mut old, &mut new] {
            fact.metadata_context_id = Uuid::nil();
            fact.metadata_term = 0;
            fact.metadata_read_index = 0;
            fact.native_page_xid = 0;
            fact.checksum_sha256.clear();
        }
        old == new
    }
    fn require_binding(
        &self,
        database: &BicDb,
        native: &NativeRecoveryAdmission,
        committed: &CommittedNativeJointDecisionObservation,
        catalogue: &NativeJointBridgeCatalogue,
    ) -> Result<()> {
        native.require_current(database)?;
        committed.require_current(database)?;
        let actual =
            database.current_native_range_source_lineage(committed.data().old_range().id)?;
        self.require_binding_with_actual(database, native, committed, catalogue, &actual)
    }
    fn require_binding_with_actual(
        &self,
        database: &BicDb,
        native: &NativeRecoveryAdmission,
        committed: &CommittedNativeJointDecisionObservation,
        catalogue: &NativeJointBridgeCatalogue,
        actual: &SourceRangeImageLineage,
    ) -> Result<()> {
        native.require_current(database)?;
        committed.require_current(database)?;
        let data = committed.data();
        let (term, index, _) = committed.metadata_anchor();
        let (global, range) = database.native_range_image_generation_pair(data.old_range().id)?;
        if self.birth_id != native.birth_id()
            || self.birth_sha256 != native.birth_sha256()
            || self.local_node_id != *native.local_node_id()
            || self.local_incarnation != native.local_member_incarnation()
            || !data.new_members().contains_key(&self.local_node_id)
            || self.decision_sha256 != data.checksum_sha256()
            || self.local_decision_sha256 != committed.native_receipt().checksum_sha256()
            || self.range_id != data.old_range().id
            || self.range_index != data.range_index()
            || self.range_index != actual.range_index()
            || self.logical_genesis_sha256 != data.logical_genesis_sha256()
            || self.logical_genesis_sha256 != actual.logical_genesis_sha256()
            || self.logical_history_sha256 != data.logical_history_sha256()
            || self.logical_history_sha256 != actual.logical_history_sha256()
            || self.physical_base_sha256 != actual.base.checksum_sha256
            || self.native_receipt_sha256 != actual.native_receipt_sha256()
            || self.actual_local_visibility_sequence != actual.actual_local_visibility_sequence()
            || self.history_generation != actual.history_generation()
            || self.global_image_generation != global
            || self.range_image_generation != range
            || self.local_namespace_ids != actual.base.local_namespace_ids
            || self.catalogue_sha256 != catalogue.checksum_sha256()
            || actual.schema_sha256() != data.schema_sha256()
            || actual.restore_activation() != data.restore_activation()
            || term < self.metadata_term
            || index < self.metadata_read_index
        {
            return Err(invalid(
                "NEW joint preparation differs from actual complete local materialization",
            ));
        }
        Ok(())
    }
}

/// Actual native-WAL materialization fact only. No constructor from a record,
/// range counter, metadata stage, vote list or serialized decision exists.
pub(crate) struct NativeJointServingPreparation {
    native: NativeRecoveryAdmission,
    record: NativeJointServingPreparationRecord,
}
impl NativeJointServingPreparation {
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.native.local_node_id()
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        self.native.database_incarnation()
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        &self.record.decision_sha256
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.record.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.record.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.record.logical_history_sha256
    }
    pub(crate) fn actual_local_visibility_sequence(&self) -> u64 {
        self.record.actual_local_visibility_sequence
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.record.metadata_term,
            self.record.metadata_read_index,
            self.record.metadata_context_id,
        )
    }
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        committed: &CommittedNativeJointDecisionObservation,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
            "prepared-native-current",
            self.local_node_id(),
            committed.deadline(),
        );
        catalogue.require_current(store, database, committed, image, frozen)?;
        database
            .range_mutations
            .validate_quiescent_frozen(self.record.range_id, frozen)?;
        self.require_record_current(database, committed, catalogue)?;
        #[cfg(test)]
        trace.complete();
        Ok(())
    }
    fn require_record_current(
        &self,
        database: &BicDb,
        committed: &CommittedNativeJointDecisionObservation,
        catalogue: &NativeJointBridgeCatalogue,
    ) -> Result<()> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
            "prepared-native-record-binding-readback",
            self.local_node_id(),
            committed.deadline(),
        );
        self.record.validate()?;
        self.record
            .require_binding(database, &self.native, committed, catalogue)?;
        let current: NativeJointServingPreparationRecord = database
            .read_native_range_image_record(&preparation_key(self.record.range_id))?
            .ok_or_else(|| invalid("NEW joint preparation native pointer missing"))?;
        let archive: NativeJointServingPreparationRecord = database
            .read_native_range_image_record(&preparation_history_key(
                self.record.range_id,
                &self.record.checksum_sha256,
            ))?
            .ok_or_else(|| invalid("NEW joint preparation immutable archive missing"))?;
        current.validate()?;
        archive.validate()?;
        if current != self.record || archive != self.record {
            return Err(invalid("NEW joint preparation current/archive changed"));
        }
        #[cfg(test)]
        trace.complete();
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn prepare_native_joint_serving(
        &self,
        committed: &CommittedNativeJointDecisionObservation,
        store: &RangeWriteStore,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<NativeJointServingPreparation> {
        parent.check()?;
        #[cfg(feature = "tls-replication")]
        let _registered_identity = image
            .and_then(|i| i.registered_target())
            .map(|t| self.borrow_registered_joint_identity(t.clone()))
            .transpose()?;
        catalogue.require_current(store, self, committed, image, frozen)?;
        let data = committed.data();
        let id = data.old_range().id;
        self.range_mutations.validate_quiescent_frozen(id, frozen)?;
        let pin = self
            .range_mutations
            .try_source_capture_read_pin(id, frozen)?;
        let actual = self.current_native_range_source_lineage(id)?;
        let member = data
            .new_members()
            .get(committed.native_receipt().local_node_id())
            .ok_or_else(|| invalid("joint preparation recipient is not a NEW voter"))?;
        let native = self.native_recovery_admission(
            data.cluster_id(),
            data.bootstrap_id(),
            committed.native_receipt().local_node_id(),
            member.incarnation,
        )?;
        data.require_actual_native(&native)?;
        // Even a genuine globally decided R cannot invent local Applied R.
        // Every retained raw/sparse/certified outcome must match actual native
        // history. Higher or unresolved obligations were refused by catalogue.
        for (index, decision) in catalogue.decisions() {
            parent.check()?;
            committed.require_current(self)?;
            let step = self.export_native_logical_history_step_with_source_pin(
                &actual, frozen, &pin, *index, 1, parent,
            )?;
            let entry = step
                .entries
                .first()
                .ok_or_else(|| invalid("joint preparation history missing"))?;
            let matches = match (decision, &entry.outcome) {
                (
                    NativeReadCatalogueDecision::Applied(expected),
                    NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => expected == command_checksum_sha256,
                (NativeReadCatalogueDecision::Aborted, NativeLogicalHistoryOutcomeV1::Aborted) => {
                    true
                }
                _ => false,
            };
            if entry.index != *index || !matches {
                return Err(invalid(
                    "joint preparation conflicts with actual irreversible native history",
                ));
            }
        }
        let (global, range) = self.native_range_image_generation_pair(id)?;
        let (term, index, context) = committed.metadata_anchor();
        let mut record = NativeJointServingPreparationRecord {
            version: 1,
            decision_sha256: data.checksum_sha256().into(),
            local_decision_sha256: committed.native_receipt().checksum_sha256().into(),
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().into(),
            local_node_id: native.local_node_id().clone(),
            local_incarnation: native.local_member_incarnation(),
            physical_base_sha256: actual.base.checksum_sha256.clone(),
            native_receipt_sha256: actual.native_receipt_sha256().into(),
            range_id: id,
            range_index: actual.range_index(),
            logical_genesis_sha256: actual.logical_genesis_sha256().into(),
            logical_history_sha256: actual.logical_history_sha256().into(),
            actual_local_visibility_sequence: actual.actual_local_visibility_sequence(),
            history_generation: actual.history_generation(),
            global_image_generation: global,
            range_image_generation: range,
            local_namespace_ids: actual.base.local_namespace_ids.clone(),
            catalogue_sha256: catalogue.checksum_sha256().into(),
            metadata_context_id: context,
            metadata_term: term,
            metadata_read_index: index,
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        record.require_binding(self, &native, committed, catalogue)?;
        catalogue.require_current(store, self, committed, image, frozen)?;
        drop(pin);
        // Frozen is read-only. The owned counted control spans the actual
        // metadata page WAL callback and readback; no rows or floors change.
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        self.range_mutations
            .validate(id, &RangeMutationAuthority::Controlled(&controlled))?;
        let journal = self.try_native_image_journal()?;
        if !self.config.fsync || !format::native_joint_data_handoff_active(self.data_path())? {
            return Err(invalid(
                "joint preparation requires actual fsync/reader fence",
            ));
        }
        if let Some(old) = self
            .read_native_range_image_record::<NativeJointServingPreparationRecord>(
                &preparation_key(id),
            )?
        {
            old.validate()?;
            if !old.same_fact(&record)
                || term < old.metadata_term
                || index < old.metadata_read_index
            {
                return Err(invalid(
                    "joint preparation retry changed physical point or regressed metadata",
                ));
            }
            record = old;
        } else {
            self.in_paged_transaction(|paged, xid| {
                committed.require_native_publication_metadata(self, store)?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let archive = preparation_history_key(id, &record.checksum_sha256);
                if self
                    .read_native_range_image_record::<NativeJointServingPreparationRecord>(
                        &archive,
                    )?
                    .is_some()
                {
                    return Err(invalid(
                        "joint preparation archive exists without its pointer",
                    ));
                }
                let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
                for key in [archive, preparation_key(id)] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                Ok(())
            })?
            .ok_or_else(|| invalid("joint preparation requires actual native page store"))?;
        }
        let receipt = NativeJointServingPreparation { native, record };
        receipt.require_record_current(self, committed, catalogue)?;
        drop(journal);
        drop(controlled);
        catalogue.require_current(store, self, committed, image, frozen)?;
        parent.check()?;
        receipt.require_current(self, store, committed, catalogue, image, frozen)?;
        committed.require_native_publication_metadata(self, store)?;
        // The actual post-write pointer/archive/lineage and original hook/A
        // are reborrowed together, then retained through the memory-only gate.
        let physical =
            receipt.publication_read_scope(self, store, committed, catalogue, image, frozen)?;
        let version = committed.native_publication_guard()?;
        drop(version);
        drop(physical);
        Ok(receipt)
    }
}

#[cfg(test)]
impl BicDb {
    /// Read actual protected facts as DATA only; never a preparation grant.
    pub(crate) fn test_joint_preparation_record(
        &self,
        range: RangeId,
    ) -> Result<Option<serde_json::Value>> {
        let Some(record) = self
            .read_native_range_image_record::<NativeJointServingPreparationRecord>(
                &preparation_key(range),
            )?
        else {
            return Ok(None);
        };
        record.validate()?;
        let archive = self.read_native_range_image_record::<NativeJointServingPreparationRecord>(
            &preparation_history_key(range, &record.checksum_sha256),
        )?;
        Ok(Some(serde_json::json!({"record":record,"archive":archive})))
    }
}
