//! Protected local joint-domain bridge from an ACTUAL NEW prepared quorum.
//! Native publication precedes catalogue/open activation. This historical
//! record alone cannot release a freeze or admit an application operation.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointActivationAdmission, NativeJointDecisionData, NativeJointPreparedServingVoteData,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeJointServingBridgeRecord {
    version: u32,
    decision: NativeJointDecisionData,
    prepared: NativeJointServingPreparationRecord,
    new_prepared: BTreeMap<ClusterNodeId, NativeJointPreparedServingVoteData>,
    post_quorum_metadata_context_id: Uuid,
    post_quorum_metadata_term: u64,
    post_quorum_metadata_read_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
fn key(range: RangeId) -> String {
    format!("joint-handoff/serving-bridge/current/{range}")
}
fn archive_key(range: RangeId, sha: &str) -> String {
    format!("joint-handoff/serving-bridge/history/{range}/{sha}")
}
impl NativeJointServingBridgeRecord {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        self.decision.validate()?;
        self.prepared.validate()?;
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.version != 1
            || self.native_page_xid == 0
            || self.prepared.decision_sha256 != self.decision.checksum_sha256()
            || self.prepared.range_id != self.decision.old_range().id
            || self.prepared.range_index != self.decision.range_index()
            || self.prepared.logical_genesis_sha256 != self.decision.logical_genesis_sha256()
            || self.prepared.logical_history_sha256 != self.decision.logical_history_sha256()
            || self.new_prepared.len() < self.decision.new_members().len() / 2 + 1
            || self.new_prepared.len() > self.decision.new_members().len()
            || !self
                .new_prepared
                .contains_key(&self.decision.activation_range().leader)
            || !self.new_prepared.contains_key(&self.prepared.local_node_id)
            || self.post_quorum_metadata_context_id.is_nil()
            || self.post_quorum_metadata_term == 0
            || self.post_quorum_metadata_read_index == 0
            || self.new_prepared.iter().any(|(node, vote)| {
                !self.decision.new_members().contains_key(node)
                    || !digest(&vote.preparation_sha256)
                    || vote.native_database_incarnation.is_empty()
                    || vote.metadata_context_id.is_nil()
                    || vote.metadata_context_id == self.post_quorum_metadata_context_id
                    || vote.metadata_term == 0
                    || vote.metadata_read_index == 0
                    || vote.metadata_term > self.post_quorum_metadata_term
                    || vote.metadata_read_index > self.post_quorum_metadata_read_index
            })
            || self
                .new_prepared
                .get(&self.prepared.local_node_id)
                .is_none_or(|vote| {
                    vote.preparation_sha256 != self.prepared.checksum_sha256
                        || vote.actual_local_visibility_sequence
                            != self.prepared.actual_local_visibility_sequence
                        || vote.metadata_term < self.prepared.metadata_term
                        || vote.metadata_read_index < self.prepared.metadata_read_index
                })
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid(
                "damaged protected NEW-quorum native serving bridge",
            ));
        }
        Ok(())
    }
    fn same_decision_and_local_point(&self, other: &Self) -> bool {
        self.version == other.version
            && self.decision == other.decision
            && self.prepared == other.prepared
    }
    fn require_actual_point(
        &self,
        database: &BicDb,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        let actual = database.current_native_range_source_lineage(self.prepared.range_id)?;
        self.require_actual_anchor(database, native, &actual.birth, &actual.base)?;
        if actual.range_index() < self.prepared.range_index
            || actual.actual_local_visibility_sequence()
                < self.prepared.actual_local_visibility_sequence
            || (actual.range_index() == self.prepared.range_index
                && (actual.native_receipt_sha256() != self.prepared.native_receipt_sha256
                    || actual.actual_local_visibility_sequence()
                        != self.prepared.actual_local_visibility_sequence))
        {
            return Err(invalid(
                "joint bridge is ahead of actual validated native materialization",
            ));
        }
        Ok(())
    }
    // No SourceLineage recursion: prefix validation may consume this protected
    // native domain, while actual local materialization is validated afterward.
    fn require_actual_anchor(
        &self,
        database: &BicDb,
        native: &NativeRecoveryAdmission,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<()> {
        native.require_current(database)?;
        self.decision.require_actual_native(native)?;
        if native.birth_id() != self.prepared.birth_id
            || native.birth_sha256() != self.prepared.birth_sha256
            || native.local_node_id() != &self.prepared.local_node_id
            || native.local_member_incarnation() != self.prepared.local_incarnation
            || native.writer_enforcement_version() != 1
            || !format::native_joint_data_handoff_active(database.data_path())?
        {
            return Err(invalid(
                "joint bridge actual native birth/member/enforcement changed",
            ));
        }
        let decision = database.read_native_joint_decision(
            native,
            self.prepared.range_id,
            self.decision.checksum_sha256(),
        )?;
        if decision.data() != &self.decision
            || decision.checksum_sha256() != self.prepared.local_decision_sha256
        {
            return Err(invalid(
                "joint bridge lost its actual complete protected old/new decision",
            ));
        }
        let generations = database.native_range_image_generation_pair(self.prepared.range_id)?;
        if birth.birth_id != self.prepared.birth_id
            || birth.checksum_sha256 != self.prepared.birth_sha256
            || base.checksum_sha256 != self.prepared.physical_base_sha256
            || base.history_generation != self.prepared.history_generation
            || base.logical_genesis_sha256 != self.prepared.logical_genesis_sha256
            || base.schema_sha256 != self.decision.schema_sha256()
            || base.restore_activation.as_ref() != self.decision.restore_activation()
            || base.local_namespace_ids != self.prepared.local_namespace_ids
            || generations
                != (
                    self.prepared.global_image_generation,
                    self.prepared.range_image_generation,
                )
        {
            return Err(invalid(
                "joint bridge has no matching actual local physical/native anchor",
            ));
        }
        let entry = database.read_native_history_entry(
            self.prepared.range_id,
            self.prepared.history_generation,
            self.prepared.range_index,
        )?;
        if entry.commitment_sha256 != self.prepared.logical_history_sha256 {
            return Err(invalid(
                "joint bridge point is not in actual immutable local history",
            ));
        }
        let prepared: NativeJointServingPreparationRecord = database
            .read_native_range_image_record(&preparation_key(self.prepared.range_id))?
            .ok_or_else(|| invalid("joint bridge native preparation missing"))?;
        let archived: NativeJointServingPreparationRecord = database
            .read_native_range_image_record(&preparation_history_key(
                self.prepared.range_id,
                &self.prepared.checksum_sha256,
            ))?
            .ok_or_else(|| invalid("joint bridge immutable native preparation missing"))?;
        prepared.validate()?;
        archived.validate()?;
        if prepared != self.prepared || archived != self.prepared {
            return Err(invalid(
                "joint bridge actual preparation pointer/archive changed",
            ));
        }
        Ok(())
    }
}

/// Loaded only from an actual protected native record and immutable archive.
/// The physical image remains unchanged; catalogue/open admission is separate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::db::range_image_native) struct NativeJointServingDomain {
    record: NativeJointServingBridgeRecord,
}
impl NativeJointServingDomain {
    pub(in crate::db::range_image_native) fn range(&self) -> &RangeDescriptor {
        self.record.decision.activation_range()
    }
    pub(in crate::db::range_image_native) fn members(
        &self,
    ) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        self.record.decision.new_members()
    }
    pub(in crate::db::range_image_native) fn decision_sha256(&self) -> &str {
        self.record.decision.checksum_sha256()
    }
    pub(in crate::db::range_image_native) fn native_bridge_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(in crate::db::range_image_native) fn range_index(&self) -> u64 {
        self.record.prepared.range_index
    }
    pub(in crate::db::range_image_native) fn require_materialized(
        &self,
        base: &NativeRangeBaseBinding,
        prefix: Option<&NativeCertifiedRangePrefix>,
    ) -> Result<()> {
        let index = prefix.map_or(base.range_index, |prefix| prefix.range_index());
        let b = prefix.map_or(base.actual_local_visibility_sequence, |prefix| {
            prefix.actual_local_visibility_sequence()
        });
        if index < self.record.prepared.range_index
            || b < self.record.prepared.actual_local_visibility_sequence
        {
            return Err(invalid(
                "joint native domain has no actual materialized local cut",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    /// Recover ONLY the actual previously protected current/archive receipt.
    /// This reads no caller audit and performs no WAL write or deadline refresh.
    pub(crate) fn read_existing_native_joint_serving_bridge(
        &self,
        fresh: &CommittedNativeJointDecisionObservation,
        prepared: &NativeJointServingPreparation,
    ) -> Result<Option<NativeJointServingBridgeReceipt>> {
        fresh.require_current(self)?;
        prepared.native.require_current(self)?;
        let Some(record) =
            self.read_native_range_image_record::<NativeJointServingBridgeRecord>(&key(prepared
                .record
                .range_id))?
        else {
            return Ok(None);
        };
        record.validate()?;
        let (term, index, _) = fresh.metadata_anchor();
        if record.prepared != prepared.record
            || &record.decision != fresh.data()
            || fresh.native_receipt().checksum_sha256() != record.prepared.local_decision_sha256
            || fresh.native_receipt().native_database_incarnation()
                != prepared.native_database_incarnation()
            || term < record.post_quorum_metadata_term
            || index < record.post_quorum_metadata_read_index
        {
            return Err(invalid("joint publication recovery changed actual preparation/open/decision or regressed metadata"));
        }
        let native = self.native_recovery_admission(
            prepared.native.cluster_id(),
            prepared.native.bootstrap_id(),
            prepared.native.local_node_id(),
            prepared.native.local_member_incarnation(),
        )?;
        let receipt = NativeJointServingBridgeReceipt { native, record };
        receipt.require_current(self)?;
        fresh.require_current(self)?;
        Ok(Some(receipt))
    }
    pub(in crate::db::range_image_native) fn read_native_joint_serving_domain(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<Option<NativeJointServingDomain>> {
        let Some(record) =
            self.read_native_range_image_record::<NativeJointServingBridgeRecord>(&key(base
                .range
                .id))?
        else {
            return Ok(None);
        };
        record.validate()?;
        let native = self.native_recovery_admission(
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            &birth.spec.node_id,
            birth.spec.member_incarnation,
        )?;
        record.require_actual_anchor(self, &native, birth, base)?;
        let archived: NativeJointServingBridgeRecord = self
            .read_native_range_image_record(&archive_key(base.range.id, &record.checksum_sha256))?
            .ok_or_else(|| invalid("native joint domain immutable archive missing"))?;
        archived.validate()?;
        if archived != record {
            return Err(invalid("native joint domain pointer/archive differ"));
        }
        Ok(Some(NativeJointServingDomain { record }))
    }
}

/// Protected historical/native-domain result, NOT an actual-open catalogue
/// activation or application permission. It has no arbitrary-parts factory.
pub(crate) struct NativeJointServingBridgeReceipt {
    native: NativeRecoveryAdmission,
    record: NativeJointServingBridgeRecord,
}
/// One SAME-service validation borrow, never a request/serving permission.
/// The actual native journal stays held and the caller supplies its real freeze.
/// No scope survives a network call or supplies a new deadline.
pub(crate) struct NativeJointBridgeReadScope<'a> {
    database: &'a BicDb,
    native: &'a NativeJointServingBridgeReceipt,
    journal: crate::db::range_image_native::NativeImageJournalGuard<'a>,
    lineage: SourceRangeImageLineage,
}
impl NativeJointBridgeReadScope<'_> {
    pub(crate) fn database(&self) -> &BicDb {
        self.database
    }
    pub(crate) fn native(&self) -> &NativeJointServingBridgeReceipt {
        self.native
    }
    pub(crate) fn require_native(&self, native: &NativeJointServingBridgeReceipt) -> Result<()> {
        self.journal.require_database(self.database)?;
        if !std::ptr::eq(native, self.native) {
            return Err(invalid(
                "joint read scope substituted actual native receipt",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_committed_identity(
        &self,
        committed: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        if committed.data() != self.native.data()
            || committed.native_receipt().checksum_sha256()
                != self.native.record.prepared.local_decision_sha256
            || committed.native_receipt().native_database_incarnation()
                != self.native.native_database_incarnation()
        {
            return Err(invalid(
                "joint read scope changed actual local protected decision/open",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_prepared_frozen(
        &self,
        prepared: &NativeJointServingPreparation,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.journal.require_database(self.database)?;
        self.database
            .range_mutation_gate()
            .validate_quiescent_frozen(self.native.record.prepared.range_id, frozen)?;
        if prepared.record != self.native.record.prepared
            || prepared.native_database_incarnation() != self.native.native_database_incarnation()
            || self.lineage.range_index() != self.native.record.prepared.range_index
            || self.lineage.actual_local_visibility_sequence()
                != self.native.record.prepared.actual_local_visibility_sequence
            || self.lineage.logical_genesis_sha256()
                != self.native.record.prepared.logical_genesis_sha256
            || self.lineage.logical_history_sha256()
                != self.native.record.prepared.logical_history_sha256
        {
            return Err(invalid(
                "joint frozen read scope changed actual preparation/materialized point",
            ));
        }
        Ok(())
    }
}
impl NativeJointServingBridgeReceipt {
    pub(crate) fn current_read_scope<'a>(
        &'a self,
        database: &'a BicDb,
    ) -> Result<NativeJointBridgeReadScope<'a>> {
        let journal = database.try_native_image_journal()?;
        self.require_current(database)?;
        let lineage =
            database.current_native_range_source_lineage(self.record.prepared.range_id)?;
        Ok(NativeJointBridgeReadScope {
            database,
            native: self,
            journal,
            lineage,
        })
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.record.decision
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.native.local_node_id()
    }
    pub(crate) fn actual_local_visibility_sequence(&self) -> u64 {
        self.record.prepared.actual_local_visibility_sequence
    }
    pub(crate) fn preparation_catalogue_sha256(&self) -> &str {
        &self.record.prepared.catalogue_sha256
    }
    pub(crate) fn native_preparation_sha256(&self) -> &str {
        &self.record.prepared.checksum_sha256
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        self.native.database_incarnation()
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.record.post_quorum_metadata_term,
            self.record.post_quorum_metadata_read_index,
            self.record.post_quorum_metadata_context_id,
        )
    }
    pub(crate) fn require_current(&self, database: &BicDb) -> Result<()> {
        self.record.validate()?;
        self.record.require_actual_point(database, &self.native)?;
        let range = self.record.prepared.range_id;
        let current: NativeJointServingBridgeRecord = database
            .read_native_range_image_record(&key(range))?
            .ok_or_else(|| invalid("native joint bridge pointer missing"))?;
        let archive: NativeJointServingBridgeRecord = database
            .read_native_range_image_record(&archive_key(range, &self.record.checksum_sha256))?
            .ok_or_else(|| invalid("immutable native joint bridge missing"))?;
        current.validate()?;
        archive.validate()?;
        if current != self.record || archive != self.record {
            return Err(invalid("native joint bridge pointer/archive changed"));
        }
        Ok(())
    }
}
impl BicDb {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_native_joint_serving_bridge(
        &self,
        admission: &NativeJointActivationAdmission,
        original: &CommittedNativeJointDecisionObservation,
        prepared: &NativeJointServingPreparation,
        store: &RangeWriteStore,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<NativeJointServingBridgeReceipt> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "bridge-publish-native-total",
            prepared.local_node_id(),
            admission.test_phase_cancellation(),
        );
        parent.check()?;
        admission.require_external_preparations_current()?;
        admission.committed().require_current(self)?;
        prepared.require_current(self, store, original, catalogue, image, frozen)?;
        if prepared.checksum_sha256() != admission.local_preparation().checksum_sha256()
            || prepared.native_database_incarnation()
                != admission.local_preparation().native_database_incarnation()
            || admission.data() != original.data()
        {
            return Err(invalid(
                "joint publisher substituted a local preparation/open",
            ));
        }
        let (term, index, context) = admission.committed().metadata_anchor();
        let mut record = NativeJointServingBridgeRecord {
            version: 1,
            decision: admission.data().clone(),
            prepared: prepared.record.clone(),
            new_prepared: admission.votes(),
            post_quorum_metadata_context_id: context,
            post_quorum_metadata_term: term,
            post_quorum_metadata_read_index: index,
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        #[cfg(test)]
        let mut anchor_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "bridge-native-actual-point",
            prepared.local_node_id(),
            admission.test_phase_cancellation(),
        );
        record.require_actual_point(self, &prepared.native)?;
        #[cfg(test)]
        {
            anchor_trace.complete();
            drop(anchor_trace);
        }
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        self.range_mutations.validate(
            record.prepared.range_id,
            &RangeMutationAuthority::Controlled(&controlled),
        )?;
        let journal = self.try_native_image_journal()?;
        if !self.config.fsync || !format::native_joint_data_handoff_active(self.data_path())? {
            return Err(invalid(
                "native joint bridge requires actual fsync/reader fence",
            ));
        }
        let range = record.prepared.range_id;
        if let Some(old) =
            self.read_native_range_image_record::<NativeJointServingBridgeRecord>(&key(range))?
        {
            old.validate()?;
            if !old.same_decision_and_local_point(&record)
                || term < old.post_quorum_metadata_term
                || index < old.post_quorum_metadata_read_index
            {
                return Err(invalid("native joint bridge retry changed decision/physical point or regressed metadata"));
            }
            // Native success/catalogue uncertainty retains the ORIGINAL real
            // NEW-quorum audit rather than rewriting it with a fresh context.
            record = old;
        } else {
            #[cfg(test)]
            let mut wal_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
                "bridge-native-page-wal",
                prepared.local_node_id(),
                admission.test_phase_cancellation(),
            );
            self.in_paged_transaction(|paged, xid| {
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let archive = archive_key(range, &record.checksum_sha256);
                if self
                    .read_native_range_image_record::<NativeJointServingBridgeRecord>(&archive)?
                    .is_some()
                {
                    return Err(invalid(
                        "native joint bridge archive exists without pointer",
                    ));
                }
                let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
                for key in [archive, key(range)] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                Ok(())
            })?
            .ok_or_else(|| invalid("joint bridge has no actual native page store"))?;
            #[cfg(test)]
            wal_trace.complete();
        }
        let native = self.native_recovery_admission(
            prepared.native.cluster_id(),
            prepared.native.bootstrap_id(),
            prepared.native.local_node_id(),
            prepared.native.local_member_incarnation(),
        )?;
        let receipt = NativeJointServingBridgeReceipt { native, record };
        #[cfg(test)]
        let mut readback_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "bridge-native-post-write-readback",
            prepared.local_node_id(),
            admission.test_phase_cancellation(),
        );
        receipt.require_current(self)?;
        drop(journal);
        drop(controlled);
        parent.check()?;
        admission.require_live()?;
        prepared.require_current(self, store, original, catalogue, image, frozen)?;
        receipt.require_current(self)?;
        #[cfg(test)]
        {
            readback_trace.complete();
            trace.complete();
        }
        Ok(receipt)
    }
}
