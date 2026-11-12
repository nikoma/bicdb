//! Actual protected no-effect intents and canonical no-data history publication.
//! A JSON projection/checksum is never a quorum or cleanup authority.
use super::*;
mod slot_journal;
use crate::db::NativeTransactionNoEffect;
use crate::distribution_range_consensus::{
    NativePrepareOnlyRejection, RangeWriteLogEntry, RangeWriteState,
};
pub(crate) use slot_journal::{NativeAbortPermanentPromise, NativeAbortSlotRecord};

fn no_effect_key(range: RangeId, index: u64) -> String {
    format!("abort-no-effect:{range}:{index:020}")
}
const MAX_NO_EFFECT_BYTES: usize = 3 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeNoEffectIntentRecord {
    version: u32,
    pub(crate) command: crate::RangeWriteCommand,
    pub(crate) domain: crate::distribution_range_consensus::native_abort::NativeAbortDomain,
    birth_id: Uuid,
    birth_sha256: String,
    origin_database_incarnation: String,
    predecessor_index: u64,
    predecessor_history_sha256: String,
    local_data_visibility: u64,
    actual_native_page_xid: u64,
    checksum_sha256: String,
}
impl NativeNoEffectIntentRecord {
    pub(crate) fn birth_id(&self) -> Uuid {
        self.birth_id
    }
    pub(crate) fn birth_sha256(&self) -> &str {
        &self.birth_sha256
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.checksum_sha256
    }
    pub(crate) fn predecessor_index(&self) -> u64 {
        self.predecessor_index
    }
    pub(crate) fn predecessor_history_sha256(&self) -> &str {
        &self.predecessor_history_sha256
    }
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("bicdb-native-no-effect-intent-v1", unsigned),
            MAX_NO_EFFECT_BYTES,
        )?)))
    }
    pub(crate) fn validate(&self) -> Result<()> {
        crate::distribution_range_consensus::validate_native_recovery_command_size(&self.command)?;
        self.command.validate()?;
        if self.version != 1
            || self.birth_id.is_nil()
            || self.birth_sha256.len() != 64
            || self.predecessor_index.checked_add(1) != Some(self.command.index)
            || self.predecessor_history_sha256.len() != 64
            || self.actual_native_page_xid == 0
            || self.origin_database_incarnation.is_empty()
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected Core no-effect intent"));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn validate_native_abort_start_history(
        &self,
        source: &SourceRangeImageLineage,
        entries: Option<&BTreeMap<u64, RangeWriteLogEntry>>,
    ) -> Result<()> {
        self.validate_source_range_image_lineage_contract(
            source,
            source.base.range.id,
            source.range_index(),
        )?;
        for (index, entry) in entries.into_iter().flat_map(|entries| entries.iter()) {
            let actual = self.read_native_history_entry(
                source.base.range.id,
                source.history_generation(),
                *index,
            )?;
            let matches = match (entry.state, &actual.outcome) {
                (
                    RangeWriteState::Applied,
                    history::NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => command_checksum_sha256 == &entry.command.checksum_sha256,
                (RangeWriteState::Aborted, history::NativeLogicalHistoryOutcomeV1::Aborted) => true,
                _ => false,
            };
            if !matches {
                return Err(invalid(
                    "abort writer-start catalogue lacks actual matching native history",
                ));
            }
        }
        self.validate_source_range_image_lineage_contract(
            source,
            source.base.range.id,
            source.range_index(),
        )
    }
    pub(crate) fn persist_native_transaction_no_effect(
        &self,
        rejected: &NativePrepareOnlyRejection,
        cleaned: &NativeTransactionNoEffect,
    ) -> Result<String> {
        cleaned.require_rejection(self, rejected)?;
        if !crate::format::native_abort_history_active(&self.path)? {
            return Err(invalid("no-effect producer lacks reader fence"));
        }
        let source = self.current_native_range_source_lineage(rejected.command().range_id)?;
        if source.birth.birth_id != rejected.birth_id()
            || source.birth.checksum_sha256 != rejected.birth_sha256()
            || source.range_index() != rejected.predecessor_index()
            || source.logical_history_sha256() != rejected.predecessor_history_sha256()
            || source.actual_local_visibility_sequence() != rejected.local_data_visibility()
        {
            return Err(invalid(
                "Core no-effect predecessor/birth/local data bound changed",
            ));
        }
        let _journal = self.try_native_image_journal()?;
        self.require_native_abort_prepare_only_slot(rejected.command())?;
        let key = no_effect_key(rejected.command().range_id, rejected.command().index);
        if self
            .read_native_range_image_record::<NativeNoEffectIntentRecord>(&key)?
            .is_some()
        {
            return Err(invalid(
                "no-effect intent already exists; use actual recovery observation",
            ));
        }
        let mut record = NativeNoEffectIntentRecord {
            version: 1,
            command: rejected.command().clone(),
            domain: rejected.domain().clone(),
            birth_id: rejected.birth_id(),
            birth_sha256: rejected.birth_sha256().into(),
            origin_database_incarnation: rejected.database_incarnation().into(),
            predecessor_index: rejected.predecessor_index(),
            predecessor_history_sha256: rejected.predecessor_history_sha256().into(),
            local_data_visibility: rejected.local_data_visibility(),
            actual_native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        self.in_paged_transaction(|paged, xid| {
            record.actual_native_page_xid = xid;
            record.checksum_sha256 = record.checksum()?;
            record.validate()?;
            paged
                .store()
                .put(
                    xid,
                    &native_key(&key),
                    &bounded_json(&record, MAX_NO_EFFECT_BYTES)?,
                )
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native no-effect intent has no actual page store"))?;
        cleaned.require_rejection(self, rejected)?;
        let actual: NativeNoEffectIntentRecord = self
            .read_native_range_image_record(&key)?
            .ok_or_else(|| invalid("actual durable no-effect intent missing after publication"))?;
        if actual != record {
            return Err(invalid("native no-effect intent readback changed"));
        }
        self.reconcile_native_abort_no_effect_slots_under_journal()?;
        Ok(actual.checksum_sha256)
    }
}

impl BicDb {
    pub(crate) fn observe_actual_native_no_effect(
        &self,
        native: &NativeRecoveryAdmission,
        role: &crate::distribution_range_recovery::RecoveryObservationRoleAdmission,
        request: &crate::distribution_range_recovery::NativeAbortRequest,
    ) -> Result<NativeNoEffectIntentRecord> {
        native.require_current(self)?;
        role.require_scope(&request.scope)?;
        role.require_native_identity(native)?;
        self.require_installed_native_writer_database_for_range(role.current_range())?;
        if native.abort_protocol_version() != 1
            || native.writer_enforcement_version() != 1
            || native.metadata_initialization_version() != 1
            || native.local_node_id() != &request.origin_node_id
            || native.local_node_id() != role.local_node_id()
            || native.local_member_incarnation() != role.local_member_incarnation()
        {
            return Err(invalid(
                "abort candidate requires actual enrolled origin birth/member",
            ));
        }
        let _journal = self.try_native_image_journal()?;
        let record: NativeNoEffectIntentRecord = self.read_native_range_image_record(&no_effect_key(request.scope.range_id, request.range_index))?
            .ok_or_else(|| invalid("actual Core no-effect candidate unavailable; never infer it from rows or raw Abort"))?;
        record.validate()?;
        record.domain.require_observer(role)?;
        if record.birth_id != native.birth_id()
            || record.birth_sha256 != native.birth_sha256()
            || record.command.leader_node_id != request.origin_node_id
            || record.command.range_id != request.scope.range_id
            || record.command.index != request.range_index
            || request
                .candidate_sha256
                .as_deref()
                .is_some_and(|h| h != record.checksum_sha256)
        {
            return Err(invalid(
                "actual protected Core candidate does not bind origin/command",
            ));
        }
        let slots = self.native_abort_slot_inventory()?;
        let slot = slots
            .iter()
            .find(|s| {
                s.command.range_id == request.scope.range_id
                    && s.command.index == request.range_index
            })
            .ok_or_else(|| invalid("Core candidate has no protected writer-start"))?;
        if slot.command != record.command
            || slot.domain != record.domain
            || slot.no_effect_sha256.as_deref() != Some(record.checksum_sha256.as_str())
            || slot.ever_commit
        {
            return Err(invalid(
                "Core candidate contradicts protected native slot history",
            ));
        }
        Ok(record)
    }
}
