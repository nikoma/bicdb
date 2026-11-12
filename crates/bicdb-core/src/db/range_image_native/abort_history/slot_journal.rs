//! Protected monotone slot facts. Replaying older JSON with the SAME StoreID
//! cannot erase a writer start, ever-Commit fact, no-effect intent or promise.
use super::*;
use crate::distribution_range_consensus::native_abort::NativeAbortDomain;
use crate::distribution_range_mutation::RangeMutationAuthority;
const INVENTORY_KEY: &str = "abort-slot:inventory";
const MAX_SLOTS: usize = 4096;
const MAX_SLOT_BYTES: usize = 3 * 1024 * 1024;
const MAX_TOTAL_SLOT_BYTES: u64 = 64 * 1024 * 1024;
fn slot_key(range: RangeId, index: u64) -> String {
    format!("abort-slot:{range}:{index:020}")
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAbortSlotRecord {
    pub(crate) store_id: Uuid,
    birth_id: Uuid,
    birth_sha256: String,
    pub(crate) command: crate::RangeWriteCommand,
    pub(crate) domain: NativeAbortDomain,
    pub(crate) predecessor_index: u64,
    pub(crate) predecessor_history_sha256: String,
    pub(crate) ever_commit: bool,
    pub(crate) no_effect_sha256: Option<String>,
    pub(crate) abort_promise_sha256: Option<String>,
    pub(crate) checksum_sha256: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    slots: BTreeMap<RangeId, BTreeMap<u64, (String, u64)>>,
}
impl NativeAbortSlotRecord {
    fn checksum(&self) -> Result<String> {
        let mut r = self.clone();
        r.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native-abort-slot-v1", r),
            MAX_SLOT_BYTES,
        )?)))
    }
    fn validate(&self, birth: &NativeDatabaseBirth, store_id: Uuid) -> Result<()> {
        crate::distribution_range_consensus::validate_native_recovery_command_size(&self.command)?;
        self.command.validate()?;
        if self.store_id != store_id
            || self.store_id.is_nil()
            || self.birth_id != birth.birth_id
            || self.birth_sha256 != birth.checksum_sha256
            || self.predecessor_index.checked_add(1) != Some(self.command.index)
            || self.predecessor_history_sha256.len() != 64
            || (self.ever_commit
                && (self.no_effect_sha256.is_some() || self.abort_promise_sha256.is_some()))
            || self
                .no_effect_sha256
                .as_ref()
                .is_some_and(|h| h.len() != 64)
            || self
                .abort_promise_sha256
                .as_ref()
                .is_some_and(|h| h.len() != 64)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged/foreign monotone native abort slot"));
        }
        Ok(())
    }
}
impl BicDb {
    fn abort_slot_identity(&self) -> Result<(NativeDatabaseBirth, Uuid)> {
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("abort slot lacks actual native birth"))?;
        if birth.abort_protocol_version != Some(1)
            || birth.format_version != 2
            || birth.metadata_initialization_version != Some(1)
        {
            return Err(invalid(
                "abort slot requires actual from-birth enforced eligibility",
            ));
        }
        let store = self
            .native_abort_catalogue_store_id()?
            .ok_or_else(|| invalid("native slot catalogue not registered"))?;
        Ok((birth, store))
    }
    pub(crate) fn native_abort_slot_inventory(&self) -> Result<Vec<NativeAbortSlotRecord>> {
        let (birth, store_id) = self.abort_slot_identity()?;
        let index: Inventory = self
            .read_native_range_image_record(INVENTORY_KEY)?
            .unwrap_or_default();
        let mut out = Vec::new();
        let mut total = 0u64;
        for (range, slots) in &index.slots {
            if slots.len() > MAX_SLOTS {
                return Err(invalid("native slot inventory bound exceeded"));
            }
            for (slot, (sha, bytes)) in slots {
                total = total
                    .checked_add(*bytes)
                    .ok_or_else(|| invalid("native slot byte counter exhausted"))?;
                let record: NativeAbortSlotRecord = self
                    .read_native_range_image_record(&slot_key(*range, *slot))?
                    .ok_or_else(|| invalid("protected slot inventory has missing record"))?;
                record.validate(&birth, store_id)?;
                if record.command.range_id != *range
                    || record.command.index != *slot
                    || record.checksum_sha256 != *sha
                {
                    return Err(invalid("protected slot inventory commitment differs"));
                }
                out.push(record);
            }
        }
        if total > MAX_TOTAL_SLOT_BYTES {
            return Err(invalid(
                "native abort permanent history byte bound exceeded",
            ));
        }
        Ok(out)
    }
    fn write_native_abort_slot(&self, next: NativeAbortSlotRecord) -> Result<()> {
        self.publish_native_abort_slot(next, |_, _| Ok(None))
            .map(|_| ())
    }
    fn publish_native_abort_slot(
        &self,
        mut next: NativeAbortSlotRecord,
        extra: impl FnOnce(
            bicdb_page::Xid,
            &mut NativeAbortSlotRecord,
        ) -> Result<Option<(String, Vec<u8>)>>,
    ) -> Result<NativeAbortSlotRecord> {
        let (birth, store_id) = self.abort_slot_identity()?;
        let key = slot_key(next.command.range_id, next.command.index);
        let previous: Option<NativeAbortSlotRecord> = self.read_native_range_image_record(&key)?;
        if let Some(old) = &previous {
            old.validate(&birth, store_id)?;
        }
        let mut inventory: Inventory = self
            .read_native_range_image_record(INVENTORY_KEY)?
            .unwrap_or_default();
        self.in_paged_transaction(|paged, xid| {
            // All DB/catalogue reads occurred before the page writer guard.
            let additional = extra(xid, &mut next)?;
            if let Some(old) = &previous {
                let mut shape = old.clone();
                shape.ever_commit = next.ever_commit;
                shape.no_effect_sha256 = next.no_effect_sha256.clone();
                shape.abort_promise_sha256 = next.abort_promise_sha256.clone();
                shape.checksum_sha256.clear();
                let mut next_shape = next.clone();
                next_shape.checksum_sha256.clear();
                if shape != next_shape
                    || (old.ever_commit && !next.ever_commit)
                    || old.no_effect_sha256.is_some()
                        && old.no_effect_sha256 != next.no_effect_sha256
                    || old.abort_promise_sha256.is_some()
                        && old.abort_promise_sha256 != next.abort_promise_sha256
                {
                    return Err(invalid(
                        "native abort slot transition would erase/replace permanent facts",
                    ));
                }
            }
            next.checksum_sha256 = next.checksum()?;
            next.validate(&birth, store_id)?;
            let encoded = bounded_json(&next, MAX_SLOT_BYTES)?;
            let slots = inventory.slots.entry(next.command.range_id).or_default();
            if !slots.contains_key(&next.command.index) && slots.len() >= MAX_SLOTS {
                return Err(invalid("permanent native slot count exhausted"));
            }
            slots.insert(
                next.command.index,
                (next.checksum_sha256.clone(), encoded.len() as u64),
            );
            let total =
                inventory
                    .slots
                    .values()
                    .flat_map(|s| s.values())
                    .try_fold(0u64, |n, (_, b)| {
                        n.checked_add(*b)
                            .ok_or_else(|| invalid("native slot byte counter exhausted"))
                    })?;
            if total > MAX_TOTAL_SLOT_BYTES {
                return Err(invalid(
                    "permanent native slot byte bound reached; no eviction allowed",
                ));
            }
            let inventory_bytes = bounded_json(&inventory, MAX_NATIVE_RECEIPT_BYTES)?;
            paged
                .store()
                .put(xid, &native_key(&key), &encoded)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            paged
                .store()
                .put(xid, &native_key(INVENTORY_KEY), &inventory_bytes)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            if let Some((key, bytes)) = additional {
                paged
                    .store()
                    .put(xid, &native_key(&key), &bytes)
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            }
            Ok(())
        })?
        .ok_or_else(|| invalid("native abort slot has no actual page WAL"))?;
        let actual: NativeAbortSlotRecord = self
            .read_native_range_image_record(&key)?
            .ok_or_else(|| invalid("native slot readback missing"))?;
        if actual != next {
            return Err(invalid("native slot readback changed"));
        }
        Ok(actual)
    }
    pub(crate) fn start_native_abort_slot(
        &self,
        command: &crate::RangeWriteCommand,
        domain: &NativeAbortDomain,
        source: &SourceRangeImageLineage,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        self.range_mutations.validate(command.range_id, authority)?;
        self.validate_source_range_image_lineage_contract(
            source,
            command.range_id,
            source.range_index(),
        )?;
        let (birth, store_id) = self.abort_slot_identity()?;
        let _journal = self.try_native_image_journal()?;
        self.validate_source_range_image_lineage_contract(
            source,
            command.range_id,
            source.range_index(),
        )?;
        if self
            .read_native_range_image_record::<NativeAbortSlotRecord>(&slot_key(
                command.range_id,
                command.index,
            ))?
            .is_some()
        {
            return Err(invalid(
                "native slot writer-start already exists; cannot reassign",
            ));
        }
        self.write_native_abort_slot(NativeAbortSlotRecord {
            store_id,
            birth_id: birth.birth_id,
            birth_sha256: birth.checksum_sha256,
            command: command.clone(),
            domain: domain.clone(),
            predecessor_index: source.range_index(),
            predecessor_history_sha256: source.logical_history_sha256().into(),
            ever_commit: false,
            no_effect_sha256: None,
            abort_promise_sha256: None,
            checksum_sha256: String::new(),
        })
    }
    pub(super) fn require_native_abort_prepare_only_slot(
        &self,
        command: &crate::RangeWriteCommand,
    ) -> Result<()> {
        let (birth, store) = self.abort_slot_identity()?;
        let slot: NativeAbortSlotRecord = self
            .read_native_range_image_record(&slot_key(command.range_id, command.index))?
            .ok_or_else(|| invalid("Core cleanup has no actual protected writer-start"))?;
        slot.validate(&birth, store)?;
        if slot.command != *command
            || slot.ever_commit
            || slot.no_effect_sha256.is_some()
            || slot.abort_promise_sha256.is_some()
        {
            return Err(invalid(
                "Core no-effect proof conflicts with actual permanent native slot history",
            ));
        }
        Ok(())
    }
    pub(crate) fn native_abort_mark_ever_commit(
        &self,
        command: &crate::RangeWriteCommand,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        self.range_mutations.validate(command.range_id, authority)?;
        let (birth, store_id) = self.abort_slot_identity()?;
        let _journal = self.try_native_image_journal()?;
        let mut slot: NativeAbortSlotRecord = self
            .read_native_range_image_record(&slot_key(command.range_id, command.index))?
            .ok_or_else(|| invalid("native Commit lacks protected writer-start"))?;
        slot.validate(&birth, store_id)?;
        if slot.command != *command
            || slot.no_effect_sha256.is_some()
            || slot.abort_promise_sha256.is_some()
        {
            return Err(invalid(
                "native Commit conflicts with permanent no-effect/abort slot",
            ));
        }
        if slot.ever_commit {
            return Ok(());
        }
        slot.ever_commit = true;
        self.write_native_abort_slot(slot)
    }
}

impl BicDb {
    pub(crate) fn reconcile_native_abort_no_effect_slots(&self) -> Result<()> {
        if self.native_abort_catalogue_store_id()?.is_none() {
            return Ok(());
        }
        let _journal = self.try_native_image_journal()?;
        self.reconcile_native_abort_no_effect_slots_under_journal()
    }
    pub(super) fn reconcile_native_abort_no_effect_slots_under_journal(&self) -> Result<()> {
        for mut slot in self.native_abort_slot_inventory()? {
            let candidate: Option<NativeNoEffectIntentRecord> = self
                .read_native_range_image_record(&no_effect_key(
                    slot.command.range_id,
                    slot.command.index,
                ))?;
            if let Some(intent) = candidate {
                intent.validate()?;
                if intent.command != slot.command
                    || intent.domain != slot.domain
                    || intent.birth_id != slot.birth_id
                    || intent.birth_sha256 != slot.birth_sha256
                    || slot.ever_commit
                {
                    return Err(invalid(
                        "protected Core no-effect conflicts with native slot history",
                    ));
                }
                if let Some(old) = &slot.no_effect_sha256 {
                    if old != &intent.checksum_sha256 {
                        return Err(invalid("protected no-effect checksum replaced"));
                    }
                } else {
                    slot.no_effect_sha256 = Some(intent.checksum_sha256);
                    self.write_native_abort_slot(slot)?;
                }
            }
        }
        Ok(())
    }
}

fn promise_key(range: RangeId, index: u64) -> String {
    format!("abort-slot-promise:{range}:{index:020}")
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAbortPermanentPromise {
    pub(crate) domain: NativeAbortDomain,
    pub(crate) command_sha256: String,
    pub(crate) candidate_sha256: String,
    pub(crate) range_id: RangeId,
    pub(crate) range_index: u64,
    pub(crate) first_scope: crate::distribution_range_consensus::RecoveryFenceScope,
    birth_id: Uuid,
    store_id: Uuid,
    local_node_id: ClusterNodeId,
    local_member_incarnation: u64,
    observed_metadata_context_id: Uuid,
    observed_metadata_term: u64,
    observed_metadata_read_index: u64,
    native_page_xid: u64,
    pub(crate) checksum_sha256: String,
}
impl NativeAbortPermanentPromise {
    fn checksum(&self) -> Result<String> {
        let mut p = self.clone();
        p.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native-permanent-abort-promise-v1", p),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self, birth: &NativeDatabaseBirth, slot: &NativeAbortSlotRecord) -> Result<()> {
        if self.birth_id != birth.birth_id
            || self.store_id != slot.store_id
            || self.domain != slot.domain
            || self.local_node_id != birth.spec.node_id
            || self.local_member_incarnation != birth.spec.member_incarnation
            || self.command_sha256 != slot.command.checksum_sha256
            || self.range_id != slot.command.range_id
            || self.range_index != slot.command.index
            || self.candidate_sha256.len() != 64
            || self.observed_metadata_context_id.is_nil()
            || self.native_page_xid == 0
            || self.observed_metadata_term < self.first_scope.metadata_term
            || self.observed_metadata_read_index < self.first_scope.metadata_read_index
            || self.checksum()? != self.checksum_sha256
            || slot.abort_promise_sha256.as_deref() != Some(self.checksum_sha256.as_str())
        {
            return Err(invalid("damaged/foreign protected permanent AbortPromise"));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn observe_actual_native_abort_promise(
        &self,
        range: RangeId,
        index: u64,
    ) -> Result<Option<NativeAbortPermanentPromise>> {
        let (birth, store_id) = self.abort_slot_identity()?;
        let slot: Option<NativeAbortSlotRecord> =
            self.read_native_range_image_record(&slot_key(range, index))?;
        let Some(slot) = slot else {
            return Ok(None);
        };
        slot.validate(&birth, store_id)?;
        let Some(_) = slot.abort_promise_sha256 else {
            return Ok(None);
        };
        let promise: NativeAbortPermanentPromise = self
            .read_native_range_image_record(&promise_key(range, index))?
            .ok_or_else(|| invalid("permanent native promise audit missing"))?;
        promise.validate(&birth, &slot)?;
        Ok(Some(promise))
    }
    pub(crate) fn persist_actual_native_abort_promise(
        &self,
        native: &NativeRecoveryAdmission,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        request: &crate::distribution_range_recovery::NativeAbortRequest,
        candidate: &crate::distribution_range_recovery::NativeAbortCandidateObservation,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<NativeAbortPermanentPromise> {
        native.require_current(self)?;
        request.scope.require_role(role)?;
        self.range_mutations
            .validate(request.scope.range_id, authority)?;
        candidate.require_for_role(role, request)?;
        let record = candidate.record();
        let (birth, store_id) = self.abort_slot_identity()?;
        if native.local_node_id() != role.local_node_id()
            || native.local_member_incarnation() != role.local_member_incarnation()
        {
            return Err(invalid(
                "native promise recipient actual birth/role differs",
            ));
        }
        // Source/history is actual local data. It never comes from candidate B.
        let source = self.current_native_range_source_lineage(request.scope.range_id)?;
        let _journal = self.try_native_image_journal()?;
        self.validate_source_range_image_lineage(
            &source,
            request.scope.range_id,
            source.range_index(),
        )?;
        if let Some(old) =
            self.observe_actual_native_abort_promise(request.scope.range_id, request.range_index)?
        {
            if old.command_sha256 != record.command.checksum_sha256
                || old.candidate_sha256 != candidate.sha256()
            {
                return Err(invalid(
                    "permanent slot promise already binds another candidate",
                ));
            }
            old.domain.require_role(role)?;
            if role.metadata_term() < old.observed_metadata_term
                || role.metadata_read_index() < old.observed_metadata_read_index
            {
                return Err(invalid(
                    "fresh promise witness regressed below protected metadata anchor",
                ));
            }
            return Ok(old);
        }
        if source.range_index() != record.predecessor_index()
            || source.logical_history_sha256() != record.predecessor_history_sha256()
        {
            return Err(invalid(
                "abort promise requires actual matching predecessor; repair the native gap first",
            ));
        }
        let mut slot: NativeAbortSlotRecord = self
            .read_native_range_image_record(&slot_key(request.scope.range_id, request.range_index))?
            .ok_or_else(|| {
                invalid("abort promise requires protected writer-start; no absent-slot inference")
            })?;
        slot.validate(&birth, store_id)?;
        if slot.command != record.command
            || slot.domain != record.domain
            || slot.ever_commit
            || slot
                .no_effect_sha256
                .as_deref()
                .is_some_and(|h| h != candidate.sha256())
        {
            return Err(invalid(
                "actual slot contains Commit/conflicting command; AbortPromise forbidden",
            ));
        }
        let mut promise = NativeAbortPermanentPromise {
            domain: slot.domain.clone(),
            command_sha256: slot.command.checksum_sha256.clone(),
            candidate_sha256: candidate.sha256().into(),
            range_id: request.scope.range_id,
            range_index: request.range_index,
            first_scope: request.scope.clone(),
            birth_id: birth.birth_id,
            store_id,
            local_node_id: native.local_node_id().clone(),
            local_member_incarnation: native.local_member_incarnation(),
            observed_metadata_context_id: role.context_id(),
            observed_metadata_term: role.metadata_term(),
            observed_metadata_read_index: role.metadata_read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        self.publish_native_abort_slot(slot.clone(), |xid, next| {
            promise.native_page_xid = xid;
            promise.checksum_sha256 = promise.checksum()?;
            next.abort_promise_sha256 = Some(promise.checksum_sha256.clone());
            Ok(Some((
                promise_key(request.scope.range_id, request.range_index),
                bounded_json(&promise, MAX_NATIVE_RECEIPT_BYTES)?,
            )))
        })?;
        slot.abort_promise_sha256 = Some(promise.checksum_sha256.clone());
        promise.validate(&birth, &slot)?;
        self.observe_actual_native_abort_promise(request.scope.range_id, request.range_index)?
            .ok_or_else(|| invalid("actual durable native promise readback missing"))
    }
}
