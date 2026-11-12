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
    fn write_native_abort_slot(&self, mut next: NativeAbortSlotRecord) -> Result<()> {
        let (birth, store_id) = self.abort_slot_identity()?;
        let key = slot_key(next.command.range_id, next.command.index);
        let previous: Option<NativeAbortSlotRecord> = self.read_native_range_image_record(&key)?;
        if let Some(old) = previous {
            old.validate(&birth, store_id)?;
            let mut shape = old.clone();
            shape.ever_commit = next.ever_commit;
            shape.no_effect_sha256 = next.no_effect_sha256.clone();
            shape.abort_promise_sha256 = next.abort_promise_sha256.clone();
            shape.checksum_sha256.clear();
            let mut next_shape = next.clone();
            next_shape.checksum_sha256.clear();
            if shape != next_shape
                || (old.ever_commit && !next.ever_commit)
                || old.no_effect_sha256.is_some() && old.no_effect_sha256 != next.no_effect_sha256
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
        let mut inventory: Inventory = self
            .read_native_range_image_record(INVENTORY_KEY)?
            .unwrap_or_default();
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
        self.in_paged_transaction(|paged, xid| {
            paged
                .store()
                .put(xid, &native_key(&key), &encoded)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            paged
                .store()
                .put(xid, &native_key(INVENTORY_KEY), &inventory_bytes)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native abort slot has no actual page WAL"))?;
        let actual: NativeAbortSlotRecord = self
            .read_native_range_image_record(&key)?
            .ok_or_else(|| invalid("native slot readback missing"))?;
        if actual != next {
            return Err(invalid("native slot readback changed"));
        }
        Ok(())
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
                    || slot.abort_promise_sha256.is_some()
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
