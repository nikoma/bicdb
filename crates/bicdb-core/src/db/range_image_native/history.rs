//! Portable logical history, separated from local native receipt identity.
//! Entries are immutable in one LOCAL generation; only actual native commit
//! producers may append them. DTOs/hash equality alone grant no authority.
use super::*;

pub(super) const HISTORY_FORMAT: u32 = 1;
const ZERO_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_HISTORY_ENTRY_BYTES: usize = 1024;
pub(crate) const MAX_HISTORY_STEP_ENTRIES: usize = 256;
pub(crate) const MAX_HISTORY_STEP_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum NativeLogicalHistoryOutcomeV1 {
    Genesis,
    Applied {
        command_checksum_sha256: String,
    },
    /// No-effect canonical resolution deliberately excludes the identity of
    /// an uncommitted prepared variant. Actual quorum-abort authority remains
    /// a separate, mandatory producer boundary.
    Aborted,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NativeLogicalHistoryEntryV1 {
    pub format_version: u32,
    pub range_id: RangeId,
    pub index: u64,
    pub outcome: NativeLogicalHistoryOutcomeV1,
    pub previous_sha256: String,
    pub commitment_sha256: String,
}

/// Bounded wire observation. A receiver must verify its exact source scope,
/// full contiguous stream and actual target history before trusting it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NativeLogicalHistoryStepV1 {
    pub format_version: u32,
    pub range_id: RangeId,
    pub source_range_index: u64,
    pub source_native_receipt_sha256: String,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub first_index: u64,
    pub next_index: Option<u64>,
    pub entries: Vec<NativeLogicalHistoryEntryV1>,
}

/// Actual local native history point. Private fields/no serde/numeric factory
/// prevent an arbitrary wire cursor from becoming a target compatibility pin.
#[derive(Debug)]
pub(crate) struct NativeLogicalHistoryPoint {
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range_id: RangeId,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    history_generation: Uuid,
    native_receipt_sha256: String,
}
impl NativeLogicalHistoryPoint {
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.range_id
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.logical_history_sha256
    }
    pub(crate) fn history_generation(&self) -> Uuid {
        self.history_generation
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        &self.native_receipt_sha256
    }
}
pub(super) fn actual_history_point(
    birth: &NativeDatabaseBirth,
    base: &NativeGenesisReceipt,
    prefix: Option<&NativeCertifiedRangePrefix>,
) -> NativeLogicalHistoryPoint {
    NativeLogicalHistoryPoint {
        cluster_id: birth.spec.cluster_id.clone(),
        bootstrap_id: birth.spec.bootstrap_id,
        range_id: base.range.id,
        range_index: prefix.map_or(base.range_index, |prefix| prefix.range_index()),
        logical_genesis_sha256: base.logical_genesis_sha256.clone(),
        logical_history_sha256: prefix
            .map_or(base.logical_genesis_sha256.as_str(), |prefix| {
                prefix.logical_history_sha256()
            })
            .to_string(),
        history_generation: base.history_generation,
        native_receipt_sha256: prefix
            .map_or(base.checksum_sha256.as_str(), |prefix| {
                prefix.checksum_sha256()
            })
            .to_string(),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn history_key(range: RangeId, generation: Uuid, index: u64) -> String {
    format!("history:{}:{generation}:{index:020}", range.get())
}
pub(super) fn logical_genesis_hash(
    birth: &NativeDatabaseBirth,
    receipt: &NativeGenesisReceipt,
) -> Result<String> {
    Ok(hex::encode(sha2::Sha256::digest(bounded_json(
        &(
            "bicdb.native.logical-history.genesis.v1",
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            receipt.range.id,
            receipt.range.start_token,
            receipt.range.end_token,
            &receipt.schema_sha256,
            &receipt.reader_features,
            &receipt.portable_catalog_sha256,
            &receipt.empty_image_root_sha256,
        ),
        MAX_NATIVE_RECEIPT_BYTES,
    )?)))
}
impl NativeLogicalHistoryEntryV1 {
    pub(super) fn genesis(range: RangeId, head: &str) -> Result<Self> {
        if !valid_sha256(head) {
            return Err(invalid("invalid actual genesis history hash"));
        }
        Ok(Self {
            format_version: HISTORY_FORMAT,
            range_id: range,
            index: 0,
            outcome: NativeLogicalHistoryOutcomeV1::Genesis,
            previous_sha256: ZERO_SHA256.into(),
            commitment_sha256: head.into(),
        })
    }
    pub(super) fn applied(
        range: RangeId,
        index: u64,
        previous: &str,
        command_sha256: &str,
    ) -> Result<Self> {
        let mut entry = Self {
            format_version: HISTORY_FORMAT,
            range_id: range,
            index,
            outcome: NativeLogicalHistoryOutcomeV1::Applied {
                command_checksum_sha256: command_sha256.into(),
            },
            previous_sha256: previous.into(),
            commitment_sha256: String::new(),
        };
        entry.commitment_sha256 = entry.calculate_link()?;
        entry.validate_shape()?;
        Ok(entry)
    }
    #[cfg(test)]
    fn canonical_aborted(range: RangeId, index: u64, previous: &str) -> Result<Self> {
        let mut entry = Self {
            format_version: HISTORY_FORMAT,
            range_id: range,
            index,
            outcome: NativeLogicalHistoryOutcomeV1::Aborted,
            previous_sha256: previous.into(),
            commitment_sha256: String::new(),
        };
        entry.commitment_sha256 = entry.calculate_link()?;
        entry.validate_shape()?;
        Ok(entry)
    }
    fn calculate_link(&self) -> Result<String> {
        if self.index == 0 || matches!(self.outcome, NativeLogicalHistoryOutcomeV1::Genesis) {
            return Err(invalid(
                "genesis commitment requires its actual native empty-image domain",
            ));
        }
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &(
                "bicdb.native.logical-history.link.v1",
                self.range_id,
                self.index,
                &self.previous_sha256,
                &self.outcome,
            ),
            MAX_HISTORY_ENTRY_BYTES,
        )?)))
    }
    pub(crate) fn validate_shape(&self) -> Result<()> {
        if self.format_version != HISTORY_FORMAT
            || !valid_sha256(&self.previous_sha256)
            || !valid_sha256(&self.commitment_sha256)
        {
            return Err(invalid("malformed native logical-history entry"));
        }
        match &self.outcome {
            NativeLogicalHistoryOutcomeV1::Genesis
                if self.index == 0 && self.previous_sha256 == ZERO_SHA256 => {}
            NativeLogicalHistoryOutcomeV1::Applied {
                command_checksum_sha256,
            } if self.index > 0 && valid_sha256(command_checksum_sha256) => {
                if self.calculate_link()? != self.commitment_sha256 {
                    return Err(invalid("native applied-history hash mismatch"));
                }
            }
            NativeLogicalHistoryOutcomeV1::Aborted if self.index > 0 => {
                if self.calculate_link()? != self.commitment_sha256 {
                    return Err(invalid("native abort-history hash mismatch"));
                }
            }
            _ => return Err(invalid("invalid native logical-history outcome/index")),
        }
        bounded_json(self, MAX_HISTORY_ENTRY_BYTES)?;
        Ok(())
    }
}

impl BicDb {
    pub(crate) fn export_native_logical_history_step(
        &self,
        lineage: &SourceRangeImageLineage,
        frozen: &FrozenRangeMutationPermit,
        first_index: u64,
        max_entries: usize,
        cancellation: &CancellationToken,
    ) -> Result<NativeLogicalHistoryStepV1> {
        if !(1..=MAX_HISTORY_STEP_ENTRIES).contains(&max_entries)
            || first_index > lineage.range_index()
        {
            return Err(invalid(
                "invalid bounded native-history export cursor/limit",
            ));
        }
        let range_id = lineage.base_range().id;
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)?;
        self.validate_source_range_image_lineage(lineage, range_id, lineage.range_index())?;
        let mut entries = Vec::with_capacity(max_entries);
        let mut index = first_index;
        let mut next_index = Some(first_index);
        let mut bytes = 0usize;
        while entries.len() < max_entries {
            cancellation.check()?;
            let entry =
                self.read_native_history_entry(range_id, lineage.history_generation(), index)?;
            if index == 0 {
                if entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis
                    || entry.commitment_sha256 != lineage.logical_genesis_sha256()
                {
                    return Err(invalid(
                        "native history export genesis differs from actual lineage",
                    ));
                }
            } else {
                let previous = self.read_native_history_entry(
                    range_id,
                    lineage.history_generation(),
                    index - 1,
                )?;
                if entry.previous_sha256 != previous.commitment_sha256 {
                    return Err(invalid("native history export has a discontinuity"));
                }
            }
            let size = bounded_json(&entry, MAX_HISTORY_ENTRY_BYTES)?.len();
            if bytes.saturating_add(size) > MAX_HISTORY_STEP_BYTES - 4096 {
                break;
            }
            bytes += size;
            entries.push(entry);
            if index == lineage.range_index() {
                next_index = None;
                break;
            }
            index = index
                .checked_add(1)
                .ok_or_else(|| invalid("native history cursor overflow"))?;
            next_index = Some(index);
        }
        if entries.is_empty() {
            return Err(invalid("native history export made no bounded progress"));
        }
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)?;
        self.validate_source_range_image_lineage(lineage, range_id, lineage.range_index())?;
        let step = NativeLogicalHistoryStepV1 {
            format_version: HISTORY_FORMAT,
            range_id,
            source_range_index: lineage.range_index(),
            source_native_receipt_sha256: lineage.native_receipt_sha256().to_string(),
            logical_genesis_sha256: lineage.logical_genesis_sha256().to_string(),
            logical_history_sha256: lineage.logical_history_sha256().to_string(),
            first_index,
            next_index,
            entries,
        };
        bounded_json(&step, MAX_HISTORY_STEP_BYTES)?;
        Ok(step)
    }
    pub(super) fn read_native_history_entry(
        &self,
        range: RangeId,
        generation: Uuid,
        index: u64,
    ) -> Result<NativeLogicalHistoryEntryV1> {
        if generation.is_nil() {
            return Err(invalid("native history generation is missing"));
        }
        let entry: NativeLogicalHistoryEntryV1 = self
            .read_native_range_image_record(&history_key(range, generation, index))?
            .ok_or_else(|| {
                invalid(format!(
                    "native logical history is incomplete at index {index}"
                ))
            })?;
        entry.validate_shape()?;
        if entry.range_id != range || entry.index != index {
            return Err(invalid("native history key/entry mismatch"));
        }
        Ok(entry)
    }
    pub(super) fn validate_native_genesis_history(
        &self,
        birth: &NativeDatabaseBirth,
        receipt: &NativeGenesisReceipt,
    ) -> Result<()> {
        if receipt.logical_genesis_sha256 != logical_genesis_hash(birth, receipt)? {
            return Err(invalid(
                "native genesis logical domain differs from actual empty-image receipt",
            ));
        }
        let genesis =
            self.read_native_history_entry(receipt.range.id, receipt.history_generation, 0)?;
        if genesis.outcome != NativeLogicalHistoryOutcomeV1::Genesis
            || genesis.commitment_sha256 != receipt.logical_genesis_sha256
        {
            return Err(invalid(
                "native genesis history entry differs from its receipt",
            ));
        }
        Ok(())
    }
}

/// Called ONLY inside the actual native page transaction, after its caller
/// has validated the owning native scope. A different existing entry refuses
/// the transaction; it cannot silently overwrite a certified history point.
pub(super) fn put_immutable_history_entry(
    paged: &crate::paged_collection::PagedRecords,
    xid: bicdb_page::Xid,
    generation: Uuid,
    entry: &NativeLogicalHistoryEntryV1,
) -> Result<()> {
    entry.validate_shape()?;
    if generation.is_nil() {
        return Err(invalid("cannot write nil native history generation"));
    }
    let key = native_key(&history_key(entry.range_id, generation, entry.index));
    if let Some(bytes) = paged
        .store()
        .get_as_of(&paged.latest_snapshot(), &key)
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?
    {
        if bytes.len() > MAX_HISTORY_ENTRY_BYTES {
            return Err(invalid("existing native history entry exceeds bound"));
        }
        let previous: NativeLogicalHistoryEntryV1 = serde_json::from_slice(&bytes)?;
        previous.validate_shape()?;
        if &previous != entry {
            return Err(invalid("conflicting immutable native logical history"));
        }
        return Ok(());
    }
    let encoded = bounded_json(entry, MAX_HISTORY_ENTRY_BYTES)?;
    paged
        .store()
        .put(xid, &key, &encoded)
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RangeReplica, ReplicaId};

    #[test]
    fn native_logical_genesis_is_common_across_actual_births_members_namespace_ids_and_opens() {
        let parent = tempfile::tempdir().unwrap();
        let mut first = super::super::tests::spec();
        let other = ClusterNodeId::new("other-history-member").unwrap();
        first.initial_members.insert(
            other.clone(),
            RangeImageBirthMember {
                incarnation: 1,
                tls_certificate_sha256: None,
                pending_tls_certificate_sha256: None,
            },
        );
        for range in &mut first.initial_ranges {
            range.replicas.push(RangeReplica {
                id: ReplicaId::new(1000 + range.id.get()).unwrap(),
                node_id: other.clone(),
                role: RangeReplicaRole::Voter,
            });
        }
        let mut second = first.clone();
        second.node_id = other;
        let (a, authority_a) =
            super::super::tests::schema_ready_database(&parent.path().join("a"), &first);
        let (b, authority_b) =
            super::super::tests::schema_ready_database(&parent.path().join("b"), &second);
        for range in &first.initial_ranges {
            super::super::tests::initialize(&a, &authority_a, range);
        }
        for range in &second.initial_ranges {
            super::super::tests::initialize(&b, &authority_b, range);
        }
        let range = first.initial_ranges[0].id;
        let a_lineage = a.read().current_native_range_source_lineage(range).unwrap();
        let b_lineage = b.read().current_native_range_source_lineage(range).unwrap();
        assert_ne!(authority_a.birth_id(), authority_b.birth_id());
        assert_ne!(
            a.read().local_namespace_identity("items").unwrap(),
            b.read().local_namespace_identity("items").unwrap()
        );
        assert_ne!(
            a_lineage.history_generation(),
            b_lineage.history_generation()
        );
        assert_ne!(
            a_lineage.native_receipt_sha256(),
            b_lineage.native_receipt_sha256()
        );
        assert_ne!(a_lineage.local_node_id(), b_lineage.local_node_id());
        assert_eq!(
            a_lineage.logical_genesis_sha256(),
            b_lineage.logical_genesis_sha256()
        );
        assert_eq!(
            a_lineage.logical_history_sha256(),
            b_lineage.logical_history_sha256()
        );
    }

    #[test]
    fn native_history_applied_forks_differ_and_canonical_abort_excludes_uncommitted_variants() {
        let range = RangeId::new(1).unwrap();
        let previous = "1".repeat(64);
        let a = NativeLogicalHistoryEntryV1::applied(range, 1, &previous, &"a".repeat(64)).unwrap();
        let b = NativeLogicalHistoryEntryV1::applied(range, 1, &previous, &"b".repeat(64)).unwrap();
        assert_ne!(a.commitment_sha256, b.commitment_sha256);
        // There is deliberately no prepared-command parameter to canonical
        // no-effect history. This hash test grants no abort decision authority.
        let abort_a = NativeLogicalHistoryEntryV1::canonical_aborted(range, 1, &previous).unwrap();
        let abort_b = NativeLogicalHistoryEntryV1::canonical_aborted(range, 1, &previous).unwrap();
        assert_eq!(abort_a, abort_b);
        assert_ne!(abort_a.commitment_sha256, a.commitment_sha256);
    }
}
