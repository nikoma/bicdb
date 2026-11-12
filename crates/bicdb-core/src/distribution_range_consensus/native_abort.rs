//! Monotone writer-start/ever-Commit facts. Format alone grants no abort vote.
use super::*;
use crate::db::{NativeRecoveryAdmission, RangeImageBirthMember, SourceRangeImageLineage};
use crate::distribution_consensus::MetadataRestoreActivation;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

pub(super) const NATIVE_ABORT_CATALOG_FORMAT: u32 = 15;
const MAX_ABORT_SLOTS: usize = 4096;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAbortDomain {
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range: RangeDescriptor,
    members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    schema_sha256: String,
    restore_activation: Option<MetadataRestoreActivation>,
}
fn stable_range(range: &RangeDescriptor) -> RangeDescriptor {
    let mut r = crate::distribution_native_read::canonical_range(range);
    // Same-voter recovery may change epoch/leader, never span, placement,
    // replica IDs, credentials, schema or restore ancestry.
    r.epoch = 1;
    if let Some(first) = r.replicas.first() {
        r.leader = first.node_id.clone();
    }
    r
}
impl NativeAbortDomain {
    fn from_source(source: &SourceRangeImageLineage, range: &RangeDescriptor) -> Result<Self> {
        if stable_range(source.native_write_range()?) != stable_range(range) {
            return Err(range_write_error(
                "abort writer-start physical/effective range differs",
            ));
        }
        Ok(Self {
            cluster_id: source.cluster_id().clone(),
            bootstrap_id: source.bootstrap_id(),
            range: stable_range(range),
            members: source.native_write_members()?,
            schema_sha256: source.schema_sha256().into(),
            restore_activation: source.restore_activation().cloned(),
        })
    }
    pub(crate) fn require_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        role.require_live_request()?;
        if !matches!(
            role.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
        ) || self.cluster_id != *role.cluster_id()
            || self.bootstrap_id != role.bootstrap_id()
            || self.range != stable_range(role.current_range())
            || self.restore_activation.as_ref() != role.restore_activation()
            || self.members.len() != role.member_credentials().len()
            || self.members.iter().any(|(id, m)| {
                role.member_credentials().get(id).is_none_or(|a| {
                    m.incarnation != a.incarnation
                        || m.tls_certificate_sha256 != a.active_certificate_sha256
                        || m.pending_tls_certificate_sha256 != a.pending_certificate_sha256
                })
            })
            || role.expected_schema_sha256() != Some(self.schema_sha256.as_str())
        {
            return Err(range_write_error(
                "abort requires exact same-voter schema/birth/restore domain",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeAbortSlot {
    command_id: String,
    command_sha256: String,
    transaction_id: u64,
    original_epoch: u64,
    original_leader: ClusterNodeId,
    ever_commit: bool,
    no_effect_sha256: Option<String>,
    promise: Option<NativeAbortPromiseRecord>,
    resolved_native_receipt_sha256: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeAbortPromiseRecord {
    candidate_sha256: String,
    command_sha256: String,
    promised_index: u64,
    first_recovery_nonce: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeAbortRange {
    domain: NativeAbortDomain,
    native_birth_id: Uuid,
    native_birth_sha256: String,
    start_native_index: u64,
    start_native_history_sha256: String,
    slots: BTreeMap<u64, NativeAbortSlot>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeAbortProtocol {
    store_id: Uuid,
    ranges: BTreeMap<RangeId, NativeAbortRange>,
}
impl NativeAbortProtocol {
    pub(super) fn validate(&self) -> Result<()> {
        if self.store_id.is_nil() {
            return Err(range_write_error("native abort catalogue StoreID is nil"));
        }
        for (range, record) in &self.ranges {
            if record.domain.range.id != *range
                || record.native_birth_id.is_nil()
                || record.native_birth_sha256.len() != 64
                || record.start_native_history_sha256.len() != 64
                || record.slots.len() > MAX_ABORT_SLOTS
            {
                return Err(range_write_error(
                    "damaged native abort writer-start record",
                ));
            }
            for (index, slot) in &record.slots {
                if *index <= record.start_native_index
                    || slot.command_sha256.len() != 64
                    || slot.command_id.is_empty()
                    || slot.original_epoch == 0
                    || slot.promise.as_ref().is_some_and(|p| {
                        slot.ever_commit
                            || p.promised_index != *index
                            || p.command_sha256 != slot.command_sha256
                            || p.candidate_sha256.len() != 64
                            || p.first_recovery_nonce.is_nil()
                    })
                {
                    return Err(range_write_error("damaged/contradictory native abort slot"));
                }
            }
        }
        Ok(())
    }
    pub(super) fn range(&self, range: RangeId) -> Option<&NativeAbortRange> {
        self.ranges.get(&range)
    }
}
/// Created only by the actual local Store start producer before any Prepare.
/// The Core receives it only through a typed native admission rejection.
#[derive(Debug)]
pub struct NativePrepareOnlyRejection {
    command: RangeWriteCommand,
    domain: NativeAbortDomain,
    native_birth_id: Uuid,
    native_birth_sha256: String,
    database_incarnation: String,
    store_open_id: Uuid,
    predecessor_index: u64,
    predecessor_history_sha256: String,
    local_data_visibility: u64,
    mutation: Option<RangeMutationPermit>,
}
impl NativePrepareOnlyRejection {
    pub(super) fn retain_mutation(mut self, mutation: RangeMutationPermit) -> Self {
        self.mutation = Some(mutation);
        self
    }
    pub(crate) fn require_mutation(&self, gate: &RangeMutationGate) -> Result<()> {
        let permit = self.mutation.as_ref().ok_or_else(|| {
            range_write_error("abort cleanup lost its actual originating mutation lease")
        })?;
        gate.validate(
            self.command.range_id,
            &RangeMutationAuthority::Mutation(permit),
        )
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        &self.command
    }
    pub(crate) fn domain(&self) -> &NativeAbortDomain {
        &self.domain
    }
    pub(crate) fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn birth_id(&self) -> Uuid {
        self.native_birth_id
    }
    pub(crate) fn birth_sha256(&self) -> &str {
        &self.native_birth_sha256
    }
    pub(crate) fn predecessor_index(&self) -> u64 {
        self.predecessor_index
    }
    pub(crate) fn predecessor_history_sha256(&self) -> &str {
        &self.predecessor_history_sha256
    }
    pub(crate) fn local_data_visibility(&self) -> u64 {
        self.local_data_visibility
    }
}
impl RangeWriteStore {
    pub(crate) fn abort_catalogue_owner(root: &Path) -> Result<bicdb_page::DirectoryLock> {
        let owned = root.join("native-range-catalogue-owner");
        fs::create_dir_all(&owned)?;
        bicdb_page::DirectoryLock::acquire(&owned).map_err(|e| {
            range_write_error(format!(
                "native range catalogue already owned/unavailable: {e}"
            ))
        })
    }
    pub(crate) fn require_abort_catalogue_identity(
        root: &Path,
        cluster: &ClusterId,
        node: &ClusterNodeId,
        store_id: Uuid,
    ) -> Result<()> {
        let bytes = fs::read(root.join(DEFAULT_RANGE_WRITE_LOG)).map_err(|e| {
            range_write_error(format!(
                "registered native range catalogue missing; never reset: {e}"
            ))
        })?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "native range catalogue byte bound exceeded",
            ));
        }
        let catalogue = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        if &catalogue.cluster_id != cluster
            || &catalogue.node_id != node
            || catalogue.native_abort_protocol.as_ref().map(|p| p.store_id) != Some(store_id)
        {
            return Err(range_write_error(
                "actual protected native range catalogue StoreID/domain differs",
            ));
        }
        Ok(())
    }
    pub(crate) fn initialize_abort_catalogue_identity(
        root: &Path,
        cluster: &ClusterId,
        node: &ClusterNodeId,
        store_id: Uuid,
        writer_enforced: bool,
    ) -> Result<()> {
        if store_id.is_nil() {
            return Err(range_write_error("nil native range catalogue StoreID"));
        }
        let _owner = Self::abort_catalogue_owner(root)?;
        let path = root.join(DEFAULT_RANGE_WRITE_LOG);
        let mut catalog = match fs::read(&path) {
            Ok(bytes) => {
                let old = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
                if &old.cluster_id != cluster || &old.node_id != node {
                    return Err(range_write_error(
                        "fresh native catalogue belongs to another identity",
                    ));
                }
                if let Some(protocol) = &old.native_abort_protocol {
                    if protocol.store_id != store_id {
                        return Err(range_write_error(
                            "pending native catalogue StoreID changed",
                        ));
                    }
                }
                if !old.ranges.is_empty()
                    || !old.recovery_fences.is_empty()
                    || !old.handoff_promises.is_empty()
                    || !old.image_install_intents.is_empty()
                    || !old.native_image_bases.is_empty()
                    || !old.backup_fences.is_empty()
                    || old
                        .sparse_committed_decisions
                        .as_ref()
                        .is_some_and(|r| !r.is_empty())
                    || old
                        .native_abort_protocol
                        .as_ref()
                        .is_some_and(|p| !p.ranges.is_empty())
                {
                    return Err(range_write_error(
                        "actual factory cannot initialize over a used range catalogue",
                    ));
                }
                old
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RangeWriteCatalog {
                format_version: NATIVE_ABORT_CATALOG_FORMAT,
                native_writer_enforcement_version: writer_enforced.then_some(1),
                cluster_id: cluster.clone(),
                node_id: node.clone(),
                ranges: BTreeMap::new(),
                backup_fences: BTreeMap::new(),
                handoff_promises: BTreeMap::new(),
                image_install_intents: BTreeMap::new(),
                recovery_fences: BTreeMap::new(),
                native_image_bases: BTreeMap::new(),
                sparse_committed_decisions: Some(BTreeMap::new()),
                native_abort_protocol: None,
            },
            Err(e) => return Err(e.into()),
        };
        catalog.format_version = catalog.format_version.max(NATIVE_ABORT_CATALOG_FORMAT);
        catalog.native_abort_protocol = Some(NativeAbortProtocol {
            store_id,
            ranges: BTreeMap::new(),
        });
        catalog
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        if writer_enforced {
            catalog.native_writer_enforcement_version = Some(1);
        }
        let envelope = RangeWriteEnvelope::new(catalog)?;
        envelope.clone().verify()?;
        crate::storage::write_atomic(&path, &serde_json::to_vec_pretty(&envelope)?, true)?;
        Self::require_abort_catalogue_identity(root, cluster, node, store_id)
    }
    pub(crate) fn require_actual_abort_catalogue(&self, database: &crate::BicDb) -> Result<()> {
        let actual = database.native_abort_catalogue_store_id()?;
        if actual
            != self
                .catalog
                .native_abort_protocol
                .as_ref()
                .map(|p| p.store_id)
        {
            return Err(range_write_error(
                "actual native range catalogue/open identity changed",
            ));
        }
        Ok(())
    }
    pub(crate) fn requires_native_abort_tracking(&self) -> Result<bool> {
        crate::format::native_abort_history_active(&self.root)
    }
    pub(crate) fn has_native_abort_attempt(&self, command: &RangeWriteCommand) -> Result<bool> {
        let slot = self
            .catalog
            .native_abort_protocol
            .as_ref()
            .and_then(|p| p.ranges.get(&command.range_id))
            .and_then(|r| r.slots.get(&command.index));
        if let Some(slot) = slot {
            if slot.command_sha256 != command.checksum_sha256
                || slot.command_id != command.command_id
            {
                return Err(range_write_error(
                    "native abort writer-start belongs to another command at this slot",
                ));
            }
        }
        Ok(slot.is_some())
    }
    pub(crate) fn start_native_abort_attempt(
        &mut self,
        database: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
    ) -> Result<NativePrepareOnlyRejection> {
        native.require_current(database)?;
        if native.abort_protocol_version() != 1
            || native.writer_enforcement_version() != 1
            || native.metadata_initialization_version() != 1
        {
            return Err(range_write_error(
                "old native birth lacks from-birth abort eligibility",
            ));
        }
        self.require_actual_abort_catalogue(database)?;
        self.require_native_catalog_durability(database.data_path())?;
        self.require_no_active_recovery(range.id)?;
        self.require_no_image_install(range.id)?;
        validate_native_recovery_command_size(command)?;
        command.validate()?;
        if self.catalog.node_id != *native.local_node_id()
            || self.catalog.cluster_id != *native.cluster_id()
            || command.range_id != range.id
            || command.range_epoch != range.epoch
            || command.leader_node_id != range.leader
        {
            return Err(range_write_error(
                "native abort start actual identity/command differs",
            ));
        }
        let source = database.current_native_range_source_lineage(range.id)?;
        let domain = NativeAbortDomain::from_source(&source, range)?;
        let progress = self.progress(range.id);
        if progress.last_index != source.range_index()
            || progress.resolved_through != source.range_index()
            || command.index
                != source
                    .range_index()
                    .checked_add(1)
                    .ok_or_else(|| range_write_error("native index exhausted"))?
        {
            return Err(range_write_error(
                "untracked/unfinished slots cannot acquire an abort writer-start fence",
            ));
        }
        database.validate_native_abort_start_history(
            &source,
            self.catalog.ranges.get(&range.id).map(|r| &r.entries),
        )?;
        if let Some(existing) = self
            .catalog
            .native_abort_protocol
            .as_ref()
            .and_then(|p| p.ranges.get(&range.id))
        {
            if existing.domain != domain
                || existing.native_birth_id != native.birth_id()
                || existing.native_birth_sha256 != native.birth_sha256()
                || existing.slots.len() >= MAX_ABORT_SLOTS
                || existing.slots.contains_key(&command.index)
            {
                return Err(range_write_error(
                    "native abort start domain changed or permanent slot bound reached",
                ));
            }
        }
        if self.catalog.image_install_intents.values().any(|i| {
            matches!(
                i.source_purpose,
                crate::distribution_range_image::RangeImageSourcePurposeV1::CurrentVoterRepair
            ) && i.protected_proposal.is_none()
        }) {
            return Err(range_write_error(
                "abort format upgrade cannot invalidate another legacy image intent",
            ));
        }
        crate::format::enable_native_abort_history(&self.root)?;
        self.catalog.format_version = self.catalog.format_version.max(NATIVE_ABORT_CATALOG_FORMAT);
        self.catalog
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        let record = self
            .catalog
            .native_abort_protocol
            .as_mut()
            .ok_or_else(|| range_write_error("native birth lacks registered abort catalogue"))?
            .ranges
            .entry(range.id)
            .or_insert_with(|| NativeAbortRange {
                domain: domain.clone(),
                native_birth_id: native.birth_id(),
                native_birth_sha256: native.birth_sha256().into(),
                start_native_index: source.range_index(),
                start_native_history_sha256: source.logical_history_sha256().into(),
                slots: BTreeMap::new(),
            });
        record.slots.insert(
            command.index,
            NativeAbortSlot {
                command_id: command.command_id.clone(),
                command_sha256: command.checksum_sha256.clone(),
                transaction_id: command.transaction_id,
                original_epoch: command.range_epoch,
                original_leader: command.leader_node_id.clone(),
                ever_commit: false,
                no_effect_sha256: None,
                promise: None,
                resolved_native_receipt_sha256: None,
            },
        );
        self.persist_recovery_catalog()?;
        Ok(NativePrepareOnlyRejection {
            command: command.clone(),
            domain,
            native_birth_id: native.birth_id(),
            native_birth_sha256: native.birth_sha256().into(),
            database_incarnation: native.database_incarnation().into(),
            store_open_id: self.open_id,
            predecessor_index: source.range_index(),
            predecessor_history_sha256: source.logical_history_sha256().into(),
            local_data_visibility: source.actual_local_visibility_sequence(),
            mutation: None,
        })
    }
    pub(super) fn require_no_native_abort_promise(&self, range: RangeId, index: u64) -> Result<()> {
        if self
            .catalog
            .native_abort_protocol
            .as_ref()
            .and_then(|p| p.ranges.get(&range))
            .and_then(|r| r.slots.get(&index))
            .is_some_and(|s| s.promise.is_some())
        {
            return Err(range_write_error(
                "permanent slot-wide native AbortPromise refuses Commit/Certify/Apply",
            ));
        }
        Ok(())
    }
    pub(super) fn preflight_native_commit_slot(&self, command: &RangeWriteCommand) -> Result<()> {
        if let Some(protocol) = &self.catalog.native_abort_protocol {
            let slot = protocol
                .ranges
                .get(&command.range_id)
                .and_then(|r| r.slots.get(&command.index))
                .ok_or_else(|| {
                    range_write_error("untracked slot lacks durable native writer-start evidence")
                })?;
            if slot.command_sha256 != command.checksum_sha256
                || slot.command_id != command.command_id
                || slot.promise.is_some()
                || slot.no_effect_sha256.is_some()
            {
                return Err(range_write_error(
                    "native Commit conflicts with permanent slot/no-effect control",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn mark_native_ever_commit(&mut self, command: &RangeWriteCommand) -> Result<()> {
        if let Some(protocol) = self.catalog.native_abort_protocol.as_mut() {
            let slot = protocol.ranges.get_mut(&command.range_id).and_then(|r| r.slots.get_mut(&command.index))
                .ok_or_else(|| range_write_error("old/untracked native slot cannot be retroactively committed by abort-aware writer"))?;
            if slot.command_sha256 != command.checksum_sha256
                || slot.command_id != command.command_id
                || slot.promise.is_some()
                || slot.no_effect_sha256.is_some()
            {
                return Err(range_write_error(
                    "native commit conflicts with immutable abort slot/no-effect intent",
                ));
            }
            slot.ever_commit = true;
        }
        Ok(())
    }
    pub(crate) fn finish_native_prepare_only_rejection(
        &mut self,
        database: &crate::BicDb,
        rejected: NativePrepareOnlyRejection,
        no_effect: &crate::db::NativeTransactionNoEffect,
    ) -> Result<()> {
        no_effect.require_rejection(database, &rejected)?;
        self.require_native_catalog_durability(database.data_path())?;
        if self.open_id != rejected.store_open_id {
            return Err(range_write_error("abort origin Store was reopened"));
        }
        let slot = self
            .catalog
            .native_abort_protocol
            .as_ref()
            .and_then(|p| p.ranges.get(&rejected.command.range_id))
            .and_then(|r| r.slots.get(&rejected.command.index))
            .ok_or_else(|| range_write_error("actual aborted attempt writer-start is absent"))?;
        if slot.ever_commit
            || slot.promise.is_some()
            || slot.command_sha256 != rejected.command.checksum_sha256
        {
            return Err(range_write_error(
                "prepare-only cleanup cannot overwrite Commit/promise/another command",
            ));
        }
        let receipt = database.persist_native_transaction_no_effect(&rejected, no_effect)?;
        let slot = self
            .catalog
            .native_abort_protocol
            .as_mut()
            .unwrap()
            .ranges
            .get_mut(&rejected.command.range_id)
            .unwrap()
            .slots
            .get_mut(&rejected.command.index)
            .unwrap();
        slot.no_effect_sha256 = Some(receipt);
        self.persist_recovery_catalog()
    }
}
