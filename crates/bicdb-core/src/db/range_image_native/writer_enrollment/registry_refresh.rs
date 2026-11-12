//! Metadata-only extension on the ORIGINAL installed writer object. Historical
//! MQ audit is never a new role; each APP/Strong operation keeps fresh proofs.
use super::*;
use crate::distribution_consensus::HostMetadataReadAcquireResponse;
use crate::distribution_data::NativeWriterRegistryRefresh;
use std::sync::atomic::Ordering;

const MAX_EXTENSIONS: u32 = 64;
const MAX_EXTENSION_BYTES: usize = 256 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegistryExtensionRecord {
    version: u32,
    sequence: u32,
    original_policy_sha256: String,
    registration_generation: Uuid,
    coordinator_id: Uuid,
    database_incarnation: String,
    store_open_id: Uuid,
    predecessor_sha256: String,
    facts: HostWriterPolicyFacts,
    metadata_context_id: Uuid,
    metadata_term: u64,
    metadata_read_index: u64,
    metadata_audit: serde_json::Value,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl RegistryExtensionRecord {
    fn checksum(&self) -> Result<String> {
        let mut copy = self.clone();
        copy.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native-writer-registry-extension-v1", copy),
            MAX_EXTENSION_BYTES,
        )?)))
    }
    fn validate(&self, original: &NativeWriterPolicyRecord) -> Result<()> {
        if self.version != 1
            || self.sequence == 0
            || self.sequence > MAX_EXTENSIONS
            || self.original_policy_sha256 != original.checksum_sha256
            || self.registration_generation != original.generation
            || self.coordinator_id != original.coordinator_id
            || self.database_incarnation != original.database_incarnation
            || self.store_open_id != original.store_open_id
            || self.predecessor_sha256.len() != 64
            || self.native_page_xid == 0
            || self.metadata_context_id.is_nil()
            || self.metadata_term < original.metadata_term
            || self.metadata_read_index < original.metadata_read_index
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged/orphan native writer registry extension"));
        }
        self.facts.require_registry_successor(&original.facts)?;
        let audit: HostMetadataReadAcquireResponse =
            serde_json::from_value(self.metadata_audit.clone())?;
        audit.validate(&audit.request)?;
        if audit.request.requester_node_id != self.facts.local_node_id
            || audit.request.requester_incarnation != self.facts.local_incarnation
            || audit.evidence.context_id != self.metadata_context_id
            || audit.evidence.term != self.metadata_term
            || audit.evidence.read_index != self.metadata_read_index
            || audit.evidence.restore_activation != self.facts.restore_activation
        {
            return Err(invalid(
                "native registry historical MQ audit binding differs",
            ));
        }
        let voter_map = |voters: &[crate::distribution_consensus::MetadataReadVoterProof]| {
            voters
                .iter()
                .map(|voter| (voter.node_id.clone(), voter.clone()))
                .collect::<BTreeMap<_, _>>()
        };
        if voter_map(&audit.evidence.old_voters) != voter_map(&audit.evidence.new_voters) {
            return Err(invalid(
                "registry historical audit contains unfinished metadata membership",
            ));
        }
        self.facts
            .require_topology(&audit.evidence.committed_topology)?;
        if let Some(candidate) = &audit.evidence.joint_configuration_candidate {
            self.facts.require_topology(candidate)?;
            for range in candidate.ranges.values() {
                self.facts.require_range(range)?;
                if range
                    .replicas
                    .iter()
                    .any(|replica| replica.role != crate::RangeReplicaRole::Voter)
                {
                    return Err(invalid(
                        "registry historical audit contains DATA learner transition",
                    ));
                }
            }
        }
        for range in audit.evidence.committed_topology.ranges.values() {
            self.facts.require_range(range)?;
        }
        Ok(())
    }
}
/// Only successful same-open native publication/readback constructs this.
/// Clone retains already-verified facts; no Deserialize/DTO constructor.
#[derive(Clone, Debug)]
pub(super) struct VerifiedRegistryExtension(RegistryExtensionRecord);
fn current_key(generation: Uuid) -> String {
    format!("writer-enrollment/registry/{generation}/current")
}
fn history_key(generation: Uuid, sequence: u32) -> String {
    format!("writer-enrollment/registry/{generation}/history/{sequence}")
}
impl NativeWriterHostState {
    pub(super) fn registry_diagnostic(&self) -> serde_json::Value {
        let cached = self.registry_extension.read();
        serde_json::json!({"publication_pending":self.registry_publication_pending.load(Ordering::Acquire),
            "verified_extension":cached.as_ref().map(|value| serde_json::json!({
                "sequence":value.0.sequence,"native_audit_sha256":value.0.checksum_sha256,
                "metadata_context_id":value.0.metadata_context_id,"metadata_term":value.0.metadata_term,
                "metadata_read_index":value.0.metadata_read_index,
                "metadata_voters":value.0.facts.metadata_voters})),
            "scope":"retained same-open verified audit DATA; not role or Ready"})
    }
    pub(super) fn require_registry_topology(
        &self,
        topology: &crate::ClusterTopology,
    ) -> Result<()> {
        if self.registry_publication_pending.load(Ordering::Acquire) {
            return Err(invalid("native writer registry publication is unresolved"));
        }
        if let Some(verified) = self.registry_extension.read().as_ref() {
            verified.0.facts.require_topology(topology)
        } else {
            self.record.facts.require_topology(topology)
        }
    }
    pub(super) fn require_verified_registry_database(&self, database: &BicDb) -> Result<()> {
        if self.registry_publication_pending.load(Ordering::Acquire) {
            return Err(invalid("native writer registry publication is unresolved"));
        }
        let cached = self.registry_extension.read().clone();
        let stored: Option<RegistryExtensionRecord> =
            database.read_native_range_image_record(&current_key(self.record.generation))?;
        match (cached, stored) {
            (None, None) => Ok(()),
            (Some(cached), Some(stored)) => {
                stored.validate(&self.record)?;
                let archive: RegistryExtensionRecord = database
                    .read_native_range_image_record(&history_key(
                        self.record.generation,
                        stored.sequence,
                    ))?
                    .ok_or_else(|| invalid("native registry extension archive missing"))?;
                if cached.0 != stored || archive != stored {
                    return Err(invalid(
                        "native registry actual current/archive/cached audit differs",
                    ));
                }
                self.require_lifetime()?;
                Ok(())
            }
            _ => Err(invalid(
                "native registry audit requires independently fresh same-open reconciliation",
            )),
        }
    }
    // Refresh-only bounded loader. No cold/open policy installation or role.
    fn load_registry_chain(&self, database: &BicDb) -> Result<Option<RegistryExtensionRecord>> {
        let current: Option<RegistryExtensionRecord> =
            database.read_native_range_image_record(&current_key(self.record.generation))?;
        let Some(current) = current else {
            return Ok(None);
        };
        current.validate(&self.record)?;
        let mut previous_sha = self.record.checksum_sha256.clone();
        let mut previous_facts = self.record.facts.clone();
        let mut previous_term = self.record.metadata_term;
        let mut previous_index = self.record.metadata_read_index;
        for sequence in 1..=current.sequence {
            let entry: RegistryExtensionRecord = database
                .read_native_range_image_record(&history_key(self.record.generation, sequence))?
                .ok_or_else(|| invalid("native registry ancestry missing"))?;
            entry.validate(&self.record)?;
            if entry.sequence != sequence
                || entry.predecessor_sha256 != previous_sha
                || entry.metadata_term < previous_term
                || entry.metadata_read_index < previous_index
            {
                return Err(invalid("native registry audit ancestry changed"));
            }
            entry.facts.require_registry_successor(&previous_facts)?;
            previous_sha = entry.checksum_sha256.clone();
            previous_facts = entry.facts.clone();
            previous_term = entry.metadata_term;
            previous_index = entry.metadata_read_index;
            if sequence == current.sequence && entry != current {
                return Err(invalid("native registry current/archive differs"));
            }
        }
        Ok(Some(current))
    }
}
impl InstalledNativeWriterHostGuard {
    /// Original identity only, deliberately NO changed-topology bypass for APP.
    /// A caller cannot obtain a serving/read grant from this unit preflight.
    pub(crate) fn require_registry_refresh_origin<T: RangeWriteTransport + 'static>(
        &self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        actual_writer: &Arc<NativeWriterHostState>,
    ) -> Result<()> {
        if !Arc::ptr_eq(actual_writer, &self.state) {
            return Err(invalid("registry refresh replaced original writer object"));
        }
        let actual: Arc<dyn CommitAdmission> = coordinator.clone();
        let expected = self
            .state
            .coordinator
            .upgrade()
            .ok_or_else(|| invalid("registry refresh original coordinator ended"))?;
        if !Arc::ptr_eq(&actual, &expected)
            || coordinator.native_writer_coordinator_id() != self.state.record.coordinator_id
        {
            return Err(invalid(
                "registry refresh is not original installed coordinator",
            ));
        }
        let range = self
            .state
            .record
            .facts
            .ranges
            .values()
            .next()
            .ok_or_else(|| invalid("registry refresh has no original DATA policy"))?;
        self.state.require_original_database(database, range)
    }
    /// All network work is completed; actual service/slot is rechecked with
    /// nonwaiting local borrows. Gate->journal->native WAL; no journal reentry.
    pub(crate) fn refresh_metadata_registry<T: RangeWriteTransport + 'static>(
        &self,
        database: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        prepared: NativeWriterRegistryRefresh,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let _gate = database
            .serializable_commit_admission
            .try_write()
            .ok_or_else(|| invalid("registry refresh actual admission gate busy"))?;
        prepared.require_current(database, coordinator, self)?;
        let policy = prepared.policy();
        policy
            .facts()
            .require_registry_successor(&self.state.record.facts)?;
        let _journal = database.try_native_image_journal()?;
        let previous = self.state.load_registry_chain(database)?;
        if let Some(previous) = &previous {
            policy.facts().require_registry_successor(&previous.facts)?;
            if policy.metadata_term() < previous.metadata_term
                || policy.metadata_read_index() < previous.metadata_read_index
            {
                return Err(invalid(
                    "registry refresh fresh MQ anchor is behind durable audit",
                ));
            }
            // A durable-but-unpublished audit is reconciled exactly; changing
            // the registry again before that is an explicit unsupported cut.
            let cache_sha = self
                .state
                .registry_extension
                .read()
                .as_ref()
                .map(|verified| verified.0.checksum_sha256.clone());
            if self
                .state
                .registry_publication_pending
                .load(Ordering::Acquire)
                && cache_sha.as_ref() != Some(&previous.checksum_sha256)
                && policy.facts() != &previous.facts
            {
                return Err(invalid(
                    "registry refresh must reconcile exact durable pending domain",
                ));
            }
        }
        let reuse = previous
            .as_ref()
            .is_some_and(|record| &record.facts == policy.facts());
        let record = if reuse {
            previous.unwrap()
        } else {
            let sequence = previous.as_ref().map_or(1, |record| record.sequence + 1);
            if sequence > MAX_EXTENSIONS {
                return Err(invalid(
                    "native registry extension history exhausted; no eviction",
                ));
            }
            let mut record = RegistryExtensionRecord {
                version: 1,
                sequence,
                original_policy_sha256: self.state.record.checksum_sha256.clone(),
                registration_generation: self.state.record.generation,
                coordinator_id: self.state.record.coordinator_id,
                database_incarnation: self.state.record.database_incarnation.clone(),
                store_open_id: self.state.record.store_open_id,
                predecessor_sha256: previous.as_ref().map_or_else(
                    || self.state.record.checksum_sha256.clone(),
                    |r| r.checksum_sha256.clone(),
                ),
                facts: policy.facts().clone(),
                metadata_context_id: policy.metadata_context_id(),
                metadata_term: policy.metadata_term(),
                metadata_read_index: policy.metadata_read_index(),
                metadata_audit: prepared.audit().clone(),
                native_page_xid: 0,
                checksum_sha256: String::new(),
            };
            record.checksum_sha256 = record.checksum()?;
            bounded_json(&record, MAX_EXTENSION_BYTES)?; // refusal BEFORE mutation; actual XID is assigned in the native transaction
            parent.check()?;
            prepared.require_current(database, coordinator, self)?;
            self.state
                .registry_publication_pending
                .store(true, Ordering::Release);
            database
                .in_paged_transaction(|paged, xid| {
                    parent.check()?;
                    prepared.require_request_and_slot_lifetime()?;
                    self.state.require_lifetime()?;
                    record.native_page_xid = xid;
                    record.checksum_sha256 = record.checksum()?;
                    record.validate(&self.state.record)?;
                    let body = bounded_json(&record, MAX_EXTENSION_BYTES)?;
                    paged
                        .store()
                        .put(
                            xid,
                            &native_key(&history_key(
                                record.registration_generation,
                                record.sequence,
                            )),
                            &body,
                        )
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                    paged
                        .store()
                        .put(
                            xid,
                            &native_key(&current_key(record.registration_generation)),
                            &body,
                        )
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                    Ok(())
                })?
                .ok_or_else(|| invalid("registry extension has no actual native page store"))?;
            record
        };
        let actual = self
            .state
            .load_registry_chain(database)?
            .ok_or_else(|| invalid("registry audit disappeared after native publication"))?;
        if actual != record {
            return Err(invalid("registry audit readback changed"));
        }
        #[cfg(test)]
        if !reuse && TEST_REGISTRY_AFTER_NATIVE_READBACK.with(|cut| cut.replace(false)) {
            return Err(invalid(
                "injected registry error after actual durable native readback",
            ));
        }
        parent.check()?;
        prepared.with_current_for_publication(database, coordinator, self, || {
            // Keep actual service SLOT and actual hook locked through cache
            // publication; even legacy clear/reinstall uses this hook lock.
            let hook = database.commit_admission.read();
            let registration = self.state.require_lifetime()?;
            if !Arc::ptr_eq(hook.registration(), &registration)
                || hook
                    .native_writer()
                    .is_none_or(|state| !Arc::ptr_eq(state, &self.state))
            {
                return Err(invalid(
                    "registry writer replaced at final cache publication",
                ));
            }
            let mut cache = self.state.registry_extension.write();
            let old = cache.clone();
            *cache = Some(Arc::new(VerifiedRegistryExtension(record)));
            if let Err(error) = parent
                .check()
                .and_then(|_| policy.require_live())
                .and_then(|_| self.state.require_lifetime().map(|_| ()))
            {
                *cache = old;
                return Err(error);
            }
            self.state
                .registry_publication_pending
                .store(false, Ordering::Release);
            Ok(())
        })
    }
}

#[cfg(test)]
thread_local! { static TEST_REGISTRY_AFTER_NATIVE_READBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
impl BicDb {
    pub(crate) fn test_fail_next_native_writer_registry_after_readback(&self) {
        TEST_REGISTRY_AFTER_NATIVE_READBACK.with(|cut| cut.set(true));
    }
    /// Actual native audit projection only, never a role or installation.
    pub(crate) fn test_current_native_writer_registry_audit(
        &self,
    ) -> Result<Option<serde_json::Value>> {
        let writer = self
            .commit_admission
            .read()
            .native_writer()
            .cloned()
            .ok_or_else(|| invalid("test actual installed writer missing"))?;
        let record: Option<RegistryExtensionRecord> =
            self.read_native_range_image_record(&current_key(writer.record.generation))?;
        if let Some(record) = &record {
            record.validate(&writer.record)?;
        }
        record
            .map(serde_json::to_value)
            .transpose()
            .map_err(Into::into)
    }
}
