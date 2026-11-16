//! Metadata-only extension on the ORIGINAL installed writer object. Historical
//! MQ audit is never a new role; each APP/Strong operation keeps fresh proofs.
use super::*;
use crate::distribution_consensus::HostMetadataReadAcquireResponse;
use crate::distribution_data::NativeWriterRegistryRefresh;
use std::sync::atomic::Ordering;

#[cfg(feature = "tls-replication")]
mod allocation;
#[cfg(all(test, feature = "tls-replication"))]
pub(super) use allocation::AllocationCut;
#[cfg(feature = "tls-replication")]
pub(super) use allocation::RetainedAllocationAttempt;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum ExtensionPurpose {
    AllocationEpoch,
}
const MAX_EXTENSIONS: u32 = 64;
const MAX_EXTENSION_BYTES: usize = 256 * 1024;

// RAM custody on the SAME actual writer State, never restored from record bytes.
// Entered/Unknown is conservative even when the original error preceded commit.
#[derive(Debug, Clone, Copy)]
enum RegistryOutcome {
    NotEntered,
    Entered,
    Known(u64),
    Unknown,
}
#[derive(Debug)]
pub(super) struct RetainedRegistryAttempt {
    record: RegistryExtensionRecord,
    outcome: RegistryOutcome,
    // Static classification only; preserve/return the original error object.
    first_error_kind: Option<&'static str>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegistryExtensionRecord {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    purpose: Option<ExtensionPurpose>,
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
        if !matches!(self.version, 1 | 2)
            || (self.version == 1 && self.purpose.is_some())
            || (self.version == 2 && self.purpose != Some(ExtensionPurpose::AllocationEpoch))
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
        if self.version == 1 {
            self.facts.require_registry_successor(&original.facts)?;
        } else {
            self.facts
                .require_allocation_domain_successor(&original.facts)?;
        }
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
            .require_refresh_topology(&audit.evidence.committed_topology)?;
        if self.purpose.is_some() && audit.evidence.joint_configuration_candidate.is_some() {
            return Err(invalid(
                "allocation audit contains uncommitted metadata candidate",
            ));
        }
        if let Some(candidate) = &audit.evidence.joint_configuration_candidate {
            self.facts.require_refresh_topology(candidate)?;
            for range in candidate.ranges.values() {
                self.facts.require_range(range)?;
                if self.version == 1
                    && range
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
impl VerifiedRegistryExtension {
    pub(super) fn facts(&self) -> &HostWriterPolicyFacts {
        &self.0.facts
    }
}
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
                "metadata_voters":value.0.facts.metadata_voters,"effective_range_policy":value.0.facts.ranges})),
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
                if stored.version == 2 {
                    if !format::native_writer_allocation_policy_active(database.data_path())? {
                        return Err(invalid("allocation policy reader barrier absent"));
                    }
                    if self.load_registry_chain(database)?.as_ref() != Some(&stored) {
                        return Err(invalid("allocation policy complete ancestry differs"));
                    }
                }
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
            if entry.version == 2
                && database
                    .paged_records
                    .as_ref()
                    .ok_or_else(|| invalid("allocation audit actual native page store absent"))?
                    .store()
                    .transactions()
                    .status(entry.native_page_xid)
                    != bicdb_page::TxStatus::Committed
            {
                return Err(invalid(
                    "allocation audit ancestry has no actual committed native XID",
                ));
            }
            if entry.sequence != sequence
                || entry.predecessor_sha256 != previous_sha
                || entry.metadata_term < previous_term
                || entry.metadata_read_index < previous_index
            {
                return Err(invalid("native registry audit ancestry changed"));
            }
            if entry.purpose == Some(ExtensionPurpose::AllocationEpoch) {
                let audit: HostMetadataReadAcquireResponse =
                    serde_json::from_value(entry.metadata_audit.clone())?;
                entry.facts.require_allocation_successor(
                    &previous_facts,
                    &audit.evidence.committed_topology,
                )?;
            } else {
                entry.facts.require_registry_successor(&previous_facts)?;
            }
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
    /// Fresh factory-only eligibility, BEFORE a sink owns the attempt mutex.
    /// Never call from with_current_for_publication (which borrows that owner).
    pub(crate) fn require_registry_refresh_preparation_available(&self) -> Result<()> {
        #[cfg(feature = "tls-replication")]
        if self
            .state
            .allocation_attempt
            .try_lock()
            .ok_or_else(|| invalid("allocation attempt busy before registry preparation"))?
            .is_some()
        {
            return Err(invalid(
                "allocation original attempt excludes registry preparation",
            ));
        }
        let actual = self
            .state
            .registry_attempt
            .try_lock()
            .ok_or_else(|| invalid("registry original attempt busy before preparation"))?;
        match actual.as_ref().map(|attempt| attempt.outcome) {
            Some(RegistryOutcome::Entered | RegistryOutcome::Unknown) => Err(invalid(
                "registry original native IO outcome unknown; no fresh preparation",
            )),
            None if self
                .state
                .registry_publication_pending
                .load(Ordering::Acquire) =>
            {
                Err(invalid(
                    "registry untracked pending excludes fresh preparation",
                ))
            }
            _ => Ok(()),
        }
    }
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
        // This factory establishes original identity, not APP admission. The
        // registry's own pending bit must not reject its final publication.
        // All checks below are the ordinary original-range suffix EXCEPT that
        // single registry pending refusal; generic range/APP helpers stay strict.
        self.state.require_original_identity_database(database)?;
        #[cfg(feature = "tls-replication")]
        if let Some(actual) = self.state.effective_serving_data_range(range.id)? {
            if crate::distribution_native_read::canonical_range(range) != actual {
                return Err(invalid(
                    "range configuration differs from verified SAME-State Serving policy",
                ));
            }
        } else if let Some(verified) = self.state.registry_extension.read().as_ref() {
            verified.facts().require_range(range)?;
        } else {
            self.state.record.facts.require_range(range)?;
        }
        #[cfg(not(feature = "tls-replication"))]
        if let Some(verified) = self.state.registry_extension.read().as_ref() {
            verified.facts().require_range(range)?;
        } else {
            self.state.record.facts.require_range(range)?;
        }
        #[cfg(feature = "tls-replication")]
        self.state
            .require_serving_data_database(database, range.id)?;
        Ok(())
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
        #[cfg(feature = "tls-replication")]
        if self
            .state
            .allocation_attempt
            .try_lock()
            .ok_or_else(|| invalid("allocation publication attempt busy during registry refresh"))?
            .is_some()
        {
            return Err(invalid("allocation publication must resolve its SAME retained attempt before registry-only refresh"));
        }
        prepared.require_current(database, coordinator, self)?;
        let policy = prepared.policy();
        policy
            .facts()
            .require_registry_successor(&self.state.record.facts)?;
        let _journal = database.try_native_image_journal()?;
        let mut retained = self
            .state
            .registry_attempt
            .try_lock()
            .ok_or_else(|| invalid("registry original publication attempt busy"))?;
        let result = (|| -> Result<()> {
            if retained.is_none() {
                if self
                    .state
                    .registry_publication_pending
                    .load(Ordering::Acquire)
                {
                    return Err(invalid(
                        "registry pending has no SAME retained attempt; no byte-based success",
                    ));
                }
                let previous = self.state.load_registry_chain(database)?;
                let cached = self.state.registry_extension.read().clone();
                if cached.as_ref().map(|value| &value.0) != previous.as_ref() {
                    return Err(invalid(
                        "registry current has no matching actual verified predecessor",
                    ));
                }
                if let Some(previous) = &previous {
                    policy.facts().require_registry_successor(&previous.facts)?;
                    if policy.metadata_term() < previous.metadata_term
                        || policy.metadata_read_index() < previous.metadata_read_index
                    {
                        return Err(invalid(
                            "registry refresh fresh MQ anchor is behind durable audit",
                        ));
                    }
                }
                let (record, outcome) = if let Some(verified) =
                    cached.filter(|verified| &verified.0.facts == policy.facts())
                {
                    // Actual private published cache, not a native record decoded
                    // into Known. It was minted only after the real helper success.
                    let xid = verified.0.native_page_xid;
                    (verified.0.clone(), RegistryOutcome::Known(xid))
                } else {
                    let sequence = previous.as_ref().map_or(1, |record| record.sequence + 1);
                    if sequence > MAX_EXTENSIONS {
                        return Err(invalid(
                            "native registry extension history exhausted; no eviction",
                        ));
                    }
                    let mut record = RegistryExtensionRecord {
                        version: 1,
                        purpose: None,
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
                    bounded_json(&record, MAX_EXTENSION_BYTES)?;
                    (record, RegistryOutcome::NotEntered)
                };
                // Own original intent before pending memory publication/native IO.
                *retained = Some(RetainedRegistryAttempt {
                    record,
                    outcome,
                    first_error_kind: None,
                });
            }
            let attempt = retained.as_mut().unwrap();
            if &attempt.record.facts != policy.facts()
                || policy.metadata_term() < attempt.record.metadata_term
                || policy.metadata_read_index() < attempt.record.metadata_read_index
            {
                return Err(invalid(
                    "registry fresh scope differs from SAME original candidate",
                ));
            }
            #[cfg(test)]
            let reused = matches!(attempt.outcome, RegistryOutcome::Known(_));
            match attempt.outcome {
                RegistryOutcome::Entered | RegistryOutcome::Unknown => {
                    return Err(invalid(
                    "registry original native IO outcome unknown; no replay or byte-based success",
                ));
                }
                RegistryOutcome::Known(_) => {}
                RegistryOutcome::NotEntered => {
                    parent.check()?;
                    prepared.require_current(database, coordinator, self)?;
                    self.state
                        .registry_publication_pending
                        .store(true, Ordering::Release);
                    attempt.outcome = RegistryOutcome::Entered;
                    let actual = database.in_paged_transaction(|paged, xid| {
                        parent.check()?;
                        prepared.require_request_and_slot_lifetime()?;
                        self.state.require_lifetime()?;
                        attempt.record.native_page_xid = xid;
                        #[cfg(test)]
                        registry_test_cut(
                            database,
                            false,
                            xid,
                            parent.check().is_ok(),
                            prepared.require_request_and_slot_lifetime().is_ok(),
                        );
                        attempt.record.checksum_sha256 = attempt.record.checksum()?;
                        attempt.record.validate(&self.state.record)?;
                        let body = bounded_json(&attempt.record, MAX_EXTENSION_BYTES)?;
                        paged
                            .store()
                            .put(
                                xid,
                                &native_key(&history_key(
                                    attempt.record.registration_generation,
                                    attempt.record.sequence,
                                )),
                                &body,
                            )
                            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                        paged
                            .store()
                            .put(
                                xid,
                                &native_key(&current_key(attempt.record.registration_generation)),
                                &body,
                            )
                            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                        Ok(())
                    });
                    // The ACTUAL helper return is retained before any fallible
                    // postcheck/cancellation. A failed sync is never read as Known.
                    match actual {
                        Ok(Some(xid)) if xid != 0 && xid == attempt.record.native_page_xid => {
                            attempt.outcome = RegistryOutcome::Known(xid);
                        }
                        Ok(Some(_)) => {
                            attempt.outcome = RegistryOutcome::Unknown;
                            return Err(invalid("registry actual returned XID differs from original issued candidate"));
                        }
                        Ok(None) => {
                            attempt.outcome = RegistryOutcome::Unknown;
                            return Err(invalid(
                                "registry extension has no actual native page store",
                            ));
                        }
                        Err(error) => {
                            attempt.outcome = RegistryOutcome::Unknown;
                            return Err(error);
                        }
                    }
                }
            }
            let RegistryOutcome::Known(xid) = attempt.outcome else {
                unreachable!()
            };
            #[cfg(test)]
            if !reused {
                registry_test_cut(
                    database,
                    true,
                    xid,
                    parent.check().is_ok(),
                    prepared.require_request_and_slot_lifetime().is_ok(),
                );
            }
            let record = &attempt.record;
            if xid != record.native_page_xid
                || xid == 0
                || database
                    .paged_records
                    .as_ref()
                    .ok_or_else(|| invalid("registry original page store ended"))?
                    .store()
                    .transactions()
                    .status(xid)
                    != bicdb_page::TxStatus::Committed
            {
                return Err(invalid(
                    "registry retained Known XID has no actual committed native status",
                ));
            }
            let actual = self
                .state
                .load_registry_chain(database)?
                .ok_or_else(|| invalid("registry audit disappeared after native publication"))?;
            if &actual != record {
                return Err(invalid("registry audit readback changed"));
            }
            #[cfg(test)]
            if !reused && TEST_REGISTRY_AFTER_NATIVE_READBACK.with(|cut| cut.replace(false)) {
                return Err(invalid(
                    "injected registry error after actual durable native readback",
                ));
            }
            parent.check()?;
            prepared.with_current_for_publication(database, coordinator, self, || {
                // Keep actual service SLOT and actual hook locked through cache
                // publication; even legacy clear/reinstall uses this hook lock.
                let hook = database
                    .commit_admission
                    .try_read()
                    .ok_or_else(|| invalid("registry final actual hook busy; no grant"))?;
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
                *cache = Some(Arc::new(VerifiedRegistryExtension(record.clone())));
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
            })?;
            // Only the actual successful cache publication completes this owner.
            // Late failure above retains the original Known intent for fresh proof.
            *retained = None;
            Ok(())
        })();
        if let Err(error) = &result {
            if let Some(attempt) = retained.as_mut() {
                if attempt.first_error_kind.is_none() {
                    attempt.first_error_kind = Some(match error {
                        BicDbError::QueryCanceled => "query_canceled",
                        BicDbError::QueryTimedOut => "query_timed_out",
                        BicDbError::CommitOutcomeUnknown { .. } => "commit_outcome_unknown",
                        BicDbError::Io(_) => "io",
                        BicDbError::PagedStorage(_) => "paged_storage",
                        BicDbError::Cluster(_) => "cluster",
                        _ => "other_original_error",
                    });
                }
            }
        }
        result
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

#[cfg(test)]
impl BicDb {
    pub(crate) fn test_current_native_writer_policy_projection(&self) -> Result<serde_json::Value> {
        let record: NativeWriterPolicyRecord = self
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| invalid("test actual writer policy missing"))?;
        record.validate()?;
        Ok(serde_json::to_value(record)?)
    }
}

#[cfg(test)]
impl InstalledNativeWriterHostGuard {
    /// Bounded native DATA for THIS retained original generation, even after
    /// revocation. Does not follow the current hook or grant writer authority.
    /// The actual same DB open/root/birth is still required; no generation DTO.
    pub(crate) fn test_registry_audit_inventory(
        &self,
        database: &BicDb,
    ) -> Result<BTreeMap<String, serde_json::Value>> {
        database.check_recovery_state()?;
        if database.incarnation_id() != self.state.record.database_incarnation
            || database.data_path().canonicalize()? != self.state.root
            || self.state.directory.upgrade().is_none()
        {
            return Err(invalid(
                "test audit inventory needs original actual DB open/root",
            ));
        }
        let facts = &self.state.record.facts;
        let native = database.native_recovery_admission(
            &facts.cluster_id,
            facts.bootstrap_id,
            &facts.local_node_id,
            facts.local_incarnation,
        )?;
        if native.writer_enforcement_version() != 1
            || native.birth_id() != facts.local_birth_id
            || native.birth_sha256() != facts.local_birth_sha256
        {
            return Err(invalid("test audit inventory actual birth differs"));
        }
        let generation = self.state.record.generation;
        let mut inventory = BTreeMap::new();
        for key in std::iter::once(current_key(generation))
            .chain((1..=MAX_EXTENSIONS).map(|sequence| history_key(generation, sequence)))
        {
            let record: Option<RegistryExtensionRecord> =
                database.read_native_range_image_record(&key)?;
            if let Some(record) = record {
                record.validate(&self.state.record)?;
                inventory.insert(key, serde_json::to_value(record)?);
            }
        }
        Ok(inventory)
    }
}

// Passive actual-native cuts for focused normal-host controls. The callback is
// taken out of TLS before invocation, and never supplies operation authority.
#[cfg(test)]
type RegistryNativeCut = Box<dyn FnMut(bool, u64, bool, bool)>;
#[cfg(test)]
thread_local! {
    static TEST_REGISTRY_NATIVE_CUT: std::cell::RefCell<Option<(PathBuf, RegistryNativeCut)>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn registry_test_cut(db: &BicDb, known: bool, xid: u64, parent_live: bool, own_live: bool) {
    let cut = TEST_REGISTRY_NATIVE_CUT.with(|slot| slot.borrow_mut().take());
    if let Some((root, mut observe)) = cut {
        if root == db.data_path() {
            observe(known, xid, parent_live, own_live);
        }
        TEST_REGISTRY_NATIVE_CUT.with(|slot| *slot.borrow_mut() = Some((root, observe)));
    }
}
#[cfg(test)]
impl BicDb {
    pub(crate) fn test_registry_native_cut(
        &self,
        observe: Option<Box<dyn FnMut(bool, u64, bool, bool)>>,
    ) {
        TEST_REGISTRY_NATIVE_CUT.with(|slot| {
            *slot.borrow_mut() = observe.map(|observe| (self.data_path().to_path_buf(), observe));
        });
    }
}
#[cfg(test)]
impl InstalledNativeWriterHostGuard {
    /// Retained DATA even when health/pending rejects ordinary APP. No grant,
    /// health reset, current record adoption or native publication is possible.
    pub(crate) fn test_retained_registry_attempt_point(
        &self,
        db: &BicDb,
    ) -> Result<serde_json::Value> {
        if db.incarnation_id() != self.state.record.database_incarnation
            || db.data_path().canonicalize()? != self.state.root
            || self.state.directory.upgrade().is_none()
        {
            return Err(invalid(
                "registry diagnostic requires SAME original DB/open",
            ));
        }
        let retained = self
            .state
            .registry_attempt
            .try_lock()
            .ok_or_else(|| invalid("registry diagnostic actual attempt busy"))?;
        let actual = db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("registry diagnostic original page store absent"))?;
        Ok(
            serde_json::json!({"retained":retained.as_ref().map(|attempt| {
            let (outcome, known_xid) = match attempt.outcome {
                RegistryOutcome::NotEntered => ("not_entered",None),
                RegistryOutcome::Entered => ("entered",None),
                RegistryOutcome::Known(xid) => ("known_actual_api_ok",Some(xid)),
                RegistryOutcome::Unknown => ("unknown",None),
            };
            serde_json::json!({"outcome":outcome,"known_xid":known_xid,
                "issued_xid":attempt.record.native_page_xid,"candidate_sha256":attempt.record.checksum_sha256,
                "sequence":attempt.record.sequence,"first_error_kind":attempt.first_error_kind})
        }),"pending":self.state.registry_publication_pending.load(Ordering::Acquire),
            "native_next_xid":actual.store().transactions().next_xid(),
            "native_next_lsn":actual.store().wal().next_lsn(),
            "native_durable_lsn":actual.store().wal().durable_lsn(),
            "read_only_retained_DATA_not_ready":true}),
        )
    }
}
