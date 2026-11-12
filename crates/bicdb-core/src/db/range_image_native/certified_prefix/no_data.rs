//! Canonical Aborted publication is one actual metadata page-WAL transaction.
//! There is no application transaction, fake WAL receipt or rangeR→dataB cast.
use super::*;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_mutation::{
    ControlledFrozenMutationPermit, FrozenRangeMutationPermit,
};
use crate::distribution_range_recovery::native_abort::{
    NativeAbortPromiseDisposition, NativeAbortPromiseReply,
};
use crate::distribution_range_recovery::{NativeAbortQuorum, RecoveryRoleAdmission};
fn decision_key(range: RangeId, index: u64) -> String {
    format!("abort-decision:{range}:{index:020}")
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeNoDataPrefixEvidence {
    pub(super) decision_sha256: String,
    pub(super) candidate_sha256: String,
    pub(super) native_page_xid: u64,
    pub(super) local_data_visibility: u64,
    pub(super) data_voters: BTreeSet<ClusterNodeId>,
}
impl NativeNoDataPrefixEvidence {
    pub(super) fn validate(&self, prefix: &NativeCertifiedRangePrefix) -> Result<()> {
        if self.decision_sha256.len() != 64
            || self.candidate_sha256.len() != 64
            || self.native_page_xid == 0
            || self.data_voters.is_empty()
            || self.data_voters.len() > 64
            || prefix.required_quorum != self.data_voters.len() / 2 + 1
            || prefix
                .certified_voters
                .iter()
                .any(|id| !self.data_voters.contains(id))
        {
            return Err(invalid("damaged canonical no-data prefix evidence"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct NativeAbortDecisionRecord {
    version: u32,
    birth_id: Uuid,
    base_receipt_sha256: String,
    scope: crate::distribution_range_consensus::RecoveryFenceScope,
    domain: crate::distribution_range_consensus::native_abort::NativeAbortDomain,
    range_index: u64,
    previous_index: u64,
    previous_receipt_sha256: String,
    previous_history_sha256: String,
    history_generation: Uuid,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    candidate_sha256: String,
    actual_promises: BTreeMap<ClusterNodeId, NativeAbortPromiseReply>,
    post_quorum_metadata_context_id: Uuid,
    post_quorum_metadata_term: u64,
    post_quorum_metadata_read_index: u64,
    native_page_xid: u64,
    local_data_visibility: u64,
    checksum_sha256: String,
}
impl NativeAbortDecisionRecord {
    fn checksum(&self) -> Result<String> {
        let mut r = self.clone();
        r.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native-canonical-abort-decision-v1", r),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self, birth: &NativeDatabaseBirth) -> Result<()> {
        self.scope.validate()?;
        let voters = self.domain.member_ids();
        if self.version != 1 || birth.abort_protocol_version != Some(1) || birth.format_version != 2
            || birth.metadata_initialization_version != Some(1) || self.birth_id != birth.birth_id
            || self.base_receipt_sha256.len() != 64 || self.previous_receipt_sha256.len() != 64
            || self.previous_index.checked_add(1) != Some(self.range_index) || self.candidate_sha256.len() != 64
            || self.native_page_xid == 0 || self.post_quorum_metadata_context_id.is_nil()
            || self.history_generation.is_nil() || self.logical_genesis_sha256.len() != 64
            || self.logical_history_sha256.len() != 64 || self.previous_history_sha256.len() != 64
            || self.actual_promises.len() < voters.len()/2+1 || self.actual_promises.len() > voters.len()
            || self.actual_promises.iter().any(|(id, p)| {
                !voters.contains(id) || id != &p.node_id || !p.request.scope.same_session(&self.scope)
                    || p.request.scope.validate().is_err() || p.request.range_index != self.range_index
                    || p.request.candidate_sha256.as_deref() != Some(self.candidate_sha256.as_str()) || p.candidate_sha256 != self.candidate_sha256
                    || p.predecessor_index != self.previous_index || p.predecessor_history_sha256 != self.previous_history_sha256
                    || p.observed_metadata_context_id == self.post_quorum_metadata_context_id
                    || p.observed_metadata_context_id.is_nil() || p.observed_metadata_term > self.post_quorum_metadata_term
                    || p.observed_metadata_read_index > self.post_quorum_metadata_read_index
                    || !matches!(&p.disposition, NativeAbortPromiseDisposition::Permanent { promise_sha256, original_candidate_sha256, original_command_sha256 } if promise_sha256.len() == 64 && original_candidate_sha256.len() == 64 && original_command_sha256.len() == 64)
            })
            || self.checksum()? != self.checksum_sha256 {
            return Err(invalid("damaged/missing actual permanent AbortPromise quorum audit"));
        }
        let entry = history::NativeLogicalHistoryEntryV1::canonical_aborted(
            self.scope.range_id,
            self.range_index,
            &self.previous_history_sha256,
        )?;
        if entry.commitment_sha256 != self.logical_history_sha256 {
            return Err(invalid("abort decision logical spine differs"));
        }
        Ok(())
    }
}
/// The actual durable no-data result. Its localB is inherited from the actual
/// predecessor and grants no read floor/serving permission by itself.
#[derive(Debug)]
pub(crate) struct NativeRecoveryAbortedReceipt {
    database_incarnation: String,
    birth_id: Uuid,
    scope: crate::distribution_range_consensus::RecoveryFenceScope,
    range_index: u64,
    decision_sha256: String,
    logical_history_sha256: String,
    local_data_visibility: u64,
}
impl NativeRecoveryAbortedReceipt {
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.scope.range_id
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        &self.decision_sha256
    }
    pub(crate) fn local_data_visibility(&self) -> u64 {
        self.local_data_visibility
    }
    pub(crate) fn require_current(&self, db: &BicDb, role: &RecoveryRoleAdmission) -> Result<()> {
        self.scope.require_role(role)?;
        db.check_recovery_state()?;
        if self.database_incarnation != db.incarnation_id() {
            return Err(invalid("abort receipt belongs to a closed DB/open"));
        }
        let birth = db
            .read_native_birth()?
            .ok_or_else(|| invalid("abort receipt actual birth absent"))?;
        if birth.birth_id != self.birth_id {
            return Err(invalid("abort receipt actual birth changed"));
        }
        let record: NativeAbortDecisionRecord = db
            .read_native_range_image_record(&decision_key(self.scope.range_id, self.range_index))?
            .ok_or_else(|| invalid("actual native abort decision missing"))?;
        record.validate(&birth)?;
        record.domain.require_role(role)?;
        let lineage = db.current_native_range_source_lineage(self.scope.range_id)?;
        let entry = db.read_native_history_entry(
            self.scope.range_id,
            lineage.history_generation(),
            self.range_index,
        )?;
        if record.checksum_sha256 != self.decision_sha256
            || record.logical_history_sha256 != self.logical_history_sha256
            || entry.outcome != history::NativeLogicalHistoryOutcomeV1::Aborted
            || entry.commitment_sha256 != self.logical_history_sha256
            || lineage.range_index() < self.range_index
        {
            return Err(invalid(
                "actual native abort receipt/history no longer agrees",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    pub(super) fn validate_native_abort_prefix_record(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
        prefix: &NativeCertifiedRangePrefix,
    ) -> Result<()> {
        let no_data = prefix
            .no_data
            .as_ref()
            .ok_or_else(|| invalid("no-data prefix evidence missing"))?;
        let record: NativeAbortDecisionRecord = self
            .read_native_range_image_record(&decision_key(prefix.range_id, prefix.range_index))?
            .ok_or_else(|| invalid("protected actual AbortPromise quorum decision missing"))?;
        record.validate(birth)?;
        if record.scope.range_id != prefix.range_id
            || record.range_index != prefix.range_index
            || record.previous_index != prefix.previous_index
            || record.previous_receipt_sha256 != prefix.previous_receipt_sha256
            || record.logical_genesis_sha256 != prefix.logical_genesis_sha256
            || record.scope.configuration_sha256 != prefix.scope_sha256
            || prefix.certificate_evidence
                != (NativeRangeCertificateEvidence::AbortPromiseQuorum {
                    recovery_nonce: record.scope.recovery_nonce,
                    configuration_sha256: record.scope.configuration_sha256.clone(),
                    current_range_epoch: record.scope.range_epoch,
                    metadata_context_id: record.post_quorum_metadata_context_id,
                })
            || record.base_receipt_sha256 != base.checksum_sha256
            || record.checksum_sha256 != no_data.decision_sha256
            || record.native_page_xid != no_data.native_page_xid
            || record.local_data_visibility != no_data.local_data_visibility
            || record.candidate_sha256 != no_data.candidate_sha256
            || record.history_generation != prefix.history_generation
            || record.logical_history_sha256 != prefix.logical_history_sha256
            || record
                .actual_promises
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != prefix.certified_voters
            || record.domain.member_ids() != no_data.data_voters
        {
            return Err(invalid(
                "no-data prefix differs from actual protected decision/WAL",
            ));
        }
        Ok(())
    }
    pub(crate) fn apply_native_abort_quorum(
        &self,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        fresh: &RecoveryRoleAdmission,
        quorum: &NativeAbortQuorum,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
        parent: &CancellationToken,
    ) -> Result<NativeRecoveryAbortedReceipt> {
        parent.check()?;
        native.require_current(self)?;
        quorum.require_recipient_role(fresh)?;
        self.require_installed_native_writer_database_for_range(fresh.current_range())?;
        store.preflight_native_abort_publication(self, native, fresh, quorum)?;
        self.range_mutations
            .validate_frozen(fresh.current_range().id, frozen)?;
        self.range_mutations.validate(
            fresh.current_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        let source = self.current_native_range_source_lineage(fresh.current_range().id)?;
        let index = quorum.request().range_index;
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("abort publisher native birth absent"))?;
        let base = source.base.clone();
        let _journal = self.try_native_image_journal()?;
        self.validate_source_range_image_lineage(
            &source,
            fresh.current_range().id,
            source.range_index(),
        )?;
        if source.range_index() >= index {
            let record: NativeAbortDecisionRecord = self.read_native_range_image_record(&decision_key(fresh.current_range().id, index))?
                .ok_or_else(|| invalid("covered abort requires original protected decision; nativeR alone is not proof"))?;
            record.validate(&birth)?;
            record.domain.require_role(fresh)?;
            let actual = self.read_native_history_entry(
                fresh.current_range().id,
                source.history_generation(),
                index,
            )?;
            if actual.outcome != history::NativeLogicalHistoryOutcomeV1::Aborted
                || actual.commitment_sha256 != record.logical_history_sha256
            {
                return Err(invalid(
                    "covered index is a committed command, not canonical Abort",
                ));
            }
            return Ok(NativeRecoveryAbortedReceipt {
                database_incarnation: self.incarnation.clone(),
                birth_id: birth.birth_id,
                scope: quorum.request().scope.clone(),
                range_index: index,
                decision_sha256: record.checksum_sha256,
                logical_history_sha256: record.logical_history_sha256,
                local_data_visibility: record.local_data_visibility,
            });
        }
        if source.range_index().checked_add(1) != Some(index)
            || source.logical_history_sha256()
                != quorum.candidate().record().predecessor_history_sha256()
        {
            return Err(invalid(
                "canonical abort cannot skip an actual native predecessor",
            ));
        }
        let local_promise = self
            .observe_actual_native_abort_promise(fresh.current_range().id, index)?
            .ok_or_else(|| {
                invalid("first abort publisher requires its actual permanent local promise")
            })?;
        if local_promise.domain != quorum.candidate().record().domain
            || local_promise.predecessor_index != source.range_index()
            || local_promise.predecessor_history_sha256 != source.logical_history_sha256()
        {
            return Err(invalid(
                "local permanent promise binds another domain/history",
            ));
        }
        let command = &quorum.candidate().record().command;
        let entry = history::NativeLogicalHistoryEntryV1::canonical_aborted(
            command.range_id,
            index,
            source.logical_history_sha256(),
        )?;
        let mut record = NativeAbortDecisionRecord {
            version: 1,
            birth_id: birth.birth_id,
            base_receipt_sha256: base.checksum_sha256.clone(),
            scope: quorum.request().scope.clone(),
            domain: quorum.candidate().record().domain.clone(),
            range_index: index,
            previous_index: source.range_index(),
            previous_receipt_sha256: source.native_receipt_sha256().into(),
            previous_history_sha256: source.logical_history_sha256().into(),
            history_generation: source.history_generation(),
            logical_genesis_sha256: source.logical_genesis_sha256().into(),
            logical_history_sha256: entry.commitment_sha256.clone(),
            candidate_sha256: quorum.candidate().sha256().into(),
            actual_promises: quorum.audit().clone(),
            post_quorum_metadata_context_id: fresh.context_id(),
            post_quorum_metadata_term: fresh.metadata_term(),
            post_quorum_metadata_read_index: fresh.metadata_read_index(),
            native_page_xid: 0,
            local_data_visibility: source.actual_local_visibility_sequence(),
            checksum_sha256: String::new(),
        };
        if record
            .actual_promises
            .values()
            .any(|p| p.observed_metadata_context_id == fresh.context_id())
        {
            return Err(invalid(
                "abort publication requires separately fresh post-quorum metadata",
            ));
        }
        let mut prefix = NativeCertifiedRangePrefix {
            format_version: 3,
            birth_id: birth.birth_id,
            base_receipt_sha256: base.checksum_sha256.clone(),
            range_id: command.range_id,
            range_index: index,
            previous_index: source.range_index(),
            previous_receipt_sha256: source.native_receipt_sha256().into(),
            command_id: command.command_id.clone(),
            command_checksum_sha256: command.checksum_sha256.clone(),
            mutations_sha256: admission_mutations_sha256(&command.mutations)?,
            scope_sha256: quorum.request().scope.configuration_sha256.clone(),
            materialized: None,
            no_data: None,
            certified_voters: record.actual_promises.keys().cloned().collect(),
            required_quorum: record.domain.member_ids().len() / 2 + 1,
            certificate_evidence: NativeRangeCertificateEvidence::AbortPromiseQuorum {
                recovery_nonce: fresh.recovery_nonce(),
                configuration_sha256: fresh.configuration_sha256()?,
                current_range_epoch: fresh.current_range().epoch,
                metadata_context_id: fresh.context_id(),
            },
            recovery_attempt: None,
            global_image_generation: base.global_image_generation,
            range_image_generation: base.range_image_generation,
            history_generation: base.history_generation,
            logical_genesis_sha256: base.logical_genesis_sha256.clone(),
            logical_history_sha256: entry.commitment_sha256.clone(),
            checksum_sha256: String::new(),
        };
        self.in_paged_transaction(|paged, xid| {
            record.native_page_xid = xid;
            record.checksum_sha256 = record.checksum()?;
            record.validate(&birth)?;
            prefix.no_data = Some(NativeNoDataPrefixEvidence {
                decision_sha256: record.checksum_sha256.clone(),
                candidate_sha256: record.candidate_sha256.clone(),
                native_page_xid: xid,
                local_data_visibility: record.local_data_visibility,
                data_voters: record.domain.member_ids(),
            });
            prefix.checksum_sha256 = prefix.checksum()?;
            prefix.validate(&birth, &base)?;
            history::put_immutable_history_entry(paged, xid, base.history_generation, &entry)?;
            paged
                .store()
                .put(
                    xid,
                    &native_key(&decision_key(command.range_id, index)),
                    &bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?,
                )
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            paged
                .store()
                .put(
                    xid,
                    &native_key(&prefix_key(command.range_id)),
                    &bounded_json(&prefix, MAX_NATIVE_RECEIPT_BYTES)?,
                )
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("canonical Abort has no actual native page-WAL transaction"))?;
        parent.check()?;
        quorum.require_recipient_role(fresh)?;
        self.validate_native_abort_prefix_record(&birth, &base, &prefix)?;
        Ok(NativeRecoveryAbortedReceipt {
            database_incarnation: self.incarnation.clone(),
            birth_id: birth.birth_id,
            scope: quorum.request().scope.clone(),
            range_index: index,
            decision_sha256: record.checksum_sha256,
            logical_history_sha256: record.logical_history_sha256,
            local_data_visibility: record.local_data_visibility,
        })
    }
}
