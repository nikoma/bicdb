//! Durable local pending/materialized/certified boundaries. A pending record
//! never raises the source floor. Only the actual WAL callback AND the opaque
//! coordinator certificate quorum can produce the completed prefix below.
use super::*;
use crate::distribution_range_consensus::{
    NativeRangeCertificateEvidence, NativeRangeCommandIntent, NativeRangeCommitCertificate,
};
use crate::distribution_range_mutation::RangeMutationAuthority;
mod no_data;
use no_data::NativeNoDataPrefixEvidence;
pub(crate) use no_data::NativeRecoveryAbortedReceipt;
mod ordinary_apply;
#[cfg(test)]
mod origin_fault;
mod recovery_apply;
pub(crate) use ordinary_apply::NativeReplicaMaterialization;
#[cfg(test)]
pub(in crate::db) use origin_fault::test_record_origin_release;
#[cfg(test)]
pub(crate) use origin_fault::{NativeOriginAppSpan, NativeOriginFaultArm, NativeOriginFaultProbe};
pub(crate) use recovery_apply::{KnownNativeRecoveryPrefix, NativeRecoveryAppliedReceipt};

fn pending_key(range: RangeId) -> String {
    format!("pending:{}", range.get())
}
fn prefix_key(range: RangeId) -> String {
    format!("prefix:{}", range.get())
}

const MAX_NATIVE_COMMAND_SCOPES: usize = 65_536;
#[derive(Debug)]
struct NativeCommandScope {
    lease: Uuid,
    database_incarnation: String,
    mutations_sha256: String,
    armed: bool,
}
#[derive(Debug, Default)]
pub(in crate::db) struct NativeRangeScopeRegistry {
    entries: BTreeMap<u64, NativeCommandScope>,
    #[cfg(test)]
    origin_fault: Option<Arc<NativeOriginFaultProbe>>,
}
#[derive(Debug)]
struct NativeCommandScopeLease {
    registry: Arc<Mutex<NativeRangeScopeRegistry>>,
    transaction_id: u64,
    lease: Uuid,
}
impl NativeCommandScopeLease {
    fn arm(&self) -> Result<()> {
        let mut registry = self.registry.lock();
        let entry = registry
            .entries
            .get_mut(&self.transaction_id)
            .ok_or_else(|| invalid("native tx scope reservation disappeared"))?;
        if entry.lease != self.lease {
            return Err(invalid("native tx scope lease changed"));
        }
        entry.armed = true;
        Ok(())
    }
}
impl Drop for NativeCommandScopeLease {
    fn drop(&mut self) {
        let mut registry = self.registry.lock();
        if registry
            .entries
            .get(&self.transaction_id)
            .is_some_and(|entry| entry.lease == self.lease)
        {
            registry.entries.remove(&self.transaction_id);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MaterializedNativeCommand {
    local_transaction_id: u64,
    local_wal_sequence: u64,
    actual_mutations_sha256: String,
    database_incarnation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PendingNativeRangeCommand {
    format_version: u32,
    birth_id: Uuid,
    range_id: RangeId,
    range_index: u64,
    previous_index: u64,
    previous_receipt_sha256: String,
    command_id: String,
    command_checksum_sha256: String,
    mutations_sha256: String,
    scope_sha256: String,
    expected_local_transaction_id: u64,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    materialized: Option<MaterializedNativeCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery_attempt: Option<recovery_apply::RecoveryNativeAttemptBinding>,
    checksum_sha256: String,
}
impl PendingNativeRangeCommand {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        if let Some(attempt) = &self.recovery_attempt {
            attempt.validate(self)?;
        }
        if self.format_version != 1
            || self.birth_id.is_nil()
            || self.previous_index.checked_add(1) != Some(self.range_index)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged pending native command"));
        }
        Ok(())
    }
}

/// Current-open handle for one actual durable pending record. Not a commit
/// decision, certificate, visibility permit or serializable recovery token.
#[derive(Debug)]
pub(crate) struct NativeRangePendingApply {
    database_incarnation: String,
    intent: NativeRangeCommandIntent,
    pending: PendingNativeRangeCommand,
    _scope: NativeCommandScopeLease,
}

/// Destructive-install guard observation. Invalidating source visibility does
/// not erase these irreversible native lower bounds. It is neither a source
/// image nor a recovery role; only actual local records can construct it.
#[derive(Debug)]
pub(crate) struct NativeRangeInstallSafety {
    database_incarnation: String,
    birth_id: Uuid,
    range_id: RangeId,
    base_index: Option<u64>,
    applied_index: Option<u64>,
    pending_index: Option<u64>,
    materialized_pending_index: Option<u64>,
    source_invalidated: bool,
    records_sha256: String,
    irreversible_records_sha256: String,
    history_point: Option<history::NativeLogicalHistoryPoint>,
    pending_command_checksum_sha256: Option<String>,
}
impl NativeRangeInstallSafety {
    pub(crate) fn base_index(&self) -> Option<u64> {
        self.base_index
    }
    pub(crate) fn applied_index(&self) -> Option<u64> {
        self.applied_index
    }
    pub(crate) fn maximum_irreversible_index(&self) -> Option<u64> {
        self.applied_index.max(self.materialized_pending_index)
    }
    pub(crate) fn maximum_pending_index(&self) -> Option<u64> {
        self.pending_index
    }
    pub(crate) fn pending_index(&self) -> Option<u64> {
        self.pending_index
    }
    pub(crate) fn materialized_pending_index(&self) -> Option<u64> {
        self.materialized_pending_index
    }
    pub(crate) fn source_invalidated(&self) -> bool {
        self.source_invalidated
    }
    pub(crate) fn birth_id(&self) -> Uuid {
        self.birth_id
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.range_id
    }
    pub(crate) fn records_sha256(&self) -> &str {
        &self.records_sha256
    }
    pub(crate) fn irreversible_records_sha256(&self) -> &str {
        &self.irreversible_records_sha256
    }
    pub(crate) fn history_point(&self) -> Option<&history::NativeLogicalHistoryPoint> {
        self.history_point.as_ref()
    }
    pub(crate) fn pending_command_checksum_sha256(&self) -> Option<&str> {
        self.pending_command_checksum_sha256.as_deref()
    }
    /// For an already-authorized install after its own invalidations. This
    /// checks immutable lower bounds only, never authorizes destructive work.
    pub(crate) fn require_irreversible_current(
        &self,
        database: &BicDb,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        let journal = database.try_native_image_journal()?;
        self.require_irreversible_current_under_journal(database, admission, frozen, &journal)
    }
    pub(super) fn require_irreversible_current_under_journal(
        &self,
        database: &BicDb,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<()> {
        let current = database.native_range_install_safety_under_journal(
            admission,
            self.range_id,
            frozen,
            journal,
        )?;
        if self.database_incarnation != current.database_incarnation
            || self.birth_id != current.birth_id
            || self.irreversible_records_sha256 != current.irreversible_records_sha256
        {
            return Err(invalid(
                "immutable native history/pending lower bounds changed during install",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        let journal = database.try_native_image_journal()?;
        self.require_current_under_journal(database, admission, frozen, &journal)
    }
    pub(in crate::db) fn require_current_under_journal(
        &self,
        database: &BicDb,
        admission: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<()> {
        let current = database.native_range_install_safety_under_journal(
            admission,
            self.range_id,
            frozen,
            journal,
        )?;
        if self.database_incarnation != current.database_incarnation
            || self.birth_id != current.birth_id
            || self.records_sha256 != current.records_sha256
        {
            return Err(invalid("destructive-install native lower bounds changed"));
        }
        Ok(())
    }
}

/// Constant-size durable prefix extension backed by actual data/WAL
/// materialization or a separately proven native no-data decision. Previous
/// receipts form local history, not portable signatures or metadata authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct NativeCertifiedRangePrefix {
    format_version: u32,
    birth_id: Uuid,
    base_receipt_sha256: String,
    range_id: RangeId,
    range_index: u64,
    previous_index: u64,
    previous_receipt_sha256: String,
    command_id: String,
    command_checksum_sha256: String,
    mutations_sha256: String,
    scope_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    materialized: Option<MaterializedNativeCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    no_data: Option<NativeNoDataPrefixEvidence>,
    certified_voters: BTreeSet<ClusterNodeId>,
    required_quorum: usize,
    #[serde(
        default,
        skip_serializing_if = "NativeRangeCertificateEvidence::is_origin"
    )]
    certificate_evidence: NativeRangeCertificateEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery_attempt: Option<recovery_apply::RecoveryNativeAttemptBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    joint_decision_sha256: Option<String>,
    global_image_generation: Uuid,
    range_image_generation: Uuid,
    history_generation: Uuid,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    checksum_sha256: String,
}
impl NativeCertifiedRangePrefix {
    pub(super) fn actual_local_visibility_sequence(&self) -> u64 {
        self.materialized.as_ref().map_or_else(
            || {
                self.no_data
                    .as_ref()
                    .expect("validated no-data prefix")
                    .local_data_visibility
                    .get()
            },
            |materialized| materialized.local_wal_sequence,
        )
    }
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    pub(super) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(super) fn checksum_sha256(&self) -> &str {
        &self.checksum_sha256
    }
    pub(super) fn logical_history_sha256(&self) -> &str {
        &self.logical_history_sha256
    }
    pub(super) fn validate(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<()> {
        self.validate_with_native_joint_domain(birth, base, None)
    }
    fn validate_with_native_joint_domain(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
        joint: Option<&NativeJointServingDomain>,
    ) -> Result<()> {
        let joint = joint.filter(|domain| self.range_index > domain.range_index());
        if self.joint_decision_sha256.as_deref() != joint.map(|domain| domain.decision_sha256()) {
            return Err(invalid(
                "native Applied prefix lacks its actual protected joint domain",
            ));
        }
        self.certificate_evidence.validate()?;
        if matches!(
            self.certificate_evidence,
            NativeRangeCertificateEvidence::HistoricalCurrentCertificateQuorum { .. }
        ) != self.recovery_attempt.is_some()
        {
            return Err(invalid(
                "native prefix certificate kind and actual recovery attempt differ",
            ));
        }
        if let Some(attempt) = &self.recovery_attempt {
            attempt.validate_prefix(
                &self.command_checksum_sha256,
                self.materialized.as_ref().ok_or_else(|| {
                    invalid("applied recovery attempt lacks actual WAL materialization")
                })?,
                &self.certificate_evidence,
            )?;
        }
        match &self.certificate_evidence {
            NativeRangeCertificateEvidence::AuthenticatedLeaderInstruction {
                leader_node_id,
                leader_incarnation,
                peer_certificate_sha256,
                plaintext_loopback,
                bootstrap_id,
                range_epoch,
                restore_activation,
                scope_sha256,
                ..
            } => {
                let members = joint.map_or(&base.base_members, |domain| domain.members());
                let member = members.get(leader_node_id).ok_or_else(|| {
                    invalid("native materialization leader is not in actual base")
                })?;
                let pin = match (
                    &member.tls_certificate_sha256,
                    &member.pending_tls_certificate_sha256,
                ) {
                    (None, None) => *plaintext_loopback && peer_certificate_sha256.is_none(),
                    (a, p) => peer_certificate_sha256
                        .as_deref()
                        .is_some_and(|sha| a.as_deref() == Some(sha) || p.as_deref() == Some(sha)),
                };
                if !self.certified_voters.is_empty()
                    || self.required_quorum != 0
                    || *bootstrap_id != birth.spec.bootstrap_id
                    || *range_epoch < base.range.epoch
                    || member.incarnation != *leader_incarnation
                    || !pin
                    || restore_activation.as_ref() != base.restore_activation.as_ref()
                    || scope_sha256 != &self.scope_sha256
                    || joint.is_some_and(|domain| {
                        *range_epoch != domain.range().epoch
                            || leader_node_id != &domain.range().leader
                    })
                {
                    return Err(invalid("native authenticated materialization cannot claim a local certificate quorum"));
                }
            }
            _ => {
                if self.required_quorum == 0
                    || self.certified_voters.len() < self.required_quorum
                    || self.certified_voters.len() > 64
                {
                    return Err(invalid("native prefix lacks its actual quorum evidence"));
                }
                if joint.is_some_and(|domain| {
                    self.required_quorum != domain.members().len() / 2 + 1
                        || !self
                            .certified_voters
                            .iter()
                            .all(|node| domain.members().contains_key(node))
                }) {
                    return Err(invalid(
                        "native Applied quorum differs from actual joint NEW voters",
                    ));
                }
            }
        }
        let materialization_shape = match (&self.materialized, &self.no_data) {
            (Some(actual), None) => {
                self.format_version == 2
                    && actual.actual_mutations_sha256 == self.mutations_sha256
                    && !matches!(
                        self.certificate_evidence,
                        NativeRangeCertificateEvidence::AbortPromiseQuorum { .. }
                    )
            }
            (None, Some(no_data)) => {
                self.format_version == 3
                    && no_data.validate(self).is_ok()
                    && birth.abort_protocol_version == Some(1)
                    && matches!(
                        self.certificate_evidence,
                        NativeRangeCertificateEvidence::AbortPromiseQuorum { .. }
                    )
            }
            _ => false,
        };
        if !materialization_shape
            || self.birth_id != birth.birth_id
            || self.base_receipt_sha256 != base.checksum_sha256
            || self.range_id != base.range.id
            || self.range_index <= base.range_index
            || self.previous_index.checked_add(1) != Some(self.range_index)
            || self.global_image_generation != base.global_image_generation
            || self.range_image_generation != base.range_image_generation
            || self.history_generation != base.history_generation
            || self.logical_genesis_sha256 != base.logical_genesis_sha256
            || self.logical_history_sha256.len() != 64
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged/mismatched native certified prefix"));
        }
        Ok(())
    }
}

enum NativeCompletionMaterializationEvidence<'a> {
    Quorum(&'a NativeRangeCommitCertificate),
    Leader(&'a crate::distribution_native_apply::NativeLeaderInstructionAdmission),
}
impl NativeCompletionMaterializationEvidence<'_> {
    fn require_live(&self) -> Result<()> {
        match self {
            Self::Quorum(_) => Ok(()),
            Self::Leader(admission) => admission.require_live(),
        }
    }
    fn intent(&self) -> &NativeRangeCommandIntent {
        match self {
            Self::Quorum(q) => q.intent(),
            Self::Leader(a) => a.intent(),
        }
    }
    fn certified_voters(&self) -> BTreeSet<ClusterNodeId> {
        match self {
            Self::Quorum(q) => q.certified_voters().clone(),
            Self::Leader(_) => BTreeSet::new(),
        }
    }
    fn required_quorum(&self) -> usize {
        match self {
            Self::Quorum(q) => q.required_quorum(),
            Self::Leader(_) => 0,
        }
    }
    fn evidence(&self) -> NativeRangeCertificateEvidence {
        match self {
            Self::Quorum(q) => q.evidence().clone(),
            Self::Leader(a) => a.materialization_evidence(),
        }
    }
}

impl BicDb {
    /// Scope ownership is not durability/quorum evidence. Its sole purpose is
    /// preventing ordinary native row apply without the actual pending journal.
    pub(in crate::db) fn require_native_transaction_mutation_scope(
        &self,
        transaction: &Transaction,
    ) -> Result<()> {
        if !self.native_range_image_birth_present()
            || (transaction.bypass_commit_admission
                && self.actual_native_writer_enforcement_version()? == 0)
            || transaction.writes.is_empty()
        {
            return Ok(());
        }
        // The private WAL-only fixtures have no actual birth or source image;
        // a flag alone must not manufacture a native lineage obligation.
        if self.read_native_birth()?.is_none() {
            return Ok(());
        }
        if let Some(grant) = &transaction.native_image_row_grant {
            return grant.require_current(self, transaction);
        }
        let actual_hash =
            super::super::native_wal_apply::actual_transaction_mutations_sha256(transaction)?;
        let registry = self.native_range_command_scopes.lock();
        let matched = registry
            .entries
            .get(&transaction.id().0)
            .is_some_and(|entry| {
                entry.armed
                    && entry.database_incarnation == self.incarnation
                    && entry.mutations_sha256 == actual_hash
            });
        if !matched {
            return Err(invalid(
                "ordinary native mutation has no owned, durable pending-command scope",
            ));
        }
        Ok(())
    }
    pub(crate) fn native_range_install_safety(
        &self,
        admission: &NativeRecoveryAdmission,
        range_id: RangeId,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeRangeInstallSafety> {
        let journal = self.try_native_image_journal()?;
        self.native_range_install_safety_under_journal(admission, range_id, frozen, &journal)
    }
    fn native_range_install_safety_under_journal(
        &self,
        admission: &NativeRecoveryAdmission,
        range_id: RangeId,
        frozen: &FrozenRangeMutationPermit,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<NativeRangeInstallSafety> {
        journal.require_database(self)?;
        admission.require_current(self)?;
        // This nonwaiting check also excludes same-owner controlled writers.
        // Draining is a caller operation outside DB/service guards.
        self.range_mutations
            .validate_quiescent_frozen(range_id, frozen)?;
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("install safety has no native birth"))?;
        let base = self.actual_native_range_integrity_binding(&birth, range_id)?;
        if base.is_none()
            && birth
                .spec
                .initial_ranges
                .iter()
                .any(|range| range.id == range_id)
        {
            return Err(invalid(
                "expected native genesis is missing; lower bound is unknown",
            ));
        }
        let prefix = match &base {
            Some(base) => {
                self.validate_native_range_base_history(&birth, base)?;
                self.read_native_certified_range_prefix(&birth, base)?
            }
            None => {
                if self
                    .read_native_range_image_record::<serde_json::Value>(&prefix_key(range_id))?
                    .is_some()
                {
                    return Err(invalid("native prefix has no known physical base"));
                }
                None
            }
        };
        let pending: Option<PendingNativeRangeCommand> =
            self.read_native_range_image_record(&pending_key(range_id))?;
        let applied_index = prefix
            .as_ref()
            .map(|prefix| prefix.range_index)
            .or_else(|| base.as_ref().map(|base| base.range_index));
        if let Some(pending) = &pending {
            pending.validate()?;
            if pending.birth_id != birth.birth_id
                || pending.range_id != range_id
                || applied_index != Some(pending.previous_index)
            {
                return Err(invalid(
                    "native pending lower bound has an unknown/mismatched prefix",
                ));
            }
        }
        let generations = self.native_range_image_generations(&birth, range_id)?;
        let source_invalidated = base.as_ref().is_some_and(|base| {
            generations != (base.global_image_generation, base.range_image_generation)
        });
        let records_sha256 = hex::encode(sha2::Sha256::digest(bounded_json(
            &(
                &birth.checksum_sha256,
                range_id,
                &base,
                &prefix,
                &pending,
                generations,
            ),
            MAX_NATIVE_RECEIPT_BYTES,
        )?));
        let irreversible_records_sha256 = hex::encode(sha2::Sha256::digest(bounded_json(
            &(&birth.checksum_sha256, range_id, &base, &prefix, &pending),
            MAX_NATIVE_RECEIPT_BYTES,
        )?));
        let history_point = base
            .as_ref()
            .map(|base| history::actual_history_point(&birth, base, prefix.as_ref()));
        let pending_command_checksum_sha256 = pending
            .as_ref()
            .map(|pending| pending.command_checksum_sha256.clone());
        Ok(NativeRangeInstallSafety {
            database_incarnation: self.incarnation.clone(),
            birth_id: birth.birth_id,
            range_id,
            base_index: base.map(|base| base.range_index),
            applied_index,
            pending_index: pending.as_ref().map(|pending| pending.range_index),
            materialized_pending_index: pending
                .as_ref()
                .and_then(|pending| pending.materialized.as_ref().map(|_| pending.range_index)),
            source_invalidated,
            records_sha256,
            irreversible_records_sha256,
            history_point,
            pending_command_checksum_sha256,
        })
    }
    /// Narrow local replica materialization: a concrete coordinator certificate
    /// and exact final mutations are mandatory. Remote/recovery admission is a
    /// separate producer; raw decoded CQ/Applied records cannot call this API.
    pub(crate) fn apply_native_locally_certified_command(
        &self,
        certificate: &NativeRangeCommitCertificate,
        mutations: &[CommitAdmissionMutation],
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        self.apply_native_origin_certificate(certificate, mutations, authority, false)
    }
    pub(crate) fn apply_native_origin_certificate(
        &self,
        certificate: &NativeRangeCommitCertificate,
        mutations: &[CommitAdmissionMutation],
        authority: &RangeMutationAuthority<'_>,
        imported_projection: bool,
    ) -> Result<()> {
        if !certificate.evidence().is_origin() {
            return Err(invalid(
                "historical materialization requires exact current-session native recovery adapter",
            ));
        }
        let intent = certificate.intent();
        if admission_mutations_sha256(mutations)? != intent.mutations_sha256() {
            return Err(invalid(
                "certified native apply differs from the actual final mutations",
            ));
        }
        let lineage = self.current_native_range_source_lineage(intent.range().id)?;
        if lineage.range_index() == intent.range_index() {
            if lineage.prefix.as_ref().is_some_and(|prefix| {
                prefix.materialized.is_some()
                    && prefix.no_data.is_none()
                    && prefix.command_id == intent.command_id()
                    && prefix.command_checksum_sha256 == intent.command_checksum_sha256()
                    && prefix.scope_sha256 == intent.scope_sha256()
                    && prefix.mutations_sha256 == intent.mutations_sha256()
            }) {
                return Ok(());
            }
            return Err(invalid(
                "native duplicate index differs from completed command",
            ));
        }
        let mut transaction = self.begin_transaction()?;
        let pending = self.prepare_native_range_command(intent, transaction.id().0, authority)?;
        transaction.bypass_commit_admission = true;
        transaction.imported_projection = imported_projection;
        transaction.range_mutation_claim = Some(authority.claim());
        transaction.set_snapshot_isolation(false, 0)?;
        let result = (|| {
            for mutation in mutations {
                let reserved = transaction.read_committed_update_record_nowait(
                    &mutation.collection,
                    &mutation.record_id,
                )?;
                let snapshot = reserved.map_or(0, |(_, sequence)| sequence);
                match &mutation.record {
                    Some(record) => transaction.write_upserts_with_statement_snapshots(
                        &mutation.collection,
                        [(record.clone(), snapshot)],
                    )?,
                    None => transaction.delete_many_with_statement_snapshots(
                        &mutation.collection,
                        [(&mutation.record_id, snapshot)],
                    )?,
                }
            }
            let sequence = self.commit_transaction(&mut transaction)?;
            self.tx_log.write_durable(sequence)?;
            let receipt = transaction.mint_native_wal_apply_receipt()?;
            self.record_native_range_materialized(&pending, &receipt, authority)?;
            transaction.finalize_commit_admission()?;
            transaction.finalize_committed_broker_outbox();
            transaction.finalize_committed_memory_jobs()?;
            self.complete_native_range_command(&pending, certificate, &receipt, authority)
        })();
        if let Err(error) = result {
            self.finish_failed_native_local_attempt(&mut transaction);
            return Err(BicDbError::CommitOutcomeUnknown {
                sequence: transaction.committed_seq,
                transaction_id: Some(transaction.id().0),
                incarnation: self.incarnation.clone(),
                detail: format!("certified native command {} is irrevocable; local prefix completion requires recovery: {error}", intent.command_id()),
            });
        }
        self.auto_checkpoint_if_due();
        Ok(())
    }

    pub(super) fn read_native_certified_range_prefix(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<Option<NativeCertifiedRangePrefix>> {
        let prefix: Option<NativeCertifiedRangePrefix> =
            self.read_native_range_image_record(&prefix_key(base.range.id))?;
        self.validate_native_range_base_history(birth, base)?;
        let joint = self.read_native_joint_serving_domain(birth, base)?;
        if let Some(prefix) = &prefix {
            prefix.validate_with_native_joint_domain(birth, base, joint.as_ref())?;
            let entry = self.read_native_history_entry(
                prefix.range_id,
                prefix.history_generation,
                prefix.range_index,
            )?;
            let expected = if prefix.no_data.is_some() {
                self.validate_native_abort_prefix_record(birth, base, prefix)?;
                history::NativeLogicalHistoryOutcomeV1::Aborted
            } else {
                history::NativeLogicalHistoryOutcomeV1::Applied {
                    command_checksum_sha256: prefix.command_checksum_sha256.clone(),
                }
            };
            if entry.commitment_sha256 != prefix.logical_history_sha256 || entry.outcome != expected
            {
                return Err(invalid(
                    "native prefix logical-history head differs from actual immutable entry",
                ));
            }
        }
        if let Some(joint) = &joint {
            joint.require_materialized(base, prefix.as_ref())?;
        }
        Ok(prefix)
    }
    /// Read-only protected pending identity, never a materialized floor.
    /// Caller owns the actual native journal across this contract observation.
    pub(super) fn joint_pending_obligation_under_journal(
        &self,
        lineage: &SourceRangeImageLineage,
    ) -> Result<Option<(u64, String)>> {
        let Some(pending) = self.read_native_range_image_record::<PendingNativeRangeCommand>(
            &pending_key(lineage.base.range.id),
        )?
        else {
            return Ok(None);
        };
        pending.validate()?;
        if pending.birth_id != lineage.birth.birth_id
            || pending.range_id != lineage.base.range.id
            || pending.previous_index != lineage.range_index()
            || pending.previous_receipt_sha256 != lineage.native_receipt_sha256()
        {
            return Err(invalid(
                "joint continuity pending obligation differs from actual native predecessor",
            ));
        }
        Ok(Some((pending.range_index, pending.command_checksum_sha256)))
    }
    pub(super) fn native_range_pending_exists(&self, range: RangeId) -> Result<bool> {
        Ok(self
            .read_native_range_image_record::<PendingNativeRangeCommand>(&pending_key(range))?
            .is_some())
    }
    pub(super) fn observe_native_pending(
        &self,
        observation: &mut NativeRangePrefixObservation,
    ) -> Result<()> {
        let pending: Option<PendingNativeRangeCommand> =
            self.read_native_range_image_record(&pending_key(observation.range_id))?;
        if let Some(pending) = pending {
            pending.validate()?;
            observation.pending_command_index = Some(pending.range_index);
            observation.pending_command_id = Some(pending.command_id);
            observation.materialized_pending_index =
                pending.materialized.map(|_| pending.range_index);
            observation.diagnostic =
                Some("unfinished native command; source image unavailable".into());
        }
        Ok(())
    }
    pub(super) fn native_range_source_lineage_contract(
        &self,
        range_id: RangeId,
    ) -> Result<SourceRangeImageLineage> {
        #[cfg(test)]
        let construct_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-source-lineage-construct-native-point",
            );
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("no native birth"))?;
        let anchor = self.actual_native_range_anchor(&birth, range_id)?;
        let base = anchor.binding();
        self.validate_native_range_base_history(&birth, &base)?;
        let prefix = self.read_native_certified_range_prefix(&birth, &base)?;
        let serving =
            self.read_native_recovered_serving_configuration(&birth, &base, prefix.as_ref())?;
        let joint = self.read_native_joint_serving_domain(&birth, &base)?;
        if let Some(joint) = &joint {
            joint.require_materialized(&base, prefix.as_ref())?;
        }
        let lineage = SourceRangeImageLineage {
            birth,
            _anchor: anchor,
            base,
            prefix,
            serving,
            joint,
            current_database_incarnation: self.incarnation.clone(),
            current_schema_generation: self.schema_compatibility.generation(),
        };
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(construct_trace);
        #[cfg(test)]
        let validate_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-source-lineage-first-full-contract",
            );
        self.validate_source_range_image_lineage_contract(
            &lineage,
            range_id,
            lineage.range_index(),
        )?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(validate_trace);
        Ok(lineage)
    }
    pub(in crate::db::range_image_native) fn current_native_range_source_lineage_under_journal(
        &self,
        range_id: RangeId,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<SourceRangeImageLineage> {
        journal.require_database(self)?;
        let lineage = self.native_range_source_lineage_contract(range_id)?;
        if self.native_range_pending_exists(range_id)? {
            return Err(invalid(
                "unfinished native command; source range image unavailable",
            ));
        }
        self.check_recovery_state()?;
        journal.require_database(self)?;
        Ok(lineage)
    }
    pub(crate) fn current_native_range_source_lineage(
        &self,
        range_id: RangeId,
    ) -> Result<SourceRangeImageLineage> {
        let lineage = self.native_range_source_lineage_contract(range_id)?;
        #[cfg(test)]
        let second_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-source-lineage-second-contract-and-pending",
        );
        #[cfg(test)]
        if second_trace.is_some() {
            eprintln!("ACTUAL_REFRESH_LINEAGE_RANGE range={range_id}");
        }
        self.validate_source_range_image_lineage(&lineage, range_id, lineage.range_index())?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(second_trace);
        Ok(lineage)
    }
    /// The coordinator must persist this before the actual native row apply.
    /// A caller-supplied index, transaction ID or mutation permit cannot mint
    /// the command/configuration intent accepted by this adapter.
    pub(crate) fn prepare_native_range_command(
        &self,
        intent: &NativeRangeCommandIntent,
        local_transaction_id: u64,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<NativeRangePendingApply> {
        self.range_mutations
            .validate(intent.range().id, authority)?;
        let _journal = self
            .native_range_image_journal
            .try_lock()
            .ok_or_else(|| invalid("native journal busy; retry before decision"))?;
        let lineage = self.current_native_range_source_lineage(intent.range().id)?;
        intent.validate_native_base(&lineage)?;
        let (global_image_generation, range_image_generation) =
            self.native_range_image_generations(&lineage.birth, intent.range().id)?;
        let mut pending = PendingNativeRangeCommand {
            format_version: 1,
            birth_id: lineage.birth.birth_id,
            range_id: intent.range().id,
            range_index: intent.range_index(),
            previous_index: lineage.range_index(),
            previous_receipt_sha256: lineage.native_receipt_sha256().to_string(),
            command_id: intent.command_id().to_string(),
            command_checksum_sha256: intent.command_checksum_sha256().to_string(),
            mutations_sha256: intent.mutations_sha256().to_string(),
            scope_sha256: intent.scope_sha256().to_string(),
            expected_local_transaction_id: local_transaction_id,
            global_image_generation,
            range_image_generation,
            materialized: None,
            recovery_attempt: None,
            checksum_sha256: String::new(),
        };
        pending.checksum_sha256 = pending.checksum()?;
        let scope =
            self.reserve_native_command_scope(local_transaction_id, intent.mutations_sha256())?;
        self.write_native_range_image_record(&pending_key(pending.range_id), &pending)?;
        // Publish ordinary-row admission only AFTER actual pending durability.
        scope.arm()?;
        Ok(NativeRangePendingApply {
            database_incarnation: self.incarnation.clone(),
            intent: intent.clone(),
            pending,
            _scope: scope,
        })
    }
    fn reserve_native_command_scope(
        &self,
        local_transaction_id: u64,
        mutations_sha256: &str,
    ) -> Result<NativeCommandScopeLease> {
        let lease = Uuid::now_v7();
        {
            let mut registry = self
                .native_range_command_scopes
                .try_lock()
                .ok_or_else(|| invalid("native tx scope registry busy"))?;
            if registry.entries.len() >= MAX_NATIVE_COMMAND_SCOPES
                || registry.entries.contains_key(&local_transaction_id)
            {
                return Err(invalid(
                    "native tx scope bound or duplicate transaction reached",
                ));
            }
            registry.entries.insert(
                local_transaction_id,
                NativeCommandScope {
                    lease,
                    database_incarnation: self.incarnation.clone(),
                    mutations_sha256: mutations_sha256.to_string(),
                    armed: false,
                },
            );
        }
        Ok(NativeCommandScopeLease {
            registry: Arc::clone(&self.native_range_command_scopes),
            transaction_id: local_transaction_id,
            lease,
        })
    }

    fn require_native_pending(
        &self,
        pending: &NativeRangePendingApply,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<PendingNativeRangeCommand> {
        self.range_mutations
            .validate(pending.pending.range_id, authority)?;
        self.validate_native_wal_apply_receipt(receipt)?;
        if pending.database_incarnation != self.incarnation
            || receipt.transaction_id() != pending.pending.expected_local_transaction_id
            || receipt.mutations_sha256() != pending.intent.mutations_sha256()
            || self.native_range_image_installing(pending.pending.range_id)?
        {
            return Err(invalid(
                "pending native apply differs from actual DB/transaction/mutations",
            ));
        }
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("native birth disappeared"))?;
        let current: PendingNativeRangeCommand = self
            .read_native_range_image_record(&pending_key(pending.pending.range_id))?
            .ok_or_else(|| invalid("pending native command disappeared"))?;
        current.validate()?;
        let mut unsigned_current = current.clone();
        unsigned_current.materialized = None;
        unsigned_current.checksum_sha256 = unsigned_current.checksum()?;
        if unsigned_current != pending.pending
            || current.birth_id != birth.birth_id
            || self.native_range_image_generations(&birth, current.range_id)?
                != (
                    current.global_image_generation,
                    current.range_image_generation,
                )
        {
            return Err(invalid(
                "native pending identity/base generation changed; repair required",
            ));
        }
        Ok(current)
    }
    /// Materialized pending is diagnostic state, not a certified floor. The
    /// actual private WAL receipt is validated under this current DB/open.
    pub(crate) fn record_native_range_materialized(
        &self,
        pending: &NativeRangePendingApply,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let _journal = self
            .native_range_image_journal
            .try_lock()
            .ok_or_else(|| invalid("native journal busy after apply"))?;
        let mut record = self.require_native_pending(pending, receipt, authority)?;
        #[cfg(test)]
        if self.test_native_after_local_wal_rejected() {
            return Err(invalid("injected actual post-WAL/pre-native-prefix cut"));
        }
        let materialized = MaterializedNativeCommand {
            local_transaction_id: receipt.transaction_id(),
            local_wal_sequence: receipt.sequence(),
            actual_mutations_sha256: receipt.mutations_sha256().to_string(),
            database_incarnation: receipt.database_incarnation().to_string(),
        };
        if record
            .materialized
            .as_ref()
            .is_some_and(|previous| previous != &materialized)
        {
            return Err(invalid("pending native application receipt changed"));
        }
        record.materialized = Some(materialized);
        record.checksum_sha256 = record.checksum()?;
        self.write_native_range_image_record(&pending_key(record.range_id), &record)
    }
    /// Atomic prefix + pending deletion inside one real native page-WAL txn,
    /// AFTER separate durable data/index/WAL apply and actual certificate QQ.
    /// This does not pretend that those earlier transactions were one txn.
    pub(crate) fn complete_native_range_command(
        &self,
        pending: &NativeRangePendingApply,
        certificate: &NativeRangeCommitCertificate,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        self.complete_native_range_command_with_evidence(
            pending,
            NativeCompletionMaterializationEvidence::Quorum(certificate),
            receipt,
            authority,
        )
    }
    fn complete_native_range_command_with_evidence(
        &self,
        pending: &NativeRangePendingApply,
        proof: NativeCompletionMaterializationEvidence<'_>,
        receipt: &NativeWalApplyReceipt,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        proof.require_live()?;
        let intent = proof.intent();
        let _journal = self
            .native_range_image_journal
            .try_lock()
            .ok_or_else(|| invalid("native journal busy after certification"))?;
        let record = self.require_native_pending(pending, receipt, authority)?;
        if intent.scope_sha256() != pending.intent.scope_sha256()
            || intent.command_id() != pending.intent.command_id()
            || intent.command_checksum_sha256() != pending.intent.command_checksum_sha256()
            || intent.mutations_sha256() != pending.intent.mutations_sha256()
        {
            return Err(invalid(
                "native completion certificate does not match pending command",
            ));
        }
        let materialized = record
            .materialized
            .clone()
            .ok_or_else(|| invalid("native completion has no actual materialization receipt"))?;
        if materialized.local_transaction_id != receipt.transaction_id()
            || materialized.local_wal_sequence != receipt.sequence()
        {
            return Err(invalid(
                "native completion changed its actual WAL application",
            ));
        }
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("no native birth"))?;
        let base = self
            .actual_native_range_integrity_binding(&birth, record.range_id)?
            .ok_or_else(|| invalid("native completion base disappeared"))?;
        let old = self.read_native_certified_range_prefix(&birth, &base)?;
        let joint = self.read_native_joint_serving_domain(&birth, &base)?;
        let previous_index = old
            .as_ref()
            .map_or(base.range_index, |previous| previous.range_index);
        let previous_sha = old
            .as_ref()
            .map_or(base.checksum_sha256.as_str(), |previous| {
                previous.checksum_sha256.as_str()
            });
        let previous_logical_sha = old
            .as_ref()
            .map_or(base.logical_history_sha256.as_str(), |previous| {
                previous.logical_history_sha256.as_str()
            });
        let history_entry = history::NativeLogicalHistoryEntryV1::applied(
            record.range_id,
            record.range_index,
            previous_logical_sha,
            &record.command_checksum_sha256,
        )?;
        if previous_index != record.previous_index || previous_sha != record.previous_receipt_sha256
        {
            return Err(invalid(
                "native completion would skip or overwrite another certified prefix",
            ));
        }
        let mut prefix = NativeCertifiedRangePrefix {
            format_version: 2,
            birth_id: record.birth_id,
            base_receipt_sha256: base.checksum_sha256.clone(),
            range_id: record.range_id,
            range_index: record.range_index,
            previous_index: record.previous_index,
            previous_receipt_sha256: record.previous_receipt_sha256,
            command_id: record.command_id,
            command_checksum_sha256: record.command_checksum_sha256,
            mutations_sha256: record.mutations_sha256,
            scope_sha256: record.scope_sha256,
            materialized: Some(materialized),
            no_data: None,
            certified_voters: proof.certified_voters(),
            required_quorum: proof.required_quorum(),
            certificate_evidence: proof.evidence(),
            recovery_attempt: record.recovery_attempt,
            joint_decision_sha256: joint
                .as_ref()
                .filter(|domain| record.range_index > domain.range_index())
                .map(|domain| domain.decision_sha256().to_owned()),
            global_image_generation: record.global_image_generation,
            range_image_generation: record.range_image_generation,
            history_generation: base.history_generation,
            logical_genesis_sha256: base.logical_genesis_sha256.clone(),
            logical_history_sha256: history_entry.commitment_sha256.clone(),
            checksum_sha256: String::new(),
        };
        prefix.checksum_sha256 = prefix.checksum()?;
        prefix.validate_with_native_joint_domain(&birth, &base, joint.as_ref())?;
        let encoded = bounded_json(&prefix, MAX_NATIVE_RECEIPT_BYTES)?;
        self.in_paged_transaction(|paged, xid| {
            history::put_immutable_history_entry(
                paged,
                xid,
                prefix.history_generation,
                &history_entry,
            )?;
            paged
                .store()
                .put(xid, &native_key(&prefix_key(prefix.range_id)), &encoded)
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            paged
                .store()
                .delete(xid, &native_key(&pending_key(prefix.range_id)))
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("native completion has no page store"))?;
        proof.require_live()?;
        Ok(())
    }
}
