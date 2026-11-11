//! Purpose-specific observations of an interrupted image REFUSAL. This type
//! cannot be cast into an ordinary recovery fence, cert vote or native Ready.
use super::*;
use crate::db::{NativeRangePrefixObservation, ProtectedCurrentImagePreimageV1};
use crate::distribution_range_consensus::NativeCurrentImageProposalPointer;
use serde::{Deserialize, Serialize};
const MAX_INSTALLING_EVIDENCE_BYTES: usize = 8 * 1024 * 1024;
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CurrentImageInstallingRequest {
    pub scope: RecoveryFenceScope,
    pub proposal: NativeCurrentImageProposalPointer,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl CurrentImageInstallingRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.proposal.validate()?;
        if self.challenge.is_nil() {
            return Err(invalid("installing fence challenge is nil"));
        }
        RecoveryEpochFenceRequest {
            scope: self.scope.clone(),
            budget_ms: self.budget_ms,
        }
        .cancellation(parent)
    }
}
/// Wire/audit projection only. No public constructor promotes this DTO into an
/// authenticated observation. Dirty current physical state remains explicit.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CurrentImageInstallingEvidence {
    pub scope: RecoveryFenceScope,
    pub proposal: NativeCurrentImageProposalPointer,
    pub intent_sha256: String,
    pub source_acceptance: HistoricalCurrentImageAcceptanceV1,
    pub preimage: ProtectedCurrentImagePreimageV1,
    pub target_birth_id: Uuid,
    pub target_birth_sha256: String,
    pub target_namespace_ids: Vec<(String, Uuid)>,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub observed_metadata_context_id: Uuid,
    pub observed_metadata_term: u64,
    pub observed_metadata_read_index: u64,
    pub challenge: Uuid,
    pub progress: RangeWriteProgress,
    pub current_native: NativeRangePrefixObservation,
    pub current_native_point: Option<RecoveryNativeHistoryPointV1>,
    pub current_pending: Option<RecoveryNativePendingHistoryV1>,
    pub current_catalogue_sha256: String,
    pub current_irreversible_commands: BTreeMap<u64, String>,
}
impl CurrentImageInstallingEvidence {
    pub(crate) fn validate_shape(&self) -> Result<()> {
        self.scope.validate()?;
        self.proposal.validate()?;
        self.source_acceptance.validate()?;
        self.preimage.validate()?;
        if !matches!(
            self.scope.purpose,
            MetadataRecoveryRolePurpose::NormalRecovery
        ) || !self.scope.same_session(&self.source_acceptance.scope)
            || self.node_id != self.source_acceptance.target_id
            || self
                .source_acceptance
                .members
                .get(&self.node_id)
                .is_none_or(|m| m.incarnation != self.member_incarnation)
            || self.target_birth_id.is_nil()
            || !digest(&self.target_birth_sha256)
            || !digest(&self.intent_sha256)
            || !digest(&self.current_catalogue_sha256)
            || self.store_open_id.is_nil()
            || self.native_database_incarnation.is_empty()
            || self.native_database_incarnation.len() > 128
            || self.observed_metadata_context_id.is_nil()
            || self.challenge.is_nil()
            || self.observed_metadata_term < self.source_acceptance.metadata_term
            || self.observed_metadata_read_index < self.source_acceptance.metadata_read_index
            || self.progress.node_id != self.node_id
            || self.progress.range_id != self.scope.range_id
            || self.progress.current_epoch != self.scope.range_epoch
            || self.progress.compacted_through > self.progress.resolved_through
            || self.progress.resolved_through > self.progress.last_index
            || self.current_native.range_id != self.scope.range_id
            || self.target_namespace_ids.len() > 65_536
            || self
                .target_namespace_ids
                .windows(2)
                .any(|p| p[0].0 >= p[1].0)
            || self
                .target_namespace_ids
                .iter()
                .any(|(n, id)| n.is_empty() || id.is_nil())
            || self.current_irreversible_commands.len() > 65_536
            || self
                .current_irreversible_commands
                .iter()
                .any(|(i, s)| *i == 0 || !digest(s))
        {
            return Err(invalid("damaged installing refusal observation"));
        }
        if let Some(point) = &self.current_native_point {
            if !digest(&point.logical_genesis_sha256)
                || !digest(&point.logical_history_sha256)
                || !digest(&point.native_receipt_sha256)
            {
                return Err(invalid("installing current native point is malformed"));
            }
        }
        if let Some(pending) = &self.current_pending {
            if pending.index == 0 || !digest(&pending.command_checksum_sha256) {
                return Err(invalid("installing current pending is malformed"));
            }
        }
        self.irreversible_union()?;
        crate::distribution_range_image::bounded_json(self, MAX_INSTALLING_EVIDENCE_BYTES)?;
        Ok(())
    }
    pub(crate) fn irreversible_union(&self) -> Result<BTreeMap<u64, String>> {
        let mut union = self.preimage.irreversible_commands.clone();
        let mut add = |index: u64, checksum: &str| -> Result<()> {
            if index == 0
                || !digest(checksum)
                || union
                    .insert(index, checksum.to_string())
                    .is_some_and(|old| old != checksum)
            {
                return Err(invalid("conflicting installing irreversible obligations"));
            }
            if union.len() > 65_536 {
                return Err(invalid(
                    "installing obligation union exceeds complete bound",
                ));
            }
            Ok(())
        };
        for (index, checksum) in &self.current_irreversible_commands {
            add(*index, checksum)?;
        }
        if let (Some(index), Some(checksum)) = (
            self.preimage.pending_index,
            self.preimage.pending_command_sha256.as_deref(),
        ) {
            add(index, checksum)?;
        }
        if let Some(p) = &self.current_pending {
            add(p.index, &p.command_checksum_sha256)?;
        }
        Ok(union)
    }
}

#[derive(Debug)]
pub(crate) struct CurrentImageInstallingObservation {
    evidence: CurrentImageInstallingEvidence,
    peer: MetadataReadParticipant,
    deadline: Instant,
}
impl CurrentImageInstallingObservation {
    pub(super) fn from_local(
        evidence: CurrentImageInstallingEvidence,
        peer: MetadataReadParticipant,
        request: &CurrentImageInstallingRequest,
        deadline: Instant,
    ) -> Result<Self> {
        evidence.validate_shape()?;
        if Instant::now() >= deadline
            || !request.scope.same_session(&evidence.scope)
            || request.proposal != evidence.proposal
            || request.challenge != evidence.challenge
            || peer.node_id() != &evidence.node_id
            || peer.member_incarnation() != evidence.member_incarnation
        {
            return Err(invalid("late/mismatched actual installing observation"));
        }
        Ok(Self {
            evidence,
            peer,
            deadline,
        })
    }
    pub(crate) fn evidence(&self) -> &CurrentImageInstallingEvidence {
        &self.evidence
    }
    pub(crate) fn authenticated_peer(&self) -> &MetadataReadParticipant {
        &self.peer
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(invalid("installing fence original deadline expired"));
        }
        Ok(())
    }
}
