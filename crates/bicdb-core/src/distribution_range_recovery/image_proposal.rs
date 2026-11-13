//! Historical source acceptance retained BEFORE target BEGIN. The DTO is an
//! audit only: only the actual verified-source-history factory mints the
//! opaque wrapper. Loading the audit never creates a fresh quorum or source.
use super::*;
use crate::db::{LocalNativeRecoveryReady, NativeRangePrefixObservation};
use crate::BicDb;
use serde::{Deserialize, Serialize};
use sha2::Digest;

const VERSION: u32 = 1;
const MAX_AUDIT_BYTES: usize = 8 * 1024 * 1024;
fn digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
fn semantic(mut range: RangeDescriptor) -> RangeDescriptor {
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|r| r.id);
    range
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum HistoricalCurrentImageVoterKind {
    Ordinary,
    InstallingReplacement {
        proposal: crate::distribution_range_consensus::NativeCurrentImageProposalPointer,
        intent_sha256: String,
        original_acceptance_sha256: String,
    },
    Installing {
        proposal: crate::distribution_range_consensus::NativeCurrentImageProposalPointer,
        intent_sha256: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalCurrentImageVoterAudit {
    pub kind: HistoricalCurrentImageVoterKind,
    pub additional_native_points: Vec<RecoveryNativeHistoryPointV1>,
    pub member_incarnation: u64,
    pub peer_certificate_sha256: Option<String>,
    pub peer_plaintext_loopback: bool,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub progress: RangeWriteProgress,
    pub native: NativeRangePrefixObservation,
    pub image_history: RecoveryImageHistoryObligationsV1,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalCurrentImageAcceptanceV1 {
    pub format_version: u32,
    pub scope: RecoveryFenceScope,
    pub range: RangeDescriptor,
    pub members: BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    pub source_id: ClusterNodeId,
    pub source_member_incarnation: u64,
    pub source_birth_id: Uuid,
    pub source_birth_sha256: String,
    /// Original audit provenance; a cold observation uses the ACTUAL new open.
    pub source_database_incarnation: String,
    pub target_id: ClusterNodeId,
    pub source_range_index: u64,
    pub source_native_receipt_sha256: String,
    pub source_native_visibility_db_sequence: u64,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub schema_sha256: String,
    pub restore_activation: Option<MetadataRestoreActivation>,
    pub metadata_context_id: Uuid,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub voters: BTreeMap<ClusterNodeId, HistoricalCurrentImageVoterAudit>,
}
impl HistoricalCurrentImageAcceptanceV1 {
    /// Integrity/domain checks on a protected historical record, NOT a new
    /// authenticated quorum or a fresh role reconstructed from persisted JSON.
    pub(crate) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self.format_version != VERSION
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.range != semantic(self.range.clone())
            || self.scope.range_id != self.range.id
            || self.scope.range_epoch != self.range.epoch
            || self.members.len() > 64
            || self.members.is_empty()
            || self.voters.len() < self.members.len() / 2 + 1
            || self.voters.len() > self.members.len()
            || self.source_birth_id.is_nil()
            || self.source_database_incarnation.is_empty()
            || self.source_database_incarnation.len() > 128
            || self.source_id == self.target_id
            || !self.members.contains_key(&self.target_id)
            || self
                .members
                .get(&self.source_id)
                .is_none_or(|m| m.incarnation != self.source_member_incarnation)
            || self.metadata_context_id.is_nil()
            || self.metadata_term < self.scope.metadata_term
            || self.metadata_read_index < self.scope.metadata_read_index
            || [
                &self.source_birth_sha256,
                &self.source_native_receipt_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
                &self.schema_sha256,
            ]
            .iter()
            .any(|s| !digest(s))
            || (self.source_range_index == 0
                && self.logical_genesis_sha256 != self.logical_history_sha256)
        {
            return Err(invalid(
                "damaged protected current-image historical source acceptance",
            ));
        }
        let actual_voters = self
            .range
            .replicas
            .iter()
            .filter(|r| r.role == RangeReplicaRole::Voter)
            .map(|r| r.node_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if actual_voters != self.members.keys().cloned().collect() {
            return Err(invalid("historical source acceptance voter domain differs"));
        }
        let mut known_points = BTreeMap::<u64, String>::new();
        let mut known_commands = BTreeMap::<u64, String>::new();
        for (id, vote) in &self.voters {
            let member = self
                .members
                .get(id)
                .ok_or_else(|| invalid("historical source audit has foreign voter"))?;
            match &vote.kind {
                HistoricalCurrentImageVoterKind::Ordinary
                    if !vote.additional_native_points.is_empty() =>
                {
                    return Err(invalid(
                        "ordinary audit cannot claim Installing preimage points",
                    ))
                }
                HistoricalCurrentImageVoterKind::InstallingReplacement {
                    proposal,
                    intent_sha256,
                    original_acceptance_sha256,
                } => {
                    proposal.validate()?;
                    if id != &self.target_id
                        || !digest(intent_sha256)
                        || !digest(original_acceptance_sha256)
                        || vote.progress.current_epoch > self.range.epoch
                    {
                        return Err(invalid("invalid replacement Installing historical audit"));
                    }
                }
                HistoricalCurrentImageVoterKind::Installing {
                    proposal,
                    intent_sha256,
                } => {
                    proposal.validate()?;
                    if id != &self.target_id || !digest(intent_sha256) {
                        return Err(invalid("invalid historical Installing audit target"));
                    }
                }
                _ => {}
            }
            for point in &vote.additional_native_points {
                if point.range_index > self.source_range_index
                    || point.logical_genesis_sha256 != self.logical_genesis_sha256
                    || !digest(&point.logical_history_sha256)
                    || !digest(&point.native_receipt_sha256)
                    || known_points
                        .insert(point.range_index, point.logical_history_sha256.clone())
                        .is_some_and(|old| old != point.logical_history_sha256)
                {
                    return Err(invalid(
                        "historical Installing audit has contradictory preimage point",
                    ));
                }
            }
            let pin = match (
                &member.active_certificate_sha256,
                &member.pending_certificate_sha256,
            ) {
                (None, None) => {
                    vote.peer_plaintext_loopback && vote.peer_certificate_sha256.is_none()
                }
                (a, p) => {
                    !vote.peer_plaintext_loopback
                        && vote
                            .peer_certificate_sha256
                            .as_deref()
                            .is_some_and(|s| a.as_deref() == Some(s) || p.as_deref() == Some(s))
                }
            };
            if !pin
                || member.incarnation != vote.member_incarnation
                || vote.store_open_id.is_nil()
                || vote.native_database_incarnation.is_empty()
                || vote.native_database_incarnation.len() > 128
                || vote.progress.node_id != *id
                || vote.progress.range_id != self.range.id
                || (vote.progress.current_epoch != self.range.epoch
                    && !matches!(
                        vote.kind,
                        HistoricalCurrentImageVoterKind::InstallingReplacement { .. }
                    ))
                || vote.progress.last_index > self.source_range_index
                || vote.native.range_id != self.range.id
            {
                return Err(invalid("damaged historical source audit identity/floor"));
            }
            vote.image_history
                .require_shape(&self.scope, &vote.progress)?;
            if vote.image_history.compacted_through
                > vote
                    .image_history
                    .native_point
                    .as_ref()
                    .map_or(0, |p| p.range_index)
            {
                return Err(invalid(
                    "historical source acceptance lost compacted native binding",
                ));
            }
            if let Some(point) = &vote.image_history.native_point {
                if point.range_index > self.source_range_index
                    || point.logical_genesis_sha256 != self.logical_genesis_sha256
                {
                    return Err(invalid(
                        "historical source acceptance native domain mismatch",
                    ));
                }
                if known_points
                    .insert(point.range_index, point.logical_history_sha256.clone())
                    .is_some_and(|old| old != point.logical_history_sha256)
                {
                    return Err(invalid(
                        "historical source audit contains conflicting native points",
                    ));
                }
            }
            for (index, checksum) in &vote.image_history.irreversible_command_sha256 {
                if *index > self.source_range_index
                    || known_commands
                        .insert(*index, checksum.clone())
                        .is_some_and(|old| old != *checksum)
                {
                    return Err(invalid(
                        "historical source audit contains omitted/conflicting command",
                    ));
                }
            }
            if vote
                .native
                .native_base_index
                .into_iter()
                .chain(vote.native.native_applied_index)
                .chain(vote.native.pending_command_index)
                .chain(vote.native.materialized_pending_index)
                .any(|r| r > self.source_range_index)
            {
                return Err(invalid("historical source audit omits native obligations"));
            }
        }
        crate::distribution_range_image::bounded_json(self, MAX_AUDIT_BYTES)?;
        Ok(())
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        self.validate()?;
        use sha2::Digest;
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(self, MAX_AUDIT_BYTES)?,
        )))
    }
}

/// Minted only from actual fully verified native source history and its live
/// fenced quorum. Persisting its audit is historical acceptance, not a lease.
#[derive(Debug)]
pub(crate) struct HistoricalCurrentImageSourceAcceptance {
    record: HistoricalCurrentImageAcceptanceV1,
}
impl HistoricalCurrentImageSourceAcceptance {
    pub(crate) fn audit(&self) -> &HistoricalCurrentImageAcceptanceV1 {
        &self.record
    }
}
impl VerifiedRecoverySourceHistory {
    pub(crate) fn historical_current_image_acceptance(
        &self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<HistoricalCurrentImageSourceAcceptance> {
        self.require_current(source, db, store)?;
        let authority = self.authority();
        let role = authority.role();
        let native = source.native_admission();
        native.require_current(db)?;
        let record = HistoricalCurrentImageAcceptanceV1 {
            format_version: VERSION,
            scope: authority.scope().clone(),
            range: semantic(role.current_range().clone()),
            members: authority.member_credentials().clone(),
            source_id: source.role().local_node_id().clone(),
            source_member_incarnation: source.role().local_member_incarnation(),
            source_birth_id: native.birth_id(),
            source_birth_sha256: native.birth_sha256().to_string(),
            source_database_incarnation: native.database_incarnation().to_string(),
            target_id: self.target().clone(),
            source_range_index: source.range_index(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_native_visibility_db_sequence: source.visibility().get(),
            logical_genesis_sha256: source.logical_genesis_sha256().to_string(),
            logical_history_sha256: source.logical_history_sha256().to_string(),
            schema_sha256: role
                .expected_schema_sha256()
                .ok_or_else(|| invalid("historical source acceptance lacks schema"))?
                .to_string(),
            restore_activation: role.restore_activation().cloned(),
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            voters: authority
                .observations()
                .iter()
                .map(|(id, observation)| {
                    let peer = observation.authenticated_peer();
                    (
                        id.clone(),
                        HistoricalCurrentImageVoterAudit {
                            kind: HistoricalCurrentImageVoterKind::Ordinary,
                            additional_native_points: vec![],
                            member_incarnation: observation.member_incarnation(),
                            peer_certificate_sha256: peer.certificate_sha256().map(str::to_string),
                            peer_plaintext_loopback: peer.plaintext_loopback(),
                            store_open_id: observation.store_open_id(),
                            native_database_incarnation: observation
                                .native_database_incarnation()
                                .to_string(),
                            progress: observation.progress().clone(),
                            native: observation.native().clone(),
                            image_history: observation.image_history().clone(),
                        },
                    )
                })
                .collect(),
        };
        record.validate()?;
        self.require_current(source, db, store)?;
        Ok(HistoricalCurrentImageSourceAcceptance { record })
    }
}

impl VerifiedCurrentImageResumeHistory {
    pub(crate) fn historical_current_image_acceptance(
        &self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<HistoricalCurrentImageSourceAcceptance> {
        self.require_current(source, db, store)?;
        let authority = self.authority();
        let role = authority.role();
        let native = source.native_admission();
        native.require_current(db)?;
        let record = HistoricalCurrentImageAcceptanceV1 {
            format_version: VERSION,
            scope: authority.scope().clone(),
            range: semantic(role.current_range().clone()),
            members: authority.member_credentials().clone(),
            source_id: source.role().local_node_id().clone(),
            source_member_incarnation: source.role().local_member_incarnation(),
            source_birth_id: native.birth_id(),
            source_birth_sha256: native.birth_sha256().to_string(),
            source_database_incarnation: native.database_incarnation().to_string(),
            target_id: authority.target().clone(),
            source_range_index: source.range_index(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_native_visibility_db_sequence: source.visibility().get(),
            logical_genesis_sha256: source.logical_genesis_sha256().to_string(),
            logical_history_sha256: source.logical_history_sha256().to_string(),
            schema_sha256: role
                .expected_schema_sha256()
                .ok_or_else(|| invalid("historical source acceptance lacks schema"))?
                .to_string(),
            restore_activation: role.restore_activation().cloned(),
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            voters: authority
                .votes()
                .iter()
                .map(|(id, vote)| {
                    let projected = match vote {
                        CurrentImageResumeVote::Ordinary(observation) => {
                            let peer = observation.authenticated_peer();
                            HistoricalCurrentImageVoterAudit {
                                kind: HistoricalCurrentImageVoterKind::Ordinary,
                                additional_native_points: vec![],
                                member_incarnation: observation.member_incarnation(),
                                peer_certificate_sha256: peer
                                    .certificate_sha256()
                                    .map(str::to_string),
                                peer_plaintext_loopback: peer.plaintext_loopback(),
                                store_open_id: observation.store_open_id(),
                                native_database_incarnation: observation
                                    .native_database_incarnation()
                                    .to_string(),
                                progress: observation.progress().clone(),
                                native: observation.native().clone(),
                                image_history: observation.image_history().clone(),
                            }
                        }
                        CurrentImageResumeVote::Installing(observation) => {
                            let evidence = observation.evidence();
                            let peer = observation.authenticated_peer();
                            let mut points = evidence
                                .preimage
                                .native_point
                                .iter()
                                .chain(evidence.current_native_point.iter())
                                .cloned()
                                .collect::<Vec<_>>();
                            points.sort_by_key(|p| p.range_index);
                            let native_point = points.pop();
                            let pending = evidence.current_pending.clone().or_else(|| {
                                match (
                                    evidence.preimage.pending_index,
                                    evidence.preimage.pending_command_sha256.clone(),
                                ) {
                                    (Some(index), Some(command_checksum_sha256)) => {
                                        Some(RecoveryNativePendingHistoryV1 {
                                            index,
                                            command_checksum_sha256,
                                            materialized: evidence
                                                .preimage
                                                .materialized_pending_index
                                                == Some(index),
                                        })
                                    }
                                    _ => None,
                                }
                            });
                            let sparse = (!evidence.current_sparse_commands.is_empty())
                                .then_some(evidence.current_sparse_commands.clone());
                            HistoricalCurrentImageVoterAudit {
                                kind: HistoricalCurrentImageVoterKind::Installing {
                                    proposal: evidence.proposal.clone(),
                                    intent_sha256: evidence.intent_sha256.clone(),
                                },
                                additional_native_points: points,
                                member_incarnation: evidence.member_incarnation,
                                peer_certificate_sha256: peer
                                    .certificate_sha256()
                                    .map(str::to_string),
                                peer_plaintext_loopback: peer.plaintext_loopback(),
                                store_open_id: evidence.store_open_id,
                                native_database_incarnation: evidence
                                    .native_database_incarnation
                                    .clone(),
                                progress: evidence.progress.clone(),
                                native: evidence.current_native.clone(),
                                image_history: RecoveryImageHistoryObligationsV1 {
                                    format_version: if sparse.is_some() { 2 } else { 1 },
                                    node_id: id.clone(),
                                    range_id: evidence.scope.range_id,
                                    last_index: evidence.progress.last_index,
                                    compacted_through: evidence.progress.compacted_through,
                                    native_point,
                                    pending_native: pending,
                                    irreversible_command_sha256: evidence.irreversible_union()?,
                                    sparse_committed_command_sha256: sparse,
                                },
                            }
                        }
                    };
                    Ok((id.clone(), projected))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        };

        record.validate()?;
        self.require_current(source, db, store)?;
        Ok(HistoricalCurrentImageSourceAcceptance { record })
    }
}

impl VerifiedCurrentImageReplacementHistory {
    pub(crate) fn historical_current_image_acceptance(
        &self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<HistoricalCurrentImageSourceAcceptance> {
        self.require_current(source, db, store)?;
        let authority = self.authority();
        let role = authority.role();
        let native = source.native_admission();
        native.require_current(db)?;
        let record = HistoricalCurrentImageAcceptanceV1 {
            format_version: VERSION,
            scope: authority.scope().clone(),
            range: semantic(role.current_range().clone()),
            members: authority.member_credentials().clone(),
            source_id: source.role().local_node_id().clone(),
            source_member_incarnation: source.role().local_member_incarnation(),
            source_birth_id: native.birth_id(),
            source_birth_sha256: native.birth_sha256().to_string(),
            source_database_incarnation: native.database_incarnation().to_string(),
            target_id: authority.target().clone(),
            source_range_index: source.range_index(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_native_visibility_db_sequence: source.visibility().get(),
            logical_genesis_sha256: source.logical_genesis_sha256().to_string(),
            logical_history_sha256: source.logical_history_sha256().to_string(),
            schema_sha256: role
                .expected_schema_sha256()
                .ok_or_else(|| invalid("historical source acceptance lacks schema"))?
                .to_string(),
            restore_activation: role.restore_activation().cloned(),
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            voters: authority
                .votes()
                .iter()
                .map(|(id, vote)| {
                    let projected = match vote {
                        InstallingCommittedVote::Ordinary(observation) => {
                            let peer = observation.authenticated_peer();
                            HistoricalCurrentImageVoterAudit {
                                kind: HistoricalCurrentImageVoterKind::Ordinary,
                                additional_native_points: vec![],
                                member_incarnation: observation.member_incarnation(),
                                peer_certificate_sha256: peer
                                    .certificate_sha256()
                                    .map(str::to_string),
                                peer_plaintext_loopback: peer.plaintext_loopback(),
                                store_open_id: observation.store_open_id(),
                                native_database_incarnation: observation
                                    .native_database_incarnation()
                                    .to_string(),
                                progress: observation.progress().clone(),
                                native: observation.native().clone(),
                                image_history: observation.image_history().clone(),
                            }
                        }
                        InstallingCommittedVote::Installing(observation) => {
                            let evidence = observation.evidence();
                            let peer = observation.authenticated_peer();
                            let mut points = evidence
                                .preimage
                                .native_point
                                .iter()
                                .chain(evidence.native_point.iter())
                                .cloned()
                                .collect::<Vec<_>>();
                            points.sort_by_key(|p| p.range_index);
                            let native_point = points.pop();
                            let pending = evidence.pending.clone().or_else(|| {
                                match (
                                    evidence.preimage.pending_index,
                                    evidence.preimage.pending_command_sha256.clone(),
                                ) {
                                    (Some(index), Some(command_checksum_sha256)) => {
                                        Some(RecoveryNativePendingHistoryV1 {
                                            index,
                                            command_checksum_sha256,
                                            materialized: evidence
                                                .preimage
                                                .materialized_pending_index
                                                == Some(index),
                                        })
                                    }
                                    _ => None,
                                }
                            });
                            // Every retained unordered obligation is explicit;
                            // no old raw count stands in for a native prefix.
                            let complete = evidence.irreversible_union()?;
                            let sparse = Some(
                                complete
                                    .iter()
                                    .filter(|(i, _)| **i > evidence.progress.last_index)
                                    .map(|(i, h)| (*i, h.clone()))
                                    .collect::<BTreeMap<_, _>>(),
                            );
                            HistoricalCurrentImageVoterAudit {
                                kind: HistoricalCurrentImageVoterKind::InstallingReplacement {
                                    proposal: evidence.proposal.clone(),
                                    intent_sha256: evidence.intent_sha256.clone(),
                                    original_acceptance_sha256: hex::encode(sha2::Sha256::digest(
                                        crate::distribution_range_image::bounded_json(
                                            &evidence.historical_source,
                                            MAX_AUDIT_BYTES,
                                        )?,
                                    )),
                                },
                                additional_native_points: points,
                                member_incarnation: evidence.member_incarnation,
                                peer_certificate_sha256: peer
                                    .certificate_sha256()
                                    .map(str::to_string),
                                peer_plaintext_loopback: peer.plaintext_loopback(),
                                store_open_id: evidence.store_open_id,
                                native_database_incarnation: evidence
                                    .native_database_incarnation
                                    .clone(),
                                progress: evidence.progress.clone(),
                                native: evidence.native.clone(),
                                image_history: RecoveryImageHistoryObligationsV1 {
                                    format_version: if sparse.is_some() { 2 } else { 1 },
                                    node_id: id.clone(),
                                    range_id: evidence.current_scope.range_id,
                                    last_index: evidence.progress.last_index,
                                    compacted_through: evidence.progress.compacted_through,
                                    native_point,
                                    pending_native: pending,
                                    irreversible_command_sha256: evidence.irreversible_union()?,
                                    sparse_committed_command_sha256: sparse,
                                },
                            }
                        }
                    };
                    Ok((id.clone(), projected))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        };

        record.validate()?;
        self.require_current(source, db, store)?;
        Ok(HistoricalCurrentImageSourceAcceptance { record })
    }
}
