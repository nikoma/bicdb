//! Fresh role witnesses and current-data-quorum recovery authority.
//!
//! Metadata evidence is not a native floor. Wire DTOs, local CQ/Applied labels
//! and progress counters cannot construct any of the opaque admissions here.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::distribution::{
    ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RangeRelocation, RangeReplicaRole,
    SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use crate::distribution_consensus::{
    MetadataReadParticipant, MetadataRecoveryRoleObservation, MetadataRecoveryRolePurpose,
    MetadataRestoreActivation,
};
use crate::distribution_range_consensus::RangeWriteCommand;
use crate::distribution_range_consensus::{RangeWriteProgress, RecoveryFenceScope};
use crate::error::{BicDbError, Result};

fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(format!("range recovery: {message}"))
}

mod image_replacement_history;
pub(crate) use image_replacement_history::{
    CurrentImageReplacementHistoryRound, VerifiedCurrentImageReplacementHistory,
};
mod image_replacement;
pub(crate) use image_replacement::{
    FreshInstallingCommittedAuthority, InstallingCommittedApplyAdmission,
    InstallingCommittedCertificationEvidence, InstallingCommittedCertificationObservation,
    InstallingCommittedDecision, InstallingCommittedDecisionRound,
    InstallingCommittedFenceEvidence, InstallingCommittedFenceObservation,
    InstallingCommittedQuorumRound, InstallingCommittedRequest, InstallingCommittedVote,
    InstallingCurrentCertificationRound, InstallingDecisionVoteAudit,
};
mod image_compatibility;
mod image_history;
mod image_proposal;
mod image_resume_history;
mod image_resume_quorum;
mod installing_fence;
pub(crate) use image_proposal::{
    HistoricalCurrentImageAcceptanceV1, HistoricalCurrentImageSourceAcceptance,
};
pub(crate) use image_resume_history::{
    CurrentImageResumeHistoryRound, VerifiedCurrentImageResumeHistory,
};
pub(crate) use image_resume_quorum::{
    CurrentImageResumeQuorumRound, CurrentImageResumeVote, FreshCurrentImageResumeAuthority,
};
pub(crate) use installing_fence::{
    CurrentImageInstallingEvidence, CurrentImageInstallingObservation,
    CurrentImageInstallingRequest,
};
#[cfg(test)]
mod recovery_order_tests;
pub(crate) use image_compatibility::{RecoverySourceHistoryRound, VerifiedRecoverySourceHistory};
#[cfg(test)]
pub(crate) use image_history::test_only_structural_fence_dtos;
pub(crate) use image_history::{
    fenced_obligation_limit_error, RecoveryImageHistoryObligationsV1, RecoveryNativeHistoryPointV1,
    RecoveryNativePendingHistoryV1, MAX_FENCED_IRREVERSIBLE_COMMANDS,
};
mod native_completion_provenance;
pub(crate) use native_completion_provenance::{
    NativeCompletionDecisionProvenance, NativeCompletionVoterProvenance,
};
mod native_completion_quorum;
pub(crate) use native_completion_quorum::{
    ActivatedNativeCompletionDecision, NativeCompletionQuorum, NativeCompletionQuorumRound,
    NativeCompletionSelection,
};
mod native_completion;
pub(crate) use native_completion::{
    NativeCompletionObservation, NativeCompletionReply, NativeCompletionRequest,
    NativeCompletionStage,
};
mod native_ready;
pub(crate) use native_ready::{
    ClosedNativeReadyQuorum, ClosedNativeReadyQuorumRound, FrozenNativeReadyStage,
    NativeReadyObservation, NativeReadyPhysicalDomain, NativeReadyReply, NativeReadyRequest,
};
mod runtime;
pub(crate) use runtime::{RangeRecoveryRuntime, RecoveryImageInstallFreeze};
mod quorum;
pub(crate) use quorum::{FreshRangeRecoveryAuthority, RecoveryFenceQuorumRound};
pub(crate) mod native_abort;
pub(crate) use native_abort::{
    NativeAbortCandidateObservation, NativeAbortPromiseObservation, NativeAbortPromiseRound,
    NativeAbortQuorum, NativeAbortRequest,
};
mod historical;
pub(crate) use historical::{
    CurrentHistoricalCertificationRound, HistoricalCommandDecision, HistoricalCommandDecisionRound,
    RecoveryApplyAdmission,
};

/// A wire request is a claim to check, never recovery or native authority.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryEpochFenceRequest {
    pub scope: RecoveryFenceScope,
    pub budget_ms: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryFencedProbeRequest {
    pub scope: RecoveryFenceScope,
    /// None requests status only. An absent compacted entry remains unknown.
    pub index: Option<u64>,
    pub budget_ms: u64,
}
/// Command body only; every recipient independently gathers its own fresh
/// role, fenced quorum and current certification quorum before side effects.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryHistoricalCommandRequest {
    pub scope: RecoveryFenceScope,
    pub command: RangeWriteCommand,
    pub budget_ms: u64,
}
impl RecoveryHistoricalCommandRequest {
    pub(crate) fn probe(&self) -> Result<RecoveryFencedProbeRequest> {
        crate::distribution_range_consensus::validate_native_recovery_command_size(&self.command)?;
        self.command.validate()?;
        if self.command.cluster_id != self.scope.cluster_id
            || self.command.range_id != self.scope.range_id
            || self.command.range_epoch > self.scope.range_epoch
        {
            return Err(invalid(
                "historical request command is outside current recovery scope",
            ));
        }
        Ok(RecoveryFencedProbeRequest {
            scope: self.scope.clone(),
            index: Some(self.command.index),
            budget_ms: self.budget_ms,
        })
    }
    pub(crate) fn cancellation(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<crate::CancellationToken> {
        self.probe()?.cancellation(parent)
    }
}
impl RecoveryFencedProbeRequest {
    pub(crate) fn cancellation(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<crate::CancellationToken> {
        if self.index == Some(0) {
            return Err(invalid("recovery probe index must be nonzero"));
        }
        RecoveryEpochFenceRequest {
            scope: self.scope.clone(),
            budget_ms: self.budget_ms,
        }
        .cancellation(parent)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryFencedProbeReply {
    pub fence: RecoveryEpochFenceReply,
    pub index: Option<u64>,
    pub entry: Option<crate::distribution_range_consensus::RangeWriteLogEntry>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub sparse_decision:
        Option<crate::distribution_range_consensus::SparseHistoricalDecisionProjection>,
    pub current_certification:
        Option<crate::distribution_range_consensus::HistoricalRangeCertification>,
}

fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}

/// A fresh, authenticated read of an actual persisted fence and catalogue.
/// It authorizes neither native Apply nor any metadata/data mutation.
#[derive(Debug)]
pub(crate) struct RecoveryHistoryObservation {
    fence: std::sync::Arc<RecoveryFenceObservation>,
    index: Option<u64>,
    entry: Option<crate::distribution_range_consensus::RangeWriteLogEntry>,
    sparse_decision:
        Option<crate::distribution_range_consensus::SparseHistoricalDecisionProjection>,
    current_certification:
        Option<crate::distribution_range_consensus::HistoricalRangeCertification>,
}
impl RecoveryHistoryObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        verified: crate::distribution_transport::AuthenticatedRecoveryProbeReply,
        request: &RecoveryFencedProbeRequest,
        deadline: Instant,
    ) -> Result<Self> {
        let (reply, peer) = verified.into_parts();
        Self::from_local(reply, peer, request, deadline)
    }
    fn from_local(
        reply: RecoveryFencedProbeReply,
        peer: MetadataReadParticipant,
        request: &RecoveryFencedProbeRequest,
        deadline: Instant,
    ) -> Result<Self> {
        if reply.index != request.index
            || (reply.index.is_none() && (reply.entry.is_some() || reply.sparse_decision.is_some()))
            || (reply.entry.is_some() && reply.sparse_decision.is_some())
        {
            return Err(invalid("recovery observation index correlation failed"));
        }
        if let Some(entry) = &reply.entry {
            entry.command.validate()?;
            if Some(entry.command.index) != request.index
                || entry.command.range_id != request.scope.range_id
                || entry.command.cluster_id != request.scope.cluster_id
                || entry.command.range_epoch > request.scope.range_epoch
            {
                return Err(invalid("recovery history command scope mismatch"));
            }
        }
        if let Some(sparse) = &reply.sparse_decision {
            sparse.validate_shape(&request.scope, request.index.unwrap())?;
            if sparse.local_node_id() != peer.node_id()
                || sparse.local_member_incarnation() != peer.member_incarnation()
                || sparse.native_database_incarnation() != reply.fence.native_database_incarnation
            {
                return Err(invalid(
                    "sparse history is not this actual peer/native incarnation",
                ));
            }
        }
        let fence =
            RecoveryFenceObservation::from_local(reply.fence, peer, &request.scope, deadline)?;
        if let Some(marker) = &reply.current_certification {
            if Some(marker.index) != request.index
                || marker.recovery_nonce != request.scope.recovery_nonce
                || marker.configuration_sha256 != request.scope.configuration_sha256
                || marker.range_epoch != request.scope.range_epoch
                || marker.command_sha256.len() != 64
                || !marker.command_sha256.bytes().all(|b| b.is_ascii_hexdigit())
                || reply
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.command.checksum_sha256 != marker.command_sha256)
                || reply
                    .sparse_decision
                    .as_ref()
                    .is_some_and(|sparse| sparse.command().checksum_sha256 != marker.command_sha256)
            {
                return Err(invalid(
                    "current historical certify-only marker scope mismatch",
                ));
            }
        }
        Ok(Self {
            fence: std::sync::Arc::new(fence),
            index: reply.index,
            entry: reply.entry,
            sparse_decision: reply.sparse_decision,
            current_certification: reply.current_certification,
        })
    }
    pub(crate) fn fence(&self) -> &RecoveryFenceObservation {
        &self.fence
    }
    pub(crate) fn fence_observation(&self) -> std::sync::Arc<RecoveryFenceObservation> {
        std::sync::Arc::clone(&self.fence)
    }
    pub(crate) fn index(&self) -> Option<u64> {
        self.index
    }
    pub(crate) fn entry(&self) -> Option<&crate::distribution_range_consensus::RangeWriteLogEntry> {
        self.entry.as_ref()
    }
    pub(crate) fn sparse_decision(
        &self,
    ) -> Option<&crate::distribution_range_consensus::SparseHistoricalDecisionProjection> {
        self.sparse_decision.as_ref()
    }
    pub(crate) fn current_certification(
        &self,
    ) -> Option<&crate::distribution_range_consensus::HistoricalRangeCertification> {
        self.current_certification.as_ref()
    }
    pub(super) fn into_reply(self) -> RecoveryFencedProbeReply {
        RecoveryFencedProbeReply {
            fence: self.fence.reply.clone(),
            index: self.index,
            entry: self.entry,
            sparse_decision: self.sparse_decision,
            current_certification: self.current_certification,
        }
    }
}
impl RecoveryEpochFenceRequest {
    pub(crate) fn cancellation(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<crate::CancellationToken> {
        if self.budget_ms == 0 || self.budget_ms > 5_000 {
            return Err(invalid("invalid recovery fence budget"));
        }
        let child =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        child.check()?;
        Ok(child)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryEpochFenceReply {
    pub phase: RecoveryFenceObservationPhase,
    pub image_history: RecoveryImageHistoryObligationsV1,
    pub scope: RecoveryFenceScope,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub birth_initialization_complete: bool,
    pub progress: RangeWriteProgress,
    pub native: crate::db::NativeRangePrefixObservation,
}
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) enum RecoveryFenceObservationPhase {
    ActiveRecovery,
    NativePrepared,
    NativeCompletedFrozen,
    NativeActivatedAnchor,
    CompletedEpochAuthority,
}

/// An actual local drain/persist result or a transport-authenticated response.
/// No Deserialize/Clone and no constructor accepting an unverified wire DTO.
#[derive(Debug)]
pub(crate) struct RecoveryFenceObservation {
    reply: RecoveryEpochFenceReply,
    authenticated_peer: MetadataReadParticipant,
    deadline: Instant,
}
impl RecoveryFenceObservation {
    pub(crate) fn is_active_recovery(&self) -> bool {
        self.reply.phase == RecoveryFenceObservationPhase::ActiveRecovery
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        verified: crate::distribution_transport::AuthenticatedRecoveryFenceReply,
        expected: &RecoveryFenceScope,
        deadline: Instant,
    ) -> Result<Self> {
        let (reply, peer) = verified.into_parts();
        if peer.node_id() != &reply.node_id || peer.member_incarnation() != reply.member_incarnation
        {
            return Err(invalid(
                "fence response does not bind actual destination identity",
            ));
        }
        Self::from_local(reply, peer, expected, deadline)
    }
    fn from_local(
        reply: RecoveryEpochFenceReply,
        authenticated_peer: MetadataReadParticipant,
        expected: &RecoveryFenceScope,
        deadline: Instant,
    ) -> Result<Self> {
        reply
            .image_history
            .require_shape(expected, &reply.progress)?;
        if Instant::now() >= deadline
            || reply.scope != *expected
            || reply.store_open_id.is_nil()
            || reply.member_incarnation == 0
            || authenticated_peer.node_id() != &reply.node_id
            || authenticated_peer.member_incarnation() != reply.member_incarnation
            || reply.native_database_incarnation.is_empty()
            || !reply.birth_initialization_complete
            || reply.progress.node_id != reply.node_id
            || reply.progress.range_id != expected.range_id
            || reply.progress.current_epoch < expected.range_epoch
            || reply.native.range_id != expected.range_id
        {
            return Err(invalid(
                "invalid, stale or late actual recovery fence observation",
            ));
        }
        Ok(Self {
            reply,
            authenticated_peer,
            deadline,
        })
    }
    pub(crate) fn authenticated_peer(&self) -> &MetadataReadParticipant {
        &self.authenticated_peer
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.reply.scope
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        &self.reply.node_id
    }
    pub(crate) fn member_incarnation(&self) -> u64 {
        self.reply.member_incarnation
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.reply.store_open_id
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        &self.reply.native_database_incarnation
    }
    pub(crate) fn progress(&self) -> &RangeWriteProgress {
        &self.reply.progress
    }
    pub(crate) fn native(&self) -> &crate::db::NativeRangePrefixObservation {
        &self.reply.native
    }
    pub(crate) fn image_history(&self) -> &RecoveryImageHistoryObligationsV1 {
        &self.reply.image_history
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(invalid("recovery fence observation budget expired"));
        }
        Ok(())
    }
    pub(super) fn into_reply(self) -> RecoveryEpochFenceReply {
        self.reply
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct RecoveryMemberIdentity {
    pub incarnation: u64,
    pub active_certificate_sha256: Option<String>,
    pub pending_certificate_sha256: Option<String>,
}

/// An operation-bound independently fresh metadata role. This grants neither
/// a current data quorum nor native DB admission, visibility or write readiness.
#[derive(Debug)]
pub(crate) struct RecoveryRoleAdmission {
    witness: MetadataRecoveryRoleObservation,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range: RangeDescriptor,
    members: BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    local_member: RecoveryMemberIdentity,
    restore: Option<MetadataRestoreActivation>,
    schema: Option<String>,
    relocation: Option<RangeRelocation>,
    handoff_target_identity: Option<RecoveryMemberIdentity>,
}

impl RecoveryRoleAdmission {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_witness(
        witness: MetadataRecoveryRoleObservation,
        actual_initiator: &MetadataReadParticipant,
        local_node_id: &ClusterNodeId,
        read_request_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        recovery_nonce: Uuid,
        recipient_challenge: Uuid,
        purpose: &MetadataRecoveryRolePurpose,
    ) -> Result<Self> {
        if matches!(
            purpose,
            MetadataRecoveryRolePurpose::ObserveFencedRecovery { .. }
        ) {
            return Err(invalid(
                "read-only observation cannot authorize recovery mutations",
            ));
        }
        Self::from_scoped_witness(
            witness,
            actual_initiator,
            local_node_id,
            read_request_id,
            range_id,
            range_epoch,
            recovery_nonce,
            recipient_challenge,
            purpose,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn from_scoped_witness(
        witness: MetadataRecoveryRoleObservation,
        actual_initiator: &MetadataReadParticipant,
        local_node_id: &ClusterNodeId,
        read_request_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        recovery_nonce: Uuid,
        recipient_challenge: Uuid,
        purpose: &MetadataRecoveryRolePurpose,
    ) -> Result<Self> {
        witness.require_live_request(read_request_id, recipient_challenge)?;
        witness.require_initiator(actual_initiator)?;
        let request = witness.request();
        if &request.recipient_node_id != local_node_id
            || request.range_id != range_id
            || request.range_epoch != range_epoch
            || request.recovery_nonce != recovery_nonce
            || &request.purpose != purpose
        {
            return Err(invalid(
                "fresh witness does not bind this operation, recipient or purpose",
            ));
        }
        let topology = witness.committed_topology(read_request_id, recipient_challenge)?;
        let bootstrap_id = topology
            .range_image_bootstrap_id
            .filter(|id| !id.is_nil())
            .ok_or_else(|| invalid("legacy metadata has no admitted native image bootstrap"))?;
        let range = topology
            .range_by_id(range_id)
            .ok_or_else(|| invalid("witness range absent"))?
            .clone();
        let identity = |node: &crate::distribution::ClusterNode| RecoveryMemberIdentity {
            incarnation: node.incarnation,
            active_certificate_sha256: node.tls_certificate_sha256.clone(),
            pending_certificate_sha256: node.pending_tls_certificate_sha256.clone(),
        };
        let members = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| {
                let node = &topology.nodes[&replica.node_id];
                (node.id.clone(), identity(node))
            })
            .collect::<BTreeMap<_, _>>();
        if members.is_empty() || members.len() > 64 {
            return Err(invalid("current data voter set exceeds recovery bound"));
        }
        let local_member = identity(
            topology
                .nodes
                .get(local_node_id)
                .ok_or_else(|| invalid("witness local member absent"))?,
        );
        let schema = topology
            .nodes
            .get(&range.leader)
            .and_then(|node| node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL))
            .cloned();
        let relocation = match purpose {
            MetadataRecoveryRolePurpose::NormalRecovery => None,
            MetadataRecoveryRolePurpose::CommittedHandoff { relocation_id, .. } => Some(
                topology
                    .relocations
                    .get(relocation_id)
                    .ok_or_else(|| invalid("committed handoff absent"))?
                    .clone(),
            ),
            MetadataRecoveryRolePurpose::ObserveFencedRecovery {
                committed_handoff, ..
            } => committed_handoff
                .as_ref()
                .map(|binding| {
                    topology
                        .relocations
                        .get(&binding.relocation_id)
                        .cloned()
                        .ok_or_else(|| invalid("observed committed handoff absent"))
                })
                .transpose()?,
        };
        let handoff_target_identity = relocation
            .as_ref()
            .map(|plan| {
                topology
                    .nodes
                    .get(&plan.target)
                    .map(identity)
                    .ok_or_else(|| invalid("committed target member absent"))
            })
            .transpose()?;
        let restore = witness
            .restore_activation(read_request_id, recipient_challenge)?
            .cloned();
        let cluster_id = topology.cluster_id.clone();
        witness.require_live_request(read_request_id, recipient_challenge)?;
        Ok(Self {
            witness,
            cluster_id,
            bootstrap_id,
            range,
            members,
            local_member,
            restore,
            schema,
            relocation,
            handoff_target_identity,
        })
    }
    pub(crate) fn native_store_birth(
        &self,
        node: &ClusterNodeId,
    ) -> Result<&crate::distribution::NativeStoreBirthV1> {
        self.require_live_request()?;
        self.witness
            .committed_topology(self.business_request_id(), self.recipient_challenge())?
            .nodes
            .get(node)
            .and_then(|n| n.native_store_birth_v1.as_ref())
            .ok_or_else(|| invalid("fresh metadata does not bind the actual native voter birth"))
    }
    pub(crate) fn configuration_sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut replicas = self
            .range
            .replicas
            .iter()
            .map(|r| (r.id, &r.node_id, &r.role))
            .collect::<Vec<_>>();
        replicas.sort_by_key(|r| r.0);
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            "bicdb-current-recovery-config-v1",
            &self.cluster_id,
            self.bootstrap_id,
            self.range.id,
            self.range.start_token,
            self.range.end_token,
            self.range.epoch,
            &self.range.leader,
            &self.range.placement,
            &replicas,
            &self.members,
            &self.schema,
            &self.restore,
            &self.handoff_target_identity,
            &self.relocation.as_ref().map(|r| {
                (
                    &r.id,
                    &r.plan_id,
                    &r.handoff_session,
                    r.expected_epoch,
                    r.learner_epoch,
                    &r.target,
                    r.target_replica_id,
                )
            }),
        ))?)))
    }
    /// The data-range identity shared by Normal and exact committed-Handoff
    /// purposes. Plan/session/target credentials remain bound by full SHA.
    pub(crate) fn range_configuration_sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut replicas = self
            .range
            .replicas
            .iter()
            .map(|r| (r.id, &r.node_id, &r.role))
            .collect::<Vec<_>>();
        replicas.sort_by_key(|r| r.0);
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            "bicdb-recovery-semantic-range-v1",
            &self.cluster_id,
            self.bootstrap_id,
            self.range.id,
            self.range.start_token,
            self.range.end_token,
            self.range.epoch,
            &self.range.leader,
            &self.range.placement,
            replicas,
            &self.members,
            &self.schema,
            &self.restore,
        ))?)))
    }
    pub(crate) fn expected_handoff_old_config_sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let members = self
            .members
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    (
                        m.incarnation,
                        m.active_certificate_sha256.clone(),
                        m.pending_certificate_sha256.clone(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            &self.cluster_id,
            self.range.id,
            self.range.start_token,
            self.range.end_token,
            self.range.epoch,
            &self.range.leader,
            members,
            self.schema.as_deref(),
            self.restore.as_ref(),
        ))?)))
    }
    pub(crate) fn require_live_request(&self) -> Result<()> {
        self.witness
            .require_live_request(self.business_request_id(), self.recipient_challenge())
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.witness.request().recipient_node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.local_member.incarnation
    }
    pub(crate) fn local_member_identity(&self) -> &RecoveryMemberIdentity {
        &self.local_member
    }
    pub(crate) fn initiator_node_id(&self) -> &ClusterNodeId {
        &self.witness.request().initiator_node_id
    }
    pub(crate) fn initiator_incarnation(&self) -> u64 {
        self.witness.request().initiator_incarnation
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        &self.members
    }
    pub(crate) fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        self.restore.as_ref()
    }
    pub(crate) fn expected_schema_sha256(&self) -> Option<&str> {
        self.schema.as_deref()
    }
    pub(crate) fn committed_relocation(&self) -> Option<&RangeRelocation> {
        self.relocation.as_ref()
    }
    pub(crate) fn purpose(&self) -> &MetadataRecoveryRolePurpose {
        &self.witness.request().purpose
    }
    pub(crate) fn context_id(&self) -> Uuid {
        self.witness.context_id()
    }
    pub(crate) fn metadata_term(&self) -> u64 {
        self.witness.term()
    }
    pub(crate) fn metadata_read_index(&self) -> u64 {
        self.witness.read_index()
    }
    pub(crate) fn recovery_nonce(&self) -> Uuid {
        self.witness.request().recovery_nonce
    }
    pub(crate) fn business_request_id(&self) -> Uuid {
        self.witness.request().read_request_id
    }
    pub(crate) fn recipient_challenge(&self) -> Uuid {
        self.witness.request().recipient_challenge
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.witness.deadline()
    }
    pub(crate) fn remaining_budget(&self) -> Option<Duration> {
        self.witness.remaining_budget()
    }
    pub(crate) fn into_handoff(self) -> Result<HandoffRoleAdmission> {
        self.require_live_request()?;
        if !matches!(
            self.purpose(),
            MetadataRecoveryRolePurpose::CommittedHandoff { .. }
        ) || self.relocation.is_none()
        {
            return Err(invalid(
                "normal recovery cannot authorize native handoff installation",
            ));
        }
        Ok(HandoffRoleAdmission(self))
    }
    pub(crate) fn require_historical_command_scope(
        &self,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        self.require_live_request()?;
        command.validate()?;
        if command.cluster_id != self.cluster_id
            || command.range_id != self.range.id
            || command.range_epoch > self.range.epoch
        {
            return Err(invalid(
                "historical command is outside the freshly authorized prefix scope",
            ));
        }
        // This is purpose/role admission only. A selected historical command
        // still needs actual distinct current-voter certification before Apply.
        if let Some(plan) = &self.relocation {
            if command.range_id != plan.range_id || command.range_epoch > plan.learner_epoch {
                return Err(invalid(
                    "historical command is outside this committed handoff's old prefix",
                ));
            }
        }
        Ok(())
    }
}

/// Only reads an existing durable fence/catalogue. The underlying witness is
/// never exposed as RecoveryRoleAdmission, even inside transport dispatch.
#[derive(Debug)]
pub(crate) struct RecoveryObservationRoleAdmission {
    fresh: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
}
impl RecoveryObservationRoleAdmission {
    pub(crate) fn purpose(scope: &RecoveryFenceScope) -> Result<MetadataRecoveryRolePurpose> {
        use crate::distribution_consensus::MetadataRecoveryHandoffBinding;
        let committed_handoff = match &scope.purpose {
            MetadataRecoveryRolePurpose::NormalRecovery => None,
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id,
                plan_id,
                session_id,
                nonce,
            } => Some(MetadataRecoveryHandoffBinding {
                relocation_id: *relocation_id,
                plan_id: plan_id.clone(),
                session_id: *session_id,
                nonce: *nonce,
            }),
            MetadataRecoveryRolePurpose::ObserveFencedRecovery { .. } => {
                return Err(invalid("recursive observation scope refused"))
            }
        };
        Ok(MetadataRecoveryRolePurpose::ObserveFencedRecovery {
            coordinator_node_id: scope.initiator_node_id.clone(),
            coordinator_incarnation: scope.initiator_incarnation,
            committed_handoff,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_witness(
        witness: MetadataRecoveryRoleObservation,
        actual_observer: &MetadataReadParticipant,
        local_node: &ClusterNodeId,
        business: Uuid,
        challenge: Uuid,
        scope: &RecoveryFenceScope,
    ) -> Result<Self> {
        let purpose = Self::purpose(scope)?;
        let fresh = RecoveryRoleAdmission::from_scoped_witness(
            witness,
            actual_observer,
            local_node,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            &purpose,
        )?;
        let result = Self {
            fresh,
            scope: scope.clone(),
        };
        result.require_scope(scope)?;
        Ok(result)
    }
    pub(crate) fn require_scope(&self, scope: &RecoveryFenceScope) -> Result<()> {
        self.fresh.require_live_request()?;
        if self.scope != *scope
            || &scope.cluster_id != self.fresh.cluster_id()
            || scope.bootstrap_id != self.fresh.bootstrap_id()
            || scope.range_id != self.fresh.current_range().id
            || scope.range_epoch != self.fresh.current_range().epoch
            || scope.configuration_sha256 != self.fresh.configuration_sha256()?
            || scope.range_configuration_sha256 != self.fresh.range_configuration_sha256()?
            || scope.metadata_term > self.fresh.metadata_term()
            || scope.metadata_read_index > self.fresh.metadata_read_index()
        {
            return Err(invalid(
                "read-only witness does not match durable fenced configuration",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_native_identity(
        &self,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        self.require_scope(&self.scope)?;
        if native.cluster_id() != self.fresh.cluster_id()
            || native.bootstrap_id() != self.fresh.bootstrap_id()
            || native.local_node_id() != self.fresh.local_node_id()
            || native.local_member_incarnation() != self.fresh.local_member_incarnation()
            || native.restore_activation() != self.fresh.restore_activation()
        {
            return Err(invalid(
                "read-only observation native birth/open identity mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.fresh.local_node_id()
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.fresh.local_member_incarnation()
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        self.fresh.cluster_id()
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.fresh.bootstrap_id()
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        self.fresh.current_range()
    }
    pub(crate) fn expected_schema_sha256(&self) -> Option<&str> {
        self.fresh.expected_schema_sha256()
    }
    pub(crate) fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        self.fresh.restore_activation()
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        self.fresh.member_credentials()
    }
    /// Readonly provenance only; this does not expose the underlying mutation
    /// role or convert an observation purpose into recovery permission.
    pub(crate) fn metadata_context_id(&self) -> Uuid {
        self.fresh.context_id()
    }
    pub(crate) fn metadata_term(&self) -> u64 {
        self.fresh.metadata_term()
    }
    pub(crate) fn metadata_read_index(&self) -> u64 {
        self.fresh.metadata_read_index()
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.fresh.deadline()
    }
}

/// Exact committed-plan permission. It cannot be constructed from a normal
/// recovery witness and is not a capture/install/native receipt by itself.
#[derive(Debug)]
pub(crate) struct HandoffRoleAdmission(RecoveryRoleAdmission);
impl HandoffRoleAdmission {
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.0
    }
    pub(crate) fn require_live_scope(
        &self,
        relocation_id: crate::distribution::RelocationId,
        plan_id: &str,
        session_id: Uuid,
        nonce: Uuid,
    ) -> Result<()> {
        self.0.require_live_request()?;
        match self.0.purpose() {
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id: rid,
                plan_id: pid,
                session_id: sid,
                nonce: n,
            } if *rid == relocation_id && pid == plan_id && *sid == session_id && *n == nonce => {
                Ok(())
            }
            _ => Err(invalid(
                "handoff witness is not for this exact plan/session",
            )),
        }
    }
    pub(crate) fn target_member_identity(&self) -> &RecoveryMemberIdentity {
        self.0
            .handoff_target_identity
            .as_ref()
            .expect("committed handoff constructor captures its target")
    }
    pub(crate) fn committed_relocation(&self) -> &RangeRelocation {
        self.0
            .relocation
            .as_ref()
            .expect("handoff constructor requires committed plan")
    }
}

/// Genuine fresh consensus/provider test fixture, with registered test-peer
/// identities and real durable metadata stores. It simulates member transport;
/// it is explicitly NOT mTLS, hardware or deployment qualification evidence.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn test_handoff_role_admission(
    root: &std::path::Path,
    topology: &crate::ClusterTopology,
    metadata_leader: &ClusterNodeId,
    initiator: &ClusterNodeId,
    recipient: &ClusterNodeId,
    relocation_id: crate::RelocationId,
    budget: Duration,
) -> Result<HandoffRoleAdmission> {
    let plan = topology
        .relocations
        .get(&relocation_id)
        .ok_or_else(|| invalid("test committed plan absent"))?;
    let session = plan
        .handoff_session
        .as_ref()
        .ok_or_else(|| invalid("test committed session absent"))?;
    test_role_admission(
        root,
        topology,
        metadata_leader,
        initiator,
        recipient,
        plan.range_id,
        plan.learner_epoch,
        session.nonce,
        MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id,
            plan_id: plan.plan_id.clone(),
            session_id: session.session_id,
            nonce: session.nonce,
        },
        budget,
    )?
    .into_handoff()
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn test_role_admission(
    root: &std::path::Path,
    topology: &crate::ClusterTopology,
    metadata_leader: &ClusterNodeId,
    initiator: &ClusterNodeId,
    recipient: &ClusterNodeId,
    range_id: RangeId,
    range_epoch: u64,
    recovery_nonce: Uuid,
    purpose: MetadataRecoveryRolePurpose,
    budget: Duration,
) -> Result<RecoveryRoleAdmission> {
    use crate::distribution_consensus::{
        MetadataConsensusStore, MetadataReadAck, MetadataReadProbeRequest,
    };
    use crate::distribution_metadata_read_provider::{
        MetadataReadProbeTransport, MetadataReadProvider,
    };
    use crate::CancellationToken;
    use parking_lot::Mutex;
    use std::sync::Arc;
    let parent = CancellationToken::uncancelable().child_with_deadline(Instant::now() + budget);
    let participant = |id: &ClusterNodeId| {
        let member = &topology.nodes[id];
        MetadataReadParticipant::from_transport(
            id.clone(),
            member.incarnation,
            member.tls_certificate_sha256.clone(),
            true,
        )
    };
    let peers = topology
        .nodes
        .keys()
        .map(|id| {
            Ok((
                id.clone(),
                Arc::new(Mutex::new(MetadataConsensusStore::open(
                    root.join(id.as_str()),
                    topology.cluster_id.clone(),
                    id.clone(),
                    topology.clone(),
                    true,
                )?)),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let leader = peers
        .get(metadata_leader)
        .ok_or_else(|| invalid("test metadata leader absent"))?
        .clone();
    {
        let mut state = leader.lock();
        state.start_election()?;
        state.become_leader(&topology.metadata_voters())?;
        for id in topology
            .metadata_voters()
            .iter()
            .filter(|id| *id != metadata_leader)
        {
            let reply = peers[id]
                .lock()
                .handle_append_request(state.append_request_from(1, 64)?)?;
            state.record_append_response(id, &reply)?;
        }
    }
    #[derive(Debug)]
    struct TestTransport {
        topology: crate::ClusterTopology,
        leader: ClusterNodeId,
        peers: BTreeMap<ClusterNodeId, Arc<Mutex<MetadataConsensusStore>>>,
    }
    impl MetadataReadProbeTransport for TestTransport {
        fn local_participant(&self) -> MetadataReadParticipant {
            let member = &self.topology.nodes[&self.leader];
            MetadataReadParticipant::from_transport(
                member.id.clone(),
                member.incarnation,
                member.tls_certificate_sha256.clone(),
                true,
            )
        }
        fn probe(
            &self,
            destination: &ClusterNodeId,
            request: MetadataReadProbeRequest,
            cancellation: &CancellationToken,
        ) -> Result<MetadataReadAck> {
            cancellation.check()?;
            let reply = self.peers[destination]
                .lock()
                .handle_read_authority_probe(request.clone())?;
            let member = &self.topology.nodes[destination];
            MetadataReadAck::from_transport(
                &request,
                destination,
                MetadataReadParticipant::from_transport(
                    member.id.clone(),
                    member.incarnation,
                    member.tls_certificate_sha256.clone(),
                    true,
                ),
                reply,
            )
        }
    }
    let transport = Arc::new(TestTransport {
        topology: topology.clone(),
        leader: metadata_leader.clone(),
        peers,
    });
    let provider = MetadataReadProvider::new(leader, transport)?;
    let request_id = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let remaining = parent
        .deadline()
        .unwrap()
        .checked_duration_since(Instant::now())
        .ok_or_else(|| invalid("test witness deadline expired"))?;
    let request = crate::distribution_consensus::MetadataRecoveryRoleRequest {
        protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
        acquisition_id: Uuid::now_v7(),
        read_request_id: request_id,
        initiator_node_id: initiator.clone(),
        initiator_incarnation: topology.nodes[initiator].incarnation,
        initiator_certificate_sha256: topology.nodes[initiator].tls_certificate_sha256.clone(),
        initiator_plaintext_loopback: true,
        recipient_node_id: recipient.clone(),
        recipient_incarnation: topology.nodes[recipient].incarnation,
        range_id,
        range_epoch,
        recovery_nonce,
        recipient_challenge: challenge,
        purpose: purpose.clone(),
        budget_ms: remaining
            .as_millis()
            .min(5000)
            .try_into()
            .map_err(|_| invalid("test witness budget overflow"))?,
    };
    let started = Instant::now();
    let response =
        provider.acquire_recovery_role(&participant(recipient), request.clone(), &parent)?;
    // The transport wrapper is test simulated; the authority consumed above
    // came exclusively from actual begin/probe/record/finish on real stores.
    let observation = MetadataRecoveryRoleObservation::from_transport(
        &request,
        &topology.cluster_id,
        metadata_leader,
        participant(metadata_leader),
        response,
        parent.deadline().unwrap(),
        started,
    )?;
    while provider.inflight_contexts() != 0 {
        parent.check()?;
        std::thread::sleep(Duration::from_millis(1));
    }
    RecoveryRoleAdmission::from_witness(
        observation,
        &participant(initiator),
        recipient,
        request_id,
        range_id,
        range_epoch,
        recovery_nonce,
        challenge,
        &purpose,
    )
}

/// Shared actual metadata stores for receiver/quorum integration tests. Only
/// member transport is simulated; all metadata leadership, probes and opaque
/// authority consumption use the production implementation and durable files.
#[cfg(test)]
pub(crate) fn test_recovery_metadata_cluster(
    root: &std::path::Path,
    topology: &crate::ClusterTopology,
    metadata_leader: &ClusterNodeId,
) -> Result<
    BTreeMap<
        ClusterNodeId,
        std::sync::Arc<crate::distribution_metadata_read_provider::MetadataReadProvider>,
    >,
> {
    use crate::distribution_consensus::{
        MetadataConsensusStore, MetadataReadAck, MetadataReadProbeRequest,
        MetadataRecoveryRoleRequest,
    };
    use crate::distribution_metadata_read_provider::{
        MetadataReadProbeTransport, MetadataReadProvider,
    };
    use crate::CancellationToken;
    use parking_lot::Mutex;
    use std::sync::Arc;
    #[derive(Debug)]
    struct Peers {
        topology: crate::ClusterTopology,
        leader: ClusterNodeId,
        stores: BTreeMap<ClusterNodeId, Arc<Mutex<MetadataConsensusStore>>>,
    }
    impl Peers {
        fn participant(&self, node: &ClusterNodeId) -> MetadataReadParticipant {
            let member = &self.topology.nodes[node];
            MetadataReadParticipant::from_transport(
                node.clone(),
                member.incarnation,
                member.tls_certificate_sha256.clone(),
                true,
            )
        }
        fn provider(self: &Arc<Self>, node: &ClusterNodeId) -> Result<MetadataReadProvider> {
            MetadataReadProvider::new(
                Arc::clone(&self.stores[node]),
                Arc::new(Member {
                    local: node.clone(),
                    peers: Arc::clone(self),
                }),
            )
        }
    }
    #[derive(Debug)]
    struct Member {
        local: ClusterNodeId,
        peers: Arc<Peers>,
    }
    impl MetadataReadProbeTransport for Member {
        fn local_participant(&self) -> MetadataReadParticipant {
            self.peers.participant(&self.local)
        }
        fn probe(
            &self,
            destination: &ClusterNodeId,
            request: MetadataReadProbeRequest,
            parent: &CancellationToken,
        ) -> Result<MetadataReadAck> {
            parent.check()?;
            let reply = self.peers.stores[destination]
                .lock()
                .handle_read_authority_probe(request.clone())?;
            MetadataReadAck::from_transport(
                &request,
                destination,
                self.peers.participant(destination),
                reply,
            )
        }
        fn acquire_recovery_role(
            &self,
            destination: &ClusterNodeId,
            request: MetadataRecoveryRoleRequest,
            parent: &CancellationToken,
        ) -> Result<MetadataRecoveryRoleObservation> {
            parent.check()?;
            if destination != &self.peers.leader {
                return Err(invalid("test metadata route is not actual leader"));
            }
            let started = Instant::now();
            let deadline = parent
                .deadline()
                .ok_or_else(|| invalid("test role acquisition needs parent deadline"))?;
            let provider = self.peers.provider(destination)?;
            let response = provider.acquire_recovery_role(
                &self.peers.participant(&self.local),
                request.clone(),
                parent,
            )?;
            MetadataRecoveryRoleObservation::from_transport(
                &request,
                &self.peers.topology.cluster_id,
                destination,
                self.peers.participant(destination),
                response,
                deadline,
                started,
            )
        }
    }
    let stores = topology
        .nodes
        .keys()
        .map(|node| {
            Ok((
                node.clone(),
                Arc::new(Mutex::new(MetadataConsensusStore::open(
                    root.join(node.as_str()),
                    topology.cluster_id.clone(),
                    node.clone(),
                    topology.clone(),
                    true,
                )?)),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    {
        let mut leader = stores[metadata_leader].lock();
        leader.start_election()?;
        leader.become_leader(&topology.metadata_voters())?;
        for node in topology
            .metadata_voters()
            .iter()
            .filter(|node| *node != metadata_leader)
        {
            let response = stores[node]
                .lock()
                .handle_append_request(leader.append_request_from(1, 64)?)?;
            leader.record_append_response(node, &response)?;
        }
    }
    let peers = Arc::new(Peers {
        topology: topology.clone(),
        leader: metadata_leader.clone(),
        stores,
    });
    topology
        .nodes
        .keys()
        .map(|node| Ok((node.clone(), Arc::new(peers.provider(node)?))))
        .collect()
}
