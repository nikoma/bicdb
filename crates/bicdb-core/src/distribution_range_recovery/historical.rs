//! Independent historical decision selection and current-session certification.
//! No deserialized voter list, raw Applied counter, or telemetry floor creates
//! either proof. Missing compacted history is unknown, never an abort decision.
use super::{
    invalid, FreshRangeRecoveryAuthority, RecoveryFenceObservation, RecoveryHistoryObservation,
    RecoveryMemberIdentity, RecoveryRoleAdmission,
};
use crate::distribution::{ClusterNodeId, RangeDescriptor};
use crate::distribution_consensus::{MetadataRecoveryRolePurpose, MetadataRestoreActivation};
use crate::distribution_range_consensus::{
    HistoricalRangeCertification, RangeWriteCommand, RangeWriteState, RecoveryFenceScope,
};
use crate::Result;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug)]
struct HistoricalVote {
    fence: Arc<RecoveryFenceObservation>,
    state: Option<RangeWriteState>,
    certification: Option<HistoricalRangeCertification>,
    sparse_decision_sha256: Option<String>,
}
fn verify_history(
    authority: &FreshRangeRecoveryAuthority,
    command: &RangeWriteCommand,
    observation: &RecoveryHistoryObservation,
) -> Result<()> {
    authority.require_live()?;
    let fence = observation.fence();
    fence.require_live()?;
    if !fence.is_active_recovery()
        || !fence.scope().same_session(authority.scope())
        || observation.index() != Some(command.index)
    {
        return Err(invalid(
            "historical observation is not from this active session/index",
        ));
    }
    fence.scope().require_role(authority.role())?;
    let member = authority
        .member_credentials()
        .get(fence.node_id())
        .ok_or_else(|| invalid("historical source is not a current voter"))?;
    let peer = fence.authenticated_peer();
    if peer.node_id() != fence.node_id()
        || peer.member_incarnation() != member.incarnation
        || fence.member_incarnation() != member.incarnation
    {
        return Err(invalid("historical source incarnation changed"));
    }
    match (
        &member.active_certificate_sha256,
        &member.pending_certificate_sha256,
    ) {
        (None, None) if peer.plaintext_loopback() => {}
        (active, pending)
            if peer.certificate_sha256().is_some_and(|actual| {
                active.as_deref() == Some(actual) || pending.as_deref() == Some(actual)
            }) => {}
        _ => {
            return Err(invalid(
                "historical source pin differs from fresh membership",
            ))
        }
    }
    let progress = fence.progress();
    let alternate_epoch = match authority.role().purpose() {
        MetadataRecoveryRolePurpose::NormalRecovery => None,
        MetadataRecoveryRolePurpose::CommittedHandoff { .. } => {
            authority.current_range().epoch.checked_add(2)
        }
        MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        | MetadataRecoveryRolePurpose::ObserveFencedRecovery { .. } => {
            return Err(invalid("readonly role cannot mint historical authority"))
        }
    };
    if progress.node_id != *fence.node_id()
        || progress.range_id != command.range_id
        || (progress.current_epoch != authority.scope().range_epoch
            && Some(progress.current_epoch) != alternate_epoch)
        || progress.resolved_through > progress.last_index
        || progress.compacted_through > progress.resolved_through
    {
        return Err(invalid("historical progress scope is inconsistent"));
    }
    if let Some(entry) = observation.entry() {
        if entry.command != *command
            || entry.command.index > progress.last_index
            || (entry.state == RangeWriteState::Applied
                && entry.command.index > progress.resolved_through)
        {
            return Err(invalid(
                "conflicting historical command/body or resolved state",
            ));
        }
    }
    if let Some(sparse) = observation.sparse_decision() {
        sparse.validate_shape(authority.scope(), command.index)?;
        if sparse.command() != command {
            return Err(invalid("conflicting sparse committed command body"));
        }
    }
    Ok(())
}
fn compact_vote(observation: RecoveryHistoryObservation) -> HistoricalVote {
    let state = observation.entry().map(|entry| entry.state);
    let certification = observation.current_certification().cloned();
    let sparse_decision_sha256 = observation
        .sparse_decision()
        .map(|s| s.checksum_sha256().to_owned());
    // Dropping the completed payload bounds memory to one candidate command
    // plus small authenticated observations, rather than one payload per voter.
    HistoricalVote {
        fence: observation.fence_observation(),
        state,
        certification,
        sparse_decision_sha256,
    }
}

#[derive(Debug)]
pub(crate) struct HistoricalCommandDecisionRound {
    authority: Arc<FreshRangeRecoveryAuthority>,
    command: RangeWriteCommand,
    votes: BTreeMap<ClusterNodeId, HistoricalVote>,
}
impl HistoricalCommandDecisionRound {
    pub(crate) fn begin(
        authority: Arc<FreshRangeRecoveryAuthority>,
        command: RangeWriteCommand,
    ) -> Result<Self> {
        // Bound borrowed complete shape before checksum validation allocates
        // a serialized command buffer, including for rejected peer input.
        crate::distribution_range_consensus::validate_native_recovery_command_size(&command)?;
        command.validate()?;
        authority.require_historical_scope(&command)?;
        Ok(Self {
            authority,
            command,
            votes: BTreeMap::new(),
        })
    }
    pub(crate) fn record(&mut self, observation: RecoveryHistoryObservation) -> Result<()> {
        verify_history(&self.authority, &self.command, &observation)?;
        let node = observation.fence().node_id().clone();
        if self.votes.contains_key(&node) {
            return Err(invalid("duplicate historical voter"));
        }
        self.votes.insert(node, compact_vote(observation));
        Ok(())
    }
    pub(super) fn is_ready(&self) -> bool {
        let q = self.authority.member_credentials().len() / 2 + 1;
        self.votes.len() >= q
            && !self
                .votes
                .values()
                .any(|v| v.state == Some(RangeWriteState::Aborted))
            && (self.votes.values().any(|v| {
                matches!(
                    v.state,
                    Some(RangeWriteState::QuorumCommitted | RangeWriteState::Applied)
                ) || v.sparse_decision_sha256.is_some()
            }) || self
                .votes
                .values()
                .filter(|v| v.state == Some(RangeWriteState::Committed))
                .count()
                >= q)
    }
    pub(crate) fn finish(self) -> Result<HistoricalCommandDecision> {
        self.authority.require_live()?;
        let quorum = self.authority.member_credentials().len() / 2 + 1;
        if self.votes.len() < quorum {
            return Err(invalid(
                "historical decision lacks current fenced voter quorum",
            ));
        }
        let has_cq = self.votes.values().any(|v| {
            matches!(
                v.state,
                Some(RangeWriteState::QuorumCommitted | RangeWriteState::Applied)
            ) || v.sparse_decision_sha256.is_some()
        });
        let committed = self
            .votes
            .values()
            .filter(|v| v.state == Some(RangeWriteState::Committed))
            .count();
        if self
            .votes
            .values()
            .any(|v| v.state == Some(RangeWriteState::Aborted))
        {
            return Err(invalid(
                "historical abort/conflict requires an independently proven abort/history decision",
            ));
        }
        if !has_cq && committed < quorum {
            return Err(invalid(
                "historical outcome unknown; compacted/missing entries are not proof of abort",
            ));
        }
        Ok(HistoricalCommandDecision {
            authority: self.authority,
            command: self.command,
            votes: self.votes,
        })
    }
}

/// Logical committed outcome only. Native materialization and a NEW distinct
/// current-session certify-only majority remain mandatory before Apply.
#[derive(Debug)]
pub(crate) struct HistoricalCommandDecision {
    authority: Arc<FreshRangeRecoveryAuthority>,
    command: RangeWriteCommand,
    votes: BTreeMap<ClusterNodeId, HistoricalVote>,
}
impl HistoricalCommandDecision {
    /// Audit projection from an ACTUAL live selected decision. Persisting or
    /// decoding this record never reconstructs this decision or its quorum.
    pub(crate) fn sparse_persistence_audit(
        &self,
    ) -> Result<crate::distribution_range_consensus::SparseDecisionAudit> {
        use crate::distribution_range_consensus::{SparseDecisionAudit, SparseDecisionVoteAudit};
        self.require_live()?;
        Ok(SparseDecisionAudit {
            scope: self.scope().clone(),
            members: self.authority.member_credentials().clone(),
            observations: self
                .votes
                .iter()
                .map(|(node, vote)| {
                    (
                        node.clone(),
                        SparseDecisionVoteAudit {
                            member_incarnation: vote.fence.member_incarnation(),
                            observed_raw_state: vote.state,
                            prior_sparse_decision_sha256: vote.sparse_decision_sha256.clone(),
                        },
                    )
                })
                .collect(),
        })
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        &self.command
    }
    pub(crate) fn authority(&self) -> &FreshRangeRecoveryAuthority {
        &self.authority
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.authority.scope()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.authority.require_live()
    }
    pub(crate) fn observed_members(&self) -> impl Iterator<Item = &ClusterNodeId> {
        self.votes.keys()
    }
    pub(crate) fn require_recipient_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.require_live()?;
        self.scope().require_role(role)?;
        if self.authority.role().local_node_id() != role.local_node_id()
            || self.authority.role().context_id() != role.context_id()
        {
            return Err(invalid(
                "historical decision is not this recipient's independently fresh operation",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CurrentHistoricalCertificationRound {
    decision: Arc<HistoricalCommandDecision>,
    votes: BTreeMap<ClusterNodeId, HistoricalVote>,
}
impl CurrentHistoricalCertificationRound {
    pub(crate) fn begin(decision: Arc<HistoricalCommandDecision>) -> Result<Self> {
        decision.require_live()?;
        Ok(Self {
            decision,
            votes: BTreeMap::new(),
        })
    }
    pub(crate) fn record(&mut self, observation: RecoveryHistoryObservation) -> Result<()> {
        verify_history(
            self.decision.authority(),
            self.decision.command(),
            &observation,
        )?;
        let marker = observation.current_certification().ok_or_else(|| {
            invalid("old raw CQ is not a current-session certify-only acknowledgment")
        })?;
        let scope = self.decision.scope();
        let command = self.decision.command();
        if marker.recovery_nonce != scope.recovery_nonce
            || marker.configuration_sha256 != scope.configuration_sha256
            || marker.range_epoch != scope.range_epoch
            || marker.index != command.index
            || marker.command_sha256 != command.checksum_sha256
            || (observation.sparse_decision().is_none()
                && (command.index > observation.fence().progress().last_index
                    || (observation.entry().is_none()
                        && command.index > observation.fence().progress().compacted_through)))
            || observation.entry().is_some_and(|entry| {
                !matches!(
                    entry.state,
                    RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                )
            })
        {
            return Err(invalid(
                "current historical certification does not match exact candidate/session",
            ));
        }
        let node = observation.fence().node_id().clone();
        if self.votes.contains_key(&node) {
            return Err(invalid("duplicate historical certifier"));
        }
        self.votes.insert(node, compact_vote(observation));
        Ok(())
    }
    pub(super) fn is_ready(&self) -> bool {
        self.votes.len() >= self.decision.authority().member_credentials().len() / 2 + 1
    }
    pub(crate) fn finish(self) -> Result<RecoveryApplyAdmission> {
        self.decision.require_live()?;
        if self.votes.len() < self.decision.authority().member_credentials().len() / 2 + 1 {
            return Err(invalid(
                "historical Apply lacks distinct current-session certification quorum",
            ));
        }
        Ok(RecoveryApplyAdmission {
            proof: RecoveryApplyProof::Ordinary {
                decision: self.decision,
                certifications: self.votes,
            },
        })
    }
}

/// Recipient-local, nontransferable physical historical Apply permission. No
/// serde/Clone or constructor from a numeric floor, wire DTO or voter list.
#[derive(Debug)]
pub(crate) struct RecoveryApplyAdmission {
    proof: RecoveryApplyProof,
}
#[derive(Debug)]
enum RecoveryApplyProof {
    Ordinary {
        decision: Arc<HistoricalCommandDecision>,
        certifications: BTreeMap<ClusterNodeId, HistoricalVote>,
    },
    InstallingExisting(super::InstallingCommittedApplyAdmission),
}
impl RecoveryApplyAdmission {
    pub(crate) fn from_installing_existing_committed(
        proof: super::InstallingCommittedApplyAdmission,
    ) -> Result<Self> {
        proof.require_live()?;
        Ok(Self {
            proof: RecoveryApplyProof::InstallingExisting(proof),
        })
    }
    pub(crate) fn installing_role(&self) -> Option<&RecoveryRoleAdmission> {
        match &self.proof {
            RecoveryApplyProof::InstallingExisting(p) => Some(p.role()),
            _ => None,
        }
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.command(),
            RecoveryApplyProof::InstallingExisting(p) => p.command(),
        }
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.authority().current_range(),
            RecoveryApplyProof::InstallingExisting(p) => p.role().current_range(),
        }
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().role().bootstrap_id()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().bootstrap_id(),
        }
    }
    pub(crate) fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().role().restore_activation()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().restore_activation(),
        }
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().member_credentials()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().member_credentials(),
        }
    }
    pub(crate) fn cluster_id(&self) -> &crate::ClusterId {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => &decision.scope().cluster_id,
            RecoveryApplyProof::InstallingExisting(p) => p.role().cluster_id(),
        }
    }
    pub(crate) fn expected_schema_sha256(&self) -> Option<&str> {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().role().expected_schema_sha256()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().expected_schema_sha256(),
        }
    }
    pub(crate) fn purpose(&self) -> &MetadataRecoveryRolePurpose {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.authority().role().purpose(),
            RecoveryApplyProof::InstallingExisting(p) => p.role().purpose(),
        }
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().role().local_node_id()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().local_node_id(),
        }
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().role().local_member_incarnation()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().local_member_incarnation(),
        }
    }
    pub(crate) fn context_id(&self) -> Uuid {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => {
                decision.authority().role().context_id()
            }
            RecoveryApplyProof::InstallingExisting(p) => p.role().context_id(),
        }
    }
    pub(crate) fn recovery_nonce(&self) -> Uuid {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.scope().recovery_nonce,
            RecoveryApplyProof::InstallingExisting(p) => p.scope().recovery_nonce,
        }
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.scope(),
            RecoveryApplyProof::InstallingExisting(p) => p.scope(),
        }
    }
    pub(crate) fn deadline(&self) -> Instant {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.authority().deadline(),
            RecoveryApplyProof::InstallingExisting(p) => p.deadline(),
        }
    }
    pub(crate) fn require_live_request(&self) -> Result<()> {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.require_live(),
            RecoveryApplyProof::InstallingExisting(p) => p.require_live(),
        }
    }
    pub(crate) fn remaining_budget(&self) -> Option<Duration> {
        self.deadline().checked_duration_since(Instant::now())
    }
    pub(crate) fn require_recipient_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        match &self.proof {
            RecoveryApplyProof::Ordinary { decision, .. } => decision.require_recipient_role(role),
            RecoveryApplyProof::InstallingExisting(p) => p.require_recipient_role(role),
        }
    }
    pub(crate) fn certified_members(&self) -> Box<dyn Iterator<Item = &ClusterNodeId> + '_> {
        match &self.proof {
            RecoveryApplyProof::Ordinary { certifications, .. } => Box::new(certifications.keys()),
            RecoveryApplyProof::InstallingExisting(p) => Box::new(p.certified_members()),
        }
    }
}
