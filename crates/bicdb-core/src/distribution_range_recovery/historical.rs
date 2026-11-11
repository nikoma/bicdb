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
        MetadataRecoveryRolePurpose::ObserveFencedRecovery { .. } => {
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
    Ok(())
}
fn compact_vote(observation: RecoveryHistoryObservation) -> HistoricalVote {
    let state = observation.entry().map(|entry| entry.state);
    let certification = observation.current_certification().cloned();
    // Dropping the completed payload bounds memory to one candidate command
    // plus small authenticated observations, rather than one payload per voter.
    HistoricalVote {
        fence: observation.fence_observation(),
        state,
        certification,
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
        command.validate()?;
        authority.require_historical_scope(&command)?;
        // Receiver entry export has the same finite bound. Larger commands
        // require a separately qualified image/history repair path.
        struct Bound(usize);
        impl std::io::Write for Bound {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
                    std::io::Error::other("historical candidate exceeds bounded 2MiB transport")
                })?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        serde_json::to_writer(Bound(2 * 1024 * 1024), &command)?;
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
            )
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
            || command.index > observation.fence().progress().last_index
            || (observation.entry().is_none()
                && command.index > observation.fence().progress().compacted_through)
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
    pub(crate) fn finish(self) -> Result<RecoveryApplyAdmission> {
        self.decision.require_live()?;
        if self.votes.len() < self.decision.authority().member_credentials().len() / 2 + 1 {
            return Err(invalid(
                "historical Apply lacks distinct current-session certification quorum",
            ));
        }
        Ok(RecoveryApplyAdmission {
            decision: self.decision,
            certifications: self.votes,
        })
    }
}

/// Recipient-local, nontransferable physical historical Apply permission. No
/// serde/Clone or constructor from a numeric floor, wire DTO or voter list.
#[derive(Debug)]
pub(crate) struct RecoveryApplyAdmission {
    decision: Arc<HistoricalCommandDecision>,
    certifications: BTreeMap<ClusterNodeId, HistoricalVote>,
}
impl RecoveryApplyAdmission {
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        self.decision.command()
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        self.decision.authority().current_range()
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.decision.authority().role().bootstrap_id()
    }
    pub(crate) fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        self.decision.authority().role().restore_activation()
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        self.decision.authority().member_credentials()
    }
    pub(crate) fn cluster_id(&self) -> &crate::ClusterId {
        &self.scope().cluster_id
    }
    pub(crate) fn expected_schema_sha256(&self) -> Option<&str> {
        self.decision.authority().role().expected_schema_sha256()
    }
    pub(crate) fn purpose(&self) -> &MetadataRecoveryRolePurpose {
        self.decision.authority().role().purpose()
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.decision.authority().role().local_node_id()
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.decision.authority().role().local_member_incarnation()
    }
    pub(crate) fn context_id(&self) -> Uuid {
        self.decision.authority().role().context_id()
    }
    pub(crate) fn recovery_nonce(&self) -> Uuid {
        self.decision.scope().recovery_nonce
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.decision.scope()
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.decision.authority().deadline()
    }
    pub(crate) fn remaining_budget(&self) -> Option<Duration> {
        self.deadline().checked_duration_since(Instant::now())
    }
    pub(crate) fn require_live_request(&self) -> Result<()> {
        self.decision.require_live()
    }
    pub(crate) fn require_recipient_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.decision.require_recipient_role(role)
    }
    pub(crate) fn certified_members(&self) -> impl Iterator<Item = &ClusterNodeId> {
        self.certifications.keys()
    }
}
