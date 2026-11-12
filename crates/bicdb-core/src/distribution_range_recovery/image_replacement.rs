//! Distinct current authority for completing an ALREADY COMMITTED decision
//! through an interrupted image. Never an ordinary fence or an Installing Apply.
use super::*;
use crate::db::{NativeRangePrefixObservation, ProtectedCurrentImagePreimageV1};
use crate::distribution_range_consensus::{
    HistoricalRangeCertification, NativeCurrentImageProposalPointer, RangeWriteLogEntry,
    RangeWriteState,
};
use crate::CancellationToken;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstallingCommittedRequest {
    pub scope: RecoveryFenceScope,
    pub proposal: NativeCurrentImageProposalPointer,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl InstallingCommittedRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.proposal.validate()?;
        if self.challenge.is_nil() {
            return Err(invalid("Installing committed request challenge is nil"));
        }
        RecoveryEpochFenceRequest {
            scope: self.scope.clone(),
            budget_ms: self.budget_ms,
        }
        .cancellation(parent)
    }
}
/// Projection only. The historical cut remains historical even when the
/// current role has a higher data epoch and another current leader.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstallingCommittedFenceEvidence {
    pub current_scope: RecoveryFenceScope,
    pub historical_source: HistoricalCurrentImageAcceptanceV1,
    pub proposal: NativeCurrentImageProposalPointer,
    pub intent_sha256: String,
    pub preimage: ProtectedCurrentImagePreimageV1,
    pub target_namespace_ids: Vec<(String, Uuid)>,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub native_birth_id: Uuid,
    pub native_birth_sha256: String,
    pub native_database_incarnation: String,
    pub store_open_id: Uuid,
    pub metadata_context_id: Uuid,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub challenge: Uuid,
    pub progress: RangeWriteProgress,
    pub native: NativeRangePrefixObservation,
    pub native_point: Option<RecoveryNativeHistoryPointV1>,
    pub pending: Option<RecoveryNativePendingHistoryV1>,
    pub irreversible_commands: BTreeMap<u64, String>,
    pub catalogue_sha256: String,
}
impl InstallingCommittedFenceEvidence {
    pub(crate) fn validate_shape(&self) -> Result<()> {
        self.current_scope.validate()?;
        self.proposal.validate()?;
        self.historical_source.validate()?;
        self.preimage.validate()?;
        if !matches!(
            self.current_scope.purpose,
            MetadataRecoveryRolePurpose::NormalRecovery
        ) || self.current_scope.cluster_id != self.historical_source.scope.cluster_id
            || self.current_scope.bootstrap_id != self.historical_source.scope.bootstrap_id
            || self.current_scope.range_id != self.historical_source.scope.range_id
            || self.current_scope.range_epoch < self.historical_source.scope.range_epoch
            || self.node_id != self.historical_source.target_id
            || self
                .historical_source
                .members
                .get(&self.node_id)
                .is_none_or(|m| m.incarnation != self.member_incarnation)
            || self.member_incarnation == 0
            || self.native_birth_id.is_nil()
            || self.store_open_id.is_nil()
            || self.metadata_context_id.is_nil()
            || self.challenge.is_nil()
            || self.native_database_incarnation.is_empty()
            || self.native_database_incarnation.len() > 128
            || !digest(&self.native_birth_sha256)
            || !digest(&self.intent_sha256)
            || !digest(&self.catalogue_sha256)
            || self.metadata_term < self.historical_source.metadata_term
            || self.metadata_read_index < self.historical_source.metadata_read_index
            || self.progress.node_id != self.node_id
            || self.progress.range_id != self.current_scope.range_id
            || self.progress.current_epoch > self.current_scope.range_epoch
            || self.progress.compacted_through > self.progress.resolved_through
            || self.progress.resolved_through > self.progress.last_index
            || self.native.range_id != self.current_scope.range_id
            || self.target_namespace_ids.len() > 65_536
            || self
                .target_namespace_ids
                .windows(2)
                .any(|p| p[0].0 >= p[1].0)
            || self
                .target_namespace_ids
                .iter()
                .any(|(n, u)| n.is_empty() || u.is_nil())
            || self.irreversible_commands.len() > 65_536
            || self
                .irreversible_commands
                .iter()
                .any(|(i, h)| *i == 0 || !digest(h))
        {
            return Err(invalid(
                "invalid current Installing committed-fence evidence",
            ));
        }
        if let Some(p) = &self.native_point {
            if !digest(&p.logical_genesis_sha256)
                || !digest(&p.logical_history_sha256)
                || !digest(&p.native_receipt_sha256)
            {
                return Err(invalid("invalid Installing current native point"));
            }
        }
        if let Some(p) = &self.pending {
            if p.index == 0 || !digest(&p.command_checksum_sha256) {
                return Err(invalid("invalid Installing current pending"));
            }
        }
        self.irreversible_union()?;
        crate::distribution_range_image::bounded_json(self, 8 * 1024 * 1024)?;
        Ok(())
    }
    pub(crate) fn irreversible_union(&self) -> Result<BTreeMap<u64, String>> {
        let mut out = self.preimage.irreversible_commands.clone();
        let mut add = |i: u64, h: &str| -> Result<()> {
            if i == 0
                || !digest(h)
                || out.insert(i, h.to_owned()).is_some_and(|old| old != h)
                || out.len() > 65_536
            {
                return Err(invalid("Installing obligation conflict/bound"));
            }
            Ok(())
        };
        for (i, h) in &self.irreversible_commands {
            add(*i, h)?;
        }
        if let (Some(i), Some(h)) = (
            self.preimage.pending_index,
            self.preimage.pending_command_sha256.as_deref(),
        ) {
            add(i, h)?;
        }
        if let Some(p) = &self.pending {
            add(p.index, &p.command_checksum_sha256)?;
        }
        Ok(out)
    }
}
#[derive(Debug)]
pub(crate) struct InstallingCommittedFenceObservation {
    evidence: InstallingCommittedFenceEvidence,
    peer: MetadataReadParticipant,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl InstallingCommittedFenceObservation {
    pub(super) fn from_local(
        evidence: InstallingCommittedFenceEvidence,
        peer: MetadataReadParticipant,
        request: &InstallingCommittedRequest,
        token: &CancellationToken,
    ) -> Result<Self> {
        token.check()?;
        evidence.validate_shape()?;
        let deadline = token
            .deadline()
            .ok_or_else(|| invalid("Installing observation lacks original deadline"))?;
        if Instant::now() >= deadline
            || !request.scope.same_session(&evidence.current_scope)
            || request.proposal != evidence.proposal
            || request.challenge != evidence.challenge
            || peer.node_id() != &evidence.node_id
            || peer.member_incarnation() != evidence.member_incarnation
        {
            return Err(invalid(
                "Installing actual observation request/peer changed",
            ));
        }
        Ok(Self {
            evidence,
            peer,
            deadline,
            cancellation: token.clone(),
        })
    }
    pub(crate) fn evidence(&self) -> &InstallingCommittedFenceEvidence {
        &self.evidence
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        if Instant::now() >= self.deadline {
            return Err(invalid("Installing actual observation expired"));
        }
        Ok(())
    }
    pub(crate) fn authenticated_peer(&self) -> &MetadataReadParticipant {
        &self.peer
    }
}
#[derive(Debug)]
pub(crate) enum InstallingCommittedVote {
    Ordinary(Arc<RecoveryFenceObservation>),
    Installing(Arc<InstallingCommittedFenceObservation>),
}
#[derive(Debug)]
pub(crate) struct InstallingCommittedQuorumRound {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    target: ClusterNodeId,
    pointer: NativeCurrentImageProposalPointer,
    votes: BTreeMap<ClusterNodeId, InstallingCommittedVote>,
    cancellation: CancellationToken,
}
impl InstallingCommittedQuorumRound {
    pub(crate) fn begin(
        role: RecoveryRoleAdmission,
        target: ClusterNodeId,
        pointer: NativeCurrentImageProposalPointer,
        parent: &CancellationToken,
    ) -> Result<Self> {
        role.require_live_request()?;
        pointer.validate()?;
        if !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || role.member_credentials().len() > 64
            || role.member_credentials().is_empty()
            || !role.member_credentials().contains_key(&target)
        {
            return Err(invalid(
                "invalid Installing committed quorum purpose/target",
            ));
        }
        let scope = RecoveryFenceScope::from_role(&role)?;
        let cancellation = parent.child_with_deadline(role.deadline());
        cancellation.check()?;
        Ok(Self {
            role,
            scope,
            target,
            pointer,
            votes: BTreeMap::new(),
            cancellation,
        })
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn record_ordinary(&mut self, o: RecoveryFenceObservation) -> Result<()> {
        self.cancellation.check()?;
        super::quorum::validate_ordinary_fence_for_role(&self.role, &self.scope, &o)?;
        if o.node_id() == &self.target || self.votes.contains_key(o.node_id()) {
            return Err(invalid(
                "duplicate or Installing-as-ordinary existing-decision voter",
            ));
        }
        self.votes.insert(
            o.node_id().clone(),
            InstallingCommittedVote::Ordinary(Arc::new(o)),
        );
        Ok(())
    }
    pub(crate) fn record_installing(
        &mut self,
        o: InstallingCommittedFenceObservation,
    ) -> Result<()> {
        self.cancellation.check()?;
        o.require_live()?;
        let e = o.evidence();
        e.validate_shape()?;
        e.current_scope.require_role(&self.role)?;
        crate::distribution_range_consensus::require_installing_same_voter_transition(
            &e.historical_source,
            &self.role,
        )?;
        let m = self
            .role
            .member_credentials()
            .get(&e.node_id)
            .ok_or_else(|| invalid("Installing current voter removed"))?;
        let p = o.authenticated_peer();
        let pins = m.active_certificate_sha256.is_some() || m.pending_certificate_sha256.is_some();
        let pin = if pins {
            !p.plaintext_loopback()
                && p.certificate_sha256().is_some_and(|h| {
                    m.active_certificate_sha256.as_deref() == Some(h)
                        || m.pending_certificate_sha256.as_deref() == Some(h)
                })
        } else {
            p.plaintext_loopback() && p.certificate_sha256().is_none()
        };
        if e.node_id != self.target
            || self.votes.contains_key(&e.node_id)
            || e.proposal != self.pointer
            || !self.scope.same_session(&e.current_scope)
            || p.node_id() != &e.node_id
            || p.member_incarnation() != m.incarnation
            || e.member_incarnation != m.incarnation
            || !pin
            || e.metadata_term < self.role.metadata_term()
            || e.metadata_read_index < self.role.metadata_read_index()
        {
            return Err(invalid(
                "Installing committed vote is not current exact authenticated domain",
            ));
        }
        self.votes.insert(
            e.node_id.clone(),
            InstallingCommittedVote::Installing(Arc::new(o)),
        );
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<FreshInstallingCommittedAuthority> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if self.votes.len() < self.role.member_credentials().len() / 2 + 1
            || !matches!(
                self.votes.get(&self.target),
                Some(InstallingCommittedVote::Installing(_))
            )
        {
            return Err(invalid("existing-decision recovery needs distinct current majority including Installing target"));
        }
        Ok(FreshInstallingCommittedAuthority {
            role: self.role,
            scope: self.scope,
            target: self.target,
            pointer: self.pointer,
            votes: self.votes,
            cancellation: self.cancellation,
        })
    }
}
#[derive(Debug)]
pub(crate) struct FreshInstallingCommittedAuthority {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    target: ClusterNodeId,
    pointer: NativeCurrentImageProposalPointer,
    votes: BTreeMap<ClusterNodeId, InstallingCommittedVote>,
    cancellation: CancellationToken,
}
impl FreshInstallingCommittedAuthority {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        // Votes were live/authenticated when consumed. Their fsynced
        // refusal is retained to THIS original role/parent deadline; a shorter
        // probe token cannot shorten or renew the enclosing round budget.
        Ok(())
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        self.role.current_range()
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        self.role.member_credentials()
    }
    pub(crate) fn proposal(&self) -> &NativeCurrentImageProposalPointer {
        &self.pointer
    }
    pub(crate) fn target(&self) -> &ClusterNodeId {
        &self.target
    }
    pub(crate) fn pointer(&self) -> &NativeCurrentImageProposalPointer {
        &self.pointer
    }
    pub(crate) fn votes(&self) -> &BTreeMap<ClusterNodeId, InstallingCommittedVote> {
        &self.votes
    }
    pub(crate) fn installing(&self) -> &InstallingCommittedFenceEvidence {
        match &self.votes[&self.target] {
            InstallingCommittedVote::Installing(o) => o.evidence(),
            _ => unreachable!(),
        }
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.cancellation
            .deadline()
            .expect("fresh Installing context")
    }
}

/// Audit-only commitment of ACTUAL current frozen participants. No loaded
/// audit may be converted back into the selected decision or an Apply proof.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstallingDecisionVoteAudit {
    pub member_incarnation: u64,
    pub raw_state: Option<RangeWriteState>,
    pub prior_sparse_sha256: Option<String>,
    pub installing_proposal: Option<NativeCurrentImageProposalPointer>,
    pub installing_intent_sha256: Option<String>,
}
#[derive(Debug)]
pub(crate) struct InstallingCommittedDecisionRound {
    invalid: bool,
    authority: Arc<FreshInstallingCommittedAuthority>,
    command: RangeWriteCommand,
    votes: BTreeMap<ClusterNodeId, InstallingDecisionVoteAudit>,
}
impl InstallingCommittedDecisionRound {
    pub(crate) fn begin(
        authority: Arc<FreshInstallingCommittedAuthority>,
        command: RangeWriteCommand,
    ) -> Result<Self> {
        crate::distribution_range_consensus::validate_native_recovery_command_size(&command)?;
        command.validate()?;
        authority.require_live()?;
        authority
            .role()
            .require_historical_command_scope(&command)?;
        let e = authority.installing();
        if command.index > e.historical_source.source_range_index
            || command.cluster_id != e.historical_source.scope.cluster_id
            || command.range_id != e.historical_source.scope.range_id
            || command.range_epoch > e.historical_source.scope.range_epoch
        {
            return Err(invalid(
                "Installing certification may only retain an existing original-cut decision",
            ));
        }
        let obligations = e.irreversible_union()?;
        if obligations
            .get(&command.index)
            .is_some_and(|h| h != &command.checksum_sha256)
        {
            return Err(invalid(
                "existing committed body conflicts with protected Installing obligation",
            ));
        }
        let mut votes = BTreeMap::new();
        votes.insert(
            e.node_id.clone(),
            InstallingDecisionVoteAudit {
                member_incarnation: e.member_incarnation,
                raw_state: None,
                prior_sparse_sha256: None,
                installing_proposal: Some(e.proposal.clone()),
                installing_intent_sha256: Some(e.intent_sha256.clone()),
            },
        );
        Ok(Self {
            invalid: false,
            authority,
            command,
            votes,
        })
    }
    pub(crate) fn record_history(&mut self, o: RecoveryHistoryObservation) -> Result<()> {
        if self.invalid {
            return Err(invalid("Installing historical round was invalidated"));
        }
        self.invalid = true;
        let out = self.record_history_inner(o);
        if out.is_ok() {
            self.invalid = false;
        }
        out
    }
    fn record_history_inner(&mut self, o: RecoveryHistoryObservation) -> Result<()> {
        self.authority.require_live()?;
        let f = o.fence();
        f.require_live()?;
        super::quorum::validate_ordinary_fence_for_role(
            self.authority.role(),
            self.authority.scope(),
            f,
        )?;
        if !matches!(
            self.authority.votes().get(f.node_id()),
            Some(InstallingCommittedVote::Ordinary(_))
        ) || o.index() != Some(self.command.index)
            || !f.is_active_recovery()
            || self.votes.contains_key(f.node_id())
        {
            return Err(invalid(
                "existing decision source is duplicate/foreign/nonactive",
            ));
        }
        if let Some(e) = o.entry() {
            if e.command != self.command
                || e.state == RangeWriteState::Aborted
                || e.command.index > f.progress().last_index
                || (e.state == RangeWriteState::Applied
                    && e.command.index > f.progress().resolved_through)
            {
                return Err(invalid(
                    "existing decision contradicts actual ordered command",
                ));
            }
        }
        if let Some(s) = o.sparse_decision() {
            s.validate_shape(self.authority.scope(), self.command.index)?;
            if s.command() != &self.command {
                return Err(invalid("existing decision contradicts actual sparse body"));
            }
        }
        self.votes.insert(
            f.node_id().clone(),
            InstallingDecisionVoteAudit {
                member_incarnation: f.member_incarnation(),
                raw_state: o.entry().map(|e| e.state),
                prior_sparse_sha256: o.sparse_decision().map(|s| s.checksum_sha256().to_owned()),
                installing_proposal: None,
                installing_intent_sha256: None,
            },
        );
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<InstallingCommittedDecision> {
        if self.invalid {
            return Err(invalid("Installing historical round was invalidated"));
        }
        self.authority.require_live()?;
        if self.votes.len() < self.authority.role().member_credentials().len() / 2 + 1
            || !self.votes.values().any(|v| {
                matches!(
                    v.raw_state,
                    Some(RangeWriteState::QuorumCommitted | RangeWriteState::Applied)
                ) || v.prior_sparse_sha256.is_some()
            })
        {
            return Err(invalid("Installing existing decision lacks actual CQ/Applied/Sparse full-body evidence in a fresh current majority"));
        }
        Ok(InstallingCommittedDecision {
            authority: self.authority,
            command: self.command,
            votes: self.votes,
        })
    }
}
#[derive(Debug)]
pub(crate) struct InstallingCommittedDecision {
    authority: Arc<FreshInstallingCommittedAuthority>,
    command: RangeWriteCommand,
    votes: BTreeMap<ClusterNodeId, InstallingDecisionVoteAudit>,
}
impl InstallingCommittedDecision {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.authority.require_live()
    }
    pub(crate) fn authority(&self) -> &FreshInstallingCommittedAuthority {
        &self.authority
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        &self.command
    }
    pub(crate) fn votes(&self) -> &BTreeMap<ClusterNodeId, InstallingDecisionVoteAudit> {
        &self.votes
    }
    pub(crate) fn require_recipient_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.require_live()?;
        self.authority.scope().require_role(role)?;
        if self.authority.role().local_node_id() != role.local_node_id()
            || self.authority.role().context_id() != role.context_id()
        {
            return Err(invalid(
                "Installing selected decision belongs to another recipient/context",
            ));
        }
        Ok(())
    }
}

/// Serialized output alone cannot produce an authenticated current certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstallingCommittedCertificationEvidence {
    pub scope: RecoveryFenceScope,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub native_birth_id: Uuid,
    pub native_birth_sha256: String,
    pub native_database_incarnation: String,
    pub store_open_id: Uuid,
    pub proposal: NativeCurrentImageProposalPointer,
    pub intent_sha256: String,
    pub command: RangeWriteCommand,
    pub marker: HistoricalRangeCertification,
    pub record_sha256: String,
    pub metadata_context_id: Uuid,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub installing: bool,
}
#[derive(Debug)]
pub(crate) struct InstallingCommittedCertificationObservation {
    evidence: InstallingCommittedCertificationEvidence,
    peer: MetadataReadParticipant,
    cancellation: CancellationToken,
}
impl InstallingCommittedCertificationObservation {
    pub(super) fn from_local(
        evidence: InstallingCommittedCertificationEvidence,
        peer: MetadataReadParticipant,
        decision: &InstallingCommittedDecision,
        token: &CancellationToken,
    ) -> Result<Self> {
        token.check()?;
        decision.require_live()?;
        let a = decision.authority();
        if !evidence.scope.same_session(a.scope())
            || evidence.command != *decision.command()
            || evidence.proposal != *a.pointer()
            || evidence.intent_sha256 != a.installing().intent_sha256
            || peer.node_id() != &evidence.node_id
            || peer.member_incarnation() != evidence.member_incarnation
        {
            return Err(invalid(
                "actual Installing certificate changed request/peer/body",
            ));
        }
        let out = Self {
            evidence,
            peer,
            cancellation: token.clone(),
        };
        out.require_for(decision)?;
        Ok(out)
    }
    pub(crate) fn evidence(&self) -> &InstallingCommittedCertificationEvidence {
        &self.evidence
    }
    fn require_for(&self, decision: &InstallingCommittedDecision) -> Result<()> {
        self.cancellation.check()?;
        decision.require_live()?;
        let a = decision.authority();
        let e = &self.evidence;
        let m = a
            .role()
            .member_credentials()
            .get(&e.node_id)
            .ok_or_else(|| invalid("Installing certifier is not current voter"))?;
        let pin = if m.active_certificate_sha256.is_some() || m.pending_certificate_sha256.is_some()
        {
            !self.peer.plaintext_loopback()
                && self.peer.certificate_sha256().is_some_and(|h| {
                    m.active_certificate_sha256.as_deref() == Some(h)
                        || m.pending_certificate_sha256.as_deref() == Some(h)
                })
        } else {
            self.peer.plaintext_loopback() && self.peer.certificate_sha256().is_none()
        };
        if self.peer.node_id() != &e.node_id
            || self.peer.member_incarnation() != m.incarnation
            || e.member_incarnation != m.incarnation
            || !pin
            || !e.scope.same_session(a.scope())
            || e.command != *decision.command()
            || e.proposal != *a.pointer()
            || e.intent_sha256 != a.installing().intent_sha256
            || e.native_birth_id.is_nil()
            || e.store_open_id.is_nil()
            || e.native_database_incarnation.is_empty()
            || !digest(&e.native_birth_sha256)
            || !digest(&e.record_sha256)
            || e.metadata_context_id.is_nil()
            || e.metadata_term < a.role().metadata_term()
            || e.metadata_read_index < a.role().metadata_read_index()
            || e.installing != (&e.node_id == a.target())
            || e.marker.recovery_nonce != a.scope().recovery_nonce
            || e.marker.configuration_sha256 != a.scope().configuration_sha256
            || e.marker.range_epoch != a.scope().range_epoch
            || e.marker.index != decision.command().index
            || e.marker.command_sha256 != decision.command().checksum_sha256
        {
            return Err(invalid(
                "Installing certificate is not actual current-scope full-body persistence",
            ));
        }
        match a.votes().get(&e.node_id) {
            Some(InstallingCommittedVote::Ordinary(o))
                if e.store_open_id == o.store_open_id()
                    && e.native_database_incarnation == o.native_database_incarnation() => {}
            Some(InstallingCommittedVote::Installing(o))
                if e.store_open_id == o.evidence().store_open_id
                    && e.native_database_incarnation
                        == o.evidence().native_database_incarnation
                    && e.native_birth_id == o.evidence().native_birth_id
                    && e.native_birth_sha256 == o.evidence().native_birth_sha256 => {}
            _ => return Err(invalid(
                "Installing certifier actual store/DB open or birth changed since current fence",
            )),
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(crate) struct InstallingCurrentCertificationRound {
    invalid: bool,
    decision: Arc<InstallingCommittedDecision>,
    votes: BTreeMap<ClusterNodeId, InstallingCommittedCertificationObservation>,
}
impl InstallingCurrentCertificationRound {
    pub(crate) fn begin(decision: Arc<InstallingCommittedDecision>) -> Result<Self> {
        decision.require_live()?;
        Ok(Self {
            invalid: false,
            decision,
            votes: BTreeMap::new(),
        })
    }
    pub(crate) fn record(&mut self, o: InstallingCommittedCertificationObservation) -> Result<()> {
        if self.invalid {
            return Err(invalid("Installing certification round was invalidated"));
        }
        self.invalid = true;
        let out = self.record_inner(o);
        if out.is_ok() {
            self.invalid = false;
        }
        out
    }
    fn record_inner(&mut self, o: InstallingCommittedCertificationObservation) -> Result<()> {
        o.require_for(&self.decision)?;
        if self.votes.contains_key(&o.evidence.node_id) {
            return Err(invalid("duplicate Installing current certifier"));
        }
        self.votes.insert(o.evidence.node_id.clone(), o);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<InstallingCommittedApplyAdmission> {
        if self.invalid {
            return Err(invalid("Installing certification round was invalidated"));
        }
        self.decision.require_live()?;
        let a = self.decision.authority();
        if self.votes.len() < a.role().member_credentials().len() / 2 + 1
            || !self.votes.contains_key(a.target())
            || a.role().local_node_id() == a.target()
        {
            return Err(invalid("native completion requires current distinct cert QQ including Installing target and an ORDINARY local recipient"));
        }
        Ok(InstallingCommittedApplyAdmission {
            decision: self.decision,
            certifications: self.votes,
        })
    }
}
/// This authorizes physical completion ONLY on the ordinary receiver. The
/// Installing participant has no Apply path and remains under its old intent.
#[derive(Debug)]
pub(crate) struct InstallingCommittedApplyAdmission {
    decision: Arc<InstallingCommittedDecision>,
    certifications: BTreeMap<ClusterNodeId, InstallingCommittedCertificationObservation>,
}
impl InstallingCommittedApplyAdmission {
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        self.decision.command()
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        self.decision.authority().role()
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.decision.authority().scope()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.decision.require_live()?;
        Ok(())
    }
    pub(crate) fn require_recipient_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.require_live()?;
        self.decision.require_recipient_role(role)?;
        if role.local_node_id() == self.decision.authority().target() {
            return Err(invalid("Installing never receives physical Apply"));
        }
        Ok(())
    }
    pub(crate) fn certified_members(&self) -> impl Iterator<Item = &ClusterNodeId> {
        self.certifications.keys()
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.decision.authority().deadline()
    }
}
