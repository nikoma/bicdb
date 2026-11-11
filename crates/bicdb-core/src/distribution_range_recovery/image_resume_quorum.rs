//! Mixed refusal quorum ONLY for exact interrupted image recovery. It has no
//! conversion to FreshRangeRecoveryAuthority or any historical-write quorum.
use super::*;
use crate::distribution_range_consensus::NativeCurrentImageProposalPointer;
use std::sync::Arc;
#[derive(Debug)]
pub(crate) enum CurrentImageResumeVote {
    Ordinary(Arc<RecoveryFenceObservation>),
    Installing(Arc<CurrentImageInstallingObservation>),
}
impl CurrentImageResumeVote {
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        match self {
            Self::Ordinary(o) => o.node_id(),
            Self::Installing(o) => &o.evidence().node_id,
        }
    }
}
#[derive(Debug)]
pub(crate) struct CurrentImageResumeQuorumRound {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    target: ClusterNodeId,
    proposal: NativeCurrentImageProposalPointer,
    votes: BTreeMap<ClusterNodeId, CurrentImageResumeVote>,
}
impl CurrentImageResumeQuorumRound {
    pub(crate) fn begin(
        role: RecoveryRoleAdmission,
        target: ClusterNodeId,
        proposal: NativeCurrentImageProposalPointer,
        parent: &CancellationToken,
    ) -> Result<Self> {
        role.require_live_request()?;
        proposal.validate()?;
        if !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || role.member_credentials().is_empty()
            || role.member_credentials().len() > 64
            || !role.member_credentials().contains_key(&target)
            || &target == role.local_node_id()
        {
            return Err(invalid("invalid interrupted-image quorum purpose/target"));
        }
        let cancellation = parent.child_with_deadline(role.deadline());
        cancellation.check()?;
        Ok(Self {
            scope: RecoveryFenceScope::from_role(&role)?,
            role,
            cancellation,
            target,
            proposal,
            votes: BTreeMap::new(),
        })
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    fn check_new(&self, node: &ClusterNodeId) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if self.votes.contains_key(node) {
            return Err(invalid("duplicate interrupted-image quorum voter"));
        }
        Ok(())
    }
    pub(crate) fn record_ordinary(&mut self, observation: RecoveryFenceObservation) -> Result<()> {
        self.check_new(observation.node_id())?;
        super::quorum::validate_ordinary_fence_for_role(&self.role, &self.scope, &observation)?;
        if observation.node_id() == &self.target {
            return Err(invalid(
                "interrupted target must supply distinct Installing observation",
            ));
        }
        self.votes.insert(
            observation.node_id().clone(),
            CurrentImageResumeVote::Ordinary(Arc::new(observation)),
        );
        Ok(())
    }
    pub(crate) fn record_installing(
        &mut self,
        observation: CurrentImageInstallingObservation,
    ) -> Result<()> {
        let evidence = observation.evidence();
        self.check_new(&evidence.node_id)?;
        observation.require_live()?;
        evidence.validate_shape()?;
        evidence.scope.require_role(&self.role)?;
        let peer = observation.authenticated_peer();
        let member = self
            .role
            .member_credentials()
            .get(&evidence.node_id)
            .ok_or_else(|| invalid("Installing voter removed from current configuration"))?;
        let has_pins = member.active_certificate_sha256.is_some()
            || member.pending_certificate_sha256.is_some();
        let pin = if has_pins {
            !peer.plaintext_loopback()
                && peer.certificate_sha256().is_some_and(|p| {
                    member.active_certificate_sha256.as_deref() == Some(p)
                        || member.pending_certificate_sha256.as_deref() == Some(p)
                })
        } else {
            peer.plaintext_loopback() && peer.certificate_sha256().is_none()
        };
        let mut current = self.role.current_range().clone();
        current.approximate_bytes = 0;
        current.approximate_qps = 0;
        current.replicas.sort_by_key(|r| r.id);
        if evidence.node_id != self.target
            || evidence.proposal != self.proposal
            || !self.scope.same_session(&evidence.scope)
            || peer.node_id() != &evidence.node_id
            || peer.member_incarnation() != member.incarnation
            || member.incarnation != evidence.member_incarnation
            || !pin
            || evidence.source_acceptance.members != *self.role.member_credentials()
            || evidence.source_acceptance.range != current
            || self.role.expected_schema_sha256()
                != Some(evidence.source_acceptance.schema_sha256.as_str())
            || self.role.restore_activation()
                != evidence.source_acceptance.restore_activation.as_ref()
            || evidence.observed_metadata_term < self.role.metadata_term()
            || evidence.observed_metadata_read_index < self.role.metadata_read_index()
        {
            return Err(invalid(
                "Installing observation is not exact current resume quorum domain",
            ));
        }
        self.votes.insert(
            evidence.node_id.clone(),
            CurrentImageResumeVote::Installing(Arc::new(observation)),
        );
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<FreshCurrentImageResumeAuthority> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if self.votes.len() < self.role.member_credentials().len() / 2 + 1
            || !matches!(
                self.votes.get(&self.target),
                Some(CurrentImageResumeVote::Installing(_))
            )
        {
            return Err(invalid(
                "no distinct current majority including exact Installing target",
            ));
        }
        Ok(FreshCurrentImageResumeAuthority {
            role: self.role,
            scope: self.scope,
            cancellation: self.cancellation,
            target: self.target,
            proposal: self.proposal,
            votes: self.votes,
        })
    }
}
#[derive(Debug)]
pub(crate) struct FreshCurrentImageResumeAuthority {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    target: ClusterNodeId,
    proposal: NativeCurrentImageProposalPointer,
    votes: BTreeMap<ClusterNodeId, CurrentImageResumeVote>,
}
impl FreshCurrentImageResumeAuthority {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()
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
    pub(crate) fn deadline(&self) -> Instant {
        self.cancellation.deadline().expect("fresh role deadline")
    }
    pub(crate) fn target(&self) -> &ClusterNodeId {
        &self.target
    }
    pub(crate) fn proposal(&self) -> &NativeCurrentImageProposalPointer {
        &self.proposal
    }
    pub(crate) fn votes(&self) -> &BTreeMap<ClusterNodeId, CurrentImageResumeVote> {
        &self.votes
    }
    pub(crate) fn installing(&self) -> &CurrentImageInstallingEvidence {
        match &self.votes[&self.target] {
            CurrentImageResumeVote::Installing(o) => o.evidence(),
            _ => unreachable!("sealed exact target"),
        }
    }
}
