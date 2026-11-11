//! Closed, actual native-prepared voter agreement. This proof is distinct from
//! epoch fencing, raw Applied counters, image placement and completed serving.
use super::*;
use crate::distribution_range_consensus::{
    PreparedNativeRecoveryReady, RangeWriteProgress, RangeWriteStore,
};
use crate::CancellationToken;

const NATIVE_READY_EVIDENCE_VERSION: u32 = 1;
const MAX_OBLIGATIONS: usize = 2048;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeReadyRequest {
    pub scope: RecoveryFenceScope,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeReadyRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        if self.challenge.is_nil()
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
        {
            return Err(invalid(
                "native readiness requires an exact normal-recovery request/challenge",
            ));
        }
        RecoveryEpochFenceRequest {
            scope: self.scope.clone(),
            budget_ms: self.budget_ms,
        }
        .cancellation(parent)
    }
}

/// A mandatory protocol attestation, produced only after actual native
/// materialization and the complete local irreversible-obligation checks. It
/// is not a caller-selectable permission or a metadata-only topology label.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) enum NativeReadyPhysicalDomain {
    SameVoterNativeAnchorV1,
}
/// Both stages retain physical exclusion. Activated completion decisions are
/// deliberately a different protocol and cannot deserialize into this type.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) enum FrozenNativeReadyStage {
    NativePrepared,
    NativeCompletedFrozen,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeReadyReply {
    pub stage: FrozenNativeReadyStage,
    pub evidence_version: u32,
    pub physical_domain: NativeReadyPhysicalDomain,
    pub scope: RecoveryFenceScope,
    pub challenge: Uuid,
    // Actual recipient's independently challenged MQ observation. The scope
    // above is the caller's requested minimum and must not be mislabeled fresh.
    pub observed_metadata_context_id: Uuid,
    pub observed_metadata_term: u64,
    pub observed_metadata_read_index: u64,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub range_index: u64,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub native_receipt_sha256: String,
    pub local_visibility_sequence: u64,
    pub progress: RangeWriteProgress,
    pub irreversible_command_sha256: BTreeMap<u64, String>,
}
impl NativeReadyReply {
    /// Structural negative-test seam only. This cannot construct an
    /// authenticated observation or any quorum/serving permission.
    #[cfg(test)]
    pub(crate) fn test_validate_shape_only(
        &self,
        request: &NativeReadyRequest,
        peer: &MetadataReadParticipant,
    ) -> Result<()> {
        self.require_shape(request, peer)
    }
    fn require_shape(
        &self,
        request: &NativeReadyRequest,
        peer: &MetadataReadParticipant,
    ) -> Result<()> {
        request.scope.validate()?;
        let sha = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.evidence_version != NATIVE_READY_EVIDENCE_VERSION
            || self.physical_domain != NativeReadyPhysicalDomain::SameVoterNativeAnchorV1
            || self.challenge != request.challenge
            || self.challenge.is_nil()
            || self.observed_metadata_context_id.is_nil()
            || self.observed_metadata_term == 0
            || self.observed_metadata_term < request.scope.metadata_term
            || self.observed_metadata_read_index < request.scope.metadata_read_index
            || !self.scope.same_session(&request.scope)
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.node_id != *peer.node_id()
            || self.member_incarnation != peer.member_incarnation()
            || self.store_open_id.is_nil()
            || self.native_database_incarnation.is_empty()
            || self.native_database_incarnation.len() > 128
            || !sha(&self.logical_genesis_sha256)
            || !sha(&self.logical_history_sha256)
            || !sha(&self.native_receipt_sha256)
            || (self.range_index == 0 && self.logical_genesis_sha256 != self.logical_history_sha256)
            || self.progress.node_id != self.node_id
            || self.progress.range_id != self.scope.range_id
            || self.progress.current_epoch != self.scope.range_epoch
            || self.progress.last_index != self.range_index
            || self.progress.resolved_through != self.range_index
            || self.progress.compacted_through > self.range_index
            || self.irreversible_command_sha256.len() > MAX_OBLIGATIONS
            || self
                .irreversible_command_sha256
                .iter()
                .any(|(i, h)| *i == 0 || *i > self.range_index || !sha(h))
        {
            return Err(invalid(
                "native-prepared reply is not the exact closed physical request/domain",
            ));
        }
        Ok(())
    }
}

/// Construction is restricted to an ACTUAL local prepared projection or a
/// response already authenticated/bound by the transport envelope. Decoding
/// NativeReadyReply cannot construct this observation.
#[derive(Debug)]
pub(crate) struct NativeReadyObservation {
    reply: NativeReadyReply,
    peer: MetadataReadParticipant,
    deadline: Instant,
}
impl NativeReadyObservation {
    pub(crate) fn from_local(
        evidence: crate::distribution_range_consensus::LocalNativeReadyEvidence,
        peer: MetadataReadParticipant,
        request: &NativeReadyRequest,
        deadline: Instant,
    ) -> Result<Self> {
        Self::checked(evidence.into_reply(), peer, request, deadline)
    }
    pub(crate) fn from_transport(
        evidence: crate::distribution_transport::AuthenticatedNativeReadyReply,
        request: &NativeReadyRequest,
        deadline: Instant,
    ) -> Result<Self> {
        let (reply, peer) = evidence.into_parts();
        Self::checked(reply, peer, request, deadline)
    }
    fn checked(
        reply: NativeReadyReply,
        peer: MetadataReadParticipant,
        request: &NativeReadyRequest,
        deadline: Instant,
    ) -> Result<Self> {
        reply.require_shape(request, &peer)?;
        let observation = Self {
            reply,
            peer,
            deadline,
        };
        observation.require_live()?;
        Ok(observation)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(invalid("native-ready response expired before admission"));
        }
        Ok(())
    }
    pub(crate) fn reply(&self) -> &NativeReadyReply {
        &self.reply
    }
    pub(crate) fn authenticated_peer(&self) -> &MetadataReadParticipant {
        &self.peer
    }
    pub(crate) fn into_reply(self) -> NativeReadyReply {
        self.reply
    }
}

#[derive(Debug)]
pub(crate) struct ClosedNativeReadyQuorumRound {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    observations: BTreeMap<ClusterNodeId, NativeReadyObservation>,
    poisoned: bool,
}
impl ClosedNativeReadyQuorumRound {
    pub(crate) fn begin(role: RecoveryRoleAdmission, parent: &CancellationToken) -> Result<Self> {
        let cancellation = parent.child_with_deadline(role.deadline());
        cancellation.check()?;
        if !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || role.member_credentials().is_empty()
            || role.member_credentials().len() > 64
            || role.member_credentials().len()
                != role
                    .current_range()
                    .replicas
                    .iter()
                    .filter(|r| r.role == RangeReplicaRole::Voter)
                    .count()
            || !role.member_credentials().contains_key(role.local_node_id())
        {
            return Err(invalid(
                "closed native-ready quorum is restricted to a current normal data voter",
            ));
        }
        let scope = RecoveryFenceScope::from_role(&role)?;
        Ok(Self {
            role,
            scope,
            cancellation,
            observations: BTreeMap::new(),
            poisoned: false,
        })
    }
    pub(crate) fn record(&mut self, observation: NativeReadyObservation) -> Result<()> {
        if self.poisoned {
            return Err(invalid("closed native-ready round is poisoned"));
        }
        let result = self.record_inner(observation);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn record_inner(&mut self, observation: NativeReadyObservation) -> Result<()> {
        self.cancellation.check()?;
        observation.require_live()?;
        self.role.require_live_request()?;
        let reply = observation.reply();
        reply.scope.require_role(&self.role)?;
        if reply.observed_metadata_term < self.scope.metadata_term
            || reply.observed_metadata_read_index < self.scope.metadata_read_index
        {
            return Err(invalid(
                "native-ready recipient observation predates this actual caller MQ anchor",
            ));
        }
        if !reply.scope.same_session(&self.scope) || self.observations.contains_key(&reply.node_id)
        {
            return Err(invalid("duplicate or wrong-session native-prepared voter"));
        }
        let member = self
            .role
            .member_credentials()
            .get(&reply.node_id)
            .ok_or_else(|| invalid("native-prepared peer is not a current voter"))?;
        let peer = observation.authenticated_peer();
        if peer.member_incarnation() != member.incarnation
            || reply.member_incarnation != member.incarnation
        {
            return Err(invalid("native-prepared voter incarnation changed"));
        }
        match (
            &member.active_certificate_sha256,
            &member.pending_certificate_sha256,
        ) {
            (None, None) if peer.plaintext_loopback() => {}
            (active, pending)
                if peer.certificate_sha256().is_some_and(|p| {
                    active.as_deref() == Some(p) || pending.as_deref() == Some(p)
                }) => {}
            _ => {
                return Err(invalid(
                    "native-prepared peer pin differs from fresh membership",
                ))
            }
        }
        for old in self.observations.values().map(|o| o.reply()) {
            if old.logical_genesis_sha256 != reply.logical_genesis_sha256
                || (old.range_index == reply.range_index
                    && old.logical_history_sha256 != reply.logical_history_sha256)
                || reply.irreversible_command_sha256.iter().any(|(i, h)| {
                    old.irreversible_command_sha256
                        .get(i)
                        .is_some_and(|old| old != h)
                })
            {
                return Err(invalid(
                    "current native-prepared voters disclose conflicting immutable history",
                ));
            }
        }
        self.observations.insert(reply.node_id.clone(), observation);
        Ok(())
    }
    fn majority_point(&self) -> Option<(u64, String, String)> {
        let q = self.role.member_credentials().len() / 2 + 1;
        let mut counts = BTreeMap::new();
        for reply in self.observations.values().map(|o| o.reply()) {
            let point = (
                reply.range_index,
                reply.logical_genesis_sha256.clone(),
                reply.logical_history_sha256.clone(),
            );
            let count = counts.entry(point.clone()).or_insert(0usize);
            *count += 1;
            if *count >= q {
                // An actual higher physical preparation is irreversible
                // evidence. It is a veto, never a numeric authority to move a
                // lower replica forward or silently discard its known tail.
                if self
                    .observations
                    .values()
                    .any(|o| o.reply().range_index > point.0)
                {
                    return None;
                }
                return Some(point);
            }
        }
        None
    }
    pub(crate) fn is_ready(&self) -> bool {
        !self.poisoned && self.majority_point().is_some()
    }
    pub(crate) fn finish(mut self) -> Result<ClosedNativeReadyQuorum> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if !self.is_ready() {
            return Err(invalid("no closed distinct current native-ready majority"));
        }
        let point = self.majority_point().unwrap();
        self.observations.retain(|_, o| {
            let r = o.reply();
            r.range_index == point.0
                && r.logical_genesis_sha256 == point.1
                && r.logical_history_sha256 == point.2
        });
        Ok(ClosedNativeReadyQuorum {
            role: self.role,
            scope: self.scope,
            cancellation: self.cancellation,
            observations: self.observations,
            point,
        })
    }
}

/// Closed physical/data-majority proof only. It neither releases a frozen
/// range nor activates serving; durable completion consumes it separately.
#[derive(Debug)]
pub(crate) struct ClosedNativeReadyQuorum {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    observations: BTreeMap<ClusterNodeId, NativeReadyObservation>,
    point: (u64, String, String),
}
impl ClosedNativeReadyQuorum {
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
    pub(crate) fn range_index(&self) -> u64 {
        self.point.0
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.point.1
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.point.2
    }
    pub(crate) fn distinct_member_ids(&self) -> impl Iterator<Item = &ClusterNodeId> {
        self.observations.keys()
    }
    pub(crate) fn require_recipient_role(&self, fresh: &RecoveryRoleAdmission) -> Result<()> {
        self.require_live()?;
        self.scope.require_role(fresh)?;
        if self.role.local_node_id() != fresh.local_node_id()
            || self.role.local_member_incarnation() != fresh.local_member_incarnation()
        {
            return Err(invalid(
                "closed native-ready quorum belongs to a different actual recipient",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_local_prepared(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.require_recipient_role(fresh)?;
        prepared.require_current_under_role(db, store, fresh)?;
        if !prepared.scope().same_session(&self.scope)
            || prepared.range_index() != self.range_index()
            || prepared.logical_genesis_sha256() != self.logical_genesis_sha256()
            || prepared.logical_history_sha256() != self.logical_history_sha256()
        {
            return Err(invalid(
                "actual local native preparation differs from the closed current quorum",
            ));
        }
        Ok(())
    }
}
