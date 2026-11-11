//! Authenticated completion observations remain FROZEN. Neither a decoded DTO
//! nor this observation activates serving, releases a freeze or grants a read.
use super::*;
use crate::CancellationToken;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCompletionRequest {
    pub scope: RecoveryFenceScope,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeCompletionRequest {
    pub(crate) fn ready_request(&self) -> NativeReadyRequest {
        NativeReadyRequest {
            scope: self.scope.clone(),
            challenge: self.challenge,
            budget_ms: self.budget_ms,
        }
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.ready_request().cancellation(parent)
    }
}
/// A durable completion decision is deliberately NOT native readiness.
/// Activated anchors retain the original completed point, even after R+1 writes.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) enum NativeCompletionStage {
    CompletedFrozen,
    ActivatedAnchor,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCompletionReply {
    pub stage: NativeCompletionStage,
    pub evidence_version: u32,
    pub scope: RecoveryFenceScope,
    pub challenge: Uuid,
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
    pub local_visibility_sequence_at_completion: u64,
    pub native_serving_sha256: String,
    pub activation_decision_sha256: Option<String>,
}
impl NativeCompletionReply {
    pub(crate) fn from_frozen_projection(
        ready: NativeReadyReply,
        native_serving_sha256: String,
    ) -> Result<Self> {
        if ready.stage != FrozenNativeReadyStage::NativeCompletedFrozen {
            return Err(invalid(
                "completion projection is not actually completed-frozen",
            ));
        }
        Ok(Self {
            stage: NativeCompletionStage::CompletedFrozen,
            evidence_version: 1,
            scope: ready.scope,
            challenge: ready.challenge,
            observed_metadata_context_id: ready.observed_metadata_context_id,
            observed_metadata_term: ready.observed_metadata_term,
            observed_metadata_read_index: ready.observed_metadata_read_index,
            node_id: ready.node_id,
            member_incarnation: ready.member_incarnation,
            store_open_id: ready.store_open_id,
            native_database_incarnation: ready.native_database_incarnation,
            range_index: ready.range_index,
            logical_genesis_sha256: ready.logical_genesis_sha256,
            logical_history_sha256: ready.logical_history_sha256,
            native_receipt_sha256: ready.native_receipt_sha256,
            local_visibility_sequence_at_completion: ready.local_visibility_sequence,
            native_serving_sha256,
            activation_decision_sha256: None,
        })
    }
    fn require_shape(
        &self,
        request: &NativeCompletionRequest,
        peer: &MetadataReadParticipant,
    ) -> Result<()> {
        request.scope.validate()?;
        let sha = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.evidence_version != 1
            || !self.scope.same_session(&request.scope)
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.challenge != request.challenge
            || self.challenge.is_nil()
            || self.observed_metadata_context_id.is_nil()
            || self.observed_metadata_term == 0
            || self.observed_metadata_term < request.scope.metadata_term
            || self.observed_metadata_read_index < request.scope.metadata_read_index
            || self.node_id != *peer.node_id()
            || self.member_incarnation != peer.member_incarnation()
            || self.store_open_id.is_nil()
            || self.native_database_incarnation.is_empty()
            || self.native_database_incarnation.len() > 128
            || !sha(&self.logical_genesis_sha256)
            || !sha(&self.logical_history_sha256)
            || !sha(&self.native_receipt_sha256)
            || !sha(&self.native_serving_sha256)
            || match self.stage {
                NativeCompletionStage::CompletedFrozen => self.activation_decision_sha256.is_some(),
                NativeCompletionStage::ActivatedAnchor => self
                    .activation_decision_sha256
                    .as_deref()
                    .is_none_or(|s| !sha(s)),
            }
            || (self.range_index == 0 && self.logical_genesis_sha256 != self.logical_history_sha256)
        {
            return Err(invalid(
                "native completion is not the exact authenticated immutable decision/domain",
            ));
        }
        Ok(())
    }
}
/// Only actual same-store local persistence/projection or a request-bound,
/// authenticated transport exchange can create this observation. No DTO ctor.
#[derive(Debug)]
pub(crate) struct NativeCompletionObservation {
    reply: NativeCompletionReply,
    peer: MetadataReadParticipant,
    deadline: Instant,
}
impl NativeCompletionObservation {
    pub(crate) fn from_local(
        evidence: crate::distribution_range_consensus::LocalNativeCompletionEvidence,
        peer: MetadataReadParticipant,
        request: &NativeCompletionRequest,
        deadline: Instant,
    ) -> Result<Self> {
        Self::checked(evidence.into_reply(), peer, request, deadline)
    }
    pub(crate) fn from_transport(
        evidence: crate::distribution_transport::AuthenticatedNativeCompletionReply,
        request: &NativeCompletionRequest,
        deadline: Instant,
    ) -> Result<Self> {
        let (reply, peer) = evidence.into_parts();
        Self::checked(reply, peer, request, deadline)
    }
    fn checked(
        reply: NativeCompletionReply,
        peer: MetadataReadParticipant,
        request: &NativeCompletionRequest,
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
            return Err(invalid(
                "native completion response expired before admission",
            ));
        }
        Ok(())
    }
    pub(crate) fn reply(&self) -> &NativeCompletionReply {
        &self.reply
    }
    pub(crate) fn authenticated_peer(&self) -> &MetadataReadParticipant {
        &self.peer
    }
    pub(crate) fn into_reply(self) -> NativeCompletionReply {
        self.reply
    }
}
