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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCompletionReply {
    pub ready: NativeReadyReply,
    pub native_serving_sha256: String,
}
impl NativeCompletionReply {
    fn require_shape(
        &self,
        request: &NativeCompletionRequest,
        peer: &MetadataReadParticipant,
    ) -> Result<()> {
        self.ready.require_shape(&request.ready_request(), peer)?;
        if self.ready.stage != FrozenNativeReadyStage::NativeCompletedFrozen
            || self.native_serving_sha256.len() != 64
            || !self
                .native_serving_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid(
                "native completion is not an exact frozen native-WAL decision",
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
