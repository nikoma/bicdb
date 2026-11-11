//! Authenticated observations only. Neither facts nor this sealed wrapper
//! establish admitted voter continuity, a read permit, or an MVCC snapshot.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;

/// Requested labels are not recipient metadata authority. The sender's round
/// independently binds both to its consumed actual fresh MQ observation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRangeReadProbeRequest {
    pub business_request_id: Uuid,
    pub requested_metadata_context_id: Uuid,
    pub observation: NativeRangeReadRequest,
}
impl std::ops::Deref for NativeRangeReadProbeRequest {
    type Target = NativeRangeReadRequest;
    fn deref(&self) -> &Self::Target {
        &self.observation
    }
}
impl std::ops::DerefMut for NativeRangeReadProbeRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.observation
    }
}
impl NativeRangeReadProbeRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        if self.business_request_id.is_nil() || self.requested_metadata_context_id.is_nil() {
            return Err(invalid("read probe business/MQ context binding is missing"));
        }
        self.observation.cancellation(parent)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRangeReadProbeReply {
    pub business_request_id: Uuid,
    pub requested_metadata_context_id: Uuid,
    pub facts: NativeRangeReadFacts,
}
impl NativeRangeReadProbeReply {
    fn require_request(
        &self,
        request: &NativeRangeReadProbeRequest,
        peer: &MetadataReadParticipant,
    ) -> Result<()> {
        if self.business_request_id != request.business_request_id
            || self.requested_metadata_context_id != request.requested_metadata_context_id
        {
            return Err(invalid(
                "actual read reply business/MQ context binding differs",
            ));
        }
        self.facts.require_request(request, peer)
    }
}

pub(crate) fn require_peer(
    scope: &NativeRangeReadScope,
    peer: &MetadataReadParticipant,
) -> Result<()> {
    let member = scope
        .members
        .get(peer.node_id())
        .ok_or_else(|| invalid("read peer is not a scoped voter"))?;
    if peer.member_incarnation() != member.incarnation {
        return Err(invalid("actual read peer incarnation differs"));
    }
    match (
        &member.active_certificate_sha256,
        &member.pending_certificate_sha256,
    ) {
        (None, None) if peer.plaintext_loopback() => Ok(()),
        (a, p)
            if peer
                .certificate_sha256()
                .is_some_and(|h| a.as_deref() == Some(h) || p.as_deref() == Some(h)) =>
        {
            Ok(())
        }
        _ => Err(invalid(
            "actual read peer certificate differs from scoped credentials",
        )),
    }
}

impl NativeRangeReadFacts {
    pub(super) fn require_request(
        &self,
        request: &NativeRangeReadRequest,
        peer: &MetadataReadParticipant,
    ) -> Result<()> {
        request.scope.validate()?;
        require_peer(&request.scope, peer)?;
        let hash = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.evidence_version != NATIVE_RANGE_READ_VERSION
            || self.scope_sha256 != request.scope.checksum_sha256()?
            || self.challenge != request.challenge
            || request.challenge.is_nil()
            || &self.node_id != peer.node_id()
            || self.member_incarnation != peer.member_incarnation()
            || self.store_open_id.is_nil()
            || self.native_birth_id.is_nil()
            || self.native_database_incarnation.is_empty()
            || self.native_database_incarnation.len() > 128
            || self.native_history_generation.is_nil()
            || self.native_global_image_generation.is_nil()
            || self.native_range_image_generation.is_nil()
            || [
                &self.native_birth_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
                &self.native_receipt_sha256,
                &self.catalogue_sha256,
            ]
            .into_iter()
            .any(|s| !hash(s))
            || self
                .native_serving_record_commitment_sha256
                .as_deref()
                .is_some_and(|s| !hash(s))
        {
            return Err(invalid(
                "read facts are malformed or not bound to this actual request/peer",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct NativeRangeReadObservation {
    reply: NativeRangeReadProbeReply,
    peer: MetadataReadParticipant,
    cancellation: CancellationToken,
    local: Option<LocalNativeRangeReadObservation>,
}
impl NativeRangeReadObservation {
    pub(crate) fn from_actual_service(
        evidence: crate::distribution_data::ActualServiceNativeReadProbeEvidence,
        peer: MetadataReadParticipant,
    ) -> Result<Self> {
        let (request, local, cancellation) = evidence.into_parts();
        cancellation.check()?;
        local.require_lifetime()?;
        local.facts().require_request(&request, &peer)?;
        Ok(Self {
            reply: NativeRangeReadProbeReply {
                business_request_id: request.business_request_id,
                requested_metadata_context_id: request.requested_metadata_context_id,
                facts: local.facts().clone(),
            },
            peer,
            cancellation,
            local: Some(local),
        })
    }
    pub(crate) fn from_transport(
        authenticated: crate::distribution_transport::AuthenticatedNativeRangeReadReply,
        request: &NativeRangeReadProbeRequest,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let (reply, peer) = authenticated.into_parts();
        reply.require_request(request, &peer)?;
        Ok(Self {
            reply,
            peer,
            cancellation,
            local: None,
        })
    }
    pub(crate) fn facts(&self) -> &NativeRangeReadFacts {
        &self.reply.facts
    }
    pub(super) fn require_request(&self, request: &NativeRangeReadProbeRequest) -> Result<()> {
        self.reply.require_request(request, &self.peer)
    }
    pub(crate) fn authenticated_peer(&self) -> &MetadataReadParticipant {
        &self.peer
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        if let Some(local) = &self.local {
            local.require_lifetime()?;
        }
        Ok(())
    }
    pub(crate) fn into_reply(self) -> Result<NativeRangeReadProbeReply> {
        self.require_live()?;
        Ok(self.reply)
    }
    pub(super) fn local(&self) -> Option<&LocalNativeRangeReadObservation> {
        self.local.as_ref()
    }
}
