//! Authenticated observations only. Neither facts nor this sealed wrapper
//! establish admitted voter continuity, a read permit, or an MVCC snapshot.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;

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
    facts: NativeRangeReadFacts,
    peer: MetadataReadParticipant,
    cancellation: CancellationToken,
    local: Option<LocalNativeRangeReadObservation>,
}
impl NativeRangeReadObservation {
    pub(crate) fn from_actual_local(
        local: LocalNativeRangeReadObservation,
        peer: MetadataReadParticipant,
        request: &NativeRangeReadRequest,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let cancellation = request.cancellation(parent)?;
        local.require_lifetime()?;
        local.facts().require_request(request, &peer)?;
        Ok(Self {
            facts: local.facts().clone(),
            peer,
            cancellation,
            local: Some(local),
        })
    }
    pub(crate) fn from_transport(
        authenticated: crate::distribution_transport::AuthenticatedNativeRangeReadReply,
        request: &NativeRangeReadRequest,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let (facts, peer) = authenticated.into_parts();
        facts.require_request(request, &peer)?;
        Ok(Self {
            facts,
            peer,
            cancellation,
            local: None,
        })
    }
    pub(crate) fn facts(&self) -> &NativeRangeReadFacts {
        &self.facts
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
    pub(crate) fn into_facts(self) -> Result<NativeRangeReadFacts> {
        self.require_live()?;
        Ok(self.facts)
    }
    pub(super) fn local(&self) -> Option<&LocalNativeRangeReadObservation> {
        self.local.as_ref()
    }
}
