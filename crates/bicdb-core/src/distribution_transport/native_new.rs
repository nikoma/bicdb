//! Pinned mTLS NEW observations. Only an actual transport exchange constructs
//! this opaque remote fact; decoded DATA is never local NEW/Role/quorum.
use super::*;
use crate::distribution_range_handoff::{
    NativeRemoteNewFactSetV1, NativeRemoteNewOperationV1, NativeRemoteNewReplyV1,
    NativeRemoteNewRequestV1,
};

/// Recipient-independent callbacks, not a decoded quorum or local owner. The
/// actual request tokens remain held through the participant's final sink.
pub(crate) struct AuthenticatedNativeRemoteNewFactSet {
    facts: NativeRemoteNewFactSetV1,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteNewFrame>,
    actual_observer: ClusterNodeId,
}
impl AuthenticatedNativeRemoteNewFactSet {
    pub(crate) fn retained_encoded_bytes(&self) -> Result<usize> {
        self.frames.values().try_fold(0usize, |sum, frame| {
            let bytes =
                crate::distribution_range_image::bounded_json(frame.reply(), 4 * 1024 * 1024)?;
            sum.checked_add(bytes.len())
                .ok_or_else(|| transport_error("remote NEW retained frame byte count overflow"))
        })
    }
    pub(crate) fn require_request_live(&self) -> Result<()> {
        for frame in self.frames.values() {
            frame.require_request_live()?;
        }
        Ok(())
    }
    pub(crate) fn decision_materialized_facts(
        &self,
    ) -> Result<
        BTreeMap<ClusterNodeId, crate::distribution_range_handoff::NativeJointMaterializedFacts>,
    > {
        self.require_request_live()?;
        self.frames
            .iter()
            .map(|(id, frame)| {
                let reply = frame.reply();
                let handoff = reply.handoff.as_ref().ok_or_else(|| {
                    transport_error("remote DECISION requires actual participant pin handoff")
                })?;
                if handoff.fact_set_sha256 != self.facts.checksum_sha256
                    || !handoff.original_freeze_retained
                {
                    return Err(transport_error(
                        "remote DECISION changed actual participant handoff intent",
                    ));
                }
                Ok((
                    id.clone(),
                    reply
                        .snapshot
                        .new_materialized
                        .clone()
                        .ok_or_else(|| transport_error("remote DECISION missing actual NEW"))?,
                ))
            })
            .collect()
    }
    pub(crate) fn facts(&self) -> &NativeRemoteNewFactSetV1 {
        &self.facts
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        &self.actual_observer
    }
}

/// Only an actual native-pinned donor exchange constructs this distinct fact.
/// The donor reads its ORIGINAL retained certificate, not caller MF constraints.
pub(crate) struct AuthenticatedNativeRemoteDecisionSource {
    actual: super::native_source_discovery::AuthenticatedNativeSourceDiscovery,
}
impl AuthenticatedNativeRemoteDecisionSource {
    pub(crate) fn historical_provenance(
        &self,
    ) -> &crate::distribution_range_handoff::NativeRemoteDecisionSourceData {
        self.actual
            .reply()
            .decision_provenance
            .as_ref()
            .expect("actual transport constructor requires source provenance")
    }
    pub(crate) fn provenance_sha256(&self) -> &str {
        &self
            .actual
            .reply()
            .decision_provenance
            .as_ref()
            .expect("actual transport constructor requires source provenance")
            .checksum_sha256
    }

    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.actual.require_request_live()
    }
    pub(crate) fn data(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
    ) -> Result<&crate::distribution_range_handoff::NativeRemoteDecisionSourceData> {
        self.require_request_live()?;
        if self.actual.reply().manifest.checksum_sha256 != manifest.checksum_sha256 {
            return Err(transport_error(
                "remote decision source returned another original MF",
            ));
        }
        let data = self
            .actual
            .reply()
            .decision_provenance
            .as_ref()
            .ok_or_else(|| {
                transport_error("remote decision source omitted original constraints")
            })?;
        data.require_manifest(manifest)?;
        Ok(data)
    }
    pub(crate) fn retained_encoded_bytes(&self) -> Result<usize> {
        Ok(
            crate::distribution_range_image::bounded_json(self.actual.reply(), 4 * 1024 * 1024)?
                .len(),
        )
    }
}
pub(crate) struct AuthenticatedNativeRemoteNewFrame {
    reply: NativeRemoteNewReplyV1,
    original_request: CancellationToken,
    actual_observer: ClusterNodeId,
}
/// Genuine original target->source Describe result. This proves only that
/// the exact S2 export reference already retired; it grants no OLD release.
pub(crate) struct AuthenticatedNativeSourceExportTerminal {
    reply: crate::distribution_range_image::NativeSourceExportReplyV1,
    original_request: CancellationToken,
}
impl AuthenticatedNativeSourceExportTerminal {
    pub(crate) fn retained_encoded_bytes(&self) -> Result<usize> {
        Ok(crate::distribution_range_image::bounded_json(&self.reply, 64 * 1024)?.len())
    }
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn require_manifest(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
    ) -> Result<()> {
        self.require_request_live()?;
        self.reply.retired.binding.require_manifest(manifest)
    }
}
impl AuthenticatedNativeRemoteNewFrame {
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn reply(&self) -> &NativeRemoteNewReplyV1 {
        &self.reply
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        &self.actual_observer
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn fetch_remote_decision_source(
        &self,
        plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteDecisionSource> {
        let remaining = parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let request = crate::distribution_range_image::NativeSourceDiscoveryRequestV1::from_plan(
            plan,
            manifest.source_id.clone(),
            manifest.target_id.clone(),
            manifest.target_attempt,
            crate::distribution_range_image::NativeSourceDiscoveryOperationV1::DecisionProvenance,
            own,
            business,
            remaining.as_millis().clamp(1, 4000) as u64,
        )?;
        let actual = self.request_remote_image_source_discovery(plan, &request, own, parent)?;
        let result = AuthenticatedNativeRemoteDecisionSource { actual };
        result.data(manifest)?;
        Ok(result)
    }
    pub(crate) fn observe_remote_source_export_terminal(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeSourceExportTerminal> {
        let remaining = parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let request = crate::distribution_range_image::NativeSourceExportRequestV1::from_manifest(
            manifest,
            crate::distribution_range_image::NativeSourceExportOperationV1::Describe,
            own,
            business,
            remaining.as_millis().clamp(1, 4000) as u64,
        )?;
        let token = request.cancellation(parent)?;
        // No decoded-DATA constructor: only THIS actual pinned exchange can
        // produce the observation consumed by target pin handoff.
        let reply =
            self.request_remote_source_export_retirement(manifest, &request, own, &token)?;
        token.check()?;
        Ok(AuthenticatedNativeSourceExportTerminal {
            reply,
            original_request: token,
        })
    }
    /// Every participant performs these real authenticated callbacks itself.
    /// All network work must precede source/table/service/DB/native guards.
    pub(crate) fn fetch_remote_native_new_fact_set(
        &self,
        request: &NativeRemoteNewRequestV1,
        own: &HostMetadataReadObservation,
        own_business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteNewFactSet> {
        request.require_shape()?;
        let (NativeRemoteNewOperationV1::PrepareDecision { facts }
        | NativeRemoteNewOperationV1::PublishDecision { facts }) = &request.operation
        else {
            return Err(transport_error(
                "remote NEW callback set requires exact prepare intent",
            ));
        };
        let mut frames = BTreeMap::new();
        for (recipient, expected) in &facts.members {
            parent.check()?;
            let remaining = parent
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .ok_or(BicDbError::QueryTimedOut)?;
            let callback = NativeRemoteNewRequestV1 {
                version: 1,
                manifest: request.manifest.clone(),
                phase_id: request.phase_id,
                operation: NativeRemoteNewOperationV1::Observe,
                business: Uuid::now_v7(),
                challenge: Uuid::now_v7(),
                budget_ms: remaining.as_millis().clamp(1, 4000) as u64,
            };
            let actual =
                self.request_remote_native_new(recipient, &callback, own, own_business, parent)?;
            if actual.reply.snapshot.checksum_sha256 != *expected
                || actual.reply.snapshot.new_materialized.is_none()
                || actual.actual_observer != self.caller_node_id
            {
                return Err(transport_error(
                    "remote NEW callback differs from exact immutable fact-set intent",
                ));
            }
            frames.insert(recipient.clone(), actual);
        }
        let result = AuthenticatedNativeRemoteNewFactSet {
            facts: facts.clone(),
            frames,
            actual_observer: self.caller_node_id.clone(),
        };
        result.require_request_live()?;
        Ok(result)
    }
    pub(crate) fn request_remote_native_new(
        &self,
        recipient: &ClusterNodeId,
        request: &NativeRemoteNewRequestV1,
        own: &HostMetadataReadObservation,
        own_business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteNewFrame> {
        request.require_shape()?;
        let token = request.cancellation(parent)?;
        super::native_source_export::require_client_point(
            self,
            &request.manifest,
            own,
            own_business,
            &self.caller_node_id,
        )?;
        let current = own.committed_topology(own_business)?;
        let member = current
            .nodes
            .get(recipient)
            .ok_or_else(|| transport_error("remote NEW recipient absent"))?;
        let caller = current
            .nodes
            .get(&self.caller_node_id)
            .ok_or_else(|| transport_error("remote NEW actual caller absent"))?;
        let birth = member
            .native_store_birth_v1
            .as_ref()
            .ok_or_else(|| transport_error("remote NEW recipient has no enrolled native birth"))?;
        let mf = &request.manifest;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || caller.lifecycle != ClusterNodeLifecycle::Active
            || (!mf.old_members.contains_key(recipient) && !mf.new_members.contains_key(recipient))
            || (recipient == &mf.target_id && birth != &mf.target_birth)
            || (recipient == &mf.source_id && birth != &mf.source_birth)
            || mf.actual_old_closures.get(recipient).is_some_and(|old| {
                old.birth_id != birth.birth_id() || old.birth_sha256 != birth.birth_sha256()
            })
        {
            return Err(transport_error(
                "remote NEW destination changes fixed participant/native birth",
            ));
        }
        self.require_numeric_metadata_destination(recipient, &token)?;
        let (response, peer, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            recipient,
            ClusterDataRequest::NativeRemoteNew {
                request: request.clone(),
            },
        )?;
        token.check()?;
        own.require_live_request(own_business)?;
        if peer.node_id() != recipient
            || peer.member_incarnation() != member.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !member.accepts_tls_certificate_sha256(pin))
            || actual_birth.as_ref() != Some(birth)
        {
            return Err(transport_error(
                "remote NEW actual TLS/native recipient differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeRemoteNew(reply) => reply,
            other => return Err(unexpected_response("native remote NEW", other)),
        };
        reply.require_request(request)?;
        if reply.snapshot.node_id != *recipient
            || reply.snapshot.birth != *birth
            || reply.metadata_term < own.term()
            || reply.metadata_read_index < own.read_index()
        {
            return Err(transport_error(
                "remote NEW actual response/metadata identity regressed",
            ));
        }
        if let NativeRemoteNewOperationV1::PrepareDecision { facts }
        | NativeRemoteNewOperationV1::PublishDecision { facts } = &request.operation
        {
            if reply.handoff.as_ref().is_none_or(|handoff| {
                handoff.fact_set_sha256 != facts.checksum_sha256
                    || !handoff.original_freeze_retained
            }) {
                return Err(transport_error(
                    "remote NEW preparation omitted same original handoff",
                ));
            }
        }
        token.check()?;
        own.require_live_request(own_business)?;
        Ok(AuthenticatedNativeRemoteNewFrame {
            reply,
            original_request: token,
            actual_observer: self.caller_node_id.clone(),
        })
    }
}
