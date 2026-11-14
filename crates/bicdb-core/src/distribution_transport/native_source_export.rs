//! Two distinct native-pinned mTLS hops. A decoded reply never constructs
//! LocalNEW, a role, quorum, Open or serving admission.
use super::*;
use crate::distribution_range_handoff::{NativeJointHandoffPlan, NativeRemoteOldCutCertificate};
use crate::distribution_range_image::{
    LocalRangeImageSealV1, NativeObserveRegisteredNewRequestV1, NativeRegisteredNewExportReplyV1,
    NativeRemoteImageManifestV1, NativeSourceExportReplyV1, NativeSourceExportRequestV1,
};

/// Only the actual pinned transport constructs this observation. The donor
/// must still prove its own original source/OLD/MStore before dropping a pin.
pub(crate) struct AuthenticatedRegisteredExportNew {
    reply: NativeRegisteredNewExportReplyV1,
    request: CancellationToken,
}
impl AuthenticatedRegisteredExportNew {
    /// Memory-only liveness of THIS actual authenticated callback request.
    /// It conveys no OLD/native/metadata authority and performs no IO.
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.request.check()
    }
    #[cfg(test)]
    pub(crate) fn test_request_liveness(&self) -> serde_json::Value {
        serde_json::json!({"cancel_requested":self.request.is_cancel_requested(),
            "remaining_us":self.request.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
            "exact_error":self.require_request_live().err().map(|e|format!("{e:?}"))})
    }
    #[cfg(test)]
    pub(crate) fn test_cancel_original_request(&self) {
        self.request.cancel();
    }
    #[cfg(test)]
    pub(crate) fn test_original_request_deadline(&self) -> Option<Instant> {
        self.request.deadline()
    }
    pub(crate) fn reply(&self) -> &NativeRegisteredNewExportReplyV1 {
        &self.reply
    }
    pub(crate) fn require_source(
        &self,
        manifest: &NativeRemoteImageManifestV1,
        old: &NativeRemoteOldCutCertificate,
        seal: &LocalRangeImageSealV1,
    ) -> Result<()> {
        self.require_request_live()?;
        old.require_current()?;
        self.reply.require_manifest(manifest)?;
        self.reply.request.binding.require_plan(&old.plan())?;
        if manifest.old_quorum_sha256 != old.checksum_sha256()?
            || &manifest.source_id != old.selected_source_id()
            || self.reply.materialized.image_root_sha256.as_deref()
                != Some(seal.image_root_sha256.as_str())
            || self.reply.record_count != seal.record_count
            || self.reply.serialized_record_bytes != seal.serialized_record_bytes
            || seal.range_index != manifest.range_index
            || seal.logical_genesis_sha256 != manifest.h0
            || seal.logical_history_sha256 != manifest.h
            || seal.reserved_db_upper_bound != manifest.source_reserved_b
            || seal.source_id != manifest.source_id
            || seal.source_native_receipt_sha256 != manifest.source_native_receipt
        {
            return Err(transport_error(
                "remote export NEW differs from actual donor OLD/completed capture",
            ));
        }
        old.require_current()?;
        self.require_request_live()
    }
}

fn require_client_point(
    transport: &TcpClusterRelocationTransport,
    mf: &NativeRemoteImageManifestV1,
    own: &HostMetadataReadObservation,
    business: Uuid,
    caller: &ClusterNodeId,
) -> Result<()> {
    mf.require_shape()?;
    own.require_live_request(business)?;
    let current = own.committed_topology(business)?;
    let source = current
        .nodes
        .get(&mf.source_id)
        .ok_or_else(|| transport_error("export source absent"))?;
    let target = current
        .nodes
        .get(&mf.target_id)
        .ok_or_else(|| transport_error("export target absent"))?;
    let actor = current
        .nodes
        .get(caller)
        .ok_or_else(|| transport_error("export actual caller absent"))?;
    let relocation = current
        .relocations
        .get(&mf.relocation_id)
        .ok_or_else(|| transport_error("export Pending relocation absent"))?;
    if transport.config.dev_localhost_plaintext
        || transport.caller_node_id != *caller
        || own.requester_node_id() != caller
        || own.requester_incarnation() != actor.incarnation
        || transport.cluster_id != mf.cluster_id
        || current.cluster_id != mf.cluster_id
        || current.range_image_bootstrap_id != Some(mf.bootstrap_id)
        || source.lifecycle != ClusterNodeLifecycle::Active
        || target.lifecycle != ClusterNodeLifecycle::Active
        || source.native_store_birth_v1.as_ref() != Some(&mf.source_birth)
        || target.native_store_birth_v1.as_ref() != Some(&mf.target_birth)
        || current.range_by_id(mf.capture_range.id).is_none_or(|r| {
            crate::distribution_native_read::canonical_range(r)
                != crate::distribution_native_read::canonical_range(&mf.capture_range)
        })
        || relocation.plan_id != mf.plan_id
        || relocation.target != mf.target_id
        || relocation.range_id != mf.capture_range.id
        || relocation.learner_epoch != mf.capture_range.epoch
        || relocation
            .handoff_session
            .as_ref()
            .is_none_or(|s| s.session_id != mf.handoff_session || s.nonce != mf.nonce)
        || !matches!(
            relocation.phase,
            crate::distribution::RelocationPhase::LearnerAllocated
                | crate::distribution::RelocationPhase::SnapshotCopying
                | crate::distribution::RelocationPhase::CatchingUp
                | crate::distribution::RelocationPhase::ReadyToPromote
        )
    {
        return Err(transport_error(
            "export hop changed actual caller/current Pending/native identity",
        ));
    }
    own.require_live_request(business)
}
impl TcpClusterRelocationTransport {
    #[cfg(test)]
    pub(crate) fn test_send_source_export_request(
        &self,
        request: &NativeSourceExportRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeSourceExportReplyV1> {
        let (response, _, _) = self.call_with_native_destination_cancellable(
            parent,
            &request.binding.source_id,
            ClusterDataRequest::NativeSourceExportRetirement {
                request: request.clone(),
            },
        )?;
        match response {
            ClusterDataResponse::NativeSourceExportRetirement(reply) => Ok(reply),
            other => Err(unexpected_response("test export retirement DATA", other)),
        }
    }
    /// Donor -> ORIGINAL Registered target. No handoff Role is requested.
    pub(crate) fn observe_remote_registered_export_new(
        &self,
        plan: &NativeJointHandoffPlan,
        mf: &NativeRemoteImageManifestV1,
        request: &NativeObserveRegisteredNewRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedRegisteredExportNew> {
        request.binding.require_manifest(mf)?;
        request.binding.require_plan(plan)?;
        plan.require_source_host_metadata(own, business, &mf.source_id)?;
        require_client_point(self, mf, own, business, &mf.source_id)?;
        let token = request.cancellation(parent)?;
        self.require_numeric_metadata_destination(&mf.target_id, &token)?;
        let (response, peer, birth) = self.call_with_native_destination_cancellable(
            &token,
            &mf.target_id,
            ClusterDataRequest::NativeObserveRegisteredExportNew {
                request: request.clone(),
            },
        )?;
        let current = own.committed_topology(business)?;
        let target = current
            .nodes
            .get(&mf.target_id)
            .ok_or_else(|| transport_error("export target disappeared"))?;
        if peer.node_id() != &mf.target_id
            || peer.member_incarnation() != target.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|p| !target.accepts_tls_certificate_sha256(p))
            || birth.as_ref() != Some(&mf.target_birth)
        {
            return Err(transport_error(
                "export NEW actual TLS/native target differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeObserveRegisteredExportNew(reply) => reply,
            other => return Err(unexpected_response("Registered export NEW", other)),
        };
        reply.require_manifest(mf)?;
        if reply.request != *request {
            return Err(transport_error(
                "export NEW exact request/challenge differs",
            ));
        }
        if reply.materialized.metadata_term < own.term()
            || reply.materialized.metadata_read_index < own.read_index()
        {
            return Err(transport_error(
                "export NEW actual metadata observation regressed",
            ));
        }
        plan.require_source_host_metadata(own, business, &mf.source_id)?;
        token.check()?;
        Ok(AuthenticatedRegisteredExportNew {
            reply,
            request: token,
        })
    }

    /// Original target -> retained donor. Returned historical DATA can never
    /// itself retire source OLD custody, activate a target or grant Serving.
    pub(crate) fn request_remote_source_export_retirement(
        &self,
        mf: &NativeRemoteImageManifestV1,
        request: &NativeSourceExportRequestV1,
        own: &HostMetadataReadObservation,
        parent: &CancellationToken,
    ) -> Result<NativeSourceExportReplyV1> {
        request.binding.require_manifest(mf)?;
        require_client_point(self, mf, own, request.business, &mf.target_id)?;
        let token = request.cancellation(parent)?;
        self.require_numeric_metadata_destination(&mf.source_id, &token)?;
        let (response, peer, birth) = self.call_with_native_destination_cancellable(
            &token,
            &mf.source_id,
            ClusterDataRequest::NativeSourceExportRetirement {
                request: request.clone(),
            },
        )?;
        let topology = own.committed_topology(request.business)?;
        let source = topology
            .nodes
            .get(&mf.source_id)
            .ok_or_else(|| transport_error("retirement source disappeared"))?;
        if peer.node_id() != &mf.source_id
            || peer.member_incarnation() != source.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|p| !source.accepts_tls_certificate_sha256(p))
            || birth.as_ref() != Some(&mf.source_birth)
        {
            return Err(transport_error(
                "retirement actual TLS/native donor differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeSourceExportRetirement(reply) => reply,
            other => return Err(unexpected_response("source export retirement", other)),
        };
        if reply.request != *request
            || reply.retired.binding != request.binding
            || reply.checksum_sha256 != reply.checksum()?
            || reply.retired.target_observation_sha256.len() != 64
            || reply.retired.source_metadata_store != mf.source_metadata_store_id
            || reply.retired.source_metadata_context.is_nil()
            || reply.source_metadata_context.is_nil()
            || reply.source_metadata_term < own.term()
            || reply.source_metadata_index < own.read_index()
        {
            return Err(transport_error(
                "retirement exact reply/native MStore correlation differs",
            ));
        }
        require_client_point(self, mf, own, request.business, &mf.target_id)?;
        token.check()?;
        Ok(reply)
    }
}
