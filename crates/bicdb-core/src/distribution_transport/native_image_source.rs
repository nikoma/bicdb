//! Actual pinned-TLS source observation, not a reconstructed local lineage.
//! Normal per-request EOF after a complete reply has no revocation meaning.
use super::*;
use crate::distribution_range_image::{
    NativeRemoteImageManifestV1, NativeRemoteImageSourceOperationV1,
    NativeRemoteImageSourcePayloadV1, NativeRemoteImageSourceReplyV1,
    NativeRemoteImageSourceRequestV1,
};

/// Only the real client exchange mints this wrapper. Original per-step request
/// lifetime is retained separately from the reply's immutable historical data.
/// No public fields, Clone/Serde, DTO constructor, target row or Serving grant.
pub(crate) struct AuthenticatedNativeRemoteImageSourceFrame {
    reply: NativeRemoteImageSourceReplyV1,
    manifest: NativeRemoteImageManifestV1,
    original_request: CancellationToken,
}
impl AuthenticatedNativeRemoteImageSourceFrame {
    #[cfg(test)]
    pub(crate) fn test_original_request_cancellation(&self) -> CancellationToken {
        self.original_request.clone()
    }
    pub(crate) fn retain_original_request_cancellation(&self) -> Result<CancellationToken> {
        self.require_request_live()?;
        Ok(self.original_request.clone())
    }
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.original_request.check()
    }
    pub(crate) fn reply(&self) -> &NativeRemoteImageSourceReplyV1 {
        &self.reply
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.manifest
    }
}
impl TcpClusterRelocationTransport {
    /// OWN metadata is acquired by the target outside its engine locks. This
    /// method authenticates historical source DATA; it never casts that HostMQ
    /// into a mutation role or asserts continuous remote pin/process liveness.
    pub(crate) fn request_remote_image_source_frame(
        &self,
        expected: &NativeRemoteImageManifestV1,
        request: &NativeRemoteImageSourceRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteImageSourceFrame> {
        expected.require_shape()?;
        let token = request.cancellation(parent)?;
        own.require_live_request(business)?;
        let current = own.committed_topology(business)?;
        let source = current
            .nodes
            .get(&expected.source_id)
            .ok_or_else(|| transport_error("source image donor absent from current metadata"))?;
        let target = current
            .nodes
            .get(&expected.target_id)
            .ok_or_else(|| transport_error("source image target absent from current metadata"))?;
        if request.business != business
            || request.source_session != expected.source_session
            || request.target_attempt != expected.target_attempt
            || request.manifest_sha256 != expected.checksum_sha256
            || self.cluster_id != expected.cluster_id
            || current.cluster_id != expected.cluster_id
            || current.range_image_bootstrap_id != Some(expected.bootstrap_id)
            || own.requester_node_id() != &expected.target_id
            || self.caller_node_id != expected.target_id
            || own.requester_incarnation() != target.incarnation
            || source.lifecycle != ClusterNodeLifecycle::Active
            || target.lifecycle != ClusterNodeLifecycle::Active
            || source.native_store_birth_v1.as_ref() != Some(&expected.source_birth)
            || target.native_store_birth_v1.as_ref() != Some(&expected.target_birth)
            || self.config.dev_localhost_plaintext
        {
            return Err(transport_error(
                "remote image original request/OWN identity/birth differs",
            ));
        }
        self.require_numeric_metadata_destination(&expected.source_id, &token)?;
        let (response, peer, actual_birth) = self.call_with_native_destination_cancellable(
            &token,
            &expected.source_id,
            ClusterDataRequest::NativeRemoteImageSourceFrame {
                request: request.clone(),
            },
        )?;
        token.check()?;
        own.require_live_request(business)?;
        if peer.node_id() != &expected.source_id
            || peer.member_incarnation() != source.incarnation
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !source.accepts_tls_certificate_sha256(pin))
            || actual_birth.as_ref() != Some(&expected.source_birth)
        {
            return Err(transport_error(
                "remote image actual TLS/source-native identity differs",
            ));
        }
        let reply = match response {
            ClusterDataResponse::NativeRemoteImageSourceFrame(value) => value,
            other => return Err(unexpected_response("remote image source frame", other)),
        };
        if reply.request != *request
            || reply.source_id != expected.source_id
            || reply.target_id != expected.target_id
            || reply.source_birth != expected.source_birth
            || reply.manifest_sha256 != expected.checksum_sha256
            || reply.source_metadata_context.is_nil()
            || reply.source_metadata_term < own.term()
            || reply.source_metadata_read_index < own.read_index()
            || reply.checksum()? != reply.checksum_sha256
        {
            return Err(transport_error(
                "remote image source response request/cut/checksum differs",
            ));
        }
        let payload_matches = match (&request.operation, &reply.payload) {
            (
                NativeRemoteImageSourceOperationV1::Manifest,
                NativeRemoteImageSourcePayloadV1::Manifest(value),
            ) => {
                value.checksum_sha256 == expected.checksum_sha256
                    && value.checksum()? == expected.checksum_sha256
            }
            (
                NativeRemoteImageSourceOperationV1::History {
                    cursor,
                    max_entries,
                },
                NativeRemoteImageSourcePayloadV1::History(value),
            ) => {
                value.previous_cursor == *cursor
                    && value.session_id == expected.handoff_session
                    && value.nonce == expected.nonce
                    && value.history.range_id == expected.capture_range.id
                    && value.history.source_range_index == expected.range_index
                    && value.history.source_native_receipt_sha256 == expected.source_native_receipt
                    && value.history.logical_genesis_sha256 == expected.h0
                    && value.history.logical_history_sha256 == expected.h
                    && value.history.entries.len() <= *max_entries
            }
            (
                NativeRemoteImageSourceOperationV1::Rows { cursor, .. },
                NativeRemoteImageSourcePayloadV1::Rows(value),
            ) => {
                value.previous_cursor == *cursor
                    && value.session_id == expected.handoff_session
                    && value.nonce == expected.nonce
                    && value.snapshot_db_sequence >= expected.source_reserved_b
            }
            (
                NativeRemoteImageSourceOperationV1::Finish,
                NativeRemoteImageSourcePayloadV1::Finished(value),
            ) => {
                value.session_id == expected.handoff_session
                    && value.nonce == expected.nonce
                    && value.cluster_id == expected.cluster_id
                    && value.bootstrap_id == expected.bootstrap_id
                    && value.source_id == expected.source_id
                    && value.range_id == expected.capture_range.id
                    && value.capture_epoch == expected.capture_range.epoch
                    && value.activation_epoch == expected.activation_range.epoch
                    && value.source_database_incarnation == expected.source_database_open
                    && value.source_native_receipt_sha256 == expected.source_native_receipt
                    && value.source_member_incarnation == expected.source_birth.node_incarnation()
                    && value.range_index == expected.range_index
                    && value.logical_genesis_sha256 == expected.h0
                    && value.logical_history_sha256 == expected.h
                    && value.old_config_sha256 == expected.old_config_sha256
                    && value.new_config_sha256 == expected.new_config_sha256
                    && value.schema_sha256 == expected.schema_sha256
                    && value.reader_features == expected.readers
                    && value.local_namespace_ids == expected.source_namespace_ids
                    && value.portable_catalog_sha256 == expected.portable_catalogue_sha256
                    && value.reserved_db_upper_bound == expected.source_reserved_b
            }
            _ => false,
        };
        if !payload_matches {
            return Err(transport_error(
                "remote image source operation/cursor/history/seal differs",
            ));
        }
        // These stamps are correlations from the actual source producer, not
        // a re-sealed metadata QQ/ACK or independent current-authority result.
        token.check()?;
        own.require_live_request(business)?;
        Ok(AuthenticatedNativeRemoteImageSourceFrame {
            reply,
            manifest: expected.clone(),
            original_request: token,
        })
    }
}
