//! Budgeted native recertification; actual socket/identity envelope only.
use super::*;
use crate::distribution_native_apply::{
    NativeRecertifyObservation, NativeRecertifyReply, NativeRecertifyRequest,
};

pub(crate) struct AuthenticatedNativeRecertifyReply {
    reply: NativeRecertifyReply,
    peer: MetadataReadParticipant,
}
impl AuthenticatedNativeRecertifyReply {
    pub(crate) fn into_parts(self) -> (NativeRecertifyReply, MetadataReadParticipant) {
        (self.reply, self.peer)
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn recertify_native_replica_bounded(
        &self,
        destination: &ClusterNodeId,
        request: &NativeRecertifyRequest,
        parent: &CancellationToken,
    ) -> Result<NativeRecertifyObservation> {
        let token = request.token(parent)?;
        let payload = ClusterDataRequest::RecertifyNativeOrigin {
            request: request.clone(),
        };
        crate::distribution_range_consensus::validate_native_json_size(
            &ClusterDataRequestEnvelope {
                expected_native_destination: None,
                protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
                cluster_id: self.cluster_id.clone(),
                caller_node_id: self.caller_node_id.clone(),
                destination_node_id: destination.clone(),
                request_id: u64::MAX,
                request: &payload,
            },
            self.config.max_frame_bytes,
        )?;
        let result =
            self.call_with_native_destination_peer_transport(&token, destination, payload, true);
        parent.check()?;
        let (response, peer, _) = result?;
        match response {
            ClusterDataResponse::NativeRecertify(reply) => {
                NativeRecertifyObservation::from_transport(
                    AuthenticatedNativeRecertifyReply { reply, peer },
                    request,
                    &token,
                    token.deadline().unwrap(),
                )
            }
            other => Err(unexpected_response(
                "actual bounded native recertification",
                other,
            )),
        }
    }
}

impl TcpClusterRelocationTransport {
    pub(crate) fn observe_hot_native_replica(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_native_apply::NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        let token = request.token(parent)?;
        self.hot_native_replica_reply(
            destination,
            ClusterDataRequest::ObserveNativeOrigin {
                request: request.clone(),
            },
            request.bootstrap_id,
            request.range_id,
            request.range_epoch,
            request.challenge,
            &token,
            parent,
        )
    }
    pub(crate) fn apply_hot_native_replica(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
        certificate: &crate::distribution_range_consensus::NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        let request = crate::distribution_native_apply::NativeOriginApplyRequest::from_certificate(
            command,
            certificate,
            parent,
        )?;
        let token = request.token(parent)?;
        self.hot_native_replica_reply(
            destination,
            ClusterDataRequest::ApplyNativeOrigin {
                request: request.clone(),
            },
            request.bootstrap_id,
            command.range_id,
            command.range_epoch,
            request.challenge,
            &token,
            parent,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn hot_native_replica_reply(
        &self,
        destination: &ClusterNodeId,
        payload: ClusterDataRequest,
        bootstrap_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        challenge: Uuid,
        token: &CancellationToken,
        original: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        crate::distribution_range_consensus::validate_native_json_size(
            &ClusterDataRequestEnvelope {
                expected_native_destination: None,
                protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
                cluster_id: self.cluster_id.clone(),
                caller_node_id: self.caller_node_id.clone(),
                destination_node_id: destination.clone(),
                request_id: u64::MAX,
                request: &payload,
            },
            self.config.max_frame_bytes,
        )?;
        let result =
            self.call_with_native_destination_peer_transport(token, destination, payload, true);
        original.check()?;
        let (response, peer, _) = result?;
        match response {
            ClusterDataResponse::NativeReplica(reply) => {
                if &reply.node_id != destination
                    || peer.node_id() != destination
                    || peer.member_incarnation() != reply.member_incarnation
                    || reply.bootstrap_id != bootstrap_id
                    || reply.range_id != range_id
                    || reply.range_epoch != range_epoch
                    || reply.challenge != challenge
                {
                    return Err(transport_error(
                        "hot replica actual response identity/domain/challenge differs",
                    ));
                }
                token.check()?;
                crate::distribution_native_apply::NativeReplicaApplyObservation::from_transport(
                    AuthenticatedNativeReplicaReply { reply },
                )
            }
            other => Err(unexpected_response(
                "actual hot replica status/materialization",
                other,
            )),
        }
    }
}

#[cfg(test)]
impl TcpClusterRelocationTransport {
    /// Actual mTLS old-wire refusal only. No DTO here can mint a vote/role.
    pub(crate) fn test_previous_hot_protocol_refusal(
        &self,
        destination: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<String> {
        parent.check()?;
        let (address, active, pending, birth) = {
            let topology = self.topology.read_recursive();
            let node = topology
                .nodes
                .get(destination)
                .ok_or_else(|| transport_error("actual test target missing"))?;
            (
                node.address.clone(),
                node.tls_certificate_sha256.clone(),
                node.pending_tls_certificate_sha256.clone(),
                node.native_store_birth_v1.clone(),
            )
        };
        let end = parent
            .deadline()
            .ok_or_else(|| transport_error("old-wire test needs original finite deadline"))?;
        let address: SocketAddr = address
            .parse()
            .map_err(|_| transport_error("old-wire actual endpoint not numeric"))?;
        let remaining = end
            .checked_duration_since(Instant::now())
            .ok_or(BicDbError::QueryTimedOut)?;
        let socket = TcpStream::connect_timeout(
            &address,
            remaining.min(Duration::from_millis(self.config.connect_timeout_ms)),
        )?;
        configure_stream(&socket, &self.config)?;
        let tls = self
            .client_tls
            .as_ref()
            .ok_or_else(|| transport_error("old-wire control requires actual TLS"))?;
        let name = server_name_from_address(&address.to_string())?;
        let raw = client_tls_stream(Arc::clone(&tls.config), &name, socket)?;
        let mut stream = StreamOwned::new(
            raw.conn,
            DeadlineTcpStream::new(raw.sock, parent.clone(), end),
        );
        let certificate = complete_client_tls_handshake(&mut stream)?;
        verify_destination_certificate(
            destination,
            active.as_deref(),
            pending.as_deref(),
            Some(&certificate),
        )?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = ClusterDataRequestEnvelope {
            expected_native_destination: None,
            protocol_version: CLUSTER_DATA_PROTOCOL_VERSION - 1,
            cluster_id: self.cluster_id.clone(),
            caller_node_id: self.caller_node_id.clone(),
            destination_node_id: destination.clone(),
            request_id,
            request: ClusterDataRequest::Ping,
        };
        send_json_frame(&mut stream, &request, self.config.max_frame_bytes)?;
        let reply: ClusterDataResponseEnvelope =
            recv_json_frame(&mut stream, self.config.max_frame_bytes)?;
        parent.check()?;
        if reply.protocol_version != CLUSTER_DATA_PROTOCOL_VERSION
            || reply.cluster_id != self.cluster_id
            || reply.source_node_id != *destination
            || reply.request_id != request_id
            || reply.actual_native_source != birth
        {
            return Err(transport_error(
                "actual old-wire refusal has wrong receiver/request/birth identity",
            ));
        }
        match reply.response {
            Err(actual) => Ok(actual.message),
            Ok(_) => Err(transport_error("previous wire was unexpectedly accepted")),
        }
    }
}
