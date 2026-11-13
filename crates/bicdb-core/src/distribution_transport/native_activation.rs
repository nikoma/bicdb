//! Authenticated delivery of the SAME actual local activation producer. A
//! decoded completion reply remains evidence; it is never recovered permission.
use super::*;
impl TcpClusterRelocationTransport {
    pub(crate) fn activate_native_recovery_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeCompletionObservation> {
        let token = request.cancellation(parent)?;
        let payload = ClusterDataRequest::ActivateNativeRecovery {
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
        let (response, peer) =
            self.call_with_read_participant_cancellable(&token, destination, payload)?;
        token.check()?;
        match response {
            ClusterDataResponse::NativeCompletion(reply) => {
                crate::distribution_range_recovery::NativeCompletionObservation::from_transport(
                    AuthenticatedNativeCompletionReply { reply, peer },
                    request,
                    token.deadline().unwrap(),
                )
            }
            response => Err(unexpected_response("actual native Activate/open", response)),
        }
    }
}
