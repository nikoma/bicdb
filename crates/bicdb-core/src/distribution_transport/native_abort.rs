//! Sealed request-bound native abort exchanges. Decoded replies are not votes.
use super::*;
use crate::distribution_range_recovery::native_abort::{
    NativeAbortCandidateReply, NativeAbortPromiseReply,
};
use crate::distribution_range_recovery::{
    NativeAbortCandidateObservation, NativeAbortPromiseObservation, NativeAbortRequest,
};

#[derive(Debug)]
pub(crate) struct AuthenticatedNativeAbortCandidateReply {
    reply: NativeAbortCandidateReply,
    original: NativeAbortRequest,
    peer: MetadataReadParticipant,
    token: CancellationToken,
}
impl AuthenticatedNativeAbortCandidateReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        NativeAbortCandidateReply,
        NativeAbortRequest,
        MetadataReadParticipant,
        CancellationToken,
    ) {
        (self.reply, self.original, self.peer, self.token)
    }
}
#[derive(Debug)]
pub(crate) struct AuthenticatedNativeAbortPromiseReply {
    reply: NativeAbortPromiseReply,
    original: NativeAbortRequest,
    peer: MetadataReadParticipant,
    token: CancellationToken,
}
impl AuthenticatedNativeAbortPromiseReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        NativeAbortPromiseReply,
        NativeAbortRequest,
        MetadataReadParticipant,
        CancellationToken,
    ) {
        (self.reply, self.original, self.peer, self.token)
    }
}
pub(super) fn dispatch(
    request: ClusterDataRequest,
    runtime: Option<&Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>>,
    caller: Option<&MetadataReadParticipant>,
    deadline: Instant,
) -> Result<ClusterDataResponse> {
    let runtime = runtime.ok_or_else(|| transport_error("native abort runtime is not active"))?;
    let caller = caller
        .ok_or_else(|| transport_error("native abort requires authenticated current leader"))?;
    let token = CancellationToken::new(Arc::new(AtomicBool::new(false)), Some(deadline));
    match request {
        ClusterDataRequest::ObserveNativeAbortCandidate { request } => runtime
            .observe_native_abort_candidate(caller, &request, &token)
            .map(|o| ClusterDataResponse::NativeAbortCandidate(o.into_reply())),
        ClusterDataRequest::PromiseNativeAbort { request } => runtime
            .promise_native_abort(caller, &request, &token)
            .map(|o| ClusterDataResponse::NativeAbortPromise(o.into_reply())),
        ClusterDataRequest::ObserveNativeAbortPromise { request } => runtime
            .observe_native_abort_promise(caller, &request, &token)
            .map(|o| ClusterDataResponse::NativeAbortPromise(o.into_reply())),
        ClusterDataRequest::ResolveNativeAbort { request } => {
            let receipt = runtime.resolve_native_abort(caller, &request, &token)?;
            Ok(ClusterDataResponse::NativeAbortResolved {
                range_index: receipt.range_index(),
                decision_sha256: receipt.decision_sha256().into(),
                local_data_visibility: receipt.local_data_visibility(),
                request,
            })
        }
        _ => Err(transport_error("wrong native abort dispatch arm")),
    }
}
impl TcpClusterRelocationTransport {
    pub(super) fn native_abort_existing_promise_exchange(
        &self,
        destination: &ClusterNodeId,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortPromiseObservation> {
        let token = request.cancellation(parent)?;
        let (reply, peer) = self.abort_exchange(
            destination,
            request,
            ClusterDataRequest::ObserveNativeAbortPromise {
                request: request.clone(),
            },
            &token,
        )?;
        match reply {
            ClusterDataResponse::NativeAbortPromise(reply) => {
                if &reply.node_id != destination || peer.node_id() != destination {
                    return Err(transport_error(
                        "readonly native AbortPromise peer/destination differs",
                    ));
                }
                NativeAbortPromiseObservation::from_transport(
                    AuthenticatedNativeAbortPromiseReply {
                        reply,
                        original: request.clone(),
                        peer,
                        token,
                    },
                )
            }
            reply => Err(unexpected_response(
                "actual existing native AbortPromise",
                reply,
            )),
        }
    }
    fn abort_exchange(
        &self,
        destination: &ClusterNodeId,
        request: &NativeAbortRequest,
        payload: ClusterDataRequest,
        token: &CancellationToken,
    ) -> Result<(ClusterDataResponse, MetadataReadParticipant)> {
        request.cancellation(token)?.check()?;
        crate::distribution_range_consensus::validate_native_json_size(
            &ClusterDataRequestEnvelope {
                protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
                cluster_id: self.cluster_id.clone(),
                caller_node_id: self.caller_node_id.clone(),
                destination_node_id: destination.clone(),
                // Abort requests are not metadata-destination envelopes; the
                // actual sender adds this binding only for the metadata arms.
                expected_native_destination: None,
                request_id: u64::MAX,
                request: &payload,
            },
            self.config.max_frame_bytes,
        )?;
        self.call_with_read_participant_cancellable(token, destination, payload)
    }
    pub(super) fn native_abort_candidate_exchange(
        &self,
        destination: &ClusterNodeId,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortCandidateObservation> {
        let token = request.cancellation(parent)?;
        let (reply, peer) = self.abort_exchange(
            destination,
            request,
            ClusterDataRequest::ObserveNativeAbortCandidate {
                request: request.clone(),
            },
            &token,
        )?;
        match reply {
            ClusterDataResponse::NativeAbortCandidate(reply) => {
                if &reply.node_id != destination || peer.node_id() != destination {
                    return Err(transport_error(
                        "native abort candidate peer/destination differs",
                    ));
                }
                NativeAbortCandidateObservation::from_transport(
                    AuthenticatedNativeAbortCandidateReply {
                        reply,
                        original: request.clone(),
                        peer,
                        token,
                    },
                )
            }
            reply => Err(unexpected_response("actual native abort candidate", reply)),
        }
    }
    pub(super) fn native_abort_promise_exchange(
        &self,
        destination: &ClusterNodeId,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortPromiseObservation> {
        let token = request.cancellation(parent)?;
        let (reply, peer) = self.abort_exchange(
            destination,
            request,
            ClusterDataRequest::PromiseNativeAbort {
                request: request.clone(),
            },
            &token,
        )?;
        match reply {
            ClusterDataResponse::NativeAbortPromise(reply) => {
                if &reply.node_id != destination || peer.node_id() != destination {
                    return Err(transport_error(
                        "native AbortPromise peer/destination differs",
                    ));
                }
                NativeAbortPromiseObservation::from_transport(
                    AuthenticatedNativeAbortPromiseReply {
                        reply,
                        original: request.clone(),
                        peer,
                        token,
                    },
                )
            }
            reply => Err(unexpected_response("actual native AbortPromise", reply)),
        }
    }
    /// A request to independently resolve; the response is a diagnostic native
    /// receipt projection, never a portable quorum or read/serving authority.
    pub(crate) fn resolve_native_abort_at(
        &self,
        destination: &ClusterNodeId,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<(u64, String, u64)> {
        let token = request.cancellation(parent)?;
        let (reply, peer) = self.abort_exchange(
            destination,
            request,
            ClusterDataRequest::ResolveNativeAbort {
                request: request.clone(),
            },
            &token,
        )?;
        token.check()?;
        match reply {
            ClusterDataResponse::NativeAbortResolved {
                request: echoed,
                range_index,
                decision_sha256,
                local_data_visibility,
            } if echoed == *request
                && peer.node_id() == destination
                && range_index == request.range_index
                && decision_sha256.len() == 64 =>
            {
                Ok((range_index, decision_sha256, local_data_visibility))
            }
            reply => Err(unexpected_response("actual native Abort resolution", reply)),
        }
    }
}
