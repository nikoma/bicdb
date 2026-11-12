//! Authenticated initial-factory observations. The wire DTO is never a permit.
use super::*;
use crate::distribution_native_host::{
    NativeInitialBirthOfferObservation, NativeInitialBirthOfferRequest,
    NativeInitialBirthOfferResponse,
};

pub(crate) struct AuthenticatedNativeInitialBirthReply {
    response: NativeInitialBirthOfferResponse,
    original: NativeInitialBirthOfferRequest,
    peer: MetadataReadParticipant,
    cluster: ClusterId,
    destination: ClusterNodeId,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl AuthenticatedNativeInitialBirthReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        NativeInitialBirthOfferResponse,
        NativeInitialBirthOfferRequest,
        MetadataReadParticipant,
        ClusterId,
        ClusterNodeId,
        Instant,
        CancellationToken,
    ) {
        (
            self.response,
            self.original,
            self.peer,
            self.cluster,
            self.destination,
            self.deadline,
            self.cancellation,
        )
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn request_initial_birth_reply(
        &self,
        destination: &ClusterNodeId,
        request: NativeInitialBirthOfferRequest,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeInitialBirthReply> {
        request.validate()?;
        if request.requester != self.caller_node_id
            || request.requester_incarnation != self.caller_incarnation
        {
            return Err(transport_error(
                "initial-birth request is not this actual transport caller",
            ));
        }
        let cancellation =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        cancellation.check()?;
        let deadline = cancellation
            .deadline()
            .expect("bounded initial-birth exchange");
        self.require_numeric_metadata_destination(destination, &cancellation)?;
        let (response, peer) = self.call_with_read_participant_cancellable(
            &cancellation,
            destination,
            ClusterDataRequest::NativeInitialBirthOffer {
                request: request.clone(),
            },
        )?;
        cancellation.check()?;
        match response {
            ClusterDataResponse::NativeInitialBirthOffer(response) => {
                Ok(AuthenticatedNativeInitialBirthReply {
                    response,
                    original: request,
                    peer,
                    cluster: self.cluster_id.clone(),
                    destination: destination.clone(),
                    deadline,
                    cancellation,
                })
            }
            other => Err(unexpected_response("actual initial native birth", other)),
        }
    }
    pub(crate) fn acquire_initial_birth_offer(
        &self,
        destination: &ClusterNodeId,
        plan: &crate::distribution_native_host::NativeClusterBootstrapPlan,
        parent: &CancellationToken,
    ) -> Result<NativeInitialBirthOfferObservation> {
        let request = NativeInitialBirthOfferRequest {
            version: 1,
            challenge: Uuid::now_v7(),
            requester: self.caller_node_id.clone(),
            requester_incarnation: self.caller_incarnation,
            plan_sha256: plan.checksum()?,
            budget_ms: 5000,
        };
        let sealed = self.request_initial_birth_reply(destination, request, parent)?;
        NativeInitialBirthOfferObservation::from_transport(sealed, plan)
    }
}
