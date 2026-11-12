//! Host metadata observations use the actual authenticated transport, not an
//! enrollment DTO or a recovery-role/freeze request.
use super::*;

#[derive(Debug)]
pub(crate) struct AuthenticatedHostMetadataReadReply {
    response: HostMetadataReadAcquireResponse,
    peer: MetadataReadParticipant,
    request: HostMetadataReadAcquireRequest,
    cluster_id: ClusterId,
    destination: ClusterNodeId,
    started: Instant,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl AuthenticatedHostMetadataReadReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        HostMetadataReadAcquireResponse,
        MetadataReadParticipant,
        HostMetadataReadAcquireRequest,
        ClusterId,
        ClusterNodeId,
        Instant,
        Instant,
        CancellationToken,
    ) {
        (
            self.response,
            self.peer,
            self.request,
            self.cluster_id,
            self.destination,
            self.started,
            self.deadline,
            self.cancellation,
        )
    }
}

impl TcpClusterRelocationTransport {
    /// An actual authenticated exchange. Request labels remain untrusted;
    /// the receiving provider validates the real TLS participant separately.
    pub(crate) fn request_host_metadata_reply(
        &self,
        destination: &ClusterNodeId,
        request: &HostMetadataReadAcquireRequest,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedHostMetadataReadReply> {
        request.validate()?;
        parent.check()?;
        let started = Instant::now();
        let token = parent.child_with_deadline(started + Duration::from_millis(request.budget_ms));
        let deadline = token.deadline().expect("bounded host metadata request");
        self.require_numeric_metadata_destination(destination, &token)?;
        let (response, peer) = self.call_with_read_participant_cancellable(
            &token,
            destination,
            ClusterDataRequest::HostMetadataReadAcquire {
                request: request.clone(),
            },
        )?;
        token.check()?;
        match response {
            ClusterDataResponse::HostMetadataReadAcquire(response) => {
                Ok(AuthenticatedHostMetadataReadReply {
                    response,
                    peer,
                    request: request.clone(),
                    cluster_id: self.cluster_id.clone(),
                    destination: destination.clone(),
                    started,
                    deadline,
                    cancellation: token,
                })
            }
            other => Err(unexpected_response("host metadata acquisition", other)),
        }
    }

    pub(crate) fn acquire_host_metadata_observation(
        &self,
        metadata_leader: &ClusterNodeId,
        business: Uuid,
        budget: Duration,
        parent: &CancellationToken,
    ) -> Result<HostMetadataReadObservation> {
        if business.is_nil() || budget.is_zero() || budget > Duration::from_secs(5) {
            return Err(transport_error(
                "host metadata acquisition needs a nonnil business and at most five seconds",
            ));
        }
        let started = Instant::now();
        let token = parent.child_with_deadline(started + budget);
        token.check()?;
        let deadline = token.deadline().expect("bounded host metadata request");
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| transport_error("host metadata acquisition expired before request"))?;
        let request = HostMetadataReadAcquireRequest {
            protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            acquisition_id: Uuid::now_v7(),
            read_request_id: business,
            requester_node_id: self.caller_node_id.clone(),
            requester_incarnation: self.caller_incarnation,
            budget_ms: u64::try_from(remaining.as_millis())
                .unwrap_or(5000)
                .clamp(1, 5000),
        };
        let reply = self.request_host_metadata_reply(metadata_leader, &request, &token)?;
        token.check()?;
        HostMetadataReadObservation::from_transport(reply, &request)
    }
}
