//! Real TLS envelope for existing-cluster learner FACTS; no authority DTO path.
use super::*;
use crate::distribution_native_host::{
    NativeLearnerBirthObservation, NativeLearnerBirthRequest, NativeLearnerBirthResponse,
};

pub(crate) struct AuthenticatedNativeLearnerBirthReply {
    response: NativeLearnerBirthResponse,
    original: NativeLearnerBirthRequest,
    peer: MetadataReadParticipant,
    cluster: ClusterId,
    destination: ClusterNodeId,
    started: Instant,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl AuthenticatedNativeLearnerBirthReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        NativeLearnerBirthResponse,
        NativeLearnerBirthRequest,
        MetadataReadParticipant,
        ClusterId,
        ClusterNodeId,
        Instant,
        Instant,
        CancellationToken,
    ) {
        (
            self.response,
            self.original,
            self.peer,
            self.cluster,
            self.destination,
            self.started,
            self.deadline,
            self.cancellation,
        )
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn request_learner_birth_reply(
        &self,
        destination: &ClusterNodeId,
        request: NativeLearnerBirthRequest,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeLearnerBirthReply> {
        request.validate()?;
        if request.requester != self.caller_node_id
            || request.requester_incarnation != self.caller_incarnation
            || &request.target != destination
        {
            return Err(transport_error(
                "learner birth request cannot relabel the actual caller/destination",
            ));
        }
        let started = Instant::now();
        let cancellation =
            parent.child_with_deadline(started + Duration::from_millis(request.budget_ms));
        cancellation.check()?;
        self.require_numeric_metadata_destination(destination, &cancellation)?;
        let (response, peer) = self.call_with_read_participant_cancellable(
            &cancellation,
            destination,
            ClusterDataRequest::NativeLearnerBirth {
                request: request.clone(),
            },
        )?;
        cancellation.check()?;
        match response {
            ClusterDataResponse::NativeLearnerBirth(response) => {
                Ok(AuthenticatedNativeLearnerBirthReply {
                    response,
                    original: request,
                    peer,
                    cluster: self.cluster_id.clone(),
                    destination: destination.clone(),
                    started,
                    deadline: cancellation
                        .deadline()
                        .expect("bounded learner birth exchange"),
                    cancellation,
                })
            }
            other => Err(unexpected_response("actual existing learner birth", other)),
        }
    }
    pub(crate) fn acquire_learner_birth(
        &self,
        destination: &ClusterNodeId,
        observed: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeLearnerBirthObservation> {
        observed.require_live_request(business)?;
        parent.check()?;
        let topology = observed.committed_topology(business)?;
        let requester = topology
            .nodes
            .get(observed.requester_node_id())
            .ok_or_else(|| transport_error("actual learner-birth requester missing"))?;
        let target = topology
            .nodes
            .get(destination)
            .ok_or_else(|| transport_error("registered learner missing"))?;
        if observed.requester_node_id() != &self.caller_node_id
            || observed.requester_incarnation() != self.caller_incarnation
            || requester.lifecycle != ClusterNodeLifecycle::Active
            || requester.metadata_role != MetadataMemberRole::Voter
            || target.lifecycle != ClusterNodeLifecycle::Active
            || target.metadata_role != MetadataMemberRole::Learner
            || topology.ranges.values().any(|r| {
                r.replicas
                    .iter()
                    .any(|replica| &replica.node_id == destination)
            })
        {
            return Err(transport_error("actual learner birth requires registered unassigned learner and real voter requester"));
        }
        let mut schema = None;
        for range in topology.ranges.values() {
            let required = range
                .required_schema_sha256(&topology.nodes)
                .ok_or_else(|| transport_error("learner birth lacks active schema"))?;
            if schema.as_ref().is_some_and(|old: &String| old != required) {
                return Err(transport_error(
                    "learner birth schema domain differs across ranges",
                ));
            }
            schema = Some(required.to_owned());
        }
        let request = NativeLearnerBirthRequest {
            version: 1,
            challenge: Uuid::now_v7(),
            business,
            requester: self.caller_node_id.clone(),
            requester_incarnation: self.caller_incarnation,
            requester_metadata_context: observed.context_id(),
            requester_metadata_term: observed.term(),
            requester_metadata_read_index: observed.read_index(),
            bootstrap_id: topology
                .range_image_bootstrap_id
                .ok_or_else(|| transport_error("learner birth bootstrap missing"))?,
            target: destination.clone(),
            target_incarnation: target.incarnation,
            expected_birth: target.native_store_birth_v1.clone(),
            expected_schema_sha256: schema
                .ok_or_else(|| transport_error("learner birth has no ranges"))?,
            budget_ms: 5000,
        };
        let token = parent.child_with_deadline(observed.deadline());
        let sealed = self.request_learner_birth_reply(destination, request, &token)?;
        NativeLearnerBirthObservation::from_transport(sealed, observed, business)
    }
}
