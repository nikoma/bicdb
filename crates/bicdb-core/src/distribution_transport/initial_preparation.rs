//! Authenticated initial-configuration preparation observations. The wire DTO is never a permit.
use super::*;
use crate::distribution_native_host::{
    NativeInitialPreparationObservation, NativeInitialPreparationRequest,
    NativeInitialPreparationResponse,
};

pub(crate) struct AuthenticatedNativeInitialPreparationReply {
    response: NativeInitialPreparationResponse,
    original: NativeInitialPreparationRequest,
    peer: MetadataReadParticipant,
    cluster: ClusterId,
    destination: ClusterNodeId,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl AuthenticatedNativeInitialPreparationReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        NativeInitialPreparationResponse,
        NativeInitialPreparationRequest,
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
    pub(crate) fn request_initial_preparation_reply(
        &self,
        destination: &ClusterNodeId,
        request: NativeInitialPreparationRequest,
        parent: &CancellationToken,
    ) -> Result<AuthenticatedNativeInitialPreparationReply> {
        request.validate()?;
        if request.requester != self.caller_node_id
            || request.requester_incarnation != self.caller_incarnation
        {
            return Err(transport_error(
                "initial-preparation request is not this actual transport caller",
            ));
        }
        let cancellation =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        cancellation.check()?;
        let deadline = cancellation
            .deadline()
            .expect("bounded initial-preparation exchange");
        self.require_numeric_metadata_destination(destination, &cancellation)?;
        let (response, peer) = self.call_with_read_participant_cancellable(
            &cancellation,
            destination,
            ClusterDataRequest::NativeInitialPreparation {
                request: request.clone(),
            },
        )?;
        cancellation.check()?;
        match response {
            ClusterDataResponse::NativeInitialPreparation(response) => {
                Ok(AuthenticatedNativeInitialPreparationReply {
                    response,
                    original: request,
                    peer,
                    cluster: self.cluster_id.clone(),
                    destination: destination.clone(),
                    deadline,
                    cancellation,
                })
            }
            other => Err(unexpected_response(
                "actual initial configuration preparation",
                other,
            )),
        }
    }
    pub(crate) fn acquire_initial_preparation(
        &self,
        destination: &ClusterNodeId,
        plan: &crate::distribution_native_host::NativeClusterBootstrapPlan,
        configuration: Option<crate::ClusterTopology>,
        parent: &CancellationToken,
    ) -> Result<NativeInitialPreparationObservation> {
        let request = NativeInitialPreparationRequest {
            version: 1,
            challenge: Uuid::now_v7(),
            requester: self.caller_node_id.clone(),
            requester_incarnation: self.caller_incarnation,
            plan_sha256: plan.checksum()?,
            configuration,
            budget_ms: 5000,
        };
        let sealed = self.request_initial_preparation_reply(destination, request, parent)?;
        NativeInitialPreparationObservation::from_transport(sealed, plan)
    }
}

pub(crate) struct AuthenticatedNativeInitialConfigurationDataReply {
    response: crate::distribution_native_host::NativeInitialConfigurationDataResponse,
    original: NativeInitialPreparationRequest,
    peer: MetadataReadParticipant,
    cluster: ClusterId,
    destination: ClusterNodeId,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl AuthenticatedNativeInitialConfigurationDataReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::distribution_native_host::NativeInitialConfigurationDataResponse,
        NativeInitialPreparationRequest,
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
    pub(crate) fn discover_initial_configuration(
        &self,
        destination: &ClusterNodeId,
        plan: &crate::distribution_native_host::NativeClusterBootstrapPlan,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_host::NativeInitialConfigurationDataObservation> {
        let request = NativeInitialPreparationRequest {
            version: 1,
            challenge: Uuid::now_v7(),
            requester: self.caller_node_id.clone(),
            requester_incarnation: self.caller_incarnation,
            plan_sha256: plan.checksum()?,
            configuration: None,
            budget_ms: 5000,
        };
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        token.check()?;
        self.require_numeric_metadata_destination(destination, &token)?;
        let (response, peer) = self.call_with_read_participant_cancellable(
            &token,
            destination,
            ClusterDataRequest::NativeInitialPreparation {
                request: request.clone(),
            },
        )?;
        let response=match response {
            ClusterDataResponse::NativeInitialConfigurationData(data)=>data,
            ClusterDataResponse::NativeInitialPreparation(accepted)=>crate::distribution_native_host::NativeInitialConfigurationDataResponse{
                request:accepted.request,configuration:accepted.configuration,node:accepted.fact.node,
                incarnation:accepted.fact.incarnation,birth:accepted.fact.birth,
                source:crate::distribution_native_host::NativeInitialConfigurationDataSource::ProtectedAcceptance,
                protected_record_sha256:accepted.fact.acceptance_sha256},
            other=>return Err(unexpected_response("initial CONFIG-only discovery",other)),
        };
        token.check()?;
        crate::distribution_native_host::NativeInitialConfigurationDataObservation::from_transport(
            AuthenticatedNativeInitialConfigurationDataReply {
                response,
                original: request,
                peer,
                cluster: self.cluster_id.clone(),
                destination: destination.clone(),
                deadline: token.deadline().unwrap(),
                cancellation: token,
            },
            plan,
        )
    }
}
