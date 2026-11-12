//! The private envelope seal can only be constructed by this original RPC
//! exchange. Decoding a response or a birth declaration cannot construct it.
use super::*;
use crate::distribution::NativeStoreBirthV1;

pub(crate) struct AuthenticatedNativeMetadataAppendReply {
    request: MetadataAppendRequest,
    destination: ClusterNodeId,
    expected_native_destination: Option<NativeStoreBirthV1>,
    peer: MetadataReadParticipant,
    response: MetadataAppendResponse,
}
impl AuthenticatedNativeMetadataAppendReply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        MetadataAppendRequest,
        ClusterNodeId,
        Option<NativeStoreBirthV1>,
        MetadataReadParticipant,
        MetadataAppendResponse,
    ) {
        (
            self.request,
            self.destination,
            self.expected_native_destination,
            self.peer,
            self.response,
        )
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn append_metadata_cancellable(
        &self,
        follower_node_id: &ClusterNodeId,
        request: MetadataAppendRequest,
        parent: &CancellationToken,
    ) -> Result<MetadataAppendResponse> {
        parent.check()?;
        if request.leader_id != self.caller_node_id {
            return Err(transport_error(
                "metadata append leader must be the transport caller",
            ));
        }
        let (response, peer, expected_native_destination) = self
            .call_with_native_destination_cancellable(
                parent,
                follower_node_id,
                ClusterDataRequest::MetadataAppend {
                    request: request.clone(),
                },
            )?;
        match response {
            ClusterDataResponse::MetadataAppend(response) => {
                MetadataAppendResponse::from_authenticated_native_append(
                    AuthenticatedNativeMetadataAppendReply {
                        request,
                        destination: follower_node_id.clone(),
                        expected_native_destination,
                        peer,
                        response,
                    },
                )
            }
            response => Err(unexpected_response("metadata append", response)),
        }
    }
}
