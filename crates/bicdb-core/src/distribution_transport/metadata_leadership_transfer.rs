//! Client side of the metadata leadership transfer request. The reply is
//! diagnostic DATA: the caller reads the outcome from its own store only.
use super::*;
use crate::distribution_consensus::{
    MetadataLeadershipTransferRequest, MetadataLeadershipTransferResponse,
};

/// How long a receiver keeps an accepted request before forgetting it.
pub(super) const CAMPAIGN_WINDOW: Duration = Duration::from_secs(3);

impl TcpClusterRelocationTransport {
    pub(crate) fn request_metadata_leadership_transfer(
        &self,
        request: MetadataLeadershipTransferRequest,
        parent: &CancellationToken,
    ) -> Result<MetadataLeadershipTransferResponse> {
        parent.check()?;
        if request.leader_id != self.caller_node_id {
            return Err(transport_error(
                "metadata leadership transfer leader must be the transport caller",
            ));
        }
        let target = request.target_id.clone();
        match self.call_cancellable(
            parent,
            &target,
            ClusterDataRequest::MetadataLeadershipTransfer { request },
        )? {
            ClusterDataResponse::MetadataLeadershipTransfer(response)
                if response.node_id == target && response.cluster_id == self.cluster_id =>
            {
                Ok(response)
            }
            ClusterDataResponse::MetadataLeadershipTransfer(_) => Err(transport_error(
                "metadata leadership transfer response identity mismatch",
            )),
            response => Err(unexpected_response(
                "metadata leadership transfer",
                response,
            )),
        }
    }
}
