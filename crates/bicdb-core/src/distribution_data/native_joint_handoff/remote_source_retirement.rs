//! Coordinator-side counterpart of the retired source's in-process receipt:
//! the retirement record the source itself reported in one exchange the
//! pinned transport authenticated. There is no constructor from decoded
//! DATA; the counted member is the authenticated peer, never reply DATA.
//! It lets the completing metadata stage record what the source wrote; it
//! deletes nothing and releases nothing.
use super::source_retirement::{source_retirement_seal, NativeJointSourceRetirementProof};
use super::*;
use crate::distribution_range_handoff::{
    NativeJointDecisionData, NativeJointHandoffStepOperationV1, NativeJointSourceRetirementDataV1,
};
use crate::distribution_transport::AuthenticatedNativeJointBridgeStepFrame;

pub(crate) struct NativeRemoteJointSourceRetirementCertificate {
    data: NativeJointDecisionData,
    frame: AuthenticatedNativeJointBridgeStepFrame,
    retirement: NativeJointSourceRetirementDataV1,
}
/// Every rule the one counted frame must satisfy.
fn require_frame(
    data: &NativeJointDecisionData,
    frame: &AuthenticatedNativeJointBridgeStepFrame,
) -> Result<NativeJointSourceRetirementDataV1> {
    let reply = frame.reply();
    // The transport already bound this reply to the request it actually sent.
    reply.require_request(&reply.request)?;
    reply.request.require_decision(data)?;
    let outcome = frame.outcome();
    let retirement = outcome
        .source_retirement
        .clone()
        .ok_or_else(|| data_error("source retirement frame carries no retirement record"))?;
    retirement.require_shape()?;
    let node = frame.actual_peer();
    let member = data
        .old_members()
        .get(node)
        .ok_or_else(|| data_error("source retirement peer is not an original member"))?;
    let promise = data.promise();
    if data.new_members().contains_key(node)
        || &reply.node_id != node
        || &retirement.node_id != node
        || frame.actual_peer_incarnation() != member.incarnation
        || retirement.node_incarnation != member.incarnation
        || &reply.birth != frame.actual_peer_birth()
        || !matches!(
            &reply.request.operation,
            NativeJointHandoffStepOperationV1::SourceRetire { .. }
        )
        || !outcome.complete
        || outcome.decision_sha256.as_deref() != Some(data.checksum_sha256())
        || retirement.decision_sha256 != data.checksum_sha256()
        || retirement.relocation_id != promise.relocation_id
        || retirement.range_id != data.old_range().id
        || retirement.session_id != promise.session_id
        || retirement.nonce != promise.nonce
        || retirement.activation_epoch != data.activation_range().epoch
    {
        return Err(data_error(
            "source retirement frame differs from authenticated retired source/decision",
        ));
    }
    Ok(retirement)
}
impl NativeRemoteJointSourceRetirementCertificate {
    /// The ONLY input besides the decision the coordinator itself observed
    /// committed is the transport-minted frame of the source's own reply.
    pub(crate) fn from_frame(
        data: &NativeJointDecisionData,
        frame: AuthenticatedNativeJointBridgeStepFrame,
    ) -> Result<Self> {
        frame.require_live()?;
        let retirement = require_frame(data, &frame)?;
        Ok(Self {
            data: data.clone(),
            frame,
            retirement,
        })
    }
}
impl source_retirement_seal::Sealed for NativeRemoteJointSourceRetirementCertificate {}
impl NativeJointSourceRetirementProof for NativeRemoteJointSourceRetirementCertificate {
    fn checksum_sha256(&self) -> &str {
        &self.retirement.checksum_sha256
    }
    fn cluster_id(&self) -> &crate::distribution::ClusterId {
        &self.retirement.cluster_id
    }
    fn node_id(&self) -> &ClusterNodeId {
        self.frame.actual_peer()
    }
    fn node_incarnation(&self) -> u64 {
        self.frame.actual_peer_incarnation()
    }
    fn relocation_id(&self) -> crate::distribution::RelocationId {
        self.retirement.relocation_id
    }
    fn range_id(&self) -> RangeId {
        self.retirement.range_id
    }
    fn session_id(&self) -> Uuid {
        self.retirement.session_id
    }
    fn nonce(&self) -> Uuid {
        self.retirement.nonce
    }
    fn activation_epoch(&self) -> u64 {
        self.retirement.activation_epoch
    }
    fn decision_sha256(&self) -> &str {
        &self.retirement.decision_sha256
    }
    fn activated_quorum_sha256(&self) -> &str {
        &self.retirement.activated_quorum_sha256
    }
    /// The source's store cannot be re-read from this process: the frame
    /// must still be inside the bounded window in which the source itself
    /// re-read its record under its own lock, and unchanged.
    fn require_current(&self) -> Result<()> {
        self.frame.require_live()?;
        if require_frame(&self.data, &self.frame)? != self.retirement {
            return Err(data_error("source retirement certificate changed"));
        }
        Ok(())
    }
}
