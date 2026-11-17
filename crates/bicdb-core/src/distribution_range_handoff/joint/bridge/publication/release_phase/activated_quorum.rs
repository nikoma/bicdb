//! The activation record of a completed joint Open: every member of the new
//! set holds its durable catalogue activation and is released and open, each
//! observed under a fresh metadata read of its own inside one phase window.
//! It authorizes the metadata activation record only: never a release, a
//! writer policy, a read or the retirement of any source data.
use super::*;
use sha2::{Digest, Sha256};

/// Domain of `activated_quorum_sha256`. The digest covers durable facts only,
/// so the same completed Open always yields the same record.
const ACTIVATED_QUORUM_DOMAIN: &str = "bicdb-native-joint-activated-quorum-v1";

/// `activated_quorum_sha256`: ONE function of the same durable facts for
/// every proof kind. `members` is every member of the new set in node order
/// with its member incarnation, its own durable native bridge receipt and
/// its own durable catalogue activation.
pub(super) fn activated_quorum_digest(
    data: &NativeJointDecisionData,
    members: &[(ClusterNodeId, u64, String, String)],
) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
        ACTIVATED_QUORUM_DOMAIN,
        data.checksum_sha256(),
        data.promise().relocation_id,
        data.activation_range().id,
        data.activation_range().epoch,
        members,
    ))?)))
}

/// Seal for [`NativeJointActivationProof`]: nameable only inside the joint
/// handoff module tree, so no other module can supply an activation proof.
pub(in crate::distribution_range_handoff::joint) mod activation_proof_seal {
    pub trait Sealed {}
}
/// Exactly what the metadata activation record reads from a proof that
/// EVERY member of the new set is durably activated, released and open. An
/// implementor is a capability, never decoded DATA: it exists only where
/// that all-released quorum was actually observed (every member's service
/// in this process, or the observer's own member state plus
/// transport-authenticated release frames of every other member).
pub(crate) trait NativeJointActivationProof: activation_proof_seal::Sealed {
    fn data(&self) -> &NativeJointDecisionData;
    /// What `activated_quorum_sha256` records.
    fn checksum_sha256(&self) -> &str;
    fn require_current(&self) -> Result<()>;
}
impl activation_proof_seal::Sealed for NativeJointActivatedQuorum {}
impl NativeJointActivationProof for NativeJointActivatedQuorum {
    fn data(&self) -> &NativeJointDecisionData {
        NativeJointActivatedQuorum::data(self)
    }
    fn checksum_sha256(&self) -> &str {
        NativeJointActivatedQuorum::checksum_sha256(self)
    }
    fn require_current(&self) -> Result<()> {
        NativeJointActivatedQuorum::require_current(self)
    }
}

/// A live all-released phase of the original release continuation. It cannot
/// be built from digests, replies or decoded state.
pub(crate) struct NativeJointActivatedQuorum {
    phase: Arc<NativeJointReleasePhaseQuorum>,
    sha256: String,
}
impl NativeJointReleasePhaseQuorum {
    /// Claim this fresh phase as the activation record of its decision. Every
    /// final member must be released and open and still current. A phase that
    /// selected a release or a writer policy attempt cannot be claimed again.
    pub(crate) fn certify_activated_quorum(self: &Arc<Self>) -> Result<NativeJointActivatedQuorum> {
        self.require_live()?;
        let data = &self.continuation.data;
        if self.members.keys().ne(data.new_members().keys()) {
            return Err(invalid(
                "joint activation record omitted/replaced a final member",
            ));
        }
        for member in self.members.values() {
            member.require_released_open()?;
        }
        if self.claimed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(invalid(
                "joint all-released phase already selected an attempt",
            ));
        }
        let mut members = Vec::with_capacity(self.members.len());
        for (node, evidence) in &self.members {
            self.require_member_current(node)?;
            evidence.require_released_open()?;
            let member = &self.continuation.members[node];
            let activation = evidence.activation().activation_sha256()?;
            if evidence.native().checksum_sha256() != member.native.checksum_sha256()
                || evidence.native().local_node_id() != node
                || member.activation.activation_sha256()? != activation
            {
                return Err(invalid(
                    "joint activation record changed an original member activation",
                ));
            }
            members.push((
                node.clone(),
                data.new_members()[node].incarnation,
                evidence.native().checksum_sha256().to_string(),
                activation,
            ));
        }
        let sha256 = activated_quorum_digest(data, &members)?;
        let result = NativeJointActivatedQuorum {
            phase: self.clone(),
            sha256,
        };
        result.require_current()?;
        Ok(result)
    }
}
impl NativeJointActivatedQuorum {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.phase.continuation.data
    }
    /// What `activated_quorum_sha256` records.
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.sha256
    }
    /// The phase window and every member's released open are still live.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.phase.require_live()?;
        for member in self.phase.members.values() {
            member.require_released_open()?;
        }
        self.phase.require_live()
    }
}
