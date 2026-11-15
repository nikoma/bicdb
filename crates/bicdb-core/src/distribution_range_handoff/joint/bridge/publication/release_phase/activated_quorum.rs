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
        let sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&(
            ACTIVATED_QUORUM_DOMAIN,
            data.checksum_sha256(),
            data.promise().relocation_id,
            data.activation_range().id,
            data.activation_range().epoch,
            &members,
        ))?));
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
