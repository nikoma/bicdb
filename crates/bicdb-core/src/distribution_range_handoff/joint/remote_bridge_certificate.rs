//! Coordinator-side counterpart of the in-process publication aggregation:
//! the fact that EVERY NEW member of one committed decision reported its own
//! confirmed durable native serving bridge, built ONLY from frames the pinned
//! transport minted after authenticating each replying member.
//!
//! Decoded reply DATA alone can never produce this type: there is no
//! constructor from DTOs, and every counted member must equal the peer the
//! transport itself authenticated for that exact exchange. It is aggregation
//! evidence for the coordinator's next step; it is not an Open, activation
//! or serving grant, and no member trusts it: each member re-observes every
//! other member itself before its own next durable write.
use super::*;
use crate::distribution_transport::AuthenticatedNativeJointBridgeStepFrame;
use crate::CancellationToken;
use std::collections::BTreeMap;

const MAX_CERTIFICATE_BYTES: usize = 64 * 1024;

pub(crate) struct NativeRemoteBridgePublicationCertificateRound {
    data: NativeJointDecisionData,
    coordinator: ClusterNodeId,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeJointBridgeStepFrame>,
}
pub(crate) struct NativeRemoteBridgePublicationCertificate {
    data: NativeJointDecisionData,
    coordinator: ClusterNodeId,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeJointBridgeStepFrame>,
}

/// Every rule one counted frame must satisfy. Pure re-validation of a
/// retained authenticated observation; liveness is checked when it is counted.
fn require_frame<'a>(
    data: &NativeJointDecisionData,
    coordinator: &ClusterNodeId,
    node: &ClusterNodeId,
    frame: &'a AuthenticatedNativeJointBridgeStepFrame,
) -> Result<&'a str> {
    let reply = frame.reply();
    // The transport already bound this reply to the request it actually sent.
    reply.require_request(&reply.request)?;
    reply.request.require_decision(data)?;
    let member = data
        .new_members()
        .get(node)
        .ok_or_else(|| invalid("bridge certificate member outside the fixed NEW set"))?;
    let outcome = frame.outcome();
    // Authenticated peer identity, NOT decoded DATA, names the counted member.
    if frame.actual_peer() != node
        || &reply.node_id != node
        || frame.actual_peer_incarnation() != member.incarnation
        || &reply.birth != frame.actual_peer_birth()
    {
        return Err(invalid(
            "bridge certificate receipt member differs from authenticated peer/incarnation/birth",
        ));
    }
    // ONE observer counts every member, over its own transport. A member
    // cannot observe itself, so a coordinator that is itself a NEW member
    // cannot complete this certificate from frames alone.
    if frame.actual_observer() != coordinator || node == coordinator {
        return Err(invalid(
            "bridge certificate frame was observed by another initiator",
        ));
    }
    if !matches!(
        &reply.request.operation,
        NativeJointHandoffStepOperationV1::BridgePublish { .. }
    ) || !outcome.complete
        || outcome.phase_kind.is_some()
        || outcome.decision_sha256.as_deref() != Some(data.checksum_sha256())
    {
        return Err(invalid(
            "bridge certificate frame is not a confirmed publication of this decision",
        ));
    }
    outcome
        .bridge_receipt_sha256
        .as_deref()
        .ok_or_else(|| invalid("bridge certificate frame carries no durable bridge receipt"))
}

impl NativeRemoteBridgePublicationCertificateRound {
    /// `data` is the decision the coordinator itself observed committed;
    /// `coordinator` is the member whose transport counts the others.
    pub(crate) fn begin(
        data: NativeJointDecisionData,
        coordinator: ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        if data.new_members().is_empty() {
            return Err(invalid("bridge certificate decision has no NEW member"));
        }
        Ok(Self {
            data,
            coordinator,
            frames: BTreeMap::new(),
        })
    }
    /// Count one member. The ONLY input is the transport-minted frame; the
    /// member is taken from its authenticated peer, never from an argument.
    pub(crate) fn record(&mut self, frame: AuthenticatedNativeJointBridgeStepFrame) -> Result<()> {
        frame.require_live()?;
        let node = frame.actual_peer().clone();
        if self.frames.contains_key(&node) {
            return Err(invalid(
                "bridge certificate member already counted; no duplicate or replacement",
            ));
        }
        require_frame(&self.data, &self.coordinator, &node, &frame)?;
        self.frames.insert(node, frame);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeRemoteBridgePublicationCertificate> {
        let result = NativeRemoteBridgePublicationCertificate {
            data: self.data,
            coordinator: self.coordinator,
            frames: self.frames,
        };
        result.verify()?;
        Ok(result)
    }
}

impl NativeRemoteBridgePublicationCertificate {
    /// The in-process aggregation rule: EVERY final NEW member, exactly once.
    fn verify(&self) -> Result<()> {
        if self.frames.keys().ne(self.data.new_members().keys()) {
            return Err(invalid(
                "bridge certificate cannot omit or replace any final NEW member",
            ));
        }
        for (node, frame) in &self.frames {
            require_frame(&self.data, &self.coordinator, node, frame)?;
        }
        Ok(())
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.data
    }
    /// Complete re-validation of the retained authenticated observations.
    /// A durable bridge does not expire, so this does not depend on the
    /// bounded windows of the original exchanges; whether every member still
    /// holds that bridge is re-proved by each member's own next step.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.verify()
    }
    /// Every counted member's own durable bridge receipt checksum.
    pub(crate) fn receipts(&self) -> Result<BTreeMap<ClusterNodeId, String>> {
        self.frames
            .iter()
            .map(|(node, frame)| {
                Ok((
                    node.clone(),
                    require_frame(&self.data, &self.coordinator, node, frame)?.to_owned(),
                ))
            })
            .collect()
    }
    /// Stable identity of the certified facts.
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    "native-remote-bridge-publication-certificate-v1",
                    self.data.checksum_sha256(),
                    self.receipts()?,
                ),
                MAX_CERTIFICATE_BYTES,
            )?,
        )))
    }
}
