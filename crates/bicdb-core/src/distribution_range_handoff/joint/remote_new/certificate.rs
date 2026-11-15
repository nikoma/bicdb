//! Remote counterpart of the in-process durable decision: the proof that the
//! SAME complete native DECISION body is durable on an OLD and a NEW data
//! majority (including the new leader), built ONLY from frames the pinned
//! transport minted after authenticating each replying member.
//!
//! Decoded reply DATA alone can never produce this type: there is no
//! constructor from DTOs, and every counted member must equal the peer the
//! transport itself authenticated for that exact exchange.
use super::*;
use crate::distribution_transport::AuthenticatedNativeRemoteNewFrame;

/// Collection of authenticated read-only durable DECISION observations.
pub(crate) struct NativeRemoteJointDecisionCertificateRound {
    plan: std::sync::Arc<NativeJointHandoffPlan>,
    manifest: NativeRemoteImageManifestV1,
    manifest_bytes: Vec<u8>,
    phase_id: Uuid,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteNewFrame>,
}
/// Actual same-body native WAL acknowledgements on BOTH configurations, each
/// observed over its own authenticated exchange. Like its in-process
/// counterpart this authorizes only a conditional metadata stage.
pub(crate) struct NativeRemoteJointDecisionCertificate {
    plan: std::sync::Arc<NativeJointHandoffPlan>,
    manifest: NativeRemoteImageManifestV1,
    manifest_bytes: Vec<u8>,
    phase_id: Uuid,
    data: NativeJointDecisionData,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteNewFrame>,
}

fn observed(
    frame: &AuthenticatedNativeRemoteNewFrame,
) -> Result<(&NativeRemoteDecisionReceiptV1, &NativeJointDecisionData)> {
    let reply = frame.reply();
    match (&reply.native_decision, &reply.native_decision_body) {
        (Some(receipt), Some(body)) => Ok((receipt, body)),
        _ => Err(invalid(
            "decision certificate frame carries no durable native DECISION observation",
        )),
    }
}

/// Every rule one counted frame must satisfy. Pure re-validation of retained
/// authenticated observations; request liveness is checked by the caller.
fn require_frame(
    plan: &NativeJointHandoffPlan,
    manifest: &NativeRemoteImageManifestV1,
    manifest_bytes: &[u8],
    phase_id: Uuid,
    node: &ClusterNodeId,
    frame: &AuthenticatedNativeRemoteNewFrame,
) -> Result<()> {
    let reply = frame.reply();
    // The transport already bound this reply to the request it actually sent.
    reply.require_request(&reply.request)?;
    let (receipt, body) = observed(frame)?;
    if !matches!(
        reply.request.operation,
        NativeRemoteNewOperationV1::ObserveDecision
    ) || reply.request.phase_id != phase_id
        || receipt.phase_id != phase_id
        || crate::distribution_range_image::bounded_json(
            &reply.request.manifest,
            MAX_NEW_WIRE_BYTES,
        )? != manifest_bytes
        || receipt.binding != NativeSourceExportBindingV1::from_manifest(manifest)?
    {
        return Err(invalid(
            "decision certificate frame changes phase/manifest/read-only operation",
        ));
    }
    let member = plan
        .old_members()
        .get(node)
        .or_else(|| plan.new_members().get(node))
        .ok_or_else(|| invalid("decision certificate member outside fixed OLD/NEW sets"))?;
    let birth = plan.enrolled_birth(node)?;
    // Authenticated peer identity, NOT decoded DATA, names the counted member.
    if frame.actual_peer() != node
        || &receipt.node_id != node
        || &reply.snapshot.node_id != node
        || frame.actual_peer_incarnation() != member.incarnation
        || frame.actual_peer_birth() != birth
        || &reply.snapshot.birth != birth
    {
        return Err(invalid(
            "decision certificate receipt member differs from authenticated peer/incarnation/birth",
        ));
    }
    body.require_remote_certificate_scope(plan, manifest)?;
    if body.checksum_sha256() != receipt.decision_sha256
        || body.plan_sha256()? != plan.checksum_sha256()?
        || receipt.binding.scope_sha256 != plan.checksum_sha256()?
    {
        return Err(invalid(
            "decision certificate receipt changes decision/plan checksum",
        ));
    }
    // The publisher must be an actually materialized participant of this exact
    // body, at the same physical point its authenticated snapshot reports.
    let old = body.old_materialized_fact(node);
    let new = body.remote_new_facts().get(node);
    if (old.is_none() && new.is_none())
        || old.is_some_and(|fact| reply.snapshot.original_old.as_ref() != Some(fact))
        || new.is_some_and(|fact| reply.snapshot.new_materialized.as_ref() != Some(fact))
    {
        return Err(invalid(
            "decision certificate publisher is not an actual materialized participant of this body",
        ));
    }
    Ok(())
}

/// Same rule as the in-process durable decision: OLD majority, NEW majority
/// and the new leader, all for one identical complete body.
fn require_quorums(
    plan: &NativeJointHandoffPlan,
    manifest: &NativeRemoteImageManifestV1,
    manifest_bytes: &[u8],
    phase_id: Uuid,
    data: &NativeJointDecisionData,
    frames: &BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteNewFrame>,
) -> Result<()> {
    data.require_remote_certificate_scope(plan, manifest)?;
    let mut observer = None;
    let mut fact_set = None;
    for (node, frame) in frames {
        require_frame(plan, manifest, manifest_bytes, phase_id, node, frame)?;
        let (receipt, body) = observed(frame)?;
        let first_fact_set: &String = fact_set.get_or_insert(&receipt.fact_set_sha256);
        let first_observer: &ClusterNodeId = observer.get_or_insert(frame.actual_observer());
        if receipt.decision_sha256 != data.checksum_sha256()
            || body != data
            || first_fact_set != &receipt.fact_set_sha256
            || first_observer != frame.actual_observer()
        {
            return Err(invalid(
                "decision certificate receipts differ in decision body/fact set/observer",
            ));
        }
    }
    let old = frames
        .keys()
        .filter(|id| data.old_members().contains_key(*id))
        .count();
    let new = frames
        .keys()
        .filter(|id| data.new_members().contains_key(*id))
        .count();
    if old < data.old_members().len() / 2 + 1
        || new < data.new_members().len() / 2 + 1
        || !frames.contains_key(&data.activation_range().leader)
    {
        return Err(invalid("complete decision must be authenticated durable on both OLD and NEW data majorities, including the new leader"));
    }
    Ok(())
}

impl NativeRemoteJointDecisionCertificateRound {
    pub(crate) fn begin(
        plan: std::sync::Arc<NativeJointHandoffPlan>,
        manifest: NativeRemoteImageManifestV1,
        phase_id: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        // The plan is exact scope DATA here. Its bounded acquisition deadline
        // guards issuing new work, not counting already durable facts; each
        // exchange carries its own fresh committed observation at both ends.
        manifest.require_shape()?;
        require_manifest_plan(&manifest, &plan)?;
        if phase_id.is_nil() {
            return Err(invalid("decision certificate phase identity is nil"));
        }
        let manifest_bytes =
            crate::distribution_range_image::bounded_json(&manifest, MAX_NEW_WIRE_BYTES)?;
        Ok(Self {
            plan,
            manifest,
            manifest_bytes,
            phase_id,
            frames: BTreeMap::new(),
        })
    }
    /// Count one member. The ONLY input is the transport-minted frame; the
    /// member is taken from its authenticated peer, never from an argument.
    pub(crate) fn record(&mut self, frame: AuthenticatedNativeRemoteNewFrame) -> Result<()> {
        frame.require_request_live()?;
        let node = frame.actual_peer().clone();
        if self.frames.contains_key(&node) {
            return Err(invalid(
                "decision certificate member already counted; no duplicate or replacement",
            ));
        }
        require_frame(
            &self.plan,
            &self.manifest,
            &self.manifest_bytes,
            self.phase_id,
            &node,
            &frame,
        )?;
        if let Some(first) = self.frames.values().next() {
            let (expected, expected_body) = observed(first)?;
            let (receipt, body) = observed(&frame)?;
            if receipt.decision_sha256 != expected.decision_sha256
                || body != expected_body
                || receipt.fact_set_sha256 != expected.fact_set_sha256
                || frame.actual_observer() != first.actual_observer()
            {
                return Err(invalid(
                    "decision certificate receipts differ in decision body/fact set/observer",
                ));
            }
        }
        self.frames.insert(node, frame);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeRemoteJointDecisionCertificate> {
        let data = observed(
            self.frames
                .values()
                .next()
                .ok_or_else(|| invalid("decision certificate has no authenticated publisher"))?,
        )?
        .1
        .clone();
        let result = NativeRemoteJointDecisionCertificate {
            plan: self.plan,
            manifest: self.manifest,
            manifest_bytes: self.manifest_bytes,
            phase_id: self.phase_id,
            data,
            frames: self.frames,
        };
        result.verify()?;
        Ok(result)
    }
}

impl NativeRemoteJointDecisionCertificate {
    fn verify(&self) -> Result<()> {
        require_quorums(
            &self.plan,
            &self.manifest,
            &self.manifest_bytes,
            self.phase_id,
            &self.data,
            &self.frames,
        )
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.data
    }
    /// Complete re-validation of the retained authenticated observations:
    /// every counted peer/incarnation/birth, the identical body and receipts,
    /// and both quorums plus the new leader.
    ///
    /// Unlike the in-process decision this cannot re-read any member's local
    /// durable state (that needs a fresh read-only observation round), and it
    /// deliberately does not depend on the bounded request deadlines of the
    /// original exchanges: a decision durable on both majorities does not
    /// expire. Whether it still applies is decided by the metadata stage's own
    /// fresh committed observation (`require_metadata_domain`), which fails on
    /// any changed member incarnation/birth/credential or relocation session.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.verify()
    }
    /// Authenticated members whose durable receipts were counted.
    pub(crate) fn publishers(&self) -> Vec<ClusterNodeId> {
        self.frames.keys().cloned().collect()
    }
    /// Stable identity of the certified facts: body, plan, source manifest,
    /// phase and every counted member's exact durable receipt.
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        let receipts = self
            .frames
            .iter()
            .map(|(node, frame)| Ok((node, observed(frame)?.0)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        digest(&(
            "native-remote-joint-decision-certificate-v1",
            self.data.checksum_sha256(),
            self.plan.checksum_sha256()?,
            &self.manifest.checksum_sha256,
            self.phase_id,
            receipts,
        ))
    }
}
impl crate::distribution_range_handoff::joint::decision::proof_seal::Sealed
    for NativeRemoteJointDecisionCertificate
{
}
impl NativeJointDecisionProof for NativeRemoteJointDecisionCertificate {
    fn data(&self) -> &NativeJointDecisionData {
        NativeRemoteJointDecisionCertificate::data(self)
    }
    fn require_current(&self) -> Result<()> {
        NativeRemoteJointDecisionCertificate::require_current(self)
    }
}
