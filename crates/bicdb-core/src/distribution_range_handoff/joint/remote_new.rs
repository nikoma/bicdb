//! Genuine remote NEW materialization and read-pin handoff. Wire values are
//! correlation DATA. Only the pinned transport creates remote observations;
//! none of these types reconstructs a local OLD/NEW owner or serving grant.
use super::*;
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_range_image::{NativeRemoteImageManifestV1, NativeSourceExportBindingV1};
use crate::CancellationToken;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
mod decision;
mod participant;
pub(crate) use decision::{
    NativeRemoteDecisionAdmission, NativeRemoteDecisionBindingV1, NativeRemoteDecisionReceiptV1,
    NativeRemoteDecisionSourceData, NativeRemoteDecisionSourceRequestV1,
};
pub(crate) use participant::{
    NativeRemoteNewParticipant, NativeRemoteNewPinHandoffPermit, NativeRemoteNewTargetPinIssuance,
};

const MAX_NEW_WIRE_BYTES: usize = 4 * 1024 * 1024;
const MAX_NEW_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_NEW_MEMBERS: usize = 128;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteNewFactSetV1 {
    pub binding: NativeSourceExportBindingV1,
    pub phase_id: Uuid,
    /// Exact immutable snapshot digests, independently fetched by EACH
    /// recipient before its own read-pin transition. These are not votes.
    pub members: BTreeMap<ClusterNodeId, String>,
    pub checksum_sha256: String,
}
impl NativeRemoteNewFactSetV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        digest(&("native-remote-new-factset-v1", value))
    }
    pub(crate) fn require_manifest(&self, mf: &NativeRemoteImageManifestV1) -> Result<()> {
        self.binding.require_manifest(mf)?;
        if self.phase_id.is_nil()
            || self.members.len() > MAX_NEW_MEMBERS
            || self.members.len() < mf.new_members.len() / 2 + 1
            || !self.members.contains_key(&mf.target_id)
            || !self.members.contains_key(&mf.activation_range.leader)
            || self
                .members
                .iter()
                .any(|(id, hash)| !mf.new_members.contains_key(id) || !is_digest(hash))
            || self.checksum_sha256 != self.checksum()?
        {
            return Err(invalid(
                "remote NEW fact set changes its exact NEW majority/leader/target",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeRemoteNewOperationV1 {
    Materialize,
    Observe,
    PrepareDecision {
        facts: NativeRemoteNewFactSetV1,
    },
    /// Publish one actual recipient native DECISION; no catalogue vote or Serving.
    PublishDecision {
        facts: NativeRemoteNewFactSetV1,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteNewRequestV1 {
    pub version: u32,
    pub manifest: NativeRemoteImageManifestV1,
    pub phase_id: Uuid,
    pub operation: NativeRemoteNewOperationV1,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeRemoteNewRequestV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        self.manifest.require_shape()?;
        crate::distribution_range_image::bounded_json(self, MAX_NEW_REQUEST_BYTES)?;
        if self.version != 1
            || self.phase_id.is_nil()
            || self.business.is_nil()
            || self.challenge.is_nil()
            || !(1..=5000).contains(&self.budget_ms)
            || self.manifest.old_members.len() > MAX_NEW_MEMBERS
            || self.manifest.new_members.len() > MAX_NEW_MEMBERS
        {
            return Err(invalid("invalid bounded remote NEW request"));
        }
        if let NativeRemoteNewOperationV1::PrepareDecision { facts }
        | NativeRemoteNewOperationV1::PublishDecision { facts } = &self.operation
        {
            facts.require_manifest(&self.manifest)?;
            if facts.phase_id != self.phase_id {
                return Err(invalid(
                    "remote NEW preparation changed original phase identity",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn require_plan(&self, plan: &NativeJointHandoffPlan) -> Result<()> {
        self.require_shape()?;
        require_manifest_plan(&self.manifest, plan)
    }
    pub(crate) fn purpose(&self) -> crate::distribution_consensus::MetadataRecoveryRolePurpose {
        crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: self.manifest.relocation_id,
            plan_id: self.manifest.plan_id.clone(),
            session_id: self.manifest.handoff_session,
            nonce: self.manifest.nonce,
        }
    }
    pub(crate) fn exact_bytes(&self) -> Result<Vec<u8>> {
        crate::distribution_range_image::bounded_json(self, MAX_NEW_REQUEST_BYTES)
    }
    pub(super) fn custody_bytes(&self) -> Result<Vec<u8>> {
        // The exact immutable canonical MF is retained ONCE by the original
        // participant. Its checked digest plus every remaining request field
        // avoids duplicating multi-MiB schema/receipt DATA per retry.
        crate::distribution_range_image::bounded_json(
            &(
                self.version,
                &self.manifest.checksum_sha256,
                self.phase_id,
                &self.operation,
                self.business,
                self.challenge,
                self.budget_ms,
            ),
            64 * 1024,
        )
    }
    /// Pure necessary current-Pending comparison. OWN is not converted into
    /// a mutation Role, local materialization or quorum by this check.
    pub(crate) fn require_current_own(
        &self,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        local: &ClusterNodeId,
    ) -> Result<()> {
        self.require_shape()?;
        own.require_live_request(business)?;
        let current = own.committed_topology(business)?;
        let mf = &self.manifest;
        let relocation = current
            .relocations
            .get(&mf.relocation_id)
            .ok_or_else(|| invalid("remote NEW current Pending plan absent"))?;
        let retired = mf
            .old_members
            .keys()
            .filter(|id| !mf.new_members.contains_key(*id))
            .collect::<Vec<_>>();
        if own.requester_node_id() != local
            || current.cluster_id != mf.cluster_id
            || current.range_image_bootstrap_id != Some(mf.bootstrap_id)
            || current
                .range_by_id(mf.capture_range.id)
                .is_none_or(|r| canonical_range(r) != canonical_range(&mf.capture_range))
            || relocation.plan_id != mf.plan_id
            || relocation.range_id != mf.capture_range.id
            || relocation.target != mf.target_id
            || retired.len() != 1
            || relocation.source.as_ref() != retired.first().copied()
            || relocation.learner_epoch != mf.capture_range.epoch
            || relocation
                .handoff_session
                .as_ref()
                .is_none_or(|s| s.session_id != mf.handoff_session || s.nonce != mf.nonce)
            || !matches!(
                relocation.phase,
                crate::distribution::RelocationPhase::LearnerAllocated
                    | crate::distribution::RelocationPhase::SnapshotCopying
                    | crate::distribution::RelocationPhase::CatchingUp
                    | crate::distribution::RelocationPhase::ReadyToPromote
            )
            || own.restore_activation(business)? != mf.restore.as_ref()
        {
            return Err(invalid(
                "remote NEW current OWN changes exact original Pending scope",
            ));
        }
        for (id, expected) in mf.old_members.iter().chain(&mf.new_members) {
            let actual = current
                .nodes
                .get(id)
                .ok_or_else(|| invalid("remote NEW current member absent"))?;
            if actual.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
                || actual.incarnation != expected.incarnation
                || actual.tls_certificate_sha256 != expected.tls_certificate_sha256
                || actual.pending_tls_certificate_sha256 != expected.pending_tls_certificate_sha256
                || actual
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&mf.schema_sha256)
            {
                return Err(invalid(
                    "remote NEW current credential/incarnation/schema changed",
                ));
            }
        }
        own.require_live_request(business)
    }
}
fn require_manifest_plan(
    mf: &NativeRemoteImageManifestV1,
    plan: &NativeJointHandoffPlan,
) -> Result<()> {
    mf.require_shape()?;
    NativeSourceExportBindingV1::from_manifest(mf)?.require_plan(plan)?;
    mf.source_old_close_record.require_plan(plan)?;
    if canonical_range(&mf.capture_range) != canonical_range(plan.old_range())
        || canonical_range(&mf.activation_range) != canonical_range(plan.activation_range())
        || canonical_range(&mf.native_predecessor)
            != canonical_range(&plan.old_native_predecessor_data()?)
        || &mf.old_members != plan.old_members()
        || &mf.new_members != plan.new_members()
        || mf.schema_sha256 != plan.constraint_schema_sha256()
        || mf.restore.as_ref() != plan.constraint_restore_activation()
        || mf.old_config_sha256 != plan.promise().old_config_sha256
        || mf.new_config_sha256 != plan.promise().new_config_sha256
    {
        return Err(invalid(
            "remote NEW manifest changes actual complete pending plan/configuration",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteNewSnapshotV1 {
    pub binding: NativeSourceExportBindingV1,
    pub phase_id: Uuid,
    pub node_id: ClusterNodeId,
    pub birth: NativeStoreBirthV1,
    pub database_open: String,
    pub store_open: Uuid,
    pub metadata_store_id: Uuid,
    /// Genuine original node-local OLD close, if this participant owns one.
    /// Its original native receipt and metadata provenance are not retagged.
    pub original_old: Option<NativeJointMaterializedFacts>,
    /// A distinct NEW materialization fact under the actual issuance Role/OWN.
    /// Retiring OLD-only participants cannot vote in the NEW majority.
    pub new_materialized: Option<NativeJointMaterializedFacts>,
    pub checksum_sha256: String,
}
impl NativeRemoteNewSnapshotV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        digest(&("native-remote-new-snapshot-v1", value))
    }
    pub(crate) fn require_request(&self, request: &NativeRemoteNewRequestV1) -> Result<()> {
        request.require_shape()?;
        let mf = &request.manifest;
        self.binding.require_manifest(mf)?;
        if self.phase_id != request.phase_id
            || self.database_open.is_empty()
            || self.database_open.len() > 256
            || self.store_open.is_nil()
            || self.metadata_store_id.is_nil()
            || self.birth.bootstrap_id() != mf.bootstrap_id
            || self.checksum_sha256 != self.checksum()?
            || (self.original_old.is_none() && self.new_materialized.is_none())
        {
            return Err(invalid(
                "remote NEW snapshot changes original process/native identity",
            ));
        }
        if let Some(old) = &self.original_old {
            if mf.actual_old_closures.get(&self.node_id) != Some(old)
                || old.birth_id != self.birth.birth_id()
                || old.birth_sha256 != self.birth.birth_sha256()
            {
                return Err(invalid(
                    "remote NEW snapshot changes original actual OLD fact",
                ));
            }
        } else if mf.old_members.contains_key(&self.node_id) {
            return Err(invalid(
                "remote NEW snapshot omits its original OLD obligation",
            ));
        }
        if let Some(new) = &self.new_materialized {
            let is_target = self.node_id == mf.target_id;
            if !mf.new_members.contains_key(&self.node_id)
                || new.node_id != self.node_id
                || new.scope_sha256 != self.binding.scope_sha256
                || new.birth_id != self.birth.birth_id()
                || new.birth_sha256 != self.birth.birth_sha256()
                || new.range_index != mf.range_index
                || new.logical_genesis_sha256 != mf.h0
                || new.logical_history_sha256 != mf.h
                || !is_digest(&new.native_receipt_sha256)
                || new.metadata_context_id.is_nil()
                || new.metadata_term == 0
                || new.metadata_read_index == 0
                || new.source_snapshot_db_sequence.is_some() != is_target
                || new.image_root_sha256.is_some() != is_target
                || (is_target && new.source_snapshot_db_sequence != Some(mf.source_reserved_b))
                || new
                    .image_root_sha256
                    .as_ref()
                    .is_some_and(|h| !is_digest(h))
            {
                return Err(invalid(
                    "remote NEW actual physical point does not match fixed source image",
                ));
            }
            if let Some(old) = &self.original_old {
                if new.native_receipt_sha256 != old.native_receipt_sha256
                    || new.actual_local_visibility_sequence != old.actual_local_visibility_sequence
                    || new.metadata_term < old.metadata_term
                    || new.metadata_read_index < old.metadata_read_index
                {
                    return Err(invalid("remote NEW survivor changed native OLD custody"));
                }
            }
        } else if mf.new_members.contains_key(&self.node_id) {
            return Err(invalid(
                "NEW participant omitted actual native materialization",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteNewHandoffV1 {
    pub snapshot_sha256: String,
    pub fact_set_sha256: String,
    pub old_read_pin_detached: bool,
    pub target_materialization_pin_detached: bool,
    pub target_export_pin_detached: bool,
    /// RAM-only original-owner retention. This is explicitly NOT a durable
    /// native DECISION, metadata commit, activation, Open or Serving receipt.
    pub original_freeze_retained: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteNewReplyV1 {
    pub request: NativeRemoteNewRequestV1,
    pub snapshot: NativeRemoteNewSnapshotV1,
    pub handoff: Option<NativeRemoteNewHandoffV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_decision: Option<NativeRemoteDecisionReceiptV1>,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub metadata_context_id: Uuid,
    pub checksum_sha256: String,
}
impl NativeRemoteNewReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        digest(&("native-remote-new-reply-v1", value))
    }
    pub(crate) fn require_request(&self, request: &NativeRemoteNewRequestV1) -> Result<()> {
        self.snapshot.require_request(request)?;
        if self.request.exact_bytes()? != request.exact_bytes()?
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || self.metadata_context_id.is_nil()
            || self.checksum_sha256 != self.checksum()?
        {
            return Err(invalid(
                "remote NEW reply changes exact request/current observation",
            ));
        }
        if let Some(new) = &self.snapshot.new_materialized {
            if self.metadata_term < new.metadata_term
                || self.metadata_read_index < new.metadata_read_index
            {
                return Err(invalid(
                    "remote NEW fresh observation regressed behind original issuance",
                ));
            }
        }
        if let Some(handoff) = &self.handoff {
            if handoff.snapshot_sha256 != self.snapshot.checksum_sha256
                || !handoff.original_freeze_retained
                || !is_digest(&handoff.fact_set_sha256)
            {
                return Err(invalid(
                    "remote NEW handoff changed retained actual snapshot",
                ));
            }
            if let NativeRemoteNewOperationV1::PrepareDecision { facts }
            | NativeRemoteNewOperationV1::PublishDecision { facts } = &request.operation
            {
                if handoff.fact_set_sha256 != facts.checksum_sha256 {
                    return Err(invalid(
                        "remote NEW handoff changed its exact intended fact set",
                    ));
                }
            }
        } else if matches!(
            request.operation,
            NativeRemoteNewOperationV1::PrepareDecision { .. }
                | NativeRemoteNewOperationV1::PublishDecision { .. }
        ) {
            return Err(invalid(
                "remote NEW preparation returned no actual handoff outcome",
            ));
        }
        match (&request.operation, &self.native_decision) {
            (NativeRemoteNewOperationV1::PublishDecision { facts }, Some(receipt)) => {
                receipt.require_request(request, facts)?;
                if receipt.node_id != self.snapshot.node_id {
                    return Err(invalid(
                        "remote DECISION receipt differs from authenticated snapshot recipient",
                    ));
                }
            }
            (NativeRemoteNewOperationV1::PublishDecision { .. }, None) => {
                return Err(invalid("native DECISION outcome not proved"))
            }
            (_, None) => {}
            _ => return Err(invalid("unrequested native DECISION outcome")),
        }
        Ok(())
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(sha2::Sha256::digest(
        crate::distribution_range_image::bounded_json(value, MAX_NEW_WIRE_BYTES)?,
    )))
}

/// A distinct remote NEW collection. It retains the actual authenticated
/// frames and original actual metadata-read plan; it never supplies local owner Arcs to
/// NativeJointNewRound or constructs NativeJointPreparedDecision.
pub(crate) struct NativeRemoteNewRound {
    plan: std::sync::Arc<NativeJointHandoffPlan>,
    caller: CancellationToken,
    manifest: NativeRemoteImageManifestV1,
    phase_id: Uuid,
    actual_observer: Option<ClusterNodeId>,
    frames:
        BTreeMap<ClusterNodeId, crate::distribution_transport::AuthenticatedNativeRemoteNewFrame>,
}
pub(crate) struct NativeRemoteNewMaterializedQuorum {
    plan: std::sync::Arc<NativeJointHandoffPlan>,
    caller: CancellationToken,
    manifest: NativeRemoteImageManifestV1,
    frames:
        BTreeMap<ClusterNodeId, crate::distribution_transport::AuthenticatedNativeRemoteNewFrame>,
    facts: NativeRemoteNewFactSetV1,
}
impl NativeRemoteNewRound {
    pub(crate) fn begin(
        plan: std::sync::Arc<NativeJointHandoffPlan>,
        manifest: NativeRemoteImageManifestV1,
        phase_id: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        plan.require_live()?;
        manifest.require_shape()?;
        require_manifest_plan(&manifest, &plan)?;
        let binding = NativeSourceExportBindingV1::from_manifest(&manifest)?;
        binding.require_plan(&plan)?;
        if phase_id.is_nil() {
            return Err(invalid("remote NEW round phase identity is nil"));
        }
        Ok(Self {
            plan,
            caller: parent.child(),
            manifest,
            phase_id,
            actual_observer: None,
            frames: BTreeMap::new(),
        })
    }
    pub(crate) fn record(
        &mut self,
        frame: crate::distribution_transport::AuthenticatedNativeRemoteNewFrame,
    ) -> Result<()> {
        self.caller.check()?;
        require_manifest_plan(&self.manifest, &self.plan)?;
        frame.require_request_live()?;
        let reply = frame.reply();
        reply.require_request(&reply.request)?;
        let snapshot = &reply.snapshot;
        NativeSourceExportBindingV1::from_manifest(&self.manifest)?
            .require_manifest(&reply.request.manifest)?;
        if snapshot.phase_id != self.phase_id
            || snapshot.new_materialized.is_none()
            || !self.manifest.new_members.contains_key(&snapshot.node_id)
            || self.frames.contains_key(&snapshot.node_id)
            || self
                .actual_observer
                .as_ref()
                .is_some_and(|id| id != frame.actual_observer())
        {
            return Err(invalid(
                "remote NEW collection changes phase/member/actual observer",
            ));
        }
        let node = snapshot.node_id.clone();
        self.actual_observer = Some(frame.actual_observer().clone());
        self.frames.insert(node, frame);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeRemoteNewMaterializedQuorum> {
        self.caller.check()?;
        let binding = NativeSourceExportBindingV1::from_manifest(&self.manifest)?;
        let mut facts = NativeRemoteNewFactSetV1 {
            binding,
            phase_id: self.phase_id,
            members: self
                .frames
                .iter()
                .map(|(id, frame)| (id.clone(), frame.reply().snapshot.checksum_sha256.clone()))
                .collect(),
            checksum_sha256: String::new(),
        };
        facts.checksum_sha256 = facts.checksum()?;
        facts.require_manifest(&self.manifest)?;
        let result = NativeRemoteNewMaterializedQuorum {
            plan: self.plan,
            caller: self.caller,
            manifest: self.manifest,
            frames: self.frames,
            facts,
        };
        result.require_current()?;
        Ok(result)
    }
}
impl NativeRemoteNewMaterializedQuorum {
    pub(crate) fn require_current(&self) -> Result<()> {
        self.caller.check()?;
        require_manifest_plan(&self.manifest, &self.plan)?;
        self.facts.require_manifest(&self.manifest)?;
        for frame in self.frames.values() {
            frame.require_request_live()?;
        }
        Ok(())
    }
    /// Historical immutable DATA for independent recipient callbacks. This is
    /// not a local materialized quorum or a native DECISION publisher grant.
    pub(crate) fn facts(&self) -> &NativeRemoteNewFactSetV1 {
        &self.facts
    }
}
