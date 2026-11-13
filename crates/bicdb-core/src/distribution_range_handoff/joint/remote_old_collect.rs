//! Distinct remote OLD certificate. Actual authenticated historical replies
//! constrain a real donor-local immutable spine; they never recreate local
//! service/Frozen observations. This first serving lane requires ALL OLD peers.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::NativeJointIrrevocableFacts;
use crate::distribution_transport::AuthenticatedNativeRemoteOldFrame;
use crate::CancellationToken;
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

pub(crate) struct NativeRemoteOldRound {
    source: Arc<NativeJointOldObservation>,
    own: HostMetadataReadObservation,
    business: Uuid,
    original_caller: CancellationToken,
    peers: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteOldFrame>,
}
pub(crate) struct NativeRemoteOldCutCertificate {
    source: Arc<NativeJointOldObservation>,
    plan: Arc<NativeJointHandoffPlan>,
    peers: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteOldFrame>,
    constraints: BTreeMap<ClusterNodeId, NativeJointIrrevocableFacts>,
    facts: BTreeMap<ClusterNodeId, NativeJointMaterializedFacts>,
    digest: String,
    cut: Option<CertifiedRangeImageCut>,
}
#[derive(Debug)]
pub(crate) struct NativeRemoteOldImageProof {
    source: Weak<NativeJointOldObservation>,
    plan: Arc<NativeJointHandoffPlan>,
    r: u64,
    h0: String,
    h: String,
    digest: String,
}
impl NativeRemoteOldImageProof {
    pub(crate) fn require_source_current(&self) -> Result<()> {
        let source = self
            .source
            .upgrade()
            .ok_or_else(|| invalid("remote OLD actual donor owner ended"))?;
        source.require_current()?;
        if source.plan().checksum_sha256()? != self.plan.checksum_sha256()?
            || source.range_index() != self.r
            || source.logical_genesis_sha256() != self.h0
            || source.logical_history_sha256() != self.h
            || self.digest.len() != 64
        {
            return Err(invalid("remote OLD actual donor/canonical point changed"));
        }
        Ok(())
    }
}
impl NativeRemoteOldRound {
    pub(crate) fn begin(
        source: Arc<NativeJointOldObservation>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        own.require_live_request(business)?;
        source.require_current()?;
        source
            .plan()
            .require_source_host_metadata(&own, business, source.node_id())?;
        Ok(Self {
            source,
            own,
            business,
            original_caller: parent.clone(),
            peers: BTreeMap::new(),
        })
    }
    fn require_live(&self) -> Result<()> {
        self.original_caller.check()?;
        self.own.require_live_request(self.business)?;
        self.source.plan().require_source_host_metadata(
            &self.own,
            self.business,
            self.source.node_id(),
        )?;
        self.source.require_current()
    }
    pub(crate) fn record(&mut self, frame: AuthenticatedNativeRemoteOldFrame) -> Result<()> {
        self.require_live()?;
        frame.require_request_live()?;
        let reply = frame.reply();
        let plan = self.source.plan();
        reply.request.require_plan(plan)?;
        if reply.request.operation != NativeRemoteOldOperationV1::Observe
            || frame.actual_observer() != self.source.node_id()
            || !plan.old_members().contains_key(&reply.node_id)
            || self.peers.contains_key(&reply.node_id)
        {
            return Err(invalid(
                "remote OLD needs distinct donor-fetched actual readonly members",
            ));
        }
        let closed = reply
            .closed
            .as_ref()
            .ok_or_else(|| invalid("remote OLD peer closure unknown; not a zero/absence vote"))?;
        if closed.scope_sha256 != plan.checksum_sha256()?
            || closed.node_id != reply.node_id
            || closed.range_index > self.source.range_index()
            || closed.logical_genesis_sha256 != self.source.logical_genesis_sha256()
        {
            return Err(invalid(
                "remote OLD source omits a higher/forked actual closed prefix",
            ));
        }
        self.source.require_remote_ancestor(
            closed.range_index,
            &closed.logical_genesis_sha256,
            &closed.logical_history_sha256,
            &self.original_caller,
        )?;
        let constraints = &reply.constraints;
        if constraints.scope_sha256 != plan.checksum_sha256()?
            || constraints.node_id != reply.node_id
            || constraints.actual_resolved > self.source.range_index()
            || constraints.actual_compacted > self.source.range_index()
        {
            return Err(invalid(
                "remote OLD irreversible floor exceeds actual donor cut",
            ));
        }
        if let Some((index, h0, h)) = &constraints.native_point {
            self.source
                .require_remote_ancestor(*index, h0, h, &self.original_caller)?;
        }
        for (index, hash) in &constraints.commands {
            if *index > self.source.range_index()
                || self
                    .source
                    .remote_command_at(*index, &self.original_caller)?
                    .as_deref()
                    != Some(hash.as_str())
            {
                return Err(invalid(
                    "remote OLD donor omits/conflicts with observed irreversible command",
                ));
            }
        }
        frame.require_request_live()?;
        self.peers.insert(reply.node_id.clone(), frame);
        self.require_live()
    }
    pub(crate) fn finish(self) -> Result<NativeRemoteOldCutCertificate> {
        self.require_live()?;
        let plan = self.source.plan().clone();
        // This does NOT silently ignore an unavailable or unresolved minority.
        // A separately sealed minority-constraint lane is still required.
        if self.peers.len() != plan.old_members().len()
            || !self.peers.contains_key(self.source.node_id())
        {
            return Err(invalid("first remote OLD source requires ALL actual old closed responses; unavailable/minority-tail lane unsupported"));
        }
        let facts = self
            .peers
            .iter()
            .map(|(id, f)| (id.clone(), f.reply().closed.as_ref().unwrap().clone()))
            .collect::<BTreeMap<_, _>>();
        let constraints = self
            .peers
            .iter()
            .map(|(id, f)| (id.clone(), f.reply().constraints.clone()))
            .collect::<BTreeMap<_, _>>();
        let r = self.source.range_index();
        let h0 = self.source.logical_genesis_sha256().to_owned();
        let h = self.source.logical_history_sha256().to_owned();
        let digest = hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    "native-remote-old-cut-certificate-v1",
                    plan.checksum_sha256()?,
                    self.source.node_id(),
                    r,
                    &h0,
                    &h,
                    &facts,
                    &constraints,
                ),
                2 * 1024 * 1024,
            )?,
        ));
        let topology = self.own.committed_topology(self.business)?;
        let relocation = topology
            .relocations
            .get(&plan.promise().relocation_id)
            .ok_or_else(|| invalid("remote OLD actual Pending relocation disappeared"))?
            .clone();
        let source_member = topology
            .nodes
            .get(self.source.node_id())
            .ok_or_else(|| invalid("remote OLD donor disappeared"))?;
        let target_member = topology
            .nodes
            .get(&relocation.target)
            .ok_or_else(|| invalid("remote OLD target disappeared"))?;
        let cut = CertifiedRangeImageCut {
            native_old: None,
            native_remote_old: Some(Arc::new(NativeRemoteOldImageProof {
                source: Arc::downgrade(&self.source),
                plan: plan.clone(),
                r,
                h0: h0.clone(),
                h: h.clone(),
                digest: digest.clone(),
            })),
            promise: plan.promise().clone(),
            cluster_id: topology.cluster_id.clone(),
            range: plan.old_range().clone(),
            range_index: r,
            source_id: self.source.node_id().clone(),
            source_incarnation: source_member.incarnation,
            target_id: relocation.target.clone(),
            target_incarnation: target_member.incarnation,
            intended_activation_range: plan.activation_range().clone(),
            intended_activation_members: plan.new_members().clone(),
            bootstrap_id: topology.range_image_bootstrap_id,
            relocation,
            old_member_credentials: plan
                .old_members()
                .iter()
                .map(|(id, m)| {
                    (
                        id.clone(),
                        (
                            m.incarnation,
                            m.tls_certificate_sha256.clone(),
                            m.pending_tls_certificate_sha256.clone(),
                        ),
                    )
                })
                .collect(),
            expected_schema_sha256: Some(plan.data().schema_sha256.clone()),
            restore_activation: self.own.restore_activation(self.business)?.cloned(),
        };
        self.require_live()?;
        Ok(NativeRemoteOldCutCertificate {
            source: self.source,
            plan,
            peers: self.peers,
            constraints,
            facts,
            digest,
            cut: Some(cut),
        })
    }
}
impl NativeRemoteOldCutCertificate {
    pub(crate) fn plan(&self) -> Arc<NativeJointHandoffPlan> {
        self.plan.clone()
    }
    pub(crate) fn selected_source_id(&self) -> &ClusterNodeId {
        self.source.node_id()
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.source.require_current()?;
        if self.peers.len() != self.plan.old_members().len()
            || self.facts.len() != self.peers.len()
            || self.constraints.len() != self.peers.len()
        {
            return Err(invalid(
                "remote OLD certificate lost its actual recorded member set",
            ));
        }
        Ok(())
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        self.require_current()?;
        Ok(self.digest.clone())
    }
    pub(crate) fn source_image_materialized_facts(
        &self,
    ) -> Result<BTreeMap<ClusterNodeId, NativeJointMaterializedFacts>> {
        self.require_current()?;
        Ok(self.facts.clone())
    }
    pub(crate) fn begin_capture_with_initial_wait(
        &mut self,
        budget: std::time::Duration,
        process: &CancellationToken,
        publish: &CancellationToken,
    ) -> Result<crate::distribution_range_image::RangeImageCapture> {
        process.check()?;
        publish.check()?;
        self.require_current()?;
        let cut = self
            .cut
            .take()
            .ok_or_else(|| invalid("remote OLD exact capture already attempted; retain owner"))?;
        cut.native_remote_old_image_proof()
            .ok_or_else(|| invalid("actual remote capture lost its distinct proof"))?
            .require_source_current()?;
        let input = self.source.prepare_capture(cut)?;
        input.begin_with_initial_wait(budget, process, publish)
    }
}
