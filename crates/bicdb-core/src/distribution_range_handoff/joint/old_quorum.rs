//! Distinct actual OLD data-member closure. Every observed irreversible
//! command/pending constraint is checked against the selected actual spine.
use super::*;
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_data::{
    ActualServiceNativeJointConstraints, ClusterDataNodeService, NativeJointIrrevocableFacts,
};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct NativeJointOldRound {
    plan: Arc<NativeJointHandoffPlan>,
    metadata: HandoffCutRound,
    observations: BTreeMap<ClusterNodeId, Arc<NativeJointOldObservation>>,
    constraints: Vec<NativeJointIrrevocableFacts>,
}
pub(crate) struct NativeJointOldClosedQuorum {
    plan: Arc<NativeJointHandoffPlan>,
    observations: BTreeMap<ClusterNodeId, Arc<NativeJointOldObservation>>,
    constraints: Vec<NativeJointIrrevocableFacts>,
    selected_source: ClusterNodeId,
    range_index: u64,
    h0: String,
    h: String,
    cut: Option<CertifiedRangeImageCut>,
}
impl NativeJointOldRound {
    pub(crate) fn begin(
        metadata: MetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
    ) -> Result<Self> {
        let (plan, round) = NativeJointHandoffPlan::begin(metadata, business, relocation)?;
        Ok(Self {
            plan: Arc::new(plan),
            metadata: round,
            observations: BTreeMap::new(),
            constraints: Vec::new(),
        })
    }
    pub(crate) fn plan(&self) -> Arc<NativeJointHandoffPlan> {
        self.plan.clone()
    }
    /// Only actual service evidence enters here, never a caller-selected list.
    /// It is nonvoting even when it records a CQ/pending point above every
    /// currently closable peer. Such a point fences finish until real repair.
    pub(crate) fn record_constraints(
        &mut self,
        evidence: ActualServiceNativeJointConstraints,
        service: &Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<()> {
        self.metadata.authority.require_live()?;
        let facts = {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint constraints service busy"))?;
            evidence.into_actual_facts(&actual)?
        };
        if facts.scope_sha256 != self.plan.checksum_sha256()?
            || !self.plan.old_members().contains_key(&facts.node_id)
            || self.constraints.len() >= 128
        {
            return Err(invalid(
                "joint irreversible observation is outside scope or exceeds bound",
            ));
        }
        self.constraints.push(facts);
        Ok(())
    }
    pub(crate) fn record_closed(&mut self, observation: NativeJointOldObservation) -> Result<()> {
        self.metadata.authority.require_live()?;
        observation.require_issuance_current()?;
        if observation.scope_sha256()? != self.plan.checksum_sha256()?
            || !self.plan.old_members().contains_key(observation.node_id())
        {
            return Err(invalid(
                "closed observation is outside actual old data configuration",
            ));
        }
        if self.observations.contains_key(observation.node_id()) {
            return Err(invalid(
                "duplicate actual old data member cannot count twice",
            ));
        }
        self.observations
            .insert(observation.node_id().clone(), Arc::new(observation));
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointOldClosedQuorum> {
        self.metadata.authority.require_live()?;
        if self.observations.len() < self.plan.old_members().len() / 2 + 1 {
            return Err(invalid("actual old data closure has no distinct majority"));
        }
        for observation in self.observations.values() {
            observation.require_issuance_current()?;
        }
        let selected = self
            .observations
            .values()
            .max_by_key(|obs| {
                (
                    obs.range_index(),
                    obs.node_id() == &self.plan.old_range().leader,
                    obs.node_id().clone(),
                )
            })
            .ok_or_else(|| invalid("old data closure is empty"))?;
        let r = selected.range_index();
        let h0 = selected.logical_genesis_sha256().to_owned();
        let h = selected.logical_history_sha256().to_owned();
        for observation in self.observations.values() {
            selected.require_ancestor(
                observation.range_index(),
                observation.logical_genesis_sha256(),
                observation.logical_history_sha256(),
            )?;
        }
        for facts in &self.constraints {
            self.metadata.authority.require_live()?;
            if facts.actual_resolved > r || facts.actual_compacted > r {
                return Err(invalid(
                    "observed actual logical floor is beyond the complete selected native cut",
                ));
            }
            if let Some((index, genesis, history)) = &facts.native_point {
                selected.require_ancestor(*index, genesis, history)?;
            }
            for (index, checksum) in &facts.commands {
                if *index > r || selected.command_at(*index)?.as_deref() != Some(checksum.as_str())
                {
                    return Err(invalid("selected old native cut omits/conflicts with an observed irreversible CQ/pending command"));
                }
            }
        }
        let selected_source = selected.node_id().clone();
        let topology = self.metadata.authority.topology()?;
        let relocation = topology
            .relocations
            .get(&self.plan.promise().relocation_id)
            .ok_or_else(|| invalid("actual old metadata plan disappeared"))?
            .clone();
        let source = topology
            .nodes
            .get(&selected_source)
            .ok_or_else(|| invalid("selected old source member absent"))?;
        let target = topology
            .nodes
            .get(&relocation.target)
            .ok_or_else(|| invalid("planned new member absent"))?;
        let cut = CertifiedRangeImageCut {
            promise: self.metadata.request.clone(),
            cluster_id: topology.cluster_id.clone(),
            range: self.metadata.range.clone(),
            range_index: r,
            source_id: selected_source.clone(),
            source_incarnation: source.incarnation,
            target_id: target.id.clone(),
            target_incarnation: target.incarnation,
            intended_activation_range: self.metadata.intended_activation_range.clone(),
            intended_activation_members: self.plan.new_members().clone(),
            bootstrap_id: topology.range_image_bootstrap_id,
            relocation: relocation.clone(),
            old_member_credentials: self
                .plan
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
            expected_schema_sha256: Some(self.plan.data().schema_sha256.clone()),
            restore_activation: self.plan.data().restore_activation.clone(),
        };
        self.metadata.authority.require_live()?;
        Ok(NativeJointOldClosedQuorum {
            plan: self.plan,
            observations: self.observations,
            constraints: self.constraints,
            selected_source,
            range_index: r,
            h0,
            h,
            cut: Some(cut),
        })
    }
}
impl NativeJointOldClosedQuorum {
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.h0
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.h
    }
    pub(crate) fn plan(&self) -> Arc<NativeJointHandoffPlan> {
        self.plan.clone()
    }
    pub(crate) fn begin_capture(
        &mut self,
        budget: std::time::Duration,
        parent: &crate::CancellationToken,
    ) -> Result<crate::distribution_range_image::RangeImageCapture> {
        for observation in self.observations.values() {
            observation.require_current()?;
        }
        let cut = self
            .cut
            .take()
            .ok_or_else(|| invalid("actual old quorum capture already started"))?;
        let input = self.observations[&self.selected_source].prepare_capture(cut)?;
        // The actual DB/service guards used to assemble inputs are gone here.
        // Its absolute shippable wait is allowed only outside those locks.
        input.begin(budget, parent)
    }
}

impl NativeJointOldClosedQuorum {
    pub(crate) fn require_current(&self) -> Result<()> {
        for observation in self.observations.values() {
            observation.require_current()?;
        }
        Ok(())
    }
    pub(super) fn observation(
        &self,
        node: &ClusterNodeId,
    ) -> Result<Arc<NativeJointOldObservation>> {
        self.observations
            .get(node)
            .cloned()
            .ok_or_else(|| invalid("new vote is not an actual retained old closure"))
    }
    pub(super) fn old_receipts(&self) -> BTreeMap<ClusterNodeId, String> {
        self.observations
            .iter()
            .map(|(node, obs)| (node.clone(), obs.receipt_sha256().into()))
            .collect()
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        self.require_current()?;
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    "bicdb-native-joint-old-closed-quorum-v1",
                    self.plan.checksum_sha256()?,
                    self.range_index,
                    &self.h0,
                    &self.h,
                    &self.selected_source,
                    self.old_receipts(),
                    &self.constraints,
                ),
                2 * 1024 * 1024,
            )?,
        )))
    }
}

impl NativeJointOldClosedQuorum {
    pub(super) fn release_capture_pins_for_decision(&self) -> Result<()> {
        self.require_current()?;
        for observation in self.observations.values() {
            observation.release_capture_pin_for_decision()?;
        }
        self.require_current()
    }
}

impl NativeJointOldClosedQuorum {
    pub(super) fn materialized_facts(
        &self,
    ) -> BTreeMap<ClusterNodeId, super::materialized::NativeJointMaterializedFacts> {
        self.observations
            .iter()
            .map(|(node, obs)| (node.clone(), obs.materialized_facts()))
            .collect()
    }
    pub(super) fn constraints(&self) -> &[NativeJointIrrevocableFacts] {
        &self.constraints
    }
    pub(super) fn selected_source(&self) -> &ClusterNodeId {
        &self.selected_source
    }
}

impl NativeJointOldClosedQuorum {
    pub(super) fn old_observation(
        &self,
        node: &ClusterNodeId,
    ) -> Option<&Arc<NativeJointOldObservation>> {
        self.observations.get(node)
    }
}
