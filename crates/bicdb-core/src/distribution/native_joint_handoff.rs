//! Metadata correlation for an independently certified native data handoff.
//! Decoding this record grants no image, write, cleanup or read permission.
use super::*;

pub const NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION: u32 = 3;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeJointHandoffStageV1 {
    /// Both old-closed and intended-new-materialized quorums decided one cut.
    Certified,
    /// Intermediate union membership; every ordinary writer remains fenced.
    PromotedFrozen,
    /// Final membership is committed; physical source cleanup is still pending.
    AwaitingActivation,
    /// A distinct actual new-data activation quorum permits retirement only.
    AwaitingCleanup,
    /// Actual protected source tombstone and full empty-range audit completed.
    Cleaned,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeJointHandoffMetadataV1 {
    version: u32,
    session_id: Uuid,
    nonce: Uuid,
    decision_sha256: String,
    old_configuration_sha256: String,
    new_configuration_sha256: String,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    source_snapshot_db_sequence: u64,
    target_local_snapshot_db_sequence: u64,
    stage: NativeJointHandoffStageV1,
    activated_quorum_sha256: Option<String>,
    source_cleanup_sha256: Option<String>,
}
impl NativeJointHandoffMetadataV1 {
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
    pub fn nonce(&self) -> Uuid {
        self.nonce
    }
    pub fn old_configuration_sha256(&self) -> &str {
        &self.old_configuration_sha256
    }
    pub fn new_configuration_sha256(&self) -> &str {
        &self.new_configuration_sha256
    }
    pub fn stage(&self) -> NativeJointHandoffStageV1 {
        self.stage
    }
    pub fn decision_sha256(&self) -> &str {
        &self.decision_sha256
    }
    pub fn range_index(&self) -> u64 {
        self.range_index
    }
    pub fn logical_genesis_sha256(&self) -> &str {
        &self.logical_genesis_sha256
    }
    pub fn logical_history_sha256(&self) -> &str {
        &self.logical_history_sha256
    }
    pub(crate) fn validate_for(&self, plan: &RangeRelocation) -> Result<()> {
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
        };
        if self.version != 1
            || plan.id.get() == 0
            || plan.range_id.get() == 0
            || plan.target_replica_id.get() == 0
            || plan.expected_epoch == 0
            || plan.expected_epoch.checked_add(1) != Some(plan.learner_epoch)
            || plan.plan_id.is_empty()
            || plan.plan_id.len() > 1024
            || plan
                .source
                .as_ref()
                .is_none_or(|source| source == &plan.target)
            || self.old_configuration_sha256 == self.new_configuration_sha256
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || plan.handoff_session.as_ref().is_none_or(|session| {
                session.session_id != self.session_id || session.nonce != self.nonce
            })
            || [
                &self.decision_sha256,
                &self.old_configuration_sha256,
                &self.new_configuration_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
            ]
            .into_iter()
            .any(|value| !digest(value))
            || self
                .activated_quorum_sha256
                .as_ref()
                .is_some_and(|v| !digest(v))
            || self
                .source_cleanup_sha256
                .as_ref()
                .is_some_and(|v| !digest(v))
            || (self.stage >= NativeJointHandoffStageV1::AwaitingCleanup)
                != self.activated_quorum_sha256.is_some()
            || (self.stage == NativeJointHandoffStageV1::Cleaned)
                != self.source_cleanup_sha256.is_some()
        {
            return Err(cluster_error(
                "invalid native joint handoff metadata correlation",
            ));
        }
        Ok(())
    }
    fn immutable_binding(&self) -> Self {
        let mut record = self.clone();
        record.stage = NativeJointHandoffStageV1::Certified;
        record.activated_quorum_sha256 = None;
        record.source_cleanup_sha256 = None;
        record
    }
}
/// Sequential proposals and authoritative snapshot jumps preserve every known
/// decision and tombstone. Stage data alone never authorizes a runtime action.
pub(crate) fn validate_native_joint_handoff_preservation(
    previous: &ClusterTopology,
    next: &ClusterTopology,
) -> Result<()> {
    if previous.format_version >= NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION
        && next.format_version < NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION
    {
        return Err(cluster_error(
            "native joint handoff topology reader fence cannot be downgraded",
        ));
    }
    for (id, plan) in &previous.relocations {
        let Some(old) = &plan.native_joint_handoff_v1 else {
            continue;
        };
        let new = next
            .relocations
            .get(id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| cluster_error("native joint decision/tombstone cannot be erased"))?;
        if new.immutable_binding() != old.immutable_binding()
            || new.stage < old.stage
            || old
                .activated_quorum_sha256
                .as_ref()
                .is_some_and(|v| new.activated_quorum_sha256.as_ref() != Some(v))
            || old
                .source_cleanup_sha256
                .as_ref()
                .is_some_and(|v| new.source_cleanup_sha256.as_ref() != Some(v))
        {
            return Err(cluster_error("native joint decision changed or regressed"));
        }
    }
    Ok(())
}

impl NativeJointHandoffMetadataV1 {
    pub(crate) fn from_actual_decision(
        proof: &crate::distribution_range_handoff::NativeJointDurableDecision,
    ) -> Result<Self> {
        proof.require_current()?;
        let actual = proof.data();
        let result = Self {
            version: 1,
            session_id: actual.promise().session_id,
            nonce: actual.promise().nonce,
            decision_sha256: actual.checksum_sha256().into(),
            old_configuration_sha256: actual.promise().old_config_sha256.clone(),
            new_configuration_sha256: actual.promise().new_config_sha256.clone(),
            range_index: actual.range_index(),
            logical_genesis_sha256: actual.logical_genesis_sha256().into(),
            logical_history_sha256: actual.logical_history_sha256().into(),
            source_snapshot_db_sequence: actual.source_snapshot_db_sequence()?,
            target_local_snapshot_db_sequence: actual.target_local_snapshot_db_sequence()?,
            stage: NativeJointHandoffStageV1::Certified,
            activated_quorum_sha256: None,
            source_cleanup_sha256: None,
        };
        Ok(result)
    }
    pub(crate) fn validate_topology(
        &self,
        plan: &RangeRelocation,
        topology: &ClusterTopology,
    ) -> Result<()> {
        self.validate_for(plan)?;
        let (phase, epoch, target_role, source_present) = match self.stage {
            NativeJointHandoffStageV1::Certified => (
                RelocationPhase::ReadyToPromote,
                plan.learner_epoch,
                RangeReplicaRole::Learner,
                true,
            ),
            NativeJointHandoffStageV1::PromotedFrozen => (
                RelocationPhase::Promoted,
                plan.learner_epoch
                    .checked_add(1)
                    .ok_or_else(|| cluster_error("joint epoch overflow"))?,
                RangeReplicaRole::Voter,
                true,
            ),
            NativeJointHandoffStageV1::AwaitingActivation
            | NativeJointHandoffStageV1::AwaitingCleanup => (
                RelocationPhase::CleaningUp,
                plan.learner_epoch
                    .checked_add(2)
                    .ok_or_else(|| cluster_error("joint epoch overflow"))?,
                RangeReplicaRole::Voter,
                false,
            ),
            NativeJointHandoffStageV1::Cleaned => {
                if plan.phase != RelocationPhase::Completed
                    || plan.promoted_epoch != plan.learner_epoch.checked_add(2)
                {
                    return Err(cluster_error(
                        "cleaned native joint handoff is not completed",
                    ));
                }
                return Ok(());
            }
        };
        let range = topology
            .range_by_id(plan.range_id)
            .ok_or_else(|| cluster_error("joint range disappeared"))?;
        let target = range
            .replicas
            .iter()
            .find(|r| r.id == plan.target_replica_id)
            .ok_or_else(|| cluster_error("joint target replica disappeared"))?;
        if plan.phase != phase
            || (self.stage == NativeJointHandoffStageV1::Certified && plan.promoted_epoch.is_some())
            || plan.resume_phase.is_some()
            || range.epoch != epoch
            || target.node_id != plan.target
            || target.role != target_role
            || plan
                .source
                .as_ref()
                .is_none_or(|s| range.replicas.iter().any(|r| &r.node_id == s) != source_present)
            || (self.stage != NativeJointHandoffStageV1::Certified
                && plan.promoted_epoch != Some(epoch))
        {
            return Err(cluster_error(
                "native joint metadata stage/configuration/phase mismatch",
            ));
        }
        Ok(())
    }
}
impl DistributionStore {
    fn require_joint_metadata_observation(
        &self,
        proof: &crate::distribution_range_handoff::NativeJointDurableDecision,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        if !self.ephemeral {
            return Err(cluster_error("native joint metadata requires an ephemeral consensus-proposal view; local topology-file publication is insufficient"));
        }
        fresh.require_live_request(business)?;
        proof.require_current()?;
        let observed = fresh.committed_topology(business)?;
        if observed != &self.topology {
            return Err(cluster_error(
                "joint metadata builder is not the actual fresh committed topology",
            ));
        }
        proof
            .data()
            .require_metadata_domain(observed, fresh.restore_activation(business)?)
    }
    /// A real dual-data-majority WAL decision is a prerequisite. Legacy source
    /// and target DB sequence equality is deliberately absent from this path.
    pub(crate) fn certify_native_joint_handoff(
        &mut self,
        proof: &crate::distribution_range_handoff::NativeJointDurableDecision,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        self.require_joint_metadata_observation(proof, fresh, business)?;
        let data = proof.data();
        let id = data.promise().relocation_id;
        let canonical = |range: &RangeDescriptor| {
            let mut r = range.clone();
            r.approximate_bytes = 0;
            r.approximate_qps = 0;
            r.replicas.sort_by_key(|r| r.id);
            r
        };
        let range = self
            .topology
            .range_by_id(data.old_range().id)
            .ok_or_else(|| cluster_error("joint old range absent"))?;
        let record = NativeJointHandoffMetadataV1::from_actual_decision(proof)?;
        if let Some(existing) = &self.topology.relocations[&id].native_joint_handoff_v1 {
            if existing.immutable_binding() != record.immutable_binding() {
                return Err(cluster_error(
                    "different native joint decision already pending",
                ));
            }
            return Ok(false);
        }
        if canonical(range) != *data.old_range() {
            return Err(cluster_error(
                "joint old metadata configuration changed before certification",
            ));
        }
        let plan = self
            .topology
            .relocations
            .get_mut(&id)
            .ok_or_else(|| cluster_error("joint plan absent"))?;
        if !matches!(
            plan.phase,
            RelocationPhase::LearnerAllocated
                | RelocationPhase::SnapshotCopying
                | RelocationPhase::CatchingUp
                | RelocationPhase::ReadyToPromote
        ) {
            return Err(cluster_error(
                "joint certification is not at its exact pending learner phase",
            ));
        }
        plan.native_joint_handoff_v1 = Some(record);
        plan.phase = RelocationPhase::ReadyToPromote;
        plan.updated_at_ms = now_ms;
        plan.last_error = None;
        plan.resume_phase = None;
        self.topology.format_version = self
            .topology
            .format_version
            .max(NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION);
        fresh.require_live_request(business)?;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{id} native OLD/NEW data decision durably certified"),
        )?;
        Ok(true)
    }
    pub(crate) fn promote_native_joint_handoff_frozen(
        &mut self,
        proof: &crate::distribution_range_handoff::NativeJointDurableDecision,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<u64> {
        self.ensure_actor(actor)?;
        self.require_joint_metadata_observation(proof, fresh, business)?;
        let data = proof.data();
        let id = data.promise().relocation_id;
        let plan = self
            .topology
            .relocations
            .get(&id)
            .ok_or_else(|| cluster_error("joint plan absent"))?;
        let record = plan
            .native_joint_handoff_v1
            .as_ref()
            .ok_or_else(|| cluster_error("joint decision not committed"))?;
        if record.decision_sha256() != data.checksum_sha256()
            || record.stage != NativeJointHandoffStageV1::Certified
        {
            return Err(cluster_error(
                "joint promotion requires its exact certified decision",
            ));
        }
        let target = plan.target_replica_id;
        let range = self
            .topology
            .ranges
            .values_mut()
            .find(|r| r.id == data.old_range().id)
            .ok_or_else(|| cluster_error("joint range absent"))?;
        if range.epoch != data.old_range().epoch {
            return Err(cluster_error("joint learner epoch changed"));
        }
        let replica = range
            .replicas
            .iter_mut()
            .find(|r| r.id == target)
            .ok_or_else(|| cluster_error("joint target replica absent"))?;
        if replica.role != RangeReplicaRole::Learner {
            return Err(cluster_error("joint target is not pending learner"));
        }
        replica.role = RangeReplicaRole::Voter;
        range.leader = data.activation_range().leader.clone();
        range.epoch = range
            .epoch
            .checked_add(1)
            .ok_or_else(|| cluster_error("joint epoch overflow"))?;
        let epoch = range.epoch;
        let plan = self.topology.relocations.get_mut(&id).unwrap();
        plan.phase = RelocationPhase::Promoted;
        plan.promoted_epoch = Some(epoch);
        plan.updated_at_ms = now_ms;
        plan.native_joint_handoff_v1.as_mut().unwrap().stage =
            NativeJointHandoffStageV1::PromotedFrozen;
        fresh.require_live_request(business)?;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{id} native union membership committed; all data writers remain frozen"),
        )?;
        Ok(epoch)
    }
    /// Membership publication precedes physical source deletion. Its explicit
    /// pending-activation/cleanup stage prevents false convergence reporting.
    pub(crate) fn finalize_native_joint_membership_frozen(
        &mut self,
        proof: &crate::distribution_range_handoff::NativeJointDurableDecision,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<u64> {
        self.ensure_actor(actor)?;
        self.require_joint_metadata_observation(proof, fresh, business)?;
        let data = proof.data();
        let id = data.promise().relocation_id;
        let plan = &self.topology.relocations[&id];
        let record = plan
            .native_joint_handoff_v1
            .as_ref()
            .ok_or_else(|| cluster_error("joint union decision absent"))?;
        if record.decision_sha256() != data.checksum_sha256()
            || record.stage != NativeJointHandoffStageV1::PromotedFrozen
        {
            return Err(cluster_error(
                "joint final membership requires its exact frozen union decision",
            ));
        }
        let range = self
            .topology
            .ranges
            .values_mut()
            .find(|r| r.id == data.old_range().id)
            .ok_or_else(|| cluster_error("joint range absent"))?;
        if range.epoch.checked_add(1) != Some(data.activation_range().epoch) {
            return Err(cluster_error("joint union epoch changed"));
        }
        *range = data.activation_range().clone();
        let epoch = range.epoch;
        let plan = self.topology.relocations.get_mut(&id).unwrap();
        plan.phase = RelocationPhase::CleaningUp;
        plan.promoted_epoch = Some(epoch);
        plan.updated_at_ms = now_ms;
        plan.native_joint_handoff_v1.as_mut().unwrap().stage =
            NativeJointHandoffStageV1::AwaitingActivation;
        fresh.require_live_request(business)?;
        self.commit_change(actor.clone(),now_ms,format!("{id} native final data membership committed; activation and physical cleanup pending"))?;
        Ok(epoch)
    }
}

impl DistributionStore {
    pub(super) fn require_legacy_relocation_mutation(
        &self,
        id: RelocationId,
        changes_membership_or_data: bool,
    ) -> Result<()> {
        let plan = self
            .topology
            .relocations
            .get(&id)
            .ok_or_else(|| cluster_error("relocation absent"))?;
        if plan.native_joint_handoff_v1.is_some() {
            return Err(cluster_error(
                "protected native joint handoff requires its typed phase producer",
            ));
        }
        if changes_membership_or_data {
            let range = self
                .topology
                .range_by_id(plan.range_id)
                .ok_or_else(|| cluster_error("relocation range absent"))?;
            if range
                .replicas
                .iter()
                .filter(|r| r.role == RangeReplicaRole::Voter)
                .any(|r| {
                    self.topology
                        .nodes
                        .get(&r.node_id)
                        .and_then(|n| n.native_store_birth_v1.as_ref())
                        .is_some_and(|birth| birth.writer_enforcement_version() == 1)
                })
            {
                return Err(cluster_error("enforced native membership/cleanup requires actual OLD/NEW data quorum handoff"));
            }
        }
        Ok(())
    }
}
