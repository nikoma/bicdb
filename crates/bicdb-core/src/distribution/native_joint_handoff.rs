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
    /// `activated_quorum_sha256` commits to the durable catalogue activation
    /// of EVERY member of the new set, each observed released and open under
    /// a fresh metadata read of its own.
    AwaitingCleanup,
    /// The retired source durably recorded its retirement from the range
    /// after reading `AwaitingCleanup` through its own committed metadata
    /// read; `source_cleanup_sha256` commits to that record. The record is a
    /// tombstone of membership: the source's permanent handoff promise and
    /// range rows are retained. Physical row reclamation and its empty-range
    /// audit are a separate protocol and are NOT asserted by this stage.
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
    pub fn activated_quorum_sha256(&self) -> Option<&str> {
        self.activated_quorum_sha256.as_deref()
    }
    pub fn source_cleanup_sha256(&self) -> Option<&str> {
        self.source_cleanup_sha256.as_deref()
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
    /// `AwaitingActivation` to `AwaitingCleanup`. Returns false, changing
    /// nothing, when exactly this activation is already recorded.
    fn record_activation(
        &mut self,
        decision_sha256: &str,
        activated_quorum_sha256: &str,
    ) -> Result<bool> {
        if self.decision_sha256 != decision_sha256 {
            return Err(cluster_error(
                "joint activation record belongs to another decision",
            ));
        }
        if !sha256_hex(activated_quorum_sha256) {
            return Err(cluster_error("joint activation quorum digest is damaged"));
        }
        match self.stage {
            NativeJointHandoffStageV1::AwaitingActivation => {}
            NativeJointHandoffStageV1::AwaitingCleanup | NativeJointHandoffStageV1::Cleaned
                if self.activated_quorum_sha256.as_deref() == Some(activated_quorum_sha256) =>
            {
                return Ok(false)
            }
            NativeJointHandoffStageV1::AwaitingCleanup | NativeJointHandoffStageV1::Cleaned => {
                return Err(cluster_error(
                    "a different joint activation quorum is already recorded",
                ))
            }
            _ => {
                return Err(cluster_error(
                    "joint activation record requires its committed final membership",
                ))
            }
        }
        self.stage = NativeJointHandoffStageV1::AwaitingCleanup;
        self.activated_quorum_sha256 = Some(activated_quorum_sha256.into());
        Ok(true)
    }
    /// `AwaitingCleanup` to `Cleaned`. Returns false, changing nothing, when
    /// exactly this source retirement is already recorded.
    fn record_source_cleanup(
        &mut self,
        decision_sha256: &str,
        activated_quorum_sha256: &str,
        source_cleanup_sha256: &str,
    ) -> Result<bool> {
        if self.decision_sha256 != decision_sha256 {
            return Err(cluster_error(
                "joint source retirement belongs to another decision",
            ));
        }
        if !sha256_hex(source_cleanup_sha256) {
            return Err(cluster_error("joint source retirement digest is damaged"));
        }
        if self.stage < NativeJointHandoffStageV1::AwaitingCleanup {
            return Err(cluster_error(
                "joint source retirement requires the recorded activation quorum",
            ));
        }
        if self.activated_quorum_sha256.as_deref() != Some(activated_quorum_sha256) {
            return Err(cluster_error(
                "joint source retirement observed another activation quorum",
            ));
        }
        if self.stage == NativeJointHandoffStageV1::Cleaned {
            if self.source_cleanup_sha256.as_deref() == Some(source_cleanup_sha256) {
                return Ok(false);
            }
            return Err(cluster_error(
                "a different joint source retirement is already recorded",
            ));
        }
        self.stage = NativeJointHandoffStageV1::Cleaned;
        self.source_cleanup_sha256 = Some(source_cleanup_sha256.into());
        Ok(true)
    }
    fn immutable_binding(&self) -> Self {
        let mut record = self.clone();
        record.stage = NativeJointHandoffStageV1::Certified;
        record.activated_quorum_sha256 = None;
        record.source_cleanup_sha256 = None;
        record
    }
}
fn sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
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
        self.require_joint_metadata_domain(proof.data(), fresh, business)
    }
    /// The builder is an ephemeral view of exactly the fresh committed
    /// configuration, in the decision's own cluster/member domain.
    fn require_joint_metadata_domain(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        if !self.ephemeral {
            return Err(cluster_error("native joint metadata requires an ephemeral consensus-proposal view; local topology-file publication is insufficient"));
        }
        fresh.require_live_request(business)?;
        let observed = fresh.committed_topology(business)?;
        if observed != &self.topology {
            return Err(cluster_error(
                "joint metadata builder is not the actual fresh committed topology",
            ));
        }
        data.require_metadata_domain(observed, fresh.restore_activation(business)?)
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
    /// Record that every member of the new set durably activated the range
    /// and was released. The proof is the live all-released quorum itself;
    /// no digest is accepted from a caller. Membership does not change. This
    /// permits the retirement of the old source and nothing else. Returns
    /// false, committing nothing, when this activation is already recorded.
    pub(crate) fn record_native_joint_activation(
        &mut self,
        proof: &crate::distribution_range_handoff::NativeJointActivatedQuorum,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        proof.require_current()?;
        let data = proof.data();
        self.require_joint_metadata_domain(data, fresh, business)?;
        let id = data.promise().relocation_id;
        let plan = self
            .topology
            .relocations
            .get(&id)
            .ok_or_else(|| cluster_error("joint plan absent"))?;
        let record = plan
            .native_joint_handoff_v1
            .as_ref()
            .ok_or_else(|| cluster_error("joint final membership decision absent"))?;
        let mut next = record.clone();
        let changed = next.record_activation(data.checksum_sha256(), proof.checksum_sha256())?;
        let range = self
            .topology
            .range_by_id(data.old_range().id)
            .ok_or_else(|| cluster_error("joint range absent"))?;
        let mut canonical = range.clone();
        canonical.approximate_bytes = 0;
        canonical.approximate_qps = 0;
        canonical.replicas.sort_by_key(|r| r.id);
        if canonical != *data.activation_range() || plan.promoted_epoch != Some(range.epoch) {
            return Err(cluster_error(
                "joint activation record is not for the committed final membership",
            ));
        }
        if !changed {
            return Ok(false);
        }
        proof.require_current()?;
        let plan = self.topology.relocations.get_mut(&id).unwrap();
        plan.native_joint_handoff_v1 = Some(next);
        plan.updated_at_ms = now_ms;
        fresh.require_live_request(business)?;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{id} native new data membership activated on every member; source retirement pending"),
        )?;
        Ok(true)
    }
    /// Record the retired source's own durable retirement from the range and
    /// complete the relocation. The proof is the receipt the source's service
    /// issued after ITS OWN committed metadata read showed the recorded
    /// activation quorum. Membership does not change and no row is deleted.
    /// Returns false, committing nothing, when it is already recorded.
    pub(crate) fn complete_native_joint_source_retirement(
        &mut self,
        proof: &crate::distribution_data::NativeJointSourceRetired,
        fresh: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
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
        let id = proof.relocation_id();
        let plan = self
            .topology
            .relocations
            .get(&id)
            .ok_or_else(|| cluster_error("joint plan absent"))?;
        let record = plan
            .native_joint_handoff_v1
            .as_ref()
            .ok_or_else(|| cluster_error("joint activation record absent"))?;
        let node = self
            .topology
            .nodes
            .get(proof.node_id())
            .ok_or_else(|| cluster_error("retired joint source is not a member"))?;
        if &self.topology.cluster_id != proof.cluster_id()
            || plan.source.as_ref() != Some(proof.node_id())
            || node.incarnation != proof.node_incarnation()
            || plan.range_id != proof.range_id()
            || plan.promoted_epoch != Some(proof.activation_epoch())
            || plan
                .handoff_session
                .as_ref()
                .is_none_or(|s| s.session_id != proof.session_id() || s.nonce != proof.nonce())
        {
            return Err(cluster_error(
                "joint source retirement is not this relocation's retired source",
            ));
        }
        // Stages before Cleaned are validated against the committed range:
        // the source is absent from it. A Cleaned record is immutable.
        if record.stage() != NativeJointHandoffStageV1::Cleaned
            && self
                .topology
                .range_by_id(plan.range_id)
                .is_none_or(|range| {
                    range.epoch != proof.activation_epoch()
                        || range.replicas.iter().any(|r| &r.node_id == proof.node_id())
                })
        {
            return Err(cluster_error(
                "joint source retirement is not for the committed final membership",
            ));
        }
        let mut next = record.clone();
        if !next.record_source_cleanup(
            proof.decision_sha256(),
            proof.activated_quorum_sha256(),
            proof.checksum_sha256(),
        )? {
            return Ok(false);
        }
        let plan = self.topology.relocations.get_mut(&id).unwrap();
        plan.native_joint_handoff_v1 = Some(next);
        plan.phase = RelocationPhase::Completed;
        plan.updated_at_ms = now_ms;
        plan.last_error = None;
        fresh.require_live_request(business)?;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{id} native retired source recorded its retirement; relocation completed"),
        )?;
        Ok(true)
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

#[cfg(test)]
mod native_joint_handoff_completion_tests {
    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }
    fn record(stage: NativeJointHandoffStageV1) -> NativeJointHandoffMetadataV1 {
        NativeJointHandoffMetadataV1 {
            version: 1,
            session_id: Uuid::from_u128(7),
            nonce: Uuid::from_u128(9),
            decision_sha256: digest('d'),
            old_configuration_sha256: digest('1'),
            new_configuration_sha256: digest('2'),
            range_index: 3,
            logical_genesis_sha256: digest('3'),
            logical_history_sha256: digest('4'),
            source_snapshot_db_sequence: 5,
            target_local_snapshot_db_sequence: 6,
            stage,
            activated_quorum_sha256: (stage >= NativeJointHandoffStageV1::AwaitingCleanup)
                .then(|| digest('a')),
            source_cleanup_sha256: (stage == NativeJointHandoffStageV1::Cleaned)
                .then(|| digest('c')),
        }
    }
    fn plan(record: &NativeJointHandoffMetadataV1) -> RangeRelocation {
        let completed = record.stage == NativeJointHandoffStageV1::Cleaned;
        RangeRelocation {
            id: RelocationId::new(1).unwrap(),
            plan_id: "joined-data-1".into(),
            handoff_session: Some(RangeHandoffSession {
                session_id: record.session_id,
                nonce: record.nonce,
            }),
            native_joint_handoff_v1: Some(record.clone()),
            range_id: RangeId::new(1).unwrap(),
            expected_epoch: 4,
            learner_epoch: 5,
            promoted_epoch: Some(7),
            source: Some(ClusterNodeId::new("n1").unwrap()),
            target: ClusterNodeId::new("n5").unwrap(),
            target_replica_id: ReplicaId::new(9).unwrap(),
            reason: ReplicaMoveReason::LoadBalance,
            phase: if completed {
                RelocationPhase::Completed
            } else {
                RelocationPhase::CleaningUp
            },
            resume_phase: None,
            snapshot_id: None,
            snapshot_sha256: None,
            snapshot_bytes_copied: 0,
            snapshot_resume_after_key: None,
            snapshot_commit_sequence: None,
            destination_durable_commit_sequence: None,
            source_commit_sequence: None,
            cleanup_resume_after_key: None,
            cleanup_records_deleted: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
            last_error: None,
        }
    }
    fn topology(record: &NativeJointHandoffMetadataV1) -> ClusterTopology {
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("cluster-a").unwrap(),
            range_image_bootstrap_id: None,
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "127.0.0.1:9444".into(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 10_000,
            replication_factor: 3,
            initial_ranges: 4,
            default_placement: PlacementPolicy::default(),
            suspect_after_ms: 100,
            dead_after_ms: 200,
            topology_history_limit: 4,
            metadata_election_timeout_ms: default_metadata_election_timeout_ms(),
            metadata_heartbeat_interval_ms: default_metadata_heartbeat_interval_ms(),
            transport: ClusterNetworkTransportConfig::default(),
        };
        let local =
            ClusterNode::new(config.node_id.clone(), "127.0.0.1:9444", 1, 10_000, 1).unwrap();
        let mut topology = ClusterTopology::new(&config, local, 1).unwrap();
        topology.format_version = NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION;
        let plan = plan(record);
        topology.relocations.insert(plan.id, plan);
        topology
    }

    #[test]
    fn native_joint_handoff_completion_activation_record_requires_final_membership() {
        use NativeJointHandoffStageV1::*;
        for early in [Certified, PromotedFrozen] {
            let mut early_record = record(early);
            let refused = early_record
                .record_activation(&digest('d'), &digest('a'))
                .unwrap_err();
            assert!(refused
                .to_string()
                .contains("requires its committed final membership"));
            assert_eq!(early_record, record(early));
        }
        let mut current = record(AwaitingActivation);
        // A proof of another decision, and a damaged digest, change nothing.
        assert!(current
            .record_activation(&digest('e'), &digest('a'))
            .unwrap_err()
            .to_string()
            .contains("another decision"));
        assert!(current.record_activation(&digest('d'), "A").is_err());
        assert!(current
            .record_activation(&digest('d'), &digest('G'))
            .is_err());
        assert_eq!(current, record(AwaitingActivation));
        assert!(current
            .record_activation(&digest('d'), &digest('a'))
            .unwrap());
        assert_eq!(current, record(AwaitingCleanup));
        current.validate_for(&plan(&current)).unwrap();
        // The same activation again is a no-op; another one is refused, also
        // after the relocation completed.
        assert!(!current
            .record_activation(&digest('d'), &digest('a'))
            .unwrap());
        assert!(current
            .record_activation(&digest('d'), &digest('b'))
            .unwrap_err()
            .to_string()
            .contains("different joint activation quorum"));
        let mut cleaned = record(Cleaned);
        assert!(!cleaned
            .record_activation(&digest('d'), &digest('a'))
            .unwrap());
        assert!(cleaned
            .record_activation(&digest('d'), &digest('b'))
            .is_err());
        assert_eq!(cleaned, record(Cleaned));
    }

    #[test]
    fn native_joint_handoff_completion_source_retirement_requires_recorded_activation() {
        use NativeJointHandoffStageV1::*;
        for early in [Certified, PromotedFrozen, AwaitingActivation] {
            let mut early_record = record(early);
            assert!(early_record
                .record_source_cleanup(&digest('d'), &digest('a'), &digest('c'))
                .unwrap_err()
                .to_string()
                .contains("requires the recorded activation quorum"));
            assert_eq!(early_record, record(early));
        }
        let mut current = record(AwaitingCleanup);
        assert!(current
            .record_source_cleanup(&digest('e'), &digest('a'), &digest('c'))
            .unwrap_err()
            .to_string()
            .contains("another decision"));
        // The source observed an activation quorum that is not the recorded one.
        assert!(current
            .record_source_cleanup(&digest('d'), &digest('b'), &digest('c'))
            .unwrap_err()
            .to_string()
            .contains("another activation quorum"));
        assert!(current
            .record_source_cleanup(&digest('d'), &digest('a'), "c")
            .is_err());
        assert_eq!(current, record(AwaitingCleanup));
        assert!(current
            .record_source_cleanup(&digest('d'), &digest('a'), &digest('c'))
            .unwrap());
        assert_eq!(current, record(Cleaned));
        current.validate_for(&plan(&current)).unwrap();
        assert!(!current
            .record_source_cleanup(&digest('d'), &digest('a'), &digest('c'))
            .unwrap());
        assert!(current
            .record_source_cleanup(&digest('d'), &digest('a'), &digest('f'))
            .unwrap_err()
            .to_string()
            .contains("different joint source retirement"));
        assert_eq!(current, record(Cleaned));
    }

    #[test]
    fn native_joint_handoff_completion_validators_bind_stage_digests_and_phase() {
        use NativeJointHandoffStageV1::*;
        for stage in [AwaitingActivation, AwaitingCleanup, Cleaned] {
            let valid = record(stage);
            valid.validate_for(&plan(&valid)).unwrap();
        }
        // A stage without its digest, and a digest without its stage.
        let mut missing = record(AwaitingCleanup);
        missing.activated_quorum_sha256 = None;
        assert!(missing.validate_for(&plan(&missing)).is_err());
        let mut early = record(AwaitingActivation);
        early.activated_quorum_sha256 = Some(digest('a'));
        assert!(early.validate_for(&plan(&early)).is_err());
        let mut missing = record(Cleaned);
        missing.source_cleanup_sha256 = None;
        assert!(missing.validate_for(&plan(&missing)).is_err());
        let mut early = record(AwaitingCleanup);
        early.source_cleanup_sha256 = Some(digest('c'));
        assert!(early.validate_for(&plan(&early)).is_err());
        // Cleaned and the Completed phase imply each other.
        let cleaned = record(Cleaned);
        let base = topology(&cleaned);
        let mut unfinished = plan(&cleaned);
        cleaned.validate_topology(&unfinished, &base).unwrap();
        assert!(!unfinished.is_active());
        unfinished.phase = RelocationPhase::CleaningUp;
        assert!(cleaned
            .validate_topology(&unfinished, &base)
            .unwrap_err()
            .to_string()
            .contains("cleaned native joint handoff is not completed"));
        let awaiting = record(AwaitingCleanup);
        let mut premature = plan(&awaiting);
        assert!(premature.is_active());
        premature.phase = RelocationPhase::Completed;
        assert!(awaiting.validate_topology(&premature, &base).is_err());
        assert!(premature.is_active());
    }

    #[test]
    fn native_joint_handoff_completion_records_never_regress_or_change() {
        use NativeJointHandoffStageV1::*;
        let forward = [AwaitingActivation, AwaitingCleanup, Cleaned];
        for (i, from) in forward.iter().enumerate() {
            for (j, to) in forward.iter().enumerate() {
                let outcome = validate_native_joint_handoff_preservation(
                    &topology(&record(*from)),
                    &topology(&record(*to)),
                );
                assert_eq!(outcome.is_ok(), j >= i, "{from:?} -> {to:?}");
            }
        }
        let recorded = topology(&record(AwaitingCleanup));
        let mut changed = record(Cleaned);
        changed.activated_quorum_sha256 = Some(digest('b'));
        assert!(
            validate_native_joint_handoff_preservation(&recorded, &topology(&changed)).is_err()
        );
        let cleaned = topology(&record(Cleaned));
        let mut changed = record(Cleaned);
        changed.source_cleanup_sha256 = Some(digest('f'));
        assert!(validate_native_joint_handoff_preservation(&cleaned, &topology(&changed)).is_err());
        // A completed joint relocation stays as a tombstone.
        let mut erased = cleaned.clone();
        erased.relocations.clear();
        assert!(validate_native_joint_handoff_preservation(&cleaned, &erased).is_err());
    }
}
