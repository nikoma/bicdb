//! Pending plan capability. Only an actual fresh range metadata observation
//! creates it; serde scope bytes remain correlation data.
use super::*;
use crate::distribution_consensus::{MetadataReadObservation, MetadataRecoveryRolePurpose};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

#[derive(Debug)]
pub(crate) struct NativeJointHandoffPlan {
    data: NativeJointScopeData,
    metadata_term: u64,
    metadata_read_index: u64,
    metadata_context: Uuid,
    deadline: std::time::Instant,
}
impl NativeJointHandoffPlan {
    pub(super) fn begin(
        observation: MetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
    ) -> Result<(Self, HandoffCutRound)> {
        observation.require_live_request(business)?;
        let metadata_term = observation.term();
        let metadata_read_index = observation.read_index();
        let metadata_context = observation.context_id();
        let deadline = observation.deadline();
        let round = HandoffCutRound::begin_from_observation(observation, business, relocation)?;
        let data = NativeJointScopeData::from_actual_round(&round)?;
        Ok((
            Self {
                data,
                metadata_term,
                metadata_read_index,
                metadata_context,
                deadline,
            },
            round,
        ))
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if std::time::Instant::now() >= self.deadline {
            return Err(invalid("original joint metadata acquisition expired"));
        }
        self.data.validate()
    }
    pub(crate) fn require_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        // The original acquisition bounds issuance of the old round. A later
        // operation on the SAME permanent scope must acquire its own fresh
        // role; retaining this plan never renews either request's deadline.
        self.data.validate()?;
        role.require_live_request()?;
        let MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id,
            plan_id,
            session_id,
            nonce,
        } = role.purpose()
        else {
            return Err(invalid(
                "joint operation needs its exact committed handoff purpose",
            ));
        };
        if *relocation_id != self.data.promise.relocation_id
            || *plan_id != self.data.promise.plan_id
            || *session_id != self.data.promise.session_id
            || *nonce != self.data.promise.nonce
            || role.cluster_id() != &self.data.cluster_id
            || role.bootstrap_id() != self.data.bootstrap_id
            || canonical_range(role.current_range()) != self.data.old_range
            || role.restore_activation() != self.data.restore_activation.as_ref()
            || role.expected_schema_sha256() != Some(self.data.schema_sha256.as_str())
            || role.metadata_term() < self.metadata_term
            || role.metadata_read_index() < self.metadata_read_index
            || role.member_credentials().len() != self.data.old_members.len()
            || role.member_credentials().iter().any(|(id, member)| {
                self.data.old_members.get(id).is_none_or(|expected| {
                    expected.incarnation != member.incarnation
                        || expected.tls_certificate_sha256 != member.active_certificate_sha256
                        || expected.pending_tls_certificate_sha256
                            != member.pending_certificate_sha256
                })
            })
        {
            return Err(invalid(
                "fresh joint role changed the actual pending plan or metadata anchor",
            ));
        }
        Ok(())
    }
    /// Necessary READONLY current-plan comparison. A HostMQ never becomes a
    /// recovery mutation role or an OLD-data quorum through this method.
    pub(crate) fn require_source_host_metadata(
        &self,
        observed: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        source: &ClusterNodeId,
    ) -> Result<()> {
        self.data.validate()?;
        observed.require_live_request(business)?;
        let current = observed.committed_topology(business)?;
        let range = current
            .range_by_id(self.data.old_range.id)
            .ok_or_else(|| invalid("remote image source range absent"))?;
        let plan = current
            .relocations
            .get(&self.data.promise.relocation_id)
            .ok_or_else(|| invalid("remote image Pending plan absent"))?;
        let session = plan
            .handoff_session
            .as_ref()
            .ok_or_else(|| invalid("remote image Pending session absent"))?;
        if observed.requester_node_id() != source
            || !self.data.old_members.contains_key(source)
            || observed.term() < self.metadata_term
            || observed.read_index() < self.metadata_read_index
            || current.cluster_id != self.data.cluster_id
            || current.range_image_bootstrap_id != Some(self.data.bootstrap_id)
            || observed.restore_activation(business)? != self.data.restore_activation.as_ref()
            || canonical_range(range) != self.data.old_range
            || plan.plan_id != self.data.promise.plan_id
            || plan.source.as_ref() != Some(&self.data.old_source)
            || plan.target != self.data.target
            || plan.range_id != range.id
            || plan.learner_epoch != range.epoch
            || session.session_id != self.data.promise.session_id
            || session.nonce != self.data.promise.nonce
            || !matches!(
                plan.phase,
                crate::distribution::RelocationPhase::LearnerAllocated
                    | crate::distribution::RelocationPhase::SnapshotCopying
                    | crate::distribution::RelocationPhase::CatchingUp
                    | crate::distribution::RelocationPhase::ReadyToPromote
            )
        {
            return Err(invalid(
                "source OWN metadata changed actual immutable Pending image plan",
            ));
        }
        for (id, identity) in self
            .data
            .old_members
            .iter()
            .chain(self.data.new_members.iter())
        {
            let node = current
                .nodes
                .get(id)
                .ok_or_else(|| invalid("source image participant absent"))?;
            if node.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
                || node.incarnation != identity.incarnation
                || node.tls_certificate_sha256 != identity.tls_certificate_sha256
                || node.pending_tls_certificate_sha256 != identity.pending_tls_certificate_sha256
                || node.native_store_birth_v1.as_ref() != self.data.enrolled_births.get(id)
                || node
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&self.data.schema_sha256)
            {
                return Err(invalid(
                    "source image current participant/birth/pin/schema differs",
                ));
            }
        }
        observed.require_live_request(business)
    }
    /// Immutable enrolled identity DATA; it cannot mint admission or a role.
    pub(crate) fn enrolled_birth(
        &self,
        node: &ClusterNodeId,
    ) -> Result<&crate::distribution::NativeStoreBirthV1> {
        self.data
            .enrolled_births
            .get(node)
            .ok_or_else(|| invalid("source image participant birth absent from actual OLD scope"))
    }
    pub(crate) fn require_old_native_configuration(
        &self,
        lineage: &crate::db::SourceRangeImageLineage,
    ) -> Result<()> {
        let mut expected = self.data.old_range.clone();
        expected.replicas.retain(|r| r.node_id != self.data.target);
        expected.epoch = expected
            .epoch
            .checked_sub(1)
            .ok_or_else(|| invalid("joint expected native epoch underflow"))?;
        if canonical_range(lineage.native_write_range()?) != canonical_range(&expected)
            || lineage.cluster_id() != &self.data.cluster_id
            || lineage.bootstrap_id() != self.data.bootstrap_id
            || lineage.schema_sha256() != self.data.schema_sha256
            || lineage.restore_activation() != self.data.restore_activation.as_ref()
            || lineage.native_write_members()? != self.data.old_members
        {
            return Err(invalid(
                "joint old native lineage lacks the exact committed learner-allocation ancestry",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_actual_native(
        &self,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        self.data.require_actual_native(native)
    }
    pub(crate) fn promise(&self) -> &RangeHandoffPromise {
        &self.data.promise
    }
    pub(crate) fn old_range(&self) -> &RangeDescriptor {
        &self.data.old_range
    }
    pub(crate) fn activation_range(&self) -> &RangeDescriptor {
        &self.data.activation_range
    }
    pub(crate) fn old_members(
        &self,
    ) -> &std::collections::BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.data.old_members
    }
    pub(crate) fn new_members(
        &self,
    ) -> &std::collections::BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.data.new_members
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        self.data.checksum()
    }
    pub(crate) fn original_deadline(&self) -> std::time::Instant {
        self.deadline
    }
    pub(super) fn data(&self) -> &NativeJointScopeData {
        &self.data
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.metadata_term,
            self.metadata_read_index,
            self.metadata_context,
        )
    }
}

impl NativeJointHandoffPlan {
    pub(crate) fn matches_capture_range(&self, range: &RangeDescriptor) -> bool {
        canonical_range(range) == self.data.old_range
    }
}
