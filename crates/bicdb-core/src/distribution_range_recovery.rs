//! Fresh role witnesses and current-data-quorum recovery authority.
//!
//! Metadata evidence is not a native floor. Wire DTOs, local CQ/Applied labels
//! and progress counters cannot construct any of the opaque admissions here.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::distribution::{
    ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RangeRelocation, RangeReplicaRole,
    SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use crate::distribution_consensus::{
    MetadataReadParticipant, MetadataRecoveryRoleObservation, MetadataRecoveryRolePurpose,
    MetadataRestoreActivation,
};
use crate::distribution_range_consensus::RangeWriteCommand;
use crate::error::{BicDbError, Result};

fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(format!("range recovery: {message}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryMemberIdentity {
    pub incarnation: u64,
    pub active_certificate_sha256: Option<String>,
    pub pending_certificate_sha256: Option<String>,
}

/// An operation-bound independently fresh metadata role. This grants neither
/// a current data quorum nor native DB admission, visibility or write readiness.
#[derive(Debug)]
pub(crate) struct RecoveryRoleAdmission {
    witness: MetadataRecoveryRoleObservation,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range: RangeDescriptor,
    members: BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    local_member: RecoveryMemberIdentity,
    restore: Option<MetadataRestoreActivation>,
    schema: Option<String>,
    relocation: Option<RangeRelocation>,
}

impl RecoveryRoleAdmission {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_witness(
        witness: MetadataRecoveryRoleObservation,
        actual_initiator: &MetadataReadParticipant,
        local_node_id: &ClusterNodeId,
        read_request_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        recovery_nonce: Uuid,
        recipient_challenge: Uuid,
        purpose: &MetadataRecoveryRolePurpose,
    ) -> Result<Self> {
        witness.require_live_request(read_request_id, recipient_challenge)?;
        witness.require_initiator(actual_initiator)?;
        let request = witness.request();
        if &request.recipient_node_id != local_node_id
            || request.range_id != range_id
            || request.range_epoch != range_epoch
            || request.recovery_nonce != recovery_nonce
            || &request.purpose != purpose
        {
            return Err(invalid(
                "fresh witness does not bind this operation, recipient or purpose",
            ));
        }
        let topology = witness.committed_topology(read_request_id, recipient_challenge)?;
        let bootstrap_id = topology
            .range_image_bootstrap_id
            .filter(|id| !id.is_nil())
            .ok_or_else(|| invalid("legacy metadata has no admitted native image bootstrap"))?;
        let range = topology
            .range_by_id(range_id)
            .ok_or_else(|| invalid("witness range absent"))?
            .clone();
        let identity = |node: &crate::distribution::ClusterNode| RecoveryMemberIdentity {
            incarnation: node.incarnation,
            active_certificate_sha256: node.tls_certificate_sha256.clone(),
            pending_certificate_sha256: node.pending_tls_certificate_sha256.clone(),
        };
        let members = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| {
                let node = &topology.nodes[&replica.node_id];
                (node.id.clone(), identity(node))
            })
            .collect::<BTreeMap<_, _>>();
        if members.is_empty() || members.len() > 64 {
            return Err(invalid("current data voter set exceeds recovery bound"));
        }
        let local_member = identity(
            topology
                .nodes
                .get(local_node_id)
                .ok_or_else(|| invalid("witness local member absent"))?,
        );
        let schema = topology
            .nodes
            .get(&range.leader)
            .and_then(|node| node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL))
            .cloned();
        let relocation = match purpose {
            MetadataRecoveryRolePurpose::NormalRecovery => None,
            MetadataRecoveryRolePurpose::CommittedHandoff { relocation_id, .. } => Some(
                topology
                    .relocations
                    .get(relocation_id)
                    .ok_or_else(|| invalid("committed handoff absent"))?
                    .clone(),
            ),
        };
        let restore = witness
            .restore_activation(read_request_id, recipient_challenge)?
            .cloned();
        let cluster_id = topology.cluster_id.clone();
        witness.require_live_request(read_request_id, recipient_challenge)?;
        Ok(Self {
            witness,
            cluster_id,
            bootstrap_id,
            range,
            members,
            local_member,
            restore,
            schema,
            relocation,
        })
    }
    pub(crate) fn require_live_request(&self) -> Result<()> {
        self.witness
            .require_live_request(self.business_request_id(), self.recipient_challenge())
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.witness.request().recipient_node_id
    }
    pub(crate) fn local_member_incarnation(&self) -> u64 {
        self.local_member.incarnation
    }
    pub(crate) fn local_member_identity(&self) -> &RecoveryMemberIdentity {
        &self.local_member
    }
    pub(crate) fn initiator_node_id(&self) -> &ClusterNodeId {
        &self.witness.request().initiator_node_id
    }
    pub(crate) fn initiator_incarnation(&self) -> u64 {
        self.witness.request().initiator_incarnation
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        &self.members
    }
    pub(crate) fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        self.restore.as_ref()
    }
    pub(crate) fn expected_schema_sha256(&self) -> Option<&str> {
        self.schema.as_deref()
    }
    pub(crate) fn committed_relocation(&self) -> Option<&RangeRelocation> {
        self.relocation.as_ref()
    }
    pub(crate) fn purpose(&self) -> &MetadataRecoveryRolePurpose {
        &self.witness.request().purpose
    }
    pub(crate) fn context_id(&self) -> Uuid {
        self.witness.context_id()
    }
    pub(crate) fn metadata_term(&self) -> u64 {
        self.witness.term()
    }
    pub(crate) fn metadata_read_index(&self) -> u64 {
        self.witness.read_index()
    }
    pub(crate) fn recovery_nonce(&self) -> Uuid {
        self.witness.request().recovery_nonce
    }
    pub(crate) fn business_request_id(&self) -> Uuid {
        self.witness.request().read_request_id
    }
    pub(crate) fn recipient_challenge(&self) -> Uuid {
        self.witness.request().recipient_challenge
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.witness.deadline()
    }
    pub(crate) fn remaining_budget(&self) -> Option<Duration> {
        self.witness.remaining_budget()
    }
    pub(crate) fn into_handoff(self) -> Result<HandoffRoleAdmission> {
        self.require_live_request()?;
        if !matches!(
            self.purpose(),
            MetadataRecoveryRolePurpose::CommittedHandoff { .. }
        ) || self.relocation.is_none()
        {
            return Err(invalid(
                "normal recovery cannot authorize native handoff installation",
            ));
        }
        Ok(HandoffRoleAdmission(self))
    }
    pub(crate) fn require_historical_command_scope(
        &self,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        self.require_live_request()?;
        command.validate()?;
        if command.cluster_id != self.cluster_id
            || command.range_id != self.range.id
            || command.range_epoch > self.range.epoch
        {
            return Err(invalid(
                "historical command is outside the freshly authorized prefix scope",
            ));
        }
        // This is purpose/role admission only. A selected historical command
        // still needs actual distinct current-voter certification before Apply.
        if let Some(plan) = &self.relocation {
            if command.range_id != plan.range_id || command.range_epoch > plan.learner_epoch {
                return Err(invalid(
                    "historical command is outside this committed handoff's old prefix",
                ));
            }
        }
        Ok(())
    }
}

/// Exact committed-plan permission. It cannot be constructed from a normal
/// recovery witness and is not a capture/install/native receipt by itself.
#[derive(Debug)]
pub(crate) struct HandoffRoleAdmission(RecoveryRoleAdmission);
impl HandoffRoleAdmission {
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.0
    }
    pub(crate) fn require_live_scope(
        &self,
        relocation_id: crate::distribution::RelocationId,
        plan_id: &str,
        session_id: Uuid,
        nonce: Uuid,
    ) -> Result<()> {
        self.0.require_live_request()?;
        match self.0.purpose() {
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id: rid,
                plan_id: pid,
                session_id: sid,
                nonce: n,
            } if *rid == relocation_id && pid == plan_id && *sid == session_id && *n == nonce => {
                Ok(())
            }
            _ => Err(invalid(
                "handoff witness is not for this exact plan/session",
            )),
        }
    }
    pub(crate) fn committed_relocation(&self) -> &RangeRelocation {
        self.0
            .relocation
            .as_ref()
            .expect("handoff constructor requires committed plan")
    }
}
