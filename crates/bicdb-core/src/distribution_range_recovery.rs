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
    handoff_target_identity: Option<RecoveryMemberIdentity>,
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
        let handoff_target_identity = relocation
            .as_ref()
            .map(|plan| {
                topology
                    .nodes
                    .get(&plan.target)
                    .map(identity)
                    .ok_or_else(|| invalid("committed target member absent"))
            })
            .transpose()?;
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
            handoff_target_identity,
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
    pub(crate) fn target_member_identity(&self) -> &RecoveryMemberIdentity {
        self.0
            .handoff_target_identity
            .as_ref()
            .expect("committed handoff constructor captures its target")
    }
    pub(crate) fn committed_relocation(&self) -> &RangeRelocation {
        self.0
            .relocation
            .as_ref()
            .expect("handoff constructor requires committed plan")
    }
}

/// Genuine fresh consensus/provider test fixture, with registered test-peer
/// identities and real durable metadata stores. It simulates member transport;
/// it is explicitly NOT mTLS, hardware or deployment qualification evidence.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn test_handoff_role_admission(
    root: &std::path::Path,
    topology: &crate::ClusterTopology,
    metadata_leader: &ClusterNodeId,
    initiator: &ClusterNodeId,
    recipient: &ClusterNodeId,
    relocation_id: crate::RelocationId,
    budget: Duration,
) -> Result<HandoffRoleAdmission> {
    use crate::distribution_consensus::{
        MetadataConsensusStore, MetadataReadAck, MetadataReadProbeRequest,
    };
    use crate::distribution_metadata_read_provider::{
        MetadataReadProbeTransport, MetadataReadProvider,
    };
    use crate::CancellationToken;
    use parking_lot::Mutex;
    use std::sync::Arc;
    let parent = CancellationToken::uncancelable().child_with_deadline(Instant::now() + budget);
    let participant = |id: &ClusterNodeId| {
        let member = &topology.nodes[id];
        MetadataReadParticipant::from_transport(
            id.clone(),
            member.incarnation,
            member.tls_certificate_sha256.clone(),
            true,
        )
    };
    let peers = topology
        .nodes
        .keys()
        .map(|id| {
            Ok((
                id.clone(),
                Arc::new(Mutex::new(MetadataConsensusStore::open(
                    root.join(id.as_str()),
                    topology.cluster_id.clone(),
                    id.clone(),
                    topology.clone(),
                    true,
                )?)),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let leader = peers
        .get(metadata_leader)
        .ok_or_else(|| invalid("test metadata leader absent"))?
        .clone();
    {
        let mut state = leader.lock();
        state.start_election()?;
        state.become_leader(&topology.metadata_voters())?;
        for id in topology
            .metadata_voters()
            .iter()
            .filter(|id| *id != metadata_leader)
        {
            let reply = peers[id]
                .lock()
                .handle_append_request(state.append_request_from(1, 64)?)?;
            state.record_append_response(id, &reply)?;
        }
    }
    #[derive(Debug)]
    struct TestTransport {
        topology: crate::ClusterTopology,
        leader: ClusterNodeId,
        peers: BTreeMap<ClusterNodeId, Arc<Mutex<MetadataConsensusStore>>>,
    }
    impl MetadataReadProbeTransport for TestTransport {
        fn local_participant(&self) -> MetadataReadParticipant {
            let member = &self.topology.nodes[&self.leader];
            MetadataReadParticipant::from_transport(
                member.id.clone(),
                member.incarnation,
                member.tls_certificate_sha256.clone(),
                true,
            )
        }
        fn probe(
            &self,
            destination: &ClusterNodeId,
            request: MetadataReadProbeRequest,
            cancellation: &CancellationToken,
        ) -> Result<MetadataReadAck> {
            cancellation.check()?;
            let reply = self.peers[destination]
                .lock()
                .handle_read_authority_probe(request.clone())?;
            let member = &self.topology.nodes[destination];
            MetadataReadAck::from_transport(
                &request,
                destination,
                MetadataReadParticipant::from_transport(
                    member.id.clone(),
                    member.incarnation,
                    member.tls_certificate_sha256.clone(),
                    true,
                ),
                reply,
            )
        }
    }
    let transport = Arc::new(TestTransport {
        topology: topology.clone(),
        leader: metadata_leader.clone(),
        peers,
    });
    let provider = MetadataReadProvider::new(leader, transport)?;
    let plan = topology
        .relocations
        .get(&relocation_id)
        .ok_or_else(|| invalid("test committed plan absent"))?;
    let session = plan
        .handoff_session
        .as_ref()
        .ok_or_else(|| invalid("test committed session absent"))?;
    let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
        relocation_id,
        plan_id: plan.plan_id.clone(),
        session_id: session.session_id,
        nonce: session.nonce,
    };
    let request_id = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let remaining = parent
        .deadline()
        .unwrap()
        .checked_duration_since(Instant::now())
        .ok_or_else(|| invalid("test witness deadline expired"))?;
    let request = crate::distribution_consensus::MetadataRecoveryRoleRequest {
        protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
        acquisition_id: Uuid::now_v7(),
        read_request_id: request_id,
        initiator_node_id: initiator.clone(),
        initiator_incarnation: topology.nodes[initiator].incarnation,
        initiator_certificate_sha256: topology.nodes[initiator].tls_certificate_sha256.clone(),
        initiator_plaintext_loopback: true,
        recipient_node_id: recipient.clone(),
        recipient_incarnation: topology.nodes[recipient].incarnation,
        range_id: plan.range_id,
        range_epoch: plan.learner_epoch,
        recovery_nonce: session.nonce,
        recipient_challenge: challenge,
        purpose: purpose.clone(),
        budget_ms: remaining
            .as_millis()
            .min(5000)
            .try_into()
            .map_err(|_| invalid("test witness budget overflow"))?,
    };
    let started = Instant::now();
    let response =
        provider.acquire_recovery_role(&participant(recipient), request.clone(), &parent)?;
    // The transport wrapper is test simulated; the authority consumed above
    // came exclusively from actual begin/probe/record/finish on real stores.
    let observation = MetadataRecoveryRoleObservation::from_transport(
        &request,
        &topology.cluster_id,
        metadata_leader,
        participant(metadata_leader),
        response,
        parent.deadline().unwrap(),
        started,
    )?;
    while provider.inflight_contexts() != 0 {
        parent.check()?;
        std::thread::sleep(Duration::from_millis(1));
    }
    RecoveryRoleAdmission::from_witness(
        observation,
        &participant(initiator),
        recipient,
        request_id,
        plan.range_id,
        plan.learner_epoch,
        session.nonce,
        challenge,
        &purpose,
    )?
    .into_handoff()
}
