//! Durable quorum state for cluster topology metadata.
//!
//! This is deliberately separate from BicDB's data-WAL consensus state. Range
//! ownership must be agreed before it is published, while row replication is
//! driven by the resulting range catalog.

#[cfg(test)]
mod append_frontier_tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{
    ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, MetadataMemberRole, RangeId,
};
use crate::error::{BicDbError, Result};

pub const CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION: u32 = 1;
pub const METADATA_RESTORE_ACTIVATION_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE: &str = "cluster-metadata-consensus.json";
const MAX_RESTORE_ACTIVATION_ACKNOWLEDGEMENTS: u64 = 100_000;
pub const METADATA_READ_AUTHORITY_PROTOCOL_VERSION: u32 = 1;
pub const METADATA_READ_ACQUISITION_PROTOCOL_VERSION: u32 = 1;
mod host_read;
mod native_metadata_store;
pub(crate) use host_read::{
    HostMetadataReadAcquireRequest, HostMetadataReadAcquireResponse, HostMetadataReadObservation,
};
pub(crate) use native_metadata_store::NativeMetadataAppendProvenance;
pub use native_metadata_store::NativeMetadataAppendReceiver;
#[cfg(test)]
pub(crate) use native_metadata_store::NativeMetadataInitializationCut;
pub(crate) use native_metadata_store::NativeMetadataPreVoteFact;

/// Wire request only. This is not a read authority or a row-read permit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReadAcquireRequest {
    pub protocol_version: u32,
    pub acquisition_id: Uuid,
    pub read_request_id: Uuid,
    pub requester_node_id: ClusterNodeId,
    pub requester_incarnation: u64,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub budget_ms: u64,
}

impl MetadataReadAcquireRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != METADATA_READ_ACQUISITION_PROTOCOL_VERSION
            || self.acquisition_id.is_nil()
            || self.read_request_id.is_nil()
            || self.requester_incarnation == 0
            || self.range_epoch == 0
            || self.budget_ms == 0
            || self.budget_ms > 5_000
        {
            return Err(metadata_error("invalid metadata read acquisition request"));
        }
        Ok(())
    }
}

/// A current-term committed anchor observed at begin, before any fresh probes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReadAnchor {
    pub index: u64,
    pub term: u64,
    pub topology_generation: u64,
    pub topology_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReadVoterProof {
    pub node_id: ClusterNodeId,
    pub incarnation: u64,
    pub active_certificate_sha256: Option<String>,
    pub pending_certificate_sha256: Option<String>,
}

/// Transfer DTO, deliberately separate from the non-deserializable authority.
/// The source leader attests its fresh transport-correlated joint quorum;
/// these fields are not transferable voter signatures.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReadAcquireResponse {
    pub protocol_version: u32,
    pub request: MetadataReadAcquireRequest,
    pub context_id: Uuid,
    pub metadata_leader_id: ClusterNodeId,
    pub metadata_leader_incarnation: u64,
    pub term: u64,
    pub anchor: MetadataReadAnchor,
    pub read_index: u64,
    pub applied_index: u64,
    pub context_budget_ms: u64,
    pub committed_topology: ClusterTopology,
    pub joint_configuration_candidate: Option<ClusterTopology>,
    pub old_voters: Vec<MetadataReadVoterProof>,
    pub new_voters: Vec<MetadataReadVoterProof>,
    pub fresh_old_voters: BTreeSet<ClusterNodeId>,
    pub fresh_new_voters: BTreeSet<ClusterNodeId>,
    pub restore_activation: Option<MetadataRestoreActivation>,
}

/// Narrow operation purpose. Neither variant grants SQL or native readiness.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) enum MetadataRecoveryRolePurpose {
    NormalRecovery,
    CommittedHandoff {
        relocation_id: crate::distribution::RelocationId,
        plan_id: String,
        session_id: Uuid,
        nonce: Uuid,
    },
    /// Read-only observation of an already persisted coordinator fence. The
    /// actual initiating peer is an old/current data voter, not the claimed
    /// coordinator, and this purpose cannot construct mutation admission.
    ObserveFencedRecovery {
        coordinator_node_id: ClusterNodeId,
        coordinator_incarnation: u64,
        committed_handoff: Option<MetadataRecoveryHandoffBinding>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataRecoveryHandoffBinding {
    pub relocation_id: crate::distribution::RelocationId,
    pub plan_id: String,
    pub session_id: Uuid,
    pub nonce: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataRecoveryRoleRequest {
    pub protocol_version: u32,
    pub acquisition_id: Uuid,
    pub read_request_id: Uuid,
    pub initiator_node_id: ClusterNodeId,
    pub initiator_incarnation: u64,
    pub initiator_certificate_sha256: Option<String>,
    pub initiator_plaintext_loopback: bool,
    pub recipient_node_id: ClusterNodeId,
    pub recipient_incarnation: u64,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub recovery_nonce: Uuid,
    pub recipient_challenge: Uuid,
    pub purpose: MetadataRecoveryRolePurpose,
    pub budget_ms: u64,
}

impl MetadataRecoveryRoleRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        self.subject_request().validate()?;
        if self.recipient_incarnation == 0
            || self.recovery_nonce.is_nil()
            || self.recipient_challenge.is_nil()
            || self
                .initiator_certificate_sha256
                .as_ref()
                .is_some_and(|pin| pin.len() != 64 || !pin.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(metadata_error("invalid metadata recovery-role request"));
        }
        if let MetadataRecoveryRolePurpose::CommittedHandoff {
            plan_id,
            session_id,
            nonce,
            ..
        } = &self.purpose
        {
            if plan_id.is_empty()
                || plan_id.len() > 1024
                || session_id.is_nil()
                || nonce.is_nil()
                || *nonce != self.recovery_nonce
            {
                return Err(metadata_error("invalid recovery-role handoff purpose"));
            }
        }
        if let MetadataRecoveryRolePurpose::ObserveFencedRecovery {
            coordinator_incarnation,
            committed_handoff,
            ..
        } = &self.purpose
        {
            if *coordinator_incarnation == 0
                || committed_handoff.as_ref().is_some_and(|binding| {
                    binding.plan_id.is_empty()
                        || binding.plan_id.len() > 1024
                        || binding.session_id.is_nil()
                        || binding.nonce.is_nil()
                        || binding.nonce != self.recovery_nonce
                })
            {
                return Err(metadata_error(
                    "invalid read-only recovery observation purpose",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn subject_request(&self) -> MetadataReadAcquireRequest {
        MetadataReadAcquireRequest {
            protocol_version: self.protocol_version,
            acquisition_id: self.acquisition_id,
            read_request_id: self.read_request_id,
            requester_node_id: self.initiator_node_id.clone(),
            requester_incarnation: self.initiator_incarnation,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            budget_ms: self.budget_ms,
        }
    }
    pub(crate) fn matches_initiator(&self, participant: &MetadataReadParticipant) -> bool {
        self.initiator_node_id == participant.node_id
            && self.initiator_incarnation == participant.member_incarnation
            && self.initiator_certificate_sha256 == participant.certificate_sha256
            && self.initiator_plaintext_loopback == participant.plaintext_loopback
    }
    fn validate_configuration(
        &self,
        topology: &ClusterTopology,
        metadata_leader: &ClusterNodeId,
    ) -> Result<crate::distribution::RangeDescriptor> {
        use crate::distribution::{RangeReplicaRole, RelocationPhase};
        self.validate()?;
        let invalid = || {
            metadata_error(
                "metadata recovery-role identity, purpose or committed configuration changed",
            )
        };
        let range = topology.range_by_id(self.range_id).ok_or_else(invalid)?;
        let initiator = topology
            .nodes
            .get(&self.initiator_node_id)
            .ok_or_else(invalid)?;
        let recipient = topology
            .nodes
            .get(&self.recipient_node_id)
            .ok_or_else(invalid)?;
        let pin_ok = match initiator.tls_certificate_sha256.as_deref() {
            Some(pin) => {
                self.initiator_certificate_sha256.as_deref() == Some(pin)
                    || initiator
                        .pending_tls_certificate_sha256
                        .as_deref()
                        .is_some_and(|pending| {
                            self.initiator_certificate_sha256.as_deref() == Some(pending)
                        })
            }
            None => {
                self.initiator_certificate_sha256.is_some() || self.initiator_plaintext_loopback
            }
        };
        if range.epoch != self.range_epoch
            || initiator.incarnation != self.initiator_incarnation
            || recipient.incarnation != self.recipient_incarnation
            || initiator.lifecycle == ClusterNodeLifecycle::Decommissioned
            || recipient.lifecycle == ClusterNodeLifecycle::Decommissioned
            || !pin_ok
        {
            return Err(invalid());
        }
        let recipient_voter = range
            .replicas
            .iter()
            .any(|r| r.node_id == recipient.id && r.role == RangeReplicaRole::Voter);
        match &self.purpose {
            MetadataRecoveryRolePurpose::NormalRecovery => {
                if range.leader != initiator.id || !recipient_voter {
                    return Err(invalid());
                }
            }
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id,
                plan_id,
                session_id,
                nonce,
            } => {
                let plan = topology
                    .relocations
                    .get(relocation_id)
                    .ok_or_else(invalid)?;
                let session = plan.handoff_session.as_ref().ok_or_else(invalid)?;
                let exact_target = range.replicas.iter().any(|r| {
                    r.id == plan.target_replica_id
                        && r.node_id == recipient.id
                        && r.node_id == plan.target
                        && r.role == RangeReplicaRole::Learner
                });
                if (range.leader != initiator.id && metadata_leader != &initiator.id)
                    || plan.plan_id != *plan_id
                    || plan.range_id != range.id
                    || plan.learner_epoch != range.epoch
                    || !matches!(
                        plan.phase,
                        RelocationPhase::LearnerAllocated
                            | RelocationPhase::SnapshotCopying
                            | RelocationPhase::CatchingUp
                            | RelocationPhase::ReadyToPromote
                    )
                    || session.session_id != *session_id
                    || session.nonce != *nonce
                    || (!recipient_voter && !exact_target)
                {
                    return Err(invalid());
                }
            }
            MetadataRecoveryRolePurpose::ObserveFencedRecovery {
                coordinator_node_id,
                coordinator_incarnation,
                committed_handoff,
            } => {
                let observer_voter = range
                    .replicas
                    .iter()
                    .any(|r| r.node_id == initiator.id && r.role == RangeReplicaRole::Voter);
                let coordinator = topology
                    .nodes
                    .get(coordinator_node_id)
                    .ok_or_else(invalid)?;
                if !observer_voter
                    || !recipient_voter
                    || coordinator.incarnation != *coordinator_incarnation
                    || coordinator.lifecycle == ClusterNodeLifecycle::Decommissioned
                {
                    return Err(invalid());
                }
                match committed_handoff {
                    None if range.leader == coordinator.id => {}
                    Some(binding) => {
                        let plan = topology
                            .relocations
                            .get(&binding.relocation_id)
                            .ok_or_else(invalid)?;
                        let session = plan.handoff_session.as_ref().ok_or_else(invalid)?;
                        if (range.leader != coordinator.id && metadata_leader != &coordinator.id)
                            || plan.plan_id != binding.plan_id
                            || plan.range_id != range.id
                            || plan.learner_epoch != range.epoch
                            || !matches!(
                                plan.phase,
                                RelocationPhase::LearnerAllocated
                                    | RelocationPhase::SnapshotCopying
                                    | RelocationPhase::CatchingUp
                                    | RelocationPhase::ReadyToPromote
                            )
                            || session.session_id != binding.session_id
                            || session.nonce != binding.nonce
                        {
                            return Err(invalid());
                        }
                    }
                    None => return Err(invalid()),
                }
            }
        }
        Ok(range.clone())
    }
}

/// Transfer evidence only. No decoded field constructs local Authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataRecoveryRoleResponse {
    pub request: MetadataRecoveryRoleRequest,
    pub evidence: MetadataReadAcquireResponse,
}
impl MetadataRecoveryRoleResponse {
    fn validate(&self, request: &MetadataRecoveryRoleRequest) -> Result<()> {
        if &self.request != request {
            return Err(metadata_error("recovery-role response request mismatch"));
        }
        self.evidence
            .validate_quorum_evidence(&request.subject_request())?;
        request.validate_configuration(
            &self.evidence.committed_topology,
            &self.evidence.metadata_leader_id,
        )?;
        Ok(())
    }
}

/// One independently fresh metadata witness, never a local Authority, lease,
/// native admission, row-read barrier or transferable data-quorum certificate.
#[derive(Debug)]
pub(crate) struct MetadataRecoveryRoleObservation {
    response: MetadataRecoveryRoleResponse,
    deadline: Instant,
}
impl MetadataRecoveryRoleObservation {
    pub(crate) fn request(&self) -> &MetadataRecoveryRoleRequest {
        &self.response.request
    }
    pub(crate) fn context_id(&self) -> Uuid {
        self.response.evidence.context_id
    }
    pub(crate) fn term(&self) -> u64 {
        self.response.evidence.term
    }
    pub(crate) fn read_index(&self) -> u64 {
        self.response.evidence.read_index
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn remaining_budget(&self) -> Option<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
    }
    pub(crate) fn require_live_request(&self, read_request: Uuid, challenge: Uuid) -> Result<()> {
        if self.request().read_request_id != read_request
            || self.request().recipient_challenge != challenge
            || self.remaining_budget().is_none()
        {
            return Err(metadata_error(
                "recovery-role witness scope changed or expired",
            ));
        }
        Ok(())
    }
    pub(crate) fn committed_topology(
        &self,
        read_request: Uuid,
        challenge: Uuid,
    ) -> Result<&ClusterTopology> {
        self.require_live_request(read_request, challenge)?;
        Ok(&self.response.evidence.committed_topology)
    }
    pub(crate) fn restore_activation(
        &self,
        read_request: Uuid,
        challenge: Uuid,
    ) -> Result<Option<&MetadataRestoreActivation>> {
        self.require_live_request(read_request, challenge)?;
        Ok(self.response.evidence.restore_activation.as_ref())
    }
    pub(crate) fn require_initiator(&self, actual: &MetadataReadParticipant) -> Result<()> {
        self.require_live_request(
            self.request().read_request_id,
            self.request().recipient_challenge,
        )?;
        if !self.request().matches_initiator(actual) {
            return Err(metadata_error(
                "recovery-role witness does not bind the actual initiating TLS peer",
            ));
        }
        Ok(())
    }
    pub(crate) fn from_transport(
        request: &MetadataRecoveryRoleRequest,
        cluster: &ClusterId,
        destination: &ClusterNodeId,
        participant: MetadataReadParticipant,
        response: MetadataRecoveryRoleResponse,
        deadline: Instant,
        started: Instant,
    ) -> Result<Self> {
        response.validate(request)?;
        let evidence = &response.evidence;
        let deadline = deadline.min(started + Duration::from_millis(evidence.context_budget_ms));
        if Instant::now() >= deadline
            || evidence.committed_topology.cluster_id != *cluster
            || evidence.metadata_leader_id != *destination
            || participant.node_id != *destination
            || participant.member_incarnation != evidence.metadata_leader_incarnation
        {
            return Err(metadata_error(
                "recovery-role metadata source or deadline mismatch",
            ));
        }
        let node = evidence
            .committed_topology
            .nodes
            .get(destination)
            .ok_or_else(|| metadata_error("recovery-role metadata source missing"))?;
        let identity = MetadataReadVoterIdentity {
            incarnation: node.incarnation,
            active_certificate: node.tls_certificate_sha256.clone(),
            pending_certificate: node.pending_tls_certificate_sha256.clone(),
        };
        if !identity.accepts(&participant) {
            return Err(metadata_error("recovery-role metadata source pin mismatch"));
        }
        Ok(Self { response, deadline })
    }
    pub(crate) fn from_local_authority(
        authority: MetadataReadAuthority,
        request: MetadataRecoveryRoleRequest,
        recipient: &MetadataReadParticipant,
    ) -> Result<Self> {
        let deadline = authority.deadline();
        let response = authority.into_recovery_role_response(request.clone())?;
        if recipient.node_id != request.recipient_node_id
            || recipient.member_incarnation != request.recipient_incarnation
        {
            return Err(metadata_error("local recovery-role recipient mismatch"));
        }
        response.validate(&request)?;
        let node = &response.evidence.committed_topology.nodes[&request.recipient_node_id];
        let identity = MetadataReadVoterIdentity {
            incarnation: node.incarnation,
            active_certificate: node.tls_certificate_sha256.clone(),
            pending_certificate: node.pending_tls_certificate_sha256.clone(),
        };
        if response.evidence.metadata_leader_id != recipient.node_id || !identity.accepts(recipient)
        {
            return Err(metadata_error(
                "local recovery-role is not its actual metadata leader participant",
            ));
        }
        if Instant::now() >= deadline {
            return Err(metadata_error("local recovery-role witness expired"));
        }
        Ok(Self { response, deadline })
    }
}

/// A request-bound metadata observation minted only after transport checks.
/// It cannot be deserialized or converted into MetadataReadAuthority. It is
/// neither a range-image permit nor an implementation of strong row reads.
#[derive(Debug)]
pub struct MetadataReadObservation {
    response: MetadataReadAcquireResponse,
    deadline: Instant,
}

impl MetadataReadObservation {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn remaining_budget(&self) -> Option<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
    }
    pub fn context_id(&self) -> Uuid {
        self.response.context_id
    }
    pub fn anchor(&self) -> &MetadataReadAnchor {
        &self.response.anchor
    }
    pub fn metadata_leader_incarnation(&self) -> u64 {
        self.response.metadata_leader_incarnation
    }
    pub fn request(&self) -> &MetadataReadAcquireRequest {
        &self.response.request
    }
    pub fn metadata_leader_id(&self) -> &ClusterNodeId {
        &self.response.metadata_leader_id
    }
    pub fn term(&self) -> u64 {
        self.response.term
    }
    pub fn read_index(&self) -> u64 {
        self.response.read_index
    }
    pub fn applied_index(&self) -> u64 {
        self.response.applied_index
    }
    pub fn require_live_request(&self, request_id: Uuid) -> Result<()> {
        if request_id != self.response.request.read_request_id || Instant::now() >= self.deadline {
            return Err(metadata_error(
                "metadata observation request changed or expired",
            ));
        }
        Ok(())
    }
    pub fn committed_topology(&self, request_id: Uuid) -> Result<&ClusterTopology> {
        self.require_live_request(request_id)?;
        Ok(&self.response.committed_topology)
    }
    pub fn restore_activation(
        &self,
        request_id: Uuid,
    ) -> Result<Option<&MetadataRestoreActivation>> {
        self.require_live_request(request_id)?;
        Ok(self.response.restore_activation.as_ref())
    }
    pub(crate) fn from_transport(
        request: &MetadataReadAcquireRequest,
        cluster_id: &ClusterId,
        destination: &ClusterNodeId,
        participant: MetadataReadParticipant,
        response: MetadataReadAcquireResponse,
        deadline: Instant,
        request_started: Instant,
    ) -> Result<Self> {
        request.validate()?;
        response.validate(request)?;
        // Anchor the remote round's ORIGINAL duration before our request,
        // never at response receipt. Transit cannot renew the server budget.
        let deadline =
            deadline.min(request_started + Duration::from_millis(response.context_budget_ms));
        if Instant::now() >= deadline
            || response.committed_topology.cluster_id != *cluster_id
            || response.metadata_leader_id != *destination
            || participant.node_id != *destination
            || participant.member_incarnation != response.metadata_leader_incarnation
        {
            return Err(metadata_error(
                "metadata acquisition source/incarnation/deadline mismatch",
            ));
        }
        let node = response
            .committed_topology
            .nodes
            .get(destination)
            .ok_or_else(|| metadata_error("metadata observation leader is absent"))?;
        let identity = MetadataReadVoterIdentity {
            incarnation: node.incarnation,
            active_certificate: node.tls_certificate_sha256.clone(),
            pending_certificate: node.pending_tls_certificate_sha256.clone(),
        };
        if !identity.accepts(&participant) {
            return Err(metadata_error(
                "metadata observation transport pin is not its leader",
            ));
        }
        Ok(Self { response, deadline })
    }
}

impl MetadataReadAcquireResponse {
    fn validate(&self, request: &MetadataReadAcquireRequest) -> Result<()> {
        self.validate_quorum_evidence(request)?;
        validate_acquire_requester(request, &self.committed_topology)?;
        if self.committed_topology.nodes[&self.metadata_leader_id].lifecycle
            != ClusterNodeLifecycle::Active
        {
            return Err(metadata_error("metadata acquisition leader is not active"));
        }
        Ok(())
    }

    fn validate_quorum_evidence(&self, request: &MetadataReadAcquireRequest) -> Result<()> {
        self.committed_topology.validate()?;
        if self.protocol_version != METADATA_READ_ACQUISITION_PROTOCOL_VERSION
            || &self.request != request
            || self.context_id.is_nil()
            || self.term == 0
            || self.read_index == 0
            || self.anchor.index != self.read_index
            || self.anchor.term != self.term
            || self.anchor.topology_generation == 0
            || self.anchor.topology_generation > self.committed_topology.generation
            || self.anchor.topology_sha256.len() != 64
            || !self
                .anchor
                .topology_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.applied_index < self.read_index
            || self.context_budget_ms == 0
            || self.context_budget_ms > request.budget_ms
        {
            return Err(metadata_error(
                "metadata acquisition proof/correlation is invalid",
            ));
        }
        if self.anchor.topology_generation == self.committed_topology.generation
            && self.anchor.topology_sha256 != topology_checksum(&self.committed_topology)?
        {
            return Err(metadata_error(
                "metadata acquisition current anchor digest differs",
            ));
        }
        let leader = self
            .committed_topology
            .nodes
            .get(&self.metadata_leader_id)
            .ok_or_else(|| metadata_error("metadata acquisition leader missing"))?;
        if leader.incarnation != self.metadata_leader_incarnation
            || leader.lifecycle == ClusterNodeLifecycle::Decommissioned
            || leader.metadata_role != MetadataMemberRole::Voter
        {
            return Err(metadata_error(
                "metadata acquisition leader identity is not active",
            ));
        }
        fn validate_side(
            topology: &ClusterTopology,
            voters: &[MetadataReadVoterProof],
            fresh: &BTreeSet<ClusterNodeId>,
        ) -> Result<()> {
            let expected = topology_voters(topology);
            let identities = voters
                .iter()
                .map(|v| v.node_id.clone())
                .collect::<BTreeSet<_>>();
            if expected.is_empty()
                || expected.len() > 64
                || identities != expected
                || identities.len() != voters.len()
                || !fresh.is_subset(&identities)
                || fresh.len() < identities.len() / 2 + 1
            {
                return Err(metadata_error(
                    "metadata acquisition lacks a fresh joint quorum",
                ));
            }
            for proof in voters {
                let node = &topology.nodes[&proof.node_id];
                if node.incarnation != proof.incarnation
                    || node.tls_certificate_sha256 != proof.active_certificate_sha256
                    || node.pending_tls_certificate_sha256 != proof.pending_certificate_sha256
                {
                    return Err(metadata_error(
                        "metadata acquisition voter identity changed",
                    ));
                }
            }
            Ok(())
        }
        validate_side(
            &self.committed_topology,
            &self.old_voters,
            &self.fresh_old_voters,
        )?;
        let new = self
            .joint_configuration_candidate
            .as_ref()
            .unwrap_or(&self.committed_topology);
        new.validate()?;
        if new.cluster_id != self.committed_topology.cluster_id {
            return Err(metadata_error("metadata acquisition joint cluster changed"));
        }
        crate::distribution::ensure_range_image_domain(
            self.committed_topology.range_image_bootstrap_id,
            new.range_image_bootstrap_id,
        )?;
        crate::distribution::validate_native_store_birth_transition(&self.committed_topology, new)?;
        validate_side(new, &self.new_voters, &self.fresh_new_voters)?;
        if let Some(activation) = &self.restore_activation {
            validate_historical_activation_against_topology(
                activation,
                &self.committed_topology,
                &self.committed_topology.cluster_id,
            )?;
        }
        Ok(())
    }
}

pub(crate) fn validate_acquire_requester(
    request: &MetadataReadAcquireRequest,
    topology: &ClusterTopology,
) -> Result<()> {
    request.validate()?;
    let node = topology
        .nodes
        .get(&request.requester_node_id)
        .ok_or_else(|| metadata_error("metadata acquisition requester is absent"))?;
    let range = topology
        .range_by_id(request.range_id)
        .ok_or_else(|| metadata_error("metadata acquisition range is absent"))?;
    if node.incarnation != request.requester_incarnation
        || node.lifecycle != ClusterNodeLifecycle::Active
        || node.metadata_role != MetadataMemberRole::Voter
        || range.epoch != request.range_epoch
        || range.leader != request.requester_node_id
    {
        return Err(metadata_error(
            "metadata acquisition requester/range authority changed",
        ));
    }
    Ok(())
}

fn metadata_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("metadata consensus: {}", message.into()))
}

struct MetadataReadSizeLimit(usize);

impl std::io::Write for MetadataReadSizeLimit {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata read byte bound",
            ));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn topology_checksum(topology: &ClusterTopology) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(topology)?)))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetadataConsensusRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataLogEntry {
    pub index: u64,
    pub term: u64,
    #[serde(default)]
    pub leadership_barrier: bool,
    pub topology: ClusterTopology,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_activation: Option<MetadataRestoreActivation>,
}

/// Compact restore-activation decision replicated through metadata consensus.
/// The full node acknowledgements remain in the activation run; consensus
/// commits their canonical set hash so one node cannot activate a different
/// admission report or silently omit a restored artifact node.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataRestoreActivation {
    pub format_version: u32,
    pub activation_id: Uuid,
    pub cluster_id: ClusterId,
    pub certificate_id: Uuid,
    pub certificate_checksum_sha256: String,
    pub admission_id: Uuid,
    pub admission_checksum_sha256: String,
    pub plan_id: Uuid,
    pub topology_generation: u64,
    pub topology_sha256: String,
    pub acknowledged_nodes: u64,
    pub node_ack_set_sha256: String,
    pub activated_at_ms: u64,
}

impl MetadataRestoreActivation {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != METADATA_RESTORE_ACTIVATION_FORMAT_VERSION
            || self.activation_id.is_nil()
            || self.certificate_id.is_nil()
            || self.admission_id.is_nil()
            || self.plan_id.is_nil()
            || self.topology_generation == 0
            || self.acknowledged_nodes == 0
            || self.acknowledged_nodes > MAX_RESTORE_ACTIVATION_ACKNOWLEDGEMENTS
            || self.activated_at_ms == 0
        {
            return Err(metadata_error(
                "restore activation identity or bounds are invalid",
            ));
        }
        for (kind, digest) in [
            ("certificate", &self.certificate_checksum_sha256),
            ("admission", &self.admission_checksum_sha256),
            ("topology", &self.topology_sha256),
            ("node acknowledgement set", &self.node_ack_set_sha256),
        ] {
            if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(metadata_error(format!(
                    "restore activation {kind} SHA-256 is invalid"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataVoteRequest {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub candidate_id: ClusterNodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataVoteResponse {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub voter_id: ClusterNodeId,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub topology: ClusterTopology,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_activation: Option<MetadataRestoreActivation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataAppendRequest {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub leader_id: ClusterNodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<MetadataLogEntry>,
    pub leader_commit: u64,
    pub snapshot: Option<MetadataSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataAppendResponse {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub node_id: ClusterNodeId,
    pub success: bool,
    pub match_index: u64,
    pub conflict_index: u64,
    /// Actual persisted receiver identity data; decoding it does not attest an ACK.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_receiver: Option<NativeMetadataAppendReceiver>,
    #[serde(skip)]
    pub(crate) native_provenance: Option<NativeMetadataAppendProvenance>,
}

/// A new, request-correlated authority round. This is not a cached heartbeat.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReadProbeRequest {
    pub protocol_version: u32,
    pub context_id: Uuid,
    pub read_request_id: Uuid,
    pub read_index: u64,
    pub append: MetadataAppendRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReadProbeResponse {
    pub protocol_version: u32,
    pub context_id: Uuid,
    pub read_request_id: Uuid,
    pub read_index: u64,
    pub append: MetadataAppendResponse,
    pub applied_index: u64,
}

/// Evidence supplied only by the authenticated cluster transport. Incarnation
/// is the transport's expected member incarnation, not a new cryptographic
/// attestation of a remote process. Existing unique-member/fencing assumptions
/// still apply.
#[derive(Debug)]
pub struct MetadataReadParticipant {
    node_id: ClusterNodeId,
    member_incarnation: u64,
    certificate_sha256: Option<String>,
    plaintext_loopback: bool,
}

impl MetadataReadParticipant {
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        &self.node_id
    }
    pub(crate) fn member_incarnation(&self) -> u64 {
        self.member_incarnation
    }
    pub(crate) fn certificate_sha256(&self) -> Option<&str> {
        self.certificate_sha256.as_deref()
    }
    pub(crate) fn plaintext_loopback(&self) -> bool {
        self.plaintext_loopback
    }
    pub(crate) fn from_transport(
        node_id: ClusterNodeId,
        member_incarnation: u64,
        certificate_sha256: Option<String>,
        plaintext_loopback: bool,
    ) -> Self {
        Self {
            node_id,
            member_incarnation,
            certificate_sha256,
            plaintext_loopback,
        }
    }
}

/// Not deserializable or publicly constructible: a bare response is not an
/// authenticated vote. The transport validates TLS/envelope identity first.
#[derive(Debug)]
pub struct MetadataReadAck {
    participant: MetadataReadParticipant,
    response: MetadataReadProbeResponse,
}

impl MetadataReadAck {
    pub fn voter_id(&self) -> &ClusterNodeId {
        &self.participant.node_id
    }
    pub fn conflict_index(&self) -> Option<u64> {
        (!self.response.append.success).then_some(self.response.append.conflict_index)
    }
    pub(crate) fn from_transport(
        request: &MetadataReadProbeRequest,
        destination: &ClusterNodeId,
        participant: MetadataReadParticipant,
        response: MetadataReadProbeResponse,
    ) -> Result<Self> {
        if request.protocol_version != METADATA_READ_AUTHORITY_PROTOCOL_VERSION
            || response.protocol_version != METADATA_READ_AUTHORITY_PROTOCOL_VERSION
            || response.context_id != request.context_id
            || response.read_request_id != request.read_request_id
            || response.read_index != request.read_index
            || participant.node_id != *destination
            || response.append.node_id != *destination
            || response.append.cluster_id != request.append.cluster_id
        {
            return Err(metadata_error(
                "read probe response correlation/identity mismatch",
            ));
        }
        Ok(Self {
            participant,
            response,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MetadataReadVoterIdentity {
    incarnation: u64,
    active_certificate: Option<String>,
    pending_certificate: Option<String>,
}

impl MetadataReadVoterIdentity {
    fn accepts(&self, participant: &MetadataReadParticipant) -> bool {
        if self.incarnation != participant.member_incarnation {
            return false;
        }
        match self.active_certificate.as_deref() {
            Some(active) => {
                participant.certificate_sha256.as_deref() == Some(active)
                    || self.pending_certificate.as_ref().is_some_and(|pending| {
                        participant.certificate_sha256.as_ref() == Some(pending)
                    })
            }
            None => participant.certificate_sha256.is_some() || participant.plaintext_loopback,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MetadataReadQuorums {
    old: BTreeMap<ClusterNodeId, MetadataReadVoterIdentity>,
    new: BTreeMap<ClusterNodeId, MetadataReadVoterIdentity>,
}

/// One-shot, monotonic-deadline context. No historical match indexes are used.
/// It is deliberately neither Clone nor Serialize and is consumed by finish.
#[derive(Debug)]
pub struct MetadataReadContext {
    context_id: Uuid,
    read_request_id: Uuid,
    leader_id: ClusterNodeId,
    term: u64,
    read_index: u64,
    deadline: Instant,
    started: Instant,
    anchor: MetadataReadAnchor,
    quorums: MetadataReadQuorums,
    fresh_old: BTreeSet<ClusterNodeId>,
    fresh_new: BTreeSet<ClusterNodeId>,
    failed: bool,
}

impl MetadataReadContext {
    pub(crate) fn restrict_deadline(&mut self, deadline: Instant) {
        self.deadline = self.deadline.min(deadline);
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn context_id(&self) -> Uuid {
        self.context_id
    }
    pub fn read_request_id(&self) -> Uuid {
        self.read_request_id
    }
    pub fn read_index(&self) -> u64 {
        self.read_index
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn remote_voters(&self) -> BTreeSet<ClusterNodeId> {
        self.quorums
            .old
            .keys()
            .chain(self.quorums.new.keys())
            .filter(|node| **node != self.leader_id)
            .cloned()
            .collect()
    }
}

/// A completed metadata-only read-index proof, not a range read permit or
/// reusable leadership lease. Strong row reads still need certified range
/// application/image evidence and final authority revalidation.
#[derive(Debug)]
pub struct MetadataReadAuthority {
    context_id: Uuid,
    read_request_id: Uuid,
    term: u64,
    read_index: u64,
    applied_index: u64,
    topology: ClusterTopology,
    restore_activation: Option<MetadataRestoreActivation>,
    leader_id: ClusterNodeId,
    leader_incarnation: u64,
    anchor: MetadataReadAnchor,
    joint_configuration_candidate: Option<ClusterTopology>,
    old_voters: Vec<MetadataReadVoterProof>,
    new_voters: Vec<MetadataReadVoterProof>,
    fresh_old_voters: BTreeSet<ClusterNodeId>,
    fresh_new_voters: BTreeSet<ClusterNodeId>,
    deadline: Instant,
    started: Instant,
}

impl MetadataReadAuthority {
    /// Original begin-round deadline, never a renewed completion-time lease.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn remaining_budget(&self) -> Option<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
    }
    pub fn is_current_budget(&self) -> bool {
        self.remaining_budget().is_some()
    }
    pub(crate) fn into_acquisition_response(
        self,
        request: MetadataReadAcquireRequest,
    ) -> Result<MetadataReadAcquireResponse> {
        let response = self.into_quorum_evidence(request)?;
        response.validate(&response.request)?;
        Ok(response)
    }
    pub(crate) fn into_recovery_role_response(
        self,
        request: MetadataRecoveryRoleRequest,
    ) -> Result<MetadataRecoveryRoleResponse> {
        let evidence = self.into_quorum_evidence(request.subject_request())?;
        let response = MetadataRecoveryRoleResponse { request, evidence };
        response.validate(&response.request)?;
        Ok(response)
    }
    fn into_quorum_evidence(
        self,
        request: MetadataReadAcquireRequest,
    ) -> Result<MetadataReadAcquireResponse> {
        if !self.is_current_budget() {
            return Err(metadata_error(
                "completed metadata authority budget expired",
            ));
        }
        let response = MetadataReadAcquireResponse {
            protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            request,
            context_id: self.context_id,
            metadata_leader_id: self.leader_id,
            metadata_leader_incarnation: self.leader_incarnation,
            term: self.term,
            anchor: self.anchor,
            read_index: self.read_index,
            applied_index: self.applied_index,
            context_budget_ms: self
                .deadline
                .saturating_duration_since(self.started)
                .as_millis()
                .try_into()
                .map_err(|_| metadata_error("metadata context duration overflow"))?,
            committed_topology: self.topology,
            joint_configuration_candidate: self.joint_configuration_candidate,
            old_voters: self.old_voters,
            new_voters: self.new_voters,
            fresh_old_voters: self.fresh_old_voters,
            fresh_new_voters: self.fresh_new_voters,
            restore_activation: self.restore_activation,
        };
        response.validate_quorum_evidence(&response.request)?;
        Ok(response)
    }
    pub fn context_id(&self) -> Uuid {
        self.context_id
    }
    pub fn read_request_id(&self) -> Uuid {
        self.read_request_id
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn read_index(&self) -> u64 {
        self.read_index
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    pub fn topology(&self) -> &ClusterTopology {
        &self.topology
    }
    pub fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        self.restore_activation.as_ref()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataConsensusStatus {
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub role: MetadataConsensusRole,
    pub current_term: u64,
    pub voted_for: Option<ClusterNodeId>,
    pub leader_id: Option<ClusterNodeId>,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub topology_generation: u64,
    pub voters: Vec<ClusterNodeId>,
    pub learners: Vec<ClusterNodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MetadataConsensusState {
    format_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_store_id: Option<Uuid>,
    cluster_id: ClusterId,
    node_id: ClusterNodeId,
    current_term: u64,
    voted_for: Option<ClusterNodeId>,
    role: MetadataConsensusRole,
    leader_id: Option<ClusterNodeId>,
    base_index: u64,
    base_term: u64,
    commit_index: u64,
    committed_topology: ClusterTopology,
    log: Vec<MetadataLogEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed_restore_activation: Option<MetadataRestoreActivation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MetadataConsensusEnvelope {
    format_version: u32,
    checksum_sha256: String,
    state: MetadataConsensusState,
}

#[derive(Debug)]
pub struct MetadataConsensusStore {
    root: PathBuf,
    fsync: bool,
    state: MetadataConsensusState,
    match_index: BTreeMap<ClusterNodeId, u64>,
    last_leader_contact_ms: u64,
    native_owner: Option<native_metadata_store::NativeMetadataStoreOwner>,
}

impl MetadataConsensusStore {
    pub(crate) fn validate_read_acquisition_caller(
        &self,
        request: &MetadataReadAcquireRequest,
        participant: &MetadataReadParticipant,
    ) -> Result<crate::RangeDescriptor> {
        validate_acquire_requester(request, &self.state.committed_topology)?;
        let node = &self.state.committed_topology.nodes[&request.requester_node_id];
        let identity = MetadataReadVoterIdentity {
            incarnation: node.incarnation,
            active_certificate: node.tls_certificate_sha256.clone(),
            pending_certificate: node.pending_tls_certificate_sha256.clone(),
        };
        if participant.node_id != request.requester_node_id || !identity.accepts(participant) {
            return Err(metadata_error(
                "metadata acquisition caller is not transport-authenticated",
            ));
        }
        Ok(self
            .state
            .committed_topology
            .range_by_id(request.range_id)
            .unwrap()
            .clone())
    }

    pub(crate) fn validate_recovery_role_caller(
        &self,
        request: &MetadataRecoveryRoleRequest,
        caller: &MetadataReadParticipant,
    ) -> Result<crate::RangeDescriptor> {
        let range =
            request.validate_configuration(&self.state.committed_topology, &self.state.node_id)?;
        let member = self
            .state
            .committed_topology
            .nodes
            .get(&request.recipient_node_id)
            .ok_or_else(|| metadata_error("recovery-role recipient absent"))?;
        let identity = MetadataReadVoterIdentity {
            incarnation: member.incarnation,
            active_certificate: member.tls_certificate_sha256.clone(),
            pending_certificate: member.pending_tls_certificate_sha256.clone(),
        };
        if caller.node_id != request.recipient_node_id
            || caller.member_incarnation != request.recipient_incarnation
            || !identity.accepts(caller)
        {
            return Err(metadata_error(
                "recovery-role recipient is not its authenticated member",
            ));
        }
        Ok(range)
    }

    pub(crate) fn read_authority_ready(&self, context: &MetadataReadContext) -> Result<bool> {
        self.validate_read_context(context)?;
        Ok(context.fresh_old.len() > context.quorums.old.len() / 2
            && context.fresh_new.len() > context.quorums.new.len() / 2)
    }

    /// Bound serialization before cloning topology-bearing metadata messages.
    pub(crate) fn read_authority_payload_fits(&self, max_bytes: usize) -> Result<()> {
        let mut writer = MetadataReadSizeLimit(max_bytes);
        serde_json::to_writer(&mut writer, &self.state.committed_topology).map_err(|_| {
            metadata_error("metadata read committed topology exceeds payload bound")
        })?;
        if let Some(pending) = self.state.log.last() {
            serde_json::to_writer(&mut writer, &pending.topology).map_err(|_| {
                metadata_error("metadata read joint topology exceeds payload bound")
            })?;
        }
        Ok(())
    }

    pub(crate) fn bounded_read_authority_probe(
        &self,
        context: &MetadataReadContext,
        destination: &ClusterNodeId,
        next_index: u64,
        max_bytes: usize,
    ) -> Result<MetadataReadProbeRequest> {
        let mut writer = MetadataReadSizeLimit(max_bytes);
        if next_index <= self.state.base_index {
            serde_json::to_writer(&mut writer, &self.state.committed_topology).map_err(|_| {
                metadata_error("metadata read catch-up snapshot exceeds payload bound")
            })?;
        }
        if let Some(entry) = self
            .state
            .log
            .iter()
            .find(|entry| entry.index >= next_index)
        {
            serde_json::to_writer(&mut writer, entry).map_err(|_| {
                metadata_error("metadata read catch-up entry exceeds payload bound")
            })?;
        }
        self.read_authority_probe(context, destination, next_index, 1)
    }
    /// Read and verify the durable metadata state without changing role or
    /// rewriting the file. This is safe for status tooling and health probes
    /// while a server owns the live consensus instance.
    pub fn inspect(root: impl AsRef<Path>) -> Result<MetadataConsensusStatus> {
        let path = root.as_ref().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        let state = decode_state(&fs::read(path)?)?;
        validate_state(&state)?;
        Ok(status_from_state(&state))
    }

    /// Read the latest quorum-committed restore activation without opening a
    /// live consensus participant or rewriting its durable state.
    pub fn inspect_restore_activation(
        root: impl AsRef<Path>,
    ) -> Result<Option<MetadataRestoreActivation>> {
        let path = root.as_ref().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        let state = decode_state(&fs::read(path)?)?;
        validate_state(&state)?;
        Ok(state.committed_restore_activation)
    }

    pub fn open(
        root: impl AsRef<Path>,
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        bootstrap_topology: ClusterTopology,
        fsync: bool,
    ) -> Result<Self> {
        bootstrap_topology.validate()?;
        if native_metadata_store::known_born_v2(&bootstrap_topology, &node_id) {
            return Err(metadata_error(
                "born-v2 metadata requires actual native owned initialization/reopen",
            ));
        }
        let expected_image_domain = bootstrap_topology.range_image_bootstrap_id;
        let expected_birth_topology = bootstrap_topology.clone();
        if expected_image_domain.is_some() && !fsync {
            return Err(metadata_error("native range image metadata requires fsync"));
        }
        if bootstrap_topology.cluster_id != cluster_id {
            return Err(metadata_error(
                "bootstrap topology belongs to another cluster",
            ));
        }
        if !bootstrap_topology.nodes.contains_key(&node_id) {
            return Err(metadata_error(format!(
                "local node {node_id} is absent from the bootstrap topology"
            )));
        }
        let root = root.as_ref().to_path_buf();
        let path = root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        let mut state = match fs::read(&path) {
            Ok(bytes) => decode_state(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => MetadataConsensusState {
                format_version: CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION,
                native_store_id: None,
                cluster_id: cluster_id.clone(),
                node_id: node_id.clone(),
                current_term: 0,
                voted_for: None,
                role: MetadataConsensusRole::Follower,
                leader_id: None,
                base_index: 0,
                base_term: 0,
                commit_index: 0,
                committed_topology: bootstrap_topology,
                log: Vec::new(),
                committed_restore_activation: None,
            },
            Err(error) => return Err(error.into()),
        };
        validate_state(&state)?;
        if state.native_store_id.is_some()
            || native_metadata_store::known_born_v2(&state.committed_topology, &node_id)
        {
            return Err(metadata_error(
                "protected metadata state requires actual native owned reopen",
            ));
        }
        crate::distribution::validate_native_store_birth_snapshot(
            &expected_birth_topology,
            &state.committed_topology,
        )?;
        crate::distribution::ensure_range_image_domain(
            expected_image_domain,
            state.committed_topology.range_image_bootstrap_id,
        )?;
        if state.cluster_id != cluster_id {
            return Err(metadata_error(format!(
                "cluster mismatch: state={} config={cluster_id}",
                state.cluster_id
            )));
        }
        if state.node_id != node_id {
            return Err(metadata_error(format!(
                "node mismatch: state={} config={node_id}",
                state.node_id
            )));
        }
        // A process restart never preserves volatile leadership authority.
        state.role = MetadataConsensusRole::Follower;
        state.leader_id = None;
        let mut store = Self {
            root,
            fsync,
            state,
            match_index: BTreeMap::new(),
            last_leader_contact_ms: unix_time_ms(),
            native_owner: None,
        };
        store.reset_match_indexes();
        store.persist()?;
        Ok(store)
    }

    pub fn status(&self) -> MetadataConsensusStatus {
        status_from_state(&self.state)
    }

    pub fn committed_topology(&self) -> &ClusterTopology {
        &self.state.committed_topology
    }

    pub fn committed_restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        self.state.committed_restore_activation.as_ref()
    }

    pub fn last_leader_contact_ms(&self) -> u64 {
        self.last_leader_contact_ms
    }

    fn read_quorums(&self) -> Result<MetadataReadQuorums> {
        fn identities(
            topology: &ClusterTopology,
        ) -> Result<BTreeMap<ClusterNodeId, MetadataReadVoterIdentity>> {
            topology_voters(topology)
                .into_iter()
                .map(|id| {
                    let node = topology
                        .nodes
                        .get(&id)
                        .ok_or_else(|| metadata_error("read quorum voter missing from topology"))?;
                    Ok((
                        id,
                        MetadataReadVoterIdentity {
                            incarnation: node.incarnation,
                            active_certificate: node.tls_certificate_sha256.clone(),
                            pending_certificate: node.pending_tls_certificate_sha256.clone(),
                        },
                    ))
                })
                .collect()
        }
        let old = identities(&self.state.committed_topology)?;
        let new = identities(
            self.state
                .log
                .last()
                .map(|entry| &entry.topology)
                .unwrap_or(&self.state.committed_topology),
        )?;
        if old.is_empty() || new.is_empty() {
            return Err(metadata_error(
                "read authority requires nonempty voter configurations",
            ));
        }
        Ok(MetadataReadQuorums { old, new })
    }

    fn validate_read_context(&self, context: &MetadataReadContext) -> Result<()> {
        self.require_native_metadata_actor_current(true)?;
        if context.failed || Instant::now() >= context.deadline {
            return Err(metadata_error("read authority context failed or expired"));
        }
        if self.state.role != MetadataConsensusRole::Leader
            || self.state.node_id != context.leader_id
            || self.state.current_term != context.term
            || self.state.commit_index < context.read_index
        {
            return Err(metadata_error(
                "read authority leader/term/applied floor changed",
            ));
        }
        if self.read_quorums()? != context.quorums {
            return Err(metadata_error(
                "read authority voter configuration changed; start a fresh context",
            ));
        }
        Ok(())
    }

    /// Begin one metadata-only linearizable read-index round. A historical
    /// quorum or an uncommitted current-term leadership barrier cannot qualify.
    pub fn begin_read_authority(
        &self,
        read_request_id: Uuid,
        budget: Duration,
        local: &MetadataReadParticipant,
    ) -> Result<MetadataReadContext> {
        self.require_native_metadata_actor_current(true)?;
        let started = Instant::now();
        if read_request_id.is_nil() || budget.is_zero() || budget > Duration::from_secs(300) {
            return Err(metadata_error("read authority request/budget is invalid"));
        }
        if self.state.role != MetadataConsensusRole::Leader
            || self.state.current_term == 0
            || self.state.commit_index == 0
            || self.term_at(self.state.commit_index) != Some(self.state.current_term)
        {
            return Err(metadata_error(
                "read authority requires a committed current-term leadership anchor",
            ));
        }
        if local.node_id != self.state.node_id {
            return Err(metadata_error(
                "read authority local transport identity differs from leader",
            ));
        }
        let quorums = self.read_quorums()?;
        let mut fresh_old = BTreeSet::new();
        let mut fresh_new = BTreeSet::new();
        if quorums
            .old
            .get(&local.node_id)
            .is_some_and(|identity| identity.accepts(local))
        {
            fresh_old.insert(local.node_id.clone());
        }
        if quorums
            .new
            .get(&local.node_id)
            .is_some_and(|identity| identity.accepts(local))
        {
            fresh_new.insert(local.node_id.clone());
        }
        if fresh_old.is_empty() && fresh_new.is_empty() {
            return Err(metadata_error(
                "read authority local incarnation/certificate is not a voter",
            ));
        }
        Ok(MetadataReadContext {
            context_id: Uuid::now_v7(),
            read_request_id,
            leader_id: self.state.node_id.clone(),
            term: self.state.current_term,
            read_index: self.state.commit_index,
            deadline: started + budget,
            started,
            anchor: MetadataReadAnchor {
                index: self.state.commit_index,
                term: self.state.current_term,
                topology_generation: self.state.committed_topology.generation,
                topology_sha256: topology_checksum(&self.state.committed_topology)?,
            },
            quorums,
            fresh_old,
            fresh_new,
            failed: false,
        })
    }

    pub fn read_authority_probe(
        &self,
        context: &MetadataReadContext,
        destination: &ClusterNodeId,
        next_index: u64,
        max_entries: usize,
    ) -> Result<MetadataReadProbeRequest> {
        self.validate_read_context(context)?;
        if destination == &self.state.node_id
            || (!context.quorums.old.contains_key(destination)
                && !context.quorums.new.contains_key(destination))
        {
            return Err(metadata_error(
                "read authority probe destination is not a remote voter",
            ));
        }
        Ok(MetadataReadProbeRequest {
            protocol_version: METADATA_READ_AUTHORITY_PROTOCOL_VERSION,
            context_id: context.context_id,
            read_request_id: context.read_request_id,
            read_index: context.read_index,
            append: self.append_request_from(next_index, max_entries)?,
        })
    }

    /// Receiver path retains the ordinary durable Append semantics and echoes
    /// this round's identity. No special no-fsync or weaker bootstrap path.
    pub fn handle_read_authority_probe(
        &mut self,
        request: MetadataReadProbeRequest,
    ) -> Result<MetadataReadProbeResponse> {
        if request.protocol_version != METADATA_READ_AUTHORITY_PROTOCOL_VERSION
            || request.context_id.is_nil()
            || request.read_request_id.is_nil()
            || request.append.term == 0
            || request.read_index == 0
            || request.read_index > request.append.leader_commit
        {
            return Err(metadata_error(
                "read authority probe identity/index/version is invalid",
            ));
        }
        let mut append = self.handle_append_request(request.append)?;
        if self.require_native_metadata_actor_current(true).is_err() {
            // A retired/nonvoting actor may persist restrictive state, but its
            // probe can never count as a fresh voter acknowledgement.
            append.success = false;
            append.match_index = 0;
            append.native_provenance = None;
        }
        Ok(MetadataReadProbeResponse {
            protocol_version: METADATA_READ_AUTHORITY_PROTOCOL_VERSION,
            context_id: request.context_id,
            read_request_id: request.read_request_id,
            read_index: request.read_index,
            append,
            applied_index: self.state.commit_index,
        })
    }

    /// Only fresh transport-correlated acknowledgments enter this context.
    /// Deliberately does not consult or update historical match_index: normal
    /// metadata replication remains responsible for committing proposals.
    pub fn record_read_authority_ack(
        &mut self,
        context: &mut MetadataReadContext,
        ack: &MetadataReadAck,
    ) -> Result<bool> {
        let result = (|| {
            if ack.response.append.cluster_id != self.state.cluster_id {
                return Err(metadata_error(
                    "read authority acknowledgment belongs to another cluster",
                ));
            }
            let current = self.read_quorums()?;
            let voter = &ack.participant.node_id;
            if !current
                .old
                .get(voter)
                .is_some_and(|identity| identity.accepts(&ack.participant))
                && !current
                    .new
                    .get(voter)
                    .is_some_and(|identity| identity.accepts(&ack.participant))
            {
                return Err(metadata_error(
                    "read authority reply is not from a current voter identity",
                ));
            }
            if ack.response.append.term > self.state.current_term {
                self.observe_remote_term(ack.response.append.term)?;
                return Err(metadata_error("read authority observed a higher term"));
            }
            self.validate_read_context(context)?;
            if ack.response.context_id != context.context_id
                || ack.response.read_request_id != context.read_request_id
                || ack.response.read_index != context.read_index
                || ack.response.append.term != context.term
            {
                return Err(metadata_error(
                    "read authority acknowledgment is from another round/term",
                ));
            }
            if !ack.response.append.success {
                return Ok(false);
            }
            if ack.response.append.match_index < context.read_index
                || ack.response.applied_index < context.read_index
            {
                return Err(metadata_error(
                    "read authority peer has not reached the requested metadata floor",
                ));
            }
            let voter = &ack.participant.node_id;
            let old = context
                .quorums
                .old
                .get(voter)
                .is_some_and(|identity| identity.accepts(&ack.participant));
            let new = context
                .quorums
                .new
                .get(voter)
                .is_some_and(|identity| identity.accepts(&ack.participant));
            if !old && !new {
                return Err(metadata_error(
                    "read authority acknowledgment incarnation/certificate is not a voter",
                ));
            }
            if old {
                context.fresh_old.insert(voter.clone());
            }
            if new {
                context.fresh_new.insert(voter.clone());
            }
            Ok(true)
        })();
        if result.is_err() {
            context.failed = true;
        }
        result
    }

    pub fn finish_read_authority(
        &self,
        context: MetadataReadContext,
    ) -> Result<MetadataReadAuthority> {
        self.validate_read_context(&context)?;
        if context.fresh_old.len() < context.quorums.old.len() / 2 + 1
            || context.fresh_new.len() < context.quorums.new.len() / 2 + 1
        {
            return Err(metadata_error(
                "read authority has no fresh old-and-new voter quorum",
            ));
        }
        fn proofs(
            voters: &BTreeMap<ClusterNodeId, MetadataReadVoterIdentity>,
        ) -> Vec<MetadataReadVoterProof> {
            voters
                .iter()
                .map(|(node_id, identity)| MetadataReadVoterProof {
                    node_id: node_id.clone(),
                    incarnation: identity.incarnation,
                    active_certificate_sha256: identity.active_certificate.clone(),
                    pending_certificate_sha256: identity.pending_certificate.clone(),
                })
                .collect()
        }
        let leader_incarnation = context
            .quorums
            .old
            .get(&context.leader_id)
            .or_else(|| context.quorums.new.get(&context.leader_id))
            .ok_or_else(|| metadata_error("metadata read leader missing from its quorum"))?
            .incarnation;
        let old_voters = proofs(&context.quorums.old);
        let new_voters = proofs(&context.quorums.new);
        Ok(MetadataReadAuthority {
            context_id: context.context_id,
            read_request_id: context.read_request_id,
            term: context.term,
            read_index: context.read_index,
            applied_index: self.state.commit_index,
            topology: self.state.committed_topology.clone(),
            restore_activation: self.state.committed_restore_activation.clone(),
            leader_id: context.leader_id,
            leader_incarnation,
            anchor: context.anchor,
            joint_configuration_candidate: self
                .state
                .log
                .last()
                .filter(|entry| entry.topology != self.state.committed_topology)
                .map(|entry| entry.topology.clone()),
            old_voters,
            new_voters,
            fresh_old_voters: context.fresh_old,
            fresh_new_voters: context.fresh_new,
            deadline: context.deadline,
            started: context.started,
        })
    }

    /// Begin an election only if the caller's complete observation is still
    /// current. RPC handlers update this store independently of the cluster
    /// supervisor. Without this compare-and-start boundary, the supervisor
    /// can decide that an election is due, grant a concurrent candidate's
    /// vote, and then overwrite that grant with a stale local candidacy.
    pub fn start_election_if_unchanged(
        &mut self,
        observed_status: &MetadataConsensusStatus,
        observed_contact_ms: u64,
    ) -> Result<Option<MetadataVoteRequest>> {
        if observed_status.role == MetadataConsensusRole::Leader
            || self.status() != *observed_status
            || self.last_leader_contact_ms != observed_contact_ms
        {
            return Ok(None);
        }
        self.start_election().map(Some)
    }

    fn mark_election_activity(&mut self) {
        // Millisecond wall-clock resolution is too coarse to distinguish
        // back-to-back election, vote, and append activity. Preserve the
        // timestamp meaning while making every timer reset observable.
        self.last_leader_contact_ms =
            unix_time_ms().max(self.last_leader_contact_ms.saturating_add(1));
    }

    pub fn observe_remote_term(&mut self, term: u64) -> Result<bool> {
        if term <= self.state.current_term {
            return Ok(false);
        }
        self.step_down(term, None);
        self.mark_election_activity();
        self.persist()?;
        Ok(true)
    }

    pub fn relinquish_leadership(&mut self) -> Result<bool> {
        if self.state.role == MetadataConsensusRole::Follower {
            return Ok(false);
        }
        self.step_down(self.state.current_term, None);
        self.mark_election_activity();
        self.persist()?;
        Ok(true)
    }

    pub fn start_election(&mut self) -> Result<MetadataVoteRequest> {
        self.require_native_metadata_actor_current(true)?;
        let voters = topology_voters(&self.state.committed_topology);
        if !voters.contains(&self.state.node_id) {
            return Err(metadata_error("non-voter cannot start an election"));
        }
        self.state.current_term = self.state.current_term.saturating_add(1);
        self.state.role = MetadataConsensusRole::Candidate;
        self.state.voted_for = Some(self.state.node_id.clone());
        self.state.leader_id = None;
        self.mark_election_activity();
        self.persist()?;
        Ok(MetadataVoteRequest {
            cluster_id: self.state.cluster_id.clone(),
            term: self.state.current_term,
            candidate_id: self.state.node_id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        })
    }

    pub fn handle_vote_request(
        &mut self,
        request: MetadataVoteRequest,
    ) -> Result<MetadataVoteResponse> {
        self.require_native_metadata_owner_current()?;
        self.ensure_cluster(&request.cluster_id)?;
        if request.term < self.state.current_term {
            return Ok(self.vote_response(false));
        }
        if request.term > self.state.current_term {
            self.step_down(request.term, None);
        }
        let voters = topology_voters(&self.state.committed_topology);
        let candidate_is_voter = voters.contains(&request.candidate_id);
        let log_is_current = request.last_log_term > self.last_log_term()
            || (request.last_log_term == self.last_log_term()
                && request.last_log_index >= self.last_log_index());
        let may_vote = self.state.voted_for.is_none()
            || self.state.voted_for.as_ref() == Some(&request.candidate_id);
        let granted = voters.contains(&self.state.node_id)
            && candidate_is_voter
            && log_is_current
            && may_vote
            && self.require_native_metadata_actor_current(true).is_ok();
        if granted {
            self.state.voted_for = Some(request.candidate_id);
            self.mark_election_activity();
        }
        self.persist()?;
        Ok(self.vote_response(granted))
    }

    pub fn become_leader(&mut self, votes: &BTreeSet<ClusterNodeId>) -> Result<()> {
        self.require_native_metadata_actor_current(true)?;
        if self.state.role != MetadataConsensusRole::Candidate {
            return Err(metadata_error("only a candidate can become leader"));
        }
        if !has_quorum(votes, &topology_voters(&self.state.committed_topology)) {
            return Err(metadata_error("candidate has not received a voter quorum"));
        }
        if !votes.contains(&self.state.node_id) {
            return Err(metadata_error("candidate vote set omits the local node"));
        }
        self.state.role = MetadataConsensusRole::Leader;
        self.state.leader_id = Some(self.state.node_id.clone());
        self.mark_election_activity();
        self.reset_match_indexes();
        let barrier = MetadataLogEntry {
            index: self.last_log_index().saturating_add(1),
            term: self.state.current_term,
            leadership_barrier: true,
            topology: self
                .state
                .log
                .last()
                .map(|entry| entry.topology.clone())
                .unwrap_or_else(|| self.state.committed_topology.clone()),
            restore_activation: None,
        };
        self.state.log.push(barrier.clone());
        self.match_index
            .insert(self.state.node_id.clone(), barrier.index);
        self.maybe_commit_leader_proposal()?;
        self.persist()
    }

    pub fn propose_topology(&mut self, topology: ClusterTopology) -> Result<MetadataLogEntry> {
        self.require_native_metadata_actor_current(true)?;
        if self.state.role != MetadataConsensusRole::Leader {
            return Err(metadata_error("only the leader can propose topology"));
        }
        topology.validate()?;
        self.ensure_cluster(&topology.cluster_id)?;
        crate::distribution::validate_native_store_birth_transition(
            self.state
                .log
                .last()
                .map(|entry| &entry.topology)
                .unwrap_or(&self.state.committed_topology),
            &topology,
        )?;
        crate::distribution::ensure_range_image_domain(
            self.state.committed_topology.range_image_bootstrap_id,
            topology.range_image_bootstrap_id,
        )?;
        self.require_native_learner_promotions_ready(&topology)?;
        let latest_generation = self
            .state
            .log
            .last()
            .map(|entry| entry.topology.generation)
            .unwrap_or(self.state.committed_topology.generation);
        if self.state.log.iter().any(|entry| !entry.leadership_barrier) {
            return Err(metadata_error(
                "only one uncommitted topology change may be in flight",
            ));
        }
        if topology.generation <= latest_generation {
            return Err(metadata_error(format!(
                "topology generation {} does not advance latest generation {latest_generation}",
                topology.generation
            )));
        }
        let entry = MetadataLogEntry {
            index: self.last_log_index().saturating_add(1),
            term: self.state.current_term,
            leadership_barrier: false,
            topology,
            restore_activation: None,
        };
        self.state.log.push(entry.clone());
        self.match_index
            .insert(self.state.node_id.clone(), entry.index);
        self.maybe_commit_leader_proposal()?;
        self.persist()?;
        Ok(entry)
    }

    /// Propose one restore activation as a compact metadata-consensus control
    /// record. The topology is intentionally unchanged. A leader cannot
    /// publish this decision concurrently with another topology/control entry.
    pub fn propose_restore_activation(
        &mut self,
        activation: MetadataRestoreActivation,
    ) -> Result<MetadataLogEntry> {
        self.require_native_metadata_actor_current(true)?;
        if self.state.role != MetadataConsensusRole::Leader {
            return Err(metadata_error(
                "only the leader can propose restore activation",
            ));
        }
        activation.validate()?;
        let topology = self
            .state
            .log
            .last()
            .map(|entry| entry.topology.clone())
            .unwrap_or_else(|| self.state.committed_topology.clone());
        let topology_metadata =
            crate::distribution_supervisor::ClusterBackupMetadata::capture(&topology)?;
        if activation.cluster_id != self.state.cluster_id
            || activation.topology_generation != topology.generation
            || activation.topology_sha256 != topology_metadata.topology_sha256
        {
            return Err(metadata_error(
                "restore activation belongs to another cluster or topology",
            ));
        }
        if self.state.log.iter().any(|entry| !entry.leadership_barrier) {
            return Err(metadata_error(
                "only one uncommitted metadata change may be in flight",
            ));
        }
        if self
            .state
            .committed_restore_activation
            .as_ref()
            .is_some_and(|committed| {
                committed.activation_id == activation.activation_id
                    || committed.activated_at_ms >= activation.activated_at_ms
            })
        {
            return Err(metadata_error(
                "restore activation is duplicate or does not advance the committed decision",
            ));
        }
        let entry = MetadataLogEntry {
            index: self.last_log_index().saturating_add(1),
            term: self.state.current_term,
            leadership_barrier: false,
            topology,
            restore_activation: Some(activation),
        };
        self.state.log.push(entry.clone());
        self.match_index
            .insert(self.state.node_id.clone(), entry.index);
        self.maybe_commit_leader_proposal()?;
        self.persist()?;
        Ok(entry)
    }

    pub fn append_request_from(
        &self,
        next_index: u64,
        max_entries: usize,
    ) -> Result<MetadataAppendRequest> {
        self.require_native_metadata_actor_current(true)?;
        if self.state.role != MetadataConsensusRole::Leader {
            return Err(metadata_error("only the leader can append metadata"));
        }
        if max_entries == 0 {
            return Err(metadata_error("metadata append batch must be non-zero"));
        }
        let snapshot = (next_index <= self.state.base_index).then(|| MetadataSnapshot {
            last_included_index: self.state.base_index,
            last_included_term: self.state.base_term,
            topology: self.state.committed_topology.clone(),
            restore_activation: self.state.committed_restore_activation.clone(),
        });
        let effective_next = snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index.saturating_add(1))
            .unwrap_or(next_index);
        let prev_log_index = effective_next.saturating_sub(1);
        let prev_log_term = self.term_at(prev_log_index).unwrap_or(0);
        Ok(MetadataAppendRequest {
            cluster_id: self.state.cluster_id.clone(),
            term: self.state.current_term,
            leader_id: self.state.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries: self
                .state
                .log
                .iter()
                .filter(|entry| entry.index >= effective_next)
                .take(max_entries)
                .cloned()
                .collect(),
            leader_commit: self.state.commit_index,
            snapshot,
        })
    }

    pub fn handle_append_request(
        &mut self,
        request: MetadataAppendRequest,
    ) -> Result<MetadataAppendResponse> {
        self.require_native_metadata_owner_current()?;
        self.ensure_cluster(&request.cluster_id)?;
        if request.term < self.state.current_term {
            return Ok(self.append_response(false, self.last_log_index().saturating_add(1)));
        }
        let voters = topology_voters(&self.state.committed_topology);
        if !voters.contains(&request.leader_id) {
            return Err(metadata_error(format!(
                "append leader {} is not a committed voter",
                request.leader_id
            )));
        }
        if let Some(snapshot) = &request.snapshot {
            crate::distribution::ensure_range_image_domain(
                self.state.committed_topology.range_image_bootstrap_id,
                snapshot.topology.range_image_bootstrap_id,
            )?;
        }
        for entry in &request.entries {
            crate::distribution::ensure_range_image_domain(
                self.state.committed_topology.range_image_bootstrap_id,
                entry.topology.range_image_bootstrap_id,
            )?;
        }
        self.validate_native_birth_append(&request)?;
        self.step_down(request.term, Some(request.leader_id));
        self.mark_election_activity();
        let snapshot_frontier = request
            .snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.last_included_index);
        if let Some(snapshot) = request.snapshot {
            self.install_snapshot(snapshot)?;
        }
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            let conflict_index = request
                .prev_log_index
                .min(self.last_log_index().saturating_add(1))
                .max(self.state.base_index.saturating_add(1));
            self.persist()?;
            return Ok(self.append_response(false, conflict_index));
        }
        // A matching previous entry (and any installed snapshot) establishes
        // only this prefix. An untouched local tail may belong to another
        // leader and cannot become evidence for this bounded request.
        let mut matched_frontier = request.prev_log_index.max(snapshot_frontier);
        for incoming in request.entries {
            validate_log_entry_shape(&incoming, &self.state.cluster_id)?;
            crate::distribution::ensure_range_image_domain(
                self.state.committed_topology.range_image_bootstrap_id,
                incoming.topology.range_image_bootstrap_id,
            )?;
            if matched_frontier.checked_add(1) != Some(incoming.index) {
                self.persist()?;
                return Ok(self.append_response(false, matched_frontier.saturating_add(1)));
            }
            if incoming.index <= self.state.base_index {
                continue;
            }
            let incoming_index = incoming.index;
            if let Some(position) = self
                .state
                .log
                .iter()
                .position(|existing| existing.index == incoming.index)
            {
                if self.state.log[position] != incoming {
                    self.state.log.truncate(position);
                    self.state.log.push(incoming);
                }
            } else if incoming.index == self.last_log_index().saturating_add(1) {
                self.state.log.push(incoming);
            } else {
                self.persist()?;
                return Ok(self.append_response(false, self.last_log_index().saturating_add(1)));
            }
            matched_frontier = incoming_index;
        }
        // Commit only this request's validated prefix, never an untouched tail.
        self.advance_follower_commit(request.leader_commit.min(matched_frontier))?;
        self.persist_owned_append_response(matched_frontier)
    }

    fn validate_native_birth_append(&self, request: &MetadataAppendRequest) -> Result<()> {
        let mut candidate = self.state.clone();
        if let Some(snapshot) = &request.snapshot {
            if snapshot.last_included_index >= candidate.commit_index {
                snapshot.topology.validate()?;
                crate::distribution::validate_native_store_birth_snapshot(
                    &candidate.committed_topology,
                    &snapshot.topology,
                )?;
                // A committed snapshot supersedes uncommitted proposals. Only
                // a matching log anchor permits retaining its later suffix.
                let matching_anchor = if snapshot.last_included_index == candidate.base_index {
                    snapshot.last_included_term == candidate.base_term
                } else {
                    candidate.log.iter().any(|entry| {
                        entry.index == snapshot.last_included_index
                            && entry.term == snapshot.last_included_term
                    })
                };
                candidate.base_index = snapshot.last_included_index;
                candidate.base_term = snapshot.last_included_term;
                candidate.commit_index = snapshot.last_included_index;
                candidate.committed_topology = snapshot.topology.clone();
                candidate.committed_restore_activation = snapshot.restore_activation.clone();
                if matching_anchor {
                    candidate
                        .log
                        .retain(|entry| entry.index > snapshot.last_included_index);
                } else {
                    candidate.log.clear();
                }
            }
        }
        let previous_term = if request.prev_log_index == candidate.base_index {
            Some(candidate.base_term)
        } else {
            candidate
                .log
                .iter()
                .find(|entry| entry.index == request.prev_log_index)
                .map(|entry| entry.term)
        };
        if previous_term != Some(request.prev_log_term) {
            return validate_state(&candidate);
        }
        for incoming in &request.entries {
            if incoming.index <= candidate.base_index {
                continue;
            }
            validate_log_entry_shape(incoming, &candidate.cluster_id)?;
            if let Some(position) = candidate
                .log
                .iter()
                .position(|entry| entry.index == incoming.index)
            {
                if candidate.log[position] != *incoming {
                    if candidate.log[position].term == incoming.term {
                        return Err(metadata_error(
                            "conflicting metadata entry at the same term and index",
                        ));
                    }
                    // Different-term uncommitted proposals are replaceable.
                    // validate_state below still checks every incoming transition
                    // against the committed identity anchor and prior entries.
                    candidate.log.truncate(position);
                    candidate.log.push(incoming.clone());
                }
            } else if incoming.index
                == candidate
                    .log
                    .last()
                    .map(|entry| entry.index)
                    .unwrap_or(candidate.base_index)
                    .saturating_add(1)
            {
                candidate.log.push(incoming.clone());
            } else {
                return validate_state(&candidate);
            }
        }
        validate_state(&candidate)
    }

    pub fn record_append_response(
        &mut self,
        expected_node_id: &ClusterNodeId,
        response: &MetadataAppendResponse,
    ) -> Result<bool> {
        self.ensure_cluster(&response.cluster_id)?;
        if &response.node_id != expected_node_id {
            return Err(metadata_error("append response node identity mismatch"));
        }
        if response.term > self.state.current_term {
            self.step_down(response.term, None);
            self.persist()?;
            return Ok(false);
        }
        if response.term != self.state.current_term
            || self.state.role != MetadataConsensusRole::Leader
            || !response.success
        {
            return Ok(false);
        }
        let old_voters = topology_voters(&self.state.committed_topology);
        let new_voters = self
            .state
            .log
            .first()
            .map(|entry| topology_voters(&entry.topology))
            .unwrap_or_else(|| old_voters.clone());
        let committed_members = self
            .state
            .committed_topology
            .nodes
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if !committed_members.contains(expected_node_id) && !new_voters.contains(expected_node_id) {
            return Err(metadata_error(format!(
                "append response from non-member {expected_node_id}"
            )));
        }
        self.require_native_metadata_actor_current(true)?;
        self.require_current_native_append_ack(expected_node_id, response)?;
        self.match_index
            .entry(expected_node_id.clone())
            .and_modify(|known| *known = (*known).max(response.match_index))
            .or_insert(response.match_index);
        let committed = self.maybe_commit_leader_proposal()?;
        self.persist()?;
        Ok(committed)
    }

    fn maybe_commit_leader_proposal(&mut self) -> Result<bool> {
        let Some(entry) = self
            .state
            .log
            .iter()
            .rev()
            .find(|entry| entry.term == self.state.current_term)
            .cloned()
        else {
            return Ok(false);
        };
        let replicated = self
            .match_index
            .iter()
            .filter_map(|(node_id, index)| (*index >= entry.index).then_some(node_id.clone()))
            .collect::<BTreeSet<_>>();
        let old_voters = topology_voters(&self.state.committed_topology);
        let new_voters = topology_voters(&entry.topology);
        if !has_quorum(&replicated, &old_voters) || !has_quorum(&replicated, &new_voters) {
            return Ok(false);
        }
        self.commit_entry(entry)?;
        Ok(true)
    }

    fn advance_follower_commit(&mut self, leader_commit: u64) -> Result<()> {
        let target = leader_commit.min(self.last_log_index());
        if target <= self.state.commit_index {
            return Ok(());
        }
        let entry = self
            .state
            .log
            .iter()
            .find(|entry| entry.index == target)
            .cloned()
            .ok_or_else(|| metadata_error("committed metadata entry is absent from local log"))?;
        self.commit_entry(entry)
    }

    fn commit_entry(&mut self, entry: MetadataLogEntry) -> Result<()> {
        validate_state(&self.state)?;
        validate_log_entry_shape(&entry, &self.state.cluster_id)?;
        crate::distribution::ensure_range_image_domain(
            self.state.committed_topology.range_image_bootstrap_id,
            entry.topology.range_image_bootstrap_id,
        )?;
        if let Some(activation) = &entry.restore_activation {
            validate_committed_activation_progress(
                self.state.committed_restore_activation.as_ref(),
                Some(activation),
            )?;
        }
        self.state.commit_index = entry.index;
        self.state.base_index = entry.index;
        self.state.base_term = entry.term;
        self.state.committed_topology = entry.topology;
        if let Some(activation) = entry.restore_activation {
            self.state.committed_restore_activation = Some(activation);
        }
        self.state
            .log
            .retain(|candidate| candidate.index > entry.index);
        self.demote_native_retired_metadata_actor();
        self.reset_match_indexes();
        Ok(())
    }

    fn install_snapshot(&mut self, snapshot: MetadataSnapshot) -> Result<()> {
        snapshot.topology.validate()?;
        self.ensure_cluster(&snapshot.topology.cluster_id)?;
        crate::distribution::ensure_range_image_domain(
            self.state.committed_topology.range_image_bootstrap_id,
            snapshot.topology.range_image_bootstrap_id,
        )?;
        if let Some(activation) = &snapshot.restore_activation {
            validate_historical_activation_against_topology(
                activation,
                &snapshot.topology,
                &self.state.cluster_id,
            )?;
        }
        if snapshot.last_included_index < self.state.commit_index {
            return Ok(());
        }
        if snapshot.last_included_index == self.state.commit_index
            && (snapshot.last_included_term != self.state.base_term
                || snapshot.topology != self.state.committed_topology
                || snapshot.restore_activation != self.state.committed_restore_activation)
        {
            return Err(metadata_error(
                "snapshot conflicts with the committed metadata index",
            ));
        }
        crate::distribution::validate_native_store_birth_snapshot(
            &self.state.committed_topology,
            &snapshot.topology,
        )?;
        let matching_anchor =
            self.term_at(snapshot.last_included_index) == Some(snapshot.last_included_term);
        validate_committed_activation_progress(
            self.state.committed_restore_activation.as_ref(),
            snapshot.restore_activation.as_ref(),
        )?;
        let mut candidate = self.state.clone();
        candidate.base_index = snapshot.last_included_index;
        candidate.base_term = snapshot.last_included_term;
        candidate.commit_index = snapshot.last_included_index;
        candidate.committed_topology = snapshot.topology;
        candidate.committed_restore_activation = snapshot.restore_activation;
        if matching_anchor {
            candidate
                .log
                .retain(|entry| entry.index > snapshot.last_included_index);
        } else {
            candidate.log.clear();
        }
        validate_state(&candidate)?;
        self.state = candidate;
        self.demote_native_retired_metadata_actor();
        Ok(())
    }

    fn last_log_index(&self) -> u64 {
        self.state
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.state.base_index)
    }

    fn last_log_term(&self) -> u64 {
        self.state
            .log
            .last()
            .map(|entry| entry.term)
            .unwrap_or(self.state.base_term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.state.base_index {
            return Some(self.state.base_term);
        }
        self.state
            .log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    fn reset_match_indexes(&mut self) {
        self.match_index.clear();
        for member in self
            .state
            .committed_topology
            .nodes
            .values()
            .filter(|node| node.lifecycle != ClusterNodeLifecycle::Decommissioned)
        {
            self.match_index.insert(member.id.clone(), 0);
        }
        self.match_index
            .insert(self.state.node_id.clone(), self.last_log_index());
    }

    fn step_down(&mut self, term: u64, leader_id: Option<ClusterNodeId>) {
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
        }
        self.state.role = MetadataConsensusRole::Follower;
        self.state.leader_id = leader_id;
    }

    fn ensure_cluster(&self, cluster_id: &ClusterId) -> Result<()> {
        if cluster_id != &self.state.cluster_id {
            return Err(metadata_error(format!(
                "cluster mismatch: local={} request={cluster_id}",
                self.state.cluster_id
            )));
        }
        Ok(())
    }

    fn vote_response(&self, vote_granted: bool) -> MetadataVoteResponse {
        MetadataVoteResponse {
            cluster_id: self.state.cluster_id.clone(),
            term: self.state.current_term,
            voter_id: self.state.node_id.clone(),
            vote_granted,
        }
    }

    fn append_response(&self, success: bool, conflict_index: u64) -> MetadataAppendResponse {
        MetadataAppendResponse {
            cluster_id: self.state.cluster_id.clone(),
            term: self.state.current_term,
            node_id: self.state.node_id.clone(),
            success,
            match_index: if success { self.last_log_index() } else { 0 },
            conflict_index,
            native_receiver: None,
            native_provenance: None,
        }
    }

    fn persist(&self) -> Result<()> {
        validate_state(&self.state)?;
        // A legacy learner may have learned its own enforced birth since
        // open. Its cached ownerless mode cannot persist/ack that transition
        // as an actual native metadata store.
        self.require_native_metadata_owner_current()?;
        fs::create_dir_all(&self.root)?;
        let mut envelope = MetadataConsensusEnvelope {
            format_version: self.state.format_version,
            checksum_sha256: String::new(),
            state: self.state.clone(),
        };
        envelope.checksum_sha256 = state_checksum(&envelope.state)?;
        crate::storage::write_atomic(
            &self.root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )?;
        // A DB replacement during durable I/O cannot turn into a positive
        // vote/Append response from the ended actual-open owner.
        self.require_native_metadata_owner_current()
    }
}

fn topology_voters(topology: &ClusterTopology) -> BTreeSet<ClusterNodeId> {
    topology.metadata_voters()
}

fn topology_learners(topology: &ClusterTopology) -> BTreeSet<ClusterNodeId> {
    topology.metadata_learners()
}

fn status_from_state(state: &MetadataConsensusState) -> MetadataConsensusStatus {
    let last_log_index = state
        .log
        .last()
        .map(|entry| entry.index)
        .unwrap_or(state.base_index);
    let last_log_term = state
        .log
        .last()
        .map(|entry| entry.term)
        .unwrap_or(state.base_term);
    MetadataConsensusStatus {
        cluster_id: state.cluster_id.clone(),
        node_id: state.node_id.clone(),
        role: state.role,
        current_term: state.current_term,
        voted_for: state.voted_for.clone(),
        leader_id: state.leader_id.clone(),
        commit_index: state.commit_index,
        last_log_index,
        last_log_term,
        topology_generation: state.committed_topology.generation,
        voters: topology_voters(&state.committed_topology)
            .into_iter()
            .collect(),
        learners: topology_learners(&state.committed_topology)
            .into_iter()
            .collect(),
    }
}

fn has_quorum(acks: &BTreeSet<ClusterNodeId>, voters: &BTreeSet<ClusterNodeId>) -> bool {
    !voters.is_empty()
        && voters
            .iter()
            .filter(|node_id| acks.contains(*node_id))
            .count()
            > voters.len() / 2
}

fn state_checksum(state: &MetadataConsensusState) -> Result<String> {
    let bytes = serde_json::to_vec(state)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn decode_state(bytes: &[u8]) -> Result<MetadataConsensusState> {
    let envelope = serde_json::from_slice::<MetadataConsensusEnvelope>(bytes)?;
    if !matches!(
        envelope.format_version,
        CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION
            | native_metadata_store::NATIVE_METADATA_CONSENSUS_FORMAT
    ) || envelope.format_version != envelope.state.format_version
    {
        return Err(metadata_error(format!(
            "unsupported state format {}; expected {}",
            envelope.format_version, CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION
        )));
    }
    let expected = state_checksum(&envelope.state)?;
    if envelope.checksum_sha256 != expected {
        return Err(metadata_error("state checksum mismatch"));
    }
    Ok(envelope.state)
}

fn validate_state(state: &MetadataConsensusState) -> Result<()> {
    if !matches!(
        (state.format_version, state.native_store_id),
        (CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION, None)
            | (
                native_metadata_store::NATIVE_METADATA_CONSENSUS_FORMAT,
                Some(_)
            )
    ) || state.native_store_id.is_some_and(|id| id.is_nil())
    {
        return Err(metadata_error("state payload format mismatch"));
    }
    state.committed_topology.validate()?;
    if state.committed_topology.cluster_id != state.cluster_id {
        return Err(metadata_error(
            "committed topology belongs to another cluster",
        ));
    }
    if state.commit_index != state.base_index {
        return Err(metadata_error(
            "compacted metadata commit and base indexes differ",
        ));
    }
    if let Some(activation) = &state.committed_restore_activation {
        validate_historical_activation_against_topology(
            activation,
            &state.committed_topology,
            &state.cluster_id,
        )?;
    }
    let mut expected = state.base_index.saturating_add(1);
    let mut latest_generation = state.committed_topology.generation;
    let mut latest_topology = state.committed_topology.clone();
    let mut latest_activation = state.committed_restore_activation.as_ref();
    let mut latest_activation_ms = latest_activation
        .map(|activation| activation.activated_at_ms)
        .unwrap_or(0);
    for entry in &state.log {
        validate_log_entry_shape(entry, &state.cluster_id)?;
        crate::distribution::ensure_range_image_domain(
            state.committed_topology.range_image_bootstrap_id,
            entry.topology.range_image_bootstrap_id,
        )?;
        if entry.index != expected {
            return Err(metadata_error("metadata log indexes are not contiguous"));
        }
        crate::distribution::validate_native_store_birth_transition(
            &latest_topology,
            &entry.topology,
        )?;
        if entry.leadership_barrier {
            if entry.topology.generation < latest_generation {
                return Err(metadata_error(
                    "leadership barrier regresses topology generation",
                ));
            }
            latest_topology = entry.topology.clone();
        } else if let Some(activation) = &entry.restore_activation {
            if entry.topology != latest_topology
                || activation.activated_at_ms <= latest_activation_ms
            {
                return Err(metadata_error(
                    "restore activation changes topology or does not advance its decision time",
                ));
            }
            validate_committed_activation_progress(latest_activation, Some(activation))?;
            latest_activation = Some(activation);
            latest_activation_ms = activation.activated_at_ms;
        } else {
            if entry.topology.generation <= latest_generation {
                return Err(metadata_error(
                    "metadata topology entries do not advance generation",
                ));
            }
            latest_topology = entry.topology.clone();
        }
        latest_generation = entry.topology.generation;
        expected = expected.saturating_add(1);
    }
    Ok(())
}

fn validate_log_entry_shape(entry: &MetadataLogEntry, cluster_id: &ClusterId) -> Result<()> {
    entry.topology.validate()?;
    if entry.topology.cluster_id != *cluster_id {
        return Err(metadata_error("log entry belongs to another cluster"));
    }
    if entry.leadership_barrier && entry.restore_activation.is_some() {
        return Err(metadata_error(
            "leadership barrier cannot carry a restore activation",
        ));
    }
    if let Some(activation) = &entry.restore_activation {
        validate_activation_against_topology(activation, &entry.topology, cluster_id)?;
    }
    Ok(())
}

/// A retained committed decision describes the topology at activation, not a
/// permanent ban on later membership/load changes. Local envelopes are checked
/// before state validation; snapshot callers must authenticate the actual
/// consensus source before reaching this store. Older topology hashes cannot
/// be recomputed from a later compacted topology, and are historical lineage,
/// never independent proof of fresh restore admission. NEW decisions still use
/// `validate_activation_against_topology` at proposal/log admission.
fn validate_historical_activation_against_topology(
    activation: &MetadataRestoreActivation,
    topology: &ClusterTopology,
    cluster_id: &ClusterId,
) -> Result<()> {
    activation.validate()?;
    if activation.cluster_id != *cluster_id
        || activation.cluster_id != topology.cluster_id
        || activation.topology_generation > topology.generation
    {
        return Err(metadata_error(
            "historical restore activation belongs to another cluster or future topology",
        ));
    }
    if activation.topology_generation == topology.generation {
        validate_activation_against_topology(activation, topology, cluster_id)?;
    }
    Ok(())
}

/// Ordinary Raft replication cannot erase or rewrite an already committed
/// restore decision. A fresh authorized restore into a new root has no previous
/// lineage; that separate admission still validates its complete certificate.
fn validate_committed_activation_progress(
    previous: Option<&MetadataRestoreActivation>,
    next: Option<&MetadataRestoreActivation>,
) -> Result<()> {
    if let Some(next) = next {
        next.validate()?;
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    let Some(next) = next else {
        return Err(metadata_error(
            "snapshot erases committed restore activation",
        ));
    };
    if previous == next {
        return Ok(());
    }
    if previous.cluster_id != next.cluster_id
        || previous.activation_id == next.activation_id
        || next.activated_at_ms <= previous.activated_at_ms
        || next.topology_generation < previous.topology_generation
    {
        return Err(metadata_error(
            "restore activation regresses or rewrites a committed decision",
        ));
    }
    Ok(())
}

fn validate_activation_against_topology(
    activation: &MetadataRestoreActivation,
    topology: &ClusterTopology,
    cluster_id: &ClusterId,
) -> Result<()> {
    activation.validate()?;
    let captured = crate::distribution_supervisor::ClusterBackupMetadata::capture(topology)?;
    if activation.cluster_id != *cluster_id
        || activation.cluster_id != topology.cluster_id
        || activation.topology_generation != topology.generation
        || activation.topology_sha256 != captured.topology_sha256
    {
        return Err(metadata_error(
            "restore activation does not match its metadata topology",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{ClusterNode, DistributionConfig, DistributionStore};

    fn topology_with_nodes(count: usize) -> ClusterTopology {
        let dir = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("metadata-cluster").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "127.0.0.1:10001".to_string(),
            replication_factor: 1,
            initial_ranges: 2,
            ..DistributionConfig::default()
        };
        let mut distribution =
            DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        for number in 2..=count {
            distribution
                .join_node(
                    ClusterNode::new(
                        ClusterNodeId::new(format!("n{number}")).unwrap(),
                        format!("127.0.0.1:{}", 10_000 + number),
                        1,
                        1_000_000,
                        10 + number as u64,
                    )
                    .unwrap(),
                    &actor,
                    10 + number as u64,
                )
                .unwrap();
        }
        distribution.topology().clone()
    }

    fn open(root: &Path, node: &str, topology: &ClusterTopology) -> MetadataConsensusStore {
        MetadataConsensusStore::open(
            root,
            topology.cluster_id.clone(),
            ClusterNodeId::new(node).unwrap(),
            topology.clone(),
            false,
        )
        .unwrap()
    }

    fn restore_activation(
        topology: &ClusterTopology,
        activated_at_ms: u64,
    ) -> MetadataRestoreActivation {
        MetadataRestoreActivation {
            format_version: METADATA_RESTORE_ACTIVATION_FORMAT_VERSION,
            activation_id: Uuid::now_v7(),
            cluster_id: topology.cluster_id.clone(),
            certificate_id: Uuid::now_v7(),
            certificate_checksum_sha256: "a".repeat(64),
            admission_id: Uuid::now_v7(),
            admission_checksum_sha256: "b".repeat(64),
            plan_id: Uuid::now_v7(),
            topology_generation: topology.generation,
            topology_sha256: crate::distribution_supervisor::ClusterBackupMetadata::capture(
                topology,
            )
            .unwrap()
            .topology_sha256,
            acknowledged_nodes: 3,
            node_ack_set_sha256: "c".repeat(64),
            activated_at_ms,
        }
    }

    fn read_participant(topology: &ClusterTopology, name: &str) -> MetadataReadParticipant {
        let id = ClusterNodeId::new(name).unwrap();
        MetadataReadParticipant::from_transport(
            id.clone(),
            topology.nodes[&id].incarnation,
            None,
            true,
        )
    }

    fn anchor_leader(leader: &mut MetadataConsensusStore, follower: &mut MetadataConsensusStore) {
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                leader.state.node_id.clone(),
                follower.state.node_id.clone(),
            ]))
            .unwrap();
        let response = follower
            .handle_append_request(leader.append_request_from(1, 16).unwrap())
            .unwrap();
        leader
            .record_append_response(&follower.state.node_id, &response)
            .unwrap();
        assert_eq!(
            leader.term_at(leader.state.commit_index),
            Some(leader.state.current_term)
        );
    }

    fn read_ack(
        leader: &MetadataConsensusStore,
        context: &MetadataReadContext,
        follower: &mut MetadataConsensusStore,
        topology: &ClusterTopology,
    ) -> MetadataReadAck {
        let voter = follower.state.node_id.clone();
        let request = leader.read_authority_probe(context, &voter, 1, 16).unwrap();
        let response = follower
            .handle_read_authority_probe(request.clone())
            .unwrap();
        MetadataReadAck::from_transport(
            &request,
            &voter,
            read_participant(topology, voter.as_str()),
            response,
        )
        .unwrap()
    }

    #[test]
    fn read_authority_rejects_uncommitted_current_term_anchor() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(dir.path(), "n1", &topology);
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();
        assert_eq!(leader.state.commit_index, 0);
        assert!(leader
            .begin_read_authority(
                Uuid::now_v7(),
                Duration::from_secs(1),
                &read_participant(&topology, "n1")
            )
            .unwrap_err()
            .to_string()
            .contains("committed current-term"));
    }

    #[test]
    fn read_authority_healthy_quorum_returns_applied_metadata_without_cached_votes() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(&dir.path().join("leader"), "n1", &topology);
        let mut follower = open(&dir.path().join("follower"), "n2", &topology);
        anchor_leader(&mut leader, &mut follower);
        let cached_match = leader.match_index.clone();
        let local = read_participant(&topology, "n1");
        let no_round = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        assert!(leader
            .finish_read_authority(no_round)
            .unwrap_err()
            .to_string()
            .contains("no fresh"));
        let mut context = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        let nonce = context.context_id();
        let ack = read_ack(&leader, &context, &mut follower, &topology);
        assert!(leader
            .record_read_authority_ack(&mut context, &ack)
            .unwrap());
        let authority = leader.finish_read_authority(context).unwrap();
        assert_eq!(authority.context_id(), nonce);
        assert!(authority.applied_index() >= authority.read_index());
        assert_eq!(authority.topology(), &topology);
        assert_eq!(
            leader.match_index, cached_match,
            "read votes must not become historical commit evidence"
        );
    }

    #[test]
    fn read_authority_rejects_cached_ack_from_another_fresh_nonce() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(&dir.path().join("leader"), "n1", &topology);
        let mut follower = open(&dir.path().join("follower"), "n2", &topology);
        anchor_leader(&mut leader, &mut follower);
        let local = read_participant(&topology, "n1");
        let old_context = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        let cached = read_ack(&leader, &old_context, &mut follower, &topology);
        let mut current = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        assert_ne!(old_context.context_id(), current.context_id());
        assert!(leader
            .record_read_authority_ack(&mut current, &cached)
            .is_err());
        assert!(leader.finish_read_authority(current).is_err());
    }

    #[test]
    fn read_authority_old_leader_cannot_serve_after_higher_term_reply() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(&dir.path().join("leader"), "n1", &topology);
        let mut follower = open(&dir.path().join("follower"), "n2", &topology);
        anchor_leader(&mut leader, &mut follower);
        let mut context = leader
            .begin_read_authority(
                Uuid::now_v7(),
                Duration::from_secs(1),
                &read_participant(&topology, "n1"),
            )
            .unwrap();
        follower.start_election().unwrap();
        follower
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n2").unwrap(),
                ClusterNodeId::new("n3").unwrap(),
            ]))
            .unwrap();
        let ack = read_ack(&leader, &context, &mut follower, &topology);
        assert!(leader
            .record_read_authority_ack(&mut context, &ack)
            .is_err());
        assert_eq!(leader.state.role, MetadataConsensusRole::Follower);
        assert_eq!(leader.state.current_term, follower.state.current_term);
        assert!(leader.finish_read_authority(context).is_err());
    }

    #[test]
    fn read_authority_joint_quorums_require_fresh_majorities_on_both_sides() {
        let mut topology = topology_with_nodes(5);
        for name in ["n4", "n5"] {
            topology
                .nodes
                .get_mut(&ClusterNodeId::new(name).unwrap())
                .unwrap()
                .metadata_role = crate::distribution::MetadataMemberRole::Learner;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(&dir.path().join("leader"), "n1", &topology);
        let mut old_voter = open(&dir.path().join("old"), "n2", &topology);
        let mut new_voter = open(&dir.path().join("new"), "n4", &topology);
        anchor_leader(&mut leader, &mut old_voter);
        let mut proposed = topology.clone();
        proposed.generation += 1;
        for name in ["n2", "n3"] {
            proposed
                .nodes
                .get_mut(&ClusterNodeId::new(name).unwrap())
                .unwrap()
                .metadata_role = crate::distribution::MetadataMemberRole::Learner;
        }
        for name in ["n4", "n5"] {
            proposed
                .nodes
                .get_mut(&ClusterNodeId::new(name).unwrap())
                .unwrap()
                .metadata_role = crate::distribution::MetadataMemberRole::Voter;
        }
        leader.propose_topology(proposed).unwrap();
        let local = read_participant(&topology, "n1");
        for old_side in [true, false] {
            let mut context = leader
                .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
                .unwrap();
            let ack = read_ack(
                &leader,
                &context,
                if old_side {
                    &mut old_voter
                } else {
                    &mut new_voter
                },
                &topology,
            );
            leader
                .record_read_authority_ack(&mut context, &ack)
                .unwrap();
            assert!(
                leader.finish_read_authority(context).is_err(),
                "one side's majority must not certify a joint read"
            );
        }
        let mut context = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        for follower in [&mut old_voter, &mut new_voter] {
            let ack = read_ack(&leader, &context, follower, &topology);
            leader
                .record_read_authority_ack(&mut context, &ack)
                .unwrap();
        }
        let authority = leader.finish_read_authority(context).unwrap();
        assert_eq!(
            authority.topology(),
            &topology,
            "uncommitted proposed topology is not served"
        );
    }

    #[test]
    fn read_authority_deadline_and_voter_identity_fail_closed() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(&dir.path().join("leader"), "n1", &topology);
        let mut follower = open(&dir.path().join("follower"), "n2", &topology);
        anchor_leader(&mut leader, &mut follower);
        let local = read_participant(&topology, "n1");
        let mut expired = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        expired.deadline = Instant::now() - Duration::from_millis(1);
        assert!(leader.finish_read_authority(expired).is_err());
        let mut context = leader
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
            .unwrap();
        let mut ack = read_ack(&leader, &context, &mut follower, &topology);
        ack.participant.member_incarnation += 1;
        assert!(leader
            .record_read_authority_ack(&mut context, &ack)
            .is_err());
        assert!(leader.finish_read_authority(context).is_err());
    }

    #[test]
    fn vote_and_term_are_durable_and_single_choice_per_term() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut voter = open(dir.path(), "n2", &topology);
        let first = MetadataVoteRequest {
            cluster_id: topology.cluster_id.clone(),
            term: 7,
            candidate_id: ClusterNodeId::new("n1").unwrap(),
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(voter.handle_vote_request(first).unwrap().vote_granted);
        drop(voter);

        let mut reopened = open(dir.path(), "n2", &topology);
        assert_eq!(reopened.status().current_term, 7);
        assert_eq!(
            reopened.status().voted_for,
            Some(ClusterNodeId::new("n1").unwrap())
        );
        let competing = MetadataVoteRequest {
            cluster_id: topology.cluster_id.clone(),
            term: 7,
            candidate_id: ClusterNodeId::new("n3").unwrap(),
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(
            !reopened
                .handle_vote_request(competing)
                .unwrap()
                .vote_granted
        );
    }

    #[test]
    fn stale_election_observation_cannot_override_a_concurrent_vote() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut voter = open(dir.path(), "n2", &topology);
        let observed_status = voter.status();
        let observed_contact_ms = voter.last_leader_contact_ms();

        let vote = MetadataVoteRequest {
            cluster_id: topology.cluster_id.clone(),
            term: observed_status.current_term.saturating_add(1),
            candidate_id: ClusterNodeId::new("n1").unwrap(),
            last_log_index: observed_status.last_log_index,
            last_log_term: observed_status.last_log_term,
        };
        assert!(voter.handle_vote_request(vote).unwrap().vote_granted);

        assert!(voter
            .start_election_if_unchanged(&observed_status, observed_contact_ms)
            .unwrap()
            .is_none());
        let current = voter.status();
        assert_eq!(current.role, MetadataConsensusRole::Follower);
        assert_eq!(current.current_term, observed_status.current_term + 1);
        assert_eq!(current.voted_for, Some(ClusterNodeId::new("n1").unwrap()));
    }

    #[test]
    fn topology_commits_only_after_joint_old_and_new_quorums() {
        let topology = topology_with_nodes(3);
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(dir.path(), "n1", &topology);
        let vote = leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();
        let mut expanded = topology_with_nodes(4);
        expanded.generation = topology.generation.saturating_add(1);
        let entry = leader.propose_topology(expanded.clone()).unwrap();
        assert_eq!(entry.term, vote.term);
        assert_eq!(leader.committed_topology().generation, topology.generation);
        let n2 = MetadataAppendResponse {
            cluster_id: topology.cluster_id.clone(),
            term: vote.term,
            node_id: ClusterNodeId::new("n2").unwrap(),
            success: true,
            match_index: entry.index,
            conflict_index: entry.index.saturating_add(1),
            native_receiver: None,
            native_provenance: None,
        };
        assert!(!leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &n2)
            .unwrap());
        let n3 = MetadataAppendResponse {
            node_id: ClusterNodeId::new("n3").unwrap(),
            ..n2
        };
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n3").unwrap(), &n3)
            .unwrap());
        assert_eq!(leader.committed_topology(), &expanded);
        assert_eq!(leader.status().last_log_index, entry.index);
    }

    #[test]
    fn a_delayed_old_term_append_ack_cannot_commit_a_new_proposal() {
        let topology = topology_with_nodes(3);
        let root = tempfile::tempdir().unwrap();
        let mut leader = open(root.path(), "n1", &topology);
        let votes = BTreeSet::from([
            ClusterNodeId::new("n1").unwrap(),
            ClusterNodeId::new("n2").unwrap(),
        ]);
        let old_term = leader.start_election().unwrap().term;
        leader.become_leader(&votes).unwrap();
        leader.observe_remote_term(old_term + 1).unwrap();
        leader.start_election().unwrap();
        leader.become_leader(&votes).unwrap();
        let mut changed = topology.clone();
        changed.generation += 1;
        let entry = leader.propose_topology(changed).unwrap();
        let before = leader.status().commit_index;
        let response = MetadataAppendResponse {
            cluster_id: topology.cluster_id.clone(),
            term: old_term,
            node_id: ClusterNodeId::new("n2").unwrap(),
            success: true,
            match_index: entry.index,
            conflict_index: entry.index + 1,
            native_receiver: None,
            native_provenance: None,
        };
        assert!(!leader
            .record_append_response(&response.node_id, &response)
            .unwrap());
        assert_eq!(leader.status().commit_index, before);
        assert_eq!(leader.committed_topology().generation, topology.generation);
    }

    #[test]
    fn learner_promotion_requires_the_caught_up_learners_new_quorum_vote() {
        let mut topology = topology_with_nodes(3);
        let learner = ClusterNodeId::new("n4").unwrap();
        topology.nodes.insert(
            learner.clone(),
            ClusterNode::new(learner.clone(), "127.0.0.1:10004", 1, 1_000_000, 14)
                .unwrap()
                .as_metadata_learner(),
        );
        topology.generation = topology.generation.saturating_add(1);
        topology.validate().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let mut leader = open(dir.path(), "n1", &topology);
        let election = leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();

        let mut promoted = topology.clone();
        promoted.nodes.get_mut(&learner).unwrap().metadata_role =
            crate::distribution::MetadataMemberRole::Voter;
        promoted.generation = promoted.generation.saturating_add(1);
        let entry = leader.propose_topology(promoted.clone()).unwrap();
        let n2 = MetadataAppendResponse {
            cluster_id: topology.cluster_id.clone(),
            term: election.term,
            node_id: ClusterNodeId::new("n2").unwrap(),
            success: true,
            match_index: entry.index,
            conflict_index: entry.index.saturating_add(1),
            native_receiver: None,
            native_provenance: None,
        };
        assert!(!leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &n2)
            .unwrap());
        assert_eq!(leader.committed_topology(), &topology);

        let learner_response = MetadataAppendResponse {
            node_id: learner.clone(),
            ..n2
        };
        assert!(leader
            .record_append_response(&learner, &learner_response)
            .unwrap());
        assert_eq!(leader.committed_topology(), &promoted);
        assert!(leader.status().voters.contains(&learner));
        assert!(!leader.status().learners.contains(&learner));
    }

    #[test]
    fn committed_snapshot_replication_survives_leader_failover() {
        let topology = topology_with_nodes(3);
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut leader = open(leader_dir.path(), "n1", &topology);
        let mut follower = open(follower_dir.path(), "n2", &topology);
        let election = leader.start_election().unwrap();
        let votes = BTreeSet::from([
            ClusterNodeId::new("n1").unwrap(),
            ClusterNodeId::new("n2").unwrap(),
        ]);
        leader.become_leader(&votes).unwrap();

        let mut changed = topology.clone();
        changed.generation = changed.generation.saturating_add(1);
        changed
            .nodes
            .get_mut(&ClusterNodeId::new("n3").unwrap())
            .unwrap()
            .used_bytes = 99;
        let entry = leader.propose_topology(changed.clone()).unwrap();
        let append = leader.append_request_from(1, 8).unwrap();
        let response = follower.handle_append_request(append).unwrap();
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &response)
            .unwrap());
        let commit = leader
            .append_request_from(entry.index.saturating_add(1), 8)
            .unwrap();
        assert!(follower.handle_append_request(commit).unwrap().success);
        assert_eq!(follower.committed_topology(), &changed);

        drop(follower);
        let mut successor = open(follower_dir.path(), "n2", &topology);
        let next_election = successor.start_election().unwrap();
        assert!(next_election.term > election.term);
        successor
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();
        assert_eq!(successor.committed_topology(), &changed);
    }

    #[test]
    fn failover_leader_can_resume_at_a_follower_compacted_commit_boundary() {
        let topology = topology_with_nodes(3);
        let roots = (0..3)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut leader = open(roots[0].path(), "n1", &topology);
        let mut successor = open(roots[1].path(), "n2", &topology);
        let mut ahead_follower = open(roots[2].path(), "n3", &topology);
        leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();

        let mut changed = topology.clone();
        changed.generation = changed.generation.saturating_add(1);
        changed
            .nodes
            .get_mut(&ClusterNodeId::new("n3").unwrap())
            .unwrap()
            .used_bytes = 99;
        let entry = leader.propose_topology(changed.clone()).unwrap();
        let append = leader.append_request_from(1, 8).unwrap();
        let successor_response = successor.handle_append_request(append.clone()).unwrap();
        assert!(
            ahead_follower
                .handle_append_request(append)
                .unwrap()
                .success
        );
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &successor_response,)
            .unwrap());

        // Only n3 learns that the entry committed before n1 fails. N2 has the
        // identical log entry and can safely win, but its local compacted base
        // still trails n3's committed/compacted base.
        let commit = leader
            .append_request_from(entry.index.saturating_add(1), 8)
            .unwrap();
        assert!(
            ahead_follower
                .handle_append_request(commit)
                .unwrap()
                .success
        );
        assert_eq!(ahead_follower.status().commit_index, entry.index);
        assert!(successor.status().commit_index < entry.index);

        successor.start_election().unwrap();
        successor
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n2").unwrap(),
                ClusterNodeId::new("n3").unwrap(),
            ]))
            .unwrap();
        let stale_snapshot = successor.append_request_from(1, 8).unwrap();
        let conflict = ahead_follower
            .handle_append_request(stale_snapshot)
            .unwrap();
        assert!(!conflict.success);
        assert_eq!(conflict.conflict_index, entry.index.saturating_add(1));

        let resumed = successor
            .append_request_from(conflict.conflict_index, 8)
            .unwrap();
        let response = ahead_follower.handle_append_request(resumed).unwrap();
        assert!(response.success);
        assert!(successor
            .record_append_response(&ClusterNodeId::new("n3").unwrap(), &response)
            .unwrap());
        assert_eq!(
            successor.status().commit_index,
            successor.status().last_log_index
        );
    }

    #[test]
    fn restore_activation_is_quorum_committed_snapshotted_and_restart_durable() {
        let topology = topology_with_nodes(3);
        let roots = (0..3)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut leader = open(roots[0].path(), "n1", &topology);
        let mut follower = open(roots[1].path(), "n2", &topology);
        let mut snapshot_target = open(roots[2].path(), "n3", &topology);
        let election = leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();
        let response = follower
            .handle_append_request(leader.append_request_from(1, 8).unwrap())
            .unwrap();
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &response)
            .unwrap());
        follower
            .handle_append_request(leader.append_request_from(1, 8).unwrap())
            .unwrap();

        let activation = restore_activation(&topology, 10_000);
        let entry = leader
            .propose_restore_activation(activation.clone())
            .unwrap();
        assert!(leader.committed_restore_activation().is_none());
        let response = follower
            .handle_append_request(leader.append_request_from(entry.index, 8).unwrap())
            .unwrap();
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &response)
            .unwrap());
        assert_eq!(leader.committed_restore_activation(), Some(&activation));

        follower
            .handle_append_request(leader.append_request_from(entry.index, 8).unwrap())
            .unwrap();
        assert_eq!(follower.committed_restore_activation(), Some(&activation));
        drop(follower);
        let mut follower = open(roots[1].path(), "n2", &topology);
        assert_eq!(follower.committed_restore_activation(), Some(&activation));
        assert_eq!(
            MetadataConsensusStore::inspect_restore_activation(roots[1].path()).unwrap(),
            Some(activation.clone())
        );

        let snapshot = leader.append_request_from(1, 8).unwrap();
        assert!(snapshot.snapshot.is_some());
        snapshot_target.handle_append_request(snapshot).unwrap();
        assert_eq!(
            snapshot_target.committed_restore_activation(),
            Some(&activation)
        );
        assert_eq!(snapshot_target.committed_topology(), &topology);

        let mut stale = restore_activation(&topology, activation.activated_at_ms);
        stale.activation_id = activation.activation_id;
        assert!(leader.propose_restore_activation(stale).is_err());
        let mut wrong_topology = restore_activation(&topology, 10_001);
        wrong_topology.topology_sha256 = "d".repeat(64);
        assert!(leader.propose_restore_activation(wrong_topology).is_err());
        assert_eq!(leader.status().current_term, election.term);

        let old_snapshot = leader.append_request_from(1, 8).unwrap();
        let newer = restore_activation(&topology, 10_002);
        let entry = leader.propose_restore_activation(newer.clone()).unwrap();
        let response = follower
            .handle_append_request(leader.append_request_from(entry.index, 8).unwrap())
            .unwrap();
        assert!(leader
            .record_append_response(&response.node_id, &response)
            .unwrap());
        follower
            .handle_append_request(leader.append_request_from(entry.index + 1, 8).unwrap())
            .unwrap();
        let newest_snapshot = leader.append_request_from(1, 8).unwrap();
        assert!(
            snapshot_target
                .handle_append_request(newest_snapshot.clone())
                .unwrap()
                .success
        );
        assert_eq!(snapshot_target.committed_restore_activation(), Some(&newer));
        // A genuine old snapshot is ignored and cannot undo the newer decision.
        snapshot_target.handle_append_request(old_snapshot).unwrap();
        assert_eq!(snapshot_target.committed_restore_activation(), Some(&newer));
        // The same complete decision remains an idempotent snapshot positive.
        assert!(
            snapshot_target
                .handle_append_request(newest_snapshot.clone())
                .unwrap()
                .success
        );

        // These are deliberately adversarial DTOs, not fabricated qualification
        // evidence. A forged higher index must not bypass lineage monotonicity.
        let template = newest_snapshot.snapshot.unwrap();
        let mut same_id_rewrite = newer.clone();
        same_id_rewrite.activated_at_ms += 1;
        same_id_rewrite.node_ack_set_sha256 = "d".repeat(64);
        let mut same_time_new_id = newer.clone();
        same_time_new_id.activation_id = Uuid::new_v4();
        let mut same_generation_wrong_hash = newer.clone();
        same_generation_wrong_hash.topology_sha256 = "e".repeat(64);
        let mut future = newer.clone();
        future.topology_generation += 1;
        let mut wrong_cluster = newer.clone();
        wrong_cluster.cluster_id = ClusterId::new("another-cluster").unwrap();
        for invalid in [
            None,
            Some(activation.clone()),
            Some(same_id_rewrite),
            Some(same_time_new_id),
            Some(same_generation_wrong_hash),
            Some(future),
            Some(wrong_cluster),
        ] {
            let before = snapshot_target.state.clone();
            let mut forged = template.clone();
            forged.last_included_index += 1;
            forged.restore_activation = invalid;
            assert!(snapshot_target.install_snapshot(forged).is_err());
            assert_eq!(
                snapshot_target.state, before,
                "rejected snapshot cannot mutate committed state"
            );
        }
        assert_eq!(leader.committed_restore_activation(), Some(&newer));
        assert_eq!(follower.committed_restore_activation(), Some(&newer));
    }

    #[test]
    fn restore_activation_historical_validation_does_not_weaken_new_decision_admission() {
        let topology = topology_with_nodes(3);
        let activation = restore_activation(&topology, 10_000);
        let mut successor = topology.clone();
        successor.generation += 1;
        validate_historical_activation_against_topology(
            &activation,
            &successor,
            &topology.cluster_id,
        )
        .unwrap();
        assert!(validate_activation_against_topology(
            &activation,
            &successor,
            &topology.cluster_id
        )
        .is_err());
        let new_decision = MetadataLogEntry {
            index: 2,
            term: 1,
            leadership_barrier: false,
            topology: successor.clone(),
            restore_activation: Some(activation.clone()),
        };
        assert!(
            validate_log_entry_shape(&new_decision, &topology.cluster_id).is_err(),
            "NEW decisions still need the exact topology at admission"
        );
        let mut corrupt = activation.clone();
        corrupt.topology_sha256 = "d".repeat(64);
        assert!(validate_historical_activation_against_topology(
            &corrupt,
            &topology,
            &topology.cluster_id
        )
        .is_err());
        let mut future = activation.clone();
        future.topology_generation = successor.generation + 1;
        assert!(validate_historical_activation_against_topology(
            &future,
            &successor,
            &topology.cluster_id
        )
        .is_err());
        let mut wrong_cluster = activation.clone();
        wrong_cluster.cluster_id = ClusterId::new("wrong-cluster").unwrap();
        assert!(validate_historical_activation_against_topology(
            &wrong_cluster,
            &successor,
            &topology.cluster_id
        )
        .is_err());
        let mut malformed = activation.clone();
        malformed.admission_checksum_sha256 = "bad".into();
        assert!(validate_historical_activation_against_topology(
            &malformed,
            &successor,
            &topology.cluster_id
        )
        .is_err());
        assert!(validate_committed_activation_progress(None, Some(&malformed)).is_err());
        // New-root bootstrap has no prior committed lineage to erase.
        validate_committed_activation_progress(None, Some(&activation)).unwrap();
    }

    #[test]
    fn restore_activation_historical_lineage_survives_load_commit_reopen_and_snapshot() {
        let topology = topology_with_nodes(3);
        let roots = (0..3)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let open_durable = |index: usize, name: &str| {
            MetadataConsensusStore::open(
                roots[index].path(),
                topology.cluster_id.clone(),
                ClusterNodeId::new(name).unwrap(),
                topology.clone(),
                true,
            )
        };
        let mut leader = open_durable(0, "n1").unwrap();
        let mut follower = open_durable(1, "n2").unwrap();
        let mut snapshot_target = open_durable(2, "n3").unwrap();
        let election = leader.start_election().unwrap();
        let mut votes = BTreeSet::from([election.candidate_id.clone()]);
        for peer in [&mut follower, &mut snapshot_target] {
            let vote = peer.handle_vote_request(election.clone()).unwrap();
            assert!(vote.vote_granted);
            assert_eq!(vote.term, election.term);
            votes.insert(vote.voter_id);
        }
        leader.become_leader(&votes).unwrap();
        let reply = follower
            .handle_append_request(leader.append_request_from(1, 8).unwrap())
            .unwrap();
        assert!(leader
            .record_append_response(&reply.node_id, &reply)
            .unwrap());
        follower
            .handle_append_request(
                leader
                    .append_request_from(leader.status().commit_index + 1, 8)
                    .unwrap(),
            )
            .unwrap();

        let activation = restore_activation(&topology, 10_000);
        let decision = leader
            .propose_restore_activation(activation.clone())
            .unwrap();
        let reply = follower
            .handle_append_request(leader.append_request_from(decision.index, 8).unwrap())
            .unwrap();
        assert!(leader
            .record_append_response(&reply.node_id, &reply)
            .unwrap());
        follower
            .handle_append_request(leader.append_request_from(decision.index + 1, 8).unwrap())
            .unwrap();
        assert_eq!(leader.committed_restore_activation(), Some(&activation));
        assert_eq!(follower.committed_restore_activation(), Some(&activation));

        // Obtain the ordinary load-only successor through real distribution
        // operations, rather than forging a generation or catalog watermark.
        let plan_root = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: topology.cluster_id.clone(),
            node_id: election.candidate_id.clone(),
            node_address: "127.0.0.1:10001".into(),
            replication_factor: 1,
            initial_ranges: 2,
            ..DistributionConfig::default()
        };
        let mut plan =
            DistributionStore::initialize_at(plan_root.path(), config, true, 10).unwrap();
        plan.install_authoritative_topology(topology.clone())
            .unwrap();
        let range_id = topology.ranges.values().next().unwrap().id;
        assert!(plan
            .update_range_load(range_id, 4321, 17, &election.candidate_id, 10_001)
            .unwrap());
        let changed = plan.topology().clone();
        assert!(changed.generation > activation.topology_generation);
        let load_entry = leader.propose_topology(changed.clone()).unwrap();
        let reply = follower
            .handle_append_request(leader.append_request_from(load_entry.index, 8).unwrap())
            .unwrap();
        assert!(leader
            .record_append_response(&reply.node_id, &reply)
            .unwrap());
        follower
            .handle_append_request(leader.append_request_from(load_entry.index + 1, 8).unwrap())
            .unwrap();
        assert_eq!(leader.committed_topology(), &changed);
        assert_eq!(follower.committed_topology(), &changed);
        assert_eq!(leader.committed_restore_activation(), Some(&activation));
        assert_eq!(follower.committed_restore_activation(), Some(&activation));

        // Obtain a real fresh quorum response from the historical state.
        // The DTO boundary must accept retained lineage, but refuse future or
        // same-generation mismatched decisions even before row-read wiring.
        let read_request_id = Uuid::new_v4();
        let context_local = read_participant(&changed, "n1");
        let mut context = leader
            .begin_read_authority(read_request_id, Duration::from_secs(1), &context_local)
            .unwrap();
        let probe = leader
            .read_authority_probe(&context, &ClusterNodeId::new("n2").unwrap(), 1, 8)
            .unwrap();
        let response = follower.handle_read_authority_probe(probe.clone()).unwrap();
        let ack = MetadataReadAck::from_transport(
            &probe,
            &ClusterNodeId::new("n2").unwrap(),
            read_participant(&changed, "n2"),
            response,
        )
        .unwrap();
        assert!(leader
            .record_read_authority_ack(&mut context, &ack)
            .unwrap());
        let range = changed.range_by_id(range_id).unwrap();
        let request = MetadataReadAcquireRequest {
            protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            acquisition_id: Uuid::new_v4(),
            read_request_id,
            requester_node_id: range.leader.clone(),
            requester_incarnation: changed.nodes[&range.leader].incarnation,
            range_id,
            range_epoch: range.epoch,
            budget_ms: 1000,
        };
        let dto = leader
            .finish_read_authority(context)
            .unwrap()
            .into_acquisition_response(request.clone())
            .unwrap();
        assert_eq!(dto.restore_activation, Some(activation.clone()));
        dto.validate(&request).unwrap();
        let mut future_dto = dto.clone();
        future_dto
            .restore_activation
            .as_mut()
            .unwrap()
            .topology_generation = changed.generation + 1;
        assert!(future_dto.validate(&request).is_err());
        let mut mismatched_dto = dto.clone();
        let mismatched = mismatched_dto.restore_activation.as_mut().unwrap();
        mismatched.topology_generation = changed.generation;
        mismatched.topology_sha256 = "d".repeat(64);
        assert!(mismatched_dto.validate(&request).is_err());

        // This is a trusted-caller store/protocol test, not an mTLS test. The
        // actual leader creates the compacted snapshot; no state is forged.
        let snapshot = leader.append_request_from(1, 8).unwrap();
        assert!(snapshot.snapshot.is_some());
        let snapshot_result = snapshot_target.handle_append_request(snapshot);
        let snapshot_error = snapshot_result.as_ref().err().map(ToString::to_string);
        if let Ok(reply) = &snapshot_result {
            assert!(reply.success);
            assert_eq!(snapshot_target.committed_topology(), &changed);
            assert_eq!(
                snapshot_target.committed_restore_activation(),
                Some(&activation)
            );
        }
        drop(leader);
        drop(follower);
        let leader_reopen = open_durable(0, "n1");
        let follower_reopen = open_durable(1, "n2");
        let report = serde_json::json!({
            "scope": "three fsync-on stores; trusted-caller vote/Append/snapshot protocol; no TLS or power loss",
            "activation_generation": activation.topology_generation,
            "current_generation": changed.generation,
            "activation_index": decision.index,
            "load_commit_index": load_entry.index,
            "snapshot_error": snapshot_error,
            "leader_reopen_error": leader_reopen.as_ref().err().map(ToString::to_string),
            "follower_reopen_error": follower_reopen.as_ref().err().map(ToString::to_string)
        });
        eprintln!("RESTORE_LINEAGE_REPORT {report}");
        assert!(
            snapshot_result.is_ok() && leader_reopen.is_ok() && follower_reopen.is_ok(),
            "{report}"
        );
        for store in [leader_reopen.unwrap(), follower_reopen.unwrap()] {
            assert_eq!(store.committed_topology(), &changed);
            assert_eq!(store.committed_restore_activation(), Some(&activation));
        }
    }

    #[test]
    fn five_node_partition_and_process_restart_preserve_one_authority() {
        let topology = topology_with_nodes(5);
        let roots = (0..5)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut n1 = open(roots[0].path(), "n1", &topology);
        let mut n2 = open(roots[1].path(), "n2", &topology);
        let mut n3 = open(roots[2].path(), "n3", &topology);
        let mut n4 = open(roots[3].path(), "n4", &topology);
        let mut n5 = open(roots[4].path(), "n5", &topology);

        let first_election = n1.start_election().unwrap();
        let mut first_votes = BTreeSet::from([ClusterNodeId::new("n1").unwrap()]);
        for response in [
            n2.handle_vote_request(first_election.clone()).unwrap(),
            n3.handle_vote_request(first_election.clone()).unwrap(),
        ] {
            assert!(response.vote_granted);
            first_votes.insert(response.voter_id);
        }
        n1.become_leader(&first_votes).unwrap();
        for (node_id, follower) in [
            (ClusterNodeId::new("n2").unwrap(), &mut n2),
            (ClusterNodeId::new("n3").unwrap(), &mut n3),
        ] {
            let response = follower
                .handle_append_request(n1.append_request_from(1, 16).unwrap())
                .unwrap();
            n1.record_append_response(&node_id, &response).unwrap();
        }
        assert_eq!(n1.status().commit_index, 1);
        for follower in [&mut n2, &mut n3, &mut n4, &mut n5] {
            assert!(
                follower
                    .handle_append_request(n1.append_request_from(1, 16).unwrap())
                    .unwrap()
                    .success
            );
            assert_eq!(follower.committed_topology(), &topology);
        }

        // Partition the old leader with only n2. Their two acknowledgements
        // are insufficient for a five-voter majority, so this conflicting
        // generation must remain uncommitted.
        let mut minority_change = topology.clone();
        minority_change.generation = minority_change.generation.saturating_add(1);
        minority_change
            .nodes
            .get_mut(&ClusterNodeId::new("n2").unwrap())
            .unwrap()
            .used_bytes = 111;
        let minority_entry = n1.propose_topology(minority_change).unwrap();
        let minority_response = n2
            .handle_append_request(n1.append_request_from(1, 16).unwrap())
            .unwrap();
        assert!(!n1
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &minority_response)
            .unwrap());
        assert_eq!(n1.committed_topology(), &topology);
        assert_eq!(n2.committed_topology(), &topology);

        // The n3/n4/n5 majority elects a higher-term leader and first commits
        // its leadership barrier, replacing the isolated leader's conflicting
        // index before publishing a topology change.
        let majority_election = n3.start_election().unwrap();
        assert!(majority_election.term > first_election.term);
        let mut majority_votes = BTreeSet::from([ClusterNodeId::new("n3").unwrap()]);
        for response in [
            n4.handle_vote_request(majority_election.clone()).unwrap(),
            n5.handle_vote_request(majority_election.clone()).unwrap(),
        ] {
            assert!(response.vote_granted);
            majority_votes.insert(response.voter_id);
        }
        n3.become_leader(&majority_votes).unwrap();
        for (node_id, follower) in [
            (ClusterNodeId::new("n4").unwrap(), &mut n4),
            (ClusterNodeId::new("n5").unwrap(), &mut n5),
        ] {
            let response = follower
                .handle_append_request(n3.append_request_from(1, 16).unwrap())
                .unwrap();
            n3.record_append_response(&node_id, &response).unwrap();
        }
        assert_eq!(n3.committed_topology(), &topology);
        for follower in [&mut n4, &mut n5] {
            follower
                .handle_append_request(n3.append_request_from(1, 16).unwrap())
                .unwrap();
        }

        let mut majority_change = topology.clone();
        majority_change.generation = majority_change.generation.saturating_add(1);
        majority_change
            .nodes
            .get_mut(&ClusterNodeId::new("n5").unwrap())
            .unwrap()
            .used_bytes = 555;
        let majority_entry = n3.propose_topology(majority_change.clone()).unwrap();
        for (node_id, follower) in [
            (ClusterNodeId::new("n4").unwrap(), &mut n4),
            (ClusterNodeId::new("n5").unwrap(), &mut n5),
        ] {
            let response = follower
                .handle_append_request(n3.append_request_from(1, 16).unwrap())
                .unwrap();
            n3.record_append_response(&node_id, &response).unwrap();
        }
        assert!(majority_entry.index > minority_entry.index);
        assert_eq!(n3.committed_topology(), &majority_change);
        for follower in [&mut n4, &mut n5] {
            follower
                .handle_append_request(n3.append_request_from(1, 16).unwrap())
                .unwrap();
            assert_eq!(follower.committed_topology(), &majority_change);
        }

        // Healing the partition installs the majority snapshot, steps the old
        // leader down, and discards its uncommitted conflicting generation.
        for (node_id, follower) in [
            (ClusterNodeId::new("n1").unwrap(), &mut n1),
            (ClusterNodeId::new("n2").unwrap(), &mut n2),
        ] {
            let response = follower
                .handle_append_request(n3.append_request_from(1, 16).unwrap())
                .unwrap();
            n3.record_append_response(&node_id, &response).unwrap();
            assert_eq!(follower.committed_topology(), &majority_change);
        }
        assert_eq!(n1.status().role, MetadataConsensusRole::Follower);
        assert_eq!(n1.status().current_term, majority_election.term);
        let delayed_minority_ack = MetadataAppendResponse {
            cluster_id: topology.cluster_id.clone(),
            term: first_election.term,
            node_id: ClusterNodeId::new("n2").unwrap(),
            success: true,
            match_index: minority_entry.index,
            conflict_index: minority_entry.index.saturating_add(1),
            native_receiver: None,
            native_provenance: None,
        };
        assert!(!n1
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &delayed_minority_ack,)
            .unwrap());
        assert_eq!(n1.committed_topology(), &majority_change);

        // A fully restarted majority member retains the winning snapshot and
        // can subsequently win a higher-term election with two peers.
        drop(n4);
        let mut n4 = open(roots[3].path(), "n4", &topology);
        assert_eq!(n4.committed_topology(), &majority_change);
        assert_eq!(n4.status().role, MetadataConsensusRole::Follower);
        let restart_election = n4.start_election().unwrap();
        let mut restart_votes = BTreeSet::from([ClusterNodeId::new("n4").unwrap()]);
        for response in [
            n3.handle_vote_request(restart_election.clone()).unwrap(),
            n5.handle_vote_request(restart_election.clone()).unwrap(),
        ] {
            assert!(response.vote_granted);
            restart_votes.insert(response.voter_id);
        }
        n4.become_leader(&restart_votes).unwrap();
        for (node_id, follower) in [
            (ClusterNodeId::new("n3").unwrap(), &mut n3),
            (ClusterNodeId::new("n5").unwrap(), &mut n5),
        ] {
            let response = follower
                .handle_append_request(n4.append_request_from(1, 16).unwrap())
                .unwrap();
            n4.record_append_response(&node_id, &response).unwrap();
        }
        assert_eq!(n4.status().role, MetadataConsensusRole::Leader);
        assert_eq!(n4.committed_topology(), &majority_change);
    }

    #[test]
    fn corrupt_consensus_state_fails_closed() {
        let topology = topology_with_nodes(1);
        let dir = tempfile::tempdir().unwrap();
        drop(open(dir.path(), "n1", &topology));
        let path = dir.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        let mut envelope =
            serde_json::from_slice::<MetadataConsensusEnvelope>(&fs::read(&path).unwrap()).unwrap();
        let replacement = if envelope.checksum_sha256.starts_with('0') {
            "1"
        } else {
            "0"
        };
        envelope.checksum_sha256.replace_range(..1, replacement);
        fs::write(path, serde_json::to_vec_pretty(&envelope).unwrap()).unwrap();
        assert!(MetadataConsensusStore::open(
            dir.path(),
            topology.cluster_id.clone(),
            ClusterNodeId::new("n1").unwrap(),
            topology,
            false,
        )
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    }

    #[test]
    fn inactive_metadata_state_keeps_the_v1_wire_shape() {
        let topology = topology_with_nodes(1);
        let directory = tempfile::tempdir().unwrap();
        drop(open(directory.path(), "n1", &topology));
        let path = directory
            .path()
            .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        let bytes = fs::read(&path).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("restore_activation"));
        let reopened = open(directory.path(), "n1", &topology);
        assert!(reopened.committed_restore_activation().is_none());
    }
}

#[cfg(test)]
mod native_store_birth_tests;
