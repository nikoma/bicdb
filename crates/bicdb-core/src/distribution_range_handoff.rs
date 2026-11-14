//! Durable data-plane promises for a relocation handoff.
//!
//! A decoded request is not authority. The RPC/service boundary must bind it to
//! fresh committed metadata, authenticated members and the exclusive range gate.
//! These promises never expire: time passing cannot make an old writer safe.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RelocationId};
use crate::error::{BicDbError, Result};

mod joint;
#[cfg(feature = "tls-replication")]
pub(crate) use joint::NativeJointReadOnlyPlanIdentity;
pub(crate) use joint::{
    CommittedNativeJointDecisionObservation, NativeJointActivationAdmission,
    NativeJointDecisionData, NativeJointDecisionPublicationAdmission,
    NativeJointDecisionPublicationRound, NativeJointDurableDecision, NativeJointHandoffPlan,
    NativeJointHandoffRuntime, NativeJointMaterializedFacts, NativeJointMaterializedQuorums,
    NativeJointNewObservation, NativeJointNewRound, NativeJointOldClosedQuorum,
    NativeJointOldImageProof, NativeJointOldObservation, NativeJointOldRound,
    NativeJointOpenAdmission, NativeJointOpenPhaseQuorum, NativeJointOpenPhaseRound,
    NativeJointPreparationPhaseQuorum, NativeJointPreparationPhaseRound,
    NativeJointPreparedDecision, NativeJointPreparedServingObservation,
    NativeJointPreparedServingQuorum, NativeJointPreparedServingRound,
    NativeJointPreparedServingVoteData, NativeJointPublishedServingQuorum,
    NativeJointReleaseAdmission, NativeJointReleaseContinuation, NativeJointReleasePhaseQuorum,
    NativeJointReleasePhaseRound, NativeJointServingPolicyAdmission,
    NativeJointServingPublicationRound,
};
#[cfg(test)]
pub(crate) use joint::{NativeJointPhaseTrace, NativeRefreshTraceContext};
#[cfg(feature = "tls-replication")]
pub(crate) use joint::{
    NativeRemoteOldCutCertificate, NativeRemoteOldImageProof, NativeRemoteOldRound,
};
#[cfg(feature = "tls-replication")]
pub(crate) use joint::{
    NativeRemoteOldOperationV1, NativeRemoteOldReplyV1, NativeRemoteOldRequestV1,
};

pub(crate) const HANDOFF_RANGE_LOG_FORMAT: u32 = 4;
pub(crate) const MAX_HANDOFF_PROMISES: usize = 65_536;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeHandoffPromise {
    pub session_id: Uuid,
    pub relocation_id: RelocationId,
    pub plan_id: String,
    pub range_id: RangeId,
    pub old_epoch: u64,
    /// The final post-cleanup epoch. The intermediate promoted epoch must not
    /// admit writes before the final image/quorum is established.
    pub activation_epoch: u64,
    pub old_config_sha256: String,
    pub new_config_sha256: String,
    pub nonce: Uuid,
    pub resolved_through: u64,
}

impl RangeHandoffPromise {
    pub(crate) fn validate(&self) -> Result<()> {
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit());
        if self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.range_id.get() == 0
            || self.relocation_id.get() == 0
            || self.plan_id.is_empty()
            || self.plan_id.len() > 1024
            || self.old_epoch == 0
            || self.old_epoch.checked_add(2) != Some(self.activation_epoch)
            || !digest(&self.old_config_sha256)
            || !digest(&self.new_config_sha256)
        {
            return Err(BicDbError::Cluster(
                "invalid permanent range handoff promise".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn same_request(&self, other: &Self) -> bool {
        // The local resolved cut is observed by the store, never accepted from
        // the request. Every other binding must match on an idempotent retry.
        let mut expected = other.clone();
        expected.resolved_through = self.resolved_through;
        self == &expected
    }
}

/// Non-serializable observation of a durable old-data-majority's resolved cut.
/// This alone does not prove source physical image or native prefix lineage.
/// Capture additionally requires that independent native/genesis/base proof.
/// A source DB commit sequence is deliberately not a field of this observation.
#[derive(Debug)]
pub(crate) struct CertifiedRangeImageCut {
    // Only the native OLD round can supply this live, non-serializable proof.
    native_old: Option<std::sync::Arc<NativeJointOldImageProof>>,
    #[cfg(feature = "tls-replication")]
    native_remote_old: Option<std::sync::Arc<NativeRemoteOldImageProof>>,
    promise: RangeHandoffPromise,
    cluster_id: ClusterId,
    range: RangeDescriptor,
    range_index: u64,
    source_id: ClusterNodeId,
    source_incarnation: u64,
    target_id: ClusterNodeId,
    target_incarnation: u64,
    intended_activation_range: RangeDescriptor,
    intended_activation_members:
        std::collections::BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    bootstrap_id: Option<Uuid>,
    relocation: crate::distribution::RangeRelocation,
    old_member_credentials:
        std::collections::BTreeMap<ClusterNodeId, (u64, Option<String>, Option<String>)>,
    expected_schema_sha256: Option<String>,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
}

impl CertifiedRangeImageCut {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn native_remote_old_image_proof(
        &self,
    ) -> Option<&std::sync::Arc<NativeRemoteOldImageProof>> {
        self.native_remote_old.as_ref()
    }
    pub(crate) fn native_old_image_proof(
        &self,
    ) -> Option<&std::sync::Arc<NativeJointOldImageProof>> {
        self.native_old.as_ref()
    }
    pub(crate) fn bootstrap_id(&self) -> Option<Uuid> {
        self.bootstrap_id
    }
    pub(crate) fn schema_sha256(&self) -> Option<&str> {
        self.expected_schema_sha256.as_deref()
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.restore_activation.as_ref()
    }
    pub(crate) fn capture_members(
        &self,
    ) -> std::collections::BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        self.old_member_credentials
            .iter()
            .map(|(id, (incarnation, active, pending))| {
                (
                    id.clone(),
                    crate::db::RangeImageBirthMember {
                        incarnation: *incarnation,
                        tls_certificate_sha256: active.clone(),
                        pending_tls_certificate_sha256: pending.clone(),
                    },
                )
            })
            .collect()
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.promise.session_id
    }
    pub(crate) fn relocation_id(&self) -> RelocationId {
        self.promise.relocation_id
    }
    pub(crate) fn plan_id(&self) -> &str {
        &self.promise.plan_id
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn source_id(&self) -> &ClusterNodeId {
        &self.source_id
    }
    pub(crate) fn source_incarnation(&self) -> u64 {
        self.source_incarnation
    }
    pub(crate) fn target_id(&self) -> &ClusterNodeId {
        &self.target_id
    }
    pub(crate) fn target_incarnation(&self) -> u64 {
        self.target_incarnation
    }
    pub(crate) fn old_config_sha256(&self) -> &str {
        &self.promise.old_config_sha256
    }
    pub(crate) fn new_config_sha256(&self) -> &str {
        &self.promise.new_config_sha256
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.promise.nonce
    }
    pub(crate) fn activation_epoch(&self) -> u64 {
        self.promise.activation_epoch
    }
    /// Intended configuration captured from the genuine metadata proof. This
    /// is not evidence that the configuration has been activated or installed.
    pub(crate) fn intended_activation_range(&self) -> &RangeDescriptor {
        &self.intended_activation_range
    }
    pub(crate) fn intended_activation_members(
        &self,
    ) -> &std::collections::BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.intended_activation_members
    }

    /// Validate only the configuration binding of an independently produced
    /// native lineage. This cannot manufacture native visibility or extend R.
    /// Load estimates are excluded. Placement-policy and voter credential
    /// changes require a re-attested native base in this first implementation.
    pub(crate) fn validate_source_lineage_configuration(
        &self,
        lineage: &crate::db::SourceRangeImageLineage,
    ) -> Result<()> {
        use crate::distribution::RangeReplicaRole;
        let invalid = || {
            BicDbError::Cluster(
                "source native lineage does not match the committed handoff configuration".into(),
            )
        };
        let bootstrap = self
            .bootstrap_id
            .filter(|id| !id.is_nil())
            .ok_or_else(invalid)?;
        let schema = self.expected_schema_sha256.as_deref().ok_or_else(invalid)?;
        let relocation = &self.relocation;
        let target = self
            .range
            .replicas
            .iter()
            .filter(|replica| replica.id == relocation.target_replica_id)
            .collect::<Vec<_>>();
        if lineage.cluster_id() != &self.cluster_id
            || lineage.bootstrap_id() != bootstrap
            || lineage.local_node_id() != &self.source_id
            || lineage.local_member_incarnation() != self.source_incarnation
            || lineage.range_index() != self.range_index
            || lineage.schema_sha256() != schema
            || lineage.restore_activation() != self.restore_activation.as_ref()
            || lineage.native_receipt_sha256().len() != 64
            || !lineage
                .native_receipt_sha256()
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || relocation.id != self.promise.relocation_id
            || relocation.range_id != self.range.id
            || relocation.learner_epoch != self.range.epoch
            || relocation.expected_epoch.checked_add(1) != Some(relocation.learner_epoch)
            || relocation.target != self.target_id
            || target.len() != 1
            || target[0].node_id != self.target_id
            || target[0].role != RangeReplicaRole::Learner
        {
            return Err(invalid());
        }
        let mut expected_base = self.range.clone();
        expected_base.epoch = relocation.expected_epoch;
        expected_base
            .replicas
            .retain(|replica| replica.id != relocation.target_replica_id);
        let base = lineage.base_range();
        let replicas = |range: &RangeDescriptor| {
            range
                .replicas
                .iter()
                .map(|r| (r.id, (r.node_id.clone(), r.role)))
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        if base.id != expected_base.id
            || base.start_token != expected_base.start_token
            || base.end_token != expected_base.end_token
            || base.epoch != expected_base.epoch
            || base.leader != expected_base.leader
            || base.placement != expected_base.placement
            || base.replicas.len() != expected_base.replicas.len()
            || replicas(base) != replicas(&expected_base)
        {
            return Err(invalid());
        }
        // Only the exact expected learner addition is admitted. Old voters
        // retain their native base identities and active/pending credentials.
        for (id, (incarnation, active, pending)) in &self.old_member_credentials {
            let member = lineage.base_members().get(id).ok_or_else(invalid)?;
            if member.incarnation != *incarnation
                || &member.tls_certificate_sha256 != active
                || &member.pending_tls_certificate_sha256 != pending
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

/// One authenticated response, not a deserializable certificate. The transport
/// constructor is used only after its mTLS/envelope/request checks.
#[derive(Debug)]
pub(crate) struct HandoffPromiseAck {
    node_id: ClusterNodeId,
    incarnation: u64,
    certificate_sha256: Option<String>,
    plaintext_loopback: bool,
    promise: RangeHandoffPromise,
}

impl HandoffPromiseAck {
    pub(crate) fn from_transport(
        request: &RangeHandoffPromise,
        destination: &crate::distribution::ClusterNode,
        certificate_sha256: Option<&str>,
        plaintext_loopback: bool,
        promise: RangeHandoffPromise,
    ) -> Result<Self> {
        promise.validate()?;
        let peer_ok = match destination.tls_certificate_sha256.as_deref() {
            Some(active) => {
                certificate_sha256 == Some(active)
                    || destination
                        .pending_tls_certificate_sha256
                        .as_deref()
                        .is_some_and(|pending| certificate_sha256 == Some(pending))
            }
            None => certificate_sha256.is_some() || plaintext_loopback,
        };
        if destination.incarnation == 0 || !peer_ok || !promise.same_request(request) {
            return Err(BicDbError::Cluster(
                "handoff promise peer/request mismatch".into(),
            ));
        }
        Ok(Self {
            node_id: destination.id.clone(),
            incarnation: destination.incarnation,
            certificate_sha256: certificate_sha256.map(str::to_string),
            plaintext_loopback,
            promise,
        })
    }
}

/// Neither variant is deserializable or convertible into the other. Remote
/// metadata remains a transport-verified observation, never local authority.
/// Its finite budget is not a leadership lease. Promotion/activation still
/// requires independent fresh revalidation and native source lineage.
#[derive(Debug)]
enum HandoffMetadataProof {
    Local(crate::distribution_consensus::MetadataReadAuthority),
    Transport {
        observation: crate::distribution_consensus::MetadataReadObservation,
        business_request_id: Uuid,
    },
}

impl HandoffMetadataProof {
    fn require_live(&self) -> Result<()> {
        match self {
            Self::Local(authority) if authority.is_current_budget() => Ok(()),
            Self::Local(_) => Err(BicDbError::Cluster(
                "handoff metadata authority budget expired".into(),
            )),
            Self::Transport {
                observation,
                business_request_id,
            } => observation.require_live_request(*business_request_id),
        }
    }
    fn topology(&self) -> Result<&crate::distribution::ClusterTopology> {
        self.require_live()?;
        match self {
            Self::Local(authority) => Ok(authority.topology()),
            Self::Transport {
                observation,
                business_request_id,
            } => observation.committed_topology(*business_request_id),
        }
    }
    fn restore_activation(
        &self,
    ) -> Result<Option<&crate::distribution_consensus::MetadataRestoreActivation>> {
        self.require_live()?;
        match self {
            Self::Local(authority) => Ok(authority.restore_activation()),
            Self::Transport {
                observation,
                business_request_id,
            } => observation.restore_activation(*business_request_id),
        }
    }
    fn require_range_scope(&self, range: &RangeDescriptor) -> Result<()> {
        self.require_live()?;
        if let Self::Transport { observation, .. } = self {
            let request = observation.request();
            if request.range_id != range.id || request.range_epoch != range.epoch {
                return Err(BicDbError::Cluster(
                    "handoff observation belongs to another range or epoch".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Bounded one-shot collection of actual old-data-voter promises. A metadata
/// quorum cannot substitute for this separate data quorum.
#[derive(Debug)]
pub(crate) struct HandoffCutRound {
    authority: HandoffMetadataProof,
    request: RangeHandoffPromise,
    range: RangeDescriptor,
    intended_activation_range: RangeDescriptor,
    old_voters: std::collections::BTreeMap<ClusterNodeId, u64>,
    acknowledgements: std::collections::BTreeMap<ClusterNodeId, HandoffPromiseAck>,
}

impl HandoffCutRound {
    pub(crate) fn begin(
        authority: crate::distribution_consensus::MetadataReadAuthority,
        relocation_id: RelocationId,
    ) -> Result<Self> {
        Self::begin_with_proof(HandoffMetadataProof::Local(authority), relocation_id)
    }

    /// Consume the actual transport observation for this business request.
    /// A decoded DTO cannot enter this path or mint a local authority.
    pub(crate) fn begin_from_observation(
        observation: crate::distribution_consensus::MetadataReadObservation,
        business_request_id: Uuid,
        relocation_id: RelocationId,
    ) -> Result<Self> {
        Self::begin_with_proof(
            HandoffMetadataProof::Transport {
                observation,
                business_request_id,
            },
            relocation_id,
        )
    }

    fn begin_with_proof(
        authority: HandoffMetadataProof,
        relocation_id: RelocationId,
    ) -> Result<Self> {
        use crate::distribution::RelocationPhase;
        let topology = authority.topology()?;
        let restore_activation = authority.restore_activation()?;
        let relocation = topology
            .relocations
            .get(&relocation_id)
            .ok_or_else(|| BicDbError::Cluster("handoff plan is not committed".into()))?;
        let range = topology
            .range_by_id(relocation.range_id)
            .ok_or_else(|| BicDbError::Cluster("committed handoff range is absent".into()))?
            .clone();
        if !matches!(
            relocation.phase,
            RelocationPhase::LearnerAllocated
                | RelocationPhase::SnapshotCopying
                | RelocationPhase::CatchingUp
                | RelocationPhase::ReadyToPromote
        ) || range.epoch != relocation.learner_epoch
        {
            return Err(BicDbError::Cluster(
                "handoff requires the exact committed pre-promotion plan".into(),
            ));
        }
        authority.require_range_scope(&range)?;
        let (request, new_range, old_voters) = pending_handoff_configuration(
            topology,
            relocation_id,
            relocation,
            &range,
            restore_activation,
        )?;
        authority.require_live()?;
        Ok(Self {
            authority,
            request,
            range,
            intended_activation_range: new_range,
            old_voters,
            acknowledgements: Default::default(),
        })
    }

    pub(crate) fn request(&self) -> &RangeHandoffPromise {
        &self.request
    }

    pub(crate) fn record(&mut self, ack: HandoffPromiseAck) -> Result<()> {
        self.authority.require_live()?;
        let peer = self
            .authority
            .topology()?
            .nodes
            .get(&ack.node_id)
            .ok_or_else(|| {
                BicDbError::Cluster("handoff responder absent from current authority".into())
            })?;
        let pin_ok = match peer.tls_certificate_sha256.as_deref() {
            Some(active) => {
                ack.certificate_sha256.as_deref() == Some(active)
                    || peer
                        .pending_tls_certificate_sha256
                        .as_deref()
                        .is_some_and(|pending| ack.certificate_sha256.as_deref() == Some(pending))
            }
            None => ack.certificate_sha256.is_some() || ack.plaintext_loopback,
        };
        if self.old_voters.get(&ack.node_id) != Some(&ack.incarnation)
            || !ack.promise.same_request(&self.request)
            || !pin_ok
        {
            return Err(BicDbError::Cluster(
                "handoff observation is not from this old data configuration".into(),
            ));
        }
        if let Some(previous) = self.acknowledgements.get(&ack.node_id) {
            if previous.promise != ack.promise {
                return Err(BicDbError::Cluster(
                    "handoff voter changed its promised cut".into(),
                ));
            }
        }
        self.authority.require_live()?;
        self.acknowledgements.insert(ack.node_id.clone(), ack);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<CertifiedRangeImageCut> {
        self.authority.require_live()?;
        if self.acknowledgements.len() < self.old_voters.len() / 2 + 1 {
            return Err(BicDbError::Cluster(
                "handoff has no durable old data majority".into(),
            ));
        }
        // Every acknowledged old write intersects the fenced old majority.
        // Each promised voter has fully resolved its tail; the greatest actual
        // applied cut therefore contains every intersecting acknowledged prefix.
        // Prefer the current leader among equally advanced actual sources.
        let best = self
            .acknowledgements
            .values()
            .max_by_key(|ack| {
                (
                    ack.promise.resolved_through,
                    ack.node_id == self.range.leader,
                    ack.node_id.clone(),
                )
            })
            .expect("nonempty promised majority");
        let topology = self.authority.topology()?;
        let relocation = &topology.relocations[&self.request.relocation_id];
        let target = &topology.nodes[&relocation.target];
        self.authority.require_live()?;
        let expected_schema_sha256 = self
            .range
            .required_schema_sha256(&topology.nodes)
            .map(str::to_string);
        let intended_activation_members = self
            .intended_activation_range
            .replicas
            .iter()
            .filter(|replica| replica.role == crate::distribution::RangeReplicaRole::Voter)
            .map(|replica| {
                let node = &topology.nodes[&replica.node_id];
                (
                    node.id.clone(),
                    crate::db::RangeImageBirthMember {
                        incarnation: node.incarnation,
                        tls_certificate_sha256: node.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: node.pending_tls_certificate_sha256.clone(),
                    },
                )
            })
            .collect();
        Ok(CertifiedRangeImageCut {
            native_old: None,
            #[cfg(feature = "tls-replication")]
            native_remote_old: None,
            promise: self.request,
            cluster_id: topology.cluster_id.clone(),
            range: self.range,
            range_index: best.promise.resolved_through,
            source_id: best.node_id.clone(),
            source_incarnation: best.incarnation,
            target_id: target.id.clone(),
            target_incarnation: target.incarnation,
            intended_activation_range: self.intended_activation_range,
            intended_activation_members,
            bootstrap_id: topology.range_image_bootstrap_id,
            relocation: relocation.clone(),
            old_member_credentials: self
                .old_voters
                .keys()
                .map(|id| {
                    let member = &topology.nodes[id];
                    (
                        id.clone(),
                        (
                            member.incarnation,
                            member.tls_certificate_sha256.clone(),
                            member.pending_tls_certificate_sha256.clone(),
                        ),
                    )
                })
                .collect(),
            expected_schema_sha256,
            restore_activation: self.authority.restore_activation()?.cloned(),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::distribution::*;
    use crate::distribution_consensus::{
        MetadataConsensusStore, MetadataReadAck, MetadataReadAuthority, MetadataReadParticipant,
    };
    use crate::distribution_range_consensus::RangeWriteStore;
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::time::Duration;

    fn topology() -> ClusterTopology {
        topology_with_ranges(1)
    }

    fn topology_with_ranges(initial_ranges: u32) -> ClusterTopology {
        topology_with_load(initial_ranges, None).0
    }

    fn topology_with_load(
        initial_ranges: u32,
        load: Option<(u64, u64)>,
    ) -> (ClusterTopology, (u64, u64)) {
        let root = tempfile::tempdir().unwrap();
        let cfg = DistributionConfig {
            enabled: true,
            initial_ranges,
            replication_factor: 1,
            cluster_id: ClusterId::new("handoff-round-test").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "127.0.0.1:10001".into(),
            ..Default::default()
        };
        let mut store = DistributionStore::initialize_at(root.path(), cfg, true, 1).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        for n in 2..=4 {
            let mut node = ClusterNode::new(
                ClusterNodeId::new(format!("n{n}")).unwrap(),
                format!("127.0.0.1:{}", 10000 + n),
                7,
                1_000_000,
                2,
            )
            .unwrap();
            if n == 2 {
                node.tls_certificate_sha256 = Some("b".repeat(64));
                node.pending_tls_certificate_sha256 = Some("c".repeat(64));
            }
            store.join_node(node, &actor, 2).unwrap();
        }
        let range = store.topology().ranges.values().next().unwrap().clone();
        let initial_load = (range.approximate_bytes, range.approximate_qps);
        if let Some((bytes, qps)) = load {
            store
                .update_range_load(range.id, bytes, qps, &actor, 3)
                .unwrap();
        }
        let plan = RebalancePlan {
            id: "stable-handoff-test-plan".into(),
            topology_generation: store.topology().generation,
            created_at_ms: 3,
            replica_moves: vec![PlannedReplicaMove {
                range_id: range.id,
                expected_epoch: range.epoch,
                source: Some(actor.clone()),
                target: ClusterNodeId::new("n4").unwrap(),
                estimated_bytes: 0,
                reason: ReplicaMoveReason::LoadBalance,
            }],
            leader_transfers: vec![],
            unplaced_ranges: vec![],
            estimated_bytes_in_flight: 0,
        };
        store.apply_rebalance_plan(&plan, &actor, 3).unwrap();
        let mut topology = store.topology().clone();
        // Explicit protocol fixture: three old data voters and a fourth learner;
        // metadata membership is independently four voters.
        let range = topology.ranges.values_mut().next().unwrap();
        for n in 2..=3 {
            range.replicas.push(RangeReplica {
                id: ReplicaId::new(topology.next_replica_id).unwrap(),
                node_id: ClusterNodeId::new(format!("n{n}")).unwrap(),
                role: RangeReplicaRole::Voter,
            });
            topology.next_replica_id += 1;
        }
        topology.replication_factor = 3;
        topology.validate().unwrap();
        (topology, initial_load)
    }

    pub(crate) fn authority(root: &Path, topology: &ClusterTopology) -> MetadataReadAuthority {
        authority_with_restore(root, topology, None, Duration::from_secs(10))
    }

    fn authority_with_restore(
        root: &Path,
        topology: &ClusterTopology,
        activation: Option<crate::MetadataRestoreActivation>,
        budget: Duration,
    ) -> MetadataReadAuthority {
        let mut stores: Vec<_> = (1..=3)
            .map(|n| {
                MetadataConsensusStore::open(
                    root.join(format!("n{n}")),
                    topology.cluster_id.clone(),
                    ClusterNodeId::new(format!("n{n}")).unwrap(),
                    topology.clone(),
                    true,
                )
                .unwrap()
            })
            .collect();
        let mut leader = stores.remove(0);
        leader.start_election().unwrap();
        leader
            .become_leader(
                &(1..=3)
                    .map(|n| ClusterNodeId::new(format!("n{n}")).unwrap())
                    .collect::<BTreeSet<_>>(),
            )
            .unwrap();
        for (n, follower) in (2..=3).zip(&mut stores) {
            let reply = follower
                .handle_append_request(leader.append_request_from(1, 64).unwrap())
                .unwrap();
            leader
                .record_append_response(&ClusterNodeId::new(format!("n{n}")).unwrap(), &reply)
                .unwrap();
        }
        if let Some(activation) = activation {
            leader.propose_restore_activation(activation).unwrap();
            for (n, follower) in (2..=3).zip(&mut stores) {
                let reply = follower
                    .handle_append_request(leader.append_request_from(1, 64).unwrap())
                    .unwrap();
                leader
                    .record_append_response(&ClusterNodeId::new(format!("n{n}")).unwrap(), &reply)
                    .unwrap();
            }
        }
        let participant = |name: &str| {
            let id = ClusterNodeId::new(name).unwrap();
            let node = &topology.nodes[&id];
            MetadataReadParticipant::from_transport(
                id,
                node.incarnation,
                node.tls_certificate_sha256.clone(),
                true,
            )
        };
        let mut context = leader
            .begin_read_authority(Uuid::new_v4(), budget, &participant("n1"))
            .unwrap();
        for (n, follower) in (2..=3).zip(&mut stores) {
            let id = ClusterNodeId::new(format!("n{n}")).unwrap();
            let probe = leader.read_authority_probe(&context, &id, 1, 64).unwrap();
            let response = follower.handle_read_authority_probe(probe.clone()).unwrap();
            let ack =
                MetadataReadAck::from_transport(&probe, &id, participant(id.as_str()), response)
                    .unwrap();
            leader
                .record_read_authority_ack(&mut context, &ack)
                .unwrap();
        }
        leader.finish_read_authority(context).unwrap()
    }

    fn activation(topology: &ClusterTopology) -> crate::MetadataRestoreActivation {
        crate::MetadataRestoreActivation {
            format_version: crate::METADATA_RESTORE_ACTIVATION_FORMAT_VERSION,
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
            activated_at_ms: 10_000,
        }
    }

    #[test]
    fn handoff_configuration_identity_binds_schema_and_restore_activation_lineage() {
        let topology = topology();
        let root = tempfile::tempdir().unwrap();
        let id = *topology.relocations.keys().next().unwrap();
        let first = activation(&topology);
        let mut second = first.clone();
        second.activation_id = Uuid::now_v7();
        let one = HandoffCutRound::begin(
            authority_with_restore(
                &root.path().join("one"),
                &topology,
                Some(first),
                Duration::from_secs(1),
            ),
            id,
        )
        .unwrap();
        let two = HandoffCutRound::begin(
            authority_with_restore(
                &root.path().join("two"),
                &topology,
                Some(second),
                Duration::from_secs(1),
            ),
            id,
        )
        .unwrap();
        assert_eq!(one.request().session_id, two.request().session_id);
        assert_ne!(
            one.request().old_config_sha256,
            two.request().old_config_sha256
        );
        assert_ne!(
            one.request().new_config_sha256,
            two.request().new_config_sha256
        );
        assert!(!one.request().same_request(two.request()));
        let mut schema_one = topology.clone();
        for node in schema_one.nodes.values_mut() {
            node.labels
                .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.into(), "a".repeat(64));
        }
        let mut schema_two = schema_one.clone();
        for node in schema_two.nodes.values_mut() {
            node.labels
                .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.into(), "b".repeat(64));
        }
        let one =
            HandoffCutRound::begin(authority(&root.path().join("schema-one"), &schema_one), id)
                .unwrap();
        let two =
            HandoffCutRound::begin(authority(&root.path().join("schema-two"), &schema_two), id)
                .unwrap();
        assert_ne!(
            one.request().old_config_sha256,
            two.request().old_config_sha256
        );
        assert_ne!(
            one.request().new_config_sha256,
            two.request().new_config_sha256
        );
    }

    #[test]
    fn handoff_local_metadata_deadline_cannot_be_renewed_at_begin_record_or_finish() {
        let topology = topology();
        let root = tempfile::tempdir().unwrap();
        let id = *topology.relocations.keys().next().unwrap();
        for stage in 0..3 {
            let authority = authority_with_restore(
                &root.path().join(format!("stage-{stage}")),
                &topology,
                None,
                Duration::from_millis(500),
            );
            let deadline = authority.deadline();
            if stage == 0 {
                std::thread::sleep(
                    deadline.saturating_duration_since(std::time::Instant::now())
                        + Duration::from_millis(1),
                );
                assert!(HandoffCutRound::begin(authority, id).is_err());
                continue;
            }
            let mut round = HandoffCutRound::begin(authority, id).unwrap();
            let make_ack = |request: &RangeHandoffPromise| {
                HandoffPromiseAck::from_transport(
                    request,
                    &topology.nodes[&ClusterNodeId::new("n1").unwrap()],
                    None,
                    true,
                    request.clone(),
                )
                .unwrap()
            };
            let ack = make_ack(round.request());
            if stage == 2 {
                round.record(ack).unwrap();
                let peer = &topology.nodes[&ClusterNodeId::new("n2").unwrap()];
                let second = HandoffPromiseAck::from_transport(
                    round.request(),
                    peer,
                    peer.tls_certificate_sha256.as_deref(),
                    false,
                    round.request().clone(),
                )
                .unwrap();
                round.record(second).unwrap();
            }
            std::thread::sleep(
                deadline.saturating_duration_since(std::time::Instant::now())
                    + Duration::from_millis(1),
            );
            if stage == 1 {
                assert!(round.record(make_ack(round.request())).is_err());
            } else {
                assert!(round.finish().is_err());
            }
        }
    }

    #[test]
    fn handoff_round_resumes_same_committed_session_after_coordinator_restart() {
        let topology = topology();
        let meta = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let id = *topology.relocations.keys().next().unwrap();
        let round = HandoffCutRound::begin(authority(meta.path(), &topology), id).unwrap();
        let request = round.request().clone();
        let mut voters = Vec::new();
        for n in 1..=2 {
            let root = data.path().join(format!("n{n}"));
            std::fs::create_dir_all(&root).unwrap();
            crate::format::enable_range_image_install(&root).unwrap();
            let mut voter = RangeWriteStore::open(
                &root,
                topology.cluster_id.clone(),
                ClusterNodeId::new(format!("n{n}")).unwrap(),
                true,
            )
            .unwrap();
            voter.install_handoff_promise(&request).unwrap();
            voters.push(voter);
        }
        drop(round); // No finish/certificate: resume through reopened metadata stores.
        let mut resumed = HandoffCutRound::begin(authority(meta.path(), &topology), id).unwrap();
        assert_eq!(resumed.request(), &request);
        for (n, voter) in (1..=2).zip(&mut voters) {
            let node = &topology.nodes[&ClusterNodeId::new(format!("n{n}")).unwrap()];
            let promised = voter.install_handoff_promise(resumed.request()).unwrap();
            resumed
                .record(
                    HandoffPromiseAck::from_transport(
                        resumed.request(),
                        node,
                        node.tls_certificate_sha256.as_deref(),
                        true,
                        promised,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let cut = resumed.finish().unwrap();
        assert_eq!(cut.session_id(), request.session_id);
        assert_eq!(cut.range_index(), 0);
    }

    #[test]
    fn handoff_round_rechecks_authority_pins_and_does_not_count_duplicates_as_quorum() {
        let topology = topology();
        let meta = tempfile::tempdir().unwrap();
        let id = *topology.relocations.keys().next().unwrap();
        let mut round = HandoffCutRound::begin(authority(meta.path(), &topology), id).unwrap();
        let node = &topology.nodes[&ClusterNodeId::new("n2").unwrap()];
        let mut obsolete = node.clone();
        obsolete.tls_certificate_sha256 = Some("a".repeat(64));
        obsolete.pending_tls_certificate_sha256 = None;
        let ack = HandoffPromiseAck::from_transport(
            round.request(),
            &obsolete,
            Some(&"a".repeat(64)),
            false,
            round.request().clone(),
        )
        .unwrap();
        assert!(
            round.record(ack).is_err(),
            "cached removed pin cannot override current authority"
        );
        for pin in ["b".repeat(64), "c".repeat(64)] {
            let ack = HandoffPromiseAck::from_transport(
                round.request(),
                node,
                Some(&pin),
                false,
                round.request().clone(),
            )
            .unwrap();
            round.record(ack).unwrap();
        }
        assert!(
            round.finish().is_err(),
            "two valid pins for one member are still only one data voter"
        );
    }
    #[test]
    fn source_lineage_requires_exact_committed_learner_transition_and_native_bindings() {
        use crate::db::{RangeImageBirthMember, RangeImageBirthSpec};
        use crate::{BicDb, CancellationToken, DbConfig, StorageMode};
        use parking_lot::RwLock;
        use std::sync::Arc;
        let root = tempfile::tempdir().unwrap();
        // Preserve actual pre-load estimates for the native base. The
        // controller commits changed load before allocating the learner.
        let (mut topology, initial_load) = topology_with_load(1, Some((8192, 137)));
        let bootstrap = Uuid::now_v7();
        topology.range_image_bootstrap_id = Some(bootstrap);
        let relocation_id = *topology.relocations.keys().next().unwrap();
        let relocation = &topology.relocations[&relocation_id];
        let mut base = topology.range_by_id(relocation.range_id).unwrap().clone();
        base.epoch = relocation.expected_epoch;
        base.approximate_bytes = initial_load.0;
        base.approximate_qps = initial_load.1;
        base.replicas
            .retain(|replica| replica.id != relocation.target_replica_id);
        let local = base.leader.clone();
        let expected = RangeImageBirthSpec {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: bootstrap,
            node_id: local.clone(),
            member_incarnation: topology.nodes[&local].incarnation,
            initial_members: topology
                .nodes
                .iter()
                .map(|(id, member)| {
                    (
                        id.clone(),
                        RangeImageBirthMember {
                            incarnation: member.incarnation,
                            tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: member
                                .pending_tls_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
            initial_ranges: vec![base.clone()],
            restore_activation: None,
        };
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let database_path = root.path().join("native-source");
        let (mut database, _) = BicDb::create_new_range_image_database(
            &database_path,
            config.clone(),
            expected.clone(),
        )
        .unwrap();
        database.create_collection("items").unwrap();
        drop(database);
        let (database, genesis) =
            BicDb::open_range_image_database(&database_path, config, &expected).unwrap();
        let schema = database.schema_compatibility_fingerprint().unwrap().sha256;
        let database = Arc::new(RwLock::new(database));
        let gate = database.read().range_mutation_gate();
        let frozen = gate.try_freeze(base.id, Uuid::now_v7()).unwrap();
        let lineage = BicDb::initialize_range_image_genesis(
            &database,
            &genesis,
            &base,
            &frozen,
            Duration::from_secs(3),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        for member in topology.nodes.values_mut() {
            member
                .labels
                .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.into(), schema.clone());
        }
        let mut round = HandoffCutRound::begin(
            authority(&root.path().join("metadata"), &topology),
            relocation_id,
        )
        .unwrap();
        for name in ["n1", "n2"] {
            let id = ClusterNodeId::new(name).unwrap();
            let peer = &topology.nodes[&id];
            let path = root.path().join(format!("resolved-{name}"));
            std::fs::create_dir_all(&path).unwrap();
            crate::format::enable_range_image_install(&path).unwrap();
            let mut store =
                RangeWriteStore::open(&path, topology.cluster_id.clone(), id, true).unwrap();
            let promised = store.install_handoff_promise(round.request()).unwrap();
            let ack = HandoffPromiseAck::from_transport(
                round.request(),
                peer,
                peer.tls_certificate_sha256.as_deref(),
                true,
                promised,
            )
            .unwrap();
            round.record(ack).unwrap();
        }
        let mut cut = round.finish().unwrap();
        assert_eq!(
            (cut.range.approximate_bytes, cut.range.approximate_qps),
            (8192, 137)
        );
        assert_ne!(
            (cut.range.approximate_bytes, cut.range.approximate_qps),
            (
                lineage.base_range().approximate_bytes,
                lineage.base_range().approximate_qps
            )
        );
        cut.validate_source_lineage_configuration(&lineage).unwrap();
        assert_eq!(lineage.base_range().epoch, cut.relocation.expected_epoch);
        assert_eq!(lineage.base_range().epoch + 1, cut.range.epoch);
        let original_bootstrap = cut.bootstrap_id;
        cut.bootstrap_id = None;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.bootstrap_id = Some(Uuid::now_v7());
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.bootstrap_id = original_bootstrap;
        cut.relocation.expected_epoch += 1;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.relocation.expected_epoch -= 1;
        cut.range.start_token += 1;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.range.start_token -= 1;
        let target = cut
            .range
            .replicas
            .iter()
            .position(|r| r.id == cut.relocation.target_replica_id)
            .unwrap();
        cut.range.replicas[target].role = RangeReplicaRole::Voter;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.range.replicas[target].role = RangeReplicaRole::Learner;
        let other = ClusterNodeId::new("n2").unwrap();
        cut.old_member_credentials.get_mut(&other).unwrap().0 += 1;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.old_member_credentials.get_mut(&other).unwrap().0 -= 1;
        let pin = cut.old_member_credentials.get_mut(&other).unwrap().2.take();
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.old_member_credentials.get_mut(&other).unwrap().2 = pin;
        let original_schema = cut.expected_schema_sha256.take();
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.expected_schema_sha256 = Some("f".repeat(64));
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.expected_schema_sha256 = original_schema;
        cut.restore_activation = Some(activation(&topology));
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.restore_activation = None;
        cut.source_incarnation += 1;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.source_incarnation -= 1;
        cut.range_index = 1;
        assert!(cut.validate_source_lineage_configuration(&lineage).is_err());
        cut.range_index = 0;
        cut.validate_source_lineage_configuration(&lineage).unwrap();
        // Exercise the production capture entry too: direct validator tests
        // must not leave the actual image path unwired.
        drop(frozen);
        let runtime_frozen = Arc::new(gate.try_freeze(base.id, cut.session_id()).unwrap());
        let capture_frozen = Arc::clone(&runtime_frozen);
        let mut capture = crate::distribution_range_image::RangeImageCapture::begin(
            Arc::clone(&database),
            cut,
            capture_frozen,
            lineage,
            Duration::from_secs(3),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        assert!(
            gate.try_controlled_mutation(&runtime_frozen).is_err(),
            "Runtime's retained Arc cannot write through actual Capture's read pin"
        );
        {
            let staging = capture.authorize_target_install().unwrap();
            staging.require_current().unwrap();
            assert_eq!(staging.bootstrap_id(), bootstrap);
            assert_eq!(staging.cut().range_index(), 0);
            assert!(!staging.catalog_sha256().is_empty());
        }
        let options = crate::RangeSnapshotOptions::default();
        loop {
            let cursor = capture.cursor().clone();
            let step = capture
                .export_step(&cursor, &options, &CancellationToken::uncancelable())
                .unwrap();
            if step.complete {
                break;
            }
        }
        assert!(
            capture.completed_seal().is_err(),
            "row completion alone cannot certify an omitted logical spine"
        );
        loop {
            let cursor = capture.history_cursor().clone();
            let step = capture
                .export_history_step(&cursor, 256, &CancellationToken::uncancelable())
                .unwrap();
            if step.next_cursor.complete() {
                break;
            }
        }
        let seal = capture.completed_seal().unwrap();
        assert_eq!(seal.record_count, 0);
        assert_eq!(seal.range_index, 0);
        assert_eq!(seal.bootstrap_id, bootstrap);
        let completed = capture.complete().unwrap();
        completed.require_current().unwrap();
        assert_eq!(completed.seal(), &seal);
        assert!(gate.try_enter(base.id).is_err());
        assert!(
            gate.try_controlled_mutation(&runtime_frozen).is_err(),
            "Completed retains the actual source read pin"
        );
        drop(completed);
        assert!(
            gate.try_enter(base.id).is_err(),
            "dropping Capture must not release Runtime's outstanding recovery freeze"
        );
        let controlled = gate.try_controlled_mutation(&runtime_frozen).unwrap();
        drop(controlled);
        drop(runtime_frozen);
        assert!(gate.try_enter(base.id).is_ok());
        // This actual healthy empty-genesis path is R=0, not proof of the
        // pending installed-image or certified-suffix producer for R>0.
    }

    #[cfg(feature = "tls-replication")]
    mod remote {
        use super::*;
        use crate::{
            start_cluster_data_server_with_metadata_read_provider, BicDb, CancellationToken,
            ClusterDataNodeService, ClusterDataServerHandle, ClusterNetworkTransportConfig,
            DbConfig, MetadataReadObservation, MetadataReadProvider, ReplicationTlsConfig,
            TcpClusterRelocationTransport,
        };
        use parking_lot::{Mutex, RwLock};
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose,
        };
        use std::collections::BTreeMap;
        use std::net::TcpListener;
        use std::path::PathBuf;
        use std::sync::Arc;
        use std::time::Instant;

        fn node(name: &str) -> ClusterNodeId {
            ClusterNodeId::new(name).unwrap()
        }
        struct Identity {
            cert: PathBuf,
            key: PathBuf,
            ca: PathBuf,
        }
        impl Identity {
            fn tls(&self) -> ReplicationTlsConfig {
                ReplicationTlsConfig {
                    cert_path: self.cert.clone(),
                    key_path: self.key.clone(),
                    ca_path: self.ca.clone(),
                    require_client_cert: true,
                    dev_localhost_plaintext: false,
                }
            }
            fn pin(&self) -> String {
                crate::replication_transport::replication_certificate_sha256(&self.cert).unwrap()
            }
        }
        fn identities(root: &Path) -> BTreeMap<ClusterNodeId, Identity> {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::default();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![
                KeyUsagePurpose::KeyCertSign,
                KeyUsagePurpose::DigitalSignature,
            ];
            let ca = params.self_signed(&key).unwrap();
            let ca_path = root.join("ca.pem");
            std::fs::write(&ca_path, ca.pem()).unwrap();
            ["n1", "n2", "n3", "n4"]
                .into_iter()
                .map(|name| {
                    let leaf_key = KeyPair::generate().unwrap();
                    let mut params =
                        CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
                            .unwrap();
                    params.is_ca = IsCa::ExplicitNoCa;
                    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
                    params.extended_key_usages = vec![
                        ExtendedKeyUsagePurpose::ServerAuth,
                        ExtendedKeyUsagePurpose::ClientAuth,
                    ];
                    let cert = params.signed_by(&leaf_key, &ca, &key).unwrap();
                    let identity = Identity {
                        cert: root.join(format!("{name}.pem")),
                        key: root.join(format!("{name}.key")),
                        ca: ca_path.clone(),
                    };
                    std::fs::write(&identity.cert, cert.pem()).unwrap();
                    std::fs::write(&identity.key, leaf_key.serialize_pem()).unwrap();
                    (node(name), identity)
                })
                .collect()
        }
        struct Fixture {
            root: tempfile::TempDir,
            _silent: TcpListener,
            servers: Vec<ClusterDataServerHandle>,
            client: TcpClusterRelocationTransport,
            topology: ClusterTopology,
        }
        impl Fixture {
            fn new() -> Self {
                let root = tempfile::tempdir().unwrap();
                let identities = identities(root.path());
                let mut topology = topology_with_ranges(2);
                let listeners = (0..4)
                    .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
                    .collect::<Vec<_>>();
                for (i, listener) in listeners.iter().enumerate() {
                    let member = topology
                        .nodes
                        .get_mut(&node(&format!("n{}", i + 1)))
                        .unwrap();
                    member.address = listener.local_addr().unwrap().to_string();
                    member.tls_certificate_sha256 = Some(identities[&member.id].pin());
                }
                for range in topology.ranges.values_mut() {
                    range.leader = node("n2");
                    if !range.replicas.iter().any(|r| r.node_id == node("n2")) {
                        range.replicas[0].node_id = node("n2");
                    }
                }
                topology.validate().unwrap();
                let mut stores = (1..=3)
                    .map(|i| {
                        MetadataConsensusStore::open(
                            root.path().join(format!("meta-{i}")),
                            topology.cluster_id.clone(),
                            node(&format!("n{i}")),
                            topology.clone(),
                            true,
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                let mut leader = stores.remove(0);
                leader.start_election().unwrap();
                leader
                    .become_leader(&(1..=3).map(|i| node(&format!("n{i}"))).collect())
                    .unwrap();
                for (i, follower) in (2..=3).zip(&mut stores) {
                    let reply = follower
                        .handle_append_request(leader.append_request_from(1, 64).unwrap())
                        .unwrap();
                    leader
                        .record_append_response(&node(&format!("n{i}")), &reply)
                        .unwrap();
                }
                stores.insert(0, leader);
                let stores = stores
                    .into_iter()
                    .map(|s| Arc::new(Mutex::new(s)))
                    .collect::<Vec<_>>();
                let shared = Arc::new(RwLock::new(topology.clone()));
                let network = |name: &str| ClusterNetworkTransportConfig {
                    connect_timeout_ms: 100,
                    io_timeout_ms: 2000,
                    max_frame_bytes: 4 * 1024 * 1024,
                    max_inbound_connections: 8,
                    dev_localhost_plaintext: false,
                    tls: Some(identities[&node(name)].tls()),
                };
                let leader_transport = Arc::new(
                    TcpClusterRelocationTransport::new(
                        topology.cluster_id.clone(),
                        node("n1"),
                        shared.clone(),
                        network("n1"),
                    )
                    .unwrap(),
                );
                let provider = Arc::new(
                    MetadataReadProvider::new(stores[0].clone(), leader_transport).unwrap(),
                );
                let client = TcpClusterRelocationTransport::new(
                    topology.cluster_id.clone(),
                    node("n2"),
                    shared.clone(),
                    network("n2"),
                )
                .unwrap();
                let mut listeners = listeners;
                let silent = listeners.pop().unwrap();
                drop(listeners);
                let mut servers = Vec::new();
                for i in 0..3 {
                    let name = format!("n{}", i + 1);
                    let db = Arc::new(RwLock::new(
                        BicDb::open_with_config(
                            root.path().join(format!("db-{name}")),
                            DbConfig::default().with_fsync(false),
                        )
                        .unwrap(),
                    ));
                    let service = Arc::new(Mutex::new(
                        ClusterDataNodeService::open(
                            topology.cluster_id.clone(),
                            node(&name),
                            root.path().join(format!("service-{name}")),
                            db,
                            false,
                        )
                        .unwrap(),
                    ));
                    servers.push(
                        start_cluster_data_server_with_metadata_read_provider(
                            &topology.nodes[&node(&name)].address,
                            network(&name),
                            shared.clone(),
                            service,
                            None,
                            Some(stores[i].clone()),
                            None,
                            if i == 0 { Some(provider.clone()) } else { None },
                        )
                        .unwrap(),
                    );
                }
                Self {
                    root,
                    _silent: silent,
                    servers,
                    client,
                    topology,
                }
            }
            fn observation(
                &self,
                range_id: RangeId,
                budget: Duration,
            ) -> (Uuid, MetadataReadObservation) {
                let request = Uuid::now_v7();
                let range = self.topology.range_by_id(range_id).unwrap();
                let observation = self
                    .client
                    .acquire_metadata_read_observation(
                        &node("n1"),
                        request,
                        range_id,
                        range.epoch,
                        budget,
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap();
                assert_eq!(observation.metadata_leader_id(), &node("n1"));
                assert_eq!(observation.request().requester_node_id, node("n2"));
                (request, observation)
            }
            fn ack(&self, request: &RangeHandoffPromise, name: &str) -> HandoffPromiseAck {
                let path = self.root.path().join(format!("promises-{name}"));
                std::fs::create_dir_all(&path).unwrap();
                crate::format::enable_range_image_install(&path).unwrap();
                let mut store = RangeWriteStore::open(
                    &path,
                    self.topology.cluster_id.clone(),
                    node(name),
                    true,
                )
                .unwrap();
                let promised = store.install_handoff_promise(request).unwrap();
                let peer = &self.topology.nodes[&node(name)];
                HandoffPromiseAck::from_transport(
                    request,
                    peer,
                    peer.tls_certificate_sha256.as_deref(),
                    false,
                    promised,
                )
                .unwrap()
            }
        }
        impl Drop for Fixture {
            fn drop(&mut self) {
                for server in self.servers.drain(..) {
                    server.join().unwrap();
                }
            }
        }

        #[test]
        fn actual_transport_observation_from_different_metadata_leader_bridges_to_old_data_cut() {
            let fixture = Fixture::new();
            let relocation = *fixture.topology.relocations.keys().next().unwrap();
            let range_id = fixture.topology.relocations[&relocation].range_id;
            let (request, observation) = fixture.observation(range_id, Duration::from_secs(1));
            let mut round =
                HandoffCutRound::begin_from_observation(observation, request, relocation).unwrap();
            let first = fixture.ack(round.request(), "n1");
            round.record(first).unwrap();
            assert!(round.acknowledgements.len() < round.old_voters.len() / 2 + 1);
            let second = fixture.ack(round.request(), "n2");
            round.record(second).unwrap();
            let cut = round.finish().unwrap();
            assert_eq!(cut.range().id, range_id);
            assert_eq!(cut.range_index(), 0);
            assert_eq!(cut.source_id(), &node("n2"));
            // This is only a resolved old-data-quorum observation. It has no
            // native source lineage and does not prove an image or strong Get.
        }
        #[test]
        fn transport_observation_rejects_wrong_uuid_other_range_and_expiration() {
            let fixture = Fixture::new();
            let relocation = *fixture.topology.relocations.keys().next().unwrap();
            let range_id = fixture.topology.relocations[&relocation].range_id;
            let (_, observation) = fixture.observation(range_id, Duration::from_secs(1));
            assert!(HandoffCutRound::begin_from_observation(
                observation,
                Uuid::now_v7(),
                relocation
            )
            .is_err());
            let other = fixture
                .topology
                .ranges
                .values()
                .find(|r| r.id != range_id)
                .unwrap()
                .id;
            let (request, observation) = fixture.observation(other, Duration::from_secs(1));
            assert!(
                HandoffCutRound::begin_from_observation(observation, request, relocation).is_err()
            );
            for stage in 0..3 {
                let (request, observation) =
                    fixture.observation(range_id, Duration::from_millis(500));
                let deadline = observation.deadline();
                if stage == 0 {
                    std::thread::sleep(
                        deadline.saturating_duration_since(Instant::now())
                            + Duration::from_millis(1),
                    );
                    assert!(HandoffCutRound::begin_from_observation(
                        observation,
                        request,
                        relocation
                    )
                    .is_err());
                    continue;
                }
                let mut round =
                    HandoffCutRound::begin_from_observation(observation, request, relocation)
                        .unwrap();
                if stage == 2 {
                    for name in ["n1", "n2"] {
                        let ack = fixture.ack(round.request(), name);
                        round.record(ack).unwrap();
                    }
                }
                std::thread::sleep(
                    deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                );
                if stage == 1 {
                    assert!(round.record(fixture.ack(round.request(), "n1")).is_err());
                } else {
                    assert!(round.finish().is_err());
                }
            }
        }
    }
}

/// Pure configuration/correlation derivation, never authority. Only the actual
/// authority/paired-role factories call it after their original scope checks.
fn pending_handoff_configuration(
    topology: &crate::ClusterTopology,
    relocation_id: RelocationId,
    relocation: &crate::distribution::RangeRelocation,
    range: &RangeDescriptor,
    restore_activation: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
) -> Result<(
    RangeHandoffPromise,
    RangeDescriptor,
    std::collections::BTreeMap<ClusterNodeId, u64>,
)> {
    use crate::distribution::RangeReplicaRole;
    use sha2::{Digest, Sha256};
    let activation_epoch = range
        .epoch
        .checked_add(2)
        .ok_or_else(|| BicDbError::Cluster("handoff epoch space exhausted".into()))?;
    let mut old_voters = std::collections::BTreeMap::new();
    let mut new_range = range.clone();
    let mut found_target = false;
    for replica in &range.replicas {
        let node = topology
            .nodes
            .get(&replica.node_id)
            .ok_or_else(|| BicDbError::Cluster("handoff replica member is absent".into()))?;
        if replica.role == RangeReplicaRole::Voter {
            if old_voters
                .insert(node.id.clone(), node.incarnation)
                .is_some()
            {
                return Err(BicDbError::Cluster("duplicate handoff data voter".into()));
            }
        }
        if replica.id == relocation.target_replica_id
            && replica.node_id == relocation.target
            && replica.role == RangeReplicaRole::Learner
        {
            found_target = true;
        }
    }
    if !found_target || old_voters.is_empty() || old_voters.len() > 64 {
        return Err(BicDbError::Cluster(
            "invalid or oversized handoff voter configuration".into(),
        ));
    }
    for replica in &mut new_range.replicas {
        if replica.id == relocation.target_replica_id {
            replica.role = RangeReplicaRole::Voter;
        }
    }
    if let Some(source) = &relocation.source {
        if !old_voters.contains_key(source) {
            return Err(BicDbError::Cluster(
                "handoff source is not an old voter".into(),
            ));
        }
        new_range
            .replicas
            .retain(|replica| &replica.node_id != source);
        if &new_range.leader == source {
            new_range.leader = relocation.target.clone();
        }
    }
    new_range.epoch = activation_epoch;
    let expected_schema_sha256 = range.required_schema_sha256(&topology.nodes);
    let config_digest = |descriptor: &RangeDescriptor| -> Result<String> {
        // Heartbeats, load estimates and placement metrics do not change
        // the data configuration. Member incarnation and pins do.
        let mut voters = std::collections::BTreeMap::new();
        for replica in &descriptor.replicas {
            if replica.role == RangeReplicaRole::Voter {
                let node = &topology.nodes[&replica.node_id];
                voters.insert(
                    node.id.clone(),
                    (
                        node.incarnation,
                        node.tls_certificate_sha256.clone(),
                        node.pending_tls_certificate_sha256.clone(),
                    ),
                );
            }
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            &topology.cluster_id,
            descriptor.id,
            descriptor.start_token,
            descriptor.end_token,
            descriptor.epoch,
            &descriptor.leader,
            voters,
            expected_schema_sha256,
            // A reused session cannot silently cross restore activation
            // lineage. This remains metadata binding, not source-image proof.
            restore_activation,
        ))?)))
    };
    let session = relocation.handoff_session.as_ref().ok_or_else(|| {
        BicDbError::Cluster("handoff session must be committed before data promises".into())
    })?;
    let request = RangeHandoffPromise {
        session_id: session.session_id,
        relocation_id,
        plan_id: relocation.plan_id.clone(),
        range_id: range.id,
        old_epoch: range.epoch,
        activation_epoch,
        old_config_sha256: config_digest(&range)?,
        new_config_sha256: config_digest(&new_range)?,
        nonce: session.nonce,
        resolved_through: 0,
    };
    request.validate()?;
    Ok((request, new_range, old_voters))
}
