//! Durable data-plane promises for a relocation handoff.
//!
//! A decoded request is not authority. The RPC/service boundary must bind it to
//! fresh committed metadata, authenticated members and the exclusive range gate.
//! These promises never expire: time passing cannot make an old writer safe.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RelocationId};
use crate::error::{BicDbError, Result};

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

/// Non-serializable proof issued only after fresh durable old-data-quorum
/// promises and source recovery have established the exact range-command cut.
/// A source DB commit sequence is deliberately not a field of this proof.
#[derive(Debug)]
pub(crate) struct CertifiedRangeImageCut {
    promise: RangeHandoffPromise,
    cluster_id: ClusterId,
    range: RangeDescriptor,
    range_index: u64,
    source_id: ClusterNodeId,
    source_incarnation: u64,
    target_id: ClusterNodeId,
    target_incarnation: u64,
}

impl CertifiedRangeImageCut {
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
}

/// One authenticated response, not a deserializable certificate. The transport
/// constructor is used only after its mTLS/envelope/request checks.
#[derive(Debug)]
pub(crate) struct HandoffPromiseAck {
    node_id: ClusterNodeId,
    incarnation: u64,
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
            promise,
        })
    }
}

/// Bounded one-shot collection of actual old-data-voter promises. A metadata
/// quorum cannot substitute for this separate data quorum.
#[derive(Debug)]
pub(crate) struct HandoffCutRound {
    authority: crate::distribution_consensus::MetadataReadAuthority,
    request: RangeHandoffPromise,
    range: RangeDescriptor,
    old_voters: std::collections::BTreeMap<ClusterNodeId, u64>,
    acknowledgements: std::collections::BTreeMap<ClusterNodeId, HandoffPromiseAck>,
}

impl HandoffCutRound {
    pub(crate) fn begin(
        authority: crate::distribution_consensus::MetadataReadAuthority,
        relocation_id: RelocationId,
    ) -> Result<Self> {
        use crate::distribution::{RangeReplicaRole, RelocationPhase};
        use sha2::{Digest, Sha256};
        let topology = authority.topology();
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
            ))?)))
        };
        let request = RangeHandoffPromise {
            session_id: Uuid::new_v4(),
            relocation_id,
            plan_id: relocation.plan_id.clone(),
            range_id: range.id,
            old_epoch: range.epoch,
            activation_epoch,
            old_config_sha256: config_digest(&range)?,
            new_config_sha256: config_digest(&new_range)?,
            nonce: Uuid::new_v4(),
            resolved_through: 0,
        };
        request.validate()?;
        Ok(Self {
            authority,
            request,
            range,
            old_voters,
            acknowledgements: Default::default(),
        })
    }

    pub(crate) fn request(&self) -> &RangeHandoffPromise {
        &self.request
    }

    pub(crate) fn record(&mut self, ack: HandoffPromiseAck) -> Result<()> {
        if self.old_voters.get(&ack.node_id) != Some(&ack.incarnation)
            || !ack.promise.same_request(&self.request)
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
        self.acknowledgements.insert(ack.node_id.clone(), ack);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<CertifiedRangeImageCut> {
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
        let topology = self.authority.topology();
        let relocation = &topology.relocations[&self.request.relocation_id];
        let target = &topology.nodes[&relocation.target];
        Ok(CertifiedRangeImageCut {
            promise: self.request,
            cluster_id: topology.cluster_id.clone(),
            range: self.range,
            range_index: best.promise.resolved_through,
            source_id: best.node_id.clone(),
            source_incarnation: best.incarnation,
            target_id: target.id.clone(),
            target_incarnation: target.incarnation,
        })
    }
}
