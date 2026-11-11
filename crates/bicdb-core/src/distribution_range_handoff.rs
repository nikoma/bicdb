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

/// Non-serializable observation of a durable old-data-majority's resolved cut.
/// This alone does not prove source physical image or native prefix lineage.
/// Capture additionally requires that independent native/genesis/base proof.
/// A source DB commit sequence is deliberately not a field of this observation.
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
        if !authority.is_current_budget() {
            return Err(BicDbError::Cluster(
                "handoff metadata authority budget expired".into(),
            ));
        }
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
        if !self.authority.is_current_budget() {
            return Err(BicDbError::Cluster(
                "handoff metadata authority budget expired".into(),
            ));
        }
        let peer = self
            .authority
            .topology()
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
        self.acknowledgements.insert(ack.node_id.clone(), ack);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<CertifiedRangeImageCut> {
        if !self.authority.is_current_budget() {
            return Err(BicDbError::Cluster(
                "handoff metadata authority budget expired".into(),
            ));
        }
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

#[cfg(test)]
mod tests {
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
        let root = tempfile::tempdir().unwrap();
        let cfg = DistributionConfig {
            enabled: true,
            initial_ranges: 1,
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
        topology
    }

    fn authority(root: &Path, topology: &ClusterTopology) -> MetadataReadAuthority {
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
            .begin_read_authority(Uuid::new_v4(), Duration::from_secs(10), &participant("n1"))
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
}
