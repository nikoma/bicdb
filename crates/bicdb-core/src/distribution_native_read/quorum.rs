//! Factual, explicitly UNADMITTED candidate. Missing durable host/birth
//! enrollment is an open gate: this type cannot authorize reads or absence.
use super::*;
use crate::distribution::{ClusterNodeLifecycle, SCHEMA_COMPATIBILITY_NODE_LABEL};
use crate::distribution_consensus::MetadataReadObservation;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn scope_from_metadata(
    metadata: &MetadataReadObservation,
    business: Uuid,
    local: &ClusterNodeId,
) -> Result<NativeRangeReadScope> {
    metadata.require_live_request(business)?;
    let request = metadata.request();
    let topology = metadata.committed_topology(business)?;
    let range = topology
        .ranges
        .values()
        .find(|r| r.id == request.range_id)
        .ok_or_else(|| invalid("fresh metadata omitted requested range"))?;
    if request.requester_node_id != *local
        || range.leader != *local
        || range.epoch != request.range_epoch
    {
        return Err(invalid(
            "read candidate requires actual fresh metadata for this current range leader",
        ));
    }
    let mut members = BTreeMap::new();
    let mut schema = None;
    for replica in range
        .replicas
        .iter()
        .filter(|r| r.role == RangeReplicaRole::Voter)
    {
        let node = topology
            .nodes
            .get(&replica.node_id)
            .ok_or_else(|| invalid("fresh voter missing node identity"))?;
        let digest = node
            .labels
            .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
            .ok_or_else(|| invalid("fresh voter schema unavailable"))?;
        if node.lifecycle == ClusterNodeLifecycle::Decommissioned
            || node.incarnation == 0
            || schema.as_ref().is_some_and(|s| s != digest)
            || (replica.node_id == *local && node.incarnation != request.requester_incarnation)
        {
            return Err(invalid("fresh voter domain/schema differs"));
        }
        schema = Some(digest.clone());
        if members
            .insert(
                replica.node_id.clone(),
                RecoveryMemberIdentity {
                    incarnation: node.incarnation,
                    active_certificate_sha256: node.tls_certificate_sha256.clone(),
                    pending_certificate_sha256: node.pending_tls_certificate_sha256.clone(),
                },
            )
            .is_some()
        {
            return Err(invalid("duplicate fresh voter"));
        }
    }
    let scope = NativeRangeReadScope {
        cluster_id: topology.cluster_id.clone(),
        bootstrap_id: topology
            .range_image_bootstrap_id
            .ok_or_else(|| invalid("fresh metadata has no native bootstrap domain"))?,
        range: canonical_range(range),
        members,
        schema_sha256: schema.ok_or_else(|| invalid("fresh schema absent"))?,
        restore_activation: metadata.restore_activation(business)?.cloned(),
    };
    scope.validate()?;
    Ok(scope)
}

#[derive(Debug)]
pub(crate) struct NativeReadQuorumRound {
    pub(super) metadata: MetadataReadObservation,
    pub(super) request: NativeRangeReadProbeRequest,
    pub(super) business: Uuid,
    pub(super) local_node: ClusterNodeId,
    pub(super) cancellation: CancellationToken,
    pub(super) observations: BTreeMap<ClusterNodeId, NativeRangeReadObservation>,
    // Expired attempts never vote. Their authenticated immutable points still
    // constrain the verdict, so retry cannot erase known higher/conflict data.
    expired_constraints: Vec<NativeRangeReadFacts>,
    poisoned: bool,
}
impl NativeReadQuorumRound {
    pub(crate) fn begin(
        metadata: MetadataReadObservation,
        business: Uuid,
        challenge: Uuid,
        local: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let scope = scope_from_metadata(&metadata, business, local)?;
        let cancellation = parent.child_with_deadline(metadata.deadline());
        cancellation.check()?;
        let remaining = cancellation
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or_else(|| invalid("read candidate needs original finite deadline"))?;
        let request = NativeRangeReadProbeRequest {
            business_request_id: business,
            requested_metadata_context_id: metadata.context_id(),
            observation: NativeRangeReadRequest {
                protocol_version: NATIVE_RANGE_READ_VERSION,
                scope,
                challenge,
                budget_ms: u64::try_from(remaining.as_millis())
                    .unwrap_or(5000)
                    .clamp(1, 5000),
            },
        };
        request.cancellation(&cancellation)?;
        Ok(Self {
            metadata,
            request,
            business,
            local_node: local.clone(),
            cancellation,
            observations: BTreeMap::new(),
            expired_constraints: Vec::new(),
            poisoned: false,
        })
    }
    pub(crate) fn request(&self) -> &NativeRangeReadProbeRequest {
        &self.request
    }
    pub(crate) fn has_matching_majority(&self) -> Result<bool> {
        self.require_live()?;
        self.require_no_known_conflict()?;
        let Some(local) = self.observations.get(&self.local_node) else {
            return Ok(false);
        };
        local.require_live()?;
        Ok(self
            .observations
            .values()
            .filter(|o| o.facts().range_index == local.facts().range_index)
            .count()
            >= self.request.scope.members.len() / 2 + 1)
    }
    pub(crate) fn record(&mut self, observation: NativeRangeReadObservation) -> Result<()> {
        let result = match observation.require_live() {
            Ok(()) => self.record_inner(observation),
            Err(error) => Err(error),
        };
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    /// Consumes attempt freshness exactly once. A successful observation is
    /// recorded as a point fact under the ORIGINAL round lifetime; an expired
    /// remote attempt never votes and may be retried without erasing its point.
    pub(crate) fn record_attempt(
        &mut self,
        observation: NativeRangeReadObservation,
    ) -> Result<bool> {
        if observation.require_live().is_err() {
            self.constrain_expired_attempt(&observation)?;
            return Ok(false);
        }
        let result = self.record_inner(observation);
        if result.is_err() {
            self.poisoned = true;
        }
        result.map(|_| true)
    }
    pub(crate) fn constrain_expired_attempt(
        &mut self,
        observation: &NativeRangeReadObservation,
    ) -> Result<()> {
        let result = (|| {
            self.require_live()?;
            observation.require_request(&self.request)?;
            if observation.local().is_some()
                || observation.facts().node_id == self.local_node
                || observation.require_live().is_ok()
                || self.expired_constraints.len() >= 128
            {
                return Err(invalid(
                    "invalid or unbounded expired remote attempt constraint",
                ));
            }
            self.expired_constraints.push(observation.facts().clone());
            self.require_no_known_conflict()
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn record_inner(&mut self, observation: NativeRangeReadObservation) -> Result<()> {
        self.require_live()?;
        let facts = observation.facts();
        observation.require_request(&self.request)?;
        if self.observations.contains_key(&facts.node_id)
            || (facts.node_id == self.local_node && observation.local().is_none())
            || (facts.node_id != self.local_node && observation.local().is_some())
        {
            return Err(invalid("duplicate or substituted local read observation"));
        }
        self.observations.insert(facts.node_id.clone(), observation);
        self.require_no_known_conflict()
    }
    fn require_live(&self) -> Result<()> {
        if self.poisoned {
            return Err(invalid("read candidate round is poisoned"));
        }
        self.cancellation.check()?;
        self.metadata.require_live_request(self.business)
    }
    fn require_no_known_conflict(&self) -> Result<()> {
        let Some(local) = self.observations.get(&self.local_node) else {
            return Ok(());
        };
        let point = local.facts();
        for peer in self
            .observations
            .values()
            .map(|o| o.facts())
            .chain(self.expired_constraints.iter())
        {
            if peer.logical_genesis_sha256 != point.logical_genesis_sha256
                || peer.range_index > point.range_index
                || (peer.range_index == point.range_index
                    && peer.logical_history_sha256 != point.logical_history_sha256)
            {
                return Err(invalid(
                    "known higher or conflicting native read point vetoes candidate",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn finish(
        self,
        service: &crate::ClusterDataNodeService,
    ) -> Result<NativeReadQuorumCandidate> {
        self.require_live()?;
        self.require_no_known_conflict()?;
        let local = self
            .observations
            .get(&self.local_node)
            .and_then(|o| o.local())
            .ok_or_else(|| invalid("actual same-service local read observation unavailable"))?;
        local.require_current(service)?;
        let point = local.facts();
        let mut matching = BTreeSet::new();
        for (node, observation) in &self.observations {
            let facts = observation.facts();
            if facts.range_index == point.range_index {
                matching.insert(node.clone());
            } else {
                service.require_native_read_ancestor(local, facts)?;
            }
        }
        for facts in &self.expired_constraints {
            if facts.range_index < point.range_index {
                service.require_native_read_ancestor(local, facts)?;
            }
        }
        if matching.len() < self.request.scope.members.len() / 2 + 1 {
            return Err(invalid(
                "distinct matching current-voter read majority unavailable",
            ));
        }
        local.require_current(service)?;
        self.require_live()?;
        Ok(NativeReadQuorumCandidate {
            round: self,
            matching,
        })
    }
}
/// No admission bit/constructor, no numeric snapshot-floor accessor and no
/// conversion to a serving/strong-read permit. Root host enrollment is pending.
#[derive(Debug)]
pub(crate) struct NativeReadQuorumCandidate {
    pub(super) round: NativeReadQuorumRound,
    pub(super) matching: BTreeSet<ClusterNodeId>,
}
impl NativeReadQuorumCandidate {
    /// Birth-continuity check only. This consumes the ACTUAL fresh metadata
    /// observation and sealed peer responses already held by the round. It
    /// does not create an MVCC snapshot or authorize a successful row/absence.
    pub(crate) fn check_enrolled_voter_births(self) -> Result<EnrolledNativeReadQuorum> {
        self.require_lifetime()?;
        let topology = self
            .round
            .metadata
            .committed_topology(self.round.business)?;
        if !matches!(
            topology.format_version,
            2 | crate::distribution::NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION
        ) {
            return Err(invalid("read voters have no fenced native birth registry"));
        }
        for (id, member) in &self.scope().members {
            let node = topology
                .nodes
                .get(id)
                .ok_or_else(|| invalid("enrolled read voter is absent"))?;
            let binding = node
                .native_store_birth_v1
                .as_ref()
                .ok_or_else(|| invalid("current read voter has no enrolled native birth"))?;
            binding.validate_for(topology.range_image_bootstrap_id, member.incarnation)?;
        }
        for id in &self.matching {
            let facts = self
                .round
                .observations
                .get(id)
                .ok_or_else(|| invalid("matching read voter has no actual observation"))?
                .facts();
            let binding = topology.nodes[id].native_store_birth_v1.as_ref().unwrap();
            if binding.birth_id() != facts.native_birth_id
                || binding.birth_sha256() != facts.native_birth_sha256
                || binding.node_incarnation() != facts.member_incarnation
                || binding.bootstrap_id() != self.scope().bootstrap_id
                || binding.writer_enforcement_version() != facts.native_writer_enforcement_version
            {
                return Err(invalid(
                    "actual read voter birth differs from quorum-committed enrollment",
                ));
            }
        }
        self.require_lifetime()?;
        Ok(EnrolledNativeReadQuorum { candidate: self })
    }
    pub(crate) fn scope(&self) -> &NativeRangeReadScope {
        &self.round.request.scope
    }
    pub(crate) fn challenge(&self) -> Uuid {
        self.round.request.challenge
    }
    pub(crate) fn business_request_id(&self) -> Uuid {
        self.round.business
    }
    pub(crate) fn metadata_context_id(&self) -> Uuid {
        self.round.metadata.context_id()
    }
    pub(crate) fn matching_member_ids(&self) -> &BTreeSet<ClusterNodeId> {
        &self.matching
    }
    pub(crate) fn point(&self) -> (u64, &str, &str) {
        let facts = self.round.observations[&self.round.local_node].facts();
        (
            facts.range_index,
            &facts.logical_genesis_sha256,
            &facts.logical_history_sha256,
        )
    }
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.round.require_live()?;
        self.round.observations[&self.round.local_node].require_live()
    }
}

/// Actual read quorum with enrolled physical birth continuity checked. Still
/// no row/absence permission: the actual MVCC barrier, local revalidation and
/// final fresh metadata observation remain mandatory separate producers.
#[derive(Debug)]
pub(crate) struct EnrolledNativeReadQuorum {
    pub(super) candidate: NativeReadQuorumCandidate,
}
impl EnrolledNativeReadQuorum {
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.candidate.require_lifetime()
    }
    #[cfg(test)]
    pub(crate) fn point_for_test(&self) -> (u64, &str, &str) {
        self.candidate.point()
    }
}
