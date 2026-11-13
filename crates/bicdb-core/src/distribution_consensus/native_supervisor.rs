//! Actual owned metadata scheduling seam. This never manufactures read authority
//! from role/status or accepts a caller's declared installed log position.
use super::*;

pub(crate) struct NativeMetadataSupervisorSnapshot {
    pub(crate) status: MetadataConsensusStatus,
    pub(crate) contact: u64,
    pub(crate) topology: ClusterTopology,
    pub(crate) election_voters: BTreeSet<ClusterNodeId>,
    pub(crate) anchored: bool,
    pub(crate) voting_actor: bool,
}
impl MetadataConsensusStore {
    pub(crate) fn native_supervisor_snapshot(
        &self,
        actual: &crate::distribution_data::ClusterServiceOpenGuard,
        store_id: Option<Uuid>,
    ) -> Result<(Uuid, NativeMetadataSupervisorSnapshot)> {
        self.require_listener_database(actual)?;
        // A retired/replaced actor may still publish restrictive durable C;
        // actual voting eligibility is checked separately before ANY action.
        self.require_native_metadata_owner_current()?;
        let id = self.state.native_store_id.ok_or_else(|| {
            metadata_error("native supervisor requires protected Registered metadata StoreUUID")
        })?;
        if store_id.is_some_and(|expected| expected != id) {
            return Err(metadata_error(
                "native supervisor metadata StoreUUID changed",
            ));
        }
        let quorums = self.read_quorums()?;
        let proposed = self
            .state
            .log
            .last()
            .map(|entry| &entry.topology)
            .unwrap_or(&self.state.committed_topology);
        for (topology, voters) in [
            (&self.state.committed_topology, &quorums.old),
            (proposed, &quorums.new),
        ] {
            for id in voters.keys() {
                let member = topology
                    .nodes
                    .get(id)
                    .ok_or_else(|| metadata_error("native election voter missing"))?;
                if !member
                    .native_store_birth_v1
                    .as_ref()
                    .is_some_and(|birth| birth.writer_enforcement_version() == 1)
                    || member.tls_certificate_sha256.is_none()
                    || !member
                        .address
                        .parse::<std::net::SocketAddr>()
                        .is_ok_and(|address| address.port() != 0)
                {
                    return Err(metadata_error("native supervisor requires birth-bound voters with pinned numeric TLS endpoints"));
                }
            }
        }
        let election_voters = quorums
            .old
            .keys()
            .chain(quorums.new.keys())
            .cloned()
            .collect();
        Ok((
            id,
            NativeMetadataSupervisorSnapshot {
                status: self.status(),
                contact: self.last_leader_contact_ms,
                topology: self.state.committed_topology.clone(),
                election_voters,
                voting_actor: self.require_native_metadata_actor_current(true).is_ok(),
                anchored: self.state.current_term > 0
                    && self.state.commit_index > 0
                    && self.term_at(self.state.commit_index) == Some(self.state.current_term),
            },
        ))
    }

    /// Only the private native scheduler calls this after collecting actual
    /// pinned-TLS responses for this persisted candidacy. The public legacy
    /// `become_leader` boundary is not widened or reused as a proof constructor.
    pub(crate) fn finish_native_supervisor_election(
        &mut self,
        actual: &crate::distribution_data::ClusterServiceOpenGuard,
        store_id: Uuid,
        request: &MetadataVoteRequest,
        votes: &BTreeSet<ClusterNodeId>,
    ) -> Result<()> {
        self.native_supervisor_snapshot(actual, Some(store_id))?;
        if self.state.role != MetadataConsensusRole::Candidate
            || self.state.current_term != request.term
            || self.state.node_id != request.candidate_id
            || self.state.cluster_id != request.cluster_id
            || self.state.voted_for.as_ref() != Some(&request.candidate_id)
            || self.last_log_index() != request.last_log_index
            || self.last_log_term() != request.last_log_term
        {
            return Err(metadata_error(
                "owned metadata candidacy changed during actual voting",
            ));
        }
        let quorums = self.read_quorums()?;
        let old = quorums.old.keys().cloned().collect();
        let new = quorums.new.keys().cloned().collect();
        // Each actual TLS vote used committed-C's expected receiver identity.
        // Reusing a node ID is not proof that the pending-new incarnation,
        // pins or native birth voted. A differing identity cannot contribute
        // to new-C's majority merely by passing numeric set arithmetic.
        let proposed = self
            .state
            .log
            .last()
            .map(|entry| &entry.topology)
            .unwrap_or(&self.state.committed_topology);
        let new_votes = votes
            .iter()
            .filter(|id| {
                match (
                    self.state.committed_topology.nodes.get(*id),
                    proposed.nodes.get(*id),
                ) {
                    (Some(old), Some(new)) => {
                        old.incarnation == new.incarnation
                            && old.tls_certificate_sha256 == new.tls_certificate_sha256
                            && old.pending_tls_certificate_sha256
                                == new.pending_tls_certificate_sha256
                            && old.native_store_birth_v1 == new.native_store_birth_v1
                    }
                    _ => false,
                }
            })
            .cloned()
            .collect();
        if !has_quorum(votes, &old) || !has_quorum(&new_votes, &new) {
            return Err(metadata_error(
                "native election lacks actual old/new voter majorities",
            ));
        }
        self.become_leader(votes)
    }
}
