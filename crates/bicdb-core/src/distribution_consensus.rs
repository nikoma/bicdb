//! Durable quorum state for cluster topology metadata.
//!
//! This is deliberately separate from BicDB's data-WAL consensus state. Range
//! ownership must be agreed before it is published, while row replication is
//! driven by the resulting range catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology};
use crate::error::{BicDbError, Result};

pub const CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION: u32 = 1;
pub const METADATA_RESTORE_ACTIVATION_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE: &str = "cluster-metadata-consensus.json";
const MAX_RESTORE_ACTIVATION_ACKNOWLEDGEMENTS: u64 = 100_000;

fn metadata_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("metadata consensus: {}", message.into()))
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataAppendResponse {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub node_id: ClusterNodeId,
    pub success: bool,
    pub match_index: u64,
    pub conflict_index: u64,
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
}

impl MetadataConsensusStore {
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
        let granted = candidate_is_voter && log_is_current && may_vote;
        if granted {
            self.state.voted_for = Some(request.candidate_id);
            self.mark_election_activity();
        }
        self.persist()?;
        Ok(self.vote_response(granted))
    }

    pub fn become_leader(&mut self, votes: &BTreeSet<ClusterNodeId>) -> Result<()> {
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
        if self.state.role != MetadataConsensusRole::Leader {
            return Err(metadata_error("only the leader can propose topology"));
        }
        topology.validate()?;
        self.ensure_cluster(&topology.cluster_id)?;
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
        self.step_down(request.term, Some(request.leader_id));
        self.mark_election_activity();
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
        for incoming in request.entries {
            validate_log_entry_shape(&incoming, &self.state.cluster_id)?;
            if incoming.index <= self.state.base_index {
                continue;
            }
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
        }
        self.advance_follower_commit(request.leader_commit)?;
        let match_index = self.last_log_index();
        self.persist()?;
        Ok(MetadataAppendResponse {
            cluster_id: self.state.cluster_id.clone(),
            term: self.state.current_term,
            node_id: self.state.node_id.clone(),
            success: true,
            match_index,
            conflict_index: match_index.saturating_add(1),
        })
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
        if self.state.role != MetadataConsensusRole::Leader || !response.success {
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
        self.match_index
            .insert(expected_node_id.clone(), response.match_index);
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
        validate_log_entry_shape(&entry, &self.state.cluster_id)?;
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
        self.reset_match_indexes();
        Ok(())
    }

    fn install_snapshot(&mut self, snapshot: MetadataSnapshot) -> Result<()> {
        snapshot.topology.validate()?;
        self.ensure_cluster(&snapshot.topology.cluster_id)?;
        if let Some(activation) = &snapshot.restore_activation {
            validate_activation_against_topology(
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
        self.state.base_index = snapshot.last_included_index;
        self.state.base_term = snapshot.last_included_term;
        self.state.commit_index = snapshot.last_included_index;
        self.state.committed_topology = snapshot.topology;
        self.state.committed_restore_activation = snapshot.restore_activation;
        self.state
            .log
            .retain(|entry| entry.index > snapshot.last_included_index);
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
        }
    }

    fn persist(&self) -> Result<()> {
        fs::create_dir_all(&self.root)?;
        let mut envelope = MetadataConsensusEnvelope {
            format_version: CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION,
            checksum_sha256: String::new(),
            state: self.state.clone(),
        };
        envelope.checksum_sha256 = state_checksum(&envelope.state)?;
        crate::storage::write_atomic(
            &self.root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )
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
    if envelope.format_version != CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION {
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
    if state.format_version != CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION {
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
        validate_activation_against_topology(
            activation,
            &state.committed_topology,
            &state.cluster_id,
        )?;
    }
    let mut expected = state.base_index.saturating_add(1);
    let mut latest_generation = state.committed_topology.generation;
    let mut latest_topology = state.committed_topology.clone();
    let mut latest_activation_ms = state
        .committed_restore_activation
        .as_ref()
        .map(|activation| activation.activated_at_ms)
        .unwrap_or(0);
    for entry in &state.log {
        validate_log_entry_shape(entry, &state.cluster_id)?;
        if entry.index != expected {
            return Err(metadata_error("metadata log indexes are not contiguous"));
        }
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
        let follower = open(roots[1].path(), "n2", &topology);
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
