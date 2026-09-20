use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::error::{BicDbError, Result};
use crate::replication::CommitFrame;

pub fn consensus_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Consensus(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusPeer {
    pub node_id: String,
    pub address: String,
    #[serde(default)]
    pub voting: bool,
}

impl ConsensusPeer {
    pub fn voting(node_id: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            address: address.into(),
            voting: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusConfig {
    pub enabled: bool,
    pub cluster_id: String,
    pub node_id: String,
    pub peers: Vec<ConsensusPeer>,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub lease_timeout_ms: u64,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cluster_id: "default".to_string(),
            node_id: String::new(),
            peers: Vec::new(),
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 250,
            lease_timeout_ms: 2_000,
        }
    }
}

impl ConsensusConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.cluster_id.trim().is_empty() {
            return Err(consensus_error("consensus.cluster_id must not be empty"));
        }
        if self.node_id.trim().is_empty() {
            return Err(consensus_error("consensus.node_id must not be empty"));
        }
        if self.election_timeout_ms == 0 || self.heartbeat_interval_ms == 0 {
            return Err(consensus_error(
                "consensus election and heartbeat timeouts must be greater than zero",
            ));
        }
        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            return Err(consensus_error(
                "consensus heartbeat_interval_ms must be less than election_timeout_ms",
            ));
        }
        let mut seen = BTreeSet::new();
        let mut has_local = false;
        for peer in &self.peers {
            if peer.node_id.trim().is_empty() {
                return Err(consensus_error("consensus peer node_id must not be empty"));
            }
            if peer.address.trim().is_empty() {
                return Err(consensus_error("consensus peer address must not be empty"));
            }
            if !seen.insert(peer.node_id.clone()) {
                return Err(consensus_error(format!(
                    "duplicate consensus peer node_id {}",
                    peer.node_id
                )));
            }
            if peer.node_id == self.node_id {
                has_local = true;
            }
        }
        if !has_local {
            return Err(consensus_error(
                "consensus peers must include the local node_id",
            ));
        }
        let voters = self.peers.iter().filter(|peer| peer.voting).count();
        if voters == 0 {
            return Err(consensus_error(
                "consensus requires at least one voting peer",
            ));
        }
        Ok(())
    }

    pub fn voter_ids(&self) -> BTreeSet<String> {
        self.peers
            .iter()
            .filter(|peer| peer.voting)
            .map(|peer| peer.node_id.clone())
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusLogEntry {
    pub index: u64,
    pub term: u64,
    pub commit_seq: u64,
    pub frame: CommitFrame,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusState {
    pub cluster_id: String,
    pub node_id: String,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub role: ConsensusRole,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log: Vec<ConsensusLogEntry>,
    pub voters: BTreeSet<String>,
    pub match_index: BTreeMap<String, u64>,
}

impl ConsensusState {
    pub fn new(config: &ConsensusConfig) -> Self {
        let voters = config.voter_ids();
        let mut match_index = BTreeMap::new();
        for voter in &voters {
            match_index.insert(voter.clone(), 0);
        }
        Self {
            cluster_id: config.cluster_id.clone(),
            node_id: config.node_id.clone(),
            current_term: 0,
            voted_for: None,
            role: ConsensusRole::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            log: Vec::new(),
            voters,
            match_index,
        }
    }

    pub fn status(&self) -> ConsensusStatus {
        ConsensusStatus {
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            role: self.role,
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            leader_id: self.leader_id.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
            voters: self.voters.iter().cloned().collect(),
        }
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    pub fn become_candidate(&mut self) -> RequestVote {
        self.current_term = self.current_term.saturating_add(1);
        self.role = ConsensusRole::Candidate;
        self.voted_for = Some(self.node_id.clone());
        self.leader_id = None;
        RequestVote {
            cluster_id: self.cluster_id.clone(),
            term: self.current_term,
            candidate_id: self.node_id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }

    pub fn become_leader(&mut self) -> Result<()> {
        if self.role != ConsensusRole::Candidate {
            return Err(consensus_error("only a candidate can become leader"));
        }
        self.role = ConsensusRole::Leader;
        self.leader_id = Some(self.node_id.clone());
        let last = self.last_log_index();
        self.match_index.insert(self.node_id.clone(), last);
        Ok(())
    }

    pub fn handle_request_vote(&mut self, request: RequestVote) -> Result<VoteResponse> {
        self.ensure_cluster(&request.cluster_id)?;
        if request.term < self.current_term {
            return Ok(self.vote_response(false));
        }
        if request.term > self.current_term {
            self.step_down(request.term, None);
        }
        let log_is_current = request.last_log_term > self.last_log_term()
            || (request.last_log_term == self.last_log_term()
                && request.last_log_index >= self.last_log_index());
        let can_vote = self.voted_for.is_none()
            || self.voted_for.as_deref() == Some(request.candidate_id.as_str());
        let granted = can_vote && log_is_current && self.voters.contains(&request.candidate_id);
        if granted {
            self.voted_for = Some(request.candidate_id);
        }
        Ok(self.vote_response(granted))
    }

    pub fn handle_append_entries(&mut self, request: AppendEntries) -> Result<AppendResponse> {
        self.ensure_cluster(&request.cluster_id)?;
        if request.term < self.current_term {
            return Ok(self.append_response(false));
        }
        if request.term >= self.current_term {
            self.step_down(request.term, Some(request.leader_id.clone()));
        }
        if request.prev_log_index > 0 {
            let Some(prev) = self
                .log
                .iter()
                .find(|entry| entry.index == request.prev_log_index)
            else {
                return Ok(self.append_response(false));
            };
            if prev.term != request.prev_log_term {
                self.truncate_from(request.prev_log_index);
                return Ok(self.append_response(false));
            }
        }
        for incoming in request.entries {
            if let Some(existing) = self.log.iter().find(|entry| entry.index == incoming.index) {
                if existing.term != incoming.term {
                    self.truncate_from(incoming.index);
                    self.log.push(incoming);
                }
            } else {
                if incoming.index != self.last_log_index().saturating_add(1) {
                    return Ok(self.append_response(false));
                }
                self.log.push(incoming);
            }
        }
        self.commit_index = self
            .commit_index
            .max(request.leader_commit.min(self.last_log_index()));
        Ok(self.append_response(true))
    }

    pub fn append_local_commit(&mut self, frame: CommitFrame) -> Result<ConsensusLogEntry> {
        if self.role != ConsensusRole::Leader {
            return Err(consensus_error(
                "only the consensus leader can append writes",
            ));
        }
        let entry = ConsensusLogEntry {
            index: self.last_log_index().saturating_add(1),
            term: self.current_term,
            commit_seq: frame.commit_seq,
            frame,
        };
        self.log.push(entry.clone());
        self.match_index.insert(self.node_id.clone(), entry.index);
        Ok(entry)
    }

    pub fn record_append_response(
        &mut self,
        node_id: &str,
        response: &AppendResponse,
    ) -> Result<u64> {
        self.ensure_cluster(&response.cluster_id)?;
        if response.term > self.current_term {
            self.step_down(response.term, None);
            return Ok(self.commit_index);
        }
        if self.role != ConsensusRole::Leader || !response.success {
            return Ok(self.commit_index);
        }
        if !self.voters.contains(node_id) {
            return Err(consensus_error(format!(
                "append response from non-voting node {node_id}"
            )));
        }
        self.match_index
            .insert(node_id.to_string(), response.match_index);
        self.recompute_commit_index();
        Ok(self.commit_index)
    }

    pub fn apply_committed_prefix(&mut self) -> Vec<CommitFrame> {
        let mut frames = Vec::new();
        while self.last_applied < self.commit_index {
            let next = self.last_applied.saturating_add(1);
            if let Some(entry) = self.log.iter().find(|entry| entry.index == next) {
                frames.push(entry.frame.clone());
                self.last_applied = next;
            } else {
                break;
            }
        }
        frames
    }

    pub fn append_entries_from(&self, next_index: u64) -> AppendEntries {
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.log
                .iter()
                .find(|entry| entry.index == prev_log_index)
                .map(|entry| entry.term)
                .unwrap_or(0)
        };
        AppendEntries {
            cluster_id: self.cluster_id.clone(),
            term: self.current_term,
            leader_id: self.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries: self
                .log
                .iter()
                .filter(|entry| entry.index >= next_index)
                .cloned()
                .collect(),
            leader_commit: self.commit_index,
        }
    }

    fn recompute_commit_index(&mut self) {
        let mut matches = self
            .voters
            .iter()
            .map(|voter| self.match_index.get(voter).copied().unwrap_or(0))
            .collect::<Vec<_>>();
        matches.sort_unstable();
        let majority_index = matches[matches.len() / 2];
        if majority_index <= self.commit_index {
            return;
        }
        let Some(entry) = self.log.iter().find(|entry| entry.index == majority_index) else {
            return;
        };
        if entry.term == self.current_term {
            self.commit_index = majority_index;
        }
    }

    fn truncate_from(&mut self, index: u64) {
        self.log.retain(|entry| entry.index < index);
        self.commit_index = self.commit_index.min(self.last_log_index());
        self.last_applied = self.last_applied.min(self.commit_index);
    }

    fn step_down(&mut self, term: u64, leader_id: Option<String>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = ConsensusRole::Follower;
        self.leader_id = leader_id;
    }

    fn vote_response(&self, vote_granted: bool) -> VoteResponse {
        VoteResponse {
            cluster_id: self.cluster_id.clone(),
            term: self.current_term,
            voter_id: self.node_id.clone(),
            vote_granted,
        }
    }

    fn append_response(&self, success: bool) -> AppendResponse {
        AppendResponse {
            cluster_id: self.cluster_id.clone(),
            term: self.current_term,
            node_id: self.node_id.clone(),
            success,
            match_index: if success { self.last_log_index() } else { 0 },
        }
    }

    fn ensure_cluster(&self, cluster_id: &str) -> Result<()> {
        if cluster_id != self.cluster_id {
            return Err(consensus_error(format!(
                "consensus cluster_id mismatch: expected {}, got {}",
                self.cluster_id, cluster_id
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusStatus {
    pub cluster_id: String,
    pub node_id: String,
    pub role: ConsensusRole,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub voters: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestVote {
    pub cluster_id: String,
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteResponse {
    pub cluster_id: String,
    pub term: u64,
    pub voter_id: String,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntries {
    pub cluster_id: String,
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<ConsensusLogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendResponse {
    pub cluster_id: String,
    pub term: u64,
    pub node_id: String,
    pub success: bool,
    pub match_index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{CommitFrame, ReplicationOperationType, ReplicationWrite};

    fn config(node_id: &str) -> ConsensusConfig {
        ConsensusConfig {
            enabled: true,
            cluster_id: "c1".to_string(),
            node_id: node_id.to_string(),
            peers: vec![
                ConsensusPeer::voting("n1", "127.0.0.1:1"),
                ConsensusPeer::voting("n2", "127.0.0.1:2"),
                ConsensusPeer::voting("n3", "127.0.0.1:3"),
            ],
            ..ConsensusConfig::default()
        }
    }

    fn frame(seq: u64) -> CommitFrame {
        CommitFrame::new(
            "c1",
            "n1",
            "default",
            seq,
            seq,
            123,
            vec![ReplicationWrite {
                collection: "items".to_string(),
                record_id: format!("r{seq}"),
                operation: ReplicationOperationType::Upsert,
                payload: b"{}".to_vec(),
                schema_version: 0,
                collection_meta: None,
            }],
        )
    }

    #[test]
    fn vote_requires_current_log_and_single_vote_per_term() {
        let mut node = ConsensusState::new(&config("n2"));
        let request = RequestVote {
            cluster_id: "c1".to_string(),
            term: 1,
            candidate_id: "n1".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(node.handle_request_vote(request).unwrap().vote_granted);
        let second = RequestVote {
            cluster_id: "c1".to_string(),
            term: 1,
            candidate_id: "n3".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(!node.handle_request_vote(second).unwrap().vote_granted);
    }

    #[test]
    fn append_entries_rejects_prev_log_mismatch() {
        let mut node = ConsensusState::new(&config("n2"));
        let request = AppendEntries {
            cluster_id: "c1".to_string(),
            term: 1,
            leader_id: "n1".to_string(),
            prev_log_index: 9,
            prev_log_term: 1,
            entries: Vec::new(),
            leader_commit: 0,
        };
        assert!(!node.handle_append_entries(request).unwrap().success);
    }

    #[test]
    fn leader_commits_after_majority_match() {
        let mut leader = ConsensusState::new(&config("n1"));
        leader.become_candidate();
        leader.become_leader().unwrap();
        let entry = leader.append_local_commit(frame(1)).unwrap();
        let response = AppendResponse {
            cluster_id: "c1".to_string(),
            term: leader.current_term,
            node_id: "n2".to_string(),
            success: true,
            match_index: entry.index,
        };
        assert_eq!(leader.record_append_response("n2", &response).unwrap(), 1);
        let frames = leader.apply_committed_prefix();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].commit_seq, 1);
    }
}
