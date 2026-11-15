//! Operator-requested transfer of metadata leadership to one named voter.
//!
//! Nothing here is persisted and nothing here grants anything. The transfer
//! is an ordinary election that starts early: the target increments its own
//! term, asks every voter for its vote under the unchanged vote rules, and
//! needs the unchanged old/new majorities. The two volatile marks below only
//! (1) stop this leader from appending new proposals while the target is
//! asked to campaign, so that the target's log stays as current as this
//! leader's, and (2) let the target's own supervisor skip the rest of its
//! election timeout once.
//!
//! The request carries identity DATA only. The receiver compares every field
//! with its OWN persisted state; a reply is diagnostic text and is never the
//! outcome. The old leader reads the outcome from its own store, which
//! changes only through the ordinary vote/append handlers.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataLeadershipTransferRequest {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub leader_id: ClusterNodeId,
    pub target_id: ClusterNodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataLeadershipTransferResponse {
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub term: u64,
    /// The receiver will campaign. Diagnostic only: never a vote or a role.
    pub accepted: bool,
    pub refusal: Option<String>,
}

#[derive(Debug)]
struct Outgoing {
    target: ClusterNodeId,
    term: u64,
    until: Instant,
}
#[derive(Debug)]
struct Campaign {
    term: u64,
    until: Instant,
}
/// Volatile, per-process. A restart forgets both marks.
#[derive(Debug, Default)]
pub(super) struct MetadataLeadershipTransferState {
    outgoing: Option<Outgoing>,
    campaign: Option<Campaign>,
}

impl MetadataConsensusStore {
    fn outgoing_leadership_transfer(&self) -> Option<&Outgoing> {
        self.leadership_transfer.outgoing.as_ref().filter(|mark| {
            self.state.role == MetadataConsensusRole::Leader
                && mark.term == self.state.current_term
                && Instant::now() < mark.until
        })
    }

    /// Called by every proposal of this leader. The window always ends by
    /// itself: a mark of another term, of a former leader or past its deadline
    /// refuses nothing.
    pub(super) fn require_no_leadership_transfer(&self) -> Result<()> {
        match self.outgoing_leadership_transfer() {
            Some(mark) => Err(metadata_error(format!(
                "metadata leadership is being transferred to {}; proposals resume when the transfer ends",
                mark.target
            ))),
            None => Ok(()),
        }
    }

    /// Open the transfer window on the current leader. Refuses unless this
    /// node is the anchored leader and `target` is another voter of the
    /// committed AND of the pending configuration.
    pub(crate) fn begin_leadership_transfer(
        &mut self,
        target: &ClusterNodeId,
        window: Duration,
    ) -> Result<u64> {
        self.require_native_metadata_actor_current(true)?;
        if self.state.role != MetadataConsensusRole::Leader {
            return Err(metadata_error(format!(
                "leadership transfer refused: this node ({}) is not the metadata leader{}",
                self.state.node_id,
                self.state
                    .leader_id
                    .as_ref()
                    .map(|leader| format!("; the leader is {leader}"))
                    .unwrap_or_default()
            )));
        }
        if target == &self.state.node_id {
            return Err(metadata_error(format!(
                "leadership transfer refused: {target} is this node and already leads metadata"
            )));
        }
        if self.state.commit_index == 0
            || self.term_at(self.state.commit_index) != Some(self.state.current_term)
        {
            return Err(metadata_error(
                "leadership transfer refused: this leader has not committed an entry of its own term yet",
            ));
        }
        let Some(member) = self.state.committed_topology.nodes.get(target) else {
            return Err(metadata_error(format!(
                "leadership transfer refused: {target} is not a member of the committed configuration"
            )));
        };
        let quorums = self.read_quorums()?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || !quorums.old.contains_key(target)
            || !quorums.new.contains_key(target)
        {
            return Err(metadata_error(format!(
                "leadership transfer refused: {target} is not an active metadata voter (role {:?}, lifecycle {:?}); only a voter can lead metadata",
                member.metadata_role, member.lifecycle
            )));
        }
        if let Some(mark) = self.outgoing_leadership_transfer() {
            return Err(metadata_error(format!(
                "leadership transfer refused: a transfer to {} is already in progress",
                mark.target
            )));
        }
        self.leadership_transfer.outgoing = Some(Outgoing {
            target: target.clone(),
            term: self.state.current_term,
            until: Instant::now() + window,
        });
        Ok(self.state.current_term)
    }

    /// Close the window opened for `term`. Proposals resume if this node
    /// still leads.
    pub(crate) fn end_leadership_transfer(&mut self, term: u64) {
        if self
            .leadership_transfer
            .outgoing
            .as_ref()
            .is_some_and(|mark| mark.term == term)
        {
            self.leadership_transfer.outgoing = None;
        }
    }

    /// The request for the target, once nothing is uncommitted on this leader
    /// and the target acknowledged this leader's whole log. `Ok(None)` while
    /// that is not so yet. Refuses outside the window.
    pub(crate) fn leadership_transfer_request(
        &self,
        target: &ClusterNodeId,
    ) -> Result<Option<MetadataLeadershipTransferRequest>> {
        self.require_native_metadata_actor_current(true)?;
        if !self
            .outgoing_leadership_transfer()
            .is_some_and(|mark| &mark.target == target)
        {
            return Err(metadata_error(
                "leadership transfer window is closed: this node no longer leads its term or the window expired",
            ));
        }
        let last = self.last_log_index();
        if self.state.commit_index < last
            || self.match_index.get(target).copied().unwrap_or(0) < last
        {
            return Ok(None);
        }
        Ok(Some(MetadataLeadershipTransferRequest {
            cluster_id: self.state.cluster_id.clone(),
            term: self.state.current_term,
            leader_id: self.state.node_id.clone(),
            target_id: target.clone(),
            last_log_index: last,
            last_log_term: self.last_log_term(),
        }))
    }

    /// Why the target is not ready yet, as operator text.
    pub(crate) fn leadership_transfer_lag(&self, target: &ClusterNodeId) -> String {
        format!(
            "leader log index {}, committed {}, acknowledged by {target}: {}",
            self.last_log_index(),
            self.state.commit_index,
            self.match_index.get(target).copied().unwrap_or(0)
        )
    }

    /// Receiver side. `request.leader_id` is the transport-authenticated
    /// caller (checked by the listener). Accepted only when that caller is
    /// the leader this node itself follows in its current term and this
    /// node's own log ends where the request says the leader's log ends.
    /// Changes no persisted state and resets no election timer.
    pub(crate) fn handle_leadership_transfer(
        &mut self,
        request: &MetadataLeadershipTransferRequest,
        window: Duration,
    ) -> Result<MetadataLeadershipTransferResponse> {
        self.require_native_metadata_owner_current()?;
        self.ensure_cluster(&request.cluster_id)?;
        let refusal = self.leadership_transfer_refusal(request);
        if refusal.is_none() {
            self.leadership_transfer.campaign = Some(Campaign {
                term: self.state.current_term,
                until: Instant::now() + window,
            });
        }
        Ok(MetadataLeadershipTransferResponse {
            cluster_id: self.state.cluster_id.clone(),
            node_id: self.state.node_id.clone(),
            term: self.state.current_term,
            accepted: refusal.is_none(),
            refusal,
        })
    }

    fn leadership_transfer_refusal(
        &self,
        request: &MetadataLeadershipTransferRequest,
    ) -> Option<String> {
        if request.target_id != self.state.node_id {
            return Some(format!(
                "the request names {}, not this node",
                request.target_id
            ));
        }
        if request.leader_id == self.state.node_id {
            return Some("a node cannot transfer leadership to itself".into());
        }
        if request.term != self.state.current_term {
            return Some(format!(
                "the request is of term {}, this node is in term {}",
                request.term, self.state.current_term
            ));
        }
        if self.state.role != MetadataConsensusRole::Follower
            || self.state.leader_id.as_ref() != Some(&request.leader_id)
        {
            return Some(format!(
                "{} is not the leader this node follows in term {}",
                request.leader_id, self.state.current_term
            ));
        }
        if self.require_native_metadata_actor_current(true).is_err() {
            return Some("this node is not a current voting metadata actor".into());
        }
        let Ok(quorums) = self.read_quorums() else {
            return Some("this node cannot read its voter configuration".into());
        };
        for (id, what) in [
            (&self.state.node_id, "this node"),
            (&request.leader_id, "the requesting leader"),
        ] {
            if !quorums.old.contains_key(id) || !quorums.new.contains_key(id) {
                return Some(format!("{what} is not a metadata voter here"));
            }
        }
        if request.last_log_index != self.last_log_index()
            || request.last_log_term != self.last_log_term()
        {
            return Some(format!(
                "this node's log ends at index {} term {}, the leader's at index {} term {}",
                self.last_log_index(),
                self.last_log_term(),
                request.last_log_index,
                request.last_log_term
            ));
        }
        None
    }

    /// The supervisor may start its next election without waiting for the
    /// rest of its timeout. True only in the term of the accepted request
    /// and only while this node is still a follower.
    pub(super) fn leadership_campaign_requested(&self) -> bool {
        self.leadership_transfer
            .campaign
            .as_ref()
            .is_some_and(|mark| {
                mark.term == self.state.current_term
                    && self.state.role == MetadataConsensusRole::Follower
                    && Instant::now() < mark.until
            })
    }
}
