//! The metadata surface of a range move over the metadata leader's OWN host
//! state: the actual owned metadata store, the owned control transport and the
//! registration controller's template. Nothing here is a second store.
//!
//! Commit. A candidate is proposed only after a fresh host metadata
//! observation proves this node the current control leader, and it is
//! replicated by the same bounded heartbeat rounds the enrollment maintenance
//! uses. `commit_topology` returns once this node's own store reports the
//! candidate committed AND every participating member acknowledged it.
//!
//! Routing of the members. No topology is ever pushed to a member. A member's
//! DATA client shares the routing table of its own metadata listener; that
//! listener publishes the committed configuration when its own store advances
//! its commit index inside the append handler, before it replies. The leader
//! therefore waits for an accepted append acknowledgement of a request that
//! carried the commit: `success`, `match_index >= entry` and
//! `leader_commit >= entry`. The acknowledgement is recorded by the owned
//! store first, which refuses a reply the pinned transport did not mint.
//!
//! An unresolved proposal. `commit_topology` can end while its entry is
//! still in the log: its wait ended, or leadership changed. That entry may
//! commit later. Its index and term are kept here, and nothing new is
//! proposed, and no caller is told what the committed configuration "is",
//! until `resolve_proposal` has read its fate from this node's own store:
//! committed-or-replaced at that index, or provably superseded. Then the
//! committed configuration is the truth for the caller.
use super::*;
use crate::distribution_native_host::joined_data_handoff::{
    JoinedHandoffMetadataHosts, NativeJoinedDataPlanningView,
};

/// Budget of one metadata observation or one append request.
const REQUEST: Duration = Duration::from_secs(5);
/// Bound of the wait for the proposed entry to commit on this leader.
const COMMIT_WAIT: Duration = Duration::from_secs(20);
/// Bound of the wait for the participating members' acknowledgement.
const ROUTE_WAIT: Duration = Duration::from_secs(10);

fn hosts_error(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range move metadata: {}", detail.into()))
}

pub(in crate::distribution_native_host) struct NativeLeaderRangeMoveHosts {
    controller: Arc<NativeMetadataRegistrationController>,
    /// A committed entry index and the participating members that have not
    /// yet acknowledged it. Kept across calls: a later call waits again.
    unsettled: Option<(u64, BTreeSet<ClusterNodeId>)>,
    /// Index and term of an entry this object proposed and did not see
    /// decided. See the module documentation.
    outstanding: Option<(u64, u64)>,
    /// Test only: the next `commit_topology` ends right after its proposal,
    /// as a commit wait that ran out would.
    #[cfg(test)]
    abandon_next_commit: bool,
    /// Test only: `resolve_proposal` reads the fate once and sends nothing,
    /// as it would while no member answers.
    #[cfg(test)]
    hold_proposal: bool,
}

impl NativeMetadataControlHost {
    /// The metadata surface of a range move coordinated by this host. It is
    /// identity and owned handles only; every method re-proves leadership.
    pub(in crate::distribution_native_host) fn range_move_hosts(
        &self,
    ) -> Result<NativeLeaderRangeMoveHosts> {
        if !self.is_running() {
            return Err(invalid("range move requires the running control listener"));
        }
        Ok(NativeLeaderRangeMoveHosts {
            controller: self
                .controller
                .clone()
                .ok_or_else(|| invalid("range move requires the owned metadata control"))?,
            unsettled: None,
            outstanding: None,
            #[cfg(test)]
            abandon_next_commit: false,
            #[cfg(test)]
            hold_proposal: false,
        })
    }
    /// Generation of the routing table this host's DATA client reads.
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_routing_generation(&self) -> u64 {
        self.backend.topology.read().generation
    }
}

impl NativeLeaderRangeMoveHosts {
    fn backend(&self) -> &NativeMetadataControlBackend {
        &self.controller.backend
    }
    pub(in crate::distribution_native_host) fn local(&self) -> ClusterNodeId {
        self.backend().node_id.clone()
    }
    /// The coordinator's own transport: every request is sent as this node.
    pub(in crate::distribution_native_host) fn transport(
        &self,
    ) -> Arc<TcpClusterRelocationTransport> {
        Arc::clone(&self.backend().transport)
    }
    /// The coordinator's OWN data service: what this node does itself (its
    /// own native decision record, its own source retirement) runs on it.
    pub(in crate::distribution_native_host) fn local_service(
        &self,
    ) -> Arc<Mutex<ClusterDataNodeService>> {
        Arc::clone(&self.backend().service)
    }
    fn store<'a>(
        &'a self,
        parent: &CancellationToken,
    ) -> Result<parking_lot::MutexGuard<'a, crate::MetadataConsensusStore>> {
        let store = self.backend().lock_metadata(parent)?;
        store.require_listener_database(&self.controller.actual_open)?;
        Ok(store)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_unsettled(&self) -> Vec<ClusterNodeId> {
        self.unsettled
            .iter()
            .flat_map(|(_, members)| members.iter().cloned())
            .collect()
    }

    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_abandon_next_commit(&mut self) {
        self.abandon_next_commit = true;
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_hold_proposal(&mut self, hold: bool) {
        self.hold_proposal = hold;
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_outstanding(&self) -> Option<(u64, u64)> {
        self.outstanding
    }

    /// Wait, bounded, until the fate of the entry recorded as outstanding is
    /// known from this node's own store. While this node leads, the wait
    /// replicates by the ordinary heartbeat rounds. An error leaves the
    /// entry recorded: it may still commit.
    pub(in crate::distribution_native_host) fn resolve_proposal(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        let Some((index, term)) = self.outstanding else {
            return Ok(());
        };
        let token = parent.child_with_deadline(Instant::now() + ROUTE_WAIT);
        let mut last = String::from("no heartbeat round finished");
        loop {
            #[cfg(test)]
            let hold = self.hold_proposal;
            #[cfg(not(test))]
            let hold = false;
            // This node's own store, read under its own short bound so that
            // the fate is still read once when the wait has just run out.
            let read = CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_secs(1));
            let fate = self.store(&read)?.proposal_fate(index, term);
            if fate != crate::distribution_consensus::MetadataProposalFate::Pending {
                self.outstanding = None;
                return Ok(());
            }
            if hold || token.check().is_err() {
                if parent.is_cancel_requested() {
                    parent.check()?;
                }
                return Err(hosts_error(format!(
                    "unresolved proposal: the configuration this node proposed at metadata index {index} (term {term}) is neither committed nor superseded yet and may still commit: {last}"
                )));
            }
            let leads = self.store(&read)?.status().role == MetadataConsensusRole::Leader;
            if leads {
                if let Err(error) = self.backend().heartbeat(&token) {
                    last = error.to_string();
                }
            } else {
                last = "this node does not lead metadata now; the entry is decided by the current leader".into();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// One append to `peer` and whether its accepted acknowledgement proves
    /// that the peer's own store committed `index`.
    fn acknowledged(
        &self,
        peer: &ClusterNodeId,
        index: u64,
        parent: &CancellationToken,
    ) -> Result<bool> {
        let request = {
            let store = self.store(parent)?;
            let status = store.status();
            if status.role != MetadataConsensusRole::Leader {
                return Err(hosts_error("this node is no longer the metadata leader"));
            }
            if status.commit_index < index {
                return Ok(false);
            }
            store.append_request_for_peer(peer, 16)?
        };
        let carried = request.leader_commit;
        let token = parent.child_with_deadline(Instant::now() + REQUEST);
        let reply = self
            .backend()
            .transport
            .append_metadata_cancellable(peer, request, &token)?;
        // Only a reply the owned store accepts counts.
        self.store(parent)?.record_append_response(peer, &reply)?;
        Ok(reply.success && reply.match_index >= index && carried >= index)
    }

    /// Wait, bounded, until every member recorded as unsettled acknowledged
    /// the committed entry. An error leaves the remaining members recorded.
    pub(in crate::distribution_native_host) fn settle_routing(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        let Some((index, mut pending)) = self.unsettled.clone() else {
            return Ok(());
        };
        let token = parent.child_with_deadline(Instant::now() + ROUTE_WAIT);
        let mut last = String::new();
        loop {
            for peer in pending.clone() {
                match self.acknowledged(&peer, index, &token) {
                    Ok(true) => {
                        pending.remove(&peer);
                    }
                    Ok(false) => {
                        last = format!("{peer} has not applied the committed configuration")
                    }
                    Err(error) => last = format!("{peer}: {error}"),
                }
            }
            if pending.is_empty() {
                self.unsettled = None;
                return Ok(());
            }
            self.unsettled = Some((index, pending.clone()));
            if token.check().is_err() {
                parent.check()?;
                return Err(hosts_error(format!(
                    "the committed configuration (metadata index {index}) is not acknowledged by {}: {last}",
                    pending
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// The members that must route by `candidate`: every remote holder of a
/// replica of a range whose descriptor the candidate changes or adds. A
/// member the candidate removes from a range is not waited for.
fn participants(
    before: &ClusterTopology,
    candidate: &ClusterTopology,
    local: &ClusterNodeId,
) -> BTreeSet<ClusterNodeId> {
    candidate
        .ranges
        .values()
        .filter(|range| before.range_by_id(range.id) != Some(*range))
        .flat_map(|range| range.replicas.iter().map(|replica| replica.node_id.clone()))
        .filter(|node| node != local)
        .collect()
}

impl JoinedHandoffMetadataHosts for NativeLeaderRangeMoveHosts {
    fn metadata_leader(&self) -> Result<ClusterNodeId> {
        let token = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(1));
        let status = self.store(&token)?.status();
        if status.role == MetadataConsensusRole::Leader {
            return Ok(status.node_id);
        }
        status
            .leader_id
            .ok_or_else(|| hosts_error("no metadata leader is known to this node"))
    }
    fn committed_topology(&self) -> Result<ClusterTopology> {
        let token = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(1));
        Ok(self.store(&token)?.committed_topology().clone())
    }
    /// The entry this node proposed is recorded as outstanding until this
    /// node's own store shows it decided; without one, a failed commit
    /// proposed nothing or was decided as another entry.
    fn commit_outcome_unknown(&self) -> bool {
        self.outstanding.is_some()
    }
    fn commit_topology(
        &mut self,
        candidate: ClusterTopology,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        // Nothing is proposed beside an entry whose fate is unknown.
        self.resolve_proposal(parent)?;
        let token = parent.child_with_deadline(Instant::now() + COMMIT_WAIT);
        let local = self.local();
        let business = Uuid::now_v7();
        let fresh = self.backend().transport.acquire_host_metadata_observation(
            &local,
            business,
            REQUEST,
            &token.child_with_deadline(Instant::now() + REQUEST),
        )?;
        let (entry, before) = {
            let mut store = self.store(&token)?;
            store.require_native_control_leader(&fresh, business)?;
            let before = store.committed_topology().clone();
            (store.propose_topology(candidate.clone())?, before)
        };
        // Recorded until this node's own store shows the entry decided.
        self.outstanding = Some((entry.index, entry.term));
        let mut last = String::from("no heartbeat round finished");
        loop {
            #[cfg(test)]
            if std::mem::take(&mut self.abandon_next_commit) {
                return Err(hosts_error(format!(
                    "the proposed configuration (metadata index {}) did not commit in time: test: the commit wait was abandoned",
                    entry.index
                )));
            }
            // Transient refusals (a busy owner, an unreachable member) are
            // repeated; the commit itself is read from the owned store below.
            if let Err(error) = self.backend().heartbeat(&token) {
                last = error.to_string();
            }
            {
                let (status, proposed) = {
                    let store = self.store(&token)?;
                    (store.status(), store.committed_topology() == &candidate)
                };
                if status.commit_index >= entry.index {
                    // Decided, as this entry or as another one.
                    self.outstanding = None;
                    if !proposed {
                        return Err(hosts_error(
                            "the committed configuration is not the proposed one",
                        ));
                    }
                    break;
                }
                if status.role != MetadataConsensusRole::Leader || status.current_term != entry.term
                {
                    return Err(hosts_error(
                        "metadata leadership changed before the proposed configuration committed",
                    ));
                }
            }
            if token.check().is_err() {
                parent.check()?;
                return Err(hosts_error(format!(
                    "the proposed configuration (metadata index {}) did not commit in time: {last}",
                    entry.index
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // This leader's own DATA client: its own committed read, verified
        // equal to the candidate above.
        self.backend()
            .transport
            .install_topology(candidate.clone())?;
        let mut members = participants(&before, &candidate, &local);
        if let Some((_, earlier)) = self.unsettled.take() {
            // An acknowledgement of the newer entry covers the older one, for
            // the members that are still part of the committed configuration.
            members.extend(
                earlier
                    .into_iter()
                    .filter(|node| candidate.nodes.contains_key(node)),
            );
        }
        self.unsettled = Some((entry.index, members));
        self.settle_routing(parent)
    }
    fn planning_view(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<NativeJoinedDataPlanningView> {
        parent.check()?;
        let local = self.local();
        let business = Uuid::now_v7();
        let fresh = self.backend().transport.acquire_host_metadata_observation(
            &local,
            business,
            REQUEST,
            &parent.child_with_deadline(Instant::now() + REQUEST),
        )?;
        let topology = {
            let store = self.store(parent)?;
            store.require_native_control_leader(&fresh, business)?;
            store.committed_topology().clone()
        };
        if &topology != fresh.committed_topology(business)? {
            return Err(hosts_error(
                "planning snapshot is not the fresh committed configuration",
            ));
        }
        // Planning data only; never a metadata store and never published.
        let view = crate::DistributionStore::native_registration_plan(
            self.controller.config.clone(),
            topology,
        )?;
        Ok(NativeJoinedDataPlanningView {
            view,
            fresh,
            business,
        })
    }
}
