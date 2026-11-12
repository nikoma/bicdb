//! Bounded initial metadata orchestration. No background election scheduler,
//! ordinary listener, range recovery, Serving or first-ACK claim is implied.
use super::*;
use crate::distribution_consensus::{MetadataConsensusRole, MetadataConsensusStore};
use crate::distribution_range_consensus::RangeWriteCoordinator;
use std::time::Instant;

/// Owns the real local hook/policy lifetime. This is not a range-ready proof.
pub(crate) struct NativeInitialWriterEnrollment {
    _coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    _guard: crate::db::InstalledNativeWriterHostGuard,
    pub(crate) metadata_context_id: Uuid,
    pub(crate) metadata_term: u64,
    pub(crate) metadata_read_index: u64,
}
impl NativeInitialConsensusLaunch {
    pub(in crate::distribution_native_host) fn lock_initial_metadata<'a>(
        &'a self,
        parent: &CancellationToken,
    ) -> Result<parking_lot::MutexGuard<'a, MetadataConsensusStore>> {
        loop {
            parent.check()?;
            if let Some(guard) = self.metadata.try_lock_for(Duration::from_millis(5)) {
                parent.check()?;
                return Ok(guard);
            }
        }
    }

    /// Publish only this actual owned store's committed configuration. The
    /// initial plan cannot authenticate a subsequently registered learner.
    /// Network work and topology publication happen after releasing metadata.
    pub(super) fn publish_initial_committed_metadata(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let actual_open = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("initial service busy before metadata publication"))?
            .listener_open_guard();
        let topology = {
            let store = self.lock_initial_metadata(parent)?;
            store.require_listener_database(&actual_open)?;
            store.committed_topology().clone()
        };
        parent.check()?;
        actual_open.require_live()?;
        self.transport.install_topology(topology)?;
        parent.check()
    }

    /// An explicit initial candidate, using actual remote votes. Caller policy
    /// decides when to attempt an election; configured node IDs are not votes.
    pub(crate) fn elect_initial_metadata(&self, parent: &CancellationToken) -> Result<u64> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let (request, peers) = {
            let mut store = self.lock_initial_metadata(&token)?;
            let peers = store
                .committed_topology()
                .nodes
                .values()
                .filter(|m| {
                    m.lifecycle == ClusterNodeLifecycle::Active
                        && m.metadata_role == MetadataMemberRole::Voter
                        && m.id != self.seed.spec.node_id
                })
                .map(|m| m.id.clone())
                .collect::<Vec<_>>();
            (store.start_election()?, peers)
        };
        let mut votes = BTreeSet::from([self.seed.spec.node_id.clone()]);
        for peer in peers {
            token.check()?;
            // No service/DB/metadata latch is held during network work.
            let reply = match self.transport.request_metadata_vote_cancellable(
                &peer,
                request.clone(),
                &token,
            ) {
                Ok(reply) => reply,
                Err(_) => {
                    // Durable acceptance of C is historical. An election
                    // requires a current quorum, not all members still live.
                    token.check()?;
                    continue;
                }
            };
            if reply.term > request.term {
                return Err(invalid("initial election observed a newer metadata term"));
            }
            if reply.vote_granted && reply.term == request.term {
                votes.insert(reply.voter_id);
            }
        }
        let barrier = {
            let mut store = self.lock_initial_metadata(&token)?;
            if store.status().current_term != request.term {
                return Err(invalid(
                    "initial election term changed during remote voting",
                ));
            }
            store.become_leader(&votes)?;
            store.status().last_log_index
        };
        self.heartbeat_initial_metadata(&token)?;
        let status = self.lock_initial_metadata(&token)?.status();
        if status.role != MetadataConsensusRole::Leader || status.commit_index < barrier {
            return Err(invalid(
                "initial metadata leadership barrier lacks durable quorum",
            ));
        }
        Ok(status.commit_index)
    }

    /// A finite batch, not a scheduler. Repeating it keeps followers current;
    /// a successful transport response never substitutes for commit quorum.
    pub(crate) fn heartbeat_initial_metadata(&self, parent: &CancellationToken) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        self.publish_initial_committed_metadata(&token)?;
        for _ in 0..3 {
            let peers = {
                let store = self.lock_initial_metadata(&token)?;
                if store.status().role != MetadataConsensusRole::Leader {
                    return Err(invalid("initial heartbeat requires actual metadata leader"));
                }
                store
                    .committed_topology()
                    .nodes
                    .values()
                    .filter(|m| {
                        m.lifecycle == ClusterNodeLifecycle::Active
                            && m.id != self.seed.spec.node_id
                    })
                    .map(|m| m.id.clone())
                    .collect::<Vec<_>>()
            };
            for peer in peers {
                let request = {
                    let store = self.lock_initial_metadata(&token)?;
                    if store.status().role != MetadataConsensusRole::Leader {
                        return Err(invalid("initial metadata leader changed during heartbeat"));
                    }
                    // An earlier peer's accepted ACK can commit retirement
                    // while this round still owns its old list of peer IDs.
                    // Check current membership under the SAME guard as request
                    // construction; all other builder refusals remain errors.
                    if !store
                        .committed_topology()
                        .nodes
                        .get(&peer)
                        .is_some_and(|member| member.lifecycle == ClusterNodeLifecycle::Active)
                    {
                        continue;
                    }
                    store.append_request_for_peer(&peer, 16)?
                };
                let reply = match self
                    .transport
                    .append_metadata_cancellable(&peer, request, &token)
                {
                    Ok(reply) => reply,
                    Err(_) => {
                        token.check()?;
                        continue;
                    }
                };
                self.lock_initial_metadata(&token)?
                    .record_append_response(&peer, &reply)?;
                self.publish_initial_committed_metadata(&token)?;
            }
        }
        if self.lock_initial_metadata(&token)?.status().role != MetadataConsensusRole::Leader {
            return Err(invalid("initial metadata leader changed during heartbeat"));
        }
        self.publish_initial_committed_metadata(&token)?;
        token.check()
    }

    /// Fresh HostMQ is data about current membership, not range recovery.
    /// Keep the returned owner alive; no ordinary-data endpoint is released.
    pub(crate) fn enroll_initial_writer(
        &self,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeInitialWriterEnrollment> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let business = Uuid::now_v7();
        let observed = self.transport.acquire_host_metadata_observation(
            metadata_leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let topology = observed.committed_topology(business)?.clone();
        self.seed.require_initial_observer_identity(&topology)?;
        self.transport.install_topology(topology)?;
        let policy = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("initial writer service busy"))?;
            let db = self
                .seed
                .database
                .try_read()
                .ok_or_else(|| invalid("initial writer DB busy"))?;
            service.prepare_native_host_writer_enrollment(&db, observed, business)?
        };
        let context = policy.metadata_context_id();
        let term = policy.metadata_term();
        let read_index = policy.metadata_read_index();
        let coordinator = Arc::new(RangeWriteCoordinator::new_schema_fenced(
            self.seed.spec.cluster_id.clone(),
            self.seed.spec.node_id.clone(),
            Arc::clone(&self.topology),
            Arc::clone(&self.service),
            Arc::clone(&self.transport),
        )?);
        // Network work finished before the local actual-hook publication.
        let db = self
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("initial writer DB busy before policy"))?;
        let guard = db.install_enrolled_native_coordinator(Arc::clone(&coordinator), policy)?;
        token.check()?;
        Ok(NativeInitialWriterEnrollment {
            _coordinator: coordinator,
            _guard: guard,
            metadata_context_id: context,
            metadata_term: term,
            metadata_read_index: read_index,
        })
    }
}
