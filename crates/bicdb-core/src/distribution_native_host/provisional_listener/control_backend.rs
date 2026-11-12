//! Sealed actual metadata-control backend. An initial launch and an existing
//! protected Registered member are distinct constructors; no InitialLaunch is
//! fabricated for a promoted learner. Both retain the same actual store/service.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataConsensusRole, MetadataConsensusStore,
    NativeOwnedMetadataMemberObservation,
};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use std::time::Instant;

pub(in crate::distribution_native_host) struct NativeMetadataControlBackend {
    pub(super) node_id: ClusterNodeId,
    pub(super) metadata: Arc<Mutex<MetadataConsensusStore>>,
    pub(super) service: Arc<Mutex<ClusterDataNodeService>>,
    pub(super) topology: Arc<RwLock<ClusterTopology>>,
    pub(super) transport: Arc<TcpClusterRelocationTransport>,
    pub(super) provider: Arc<MetadataReadProvider>,
    registered_store: Option<Uuid>,
    _initial: Option<Arc<NativeInitialConsensusLaunch>>,
}
impl NativeMetadataControlBackend {
    pub(super) fn registered_replica_store_id(&self) -> Result<Uuid> {
        if self._initial.is_some() {
            return Err(invalid(
                "registered replica receiver cannot fabricate an INITIAL owner",
            ));
        }
        self.registered_store
            .ok_or_else(|| invalid("actual Registered metadata owner required"))
    }
    pub(super) fn initial_data_launch(&self) -> Result<Arc<NativeInitialConsensusLaunch>> {
        self._initial.clone().ok_or_else(|| invalid(
            "DATA bootstrap requires actual INITIAL owner; assigned-image/joint learner activation is separate"))
    }

    pub(super) fn borrow_initial(initial: &NativeInitialConsensusLaunch) -> Self {
        Self {
            node_id: initial.seed.spec.node_id.clone(),
            metadata: Arc::clone(&initial.metadata),
            service: Arc::clone(&initial.service),
            topology: Arc::clone(&initial.topology),
            transport: Arc::clone(&initial.transport),
            provider: Arc::clone(&initial.provider),
            registered_store: None,
            _initial: None,
        }
    }
    pub(super) fn from_initial(initial: Arc<NativeInitialConsensusLaunch>) -> Self {
        let mut backend = Self::borrow_initial(&initial);
        backend._initial = Some(initial);
        backend
    }
    pub(in crate::distribution_native_host) fn from_registered_member(
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        topology: Arc<RwLock<ClusterTopology>>,
        transport: Arc<TcpClusterRelocationTransport>,
        provider: Arc<MetadataReadProvider>,
        proof: NativeOwnedMetadataMemberObservation,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        observed.require_live_request(business)?;
        let current = observed.committed_topology(business)?;
        let node_id = observed.requester_node_id().clone();
        let member = current
            .nodes
            .get(&node_id)
            .ok_or_else(|| invalid("registered control member absent"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || member.metadata_role != MetadataMemberRole::Voter
            || member.incarnation != observed.requester_incarnation()
            || proof.topology() != current
        {
            return Err(invalid(
                "registered control requires actual current owned metadata voter",
            ));
        }
        for participant in [
            provider.local_participant(),
            transport.local_metadata_read_participant(),
        ] {
            if participant.node_id() != &node_id
                || participant.member_incarnation() != member.incarnation
                || participant.plaintext_loopback()
                || !participant
                    .certificate_sha256()
                    .is_some_and(|pin| member.accepts_tls_certificate_sha256(pin))
            {
                return Err(invalid(
                    "registered control provider/transport actual identity differs",
                ));
            }
        }
        let (database, open) = {
            let service = service
                .try_lock()
                .ok_or_else(|| invalid("registered control service busy"))?;
            (service.database(), service.listener_open_guard())
        };
        {
            let db = database
                .try_read()
                .ok_or_else(|| invalid("registered control actual DB busy"))?;
            open.require_database(&db)?;
            proof.require_database(&db)?;
        }
        {
            let store = metadata
                .try_lock()
                .ok_or_else(|| invalid("registered control store busy"))?;
            store.require_listener_database(&open)?;
            proof.require_current(&store)?;
            if store.status().node_id != node_id {
                return Err(invalid("registered control local actor differs"));
            }
        }
        provider.bind_listener_database(&metadata, &open)?;
        transport.install_topology(current.clone())?;
        observed.require_live_request(business)?;
        parent.check()?;
        Ok(Self {
            node_id,
            metadata,
            service,
            topology,
            transport,
            provider,
            registered_store: Some(proof.store_id()),
            _initial: None,
        })
    }
    pub(super) fn lock_metadata<'a>(
        &'a self,
        parent: &CancellationToken,
    ) -> Result<parking_lot::MutexGuard<'a, MetadataConsensusStore>> {
        loop {
            parent.check()?;
            if let Some(store) = self.metadata.try_lock_for(Duration::from_millis(5)) {
                parent.check()?;
                if store.status().node_id != self.node_id
                    || self.registered_store.is_some_and(|id| {
                        store.registered_native_metadata_member_store_id().ok() != Some(id)
                    })
                {
                    return Err(invalid("actual registered control owner/StoreUUID changed"));
                }
                return Ok(store);
            }
        }
    }
    pub(super) fn require_database(&self, db: &BicDb, topology: &ClusterTopology) -> Result<()> {
        if let Some(expected) = self.registered_store {
            let record = db.read_native_metadata_store(topology, &self.node_id)?;
            if !record.registered() || record.store_id() != expected {
                return Err(invalid(
                    "actual protected registered control StoreUUID changed",
                ));
            }
            record.require_namespaces_current(db)?;
        }
        Ok(())
    }
    pub(super) fn publish_committed_metadata(&self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        let actual = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("control service busy before publication"))?
            .listener_open_guard();
        let topology = {
            let store = self.lock_metadata(parent)?;
            store.require_listener_database(&actual)?;
            store.committed_topology().clone()
        };
        self.transport.install_topology(topology)?;
        actual.require_live()?;
        parent.check()
    }
    pub(super) fn start_listener(
        &self,
        network: ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<ClusterDataServerHandle> {
        parent.check()?;
        let address: std::net::SocketAddr = {
            let store = self.lock_metadata(parent)?;
            store
                .committed_topology()
                .nodes
                .get(&self.node_id)
                .ok_or_else(|| invalid("control listener member absent"))?
                .address
                .parse()
                .map_err(|_| invalid("control listener requires numeric actual endpoint"))?
        };
        // The caller stores the actual handle BEFORE checking cancellation.
        crate::distribution_transport::start_cluster_data_server_with_metadata_read_provider(
            address,
            network,
            Arc::clone(&self.topology),
            Arc::clone(&self.service),
            None,
            Some(Arc::clone(&self.metadata)),
            None,
            Some(Arc::clone(&self.provider)),
        )
    }
    pub(super) fn start_automatic_metadata(
        &self,
        network: ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataSupervisor> {
        NativeMetadataSupervisor::start(
            Arc::clone(&self.metadata),
            &self.service,
            Arc::clone(&self.topology),
            network,
            NativeMetadataSupervisorConfig::default(),
            parent,
        )
    }
    pub(crate) fn elect(&self, parent: &CancellationToken) -> Result<u64> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let (request, peers) = {
            let mut store = self.lock_metadata(&token)?;
            let peers = store
                .committed_topology()
                .nodes
                .values()
                .filter(|m| {
                    m.lifecycle == ClusterNodeLifecycle::Active
                        && m.metadata_role == MetadataMemberRole::Voter
                        && m.id != self.node_id
                })
                .map(|m| m.id.clone())
                .collect::<Vec<_>>();
            (store.start_election()?, peers)
        };
        let mut votes = BTreeSet::from([self.node_id.clone()]);
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
            let mut store = self.lock_metadata(&token)?;
            if store.status().current_term != request.term {
                return Err(invalid(
                    "initial election term changed during remote voting",
                ));
            }
            store.become_leader(&votes)?;
            store.status().last_log_index
        };
        self.heartbeat(&token)?;
        let status = self.lock_metadata(&token)?.status();
        if status.role != MetadataConsensusRole::Leader || status.commit_index < barrier {
            return Err(invalid(
                "initial metadata leadership barrier lacks durable quorum",
            ));
        }
        Ok(status.commit_index)
    }

    pub(crate) fn heartbeat(&self, parent: &CancellationToken) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        self.publish_committed_metadata(&token)?;
        for _ in 0..3 {
            let peers = {
                let store = self.lock_metadata(&token)?;
                if store.status().role != MetadataConsensusRole::Leader {
                    return Err(invalid("initial heartbeat requires actual metadata leader"));
                }
                store
                    .committed_topology()
                    .nodes
                    .values()
                    .filter(|m| m.lifecycle == ClusterNodeLifecycle::Active && m.id != self.node_id)
                    .map(|m| m.id.clone())
                    .collect::<Vec<_>>()
            };
            for peer in peers {
                let request = {
                    let store = self.lock_metadata(&token)?;
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
                    // Native operator endpoints are numeric. An invalid
                    // registered learner must not block maintenance in DNS.
                    if store.committed_topology().nodes[&peer]
                        .address
                        .parse::<std::net::SocketAddr>()
                        .is_err()
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
                self.lock_metadata(&token)?
                    .record_append_response(&peer, &reply)?;
                self.publish_committed_metadata(&token)?;
            }
        }
        if self.lock_metadata(&token)?.status().role != MetadataConsensusRole::Leader {
            return Err(invalid("initial metadata leader changed during heartbeat"));
        }
        self.publish_committed_metadata(&token)?;
        token.check()
    }
}
