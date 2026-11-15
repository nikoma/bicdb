//! Bounded initial metadata orchestration. No background election scheduler,
//! ordinary listener, range recovery, Serving or first-ACK claim is implied.
use super::*;
use crate::distribution_consensus::MetadataConsensusStore;
use crate::distribution_range_consensus::RangeWriteCoordinator;
use std::time::Instant;

/// Owns the real local hook/policy lifetime. This is not a range-ready proof.
pub(crate) struct NativeInitialWriterEnrollment {
    pub(super) coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    pub(super) guard: Arc<crate::db::InstalledNativeWriterHostGuard>,
    pub(crate) metadata_context_id: Uuid,
    pub(crate) metadata_term: u64,
    pub(crate) metadata_read_index: u64,
}
impl NativeInitialConsensusLaunch {
    pub(in crate::distribution_native_host) fn start_automatic_metadata(
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
        NativeMetadataControlBackend::borrow_initial(self).elect(parent)
    }
    /// A finite call with accepted per-peer progress; scheduling is separate.
    pub(crate) fn heartbeat_initial_metadata(&self, parent: &CancellationToken) -> Result<()> {
        NativeMetadataControlBackend::borrow_initial(self).heartbeat(parent)
    }

    /// Fresh HostMQ is data about current membership, not range recovery.
    /// Keep the returned owner alive; no ordinary-data endpoint is released.
    pub(crate) fn enroll_initial_writer(
        &self,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeInitialWriterEnrollment> {
        let mut retained = None;
        self.enroll_initial_writer_retained(metadata_leader, parent, |owner| {
            retained = Some(owner)
        })?;
        retained.ok_or_else(|| invalid("initial writer owner was not retained"))
    }
    pub(super) fn enroll_initial_writer_retained(
        &self,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
        retain: impl FnOnce(NativeInitialWriterEnrollment),
    ) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let (observed, business) =
            self.acquire_initial_enrollment_host_read(metadata_leader, &token)?;
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
        let coordinator = Arc::new(RangeWriteCoordinator::new_initial_schema_fenced(
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
        // Transfer actual installed ownership BEFORE a fallible post-install
        // deadline check. DataOpen retains it on timeout/unknown publication.
        retain(NativeInitialWriterEnrollment {
            coordinator,
            guard: Arc::new(guard),
            metadata_context_id: context,
            metadata_term: term,
            metadata_read_index: read_index,
        });
        #[cfg(test)]
        self.transport.test_initial_enrollment_installed();
        token.check()
    }
}

impl NativeInitialConsensusLaunch {
    /// Only the readonly PRE-enrollment exchange repeats. The real original
    /// launch/service/open/slot and the SAME action token remain mandatory.
    /// Neither writer enrollment, hook install nor native WAL is in this loop.
    fn acquire_initial_enrollment_host_read(
        &self,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<(
        crate::distribution_consensus::HostMetadataReadObservation,
        Uuid,
    )> {
        parent.check()?;
        let (open, slot) = {
            let service = loop {
                parent.check()?;
                if let Some(service) = self.service.try_lock_for(Duration::from_millis(5)) {
                    break service;
                }
            };
            parent.check()?;
            (
                service.listener_open_guard(),
                service.native_replica_listener_store_open_guard(),
            )
        };
        loop {
            parent.check()?;
            open.require_live()?;
            slot.require_live()?;
            let remaining = parent
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .filter(|d| !d.is_zero())
                .ok_or(BicDbError::QueryTimedOut)?;
            let business = Uuid::now_v7();
            let actual = self.transport.acquire_initial_host_metadata_observation(
                metadata_leader,
                business,
                remaining.min(Duration::from_secs(5)),
                parent,
            );
            #[cfg(test)]
            self.transport.test_initial_read_return(&actual, parent);
            match actual {
                Ok(observed) => {
                    parent.check()?;
                    open.require_live()?;
                    slot.require_live()?;
                    // These are the SAME actual service's open/slot refusal
                    // fences. Existing policy/install validation below still
                    // checks the actual database; READ success is no writer grant.
                    return Ok((observed, business));
                }
                Err(error) => {
                    let transient =
                        crate::distribution_transport::HotRecoveryPeerFailure::from_error(&error)
                            .is_some_and(|e| {
                                matches!(
                                    e.kind(),
                                    std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::ConnectionRefused
                                        | std::io::ErrorKind::ConnectionAborted
                                        | std::io::ErrorKind::BrokenPipe
                                        | std::io::ErrorKind::UnexpectedEof
                                )
                            });
                    if !transient {
                        return Err(error);
                    }
                    parent.check()?;
                    open.require_live()?;
                    slot.require_live()?;
                    // Fresh authenticated READ/new business on the next pass.
                    // Connect/TLS/read and pacing all consume the original
                    // absolute action window, never a restarted five seconds.
                    std::thread::sleep(Duration::from_millis(2));
                    parent.check()?;
                }
            }
        }
    }
}
