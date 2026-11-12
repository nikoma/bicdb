//! A NEW immutable initial-DATA receiver, never an expanded old control mode.
//! The owner is retained before every fallible publication/drain/network step.
use super::*;
use crate::db::{NativeInitialDataGenesis, NativeWriterHostState};
use crate::distribution_native_read::NativeStrongEventDispatcher;
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use std::sync::Weak;

mod cleanup;
use cleanup::{NativeInitialDataCleanup, NativeInitialDataCleanupSender};

#[derive(Clone, Debug, serde::Serialize)]
pub struct NativeInitialDataRangeStatus {
    pub range_id: crate::RangeId,
    pub epoch: u64,
    pub local_leader: bool,
    pub leader_admission_observed: bool,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct NativeInitialDataStatus {
    pub receiver_listening: bool,
    pub writer_policy_current: bool,
    pub dispatcher_published: bool,
    pub strong_readiness: &'static str,
    pub replica_readiness: &'static str,
    pub ranges: Vec<NativeInitialDataRangeStatus>,
    pub cleanup_quarantined_owners: usize,
}

/// Only this actual INITIAL owner constructs it after policy publication. It
/// is a lifetime/ingress fence, never a data QQ, Strong permit or replica vote.
pub(crate) struct NativeInitialDataAdmission {
    service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    writer: Arc<NativeWriterHostState>,
    root: PathBuf,
    store_open: Uuid,
    // Actual service SLOT identity, not merely a live removed Store/old ID.
    store_slot: crate::distribution_data::RangeWriteServiceSlotGuard,
    life: Weak<AtomicBool>,
    published: Arc<AtomicBool>,
}
impl NativeInitialDataAdmission {
    pub(crate) fn require_live(&self) -> Result<()> {
        if !self
            .life
            .upgrade()
            .is_some_and(|live| live.load(Ordering::Acquire))
        {
            return Err(invalid("initial DATA owner is stopping or ended"));
        }
        self.store_slot.require_live()?;
        if self.store_slot.id() != self.store_open {
            return Err(invalid("initial DATA actual Store slot/open differs"));
        }
        self.writer.require_store(&self.root, self.store_open)
    }
    /// Necessary same-service/current-slot refusal, never routing/Q/read/write
    /// authority. The constructor captured the actual slot under that service.
    pub(crate) fn require_routing_service(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        expected_store_open: Uuid,
    ) -> Result<()> {
        self.require_live()?;
        if self
            .service
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, service))
            || self.store_open != expected_store_open
        {
            return Err(invalid(
                "initial DATA routing belongs to another actual service/Store",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_listener(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        runtime: &Arc<RangeRecoveryRuntime>,
    ) -> Result<()> {
        self.require_live()?;
        if self
            .service
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, service))
            || self
                .runtime
                .upgrade()
                .is_none_or(|actual| !Arc::ptr_eq(&actual, runtime))
        {
            return Err(invalid(
                "initial DATA receiver changed actual service/runtime",
            ));
        }
        Ok(())
    }
    pub(crate) fn application_published(&self) -> Result<bool> {
        self.require_live()?;
        Ok(self.published.load(Ordering::Acquire))
    }
}

pub(super) struct NativeInitialDataHost {
    initial: Arc<NativeInitialConsensusLaunch>,
    enrollment: Option<NativeInitialWriterEnrollment>,
    genesis: BTreeMap<crate::RangeId, NativeInitialDataGenesis>,
    runtime: Option<Arc<RangeRecoveryRuntime>>,
    dispatcher: Option<Arc<NativeStrongEventDispatcher>>,
    admission: Option<Arc<NativeInitialDataAdmission>>,
    old_listener: Option<ClusterDataServerHandle>,
    drained: Option<DrainedClusterDataListener>,
    listener: Option<ClusterDataServerHandle>,
    life: Arc<AtomicBool>,
    published: Arc<AtomicBool>,
    stopping: bool,
    cleanup: Option<NativeInitialDataCleanupSender>,
}
impl Drop for NativeInitialDataHost {
    fn drop(&mut self) {
        self.life.store(false, Ordering::Release);
        self.published.store(false, Ordering::Release);
        if let Some(dispatcher) = &self.dispatcher {
            // Atomic ingress flags above are immediate. A contended binding
            // is cleared by the retained cleanup worker, never waited on here.
            dispatcher.try_clear_for_cleanup();
        }
        for listener in [&self.listener, &self.old_listener].into_iter().flatten() {
            listener.request_shutdown();
        }
        if self.enrollment.is_some() {
            // The reaper exists BEFORE any installed hook. Dropping never
            // waits for network/DB/commit locks and never creates new grants.
            let cleanup = NativeInitialDataCleanup::take_from(self);
            if let Some(sender) = &self.cleanup {
                sender.defer(cleanup);
            } else {
                // Construction cannot install without the reaper. Preserve
                // owners rather than accidentally revoke/lose an unknown cut.
                cleanup.quarantine();
            }
        }
    }
}
impl NativeInitialDataHost {
    pub(super) fn is_running(&self) -> bool {
        // A failed finite drain retains the owner/handle for retry, but must
        // not let the CLI restart a metadata supervisor on a stopping socket.
        !self.stopping && self.listener.is_some()
    }
    fn new(
        initial: Arc<NativeInitialConsensusLaunch>,
        listener: ClusterDataServerHandle,
    ) -> Result<Self> {
        if initial.seed.spec.initial_ranges.is_empty() {
            return Err(invalid("blank learner has no INITIAL data ranges"));
        }
        Ok(Self {
            initial,
            enrollment: None,
            genesis: BTreeMap::new(),
            runtime: None,
            dispatcher: None,
            admission: None,
            old_listener: Some(listener),
            drained: None,
            listener: None,
            life: Arc::new(AtomicBool::new(true)),
            published: Arc::new(AtomicBool::new(false)),
            stopping: false,
            cleanup: None,
        })
    }
    fn advance(
        &mut self,
        leader: &ClusterNodeId,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.stopping {
            return Err(invalid("stopping INITIAL data owner cannot restart"));
        }
        if self.cleanup.is_none() {
            // This retained revocation-only worker must exist BEFORE an
            // actual hook can create coor -> service -> DB ownership.
            self.cleanup = Some(NativeInitialDataCleanupSender::ensure(parent)?);
        }
        if self.enrollment.is_none() {
            let topology = self
                .initial
                .topology
                .try_read()
                .ok_or_else(|| invalid("initial DATA routing busy"))?
                .clone();
            let database = self
                .initial
                .seed
                .database
                .try_read()
                .ok_or_else(|| invalid("initial DATA database busy"))?;
            // No R0 reconstruction: these sealed facts come from this SAME
            // retained INITIAL factory, protected original native anchors.
            for range in &self.initial.seed.spec.initial_ranges {
                let proof = database.observe_initial_data_genesis(
                    &self.initial.seed.factory,
                    range,
                    &topology,
                )?;
                self.genesis.insert(range.id, proof);
            }
            drop(database);
            let initial = self.initial.clone();
            initial.enroll_initial_writer_retained(leader, parent, |owner| {
                self.enrollment = Some(owner)
            })?;
        }
        let enrollment = self.enrollment.as_ref().unwrap();
        if self.runtime.is_none() {
            self.runtime = Some(Arc::new(
                RangeRecoveryRuntime::new_cancellable(
                    self.initial.service.clone(),
                    self.initial.provider.clone(),
                    parent,
                )?
                .with_participant_transport(self.initial.transport.clone()),
            ));
        }
        let runtime = self.runtime.as_ref().unwrap();
        if self.dispatcher.is_none() {
            self.dispatcher = Some(NativeStrongEventDispatcher::new(
                &self.initial.service,
                runtime,
                &self.initial.transport,
                &self.initial.topology,
            )?);
        }
        if self.admission.is_none() {
            let service = self
                .initial
                .service
                .try_lock()
                .ok_or_else(|| invalid("initial DATA service busy"))?;
            let range = self.genesis.values().next().unwrap().range();
            let (writer, root) = service.retain_initial_data_writer(
                &enrollment.coordinator,
                &enrollment.guard,
                range,
            )?;
            self.admission = Some(Arc::new(NativeInitialDataAdmission {
                service: Arc::downgrade(&self.initial.service),
                runtime: Arc::downgrade(runtime),
                writer,
                root,
                store_open: service.native_initial_listener_store_open_id(),
                store_slot: service.native_replica_listener_store_open_guard(),
                life: Arc::downgrade(&self.life),
                published: self.published.clone(),
            }));
        }
        let admission = self.admission.as_ref().unwrap();
        admission.require_live()?;
        if self.drained.is_none() {
            self.drained = Some(
                self.old_listener
                    .as_mut()
                    .ok_or_else(|| invalid("initial DATA stopping owner absent"))?
                    .stop_and_drain(parent)?,
            );
        }
        self.drained
            .as_ref()
            .unwrap()
            .require_service(&self.initial.service)?;
        self.old_listener.take();
        if self.listener.is_none() {
            let address: std::net::SocketAddr = network_address(&self.initial, parent)?
                .parse()
                .map_err(|_| invalid("initial DATA requires numeric local address"))?;
            self.listener = Some(
                crate::distribution_transport::start_cluster_initial_native_data_server(
                    address,
                    network.clone(),
                    self.initial.topology.clone(),
                    self.initial.service.clone(),
                    self.initial.metadata.clone(),
                    self.initial.provider.clone(),
                    runtime.clone(),
                    admission.clone(),
                )?,
            );
        }
        parent.check()?;
        // Replica protocol is available now; APP remains explicitly closed
        // until all LOCAL leader ranges pass actual cancellable readiness.
        for proof in self.genesis.values() {
            if proof.range().leader == self.initial.seed.spec.node_id
                && !enrollment
                    .coordinator
                    .initial_data_leader_admission_observed(proof.range())
            {
                enrollment.coordinator.recover_initial_native_genesis(
                    runtime,
                    &enrollment.guard,
                    proof,
                    leader,
                    parent,
                )?;
            }
        }
        parent.check()?;
        let already_published = self.published.load(Ordering::Acquire);
        let dispatcher = self.dispatcher.as_ref().unwrap();
        dispatcher.publish(&enrollment.coordinator, &enrollment.guard)?;
        if let Err(error) = parent.check().and_then(|_| admission.require_live()) {
            // APP and typed Get are still fenced by published=false. Clearing
            // this first unpublished slot cannot revoke an acknowledged APP.
            if !already_published {
                dispatcher.clear();
            }
            return Err(error);
        }
        // Ingress publication is the successful action's linearization point.
        // Do not revoke an earlier successful publication on a later retry.
        self.published.store(true, Ordering::Release);
        Ok(())
    }
    fn status(&self, parent: &CancellationToken) -> Result<NativeInitialDataStatus> {
        parent.check()?;
        let writer_policy_current = match &self.admission {
            Some(admission) => {
                admission.require_live().is_ok()
                    && self.initial.topology.try_read().is_some_and(|topology| {
                        admission.writer.require_topology(&topology).is_ok()
                    })
            }
            None => false,
        };
        let ranges = self
            .genesis
            .values()
            .map(|proof| NativeInitialDataRangeStatus {
                range_id: proof.range().id,
                epoch: proof.range().epoch,
                local_leader: proof.range().leader == self.initial.seed.spec.node_id,
                leader_admission_observed: self.enrollment.as_ref().is_some_and(|owner| {
                    owner
                        .coordinator
                        .initial_data_leader_admission_observed(proof.range())
                }),
            })
            .collect();
        Ok(NativeInitialDataStatus {
            receiver_listening: self.listener.is_some(),
            writer_policy_current,
            dispatcher_published: self.published.load(Ordering::Acquire),
            replica_readiness: "actual policy/native/refusal checks at every replica RPC",
            strong_readiness:
                "request-scoped fresh MQ/data QQ/MVCC/final MQ; publication is not readiness",
            ranges,
            cleanup_quarantined_owners: NativeInitialDataCleanupSender::quarantined_owners(),
        })
    }
    pub(super) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.stopping = true;
        self.life.store(false, Ordering::Release);
        self.published.store(false, Ordering::Release);
        if let Some(dispatcher) = &self.dispatcher {
            while !dispatcher.try_clear_for_cleanup() {
                parent.check()?;
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        if let Some(listener) = self.old_listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        loop {
            parent.check()?;
            let active = self.dispatcher.as_ref().map_or(0, |d| d.active_workers());
            let contexts = match &self.runtime {
                Some(runtime) => runtime.active_contexts()?,
                None => 0,
            };
            if active == 0 && contexts == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if let Some(owner) = &self.enrollment {
            let database = self
                .initial
                .seed
                .database
                .try_read()
                .ok_or_else(|| invalid("initial DATA database busy during stop"))?;
            // Dispatcher clear above fences NEW requests only. Existing
            // workers retain their real proofs; keep the hook installed until
            // actual listener/worker/context drain. Compare AND clear under the
            // actual admission/hook locks: never erase a replacement writer.
            owner
                .guard
                .retire_if_current(&database, &owner.coordinator)?;
        }
        self.listener.take();
        self.old_listener.take();
        Ok(())
    }
}
fn network_address(
    initial: &NativeInitialConsensusLaunch,
    parent: &CancellationToken,
) -> Result<String> {
    parent.check()?;
    let topology = initial
        .topology
        .try_read()
        .ok_or_else(|| invalid("initial DATA topology busy"))?;
    Ok(topology
        .nodes
        .get(&initial.seed.spec.node_id)
        .ok_or_else(|| invalid("initial DATA local node absent"))?
        .address
        .clone())
}

impl NativeMetadataControlHost {
    pub(crate) fn open_initial_data(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if self.data.is_none() {
            let initial = self.backend.initial_data_launch()?;
            if initial.seed.spec.initial_ranges.is_empty() {
                return Err(invalid("initial DATA factory has no ranges"));
            }
            let listener = self.listener.take().ok_or_else(|| {
                invalid("running actual control listener required before DATA transition")
            })?;
            self.data = Some(NativeInitialDataHost::new(initial, listener)?);
        }
        let leader = self
            .controller
            .as_ref()
            .ok_or_else(|| invalid("actual control provider absent"))?
            .bootstrap(parent)?
            .metadata_leader_id
            .ok_or_else(|| invalid("current metadata leader unknown for DATA open"))?;
        self.data
            .as_mut()
            .unwrap()
            .advance(&leader, &self.network, parent)
    }
    #[cfg(test)]
    pub(crate) fn test_replace_initial_data_writer_after_identity_check(
        &self,
        leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeInitialWriterEnrollment> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("test needs actual DATA owner"))?;
        let old = data
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("test needs actual old writer"))?;
        {
            let database = data
                .initial
                .seed
                .database
                .try_read()
                .ok_or_else(|| invalid("test actual DB busy"))?;
            let range = data
                .genesis
                .values()
                .next()
                .ok_or_else(|| invalid("test actual range missing"))?
                .range();
            old.guard
                .require_for_coordinator(&database, range, &old.coordinator)?;
        }
        data.initial.enroll_initial_writer(leader, parent)
    }
    #[cfg(test)]
    pub(crate) fn test_initial_data_routing_inputs(
        &self,
    ) -> Result<(
        Arc<NativeMetadataRegistrationController>,
        Arc<NativeInitialDataAdmission>,
    )> {
        Ok((
            self.controller
                .clone()
                .ok_or_else(|| invalid("actual controller absent"))?,
            self.data
                .as_ref()
                .and_then(|data| data.admission.clone())
                .ok_or_else(|| invalid("actual INITIAL admission absent"))?,
        ))
    }
    pub(crate) fn initial_data_status(
        &self,
        parent: &CancellationToken,
    ) -> Result<Option<NativeInitialDataStatus>> {
        self.data
            .as_ref()
            .map(|owner| owner.status(parent))
            .transpose()
    }
}
