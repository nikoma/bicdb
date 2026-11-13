//! A NEW immutable initial-DATA receiver, never an expanded old control mode.
//! The owner is retained before every fallible publication/drain/network step.
use super::*;
use crate::db::{NativeInitialDataGenesis, NativeWriterHostState};
use crate::distribution_native_read::NativeStrongEventDispatcher;
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use std::sync::Weak;

mod cleanup;
mod hot_recovery;
mod old_step;
use cleanup::{NativeInitialDataCleanup, NativeInitialDataCleanupSender};
use hot_recovery::NativeHotRecoveryMaintenance;
pub(crate) use old_step::NativeInitialOldStep;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer_binding_diagnostic: Option<serde_json::Value>,
    pub registry_refresh_last_attempt: Option<serde_json::Value>,
    pub hot_recovery_last_attempt: Option<serde_json::Value>,
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
    image_sources: Weak<crate::distribution_range_image::NativeRemoteImageSourceRegistry>,
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
    pub(crate) fn observe_remote_image_source_frame(
        &self,
        actual_target: &crate::distribution_consensus::MetadataReadParticipant,
        request: &crate::distribution_range_image::NativeRemoteImageSourceRequestV1,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_image::ActualNativeRemoteImageSourceFrame> {
        self.require_live()?;
        let sources = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("actual source image owner ended"))?;
        let frame = sources.observe_frame(actual_target, request, parent)?;
        self.require_live()?;
        parent.check()?;
        Ok(frame)
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
    hot_recovery: Option<Arc<NativeHotRecoveryMaintenance>>,
    registry_refresh_last_attempt: Option<serde_json::Value>,
    image_sources: Option<Arc<crate::distribution_range_image::NativeRemoteImageSourceRegistry>>,
}
impl Drop for NativeInitialDataHost {
    fn drop(&mut self) {
        self.life.store(false, Ordering::Release);
        self.published.store(false, Ordering::Release);
        if let Some(hot) = &self.hot_recovery {
            hot.request_shutdown();
        }
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
    pub(super) fn start_hot_process(&self, process: &CancellationToken) -> Result<()> {
        process.check()?;
        if !self.published.load(Ordering::Acquire) {
            return Ok(());
        }
        self.hot_recovery
            .as_ref()
            .ok_or_else(|| invalid("published DATA host lost retained maintenance owner"))?
            .start(process)
    }
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
            image_sources: None,
            registry_refresh_last_attempt: None,
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
            hot_recovery: None,
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
        if self.image_sources.is_none() {
            self.image_sources = Some(Arc::new(
                crate::distribution_range_image::NativeRemoteImageSourceRegistry::new(
                    self.initial.transport.clone(),
                    self.initial.provider.clone(),
                ),
            ));
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
                image_sources: Arc::downgrade(self.image_sources.as_ref().unwrap()),
            }));
        }
        if self.hot_recovery.is_none() {
            // Construction retains original objects only. Finite DataOpen
            // NEVER starts a maintenance worker or creates process authority.
            self.hot_recovery = Some(NativeHotRecoveryMaintenance::new(self)?);
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
    pub(super) fn admission_for_old_step(&self) -> Result<&Arc<NativeInitialDataAdmission>> {
        if !self.is_running() {
            return Err(invalid("original initial DATA source is stopping"));
        }
        let admission = self
            .admission
            .as_ref()
            .ok_or_else(|| invalid("actual source admission absent"))?;
        admission.require_live()?;
        Ok(admission)
    }
    /// Capture actual owner bindings before consuming OLD ownership. No
    /// fallible publication follows a take without an opaque retained owner.
    pub(super) fn retain_remote_image_source(
        &self,
        old: &mut Option<crate::distribution_range_handoff::NativeJointOldClosedQuorum>,
        target_attempt: Uuid,
        original_session: &CancellationToken,
    ) -> Result<crate::distribution_range_image::NativeRetainedRemoteImageSource> {
        original_session.check()?;
        if !self.is_running() {
            return Err(invalid("source initial DATA owner is not running"));
        }
        let admission = self
            .admission
            .as_ref()
            .ok_or_else(|| invalid("source admission absent"))?;
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| invalid("source runtime absent"))?;
        let enrollment = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("source original writer absent"))?;
        let registry = self
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("source image registry absent"))?;
        admission.require_listener(&self.initial.service, runtime)?;
        let actual = old
            .take()
            .ok_or_else(|| invalid("source OLD quorum already transferred"))?;
        let session = crate::distribution_range_image::NativeRemoteImageSourceSession::retain_actual_old_quorum(
            admission.clone(), runtime.clone(), self.initial.service.clone(), self.initial.metadata.clone(),
            enrollment.guard.clone(), enrollment.coordinator.clone(), actual, target_attempt, original_session);
        Ok(
            crate::distribution_range_image::NativeRetainedRemoteImageSource::retain(
                session,
                registry.clone(),
            ),
        )
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
            registry_refresh_last_attempt: self.registry_refresh_last_attempt.clone(),
            hot_recovery_last_attempt: self.hot_recovery.as_ref().and_then(|h| h.diagnostic()),
            writer_binding_diagnostic: self.enrollment.as_ref().map(|owner| {
                let mut diagnostic = owner.guard.binding_diagnostic();
                diagnostic["retained_owner_coordinator_id"] =
                    serde_json::json!(owner.coordinator.native_writer_coordinator_id());
                diagnostic
            }),
        })
    }
    fn refresh_registry(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if self.stopping || self.enrollment.is_none() || self.admission.is_none() {
            return Ok(());
        }
        let admission = self.admission.as_ref().unwrap();
        admission.require_live()?;
        // Cached C only skips unnecessary maintenance. It never publishes a
        // policy or grants APP/Strong; those paths keep all actual checks.
        if self
            .initial
            .topology
            .try_read()
            .is_some_and(|c| admission.writer.require_topology(&c).is_ok())
        {
            return Ok(());
        }
        let result = self.perform_registry_refresh(parent);
        self.registry_refresh_last_attempt = Some(match &result {
            Ok(()) => serde_json::json!({"result":"verified_same_writer_registry_audit_published",
                "scope":"metadata-only policy facts; not Ready or a new role"}),
            Err(error) => {
                serde_json::json!({"result":"refused_or_pending","error":error.to_string()})
            }
        });
        result
    }
    fn perform_registry_refresh(&self, parent: &CancellationToken) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        let (route, current) = {
            let metadata = self.initial.lock_initial_metadata(&token)?;
            (
                metadata
                    .status()
                    .leader_id
                    .ok_or_else(|| invalid("registry refresh metadata leader unknown"))?,
                metadata.committed_topology().clone(),
            )
        };
        // Real owned committed C is routing DATA only. Fresh authority is
        // obtained below with no DB/service/metadata guards across network.
        self.initial.transport.install_topology(current)?;
        let business = Uuid::now_v7();
        let metadata = self.initial.transport.acquire_host_metadata_observation(
            &route,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let current = metadata.committed_topology(business)?.clone();
        let enrollment = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("registry refresh original enrollment ended"))?;
        let database = self
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("registry refresh actual database busy"))?;
        let prepared = ClusterDataNodeService::prepare_native_writer_registry_refresh(
            &self.initial.service,
            &database,
            &enrollment.coordinator,
            &enrollment.guard,
            metadata,
            business,
        )?;
        // Installs routing facts only, never creates a recovery/serving grant.
        self.initial.transport.install_topology(current)?;
        enrollment.guard.refresh_metadata_registry(
            &database,
            &enrollment.coordinator,
            prepared,
            &token,
        )
    }
    pub(super) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.stopping = true;
        self.life.store(false, Ordering::Release);
        self.published.store(false, Ordering::Release);
        if let Some(hot) = &self.hot_recovery {
            hot.request_shutdown();
        }
        if let Some(dispatcher) = &self.dispatcher {
            while !dispatcher.try_clear_for_cleanup() {
                parent.check()?;
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        if let Some(hot) = &self.hot_recovery {
            hot.stop(parent)?;
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
    pub(crate) fn refresh_initial_data_registry(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        if let Some(data) = self.data.as_mut() {
            let registry = data.refresh_registry(parent);
            let hot = data
                .hot_recovery
                .as_ref()
                .map_or(Ok(()), |h| h.tick(parent));
            registry?;
            hot?;
        }
        Ok(())
    }
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
            .advance(&leader, &self.network, parent)?;
        // Only an actual retained automatic-metadata owner supplies this
        // separate PROCESS authority; never substitute the finite open token.
        if let Some(process) = self.data_process.get() {
            if let Err(error) = self.data.as_ref().unwrap().start_hot_process(process) {
                if let Some(hot) = &self.data.as_ref().unwrap().hot_recovery {
                    hot.note_start_error(&error);
                }
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_retained_initial_data_writer_guard(
        &self,
    ) -> Result<Arc<crate::db::InstalledNativeWriterHostGuard>> {
        Ok(Arc::clone(
            &self
                .data
                .as_ref()
                .and_then(|data| data.enrollment.as_ref())
                .ok_or_else(|| invalid("test original retained writer absent"))?
                .guard,
        ))
    }
    #[cfg(test)]
    pub(crate) fn test_prepare_initial_data_registry_refresh(
        &self,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_data::NativeWriterRegistryRefresh> {
        parent.check()?;
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("test needs actual DATA owner"))?;
        let (route, c) = {
            let metadata = data.initial.lock_initial_metadata(parent)?;
            (
                metadata
                    .status()
                    .leader_id
                    .ok_or_else(|| invalid("test metadata leader absent"))?,
                metadata.committed_topology().clone(),
            )
        };
        data.initial.transport.install_topology(c)?;
        let business = Uuid::now_v7();
        let metadata = data.initial.transport.acquire_host_metadata_observation(
            &route,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        let original = data
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("test original writer absent"))?;
        let database = data
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("test DB busy"))?;
        ClusterDataNodeService::prepare_native_writer_registry_refresh(
            &data.initial.service,
            &database,
            &original.coordinator,
            &original.guard,
            metadata,
            business,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_apply_initial_data_registry_refresh(
        &self,
        prepared: crate::distribution_data::NativeWriterRegistryRefresh,
        parent: &CancellationToken,
    ) -> Result<()> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("test needs actual DATA owner"))?;
        let original = data
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("test original writer absent"))?;
        let database = data
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("test DB busy"))?;
        original
            .guard
            .refresh_metadata_registry(&database, &original.coordinator, prepared, parent)
    }
    #[cfg(test)]
    pub(crate) fn test_force_initial_data_registry_refresh(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("test needs actual DATA owner"))?
            .perform_registry_refresh(parent)
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

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_hot_recovery_bindings(
        &self,
    ) -> Result<(
        Arc<
            crate::distribution_range_consensus::RangeWriteCoordinator<
                TcpClusterRelocationTransport,
            >,
        >,
        Arc<crate::db::InstalledNativeWriterHostGuard>,
        Arc<RangeRecoveryRuntime>,
        Arc<TcpClusterRelocationTransport>,
    )> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("actual DATA owner absent"))?;
        let enrollment = data
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("actual original writer absent"))?;
        Ok((
            enrollment.coordinator.clone(),
            enrollment.guard.clone(),
            data.runtime.as_ref().unwrap().clone(),
            data.initial.transport.clone(),
        ))
    }
    pub(crate) fn test_hot_worker_started(&self) -> bool {
        self.data
            .as_ref()
            .and_then(|d| d.hot_recovery.as_ref())
            .is_some_and(|h| h.started_for_test())
    }
}
#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_actual_hot_scope(
        &self,
        range: &crate::RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::RecoveryFenceScope> {
        self.data
            .as_ref()
            .and_then(|d| d.hot_recovery.as_ref())
            .ok_or_else(|| invalid("actual hot owner absent"))?
            .scope_for_test(range, parent)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_cancel_next_successor_acquisition(
        &self,
        command: &crate::RangeWriteCommand,
    ) -> Result<hot_recovery::NativeSuccessorCancelArm> {
        self.data
            .as_ref()
            .and_then(|d| d.hot_recovery.as_ref())
            .ok_or_else(|| invalid("successor test needs actual original DATA owner"))?
            .arm_successor_cancel(command)
    }
    pub(crate) fn test_hold_hot_controller_for_fault(
        &self,
        parent: &CancellationToken,
    ) -> Result<hot_recovery::NativeHotControllerTestHold<'_>> {
        self.data
            .as_ref()
            .and_then(|data| data.hot_recovery.as_ref())
            .ok_or_else(|| invalid("test hold requires actual retained DATA controller"))?
            .hold_controller_for_test(parent)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_hold_hot_replica_debt_schedule(
        &self,
        parent: &CancellationToken,
    ) -> Result<hot_recovery::NativeHotDebtScheduleHold<'_>> {
        self.data
            .as_ref()
            .and_then(|data| data.hot_recovery.as_ref())
            .ok_or_else(|| invalid("test debt hold requires actual retained DATA controller"))?
            .hold_debt_schedule_for_test(parent)
    }
}

// Network-only TEST cut: preserve actual DATA owner, policy, coordinator,
// dispatcher, runtime, DB/Store and metadata supervisor. No cold authority.
#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_pause_initial_data_listener_only(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        let data = self
            .data
            .as_mut()
            .ok_or_else(|| invalid("actual DATA owner required"))?;
        data.admission
            .as_ref()
            .ok_or_else(|| invalid("actual DATA admission absent"))?
            .require_live()?;
        let listener = data
            .listener
            .as_mut()
            .ok_or_else(|| invalid("actual DATA listener absent"))?;
        let drained = listener.stop_and_drain(parent)?;
        drained.require_service(&data.initial.service)?;
        data.drained = Some(drained);
        data.listener.take();
        parent.check()
    }
    pub(crate) fn test_resume_same_initial_data_listener_only(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let data = self
            .data
            .as_mut()
            .ok_or_else(|| invalid("same retained DATA owner required"))?;
        if data.stopping || data.listener.is_some() {
            return Err(invalid("actual DATA listener is stopping or still running"));
        }
        let admission = data
            .admission
            .as_ref()
            .ok_or_else(|| invalid("actual original DATA admission absent"))?;
        let runtime = data
            .runtime
            .as_ref()
            .ok_or_else(|| invalid("actual original runtime absent"))?;
        admission.require_listener(&data.initial.service, runtime)?;
        data.drained
            .as_ref()
            .ok_or_else(|| invalid("actual stopped/drained listener receipt required"))?
            .require_service(&data.initial.service)?;
        let address: std::net::SocketAddr = network_address(&data.initial, parent)?
            .parse()
            .map_err(|_| invalid("actual local address is not numeric"))?;
        data.listener = Some(
            crate::distribution_transport::start_cluster_initial_native_data_server(
                address,
                self.network.clone(),
                data.initial.topology.clone(),
                data.initial.service.clone(),
                data.initial.metadata.clone(),
                data.initial.provider.clone(),
                runtime.clone(),
                admission.clone(),
            )?,
        );
        admission.require_listener(&data.initial.service, runtime)?;
        parent.check()
    }
}
