//! A real socket owner for the pre-metadata phase. It retains the actual
//! factory but exposes no database, data-service or coordinator handles.
use super::*;
use crate::distribution_transport::{
    start_cluster_data_server, ClusterDataServerHandle, TcpClusterRelocationTransport,
};
use crate::{ClusterDataNodeService, ClusterNetworkTransportConfig};
use parking_lot::Mutex;
mod learner_enrollment;
mod metadata_startup;
mod registration;
pub(crate) use learner_enrollment::NativeLearnerMetadataEnrollment;
pub(crate) use metadata_startup::NativeInitialWriterEnrollment;
pub(crate) use registration::{NativeMetadataControlHost, NativeMetadataRegistrationController};

pub(crate) struct NativeProvisionalHost {
    listener: Option<ClusterDataServerHandle>,
    drained: Option<crate::distribution_transport::DrainedClusterDataListener>,
    seed: Arc<NativeProvisionalNode>,
    // These are owned here, never exposed as a bypass around the factory.
    _service: Arc<Mutex<ClusterDataNodeService>>,
    transport: Arc<TcpClusterRelocationTransport>,
    network: ClusterNetworkTransportConfig,
}
impl NativeProvisionalHost {
    pub(crate) fn start(
        seed: Arc<NativeProvisionalNode>,
        network: ClusterNetworkTransportConfig,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        seed.require_initial_preparation_surface()?;
        network.validate()?;
        if network.dev_localhost_plaintext {
            return Err(invalid(
                "initial factory bootstrap requires pinned mutual TLS",
            ));
        }
        let topology = Arc::new(RwLock::new(seed.plan.topology.clone()));
        let transport = Arc::new(TcpClusterRelocationTransport::new(
            seed.spec.cluster_id.clone(),
            seed.spec.node_id.clone(),
            Arc::clone(&topology),
            network.clone(),
        )?);
        let member = &seed.plan.topology.nodes[&seed.spec.node_id];
        let address: std::net::SocketAddr = member
            .address
            .parse()
            .map_err(|_| invalid("initial factory listener needs the planned numeric address"))?;
        if address.port() == 0 {
            return Err(invalid(
                "initial factory listener needs a fixed planned port",
            ));
        }
        let mut service = ClusterDataNodeService::open(
            seed.spec.cluster_id.clone(),
            seed.spec.node_id.clone(),
            &seed.root,
            Arc::clone(&seed.database),
            true,
        )?;
        service.attach_initial_birth_provider(&seed)?;
        let service = Arc::new(Mutex::new(service));
        let listener = start_cluster_data_server(
            address,
            network.clone(),
            topology,
            Arc::clone(&service),
            None,
            None,
            None,
        )?;
        cancellation.check()?;
        Ok(Self {
            listener: Some(listener),
            drained: None,
            seed,
            _service: service,
            transport,
            network,
        })
    }

    /// This obtains current factory facts from every planned actual listener,
    /// including this node through its own authenticated socket. No shared
    /// database references, test DTOs, or metadata authority are substituted.
    pub(crate) fn observe_initial_configuration(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<NativeInitialMetadataConfiguration> {
        if let Some(configuration) = self.seed.initial_prepared_configuration()? {
            return Ok(configuration);
        }
        // A peer may already have accepted C after another host's partial
        // broadcast. Discover only protected CONFIG data, never a read grant.
        for member in self.seed.plan.topology.nodes.keys() {
            if let Ok(observation) =
                self.transport
                    .discover_initial_configuration(member, &self.seed.plan, cancellation)
            {
                return NativeInitialMetadataConfiguration::from_configuration_data_observation(
                    &self.seed,
                    observation,
                );
            }
        }
        self.seed.require_unregistered()?;
        // All exchanges use the same parent budget. Repeating a peer or
        // opening a new connection cannot extend an expired attempt.
        let attempt =
            cancellation.child_with_deadline(std::time::Instant::now() + Duration::from_secs(5));
        let mut offers = Vec::with_capacity(self.seed.plan.topology.nodes.len());
        for member in self.seed.plan.topology.nodes.keys() {
            attempt.check()?;
            offers.push(self.transport.acquire_initial_birth_offer(
                member,
                &self.seed.plan,
                &attempt,
            )?);
        }
        let configuration = NativeInitialMetadataConfiguration::from_authenticated_offers(
            &self.seed.plan,
            offers,
            &attempt,
        )?;
        configuration.topology_for_actual_seed(&self.seed, &attempt)?;
        Ok(configuration)
    }

    pub(super) fn stop_retained(&mut self, parent: &CancellationToken) -> Result<()> {
        if let Some(listener) = self.listener.as_mut() {
            self.drained = Some(listener.stop_and_drain(parent)?);
        }
        self.listener.take();
        Ok(())
    }
    pub(crate) fn shutdown(mut self) -> Result<()> {
        if let Some(listener) = self.listener.take() {
            listener.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Actual stopped-listener/service/store handoff, not data/read admission.
/// The native marker means MAY HAVE BEEN EXPOSED, not running/deployed.
pub(crate) struct NativeInitialConsensusLaunch {
    pub(crate) metadata: Arc<Mutex<crate::MetadataConsensusStore>>,
    pub(crate) service: Arc<Mutex<ClusterDataNodeService>>,
    pub(crate) topology: Arc<RwLock<crate::ClusterTopology>>,
    seed: Arc<NativeProvisionalNode>,
    transport: Arc<TcpClusterRelocationTransport>,
    provider: Arc<crate::distribution_metadata_read_provider::MetadataReadProvider>,
}
impl NativeInitialConsensusLaunch {
    pub(crate) fn start_metadata_listener(
        &self,
        network: ClusterNetworkTransportConfig,
        cancellation: &CancellationToken,
    ) -> Result<ClusterDataServerHandle> {
        cancellation.check()?;
        if network.dev_localhost_plaintext {
            return Err(invalid("initial consensus needs pinned mutual TLS"));
        }
        let address: std::net::SocketAddr = self.topology.read().nodes[&self.seed.spec.node_id]
            .address
            .parse()
            .map_err(|_| invalid("initial consensus needs numeric planned address"))?;
        let server =
            crate::distribution_transport::start_cluster_data_server_with_metadata_read_provider(
                address,
                network,
                Arc::clone(&self.topology),
                Arc::clone(&self.service),
                None,
                Some(Arc::clone(&self.metadata)),
                None,
                Some(Arc::clone(&self.provider)),
            )?;
        cancellation.check()?;
        Ok(server)
    }
}
impl NativeProvisionalHost {
    pub(crate) fn prepare_common_initial_configuration(
        &self,
        configuration: &NativeInitialMetadataConfiguration,
        parent: &CancellationToken,
    ) -> Result<super::initial_preparation::NativeInitialConsensusPermit> {
        let token = parent.child_with_deadline(std::time::Instant::now() + Duration::from_secs(5));
        let mut observations = Vec::new();
        for member in self.seed.plan.topology.nodes.keys() {
            token.check()?;
            observations.push(self.transport.acquire_initial_preparation(
                member,
                &self.seed.plan,
                Some(configuration.topology.clone()),
                &token,
            )?);
        }
        super::initial_preparation::NativeInitialConsensusPermit::from_observations(
            &self.seed,
            configuration,
            observations,
            &token,
        )
    }
    pub(crate) fn finish_initial_consensus(
        &mut self,
        permit: &super::initial_preparation::NativeInitialConsensusPermit,
        parent: &CancellationToken,
    ) -> Result<NativeInitialConsensusLaunch> {
        permit.require_live()?;
        parent.check()?;
        let token = parent.child_with_deadline(permit.deadline()?);
        // Keep the SAME stopping handle/counter on every drain error.
        if self.drained.is_none() {
            let drained = self
                .listener
                .as_mut()
                .ok_or_else(|| invalid("provisional listener already released"))?
                .stop_and_drain(&token)?;
            self.drained = Some(drained);
        }
        let drained = self
            .drained
            .as_ref()
            .ok_or_else(|| invalid("actual drained listener proof missing"))?;
        let mut slot = self
            .seed
            .prepared_metadata
            .try_lock()
            .ok_or_else(|| invalid("prepared local store busy"))?;
        let fact = slot
            .as_ref()
            .ok_or_else(|| invalid("actual prepared store missing"))?
            .native_initial_pre_vote_fact()?;
        {
            let db = self
                .seed
                .database
                .try_read()
                .ok_or_else(|| invalid("initial DB busy before exposure"))?;
            db.expose_native_initial_consensus(permit, &fact)?;
        }
        drop(fact);
        self._service
            .lock()
            .transition_initial_preparation_listener(&self._service, drained, permit, &self.seed)?;
        permit.require_live()?;
        token.check()?;
        let metadata = Arc::new(Mutex::new(
            slot.take()
                .ok_or_else(|| invalid("prepared store disappeared"))?,
        ));
        let topology = Arc::new(RwLock::new(permit.configuration().clone()));
        let transport = Arc::new(TcpClusterRelocationTransport::new(
            self.seed.spec.cluster_id.clone(),
            self.seed.spec.node_id.clone(),
            Arc::clone(&topology),
            self.network.clone(),
        )?);
        let provider = Arc::new(
            crate::distribution_metadata_read_provider::MetadataReadProvider::new(
                Arc::clone(&metadata),
                transport.clone(),
            )?,
        );
        self.listener.take();
        self.drained.take();
        Ok(NativeInitialConsensusLaunch {
            metadata,
            service: Arc::clone(&self._service),
            topology,
            seed: Arc::clone(&self.seed),
            transport,
            provider,
        })
    }
}

impl NativeProvisionalHost {
    /// Resume a stopping PRE-VOTE owner using the SAME service/store. It never
    /// clears an exposure marker or rebuilds an initialized metadata store.
    pub(crate) fn restart_stopped_initial_listener(
        &mut self,
        network: ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        let operation =
            parent.child_with_deadline(std::time::Instant::now() + Duration::from_secs(5));
        operation.check()?;
        self.seed.require_initial_preparation_surface()?;
        if self.drained.is_none() {
            self.drained = Some(
                self.listener
                    .as_mut()
                    .ok_or_else(|| invalid("stopping listener custody missing"))?
                    .stop_and_drain(&operation)?,
            );
        }
        self.drained
            .as_ref()
            .unwrap()
            .require_service(&self._service)?;
        self.drained
            .as_ref()
            .unwrap()
            .require_store(self._service.lock().native_initial_listener_store_open_id())?;
        self.listener.take();
        network.validate()?;
        if network.dev_localhost_plaintext {
            return Err(invalid("preparation restart needs pinned TLS"));
        }
        let address: std::net::SocketAddr = self.seed.plan.topology.nodes[&self.seed.spec.node_id]
            .address
            .parse()
            .map_err(|_| invalid("preparation restart needs planned numeric address"))?;
        let listener = start_cluster_data_server(
            address,
            network,
            Arc::new(RwLock::new(self.seed.plan.topology.clone())),
            Arc::clone(&self._service),
            None,
            None,
            None,
        )?;
        self.listener = Some(listener);
        self.drained.take();
        operation.check()?;
        Ok(())
    }
}
