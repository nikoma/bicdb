//! A real socket owner for the pre-metadata phase. It retains the actual
//! factory but exposes no database, data-service or coordinator handles.
use super::*;
use crate::distribution_transport::{
    start_cluster_data_server, ClusterDataServerHandle, TcpClusterRelocationTransport,
};
use crate::{ClusterDataNodeService, ClusterNetworkTransportConfig};
use parking_lot::Mutex;

pub(crate) struct NativeProvisionalHost {
    listener: Option<ClusterDataServerHandle>,
    seed: Arc<NativeProvisionalNode>,
    // These are owned here, never exposed as a bypass around the factory.
    _service: Arc<Mutex<ClusterDataNodeService>>,
    transport: Arc<TcpClusterRelocationTransport>,
}
impl NativeProvisionalHost {
    pub(crate) fn start(
        seed: Arc<NativeProvisionalNode>,
        network: ClusterNetworkTransportConfig,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        seed.require_unregistered()?;
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
            network,
            topology,
            Arc::clone(&service),
            None,
            None,
            None,
        )?;
        cancellation.check()?;
        Ok(Self {
            listener: Some(listener),
            seed,
            _service: service,
            transport,
        })
    }

    /// This obtains current factory facts from every planned actual listener,
    /// including this node through its own authenticated socket. No shared
    /// database references, test DTOs, or metadata authority are substituted.
    pub(crate) fn observe_initial_configuration(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<NativeInitialMetadataConfiguration> {
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

    pub(crate) fn shutdown(mut self) -> Result<()> {
        if let Some(listener) = self.listener.take() {
            listener.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
