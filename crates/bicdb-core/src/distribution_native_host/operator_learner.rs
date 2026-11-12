//! Fresh unassigned learner process owner. No raw DB/service/metadata handles,
//! promotion, data Serving or cold reconstruction path is exposed.
use super::*;
use crate::{
    ClusterNetworkTransportConfig, MetadataConsensusStatus, TcpClusterBootstrapClient,
    TcpClusterRelocationTransport,
};
use std::time::Instant;

#[derive(Clone, Debug, Serialize)]
pub struct NativeMetadataLearnerStatus {
    pub version: u32,
    pub node_id: ClusterNodeId,
    pub birth_id: Uuid,
    pub birth_sha256: String,
    pub birth_bound: bool,
    pub schema_sha256: String,
    pub namespace_ids: Vec<(String, Uuid)>,
    pub metadata_store_id: Option<Uuid>,
    pub metadata: Option<MetadataConsensusStatus>,
    pub metadata_listener_owned: bool,
    pub phase: &'static str,
    pub data_serving: bool,
}
pub struct NativeMetadataLearnerHost {
    factory: Arc<NativeUnassignedLearnerFactory>,
    host: NativeUnassignedLearnerHost,
    transport: Arc<TcpClusterRelocationTransport>,
    leader: ClusterNodeId,
    network: ClusterNetworkTransportConfig,
    metadata_started: bool,
    transitioning: bool,
    stopped: bool,
}
impl NativeMetadataLearnerHost {
    pub fn start_fresh_metadata_only(
        root: &Path,
        cluster: crate::ClusterId,
        local: ClusterNodeId,
        address: &str,
        incarnation: u64,
        seed: ClusterNodeId,
        seed_address: &str,
        network: ClusterNetworkTransportConfig,
        paged_buffer_pool_bytes: u64,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        network.validate()?;
        if root.exists()
            || network.dev_localhost_plaintext
            || !(8 * 1024 * 1024..=1024 * 1024 * 1024).contains(&paged_buffer_pool_bytes)
        {
            return Err(invalid(
                "fresh learner refuses an existing root, plaintext or unsupported memory",
            ));
        }
        let numeric: std::net::SocketAddr = address
            .parse()
            .map_err(|_| invalid("fresh learner needs own numeric endpoint"))?;
        if numeric.port() == 0 {
            return Err(invalid("fresh learner requires a fixed port"));
        }
        let bootstrap =
            TcpClusterBootstrapClient::new(cluster.clone(), local.clone(), network.clone())?;
        let snapshot = bootstrap.fetch_snapshot(&seed, seed_address)?;
        token.check()?;
        let member = snapshot.topology.nodes.get(&local).ok_or_else(|| {
            invalid("learner identity must be actually registered before factory creation")
        })?;
        if member.address != address
            || member.incarnation != incarnation
            || member.native_store_birth_v1.is_some()
            || member.metadata_role != MetadataMemberRole::Learner
            || member.lifecycle != ClusterNodeLifecycle::Active
        {
            return Err(invalid(
                "fresh learner registration/address/incarnation/birth differs",
            ));
        }
        let leader = snapshot
            .metadata_leader_id
            .ok_or_else(|| invalid("fresh learner metadata leader unknown"))?;
        let transport = Arc::new(TcpClusterRelocationTransport::new(
            cluster,
            local,
            Arc::new(RwLock::new(snapshot.topology)),
            network.clone(),
        )?);
        let business = Uuid::now_v7();
        let first = transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let range = first
            .committed_topology(business)?
            .ranges
            .values()
            .next()
            .ok_or_else(|| invalid("learner has no committed schema domain"))?
            .clone();
        transport.install_topology(first.committed_topology(business)?.clone())?;
        let bundle = transport.export_native_learner_schema(&range.leader, &range, &token)?;
        // Factory authority is a separately fresh OWN learner MQ after schema
        // DATA transfer, never a source fingerprint or bootstrap snapshot.
        let factory_business = Uuid::now_v7();
        let current = transport.acquire_host_metadata_observation(
            &leader,
            factory_business,
            Duration::from_secs(5),
            &token,
        )?;
        transport.install_topology(current.committed_topology(factory_business)?.clone())?;
        let db = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(paged_buffer_pool_bytes);
        let factory = Arc::new(NativeUnassignedLearnerFactory::create(
            root,
            db,
            &current,
            factory_business,
            &network,
            &bundle,
            &token,
        )?);
        let host = NativeUnassignedLearnerHost::start(
            Arc::clone(&factory),
            current,
            factory_business,
            network.clone(),
            &token,
        )?;
        Ok(Self {
            factory,
            host,
            transport,
            leader,
            network,
            metadata_started: false,
            transitioning: false,
            stopped: false,
        })
    }
    fn own_metadata(
        &self,
        parent: &CancellationToken,
    ) -> Result<(
        crate::distribution_consensus::HostMetadataReadObservation,
        Uuid,
    )> {
        let business = Uuid::now_v7();
        let observed = self.transport.acquire_host_metadata_observation(
            &self.leader,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        self.transport
            .install_topology(observed.committed_topology(business)?.clone())?;
        Ok((observed, business))
    }
    pub fn refresh(&mut self, parent: &CancellationToken) -> Result<NativeMetadataLearnerStatus> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        if self.stopped {
            return Err(invalid("stopped learner cannot refresh or restart"));
        }
        let (observed, business) = self.own_metadata(&token)?;
        let topology = observed.committed_topology(business)?;
        if self.metadata_started || self.transitioning {
            self.factory
                .enrolled_metadata_context(&observed, business, &self.network)?;
        } else {
            self.factory
                .enrollment_proposal(&observed, business, &self.network)?;
        }
        let birth = self.factory.actual_birth();
        let bound = topology.nodes[&self.factory.spec.node_id]
            .native_store_birth_v1
            .as_ref()
            == Some(birth);
        let (schema_sha256, namespace_ids) = {
            let db = self
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("learner status DB busy"))?;
            let schema = db.schema_compatibility_fingerprint()?.sha256;
            let mut ids = db
                .collections()
                .into_iter()
                .map(|m| {
                    m.local_namespace_id
                        .filter(|u| !u.is_nil())
                        .map(|u| (m.name, u))
                        .ok_or_else(|| invalid("actual learner namespace missing"))
                })
                .collect::<Result<Vec<_>>>()?;
            ids.sort();
            (schema, ids)
        };
        let metadata = self.host.owned_metadata_snapshot(&token)?;
        let (metadata_store_id, metadata, listener) = match metadata {
            Some((id, status, listener)) => (Some(id), Some(status), listener),
            None => (None, None, false),
        };
        observed.require_live_request(business)?;
        token.check()?;
        if !self.metadata_started && !self.transitioning {
            self.host.refresh_metadata(observed, business, &token)?;
        }
        Ok(NativeMetadataLearnerStatus {
            version: 1,
            node_id: self.factory.spec.node_id.clone(),
            birth_id: birth.birth_id(),
            birth_sha256: birth.birth_sha256().to_string(),
            birth_bound: bound,
            schema_sha256,
            namespace_ids,
            metadata_store_id,
            metadata,
            metadata_listener_owned: listener,
            phase: if self.metadata_started {
                "owned_metadata_learner"
            } else if self.transitioning {
                "metadata_transition"
            } else {
                "provisional_birth"
            },
            data_serving: false,
        })
    }
    pub fn start_metadata(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataLearnerStatus> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        if self.stopped {
            return Err(invalid("stopped learner cannot start metadata"));
        }
        let (observed, business) = self.own_metadata(&token)?;
        self.factory
            .enrolled_metadata_context(&observed, business, &self.network)?;
        self.transitioning = true;
        self.host
            .start_owned_metadata(&observed, business, &token)?;
        self.metadata_started = true;
        self.transitioning = false;
        self.refresh(&token)
    }
    pub fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.host.stop_owned_metadata(parent)?;
        self.host.stop_and_drain(parent)?;
        self.stopped = true;
        Ok(())
    }
}
