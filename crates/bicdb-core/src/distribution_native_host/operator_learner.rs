//! Explicit fresh or strict persisted learner owner. No raw handles, promotion,
//! data Serving, schema retrofit or metadata identity reset is exposed.
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
    host: NativeLearnerLifecycle,
    transport: Arc<TcpClusterRelocationTransport>,
    leader: ClusterNodeId,
    address: String,
    network: ClusterNetworkTransportConfig,
    metadata_started: bool,
    transitioning: bool,
    stopped: bool,
}
enum NativeLearnerLifecycle {
    Fresh(NativeUnassignedLearnerHost),
    Cold(NativeColdLearnerMetadataHost),
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
            local.clone(),
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
        let first_member = first
            .committed_topology(business)?
            .nodes
            .get(&local)
            .ok_or_else(|| invalid("own learner absent from fresh metadata"))?;
        if first_member.address != address || first_member.incarnation != incarnation {
            return Err(invalid(
                "fresh learner explicit endpoint/incarnation changed before schema transfer",
            ));
        }
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
        let current_member = current
            .committed_topology(factory_business)?
            .nodes
            .get(&local)
            .ok_or_else(|| invalid("own learner absent before factory"))?;
        if current_member.address != address || current_member.incarnation != incarnation {
            return Err(invalid(
                "fresh learner explicit endpoint/incarnation changed before factory",
            ));
        }
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
            host: NativeLearnerLifecycle::Fresh(host),
            transport,
            leader,
            address: address.to_owned(),
            network,
            metadata_started: false,
            transitioning: false,
            stopped: false,
        })
    }
    /// Reopen ONLY the existing enrolled learner root. Bootstrap configuration
    /// supplies routing DATA; this owner's independently fresh Host MQ and the
    /// actual protected birth/UUID/original-C checks authorize the cold path.
    /// No schema import, factory creation or genesis initialization occurs.
    pub fn reopen_metadata_only(
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
        if !root.is_dir()
            || network.dev_localhost_plaintext
            || !(8 * 1024 * 1024..=1024 * 1024 * 1024).contains(&paged_buffer_pool_bytes)
        {
            return Err(invalid(
                "cold learner requires an existing root, TLS and supported memory",
            ));
        }
        let numeric: std::net::SocketAddr = address
            .parse()
            .map_err(|_| invalid("cold learner needs own numeric endpoint"))?;
        if numeric.port() == 0 {
            return Err(invalid("cold learner requires a fixed port"));
        }
        let bootstrap =
            TcpClusterBootstrapClient::new(cluster.clone(), local.clone(), network.clone())?;
        let snapshot = bootstrap.fetch_snapshot(&seed, seed_address)?;
        token.check()?;
        let leader = snapshot
            .metadata_leader_id
            .ok_or_else(|| invalid("cold learner metadata leader unknown"))?;
        let transport = Arc::new(TcpClusterRelocationTransport::new(
            cluster,
            local.clone(),
            Arc::new(RwLock::new(snapshot.topology)),
            network.clone(),
        )?);
        let business = Uuid::now_v7();
        let current = transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let member = current
            .committed_topology(business)?
            .nodes
            .get(&local)
            .ok_or_else(|| invalid("cold learner absent from actual current metadata"))?;
        if member.address != address || member.incarnation != incarnation {
            return Err(invalid(
                "cold learner explicit endpoint/incarnation differs",
            ));
        }
        transport.install_topology(current.committed_topology(business)?.clone())?;
        let host = NativeColdLearnerMetadataHost::reopen(
            root,
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(paged_buffer_pool_bytes),
            &current,
            business,
            network.clone(),
            &token,
        )?;
        let factory = host.factory_owner();
        current.require_live_request(business)?;
        token.check()?;
        Ok(Self {
            factory,
            host: NativeLearnerLifecycle::Cold(host),
            transport,
            leader,
            address: address.to_owned(),
            network,
            metadata_started: false,
            transitioning: true,
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
        let member = observed
            .committed_topology(business)?
            .nodes
            .get(&self.factory.spec.node_id)
            .ok_or_else(|| invalid("actual learner member absent"))?;
        if member.address != self.address
            || member.incarnation != self.factory.spec.member_incarnation
        {
            return Err(invalid(
                "learner endpoint/incarnation changed; old owner cannot rebind itself",
            ));
        }
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
        let metadata = match &self.host {
            NativeLearnerLifecycle::Fresh(host) => host.owned_metadata_snapshot(&token)?,
            NativeLearnerLifecycle::Cold(host) => host.owned_metadata_snapshot(&token)?,
        };
        let (metadata_store_id, metadata, listener) = match metadata {
            Some((id, status, listener)) => (Some(id), Some(status), listener),
            None => (None, None, false),
        };
        observed.require_live_request(business)?;
        token.check()?;
        if !self.metadata_started && !self.transitioning {
            if let NativeLearnerLifecycle::Fresh(host) = &self.host {
                host.refresh_metadata(observed, business, &token)?;
            }
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
        match &mut self.host {
            NativeLearnerLifecycle::Fresh(host) => {
                host.start_owned_metadata(&observed, business, &token)?;
            }
            NativeLearnerLifecycle::Cold(host) => {
                host.start(&observed, business, &token)?;
            }
        }
        self.metadata_started = true;
        self.transitioning = false;
        self.refresh(&token)
    }
    pub fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        match &mut self.host {
            NativeLearnerLifecycle::Fresh(host) => {
                host.stop_owned_metadata(parent)?;
                host.stop_and_drain(parent)?;
            }
            NativeLearnerLifecycle::Cold(host) => host.stop(parent)?,
        }
        self.stopped = true;
        Ok(())
    }
}
