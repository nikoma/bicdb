//! All-owner cold learner restart. No dead listener is relabelled as drained;
//! no historical C/UUID is relabelled as current quorum or data admission.
use super::learner_birth::lock_owned_member_metadata;
use super::*;
use crate::distribution::ClusterNetworkTransportConfig;
use crate::distribution_consensus::{HostMetadataReadObservation, MetadataConsensusStore};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_transport::{ClusterDataServerHandle, TcpClusterRelocationTransport};
use parking_lot::Mutex;

mod control;

const DIRECTORY: &str = "native-learner-metadata";
pub(crate) struct NativeColdLearnerMetadataHost {
    factory: Arc<NativeUnassignedLearnerFactory>,
    network: ClusterNetworkTransportConfig,
    retained: Option<MetadataConsensusStore>,
    metadata: Option<Arc<Mutex<MetadataConsensusStore>>>,
    service: Option<Arc<Mutex<crate::ClusterDataNodeService>>>,
    topology: Option<Arc<RwLock<ClusterTopology>>>,
    transport: Option<Arc<TcpClusterRelocationTransport>>,
    provider: Option<Arc<MetadataReadProvider>>,
    listener: Option<ClusterDataServerHandle>,
    stopping: bool,
    supervisor: Option<NativeMetadataSupervisor>,
    control: Option<NativeMetadataControlHost>,
}
impl NativeColdLearnerMetadataHost {
    pub(in crate::distribution_native_host) fn start_automatic_metadata(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if (self.listener.is_none() && !self.control.as_ref().is_some_and(|c| c.is_running()))
            || self.stopping
        {
            return Err(invalid(
                "automatic cold metadata requires running Registered listener",
            ));
        }
        if self.supervisor.is_some() {
            return Ok(());
        }
        if let Some(control) = &self.control {
            self.supervisor = Some(control.start_automatic_metadata(parent)?);
            return Ok(());
        }
        self.supervisor = Some(NativeMetadataSupervisor::start(
            Arc::clone(
                self.metadata
                    .as_ref()
                    .ok_or_else(|| invalid("cold owned metadata absent"))?,
            ),
            self.service
                .as_ref()
                .ok_or_else(|| invalid("cold service absent"))?,
            Arc::clone(
                self.topology
                    .as_ref()
                    .ok_or_else(|| invalid("cold routing absent"))?,
            ),
            self.network.clone(),
            NativeMetadataSupervisorConfig::default(),
            parent,
        )?);
        Ok(())
    }
    /// Listener/catch-up only. False means local installed C/term/index still
    /// lags current OWN MQ; it can NEVER replace strict owned-member READY.
    pub(in crate::distribution_native_host) fn validate_registered_metadata_catchup(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<bool> {
        observed.require_live_request(business)?;
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| invalid("actual Registered metadata owner absent"))?;
        let proof = {
            let store = lock_owned_member_metadata(metadata, observed, business, &token)?;
            store.observe_registered_native_metadata_catchup(observed, business, &token)?
        };
        self.factory.require_registered_metadata_catchup(
            &proof,
            observed,
            business,
            &self.network,
        )?;
        // Factory/native validation occurs after the first guard is dropped.
        // This is the SAME original proof and store, not a fresh-role retry.
        let store = lock_owned_member_metadata(metadata, observed, business, &token)?;
        proof.require_current(&store)?;
        let current = proof.local_installed_current()?;
        observed.require_live_request(business)?;
        token.check()?;
        Ok(current)
    }

    pub(in crate::distribution_native_host) fn validate_owned_metadata_member(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        observed.require_live_request(business)?;
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        let open = self
            .service
            .as_ref()
            .ok_or_else(|| invalid("actual cold metadata service absent"))?
            .try_lock()
            .ok_or_else(|| invalid("cold metadata service busy"))?
            .listener_open_guard();
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| invalid("actual cold metadata backend absent"))?;
        let proof = {
            let store = lock_owned_member_metadata(metadata, observed, business, &token)?;
            store.require_listener_database(&open)?;
            store.observe_owned_native_metadata_member(observed, business, &token)?
        };
        self.factory
            .require_owned_metadata_member(&proof, observed, business, &self.network)?;
        let store = lock_owned_member_metadata(metadata, observed, business, &token)?;
        proof.require_current(&store)?;
        observed.require_live_request(business)?;
        token.check()
    }

    pub(in crate::distribution_native_host) fn factory_owner(
        &self,
    ) -> Arc<NativeUnassignedLearnerFactory> {
        Arc::clone(&self.factory)
    }
    pub(in crate::distribution_native_host) fn owned_metadata_snapshot(
        &self,
        parent: &CancellationToken,
    ) -> Result<Option<(Uuid, crate::MetadataConsensusStatus, bool)>> {
        parent.check()?;
        let Some(metadata) = self.metadata.as_ref() else {
            return Ok(None);
        };
        let store = metadata
            .try_lock()
            .ok_or_else(|| invalid("cold owned metadata status busy"))?;
        let id = store.registered_native_metadata_member_store_id()?;
        Ok(Some((
            id,
            store.status(),
            (self.listener.is_some() || self.control.as_ref().is_some_and(|c| c.is_running()))
                && !self.stopping,
        )))
    }

    /// This opens ONLY the existing actual root/manifest. It neither starts a
    /// listener nor initializes metadata. The caller keeps this owner when a
    /// later `start` fails, including protected Prepare/Registered ambiguity.
    pub(crate) fn reopen(
        root: &Path,
        config: DbConfig,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let voter = observed
            .committed_topology(business)?
            .nodes
            .get(observed.requester_node_id())
            .is_some_and(|member| member.metadata_role == MetadataMemberRole::Voter);
        let factory = if voter {
            NativeUnassignedLearnerFactory::reopen_registered_metadata(
                root, config, observed, business, &network, parent,
            )?
        } else {
            NativeUnassignedLearnerFactory::reopen_enrolled_metadata(
                root, config, observed, business, &network, parent,
            )?
        };
        {
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("cold learner DB busy checking host contract"))?;
            let native = db.native_recovery_admission(
                &factory.spec.cluster_id,
                factory.spec.bootstrap_id,
                &factory.spec.node_id,
                factory.spec.member_incarnation,
            )?;
            if native.writer_enforcement_version() != 1
                || native.metadata_initialization_version() != 1
                || native.abort_protocol_version() != 1
            {
                return Err(invalid("cold learner host requires the complete actual from-birth writer/metadata/abort contract"));
            }
        }
        observed.require_live_request(business)?;
        parent.check()?;
        Ok(Self {
            factory: Arc::new(factory),
            network,
            retained: None,
            metadata: None,
            service: None,
            topology: None,
            transport: None,
            provider: None,
            listener: None,
            stopping: false,
            supervisor: None,
            control: None,
        })
    }

    pub(crate) fn start(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Uuid> {
        self.start_with(observed, business, parent, |_| Ok(()))
    }
    fn start_with(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        mut cut: impl FnMut(&'static str) -> Result<()>,
    ) -> Result<Uuid> {
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        let registered_catchup = observed
            .committed_topology(business)?
            .nodes
            .get(&self.factory.spec.node_id)
            .is_some_and(|member| member.metadata_role == MetadataMemberRole::Voter);
        let current = if registered_catchup {
            self.factory
                .registered_metadata_context(observed, business, &self.network)?
        } else {
            self.factory
                .enrolled_metadata_context(observed, business, &self.network)?
        };
        let (root, metadata_root) = {
            let db = self
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("cold learner DB busy before metadata recovery"))?;
            (db.data_path().to_path_buf(), db.data_path().join(DIRECTORY))
        };
        if self.metadata.is_none() {
            if registered_catchup {
                MetadataConsensusStore::recover_registered_native_metadata_retaining(
                    &metadata_root,
                    Arc::clone(&self.factory.database),
                    observed,
                    business,
                    &token,
                    &mut self.retained,
                )?;
            } else {
                MetadataConsensusStore::recover_native_learner_owned_retaining(
                    &metadata_root,
                    Arc::clone(&self.factory.database),
                    &self.factory.factory,
                    observed,
                    business,
                    &token,
                    &mut self.retained,
                )?;
            }
            if self.service.is_none() {
                // Plain exclusive store borrow BEFORE it is exposed behind a
                // metadata mutex: no MetaMutex -> BicDb lock inversion.
                let store = self
                    .retained
                    .as_ref()
                    .ok_or_else(|| invalid("cold actual metadata owner absent"))?;
                let service = if registered_catchup {
                    let proof = store
                        .observe_registered_native_metadata_catchup(observed, business, &token)?;
                    self.factory.require_registered_metadata_catchup(
                        &proof,
                        observed,
                        business,
                        &self.network,
                    )?;
                    crate::ClusterDataNodeService::open_cold_registered_metadata_catchup(
                        self.factory.spec.cluster_id.clone(),
                        self.factory.spec.node_id.clone(),
                        &root,
                        Arc::clone(&self.factory.database),
                        store,
                        &proof,
                    )?
                } else {
                    crate::ClusterDataNodeService::open_cold_native_learner_metadata(
                        self.factory.spec.cluster_id.clone(),
                        self.factory.spec.node_id.clone(),
                        &root,
                        Arc::clone(&self.factory.database),
                        store,
                    )?
                };
                self.service = Some(Arc::new(Mutex::new(service)));
            }
            self.metadata =
                Some(Arc::new(Mutex::new(self.retained.take().ok_or_else(
                    || invalid("cold actual metadata owner absent"),
                )?)));
        }
        let metadata = self.metadata.as_ref().unwrap().clone();
        let id = {
            let store = metadata
                .try_lock()
                .ok_or_else(|| invalid("cold learner metadata busy"))?;
            if !store
                .committed_topology()
                .is_metadata_voter(observed.metadata_leader_id())
            {
                return Err(invalid("cold current leader is absent from retained committed voters; exact fresh snapshot admission unsupported"));
            }
            if registered_catchup {
                store.registered_native_metadata_member_store_id()?
            } else {
                store.registered_native_learner_store_id()?
            }
        };
        if registered_catchup {
            self.validate_registered_metadata_catchup(observed, business, &token)?;
        }
        if self.service.is_none() {
            return Err(invalid("cold metadata lost its retained SAME service"));
        }
        if self.stopping {
            self.listener
                .as_mut()
                .ok_or_else(|| invalid("cold stopping listener custody absent"))?
                .stop_and_drain(&token)?;
            self.listener.take();
            self.stopping = false;
        }
        if self.topology.is_none() {
            self.topology = Some(Arc::new(RwLock::new(current.clone())));
        }
        let topology = self.topology.as_ref().unwrap().clone();
        if self.transport.is_none() {
            self.transport = Some(Arc::new(TcpClusterRelocationTransport::new(
                self.factory.spec.cluster_id.clone(),
                self.factory.spec.node_id.clone(),
                topology.clone(),
                self.network.clone(),
            )?));
        }
        self.transport
            .as_ref()
            .unwrap()
            .install_topology(current.clone())?;
        if self.provider.is_none() {
            self.provider = Some(Arc::new(MetadataReadProvider::new(
                metadata.clone(),
                self.transport.as_ref().unwrap().clone(),
            )?));
        }
        observed.require_live_request(business)?;
        token.check()?;
        if self.listener.is_some() {
            return Ok(id);
        }
        cut("before-bind")?;
        let address: std::net::SocketAddr = current.nodes[&self.factory.spec.node_id]
            .address
            .parse()
            .map_err(|_| invalid("cold learner needs numeric committed endpoint"))?;
        let listener =
            crate::distribution_transport::start_cluster_data_server_with_recovery_runtime(
                address,
                self.network.clone(),
                topology,
                self.service.as_ref().unwrap().clone(),
                None,
                Some(metadata),
                None,
                self.provider.as_ref().cloned(),
                None,
            )?;
        // Retain before checking BOTH cancellation ancestries or injected cuts.
        self.listener = Some(listener);
        let finish = (|| {
            cut("after-bind")?;
            observed.require_live_request(business)?;
            token.check()?;
            if registered_catchup {
                self.validate_registered_metadata_catchup(observed, business, &token)?;
            } else {
                self.factory
                    .enrolled_metadata_context(observed, business, &self.network)?;
            }
            Ok(())
        })();
        if let Err(error) = finish {
            self.listener.as_ref().unwrap().request_shutdown();
            self.stopping = true;
            return Err(error);
        }
        Ok(id)
    }
    pub(crate) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if let Some(supervisor) = self.supervisor.as_mut() {
            supervisor.stop(parent)?;
            self.supervisor.take();
        }
        if let Some(control) = self.control.as_mut() {
            control.stop(parent)?;
        }
        if let Some(listener) = self.listener.as_mut() {
            self.stopping = true;
            listener.stop_and_drain(parent)?;
            self.listener.take();
            self.stopping = false;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_start_with(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        cut: impl FnMut(&'static str) -> Result<()>,
    ) -> Result<Uuid> {
        self.start_with(observed, business, parent, cut)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_factory(
        &self,
    ) -> &NativeUnassignedLearnerFactory {
        &self.factory
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_metadata(
        &self,
    ) -> Option<Arc<Mutex<MetadataConsensusStore>>> {
        self.metadata.clone()
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_custody(&self) -> (bool, bool, bool, bool) {
        (
            self.retained.is_some(),
            self.metadata.is_some(),
            self.listener.is_some(),
            self.stopping,
        )
    }
}
