//! Retained same-service ownership across the enrolled learner's metadata start.
//! Unsupported Prepared cuts quarantine; no initial-all-C or term-zero reset.
use super::*;
use crate::distribution_consensus::MetadataConsensusStore;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_transport::DrainedClusterDataListener;

const DIRECTORY: &str = "native-learner-metadata";
#[derive(Default)]
pub(super) struct NativeLearnerMetadataTransition {
    drained: Option<DrainedClusterDataListener>,
    retained: Option<MetadataConsensusStore>,
    metadata: Option<Arc<Mutex<MetadataConsensusStore>>>,
    topology: Option<Arc<RwLock<ClusterTopology>>>,
    transport: Option<Arc<TcpClusterRelocationTransport>>,
    provider: Option<Arc<MetadataReadProvider>>,
    pub(super) listener: Option<ClusterDataServerHandle>,
    stopping: bool,
    namespace_ids: Option<Vec<(String, Uuid)>>,
}
impl NativeUnassignedLearnerHost {
    /// Current OWN learner MQ is mandatory. Enrollment's audit result is not
    /// accepted as authority, and all network acquisition happens in the caller.
    pub(crate) fn start_owned_metadata(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Uuid> {
        self.start_owned_metadata_with(
            observed,
            business,
            parent,
            |root, database, factory, topology, token, retained| {
                MetadataConsensusStore::initialize_native_learner_owned_retaining(
                    root, database, factory, topology, token, retained,
                )
            },
            |_| Ok(()),
        )
    }

    fn start_owned_metadata_with(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        initialize: impl FnOnce(
            &Path,
            Arc<RwLock<BicDb>>,
            &FreshRangeGenesisAuthority,
            ClusterTopology,
            &CancellationToken,
            &mut Option<MetadataConsensusStore>,
        ) -> Result<()>,
        mut cut: impl FnMut(&'static str) -> Result<()>,
    ) -> Result<Uuid> {
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        self.provider.actual_open.require_live()?;
        let factory = Arc::clone(&self.provider.factory);
        let current =
            factory.enrolled_metadata_context(observed, business, &self.provider.network)?;
        let (metadata_root, namespace_ids, record) = {
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("learner DB busy before owned metadata"))?;
            self.provider.actual_open.require_database(&db)?;
            let mut ids = db
                .collections()
                .into_iter()
                .map(|meta| {
                    meta.local_namespace_id
                        .filter(|id| !id.is_nil())
                        .map(|id| (meta.name, id))
                        .ok_or_else(|| invalid("actual learner namespace identity absent"))
                })
                .collect::<Result<Vec<_>>>()?;
            ids.sort();
            let record = db.native_metadata_initialization_record(
                &factory.spec.cluster_id,
                factory.spec.bootstrap_id,
                &factory.spec.node_id,
                factory.spec.member_incarnation,
            )?;
            (db.data_path().join(DIRECTORY), ids, record)
        };
        if self
            .metadata_transition
            .namespace_ids
            .as_ref()
            .is_some_and(|old| old != &namespace_ids)
        {
            return Err(invalid(
                "learner namespace backing changed during metadata transition",
            ));
        }
        self.metadata_transition
            .namespace_ids
            .get_or_insert(namespace_ids);
        if record.as_ref().is_some_and(|record| !record.registered()) {
            return Err(invalid("Prepared learner metadata initialization is quarantined; no fresh UUID or term-zero reset"));
        }
        observed.require_live_request(business)?;
        token.check()?;
        if self.metadata_transition.drained.is_none() {
            let drained = self
                .listener
                .as_mut()
                .ok_or_else(|| invalid("actual provisional listener owner absent"))?
                .stop_and_drain(&token)?;
            drained.require_service(&self._service)?;
            self.metadata_transition.drained = Some(drained);
        }
        // No service/metadata latch is held by initialize/reopen's DB reads.
        if self.metadata_transition.metadata.is_none() {
            if self.metadata_transition.retained.is_none() {
                // Keep the verified actual-open DB borrow through constructor
                // work. No service/metadata mutex is held. Recursive reads are
                // try-only; competing writers yield a retryable busy refusal.
                let actual_database = factory.database.try_read().ok_or_else(|| {
                    invalid("actual learner DB busy before metadata owner construction")
                })?;
                self.provider
                    .actual_open
                    .require_database(&actual_database)?;
                if record.is_some() {
                    self.metadata_transition.retained =
                        Some(MetadataConsensusStore::reopen_native_owned(
                            &metadata_root,
                            Arc::clone(&factory.database),
                            factory.spec.node_id.clone(),
                            current.clone(),
                            &token,
                        )?);
                } else {
                    // Actual owner slot is filled BEFORE state persistence.
                    // Every error retains it; Prepared remains quarantined.
                    initialize(
                        &metadata_root,
                        Arc::clone(&factory.database),
                        &factory.factory,
                        current.clone(),
                        &token,
                        &mut self.metadata_transition.retained,
                    )?;
                }
                drop(actual_database);
            }
            let store = self
                .metadata_transition
                .retained
                .as_ref()
                .ok_or_else(|| invalid("actual retained metadata owner absent"))?;
            store.registered_native_learner_store_id()?;
            self.metadata_transition.metadata = Some(Arc::new(Mutex::new(
                self.metadata_transition.retained.take().unwrap(),
            )));
        }
        let metadata = Arc::clone(self.metadata_transition.metadata.as_ref().unwrap());
        let store_id = {
            let id = metadata
                .try_lock()
                .ok_or_else(|| invalid("owned learner metadata busy"))?
                .registered_native_learner_store_id()?;
            // Release metadata BEFORE any DB borrow.
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("learner DB busy checking Registered identity"))?;
            self.provider.actual_open.require_database(&db)?;
            let actual = db.read_native_metadata_store(&current, &factory.spec.node_id)?;
            if !actual.registered() || actual.store_id() != id {
                return Err(invalid(
                    "actual protected Registered metadata identity differs",
                ));
            }
            id
        };
        cut("registered")?;
        token.check()?;
        observed.require_live_request(business)?;
        if self.metadata_transition.stopping {
            self.metadata_transition
                .listener
                .as_mut()
                .ok_or_else(|| invalid("stopping metadata listener custody absent"))?
                .stop_and_drain(&token)?;
            self.metadata_transition.listener.take();
            self.metadata_transition.stopping = false;
        }
        if self.metadata_transition.topology.is_none() {
            self.metadata_transition.topology = Some(Arc::new(RwLock::new(current.clone())));
        }
        let topology = Arc::clone(self.metadata_transition.topology.as_ref().unwrap());
        if self.metadata_transition.transport.is_none() {
            self.metadata_transition.transport =
                Some(Arc::new(TcpClusterRelocationTransport::new(
                    factory.spec.cluster_id.clone(),
                    factory.spec.node_id.clone(),
                    Arc::clone(&topology),
                    self.provider.network.clone(),
                )?));
        }
        let transport = Arc::clone(self.metadata_transition.transport.as_ref().unwrap());
        transport.install_topology(current.clone())?;
        if self.metadata_transition.provider.is_none() {
            self.metadata_transition.provider = Some(Arc::new(MetadataReadProvider::new(
                Arc::clone(&metadata),
                transport,
            )?));
        }
        if self.metadata_transition.listener.is_some() {
            // Same-identity live retry: no second listener/store is created.
            observed.require_live_request(business)?;
            token.check()?;
            return Ok(store_id);
        }
        {
            let store = metadata
                .try_lock()
                .ok_or_else(|| invalid("learner metadata busy at drain transition"))?;
            let mut service = self
                ._service
                .try_lock()
                .ok_or_else(|| invalid("drained learner service busy"))?;
            // This path takes NO DB latch while holding metadata/service.
            service.transition_enrolled_learner_metadata_listener(
                &self._service,
                self.metadata_transition.drained.as_ref().unwrap(),
                &factory.database,
                &store,
            )?;
        }
        cut("before-bind")?;
        observed.require_live_request(business)?;
        token.check()?;
        let address: std::net::SocketAddr = current.nodes[&factory.spec.node_id]
            .address
            .parse()
            .map_err(|_| invalid("learner metadata needs numeric registered endpoint"))?;
        let listener =
            crate::distribution_transport::start_cluster_data_server_with_recovery_runtime(
                address,
                self.provider.network.clone(),
                topology,
                Arc::clone(&self._service),
                None,
                Some(metadata),
                None,
                self.metadata_transition.provider.as_ref().map(Arc::clone),
                None,
            )?;
        self.metadata_transition.listener = Some(listener);
        let finish = (|| {
            cut("after-bind")?;
            observed.require_live_request(business)?;
            token.check()?;
            factory.enrolled_metadata_context(observed, business, &self.provider.network)?;
            self.provider.actual_open.require_live()?;
            Ok(())
        })();
        if let Err(error) = finish {
            self.metadata_transition
                .listener
                .as_ref()
                .unwrap()
                .request_shutdown();
            self.metadata_transition.stopping = true;
            return Err(error);
        }
        // The old handle stays stopped. Its captured Some never broadens.
        Ok(store_id)
    }

    /// Explicit stopped-backend restart, never a term-zero reinitialization.
    /// The SAME service/range-store remains owned and permanently metadata-only.
    pub(crate) fn stop_owned_metadata(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if let Some(listener) = self.metadata_transition.listener.as_mut() {
            self.metadata_transition.stopping = true;
            listener.stop_and_drain(parent)?;
            self.metadata_transition.listener.take();
            self.metadata_transition.stopping = false;
        }
        Ok(())
    }
    pub(crate) fn close_stopped_owned_metadata(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.metadata_transition.listener.is_some() || self.metadata_transition.stopping {
            return Err(invalid(
                "actual metadata listener must drain before backend close",
            ));
        }
        if let Some(store) = self.metadata_transition.metadata.as_ref() {
            store
                .try_lock()
                .ok_or_else(|| invalid("metadata busy before close"))?
                .registered_native_learner_store_id()?;
            let expected = if self.metadata_transition.provider.is_some() {
                2
            } else {
                1
            };
            if Arc::strong_count(store) != expected {
                return Err(invalid("actual learner metadata owner still borrowed"));
            }
            self.metadata_transition.provider.take();
            self.metadata_transition.metadata.take();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_start_owned_metadata_with(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        init_cut: impl FnMut(
            crate::distribution_consensus::NativeMetadataInitializationCut,
        ) -> Result<()>,
        transition_cut: impl FnMut(&'static str) -> Result<()>,
    ) -> Result<Uuid> {
        self.start_owned_metadata_with(
            observed,
            business,
            parent,
            |root, database, factory, topology, token, retained| {
                MetadataConsensusStore::test_initialize_native_learner_owned_retaining_with(
                    root, database, factory, topology, token, retained, init_cut,
                )
            },
            transition_cut,
        )
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_owned_metadata(
        &self,
    ) -> Option<Arc<Mutex<MetadataConsensusStore>>> {
        self.metadata_transition.metadata.as_ref().map(Arc::clone)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_transition_custody(
        &self,
    ) -> (bool, bool, bool, bool, bool) {
        (
            self.metadata_transition.drained.is_some(),
            self.metadata_transition.retained.is_some(),
            self.metadata_transition.metadata.is_some(),
            self.metadata_transition.listener.is_some(),
            self.metadata_transition.stopping,
        )
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_old_listener_drained(&self) -> bool {
        self.metadata_transition.drained.is_some()
            && self
                .listener
                .as_ref()
                .is_some_and(|listener| listener.active_connections() == 0)
    }
}
