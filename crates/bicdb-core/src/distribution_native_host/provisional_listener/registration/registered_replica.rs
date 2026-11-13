//! SAME registered learner DB/service/Store, NEW immutable replica-only socket.
//! Listener capability is not native materialization, a writer policy, DATA
//! readiness or a Strong permit. Native image/joint wire/controller remain open.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::{ClusterServiceOpenGuard, RangeWriteServiceSlotGuard};
use crate::distribution_native_host::NativeUnassignedLearnerFactory;
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use std::sync::Weak;

mod closed_fence;
pub(crate) use closed_fence::NativeAssignedRegisteredClosedFence;

#[derive(Clone, Debug, serde::Serialize)]
pub struct NativeRegisteredReplicaReceiverStatus {
    pub receiver_ready: bool,
    pub stopping: bool,
    pub range_store_open_id: Uuid,
    pub metadata_store_id: Uuid,
    pub database_incarnation: String,
    pub birth_id: Uuid,
    pub birth_sha256: String,
    pub writer_admission: &'static str,
    pub range_readiness: &'static str,
    pub next_required_gates: [&'static str; 3],
    pub data_serving: bool,
}

pub(crate) struct NativeRegisteredReplicaAdmission {
    service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    database: ClusterServiceOpenGuard,
    store: RangeWriteServiceSlotGuard,
    live: Weak<AtomicBool>,
    metadata_store_id: Uuid,
    database_incarnation: String,
    birth_id: Uuid,
    birth_sha256: String,
    backend: Weak<NativeMetadataControlBackend>,
    factory: Weak<NativeUnassignedLearnerFactory>,
    original_hook: crate::db::NativeRegisteredReceiverHook,
    closed_fence_ready: Weak<AtomicBool>,
}
impl NativeRegisteredReplicaAdmission {
    pub(crate) fn require_live(&self) -> Result<()> {
        if !self
            .live
            .upgrade()
            .is_some_and(|life| life.load(Ordering::Acquire))
        {
            return Err(invalid(
                "registered replica receiver owner ended or is stopping",
            ));
        }
        self.database.require_live()?;
        self.store.require_live()
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
            return Err(invalid("replica receiver changed actual service/runtime"));
        }
        runtime.require_point_read_service(service, runtime.point_read_local_node_id())
    }
}

pub(super) struct NativeRegisteredReplicaReceiver {
    backend: Arc<NativeMetadataControlBackend>,
    _factory: Arc<NativeUnassignedLearnerFactory>,
    runtime: Arc<RangeRecoveryRuntime>,
    admission: Arc<NativeRegisteredReplicaAdmission>,
    life: Arc<AtomicBool>,
    closed_fence_ready: Arc<AtomicBool>,
    old_listener: Option<ClusterDataServerHandle>,
    drained: Option<DrainedClusterDataListener>,
    listener: Option<ClusterDataServerHandle>,
    stopping: bool,
}
impl Drop for NativeRegisteredReplicaReceiver {
    fn drop(&mut self) {
        self.closed_fence_ready.store(false, Ordering::Release);
        self.life.store(false, Ordering::Release);
        for listener in [&self.old_listener, &self.listener].into_iter().flatten() {
            listener.request_shutdown();
        }
        // No writer hook/coordinator was installed. Existing real connection
        // workers retain their service/runtime until exit; no hook cycle,
        // blocking Drop, blank genesis or release of a foreign writer exists.
    }
}
impl NativeRegisteredReplicaReceiver {
    fn prepare(
        backend: Arc<NativeMetadataControlBackend>,
        factory: &Arc<NativeUnassignedLearnerFactory>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        let metadata_store_id = backend.registered_replica_store_id()?;
        let (database, database_open, store) = {
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("registered receiver service busy"))?;
            (
                service.database(),
                service.listener_open_guard(),
                service.native_replica_listener_store_open_guard(),
            )
        };
        if !Arc::ptr_eq(&database, &factory.database) {
            return Err(invalid(
                "registered receiver must retain SAME JoinFresh factory database",
            ));
        }
        let proof = {
            let metadata = backend.lock_metadata(parent)?;
            metadata.require_listener_database(&database_open)?;
            metadata.observe_owned_native_metadata_member(observed, business, parent)?
        };
        factory.require_owned_metadata_member(&proof, observed, business, network)?;
        let (database_incarnation, birth_id, birth_sha256, original_hook) = {
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("registered receiver store busy before binding"))?;
            let db = database
                .try_read()
                .ok_or_else(|| invalid("registered receiver actual DB busy"))?;
            database_open.require_database(&db)?;
            service.require_native_catalog_database(&db)?;
            if service.native_initial_listener_store_open_id() != store.id() {
                return Err(invalid(
                    "registered receiver actual Store open changed before binding",
                ));
            }
            backend.require_database(&db, observed.committed_topology(business)?)?;
            if db.commit_admission_ready() {
                return Err(invalid("replica-only transition refuses an already installed writer; no hook is created or replaced"));
            }
            let native = db.native_recovery_admission(
                &factory.spec.cluster_id,
                factory.spec.bootstrap_id,
                &factory.spec.node_id,
                factory.spec.member_incarnation,
            )?;
            if native.birth_id() != factory.actual_birth().birth_id()
                || native.birth_sha256() != factory.actual_birth().birth_sha256()
                || native.writer_enforcement_version() != 1
                || native.abort_protocol_version() != 1
            {
                return Err(invalid("registered receiver actual native birth changed"));
            }
            (
                db.incarnation_id().to_string(),
                native.birth_id(),
                native.birth_sha256().to_string(),
                db.capture_registered_receiver_hook()?,
            )
        };
        {
            let metadata = backend.lock_metadata(parent)?;
            proof.require_current(&metadata)?;
            if proof.store_id() != metadata_store_id {
                return Err(invalid(
                    "registered receiver protected metadata StoreUUID changed",
                ));
            }
        }
        observed.require_live_request(business)?;
        let runtime = Arc::new(
            RangeRecoveryRuntime::new_cancellable(
                backend.service.clone(),
                backend.provider.clone(),
                parent,
            )?
            .with_participant_transport(backend.transport.clone()),
        );
        let life = Arc::new(AtomicBool::new(true));
        let closed_fence_ready = Arc::new(AtomicBool::new(false));
        let admission = Arc::new(NativeRegisteredReplicaAdmission {
            service: Arc::downgrade(&backend.service),
            runtime: Arc::downgrade(&runtime),
            database: database_open,
            store,
            live: Arc::downgrade(&life),
            metadata_store_id,
            database_incarnation,
            birth_id,
            birth_sha256,
            backend: Arc::downgrade(&backend),
            factory: Arc::downgrade(factory),
            original_hook,
            closed_fence_ready: Arc::downgrade(&closed_fence_ready),
        });
        admission.require_listener(&backend.service, &runtime)?;
        parent.check()?;
        Ok(Self {
            backend,
            _factory: factory.clone(),
            runtime,
            admission,
            life,
            closed_fence_ready,
            old_listener: None,
            drained: None,
            listener: None,
            stopping: false,
        })
    }
    fn advance(
        &mut self,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.stopping {
            return Err(invalid("stopping replica receiver cannot restart"));
        }
        self.admission.require_live()?;
        if self.listener.is_some() {
            parent.check()?;
            self.admission.require_live()?;
            self.closed_fence_ready.store(true, Ordering::Release);
            return Ok(());
        }
        if self.drained.is_none() {
            self.drained = Some(
                self.old_listener
                    .as_mut()
                    .ok_or_else(|| invalid("actual old control listener missing"))?
                    .stop_and_drain(parent)?,
            );
        }
        let drained = self.drained.as_ref().unwrap();
        drained.require_service(&self.backend.service)?;
        drained.require_store(self.admission.store.id())?;
        self.old_listener.take();
        let address: std::net::SocketAddr = {
            let store = self.backend.lock_metadata(parent)?;
            let member = store
                .committed_topology()
                .nodes
                .get(&self.backend.node_id)
                .ok_or_else(|| invalid("registered receiver member absent"))?;
            if member.lifecycle != ClusterNodeLifecycle::Active
                || member.metadata_role != MetadataMemberRole::Voter
                || member.native_store_birth_v1.as_ref().is_none_or(|birth| {
                    birth.birth_id() != self.admission.birth_id
                        || birth.birth_sha256() != self.admission.birth_sha256
                })
            {
                return Err(invalid(
                    "registered receiver current committed member/birth changed before bind",
                ));
            }
            member
                .address
                .parse()
                .map_err(|_| invalid("registered receiver needs numeric own endpoint"))?
        };
        self.listener = Some(
            crate::distribution_transport::start_cluster_registered_replica_server(
                address,
                network.clone(),
                self.backend.topology.clone(),
                self.backend.service.clone(),
                self.backend.metadata.clone(),
                self.backend.provider.clone(),
                self.runtime.clone(),
                self.admission.clone(),
            )?,
        );
        parent.check()?;
        self.admission.require_live()?;
        self.closed_fence_ready.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) fn is_running(&self) -> bool {
        !self.stopping && self.listener.is_some()
    }
    pub(super) fn status(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeRegisteredReplicaReceiverStatus> {
        parent.check()?;
        Ok(NativeRegisteredReplicaReceiverStatus {
            receiver_ready: self.is_running() && self.admission.require_live().is_ok(),
            stopping: self.stopping,
            range_store_open_id: self.admission.store.id(),
            metadata_store_id: self.admission.metadata_store_id,
            database_incarnation: self.admission.database_incarnation.clone(),
            birth_id: self.admission.birth_id,
            birth_sha256: self.admission.birth_sha256.clone(),
            writer_admission: "not installed by replica-only owner",
            range_readiness: "UNKNOWN: no assigned image/native materialization or serving proof",
            next_required_gates: [
                "native_placement_controller",
                "authenticated_native_image_joint_wire",
                "actual_image_joint_open_and_writer_admission",
            ],
            data_serving: false,
        })
    }
    pub(super) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.stopping = true;
        self.closed_fence_ready.store(false, Ordering::Release);
        self.life.store(false, Ordering::Release);
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        if let Some(listener) = self.old_listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        while self.runtime.active_contexts()? != 0 {
            parent.check()?;
            std::thread::sleep(Duration::from_millis(5));
        }
        self.listener.take();
        self.old_listener.take();
        Ok(())
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn open_registered_replica_receiver(
        &mut self,
        factory: &Arc<NativeUnassignedLearnerFactory>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.data.is_some() {
            return Err(invalid(
                "INITIAL DATA owner cannot become a blank registered receiver",
            ));
        }
        if self.replica.is_none() {
            let mut owner = NativeRegisteredReplicaReceiver::prepare(
                self.backend.clone(),
                factory,
                observed,
                business,
                &self.network,
                parent,
            )?;
            owner.old_listener = Some(self.listener.take().ok_or_else(|| {
                invalid(
                    "actual running metadata control listener required before replica transition",
                )
            })?);
            self.replica = Some(owner); // Retain BEFORE drain/bind/cancel.
        }
        self.replica
            .as_mut()
            .unwrap()
            .advance(&self.network, parent)
    }
    pub(in crate::distribution_native_host) fn replica_receiver_status(
        &self,
        parent: &CancellationToken,
    ) -> Result<Option<NativeRegisteredReplicaReceiverStatus>> {
        self.replica
            .as_ref()
            .map(|receiver| receiver.status(parent))
            .transpose()
    }
    pub(in crate::distribution_native_host) fn has_replica_receiver(&self) -> bool {
        self.replica.is_some()
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_registered_receiver_parts(
        &self,
    ) -> Result<(
        Arc<NativeRegisteredReplicaAdmission>,
        Arc<RangeRecoveryRuntime>,
    )> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual receiver absent"))?;
        receiver
            .admission
            .require_listener(&receiver.backend.service, &receiver.runtime)?;
        Ok((receiver.admission.clone(), receiver.runtime.clone()))
    }
}
