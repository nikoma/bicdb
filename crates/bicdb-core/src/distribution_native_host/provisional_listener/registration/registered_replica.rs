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
mod first_receive;
mod no_domain;
mod remote_attempt;
mod remote_image;
pub(crate) use closed_fence::NativeAssignedRegisteredClosedFence;
pub(crate) use no_domain::NativeRegisteredSelectedRangeNoDomain;
pub(crate) use remote_attempt::NativeRegisteredRemoteProposalPoint;
pub(crate) use remote_attempt::NativeRegisteredRemoteTransactionRequest;
pub(crate) use remote_attempt::{
    NativeRegisteredRemoteInstallPreflight, NativeRegisteredRemoteJointOwner,
    NativeRegisteredRemoteJointReleaseOwner, NativeRegisteredRemoteJointTarget,
    NativeRegisteredServingDataPolicyOrigin,
};
pub(crate) use remote_image::NativeRegisteredRemoteImageStep;
pub(crate) use remote_image::NativeRegisteredRemoteTransferRequest;
mod assigned_policy;
pub(crate) use assigned_policy::NativeAssignedDataPolicyOrigin;

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

type RegisteredJointTargets =
    Mutex<std::collections::BTreeMap<crate::RangeId, Arc<NativeRegisteredRemoteJointOwner>>>;
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
    joint_targets: Weak<RegisteredJointTargets>,
    first_receive: Weak<crate::distribution_native_host::remote_first::NativeFirstReceiveQueue>,
    first_origin: Option<Weak<crate::distribution_native_host::NativeFirstJoinFreshReceiverOrigin>>,
    remote_install: Weak<RegisteredRemoteInstall>,
}
/// Original Registered install custody shared between the owning host and
/// the same receiver's authenticated step RPC. Sharing changes who may drive
/// one bounded operation, never what that operation must independently prove.
pub(super) struct RegisteredRemoteInstall {
    tables: Mutex<RegisteredRemoteInstallTables>,
    #[cfg(test)]
    drop_next_step_reply: AtomicBool,
}
#[derive(Default)]
pub(super) struct RegisteredRemoteInstallTables {
    /// Set by the owning host once a distinct DATA owner exists; the receiver
    /// path then refuses exactly like the host's own continuation methods.
    /// It lives under the custody lock: a step reads it while it holds the
    /// tables for its whole duration, and the host sets it only while holding
    /// them, so "a step advances install state" and "this host becomes a
    /// DATA owner" exclude each other.
    data_owner: bool,
    /// Denial-only protocol claim for the SAME actual FIRST owner retained
    /// strongly by NativeRegisteredReplicaReceiver.first_attempts. A dead
    /// Weak remains a refusal; neither protocol clears it into a new attempt.
    /// This RAM claim is not a cold reconstruction or mutation permission.
    first_receive_claim: Option<Weak<first_receive::NativeFirstReceiverAttempt>>,
    remote_attempts: std::collections::BTreeMap<
        crate::RangeId,
        remote_attempt::NativeRegisteredRemoteInstallAttempt,
    >,
    remote_images: std::collections::BTreeMap<
        crate::RangeId,
        crate::db::NativeRegisteredRemoteImageInstallation,
    >,
    remote_steps:
        std::collections::BTreeMap<crate::RangeId, remote_attempt::RegisteredTargetInstallStep>,
}
impl RegisteredRemoteInstallTables {
    pub(super) fn close_for_data_owner(&mut self) {
        self.data_owner = true;
    }
    pub(super) fn data_owner_present(&self) -> bool {
        self.data_owner
    }
}
impl RegisteredRemoteInstall {
    /// Nonwaiting: one bounded operation owns the tables; a concurrent caller
    /// is refused instead of queued behind engine or network work.
    pub(super) fn hold(
        &self,
    ) -> Result<parking_lot::MutexGuard<'_, RegisteredRemoteInstallTables>> {
        self.tables
            .try_lock()
            .ok_or_else(|| invalid("Registered remote install custody busy"))
    }
    /// The owning host's transition to a distinct DATA owner takes the same
    /// custody a receiver-driven step holds. Nonwaiting: while a step is in
    /// flight the transition is refused and may simply be retried.
    pub(super) fn hold_for_data_owner(
        &self,
    ) -> Result<parking_lot::MutexGuard<'_, RegisteredRemoteInstallTables>> {
        self.tables.try_lock().ok_or_else(|| {
            invalid("Registered remote install step in flight; retry the DATA owner transition")
        })
    }
    #[cfg(test)]
    pub(super) fn test_drop_next_step_reply(&self) {
        self.drop_next_step_reply.store(true, Ordering::Release);
    }
    #[cfg(test)]
    pub(super) fn test_take_step_reply_loss(&self) -> bool {
        self.drop_next_step_reply.swap(false, Ordering::AcqRel)
    }
}
/// One bounded target-local operation over the original receiver's owners.
pub(super) struct RegisteredRemoteInstallCustody<'a> {
    data_owner: bool,
    backend: Arc<NativeMetadataControlBackend>,
    runtime: Arc<RangeRecoveryRuntime>,
    admission: Arc<NativeRegisteredReplicaAdmission>,
    remote_joint_targets: Arc<RegisteredJointTargets>,
    remote_attempts: &'a mut std::collections::BTreeMap<
        crate::RangeId,
        remote_attempt::NativeRegisteredRemoteInstallAttempt,
    >,
    remote_images: &'a mut std::collections::BTreeMap<
        crate::RangeId,
        crate::db::NativeRegisteredRemoteImageInstallation,
    >,
}
impl NativeRegisteredReplicaAdmission {
    pub(crate) fn receive_first_from_authenticated_dispatch(
        self: &Arc<Self>,
        peer: crate::distribution_consensus::MetadataReadParticipant,
        request: crate::distribution_native_host::NativeFirstReceiveRequestV1,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_host::NativeFirstReceiveReplyV1> {
        self.require_live()?;
        self.require_first_install_open()?;
        self.first_origin
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| invalid("FIRST refuses cold/generic receiver reconstruction"))?
            .require_factory(
                &self
                    .factory
                    .upgrade()
                    .ok_or_else(|| invalid("FIRST original factory ended"))?,
            )?;
        self.first_receive
            .upgrade()
            .ok_or_else(|| invalid("FIRST original queue ended"))?
            .deny_consumed_sequence(&request)?;
        self.admit_first_from_actual_peer(peer, request, parent)
    }
    #[cfg(test)]
    pub(crate) fn test_take_first_reply_loss(&self, sequence: u64) -> bool {
        self.first_receive
            .upgrade()
            .is_some_and(|queue| queue.test_take_reply_loss(sequence))
    }
    #[cfg(test)]
    pub(crate) fn test_keep_original_registration(
        &self,
    ) -> Result<impl std::any::Any + Send + Sync> {
        self.original_hook.test_keep_original_registration()
    }
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
    remote_install: Arc<RegisteredRemoteInstall>,
    remote_joint_targets: Arc<RegisteredJointTargets>,
    first_receive: Arc<crate::distribution_native_host::remote_first::NativeFirstReceiveQueue>,
    first_origin: Option<Arc<crate::distribution_native_host::NativeFirstJoinFreshReceiverOrigin>>,
    first_attempts:
        std::collections::BTreeMap<crate::RangeId, Arc<first_receive::NativeFirstReceiverAttempt>>,
}
#[derive(Clone)]
pub(super) struct RegisteredAssignedParts {
    pub(super) backend: Arc<NativeMetadataControlBackend>,
    pub(super) factory: Arc<NativeUnassignedLearnerFactory>,
    pub(super) runtime: Arc<RangeRecoveryRuntime>,
    pub(super) admission: Arc<NativeRegisteredReplicaAdmission>,
    pub(super) database: ClusterServiceOpenGuard,
    pub(super) store: RangeWriteServiceSlotGuard,
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
    /// The owning host's own continuation over the same shared custody.
    pub(super) fn remote_install_custody<'a>(
        &self,
        tables: &'a mut RegisteredRemoteInstallTables,
        data_owner: bool,
    ) -> RegisteredRemoteInstallCustody<'a> {
        let data_owner = data_owner || tables.data_owner_present();
        RegisteredRemoteInstallCustody {
            data_owner,
            backend: self.backend.clone(),
            runtime: self.runtime.clone(),
            admission: self.admission.clone(),
            remote_joint_targets: self.remote_joint_targets.clone(),
            remote_attempts: &mut tables.remote_attempts,
            remote_images: &mut tables.remote_images,
        }
    }
    pub(super) fn assigned_parts(&self) -> Result<RegisteredAssignedParts> {
        self.admission
            .require_listener(&self.backend.service, &self.runtime)?;
        Ok(RegisteredAssignedParts {
            backend: self.backend.clone(),
            factory: self._factory.clone(),
            runtime: self.runtime.clone(),
            admission: self.admission.clone(),
            database: self.admission.database.clone(),
            store: self.admission.store.clone(),
        })
    }
    /// Close the old immutable socket without dropping the same actual runtime
    /// or metadata owner. This does not create a writer or release a freeze.
    pub(super) fn drain_for_assigned(&mut self, parent: &CancellationToken) -> Result<()> {
        self.closed_fence_ready.store(false, Ordering::Release);
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        if let Some(listener) = self.old_listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        self.listener.take();
        self.old_listener.take();
        parent.check()
    }
    fn prepare(
        backend: Arc<NativeMetadataControlBackend>,
        factory: &Arc<NativeUnassignedLearnerFactory>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
        first_origin: Option<crate::distribution_native_host::NativeFirstJoinFreshReceiverOrigin>,
    ) -> Result<Self> {
        parent.check()?;
        if let Some(origin) = &first_origin {
            origin.require_factory(factory)?;
        }
        let first_origin = first_origin.map(Arc::new);
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
        let remote_joint_targets = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let first_receive =
            Arc::new(crate::distribution_native_host::remote_first::NativeFirstReceiveQueue::new());
        let remote_install = Arc::new(RegisteredRemoteInstall {
            tables: Mutex::new(RegisteredRemoteInstallTables::default()),
            #[cfg(test)]
            drop_next_step_reply: AtomicBool::new(false),
        });
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
            joint_targets: Arc::downgrade(&remote_joint_targets),
            first_receive: Arc::downgrade(&first_receive),
            first_origin: first_origin.as_ref().map(Arc::downgrade),
            remote_install: Arc::downgrade(&remote_install),
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
            remote_install,
            remote_joint_targets,
            first_receive,
            first_origin,
            first_attempts: Default::default(),
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
        self.open_registered_replica_receiver_inner(factory, observed, business, parent, None)
    }
    pub(in crate::distribution_native_host) fn open_registered_replica_receiver_from_join_fresh(
        &mut self,
        origin: crate::distribution_native_host::NativeFirstJoinFreshReceiverOrigin,
        factory: &Arc<NativeUnassignedLearnerFactory>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        origin.require_factory(factory)?;
        if let Some(receiver) = &self.replica {
            receiver
                .first_origin
                .as_ref()
                .ok_or_else(|| invalid("FIRST cannot broaden an existing generic receiver"))?
                .require_same_origin(&origin)?;
        }
        self.open_registered_replica_receiver_inner(
            factory,
            observed,
            business,
            parent,
            Some(origin),
        )
    }
    fn open_registered_replica_receiver_inner(
        &mut self,
        factory: &Arc<NativeUnassignedLearnerFactory>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        first_origin: Option<crate::distribution_native_host::NativeFirstJoinFreshReceiverOrigin>,
    ) -> Result<()> {
        parent.check()?;
        if self.assigned.is_some() {
            return Err(invalid(
                "Assigned DATA owner cannot return to blank Registered mode",
            ));
        }
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
                first_origin,
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
    /// The original receiver's shared install custody, for the host's own
    /// transition to a distinct DATA owner under that custody.
    pub(super) fn registered_remote_install(&self) -> Result<Arc<RegisteredRemoteInstall>> {
        self.replica
            .as_ref()
            .map(|receiver| receiver.remote_install.clone())
            .ok_or_else(|| invalid("actual Registered receiver required"))
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

impl NativeRegisteredReplicaReceiver {
    pub(in super::super) fn capture_remote_policy_read_route(
        &self,
        backend: &Arc<NativeMetadataControlBackend>,
        participant: &Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
        successor: &Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
    ) -> Result<Arc<super::remote_serving_successor_read::NativeRemoteSuccessorReadRegistration>>
    {
        if self.stopping {
            return Err(invalid("stopping actual host cannot retain Policy reader"));
        }
        let runtime = &self.runtime;
        self.admission
            .require_listener(&self.backend.service, runtime)?;
        super::remote_serving_successor_read::NativeRemoteSuccessorReadRegistration::capture(
            backend,
            runtime,
            participant,
            successor,
        )
    }
}
