//! SAME original Registered image target, distinct CLOSED retained DATA owner.
//! This phase installs necessary policy only. It cannot fabricate INITIAL
//! genesis, a recovered epoch, Ready/APP/Strong publication or a cold owner.
use super::*;
use crate::db::{NativeAssignedWriterInstallAttempt, NativeWriterHostState};
use crate::distribution_data::{
    ClusterServiceOpenGuard, NativeJointServingLease, RangeWriteServiceSlotGuard,
};
use crate::distribution_range_consensus::RangeWriteCoordinator;
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use std::sync::Weak;
mod cleanup;
use cleanup::{AssignedCleanup, AssignedCleanupSender};

pub(crate) struct NativeAssignedDataAdmission {
    service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    database: ClusterServiceOpenGuard,
    slot: RangeWriteServiceSlotGuard,
    writer: Arc<NativeWriterHostState>,
    root: PathBuf,
    life: Weak<AtomicBool>,
}
impl NativeAssignedDataAdmission {
    pub(crate) fn require_routing_service(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        store: Uuid,
    ) -> Result<()> {
        self.require_live()?;
        if store != self.slot.id()
            || self
                .service
                .upgrade()
                .is_none_or(|s| !Arc::ptr_eq(&s, service))
        {
            return Err(invalid("Assigned routing changed actual service/Store"));
        }
        Ok(())
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if !self
            .life
            .upgrade()
            .is_some_and(|l| l.load(Ordering::Acquire))
        {
            return Err(invalid("assigned CLOSED owner stopped or ended"));
        }
        self.database.require_live()?;
        self.slot.require_live()?;
        self.writer.require_store(&self.root, self.slot.id())
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
            .is_none_or(|s| !Arc::ptr_eq(&s, service))
            || self
                .runtime
                .upgrade()
                .is_none_or(|r| !Arc::ptr_eq(&r, runtime))
        {
            return Err(invalid("assigned CLOSED actual service/runtime changed"));
        }
        runtime.require_point_read_service(service, runtime.point_read_local_node_id())
    }
}
pub(super) struct NativeAssignedDataHost {
    parts: registered_replica::RegisteredAssignedParts,
    coordinator: Option<Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>>,
    attempt: Option<NativeAssignedWriterInstallAttempt>,
    image: Option<NativeJointServingLease>,
    range: crate::RangeDescriptor,
    admission: Option<Arc<NativeAssignedDataAdmission>>,
    listener: Option<ClusterDataServerHandle>,
    life: Arc<AtomicBool>,
    stopping: bool,
    cleanup: Option<AssignedCleanupSender>,
}
impl NativeAssignedDataHost {
    fn new(
        parts: registered_replica::RegisteredAssignedParts,
        image: NativeJointServingLease,
    ) -> Result<Self> {
        let NativeJointServingLease::Image { image: actual, .. } = &image else {
            return Err(invalid(
                "Assigned DATA owner requires a real target Image lease",
            ));
        };
        if !parts.factory.spec.initial_ranges.is_empty()
            || actual.native_admission().local_node_id() != &parts.backend.node_id
        {
            return Err(invalid(
                "Assigned DATA must retain the genuine Registered target birth",
            ));
        }
        let range = actual.base().intended_activation_range().clone();
        Ok(Self {
            parts,
            coordinator: None,
            attempt: None,
            image: Some(image),
            range,
            admission: None,
            listener: None,
            life: Arc::new(AtomicBool::new(true)),
            stopping: false,
            cleanup: None,
        })
    }
    fn install_closed(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if self.stopping {
            return Err(invalid("stopping Assigned DATA owner cannot restart"));
        }
        self.parts.database.require_live()?;
        self.parts.store.require_live()?;
        if self.cleanup.is_none() {
            self.cleanup = Some(AssignedCleanupSender::ensure(parent)?);
        }
        if self
            .attempt
            .as_ref()
            .and_then(|a| a.installed_guard())
            .is_some()
        {
            return Ok(());
        }
        let route = self
            .parts
            .backend
            .lock_metadata(parent)?
            .status()
            .leader_id
            .ok_or_else(|| invalid("Assigned policy metadata leader unknown"))?;
        let business = Uuid::now_v7();
        let observed = self
            .parts
            .backend
            .transport
            .acquire_host_metadata_observation(&route, business, Duration::from_secs(5), parent)?;
        self.parts
            .backend
            .transport
            .install_topology(observed.committed_topology(business)?.clone())?;
        let NativeJointServingLease::Image { image, frozen } = self
            .image
            .as_ref()
            .ok_or_else(|| invalid("Assigned image custody already transferred"))?
        else {
            unreachable!()
        };
        let (origin, policy) = self.parts.admission.prepare_assigned_data_policy(
            image.clone(),
            frozen.clone(),
            observed,
            business,
            parent,
        )?;
        if origin.range() != &self.range {
            return Err(invalid("Assigned policy range differs from actual image"));
        }
        if self.coordinator.is_none() {
            // This constructor only requires actual native/schema custody; it
            // does not consume INITIAL genesis or set any recovered epoch.
            self.coordinator = Some(Arc::new(RangeWriteCoordinator::new_assigned_schema_fenced(
                policy.facts().cluster_id.clone(),
                self.parts.backend.node_id.clone(),
                self.parts.backend.topology.clone(),
                self.parts.backend.service.clone(),
                self.parts.backend.transport.clone(),
                &origin,
            )?));
        }
        let coordinator = self.coordinator.as_ref().unwrap();
        let database = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Assigned policy DB busy"))?;
        if self.attempt.is_none() {
            self.attempt = Some(NativeAssignedWriterInstallAttempt::begin(
                &database,
                coordinator,
                &origin,
                &policy,
            )?);
        }
        // Owner, coordinator and attempt already retained before publication.
        let result =
            self.attempt
                .as_mut()
                .unwrap()
                .advance(&database, coordinator, &origin, &policy);
        drop(database);
        result?;
        origin.require_owned_metadata_current()
    }
    fn bind_closed(
        &mut self,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.stopping {
            return Err(invalid("Assigned DATA owner stopping"));
        }
        let guard = self
            .attempt
            .as_ref()
            .and_then(|a| a.installed_guard())
            .ok_or_else(|| invalid("actual Assigned policy not installed"))?;
        {
            let database = self
                .parts
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("Assigned current policy DB busy"))?;
            guard.require_for_coordinator(
                &database,
                &self.range,
                self.coordinator.as_ref().unwrap(),
            )?;
        }
        if self.admission.is_none() {
            let service = self
                .parts
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Assigned service busy"))?;
            let db = self
                .parts
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("Assigned DB busy"))?;
            let (writer, root) = service.retain_assigned_data_writer(
                &db,
                self.coordinator.as_ref().unwrap(),
                guard,
                &self.range,
            )?;
            self.admission = Some(Arc::new(NativeAssignedDataAdmission {
                service: Arc::downgrade(&self.parts.backend.service),
                runtime: Arc::downgrade(&self.parts.runtime),
                database: self.parts.database.clone(),
                slot: self.parts.store.clone(),
                writer,
                root,
                life: Arc::downgrade(&self.life),
            }));
        }
        let admission = self.admission.as_ref().unwrap();
        admission.require_live()?;
        if self.listener.is_none() {
            let address = {
                let metadata = self.parts.backend.lock_metadata(parent)?;
                metadata.require_listener_database(&self.parts.database)?;
                let member = metadata
                    .committed_topology()
                    .nodes
                    .get(&self.parts.backend.node_id)
                    .ok_or_else(|| invalid("Assigned actual member missing"))?;
                if member.lifecycle != ClusterNodeLifecycle::Active
                    || member.metadata_role != MetadataMemberRole::Voter
                    || member.incarnation != self.parts.factory.spec.member_incarnation
                    || member.native_store_birth_v1.as_ref().is_none_or(|b| {
                        b.birth_id() != self.parts.factory.actual_birth().birth_id()
                            || b.birth_sha256() != self.parts.factory.actual_birth().birth_sha256()
                    })
                {
                    return Err(invalid(
                        "Assigned current owned metadata member/birth changed",
                    ));
                }
                member
                    .address
                    .parse()
                    .map_err(|_| invalid("Assigned requires numeric local listener endpoint"))?
            };
            self.listener = Some(
                crate::distribution_transport::start_cluster_assigned_closed_data_server(
                    address,
                    network.clone(),
                    self.parts.backend.topology.clone(),
                    self.parts.backend.service.clone(),
                    self.parts.backend.metadata.clone(),
                    self.parts.backend.provider.clone(),
                    self.parts.runtime.clone(),
                    admission.clone(),
                )?,
            );
        }
        parent.check()?;
        admission.require_live()?;
        let database = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Assigned policy DB busy after bind"))?;
        guard.require_for_coordinator(&database, &self.range, self.coordinator.as_ref().unwrap())
    }
    pub(super) fn is_running(&self) -> bool {
        !self.stopping && self.listener.is_some()
    }
    pub(super) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.stopping = true;
        self.life.store(false, Ordering::Release);
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        loop {
            parent.check()?;
            if self.parts.runtime.active_contexts()? == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if let Some(attempt) = &self.attempt {
            if attempt.unresolved_native_publication() {
                return Err(invalid("Assigned unknown native policy retained CLOSED; explicit reconciliation required"));
            }
            if let Some(guard) = attempt.installed_guard() {
                let database = self
                    .parts
                    .factory
                    .database
                    .try_read()
                    .ok_or_else(|| invalid("Assigned stop DB busy"))?;
                guard.retire_if_current(&database, self.coordinator.as_ref().unwrap())?;
            }
        }
        self.listener.take();
        Ok(())
    }
}
impl Drop for NativeAssignedDataHost {
    fn drop(&mut self) {
        self.stopping = true;
        self.life.store(false, Ordering::Release);
        if let Some(listener) = &self.listener {
            listener.request_shutdown();
        }
        if self.attempt.is_some() {
            let cleanup = AssignedCleanup::take(self);
            if let Some(sender) = &self.cleanup {
                sender.defer(cleanup);
            } else {
                cleanup.quarantine();
            }
        }
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn install_assigned_closed_data_policy(
        &mut self,
        image: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.data.is_some() || self.assigned.is_some() {
            return Err(invalid(
                "distinct Assigned owner already exists; use its retained continuation",
            ));
        }
        let parts = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver required"))?
            .assigned_parts()?;
        self.assigned = Some(NativeAssignedDataHost::new(parts, image)?);
        self.advance_assigned_closed_data_policy(parent)
    }
    pub(in crate::distribution_native_host) fn advance_assigned_closed_data_policy(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        let owner = self
            .assigned
            .as_mut()
            .ok_or_else(|| invalid("retained Assigned owner absent"))?;
        owner.install_closed(parent)?;
        self.replica
            .as_mut()
            .ok_or_else(|| invalid("original Registered drain owner absent"))?
            .drain_for_assigned(parent)?;
        owner.bind_closed(&self.network, parent)
    }
    /// Exclusive custody transfer, not a serving grant. The real consumer must
    /// still obtain fresh committed authority and actual NEW preparation.
    pub(in crate::distribution_native_host) fn take_assigned_joint_image_lease(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<NativeJointServingLease> {
        parent.check()?;
        let owner = self
            .assigned
            .as_mut()
            .ok_or_else(|| invalid("Assigned owner absent"))?;
        owner
            .admission
            .as_ref()
            .ok_or_else(|| invalid("Assigned CLOSED listener not bound"))?
            .require_live()?;
        let guard = owner
            .attempt
            .as_ref()
            .and_then(|a| a.installed_guard())
            .ok_or_else(|| invalid("Assigned installed policy absent"))?;
        let database = owner
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Assigned transfer DB busy"))?;
        guard.require_for_coordinator(
            &database,
            &owner.range,
            owner.coordinator.as_ref().unwrap(),
        )?;
        parent.check()?;
        owner
            .image
            .take()
            .ok_or_else(|| invalid("Assigned real image custody already transferred"))
    }
}
