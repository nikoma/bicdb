//! Native CONFIG ingress and a finite queue consumer. No data surface.
use super::*;
use crate::distribution::{ClusterBootstrapSnapshot, ClusterNode, DistributionConfig};
use crate::distribution_consensus::{HostMetadataReadObservation, MetadataConsensusRole};
use crate::distribution_transport::{
    DrainedClusterDataListener, NativeMetadataRegistrationIngress,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

mod assigned_data;
mod initial_data;
pub(crate) use assigned_data::NativeAssignedDataAdmission;
use assigned_data::NativeAssignedDataHost;
mod data_owner;
mod joined_data;
pub(crate) use data_owner::NativeDataOwnerAdmission;
use joined_data::NativeJoinedDataHost;
pub use joined_data::NativeJoinedDataStatus;
pub(crate) use joined_data::NativeJoinedRemoteInstallPreflight;
pub(crate) use joined_data::NativeJoinedRemoteMaterializedOwner;
pub(crate) use joined_data::{
    NativeJoinedRangeReceiveOrigin, NativeJoinedRangeReceivePreflight,
    NativeJoinedRangeReceiveRequest,
};
mod registered_replica;
mod writer_data_domain;
use registered_replica::NativeRegisteredReplicaReceiver;
pub use registered_replica::NativeRegisteredReplicaReceiverStatus;
pub(crate) use registered_replica::{
    NativeAssignedDataPolicyOrigin, NativeAssignedRegisteredClosedFence,
    NativeRegisteredRemoteImageStep, NativeRegisteredRemoteInstallPreflight,
    NativeRegisteredRemoteJointOwner, NativeRegisteredRemoteJointReleaseOwner,
    NativeRegisteredRemoteJointTarget, NativeRegisteredRemoteProposalPoint,
    NativeRegisteredRemoteTransactionRequest, NativeRegisteredRemoteTransferRequest,
    NativeRegisteredReplicaAdmission, NativeRegisteredSelectedRangeNoDomain,
    NativeRegisteredServingDataPolicyOrigin,
};
mod maintenance;
use initial_data::NativeInitialDataHost;
#[cfg(test)]
pub(crate) use initial_data::{
    after_remote_native_old_wal, before_native_old_wal, CancelOldOwnBeforeNativeWal,
    CancelRemoteOldAfterNativeWal,
};
pub(crate) use initial_data::{
    NativeInitialDataAdmission, NativeInitialOldConstraintStep, NativeInitialOldStep,
};
pub use initial_data::{NativeInitialDataRangeStatus, NativeInitialDataStatus};
mod schema_export;
pub(crate) use maintenance::NativeMetadataMaintenanceReport;

const MAX_PENDING: usize = 32;
const MAX_QUEUE_BYTES: usize = 1024 * 1024;

pub(crate) struct NativeMetadataRegistrationController {
    backend: Arc<NativeMetadataControlBackend>,
    config: DistributionConfig,
    store_open_id: Uuid,
    actual_open: crate::distribution_data::ClusterServiceOpenGuard,
    queue: Mutex<BTreeMap<ClusterNodeId, Arc<NativeMetadataRegistrationIngress>>>,
    driving: AtomicBool,
    maintenance: Mutex<maintenance::NativeMetadataMaintenanceState>,
    #[cfg(test)]
    test_fail_after_proposal: AtomicBool,
}
impl std::fmt::Debug for NativeMetadataRegistrationController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeMetadataRegistrationController")
            .finish_non_exhaustive()
    }
}
impl NativeMetadataRegistrationController {
    fn new(
        backend: Arc<NativeMetadataControlBackend>,
        config: DistributionConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        config.validate()?;
        let (actual_open, store_open_id) = {
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("native control service busy before construction"))?;
            (
                service.listener_open_guard(),
                service.native_initial_listener_store_open_id(),
            )
        };
        let store = backend.lock_metadata(parent)?;
        store.require_listener_database(&actual_open)?;
        let topology = store.committed_topology();
        let member = topology
            .nodes
            .get(&config.node_id)
            .ok_or_else(|| invalid("native control template member absent"))?;
        if config.node_id != backend.node_id
            || config.node_incarnation != member.incarnation
            || config.node_address != member.address
            || config.node_capacity_bytes != member.capacity_bytes
            || config.replication_factor != topology.replication_factor
            || config.node_tls_certificate_sha256 != member.tls_certificate_sha256
            || config.range_image_bootstrap_id.is_none()
            || config.transport.dev_localhost_plaintext
        {
            return Err(invalid(
                "native control template disagrees with actual local CONFIG",
            ));
        }
        crate::DistributionStore::native_registration_plan(config.clone(), topology.clone())?;
        drop(store);
        Ok(Self {
            backend,
            config,
            store_open_id,
            actual_open,
            queue: Mutex::new(BTreeMap::new()),
            driving: AtomicBool::new(false),
            maintenance: Mutex::new(Default::default()),
            #[cfg(test)]
            test_fail_after_proposal: AtomicBool::new(false),
        })
    }
    pub(crate) fn require_service(
        &self,
        actual: &Arc<Mutex<ClusterDataNodeService>>,
        store_open_id: Uuid,
    ) -> Result<()> {
        if !Arc::ptr_eq(actual, &self.backend.service) || store_open_id != self.store_open_id {
            return Err(invalid(
                "native controller belongs to another actual service/store open",
            ));
        }
        Ok(())
    }
    pub(crate) fn bootstrap(&self, parent: &CancellationToken) -> Result<ClusterBootstrapSnapshot> {
        parent.check()?;
        let actual = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("native bootstrap service busy"))?
            .listener_open_guard();
        let store = self.backend.lock_metadata(parent)?;
        store.require_listener_database(&actual)?;
        let status = store.status();
        let snapshot = ClusterBootstrapSnapshot {
            topology: store.committed_topology().clone(),
            config_template: self.config.clone(),
            metadata_leader_id: if status.role == MetadataConsensusRole::Leader {
                Some(status.node_id)
            } else {
                status.leader_id
            },
        };
        snapshot.validate()?;
        parent.check()?;
        Ok(snapshot)
    }
    /// INITIAL-only committed routing DATA before the DATA service latch.
    /// The real captured open/current slot/owned MStore remain mandatory; no
    /// quorum/readiness/write grant or cached schema authority is returned.
    pub(crate) fn bootstrap_initial_data(
        &self,
        admission: &Arc<NativeInitialDataAdmission>,
        parent: &CancellationToken,
    ) -> Result<ClusterBootstrapSnapshot> {
        self.bootstrap_data_owner(
            &NativeDataOwnerAdmission::Initial(admission.clone()),
            parent,
        )
    }
    /// Routing DATA only for either actual retained owner. No role or current
    /// read/write permission is derived from this common necessary fence.
    pub(crate) fn bootstrap_data_owner(
        &self,
        admission: &NativeDataOwnerAdmission,
        parent: &CancellationToken,
    ) -> Result<ClusterBootstrapSnapshot> {
        parent.check()?;
        admission.require_routing_service(&self.backend.service, self.store_open_id)?;
        self.actual_open.require_live()?;
        let store = self.backend.lock_metadata(parent)?;
        store.require_listener_database(&self.actual_open)?;
        let status = store.status();
        let snapshot = ClusterBootstrapSnapshot {
            topology: store.committed_topology().clone(),
            config_template: self.config.clone(),
            metadata_leader_id: if status.role == MetadataConsensusRole::Leader {
                Some(status.node_id)
            } else {
                status.leader_id
            },
        };
        snapshot.validate()?;
        // A retained removed Store or ended owner must not keep this route
        // alive across the read. Same metadata lock, no service/DB reborrow.
        store.require_listener_database(&self.actual_open)?;
        self.actual_open.require_live()?;
        admission.require_routing_service(&self.backend.service, self.store_open_id)?;
        parent.check()?;
        Ok(snapshot)
    }
    fn replicate_registration(
        &self,
        required: Option<u64>,
        parent: &CancellationToken,
    ) -> Result<()> {
        // New unbound learners have no owned metadata backend yet. They cannot
        // contribute an ACK and are not contacted to commit their registration.
        for _ in 0..3 {
            let peers = {
                let store = self.backend.lock_metadata(parent)?;
                if store.status().role != MetadataConsensusRole::Leader {
                    return Err(invalid("native registration metadata leader changed"));
                }
                if required.is_none_or(|index| store.status().commit_index >= index) {
                    break;
                }
                store
                    .committed_topology()
                    .nodes
                    .values()
                    .filter(|n| {
                        n.lifecycle == ClusterNodeLifecycle::Active
                            && n.metadata_role == MetadataMemberRole::Voter
                            && n.id != self.backend.node_id
                    })
                    .map(|n| n.id.clone())
                    .collect::<Vec<_>>()
            };
            for peer in peers {
                parent.check()?;
                let request = {
                    let store = self.backend.lock_metadata(parent)?;
                    if store.status().role != MetadataConsensusRole::Leader {
                        return Err(invalid("native registration metadata leader changed"));
                    }
                    if required.is_none_or(|index| store.status().commit_index >= index) {
                        break;
                    }
                    if !store
                        .committed_topology()
                        .nodes
                        .get(&peer)
                        .is_some_and(|n| {
                            n.lifecycle == ClusterNodeLifecycle::Active
                                && n.metadata_role == MetadataMemberRole::Voter
                        })
                    {
                        continue;
                    }
                    store.append_request_for_peer(&peer, 16)?
                };
                match self
                    .backend
                    .transport
                    .append_metadata_cancellable(&peer, request, parent)
                {
                    Ok(reply) => {
                        self.backend
                            .lock_metadata(parent)?
                            .record_append_response(&peer, &reply)?;
                    }
                    Err(_) => {
                        parent.check()?;
                    }
                }
            }
        }
        self.backend.publish_committed_metadata(parent)
    }
    pub(crate) fn register(
        &self,
        ingress: NativeMetadataRegistrationIngress,
    ) -> Result<crate::ClusterTopology> {
        let token = ingress
            .token()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let snapshot = self.bootstrap(&token)?;
        ingress.require_current(&snapshot.topology)?;
        if snapshot.metadata_leader_id.as_ref() != Some(&self.backend.node_id) {
            return Err(invalid(&format!(
                "native registration must route to current metadata leader {:?}",
                snapshot.metadata_leader_id
            )));
        }
        let id = ingress.node().id.clone();
        let ingress = Arc::new(ingress);
        {
            let mut queue = self
                .queue
                .try_lock()
                .ok_or_else(|| invalid("native registration queue busy"))?;
            queue.retain(|_, pending| pending.token().check().is_ok());
            if let Some(pending) = queue.get(&id) {
                if !crate::distribution_consensus::same_native_registration(
                    pending.node(),
                    ingress.node(),
                ) {
                    return Err(invalid("conflicting native registration already queued"));
                }
            }
            if !queue.contains_key(&id) && queue.len() >= MAX_PENDING {
                return Err(invalid("native registration queue full"));
            }
            let mut bytes = ingress.queue_bytes()?;
            for (key, value) in queue.iter() {
                if key != &id {
                    bytes = bytes
                        .checked_add(value.queue_bytes()?)
                        .ok_or_else(|| invalid("native registration queue size overflow"))?;
                }
            }
            if bytes > MAX_QUEUE_BYTES {
                return Err(invalid("native registration queue byte bound exceeded"));
            }
            // A fresh authenticated identical retry replaces only unproposed
            // ingress provenance; any durable CONFIG remains in the MStore.
            queue.insert(id.clone(), Arc::clone(&ingress));
        }
        if self
            .driving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(snapshot.topology); // COMMITTED data, never "joined".
        }
        struct Driving<'a>(&'a AtomicBool);
        impl Drop for Driving<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _operation = Driving(&self.driving);
        let leader = &self.backend.node_id;
        let business = Uuid::now_v7();
        let current = self.backend.transport.acquire_host_metadata_observation(
            leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let actual = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("native registration service busy"))?
            .listener_open_guard();
        let proposed = {
            let mut store = self.backend.lock_metadata(&token)?;
            store.require_listener_database(&actual)?;
            store.propose_native_metadata_registration(
                &ingress,
                &current,
                business,
                &self.config,
            )?
        };
        #[cfg(test)]
        if proposed.is_some() && self.test_fail_after_proposal.swap(false, Ordering::AcqRel) {
            return Err(invalid(
                "injected interruption after actual durable registration CONFIG",
            ));
        }
        self.replicate_registration(proposed.as_ref().map(|p| p.index), &token)?;
        let final_business = Uuid::now_v7();
        let final_mq = self.backend.transport.acquire_host_metadata_observation(
            leader,
            final_business,
            Duration::from_secs(5),
            &token,
        )?;
        final_mq.require_live_request(final_business)?;
        token.check()?;
        let store = self.backend.lock_metadata(&token)?;
        store.require_listener_database(&actual)?;
        let status = store.status();
        if status.role != MetadataConsensusRole::Leader
            || status.current_term != final_mq.term()
            || final_mq.metadata_leader_id() != leader
            || final_mq.requester_node_id() != leader
            || final_mq.read_index() != status.commit_index
            || final_mq.committed_topology(final_business)? != store.committed_topology()
            || final_mq.restore_activation(final_business)? != store.committed_restore_activation()
            || proposed
                .as_ref()
                .is_some_and(|p| status.commit_index < p.index)
            || !store.committed_topology().nodes.get(&id).is_some_and(|n| {
                crate::distribution_consensus::same_native_registration(n, ingress.node())
            })
        {
            return Err(invalid(
                "native learner registration lacks actual committed old-voter CONFIG",
            ));
        }
        let committed = store.committed_topology().clone();
        drop(store);
        token.check()?;
        let mut queue = self
            .queue
            .try_lock()
            .ok_or_else(|| invalid("native registration queue busy after commit"))?;
        if queue.get(&id).is_some_and(|v| Arc::ptr_eq(v, &ingress)) {
            queue.remove(&id);
        }
        Ok(committed)
    }
}

/// Owns all phase inputs before fallible drain/bind. The old socket cannot
/// broaden in place and failed advance never drops the actual metadata owner.
pub(crate) struct NativeMetadataControlHost {
    backend: Arc<NativeMetadataControlBackend>,
    data: Option<NativeInitialDataHost>,
    data_process: std::sync::OnceLock<CancellationToken>,
    replica: Option<NativeRegisteredReplicaReceiver>,
    assigned: Option<NativeAssignedDataHost>,
    joined: Option<NativeJoinedDataHost>,
    old_listener: Option<ClusterDataServerHandle>,
    drained: Option<DrainedClusterDataListener>,
    controller: Option<Arc<NativeMetadataRegistrationController>>,
    listener: Option<ClusterDataServerHandle>,
    network: ClusterNetworkTransportConfig,
    config: DistributionConfig,
}
impl NativeMetadataControlHost {
    pub(crate) fn new(
        initial: NativeInitialConsensusLaunch,
        listener: ClusterDataServerHandle,
        network: ClusterNetworkTransportConfig,
        config: DistributionConfig,
    ) -> Self {
        Self {
            backend: Arc::new(NativeMetadataControlBackend::from_initial(Arc::new(
                initial,
            ))),
            data: None,
            data_process: std::sync::OnceLock::new(),
            replica: None,
            assigned: None,
            joined: None,
            old_listener: Some(listener),
            drained: None,
            controller: None,
            listener: None,
            network,
            config,
        }
    }
    pub(in crate::distribution_native_host) fn from_registered_member(
        backend: NativeMetadataControlBackend,
        listener: ClusterDataServerHandle,
        network: ClusterNetworkTransportConfig,
        config: DistributionConfig,
    ) -> Self {
        Self {
            backend: Arc::new(backend),
            data: None,
            data_process: std::sync::OnceLock::new(),
            replica: None,
            assigned: None,
            joined: None,
            old_listener: Some(listener),
            drained: None,
            controller: None,
            listener: None,
            network,
            config,
        }
    }
    pub(in crate::distribution_native_host) fn is_running(&self) -> bool {
        self.listener.is_some()
            || self
                .assigned
                .as_ref()
                .is_some_and(|owner| owner.is_running())
            || self.data.as_ref().is_some_and(|data| data.is_running())
            || self.joined.as_ref().is_some_and(|owner| owner.is_running())
            || self
                .replica
                .as_ref()
                .is_some_and(|receiver| receiver.is_running())
    }
    pub(crate) fn advance(&mut self, parent: &CancellationToken) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        if self.data.is_some() || self.replica.is_some() {
            return Err(invalid(
                "INITIAL DATA transition cannot revert to old control listener",
            ));
        }
        if self.network.dev_localhost_plaintext {
            return Err(invalid("native metadata control requires mutual TLS"));
        }
        self.network.validate()?;
        let tls = self
            .network
            .tls
            .as_ref()
            .ok_or_else(|| invalid("native control TLS configuration absent"))?;
        if !tls.require_client_cert {
            return Err(invalid("native control requires client certificates"));
        }
        let actual_pin =
            crate::replication_transport::replication_certificate_sha256(&tls.cert_path)?;
        if self.config.node_tls_certificate_sha256.as_deref() != Some(actual_pin.as_str()) {
            return Err(invalid(
                "native control listener TLS leaf differs from its actual CONFIG template",
            ));
        }
        if self.listener.is_some() {
            return self
                .controller
                .as_ref()
                .ok_or_else(|| invalid("native controller owner absent"))?
                .bootstrap(&token)
                .map(|_| ());
        }
        if self.drained.is_none() {
            self.drained = Some(
                self.old_listener
                    .as_mut()
                    .ok_or_else(|| invalid("native stopping listener custody absent"))?
                    .stop_and_drain(&token)?,
            );
        }
        let drained = self.drained.as_ref().unwrap();
        drained.require_service(&self.backend.service)?;
        if self.controller.is_none() {
            self.controller = Some(Arc::new(NativeMetadataRegistrationController::new(
                Arc::clone(&self.backend),
                self.config.clone(),
                &token,
            )?));
        }
        let controller = self.controller.as_ref().unwrap();
        {
            let store = self.backend.lock_metadata(&token)?;
            let mut service = self
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("native transition service busy"))?;
            service.attach_native_metadata_control(
                &self.backend.service,
                drained,
                &store,
                controller,
            )?;
        }
        self.old_listener.take();
        self.listener = Some(self.backend.start_listener(self.network.clone(), &token)?);
        token.check()
    }
    pub(crate) fn close_initial_old_step(
        &self,
        runtime: Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>,
        role: Arc<crate::distribution_range_recovery::RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeJointOldObservation>> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("original initial DATA owner absent"))?
            .close_initial_old(runtime, role, own, business, parent)
    }
    /// Actual receiver step; neither its borrow nor a cached validation result
    /// can escape. Network acquisitions must precede this call. The closure
    /// must retain actual mutation outcomes before fallible postchecks.
    #[cfg(test)]
    pub(crate) fn with_initial_old_step<T>(
        &self,
        role: Arc<crate::distribution_range_recovery::RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        operation: impl for<'step> FnOnce(&NativeInitialOldStep<'step>) -> Result<T>,
    ) -> Result<T> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("original initial DATA owner absent"))?;
        let admission = data.admission_for_old_step()?;
        let step = admission.observe_initial_old_step(
            self.backend.metadata.clone(),
            role,
            own,
            business,
            parent,
        )?;
        let result = operation(&step)?;
        step.require_metadata_current()?;
        step.require_live()?;
        Ok(result)
    }
    pub(crate) fn bind_initial_image_source_process(
        &self,
        process: &CancellationToken,
    ) -> Result<()> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual initial DATA owner absent"))?
            .bind_image_source_process(process)
    }
    pub(crate) fn collect_initial_remote_old_at_actual_donor(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_handoff::NativeRemoteOldCutCertificate> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual initial donor absent"))?
            .collect_remote_old_at_actual_donor(range, parent)
    }
    #[cfg(test)]
    pub(crate) fn test_collect_initial_remote_old_at_actual_donor(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
        after_fetch: impl FnOnce(
            &HostMetadataReadObservation,
            Uuid,
            &[crate::distribution_transport::AuthenticatedNativeRemoteOldFrame],
        ) -> Result<()>,
    ) -> Result<crate::distribution_range_handoff::NativeRemoteOldCutCertificate> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual initial donor absent"))?
            .test_collect_remote_old_at_actual_donor(range, parent, after_fetch)
    }
    pub(crate) fn retain_initial_image_from_remote_old(
        &self,
        old: &mut Option<crate::distribution_range_handoff::NativeRemoteOldCutCertificate>,
        attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_image::NativeRetainedRemoteImageSource> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual initial donor absent"))?
            .retain_remote_image_from_remote_old(old, attempt, parent)
    }
    pub(crate) fn retain_initial_remote_image_source(
        &self,
        old: &mut Option<crate::distribution_range_handoff::NativeJointOldClosedQuorum>,
        target_attempt: Uuid,
        original_request: &CancellationToken,
    ) -> Result<crate::distribution_range_image::NativeRetainedRemoteImageSource> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual initial DATA owner is absent"))?
            .retain_remote_image_source(old, target_attempt, original_request)
    }
    pub(crate) fn ensure_enrolled_target(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<ClusterNodeId> {
        let business = Uuid::now_v7();
        let current = self.backend.transport.acquire_host_metadata_observation(
            &self.backend.node_id,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        let store = self.backend.lock_metadata(parent)?;
        store.require_native_control_leader(&current, business)?;
        let member = store
            .committed_topology()
            .nodes
            .get(target)
            .ok_or_else(|| invalid("enrollment target absent"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active {
            return Err(invalid("enrollment target retired"));
        }
        if member.native_store_birth_v1.is_some() {
            return Ok(target.clone());
        }
        drop(store);
        Ok(self
            .backend
            .enroll_registered_native_learner(target, parent)?
            .node()
            .clone())
    }
    pub(crate) fn enroll_learner(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeLearnerMetadataEnrollment> {
        self.backend
            .enroll_registered_native_learner(target, parent)
    }
    pub(crate) fn catch_up_learner(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<Uuid> {
        if target == &self.backend.node_id {
            return Err(invalid("learner catch-up requires an actual REMOTE member"));
        }
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let business = Uuid::now_v7();
        let current = self.backend.transport.acquire_host_metadata_observation(
            &self.backend.node_id,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let open = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("learner catch-up service busy"))?
            .listener_open_guard();
        let mut accepted = None;
        for _ in 0..3 {
            let (request, required) = {
                let store = self.backend.lock_metadata(&token)?;
                store.require_listener_database(&open)?;
                store.require_native_control_leader(&current, business)?;
                let member = store
                    .committed_topology()
                    .nodes
                    .get(target)
                    .ok_or_else(|| invalid("catch-up learner absent"))?;
                if member.lifecycle != ClusterNodeLifecycle::Active
                    || member.native_store_birth_v1.is_none()
                {
                    return Err(invalid(
                        "catch-up requires exact committed bound metadata learner",
                    ));
                }
                (
                    store.append_request_for_peer(target, 16)?,
                    store.status().commit_index,
                )
            };
            let reply = self
                .backend
                .transport
                .append_metadata_cancellable(target, request, &token)?;
            let receiver = reply
                .native_receiver
                .as_ref()
                .ok_or_else(|| invalid("learner catch-up has no actual native owned ACK"))?;
            let id = receiver.metadata_store_id();
            {
                let mut store = self.backend.lock_metadata(&token)?;
                store.require_listener_database(&open)?;
                store.record_append_response(target, &reply)?;
                store.require_native_control_leader(&current, business)?;
            }
            if reply.success && reply.match_index >= required {
                accepted = Some(id);
                break;
            }
        }
        let id = accepted.ok_or_else(|| {
            invalid("bounded learner catch-up has not reached actual leader commit")
        })?;
        let post_business = Uuid::now_v7();
        let post = self.backend.transport.acquire_host_metadata_observation(
            &self.backend.node_id,
            post_business,
            Duration::from_secs(5),
            &token,
        )?;
        if post.context_id() == current.context_id()
            || post.committed_topology(post_business)? != current.committed_topology(business)?
            || post.restore_activation(post_business)? != current.restore_activation(business)?
        {
            return Err(invalid("learner catch-up CONFIG changed"));
        }
        let store = self.backend.lock_metadata(&token)?;
        store.require_listener_database(&open)?;
        store.require_native_control_leader(&post, post_business)?;
        token.check()?;
        Ok(id)
    }
    pub(in crate::distribution_native_host) fn start_automatic_metadata(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataSupervisor> {
        if !self.is_running() {
            return Err(invalid(
                "automatic metadata requires owned control listener",
            ));
        }
        let supervisor = self
            .backend
            .start_automatic_metadata(self.network.clone(), parent)?;
        self.retain_data_process(&supervisor.lifecycle_cancellation())?;
        Ok(supervisor)
    }
    pub(in crate::distribution_native_host) fn retain_data_process(
        &self,
        process: &CancellationToken,
    ) -> Result<()> {
        process.check()?;
        if self.data_process.get().is_none() {
            let _ = self.data_process.set(process.clone());
        }
        let original = self.data_process.get().unwrap();
        original.check()?;
        if let Some(data) = &self.data {
            data.start_hot_process(original)?;
        }
        Ok(())
    }
    pub(crate) fn maintain_joins(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataMaintenanceReport> {
        self.controller
            .as_ref()
            .ok_or_else(|| invalid("actual controller absent"))?
            .maintain(parent)
    }
    pub(crate) fn metadata_status(
        &self,
        parent: &CancellationToken,
    ) -> Result<crate::MetadataConsensusStatus> {
        parent.check()?;
        let actual = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("native control status service busy"))?
            .listener_open_guard();
        let store = self.backend.lock_metadata(parent)?;
        store.require_listener_database(&actual)?;
        Ok(store.status())
    }
    #[cfg(test)]
    pub(super) fn fail_after_next_proposal(&self) {
        self.controller
            .as_ref()
            .unwrap()
            .test_fail_after_proposal
            .store(true, Ordering::Release);
    }
    pub(in crate::distribution_native_host) fn elect(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        if !self.is_running() {
            return Err(invalid("actual metadata control listener is not running"));
        }
        self.backend.elect(parent).map(|_| ())
    }
    pub(crate) fn heartbeat(&self, parent: &CancellationToken) -> Result<()> {
        self.backend.heartbeat(parent)
    }
    pub(crate) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        if let Some(assigned) = self.assigned.as_mut() {
            assigned.stop(parent)?;
        }
        if let Some(joined) = self.joined.as_mut() {
            joined.stop(parent)?;
        }
        if let Some(replica) = self.replica.as_mut() {
            replica.stop(parent)?;
        }
        if let Some(data) = self.data.as_mut() {
            data.stop(parent)?;
        }
        if let Some(old) = self.old_listener.as_mut() {
            let drained = old.stop_and_drain(parent)?;
            self.drained.get_or_insert(drained);
            self.old_listener.take();
        }
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        self.listener.take();
        Ok(())
    }
}

pub(crate) use joined_data::{NativeJoinedRemoteImagePoint, NativeJoinedRemoteTransactionRequest};
