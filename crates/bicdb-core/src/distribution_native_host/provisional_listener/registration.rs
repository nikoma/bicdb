//! Native CONFIG ingress and a finite queue consumer. No data surface.
use super::*;
use crate::distribution::{ClusterBootstrapSnapshot, ClusterNode, DistributionConfig};
use crate::distribution_consensus::MetadataConsensusRole;
use crate::distribution_transport::{
    DrainedClusterDataListener, NativeMetadataRegistrationIngress,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

mod schema_export;

const MAX_PENDING: usize = 32;
const MAX_QUEUE_BYTES: usize = 1024 * 1024;

pub(crate) struct NativeMetadataRegistrationController {
    launch: Arc<NativeInitialConsensusLaunch>,
    config: DistributionConfig,
    store_open_id: Uuid,
    queue: Mutex<BTreeMap<ClusterNodeId, Arc<NativeMetadataRegistrationIngress>>>,
    driving: AtomicBool,
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
        launch: Arc<NativeInitialConsensusLaunch>,
        config: DistributionConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        config.validate()?;
        let (actual_open, store_open_id) = {
            let service = launch
                .service
                .try_lock()
                .ok_or_else(|| invalid("native control service busy before construction"))?;
            (
                service.listener_open_guard(),
                service.native_initial_listener_store_open_id(),
            )
        };
        let store = launch.lock_initial_metadata(parent)?;
        store.require_listener_database(&actual_open)?;
        let topology = store.committed_topology();
        let member = topology
            .nodes
            .get(&config.node_id)
            .ok_or_else(|| invalid("native control template member absent"))?;
        if config.node_id != launch.seed.spec.node_id
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
            launch,
            config,
            store_open_id,
            queue: Mutex::new(BTreeMap::new()),
            driving: AtomicBool::new(false),
            #[cfg(test)]
            test_fail_after_proposal: AtomicBool::new(false),
        })
    }
    pub(crate) fn require_service(
        &self,
        actual: &Arc<Mutex<ClusterDataNodeService>>,
        store_open_id: Uuid,
    ) -> Result<()> {
        if !Arc::ptr_eq(actual, &self.launch.service) || store_open_id != self.store_open_id {
            return Err(invalid(
                "native controller belongs to another actual service/store open",
            ));
        }
        Ok(())
    }
    pub(crate) fn bootstrap(&self, parent: &CancellationToken) -> Result<ClusterBootstrapSnapshot> {
        parent.check()?;
        let actual = self
            .launch
            .service
            .try_lock()
            .ok_or_else(|| invalid("native bootstrap service busy"))?
            .listener_open_guard();
        let store = self.launch.lock_initial_metadata(parent)?;
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
    fn replicate_registration(
        &self,
        required: Option<u64>,
        parent: &CancellationToken,
    ) -> Result<()> {
        // New unbound learners have no owned metadata backend yet. They cannot
        // contribute an ACK and are not contacted to commit their registration.
        for _ in 0..3 {
            let peers = {
                let store = self.launch.lock_initial_metadata(parent)?;
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
                            && n.id != self.launch.seed.spec.node_id
                    })
                    .map(|n| n.id.clone())
                    .collect::<Vec<_>>()
            };
            for peer in peers {
                parent.check()?;
                let request = {
                    let store = self.launch.lock_initial_metadata(parent)?;
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
                    .launch
                    .transport
                    .append_metadata_cancellable(&peer, request, parent)
                {
                    Ok(reply) => {
                        self.launch
                            .lock_initial_metadata(parent)?
                            .record_append_response(&peer, &reply)?;
                    }
                    Err(_) => {
                        parent.check()?;
                    }
                }
            }
        }
        self.launch.publish_initial_committed_metadata(parent)
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
        if snapshot.metadata_leader_id.as_ref() != Some(&self.launch.seed.spec.node_id) {
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
        let leader = &self.launch.seed.spec.node_id;
        let business = Uuid::now_v7();
        let current = self.launch.transport.acquire_host_metadata_observation(
            leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let actual = self
            .launch
            .service
            .try_lock()
            .ok_or_else(|| invalid("native registration service busy"))?
            .listener_open_guard();
        let proposed = {
            let mut store = self.launch.lock_initial_metadata(&token)?;
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
        let final_mq = self.launch.transport.acquire_host_metadata_observation(
            leader,
            final_business,
            Duration::from_secs(5),
            &token,
        )?;
        final_mq.require_live_request(final_business)?;
        token.check()?;
        let store = self.launch.lock_initial_metadata(&token)?;
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
    launch: Arc<NativeInitialConsensusLaunch>,
    old_listener: Option<ClusterDataServerHandle>,
    drained: Option<DrainedClusterDataListener>,
    controller: Option<Arc<NativeMetadataRegistrationController>>,
    listener: Option<ClusterDataServerHandle>,
    network: ClusterNetworkTransportConfig,
    config: DistributionConfig,
}
impl NativeMetadataControlHost {
    pub(crate) fn new(
        launch: NativeInitialConsensusLaunch,
        listener: ClusterDataServerHandle,
        network: ClusterNetworkTransportConfig,
        config: DistributionConfig,
    ) -> Self {
        Self {
            launch: Arc::new(launch),
            old_listener: Some(listener),
            drained: None,
            controller: None,
            listener: None,
            network,
            config,
        }
    }
    pub(crate) fn advance(&mut self, parent: &CancellationToken) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
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
        drained.require_service(&self.launch.service)?;
        if self.controller.is_none() {
            self.controller = Some(Arc::new(NativeMetadataRegistrationController::new(
                Arc::clone(&self.launch),
                self.config.clone(),
                &token,
            )?));
        }
        let controller = self.controller.as_ref().unwrap();
        {
            let store = self.launch.lock_initial_metadata(&token)?;
            let mut service = self
                .launch
                .service
                .try_lock()
                .ok_or_else(|| invalid("native transition service busy"))?;
            service.attach_native_metadata_control(
                &self.launch.service,
                drained,
                &store,
                controller,
            )?;
        }
        self.old_listener.take();
        self.listener = Some(
            self.launch
                .start_metadata_listener(self.network.clone(), &token)?,
        );
        token.check()
    }
    pub(crate) fn enroll_learner(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeLearnerMetadataEnrollment> {
        self.launch.enroll_registered_native_learner(target, parent)
    }
    pub(crate) fn catch_up_learner(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<Uuid> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let business = Uuid::now_v7();
        let current = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let open = self
            .launch
            .service
            .try_lock()
            .ok_or_else(|| invalid("learner catch-up service busy"))?
            .listener_open_guard();
        let mut accepted = None;
        for _ in 0..3 {
            let (request, required) = {
                let store = self.launch.lock_initial_metadata(&token)?;
                store.require_listener_database(&open)?;
                store.require_native_control_leader(&current, business)?;
                let member = store
                    .committed_topology()
                    .nodes
                    .get(target)
                    .ok_or_else(|| invalid("catch-up learner absent"))?;
                if member.lifecycle != ClusterNodeLifecycle::Active
                    || member.metadata_role != MetadataMemberRole::Learner
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
                .launch
                .transport
                .append_metadata_cancellable(target, request, &token)?;
            let receiver = reply
                .native_receiver
                .as_ref()
                .ok_or_else(|| invalid("learner catch-up has no actual native owned ACK"))?;
            let id = receiver.metadata_store_id();
            {
                let mut store = self.launch.lock_initial_metadata(&token)?;
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
        let post = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
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
        let store = self.launch.lock_initial_metadata(&token)?;
        store.require_listener_database(&open)?;
        store.require_native_control_leader(&post, post_business)?;
        token.check()?;
        Ok(id)
    }
    pub(in crate::distribution_native_host) fn start_automatic_metadata(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataSupervisor> {
        if self.listener.is_none() {
            return Err(invalid(
                "automatic metadata requires owned control listener",
            ));
        }
        self.launch
            .start_automatic_metadata(self.network.clone(), parent)
    }
    pub(crate) fn metadata_status(
        &self,
        parent: &CancellationToken,
    ) -> Result<crate::MetadataConsensusStatus> {
        parent.check()?;
        let actual = self
            .launch
            .service
            .try_lock()
            .ok_or_else(|| invalid("native control status service busy"))?
            .listener_open_guard();
        let store = self.launch.lock_initial_metadata(parent)?;
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
    pub(crate) fn heartbeat(&self, parent: &CancellationToken) -> Result<()> {
        self.launch.heartbeat_initial_metadata(parent)
    }
    pub(crate) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        self.listener.take();
        Ok(())
    }
}
