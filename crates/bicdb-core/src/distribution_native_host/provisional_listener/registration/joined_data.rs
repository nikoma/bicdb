//! A Registered member becoming a DATA owner through the joint handoff.
//!
//! The member keeps its original factory database, service, Store slot,
//! runtime and owned metadata. It has no INITIAL ranges and no range genesis:
//! every range it serves arrives as a verified image through the existing
//! joint handoff, which also supplies the only serving permission.
//!
//! This owner installs the ordinary host writer policy from a fresh host
//! metadata observation (the same producer every other enforced member uses),
//! replaces the replica-only socket with a DATA socket, and publishes
//! application ingress only after the caller asks for it. Policy installation
//! is not leadership, a recovered epoch, Ready or a Strong permit; those stay
//! with the per-request checks of the coordinator and the joint Open.
use super::*;
use crate::distribution_native_read::NativeStrongEventDispatcher;
use crate::distribution_range_consensus::RangeWriteCoordinator;
mod receive;
mod registered;
pub(crate) use receive::{
    NativeJoinedRangeReceiveOrigin, NativeJoinedRangeReceivePreflight,
    NativeJoinedRangeReceiveRequest,
};
mod writer_data_domain;

#[derive(Clone, Debug, serde::Serialize)]
pub struct NativeJoinedDataStatus {
    pub receiver_listening: bool,
    pub writer_policy_installed: bool,
    pub dispatcher_published: bool,
    pub stopping: bool,
    /// Observational range/epoch/watermarks; no shared future Strong-ready bit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_range: Option<serde_json::Value>,
}

pub(super) struct NativeJoinedDataHost {
    parts: registered_replica::RegisteredAssignedParts,
    enrollment: Option<NativeInitialWriterEnrollment>,
    dispatcher: Option<Arc<NativeStrongEventDispatcher>>,
    admission: Option<Arc<NativeInitialDataAdmission>>,
    listener: Option<ClusterDataServerHandle>,
    life: Arc<AtomicBool>,
    published: Arc<AtomicBool>,
    stopping: bool,
    // Distinct original-A provenance. Its exact FIRST attempt/guard/coordinator
    // stays owned through all subsequent failures; no generic re-enrollment.
    registered: Option<assigned_data::NativeRegisteredJoinedDataTransfer>,
    selected_range: Option<crate::RangeDescriptor>,
}
impl Drop for NativeJoinedDataHost {
    fn drop(&mut self) {
        self.life.store(false, Ordering::Release);
        self.published.store(false, Ordering::Release);
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher.try_clear_for_cleanup();
        }
        if let Some(listener) = &self.listener {
            listener.request_shutdown();
        }
        if let Some(registered) = self.registered.take() {
            // Reuse the original pre-hook cleanup worker. It retains the exact
            // attempt until actual listener/dispatcher/runtime workers drain;
            // dropping an RPC or host cannot retire a live worker's hook.
            registered.defer_joined_cleanup(self.listener.take(), self.dispatcher.take());
        }
    }
}
impl NativeJoinedDataHost {
    fn new(parts: registered_replica::RegisteredAssignedParts) -> Result<Self> {
        if !parts.factory.spec.initial_ranges.is_empty() {
            return Err(invalid(
                "joined DATA owner requires the genuine Registered birth without INITIAL ranges",
            ));
        }
        Ok(Self {
            parts,
            enrollment: None,
            dispatcher: None,
            admission: None,
            listener: None,
            life: Arc::new(AtomicBool::new(true)),
            published: Arc::new(AtomicBool::new(false)),
            stopping: false,
            registered: None,
            selected_range: None,
        })
    }
    /// Infallible owner retention immediately after the source moved its real
    /// custody. Every fallible validation/drain happened before transfer.
    fn from_registered_first_policy(
        transfer: assigned_data::NativeRegisteredJoinedDataTransfer,
    ) -> Self {
        let (context, term, index) = transfer.metadata_anchor;
        Self {
            parts: transfer.parts.clone(),
            enrollment: Some(NativeInitialWriterEnrollment {
                coordinator: transfer.coordinator.clone(),
                guard: transfer.guard.clone(),
                metadata_context_id: context,
                metadata_term: term,
                metadata_read_index: index,
            }),
            dispatcher: None,
            admission: None,
            listener: None,
            life: Arc::new(AtomicBool::new(true)),
            published: Arc::new(AtomicBool::new(false)),
            stopping: false,
            selected_range: Some(transfer.range.clone()),
            registered: Some(transfer),
        }
    }
    pub(super) fn is_running(&self) -> bool {
        !self.stopping && self.listener.is_some()
    }
    /// Install (or replace) the host writer policy at the current committed
    /// configuration. The first call creates the coordinator; later calls
    /// keep that same coordinator. APP ingress is closed across the change
    /// and is reopened only by `publish`.
    fn enroll(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if self.registered.is_some() {
            return Err(invalid("original Registered Joined must keep its exact FIRST writer; generic policy re-enrollment refused"));
        }
        if self.stopping {
            return Err(invalid("stopping joined DATA owner cannot enroll"));
        }
        self.parts.database.require_live()?;
        self.parts.store.require_live()?;
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let route = {
            let metadata = self.parts.backend.lock_metadata(&token)?;
            let status = metadata.status();
            if status.role == MetadataConsensusRole::Leader {
                status.node_id
            } else {
                status
                    .leader_id
                    .ok_or_else(|| invalid("joined DATA metadata leader unknown"))?
            }
        };
        let business = Uuid::now_v7();
        let observed = self
            .parts
            .backend
            .transport
            .acquire_host_metadata_observation(&route, business, Duration::from_secs(5), &token)?;
        let topology = observed.committed_topology(business)?.clone();
        {
            let member = topology
                .nodes
                .get(&self.parts.backend.node_id)
                .ok_or_else(|| invalid("joined DATA member absent from committed metadata"))?;
            if member.lifecycle != ClusterNodeLifecycle::Active
                || member.metadata_role != MetadataMemberRole::Voter
                || member.incarnation != self.parts.factory.spec.member_incarnation
                || member.native_store_birth_v1.as_ref().is_none_or(|birth| {
                    birth.birth_id() != self.parts.factory.actual_birth().birth_id()
                        || birth.birth_sha256() != self.parts.factory.actual_birth().birth_sha256()
                })
            {
                return Err(invalid(
                    "joined DATA requires the committed Active voter with this exact native birth",
                ));
            }
        }
        self.parts.backend.transport.install_topology(topology)?;
        let policy = {
            let service = self
                .parts
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("joined DATA service busy"))?;
            let database = self
                .parts
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("joined DATA database busy"))?;
            self.parts.database.require_database(&database)?;
            service.prepare_native_host_writer_enrollment(&database, observed, business)?
        };
        let context = policy.metadata_context_id();
        let term = policy.metadata_term();
        let read_index = policy.metadata_read_index();
        let coordinator = match &self.enrollment {
            Some(owner) => Arc::clone(&owner.coordinator),
            None => Arc::new(RangeWriteCoordinator::new_initial_schema_fenced(
                self.parts.factory.spec.cluster_id.clone(),
                self.parts.backend.node_id.clone(),
                Arc::clone(&self.parts.backend.topology),
                Arc::clone(&self.parts.backend.service),
                Arc::clone(&self.parts.backend.transport),
            )?),
        };
        self.published.store(false, Ordering::Release);
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher.clear();
        }
        let guard = {
            let database = self
                .parts
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("joined DATA database busy before policy"))?;
            Arc::new(
                database.install_enrolled_native_coordinator(Arc::clone(&coordinator), policy)?,
            )
        };
        // Retain the actual installed owner before any fallible postcheck.
        self.enrollment = Some(NativeInitialWriterEnrollment {
            coordinator,
            guard,
            metadata_context_id: context,
            metadata_term: term,
            metadata_read_index: read_index,
        });
        token.check()
    }
    /// Bind the retained admission to the currently installed writer and make
    /// sure the DATA socket exists. APP stays closed.
    fn bind(
        &mut self,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.stopping {
            return Err(invalid("stopping joined DATA owner cannot bind"));
        }
        let owner = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("joined DATA writer policy is not installed"))?;
        let range = if let Some(selected) = &self.selected_range {
            let actual = self
                .parts
                .backend
                .topology
                .try_read()
                .ok_or_else(|| invalid("Registered Joined selected routing busy"))?;
            let current = actual
                .range_by_id(selected.id)
                .ok_or_else(|| invalid("Registered Joined selected range disappeared"))?;
            if crate::distribution_native_read::canonical_range(current)
                != crate::distribution_native_read::canonical_range(selected)
            {
                return Err(invalid("Registered Joined selected domain changed"));
            }
            selected.clone()
        } else {
            self.parts
                .backend
                .topology
                .try_read()
                .ok_or_else(|| invalid("joined DATA routing busy"))?
                .ranges
                .values()
                .next()
                .cloned()
                .ok_or_else(|| invalid("joined DATA has no committed range"))?
        };
        let (writer, root) = {
            let service = self
                .parts
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("joined DATA service busy before admission"))?;
            service.retain_initial_data_writer(&owner.coordinator, &owner.guard, &range)?
        };
        match &self.admission {
            Some(admission) => admission.replace_writer(writer),
            None => {
                self.admission = Some(Arc::new(NativeInitialDataAdmission::for_joined_owner(
                    &self.parts.backend.service,
                    &self.parts.runtime,
                    self.parts.database.clone(),
                    writer,
                    root,
                    self.parts.store.clone(),
                    &self.life,
                    Arc::clone(&self.published),
                )));
            }
        }
        let admission = self.admission.as_ref().unwrap();
        admission.require_listener(&self.parts.backend.service, &self.parts.runtime)?;
        if self.dispatcher.is_none() {
            self.dispatcher = Some(NativeStrongEventDispatcher::new(
                &self.parts.backend.service,
                &self.parts.runtime,
                &self.parts.backend.transport,
                &self.parts.backend.topology,
            )?);
        }
        if self.listener.is_none() {
            let address: std::net::SocketAddr = {
                let metadata = self.parts.backend.lock_metadata(parent)?;
                metadata.require_listener_database(&self.parts.database)?;
                metadata
                    .committed_topology()
                    .nodes
                    .get(&self.parts.backend.node_id)
                    .ok_or_else(|| invalid("joined DATA actual member missing"))?
                    .address
                    .parse()
                    .map_err(|_| invalid("joined DATA requires numeric local listener endpoint"))?
            };
            self.listener = Some(
                crate::distribution_transport::start_cluster_initial_native_data_server(
                    address,
                    network.clone(),
                    Arc::clone(&self.parts.backend.topology),
                    Arc::clone(&self.parts.backend.service),
                    Arc::clone(&self.parts.backend.metadata),
                    Arc::clone(&self.parts.backend.provider),
                    Arc::clone(&self.parts.runtime),
                    Arc::clone(admission),
                )?,
            );
        }
        parent.check()?;
        admission.require_live()
    }
    /// Recover leadership for the ranges this member now leads and open APP
    /// and Strong ingress. Every request still proves its own authority.
    fn publish(&mut self, parent: &CancellationToken) -> Result<usize> {
        parent.check()?;
        if self.registered.is_some() {
            return self.publish_registered(parent);
        }
        if self.stopping {
            return Err(invalid("stopping joined DATA owner cannot publish"));
        }
        let owner = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("joined DATA writer policy is not installed"))?;
        let admission = self
            .admission
            .as_ref()
            .ok_or_else(|| invalid("joined DATA admission is not bound"))?;
        let dispatcher = self
            .dispatcher
            .as_ref()
            .ok_or_else(|| invalid("joined DATA dispatcher is not bound"))?;
        admission.require_listener(&self.parts.backend.service, &self.parts.runtime)?;
        let recovered = owner.coordinator.recover_local_leadership(64)?;
        parent.check()?;
        dispatcher.publish(&owner.coordinator, &owner.guard)?;
        if let Err(error) = parent.check().and_then(|_| admission.require_live()) {
            dispatcher.clear();
            return Err(error);
        }
        self.published.store(true, Ordering::Release);
        Ok(recovered)
    }
    fn status(&self) -> NativeJoinedDataStatus {
        NativeJoinedDataStatus {
            receiver_listening: self.listener.is_some(),
            writer_policy_installed: self.enrollment.is_some(),
            dispatcher_published: self.published.load(Ordering::Acquire),
            stopping: self.stopping,
            registered_range: self.registered_range_status(),
        }
    }
    pub(super) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.stopping = true;
        self.life.store(false, Ordering::Release);
        self.published.store(false, Ordering::Release);
        if let Some(dispatcher) = &self.dispatcher {
            while !dispatcher.try_clear_for_cleanup() {
                parent.check()?;
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        if let Some(listener) = self.listener.as_mut() {
            listener.stop_and_drain(parent)?;
        }
        loop {
            parent.check()?;
            let active = self.dispatcher.as_ref().map_or(0, |d| d.active_workers());
            if active == 0 && self.parts.runtime.active_contexts()? == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if let Some(owner) = &self.enrollment {
            let database = self
                .parts
                .factory
                .database
                .try_read()
                .ok_or_else(|| invalid("joined DATA database busy during stop"))?;
            owner
                .guard
                .retire_if_current(&database, &owner.coordinator)?;
        }
        self.listener.take();
        Ok(())
    }
}
impl NativeMetadataControlHost {
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_registered_first_policy_evidence(
        &self,
    ) -> Result<serde_json::Value> {
        if let Some(assigned) = &self.assigned {
            return assigned.test_first_policy_evidence();
        }
        self.joined
            .as_ref()
            .and_then(|joined| joined.registered.as_ref())
            .ok_or_else(|| invalid("actual Registered FIRST custody absent"))?
            .test_first_policy_evidence()
    }
    /// Continue the exact Registered FIRST owner. Unlike open_joined_data this
    /// never enrolls or replaces a writer; every later failure leaves Joined
    /// custody retained and APP closed until genuine publication succeeds.
    pub(in crate::distribution_native_host) fn open_registered_joined_data(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.data.is_some() {
            return Err(invalid("INITIAL cannot become original Registered Joined"));
        }
        if self.joined.is_none() {
            let assigned = self
                .assigned
                .as_mut()
                .ok_or_else(|| invalid("original Registered FIRST owner absent"))?;
            let drained = assigned.prepare_registered_joined_transfer(parent)?;
            let transfer = assigned.take_registered_joined_transfer(drained)?;
            // Infallible owner retention BEFORE any later bind/recovery call.
            self.joined = Some(NativeJoinedDataHost::from_registered_first_policy(transfer));
            drop(self.assigned.take()); // Source now owns no policy/attempt.
        }
        let joined = self.joined.as_mut().unwrap();
        let original = joined
            .registered
            .as_ref()
            .ok_or_else(|| invalid("foreign Joined cannot become original Registered Joined"))?;
        original.require_original_lifetime()?;
        original
            .drained
            .require_service(&joined.parts.backend.service)?;
        original.drained.require_store(joined.parts.store.id())?;
        self.replica
            .as_mut()
            .ok_or_else(|| invalid("original Registered receiver custody absent"))?
            .drain_for_assigned(parent)?;
        joined.bind(&self.network, parent)
    }
    /// Install the ordinary host writer policy on this Registered member at
    /// the current committed configuration and replace its replica-only
    /// socket with a DATA socket. Application ingress remains closed.
    pub(in crate::distribution_native_host) fn open_joined_data(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.data.is_some() || self.assigned.is_some() {
            return Err(invalid(
                "joined DATA requires a Registered member without another DATA owner",
            ));
        }
        if self.joined.is_none() {
            let parts = self
                .replica
                .as_ref()
                .ok_or_else(|| invalid("actual Registered receiver required"))?
                .assigned_parts()?;
            self.joined = Some(NativeJoinedDataHost::new(parts)?);
        }
        let owner = self.joined.as_mut().unwrap();
        if owner.enrollment.is_none() {
            owner.enroll(parent)?;
        }
        self.replica
            .as_mut()
            .ok_or_else(|| invalid("original Registered drain owner absent"))?
            .drain_for_assigned(parent)?;
        owner.bind(&self.network, parent)
    }
    /// Replace the installed policy after the committed DATA configuration
    /// changed (a completed joint membership change). APP is closed until
    /// `publish_joined_data`.
    pub(in crate::distribution_native_host) fn refresh_joined_data_policy(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        let owner = self
            .joined
            .as_mut()
            .ok_or_else(|| invalid("retained joined DATA owner absent"))?;
        owner.enroll(parent)?;
        owner.bind(&self.network, parent)
    }
    pub(in crate::distribution_native_host) fn publish_joined_data(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<usize> {
        self.joined
            .as_mut()
            .ok_or_else(|| invalid("retained joined DATA owner absent"))?
            .publish(parent)
    }
    pub(in crate::distribution_native_host) fn joined_data_status(
        &self,
    ) -> Option<NativeJoinedDataStatus> {
        self.joined.as_ref().map(|owner| owner.status())
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    #[allow(clippy::type_complexity)]
    pub(in crate::distribution_native_host) fn test_joined_data_bindings(
        &self,
    ) -> Result<(
        Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
        Arc<crate::db::InstalledNativeWriterHostGuard>,
        Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
        Arc<TcpClusterRelocationTransport>,
    )> {
        let owner = self
            .joined
            .as_ref()
            .ok_or_else(|| invalid("joined DATA owner absent"))?;
        let enrollment = owner
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("joined DATA writer absent"))?;
        Ok((
            Arc::clone(&enrollment.coordinator),
            Arc::clone(&enrollment.guard),
            Arc::clone(&owner.parts.runtime),
            Arc::clone(&owner.parts.backend.transport),
        ))
    }
}
