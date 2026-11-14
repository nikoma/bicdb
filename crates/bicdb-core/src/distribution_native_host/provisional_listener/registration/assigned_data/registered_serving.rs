//! Original Registered receiver's FIRST ordinary policy after actual ALL-new
//! release/Open. Reuses same service/slot/runtime/socket owner; no INITIAL.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteJointOwner;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

pub(super) struct RegisteredServingPolicy {
    owner: Arc<NativeRegisteredRemoteJointOwner>,
    admission: Arc<NativeJointServingPolicyAdmission>,
}
/// Exact installed custody transfer. Constructed only by the real retained
/// Assigned owner; no policy DTO or historical final-image pointer can mint it.
pub(in super::super) struct NativeRegisteredJoinedDataTransfer {
    pub(in super::super) parts: registered_replica::RegisteredAssignedParts,
    pub(in super::super) coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    pub(super) attempt: NativeAssignedWriterInstallAttempt,
    pub(in super::super) guard: Arc<crate::db::InstalledNativeWriterHostGuard>,
    pub(in super::super) metadata_anchor: (Uuid, u64, u64),
    pub(super) registered: RegisteredServingPolicy,
    pub(in super::super) range: crate::RangeDescriptor,
    pub(super) cleanup: AssignedCleanupSender,
    pub(in super::super) drained: DrainedClusterDataListener,
    pub(super) writer_data_domains: super::super::writer_data_domain::NativeHostWriterDataDomains,
}
impl NativeRegisteredJoinedDataTransfer {
    #[cfg(test)]
    pub(in super::super) fn test_first_policy_evidence(&self) -> Result<serde_json::Value> {
        self.attempt.test_publication_evidence()
    }
    pub(in super::super) fn historical_native_bridge(
        &self,
    ) -> Arc<crate::db::NativeJointServingBridgeReceipt> {
        self.registered.admission.historical_native_bridge()
    }
    pub(in super::super) fn historical_released_publication_point(
        &self,
    ) -> Result<(Arc<crate::db::NativeJointActivatedOpenGuard>, Uuid, String)> {
        self.registered
            .admission
            .historical_released_publication_point()
    }
    pub(in super::super) fn require_original_lifetime(&self) -> Result<()> {
        self.registered
            .owner
            .require_first_policy_attempt_lifetime()
    }
    pub(in super::super) fn defer_joined_cleanup(
        self,
        listener: Option<ClusterDataServerHandle>,
        dispatcher: Option<Arc<crate::distribution_native_read::NativeStrongEventDispatcher>>,
    ) {
        let sender = self.cleanup.clone();
        sender.defer(AssignedCleanup::from_registered_joined(
            self, listener, dispatcher,
        ));
    }
}
impl NativeAssignedDataHost {
    #[cfg(test)]
    pub(in super::super) fn test_first_policy_evidence(&self) -> Result<serde_json::Value> {
        self.attempt
            .as_ref()
            .ok_or_else(|| invalid("actual FIRST policy attempt absent"))?
            .test_publication_evidence()
    }
    /// Every fallible check and finite old-socket drain precedes transfer. The
    /// actual attempt remains in this host on all failures; no stop/re-enroll.
    pub(in super::super) fn prepare_registered_joined_transfer(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<DrainedClusterDataListener> {
        parent.check()?;
        let registered = self.registered.as_ref().ok_or_else(|| {
            invalid("local-image Assigned owner cannot become original Registered Joined")
        })?;
        registered.owner.require_first_policy_attempt_lifetime()?;
        let attempt = self
            .attempt
            .as_ref()
            .ok_or_else(|| invalid("Registered FIRST attempt absent before Joined transfer"))?;
        if attempt.unresolved_native_publication() {
            return Err(invalid(
                "Registered FIRST native outcome remains unresolved before Joined transfer",
            ));
        }
        let guard = attempt
            .installed_guard()
            .ok_or_else(|| invalid("Registered FIRST exact Installed guard absent"))?;
        let coordinator = self
            .coordinator
            .as_ref()
            .ok_or_else(|| invalid("Registered FIRST original coordinator absent"))?;
        let database = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Registered Joined transfer actual DB busy"))?;
        guard.require_for_coordinator(&database, &self.range, coordinator)?;
        attempt.installed_metadata_anchor()?;
        drop(database);
        if self.cleanup.is_none() {
            return Err(invalid(
                "Registered Joined transfer lost original cleanup owner",
            ));
        }
        // This old mode is always Closed. Its actual listener must drain and
        // cannot return by flipping a flag. Original receiver lifetime survives.
        self.life.store(false, Ordering::Release);
        let drained = self
            .listener
            .as_mut()
            .ok_or_else(|| {
                invalid("Registered FIRST Closed socket must be bound before actual Joined drain")
            })?
            .stop_and_drain(parent)?;
        while self.parts.runtime.active_contexts()? != 0 {
            parent.check()?;
            std::thread::sleep(Duration::from_millis(5));
        }
        parent.check()?;
        Ok(drained)
    }
    /// Exclusive host custody and the preceding preparation establish every
    /// option. No fallible work after the first actual owner is moved.
    pub(in super::super) fn take_registered_joined_transfer(
        &mut self,
        drained: DrainedClusterDataListener,
    ) -> Result<NativeRegisteredJoinedDataTransfer> {
        let registered = self
            .registered
            .as_ref()
            .ok_or_else(|| invalid("Registered Joined provenance absent"))?;
        registered.owner.require_first_policy_attempt_lifetime()?;
        drained.require_service(&self.parts.backend.service)?;
        drained.require_store(self.parts.store.id())?;
        let attempt = self
            .attempt
            .as_ref()
            .ok_or_else(|| invalid("Registered Joined exact attempt absent"))?;
        let guard = attempt
            .installed_guard()
            .cloned()
            .ok_or_else(|| invalid("Registered Joined Installed guard absent"))?;
        let metadata_anchor = attempt.installed_metadata_anchor()?;
        if self.coordinator.is_none()
            || self.cleanup.is_none()
            || self.writer_data_domains.unresolved()
        {
            return Err(invalid(
                "Registered Joined exact retained owners incomplete/unresolved",
            ));
        }
        let result = NativeRegisteredJoinedDataTransfer {
            parts: self.parts.clone(),
            coordinator: self.coordinator.take().unwrap(),
            attempt: self.attempt.take().unwrap(),
            guard,
            metadata_anchor,
            registered: self.registered.take().unwrap(),
            range: self.range.clone(),
            cleanup: self.cleanup.take().unwrap(),
            drained,
            writer_data_domains: std::mem::take(&mut self.writer_data_domains),
        };
        self.listener.take();
        Ok(result)
    }
    pub(super) fn new_registered_serving(
        parts: registered_replica::RegisteredAssignedParts,
        owner: Arc<NativeRegisteredRemoteJointOwner>,
        admission: Arc<NativeJointServingPolicyAdmission>,
    ) -> Result<Self> {
        admission.require_external_current()?;
        if !parts.factory.spec.initial_ranges.is_empty()
            || !Arc::ptr_eq(&parts.backend.service, owner.service())
            || owner.target().native().local_node_id() != &parts.backend.node_id
            || owner.target().base().intended_activation_range()
                != admission.data().activation_range()
        {
            return Err(invalid(
                "Registered Serving owner changed original no-INITIAL service/NEW range",
            ));
        }
        let range = admission.data().activation_range().clone();
        Ok(Self {
            parts,
            coordinator: None,
            attempt: None,
            image: None,
            range,
            admission: None,
            listener: None,
            life: Arc::new(AtomicBool::new(true)),
            stopping: false,
            cleanup: None,
            writer_data_domains: Default::default(),
            registered: Some(RegisteredServingPolicy { owner, admission }),
        })
    }
    pub(super) fn replace_registered_policy_phase(
        &mut self,
        admission: Arc<NativeJointServingPolicyAdmission>,
    ) -> Result<()> {
        admission.require_external_current()?;
        let retained = self.registered.as_mut().ok_or_else(|| {
            invalid("legacy Assigned owner cannot become original Registered Serving")
        })?;
        if admission.data() != retained.admission.data()
            || admission.local_node_id() != retained.admission.local_node_id()
        {
            return Err(invalid(
                "Registered policy retry changed actual original decision/recipient",
            ));
        }
        retained.admission = admission;
        Ok(())
    }
    pub(super) fn install_registered_serving(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        if self.stopping {
            return Err(invalid(
                "stopping Registered Serving owner cannot install policy",
            ));
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
        let retained = self
            .registered
            .as_ref()
            .ok_or_else(|| invalid("Registered Serving provenance absent"))?;
        retained.admission.require_external_current()?;
        let route = self
            .parts
            .backend
            .lock_metadata(parent)?
            .status()
            .leader_id
            .ok_or_else(|| invalid("Registered first DATA metadata leader unknown"))?;
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
        let (origin, policy) = retained.owner.prepare_first_serving_data_policy(
            retained.admission.clone(),
            observed,
            business,
            parent,
        )?;
        if origin.range() != &self.range {
            return Err(invalid(
                "Registered first DATA policy changed actual NEW range",
            ));
        }
        if self.coordinator.is_none() {
            self.coordinator = Some(Arc::new(
                RangeWriteCoordinator::new_registered_serving_schema_fenced(
                    policy.facts().cluster_id.clone(),
                    self.parts.backend.node_id.clone(),
                    self.parts.backend.topology.clone(),
                    self.parts.backend.service.clone(),
                    self.parts.backend.transport.clone(),
                    &origin,
                )?,
            ));
        }
        let coordinator = self.coordinator.as_ref().unwrap();
        let database = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Registered first DATA DB busy"))?;
        if self.attempt.is_none() {
            self.attempt = Some(NativeAssignedWriterInstallAttempt::begin_registered(
                &database,
                coordinator,
                &origin,
                &policy,
            )?);
        }
        // Host, original owner, coordinator and exact attempt precede WAL.
        let result = self.attempt.as_mut().unwrap().advance_registered(
            &database,
            coordinator,
            &origin,
            &policy,
        );
        drop(database);
        result?;
        origin.require_owned_metadata_current()
    }
}
impl NativeMetadataControlHost {
    pub(crate) fn install_registered_serving_data_policy(
        &mut self,
        range: crate::RangeId,
        admission: Arc<NativeJointServingPolicyAdmission>,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        admission.require_external_current()?;
        if self.data.is_some() {
            return Err(invalid(
                "INITIAL DATA cannot become original Registered Serving",
            ));
        }
        if let Some(retained) = self.assigned.as_mut() {
            retained.replace_registered_policy_phase(admission)?;
        } else {
            let owner = self.registered_remote_joint_owner(range)?;
            let receiver = self
                .replica
                .as_ref()
                .ok_or_else(|| invalid("Registered Serving original receiver absent"))?;
            let parts = receiver.assigned_parts()?;
            self.assigned = Some(NativeAssignedDataHost::new_registered_serving(
                parts, owner, admission,
            )?);
        }
        self.advance_assigned_closed_data_policy(parent)
    }
}
