//! Original Registered receiver's FIRST ordinary policy after actual ALL-new
//! release/Open. Reuses same service/slot/runtime/socket owner; no INITIAL.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteJointOwner;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

pub(super) struct RegisteredServingPolicy {
    owner: Arc<NativeRegisteredRemoteJointOwner>,
    admission: Arc<NativeJointServingPolicyAdmission>,
}
impl NativeAssignedDataHost {
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
            let receiver = self
                .replica
                .as_ref()
                .ok_or_else(|| invalid("Registered Serving original receiver absent"))?;
            let owner = receiver
                .remote_joint_targets
                .get(&range)
                .cloned()
                .ok_or_else(|| invalid("Registered Serving original A continuation absent"))?;
            let parts = receiver.assigned_parts()?;
            self.assigned = Some(NativeAssignedDataHost::new_registered_serving(
                parts, owner, admission,
            )?);
        }
        self.advance_assigned_closed_data_policy(parent)
    }
}
