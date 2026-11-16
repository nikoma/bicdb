//! Original Registered receiver's FIRST ordinary policy after actual ALL-new
//! release/Open. Reuses same service/slot/runtime/socket owner; no INITIAL.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteJointOwner;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;
#[cfg(test)]
mod drain_tests;
#[cfg(test)]
mod entry_tests;
#[cfg(test)]
mod partial_drain_tests;

/// Single-use borrow of the exact entry admission and original caller. This
/// carries no physical/currentness authority into native work: the factory
/// performs its full external check again after the fresh HostMQ observation.
struct RegisteredPolicyEntry<'a> {
    admission: &'a Arc<NativeJointServingPolicyAdmission>,
    caller: &'a CancellationToken,
}
impl<'a> RegisteredPolicyEntry<'a> {
    fn acquire(
        admission: &'a Arc<NativeJointServingPolicyAdmission>,
        caller: &'a CancellationToken,
    ) -> Result<Self> {
        caller.check()?;
        admission.require_external_current()?;
        caller.check()?;
        Ok(Self { admission, caller })
    }
    fn consume(self) -> Result<Arc<NativeJointServingPolicyAdmission>> {
        self.caller.check()?;
        self.admission.require_live()?;
        Ok(Arc::clone(self.admission))
    }
}

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
    pub(in super::super) writer_data_domains:
        super::super::writer_data_domain::NativeHostWriterDataDomains,
}
impl NativeRegisteredJoinedDataTransfer {
    pub(in super::super) fn registered_owner(&self) -> Arc<NativeRegisteredRemoteJointOwner> {
        self.registered.owner.clone()
    }
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
        if self.stopping {
            return Err(invalid(
                "stopping Registered Assigned owner cannot transfer to Joined",
            ));
        }
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
        // A failed missing-listener precondition must not end this still
        // bindable owner. An actual retained listener is required BEFORE the
        // one-way life transition; a retry resumes that SAME handle's drain.
        if self.listener.is_none() {
            return Err(invalid(
                "Registered FIRST Closed socket must be bound before actual Joined drain",
            ));
        }
        // This old mode is always Closed. Its actual listener must drain and
        // cannot return by flipping a flag. Original receiver lifetime survives.
        self.life.store(false, Ordering::Release);
        #[cfg(test)]
        drain_tests::after_actual_closed_life_ended(parent)?;
        #[cfg(test)]
        partial_drain_tests::before_actual_listener_drain(self.listener.as_ref().unwrap(), parent)?;
        let drained = self
            .listener
            .as_mut()
            .ok_or_else(|| {
                invalid("Registered FIRST Closed socket must be bound before actual Joined drain")
            })?
            .stop_and_drain(parent);
        #[cfg(test)]
        drain_tests::note_actual_drain_return(&drained);
        #[cfg(test)]
        partial_drain_tests::note_actual_listener_drain_return(&drained);
        let drained = drained?;
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
    fn new_registered_serving(
        parts: registered_replica::RegisteredAssignedParts,
        owner: Arc<NativeRegisteredRemoteJointOwner>,
        entry: RegisteredPolicyEntry<'_>,
    ) -> Result<Self> {
        let admission = entry.consume()?;
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
    fn replace_registered_policy_phase(&mut self, entry: RegisteredPolicyEntry<'_>) -> Result<()> {
        let admission = entry.consume()?;
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
        #[cfg(test)]
        if let Some(attempt) = &self.attempt {
            attempt.test_trace_publication_state("registered_install_entry");
        } else {
            crate::native_policy_trace::note("actual_registered_install_attempt_absent");
        }
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
        crate::native_policy_trace::stage!(
            "retained_actual_admission_live",
            retained.admission.require_live()
        )?;
        // Both real requests constrain routing and fresh HostMQ. The factory
        // still validates ALL external/current bindings after HostMQ returns.
        let acquisition = parent.child_linked_to(&retained.admission.bound_policy_request());
        #[cfg(test)]
        entry_tests::at_actual_acquisition(false, &acquisition);
        #[cfg(test)]
        crate::native_policy_trace::note("actual_route_MStore_lock_entry");
        let route = self
            .parts
            .backend
            .lock_metadata(&acquisition)?
            .status()
            .leader_id
            .ok_or_else(|| invalid("Registered first DATA metadata leader unknown"))?;
        #[cfg(test)]
        crate::native_policy_trace::note("actual_route_MStore_lock_returned");
        let business = Uuid::now_v7();
        let observed = crate::native_policy_trace::stage!(
            "actual_fresh_origin_OWN",
            self.parts
                .backend
                .transport
                .acquire_host_metadata_observation(
                    &route,
                    business,
                    Duration::from_secs(5),
                    &acquisition
                )
        )?;
        #[cfg(test)]
        crate::native_policy_trace::origin_own(observed.deadline());
        self.parts
            .backend
            .transport
            .install_topology(observed.committed_topology(business)?.clone())?;
        #[cfg(test)]
        entry_tests::at_actual_acquisition(true, &acquisition);
        let (origin, policy) = crate::native_policy_trace::stage!(
            "actual_prepare_first_policy_origin",
            retained.owner.prepare_first_serving_data_policy(
                retained.admission.clone(),
                observed,
                business,
                parent
            )
        )?;
        if origin.range() != &self.range {
            return Err(invalid(
                "Registered first DATA policy changed actual NEW range",
            ));
        }
        if self.coordinator.is_none() {
            self.coordinator = Some(Arc::new(crate::native_policy_trace::stage!(
                "actual_new_registered_coordinator",
                RangeWriteCoordinator::new_registered_serving_schema_fenced(
                    policy.facts().cluster_id.clone(),
                    self.parts.backend.node_id.clone(),
                    self.parts.backend.topology.clone(),
                    self.parts.backend.service.clone(),
                    self.parts.backend.transport.clone(),
                    &origin,
                )
            )?));
        }
        let coordinator = self.coordinator.as_ref().unwrap();
        let database = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Registered first DATA DB busy"))?;
        if self.attempt.is_none() {
            self.attempt = Some(crate::native_policy_trace::stage!(
                "begin_actual_registered_native_attempt",
                NativeAssignedWriterInstallAttempt::begin_registered(
                    &database,
                    coordinator,
                    &origin,
                    &policy
                )
            )?);
        }
        // Host, original owner, coordinator and exact attempt precede WAL.
        let result = crate::native_policy_trace::stage!(
            "advance_actual_registered_native_attempt",
            self.attempt.as_mut().unwrap().advance_registered(
                &database,
                coordinator,
                &origin,
                &policy
            )
        );
        #[cfg(test)]
        self.attempt
            .as_ref()
            .unwrap()
            .test_trace_publication_state("actual_native_advance_return_state");
        drop(database);
        result?;
        crate::native_policy_trace::stage!(
            "actual_post_install_owned_MStore_current",
            origin.require_owned_metadata_current()
        )
    }
}
impl NativeMetadataControlHost {
    /// Repair only a socket whose SAME actual Installed owner already exists.
    /// This runs before fresh self-TLS collection on a later finite step. It
    /// neither retries native installation nor revives the old phase admission.
    pub(in crate::distribution_native_host) fn restore_retained_remote_policy_socket(
        &mut self,
        successor: &Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if self.joined.is_some() {
            self.require_same_remote_registered_joined_policy(successor, range, parent)?;
            return self.open_registered_joined_data(parent);
        }
        let Some(owner) = self.assigned.as_mut() else {
            return Ok(());
        };
        if owner.stopping {
            return Err(invalid(
                "stopping Registered Assigned owner cannot repair a policy socket",
            ));
        }
        // Before an actual Installed return the original replica listener is
        // still the owner. Absence is not permission to drain or mint a policy.
        let Some(guard) = owner.attempt.as_ref().and_then(|a| a.installed_guard()) else {
            return Ok(());
        };
        successor.require_memory_lifetime()?;
        let registered = owner
            .registered
            .as_ref()
            .ok_or_else(|| invalid("Policy socket repair cannot use generic Assigned custody"))?;
        registered.owner.require_first_policy_attempt_lifetime()?;
        if !successor.domain().phase_owner().same_actual(
            &crate::db::NativeJointPhaseOwner::Registered(Arc::downgrade(&registered.owner)),
        ) || !Arc::ptr_eq(successor.service(), &owner.parts.backend.service)
            || owner.range.id != range
            || owner.range != *successor.native().data().activation_range()
            || registered
                .admission
                .historical_native_bridge()
                .checksum_sha256()
                != successor.native().checksum_sha256()
        {
            return Err(invalid(
                "Policy socket repair changed original Installed owner",
            ));
        }
        let attempt = owner.attempt.as_ref().unwrap();
        if attempt.unresolved_native_publication() {
            return Err(invalid(
                "Policy socket repair refuses unresolved native publication",
            ));
        }
        attempt.installed_metadata_anchor()?;
        let coordinator = owner
            .coordinator
            .as_ref()
            .ok_or_else(|| invalid("Policy socket repair lost actual Installed coordinator"))?;
        let database = owner
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Policy socket repair actual DB busy"))?;
        guard.require_for_coordinator(&database, &owner.range, coordinator)?;
        drop(database);
        if owner.listener.is_some() {
            // SAME actual Installed owner/handle, even if a prior fallible
            // transfer already ended its Closed admission. Resume the real
            // drain→Drained proof→owner transfer; never revive life or try to
            // bind the obsolete Closed admission before fresh self metadata.
            return self.open_registered_joined_data(parent);
        }
        self.replica
            .as_mut()
            .ok_or_else(|| invalid("Policy socket repair lost original drain owner"))?
            .drain_for_assigned(parent)?;
        owner.bind_closed(&self.network, parent)
    }

    pub(crate) fn install_registered_serving_data_policy(
        &mut self,
        range: crate::RangeId,
        admission: Arc<NativeJointServingPolicyAdmission>,
        parent: &CancellationToken,
    ) -> Result<()> {
        crate::native_policy_trace::root!(
            crate::native_policy_trace::Seed::new(
                "registered_first_policy",
                admission.local_node_id().to_string(),
                range,
                parent,
                admission.test_policy_trace_clocks()
            ),
            {
                #[cfg(test)]
                if let Some(attempt) = self
                    .assigned
                    .as_ref()
                    .and_then(|owner| owner.attempt.as_ref())
                {
                    attempt.test_trace_publication_state("host_actual_entry_retained_attempt");
                }
                parent.check()?;
                let entry = crate::native_policy_trace::stage!(
                    "entry_actual_external_ALL_current",
                    RegisteredPolicyEntry::acquire(&admission, parent)
                )?;
                if self.data.is_some() {
                    return Err(invalid(
                        "INITIAL DATA cannot become original Registered Serving",
                    ));
                }
                if let Some(retained) = self.assigned.as_mut() {
                    retained.replace_registered_policy_phase(entry)?;
                } else {
                    let owner = crate::native_policy_trace::stage!(
                        "actual_registered_joint_owner_lookup",
                        self.registered_remote_joint_owner(range)
                    )?;
                    let receiver = self
                        .replica
                        .as_ref()
                        .ok_or_else(|| invalid("Registered Serving original receiver absent"))?;
                    let parts = crate::native_policy_trace::stage!(
                        "actual_registered_parts",
                        receiver.assigned_parts()
                    )?;
                    // No receiver-driven install step may be in flight while the
                    // distinct DATA owner is created; the flag is set under that
                    // same custody. A refusal consumed nothing and is retryable.
                    let install = crate::native_policy_trace::stage!(
                        "actual_install_table_lookup",
                        self.registered_remote_install()
                    )?;
                    let mut custody = crate::native_policy_trace::stage!(
                        "actual_install_table_hold_for_data_owner",
                        install.hold_for_data_owner()
                    )?;
                    self.assigned = Some(NativeAssignedDataHost::new_registered_serving(
                        parts, owner, entry,
                    )?);
                    custody.close_for_data_owner();
                    drop(custody);
                }
                crate::native_policy_trace::stage!(
                    "actual_install_drain_bind_return",
                    self.advance_assigned_closed_data_policy(parent)
                )
            }
        )
    }
}
