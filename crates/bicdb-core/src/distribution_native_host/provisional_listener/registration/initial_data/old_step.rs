//! Actual source-owner admission for ONLY OLD refusal/closure. Purpose/plan
//! DATA alone is insufficient. No APP/Strong/image-row/Serving grant.
use super::*;
mod constraint_step;
mod remote_new;
mod remote_old;
pub(crate) use constraint_step::NativeInitialOldConstraintStep;
#[cfg(test)]
mod sink_test;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataConsensusStore, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
#[cfg(test)]
pub(crate) use sink_test::{
    after_remote_native_old_wal, before_native_old_wal, CancelOldOwnBeforeNativeWal,
    CancelRemoteOldAfterNativeWal,
};

const METADATA_LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(5);

pub(crate) struct NativeInitialOldStep<'request> {
    admission: &'request NativeInitialDataAdmission,
    writer: Arc<crate::db::NativeWriterHostState>,
    predecessor: Arc<crate::db::NativeWriterHistoricalPredecessor>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    runtime: Arc<RangeRecoveryRuntime>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    database: crate::distribution_data::ClusterServiceOpenGuard,
    slot: crate::distribution_data::RangeWriteServiceSlotGuard,
    role: Arc<RecoveryRoleAdmission>,
    plan: Arc<NativeJointHandoffPlan>,
    own: HostMetadataReadObservation,
    business: Uuid,
    member: NativeOwnedMetadataMemberObservation,
    parent: &'request CancellationToken,
    hosted_process: Option<Arc<crate::distribution_range_image::NativeRemoteImageSourceRegistry>>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeInitialDataAdmission {
    fn observe_original_old_predecessor(
        &self,
        plan: &Arc<NativeJointHandoffPlan>,
        writer: &Arc<crate::db::NativeWriterHostState>,
        service: &Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<Arc<crate::db::NativeWriterHistoricalPredecessor>> {
        // Reuse only the SAME original pre-freeze owner from the actual table.
        // A native/JSON receipt never reconstructs provenance after closure.
        let previous = {
            let table = self
                .old_closures
                .upgrade()
                .ok_or_else(|| invalid("OLD predecessor original table ended"))?;
            let entries = table
                .try_lock()
                .ok_or_else(|| invalid("OLD predecessor original table busy"))?;
            let held = entries.get(&plan.old_range().id).cloned();
            drop(entries);
            match held {
                None => None,
                Some(held) => {
                    let held = held
                        .try_lock()
                        .ok_or_else(|| invalid("OLD predecessor original owner busy"))?;
                    if held.plan_sha256 != plan.checksum_sha256()?
                        || !Arc::ptr_eq(&held.writer, writer)
                    {
                        return Err(invalid(
                            "OLD predecessor changed original retained plan/State",
                        ));
                    }
                    Some(
                        held.predecessor
                            .as_ref()
                            .ok_or_else(|| {
                                invalid("OLD retained owner has no original pre-freeze predecessor")
                            })?
                            .clone(),
                    )
                }
            }
        };
        match previous {
            Some(actual) => Ok(actual),
            None => {
                let actual = service
                    .try_lock()
                    .ok_or_else(|| invalid("OLD pre-freeze predecessor actual service busy"))?;
                let db = actual.database();
                let db = db
                    .try_read()
                    .ok_or_else(|| invalid("OLD pre-freeze predecessor actual DB busy"))?;
                db.capture_writer_historical_predecessor(writer.clone(), plan)
            }
        }
    }
    /// Network acquisition has already finished. Only an actual same-open
    /// native metadata owner can complete this constructor. No metadata guard
    /// spans its DB validation, and the actual observed OWN is retained.
    pub(crate) fn observe_initial_old_step<'request>(
        &'request self,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &'request CancellationToken,
    ) -> Result<NativeInitialOldStep<'request>> {
        self.observe_initial_old_step_with_process(metadata, role, own, business, parent, None)
    }
    fn observe_hosted_initial_old_step<'request>(
        &'request self,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &'request CancellationToken,
    ) -> Result<NativeInitialOldStep<'request>> {
        let process = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("actual OLD hosted process owner ended"))?;
        process.require_original_process()?;
        self.observe_initial_old_step_with_process(
            metadata,
            role,
            own,
            business,
            parent,
            Some(process),
        )
    }
    fn observe_initial_old_step_with_process<'request>(
        &'request self,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &'request CancellationToken,
        hosted_process: Option<
            Arc<crate::distribution_range_image::NativeRemoteImageSourceRegistry>,
        >,
    ) -> Result<NativeInitialOldStep<'request>> {
        parent.check()?;
        role.require_own_host_metadata(&own, business)?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("original OLD source service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("original OLD source runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let (database, slot) = {
            let source = service
                .try_lock()
                .ok_or_else(|| invalid("original OLD source service busy"))?;
            if source.node_id() != role.local_node_id() {
                return Err(invalid("OLD recipient is another actual source"));
            }
            (
                source.listener_open_guard(),
                source.native_replica_listener_store_open_guard(),
            )
        };
        let member = {
            // The metadata Store is shared with heartbeat/ingress work, so a
            // momentary holder is contention, not a refusal. The wait stays
            // bounded by the original caller and the actual OWN observation;
            // no service, DB or native-journal guard is held here.
            let store = loop {
                parent.check()?;
                own.require_live_request(business)?;
                if let Some(store) = metadata.try_lock_for(METADATA_LOCK_POLL) {
                    break store;
                }
            };
            parent.check()?;
            store.require_listener_database(&database)?;
            store.observe_owned_native_metadata_member(&own, business, parent)?
        };
        let plan = Arc::new(NativeJointHandoffPlan::from_actual_role_and_own(
            &role, &own, business,
        )?);
        let writer = self.writer();
        let predecessor = self.observe_original_old_predecessor(&plan, &writer, &service)?;
        let step = NativeInitialOldStep {
            writer,
            predecessor,
            admission: self,
            service,
            runtime,
            metadata,
            database,
            slot,
            role,
            plan,
            own,
            business,
            member,
            parent,
            hosted_process,
            _local: std::marker::PhantomData,
        };
        {
            let source = step
                .service
                .try_lock()
                .ok_or_else(|| invalid("OLD source service changed before admission"))?;
            let db = source.database();
            let db = db
                .try_read()
                .ok_or_else(|| invalid("OLD source DB busy before admission"))?;
            source.require_initial_old_step(&step, &step.plan, &step.role, &db)?;
        }
        step.require_metadata_current()?;
        step.require_live()?;
        Ok(step)
    }
}
impl NativeInitialOldStep<'_> {
    pub(crate) fn historical_predecessor(
        &self,
    ) -> &Arc<crate::db::NativeWriterHistoricalPredecessor> {
        &self.predecessor
    }
    #[cfg(test)]
    pub(crate) fn test_duplicate_predecessor_admission(&self) -> Result<serde_json::Value> {
        self.require_metadata_current()?;
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("duplicate predecessor actual service busy"))?;
        let database = service.database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("duplicate predecessor actual DB busy"))?;
        let before = db.test_native_old_close_record(self.plan.old_range().id)?;
        let other = db.capture_writer_historical_predecessor(self.writer.clone(), &self.plan)?;
        self.predecessor.require_same_owner(&self.predecessor)?;
        let different = self.predecessor.require_same_owner(&other);
        let after = db.test_native_old_close_record(self.plan.old_range().id)?;
        Ok(
            serde_json::json!({"actual_original":self.predecessor.test_evidence(),
            "actual_second_capture":other.test_evidence(),
            "same_original_owner_accepted":true,
            "distinct_genuine_capture_refusal":different.err().map(|e|e.to_string()),
            "native_close_before":before,"native_close_after":after,
            "component_actual_producers_and_same_production_guard_not_concurrent_RPC":true}),
        )
    }
    pub(crate) fn require_joint_runtime(
        &self,
        runtime: &crate::distribution_range_handoff::NativeJointHandoffRuntime,
    ) -> Result<()> {
        self.require_live()?;
        runtime.require_same_service(&self.service)
    }
    pub(crate) fn plan(&self) -> &Arc<NativeJointHandoffPlan> {
        &self.plan
    }
    pub(crate) fn role(&self) -> &Arc<RecoveryRoleAdmission> {
        &self.role
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        if !Arc::ptr_eq(&self.writer, &self.admission.writer()) {
            return Err(invalid(
                "OLD step original writer snapshot replaced by policy refresh",
            ));
        }
        if let Some(process) = &self.hosted_process {
            process.require_original_process()?;
        }
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.admission
            .require_listener(&self.service, &self.runtime)?;
        self.database.require_live()?;
        self.slot.require_live()
    }
    /// ONLY outside service/DB/native-journal guards.
    pub(crate) fn require_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let store = loop {
            self.require_live()?;
            self.own.require_live_request(self.business)?;
            if let Some(store) = self.metadata.try_lock_for(METADATA_LOCK_POLL) {
                break store;
            }
        };
        store.require_listener_database(&self.database)?;
        self.member.require_current(&store)?;
        self.require_live()
    }
    /// Complete actual local guard at each raw/native persistence boundary,
    /// without acquiring service/MStore or network. Native record validation
    /// runs OUTSIDE the producer's own native-journal critical section.
    pub(crate) fn require_database_store(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        plan: &Arc<NativeJointHandoffPlan>,
        role: &Arc<RecoveryRoleAdmission>,
    ) -> Result<()> {
        self.require_live()?;
        if !Arc::ptr_eq(plan, &self.plan)
            || !Arc::ptr_eq(role, &self.role)
            || store.native_writer_store_open_id() != self.slot.id()
            || self.admission.store_slot.id() != self.slot.id()
        {
            return Err(invalid("OLD step changed original plan/role/Store slot"));
        }
        self.database.require_database(database)?;
        self.member.require_database(database)?;
        let actual = database.current_native_range_source_lineage(plan.old_range().id)?;
        plan.require_old_native_configuration(&actual)?;
        self.writer
            .require_store(database.data_path(), self.slot.id())?;
        self.predecessor.require_original_database(database, plan)?;
        if !Arc::ptr_eq(&store.native_writer_host_state()?, &self.writer) {
            return Err(invalid("OLD step original actual writer replaced"));
        }
        database.require_native_writer_catalogue_custody(store)?;
        self.require_live()
    }
}

/// Retained BEFORE any real freeze/write and kept in the actual DATA host.
/// An uncertain producer never removes this owner or invents an observation.
pub(super) struct RetainedInitialOld {
    remote_new: Option<Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>>,
    predecessor: Option<Arc<crate::db::NativeWriterHistoricalPredecessor>>,
    writer: Arc<crate::db::NativeWriterHostState>,
    plan_sha256: String,
    plan: Arc<NativeJointHandoffPlan>,
    runtime: Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>,
    actual: Option<Arc<crate::distribution_range_handoff::NativeJointOldObservation>>,
    constraints: Option<crate::distribution_data::NativeJointIrrevocableFacts>,
    constraint_owner: Option<Arc<crate::distribution_data::ActualServiceNativeJointConstraints>>,
    // Distinct actual durable outcomes, retained before postchecks. Cleared
    // only after the SAME produced observation is stored in `actual`.
    outcome: Option<crate::distribution_data::NativeJointOldRetainedOutcome>,
}
pub(super) type RetainedInitialOldTable =
    Mutex<BTreeMap<crate::RangeId, Arc<Mutex<RetainedInitialOld>>>>;
impl NativeInitialDataHost {
    pub(super) fn retained_actual_old_source(
        &self,
        range: crate::RangeId,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeJointOldObservation>> {
        self.admission_for_old_step()?.require_live()?;
        let entries = self
            .old_closures
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor table busy"))?;
        let owner = entries
            .get(&range)
            .ok_or_else(|| invalid("actual OLD donor has no retained local closure"))?;
        let owner = owner
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor closure still active"))?;
        if !Arc::ptr_eq(&owner.writer, &self.admission_for_old_step()?.writer()) {
            return Err(invalid("retained OLD source original writer replaced"));
        }
        let actual = owner
            .actual
            .clone()
            .ok_or_else(|| invalid("actual OLD donor outcome remains uncertain"))?;
        if actual.node_id() != &self.initial.seed.spec.node_id
            || actual.plan().checksum_sha256()? != owner.plan_sha256
        {
            return Err(invalid(
                "actual OLD donor changed original local identity/scope",
            ));
        }
        actual.require_current()?;
        Ok(actual)
    }
    pub(in crate::distribution_native_host::provisional_listener::registration) fn close_initial_old(
        &self,
        runtime: Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeJointOldObservation>> {
        let admission = self.admission_for_old_step()?;
        let step = admission.observe_initial_old_step(
            self.initial.metadata.clone(),
            role,
            own,
            business,
            parent,
        )?;
        step.require_joint_runtime(&runtime)?;
        let range = step.plan().old_range().id;
        let digest = step.plan().checksum_sha256()?;
        let retained = {
            let mut owners = self
                .old_closures
                .try_lock()
                .ok_or_else(|| invalid("retained OLD owner table busy"))?;
            if let Some(previous) = owners.get(&range) {
                previous.clone()
            } else {
                if owners.len() >= 8 {
                    return Err(invalid(
                        "retained OLD capacity; existing refusals not evicted",
                    ));
                }
                let owner = Arc::new(Mutex::new(RetainedInitialOld {
                    remote_new: None,
                    predecessor: Some(step.predecessor.clone()),
                    writer: step.writer.clone(),
                    plan_sha256: digest.clone(),
                    plan: step.plan().clone(),
                    runtime: runtime.clone(),
                    actual: None,
                    constraints: None,
                    constraint_owner: None,
                    outcome: None,
                }));
                owners.insert(range, owner.clone());
                owner
            }
        };
        let mut owner = retained
            .try_lock()
            .ok_or_else(|| invalid("actual OLD operation already active"))?;
        if owner.plan_sha256 != digest
            || !Arc::ptr_eq(&owner.runtime, &runtime)
            || !Arc::ptr_eq(&owner.writer, &step.writer)
        {
            return Err(invalid(
                "OLD continuation changed original plan/runtime owner",
            ));
        }
        owner
            .predecessor
            .as_ref()
            .ok_or_else(|| invalid("OLD original pre-freeze predecessor absent"))?
            .require_same_owner(step.historical_predecessor())?;
        if owner.actual.is_none() {
            let observation = runtime.close_old_for_initial_step(&step, parent)?;
            // Actual native/promise/pin ownership BEFORE the next fallible
            // MStore/context/reply check. Error preserves this exact object.
            owner.actual = Some(Arc::new(observation));
        }
        let actual = owner.actual.as_ref().unwrap().clone();
        actual.require_current()?;
        drop(owner);
        step.require_metadata_current()?;
        step.require_live()?;
        Ok(actual)
    }
}

impl NativeMetadataControlHost {
    /// Independently retained ACTUAL native recipient objects. The remote OLD
    /// certificate remains a separate authority and must match these exactly.
    pub(crate) fn retained_initial_old_joint_owners(
        &self,
        range: crate::RangeId,
    ) -> Result<(
        Arc<crate::distribution_range_handoff::NativeJointOldObservation>,
        Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>,
    )> {
        let initial = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("actual INITIAL old recipient owner absent"))?;
        let actual = initial.retained_actual_old_source(range)?;
        let entries = initial
            .old_closures
            .try_lock()
            .ok_or_else(|| invalid("actual OLD recipient table busy"))?;
        let retained = entries
            .get(&range)
            .ok_or_else(|| invalid("actual OLD recipient entry absent"))?
            .try_lock()
            .ok_or_else(|| invalid("actual OLD recipient entry busy"))?;
        if retained
            .actual
            .as_ref()
            .is_none_or(|old| !Arc::ptr_eq(old, &actual))
        {
            return Err(invalid("actual OLD recipient identity changed"));
        }
        Ok((actual, retained.runtime.clone()))
    }
    /// Only final OLD survivors may retire their obsolete observation-table
    /// references. Actual runtime/NEW owners remain retained; retired sources
    /// keep their original CLOSED runtime until a separate cleanup protocol.
    pub(crate) fn retire_initial_old_observation_after_joint_open(
        &self,
        range: crate::RangeId,
        phase: &crate::distribution_range_handoff::NativeJointReleasePhaseQuorum,
    ) -> Result<()> {
        let initial = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("actual INITIAL survivor absent"))?;
        let actual = initial.retained_actual_old_source(range)?;
        phase.require_actual_old_owner_retirement(&actual)?;
        let mut entries = initial
            .old_closures
            .try_lock()
            .ok_or_else(|| invalid("actual survivor OLD table busy"))?;
        let retained = entries
            .get(&range)
            .ok_or_else(|| invalid("actual survivor OLD entry missing"))?;
        let entry = retained
            .try_lock()
            .ok_or_else(|| invalid("actual survivor OLD entry active"))?;
        if entry
            .actual
            .as_ref()
            .is_none_or(|old| !Arc::ptr_eq(old, &actual))
            || entry.outcome.is_some()
        {
            return Err(invalid(
                "cannot retire an uncertain/replaced OLD native outcome",
            ));
        }
        drop(entry);
        entries.remove(&range);
        Ok(())
    }
}

impl NativeInitialDataAdmission {
    pub(super) fn retained_actual_old_source(
        &self,
        range: crate::RangeId,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeJointOldObservation>> {
        self.require_live()?;
        let table = self
            .old_closures
            .upgrade()
            .ok_or_else(|| invalid("actual OLD donor table ended"))?;
        let entries = table
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor table busy"))?;
        let owner = entries
            .get(&range)
            .ok_or_else(|| invalid("actual OLD donor has no retained local closure"))?;
        let owner = owner
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor closure still active"))?;
        if !Arc::ptr_eq(&owner.writer, &self.writer()) {
            return Err(invalid("retained OLD source original writer replaced"));
        }
        let actual = owner
            .actual
            .clone()
            .ok_or_else(|| invalid("actual OLD donor outcome remains uncertain"))?;
        if actual.node_id()
            != self
                .old_transport
                .upgrade()
                .ok_or_else(|| invalid("actual OLD donor transport ended"))?
                .local_metadata_read_participant()
                .node_id()
            || actual.plan().checksum_sha256()? != owner.plan_sha256
        {
            return Err(invalid(
                "actual OLD donor changed original local identity/scope",
            ));
        }
        actual.require_current()?;
        Ok(actual)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    /// Actual retained custody diagnostics only; no owner/pin handle escapes.
    pub(crate) fn test_constraint_close_custody(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let initial = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("constraint-close original INITIAL host absent"))?;
        let table = initial
            .old_closures
            .try_lock()
            .ok_or_else(|| invalid("constraint-close original table busy"))?;
        let held = table
            .get(&range)
            .ok_or_else(|| invalid("constraint-close original owner absent"))?
            .clone();
        drop(table);
        let held = held
            .try_lock()
            .ok_or_else(|| invalid("constraint-close original custody busy"))?;
        Ok(serde_json::json!({"scope_sha256":held.plan_sha256,
            "runtime_allocation_diagnostic_only":format!("{:p}",Arc::as_ptr(&held.runtime)),
            "predecessor_allocation_diagnostic_only":held.predecessor.as_ref().map(|p|format!("{:p}",Arc::as_ptr(p))),
            "constraint_owner_allocation_diagnostic_only":held.constraint_owner.as_ref().map(|p|format!("{:p}",Arc::as_ptr(p))),
            "predecessor":held.predecessor.as_ref().map(|p|p.test_evidence()),
            "constraints":held.constraints,
            "constraint_frozen":held.constraint_owner.as_ref().map(|p|p.test_original_frozen_custody()),
            "actual_closed_frozen":held.actual.as_ref().map(|p|p.test_original_frozen_custody()),
            "actual_close_present":held.actual.is_some(),"outcome_present":held.outcome.is_some()}))
    }
}
