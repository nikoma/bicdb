//! Actual source-owner admission for ONLY OLD refusal/closure. Purpose/plan
//! DATA alone is insufficient. No APP/Strong/image-row/Serving grant.
use super::*;
#[cfg(test)]
mod sink_test;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataConsensusStore, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
#[cfg(test)]
pub(crate) use sink_test::{before_native_old_wal, CancelOldOwnBeforeNativeWal};

const METADATA_LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(5);

pub(crate) struct NativeInitialOldStep<'request> {
    admission: &'request NativeInitialDataAdmission,
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
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeInitialDataAdmission {
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
        let step = NativeInitialOldStep {
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
        self.admission
            .writer
            .require_store(database.data_path(), self.slot.id())?;
        self.admission
            .writer
            .require_database(database, actual.native_write_range()?)?;
        if !Arc::ptr_eq(&store.native_writer_host_state()?, &self.admission.writer) {
            return Err(invalid("OLD step original actual writer replaced"));
        }
        database.require_native_writer_catalogue_custody(store)?;
        self.require_live()
    }
}

/// Retained BEFORE any real freeze/write and kept in the actual DATA host.
/// An uncertain producer never removes this owner or invents an observation.
pub(super) struct RetainedInitialOld {
    plan_sha256: String,
    runtime: Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>,
    actual: Option<Arc<crate::distribution_range_handoff::NativeJointOldObservation>>,
}
impl NativeInitialDataHost {
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
                    plan_sha256: digest.clone(),
                    runtime: runtime.clone(),
                    actual: None,
                }));
                owners.insert(range, owner.clone());
                owner
            }
        };
        let mut owner = retained
            .try_lock()
            .ok_or_else(|| invalid("actual OLD operation already active"))?;
        if owner.plan_sha256 != digest || !Arc::ptr_eq(&owner.runtime, &runtime) {
            return Err(invalid(
                "OLD continuation changed original plan/runtime owner",
            ));
        }
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
