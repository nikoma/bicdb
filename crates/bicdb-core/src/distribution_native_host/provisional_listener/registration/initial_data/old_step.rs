//! Actual source-owner admission for ONLY OLD refusal/closure. Purpose/plan
//! DATA alone is insufficient. No APP/Strong/image-row/Serving grant.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataConsensusStore, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

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
            let store = metadata
                .try_lock()
                .ok_or_else(|| invalid("original OLD source metadata busy"))?;
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
        let store = self
            .metadata
            .try_lock()
            .ok_or_else(|| invalid("OLD source metadata changed during operation"))?;
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
