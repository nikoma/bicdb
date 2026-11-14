//! Separate actual admission for constraint capture on an unresolved OLD peer.
//! A known source lineage/closed vote is deliberately not constructed here.
use super::*;

pub(crate) struct NativeInitialOldConstraintStep<'request> {
    admission: &'request NativeInitialDataAdmission,
    writer: Arc<crate::db::NativeWriterHostState>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    runtime: Arc<RangeRecoveryRuntime>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    role: Arc<RecoveryRoleAdmission>,
    plan: Arc<NativeJointHandoffPlan>,
    own: HostMetadataReadObservation,
    business: Uuid,
    member: NativeOwnedMetadataMemberObservation,
    parent: &'request CancellationToken,
    process: Arc<crate::distribution_range_image::NativeRemoteImageSourceRegistry>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeInitialDataAdmission {
    pub(super) fn observe_initial_old_constraint_step<'request>(
        &'request self,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &'request CancellationToken,
    ) -> Result<NativeInitialOldConstraintStep<'request>> {
        parent.check()?;
        role.require_own_host_metadata(&own, business)?;
        let process = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("OLD constraint original process ended"))?;
        process.require_original_process()?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("OLD constraint original service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("OLD constraint original runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let member = {
            let store = loop {
                parent.check()?;
                own.require_live_request(business)?;
                if let Some(store) = metadata.try_lock_for(METADATA_LOCK_POLL) {
                    break store;
                }
            };
            store.require_listener_database(&self.database)?;
            store.observe_owned_native_metadata_member(&own, business, parent)?
        };
        let plan = Arc::new(NativeJointHandoffPlan::from_actual_role_and_own(
            &role, &own, business,
        )?);
        let step = NativeInitialOldConstraintStep {
            writer: self.writer(),
            admission: self,
            service,
            runtime,
            metadata,
            role,
            plan,
            own,
            business,
            member,
            parent,
            process,
            _local: std::marker::PhantomData,
        };
        {
            let service = step
                .service
                .try_lock()
                .ok_or_else(|| invalid("OLD constraint actual service busy at admission"))?;
            let db = service.database();
            let db = db
                .try_read()
                .ok_or_else(|| invalid("OLD constraint actual DB busy at admission"))?;
            service.require_initial_old_constraint_step(&step, &step.plan, &step.role, &db)?;
        }
        step.require_metadata_current()?;
        step.require_live()?;
        Ok(step)
    }
}
impl NativeInitialOldConstraintStep<'_> {
    /// Exact retained writer Arc for owner identity; no current-writer lookup
    /// and no new authority. All existing live/store/plan checks still apply.
    pub(super) fn writer_snapshot(&self) -> &Arc<crate::db::NativeWriterHostState> {
        &self.writer
    }
    pub(crate) fn plan(&self) -> &Arc<NativeJointHandoffPlan> {
        &self.plan
    }
    pub(crate) fn role(&self) -> &Arc<RecoveryRoleAdmission> {
        &self.role
    }
    pub(super) fn own(&self) -> &HostMetadataReadObservation {
        &self.own
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        if !Arc::ptr_eq(&self.writer, &self.admission.writer()) {
            return Err(invalid("OLD constraint original writer snapshot replaced"));
        }
        self.process.require_original_process()?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.admission
            .require_listener(&self.service, &self.runtime)
    }
    pub(crate) fn require_joint_runtime(
        &self,
        runtime: &crate::distribution_range_handoff::NativeJointHandoffRuntime,
    ) -> Result<()> {
        self.require_live()?;
        runtime.require_same_service(&self.service)
    }
    /// Outside ALL service/DB/native guards. The original MStore is retained.
    pub(crate) fn require_metadata_current(&self) -> Result<()> {
        let store = loop {
            self.require_live()?;
            if let Some(store) = self.metadata.try_lock_for(METADATA_LOCK_POLL) {
                break store;
            }
        };
        store.require_listener_database(&self.admission.database)?;
        self.member.require_current(&store)?;
        self.require_live()
    }
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
            || store.native_writer_store_open_id() != self.admission.store_slot.id()
            || !Arc::ptr_eq(&store.native_writer_host_state()?, &self.writer)
        {
            return Err(invalid(
                "OLD constraint changed original plan/role/slot/writer",
            ));
        }
        self.admission.database.require_database(database)?;
        self.member.require_database(database)?;
        self.admission.store_slot.require_live()?;
        self.writer
            .require_store(database.data_path(), self.admission.store_slot.id())?;
        self.writer
            .require_old_constraint_predecessor(database, plan)?;
        database.require_native_writer_catalogue_custody(store)?;
        self.require_live()
    }
}
