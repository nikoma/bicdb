//! First ordinary DATA policy only AFTER actual ALL-new release/Open. Distinct
//! from the legacy pre-release CLOSED assigned-image origin.
use super::*;
use crate::distribution_consensus::NativeOwnedMetadataMemberObservation;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

pub(crate) struct NativeRegisteredServingDataPolicyOrigin {
    owner: Arc<NativeRegisteredRemoteJointOwner>,
    admission: Arc<NativeJointServingPolicyAdmission>,
    member: NativeOwnedMetadataMemberObservation,
    policy: Arc<SealedHostWriterEnrollment>,
    parent: CancellationToken,
    phase: CancellationToken,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeRegisteredServingDataPolicyOrigin {
    pub(crate) fn original_joint_owner(&self) -> &Arc<NativeRegisteredRemoteJointOwner> {
        &self.owner
    }
    pub(crate) fn original_caller(&self) -> &CancellationToken {
        &self.parent
    }
    pub(crate) fn range(&self) -> &crate::RangeDescriptor {
        self.admission.data().activation_range()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        self.phase.check()?;
        self.owner.target.preflight.require_original_process()?;
        self.admission.require_live()?;
        self.policy.require_live()
    }
    pub(crate) fn require_original_empty_hook(&self, db: &BicDb) -> Result<()> {
        self.require_live()?;
        self.owner
            .target
            .preflight
            .original
            .database
            .require_database(db)?;
        self.owner.target.original_hook().require_current(db)
    }
    pub(crate) fn require_owned_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let original = &self.owner.target.preflight.original;
        let backend = original
            .backend
            .upgrade()
            .ok_or_else(|| invalid("Registered first DATA original backend ended"))?;
        let metadata = loop {
            self.require_live()?;
            if let Some(m) = backend.metadata.try_lock_for(Duration::from_millis(5)) {
                break m;
            }
        };
        metadata.require_listener_database(&original.database)?;
        self.member.require_current(&metadata)?;
        if self.member.store_id() != original.metadata_store_id {
            return Err(invalid(
                "Registered first DATA changed original metadata Store",
            ));
        }
        self.require_live()
    }
    fn require_service_identity(
        &self,
        service: &crate::distribution_data::ClusterDataNodeService,
        db: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.require_live()?;
        if !std::ptr::eq(self.policy.as_ref(), policy) {
            return Err(invalid("Registered first DATA changed exact sealed policy"));
        }
        let target = self.owner.target();
        target.preflight.original.database.require_database(db)?;
        target.preflight.original.store.require_live()?;
        self.member.require_database(db)?;
        service.with_writer_data_domain_store(db, |store| {
            if let Some(writer) = self.owner.terminal_writer() {
                writer.require_database(db, self.range())?;
                if !Arc::ptr_eq(writer, &store.native_writer_host_state()?) {
                    return Err(invalid(
                        "Registered first DATA successor changed actual installed State",
                    ));
                }
            } else {
                target.require_joint_identity(db, store)?;
            }
            if policy.store_open_id() != store.native_writer_store_open_id()
                || policy.database_incarnation() != db.incarnation_id()
                || policy.facts().local_node_id != *target.native().local_node_id()
                || policy.facts().ranges.len() != 1
            {
                return Err(invalid(
                    "Registered first DATA changed DB/slot/one-range policy scope",
                ));
            }
            policy.facts().require_range(self.range())
        })?;
        self.require_live()
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.require_live()?;
        let service = self
            .owner
            .service
            .try_lock()
            .ok_or_else(|| invalid("Registered first DATA actual service busy"))?;
        self.require_service_identity(&service, db, policy)?;
        self.admission.require_local_registered_current(
            &self.owner.service,
            &service,
            db,
            &self.owner,
        )?;
        self.require_live()
    }
    pub(crate) fn with_publication_service<R>(
        &self,
        db: &BicDb,
        policy: &SealedHostWriterEnrollment,
        operation: impl for<'scope> FnOnce(
            &'scope Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
            &'scope mut crate::distribution_data::ClusterDataNodeService,
        ) -> Result<R>,
    ) -> Result<R> {
        self.require_live()?;
        let mut service = self
            .owner
            .service
            .try_lock()
            .ok_or_else(|| invalid("Registered first DATA publication service busy"))?;
        self.require_service_identity(&service, db, policy)?;
        let result = operation(&self.owner.service, &mut service)?;
        self.require_service_identity(&service, db, policy)?;
        self.require_live()?;
        Ok(result)
    }
    /// Called only by the retained native installer AFTER exact hook publish,
    /// with its Installed guard already retained before fallible postchecks.
    pub(crate) fn retain_installed_writer(
        &self,
        state: Arc<crate::db::NativeWriterHostState>,
    ) -> Result<()> {
        self.owner.retain_terminal_writer(state)
    }
}
impl NativeRegisteredRemoteJointOwner {
    pub(crate) fn prepare_first_serving_data_policy(
        self: &Arc<Self>,
        admission: Arc<NativeJointServingPolicyAdmission>,
        metadata: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<(
        NativeRegisteredServingDataPolicyOrigin,
        Arc<SealedHostWriterEnrollment>,
    )> {
        parent.check()?;
        admission.require_external_current()?;
        admission.require_post_policy_metadata(&metadata, business)?;
        let original = &self.target.preflight.original;
        original.require_live()?;
        let backend = original
            .backend
            .upgrade()
            .ok_or_else(|| invalid("Registered first DATA backend ended"))?;
        let factory = original
            .factory
            .upgrade()
            .ok_or_else(|| invalid("Registered first DATA actual factory ended"))?;
        let runtime = original
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Registered first DATA runtime ended"))?;
        original.require_listener(&self.service, &runtime)?;
        if !factory.spec.initial_ranges.is_empty()
            || !Arc::ptr_eq(&self.service, &backend.service)
            || self.target.base().intended_activation_range() != admission.data().activation_range()
            || metadata.requester_node_id() != &factory.spec.node_id
            || metadata.requester_incarnation() != factory.spec.member_incarnation
        {
            return Err(invalid(
                "Registered first DATA lost original no-INITIAL receiver/NEW scope",
            ));
        }
        let phase = parent.child_with_deadline(metadata.deadline());
        let member = {
            let store = loop {
                phase.check()?;
                metadata.require_live_request(business)?;
                if let Some(store) = backend.metadata.try_lock_for(Duration::from_millis(5)) {
                    break store;
                }
            };
            store.require_listener_database(&original.database)?;
            store.observe_owned_native_metadata_member(&metadata, business, &phase)?
        };
        let policy = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("Registered first DATA service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Registered first DATA changed actual original DB"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered first DATA actual DB busy"))?;
            original.database.require_database(&db)?;
            member.require_database(&db)?;
            admission.require_local_registered_current(&self.service, &service, &db, self)?;
            let policy = service.prepare_native_host_writer_enrollment(&db, metadata, business)?;
            policy
                .facts()
                .require_range(admission.data().activation_range())?;
            Arc::new(policy)
        };
        let origin = NativeRegisteredServingDataPolicyOrigin {
            owner: self.clone(),
            admission,
            member,
            policy: policy.clone(),
            parent: parent.clone(),
            phase,
            _local: std::marker::PhantomData,
        };
        origin.require_owned_metadata_current()?;
        origin.require_live()?;
        Ok((origin, policy))
    }
}
