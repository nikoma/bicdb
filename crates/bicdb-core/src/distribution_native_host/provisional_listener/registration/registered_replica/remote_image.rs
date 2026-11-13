//! Actual Registered target request, distinct from historical source DATA.
//! This necessary request fence is not a staged-history, row or Serving grant.
use super::*;
use crate::distribution_consensus::NativeOwnedMetadataMemberObservation;
use crate::distribution_range_consensus::{NativeImageInstallRole, RangeWriteStore};
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_image::{NativeRemoteImageFact, NativeRemoteImageStep};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::HandoffRoleAdmission;

pub(crate) struct NativeRegisteredRemoteImageStep<'request> {
    original: &'request NativeRegisteredReplicaAdmission,
    frame: &'request NativeRemoteImageStep<'request>,
    role: HandoffRoleAdmission,
    own: HostMetadataReadObservation,
    business: Uuid,
    member: NativeOwnedMetadataMemberObservation,
    service: Arc<Mutex<ClusterDataNodeService>>,
    runtime: Arc<RangeRecoveryRuntime>,
    backend: Arc<NativeMetadataControlBackend>,
    plan: NativeJointHandoffPlan,
    bindings: crate::db::RangeImageDbBindings,
    frozen: &'request FrozenRangeMutationPermit,
    catalogue: Uuid,
    parent: &'request CancellationToken,
    phase: CancellationToken,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

fn canonical(mut range: crate::RangeDescriptor) -> crate::RangeDescriptor {
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|replica| replica.id);
    range
}
impl NativeRegisteredReplicaAdmission {
    /// Caller has acquired the actual recipient role, OWN MQ and pinned
    /// source frame before entering any target engine guards. Retain all
    /// three original observations; no cached manifest becomes permission.
    pub(crate) fn observe_remote_image_step<'request>(
        &'request self,
        frame: &'request NativeRemoteImageStep<'request>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        frozen: &'request FrozenRangeMutationPermit,
        parent: &'request CancellationToken,
    ) -> Result<NativeRegisteredRemoteImageStep<'request>> {
        parent.check()?;
        frame.require_request_live()?;
        role.role().require_own_host_metadata(&own, business)?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("remote image target backend ended"))?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("remote image target service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("remote image target runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| invalid("remote image target factory ended"))?;
        if !Arc::ptr_eq(&service, &backend.service)
            || !factory.spec.initial_ranges.is_empty()
            || role.role().local_node_id() != &factory.spec.node_id
            || role.role().local_member_incarnation() != factory.spec.member_incarnation
        {
            return Err(invalid(
                "remote image requires the original Registered target birth/service",
            ));
        }
        let phase = parent.child_with_deadline(role.role().deadline().min(own.deadline()));
        let member = {
            let metadata = backend.lock_metadata(&phase)?;
            metadata.require_listener_database(&self.database)?;
            metadata.observe_owned_native_metadata_member(&own, business, &phase)?
        };
        if member.store_id() != self.metadata_store_id {
            return Err(invalid("remote image target protected MStore changed"));
        }
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(role.role(), &own, business)?;
        let (bindings, catalogue) = {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("remote image target service busy"))?;
            let database = actual.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("remote image changed original target DB owner"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("remote image target DB busy"))?;
            self.database.require_database(&db)?;
            member.require_database(&db)?;
            db.require_registered_remote_image_writer_absence(&self.original_hook)?;
            db.range_mutation_gate()
                .validate_quiescent_frozen(plan.old_range().id, frozen)?;
            actual.require_native_catalog_database(&db)?;
            let native = db.native_recovery_admission(
                role.role().cluster_id(),
                role.role().bootstrap_id(),
                role.role().local_node_id(),
                role.role().local_member_incarnation(),
            )?;
            if crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?
                != *factory.actual_birth()
            {
                return Err(invalid("remote image original actual target birth changed"));
            }
            (
                db.capture_range_image_bindings()?,
                db.native_abort_catalogue_store_id()?
                    .ok_or_else(|| invalid("remote image target has no protected catalogue"))?,
            )
        };
        let step = NativeRegisteredRemoteImageStep {
            original: self,
            frame,
            role,
            own,
            business,
            member,
            service,
            runtime,
            backend,
            plan,
            bindings,
            frozen,
            catalogue,
            parent,
            phase,
            _local: std::marker::PhantomData,
        };
        step.require_live()?;
        step.require_source()?;
        {
            let actual = step
                .service
                .try_lock()
                .ok_or_else(|| invalid("remote image target service changed before return"))?;
            let database = actual.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("remote image target DB changed before return"))?;
            actual.require_registered_remote_image_step(&step, &db)?;
        }
        step.require_metadata_current()?;
        Ok(step)
    }
}

impl NativeMetadataControlHost {
    /// The actual retained receiver owns this request. Metadata checks occur
    /// outside the caller's bounded engine operation; the operation receives
    /// no reusable writer/row grant and cannot return a borrowed step.
    pub(crate) fn with_registered_remote_image_step<'request, R>(
        &'request self,
        frame: &'request NativeRemoteImageStep<'request>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        frozen: &'request FrozenRangeMutationPermit,
        parent: &'request CancellationToken,
        operation: impl for<'scope> FnOnce(
            &'scope NativeRegisteredRemoteImageStep<'request>,
        ) -> Result<R>,
    ) -> Result<R> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("remote image needs actual retained Registered receiver"))?;
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid(
                "remote Registered request cannot substitute an existing DATA owner",
            ));
        }
        let step = receiver
            .admission
            .observe_remote_image_step(frame, role, own, business, frozen, parent)?;
        step.require_metadata_current()?;
        let result = operation(&step);
        // An operation can fail after a durable refusal. Still attempt both
        // original-owner postchecks; preserve its primary error and never
        // turn a failed operation or postcheck into a successful result.
        let post = step
            .require_metadata_current()
            .and_then(|()| step.require_live());
        match result {
            Err(primary) => Err(primary),
            Ok(value) => {
                post?;
                Ok(value)
            }
        }
    }
}
impl NativeRegisteredRemoteImageStep<'_> {
    pub(crate) fn source(&self) -> &NativeRemoteImageFact {
        self.frame.source()
    }
    pub(crate) fn install_role(&self) -> NativeImageInstallRole<'_> {
        NativeImageInstallRole::CommittedHandoff(&self.role)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        self.phase.check()?;
        self.frame.require_request_live()?;
        self.role
            .role()
            .require_own_host_metadata(&self.own, self.business)?;
        self.original.require_listener(&self.service, &self.runtime)
    }
    fn require_source(&self) -> Result<()> {
        self.require_live()?;
        let source = self.source().manifest();
        let promise = self.plan.promise();
        let mut predecessor = self.plan.old_range().clone();
        predecessor
            .replicas
            .retain(|r| r.node_id != source.target_id);
        predecessor.epoch = predecessor
            .epoch
            .checked_sub(1)
            .ok_or_else(|| invalid("remote image predecessor epoch underflow"))?;
        if source.cluster_id != *self.role.role().cluster_id()
            || source.bootstrap_id != self.role.role().bootstrap_id()
            || source.target_id != *self.role.role().local_node_id()
            || source.target_birth != *self.plan.enrolled_birth(&source.target_id)?
            || source.source_birth != *self.plan.enrolled_birth(&source.source_id)?
            || !self.plan.old_members().contains_key(&source.source_id)
            || source.handoff_session != promise.session_id
            || source.nonce != promise.nonce
            || source.relocation_id != promise.relocation_id
            || source.plan_id != promise.plan_id
            || canonical(source.capture_range.clone()) != canonical(self.plan.old_range().clone())
            || canonical(source.native_predecessor.clone()) != canonical(predecessor)
            || canonical(source.activation_range.clone())
                != canonical(self.plan.activation_range().clone())
            || source.old_members != *self.plan.old_members()
            || source.new_members != *self.plan.new_members()
            || source.schema_sha256.as_str()
                != self.role.role().expected_schema_sha256().unwrap_or("")
            || source.restore.as_ref() != self.role.role().restore_activation()
        {
            return Err(invalid(
                "actual remote image fact differs from original target handoff/OWN scope",
            ));
        }
        self.require_live()
    }
    /// Full protected target read outside a native journal. Actual row/Tx
    /// admission still needs a separate verified no-domain/Installing proof
    /// and complete staged history; this method cannot supply either.
    pub(crate) fn require_database_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_source()?;
        self.original.database.require_database(db)?;
        self.member.require_database(db)?;
        db.require_registered_remote_image_writer_absence(&self.original.original_hook)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.plan.old_range().id, self.frozen)?;
        db.validate_range_image_bindings(&self.bindings)?;
        let bindings = &self.bindings;
        let source = self.source().manifest();
        if self.original.store.id() != store.native_writer_store_open_id()
            || self.original.database_incarnation != db.incarnation_id()
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue)
            || bindings.schema_sha256() != source.schema_sha256
            || bindings.reader_features() != source.readers
            || bindings.collections() != source.collections
            || bindings
                .collections()
                .iter()
                .any(|meta| meta.storage_name.is_some() || meta.renamed_from.is_some())
        {
            return Err(invalid(
                "remote image target changed original DB/Store/UID/catalogue",
            ));
        }
        db.require_native_writer_catalogue_custody(store)?;
        self.require_live()
    }
    /// Never call this while holding target service/DB/native-journal guards.
    pub(crate) fn require_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let metadata = self.backend.lock_metadata(&self.phase)?;
        metadata.require_listener_database(&self.original.database)?;
        self.member.require_current(&metadata)?;
        self.require_live()
    }
}
