//! Original Registered receiver -> CLOSED policy provenance. No INITIAL owner,
//! empty-genesis reconstruction, Ready, serving or deserialized authority.
use super::*;
use crate::distribution_consensus::NativeOwnedMetadataMemberObservation;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze,
};

pub(crate) struct NativeAssignedDataPolicyOrigin {
    original: Arc<NativeRegisteredReplicaAdmission>,
    evidence: crate::distribution_data::NativeAssignedImagePolicyEvidence,
    member: NativeOwnedMetadataMemberObservation,
    parent: CancellationToken,
    phase: CancellationToken,
    policy: Arc<SealedHostWriterEnrollment>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeAssignedDataPolicyOrigin {
    pub(crate) fn with_publication_service<R>(
        &self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
        operation: impl for<'scope> FnOnce(
            &'scope Arc<Mutex<ClusterDataNodeService>>,
            &'scope mut ClusterDataNodeService,
        ) -> Result<R>,
    ) -> Result<R> {
        self.require_live()?;
        if !std::ptr::eq(self.policy.as_ref(), policy) {
            return Err(invalid("assigned publication changed sealed policy"));
        }
        let actual = self
            .original
            .service
            .upgrade()
            .ok_or_else(|| invalid("assigned publication service ended"))?;
        let backend = self
            .original
            .backend
            .upgrade()
            .ok_or_else(|| invalid("assigned publication backend ended"))?;
        if !Arc::ptr_eq(&actual, &backend.service) {
            return Err(invalid("assigned publication actual service changed"));
        }
        let mut service = actual
            .try_lock()
            .ok_or_else(|| invalid("assigned publication service busy"))?;
        self.evidence
            .require_service_identity(&service, database, policy)?;
        let result = operation(&actual, &mut service)?;
        self.evidence
            .require_service_identity(&service, database, policy)?;
        self.require_live()?;
        Ok(result)
    }
    pub(crate) fn original_caller(&self) -> &CancellationToken {
        &self.parent
    }
    pub(crate) fn range(&self) -> &crate::RangeDescriptor {
        self.evidence.range()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        self.phase.check()?;
        self.policy.require_live()?;
        self.original.require_live()?;
        self.evidence.require_live()
    }
    /// Original EMPTY HOOK identity until actual hook publication. This does
    /// not assert that native policy records are absent: the retained attempt
    /// separately checks absence before its first write, then reconciles its
    /// exact NEW audit. After hook publication this check is never used again.
    pub(crate) fn require_original_empty_hook(&self, database: &BicDb) -> Result<()> {
        self.require_live()?;
        self.original.database.require_database(database)?;
        self.original.original_hook.require_current(database)
    }
    /// No metadata/network wait belongs under DB/admission/journal guards.
    pub(crate) fn require_owned_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let backend = self
            .original
            .backend
            .upgrade()
            .ok_or_else(|| invalid("assigned policy original backend ended"))?;
        let metadata = loop {
            self.require_live()?;
            if let Some(metadata) = backend.metadata.try_lock_for(Duration::from_millis(5)) {
                self.require_live()?;
                break metadata;
            }
        };
        metadata.require_listener_database(&self.original.database)?;
        self.member.require_current(&metadata)?;
        if self.member.store_id() != self.original.metadata_store_id {
            return Err(invalid("assigned policy protected metadata Store changed"));
        }
        self.require_live()
    }
    /// Full actual image/decision/open/Store check, WITHOUT inherited journal
    /// or service guards. Captured actual image pin remains owned throughout.
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.require_live()?;
        if !std::ptr::eq(self.policy.as_ref(), policy) {
            return Err(invalid(
                "assigned origin changed its actual sealed policy object",
            ));
        }
        self.original.database.require_database(database)?;
        self.member.require_database(database)?;
        let backend = self
            .original
            .backend
            .upgrade()
            .ok_or_else(|| invalid("assigned policy original backend ended"))?;
        let actual = self
            .original
            .service
            .upgrade()
            .ok_or_else(|| invalid("assigned policy actual service ended"))?;
        if !Arc::ptr_eq(&actual, &backend.service) {
            return Err(invalid("assigned policy original actual service changed"));
        }
        let service = actual
            .try_lock()
            .ok_or_else(|| invalid("assigned policy actual service busy"))?;
        self.evidence.require_current(&service, database, policy)?;
        self.require_live()
    }
}
impl NativeRegisteredReplicaAdmission {
    pub(crate) fn prepare_assigned_data_policy(
        self: &Arc<Self>,
        image: Arc<NativeImageCatalogueFinalization>,
        frozen: Arc<NativeImageInstallFreeze>,
        metadata: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<(
        NativeAssignedDataPolicyOrigin,
        Arc<SealedHostWriterEnrollment>,
    )> {
        self.require_live()?;
        parent.check()?;
        metadata.require_live_request(business)?;
        let phase = parent.child_with_deadline(metadata.deadline());
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("assigned policy backend ended"))?;
        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| invalid("assigned policy original factory ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("assigned policy original runtime ended"))?;
        self.require_listener(&backend.service, &runtime)?;
        if !factory.spec.initial_ranges.is_empty()
            || metadata.requester_node_id() != &factory.spec.node_id
            || metadata.requester_incarnation() != factory.spec.member_incarnation
        {
            return Err(invalid(
                "assigned policy requires the actual original unassigned birth",
            ));
        }
        let member = {
            let store = loop {
                phase.check()?;
                metadata.require_live_request(business)?;
                if let Some(store) = backend.metadata.try_lock_for(Duration::from_millis(5)) {
                    phase.check()?;
                    metadata.require_live_request(business)?;
                    break store;
                }
            };
            store.require_listener_database(&self.database)?;
            store.observe_owned_native_metadata_member(&metadata, business, &phase)?
        };
        let (evidence, policy) = {
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("assigned policy service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid(
                    "assigned policy cannot change the original JoinFresh DB",
                ));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("assigned policy DB busy"))?;
            self.database.require_database(&db)?;
            self.original_hook.require_current(&db)?;
            member.require_database(&db)?;
            if image.native_admission().birth_id() != factory.actual_birth().birth_id()
                || image.native_admission().birth_sha256() != factory.actual_birth().birth_sha256()
            {
                return Err(invalid(
                    "assigned policy actual finalized image birth differs",
                ));
            }
            drop(db);
            service
                .prepare_assigned_image_writer_policy(image, frozen, metadata, business, &phase)?
        };
        let policy = Arc::new(policy);
        let origin = NativeAssignedDataPolicyOrigin {
            original: self.clone(),
            evidence,
            member,
            parent: parent.clone(),
            phase,
            policy: policy.clone(),
            _local: std::marker::PhantomData,
        };
        origin.require_owned_metadata_current()?;
        origin.require_live()?;
        Ok((origin, policy))
    }
}
