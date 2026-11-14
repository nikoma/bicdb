//! Distinct identity of an already-serving Joined recipient. This is NOT a
//! blank FIRST receiver, selected NoDomain proof, image or serving permission.
//! Every later operation must supply its own actual Role/OWN/native evidence.
use super::*;
mod attempt;
mod catalogue;
mod joint_owner;
mod materialized;
pub(super) use attempt::NativeJoinedRemoteInstallAttempt;
pub(crate) use attempt::{NativeJoinedRemoteImagePoint, NativeJoinedRemoteTransactionRequest};
pub(crate) use joint_owner::NativeJoinedRemoteJointOwner;
pub(crate) use materialized::NativeJoinedRemoteMaterializedOwner;
mod install;
mod preflight;
use crate::distribution_consensus::{
    HostMetadataReadObservation, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_data::{ClusterServiceOpenGuard, RangeWriteServiceSlotGuard};
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_recovery::HandoffRoleAdmission;
pub(crate) use install::NativeJoinedRemoteInstallPreflight;
pub(crate) use preflight::NativeJoinedRangeReceivePreflight;

pub(crate) struct NativeJoinedRangeReceiveOrigin {
    // Runtime retains this origin through its real pending A. No strong edge
    // may lead back to runtime/service/host, including through FIRST release.
    backend: std::sync::Weak<NativeMetadataControlBackend>,
    factory: std::sync::Weak<NativeUnassignedLearnerFactory>,
    runtime: std::sync::Weak<RangeRecoveryRuntime>,
    original: std::sync::Weak<NativeRegisteredRemoteJointOwner>,
    database: ClusterServiceOpenGuard,
    store: RangeWriteServiceSlotGuard,
    node_id: ClusterNodeId,
    incarnation: u64,
    coordinator: std::sync::Weak<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    guard: Arc<crate::db::InstalledNativeWriterHostGuard>,
    admission: std::sync::Weak<NativeInitialDataAdmission>,
    life: std::sync::Weak<AtomicBool>,
    metadata_store: Uuid,
    range: crate::RangeId,
}
impl NativeJoinedRangeReceiveOrigin {
    fn backend(&self) -> Result<Arc<NativeMetadataControlBackend>> {
        self.backend
            .upgrade()
            .ok_or_else(|| invalid("Joined receive original backend ended"))
    }
    fn factory(&self) -> Result<Arc<NativeUnassignedLearnerFactory>> {
        self.factory
            .upgrade()
            .ok_or_else(|| invalid("Joined receive original factory ended"))
    }
    fn runtime(&self) -> Result<Arc<RangeRecoveryRuntime>> {
        self.runtime
            .upgrade()
            .ok_or_else(|| invalid("Joined receive original runtime ended"))
    }
    fn admission(&self) -> Result<Arc<NativeInitialDataAdmission>> {
        self.admission
            .upgrade()
            .ok_or_else(|| invalid("Joined receive original admission ended"))
    }
    fn original(&self) -> Result<Arc<NativeRegisteredRemoteJointOwner>> {
        self.original
            .upgrade()
            .ok_or_else(|| invalid("Joined receive original FIRST owner ended"))
    }
    pub(crate) fn range_id(&self) -> crate::RangeId {
        self.range
    }
    pub(crate) fn guard(&self) -> &Arc<crate::db::InstalledNativeWriterHostGuard> {
        &self.guard
    }
    /// Memory/open/slot lifetime only. Unlike require_live, this never calls
    /// the installed writer's filesystem/root checks or obtains engine locks.
    pub(crate) fn require_live_memory(&self) -> Result<()> {
        if !self
            .life
            .upgrade()
            .is_some_and(|v| v.load(Ordering::Acquire))
        {
            return Err(invalid("additional-range original Joined owner ended"));
        }
        // This is only an ended-owner refusal, not an authority or usable
        // owner grant. Never upgrade/drop a potentially final complex owner
        // under a publication guard. The actual lexical physical point holds
        // the checked State/hook/open; full owner checks run before that gate.
        if self.original.strong_count() == 0 {
            return Err(invalid("Joined receive original FIRST owner ended"));
        }
        self.database.require_live()?;
        self.store.require_live()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.require_live_memory()?;
        self.original()?.require_first_policy_attempt_lifetime()?;
        let backend = self.backend()?;
        let runtime = self.runtime()?;
        self.admission()?
            .require_listener(&backend.service, &runtime)
    }
    pub(crate) fn require_database_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_live()?;
        self.database.require_database(db)?;
        db.require_joined_publication_storage_health()?;
        if self.store.id() != store.native_writer_store_open_id() {
            return Err(invalid(
                "additional-range Joined original Store slot changed",
            ));
        }
        let writer = store.native_writer_host_state()?;
        if self
            .original()?
            .terminal_writer()
            .is_none_or(|old| !Arc::ptr_eq(old, &writer))
        {
            return Err(invalid(
                "additional-range Joined replaced original FIRST State",
            ));
        }
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| invalid("additional-range original Joined coordinator ended"))?;
        self.guard
            .require_data_domain_origin(db, &coordinator, &writer)?;
        self.require_live()
    }
    /// No engine/service/native journal locks span the actual MStore capture.
    /// The caller supplies actual request-specific Role and OWN, never DTOs.
    pub(crate) fn observe_request(
        self: &Arc<Self>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeJoinedRangeReceiveRequest> {
        self.require_live()?;
        parent.check()?;
        role.role().require_own_host_metadata(&own, business)?;
        let selected = role.role().current_range();
        let movement = role.committed_relocation();
        if selected.id != self.range
            || role.role().local_node_id() != &self.node_id
            || role.role().local_member_incarnation() != self.incarnation
            || movement.target != self.node_id
            || movement.phase != crate::RelocationPhase::LearnerAllocated
            || movement.promoted_epoch.is_some()
            || !selected.replicas.iter().any(|r| {
                r.id == movement.target_replica_id
                    && r.node_id == self.node_id
                    && r.role == crate::RangeReplicaRole::Learner
            })
        {
            return Err(invalid(
                "Joined receive request changed actual selected target learner/plan",
            ));
        }
        let phase = parent.child_with_deadline(role.role().deadline().min(own.deadline()));
        let member = {
            let backend = self.backend()?;
            let metadata = backend.lock_metadata(&phase)?;
            metadata.require_listener_database(&self.database)?;
            metadata.observe_owned_native_metadata_member(&own, business, &phase)?
        };
        if member.store_id() != self.metadata_store {
            return Err(invalid("Joined receive request replaced protected MStore"));
        }
        let request = NativeJoinedRangeReceiveRequest {
            origin: self.clone(),
            role,
            own: Arc::new(own),
            business,
            member: Arc::new(member),
            parent: parent.clone(),
            phase,
        };
        request.require_metadata_current()?;
        Ok(request)
    }
}
/// Finite genuine current request. Neither cloning the origin nor retaining
/// this value renews a Role/OWN deadline or supplies any absent storage proof.
pub(crate) struct NativeJoinedRangeReceiveRequest {
    origin: Arc<NativeJoinedRangeReceiveOrigin>,
    role: HandoffRoleAdmission,
    own: Arc<HostMetadataReadObservation>,
    business: Uuid,
    member: Arc<NativeOwnedMetadataMemberObservation>,
    parent: CancellationToken,
    phase: CancellationToken,
}
impl NativeJoinedRangeReceiveRequest {
    pub(crate) fn origin(&self) -> &NativeJoinedRangeReceiveOrigin {
        &self.origin
    }
    pub(crate) fn plan(&self) -> Result<crate::distribution_range_handoff::NativeJointHandoffPlan> {
        self.require_live()?;
        crate::distribution_range_handoff::NativeJointHandoffPlan::from_actual_role_and_own(
            self.role.role(),
            &self.own,
            self.business,
        )
    }
    pub(crate) fn role(&self) -> &HandoffRoleAdmission {
        &self.role
    }
    pub(crate) fn origin_arc(&self) -> &Arc<NativeJoinedRangeReceiveOrigin> {
        &self.origin
    }
    /// Runtime identity lookup occurs outside service/DB/journal locks. A
    /// caller-supplied Frozen with the same scalar IDs is insufficient.
    pub(crate) fn require_original_frozen(
        &self,
        frozen: &Arc<crate::distribution_range_mutation::FrozenRangeMutationPermit>,
    ) -> Result<()> {
        self.require_live()?;
        let scope =
            crate::distribution_range_consensus::RecoveryFenceScope::from_role(self.role.role())?;
        let actual = self.origin.runtime()?.source_capture_freeze(&scope)?;
        if !Arc::ptr_eq(&actual, frozen) {
            return Err(invalid(
                "Joined selected scan lost actual original runtime Frozen",
            ));
        }
        let factory = self.origin.factory()?;
        factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Joined selected scan actual DB busy before freeze check"))?
            .range_mutation_gate()
            .validate_quiescent_frozen(self.origin.range, frozen)?;
        self.require_live()
    }
    /// Own all external checks around the actual DB borrow. No MStore or
    /// runtime/session guard spans an engine borrow or the actual scan IO.
    pub(crate) fn begin_row_absence_scan(
        &self,
        frozen: &Arc<crate::distribution_range_mutation::FrozenRangeMutationPermit>,
    ) -> Result<crate::db::NativeJoinedRowAbsenceScan> {
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        let scan = {
            let factory = self.origin.factory()?;
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("Joined selected scan DB busy at begin"))?;
            crate::db::NativeJoinedRowAbsenceScan::begin_local(&db, self, frozen)?
        };
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        Ok(scan)
    }
    pub(crate) fn advance_row_absence_scan(
        &self,
        scan: &mut crate::db::NativeJoinedRowAbsenceScan,
        frozen: &Arc<crate::distribution_range_mutation::FrozenRangeMutationPermit>,
    ) -> Result<bool> {
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        let result = {
            let factory = self.origin.factory()?;
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("Joined selected scan DB busy at advance"))?;
            scan.advance_local(&db, self, frozen)
        };
        // A local successful chunk can precede a stale outside postcheck. Its
        // cursor remains retained; no absence/grant is returned on that error.
        let complete = result?;
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        Ok(complete)
    }
    pub(crate) fn require_row_absence_complete(
        &self,
        scan: &crate::db::NativeJoinedRowAbsenceScan,
        frozen: &Arc<crate::distribution_range_mutation::FrozenRangeMutationPermit>,
    ) -> Result<()> {
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        {
            let factory = self.origin.factory()?;
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("Joined selected scan DB busy at completion check"))?;
            scan.require_complete_local(&db, self, frozen)?;
        }
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)
    }
    pub(crate) fn own(&self) -> &HostMetadataReadObservation {
        &self.own
    }
    pub(crate) fn business(&self) -> Uuid {
        self.business
    }
    pub(crate) fn member(&self) -> &NativeOwnedMetadataMemberObservation {
        &self.member
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.phase
    }
    pub(crate) fn require_request_memory(&self) -> Result<()> {
        self.parent.check()?;
        self.phase.check()?;
        self.origin.require_live_memory()?;
        self.role.role().require_live_request()?;
        self.own.require_live_request(self.business)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        self.phase.check()?;
        self.origin.require_live()?;
        self.role
            .role()
            .require_own_host_metadata(&self.own, self.business)
    }
    /// Outside engine/service/journal locks. Local version is NOT a distributed
    /// metadata lease; later sinks still need actual native epoch/quorum fences.
    pub(crate) fn require_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let backend = self.origin.backend()?;
        let metadata = backend.lock_metadata(&self.phase)?;
        metadata.require_listener_database(&self.origin.database)?;
        self.member.require_current(&metadata)?;
        self.require_live()
    }
}
impl NativeJoinedDataHost {
    fn capture_receive_origin(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJoinedRangeReceiveOrigin>> {
        parent.check()?;
        self.require_writer_data_owner()?;
        if !self.parts.factory.spec.initial_ranges.is_empty() {
            return Err(invalid(
                "Joined receive origin cannot adopt INITIAL genesis",
            ));
        }
        if self.selected_range.as_ref().is_some_and(|r| r.id == range) {
            return Err(invalid(
                "additional Joined receive cannot replace its first range",
            ));
        }
        let metadata_store = {
            let metadata = self.parts.backend.lock_metadata(parent)?;
            metadata.require_listener_database(&self.parts.database)?;
            metadata.registered_native_metadata_member_store_id()?
        };
        let original = self.registered.as_ref().unwrap();
        let enrollment = self.enrollment.as_ref().unwrap();
        let origin = Arc::new(NativeJoinedRangeReceiveOrigin {
            backend: Arc::downgrade(&self.parts.backend),
            factory: Arc::downgrade(&self.parts.factory),
            runtime: Arc::downgrade(&self.parts.runtime),
            original: Arc::downgrade(&original.registered_owner()),
            database: self.parts.database.clone(),
            store: self.parts.store.clone(),
            node_id: self.parts.backend.node_id.clone(),
            incarnation: self.parts.factory.spec.member_incarnation,
            coordinator: Arc::downgrade(&enrollment.coordinator),
            guard: enrollment.guard.clone(),
            admission: Arc::downgrade(self.admission.as_ref().unwrap()),
            life: Arc::downgrade(&self.life),
            metadata_store,
            range,
        });
        let service = self
            .parts
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("Joined receive identity actual service busy"))?;
        let database = service.database();
        if !Arc::ptr_eq(&database, &self.parts.factory.database) {
            return Err(invalid("Joined receive identity actual DB owner changed"));
        }
        let db = database
            .try_read()
            .ok_or_else(|| invalid("Joined receive identity actual DB busy"))?;
        service.with_writer_data_domain_store(&db, |store| {
            origin.require_database_store(&db, store)
        })?;
        parent.check()?;
        Ok(origin)
    }
}
impl NativeMetadataControlHost {
    /// Necessary same-open Joined identity only. The receiver attempt must
    /// retain this value before work; it cannot be used as FIRST/NoDomain/R0.
    pub(crate) fn capture_joined_range_receive_origin(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJoinedRangeReceiveOrigin>> {
        if self.data.is_some() || self.assigned.is_some() {
            return Err(invalid(
                "additional Joined origin requires its exclusive actual host",
            ));
        }
        self.joined
            .as_ref()
            .ok_or_else(|| invalid("actual Joined receive owner absent"))?
            .capture_receive_origin(range, parent)
    }
}
