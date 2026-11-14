//! Distinct identity of an already-serving Joined recipient. This is NOT a
//! blank FIRST receiver, selected NoDomain proof, image or serving permission.
//! Every later operation must supply its own actual Role/OWN/native evidence.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_recovery::HandoffRoleAdmission;

pub(crate) struct NativeJoinedRangeReceiveOrigin {
    parts: registered_replica::RegisteredAssignedParts,
    original: Arc<NativeRegisteredRemoteJointOwner>,
    coordinator: std::sync::Weak<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    guard: Arc<crate::db::InstalledNativeWriterHostGuard>,
    admission: Arc<NativeInitialDataAdmission>,
    life: std::sync::Weak<AtomicBool>,
    metadata_store: Uuid,
    range: crate::RangeId,
}
impl NativeJoinedRangeReceiveOrigin {
    pub(crate) fn range_id(&self) -> crate::RangeId {
        self.range
    }
    pub(crate) fn guard(&self) -> &Arc<crate::db::InstalledNativeWriterHostGuard> {
        &self.guard
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if !self
            .life
            .upgrade()
            .is_some_and(|v| v.load(Ordering::Acquire))
        {
            return Err(invalid("additional-range original Joined owner ended"));
        }
        self.original.require_first_policy_attempt_lifetime()?;
        self.parts.database.require_live()?;
        self.parts.store.require_live()?;
        self.admission
            .require_listener(&self.parts.backend.service, &self.parts.runtime)
    }
    pub(crate) fn require_database_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_live()?;
        self.parts.database.require_database(db)?;
        db.require_joined_publication_storage_health()?;
        if self.parts.store.id() != store.native_writer_store_open_id() {
            return Err(invalid(
                "additional-range Joined original Store slot changed",
            ));
        }
        let writer = store.native_writer_host_state()?;
        if self
            .original
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
            || role.role().local_node_id() != &self.parts.backend.node_id
            || role.role().local_member_incarnation() != self.parts.factory.spec.member_incarnation
            || movement.target != self.parts.backend.node_id
            || movement.phase != crate::RelocationPhase::LearnerAllocated
            || movement.promoted_epoch.is_some()
            || !selected.replicas.iter().any(|r| {
                r.id == movement.target_replica_id
                    && r.node_id == self.parts.backend.node_id
                    && r.role == crate::RangeReplicaRole::Learner
            })
        {
            return Err(invalid(
                "Joined receive request changed actual selected target learner/plan",
            ));
        }
        let phase = parent.child_with_deadline(role.role().deadline().min(own.deadline()));
        let member = {
            let metadata = self.parts.backend.lock_metadata(&phase)?;
            metadata.require_listener_database(&self.parts.database)?;
            metadata.observe_owned_native_metadata_member(&own, business, &phase)?
        };
        if member.store_id() != self.metadata_store {
            return Err(invalid("Joined receive request replaced protected MStore"));
        }
        let request = NativeJoinedRangeReceiveRequest {
            origin: self.clone(),
            role,
            own,
            business,
            member,
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
    own: HostMetadataReadObservation,
    business: Uuid,
    member: NativeOwnedMetadataMemberObservation,
    parent: CancellationToken,
    phase: CancellationToken,
}
impl NativeJoinedRangeReceiveRequest {
    pub(crate) fn origin(&self) -> &NativeJoinedRangeReceiveOrigin {
        &self.origin
    }
    pub(crate) fn role(&self) -> &HandoffRoleAdmission {
        &self.role
    }
    pub(crate) fn member(&self) -> &NativeOwnedMetadataMemberObservation {
        &self.member
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.phase
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
        let metadata = self.origin.parts.backend.lock_metadata(&self.phase)?;
        metadata.require_listener_database(&self.origin.parts.database)?;
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
            parts: self.parts.clone(),
            original: original.registered_owner(),
            coordinator: Arc::downgrade(&enrollment.coordinator),
            guard: enrollment.guard.clone(),
            admission: self.admission.as_ref().unwrap().clone(),
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
