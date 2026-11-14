//! Actual same-service metadata phase point, captured OUTSIDE engine locks.
//! Local version protection composes with fresh quorum/range fencing; it is
//! never a distributed metadata lease or actual row-read readiness.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, NativeMetadataPublicationGuard,
    NativeOwnedMetadataMemberObservation,
};

pub(crate) struct ActualNativeJointMetadataPoint {
    registration: Arc<NativeMetadataIngressRegistration>,
    metadata: Arc<Mutex<crate::MetadataConsensusStore>>,
    database: ClusterServiceOpenGuard,
    slot: RangeWriteServiceSlotGuard,
    member: NativeOwnedMetadataMemberObservation,
    context: Uuid,
    term: u64,
    index: u64,
    phase: CancellationToken,
}
pub(crate) struct ActualNativeJointMetadataPublicationGuard<'a> {
    point: &'a ActualNativeJointMetadataPoint,
    version: NativeMetadataPublicationGuard<'a>,
}
impl ActualNativeJointMetadataPoint {
    pub(crate) fn require_request(
        &self,
        own: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        self.phase.check()?;
        own.require_live_request(business)?;
        if own.context_id() != self.context
            || own.term() != self.term
            || own.read_index() != self.index
            || own.committed_topology(business)? != self.member.topology()
        {
            return Err(data_error(
                "actual joint metadata point changed original OWN request",
            ));
        }
        self.require_current_memory()
    }
    /// Actual MStore recheck only outside service/DB/journal guards.
    pub(crate) fn require_external_current(&self) -> Result<()> {
        self.require_current_memory()?;
        let metadata = loop {
            self.phase.check()?;
            if let Some(metadata) = self
                .metadata
                .try_lock_for(std::time::Duration::from_millis(5))
            {
                break metadata;
            }
        };
        metadata.require_listener_database(&self.database)?;
        self.member.require_current(&metadata)?;
        self.require_current_memory()
    }
    /// Immutable protected owner/open/birth/namespaces + actual version. This
    /// does NOT relabel require_database_point as a current-MStore check.
    pub(crate) fn require_native_point(&self, db: &BicDb) -> Result<()> {
        self.database.require_database(db)?;
        self.slot.require_live()?;
        self.member.require_database_point(db)?;
        self.require_current_memory()
    }
    pub(crate) fn require_native_store_point(
        &self,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_native_point(db)?;
        if self.slot.id() != store.native_writer_store_open_id() {
            return Err(data_error(
                "joint metadata point changed actual range Store slot",
            ));
        }
        Ok(())
    }
    /// Memory-only; safe inside a short version publication guard.
    pub(crate) fn require_current_memory(&self) -> Result<()> {
        self.phase.check()?;
        self.database.require_live()?;
        self.slot.require_live()?;
        self.registration.require_current_memory(&self.metadata)?;
        self.member.publication_version().require_current_memory()
    }
    pub(crate) fn try_publication_guard(
        &self,
    ) -> Result<ActualNativeJointMetadataPublicationGuard<'_>> {
        self.require_current_memory()?;
        let version = self.member.publication_version().try_publication_guard()?;
        let guard = ActualNativeJointMetadataPublicationGuard {
            point: self,
            version,
        };
        guard.require_current_memory()?;
        Ok(guard)
    }
}
impl ActualNativeJointMetadataPublicationGuard<'_> {
    pub(crate) fn require_current_memory(&self) -> Result<()> {
        self.point.phase.check()?;
        self.point.database.require_live()?;
        self.point.slot.require_live()?;
        self.point
            .registration
            .require_current_memory(&self.point.metadata)?;
        self.version.require_current_memory()
    }
}
impl ClusterDataNodeService {
    /// A momentary service latch obtains the actual attached MStore/slot only.
    /// It is dropped BEFORE current-MStore acquisition or any network work.
    pub(crate) fn capture_actual_joint_metadata_point(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Option<Arc<ActualNativeJointMetadataPoint>>> {
        parent.check()?;
        own.require_live_request(business)?;
        let (registration, database, slot) = {
            let actual = service
                .try_lock()
                .ok_or_else(|| data_error("joint metadata service identity busy"))?;
            let database = actual.listener_open_guard();
            let slot = actual.native_replica_listener_store_open_guard();
            // Explicit legacy metadata-init0 fixtures have no protected native
            // owner. Never try to mint an owned member from their generic
            // listener. Every protected-init1 native sink independently refuses
            // None, so this is not a compatibility escape for a born actor.
            if !actual.storage_policy.requires_owned_metadata_point()? {
                return Ok(None);
            }
            let registration = database.actual_joint_metadata_registration(slot.id())?;
            if registration.is_none() {
                return Err(data_error(
                    "native metadata joint phase lacks actual attached current-MStore owner",
                ));
            }
            (registration, database, slot)
        };
        // Explicitly legacy lower-level fixtures have no protected native
        // metadata actor. They do not acquire a versioned-native qualification.
        let Some(registration) = registration else {
            return Ok(None);
        };
        let metadata = registration.actual_metadata()?;
        let phase = parent.child_with_deadline(own.deadline());
        let member = {
            let actual = loop {
                phase.check()?;
                own.require_live_request(business)?;
                if let Some(actual) = metadata.try_lock_for(std::time::Duration::from_millis(5)) {
                    break actual;
                }
            };
            actual.require_listener_database(&database)?;
            let member = actual.observe_owned_native_metadata_member(own, business, &phase)?;
            member.require_current(&actual)?;
            member
        };
        let point = Arc::new(ActualNativeJointMetadataPoint {
            registration,
            metadata,
            database,
            slot,
            member,
            context: own.context_id(),
            term: own.term(),
            index: own.read_index(),
            phase,
        });
        point.require_request(own, business)?;
        Ok(Some(point))
    }
}
