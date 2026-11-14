//! Fresh OWN metadata plus the actual protected local metadata owner. This
//! is not a range quorum, listener registration, or serving permission.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataConsensusStore, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_data::{
    ClusterDataNodeService, ClusterServiceOpenGuard, RangeWriteServiceSlotGuard,
};
use parking_lot::{Mutex, MutexGuard};

/// Only local lock contention is retried, under the SAME already acquired OWN
/// and caller. No network operation, proof renewal, engine guard or TTL change.
fn lock_actual_metadata<'a>(
    metadata: &'a Mutex<MetadataConsensusStore>,
    require_live: impl Fn() -> Result<()>,
) -> Result<MutexGuard<'a, MetadataConsensusStore>> {
    for _ in 0..1024 {
        require_live()?;
        if let Some(actual) = metadata.try_lock_for(std::time::Duration::from_millis(5)) {
            require_live()?;
            return Ok(actual);
        }
        #[cfg(test)]
        metadata_test_cut::fire("busy_slice", None);
    }
    require_live()?;
    Err(invalid(
        "writer DATA actual MStore contention slice bound exhausted",
    ))
}

pub(crate) struct NativeWriterDataDomainPolicy {
    policy: SealedHostWriterEnrollment,
    member: NativeOwnedMetadataMemberObservation,
    metadata: Weak<Mutex<MetadataConsensusStore>>,
    service: Weak<Mutex<ClusterDataNodeService>>,
    open: ClusterServiceOpenGuard,
    slot: RangeWriteServiceSlotGuard,
    caller: CancellationToken,
}
impl std::ops::Deref for NativeWriterDataDomainPolicy {
    type Target = SealedHostWriterEnrollment;
    fn deref(&self) -> &Self::Target {
        &self.policy
    }
}
impl NativeWriterDataDomainPolicy {
    pub(crate) fn from_actual(
        service: &Arc<Mutex<ClusterDataNodeService>>,
        metadata: &Arc<Mutex<MetadataConsensusStore>>,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        own.require_live_request(business)?;
        let (open, slot) = {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("writer DATA service busy before metadata"))?;
            (
                actual.listener_open_guard(),
                actual.native_replica_listener_store_open_guard(),
            )
        };
        #[cfg(test)]
        metadata_test_cut::fire("before_acquire", Some(own.deadline()));
        let member = {
            let actual = lock_actual_metadata(metadata, || {
                parent.check()?;
                own.require_live_request(business)
            })?;
            actual.require_listener_database(&open)?;
            actual.observe_owned_native_metadata_member(&own, business, parent)?
        };
        let policy = {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("writer DATA service busy before policy"))?;
            let database = actual.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("writer DATA database busy before policy"))?;
            open.require_database(&db)?;
            member.require_database(&db)?;
            if slot.id() != actual.native_replica_listener_store_open_guard().id() {
                return Err(invalid(
                    "writer DATA Store slot changed during metadata acquisition",
                ));
            }
            actual.prepare_native_host_writer_enrollment(&db, own, business)?
        };
        let result = Self {
            policy,
            member,
            metadata: Arc::downgrade(metadata),
            service: Arc::downgrade(service),
            open,
            slot,
            caller: parent.clone(),
        };
        result.require_owned_metadata_current()?;
        Ok(result)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.caller.check()?;
        self.policy.require_live()?;
        self.open.require_live()?;
        self.slot.require_live()
    }
    /// Called only outside DB/service/journal/admission guards.
    pub(crate) fn require_owned_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let metadata = self
            .metadata
            .upgrade()
            .ok_or_else(|| invalid("writer DATA actual MStore ended"))?;
        let actual = lock_actual_metadata(&metadata, || self.require_live())?;
        actual.require_listener_database(&self.open)?;
        self.member.require_current(&actual)?;
        self.require_live()
    }
    pub(super) fn publication_version_guard(
        &self,
    ) -> Result<crate::distribution_consensus::NativeMetadataPublicationGuard<'_>> {
        // These helpers are explicitly memory-only; actual native identity
        // checks already ran under the caller's lexical journal/hook scope.
        self.caller.check()?;
        self.open.require_live()?;
        self.slot.require_live()?;
        self.member.publication_version().try_publication_guard()
    }
    pub(super) fn metadata_store_id(&self) -> Uuid {
        self.member.store_id()
    }
    pub(super) fn metadata_owner(&self) -> Weak<Mutex<MetadataConsensusStore>> {
        self.metadata.clone()
    }
    pub(super) fn require_local(
        &self,
        db: &BicDb,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        slot: Uuid,
    ) -> Result<()> {
        self.require_live()?;
        if self
            .service
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, service))
            || self.slot.id() != slot
        {
            return Err(invalid("writer DATA policy changed actual service/Store"));
        }
        self.open.require_database(db)?;
        self.member.require_database_point(db)?;
        self.member.publication_version().require_current_memory()?;
        self.require_live()
    }
}

#[cfg(test)]
mod metadata_test_cut {
    use std::cell::RefCell;
    pub(crate) type Cut = Box<dyn FnMut(&'static str, Option<std::time::Instant>)>;
    thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
    pub(crate) struct Guard(Option<Cut>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|slot| {
                *slot.borrow_mut() = self.0.take();
            });
        }
    }
    pub(crate) fn install(cut: Cut) -> Guard {
        Guard(CUT.with(|slot| slot.borrow_mut().replace(cut)))
    }
    pub(super) fn fire(stage: &'static str, deadline: Option<std::time::Instant>) {
        CUT.with(|slot| {
            if let Some(cut) = slot.borrow_mut().as_mut() {
                cut(stage, deadline);
            }
        });
    }
}
#[cfg(test)]
impl BicDb {
    pub(crate) fn test_writer_policy_metadata_cut(
        cut: impl FnMut(&'static str, Option<std::time::Instant>) + 'static,
    ) -> metadata_test_cut::Guard {
        metadata_test_cut::install(Box::new(cut))
    }
}
