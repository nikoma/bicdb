//! Local accept-loop readiness only. This is a necessary scheduler refusal,
//! never a vote, quorum, metadata lease or proof of remote reachability.
use super::*;
use crate::distribution_consensus::MetadataConsensusStore;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;

#[derive(Debug, Default)]
pub(super) struct NativeMetadataIngressRegistry {
    listeners: Mutex<Vec<Weak<NativeMetadataIngressRegistration>>>,
}
#[derive(Debug)]
pub(crate) struct NativeMetadataIngressRegistration {
    started: AtomicBool,
    revoked: AtomicBool,
    metadata: Weak<Mutex<MetadataConsensusStore>>,
    slot: RangeWriteServiceSlotGuard,
}
impl NativeMetadataIngressRegistration {
    /// Only the actual accept thread marks start; a stopped registration can
    /// never be revived, even if shutdown wins the thread-start race.
    pub(crate) fn accept_loop_started(&self) {
        self.started.store(true, Ordering::Release);
    }
    pub(crate) fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }
    fn live_for(&self, actual: &Arc<Mutex<MetadataConsensusStore>>) -> bool {
        self.started.load(Ordering::Acquire)
            && !self.revoked.load(Ordering::Acquire)
            && self.slot.require_live().is_ok()
            && self
                .metadata
                .upgrade()
                .is_some_and(|m| Arc::ptr_eq(&m, actual))
    }
}
impl NativeMetadataIngressRegistry {
    pub(super) fn register(
        &self,
        metadata: &Arc<Mutex<MetadataConsensusStore>>,
        slot: RangeWriteServiceSlotGuard,
    ) -> Result<Arc<NativeMetadataIngressRegistration>> {
        slot.require_live()?;
        let mut listeners = self
            .listeners
            .try_lock()
            .ok_or_else(|| data_error("metadata ingress registration busy"))?;
        listeners.retain(|entry| entry.strong_count() != 0);
        if listeners.len() >= 64 {
            return Err(data_error("metadata ingress registration bound exceeded"));
        }
        let actual = Arc::new(NativeMetadataIngressRegistration {
            started: AtomicBool::new(false),
            revoked: AtomicBool::new(false),
            metadata: Arc::downgrade(metadata),
            slot,
        });
        listeners.push(Arc::downgrade(&actual));
        Ok(actual)
    }
    pub(super) fn has_live_for(
        &self,
        metadata: &Arc<Mutex<MetadataConsensusStore>>,
    ) -> Result<bool> {
        let listeners = self
            .listeners
            .try_lock()
            .ok_or_else(|| data_error("metadata ingress observation busy"))?;
        Ok(listeners
            .iter()
            .filter_map(Weak::upgrade)
            .any(|entry| entry.live_for(metadata)))
    }
}
