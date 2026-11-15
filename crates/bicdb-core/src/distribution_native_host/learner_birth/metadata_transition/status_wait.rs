//! Test-only rendezvous at the real owned-status MStore borrow.
use super::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(crate) struct OwnedMetadataStatusBarrier {
    entered: AtomicBool,
    released: AtomicBool,
    busy: AtomicBool,
}
impl OwnedMetadataStatusBarrier {
    pub(crate) fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
    }
    pub(crate) fn busy(&self) -> bool {
        self.busy.load(Ordering::Acquire)
    }
}
thread_local! {
    static ACTIVE: RefCell<Option<Arc<OwnedMetadataStatusBarrier>>> = const { RefCell::new(None) };
}
struct Restore(Option<Arc<OwnedMetadataStatusBarrier>>);
impl Drop for Restore {
    fn drop(&mut self) {
        ACTIVE.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
pub(super) fn before_actual_lock(
    observed: &HostMetadataReadObservation,
    business: Uuid,
    parent: &CancellationToken,
) -> Result<()> {
    let barrier = ACTIVE.with(|slot| slot.borrow().clone());
    if let Some(barrier) = barrier {
        barrier.entered.store(true, Ordering::Release);
        while !barrier.released.load(Ordering::Acquire) {
            parent.check()?;
            observed.require_live_request(business)?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    parent.check()?;
    observed.require_live_request(business)
}
pub(super) fn record_actual_busy_acquisition() {
    ACTIVE.with(|slot| {
        if let Some(barrier) = slot.borrow().as_ref() {
            barrier.busy.store(true, Ordering::Release);
        }
    });
}
impl NativeUnassignedLearnerHost {
    pub(crate) fn test_owned_metadata_status_barrier() -> Arc<OwnedMetadataStatusBarrier> {
        Arc::new(OwnedMetadataStatusBarrier::default())
    }
    pub(crate) fn test_owned_metadata_snapshot_wait(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        barrier: Arc<OwnedMetadataStatusBarrier>,
    ) -> Result<Option<(Uuid, crate::MetadataConsensusStatus, bool)>> {
        let previous = ACTIVE.with(|slot| slot.replace(Some(barrier)));
        let _restore = Restore(previous);
        self.owned_metadata_snapshot(observed, business, parent)
    }
}
