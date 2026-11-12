//! Test-only rendezvous AFTER actual factory validation, before final MStore borrow.
use super::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(crate) struct OwnedMetadataValidationBarrier {
    entered: AtomicBool,
    released: AtomicBool,
    busy: AtomicBool,
}
impl OwnedMetadataValidationBarrier {
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
    static ACTIVE: RefCell<Option<Arc<OwnedMetadataValidationBarrier>>> = const { RefCell::new(None) };
}
struct Restore(Option<Arc<OwnedMetadataValidationBarrier>>);
impl Drop for Restore {
    fn drop(&mut self) {
        ACTIVE.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
pub(super) fn after_actual_factory_validation(token: &CancellationToken) -> Result<()> {
    let barrier = ACTIVE.with(|slot| slot.borrow().clone());
    if let Some(barrier) = barrier {
        barrier.entered.store(true, Ordering::Release);
        while !barrier.released.load(Ordering::Acquire) {
            token.check()?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    token.check()
}
pub(super) fn record_actual_busy_acquisition() {
    ACTIVE.with(|slot| {
        if let Some(barrier) = slot.borrow().as_ref() {
            if barrier.entered() {
                barrier.busy.store(true, Ordering::Release);
            }
        }
    });
}
impl NativeUnassignedLearnerHost {
    pub(crate) fn test_owned_metadata_validation_barrier() -> Arc<OwnedMetadataValidationBarrier> {
        Arc::new(OwnedMetadataValidationBarrier::default())
    }
    pub(crate) fn test_validate_registered_metadata_catchup_after_factory(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        barrier: Arc<OwnedMetadataValidationBarrier>,
    ) -> Result<bool> {
        let previous = ACTIVE.with(|slot| slot.replace(Some(barrier)));
        let _restore = Restore(previous);
        self.validate_registered_metadata_catchup(observed, business, parent)
    }
    pub(crate) fn test_validate_owned_metadata_member_after_factory(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        barrier: Arc<OwnedMetadataValidationBarrier>,
    ) -> Result<()> {
        let previous = ACTIVE.with(|slot| slot.replace(Some(barrier)));
        let _restore = Restore(previous);
        self.validate_owned_metadata_member(observed, business, parent)
    }
}
