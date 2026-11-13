//! Keyed fault only: cancel the ACTUAL publication request after real source
//! bindings/pin/reservation capture, before the wait loop. Never a grant.
use super::*;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};

struct Pending {
    database: Weak<RwLock<BicDb>>,
    range: RangeId,
    nonce: Uuid,
    original_request: CancellationToken,
    fired: Arc<AtomicBool>,
}
thread_local! { static PENDING: RefCell<Option<Pending>> = const { RefCell::new(None) }; }
pub(crate) struct CancelOriginalAtActualWait {
    fired: Arc<AtomicBool>,
    _thread: PhantomData<Rc<()>>,
}
impl CancelOriginalAtActualWait {
    pub(crate) fn install(
        database: &Arc<RwLock<BicDb>>,
        range: RangeId,
        nonce: Uuid,
        original_request: &CancellationToken,
    ) -> Self {
        let fired = Arc::new(AtomicBool::new(false));
        PENDING.with(|pending| {
            assert!(
                pending.borrow().is_none(),
                "nested actual capture cancellation hook"
            );
            *pending.borrow_mut() = Some(Pending {
                database: Arc::downgrade(database),
                range,
                nonce,
                original_request: original_request.clone(),
                fired: fired.clone(),
            });
        });
        Self {
            fired,
            _thread: PhantomData,
        }
    }
    pub(crate) fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}
impl Drop for CancelOriginalAtActualWait {
    fn drop(&mut self) {
        PENDING.with(|pending| {
            if pending
                .borrow()
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(&p.fired, &self.fired))
            {
                pending.borrow_mut().take();
            }
        });
    }
}
pub(super) fn cancel_at_actual_wait(database: &Arc<RwLock<BicDb>>, range: RangeId, nonce: Uuid) {
    let action = PENDING.with(|pending| {
        let matches = pending.borrow().as_ref().is_some_and(|p| {
            p.range == range
                && p.nonce == nonce
                && Weak::ptr_eq(&p.database, &Arc::downgrade(database))
        });
        if matches {
            pending.borrow_mut().take()
        } else {
            None
        }
    });
    if let Some(actual) = action {
        actual.original_request.cancel();
        actual.fired.store(true, Ordering::Release);
    }
}
