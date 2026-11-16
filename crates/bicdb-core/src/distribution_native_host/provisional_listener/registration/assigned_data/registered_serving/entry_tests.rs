//! Passive one-shot test cut after actual RAM-owner custody is released, before
//! routing/HostMQ, or after real HostMQ before the full factory check.
//! It exposes the actual constrained token, never authority.
use super::*;
use std::cell::RefCell;
type Cut = Box<dyn FnOnce(&CancellationToken)>;
thread_local! { static NEXT: RefCell<Option<(bool, Cut)>> = RefCell::new(None); }
pub(crate) struct EntryCut;
impl Drop for EntryCut {
    fn drop(&mut self) {
        NEXT.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
impl NativeMetadataControlHost {
    pub(crate) fn test_registered_policy_entry_cut(after_host_mq: bool, callback: Cut) -> EntryCut {
        NEXT.with(|slot| {
            let mut slot = slot.borrow_mut();
            assert!(slot.is_none(), "actual Registered entry cut already armed");
            *slot = Some((after_host_mq, callback));
        });
        EntryCut
    }
}
// Stage selector is test DATA only. A mismatching actual cut leaves the
// one-shot armed; no caller can receive authority or synthesize an event.
pub(super) fn at_actual_acquisition(after_host_mq: bool, actual: &CancellationToken) {
    let callback = NEXT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|(selected, _)| *selected == after_host_mq)
        {
            slot.take().map(|(_, callback)| callback)
        } else {
            None
        }
    });
    if let Some(callback) = callback {
        callback(actual);
    }
}
