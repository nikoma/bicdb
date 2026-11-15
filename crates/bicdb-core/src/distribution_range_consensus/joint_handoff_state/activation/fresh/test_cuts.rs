//! Thread-local, one-shot test scheduling only; no proof/state mutation API.
use super::*;
use std::cell::RefCell;
type Cut = Box<dyn FnOnce() -> Result<()>>;
thread_local! {
    static EXTERNAL: RefCell<Option<Cut>> = RefCell::new(None);
    static PERSISTED: RefCell<Option<Cut>> = RefCell::new(None);
    static ABSENT: RefCell<Option<Cut>> = RefCell::new(None);
}
pub(super) fn after_actual_absence() -> Result<()> {
    let cut = ABSENT.with(|slot| slot.borrow_mut().take());
    cut.map_or(Ok(()), |cut| cut())
}
pub(super) fn after_external() -> Result<()> {
    let cut = EXTERNAL.with(|slot| slot.borrow_mut().take());
    cut.map_or(Ok(()), |cut| cut())
}
pub(super) fn after_persist() -> Result<()> {
    let cut = PERSISTED.with(|slot| slot.borrow_mut().take());
    cut.map_or(Ok(()), |cut| cut())
}
struct Clear;
impl Drop for Clear {
    fn drop(&mut self) {
        let _ = EXTERNAL.with(|slot| slot.borrow_mut().take());
        let _ = PERSISTED.with(|slot| slot.borrow_mut().take());
        let _ = ABSENT.with(|slot| slot.borrow_mut().take());
    }
}
impl RangeWriteStore {
    /// Observe the real no-owner/no-activation branch. The callback may perform
    /// an actual cancellation but cannot supply a recovery/publication grant.
    pub(crate) fn test_joint_open_after_actual_absence<T>(
        cut: impl FnOnce() -> Result<()> + 'static,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        assert!(ABSENT.with(|slot| slot.borrow_mut().replace(Box::new(cut)).is_none()));
        let _clear = Clear;
        operation()
    }
    /// Schedule a genuine external action after the LAST foreign sweep, before
    /// the actual local journal. The callback cannot supply an admission.
    pub(crate) fn test_joint_open_after_external<T>(
        cut: impl FnOnce() -> Result<()> + 'static,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        assert!(EXTERNAL.with(|slot| slot.borrow_mut().replace(Box::new(cut)).is_none()));
        let _clear = Clear;
        operation()
    }
    /// The actual catalogue fsync has succeeded, uncertainty is cleared and
    /// Controlled dropped; the original production readback/liveness follows.
    pub(crate) fn test_joint_open_after_persist<T>(
        cut: impl FnOnce() -> Result<()> + 'static,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        assert!(PERSISTED.with(|slot| slot.borrow_mut().replace(Box::new(cut)).is_none()));
        let _clear = Clear;
        operation()
    }
}
