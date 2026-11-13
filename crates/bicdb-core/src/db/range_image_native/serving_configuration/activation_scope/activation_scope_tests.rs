//! Test-only scoped cuts. No hook or scope crosses a worker/serde boundary.
use super::*;
use std::cell::RefCell;
use std::rc::Rc;
type Cut = Rc<dyn for<'scope> Fn(&NativeActivationReadScope<'scope>) -> Result<()>>;
thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
struct Restore(Option<Cut>);
impl Drop for Restore {
    fn drop(&mut self) {
        CUT.with(|cut| {
            cut.replace(self.0.take());
        });
    }
}
pub(super) fn after_entry(scope: &NativeActivationReadScope<'_>) -> Result<()> {
    let cut = CUT.with(|cut| cut.borrow().clone());
    if let Some(cut) = cut {
        cut(scope)?;
    }
    Ok(())
}
impl BicDb {
    pub(crate) fn test_with_native_activation_post_catalogue_cut<T>(
        cut: impl for<'scope> Fn(&NativeActivationReadScope<'scope>) -> Result<()> + 'static,
        action: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let old = CUT.with(|slot| slot.replace(Some(Rc::new(cut))));
        let _restore = Restore(old);
        action()
    }
}
