//! Passive selected-thread barriers on genuine namespace/owner boundaries.
//! Only bounded DATA reaches a callback; no owner, File, guard or token escapes.
use super::*;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
struct Selected {
    root: PathBuf,
    phase: &'static str,
    callback: Box<dyn FnOnce(serde_json::Value)>,
}
thread_local! { static SELECTED: RefCell<Option<Selected>> = const { RefCell::new(None) }; }
struct Guard(PhantomData<Rc<()>>);
impl Drop for Guard {
    fn drop(&mut self) {
        SELECTED.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
fn key(root: &Path) -> PathBuf {
    root.canonicalize().unwrap_or_else(|_| root.to_path_buf())
}
pub(super) fn arm(
    root: &Path,
    phase: &'static str,
    callback: Box<dyn FnOnce(serde_json::Value)>,
) -> impl Drop {
    SELECTED.with(|slot| {
        assert!(
            slot.borrow().is_none(),
            "one actual bootstrap boundary per selected thread"
        );
        *slot.borrow_mut() = Some(Selected {
            root: key(root),
            phase,
            callback,
        });
    });
    Guard(PhantomData)
}
pub(super) fn observe(root: &Path, phase: &'static str, data: serde_json::Value) {
    let selected = SELECTED.with(|slot| {
        if slot
            .borrow()
            .as_ref()
            .is_some_and(|s| s.root == key(root) && s.phase == phase)
        {
            slot.borrow_mut().take()
        } else {
            None
        }
    });
    if let Some(selected) = selected {
        (selected.callback)(data);
    }
}
