//! Passive test-only DATA from actual physical gate syscalls/descriptors.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
type Result<T> = std::io::Result<T>;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};

#[derive(Clone, Debug)]
pub struct GuardSnapshot {
    pub guard_id: u64,
    pub gate: PathBuf,
    pub exclusive: bool,
    pub file_identity: Option<(u64, u64)>,
    pub descriptor: Option<i64>,
    pub acquiring_thread: String,
    // Private observation state: consumers can read, never mark a guard live.
    live: Arc<AtomicBool>,
}
impl GuardSnapshot {
    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }
}
#[derive(Clone, Debug)]
pub struct Observation {
    pub gate: PathBuf,
    pub exclusive: bool,
    pub acquired: bool,
    pub released: bool,
    pub file_identity: Option<(u64, u64)>,
    pub guard_id: Option<u64>,
    pub descriptor: Option<i64>,
    pub actual_thread: String,
}
type Callback = dyn Fn(Observation) + Send + Sync;
struct GuardState {
    snapshot: GuardSnapshot,
    callback: Arc<Callback>,
}
pub(super) struct GuardLifetime(Arc<GuardState>);
impl std::fmt::Debug for GuardLifetime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ActualBackupGateLifetime")
            .field(&self.0.snapshot)
            .finish()
    }
}
impl GuardLifetime {
    pub(super) fn observe_acquired(&self) {
        (self.0.callback)(from_snapshot(&self.0.snapshot, true, false));
    }
}
impl Drop for GuardLifetime {
    fn drop(&mut self) {
        // BackupGateGuard._file precedes this field and has already closed.
        // This also works if the actual guard moved to another thread.
        self.0.snapshot.live.store(false, Ordering::SeqCst);
        (self.0.callback)(from_snapshot(&self.0.snapshot, false, true));
    }
}
fn from_snapshot(snapshot: &GuardSnapshot, acquired: bool, released: bool) -> Observation {
    Observation {
        gate: snapshot.gate.clone(),
        exclusive: snapshot.exclusive,
        acquired,
        released,
        file_identity: snapshot.file_identity,
        guard_id: Some(snapshot.guard_id),
        descriptor: snapshot.descriptor,
        actual_thread: format!("{:?}", std::thread::current().id()),
    }
}
struct Selected {
    root: PathBuf,
    exclusive: bool,
    contended: Cell<bool>,
    next_guard: Cell<u64>,
    guards: RefCell<Vec<Weak<GuardState>>>,
    callback: Arc<Callback>,
}
thread_local! { static SELECTED: RefCell<Option<Rc<Selected>>> = const { RefCell::new(None) }; }
struct SelectedGuard(Rc<Selected>);
impl Drop for SelectedGuard {
    fn drop(&mut self) {
        SELECTED.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.as_ref().is_some_and(|s| Rc::ptr_eq(s, &self.0)) {
                slot.take();
            }
        });
    }
}
pub fn arm(root: &Path, exclusive: bool, callback: Box<Callback>) -> Result<impl Drop> {
    let selected = Rc::new(Selected {
        root: root.canonicalize()?,
        exclusive,
        contended: Cell::new(false),
        next_guard: Cell::new(0),
        guards: RefCell::new(Vec::new()),
        callback: Arc::from(callback),
    });
    SELECTED.with(|slot| {
        assert!(
            slot.borrow().is_none(),
            "actual backup gate observer already armed"
        );
        *slot.borrow_mut() = Some(selected.clone());
    });
    Ok(SelectedGuard(selected))
}
fn selected(path: &Path, exclusive: bool) -> Option<Rc<Selected>> {
    let selected = SELECTED.with(|slot| slot.borrow().as_ref().cloned())?;
    (exclusive == selected.exclusive && path.parent() == Some(selected.root.as_path()))
        .then_some(selected)
}
fn file_identity(file: &File) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        file.metadata().ok().map(|m| (m.dev(), m.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        None
    }
}
pub(super) fn actual_guard(path: &Path, exclusive: bool, file: &File) -> Option<GuardLifetime> {
    let selected = selected(path, exclusive)?;
    let guard_id = selected
        .next_guard
        .get()
        .checked_add(1)
        .expect("test observer registration exhausted");
    selected.next_guard.set(guard_id);
    #[cfg(unix)]
    let descriptor = {
        use std::os::fd::AsRawFd;
        Some(i64::from(file.as_raw_fd()))
    };
    #[cfg(not(unix))]
    let descriptor = None;
    let state = Arc::new(GuardState {
        snapshot: GuardSnapshot {
            guard_id,
            gate: path.to_path_buf(),
            exclusive,
            file_identity: file_identity(file),
            descriptor,
            acquiring_thread: format!("{:?}", std::thread::current().id()),
            live: Arc::new(AtomicBool::new(true)),
        },
        callback: selected.callback.clone(),
    });
    {
        let mut guards = selected.guards.borrow_mut();
        guards.retain(|old| old.strong_count() != 0);
        guards.push(Arc::downgrade(&state));
    }
    Some(GuardLifetime(state))
}
pub(super) fn observe_refused(path: &Path, exclusive: bool) {
    let Some(selected) = selected(path, exclusive) else {
        return;
    };
    if selected.contended.replace(true) {
        return;
    }
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
    };
    #[cfg(not(unix))]
    let identity = None;
    (selected.callback)(Observation {
        gate: path.to_path_buf(),
        exclusive,
        acquired: false,
        released: false,
        file_identity: identity,
        guard_id: None,
        descriptor: None,
        actual_thread: format!("{:?}", std::thread::current().id()),
    });
}
pub fn live_shared_guards(root: &Path) -> Vec<GuardSnapshot> {
    live_guards(root, false)
}
pub fn live_exclusive_guards(root: &Path) -> Vec<GuardSnapshot> {
    live_guards(root, true)
}
fn live_guards(root: &Path, exclusive: bool) -> Vec<GuardSnapshot> {
    let Some(selected) = SELECTED.with(|slot| slot.borrow().as_ref().cloned()) else {
        return Vec::new();
    };
    if selected.exclusive != exclusive || selected.root != root {
        return Vec::new();
    }
    let actual_thread = format!("{:?}", std::thread::current().id());
    let snapshots = selected
        .guards
        .borrow()
        .iter()
        .filter_map(|guard| guard.upgrade())
        .filter(|guard| {
            guard.snapshot.exclusive == exclusive
                && guard.snapshot.is_live()
                && guard.snapshot.acquiring_thread == actual_thread
        })
        .map(|guard| guard.snapshot.clone())
        .collect();
    snapshots
}
