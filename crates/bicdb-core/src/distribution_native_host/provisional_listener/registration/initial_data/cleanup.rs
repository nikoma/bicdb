//! Revocation-only deferred cleanup. Started before any initial DATA hook,
//! never acquires MQ/recovery/native mutation authority or renews a request.
//! Original caller cancellation still governs DATA operations. This process
//! worker only retains stopping owners until their ACTUAL operations drain.
use super::*;
use std::collections::VecDeque;
use std::sync::{mpsc, OnceLock};

static CLEANUP: OnceLock<NativeInitialDataCleanupSender> = OnceLock::new();
static INITIALIZING: Mutex<()> = Mutex::new(());
static QUARANTINED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[derive(Clone)]
pub(super) struct NativeInitialDataCleanupSender(mpsc::Sender<NativeInitialDataCleanup>);
pub(super) struct NativeInitialDataCleanup {
    initial: Arc<NativeInitialConsensusLaunch>,
    enrollment: Option<NativeInitialWriterEnrollment>,
    runtime: Option<Arc<RangeRecoveryRuntime>>,
    hot_recovery: Option<Arc<NativeHotRecoveryMaintenance>>,
    dispatcher: Option<Arc<NativeStrongEventDispatcher>>,
    listeners: Vec<ClusterDataServerHandle>,
}
impl NativeInitialDataCleanup {
    pub(super) fn take_from(owner: &mut NativeInitialDataHost) -> Self {
        Self {
            initial: owner.initial.clone(),
            enrollment: owner.enrollment.take(),
            runtime: owner.runtime.take(),
            hot_recovery: owner.hot_recovery.take(),
            dispatcher: owner.dispatcher.take(),
            listeners: [owner.listener.take(), owner.old_listener.take()]
                .into_iter()
                .flatten()
                .collect(),
        }
    }
    pub(super) fn quarantine(self) {
        // Only unexpected worker/construction failure reaches this branch.
        // Keep exact owners closed rather than silently abandoning an active
        // hook or clearing a foreign installation. No authority is produced.
        // Pure diagnostic counter: no stdout/stderr lock or I/O from Drop.
        QUARANTINED.fetch_add(1, Ordering::Relaxed);
        let _ = Box::leak(Box::new(self));
    }
    fn try_finish(&mut self) -> bool {
        if let Some(hot) = &self.hot_recovery {
            hot.request_shutdown();
            if !hot.try_join_finished() {
                return false;
            }
        }
        let mut drained = true;
        for listener in &mut self.listeners {
            drained &= listener.poll_shutdown_drained_for_cleanup();
        }
        let binding_cleared = self
            .dispatcher
            .as_ref()
            .is_none_or(|dispatcher| dispatcher.try_clear_for_cleanup());
        if !drained
            || !binding_cleared
            || self
                .dispatcher
                .as_ref()
                .is_some_and(|d| d.active_workers() != 0)
            || self
                .runtime
                .as_ref()
                .is_some_and(|r| !matches!(r.active_contexts(), Ok(0)))
        {
            return false;
        }
        let Some(owner) = &self.enrollment else {
            return true;
        };
        if owner.guard.registration_ended() {
            return true;
        }
        let Some(database) = self.initial.seed.database.try_read() else {
            return false;
        };
        // This is pure exact-slot revocation under nonwaiting locks. Poisoned
        // native health does not prevent cleanup, and no currentness/DB-native
        // validation may accidentally reacquire a journal under the hook lock.
        owner
            .guard
            .retire_if_current(&database, &owner.coordinator)
            .is_ok()
    }
}
impl NativeInitialDataCleanupSender {
    pub(super) fn quarantined_owners() -> usize {
        QUARANTINED.load(Ordering::Relaxed)
    }
    pub(super) fn ensure(parent: &CancellationToken) -> Result<Self> {
        parent.check()?;
        if let Some(sender) = CLEANUP.get() {
            return Ok(sender.clone());
        }
        let _initializing = INITIALIZING
            .try_lock()
            .ok_or_else(|| invalid("initial DATA cleanup worker is being initialized; retry"))?;
        if let Some(sender) = CLEANUP.get() {
            return Ok(sender.clone());
        }
        let (tx, rx) = mpsc::channel::<NativeInitialDataCleanup>();
        std::thread::Builder::new()
            .name("native-data-revoke".into())
            .spawn(move || {
                let mut pending = VecDeque::new();
                loop {
                    match rx.recv_timeout(Duration::from_millis(20)) {
                        Ok(owner) => pending.push_back(owner),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) if pending.is_empty() => return,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            std::thread::sleep(Duration::from_millis(20))
                        }
                    }
                    // Continuous dropping cannot starve already pending owners.
                    // Work per iteration is bounded; retained owners rotate.
                    for _ in 0..16 {
                        match rx.try_recv() {
                            Ok(owner) => pending.push_back(owner),
                            Err(_) => break,
                        }
                    }
                    for _ in 0..pending.len().min(32) {
                        let Some(mut owner) = pending.pop_front() else {
                            break;
                        };
                        // Retain this owner if an unexpected engine panic occurs;
                        // all ingress was already closed. No panic drops the queue.
                        let done = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            owner.try_finish()
                        }))
                        .unwrap_or(false);
                        if !done {
                            pending.push_back(owner);
                        }
                    }
                }
            })
            .map_err(|error| {
                invalid(&format!(
                    "cannot start initial DATA cleanup worker: {error}"
                ))
            })?;
        let sender = Self(tx);
        let _ = CLEANUP.set(sender.clone());
        parent.check()?;
        Ok(sender)
    }
    pub(super) fn defer(&self, owner: NativeInitialDataCleanup) {
        // Drop performs no join/DB/native operation. Actual drain and exact
        // hook compare-retirement happen only on the retained cleanup worker.
        // Unbounded channel send has no capacity wait; only the stopping owner
        // moves here. The worker cannot publish/renew any serving authority.
        if let Err(error) = self.0.send(owner) {
            error.0.quarantine();
        }
    }
}
