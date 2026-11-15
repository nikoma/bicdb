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
    writer_data_domains: super::super::writer_data_domain::NativeHostWriterDataDomains,
}
impl NativeInitialDataCleanup {
    pub(super) fn take_from(owner: &mut NativeInitialDataHost) -> Self {
        Self {
            initial: owner.initial.clone(),
            enrollment: owner.enrollment.take(),
            runtime: owner.runtime.take(),
            hot_recovery: owner.hot_recovery.take(),
            dispatcher: owner.dispatcher.take(),
            writer_data_domains: std::mem::take(&mut owner.writer_data_domains),
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
            || self.writer_data_domains.unresolved()
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
        Self::ensure_in(&CLEANUP, &INITIALIZING, parent)
    }
    fn ensure_in(
        cleanup: &OnceLock<Self>,
        initializing: &Mutex<()>,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        if let Some(sender) = cleanup.get() {
            return Ok(sender.clone());
        }
        // Concurrent normal DATA opens share this revocation-only worker.
        // Before any hook/service/DB guard exists, await initialization only
        // inside the original finite opening request. No fresh retry action.
        let _initializing = loop {
            parent.check()?;
            let acquired = if parent.deadline().is_some() {
                initializing.try_lock_for(Duration::from_millis(5))
            } else {
                initializing.try_lock()
            };
            if let Some(guard) = acquired {
                parent.check()?;
                break guard;
            }
            if parent.deadline().is_none() {
                return Err(invalid(
                    "initial DATA cleanup worker is being initialized; retry",
                ));
            }
            #[cfg(test)]
            tests::after_actual_contention()?;
        };
        if let Some(sender) = cleanup.get() {
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
        let _ = cleanup.set(sender.clone());
        #[cfg(test)]
        tests::after_actual_startup();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    type AfterContention = Box<dyn FnOnce() -> Result<()>>;
    thread_local! {
        static CUT: RefCell<Option<AfterContention>> = RefCell::new(None);
        static AFTER_STARTUP: RefCell<Option<CancellationToken>> = const { RefCell::new(None) };
    }
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            CUT.with(|slot| slot.borrow_mut().take());
            AFTER_STARTUP.with(|slot| slot.borrow_mut().take());
        }
    }
    struct Release(mpsc::Sender<()>);
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    pub(super) fn after_actual_contention() -> Result<()> {
        let cut = CUT.with(|slot| slot.borrow_mut().take());
        cut.map_or(Ok(()), |cut| cut())
    }
    pub(super) fn after_actual_startup() {
        if let Some(original) = AFTER_STARTUP.with(|slot| slot.borrow_mut().take()) {
            original.cancel();
        }
    }
    #[test]
    fn cleanup_original_cancel_after_real_start_retains_published_worker() {
        let cleanup = OnceLock::new();
        let initializing = Mutex::new(());
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        AFTER_STARTUP.with(|slot| *slot.borrow_mut() = Some(parent.clone()));
        let _clear = Clear;
        let actual = NativeInitialDataCleanupSender::ensure_in(&cleanup, &initializing, &parent);
        assert!(matches!(actual, Err(crate::BicDbError::QueryCanceled)));
        assert!(
            cleanup.get().is_some(),
            "actual started worker stays retained"
        );
        let fresh = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        assert!(NativeInitialDataCleanupSender::ensure_in(&cleanup, &initializing, &fresh).is_ok());
    }
    #[test]
    fn cleanup_initialization_waits_then_starts_one_actual_worker() {
        run(0);
    }
    #[test]
    fn cleanup_initialization_preserves_original_cancel() {
        run(1);
    }
    #[test]
    fn cleanup_initialization_preserves_original_deadline() {
        run(2);
    }
    #[test]
    fn cleanup_initialization_unbounded_caller_refuses_immediately() {
        run(3);
    }
    fn run(mode: u8) {
        let cleanup = OnceLock::new();
        let initializing = Mutex::new(());
        let parent = if mode == 3 {
            CancellationToken::uncancelable()
        } else {
            CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_secs(5))
        };
        std::thread::scope(|scope| {
            let (release_send, release_recv) = mpsc::channel();
            let (held_send, held_recv) = mpsc::sync_channel(1);
            let held_initializing = &initializing;
            let holder = scope.spawn(move || {
                let held = held_initializing.lock();
                held_send.send(()).unwrap();
                release_recv.recv_timeout(Duration::from_secs(30)).unwrap();
                drop(held);
            });
            // Declared before any assertion: unwind releases before scope join.
            let release = Release(release_send.clone());
            held_recv.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(initializing.try_lock().is_none());
            let observed = std::rc::Rc::new(std::cell::Cell::new(false));
            let seen = observed.clone();
            let original = parent.clone();
            CUT.with(|slot| {
                assert!(slot.borrow().is_none());
                *slot.borrow_mut() = Some(Box::new(move || {
                    seen.set(true);
                    original.check()?;
                    match mode {
                        0 => release_send
                            .send(())
                            .map_err(|_| invalid("initializer holder ended"))?,
                        1 => original.cancel(),
                        _ => {}
                    }
                    Ok(())
                }));
            });
            let clear = Clear;
            let actual =
                NativeInitialDataCleanupSender::ensure_in(&cleanup, &initializing, &parent);
            let held_at_return = initializing.try_lock().is_none();
            // Capture original return and custody before cleanup releases it.
            drop(clear);
            drop(release);
            holder.join().unwrap();
            assert_eq!(observed.get(), mode != 3);
            if mode == 0 {
                let sender = actual.unwrap();
                assert!(cleanup.get().is_some());
                let again =
                    NativeInitialDataCleanupSender::ensure_in(&cleanup, &initializing, &parent)
                        .unwrap();
                // Both actual senders remain connected to the real worker.
                drop(again);
                drop(sender);
            } else {
                assert!(held_at_return);
                assert!(
                    cleanup.get().is_none(),
                    "failed wait cannot publish a worker"
                );
                match actual {
                    Err(crate::BicDbError::QueryCanceled) if mode == 1 => {}
                    Err(crate::BicDbError::QueryTimedOut) if mode == 2 => {}
                    Err(error) if mode == 3 => assert!(error
                        .to_string()
                        .contains("cleanup worker is being initialized")),
                    _ => panic!("original cleanup initialization error changed"),
                }
            }
        });
    }
}
