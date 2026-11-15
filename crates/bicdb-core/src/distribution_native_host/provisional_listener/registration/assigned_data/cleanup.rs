//! Revocation/custody only. No MQ, new policy, recovery or serving authority.
use super::*;
use std::collections::VecDeque;
use std::sync::{mpsc, OnceLock};
static CLEANUP: OnceLock<AssignedCleanupSender> = OnceLock::new();
static INITIALIZING: Mutex<()> = Mutex::new(());
static QUARANTINED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[derive(Clone)]
pub(super) struct AssignedCleanupSender(mpsc::Sender<AssignedCleanup>);
pub(super) struct AssignedCleanup {
    parts: registered_replica::RegisteredAssignedParts,
    coordinator: Option<Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>>,
    attempt: Option<NativeAssignedWriterInstallAttempt>,
    listener: Option<ClusterDataServerHandle>,
    _image: Option<NativeJointServingLease>,
    _registered: Option<RegisteredServingPolicy>,
    writer_data_domains: super::super::writer_data_domain::NativeHostWriterDataDomains,
    dispatcher: Option<Arc<crate::distribution_native_read::NativeStrongEventDispatcher>>,
}
impl AssignedCleanup {
    pub(super) fn take(owner: &mut NativeAssignedDataHost) -> Self {
        Self {
            parts: owner.parts.clone(),
            coordinator: owner.coordinator.take(),
            attempt: owner.attempt.take(),
            listener: owner.listener.take(),
            _image: owner.image.take(),
            _registered: owner.registered.take(),
            writer_data_domains: std::mem::take(&mut owner.writer_data_domains),
            dispatcher: None,
        }
    }
    pub(super) fn from_registered_joined(
        transfer: super::NativeRegisteredJoinedDataTransfer,
        listener: Option<ClusterDataServerHandle>,
        dispatcher: Option<Arc<crate::distribution_native_read::NativeStrongEventDispatcher>>,
    ) -> Self {
        Self {
            parts: transfer.parts,
            coordinator: Some(transfer.coordinator),
            attempt: Some(transfer.attempt),
            listener,
            dispatcher,
            _image: None,
            _registered: Some(transfer.registered),
            writer_data_domains: transfer.writer_data_domains,
        }
    }
    pub(super) fn quarantine(self) {
        QUARANTINED.fetch_add(1, Ordering::Relaxed);
        let _ = Box::leak(Box::new(self));
    }
    fn try_finish(&mut self) -> bool {
        if self.dispatcher.as_ref().is_some_and(|dispatcher| {
            !dispatcher.try_clear_for_cleanup() || dispatcher.active_workers() != 0
        }) {
            return false;
        }
        if self
            .listener
            .as_mut()
            .is_some_and(|l| !l.poll_shutdown_drained_for_cleanup())
            || !matches!(self.parts.runtime.active_contexts(), Ok(0))
            || self.writer_data_domains.unresolved()
        {
            return false;
        }
        let Some(attempt) = &self.attempt else {
            return true;
        };
        // A possibly durable policy is not discarded merely because its
        // publication failed. This retained CLOSED owner needs explicit same-
        // operation reconciliation; cleanup never replays or refreshes it.
        if attempt.unresolved_native_publication() {
            return false;
        }
        let Some(guard) = attempt.installed_guard() else {
            return true;
        };
        if guard.registration_ended() {
            return true;
        }
        let Some(database) = self.parts.factory.database.try_read() else {
            return false;
        };
        let Some(coordinator) = &self.coordinator else {
            return false;
        };
        guard.retire_if_current(&database, coordinator).is_ok()
    }
}
impl AssignedCleanupSender {
    pub(super) fn ensure(parent: &CancellationToken) -> Result<Self> {
        parent.check()?;
        if let Some(sender) = CLEANUP.get() {
            return Ok(sender.clone());
        }
        let _initializing = INITIALIZING
            .try_lock()
            .ok_or_else(|| invalid("Assigned cleanup initialization busy"))?;
        if let Some(sender) = CLEANUP.get() {
            return Ok(sender.clone());
        }
        let (tx, rx) = mpsc::channel::<AssignedCleanup>();
        std::thread::Builder::new()
            .name("native-assigned-revoke".into())
            .spawn(move || {
                let mut pending = VecDeque::new();
                loop {
                    match rx.recv_timeout(Duration::from_millis(20)) {
                        Ok(owner) => pending.push_back(owner),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            for owner in pending {
                                owner.quarantine();
                            }
                            break;
                        }
                    }
                    for _ in 0..pending.len() {
                        let mut owner = pending.pop_front().unwrap();
                        let finished =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                owner.try_finish()
                            }))
                            .unwrap_or(false);
                        if !finished {
                            pending.push_back(owner);
                        }
                    }
                }
            })
            .map_err(|e| invalid(&format!("cannot create Assigned revocation worker: {e}")))?;
        let sender = Self(tx);
        let _ = CLEANUP.set(sender.clone());
        parent.check()?;
        Ok(sender)
    }
    pub(super) fn defer(&self, owner: AssignedCleanup) {
        if let Err(error) = self.0.send(owner) {
            error.0.quarantine();
        }
    }
}
