//! Test scheduling at a genuine failed acquisition of the original service.
//! The held guard and actual dispatcher remain the production objects.
use super::*;
use crate::distribution_native_read::NativeStrongEventDispatcher;
use std::cell::RefCell;
use std::sync::mpsc;

struct Cut {
    service: usize,
    action: Box<dyn FnOnce() -> Result<()>>,
}
thread_local! { static CUT: RefCell<Option<Cut>> = RefCell::new(None); }
pub(super) fn after_failed_service_slice(service: usize) -> Result<()> {
    let cut = CUT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|cut| cut.service == service) {
            slot.take()
        } else {
            None
        }
    });
    cut.map_or(Ok(()), |cut| (cut.action)())
}
struct Clear;
impl Drop for Clear {
    fn drop(&mut self) {
        CUT.with(|slot| slot.borrow_mut().take());
    }
}
struct Release(mpsc::Sender<()>);
impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
impl RangeWriteCoordinator<TcpClusterRelocationTransport> {
    pub(crate) fn test_actual_dispatch_publication_service_contention(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        dispatcher: &Arc<NativeStrongEventDispatcher>,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        if mode > 2 {
            return Err(range_write_error("unknown dispatcher contention control"));
        }
        dispatcher.require_runtime(runtime)?;
        self.validate_strong_event_dispatch_install_bounded(runtime, host, parent)?;
        let before = dispatcher.test_actual_binding_generation()?;
        if before.is_none() {
            return Err(range_write_error(
                "control requires an actual published dispatcher",
            ));
        }
        let epochs_before = self.recovered_epochs.lock().clone();
        let request = parent.child_with_deadline(
            Instant::now() + Duration::from_millis(if mode == 2 { 100 } else { 5000 }),
        );
        let original_deadline = request.deadline();
        std::thread::scope(|scope| -> Result<serde_json::Value> {
            let (release_send, release_recv) = mpsc::channel();
            let (held_send, held_recv) = mpsc::sync_channel(1);
            // Release on every early return before the scoped join.
            let release = Release(release_send.clone());
            let holder = scope.spawn(move || -> Result<()> {
                let actual = self.local_service.lock();
                actual.listener_open_guard().require_live()?;
                actual
                    .native_replica_listener_store_open_guard()
                    .require_live()?;
                held_send
                    .send(())
                    .map_err(|_| range_write_error("dispatcher holder observer ended"))?;
                release_recv
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| range_write_error("dispatcher holder watchdog expired"))?;
                drop(actual);
                Ok(())
            });
            held_recv
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| range_write_error("actual dispatcher service was not held"))?;
            let immediate = self.validate_strong_event_dispatch_install(runtime, host);
            let one_shot_refused = matches!(&immediate,
                Err(BicDbError::Cluster(body)) if body == "service busy publishing Strong dispatcher");
            let immediate_error = immediate.as_ref().err().map(ToString::to_string);
            let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = fired.clone();
            let selected = request.clone();
            let release_at_cut = release_send.clone();
            let cut = Cut {
                service: Arc::as_ptr(&self.local_service) as usize,
                action: Box::new(move || {
                    selected.check()?;
                    observed.store(true, std::sync::atomic::Ordering::Release);
                    match mode {
                        0 => release_at_cut.send(()).map_err(|_| {
                            range_write_error("actual dispatcher holder ended before release")
                        })?,
                        1 => selected.cancel(),
                        _ => {}
                    }
                    Ok(())
                }),
            };
            assert!(CUT.with(|slot| slot.borrow_mut().replace(cut).is_none()));
            let clear = Clear;
            let actual = dispatcher.publish_bounded_initial(self, host, &request);
            // Retain the original publication return before cleanup/results.
            let original_error = actual.as_ref().err().map(ToString::to_string);
            let original_class = actual.as_ref().err().map(|error| match error {
                BicDbError::QueryCanceled => "QueryCanceled",
                BicDbError::QueryTimedOut => "QueryTimedOut",
                _ => "Other",
            });
            let observed = fired.load(std::sync::atomic::Ordering::Acquire);
            drop(clear);
            drop(release);
            let cleanup = holder.join();
            let (holder_ok, cleanup_error) = match cleanup {
                Ok(Ok(())) => (true, None),
                Ok(Err(error)) => (false, Some(error.to_string())),
                Err(_) => (false, Some("actual dispatcher holder panicked".to_owned())),
            };
            let after = dispatcher.test_actual_binding_generation()?;
            let epochs_after = self.recovered_epochs.lock().clone();
            Ok(serde_json::json!({
                "mode":mode,"actual_original_service_held":true,
                "actual_failed_service_lock_slice":observed,
                "one_shot_still_refused":one_shot_refused,"one_shot_error":immediate_error,
                "original_returned_ok":actual.is_ok(),"original_generation":actual.as_ref().ok(),
                "original_error":original_error,"original_error_class":original_class,
                "binding_before":before,"binding_after":after,
                "recovered_epochs_before":epochs_before,"recovered_epochs_after":epochs_after,
                "original_deadline_preserved":request.deadline()==original_deadline,
                "original_parent_cancelled":request.is_cancel_requested(),
                "original_parent_expired":request.deadline().is_some_and(|d| Instant::now()>=d),
                "actual_holder_joined_ok":holder_ok,"cleanup_error":cleanup_error,
                "scope":"actual already-published INITIAL dispatcher component; not first publication, relocation or physical HA"
            }))
        })
    }
}
