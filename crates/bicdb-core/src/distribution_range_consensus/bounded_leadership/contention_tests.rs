//! Scheduling at a genuine failed acquisition of the original service mutex.
//! No history, epoch, service or admission constructors are exposed.
use super::*;
use std::cell::RefCell;
use std::sync::mpsc;

struct Cut {
    lock: usize,
    action: Box<dyn FnOnce() -> Result<()>>,
}
thread_local! { static CUT: RefCell<Option<Cut>> = RefCell::new(None); }
pub(super) fn after_failed_lock_slice(lock: usize) -> Result<()> {
    let cut = CUT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|cut| cut.lock == lock) {
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
        let _ = CUT.with(|slot| slot.borrow_mut().take());
    }
}
struct Release(mpsc::Sender<()>);
impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
impl<Transport: RangeWriteTransport> RangeWriteCoordinator<Transport> {
    pub(crate) fn test_bounded_leadership_actual_service_contention(
        &self,
        selected: &RangeDescriptor,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        if mode > 2 {
            return Err(range_write_error(
                "unknown actual service scheduling control",
            ));
        }
        let request = parent.child_with_deadline(
            std::time::Instant::now() + Duration::from_millis(if mode == 2 { 100 } else { 5000 }),
        );
        let before = self.recovered_epochs.lock().clone();
        std::thread::scope(|scope| -> Result<serde_json::Value> {
            let (release_send, release_recv) = mpsc::channel();
            let (held_send, held_recv) = mpsc::sync_channel(1);
            // Every early return releases the holder before scope joins it.
            let release = Release(release_send.clone());
            let worker = scope.spawn(move || -> Result<()> {
                let actual = self.local_service.lock();
                actual.listener_open_guard().require_live()?;
                actual
                    .native_replica_listener_store_open_guard()
                    .require_live()?;
                held_send
                    .send(())
                    .map_err(|_| range_write_error("actual service witness receiver ended"))?;
                release_recv
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| range_write_error("actual service holder watchdog expired"))?;
                drop(actual);
                Ok(())
            });
            held_recv
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| range_write_error("actual service guard was not acquired"))?;
            let cancel = request.clone();
            let release_at_cut = release_send.clone();
            // This closure lives only in this thread until the actual call
            // returns; it receives no service/guard or mutation permission.
            let observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let at_cut = observed.clone();
            let cut = Cut {
                lock: std::sync::Arc::as_ptr(&self.local_service) as usize,
                action: Box::new(move || {
                    cancel.check()?;
                    at_cut.store(true, std::sync::atomic::Ordering::Release);
                    match mode {
                        0 => release_at_cut.send(()).map_err(|_| {
                            range_write_error("actual service holder ended before release")
                        })?,
                        1 => cancel.cancel(),
                        _ => {}
                    }
                    Ok(())
                }),
            };
            assert!(CUT.with(|slot| slot.borrow_mut().replace(cut).is_none()));
            let clear = Clear;
            let returned = self.recover_native_range_leadership_bounded(selected, &request);
            // Preserve the real result before cleanup can fail.
            let original_error = returned.as_ref().err().map(ToString::to_string);
            let original_class = returned.as_ref().err().map(|error| match error {
                BicDbError::QueryCanceled => "QueryCanceled",
                BicDbError::QueryTimedOut => "QueryTimedOut",
                _ => "Other",
            });
            let fired = observed.load(std::sync::atomic::Ordering::Acquire);
            drop(clear);
            drop(release);
            let worker_result = worker.join();
            let (worker_ok, cleanup_error) = match worker_result {
                Ok(Ok(())) => (true, None),
                Ok(Err(error)) => (false, Some(error.to_string())),
                Err(payload) => (
                    false,
                    Some(
                        payload
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| {
                                payload
                                    .downcast_ref::<&str>()
                                    .map(|value| (*value).to_owned())
                            })
                            .unwrap_or_else(|| "opaque service-holder panic".into()),
                    ),
                ),
            };
            let after = self.recovered_epochs.lock().clone();
            Ok(serde_json::json!({
                "mode":mode,"original_service_guard_held":true,
                "actual_failed_service_lock_slice":fired,
                "original_returned_ok":returned.is_ok(),"original_marker_changed":returned.as_ref().ok(),
                "original_error":original_error,"original_error_class":original_class,
                "actual_holder_joined_ok":worker_ok,"cleanup_error":cleanup_error,
                "recovered_epochs_before":before,"recovered_epochs_after":after,
                "original_request_cancelled":request.is_cancel_requested(),
                "original_request_expired":request.deadline().is_some_and(|deadline|std::time::Instant::now()>=deadline),
                "scope":"actual retained enrolled service; no new-epoch or physical qualification"
            }))
        })
    }
}
