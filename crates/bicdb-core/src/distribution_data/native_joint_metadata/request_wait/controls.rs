//! Selected-thread scheduling of a REAL original service guard. No owner,
//! receipt, Role, OWN or native outcome is manufactured by this harness.
use super::*;
use std::cell::RefCell;
use std::sync::{mpsc, Weak};
use std::time::{Duration, Instant};

struct Cut {
    service: Weak<Mutex<ClusterDataNodeService>>,
    action: Box<dyn FnOnce(&CancellationToken) -> Result<()>>,
}
thread_local! { static CUT: RefCell<Option<Cut>> = RefCell::new(None); }
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
impl ClusterDataNodeService {
    pub(crate) fn test_after_actual_request_service_contention(
        service: &Arc<Mutex<Self>>,
        parent: &CancellationToken,
    ) -> Result<()> {
        let cut = CUT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|c| Weak::ptr_eq(&c.service, &Arc::downgrade(service)))
            {
                slot.take()
            } else {
                None
            }
        });
        cut.map_or(Ok(()), |cut| (cut.action)(parent))
    }

    pub(crate) fn test_actual_request_service_wait<T: serde::Serialize>(
        service: &Arc<Mutex<Self>>,
        parent: &CancellationToken,
        mode: u8,
        call: impl FnOnce(&CancellationToken) -> Result<T>,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        if mode > 2 || parent.deadline().is_none() {
            return Err(data_error("invalid actual finite service-wait control"));
        }
        let facts = Arc::new(Mutex::new(None::<serde_json::Value>));
        std::thread::scope(|scope| {
            let (release_send, release_recv) = mpsc::channel();
            let (held_send, held_recv) = mpsc::sync_channel(1);
            // This guard is inside scope: every unwind releases before joins.
            let release = Release(release_send.clone());
            let worker = scope.spawn(move || -> Result<()> {
                let actual = loop {
                    parent.check()?;
                    if let Some(actual) = service.try_lock_for(Duration::from_millis(5)) {
                        break actual;
                    }
                };
                let open = actual.listener_open_guard();
                open.require_live()?;
                held_send
                    .send(actual.native_replica_listener_store_open_guard().id())
                    .map_err(|_| data_error("actual service-holder observer ended"))?;
                release_recv
                    .recv_timeout(Duration::from_secs(30))
                    .map_err(|_| data_error("actual service-holder cleanup watchdog"))?;
                drop(actual);
                Ok(())
            });
            let held = held_recv
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| data_error("actual service-holder prerequisite unavailable"))?;
            let original_instant_busy = service.try_lock().is_none();
            let observed = facts.clone();
            let release_after_contention = release_send.clone();
            let cut = Cut {
                service: Arc::downgrade(service),
                action: Box::new(move |actual| {
                    let now = Instant::now();
                    *observed.lock() = Some(serde_json::json!({
                        "actual_failed_service_slice":true,
                        "parent_live_before_action":actual.check().is_ok(),
                        "parent_cancelled_before_action":actual.is_cancel_requested(),
                        "parent_deadline_identity":format!("{:?}",actual.deadline()),
                        "parent_remaining_us":actual.deadline().map(|d|d.saturating_duration_since(now).as_micros())
                    }));
                    match mode {
                        0 => release_after_contention.send(()).map_err(|_| {
                            data_error("actual service-holder ended before release")
                        })?,
                        1 => actual.cancel(),
                        _ => {}
                    }
                    Ok(())
                }),
            };
            let admitted = CUT.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_some() {
                    false
                } else {
                    *slot = Some(cut);
                    true
                }
            });
            if !admitted {
                return Err(data_error("another original service cut remains armed"));
            }
            let clear = Clear;
            let actual = call(parent);
            // Actual result is captured BEFORE any fallible serialization or
            // holder cleanup, with diagnostic errors retained separately.
            let error = actual.as_ref().err().map(ToString::to_string);
            let class = actual.as_ref().err().map(|e| match e {
                BicDbError::QueryCanceled => "QueryCanceled",
                BicDbError::QueryTimedOut => "QueryTimedOut",
                _ => "Other",
            });
            let returned_data = actual.as_ref().ok().map(serde_json::to_value).transpose();
            let holder_still_held_at_return = service.try_lock().is_none();
            drop(clear);
            drop(release);
            let joined = worker.join();
            let cleanup = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("actual service-holder panicked".into()),
            };
            Ok(serde_json::json!({"mode":mode,"actual_held_store":held,
                "actual_original_instant_busy":original_instant_busy,
                "cut":facts.lock().clone(),"actual_returned_ok":actual.is_ok(),
                "original_error":error,"original_error_class":class,
                "returned_data":returned_data.as_ref().ok(),
                "serialization_error":returned_data.as_ref().err().map(ToString::to_string),
                "holder_still_held_at_original_return":holder_still_held_at_return,
                "cleanup_error":cleanup,"same_original_request_no_new_deadline":true,
                "parent_cancelled":parent.is_cancel_requested(),
                "parent_expired":parent.deadline().is_some_and(|d|Instant::now()>=d),
                "cleanup_watchdog_not_acceptance_budget_seconds":30}))
        })
    }
}
