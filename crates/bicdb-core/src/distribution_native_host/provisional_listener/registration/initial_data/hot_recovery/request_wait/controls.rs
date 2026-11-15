//! Scheduling a genuine retained State guard; no State/job constructor or
//! mutation selector escapes this test-only owner module.
use super::*;
use std::cell::RefCell;
use std::sync::mpsc;
struct Cut {
    owner: *const NativeHotRecoveryMaintenance,
    action: Box<dyn FnOnce(&CancellationToken) -> Result<()>>,
}
thread_local! { static CUT: RefCell<Option<Cut>> = RefCell::new(None); }
pub(super) fn after_actual_contention(
    owner: &NativeHotRecoveryMaintenance,
    parent: &CancellationToken,
) -> Result<()> {
    let cut = CUT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|c| std::ptr::eq(c.owner, owner)) {
            slot.take()
        } else {
            None
        }
    });
    cut.map_or(Ok(()), |cut| (cut.action)(parent))
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
impl NativeHotRecoveryMaintenance {
    pub(in crate::distribution_native_host::provisional_listener::registration::initial_data) fn test_actual_request_state_wait(
        self: &Arc<Self>,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.live(parent)?;
        if mode > 2 || parent.deadline().is_none() {
            return Err(invalid("invalid original maintenance State control"));
        }
        let facts = Arc::new(Mutex::new(None::<serde_json::Value>));
        std::thread::scope(|scope| {
            let (release_send, release_recv) = mpsc::channel();
            let (held_send, held_recv) = mpsc::sync_channel(1);
            let release = Release(release_send.clone());
            let owner = self.clone();
            let worker = scope.spawn(move || -> Result<serde_json::Value> {
                let state = loop {
                    owner.live(parent)?;
                    if let Some(state) = owner.state.try_lock_for(Duration::from_millis(5)) { break state; }
                };
                let held = serde_json::json!({"actual_State_address":format!("{:p}",&*state),
                    "actual_jobs":state.jobs.len(),"actual_replica_debts":state.replica_debts.len()});
                held_send.send(held.clone()).map_err(|_| invalid("State holder witness ended"))?;
                release_recv.recv_timeout(Duration::from_secs(30))
                    .map_err(|_| invalid("State holder cleanup watchdog"))?;
                let before_drop=serde_json::json!({"actual_State_address":format!("{:p}",&*state),
                    "actual_jobs":state.jobs.len(),"actual_replica_debts":state.replica_debts.len()});
                drop(state);
                Ok(before_drop)
            });
            let held = held_recv
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| invalid("actual State-holder prerequisite unavailable"))?;
            let old = self.tick(parent);
            let observed = facts.clone();
            let release_after_contention = release_send.clone();
            let cut = Cut {
                owner: self.as_ref(),
                action: Box::new(move |actual| {
                    *observed.lock() = Some(serde_json::json!({"actual_failed_State_slice":true,
                    "parent_live_before_action":actual.check().is_ok(),
                    "parent_deadline_identity":format!("{:?}",actual.deadline())}));
                    match mode {
                        0 => release_after_contention
                            .send(())
                            .map_err(|_| invalid("State holder ended"))?,
                        1 => actual.cancel(),
                        _ => {}
                    }
                    Ok(())
                }),
            };
            let armed = CUT.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_some() {
                    false
                } else {
                    *slot = Some(cut);
                    true
                }
            });
            if !armed {
                return Err(invalid("another State cut is still armed"));
            }
            let clear = Clear;
            let actual = self.tick_for_request(parent);
            let class = actual.as_ref().err().map(|error| match error {
                BicDbError::QueryCanceled => "QueryCanceled",
                BicDbError::QueryTimedOut => "QueryTimedOut",
                _ => "Other",
            });
            let original_error = actual.as_ref().err().map(ToString::to_string);
            let still_held = self.state.try_lock().is_none();
            drop(clear);
            drop(release);
            let (cleanup, holder_before_drop) = match worker.join() {
                Ok(Ok(value)) => (None, Some(value)),
                Ok(Err(e)) => (Some(e.to_string()), None),
                Err(_) => (Some("actual State-holder panicked".into()), None),
            };
            Ok(
                serde_json::json!({"mode":mode,"held":held,"cut":facts.lock().clone(),
                "original_instant_error":old.as_ref().err().map(ToString::to_string),
                "actual_returned_ok":actual.is_ok(),"original_error":original_error,
                "original_error_class":class,"actual_State_address":format!("{:p}",self.state.data_ptr()),
                "holder_still_held_at_original_return":still_held,"cleanup_error":cleanup,
                "same_holder_State_before_drop":holder_before_drop,
                "same_original_request_no_new_deadline":true,
                "no_State_replacement_or_job_reset":true}),
            )
        })
    }
}
