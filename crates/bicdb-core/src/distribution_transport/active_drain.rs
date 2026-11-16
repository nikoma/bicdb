//! Selected-thread observation/cancellation of the SAME actual listener in
//! its real active-worker drain loop, after shutdown and actual accept join.
//! No active counter, owner, admission or shutdown state is manufactured.
use super::*;
use std::cell::RefCell;
use std::rc::Rc;

struct Selected {
    active: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    service: usize,
    store: Uuid,
    address: SocketAddr,
    actual_cut: Option<serde_json::Value>,
}
thread_local! {
    static SELECTED: RefCell<Option<Rc<RefCell<Selected>>>> = const { RefCell::new(None) };
}
pub(crate) struct ActualActiveDrainCancel(Rc<RefCell<Selected>>);
impl ActualActiveDrainCancel {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        self.0
            .borrow()
            .actual_cut
            .clone()
            .unwrap_or(serde_json::Value::Null)
    }
}
impl Drop for ActualActiveDrainCancel {
    fn drop(&mut self) {
        SELECTED.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
impl ClusterDataServerHandle {
    pub(crate) fn test_cancel_same_parent_at_actual_active_drain(
        &self,
    ) -> Result<ActualActiveDrainCancel> {
        let selected = Rc::new(RefCell::new(Selected {
            active: self.active.clone(),
            shutdown: self.shutdown.clone(),
            service: self.service_identity,
            store: self.service_store_open_id,
            address: self.local_addr,
            actual_cut: None,
        }));
        SELECTED.with(|slot| {
            if slot.borrow().is_some() {
                return Err(transport_error(
                    "actual active-drain control already selected",
                ));
            }
            *slot.borrow_mut() = Some(selected.clone());
            Ok(ActualActiveDrainCancel(selected))
        })
    }
}
pub(super) fn after_actual_shutdown_and_accept_join(
    listener: &ClusterDataServerHandle,
    parent: &CancellationToken,
) -> Result<()> {
    let Some(selected) = SELECTED.with(|slot| slot.borrow().clone()) else {
        return Ok(());
    };
    let mut state = selected.borrow_mut();
    if !Arc::ptr_eq(&listener.active, &state.active)
        || !Arc::ptr_eq(&listener.shutdown, &state.shutdown)
        || listener.service_identity != state.service
        || listener.service_store_open_id != state.store
        || listener.local_addr != state.address
    {
        return Err(transport_error(
            "active-drain control reached a different actual listener",
        ));
    }
    if state.actual_cut.is_some() {
        return Ok(());
    }
    parent.check()?;
    let active = listener.active.load(Ordering::SeqCst);
    let shutdown = listener.shutdown.load(Ordering::SeqCst);
    if !shutdown || listener.thread.is_some() || active == 0 {
        return Err(transport_error(
            "actual shutdown/join/active-handler drain cut not reached",
        ));
    }
    let observation = listener
        .initial_transition
        .control_snapshot(&listener.active);
    state.actual_cut = Some(serde_json::json!({
        "same_original_listener":true,"actual_shutdown_requested":shutdown,
        "actual_accept_handle_joined":listener.thread.is_none(),"actual_active_handlers":active,
        "actual_listener_address":listener.local_addr,
        "original_parent_live_before_cancel":true,
        "original_parent_remaining_us_before_cancel":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
        "actual_authenticated_handler":observation,
    }));
    // This is the SAME finite caller supplied to the original real drain.
    // The unchanged parent.check immediately after this hook returns the error.
    parent.cancel();
    Ok(())
}
