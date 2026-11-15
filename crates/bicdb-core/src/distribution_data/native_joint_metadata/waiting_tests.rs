//! Genuine held-service scheduling for the actual identity acquisition. No
//! service/OWN/point constructor or mutation permission is exposed to tests.
use super::*;
use std::cell::RefCell;
use std::sync::{mpsc, Weak};

struct ContentionCut {
    service: Weak<Mutex<ClusterDataNodeService>>,
    call: Box<dyn FnOnce() -> Result<()>>,
}
thread_local! { static CUT: RefCell<Option<ContentionCut>> = RefCell::new(None); }
pub(super) fn after_actual_contention(service: &Arc<Mutex<ClusterDataNodeService>>) -> Result<()> {
    let cut = CUT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|cut| Weak::ptr_eq(&cut.service, &Arc::downgrade(service)))
        {
            slot.take()
        } else {
            None
        }
    });
    cut.map_or(Ok(()), |cut| (cut.call)())
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
impl ClusterDataNodeService {
    /// Caller supplies the SAME genuine OWN just obtained by its real FIRST
    /// coordinator. mode controls scheduling only: release/cancel/short expiry.
    pub(crate) fn test_same_joint_metadata_identity_wait(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        mode: u8,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        own.require_live_request(business)?;
        if mode > 2 {
            return Err(data_error("unknown metadata identity scheduling control"));
        }
        let request = parent.child_with_deadline(
            std::time::Instant::now()
                + std::time::Duration::from_millis(if mode == 2 { 100 } else { 5000 }),
        );
        let cut_data = Arc::new(Mutex::new(None::<serde_json::Value>));
        let result = std::thread::scope(|scope| -> Result<serde_json::Value> {
            let (release_send, release_recv) = mpsc::channel();
            let (held_send, held_recv) = mpsc::sync_channel(1);
            // Declared inside scope so every early return releases BEFORE the
            // automatic join. Kernel/worker joins are not hard preemptible.
            let release = Release(release_send.clone());
            let worker = scope.spawn(move || -> Result<serde_json::Value> {
                let actual = service.lock();
                let database = actual.listener_open_guard();
                let slot = actual.native_replica_listener_store_open_guard();
                database.require_live()?;
                slot.require_live()?;
                let held = serde_json::json!({"actual_service_guard_held":true,
                    "actual_database_guard_live":true,"actual_store":slot.id()});
                held_send
                    .send(held.clone())
                    .map_err(|_| data_error("identity held-service witness receiver ended"))?;
                release_recv
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .map_err(|_| data_error("identity held-service watchdog expired"))?;
                drop(actual);
                Ok(held)
            });
            let held = held_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .map_err(|_| data_error("actual held-service acquisition not reached"))?;
            let one_shot =
                Self::capture_actual_joint_metadata_point(service, own, business, &request);
            let one_shot_busy = matches!(&one_shot, Err(BicDbError::Cluster(body))
                if body == "joint metadata service identity busy");
            let one_shot_error = one_shot.as_ref().err().map(ToString::to_string);
            let observed = cut_data.clone();
            let cancel = request.clone();
            let release_on_contention = release_send.clone();
            let cut = ContentionCut {
                service: Arc::downgrade(service),
                call: Box::new(move || {
                    cancel.check()?;
                    *observed.lock() = Some(
                        serde_json::json!({"actual_service_try_lock_slice_failed":true,
                    "actual_capture_parent_remaining_us":cancel.deadline().map(|d|d.saturating_duration_since(std::time::Instant::now()).as_micros()),
                    "actual_capture_parent_live_before_action":true}),
                    );
                    match mode {
                        0 => release_on_contention
                            .send(())
                            .map_err(|_| data_error("actual service release owner ended"))?,
                        1 => cancel.cancel(),
                        _ => {}
                    }
                    Ok(())
                }),
            };
            assert!(CUT.with(|slot| slot.borrow_mut().replace(cut).is_none()));
            let clear = Clear;
            let captured =
                Self::capture_actual_joint_metadata_point_waiting(service, own, business, &request);
            // Retain the exact original result before cleanup or later checks.
            let original_error = captured.as_ref().err().map(ToString::to_string);
            let original_class = captured.as_ref().err().map(|e| match e {
                BicDbError::QueryCanceled => "QueryCanceled",
                BicDbError::QueryTimedOut => "QueryTimedOut",
                _ => "Other",
            });
            let point_data = captured.as_ref().ok().and_then(|p| p.as_ref()).map(|p|
                serde_json::json!({"context":p.context,"term":p.term,"index":p.index,
                    "actual_store":p.slot.id(),"original_request_binding":p.require_request(own,business).is_ok()}));
            drop(clear);
            drop(release); // Always release actual guard before explicit join.
            let joined = worker.join();
            let (worker_ok, worker_error) = match joined {
                Ok(Ok(_)) => (true, None),
                Ok(Err(error)) => (false, Some(error.to_string())),
                Err(payload) => (
                    false,
                    Some(
                        payload
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                            .unwrap_or_else(|| "opaque actual service-holder panic".into()),
                    ),
                ),
            };
            let own_still_live = own.require_live_request(business).is_ok();
            Ok(
                serde_json::json!({"mode":mode,"held":held,"contention":cut_data.lock().clone(),
                "actual_capture_returned_ok":captured.is_ok(),"actual_point":point_data,
                "original_one_shot_exact_busy":one_shot_busy,"original_one_shot_error":one_shot_error,
                "original_capture_error":original_error,"original_capture_error_class":original_class,
                "original_OWN_context":own.context_id(),"original_OWN_term":own.term(),"original_OWN_index":own.read_index(),
                "original_OWN_still_live":own_still_live,
                "capture_parent_cancelled":request.is_cancel_requested(),
                "capture_parent_expired":request.deadline().is_some_and(|d|std::time::Instant::now()>=d),
                "actual_worker_joined_ok":worker_ok,"cleanup_error":worker_error,
                "fresh_OWN_acquired_during_wait":false,"native_mutation_or_Close_RPC_called":false,
                "scope":"same-process real original service/OWN, not HA qualification"}),
            )
        });
        result
    }
}
