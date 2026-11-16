//! Same actual Installed target actor plus genuine authenticated peer. These
//! test-only handles coordinate existing sockets; they contain no authority.
use super::*;
use crate::distribution_transport::{ActualActiveDrainCancel, InitialTransitionControl};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;

struct Selected {
    peer: ClusterNodeId,
    business: Uuid,
    ready: mpsc::SyncSender<Arc<InitialTransitionControl>>,
    listener: Option<Arc<InitialTransitionControl>>,
    drain: Option<ActualActiveDrainCancel>,
    before_drain: Option<serde_json::Value>,
    actual_return: Option<serde_json::Value>,
}
thread_local! {
    static SELECTED: RefCell<Option<Rc<RefCell<Selected>>>> = const { RefCell::new(None) };
}
struct Cleanup;
impl Drop for Cleanup {
    fn drop(&mut self) {
        // Last Arc drop releases the held peer on any failure; the harness's
        // explicit owner also releases before its scoped peer join.
        SELECTED.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
pub(super) fn before_actual_listener_drain(
    listener: &ClusterDataServerHandle,
    parent: &CancellationToken,
) -> Result<()> {
    let Some(selected) = SELECTED.with(|slot| slot.borrow().clone()) else {
        return Ok(());
    };
    parent.check()?;
    let control = Arc::new(listener.test_initial_transition_control());
    let peer = selected.borrow().peer.clone();
    control.pause_authenticated_host_read_request(peer, selected.borrow().business);
    let actual = listener.test_cancel_same_parent_at_actual_active_drain()?;
    {
        let mut state = selected.borrow_mut();
        state.listener = Some(control.clone());
        state.drain = Some(actual);
        state
            .ready
            .try_send(control.clone())
            .map_err(|_| invalid("actual partial-drain handshake receiver unavailable"))?;
    }
    // Do not enter shutdown until a real authenticated member request is
    // executing on this SAME original Assigned listener's counted worker.
    control.wait("auth", parent)?;
    selected.borrow_mut().before_drain = Some(control.snapshot());
    parent.check()
}
pub(super) fn note_actual_listener_drain_return(actual: &Result<DrainedClusterDataListener>) {
    if let Some(selected) = SELECTED.with(|slot| slot.borrow().clone()) {
        selected.borrow_mut().actual_return = Some(serde_json::json!({
            "actual_returned_ok":actual.is_ok(),
            "actual_typed_QueryCanceled":matches!(actual,Err(crate::BicDbError::QueryCanceled)),
            "original_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        }));
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_actual_registered_partial_drain_cancel(
        &mut self,
        range: crate::RangeId,
        phase: Uuid,
        peer: ClusterNodeId,
        business: Uuid,
        ready: mpsc::SyncSender<Arc<InitialTransitionControl>>,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        let state = Rc::new(RefCell::new(Selected {
            peer,
            business,
            ready,
            listener: None,
            drain: None,
            before_drain: None,
            actual_return: None,
        }));
        SELECTED.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "actual partial-drain control already armed"
            );
            *slot.borrow_mut() = Some(state.clone());
        });
        let cleanup = Cleanup;
        let actual = self.advance_finite_remote_serving_policy(range, phase, parent);
        let assigned=self.assigned.as_ref().map(|owner|serde_json::json!({
            "actual_original_Closed_life":owner.life.load(Ordering::Acquire),
            "actual_same_listener_retained":owner.listener.is_some(),"actual_stopping":owner.stopping,
        }));
        let evidence = self.test_registered_first_policy_evidence();
        let observed = state.borrow();
        let value = serde_json::json!({"actual_shutdown_active_drain_cut":observed.drain.as_ref().map(|d|d.snapshot()),
            "actual_authenticated_handler_before_drain":observed.before_drain,
            "actual_original_stop_and_drain_return":observed.actual_return,
            "actual_listener_after_failed_drain":observed.listener.as_ref().map(|h|h.snapshot()),
            "original_policy_API_returned_ok":actual.is_ok(),
            "original_policy_typed_QueryCanceled":matches!(&actual,Err(crate::BicDbError::QueryCanceled)),
            "original_policy_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_retained_Assigned":assigned,"actual_Joined_owner_present":self.joined.is_some(),
            "actual_original_install_evidence":evidence.as_ref().ok(),
            "install_evidence_error":evidence.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_policy_error_is_not_rewritten_by_test_DATA_return":true});
        drop(observed);
        drop(cleanup);
        Ok(value)
    }
    pub(in crate::distribution_native_host) fn test_actual_partial_drain_peer_call(
        &self,
        destination: &ClusterNodeId,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<impl FnOnce() -> Result<serde_json::Value> + Send + 'static> {
        parent.check()?;
        let transport = self.backend.transport.clone();
        let actual_member = self.backend.node_id.clone();
        let destination = destination.clone();
        let parent = parent.clone();
        // Return only an opaque single actual RPC call, not a raw transport,
        // native owner, Role/OWN or a caller/endpoint substitution API.
        Ok(move || {
            parent.check()?;
            let budget = parent
                .deadline()
                .and_then(|d| d.checked_duration_since(std::time::Instant::now()))
                .ok_or(crate::BicDbError::QueryTimedOut)?
                .min(Duration::from_secs(5));
            let actual = transport.acquire_host_metadata_observation(
                &destination,
                business,
                budget,
                &parent,
            );
            Ok(serde_json::json!({"actual_member_caller":actual_member,
                "actual_pinned_destination":destination,"actual_business":business,
                "actual_API_returned_ok":actual.is_ok(),
                "original_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                "actual_cluster_body":actual.as_ref().err().and_then(|e|match e{crate::BicDbError::Cluster(body)=>Some(body),_=>None}),
                "DATA_only_not_host_read_or_policy_authority":true}))
        })
    }
}
