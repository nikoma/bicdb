//! Passive selected-thread cancellation at the actual one-way Closed lifetime
//! boundary. No guard, admission, native outcome or listener state is set here.
use super::*;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Default)]
struct Observation {
    armed: bool,
    actual_cut: bool,
    remaining_us: Option<u128>,
    actual_drain_return: Option<serde_json::Value>,
}
thread_local! {
    static SELECTED: RefCell<Option<Rc<RefCell<Observation>>>> = const { RefCell::new(None) };
}
struct Selected;
impl Drop for Selected {
    fn drop(&mut self) {
        SELECTED.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
pub(super) fn after_actual_closed_life_ended(parent: &CancellationToken) -> Result<()> {
    let selected = SELECTED.with(|slot| slot.borrow().clone());
    let Some(selected) = selected else {
        return Ok(());
    };
    if !selected.borrow().armed {
        return Ok(());
    }
    parent.check()?;
    {
        let mut observed = selected.borrow_mut();
        observed.armed = false;
        observed.actual_cut = true;
        observed.remaining_us = parent.deadline().map(|d| {
            d.saturating_duration_since(std::time::Instant::now())
                .as_micros()
        });
    }
    // Cancel the SAME actual finite caller. The real stop_and_drain still
    // executes and its original typed Result is recorded, not manufactured.
    parent.cancel();
    Ok(())
}
pub(super) fn note_actual_drain_return(result: &Result<DrainedClusterDataListener>) {
    SELECTED.with(|slot| {
        let selected = slot.borrow().clone();
        if let Some(selected) = selected {
            let mut observed = selected.borrow_mut();
            if observed.actual_cut {
                observed.actual_drain_return = Some(serde_json::json!({
                    "actual_returned_ok":result.is_ok(),
                    "actual_typed_QueryCanceled":matches!(result,Err(crate::BicDbError::QueryCanceled)),
                    "original_error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                }));
            }
        }
    });
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_actual_registered_policy_drain_cancel(
        &mut self,
        range: crate::RangeId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        let observed = Rc::new(RefCell::new(Observation {
            armed: true,
            ..Observation::default()
        }));
        SELECTED.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "selected actual drain control already armed"
            );
            *slot.borrow_mut() = Some(observed.clone());
        });
        let selected = Selected;
        let actual = self.advance_finite_remote_serving_policy(range, phase, parent);
        drop(selected);
        // Capture after the exact actual failure before assertions. This DATA
        // cannot be consumed as recovery authority or replace the old error.
        let assigned = self.assigned.as_ref().map(|owner| {
            serde_json::json!({
                "actual_original_Closed_life":owner.life.load(Ordering::Acquire),
                "actual_same_listener_retained":owner.listener.is_some(),
                "actual_stopping":owner.stopping,
            })
        });
        let evidence = self.test_registered_first_policy_evidence();
        let observed = observed.borrow();
        Ok(serde_json::json!({
            "actual_cut_after_original_Closed_life_ended":observed.actual_cut,
            "actual_original_remaining_us_before_cancel":observed.remaining_us,
            "actual_stop_and_drain_return":observed.actual_drain_return,
            "original_policy_API_returned_ok":actual.is_ok(),
            "original_policy_typed_QueryCanceled":matches!(&actual,Err(crate::BicDbError::QueryCanceled)),
            "original_policy_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_retained_Assigned":assigned,
            "actual_Joined_owner_present":self.joined.is_some(),
            "actual_original_install_evidence":evidence.as_ref().ok(),
            "install_evidence_error":evidence.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "read_only_DATA_separate_from_original_policy_outcome":true,
        }))
    }
}
