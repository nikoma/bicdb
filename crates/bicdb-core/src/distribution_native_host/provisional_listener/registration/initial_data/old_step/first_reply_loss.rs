//! One actual retained INITIAL owner, actual returned Close, then wire loss.
//! Weak-owner test synchronization only; no native permission/owner escapes.
use super::*;
use crate::distribution_range_handoff::{
    NativeRemoteOldOperationV1 as Op, NativeRemoteOldReplyV1, NativeRemoteOldRequestV1,
};
struct Receipt {
    close_dispatches: u64,
    observe_dispatches: u64,
    armed: bool,
    event: Option<serde_json::Value>,
}
struct Hook {
    original: std::sync::Weak<NativeInitialDataAdmission>,
    range: crate::RangeId,
    receipt: Arc<Mutex<Receipt>>,
}
static HOOKS: Mutex<Vec<Hook>> = Mutex::new(Vec::new());
struct Guard(Arc<Mutex<Receipt>>);
impl Drop for Guard {
    fn drop(&mut self) {
        HOOKS.lock().retain(|h| !Arc::ptr_eq(&h.receipt, &self.0));
    }
}
impl NativeInitialDataAdmission {
    pub(crate) fn test_first_arm_close_reply_loss(
        self: &Arc<Self>,
        range: crate::RangeId,
    ) -> Result<impl Drop + Send + 'static> {
        self.require_live()?;
        let mut hooks = HOOKS.lock();
        hooks.retain(|h| h.original.strong_count() > 0);
        if hooks.len() >= 16
            || hooks.iter().any(|h| {
                std::sync::Weak::ptr_eq(&h.original, &Arc::downgrade(self)) && h.range == range
            })
        {
            return Err(invalid(
                "FIRST actual Close reply-loss control already owned/capacity",
            ));
        }
        let receipt = Arc::new(Mutex::new(Receipt {
            close_dispatches: 0,
            observe_dispatches: 0,
            armed: true,
            event: None,
        }));
        hooks.push(Hook {
            original: Arc::downgrade(self),
            range,
            receipt: receipt.clone(),
        });
        Ok(Guard(receipt))
    }
    pub(crate) fn test_first_note_old_dispatch(&self, request: &NativeRemoteOldRequestV1) {
        let receipt = HOOKS
            .lock()
            .iter()
            .find(|h| h.original.as_ptr() == self as *const Self && h.range == request.range_id)
            .map(|h| h.receipt.clone());
        if let Some(receipt) = receipt {
            let mut actual = receipt.lock();
            match request.operation {
                Op::Close => actual.close_dispatches = actual.close_dispatches.saturating_add(1),
                Op::Observe => {
                    actual.observe_dispatches = actual.observe_dispatches.saturating_add(1)
                }
                _ => {}
            }
        }
    }
    pub(crate) fn test_first_take_close_reply_loss(&self, reply: &NativeRemoteOldReplyV1) -> bool {
        if reply.request.operation != Op::Close
            || reply.closed.is_none()
            || reply.closed_native_record.is_none()
        {
            return false;
        }
        let receipt = HOOKS
            .lock()
            .iter()
            .find(|h| {
                h.original.as_ptr() == self as *const Self && h.range == reply.request.range_id
            })
            .map(|h| h.receipt.clone());
        let Some(receipt) = receipt else {
            return false;
        };
        // Real API has returned, all native/session guards ended. Serialize
        // only its actual already-validated DATA before the control lock.
        let event = serde_json::json!({"cut":"actual_Close_success_before_wire_reply_loss","fired":true,
            "actual_reply":reply,"local_quorum_or_role_created":false});
        let mut actual = receipt.lock();
        if !actual.armed {
            return false;
        }
        actual.armed = false;
        actual.event = Some(event);
        true
    }
    pub(crate) fn test_first_close_reply_loss_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let receipt = HOOKS
            .lock()
            .iter()
            .find(|h| h.original.as_ptr() == self as *const Self && h.range == range)
            .map(|h| h.receipt.clone())
            .ok_or_else(|| invalid("FIRST actual Close control absent"))?;
        let actual = receipt.lock();
        Ok(
            serde_json::json!({"close_dispatches":actual.close_dispatches,"observe_dispatches":actual.observe_dispatches,
            "armed":actual.armed,"event":actual.event,"data_only":true}),
        )
    }
}
