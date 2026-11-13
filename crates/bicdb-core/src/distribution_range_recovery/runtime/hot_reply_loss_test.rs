//! Test-only loss AFTER the actual completion producer returned valid durable
//! evidence. No fabricated response, policy, native effect or recovery grant.
use super::*;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

#[derive(Debug)]
pub(super) struct ReplyLoss {
    range: RangeId,
    index: u64,
    armed: AtomicBool,
    suppressed: AtomicBool,
    actual_reply: OnceLock<crate::distribution_range_recovery::NativeCompletionReply>,
}
pub(crate) struct NativeHotReplyLossArm<'a> {
    runtime: &'a RangeRecoveryRuntime,
    actual: Arc<ReplyLoss>,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for NativeHotReplyLossArm<'_> {
    fn drop(&mut self) {
        self.actual.armed.store(false, Ordering::Release);
        if let Some(mut slot) = self.runtime.test_hot_completion_reply_loss.try_lock() {
            if slot
                .as_ref()
                .is_some_and(|old| old.ptr_eq(&Arc::downgrade(&self.actual)))
            {
                *slot = None;
            }
        }
    }
}
impl NativeHotReplyLossArm<'_> {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let suppressed = self.actual.suppressed.load(Ordering::Acquire);
        serde_json::json!({"suppressed_after_actual_producer":suppressed,
            "actual_reply":self.actual.actual_reply.get(),"armed":self.actual.armed.load(Ordering::Acquire),
            "scope":"cfg(test) reply loss only; no authority; no user ACK inferred"})
    }
}
impl RangeRecoveryRuntime {
    pub(crate) fn test_arm_hot_completion_reply_loss(
        &self,
        range: RangeId,
        index: u64,
    ) -> Result<NativeHotReplyLossArm<'_>> {
        let mut slot = self
            .test_hot_completion_reply_loss
            .try_lock()
            .ok_or_else(|| invalid("actual runtime reply-loss test slot busy"))?;
        if slot
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|old| old.armed.load(Ordering::Acquire))
        {
            return Err(invalid("actual runtime reply-loss test already armed"));
        }
        let actual = Arc::new(ReplyLoss {
            range,
            index,
            armed: AtomicBool::new(true),
            suppressed: AtomicBool::new(false),
            actual_reply: OnceLock::new(),
        });
        *slot = Some(Arc::downgrade(&actual));
        Ok(NativeHotReplyLossArm {
            runtime: self,
            actual,
            _not_send: PhantomData,
        })
    }
    pub(crate) fn test_suppress_actual_hot_completion_reply(
        &self,
        reply: &crate::distribution_range_recovery::NativeCompletionReply,
    ) -> bool {
        let Some(slot) = self.test_hot_completion_reply_loss.try_lock() else {
            return false;
        };
        let Some(actual) = slot.as_ref().and_then(std::sync::Weak::upgrade) else {
            return false;
        };
        if reply.scope.range_id != actual.range
            || reply.range_index != actual.index
            || reply.stage
                != crate::distribution_range_recovery::NativeCompletionStage::CompletedFrozen
            || actual
                .armed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        // Actual producer fact, not a caller DTO. No DB/service/journal guard
        // is held here. Publish immutable data before the test observes fired.
        let _ = actual.actual_reply.set(reply.clone());
        actual.suppressed.store(true, Ordering::Release);
        true
    }
}
