//! Actual authenticated receiver pre-effect fault only. Weak registration,
//! borrowed !Send owner; no native/role/QQ or successful reply is fabricated.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
#[derive(Debug)]
pub(super) struct NativeApplyPreEffect {
    range: RangeId,
    index: u64,
    armed: AtomicBool,
    actual: OnceLock<serde_json::Value>,
}
pub(crate) struct NativeApplyPreEffectArm<'a> {
    runtime: &'a RangeRecoveryRuntime,
    actual: Arc<NativeApplyPreEffect>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for NativeApplyPreEffectArm<'_> {
    fn drop(&mut self) {
        self.actual.armed.store(false, Ordering::Release);
        if let Some(mut slot) = self.runtime.test_native_apply_pre_effect.try_lock() {
            if slot
                .as_ref()
                .is_some_and(|old| old.ptr_eq(&Arc::downgrade(&self.actual)))
            {
                *slot = None;
            }
        }
    }
}
impl NativeApplyPreEffectArm<'_> {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"actual_authenticated_pre_effect_request":self.actual.actual.get(),
            "armed":self.actual.armed.load(Ordering::Acquire),"diagnostic_not_ACK_or_native_floor":true})
    }
}
impl RangeRecoveryRuntime {
    pub(crate) fn test_arm_native_apply_pre_effect(
        &self,
        range: RangeId,
        index: u64,
    ) -> Result<NativeApplyPreEffectArm<'_>> {
        let mut slot = self
            .test_native_apply_pre_effect
            .try_lock()
            .ok_or_else(|| invalid("actual pre-effect fault slot busy"))?;
        if slot
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|old| old.armed.load(Ordering::Acquire))
        {
            return Err(invalid(
                "actual native apply pre-effect fault already armed",
            ));
        }
        let actual = Arc::new(NativeApplyPreEffect {
            range,
            index,
            armed: AtomicBool::new(true),
            actual: OnceLock::new(),
        });
        *slot = Some(Arc::downgrade(&actual));
        Ok(NativeApplyPreEffectArm {
            runtime: self,
            actual,
            _not_send: std::marker::PhantomData,
        })
    }
    pub(super) fn test_refuse_actual_native_apply_before_effect(
        &self,
        admission: &crate::distribution_native_apply::NativeLeaderInstructionAdmission,
    ) -> bool {
        let Some(slot) = self.test_native_apply_pre_effect.try_lock() else {
            return false;
        };
        let Some(actual) = slot.as_ref().and_then(std::sync::Weak::upgrade) else {
            return false;
        };
        let command = admission.command();
        if command.range_id != actual.range
            || command.index != actual.index
            || admission.require_live().is_err()
            || actual
                .armed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        // Immutable actual admission facts only; no DB read/fsync/wait or
        // callback behind an engine latch. Native/CQ cuts are proved later.
        let _ = actual.actual.set(serde_json::json!({"actual_command":command,
            "actual_current_role_context":admission.role().context_id(),"actual_authenticated_leader":admission.peer_node_id(),
            "actual_recipient":admission.role().local_node_id(),"before_native_effect":true}));
        true
    }
}
