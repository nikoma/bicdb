//! A real canceled bounded maintenance turn, keyed to the actual pending
//! command on this original DATA owner. No fake role/error or engine mutation.
use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::{OnceLock, Weak};

pub(crate) struct SuccessorCancelProbe {
    owner: Weak<NativeHotRecoveryMaintenance>,
    database: Weak<parking_lot::RwLock<crate::BicDb>>,
    range: crate::RangeId,
    command: String,
    checksum: String,
    fired: AtomicBool,
    observed: OnceLock<serde_json::Value>,
}
pub(crate) struct NativeSuccessorCancelArm {
    probe: Arc<SuccessorCancelProbe>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for NativeSuccessorCancelArm {
    fn drop(&mut self) {
        if let Some(owner) = self.probe.owner.upgrade() {
            if let Some(mut slot) = owner.successor_cancel_test.try_lock() {
                if slot
                    .as_ref()
                    .is_some_and(|old| Weak::ptr_eq(old, &Arc::downgrade(&self.probe)))
                {
                    slot.take();
                }
            }
        }
    }
}
impl NativeSuccessorCancelArm {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"fired":self.probe.fired.load(Ordering::Acquire),
            "actual_failed_acquisition":self.probe.observed.get(),
            "diagnostic_not_authority":true})
    }
}
impl NativeHotRecoveryMaintenance {
    pub(in crate::distribution_native_host) fn arm_successor_cancel(
        self: &Arc<Self>,
        command: &RangeWriteCommand,
    ) -> Result<NativeSuccessorCancelArm> {
        command.validate()?;
        let mut slot = self
            .successor_cancel_test
            .try_lock()
            .ok_or_else(|| invalid("actual successor cancellation test slot busy"))?;
        if slot.as_ref().and_then(Weak::upgrade).is_some() {
            return Err(invalid("actual successor cancellation test already armed"));
        }
        let probe = Arc::new(SuccessorCancelProbe {
            owner: Arc::downgrade(self),
            database: Arc::downgrade(&self.initial.seed.database),
            range: command.range_id,
            command: command.command_id.clone(),
            checksum: command.checksum_sha256.clone(),
            fired: AtomicBool::new(false),
            observed: OnceLock::new(),
        });
        *slot = Some(Arc::downgrade(&probe));
        Ok(NativeSuccessorCancelArm {
            probe,
            _local: std::marker::PhantomData,
        })
    }
    pub(super) fn cancel_actual_successor_turn(
        &self,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Option<Arc<SuccessorCancelProbe>> {
        let probe = self.successor_cancel_test.try_lock()?.as_ref()?.upgrade()?;
        if parent.check().is_err()
            || probe.range != command.range_id
            || probe.command != command.command_id
            || probe.checksum != command.checksum_sha256
            || probe
                .database
                .upgrade()
                .is_none_or(|db| !Arc::ptr_eq(&db, &self.initial.seed.database))
            || probe
                .fired
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return None;
        }
        // Cancel only this real worker turn's token, never its process parent.
        parent.cancel();
        Some(probe)
    }
    pub(super) fn observe_successor_acquisition(
        &self,
        probe: Option<Arc<SuccessorCancelProbe>>,
        state: &State,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
        result: &Result<RecoveryRoleAdmission>,
    ) {
        let Some(probe) = probe else {
            return;
        };
        let actual = serde_json::json!({"range_id":command.range_id,
            "actual_command":command.command_id,"actual_turn_canceled":parent.check().is_err(),
            "actual_role_error":result.as_ref().err().map(ToString::to_string),
            "prior_session_retained":state.replica_debts.contains_key(&command.range_id),
            "successor_job_created":state.jobs.contains_key(&command.range_id),
            "prior_session":state.replica_debts.get(&command.range_id).map(|d|d.diagnostic("actual_canceled_successor_acquisition"))});
        let _ = probe.observed.set(actual);
    }
}
