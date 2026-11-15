//! Private cfg(test) cuts on the ORIGINAL actual pinned callback. No token,
//! Role, DTO authority, new request, or mutable native owner is exposed.
use super::*;
use crate::distribution_transport::AuthenticatedRegisteredExportNew;
use std::sync::atomic::Ordering;

const CANCEL: u8 = 1;
const EXPIRE: u8 = 2;
const EXPIRY_CALLBACK_MS: u64 = 600;

impl NativeRemoteImageSourceRegistry {
    pub(crate) fn test_arm_callback_request_fault(&self, expire: bool) -> Result<()> {
        self.export_callback_fault
            .compare_exchange(
                0,
                if expire { EXPIRE } else { CANCEL },
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| invalid("actual callback request cut already armed"))?;
        Ok(())
    }
    pub(crate) fn test_callback_request_budget(&self, unchanged: u64) -> u64 {
        if self.export_callback_fault.load(Ordering::Acquire) == EXPIRE {
            // SHORTEN the actual outbound request before its real mTLS call.
            // The target and sealed client token both receive this same budget.
            unchanged.min(EXPIRY_CALLBACK_MS)
        } else {
            unchanged
        }
    }
    pub(crate) fn test_callback_request_trace(&self) -> Result<serde_json::Value> {
        let mut trace = self
            .export_callback_trace
            .try_lock()
            .ok_or_else(|| invalid("actual callback request trace busy"))?
            .clone()
            .unwrap_or(serde_json::Value::Null);
        let bits = self.export_callback_final_bits.load(Ordering::Acquire);
        trace["actual_final_point"] = serde_json::json!({
            "guard_check_reached":bits & 1 != 0,"callback_canceled":bits & 2 != 0,
            "callback_expired":bits & 4 != 0,"callback_live":bits & 8 != 0,
            "outer_parent_live":bits & 16 != 0,"original_source_session_live":bits & 32 != 0,
            "original_source_process_live":bits & 64 != 0,"own_deadline_live":bits & 128 != 0,
            "own_remaining_us":self.export_callback_final_own_remaining_us.load(Ordering::Acquire),
            "memory_only_observation":true});
        Ok(trace)
    }
    /// Only atomics, existing request-memory checks and monotonic-clock reads.
    /// No allocation, serialization, callback, native access or extra latch.
    pub(super) fn test_record_callback_final_point(
        &self,
        callback: &Result<()>,
        parent: &CancellationToken,
        session: &CancellationToken,
        own_deadline: Instant,
    ) {
        let mut bits = 1;
        if matches!(callback, Err(BicDbError::QueryCanceled)) {
            bits |= 2;
        }
        if matches!(callback, Err(BicDbError::QueryTimedOut)) {
            bits |= 4;
        }
        if callback.is_ok() {
            bits |= 8;
        }
        if parent.check().is_ok() {
            bits |= 16;
        }
        if session.check().is_ok() {
            bits |= 32;
        }
        if self
            .original_process
            .get()
            .is_some_and(|process| process.check().is_ok())
        {
            bits |= 64;
        }
        let remaining = own_deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            bits |= 128;
        }
        self.export_callback_final_own_remaining_us.store(
            remaining.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Release,
        );
        self.export_callback_final_bits
            .store(bits, Ordering::Release);
    }
    pub(super) fn test_run_callback_request_cut(
        &self,
        callback: &AuthenticatedRegisteredExportNew,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        session: &CancellationToken,
    ) -> Result<()> {
        let mode = self.export_callback_fault.swap(0, Ordering::AcqRel);
        if mode == 0 {
            return Ok(());
        }
        let started = Instant::now();
        let scopes = || {
            serde_json::json!({
            "callback":callback.test_request_liveness(),
            "own_live":own.require_live_request(business).is_ok(),
            "own_remaining_us":own.deadline().saturating_duration_since(Instant::now()).as_micros(),
            "outer_parent_live":parent.check().is_ok(),
            "outer_parent_remaining_us":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
            "original_source_process_live":self.require_original_process().is_ok(),
            "original_source_session_live":session.check().is_ok(),
            "original_source_session_cancel_requested":session.is_cancel_requested()})
        };
        let before = scopes();
        *self
            .export_callback_trace
            .try_lock()
            .ok_or_else(|| invalid("actual callback trace custody busy before cut"))? =
            Some(serde_json::json!({
                "seam_reached":true,"phase":"before_actual_callback_fault","before":before,
                "actual_callback_request":callback.reply().request,"mode":mode,
                "no_new_request_or_deadline_renewal":true}));
        let action = (|| -> Result<()> {
            callback.require_request_live()?;
            own.require_live_request(business)?;
            parent.check()?;
            self.require_original_process()?;
            session.check()?;
            match mode {
                CANCEL => callback.test_cancel_original_request(),
                EXPIRE => {
                    let deadline = callback
                        .test_original_request_deadline()
                        .ok_or_else(|| invalid("actual callback original deadline missing"))?;
                    while Instant::now() < deadline {
                        // These DISTINCT original scopes must remain live. A
                        // slow prerequisite is preserved as a control failure.
                        own.require_live_request(business)?;
                        parent.check()?;
                        self.require_original_process()?;
                        session.check()?;
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
                _ => return Err(invalid("unknown actual callback request test cut")),
            }
            Ok(())
        })();
        let after = scopes();
        *self
            .export_callback_trace
            .try_lock()
            .ok_or_else(|| invalid("actual callback trace custody busy after cut"))? = Some(
            serde_json::json!({
                "seam_reached":true,"phase":"after_actual_callback_fault","before":before,"after":after,
                "actual_callback_request":callback.reply().request,"mode":mode,
                "elapsed_us":started.elapsed().as_micros(),
                "action_error":action.as_ref().err().map(|e|format!("{e:?}")),
                "no_new_request_or_deadline_renewal":true}),
        );
        action
    }
}
