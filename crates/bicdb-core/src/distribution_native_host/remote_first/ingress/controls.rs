//! Actual server-request cuts. No token/owner escapes this test-only module.
//! These controls are separate from the bounded passive returned-DATA ledger.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::distribution_native_host) enum NativeFirstIngressFault {
    CancelBeforeConsume(NativeFirstReceiveOperationV1),
    ExpireBeforeConsume(NativeFirstReceiveOperationV1),
    LoseSuccessfulReply(NativeFirstReceiveOperationV1),
    CancelOriginalAfterFinalWal,
    HoldBeforeConsume(NativeFirstReceiveOperationV1),
}
pub(super) struct State {
    armed: Option<NativeFirstIngressFault>,
    event: Option<serde_json::Value>,
    reply_loss_sequence: Option<u64>,
    witness_attempts: u64,
    barrier: Option<Arc<BarrierState>>,
}
impl State {
    pub(super) fn new() -> Self {
        Self {
            armed: None,
            event: None,
            reply_loss_sequence: None,
            witness_attempts: 0,
            barrier: None,
        }
    }
}
struct BarrierState {
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    event: Mutex<Option<serde_json::Value>>,
}
/// Test synchronization DATA only. It has no token, native owner, Frozen,
/// Role/OWN or admission method. Dropping it releases only the test barrier.
pub(in crate::distribution_native_host) struct NativeFirstIngressBarrier(Arc<BarrierState>);
impl Drop for NativeFirstIngressBarrier {
    fn drop(&mut self) {
        self.release();
    }
}
impl NativeFirstIngressBarrier {
    pub(in crate::distribution_native_host) fn wait_entered(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        while !self.0.entered.load(std::sync::atomic::Ordering::Acquire) {
            parent.check()?;
            std::thread::sleep(Duration::from_millis(2));
        }
        parent.check()
    }
    pub(in crate::distribution_native_host) fn release(&self) {
        self.0
            .released
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(in crate::distribution_native_host) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"entered":self.0.entered.load(std::sync::atomic::Ordering::Acquire),
            "released":self.0.released.load(std::sync::atomic::Ordering::Acquire),"event":*self.0.event.lock(),
            "native_owner_or_token_exported":false})
    }
}
impl NativeFirstReceiveQueue {
    pub(in crate::distribution_native_host) fn test_arm(
        &self,
        fault: NativeFirstIngressFault,
    ) -> Result<()> {
        let mut state = self.controls.lock();
        if state.armed.is_some() || state.reply_loss_sequence.is_some() {
            return Err(invalid("FIRST actual ingress test cut already armed"));
        }
        state.armed = Some(fault);
        state.event = None;
        Ok(())
    }
    pub(in crate::distribution_native_host) fn test_arm_barrier(
        &self,
        op: NativeFirstReceiveOperationV1,
    ) -> Result<NativeFirstIngressBarrier> {
        let mut controls = self.controls.lock();
        if controls.armed.is_some() || controls.barrier.is_some() {
            return Err(invalid("FIRST actual ingress barrier already armed"));
        }
        let state = Arc::new(BarrierState {
            entered: std::sync::atomic::AtomicBool::new(false),
            released: std::sync::atomic::AtomicBool::new(false),
            event: Mutex::new(None),
        });
        controls.armed = Some(NativeFirstIngressFault::HoldBeforeConsume(op));
        controls.barrier = Some(state.clone());
        controls.event = None;
        Ok(NativeFirstIngressBarrier(state))
    }
    pub(in crate::distribution_native_host) fn test_witness_started(&self) {
        let mut state = self.controls.lock();
        state.witness_attempts = state.witness_attempts.saturating_add(1);
    }
    pub(super) fn test_before_consume(&self, actual: &mut NativeFirstReceiveIngress) {
        let fault = {
            let mut state = self.controls.lock();
            let matching = state.armed.is_some_and(|f| match f {
                NativeFirstIngressFault::CancelBeforeConsume(op)
                | NativeFirstIngressFault::ExpireBeforeConsume(op)
                | NativeFirstIngressFault::LoseSuccessfulReply(op)
                | NativeFirstIngressFault::HoldBeforeConsume(op) => op == actual.request.operation,
                NativeFirstIngressFault::CancelOriginalAfterFinalWal => {
                    actual.request.operation == NativeFirstReceiveOperationV1::PublishFinalImage
                }
            });
            if matching {
                state.armed.take()
            } else {
                None
            }
        };
        actual.test_fault = fault;
        match fault {
            Some(NativeFirstIngressFault::CancelBeforeConsume(_)) => {
                actual.token.cancel();
                self.test_fired(
                    actual,
                    "actual_original_server_token_cancelled_before_consumer",
                );
            }
            Some(NativeFirstIngressFault::ExpireBeforeConsume(_)) => {
                // NO queue/service/DB/journal/version guard is retained here.
                // Wait ONLY for this actual original ingress deadline.
                while actual.token.check().is_ok() {
                    std::thread::sleep(Duration::from_millis(2));
                }
                self.test_fired(
                    actual,
                    "actual_original_server_window_ended_before_consumer",
                );
            }
            Some(NativeFirstIngressFault::HoldBeforeConsume(_)) => {
                let barrier = self
                    .controls
                    .lock()
                    .barrier
                    .take()
                    .expect("armed actual barrier state");
                *barrier.event.lock() = Some(
                    serde_json::json!({"sequence":actual.request.sequence,
                    "original_window_live_at_entry":actual.token.check().is_ok(),"actual_original_server_token":true}),
                );
                barrier
                    .entered
                    .store(true, std::sync::atomic::Ordering::Release);
                // No queue/service/DB/journal/version guard. This original
                // actual ingress window is NEVER lengthened by the barrier.
                while !barrier.released.load(std::sync::atomic::Ordering::Acquire)
                    && actual.token.check().is_ok()
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                *barrier.event.lock() = Some(
                    serde_json::json!({"sequence":actual.request.sequence,
                    "original_window_live_after_hold":actual.token.check().is_ok(),
                    "original_cancelled_after_hold":actual.token.is_cancel_requested(),
                    "original_deadline_expired_after_hold":actual.token.deadline().is_some_and(|d|Instant::now()>=d),
                    "actual_original_server_token":true}),
                );
                self.test_fired(
                    actual,
                    "actual_original_admitted_window_held_before_consumer",
                );
            }
            _ => {}
        }
    }
    fn test_fired(&self, actual: &NativeFirstReceiveIngress, cut: &str) {
        self.controls.lock().event = Some(serde_json::json!({"cut":cut,"fired":true,
            "sequence":actual.request.sequence,"operation":actual.request.operation,
            "original_operation":actual.request.original_operation,
            "original_server_deadline_expired_at_snapshot":actual.token.deadline().is_some_and(|d|Instant::now()>=d),
            "original_server_cancelled_at_snapshot":actual.token.is_cancel_requested(),
            "actual_original_server_token":true,"native_grant_created":false}));
    }
    pub(super) fn test_after_complete(
        &self,
        actual: &NativeFirstReceiveIngress,
        result: &Result<NativeFirstReceiveReplyV1>,
    ) {
        if result.is_ok()
            && matches!(
                actual.test_fault,
                Some(NativeFirstIngressFault::LoseSuccessfulReply(_))
            )
        {
            self.test_fired(
                actual,
                "actual_success_retained_before_wire_completion_reply_loss",
            );
            self.controls.lock().reply_loss_sequence = Some(actual.request.sequence);
        }
    }
    pub(in crate::distribution_native_host) fn test_take_reply_loss(&self, sequence: u64) -> bool {
        let mut state = self.controls.lock();
        if state.reply_loss_sequence == Some(sequence) {
            state.reply_loss_sequence = None;
            true
        } else {
            false
        }
    }
    pub(in crate::distribution_native_host) fn test_snapshot(&self) -> serde_json::Value {
        // Locks are deliberately separate; this is diagnostic, not an atomic
        // cross-thread completeness claim or authority.
        let admission = {
            let state = self.state.lock();
            serde_json::json!({
            "admitted_watermark":state.admitted,"last_effect_admitted":state.last_effect_admitted,
            "in_flight":state.current.as_ref().map(|x|serde_json::json!({"sequence":x.sequence,
                "processing":x.processing,"original_server_window_live_at_snapshot":x.token.check().is_ok(),
                "original_server_deadline_expired_at_snapshot":x.token.deadline().is_some_and(|d|Instant::now()>=d),
                "original_server_cancelled_at_snapshot":x.token.is_cancel_requested()})),
            "original_unknown":state.unknown.as_ref().map(|x|serde_json::json!({"sequence":x.request.sequence,
                "request_sha256":x.hash,"classification":x.classification,"error":x.error})),
            "last_return":state.last.as_ref().map(|x|serde_json::json!({"sequence":x.request.sequence,
                "classification":x.classification,"reply":x.reply,"error":x.error})),
            "returned_count":state.returned_count,"uncertain_count":state.uncertain_count,
            "chain_sha256":hex::encode(state.chain.clone().finalize())})
        };
        let controls = self.controls.lock();
        serde_json::json!({"admission":admission,"control":controls.event,
            "armed":controls.armed.map(|x|format!("{x:?}")),"actual_witness_attempts":controls.witness_attempts,
            "global_capture_complete":false,"data_only":true})
    }
}
impl NativeFirstReceiveIngress {
    pub(in crate::distribution_native_host) fn test_native_final_cut(&self) -> Option<impl Drop> {
        if !matches!(
            self.test_fault,
            Some(NativeFirstIngressFault::CancelOriginalAfterFinalWal)
        ) {
            return None;
        }
        let token = self.token.clone();
        let queue = self.queue.clone();
        let request = self.request.clone();
        Some(crate::db::test_registered_remote_final_image_after_wal(
            Box::new(move || {
                // Existing actual native seam: returned xid retained, Controlled
                // already dropped. Cancel SAME original ingress, not client-only.
                token.cancel();
                if let Some(queue) = queue.upgrade() {
                    queue.controls.lock().event = Some(
                        serde_json::json!({"cut":"actual_native_final_return_then_original_server_cancel",
                    "fired":true,"sequence":request.sequence,"original_operation":request.original_operation,
                    "original_server_cancelled_at_snapshot":token.is_cancel_requested(),
                    "original_server_deadline_expired_at_snapshot":token.deadline().is_some_and(|d|Instant::now()>=d),
                    "new_transaction_or_owner_created":false}),
                    );
                }
            }),
        ))
    }
}
