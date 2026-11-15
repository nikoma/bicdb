//! One-shot observations on the actual admitted source request. These test
//! callbacks run outside registry, source-session and engine latch guards.
use super::*;

type Callback = Box<dyn FnOnce(&CancellationToken, Instant, serde_json::Value) + Send>;
pub(super) struct SourceMetadataWaitControl {
    stage: &'static str,
    before: Mutex<Option<Callback>>,
    busy: Mutex<Option<Callback>>,
    failure: Mutex<Option<Box<dyn FnOnce(&'static str) + Send>>>,
}
impl NativeRemoteImageSourceRegistry {
    pub(crate) fn test_arm_source_metadata_wait(
        &self,
        after_local: bool,
        before: Callback,
        busy: Callback,
        failure: Box<dyn FnOnce(&'static str) + Send>,
    ) -> Result<()> {
        let mut slot = self.metadata_wait_control.lock();
        if slot.is_some() {
            return Err(invalid("source metadata wait control already armed"));
        }
        *slot = Some(Arc::new(SourceMetadataWaitControl {
            stage: if after_local {
                "after_local_check"
            } else {
                "boundary"
            },
            before: Mutex::new(Some(before)),
            busy: Mutex::new(Some(busy)),
            failure: Mutex::new(Some(failure)),
        }));
        Ok(())
    }
    fn test_source_metadata_wait_control(
        &self,
        stage: &'static str,
    ) -> Option<Arc<SourceMetadataWaitControl>> {
        self.metadata_wait_control
            .lock()
            .as_ref()
            .filter(|control| control.stage == stage)
            .cloned()
    }
    pub(crate) fn test_before_source_metadata_wait(
        &self,
        stage: &'static str,
        caller: &CancellationToken,
        own: &HostMetadataReadObservation,
    ) {
        self.test_run_source_metadata_wait(stage, caller, own, false);
    }
    pub(crate) fn test_busy_source_metadata_wait(
        &self,
        stage: &'static str,
        caller: &CancellationToken,
        own: &HostMetadataReadObservation,
    ) {
        self.test_run_source_metadata_wait(stage, caller, own, true);
    }
    fn test_run_source_metadata_wait(
        &self,
        stage: &'static str,
        caller: &CancellationToken,
        own: &HostMetadataReadObservation,
        busy: bool,
    ) {
        let Some(control) = self.test_source_metadata_wait_control(stage) else {
            return;
        };
        let callback = if busy {
            control.busy.lock().take()
        } else {
            control.before.lock().take()
        };
        if let Some(callback) = callback {
            let now = Instant::now();
            callback(
                caller,
                own.deadline(),
                serde_json::json!({
                    "stage":stage,"actual_failed_lock_wait":busy,
                    "actual_own_context":own.context_id(),
                "actual_own_original_request_live":own.original_request_cancellation().check().is_ok(),
                    "actual_own_remaining_us":own.deadline().saturating_duration_since(now).as_micros(),
                    "actual_server_caller_remaining_us":caller.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
                    "actual_server_caller_cancel_requested":caller.is_cancel_requested(),
                    "comparison_DATA_not_authority":true
                }),
            );
        }
    }
    pub(crate) fn test_failed_source_metadata_wait(&self, stage: &'static str, error: &BicDbError) {
        let Some(control) = self.test_source_metadata_wait_control(stage) else {
            return;
        };
        let callback = control.failure.lock().take();
        if let Some(callback) = callback {
            callback(match error {
                BicDbError::QueryCanceled => "QueryCanceled",
                BicDbError::QueryTimedOut => "QueryTimedOut",
                _ => "OtherActualError",
            });
        }
    }
}
