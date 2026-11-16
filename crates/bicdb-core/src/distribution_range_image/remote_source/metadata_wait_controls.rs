//! One-shot observation callbacks, extracted without retaining the registry or
//! any source/session/native owner across an engine wait.
use super::*;

type Callback = Box<dyn FnOnce(&CancellationToken, Instant, serde_json::Value) + Send>;
type Failure = Box<dyn FnOnce(&'static str) + Send>;
pub(super) struct SourceMetadataWaitControl {
    stage: &'static str,
    before: Callback,
    busy: Callback,
    failure: Failure,
}
impl NativeRemoteImageSourceRegistry {
    pub(crate) fn test_arm_source_metadata_wait(
        &self,
        after_local: bool,
        before: Callback,
        busy: Callback,
        failure: Failure,
    ) -> Result<()> {
        self.metadata_wait_control
            .set(Mutex::new(Some(SourceMetadataWaitControl {
                stage: if after_local {
                    "after_local_check"
                } else {
                    "boundary"
                },
                before,
                busy,
                failure,
            })))
            .map_err(|_| invalid("source metadata wait control already armed"))
    }
    pub(crate) fn test_take_source_metadata_wait(
        &self,
        stage: &'static str,
    ) -> Option<(Callback, Callback, Failure)> {
        let mut slot = self.metadata_wait_control.get()?.lock();
        if !slot.as_ref().is_some_and(|control| control.stage == stage) {
            return None;
        }
        let control = slot.take()?;
        // Only the callbacks escape. The mutex guard and the caller's
        // temporary registry Arc drop before any callback or blocking wait.
        Some((control.before, control.busy, control.failure))
    }
}
