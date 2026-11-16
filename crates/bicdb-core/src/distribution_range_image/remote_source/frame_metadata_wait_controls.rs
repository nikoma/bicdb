//! One-shot controls on the actual published source, at its FINAL MStore check.
//! Callbacks retain only test state; no source/metadata/DB owner escapes.
use super::*;

type Callback = Box<dyn FnOnce(&CancellationToken, serde_json::Value) + Send>;
type Returned = Box<dyn FnOnce(serde_json::Value) + Send>;
pub(super) struct FrameMetadataWaitControl {
    before: Mutex<Option<Callback>>,
    busy: Mutex<Option<Callback>>,
    returned: Mutex<Option<Returned>>,
}
impl NativeRemoteImageSourceSession {
    fn frame_wait_facts(
        &self,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> serde_json::Value {
        let now = Instant::now();
        serde_json::json!({"source_session":self.source_session,"target_attempt":self.target_attempt,
            "manifest_sha256":self.manifest.as_ref().map(|m|&m.checksum_sha256),
            "business":business,"own_context":own.context_id(),"own_live":own.require_live_request(business).is_ok(),
            "own_remaining_us":own.deadline().saturating_duration_since(now).as_micros(),
            "request_live":parent.check().is_ok(),"request_cancel_requested":parent.is_cancel_requested(),
            "request_remaining_us":parent.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
            "original_source_session_live":self.original_session.check().is_ok(),
            "original_source_session_cancel_requested":self.original_session.is_cancel_requested()})
    }
    pub(super) fn test_before_frame_final_metadata_lock(
        &self,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) {
        let callback = self
            .frame_metadata_wait
            .get()
            .and_then(|control| control.before.lock().take());
        if let Some(callback) = callback {
            callback(parent, self.frame_wait_facts(own, business, parent));
        }
    }
    pub(super) fn test_frame_final_metadata_busy(
        &self,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) {
        let callback = self
            .frame_metadata_wait
            .get()
            .and_then(|control| control.busy.lock().take());
        if let Some(callback) = callback {
            callback(parent, self.frame_wait_facts(own, business, parent));
        }
    }
    pub(super) fn test_take_frame_wait_return(
        &self,
        actual: &Result<ActualNativeRemoteImageSourceFrame>,
        request: &NativeRemoteImageSourceRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        registry_request: &CancellationToken,
    ) -> Option<(Returned, serde_json::Value)> {
        let callback = self
            .frame_metadata_wait
            .get()
            .and_then(|control| control.returned.lock().take());
        if let Some(callback) = callback {
            let mut facts = self.frame_wait_facts(own, business, registry_request);
            facts["actual_server_result"] = match actual {
                Ok(_) => "Ok",
                Err(BicDbError::QueryCanceled) => "QueryCanceled",
                Err(BicDbError::QueryTimedOut) => "QueryTimedOut",
                Err(_) => "OtherError",
            }
            .into();
            facts["request_is_outer_registry_parent"] = true.into();
            facts["actual_request"] = serde_json::to_value(request).unwrap();
            // Facts reflect the original return cut. Delivery must wait until
            // the registry releases the actual source-session guard.
            Some((callback, facts))
        } else {
            None
        }
    }
}
impl NativeRemoteImageSourceRegistry {
    pub(crate) fn test_arm_frame_wait(
        &self,
        source_session: Uuid,
        expected: &Arc<Mutex<NativeRemoteImageSourceSession>>,
        before: Callback,
        busy: Callback,
        returned: Returned,
    ) -> Result<()> {
        self.require_original_process()?;
        let actual = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("frame wait actual source table busy"))?
            .get(&source_session)
            .cloned()
            .ok_or_else(|| invalid("frame wait actual source absent"))?;
        if !Arc::ptr_eq(&actual, expected) {
            return Err(invalid("frame wait changed original retained source"));
        }
        let source = actual
            .try_lock()
            .ok_or_else(|| invalid("frame wait actual source busy"))?;
        let manifest = source.manifest()?;
        if !source.published
            || source.capture.is_none()
            || manifest.source_session != source_session
        {
            return Err(invalid("frame wait requires actual published capture"));
        }
        source
            .frame_metadata_wait
            .set(FrameMetadataWaitControl {
                before: Mutex::new(Some(before)),
                busy: Mutex::new(Some(busy)),
                returned: Mutex::new(Some(returned)),
            })
            .map_err(|_| invalid("actual source frame wait already armed"))
    }
}
impl NativeRetainedRemoteImageSource {
    pub(crate) fn test_arm_frame_wait(
        &self,
        before: Callback,
        busy: Callback,
        returned: Returned,
    ) -> Result<()> {
        let id = self
            .session
            .try_lock()
            .ok_or_else(|| invalid("frame wait retained source busy"))?
            .source_session;
        self.registry
            .test_arm_frame_wait(id, &self.session, before, busy, returned)
    }
    /// Actual immutable facts only. No Frozen/read-pin/session handle escapes.
    pub(crate) fn test_frame_wait_source_snapshot(&self) -> Result<serde_json::Value> {
        let source = self
            .session
            .try_lock()
            .ok_or_else(|| invalid("frame wait source diagnostic busy"))?;
        let capture = source
            .capture
            .as_ref()
            .ok_or_else(|| invalid("frame wait capture diagnostic absent"))?;
        let db = read_database(&capture.db)?;
        let range = source.manifest()?.capture_range.id;
        Ok(
            serde_json::json!({"source_session":source.source_session,"manifest":source.manifest,
            "published":source.published,"capture_attempted":source.capture_attempted,
            "capture_frozen_owner":capture.frozen.owner(),
            "capture_lifetime_allocation":Arc::as_ptr(&capture.closed_image_lifetime) as usize,
            "capture_read_pin_allocation":Arc::as_ptr(&capture.source_read_pin) as usize,
            "source_session_live":source.original_session.check().is_ok(),
            "source_session_cancel_requested":source.original_session.is_cancel_requested(),
            "database_incarnation":db.incarnation_id(),"commit_sequence":db.current_commit_seq(),
            "native_storage":db.test_joint_prepublication_storage(range)?,
            "rows":db.scan_collection(crate::CLUSTER_EVENTS_COLLECTION)?,
            "actual_gate":db.range_mutation_gate().test_source_capture_pin_state(range)?}),
        )
    }
}
