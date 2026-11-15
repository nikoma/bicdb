//! Test-only selected authenticated response hold. No protocol or DATA grant.
use super::*;
use crate::distribution_range_image::{
    NativeRemoteImageManifestV1, NativeRemoteImageSourceOperationV1, NativeRemoteImageSourceReplyV1,
};
use parking_lot::Condvar;
use sha2::Digest;
use std::sync::{OnceLock, Weak};

#[derive(Default)]
struct State {
    reached: Option<serde_json::Value>,
    released: bool,
    resumed: bool,
    expired: bool,
}
struct Hold {
    manifest_sha: String,
    target_attempt: Uuid,
    source: ClusterNodeId,
    target: ClusterNodeId,
    state: Mutex<State>,
    changed: Condvar,
}
fn holds() -> &'static Mutex<BTreeMap<Uuid, Weak<Hold>>> {
    static HOLDS: OnceLock<Mutex<BTreeMap<Uuid, Weak<Hold>>>> = OnceLock::new();
    HOLDS.get_or_init(Default::default)
}
pub(crate) struct ActualRowsReplyHold {
    session: Uuid,
    hold: Arc<Hold>,
}
impl ActualRowsReplyHold {
    pub(crate) fn install(manifest: &NativeRemoteImageManifestV1) -> Self {
        let hold = Arc::new(Hold {
            manifest_sha: manifest.checksum_sha256.clone(),
            target_attempt: manifest.target_attempt,
            source: manifest.source_id.clone(),
            target: manifest.target_id.clone(),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        });
        let mut entries = holds().lock();
        assert!(
            entries
                .get(&manifest.source_session)
                .and_then(Weak::upgrade)
                .is_none(),
            "selected session already held"
        );
        entries.insert(manifest.source_session, Arc::downgrade(&hold));
        Self {
            session: manifest.source_session,
            hold,
        }
    }
    pub(crate) fn wait_reached(&self, parent: &CancellationToken) -> Result<()> {
        let mut state = self.hold.state.lock();
        while state.reached.is_none() {
            parent.check()?;
            self.hold
                .changed
                .wait_for(&mut state, Duration::from_millis(5));
        }
        Ok(())
    }
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let state = self.hold.state.lock();
        serde_json::json!({"actual_rows_reply":state.reached,"released":state.released,
            "handler_resumed":state.resumed,"original_request_expired":state.expired,
            "currently_held":state.reached.is_some() && !state.released && !state.resumed,
            "hold_is_transport_only_after_actual_export":true})
    }
    pub(crate) fn release(&self) {
        self.hold.state.lock().released = true;
        self.hold.changed.notify_all();
    }
}
impl Drop for ActualRowsReplyHold {
    fn drop(&mut self) {
        // Even an assertion unwind releases the handler; its own unchanged
        // request deadline independently bounds a missing controller release.
        self.release();
        let mut entries = holds().lock();
        if entries
            .get(&self.session)
            .and_then(Weak::upgrade)
            .is_some_and(|actual| Arc::ptr_eq(&actual, &self.hold))
        {
            entries.remove(&self.session);
        }
    }
}
pub(super) fn hold_actual_rows_reply(
    reply: &NativeRemoteImageSourceReplyV1,
    parent: &CancellationToken,
) -> Result<()> {
    if !matches!(
        &reply.request.operation,
        NativeRemoteImageSourceOperationV1::Rows { .. }
    ) {
        return Ok(());
    }
    let hold = holds()
        .lock()
        .get(&reply.request.source_session)
        .and_then(Weak::upgrade);
    let Some(hold) = hold else { return Ok(()) };
    if hold.manifest_sha != reply.manifest_sha256
        || hold.target_attempt != reply.request.target_attempt
        || hold.source != reply.source_id
        || hold.target != reply.target_id
    {
        return Err(transport_error(
            "selected test hold actual reply scope differs",
        ));
    }
    // Serialization/checksum computation is BEFORE the hold; no session,
    // service, DB, native journal, or metadata guard is retained at this point.
    let bytes = serde_json::to_vec(reply)?;
    let point = serde_json::json!({"request":reply.request,"source":reply.source_id,
        "target":reply.target_id,"manifest_sha256":reply.manifest_sha256,
        "reply_checksum":reply.checksum_sha256,"reply_json_sha256":hex::encode(sha2::Sha256::digest(&bytes)),
        "source_metadata_context":reply.source_metadata_context,"source_metadata_term":reply.source_metadata_term,
        "source_metadata_read_index":reply.source_metadata_read_index,
        "remaining_original_request_us":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
        "handler_reached_unix_ns":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()});
    let mut state = hold.state.lock();
    if state.reached.is_some() {
        return Err(transport_error("selected Rows hold reached more than once"));
    }
    state.reached = Some(point);
    hold.changed.notify_all();
    loop {
        if let Err(error) = parent.check() {
            state.expired = true;
            state.resumed = true;
            hold.changed.notify_all();
            return Err(error);
        }
        if state.released {
            break;
        }
        hold.changed.wait_for(&mut state, Duration::from_millis(5));
    }
    state.resumed = true;
    hold.changed.notify_all();
    Ok(())
}
