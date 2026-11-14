//! Test-only observations. No callbacks, protocol fields, readiness or grant.
//! Active operations append bounded memory only. The outer test guard emits
//! after caller-owned fixtures/guards have dropped; no per-event file/fsync.
use parking_lot::Mutex;
use serde_json::Value;
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MAX_RECORDS: usize = 4096;
const MAX_BYTES: usize = 4 * 1024 * 1024;
// Post-serialization retention cap. Selected payloads/fields are fixed test
// DATA; actual error formatting is separately bounded before string growth.
const MAX_RECORD_BYTES: usize = 32 * 1024;
const SELECTED: [&str; 4] = [
    "native_serving_observation_scope_actual_normal_serving_current_damage_refused_at_exit",
    "native_hot_recovery_finite_open_never_starts_process_worker",
    "native_hot_recovery_retained_original_store_replace_restore_revokes_held_reservation",
    "native_initial_data_actual_tls_drop_without_stop_ends_inner_owner_and_reopens_same_root",
];
#[derive(Debug, Default)]
struct Records {
    entries: Vec<Value>,
    bytes: usize,
}
#[derive(Debug)]
struct Capture {
    test: String,
    id: uuid::Uuid,
    started: Instant,
    started_unix_ns: Option<u128>,
    records: Mutex<Records>,
    closed: AtomicBool,
    dropped_busy: AtomicU64,
    dropped_bound: AtomicU64,
    dropped_closed: AtomicU64,
    serialization_errors: AtomicU64,
}
/// Weak observational handle; it cannot keep a native actor or scope alive.
#[derive(Clone, Debug)]
pub(crate) struct Handle {
    capture: Weak<Capture>,
    mode: Option<usize>,
}
thread_local! {
    static CURRENT: RefCell<Option<Handle>> = const { RefCell::new(None) };
}
pub(crate) fn current() -> Option<Handle> {
    CURRENT.with(|slot| slot.borrow().clone())
}
pub(crate) fn record(stage: &str, data: Value) {
    if let Some(handle) = current() {
        handle.record(stage, data);
    }
}
pub(crate) fn error_chain(error: &(dyn std::error::Error + 'static)) -> Value {
    const MAX_DEPTH: usize = 8;
    const MAX_TEXT_BYTES: usize = 2048;
    struct BoundedText {
        text: String,
        truncated: bool,
    }
    impl std::fmt::Write for BoundedText {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let remaining = MAX_TEXT_BYTES.saturating_sub(self.text.len());
            let mut end = remaining.min(value.len());
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            self.text.push_str(&value[..end]);
            if end != value.len() {
                self.truncated = true;
                Err(std::fmt::Error)
            } else {
                Ok(())
            }
        }
    }
    let mut entries = Vec::new();
    let mut next = Some(error);
    let mut complete = true;
    for _ in 0..MAX_DEPTH {
        let Some(actual) = next else { break };
        let mut text = BoundedText {
            text: String::with_capacity(MAX_TEXT_BYTES),
            truncated: false,
        };
        use std::fmt::Write;
        let formatted = write!(&mut text, "{actual}").is_ok();
        complete &= formatted && !text.truncated;
        let io = actual.downcast_ref::<std::io::Error>();
        entries.push(serde_json::json!({"text":text.text,"truncated":text.truncated,"format_complete":formatted,
            "io_kind":io.map(|e|format!("{:?}",e.kind())),"raw_os_error":io.and_then(|e|e.raw_os_error())}));
        next = actual.source();
    }
    let depth_limited = next.is_some();
    serde_json::json!({"entries":entries,"complete":complete && !depth_limited,
        "depth_limited":depth_limited,"max_depth":MAX_DEPTH,"max_text_bytes":MAX_TEXT_BYTES})
}
pub(crate) fn payload_sha256(value: &Value) -> Option<String> {
    use sha2::{Digest, Sha256};
    serde_json::to_vec(value)
        .ok()
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
}
impl Handle {
    pub(crate) fn deadline(&self, deadline: Option<Instant>) -> Value {
        let Some(deadline) = deadline else {
            return Value::Null;
        };
        let Some(capture) = self.capture.upgrade() else {
            return Value::Null;
        };
        let now = Instant::now();
        serde_json::json!({"from_capture_start_us":deadline.checked_duration_since(capture.started).map(|d|d.as_micros()),
            "remaining_us":deadline.checked_duration_since(now).map(|d|d.as_micros()),"expired":now>=deadline})
    }
    pub(crate) fn record(&self, stage: &str, data: Value) {
        let Some(capture) = self.capture.upgrade() else {
            return;
        };
        if capture.closed.load(Ordering::Acquire) {
            capture.dropped_closed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let entry = serde_json::json!({"stage":stage,"capture_elapsed_us":capture.started.elapsed().as_micros(),
            "lifecycle_mode":self.mode,"data":data});
        let bytes = match serde_json::to_vec(&entry) {
            Ok(bytes) => bytes.len(),
            Err(_) => {
                capture.serialization_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if bytes > MAX_RECORD_BYTES {
            capture.dropped_bound.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Some(mut records) = capture.records.try_lock() else {
            capture.dropped_busy.fetch_add(1, Ordering::Relaxed);
            return;
        };
        // A recorder may have passed the first check before the final snapshot.
        // Recheck under the SAME memory lock used by snapshot/take: no append
        // can occur after that snapshot, even when a worker returns late.
        if capture.closed.load(Ordering::Acquire) {
            capture.dropped_closed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if records.entries.len() >= MAX_RECORDS
            || records
                .bytes
                .checked_add(bytes)
                .is_none_or(|n| n > MAX_BYTES)
        {
            capture.dropped_bound.fetch_add(1, Ordering::Relaxed);
            return;
        }
        records.bytes += bytes;
        records.entries.push(entry);
    }
}
/// Declare BEFORE fixtures/guards. Drop performs no native call, wait, drain,
/// callback or file access. It snapshots memory then emits outside its lock.
pub(crate) struct CaseGuard {
    capture: Option<Arc<Capture>>,
    previous: Option<Handle>,
}
impl CaseGuard {
    pub(crate) fn selected() -> Self {
        let test = std::thread::current().name().unwrap_or("").to_owned();
        if !SELECTED.iter().any(|name| test.ends_with(name)) {
            return Self {
                capture: None,
                previous: None,
            };
        }
        let capture = Arc::new(Capture {
            test,
            id: uuid::Uuid::now_v7(),
            started: Instant::now(),
            started_unix_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|d| d.as_nanos()),
            records: Mutex::new(Records::default()),
            closed: AtomicBool::new(false),
            dropped_busy: AtomicU64::new(0),
            dropped_bound: AtomicU64::new(0),
            dropped_closed: AtomicU64::new(0),
            serialization_errors: AtomicU64::new(0),
        });
        let handle = Handle {
            capture: Arc::downgrade(&capture),
            mode: None,
        };
        let previous = CURRENT.with(|slot| slot.replace(Some(handle)));
        Self {
            capture: Some(capture),
            previous,
        }
    }
    pub(crate) fn mode(&self, mode: usize) {
        if let Some(capture) = &self.capture {
            CURRENT.with(|slot| {
                *slot.borrow_mut() = Some(Handle {
                    capture: Arc::downgrade(capture),
                    mode: Some(mode),
                });
            });
        }
    }
}
impl Drop for CaseGuard {
    fn drop(&mut self) {
        let Some(capture) = self.capture.take() else {
            return;
        };
        CURRENT.with(|slot| *slot.borrow_mut() = self.previous.take());
        capture.closed.store(true, Ordering::Release);
        let records = {
            let mut held = capture.records.lock();
            std::mem::take(&mut *held)
        };
        let busy = capture.dropped_busy.load(Ordering::Acquire);
        let bound = capture.dropped_bound.load(Ordering::Acquire);
        let closed = capture.dropped_closed.load(Ordering::Acquire);
        let serialization = capture.serialization_errors.load(Ordering::Acquire);
        let snapshot = serde_json::json!({"version":1,"test":capture.test,"capture_id":capture.id,
            "process_id":std::process::id(),"started_unix_ns":capture.started_unix_ns,
            "source":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),
            "workflow_sha_environment":std::env::var("GITHUB_SHA").ok(),"package_version":env!("CARGO_PKG_VERSION"),
            "bounds":{"records":MAX_RECORDS,"bytes":MAX_BYTES,"record_bytes":MAX_RECORD_BYTES},
            "stored_records":records.entries.len(),"stored_record_bytes":records.bytes,
            "retained_buffer_incomplete_at_snapshot":busy!=0 || bound!=0 || closed!=0 || serialization!=0,
            "global_capture_complete":false,"background_quiescence_proven":false,
            "closed_before_buffer_snapshot":true,"counters_are_snapshot_only":true,
            "dropped_busy":busy,"dropped_bound":bound,
            "dropped_closed_at_snapshot":closed,"serialization_errors":serialization,
            "gaps":["no SDK raw frames or APP internal retry trace","no extra worker join/drain; late background results may follow this snapshot","test-only memory observations are not a durable external ACK ledger","no host resource sampling or native-data reconciliation"],
            "original_assertions_and_deadlines_unchanged":true,"authority":false,"records":records.entries});
        // The records mutex and all caller-owned test fixtures are gone.
        // Output failure never changes the original test/API result.
        if let Ok(text) = serde_json::to_string(&snapshot) {
            use std::io::Write;
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "NATIVE_PASSIVE_DIAGNOSTICS {text}");
        }
    }
}
