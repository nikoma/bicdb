//! Optional host diagnostics. These bytes are never protocol/read/write authority.
use super::*;
use std::error::Error;
use std::io;
use std::sync::atomic::AtomicU8;

/// Exact evidence from the existing client path. Frame bytes include the four
/// byte length header. Payloads may contain application data; keep journals private.
#[derive(Debug, Serialize)]
pub struct ClusterRpcEvidence {
    pub attempt_id: Uuid,
    /// Reservation order across this observer; sink completion order may differ.
    pub journal_sequence: u64,
    pub phase_unix_ns: u128,
    pub elapsed_ns: u128,
    pub phase: &'static str,
    pub operation: &'static str,
    pub caller: String,
    pub destination: String,
    pub topology_generation: Option<u64>,
    pub address: Option<String>,
    pub request_id: Option<u64>,
    pub raw_frame: Option<Vec<u8>>,
    pub bytes_written: usize,
    pub may_have_dispatched: bool,
    pub error_chain: Vec<String>,
}
/// Called outside engine latches. Success must mean the complete record is
/// durable. Calls can run concurrently: sinks must atomically persist each full
/// record, bound their IO, and must not reenter this observed client. Sequence
/// fields preserve reservation order even if callback completion is reordered.
/// A failed callback fences subsequent observed dispatch; it cannot supply replies.
pub trait ClusterRpcEvidenceObserver: Send + Sync {
    fn record(&self, evidence: &ClusterRpcEvidence) -> io::Result<()>;
}
#[derive(Clone, Copy, Debug)]
pub struct ClusterRpcEvidenceLimits {
    pub max_records: u64,
    pub max_bytes: u64,
}
impl Default for ClusterRpcEvidenceLimits {
    fn default() -> Self {
        Self {
            max_records: 4096,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}
pub(super) struct Observer {
    sink: Arc<dyn ClusterRpcEvidenceObserver>,
    limits: ClusterRpcEvidenceLimits,
    counts: Mutex<(u64, u64)>,
    failed: AtomicU8,
}
impl Observer {
    pub(super) fn new(
        sink: Arc<dyn ClusterRpcEvidenceObserver>,
        limits: ClusterRpcEvidenceLimits,
    ) -> Result<Arc<Self>> {
        if limits.max_records == 0
            || limits.max_records > 65_536
            || limits.max_bytes == 0
            || limits.max_bytes > 1024 * 1024 * 1024
        {
            return Err(transport_error(
                "RPC evidence limits must be positive, at most65536 records/1GiB",
            ));
        }
        Ok(Arc::new(Self {
            sink,
            limits,
            counts: Mutex::new((0, 0)),
            failed: AtomicU8::new(0),
        }))
    }
    pub(super) fn require_current(&self) -> Result<()> {
        let failed = self.failed.load(Ordering::Acquire);
        if failed != 0 {
            return Err(BicDbError::RpcEvidenceFailure { may_have_dispatched: failed == 2, detail: "evidence observer failed or reached its bound; future observed dispatch is fenced".into() });
        }
        Ok(())
    }
    pub(super) fn require_call_entry(&self) -> Result<()> {
        self.require_current()?;
        let key = Arc::as_ptr(&self.sink) as *const () as usize;
        let parent_dispatched = ACTIVE_SINKS.with(|active| active.borrow().get(&key).copied());
        if let Some(dispatched) = parent_dispatched {
            return Err(self.fail(
                dispatched,
                "same-thread evidence sink reentry before worker dispatch",
            ));
        }
        Ok(())
    }
    fn fail(&self, dispatched: bool, detail: impl std::fmt::Display) -> BicDbError {
        // An uncertainty discovered concurrently must never be downgraded.
        self.failed
            .fetch_max(if dispatched { 2 } else { 1 }, Ordering::AcqRel);
        BicDbError::RpcEvidenceFailure {
            may_have_dispatched: self.failed.load(Ordering::Acquire) == 2,
            detail: detail.to_string(),
        }
    }
    fn record(
        &self,
        record: &mut ClusterRpcEvidence,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<()> {
        self.require_current()
            .map_err(|error| self.fail(record.may_have_dispatched, error))?;
        let key = Arc::as_ptr(&self.sink) as *const () as usize;
        let _callback = CallbackScope::enter(key, record.may_have_dispatched).ok_or_else(|| {
            self.fail(
                record.may_have_dispatched,
                "same-thread evidence sink reentry; observed dispatch is fenced",
            )
        })?;
        // Serialize only into a size counter BEFORE reserving: no sink/IO is
        // called while counts is held. Charge the largest possible sequence
        // field, so accounting remains conservative after assigning the real ID.
        record.journal_sequence = self.limits.max_records;
        let mut counter = CountBytes {
            bytes: 0,
            limit: self.limits.max_bytes,
        };
        serde_json::to_writer(&mut counter, &*record)
            .map_err(|error| self.fail(record.may_have_dispatched, error))?;
        let bytes = counter
            .bytes
            .checked_add(1)
            .ok_or_else(|| self.fail(record.may_have_dispatched, "evidence byte overflow"))?;
        let mut waited = false;
        let mut counts = loop {
            if let Some(counts) = self.counts.try_lock_for(Duration::from_millis(1)) {
                if waited {
                    cancellation
                        .check()
                        .map_err(|error| self.fail(record.may_have_dispatched, error))?;
                    if Instant::now() >= deadline {
                        return Err(self.fail(
                            record.may_have_dispatched,
                            "original RPC deadline expired reserving evidence quota",
                        ));
                    }
                }
                break counts;
            }
            waited = true;
            // This is reservation serialization, not callback IO. Waiting
            // cannot outlive the SAME original caller/network window.
            cancellation
                .check()
                .map_err(|error| self.fail(record.may_have_dispatched, error))?;
            if Instant::now() >= deadline {
                return Err(self.fail(
                    record.may_have_dispatched,
                    "original RPC deadline expired reserving evidence quota",
                ));
            }
            self.require_current()
                .map_err(|error| self.fail(record.may_have_dispatched, error))?;
        };
        self.require_current()
            .map_err(|error| self.fail(record.may_have_dispatched, error))?;
        if counts.0 >= self.limits.max_records
            || bytes > self.limits.max_bytes.saturating_sub(counts.1)
        {
            return Err(self.fail(
                record.may_have_dispatched,
                "complete evidence record exceeds quota; no truncation",
            ));
        }
        record.journal_sequence = counts.0 + 1;
        counts.0 += 1;
        counts.1 += bytes;
        drop(counts);
        // Legitimate parallel callbacks are allowed. A sink is Send+Sync and
        // owns atomic record writes/ordering; this component holds NO mutex
        // over sink IO. Only actual same-thread reentry is prohibited above.
        let mut in_flight = FenceOnUnwind {
            observer: self,
            dispatched: record.may_have_dispatched,
            armed: true,
        };
        self.sink
            .record(record)
            .map_err(|error| self.fail(record.may_have_dispatched, error))?;
        in_flight.armed = false;
        self.require_current()
            .map_err(|error| self.fail(record.may_have_dispatched, error))
    }
}
std::thread_local! {
    static ACTIVE_SINKS: std::cell::RefCell<BTreeMap<usize, bool>> = std::cell::RefCell::new(BTreeMap::new());
}
struct CallbackScope {
    key: usize,
}
impl CallbackScope {
    fn enter(key: usize, dispatched: bool) -> Option<Self> {
        ACTIVE_SINKS.with(|active| {
            let mut active = active.borrow_mut();
            if let std::collections::btree_map::Entry::Vacant(entry) = active.entry(key) {
                entry.insert(dispatched);
                Some(Self { key })
            } else {
                None
            }
        })
    }
}
impl Drop for CallbackScope {
    fn drop(&mut self) {
        ACTIVE_SINKS.with(|active| {
            active.borrow_mut().remove(&self.key);
        });
    }
}
struct FenceOnUnwind<'a> {
    observer: &'a Observer,
    dispatched: bool,
    armed: bool,
}
impl Drop for FenceOnUnwind<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.observer
                .failed
                .fetch_max(if self.dispatched { 2 } else { 1 }, Ordering::AcqRel);
        }
    }
}
struct CountBytes {
    bytes: u64,
    limit: u64,
}
impl Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("evidence size overflow"))?;
        if next > self.limit {
            return Err(io::Error::other(
                "complete evidence record exceeds byte quota",
            ));
        }
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) struct Attempt {
    observer: Arc<Observer>,
    record: ClusterRpcEvidence,
    request_frame: Vec<u8>,
    started: Instant,
    cancellation: CancellationToken,
    deadline: Instant,
}
impl Attempt {
    pub(super) fn begin(
        observer: Arc<Observer>,
        caller: &ClusterNodeId,
        destination: &ClusterNodeId,
        request: &ClusterDataRequest,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self> {
        observer.require_current()?;
        let operation = match request {
            ClusterDataRequest::ApplicationEvent(request) => match &request.operation {
                crate::ClusterEventOperation::Append { .. } => "event_append",
                crate::ClusterEventOperation::Get { .. } => "event_get",
                crate::ClusterEventOperation::Scan { .. } => "event_scan",
            },
            ClusterDataRequest::NativeEventGet(_) => "native_event_get",
            ClusterDataRequest::FetchBootstrapSnapshot => "metadata_snapshot_discovery",
            ClusterDataRequest::FetchTopology => "topology_discovery",
            ClusterDataRequest::Ping => "ping",
            _ => "other_rpc",
        };
        let mut result = Self {
            observer,
            record: ClusterRpcEvidence {
                attempt_id: Uuid::now_v7(),
                journal_sequence: 0,
                phase_unix_ns: 0,
                elapsed_ns: 0,
                phase: "invoked",
                operation,
                caller: caller.to_string(),
                destination: destination.to_string(),
                topology_generation: None,
                address: None,
                request_id: None,
                raw_frame: None,
                bytes_written: 0,
                may_have_dispatched: false,
                error_chain: Vec::new(),
            },
            request_frame: Vec::new(),
            started: Instant::now(),
            cancellation: cancellation.clone(),
            deadline,
        };
        result.emit()?;
        Ok(result)
    }
    fn emit(&mut self) -> Result<()> {
        self.record.phase_unix_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| self.observer.fail(self.record.may_have_dispatched, error))?
            .as_nanos();
        self.record.elapsed_ns = self.started.elapsed().as_nanos();
        self.observer
            .record(&mut self.record, &self.cancellation, self.deadline)
    }
    pub(super) fn dispatch(
        &mut self,
        envelope: &ClusterDataRequestEnvelope,
        generation: u64,
        address: &str,
        max: usize,
    ) -> Result<()> {
        // Even a framing refusal retains the REAL allocated ID/route. No
        // frame is invented when encoding or the existing bound rejects it.
        self.record.topology_generation = Some(generation);
        self.record.address = Some(address.to_owned());
        self.record.request_id = Some(envelope.request_id);
        let body = serde_json::to_vec(envelope)?;
        if body.is_empty() || body.len() > max || body.len() > u32::MAX as usize {
            return Err(transport_error(format!(
                "cluster RPC frame size {} exceeds configured bound {}",
                body.len(),
                max
            )));
        }
        self.request_frame = Vec::with_capacity(body.len() + 4);
        self.request_frame
            .extend_from_slice(&(body.len() as u32).to_be_bytes());
        self.request_frame.extend_from_slice(&body);
        self.record.phase = "request_frame_before_dispatch";
        self.record.raw_frame = Some(self.request_frame.clone());
        self.emit()?;
        self.record.raw_frame = None;
        Ok(())
    }
    pub(super) fn send(&mut self, stream: &mut impl Write) -> Result<()> {
        self.observer.require_current()?;
        while self.record.bytes_written < self.request_frame.len() {
            self.record.may_have_dispatched = true;
            match stream.write(&self.request_frame[self.record.bytes_written..]) {
                Ok(0) => {
                    return Err(
                        io::Error::new(io::ErrorKind::WriteZero, "failed RPC frame write").into(),
                    )
                }
                Ok(n) => self.record.bytes_written += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        stream.flush()?;
        Ok(())
    }
    pub(super) fn receive(
        &mut self,
        stream: &mut impl Read,
        max: usize,
    ) -> Result<ClusterDataResponseEnvelope> {
        let mut frame = Vec::new();
        let read = (|| -> Result<()> {
            read_exact_retained(stream, &mut frame, 4)?;
            let length = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
            if length == 0 || length > max {
                return Err(transport_error(format!(
                    "cluster RPC frame size {length} exceeds configured bound {max}"
                )));
            }
            read_exact_retained(stream, &mut frame, length + 4)?;
            Ok(())
        })();
        self.record.phase = if read.is_ok() {
            "response_frame_before_decode"
        } else {
            "partial_or_rejected_response_frame"
        };
        self.record.error_chain = read.as_ref().err().map(error_chain).unwrap_or_default();
        self.record.raw_frame = Some(frame);
        self.emit()?;
        let frame = self.record.raw_frame.take().unwrap();
        self.record.error_chain.clear();
        read?;
        Ok(serde_json::from_slice(&frame[4..])?)
    }
    pub(super) fn exchange_error(&mut self, error: &BicDbError) -> Result<()> {
        self.record.phase = "exchange_error_before_cancellation_override";
        self.record.error_chain = error_chain(error);
        self.emit()?;
        self.record.error_chain.clear();
        Ok(())
    }
    pub(super) fn return_boundary_error(&mut self, error: &BicDbError) -> Result<()> {
        self.record.phase = "return_boundary_error_after_evidence";
        self.record.error_chain = error_chain(error);
        self.emit()
    }
    pub(super) fn finish<T>(&mut self, result: &Result<T>) -> Result<()> {
        self.record.phase = if result.is_ok() {
            "validated_transport_result_ok"
        } else {
            "validated_transport_result_error"
        };
        self.record.error_chain = result.as_ref().err().map(error_chain).unwrap_or_default();
        self.emit()
    }
}
fn read_exact_retained(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    target: usize,
) -> io::Result<()> {
    let mut buffer = [0u8; 8192];
    while bytes.len() < target {
        let limit = buffer.len().min(target - bytes.len());
        match reader.read(&mut buffer[..limit]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            }
            Ok(n) => bytes.extend_from_slice(&buffer[..n]),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
fn error_chain(error: &BicDbError) -> Vec<String> {
    let mut chain = vec![error.to_string()];
    let mut next = error.source();
    while let Some(error) = next {
        chain.push(error.to_string());
        next = error.source();
    }
    chain
}

#[cfg(test)]
mod tests;
