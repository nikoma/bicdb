//! Test-only timing DATA. No cancellation token, role, proof, mutex or native
//! reader is retained. Contexts are thread-local/!Send and restore on unwind.
use super::*;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;

#[derive(Clone)]
struct ProbeData {
    phase: &'static str,
    node: String,
    challenge: Uuid,
    nonce: Uuid,
    range: String,
    epoch: u64,
    configuration: String,
    deadline: Option<Instant>,
}
thread_local! { static CURRENT: RefCell<Option<ProbeData>> = const { RefCell::new(None) }; }
static ANCHOR: OnceLock<Instant> = OnceLock::new();
fn timestamp_us() -> u128 {
    ANCHOR.get_or_init(Instant::now).elapsed().as_micros()
}
fn deadline_us(deadline: Option<Instant>) -> Option<i128> {
    let anchor = *ANCHOR.get_or_init(Instant::now);
    deadline.map(|deadline| match deadline.checked_duration_since(anchor) {
        Some(delta) => delta.as_micros() as i128,
        None => -(anchor.duration_since(deadline).as_micros() as i128),
    })
}
fn remaining(deadline: Option<Instant>) -> Option<u128> {
    deadline.map(|d| d.saturating_duration_since(Instant::now()).as_millis())
}
impl ProbeData {
    fn new(
        phase: &'static str,
        node: &ClusterNodeId,
        scope: &RecoveryFenceScope,
        challenge: Uuid,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            phase,
            node: node.to_string(),
            challenge,
            nonce: scope.recovery_nonce,
            range: scope.range_id.to_string(),
            epoch: scope.range_epoch,
            configuration: scope.range_configuration_sha256.clone(),
            deadline,
        }
    }
    fn emit(&self, stage: &str, event: &str, fields: &str) {
        eprintln!("NATIVE_PROBE_TRACE pid={} t_us={} phase={} node={} challenge={} session={} range={} epoch={} configuration={} remaining_ms={:?} deadline_us={:?} stage={} event={} {}",
            std::process::id(),timestamp_us(),self.phase,self.node,self.challenge,self.nonce,self.range,self.epoch,self.configuration,remaining(self.deadline),deadline_us(self.deadline),stage,event,fields);
    }
}
pub(crate) struct NativeProbeTraceContext {
    previous: Option<ProbeData>,
    _local: PhantomData<Rc<()>>,
}
impl NativeProbeTraceContext {
    pub(crate) fn enter(
        phase: &'static str,
        node: &ClusterNodeId,
        scope: &RecoveryFenceScope,
        challenge: Uuid,
        deadline: Option<Instant>,
    ) -> Self {
        let data = ProbeData::new(phase, node, scope, challenge, deadline);
        let previous = CURRENT.with(|current| current.replace(Some(data)));
        Self {
            previous,
            _local: PhantomData,
        }
    }
}
impl Drop for NativeProbeTraceContext {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}
pub(crate) struct NativeProbeTraceSpan {
    data: ProbeData,
    stage: &'static str,
    started: Instant,
    completed: bool,
    _local: PhantomData<Rc<()>>,
}
impl NativeProbeTraceSpan {
    pub(crate) fn deadline_us(deadline: Option<Instant>) -> Option<i128> {
        deadline_us(deadline)
    }
    pub(crate) fn new(
        phase: &'static str,
        stage: &'static str,
        node: &ClusterNodeId,
        scope: &RecoveryFenceScope,
        challenge: Uuid,
        deadline: Option<Instant>,
    ) -> Self {
        Self::from_data(
            ProbeData::new(phase, node, scope, challenge, deadline),
            stage,
        )
    }
    fn from_data(data: ProbeData, stage: &'static str) -> Self {
        let started = Instant::now();
        data.emit(stage, "begin", "");
        Self {
            data,
            stage,
            started,
            completed: false,
            _local: PhantomData,
        }
    }
    pub(crate) fn current(stage: &'static str) -> Option<Self> {
        // Release the RefCell borrow before logging or any nested call.
        let data = CURRENT.with(|current| current.borrow().clone());
        data.map(|data| Self::from_data(data, stage))
    }
    pub(crate) fn finish(span: Option<Self>) {
        if let Some(mut span) = span {
            span.complete();
        }
    }
    pub(crate) fn complete(&mut self) {
        self.completed = true;
    }
    pub(crate) fn detail(&self, fields: impl AsRef<str>) {
        self.data.emit(self.stage, "detail", fields.as_ref());
    }
    pub(crate) fn event(
        phase: &'static str,
        stage: &str,
        node: &ClusterNodeId,
        scope: &RecoveryFenceScope,
        challenge: Uuid,
        deadline: Option<Instant>,
        fields: impl AsRef<str>,
    ) {
        ProbeData::new(phase, node, scope, challenge, deadline).emit(
            stage,
            "event",
            fields.as_ref(),
        );
    }
}
impl Drop for NativeProbeTraceSpan {
    fn drop(&mut self) {
        self.data.emit(
            self.stage,
            "end",
            &format!(
                "elapsed_us={} completed={}",
                self.started.elapsed().as_micros(),
                self.completed
            ),
        );
    }
}
