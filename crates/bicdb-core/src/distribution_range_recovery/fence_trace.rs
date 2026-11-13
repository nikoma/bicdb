//! Bounded passive timing DATA; never owns a token, role, proof or engine guard.
//! Runtime context enters before guards. Its deferred stderr writes run after
//! all later-declared service/DB/freeze/Operation owners have been dropped.
use super::*;
use std::cell::RefCell;
use std::io::Write;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;
const LIMIT: usize = 128;
static ANCHOR: OnceLock<Instant> = OnceLock::new();
struct Event {
    stage: &'static str,
    at: Instant,
    caller: Option<Instant>,
    role: Option<Instant>,
    effective: Option<Instant>,
}
struct Data {
    phase: &'static str,
    node: String,
    range: String,
    epoch: u64,
    nonce: Uuid,
    context: Uuid,
    challenge: Option<Uuid>,
    role_business: Option<Uuid>,
    role_context: Option<Uuid>,
    role_challenge: Option<Uuid>,
    started: Instant,
    events: Vec<Event>,
    dropped: usize,
}
thread_local! { static CURRENT: RefCell<Option<Data>> = const { RefCell::new(None) }; }
pub(crate) struct Context {
    previous: Option<Data>,
    enabled: bool,
    _local: PhantomData<Rc<()>>,
}
impl Context {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn enter(
        phase: &'static str,
        node: &ClusterNodeId,
        scope: &RecoveryFenceScope,
        challenge: Option<Uuid>,
        caller: Option<Instant>,
        role: Option<Instant>,
        effective: Option<Instant>,
    ) -> Self {
        let enabled = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT").is_some()
            || std::env::var_os("BICDB_TEST_STRONG_READ_TRACE").is_some();
        if !enabled {
            return Self {
                previous: None,
                enabled: false,
                _local: PhantomData,
            };
        }
        let started = Instant::now();
        ANCHOR.get_or_init(|| started);
        let data = Data {
            phase,
            node: node.to_string(),
            range: scope.range_id.to_string(),
            epoch: scope.range_epoch,
            nonce: scope.recovery_nonce,
            context: scope.metadata_context_id,
            challenge,
            role_business: None,
            role_context: None,
            role_challenge: None,
            started,
            events: Vec::with_capacity(LIMIT),
            dropped: 0,
        };
        let previous = CURRENT.with(|slot| slot.replace(Some(data)));
        deadlines("context-entry", caller, role, effective);
        Self {
            previous,
            enabled: true,
            _local: PhantomData,
        }
    }
}
pub(crate) fn role_ids(business: Uuid, context: Uuid, challenge: Uuid) {
    CURRENT.with(|slot| {
        if let Some(data) = slot.borrow_mut().as_mut() {
            data.role_business = Some(business);
            data.role_context = Some(context);
            data.role_challenge = Some(challenge);
        }
    });
}
pub(crate) fn mark(stage: &'static str) {
    deadlines(stage, None, None, None);
}
pub(crate) fn deadlines(
    stage: &'static str,
    caller: Option<Instant>,
    role: Option<Instant>,
    effective: Option<Instant>,
) {
    CURRENT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(data) = slot.as_mut() {
            if data.events.len() < LIMIT {
                data.events.push(Event {
                    stage,
                    at: Instant::now(),
                    caller,
                    role,
                    effective,
                });
            } else {
                data.dropped += 1;
            }
        }
    });
}
fn micros(at: Instant, anchor: Instant) -> i128 {
    match at.checked_duration_since(anchor) {
        Some(d) => d.as_micros() as i128,
        None => -(anchor.duration_since(at).as_micros() as i128),
    }
}
fn remaining(deadline: Option<Instant>, at: Instant) -> Option<i128> {
    deadline.map(|d| micros(d, at))
}
impl Drop for Context {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        // Restore first; no RefCell borrow or engine guard survives into IO.
        let data = CURRENT.with(|slot| slot.replace(self.previous.take()));
        if let Some(data) = data {
            let anchor = *ANCHOR.get_or_init(Instant::now);
            let mut output = std::io::stderr().lock();
            for e in &data.events {
                let _=writeln!(output,"NATIVE_FENCE_TRACE pid={} phase={} node={} range={} epoch={} nonce={} context={} challenge={:?} acquired_role_business={:?} acquired_role_context={:?} acquired_role_challenge={:?} t_us={} elapsed_us={} stage={} caller_deadline_us={:?} role_deadline_us={:?} effective_deadline_us={:?} caller_left_us={:?} role_left_us={:?} effective_left_us={:?} dropped={}",
                    std::process::id(),data.phase,data.node,data.range,data.epoch,data.nonce,data.context,data.challenge,data.role_business,data.role_context,data.role_challenge,
                    micros(e.at,anchor),e.at.saturating_duration_since(data.started).as_micros(),e.stage,
                    e.caller.map(|d|micros(d,anchor)),e.role.map(|d|micros(d,anchor)),e.effective.map(|d|micros(d,anchor)),
                    remaining(e.caller,e.at),remaining(e.role,e.at),remaining(e.effective,e.at),data.dropped);
            }
        }
    }
}
