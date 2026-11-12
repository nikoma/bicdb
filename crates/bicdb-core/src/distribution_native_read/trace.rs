//! Test-only timing DATA, enabled by the focused evidence environment. No role,
//! token, proof, DB/Store handle or Operation is retained or created here.
use super::*;
use std::cell::RefCell;
use std::io::Write;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;

#[derive(Clone)]
struct Data {
    node: String,
    business: Uuid,
    challenge: Uuid,
    range: String,
    epoch: u64,
    deadline: Option<Instant>,
}
thread_local! { static CURRENT: RefCell<Option<Data>> = const { RefCell::new(None) }; }
static ANCHOR: OnceLock<Instant> = OnceLock::new();
fn stamp(deadline: Option<Instant>) -> (u128, Option<i128>, Option<u128>) {
    let anchor = *ANCHOR.get_or_init(Instant::now);
    (
        anchor.elapsed().as_micros(),
        deadline.map(|d| {
            if d >= anchor {
                d.duration_since(anchor).as_micros() as i128
            } else {
                -(anchor.duration_since(d).as_micros() as i128)
            }
        }),
        deadline.map(|d| d.saturating_duration_since(Instant::now()).as_micros()),
    )
}
impl Data {
    fn emit(&self, stage: &str, event: &str, fields: &str) {
        let (time, deadline, remaining) = stamp(self.deadline);
        let _ = writeln!(std::io::stderr(), "NATIVE_STRONG_TRACE pid={} t_us={} node={} business={} challenge={} range={} epoch={} deadline_us={:?} remaining_us={:?} stage={} event={} {}",
            std::process::id(), time, self.node, self.business, self.challenge,
            self.range, self.epoch, deadline, remaining, stage, event, fields);
    }
}
pub(crate) struct Context {
    previous: Option<Data>,
    _local: PhantomData<Rc<()>>,
}
impl Context {
    pub(crate) fn enter(
        node: &ClusterNodeId,
        business: Uuid,
        challenge: Uuid,
        range: crate::RangeId,
        epoch: u64,
        deadline: Option<Instant>,
    ) -> Self {
        let enabled = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT").is_some()
            || std::env::var_os("BICDB_TEST_STRONG_READ_TRACE").is_some();
        let data = enabled.then(|| Data {
            node: node.to_string(),
            business,
            challenge,
            range: range.to_string(),
            epoch,
            deadline,
        });
        let previous = CURRENT.with(|current| current.replace(data));
        Self {
            previous,
            _local: PhantomData,
        }
    }
}
impl Drop for Context {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}
pub(crate) struct Span {
    data: Data,
    stage: &'static str,
    start: Instant,
    _local: PhantomData<Rc<()>>,
}
impl Span {
    pub(crate) fn current(stage: &'static str) -> Option<Self> {
        CURRENT
            .with(|current| current.borrow().clone())
            .map(|data| {
                data.emit(stage, "begin", "");
                Self {
                    data,
                    stage,
                    start: Instant::now(),
                    _local: PhantomData,
                }
            })
    }
    pub(crate) fn detail(stage: &str, fields: std::fmt::Arguments<'_>) {
        let data = CURRENT.with(|current| current.borrow().clone());
        if let Some(data) = data {
            data.emit(stage, "detail", &fields.to_string());
        }
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        self.data.emit(
            self.stage,
            "end",
            &format!("elapsed_us={}", self.start.elapsed().as_micros()),
        );
    }
}
pub(crate) fn deadline_us(deadline: Option<Instant>) -> Option<i128> {
    stamp(deadline).1
}
