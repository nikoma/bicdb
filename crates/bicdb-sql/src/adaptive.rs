//! Adaptive execution engine — Slice 1 scaffold (observe-only).
//!
//! This is the foundation for turning bicdb from a fixed interpreter into a
//! self-optimizing execution engine. The long-term pipeline is:
//!
//! ```text
//!   request -> ExecutionSignature -> HotnessDetector -> (Execution IR)
//!           -> background compile -> Verification (interp vs specialized)
//!           -> module cache -> specialized execution, with interpreter fallback
//! ```
//!
//! Slice 1 builds the **backend-agnostic** front of that pipeline and nothing
//! that changes behavior:
//!
//! * [`ExecutionSignature`] — a deterministic, literal-free identity for an
//!   execution shape (e.g. a stored-procedure call site).
//! * [`HotnessDetector`] — per-signature call/latency stats, recorded with
//!   **zero lock contention on the hot path** (thread-local, amortized merge
//!   into a global registry only for introspection). This matters: the server
//!   is CPU-bound, so the detector must not add per-call synchronization.
//! * The verification framework — [`SpecializedExecutor`], a generation-stamped
//!   module registry, [`Activation`] state machine, and [`results_equal`]. A
//!   registered specialized module runs in **shadow** alongside the interpreter
//!   and is only promoted to `Active` after K consecutive identical results;
//!   any mismatch disables it. Slice 1 ships **no** real backend — the
//!   interpreter always remains authoritative — but the gate is tested with a
//!   stub so Slice 2 (the first compiled backend) drops in behind it.
//!
//! The whole layer is gated behind the `BICDB_AEE` env var and is a single
//! relaxed atomic load when disabled, so the default execution path is
//! unchanged.

use std::cell::RefCell;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use rustc_hash::{FxHashMap, FxHasher};

use crate::{Result, SqlResult, SqlValue};

pub mod exec_ir;
pub mod host;
pub mod ir;
#[cfg(feature = "adaptive-procs")]
pub mod lower;
#[cfg(feature = "adaptive-procs")]
pub mod procir;
#[cfg(feature = "adaptive-procs")]
pub mod wasm;

// ---------------------------------------------------------------------------
// Enable gate (zero-cost when off)
// ---------------------------------------------------------------------------

const STATE_UNINIT: u8 = 0;
const STATE_OFF: u8 = 1;
const STATE_ON: u8 = 2;

static ENABLED: AtomicU8 = AtomicU8::new(STATE_UNINIT);

/// Whether the adaptive layer is active. Reads `BICDB_AEE` once, then is a
/// single relaxed atomic load. When this returns `false` the caller must do
/// nothing adaptive, leaving the original execution path untouched.
#[inline]
pub fn enabled() -> bool {
    match ENABLED.load(Ordering::Relaxed) {
        STATE_ON => true,
        STATE_OFF => false,
        _ => {
            let on = std::env::var("BICDB_AEE")
                .map(|v| matches!(v.as_str(), "1" | "on" | "true" | "yes"))
                .unwrap_or(false);
            ENABLED.store(if on { STATE_ON } else { STATE_OFF }, Ordering::Relaxed);
            on
        }
    }
}

// ---------------------------------------------------------------------------
// Execution signature
// ---------------------------------------------------------------------------

/// Signature kind tags. Mixed into the hash so identical bytes from different
/// request shapes never collide.
pub const KIND_PROCEDURE: u8 = 1;
pub const KIND_QUERY: u8 = 2;

/// A deterministic, literal-free identity for an execution shape.
///
/// Two calls with the same shape (same routine, same argument *types*, same
/// projection/predicate structure) share a signature; literal argument
/// *values* never participate. The hash uses [`FxHasher`], which is seedless
/// and therefore stable across threads and process runs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ExecutionSignature(pub u64);

/// Incremental builder for an [`ExecutionSignature`].
pub struct SignatureBuilder {
    hasher: FxHasher,
}

impl SignatureBuilder {
    pub fn new(kind: u8) -> Self {
        let mut hasher = FxHasher::default();
        hasher.write_u8(kind);
        Self { hasher }
    }

    pub fn str(mut self, s: &str) -> Self {
        self.hasher.write(s.as_bytes());
        self.hasher.write_u8(0xff); // length/terminator guard
        self
    }

    pub fn u8(mut self, v: u8) -> Self {
        self.hasher.write_u8(v);
        self
    }

    pub fn u64(mut self, v: u64) -> Self {
        self.hasher.write_u64(v);
        self
    }

    pub fn finish(self) -> ExecutionSignature {
        ExecutionSignature(self.hasher.finish())
    }
}

/// Cheap, allocation-free type tag for a [`SqlValue`]. Used to fold argument
/// *types* (never values) into a signature.
#[inline]
pub fn value_type_tag(v: &SqlValue) -> u8 {
    match v {
        SqlValue::Null => 0,
        SqlValue::Bool(_) => 1,
        SqlValue::Int(_) => 2,
        SqlValue::Float(_) => 3,
        SqlValue::String(_) => 4,
        SqlValue::TsQuery(_) => 7,
        SqlValue::JsonText(_) => 5,
        SqlValue::Json(_) => 5,
        SqlValue::Geometry(_) => 6,
        SqlValue::Composite(_) => 8,
    }
}

/// Signature for a stored-procedure / `CALL` site: the procedure name plus the
/// arity and argument *types*. Generation counters are deliberately excluded —
/// they belong to [`Validity`] (module invalidation), not to shape identity, so
/// hotness for a procedure persists across unrelated schema/routine bumps.
pub fn routine_signature(name: &str, args: &[SqlValue]) -> ExecutionSignature {
    let mut b = SignatureBuilder::new(KIND_PROCEDURE)
        .str(name)
        .u8(args.len() as u8);
    for a in args {
        b = b.u8(value_type_tag(a));
    }
    b.finish()
}

// ---------------------------------------------------------------------------
// Hotness detection
// ---------------------------------------------------------------------------

/// EMA smoothing factor for latency.
const EMA_ALPHA: f64 = 0.1;
/// Per-thread calls between amortized publishes to the global registry.
const FLUSH_EVERY: u32 = 256;

/// Per-signature execution statistics.
#[derive(Clone, Debug)]
pub struct ExecutionStats {
    pub calls: u64,
    pub ema_latency_ns: f64,
    pub rows: u64,
    pub label: String,
}

impl ExecutionStats {
    fn observe(&mut self, latency_ns: u64, rows: u64) {
        self.calls += 1;
        let sample = latency_ns as f64;
        if self.calls == 1 {
            self.ema_latency_ns = sample;
        } else {
            self.ema_latency_ns = EMA_ALPHA * sample + (1.0 - EMA_ALPHA) * self.ema_latency_ns;
        }
        self.rows += rows;
    }

    /// Compile score: how much total interpreter time this shape represents.
    /// Hotter (more frequent and/or more expensive) shapes score higher and are
    /// the first candidates for specialization.
    pub fn score(&self) -> f64 {
        self.calls as f64 * self.ema_latency_ns
    }
}

struct LocalHotness {
    map: FxHashMap<u64, ExecutionStats>,
    since_flush: u32,
}

thread_local! {
    static LOCAL: RefCell<LocalHotness> = RefCell::new(LocalHotness {
        map: FxHashMap::default(),
        since_flush: 0,
    });
}

// Each thread gets a stable id so the global registry can store per-thread
// contributions and the report can sum them without double-counting.
static NEXT_TID: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static TID: usize = NEXT_TID.fetch_add(1, Ordering::Relaxed);
}

/// Global registry: signature -> (thread id -> that thread's latest totals).
/// Written only on amortized flush (every `FLUSH_EVERY` calls per thread) and
/// read only by [`hotness_report`], so the hot path never touches this lock.
type GlobalRegistry = FxHashMap<u64, FxHashMap<usize, ExecutionStats>>;
static GLOBAL: OnceLock<Mutex<GlobalRegistry>> = OnceLock::new();

fn global() -> &'static Mutex<GlobalRegistry> {
    GLOBAL.get_or_init(|| Mutex::new(FxHashMap::default()))
}

fn publish(tid: usize, snapshot: Vec<(u64, ExecutionStats)>) {
    let mut g = global().lock().unwrap();
    for (sig, stats) in snapshot {
        g.entry(sig).or_default().insert(tid, stats);
    }
}

/// Record one execution of `sig`. `label` is invoked **only** the first time a
/// signature is seen on this thread (so the hot path never formats a string).
///
/// Hot-path cost: one thread-local borrow + a hashmap entry + a few integer
/// updates. The global lock is taken at most once per `FLUSH_EVERY` calls.
pub fn record(sig: ExecutionSignature, latency_ns: u64, rows: u64, label: impl FnOnce() -> String) {
    let snapshot = LOCAL.with(|l| {
        let mut l = l.borrow_mut();
        l.map
            .entry(sig.0)
            .or_insert_with(|| ExecutionStats {
                calls: 0,
                ema_latency_ns: 0.0,
                rows: 0,
                label: label(),
            })
            .observe(latency_ns, rows);
        l.since_flush += 1;
        if l.since_flush >= FLUSH_EVERY {
            l.since_flush = 0;
            Some(
                l.map
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        }
    });
    if let Some(snapshot) = snapshot {
        let tid = TID.with(|t| *t);
        publish(tid, snapshot);
    }
}

/// Publish the calling thread's current stats to the global registry. Called by
/// [`hotness_report`] so a report reflects the caller's own recent activity.
pub fn flush_current_thread() {
    let snapshot = LOCAL.with(|l| {
        let l = l.borrow();
        l.map
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect::<Vec<_>>()
    });
    if !snapshot.is_empty() {
        let tid = TID.with(|t| *t);
        publish(tid, snapshot);
    }
}

/// One row of the hotness report.
#[derive(Clone, Debug)]
pub struct SignatureReport {
    pub signature: ExecutionSignature,
    pub label: String,
    pub calls: u64,
    pub ema_latency_ns: f64,
    pub rows: u64,
    pub score: f64,
}

/// Snapshot of all observed signatures, hottest first. Aggregates per-thread
/// contributions; `ema_latency_ns` is the call-weighted mean of the threads'
/// EMAs. Cheap and intended for introspection / deciding what to specialize.
pub fn hotness_report() -> Vec<SignatureReport> {
    flush_current_thread();
    let g = global().lock().unwrap();
    let mut out: Vec<SignatureReport> = g
        .iter()
        .map(|(sig, per_thread)| {
            let mut calls = 0u64;
            let mut rows = 0u64;
            let mut ema_weighted = 0.0f64;
            let mut label = String::new();
            for stats in per_thread.values() {
                calls += stats.calls;
                rows += stats.rows;
                ema_weighted += stats.ema_latency_ns * stats.calls as f64;
                if label.is_empty() {
                    label = stats.label.clone();
                }
            }
            let ema = if calls > 0 {
                ema_weighted / calls as f64
            } else {
                0.0
            };
            SignatureReport {
                signature: ExecutionSignature(*sig),
                label,
                calls,
                ema_latency_ns: ema,
                rows,
                score: calls as f64 * ema,
            }
        })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

// ---------------------------------------------------------------------------
// Specialized-execution framework + verification gate
// ---------------------------------------------------------------------------

/// A compiled, specialized replacement for the interpreter on one signature.
/// Slice 2+ supplies real implementations (native-compiled IR, then WASM); the
/// contract is: given the call's arguments, produce a [`SqlResult`] that must
/// be byte-identical to the interpreter's.
pub trait SpecializedExecutor: Send + Sync {
    fn execute(&self, args: &[SqlValue]) -> Result<SqlResult>;
}

/// Generation stamps captured when a module was built. A module is only used
/// while every stamp still matches the engine's current generation; any
/// schema/routine change bumps a counter and silently invalidates it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Validity {
    pub schema_gen: u64,
    pub routine_gen: u64,
}

impl Validity {
    #[inline]
    pub fn is_valid(&self, schema_gen: u64, routine_gen: u64) -> bool {
        self.schema_gen == schema_gen && self.routine_gen == routine_gen
    }
}

/// Lifecycle of a specialized module for a signature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Activation {
    /// Runs alongside the interpreter; results compared, not yet trusted.
    Shadow,
    /// Verified (K consecutive matches); eligible to replace the interpreter.
    Active,
    /// A mismatch (or error) was observed; permanently demoted, interpreter only.
    Disabled,
}

struct Module {
    exec: Arc<dyn SpecializedExecutor>,
    validity: Validity,
    activation: Activation,
    matches: u32,
    mismatches: u32,
}

static REGISTRY: OnceLock<Mutex<FxHashMap<u64, Module>>> = OnceLock::new();
// Non-empty fast-check so the hot path skips the registry lock entirely until a
// module is actually registered (Slice 1: always zero).
static REG_COUNT: AtomicUsize = AtomicUsize::new(0);

fn registry() -> &'static Mutex<FxHashMap<u64, Module>> {
    REGISTRY.get_or_init(|| Mutex::new(FxHashMap::default()))
}

/// Consecutive shadow matches required before a module is promoted to `Active`.
pub const PROMOTE_AFTER: u32 = 5;

/// Register a specialized module for a signature in `Shadow` state. Intended
/// for the background compiler (and, in Slice 1, tests).
pub fn register_module(
    sig: ExecutionSignature,
    exec: Arc<dyn SpecializedExecutor>,
    validity: Validity,
) {
    let mut g = registry().lock().unwrap();
    if g.insert(
        sig.0,
        Module {
            exec,
            validity,
            activation: Activation::Shadow,
            matches: 0,
            mismatches: 0,
        },
    )
    .is_none()
    {
        REG_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

/// Current activation state of a signature's module, if any.
pub fn activation_of(sig: ExecutionSignature) -> Option<Activation> {
    if REG_COUNT.load(Ordering::Relaxed) == 0 {
        return None;
    }
    registry().lock().unwrap().get(&sig.0).map(|m| m.activation)
}

/// Run the registered module (if any, in `Shadow`, and currently valid) and
/// compare its output to the interpreter's. Promotes after [`PROMOTE_AFTER`]
/// matches; disables on the first mismatch or error.
///
/// Slice 1 keeps the interpreter authoritative regardless of outcome — this is
/// pure shadow verification. Returns the resulting [`Activation`] for the
/// signature, or `None` if nothing was registered/eligible.
pub fn verify_shadow(
    sig: ExecutionSignature,
    interp: &SqlResult,
    args: &[SqlValue],
    schema_gen: u64,
    routine_gen: u64,
) -> Option<Activation> {
    if REG_COUNT.load(Ordering::Relaxed) == 0 {
        return None;
    }
    // Take a clone of the executor under the lock, then run it unlocked so a
    // slow candidate never blocks other threads' verification.
    let exec = {
        let g = registry().lock().unwrap();
        match g.get(&sig.0) {
            Some(m)
                if m.activation == Activation::Shadow
                    && m.validity.is_valid(schema_gen, routine_gen) =>
            {
                m.exec.clone()
            }
            _ => return None,
        }
    };
    let candidate = exec.execute(args);
    let matched = matches!(&candidate, Ok(r) if results_equal(interp, r));

    let mut g = registry().lock().unwrap();
    let m = g.get_mut(&sig.0)?;
    if matched {
        m.matches += 1;
        if m.matches >= PROMOTE_AFTER {
            m.activation = Activation::Active;
        }
    } else {
        m.mismatches += 1;
        m.activation = Activation::Disabled;
    }
    Some(m.activation)
}

/// Structural equality of two results, used as the verification oracle.
/// Column names and row/column counts must match exactly; values compare with
/// [`value_eq`] (bit-exact floats, `NaN == NaN`).
pub fn results_equal(a: &SqlResult, b: &SqlResult) -> bool {
    if a.columns != b.columns || a.rows.len() != b.rows.len() {
        return false;
    }
    for (ra, rb) in a.rows.iter().zip(&b.rows) {
        if ra.len() != rb.len() {
            return false;
        }
        for (va, vb) in ra.iter().zip(rb) {
            if !value_eq(va, vb) {
                return false;
            }
        }
    }
    true
}

/// Verification-grade value equality. Floats compare by bit pattern (so a
/// faithful specialization must reproduce the exact f64), with `NaN == NaN`
/// treated as equal. All other variants use structural equality.
fn value_eq(a: &SqlValue, b: &SqlValue) -> bool {
    match (a, b) {
        (SqlValue::Float(x), SqlValue::Float(y)) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(cols: &[&str], rows: Vec<Vec<SqlValue>>) -> SqlResult {
        SqlResult {
            columns: cols.iter().map(|s| s.to_string()).collect(),
            rows,
            command_tag: None,
            column_types: Vec::new(),
            column_metadata: Vec::new(),
        }
    }

    #[test]
    fn signature_is_deterministic_and_shape_sensitive() {
        let a = routine_signature(
            "neworder",
            &[SqlValue::Int(1), SqlValue::String("x".into())],
        );
        let b = routine_signature(
            "neworder",
            &[SqlValue::Int(9), SqlValue::String("y".into())],
        );
        // Same shape, different literal values -> same signature.
        assert_eq!(a, b);

        // Different arg type -> different signature.
        let c = routine_signature(
            "neworder",
            &[SqlValue::Float(1.0), SqlValue::String("x".into())],
        );
        assert_ne!(a, c);
        // Different name -> different signature.
        let d = routine_signature("payment", &[SqlValue::Int(1), SqlValue::String("x".into())]);
        assert_ne!(a, d);
        // Different arity -> different signature.
        let e = routine_signature("neworder", &[SqlValue::Int(1)]);
        assert_ne!(a, e);
    }

    #[test]
    fn hotness_accumulates_and_ranks_by_score() {
        // Unique names so this test is isolated from others sharing the global.
        let hot = routine_signature("aee_test_hot_proc_zzz", &[SqlValue::Int(0)]);
        let cold = routine_signature("aee_test_cold_proc_zzz", &[SqlValue::Int(0)]);
        for _ in 0..50 {
            record(hot, 10_000, 3, || "proc:hot".into());
        }
        record(cold, 10_000, 1, || "proc:cold".into());

        let report = hotness_report();
        let h = report
            .iter()
            .find(|r| r.signature == hot)
            .expect("hot present");
        let c = report
            .iter()
            .find(|r| r.signature == cold)
            .expect("cold present");
        assert_eq!(h.calls, 50);
        assert_eq!(c.calls, 1);
        assert_eq!(h.rows, 150);
        assert!(h.score > c.score, "frequent shape must outscore rare one");
    }

    struct FixedExec(SqlResult);
    impl SpecializedExecutor for FixedExec {
        fn execute(&self, _args: &[SqlValue]) -> Result<SqlResult> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn shadow_module_promotes_after_consecutive_matches() {
        let sig = routine_signature("aee_test_promote_proc", &[SqlValue::Int(0)]);
        let interp = result(&["a"], vec![vec![SqlValue::Int(7)]]);
        register_module(
            sig,
            Arc::new(FixedExec(interp.clone())),
            Validity {
                schema_gen: 1,
                routine_gen: 1,
            },
        );
        let mut last = None;
        for _ in 0..PROMOTE_AFTER {
            last = verify_shadow(sig, &interp, &[SqlValue::Int(0)], 1, 1);
        }
        assert_eq!(last, Some(Activation::Active));
    }

    #[test]
    fn shadow_module_disabled_on_mismatch() {
        let sig = routine_signature("aee_test_mismatch_proc", &[SqlValue::Int(0)]);
        let interp = result(&["a"], vec![vec![SqlValue::Int(7)]]);
        let wrong = result(&["a"], vec![vec![SqlValue::Int(8)]]);
        register_module(
            sig,
            Arc::new(FixedExec(wrong)),
            Validity {
                schema_gen: 1,
                routine_gen: 1,
            },
        );
        let act = verify_shadow(sig, &interp, &[SqlValue::Int(0)], 1, 1);
        assert_eq!(act, Some(Activation::Disabled));
    }

    #[test]
    fn stale_module_is_not_consulted() {
        let sig = routine_signature("aee_test_stale_proc", &[SqlValue::Int(0)]);
        let interp = result(&["a"], vec![vec![SqlValue::Int(7)]]);
        register_module(
            sig,
            Arc::new(FixedExec(interp.clone())),
            Validity {
                schema_gen: 1,
                routine_gen: 1,
            },
        );
        // Current generation moved on -> module invalid -> shadow is skipped.
        assert_eq!(verify_shadow(sig, &interp, &[SqlValue::Int(0)], 2, 1), None);
    }

    #[test]
    fn float_equality_is_bit_exact_with_nan() {
        let a = result(&["x"], vec![vec![SqlValue::Float(f64::NAN)]]);
        let b = result(&["x"], vec![vec![SqlValue::Float(f64::NAN)]]);
        assert!(results_equal(&a, &b));
        let c = result(&["x"], vec![vec![SqlValue::Float(1.0)]]);
        let d = result(&["x"], vec![vec![SqlValue::Float(1.0 + f64::EPSILON)]]);
        assert!(!results_equal(&c, &d));
    }
}
