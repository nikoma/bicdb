//! WASM backend — Phase 2, Slice A: compile scalar Execution IR to WebAssembly
//! and run it in-process via Wasmtime.
//!
//! This is the first end-to-end exercise of the real specialization pipeline:
//!
//! ```text
//!   IrExpr -> WAT text -> wasmtime::Module (Cranelift JIT) -> instantiate
//!          -> call -> compare against the IR interpreter (verification)
//! ```
//!
//! It deliberately targets the smallest meaningful unit (a pure scalar
//! expression) so every piece of machinery — the cached engine, code
//! generation, the call ABI, module compilation, and the verification oracle —
//! is proven correct before the bigger targets (row predicates, then whole
//! stored procedures with a host API) reuse it.
//!
//! ABI: the generated module exports `eval(param i64 × num_cols) -> i64`. Every
//! value on the stack is an `i64`; booleans are canonical `0`/`1`. This matches
//! [`super::ir::eval_i64`] exactly, so a verification mismatch is a real codegen
//! defect.
//!
//! NOTE (perf, intentionally deferred): [`WasmExpr::eval`] instantiates a fresh
//! `Store`/`Instance` per call. That is fine for correctness and for the
//! verification gate; instance reuse / pooling is a later optimization once a
//! target is shown to be worth specializing. Slice A is about *correctness of
//! the pipeline*, not yet beating the interpreter on wall-clock.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use wasmtime::{Engine, Func, Instance, Module, Store, Val};

use super::ir::{self, CmpOp, IrExpr, IrType};

/// Errors from compiling or running a specialized WASM expression.
#[derive(Debug)]
pub enum WasmError {
    /// The IR was ill-typed or referenced an out-of-range column.
    Type(String),
    /// WAT generation produced a module Wasmtime could not compile.
    Compile(String),
    /// Instantiation or invocation failed (trap, missing export, bad result).
    Runtime(String),
}

impl std::fmt::Display for WasmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasmError::Type(m) => write!(f, "ir type error: {m}"),
            WasmError::Compile(m) => write!(f, "wasm compile error: {m}"),
            WasmError::Runtime(m) => write!(f, "wasm runtime error: {m}"),
        }
    }
}

impl std::error::Error for WasmError {}

/// Process-wide Wasmtime engine (Cranelift JIT). Compiling a module is
/// expensive; the engine itself is shared and cheap to clone-by-reference.
/// Shared with [`super::procir`] so all adaptive modules use one engine.
pub(crate) fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(Engine::default)
}

fn cmp_instr(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Lt => "i64.lt_s",
        CmpOp::Le => "i64.le_s",
        CmpOp::Gt => "i64.gt_s",
        CmpOp::Ge => "i64.ge_s",
        CmpOp::Eq => "i64.eq",
        CmpOp::Ne => "i64.ne",
    }
}

/// Emit `expr` as a sequence of stack-machine WAT instructions (post-order).
fn emit(expr: &IrExpr, out: &mut String) {
    use std::fmt::Write;
    match expr {
        IrExpr::ConstInt(v) => {
            let _ = writeln!(out, "i64.const {v}");
        }
        IrExpr::ConstBool(b) => {
            let _ = writeln!(out, "i64.const {}", *b as i64);
        }
        IrExpr::Col(i) => {
            let _ = writeln!(out, "local.get {i}");
        }
        IrExpr::Neg(a) => {
            // 0 - a == -a (WASM has no i64.neg).
            out.push_str("i64.const 0\n");
            emit(a, out);
            out.push_str("i64.sub\n");
        }
        IrExpr::Add(a, b) => {
            emit(a, out);
            emit(b, out);
            out.push_str("i64.add\n");
        }
        IrExpr::Sub(a, b) => {
            emit(a, out);
            emit(b, out);
            out.push_str("i64.sub\n");
        }
        IrExpr::Mul(a, b) => {
            emit(a, out);
            emit(b, out);
            out.push_str("i64.mul\n");
        }
        IrExpr::Cmp(op, a, b) => {
            emit(a, out);
            emit(b, out);
            // Comparison yields i32; widen the 0/1 to i64 for a uniform stack.
            let _ = writeln!(out, "{}", cmp_instr(*op));
            out.push_str("i64.extend_i32_u\n");
        }
        IrExpr::And(a, b) => {
            // Operands are canonical 0/1, so bitwise i64.and == logical and.
            emit(a, out);
            emit(b, out);
            out.push_str("i64.and\n");
        }
        IrExpr::Or(a, b) => {
            emit(a, out);
            emit(b, out);
            out.push_str("i64.or\n");
        }
        IrExpr::Not(a) => {
            emit(a, out);
            out.push_str("i64.eqz\n");
            out.push_str("i64.extend_i32_u\n");
        }
    }
}

/// Generate the full WAT module text for `expr` taking `num_cols` `i64` params.
pub fn expr_to_wat(expr: &IrExpr, num_cols: u32) -> String {
    let mut body = String::new();
    emit(expr, &mut body);
    let mut params = String::new();
    if num_cols > 0 {
        params.push_str(" (param");
        for _ in 0..num_cols {
            params.push_str(" i64");
        }
        params.push(')');
    }
    format!(
        "(module\n  (func (export \"eval\"){params} (result i64)\n{body}  )\n)\n",
        params = params,
        body = body,
    )
}

/// Monotonic id for pooling compiled modules per thread.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// Per-thread pool of live `(Store, Func)` for each compiled module, keyed
    /// by [`WasmExpr::id`]. A pure expression module has no observable state, so
    /// reusing its instance across calls is sound and removes the
    /// instantiation cost that dominates [`WasmExpr::eval`]. `Store` is not
    /// `Send`/`Sync`, so the pool is thread-local rather than shared.
    static POOL: RefCell<HashMap<u64, (Store<()>, Func)>> = RefCell::new(HashMap::new());

    /// Best-case pool: statically-typed 3-arg functions, avoiding the per-call
    /// `Vec<Val>` marshalling of the untyped path. Used to measure WASM's
    /// lowest achievable per-call boundary cost.
    static POOL3: RefCell<HashMap<u64, (Store<()>, wasmtime::TypedFunc<(i64, i64, i64), i64>)>> =
        RefCell::new(HashMap::new());
}

/// A scalar [`IrExpr`] compiled to a Wasmtime module.
pub struct WasmExpr {
    module: Module,
    num_cols: u32,
    result_type: IrType,
    id: u64,
}

impl WasmExpr {
    /// Type-check, generate WAT, and JIT-compile `expr`. Fails before producing
    /// a module if the IR is ill-typed.
    pub fn compile(expr: &IrExpr, num_cols: u32) -> std::result::Result<Self, WasmError> {
        let result_type = ir::type_of(expr, num_cols).map_err(WasmError::Type)?;
        let wat = expr_to_wat(expr, num_cols);
        let module = Module::new(engine(), wat.as_bytes())
            .map_err(|e| WasmError::Compile(format!("{e:#}")))?;
        Ok(Self {
            module,
            num_cols,
            result_type,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// Logical result type (how to interpret the returned `i64`).
    pub fn result_type(&self) -> IrType {
        self.result_type
    }

    /// Evaluate the compiled expression on one row of `i64` inputs.
    pub fn eval(&self, row: &[i64]) -> std::result::Result<i64, WasmError> {
        if row.len() != self.num_cols as usize {
            return Err(WasmError::Runtime(format!(
                "row arity {} != module arity {}",
                row.len(),
                self.num_cols
            )));
        }
        let mut store = Store::new(engine(), ());
        let instance = Instance::new(&mut store, &self.module, &[])
            .map_err(|e| WasmError::Runtime(format!("instantiate: {e:#}")))?;
        let func = instance
            .get_func(&mut store, "eval")
            .ok_or_else(|| WasmError::Runtime("missing export `eval`".into()))?;
        let params: Vec<Val> = row.iter().map(|v| Val::I64(*v)).collect();
        let mut results = [Val::I64(0)];
        func.call(&mut store, &params, &mut results)
            .map_err(|e| WasmError::Runtime(format!("call: {e:#}")))?;
        match results[0] {
            Val::I64(v) => Ok(v),
            other => Err(WasmError::Runtime(format!("unexpected result {other:?}"))),
        }
    }

    /// Evaluate using a pooled, per-thread `Store`/`Instance` that is created
    /// once and reused across calls. This is the path a real hot loop would
    /// use; it removes the per-call instantiation cost of [`eval`]. The
    /// remaining per-call cost is the call trampoline + argument marshalling —
    /// exactly the boundary overhead we need to measure.
    pub fn eval_pooled(&self, row: &[i64]) -> std::result::Result<i64, WasmError> {
        if row.len() != self.num_cols as usize {
            return Err(WasmError::Runtime(format!(
                "row arity {} != module arity {}",
                row.len(),
                self.num_cols
            )));
        }
        POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if !pool.contains_key(&self.id) {
                let mut store = Store::new(engine(), ());
                let instance = Instance::new(&mut store, &self.module, &[])
                    .map_err(|e| WasmError::Runtime(format!("instantiate: {e:#}")))?;
                let func = instance
                    .get_func(&mut store, "eval")
                    .ok_or_else(|| WasmError::Runtime("missing export `eval`".into()))?;
                pool.insert(self.id, (store, func));
            }
            let (store, func) = pool.get_mut(&self.id).unwrap();
            let params: Vec<Val> = row.iter().map(|v| Val::I64(*v)).collect();
            let mut results = [Val::I64(0)];
            func.call(&mut *store, &params, &mut results)
                .map_err(|e| WasmError::Runtime(format!("call: {e:#}")))?;
            match results[0] {
                Val::I64(v) => Ok(v),
                other => Err(WasmError::Runtime(format!("unexpected result {other:?}"))),
            }
        })
    }

    /// Best-case pooled evaluation for exactly-3-column modules using a
    /// statically-typed function handle (no per-call `Vec<Val>`). This measures
    /// the lowest per-call boundary cost WASM can achieve; it is intentionally
    /// arity-specialized and exists for the boundary-cost benchmark.
    pub fn eval3_typed_pooled(
        &self,
        a: i64,
        b: i64,
        c: i64,
    ) -> std::result::Result<i64, WasmError> {
        if self.num_cols != 3 {
            return Err(WasmError::Runtime(format!(
                "eval3 requires arity 3, module is {}",
                self.num_cols
            )));
        }
        POOL3.with(|pool| {
            let mut pool = pool.borrow_mut();
            if !pool.contains_key(&self.id) {
                let mut store = Store::new(engine(), ());
                let instance = Instance::new(&mut store, &self.module, &[])
                    .map_err(|e| WasmError::Runtime(format!("instantiate: {e:#}")))?;
                let tf = instance
                    .get_typed_func::<(i64, i64, i64), i64>(&mut store, "eval")
                    .map_err(|e| WasmError::Runtime(format!("typed func: {e:#}")))?;
                pool.insert(self.id, (store, tf));
            }
            let (store, tf) = pool.get_mut(&self.id).unwrap();
            tf.call(&mut *store, (a, b, c))
                .map_err(|e| WasmError::Runtime(format!("call: {e:#}")))
        })
    }
}

/// Verify the compiled WASM reproduces the IR interpreter on every supplied
/// row. Returns `Ok(true)` only if all rows match. This is the per-expression
/// analogue of the routine-level verification gate in [`super`]: a specialized
/// expression is trustworthy only once it is proven equivalent to the
/// authoritative interpreter.
pub fn verify_against_interp(
    expr: &IrExpr,
    num_cols: u32,
    rows: &[Vec<i64>],
) -> std::result::Result<bool, WasmError> {
    let compiled = WasmExpr::compile(expr, num_cols)?;
    for row in rows {
        let expected = ir::eval_i64(expr, row);
        let got = compiled.eval(row)?;
        if expected != got {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(e: IrExpr) -> Box<IrExpr> {
        Box::new(e)
    }

    /// A deterministic spread of input rows for `num_cols` columns, drawn from a
    /// fixed value set including negatives, zero, and large magnitudes.
    fn sample_rows(num_cols: u32) -> Vec<Vec<i64>> {
        let vals = [-100i64, -3, -1, 0, 1, 2, 7, 50, 1_000_000];
        let mut rows = Vec::new();
        match num_cols {
            0 => rows.push(vec![]),
            1 => {
                for &a in &vals {
                    rows.push(vec![a]);
                }
            }
            _ => {
                // Pairwise grid over the first two columns; remaining columns
                // cycle through the value set so they vary too.
                for (i, &a) in vals.iter().enumerate() {
                    for &c in &vals {
                        let mut row = vec![a, c];
                        for k in 2..num_cols as usize {
                            row.push(vals[(i + k) % vals.len()]);
                        }
                        rows.push(row);
                    }
                }
            }
        }
        rows
    }

    #[test]
    fn constant_expression_round_trips() {
        // (2 + 3) * 4 == 20, no inputs.
        let e = IrExpr::Mul(
            b(IrExpr::Add(b(IrExpr::ConstInt(2)), b(IrExpr::ConstInt(3)))),
            b(IrExpr::ConstInt(4)),
        );
        let w = WasmExpr::compile(&e, 0).expect("compile");
        assert_eq!(w.eval(&[]).expect("eval"), 20);
        assert_eq!(w.result_type(), IrType::Int);
    }

    #[test]
    fn arithmetic_matches_interpreter() {
        // (col0 + col1) * col2 - (-col0)
        let e = IrExpr::Sub(
            b(IrExpr::Mul(
                b(IrExpr::Add(b(IrExpr::Col(0)), b(IrExpr::Col(1)))),
                b(IrExpr::Col(2)),
            )),
            b(IrExpr::Neg(b(IrExpr::Col(0)))),
        );
        assert!(verify_against_interp(&e, 3, &sample_rows(3)).expect("verify"));
    }

    #[test]
    fn predicate_matches_interpreter() {
        // (col0 > 0) AND (col1 <= col0) OR NOT(col1 == 0)
        let e = IrExpr::Or(
            b(IrExpr::And(
                b(IrExpr::Cmp(
                    CmpOp::Gt,
                    b(IrExpr::Col(0)),
                    b(IrExpr::ConstInt(0)),
                )),
                b(IrExpr::Cmp(CmpOp::Le, b(IrExpr::Col(1)), b(IrExpr::Col(0)))),
            )),
            b(IrExpr::Not(b(IrExpr::Cmp(
                CmpOp::Eq,
                b(IrExpr::Col(1)),
                b(IrExpr::ConstInt(0)),
            )))),
        );
        let w = WasmExpr::compile(&e, 2).expect("compile");
        assert_eq!(w.result_type(), IrType::Bool);
        assert!(verify_against_interp(&e, 2, &sample_rows(2)).expect("verify"));
    }

    #[test]
    fn wrapping_arithmetic_matches_interpreter() {
        // col0 * col0 can overflow i64 -> both interp and WASM must wrap equally.
        let e = IrExpr::Mul(b(IrExpr::Col(0)), b(IrExpr::Col(0)));
        let rows = vec![vec![i64::MAX], vec![i64::MIN], vec![3_037_000_500]];
        assert!(verify_against_interp(&e, 1, &rows).expect("verify"));
    }

    #[test]
    fn pooled_eval_matches_cold_eval() {
        let e = IrExpr::Add(
            b(IrExpr::Mul(b(IrExpr::Col(0)), b(IrExpr::ConstInt(3)))),
            b(IrExpr::Col(1)),
        );
        let w = WasmExpr::compile(&e, 2).expect("compile");
        for row in sample_rows(2) {
            let cold = w.eval(&row).expect("cold");
            let pooled = w.eval_pooled(&row).expect("pooled");
            assert_eq!(cold, pooled);
            assert_eq!(pooled, ir::eval_i64(&e, &row));
        }
    }

    #[test]
    fn ill_typed_expression_is_rejected_before_compile() {
        // AND over an integer operand is ill-typed.
        let e = IrExpr::And(b(IrExpr::Col(0)), b(IrExpr::ConstBool(true)));
        assert!(matches!(WasmExpr::compile(&e, 1), Err(WasmError::Type(_))));
        // Column out of range is also rejected.
        let e2 = IrExpr::Col(5);
        assert!(matches!(WasmExpr::compile(&e2, 1), Err(WasmError::Type(_))));
    }
}
