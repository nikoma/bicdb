//! Whole-procedure Execution IR + WASM backend — Phase 2, Slice C (Increment 1).
//!
//! This lifts the expression pipeline ([`super::ir`]/[`super::wasm`]) up to
//! whole stored procedures: a procedure's **control flow** (variable
//! assignment, `IF`, integer `FOR` loops, early return) compiles into a single
//! WASM function, and embedded SQL statements become **one host call each** via
//! a `host_sql` import — matching bicdb's real execution contract, where the
//! interpreter re-enters the engine once per embedded statement
//! (`execute_routine_sql`), not once per row. That coarse boundary is what the
//! Slice-B measurement showed we need: ~13 ns/call amortized over a whole SQL
//! statement's work is negligible.
//!
//! ## Model (matches `RoutineFrame`)
//!
//! * Variables are **index-based slots** (`RoutineFrame.slot_values`,
//!   `VarId(usize)`). Here every slot is an `i64` living in WASM **linear
//!   memory** at `slot * 8`, so the WASM code and the host share the exact same
//!   storage with no copy: a `host_sql` call sees the proc's current variable
//!   values and writes results straight back.
//! * IN params are the first `num_params` slots; the `run` function receives
//!   them as parameters and stores them into their slots on entry.
//! * OUT values are read from designated slots after `run` returns (mirroring
//!   bicdb collecting OUT params into the `SqlResult` row).
//!
//! ## Increment scope
//!
//! Pure-compute control flow + the host-call **mechanism** (driven by a
//! [`ProcHost`] trait). Increment 1 proves the machinery against a reference
//! interpreter ([`eval_proc`]) using a test host. Increment 2 wires the host to
//! the real `SqlEngine` (re-entrant `&mut self` via a scoped raw pointer) and
//! lowers real `RoutineIR`/`BoundExpr` into [`ProcIr`] with interpreter
//! fallback. Slots are `i64` only for now; other `SqlValue` types fall back to
//! the interpreter.

use std::fmt::Write as _;

use wasmtime::{Caller, Instance, Linker, Memory, Module, Store, Val};

use super::ir::{self, CmpOp, IrExpr};
use super::wasm::{engine, WasmError};

/// A statement in the procedure control-flow IR. Expressions reuse [`IrExpr`],
/// where `Col(i)` denotes **slot `i`** (read from linear memory at `i*8`).
#[derive(Clone, Debug)]
pub enum ProcStmt {
    /// `slot := expr` (expr result stored as `i64`).
    Assign { slot: u32, expr: IrExpr },
    /// `IF cond THEN then_body ELSE else_body`.
    If {
        cond: IrExpr,
        then_body: Vec<ProcStmt>,
        else_body: Vec<ProcStmt>,
    },
    /// `FOR slot IN lower..=upper LOOP body`. `upper` is evaluated once.
    ForLoop {
        slot: u32,
        lower: IrExpr,
        upper: IrExpr,
        body: Vec<ProcStmt>,
    },
    /// Embedded SQL statement `stmt_idx`: one host call. The host reads/writes
    /// the shared slot memory (current variable values in, result targets out).
    HostSql { stmt_idx: u32 },
    /// Early return from the procedure.
    Return,
}

/// A whole procedure compiled/interpreted over `num_slots` `i64` slots.
#[derive(Clone, Debug)]
pub struct ProcIr {
    /// Arity of the generated `run` function (number of IN/INOUT args).
    pub num_params: u32,
    pub num_slots: u32,
    /// `(slot, arg_index)` pairs: on entry, `slot` is initialized from the
    /// `arg_index`-th call argument. Real procedures bind one argument to
    /// several slots (e.g. `$1` and the named param), so this is an explicit
    /// map rather than the identity `arg i -> slot i`.
    pub param_inits: Vec<(u32, u32)>,
    pub body: Vec<ProcStmt>,
    pub out_slots: Vec<u32>,
}

/// Host interface for embedded SQL. `slots` is the procedure's full variable
/// array; the host reads the values it needs and writes results (e.g. `SELECT
/// INTO` targets) back in place. In Increment 2 this is implemented by a shim
/// that re-enters the real `SqlEngine`.
pub trait ProcHost {
    /// Run embedded statement `stmt_idx`, reading/writing `slots`. Returns
    /// `false` if the statement errored, which aborts the WASM run (so the
    /// procedure fails and its transaction rolls back, like the interpreter).
    fn sql(&mut self, stmt_idx: u32, slots: &mut [i64]) -> bool;
}

/// A [`ProcHost`] that does nothing — for pure-compute procedures.
pub struct NoHost;
impl ProcHost for NoHost {
    fn sql(&mut self, _stmt_idx: u32, _slots: &mut [i64]) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Reference interpreter (verification oracle)
// ---------------------------------------------------------------------------

enum Flow {
    Normal,
    Return,
}

fn eval_expr(expr: &IrExpr, slots: &[i64]) -> i64 {
    // Slot variables are modeled as `Col(i)`; delegate everything else to the
    // shared expression interpreter, which reads `Col(i)` from the slice.
    ir::eval_i64(expr, slots)
}

fn exec_body<H: ProcHost>(body: &[ProcStmt], slots: &mut [i64], host: &mut H) -> Flow {
    for stmt in body {
        match stmt {
            ProcStmt::Assign { slot, expr } => {
                slots[*slot as usize] = eval_expr(expr, slots);
            }
            ProcStmt::If {
                cond,
                then_body,
                else_body,
            } => {
                let branch = if eval_expr(cond, slots) != 0 {
                    then_body
                } else {
                    else_body
                };
                if let Flow::Return = exec_body(branch, slots, host) {
                    return Flow::Return;
                }
            }
            ProcStmt::ForLoop {
                slot,
                lower,
                upper,
                body,
            } => {
                let lo = eval_expr(lower, slots);
                let hi = eval_expr(upper, slots);
                slots[*slot as usize] = lo;
                while slots[*slot as usize] <= hi {
                    if let Flow::Return = exec_body(body, slots, host) {
                        return Flow::Return;
                    }
                    slots[*slot as usize] = slots[*slot as usize].wrapping_add(1);
                }
            }
            ProcStmt::HostSql { stmt_idx } => {
                host.sql(*stmt_idx, slots);
            }
            ProcStmt::Return => return Flow::Return,
        }
    }
    Flow::Normal
}

/// Reference execution of a [`ProcIr`]: the authoritative semantics the WASM
/// backend must reproduce. Returns the OUT-slot values.
pub fn eval_proc<H: ProcHost>(ir: &ProcIr, args: &[i64], host: &mut H) -> Vec<i64> {
    let mut slots = vec![0i64; ir.num_slots as usize];
    for &(slot, arg) in &ir.param_inits {
        slots[slot as usize] = args[arg as usize];
    }
    exec_body(&ir.body, &mut slots, host);
    ir.out_slots.iter().map(|s| slots[*s as usize]).collect()
}

// ---------------------------------------------------------------------------
// WASM code generation (WAT text)
// ---------------------------------------------------------------------------

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

/// Emit an expression, reading `Col(i)` as slot `i` from linear memory (`i*8`).
fn emit_expr(expr: &IrExpr, out: &mut String) {
    match expr {
        IrExpr::ConstInt(v) => {
            let _ = writeln!(out, "i64.const {v}");
        }
        IrExpr::ConstBool(b) => {
            let _ = writeln!(out, "i64.const {}", *b as i64);
        }
        IrExpr::Col(i) => {
            let _ = writeln!(out, "i32.const {}", i * 8);
            out.push_str("i64.load\n");
        }
        IrExpr::Neg(a) => {
            out.push_str("i64.const 0\n");
            emit_expr(a, out);
            out.push_str("i64.sub\n");
        }
        IrExpr::Add(a, b) => {
            emit_expr(a, out);
            emit_expr(b, out);
            out.push_str("i64.add\n");
        }
        IrExpr::Sub(a, b) => {
            emit_expr(a, out);
            emit_expr(b, out);
            out.push_str("i64.sub\n");
        }
        IrExpr::Mul(a, b) => {
            emit_expr(a, out);
            emit_expr(b, out);
            out.push_str("i64.mul\n");
        }
        IrExpr::Cmp(op, a, b) => {
            emit_expr(a, out);
            emit_expr(b, out);
            let _ = writeln!(out, "{}", cmp_instr(*op));
            out.push_str("i64.extend_i32_u\n");
        }
        IrExpr::And(a, b) => {
            emit_expr(a, out);
            emit_expr(b, out);
            out.push_str("i64.and\n");
        }
        IrExpr::Or(a, b) => {
            emit_expr(a, out);
            emit_expr(b, out);
            out.push_str("i64.or\n");
        }
        IrExpr::Not(a) => {
            emit_expr(a, out);
            out.push_str("i64.eqz\n");
            out.push_str("i64.extend_i32_u\n");
        }
    }
}

struct Codegen {
    out: String,
    /// Function param count (loop-bound locals are indexed after the params).
    num_params: u32,
    /// Number of i64 locals allocated for loop upper-bounds (one per loop).
    loop_locals: u32,
    /// Counter for unique block/loop labels.
    label_seq: u32,
}

impl Codegen {
    fn new(num_params: u32) -> Self {
        Codegen {
            out: String::new(),
            num_params,
            loop_locals: 0,
            label_seq: 0,
        }
    }

    fn store_slot(&mut self, slot: u32, value: &IrExpr) {
        // i64.store takes (addr i32, value i64).
        let _ = writeln!(self.out, "i32.const {}", slot * 8);
        emit_expr(value, &mut self.out);
        self.out.push_str("i64.store\n");
    }

    fn emit_body(&mut self, body: &[ProcStmt]) {
        for stmt in body {
            self.emit_stmt(stmt);
        }
    }

    fn emit_stmt(&mut self, stmt: &ProcStmt) {
        match stmt {
            ProcStmt::Assign { slot, expr } => self.store_slot(*slot, expr),
            ProcStmt::If {
                cond,
                then_body,
                else_body,
            } => {
                emit_expr(cond, &mut self.out);
                self.out.push_str("i32.wrap_i64\n");
                self.out.push_str("if\n");
                self.emit_body(then_body);
                self.out.push_str("else\n");
                self.emit_body(else_body);
                self.out.push_str("end\n");
            }
            ProcStmt::ForLoop {
                slot,
                lower,
                upper,
                body,
            } => {
                // Allocate a local for the once-evaluated upper bound.
                let bound_local = self.loop_locals;
                self.loop_locals += 1;
                let brk = self.label_seq;
                let cont = self.label_seq + 1;
                self.label_seq += 2;

                // slot := lower
                self.store_slot(*slot, lower);
                // local := upper
                emit_expr(upper, &mut self.out);
                let _ = writeln!(self.out, "local.set {}", self.local_index(bound_local));
                // loop
                let _ = writeln!(self.out, "block $brk{brk}");
                let _ = writeln!(self.out, "loop $cont{cont}");
                // if slot > bound: break
                let _ = writeln!(self.out, "i32.const {}", slot * 8);
                self.out.push_str("i64.load\n");
                let _ = writeln!(self.out, "local.get {}", self.local_index(bound_local));
                self.out.push_str("i64.gt_s\n");
                let _ = writeln!(self.out, "br_if $brk{brk}");
                // body
                self.emit_body(body);
                // slot := slot + 1
                let _ = writeln!(self.out, "i32.const {}", slot * 8);
                let _ = writeln!(self.out, "i32.const {}", slot * 8);
                self.out.push_str("i64.load\n");
                self.out.push_str("i64.const 1\n");
                self.out.push_str("i64.add\n");
                self.out.push_str("i64.store\n");
                let _ = writeln!(self.out, "br $cont{cont}");
                self.out.push_str("end\n"); // loop
                self.out.push_str("end\n"); // block
            }
            ProcStmt::HostSql { stmt_idx } => {
                let _ = writeln!(self.out, "i32.const {stmt_idx}");
                self.out.push_str("call $host_sql\n");
            }
            ProcStmt::Return => {
                self.out.push_str("br $proc_end\n");
            }
        }
    }

    /// Loop-bound locals come after the `num_params` function params.
    fn local_index(&self, bound_ordinal: u32) -> u32 {
        self.num_params + bound_ordinal
    }
}

/// Validate the IR (slot ranges, well-typed expressions) and generate the WAT
/// module text.
pub fn proc_to_wat(ir: &ProcIr) -> std::result::Result<String, WasmError> {
    validate(ir)?;

    let mut cg = Codegen::new(ir.num_params);
    cg.emit_body(&ir.body);

    let params: String = (0..ir.num_params).map(|_| " i64").collect();
    let locals = if cg.loop_locals > 0 {
        let mut s = String::from("    (local");
        for _ in 0..cg.loop_locals {
            s.push_str(" i64");
        }
        s.push_str(")\n");
        s
    } else {
        String::new()
    };

    // Initialize slots from the call arguments on entry.
    let mut prologue = String::new();
    for &(slot, arg) in &ir.param_inits {
        let _ = writeln!(
            prologue,
            "    i32.const {}\n    local.get {arg}\n    i64.store",
            slot * 8
        );
    }

    Ok(format!(
        "(module\n  \
         (import \"host\" \"sql\" (func $host_sql (param i32)))\n  \
         (memory (export \"mem\") 1)\n  \
         (func (export \"run\") (param{params})\n{locals}{prologue}\n    block $proc_end\n{body}    end\n  )\n)\n",
        params = params,
        locals = locals,
        prologue = prologue,
        body = cg.out,
    ))
}

fn validate(ir: &ProcIr) -> std::result::Result<(), WasmError> {
    let n = ir.num_slots;
    if ir.num_params > ir.num_slots {
        return Err(WasmError::Type(format!(
            "num_params {} > num_slots {}",
            ir.num_params, ir.num_slots
        )));
    }
    for &s in &ir.out_slots {
        if s >= n {
            return Err(WasmError::Type(format!("out slot {s} >= num_slots {n}")));
        }
    }
    for &(slot, arg) in &ir.param_inits {
        if slot >= n {
            return Err(WasmError::Type(format!(
                "param init slot {slot} >= num_slots {n}"
            )));
        }
        if arg >= ir.num_params {
            return Err(WasmError::Type(format!(
                "param init arg {arg} >= num_params {}",
                ir.num_params
            )));
        }
    }
    validate_body(&ir.body, n)
}

fn validate_body(body: &[ProcStmt], n: u32) -> std::result::Result<(), WasmError> {
    for stmt in body {
        match stmt {
            ProcStmt::Assign { slot, expr } => {
                check_slot(*slot, n)?;
                ir::type_of(expr, n).map_err(WasmError::Type)?;
            }
            ProcStmt::If {
                cond,
                then_body,
                else_body,
            } => {
                ir::type_of(cond, n).map_err(WasmError::Type)?;
                validate_body(then_body, n)?;
                validate_body(else_body, n)?;
            }
            ProcStmt::ForLoop {
                slot,
                lower,
                upper,
                body,
            } => {
                check_slot(*slot, n)?;
                ir::type_of(lower, n).map_err(WasmError::Type)?;
                ir::type_of(upper, n).map_err(WasmError::Type)?;
                validate_body(body, n)?;
            }
            ProcStmt::HostSql { .. } | ProcStmt::Return => {}
        }
    }
    Ok(())
}

fn check_slot(slot: u32, n: u32) -> std::result::Result<(), WasmError> {
    if slot >= n {
        Err(WasmError::Type(format!("slot {slot} >= num_slots {n}")))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// Store data for a running procedure module: the shared linear memory and the
/// host. Owned (so `'static`); Increment 2 replaces `host` with a shim holding
/// a scoped pointer to the real engine.
struct ProcRun<H: ProcHost> {
    memory: Option<Memory>,
    num_slots: u32,
    host: H,
}

/// A [`ProcIr`] compiled to a Wasmtime module.
pub struct ProcModule {
    module: Module,
    num_params: u32,
    num_slots: u32,
    out_slots: Vec<u32>,
}

impl ProcModule {
    pub fn compile(ir: &ProcIr) -> std::result::Result<Self, WasmError> {
        let wat = proc_to_wat(ir)?;
        let module = Module::new(engine(), wat.as_bytes())
            .map_err(|e| WasmError::Compile(format!("{e:#}\n--- WAT ---\n{wat}")))?;
        Ok(Self {
            module,
            num_params: ir.num_params,
            num_slots: ir.num_slots,
            out_slots: ir.out_slots.clone(),
        })
    }

    /// Run the procedure with integer `args`, driving embedded SQL through
    /// `host`. Returns the OUT-slot values and the (possibly mutated) host.
    ///
    /// Wasmtime requires the Store data to be `'static`, so a host that needs to
    /// borrow (e.g. the live `*mut SqlSession` re-entry) must type-erase the
    /// borrow to a raw pointer; see [`super::lower::SessionHost`].
    pub fn run<H: ProcHost + 'static>(
        &self,
        args: &[i64],
        host: H,
    ) -> std::result::Result<(Vec<i64>, H), WasmError> {
        if args.len() != self.num_params as usize {
            return Err(WasmError::Runtime(format!(
                "arg count {} != num_params {}",
                args.len(),
                self.num_params
            )));
        }
        let mut store = Store::new(
            engine(),
            ProcRun {
                memory: None,
                num_slots: self.num_slots,
                host,
            },
        );

        let mut linker = Linker::new(engine());
        linker
            .func_wrap(
                "host",
                "sql",
                |mut caller: Caller<'_, ProcRun<H>>, stmt_idx: i32| -> wasmtime::Result<()> {
                    let mem = caller
                        .data()
                        .memory
                        .ok_or_else(|| wasmtime::Error::msg("memory not set"))?;
                    let num = caller.data().num_slots as usize;
                    let mut buf = vec![0u8; num * 8];
                    mem.read(&caller, 0, &mut buf)?;
                    let mut slots: Vec<i64> = buf
                        .chunks_exact(8)
                        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                        .collect();
                    let ok = caller.data_mut().host.sql(stmt_idx as u32, &mut slots);
                    let mut back = vec![0u8; num * 8];
                    for (i, v) in slots.iter().enumerate() {
                        back[i * 8..i * 8 + 8].copy_from_slice(&v.to_le_bytes());
                    }
                    mem.write(&mut caller, 0, &back)?;
                    if !ok {
                        // Embedded statement errored: abort the run so the proc
                        // fails and rolls back. The host has recorded the error.
                        return Err(wasmtime::Error::msg("embedded sql aborted"));
                    }
                    Ok(())
                },
            )
            .map_err(|e| WasmError::Runtime(format!("link host.sql: {e:#}")))?;

        let instance: Instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| WasmError::Runtime(format!("instantiate: {e:#}")))?;
        let mem = instance
            .get_memory(&mut store, "mem")
            .ok_or_else(|| WasmError::Runtime("missing export `mem`".into()))?;
        store.data_mut().memory = Some(mem);

        let run = instance
            .get_func(&mut store, "run")
            .ok_or_else(|| WasmError::Runtime("missing export `run`".into()))?;
        let params: Vec<Val> = args.iter().map(|a| Val::I64(*a)).collect();
        run.call(&mut store, &params, &mut [])
            .map_err(|e| WasmError::Runtime(format!("call run: {e:#}")))?;

        let mut out = Vec::with_capacity(self.out_slots.len());
        for &s in &self.out_slots {
            let mut buf = [0u8; 8];
            mem.read(&store, s as usize * 8, &mut buf)
                .map_err(|e| WasmError::Runtime(format!("read out slot: {e:#}")))?;
            out.push(i64::from_le_bytes(buf));
        }
        Ok((out, store.into_data().host))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bx(e: IrExpr) -> Box<IrExpr> {
        Box::new(e)
    }

    /// FOR i IN 1..=n LOOP acc := acc + i  =>  out = sum(1..=n).
    fn sum_proc() -> ProcIr {
        // slots: 0 = n (param), 1 = acc, 2 = i
        ProcIr {
            num_params: 1,
            num_slots: 3,
            param_inits: vec![(0, 0)],
            body: vec![
                ProcStmt::Assign {
                    slot: 1,
                    expr: IrExpr::ConstInt(0),
                },
                ProcStmt::ForLoop {
                    slot: 2,
                    lower: IrExpr::ConstInt(1),
                    upper: IrExpr::Col(0),
                    body: vec![ProcStmt::Assign {
                        slot: 1,
                        expr: IrExpr::Add(bx(IrExpr::Col(1)), bx(IrExpr::Col(2))),
                    }],
                },
            ],
            out_slots: vec![1],
        }
    }

    #[test]
    fn loop_and_assignment_match_interpreter() {
        let ir = sum_proc();
        let m = ProcModule::compile(&ir).expect("compile");
        for n in [-1i64, 0, 1, 5, 100, 1000] {
            let want = eval_proc(&ir, &[n], &mut NoHost);
            let (got, _) = m.run(&[n], NoHost).expect("run");
            assert_eq!(got, want, "n={n}");
            // sum 1..=n closed form for sanity (n>=1)
            if n >= 1 {
                assert_eq!(got[0], n * (n + 1) / 2);
            } else {
                assert_eq!(got[0], 0);
            }
        }
    }

    #[test]
    fn if_and_early_return_match_interpreter() {
        // IF n < 0 THEN RETURN (out stays 0) ELSE out := n * 2
        let ir = ProcIr {
            num_params: 1,
            num_slots: 2, // 0 = n, 1 = out
            param_inits: vec![(0, 0)],
            body: vec![ProcStmt::If {
                cond: IrExpr::Cmp(CmpOp::Lt, bx(IrExpr::Col(0)), bx(IrExpr::ConstInt(0))),
                then_body: vec![ProcStmt::Return],
                else_body: vec![ProcStmt::Assign {
                    slot: 1,
                    expr: IrExpr::Mul(bx(IrExpr::Col(0)), bx(IrExpr::ConstInt(2))),
                }],
            }],
            out_slots: vec![1],
        };
        let m = ProcModule::compile(&ir).expect("compile");
        for n in [-5i64, -1, 0, 3, 21] {
            let want = eval_proc(&ir, &[n], &mut NoHost);
            let (got, _) = m.run(&[n], NoHost).expect("run");
            assert_eq!(got, want, "n={n}");
        }
    }

    struct RecordingHost {
        calls: Vec<u32>,
    }
    impl ProcHost for RecordingHost {
        fn sql(&mut self, stmt_idx: u32, slots: &mut [i64]) -> bool {
            self.calls.push(stmt_idx);
            // Deterministic effect that reads and writes shared slots.
            if !slots.is_empty() {
                slots[0] = slots[0].wrapping_mul(2).wrapping_add(stmt_idx as i64);
            }
            true
        }
    }

    #[test]
    fn host_calls_share_slot_memory_with_interpreter() {
        // out := x ; CALL sql(0) ; CALL sql(1)  (each mutates slot 0 in place)
        let ir = ProcIr {
            num_params: 1,
            num_slots: 1, // slot 0 = x / out
            param_inits: vec![(0, 0)],
            body: vec![
                ProcStmt::HostSql { stmt_idx: 0 },
                ProcStmt::HostSql { stmt_idx: 1 },
            ],
            out_slots: vec![0],
        };
        let m = ProcModule::compile(&ir).expect("compile");
        for x in [0i64, 7, -3, 1000] {
            let mut h_interp = RecordingHost { calls: vec![] };
            let want = eval_proc(&ir, &[x], &mut h_interp);
            let (got, h_wasm) = m.run(&[x], RecordingHost { calls: vec![] }).expect("run");
            assert_eq!(got, want, "x={x}");
            assert_eq!(h_wasm.calls, h_interp.calls, "host call sequence x={x}");
            // Sanity: ((x*2)+0)*2+1
            assert_eq!(got[0], (x.wrapping_mul(2)).wrapping_mul(2).wrapping_add(1));
        }
    }

    #[test]
    fn host_call_inside_loop_matches_interpreter() {
        // FOR i IN 1..=n LOOP CALL sql(7) END  (sql doubles+7 on slot 0 each time)
        let ir = ProcIr {
            num_params: 1,
            num_slots: 2, // 0 = acc/out, 1 = i
            param_inits: vec![(0, 0)],
            body: vec![ProcStmt::ForLoop {
                slot: 1,
                lower: IrExpr::ConstInt(1),
                upper: IrExpr::Col(0),
                body: vec![ProcStmt::HostSql { stmt_idx: 7 }],
            }],
            out_slots: vec![0],
        };
        let m = ProcModule::compile(&ir).expect("compile");
        for n in [0i64, 1, 3, 6] {
            let mut hi = RecordingHost { calls: vec![] };
            let want = eval_proc(&ir, &[n], &mut hi);
            let (got, hw) = m.run(&[n], RecordingHost { calls: vec![] }).expect("run");
            assert_eq!(got, want, "n={n}");
            assert_eq!(hw.calls, hi.calls, "n={n}");
        }
    }
}
