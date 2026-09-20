//! Lowering bicdb's PL/pgSQL IR (`RoutineIR`/`BoundExpr`) into the whole-proc
//! WASM IR ([`ProcIr`]), the real-`SqlSession` host that runs embedded SQL, and
//! the live-path glue (per-signature compile cache, result builder). Phase 2,
//! Slice C.
//!
//! Lowering is conservative: it handles the integer pure-compute subset (slots,
//! integer/boolean expressions, assignment, `IF`, integer `FOR` loops, early
//! `RETURN`) plus embedded `SELECT INTO` and DML statements (run by the host),
//! and returns `None` for anything else (cursors, casts, non-integer literals,
//! array targets, `RETURN`-with-value, exception handlers, `QueryForLoop`). A
//! `None` means "run on the interpreter", so a partial lowering never risks
//! correctness. The activation is additionally env-gated and (when used as a
//! `SpecializedExecutor`) shadow-verified against the interpreter.
//!
//! ## Host re-entry ([`SessionHost`])
//!
//! `execute_call`, `execute_parsed_statement`, `execute_query`, and
//! `routine_vars` all live on `SqlSession`, so the host re-enters a
//! `*mut SqlSession` — the very session running the `CALL`, preserving its live
//! transaction. On each `host_sql` call the host installs the proc's current
//! integer slots as routine variables (mirroring `execute_routine_sql`'s
//! `routine_vars` swap), runs the query (`&mut` read) or statement (`&mut`
//! write), then writes any result columns back into the target slots
//! (mirroring `assign_routine_targets`). The raw pointer is sound because the
//! session outlives the synchronous `ProcModule::run`, and nothing else touches
//! it while WASM holds control.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use sqlparser::ast::{BinaryOperator, Query, Statement};

use super::ir::{CmpOp, IrExpr};
use super::procir::{ProcHost, ProcIr, ProcModule, ProcStmt};
use super::ExecutionSignature;

use crate::{
    normalize_object_name, BoundExpr, RoutineArgMode, RoutineAssignmentTarget, RoutineDecl,
    RoutineExpr, RoutineIR, RoutineStmt, SqlResult, SqlSession, SqlValue, VarId,
};

fn bx(e: IrExpr) -> Box<IrExpr> {
    Box::new(e)
}

/// An embedded SQL operation: a read query or a (DML) statement, with the slots
/// its result columns are written back into (empty for plain writes).
#[derive(Clone, Debug)]
pub(crate) enum EmbKind {
    Query(Query),
    Statement(Statement),
}

#[derive(Clone, Debug)]
pub(crate) struct EmbeddedStmt {
    pub kind: EmbKind,
    pub targets: Vec<u32>,
}

/// A lowered procedure plus the side tables the host/live-path need.
pub(crate) struct LoweredProc {
    pub proc: ProcIr,
    pub embedded: Vec<EmbeddedStmt>,
    pub slot_names: Vec<String>,
    pub output_names: Vec<String>,
}

struct LowerCtx {
    ids: HashMap<String, u32>,
    embedded: Vec<EmbeddedStmt>,
}

fn build_slots(symbol_names: &[String]) -> (HashMap<String, u32>, Vec<String>) {
    let mut ids: HashMap<String, u32> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    for name in symbol_names {
        let key = normalize_object_name(name);
        if key.is_empty() || ids.contains_key(&key) {
            continue;
        }
        ids.insert(key.clone(), names.len() as u32);
        names.push(key);
    }
    (ids, names)
}

fn slot_of(ids: &HashMap<String, u32>, name: &str) -> Option<u32> {
    ids.get(&normalize_object_name(name)).copied()
}

pub(crate) fn lower_routine(ir: &RoutineIR) -> Option<LoweredProc> {
    if !ir.exception_handlers.is_empty() {
        return None;
    }

    let (ids, slot_names) = build_slots(&ir.symbol_names);
    let num_slots = slot_names.len() as u32;
    if num_slots == 0 {
        return None;
    }

    let num_params = ir
        .params
        .iter()
        .filter(|p| matches!(p.mode, RoutineArgMode::In | RoutineArgMode::InOut))
        .count() as u32;

    let mut param_inits: Vec<(u32, u32)> = Vec::new();
    for arg in 0..num_params {
        if let Some(slot) = slot_of(&ids, &format!("${}", arg + 1)) {
            param_inits.push((slot, arg));
        }
    }
    let mut input_idx = 0u32;
    for p in &ir.params {
        match p.mode {
            RoutineArgMode::In | RoutineArgMode::InOut => {
                let arg = input_idx;
                input_idx += 1;
                if let Some(slot) = slot_of(&ids, &format!("${}", p.index + 1)) {
                    param_inits.push((slot, arg));
                }
                if let Some(name) = &p.name {
                    if let Some(slot) = slot_of(&ids, name) {
                        param_inits.push((slot, arg));
                    }
                }
            }
            RoutineArgMode::Out => {}
        }
    }

    // OUT slots + names, in new_with_symbols' output_names order/normalization.
    let mut out_slots: Vec<u32> = Vec::new();
    let mut output_names: Vec<String> = Vec::new();
    for p in &ir.params {
        if matches!(p.mode, RoutineArgMode::Out | RoutineArgMode::InOut) {
            let name = p
                .name
                .clone()
                .unwrap_or_else(|| format!("arg{}", p.index + 1));
            out_slots.push(slot_of(&ids, &name)?);
            output_names.push(normalize_object_name(&name));
        }
    }

    let mut ctx = LowerCtx {
        ids,
        embedded: Vec::new(),
    };

    let mut body: Vec<ProcStmt> = Vec::new();
    for decl in &ir.declarations {
        match decl {
            RoutineDecl::Variable {
                name,
                default_expr: Some(expr),
                ..
            } => {
                let slot = slot_of(&ctx.ids, name)?;
                body.push(ProcStmt::Assign {
                    slot,
                    expr: lower_routine_expr(expr)?,
                });
            }
            RoutineDecl::Variable {
                default_expr: None, ..
            } => {}
            RoutineDecl::Alias { .. } | RoutineDecl::Cursor { .. } => return None,
        }
    }

    lower_body(&ir.statements, &mut ctx, &mut body)?;

    Some(LoweredProc {
        proc: ProcIr {
            num_params,
            num_slots,
            param_inits,
            body,
            out_slots,
        },
        embedded: ctx.embedded,
        slot_names,
        output_names,
    })
}

/// Pure-compute convenience for tests: the `ProcIr` only.
pub(crate) fn lower_routine_ir(ir: &RoutineIR) -> Option<ProcIr> {
    lower_routine(ir).map(|l| l.proc)
}

fn lower_body(stmts: &[RoutineStmt], ctx: &mut LowerCtx, out: &mut Vec<ProcStmt>) -> Option<()> {
    for s in stmts {
        let lowered = lower_stmt(s, ctx)?;
        out.push(lowered);
    }
    Some(())
}

fn push_embedded(ctx: &mut LowerCtx, kind: EmbKind, targets: Vec<u32>) -> ProcStmt {
    let idx = ctx.embedded.len() as u32;
    ctx.embedded.push(EmbeddedStmt { kind, targets });
    ProcStmt::HostSql { stmt_idx: idx }
}

fn target_slots(ctx: &LowerCtx, targets: &[String]) -> Option<Vec<u32>> {
    targets.iter().map(|t| slot_of(&ctx.ids, t)).collect()
}

fn lower_stmt(stmt: &RoutineStmt, ctx: &mut LowerCtx) -> Option<ProcStmt> {
    match stmt {
        RoutineStmt::Assignment {
            target: RoutineAssignmentTarget::Variable(name),
            expr,
        } => Some(ProcStmt::Assign {
            slot: slot_of(&ctx.ids, name)?,
            expr: lower_routine_expr(expr)?,
        }),
        RoutineStmt::If {
            condition,
            then_body,
            else_body,
        } => {
            let cond = lower_routine_expr(condition)?;
            let mut then_out = Vec::new();
            lower_body(then_body, ctx, &mut then_out)?;
            let mut else_out = Vec::new();
            lower_body(else_body, ctx, &mut else_out)?;
            Some(ProcStmt::If {
                cond,
                then_body: then_out,
                else_body: else_out,
            })
        }
        RoutineStmt::ForLoop {
            iterator,
            lower,
            upper,
            body,
        } => {
            let slot = slot_of(&ctx.ids, iterator)?;
            let lo = lower_routine_expr(lower)?;
            let hi = lower_routine_expr(upper)?;
            let mut loop_out = Vec::new();
            lower_body(body, ctx, &mut loop_out)?;
            Some(ProcStmt::ForLoop {
                slot,
                lower: lo,
                upper: hi,
                body: loop_out,
            })
        }
        RoutineStmt::ForeachLoop { .. } => None,
        RoutineStmt::SelectInto {
            query,
            targets,
            strict,
        } => {
            if *strict {
                return None;
            }
            let tslots = target_slots(ctx, targets)?;
            Some(push_embedded(ctx, EmbKind::Query(query.clone()), tslots))
        }
        RoutineStmt::Sql(statement) => Some(push_embedded(
            ctx,
            EmbKind::Statement(statement.clone()),
            Vec::new(),
        )),
        RoutineStmt::SqlInto { statement, targets } => {
            let tslots = target_slots(ctx, targets)?;
            Some(push_embedded(
                ctx,
                EmbKind::Statement(statement.clone()),
                tslots,
            ))
        }
        RoutineStmt::Return(None) => Some(ProcStmt::Return),
        // Return-with-value, QueryForLoop, cursors, array-element assignment.
        _ => None,
    }
}

fn lower_routine_expr(expr: &RoutineExpr) -> Option<IrExpr> {
    lower_bound(expr.bound.as_ref()?)
}

fn lower_bound(be: &BoundExpr) -> Option<IrExpr> {
    match be {
        BoundExpr::Var(VarId(i)) => Some(IrExpr::Col(*i as u32)),
        BoundExpr::Literal(SqlValue::Int(v)) => Some(IrExpr::ConstInt(*v)),
        BoundExpr::Literal(SqlValue::Bool(b)) => Some(IrExpr::ConstBool(*b)),
        BoundExpr::Binary { left, op, right } => {
            let l = bx(lower_bound(left)?);
            let r = bx(lower_bound(right)?);
            match op {
                BinaryOperator::Plus => Some(IrExpr::Add(l, r)),
                BinaryOperator::Minus => Some(IrExpr::Sub(l, r)),
                BinaryOperator::Multiply => Some(IrExpr::Mul(l, r)),
                _ => None,
            }
        }
        BoundExpr::Compare { left, op, right } => {
            let l = bx(lower_bound(left)?);
            let r = bx(lower_bound(right)?);
            let cmp = match op {
                BinaryOperator::Eq => CmpOp::Eq,
                BinaryOperator::NotEq => CmpOp::Ne,
                BinaryOperator::Lt => CmpOp::Lt,
                BinaryOperator::LtEq => CmpOp::Le,
                BinaryOperator::Gt => CmpOp::Gt,
                BinaryOperator::GtEq => CmpOp::Ge,
                _ => return None,
            };
            Some(IrExpr::Cmp(cmp, l, r))
        }
        BoundExpr::And(a, b) => Some(IrExpr::And(bx(lower_bound(a)?), bx(lower_bound(b)?))),
        BoundExpr::Or(a, b) => Some(IrExpr::Or(bx(lower_bound(a)?), bx(lower_bound(b)?))),
        BoundExpr::UnaryNot(a) => Some(IrExpr::Not(bx(lower_bound(a)?))),
        BoundExpr::UnaryMinus(a) => Some(IrExpr::Neg(bx(lower_bound(a)?))),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Real-session host
// ---------------------------------------------------------------------------

/// Host that runs a lowered proc's embedded SQL on the live `SqlSession`
/// (preserving its transaction). Owns its tables to stay valid for the run;
/// reaches the session through a raw pointer.
///
/// Wasmtime requires Store data to be `'static`, so the session's borrow
/// lifetime is erased to `'static` in the stored pointer. This is sound:
/// the pointer is only dereferenced inside [`ProcHost::sql`], which only runs
/// synchronously within the `ProcModule::run` the host is passed to, during
/// which the real session is alive and not otherwise accessed.
pub(crate) struct SessionHost {
    session: *mut SqlSession<'static>,
    embedded: Vec<EmbeddedStmt>,
    slot_names: Vec<String>,
    /// Where an embedded-statement error is recorded so the caller can
    /// propagate it after the aborted run (preserving rollback semantics).
    err: *mut Option<crate::SqlError>,
}

impl SessionHost {
    /// # Safety
    /// `session` and `err` must remain valid and unaliased for the whole
    /// `ProcModule::run` this host is passed to.
    pub(crate) unsafe fn new<'db>(
        session: *mut SqlSession<'db>,
        err: *mut Option<crate::SqlError>,
        lowered: &LoweredProc,
    ) -> Self {
        SessionHost {
            session: session as *mut SqlSession<'static>,
            embedded: lowered.embedded.clone(),
            slot_names: lowered.slot_names.clone(),
            err,
        }
    }
}

impl ProcHost for SessionHost {
    fn sql(&mut self, stmt_idx: u32, slots: &mut [i64]) -> bool {
        let Some(emb) = self.embedded.get(stmt_idx as usize) else {
            return true;
        };
        let mut vars: BTreeMap<String, SqlValue> = BTreeMap::new();
        for (i, name) in self.slot_names.iter().enumerate() {
            if !name.is_empty() {
                vars.insert(name.clone(), SqlValue::Int(slots[i]));
            }
        }
        // SAFETY: the session outlives this synchronous call and is not
        // otherwise accessed while WASM holds control.
        let session = unsafe { &mut *self.session };
        let saved = std::mem::replace(&mut session.routine_vars, Arc::new(vars));
        let result = match &emb.kind {
            EmbKind::Query(q) => session.execute_query(q),
            EmbKind::Statement(s) => session.execute_parsed_statement(s),
        };
        session.routine_vars = saved;
        match result {
            Ok(res) => {
                if !emb.targets.is_empty() {
                    if let Some(row) = res.rows.first() {
                        for (col, &slot) in emb.targets.iter().enumerate() {
                            if let Some(&SqlValue::Int(v)) = row.get(col) {
                                slots[slot as usize] = v;
                            }
                        }
                    }
                }
                true
            }
            Err(e) => {
                // Record the error and abort: the proc fails and the caller
                // propagates this, rolling back exactly as the interpreter does.
                unsafe { *self.err = Some(e) };
                false
            }
        }
    }
}

/// Conservative integer-only gate from a routine's declared arg signature
/// (e.g. `"trailing_note OUT TEXT"`). Returns `true` only if every parameter's
/// declared type is an integer type — so procs with TEXT/NUMERIC/etc. params
/// (which the i64 slot model can't represent) fall back to the interpreter.
pub(crate) fn proc_is_all_integer(args: &[String]) -> bool {
    args.iter().all(|arg| {
        let ty = arg
            .split_whitespace()
            .last()
            .unwrap_or("")
            .to_ascii_lowercase();
        matches!(
            ty.as_str(),
            "int" | "integer" | "int2" | "int4" | "int8" | "smallint" | "bigint"
        )
    })
}

// ---------------------------------------------------------------------------
// Live-path glue (compile cache, arg check, result builder, env gate)
// ---------------------------------------------------------------------------

/// A compiled, live-path-ready procedure.
pub(crate) struct CompiledProc {
    pub module: ProcModule,
    pub slot_names: Vec<String>,
    pub embedded: Vec<EmbeddedStmt>,
    pub output_names: Vec<String>,
}

thread_local! {
    // signature -> compiled proc (None = tried, not lowerable; don't retry).
    static PROC_CACHE: RefCell<HashMap<u64, Option<Arc<CompiledProc>>>> =
        RefCell::new(HashMap::new());
}

/// Lower+compile `ir` for `sig`, memoized per thread. `None` if not lowerable.
pub(crate) fn get_or_build(sig: ExecutionSignature, ir: &RoutineIR) -> Option<Arc<CompiledProc>> {
    if let Some(entry) = PROC_CACHE.with(|c| c.borrow().get(&sig.0).cloned()) {
        return entry;
    }
    // The integer-only prototype has no SQL assignment casts, range checks,
    // or typmod operations. Keep its direct IR tests, but do not authorize it
    // to bypass a declared routine type on the live path.
    let needs_declared_types = ir.params.iter().any(|param| param.type_schema.is_some())
        || ir.declarations.iter().any(|decl| {
            matches!(
                decl,
                RoutineDecl::Variable {
                    pg_type: Some(_),
                    ..
                }
            )
        });
    let lowered = if needs_declared_types {
        None
    } else {
        lower_routine(ir)
    };
    let built = lowered.and_then(|l| {
        ProcModule::compile(&l.proc).ok().map(|module| {
            Arc::new(CompiledProc {
                module,
                slot_names: l.slot_names,
                embedded: l.embedded,
                output_names: l.output_names,
            })
        })
    });
    PROC_CACHE.with(|c| c.borrow_mut().insert(sig.0, built.clone()));
    built
}

/// Build a [`SessionHost`] from a cached compiled proc.
///
/// # Safety
/// `session` must remain valid and unaliased for the whole `ProcModule::run`
/// this host is passed to.
pub(crate) unsafe fn host_for<'db>(
    session: *mut SqlSession<'db>,
    err: *mut Option<crate::SqlError>,
    compiled: &CompiledProc,
) -> SessionHost {
    SessionHost {
        session: session as *mut SqlSession<'static>,
        embedded: compiled.embedded.clone(),
        slot_names: compiled.slot_names.clone(),
        err,
    }
}

/// All-integer args as `i64`, or `None` if any arg is non-integer (the proc
/// then falls back to the interpreter).
pub(crate) fn all_int_args(args: &[SqlValue]) -> Option<Vec<i64>> {
    args.iter()
        .map(|v| match v {
            SqlValue::Int(x) => Some(*x),
            _ => None,
        })
        .collect()
}

/// Build the `CALL` result from OUT-slot values, matching the interpreter.
pub(crate) fn build_result(output_names: &[String], out: &[i64]) -> SqlResult {
    if output_names.is_empty() {
        SqlResult::command("CALL")
    } else {
        let row: Vec<SqlValue> = out.iter().map(|v| SqlValue::Int(*v)).collect();
        SqlResult::new(output_names.to_vec(), vec![row])
    }
}

/// Whether WASM-authoritative procedure execution is enabled (`BICDB_AEE_WASM`).
/// Default off; the architecture is wired but only engaged when explicitly set.
pub(crate) fn wasm_procs_enabled() -> bool {
    std::env::var("BICDB_AEE_WASM")
        .map(|v| matches!(v.as_str(), "1" | "on" | "true" | "yes"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::procir::{NoHost, ProcModule};
    use super::*;
    use crate::{RoutineKind, SqlSession};
    use bicdb_core::BicDb;

    #[test]
    fn live_compilation_cannot_bypass_declared_assignment_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE PROCEDURE typed_assignment_guard() LANGUAGE plpgsql AS $$ DECLARE value SMALLINT; BEGIN value := 32768; END; $$").unwrap();
        }
        let ir = proc_ir(&db, "typed_assignment_guard");
        assert!(
            lower_routine(&ir).is_some(),
            "the prototype can lower this integer shape"
        );
        let signature = super::super::routine_signature("typed_assignment_guard", &[]);
        assert!(
            get_or_build(signature, &ir).is_none(),
            "live execution must retain the SMALLINT range check"
        );
        let error = SqlSession::new(&mut db)
            .execute("CALL typed_assignment_guard()")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22003");
    }

    fn proc_ir(db: &BicDb, name: &str) -> Arc<RoutineIR> {
        crate::resolve_routine_cached(db, RoutineKind::Procedure, name)
            .unwrap()
            .unwrap()
            .ir
    }

    fn out_ints(rows: &[Vec<SqlValue>]) -> Vec<i64> {
        rows[0]
            .iter()
            .map(|v| match v {
                SqlValue::Int(x) => *x,
                other => panic!("expected Int, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn lowers_nonloop_proc_and_matches_interpreter() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut s = SqlSession::new(&mut db);
            s.execute(
                "CREATE PROCEDURE calc(a IN INTEGER, b IN INTEGER, r OUT INTEGER) \
                 LANGUAGE 'plpgsql' AS $$ \
                 DECLARE t INTEGER DEFAULT a * 2; \
                 BEGIN IF a > b THEN r := t + b; ELSE r := t - b; END IF; END; $$",
            )
            .unwrap();
        }
        let ir = proc_ir(&db, "calc");
        let proc = lower_routine_ir(&ir).expect("calc should lower");
        let module = ProcModule::compile(&proc).expect("compile");

        for (a, b) in [(5i64, 3i64), (2, 9), (0, 0), (-4, 1), (7, 7)] {
            let interp = {
                let mut s = SqlSession::new(&mut db);
                s.execute(&format!("CALL calc({a}, {b})")).unwrap()
            };
            let want = out_ints(&interp.rows);
            let (got, _) = module.run(&[a, b], NoHost).expect("run");
            assert_eq!(got, want, "a={a} b={b}");
        }
    }

    #[test]
    fn select_into_proc_runs_via_session_host_and_matches_interpreter() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut s = SqlSession::new(&mut db);
            s.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, val INTEGER)")
                .unwrap();
            s.execute("INSERT INTO kv (k, val) VALUES (1, 100), (2, 250), (3, 999)")
                .unwrap();
            s.execute(
                "CREATE PROCEDURE lookup(id IN INTEGER, result OUT INTEGER) \
                 LANGUAGE 'plpgsql' AS $$ \
                 BEGIN SELECT val INTO result FROM kv WHERE k = id; END; $$",
            )
            .unwrap();
        }
        let ir = proc_ir(&db, "lookup");
        let lowered = lower_routine(&ir).expect("lookup should lower");
        assert_eq!(lowered.embedded.len(), 1);
        let module = ProcModule::compile(&lowered.proc).expect("compile");

        for id in [1i64, 2, 3] {
            let want = {
                let mut s = SqlSession::new(&mut db);
                out_ints(&s.execute(&format!("CALL lookup({id})")).unwrap().rows)
            };
            let got = {
                let mut s = SqlSession::new(&mut db);
                let sptr = &mut s as *mut SqlSession;
                let mut err = None;
                let host = unsafe { SessionHost::new(sptr, &mut err, &lowered) };
                module.run(&[id], host).expect("run").0
            };
            assert_eq!(got, want, "id={id}");
        }
    }

    fn read_kv(db: &mut BicDb) -> Vec<(i64, i64)> {
        let mut s = SqlSession::new(db);
        let r = s.execute("SELECT k, val FROM kv ORDER BY k").unwrap();
        r.rows
            .iter()
            .map(|row| match (&row[0], &row[1]) {
                (SqlValue::Int(k), SqlValue::Int(v)) => (*k, *v),
                _ => panic!("non-int row"),
            })
            .collect()
    }

    fn setup_write_db(dir: &std::path::Path) -> BicDb {
        let mut db = BicDb::open(dir).unwrap();
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        s.execute("INSERT INTO kv (k, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
            .unwrap();
        s.execute(
            "CREATE PROCEDURE bump(n IN INTEGER) LANGUAGE 'plpgsql' AS $$ \
             BEGIN FOR i IN 1 .. n LOOP UPDATE kv SET val = val + i WHERE k = i; END LOOP; END; $$",
        )
        .unwrap();
        drop(s);
        db
    }

    #[test]
    fn write_proc_via_session_host_matches_interpreter_db_state() {
        // Two identical DBs; run the write proc on each (interpreter vs WASM)
        // and compare resulting table state.
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let mut db_interp = setup_write_db(d1.path());
        let mut db_wasm = setup_write_db(d2.path());

        {
            let mut s = SqlSession::new(&mut db_interp);
            s.execute("CALL bump(3)").unwrap();
        }

        let ir = proc_ir(&db_wasm, "bump");
        let lowered = lower_routine(&ir).expect("bump should lower (loop + UPDATE)");
        assert_eq!(lowered.embedded.len(), 1, "one embedded UPDATE");
        let module = ProcModule::compile(&lowered.proc).expect("compile");
        {
            let mut s = SqlSession::new(&mut db_wasm);
            let sptr = &mut s as *mut SqlSession;
            let mut err = None;
            let host = unsafe { SessionHost::new(sptr, &mut err, &lowered) };
            module.run(&[3], host).expect("run");
        }

        let state_interp = read_kv(&mut db_interp);
        let state_wasm = read_kv(&mut db_wasm);
        assert_eq!(
            state_wasm, state_interp,
            "WASM write re-entry must produce identical table state"
        );
        // k=1:+1, k=2:+2, k=3:+3, k=4 untouched.
        assert_eq!(state_wasm, vec![(1, 11), (2, 22), (3, 33), (4, 40)]);
    }

    #[test]
    #[ignore]
    fn bench_proc_with_sql_interpreter_vs_wasm() {
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut s = SqlSession::new(&mut db);
            s.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, val INTEGER)")
                .unwrap();
            s.execute("INSERT INTO kv (k, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
                .unwrap();
            s.execute(
                "CREATE PROCEDURE accum(n IN INTEGER, total OUT INTEGER) \
                 LANGUAGE 'plpgsql' AS $$ \
                 DECLARE tmp INTEGER; \
                 BEGIN total := 0; \
                 FOR i IN 1 .. n LOOP SELECT val INTO tmp FROM kv WHERE k = 1; \
                 total := total + tmp; END LOOP; END; $$",
            )
            .unwrap();
        }
        let ir = proc_ir(&db, "accum");
        let lowered = lower_routine(&ir).expect("accum should lower (loop + SELECT INTO)");
        let module = ProcModule::compile(&lowered.proc).expect("compile");

        let n: i64 = 100_000; // 100k embedded point-SELECTs per call

        // Correctness.
        let want = {
            let mut s = SqlSession::new(&mut db);
            out_ints(&s.execute(&format!("CALL accum({n})")).unwrap().rows)
        };
        let got = {
            let mut s = SqlSession::new(&mut db);
            let sptr = &mut s as *mut SqlSession;
            let mut err = None;
            let host = unsafe { SessionHost::new(sptr, &mut err, &lowered) };
            module.run(&[n], host).expect("run").0
        };
        assert_eq!(got, want);

        let t = Instant::now();
        let _ = {
            let mut s = SqlSession::new(&mut db);
            s.execute(&format!("CALL accum({n})")).unwrap()
        };
        let interp = t.elapsed();

        let t = Instant::now();
        {
            let mut s = SqlSession::new(&mut db);
            let sptr = &mut s as *mut SqlSession;
            let mut err = None;
            let host = unsafe { SessionHost::new(sptr, &mut err, &lowered) };
            let _ = module.run(&[n], host).unwrap();
        }
        let wasm = t.elapsed();

        println!("--- proc with embedded SELECT: {n} iterations (loop + point-SELECT + add) ---");
        println!(
            "interpreter : {interp:?}  ({:.1} ns/iter)",
            interp.as_nanos() as f64 / n as f64
        );
        println!(
            "wasm        : {wasm:?}  ({:.1} ns/iter)",
            wasm.as_nanos() as f64 / n as f64
        );
        println!(
            "speedup     : {:.2}x  (SQL dominates; win = control-flow dispatch removed)",
            interp.as_nanos() as f64 / wasm.as_nanos() as f64
        );
        println!("result      : {} (both agree)", got[0]);
    }

    #[test]
    #[ignore]
    fn bench_control_flow_interpreter_vs_wasm() {
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut s = SqlSession::new(&mut db);
            s.execute(
                "CREATE PROCEDURE sump(n IN INTEGER, total OUT INTEGER) \
                 LANGUAGE 'plpgsql' AS $$ \
                 BEGIN total := 0; FOR i IN 1 .. n LOOP total := total + i; END LOOP; END; $$",
            )
            .unwrap();
        }
        let ir = proc_ir(&db, "sump");
        let proc = lower_routine_ir(&ir).expect("sump should lower");
        let module = ProcModule::compile(&proc).expect("compile");

        let n: i64 = 3_000_000;
        let interp_once = {
            let mut s = SqlSession::new(&mut db);
            s.execute(&format!("CALL sump({n})")).unwrap()
        };
        let want = out_ints(&interp_once.rows);
        let (got, _) = module.run(&[n], NoHost).expect("run");
        assert_eq!(got, want);

        let t = Instant::now();
        let r = {
            let mut s = SqlSession::new(&mut db);
            s.execute(&format!("CALL sump({n})")).unwrap()
        };
        let interp = t.elapsed();
        std::hint::black_box(&r);

        let _ = module.run(&[n], NoHost).unwrap();
        let t = Instant::now();
        let (w, _) = module.run(&[n], NoHost).unwrap();
        let wasm = t.elapsed();
        std::hint::black_box(&w);

        println!("--- control-flow proc: sum 1..={n} ---");
        println!(
            "interpreter : {interp:?}  ({:.2} ns/iter)",
            interp.as_nanos() as f64 / n as f64
        );
        println!(
            "wasm        : {wasm:?}  ({:.2} ns/iter)",
            wasm.as_nanos() as f64 / n as f64
        );
        println!(
            "speedup     : {:.1}x",
            interp.as_nanos() as f64 / wasm.as_nanos() as f64
        );
    }
}
