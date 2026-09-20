//! Execution IR — the relational layer + the backend contract (DRAFT v2).
//!
//! This module is the *product* of the adaptive execution engine: a stable,
//! backend-agnostic intermediate representation that the SQL planner (and, in
//! future, BicDB application / REST / prepared statements) emit, and that any number of
//! interchangeable **execution backends** consume. The optimizer never asks
//! "should I compile this to WASM?" — it asks "what is the best backend for
//! this Execution IR?" WASM is just one backend; host-interpret, host-compiled,
//! and future native backends consume the *same* IR.
//!
//! # Three layers, one boundary
//!
//! 1. **Execution IR** ([`RelOp`]/[`ScalarExpr`]) — pure data: a tree of
//!    relational operators over scalar expressions. No engine, no storage, no
//!    backend baked in.
//! 2. **Storage boundary** ([`StorageAccess`]) — the *only* place an operator
//!    touches data. **Cursor/batch from day one** (`open_*` → [`CursorId`],
//!    [`StorageAccess::next_batch`], `close`): never materialize a whole scan,
//!    always amortize the boundary. This is the interface whose granularity
//!    decides everything — the SQL-heavy proc regression (0.43×) was a boundary
//!    cost. The host backend implements it in-process against `BicDb` (live
//!    transaction/snapshot); a WASM backend's host imports delegate to the same
//!    primitives.
//! 3. **Backend strategy** ([`ExecutionBackend`]) — *how* a plan is executed
//!    (host tree-walk, host-compiled closures/bytecode, WASM, native). Every
//!    backend produces identical output for identical IR.
//!
//! Operators split cleanly into **boundary** (access paths + mutations, which
//! call [`StorageAccess`]; cost is the engine's, identical under any backend)
//! and **pure-compute** (`Filter`/`Project`/`Sort`/`Limit`/`Aggregate`/`Join`/
//! `Values`; no storage, where compiling away dispatch pays off).
//!
//! # Row identity is first-class
//!
//! Every row out of an access path carries a [`RowRef`] (`collection` +
//! `record_id`) alongside its [`SqlValue`]s ([`ExecRow`]). Mutations target a
//! `RowRef`, never a re-derived key — so `UPDATE`/`DELETE` semantics stay sound
//! as the IR grows. Computed rows (`Values`, projections, aggregates) carry no
//! `RowRef`.
//!
//! # Migration path (incremental, behind verification)
//!
//! The existing AST+`QueryPlan` executor stays the **oracle**. A new host
//! [`ExecutionBackend`] over the real [`StorageAccess`] is built *alongside* it
//! and adopted **per query shape** only once its output hashes identically to
//! the oracle (the Slice-1 gate). No big-bang executor replacement; value lands
//! slice-by-slice with stability preserved.
//!
//! # Verification (absolute)
//!
//! On any mismatch vs the oracle: invalidate, fall back, never expose a wrong
//! result.
//!
//! This file is a DRAFT contract: operator set + traits + a tiny reference
//! backend proving the IR is walkable/executable over the cursor/batch boundary
//! and the `RowRef` mutation model. It does not yet refactor the planner.

use crate::{Result, SqlError, SqlValue};

/// A client-facing output row (values only; identity stripped).
pub type Row = Vec<SqlValue>;

/// Stable identity of a stored row. `collection` is the collection/table name
/// (interning to a numeric id is a later optimization); `record_id` is the
/// engine's record id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RowRef {
    pub collection: String,
    pub record_id: String,
}

/// A row flowing through the operator tree: its values plus, for rows that came
/// from storage, the identity needed to mutate them. `row_ref` is `None` for
/// computed rows.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecRow {
    pub row_ref: Option<RowRef>,
    pub values: Vec<SqlValue>,
}

impl ExecRow {
    pub fn computed(values: Vec<SqlValue>) -> Self {
        ExecRow {
            row_ref: None,
            values,
        }
    }
    pub fn stored(row_ref: RowRef, values: Vec<SqlValue>) -> Self {
        ExecRow {
            row_ref: Some(row_ref),
            values,
        }
    }
}

/// Logical scalar types (mirrors [`SqlValue`] discriminants), for casts/typing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarType {
    Bool,
    Int,
    Float,
    Text,
    Json,
    Geometry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// A scalar expression evaluated against a single input row and a parameter
/// vector (bound query params / routine variables). The relational layer's
/// expression type; [`super::ir::IrExpr`] is its compilable subset.
#[derive(Clone, Debug)]
pub enum ScalarExpr {
    /// Value of input column `n` in the current row.
    Column(u32),
    /// Bound parameter / routine variable `n`.
    Param(u32),
    /// Constant.
    Literal(SqlValue),
    Unary {
        op: UnaryOp,
        expr: Box<ScalarExpr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    Compare {
        op: CompareOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    And(Box<ScalarExpr>, Box<ScalarExpr>),
    Or(Box<ScalarExpr>, Box<ScalarExpr>),
    Not(Box<ScalarExpr>),
    IsNull {
        expr: Box<ScalarExpr>,
        negated: bool,
    },
    Cast {
        expr: Box<ScalarExpr>,
        ty: ScalarType,
    },
    /// Escape hatch (SQL functions, etc.). Compilable backends fall back to host
    /// evaluation for these; keeps the IR core small without losing coverage.
    Function {
        name: String,
        args: Vec<ScalarExpr>,
    },
}

/// One end of an index range scan.
#[derive(Clone, Debug)]
pub enum Bound {
    Unbounded,
    Inclusive(ScalarExpr),
    Exclusive(ScalarExpr),
}

#[derive(Clone, Debug)]
pub struct SortKey {
    pub expr: ScalarExpr,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug)]
pub struct ProjectItem {
    pub expr: ScalarExpr,
    pub alias: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Debug)]
pub struct AggExpr {
    pub func: AggFunc,
    /// `None` for `COUNT(*)`.
    pub arg: Option<ScalarExpr>,
    pub distinct: bool,
    pub output_alias: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// A relational operator. Inputs are owned children, so a [`RelOp`] is a
/// self-contained plan tree.
#[derive(Clone, Debug)]
pub enum RelOp {
    // ---- Access paths (boundary: open a cursor on StorageAccess) ----
    SeqScan {
        table: String,
    },
    PrimaryKeyLookup {
        table: String,
        key: ScalarExpr,
    },
    IndexSeek {
        table: String,
        index: String,
        key: Vec<ScalarExpr>,
    },
    IndexRange {
        table: String,
        index: String,
        lower: Bound,
        upper: Bound,
        descending: bool,
    },
    Values {
        rows: Vec<Vec<ScalarExpr>>,
    },

    // ---- Pipeline operators (pure compute: backend-identical) ----
    Filter {
        input: Box<RelOp>,
        predicate: ScalarExpr,
    },
    Project {
        input: Box<RelOp>,
        columns: Vec<ProjectItem>,
    },
    Sort {
        input: Box<RelOp>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<RelOp>,
        limit: Option<u64>,
        offset: u64,
    },
    Aggregate {
        input: Box<RelOp>,
        group_by: Vec<ScalarExpr>,
        aggregates: Vec<AggExpr>,
    },
    Join {
        left: Box<RelOp>,
        right: Box<RelOp>,
        kind: JoinKind,
        on: Option<ScalarExpr>,
    },

    // ---- Mutations (boundary; target RowRef from `scan`/`input`) ----
    Insert {
        table: String,
        input: Box<RelOp>,
    },
    Update {
        table: String,
        scan: Box<RelOp>,
        assignments: Vec<(String, ScalarExpr)>,
    },
    Delete {
        table: String,
        scan: Box<RelOp>,
    },
}

/// A complete plan: the operator tree plus its output column names.
#[derive(Clone, Debug)]
pub struct RelPlan {
    pub root: RelOp,
    pub output_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Storage boundary — cursor/batch
// ---------------------------------------------------------------------------

/// Opaque handle to an open scan/seek cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CursorId(pub u64);

/// A batch of rows from a cursor. `exhausted` is set on the final batch (which
/// may itself be empty), so a drain loop is `loop { b = next_batch(..);
/// use(b.rows); if b.exhausted break }`.
#[derive(Clone, Debug)]
pub struct RowBatch {
    pub rows: Vec<ExecRow>,
    pub exhausted: bool,
}

/// The high-level, **batched** data-access boundary every backend goes through.
///
/// Coarse on purpose (one call opens an access path, `next_batch` pulls many
/// rows) so the boundary amortizes. The host backend implements this directly
/// over `BicDb`; a WASM backend's imports delegate to the same primitives.
///
/// Mutations take a [`RowRef`] obtained from an access path — never a
/// re-derived key — and `insert_row` returns the new row's identity.
pub trait StorageAccess {
    fn column_schema(&self, table: &str) -> Result<Vec<String>>;

    fn open_seq_scan(&mut self, table: &str) -> Result<CursorId>;
    fn open_primary_key_lookup(&mut self, table: &str, key: &SqlValue) -> Result<CursorId>;
    fn open_index_seek(&mut self, table: &str, index: &str, key: &[SqlValue]) -> Result<CursorId>;
    fn open_index_range(
        &mut self,
        table: &str,
        index: &str,
        lower: &Bound,
        upper: &Bound,
        descending: bool,
    ) -> Result<CursorId>;

    fn next_batch(&mut self, cursor: CursorId, max_rows: usize) -> Result<RowBatch>;
    fn close_cursor(&mut self, cursor: CursorId) -> Result<()>;

    fn insert_row(&mut self, table: &str, values: &[SqlValue]) -> Result<RowRef>;
    fn update_row(&mut self, row_ref: &RowRef, assignments: &[(String, SqlValue)]) -> Result<()>;
    fn delete_row(&mut self, row_ref: &RowRef) -> Result<()>;
}

/// Drain a cursor fully into a `Vec<ExecRow>` (closing it). Convenience for
/// backends that don't yet stream; real pipelines pull batches lazily.
pub fn drain_cursor(
    storage: &mut dyn StorageAccess,
    cursor: CursorId,
    batch_rows: usize,
) -> Result<Vec<ExecRow>> {
    let mut out = Vec::new();
    loop {
        let batch = storage.next_batch(cursor, batch_rows)?;
        out.extend(batch.rows);
        if batch.exhausted {
            break;
        }
    }
    storage.close_cursor(cursor)?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Backend contract
// ---------------------------------------------------------------------------

/// Output of executing a [`RelPlan`] (values only; identity stripped).
#[derive(Clone, Debug, PartialEq)]
pub struct ExecOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

/// An interchangeable execution backend. Every backend executes the *same*
/// [`RelPlan`] against the *same* [`StorageAccess`] and must produce identical
/// [`ExecOutput`]. The host tree-walk backend is the verification oracle;
/// others are activated per signature only after hashing identically to it.
pub trait ExecutionBackend {
    fn name(&self) -> &'static str;
    fn execute(
        &self,
        plan: &RelPlan,
        storage: &mut dyn StorageAccess,
        params: &[SqlValue],
    ) -> Result<ExecOutput>;
}

// ---------------------------------------------------------------------------
// Reference scalar evaluation + a minimal reference backend (illustrative)
// ---------------------------------------------------------------------------
//
// Intentionally small: proves the IR is walkable/executable over the
// cursor/batch boundary and the RowRef mutation model. The REAL host backend
// will reuse the engine's value semantics (`BoundExpr::eval`) rather than
// re-implement SQL three-valued logic. The integer/boolean subset below is
// enough for the tests.

const REF_BATCH_ROWS: usize = 1024;

fn as_int(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Int(x) => Some(*x),
        _ => None,
    }
}

fn truthy(v: &SqlValue) -> bool {
    matches!(v, SqlValue::Bool(true))
}

fn unsupported(what: &str) -> SqlError {
    SqlError::InvalidSql(format!(
        "reference exec backend does not implement {what} (the real host backend will)"
    ))
}

/// Evaluate a scalar expression (illustrative integer/boolean subset).
pub fn eval_scalar_ref(
    expr: &ScalarExpr,
    row: &[SqlValue],
    params: &[SqlValue],
) -> Result<SqlValue> {
    match expr {
        ScalarExpr::Column(i) => row
            .get(*i as usize)
            .cloned()
            .ok_or_else(|| unsupported("out-of-range column")),
        ScalarExpr::Param(i) => params
            .get(*i as usize)
            .cloned()
            .ok_or_else(|| unsupported("out-of-range param")),
        ScalarExpr::Literal(v) => Ok(v.clone()),
        ScalarExpr::Binary { op, left, right } => {
            let a = as_int(&eval_scalar_ref(left, row, params)?)
                .ok_or_else(|| unsupported("non-int arithmetic"))?;
            let b = as_int(&eval_scalar_ref(right, row, params)?)
                .ok_or_else(|| unsupported("non-int arithmetic"))?;
            let r = match op {
                BinaryOp::Add => a.wrapping_add(b),
                BinaryOp::Sub => a.wrapping_sub(b),
                BinaryOp::Mul => a.wrapping_mul(b),
                BinaryOp::Div => a
                    .checked_div(b)
                    .ok_or_else(|| unsupported("division by zero"))?,
                BinaryOp::Mod => a
                    .checked_rem(b)
                    .ok_or_else(|| unsupported("modulo by zero"))?,
            };
            Ok(SqlValue::Int(r))
        }
        ScalarExpr::Compare { op, left, right } => {
            let a = as_int(&eval_scalar_ref(left, row, params)?)
                .ok_or_else(|| unsupported("non-int compare"))?;
            let b = as_int(&eval_scalar_ref(right, row, params)?)
                .ok_or_else(|| unsupported("non-int compare"))?;
            let r = match op {
                CompareOp::Eq => a == b,
                CompareOp::Ne => a != b,
                CompareOp::Lt => a < b,
                CompareOp::Le => a <= b,
                CompareOp::Gt => a > b,
                CompareOp::Ge => a >= b,
            };
            Ok(SqlValue::Bool(r))
        }
        ScalarExpr::And(a, b) => Ok(SqlValue::Bool(
            truthy(&eval_scalar_ref(a, row, params)?) && truthy(&eval_scalar_ref(b, row, params)?),
        )),
        ScalarExpr::Or(a, b) => Ok(SqlValue::Bool(
            truthy(&eval_scalar_ref(a, row, params)?) || truthy(&eval_scalar_ref(b, row, params)?),
        )),
        ScalarExpr::Not(a) => Ok(SqlValue::Bool(!truthy(&eval_scalar_ref(a, row, params)?))),
        ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => {
            let a = as_int(&eval_scalar_ref(expr, row, params)?)
                .ok_or_else(|| unsupported("non-int negate"))?;
            Ok(SqlValue::Int(a.wrapping_neg()))
        }
        ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Ok(SqlValue::Bool(!truthy(&eval_scalar_ref(
            expr, row, params,
        )?))),
        ScalarExpr::IsNull { expr, negated } => {
            let value = eval_scalar_ref(expr, row, params)?;
            Ok(SqlValue::Bool(if *negated {
                crate::value_is_not_null_predicate(&value)
            } else {
                crate::value_is_null_predicate(&value)
            }))
        }
        ScalarExpr::Cast { .. } => Err(unsupported("CAST")),
        ScalarExpr::Function { name, .. } => Err(unsupported(&format!("function {name}"))),
    }
}

/// A minimal tree-walk backend: proves the IR executes over the cursor/batch
/// boundary and the `RowRef` mutation model. Seed/oracle shape for the real
/// host backend.
pub struct ReferenceBackend;

impl ReferenceBackend {
    fn eval_op(
        &self,
        op: &RelOp,
        storage: &mut dyn StorageAccess,
        params: &[SqlValue],
    ) -> Result<Vec<ExecRow>> {
        match op {
            RelOp::Values { rows } => rows
                .iter()
                .map(|exprs| {
                    let values = exprs
                        .iter()
                        .map(|e| eval_scalar_ref(e, &[], params))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ExecRow::computed(values))
                })
                .collect(),
            RelOp::SeqScan { table } => {
                let cursor = storage.open_seq_scan(table)?;
                drain_cursor(storage, cursor, REF_BATCH_ROWS)
            }
            RelOp::PrimaryKeyLookup { table, key } => {
                let k = eval_scalar_ref(key, &[], params)?;
                let cursor = storage.open_primary_key_lookup(table, &k)?;
                drain_cursor(storage, cursor, REF_BATCH_ROWS)
            }
            RelOp::IndexSeek { table, index, key } => {
                let k = key
                    .iter()
                    .map(|e| eval_scalar_ref(e, &[], params))
                    .collect::<Result<Vec<_>>>()?;
                let cursor = storage.open_index_seek(table, index, &k)?;
                drain_cursor(storage, cursor, REF_BATCH_ROWS)
            }
            RelOp::Filter { input, predicate } => {
                let rows = self.eval_op(input, storage, params)?;
                let mut out = Vec::new();
                for r in rows {
                    if truthy(&eval_scalar_ref(predicate, &r.values, params)?) {
                        out.push(r);
                    }
                }
                Ok(out)
            }
            RelOp::Project { input, columns } => {
                let rows = self.eval_op(input, storage, params)?;
                rows.iter()
                    .map(|r| {
                        let values = columns
                            .iter()
                            .map(|c| eval_scalar_ref(&c.expr, &r.values, params))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(ExecRow::computed(values))
                    })
                    .collect()
            }
            RelOp::Limit {
                input,
                limit,
                offset,
            } => {
                let rows = self.eval_op(input, storage, params)?;
                let mut it = rows.into_iter().skip(*offset as usize);
                Ok(match limit {
                    Some(n) => it.by_ref().take(*n as usize).collect(),
                    None => it.collect(),
                })
            }
            RelOp::Insert { table, input } => {
                let rows = self.eval_op(input, storage, params)?;
                for r in &rows {
                    storage.insert_row(table, &r.values)?;
                }
                Ok(Vec::new())
            }
            RelOp::Update {
                scan, assignments, ..
            } => {
                let rows = self.eval_op(scan, storage, params)?;
                for r in &rows {
                    let row_ref = r
                        .row_ref
                        .as_ref()
                        .ok_or_else(|| unsupported("UPDATE of a row without identity"))?;
                    let assigns = assignments
                        .iter()
                        .map(|(name, expr)| {
                            Ok((name.clone(), eval_scalar_ref(expr, &r.values, params)?))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    storage.update_row(row_ref, &assigns)?;
                }
                Ok(Vec::new())
            }
            RelOp::Delete { scan, .. } => {
                let rows = self.eval_op(scan, storage, params)?;
                for r in &rows {
                    let row_ref = r
                        .row_ref
                        .as_ref()
                        .ok_or_else(|| unsupported("DELETE of a row without identity"))?;
                    storage.delete_row(row_ref)?;
                }
                Ok(Vec::new())
            }
            other => Err(unsupported(&format!("operator {}", op_name(other)))),
        }
    }
}

impl ExecutionBackend for ReferenceBackend {
    fn name(&self) -> &'static str {
        "reference"
    }
    fn execute(
        &self,
        plan: &RelPlan,
        storage: &mut dyn StorageAccess,
        params: &[SqlValue],
    ) -> Result<ExecOutput> {
        let rows = self.eval_op(&plan.root, storage, params)?;
        Ok(ExecOutput {
            columns: plan.output_columns.clone(),
            rows: rows.into_iter().map(|r| r.values).collect(),
        })
    }
}

fn op_name(op: &RelOp) -> &'static str {
    match op {
        RelOp::SeqScan { .. } => "SeqScan",
        RelOp::PrimaryKeyLookup { .. } => "PrimaryKeyLookup",
        RelOp::IndexSeek { .. } => "IndexSeek",
        RelOp::IndexRange { .. } => "IndexRange",
        RelOp::Values { .. } => "Values",
        RelOp::Filter { .. } => "Filter",
        RelOp::Project { .. } => "Project",
        RelOp::Sort { .. } => "Sort",
        RelOp::Limit { .. } => "Limit",
        RelOp::Aggregate { .. } => "Aggregate",
        RelOp::Join { .. } => "Join",
        RelOp::Insert { .. } => "Insert",
        RelOp::Update { .. } => "Update",
        RelOp::Delete { .. } => "Delete",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(n: i64) -> ScalarExpr {
        ScalarExpr::Literal(SqlValue::Int(n))
    }
    fn col(i: u32) -> ScalarExpr {
        ScalarExpr::Column(i)
    }
    fn b(e: ScalarExpr) -> Box<ScalarExpr> {
        Box::new(e)
    }
    fn rref(id: &str) -> RowRef {
        RowRef {
            collection: "kv".into(),
            record_id: id.into(),
        }
    }

    /// In-memory storage that honors the cursor/batch contract (returns at most
    /// 2 rows per batch to exercise multi-batch draining) and records mutations.
    struct MockStorage {
        rows: Vec<ExecRow>,
        cursors: std::collections::HashMap<u64, usize>, // cursor -> position
        next_cursor: u64,
        updates: Vec<(RowRef, Vec<(String, SqlValue)>)>,
        deletes: Vec<RowRef>,
        inserts: Vec<Vec<SqlValue>>,
    }
    impl MockStorage {
        fn new(rows: Vec<ExecRow>) -> Self {
            MockStorage {
                rows,
                cursors: Default::default(),
                next_cursor: 1,
                updates: vec![],
                deletes: vec![],
                inserts: vec![],
            }
        }
        fn open(&mut self) -> CursorId {
            let id = self.next_cursor;
            self.next_cursor += 1;
            self.cursors.insert(id, 0);
            CursorId(id)
        }
    }
    impl StorageAccess for MockStorage {
        fn column_schema(&self, _: &str) -> Result<Vec<String>> {
            Ok(vec!["k".into(), "val".into()])
        }
        fn open_seq_scan(&mut self, _: &str) -> Result<CursorId> {
            Ok(self.open())
        }
        fn open_primary_key_lookup(&mut self, _: &str, _: &SqlValue) -> Result<CursorId> {
            Ok(self.open())
        }
        fn open_index_seek(&mut self, _: &str, _: &str, _: &[SqlValue]) -> Result<CursorId> {
            Ok(self.open())
        }
        fn open_index_range(
            &mut self,
            _: &str,
            _: &str,
            _: &Bound,
            _: &Bound,
            _: bool,
        ) -> Result<CursorId> {
            Ok(self.open())
        }
        fn next_batch(&mut self, cursor: CursorId, max_rows: usize) -> Result<RowBatch> {
            let pos = self.cursors.get(&cursor.0).copied().unwrap_or(0);
            let take = max_rows.min(2).min(self.rows.len() - pos); // cap 2/batch
            let rows = self.rows[pos..pos + take].to_vec();
            let new_pos = pos + take;
            self.cursors.insert(cursor.0, new_pos);
            Ok(RowBatch {
                rows,
                exhausted: new_pos >= self.rows.len(),
            })
        }
        fn close_cursor(&mut self, cursor: CursorId) -> Result<()> {
            self.cursors.remove(&cursor.0);
            Ok(())
        }
        fn insert_row(&mut self, _: &str, values: &[SqlValue]) -> Result<RowRef> {
            self.inserts.push(values.to_vec());
            Ok(rref(&format!("new{}", self.inserts.len())))
        }
        fn update_row(
            &mut self,
            row_ref: &RowRef,
            assignments: &[(String, SqlValue)],
        ) -> Result<()> {
            self.updates.push((row_ref.clone(), assignments.to_vec()));
            Ok(())
        }
        fn delete_row(&mut self, row_ref: &RowRef) -> Result<()> {
            self.deletes.push(row_ref.clone());
            Ok(())
        }
    }

    fn kv_rows() -> Vec<ExecRow> {
        vec![
            ExecRow::stored(rref("1"), vec![SqlValue::Int(1), SqlValue::Int(10)]),
            ExecRow::stored(rref("2"), vec![SqlValue::Int(2), SqlValue::Int(5)]),
            ExecRow::stored(rref("3"), vec![SqlValue::Int(3), SqlValue::Int(20)]),
        ]
    }

    #[test]
    fn pure_compute_plan_executes_via_backend() {
        // SELECT c1 FROM VALUES (1,10),(6,20),(8,5) WHERE c0 > 5  -> [[20],[5]]
        let plan = RelPlan {
            output_columns: vec!["c1".into()],
            root: RelOp::Project {
                columns: vec![ProjectItem {
                    expr: col(1),
                    alias: "c1".into(),
                }],
                input: Box::new(RelOp::Filter {
                    predicate: ScalarExpr::Compare {
                        op: CompareOp::Gt,
                        left: b(col(0)),
                        right: b(lit(5)),
                    },
                    input: Box::new(RelOp::Values {
                        rows: vec![
                            vec![lit(1), lit(10)],
                            vec![lit(6), lit(20)],
                            vec![lit(8), lit(5)],
                        ],
                    }),
                }),
            },
        };
        let mut storage = MockStorage::new(vec![]);
        let out = ReferenceBackend
            .execute(&plan, &mut storage, &[])
            .expect("execute");
        assert_eq!(
            out.rows,
            vec![vec![SqlValue::Int(20)], vec![SqlValue::Int(5)]]
        );
    }

    #[test]
    fn cursor_batched_scan_filter_project() {
        // SELECT k FROM kv WHERE val > 8  (3 rows, drained in 2-row batches)
        let plan = RelPlan {
            output_columns: vec!["k".into()],
            root: RelOp::Project {
                columns: vec![ProjectItem {
                    expr: col(0),
                    alias: "k".into(),
                }],
                input: Box::new(RelOp::Filter {
                    predicate: ScalarExpr::Compare {
                        op: CompareOp::Gt,
                        left: b(col(1)),
                        right: b(lit(8)),
                    },
                    input: Box::new(RelOp::SeqScan { table: "kv".into() }),
                }),
            },
        };
        let mut storage = MockStorage::new(kv_rows());
        let out = ReferenceBackend
            .execute(&plan, &mut storage, &[])
            .expect("execute");
        // val>8 -> k=1 (val10), k=3 (val20)
        assert_eq!(
            out.rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(3)]]
        );
        assert!(
            storage.cursors.is_empty(),
            "cursor must be closed after drain"
        );
    }

    #[test]
    fn update_targets_rowref_from_scan() {
        // UPDATE kv SET val = val + 1   (over all rows from a scan)
        let plan = RelPlan {
            output_columns: vec![],
            root: RelOp::Update {
                table: "kv".into(),
                assignments: vec![(
                    "val".into(),
                    ScalarExpr::Binary {
                        op: BinaryOp::Add,
                        left: b(col(1)),
                        right: b(lit(1)),
                    },
                )],
                scan: Box::new(RelOp::SeqScan { table: "kv".into() }),
            },
        };
        let mut storage = MockStorage::new(kv_rows());
        ReferenceBackend
            .execute(&plan, &mut storage, &[])
            .expect("execute");
        assert_eq!(
            storage.updates,
            vec![
                (rref("1"), vec![("val".into(), SqlValue::Int(11))]),
                (rref("2"), vec![("val".into(), SqlValue::Int(6))]),
                (rref("3"), vec![("val".into(), SqlValue::Int(21))]),
            ],
            "UPDATE must target the RowRef each row carried from the access path"
        );
    }

    /// Expressiveness: the IR represents a transactional indexed read.
    #[test]
    fn ir_can_express_indexed_select() {
        let _select = RelPlan {
            output_columns: vec!["d_next_o_id".into()],
            root: RelOp::Project {
                columns: vec![ProjectItem {
                    expr: col(2),
                    alias: "d_next_o_id".into(),
                }],
                input: Box::new(RelOp::IndexSeek {
                    table: "district".into(),
                    index: "district_pkey".into(),
                    key: vec![ScalarExpr::Param(0), ScalarExpr::Param(1)],
                }),
            },
        };
    }
}
