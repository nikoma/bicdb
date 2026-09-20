//! Host backend — a real [`StorageAccess`] + [`ExecutionBackend`] over a live
//! `bicdb_core::Transaction`, plus a best-effort SQL→Execution-IR lowering for
//! single-table `SELECT`.
//!
//! This is Slice-2's first *real* backend behind the verification gate. It is
//! deliberately built **alongside** the AST+`QueryPlan` interpreter (the oracle)
//! and is only ever adopted per query shape once [`HostBackend`] reproduces the
//! oracle byte-for-byte. Nothing here touches `execute_query`; integration is
//! wired separately.
//!
//! ## Materialization parity (the load-bearing invariant)
//!
//! A row out of an access path is materialized in **non-hidden schema column
//! order** using the *same* engine primitive the oracle's projection consumes —
//! [`crate::record_schema_column_value`] `(record, column, true)`. The oracle's
//! `SELECT *` expands to exactly the non-hidden schema columns (see
//! `FieldRef::wildcard`) and its `SqlRow` cells come from the same function, so
//! the candidate's columns/values match the oracle for the supported shapes.
//!
//! ## Scope
//!
//! * Implemented: `column_schema`, `open_seq_scan`, `open_primary_key_lookup`,
//!   cursor `next_batch`/`close_cursor`, and `insert_row`/`update_row`/
//!   `delete_row`.
//! * **Descoped (returns `Err`):** `open_index_seek` and `open_index_range`.
//!   Transactional index access on this engine lives on `BicDb`, not
//!   `Transaction`, and needs index-name + `IndexValue` resolution; the
//!   verification gate treats an `Err` as "unsupported shape → fall back to the
//!   oracle", which is always safe. `lower_select` never emits index access, so
//!   the supported `SELECT` slice is fully exercised without it.

use std::collections::HashMap;

use bicdb_core::{Record, Transaction};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, LimitClause, Query, SelectItem, SetExpr, TableFactor, UnaryOperator,
    Value,
};

use super::exec_ir::{
    BinaryOp, Bound, CompareOp, CursorId, ExecOutput, ExecRow, ExecutionBackend, ProjectItem,
    RelOp, RelPlan, RowBatch, RowRef, ScalarExpr, StorageAccess,
};
use crate::{ColumnSchema, Result, SqlError, SqlValue, TableSchema};

// `BinaryOp` is the IR arithmetic op; `BinaryOperator` is sqlparser's — they
// collide by name only.

fn unsupported(what: impl Into<String>) -> SqlError {
    SqlError::Unsupported(what.into())
}

// ---------------------------------------------------------------------------
// BicDbStorage — StorageAccess over a live Transaction
// ---------------------------------------------------------------------------

struct CursorState {
    rows: Vec<ExecRow>,
    pos: usize,
}

/// A [`StorageAccess`] implementation backed by a live `bicdb_core::Transaction`
/// plus the per-table [`TableSchema`]s needed to materialize records in
/// canonical (non-hidden, schema-ordered) column order.
///
/// The `schemas` map is keyed by table/collection name; the caller resolves
/// schemas up front (e.g. via [`crate::load_schema`]). The borrow of the
/// transaction is a normal `&mut` for the lifetime of the storage object.
pub(crate) struct BicDbStorage<'a> {
    tx: &'a mut Transaction,
    schemas: HashMap<String, TableSchema>,
    cursors: HashMap<u64, CursorState>,
    next_cursor: u64,
}

impl<'a> BicDbStorage<'a> {
    /// Build storage over `tx`, with `schemas` providing the column layout for
    /// every table that will be accessed.
    pub(crate) fn new(tx: &'a mut Transaction, schemas: HashMap<String, TableSchema>) -> Self {
        BicDbStorage {
            tx,
            schemas,
            cursors: HashMap::new(),
            next_cursor: 1,
        }
    }

    fn schema_for(&self, table: &str) -> Result<&TableSchema> {
        self.schemas
            .get(table)
            .ok_or_else(|| unsupported(format!("host backend: no schema for table {table}")))
    }

    fn open_cursor(&mut self, rows: Vec<ExecRow>) -> CursorId {
        let id = self.next_cursor;
        self.next_cursor += 1;
        self.cursors.insert(id, CursorState { rows, pos: 0 });
        CursorId(id)
    }
}

/// Materialize a `Record` into an [`ExecRow`] in non-hidden schema column order,
/// using the exact engine primitive the oracle's projection consumes.
fn materialize_record(schema: &TableSchema, table: &str, record: &Record) -> ExecRow {
    let values = schema
        .columns
        .iter()
        .filter(|c| !c.hidden)
        .map(|c| crate::record_schema_column_value(record, c, true))
        .collect();
    ExecRow::stored(
        RowRef {
            collection: table.to_string(),
            record_id: record.id.clone(),
        },
        values,
    )
}

impl StorageAccess for BicDbStorage<'_> {
    fn column_schema(&self, table: &str) -> Result<Vec<String>> {
        Ok(self
            .schema_for(table)?
            .columns
            .iter()
            .filter(|c| !c.hidden)
            .map(|c| c.name.clone())
            .collect())
    }

    fn open_seq_scan(&mut self, table: &str) -> Result<CursorId> {
        let schema = self.schema_for(table)?.clone();
        let records = self.tx.scan_collection(table).map_err(SqlError::from)?;
        let rows = records
            .iter()
            .map(|r| materialize_record(&schema, table, r))
            .collect();
        Ok(self.open_cursor(rows))
    }

    fn open_primary_key_lookup(&mut self, table: &str, key: &SqlValue) -> Result<CursorId> {
        let schema = self.schema_for(table)?.clone();
        // Same record-id rendering the engine uses for a single-column PK
        // (`record_id_from_fields` → `SqlValue::to_cell`).
        let id = key.to_cell();
        let rows = match self.tx.get(table, &id).map_err(SqlError::from)? {
            Some(record) => vec![materialize_record(&schema, table, &record)],
            None => Vec::new(),
        };
        Ok(self.open_cursor(rows))
    }

    fn open_index_seek(
        &mut self,
        _table: &str,
        index: &str,
        _key: &[SqlValue],
    ) -> Result<CursorId> {
        Err(unsupported(format!(
            "host backend: index seek on {index} is not yet supported (falls back to oracle)"
        )))
    }

    fn open_index_range(
        &mut self,
        _table: &str,
        index: &str,
        _lower: &Bound,
        _upper: &Bound,
        _descending: bool,
    ) -> Result<CursorId> {
        Err(unsupported(format!(
            "host backend: index range on {index} is not yet supported (falls back to oracle)"
        )))
    }

    fn next_batch(&mut self, cursor: CursorId, max_rows: usize) -> Result<RowBatch> {
        let state = self
            .cursors
            .get_mut(&cursor.0)
            .ok_or_else(|| unsupported("host backend: unknown cursor"))?;
        let end = state.rows.len().min(state.pos + max_rows);
        let rows = state.rows[state.pos..end].to_vec();
        state.pos = end;
        let exhausted = state.pos >= state.rows.len();
        Ok(RowBatch { rows, exhausted })
    }

    fn close_cursor(&mut self, cursor: CursorId) -> Result<()> {
        self.cursors.remove(&cursor.0);
        Ok(())
    }

    fn insert_row(&mut self, table: &str, values: &[SqlValue]) -> Result<RowRef> {
        let schema = self.schema_for(table)?.clone();
        let cols: Vec<&ColumnSchema> = schema.columns.iter().filter(|c| !c.hidden).collect();
        if values.len() != cols.len() {
            return Err(unsupported(format!(
                "host backend: insert arity {} != {} columns",
                values.len(),
                cols.len()
            )));
        }
        let mut metadata = JsonValue::Object(Default::default());
        let mut id: Option<String> = None;
        for (col, val) in cols.iter().zip(values) {
            crate::set_json_object_value(
                &mut metadata,
                &col.name,
                crate::sql_value_to_column_storage_json(val.clone(), Some(col))?,
            );
            if col.primary_key {
                id = Some(val.to_cell());
            }
        }
        let id = id.ok_or_else(|| unsupported("host backend: insert without a primary key"))?;
        let record = Record::new(id.clone()).with_metadata(metadata);
        self.tx.insert(table, record).map_err(SqlError::from)?;
        Ok(RowRef {
            collection: table.to_string(),
            record_id: id,
        })
    }

    fn update_row(&mut self, row_ref: &RowRef, assignments: &[(String, SqlValue)]) -> Result<()> {
        let schema = self.schema_for(&row_ref.collection)?.clone();
        let existing = self
            .tx
            .get(&row_ref.collection, &row_ref.record_id)
            .map_err(SqlError::from)?
            .ok_or_else(|| unsupported("host backend: update target row not found"))?;
        let mut record: Record = (*existing).clone();
        for (name, value) in assignments {
            crate::set_json_object_value(
                &mut record.metadata,
                name,
                crate::sql_value_to_column_storage_json(value.clone(), schema.column(name))?,
            );
        }
        self.tx
            .update(&row_ref.collection, record)
            .map_err(SqlError::from)?;
        Ok(())
    }

    fn delete_row(&mut self, row_ref: &RowRef) -> Result<()> {
        self.tx
            .delete(&row_ref.collection, &row_ref.record_id)
            .map_err(SqlError::from)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HostBackend — ExecutionBackend with fuller scalar evaluation
// ---------------------------------------------------------------------------

const HOST_BATCH_ROWS: usize = 1024;

/// The host tree-walk execution backend. Reproduces the oracle over the real
/// [`BicDbStorage`] for the operator/scalar subset that [`lower_select`] emits.
pub(crate) struct HostBackend;

/// Coerce a value to a three-valued boolean: `Null` → `None`, `Bool` → `Some`,
/// anything else is an error (we refuse to guess truthiness).
fn as_bool_opt(v: &SqlValue) -> Result<Option<bool>> {
    match v {
        SqlValue::Null => Ok(None),
        SqlValue::Bool(b) => Ok(Some(*b)),
        _ => Err(unsupported("host backend: expected boolean operand")),
    }
}

fn bool3(opt: Option<bool>) -> SqlValue {
    match opt {
        Some(b) => SqlValue::Bool(b),
        None => SqlValue::Null,
    }
}

/// Numeric ordering with Int/Float coercion; `None` for NaN/non-comparable.
fn numeric_cmp(a: &SqlValue, b: &SqlValue) -> Option<Option<std::cmp::Ordering>> {
    use SqlValue::*;
    match (a, b) {
        (Int(x), Int(y)) => Some(Some(x.cmp(y))),
        (Float(x), Float(y)) => Some(x.partial_cmp(y)),
        (Int(x), Float(y)) => Some((*x as f64).partial_cmp(y)),
        (Float(x), Int(y)) => Some(x.partial_cmp(&(*y as f64))),
        _ => None,
    }
}

fn compare_values(op: CompareOp, a: &SqlValue, b: &SqlValue) -> Result<SqlValue> {
    use std::cmp::Ordering;
    if matches!(a, SqlValue::Null) || matches!(b, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let ordering: Ordering = if let Some(num) = numeric_cmp(a, b) {
        num.ok_or_else(|| unsupported("host backend: non-comparable numeric (NaN)"))?
    } else {
        match (a, b) {
            (SqlValue::String(x), SqlValue::String(y)) => x.cmp(y),
            (SqlValue::Bool(x), SqlValue::Bool(y)) => x.cmp(y),
            _ => return Err(unsupported("host backend: incomparable operand types")),
        }
    };
    let result = match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Le => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Ge => ordering != Ordering::Less,
    };
    Ok(SqlValue::Bool(result))
}

fn arithmetic(op: BinaryOp, a: &SqlValue, b: &SqlValue) -> Result<SqlValue> {
    use SqlValue::*;
    if matches!(a, Null) || matches!(b, Null) {
        return Ok(Null);
    }
    match (a, b) {
        (Int(x), Int(y)) => {
            let r = match op {
                BinaryOp::Add => x.wrapping_add(*y),
                BinaryOp::Sub => x.wrapping_sub(*y),
                BinaryOp::Mul => x.wrapping_mul(*y),
                BinaryOp::Div => x
                    .checked_div(*y)
                    .ok_or_else(|| unsupported("host backend: division by zero"))?,
                BinaryOp::Mod => x
                    .checked_rem(*y)
                    .ok_or_else(|| unsupported("host backend: modulo by zero"))?,
            };
            Ok(Int(r))
        }
        _ => {
            let (x, y) = match (numeric_as_f64(a), numeric_as_f64(b)) {
                (Some(x), Some(y)) => (x, y),
                _ => return Err(unsupported("host backend: non-numeric arithmetic")),
            };
            let r = match op {
                BinaryOp::Add => x + y,
                BinaryOp::Sub => x - y,
                BinaryOp::Mul => x * y,
                BinaryOp::Div => x / y,
                BinaryOp::Mod => x % y,
            };
            Ok(Float(r))
        }
    }
}

fn numeric_as_f64(v: &SqlValue) -> Option<f64> {
    match v {
        SqlValue::Int(x) => Some(*x as f64),
        SqlValue::Float(x) => Some(*x),
        _ => None,
    }
}

/// Evaluate a scalar against a row + params. Errs (safe) on anything unsupported.
fn eval_scalar(expr: &ScalarExpr, row: &[SqlValue], params: &[SqlValue]) -> Result<SqlValue> {
    match expr {
        ScalarExpr::Column(i) => row
            .get(*i as usize)
            .cloned()
            .ok_or_else(|| unsupported("host backend: out-of-range column")),
        ScalarExpr::Param(i) => params
            .get(*i as usize)
            .cloned()
            .ok_or_else(|| unsupported("host backend: out-of-range param")),
        ScalarExpr::Literal(v) => Ok(v.clone()),
        ScalarExpr::Compare { op, left, right } => {
            let a = eval_scalar(left, row, params)?;
            let b = eval_scalar(right, row, params)?;
            compare_values(*op, &a, &b)
        }
        ScalarExpr::Binary { op, left, right } => {
            let a = eval_scalar(left, row, params)?;
            let b = eval_scalar(right, row, params)?;
            arithmetic(*op, &a, &b)
        }
        ScalarExpr::And(a, b) => {
            let x = as_bool_opt(&eval_scalar(a, row, params)?)?;
            let y = as_bool_opt(&eval_scalar(b, row, params)?)?;
            Ok(bool3(match (x, y) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            }))
        }
        ScalarExpr::Or(a, b) => {
            let x = as_bool_opt(&eval_scalar(a, row, params)?)?;
            let y = as_bool_opt(&eval_scalar(b, row, params)?)?;
            Ok(bool3(match (x, y) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            }))
        }
        ScalarExpr::Not(a) => {
            let x = as_bool_opt(&eval_scalar(a, row, params)?)?;
            Ok(bool3(x.map(|b| !b)))
        }
        ScalarExpr::Unary { op, expr } => {
            let v = eval_scalar(expr, row, params)?;
            match op {
                super::exec_ir::UnaryOp::Not => Ok(bool3(as_bool_opt(&v)?.map(|b| !b))),
                super::exec_ir::UnaryOp::Neg => match v {
                    SqlValue::Null => Ok(SqlValue::Null),
                    SqlValue::Int(x) => Ok(SqlValue::Int(x.wrapping_neg())),
                    SqlValue::Float(x) => Ok(SqlValue::Float(-x)),
                    _ => Err(unsupported("host backend: negate of non-numeric")),
                },
            }
        }
        ScalarExpr::IsNull { expr, negated } => {
            let value = eval_scalar(expr, row, params)?;
            Ok(SqlValue::Bool(if *negated {
                crate::value_is_not_null_predicate(&value)
            } else {
                crate::value_is_null_predicate(&value)
            }))
        }
        ScalarExpr::Cast { .. } => Err(unsupported("host backend: CAST")),
        ScalarExpr::Function { name, .. } => {
            Err(unsupported(format!("host backend: function {name}")))
        }
    }
}

impl HostBackend {
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
                        .map(|e| eval_scalar(e, &[], params))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ExecRow::computed(values))
                })
                .collect(),
            RelOp::SeqScan { table } => {
                let cursor = storage.open_seq_scan(table)?;
                super::exec_ir::drain_cursor(storage, cursor, HOST_BATCH_ROWS)
            }
            RelOp::PrimaryKeyLookup { table, key } => {
                let k = eval_scalar(key, &[], params)?;
                let cursor = storage.open_primary_key_lookup(table, &k)?;
                super::exec_ir::drain_cursor(storage, cursor, HOST_BATCH_ROWS)
            }
            RelOp::IndexSeek { table, index, key } => {
                let k = key
                    .iter()
                    .map(|e| eval_scalar(e, &[], params))
                    .collect::<Result<Vec<_>>>()?;
                let cursor = storage.open_index_seek(table, index, &k)?;
                super::exec_ir::drain_cursor(storage, cursor, HOST_BATCH_ROWS)
            }
            RelOp::Filter { input, predicate } => {
                let rows = self.eval_op(input, storage, params)?;
                let mut out = Vec::new();
                for r in rows {
                    match eval_scalar(predicate, &r.values, params)? {
                        SqlValue::Bool(true) => out.push(r),
                        SqlValue::Bool(false) | SqlValue::Null => {}
                        _ => return Err(unsupported("host backend: non-boolean filter predicate")),
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
                            .map(|c| eval_scalar(&c.expr, &r.values, params))
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
                    let row_ref = r.row_ref.as_ref().ok_or_else(|| {
                        unsupported("host backend: UPDATE of a row without identity")
                    })?;
                    let assigns = assignments
                        .iter()
                        .map(|(name, expr)| {
                            Ok((name.clone(), eval_scalar(expr, &r.values, params)?))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    storage.update_row(row_ref, &assigns)?;
                }
                Ok(Vec::new())
            }
            RelOp::Delete { scan, .. } => {
                let rows = self.eval_op(scan, storage, params)?;
                for r in &rows {
                    let row_ref = r.row_ref.as_ref().ok_or_else(|| {
                        unsupported("host backend: DELETE of a row without identity")
                    })?;
                    storage.delete_row(row_ref)?;
                }
                Ok(Vec::new())
            }
            other => Err(unsupported(format!(
                "host backend: operator {} not supported",
                op_name(other)
            ))),
        }
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

impl ExecutionBackend for HostBackend {
    fn name(&self) -> &'static str {
        "host"
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

// ---------------------------------------------------------------------------
// lower_select — best-effort single-table SELECT → RelPlan
// ---------------------------------------------------------------------------

/// Map a sqlparser `BinaryOperator` to an IR comparison op.
fn compare_op(op: &BinaryOperator) -> Option<CompareOp> {
    Some(match op {
        BinaryOperator::Eq => CompareOp::Eq,
        BinaryOperator::NotEq => CompareOp::Ne,
        BinaryOperator::Lt => CompareOp::Lt,
        BinaryOperator::LtEq => CompareOp::Le,
        BinaryOperator::Gt => CompareOp::Gt,
        BinaryOperator::GtEq => CompareOp::Ge,
        _ => return None,
    })
}

fn arith_op(op: &BinaryOperator) -> Option<BinaryOp> {
    Some(match op {
        BinaryOperator::Plus => BinaryOp::Add,
        BinaryOperator::Minus => BinaryOp::Sub,
        BinaryOperator::Multiply => BinaryOp::Mul,
        BinaryOperator::Divide => BinaryOp::Div,
        BinaryOperator::Modulo => BinaryOp::Mod,
        _ => return None,
    })
}

fn col_index(cols: &[&ColumnSchema], name: &str) -> Option<u32> {
    cols.iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .map(|i| i as u32)
}

/// Lower a scalar expression. Returns `None` for any shape we can't represent.
fn lower_scalar(expr: &Expr, cols: &[&ColumnSchema]) -> Option<ScalarExpr> {
    match expr {
        Expr::Identifier(ident) => Some(ScalarExpr::Column(col_index(cols, &ident.value)?)),
        Expr::CompoundIdentifier(idents) => {
            let last = idents.last()?;
            Some(ScalarExpr::Column(col_index(cols, &last.value)?))
        }
        Expr::Value(vws) => match &vws.value {
            Value::Placeholder(s) => {
                // `$N` (1-based) → Param(N-1).
                let n: u32 = s.strip_prefix('$')?.parse().ok()?;
                Some(ScalarExpr::Param(n.checked_sub(1)?))
            }
            _ => Some(ScalarExpr::Literal(crate::literal_to_value(vws).ok()?)),
        },
        Expr::Nested(inner) => lower_scalar(inner, cols),
        Expr::IsNull(inner) => Some(ScalarExpr::IsNull {
            expr: Box::new(lower_scalar(inner, cols)?),
            negated: false,
        }),
        Expr::IsNotNull(inner) => Some(ScalarExpr::IsNull {
            expr: Box::new(lower_scalar(inner, cols)?),
            negated: true,
        }),
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Not => Some(ScalarExpr::Not(Box::new(lower_scalar(inner, cols)?))),
            UnaryOperator::Minus => Some(ScalarExpr::Unary {
                op: super::exec_ir::UnaryOp::Neg,
                expr: Box::new(lower_scalar(inner, cols)?),
            }),
            UnaryOperator::Plus => lower_scalar(inner, cols),
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Some(ScalarExpr::And(
                Box::new(lower_scalar(left, cols)?),
                Box::new(lower_scalar(right, cols)?),
            )),
            BinaryOperator::Or => Some(ScalarExpr::Or(
                Box::new(lower_scalar(left, cols)?),
                Box::new(lower_scalar(right, cols)?),
            )),
            _ => {
                if let Some(cop) = compare_op(op) {
                    Some(ScalarExpr::Compare {
                        op: cop,
                        left: Box::new(lower_scalar(left, cols)?),
                        right: Box::new(lower_scalar(right, cols)?),
                    })
                } else if let Some(aop) = arith_op(op) {
                    Some(ScalarExpr::Binary {
                        op: aop,
                        left: Box::new(lower_scalar(left, cols)?),
                        right: Box::new(lower_scalar(right, cols)?),
                    })
                } else {
                    None
                }
            }
        },
        _ => None,
    }
}

/// `true` if `expr` references no columns (suitable as a PK lookup key).
fn is_literal_or_param(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(_))
        || matches!(expr, Expr::Nested(inner) if is_literal_or_param(inner))
        || matches!(expr, Expr::UnaryOp { expr, .. } if is_literal_or_param(expr))
}

enum Access {
    Pk(ScalarExpr),
    Filter(ScalarExpr),
}

/// Lower a `WHERE` clause: detect a single-column PK equality (→ PK lookup),
/// otherwise lower the whole predicate (→ Filter). `None` = can't lower.
fn lower_where(
    expr: &Expr,
    cols: &[&ColumnSchema],
    single_pk: Option<&ColumnSchema>,
) -> Option<Access> {
    if let (
        Some(pk),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        },
    ) = (single_pk, expr)
    {
        let pk_name = pk.name.as_str();
        let try_side = |ident: &Expr, lit: &Expr| -> Option<ScalarExpr> {
            let is_pk = match ident {
                Expr::Identifier(id) => id.value.eq_ignore_ascii_case(pk_name),
                Expr::CompoundIdentifier(ids) => ids
                    .last()
                    .is_some_and(|id| id.value.eq_ignore_ascii_case(pk_name)),
                _ => false,
            };
            if is_pk && is_literal_or_param(lit) {
                lower_scalar(lit, cols)
            } else {
                None
            }
        };
        if let Some(key) = try_side(left, right).or_else(|| try_side(right, left)) {
            return Some(Access::Pk(key));
        }
    }
    Some(Access::Filter(lower_scalar(expr, cols)?))
}

/// Best-effort lowering of a single-table `SELECT` to a [`RelPlan`]. Returns
/// `None` (→ fall back to the oracle, always safe) for anything outside the
/// supported slice: CTEs, set ops, joins, multiple FROM items, GROUP BY/HAVING/
/// DISTINCT/window, ORDER BY, table-function sources, or projections/predicates
/// that aren't bare column refs / lowerable scalars.
///
/// `resolve_schema` maps a table name to its [`TableSchema`] (e.g. a closure
/// over [`crate::load_schema`]).
pub(crate) fn lower_select<F>(query: &Query, resolve_schema: F) -> Option<RelPlan>
where
    F: Fn(&str) -> Option<TableSchema>,
{
    if query.with.is_some() || query.order_by.is_some() {
        return None;
    }
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };

    // Reject anything we don't model.
    if select.distinct.is_some()
        || select.having.is_some()
        || select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || has_group_by(select)
    {
        return None;
    }
    let from = &select.from[0];
    let (name, args_present) = match &from.relation {
        TableFactor::Table { name, args, .. } => (name, args.is_some()),
        _ => return None,
    };
    if args_present {
        return None;
    }
    let table = crate::relation_name(name).ok()?;
    let schema = resolve_schema(&table)?;

    let cols: Vec<&ColumnSchema> = schema.columns.iter().filter(|c| !c.hidden).collect();
    let pk_cols: Vec<&ColumnSchema> = schema.columns.iter().filter(|c| c.primary_key).collect();
    let single_pk = if pk_cols.len() == 1 && !pk_cols[0].hidden {
        Some(pk_cols[0])
    } else {
        None
    };

    // ---- access path (FROM + WHERE) ----
    let access_root = match &select.selection {
        None => RelOp::SeqScan {
            table: table.clone(),
        },
        Some(expr) => match lower_where(expr, &cols, single_pk)? {
            Access::Pk(key) => RelOp::PrimaryKeyLookup {
                table: table.clone(),
                key,
            },
            Access::Filter(pred) => RelOp::Filter {
                input: Box::new(RelOp::SeqScan {
                    table: table.clone(),
                }),
                predicate: pred,
            },
        },
    };

    // ---- projection ----
    let mut output_columns = Vec::new();
    let mut items = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (i, c) in cols.iter().enumerate() {
                    output_columns.push(c.name.clone());
                    items.push(ProjectItem {
                        expr: ScalarExpr::Column(i as u32),
                        alias: c.name.clone(),
                    });
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = col_index(&cols, &ident.value)?;
                output_columns.push(ident.value.clone());
                items.push(ProjectItem {
                    expr: ScalarExpr::Column(idx),
                    alias: ident.value.clone(),
                });
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                let last = idents.last()?;
                let idx = col_index(&cols, &last.value)?;
                output_columns.push(last.value.clone());
                items.push(ProjectItem {
                    expr: ScalarExpr::Column(idx),
                    alias: last.value.clone(),
                });
            }
            _ => return None,
        }
    }
    if items.is_empty() {
        return None;
    }

    let mut root = RelOp::Project {
        input: Box::new(access_root),
        columns: items,
    };

    // ---- LIMIT / OFFSET ----
    if let Some(limit_clause) = &query.limit_clause {
        let (limit, offset) = match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } if limit_by.is_empty() => {
                let lim = match limit {
                    Some(e) => Some(crate::integer_expr(e).ok()? as u64),
                    None => None,
                };
                let off = match offset {
                    Some(o) => crate::integer_expr(&o.value).ok()? as u64,
                    None => 0,
                };
                (lim, off)
            }
            _ => return None,
        };
        root = RelOp::Limit {
            input: Box::new(root),
            limit,
            offset,
        };
    }

    Some(RelPlan {
        root,
        output_columns,
    })
}

/// `has_group_by` over the crate helper, swallowing its `Result` into a
/// conservative `true` (= "don't lower") on error.
fn has_group_by(select: &sqlparser::ast::Select) -> bool {
    crate::has_group_by(select).unwrap_or(true)
}

// ---------------------------------------------------------------------------
// End-to-end verification test: candidate (host backend over real storage)
// must reproduce the oracle (the interpreter) byte-for-byte.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SqlResult, SqlSession};
    use bicdb_core::BicDb;
    use sqlparser::ast::Statement;

    fn parse_query(sql: &str) -> Query {
        let stmts = crate::parse_statements(sql).expect("parse");
        match stmts.into_iter().next().expect("one statement") {
            Statement::Query(q) => *q,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    /// Run the candidate path: lower → host backend over a fresh transaction.
    fn run_candidate(db: &BicDb, sql: &str) -> ExecOutput {
        let query = parse_query(sql);
        let plan = lower_select(&query, |name| crate::load_schema(db, name).ok().flatten())
            .expect("lowering should succeed for the supported shapes");

        // Collect every schema the plan might touch (here: a single table).
        let mut schemas = HashMap::new();
        if let SetExpr::Select(select) = query.body.as_ref() {
            if let TableFactor::Table { name, .. } = &select.from[0].relation {
                let table = crate::relation_name(name).unwrap();
                let schema = crate::load_schema(db, &table).unwrap().unwrap();
                schemas.insert(table, schema);
            }
        }

        let mut tx = db.begin_transaction().expect("begin tx");
        let mut storage = BicDbStorage::new(&mut tx, schemas);
        HostBackend
            .execute(&plan, &mut storage, &[])
            .expect("host execute")
    }

    fn assert_parity(oracle: &SqlResult, candidate: &ExecOutput, what: &str) {
        assert_eq!(
            oracle.columns, candidate.columns,
            "columns mismatch for: {what}"
        );
        assert_eq!(oracle.rows, candidate.rows, "rows mismatch for: {what}");
    }

    #[test]
    fn host_backend_reproduces_oracle_on_real_storage() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();

        let selects = [
            "SELECT * FROM t",
            "SELECT name, n FROM t WHERE id = 3",
            "SELECT name FROM t WHERE n > 5",
            "SELECT id, name, n FROM t WHERE n >= 10 LIMIT 2",
        ];

        // Oracle pass: build data + capture interpreter results, then drop the
        // session so the db handle is free for the candidate's transaction.
        let mut oracle_results: Vec<SqlResult> = Vec::new();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute(
                    "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, n INT);
                     INSERT INTO t (id, name, n) VALUES (1, 'alice', 10);
                     INSERT INTO t (id, name, n) VALUES (2, 'bob', 5);
                     INSERT INTO t (id, name, n) VALUES (3, 'carol', 20);
                     INSERT INTO t (id, name, n) VALUES (4, 'dave', 3);",
                )
                .expect("ddl + dml");
            for sql in &selects {
                oracle_results.push(session.execute(sql).expect("oracle select"));
            }
        }

        for (sql, oracle) in selects.iter().zip(&oracle_results) {
            let candidate = run_candidate(&db, sql);
            assert_parity(oracle, &candidate, sql);
        }
    }

    #[test]
    fn lower_select_returns_none_for_unsupported_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, n INT);")
                .unwrap();
        }
        let resolve = |name: &str| crate::load_schema(&db, name).ok().flatten();

        // Aggregate / GROUP BY / ORDER BY / join are all out of scope → None.
        for sql in [
            "SELECT count(*) FROM t",
            "SELECT n, count(*) FROM t GROUP BY n",
            "SELECT * FROM t ORDER BY n",
            "SELECT * FROM t a JOIN t b ON a.id = b.id",
            "SELECT * FROM t, t t2",
        ] {
            let query = parse_query(sql);
            assert!(
                lower_select(&query, &resolve).is_none(),
                "expected None (fall back to oracle) for: {sql}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // A/B microbenchmark: host backend vs the oracle on a transactional read
    // path. Single-threaded, in-process, no pgwire. Run with:
    //   cargo test --release -p bicdb-sql adaptive::host::tests::bench \
    //       -- --ignored --nocapture
    // Measurement only — touches no hot path.
    // -----------------------------------------------------------------------

    use std::hint::black_box;
    use std::time::Instant;

    /// Tiny deterministic PRNG so the bench is reproducible without a dep.
    struct XorShift(u64);
    impl XorShift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        /// Uniform-ish pk in `1..=n`.
        fn pk(&mut self, n: i64) -> i64 {
            (self.next() % n as u64) as i64 + 1
        }
    }

    /// Non-hidden schema-order index of a column by name.
    fn col_idx(schema: &TableSchema, name: &str) -> u32 {
        schema
            .columns
            .iter()
            .filter(|c| !c.hidden)
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .expect("column present") as u32
    }

    /// Sorted (order-independent) row comparison for the seq-scan shape.
    fn sorted_rows(rows: &[Vec<SqlValue>]) -> Vec<String> {
        let mut keys: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
        keys.sort();
        keys
    }

    const ROWS: i64 = 10_000;
    const POINT_ITERS: usize = 100_000;
    const SCAN_ITERS: usize = 1_000;
    const SCAN_THRESHOLD: i64 = 9_950; // `seq > 9950` → ~50 rows

    #[test]
    #[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
    fn bench_host_vs_oracle_read_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();

        // ---- schema + 10k rows (customer-like) ----
        // id (pk), name (text), seq/bucket/payment_cnt (int), balance/ytd (decimal,
        // stored as the engine stores exact money: whatever literal_to_value yields
        // for a decimal literal).
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute(
                    "CREATE TABLE customer (\
                       id BIGINT PRIMARY KEY, \
                       name TEXT, \
                       seq INT, \
                       bucket INT, \
                       balance DECIMAL(12,2), \
                       ytd DECIMAL(12,2), \
                       payment_cnt INT);",
                )
                .expect("create table");

            // Batched multi-row INSERTs to keep setup fast.
            let mut i: i64 = 1;
            while i <= ROWS {
                let mut sql = String::from(
                    "INSERT INTO customer (id, name, seq, bucket, balance, ytd, payment_cnt) VALUES ",
                );
                let batch_end = (i + 500).min(ROWS + 1);
                let mut first = true;
                for j in i..batch_end {
                    if !first {
                        sql.push(',');
                    }
                    first = false;
                    sql.push_str(&format!(
                        "({j}, 'name{j}', {j}, {bucket}, {bal}.{cents:02}, {ytd}.00, {pc})",
                        bucket = j % 10,
                        bal = 1000 + j,
                        cents = (j % 100),
                        ytd = 5000 + j,
                        pc = j % 50,
                    ));
                }
                sql.push(';');
                session.execute(&sql).expect("insert batch");
                i = batch_end;
            }

            // Sanity: row count.
            let cnt = session.execute("SELECT id FROM customer").unwrap();
            assert_eq!(cnt.rows.len(), ROWS as usize, "expected {ROWS} rows");
        }

        // Schema for the host path (resolved once).
        let schema = crate::load_schema(&db, "customer").unwrap().unwrap();
        let mut schemas = HashMap::new();
        schemas.insert("customer".to_string(), schema.clone());

        // ---- hand-built cached plans ----
        // Shape 1: point lookup  SELECT id, name, balance, ytd WHERE id = $1
        let point_cols = ["id", "name", "balance", "ytd"];
        let point_plan = RelPlan {
            output_columns: point_cols.iter().map(|s| s.to_string()).collect(),
            root: RelOp::Project {
                columns: point_cols
                    .iter()
                    .map(|c| ProjectItem {
                        expr: ScalarExpr::Column(col_idx(&schema, c)),
                        alias: c.to_string(),
                    })
                    .collect(),
                input: Box::new(RelOp::PrimaryKeyLookup {
                    table: "customer".into(),
                    key: ScalarExpr::Param(0),
                }),
            },
        };

        // Shape 2: seq scan + filter  SELECT id, name WHERE seq > 9950
        let scan_cols = ["id", "name"];
        let scan_plan = RelPlan {
            output_columns: scan_cols.iter().map(|s| s.to_string()).collect(),
            root: RelOp::Project {
                columns: scan_cols
                    .iter()
                    .map(|c| ProjectItem {
                        expr: ScalarExpr::Column(col_idx(&schema, c)),
                        alias: c.to_string(),
                    })
                    .collect(),
                input: Box::new(RelOp::Filter {
                    input: Box::new(RelOp::SeqScan {
                        table: "customer".into(),
                    }),
                    predicate: ScalarExpr::Compare {
                        op: CompareOp::Gt,
                        left: Box::new(ScalarExpr::Column(col_idx(&schema, "seq"))),
                        right: Box::new(ScalarExpr::Literal(SqlValue::Int(SCAN_THRESHOLD))),
                    },
                }),
            },
        };

        // ---- parity guards (must hold before timing) ----
        {
            let mut session = SqlSession::new(&mut db);
            let sample_pk = 4242i64;
            let oracle_point = session
                .execute(&format!(
                    "SELECT id, name, balance, ytd FROM customer WHERE id = {sample_pk}"
                ))
                .unwrap();
            let oracle_scan = session
                .execute(&format!(
                    "SELECT id, name FROM customer WHERE seq > {SCAN_THRESHOLD}"
                ))
                .unwrap();
            drop(session);

            let mut tx = db.begin_transaction().unwrap();
            let mut storage = BicDbStorage::new(&mut tx, schemas.clone());
            let cand_point = HostBackend
                .execute(&point_plan, &mut storage, &[SqlValue::Int(sample_pk)])
                .unwrap();
            let cand_scan = HostBackend.execute(&scan_plan, &mut storage, &[]).unwrap();
            drop(tx);

            assert_eq!(oracle_point.columns, cand_point.columns, "point columns");
            assert_eq!(oracle_point.rows, cand_point.rows, "point rows");
            assert_eq!(oracle_scan.columns, cand_scan.columns, "scan columns");
            assert_eq!(
                sorted_rows(&oracle_scan.rows),
                sorted_rows(&cand_scan.rows),
                "scan rows (order-independent)"
            );
            assert_eq!(oracle_scan.rows.len(), 50, "expected ~50 scan rows");
        }

        // Pre-generate the pk stream so PRNG cost isn't in the timed loop.
        let mut rng = XorShift(0x9E3779B97F4A7C15);
        let point_pks: Vec<i64> = (0..POINT_ITERS).map(|_| rng.pk(ROWS)).collect();

        let mut sink = 0i64;

        // ===================== SHAPE 1: POINT LOOKUP =====================

        // A. ORACLE full path (re-parse + plan + exec per iter).
        let a_point = {
            let mut session = SqlSession::new(&mut db);
            let t = Instant::now();
            for &pk in &point_pks {
                let r = session
                    .execute(&format!(
                        "SELECT id, name, balance, ytd FROM customer WHERE id = {pk}"
                    ))
                    .unwrap();
                sink += r.rows.len() as i64;
            }
            let ns = t.elapsed().as_nanos() as f64 / POINT_ITERS as f64;
            drop(session);
            ns
        };

        // A2. ORACLE pre-parsed: parse ONCE, then execute_query (plan+exec, no
        // parse) per iter — the real per-call cost the proc path pays (embedded
        // SQL is parsed once at CREATE PROCEDURE). Fixed pk because the Query is
        // parsed once; parse/plan cost is pk-independent.
        //
        // The proc path runs embedded SQL *inside* the outer `execute("CALL …")`'s
        // schema-cache scope, so a bare `execute_query` is only representative
        // with that scope active — otherwise each call re-reads/reconciles the
        // catalog and is *slower* than `execute()`. We hold the same thread-local
        // scope (`SqlSchemaCacheScope`) across the loop to match the proc path.
        let a2_point = {
            let a2_pk = 4242i64;
            let query = parse_query(&format!(
                "SELECT id, name, balance, ytd FROM customer WHERE id = {a2_pk}"
            ));
            let mut session = SqlSession::new(&mut db);
            let _schema_scope = crate::SqlSchemaCacheScope::new();
            let t = Instant::now();
            for _ in 0..POINT_ITERS {
                let r = session.execute_query(&query).unwrap();
                sink += r.rows.len() as i64;
            }
            let ns = t.elapsed().as_nanos() as f64 / POINT_ITERS as f64;
            drop(session);
            ns
        };

        // A2C. Same as A2 (pre-parsed `execute_query`, proc-style, under the
        // schema-cache scope) but with the bound-plan cache ENABLED. This is the
        // headline of the change: collapse per-call planning to a cached plan +
        // exec. The cache is warmed once so the timed loop measures steady-state
        // hits.
        let a2c_point = {
            let a2_pk = 4242i64;
            let query = parse_query(&format!(
                "SELECT id, name, balance, ytd FROM customer WHERE id = {a2_pk}"
            ));
            let mut session = SqlSession::new(&mut db);
            let _schema_scope = crate::SqlSchemaCacheScope::new();
            crate::plan_cache::set_test_override(Some(true));
            let _ = session.execute_query(&query).unwrap(); // warm the plan cache
            let t = Instant::now();
            for _ in 0..POINT_ITERS {
                let r = session.execute_query(&query).unwrap();
                sink += r.rows.len() as i64;
            }
            let ns = t.elapsed().as_nanos() as f64 / POINT_ITERS as f64;
            crate::plan_cache::set_test_override(None);
            drop(session);
            ns
        };

        // B. HOST steady-state: cached plan, fresh tx + storage per iter.
        let b_point = {
            let t = Instant::now();
            for &pk in &point_pks {
                let mut tx = db.begin_transaction().unwrap();
                let mut storage = BicDbStorage::new(&mut tx, schemas.clone());
                let out = HostBackend
                    .execute(&point_plan, &mut storage, &[SqlValue::Int(pk)])
                    .unwrap();
                sink += out.rows.len() as i64;
            }
            t.elapsed().as_nanos() as f64 / POINT_ITERS as f64
        };

        // B2. HOST steady-state: cached plan, ONE long-lived read tx + storage.
        let b2_point = {
            let mut tx = db.begin_transaction().unwrap();
            let mut storage = BicDbStorage::new(&mut tx, schemas.clone());
            let t = Instant::now();
            for &pk in &point_pks {
                let out = HostBackend
                    .execute(&point_plan, &mut storage, &[SqlValue::Int(pk)])
                    .unwrap();
                sink += out.rows.len() as i64;
            }
            let ns = t.elapsed().as_nanos() as f64 / POINT_ITERS as f64;
            drop(storage);
            drop(tx);
            ns
        };

        // C. HOST incl. lowering (parse + lower + exec per iter; never-cached).
        let c_point = {
            let resolve = |name: &str| crate::load_schema(&db, name).ok().flatten();
            let point_sql = "SELECT id, name, balance, ytd FROM customer WHERE id = $1";
            let t = Instant::now();
            for &pk in &point_pks {
                let query = parse_query(point_sql);
                let plan = lower_select(&query, &resolve).expect("lower");
                let mut tx = db.begin_transaction().unwrap();
                let mut storage = BicDbStorage::new(&mut tx, schemas.clone());
                let out = HostBackend
                    .execute(&plan, &mut storage, &[SqlValue::Int(pk)])
                    .unwrap();
                sink += out.rows.len() as i64;
            }
            t.elapsed().as_nanos() as f64 / POINT_ITERS as f64
        };

        // ===================== SHAPE 2: SEQ SCAN + FILTER =====================

        let a_scan = {
            let mut session = SqlSession::new(&mut db);
            let sql = format!("SELECT id, name FROM customer WHERE seq > {SCAN_THRESHOLD}");
            let t = Instant::now();
            for _ in 0..SCAN_ITERS {
                let r = session.execute(&sql).unwrap();
                sink += r.rows.len() as i64;
            }
            let ns = t.elapsed().as_nanos() as f64 / SCAN_ITERS as f64;
            drop(session);
            ns
        };

        let b_scan = {
            let t = Instant::now();
            for _ in 0..SCAN_ITERS {
                let mut tx = db.begin_transaction().unwrap();
                let mut storage = BicDbStorage::new(&mut tx, schemas.clone());
                let out = HostBackend.execute(&scan_plan, &mut storage, &[]).unwrap();
                sink += out.rows.len() as i64;
            }
            t.elapsed().as_nanos() as f64 / SCAN_ITERS as f64
        };

        let b2_scan = {
            let mut tx = db.begin_transaction().unwrap();
            let mut storage = BicDbStorage::new(&mut tx, schemas.clone());
            let t = Instant::now();
            for _ in 0..SCAN_ITERS {
                let out = HostBackend.execute(&scan_plan, &mut storage, &[]).unwrap();
                sink += out.rows.len() as i64;
            }
            let ns = t.elapsed().as_nanos() as f64 / SCAN_ITERS as f64;
            drop(storage);
            drop(tx);
            ns
        };

        black_box(sink);

        // ---- report ----
        println!("\n=== Host backend vs oracle: per-statement read path ===");
        println!(
            "rows={ROWS}  point_iters={POINT_ITERS}  scan_iters={SCAN_ITERS}  (single-thread, in-process)\n"
        );
        println!("SHAPE 1 — point lookup  (SELECT id,name,balance,ytd WHERE id=pk)");
        println!("  {:<34} {:>12}  {:>10}", "variant", "ns/op", "ratio vs A");
        println!(
            "  {:<34} {:>12.0}  {:>10.2}",
            "A  oracle full (parse+plan+exec)", a_point, 1.0
        );
        println!(
            "  {:<34} {:>12.0}  {:>10.2}",
            "A2 oracle pre-parsed (plan+exec)",
            a2_point,
            a2_point / a_point
        );
        println!(
            "  {:<34} {:>12.0}  {:>10.2}",
            "A2C plan-cache (cached plan+exec)",
            a2c_point,
            a2c_point / a_point
        );
        println!(
            "  {:<34} {:>12.0}  {:>10.2}",
            "B  host cached (exec only)",
            b_point,
            b_point / a_point
        );
        println!(
            "  {:<34} {:>12.0}  {:>10.2}",
            "B2 host cached, 1 long-lived tx",
            b2_point,
            b2_point / a_point
        );
        println!(
            "  {:<34} {:>12.0}  {:>10.2}",
            "C  host + lowering, tx/iter",
            c_point,
            c_point / a_point
        );
        println!(
            "  per-tx overhead (B - B2) = {:.0} ns/op",
            b_point - b2_point
        );
        // Decomposition of the oracle full-path cost:
        //   parse ≈ A − A2   plan ≈ A2 − B   exec ≈ B
        println!(
            "  decomposition:  parse≈{:.0}  plan≈{:.0}  exec≈{:.0}  ns/op",
            a_point - a2_point,
            a2_point - b_point,
            b_point
        );
        println!(
            "  proc path (pre-parsed A2) avoidable-by-host = plan≈{:.0} ns/op ({:.0}% of A2)",
            a2_point - b_point,
            100.0 * (a2_point - b_point) / a2_point
        );
        println!(
            "  plan-cache (A2C) vs A2: {:.0} -> {:.0} ns/op  ({:.2}x, -{:.0}% per call)\n",
            a2_point,
            a2c_point,
            a2_point / a2c_point,
            100.0 * (a2_point - a2c_point) / a2_point
        );

        println!(
            "SHAPE 2 — seq scan + filter  (SELECT id,name WHERE seq>{SCAN_THRESHOLD}, ~50 rows)"
        );
        println!("  {:<34} {:>12}", "variant", "ns/op");
        println!("  {:<34} {:>12.0}", "A  oracle full path", a_scan);
        println!("  {:<34} {:>12.0}", "B  host cached, tx/iter", b_scan);
        println!(
            "  {:<34} {:>12.0}",
            "B2 host cached, 1 long-lived tx", b2_scan
        );
        println!(
            "  ratios:  B/A={:.2}  B2/A={:.2}",
            b_scan / a_scan,
            b2_scan / a_scan
        );
        println!("  per-tx overhead (B - B2) = {:.0} ns/op", b_scan - b2_scan);
        println!("=== end ===\n");
    }
}
