//! Row/plan runtime types: SqlRow/SlotRow, CteResult, column lookups, slot row and bound expression scopes (BoundExpr*), prepared dynamic record lookups, join transitive predicates, join plan memo, and catalog view materializers.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use planner::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;
use rustc_hash::FxHashSet;

pub(crate) type SqlRow = FxHashMap<String, SqlValue>;
pub(crate) type SlotRow = Vec<SqlValue>;

#[derive(Clone, Debug)]
pub(crate) struct CteResult {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Arc<Vec<Vec<SqlValue>>>,
    /// Per-column logical PostgreSQL type names, aligned 1:1 with `columns`
    /// (`None` for columns whose type could not be resolved). Empty when the CTE
    /// was materialized without type information. Used so a query reading the CTE
    /// (e.g. `SELECT * FROM cte`) reports the same OID as the CTE's own query.
    pub(crate) column_types: Vec<Option<String>>,
}

impl CteResult {
    pub(crate) fn new(name: String, columns: Vec<String>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self {
            name,
            columns,
            rows: Arc::new(rows),
            column_types: Vec::new(),
        }
    }

    /// Attach per-column logical type names, normalized to exactly `columns.len()`
    /// entries (padded with `None`, truncated if longer) so positional lookups by
    /// the type resolver are always in range.
    pub(crate) fn with_column_types(mut self, mut column_types: Vec<Option<String>>) -> Self {
        if column_types.iter().any(Option::is_some) {
            column_types.resize(self.columns.len(), None);
            self.column_types = column_types;
        }
        self
    }

    /// Logical PostgreSQL type name of the column at `index`, if known.
    pub(crate) fn column_type(&self, index: usize) -> Option<String> {
        self.column_types.get(index).cloned().flatten()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RowSet {
    pub(crate) rows: Vec<SlotRow>,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct UpdateAssignmentCandidate {
    pub(crate) record: Record,
    pub(crate) columns: Arc<Vec<String>>,
    pub(crate) row: SlotRow,
}

#[derive(Clone, Debug)]
pub(crate) struct DeleteCandidate {
    pub(crate) record: Record,
    pub(crate) columns: Arc<Vec<String>>,
    pub(crate) row: SlotRow,
}

/// Column-layout-derived lookup maps. These depend only on the ordered list of
/// column names for a row shape, so they are identical across every execution of
/// a statement against the same table/join shape. Building them allocates a
/// lowercased key plus several hash-map entries per column, which previously ran
/// on every embedded statement inside a stored procedure. They are now built
/// once per distinct column layout and shared via `Arc` (see
/// `cached_column_lookups`), which removes the dominant per-statement allocation
/// and hashing cost under high-frequency routine workloads.
#[derive(Debug)]
pub(crate) struct ColumnLookups {
    pub(crate) columns: Vec<String>,
    /// Lowercased full column name -> id (last definition wins, matching prior
    /// `HashMap::insert` semantics).
    pub(crate) exact: FxHashMap<String, ColumnId>,
    /// Lowercased trailing identifier -> id (first definition wins).
    pub(crate) lookup_unqualified: FxHashMap<String, ColumnId>,
    /// Lowercased trailing identifier -> id, or `None` when ambiguous across
    /// multiple columns (scope binding semantics).
    pub(crate) scope_unqualified: FxHashMap<String, Option<ColumnId>>,
    /// Lowercased relation qualifier -> the one column it qualifies, or
    /// `None` when it qualifies several. An identifier that is not a column
    /// (every routine variable in an unbound predicate) used to be checked
    /// against each column's qualifier by a linear scan, per row.
    pub(crate) qualifiers: FxHashMap<String, Option<ColumnId>>,
}

impl ColumnLookups {
    pub(crate) fn build(columns: &[String]) -> Self {
        let mut exact = FxHashMap::with_capacity_and_hasher(columns.len(), Default::default());
        let mut lookup_unqualified =
            FxHashMap::with_capacity_and_hasher(columns.len(), Default::default());
        let mut scope_unqualified =
            FxHashMap::with_capacity_and_hasher(columns.len(), Default::default());
        let mut qualifiers: FxHashMap<String, Option<ColumnId>> = FxHashMap::default();
        for (idx, column) in columns.iter().enumerate() {
            let id = ColumnId(idx);
            if let Some((qualifier, _)) = column.split_once('.') {
                qualifiers
                    .entry(qualifier.to_ascii_lowercase())
                    .and_modify(|existing| *existing = None)
                    .or_insert(Some(id));
            }
            exact.insert(column.to_ascii_lowercase(), id);
            let short = column
                .rsplit('.')
                .next()
                .unwrap_or(column)
                .to_ascii_lowercase();
            lookup_unqualified.entry(short.clone()).or_insert(id);
            scope_unqualified
                .entry(short)
                .and_modify(|existing: &mut Option<ColumnId>| {
                    if *existing != Some(id) {
                        *existing = None;
                    }
                })
                .or_insert(Some(id));
        }
        Self {
            columns: columns.to_vec(),
            exact,
            lookup_unqualified,
            scope_unqualified,
            qualifiers,
        }
    }

    /// The columns `alias` qualifies: `None` = none, `Some(None)` = several,
    /// `Some(Some(id))` = exactly one.
    fn qualifier(&self, alias: &str) -> Option<Option<ColumnId>> {
        if let Some(found) = self.qualifiers.get(alias) {
            return Some(*found);
        }
        if alias.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return self.qualifiers.get(&alias.to_ascii_lowercase()).copied();
        }
        None
    }
}

thread_local! {
    /// Per-thread cache of column-layout-derived lookup maps. Keyed by the
    /// ordered column names; entries are pure functions of the key so they never
    /// need invalidation. Bounded in practice by the number of distinct table/
    /// join column shapes a connection touches.
    pub(crate) static COLUMN_LOOKUPS_CACHE: RefCell<FxHashMap<Vec<String>, Arc<ColumnLookups>>> =
        RefCell::new(FxHashMap::default());
}

pub(crate) fn cached_column_lookups(columns: &[String]) -> Arc<ColumnLookups> {
    COLUMN_LOOKUPS_CACHE.with(|cache| {
        if let Some(found) = cache.borrow().get(columns) {
            return found.clone();
        }
        let built = Arc::new(ColumnLookups::build(columns));
        cache.borrow_mut().insert(columns.to_vec(), built.clone());
        built
    })
}

#[derive(Clone, Debug)]
pub(crate) struct SlotRowLookup {
    pub(crate) cols: Arc<ColumnLookups>,
}

impl SlotRowLookup {
    pub(crate) fn new(columns: &[String]) -> Self {
        Self {
            cols: cached_column_lookups(columns),
        }
    }

    pub(crate) fn columns(&self) -> &[String] {
        &self.cols.columns
    }

    pub(crate) fn value_for_key<'a>(&self, row: &'a [SqlValue], key: &str) -> Option<&'a SqlValue> {
        if let Some(value) = self
            .cols
            .exact
            .get(key)
            .or_else(|| self.cols.lookup_unqualified.get(key))
            .and_then(|id| row.get(id.0))
        {
            return Some(value);
        }
        let key = key.to_ascii_lowercase();
        self.cols
            .exact
            .get(&key)
            .or_else(|| self.cols.lookup_unqualified.get(&key))
            .and_then(|id| row.get(id.0))
    }

    pub(crate) fn scalar_value_for_relation_alias(
        &self,
        row: &[SqlValue],
        alias: &str,
    ) -> Option<SqlValue> {
        // Exactly one column qualified by `alias` yields its value.
        match self.cols.qualifier(alias)? {
            Some(id) => row.get(id.0).cloned(),
            None => None,
        }
    }

    pub(crate) fn composite_value_for_relation_alias(
        &self,
        row: &[SqlValue],
        alias: &str,
    ) -> Option<SqlValue> {
        // No column carries this qualifier: nothing to compose.
        self.cols.qualifier(alias)?;
        let fields = self
            .cols
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                let (qualifier, field) = column.split_once('.')?;
                (qualifier.eq_ignore_ascii_case(alias)
                    && !matches!(
                        field.to_ascii_lowercase().as_str(),
                        "tableoid" | "xmin" | "xmax" | "cmin" | "cmax" | "ctid"
                    ))
                .then(|| SqlCompositeField {
                    name: field.to_string(),
                    pg_type: "unknown".to_string(),
                    value: row.get(idx).cloned().unwrap_or(SqlValue::Null),
                })
            })
            .collect::<Vec<_>>();
        (!fields.is_empty()).then(|| {
            SqlValue::Composite(SqlComposite {
                type_oid: None,
                type_name: alias.to_string(),
                fields,
            })
        })
    }

    pub(crate) fn value_from_parts(&self, row: &[SqlValue], parts: &[String]) -> Option<SqlValue> {
        if parts.is_empty() {
            return None;
        }
        if parts.len() == 1 {
            return self.value_for_key(row, &parts[0]).cloned();
        }
        #[cfg(test)]
        SQL_ROW_LOOKUP_COMPOUND_JOINS.with(|joins| *joins.borrow_mut() += 1);
        let full_key = parts.join(".");
        if let Some(value) = self.value_for_key(row, &full_key) {
            return Some(value.clone());
        }
        let qualified_column = format!("{}.{}", parts[0], parts[1]);
        if let Some(value) = self.value_for_key(row, &qualified_column) {
            return if parts.len() == 2 {
                Some(value.clone())
            } else {
                Some(
                    sql_json_path(value, &parts[2..])
                        .map(json_to_sql_value)
                        .unwrap_or(SqlValue::Null),
                )
            };
        }
        if parts[0].eq_ignore_ascii_case("metadata") {
            if let Some(value) = self.value_for_key(row, "metadata") {
                return Some(
                    sql_json_path(value, &parts[1..])
                        .map(json_to_sql_value)
                        .unwrap_or(SqlValue::Null),
                );
            }
        }
        if let Some(value) = self.value_for_key(row, &parts[0]) {
            if let Some(path_value) = sql_json_path(value, &parts[1..]) {
                return Some(json_to_sql_value(path_value));
            }
        }
        self.value_for_key(row, parts.last().expect("checked non-empty parts"))
            .cloned()
    }

    pub(crate) fn qualified_value_from_parts(
        &self,
        row: &[SqlValue],
        parts: &[String],
    ) -> Option<SqlValue> {
        if parts.len() <= 1 {
            return self.value_from_parts(row, parts);
        }
        let full_key = parts.join(".");
        if let Some(value) = self.value_for_key(row, &full_key) {
            return Some(value.clone());
        }
        let qualified_column = format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
        if let Some(value) = self.value_for_key(row, &qualified_column) {
            return Some(value.clone());
        }
        if parts[0].eq_ignore_ascii_case("metadata") {
            if let Some(value) = self.value_for_key(row, "metadata") {
                return Some(
                    sql_json_path(value, &parts[1..])
                        .map(json_to_sql_value)
                        .unwrap_or(SqlValue::Null),
                );
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OuterSlotRow {
    pub(crate) lookup: SlotRowLookup,
    pub(crate) values: SlotRow,
}

impl OuterSlotRow {
    pub(crate) fn new(columns: Vec<String>, values: SlotRow) -> Self {
        Self {
            lookup: SlotRowLookup::new(&columns),
            values,
        }
    }

    pub(crate) fn from_slot(columns: &[String], values: &[SqlValue]) -> Self {
        Self::new(columns.to_vec(), values.to_vec())
    }

    pub(crate) fn from_sql_row(row: &SqlRow) -> Self {
        let mut columns = row.keys().cloned().collect::<Vec<_>>();
        columns.sort();
        let values = columns
            .iter()
            .map(|column| row.get(column).cloned().unwrap_or(SqlValue::Null))
            .collect();
        Self::new(columns, values)
    }

    pub(crate) fn columns(&self) -> &[String] {
        self.lookup.columns()
    }

    pub(crate) fn value_from_parts(&self, parts: &[String]) -> Option<SqlValue> {
        self.lookup.value_from_parts(&self.values, parts)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)]
pub(crate) struct ColumnId(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)]
pub(crate) struct VarId(pub(crate) usize);

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum BoundExpr {
    /// The result of a hoisted user-function call (see `BoundUserCall`): slot
    /// `n` of `BoundExprFrame::user_calls`, evaluated by the routine executor
    /// before the bound tree.
    UserCall(usize),
    /// `array_var[index]` on a routine variable declared with an array type
    /// (`BoundExprScope::with_array_vars`); evaluated by the same single-index
    /// helper the generic access-chain evaluator uses.
    Subscript {
        array: Box<BoundExpr>,
        index: Box<BoundExpr>,
        subscript: Box<Subscript>,
    },
    Column(ColumnId),
    Var(VarId),
    Literal(SqlValue),
    Binary {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        right: Box<BoundExpr>,
    },
    Compare {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        right: Box<BoundExpr>,
    },
    And(Box<BoundExpr>, Box<BoundExpr>),
    Or(Box<BoundExpr>, Box<BoundExpr>),
    Cast {
        expr: Box<BoundExpr>,
        data_type: DataType,
    },
    UnaryMinus(Box<BoundExpr>),
    UnaryNot(Box<BoundExpr>),
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    /// Only built when the caller has verified branch type compatibility in
    /// its schema/column scope. Branches remain lazy, including their errors.
    Case {
        operand: Option<Box<BoundExpr>>,
        conditions: Vec<(BoundExpr, BoundExpr)>,
        else_result: Option<Box<BoundExpr>>,
    },
    /// A pure builtin from `BOUND_BUILTIN_FUNCTIONS`, evaluated by the same
    /// value-level dispatcher the generic path ends in. Routine bodies call
    /// these on every row (TPC-C's DBMS_RANDOM is `trunc(random() * ...)`,
    /// ~40 calls per NEWORD); unbound, each call materialized the frame's
    /// variable map and walked the session evaluator.
    Call {
        name: String,
        args: Vec<BoundExpr>,
    },
}

/// Builtins a bound expression may call: pure functions of their argument
/// values that the session evaluator resolves through
/// `eval_compatibility_function_value` regardless of argument types.
const BOUND_BUILTIN_FUNCTIONS: &[&str] = &["random", "trunc", "abs", "round", "mod", "char_length"];

fn bound_builtin_name(name: &ObjectName) -> Option<String> {
    let name = object_name(name).ok()?.to_ascii_lowercase();
    let bare = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    BOUND_BUILTIN_FUNCTIONS
        .contains(&bare)
        .then(|| bare.to_string())
}

/// A call to a non-builtin function inside an otherwise bound routine
/// expression. The binder cannot evaluate stored routines (that needs the
/// session), so the call is hoisted: the routine executor evaluates every
/// hoisted call in slot order (post-order = PostgreSQL's left-to-right,
/// innermost-first evaluation) with the bound args, then evaluates the tree
/// with the results. Hoisting is only allowed where every operand is
/// evaluated anyway — never under AND/OR (short circuit) and never alongside a
/// volatile builtin whose relative order would be observable.
#[derive(Clone, Debug)]
pub(crate) struct BoundUserCall {
    /// Address of the `Expr::Function` node in the routine IR; keys the
    /// session's function-dispatch memo (the IR is immutable and `Arc`-held).
    pub(crate) node: usize,
    pub(crate) name: String,
    pub(crate) args: Vec<BoundExpr>,
}

struct BindCtx<'a> {
    /// `Some` while binding a routine expression that may hoist user calls.
    calls: Option<Vec<BoundUserCall>>,
    /// False under a short-circuit operator.
    hoistable: bool,
    validate_case: Option<&'a dyn Fn(&[sqlparser::ast::CaseWhen], Option<&Expr>) -> bool>,
}

fn bound_expr_calls_builtin(expr: &BoundExpr, builtin: &str) -> bool {
    match expr {
        BoundExpr::Column(_)
        | BoundExpr::Var(_)
        | BoundExpr::Literal(_)
        | BoundExpr::UserCall(_) => false,
        BoundExpr::Binary { left, right, .. } | BoundExpr::Compare { left, right, .. } => {
            bound_expr_calls_builtin(left, builtin) || bound_expr_calls_builtin(right, builtin)
        }
        BoundExpr::Subscript { array, index, .. } => {
            bound_expr_calls_builtin(array, builtin) || bound_expr_calls_builtin(index, builtin)
        }
        BoundExpr::And(left, right) | BoundExpr::Or(left, right) => {
            bound_expr_calls_builtin(left, builtin) || bound_expr_calls_builtin(right, builtin)
        }
        BoundExpr::Cast { expr, .. }
        | BoundExpr::UnaryMinus(expr)
        | BoundExpr::UnaryNot(expr)
        | BoundExpr::IsNull { expr, .. } => bound_expr_calls_builtin(expr, builtin),
        BoundExpr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| bound_expr_calls_builtin(expr, builtin))
                || conditions.iter().any(|(condition, result)| {
                    bound_expr_calls_builtin(condition, builtin)
                        || bound_expr_calls_builtin(result, builtin)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| bound_expr_calls_builtin(expr, builtin))
        }
        BoundExpr::Call { name, args } => {
            name == builtin
                || args
                    .iter()
                    .any(|arg| bound_expr_calls_builtin(arg, builtin))
        }
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct BoundExprScope {
    pub(crate) cols: Arc<ColumnLookups>,
    pub(crate) exact_vars: Arc<FxHashMap<String, VarId>>,
    /// Variables declared with an array type: the only roots `var[index]`
    /// binds for (a jsonb-typed variable subscripts differently).
    pub(crate) array_vars: Arc<FxHashSet<VarId>>,
}

#[allow(dead_code)]
pub(crate) struct BoundExprFrame<'a> {
    pub(crate) db: &'a BicDb,
    pub(crate) columns: BoundExprColumns<'a>,
    pub(crate) vars: &'a [SqlValue],
    /// Results of the expression's hoisted user calls, in slot order.
    pub(crate) user_calls: &'a [SqlValue],
}

#[allow(dead_code)]
pub(crate) enum BoundExprColumns<'a> {
    Values(&'a [SqlValue]),
    Joined {
        left: &'a [SqlValue],
        right: &'a [SqlValue],
        sources: &'a [JoinedSlotSource],
    },
    Row {
        row: &'a SqlRow,
        column_keys: &'a [String],
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum JoinedSlotSource {
    Left(usize),
    Right(usize),
}

#[derive(Clone, Debug)]
pub(crate) struct BoundRowContext {
    pub(crate) row_lookup: SlotRowLookup,
    pub(crate) var_values: Rc<Vec<SqlValue>>,
}

impl BoundRowContext {
    /// The row's column names. Shared with the cached column lookup rather than
    /// cloned per context build.
    pub(crate) fn column_keys(&self) -> &[String] {
        self.row_lookup.columns()
    }
}

/// A cached verdict for one `(database, query-text)` key in the bound-plan cache
/// fast path (`BICDB_PLAN_CACHE`).
#[derive(Clone)]
pub(crate) struct CachedPointLookup {
    /// Raw routine-variable names in `BTreeMap` order, captured when this verdict
    /// was computed. The `VarId`s baked into a plan's `BoundExpr`s index into this
    /// exact ordered set, so a cached plan is only honored when the current
    /// routine variables present the identical ordered name list — otherwise a
    /// reused plan could bind a `VarId` to a different variable. This is the
    /// ABA-safety guard against two different procedures whose statement text
    /// happens to be identical but whose variable scopes differ.
    pub(crate) var_names: Rc<[String]>,
    /// `Some` = a verified single-table full-primary-key point lookup the fused
    /// `execute_row_query` path reproduces exactly. `None` = this query shape is
    /// known not to be cacheable; the fast path falls straight through without
    /// re-planning on subsequent calls.
    pub(crate) plan: Option<Rc<PointLookupTemplate>>,
}

/// The value-independent part of a single-table full-primary-key point lookup:
/// the resolved table, its schema, the primary-key columns, and the key
/// expressions as `BoundExpr`s referencing routine variables by `VarId`. Reused
/// across calls; per-call variable *values* are supplied fresh at execution.
pub(crate) struct PointLookupTemplate {
    pub(crate) table: String,
    pub(crate) alias: String,
    pub(crate) schema: TableSchema,
    pub(crate) pk_columns: Vec<String>,
    pub(crate) key_exprs: Vec<BoundExpr>,
    /// The fields the statement references (`retain_referenced_fields` over
    /// the wildcard); `output_columns` is derived from exactly this list.
    pub(crate) fields: Vec<FieldRef>,
    pub(crate) output_columns: Vec<String>,
}

/// Plans for routine-IR-owned SELECT nodes, keyed by (database, catalog
/// generation, routine IR, node address). A node's text never changes for
/// the life of its IR, so no SQL rendering or variable-shape check is needed
/// per execution; a recompiled routine or a DDL change moves the generation.
type SqlPlanNodeCache = FxHashMap<(usize, u64, usize, usize), CachedPointLookup>;
const SQL_PLAN_NODE_CACHE_MAX: usize = 65_536;

thread_local! {
    static SQL_PLAN_NODE_CACHE: RefCell<SqlPlanNodeCache> = RefCell::new(FxHashMap::default());
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_IR_PLAN_HITS: RefCell<usize> = const { RefCell::new(0) };
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_BOUND_ROW_FILTER_HITS: RefCell<usize> = const { RefCell::new(0) };
}

/// The value-independent part of an UPDATE whose WHERE is exactly a full
/// primary-key equality (no residual): the key columns and the bound key
/// expressions (routine variables by `VarId`, literals). Keyed like the SELECT
/// plans, by the statement's selection node inside the routine IR.
pub(crate) struct UpdatePointTemplate {
    pub(crate) pk_columns: Vec<String>,
    pub(crate) key_exprs: Vec<BoundExpr>,
    /// The outer (FROM) row layout the key expressions were bound against:
    /// column references are slot indexes into it. Empty for an UPDATE
    /// without FROM; an execution whose layout differs must not use the plan.
    pub(crate) outer_columns: Vec<String>,
}

type SqlUpdatePlanNodeCache =
    FxHashMap<(usize, u64, usize, usize), Option<Rc<UpdatePointTemplate>>>;

thread_local! {
    /// `None` = the node is known not to be a full-primary-key UPDATE.
    static SQL_UPDATE_PLAN_NODE_CACHE: RefCell<SqlUpdatePlanNodeCache> = RefCell::new(FxHashMap::default());
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_IR_UPDATE_PLAN_HITS: RefCell<usize> = const { RefCell::new(0) };
}

/// A routine-owned UPDATE assignment expression bound over the candidate
/// row layout (column references are slot indexes into `columns`) and the
/// routine variables; `None` = the binder declined, the generic evaluator
/// runs.
pub(crate) struct BoundAssignment {
    pub(crate) columns: Vec<String>,
    pub(crate) expr: BoundExpr,
}

type SqlBoundAssignmentCache = FxHashMap<(usize, u64, usize, usize), Option<Rc<BoundAssignment>>>;

thread_local! {
    static SQL_BOUND_ASSIGNMENT_CACHE: RefCell<SqlBoundAssignmentCache> = RefCell::new(FxHashMap::default());
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_IR_BOUND_ASSIGNMENT_HITS: RefCell<usize> = const { RefCell::new(0) };
}

pub(crate) fn sql_bound_assignment_cache_get(
    key: (usize, u64, usize, usize),
) -> Option<Option<Rc<BoundAssignment>>> {
    let hit = SQL_BOUND_ASSIGNMENT_CACHE.with(|cache| cache.borrow().get(&key).cloned());
    #[cfg(test)]
    if hit.is_some() {
        SQL_IR_BOUND_ASSIGNMENT_HITS.with(|hits| *hits.borrow_mut() += 1);
    }
    hit
}

pub(crate) fn sql_bound_assignment_cache_set(
    key: (usize, u64, usize, usize),
    entry: Option<Rc<BoundAssignment>>,
) {
    SQL_BOUND_ASSIGNMENT_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= SQL_PLAN_NODE_CACHE_MAX {
            cache.clear();
        }
        cache.insert(key, entry);
    });
}

/// A routine-owned `INSERT ... VALUES`: everything the statement and the
/// table schema fix ahead of time — each VALUES expression bound over the
/// routine variables, the schema column every target column feeds, the
/// static defaults of the columns the statement leaves out, and where the
/// record id comes from. Built once per IR node; an execution evaluates the
/// expressions and encodes the storage row (`build_record`) without the
/// name-keyed field map, the per-field schema lookups, and the decode of
/// the freshly encoded row that the generic NOT NULL / type checks perform.
/// A routine-owned SELECT over two relations where the WHERE (and ON) terms
/// are exactly full primary-key equalities for both: the first relation's
/// key from the routine variables, the second's from the variables and the
/// first row. Every term is consumed by one of the two lookups, so the join
/// is two point fetches and a merge with no residual predicate, no join
/// planning, no constraint derivation and no engine construction per call.
pub(crate) struct PointJoinTemplate {
    pub(crate) left: PointLookupTemplate,
    /// Key expressions bound over `left.output_columns` (column references
    /// are slot indexes into the left row) and the routine variables.
    pub(crate) right: PointLookupTemplate,
    pub(crate) output_columns: Vec<String>,
}

type SqlJoinPlanNodeCache = FxHashMap<(usize, u64, usize, usize), Option<Rc<PointJoinTemplate>>>;

thread_local! {
    static SQL_JOIN_PLAN_NODE_CACHE: RefCell<SqlJoinPlanNodeCache> = RefCell::new(FxHashMap::default());
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_IR_JOIN_PLAN_HITS: RefCell<usize> = const { RefCell::new(0) };
}

pub(crate) fn sql_join_plan_node_cache_get(
    key: (usize, u64, usize, usize),
) -> Option<Option<Rc<PointJoinTemplate>>> {
    let hit = SQL_JOIN_PLAN_NODE_CACHE.with(|cache| cache.borrow().get(&key).cloned());
    #[cfg(test)]
    if hit.is_some() {
        SQL_IR_JOIN_PLAN_HITS.with(|hits| *hits.borrow_mut() += 1);
    }
    hit
}

pub(crate) fn sql_join_plan_node_cache_set(
    key: (usize, u64, usize, usize),
    entry: Option<Rc<PointJoinTemplate>>,
) {
    SQL_JOIN_PLAN_NODE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= SQL_PLAN_NODE_CACHE_MAX {
            cache.clear();
        }
        cache.insert(key, entry);
    });
}

pub(crate) struct InsertValuesTemplate {
    pub(crate) schema: Arc<TableSchema>,
    /// One plan per schema column, in schema order.
    pub(crate) columns: Vec<InsertColumnPlan>,
    /// `columns` indexes in column-name order: the order the generic path's
    /// name-keyed field map cast and encoded in.
    pub(crate) name_order: Vec<usize>,
    /// Per VALUES row, per target column: the bound expression, or `None`
    /// where the binder declined and the generic evaluator runs.
    pub(crate) rows: Vec<Vec<Option<BoundExpr>>>,
    pub(crate) identity: InsertIdentity,
    pub(crate) omit_primary_key_metadata: bool,
}

pub(crate) struct InsertColumnPlan {
    /// Position in the VALUES row, or `None` when the column takes its
    /// static default (or stays absent).
    pub(crate) source: Option<usize>,
    pub(crate) default: Option<SqlValue>,
    pub(crate) json: bool,
    pub(crate) primary_key: bool,
    pub(crate) needs_not_null: bool,
}

pub(crate) enum InsertIdentity {
    Synthetic,
    /// `columns` index of the single primary-key column.
    Single(usize),
    Composite(Vec<usize>),
}

impl InsertValuesTemplate {
    /// `None` when the statement or the table needs something the template
    /// does not model (generated, vector, geometry, payload, timestamp, user
    /// or oid-alias typed columns; sequence or expression defaults for
    /// omitted columns; DEFAULT values; duplicate or unknown targets).
    pub(crate) fn build(
        schema: &Arc<TableSchema>,
        columns: &[String],
        values: &sqlparser::ast::Values,
        scope: &BoundExprScope,
    ) -> Option<Self> {
        for column in &schema.columns {
            if column.generated_expr.is_some()
                || column.user_type.is_some()
                || column.vector_dim.is_some()
                || column.name == "timestamp"
                || column.name == "payload"
                || column.name.eq_ignore_ascii_case("geometry")
                || is_vector_column(Some(schema), &column.name)
                || is_oid_alias_type(&column.pg_type)
                || column
                    .pg_type
                    .strip_suffix("[]")
                    .is_some_and(is_oid_alias_type)
            {
                return None;
            }
        }
        let mut source_of = vec![None; schema.columns.len()];
        for (position, name) in columns.iter().enumerate() {
            let index = schema
                .columns
                .iter()
                .position(|column| column.name == *name)?;
            if source_of[index].is_some() {
                return None;
            }
            source_of[index] = Some(position);
        }
        for row in &values.rows {
            if row.len() != columns.len() || row.iter().any(expr_is_default) {
                return None;
            }
        }
        let mut plans = Vec::with_capacity(schema.columns.len());
        for (index, column) in schema.columns.iter().enumerate() {
            let source = source_of[index];
            let default = if source.is_none() {
                if column.default_sequence.is_some() || column.effective_default_expr().is_some() {
                    return None;
                }
                column.default_value.clone()
            } else {
                None
            };
            plans.push(InsertColumnPlan {
                source,
                default,
                json: is_json_pg_type(&column.pg_type),
                primary_key: column.primary_key,
                needs_not_null: !column.hidden && (!column.nullable || column.primary_key),
            });
        }
        let primary_key_columns = primary_key_columns_for_schema(schema);
        let primary_key = primary_key_columns
            .iter()
            .map(|name| {
                schema
                    .columns
                    .iter()
                    .position(|column| column.name == *name)
            })
            .collect::<Option<Vec<_>>>()?;
        let identity = if schema.has_hidden_primary_key() || primary_key.is_empty() {
            InsertIdentity::Synthetic
        } else if let [single] = primary_key.as_slice() {
            InsertIdentity::Single(*single)
        } else {
            InsertIdentity::Composite(primary_key.clone())
        };
        let omit_primary_key_metadata = primary_key_columns.len() == 1
            && schema
                .column(&primary_key_columns[0])
                .is_none_or(|column| !pg_type_requires_typed_identity(&column.pg_type));
        let mut name_order = (0..schema.columns.len()).collect::<Vec<_>>();
        name_order.sort_by(|a, b| schema.columns[*a].name.cmp(&schema.columns[*b].name));
        let rows = values
            .rows
            .iter()
            .map(|row| row.iter().map(|expr| scope.bind(expr)).collect())
            .collect();
        Some(Self {
            schema: schema.clone(),
            columns: plans,
            name_order,
            rows,
            identity,
            omit_primary_key_metadata,
        })
    }

    /// The record for one evaluated VALUES row plus the cast column values
    /// (schema order; `None` = absent) `check_row` verifies afterwards.
    pub(crate) fn build_record(
        &self,
        table: &str,
        mut evaluated: Vec<SqlValue>,
    ) -> Result<(Record, Vec<Option<SqlValue>>)> {
        let schema = &*self.schema;
        let mut cells = self
            .columns
            .iter()
            .map(|plan| match plan.source {
                Some(position) => Some(std::mem::replace(&mut evaluated[position], SqlValue::Null)),
                None => plan.default.clone(),
            })
            .collect::<Vec<_>>();
        let mut cast_done = vec![false; cells.len()];
        // The id comes first, as on the generic path (its cast errors
        // surface raw; the other columns' are reported as type mismatches).
        let id = match &self.identity {
            InsertIdentity::Synthetic => synthetic_record_id(table),
            InsertIdentity::Single(slot) => {
                self.identity_cell(table, &mut cells, &mut cast_done, *slot)?
            }
            InsertIdentity::Composite(slots) => {
                let parts = slots
                    .iter()
                    .map(|slot| self.identity_cell(table, &mut cells, &mut cast_done, *slot))
                    .collect::<Result<Vec<_>>>()?;
                serde_json::to_string(&parts)?
            }
        };
        if id.is_empty() {
            return Err(SqlError::InvalidSql(
                "record id must not be empty".to_string(),
            ));
        }
        for &index in &self.name_order {
            if cast_done[index] {
                continue;
            }
            let Some(value) = cells[index].take() else {
                continue;
            };
            let column = &schema.columns[index];
            let value = cast_value_to_column_type(value, column).map_err(|error| match error {
                error @ (SqlError::ConstraintViolation { .. } | SqlError::DataException { .. }) => {
                    error
                }
                error => SqlError::TypeMismatch {
                    table: table.to_string(),
                    column: column.name.clone(),
                    expected: column.pg_type.clone(),
                    message: format!(
                        "column \"{}\" of relation \"{}\" expects type {}: {error}",
                        column.name, table, column.pg_type
                    ),
                },
            })?;
            cells[index] = Some(value);
        }
        let mut metadata = JsonMap::with_capacity(self.columns.len());
        for &index in &self.name_order {
            let plan = &self.columns[index];
            let Some(value) = cells[index].as_ref() else {
                continue;
            };
            // Absence represents SQL NULL for JSON columns; the primary key
            // lives in the record id unless its type needs the typed cell.
            if (plan.json && matches!(value, SqlValue::Null))
                || (self.omit_primary_key_metadata && plan.primary_key)
            {
                continue;
            }
            let column = &schema.columns[index];
            metadata.insert(
                column.name.clone(),
                sql_value_to_column_storage_json(value.clone(), Some(column))?,
            );
        }
        let record = Record::new(id).with_metadata(JsonValue::Object(metadata));
        Ok((record, cells))
    }

    fn identity_cell(
        &self,
        table: &str,
        cells: &mut [Option<SqlValue>],
        cast_done: &mut [bool],
        slot: usize,
    ) -> Result<String> {
        let column = &self.schema.columns[slot];
        let value = cells[slot]
            .take()
            .filter(|value| !matches!(value, SqlValue::Null))
            .ok_or_else(|| {
                SqlError::InvalidSql(format!("INSERT into {table} requires {}", column.name))
            })?;
        let value = cast_value_to_column_type(value, column)?;
        let cell = primary_key_identity_cell(&self.schema, &column.name, &value)?;
        if cell.is_empty() {
            return Err(SqlError::InvalidSql(format!(
                "INSERT into {table} requires {}",
                column.name
            )));
        }
        cells[slot] = Some(value);
        cast_done[slot] = true;
        Ok(cell)
    }

    /// `build` for rows that arrive already evaluated (INSERT ... SELECT):
    /// the same column plans and identity, no expressions to bind. `None`
    /// when the table declines the template.
    pub(crate) fn build_for_rows(schema: &Arc<TableSchema>, columns: &[String]) -> Option<Self> {
        let values = sqlparser::ast::Values {
            explicit_row: false,
            rows: Vec::new(),
            value_keyword: false,
        };
        let scope = BoundExprScope {
            cols: cached_column_lookups(&[]),
            exact_vars: Arc::new(FxHashMap::default()),
            array_vars: Arc::new(FxHashSet::default()),
        };
        Self::build(schema, columns, &values, &scope)
    }

    /// `build_record` producing the row's stored form instead of a `Record`:
    /// the id and the metadata JSON text (members in column-name order, the
    /// canonical `Value` serialization) plus the cast values for the NOT NULL
    /// check. Same casts, same errors, same omissions (JSON NULL absent,
    /// primary key in the id) as `build_record`.
    pub(crate) fn build_stored(
        &self,
        table: &str,
        mut evaluated: Vec<SqlValue>,
    ) -> Result<(String, String, Vec<Option<SqlValue>>)> {
        let schema = &*self.schema;
        let mut cells = self
            .columns
            .iter()
            .map(|plan| match plan.source {
                Some(position) => Some(std::mem::replace(&mut evaluated[position], SqlValue::Null)),
                None => plan.default.clone(),
            })
            .collect::<Vec<_>>();
        let mut cast_done = vec![false; cells.len()];
        let id = match &self.identity {
            InsertIdentity::Synthetic => synthetic_record_id(table),
            InsertIdentity::Single(slot) => {
                self.identity_cell(table, &mut cells, &mut cast_done, *slot)?
            }
            InsertIdentity::Composite(slots) => {
                let parts = slots
                    .iter()
                    .map(|slot| self.identity_cell(table, &mut cells, &mut cast_done, *slot))
                    .collect::<Result<Vec<_>>>()?;
                serde_json::to_string(&parts)?
            }
        };
        if id.is_empty() {
            return Err(SqlError::InvalidSql(
                "record id must not be empty".to_string(),
            ));
        }
        for &index in &self.name_order {
            if cast_done[index] {
                continue;
            }
            let Some(value) = cells[index].take() else {
                continue;
            };
            let column = &schema.columns[index];
            let value = cast_value_to_column_type(value, column).map_err(|error| match error {
                error @ (SqlError::ConstraintViolation { .. } | SqlError::DataException { .. }) => {
                    error
                }
                error => SqlError::TypeMismatch {
                    table: table.to_string(),
                    column: column.name.clone(),
                    expected: column.pg_type.clone(),
                    message: format!(
                        "column \"{}\" of relation \"{}\" expects type {}: {error}",
                        column.name, table, column.pg_type
                    ),
                },
            })?;
            cells[index] = Some(value);
        }
        let mut out = Vec::with_capacity(self.columns.len() * 24 + 2);
        out.push(b'{');
        let mut first = true;
        for &index in &self.name_order {
            let plan = &self.columns[index];
            let Some(value) = cells[index].as_ref() else {
                continue;
            };
            if (plan.json && matches!(value, SqlValue::Null))
                || (self.omit_primary_key_metadata && plan.primary_key)
            {
                continue;
            }
            let column = &schema.columns[index];
            if !first {
                out.push(b',');
            }
            first = false;
            serde_json::to_writer(&mut out, &column.name)?;
            out.push(b':');
            // The envelope goes straight into the row text (no `Value` tree).
            sql_value_write_column_storage(&mut out, value.clone(), Some(column))?;
        }
        out.push(b'}');
        let text = String::from_utf8(out).expect("JSON rendering is UTF-8");
        Ok((id, text, cells))
    }

    /// The NOT NULL check of `check_row` alone (for rows whose table has no
    /// CHECK constraints).
    pub(crate) fn check_cells_not_null(
        &self,
        table: &str,
        cells: &[Option<SqlValue>],
    ) -> Result<()> {
        for (index, (plan, cell)) in self.columns.iter().zip(cells).enumerate() {
            if !plan.needs_not_null {
                continue;
            }
            if cell
                .as_ref()
                .is_none_or(|value| matches!(value, SqlValue::Null))
            {
                let column = &self.schema.columns[index];
                return Err(constraint_violation(
                    "23502",
                    format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        column.name, table
                    ),
                    Some(table.to_string()),
                    Some(column.name.clone()),
                    Some(not_null_constraint_name(table, &column.name)),
                ));
            }
        }
        Ok(())
    }

    /// The local constraints on one built row: NOT NULL over the cast values
    /// (the type check is implied by the cast), then the CHECK constraints.
    pub(crate) fn check_row(
        &self,
        table: &str,
        record: &Record,
        cells: &[Option<SqlValue>],
    ) -> Result<()> {
        for (index, (plan, cell)) in self.columns.iter().zip(cells).enumerate() {
            if !plan.needs_not_null {
                continue;
            }
            if cell
                .as_ref()
                .is_none_or(|value| matches!(value, SqlValue::Null))
            {
                let column = &self.schema.columns[index];
                return Err(constraint_violation(
                    "23502",
                    format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        column.name, table
                    ),
                    Some(table.to_string()),
                    Some(column.name.clone()),
                    Some(not_null_constraint_name(table, &column.name)),
                ));
            }
        }
        validate_checks(table, &self.schema, record)
    }
}

type SqlInsertPlanNodeCache =
    FxHashMap<(usize, u64, usize, usize), Option<Rc<InsertValuesTemplate>>>;

thread_local! {
    static SQL_INSERT_PLAN_NODE_CACHE: RefCell<SqlInsertPlanNodeCache> = RefCell::new(FxHashMap::default());
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_IR_INSERT_PLAN_HITS: RefCell<usize> = const { RefCell::new(0) };
}

pub(crate) fn sql_insert_plan_node_cache_get(
    key: (usize, u64, usize, usize),
) -> Option<Option<Rc<InsertValuesTemplate>>> {
    let hit = SQL_INSERT_PLAN_NODE_CACHE.with(|cache| cache.borrow().get(&key).cloned());
    #[cfg(test)]
    if hit.is_some() {
        SQL_IR_INSERT_PLAN_HITS.with(|hits| *hits.borrow_mut() += 1);
    }
    hit
}

pub(crate) fn sql_insert_plan_node_cache_set(
    key: (usize, u64, usize, usize),
    entry: Option<Rc<InsertValuesTemplate>>,
) {
    SQL_INSERT_PLAN_NODE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= SQL_PLAN_NODE_CACHE_MAX {
            cache.clear();
        }
        cache.insert(key, entry);
    });
}

pub(crate) fn sql_update_plan_node_cache_get(
    key: (usize, u64, usize, usize),
) -> Option<Option<Rc<UpdatePointTemplate>>> {
    let hit = SQL_UPDATE_PLAN_NODE_CACHE.with(|cache| cache.borrow().get(&key).cloned());
    #[cfg(test)]
    if hit.is_some() {
        SQL_IR_UPDATE_PLAN_HITS.with(|hits| *hits.borrow_mut() += 1);
    }
    hit
}

pub(crate) fn sql_update_plan_node_cache_set(
    key: (usize, u64, usize, usize),
    entry: Option<Rc<UpdatePointTemplate>>,
) {
    SQL_UPDATE_PLAN_NODE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= SQL_PLAN_NODE_CACHE_MAX {
            cache.clear();
        }
        cache.insert(key, entry);
    });
}

pub(crate) fn sql_plan_node_cache_get(
    key: (usize, u64, usize, usize),
) -> Option<CachedPointLookup> {
    let hit = SQL_PLAN_NODE_CACHE.with(|cache| cache.borrow().get(&key).cloned());
    #[cfg(test)]
    if hit.is_some() {
        SQL_IR_PLAN_HITS.with(|hits| *hits.borrow_mut() += 1);
    }
    hit
}

pub(crate) fn sql_plan_node_cache_set(key: (usize, u64, usize, usize), entry: CachedPointLookup) {
    SQL_PLAN_NODE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= SQL_PLAN_NODE_CACHE_MAX {
            cache.clear();
        }
        cache.insert(key, entry);
    });
}

/// Per-thread bound-plan cache. Plans are pure functions of (database, query
/// text, routine-variable name shape, schema generation), so a thread-local map
/// is correct and persists across `execute()` calls — and therefore across
/// stored-procedure `CALL` invocations — on the connection's thread (the
/// pgwire layer recreates the `SqlSession` per request, so a session field would
/// not survive). It is dropped whenever the active database pointer or the
/// `SCHEMA_COLLECTION` generation changes, the same invalidation signal the
/// schema cache uses; a long-lived database keeps a stable pointer so a freed
/// pointer can never alias a different live database here.
pub(crate) struct PlanCache {
    pub(crate) db_id: usize,
    pub(crate) schema_generation: u64,
    pub(crate) entries: FxHashMap<String, CachedPointLookup>,
}

thread_local! {
    pub(crate) static SQL_PLAN_CACHE: RefCell<Option<PlanCache>> = const { RefCell::new(None) };
}

pub(crate) fn sql_plan_cache_get(db: &BicDb, sql: &str) -> Option<CachedPointLookup> {
    let db_id = db as *const BicDb as usize;
    let generation = db.collection_generation(SCHEMA_COLLECTION);
    SQL_PLAN_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        match slot.as_mut() {
            Some(cache) if cache.db_id == db_id && cache.schema_generation == generation => {
                cache.entries.get(sql).cloned()
            }
            _ => {
                // Different database or a committed schema change: discard stale
                // plans and start fresh.
                *slot = Some(PlanCache {
                    db_id,
                    schema_generation: generation,
                    entries: FxHashMap::default(),
                });
                None
            }
        }
    })
}

pub(crate) fn sql_plan_cache_set(db: &BicDb, sql: &str, entry: CachedPointLookup) {
    let db_id = db as *const BicDb as usize;
    let generation = db.collection_generation(SCHEMA_COLLECTION);
    SQL_PLAN_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        match slot.as_mut() {
            Some(cache) if cache.db_id == db_id && cache.schema_generation == generation => {
                cache.entries.insert(sql.to_string(), entry);
            }
            _ => {
                let mut entries = FxHashMap::default();
                entries.insert(sql.to_string(), entry);
                *slot = Some(PlanCache {
                    db_id,
                    schema_generation: generation,
                    entries,
                });
            }
        }
    });
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedDynamicRecordLookup {
    PrimaryKeyExact {
        schema: TableSchema,
        columns: Vec<String>,
        values: Vec<PreparedDynamicBoundExpr>,
        covered_terms: BTreeSet<usize>,
        context: BoundRowContext,
    },
    IndexPrefix {
        index: IndexDefinition,
        prefix: Vec<PreparedDynamicBoundExpr>,
        covered_terms: BTreeSet<usize>,
        context: BoundRowContext,
    },
}

pub(crate) struct PreparedPrimaryKeyRightJoin<'a> {
    pub(crate) table: &'a str,
    pub(crate) alias_name: &'a str,
    pub(crate) schema: Option<&'a TableSchema>,
    pub(crate) right_columns: Vec<String>,
    pub(crate) right_fields: Vec<FieldRef>,
    pub(crate) kind: RowJoinKind,
    pub(crate) lookup: &'a PreparedDynamicRecordLookup,
    pub(crate) residual_constraint: &'a JoinConstraint,
}

impl PreparedDynamicRecordLookup {
    pub(crate) fn prefix_len(&self) -> usize {
        match self {
            Self::PrimaryKeyExact { columns, .. } => columns.len(),
            Self::IndexPrefix { prefix, .. } => prefix.len(),
        }
    }

    pub(crate) fn covered_terms(&self) -> &BTreeSet<usize> {
        match self {
            Self::PrimaryKeyExact { covered_terms, .. }
            | Self::IndexPrefix { covered_terms, .. } => covered_terms,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedDynamicBoundExpr {
    pub(crate) expr: BoundExpr,
    pub(crate) term_idx: usize,
}

pub(crate) fn lookup_covered_terms(bounds: &[PreparedDynamicBoundExpr]) -> BTreeSet<usize> {
    bounds.iter().map(|bound| bound.term_idx).collect()
}

pub(crate) fn residual_join_constraint_after_lookup(
    selection: &Expr,
    lookup: &PreparedDynamicRecordLookup,
) -> JoinConstraint {
    let covered_terms = lookup.covered_terms();
    let residual_terms = and_terms(selection)
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| !covered_terms.contains(idx))
        .map(|(_, term)| term.clone())
        .collect::<Vec<_>>();
    expr_from_and_terms(residual_terms)
        .map(JoinConstraint::On)
        .unwrap_or(JoinConstraint::None)
}

pub(crate) fn expr_from_and_terms(terms: Vec<Expr>) -> Option<Expr> {
    terms.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    })
}

#[allow(dead_code)]
impl BoundExprScope {
    pub(crate) fn new(columns: &[String], vars: &[String]) -> Self {
        let mut exact_vars = FxHashMap::with_capacity_and_hasher(vars.len(), Default::default());
        for (idx, var) in vars.iter().enumerate() {
            exact_vars.insert(normalize_object_name(var), VarId(idx));
        }

        Self {
            cols: cached_column_lookups(columns),
            exact_vars: Arc::new(exact_vars),
            array_vars: Arc::new(FxHashSet::default()),
        }
    }

    /// Mark the variables (by name) declared with an array type.
    pub(crate) fn with_array_vars(mut self, names: &[String]) -> Self {
        let array_vars = names
            .iter()
            .filter_map(|name| self.var_id(name))
            .collect::<FxHashSet<_>>();
        self.array_vars = Arc::new(array_vars);
        self
    }

    pub(crate) fn bind(&self, expr: &Expr) -> Option<BoundExpr> {
        let mut ctx = BindCtx {
            calls: None,
            hoistable: false,
            validate_case: None,
        };
        self.bind_in(expr, &mut ctx)
    }

    pub(crate) fn bind_with_case_validator(
        &self,
        expr: &Expr,
        validate_case: &dyn Fn(&[sqlparser::ast::CaseWhen], Option<&Expr>) -> bool,
    ) -> Option<BoundExpr> {
        self.bind_in(
            expr,
            &mut BindCtx {
                calls: None,
                hoistable: false,
                validate_case: Some(validate_case),
            },
        )
    }

    /// `bind` for a routine expression: non-builtin function calls are hoisted
    /// (see `BoundUserCall`) instead of declining the whole expression. `None`
    /// exactly when `bind` would decline or when hoisting would reorder a
    /// volatile builtin relative to the calls.
    pub(crate) fn bind_with_user_calls(
        &self,
        expr: &Expr,
    ) -> Option<(BoundExpr, Vec<BoundUserCall>)> {
        let mut ctx = BindCtx {
            calls: Some(Vec::new()),
            hoistable: true,
            validate_case: None,
        };
        let bound = self.bind_in(expr, &mut ctx)?;
        let calls = ctx.calls.take().unwrap_or_default();
        if !calls.is_empty()
            && (bound_expr_calls_builtin(&bound, "random")
                || calls.iter().any(|call| {
                    call.args
                        .iter()
                        .any(|arg| bound_expr_calls_builtin(arg, "random"))
                }))
        {
            return None;
        }
        Some((bound, calls))
    }

    fn bind_short_circuit_operand(&self, expr: &Expr, ctx: &mut BindCtx) -> Option<BoundExpr> {
        let hoistable = std::mem::replace(&mut ctx.hoistable, false);
        let bound = self.bind_in(expr, ctx);
        ctx.hoistable = hoistable;
        bound
    }

    fn bind_user_call(&self, function: &Function, ctx: &mut BindCtx) -> Option<BoundExpr> {
        if ctx.calls.is_none()
            || !ctx.hoistable
            || function.filter.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
            || function.null_treatment.is_some()
            || function.parameters != FunctionArguments::None
        {
            return None;
        }
        let args = match &function.args {
            FunctionArguments::None => Vec::new(),
            FunctionArguments::List(list) => {
                if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                    return None;
                }
                list.args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            self.bind_in(expr, ctx)
                        }
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?
            }
            FunctionArguments::Subquery(_) => return None,
        };
        let name = object_name_lowercase(&function.name).ok()?.into_owned();
        let calls = ctx.calls.as_mut()?;
        let slot = calls.len();
        calls.push(BoundUserCall {
            node: function as *const Function as usize,
            name,
            args,
        });
        Some(BoundExpr::UserCall(slot))
    }

    fn bind_in(&self, expr: &Expr, ctx: &mut BindCtx) -> Option<BoundExpr> {
        match expr {
            Expr::Identifier(ident) => self.bind_identifier(std::slice::from_ref(&ident.value)),
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                self.bind_identifier(&parts)
            }
            Expr::Value(value) => match &value.value {
                Value::Placeholder(name) => self.var_id(name).map(BoundExpr::Var),
                _ => literal_to_value(value).ok().map(BoundExpr::Literal),
            },
            Expr::TypedString(value) => typed_string_to_value(value).ok().map(BoundExpr::Literal),
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Some(BoundExpr::And(
                    Box::new(self.bind_short_circuit_operand(left, ctx)?),
                    Box::new(self.bind_short_circuit_operand(right, ctx)?),
                )),
                BinaryOperator::Or => Some(BoundExpr::Or(
                    Box::new(self.bind_short_circuit_operand(left, ctx)?),
                    Box::new(self.bind_short_circuit_operand(right, ctx)?),
                )),
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                    if !expr_may_need_tuple_comparison(left)
                        && !expr_may_need_tuple_comparison(right) =>
                {
                    Some(BoundExpr::Compare {
                        left: Box::new(self.bind_in(left, ctx)?),
                        op: op.clone(),
                        right: Box::new(self.bind_in(right, ctx)?),
                    })
                }
                BinaryOperator::Arrow | BinaryOperator::LongArrow => None,
                _ if bound_expr_supports_binary_operator(op) => Some(BoundExpr::Binary {
                    left: Box::new(self.bind_in(left, ctx)?),
                    op: op.clone(),
                    right: Box::new(self.bind_in(right, ctx)?),
                }),
                _ => None,
            },
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                if regclass_display_cast_source(inner, data_type).is_some()
                    || regclass_text_cast_source(inner, data_type)
                        .ok()
                        .flatten()
                        .is_some()
                    || regtype_text_cast_source(inner, data_type)
                        .ok()
                        .flatten()
                        .is_some()
                    || pg_type_from_data_type(data_type).is_ok_and(|(pg_type, _)| {
                        matches!(pg_type.as_str(), "text" | "varchar" | "bpchar" | "name")
                    })
                {
                    None
                } else {
                    Some(BoundExpr::Cast {
                        expr: Box::new(self.bind_in(inner, ctx)?),
                        data_type: data_type.clone(),
                    })
                }
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                let [AccessExpr::Subscript(subscript @ Subscript::Index { index })] =
                    access_chain.as_slice()
                else {
                    return None;
                };
                let Expr::Identifier(ident) = root.as_ref() else {
                    return None;
                };
                let array = self.bind_identifier(std::slice::from_ref(&ident.value))?;
                let BoundExpr::Var(var) = array else {
                    return None;
                };
                if !self.array_vars.contains(&var) {
                    return None;
                }
                Some(BoundExpr::Subscript {
                    array: Box::new(array),
                    index: Box::new(self.bind_in(index, ctx)?),
                    subscript: Box::new(subscript.clone()),
                })
            }
            Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => self.bind_in(inner, ctx),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if !ctx
                    .validate_case
                    .is_some_and(|validate| validate(conditions, else_result.as_deref()))
                {
                    return None;
                }
                let operand = match operand {
                    Some(expr) => Some(Box::new(self.bind_short_circuit_operand(expr, ctx)?)),
                    None => None,
                };
                let conditions = conditions
                    .iter()
                    .map(|branch| {
                        Some((
                            self.bind_short_circuit_operand(&branch.condition, ctx)?,
                            self.bind_short_circuit_operand(&branch.result, ctx)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?;
                let else_result = match else_result {
                    Some(expr) => Some(Box::new(self.bind_short_circuit_operand(expr, ctx)?)),
                    None => None,
                };
                Some(BoundExpr::Case {
                    operand,
                    conditions,
                    else_result,
                })
            }
            Expr::Function(function) => {
                let Some(name) = bound_builtin_name(&function.name) else {
                    return self.bind_user_call(function, ctx);
                };
                // Names with type-dispatched overloads (trunc on macaddr) bind
                // only when the argument's shape rules the overload out: a
                // bare column or variable could carry any type.
                if name == "trunc" {
                    if let FunctionArguments::List(list) = &function.args {
                        if list.args.iter().any(|arg| {
                            matches!(
                                arg,
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(_)
                                        | Expr::CompoundIdentifier(_)
                                        | Expr::Value(ValueWithSpan {
                                            value: Value::Placeholder(_),
                                            ..
                                        })
                                ))
                            )
                        }) {
                            return None;
                        }
                    }
                }
                if function.filter.is_some()
                    || function.over.is_some()
                    || !function.within_group.is_empty()
                    || function.null_treatment.is_some()
                {
                    return None;
                }
                let args = match &function.args {
                    FunctionArguments::None => Vec::new(),
                    FunctionArguments::List(list) => {
                        if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                            return None;
                        }
                        list.args
                            .iter()
                            .map(|arg| match arg {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                    self.bind_in(expr, ctx)
                                }
                                _ => None,
                            })
                            .collect::<Option<Vec<_>>>()?
                    }
                    FunctionArguments::Subquery(_) => return None,
                };
                Some(BoundExpr::Call { name, args })
            }
            Expr::UnaryOp { op, expr: inner } if op.to_string().eq_ignore_ascii_case("NOT") => {
                Some(BoundExpr::UnaryNot(Box::new(self.bind_in(inner, ctx)?)))
            }
            Expr::UnaryOp { op, expr: inner } if op.to_string() == "-" => {
                Some(BoundExpr::UnaryMinus(Box::new(self.bind_in(inner, ctx)?)))
            }
            Expr::UnaryOp { op, expr: inner } if op.to_string() == "+" => self.bind_in(inner, ctx),
            Expr::IsNull(inner) => Some(BoundExpr::IsNull {
                expr: Box::new(self.bind_in(inner, ctx)?),
                negated: false,
            }),
            Expr::IsNotNull(inner) => Some(BoundExpr::IsNull {
                expr: Box::new(self.bind_in(inner, ctx)?),
                negated: true,
            }),
            _ => None,
        }
    }

    pub(crate) fn bind_identifier(&self, parts: &[String]) -> Option<BoundExpr> {
        self.column_id(parts)
            .map(BoundExpr::Column)
            .or_else(|| self.var_id_for_parts(parts).map(BoundExpr::Var))
    }

    pub(crate) fn column_id(&self, parts: &[String]) -> Option<ColumnId> {
        let [single] = parts else {
            let full = parts.join(".");
            if let Some(id) = self.cols.exact.get(&full.to_ascii_lowercase()) {
                return Some(*id);
            }
            if parts.len() >= 2 {
                let qualified = format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
                return self
                    .cols
                    .exact
                    .get(&qualified.to_ascii_lowercase())
                    .copied();
            }
            return None;
        };
        let lower = single.to_ascii_lowercase();
        self.cols
            .exact
            .get(&lower)
            .copied()
            .or_else(|| self.cols.scope_unqualified.get(&lower).copied().flatten())
    }

    pub(crate) fn var_id_for_parts(&self, parts: &[String]) -> Option<VarId> {
        let [single] = parts else {
            return None;
        };
        self.var_id(single)
    }

    pub(crate) fn var_id(&self, name: &str) -> Option<VarId> {
        self.exact_vars.get(&normalize_object_name(name)).copied()
    }
}

pub(crate) fn bound_expr_supports_binary_operator(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::StringConcat
            | BinaryOperator::PGLikeMatch
            | BinaryOperator::PGILikeMatch
            | BinaryOperator::PGNotLikeMatch
            | BinaryOperator::PGNotILikeMatch
            | BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch
            | BinaryOperator::AtArrow
            | BinaryOperator::ArrowAt
    )
}

/// Per-expression values borrow their input or own only a computed result.
/// Decimal arithmetic retains its coefficient/scale between arithmetic nodes,
/// eliminating repeated numeric String formatting and parsing. This is not a
/// resident row cache: it adds no bytes to stored records or version chains.
enum BoundValue<'a> {
    Scalar(std::borrow::Cow<'a, SqlValue>),
    Decimal(crate::eval::SmallDecimal),
}

impl<'a> BoundValue<'a> {
    fn borrowed(value: &'a SqlValue) -> Self {
        Self::Scalar(std::borrow::Cow::Borrowed(value))
    }

    fn into_cow(self) -> std::borrow::Cow<'a, SqlValue> {
        match self {
            Self::Scalar(value) => value,
            Self::Decimal(value) => {
                std::borrow::Cow::Owned(SqlValue::String(crate::eval::small_decimal_format(value)))
            }
        }
    }

    fn decimal(&self) -> Option<crate::eval::SmallDecimal> {
        match self {
            Self::Decimal(value) => Some(*value),
            Self::Scalar(value) => crate::eval::small_decimal_value(value),
        }
    }

    fn is_integer(&self) -> bool {
        matches!(self, Self::Scalar(value) if matches!(value.as_ref(), SqlValue::Int(_)))
    }

    fn binary(self, op: &BinaryOperator, right: Self) -> Result<Self> {
        if matches!(
            op,
            BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
        ) && !(self.is_integer() && right.is_integer())
        {
            if let Some(result) = self
                .decimal()
                .zip(right.decimal())
                .and_then(|(left, right)| crate::eval::small_decimal_result(left, op, right))
            {
                return Ok(Self::Decimal(result));
            }
        }
        eval_binary_borrowed(self.into_cow(), op, right.into_cow())
            .map(|value| Self::Scalar(std::borrow::Cow::Owned(value)))
    }
}

#[allow(dead_code)]
impl BoundExpr {
    pub(crate) fn eval(&self, frame: &BoundExprFrame<'_>) -> Result<SqlValue> {
        self.eval_borrowed(frame).map(std::borrow::Cow::into_owned)
    }

    /// Inputs stay borrowed through operators that only inspect them. Only
    /// values crossing an ownership boundary (assignment/output) are cloned.
    fn eval_borrowed<'a>(
        &'a self,
        frame: &'a BoundExprFrame<'_>,
    ) -> Result<std::borrow::Cow<'a, SqlValue>> {
        self.eval_value(frame).map(BoundValue::into_cow)
    }

    fn eval_value<'a>(&'a self, frame: &'a BoundExprFrame<'_>) -> Result<BoundValue<'a>> {
        use std::borrow::Cow;
        let value = match self {
            Self::Column(id) => return Ok(BoundValue::borrowed(frame.column_ref(*id))),
            Self::Var(id) => {
                return Ok(BoundValue::borrowed(
                    frame.vars.get(id.0).unwrap_or(&SqlValue::Null),
                ))
            }
            Self::Literal(value) => return Ok(BoundValue::borrowed(value)),
            Self::Subscript {
                array,
                index,
                subscript,
            } => eval_single_index_value(array.eval(frame)?, subscript, index.eval(frame)?),
            Self::UserCall(slot) => {
                return frame
                    .user_calls
                    .get(*slot)
                    .map(BoundValue::borrowed)
                    .ok_or_else(|| {
                        SqlError::Unsupported(
                            "hoisted routine call evaluated without its result".into(),
                        )
                    })
            }
            Self::Binary { left, op, right } => {
                return left.eval_value(frame)?.binary(op, right.eval_value(frame)?);
            }
            Self::Compare { .. } | Self::And(_, _) | Self::Or(_, _) | Self::UnaryNot(_) => self
                .eval_truth(frame)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Self::Cast { expr, data_type } => {
                cast_value_with_db(frame.db, expr.eval(frame)?, data_type)
            }
            Self::UnaryMinus(expr) => negate_numeric_value(expr.eval(frame)?),
            Self::Case {
                operand,
                conditions,
                else_result,
            } => {
                let operand = operand
                    .as_ref()
                    .map(|expr| expr.eval_borrowed(frame))
                    .transpose()?;
                for (condition, result) in conditions {
                    let matched = match &operand {
                        Some(value) => {
                            values_equal(value, condition.eval_borrowed(frame)?.as_ref())
                        }
                        None => condition.eval_truth(frame)?.unwrap_or(false),
                    };
                    if matched {
                        return result.eval_value(frame);
                    }
                }
                return else_result
                    .as_ref()
                    .map(|expr| expr.eval_value(frame))
                    .unwrap_or(Ok(BoundValue::borrowed(&SqlValue::Null)));
            }
            Self::IsNull { .. } => self
                .eval_truth(frame)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Self::Call { name, args } => {
                // Argument lists are short; keep them off the heap.
                let mut values: smallvec::SmallVec<[SqlValue; 4]> =
                    smallvec::SmallVec::with_capacity(args.len());
                for arg in args {
                    values.push(arg.eval(frame)?);
                }
                eval_compatibility_function_value(name, &values, None)?
                    .ok_or_else(|| SqlError::Unsupported(format!("unsupported function {name}")))
            }
        };
        value.map(|value| BoundValue::Scalar(Cow::Owned(value)))
    }

    pub(crate) fn eval_truth(&self, frame: &BoundExprFrame<'_>) -> Result<Option<bool>> {
        match self {
            Self::Compare { left, op, right } => compare_values(
                left.eval_borrowed(frame)?.as_ref(),
                op,
                right.eval_borrowed(frame)?.as_ref(),
            ),
            Self::And(left, right) => {
                let left = left.eval_truth(frame)?;
                if matches!(left, Some(false)) {
                    return Ok(Some(false));
                }
                Ok(sql_and(left, right.eval_truth(frame)?))
            }
            Self::Or(left, right) => {
                let left = left.eval_truth(frame)?;
                if matches!(left, Some(true)) {
                    return Ok(Some(true));
                }
                Ok(sql_or(left, right.eval_truth(frame)?))
            }
            Self::UnaryNot(expr) => Ok(sql_not(expr.eval_truth(frame)?)),
            Self::IsNull { expr, negated } => {
                let value = expr.eval_borrowed(frame)?;
                Ok(Some(if *negated {
                    value_is_not_null_predicate(&value)
                } else {
                    value_is_null_predicate(&value)
                }))
            }
            _ => sql_value_truth(self.eval(frame)?),
        }
    }
}

#[allow(dead_code)]
impl BoundExprFrame<'_> {
    pub(crate) fn column(&self, id: ColumnId) -> SqlValue {
        self.column_ref(id).clone()
    }

    fn column_ref(&self, id: ColumnId) -> &SqlValue {
        match self.columns {
            BoundExprColumns::Values(values) => values.get(id.0).unwrap_or(&SqlValue::Null),
            BoundExprColumns::Joined {
                left,
                right,
                sources,
            } => sources
                .get(id.0)
                .and_then(|source| match source {
                    JoinedSlotSource::Left(idx) => left.get(*idx),
                    JoinedSlotSource::Right(idx) => right.get(*idx),
                })
                .unwrap_or(&SqlValue::Null),
            BoundExprColumns::Row { row, column_keys } => column_keys
                .get(id.0)
                .and_then(|key| row.get(key))
                .unwrap_or(&SqlValue::Null),
        }
    }

    pub(crate) fn var(&self, id: VarId) -> SqlValue {
        self.vars.get(id.0).cloned().unwrap_or(SqlValue::Null)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PgDumpRelationInventoryAliases {
    pub(crate) class: String,
    pub(crate) depend: String,
    pub(crate) tablespace: String,
    pub(crate) access_method: String,
    pub(crate) toast_class: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PgDumpColumnInfoAliases {
    pub(crate) attribute: String,
    pub(crate) attrelids: BTreeSet<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct PgDumpConstraintInventoryAliases {
    pub(crate) constraint: String,
    pub(crate) conrelids: BTreeSet<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct PgDumpProcInventoryAliases {
    pub(crate) proc: String,
    pub(crate) init_privs: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PgDumpProcInventoryKind {
    Aggregate,
    Function,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PgDumpConstraintInventoryKind {
    Check,
    ForeignKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowJoinKind {
    Inner,
    Left,
    Right,
    FullOuter,
}

pub(crate) fn equi_join_key_exprs<'a>(
    constraint: &'a JoinConstraint,
    left_columns: &[String],
    right_columns: &[String],
) -> Option<(&'a Expr, &'a Expr)> {
    let JoinConstraint::On(expr) = constraint else {
        return None;
    };
    let left_column_set = left_columns.iter().cloned().collect::<BTreeSet<_>>();
    let right_column_set = right_columns.iter().cloned().collect::<BTreeSet<_>>();
    fn find_key<'a>(
        expr: &'a Expr,
        left_columns: &BTreeSet<String>,
        right_columns: &BTreeSet<String>,
    ) -> Option<(&'a Expr, &'a Expr)> {
        match unwrap_nested_expr(expr) {
            // An equality in AND is necessary for every matching pair. The
            // hash join still evaluates the complete ON condition for each
            // candidate; neither residual terms nor duplicate keys are lost.
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => find_key(left, left_columns, right_columns)
                .or_else(|| find_key(right, left_columns, right_columns)),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                if predicate_references_only_columns(left, left_columns)
                    && predicate_references_only_columns(right, right_columns)
                {
                    Some((left, right))
                } else if predicate_references_only_columns(left, right_columns)
                    && predicate_references_only_columns(right, left_columns)
                {
                    Some((right, left))
                } else {
                    None
                }
            }
            // In particular, no equality underneath OR is a necessary key.
            _ => None,
        }
    }
    find_key(expr, &left_column_set, &right_column_set)
}

pub(crate) fn join_constraint_can_eval_with_outer_row(
    constraint: &JoinConstraint,
    left_columns: &[String],
    right_columns: &[String],
) -> bool {
    let JoinConstraint::On(expr) = constraint else {
        return false;
    };
    let left_unqualified = unqualified_column_names(left_columns);
    let right_unqualified = unqualified_column_names(right_columns);
    let qualified_columns = left_columns
        .iter()
        .chain(right_columns.iter())
        .cloned()
        .collect::<Vec<_>>();
    expr_can_eval_with_outer_row(
        expr,
        &left_unqualified,
        &right_unqualified,
        &qualified_columns,
    )
}

pub(crate) fn unqualified_column_names(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| column.rsplit('.').next().unwrap_or(column).to_string())
        .collect()
}

pub(crate) fn expr_can_eval_with_outer_row(
    expr: &Expr,
    left_unqualified: &[String],
    right_unqualified: &[String],
    qualified_columns: &[String],
) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            !column_name_in_both_sides(&ident.value, left_unqualified, right_unqualified)
        }
        Expr::CompoundIdentifier(idents) => {
            let parts = idents
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>();
            if parts.len() == 1 {
                return !column_name_in_both_sides(&parts[0], left_unqualified, right_unqualified);
            }
            let full_key = parts.join(".");
            qualified_columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(&full_key))
        }
        Expr::Value(_) | Expr::TypedString(_) | Expr::Interval(_) => true,
        Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Collate { expr, .. } => expr_can_eval_with_outer_row(
            expr,
            left_unqualified,
            right_unqualified,
            qualified_columns,
        ),
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            expr_can_eval_with_outer_row(
                left,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            ) && expr_can_eval_with_outer_row(
                right,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            )
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_can_eval_with_outer_row(
                expr,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            ) && expr_can_eval_with_outer_row(
                low,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            ) && expr_can_eval_with_outer_row(
                high,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            )
        }
        Expr::InList { expr, list, .. } => {
            expr_can_eval_with_outer_row(
                expr,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            ) && list.iter().all(|item| {
                expr_can_eval_with_outer_row(
                    item,
                    left_unqualified,
                    right_unqualified,
                    qualified_columns,
                )
            })
        }
        Expr::Function(function) => function_args(function).iter().all(|arg| {
            expr_can_eval_with_outer_row(
                arg,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            )
        }),
        Expr::Array(array) => array.elem.iter().all(|arg| {
            expr_can_eval_with_outer_row(
                arg,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            )
        }),
        Expr::Position { expr, r#in } => {
            expr_can_eval_with_outer_row(
                expr,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            ) && expr_can_eval_with_outer_row(
                r#in,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            )
        }
        Expr::Extract { expr, .. } => expr_can_eval_with_outer_row(
            expr,
            left_unqualified,
            right_unqualified,
            qualified_columns,
        ),
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            expr_can_eval_with_outer_row(
                expr,
                left_unqualified,
                right_unqualified,
                qualified_columns,
            ) && trim_what.as_ref().is_none_or(|expr| {
                expr_can_eval_with_outer_row(
                    expr,
                    left_unqualified,
                    right_unqualified,
                    qualified_columns,
                )
            }) && trim_characters.as_ref().is_none_or(|exprs| {
                exprs.iter().all(|expr| {
                    expr_can_eval_with_outer_row(
                        expr,
                        left_unqualified,
                        right_unqualified,
                        qualified_columns,
                    )
                })
            })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_none_or(|expr| {
                expr_can_eval_with_outer_row(
                    expr,
                    left_unqualified,
                    right_unqualified,
                    qualified_columns,
                )
            }) && conditions.iter().all(|condition| {
                expr_can_eval_with_outer_row(
                    &condition.condition,
                    left_unqualified,
                    right_unqualified,
                    qualified_columns,
                ) && expr_can_eval_with_outer_row(
                    &condition.result,
                    left_unqualified,
                    right_unqualified,
                    qualified_columns,
                )
            }) && else_result.as_ref().is_none_or(|expr| {
                expr_can_eval_with_outer_row(
                    expr,
                    left_unqualified,
                    right_unqualified,
                    qualified_columns,
                )
            })
        }
        _ => false,
    }
}

pub(crate) fn column_name_in_both_sides(
    name: &str,
    left_unqualified: &[String],
    right_unqualified: &[String],
) -> bool {
    left_unqualified
        .iter()
        .any(|column| column.eq_ignore_ascii_case(name))
        && right_unqualified
            .iter()
            .any(|column| column.eq_ignore_ascii_case(name))
}

pub(crate) fn selection_with_join_constraint(
    selection: Option<&Expr>,
    constraint: &JoinConstraint,
) -> Option<Expr> {
    let JoinConstraint::On(join_expr) = constraint else {
        return selection.cloned();
    };
    Some(match selection {
        Some(selection) => Expr::BinaryOp {
            left: Box::new(selection.clone()),
            op: BinaryOperator::And,
            right: Box::new(join_expr.clone()),
        },
        None => join_expr.clone(),
    })
}

pub(crate) fn selection_with_join_transitive_predicates(
    selection: Option<&Expr>,
    target: &TableFactor,
    joins: &[Join],
) -> Result<Option<Expr>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let Some(qualifiers) = table_factor_qualifiers(target)? else {
        return Ok(None);
    };
    let mut derived = None;
    for join in joins {
        let Some(JoinConstraint::On(join_expr)) = join_operator_constraint(&join.join_operator)
        else {
            continue;
        };
        for term in and_terms(join_expr) {
            let Some(predicate) = transitive_join_predicate(selection, term, &qualifiers) else {
                continue;
            };
            derived = Some(match derived {
                Some(existing) => and_expr(existing, predicate),
                None => predicate,
            });
        }
    }
    Ok(derived.map(|derived| and_expr(selection.clone(), derived)))
}

pub(crate) fn join_operator_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint) => Some(constraint),
        _ => None,
    }
}

pub(crate) fn is_left_join_operator(operator: &JoinOperator) -> bool {
    matches!(operator, JoinOperator::Left(_) | JoinOperator::LeftOuter(_))
}

pub(crate) fn odoo_auto_install_dependency_query_matches(
    select: &Select,
    from: &TableWithJoins,
) -> Result<bool> {
    if !from.joins.is_empty() {
        return Ok(false);
    }
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &from.relation
    else {
        return Ok(false);
    };
    if !relation_name(name)?.eq_ignore_ascii_case("ir_module_module") {
        return Ok(false);
    }
    let module_alias = alias
        .as_ref()
        .map(|alias| alias.name.value.as_str())
        .unwrap_or("ir_module_module");
    if !select
        .projection
        .iter()
        .any(|item| select_item_projects_column(item, module_alias, "name"))
    {
        return Ok(false);
    }
    let Some(selection) = select.selection.as_ref() else {
        return Ok(false);
    };
    Ok(and_terms(selection).into_iter().any(|term| {
        matches!(
            unwrap_nested_expr(term),
            Expr::Exists {
                subquery,
                negated: true,
            } if odoo_auto_install_dependency_subquery_matches(subquery).unwrap_or(false)
        )
    }))
}

pub(crate) fn odoo_auto_install_dependency_subquery_matches(query: &Query) -> Result<bool> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(false);
    };
    let [from] = select.from.as_slice() else {
        return Ok(false);
    };
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &from.relation
    else {
        return Ok(false);
    };
    if !relation_name(name)?.eq_ignore_ascii_case("ir_module_module_dependency") {
        return Ok(false);
    }
    let dependency_alias = alias
        .as_ref()
        .map(|alias| alias.name.value.as_str())
        .unwrap_or("ir_module_module_dependency");
    let [join] = from.joins.as_slice() else {
        return Ok(false);
    };
    if !is_left_join_operator(&join.join_operator) {
        return Ok(false);
    }
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &join.relation
    else {
        return Ok(false);
    };
    if !relation_name(name)?.eq_ignore_ascii_case("ir_module_module") {
        return Ok(false);
    }
    let module_dependency_alias = alias
        .as_ref()
        .map(|alias| alias.name.value.as_str())
        .unwrap_or("ir_module_module");
    let Some(selection) = select.selection.as_ref() else {
        return Ok(false);
    };
    let mut saw_module_id = false;
    let mut saw_missing_dependency = false;
    let mut saw_required_state = false;
    for term in and_terms(selection) {
        if expr_has_column_equality(
            term,
            dependency_alias,
            "ir_module_module_dependency",
            "module_id",
            "m",
            "ir_module_module",
            "id",
        ) {
            saw_module_id = true;
            continue;
        }
        if odoo_dependency_blocking_term_matches(term, module_dependency_alias, dependency_alias) {
            saw_missing_dependency = true;
            saw_required_state = true;
        }
    }
    Ok(saw_module_id && saw_missing_dependency && saw_required_state)
}

pub(crate) fn odoo_dependency_blocking_term_matches(
    expr: &Expr,
    module_dependency_alias: &str,
    dependency_alias: &str,
) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Or,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    (odoo_dependency_missing_term_matches(left, module_dependency_alias)
        && odoo_dependency_required_state_term_matches(
            right,
            module_dependency_alias,
            dependency_alias,
        ))
        || (odoo_dependency_missing_term_matches(right, module_dependency_alias)
            && odoo_dependency_required_state_term_matches(
                left,
                module_dependency_alias,
                dependency_alias,
            ))
}

pub(crate) fn odoo_dependency_missing_term_matches(
    expr: &Expr,
    module_dependency_alias: &str,
) -> bool {
    let Expr::IsNull(expr) = unwrap_nested_expr(expr) else {
        return false;
    };
    column_ref_matches_table_field(expr, module_dependency_alias, "ir_module_module", "id")
}

pub(crate) fn odoo_dependency_required_state_term_matches(
    expr: &Expr,
    module_dependency_alias: &str,
    dependency_alias: &str,
) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    (column_ref_matches_table_field(
        left,
        dependency_alias,
        "ir_module_module_dependency",
        "auto_install_required",
    ) && odoo_dependency_state_not_to_install_matches(right, module_dependency_alias))
        || (column_ref_matches_table_field(
            right,
            dependency_alias,
            "ir_module_module_dependency",
            "auto_install_required",
        ) && odoo_dependency_state_not_to_install_matches(left, module_dependency_alias))
}

pub(crate) fn odoo_dependency_state_not_to_install_matches(
    expr: &Expr,
    module_dependency_alias: &str,
) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::NotEq,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    (column_ref_matches_table_field(left, module_dependency_alias, "ir_module_module", "state")
        && sql_literal_text(right).is_some_and(|value| value.eq_ignore_ascii_case("to install")))
        || (column_ref_matches_table_field(
            right,
            module_dependency_alias,
            "ir_module_module",
            "state",
        ) && sql_literal_text(left)
            .is_some_and(|value| value.eq_ignore_ascii_case("to install")))
}

pub(crate) fn select_item_projects_column(item: &SelectItem, alias: &str, field: &str) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => column_ref_matches_table_field(expr, alias, alias, field),
        SelectItem::ExprWithAlias { expr, .. } => {
            column_ref_matches_table_field(expr, alias, alias, field)
        }
        _ => false,
    }
}

pub(crate) fn column_ref_matches_table_field(
    expr: &Expr,
    alias: &str,
    table: &str,
    field: &str,
) -> bool {
    let Some(column) = column_ref_from_expr(expr) else {
        return false;
    };
    if !column.field.eq_ignore_ascii_case(field) {
        return false;
    }
    column.qualifier.is_none_or(|qualifier| {
        qualifier.eq_ignore_ascii_case(alias) || qualifier.eq_ignore_ascii_case(table)
    })
}

pub(crate) fn sql_literal_text(expr: &Expr) -> Option<String> {
    match unwrap_nested_expr(expr) {
        Expr::Value(value) => literal_to_value(value)
            .ok()
            .and_then(|value| sql_value_text(&value)),
        Expr::TypedString(value) => typed_string_to_value(value)
            .ok()
            .and_then(|value| sql_value_text(&value)),
        _ => None,
    }
}

pub(crate) fn transitive_join_predicate(
    selection: &Expr,
    join_term: &Expr,
    target_qualifiers: &[String],
) -> Option<Expr> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested_expr(join_term)
    else {
        return None;
    };
    if column_expr_qualifier_matches(left, target_qualifiers) {
        if let Some(value) = selection_literal_for_column(selection, right) {
            return Some(Expr::BinaryOp {
                left: Box::new((**left).clone()),
                op: BinaryOperator::Eq,
                right: Box::new(value),
            });
        }
    }
    if column_expr_qualifier_matches(right, target_qualifiers) {
        if let Some(value) = selection_literal_for_column(selection, left) {
            return Some(Expr::BinaryOp {
                left: Box::new((**right).clone()),
                op: BinaryOperator::Eq,
                right: Box::new(value),
            });
        }
    }
    None
}

pub(crate) fn selection_literal_for_column(selection: &Expr, column: &Expr) -> Option<Expr> {
    for term in and_terms(selection) {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = unwrap_nested_expr(term)
        else {
            continue;
        };
        if column_refs_equivalent(left, column) && row_independent_expr(right) {
            return Some((**right).clone());
        }
        if column_refs_equivalent(right, column) && row_independent_expr(left) {
            return Some((**left).clone());
        }
    }
    None
}

pub(crate) fn table_factor_qualifiers(relation: &TableFactor) -> Result<Option<Vec<String>>> {
    if let TableFactor::Derived { alias, .. } = relation {
        return Ok(alias.as_ref().map(|alias| vec![alias.name.value.clone()]));
    }
    let TableFactor::Table { name, alias, .. } = relation else {
        return Ok(None);
    };
    let table = relation_name(name)?;
    let mut qualifiers = vec![table.clone(), unqualified_relation(&table)];
    if let Some(alias) = alias {
        qualifiers.push(alias.name.value.clone());
    }
    qualifiers.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    Ok(Some(qualifiers))
}

pub(crate) fn column_expr_qualifier_matches(expr: &Expr, qualifiers: &[String]) -> bool {
    let Some(column) = column_ref_from_expr(expr) else {
        return false;
    };
    let Some(qualifier) = column.qualifier else {
        return false;
    };
    qualifiers
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(&qualifier))
}

pub(crate) fn column_refs_equivalent(left: &Expr, right: &Expr) -> bool {
    let (Some(left), Some(right)) = (column_ref_from_expr(left), column_ref_from_expr(right))
    else {
        return false;
    };
    if !left.field.eq_ignore_ascii_case(&right.field) {
        return false;
    }
    match (left.qualifier, right.qualifier) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(&right),
        _ => true,
    }
}

pub(crate) struct ColumnRef {
    pub(crate) qualifier: Option<String>,
    pub(crate) field: String,
}

pub(crate) fn column_ref_from_expr(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Identifier(ident) => Some(ColumnRef {
            qualifier: None,
            field: ident.value.clone(),
        }),
        Expr::CompoundIdentifier(idents) => {
            let field = idents.last()?.value.clone();
            let qualifier = (idents.len() >= 2).then(|| idents[idents.len() - 2].value.clone());
            Some(ColumnRef { qualifier, field })
        }
        Expr::Nested(expr) | Expr::Collate { expr, .. } => column_ref_from_expr(expr),
        _ => None,
    }
}

pub(crate) fn normalize_table_field_expr(
    expr: &Expr,
    table: &str,
    alias: &str,
    schema: Option<&TableSchema>,
) -> Option<Expr> {
    match expr {
        Expr::Identifier(ident) if table_has_field(schema, &ident.value) => {
            Some(Expr::Identifier(ident.clone()))
        }
        Expr::CompoundIdentifier(idents) if idents.len() >= 2 => {
            let qualifier = &idents[idents.len() - 2].value;
            if !qualifier.eq_ignore_ascii_case(alias) && !qualifier.eq_ignore_ascii_case(table) {
                return None;
            }
            let field = idents.last()?.clone();
            table_has_field(schema, &field.value).then_some(Expr::Identifier(field))
        }
        Expr::Nested(expr) => normalize_table_field_expr(expr, table, alias, schema),
        Expr::Cast { expr, .. } => normalize_table_field_expr(expr, table, alias, schema),
        _ => None,
    }
}

pub(crate) fn target_record_field_from_parts(
    table: &str,
    alias: &str,
    schema: Option<&TableSchema>,
    parts: &[String],
) -> Option<(String, Option<String>)> {
    match parts {
        [field] if table_has_field(schema, field) => Some((field.clone(), None)),
        [qualifier, field] if qualifier.eq_ignore_ascii_case(alias) => {
            table_has_field(schema, field).then(|| (field.clone(), Some(alias.to_string())))
        }
        [qualifier, field] if qualifier.eq_ignore_ascii_case(table) => {
            table_has_field(schema, field).then(|| (field.clone(), Some(table.to_string())))
        }
        parts if parts.len() > 2 => {
            let qualifier = &parts[parts.len() - 2];
            let field = parts.last()?;
            if qualifier.eq_ignore_ascii_case(alias) || qualifier.eq_ignore_ascii_case(table) {
                table_has_field(schema, field).then(|| (field.clone(), Some(qualifier.clone())))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn table_has_field(schema: Option<&TableSchema>, field: &str) -> bool {
    // Avoid allocating a lowercased copy on every call (this runs per referenced
    // column); compare case-insensitively in place instead.
    const BUILTIN_FIELDS: [&str; 6] = [
        "id",
        "metadata",
        "timestamp",
        "payload",
        "vector",
        "geometry",
    ];
    if BUILTIN_FIELDS
        .iter()
        .any(|builtin| field.eq_ignore_ascii_case(builtin))
    {
        return true;
    }
    schema.is_some_and(|schema| schema.column(field).is_some())
}

pub(crate) fn row_independent_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::Nested(expr) | Expr::Cast { expr, .. } => row_independent_expr(expr),
        Expr::UnaryOp { expr, .. } => row_independent_expr(expr),
        Expr::BinaryOp { left, right, .. } => {
            row_independent_expr(left) && row_independent_expr(right)
        }
        Expr::Function(function) => function_args(function).iter().all(row_independent_expr),
        Expr::Array(array) => array.elem.iter().all(row_independent_expr),
        Expr::Position { expr, r#in } => row_independent_expr(expr) && row_independent_expr(r#in),
        Expr::Extract { expr, .. } => row_independent_expr(expr),
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            row_independent_expr(expr)
                && trim_what.as_deref().is_none_or(row_independent_expr)
                && trim_characters
                    .as_deref()
                    .is_none_or(|characters| characters.iter().all(row_independent_expr))
        }
        Expr::Interval(interval) => row_independent_expr(interval.value.as_ref()),
        _ => false,
    }
}

pub(crate) fn and_expr(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }
}

pub(crate) fn unwrap_nested_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(expr) => unwrap_nested_expr(expr),
        other => other,
    }
}

#[allow(dead_code)]
pub(crate) fn expr_may_need_tuple_comparison(expr: &Expr) -> bool {
    matches!(unwrap_nested_expr(expr), Expr::Tuple(_))
}

pub(crate) fn join_key_value(value: SqlValue) -> Option<String> {
    (!matches!(value, SqlValue::Null)).then(|| value.to_cell())
}

pub(crate) fn available_join_columns(columns: &[String]) -> BTreeSet<String> {
    let mut available = BTreeSet::new();
    for column in columns {
        available.insert(column.clone());
        if let Some(unqualified) = column.rsplit('.').next() {
            available.insert(unqualified.to_string());
        }
    }
    available
}

pub(crate) fn join_column_reference_available(
    reference: &[String],
    columns: &BTreeSet<String>,
) -> bool {
    if reference.is_empty() {
        return false;
    }
    if reference.len() == 1 {
        return columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case(&reference[0]));
    }
    let qualified = format!(
        "{}.{}",
        reference[reference.len() - 2],
        reference[reference.len() - 1]
    );
    columns
        .iter()
        .any(|column| column.eq_ignore_ascii_case(&qualified))
}

pub(crate) fn simple_reorderable_table_factor(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table {
            args: None,
            with_ordinality: false,
            ..
        }
    )
}

pub(crate) fn is_reorderable_inner_join(join: &Join) -> bool {
    matches!(
        join.join_operator,
        JoinOperator::Join(_) | JoinOperator::Inner(_) | JoinOperator::CrossJoin(_)
    )
}

/// Whether projection pushdown (`retain_referenced_fields`) may run through
/// this join: its constraint is `ON` (an expression the referenced-column
/// scan visits) or absent (a cross/comma join whose predicates live in
/// WHERE). USING and NATURAL name columns outside any expression, so they
/// keep every field.
pub(crate) fn join_allows_projection_pushdown(join: &Join) -> bool {
    match &join.join_operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint) => {
            matches!(constraint, JoinConstraint::On(_) | JoinConstraint::None)
        }
        _ => false,
    }
}

pub(crate) fn table_factor_explain_name(relation: &TableFactor) -> Result<String> {
    let TableFactor::Table { name, .. } = relation else {
        return Err(SqlError::Unsupported(
            "EXPLAIN supports table joins only".to_string(),
        ));
    };
    relation_name(name)
}

pub(crate) fn table_factor_trace_name(relation: &TableFactor) -> String {
    table_factor_explain_name(relation).unwrap_or_else(|_| relation.to_string())
}

pub(crate) fn row_output_columns(
    table: &str,
    alias: &str,
    schema: Option<&TableSchema>,
) -> Vec<String> {
    let fields = FieldRef::wildcard(schema);
    row_output_columns_from_fields(table, alias, &fields)
}

pub(crate) fn row_output_columns_from_fields(
    table: &str,
    alias: &str,
    fields: &[FieldRef],
) -> Vec<String> {
    fields
        .iter()
        .map(|field| format!("{alias}.{}", field.name()))
        .chain(
            (alias != table)
                .then(|| {
                    fields
                        .iter()
                        .map(|field| format!("{table}.{}", field.name()))
                        .collect::<Vec<_>>()
                })
                .into_iter()
                .flatten(),
        )
        .collect()
}

pub(crate) fn row_fields_for_records(
    schema: Option<&TableSchema>,
    records: &[Arc<Record>],
) -> Vec<FieldRef> {
    let mut fields = FieldRef::wildcard(schema);
    if records.iter().any(|record| record.geometry.is_some())
        && !fields
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case("geometry"))
    {
        fields.push(FieldRef::Geometry);
    }
    if schema.is_some() {
        return fields;
    }
    let existing = fields
        .iter()
        .map(|field| field.name())
        .collect::<BTreeSet<_>>();
    let mut metadata_keys = BTreeSet::new();
    for record in records {
        if let Some(metadata) = record.metadata.as_object() {
            for key in metadata.keys() {
                if !existing
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(key))
                {
                    metadata_keys.insert(key.clone());
                }
            }
        }
    }
    fields.extend(metadata_keys.into_iter().map(FieldRef::Column));
    fields
}

pub(crate) fn slot_row_from_record(
    table: &str,
    alias: &str,
    schema: Option<&TableSchema>,
    record: &Record,
) -> Result<SlotRow> {
    let fields = FieldRef::wildcard(schema);
    slot_row_from_record_fields(table, alias, &fields, record)
}

/// Materializes a slot row from a record using a pre-built wildcard field list.
/// The field list depends only on the table schema, so materialization loops
/// build it once and reuse it across every record instead of rebuilding it (and
/// re-cloning every column name) per row.
pub(crate) fn slot_row_from_record_fields(
    table: &str,
    alias: &str,
    fields: &[FieldRef],
    record: &Record,
) -> Result<SlotRow> {
    let mut values = Vec::with_capacity(fields.len() * if alias == table { 1 } else { 2 });
    for field in fields {
        values.push(field.value(record)?);
    }
    if alias != table {
        for idx in 0..fields.len() {
            values.push(values[idx].clone());
        }
    }
    Ok(values)
}

/// Per-statement resolution of wildcard fields to cell positions. Metadata
/// keys serialize in sorted order, so a field's cell index is stable across a
/// table's rows; it is resolved from the first row and verified per row with
/// one key compare (mismatch → linear scan).
pub(crate) type CellPlan = Vec<Option<usize>>;

fn cell_field_name(field: &FieldRef) -> Option<&str> {
    match field {
        FieldRef::PrimaryKey { name, .. }
        | FieldRef::TypedColumn { name, .. }
        | FieldRef::Column(name)
        | FieldRef::JsonColumn(name) => Some(name.as_str()),
        _ => None,
    }
}

/// Text types whose plain-string storage `storage_json_to_sql_value` returns
/// verbatim (no re-parse, no re-rendering). Everything else takes the
/// `Record` path so the cell reader never has to know a type's storage form.
fn cell_text_is_verbatim(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "text"
            | "varchar"
            | "bpchar"
            | "char"
            | "name"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "money"
            | "bool"
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "timestamptz"
            | "interval"
    )
}

/// The SQL value of one borrowed cell for a column of `pg_type`, exactly as
/// `storage_json_to_sql_value` would produce it from the parsed `Value`;
/// `None` for cell/type shapes it does not cover (the row then takes the
/// `Record` path).
fn cell_to_sql_value(cell: &bicdb_core::CellRef<'_>, pg_type: &str) -> Option<SqlValue> {
    // Scalar JSON values still need JSON semantics. In particular a borrowed
    // Float has already lost decimal scale/precision, and JSON null is not
    // SQL NULL. Use the lossless record decoder just as object/array cells do.
    if is_json_pg_type(pg_type) {
        return None;
    }
    Some(match cell {
        bicdb_core::CellRef::Null => SqlValue::Null,
        bicdb_core::CellRef::Bool(value) => SqlValue::Bool(*value),
        bicdb_core::CellRef::Int(value) => SqlValue::Int(*value),
        bicdb_core::CellRef::Float(value) => SqlValue::Float(*value),
        bicdb_core::CellRef::Str(text) => {
            if !cell_text_is_verbatim(pg_type) {
                return None;
            }
            SqlValue::String(text.to_string())
        }
        bicdb_core::CellRef::Envelope {
            pg_type: stored_type,
            text,
            ..
        } => {
            if pg_type == "numeric" && *stored_type == "numeric" {
                SqlValue::String((*text).to_string())
            } else if crate::records::is_temporal_storage_type(pg_type) && *stored_type == pg_type {
                SqlValue::String(crate::records::temporal_storage_text_to_display(
                    pg_type,
                    (*text).to_string(),
                ))
            } else {
                return None;
            }
        }
        bicdb_core::CellRef::Raw(_) => return None,
    })
}

/// `slot_row_from_record_fields` from borrowed cells instead of a parsed
/// `Record`. `None` means some field cannot be read from cells (json columns,
/// geometry/vector/payload fields, unusual storage shapes): the caller builds
/// that row through the `Record` path. Output shape is identical, including
/// the duplicated block when the alias differs from the table.
pub(crate) fn slot_row_from_cells(
    table: &str,
    alias: &str,
    fields: &[FieldRef],
    stored: &bicdb_core::StoredRecord,
    cells: &bicdb_core::CellRow<'_>,
    plan: &mut Option<CellPlan>,
) -> Option<SlotRow> {
    let plan = plan.get_or_insert_with(|| {
        fields
            .iter()
            .map(|field| {
                cell_field_name(field).and_then(|name| {
                    cells
                        .iter()
                        .position(|(key, _)| key.as_ref() == name || key.eq_ignore_ascii_case(name))
                })
            })
            .collect()
    });
    let duplicate = alias != table;
    let mut values = Vec::with_capacity(fields.len() * if duplicate { 2 } else { 1 });
    for (field, planned) in fields.iter().zip(plan.iter()) {
        let value = match field {
            FieldRef::Id => SqlValue::String(stored.id.clone()),
            FieldRef::Timestamp => stored
                .timestamp
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null),
            FieldRef::PrimaryKey { name, pg_type, .. }
            | FieldRef::TypedColumn { name, pg_type, .. } => {
                let cell = planned
                    .and_then(|index| cells.get(index))
                    .filter(|(key, _)| key.as_ref() == name || key.eq_ignore_ascii_case(name))
                    .or_else(|| {
                        cells
                            .iter()
                            .find(|(key, _)| key.as_ref() == name || key.eq_ignore_ascii_case(name))
                    })
                    .map(|(_, cell)| cell);
                match cell {
                    Some(cell) => {
                        let value = cell_to_sql_value(cell, pg_type)?;
                        if matches!(field, FieldRef::PrimaryKey { .. })
                            && matches!(value, SqlValue::Null)
                        {
                            // A pk column absent from (or null in) the metadata
                            // is projected from the record id.
                            cast_value_to_pg_type(SqlValue::String(stored.id.clone()), pg_type)
                                .unwrap_or_else(|_| SqlValue::String(stored.id.clone()))
                        } else {
                            value
                        }
                    }
                    None => {
                        if matches!(field, FieldRef::PrimaryKey { .. }) {
                            cast_value_to_pg_type(SqlValue::String(stored.id.clone()), pg_type)
                                .unwrap_or_else(|_| SqlValue::String(stored.id.clone()))
                        } else if name.eq_ignore_ascii_case("payload") && pg_type == "bytea" {
                            return None;
                        } else {
                            SqlValue::Null
                        }
                    }
                }
            }
            _ => return None,
        };
        values.push(value);
    }
    if duplicate {
        for idx in 0..fields.len() {
            values.push(values[idx].clone());
        }
    }
    Some(values)
}

/// Whether an expression contains a subquery of any form (scalar, EXISTS, IN,
/// ANY/ALL over a subquery). Callers use it to keep address-keyed memos away
/// from expressions whose evaluation can run other statements.
pub(crate) fn expr_contains_subquery(expr: &Expr) -> bool {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    visit_expressions(expr, |e| match e {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

pub(crate) fn slot_row_from_values(columns: &[String], source: &[SqlValue]) -> SlotRow {
    let column_count = columns.len();
    if source.len() == column_count {
        return source.to_vec();
    }
    let mut row = Vec::with_capacity(column_count);
    row.extend(source.iter().take(column_count).cloned());
    row.resize(column_count, SqlValue::Null);
    row
}

pub(crate) fn slot_row_from_sql_row(columns: &[String], row: &SqlRow) -> SlotRow {
    columns
        .iter()
        .map(|column| {
            let parts = column.split('.').map(str::to_string).collect::<Vec<_>>();
            row_value_from_parts_opt(row, &parts).unwrap_or(SqlValue::Null)
        })
        .collect()
}

pub(crate) fn slot_rows_from_sql_rows(columns: &[String], rows: Vec<SqlRow>) -> Vec<SlotRow> {
    rows.into_iter()
        .map(|row| slot_row_from_sql_row(columns, &row))
        .collect()
}

pub(crate) fn row_set_from_sql_rows(rows: Vec<SqlRow>, columns: Vec<String>) -> RowSet {
    let rows = slot_rows_from_sql_rows(&columns, rows);
    RowSet { rows, columns }
}

pub(crate) fn slot_row_to_sql_row(columns: &[String], row: &[SqlValue]) -> SqlRow {
    #[cfg(test)]
    SQL_SLOT_ROW_TO_MAP_CALLS.with(|calls| *calls.borrow_mut() += 1);

    let mut sql_row =
        SqlRow::with_capacity_and_hasher(columns.len().saturating_mul(2), Default::default());
    for (idx, column) in columns.iter().enumerate() {
        let value = row.get(idx).cloned().unwrap_or(SqlValue::Null);
        if let Some(unqualified) = column.rsplit('.').next() {
            sql_row
                .entry(unqualified.to_string())
                .or_insert_with(|| value.clone());
        }
        sql_row.insert(column.clone(), value);
    }
    sql_row
}

pub(crate) fn slot_row_column_index(columns: &[String], column: &str) -> Option<usize> {
    columns
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(column))
}

pub(crate) fn slot_row_value_from_parts(
    columns: &[String],
    row: &[SqlValue],
    parts: &[String],
) -> SqlValue {
    SlotRowLookup::new(columns)
        .value_from_parts(row, parts)
        .unwrap_or(SqlValue::Null)
}

/// Convert one typed metadata cell to its SQL value (typed-row read path).
/// `None` for nested raw JSON that fails to parse — callers fall back to the
/// `Record` form.
pub(crate) fn typed_cell_to_sql_value(cell: &bicdb_core::TypedCell) -> Option<SqlValue> {
    Some(match cell {
        bicdb_core::TypedCell::Null => SqlValue::Null,
        bicdb_core::TypedCell::Bool(value) => SqlValue::Bool(*value),
        bicdb_core::TypedCell::Int(value) => SqlValue::Int(*value),
        bicdb_core::TypedCell::Float(value) => SqlValue::Float(*value),
        bicdb_core::TypedCell::Number(value) => value
            .parse::<f64>()
            .map(SqlValue::Float)
            .unwrap_or_else(|_| SqlValue::String(value.to_string())),
        bicdb_core::TypedCell::Str(value) => SqlValue::String(value.to_string()),
        bicdb_core::TypedCell::Raw(raw) => match serde_json::from_str(raw) {
            Ok(value) => json_to_sql_value(&value),
            Err(_) => return None,
        },
    })
}

pub(crate) fn typed_json_cell_to_sql_value(cell: &bicdb_core::TypedCell) -> Option<SqlValue> {
    Some(SqlValue::Json(match cell {
        bicdb_core::TypedCell::Null => JsonValue::Null,
        bicdb_core::TypedCell::Bool(value) => JsonValue::Bool(*value),
        bicdb_core::TypedCell::Int(value) => JsonValue::from(*value),
        bicdb_core::TypedCell::Float(value) => JsonValue::from(*value),
        bicdb_core::TypedCell::Number(value) => serde_json::from_str(value).ok()?,
        bicdb_core::TypedCell::Str(value) => JsonValue::String(value.to_string()),
        bicdb_core::TypedCell::Raw(raw) => serde_json::from_str(raw).ok()?,
    }))
}

/// One repairable `col = col +/- expr` assignment (see core `RepairPlan`).
pub(crate) struct RepairableAssignment {
    pub(crate) storage_key: String,
    pub(crate) delta_expr: Expr,
    pub(crate) negate: bool,
}

/// Match `col = col + expr`, `col = expr + col`, or `col = col - expr` and
/// return the delta expression (and whether to negate its value).
pub(crate) fn repair_delta_expr(
    value: &Expr,
    column: &str,
    table: &str,
    target_alias: &str,
) -> Option<(Expr, bool)> {
    let Expr::BinaryOp { left, op, right } = value else {
        return None;
    };
    let is_col = |expr: &Expr| expr_is_column(expr, column, table, target_alias);
    match op {
        BinaryOperator::Plus if is_col(left) => Some(((**right).clone(), false)),
        BinaryOperator::Plus if is_col(right) => Some(((**left).clone(), false)),
        BinaryOperator::Minus if is_col(left) => Some(((**right).clone(), true)),
        _ => None,
    }
}

pub(crate) fn expr_is_column(expr: &Expr, column: &str, table: &str, target_alias: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            (idents[0].value.eq_ignore_ascii_case(table)
                || idents[0].value.eq_ignore_ascii_case(target_alias))
                && idents[1].value.eq_ignore_ascii_case(column)
        }
        Expr::Nested(inner) => expr_is_column(inner, column, table, target_alias),
        _ => false,
    }
}

/// Conservative: true when the expression might read any column of the target
/// table (so its value could depend on the row being updated). Unknown or
/// complex expression shapes count as references.
pub(crate) fn expr_references_table_columns(
    expr: &Expr,
    schema: &TableSchema,
    table: &str,
    target_alias: &str,
) -> bool {
    match expr {
        Expr::Identifier(ident) => schema.column(&ident.value).is_some(),
        Expr::CompoundIdentifier(idents) => {
            // Qualified by the target table/alias -> column reference; any
            // other qualifier is opaque here (record vars etc.) -> conservative.
            idents.first().is_some_and(|first| {
                first.value.eq_ignore_ascii_case(table)
                    || first.value.eq_ignore_ascii_case(target_alias)
            }) || idents.len() != 2
        }
        Expr::Value(_) => false,
        Expr::BinaryOp { left, op, right } => {
            !matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) || expr_references_table_columns(left, schema, table, target_alias)
                || expr_references_table_columns(right, schema, table, target_alias)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            expr_references_table_columns(expr, schema, table, target_alias)
        }
        Expr::Cast { expr, .. } => expr_references_table_columns(expr, schema, table, target_alias),
        _ => true,
    }
}

pub(crate) fn collect_index_column_heads(fields: &[IndexField], out: &mut BTreeSet<String>) {
    for field in fields {
        match field {
            IndexField::Id | IndexField::Timestamp | IndexField::Geometry => {}
            IndexField::MetadataPath(path) => {
                if let Some(head) = path.first() {
                    out.insert(head.to_ascii_lowercase());
                }
            }
            IndexField::Lower(inner) | IndexField::Trim(inner) => {
                collect_index_column_heads(std::slice::from_ref(inner), out);
            }
        }
    }
}

/// Per-table repair-eligibility facts, memoized per thread: whether enabled
/// triggers disqualify the table, and the set of column heads touched by any
/// executable index on it. Validated by schema/trigger generations plus the
/// index-catalog length; the commit-side index-key equality check remains as
/// defense in depth against a stale entry after an index redefinition.
pub(crate) struct RepairTableInfo {
    pub(crate) schema_generation: u64,
    pub(crate) trigger_generation: u64,
    pub(crate) index_len: usize,
    pub(crate) has_triggers: bool,
    pub(crate) indexed_columns: BTreeSet<String>,
}

thread_local! {
    pub(crate) static REPAIR_TABLE_MEMO: RefCell<FxHashMap<(usize, String), RepairTableInfo>> =
        RefCell::new(FxHashMap::default());
}

pub(crate) fn repair_table_info(
    db: &BicDb,
    table: &str,
    build: impl FnOnce() -> Result<(bool, BTreeSet<String>)>,
) -> Result<(bool, BTreeSet<String>)> {
    let db_key = db as *const BicDb as usize;
    let schema_generation = db.collection_generation(SCHEMA_COLLECTION);
    let trigger_generation = db.collection_generation(TRIGGER_COLLECTION);
    let index_len = db.index_catalog_len();
    let key = (db_key, table.to_string());
    let hit = REPAIR_TABLE_MEMO.with(|memo| {
        memo.borrow().get(&key).and_then(|entry| {
            (entry.schema_generation == schema_generation
                && entry.trigger_generation == trigger_generation
                && entry.index_len == index_len)
                .then(|| (entry.has_triggers, entry.indexed_columns.clone()))
        })
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let (has_triggers, indexed_columns) = build()?;
    REPAIR_TABLE_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= 512 {
            memo.clear();
        }
        memo.insert(
            key,
            RepairTableInfo {
                schema_generation,
                trigger_generation,
                index_len,
                has_triggers,
                indexed_columns: indexed_columns.clone(),
            },
        );
    });
    Ok((has_triggers, indexed_columns))
}

/// Per-thread memo for [`SqlEngine::plan_row_join_order`]. Keyed by the FROM +
/// WHERE ASTs (full equality verified on hit, so hash collisions cannot serve
/// a wrong plan). A plan is a reordering of the probed statement's own AST
/// nodes, so entries never go semantically stale; the map is only size-capped.
pub(crate) struct JoinPlanMemoEntry {
    pub(crate) from: TableWithJoins,
    pub(crate) selection: Option<Expr>,
    pub(crate) relation: TableFactor,
    pub(crate) joins: Vec<Join>,
}

thread_local! {
    pub(crate) static JOIN_PLAN_MEMO: RefCell<FxHashMap<u64, JoinPlanMemoEntry>> =
        RefCell::new(FxHashMap::default());
}

pub(crate) const JOIN_PLAN_MEMO_CAP: usize = 512;

pub(crate) fn join_plan_memo_key(from: &TableWithJoins, selection: Option<&Expr>) -> u64 {
    use std::hash::Hasher;
    let mut hasher = rustc_hash::FxHasher::default();
    from.hash(&mut hasher);
    selection.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn join_plan_memo_get(
    from: &TableWithJoins,
    selection: Option<&Expr>,
) -> Option<(TableFactor, Vec<Join>)> {
    let key = join_plan_memo_key(from, selection);
    JOIN_PLAN_MEMO.with(|memo| {
        let memo = memo.borrow();
        let entry = memo.get(&key)?;
        if entry.from == *from && entry.selection.as_ref() == selection {
            Some((entry.relation.clone(), entry.joins.clone()))
        } else {
            None
        }
    })
}

pub(crate) fn join_plan_memo_insert(
    from: &TableWithJoins,
    selection: Option<&Expr>,
    plan: &(TableFactor, Vec<Join>),
) {
    let key = join_plan_memo_key(from, selection);
    JOIN_PLAN_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= JOIN_PLAN_MEMO_CAP {
            memo.clear();
        }
        memo.insert(
            key,
            JoinPlanMemoEntry {
                from: from.clone(),
                selection: selection.cloned(),
                relation: plan.0.clone(),
                joins: plan.1.clone(),
            },
        );
    });
}

thread_local! {
    /// Per-thread cache of the "novel right ordinals" for a (left, right)
    /// column-list pair in [`merge_slot_rows`]: the right-column indices not
    /// shadowed by a left column. A pure function of the two name lists (never
    /// the row values), but the O(left × right) string scan previously ran per
    /// merged ROW; joins replay the same pair for every row. Nested maps so the
    /// per-row probe borrows both slices without allocating a key.
    static MERGE_NOVEL_ORDINALS_CACHE: RefCell<
        FxHashMap<Vec<String>, FxHashMap<Vec<String>, Rc<[usize]>>>,
    > = RefCell::new(FxHashMap::default());
}

pub(crate) fn merge_novel_right_ordinals(
    left_columns: &[String],
    right_columns: &[String],
) -> Rc<[usize]> {
    MERGE_NOVEL_ORDINALS_CACHE.with(|cache| {
        if let Some(found) = cache
            .borrow()
            .get(left_columns)
            .and_then(|inner| inner.get(right_columns))
        {
            return found.clone();
        }
        let built: Rc<[usize]> = right_columns
            .iter()
            .enumerate()
            .filter(|(_, column)| !left_columns.iter().any(|existing| existing == *column))
            .map(|(idx, _)| idx)
            .collect();
        cache
            .borrow_mut()
            .entry(left_columns.to_vec())
            .or_default()
            .insert(right_columns.to_vec(), built.clone());
        built
    })
}

pub(crate) fn merge_slot_rows(
    left: &[SqlValue],
    left_columns: &[String],
    right: &[SqlValue],
    right_columns: &[String],
) -> SlotRow {
    let novel = merge_novel_right_ordinals(left_columns, right_columns);
    let mut row = Vec::with_capacity(left.len().saturating_add(novel.len()));
    row.extend(left.iter().cloned());
    for &idx in novel.iter() {
        row.push(right.get(idx).cloned().unwrap_or(SqlValue::Null));
    }
    row
}

pub(crate) fn null_slot_row_for_columns(columns: &[String]) -> SlotRow {
    vec![SqlValue::Null; columns.len()]
}

pub(crate) fn row_from_record(
    table: &str,
    alias: &str,
    schema: Option<&TableSchema>,
    record: &Record,
) -> Result<SqlRow> {
    #[cfg(test)]
    SQL_ROW_FROM_RECORD_CALLS.with(|calls| *calls.borrow_mut() += 1);

    if let Some(schema) = schema {
        return Ok(row_from_record_with_schema(table, alias, schema, record));
    }

    let fields = FieldRef::wildcard(schema);
    let metadata_len = record.metadata.as_object().map_or(0, serde_json::Map::len);
    let base_values = fields.len() + usize::from(record.geometry.is_some()) + metadata_len;
    let mut row = SqlRow::with_capacity_and_hasher(
        row_value_key_capacity(table, alias, base_values),
        Default::default(),
    );
    for field in fields {
        insert_row_value(&mut row, table, alias, &field.name(), field.value(record)?);
    }
    if let Some(geometry) = &record.geometry {
        insert_row_value(
            &mut row,
            table,
            alias,
            "geometry",
            SqlValue::Geometry(geometry.clone()),
        );
    }
    if let Some(metadata) = record.metadata.as_object() {
        for (key, value) in metadata {
            insert_row_value(&mut row, table, alias, key, json_to_sql_value(value));
        }
    }
    Ok(row)
}

pub(crate) fn row_from_record_with_schema(
    table: &str,
    alias: &str,
    schema: &TableSchema,
    record: &Record,
) -> SqlRow {
    let visible_column_count = schema
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .count();
    let metadata_len = record.metadata.as_object().map_or(0, serde_json::Map::len);
    let base_values = visible_column_count + usize::from(record.geometry.is_some()) + metadata_len;
    let mut row = SqlRow::with_capacity_and_hasher(
        row_value_key_capacity(table, alias, base_values),
        Default::default(),
    );
    for column in schema.columns.iter().filter(|column| !column.hidden) {
        insert_row_value(
            &mut row,
            table,
            alias,
            &column.name,
            record_schema_column_value(record, column, true),
        );
    }
    if let Some(geometry) = &record.geometry {
        insert_row_value(
            &mut row,
            table,
            alias,
            "geometry",
            SqlValue::Geometry(geometry.clone()),
        );
    }
    if let Some(metadata) = record.metadata.as_object() {
        for (key, value) in metadata {
            if schema.column(key).is_some() {
                continue;
            }
            insert_row_value(&mut row, table, alias, key, json_to_sql_value(value));
        }
    }
    row
}

pub(crate) fn row_from_virtual_row<I>(table: &str, alias: &str, source: I) -> SqlRow
where
    I: IntoIterator<Item = (String, SqlValue)>,
{
    let mut source = source.into_iter().collect::<FxHashMap<_, _>>();
    if !source
        .keys()
        .any(|column| column.eq_ignore_ascii_case("tableoid"))
    {
        if let Some(oid) = virtual_catalog_table_oid(table) {
            source.insert("tableoid".to_string(), SqlValue::Int(oid));
        }
    }
    let mut row = SqlRow::with_capacity_and_hasher(
        row_value_key_capacity(table, alias, source.len()),
        Default::default(),
    );
    for (column, value) in source {
        insert_row_value(&mut row, table, alias, &column, value);
    }
    row
}

pub(crate) fn row_value_key_capacity(table: &str, alias: &str, value_count: usize) -> usize {
    value_count.saturating_mul(if alias == table { 2 } else { 3 })
}

pub(crate) fn virtual_catalog_table_oid(table: &str) -> Option<i64> {
    match table.strip_prefix("pg_catalog.").unwrap_or(table) {
        "pg_database" => Some(1262),
        "pg_default_acl" => Some(826),
        "pg_user_mapping" => Some(1418),
        "pg_authid" | "pg_roles" | "pg_user" => Some(1260),
        "pg_auth_members" => Some(1261),
        "pg_tablespace" => Some(1213),
        "pg_type" => Some(1247),
        "pg_attribute" => Some(1249),
        "pg_foreign_data_wrapper" => Some(2328),
        "pg_proc" => Some(PG_PROC_CATALOG_OID),
        "pg_class" => Some(PG_CLASS_CATALOG_OID),
        "pg_attrdef" => Some(2604),
        "pg_cast" => Some(2605),
        "pg_constraint" => Some(PG_CONSTRAINT_CATALOG_OID),
        "pg_conversion" => Some(2607),
        "pg_depend" => Some(2608),
        "pg_db_role_setting" => Some(2964),
        "pg_description" => Some(2609),
        "pg_index" => Some(2610),
        "pg_inherits" => Some(2611),
        "pg_largeobject_metadata" => Some(2995),
        "pg_amop" => Some(2602),
        "pg_amproc" => Some(2603),
        "pg_namespace" => Some(2615),
        "pg_language" => Some(PG_LANGUAGE_CATALOG_OID),
        "pg_opclass" => Some(2616),
        "pg_operator" => Some(2617),
        "pg_rewrite" => Some(2618),
        "pg_trigger" => Some(2620),
        "pg_am" => Some(2601),
        "pg_opfamily" => Some(2753),
        "pg_extension" => Some(PG_EXTENSION_CATALOG_OID),
        "pg_foreign_table" => Some(3118),
        "pg_policy" => Some(3256),
        "pg_init_privs" => Some(3394),
        "pg_partitioned_table" => Some(3350),
        "pg_statistic_ext" => Some(3381),
        "pg_statistic_ext_data" => Some(3429),
        "pg_collation" => Some(3456),
        "pg_enum" => Some(3501),
        "pg_range" => Some(3541),
        "pg_event_trigger" => Some(3466),
        "pg_transform" => Some(3576),
        "pg_shseclabel" => Some(3592),
        "pg_seclabel" => Some(3596),
        "pg_ts_dict" => Some(3600),
        "pg_ts_parser" => Some(3601),
        "pg_ts_config" => Some(3602),
        "pg_ts_config_map" => Some(3603),
        "pg_ts_template" => Some(3764),
        "pg_subscription" => Some(6100),
        "pg_subscription_rel" => Some(6102),
        "pg_publication" => Some(6104),
        "pg_publication_rel" => Some(6106),
        "pg_publication_namespace" => Some(6237),
        "pg_sequence" => Some(2224),
        "pg_foreign_server" => Some(1417),
        _ => None,
    }
}

pub(crate) fn table_factor_relation_alias(
    relation: &TableFactor,
    expected_table: &str,
) -> Result<Option<(String, String)>> {
    let TableFactor::Table { name, alias, .. } = relation else {
        return Ok(None);
    };
    let table = relation_name(name)?;
    let stripped = table.strip_prefix("pg_catalog.").unwrap_or(&table);
    if !stripped.eq_ignore_ascii_case(expected_table) {
        return Ok(None);
    }
    let alias = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| stripped.to_string());
    Ok(Some((stripped.to_string(), alias)))
}

pub(crate) fn join_constraint_has_column_equality(
    constraint: &JoinConstraint,
    left_alias: &str,
    left_table: &str,
    left_field: &str,
    right_alias: &str,
    right_table: &str,
    right_field: &str,
) -> bool {
    let JoinConstraint::On(expr) = constraint else {
        return false;
    };
    and_terms(expr).into_iter().any(|term| {
        expr_has_column_equality(
            term,
            left_alias,
            left_table,
            left_field,
            right_alias,
            right_table,
            right_field,
        )
    })
}

pub(crate) fn expr_has_column_equality(
    expr: &Expr,
    left_alias: &str,
    left_table: &str,
    left_field: &str,
    right_alias: &str,
    right_table: &str,
    right_field: &str,
) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    (relation_oid_column_matches(left, left_alias, left_table, &[left_field])
        && relation_oid_column_matches(right, right_alias, right_table, &[right_field]))
        || (relation_oid_column_matches(right, left_alias, left_table, &[left_field])
            && relation_oid_column_matches(left, right_alias, right_table, &[right_field]))
}

pub(crate) fn join_constraint_has_attribute_key_equality(
    constraint: &JoinConstraint,
    attribute_alias: &str,
    constraint_alias: &str,
    key_field: &str,
) -> bool {
    let JoinConstraint::On(expr) = constraint else {
        return false;
    };
    and_terms(expr).into_iter().any(|term| {
        expr_has_attribute_key_equality(term, attribute_alias, constraint_alias, key_field)
    })
}

pub(crate) fn expr_has_attribute_key_equality(
    expr: &Expr,
    attribute_alias: &str,
    constraint_alias: &str,
    key_field: &str,
) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    (relation_oid_column_matches(left, attribute_alias, "pg_attribute", &["attnum"])
        && constraint_key_subscript_matches(right, constraint_alias, key_field))
        || (relation_oid_column_matches(right, attribute_alias, "pg_attribute", &["attnum"])
            && constraint_key_subscript_matches(left, constraint_alias, key_field))
}

pub(crate) fn constraint_key_subscript_matches(
    expr: &Expr,
    constraint_alias: &str,
    key_field: &str,
) -> bool {
    match unwrap_nested_expr(expr) {
        Expr::CompoundFieldAccess { root, access_chain } => {
            relation_oid_column_matches(root, constraint_alias, "pg_constraint", &[key_field])
                && matches!(
                    access_chain.as_slice(),
                    [AccessExpr::Subscript(subscript)] if subscript_index_is_one(subscript)
                )
        }
        _ => false,
    }
}

pub(crate) fn subscript_index_is_one(subscript: &Subscript) -> bool {
    let Subscript::Index { index } = subscript else {
        return false;
    };
    eval_constant_expr(index)
        .ok()
        .and_then(|value| sql_value_i64(&value))
        == Some(1)
}

pub(crate) fn pg_class_index_join_columns(
    table_alias: &str,
    index_alias: &str,
    index_class_alias: &str,
    namespace_alias: Option<&str>,
) -> Vec<String> {
    let mut columns = aliased_virtual_columns("pg_class", table_alias);
    columns.extend(aliased_virtual_columns("pg_index", index_alias));
    columns.extend(aliased_virtual_columns("pg_class", index_class_alias));
    if let Some(namespace_alias) = namespace_alias {
        columns.extend(aliased_virtual_columns("pg_namespace", namespace_alias));
    }
    columns
}

pub(crate) fn aliased_virtual_columns(table: &str, alias: &str) -> Vec<String> {
    virtual_table_columns_for_empty_join(table)
        .unwrap_or_default()
        .into_iter()
        .map(|column| format!("{alias}.{column}"))
        .collect()
}

pub(crate) fn pg_class_table_row_for_schema(
    db: &BicDb,
    schema: &TableSchema,
    catalog_indexes: &[CatalogIndex],
    triggers: &[TriggerSchema],
    table_oids: &BTreeMap<String, i64>,
) -> Result<BTreeMap<String, SqlValue>> {
    let columns = columns_for_relation(std::slice::from_ref(schema), &schema.name);
    let relkind = if schema.partitioning.is_some() {
        "p"
    } else {
        "r"
    };
    let has_index = schema
        .primary_key_column()
        .is_some_and(|column| !column.hidden)
        || catalog_indexes
            .iter()
            .any(|index| index.collection.eq_ignore_ascii_case(&schema.name));
    let has_triggers = triggers
        .iter()
        .any(|trigger| trigger.table_name.eq_ignore_ascii_case(&schema.name));
    Ok(pg_class_row(
        *table_oids.get(&schema.name).unwrap_or(&0),
        &schema.name,
        namespace_oid(&schema.schema_name),
        Some(schema.row_type_oid()),
        relkind,
        columns.len() as i64,
        has_index,
        false,
        reltuples_for_relation(db, &schema.name, false),
        check_constraint_count(schema),
        has_triggers,
        schema.partitioning.is_some(),
        schema.partition_of.is_some(),
        schema
            .partition_of
            .as_ref()
            .map(|partition_of| SqlValue::String(partition_of.bound.clone()))
            .unwrap_or(SqlValue::Null),
        table_acl_value(db, &schema.name)?,
        2,
        schema.rls_enabled,
        schema.rls_forced,
    ))
}

pub(crate) fn namespace_catalog_row(
    db: &BicDb,
    namespace: &str,
) -> Result<BTreeMap<String, SqlValue>> {
    let owner = namespace_owner(db, namespace)?;
    Ok(virtual_row([
        ("oid", SqlValue::Int(namespace_oid(namespace))),
        ("nspname", SqlValue::String(namespace.to_string())),
        ("nspowner", SqlValue::Int(role_oid(&owner))),
        ("nspacl", schema_acl_value(db, namespace)?),
    ]))
}

pub(crate) fn pg_class_index_row(
    oid: i64,
    name: &str,
    schema_name: &str,
    relnatts: i64,
    relam: i64,
) -> BTreeMap<String, SqlValue> {
    pg_class_row(
        oid,
        name,
        namespace_oid(schema_name),
        None,
        "i",
        relnatts,
        false,
        false,
        0.0,
        0,
        false,
        false,
        false,
        SqlValue::Null,
        SqlValue::Null,
        relam,
        false,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pg_class_index_join_row(
    table_alias: &str,
    index_alias: &str,
    index_class_alias: &str,
    namespace_alias: Option<&str>,
    table_row: &BTreeMap<String, SqlValue>,
    namespace_row: &BTreeMap<String, SqlValue>,
    pg_index_row: BTreeMap<String, SqlValue>,
    index_class_row: BTreeMap<String, SqlValue>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_class", table_alias, table_row.clone());
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_index", index_alias, pg_index_row),
    );
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_class", index_class_alias, index_class_row),
    );
    if let Some(namespace_alias) = namespace_alias {
        row = merge_rows(
            &row,
            &row_from_virtual_row("pg_namespace", namespace_alias, namespace_row.clone()),
        );
    }
    row
}

pub(crate) fn pg_class_indexrelid_join_columns(
    class_alias: &str,
    index_alias: &str,
    namespace_alias: Option<&str>,
) -> Vec<String> {
    let mut columns = aliased_virtual_columns("pg_class", class_alias);
    columns.extend(aliased_virtual_columns("pg_index", index_alias));
    if let Some(alias) = namespace_alias {
        columns.extend(aliased_virtual_columns("pg_namespace", alias));
    }
    columns
}

/// Column names (lower-cased) referenced anywhere in a SELECT, or `None` when
/// the query needs whole rows: a wildcard in the projection, an expression
/// wildcard, a row lock, or SELECT INTO. Over-inclusion is harmless — the set
/// only decides which stored fields are converted into SQL values before
/// projection, so an extra name costs one conversion and a missing name would
/// be an error. Identifiers that are not columns (projection aliases, outer
/// references) are therefore collected without resolution.
pub(crate) fn referenced_column_names(select: &Select, query: &Query) -> Option<ReferencedColumns> {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    if select.into.is_some() || !query.locks.is_empty() {
        return None;
    }
    if select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    }) {
        return None;
    }
    let mut names = ReferencedColumns::default();
    let flow: ControlFlow<()> = visit_expressions(query, |expr| {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                names.plain.insert(name.clone());
                names.all.insert(name);
            }
            // Every part: `alias.col` qualifies a column, but `metadata.clinic`
            // is a column followed by a JSON key, and `schema.table.col` mixes
            // both. Qualifier parts match no field name, so keeping them is
            // free; dropping the wrong part would lose the column.
            Expr::CompoundIdentifier(parts) => {
                for part in parts {
                    names.all.insert(part.value.to_ascii_lowercase());
                }
            }
            Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => return ControlFlow::Break(()),
            _ => {}
        }
        ControlFlow::Continue(())
    });
    if flow.is_break() {
        return None;
    }
    Some(names)
}

/// Identifiers a query references, lower-cased. `plain` holds bare
/// identifiers only (where a table or alias name means a whole-row
/// reference); `all` adds every part of each compound identifier.
#[derive(Debug, Default, Clone)]
pub(crate) struct ReferencedColumns {
    pub(crate) plain: std::collections::HashSet<String>,
    pub(crate) all: std::collections::HashSet<String>,
}

/// Keeps only the fields a single-relation query references, so the row set
/// converts two columns instead of twenty. Keeps every field when the query
/// needs whole rows (`needed` is `None`), references nothing by name, uses the
/// table or alias name as a value (a whole-row reference), or would otherwise
/// be left with no fields at all.
pub(crate) fn retain_referenced_fields(
    fields: &mut Vec<FieldRef>,
    needed: Option<&ReferencedColumns>,
    table: &str,
    alias: &str,
) {
    let Some(needed) = needed else {
        return;
    };
    if needed.all.is_empty() {
        return;
    }
    let short = table.rsplit('.').next().unwrap_or(table);
    if needed.plain.contains(&alias.to_ascii_lowercase())
        || needed.plain.contains(&short.to_ascii_lowercase())
    {
        return;
    }
    let kept = fields
        .iter()
        .filter(|field| needed.all.contains(&field.name().to_ascii_lowercase()))
        .count();
    if kept == 0 || kept == fields.len() {
        return;
    }
    fields.retain(|field| needed.all.contains(&field.name().to_ascii_lowercase()));
}

#[cfg(test)]
mod bound_builtin_tests {
    use super::*;

    fn expr(sql: &str) -> Expr {
        let statement =
            crate::routines::parse_single_statement(&format!("SELECT {sql}")).expect("parses");
        let Statement::Query(query) = statement else {
            panic!("expected a query");
        };
        let SetExpr::Select(select) = *query.body else {
            panic!("expected a select");
        };
        match select.projection.into_iter().next() {
            Some(SelectItem::UnnamedExpr(expr)) => expr,
            other => panic!("expected an expression, got {other:?}"),
        }
    }

    fn eval_bound(bound: &BoundExpr) -> SqlValue {
        let dir = tempfile::tempdir().unwrap();
        let db = BicDb::open(dir.path()).unwrap();
        let empty = SqlRow::default();
        bound
            .eval(&BoundExprFrame {
                user_calls: &[],
                db: &db,
                columns: BoundExprColumns::Row {
                    row: &empty,
                    column_keys: &[],
                },
                vars: &[],
            })
            .unwrap()
    }

    #[test]
    fn typed_decimal_intermediates_match_generic_arithmetic_and_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let db = BicDb::open(dir.path()).unwrap();
        let frame = BoundExprFrame {
            db: &db,
            columns: BoundExprColumns::Values(&[]),
            vars: &[],
            user_calls: &[],
        };
        let scope = BoundExprScope::new(&[], &[]);
        let cases = [
            "(CAST('12.50' AS numeric) * 3) * (1 + CAST('0.05' AS numeric) + CAST('0.07' AS numeric)) * (1 - CAST('0.10' AS numeric))",
            "(CAST('999999999999999999' AS numeric) * CAST('999999999999999999' AS numeric)) * 999999999999999999",
            "(CAST('0.000000001' AS numeric) * CAST('0.000000001' AS numeric)) * CAST('0.000000001' AS numeric)",
            "(CAST('-12.500' AS numeric) + CAST('12.500' AS numeric)) * 3",
            "(CAST('10.00' AS numeric) * 3) / 7",
            "(CAST('10.00' AS numeric) * 3) + 0.5",
            "(CAST('10.00' AS numeric) * 3) = CAST('30.00' AS numeric)",
            "CAST((CAST('10.00' AS numeric) * 3) AS int)",
            "round((CAST('10.00' AS numeric) * 3) / 7, 2)",
            "(CAST('10.00' AS numeric) * 3) + NULL",
            "(CAST('10.00' AS numeric) * 3) / 0",
            "9223372036854775807 + 1",
            "(9223372036854775807 + 1) + CAST('1.0' AS numeric)",
        ];
        for sql in cases {
            let expr = expr(sql);
            let bound = scope
                .bind(&expr)
                .unwrap_or_else(|| panic!("must bind: {sql}"));
            assert_eq!(
                bound.eval(&frame).map_err(|error| error.to_string()),
                crate::eval::eval_constant_expr(&expr).map_err(|error| error.to_string()),
                "{sql}"
            );
        }
        let bound = scope
            .bind(&expr(
                "CAST('2.50' AS numeric) * 3 + CAST('1.25' AS numeric)",
            ))
            .unwrap();
        assert!(matches!(
            bound.eval_value(&frame).unwrap(),
            BoundValue::Decimal(_)
        ));
        assert_eq!(bound.eval(&frame).unwrap(), SqlValue::String("8.75".into()));
    }

    #[test]
    fn bound_inputs_and_case_results_borrow_the_frame() {
        let dir = tempfile::tempdir().unwrap();
        let db = BicDb::open(dir.path()).unwrap();
        let columns = [SqlValue::String(
            "a long value that should not be cloned".repeat(32),
        )];
        let vars = [SqlValue::String("12.50".into())];
        let frame = BoundExprFrame {
            db: &db,
            columns: BoundExprColumns::Values(&columns),
            vars: &vars,
            user_calls: &[],
        };
        let column = BoundExpr::Column(ColumnId(0));
        let value = column.eval_borrowed(&frame).unwrap();
        assert!(matches!(value, std::borrow::Cow::Borrowed(_)));
        assert!(std::ptr::eq(value.as_ref(), &columns[0]));
        let case = BoundExpr::Case {
            operand: None,
            conditions: vec![(BoundExpr::Literal(SqlValue::Bool(true)), column)],
            else_result: None,
        };
        let value = case.eval_borrowed(&frame).unwrap();
        assert!(std::ptr::eq(value.as_ref(), &columns[0]));
        let sum = BoundExpr::Binary {
            left: Box::new(BoundExpr::Var(VarId(0))),
            op: BinaryOperator::Plus,
            right: Box::new(BoundExpr::Literal(SqlValue::String("2.25".into()))),
        };
        assert_eq!(sum.eval(&frame).unwrap(), SqlValue::String("14.75".into()));
        assert_eq!(vars[0], SqlValue::String("12.50".into()));
    }

    #[test]
    fn validated_case_is_lazy_and_preserves_null_matching() {
        let scope = BoundExprScope::new(&[], &[]);
        for sql in [
            "CASE WHEN true THEN 7 ELSE 1 / 0 END",
            "CASE WHEN false THEN 1 / 0 WHEN true THEN 7 ELSE 1 / 0 END",
            "CASE 2 WHEN 1 THEN 1 / 0 WHEN 2 THEN 7 ELSE 1 / 0 END",
            "CASE NULL WHEN NULL THEN 1 / 0 ELSE 7 END",
            "CASE WHEN NULL THEN 1 / 0 ELSE 7 END",
            "CASE WHEN false THEN 7 END",
            "CASE WHEN true THEN CASE WHEN false THEN 1 / 0 ELSE 7 END ELSE 1 / 0 END",
        ] {
            let expr = expr(sql);
            assert!(scope.bind(&expr).is_none(), "unvalidated CASE must decline");
            assert!(scope
                .bind_with_case_validator(&expr, &|_, _| false)
                .is_none());
            let bound = scope.bind_with_case_validator(&expr, &|_, _| true).unwrap();
            assert_eq!(
                eval_bound(&bound),
                crate::eval::eval_constant_expr(&expr).unwrap(),
                "{sql}"
            );
        }
    }

    #[test]
    fn whitelisted_builtins_bind_and_match_the_generic_evaluator() {
        let scope = BoundExprScope::new(&[], &[]);
        for sql in [
            "abs(-3) + trunc(2.7)",
            "trunc(17.9 / 3) * 2",
            "round(2.5) - round(1.26, 1)",
            "mod(17, 5) + abs(-8)",
            "char_length('abc') + char_length('DEFG')",
            "trunc(1.5 * (9 - 4 + 1) + 4)",
        ] {
            let expr = expr(sql);
            let bound = scope
                .bind(&expr)
                .unwrap_or_else(|| panic!("{sql} must bind"));
            assert!(
                matches!(bound, BoundExpr::Binary { .. } | BoundExpr::Call { .. }),
                "{sql} binds as arithmetic over calls"
            );
            assert_eq!(
                eval_bound(&bound),
                crate::eval::eval_constant_expr(&expr).unwrap(),
                "{sql}"
            );
        }
        let random = expr("trunc(random() * 10)");
        assert!(matches!(
            scope.bind(&random),
            Some(BoundExpr::Binary { .. }) | Some(BoundExpr::Call { .. })
        ));
        let bound = scope.bind(&random).unwrap();
        for _ in 0..50 {
            let SqlValue::Float(value) = eval_bound(&bound) else {
                panic!("trunc yields a float");
            };
            assert!((0.0..10.0).contains(&value) && value.fract() == 0.0);
        }
    }

    #[test]
    fn user_calls_hoist_only_where_every_operand_is_evaluated() {
        let scope = BoundExprScope::new(&[], &["x".to_string()]);
        // A non-builtin call under arithmetic/builtins hoists; the tree
        // references its slot.
        let (bound, calls) = scope
            .bind_with_user_calls(&expr("round(dbms_random(1, x)) + 1"))
            .expect("hoists the user call");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "dbms_random");
        assert!(matches!(
            calls[0].args.as_slice(),
            [BoundExpr::Literal(_), BoundExpr::Var(_)]
        ));
        assert!(matches!(bound, BoundExpr::Binary { .. }));
        // Nested calls: inner first (post-order = PostgreSQL's evaluation order).
        let (bound, calls) = scope
            .bind_with_user_calls(&expr("f(g(1), 2)"))
            .expect("hoists nested calls");
        assert_eq!(
            calls
                .iter()
                .map(|call| call.name.as_str())
                .collect::<Vec<_>>(),
            ["g", "f"]
        );
        assert!(matches!(bound, BoundExpr::UserCall(1)));
        assert!(matches!(
            calls[1].args.as_slice(),
            [BoundExpr::UserCall(0), BoundExpr::Literal(_)]
        ));
        // Under a short-circuit operator the call may never run: no hoisting,
        // the whole expression stays with the generic evaluator.
        assert!(scope
            .bind_with_user_calls(&expr("x > 1 AND f(5) > 0"))
            .is_none());
        assert!(scope
            .bind_with_user_calls(&expr("f(1) > 0 OR x > 1"))
            .is_none());
        // A volatile builtin next to a hoisted call would change evaluation
        // order: declined.
        assert!(scope
            .bind_with_user_calls(&expr("f(1) + random()"))
            .is_none());
        assert!(scope
            .bind_with_user_calls(&expr("f(trunc(random() * 10))"))
            .is_none());
        // Plain `bind` (row predicates, projections) never hoists.
        assert!(scope.bind(&expr("f(1) + 1")).is_none());
        // Expressions without user calls bind exactly as before, with no slots.
        let (bound, calls) = scope.bind_with_user_calls(&expr("x + 1")).expect("binds");
        assert!(calls.is_empty());
        assert!(matches!(bound, BoundExpr::Binary { .. }));
    }

    #[test]
    fn array_subscripts_bind_only_for_array_typed_variables() {
        let scope =
            BoundExprScope::new(&[], &["arr".to_string(), "v".to_string(), "i".to_string()])
                .with_array_vars(&["arr".to_string()]);
        let bound = scope.bind(&expr("arr[i]")).expect("binds the subscript");
        assert!(matches!(bound, BoundExpr::Subscript { .. }));
        let bound = scope
            .bind(&expr("v + CAST(arr[i + 1] AS NUMERIC)"))
            .expect("binds");
        assert!(matches!(bound, BoundExpr::Binary { .. }));
        // Not declared as an array (could be jsonb, text, a record): generic.
        assert!(scope.bind(&expr("v[1]")).is_none());
        // Slices and multi-step chains stay generic.
        assert!(scope.bind(&expr("arr[1:2]")).is_none());
        assert!(scope.bind(&expr("arr[1][2]")).is_none());
        let plain = BoundExprScope::new(&[], &["arr".to_string()]);
        assert!(plain.bind(&expr("arr[1]")).is_none());
    }

    #[test]
    fn other_functions_stay_unbound() {
        let scope = BoundExprScope::new(&[], &[]);
        for sql in [
            "now()",
            "to_char(1, '9')",
            "coalesce(1, 2)",
            "count(*)",
            "concat('a', 'b')",
            // lower/upper have range and bit-string overloads, trunc a macaddr
            // one: none may bind to the string/numeric builtin.
            "lower(span)",
            "upper(x)",
            "trunc(addr)",
        ] {
            assert!(scope.bind(&expr(sql)).is_none(), "{sql} must not bind");
        }
    }
}

#[cfg(test)]
mod relation_alias_lookup_tests {
    use super::*;

    fn reference(columns: &[String], row: &[SqlValue], alias: &str) -> Option<SqlValue> {
        let mut matched = None;
        for (idx, column) in columns.iter().enumerate() {
            let Some((qualifier, _)) = column.split_once('.') else {
                continue;
            };
            if !qualifier.eq_ignore_ascii_case(alias) {
                continue;
            }
            if matched.is_some() {
                return None;
            }
            matched = row.get(idx).cloned();
        }
        matched
    }

    #[test]
    fn qualifier_index_matches_the_linear_scan() {
        let layouts: Vec<Vec<String>> = vec![
            vec!["c.c_id", "c.c_last", "w", "d.d_id"],
            vec!["Cust.id", "cust.name", "plain", "o.total"],
            vec!["a", "b", "c"],
            vec![],
            vec!["t.x"],
            vec!["x.y.z", "x.q", "y.z"],
        ]
        .into_iter()
        .map(|cols| cols.into_iter().map(String::from).collect())
        .collect();
        for columns in &layouts {
            let row: Vec<SqlValue> = (0..columns.len())
                .map(|i| SqlValue::Int(i as i64))
                .collect();
            let lookup = SlotRowLookup {
                cols: cached_column_lookups(columns),
            };
            for alias in [
                "c", "C", "cust", "CUST", "w", "d", "o", "t", "x", "y", "zz", "plain", "",
            ] {
                assert_eq!(
                    lookup.scalar_value_for_relation_alias(&row, alias),
                    reference(columns, &row, alias),
                    "{columns:?} {alias}"
                );
                let composite = lookup.composite_value_for_relation_alias(&row, alias);
                let expected_fields = columns
                    .iter()
                    .filter(|column| {
                        column
                            .split_once('.')
                            .is_some_and(|(qualifier, _)| qualifier.eq_ignore_ascii_case(alias))
                    })
                    .count();
                assert_eq!(
                    composite.is_some(),
                    expected_fields > 0,
                    "{columns:?} {alias}"
                );
            }
        }
    }
}
