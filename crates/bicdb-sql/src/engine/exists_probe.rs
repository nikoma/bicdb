//! Statement-local candidate indexes for pure correlated equality EXISTS.
//! The original subquery still executes with a necessary primary-key filter:
//! projection, type comparisons, privileges and transaction visibility retain
//! the ordinary execution path. No application-specific identifiers are used.
use super::*;
use sqlparser::ast::{LimitClause, Visit, Visitor};
use std::cell::Cell;
use std::ops::ControlFlow;

const MAX_PROBE_ENTRIES: usize = 4;
const MAX_PROBE_ROWS: usize = 65_536;
const MAX_BUCKET_KEYS: usize = 256;
const MAX_PROBE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Default)]
pub(crate) struct ExistsProbeMemo {
    depth: Cell<usize>,
    enabled: Cell<bool>,
    entries: RefCell<Vec<ProbeEntry>>,
}

#[derive(Debug)]
struct ProbeEntry {
    query: Box<Query>,
    columns: Vec<String>,
    env: Vec<RelationColumns>,
    plan: Option<Rc<CandidateProbe>>,
}

#[derive(Debug)]
struct CandidateProbe {
    outer_slot: usize,
    primary_key: Expr,
    source: CandidateSource,
    /// The original subquery has run once with an empty candidate set, so
    /// its relation and projection authorization held for this statement.
    empty_verified: Cell<bool>,
}

#[derive(Debug)]
enum CandidateSource {
    /// Primary keys of the inner rows that pass the residual filters, by
    /// canonical numeric key, from one scan of the inner table.
    Scanned(BTreeMap<String, Vec<SqlValue>>),
    /// Record ids by index key from an index-only read of an index that
    /// holds the correlation column at any position (no record decoded).
    Indexed(IndexedCandidates),
}

/// A bucket is resolved to primary-key values and narrowed by the residual
/// filters the first time an outer value probes it, so each inner row is
/// read at most once per statement, and only for correlated values that
/// actually occur among the outer rows.
#[derive(Debug)]
struct IndexedCandidates {
    table: String,
    schema: TableSchema,
    primary_name: String,
    field: IndexField,
    buckets: RefCell<BTreeMap<IndexValue, IndexedBucket>>,
    /// `SELECT <primary key> FROM <inner> WHERE <residual filters>`, or
    /// `None` when the subquery has no residual filter.
    residual_scan: Option<Query>,
}

#[derive(Debug)]
enum IndexedBucket {
    Ids(Vec<String>),
    Keys(Vec<SqlValue>),
}

fn candidate_literals(keys: &[SqlValue]) -> Vec<Expr> {
    if keys.is_empty() {
        return vec![Expr::Value(Value::Null.into())];
    }
    keys.iter()
        .map(|value| match value {
            SqlValue::Int(value) => Expr::Value(Value::Number(value.to_string(), false).into()),
            _ => Expr::Value(Value::SingleQuotedString(value.to_cell()).into()),
        })
        .collect()
}

/// How a correlated EXISTS is answered from its statement-local candidates.
pub(super) enum ExistsCandidate {
    /// The original subquery restricted to candidate primary keys.
    Query(Query),
    /// No inner row can satisfy the correlation and filters: EXISTS is false.
    Absent,
}

pub(crate) struct ProbeScope<'a> {
    memo: &'a ExistsProbeMemo,
    previous: bool,
}

impl Drop for ProbeScope<'_> {
    fn drop(&mut self) {
        let depth = self.memo.depth.get().saturating_sub(1);
        self.memo.depth.set(depth);
        self.memo.enabled.set(self.previous);
        if depth == 0 {
            self.memo.entries.borrow_mut().clear();
        }
    }
}

impl ExistsProbeMemo {
    pub(super) fn scope(&self, enabled: bool) -> ProbeScope<'_> {
        let previous = self.enabled.get();
        let depth = self.depth.get();
        self.enabled.set(enabled && (depth == 0 || previous));
        self.depth.set(depth + 1);
        ProbeScope {
            memo: self,
            previous,
        }
    }
}

/// No outer callbacks or hidden row policies may change a scanned table while
/// its statement-local candidate index is being reused. Keep unsupported trees
/// on the ordinary evaluator, including views, functions and modifying CTEs.
fn cast_may_hide_callbacks(kind: &DataType) -> bool {
    match kind {
        DataType::Custom(..) => true,
        DataType::Array(
            sqlparser::ast::ArrayElemTypeDef::AngleBracket(element)
            | sqlparser::ast::ArrayElemTypeDef::SquareBracket(element, _)
            | sqlparser::ast::ArrayElemTypeDef::Parenthesis(element),
        ) => cast_may_hide_callbacks(element),
        _ => false,
    }
}

impl SqlEngine<'_> {
    /// Statement-local EXISTS candidates for an UPDATE or DELETE predicate.
    /// The candidate index is built once per statement, so it is only reused
    /// when no subquery reads the table the statement modifies.
    pub(crate) fn dml_predicate_probe_scope(
        &self,
        table: &str,
        selection: Option<&Expr>,
    ) -> ProbeScope<'_> {
        thread_local! {
            static TEMPLATE: Option<Box<Query>> = match parse_statements("SELECT 1") {
                Ok(mut statements) => match statements.pop() {
                    Some(Statement::Query(query)) => Some(query),
                    _ => None,
                },
                Err(_) => None,
            };
        }
        struct HasExists;
        impl Visitor for HasExists {
            type Break = ();
            fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<()> {
                if matches!(expression, Expr::Exists { .. }) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
        }
        let safe = self.outer_row.is_none()
            && selection.is_some_and(|selection| {
                if selection.visit(&mut HasExists).is_continue() {
                    return false;
                }
                let Some(mut query) = TEMPLATE.with(Clone::clone) else {
                    return false;
                };
                let SetExpr::Select(select) = query.body.as_mut() else {
                    return false;
                };
                select.selection = Some(selection.clone());
                struct ReadsTarget<'a>(&'a str);
                impl Visitor for ReadsTarget<'_> {
                    type Break = ();
                    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
                        match factor {
                            TableFactor::Table { name, .. }
                                if relation_name(name).map_or(true, |name| name == self.0) =>
                            {
                                ControlFlow::Break(())
                            }
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }
                probe_query_safe(self.db_ref(), &query)
                    && query.visit(&mut ReadsTarget(table)).is_continue()
            });
        self.exists_probe_memo.scope(safe)
    }
}

pub(super) fn probe_query_safe(db: &BicDb, query: &Query) -> bool {
    struct HasExists;
    impl Visitor for HasExists {
        type Break = ();
        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<()> {
            if matches!(expression, Expr::Exists { .. }) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    // Ordinary point reads do not need schema/policy eligibility probes.
    if query.visit(&mut HasExists).is_continue() {
        return false;
    }
    if !super::row_eval::subquery_is_memoizable(db, query) {
        return false;
    }
    struct Check<'a>(&'a BicDb);
    impl Visitor for Check<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if query.for_clause.is_some()
                || query.settings.is_some()
                || query.format_clause.is_some()
                || !query.pipe_operators.is_empty()
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
            if select.into.is_some() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<()> {
            // Domain casts and custom operators can invoke callbacks whose
            // effects are absent from this expression's function-call nodes.
            if matches!(
                expression,
                Expr::BinaryOp {
                    op: BinaryOperator::Custom(_) | BinaryOperator::PGCustomBinaryOperator(_),
                    ..
                }
            ) || matches!(expression, Expr::Cast { data_type, .. } if cast_may_hide_callbacks(data_type))
            {
                return ControlFlow::Break(());
            }
            if let Expr::Function(function) = expression {
                let Ok(name) = object_name_lowercase(&function.name) else {
                    return ControlFlow::Break(());
                };
                if !matches!(
                    name.as_ref(),
                    "count"
                        | "min"
                        | "max"
                        | "sum"
                        | "avg"
                        | "pg_catalog.count"
                        | "pg_catalog.min"
                        | "pg_catalog.max"
                        | "pg_catalog.sum"
                        | "pg_catalog.avg"
                ) {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table {
                name,
                args: None,
                sample: None,
                version: None,
                ..
            } = factor
            {
                let Ok(name) = relation_name(name) else {
                    return ControlFlow::Break(());
                };
                let resolved = resolve_session_relation_name_if_exists(self.0, &name);
                if matches!(load_schema(self.0, &resolved), Ok(Some(schema)) if !schema.rls_enabled && schema.policies.is_empty())
                {
                    return ControlFlow::Continue(());
                }
            } else if !matches!(factor, TableFactor::Table { .. }) {
                return ControlFlow::Continue(());
            }
            ControlFlow::Break(())
        }
    }
    query.visit(&mut Check(db)).is_continue()
}

fn bounded_numeric(column: &ColumnSchema) -> bool {
    match pg_type_oid_by_name(&column.pg_type) {
        Some(20 | 21 | 23) => true,
        Some(1700) => matches!(
            column.type_modifier,
            Some(PgTypeModifier::Numeric {
                precision: 1..=38,
                scale: -38..=38
            })
        ),
        _ => false,
    }
}

fn numeric_key(db: &BicDb, value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Int(_) | SqlValue::String(_) => {
            pg_typed_index_label_for_db(db, "numeric", value).ok()
        }
        _ => None,
    }
}

fn parts(expr: &Expr) -> Option<Vec<String>> {
    let names = match unwrap_nested_expr(expr) {
        Expr::Identifier(name) => vec![ident_value(name)],
        Expr::CompoundIdentifier(names) => names.iter().map(ident_value).collect(),
        _ => return None,
    };
    (!names.iter().any(|name| name.contains('.'))).then_some(names)
}

fn numeric_correlation(
    expression: &Expr,
    qualifier: &[String],
    schema: &TableSchema,
    context: &BoundRowContext,
    engine: &SqlEngine<'_>,
    env: &[RelationColumns],
) -> Option<(Expr, usize, Vec<Expr>)> {
    fn inner<'a>(
        expression: &'a Expr,
        qualifier: &[String],
        schema: &'a TableSchema,
    ) -> Option<&'a ColumnSchema> {
        let names = parts(expression)?;
        let (column, owner) = names.split_last()?;
        if !(owner.is_empty() || owner == qualifier || owner == &qualifier[qualifier.len() - 1..]) {
            return None;
        }
        schema.column(column).filter(|column| !column.hidden)
    }
    // Residual filters stay on the original subquery. Only admit expressions
    // whose evaluation cannot introduce conversion/collation/function errors:
    // pruning another key must not suppress a fallible residual evaluation.
    fn safe_residual(expression: &Expr, qualifier: &[String], schema: &TableSchema) -> bool {
        match unwrap_nested_expr(expression) {
            Expr::BinaryOp { left, op: BinaryOperator::And | BinaryOperator::Or, right } =>
                safe_residual(left, qualifier, schema) && safe_residual(right, qualifier, schema),
            Expr::UnaryOp { op: UnaryOperator::Not, expr } => safe_residual(expr, qualifier, schema),
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => inner(expr, qualifier, schema).is_some(),
            Expr::BinaryOp { left, op: BinaryOperator::Eq | BinaryOperator::NotEq, right }
                if numeric_literal_comparison(left, right, qualifier, schema) => true,
            Expr::BinaryOp { left, op: BinaryOperator::Eq | BinaryOperator::NotEq, right } => {
                [(left.as_ref(), right.as_ref()), (right.as_ref(), left.as_ref())].into_iter().any(|(column, value)| {
                    inner(column, qualifier, schema).is_some_and(|column|
                        column.user_type.is_none() && column.collation.is_none()
                        && matches!(pg_type_oid_by_name(&column.pg_type), Some(25 | 1042 | 1043))
                        && matches!(unwrap_nested_expr(value), Expr::Value(value) if matches!(value.value, Value::SingleQuotedString(_) | Value::Null)))
                })
            }
            Expr::BinaryOp {
                left,
                op:
                    BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq,
                right,
            } if numeric_literal_comparison(left, right, qualifier, schema) => true,
            Expr::Value(value) => matches!(value.value, Value::Boolean(_) | Value::Null),
            _ => false,
        }
    }
    // A numeric column against a numeric literal cannot raise a conversion
    // error, so evaluating it on more rows than the original is harmless.
    fn numeric_literal_comparison(
        left: &Expr,
        right: &Expr,
        qualifier: &[String],
        schema: &TableSchema,
    ) -> bool {
        [(left, right), (right, left)]
            .into_iter()
            .any(|(column, value)| {
                inner(column, qualifier, schema).is_some_and(|column| {
                    column.user_type.is_none()
                        && matches!(
                            pg_type_oid_by_name(&column.pg_type),
                            Some(20 | 21 | 23 | 1700)
                        )
                        && matches!(unwrap_nested_expr(value), Expr::Value(value)
                        if matches!(value.value, Value::Number(_, _) | Value::Null))
                })
            })
    }
    fn outer(
        expression: &Expr,
        qualifier: &[String],
        schema: &TableSchema,
        context: &BoundRowContext,
    ) -> Option<usize> {
        // The same unqualified name belongs to the inner scope first.
        let names = parts(expression)?;
        if inner(expression, qualifier, schema).is_some() {
            return None;
        }
        context.row_lookup.slot_for_key(&names.join("."))
    }
    fn collect(
        expression: &Expr,
        qualifier: &[String],
        schema: &TableSchema,
        context: &BoundRowContext,
        engine: &SqlEngine<'_>,
        env: &[RelationColumns],
        key: &mut Option<(Expr, usize)>,
        residuals: &mut Vec<Expr>,
    ) -> Option<()> {
        match unwrap_nested_expr(expression) {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                collect(
                    left, qualifier, schema, context, engine, env, key, residuals,
                )?;
                collect(
                    right, qualifier, schema, context, engine, env, key, residuals,
                )
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                for (candidate, outside) in [
                    (left.as_ref(), right.as_ref()),
                    (right.as_ref(), left.as_ref()),
                ] {
                    if let (Some(column), Some(slot)) = (
                        inner(candidate, qualifier, schema),
                        outer(outside, qualifier, schema, context),
                    ) {
                        let outer_type = engine
                            .infer_env_expr_type(outside, env)
                            .or_else(|| (*engine.row_expr_pg_type(outside, context)).clone());
                        let outer_oid = outer_type
                            .as_deref()
                            .and_then(|kind| pg_type_oid_by_name(kind));
                        let inner_oid = pg_type_oid_by_name(&column.pg_type);
                        let numeric = |oid| matches!(oid, Some(20 | 21 | 23 | 1700));
                        let character = |oid| matches!(oid, Some(25 | 1042 | 1043));
                        if !((numeric(inner_oid) && numeric(outer_oid))
                            || (character(inner_oid) && character(outer_oid)))
                        {
                            return None;
                        }
                        if key.is_none() && bounded_numeric(column) {
                            *key = Some((candidate.clone(), slot));
                        }
                        return Some(());
                    }
                }
                safe_residual(expression, qualifier, schema)
                    .then(|| residuals.push(expression.clone()))
            }
            _ => safe_residual(expression, qualifier, schema)
                .then(|| residuals.push(expression.clone())),
        }
    }
    let mut key = None;
    let mut residuals = Vec::new();
    collect(
        expression,
        qualifier,
        schema,
        context,
        engine,
        env,
        &mut key,
        &mut residuals,
    )?;
    key.map(|(expr, slot)| (expr, slot, residuals))
}

impl SqlEngine<'_> {
    /// Returns an equivalent subquery restricted to candidate primary keys, or
    /// declines. Numeric canonicalization may include extra candidates; the
    /// original equality predicates remain responsible for the final result.
    pub(super) fn exists_candidate_query(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        query: &Query,
        env: &[RelationColumns],
    ) -> Result<Option<ExistsCandidate>> {
        if !self.exists_probe_memo.enabled.get() || self.outer_row.is_some() {
            return Ok(None);
        }
        let columns = context.row_lookup.columns();
        let cached = self
            .exists_probe_memo
            .entries
            .borrow()
            .iter()
            .find(|entry| {
                entry.query.as_ref() == query
                    && entry.columns == columns
                    && entry.env.len() == env.len()
                    && entry.env.iter().zip(env).all(|(left, right)| {
                        left.alias == right.alias
                            && left.row_type == right.row_type
                            && left.columns == right.columns
                    })
            })
            .map(|entry| entry.plan.clone());
        let plan = match cached {
            Some(plan) => plan,
            None => {
                if self.exists_probe_memo.entries.borrow().len() >= MAX_PROBE_ENTRIES {
                    return Ok(None);
                }
                let plan = self
                    .build_exists_candidate_probe(context, query, env)?
                    .map(Rc::new);
                self.exists_probe_memo
                    .entries
                    .borrow_mut()
                    .push(ProbeEntry {
                        query: Box::new(query.clone()),
                        columns: columns.to_vec(),
                        env: env.to_vec(),
                        plan: plan.clone(),
                    });
                plan
            }
        };
        let Some(plan) = plan else {
            return Ok(None);
        };
        let Some(value) = row.get(plan.outer_slot) else {
            return Ok(None);
        };
        let keys: Cow<'_, [SqlValue]> = if matches!(value, SqlValue::Null) {
            Cow::Borrowed(&[][..])
        } else {
            match &plan.source {
                CandidateSource::Scanned(keys) => {
                    let Some(key) = numeric_key(self.db_ref(), value) else {
                        return Ok(None);
                    };
                    Cow::Borrowed(keys.get(&key).map(Vec::as_slice).unwrap_or(&[]))
                }
                CandidateSource::Indexed(index) => {
                    match self.indexed_candidates(index, &plan.primary_key, value)? {
                        Some(keys) => Cow::Owned(keys),
                        None => return Ok(None),
                    }
                }
            }
        };
        if keys.len() > MAX_BUCKET_KEYS {
            return Ok(None);
        }
        if keys.is_empty() && plan.empty_verified.get() {
            return Ok(Some(ExistsCandidate::Absent));
        }
        plan.empty_verified
            .set(plan.empty_verified.get() || keys.is_empty());
        let mut limited = query.clone();
        let SetExpr::Select(select) = limited.body.as_mut() else {
            return Ok(None);
        };
        let list = candidate_literals(&keys);
        // Even an empty candidate set executes the original projection and
        // relation authorization path; it is never an authorization shortcut.
        let filter = Expr::InList {
            expr: Box::new(plan.primary_key.clone()),
            list,
            negated: false,
        };
        let Some(original) = select.selection.take() else {
            return Ok(None);
        };
        select.selection = Some(Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(original))),
            op: BinaryOperator::And,
            right: Box::new(filter),
        });
        Ok(Some(ExistsCandidate::Query(limited)))
    }

    fn build_exists_candidate_probe(
        &self,
        context: &BoundRowContext,
        query: &Query,
        env: &[RelationColumns],
    ) -> Result<Option<CandidateProbe>> {
        if query.with.is_some()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
            || query.limit_clause.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
        {
            return Ok(None);
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        if select.top.is_some()
            || select.into.is_some()
            || select.exclude.is_some()
            || select.prewhere.is_some()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || select.select_modifiers.is_some()
            || !select.lateral_views.is_empty()
            || !select.connect_by.is_empty()
            || !select.named_window.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || !select.optimizer_hints.is_empty()
        {
            return Ok(None);
        }
        if select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.distinct.is_some()
            || select.having.is_some()
            || !matches!(&select.group_by, GroupByExpr::Expressions(values, modifiers) if values.is_empty() && modifiers.is_empty())
        {
            return Ok(None);
        }
        let TableFactor::Table {
            name,
            alias,
            args: None,
            sample: None,
            version: None,
            partitions,
            with_hints,
            with_ordinality: false,
            json_path: None,
            index_hints,
        } = &select.from[0].relation
        else {
            return Ok(None);
        };
        if !partitions.is_empty()
            || !with_hints.is_empty()
            || !index_hints.is_empty()
            || alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
        {
            return Ok(None);
        }
        let name_string = relation_name(name)?;
        let resolved = resolve_session_relation_name_if_exists(self.db_ref(), &name_string);
        let Some(schema) = load_schema(self.db_ref(), &resolved)? else {
            return Ok(None);
        };
        if schema.partitioning.is_some() || schema.rls_enabled || !schema.policies.is_empty() {
            return Ok(None);
        }
        let primary_names = primary_key_columns_for_schema(&schema);
        let [primary_name] = primary_names.as_slice() else {
            return Ok(None);
        };
        let Some(primary) = schema
            .column(primary_name)
            .filter(|column| !column.hidden && bounded_numeric(column))
        else {
            return Ok(None);
        };
        let qualifier = alias
            .as_ref()
            .map(|alias| vec![ident_value(&alias.name)])
            .unwrap_or_else(|| object_name_parts(name));
        if qualifier.is_empty() {
            return Ok(None);
        }
        let Some(selection) = select.selection.as_ref() else {
            return Ok(None);
        };
        let Some((key_expr, outer_slot, residuals)) =
            numeric_correlation(selection, &qualifier, &schema, context, self, env)
        else {
            return Ok(None);
        };
        let mut primary_parts = qualifier
            .iter()
            .map(|name| Ident::with_quote('"', name))
            .collect::<Vec<_>>();
        primary_parts.push(Ident::with_quote('"', &primary.name));
        let primary_key = Expr::CompoundIdentifier(primary_parts);
        let mut scan = query.clone();
        let SetExpr::Select(projection) = scan.body.as_mut() else {
            return Ok(None);
        };
        projection.projection = vec![
            SelectItem::UnnamedExpr(key_expr),
            SelectItem::UnnamedExpr(primary_key.clone()),
        ];
        // Infallible residual filters narrow the candidates to keys some
        // qualifying inner row has; correlated equalities stay per row.
        projection.selection = residuals.into_iter().reduce(|left, right| Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(left))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(right))),
        });
        scan.order_by = None;
        scan.limit_clause = Some(LimitClause::LimitOffset {
            limit: Some(Expr::Value(
                Value::Number((MAX_PROBE_ROWS + 1).to_string(), false).into(),
            )),
            offset: None,
            limit_by: Vec::new(),
        });
        // A scan decodes every inner row in full. When an index holds the
        // correlation column (even as a non-leading key column), read its
        // entries instead and fetch only the rows outer values ask for.
        if let Some(indexed) = self.indexed_exists_candidates(
            &resolved,
            &qualifier[qualifier.len() - 1],
            &schema,
            &primary.name,
            &scan,
            &primary_key,
        )? {
            return Ok(Some(CandidateProbe {
                outer_slot,
                primary_key,
                source: CandidateSource::Indexed(indexed),
                empty_verified: Cell::new(false),
            }));
        }
        let result = match self.execute_query(&scan) {
            Ok(result) => result,
            Err(_) => {
                self.check_cancellation()?;
                return Ok(None);
            }
        };
        if result.rows.len() > MAX_PROBE_ROWS
            || sql_result_memory_estimate(&result) > MAX_PROBE_BYTES
        {
            return Ok(None);
        }
        let mut keys = BTreeMap::<String, Vec<SqlValue>>::new();
        let mut bytes = 0usize;
        for (index, values) in result.rows.into_iter().enumerate() {
            if index % 1024 == 0 {
                self.check_cancellation()?;
            }
            let [value, primary] = values.as_slice() else {
                return Ok(None);
            };
            if matches!(value, SqlValue::Null) {
                continue;
            }
            let Some(key) = numeric_key(self.db_ref(), value) else {
                return Ok(None);
            };
            bytes = bytes.saturating_add(key.len() + sql_value_memory_estimate(primary) + 128);
            if bytes > MAX_PROBE_BYTES {
                return Ok(None);
            }
            keys.entry(key).or_default().push(primary.clone());
        }
        Ok(Some(CandidateProbe {
            outer_slot,
            primary_key,
            source: CandidateSource::Scanned(keys),
            empty_verified: Cell::new(false),
        }))
    }

    /// Index-only candidates for the correlation column projected first by
    /// `scan`. Candidates are a superset: every visible inner row whose
    /// column equals an outer value is filed under that value's index key
    /// (the property every index locator relies on), and the original
    /// subquery still decides each outer row.
    fn indexed_exists_candidates(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        primary_name: &str,
        scan: &Query,
        primary_key: &Expr,
    ) -> Result<Option<IndexedCandidates>> {
        // As for the rowid fast path: the committed index is authoritative
        // only without a security context and without this transaction's
        // buffered writes to the table.
        if self.security_context.is_some() || self.tx.is_some_and(|tx| tx.has_pending_writes(table))
        {
            return Ok(None);
        }
        let SetExpr::Select(select) = scan.body.as_ref() else {
            return Ok(None);
        };
        let Some(SelectItem::UnnamedExpr(key_expr)) = select.projection.first() else {
            return Ok(None);
        };
        // The narrowest plain B-tree index that holds the column. Partial
        // indexes are already excluded: they omit rows.
        let mut best: Option<(IndexDefinition, usize)> = None;
        for index in sql_index_definitions_for_collection(self.db_ref(), table) {
            if index.kind != IndexKind::BTree
                || index.predicate.is_some()
                || index.exclusion.is_some()
                || best
                    .as_ref()
                    .is_some_and(|(chosen, _)| chosen.fields.len() <= index.fields.len())
            {
                continue;
            }
            let mut position = None;
            for (at, field) in index.fields.iter().enumerate() {
                if self.expr_matches_table_index_field(
                    key_expr,
                    table,
                    alias,
                    Some(schema),
                    field,
                )? {
                    position = Some(at);
                    break;
                }
            }
            if let Some(position) = position {
                best = Some((index, position));
            }
        }
        let Some((index, position)) = best else {
            return Ok(None);
        };
        let entries = match self
            .db_ref()
            .index_field_entries(&index.name, position, MAX_PROBE_ROWS)
        {
            Ok(Some(entries)) => entries,
            Ok(None) | Err(_) => return Ok(None),
        };
        let mut grouped = BTreeMap::<IndexValue, Vec<String>>::new();
        let mut bytes = 0usize;
        for (at, (value, id)) in entries.into_iter().enumerate() {
            if at % 1024 == 0 {
                self.check_cancellation()?;
            }
            // NULL never equals an outer value.
            if matches!(value, IndexValue::Null) {
                continue;
            }
            bytes = bytes.saturating_add(id.len() + 128);
            if bytes > MAX_PROBE_BYTES {
                return Ok(None);
            }
            grouped.entry(value).or_default().push(id);
        }
        let residual_scan = select.selection.is_some().then(|| {
            let mut residual = scan.clone();
            if let SetExpr::Select(select) = residual.body.as_mut() {
                select.projection = vec![SelectItem::UnnamedExpr(primary_key.clone())];
            }
            residual
        });
        Ok(Some(IndexedCandidates {
            table: table.to_string(),
            schema: schema.clone(),
            primary_name: primary_name.to_string(),
            field: index.fields[position].clone(),
            buckets: RefCell::new(
                grouped
                    .into_iter()
                    .map(|(value, ids)| (value, IndexedBucket::Ids(ids)))
                    .collect(),
            ),
            residual_scan,
        }))
    }

    /// The candidate primary keys for one non-NULL outer value, or `None`
    /// to decline to the ordinary evaluator.
    fn indexed_candidates(
        &self,
        index: &IndexedCandidates,
        primary_key: &Expr,
        value: &SqlValue,
    ) -> Result<Option<Vec<SqlValue>>> {
        // The probe key an ordinary index locator would use for `column = value`.
        let Ok(probe) = index_value_from_sql(value.clone()) else {
            return Ok(None);
        };
        let Ok(probe) = typed_index_predicate_value(&index.schema, &index.field, probe) else {
            return Ok(None);
        };
        let ids = match index.buckets.borrow().get(&probe) {
            None => return Ok(Some(Vec::new())),
            Some(IndexedBucket::Keys(keys)) => return Ok(Some(keys.clone())),
            Some(IndexedBucket::Ids(ids)) => ids.clone(),
        };
        if ids.len() > MAX_BUCKET_KEYS {
            return Ok(None);
        }
        let columns = [index.primary_name.clone()];
        let typed_identity = primary_key_requires_typed_identity(&index.schema);
        let mut keys = Vec::with_capacity(ids.len());
        for id in &ids {
            match self.primary_key_values_for_record_id(
                &index.table,
                &index.schema,
                &columns,
                id,
            )? {
                Some(values) => match <[SqlValue; 1]>::try_from(values) {
                    Ok([key]) => keys.push(key),
                    Err(_) => return Ok(None),
                },
                // A typed identity is read from its row: no visible row, no
                // candidate.
                None if typed_identity => {}
                None => return Ok(None),
            }
        }
        if let (Some(residual), false) = (&index.residual_scan, keys.is_empty()) {
            // Residual filters are infallible (see `numeric_correlation`), so
            // evaluating them on this bucket's rows cannot add an error.
            let mut residual = residual.clone();
            let SetExpr::Select(select) = residual.body.as_mut() else {
                return Ok(None);
            };
            let filter = Expr::InList {
                expr: Box::new(primary_key.clone()),
                list: candidate_literals(&keys),
                negated: false,
            };
            select.selection = Some(match select.selection.take() {
                Some(original) => Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(original))),
                    op: BinaryOperator::And,
                    right: Box::new(filter),
                },
                None => filter,
            });
            let result = match self.execute_query(&residual) {
                Ok(result) => result,
                Err(_) => {
                    self.check_cancellation()?;
                    return Ok(None);
                }
            };
            keys = result
                .rows
                .into_iter()
                .filter_map(|row| row.into_iter().next())
                .collect();
        }
        index
            .buckets
            .borrow_mut()
            .insert(probe, IndexedBucket::Keys(keys.clone()));
        Ok(Some(keys))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exists_candidate_probes_decline_hidden_effects_and_row_policies() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE probe_source(id INTEGER PRIMARY KEY)")
                .unwrap();
            session
                .execute("CREATE TABLE probe_protected(id INTEGER PRIMARY KEY)")
                .unwrap();
            session
                .execute("ALTER TABLE probe_protected ENABLE ROW LEVEL SECURITY")
                .unwrap();
            session.execute("CREATE SEQUENCE probe_sequence").unwrap();
            session
                .execute("CREATE VIEW probe_view AS SELECT nextval('probe_sequence') AS id")
                .unwrap();
        }
        for (sql, expected) in [
            ("SELECT * FROM probe_source r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", true),
            ("SELECT * FROM probe_source", false),
            ("SELECT CAST(r.id AS INTEGER) FROM probe_source r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", true),
            ("SELECT CAST(r.id AS callback_domain) FROM probe_source r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", false),
            ("SELECT CAST(ARRAY[r.id] AS callback_domain[]) FROM probe_source r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", false),
            ("SELECT * FROM probe_source r WHERE r.id OPERATOR(public.##) 1 AND EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", false),

            ("SELECT nextval('probe_sequence') FROM probe_source r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", false),
            ("SELECT * FROM probe_source r WHERE EXISTS(SELECT * FROM probe_view c WHERE c.id=r.id)", false),
            ("SELECT * FROM probe_source r WHERE EXISTS(SELECT * FROM probe_protected c WHERE c.id=r.id)", false),
            ("SELECT * FROM probe_protected r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id)", false),
            ("SELECT * FROM probe_source r WHERE EXISTS(SELECT * FROM probe_source c WHERE c.id=r.id FOR UPDATE)", false),
            ("WITH changed AS (DELETE FROM probe_source RETURNING id) SELECT * FROM probe_source r WHERE EXISTS(SELECT * FROM changed c WHERE c.id=r.id)", false),
        ] {
            let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
            let Statement::Query(query) = &statements[0] else { panic!("expected query"); };
            assert_eq!(probe_query_safe(&db, query), expected, "{sql}");
        }
    }
    #[test]
    fn exists_candidate_probes_decline_hidden_residual_columns() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE TABLE hidden_probe_rows(id INTEGER PRIMARY KEY,period INTEGER,label TEXT)").unwrap();
            session
                .execute("INSERT INTO hidden_probe_rows VALUES(1,1,'present')")
                .unwrap();
        }
        let mut schema = load_schema(&db, "hidden_probe_rows").unwrap().unwrap();
        schema
            .columns
            .iter_mut()
            .find(|column| column.name == "label")
            .unwrap()
            .hidden = true;
        save_schema(&mut db, &schema).unwrap();
        let statements = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT * FROM hidden_probe_rows c WHERE c.period=r.period AND c.label IS NOT NULL",
        )
        .unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("query");
        };
        let engine = SqlEngine::new(&db);
        let columns = vec!["r.period".to_string()];
        let (_scope, context) = engine.bound_row_context(&columns);
        let env = vec![RelationColumns {
            alias: "r".into(),
            row_type: None,
            columns: vec![("period".into(), Some("int4".into()))],
        }];
        assert!(engine
            .build_exists_candidate_probe(&context, query, &env)
            .unwrap()
            .is_none());
    }
}
