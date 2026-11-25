//! Set-returning functions in a select list, with PostgreSQL 10+ semantics
//! (ProjectSet): every input row is repeated once per value the functions
//! return, several functions advance in lockstep and the shorter ones are
//! padded with NULL, and a row whose functions all return no rows vanishes.
//!
//! The executors expand built-in functions at the top of a select-list item
//! themselves. A user-defined set-returning function, or one nested inside a
//! larger expression (`generate_series(1, 3) * 10`, `(f(x)).col`), is moved
//! into the FROM clause instead, where it already runs per row:
//!
//! - one function: `SELECT a, f(a) FROM t` runs as
//!   `SELECT a, s.c0 AS f FROM t, LATERAL f(a) AS s(c0)`;
//! - several functions with scalar results run in lockstep as
//!   `LATERAL unnest(ARRAY(SELECT f(a)), ARRAY(SELECT g(a))) AS s(c0, c1)`,
//!   whose multi-argument form pads the shorter sets with NULL exactly as
//!   ProjectSet does.
//!
//! Queries whose select list is computed after grouping (aggregates, GROUP
//! BY, HAVING, DISTINCT, window functions) evaluate set-returning functions
//! after aggregation in PostgreSQL; they are left as written.

use crate::*;
use sqlparser::ast::{
    AccessExpr, Expr, FunctionArguments, GroupByExpr, Query, SelectItem, SetExpr, VisitMut,
    VisitorMut,
};
use std::ops::ControlFlow;

const SRF_ALIAS: &str = "bicdb_srf";

/// Built-in set-returning functions with one output column.
fn builtin_single_column_srf(name: &str, args: usize) -> bool {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    match name {
        "unnest" => args == 1,
        "generate_series"
        | "generate_subscripts"
        | "json_array_elements"
        | "json_array_elements_text"
        | "jsonb_array_elements"
        | "jsonb_array_elements_text"
        | "json_object_keys"
        | "jsonb_object_keys"
        | "regexp_split_to_table"
        | "regexp_matches"
        | "jsonb_path_query" => true,
        _ => false,
    }
}

#[derive(Clone)]
enum SrfShape {
    /// One output value per row.
    Scalar,
    /// Named output columns (`RETURNS TABLE`, OUT parameters).
    Columns(Vec<String>),
}

struct SrfCall {
    call: Expr,
    shape: SrfShape,
    user: bool,
}

struct Collector<'a> {
    db: &'a BicDb,
    calls: Vec<SrfCall>,
    /// Depth inside subqueries, which keep their own select lists.
    query_depth: usize,
}

impl Collector<'_> {
    fn classify(&self, function: &Function) -> Option<(SrfShape, bool)> {
        if function.over.is_some() {
            return None;
        }
        let FunctionArguments::List(list) = &function.args else {
            return None;
        };
        let name = object_name(&function.name).ok()?.to_ascii_lowercase();
        if builtin_single_column_srf(&name, list.args.len()) {
            return Some((SrfShape::Scalar, false));
        }
        let routine = load_routine(self.db, RoutineKind::Function, &name).ok()??;
        if !routine.returns_set {
            return None;
        }
        match crate::engine::materialize::declared_table_function_columns(self.db, &name) {
            Some(columns) => Some((
                SrfShape::Columns(columns.into_iter().map(|(name, _)| name).collect()),
                true,
            )),
            None => Some((SrfShape::Scalar, true)),
        }
    }

    fn index_of(&mut self, call: &Expr, shape: SrfShape, user: bool) -> usize {
        if let Some(index) = self.calls.iter().position(|known| known.call == *call) {
            return index;
        }
        self.calls.push(SrfCall {
            call: call.clone(),
            shape,
            user,
        });
        self.calls.len() - 1
    }
}

fn srf_column(index: usize, column: &str) -> Expr {
    Expr::CompoundIdentifier(vec![
        Ident::new(SRF_ALIAS),
        Ident::new(format!("{column}{index}")),
    ])
}

fn srf_field(name: &str) -> Expr {
    Expr::CompoundIdentifier(vec![Ident::new(SRF_ALIAS), Ident::new(name)])
}

impl VisitorMut for Collector<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.query_depth > 0 {
            return ControlFlow::Continue(());
        }
        // `(f(x)).column` over a function with named output columns.
        if let Expr::CompoundFieldAccess { root, access_chain } = expr {
            let mut inner = root.as_ref();
            while let Expr::Nested(nested) = inner {
                inner = nested;
            }
            if let (Expr::Function(function), [AccessExpr::Dot(Expr::Identifier(field))]) =
                (inner, access_chain.as_slice())
            {
                if let Some((SrfShape::Columns(columns), user)) = self.classify(function) {
                    let field_name = field.value.to_ascii_lowercase();
                    if !columns.contains(&field_name) {
                        return ControlFlow::Continue(());
                    }
                    let call = inner.clone();
                    self.index_of(&call, SrfShape::Columns(columns), user);
                    // The field resolves after all calls are known; mark it.
                    *expr = Expr::CompoundIdentifier(vec![
                        Ident::new(SRF_ALIAS),
                        Ident::new(format!("{}\u{0}{field_name}", call)),
                    ]);
                    return ControlFlow::Continue(());
                }
            }
        }
        if let Expr::Function(function) = expr {
            if let Some((shape, user)) = self.classify(function) {
                let call = expr.clone();
                let index = self.index_of(&call, shape.clone(), user);
                *expr = match shape {
                    SrfShape::Scalar => srf_column(index, "c"),
                    SrfShape::Columns(_) => {
                        // A whole row: PostgreSQL shows it as a record.
                        Expr::CompoundIdentifier(vec![
                            Ident::new(SRF_ALIAS),
                            Ident::new(format!("{call}\u{0}*")),
                        ])
                    }
                };
            }
        }
        ControlFlow::Continue(())
    }
}

/// The output name PostgreSQL gives an unaliased select-list item.
fn item_name(expr: &Expr) -> String {
    let mut inner = expr;
    while let Expr::Nested(nested) = inner {
        inner = nested;
    }
    if let Expr::Function(function) = inner {
        if let Some(part) = function.name.0.last().and_then(|part| part.as_ident()) {
            return part.value.to_ascii_lowercase();
        }
    }
    select_expr_column_name(inner)
}

fn select_is_grouped(select: &Select, query: &Query) -> Result<bool> {
    let grouped = match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
    };
    Ok(grouped
        || select.having.is_some()
        || select.distinct.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                expr_contains_aggregate(expr)
            }
            _ => false,
        })
        || crate::select_exec::select_has_window_functions(select, query)?)
}

/// The rewritten SELECT for `query`, or `None` when its select list needs no
/// FROM-clause expansion.
fn rewrite_select(db: &BicDb, query: &Query) -> Result<Option<Query>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select_is_grouped(select, query)? {
        return Ok(None);
    }
    let mut collector = Collector {
        db,
        calls: Vec::new(),
        query_depth: 0,
    };
    let mut projection = select.projection.clone();
    let mut needs_rewrite = false;
    for item in &mut projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        let original = expr.clone();
        let _ = expr.visit(&mut collector);
        if *expr == original {
            continue;
        }
        // A built-in call that is the whole item is expanded by the
        // executors already; anything else needs the FROM-clause form.
        let whole_builtin = matches!(&original, Expr::Function(function)
            if collector.classify(function).is_some_and(|(_, user)| !user));
        needs_rewrite |= !whole_builtin;
        let replaced = expr.clone();
        if let SelectItem::UnnamedExpr(_) = item {
            *item = SelectItem::ExprWithAlias {
                expr: replaced,
                alias: Ident::with_quote('"', item_name(&original)),
            };
        }
    }
    needs_rewrite |= collector.calls.iter().any(|call| call.user);
    if collector.calls.is_empty() || !needs_rewrite {
        return Ok(None);
    }
    // Resolve the markers left for record outputs and their fields.
    let calls = collector.calls;
    let single = calls.len() == 1;
    if !single
        && calls
            .iter()
            .any(|call| matches!(call.shape, SrfShape::Columns(_)))
    {
        return Err(SqlError::Unsupported(
            "several set-returning functions in a select list are supported only with scalar results"
                .to_string(),
        ));
    }
    struct Resolver<'c> {
        calls: &'c [SrfCall],
    }
    impl VisitorMut for Resolver<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            let Expr::CompoundIdentifier(parts) = expr else {
                return ControlFlow::Continue(());
            };
            let [alias, marker] = parts.as_slice() else {
                return ControlFlow::Continue(());
            };
            if alias.value != SRF_ALIAS {
                return ControlFlow::Continue(());
            }
            let Some((call_text, field)) = marker.value.split_once('\u{0}') else {
                return ControlFlow::Continue(());
            };
            let Some(call) = self
                .calls
                .iter()
                .find(|call| call.call.to_string() == call_text)
            else {
                return ControlFlow::Continue(());
            };
            let SrfShape::Columns(columns) = &call.shape else {
                return ControlFlow::Continue(());
            };
            *expr = if field == "*" {
                Expr::Function(Function {
                    name: ObjectName::from(vec![Ident::new("ROW")]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args: columns
                            .iter()
                            .map(|column| {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(srf_field(column)))
                            })
                            .collect(),
                        clauses: Vec::new(),
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: Vec::new(),
                })
            } else {
                srf_field(field)
            };
            ControlFlow::Continue(())
        }
    }
    let mut resolver = Resolver { calls: &calls };
    for item in &mut projection {
        let _ = item.visit(&mut resolver);
    }
    let source = if single {
        let call = &calls[0];
        let columns = match &call.shape {
            SrfShape::Scalar => "c0".to_string(),
            SrfShape::Columns(columns) => columns
                .iter()
                .map(|column| format!("\"{}\"", column.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", "),
        };
        format!("{} AS {SRF_ALIAS}({columns})", call.call)
    } else {
        let arrays = calls
            .iter()
            .map(|call| format!("ARRAY(SELECT x.c0 FROM {} AS x(c0))", call.call))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = (0..calls.len())
            .map(|index| format!("c{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("unnest({arrays}) AS {SRF_ALIAS}({columns})")
    };
    let from = if select.from.is_empty() {
        source
    } else {
        format!(
            "{}, LATERAL {source}",
            select
                .from
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let probe = format!("SELECT 1 FROM {from}");
    let mut parsed = parse_statements(&probe)?;
    let Some(Statement::Query(parsed_query)) = parsed.pop() else {
        return Ok(None);
    };
    let SetExpr::Select(parsed_select) = *parsed_query.body else {
        return Ok(None);
    };
    let mut rewritten = query.clone();
    let SetExpr::Select(target) = rewritten.body.as_mut() else {
        return Ok(None);
    };
    target.projection = projection;
    target.from = parsed_select.from;
    Ok(Some(rewritten))
}

struct QueryRewriter<'a> {
    db: &'a BicDb,
    changed: bool,
    error: Option<SqlError>,
}

impl VisitorMut for QueryRewriter<'_> {
    type Break = ();

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        match rewrite_select(self.db, query) {
            Ok(Some(rewritten)) => {
                *query = rewritten;
                self.changed = true;
            }
            Ok(None) => {}
            Err(error) => {
                self.error = Some(error);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

/// Whether `statement` mentions a set-returning call that the select list
/// cannot expand by itself (cheap pre-check before cloning the statement).
fn mentions_candidate(db: &BicDb, statement: &Statement) -> Result<bool> {
    use sqlparser::ast::{Visit, Visitor};
    struct Probe<'a> {
        db: &'a BicDb,
        found: bool,
    }
    impl Visitor for Probe<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let function = match expr {
                Expr::Function(function) => function,
                _ => return ControlFlow::Continue(()),
            };
            let Ok(name) = object_name(&function.name) else {
                return ControlFlow::Continue(());
            };
            let name = name.to_ascii_lowercase();
            let bare = name.strip_prefix("pg_catalog.").unwrap_or(&name);
            if matches!(
                bare,
                "unnest"
                    | "generate_series"
                    | "generate_subscripts"
                    | "json_array_elements"
                    | "json_array_elements_text"
                    | "jsonb_array_elements"
                    | "jsonb_array_elements_text"
                    | "json_object_keys"
                    | "jsonb_object_keys"
                    | "regexp_split_to_table"
                    | "regexp_matches"
                    | "jsonb_path_query"
            ) || matches!(
                load_routine(self.db, RoutineKind::Function, &name),
                Ok(Some(routine)) if routine.returns_set
            ) {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut probe = Probe { db, found: false };
    let _ = statement.visit(&mut probe);
    Ok(probe.found)
}

/// Move the select-list set-returning calls of `statement` into its FROM
/// clause (see the module documentation); `Some` when anything changed.
pub(crate) fn bind_statement_set_returning_calls(
    db: &BicDb,
    statement: &Statement,
) -> Result<Option<Statement>> {
    if !matches!(statement, Statement::Query(_)) || !mentions_candidate(db, statement)? {
        return Ok(None);
    }
    let mut bound = statement.clone();
    let mut rewriter = QueryRewriter {
        db,
        changed: false,
        error: None,
    };
    let _ = bound.visit(&mut rewriter);
    if let Some(error) = rewriter.error {
        return Err(error);
    }
    Ok(rewriter.changed.then_some(bound))
}
