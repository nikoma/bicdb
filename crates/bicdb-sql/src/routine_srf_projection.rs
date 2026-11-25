//! A set-returning SQL-language function called in a select list
//! (`SELECT f(x) FROM t`, `SELECT (f()).col`) returns one output row per row
//! of the function for each input row. With a single such call and no
//! grouping, aggregation or window, that is exactly a lateral join of the
//! function, which the FROM-clause executor already runs:
//! `SELECT s.v FROM t, LATERAL f(x) AS s(v)`, the call replaced by `s.v`
//! wherever it stands in the item. Other shapes (several calls, a whole
//! composite row, grouping) keep their existing (refused) handling.
use crate::*;
use sqlparser::ast::{
    AccessExpr, FunctionArgumentList, FunctionArguments, GroupByExpr, Visit, VisitMut, Visitor,
    VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

const SOURCE_ALIAS: &str = "__bicdb_srf";
const VALUE_COLUMN: &str = "__bicdb_srf_value";

/// The set-returning call a select-list item contains: the call, the field
/// selected from its row (`(f()).col`), and whether it is the whole item.
struct Candidate {
    index: usize,
    function: Function,
    field: Option<Ident>,
    whole_item: bool,
}

/// `expr` as a call, possibly under a field selection.
fn call_target(expr: &Expr) -> Option<(&Function, Option<&Ident>)> {
    let mut expr = expr;
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    match expr {
        Expr::Function(function) => Some((function, None)),
        Expr::CompoundFieldAccess { root, access_chain } => {
            let [AccessExpr::Dot(Expr::Identifier(field))] = access_chain.as_slice() else {
                return None;
            };
            let mut root = root.as_ref();
            while let Expr::Nested(inner) = root {
                root = inner;
            }
            match root {
                Expr::Function(function) => Some((function, Some(field))),
                _ => None,
            }
        }
        _ => None,
    }
}

fn plain_call(function: &Function) -> bool {
    function.filter.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
        && matches!(&function.args, FunctionArguments::List(list)
            if list.duplicate_treatment.is_none() && list.clauses.is_empty())
}

fn call_args(function: &Function) -> Vec<FunctionArg> {
    match &function.args {
        FunctionArguments::List(list) => list.args.clone(),
        _ => Vec::new(),
    }
}

/// Whether `routine` returns a set of a base (non-composite) type.
fn returns_scalar_set(db: &BicDb, name: &str, routine: &RoutineSchema) -> bool {
    routine.return_type != "record"
        && pg_type_oid_by_name(&routine.return_type).is_some()
        && crate::engine::materialize::declared_table_function_columns(db, name).is_none()
}

/// Finds (and, with `replacement`, replaces) set-returning SQL function
/// calls in one select-list item, outside any subquery.
struct CallFinder<'a> {
    db: &'a BicDb,
    depth: usize,
    calls: Vec<(Function, Option<Ident>)>,
    /// A call this rewrite cannot place (a whole composite row).
    blocked: bool,
    replacement: Option<Expr>,
}

impl CallFinder<'_> {
    fn set_returning(&mut self, expr: &Expr) -> Option<(Function, Option<Ident>)> {
        if self.depth > 0 {
            return None;
        }
        let (function, field) = call_target(expr)?;
        if !plain_call(function) {
            return None;
        }
        let name = object_name(&function.name).ok()?;
        // A lookup that fails leaves the query to its normal handling.
        let routine = resolve_routine_cached(self.db, RoutineKind::Function, &name).ok()??;
        let routine = &routine.schema;
        if !routine.returns_set || !routine.language.eq_ignore_ascii_case("sql") {
            return None;
        }
        if field.is_none() && !returns_scalar_set(self.db, &name, routine) {
            self.blocked = true;
            return None;
        }
        Some((function.clone(), field.cloned()))
    }
}

impl VisitorMut for CallFinder<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        // Pre-order, so `(f()).col` is taken whole rather than as `f()`;
        // the replaced node has no call left to visit.
        let Some(call) = self.set_returning(expr) else {
            return ControlFlow::Continue(());
        };
        *expr = self
            .replacement
            .clone()
            .unwrap_or_else(|| Expr::value(Value::Null));
        self.calls.push(call);
        ControlFlow::Continue(())
    }
}

pub(crate) fn rewrite_routine_set_returning_projection(
    db: &BicDb,
    query: &Query,
) -> Result<Option<Query>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if !select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
        )
    }) {
        return Ok(None);
    }
    let mut candidate = None;
    for (index, item) in select.projection.iter().enumerate() {
        let mut expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            _ => continue,
        };
        let item_call =
            call_target(&expr).map(|(function, field)| (function.clone(), field.cloned()));
        let mut finder = CallFinder {
            db,
            depth: 0,
            calls: Vec::new(),
            blocked: false,
            replacement: None,
        };
        let _ = VisitMut::visit(&mut expr, &mut finder);
        if finder.blocked {
            return Ok(None);
        }
        let mut calls = finder.calls;
        match calls.len() {
            0 => continue,
            1 if candidate.is_none() => {
                let (function, field) = calls.pop().expect("one call");
                let whole_item = item_call.as_ref().is_some_and(|(found, found_field)| {
                    *found == function && *found_field == field
                });
                candidate = Some(Candidate {
                    index,
                    function,
                    field,
                    whole_item,
                });
            }
            // Several set-returning calls advance in lockstep; not a join.
            _ => return Ok(None),
        }
    }
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let grouped = !matches!(&select.group_by, GroupByExpr::Expressions(expressions, modifiers)
        if expressions.is_empty() && modifiers.is_empty());
    if grouped
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || has_aggregates(&select.projection)
    {
        return Ok(None);
    }
    // The source factor, built from a parsed template so every AST field
    // has its parser default.
    let template = format!(
        "SELECT 1 FROM {}{}() AS {SOURCE_ALIAS}{}",
        if select.from.is_empty() {
            ""
        } else {
            "LATERAL "
        },
        candidate.function.name,
        if candidate.field.is_some() {
            String::new()
        } else {
            format!("({VALUE_COLUMN})")
        }
    );
    let Ok(mut parsed) = Parser::parse_sql(&PostgreSqlDialect {}, &template) else {
        return Ok(None);
    };
    let Some(Statement::Query(parsed)) = parsed.pop() else {
        return Ok(None);
    };
    let SetExpr::Select(mut parsed) = *parsed.body else {
        return Ok(None);
    };
    let Some(mut source) = parsed.from.pop() else {
        return Ok(None);
    };
    let call_name = candidate.function.name.clone();
    let arguments = call_args(&candidate.function);
    match &mut source.relation {
        TableFactor::Function { name, args, .. } => {
            *name = call_name;
            *args = arguments;
        }
        TableFactor::Table {
            name,
            args: Some(args),
            ..
        } => {
            *name = call_name;
            args.args = arguments;
        }
        _ => return Ok(None),
    }
    let column = candidate
        .field
        .clone()
        .unwrap_or_else(|| Ident::new(VALUE_COLUMN));
    let reference = Expr::CompoundIdentifier(vec![Ident::new(SOURCE_ALIAS), column]);
    let mut rewritten = query.clone();
    let SetExpr::Select(select) = rewritten.body.as_mut() else {
        return Ok(None);
    };
    let item = &mut select.projection[candidate.index];
    *item = match std::mem::replace(item, SelectItem::UnnamedExpr(Expr::value(Value::Null))) {
        SelectItem::ExprWithAlias { mut expr, alias } => {
            replace_call(db, &mut expr, &reference)?;
            SelectItem::ExprWithAlias { expr, alias }
        }
        SelectItem::UnnamedExpr(mut expr) => {
            if candidate.whole_item {
                // The column keeps the name PostgreSQL gives the call.
                let name = candidate.field.clone().unwrap_or_else(|| {
                    let name = object_name(&candidate.function.name).unwrap_or_default();
                    Ident::new(name.rsplit('.').next().unwrap_or(&name))
                });
                SelectItem::ExprWithAlias {
                    expr: reference.clone(),
                    alias: name,
                }
            } else {
                replace_call(db, &mut expr, &reference)?;
                SelectItem::UnnamedExpr(expr)
            }
        }
        other => other,
    };
    select.from.push(source);
    // ORDER BY may name the call itself (an ordinal or output alias is
    // resolved to the item's expression before this rewrite runs).
    if let Some(order_by) = rewritten.order_by.as_mut() {
        let mut finder = CallFinder {
            db,
            depth: 0,
            calls: Vec::new(),
            blocked: false,
            replacement: Some(reference),
        };
        let _ = VisitMut::visit(order_by, &mut finder);
    }
    Ok(Some(rewritten))
}

fn replace_call(db: &BicDb, expr: &mut Expr, reference: &Expr) -> Result<()> {
    let mut finder = CallFinder {
        db,
        depth: 0,
        calls: Vec::new(),
        blocked: false,
        replacement: Some(reference.clone()),
    };
    let _ = VisitMut::visit(expr, &mut finder);
    Ok(())
}

/// hstore's `populate_record(base, h)` is `jsonb_populate_record(base,
/// hstore_to_jsonb(h))`: the same keys-override-attributes rule over the
/// pairs' text values. Rewritten in expressions and FROM so both forms take
/// the jsonb implementation (and its row-type resolution).
pub(crate) fn rewrite_hstore_populate_record(query: &Query) -> Option<Query> {
    struct Finder(bool);
    impl Visitor for Finder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if let Expr::Function(function) = expr {
                self.0 |= is_populate_record(&function.name);
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Function { name, .. }
            | TableFactor::Table {
                name,
                args: Some(_),
                ..
            } = factor
            {
                self.0 |= is_populate_record(name);
            }
            ControlFlow::Continue(())
        }
    }
    struct Rename;
    impl VisitorMut for Rename {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if let Expr::Function(function) = expr {
                if is_populate_record(&function.name) {
                    if let FunctionArguments::List(list) = &mut function.args {
                        wrap_second_argument(&mut list.args);
                    }
                    function.name = ObjectName::from(vec![Ident::new("jsonb_populate_record")]);
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
            match factor {
                TableFactor::Function { name, args, .. } if is_populate_record(name) => {
                    wrap_second_argument(args);
                    *name = ObjectName::from(vec![Ident::new("jsonb_populate_record")]);
                }
                TableFactor::Table {
                    name,
                    args: Some(args),
                    ..
                } if is_populate_record(name) => {
                    wrap_second_argument(&mut args.args);
                    *name = ObjectName::from(vec![Ident::new("jsonb_populate_record")]);
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    let mut finder = Finder(false);
    let _ = Visit::visit(query, &mut finder);
    if !finder.0 {
        return None;
    }
    let mut rewritten = query.clone();
    let _ = VisitMut::visit(&mut rewritten, &mut Rename);
    Some(rewritten)
}

fn is_populate_record(name: &ObjectName) -> bool {
    object_name(name).is_ok_and(|name| {
        let name = name.to_ascii_lowercase();
        matches!(name.as_str(), "populate_record" | "public.populate_record")
    })
}

fn wrap_second_argument(args: &mut [FunctionArg]) {
    if args.len() != 2 {
        return;
    }
    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &mut args[1] {
        let inner = std::mem::replace(expr, Expr::value(Value::Null));
        *expr = Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("hstore_to_jsonb")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
                clauses: Vec::new(),
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: Vec::new(),
        });
    }
}
