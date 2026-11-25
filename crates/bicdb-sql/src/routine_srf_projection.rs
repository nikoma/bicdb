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

/// The FROM source the rewrite adds: `__bicdb_srf_zip(call, ...)` runs the
/// calls in lockstep (see `set_returning_zip_row_set`).
pub(crate) const ZIP_FUNCTION: &str = "__bicdb_srf_zip";

/// The zip source's column for call `index`.
pub(crate) fn zip_column(index: usize) -> String {
    format!("{VALUE_COLUMN}_{index}")
}

/// A zip argument as its call and selected field.
pub(crate) fn zip_call(expr: &Expr) -> Option<(&Function, Option<&Ident>)> {
    call_target(expr)
}

/// The argument expressions of a zip call.
pub(crate) fn zip_call_args(function: &Function) -> Vec<Expr> {
    call_args(function)
        .into_iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
            _ => None,
        })
        .collect()
}

/// Whether the set-returning function `name` returns rows (a composite)
/// rather than one base-type value per row.
pub(crate) fn returns_row(db: &BicDb, name: &str) -> Result<bool> {
    Ok(resolve_routine_cached(db, RoutineKind::Function, name)?
        .is_some_and(|routine| !returns_scalar_set(db, name, &routine.schema)))
}

/// The field types of the rows `name` returns, for a whole-row value.
pub(crate) fn row_field_types(db: &BicDb, name: &str, count: usize) -> Result<Vec<Option<String>>> {
    if let Some(declared) = crate::engine::materialize::declared_table_function_columns(db, name) {
        return Ok(declared.into_iter().map(|(_, pg_type)| pg_type).collect());
    }
    if let Some(routine) = resolve_routine_cached(db, RoutineKind::Function, name)? {
        if let Some(schema) = load_schema(db, &routine.schema.return_type)? {
            return Ok(schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(|column| Some(column.pg_type.clone()))
                .collect());
        }
    }
    Ok(vec![None; count])
}

/// Finds (and, with `replacement`, replaces) set-returning SQL function
/// calls in one select-list item, outside any subquery.
struct CallFinder<'a> {
    db: &'a BicDb,
    depth: usize,
    calls: Vec<Expr>,
    /// Replaces call `n` (counted from `first`) with its zip column.
    replacement: Option<usize>,
    first: usize,
}

impl CallFinder<'_> {
    fn set_returning(&mut self, expr: &Expr) -> bool {
        if self.depth > 0 {
            return false;
        }
        let Some((function, _)) = call_target(expr) else {
            return false;
        };
        if !plain_call(function) {
            return false;
        }
        let Ok(name) = object_name(&function.name) else {
            return false;
        };
        // A lookup that fails leaves the query to its normal handling.
        let Ok(Some(routine)) = resolve_routine_cached(self.db, RoutineKind::Function, &name)
        else {
            return false;
        };
        routine.schema.returns_set
            && (routine.schema.language.eq_ignore_ascii_case("sql")
                || routine.schema.language.eq_ignore_ascii_case("plpgsql"))
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
        if !self.set_returning(expr) {
            return ControlFlow::Continue(());
        }
        let index = self.first + self.calls.len();
        let call = std::mem::replace(
            expr,
            match self.replacement {
                Some(_) => Expr::CompoundIdentifier(vec![
                    Ident::new(SOURCE_ALIAS),
                    Ident::new(zip_column(index)),
                ]),
                None => Expr::value(Value::Null),
            },
        );
        self.calls.push(call);
        ControlFlow::Continue(())
    }
}

/// The name PostgreSQL gives a select-list item that is just the call.
fn call_column_name(expr: &Expr) -> Option<Ident> {
    let (function, field) = call_target(expr)?;
    Some(match field {
        Some(field) => field.clone(),
        None => {
            let name = object_name(&function.name).ok()?;
            Ident::new(name.rsplit('.').next().unwrap_or(&name))
        }
    })
}

pub(crate) fn rewrite_routine_set_returning_projection(
    db: &BicDb,
    query: &Query,
) -> Result<Option<Query>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    // Find the calls first (on copies), so a query without any is untouched.
    let mut found = 0usize;
    for item in &select.projection {
        let (SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. }) = item else {
            continue;
        };
        let mut copy = expr.clone();
        let mut finder = CallFinder {
            db,
            depth: 0,
            calls: Vec::new(),
            replacement: None,
            first: 0,
        };
        let _ = VisitMut::visit(&mut copy, &mut finder);
        found += finder.calls.len();
    }
    if found == 0 {
        return Ok(None);
    }
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
    let mut rewritten = query.clone();
    let SetExpr::Select(select) = rewritten.body.as_mut() else {
        return Ok(None);
    };
    let mut calls = Vec::new();
    // Replacement for ORDER BY: each call expression and its zip column.
    let mut call_columns = Vec::new();
    for item in select.projection.iter_mut() {
        let (expr, aliased) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, false),
            SelectItem::ExprWithAlias { expr, .. } => (expr, true),
            _ => continue,
        };
        let name = call_column_name(expr);
        let before = calls.len();
        let mut finder = CallFinder {
            db,
            depth: 0,
            calls: Vec::new(),
            replacement: Some(0),
            first: before,
        };
        let _ = VisitMut::visit(expr, &mut finder);
        for (offset, call) in finder.calls.into_iter().enumerate() {
            call_columns.push((call.clone(), before + offset));
            calls.push(call);
        }
        // A bare call keeps the column name PostgreSQL gives it.
        let bare =
            !aliased && calls.len() == before + 1 && matches!(expr, Expr::CompoundIdentifier(_));
        if let (true, Some(name)) = (bare, name) {
            let expr = expr.clone();
            *item = SelectItem::ExprWithAlias { expr, alias: name };
        }
    }
    // The source factor, built from a parsed template so every AST field
    // has its parser default.
    let template = format!(
        "SELECT 1 FROM {}{ZIP_FUNCTION}() AS {SOURCE_ALIAS}",
        if select.from.is_empty() {
            ""
        } else {
            "LATERAL "
        },
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
    let arguments = calls
        .into_iter()
        .map(|call| FunctionArg::Unnamed(FunctionArgExpr::Expr(call)))
        .collect::<Vec<_>>();
    match &mut source.relation {
        TableFactor::Function { args, .. } => *args = arguments,
        TableFactor::Table {
            args: Some(args), ..
        } => args.args = arguments,
        _ => return Ok(None),
    }
    select.from.push(source);
    // ORDER BY may name a call itself (an ordinal or output alias is
    // resolved to the item's expression before this rewrite runs).
    if let Some(order_by) = rewritten.order_by.as_mut() {
        let mut replace = ReplaceCalls(call_columns);
        let _ = VisitMut::visit(order_by, &mut replace);
    }
    Ok(Some(rewritten))
}

/// Replaces each listed call expression by its zip column.
struct ReplaceCalls(Vec<(Expr, usize)>);

impl VisitorMut for ReplaceCalls {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Some((_, index)) = self.0.iter().find(|(call, _)| call == expr) {
            *expr = Expr::CompoundIdentifier(vec![
                Ident::new(SOURCE_ALIAS),
                Ident::new(zip_column(*index)),
            ]);
        }
        ControlFlow::Continue(())
    }
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
            match expr {
                Expr::Function(function) => self.0 |= is_populate_record(&function.name),
                Expr::BinaryOp { op, .. } => self.0 |= is_hash_equals(op),
                _ => {}
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
            // hstore's `record #= hstore` is populate_record(record, hstore).
            if let Expr::BinaryOp { left, op, right } = expr {
                if is_hash_equals(op) {
                    let mut args = vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                    ];
                    wrap_second_argument(&mut args);
                    *expr = Expr::Nested(Box::new(Expr::Function(Function {
                        name: ObjectName::from(vec![Ident::new("jsonb_populate_record")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args,
                            clauses: Vec::new(),
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Vec::new(),
                    })));
                    return ControlFlow::Continue(());
                }
            }
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

fn is_hash_equals(op: &BinaryOperator) -> bool {
    match op {
        BinaryOperator::Custom(name) => name == "#=",
        BinaryOperator::PGCustomBinaryOperator(parts) => {
            parts.last().is_some_and(|part| part == "#=")
        }
        _ => false,
    }
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
