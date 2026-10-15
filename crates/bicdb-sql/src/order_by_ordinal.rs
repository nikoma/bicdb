//! Query normalization before planning.
//!
//! 1. Left-nested joins as pg_dump writes them, `FROM ((a JOIN b ON x) JOIN c ON y)`, are the same as the
//!    flat chain `FROM a JOIN b ON x JOIN c ON y`; the nested form was evaluated as an opaque row set
//!    (no join planning, 25-100x slower), so it is flattened.
//! 2. ORDER BY output-column references. PostgreSQL resolves an integer constant in ORDER BY to that
//! output column (1-based), and a bare name first to an output column alias (`SELECT b AS y ...
//! ORDER BY y`). BicDB sorted by the constant / ignored the alias on several paths. This rewrites such
//! keys — at every query level: subqueries, CTEs, UNION branches — into the expression or output column
//! they name, before planning, so every execution path sees an ordinary sort expression.
use crate::*;
use sqlparser::ast::{
    CeilFloorKind, DateTimeField, FunctionArgumentList, OrderByExpr, Visit, VisitMut, Visitor,
    VisitorMut,
};
use std::ops::ControlFlow;

/// Output column names of a FROM relation, for `SELECT *` (None when unknown).
pub(crate) type RelationColumns<'a> = dyn Fn(&TableFactor) -> Option<Vec<String>> + 'a;

fn ordinal(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse::<usize>().ok(),
            _ => None,
        },
        Expr::Nested(inner) => ordinal(inner),
        _ => None,
    }
}

/// An identifier as PostgreSQL names the column: unquoted names fold to lower case.
fn folded(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

fn order_expressions(query: &Query) -> Option<&Vec<OrderByExpr>> {
    match &query.order_by.as_ref()?.kind {
        OrderByKind::Expressions(expressions) => Some(expressions),
        _ => None,
    }
}

/// The select-list expression a bare ORDER BY name refers to, when it is an output alias.
fn alias_target(body: &SetExpr, key: &Expr) -> Option<Expr> {
    let (SetExpr::Select(select), Expr::Identifier(name)) = (body, key) else {
        return None;
    };
    let name = folded(name);
    let mut matches = select.projection.iter().filter_map(|item| match item {
        SelectItem::ExprWithAlias { expr, alias } if folded(alias) == name => Some(expr),
        _ => None,
    });
    let expr = matches.next()?;
    // An alias naming a plain column of the same name needs no rewrite; ambiguous aliases are left
    // for the engine to report.
    if matches.next().is_some() || matches!(expr, Expr::Identifier(ident) if folded(ident) == name)
    {
        return None;
    }
    Some(expr.clone())
}

fn rewritable(query: &Query, key: &Expr) -> bool {
    ordinal(key).is_some() || alias_target(query.body.as_ref(), key).is_some()
}

/// `GROUP BY 1`: positional grouping keys in this body (or the SELECTs of a set operation).
fn body_has_group_ordinal(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => {
            matches!(&select.group_by, GroupByExpr::Expressions(keys, _) if keys.iter().any(|key| ordinal(key).is_some()))
        }
        SetExpr::SetOperation { left, right, .. } => {
            body_has_group_ordinal(left) || body_has_group_ordinal(right)
        }
        _ => false,
    }
}

/// Replaces positional GROUP BY keys with the select-list expressions they name.
fn rewrite_group_ordinals(body: &mut SetExpr, columns: &RelationColumns<'_>) -> bool {
    match body {
        SetExpr::Select(select) => {
            let GroupByExpr::Expressions(keys, _) = &select.group_by else {
                return false;
            };
            if !keys.iter().any(|key| ordinal(key).is_some()) {
                return false;
            }
            let items = select_items(select, columns);
            let mut changed = false;
            let targets = keys
                .iter()
                .map(|key| {
                    ordinal(key)
                        .filter(|position| *position > 0)
                        .and_then(|position| {
                            items.get(position - 1).map(|item| match item {
                                Item::Expr(expr) => expr.clone(),
                                Item::Column(name) => {
                                    Expr::Identifier(Ident::with_quote('"', name.clone()))
                                }
                            })
                        })
                })
                .collect::<Vec<_>>();
            if let GroupByExpr::Expressions(keys, _) = &mut select.group_by {
                for (key, target) in keys.iter_mut().zip(targets) {
                    if let Some(target) = target {
                        *key = target;
                        changed = true;
                    }
                }
            }
            changed
        }
        SetExpr::SetOperation { left, right, .. } => {
            let left = rewrite_group_ordinals(left, columns);
            rewrite_group_ordinals(right, columns) || left
        }
        _ => false,
    }
}

/// A FROM item whose relation is an unaliased parenthesized join: `((a JOIN b) JOIN c)`.
fn left_nested(table: &TableWithJoins) -> bool {
    matches!(&table.relation, TableFactor::NestedJoin { alias: None, .. })
}

fn body_has_left_nested_join(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select.from.iter().any(left_nested),
        SetExpr::SetOperation { left, right, .. } => {
            body_has_left_nested_join(left) || body_has_left_nested_join(right)
        }
        _ => false,
    }
}

/// `((a JOIN b ON x) JOIN c ON y)` -> `a JOIN b ON x JOIN c ON y` (left-deep association is unchanged).
fn flatten_table(table: &mut TableWithJoins) -> bool {
    let mut changed = false;
    while let TableFactor::NestedJoin {
        table_with_joins,
        alias: None,
    } = &mut table.relation
    {
        let inner = std::mem::replace(
            table_with_joins.as_mut(),
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(Vec::new()),
                    alias: None,
                    args: None,
                    with_hints: Vec::new(),
                    version: None,
                    with_ordinality: false,
                    partitions: Vec::new(),
                    json_path: None,
                    sample: None,
                    index_hints: Vec::new(),
                },
                joins: Vec::new(),
            },
        );
        let outer_joins = std::mem::take(&mut table.joins);
        table.relation = inner.relation;
        table.joins = inner.joins;
        table.joins.extend(outer_joins);
        changed = true;
    }
    changed
}

fn flatten_body(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => {
            let mut changed = false;
            for table in &mut select.from {
                changed |= flatten_table(table);
            }
            changed
        }
        SetExpr::SetOperation { left, right, .. } => {
            let left = flatten_body(left);
            flatten_body(right) || left
        }
        _ => false,
    }
}

/// `make_interval(secs => 5)`: PostgreSQL's named arguments, in its parameter order.
const MAKE_INTERVAL_PARAMS: [&str; 7] =
    ["years", "months", "weeks", "days", "hours", "mins", "secs"];

/// A plain function call node `name(args...)`.
pub(crate) fn function_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// `CEIL(x)` / `FLOOR(x)` parse to dedicated nodes the row evaluator does not handle; as calls they
/// reach the scalar function implementations.
fn plain_ceil_floor(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Ceil {
            field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
            ..
        } | Expr::Floor {
            field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
            ..
        }
    )
}

fn ceil_floor_as_call(expr: &mut Expr) -> bool {
    if !plain_ceil_floor(expr) {
        return false;
    }
    let (name, inner) = match expr {
        Expr::Ceil { expr: inner, .. } => (
            "ceil",
            std::mem::replace(inner.as_mut(), Expr::value(Value::Null)),
        ),
        Expr::Floor { expr: inner, .. } => (
            "floor",
            std::mem::replace(inner.as_mut(), Expr::value(Value::Null)),
        ),
        _ => return false,
    };
    *expr = function_call(name, vec![inner]);
    true
}

fn named_make_interval(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    let name = function.name.to_string().to_ascii_lowercase();
    if name != "make_interval" && name != "pg_catalog.make_interval" {
        return false;
    }
    matches!(&function.args, FunctionArguments::List(list) if list.args.iter().any(|arg| matches!(arg, FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. })))
}

fn positional_make_interval(expr: &mut Expr) -> bool {
    if !named_make_interval(expr) {
        return false;
    }
    let Expr::Function(function) = expr else {
        return false;
    };
    let FunctionArguments::List(list) = &mut function.args else {
        return false;
    };
    let zero = || {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(Value::Number(
            "0".to_string(),
            false,
        ))))
    };
    let mut slots: Vec<FunctionArg> = (0..MAKE_INTERVAL_PARAMS.len()).map(|_| zero()).collect();
    let mut position = 0usize;
    for arg in list.args.drain(..) {
        match arg {
            FunctionArg::Named { name, arg, .. } => {
                if let Some(index) = MAKE_INTERVAL_PARAMS
                    .iter()
                    .position(|param| name.value.eq_ignore_ascii_case(param))
                {
                    slots[index] = FunctionArg::Unnamed(arg);
                }
            }
            FunctionArg::ExprNamed {
                name: Expr::Identifier(name),
                arg,
                ..
            } => {
                if let Some(index) = MAKE_INTERVAL_PARAMS
                    .iter()
                    .position(|param| name.value.eq_ignore_ascii_case(param))
                {
                    slots[index] = FunctionArg::Unnamed(arg);
                }
            }
            unnamed => {
                if position < slots.len() {
                    slots[position] = unnamed;
                }
                position += 1;
            }
        }
    }
    list.args = slots;
    true
}

struct Finder(bool);
impl Visitor for Finder {
    type Break = ();
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if named_make_interval(expr) || plain_ceil_floor(expr) {
            self.0 = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if body_has_left_nested_join(query.body.as_ref())
            || body_has_group_ordinal(query.body.as_ref())
            || order_expressions(query)
                .is_some_and(|keys| keys.iter().any(|key| rewritable(query, &key.expr)))
        {
            self.0 = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Cheap check before cloning: does any query level order by position?
pub(crate) fn query_has_ordinal_order_by(query: &Query) -> bool {
    let mut finder = Finder(false);
    let _ = Visit::visit(query, &mut finder);
    finder.0
}

/// The query with every positional ORDER BY key replaced; None when nothing could be rewritten.
pub(crate) fn rewrite_ordinal_order_by(
    query: &Query,
    columns: &RelationColumns<'_>,
) -> Option<Query> {
    if !query_has_ordinal_order_by(query) {
        return None;
    }
    let mut rewritten = query.clone();
    let mut rewriter = Rewriter {
        columns,
        changed: false,
    };
    let _ = VisitMut::visit(&mut rewritten, &mut rewriter);
    rewriter.changed.then_some(rewritten)
}

struct Rewriter<'a, 'b> {
    columns: &'a RelationColumns<'b>,
    changed: bool,
}

impl VisitorMut for Rewriter<'_, '_> {
    type Break = ();
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if positional_make_interval(expr) || ceil_floor_as_call(expr) {
            self.changed = true;
        }
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        // ORDER BY is resolved against the select list as written, so group keys are rewritten
        // after the order keys have been computed from the same items (both read the projection).
        if flatten_body(query.body.as_mut()) {
            self.changed = true;
        }
        let regrouped = body_has_group_ordinal(query.body.as_ref());
        self.rewrite_order_by(query);
        if regrouped && rewrite_group_ordinals(query.body.as_mut(), self.columns) {
            self.changed = true;
        }
        ControlFlow::Continue(())
    }
}

impl Rewriter<'_, '_> {
    fn rewrite_order_by(&mut self, query: &mut Query) {
        let Some(order_by) = query.order_by.as_ref() else {
            return;
        };
        let OrderByKind::Expressions(keys) = &order_by.kind else {
            return;
        };
        if !keys.iter().any(|key| rewritable(query, &key.expr)) {
            return;
        }
        let targets = keys
            .iter()
            .map(|key| match ordinal(&key.expr) {
                Some(position) => self.target(query.body.as_ref(), position),
                None => alias_target(query.body.as_ref(), &key.expr),
            })
            .collect::<Vec<_>>();
        if let Some(order_by) = query.order_by.as_mut() {
            if let OrderByKind::Expressions(keys) = &mut order_by.kind {
                for (key, target) in keys.iter_mut().zip(targets) {
                    if let Some(target) = target {
                        key.expr = target;
                        self.changed = true;
                    }
                }
            }
        }
    }
}

impl Rewriter<'_, '_> {
    /// What output column `position` (1-based) of `body` is, as an ORDER BY expression.
    fn target(&self, body: &SetExpr, position: usize) -> Option<Expr> {
        if position == 0 {
            return None;
        }
        match body {
            // In a plain SELECT the key may be any expression of the select list.
            SetExpr::Select(select) => select_items(select, self.columns)
                .into_iter()
                .nth(position - 1)
                .map(|item| match item {
                    Item::Expr(expr) => expr,
                    Item::Column(name) => Expr::Identifier(Ident::with_quote('"', name)),
                }),
            // Above a set operation (or VALUES / a nested query) only output column names exist.
            _ => output_names(body, self.columns)?
                .into_iter()
                .nth(position - 1)
                .map(|name| Expr::Identifier(Ident::with_quote('"', name))),
        }
    }
}

enum Item {
    Expr(Expr),
    Column(String),
}

fn select_items(select: &Select, columns: &RelationColumns<'_>) -> Vec<Item> {
    let mut items = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => items.push(Item::Expr(expr.clone())),
            SelectItem::ExprWithAlias { expr, .. } | SelectItem::ExprWithAliases { expr, .. } => {
                items.push(Item::Expr(expr.clone()))
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                // `*` expands to the columns of a single FROM relation; with joins the
                // expansion is ambiguous here, so later positions stay unresolved.
                let [from] = select.from.as_slice() else {
                    return items;
                };
                if !from.joins.is_empty() {
                    return items;
                }
                let Some(names) = relation_names(&from.relation, columns) else {
                    return items;
                };
                items.extend(names.into_iter().map(Item::Column));
            }
        }
    }
    items
}

fn relation_names(relation: &TableFactor, columns: &RelationColumns<'_>) -> Option<Vec<String>> {
    match relation {
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let names = output_names(subquery.body.as_ref(), columns)?;
            Some(apply_alias_columns(names, alias.as_ref()))
        }
        TableFactor::Table { alias, .. } => {
            let names = columns(relation)?;
            Some(apply_alias_columns(names, alias.as_ref()))
        }
        _ => None,
    }
}

fn apply_alias_columns(mut names: Vec<String>, alias: Option<&TableAlias>) -> Vec<String> {
    if let Some(alias) = alias {
        for (name, column) in names.iter_mut().zip(&alias.columns) {
            *name = folded(&column.name);
        }
    }
    names
}

/// Output column names of a query body (from its first SELECT for set operations).
fn output_names(body: &SetExpr, columns: &RelationColumns<'_>) -> Option<Vec<String>> {
    match body {
        SetExpr::Select(select) => {
            let mut names = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::ExprWithAlias { alias, .. } => names.push(folded(alias)),
                    SelectItem::ExprWithAliases { aliases, .. } => {
                        names.extend(aliases.iter().map(folded))
                    }
                    SelectItem::UnnamedExpr(expr) => names.push(match expr {
                        Expr::Identifier(ident) => folded(ident),
                        Expr::CompoundIdentifier(parts) => folded(parts.last()?),
                        // PostgreSQL's names for other expressions (`?column?`, function names)
                        // are not safe to refer back to; leave such positions unresolved.
                        _ => return Some(names).filter(|names| !names.is_empty()),
                    }),
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                        let [from] = select.from.as_slice() else {
                            return Some(names).filter(|names| !names.is_empty());
                        };
                        if !from.joins.is_empty() {
                            return Some(names).filter(|names| !names.is_empty());
                        }
                        names.extend(relation_names(&from.relation, columns)?);
                    }
                }
            }
            Some(names)
        }
        SetExpr::SetOperation { left, .. } => output_names(left, columns),
        SetExpr::Query(query) => output_names(query.body.as_ref(), columns),
        SetExpr::Values(values) => Some(
            (1..=values
                .rows
                .first()
                .map(|row| row.content.len())
                .unwrap_or(0))
                .map(|n| format!("column{n}"))
                .collect(),
        ),
        _ => None,
    }
}

/// INSERT/UPDATE/DELETE with `make_interval(mins => n)` or plain `ceil(x)`/`floor(x)`: the same call
/// normalization queries get, or the named arguments of make_interval were dropped (a zero interval).
pub(crate) fn rewrite_dml_call_forms(statement: &Statement) -> Option<Statement> {
    struct CallFinder(bool);
    impl Visitor for CallFinder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if named_make_interval(expr) || plain_ceil_floor(expr) {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    struct CallRewriter;
    impl VisitorMut for CallRewriter {
        type Break = ();
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            let _ = positional_make_interval(expr) || ceil_floor_as_call(expr);
            ControlFlow::Continue(())
        }
    }
    let mut finder = CallFinder(false);
    let _ = Visit::visit(statement, &mut finder);
    if !finder.0 {
        return None;
    }
    let mut rewritten = statement.clone();
    let _ = VisitMut::visit(&mut rewritten, &mut CallRewriter);
    Some(rewritten)
}
