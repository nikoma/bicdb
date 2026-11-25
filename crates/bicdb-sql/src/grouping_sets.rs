//! `GROUP BY GROUPING SETS (...)`, `ROLLUP (...)` and `CUBE (...)`, alone or
//! mixed with plain grouping columns, as PostgreSQL evaluates them: one
//! grouped result per grouping set, concatenated. Each set becomes one
//! ordinary GROUP BY branch of a `UNION ALL`; in a branch, a grouping
//! expression outside the set reads as NULL (outside aggregate arguments)
//! and `GROUPING(...)` is the set's bit mask. HAVING applies per set, and
//! DISTINCT, ORDER BY, LIMIT and OFFSET apply to the combined rows.
use crate::*;
use sqlparser::ast::{
    Distinct, GroupByExpr, OrderByKind, SetOperator, SetQuantifier, Value, VisitMut, VisitorMut,
};

/// PostgreSQL's limit on the expanded grouping sets of one query level.
const MAX_GROUPING_SETS: usize = 4096;
const KEPT_PREFIX: &str = "\u{0}grouping_kept_";
const OUTPUT_PREFIX: &str = "bicdb_grouping_set_column_";
const ORDER_PREFIX: &str = "bicdb_grouping_set_order_";
const DERIVED_ALIAS: &str = "bicdb grouping sets";

fn is_grouping_construct(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::GroupingSets(_) | Expr::Rollup(_) | Expr::Cube(_)
    ) || matches!(expr, Expr::Tuple(items) if items.is_empty())
}

fn nested_construct_items(expr: &Expr) -> Option<(&'static str, Vec<Vec<Expr>>)> {
    let Expr::Function(function) = expr else {
        return None;
    };
    let name = object_name(&function.name).ok()?.to_ascii_lowercase();
    if name != "rollup" && name != "cube" {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let mut items = Vec::new();
    for arg in &list.args {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return None;
        };
        items.push(match expr {
            Expr::Tuple(exprs) => exprs.clone(),
            Expr::Nested(inner) => vec![(**inner).clone()],
            other => vec![other.clone()],
        });
    }
    Some((if name == "rollup" { "rollup" } else { "cube" }, items))
}

fn check_set_count(count: usize) -> Result<()> {
    if count > MAX_GROUPING_SETS {
        return Err(SqlError::data_exception(
            "54001",
            format!("too many grouping sets present (maximum {MAX_GROUPING_SETS})"),
            None,
        ));
    }
    Ok(())
}

fn rollup_sets(items: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    (0..=items.len())
        .rev()
        .map(|len| items[..len].concat())
        .collect()
}

fn cube_sets(items: &[Vec<Expr>]) -> Result<Vec<Vec<Expr>>> {
    check_set_count(1usize.checked_shl(items.len() as u32).unwrap_or(usize::MAX))?;
    let full = (1usize << items.len()) - 1;
    // Larger sets first, as PostgreSQL lists them.
    let mut masks = (0..=full).collect::<Vec<_>>();
    masks.sort_by_key(|mask| std::cmp::Reverse(mask.count_ones()));
    Ok(masks
        .into_iter()
        .map(|mask| {
            items
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << (items.len() - 1 - index)) != 0)
                .flat_map(|(_, item)| item.iter().cloned())
                .collect()
        })
        .collect())
}

/// The grouping sets one GROUP BY element stands for.
fn element_sets(expr: &Expr) -> Result<Vec<Vec<Expr>>> {
    match expr {
        Expr::Rollup(items) => Ok(rollup_sets(items)),
        Expr::Cube(items) => cube_sets(items),
        Expr::GroupingSets(items) => {
            let mut sets = Vec::new();
            for item in items {
                match item.as_slice() {
                    [single] if is_grouping_construct(single) => sets.extend(element_sets(single)?),
                    [single] if nested_construct_items(single).is_some() => {
                        let (kind, nested) = nested_construct_items(single).unwrap();
                        sets.extend(if kind == "rollup" {
                            rollup_sets(&nested)
                        } else {
                            cube_sets(&nested)?
                        });
                    }
                    plain => sets.push(plain.to_vec()),
                }
                check_set_count(sets.len())?;
            }
            Ok(sets)
        }
        Expr::Tuple(items) if items.is_empty() => Ok(vec![Vec::new()]),
        other => Ok(vec![vec![other.clone()]]),
    }
}

fn strip_nested(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => strip_nested(inner),
        other => other,
    }
}

/// Whether two expressions name the same grouping expression: equal ASTs,
/// or the same column with or without its relation qualifier.
fn same_grouping_expr(left: &Expr, right: &Expr) -> bool {
    let (left, right) = (strip_nested(left), strip_nested(right));
    if left == right {
        return true;
    }
    fn column(expr: &Expr) -> Option<(Option<String>, String)> {
        match expr {
            Expr::Identifier(ident) => Some((None, ident_value(ident))),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some((
                Some(ident_value(&parts[parts.len() - 2])),
                ident_value(&parts[parts.len() - 1]),
            )),
            _ => None,
        }
    }
    match (column(left), column(right)) {
        (Some((left_relation, left_name)), Some((right_relation, right_name))) => {
            left_name == right_name
                && (left_relation.is_none()
                    || right_relation.is_none()
                    || left_relation == right_relation)
        }
        _ => false,
    }
}

fn contains_expr(exprs: &[Expr], expr: &Expr) -> bool {
    exprs
        .iter()
        .any(|candidate| same_grouping_expr(candidate, expr))
}

fn is_grouping_call(expr: &Expr) -> bool {
    matches!(expr, Expr::Function(function)
        if function.over.is_none()
            && object_name(&function.name)
                .is_ok_and(|name| name.eq_ignore_ascii_case("grouping")))
}

fn contains_window_function(expr: &Expr) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(expr, |expr| {
        if matches!(expr, Expr::Function(function) if function.over.is_some()) {
            found = true;
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

/// Rewrites one expression for one grouping set. Kept subtrees (grouping
/// expressions of the set, aggregate calls, subqueries) are swapped for
/// placeholder leaves during the walk so it does not descend into them, and
/// restored afterwards.
struct GroupingSubstitution<'a> {
    in_set: &'a [Expr],
    all: &'a [Expr],
    kept: Vec<Expr>,
    error: Option<SqlError>,
}

impl GroupingSubstitution<'_> {
    fn keep(&mut self, expr: &mut Expr) {
        let placeholder = Expr::Identifier(Ident::new(format!("{KEPT_PREFIX}{}", self.kept.len())));
        self.kept.push(std::mem::replace(expr, placeholder));
    }
}

impl VisitorMut for GroupingSubstitution<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<()> {
        if contains_expr(self.in_set, expr) {
            self.keep(expr);
        } else if contains_expr(self.all, expr) {
            *expr = Expr::value(Value::Null);
        } else if is_grouping_call(expr) {
            let Expr::Function(function) = &*expr else {
                unreachable!()
            };
            let args = match &function.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>(),
                _ => None,
            };
            let Some(args) = args.filter(|args| !args.is_empty() && args.len() <= 31) else {
                self.error = Some(SqlError::InvalidSql(
                    "GROUPING must have between 1 and 31 arguments".into(),
                ));
                return std::ops::ControlFlow::Break(());
            };
            let mut mask = 0i64;
            for arg in &args {
                if !contains_expr(self.all, arg) {
                    self.error = Some(SqlError::data_exception(
                        "42803",
                        "arguments to GROUPING must be grouping expressions of the associated query level",
                        None,
                    ));
                    return std::ops::ControlFlow::Break(());
                }
                mask = (mask << 1) | i64::from(!contains_expr(self.in_set, arg));
            }
            *expr = Expr::value(Value::Number(mask.to_string(), false));
        } else if match &*expr {
            Expr::Function(function) => function.over.is_some() || is_aggregate_function(function),
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
            _ => false,
        } {
            self.keep(expr);
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn substitute(expr: &Expr, in_set: &[Expr], all: &[Expr]) -> Result<Expr> {
    let mut expr = expr.clone();
    let mut substitution = GroupingSubstitution {
        in_set,
        all,
        kept: Vec::new(),
        error: None,
    };
    let _ = expr.visit(&mut substitution);
    if let Some(error) = substitution.error {
        return Err(error);
    }
    let mut kept = substitution.kept.into_iter().map(Some).collect::<Vec<_>>();
    let _ = visit_expressions_mut(&mut expr, |expr| {
        if let Expr::Identifier(ident) = expr {
            if let Some(index) = ident
                .value
                .strip_prefix(KEPT_PREFIX)
                .and_then(|index| index.parse::<usize>().ok())
            {
                if let Some(original) = kept.get_mut(index).and_then(Option::take) {
                    *expr = original;
                }
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    Ok(expr)
}

/// The input column names of `select`'s FROM list, or `None` when some
/// relation's columns are unknown (aliases are then never substituted).
fn input_column_names(
    select: &Select,
    input_columns: &dyn Fn(&TableFactor) -> Option<Vec<String>>,
) -> Option<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for from in &select.from {
        for relation in
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        {
            let columns = match relation {
                TableFactor::Derived {
                    alias: Some(alias), ..
                } if !alias.columns.is_empty() => alias
                    .columns
                    .iter()
                    .map(|column| ident_value(&column.name))
                    .collect(),
                _ => input_columns(relation)?,
            };
            names.extend(columns.into_iter().map(|name| name.to_ascii_lowercase()));
        }
    }
    Some(names)
}

fn output_column_expr(
    expr: &Expr,
    projection: &[SelectItem],
    input_names: Option<&BTreeSet<String>>,
) -> Option<Expr> {
    let item_expr = |item: &SelectItem| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            Some(expr.clone())
        }
        _ => None,
    };
    match strip_nested(expr) {
        Expr::Value(value) => {
            let Value::Number(number, _) = &value.value else {
                return None;
            };
            let position = number.parse::<usize>().ok()?;
            item_expr(projection.get(position.checked_sub(1)?)?)
        }
        Expr::Identifier(ident) => {
            let name = ident_value(ident);
            if input_names?.contains(&name.to_ascii_lowercase()) {
                return None;
            }
            projection.iter().find_map(|item| match item {
                SelectItem::ExprWithAlias { expr, alias } if ident_value(alias) == name => {
                    Some(expr.clone())
                }
                _ => None,
            })
        }
        _ => None,
    }
}

fn quoted(name: &str) -> String {
    Ident::with_quote('"', name).to_string()
}

/// Whether `query`'s GROUP BY has a grouping-sets element (the cheap test
/// that guards `rewrite_grouping_sets`).
pub(crate) fn has_grouping_sets(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    matches!(&select.group_by, GroupByExpr::Expressions(elements, modifiers)
        if modifiers.is_empty() && elements.iter().any(is_grouping_construct))
}

/// `query` with its grouping sets expanded, or `None` when its GROUP BY has
/// none. Forms whose combined result PostgreSQL computes differently from
/// a union of grouped branches (window functions, `*`) fail with 0A000.
pub(crate) fn rewrite_grouping_sets(
    query: &Query,
    input_columns: &dyn Fn(&TableFactor) -> Option<Vec<String>>,
) -> Result<Option<Query>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let GroupByExpr::Expressions(elements, modifiers) = &select.group_by else {
        return Ok(None);
    };
    if !modifiers.is_empty() || !elements.iter().any(is_grouping_construct) {
        return Ok(None);
    }
    let mut sets: Vec<Vec<Expr>> = vec![Vec::new()];
    for element in elements {
        let element_sets = element_sets(element)?;
        check_set_count(sets.len().saturating_mul(element_sets.len()))?;
        sets = sets
            .iter()
            .flat_map(|left| {
                element_sets
                    .iter()
                    .map(move |right| [left.as_slice(), right.as_slice()].concat())
            })
            .collect();
    }
    // A grouping element naming an output column (by position, or by alias
    // when no input column has that name) groups by that column's
    // expression, as PostgreSQL resolves GROUP BY.
    let input_names = input_column_names(select, input_columns);
    for set in &mut sets {
        for expr in set.iter_mut() {
            if let Some(resolved) =
                output_column_expr(expr, &select.projection, input_names.as_ref())
            {
                *expr = resolved;
            }
        }
    }
    let mut all = Vec::new();
    for expr in sets.iter().flatten() {
        if !contains_expr(&all, expr) {
            all.push(expr.clone());
        }
    }
    let mut items = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => {
                return Err(SqlError::Unsupported(
                    "GROUPING SETS with * in the select list is not supported".into(),
                ))
            }
        };
        let name = alias.unwrap_or_else(|| row_expr_column_name(expr));
        items.push((expr, name));
    }
    let mut order_by = query.order_by.clone();
    let mut order_columns: Vec<Expr> = Vec::new();
    if let Some(order) = order_by.as_mut() {
        let OrderByKind::Expressions(order_exprs) = &mut order.kind else {
            return Err(SqlError::Unsupported(
                "ORDER BY ALL with GROUPING SETS is not supported".into(),
            ));
        };
        for order_expr in order_exprs.iter_mut() {
            let ordinal = match strip_nested(&order_expr.expr) {
                Expr::Value(value) => match &value.value {
                    Value::Number(number, _) => number
                        .parse::<usize>()
                        .ok()
                        .filter(|position| (1..=items.len()).contains(position))
                        .map(|position| position - 1),
                    _ => None,
                },
                _ => None,
            };
            // A position already resolved upstream to a constant item reads `+n`.
            let resolved_constant = match strip_nested(&order_expr.expr) {
                Expr::UnaryOp {
                    op: sqlparser::ast::UnaryOperator::Plus,
                    expr,
                } if matches!(strip_nested(expr), Expr::Value(_)) => Some(strip_nested(expr)),
                _ => None,
            };
            let position = ordinal.or_else(|| {
                items.iter().position(|(expr, name)| {
                    resolved_constant.is_some_and(|constant| strip_nested(expr) == constant)
                        || same_grouping_expr(expr, &order_expr.expr)
                        || matches!(strip_nested(&order_expr.expr), Expr::Identifier(ident)
                        if ident.value == *name || ident_value(ident) == *name)
                })
            });
            let column = match position {
                Some(position) => format!("{OUTPUT_PREFIX}{position}"),
                None => {
                    order_columns.push(order_expr.expr.clone());
                    format!("{ORDER_PREFIX}{}", order_columns.len() - 1)
                }
            };
            order_expr.expr = Expr::Identifier(Ident::with_quote('"', column));
        }
    }
    if select.distinct.is_some() && !order_columns.is_empty() {
        return Err(SqlError::data_exception(
            "42P10",
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
            None,
        ));
    }
    if matches!(select.distinct, Some(Distinct::On(_))) {
        return Err(SqlError::Unsupported(
            "DISTINCT ON with GROUPING SETS is not supported".into(),
        ));
    }
    if items.iter().any(|(expr, _)| contains_window_function(expr))
        || order_columns.iter().any(contains_window_function)
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return Err(SqlError::Unsupported(
            "window functions over GROUPING SETS are not supported".into(),
        ));
    }

    let mut branches = Vec::with_capacity(sets.len());
    for set in &sets {
        let mut branch = select.as_ref().clone();
        branch.distinct = None;
        if branch.from.is_empty() {
            // A FROM-less query has one input row; give the branch that row
            // explicitly so it is an ordinary grouped query.
            let Some(Statement::Query(one_row)) = parse_statements(
                "SELECT 1 FROM (VALUES (1)) AS \"bicdb grouping sets row\"(bicdb_grouping_set_row)",
            )?
            .pop() else {
                return Err(SqlError::data_exception(
                    "XX000",
                    "grouping sets adapter",
                    None,
                ));
            };
            if let SetExpr::Select(one_row) = *one_row.body {
                branch.from = one_row.from;
            }
        }
        let mut group_by = Vec::new();
        for expr in set {
            if !contains_expr(&group_by, expr) {
                group_by.push(expr.clone());
            }
        }
        let mut projection = Vec::with_capacity(items.len() + order_columns.len() + 1);
        for (index, (expr, _)) in items.iter().enumerate() {
            projection.push(SelectItem::ExprWithAlias {
                expr: substitute(expr, &group_by, &all)?,
                alias: Ident::with_quote('"', format!("{OUTPUT_PREFIX}{index}")),
            });
        }
        for (index, expr) in order_columns.iter().enumerate() {
            projection.push(SelectItem::ExprWithAlias {
                expr: substitute(expr, &group_by, &all)?,
                alias: Ident::with_quote('"', format!("{ORDER_PREFIX}{index}")),
            });
        }
        // Grouping by the empty set yields its one row even with no
        // aggregate in the select list; an aggregate keeps every branch an
        // aggregation.
        projection.push(SelectItem::ExprWithAlias {
            expr: parse_grouping_expr("count(*)")?,
            alias: Ident::with_quote('"', "bicdb_grouping_set_rows"),
        });
        branch.projection = projection;
        branch.having = branch
            .having
            .as_ref()
            .map(|having| substitute(having, &group_by, &all))
            .transpose()?;
        // A constant key groups every input row into one group: grouping by
        // nothing, except that empty input yields no group. Dropping it keeps
        // numeric constants from reading as GROUP BY positions.
        let only_constants = !group_by.is_empty()
            && group_by
                .iter()
                .all(|expr| matches!(strip_nested(expr), Expr::Value(_)));
        let keys: Vec<Expr> = group_by
            .into_iter()
            .filter(|expr| !matches!(strip_nested(expr), Expr::Value(_)))
            .collect();
        if only_constants {
            let nonempty = parse_grouping_expr("count(*) > 0")?;
            branch.having = Some(match branch.having.take() {
                Some(having) => Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(having))),
                    op: sqlparser::ast::BinaryOperator::And,
                    right: Box::new(nonempty),
                },
                None => nonempty,
            });
        }
        branch.group_by = GroupByExpr::Expressions(keys, Vec::new());
        branches.push(SetExpr::Select(Box::new(branch)));
    }
    let mut branches = branches.into_iter();
    let mut union = branches.next().expect("at least one grouping set");
    for branch in branches {
        union = SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left: Box::new(union),
            right: Box::new(branch),
        };
    }

    let outer_sql = format!(
        "SELECT {} FROM (SELECT 1) AS {}",
        items
            .iter()
            .enumerate()
            .map(|(index, (_, name))| format!(
                "{} AS {}",
                quoted(&format!("{OUTPUT_PREFIX}{index}")),
                quoted(name)
            ))
            .collect::<Vec<_>>()
            .join(", "),
        quoted(DERIVED_ALIAS)
    );
    let Some(Statement::Query(outer)) = parse_statements(&outer_sql)?.pop() else {
        return Err(SqlError::data_exception(
            "XX000",
            "grouping sets adapter",
            None,
        ));
    };
    let mut outer_body = match *outer.body {
        SetExpr::Select(select) => select,
        _ => {
            return Err(SqlError::data_exception(
                "XX000",
                "grouping sets adapter",
                None,
            ))
        }
    };
    let Some(TableFactor::Derived { subquery, .. }) =
        outer_body.from.first_mut().map(|from| &mut from.relation)
    else {
        return Err(SqlError::data_exception(
            "XX000",
            "grouping sets adapter",
            None,
        ));
    };
    subquery.body = Box::new(union);
    outer_body.distinct = select.distinct.clone();
    let mut rewritten = query.clone();
    rewritten.body = Box::new(SetExpr::Select(outer_body));
    rewritten.order_by = order_by;
    Ok(Some(rewritten))
}

fn parse_grouping_expr(sql: &str) -> Result<Expr> {
    let Some(Statement::Query(query)) = parse_statements(&format!("SELECT {sql}"))?.pop() else {
        return Err(SqlError::data_exception(
            "XX000",
            "grouping sets adapter",
            None,
        ));
    };
    match *query.body {
        SetExpr::Select(select) => match select.projection.into_iter().next() {
            Some(SelectItem::UnnamedExpr(expr)) => Ok(expr),
            _ => Err(SqlError::data_exception(
                "XX000",
                "grouping sets adapter",
                None,
            )),
        },
        _ => Err(SqlError::data_exception(
            "XX000",
            "grouping sets adapter",
            None,
        )),
    }
}
