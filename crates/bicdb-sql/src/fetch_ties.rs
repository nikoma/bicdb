//! `FETCH FIRST n ROWS WITH TIES`: the first `offset + n` rows in ORDER BY
//! order plus every row tied with the last of them, then OFFSET. Those are
//! exactly the rows whose `rank()` over the ORDER BY keys is at most
//! `offset + n`, so the query is evaluated with the sort keys and that rank
//! as trailing hidden columns, filtered on the rank, and the hidden columns
//! are dropped from the result.
use crate::*;
use sqlparser::ast::{BinaryOperator, OrderByExpr, OrderByKind, Value, WindowType};

const KEY_PREFIX: &str = "bicdb_ties_key_";
const RANK_COLUMN: &str = "bicdb_ties_rank";
const DERIVED_ALIAS: &str = "bicdb ties";

fn adapter_error() -> SqlError {
    SqlError::data_exception("XX000", "WITH TIES adapter", None)
}

fn parse_query(sql: &str) -> Result<Query> {
    match parse_statements(sql)?.pop() {
        Some(Statement::Query(query)) => Ok(*query),
        _ => Err(adapter_error()),
    }
}

/// The rewritten query and the number of hidden trailing columns, or `None`
/// when the query has no `WITH TIES`.
pub(crate) fn rewrite_with_ties(query: &Query) -> Result<Option<(Query, usize)>> {
    let Some(fetch) = query.fetch.as_ref().filter(|fetch| fetch.with_ties) else {
        return Ok(None);
    };
    if fetch.percent {
        return Err(SqlError::Unsupported(
            "FETCH PERCENT is not implemented".into(),
        ));
    }
    let order = match query.order_by.as_ref().map(|order| &order.kind) {
        Some(OrderByKind::Expressions(exprs)) if !exprs.is_empty() => exprs.clone(),
        _ => {
            return Err(SqlError::data_exception(
                "42601",
                "WITH TIES cannot be specified without ORDER BY clause",
                None,
            ))
        }
    };
    let offset = match &query.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit: None,
            offset,
            limit_by,
        }) if limit_by.is_empty() => offset.as_ref().map(|offset| offset.value.clone()),
        Some(_) => {
            return Err(SqlError::InvalidSql(
                "FETCH cannot be combined with LIMIT".into(),
            ))
        }
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SqlError::Unsupported(
            "WITH TIES over a set operation is not supported".into(),
        ));
    };
    if select.distinct.is_some() {
        return Err(SqlError::Unsupported(
            "WITH TIES with DISTINCT is not supported".into(),
        ));
    }
    // ORDER BY may name an output column by alias or position; the window
    // and hidden key columns need the underlying expression.
    let mut keys = Vec::with_capacity(order.len());
    for item in &order {
        let mut expr = item.expr.clone();
        if let Expr::Value(value) = &item.expr {
            if let Value::Number(number, _) = &value.value {
                let position = number
                    .parse::<usize>()
                    .ok()
                    .filter(|position| *position >= 1 && *position <= select.projection.len())
                    .ok_or_else(|| {
                        SqlError::data_exception(
                            "42P10",
                            format!("ORDER BY position {number} is not in select list"),
                            None,
                        )
                    })?;
                expr = match &select.projection[position - 1] {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        expr.clone()
                    }
                    _ => {
                        return Err(SqlError::Unsupported(
                            "WITH TIES ordered by a * position is not supported".into(),
                        ))
                    }
                };
            }
        } else if let Expr::Identifier(ident) = &item.expr {
            if let Some(aliased) = select
                .projection
                .iter()
                .find_map(|projected| match projected {
                    SelectItem::ExprWithAlias { expr, alias }
                        if ident_value(alias) == ident_value(ident) =>
                    {
                        Some(expr.clone())
                    }
                    _ => None,
                })
            {
                expr = aliased;
            }
        }
        keys.push(OrderByExpr {
            expr,
            options: item.options.clone(),
            with_fill: None,
        });
    }

    let mut inner = select.as_ref().clone();
    for (index, key) in keys.iter().enumerate() {
        inner.projection.push(SelectItem::ExprWithAlias {
            expr: key.expr.clone(),
            alias: Ident::with_quote('"', format!("{KEY_PREFIX}{index}")),
        });
    }
    let mut rank = match parse_query("SELECT rank() OVER ()")?.body.as_ref() {
        SetExpr::Select(select) => match select.projection.first() {
            Some(SelectItem::UnnamedExpr(expr)) => expr.clone(),
            _ => return Err(adapter_error()),
        },
        _ => return Err(adapter_error()),
    };
    if let Expr::Function(function) = &mut rank {
        if let Some(WindowType::WindowSpec(spec)) = function.over.as_mut() {
            spec.order_by = keys.clone();
        }
    }
    inner.projection.push(SelectItem::ExprWithAlias {
        expr: rank,
        alias: Ident::with_quote('"', RANK_COLUMN),
    });
    let mut inner_query = query.clone();
    inner_query.body = Box::new(SetExpr::Select(Box::new(inner)));
    inner_query.order_by = None;
    inner_query.limit_clause = None;
    inner_query.fetch = None;

    let mut outer = parse_query(&format!("SELECT * FROM (SELECT 1) AS \"{DERIVED_ALIAS}\""))?;
    let SetExpr::Select(outer_select) = outer.body.as_mut() else {
        return Err(adapter_error());
    };
    let Some(TableFactor::Derived { subquery, .. }) =
        outer_select.from.first_mut().map(|from| &mut from.relation)
    else {
        return Err(adapter_error());
    };
    *subquery = Box::new(inner_query);
    let count = fetch
        .quantity
        .clone()
        .unwrap_or_else(|| Expr::value(Value::Number("1".into(), false)));
    let bound = match offset.clone() {
        Some(offset) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(offset))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Nested(Box::new(count))),
        },
        None => count,
    };
    outer_select.selection = Some(Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::with_quote('"', RANK_COLUMN))),
        op: BinaryOperator::LtEq,
        right: Box::new(bound),
    });
    let mut order_by = query.order_by.clone().ok_or_else(adapter_error)?;
    order_by.kind = OrderByKind::Expressions(
        keys.iter()
            .enumerate()
            .map(|(index, key)| OrderByExpr {
                expr: Expr::Identifier(Ident::with_quote('"', format!("{KEY_PREFIX}{index}"))),
                options: key.options.clone(),
                with_fill: None,
            })
            .collect(),
    );
    outer.order_by = Some(order_by);
    outer.limit_clause = offset.map(|_| query.limit_clause.clone()).flatten();
    outer.with = None;
    Ok(Some((outer, keys.len() + 1)))
}

/// Drops the hidden trailing columns a rewrite added.
pub(crate) fn drop_trailing_columns(result: &mut SqlResult, hidden: usize) {
    let keep = result.columns.len().saturating_sub(hidden);
    result.columns.truncate(keep);
    for row in &mut result.rows {
        row.truncate(keep);
    }
    result.column_types.truncate(keep);
    result.column_metadata.truncate(keep);
}
