//! SQL-visible columns of a join with USING or NATURAL.
use super::*;
use sqlparser::ast::{VisitMut, VisitorMut};
use std::ops::ControlFlow;

#[derive(Clone, Copy)]
enum MergedSide {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Clone)]
pub(super) struct NamedJoinColumn {
    name: String,
    expression: Expr,
    data_type: Option<String>,
    merged: bool,
}

impl SqlEngine<'_> {
    pub(super) fn rewrite_named_join_select(
        &self,
        select: &Select,
    ) -> Result<Option<(Select, Vec<NamedJoinColumn>)>> {
        let [from] = select.from.as_slice() else {
            return Ok(None);
        };
        if !from.joins.iter().any(|join| {
            matches!(
                join_constraint(&join.join_operator),
                Some((JoinConstraint::Using(_) | JoinConstraint::Natural, _))
            )
        }) || oracle::is_oracle_database(self.db_ref())?
        {
            return Ok(None);
        }

        let Some(mut columns) = self.named_join_input(&from.relation) else {
            return Ok(None);
        };
        let mut output = select.clone();
        for join in &mut output.from[0].joins {
            let Some(right) = self.named_join_input(&join.relation) else {
                return Ok(None);
            };
            if let Some((constraint, side)) = join_constraint(&join.join_operator) {
                if matches!(
                    constraint,
                    JoinConstraint::Using(_) | JoinConstraint::Natural
                ) {
                    let (operator, joined) =
                        self.bind_named_join(columns, right, constraint, side)?;
                    join.join_operator = operator;
                    columns = joined;
                    continue;
                }
            }
            columns.extend(right);
            let merged = unique_merged_columns(&columns);
            let _ = join
                .join_operator
                .visit(&mut ReplaceMergedColumns::new(&merged));
        }
        let merged = unique_merged_columns(&columns);
        output.projection.clear();
        for mut item in select.projection.clone() {
            match item {
                SelectItem::Wildcard(ref options)
                    if options == &sqlparser::ast::WildcardAdditionalOptions::default() =>
                {
                    output.projection.extend(columns.iter().map(|column| {
                        if column.merged {
                            SelectItem::ExprWithAlias {
                                expr: column.expression.clone(),
                                alias: Ident::with_quote('"', &column.name),
                            }
                        } else {
                            SelectItem::UnnamedExpr(column.expression.clone())
                        }
                    }));
                    continue;
                }
                SelectItem::Wildcard(_) => return Ok(None),
                SelectItem::UnnamedExpr(ref mut expr) => {
                    if let Expr::Identifier(ident) = expr {
                        if let Some(column) = merged
                            .iter()
                            .find(|column| column.name == ident_value(ident))
                        {
                            item = SelectItem::ExprWithAlias {
                                expr: column.expression.clone(),
                                alias: Ident::with_quote('"', &column.name),
                            };
                        }
                    }
                }
                _ => {}
            }
            let _ = item.visit(&mut ReplaceMergedColumns::new(&merged));
            output.projection.push(item);
        }
        let mut rewrite = ReplaceMergedColumns::new(&merged);
        let _ = output.selection.visit(&mut rewrite);
        let _ = output.group_by.visit(&mut rewrite);
        let _ = output.having.visit(&mut rewrite);
        Ok(Some((output, merged)))
    }

    fn bind_named_join(
        &self,
        left: Vec<NamedJoinColumn>,
        right: Vec<NamedJoinColumn>,
        constraint: &JoinConstraint,
        side: MergedSide,
    ) -> Result<(JoinOperator, Vec<NamedJoinColumn>)> {
        let names = match constraint {
            JoinConstraint::Using(names) => {
                names.iter().map(object_name).collect::<Result<Vec<_>>>()?
            }
            JoinConstraint::Natural => {
                let mut names = Vec::new();
                for column in &left {
                    if right.iter().any(|candidate| candidate.name == column.name)
                        && !names.contains(&column.name)
                    {
                        names.push(column.name.clone());
                    }
                }
                names
            }
            _ => unreachable!(),
        };
        let mut merged = Vec::with_capacity(names.len());
        let mut equality = None;
        for name in &names {
            if merged
                .iter()
                .any(|column: &NamedJoinColumn| column.name == *name)
            {
                return Err(SqlError::data_exception_public(
                    "42701",
                    format!("column name \"{name}\" appears more than once in USING clause"),
                    None,
                ));
            }
            let left_column = using_input_column(&left, name, "left")?;
            let right_column = using_input_column(&right, name, "right")?;
            let left_type = &left_column.data_type;
            let right_type = &right_column.data_type;
            let common = select_common_pg_type(
                self.db_ref(),
                &[left_type.clone(), right_type.clone()],
                "JOIN/USING",
            )?;
            let left_column = left_column.expression.clone();
            let right_column = right_column.expression.clone();
            let predicate = Expr::BinaryOp {
                left: Box::new(left_column.clone()),
                op: BinaryOperator::Eq,
                right: Box::new(right_column.clone()),
            };
            equality = Some(match equality {
                None => predicate,
                Some(previous) => Expr::BinaryOp {
                    left: Box::new(previous),
                    op: BinaryOperator::And,
                    right: Box::new(predicate),
                },
            });
            let from_left = || cast_join_column(left_column, left_type.as_deref(), &common);
            let from_right = || cast_join_column(right_column, right_type.as_deref(), &common);
            let value = match side {
                MergedSide::Right => from_right()?,
                MergedSide::Inner
                    if left_type.as_deref() != Some(common.as_str())
                        && right_type.as_deref() == Some(common.as_str()) =>
                {
                    from_right()?
                }
                MergedSide::Inner | MergedSide::Left => from_left()?,
                MergedSide::Full => {
                    let args = vec![from_left()?, from_right()?]
                        .into_iter()
                        .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                        .collect();
                    Expr::Function(Function {
                        name: ObjectName::from(vec![Ident::new("coalesce")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                            duplicate_treatment: None,
                            args,
                            clauses: Vec::new(),
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Vec::new(),
                    })
                }
            };
            merged.push(NamedJoinColumn {
                name: name.clone(),
                expression: value,
                data_type: Some(common),
                merged: true,
            });
        }

        let constraint = JoinConstraint::On(
            equality.unwrap_or_else(|| Expr::value(sqlparser::ast::Value::Boolean(true))),
        );
        let operator = match side {
            MergedSide::Inner => JoinOperator::Inner(constraint),
            MergedSide::Left => JoinOperator::Left(constraint),
            MergedSide::Right => JoinOperator::Right(constraint),
            MergedSide::Full => JoinOperator::FullOuter(constraint),
        };
        merged.extend(
            left.into_iter()
                .chain(right)
                .filter(|column| !names.contains(&column.name)),
        );
        Ok((operator, merged))
    }

    fn named_join_input(&self, factor: &TableFactor) -> Option<Vec<NamedJoinColumn>> {
        if !matches!(
            factor,
            TableFactor::Table { args: None, .. } | TableFactor::Derived { .. }
        ) {
            return None;
        }
        let mut env = Vec::new();
        self.collect_relation_columns(factor, &mut env)?;
        let [relation] = <[RelationColumns; 1]>::try_from(env).ok()?;
        if relation.alias.is_empty()
            || relation
                .columns
                .iter()
                .any(|(name, _)| name == column_check::UNKNOWN_CTE_COLUMN)
        {
            return None;
        }
        Some(
            relation
                .columns
                .into_iter()
                .map(|(name, data_type)| NamedJoinColumn {
                    expression: qualified_column(&relation.alias, &name),
                    name,
                    data_type,
                    merged: false,
                })
                .collect(),
        )
    }

    pub(super) fn rewrite_named_join_query(&self, query: &Query) -> Result<Option<Box<Query>>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        let Some((select, merged)) = self.rewrite_named_join_select(select)? else {
            return Ok(None);
        };
        let output_names = select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(ident_value(alias)),
                SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut query = Box::new(query.clone());
        query.body = Box::new(SetExpr::Select(Box::new(select)));
        if let Some(OrderBy {
            kind: OrderByKind::Expressions(order),
            ..
        }) = &mut query.order_by
        {
            for order in order {
                if !matches!(&order.expr, Expr::Identifier(ident) if output_names.contains(&ident_value(ident)))
                {
                    let _ = order.expr.visit(&mut ReplaceMergedColumns::new(&merged));
                }
            }
        }
        Ok(Some(query))
    }
}

fn join_constraint(operator: &JoinOperator) -> Option<(&JoinConstraint, MergedSide)> {
    match operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            Some((constraint, MergedSide::Inner))
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            Some((constraint, MergedSide::Left))
        }
        JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
            Some((constraint, MergedSide::Right))
        }
        JoinOperator::FullOuter(constraint) => Some((constraint, MergedSide::Full)),
        _ => None,
    }
}

fn unique_merged_columns(columns: &[NamedJoinColumn]) -> Vec<NamedJoinColumn> {
    columns
        .iter()
        .filter(|column| {
            column.merged
                && columns
                    .iter()
                    .filter(|candidate| candidate.name == column.name)
                    .take(2)
                    .count()
                    == 1
        })
        .cloned()
        .collect()
}

fn using_input_column<'a>(
    input: &'a [NamedJoinColumn],
    name: &str,
    side: &str,
) -> Result<&'a NamedJoinColumn> {
    let mut matches = input.iter().filter(|column| column.name == name);
    let Some(column) = matches.next() else {
        return Err(SqlError::data_exception_public(
            "42703",
            format!("column \"{name}\" specified in USING clause does not exist in {side} table"),
            None,
        ));
    };
    if matches.next().is_some() {
        return Err(SqlError::data_exception_public(
            "42702",
            format!("common column name \"{name}\" appears more than once in {side} table"),
            None,
        ));
    }
    Ok(column)
}

fn qualified_column(alias: &str, name: &str) -> Expr {
    Expr::CompoundIdentifier(vec![
        Ident::with_quote('"', alias),
        Ident::with_quote('"', name),
    ])
}

fn cast_join_column(column: Expr, input: Option<&str>, common: &str) -> Result<Expr> {
    if input.is_none_or(|input| input == common) {
        return Ok(column);
    }
    let data_type = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(common)
        .and_then(|mut parser| parser.parse_data_type())
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    Ok(Expr::Cast {
        kind: sqlparser::ast::CastKind::DoubleColon,
        expr: Box::new(column),
        data_type,
        array: false,
        format: None,
    })
}

struct ReplaceMergedColumns<'a> {
    columns: &'a [NamedJoinColumn],
    query_depth: usize,
}

impl<'a> ReplaceMergedColumns<'a> {
    fn new(columns: &'a [NamedJoinColumn]) -> Self {
        Self {
            columns,
            query_depth: 0,
        }
    }
}

impl VisitorMut for ReplaceMergedColumns<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.query_depth == 0 {
            if let Expr::Identifier(ident) = expr {
                if let Some(column) = self
                    .columns
                    .iter()
                    .find(|column| column.name == ident_value(ident))
                {
                    *expr = column.expression.clone();
                }
            }
        }
        ControlFlow::Continue(())
    }
}
